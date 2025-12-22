# app.py
"""
Emad Bahbah – Financial Engine (Sheets Analyzer)
------------------------------------------------
FULL REPLACEMENT (ONE-SHOT) – v2.3.0

PRIMARY GOAL (ALIGNMENT):
✅ Keeps the FIRST columns exactly aligned with your setup_sheet.py / sheet_schema.py
   so existing column-letter formatting (prices/percent) does NOT break.

OPTIONAL (EXTENDED DIAGNOSTICS):
✅ Appends extra diagnostic columns at the END only (no shifting):
   - EODHD Symbol
   - Risk Bucket
   - Why Selected
   - Data Quality Label / Score / Flags

Top 7:
✅ Never empty (Shariah-first, fallback to best overall)

Portfolio:
✅ Refreshes "My Investment" using Market Data current prices

ENV required:
- GOOGLE_CREDENTIALS  (service account JSON string)
- EODHD_API_TOKEN     (avoid "demo" in production)

Procfile:
  web: gunicorn app:app --bind 0.0.0.0:$PORT --timeout 600
"""

from __future__ import annotations

import os
import json
import math
import logging
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

from flask import Flask, request, jsonify
import gspread
from google.oauth2.service_account import Credentials
from gspread.exceptions import WorksheetNotFound

from finance_engine import FinanceEngine
from ai_advanced import analyze as advanced_analyze
from portfolio_engine import (
    PortfolioEngine,
    PORTFOLIO_HEADERS as PORTFOLIO_HEADERS_LEGACY,
    build_price_map_from_market_data,
)

# -----------------------------------------------------------------------------
# Try to import schema as Source of Truth (preferred)
# -----------------------------------------------------------------------------
try:
    from sheet_schema import (
        SHEET_MARKET_DATA,
        SHEET_TOP_7,
        SHEET_PORTFOLIO,
        MARKET_HEADERS as BASE_MARKET_HEADERS,
        TOP_HEADERS as BASE_TOP_HEADERS,
        PORTFOLIO_HEADERS as BASE_PORTFOLIO_HEADERS,
    )
    _SCHEMA_OK = True
except Exception:
    _SCHEMA_OK = False
    SHEET_MARKET_DATA = "Market Data"
    SHEET_TOP_7 = "Top 7 Opportunities"
    SHEET_PORTFOLIO = "My Investment"

    # Fallback (must match setup_sheet.py exactly)
    BASE_MARKET_HEADERS: List[str] = [
        "Ticker",
        "Name",
        "Market",
        "Currency",
        "Sector",
        "Industry",
        "Current Price",
        "Previous Close",
        "Change",
        "Change %",
        "Day High",
        "Day Low",
        "52W High",
        "52W Low",
        "Volume",
        "Market Cap",
        "Shares Outstanding",
        "EPS (TTM)",
        "P/E (TTM)",
        "P/B",
        "Dividend Yield %",
        "Beta",
        "Volatility 30D (Ann.)",
        "Max Drawdown 90D",
        "AI Predicted Price 30D",
        "AI Expected ROI 30D %",
        "AI Predicted Price 90D",
        "AI Expected ROI 90D %",
        "AI Confidence (0-100)",
        "Shariah Compliant",
        "Score (0-100)",
        "Rank",
        "Recommendation",
        "AI Summary",
        "Updated At (UTC)",
        "Data Source",
        "Data Quality",
    ]

    BASE_TOP_HEADERS: List[str] = [
        "Rank",
        "Ticker",
        "Name",
        "Sector",
        "Current Price",
        "AI Predicted Price 30D",
        "AI Expected ROI 30D %",
        "Score (0-100)",
        "Recommendation",
        "Shariah Compliant",
        "Updated At (UTC)",
    ]

    BASE_PORTFOLIO_HEADERS: List[str] = list(PORTFOLIO_HEADERS_LEGACY)


# -----------------------------------------------------------------------------
# App config
# -----------------------------------------------------------------------------
app = Flask(__name__)

APP_VERSION = "2.3.0"

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO").upper(),
    format="%(asctime)s | %(levelname)s | %(message)s",
)
log = logging.getLogger("financial-engine")

SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]

_GOOGLE_CLIENT: Optional[gspread.Client] = None


# -----------------------------------------------------------------------------
# Headers (base + appended diagnostics; appended ONLY to avoid shifting columns)
# -----------------------------------------------------------------------------
EXTRA_MARKET_HEADERS: List[str] = [
    "EODHD Symbol",
    "Risk Bucket",
    "Why Selected",
    "Data Quality Label",
    "Data Quality Score",
    "Data Quality Flags",
]

EXTRA_TOP_HEADERS: List[str] = [
    "Risk Bucket",
    "Why Selected",
]

# Final headers written to sheets (safe: appended at end)
MARKET_HEADERS: List[str] = list(BASE_MARKET_HEADERS) + [
    h for h in EXTRA_MARKET_HEADERS if h not in BASE_MARKET_HEADERS
]
TOP_HEADERS: List[str] = list(BASE_TOP_HEADERS) + [
    h for h in EXTRA_TOP_HEADERS if h not in BASE_TOP_HEADERS
]
PORTFOLIO_HEADERS: List[str] = list(BASE_PORTFOLIO_HEADERS)


# -----------------------------------------------------------------------------
# Helpers
# -----------------------------------------------------------------------------
def now_utc_str() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")


def safe_float(x: Any) -> Optional[float]:
    try:
        if x is None or isinstance(x, bool):
            return None
        if isinstance(x, (int, float)):
            v = float(x)
            if math.isnan(v):
                return None
            return v
        s = str(x).strip().replace(",", "")
        if s == "" or s.lower() in {"na", "n/a", "none", "null", "-"}:
            return None
        if s.endswith("%"):
            return float(s[:-1].strip())
        return float(s)
    except Exception:
        return None


def normalize_ticker(t: str) -> str:
    return (t or "").strip().upper()


def is_ksa_ticker(ticker: str) -> bool:
    t = normalize_ticker(ticker)
    return t.endswith(".SR") or t.replace(".", "").isdigit()


def infer_market_from_ticker(ticker: str) -> str:
    return "KSA" if is_ksa_ticker(ticker) else "GLOBAL"


def to_eodhd_symbol(ticker: str) -> str:
    """
    - KSA numeric: 1120 -> 1120.SR
    - KSA .SR stays
    - If contains '.', keep (already has suffix)
    - Otherwise append DEFAULT_EOD_EXCHANGE (default US): AAPL -> AAPL.US
    """
    t = normalize_ticker(ticker)
    if not t:
        return t

    if t.replace(".", "").isdigit():
        return f"{t}.SR" if not t.endswith(".SR") else t

    if t.endswith(".SR"):
        return t

    if "." in t:
        return t

    default_ex = os.getenv("DEFAULT_EOD_EXCHANGE", "US").strip().upper() or "US"
    return f"{t}.{default_ex}"


def _load_google_creds_dict() -> Dict[str, Any]:
    creds_json = os.environ.get("GOOGLE_CREDENTIALS")
    if not creds_json:
        raise ValueError("Missing GOOGLE_CREDENTIALS environment variable")

    creds_dict = json.loads(creds_json)
    pk = creds_dict.get("private_key")
    if isinstance(pk, str) and "\\n" in pk:
        creds_dict["private_key"] = pk.replace("\\n", "\n")
    return creds_dict


def get_google_client() -> gspread.Client:
    global _GOOGLE_CLIENT
    if _GOOGLE_CLIENT is not None:
        return _GOOGLE_CLIENT

    creds_dict = _load_google_creds_dict()
    creds = Credentials.from_service_account_info(creds_dict, scopes=SCOPES)
    _GOOGLE_CLIENT = gspread.authorize(creds)
    return _GOOGLE_CLIENT


def ensure_worksheet(sh: gspread.Spreadsheet, name: str, rows: int = 2000, cols: int = 60) -> gspread.Worksheet:
    try:
        return sh.worksheet(name)
    except WorksheetNotFound:
        return sh.add_worksheet(title=name, rows=rows, cols=cols)


def ensure_headers(ws: gspread.Worksheet, headers: List[str], force: bool = True) -> None:
    """
    force=True (default): always enforce our header row for alignment.
    This is safest because app writes full rows by position.
    """
    existing = ws.row_values(1)
    if existing[: len(headers)] != headers:
        if force:
            ws.update(values=[headers], range_name="A1")
        else:
            raise ValueError(f"Header mismatch on sheet '{ws.title}'. Set force_headers=true to overwrite.")


def _col_letter(n: int) -> str:
    return gspread.utils.rowcol_to_a1(1, n).split("1")[0]


def _make_empty_row(headers: List[str]) -> List[Any]:
    return [""] * len(headers)


def _header_map(headers: List[str]) -> Dict[str, int]:
    return {h: i for i, h in enumerate(headers)}


def _set_if_exists(row: List[Any], H: Dict[str, int], col: str, value: Any) -> None:
    i = H.get(col)
    if i is not None and i < len(row):
        row[i] = value


def _dq_label_from_score(score: Optional[float]) -> str:
    if score is None:
        return "UNKNOWN"
    if score >= 85:
        return "EXCELLENT"
    if score >= 70:
        return "GOOD"
    if score >= 50:
        return "FAIR"
    return "POOR"


# -----------------------------------------------------------------------------
# Finance Engine init
# -----------------------------------------------------------------------------
EODHD_API_KEY = os.environ.get("EODHD_API_TOKEN", "demo").strip()
if not EODHD_API_KEY or EODHD_API_KEY.lower() == "demo":
    log.warning("EODHD_API_TOKEN is missing or set to 'demo'. Expect limited/blocked responses on EODHD.")
engine = FinanceEngine(EODHD_API_KEY)


# -----------------------------------------------------------------------------
# Routes
# -----------------------------------------------------------------------------
@app.route("/")
def home():
    return f"Emad Bahbah Financial Engine is Running. v{APP_VERSION}"


@app.route("/api/health", methods=["GET"])
def health():
    return jsonify({"status": "ok", "version": APP_VERSION, "time_utc": now_utc_str(), "schema_ok": _SCHEMA_OK})


@app.route("/api/analyze", methods=["POST"])
def analyze_portfolio():
    """
    Reads tickers from Market sheet column A (row2+),
    writes Market Data, Top 7, and refreshes My Investment.
    """
    try:
        payload = request.get_json(silent=True) or {}
        sheet_url = payload.get("sheet_url")
        if not sheet_url:
            return jsonify({"status": "error", "message": "sheet_url is required"}), 400

        market_sheet_name = payload.get("market_sheet_name", SHEET_MARKET_DATA)
        top_sheet_name = payload.get("top_sheet_name", SHEET_TOP_7)
        portfolio_sheet_name = payload.get("portfolio_sheet_name", SHEET_PORTFOLIO)

        max_tickers = int(payload.get("max_tickers", 1000))
        top_shariah_only = bool(payload.get("top_shariah_only", True))
        clear_extra_rows = bool(payload.get("clear_extra_rows", True))
        force_headers = bool(payload.get("force_headers", True))

        gc = get_google_client()
        sh = gc.open_by_url(sheet_url)

        ws_market = ensure_worksheet(sh, market_sheet_name, rows=4000, cols=max(60, len(MARKET_HEADERS) + 10))
        ws_top = ensure_worksheet(sh, top_sheet_name, rows=300, cols=max(30, len(TOP_HEADERS) + 10))

        ws_port = None
        try:
            ws_port = ensure_worksheet(sh, portfolio_sheet_name, rows=1000, cols=max(30, len(PORTFOLIO_HEADERS) + 10))
        except Exception:
            ws_port = None

        ensure_headers(ws_market, MARKET_HEADERS, force=force_headers)
        ensure_headers(ws_top, TOP_HEADERS, force=force_headers)
        if ws_port:
            ensure_headers(ws_port, PORTFOLIO_HEADERS, force=force_headers)

        H = _header_map(MARKET_HEADERS)

        # Read tickers (A2:A)
        raw = ws_market.col_values(1)[1:]
        tickers: List[str] = []
        seen = set()
        for t in raw:
            tt = normalize_ticker(t)
            if not tt or tt in seen:
                continue
            tickers.append(tt)
            seen.add(tt)
            if len(tickers) >= max_tickers:
                break

        log.info("Analyzing %s tickers...", len(tickers))

        rows: List[List[Any]] = []
        picks_all: List[Dict[str, Any]] = []
        picks_shariah: List[Dict[str, Any]] = []

        for ticker in tickers:
            eod_symbol = to_eodhd_symbol(ticker)

            try:
                # IMPORTANT: call engine using normalized EODHD symbol
                fund, hist = engine.get_stock_data(eod_symbol)

                ai = engine.run_ai_forecasting(hist)
                shariah = engine.check_shariah_compliance(fund)
                base_score = engine.generate_score(fund, ai)

                adv = advanced_analyze(
                    ticker=ticker,
                    fundamentals=fund,
                    hist_data=hist,
                    ai_forecast=ai,
                    shariah=shariah,
                    base_score=safe_float(base_score),
                )

                # Prefer advanced score
                score = safe_float(adv.get("opportunity_score")) or safe_float(base_score) or 0.0
                recommendation = adv.get("recommendation") or "WATCH"
                ai_summary = adv.get("ai_summary") or ""
                why_selected = adv.get("why_selected") or ""
                risk_bucket = adv.get("risk_bucket") or "MODERATE"

                dq_score = safe_float(adv.get("data_quality_score"))
                dq_flags = adv.get("data_quality_flags")
                if isinstance(dq_flags, list):
                    dq_flags_str = ",".join(str(x) for x in dq_flags[:10])
                else:
                    dq_flags_str = ""

                general = fund.get("General", {}) if isinstance(fund.get("General", {}), dict) else {}
                highlights = fund.get("Highlights", {}) if isinstance(fund.get("Highlights", {}), dict) else {}
                valuation = fund.get("Valuation", {}) if isinstance(fund.get("Valuation", {}), dict) else {}
                shares = fund.get("SharesStats", {}) if isinstance(fund.get("SharesStats", {}), dict) else {}
                tech = fund.get("Technicals", {}) if isinstance(fund.get("Technicals", {}), dict) else {}

                market = fund.get("market") or infer_market_from_ticker(ticker)
                currency = general.get("CurrencyCode") or fund.get("currency") or "N/A"

                # Prices/trading
                current_price = safe_float(ai.get("current_price")) or safe_float(tech.get("Price"))
                previous_close = safe_float(ai.get("previous_close")) or safe_float(tech.get("PreviousClose"))
                change = safe_float(ai.get("price_change")) or safe_float(tech.get("Change"))

                change_pct = safe_float(ai.get("percent_change"))
                if change_pct is None:
                    change_pct = safe_float(tech.get("ChangePercent"))
                change_pct_frac = (change_pct / 100.0) if change_pct is not None else None

                day_high = safe_float(ai.get("day_high")) or safe_float(tech.get("DayHigh"))
                day_low = safe_float(ai.get("day_low")) or safe_float(tech.get("DayLow"))
                high_52w = safe_float(ai.get("high_52w")) or safe_float(tech.get("52WeekHigh"))
                low_52w = safe_float(ai.get("low_52w")) or safe_float(tech.get("52WeekLow"))
                volume = safe_float(ai.get("volume")) or safe_float(tech.get("Volume"))

                # Fundamentals
                market_cap = (
                    safe_float(adv.get("market_cap"))
                    or safe_float(valuation.get("MarketCapitalization"))
                    or safe_float(highlights.get("MarketCapitalization"))
                )
                shares_out = safe_float(shares.get("SharesOutstanding")) or safe_float(ai.get("shares_outstanding"))
                eps = safe_float(highlights.get("EarningsShare")) or safe_float(ai.get("eps_ttm"))
                pe = safe_float(valuation.get("TrailingPE")) or safe_float(ai.get("pe_ttm"))
                pb = safe_float(valuation.get("PriceBookMRQ")) or safe_float(ai.get("pb"))
                divy = safe_float(highlights.get("DividendYield"))
                divy_frac = (divy / 100.0) if divy is not None else None
                beta = safe_float(highlights.get("Beta")) or safe_float(ai.get("beta")) or safe_float(tech.get("Beta"))

                # Risk (already fraction)
                vol_ann = safe_float(tech.get("Volatility30D_Ann")) or safe_float(adv.get("vol_ann"))
                dd90 = safe_float(tech.get("MaxDrawdown90D")) or safe_float(adv.get("max_dd_90d"))

                # AI forecasts
                pred30 = safe_float(ai.get("predicted_price_30d"))
                roi30 = safe_float(ai.get("expected_roi_pct"))
                pred90 = safe_float(ai.get("predicted_price_90d"))
                roi90 = safe_float(ai.get("expected_roi_90d_pct"))
                conf = safe_float(ai.get("confidence"))

                roi30_frac = (roi30 / 100.0) if roi30 is not None else None
                roi90_frac = (roi90 / 100.0) if roi90 is not None else None

                compliant_val = shariah.get("compliant", None)
                compliant_txt = "YES" if compliant_val is True else "NO" if compliant_val is False else "UNKNOWN"

                # Data source / quality
                meta = fund.get("meta", {}) if isinstance(fund.get("meta", {}), dict) else {}
                provider_quality = meta.get("data_quality") or meta.get("provider") or ""
                dq_label = _dq_label_from_score(dq_score)
                data_quality_text = provider_quality or dq_label

                row = _make_empty_row(MARKET_HEADERS)

                # Base columns (must match setup_sheet.py ordering)
                _set_if_exists(row, H, "Ticker", ticker)
                _set_if_exists(row, H, "Name", general.get("Name") or fund.get("name") or "N/A")
                _set_if_exists(row, H, "Market", market)
                _set_if_exists(row, H, "Currency", currency)
                _set_if_exists(row, H, "Sector", general.get("Sector") or "N/A")
                _set_if_exists(row, H, "Industry", general.get("Industry") or "N/A")

                _set_if_exists(row, H, "Current Price", current_price)
                _set_if_exists(row, H, "Previous Close", previous_close)
                _set_if_exists(row, H, "Change", change)
                _set_if_exists(row, H, "Change %", change_pct_frac)
                _set_if_exists(row, H, "Day High", day_high)
                _set_if_exists(row, H, "Day Low", day_low)
                _set_if_exists(row, H, "52W High", high_52w)
                _set_if_exists(row, H, "52W Low", low_52w)
                _set_if_exists(row, H, "Volume", volume)

                _set_if_exists(row, H, "Market Cap", market_cap)
                _set_if_exists(row, H, "Shares Outstanding", shares_out)
                _set_if_exists(row, H, "EPS (TTM)", eps)
                _set_if_exists(row, H, "P/E (TTM)", pe)
                _set_if_exists(row, H, "P/B", pb)
                _set_if_exists(row, H, "Dividend Yield %", divy_frac)
                _set_if_exists(row, H, "Beta", beta)
                _set_if_exists(row, H, "Volatility 30D (Ann.)", vol_ann)
                _set_if_exists(row, H, "Max Drawdown 90D", dd90)

                _set_if_exists(row, H, "AI Predicted Price 30D", pred30)
                _set_if_exists(row, H, "AI Expected ROI 30D %", roi30_frac)
                _set_if_exists(row, H, "AI Predicted Price 90D", pred90)
                _set_if_exists(row, H, "AI Expected ROI 90D %", roi90_frac)
                _set_if_exists(row, H, "AI Confidence (0-100)", conf)

                _set_if_exists(row, H, "Shariah Compliant", compliant_txt)
                _set_if_exists(row, H, "Score (0-100)", score)
                # Rank assigned later
                _set_if_exists(row, H, "Recommendation", recommendation)

                # AI Summary (include compact “why” & risk without needing extra columns)
                summary_compact = ai_summary or ""
                if why_selected:
                    summary_compact = f"{summary_compact} | Why: {why_selected}".strip(" |")
                if risk_bucket:
                    summary_compact = f"{summary_compact} | Risk: {risk_bucket}".strip(" |")
                _set_if_exists(row, H, "AI Summary", summary_compact)

                _set_if_exists(row, H, "Updated At (UTC)", now_utc_str())

                data_source = ai.get("data_source") or meta.get("provider") or f"EODHD({eod_symbol})"
                _set_if_exists(row, H, "Data Source", data_source)
                _set_if_exists(row, H, "Data Quality", data_quality_text)

                # Appended diagnostics (at END only)
                _set_if_exists(row, H, "EODHD Symbol", eod_symbol)
                _set_if_exists(row, H, "Risk Bucket", risk_bucket)
                _set_if_exists(row, H, "Why Selected", why_selected)
                _set_if_exists(row, H, "Data Quality Label", dq_label)
                _set_if_exists(row, H, "Data Quality Score", dq_score)
                _set_if_exists(row, H, "Data Quality Flags", dq_flags_str)

                rows.append(row)

                # Candidate picks
                pick_obj = {
                    "ticker": ticker,
                    "name": general.get("Name") or fund.get("name") or "N/A",
                    "sector": general.get("Sector") or "N/A",
                    "current_price": current_price,
                    "pred30": pred30,
                    "roi30": roi30,  # percent number
                    "score": score,
                    "recommendation": recommendation,
                    "compliant": (compliant_val is True),
                    "why_selected": why_selected,
                    "risk_bucket": risk_bucket,
                    "updated_at": row[H.get("Updated At (UTC)", H.get("Updated At (UTC)", 0))] if row else now_utc_str(),
                }

                if score >= 60:
                    picks_all.append(pick_obj)
                    if pick_obj["compliant"]:
                        picks_shariah.append(pick_obj)

            except Exception as inner:
                msg = str(inner)
                if "403" in msg or "Forbidden" in msg:
                    msg = (
                        f"{msg} | HINT: Check EODHD_API_TOKEN and your EODHD plan permissions. "
                        f"Also ensure symbols have exchange suffix (app auto-uses {to_eodhd_symbol(ticker)})."
                    )

                log.warning("Ticker failed (%s -> %s): %s", ticker, eod_symbol, msg)

                err_row = _make_empty_row(MARKET_HEADERS)
                _set_if_exists(err_row, H, "Ticker", ticker)
                _set_if_exists(err_row, H, "Name", "N/A")
                _set_if_exists(err_row, H, "Market", infer_market_from_ticker(ticker))
                _set_if_exists(err_row, H, "AI Summary", f"ERROR: {msg}")
                _set_if_exists(err_row, H, "Updated At (UTC)", now_utc_str())
                _set_if_exists(err_row, H, "Data Source", "FinanceEngine")
                _set_if_exists(err_row, H, "Data Quality", "ERROR")

                # diagnostics
                _set_if_exists(err_row, H, "EODHD Symbol", eod_symbol)
                _set_if_exists(err_row, H, "Risk Bucket", "UNKNOWN")
                _set_if_exists(err_row, H, "Why Selected", "")
                _set_if_exists(err_row, H, "Data Quality Label", "POOR")
                _set_if_exists(err_row, H, "Data Quality Score", 0)
                _set_if_exists(err_row, H, "Data Quality Flags", "ERROR")

                rows.append(err_row)

        # Sort by Score desc then ROI30 desc
        def sort_key(r: List[Any]) -> Tuple[float, float]:
            s = safe_float(r[H.get("Score (0-100)", 0)]) or 0.0
            roi30_frac = safe_float(r[H.get("AI Expected ROI 30D %", 0)])
            roi30_pct = (roi30_frac * 100.0) if roi30_frac is not None else -9999.0
            return (s, roi30_pct)

        rows_sorted = sorted(rows, key=sort_key, reverse=True)

        # Assign Rank
        rank_col = H.get("Rank")
        if rank_col is not None:
            for i, r in enumerate(rows_sorted, start=1):
                if rank_col < len(r):
                    r[rank_col] = i

        # Write Market Data rows (A2:..)
        end_col_letter = _col_letter(len(MARKET_HEADERS))
        if rows_sorted:
            end_row = 1 + len(rows_sorted)
            ws_market.update(values=rows_sorted, range_name=f"A2:{end_col_letter}{end_row}")

            # Clear leftovers below
            if clear_extra_rows:
                start_clear = end_row + 1
                if start_clear <= ws_market.row_count:
                    clear_range = f"A{start_clear}:{end_col_letter}{ws_market.row_count}"
                    try:
                        ws_market.batch_clear([clear_range])
                    except Exception:
                        pass

        # Build Top 7 (Shariah-first)
        picks_preferred = picks_shariah if top_shariah_only else picks_all
        if not picks_preferred:
            picks_preferred = picks_all

        picks_preferred.sort(
            key=lambda x: (float(x["score"]), float(x["roi30"]) if x["roi30"] is not None else -9999.0),
            reverse=True,
        )
        top7 = picks_preferred[:7]

        top_rows: List[List[Any]] = []
        for idx, p in enumerate(top7, start=1):
            # TOP_HEADERS base alignment first, then appended extras (if present)
            base_row = [
                idx,
                p["ticker"],
                p["name"],
                p["sector"],
                p["current_price"],
                p["pred30"],
                (p["roi30"] / 100.0) if p["roi30"] is not None else None,  # fraction for percent format
                p["score"],
                p["recommendation"],
                "YES" if p["compliant"] else "NO",
                p["updated_at"],
            ]

            # If top sheet has extra columns appended (Risk Bucket / Why Selected), add them
            # We append in the same order as TOP_HEADERS definition above.
            if "Risk Bucket" in TOP_HEADERS:
                base_row.append(p.get("risk_bucket") or "")
            if "Why Selected" in TOP_HEADERS:
                base_row.append(p.get("why_selected") or "")

            top_rows.append(base_row)

        # Write Top sheet
        ws_top.update(values=[TOP_HEADERS], range_name="A1")
        if top_rows:
            end_top_col_letter = _col_letter(len(TOP_HEADERS))
            ws_top.update(values=top_rows, range_name=f"A2:{end_top_col_letter}{1+len(top_rows)}")
        else:
            try:
                ws_top.batch_clear([f"A2:{_col_letter(len(TOP_HEADERS))}300"])
            except Exception:
                pass

        # Refresh Portfolio from current Market prices
        portfolio_kpi = None
        if ws_port:
            try:
                # price_map expects header names: "Ticker" and "Current Price"
                price_map = build_price_map_from_market_data(MARKET_HEADERS, rows_sorted)
                pe = PortfolioEngine()

                port_values = ws_port.get_all_values()
                port_rows = port_values[1:] if len(port_values) > 1 else []

                updated_port_rows, kpi = pe.compute_table(port_rows, price_map)

                end_port_col_letter = _col_letter(len(PORTFOLIO_HEADERS))
                if updated_port_rows:
                    end_port_row = 1 + len(updated_port_rows)
                    ws_port.update(values=updated_port_rows, range_name=f"A2:{end_port_col_letter}{end_port_row}")

                portfolio_kpi = {
                    "total_cost": kpi.total_cost,
                    "current_value": kpi.current_value,
                    "unrealized_pl": kpi.unrealized_pl,
                    "unrealized_pl_pct": kpi.unrealized_pl_pct,  # fraction
                    "positions": kpi.positions,
                    "active_positions": kpi.active_positions,
                }
            except Exception as pe_err:
                log.warning("Portfolio refresh skipped: %s", pe_err)

        return jsonify({
            "status": "success",
            "message": "Analysis complete",
            "version": APP_VERSION,
            "tickers_analyzed": len(tickers),
            "rows_written": len(rows_sorted),
            "top_picks": [
                {
                    "rank": i + 1,
                    "ticker": p["ticker"],
                    "score": round(float(p["score"]), 2),
                    "roi_30d_pct": p["roi30"],
                    "recommendation": p["recommendation"],
                    "risk_bucket": p.get("risk_bucket"),
                    "shariah": "YES" if p["compliant"] else "NO",
                }
                for i, p in enumerate(top7)
            ],
            "portfolio_kpi": portfolio_kpi,
        })

    except Exception as e:
        log.exception("Analyze error")
        return jsonify({"status": "error", "message": str(e)}), 500


@app.route("/api/portfolio/refresh", methods=["POST"])
def refresh_portfolio_only():
    """
    Refresh My Investment only (uses current prices from Market Data).
    Body:
      { "sheet_url": "...", "market_sheet_name": "Market Data", "portfolio_sheet_name": "My Investment" }
    """
    try:
        payload = request.get_json(silent=True) or {}
        sheet_url = payload.get("sheet_url")
        if not sheet_url:
            return jsonify({"status": "error", "message": "sheet_url is required"}), 400

        market_sheet_name = payload.get("market_sheet_name", SHEET_MARKET_DATA)
        portfolio_sheet_name = payload.get("portfolio_sheet_name", SHEET_PORTFOLIO)
        force_headers = bool(payload.get("force_headers", True))

        gc = get_google_client()
        sh = gc.open_by_url(sheet_url)

        ws_market = ensure_worksheet(sh, market_sheet_name, rows=4000, cols=max(60, len(MARKET_HEADERS) + 10))
        ws_port = ensure_worksheet(sh, portfolio_sheet_name, rows=1000, cols=max(30, len(PORTFOLIO_HEADERS) + 10))

        ensure_headers(ws_market, MARKET_HEADERS, force=force_headers)
        ensure_headers(ws_port, PORTFOLIO_HEADERS, force=force_headers)

        market_values = ws_market.get_all_values()
        market_rows = market_values[1:] if len(market_values) > 1 else []
        price_map = build_price_map_from_market_data(MARKET_HEADERS, market_rows)

        pe = PortfolioEngine()
        port_values = ws_port.get_all_values()
        port_rows = port_values[1:] if len(port_values) > 1 else []
        updated_port_rows, kpi = pe.compute_table(port_rows, price_map)

        end_col_letter = _col_letter(len(PORTFOLIO_HEADERS))
        if updated_port_rows:
            end_row = 1 + len(updated_port_rows)
            ws_port.update(values=updated_port_rows, range_name=f"A2:{end_col_letter}{end_row}")

        return jsonify({
            "status": "success",
            "message": "Portfolio refreshed",
            "portfolio_kpi": {
                "total_cost": kpi.total_cost,
                "current_value": kpi.current_value,
                "unrealized_pl": kpi.unrealized_pl,
                "unrealized_pl_pct": kpi.unrealized_pl_pct,
                "positions": kpi.positions,
                "active_positions": kpi.active_positions,
            }
        })

    except Exception as e:
        log.exception("Portfolio refresh error")
        return jsonify({"status": "error", "message": str(e)}), 500


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 10000))
    app.run(host="0.0.0.0", port=port)
