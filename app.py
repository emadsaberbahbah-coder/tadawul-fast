# app.py
"""
Emad Bahbah – Financial Engine (Sheets Analyzer)
------------------------------------------------
FULL UPDATED SCRIPT (ONE-SHOT) – v2.2.0

Main fixes & upgrades:
✅ Fixes common EODHD symbol issue: auto-normalizes GLOBAL tickers to ".US" when no exchange suffix
   - Example: AAPL -> AAPL.US   |   COST -> COST.US
   - KSA: 1120 or 1120.SR -> 1120.SR
✅ Adds EODHD Symbol column (so you can SEE what was used for API calls)
✅ Stronger error rows (no broken indices if headers change)
✅ Top 7 never stays empty:
   - Default: prefers Shariah-compliant picks
   - If none available, falls back to best scores regardless of Shariah (still shows YES/NO)
✅ Better data-quality output:
   - Writes Data Quality Label + Data Quality Score + Flags (from ai_advanced)
✅ Optional clearing of old leftover rows (avoids stale data below)

ENV required:
- GOOGLE_CREDENTIALS  (service account JSON as string)
- EODHD_API_TOKEN     (your token; avoid "demo" in production)

Run command (Procfile):
  web: gunicorn app:app --bind 0.0.0.0:$PORT --timeout 600
"""

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
    PORTFOLIO_HEADERS,
    build_price_map_from_market_data,
)

app = Flask(__name__)

APP_VERSION = "2.2.0"

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO").upper(),
    format="%(asctime)s | %(levelname)s | %(message)s",
)
log = logging.getLogger("financial-engine")

# --- CONFIGURATION ---
SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]

_GOOGLE_CLIENT: Optional[gspread.Client] = None


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


def to_eodhd_symbol(ticker: str) -> str:
    """
    EODHD commonly requires an exchange suffix for GLOBAL tickers (e.g., .US).
    - KSA numeric: 1120 -> 1120.SR
    - KSA .SR stays .SR
    - Already has a suffix (contains "."): keep as-is
    - Otherwise assume US: AAPL -> AAPL.US
    """
    t = normalize_ticker(ticker)
    if not t:
        return t

    # KSA numeric
    if t.replace(".", "").isdigit():
        return f"{t}.SR" if not t.endswith(".SR") else t

    # KSA explicit
    if t.endswith(".SR"):
        return t

    # already includes exchange
    if "." in t:
        return t

    # default global exchange
    return f"{t}.US"


def infer_market_from_ticker(ticker: str) -> str:
    return "KSA" if is_ksa_ticker(ticker) else "GLOBAL"


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


# Initialize Finance Engine
EODHD_API_KEY = os.environ.get("EODHD_API_TOKEN", "demo")
if not EODHD_API_KEY or EODHD_API_KEY.strip().lower() == "demo":
    log.warning("EODHD_API_TOKEN is missing or set to 'demo'. Expect limited/blocked responses on EODHD.")
engine = FinanceEngine(EODHD_API_KEY)


# =============================================================================
# Headers
# =============================================================================

MARKET_HEADERS: List[str] = [
    "Ticker",
    "EODHD Symbol",
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
    "Risk Bucket",
    "Score (0-100)",
    "Rank",
    "Recommendation",
    "Why Selected",
    "AI Summary",
    "Updated At (UTC)",
    "Data Source",
    "Data Quality Label",
    "Data Quality Score",
    "Data Quality Flags",
]

TOP_HEADERS: List[str] = [
    "Rank",
    "Ticker",
    "Name",
    "Sector",
    "Current Price",
    "AI Predicted Price 30D",
    "AI Expected ROI 30D %",
    "Score (0-100)",
    "Risk Bucket",
    "Recommendation",
    "Shariah Compliant",
    "Why Selected",
    "Updated At (UTC)",
]


def data_quality_label_from_row(row: List[Any], header_map: Dict[str, int]) -> str:
    """
    Simple completeness-based label (kept for readability).
    We also store numeric/flags from ai_advanced separately.
    """
    try:
        # ignore first 2 columns: Ticker, EODHD Symbol
        vals = row[2:]
        filled = sum(1 for v in vals if v not in (None, "", "N/A"))
        ratio = filled / max(1, len(vals))
        if ratio >= 0.85:
            return "EXCELLENT"
        if ratio >= 0.70:
            return "GOOD"
        if ratio >= 0.50:
            return "FAIR"
        return "POOR"
    except Exception:
        return "POOR"


def ensure_worksheet(sh: gspread.Spreadsheet, name: str, rows: int = 2000, cols: int = 60) -> gspread.Worksheet:
    try:
        return sh.worksheet(name)
    except WorksheetNotFound:
        return sh.add_worksheet(title=name, rows=rows, cols=cols)


def ensure_headers(ws: gspread.Worksheet, headers: List[str]) -> None:
    existing = ws.row_values(1)
    if existing[: len(headers)] != headers:
        ws.update(values=[headers], range_name="A1")


def _col_letter(n: int) -> str:
    # A1 string like 'AN1' -> split '1' to get 'AN'
    return gspread.utils.rowcol_to_a1(1, n).split("1")[0]


def _make_empty_row(headers: List[str]) -> List[Any]:
    return [""] * len(headers)


@app.route("/")
def home():
    return f"Emad Bahbah Financial Engine is Running. v{APP_VERSION}"


@app.route("/api/health", methods=["GET"])
def health():
    return jsonify({"status": "ok", "version": APP_VERSION, "time_utc": now_utc_str()})


@app.route("/api/analyze", methods=["POST"])
def analyze_portfolio():
    """
    Reads tickers from Market sheet col A (row2+),
    writes full enriched outputs, Top 7, and refreshes My Investment values.
    """
    try:
        payload = request.get_json(silent=True) or {}
        sheet_url = payload.get("sheet_url")
        if not sheet_url:
            return jsonify({"status": "error", "message": "sheet_url is required"}), 400

        market_sheet_name = payload.get("market_sheet_name", "Market Data")
        top_sheet_name = payload.get("top_sheet_name", "Top 7 Opportunities")
        portfolio_sheet_name = payload.get("portfolio_sheet_name", "My Investment")
        max_tickers = int(payload.get("max_tickers", 1000))

        # Optional behaviors
        top_shariah_only = bool(payload.get("top_shariah_only", True))
        clear_extra_rows = bool(payload.get("clear_extra_rows", True))

        gc = get_google_client()
        sh = gc.open_by_url(sheet_url)

        ws_market = ensure_worksheet(sh, market_sheet_name, rows=4000, cols=max(60, len(MARKET_HEADERS) + 2))
        ws_top = ensure_worksheet(sh, top_sheet_name, rows=300, cols=max(30, len(TOP_HEADERS) + 2))

        ws_port = None
        try:
            ws_port = ensure_worksheet(sh, portfolio_sheet_name, rows=1000, cols=max(30, len(PORTFOLIO_HEADERS) + 2))
        except Exception:
            ws_port = None

        ensure_headers(ws_market, MARKET_HEADERS)
        ensure_headers(ws_top, TOP_HEADERS)
        if ws_port:
            ensure_headers(ws_port, PORTFOLIO_HEADERS)

        H = {h: i for i, h in enumerate(MARKET_HEADERS)}

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
                    ticker=ticker,          # keep original for analysis labels
                    fundamentals=fund,
                    hist_data=hist,
                    ai_forecast=ai,
                    shariah=shariah,
                    base_score=safe_float(base_score),
                )

                # Choose best score source
                score = safe_float(adv.get("opportunity_score")) or safe_float(base_score) or 0.0
                recommendation = adv.get("recommendation") or "WATCH"
                ai_summary = adv.get("ai_summary") or ""
                why_selected = adv.get("why_selected") or ""
                risk_bucket = adv.get("risk_bucket") or "MODERATE"

                # Data quality
                dq_score = safe_float(adv.get("data_quality_score"))
                dq_flags = adv.get("data_quality_flags")
                if isinstance(dq_flags, list):
                    dq_flags_str = ",".join(str(x) for x in dq_flags[:8])
                else:
                    dq_flags_str = ""

                general = fund.get("General", {}) if isinstance(fund.get("General", {}), dict) else {}
                highlights = fund.get("Highlights", {}) if isinstance(fund.get("Highlights", {}), dict) else {}
                valuation = fund.get("Valuation", {}) if isinstance(fund.get("Valuation", {}), dict) else {}
                shares = fund.get("SharesStats", {}) if isinstance(fund.get("SharesStats", {}), dict) else {}
                tech = fund.get("Technicals", {}) if isinstance(fund.get("Technicals", {}), dict) else {}

                market = fund.get("market") or infer_market_from_ticker(ticker)
                currency = general.get("CurrencyCode") or fund.get("currency") or "N/A"

                # Prices / trading (from ai_forecast extras or Technicals)
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

                # Risk (from Technicals or advanced)
                vol_ann = safe_float(tech.get("Volatility30D_Ann")) or safe_float(adv.get("vol_ann"))
                dd90 = safe_float(tech.get("MaxDrawdown90D")) or safe_float(adv.get("max_dd_90d"))

                # AI forecast outputs
                pred30 = safe_float(ai.get("predicted_price_30d"))
                roi30 = safe_float(ai.get("expected_roi_pct"))
                pred90 = safe_float(ai.get("predicted_price_90d"))
                roi90 = safe_float(ai.get("expected_roi_90d_pct"))
                conf = safe_float(ai.get("confidence"))

                roi30_frac = (roi30 / 100.0) if roi30 is not None else None
                roi90_frac = (roi90 / 100.0) if roi90 is not None else None

                compliant = bool(shariah.get("compliant", False))

                row = _make_empty_row(MARKET_HEADERS)
                row[H["Ticker"]] = ticker
                row[H["EODHD Symbol"]] = eod_symbol
                row[H["Name"]] = general.get("Name") or fund.get("name") or "N/A"
                row[H["Market"]] = market
                row[H["Currency"]] = currency
                row[H["Sector"]] = general.get("Sector") or "N/A"
                row[H["Industry"]] = general.get("Industry") or "N/A"

                row[H["Current Price"]] = current_price
                row[H["Previous Close"]] = previous_close
                row[H["Change"]] = change
                row[H["Change %"]] = change_pct_frac
                row[H["Day High"]] = day_high
                row[H["Day Low"]] = day_low
                row[H["52W High"]] = high_52w
                row[H["52W Low"]] = low_52w
                row[H["Volume"]] = volume

                row[H["Market Cap"]] = market_cap
                row[H["Shares Outstanding"]] = shares_out
                row[H["EPS (TTM)"]] = eps
                row[H["P/E (TTM)"]] = pe
                row[H["P/B"]] = pb
                row[H["Dividend Yield %"]] = divy_frac
                row[H["Beta"]] = beta
                row[H["Volatility 30D (Ann.)"]] = vol_ann
                row[H["Max Drawdown 90D"]] = dd90

                row[H["AI Predicted Price 30D"]] = pred30
                row[H["AI Expected ROI 30D %"]] = roi30_frac
                row[H["AI Predicted Price 90D"]] = pred90
                row[H["AI Expected ROI 90D %"]] = roi90_frac
                row[H["AI Confidence (0-100)"]] = conf

                row[H["Risk Bucket"]] = risk_bucket
                row[H["Score (0-100)"]] = score
                # Rank filled later
                row[H["Recommendation"]] = recommendation
                row[H["Why Selected"]] = why_selected
                row[H["AI Summary"]] = ai_summary
                row[H["Updated At (UTC)"]] = now_utc_str()

                row[H["Data Source"]] = ai.get("data_source") or (fund.get("meta", {}) or {}).get("data_source") or f"EODHD({eod_symbol})"

                # Data quality outputs
                label = data_quality_label_from_row(row, H)
                row[H["Data Quality Label"]] = label
                row[H["Data Quality Score"]] = dq_score
                row[H["Data Quality Flags"]] = dq_flags_str

                rows.append(row)

                # Candidate picks
                pick_obj = {
                    "ticker": ticker,
                    "name": row[H["Name"]],
                    "sector": row[H["Sector"]],
                    "current_price": current_price,
                    "pred30": pred30,
                    "roi30": roi30,  # percent number
                    "score": score,
                    "risk_bucket": risk_bucket,
                    "recommendation": recommendation,
                    "compliant": compliant,
                    "why_selected": why_selected,
                    "updated_at": row[H["Updated At (UTC)"]],
                }

                if score >= 60:
                    picks_all.append(pick_obj)
                    if compliant:
                        picks_shariah.append(pick_obj)

            except Exception as inner:
                msg = str(inner)

                # Hint for the most common setup problems
                if "403" in msg or "Forbidden" in msg:
                    msg = (
                        f"{msg} | HINT: Check EODHD_API_TOKEN and ensure GLOBAL symbols include exchange "
                        f"(we auto-try .US). Also confirm your EODHD plan allows fundamentals/eod endpoints."
                    )

                log.warning("Ticker failed (%s -> %s): %s", ticker, eod_symbol, msg)

                err_row = _make_empty_row(MARKET_HEADERS)
                err_row[H["Ticker"]] = ticker
                err_row[H["EODHD Symbol"]] = eod_symbol
                err_row[H["AI Summary"]] = f"ERROR: {msg}"
                err_row[H["Updated At (UTC)"]] = now_utc_str()
                err_row[H["Data Source"]] = "FinanceEngine"
                err_row[H["Data Quality Label"]] = "POOR"
                err_row[H["Data Quality Score"]] = 0
                err_row[H["Data Quality Flags"]] = "ERROR"
                rows.append(err_row)

        # Sort by Score desc, then ROI30 desc
        def sort_key(r: List[Any]) -> Tuple[float, float]:
            s = safe_float(r[H["Score (0-100)"]]) or 0.0
            roi30_frac = safe_float(r[H["AI Expected ROI 30D %"]])
            roi30_pct = (roi30_frac * 100.0) if roi30_frac is not None else -9999.0
            return (s, roi30_pct)

        rows_sorted = sorted(rows, key=sort_key, reverse=True)

        # Assign Rank
        for i, r in enumerate(rows_sorted, start=1):
            r[H["Rank"]] = i

        # Write Market Data rows in one batch (A2:...)
        end_col_letter = _col_letter(len(MARKET_HEADERS))
        if rows_sorted:
            end_row = 1 + len(rows_sorted)
            ws_market.update(values=rows_sorted, range_name=f"A2:{end_col_letter}{end_row}")

            # Clear leftovers (so old tickers don't remain visible)
            if clear_extra_rows:
                last_row_to_clear_from = end_row + 1
                if last_row_to_clear_from <= ws_market.row_count:
                    # Clear only the area we might have previously written
                    clear_range = f"A{last_row_to_clear_from}:{end_col_letter}{ws_market.row_count}"
                    try:
                        ws_market.batch_clear([clear_range])
                    except Exception:
                        # fallback: overwrite with blanks for a limited window
                        pass

        # Build Top 7
        picks_preferred = picks_shariah if top_shariah_only else picks_all
        if not picks_preferred:
            # fallback to ensure top sheet is never empty
            picks_preferred = picks_all

        picks_preferred.sort(key=lambda x: (x["score"], (x["roi30"] if x["roi30"] is not None else -9999)), reverse=True)
        top7 = picks_preferred[:7]

        top_rows: List[List[Any]] = []
        for idx, p in enumerate(top7, start=1):
            top_rows.append([
                idx,
                p["ticker"],
                p["name"],
                p["sector"],
                p["current_price"],
                p["pred30"],
                (p["roi30"] / 100.0) if p["roi30"] is not None else None,  # fraction for percent format
                p["score"],
                p["risk_bucket"],
                p["recommendation"],
                "YES" if p["compliant"] else "NO",
                p["why_selected"],
                p["updated_at"],
            ])

        ws_top.update(values=[TOP_HEADERS], range_name="A1")
        if top_rows:
            ws_top.update(values=top_rows, range_name=f"A2:M{1+len(top_rows)}")
        else:
            # Clear old rows if no picks
            try:
                ws_top.batch_clear(["A2:M300"])
            except Exception:
                pass

        # Refresh My Investment using latest Market Data prices
        portfolio_kpi = None
        if ws_port:
            try:
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
                    "risk_bucket": p["risk_bucket"],
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

        market_sheet_name = payload.get("market_sheet_name", "Market Data")
        portfolio_sheet_name = payload.get("portfolio_sheet_name", "My Investment")

        gc = get_google_client()
        sh = gc.open_by_url(sheet_url)

        ws_market = ensure_worksheet(sh, market_sheet_name, rows=4000, cols=max(60, len(MARKET_HEADERS) + 2))
        ws_port = ensure_worksheet(sh, portfolio_sheet_name, rows=1000, cols=max(30, len(PORTFOLIO_HEADERS) + 2))

        ensure_headers(ws_market, MARKET_HEADERS)
        ensure_headers(ws_port, PORTFOLIO_HEADERS)

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
