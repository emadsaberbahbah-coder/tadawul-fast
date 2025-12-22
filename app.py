# app.py
"""
Emad Bahbah – Financial Engine (Sheets Analyzer)
------------------------------------------------
FULL REPLACEMENT (ONE-SHOT) – v2.3.2 (HARDENED)

Key improvements vs your current v2.3.1:
✅ No secrets in code (token never printed/returned/logged)
✅ EODHD token read from ENV (EODHD_API_TOKEN preferred, or EODHD_API_KEY)
✅ Engine is a lazy singleton; auto-reloads if token changes (uses SHA fingerprint)
✅ Optional API auth via ENV:
   - REQUIRE_AUTH=true
   - APP_TOKEN or TFB_APP_TOKEN or BACKUP_APP_TOKEN
   Header: X-APP-TOKEN
✅ Auto-detect header row (supports Row 1, Row 5, etc.)
✅ Uses chunked sheet updates to avoid Google API payload limits
✅ Advanced analysis import is optional + safe fallback
✅ Keeps base headers unchanged; diagnostics appended only at END

ENV used:
- GOOGLE_CREDENTIALS (required)
- EODHD_API_TOKEN or EODHD_API_KEY (required for real data)
- REQUIRE_AUTH (optional) + APP_TOKEN / TFB_APP_TOKEN / BACKUP_APP_TOKEN
- LOG_LEVEL (optional)
- SHEET_HEADER_SCAN_MAX (optional, default 10)
- SHEET_HEADER_ROW_DEFAULT (optional, default 1)
"""

from __future__ import annotations

import os
import json
import math
import logging
import hashlib
from datetime import datetime, timezone
from functools import wraps
from typing import Any, Dict, List, Optional, Tuple

from flask import Flask, request, jsonify
import gspread
from google.oauth2.service_account import Credentials
from gspread.exceptions import WorksheetNotFound

from finance_engine import FinanceEngine

# -----------------------------------------------------------------------------
# Optional Advanced Analysis (safe import)
# -----------------------------------------------------------------------------
_ADVANCED_OK = False
def _advanced_fallback(**kwargs) -> Dict[str, Any]:
    # Minimal fallback so the app never crashes if ai_advanced is missing.
    base_score = kwargs.get("base_score")
    shariah = kwargs.get("shariah") or {}
    ticker = kwargs.get("ticker") or ""
    compliant = shariah.get("compliant", None)
    sh_txt = "YES" if compliant is True else "NO" if compliant is False else "UNKNOWN"
    rec = "BUY" if (base_score is not None and float(base_score) >= 75) else "WATCH"
    return {
        "opportunity_score": float(base_score) if base_score is not None else 50.0,
        "recommendation": rec,
        "risk_bucket": "MODERATE",
        "ai_summary": f"{ticker} | Score={base_score} | Shariah={sh_txt}",
        "why_selected": "Fallback analysis (ai_advanced not enabled)",
        "data_quality_score": None,
        "data_quality_flags": [],
        "market_cap": None,
        "vol_ann": None,
        "max_dd_90d": None,
    }

try:
    from ai_advanced import analyze as advanced_analyze  # type: ignore
    _ADVANCED_OK = True
except Exception:
    advanced_analyze = _advanced_fallback  # type: ignore
    _ADVANCED_OK = False

# -----------------------------------------------------------------------------
# Portfolio (keep as-is; if missing we degrade gracefully)
# -----------------------------------------------------------------------------
_PORTFOLIO_OK = False
try:
    from portfolio_engine import (  # type: ignore
        PortfolioEngine,
        PORTFOLIO_HEADERS as PORTFOLIO_HEADERS_LEGACY,
        build_price_map_from_market_data,
    )
    _PORTFOLIO_OK = True
except Exception:
    PortfolioEngine = None  # type: ignore
    PORTFOLIO_HEADERS_LEGACY = []  # type: ignore
    build_price_map_from_market_data = None  # type: ignore
    _PORTFOLIO_OK = False

# -----------------------------------------------------------------------------
# Try to import schema as Source of Truth (preferred)
# -----------------------------------------------------------------------------
try:
    from sheet_schema import (  # type: ignore
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
# ENV helpers (NO secrets hardcoded)
# -----------------------------------------------------------------------------
def env_str(name: str, default: str = "") -> str:
    v = os.getenv(name)
    return (v if v is not None else default).strip()

def env_bool(name: str, default: bool = False) -> bool:
    v = env_str(name, "")
    if v == "":
        return default
    return v.lower() in ("1", "true", "yes", "y", "on")

def env_int(name: str, default: int) -> int:
    try:
        return int(env_str(name, str(default)))
    except Exception:
        return default

APP_VERSION = "2.3.2"

LOG_LEVEL = env_str("LOG_LEVEL", "INFO").upper()
logging.basicConfig(level=LOG_LEVEL, format="%(asctime)s | %(levelname)s | %(message)s")
log = logging.getLogger("financial-engine")

SHEET_HEADER_SCAN_MAX = env_int("SHEET_HEADER_SCAN_MAX", 10)
SHEET_HEADER_ROW_DEFAULT = env_int("SHEET_HEADER_ROW_DEFAULT", 1)

REQUIRE_AUTH = env_bool("REQUIRE_AUTH", False)
APP_TOKEN = env_str("APP_TOKEN") or env_str("TFB_APP_TOKEN") or env_str("BACKUP_APP_TOKEN")

DEFAULT_EOD_EXCHANGE = env_str("DEFAULT_EOD_EXCHANGE", "US").upper()

app = Flask(__name__)

SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]
_GOOGLE_CLIENT: Optional[gspread.Client] = None

# -----------------------------------------------------------------------------
# Headers (append diagnostics only at end, do NOT shift base columns)
# -----------------------------------------------------------------------------
EXTRA_MARKET_HEADERS: List[str] = [
    "EODHD Symbol",
    "Risk Bucket",
    "Why Selected",
    "Data Quality Label",
    "Data Quality Score",
    "Data Quality Flags",
]
EXTRA_TOP_HEADERS: List[str] = ["Risk Bucket", "Why Selected"]

MARKET_HEADERS: List[str] = list(BASE_MARKET_HEADERS) + [h for h in EXTRA_MARKET_HEADERS if h not in BASE_MARKET_HEADERS]
TOP_HEADERS: List[str] = list(BASE_TOP_HEADERS) + [h for h in EXTRA_TOP_HEADERS if h not in BASE_TOP_HEADERS]
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
    t = normalize_ticker(ticker)
    if not t:
        return t
    if t.replace(".", "").isdigit():
        return f"{t}.SR" if not t.endswith(".SR") else t
    if t.endswith(".SR"):
        return t
    if "." in t:
        return t
    return f"{t}.{DEFAULT_EOD_EXCHANGE}"

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

def detect_header_row(ws: gspread.Worksheet, headers: List[str], max_scan: int, default_row: int) -> int:
    """
    Find the row where headers exist (supports Row 1, Row 5, etc.).
    If not found, return default_row.
    """
    want0 = headers[0] if headers else ""
    for r in range(1, max(1, max_scan) + 1):
        vals = ws.row_values(r)
        if vals[: len(headers)] == headers:
            return r
        # tolerant quick match: first cell equals header[0]
        if vals and want0 and vals[0].strip() == want0:
            # if 3+ first cols match, accept
            if len(headers) >= 3 and len(vals) >= 3:
                if vals[1].strip() == headers[1] and vals[2].strip() == headers[2]:
                    return r
    return max(1, int(default_row))

def ensure_headers_at(ws: gspread.Worksheet, headers: List[str], header_row: int, force: bool = True) -> None:
    existing = ws.row_values(header_row)
    if existing[: len(headers)] != headers:
        if not force:
            raise ValueError(f"Header mismatch on sheet '{ws.title}' at row {header_row}.")
        end_col = _col_letter(len(headers))
        ws.update(values=[headers], range_name=f"A{header_row}:{end_col}{header_row}")

def chunked_update(ws: gspread.Worksheet, start_row: int, start_col: int, rows: List[List[Any]], total_cols: int, chunk_size: int = 300) -> None:
    """
    Writes rows in chunks to avoid Google API payload limits.
    """
    if not rows:
        return
    left = _col_letter(start_col)
    right = _col_letter(start_col + total_cols - 1)
    for i in range(0, len(rows), chunk_size):
        chunk = rows[i : i + chunk_size]
        r1 = start_row + i
        r2 = r1 + len(chunk) - 1
        rng = f"{left}{r1}:{right}{r2}"
        ws.update(values=chunk, range_name=rng)

def safe_batch_clear(ws: gspread.Worksheet, ranges: List[str]) -> None:
    try:
        ws.batch_clear(ranges)
    except Exception:
        pass

# -----------------------------------------------------------------------------
# Optional API auth (ENV-controlled)
# -----------------------------------------------------------------------------
def require_token(fn):
    @wraps(fn)
    def wrapper(*args, **kwargs):
        if not REQUIRE_AUTH:
            return fn(*args, **kwargs)
        if not APP_TOKEN:
            return jsonify({"status": "error", "message": "REQUIRE_AUTH enabled but no APP_TOKEN/TFB_APP_TOKEN/BACKUP_APP_TOKEN set"}), 500
        token = request.headers.get("X-APP-TOKEN", "").strip()
        if token != APP_TOKEN:
            return jsonify({"status": "error", "message": "Unauthorized"}), 401
        return fn(*args, **kwargs)
    return wrapper

# -----------------------------------------------------------------------------
# Finance Engine singleton (token from ENV, never exposed)
# -----------------------------------------------------------------------------
_ENGINE: Optional[FinanceEngine] = None
_ENGINE_FP: Optional[str] = None

def _read_eodhd_token_from_env() -> str:
    return env_str("EODHD_API_TOKEN") or env_str("EODHD_API_KEY") or "demo"

def _fingerprint(s: str) -> str:
    # Not reversible, does not leak token
    return hashlib.sha256((s or "").encode("utf-8")).hexdigest()[:12]

def get_engine() -> FinanceEngine:
    global _ENGINE, _ENGINE_FP
    token = _read_eodhd_token_from_env()
    fp = _fingerprint(token)
    if _ENGINE is None or _ENGINE_FP != fp:
        if not token or token.strip().lower() == "demo":
            log.warning("EODHD token missing or 'demo'. Set EODHD_API_TOKEN or EODHD_API_KEY for real data.")
        _ENGINE = FinanceEngine(token)
        _ENGINE_FP = fp
    return _ENGINE

def eodhd_token_is_set() -> bool:
    token = env_str("EODHD_API_TOKEN") or env_str("EODHD_API_KEY")
    if not token:
        return False
    return token.strip().lower() != "demo"

# -----------------------------------------------------------------------------
# Routes
# -----------------------------------------------------------------------------
@app.route("/")
def home():
    return f"Emad Bahbah Financial Engine is Running. v{APP_VERSION}"

@app.route("/api/health", methods=["GET"])
def health():
    return jsonify({
        "status": "ok",
        "version": APP_VERSION,
        "time_utc": now_utc_str(),
        "schema_ok": _SCHEMA_OK,
        "advanced_ok": _ADVANCED_OK,
        "portfolio_ok": _PORTFOLIO_OK,
        "require_auth": REQUIRE_AUTH,
        "eodhd_token_set": eodhd_token_is_set(),
    })

@app.route("/api/analyze", methods=["POST"])
@require_token
def analyze_portfolio():
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

        # Detect header rows (supports Row 1 or Row 5 etc.)
        market_header_row = detect_header_row(ws_market, MARKET_HEADERS, SHEET_HEADER_SCAN_MAX, SHEET_HEADER_ROW_DEFAULT)
        top_header_row = detect_header_row(ws_top, TOP_HEADERS, SHEET_HEADER_SCAN_MAX, 1)
        port_header_row = detect_header_row(ws_port, PORTFOLIO_HEADERS, SHEET_HEADER_SCAN_MAX, 1) if ws_port else 1

        ensure_headers_at(ws_market, MARKET_HEADERS, market_header_row, force=force_headers)
        ensure_headers_at(ws_top, TOP_HEADERS, top_header_row, force=force_headers)
        if ws_port:
            ensure_headers_at(ws_port, PORTFOLIO_HEADERS, port_header_row, force=force_headers)

        market_first_data_row = market_header_row + 1
        top_first_data_row = top_header_row + 1
        port_first_data_row = port_header_row + 1

        H = _header_map(MARKET_HEADERS)

        # Read tickers from column A starting from first data row
        colA = ws_market.col_values(1)  # 1-indexed, returns list where index 0 is row 1
        raw = colA[market_first_data_row - 1 :] if len(colA) >= market_first_data_row else []
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

        engine = get_engine()

        for ticker in tickers:
            eod_symbol = to_eodhd_symbol(ticker)

            try:
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
                ) or {}

                score = safe_float(adv.get("opportunity_score")) or safe_float(base_score) or 0.0
                recommendation = adv.get("recommendation") or "WATCH"
                ai_summary = adv.get("ai_summary") or ""
                why_selected = adv.get("why_selected") or ""
                risk_bucket = adv.get("risk_bucket") or "MODERATE"

                dq_score = safe_float(adv.get("data_quality_score"))
                dq_flags = adv.get("data_quality_flags")
                dq_flags_str = ",".join(str(x) for x in dq_flags[:10]) if isinstance(dq_flags, list) else ""
                dq_label = _dq_label_from_score(dq_score)

                general = fund.get("General", {}) if isinstance(fund.get("General", {}), dict) else {}
                highlights = fund.get("Highlights", {}) if isinstance(fund.get("Highlights", {}), dict) else {}
                valuation = fund.get("Valuation", {}) if isinstance(fund.get("Valuation", {}), dict) else {}
                shares = fund.get("SharesStats", {}) if isinstance(fund.get("SharesStats", {}), dict) else {}
                tech = fund.get("Technicals", {}) if isinstance(fund.get("Technicals", {}), dict) else {}
                meta = fund.get("meta", {}) if isinstance(fund.get("meta", {}), dict) else {}

                market = fund.get("market") or infer_market_from_ticker(ticker)
                currency = general.get("CurrencyCode") or fund.get("currency") or "N/A"

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

                vol_ann = safe_float(tech.get("Volatility30D_Ann")) or safe_float(adv.get("vol_ann"))
                dd90 = safe_float(tech.get("MaxDrawdown90D")) or safe_float(adv.get("max_dd_90d"))

                pred30 = safe_float(ai.get("predicted_price_30d"))
                roi30 = safe_float(ai.get("expected_roi_pct"))
                pred90 = safe_float(ai.get("predicted_price_90d"))
                roi90 = safe_float(ai.get("expected_roi_90d_pct"))
                conf = safe_float(ai.get("confidence"))

                roi30_frac = (roi30 / 100.0) if roi30 is not None else None
                roi90_frac = (roi90 / 100.0) if roi90 is not None else None

                compliant_val = shariah.get("compliant", None)
                compliant_txt = "YES" if compliant_val is True else "NO" if compliant_val is False else "UNKNOWN"

                data_source = ai.get("data_source") or meta.get("provider") or f"EODHD({eod_symbol})"
                data_quality_text = meta.get("data_quality") or dq_label

                row = _make_empty_row(MARKET_HEADERS)

                # Base columns
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
                _set_if_exists(row, H, "Recommendation", recommendation)

                summary_compact = (ai_summary or "").strip()
                if why_selected:
                    summary_compact = f"{summary_compact} | Why: {why_selected}".strip(" |")
                if risk_bucket:
                    summary_compact = f"{summary_compact} | Risk: {risk_bucket}".strip(" |")
                _set_if_exists(row, H, "AI Summary", summary_compact)

                _set_if_exists(row, H, "Updated At (UTC)", now_utc_str())
                _set_if_exists(row, H, "Data Source", data_source)
                _set_if_exists(row, H, "Data Quality", data_quality_text)

                # Diagnostics appended (safe)
                _set_if_exists(row, H, "EODHD Symbol", eod_symbol)
                _set_if_exists(row, H, "Risk Bucket", risk_bucket)
                _set_if_exists(row, H, "Why Selected", why_selected)
                _set_if_exists(row, H, "Data Quality Label", dq_label)
                _set_if_exists(row, H, "Data Quality Score", dq_score)
                _set_if_exists(row, H, "Data Quality Flags", dq_flags_str)

                rows.append(row)

                pick_obj = {
                    "ticker": ticker,
                    "name": general.get("Name") or fund.get("name") or "N/A",
                    "sector": general.get("Sector") or "N/A",
                    "current_price": current_price,
                    "pred30": pred30,
                    "roi30": roi30,   # percent
                    "score": float(score),
                    "recommendation": recommendation,
                    "compliant": (compliant_val is True),
                    "why_selected": why_selected,
                    "risk_bucket": risk_bucket,
                    "updated_at": now_utc_str(),
                }

                if score >= 60:
                    picks_all.append(pick_obj)
                    if pick_obj["compliant"]:
                        picks_shariah.append(pick_obj)

            except Exception as inner:
                msg = str(inner)
                log.warning("Ticker failed (%s -> %s): %s", ticker, eod_symbol, msg)

                err_row = _make_empty_row(MARKET_HEADERS)
                _set_if_exists(err_row, H, "Ticker", ticker)
                _set_if_exists(err_row, H, "Market", infer_market_from_ticker(ticker))
                _set_if_exists(err_row, H, "AI Summary", f"ERROR: {msg}")
                _set_if_exists(err_row, H, "Updated At (UTC)", now_utc_str())
                _set_if_exists(err_row, H, "Data Source", "FinanceEngine")
                _set_if_exists(err_row, H, "Data Quality", "ERROR")

                _set_if_exists(err_row, H, "EODHD Symbol", eod_symbol)
                _set_if_exists(err_row, H, "Risk Bucket", "UNKNOWN")
                _set_if_exists(err_row, H, "Data Quality Label", "POOR")
                _set_if_exists(err_row, H, "Data Quality Score", 0)
                _set_if_exists(err_row, H, "Data Quality Flags", "ERROR")
                rows.append(err_row)

        def sort_key(r: List[Any]) -> Tuple[float, float]:
            s = safe_float(r[H.get("Score (0-100)", 0)]) or 0.0
            roi30_frac_val = safe_float(r[H.get("AI Expected ROI 30D %", 0)])
            roi30_pct_val = (roi30_frac_val * 100.0) if roi30_frac_val is not None else -9999.0
            return (s, roi30_pct_val)

        rows_sorted = sorted(rows, key=sort_key, reverse=True)

        rank_col = H.get("Rank")
        if rank_col is not None:
            for i, r in enumerate(rows_sorted, start=1):
                if rank_col < len(r):
                    r[rank_col] = i

        # Write Market rows (chunked)
        if rows_sorted:
            total_cols = len(MARKET_HEADERS)
            start_row = market_first_data_row
            chunked_update(ws_market, start_row=start_row, start_col=1, rows=rows_sorted, total_cols=total_cols, chunk_size=250)

            if clear_extra_rows:
                end_row = start_row + len(rows_sorted) - 1
                end_col_letter = _col_letter(total_cols)
                start_clear = end_row + 1
                if start_clear <= ws_market.row_count:
                    safe_batch_clear(ws_market, [f"A{start_clear}:{end_col_letter}{ws_market.row_count}"])

        picks_preferred = picks_shariah if top_shariah_only else picks_all
        if not picks_preferred:
            picks_preferred = picks_all

        picks_preferred.sort(
            key=lambda x: (float(x["score"]), float(x["roi30"]) if x["roi30"] is not None else -9999.0),
            reverse=True,
        )
        top7 = picks_preferred[:7]

        # Build Top rows
        top_rows: List[List[Any]] = []
        for idx, p in enumerate(top7, start=1):
            base_row = [
                idx,
                p["ticker"],
                p["name"],
                p["sector"],
                p["current_price"],
                p["pred30"],
                (p["roi30"] / 100.0) if p["roi30"] is not None else None,
                p["score"],
                p["recommendation"],
                "YES" if p["compliant"] else "NO",
                p["updated_at"],
            ]
            if "Risk Bucket" in TOP_HEADERS:
                base_row.append(p.get("risk_bucket") or "")
            if "Why Selected" in TOP_HEADERS:
                base_row.append(p.get("why_selected") or "")
            top_rows.append(base_row)

        # Ensure top headers at detected row, then write data at top_first_data_row
        ensure_headers_at(ws_top, TOP_HEADERS, top_header_row, force=True)
        if top_rows:
            chunked_update(ws_top, start_row=top_first_data_row, start_col=1, rows=top_rows, total_cols=len(TOP_HEADERS), chunk_size=50)
            # clear leftover rows below (optional, small sheet)
            if clear_extra_rows:
                end_row = top_first_data_row + len(top_rows) - 1
                end_col_letter = _col_letter(len(TOP_HEADERS))
                start_clear = end_row + 1
                if start_clear <= ws_top.row_count:
                    safe_batch_clear(ws_top, [f"A{start_clear}:{end_col_letter}{ws_top.row_count}"])

        portfolio_kpi = None
        if ws_port and _PORTFOLIO_OK and PortfolioEngine and build_price_map_from_market_data:
            try:
                # Read market rows from sheet (we already have rows_sorted in memory)
                price_map = build_price_map_from_market_data(MARKET_HEADERS, rows_sorted)

                pe = PortfolioEngine()
                port_values = ws_port.get_all_values()
                port_rows = port_values[port_header_row:] if len(port_values) > port_header_row else []

                updated_port_rows, kpi = pe.compute_table(port_rows, price_map)

                if updated_port_rows:
                    chunked_update(ws_port, start_row=port_first_data_row, start_col=1, rows=updated_port_rows, total_cols=len(PORTFOLIO_HEADERS), chunk_size=250)

                portfolio_kpi = {
                    "total_cost": kpi.total_cost,
                    "current_value": kpi.current_value,
                    "unrealized_pl": kpi.unrealized_pl,
                    "unrealized_pl_pct": kpi.unrealized_pl_pct,
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
            "require_auth": REQUIRE_AUTH,
            "top_picks": [
                {
                    "rank": i + 1,
                    "ticker": p["ticker"],
                    "score": round(float(p["score"]), 2),
                    "roi_30d_pct": p["roi30"],  # percent
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
@require_token
def refresh_portfolio_only():
    try:
        if not (_PORTFOLIO_OK and PortfolioEngine and build_price_map_from_market_data):
            return jsonify({"status": "error", "message": "portfolio_engine not available"}), 500

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

        market_header_row = detect_header_row(ws_market, MARKET_HEADERS, SHEET_HEADER_SCAN_MAX, SHEET_HEADER_ROW_DEFAULT)
        port_header_row = detect_header_row(ws_port, PORTFOLIO_HEADERS, SHEET_HEADER_SCAN_MAX, 1)

        ensure_headers_at(ws_market, MARKET_HEADERS, market_header_row, force=force_headers)
        ensure_headers_at(ws_port, PORTFOLIO_HEADERS, port_header_row, force=force_headers)

        # Read market table
        market_values = ws_market.get_all_values()
        market_rows = market_values[market_header_row:] if len(market_values) > market_header_row else []
        price_map = build_price_map_from_market_data(MARKET_HEADERS, market_rows)

        pe = PortfolioEngine()
        port_values = ws_port.get_all_values()
        port_rows = port_values[port_header_row:] if len(port_values) > port_header_row else []
        updated_port_rows, kpi = pe.compute_table(port_rows, price_map)

        port_first_data_row = port_header_row + 1
        if updated_port_rows:
            chunked_update(ws_port, start_row=port_first_data_row, start_col=1, rows=updated_port_rows, total_cols=len(PORTFOLIO_HEADERS), chunk_size=250)

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
