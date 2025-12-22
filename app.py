# app.py
"""
Emad Bahbah Financial Engine (Google Sheets + AI Advanced Analysis)
------------------------------------------------------------------
FULL UPDATED SCRIPT (ONE-SHOT) – v2.0.0

What this version adds (vs your original):
1) ✅ Strong input validation + safer error handling + structured logging
2) ✅ Header auto-build (Market Data) with a much richer schema (current + risk + AI forecast)
3) ✅ Writes results in one batch + clears old extra rows
4) ✅ Auto-ranking + AI recommendation + AI summary (human-readable)
5) ✅ Risk metrics from historical prices (Volatility 30D + Max Drawdown 90D)
6) ✅ Creates/updates a dedicated sheet: "Top 7 Opportunities"
7) ✅ Google Sheets formatting (header style, freeze row, filters, number formats, column widths)
8) ✅ Robust GOOGLE_CREDENTIALS parsing (fixes private_key newline issues)

Expected sheet structure:
- Sheet 1: "Market Data" (tickers in Col A starting from row 2; header in row 1)
- Sheet 2: "Top 7 Opportunities" auto-generated

ENV Vars required on Render:
- GOOGLE_CREDENTIALS   (service account json as a single string)
- EODHD_API_TOKEN      (or demo)
- PORT                 (Render provides)
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

# =============================================================================
# App + Logging
# =============================================================================

APP_VERSION = "2.0.0"
app = Flask(__name__)

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO").upper(),
    format="%(asctime)s | %(levelname)s | %(message)s",
)
log = logging.getLogger("financial-engine")


# =============================================================================
# Google Auth
# =============================================================================

SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]

_GOOGLE_CLIENT: Optional[gspread.Client] = None


def _load_google_creds_dict() -> Dict[str, Any]:
    creds_json = os.environ.get("GOOGLE_CREDENTIALS")
    if not creds_json:
        raise ValueError("Missing GOOGLE_CREDENTIALS environment variable")

    # Render often stores JSON as a single line; ensure private_key newlines are correct.
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


# =============================================================================
# Finance Engine
# =============================================================================

EODHD_API_KEY = os.environ.get("EODHD_API_TOKEN", "demo")
engine = FinanceEngine(EODHD_API_KEY)


# =============================================================================
# Sheet Schema (Enhanced Headers)
# =============================================================================

MARKET_HEADERS: List[str] = [
    # Identity
    "Ticker",
    "Name",
    "Market",
    "Currency",
    "Sector",
    "Industry",
    # Current / Trading
    "Current Price",
    "Previous Close",
    "Change",
    "Change %",
    "Day High",
    "Day Low",
    "52W High",
    "52W Low",
    "Volume",
    # Size / Fundamentals
    "Market Cap",
    "Shares Outstanding",
    "EPS (TTM)",
    "P/E (TTM)",
    "P/B",
    "Dividend Yield %",
    "Beta",
    # Risk (from history)
    "Volatility 30D (Ann.)",
    "Max Drawdown 90D",
    # AI Forecast
    "AI Predicted Price 30D",
    "AI Expected ROI 30D %",
    "AI Predicted Price 90D",
    "AI Expected ROI 90D %",
    "AI Confidence (0-100)",
    # Compliance + Scoring
    "Shariah Compliant",
    "Score (0-100)",
    "Rank",
    "Recommendation",
    "AI Summary",
    # Provenance
    "Updated At (UTC)",
    "Data Source",
    "Data Quality",
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
    "Recommendation",
    "Shariah Compliant",
    "Updated At (UTC)",
]


# =============================================================================
# Helpers
# =============================================================================

def now_utc_str() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")


def safe_float(x: Any) -> Optional[float]:
    try:
        if x is None:
            return None
        if isinstance(x, bool):
            return None
        if isinstance(x, (int, float)):
            if math.isnan(float(x)):
                return None
            return float(x)
        s = str(x).strip().replace(",", "")
        if s == "" or s.lower() in {"na", "n/a", "none", "null", "-"}:
            return None
        return float(s)
    except Exception:
        return None


def normalize_ticker(t: str) -> str:
    return (t or "").strip().upper()


def extract_hist_closes(hist: Any) -> List[float]:
    """
    Accepts different possible hist formats:
    - list[dict] with keys close/Close/adj_close/Adj Close
    - dict with "close" list
    - pandas DataFrame-like with to_dict('records')
    """
    if hist is None:
        return []

    records: List[Dict[str, Any]] = []

    try:
        if hasattr(hist, "to_dict"):
            # pandas DF
            try:
                records = hist.to_dict("records")  # type: ignore
            except Exception:
                pass
        elif isinstance(hist, list) and (len(hist) == 0 or isinstance(hist[0], dict)):
            records = hist  # type: ignore
        elif isinstance(hist, dict):
            # maybe {"close":[...]} or {"data":[{...}]}
            if isinstance(hist.get("data"), list):
                records = hist["data"]  # type: ignore
            elif isinstance(hist.get("close"), list):
                closes = [safe_float(v) for v in hist.get("close", [])]
                return [c for c in closes if c is not None]
    except Exception:
        return []

    closes: List[float] = []
    for r in records:
        if not isinstance(r, dict):
            continue
        c = (
            safe_float(r.get("close"))
            or safe_float(r.get("Close"))
            or safe_float(r.get("adj_close"))
            or safe_float(r.get("Adj Close"))
            or safe_float(r.get("adjusted_close"))
        )
        if c is not None:
            closes.append(c)
    return closes


def compute_risk_metrics(hist: Any) -> Tuple[Optional[float], Optional[float]]:
    """
    Returns:
      - vol_30d_annualized (as fraction, e.g., 0.25 = 25%)
      - max_drawdown_90d (as fraction, e.g., -0.18 = -18%)
    """
    closes = extract_hist_closes(hist)
    if len(closes) < 25:
        return None, None

    # daily returns
    rets: List[float] = []
    for i in range(1, len(closes)):
        if closes[i - 1] and closes[i - 1] != 0:
            rets.append((closes[i] / closes[i - 1]) - 1.0)

    # Volatility 30D (use last ~30 returns)
    last = rets[-30:] if len(rets) >= 30 else rets
    if len(last) >= 10:
        mean = sum(last) / len(last)
        var = sum((r - mean) ** 2 for r in last) / (len(last) - 1)
        vol_daily = math.sqrt(var)
        vol_ann = vol_daily * math.sqrt(252)
    else:
        vol_ann = None

    # Max Drawdown 90D (use last 90 closes)
    window = closes[-90:] if len(closes) >= 90 else closes
    peak = window[0]
    max_dd = 0.0
    for p in window:
        peak = max(peak, p)
        dd = (p / peak) - 1.0
        if dd < max_dd:
            max_dd = dd

    return vol_ann, max_dd


def infer_market_from_ticker(ticker: str) -> str:
    # Simple heuristic; your FinanceEngine may return a better value
    if ticker.endswith(".SR") or ticker.replace(".", "").isdigit():
        return "KSA"
    if ticker.endswith(".US") or ticker.isalpha():
        return "GLOBAL"
    return "GLOBAL"


def build_ai_recommendation(score: Optional[float], roi30: Optional[float], vol_ann: Optional[float]) -> str:
    s = score if score is not None else 0.0
    r = roi30 if roi30 is not None else 0.0
    v = vol_ann if vol_ann is not None else 0.0

    # Simple, explainable rules
    if s >= 80 and r >= 8 and v <= 0.45:
        return "STRONG BUY"
    if s >= 70 and r >= 5:
        return "BUY"
    if s >= 55 and r >= 0:
        return "HOLD"
    if r < 0 and s < 55:
        return "SELL"
    return "WATCH"


def build_ai_summary(
    roi30: Optional[float],
    pred30: Optional[float],
    vol_ann: Optional[float],
    max_dd: Optional[float],
    conf: Optional[float],
    recommendation: str,
) -> str:
    parts = []
    if pred30 is not None:
        parts.append(f"Pred30={pred30:.2f}")
    if roi30 is not None:
        parts.append(f"ROI30={roi30:.1f}%")
    if conf is not None:
        parts.append(f"Conf={conf:.0f}/100")
    if vol_ann is not None:
        parts.append(f"Vol={vol_ann*100:.0f}%")
    if max_dd is not None:
        parts.append(f"DD90={max_dd*100:.0f}%")
    parts.append(f"Action={recommendation}")
    return " | ".join(parts)


def data_quality_label(row: List[Any]) -> str:
    # crude but practical: % filled (excluding ticker)
    vals = row[1:]
    filled = sum(1 for v in vals if v not in (None, "", "N/A"))
    ratio = filled / max(1, len(vals))
    if ratio >= 0.85:
        return "EXCELLENT"
    if ratio >= 0.70:
        return "GOOD"
    if ratio >= 0.50:
        return "FAIR"
    return "POOR"


def ensure_worksheet(sh: gspread.Spreadsheet, name: str, rows: int = 2000, cols: int = 40) -> gspread.Worksheet:
    try:
        return sh.worksheet(name)
    except WorksheetNotFound:
        ws = sh.add_worksheet(title=name, rows=rows, cols=cols)
        return ws


def ensure_headers(ws: gspread.Worksheet, headers: List[str]) -> None:
    existing = ws.row_values(1)
    if existing[: len(headers)] != headers:
        ws.update(values=[headers], range_name="A1")


def apply_formatting_market(ws: gspread.Worksheet, total_cols: int) -> None:
    """
    Uses gspread's Sheets API wrapper methods (format/batch_update) where available.
    Safe: wrapped in try/except so it won't break your analysis if formatting fails.
    """
    try:
        last_col_letter = gspread.utils.rowcol_to_a1(1, total_cols).split("1")[0]
        header_range = f"A1:{last_col_letter}1"

        # Header style
        ws.format(header_range, {
            "textFormat": {"bold": True},
            "horizontalAlignment": "CENTER",
            "verticalAlignment": "MIDDLE",
            "wrapStrategy": "WRAP",
            "backgroundColor": {"red": 0.12, "green": 0.12, "blue": 0.12},
            "foregroundColor": {"red": 1, "green": 1, "blue": 1},
        })

        # Freeze header row
        try:
            ws.freeze(rows=1)
        except Exception:
            pass

        # Basic filter on header row
        try:
            ws.set_basic_filter()
        except Exception:
            pass

        # Column widths (identity narrower, analysis wider)
        sheet_id = ws.id
        requests = [
            # A-F
            {"updateDimensionProperties": {
                "range": {"sheetId": sheet_id, "dimension": "COLUMNS", "startIndex": 0, "endIndex": 6},
                "properties": {"pixelSize": 140},
                "fields": "pixelSize"
            }},
            # G-O (prices/volume)
            {"updateDimensionProperties": {
                "range": {"sheetId": sheet_id, "dimension": "COLUMNS", "startIndex": 6, "endIndex": 15},
                "properties": {"pixelSize": 130},
                "fields": "pixelSize"
            }},
            # P-W (fundamentals/risk)
            {"updateDimensionProperties": {
                "range": {"sheetId": sheet_id, "dimension": "COLUMNS", "startIndex": 15, "endIndex": 23},
                "properties": {"pixelSize": 150},
                "fields": "pixelSize"
            }},
            # X-AI (AI + notes)
            {"updateDimensionProperties": {
                "range": {"sheetId": sheet_id, "dimension": "COLUMNS", "startIndex": 23, "endIndex": total_cols},
                "properties": {"pixelSize": 220},
                "fields": "pixelSize"
            }},
        ]
        ws.spreadsheet.batch_update({"requests": requests})

        # Number formats (best-effort; apply on large ranges)
        # Prices
        ws.format("G2:N", {"numberFormat": {"type": "NUMBER", "pattern": "#,##0.00"}})
        # Percent columns (J, U, Z, AB)
        ws.format("J2:J", {"numberFormat": {"type": "PERCENT", "pattern": "0.00%"}})
        ws.format("U2:U", {"numberFormat": {"type": "PERCENT", "pattern": "0.00%"}})
        ws.format("Z2:Z", {"numberFormat": {"type": "PERCENT", "pattern": "0.00%"}})
        ws.format("AB2:AB", {"numberFormat": {"type": "PERCENT", "pattern": "0.00%"}})
        # Vol/Drawdown (W, X) percent
        ws.format("W2:X", {"numberFormat": {"type": "PERCENT", "pattern": "0.00%"}})
        # Market cap / shares / volume
        ws.format("O2:Q", {"numberFormat": {"type": "NUMBER", "pattern": "#,##0"}})
        ws.format("P2:P", {"numberFormat": {"type": "NUMBER", "pattern": "#,##0"}})

    except Exception as e:
        log.warning("Formatting skipped (non-fatal): %s", e)


def apply_formatting_top(ws: gspread.Worksheet, total_cols: int) -> None:
    try:
        last_col_letter = gspread.utils.rowcol_to_a1(1, total_cols).split("1")[0]
        header_range = f"A1:{last_col_letter}1"
        ws.format(header_range, {
            "textFormat": {"bold": True},
            "horizontalAlignment": "CENTER",
            "verticalAlignment": "MIDDLE",
            "wrapStrategy": "WRAP",
            "backgroundColor": {"red": 0.00, "green": 0.23, "blue": 0.40},
            "foregroundColor": {"red": 1, "green": 1, "blue": 1},
        })
        try:
            ws.freeze(rows=1)
        except Exception:
            pass
        try:
            ws.set_basic_filter()
        except Exception:
            pass
        ws.format("E2:F", {"numberFormat": {"type": "NUMBER", "pattern": "#,##0.00"}})
        ws.format("G2:G", {"numberFormat": {"type": "PERCENT", "pattern": "0.00%"}})
    except Exception as e:
        log.warning("Top sheet formatting skipped (non-fatal): %s", e)


# =============================================================================
# Routes
# =============================================================================

@app.route("/")
def home():
    return f"Emad Bahbah Financial Engine is Running. v{APP_VERSION}"


@app.route("/api/health", methods=["GET"])
def health():
    return jsonify({
        "status": "ok",
        "version": APP_VERSION,
        "time_utc": now_utc_str()
    })


@app.route("/api/setup", methods=["POST"])
def setup_sheet():
    """
    Ensures required worksheets exist + headers + formatting.
    Body:
      {
        "sheet_url": "...",
        "market_sheet_name": "Market Data",
        "top_sheet_name": "Top 7 Opportunities"
      }
    """
    try:
        payload = request.get_json(silent=True) or {}
        sheet_url = payload.get("sheet_url")
        if not sheet_url:
            return jsonify({"status": "error", "message": "sheet_url is required"}), 400

        market_sheet_name = payload.get("market_sheet_name", "Market Data")
        top_sheet_name = payload.get("top_sheet_name", "Top 7 Opportunities")

        gc = get_google_client()
        sh = gc.open_by_url(sheet_url)

        ws_market = ensure_worksheet(sh, market_sheet_name, rows=4000, cols=max(40, len(MARKET_HEADERS) + 2))
        ensure_headers(ws_market, MARKET_HEADERS)
        apply_formatting_market(ws_market, total_cols=len(MARKET_HEADERS))

        ws_top = ensure_worksheet(sh, top_sheet_name, rows=200, cols=max(20, len(TOP_HEADERS) + 2))
        ensure_headers(ws_top, TOP_HEADERS)
        apply_formatting_top(ws_top, total_cols=len(TOP_HEADERS))

        return jsonify({
            "status": "success",
            "message": "Setup complete",
            "market_sheet": market_sheet_name,
            "top_sheet": top_sheet_name,
            "headers_market_cols": len(MARKET_HEADERS),
            "headers_top_cols": len(TOP_HEADERS)
        })

    except Exception as e:
        log.exception("Setup error")
        return jsonify({"status": "error", "message": str(e)}), 500


@app.route("/api/analyze", methods=["POST"])
def analyze_portfolio():
    """
    Reads tickers from Market Data col A (starting row 2),
    computes enriched + AI + risk metrics, writes full table.

    Body example:
    {
      "sheet_url": "https://docs.google.com/spreadsheets/d/....",
      "market_sheet_name": "Market Data",
      "top_sheet_name": "Top 7 Opportunities",
      "apply_formatting": true,
      "max_tickers": 500
    }
    """
    try:
        payload = request.get_json(silent=True) or {}
        sheet_url = payload.get("sheet_url")
        if not sheet_url:
            return jsonify({"status": "error", "message": "sheet_url is required"}), 400

        market_sheet_name = payload.get("market_sheet_name", "Market Data")
        top_sheet_name = payload.get("top_sheet_name", "Top 7 Opportunities")
        apply_fmt = bool(payload.get("apply_formatting", True))
        max_tickers = int(payload.get("max_tickers", 1000))

        gc = get_google_client()
        sh = gc.open_by_url(sheet_url)

        # Ensure sheets + headers (safe to call always)
        ws_market = ensure_worksheet(sh, market_sheet_name, rows=4000, cols=max(40, len(MARKET_HEADERS) + 2))
        ws_top = ensure_worksheet(sh, top_sheet_name, rows=200, cols=max(20, len(TOP_HEADERS) + 2))

        ensure_headers(ws_market, MARKET_HEADERS)
        ensure_headers(ws_top, TOP_HEADERS)

        if apply_fmt:
            apply_formatting_market(ws_market, total_cols=len(MARKET_HEADERS))
            apply_formatting_top(ws_top, total_cols=len(TOP_HEADERS))

        # Read tickers from col A (skip header)
        raw = ws_market.col_values(1)[1:]
        tickers: List[str] = []
        seen = set()
        for t in raw:
            tt = normalize_ticker(t)
            if not tt:
                continue
            if tt in seen:
                continue
            tickers.append(tt)
            seen.add(tt)
            if len(tickers) >= max_tickers:
                break

        log.info("Analyzing %s tickers...", len(tickers))

        rows: List[List[Any]] = []
        picks: List[Dict[str, Any]] = []

        for ticker in tickers:
            try:
                fund, hist = engine.get_stock_data(ticker)
                ai = engine.run_ai_forecasting(hist)
                shariah = engine.check_shariah_compliance(fund)
                score_raw = engine.generate_score(fund, ai)

                score = safe_float(score_raw)
                market = (fund or {}).get("market") or infer_market_from_ticker(ticker)

                general = (fund or {}).get("General", {}) if isinstance((fund or {}).get("General", {}), dict) else {}
                highlights = (fund or {}).get("Highlights", {}) if isinstance((fund or {}).get("Highlights", {}), dict) else {}
                valuation = (fund or {}).get("Valuation", {}) if isinstance((fund or {}).get("Valuation", {}), dict) else {}
                shares = (fund or {}).get("SharesStats", {}) if isinstance((fund or {}).get("SharesStats", {}), dict) else {}
                tech = (fund or {}).get("Technicals", {}) if isinstance((fund or {}).get("Technicals", {}), dict) else {}

                # Current price from AI output first, fallback to technicals/highlights if present
                current_price = safe_float(ai.get("current_price")) if isinstance(ai, dict) else None
                if current_price is None:
                    current_price = safe_float(tech.get("Price")) or safe_float(highlights.get("MarketCapitalization"))

                pred30 = safe_float(ai.get("predicted_price_30d")) if isinstance(ai, dict) else None
                roi30 = safe_float(ai.get("expected_roi_pct")) if isinstance(ai, dict) else None

                # Optional 90D (if your FinanceEngine supports it)
                pred90 = safe_float(ai.get("predicted_price_90d")) if isinstance(ai, dict) else None
                roi90 = safe_float(ai.get("expected_roi_90d_pct")) if isinstance(ai, dict) else None
                conf = safe_float(ai.get("confidence")) if isinstance(ai, dict) else safe_float(ai.get("confidence_score")) if isinstance(ai, dict) else None

                vol_ann, max_dd = compute_risk_metrics(hist)

                compliant = bool((shariah or {}).get("compliant", False))
                score_for_rank = score if score is not None else 0.0
                roi_for_rank = roi30 if roi30 is not None else -9999.0

                recommendation = build_ai_recommendation(score, roi30, vol_ann)
                summary = build_ai_summary(roi30, pred30, vol_ann, max_dd, conf, recommendation)

                # Build row aligned with MARKET_HEADERS
                row = [
                    ticker,
                    general.get("Name") or general.get("CompanyName") or (fund or {}).get("name") or "N/A",
                    market,
                    general.get("CurrencyCode") or (fund or {}).get("currency") or "N/A",
                    general.get("Sector") or "N/A",
                    general.get("Industry") or "N/A",
                    safe_float(ai.get("current_price")) if isinstance(ai, dict) else None,
                    safe_float(ai.get("previous_close")) if isinstance(ai, dict) else safe_float(tech.get("PreviousClose")),
                    safe_float(ai.get("price_change")) if isinstance(ai, dict) else safe_float(tech.get("Change")),
                    (safe_float(ai.get("percent_change")) / 100.0) if (isinstance(ai, dict) and safe_float(ai.get("percent_change")) is not None) else None,
                    safe_float(ai.get("day_high")) if isinstance(ai, dict) else safe_float(tech.get("DayHigh")),
                    safe_float(ai.get("day_low")) if isinstance(ai, dict) else safe_float(tech.get("DayLow")),
                    safe_float(ai.get("high_52w")) if isinstance(ai, dict) else safe_float(tech.get("52WeekHigh")),
                    safe_float(ai.get("low_52w")) if isinstance(ai, dict) else safe_float(tech.get("52WeekLow")),
                    safe_float(ai.get("volume")) if isinstance(ai, dict) else safe_float(tech.get("Volume")),
                    safe_float(ai.get("market_cap")) if isinstance(ai, dict) else safe_float(highlights.get("MarketCapitalization")),
                    safe_float(shares.get("SharesOutstanding")) or safe_float(ai.get("shares_outstanding")) if isinstance(ai, dict) else None,
                    safe_float(highlights.get("EarningsShare")) or safe_float(ai.get("eps_ttm")) if isinstance(ai, dict) else None,
                    safe_float(valuation.get("TrailingPE")) or safe_float(ai.get("pe_ttm")) if isinstance(ai, dict) else None,
                    safe_float(valuation.get("PriceBookMRQ")) or safe_float(ai.get("pb")) if isinstance(ai, dict) else None,
                    (safe_float(highlights.get("DividendYield")) / 100.0) if safe_float(highlights.get("DividendYield")) is not None else None,
                    safe_float(tech.get("Beta")) or safe_float(ai.get("beta")) if isinstance(ai, dict) else None,
                    vol_ann,
                    max_dd,
                    pred30,
                    (roi30 / 100.0) if roi30 is not None else None,
                    pred90,
                    (roi90 / 100.0) if roi90 is not None else None,
                    conf,
                    "YES" if compliant else "NO",
                    score,
                    None,  # Rank (filled later)
                    recommendation,
                    summary,
                    now_utc_str(),
                    (ai.get("data_source") if isinstance(ai, dict) else None) or (fund or {}).get("data_source") or "FinanceEngine",
                    None,  # Data Quality (filled later)
                ]

                # fill Data Quality
                row[-1] = data_quality_label(row)

                rows.append(row)

                # top picks candidates
                if compliant and score_for_rank >= 60:
                    picks.append({
                        "ticker": ticker,
                        "name": row[1],
                        "sector": row[4],
                        "current_price": row[6],
                        "pred30": pred30,
                        "roi30": roi30,
                        "score": score_for_rank,
                        "recommendation": recommendation,
                        "updated_at": row[-3],
                        "_roi_rank": roi_for_rank,
                        "_score_rank": score_for_rank,
                    })

            except Exception as inner:
                log.warning("Ticker failed (%s): %s", ticker, inner)
                # Still write a row so you can see failures inside the sheet
                err_row = [
                    ticker, "N/A", infer_market_from_ticker(ticker), "N/A", "N/A", "N/A",
                    None, None, None, None, None, None, None, None, None,
                    None, None, None, None, None, None, None,
                    None, None,
                    None, None, None, None, None,
                    "NO", None, None, "WATCH", f"ERROR: {inner}",
                    now_utc_str(), "FinanceEngine", "POOR"
                ]
                rows.append(err_row)

        # Ranking (Score primary, ROI secondary)
        def sort_key(r: List[Any]) -> Tuple[float, float]:
            score = safe_float(r[30]) or 0.0
            roi30_frac = safe_float(r[26]) or -9999.0  # stored as fraction
            # convert fraction to pct for comparison (still monotonic)
            roi30_pct = roi30_frac * 100.0 if roi30_frac > -1000 else -9999.0
            return (score, roi30_pct)

        rows_sorted = sorted(rows, key=sort_key, reverse=True)

        # assign rank
        for i, r in enumerate(rows_sorted, start=1):
            r[31] = i  # Rank column index

        # Write back to Market Data (start A2)
        if rows_sorted:
            end_row = 1 + len(rows_sorted)
            rng = f"A2:{gspread.utils.rowcol_to_a1(2, len(MARKET_HEADERS)).split('2')[0]}{end_row}"
            # update expects 2D list sized to headers
            ws_market.update(values=rows_sorted, range_name=f"A2:{gspread.utils.rowcol_to_a1(end_row, len(MARKET_HEADERS)).split(str(end_row))[0]}{end_row}")

            # Clear extra old rows below (optional)
            try:
                last_existing = ws_market.get_all_values()
                existing_rows = max(1, len(last_existing))
                if existing_rows > end_row:
                    ws_market.batch_clear([f"A{end_row+1}:{gspread.utils.rowcol_to_a1(existing_rows, len(MARKET_HEADERS)).split(str(existing_rows))[0]}{existing_rows}"])
            except Exception:
                pass

        # Build Top 7 Opportunities sheet
        picks.sort(key=lambda x: (x["_score_rank"], x["_roi_rank"]), reverse=True)
        top7 = picks[:7]

        top_rows: List[List[Any]] = []
        for idx, p in enumerate(top7, start=1):
            top_rows.append([
                idx,
                p["ticker"],
                p["name"],
                p["sector"],
                p["current_price"],
                p["pred30"],
                (p["roi30"] / 100.0) if p["roi30"] is not None else None,
                p["score"],
                p["recommendation"],
                "YES",
                p["updated_at"],
            ])

        # Write Top sheet (A2...) and clear below
        ws_top.update(values=[TOP_HEADERS], range_name="A1")
        if top_rows:
            ws_top.update(values=top_rows, range_name=f"A2:K{1+len(top_rows)}")
        try:
            ws_top.batch_clear([f"A{2+len(top_rows)}:K200"])
        except Exception:
            pass

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
                    "score": round(p["score"], 2),
                    "roi_30d_pct": p["roi30"],
                    "recommendation": p["recommendation"]
                }
                for i, p in enumerate(top7)
            ],
        })

    except Exception as e:
        log.exception("Analyze error")
        return jsonify({"status": "error", "message": str(e)}), 500


# =============================================================================
# Entrypoint
# =============================================================================

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 10000))
    app.run(host="0.0.0.0", port=port)
