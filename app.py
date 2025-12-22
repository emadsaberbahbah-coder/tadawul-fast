# app.py
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

APP_VERSION = "2.1.0"

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
engine = FinanceEngine(EODHD_API_KEY)


# =============================================================================
# Headers (must match setup_sheet.py)
# =============================================================================

MARKET_HEADERS: List[str] = [
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


def data_quality_label(row: List[Any]) -> str:
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
        return sh.add_worksheet(title=name, rows=rows, cols=cols)


def ensure_headers(ws: gspread.Worksheet, headers: List[str]) -> None:
    existing = ws.row_values(1)
    if existing[: len(headers)] != headers:
        ws.update(values=[headers], range_name="A1")


def infer_market_from_ticker(ticker: str) -> str:
    if ticker.endswith(".SR") or ticker.replace(".", "").isdigit():
        return "KSA"
    return "GLOBAL"


@app.route("/")
def home():
    return f"Emad Bahbah Financial Engine is Running. v{APP_VERSION}"


@app.route("/api/health", methods=["GET"])
def health():
    return jsonify({"status": "ok", "version": APP_VERSION, "time_utc": now_utc_str()})


@app.route("/api/analyze", methods=["POST"])
def analyze_portfolio():
    """
    Reads tickers from Market Data col A (row2+),
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

        gc = get_google_client()
        sh = gc.open_by_url(sheet_url)

        ws_market = ensure_worksheet(sh, market_sheet_name, rows=4000, cols=max(40, len(MARKET_HEADERS) + 2))
        ws_top = ensure_worksheet(sh, top_sheet_name, rows=300, cols=max(20, len(TOP_HEADERS) + 2))
        ws_port = None
        try:
            ws_port = ensure_worksheet(sh, portfolio_sheet_name, rows=1000, cols=max(20, len(PORTFOLIO_HEADERS) + 2))
        except Exception:
            ws_port = None

        ensure_headers(ws_market, MARKET_HEADERS)
        ensure_headers(ws_top, TOP_HEADERS)
        if ws_port:
            ensure_headers(ws_port, PORTFOLIO_HEADERS)

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
        picks: List[Dict[str, Any]] = []

        for ticker in tickers:
            try:
                fund, hist = engine.get_stock_data(ticker)
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

                # Use advanced score + recommendation/summary (more powerful)
                score = safe_float(adv.get("opportunity_score")) or safe_float(base_score) or 0.0
                recommendation = adv.get("recommendation") or "WATCH"
                ai_summary = adv.get("ai_summary") or ""

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
                market_cap = safe_float(ai.get("market_cap")) or safe_float(valuation.get("MarketCapitalization")) or safe_float(highlights.get("MarketCapitalization"))
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

                row = [
                    ticker,
                    general.get("Name") or fund.get("name") or "N/A",
                    market,
                    currency,
                    general.get("Sector") or "N/A",
                    general.get("Industry") or "N/A",
                    current_price,
                    previous_close,
                    change,
                    change_pct_frac,
                    day_high,
                    day_low,
                    high_52w,
                    low_52w,
                    volume,
                    market_cap,
                    shares_out,
                    eps,
                    pe,
                    pb,
                    divy_frac,
                    beta,
                    vol_ann,
                    dd90,
                    pred30,
                    roi30_frac,
                    pred90,
                    roi90_frac,
                    conf,
                    "YES" if compliant else "NO",
                    score,
                    None,  # Rank (filled after sorting)
                    recommendation,
                    ai_summary,
                    now_utc_str(),
                    ai.get("data_source") or (fund.get("meta", {}) or {}).get("data_source") or "FinanceEngine",
                    None,  # Data Quality
                ]

                row[-1] = data_quality_label(row)
                rows.append(row)

                if compliant and score >= 60:
                    picks.append({
                        "ticker": ticker,
                        "name": row[1],
                        "sector": row[4],
                        "current_price": current_price,
                        "pred30": pred30,
                        "roi30": roi30,
                        "score": score,
                        "recommendation": recommendation,
                        "updated_at": row[-3],
                    })

            except Exception as inner:
                log.warning("Ticker failed (%s): %s", ticker, inner)
                # Write an error row for visibility
                err_row = [""] * len(MARKET_HEADERS)
                err_row[0] = ticker
                err_row[33] = f"ERROR: {inner}"  # AI Summary column
                err_row[34] = now_utc_str()
                err_row[35] = "FinanceEngine"
                err_row[36] = "POOR"
                rows.append(err_row)

        # Sort by Score desc, then ROI30 desc
        def sort_key(r: List[Any]) -> Tuple[float, float]:
            score = safe_float(r[30]) or 0.0
            roi30_frac = safe_float(r[25])
            roi30_pct = (roi30_frac * 100.0) if roi30_frac is not None else -9999.0
            return (score, roi30_pct)

        rows_sorted = sorted(rows, key=sort_key, reverse=True)

        # Assign Rank
        for i, r in enumerate(rows_sorted, start=1):
            if len(r) > 31:
                r[31] = i

        # Write Market Data rows in one batch (A2:...)
        if rows_sorted:
            end_row = 1 + len(rows_sorted)
            end_col_letter = gspread.utils.rowcol_to_a1(1, len(MARKET_HEADERS)).split("1")[0]
            ws_market.update(values=rows_sorted, range_name=f"A2:{end_col_letter}{end_row}")

        # Build Top 7
        picks.sort(key=lambda x: (x["score"], (x["roi30"] or -9999)), reverse=True)
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
                (p["roi30"] / 100.0) if p["roi30"] is not None else None,  # fraction for percent format
                p["score"],
                p["recommendation"],
                "YES",
                p["updated_at"],
            ])

        ws_top.update(values=[TOP_HEADERS], range_name="A1")
        if top_rows:
            ws_top.update(values=top_rows, range_name=f"A2:K{1+len(top_rows)}")

        # Refresh My Investment using latest Market Data prices
        portfolio_kpi = None
        if ws_port:
            try:
                price_map = build_price_map_from_market_data(MARKET_HEADERS, rows_sorted)
                pe = PortfolioEngine()

                # read portfolio rows (data only)
                port_values = ws_port.get_all_values()
                port_rows = port_values[1:] if len(port_values) > 1 else []

                updated_port_rows, kpi = pe.compute_table(port_rows, price_map)

                # write back
                end_col_letter = gspread.utils.rowcol_to_a1(1, len(PORTFOLIO_HEADERS)).split("1")[0]
                if updated_port_rows:
                    end_row = 1 + len(updated_port_rows)
                    ws_port.update(values=updated_port_rows, range_name=f"A2:{end_col_letter}{end_row}")

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
                    "score": round(p["score"], 2),
                    "roi_30d_pct": p["roi30"],
                    "recommendation": p["recommendation"],
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

        ws_market = ensure_worksheet(sh, market_sheet_name, rows=4000, cols=max(40, len(MARKET_HEADERS) + 2))
        ws_port = ensure_worksheet(sh, portfolio_sheet_name, rows=1000, cols=max(20, len(PORTFOLIO_HEADERS) + 2))

        ensure_headers(ws_market, MARKET_HEADERS)
        ensure_headers(ws_port, PORTFOLIO_HEADERS)

        market_values = ws_market.get_all_values()
        market_rows = market_values[1:] if len(market_values) > 1 else []
        price_map = build_price_map_from_market_data(MARKET_HEADERS, market_rows)

        pe = PortfolioEngine()
        port_values = ws_port.get_all_values()
        port_rows = port_values[1:] if len(port_values) > 1 else []
        updated_port_rows, kpi = pe.compute_table(port_rows, price_map)

        end_col_letter = gspread.utils.rowcol_to_a1(1, len(PORTFOLIO_HEADERS)).split("1")[0]
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
