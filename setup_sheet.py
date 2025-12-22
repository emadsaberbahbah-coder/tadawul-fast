# setup_sheet.py
"""
Google Sheet Builder (Aligned with app.py + FinanceEngine v2.1.x)
-----------------------------------------------------------------
FULL UPDATED SCRIPT (ONE-SHOT) – v2.1.1

Creates/updates these tabs (idempotent):
1) Market Data
2) Top 7 Opportunities
3) My Investment

Key fixes vs your current file:
✅ FIXED invalid A1 ranges (was using "A2:AL" / "E2:F" style ranges → can break formatting)
✅ Dynamic column detection by header name (no hard-coded letters)
✅ Resizes sheets safely (rows/cols) + freezes row 1 and col 1
✅ Cleaner number formats + conditional formats (ROI/Score/Drawdown/P&L)
✅ Best-effort formatting (never crash on formatting failures)

ENV:
- GOOGLE_CREDENTIALS  (service account JSON string)
Optional:
- SHEET_URL           (if you want to run this file directly)
"""

from __future__ import annotations

import os
import json
from typing import Any, Dict, List, Optional, Tuple

import gspread
from google.oauth2.service_account import Credentials

from gspread_formatting import (
    cellFormat,
    textFormat,
    color,
    format_cell_range,
    set_frozen,
    set_basic_filter,
    set_column_width,
    numberFormat,
    ConditionalFormatRule,
    BooleanRule,
    BooleanCondition,
    GridRange,
    set_conditional_formatting,
)


# =============================================================================
# CONFIG
# =============================================================================

VERSION = "2.1.1"

SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]

DEFAULT_MARKET_SHEET = "Market Data"
DEFAULT_TOP_SHEET = "Top 7 Opportunities"
DEFAULT_PORTFOLIO_SHEET = "My Investment"

DEFAULT_ROWS = {
    DEFAULT_MARKET_SHEET: 4000,
    DEFAULT_TOP_SHEET: 300,
    DEFAULT_PORTFOLIO_SHEET: 1000,
}

DEFAULT_COL_PADDING = 6  # extra blank columns for future expansion

# Must match app.py headers (MARKET_HEADERS)
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

# Must match app.py headers (TOP_HEADERS)
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

# Must match portfolio_engine.py (PORTFOLIO_HEADERS)
PORTFOLIO_HEADERS: List[str] = [
    "Ticker",
    "Buy Date",
    "Buy Price",
    "Quantity",
    "Total Cost",
    "Current Price",
    "Current Value",
    "Unrealized P/L",
    "Unrealized P/L %",
    "Realized P/L",
    "Status (Active/Sold)",
    "Notes",
    "Updated At (UTC)",
]


# =============================================================================
# Auth Helpers
# =============================================================================

def _load_creds_dict() -> dict:
    creds_json = os.environ.get("GOOGLE_CREDENTIALS")
    if not creds_json:
        raise ValueError("Missing GOOGLE_CREDENTIALS env var")

    d = json.loads(creds_json)
    pk = d.get("private_key")
    if isinstance(pk, str) and "\\n" in pk:
        d["private_key"] = pk.replace("\\n", "\n")
    return d


def _get_client() -> gspread.Client:
    creds_dict = _load_creds_dict()
    creds = Credentials.from_service_account_info(creds_dict, scopes=SCOPES)
    return gspread.authorize(creds)


# =============================================================================
# Formatting Builders
# =============================================================================

def _fmt_header_dark() -> cellFormat:
    return cellFormat(
        textFormat=textFormat(bold=True, fontFamily="Verdana", fontSize=10, foregroundColor=color(1, 1, 1)),
        backgroundColor=color(0.12, 0.12, 0.12),
        horizontalAlignment="CENTER",
        verticalAlignment="MIDDLE",
        wrapStrategy="WRAP",
    )


def _fmt_header_blue() -> cellFormat:
    return cellFormat(
        textFormat=textFormat(bold=True, fontFamily="Verdana", fontSize=10, foregroundColor=color(1, 1, 1)),
        backgroundColor=color(0.00, 0.23, 0.40),
        horizontalAlignment="CENTER",
        verticalAlignment="MIDDLE",
        wrapStrategy="WRAP",
    )


def _fmt_body() -> cellFormat:
    return cellFormat(
        textFormat=textFormat(fontFamily="Verdana", fontSize=9),
        verticalAlignment="MIDDLE",
        wrapStrategy="OVERFLOW_CELL",
    )


def _fmt_positive() -> cellFormat:
    return cellFormat(
        textFormat=textFormat(fontFamily="Verdana", foregroundColor=color(0, 0.45, 0), bold=True),
        backgroundColor=color(0.90, 1.00, 0.90),
    )


def _fmt_negative() -> cellFormat:
    return cellFormat(
        textFormat=textFormat(fontFamily="Verdana", foregroundColor=color(0.75, 0, 0), bold=True),
        backgroundColor=color(1.00, 0.90, 0.90),
    )


def _fmt_neutral() -> cellFormat:
    return cellFormat(
        textFormat=textFormat(fontFamily="Verdana", foregroundColor=color(0.2, 0.2, 0.2)),
        backgroundColor=color(0.96, 0.96, 0.96),
    )


# =============================================================================
# A1 Helpers (FIX: always produce valid A1 ranges)
# =============================================================================

def _a1(row: int, col: int) -> str:
    return gspread.utils.rowcol_to_a1(row, col)


def _a1_range(r1: int, c1: int, r2: int, c2: int) -> str:
    return f"{_a1(r1, c1)}:{_a1(r2, c2)}"


def _header_index_map(headers: List[str]) -> Dict[str, int]:
    # 1-based index
    return {h: i + 1 for i, h in enumerate(headers)}


def _grid_range_col(ws: gspread.Worksheet, col_1based: int, start_row_1based: int, end_row_1based: int) -> GridRange:
    # GridRange uses 0-based indices; end indices are exclusive
    return GridRange(
        sheetId=ws.id,
        startRowIndex=max(0, start_row_1based - 1),
        endRowIndex=max(1, end_row_1based),
        startColumnIndex=max(0, col_1based - 1),
        endColumnIndex=max(1, col_1based),
    )


# =============================================================================
# Sheet Ops
# =============================================================================

def _ensure_ws(sh: gspread.Spreadsheet, name: str, rows: int, cols: int) -> gspread.Worksheet:
    try:
        ws = sh.worksheet(name)
    except Exception:
        ws = sh.add_worksheet(title=name, rows=rows, cols=cols)

    # Resize to be safe / idempotent
    try:
        if ws.row_count < rows or ws.col_count < cols:
            ws.resize(rows=max(ws.row_count, rows), cols=max(ws.col_count, cols))
    except Exception:
        pass

    return ws


def _write_headers(ws: gspread.Worksheet, headers: List[str]) -> None:
    # Only rewrite if different (to preserve API quota & user edits below header)
    try:
        existing = ws.row_values(1)
        if existing[: len(headers)] == headers:
            return
    except Exception:
        pass
    ws.update(values=[headers], range_name=_a1_range(1, 1, 1, len(headers)))


def _apply_common(ws: gspread.Worksheet, headers: List[str], header_format: cellFormat, data_rows: int) -> None:
    cols = len(headers)
    # Header style
    try:
        format_cell_range(ws, _a1_range(1, 1, 1, cols), header_format)
    except Exception:
        pass

    # Body style (row 2 -> data_rows)
    try:
        format_cell_range(ws, _a1_range(2, 1, data_rows, cols), _fmt_body())
    except Exception:
        pass

    # Freeze header row + first col
    try:
        set_frozen(ws, rows=1, cols=1)
    except Exception:
        pass

    # Filter on header row (full width)
    try:
        set_basic_filter(ws, _a1_range(1, 1, 1, cols))
    except Exception:
        pass


# =============================================================================
# Formatting per sheet
# =============================================================================

def _apply_widths_market(ws: gspread.Worksheet, headers: List[str]) -> None:
    h = _header_index_map(headers)

    def w(col_name: str, width: int) -> None:
        idx = h.get(col_name)
        if not idx:
            return
        try:
            col_letter = gspread.utils.rowcol_to_a1(1, idx).split("1")[0]
            set_column_width(ws, col_letter, width)
        except Exception:
            pass

    # Compact identity
    for nm in ["Ticker", "Market", "Currency"]:
        w(nm, 110)
    w("Name", 220)
    w("Sector", 170)
    w("Industry", 170)

    # Prices/trading
    for nm in [
        "Current Price", "Previous Close", "Change", "Change %",
        "Day High", "Day Low", "52W High", "52W Low", "Volume",
    ]:
        w(nm, 130)

    # Fundamentals / risk
    for nm in [
        "Market Cap", "Shares Outstanding", "EPS (TTM)", "P/E (TTM)", "P/B",
        "Dividend Yield %", "Beta", "Volatility 30D (Ann.)", "Max Drawdown 90D",
    ]:
        w(nm, 150)

    # AI columns
    for nm in [
        "AI Predicted Price 30D", "AI Expected ROI 30D %",
        "AI Predicted Price 90D", "AI Expected ROI 90D %",
        "AI Confidence (0-100)", "Score (0-100)", "Rank",
        "Recommendation", "Shariah Compliant",
    ]:
        w(nm, 170)

    w("AI Summary", 420)
    w("Updated At (UTC)", 170)
    w("Data Source", 140)
    w("Data Quality", 130)


def _apply_number_formats(ws: gspread.Worksheet, headers: List[str], data_rows: int) -> None:
    h = _header_index_map(headers)
    last_row = data_rows

    def fmt(col_name: str, nf: numberFormat) -> None:
        idx = h.get(col_name)
        if not idx:
            return
        try:
            rng = _a1_range(2, idx, last_row, idx)
            format_cell_range(ws, rng, cellFormat(numberFormat=nf))
        except Exception:
            pass

    NF_PRICE = numberFormat("NUMBER", "#,##0.00")
    NF_INT = numberFormat("NUMBER", "#,##0")
    NF_SCORE = numberFormat("NUMBER", "0.0")
    NF_RANK = numberFormat("NUMBER", "0")
    NF_PCT = numberFormat("PERCENT", "0.00%")

    # Prices
    for nm in [
        "Current Price", "Previous Close", "Change",
        "Day High", "Day Low", "52W High", "52W Low",
        "AI Predicted Price 30D", "AI Predicted Price 90D",
        "EPS (TTM)",
    ]:
        fmt(nm, NF_PRICE)

    # Integers / big numbers
    for nm in ["Volume", "Market Cap", "Shares Outstanding"]:
        fmt(nm, NF_INT)

    # Percent-like columns (your API writes fractions for these)
    for nm in [
        "Change %",
        "Dividend Yield %",
        "Volatility 30D (Ann.)",
        "Max Drawdown 90D",
        "AI Expected ROI 30D %",
        "AI Expected ROI 90D %",
    ]:
        fmt(nm, NF_PCT)

    # Beta
    fmt("Beta", numberFormat("NUMBER", "0.00"))

    # Score / rank / confidence
    fmt("Score (0-100)", NF_SCORE)
    fmt("Rank", NF_RANK)
    fmt("AI Confidence (0-100)", NF_RANK)


def _apply_conditional_market(ws: gspread.Worksheet, headers: List[str], data_rows: int) -> None:
    h = _header_index_map(headers)
    rules: List[ConditionalFormatRule] = []
    end_row = data_rows

    def add_pos_neg(col_name: str) -> None:
        idx = h.get(col_name)
        if not idx:
            return
        gr = _grid_range_col(ws, idx, 2, end_row)
        rules.append(
            ConditionalFormatRule(
                ranges=[gr],
                booleanRule=BooleanRule(
                    condition=BooleanCondition("NUMBER_GREATER", ["0"]),
                    format=_fmt_positive(),
                ),
            )
        )
        rules.append(
            ConditionalFormatRule(
                ranges=[gr],
                booleanRule=BooleanRule(
                    condition=BooleanCondition("NUMBER_LESS", ["0"]),
                    format=_fmt_negative(),
                ),
            )
        )

    # ROI columns
    add_pos_neg("AI Expected ROI 30D %")
    add_pos_neg("AI Expected ROI 90D %")

    # Drawdown: <= -20% red, >= -10% neutral
    dd_idx = h.get("Max Drawdown 90D")
    if dd_idx:
        gr = _grid_range_col(ws, dd_idx, 2, end_row)
        rules.append(
            ConditionalFormatRule(
                ranges=[gr],
                booleanRule=BooleanRule(
                    condition=BooleanCondition("NUMBER_LESS_EQUAL", ["-0.20"]),
                    format=_fmt_negative(),
                ),
            )
        )
        rules.append(
            ConditionalFormatRule(
                ranges=[gr],
                booleanRule=BooleanRule(
                    condition=BooleanCondition("NUMBER_GREATER_EQUAL", ["-0.10"]),
                    format=_fmt_neutral(),
                ),
            )
        )

    # Score: >=80 green, <55 red
    score_idx = h.get("Score (0-100)")
    if score_idx:
        gr = _grid_range_col(ws, score_idx, 2, end_row)
        rules.append(
            ConditionalFormatRule(
                ranges=[gr],
                booleanRule=BooleanRule(
                    condition=BooleanCondition("NUMBER_GREATER_EQUAL", ["80"]),
                    format=_fmt_positive(),
                ),
            )
        )
        rules.append(
            ConditionalFormatRule(
                ranges=[gr],
                booleanRule=BooleanRule(
                    condition=BooleanCondition("NUMBER_LESS", ["55"]),
                    format=_fmt_negative(),
                ),
            )
        )

    if rules:
        try:
            set_conditional_formatting(ws, rules)
        except Exception:
            pass


def _apply_top_formats(ws: gspread.Worksheet, headers: List[str], data_rows: int) -> None:
    h = _header_index_map(headers)

    # widths (simple + readable)
    widths = {
        "Rank": 60,
        "Ticker": 120,
        "Name": 240,
        "Sector": 180,
        "Current Price": 130,
        "AI Predicted Price 30D": 160,
        "AI Expected ROI 30D %": 160,
        "Score (0-100)": 130,
        "Recommendation": 160,
        "Shariah Compliant": 140,
        "Updated At (UTC)": 170,
    }
    for nm, w in widths.items():
        idx = h.get(nm)
        if not idx:
            continue
        try:
            col_letter = gspread.utils.rowcol_to_a1(1, idx).split("1")[0]
            set_column_width(ws, col_letter, w)
        except Exception:
            pass

    # number formats
    last_row = data_rows
    def fmt(nm: str, nf: numberFormat) -> None:
        idx = h.get(nm)
        if not idx:
            return
        try:
            format_cell_range(ws, _a1_range(2, idx, last_row, idx), cellFormat(numberFormat=nf))
        except Exception:
            pass

    fmt("Current Price", numberFormat("NUMBER", "#,##0.00"))
    fmt("AI Predicted Price 30D", numberFormat("NUMBER", "#,##0.00"))
    fmt("AI Expected ROI 30D %", numberFormat("PERCENT", "0.00%"))
    fmt("Score (0-100)", numberFormat("NUMBER", "0.0"))

    # conditional ROI
    rules: List[ConditionalFormatRule] = []
    roi_idx = h.get("AI Expected ROI 30D %")
    if roi_idx:
        gr = _grid_range_col(ws, roi_idx, 2, last_row)
        rules.append(
            ConditionalFormatRule(
                ranges=[gr],
                booleanRule=BooleanRule(
                    condition=BooleanCondition("NUMBER_GREATER", ["0"]),
                    format=_fmt_positive(),
                ),
            )
        )
        rules.append(
            ConditionalFormatRule(
                ranges=[gr],
                booleanRule=BooleanRule(
                    condition=BooleanCondition("NUMBER_LESS", ["0"]),
                    format=_fmt_negative(),
                ),
            )
        )
    if rules:
        try:
            set_conditional_formatting(ws, rules)
        except Exception:
            pass


def _apply_portfolio_formats(ws: gspread.Worksheet, headers: List[str], data_rows: int) -> None:
    h = _header_index_map(headers)

    widths = {
        "Ticker": 120,
        "Buy Date": 120,
        "Buy Price": 120,
        "Quantity": 100,
        "Total Cost": 140,
        "Current Price": 130,
        "Current Value": 150,
        "Unrealized P/L": 150,
        "Unrealized P/L %": 150,
        "Realized P/L": 130,
        "Status (Active/Sold)": 170,
        "Notes": 260,
        "Updated At (UTC)": 180,
    }
    for nm, w in widths.items():
        idx = h.get(nm)
        if not idx:
            continue
        try:
            col_letter = gspread.utils.rowcol_to_a1(1, idx).split("1")[0]
            set_column_width(ws, col_letter, w)
        except Exception:
            pass

    last_row = data_rows
    def fmt(nm: str, nf: numberFormat) -> None:
        idx = h.get(nm)
        if not idx:
            return
        try:
            format_cell_range(ws, _a1_range(2, idx, last_row, idx), cellFormat(numberFormat=nf))
        except Exception:
            pass

    fmt("Buy Price", numberFormat("NUMBER", "#,##0.00"))
    fmt("Quantity", numberFormat("NUMBER", "#,##0.####"))
    for nm in ["Total Cost", "Current Price", "Current Value", "Unrealized P/L", "Realized P/L"]:
        fmt(nm, numberFormat("NUMBER", "#,##0.00"))
    fmt("Unrealized P/L %", numberFormat("PERCENT", "0.00%"))

    # Conditional on P/L columns
    rules: List[ConditionalFormatRule] = []
    for nm in ["Unrealized P/L", "Unrealized P/L %", "Realized P/L"]:
        idx = h.get(nm)
        if not idx:
            continue
        gr = _grid_range_col(ws, idx, 2, last_row)
        rules.append(
            ConditionalFormatRule(
                ranges=[gr],
                booleanRule=BooleanRule(
                    condition=BooleanCondition("NUMBER_GREATER", ["0"]),
                    format=_fmt_positive(),
                ),
            )
        )
        rules.append(
            ConditionalFormatRule(
                ranges=[gr],
                booleanRule=BooleanRule(
                    condition=BooleanCondition("NUMBER_LESS", ["0"]),
                    format=_fmt_negative(),
                ),
            )
        )

    if rules:
        try:
            set_conditional_formatting(ws, rules)
        except Exception:
            pass


# =============================================================================
# Main Setup
# =============================================================================

def setup_google_sheet(
    sheet_url: str,
    market_sheet_name: str = DEFAULT_MARKET_SHEET,
    top_sheet_name: str = DEFAULT_TOP_SHEET,
    portfolio_sheet_name: str = DEFAULT_PORTFOLIO_SHEET,
) -> None:
    if not sheet_url:
        raise ValueError("sheet_url is required")

    gc = _get_client()
    sh = gc.open_by_url(sheet_url)

    # --- Market Data ---
    market_rows = DEFAULT_ROWS[DEFAULT_MARKET_SHEET]
    market_cols = len(MARKET_HEADERS) + DEFAULT_COL_PADDING
    ws_market = _ensure_ws(sh, market_sheet_name, rows=market_rows, cols=market_cols)
    _write_headers(ws_market, MARKET_HEADERS)
    _apply_common(ws_market, MARKET_HEADERS, _fmt_header_dark(), data_rows=market_rows)
    _apply_widths_market(ws_market, MARKET_HEADERS)
    _apply_number_formats(ws_market, MARKET_HEADERS, data_rows=market_rows)
    _apply_conditional_market(ws_market, MARKET_HEADERS, data_rows=market_rows)

    # --- Top 7 ---
    top_rows = DEFAULT_ROWS[DEFAULT_TOP_SHEET]
    top_cols = len(TOP_HEADERS) + DEFAULT_COL_PADDING
    ws_top = _ensure_ws(sh, top_sheet_name, rows=top_rows, cols=top_cols)
    _write_headers(ws_top, TOP_HEADERS)
    _apply_common(ws_top, TOP_HEADERS, _fmt_header_blue(), data_rows=top_rows)
    _apply_top_formats(ws_top, TOP_HEADERS, data_rows=top_rows)

    # --- My Investment ---
    port_rows = DEFAULT_ROWS[DEFAULT_PORTFOLIO_SHEET]
    port_cols = len(PORTFOLIO_HEADERS) + DEFAULT_COL_PADDING
    ws_port = _ensure_ws(sh, portfolio_sheet_name, rows=port_rows, cols=port_cols)
    _write_headers(ws_port, PORTFOLIO_HEADERS)
    _apply_common(ws_port, PORTFOLIO_HEADERS, _fmt_header_dark(), data_rows=port_rows)
    _apply_portfolio_formats(ws_port, PORTFOLIO_HEADERS, data_rows=port_rows)

    print(f"✅ Sheet structure built/updated successfully (setup_sheet.py v{VERSION}).")


if __name__ == "__main__":
    sheet_url = os.environ.get("SHEET_URL", "").strip()
    if sheet_url:
        setup_google_sheet(sheet_url)
    else:
        print("Set SHEET_URL env var to run setup_sheet.py directly.")
