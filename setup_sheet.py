# setup_sheet.py
"""
Google Sheet Builder (Aligned with app.py + FinanceEngine v2.0.0)
-----------------------------------------------------------------
FULL UPDATED SCRIPT (ONE-SHOT) – v2.0.0

Creates/updates these tabs (idempotent):
1) Market Data                (enhanced headers + formatting + conditional formats)
2) Top 7 Opportunities        (auto-populated by /api/analyze, but we build/format it here)
3) My Investment              (portfolio ledger + P/L + formatting)

Notes:
- Uses Verdana everywhere.
- Rebuilds headers to match the app.py schema.
- Applies:
  * header style + freeze row + filter
  * column widths + number formats
  * conditional formatting for ROI / P&L / Score

ENV:
- GOOGLE_CREDENTIALS  (service account JSON string)
Optional CLI:
- SHEET_URL           (if you want to run this file directly)
"""

from __future__ import annotations

import os
import json
from typing import List, Optional

import gspread
from google.oauth2.service_account import Credentials

# gspread-formatting
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

SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]

DEFAULT_MARKET_SHEET = "Market Data"
DEFAULT_TOP_SHEET = "Top 7 Opportunities"
DEFAULT_PORTFOLIO_SHEET = "My Investment"

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

# Portfolio / investment ledger (works well with your goal “income-statement style”)
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
    if isinstance(d.get("private_key"), str) and "\\n" in d["private_key"]:
        d["private_key"] = d["private_key"].replace("\\n", "\n")
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


def _col_letter(n: int) -> str:
    # 1-based col -> A1 notation letter
    import gspread.utils
    return gspread.utils.rowcol_to_a1(1, n).split("1")[0]


# =============================================================================
# Sheet Ops
# =============================================================================

def _ensure_ws(sh: gspread.Spreadsheet, name: str, rows: int, cols: int) -> gspread.Worksheet:
    try:
        ws = sh.worksheet(name)
        return ws
    except Exception:
        return sh.add_worksheet(title=name, rows=rows, cols=cols)


def _write_headers(ws: gspread.Worksheet, headers: List[str]) -> None:
    ws.update(values=[headers], range_name="A1")


def _apply_common(ws: gspread.Worksheet, header_cols: int, header_format: cellFormat) -> None:
    last_col = _col_letter(header_cols)
    format_cell_range(ws, f"A1:{last_col}1", header_format)
    format_cell_range(ws, f"A2:{last_col}", _fmt_body())

    # Freeze header row + filter
    try:
        set_frozen(ws, rows=1)
    except Exception:
        pass
    try:
        set_basic_filter(ws, f"A1:{last_col}1")
    except Exception:
        pass


def _apply_market_formats(ws: gspread.Worksheet) -> None:
    # Column widths (tuned for dashboard readability)
    # Identity
    for c in range(1, 7):   # A-F
        set_column_width(ws, _col_letter(c), 150)
    # Prices/trading
    for c in range(7, 16):  # G-O
        set_column_width(ws, _col_letter(c), 130)
    # Fundamentals/risk
    for c in range(16, 25): # P-X
        set_column_width(ws, _col_letter(c), 150)
    # AI + text
    for c in range(25, 38): # Y-AK-ish (depends)
        set_column_width(ws, _col_letter(c), 220)

    # Number formats (best-effort)
    # Prices: G, H, I, K, L, M, N, Y, AA, etc.
    price_cols = ["G", "H", "I", "K", "L", "M", "N", "Y", "AA"]
    for col in price_cols:
        format_cell_range(ws, f"{col}2:{col}", cellFormat(numberFormat=numberFormat("NUMBER", "#,##0.00")))

    # Integers: Volume, Market Cap, Shares
    int_cols = ["O", "P", "Q"]
    for col in int_cols:
        format_cell_range(ws, f"{col}2:{col}", cellFormat(numberFormat=numberFormat("NUMBER", "#,##0")))

    # Percent columns (stored as fractions in app.py: 0.05 => 5%)
    pct_cols = ["J", "U", "W", "X", "Z", "AB"]
    for col in pct_cols:
        format_cell_range(ws, f"{col}2:{col}", cellFormat(numberFormat=numberFormat("PERCENT", "0.00%")))

    # Score / Confidence / Rank
    format_cell_range(ws, "AC2:AC", cellFormat(numberFormat=numberFormat("NUMBER", "0")))  # confidence
    format_cell_range(ws, "AE2:AE", cellFormat(numberFormat=numberFormat("NUMBER", "0.0")))  # score
    format_cell_range(ws, "AF2:AF", cellFormat(numberFormat=numberFormat("NUMBER", "0")))  # rank

    # Conditional formatting rules (ROI & Score & Drawdown)
    rules: List[ConditionalFormatRule] = []

    # Helper to create GridRange for full column (starting row 2)
    def col_range(col_index_1based: int) -> GridRange:
        return GridRange(
            sheetId=ws.id,
            startRowIndex=1,     # row 2 (0-based)
            endRowIndex=4000,    # reasonable
            startColumnIndex=col_index_1based - 1,
            endColumnIndex=col_index_1based,
        )

    # ROI30 (Z) positive/negative
    # Z is 26th col? Let's map by headers to be safe:
    header_to_index = {h: i + 1 for i, h in enumerate(MARKET_HEADERS)}  # 1-based
    roi30_idx = header_to_index.get("AI Expected ROI 30D %")
    roi90_idx = header_to_index.get("AI Expected ROI 90D %")
    score_idx = header_to_index.get("Score (0-100)")
    dd_idx = header_to_index.get("Max Drawdown 90D")

    def add_pos_neg_rules(col_idx: int):
        rules.append(
            ConditionalFormatRule(
                ranges=[col_range(col_idx)],
                booleanRule=BooleanRule(
                    condition=BooleanCondition("NUMBER_GREATER", ["0"]),
                    format=_fmt_positive(),
                ),
            )
        )
        rules.append(
            ConditionalFormatRule(
                ranges=[col_range(col_idx)],
                booleanRule=BooleanRule(
                    condition=BooleanCondition("NUMBER_LESS", ["0"]),
                    format=_fmt_negative(),
                ),
            )
        )

    if roi30_idx:
        add_pos_neg_rules(roi30_idx)
    if roi90_idx:
        add_pos_neg_rules(roi90_idx)

    # Drawdown (more negative = worse): highlight <= -0.20 in red, >= -0.10 neutral
    if dd_idx:
        rules.append(
            ConditionalFormatRule(
                ranges=[col_range(dd_idx)],
                booleanRule=BooleanRule(
                    condition=BooleanCondition("NUMBER_LESS_EQUAL", ["-0.20"]),
                    format=_fmt_negative(),
                ),
            )
        )
        rules.append(
            ConditionalFormatRule(
                ranges=[col_range(dd_idx)],
                booleanRule=BooleanRule(
                    condition=BooleanCondition("NUMBER_GREATER_EQUAL", ["-0.10"]),
                    format=_fmt_neutral(),
                ),
            )
        )

    # Score: >=80 green, <55 red
    if score_idx:
        rules.append(
            ConditionalFormatRule(
                ranges=[col_range(score_idx)],
                booleanRule=BooleanRule(
                    condition=BooleanCondition("NUMBER_GREATER_EQUAL", ["80"]),
                    format=_fmt_positive(),
                ),
            )
        )
        rules.append(
            ConditionalFormatRule(
                ranges=[col_range(score_idx)],
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
            # Non-fatal: formatting is best-effort
            pass


def _apply_top_formats(ws: gspread.Worksheet) -> None:
    # widths
    set_column_width(ws, "A", 60)
    set_column_width(ws, "B", 120)
    set_column_width(ws, "C", 200)
    set_column_width(ws, "D", 180)
    set_column_width(ws, "E", 130)
    set_column_width(ws, "F", 160)
    set_column_width(ws, "G", 160)
    set_column_width(ws, "H", 120)
    set_column_width(ws, "I", 160)
    set_column_width(ws, "J", 140)
    set_column_width(ws, "K", 170)

    # number formats
    format_cell_range(ws, "E2:F", cellFormat(numberFormat=numberFormat("NUMBER", "#,##0.00")))
    format_cell_range(ws, "G2:G", cellFormat(numberFormat=numberFormat("PERCENT", "0.00%")))
    format_cell_range(ws, "H2:H", cellFormat(numberFormat=numberFormat("NUMBER", "0.0")))

    # conditional: ROI (G)
    rules = [
        ConditionalFormatRule(
            ranges=[GridRange(sheetId=ws.id, startRowIndex=1, endRowIndex=300, startColumnIndex=6, endColumnIndex=7)],
            booleanRule=BooleanRule(
                condition=BooleanCondition("NUMBER_GREATER", ["0"]),
                format=_fmt_positive(),
            ),
        ),
        ConditionalFormatRule(
            ranges=[GridRange(sheetId=ws.id, startRowIndex=1, endRowIndex=300, startColumnIndex=6, endColumnIndex=7)],
            booleanRule=BooleanRule(
                condition=BooleanCondition("NUMBER_LESS", ["0"]),
                format=_fmt_negative(),
            ),
        ),
    ]
    try:
        set_conditional_formatting(ws, rules)
    except Exception:
        pass


def _apply_portfolio_formats(ws: gspread.Worksheet) -> None:
    # widths
    widths = {
        "A": 120, "B": 110, "C": 110, "D": 90,
        "E": 130, "F": 120, "G": 140, "H": 140,
        "I": 140, "J": 120, "K": 160, "L": 220, "M": 170
    }
    for col, w in widths.items():
        set_column_width(ws, col, w)

    # Number formats
    format_cell_range(ws, "C2:C", cellFormat(numberFormat=numberFormat("NUMBER", "#,##0.00")))  # buy price
    format_cell_range(ws, "D2:D", cellFormat(numberFormat=numberFormat("NUMBER", "#,##0.####")))  # qty
    format_cell_range(ws, "E2:H", cellFormat(numberFormat=numberFormat("NUMBER", "#,##0.00")))  # totals + PL
    format_cell_range(ws, "F2:G", cellFormat(numberFormat=numberFormat("NUMBER", "#,##0.00")))  # current price/value
    format_cell_range(ws, "I2:I", cellFormat(numberFormat=numberFormat("PERCENT", "0.00%")))     # PL %

    # Conditional P/L (H, I, J)
    rules = []
    # H = Unrealized P/L (col 8 index 7 zero-based startColumnIndex=7 end=8)
    for col_start in [7, 8, 9]:  # H, I, J
        rules.append(
            ConditionalFormatRule(
                ranges=[GridRange(sheetId=ws.id, startRowIndex=1, endRowIndex=1000, startColumnIndex=col_start, endColumnIndex=col_start + 1)],
                booleanRule=BooleanRule(
                    condition=BooleanCondition("NUMBER_GREATER", ["0"]),
                    format=_fmt_positive(),
                ),
            )
        )
        rules.append(
            ConditionalFormatRule(
                ranges=[GridRange(sheetId=ws.id, startRowIndex=1, endRowIndex=1000, startColumnIndex=col_start, endColumnIndex=col_start + 1)],
                booleanRule=BooleanRule(
                    condition=BooleanCondition("NUMBER_LESS", ["0"]),
                    format=_fmt_negative(),
                ),
            )
        )
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
    ws_market = _ensure_ws(sh, market_sheet_name, rows=4000, cols=max(40, len(MARKET_HEADERS) + 5))
    _write_headers(ws_market, MARKET_HEADERS)
    _apply_common(ws_market, header_cols=len(MARKET_HEADERS), header_format=_fmt_header_dark())
    _apply_market_formats(ws_market)

    # --- Top 7 ---
    ws_top = _ensure_ws(sh, top_sheet_name, rows=300, cols=max(20, len(TOP_HEADERS) + 5))
    _write_headers(ws_top, TOP_HEADERS)
    _apply_common(ws_top, header_cols=len(TOP_HEADERS), header_format=_fmt_header_blue())
    _apply_top_formats(ws_top)

    # --- My Investment ---
    ws_port = _ensure_ws(sh, portfolio_sheet_name, rows=1000, cols=max(20, len(PORTFOLIO_HEADERS) + 5))
    _write_headers(ws_port, PORTFOLIO_HEADERS)
    _apply_common(ws_port, header_cols=len(PORTFOLIO_HEADERS), header_format=_fmt_header_dark())
    _apply_portfolio_formats(ws_port)

    print("✅ Sheet structure built/updated successfully (Aligned with app.py v2.0.0).")


if __name__ == "__main__":
    sheet_url = os.environ.get("SHEET_URL", "").strip()
    if sheet_url:
        setup_google_sheet(sheet_url)
    else:
        print("Set SHEET_URL env var to run setup_sheet.py directly.")
