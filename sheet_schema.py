# sheet_schema.py
"""
Sheet Schema – Single Source of Truth (Headers + Index Maps + Column Groups)
----------------------------------------------------------------------------
FULL REPLACEMENT – v2.0.0

Purpose
- This file is the MASTER schema for your Google Sheets tabs.
- Import from here in:
    - setup_sheet.py  (to build headers and formatting)
    - app.py          (to write Market Data + Top 7 outputs aligned 100%)
    - portfolio_engine.py (to compute My Investment columns safely)

Design rules
✅ Headers are immutable (change here ONLY, then everything follows)
✅ Provides:
   - SHEET NAMES
   - HEADERS for Market Data / Top 7 / My Investment
   - Index maps (header -> col index)
   - Column groups + formatting hints (price / percent / integer / text)
✅ Safe helpers to align any row-dict or list-row to the schema
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Sequence, Tuple


# =============================================================================
# Sheet Names (tabs)
# =============================================================================

SHEET_MARKET_DATA = "Market Data"
SHEET_TOP_7 = "Top 7 Opportunities"
SHEET_PORTFOLIO = "My Investment"


# =============================================================================
# Headers (Single Source of Truth)
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
# Column Groups (UI readability + setup_sheet formatting hints)
# =============================================================================

# Market Data groups (for column width strategies / layout)
MARKET_GROUP_IDENTITY = ["Ticker", "Name", "Market", "Currency", "Sector", "Industry"]
MARKET_GROUP_PRICES = [
    "Current Price", "Previous Close", "Change", "Change %",
    "Day High", "Day Low", "52W High", "52W Low",
]
MARKET_GROUP_SIZE_LIQUIDITY = ["Volume", "Market Cap", "Shares Outstanding"]
MARKET_GROUP_FUNDAMENTALS = ["EPS (TTM)", "P/E (TTM)", "P/B", "Dividend Yield %", "Beta"]
MARKET_GROUP_RISK = ["Volatility 30D (Ann.)", "Max Drawdown 90D"]
MARKET_GROUP_AI = [
    "AI Predicted Price 30D", "AI Expected ROI 30D %",
    "AI Predicted Price 90D", "AI Expected ROI 90D %",
    "AI Confidence (0-100)", "Score (0-100)", "Rank",
    "Recommendation", "AI Summary",
]
MARKET_GROUP_META = ["Updated At (UTC)", "Data Source", "Data Quality"]

# Formatting hints (setup_sheet can import these instead of hardcoding letters)
MARKET_PRICE_COLS = [
    "Current Price", "Previous Close", "Change", "Day High", "Day Low",
    "52W High", "52W Low", "AI Predicted Price 30D", "AI Predicted Price 90D",
]
MARKET_INTEGER_COLS = ["Volume", "Market Cap", "Shares Outstanding"]
MARKET_PERCENT_COLS = ["Change %", "Dividend Yield %", "Volatility 30D (Ann.)", "Max Drawdown 90D",
                       "AI Expected ROI 30D %", "AI Expected ROI 90D %"]
MARKET_SCORE_COLS = ["AI Confidence (0-100)", "Score (0-100)", "Rank"]

TOP_PRICE_COLS = ["Current Price", "AI Predicted Price 30D"]
TOP_PERCENT_COLS = ["AI Expected ROI 30D %"]
TOP_SCORE_COLS = ["Score (0-100)", "Rank"]

PORT_PRICE_COLS = ["Buy Price", "Total Cost", "Current Price", "Current Value", "Unrealized P/L", "Realized P/L"]
PORT_PERCENT_COLS = ["Unrealized P/L %"]


# =============================================================================
# Helper core
# =============================================================================

def header_index_map(headers: Sequence[str]) -> Dict[str, int]:
    """Return {header: index} (0-based)."""
    return {h: i for i, h in enumerate(list(headers))}


def ensure_row_length(row: Sequence[Any], length: int, fill: Any = "") -> List[Any]:
    r = list(row or [])
    if len(r) < length:
        r += [fill] * (length - len(r))
    elif len(r) > length:
        r = r[:length]
    return r


def make_empty_row(headers: Sequence[str], fill: Any = "") -> List[Any]:
    return [fill for _ in headers]


def row_dict_to_row_list(headers: Sequence[str], row_dict: Dict[str, Any], fill: Any = "") -> List[Any]:
    """
    Convert {"Ticker":"AAPL.US", "Current Price": 123} into a list aligned to headers.
    Unknown keys are ignored.
    """
    out = [fill for _ in headers]
    idx = header_index_map(headers)
    for k, v in (row_dict or {}).items():
        if k in idx:
            out[idx[k]] = v
    return out


def align_row_list_to_headers(
    src_headers: Sequence[str],
    src_row: Sequence[Any],
    dst_headers: Sequence[str],
    fill: Any = "",
) -> List[Any]:
    """
    Map values by header-name from one schema to another (safe reordering).
    Useful if you changed order or have partial rows.
    """
    src_idx = header_index_map(src_headers)
    dst = [fill for _ in dst_headers]
    for j, h in enumerate(dst_headers):
        i = src_idx.get(h)
        if i is None:
            continue
        if i < len(src_row):
            dst[j] = src_row[i]
    return dst


def validate_headers(actual: Sequence[str], expected: Sequence[str]) -> Tuple[bool, List[str], List[str]]:
    """
    Returns: (ok, missing, extra)
    ok=True only if exact same set and same order.
    """
    a = list(actual or [])
    e = list(expected or [])

    missing = [h for h in e if h not in a]
    extra = [h for h in a if h not in e]
    ok = (a == e)
    return ok, missing, extra


# =============================================================================
# Schema objects (optional convenience)
# =============================================================================

@dataclass(frozen=True)
class SheetSchema:
    name: str
    headers: List[str]

    @property
    def idx(self) -> Dict[str, int]:
        return header_index_map(self.headers)

    def empty_row(self, fill: Any = "") -> List[Any]:
        return make_empty_row(self.headers, fill=fill)

    def dict_to_row(self, row_dict: Dict[str, Any], fill: Any = "") -> List[Any]:
        return row_dict_to_row_list(self.headers, row_dict, fill=fill)

    def ensure_length(self, row: Sequence[Any], fill: Any = "") -> List[Any]:
        return ensure_row_length(row, len(self.headers), fill=fill)


MARKET_SCHEMA = SheetSchema(name=SHEET_MARKET_DATA, headers=MARKET_HEADERS)
TOP_SCHEMA = SheetSchema(name=SHEET_TOP_7, headers=TOP_HEADERS)
PORTFOLIO_SCHEMA = SheetSchema(name=SHEET_PORTFOLIO, headers=PORTFOLIO_HEADERS)


# =============================================================================
# Common lookups (used by portfolio_engine/app.py)
# =============================================================================

def build_market_price_map(
    market_headers: Sequence[str],
    market_rows: Sequence[Sequence[Any]],
    ticker_col: str = "Ticker",
    price_col: str = "Current Price",
) -> Dict[str, float]:
    """
    Build {TICKER: current_price} from Market Data table.

    Notes:
    - Ticker is normalized to upper()
    - Non-numeric prices are ignored
    """
    idx = header_index_map(market_headers)
    ti = idx.get(ticker_col)
    pi = idx.get(price_col)
    if ti is None or pi is None:
        return {}

    out: Dict[str, float] = {}
    for r in market_rows or []:
        if not r:
            continue
        if len(r) <= max(ti, pi):
            continue
        t = str(r[ti] or "").strip().upper()
        if not t:
            continue
        try:
            p = float(str(r[pi]).replace(",", "").strip())
        except Exception:
            continue
        out[t] = float(p)
    return out
