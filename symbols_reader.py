"""
symbols_reader.py
===========================================================
Central symbol reader for ALL 9 Google Sheets pages.

GOALS
- Be the single place where we read "which symbols should we fetch" from
  Google Sheets (KSA, Global, Funds, FX, Portfolio, etc.).
- Cleanly separate:
    • KSA symbols (.SR)  -> to be fetched by KSA/Tadawul/Argaam providers
      (NO EODHD for KSA).
    • Non-KSA symbols    -> to be fetched by global providers (EODHD, FMP, etc.).
- Work nicely with:
    • google_sheets_service.py (for Sheets API access)
    • core/data_engine.py      (unified engine, provider routing)
    • routes/*                 (enriched, analysis, advanced, etc.)
    • Google Apps Script / tools calling into Python

ASSUMPTIONS
-----------
1) Your Sheets use a consistent pattern:
    - Header row is often ROW 5.
    - Data starts at ROW 6.
    - There is a "Symbol" or "Ticker" column in the header row.

2) Sheet names (default, can be overridden in callers):
    - "KSA_Tadawul"
    - "Global_Markets"
    - "Mutual_Funds"
    - "Commodities_FX"
    - "My_Portfolio"
    - "Insights_Analysis"   (for analysis-based symbols)
    - etc.

USAGE EXAMPLES
--------------
    from symbols_reader import (
        get_symbols_from_sheet,
        get_page_symbols,
    )

    # 1) Read symbols from a sheet (generic)
    syms = get_symbols_from_sheet(
        spreadsheet_id="YOUR_SHEET_ID",
        sheet_name="KSA_Tadawul",
        header_row=5,
    )
    # syms["all"]   -> all valid symbols
    # syms["ksa"]   -> only .SR
    # syms["global"]-> all non-.SR symbols

    # 2) Use page presets (optional)
    page_syms = get_page_symbols(
        page_key="KSA_TADAWUL",
        spreadsheet_id="YOUR_SHEET_ID",   # or omit if you set DEFAULT_SPREADSHEET_ID
    )

NOTE
----
- This module does NOT call EODHD or any provider.
- It only reads symbols from Google Sheets and classifies them.
- The actual provider routing (KSA vs global) is done by core.data_engine.
"""

from __future__ import annotations

import os
import logging
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple

from google_sheets_service import read_range, split_tickers_by_market

logger = logging.getLogger(__name__)

# Try to pull a default spreadsheet id from env settings if available,
# but do NOT require env.py to change immediately.
try:
    from env import settings  # type: ignore

    DEFAULT_SPREADSHEET_ID: str = (
        getattr(settings, "default_spreadsheet_id", "") or os.getenv("DEFAULT_SPREADSHEET_ID", "")
    )
except Exception:  # pragma: no cover - fallback if env.py not yet updated
    DEFAULT_SPREADSHEET_ID = os.getenv("DEFAULT_SPREADSHEET_ID", "")


# ----------------------------------------------------------------------
# PAGE CONFIG (for your 9 pages) – can be overridden by callers
# ----------------------------------------------------------------------

@dataclass
class PageConfig:
    key: str
    sheet_name: str
    header_row: int = 5  # where headers live
    max_columns: int = 52  # up to column AZ (~52) for header detection


DEFAULT_PAGE_CONFIGS: Dict[str, PageConfig] = {
    # KSA Tadawul market
    "KSA_TADAWUL": PageConfig(
        key="KSA_TADAWUL",
        sheet_name="KSA_Tadawul",
        header_row=5,
    ),
    # Global markets
    "GLOBAL_MARKETS": PageConfig(
        key="GLOBAL_MARKETS",
        sheet_name="Global_Markets",
        header_row=5,
    ),
    # Mutual funds
    "MUTUAL_FUNDS": PageConfig(
        key="MUTUAL_FUNDS",
        sheet_name="Mutual_Funds",
        header_row=5,
    ),
    # Commodities & FX
    "COMMODITIES_FX": PageConfig(
        key="COMMODITIES_FX",
        sheet_name="Commodities_FX",
        header_row=5,
    ),
    # Portfolio (user positions)
    "MY_PORTFOLIO": PageConfig(
        key="MY_PORTFOLIO",
        sheet_name="My_Portfolio",
        header_row=5,
    ),
    # Insights / Analysis (if it has symbols)
    "INSIGHTS_ANALYSIS": PageConfig(
        key="INSIGHTS_ANALYSIS",
        sheet_name="Insights_Analysis",
        header_row=5,
    ),
    # You can extend with:
    # "INVESTMENT_ADVISOR": PageConfig(...),
    # "MARKET_LEADERS": PageConfig(...),
}


# ----------------------------------------------------------------------
# HELPER FUNCTIONS
# ----------------------------------------------------------------------


def _resolve_spreadsheet_id(spreadsheet_id: Optional[str] = None) -> str:
    """
    Resolve spreadsheet ID from argument or DEFAULT_SPREADSHEET_ID.

    Raises ValueError if nothing is configured.
    """
    sid = (spreadsheet_id or "").strip() or DEFAULT_SPREADSHEET_ID
    if not sid:
        raise ValueError(
            "Spreadsheet ID is required but not provided. "
            "Either pass `spreadsheet_id` explicitly or configure "
            "DEFAULT_SPREADSHEET_ID (env var or settings.default_spreadsheet_id)."
        )
    return sid


def _column_index_to_letter(idx: int) -> str:
    """
    Convert 0-based column index to A1 column letter.
    e.g. 0 -> 'A', 25 -> 'Z', 26 -> 'AA'
    """
    letters = ""
    idx += 1  # 1-based
    while idx > 0:
        idx, remainder = divmod(idx - 1, 26)
        letters = chr(65 + remainder) + letters
    return letters


def _dedupe_preserve_order(values: List[str]) -> List[str]:
    seen = set()
    out: List[str] = []
    for v in values:
        if v not in seen:
            seen.add(v)
            out.append(v)
    return out


def _normalize_symbol(cell_value: Any) -> Optional[str]:
    """
    Convert a raw cell value to a clean symbol string or None.

    - Strips whitespace.
    - Uppercases.
    - Treats '', 'NA', '-' as None.
    """
    if cell_value is None:
        return None
    if isinstance(cell_value, (int, float)):
        # Some people store 1120 as number; we can accept it and treat as string
        text = str(cell_value)
    else:
        text = str(cell_value)
    text = text.strip().upper()
    if not text:
        return None
    if text in {"NA", "N/A", "-", "--"}:
        return None
    return text


def _detect_symbol_column(
    spreadsheet_id: str,
    sheet_name: str,
    header_row: int,
    max_columns: int,
) -> Optional[int]:
    """
    Read the header row and detect which column contains 'Symbol' or 'Ticker'.

    Returns:
        0-based column index, or None if not found.
    """
    # Example: "KSA_Tadawul!A5:AZ5"
    last_col_letter = _column_index_to_letter(max_columns - 1)
    range_a1 = f"{sheet_name}!A{header_row}:{last_col_letter}{header_row}"
    rows = read_range(spreadsheet_id, range_a1)
    if not rows:
        return None

    header = rows[0]  # first row in the returned range
    normalized = [str(c or "").strip().lower() for c in header]

    candidates = {"symbol", "ticker", "code"}
    for idx, col_name in enumerate(normalized):
        if col_name in candidates:
            return idx

    return None


def _read_symbols_from_column(
    spreadsheet_id: str,
    sheet_name: str,
    col_idx: int,
    header_row: int,
) -> List[str]:
    """
    Read all symbols from a single column (0-based col index), below the header row.
    """
    col_letter = _column_index_to_letter(col_idx)
    start_row = header_row + 1  # data starts after header
    range_a1 = f"{sheet_name}!{col_letter}{start_row}:{col_letter}"

    values = read_range(spreadsheet_id, range_a1)
    symbols: List[str] = []
    for row in values:
        if not row:
            continue
        cell = row[0]
        sym = _normalize_symbol(cell)
        if sym:
            symbols.append(sym)

    return _dedupe_preserve_order(symbols)


# ----------------------------------------------------------------------
# PUBLIC API
# ----------------------------------------------------------------------


def get_symbols_from_sheet(
    spreadsheet_id: Optional[str],
    sheet_name: str,
    header_row: int = 5,
    max_columns: int = 52,
) -> Dict[str, List[str]]:
    """
    Generic reader: read symbols from the given sheet.

    Returns a dict:
        {
          "all":    [... all valid symbols ...],
          "ksa":    [... only .SR tickers (KSA) ...],
          "global": [... non-.SR symbols ...]
        }

    This is the main function you'll use when you want to:
        - Refresh KSA_Tadawul page
        - Refresh Global_Markets / Mutual_Funds / Commodities_FX
        - Or any other sheet that has a Symbol/Ticker column.
    """
    sid = _resolve_spreadsheet_id(spreadsheet_id)
    if not sheet_name:
        raise ValueError("sheet_name is required for get_symbols_from_sheet().")

    # 1) Detect symbol column from the header row
    col_idx = _detect_symbol_column(
        spreadsheet_id=sid,
        sheet_name=sheet_name,
        header_row=header_row,
        max_columns=max_columns,
    )
    if col_idx is None:
        logger.warning(
            "[symbols_reader] Could not detect Symbol/Ticker column in %s!row=%d; "
            "no symbols will be returned.",
            sheet_name,
            header_row,
        )
        return {"all": [], "ksa": [], "global": []}

    # 2) Read all symbols from that column
    raw_symbols = _read_symbols_from_column(
        spreadsheet_id=sid,
        sheet_name=sheet_name,
        col_idx=col_idx,
        header_row=header_row,
    )

    # 3) Split into KSA vs GLOBAL (consistent with google_sheets_service)
    split = split_tickers_by_market(raw_symbols)

    return {
        "all": raw_symbols,
        "ksa": split["ksa"],
        "global": split["global"],
    }


def get_page_symbols(
    page_key: str,
    spreadsheet_id: Optional[str] = None,
    overrides: Optional[Dict[str, Any]] = None,
) -> Dict[str, List[str]]:
    """
    Read symbols for a specific logical "page" (KSA, Global, Funds, FX, etc.)
    using DEFAULT_PAGE_CONFIGS, optionally overridden.

    Parameters
    ----------
    page_key : str
        One of:
          - 'KSA_TADAWUL'
          - 'GLOBAL_MARKETS'
          - 'MUTUAL_FUNDS'
          - 'COMMODITIES_FX'
          - 'MY_PORTFOLIO'
          - 'INSIGHTS_ANALYSIS'
        (and any others you add to DEFAULT_PAGE_CONFIGS)
    spreadsheet_id : Optional[str]
        If None, uses DEFAULT_SPREADSHEET_ID.
    overrides : Optional[dict]
        For dynamic tweaks like custom header_row or sheet_name:
            {
              "sheet_name": "Custom_Sheet",
              "header_row": 6,
              "max_columns": 40
            }

    Returns
    -------
    dict:
        {
          "all": [...],
          "ksa": [...],
          "global": [...]
        }

    EXAMPLE:
        syms = get_page_symbols("KSA_TADAWUL", spreadsheet_id="...")
        # syms["ksa"]   -> all .SR to send to Tadawul/Argaam engine
        # syms["global"]-> should be empty for this page (normally)
    """
    key = page_key.upper().strip()
    base_cfg = DEFAULT_PAGE_CONFIGS.get(key)
    if not base_cfg:
        raise ValueError(
            f"Unknown page_key '{page_key}'. "
            f"Valid keys: {', '.join(DEFAULT_PAGE_CONFIGS.keys())}"
        )

    cfg = PageConfig(
        key=base_cfg.key,
        sheet_name=base_cfg.sheet_name,
        header_row=base_cfg.header_row,
        max_columns=base_cfg.max_columns,
    )

    if overrides:
        if "sheet_name" in overrides:
            cfg.sheet_name = str(overrides["sheet_name"])
        if "header_row" in overrides:
            cfg.header_row = int(overrides["header_row"])
        if "max_columns" in overrides:
            cfg.max_columns = int(overrides["max_columns"])

    return get_symbols_from_sheet(
        spreadsheet_id=spreadsheet_id,
        sheet_name=cfg.sheet_name,
        header_row=cfg.header_row,
        max_columns=cfg.max_columns,
    )


def get_all_pages_symbols(
    page_keys: Optional[List[str]] = None,
    spreadsheet_id: Optional[str] = None,
) -> Dict[str, Dict[str, List[str]]]:
    """
    Convenience helper: read symbols for multiple pages at once.

    Parameters
    ----------
    page_keys : Optional[List[str]]
        If None, uses all keys from DEFAULT_PAGE_CONFIGS.
        Otherwise a list like ["KSA_TADAWUL","GLOBAL_MARKETS",...].
    spreadsheet_id : Optional[str]
        If None, uses DEFAULT_SPREADSHEET_ID.

    Returns
    -------
    dict:
        {
          "KSA_TADAWUL": { "all": [...], "ksa": [...], "global": [...] },
          "GLOBAL_MARKETS": { ... },
          ...
        }
    """
    sid = _resolve_spreadsheet_id(spreadsheet_id)
    keys = page_keys or list(DEFAULT_PAGE_CONFIGS.keys())

    result: Dict[str, Dict[str, List[str]]] = {}
    for key in keys:
        try:
            result[key] = get_page_symbols(
                page_key=key,
                spreadsheet_id=sid,
            )
        except Exception as exc:
            logger.error(
                "[symbols_reader] Error reading symbols for page %s: %s",
                key,
                exc,
            )
            result[key] = {"all": [], "ksa": [], "global": []}

    return result
