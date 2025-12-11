"""
symbols_reader.py
===========================================================
Central symbol reader for ALL 9 Google Sheets pages.

GOALS
- Be the single place where we read "which symbols should we fetch" from
  Google Sheets (KSA, Global, Funds, FX, Portfolio, etc.).
- Cleanly separate:
    • KSA symbols (.SR)  -> KSA/Tadawul/Argaam providers
      (NO EODHD for KSA anywhere).
    • Non-KSA symbols    -> Global providers (FMP, etc.).
- Work nicely with:
    • google_sheets_service.py (Sheets API access + helpers)
    • core.data_engine / core.data_engine_v2 (unified engine)
    • routes/* (enriched, analysis, advanced, KSA gateway)
    • Google Apps Script / external tools calling into Python.

ASSUMPTIONS
-----------
1) Sheets follow a consistent pattern:
    - Header row is usually ROW 5.
    - Data starts at ROW 6.
    - There is a "Symbol" / "Ticker" / "Code" column in the header row.

2) Default sheet names (can be overridden by callers):
    - "KSA_Tadawul"
    - "Global_Markets"
    - "Mutual_Funds"
    - "Commodities_FX"
    - "My_Portfolio"
    - "Insights_Analysis"
    - (Optionally: "Market_Leaders", "Investment_Advisor", etc.)

USAGE EXAMPLES
--------------
    from symbols_reader import (
        get_symbols_from_sheet,
        get_page_symbols,
        get_all_pages_symbols,
    )

    # 1) Generic: read symbols from a sheet
    syms = get_symbols_from_sheet(
        spreadsheet_id="YOUR_SHEET_ID",
        sheet_name="KSA_Tadawul",
        header_row=5,
    )
    # syms["all"]    -> all valid symbols on that page
    # syms["ksa"]    -> only .SR symbols
    # syms["global"] -> non-.SR symbols

    # 2) Using logical pages (KSA / Global / Funds / FX / Portfolio...)
    page_syms = get_page_symbols(
        page_key="KSA_TADAWUL",
        spreadsheet_id="YOUR_SHEET_ID",
    )

NOTE
----
- This module does NOT talk to any market data provider.
- It ONLY reads symbols from Google Sheets and classifies them.
- Actual market routing (KSA vs Global) is handled by the unified data engine.
"""

from __future__ import annotations

import logging
import os
from dataclasses import dataclass
from typing import Any, Dict, List, Optional

# Sheets client – this is the ONLY dependency:
#   read_range: low-level reader
#   split_tickers_by_market: returns {"ksa": [...], "global": [...], "all": [...]?}
from google_sheets_service import read_range, split_tickers_by_market

logger = logging.getLogger("symbols_reader")
if not logger.handlers:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )

# ----------------------------------------------------------------------
# DEFAULT SPREADSHEET ID RESOLUTION (env.py + env vars)
# ----------------------------------------------------------------------

try:  # pragma: no cover - env.py is optional
    import env as _env_mod  # type: ignore
except Exception:  # pragma: no cover
    _env_mod = None  # type: ignore


def _resolve_default_spreadsheet_id() -> str:
    """
    Read the default spreadsheet ID from env.settings.default_spreadsheet_id
    if available; otherwise from the DEFAULT_SPREADSHEET_ID env var.
    """
    sid = ""
    try:
        if _env_mod is not None and hasattr(_env_mod, "settings"):
            settings = getattr(_env_mod, "settings")
            sid = getattr(settings, "default_spreadsheet_id", "") or ""
    except Exception:
        sid = ""

    if not sid:
        sid = os.getenv("DEFAULT_SPREADSHEET_ID", "").strip()

    return sid


DEFAULT_SPREADSHEET_ID: str = _resolve_default_spreadsheet_id()

# ----------------------------------------------------------------------
# PAGE CONFIG (for your 9+ pages) – can be overridden by callers
# ----------------------------------------------------------------------


@dataclass
class PageConfig:
    key: str
    sheet_name: str
    header_row: int = 5        # where headers live
    max_columns: int = 64      # enough for ~59-column layout


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
    # Optional page presets (you can use these later if you like)
    "MARKET_LEADERS": PageConfig(
        key="MARKET_LEADERS",
        sheet_name="Market_Leaders",
        header_row=5,
    ),
    "INVESTMENT_ADVISOR": PageConfig(
        key="INVESTMENT_ADVISOR",
        sheet_name="Investment_Advisor",
        header_row=5,
    ),
}

# ----------------------------------------------------------------------
# HELPER FUNCTIONS – internal
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
    Convert 0-based column index to an A1-style column letter.
      0 -> 'A', 25 -> 'Z', 26 -> 'AA', etc.
    """
    if idx < 0:
        raise ValueError("Column index must be >= 0")
    letters = ""
    n = idx + 1  # 1-based
    while n > 0:
        n, remainder = divmod(n - 1, 26)
        letters = chr(65 + remainder) + letters
    return letters


def _dedupe_preserve_order(values: List[str]) -> List[str]:
    """
    Remove duplicates while preserving the original order.
    """
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
    - Treats '', 'NA', '-', '--' as None.
    - Accepts numbers (e.g. 1120) and converts to string ("1120").
    """
    if cell_value is None:
        return None

    if isinstance(cell_value, (int, float)):
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
    last_col_letter = _column_index_to_letter(max_columns - 1)
    range_a1 = f"{sheet_name}!A{header_row}:{last_col_letter}{header_row}"

    try:
        rows = read_range(spreadsheet_id, range_a1)
    except Exception as exc:
        logger.error(
            "[symbols_reader] Error reading header row for %s!%s: %s",
            sheet_name,
            range_a1,
            exc,
        )
        return None

    if not rows:
        logger.warning(
            "[symbols_reader] No header row data found in range %s for sheet %s.",
            range_a1,
            sheet_name,
        )
        return None

    header = rows[0]
    normalized = [str(c or "").strip().lower() for c in header]

    # Accept exact logical names; avoid accidental match with random text
    candidates = {"symbol", "ticker", "code"}
    for idx, col_name in enumerate(normalized):
        if col_name in candidates:
            logger.info(
                "[symbols_reader] Detected symbol column '%s' at index %d on %s.",
                col_name,
                idx,
                sheet_name,
            )
            return idx

    logger.warning(
        "[symbols_reader] Could not detect Symbol/Ticker column in %s (row %d). "
        "Header row was: %s",
        sheet_name,
        header_row,
        header,
    )
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

    try:
        values = read_range(spreadsheet_id, range_a1)
    except Exception as exc:
        logger.error(
            "[symbols_reader] Error reading symbols column for %s!%s: %s",
            sheet_name,
            range_a1,
            exc,
        )
        return []

    symbols: List[str] = []
    for row in values:
        if not row:
            continue
        cell = row[0]
        sym = _normalize_symbol(cell)
        if sym:
            symbols.append(sym)

    deduped = _dedupe_preserve_order(symbols)
    logger.info(
        "[symbols_reader] Read %d raw symbols (%d unique) from %s!%s.",
        len(symbols),
        len(deduped),
        sheet_name,
        col_letter,
    )
    return deduped


# ----------------------------------------------------------------------
# PUBLIC API
# ----------------------------------------------------------------------


def get_symbols_from_sheet(
    spreadsheet_id: Optional[str],
    sheet_name: str,
    header_row: int = 5,
    max_columns: int = 64,
) -> Dict[str, List[str]]:
    """
    Generic reader: read symbols from the given sheet.

    Returns a dict:
        {
          "all":    [... all valid symbols ...],
          "ksa":    [... only .SR tickers (KSA) ...],
          "global": [... non-.SR symbols ...]
        }

    This is the main function you will use when you want to:
        - Refresh KSA_Tadawul page
        - Refresh Global_Markets / Mutual_Funds / Commodities_FX
        - Or any sheet that has a Symbol/Ticker column.
    """
    sid = _resolve_spreadsheet_id(spreadsheet_id)
    if not sheet_name:
        raise ValueError("sheet_name is required for get_symbols_from_sheet().")

    # 1) Detect symbol column index from header row
    col_idx = _detect_symbol_column(
        spreadsheet_id=sid,
        sheet_name=sheet_name,
        header_row=header_row,
        max_columns=max_columns,
    )
    if col_idx is None:
        logger.warning(
            "[symbols_reader] No symbol column detected for sheet '%s'. "
            "Returning empty symbol lists.",
            sheet_name,
        )
        return {"all": [], "ksa": [], "global": []}

    # 2) Read all symbols from that column
    raw_symbols = _read_symbols_from_column(
        spreadsheet_id=sid,
        sheet_name=sheet_name,
        col_idx=col_idx,
        header_row=header_row,
    )

    if not raw_symbols:
        return {"all": [], "ksa": [], "global": []}

    # 3) Split into KSA vs GLOBAL (consistent with google_sheets_service)
    split = split_tickers_by_market(raw_symbols)

    ksa_syms = split.get("ksa", []) if isinstance(split, dict) else []
    global_syms = split.get("global", []) if isinstance(split, dict) else []

    return {
        "all": raw_symbols,
        "ksa": ksa_syms,
        "global": global_syms,
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
        One of (case-insensitive):
          - 'KSA_TADAWUL'
          - 'GLOBAL_MARKETS'
          - 'MUTUAL_FUNDS'
          - 'COMMODITIES_FX'
          - 'MY_PORTFOLIO'
          - 'INSIGHTS_ANALYSIS'
          - 'MARKET_LEADERS'
          - 'INVESTMENT_ADVISOR'
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
          "global": [...],
        }
    """
    key = page_key.upper().strip()
    base_cfg = DEFAULT_PAGE_CONFIGS.get(key)
    if not base_cfg:
        raise ValueError(
            f"Unknown page_key '{page_key}'. "
            f"Valid keys: {', '.join(sorted(DEFAULT_PAGE_CONFIGS.keys()))}"
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

    logger.info(
        "[symbols_reader] Reading symbols for page '%s' from sheet '%s' "
        "(header_row=%d, max_columns=%d).",
        cfg.key,
        cfg.sheet_name,
        cfg.header_row,
        cfg.max_columns,
    )

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


__all__ = [
    "PageConfig",
    "DEFAULT_PAGE_CONFIGS",
    "DEFAULT_SPREADSHEET_ID",
    "get_symbols_from_sheet",
    "get_page_symbols",
    "get_all_pages_symbols",
]
