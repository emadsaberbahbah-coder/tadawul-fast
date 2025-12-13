"""
symbols_reader.py
===========================================================
Central symbol reader for ALL 9 Google Sheets pages.

GOALS
- Single source of truth for reading "Symbol" columns from Sheets.
- Dynamically resolves Sheet Names from `env.py`.
- Auto-detects "Symbol"/"Ticker" column index.
- Splits KSA (.SR) vs Global tickers for routing.

DEPENDENCIES
- google_sheets_service (for API calls)
- env (for configuration)

Usage:
    from symbols_reader import get_page_symbols
    data = get_page_symbols("KSA_TADAWUL")
    # -> {"all": [...], "ksa": [...], "global": [...]}
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Any, Dict, List, Optional

# --- CORE IMPORTS ---
try:
    from env import settings
except ImportError:
    # Fallback for limited environments
    import os
    class MockSettings:
        default_spreadsheet_id = os.getenv("DEFAULT_SPREADSHEET_ID", "")
        # Default standard names
        sheet_ksa_tadawul = "KSA_Tadawul_Market"
        sheet_global_markets = "Global_Markets"
        sheet_mutual_funds = "Mutual_Funds"
        sheet_commodities_fx = "Commodities_FX"
        sheet_market_leaders = "Market_Leaders"
        sheet_my_portfolio = "My_Portfolio"
        sheet_insights_analysis = "Insights_Analysis"
        sheet_investment_advisor = "Investment_Advisor"
        sheet_economic_calendar = "Economic_Calendar"
        sheet_investment_income = "Investment_Income_Statement"
    settings = MockSettings() # type: ignore

from google_sheets_service import read_range, split_tickers_by_market

logger = logging.getLogger("symbols_reader")

# ----------------------------------------------------------------------
# Page Configuration
# ----------------------------------------------------------------------

@dataclass
class PageConfig:
    key: str
    sheet_name: str
    header_row: int = 5       # Standard dashboard header row
    max_columns: int = 26     # Search A-Z for "Symbol" column

# Map logical keys to Env Settings
# This ensures changing .env updates reading logic automatically
PAGE_REGISTRY: Dict[str, PageConfig] = {
    "KSA_TADAWUL": PageConfig("KSA_TADAWUL", settings.sheet_ksa_tadawul),
    "GLOBAL_MARKETS": PageConfig("GLOBAL_MARKETS", settings.sheet_global_markets),
    "MUTUAL_FUNDS": PageConfig("MUTUAL_FUNDS", settings.sheet_mutual_funds),
    "COMMODITIES_FX": PageConfig("COMMODITIES_FX", settings.sheet_commodities_fx),
    "MY_PORTFOLIO": PageConfig("MY_PORTFOLIO", settings.sheet_my_portfolio),
    "INSIGHTS_ANALYSIS": PageConfig("INSIGHTS_ANALYSIS", settings.sheet_insights_analysis),
    "MARKET_LEADERS": PageConfig("MARKET_LEADERS", settings.sheet_market_leaders),
    "INVESTMENT_ADVISOR": PageConfig("INVESTMENT_ADVISOR", settings.sheet_investment_advisor),
}

# ----------------------------------------------------------------------
# Internal Helpers
# ----------------------------------------------------------------------

def _col_idx_to_a1(n: int) -> str:
    """0 -> A, 1 -> B, ... 25 -> Z, 26 -> AA"""
    string = ""
    n += 1
    while n > 0:
        n, remainder = divmod(n - 1, 26)
        string = chr(65 + remainder) + string
    return string

def _normalize_cell(cell: Any) -> str:
    if cell is None: return ""
    return str(cell).strip().upper()

def _find_symbol_column(
    spreadsheet_id: str, 
    sheet_name: str, 
    header_row: int, 
    max_cols: int
) -> Optional[int]:
    """
    Reads the header row and looks for 'Symbol', 'Ticker', or 'Code'.
    Returns 0-based column index.
    """
    last_col = _col_idx_to_a1(max_cols)
    range_name = f"'{sheet_name}'!A{header_row}:{last_col}{header_row}"
    
    try:
        rows = read_range(spreadsheet_id, range_name)
        if not rows or not rows[0]:
            logger.warning(f"[SymbolsReader] Header row empty at {range_name}")
            return None
        
        headers = [_normalize_cell(h) for h in rows[0]]
        
        # Priority search
        for candidate in ["SYMBOL", "TICKER", "CODE", "STOCK"]:
            if candidate in headers:
                idx = headers.index(candidate)
                logger.info(f"[SymbolsReader] Found '{candidate}' at column {_col_idx_to_a1(idx)} in {sheet_name}")
                return idx
        
        # Fallback: Check if column A is likely valid (heuristic)
        if headers[0] in ["ASSET", "COMPANY"]:
            return 0
            
        logger.warning(f"[SymbolsReader] Could not identify Symbol column in {sheet_name}. Headers: {headers}")
        return None

    except Exception as e:
        logger.error(f"[SymbolsReader] Error detecting columns: {e}")
        return None

# ----------------------------------------------------------------------
# Public API
# ----------------------------------------------------------------------

def get_symbols_from_sheet(
    spreadsheet_id: str,
    sheet_name: str,
    header_row: int = 5
) -> Dict[str, List[str]]:
    """
    Reads a specific sheet, finds the symbol column, and returns categorized tickers.
    """
    sid = spreadsheet_id or settings.default_spreadsheet_id
    if not sid:
        logger.error("[SymbolsReader] No Spreadsheet ID configured.")
        return {"all": [], "ksa": [], "global": []}

    # 1. Detect Column
    col_idx = _find_symbol_column(sid, sheet_name, header_row, 30)
    
    # Default to Column A (0) if detection fails but we proceed
    target_col = col_idx if col_idx is not None else 0
    col_letter = _col_idx_to_a1(target_col)
    
    # 2. Read Data (Header + 1 down to end)
    data_range = f"'{sheet_name}'!{col_letter}{header_row + 1}:{col_letter}"
    
    try:
        raw_rows = read_range(sid, data_range)
    except Exception as e:
        logger.error(f"[SymbolsReader] Failed to read data range {data_range}: {e}")
        return {"all": [], "ksa": [], "global": []}

    # 3. Clean & Normalize
    all_symbols = []
    seen = set()
    
    for row in raw_rows:
        if not row: continue
        val = _normalize_cell(row[0])
        
        # Skip garbage
        if not val or val in ["-", "N/A", "SYMBOL", "TICKER"]: 
            continue
            
        # Normalize KSA (numeric -> .SR)
        # Note: routes_argaam handles this too, but good to be safe early
        if val.isdigit():
            val = f"{val}.SR"
            
        if val not in seen:
            seen.add(val)
            all_symbols.append(val)

    # 4. Split
    split = split_tickers_by_market(all_symbols)
    
    return {
        "all": all_symbols,
        "ksa": split["ksa"],
        "global": split["global"]
    }

def get_page_symbols(page_key: str, spreadsheet_id: Optional[str] = None) -> Dict[str, List[str]]:
    """
    High-level accessor using logical page keys (e.g. "GLOBAL_MARKETS").
    """
    config = PAGE_REGISTRY.get(page_key.upper())
    if not config:
        logger.error(f"[SymbolsReader] Invalid page key: {page_key}")
        return {"all": [], "ksa": [], "global": []}
    
    return get_symbols_from_sheet(
        spreadsheet_id=spreadsheet_id,
        sheet_name=config.sheet_name,
        header_row=config.header_row
    )

def get_all_pages_symbols(spreadsheet_id: Optional[str] = None) -> Dict[str, Dict[str, List[str]]]:
    """
    Scans ALL registered pages. Heavy operation.
    Returns: { "KSA_TADAWUL": { "all": [...], ... }, ... }
    """
    results = {}
    for key in PAGE_REGISTRY:
        results[key] = get_page_symbols(key, spreadsheet_id)
    return results

__all__ = [
    "get_symbols_from_sheet",
    "get_page_symbols",
    "get_all_pages_symbols",
    "PAGE_REGISTRY"
]
