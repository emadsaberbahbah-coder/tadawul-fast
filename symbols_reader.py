"""
symbols_reader.py
===========================================================
Central symbol reader for ALL dashboard pages.

- Reads symbol column from Sheets reliably (auto-detect header index).
- Splits KSA (.SR / numeric) vs Global.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Any, Dict, List, Optional

try:
    from env import settings
except Exception:  # pragma: no cover
    import os
    class MockSettings:
        default_spreadsheet_id = os.getenv("DEFAULT_SPREADSHEET_ID", "")
        sheet_ksa_tadawul = "KSA_Tadawul_Market"
        sheet_global_markets = "Global_Markets"
        sheet_mutual_funds = "Mutual_Funds"
        sheet_commodities_fx = "Commodities_FX"
        sheet_market_leaders = "Market_Leaders"
        sheet_my_portfolio = "My_Portfolio"
        sheet_insights_analysis = "Insights_Analysis"
        sheet_investment_advisor = "Investment_Advisor"
    settings = MockSettings()  # type: ignore

from google_sheets_service import read_range, split_tickers_by_market

logger = logging.getLogger("symbols_reader")

@dataclass
class PageConfig:
    key: str
    sheet_name: str
    header_row: int = 5
    max_columns: int = 30

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

def _col_idx_to_a1(n: int) -> str:
    s = ""
    n += 1
    while n > 0:
        n, r = divmod(n - 1, 26)
        s = chr(65 + r) + s
    return s

def _normalize_cell(cell: Any) -> str:
    return "" if cell is None else str(cell).strip().upper()

def _find_symbol_column(spreadsheet_id: str, sheet_name: str, header_row: int, max_cols: int) -> Optional[int]:
    last_col = _col_idx_to_a1(max_cols - 1)
    rng = f"'{sheet_name}'!A{header_row}:{last_col}{header_row}"
    try:
        rows = read_range(spreadsheet_id, rng)
        if not rows or not rows[0]:
            logger.warning("[SymbolsReader] Header row empty: %s", rng)
            return None

        headers = [_normalize_cell(h) for h in rows[0]]
        for candidate in ["SYMBOL", "TICKER", "CODE", "STOCK", "ASSET"]:
            if candidate in headers:
                idx = headers.index(candidate)
                logger.info("[SymbolsReader] Found %s at %s in %s", candidate, _col_idx_to_a1(idx), sheet_name)
                return idx

        return 0  # fallback
    except Exception as e:
        logger.error("[SymbolsReader] Header detection error: %s", e)
        return None

def get_symbols_from_sheet(spreadsheet_id: str, sheet_name: str, header_row: int = 5) -> Dict[str, List[str]]:
    sid = spreadsheet_id or settings.default_spreadsheet_id
    if not sid:
        logger.error("[SymbolsReader] DEFAULT_SPREADSHEET_ID is missing.")
        return {"all": [], "ksa": [], "global": []}

    col_idx = _find_symbol_column(sid, sheet_name, header_row, 30)
    target_col = col_idx if col_idx is not None else 0
    col_letter = _col_idx_to_a1(target_col)

    data_range = f"'{sheet_name}'!{col_letter}{header_row + 1}:{col_letter}"
    try:
        raw_rows = read_range(sid, data_range)
    except Exception as e:
        logger.error("[SymbolsReader] Read failed %s: %s", data_range, e)
        return {"all": [], "ksa": [], "global": []}

    all_symbols: List[str] = []
    seen = set()

    for row in raw_rows or []:
        if not row:
            continue
        val = _normalize_cell(row[0])
        if not val or val in {"-", "N/A", "SYMBOL", "TICKER"}:
            continue
        if val.isdigit():
            val = f"{val}.SR"
        if val not in seen:
            seen.add(val)
            all_symbols.append(val)

    split = split_tickers_by_market(all_symbols)
    return {"all": all_symbols, "ksa": split["ksa"], "global": split["global"]}

def get_page_symbols(page_key: str, spreadsheet_id: Optional[str] = None) -> Dict[str, List[str]]:
    cfg = PAGE_REGISTRY.get((page_key or "").upper())
    if not cfg:
        logger.error("[SymbolsReader] Invalid page key: %s", page_key)
        return {"all": [], "ksa": [], "global": []}
    return get_symbols_from_sheet(spreadsheet_id=spreadsheet_id, sheet_name=cfg.sheet_name, header_row=cfg.header_row)

def get_all_pages_symbols(spreadsheet_id: Optional[str] = None) -> Dict[str, Dict[str, List[str]]]:
    return {k: get_page_symbols(k, spreadsheet_id) for k in PAGE_REGISTRY.keys()}

__all__ = ["get_symbols_from_sheet", "get_page_symbols", "get_all_pages_symbols", "PAGE_REGISTRY"]
