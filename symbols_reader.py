"""
symbols_reader.py
===========================================================
Central symbol reader for ALL dashboard Google Sheets pages.

Fixes / Enhancements
- Works with google_sheets_service.read_range()
- Stronger symbol column detection
- Adds missing pages (economic_calendar, investment_income) safely
- Better normalization for KSA numeric symbols -> .SR
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Any, Dict, List, Optional

try:
    from env import settings
except Exception:
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
        sheet_economic_calendar = "Economic_Calendar"
        sheet_investment_income = "Investment_Income_Statement"
    settings = MockSettings()  # type: ignore

from google_sheets_service import read_range, split_tickers_by_market

logger = logging.getLogger("symbols_reader")

@dataclass
class PageConfig:
    key: str
    sheet_name: str
    header_row: int = 5
    max_columns: int = 52  # scan wider (A-AZ)

PAGE_REGISTRY: Dict[str, PageConfig] = {
    "KSA_TADAWUL": PageConfig("KSA_TADAWUL", settings.sheet_ksa_tadawul),
    "GLOBAL_MARKETS": PageConfig("GLOBAL_MARKETS", settings.sheet_global_markets),
    "MUTUAL_FUNDS": PageConfig("MUTUAL_FUNDS", settings.sheet_mutual_funds),
    "COMMODITIES_FX": PageConfig("COMMODITIES_FX", settings.sheet_commodities_fx),
    "MY_PORTFOLIO": PageConfig("MY_PORTFOLIO", settings.sheet_my_portfolio),
    "INSIGHTS_ANALYSIS": PageConfig("INSIGHTS_ANALYSIS", settings.sheet_insights_analysis),
    "MARKET_LEADERS": PageConfig("MARKET_LEADERS", settings.sheet_market_leaders),
    "INVESTMENT_ADVISOR": PageConfig("INVESTMENT_ADVISOR", settings.sheet_investment_advisor),
    "ECONOMIC_CALENDAR": PageConfig("ECONOMIC_CALENDAR", getattr(settings, "sheet_economic_calendar", "Economic_Calendar")),
    "INVESTMENT_INCOME": PageConfig("INVESTMENT_INCOME", getattr(settings, "sheet_investment_income", "Investment_Income_Statement")),
}

def _col_idx_to_a1(n: int) -> str:
    """0 -> A, 25 -> Z, 26 -> AA"""
    s = ""
    n += 1
    while n > 0:
        n, r = divmod(n - 1, 26)
        s = chr(65 + r) + s
    return s

def _normalize_cell(cell: Any) -> str:
    if cell is None:
        return ""
    return str(cell).strip().upper()

def _find_symbol_column(spreadsheet_id: str, sheet_name: str, header_row: int, max_cols: int) -> Optional[int]:
    last_col = _col_idx_to_a1(max_cols - 1)
    rng = f"'{sheet_name}'!A{header_row}:{last_col}{header_row}"
    try:
        rows = read_range(spreadsheet_id, rng)
        if not rows or not rows[0]:
            logger.warning("[SymbolsReader] Header row empty: %s", rng)
            return None

        headers = [_normalize_cell(h) for h in rows[0]]

        # Strict priority list
        candidates = [
            "SYMBOL", "TICKER", "STOCK", "CODE",
            "ASSET", "SECURITY", "INSTRUMENT"
        ]
        for c in candidates:
            if c in headers:
                idx = headers.index(c)
                logger.info("[SymbolsReader] Found %s at %s in %s", c, _col_idx_to_a1(idx), sheet_name)
                return idx

        # Heuristic: if first header is empty but column A contains symbols
        if headers and headers[0] in ("", " "):
            return 0

        logger.warning("[SymbolsReader] No symbol column detected in %s. Headers=%s", sheet_name, headers[:20])
        return None
    except Exception as e:
        logger.error("[SymbolsReader] Column detect failed in %s: %s", sheet_name, e)
        return None

def get_symbols_from_sheet(spreadsheet_id: str, sheet_name: str, header_row: int = 5) -> Dict[str, List[str]]:
    sid = (spreadsheet_id or settings.default_spreadsheet_id or "").strip()
    if not sid:
        logger.error("[SymbolsReader] No Spreadsheet ID configured.")
        return {"all": [], "ksa": [], "global": []}

    col_idx = _find_symbol_column(sid, sheet_name, header_row, 52)
    target_col = col_idx if col_idx is not None else 0
    col_letter = _col_idx_to_a1(target_col)

    # Read from row below header to end
    data_rng = f"'{sheet_name}'!{col_letter}{header_row + 1}:{col_letter}"
    try:
        raw_rows = read_range(sid, data_rng)
    except Exception as e:
        logger.error("[SymbolsReader] Failed reading %s: %s", data_rng, e)
        return {"all": [], "ksa": [], "global": []}

    all_symbols: List[str] = []
    seen = set()

    for row in raw_rows or []:
        if not row:
            continue
        val = _normalize_cell(row[0])

        if not val or val in {"-", "N/A", "NA", "SYMBOL", "TICKER"}:
            continue

        # Normalize KSA numeric -> .SR
        if val.isdigit():
            val = f"{val}.SR"

        # Normalize whitespace/dots
        val = val.replace(" ", "")
        if val not in seen:
            seen.add(val)
