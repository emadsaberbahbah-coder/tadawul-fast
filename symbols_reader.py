"""
symbols_reader.py
===========================================================
Central symbol reader for ALL dashboard Google Sheets pages.

GOALS
- Single source of truth for reading "Symbol" columns from Sheets.
- Dynamically resolves Sheet Names from `env.py`.
- Auto-detects "Symbol"/"Ticker" column index (header row is typically 5).
- Splits KSA (.SR) vs Global tickers for routing.
- Defensive + stable: never raises for normal usage.

DEPENDENCIES
- google_sheets_service (get_sheets_service + split_tickers_by_market)
- env (optional; for configuration)

Usage:
    from symbols_reader import get_page_symbols
    data = get_page_symbols("KSA_TADAWUL")
    # -> {"all": [...], "ksa": [...], "global": [...]}
"""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple

# --- CONFIG IMPORT ---
try:
    from env import settings  # type: ignore
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

# --- SHEETS SERVICE IMPORT ---
try:
    from google_sheets_service import get_sheets_service, split_tickers_by_market  # type: ignore
    try:
        # Older versions may export read_range; new versions may not.
        from google_sheets_service import read_range as _read_range_external  # type: ignore
    except Exception:
        _read_range_external = None  # type: ignore
except Exception:
    get_sheets_service = None  # type: ignore

    def split_tickers_by_market(tickers: List[str]) -> Dict[str, List[str]]:  # type: ignore
        ksa, glob = [], []
        for t in tickers or []:
            x = (t or "").strip().upper()
            if not x:
                continue
            if x.endswith(".SR") or x.isdigit():
                ksa.append(x)
            else:
                glob.append(x)
        return {"ksa": ksa, "global": glob}


logger = logging.getLogger("symbols_reader")


# ----------------------------------------------------------------------
# Page Configuration
# ----------------------------------------------------------------------
@dataclass(frozen=True)
class PageConfig:
    key: str
    sheet_name: str
    header_row: int = 5          # Standard dashboard header row
    max_columns: int = 60        # Search A:BH (covers wide dashboards)


def _safe_sheet_name(name: Any) -> str:
    return (str(name or "").strip()) if name is not None else ""


# Map logical keys to Env Settings (changing env updates logic automatically)
PAGE_REGISTRY: Dict[str, PageConfig] = {
    "KSA_TADAWUL": PageConfig("KSA_TADAWUL", _safe_sheet_name(getattr(settings, "sheet_ksa_tadawul", "KSA_Tadawul_Market"))),
    "GLOBAL_MARKETS": PageConfig("GLOBAL_MARKETS", _safe_sheet_name(getattr(settings, "sheet_global_markets", "Global_Markets"))),
    "MUTUAL_FUNDS": PageConfig("MUTUAL_FUNDS", _safe_sheet_name(getattr(settings, "sheet_mutual_funds", "Mutual_Funds"))),
    "COMMODITIES_FX": PageConfig("COMMODITIES_FX", _safe_sheet_name(getattr(settings, "sheet_commodities_fx", "Commodities_FX"))),
    "MY_PORTFOLIO": PageConfig("MY_PORTFOLIO", _safe_sheet_name(getattr(settings, "sheet_my_portfolio", "My_Portfolio"))),
    "INSIGHTS_ANALYSIS": PageConfig("INSIGHTS_ANALYSIS", _safe_sheet_name(getattr(settings, "sheet_insights_analysis", "Insights_Analysis"))),
    "MARKET_LEADERS": PageConfig("MARKET_LEADERS", _safe_sheet_name(getattr(settings, "sheet_market_leaders", "Market_Leaders"))),
    "INVESTMENT_ADVISOR": PageConfig("INVESTMENT_ADVISOR", _safe_sheet_name(getattr(settings, "sheet_investment_advisor", "Investment_Advisor"))),
    # Optional / future pages (safe to keep registered)
    "ECONOMIC_CALENDAR": PageConfig("ECONOMIC_CALENDAR", _safe_sheet_name(getattr(settings, "sheet_economic_calendar", "Economic_Calendar"))),
    "INVESTMENT_INCOME": PageConfig("INVESTMENT_INCOME", _safe_sheet_name(getattr(settings, "sheet_investment_income", "Investment_Income_Statement"))),
}


# ----------------------------------------------------------------------
# Sheets read wrapper (works whether google_sheets_service exports read_range or not)
# ----------------------------------------------------------------------
def read_range(spreadsheet_id: str, range_name: str) -> List[List[Any]]:
    """
    Read a range from Google Sheets.

    Returns: list of rows (2D list). Empty list if range is empty/unreadable.
    """
    if _read_range_external:
        try:
            return _read_range_external(spreadsheet_id, range_name)  # type: ignore[misc]
        except Exception as e:
            logger.warning("[SymbolsReader] external read_range failed for %s: %s", range_name, e)

    if not get_sheets_service:
        raise RuntimeError("google_sheets_service.get_sheets_service is not available in this environment.")

    service = get_sheets_service()
    try:
        result = (
            service.spreadsheets()
            .values()
            .get(spreadsheetId=spreadsheet_id, range=range_name, valueRenderOption="UNFORMATTED_VALUE")
            .execute()
        )
        return result.get("values", []) or []
    except Exception as e:
        logger.error("[SymbolsReader] Failed to read range %s: %s", range_name, e)
        return []


# ----------------------------------------------------------------------
# Internal Helpers
# ----------------------------------------------------------------------
def _col_idx_to_a1(n: int) -> str:
    """0 -> A, 1 -> B, ... 25 -> Z, 26 -> AA"""
    s = ""
    n += 1
    while n > 0:
        n, rem = divmod(n - 1, 26)
        s = chr(65 + rem) + s
    return s


def _normalize_header(cell: Any) -> str:
    if cell is None:
        return ""
    return str(cell).strip().upper()


def _quote_sheet_name(sheet_name: str) -> str:
    # A1 notation requires doubling single-quotes inside name
    return f"'{(sheet_name or '').replace(\"'\", \"''\")}'"


def _normalize_symbol(cell: Any) -> str:
    """
    Normalize symbol values coming from sheets.
    - Uppercase, trim
    - Remove "TADAWUL:" prefix
    - Convert numeric -> .SR
    - Ignore obvious junk
    """
    if cell is None:
        return ""
    s = str(cell).strip().upper()
    if not s:
        return ""

    if s in ("-", "N/A", "NA", "NULL", "NONE"):
        return ""

    # strip common label remnants
    if s in ("SYMBOL", "TICKER", "CODE", "STOCK"):
        return ""

    if s.startswith("TADAWUL:"):
        s = s.split(":", 1)[1].strip()

    if s.endswith(".TADAWUL"):
        s = s.replace(".TADAWUL", ".SR")

    # numeric ksa
    if s.isdigit():
        return f"{s}.SR"

    # remove internal spaces
    s = s.replace(" ", "")

    return s


# ----------------------------------------------------------------------
# Column detection cache (to reduce header reads)
# ----------------------------------------------------------------------
_COL_CACHE: Dict[Tuple[str, str, int], Tuple[float, Optional[int]]] = {}
_COL_CACHE_TTL_SEC = 300.0  # 5 minutes


def _get_cached_col(sid: str, sheet: str, header_row: int) -> Optional[int]:
    key = (sid, sheet, header_row)
    item = _COL_CACHE.get(key)
    if not item:
        return None
    ts, idx = item
    if (time.time() - ts) > _COL_CACHE_TTL_SEC:
        _COL_CACHE.pop(key, None)
        return None
    return idx


def _set_cached_col(sid: str, sheet: str, header_row: int, idx: Optional[int]) -> None:
    _COL_CACHE[(sid, sheet, header_row)] = (time.time(), idx)


def _find_symbol_column(
    spreadsheet_id: str,
    sheet_name: str,
    header_row: int,
    max_cols: int,
) -> Optional[int]:
    """
    Reads the header row and looks for 'Symbol', 'Ticker', or 'Code'.
    Returns 0-based column index, or None if not found.
    """
    sid = spreadsheet_id
    sname = sheet_name

    cached = _get_cached_col(sid, sname, header_row)
    if cached is not None:
        return cached

    last_col = _col_idx_to_a1(max(1, int(max_cols)))
    range_name = f"{_quote_sheet_name(sname)}!A{header_row}:{last_col}{header_row}"

    rows = read_range(sid, range_name)
    if not rows or not rows[0]:
        logger.warning("[SymbolsReader] Header row empty at %s", range_name)
        _set_cached_col(sid, sname, header_row, None)
        return None

    headers = [_normalize_header(h) for h in rows[0]]

    # Priority search
    for candidate in ("SYMBOL", "TICKER", "CODE", "STOCK", "ASSET"):
        if candidate in headers:
            idx = headers.index(candidate)
            logger.info(
                "[SymbolsReader] Found '%s' at column %s in %s",
                candidate,
                _col_idx_to_a1(idx),
                sname,
            )
            _set_cached_col(sid, sname, header_row, idx)
            return idx

    # Heuristic fallback: if col A header looks like "COMPANY" but symbols might still be in A
    if headers and headers[0] in ("ASSET", "COMPANY", "COMPANYNAME"):
        _set_cached_col(sid, sname, header_row, 0)
        return 0

    logger.warning("[SymbolsReader] Could not identify Symbol column in %s. Headers: %s", sname, headers)
    _set_cached_col(sid, sname, header_row, None)
    return None


# ----------------------------------------------------------------------
# Public API
# ----------------------------------------------------------------------
def get_symbols_from_sheet(
    spreadsheet_id: str,
    sheet_name: str,
    header_row: int = 5,
    max_cols: int = 60,
) -> Dict[str, List[str]]:
    """
    Reads a specific sheet, finds the symbol column, and returns categorized tickers.
    """
    sid = (spreadsheet_id or getattr(settings, "default_spreadsheet_id", "") or "").strip()
    if not sid:
        logger.error("[SymbolsReader] No Spreadsheet ID configured.")
        return {"all": [], "ksa": [], "global": []}

    sname = (sheet_name or "").strip()
    if not sname:
        logger.error("[SymbolsReader] Sheet name is empty.")
        return {"all": [], "ksa": [], "global": []}

    # 1) Detect column
    col_idx = _find_symbol_column(sid, sname, int(header_row), int(max_cols))
    target_col = col_idx if col_idx is not None else 0
    col_letter = _col_idx_to_a1(target_col)

    # 2) Read data from row after header down the full column
    data_range = f"{_quote_sheet_name(sname)}!{col_letter}{int(header_row) + 1}:{col_letter}"
    raw_rows = read_range(sid, data_range)

    # 3) Clean & normalize, dedupe preserve order
    all_symbols: List[str] = []
    seen = set()

    for row in raw_rows or []:
        if not row:
            continue
        val = _normalize_symbol(row[0])
        if not val:
            continue

        if val not in seen:
            seen.add(val)
            all_symbols.append(val)

    # 4) Split
    split = split_tickers_by_market(all_symbols)

    return {"all": all_symbols, "ksa": split.get("ksa", []), "global": split.get("global", [])}


def get_page_symbols(page_key: str, spreadsheet_id: Optional[str] = None) -> Dict[str, List[str]]:
    """
    High-level accessor using logical page keys (e.g. "GLOBAL_MARKETS").
    """
    key = (page_key or "").strip().upper()
    cfg = PAGE_REGISTRY.get(key)
    if not cfg:
        logger.error("[SymbolsReader] Invalid page key: %s", page_key)
        return {"all": [], "ksa": [], "global": []}

    # If sheet_name is empty (misconfigured), return empty safely
    if not (cfg.sheet_name or "").strip():
        logger.error("[SymbolsReader] Page key %s has empty sheet_name (check env settings).", key)
        return {"all": [], "ksa": [], "global": []}

    return get_symbols_from_sheet(
        spreadsheet_id=spreadsheet_id or getattr(settings, "default_spreadsheet_id", ""),
        sheet_name=cfg.sheet_name,
        header_row=cfg.header_row,
        max_cols=cfg.max_columns,
    )


def get_all_pages_symbols(spreadsheet_id: Optional[str] = None) -> Dict[str, Dict[str, List[str]]]:
    """
    Scans ALL registered pages. Heavy operation.
    Returns: { "KSA_TADAWUL": { "all": [...], ... }, ... }
    """
    out: Dict[str, Dict[str, List[str]]] = {}
    for key in PAGE_REGISTRY.keys():
        out[key] = get_page_symbols(key, spreadsheet_id)
    return out


__all__ = [
    "PAGE_REGISTRY",
    "PageConfig",
    "read_range",
    "get_symbols_from_sheet",
    "get_page_symbols",
    "get_all_pages_symbols",
]
