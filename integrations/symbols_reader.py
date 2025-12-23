"""
symbols_reader.py
===========================================================
Central symbol reader for ALL dashboard Google Sheets pages.
(v3.2.0) — Production-hardened, KSA-safe, Sheets-service aligned

What’s improved vs your draft
- Robust A1 range building (finite ranges; avoids open-ended "A6:A" quirks)
- Stronger symbol column detection:
    • checks multiple header rows if needed
    • supports header aliases like "Ticker Symbol", "Stock Code"
    • fallback: scan first N rows and pick the column that "looks like symbols"
- Better normalization:
    • KSA numeric -> ####.SR
    • strips "TADAWUL:" and ".TADAWUL"
    • preserves special tickers (^GSPC, BRK.B, BTC-USD, EURUSD=X, GC=F, etc.)
- De-dup preserves order
- Safe for missing pages / missing settings
- Returns consistent dict: {all, ksa, global, meta}

Dependencies
- google_sheets_service.read_range
- google_sheets_service.split_tickers_by_market
"""

from __future__ import annotations

import logging
import re
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple

try:
    from env import settings  # type: ignore
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
        sheet_economic_calendar = "Economic_Calendar"
        sheet_investment_income = "Investment_Income_Statement"

    settings = MockSettings()  # type: ignore

from google_sheets_service import read_range, split_tickers_by_market

logger = logging.getLogger("symbols_reader")

SYMBOLS_READER_VERSION = "3.2.0"

# How much to scan (keep modest to avoid API cost)
_MAX_COLS_SCAN = 52          # A..AZ
_MAX_DATA_ROWS_SCAN = 2000   # read up to this many data rows for symbol column heuristics
_HEADER_ROWS_TO_TRY = (5, 4, 3)  # primary then fallbacks


# -----------------------------------------------------------------------------
# Data model
# -----------------------------------------------------------------------------
@dataclass(frozen=True)
class PageConfig:
    key: str
    sheet_name: str
    header_row: int = 5
    max_columns: int = _MAX_COLS_SCAN


# -----------------------------------------------------------------------------
# Registry (safe: uses getattr for optional attributes)
# -----------------------------------------------------------------------------
def _safe_sheet_attr(attr: str, default: str) -> str:
    try:
        v = getattr(settings, attr, None)
        if isinstance(v, str) and v.strip():
            return v.strip()
    except Exception:
        pass
    return default


PAGE_REGISTRY: Dict[str, PageConfig] = {
    "KSA_TADAWUL": PageConfig("KSA_TADAWUL", _safe_sheet_attr("sheet_ksa_tadawul", "KSA_Tadawul_Market")),
    "GLOBAL_MARKETS": PageConfig("GLOBAL_MARKETS", _safe_sheet_attr("sheet_global_markets", "Global_Markets")),
    "MUTUAL_FUNDS": PageConfig("MUTUAL_FUNDS", _safe_sheet_attr("sheet_mutual_funds", "Mutual_Funds")),
    "COMMODITIES_FX": PageConfig("COMMODITIES_FX", _safe_sheet_attr("sheet_commodities_fx", "Commodities_FX")),
    "MY_PORTFOLIO": PageConfig("MY_PORTFOLIO", _safe_sheet_attr("sheet_my_portfolio", "My_Portfolio")),
    "INSIGHTS_ANALYSIS": PageConfig("INSIGHTS_ANALYSIS", _safe_sheet_attr("sheet_insights_analysis", "Insights_Analysis")),
    "MARKET_LEADERS": PageConfig("MARKET_LEADERS", _safe_sheet_attr("sheet_market_leaders", "Market_Leaders")),
    "INVESTMENT_ADVISOR": PageConfig("INVESTMENT_ADVISOR", _safe_sheet_attr("sheet_investment_advisor", "Investment_Advisor")),
    "ECONOMIC_CALENDAR": PageConfig("ECONOMIC_CALENDAR", _safe_sheet_attr("sheet_economic_calendar", "Economic_Calendar")),
    "INVESTMENT_INCOME": PageConfig("INVESTMENT_INCOME", _safe_sheet_attr("sheet_investment_income", "Investment_Income_Statement")),
}


# -----------------------------------------------------------------------------
# Small helpers
# -----------------------------------------------------------------------------
_A1_COL_RE = re.compile(r"^[A-Z]+$")
_KSA_NUM_RE = re.compile(r"^\d{3,5}$")

# "looks like a tradable symbol" heuristic:
# - allows: AAPL, BRK.B, BTC-USD, EURUSD=X, GC=F, ^GSPC, 1120.SR, 1120
_SYMBOL_LIKE_RE = re.compile(r"^(?:\^?[A-Z0-9][A-Z0-9\.\-=_]{0,24})(?:\.SR|\.TADAWUL)?$", re.IGNORECASE)


def _col_idx_to_a1(n: int) -> str:
    """0 -> A, 25 -> Z, 26 -> AA"""
    s = ""
    n += 1
    while n > 0:
        n, r = divmod(n - 1, 26)
        s = chr(65 + r) + s
    return s


def _safe_sheet_name(sheet_name: str) -> str:
    # Sheets ranges: 'My Sheet'!A1:B2; escape single quotes
    nm = (sheet_name or "").strip().replace("'", "''")
    return f"'{nm}'"


def _norm_cell(x: Any) -> str:
    if x is None:
        return ""
    return str(x).strip()


def _norm_header(x: Any) -> str:
    # uppercase + collapse spaces/underscores for robust matching
    s = _norm_cell(x).upper()
    s = s.replace("_", " ")
    s = " ".join(s.split())
    return s


def _dedupe_preserve(items: List[str]) -> List[str]:
    out: List[str] = []
    seen = set()
    for s in items:
        k = s.upper()
        if k in seen:
            continue
        seen.add(k)
        out.append(s)
    return out


def _normalize_symbol(raw: Any) -> str:
    """
    Normalize symbol text WITHOUT being overly destructive.
    - trims, uppercases, removes spaces
    - KSA numeric -> ####.SR
    - strips TADAWUL prefixes/suffixes
    """
    s = _norm_cell(raw).upper()
    if not s:
        return ""

    # Remove spaces (inside)
    s = s.replace(" ", "")

    # Strip common KSA decorations
    if s.startswith("TADAWUL:"):
        s = s.split(":", 1)[1] or s

    if s.endswith(".TADAWUL"):
        s = s[:-7]  # remove ".TADAWUL"

    # KSA numeric
    if _KSA_NUM_RE.match(s):
        return f"{s}.SR"

    # KSA already
    if s.endswith(".SR"):
        base = s[:-3]
        return s if base.isdigit() else s

    return s


def _looks_like_symbol(s: str) -> bool:
    if not s:
        return False
    if s in {"-", "N/A", "NA", "NULL", "NONE"}:
        return False
    if s.upper() in {"SYMBOL", "TICKER", "TICKERS", "CODE"}:
        return False

    # Accept if it matches typical patterns
    if _SYMBOL_LIKE_RE.match(s):
        return True

    # Extra allowance for some FX styles like "EURUSD" without "=X"
    if len(s) == 6 and s.isalpha():
        return True

    return False


def _require_spreadsheet_id(spreadsheet_id: str) -> str:
    sid = (spreadsheet_id or getattr(settings, "default_spreadsheet_id", "") or "").strip()
    if not sid:
        raise ValueError("No Spreadsheet ID configured (DEFAULT_SPREADSHEET_ID missing).")
    return sid


# -----------------------------------------------------------------------------
# Column detection
# -----------------------------------------------------------------------------
_HEADER_ALIASES = [
    # exact-ish
    "SYMBOL", "TICKER", "TICKER SYMBOL", "SYMBOL/TICKER",
    "STOCK", "STOCK CODE", "CODE", "ASSET", "SECURITY", "INSTRUMENT",
    # common in your sheets
    "SYMBOL (TICKER)", "SYMBOL(TICKER)", "SYMBOL CODE",
]


def _read_header_row(sid: str, sheet_name: str, header_row: int, max_cols: int) -> List[str]:
    last_col = _col_idx_to_a1(max_cols - 1)
    rng = f"{_safe_sheet_name(sheet_name)}!A{header_row}:{last_col}{header_row}"
    rows = read_range(sid, rng) or []
    if not rows or not rows[0]:
        return []
    return [_norm_header(h) for h in rows[0]]


def _find_symbol_column_by_header(headers: List[str]) -> Optional[int]:
    if not headers:
        return None

    # direct match
    for alias in _HEADER_ALIASES:
        a = _norm_header(alias)
        for i, h in enumerate(headers):
            if h == a:
                return i

    # contains match (e.g., "Ticker (Symbol)" or "Symbol - Global")
    for i, h in enumerate(headers):
        if "SYMBOL" in h or "TICKER" in h:
            return i

    return None


def _score_column_as_symbol(
    sid: str,
    sheet_name: str,
    col_idx: int,
    header_row: int,
    scan_rows: int,
) -> float:
    """
    Heuristic scorer:
    - reads up to scan_rows cells from that column (below header)
    - score = % of non-empty cells that look like symbols (weighted)
    """
    col_letter = _col_idx_to_a1(col_idx)
    start = header_row + 1
    end = header_row + scan_rows
    rng = f"{_safe_sheet_name(sheet_name)}!{col_letter}{start}:{col_letter}{end}"

    try:
        values = read_range(sid, rng) or []
    except Exception:
        return 0.0

    non_empty = 0
    looks = 0
    for r in values:
        if not r:
            continue
        v = _normalize_symbol(r[0])
        if not v:
            continue
        non_empty += 1
        if _looks_like_symbol(v):
            looks += 1

    if non_empty == 0:
        return 0.0

    ratio = looks / non_empty

    # bonus if many are KSA numeric->.SR or end with .SR (common in KSA sheet)
    ksa_like = 0
    for r in values[: min(len(values), 200)]:
        if not r:
            continue
        v = _normalize_symbol(r[0])
        if v.endswith(".SR"):
            ksa_like += 1

    bonus = min(0.10, ksa_like / max(1, min(len(values), 200)) * 0.10)
    return float(ratio + bonus)


def _find_symbol_column(
    sid: str,
    sheet_name: str,
    header_row: int,
    max_cols: int,
) -> Tuple[Optional[int], Dict[str, Any]]:
    """
    Returns (col_idx, meta)
    """
    meta: Dict[str, Any] = {"method": None, "header_row_used": header_row}

    # 1) Header-based detection (try multiple header rows)
    for hr in (header_row, *[r for r in _HEADER_ROWS_TO_TRY if r != header_row]):
        headers = _read_header_row(sid, sheet_name, hr, max_cols)
        if not headers:
            continue

        idx = _find_symbol_column_by_header(headers)
        if idx is not None:
            meta.update({"method": "header", "header_row_used": hr, "header_sample": headers[:15], "col_idx": idx})
            return idx, meta

    # 2) Heuristic scoring across first N columns
    best_idx: Optional[int] = None
    best_score: float = 0.0
    scores: List[Tuple[int, float]] = []

    for i in range(0, max_cols):
        sc = _score_column_as_symbol(sid, sheet_name, i, header_row, scan_rows=min(_MAX_DATA_ROWS_SCAN, 500))
        scores.append((i, sc))
        if sc > best_score:
            best_score = sc
            best_idx = i

    scores_sorted = sorted(scores, key=lambda x: x[1], reverse=True)[:5]
    meta.update({"method": "heuristic", "best_score": best_score, "top_scores": scores_sorted, "col_idx": best_idx})

    # accept only if convincing
    if best_idx is not None and best_score >= 0.55:
        return best_idx, meta

    return None, meta


# -----------------------------------------------------------------------------
# Public functions
# -----------------------------------------------------------------------------
def get_symbols_from_sheet(
    spreadsheet_id: str,
    sheet_name: str,
    header_row: int = 5,
    max_cols: int = _MAX_COLS_SCAN,
    scan_rows: int = _MAX_DATA_ROWS_SCAN,
) -> Dict[str, Any]:
    """
    Returns:
      {
        "all": [...],
        "ksa": [...],
        "global": [...],
        "meta": {...}
      }
    """
    try:
        sid = _require_spreadsheet_id(spreadsheet_id)
    except Exception as e:
        logger.error("[SymbolsReader] %s", e)
        return {"all": [], "ksa": [], "global": [], "meta": {"status": "error", "error": str(e)}}

    sh = (sheet_name or "").strip()
    if not sh:
        return {"all": [], "ksa": [], "global": [], "meta": {"status": "error", "error": "sheet_name is required"}}

    col_idx, detect_meta = _find_symbol_column(sid, sh, header_row, max_cols)
    target_col = col_idx if col_idx is not None else 0
    col_letter = _col_idx_to_a1(target_col)

    # Finite range (avoid open-ended A:A which may behave differently across APIs)
    start = header_row + 1
    end = header_row + scan_rows
    data_rng = f"{_safe_sheet_name(sh)}!{col_letter}{start}:{col_letter}{end}"

    try:
        raw_rows = read_range(sid, data_rng) or []
    except Exception as e:
        logger.error("[SymbolsReader] Failed reading %s: %s", data_rng, e)
        return {"all": [], "ksa": [], "global": [], "meta": {"status": "error", "error": str(e), **detect_meta}}

    symbols: List[str] = []
    for row in raw_rows:
        if not row:
            continue
        val = _normalize_symbol(row[0])

        if not val:
            continue
        if not _looks_like_symbol(val):
            continue

        symbols.append(val)

    symbols = _dedupe_preserve(symbols)

    split = split_tickers_by_market(symbols)
    meta: Dict[str, Any] = {
        "status": "success",
        "version": SYMBOLS_READER_VERSION,
        "sheet_name": sh,
        "header_row": header_row,
        "symbol_col": col_letter,
        "symbol_col_idx": target_col,
        "count_all": len(symbols),
        "count_ksa": len(split.get("ksa", []) or []),
        "count_global": len(split.get("global", []) or []),
        **detect_meta,
    }

    return {"all": symbols, "ksa": split.get("ksa", []), "global": split.get("global", []), "meta": meta}


def get_page_symbols(page_key: str, spreadsheet_id: Optional[str] = None) -> Dict[str, Any]:
    cfg = PAGE_REGISTRY.get((page_key or "").strip().upper())
    if not cfg:
        err = f"Invalid page key: {page_key}"
        logger.error("[SymbolsReader] %s", err)
        return {"all": [], "ksa": [], "global": [], "meta": {"status": "error", "error": err}}

    return get_symbols_from_sheet(
        spreadsheet_id or "",
        cfg.sheet_name,
        header_row=cfg.header_row,
        max_cols=cfg.max_columns,
        scan_rows=_MAX_DATA_ROWS_SCAN,
    )


def get_all_pages_symbols(spreadsheet_id: Optional[str] = None) -> Dict[str, Dict[str, Any]]:
    out: Dict[str, Dict[str, Any]] = {}
    for key in PAGE_REGISTRY.keys():
        out[key] = get_page_symbols(key, spreadsheet_id)
    return out


__all__ = [
    "PageConfig",
    "PAGE_REGISTRY",
    "get_symbols_from_sheet",
    "get_page_symbols",
    "get_all_pages_symbols",
]
