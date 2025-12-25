```python
# symbols_reader.py  (FULL REPLACEMENT)
"""
symbols_reader.py
===========================================================
Central symbol reader for ALL dashboard Google Sheets pages.
(v3.3.0) — Production-hardened, KSA-safe, Sheets-service aligned

Key upgrades (v3.3.0)
- FIX: Avoids calling split_tickers_by_market from google_sheets_service (may not exist)
  -> provides safe local fallback splitter (still tries import if available)
- PERFORMANCE: Heuristic column detection uses ONE block read (A..AZ sample rows),
  instead of 50+ per-column API calls
- Keeps your normalization + de-dup + consistent output: {all, ksa, global, meta}

Dependencies
- google_sheets_service.read_range
"""

from __future__ import annotations

import logging
import re
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple

logger = logging.getLogger("symbols_reader")

SYMBOLS_READER_VERSION = "3.3.0"

# How much to scan (keep modest to avoid API cost)
_MAX_COLS_SCAN = 52          # A..AZ
_MAX_DATA_ROWS_SCAN = 2000   # final extraction rows
_SCORE_SAMPLE_ROWS = 300     # heuristic scoring rows (fast + enough)
_HEADER_ROWS_TO_TRY = (5, 4, 3)

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

from google_sheets_service import read_range  # type: ignore

# Try optional splitter from google_sheets_service if present; fallback locally.
try:
    from google_sheets_service import split_tickers_by_market as _split_tickers_by_market  # type: ignore
except Exception:
    _split_tickers_by_market = None


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
# Helpers
# -----------------------------------------------------------------------------
_A1_RE = re.compile(r"^\$?([A-Za-z]+)\$?(\d+)$")
_KSA_NUM_RE = re.compile(r"^\d{3,5}$")

# allows: AAPL, BRK.B, BTC-USD, EURUSD=X, GC=F, ^GSPC, 1120.SR
_SYMBOL_LIKE_RE = re.compile(r"^(?:\^?[A-Z0-9][A-Z0-9\.\-=_]{0,24})(?:\.SR|\.TADAWUL)?$", re.IGNORECASE)

_HEADER_ALIASES = [
    "SYMBOL",
    "SYMBOLS",
    "TICKER",
    "TICKERS",
    "TICKER SYMBOL",
    "SYMBOL/TICKER",
    "STOCK",
    "STOCK CODE",
    "CODE",
    "ASSET",
    "SECURITY",
    "INSTRUMENT",
    "SYMBOL (TICKER)",
    "SYMBOL(TICKER)",
    "SYMBOL CODE",
]


def _col_idx_to_a1(n: int) -> str:
    """0 -> A, 25 -> Z, 26 -> AA"""
    s = ""
    n += 1
    while n > 0:
        n, r = divmod(n - 1, 26)
        s = chr(65 + r) + s
    return s


def _safe_sheet_name(sheet_name: str) -> str:
    nm = (sheet_name or "").strip().replace("'", "''")
    return f"'{nm}'"


def _norm_cell(x: Any) -> str:
    if x is None:
        return ""
    return str(x).strip()


def _norm_header(x: Any) -> str:
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
    s = _norm_cell(raw).upper()
    if not s:
        return ""
    s = s.replace(" ", "")

    if s.startswith("TADAWUL:"):
        s = s.split(":", 1)[1] or s

    if s.endswith(".TADAWUL"):
        s = s[:-7]

    if _KSA_NUM_RE.match(s):
        return f"{s}.SR"

    return s


def _looks_like_symbol(s: str) -> bool:
    if not s:
        return False
    su = s.upper()
    if su in {"-", "N/A", "NA", "NULL", "NONE"}:
        return False
    if su in {"SYMBOL", "SYMBOLS", "TICKER", "TICKERS", "CODE"}:
        return False

    if _SYMBOL_LIKE_RE.match(s):
        return True

    # FX style without "=X" (EURUSD)
    if len(s) == 6 and s.isalpha():
        return True

    return False


def _require_spreadsheet_id(spreadsheet_id: str) -> str:
    sid = (spreadsheet_id or getattr(settings, "default_spreadsheet_id", "") or "").strip()
    if not sid:
        raise ValueError("No Spreadsheet ID configured (DEFAULT_SPREADSHEET_ID missing).")
    return sid


def _split_market_fallback(tickers: List[str]) -> Dict[str, List[str]]:
    ksa: List[str] = []
    glob: List[str] = []
    for t in tickers:
        u = (t or "").upper()
        if u.endswith(".SR") and u[:-3].isdigit():
            ksa.append(t)
        else:
            glob.append(t)
    return {"ksa": ksa, "global": glob}


def split_tickers_by_market(tickers: List[str]) -> Dict[str, List[str]]:
    """
    Uses google_sheets_service.split_tickers_by_market if available,
    otherwise falls back to local logic.
    """
    if callable(_split_tickers_by_market):
        try:
            out = _split_tickers_by_market(tickers)  # type: ignore
            if isinstance(out, dict) and ("ksa" in out or "global" in out):
                out.setdefault("ksa", [])
                out.setdefault("global", [])
                return {"ksa": list(out.get("ksa") or []), "global": list(out.get("global") or [])}
        except Exception:
            pass
    return _split_market_fallback(tickers)


# -----------------------------------------------------------------------------
# Column detection (fast)
# -----------------------------------------------------------------------------
def _read_row(sid: str, sheet_name: str, row: int, max_cols: int) -> List[str]:
    last_col = _col_idx_to_a1(max_cols - 1)
    rng = f"{_safe_sheet_name(sheet_name)}!A{row}:{last_col}{row}"
    rows = read_range(sid, rng) or []
    if not rows or not rows[0]:
        return []
    return [_norm_header(h) for h in rows[0]]


def _find_symbol_column_by_header(headers: List[str]) -> Optional[int]:
    if not headers:
        return None

    # direct alias match
    aliases = {_norm_header(a) for a in _HEADER_ALIASES}
    for i, h in enumerate(headers):
        if h in aliases:
            return i

    # contains match
    for i, h in enumerate(headers):
        if "SYMBOL" in h or "TICKER" in h:
            return i

    return None


def _read_block(
    sid: str,
    sheet_name: str,
    start_row: int,
    end_row: int,
    max_cols: int,
) -> List[List[str]]:
    last_col = _col_idx_to_a1(max_cols - 1)
    rng = f"{_safe_sheet_name(sheet_name)}!A{start_row}:{last_col}{end_row}"
    raw = read_range(sid, rng) or []
    out: List[List[str]] = []
    for r in raw:
        rr = [(_norm_cell(x)) for x in (r or [])]
        if len(rr) < max_cols:
            rr += [""] * (max_cols - len(rr))
        else:
            rr = rr[:max_cols]
        out.append(rr)
    # pad missing rows (rare)
    need = max(0, (end_row - start_row + 1) - len(out))
    for _ in range(need):
        out.append([""] * max_cols)
    return out


def _score_columns_from_block(block: List[List[str]]) -> Tuple[Optional[int], float, List[Tuple[int, float]]]:
    """
    block: rows x cols, already normalized to fixed col count.
    Returns (best_idx, best_score, top5_scores)
    """
    if not block:
        return None, 0.0, []

    cols = len(block[0]) if block[0] else 0
    if cols <= 0:
        return None, 0.0, []

    scores: List[Tuple[int, float]] = []
    for c in range(cols):
        non_empty = 0
        looks = 0
        ksa_like = 0

        for r in block:
            v = _normalize_symbol(r[c]) if c < len(r) else ""
            if not v:
                continue
            non_empty += 1
            if _looks_like_symbol(v):
                looks += 1
            if v.endswith(".SR"):
                ksa_like += 1

        if non_empty == 0:
            sc = 0.0
        else:
            ratio = looks / non_empty
            bonus = min(0.10, (ksa_like / non_empty) * 0.10)
            sc = float(ratio + bonus)

        scores.append((c, sc))

    scores_sorted = sorted(scores, key=lambda x: x[1], reverse=True)
    best_idx, best_score = scores_sorted[0]
    return best_idx, float(best_score), scores_sorted[:5]


def _find_symbol_column(
    sid: str,
    sheet_name: str,
    header_row: int,
    max_cols: int,
) -> Tuple[Optional[int], Dict[str, Any]]:
    meta: Dict[str, Any] = {"method": None, "header_row_used": header_row}

    # 1) Header-based (try multiple header rows)
    for hr in (header_row, *[r for r in _HEADER_ROWS_TO_TRY if r != header_row]):
        headers = _read_row(sid, sheet_name, hr, max_cols)
        if not headers:
            continue
        idx = _find_symbol_column_by_header(headers)
        if idx is not None:
            meta.update(
                {"method": "header", "header_row_used": hr, "header_sample": headers[:15], "col_idx": idx}
            )
            return idx, meta

    # 2) Heuristic scoring (ONE block read)
    start = header_row + 1
    end = header_row + max(5, _SCORE_SAMPLE_ROWS)

    try:
        block = _read_block(sid, sheet_name, start, end, max_cols)
    except Exception as e:
        meta.update({"method": "heuristic", "error": str(e)})
        return None, meta

    best_idx, best_score, top5 = _score_columns_from_block(block)
    meta.update({"method": "heuristic", "best_score": best_score, "top_scores": top5, "col_idx": best_idx})

    # accept only if convincing
    if best_idx is not None and best_score >= 0.55:
        return best_idx, meta

    return None, meta


# -----------------------------------------------------------------------------
# Public API
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

    # Finite range (avoid open-ended A:A)
    start = header_row + 1
    end = header_row + int(max(1, scan_rows))
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
    "split_tickers_by_market",
    "get_symbols_from_sheet",
    "get_page_symbols",
    "get_all_pages_symbols",
]
```
