#!/usr/bin/env python3
# symbols_reader.py
"""
TADAWUL FAST BRIDGE – SYMBOLS READER (v3.2.0) – PROD SAFE
=========================================================
ADVANCED INTELLIGENT EDITION (FULL REPLACEMENT)

Purpose
- Single stable API to load symbols/tickers from Google Sheets dashboard tabs.
- Used by backend scripts (run_dashboard_sync.py, run_market_scan.py, etc.).

Key Enhancements (v3.2.0)
- ✅ Ultra-robust credentials loader:
      - raw JSON, Base64 JSON, quoted JSON, and escaped private_key repair
      - supports GOOGLE_SHEETS_CREDENTIALS / GOOGLE_CREDENTIALS / GOOGLE_APPLICATION_CREDENTIALS (file)
- ✅ Adaptive column detection:
      - Header-based (Symbol/Ticker/Code/Stock) with fuzzy matching
      - Data-based fallback scan (find the column with the highest “ticker-likeness”)
- ✅ Better normalization:
      - Uses core.symbols.normalize if available
      - Safe fallback normalization for KSA/Global parity
- ✅ Composite caching:
      - TTL cache keyed by (spreadsheet_id, sheet_key, header_row/start_row/max_rows)
      - optional cache disable via env
- ✅ Observability:
      - metadata includes strategy used, column selected, counts, durations, cache hit
- ✅ Registry flexibility:
      - aliases + multiple candidate sheet names per page
      - easy to extend without touching core logic

Environment (aligned)
- DEFAULT_SPREADSHEET_ID / SPREADSHEET_ID
- GOOGLE_SHEETS_CREDENTIALS / GOOGLE_CREDENTIALS / GOOGLE_APPLICATION_CREDENTIALS
- TFB_SYMBOL_HEADER_ROW (default 5)
- TFB_SYMBOL_START_ROW  (default 6)
- TFB_SYMBOL_MAX_ROWS   (default 5000)
- TFB_SYMBOLS_CACHE_TTL_SEC (default 45)
- TFB_SYMBOLS_CACHE_DISABLE (1/true disables)

Public API
- get_page_symbols(key, spreadsheet_id=None) -> dict
- list_tabs(spreadsheet_id) -> list[str]
- get_universe(keys, spreadsheet_id=None) -> dict (symbols + origin_map)
"""

from __future__ import annotations

import base64
import json
import logging
import os
import re
import threading
import time
from dataclasses import dataclass
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple

# -----------------------------------------------------------------------------
# Version / Constants
# -----------------------------------------------------------------------------
VERSION = "3.2.0"

logger = logging.getLogger("symbols_reader")

_TRUTHY = {"1", "true", "yes", "y", "on", "t", "enable", "enabled", "ok"}
_FALSY = {"0", "false", "no", "n", "off", "f", "disable", "disabled"}

# Header candidates (fuzzy)
_DEFAULT_HEADER_CANDIDATES = (
    "SYMBOL", "TICKER", "CODE", "STOCK", "SECURITY", "INSTRUMENT"
)

# Common false-positives / non-symbol words you want to ignore
_BLOCKLIST_EXACT = {
    "SYMBOL", "TICKER", "CODE", "STOCK", "NAME", "RANK", "MARKET", "CURRENCY",
    "PRICE", "LAST", "UPDATED", "LASTUPDATED", "DATE", "TIME", "NOTES"
}

# -----------------------------------------------------------------------------
# Safe Env parsing
# -----------------------------------------------------------------------------
def _strip(v: Any) -> str:
    try:
        return str(v).strip()
    except Exception:
        return ""

def _safe_int(v: Any, default: int) -> int:
    try:
        x = int(float(_strip(v)))
        return x if x > 0 else default
    except Exception:
        return default

def _safe_float(v: Any, default: float) -> float:
    try:
        x = float(_strip(v))
        return x if x > 0 else default
    except Exception:
        return default

def _safe_bool(v: Any, default: bool) -> bool:
    s = _strip(v).lower()
    if not s:
        return default
    if s in _TRUTHY:
        return True
    if s in _FALSY:
        return False
    return default

_DEFAULT_HEADER_ROW = _safe_int(os.getenv("TFB_SYMBOL_HEADER_ROW", "5"), 5)
_DEFAULT_START_ROW = _safe_int(os.getenv("TFB_SYMBOL_START_ROW", "6"), 6)
_DEFAULT_MAX_ROWS = _safe_int(os.getenv("TFB_SYMBOL_MAX_ROWS", "5000"), 5000)

_CACHE_TTL_SEC = _safe_float(os.getenv("TFB_SYMBOLS_CACHE_TTL_SEC", "45"), 45.0)
_CACHE_DISABLE = _safe_bool(os.getenv("TFB_SYMBOLS_CACHE_DISABLE", ""), False)

# -----------------------------------------------------------------------------
# Best-effort Google libraries
# -----------------------------------------------------------------------------
_GOOGLE_OK = False
_Credentials = None
_build = None
try:
    from google.oauth2.service_account import Credentials as _Credentials  # type: ignore
    from googleapiclient.discovery import build as _build  # type: ignore
    _GOOGLE_OK = True
except Exception:
    _GOOGLE_OK = False

# -----------------------------------------------------------------------------
# Cache (TTL) - keyed by spreadsheet_id + page_key + params
# -----------------------------------------------------------------------------
_cache: Dict[str, Tuple[float, Any]] = {}
_cache_lock = threading.Lock()

def _cache_get(key: str) -> Optional[Any]:
    if _CACHE_DISABLE:
        return None
    with _cache_lock:
        it = _cache.get(key)
        if not it:
            return None
        ts, val = it
        if (time.time() - ts) <= _CACHE_TTL_SEC:
            return val
        # expire
        _cache.pop(key, None)
        return None

def _cache_set(key: str, val: Any) -> None:
    if _CACHE_DISABLE:
        return
    with _cache_lock:
        _cache[key] = (time.time(), val)

# -----------------------------------------------------------------------------
# Normalization (optional shared module)
# -----------------------------------------------------------------------------
def _get_normalizer():
    """
    Try shared normalization logic from core/symbols/normalize.py
    Expected exports:
      - normalize_symbol(sym: str) -> str
      - is_ksa(sym: str) -> bool
    """
    try:
        from core.symbols.normalize import normalize_symbol, is_ksa  # type: ignore
        return normalize_symbol, is_ksa
    except Exception:
        return None, None

def _fallback_normalize(sym: str) -> str:
    s = _strip(sym).upper()
    s = s.replace("’", "'").replace("`", "'")
    s = re.sub(r"\s+", "", s)
    # Remove surrounding quotes
    if len(s) >= 2 and ((s[0] == s[-1] == '"') or (s[0] == s[-1] == "'")):
        s = s[1:-1].strip()
    return s

def _fallback_is_ksa(sym: str) -> bool:
    s = _fallback_normalize(sym)
    # KSA frequently appears as 4 digits or digits.SR
    if re.fullmatch(r"\d{4}(\.SR)?", s):
        return True
    return s.endswith(".SR")

# -----------------------------------------------------------------------------
# Credentials: parse/repair JSON (raw, base64, quoted, file)
# -----------------------------------------------------------------------------
def _strip_wrapping_quotes(s: str) -> str:
    t = (s or "").strip()
    if len(t) >= 2 and ((t[0] == t[-1] == '"') or (t[0] == t[-1] == "'")):
        return t[1:-1].strip()
    return t

def _repair_private_key(pk: Any) -> Any:
    if not isinstance(pk, str):
        return pk
    # Common Render/ENV issue: private_key contains literal "\n"
    return pk.replace("\\n", "\n") if "\\n" in pk else pk

def _try_parse_json(text: str) -> Optional[Dict[str, Any]]:
    try:
        obj = json.loads(text)
        return obj if isinstance(obj, dict) else None
    except Exception:
        return None

def _decode_base64_maybe(s: str) -> str:
    """
    Best-effort: if it's base64, decode; otherwise return original.
    """
    t = _strip_wrapping_quotes(s)
    if not t or t.startswith("{"):
        return t
    # Heuristic: long-ish + base64-ish
    if len(t) < 50:
        return t
    try:
        dec = base64.b64decode(t, validate=False).decode("utf-8", errors="replace").strip()
        return dec if dec.startswith("{") else t
    except Exception:
        return t

def _load_creds_from_file(path: str) -> Optional[Dict[str, Any]]:
    try:
        with open(path, "r", encoding="utf-8") as f:
            return _try_parse_json(f.read())
    except Exception:
        return None

def _load_service_account_info() -> Optional[Dict[str, Any]]:
    """
    Priority:
    1) GOOGLE_SHEETS_CREDENTIALS (raw/base64/quoted JSON)
    2) GOOGLE_CREDENTIALS (raw/base64/quoted JSON)
    3) GOOGLE_APPLICATION_CREDENTIALS (file path)
    """
    raw = (os.getenv("GOOGLE_SHEETS_CREDENTIALS") or "").strip()
    if raw:
        t = _decode_base64_maybe(raw)
        obj = _try_parse_json(t)
        if obj:
            if "private_key" in obj:
                obj["private_key"] = _repair_private_key(obj["private_key"])
            return obj

    raw = (os.getenv("GOOGLE_CREDENTIALS") or "").strip()
    if raw:
        t = _decode_base64_maybe(raw)
        obj = _try_parse_json(t)
        if obj:
            if "private_key" in obj:
                obj["private_key"] = _repair_private_key(obj["private_key"])
            return obj

    fpath = (os.getenv("GOOGLE_APPLICATION_CREDENTIALS") or "").strip()
    if fpath and os.path.exists(fpath):
        obj = _load_creds_from_file(fpath)
        if obj and "private_key" in obj:
            obj["private_key"] = _repair_private_key(obj["private_key"])
        return obj

    return None

# -----------------------------------------------------------------------------
# Registry
# -----------------------------------------------------------------------------
@dataclass(frozen=True)
class PageSpec:
    key: str
    sheet_names: Tuple[str, ...]
    header_row: int = _DEFAULT_HEADER_ROW
    start_row: int = _DEFAULT_START_ROW
    max_rows: int = _DEFAULT_MAX_ROWS
    header_candidates: Tuple[str, ...] = _DEFAULT_HEADER_CANDIDATES

PAGE_REGISTRY: Dict[str, PageSpec] = {
    "MARKET_LEADERS": PageSpec("MARKET_LEADERS", ("Market_Leaders", "Market Leaders")),
    "GLOBAL_MARKETS": PageSpec("GLOBAL_MARKETS", ("Global_Markets", "Global Markets")),
    "KSA_TADAWUL": PageSpec("KSA_TADAWUL", ("KSA_Tadawul", "KSA Tadawul", "KSA_Tadawul")),
    "MUTUAL_FUNDS": PageSpec("MUTUAL_FUNDS", ("Mutual_Funds", "Mutual Funds")),
    "COMMODITIES_FX": PageSpec("COMMODITIES_FX", ("Commodities_FX", "Commodities & FX")),
    "MY_PORTFOLIO": PageSpec("MY_PORTFOLIO", ("My_Portfolio", "My Portfolio")),
    "INSIGHTS_ANALYSIS": PageSpec("INSIGHTS_ANALYSIS", ("Insights_Analysis", "Insights Analysis")),
    "INVESTMENT_ADVISOR": PageSpec("INVESTMENT_ADVISOR", ("Investment_Advisor", "Investment Advisor")),
    "MARKET_SCAN": PageSpec("MARKET_SCAN", ("Market_Scan", "Market Scan")),
}

def _resolve_key(key: str) -> str:
    k = _strip(key).upper().replace("-", "_").replace(" ", "_")
    aliases = {
        "KSA": "KSA_TADAWUL",
        "TADAWUL": "KSA_TADAWUL",
        "TASI": "KSA_TADAWUL",
        "GLOBAL": "GLOBAL_MARKETS",
        "LEADERS": "MARKET_LEADERS",
        "PORTFOLIO": "MY_PORTFOLIO",
        "INSIGHTS": "INSIGHTS_ANALYSIS",
        "ADVISOR": "INVESTMENT_ADVISOR",
        "SCAN": "MARKET_SCAN",
    }
    return aliases.get(k, k)

# -----------------------------------------------------------------------------
# Google Sheets service (singleton, thread-safe)
# -----------------------------------------------------------------------------
_svc_lock = threading.Lock()
_svc = None

def _get_sheets_service():
    """
    Prefer local project google_sheets_service (shared auth/cache).
    Fallback to direct googleapiclient build if available.
    """
    global _svc
    if _svc is not None:
        return _svc

    with _svc_lock:
        if _svc is not None:
            return _svc

        # 1) Try project service to stay aligned with your stack
        try:
            import google_sheets_service as project_sheets  # type: ignore
            if hasattr(project_sheets, "get_sheets_service"):
                _svc = project_sheets.get_sheets_service()
                return _svc
        except Exception:
            pass

        # 2) Direct build
        if not _GOOGLE_OK:
            _svc = None
            return None

        info = _load_service_account_info()
        if not info:
            _svc = None
            return None

        try:
            scopes = ["https://www.googleapis.com/auth/spreadsheets.readonly"]
            creds = _Credentials.from_service_account_info(info, scopes=scopes)  # type: ignore
            _svc = _build("sheets", "v4", credentials=creds, cache_discovery=False)  # type: ignore
            return _svc
        except Exception as e:
            logger.error("Sheets Service Init Error: %s", e)
            _svc = None
            return None

# -----------------------------------------------------------------------------
# Low-level Sheets helpers
# -----------------------------------------------------------------------------
def _safe_sheet_name(name: str) -> str:
    n = _strip(name)
    if not n:
        return "Sheet1"
    if "'" in n or " " in n or "-" in n:
        return f"'{n.replace(chr(39), chr(39)*2)}'"
    return n

def _index_to_a1_col(idx1: int) -> str:
    idx = int(idx1)
    if idx <= 0:
        return "A"
    s = ""
    while idx > 0:
        idx, r = divmod(idx - 1, 26)
        s = chr(ord("A") + r) + s
    return s or "A"

def _read_values(spreadsheet_id: str, range_a1: str, retries: int = 2) -> List[List[Any]]:
    svc = _get_sheets_service()
    if not svc:
        return []

    last_err: Optional[Exception] = None
    for attempt in range(retries + 1):
        try:
            t0 = time.perf_counter()
            res = svc.spreadsheets().values().get(  # type: ignore
                spreadsheetId=spreadsheet_id,
                range=range_a1
            ).execute()
            _ = time.perf_counter() - t0
            return res.get("values", []) or []
        except Exception as e:
            last_err = e
            # light backoff
            time.sleep(0.25 * (attempt + 1))
            continue

    logger.warning("Sheets Read Error [%s]: %s", range_a1, last_err)
    return []

def list_tabs(spreadsheet_id: str) -> List[str]:
    svc = _get_sheets_service()
    if not svc:
        return []
    try:
        meta = svc.spreadsheets().get(spreadsheetId=spreadsheet_id).execute()  # type: ignore
        return [s["properties"]["title"] for s in meta.get("sheets", []) if s.get("properties", {}).get("title")]
    except Exception as e:
        logger.warning("list_tabs failed: %s", e)
        return []

# -----------------------------------------------------------------------------
# Symbol parsing / heuristics
# -----------------------------------------------------------------------------
_TICKER_TOKEN_RE = re.compile(r"^[A-Z0-9][A-Z0-9.\-_=]{0,20}$")
_KSA_NUMERIC_RE = re.compile(r"^\d{4}$")
_KSA_SUFFIX_RE = re.compile(r"^\d{4}\.SR$")

def _split_cell(cell: Any) -> List[str]:
    raw = _strip(cell)
    if not raw:
        return []
    # separators: commas/space/newline/semicolon/pipe
    parts = re.split(r"[\s,;|\n\r]+", raw)
    return [p.strip() for p in parts if p and p.strip()]

def _is_probable_ticker(token: str) -> bool:
    t = _fallback_normalize(token)
    if not t:
        return False
    if t in _BLOCKLIST_EXACT:
        return False
    # reject pure punctuation
    if re.fullmatch(r"[\W_]+", t):
        return False
    # accept: AAPL, 1120.SR, BTC-USD, ^GSPC? (caret not in our token regex)
    if t.startswith("^"):
        t2 = t[1:]
        return bool(_TICKER_TOKEN_RE.fullmatch(t2))
    return bool(_TICKER_TOKEN_RE.fullmatch(t))

def _normalize_and_dedupe(symbols: Iterable[str]) -> Tuple[List[str], List[str], List[str]]:
    norm_fn, is_ksa_fn = _get_normalizer()
    seen: set[str] = set()
    ksa: List[str] = []
    glob: List[str] = []

    for s in symbols:
        if not s:
            continue

        base = norm_fn(s) if norm_fn else _fallback_normalize(s)

        if not base:
            continue

        # Normalize KSA numeric to keep consistent form:
        # keep "1120" as "1120.SR"? (safe default for your stack)
        if _KSA_NUMERIC_RE.fullmatch(base):
            base = f"{base}.SR"
        elif _KSA_SUFFIX_RE.fullmatch(base):
            pass

        if base in seen:
            continue
        seen.add(base)

        is_sa = is_ksa_fn(base) if is_ksa_fn else _fallback_is_ksa(base)
        (ksa if is_sa else glob).append(base)

    all_syms = ksa + glob
    return all_syms, ksa, glob

# -----------------------------------------------------------------------------
# Column detection strategies
# -----------------------------------------------------------------------------
def _detect_col_by_header(spreadsheet_id: str, sheet_name: str, spec: PageSpec) -> Optional[str]:
    """
    Header-based detection (fast).
    """
    rng = f"{_safe_sheet_name(sheet_name)}!A{spec.header_row}:AZ{spec.header_row}"
    header_rows = _read_values(spreadsheet_id, rng)
    if not header_rows:
        return None

    headers = [(_strip(h).upper()) for h in (header_rows[0] or [])]
    if not headers:
        return None

    # Fuzzy match: contains any candidate token
    for idx, h in enumerate(headers, 1):
        if not h:
            continue
        for cand in spec.header_candidates:
            if cand in h:
                return _index_to_a1_col(idx)

    # Common standard: column B often used
    if len(headers) >= 2:
        return "B"
    return None

def _detect_col_by_data(spreadsheet_id: str, sheet_name: str, spec: PageSpec) -> Optional[str]:
    """
    Data-based detection (slower, but robust).
    Reads a small window and picks the column with highest ticker-likeness ratio.
    """
    sample_rows = min(150, max(30, spec.max_rows // 40))
    end_row = spec.start_row + sample_rows

    rng = f"{_safe_sheet_name(sheet_name)}!A{spec.start_row}:AZ{end_row}"
    values = _read_values(spreadsheet_id, rng)
    if not values:
        return None

    # score per column
    col_scores: Dict[int, Tuple[int, int]] = {}  # col -> (hits, total_nonempty)
    max_cols = max((len(r) for r in values), default=0)
    if max_cols <= 0:
        return None

    for r in values:
        for c in range(max_cols):
            if c >= len(r):
                continue
            cell = r[c]
            if cell is None:
                continue
            cell_s = _strip(cell)
            if not cell_s:
                continue

            tokens = _split_cell(cell_s)
            total = 0
            hits = 0
            for tok in tokens:
                total += 1
                if _is_probable_ticker(tok):
                    hits += 1

            if total <= 0:
                continue
            h0, t0 = col_scores.get(c, (0, 0))
            col_scores[c] = (h0 + hits, t0 + total)

    if not col_scores:
        return None

    # choose best ratio with minimum evidence
    best_col = None
    best_ratio = 0.0
    for c, (hits, total) in col_scores.items():
        if total < 10:  # need enough evidence
            continue
        ratio = hits / float(total) if total else 0.0
        # slight preference for early columns (A..F) by adding a tiny tie-breaker
        ratio_adj = ratio + (0.0005 * max(0, 10 - c))
        if ratio_adj > best_ratio:
            best_ratio = ratio_adj
            best_col = c

    if best_col is None:
        return None
    return _index_to_a1_col(best_col + 1)

def _resolve_symbol_col(spreadsheet_id: str, sheet_name: str, spec: PageSpec) -> Tuple[str, str]:
    """
    Returns (column_letter, strategy).
    """
    col = _detect_col_by_header(spreadsheet_id, sheet_name, spec)
    if col:
        return col, "header"
    col = _detect_col_by_data(spreadsheet_id, sheet_name, spec)
    if col:
        return col, "data_scan"
    return "B", "default_B"

# -----------------------------------------------------------------------------
# Spec reader
# -----------------------------------------------------------------------------
def _read_from_spec(spreadsheet_id: str, spec: PageSpec) -> Tuple[List[str], Dict[str, Any]]:
    meta: Dict[str, Any] = {
        "key": spec.key,
        "header_row": spec.header_row,
        "start_row": spec.start_row,
        "max_rows": spec.max_rows,
        "status": "empty",
    }

    for name in spec.sheet_names:
        t0 = time.perf_counter()
        col, strategy = _resolve_symbol_col(spreadsheet_id, name, spec)
        end_row = spec.start_row + spec.max_rows

        range_a1 = f"{_safe_sheet_name(name)}!{col}{spec.start_row}:{col}{end_row}"
        values = _read_values(spreadsheet_id, range_a1)
        elapsed_ms = (time.perf_counter() - t0) * 1000.0

        if not values:
            continue

        found: List[str] = []
        for row in values:
            if not row:
                continue
            cell = row[0] if len(row) >= 1 else None
            if cell is None:
                continue
            for tok in _split_cell(cell):
                if _is_probable_ticker(tok):
                    found.append(tok)

        if found:
            meta.update({
                "sheet": name,
                "col": col,
                "strategy": strategy,
                "raw_count": len(found),
                "read_ms": round(elapsed_ms, 2),
                "status": "success",
            })
            return found, meta

    return [], meta

# -----------------------------------------------------------------------------
# Public API
# -----------------------------------------------------------------------------
def get_page_symbols(key: str, spreadsheet_id: Optional[str] = None) -> Dict[str, Any]:
    """
    Primary entry point.

    Returns:
    {
      "all": [...],
      "ksa": [...],
      "global": [...],
      "tickers": [...],  # alias to all
      "symbols": [...],  # alias to all
      "meta": {...}
    }
    """
    key_norm = _resolve_key(key)
    sid = _strip(spreadsheet_id) or _strip(os.getenv("DEFAULT_SPREADSHEET_ID") or os.getenv("SPREADSHEET_ID") or "")
    if not sid:
        return {"all": [], "meta": {"status": "error", "error": "No Spreadsheet ID"}}

    spec = PAGE_REGISTRY.get(key_norm)
    if not spec:
        return {"all": [], "meta": {"status": "error", "error": f"Unknown key: {key}"}}

    cache_key = f"syms::{sid}::{key_norm}::{spec.header_row}:{spec.start_row}:{spec.max_rows}"
    hit = _cache_get(cache_key)
    if hit:
        # mark cache hit in meta without mutating cached dict
        out = dict(hit)
        meta = dict(out.get("meta") or {})
        meta["cache_hit"] = True
        out["meta"] = meta
        return out

    t0 = time.perf_counter()
    raw_symbols, meta = _read_from_spec(sid, spec)
    all_syms, ksa, glob = _normalize_and_dedupe(raw_symbols)
    elapsed_ms = (time.perf_counter() - t0) * 1000.0

    meta = dict(meta)
    meta.update({
        "normalized_count": len(all_syms),
        "ksa_count": len(ksa),
        "global_count": len(glob),
        "total_ms": round(elapsed_ms, 2),
        "cache_hit": False,
        "version": VERSION,
    })

    result = {
        "all": all_syms,
        "ksa": ksa,
        "global": glob,
        "tickers": all_syms,
        "symbols": all_syms,
        "meta": meta,
    }
    _cache_set(cache_key, result)
    return result

def get_universe(keys: Sequence[str], spreadsheet_id: Optional[str] = None) -> Dict[str, Any]:
    """
    Aggregates symbols across multiple page keys.
    Returns:
      {
        "symbols": [...unique...],
        "origin_map": {SYM: "PAGE_KEY", ...first origin...},
        "meta": { per_key_meta ... }
      }
    """
    sid = _strip(spreadsheet_id) or _strip(os.getenv("DEFAULT_SPREADSHEET_ID") or os.getenv("SPREADSHEET_ID") or "")
    if not sid:
        return {"symbols": [], "origin_map": {}, "meta": {"status": "error", "error": "No Spreadsheet ID"}}

    origin_map: Dict[str, str] = {}
    merged: List[str] = []
    per_key: Dict[str, Any] = {}

    for k in keys:
        r = get_page_symbols(k, spreadsheet_id=sid)
        syms = r.get("all") or []
        per_key[_resolve_key(k)] = r.get("meta") or {}
        for s in syms:
            ss = _strip(s).upper()
            if not ss:
                continue
            if ss not in origin_map:
                origin_map[ss] = _resolve_key(k)
                merged.append(ss)

    # merged already unique by first-seen, but normalize KSA numeric cases are handled earlier
    return {"symbols": merged, "origin_map": origin_map, "meta": {"per_key": per_key, "count": len(merged)}}

# -----------------------------------------------------------------------------
# CLI (quick diagnostics)
# -----------------------------------------------------------------------------
if __name__ == "__main__":
    import argparse

    logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s", datefmt="%H:%M:%S")

    p = argparse.ArgumentParser(description="TFB Symbols Reader (advanced)")
    p.add_argument("--sheet-id", help="Spreadsheet ID override")
    p.add_argument("--key", default="KSA", help="Page key (KSA, GLOBAL, LEADERS, PORTFOLIO, etc.)")
    p.add_argument("--list-tabs", action="store_true", help="List tabs and exit")
    p.add_argument("--json", action="store_true", help="Print JSON output")
    args = p.parse_args()

    sid_cli = args.sheet_id or os.getenv("DEFAULT_SPREADSHEET_ID") or os.getenv("SPREADSHEET_ID") or ""

    if args.list_tabs:
        tabs = list_tabs(sid_cli) if sid_cli else []
        print("\n".join(tabs) if tabs else "(no tabs / no access)")
        raise SystemExit(0)

    res = get_page_symbols(args.key, spreadsheet_id=sid_cli if sid_cli else None)
    if args.json:
        print(json.dumps(res, indent=2, ensure_ascii=False))
    else:
        meta = res.get("meta") or {}
        print(f"symbols_reader v{VERSION}")
        print(f"key={_resolve_key(args.key)}  status={meta.get('status')}  sheet={meta.get('sheet')}  col={meta.get('col')}  strategy={meta.get('strategy')}")
        print(f"count={len(res.get('all') or [])}  ksa={len(res.get('ksa') or [])}  global={len(res.get('global') or [])}  total_ms={meta.get('total_ms')}")
        preview = (res.get("all") or [])[:20]
        print("preview:", ", ".join(preview))
