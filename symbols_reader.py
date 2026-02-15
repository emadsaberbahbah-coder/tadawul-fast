#!/usr/bin/env python3
# symbols_reader.py
"""
TADAWUL FAST BRIDGE – SYMBOLS READER (v2.7.0) – PROD SAFE
===========================================================
ADVANCED INTELLIGENT EDITION

Purpose
- A single, stable API to load symbols/tickers from your Google Sheets dashboard tabs.
- Used by backend scripts (e.g., scripts/run_dashboard_sync.py, scripts/run_market_scan.py).

v2.7.0 Enhancements:
- ✅ **Python 3.11.9 Optimized**: Uses modern typing and asynchronous-friendly patterns.
- ✅ **Smart Normalization**: Native integration with `core.symbols.normalize` for KSA/Global parity.
- ✅ **Adaptive Scanning**: Auto-detects ticker columns if standard headers are missing.
- ✅ **Credential Repair**: Auto-fixes common escaping issues in GOOGLE_SHEETS_CREDENTIALS.
- ✅ **Composite Caching**: Spreadsheet-specific cache keys prevent data pollution.
"""

from __future__ import annotations

import base64
import json
import logging
import os
import re
import time
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Sequence, Tuple

# -----------------------------------------------------------------------------
# Version / Constants
# -----------------------------------------------------------------------------
VERSION = "2.7.0"
_TRUTHY = {"1", "true", "yes", "y", "on", "t", "enable", "enabled", "ok"}

# Logging Setup
logger = logging.getLogger("symbols_reader")

# -----------------------------------------------------------------------------
# Lazy Import for Core Normalizer
# -----------------------------------------------------------------------------
def _get_normalizer():
    """Import shared normalization logic from core/symbols/normalize.py."""
    try:
        from core.symbols.normalize import normalize_symbol, is_ksa
        return normalize_symbol, is_ksa
    except ImportError:
        return None, None

# -----------------------------------------------------------------------------
# Safe Environment Parsing
# -----------------------------------------------------------------------------
def _safe_int(v: Any, default: int) -> int:
    try:
        x = int(float(str(v).strip()))
        return x if x > 0 else default
    except Exception:
        return default


def _safe_float(v: Any, default: float) -> float:
    try:
        x = float(str(v).strip())
        return x if x > 0 else default
    except Exception:
        return default


_DEFAULT_HEADER_ROW = _safe_int(os.getenv("TFB_SYMBOL_HEADER_ROW", "5"), 5)
_DEFAULT_START_ROW = _safe_int(os.getenv("TFB_SYMBOL_START_ROW", "6"), 6)
_DEFAULT_MAX_ROWS = _safe_int(os.getenv("TFB_SYMBOL_MAX_ROWS", "5000"), 5000)

# -----------------------------------------------------------------------------
# Best-effort Google Libraries
# -----------------------------------------------------------------------------
_GOOGLE_OK = False
_Credentials = None
_build = None
try:
    from google.oauth2.service_account import Credentials as _Credentials
    from googleapiclient.discovery import build as _build
    _GOOGLE_OK = True
except Exception:
    _GOOGLE_OK = False

# -----------------------------------------------------------------------------
# Caching System
# -----------------------------------------------------------------------------
_CACHE_TTL_SEC = _safe_float(os.getenv("TFB_SYMBOLS_CACHE_TTL_SEC", "45"), 45.0)
_cache: Dict[str, Tuple[float, Any]] = {}


def _cache_get(key: str) -> Optional[Any]:
    it = _cache.get(key)
    if not it:
        return None
    ts, val = it
    if (time.time() - ts) <= _CACHE_TTL_SEC:
        return val
    return None


def _cache_set(key: str, val: Any) -> None:
    _cache[key] = (time.time(), val)


# -----------------------------------------------------------------------------
# Credential & String Sanitization
# -----------------------------------------------------------------------------
def _strip_wrapping_quotes(s: str) -> str:
    t = (s or "").strip()
    if len(t) >= 2 and ((t[0] == t[-1] == '"') or (t[0] == t[-1] == "'")):
        return t[1:-1].strip()
    return t


def _validate_json_creds(raw: str) -> Optional[Dict[str, Any]]:
    """Advanced validator/repair for Service Account JSON."""
    t = _strip_wrapping_quotes(raw or "").strip()
    if not t: return None
    
    # Handle Base64 encoding
    if not t.startswith("{") and len(t) > 50:
        try:
            decoded = base64.b64decode(t, validate=False).decode("utf-8", errors="replace").strip()
            if decoded.startswith("{"): t = decoded
        except: pass

    try:
        obj = json.loads(t)
        if isinstance(obj, dict) and "private_key" in obj:
            # Auto-repair escaped newlines common in ENV strings
            pk = obj["private_key"]
            if isinstance(pk, str) and "\\n" in pk:
                obj["private_key"] = pk.replace("\\n", "\n")
            return obj
    except: pass
    return None


def _load_service_account_info() -> Optional[Dict[str, Any]]:
    for k in ("GOOGLE_SHEETS_CREDENTIALS", "GOOGLE_CREDENTIALS"):
        raw = (os.getenv(k) or "").strip()
        if raw:
            obj = _validate_json_creds(raw)
            if obj: return obj
    return None

# -----------------------------------------------------------------------------
# Registry & Spec
# -----------------------------------------------------------------------------
@dataclass(frozen=True)
class PageSpec:
    key: str
    sheet_names: Tuple[str, ...]
    header_row: int = _DEFAULT_HEADER_ROW
    start_row: int = _DEFAULT_START_ROW
    max_rows: int = _DEFAULT_MAX_ROWS
    header_candidates: Tuple[str, ...] = ("SYMBOL", "TICKER", "CODE", "STOCK")


PAGE_REGISTRY: Dict[str, PageSpec] = {
    "MARKET_LEADERS": PageSpec(key="MARKET_LEADERS", sheet_names=("Market_Leaders", "Market Leaders")),
    "GLOBAL_MARKETS": PageSpec(key="GLOBAL_MARKETS", sheet_names=("Global_Markets", "Global Markets")),
    "KSA_TADAWUL": PageSpec(key="KSA_TADAWUL", sheet_names=("KSA_Tadawul", "KSA Tadawul")),
    "MUTUAL_FUNDS": PageSpec(key="MUTUAL_FUNDS", sheet_names=("Mutual_Funds", "Mutual Funds")),
    "COMMODITIES_FX": PageSpec(key="COMMODITIES_FX", sheet_names=("Commodities_FX", "Commodities & FX")),
    "MY_PORTFOLIO": PageSpec(key="MY_PORTFOLIO", sheet_names=("My_Portfolio", "My Portfolio")),
    "INSIGHTS_ANALYSIS": PageSpec(key="INSIGHTS_ANALYSIS", sheet_names=("Insights_Analysis", "Insights Analysis")),
    "INVESTMENT_ADVISOR": PageSpec(key="INVESTMENT_ADVISOR", sheet_names=("Investment_Advisor", "Investment Advisor")),
}

def _resolve_key(key: str) -> str:
    k = str(key or "").strip().upper().replace("-", "_").replace(" ", "_")
    aliases = {
        "KSA": "KSA_TADAWUL", "TASI": "KSA_TADAWUL",
        "GLOBAL": "GLOBAL_MARKETS", "LEADERS": "MARKET_LEADERS",
        "PORTFOLIO": "MY_PORTFOLIO", "INSIGHTS": "INSIGHTS_ANALYSIS",
        "ADVISOR": "INVESTMENT_ADVISOR"
    }
    return aliases.get(k, k)

# -----------------------------------------------------------------------------
# Internal Logic Helpers
# -----------------------------------------------------------------------------
def _index_to_a1_col(idx: int) -> str:
    s = ""
    while idx > 0:
        idx, r = divmod(idx - 1, 26)
        s = chr(ord("A") + r) + s
    return s or "A"


def _safe_sheet_name(name: str) -> str:
    n = (name or "").strip()
    if "'" in n or " " in n or "-" in n:
        return f"'{n.replace(chr(39), chr(39)*2)}'"
    return n


def _split_cell_into_symbols(cell: str) -> List[str]:
    raw = str(cell or "").strip()
    if not raw or raw.upper() == "SYMBOL": return []
    # Handle common separators: comma, newline, semicolon, pipe
    return [s.strip() for s in re.split(r"[\s,;|\n\r]+", raw) if s.strip()]


def _classify_symbols(symbols: Sequence[str]) -> Tuple[List[str], List[str]]:
    norm_fn, is_ksa_fn = _get_normalizer()
    ksa, glob = [], []
    seen = set()

    for s in symbols:
        # Canonical resolution
        norm = norm_fn(s) if norm_fn else s.strip().upper()
        if not norm or norm in seen: continue
        seen.add(norm)

        is_saudi = is_ksa_fn(norm) if is_ksa_fn else (norm.endswith(".SR") or norm.isdigit())
        if is_saudi: ksa.append(norm)
        else: glob.append(norm)

    return ksa, glob

# -----------------------------------------------------------------------------
# Google Sheets Reader
# -----------------------------------------------------------------------------
_svc = None

def _get_sheets_service():
    global _svc
    if _svc: return _svc
    if not _GOOGLE_OK: return None
    info = _load_service_account_info()
    if not info: return None
    try:
        creds = _Credentials.from_service_account_info(info, scopes=["https://www.googleapis.com/auth/spreadsheets.readonly"])
        _svc = _build("sheets", "v4", credentials=creds, cache_discovery=False)
        return _svc
    except Exception as e:
        logger.error(f"Sheets Service Init Error: {e}")
        return None


def _read_values(spreadsheet_id: str, range_a1: str) -> List[List[Any]]:
    svc = _get_sheets_service()
    if not svc: return []
    try:
        res = svc.spreadsheets().values().get(spreadsheetId=spreadsheet_id, range=range_a1).execute()
        return res.get("values", [])
    except Exception as e:
        logger.warning(f"Sheets Read Error [{range_a1}]: {e}")
        return []


def _resolve_symbol_col(spreadsheet_id: str, sheet_name: str, spec: PageSpec) -> str:
    """Detects the 'Symbol' column automatically from the header row."""
    rng = f"{_safe_sheet_name(sheet_name)}!A{spec.header_row}:Z{spec.header_row}"
    header_rows = _read_values(spreadsheet_id, rng)
    if header_rows:
        headers = [str(h).strip().upper() for h in header_rows[0]]
        for idx, h in enumerate(headers, 1):
            if any(cand in h for cand in spec.header_candidates):
                return _index_to_a1_col(idx)
    return "B" # Default Standard


def _read_from_spec(spreadsheet_id: str, spec: PageSpec) -> Tuple[List[str], Dict[str, Any]]:
    meta = {"key": spec.key, "start_row": spec.start_row}
    
    for name in spec.sheet_names:
        # Detect Column
        col = _resolve_symbol_col(spreadsheet_id, name, spec)
        range_a1 = f"{_safe_sheet_name(name)}!{col}{spec.start_row}:{col}{spec.start_row + spec.max_rows}"
        
        values = _read_values(spreadsheet_id, range_a1)
        if not values: continue

        found = []
        for row in values:
            if row and str(row[0]).strip():
                found.extend(_split_cell_into_symbols(row[0]))
        
        if found:
            meta.update({"sheet": name, "col": col, "count": len(found), "status": "success"})
            return found, meta

    return [], {**meta, "status": "empty"}

# -----------------------------------------------------------------------------
# Public API
# -----------------------------------------------------------------------------
def get_page_symbols(key: str, spreadsheet_id: Optional[str] = None) -> Dict[str, Any]:
    """
    Primary Entry Point.
    Returns categorized symbols and metadata from the specified dashboard key.
    """
    key_norm = _resolve_key(key)
    sid = (spreadsheet_id or os.getenv("DEFAULT_SPREADSHEET_ID") or "").strip()
    
    if not sid:
        return {"all": [], "meta": {"error": "No Spreadsheet ID"}}

    # Cache Check
    cache_key = f"syms::{sid}::{key_norm}"
    hit = _cache_get(cache_key)
    if hit: return hit

    spec = PAGE_REGISTRY.get(key_norm)
    if not spec:
        return {"all": [], "meta": {"error": f"Unknown key: {key}"}}

    raw_symbols, meta = _read_from_spec(sid, spec)
    ksa, glob = _classify_symbols(raw_symbols)
    all_syms = ksa + glob

    result = {
        "all": all_syms,
        "ksa": ksa,
        "global": glob,
        "tickers": all_syms,
        "symbols": all_syms,
        "meta": meta
    }
    
    _cache_set(cache_key, result)
    return result

if __name__ == "__main__":
    # Quick Test
    logging.basicConfig(level=logging.INFO)
    test_key = "KSA"
    print(f"Testing symbols_reader v{VERSION} for key: {test_key}")
    res = get_page_symbols(test_key)
    print(json.dumps(res, indent=2))
