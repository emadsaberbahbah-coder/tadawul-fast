#!/usr/bin/env python3
# symbols_reader.py  (FULL REPLACEMENT)
"""
symbols_reader.py
===========================================================
TADAWUL FAST BRIDGE – SYMBOLS READER (v2.3.0) – PROD SAFE
===========================================================

Purpose
- Provide a single, stable API for scripts (like scripts/run_market_scan.py)
  to load symbols from your Google Sheets dashboard tabs.

Key guarantees
- ✅ Never makes network calls at import-time
- ✅ Never crashes startup (imports are best-effort)
- ✅ Returns a predictable shape:
      {"all":[...], "ksa":[...], "global":[...], "meta": {...}}
- ✅ Supports overrides via env (no Sheets needed)
- ✅ Google Sheets read via service account JSON in env:
      GOOGLE_SHEETS_CREDENTIALS  (minified JSON or base64 JSON)

Used by
- scripts/run_market_scan.py (expects PAGE_REGISTRY + get_page_symbols)

Env (optional)
- DEFAULT_SPREADSHEET_ID / TFB_SPREADSHEET_ID
- GOOGLE_SHEETS_CREDENTIALS (minified JSON OR base64(minified JSON))
- GOOGLE_APPLICATION_CREDENTIALS (path to service account json file)

Overrides (optional)
- SYMBOLS_JSON  (JSON object mapping keys -> list or dict with "all"/"ksa"/"global")
- SYMBOLS_<KEY> (comma-separated list for a specific key)
  e.g. SYMBOLS_MARKET_LEADERS="1120.SR,2222.SR,GOOGL,AAPL"
- TFB_SYMBOL_HEADER_ROW (default 5)
- TFB_SYMBOL_START_ROW  (default 6)
- TFB_SYMBOL_MAX_ROWS   (default 5000)
"""

from __future__ import annotations

import base64
import json
import os
import re
import time
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Sequence, Tuple

# -----------------------------------------------------------------------------
# Version / constants
# -----------------------------------------------------------------------------
VERSION = "2.3.0"
_DEFAULT_HEADER_ROW = int(os.getenv("TFB_SYMBOL_HEADER_ROW", "5") or "5")
_DEFAULT_START_ROW = int(os.getenv("TFB_SYMBOL_START_ROW", "6") or "6")
_DEFAULT_MAX_ROWS = int(os.getenv("TFB_SYMBOL_MAX_ROWS", "5000") or "5000")

_TRUTHY = {"1", "true", "yes", "y", "on", "t"}

# -----------------------------------------------------------------------------
# Best-effort Google libs
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
# Simple in-process cache (avoid hammering Sheets during scans)
# -----------------------------------------------------------------------------
_CACHE_TTL_SEC = float(os.getenv("TFB_SYMBOLS_CACHE_TTL_SEC", "45") or "45")
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
# Helpers
# -----------------------------------------------------------------------------
def _env_bool(name: str, default: bool = False) -> bool:
    v = os.getenv(name)
    if v is None:
        return default
    return str(v).strip().lower() in _TRUTHY


def _get_spreadsheet_id() -> str:
    return (
        (os.getenv("TFB_SPREADSHEET_ID") or "").strip()
        or (os.getenv("DEFAULT_SPREADSHEET_ID") or "").strip()
    )


def _strip_wrapping_quotes(s: str) -> str:
    t = (s or "").strip()
    if len(t) >= 2 and ((t[0] == t[-1] == '"') or (t[0] == t[-1] == "'")):
        return t[1:-1].strip()
    return t


def _maybe_b64_decode(s: str) -> str:
    raw = (s or "").strip()
    if not raw:
        return raw
    if raw.startswith("{"):
        return raw
    if len(raw) < 120:
        return raw
    try:
        dec = base64.b64decode(raw).decode("utf-8", errors="strict").strip()
        if dec.startswith("{") and '"private_key"' in dec:
            return dec
    except Exception:
        return raw
    return raw


def _load_service_account_info() -> Optional[Dict[str, Any]]:
    """
    Reads service account info from:
    - GOOGLE_SHEETS_CREDENTIALS (minified json OR base64 json)
    - else GOOGLE_APPLICATION_CREDENTIALS file path
    """
    raw = (os.getenv("GOOGLE_SHEETS_CREDENTIALS") or "").strip()
    if raw:
        raw = _strip_wrapping_quotes(raw)
        raw = _maybe_b64_decode(raw)
        raw = _strip_wrapping_quotes(raw)
        try:
            obj = json.loads(raw)
            if isinstance(obj, dict) and obj.get("private_key") and obj.get("client_email"):
                return obj
        except Exception:
            return None

    path = (os.getenv("GOOGLE_APPLICATION_CREDENTIALS") or "").strip()
    if path and os.path.exists(path):
        try:
            with open(path, "r", encoding="utf-8") as f:
                obj = json.load(f)
            if isinstance(obj, dict) and obj.get("private_key") and obj.get("client_email"):
                return obj
        except Exception:
            return None

    return None


def _a1_col_to_index(col: str) -> int:
    col = (col or "").strip().upper()
    n = 0
    for ch in col:
        if not ("A" <= ch <= "Z"):
            break
        n = n * 26 + (ord(ch) - ord("A") + 1)
    return n


def _index_to_a1_col(idx: int) -> str:
    if idx <= 0:
        return "A"
    s = ""
    n = idx
    while n > 0:
        n, r = divmod(n - 1, 26)
        s = chr(ord("A") + r) + s
    return s


def _safe_sheet_name(name: str) -> str:
    # For A1 ranges: sheet names with spaces should be quoted
    n = (name or "").strip()
    if not n:
        return "Sheet1"
    if any(ch in n for ch in (" ", "-", "(", ")", ".", ",")):
        return f"'{n}'"
    return n


def _normalize_symbol(s: str) -> str:
    x = (s or "").strip().upper().replace(" ", "")
    if not x:
        return ""
    if x.startswith("TADAWUL:"):
        x = x.split(":", 1)[1].strip().upper()
    if x.endswith(".TADAWUL"):
        x = x.replace(".TADAWUL", "")
    if x.isdigit() and 3 <= len(x) <= 6:
        return f"{x}.SR"
    return x


def _split_cell_into_symbols(cell: str) -> List[str]:
    """
    Accepts:
    - single symbol: "1120.SR"
    - comma separated: "AAPL,MSFT,GOOGL"
    - space separated (rare): "AAPL MSFT"
    - lines / bullets
    """
    raw = str(cell or "").strip()
    if not raw:
        return []
    # remove bullet-like prefixes
    raw = raw.replace("•", " ").replace("·", " ").replace("\t", " ")
    # split by comma / semicolon / whitespace / newlines
    parts = re.split(r"[,\n;\r]+|\s{2,}", raw)
    # also split if user used single spaces between tickers
    out: List[str] = []
    for p in parts:
        p = (p or "").strip()
        if not p:
            continue
        # if still contains spaces, split once more
        for q in re.split(r"\s+", p):
            q = (q or "").strip()
            if q:
                out.append(q)
    return out


def _dedupe(seq: Sequence[str]) -> List[str]:
    out: List[str] = []
    seen = set()
    for s in seq or []:
        x = _normalize_symbol(s)
        if not x:
            continue
        if x in seen:
            continue
        seen.add(x)
        out.append(x)
    return out


def _classify(symbols: Sequence[str]) -> Tuple[List[str], List[str]]:
    ksa: List[str] = []
    glob: List[str] = []
    for s in symbols or []:
        x = _normalize_symbol(s)
        if not x:
            continue
        if x.endswith(".SR") or x.replace(".SR", "").isdigit():
            ksa.append(x)
        else:
            glob.append(x)
    return _dedupe(ksa), _dedupe(glob)


# -----------------------------------------------------------------------------
# Registry (keys -> sheet specs)
# -----------------------------------------------------------------------------
@dataclass(frozen=True)
class PageSpec:
    key: str
    sheet_names: Tuple[str, ...]
    header_row: int = _DEFAULT_HEADER_ROW
    start_row: int = _DEFAULT_START_ROW
    max_rows: int = _DEFAULT_MAX_ROWS
    # If fixed_col is provided, we won't search the header
    fixed_col: Optional[str] = None
    # If you prefer fixed A1 range (single column), set this (e.g. "B6:B1000")
    fixed_range: Optional[str] = None
    header_candidates: Tuple[str, ...] = ("SYMBOL", "TICKER", "CODE")


# NOTE: Keep these keys in sync with scripts/run_market_scan.py defaults
PAGE_REGISTRY: Dict[str, PageSpec] = {
    "MARKET_LEADERS": PageSpec(
        key="MARKET_LEADERS",
        sheet_names=("Market_Leaders", "Market Leaders", "MARKET_LEADERS"),
        header_candidates=("SYMBOL", "TICKER", "STOCK", "CODE"),
    ),
    "GLOBAL_MARKETS": PageSpec(
        key="GLOBAL_MARKETS",
        sheet_names=("Global_Markets", "Global Markets", "GLOBAL_MARKETS"),
        header_candidates=("SYMBOL", "TICKER", "STOCK", "CODE"),
    ),
    "KSA_TADAWUL": PageSpec(
        key="KSA_TADAWUL",
        sheet_names=("KSA_Tadawul", "KSA_Tadawul_Market", "KSA Tadawul", "KSA_TADAWUL"),
        header_candidates=("SYMBOL", "TICKER", "CODE"),
    ),
    # Optional pages (safe to keep; not used unless requested)
    "MUTUAL_FUNDS": PageSpec(
        key="MUTUAL_FUNDS",
        sheet_names=("Mutual_Funds", "Mutual Funds", "MUTUAL_FUNDS"),
        header_candidates=("SYMBOL", "TICKER", "FUND", "CODE"),
    ),
    "COMMODITIES_FX": PageSpec(
        key="COMMODITIES_FX",
        sheet_names=("Commodities_FX", "Commodities & FX", "COMMODITIES_FX"),
        header_candidates=("SYMBOL", "TICKER", "CODE"),
    ),
    "MY_PORTFOLIO": PageSpec(
        key="MY_PORTFOLIO",
        sheet_names=("My_Portfolio", "My Portfolio", "MY_PORTFOLIO"),
        header_candidates=("SYMBOL", "TICKER", "CODE"),
    ),
}


# -----------------------------------------------------------------------------
# Overrides via ENV
# -----------------------------------------------------------------------------
def _try_env_override_for_key(key: str) -> Optional[List[str]]:
    env_name = f"SYMBOLS_{key.strip().upper()}"
    raw = (os.getenv(env_name) or "").strip()
    if not raw:
        return None
    parts = _split_cell_into_symbols(raw)
    return _dedupe(parts)


def _try_symbols_json_override() -> Optional[Dict[str, Any]]:
    raw = (os.getenv("SYMBOLS_JSON") or "").strip()
    if not raw:
        return None
    raw = _strip_wrapping_quotes(raw)
    try:
        obj = json.loads(raw)
        return obj if isinstance(obj, dict) else None
    except Exception:
        return None


# -----------------------------------------------------------------------------
# Google Sheets reading (lazy)
# -----------------------------------------------------------------------------
_svc = None


def _get_sheets_service():
    """
    Build Google Sheets API service lazily.
    """
    global _svc
    if _svc is not None:
        return _svc

    if not _GOOGLE_OK:
        return None

    info = _load_service_account_info()
    if not info:
        return None

    try:
        scopes = ["https://www.googleapis.com/auth/spreadsheets.readonly"]
        creds = _Credentials.from_service_account_info(info, scopes=scopes)  # type: ignore
        _svc = _build("sheets", "v4", credentials=creds, cache_discovery=False)  # type: ignore
        return _svc
    except Exception:
        return None


def _read_values(spreadsheet_id: str, range_a1: str) -> List[List[Any]]:
    """
    Returns values as list of rows.
    Never raises; returns [] on any error.
    """
    if not spreadsheet_id or not range_a1:
        return []

    cache_key = f"values::{spreadsheet_id}::{range_a1}"
    hit = _cache_get(cache_key)
    if hit is not None:
        return hit

    svc = _get_sheets_service()
    if svc is None:
        return []

    try:
        res = (
            svc.spreadsheets()
            .values()
            .get(spreadsheetId=spreadsheet_id, range=range_a1, valueRenderOption="UNFORMATTED_VALUE")
            .execute()
        )
        values = res.get("values") or []
        if not isinstance(values, list):
            values = []
        _cache_set(cache_key, values)
        return values
    except Exception:
        return []


def _resolve_symbol_column_letter(spreadsheet_id: str, sheet_name: str, spec: PageSpec) -> Optional[str]:
    """
    Reads the header row A:AZ and finds the column where header matches candidates.
    """
    try_cols = 52  # up to AZ
    end_col = _index_to_a1_col(try_cols)
    rng = f"{_safe_sheet_name(sheet_name)}!A{spec.header_row}:{end_col}{spec.header_row}"
    rows = _read_values(spreadsheet_id, rng)
    if not rows or not isinstance(rows, list) or not rows[0]:
        return None

    header = rows[0]
    # normalize header cells
    cand = {c.strip().upper() for c in spec.header_candidates if c and c.strip()}
    for idx, cell in enumerate(header, start=1):
        h = str(cell or "").strip().upper()
        # strip extra chars
        h = re.sub(r"[^A-Z0-9_% ]+", " ", h).strip()
        if not h:
            continue
        # exact or startswith
        if h in cand or any(h.startswith(c) for c in cand):
            return _index_to_a1_col(idx)

    # fallback: try common names even if not in spec
    common = {"SYMBOL", "TICKER", "CODE"}
    for idx, cell in enumerate(header, start=1):
        h = str(cell or "").strip().upper()
        h = re.sub(r"[^A-Z0-9_% ]+", " ", h).strip()
        if h in common:
            return _index_to_a1_col(idx)

    return None


def _read_symbols_from_sheet(spreadsheet_id: str, spec: PageSpec) -> Tuple[List[str], Dict[str, Any]]:
    """
    Tries the first sheet name that returns data. Returns (symbols, meta).
    """
    meta: Dict[str, Any] = {
        "key": spec.key,
        "version": VERSION,
        "mode": "sheets",
        "sheet_used": "",
        "range_used": "",
        "header_row": spec.header_row,
        "start_row": spec.start_row,
        "max_rows": spec.max_rows,
    }

    for sheet in spec.sheet_names:
        sheet = (sheet or "").strip()
        if not sheet:
            continue

        # If fixed_range is provided, use it directly
        if spec.fixed_range:
            rng = f"{_safe_sheet_name(sheet)}!{spec.fixed_range}"
            values = _read_values(spreadsheet_id, rng)
            meta["sheet_used"] = sheet
            meta["range_used"] = rng
            syms: List[str] = []
            for row in values or []:
                if not row:
                    continue
                syms.extend(_split_cell_into_symbols(str(row[0] if row else "")))
            syms = _dedupe(syms)
            if syms:
                return syms, meta
            # if empty, try next sheet name
            continue

        # Determine symbol column
        col = (spec.fixed_col or "").strip().upper() or None
        if not col:
            col = _resolve_symbol_column_letter(spreadsheet_id, sheet, spec)

        # Fallback column if header search failed
        if not col:
            col = "A"

        end_row = spec.start_row + max(1, spec.max_rows) - 1
        rng = f"{_safe_sheet_name(sheet)}!{col}{spec.start_row}:{col}{end_row}"
        values = _read_values(spreadsheet_id, rng)

        meta["sheet_used"] = sheet
        meta["range_used"] = rng
        meta["col_used"] = col

        syms2: List[str] = []
        for row in values or []:
            if not row:
                continue
            syms2.extend(_split_cell_into_symbols(str(row[0] if row else "")))

        syms2 = _dedupe(syms2)
        if syms2:
            return syms2, meta

    # none worked
    meta["mode"] = "sheets_empty"
    return [], meta


# -----------------------------------------------------------------------------
# Public API
# -----------------------------------------------------------------------------
def list_keys() -> List[str]:
    return sorted(PAGE_REGISTRY.keys())


def get_page_symbols(key: str, spreadsheet_id: Optional[str] = None) -> Dict[str, Any]:
    """
    Returns:
      {
        "all": [...],
        "ksa": [...],
        "global": [...],
        "meta": {...}
      }

    Never raises.
    """
    k = (key or "").strip().upper()
    if not k:
        return {"all": [], "ksa": [], "global": [], "meta": {"error": "empty key", "version": VERSION}}

    # 1) Per-key env override: SYMBOLS_<KEY>
    ov = _try_env_override_for_key(k)
    if ov is not None:
        ksa, glob = _classify(ov)
        return {
            "all": _dedupe(ov),
            "ksa": ksa,
            "global": glob,
            "meta": {"mode": "env_override", "key": k, "version": VERSION, "env": f"SYMBOLS_{k}"},
        }

    # 2) Bulk JSON override: SYMBOLS_JSON
    j = _try_symbols_json_override()
    if isinstance(j, dict) and k in j:
        val = j.get(k)
        if isinstance(val, list):
            all_syms = _dedupe([str(x) for x in val])
            ksa, glob = _classify(all_syms)
            return {"all": all_syms, "ksa": ksa, "global": glob, "meta": {"mode": "symbols_json", "key": k, "version": VERSION}}
        if isinstance(val, dict):
            all_syms = _dedupe([str(x) for x in (val.get("all") or val.get("symbols") or val.get("tickers") or [])])
            ksa, glob = _classify(all_syms)
            return {"all": all_syms, "ksa": ksa, "global": glob, "meta": {"mode": "symbols_json", "key": k, "version": VERSION}}

    # 3) Sheets registry lookup
    spec = PAGE_REGISTRY.get(k)
    if spec is None:
        return {"all": [], "ksa": [], "global": [], "meta": {"error": f"unknown key: {k}", "version": VERSION}}

    sid = (spreadsheet_id or "").strip() or _get_spreadsheet_id()
    if not sid:
        return {
            "all": [],
            "ksa": [],
            "global": [],
            "meta": {"error": "Spreadsheet ID missing (set DEFAULT_SPREADSHEET_ID/TFB_SPREADSHEET_ID)", "key": k, "version": VERSION},
        }

    # Cache per key+sheet id
    cache_key = f"page::{sid}::{k}"
    hit = _cache_get(cache_key)
    if isinstance(hit, dict) and "all" in hit:
        return hit

    syms, meta = _read_symbols_from_sheet(sid, spec)
    ksa, glob = _classify(syms)

    out = {"all": syms, "ksa": ksa, "global": glob, "meta": meta}
    _cache_set(cache_key, out)
    return out


# -----------------------------------------------------------------------------
# Optional CLI (safe)
# -----------------------------------------------------------------------------
if __name__ == "__main__":
    sid = _get_spreadsheet_id()
    print(f"symbols_reader v{VERSION}")
    print(f"Spreadsheet ID: {'SET' if sid else 'NOT SET'}")
    print("Keys:", ", ".join(list_keys()))
    print()

    for k in list_keys():
        data = get_page_symbols(k, spreadsheet_id=sid)
        all_syms = data.get("all") or []
        meta = data.get("meta") or {}
        print(f"[{k}] count={len(all_syms)} mode={meta.get('mode','')} sheet={meta.get('sheet_used','')}")
        if all_syms:
            print("  first:", all_syms[:10])
        else:
            err = meta.get("error")
            if err:
                print("  error:", err)
        print()
