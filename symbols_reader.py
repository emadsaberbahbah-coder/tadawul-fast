#!/usr/bin/env python3
# symbols_reader.py  (FULL REPLACEMENT)
"""
symbols_reader.py
===========================================================
TADAWUL FAST BRIDGE – SYMBOLS READER (v2.4.1) – PROD SAFE
===========================================================

What’s improved vs your v2.4.0:
- ✅ Fix: sheet names with apostrophes now work (proper A1 escaping: 'Bob''s Sheet')
- ✅ Add: optional env sheet-name overrides (uses env keys like SHEET_MARKET_LEADERS, etc.)
- ✅ Add: base64url credentials support (- and _), safer padding handling
- ✅ Add: INSIGHTS_ANALYSIS + INVESTMENT_ADVISOR keys (safe; won’t break anything)

Purpose
- Provide a single, stable API for scripts (like scripts/run_market_scan.py)
  to load symbols from your Google Sheets dashboard tabs.

Key guarantees
- ✅ Never makes network calls at import-time
- ✅ Never crashes startup (imports + env parsing are best-effort)
- ✅ Returns a predictable shape:
      {"all":[...], "ksa":[...], "global":[...], "meta": {...}}
- ✅ Supports overrides via env (no Sheets needed)
- ✅ Google Sheets read via service account JSON in env:
      GOOGLE_SHEETS_CREDENTIALS  (minified JSON / pretty JSON / quoted JSON / base64(JSON))

Used by
- scripts/run_market_scan.py (expects PAGE_REGISTRY + get_page_symbols)

Env (optional)
- DEFAULT_SPREADSHEET_ID / TFB_SPREADSHEET_ID / SPREADSHEET_ID / GOOGLE_SHEETS_ID
- GOOGLE_SHEETS_CREDENTIALS / GOOGLE_CREDENTIALS / GOOGLE_SA_JSON (JSON or base64(JSON))
- GOOGLE_APPLICATION_CREDENTIALS (path to service account json file)

Overrides (optional)
- SYMBOLS_JSON  (JSON object mapping keys -> list or dict with "all"/"ksa"/"global")
- SYMBOLS_<KEY> (comma-separated list for a specific key)
  e.g. SYMBOLS_MARKET_LEADERS="1120.SR,2222.SR,GOOGL,AAPL"
- TFB_SYMBOL_HEADER_ROW (default 5)
- TFB_SYMBOL_START_ROW  (default 6)
- TFB_SYMBOL_MAX_ROWS   (default 5000)
- TFB_SYMBOLS_CACHE_TTL_SEC (default 45)

Sheet name overrides (optional)
- SHEET_MARKET_LEADERS, SHEET_GLOBAL_MARKETS, SHEET_KSA_TADAWUL, etc.
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
VERSION = "2.4.1"
_TRUTHY = {"1", "true", "yes", "y", "on", "t"}

# -----------------------------------------------------------------------------
# Safe env parsing (never crash import)
# -----------------------------------------------------------------------------
def _safe_int(v: Any, default: int) -> int:
    try:
        x = int(str(v).strip())
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
# Helpers
# -----------------------------------------------------------------------------
def _env_bool(name: str, default: bool = False) -> bool:
    v = os.getenv(name)
    if v is None:
        return default
    return str(v).strip().lower() in _TRUTHY


def _get_spreadsheet_id() -> str:
    # Support your env.py aliases + legacy names
    for k in (
        "TFB_SPREADSHEET_ID",
        "DEFAULT_SPREADSHEET_ID",
        "SPREADSHEET_ID",
        "GOOGLE_SHEETS_ID",
    ):
        v = (os.getenv(k) or "").strip()
        if v:
            return v
    return ""


def _strip_wrapping_quotes(s: str) -> str:
    t = (s or "").strip()
    if len(t) >= 2 and ((t[0] == t[-1] == '"') or (t[0] == t[-1] == "'")):
        return t[1:-1].strip()
    return t


def _looks_like_b64(s: str) -> bool:
    raw = (s or "").strip()
    if len(raw) < 80:
        return False
    # allow base64 + base64url + whitespace/newlines
    allowed = set("ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/=_-\n\r")
    return all((c in allowed) for c in raw)


def _b64_decode_any(raw: str) -> Optional[str]:
    """
    Best-effort decode base64/base64url with padding fixes.
    Returns decoded string or None.
    """
    s = (raw or "").strip()
    if not s:
        return None

    # base64url -> base64
    s2 = s.replace("-", "+").replace("_", "/").strip()

    # add padding if needed
    pad = len(s2) % 4
    if pad:
        s2 = s2 + ("=" * (4 - pad))

    try:
        dec = base64.b64decode(s2.encode("utf-8")).decode("utf-8", errors="strict").strip()
        return dec
    except Exception:
        return None


def _maybe_b64_decode(s: str) -> str:
    """
    If s looks like base64/base64url and decodes to JSON-like service account, return decoded.
    Otherwise return original.
    """
    raw = (s or "").strip()
    if not raw:
        return raw
    if raw.startswith("{"):
        return raw
    if not _looks_like_b64(raw):
        return raw

    dec = _b64_decode_any(raw)
    if not dec:
        return raw
    if dec.startswith("{") and ("private_key" in dec or '"type"' in dec):
        return dec
    return raw


def _try_parse_json_dict(raw: Any) -> Optional[Dict[str, Any]]:
    """
    Accepts:
    - dict
    - JSON string
    - base64(JSON string) / base64url(JSON string)
    - quoted JSON string
    Returns dict or None.
    """
    if raw is None:
        return None
    if isinstance(raw, dict):
        return raw
    if not isinstance(raw, str):
        return None

    s = raw.strip()
    if not s:
        return None

    s = _strip_wrapping_quotes(s)
    s = _maybe_b64_decode(s)
    s = _strip_wrapping_quotes(s).strip()

    if not s or not s.startswith("{"):
        return None

    try:
        obj = json.loads(s)
        return obj if isinstance(obj, dict) else None
    except Exception:
        return None


def _load_service_account_info() -> Optional[Dict[str, Any]]:
    """
    Reads service account info from:
    - GOOGLE_SHEETS_CREDENTIALS / GOOGLE_CREDENTIALS / GOOGLE_SA_JSON
      (minified json OR pretty json OR quoted json OR base64 json OR base64url json)
    - else GOOGLE_APPLICATION_CREDENTIALS file path
    """
    for k in ("GOOGLE_SHEETS_CREDENTIALS", "GOOGLE_CREDENTIALS", "GOOGLE_SA_JSON"):
        raw = (os.getenv(k) or "").strip()
        if raw:
            obj = _try_parse_json_dict(raw)
            if isinstance(obj, dict) and obj.get("client_email") and obj.get("private_key"):
                return obj

    path = (os.getenv("GOOGLE_APPLICATION_CREDENTIALS") or "").strip()
    if path and os.path.exists(path):
        try:
            with open(path, "r", encoding="utf-8") as f:
                obj = json.load(f)
            if isinstance(obj, dict) and obj.get("client_email") and obj.get("private_key"):
                return obj
        except Exception:
            return None

    return None


def _index_to_a1_col(idx: int) -> str:
    if idx <= 0:
        return "A"
    s = ""
    n = idx
    while n > 0:
        n, r = divmod(n - 1, 26)
        s = chr(ord("A") + r) + s
    return s


def _escape_sheet_name_for_a1(name: str) -> str:
    """
    Google Sheets A1 quoting:
    - sheet names with special chars should be in single quotes
    - any single quote inside name must be doubled: Bob's -> 'Bob''s'
    """
    n = (name or "").strip()
    if not n:
        return "Sheet1"
    return n.replace("'", "''")


def _safe_sheet_name(name: str) -> str:
    """
    For A1 ranges: quote when needed.
    Always escape embedded apostrophes correctly.
    """
    n = (name or "").strip()
    if not n:
        return "Sheet1"

    # quote if contains spaces or typical special chars (safe + consistent)
    needs_quote = any(ch in n for ch in (" ", "-", "(", ")", ".", ",", "!", "[", "]", "{", "}", "&", "/"))
    if needs_quote or ("'" in n):
        return f"'{_escape_sheet_name_for_a1(n)}'"
    return n


def _normalize_symbol(s: str) -> str:
    x = (s or "").strip().upper().replace(" ", "")
    if not x:
        return ""
    if x.startswith("TADAWUL:"):
        x = x.split(":", 1)[1].strip().upper()
    if x.endswith(".TADAWUL"):
        x = x.replace(".TADAWUL", "")
    # numeric Tadawul code -> .SR
    if x.isdigit() and 3 <= len(x) <= 6:
        return f"{x}.SR"
    return x


def _split_cell_into_symbols(cell: str) -> List[str]:
    """
    Accepts:
    - single symbol: "1120.SR"
    - comma separated: "AAPL,MSFT,GOOGL"
    - space separated: "AAPL MSFT"
    - lines / bullets
    """
    raw = str(cell or "").strip()
    if not raw:
        return []
    raw = raw.replace("•", " ").replace("·", " ").replace("\t", " ")
    parts = re.split(r"[,\n;\r]+", raw)
    out: List[str] = []
    for p in parts:
        p = (p or "").strip()
        if not p:
            continue
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
    fixed_col: Optional[str] = None
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
    # Safe extras (won’t be used unless requested)
    "INSIGHTS_ANALYSIS": PageSpec(
        key="INSIGHTS_ANALYSIS",
        sheet_names=("Insights_Analysis", "Insights Analysis", "INSIGHTS_ANALYSIS"),
        header_candidates=("SYMBOL", "TICKER", "CODE"),
    ),
    "INVESTMENT_ADVISOR": PageSpec(
        key="INVESTMENT_ADVISOR",
        sheet_names=("Investment_Advisor", "Investment Advisor", "INVESTMENT_ADVISOR"),
        header_candidates=("SYMBOL", "TICKER", "CODE"),
    ),
}

_SHEET_ENV_BY_KEY: Dict[str, Tuple[str, ...]] = {
    "MARKET_LEADERS": ("SHEET_MARKET_LEADERS",),
    "GLOBAL_MARKETS": ("SHEET_GLOBAL_MARKETS",),
    "KSA_TADAWUL": ("SHEET_KSA_TADAWUL",),
    "MUTUAL_FUNDS": ("SHEET_MUTUAL_FUNDS",),
    "COMMODITIES_FX": ("SHEET_COMMODITIES_FX",),
    "MY_PORTFOLIO": ("SHEET_MY_PORTFOLIO",),
    "INSIGHTS_ANALYSIS": ("SHEET_INSIGHTS_ANALYSIS",),
    "INVESTMENT_ADVISOR": ("SHEET_INVESTMENT_ADVISOR",),
}


def _candidate_sheet_names(spec: PageSpec) -> Tuple[str, ...]:
    """
    Prepend env-provided sheet name (if any) in front of registry names, deduped.
    """
    env_keys = _SHEET_ENV_BY_KEY.get(spec.key, ())
    env_names: List[str] = []
    for ek in env_keys:
        v = (os.getenv(ek) or "").strip()
        if v:
            env_names.append(v)

    all_names = list(env_names) + list(spec.sheet_names)
    out: List[str] = []
    seen = set()
    for n in all_names:
        nn = (n or "").strip()
        if not nn:
            continue
        if nn.lower() in seen:
            continue
        seen.add(nn.lower())
        out.append(nn)
    return tuple(out)


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
    Reads the header row A:?? and finds the column where header matches candidates.
    """
    try_cols = 78  # up to BZ
    end_col = _index_to_a1_col(try_cols)
    rng = f"{_safe_sheet_name(sheet_name)}!A{spec.header_row}:{end_col}{spec.header_row}"
    rows = _read_values(spreadsheet_id, rng)
    if not rows or not isinstance(rows, list) or not rows[0]:
        return None

    header = rows[0]
    cand = {c.strip().upper() for c in spec.header_candidates if c and c.strip()}
    for idx, cell in enumerate(header, start=1):
        h = str(cell or "").strip().upper()
        h = re.sub(r"[^A-Z0-9_% ]+", " ", h).strip()
        if not h:
            continue
        if h in cand or any(h.startswith(c) for c in cand):
            return _index_to_a1_col(idx)

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

    for sheet in _candidate_sheet_names(spec):
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
            "meta": {"error": "Spreadsheet ID missing (set DEFAULT_SPREADSHEET_ID/TFB_SPREADSHEET_ID/SPREADSHEET_ID)", "key": k, "version": VERSION},
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
