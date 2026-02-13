#!/usr/bin/env python3
# symbols_reader.py
"""
symbols_reader.py
===========================================================
TADAWUL FAST BRIDGE – SYMBOLS READER (v2.5.0) – PROD SAFE
===========================================================

Purpose
- A single, stable API to load symbols/tickers from your Google Sheets dashboard tabs.
- Used by backend scripts (e.g., scripts/run_dashboard_sync.py, scripts/run_market_scan.py).

Key guarantees
- ✅ Never makes network calls at import-time
- ✅ Never crashes startup (all imports + env parsing are best-effort)
- ✅ Returns a predictable shape:
      {
        "all": [...],
        "ksa": [...],
        "global": [...],
        "tickers": [...],   # alias (same as "all")
        "symbols": [...],   # alias (same as "all")
        "meta": {...}
      }
- ✅ Supports env overrides (no Sheets needed)
- ✅ Google Sheets via service account JSON in env:
      GOOGLE_SHEETS_CREDENTIALS / GOOGLE_CREDENTIALS / GOOGLE_SA_JSON
      (minified JSON / pretty JSON / quoted JSON / base64(JSON) / base64url(JSON))
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
VERSION = "2.5.0"
_TRUTHY = {"1", "true", "yes", "y", "on", "t", "enable", "enabled", "ok"}

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
# Best-effort Google libs (NO network calls here)
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
# Simple in-process cache
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


def _looks_like_json_object(s: str) -> bool:
    ss = (s or "").strip()
    return ss.startswith("{") and ss.endswith("}") and len(ss) >= 2


def _looks_like_b64ish(s: str) -> bool:
    raw = (s or "").strip()
    if len(raw) < 80:
        return False
    allowed = set("ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/=_-\n\r")
    return all((c in allowed) for c in raw)


def _b64_decode_any(raw: str) -> Optional[str]:
    """
    Best-effort decode base64/base64url with padding fixes.
    """
    s = (raw or "").strip()
    if not s:
        return None

    s2 = s.strip()
    pad = len(s2) % 4
    if pad:
        s2 = s2 + ("=" * (4 - pad))

    try:
        dec = base64.urlsafe_b64decode(s2.encode("utf-8")).decode("utf-8", errors="strict").strip()
        return dec
    except Exception:
        pass

    try:
        s3 = s.replace("-", "+").replace("_", "/").strip()
        pad2 = len(s3) % 4
        if pad2:
            s3 = s3 + ("=" * (4 - pad2))
        dec2 = base64.b64decode(s3.encode("utf-8")).decode("utf-8", errors="strict").strip()
        return dec2
    except Exception:
        return None


def _maybe_b64_decode_json(s: str) -> str:
    raw = (s or "").strip()
    if not raw:
        return raw
    if _looks_like_json_object(raw):
        return raw
    if not _looks_like_b64ish(raw):
        return raw

    dec = _b64_decode_any(raw)
    if not dec:
        return raw

    dec2 = dec.strip()
    if _looks_like_json_object(dec2):
        try:
            obj = json.loads(dec2)
            if isinstance(obj, dict):
                return dec2
        except Exception:
            return raw

    return raw


def _try_parse_json_dict(raw: Any) -> Optional[Dict[str, Any]]:
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
    s = _maybe_b64_decode_json(s)
    s = _strip_wrapping_quotes(s).strip()

    if not _looks_like_json_object(s):
        return None

    try:
        obj = json.loads(s)
        return obj if isinstance(obj, dict) else None
    except Exception:
        return None


def _load_service_account_info() -> Optional[Dict[str, Any]]:
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
    n = (name or "").strip()
    if not n:
        return "Sheet1"
    return n.replace("'", "''")


def _safe_sheet_name(name: str) -> str:
    n = (name or "").strip()
    if not n:
        return "Sheet1"

    needs_quote = any(ch in n for ch in (" ", "-", "(", ")", ".", ",", "!", "[", "]", "{", "}", "&", "/", "\\"))
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

    if x.isdigit() and 3 <= len(x) <= 6:
        return f"{x}.SR"

    return x


def _split_cell_into_symbols(cell: str) -> List[str]:
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


def _norm_key(k: str) -> str:
    s = (k or "").strip().upper()
    if not s:
        return ""
    s = re.sub(r"[\s\-]+", "_", s)
    s = re.sub(r"[^A-Z0-9_]+", "", s)
    s = re.sub(r"_+", "_", s).strip("_")
    return s


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
    fixed_col: Optional[str] = None
    fixed_range: Optional[str] = None
    header_candidates: Tuple[str, ...] = ("SYMBOL", "TICKER", "CODE")


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

_KEY_ALIASES: Dict[str, str] = {
    "MARKETLEADERS": "MARKET_LEADERS",
    "MARKET_LEADER": "MARKET_LEADERS",
    "MARKETLEADER": "MARKET_LEADERS",
    "GLOBAL": "GLOBAL_MARKETS",
    "GLOBAL_MARKET": "GLOBAL_MARKETS",
    "KSATADAWUL": "KSA_TADAWUL",
    "KSA": "KSA_TADAWUL",
    "FUNDS": "MUTUAL_FUNDS",
    "MUTUALFUND": "MUTUAL_FUNDS",
    "COMMODITIES": "COMMODITIES_FX",
    "FX": "COMMODITIES_FX",
    "PORTFOLIO": "MY_PORTFOLIO",
}

def _resolve_key(key: str) -> str:
    k = _norm_key(key)
    if not k:
        return ""
    if k in PAGE_REGISTRY:
        return k
    k2 = k.replace("_", "")
    if k2 in _KEY_ALIASES:
        return _KEY_ALIASES[k2]
    if k in _KEY_ALIASES:
        return _KEY_ALIASES[k]
    return k


def _candidate_sheet_names(spec: PageSpec) -> Tuple[str, ...]:
    env_keys = (f"SHEET_{spec.key}",)
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
        low = nn.lower()
        if low in seen:
            continue
        seen.add(low)
        out.append(nn)
    return tuple(out)


# -----------------------------------------------------------------------------
# Overrides
# -----------------------------------------------------------------------------
def _try_env_override_for_key(key_norm: str) -> Optional[List[str]]:
    env_name = f"SYMBOLS_{key_norm}"
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


def _symbols_from_override_value(val: Any) -> List[str]:
    if isinstance(val, list):
        return _dedupe([str(x) for x in val if str(x).strip()])
    if isinstance(val, dict):
        for k in ("all", "symbols", "tickers"):
            vv = val.get(k)
            if isinstance(vv, list):
                return _dedupe([str(x) for x in vv if str(x).strip()])
        ksa = val.get("ksa")
        glob = val.get("global")
        out: List[str] = []
        if isinstance(ksa, list):
            out.extend([str(x) for x in ksa if str(x).strip()])
        if isinstance(glob, list):
            out.extend([str(x) for x in glob if str(x).strip()])
        return _dedupe(out)
    return []


# -----------------------------------------------------------------------------
# Google Sheets
# -----------------------------------------------------------------------------
_svc = None

def _get_sheets_service():
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
        creds = _Credentials.from_service_account_info(info, scopes=scopes)
        _svc = _build("sheets", "v4", credentials=creds, cache_discovery=False)
        return _svc
    except Exception:
        return None


def _read_values(spreadsheet_id: str, range_a1: str) -> List[List[Any]]:
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
        res = svc.spreadsheets().values().get(
            spreadsheetId=spreadsheet_id, range=range_a1, valueRenderOption="UNFORMATTED_VALUE"
        ).execute()
        values = res.get("values") or []
        if not isinstance(values, list):
            values = []
        _cache_set(cache_key, values)
        return values
    except Exception:
        return []


def _clean_header_text(v: Any) -> str:
    h = str(v or "").strip().upper()
    if not h:
        return ""
    h = re.sub(r"[^A-Z0-9_% ]+", " ", h).strip()
    h = re.sub(r"\s+", " ", h).strip()
    return h


def _resolve_symbol_column_letter(spreadsheet_id: str, sheet_name: str, spec: PageSpec) -> Optional[str]:
    try_cols = 78  # up to BZ
    end_col = _index_to_a1_col(try_cols)
    rng = f"{_safe_sheet_name(sheet_name)}!A{spec.header_row}:{end_col}{spec.header_row}"
    rows = _read_values(spreadsheet_id, rng)
    if not rows or not rows[0]:
        return None
    header = rows[0]
    cand = {_clean_header_text(c) for c in spec.header_candidates if str(c).strip()}
    cand.discard("")
    for idx, cell in enumerate(header, start=1):
        h = _clean_header_text(cell)
        if not h:
            continue
        if h in cand or any(h.startswith(c) for c in cand):
            return _index_to_a1_col(idx)
    common = {"SYMBOL", "TICKER", "CODE"}
    for idx, cell in enumerate(header, start=1):
        h = _clean_header_text(cell)
        if h in common:
            return _index_to_a1_col(idx)
    return None


def _guess_first_nonempty_column(spreadsheet_id: str, sheet_name: str, spec: PageSpec) -> str:
    candidates = ["A", "B", "C"]
    end_row = spec.start_row + max(25, min(200, spec.max_rows)) - 1
    for col in candidates:
        rng = f"{_safe_sheet_name(sheet_name)}!{col}{spec.start_row}:{col}{end_row}"
        values = _read_values(spreadsheet_id, rng)
        for row in values or []:
            if row and str(row[0] or "").strip():
                return col
    return "A"


def _read_symbols_from_sheet(spreadsheet_id: str, spec: PageSpec) -> Tuple[List[str], Dict[str, Any]]:
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

        col = (spec.fixed_col or "").strip().upper() or None
        if not col:
            col = _resolve_symbol_column_letter(spreadsheet_id, sheet, spec)
        if not col:
            col = _guess_first_nonempty_column(spreadsheet_id, sheet, spec)

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
def get_page_symbols(key: str, spreadsheet_id: Optional[str] = None) -> Dict[str, Any]:
    raw_key = (key or "").strip()
    key_norm = _resolve_key(raw_key)

    if not key_norm:
        return {"all": [], "ksa": [], "global": [], "tickers": [], "symbols": [], "meta": {"error": "empty key", "version": VERSION}}

    ov = _try_env_override_for_key(key_norm)
    if ov is not None:
        ksa, glob = _classify(ov)
        all_syms = _dedupe(ov)
        return {
            "all": all_syms,
            "ksa": ksa,
            "global": glob,
            "tickers": all_syms,
            "symbols": all_syms,
            "meta": {"mode": "env_override", "key": key_norm, "version": VERSION},
        }

    j = _try_symbols_json_override()
    if isinstance(j, dict):
        norm_map: Dict[str, str] = { _resolve_key(k): k for k in j.keys() }
        pick = key_norm if key_norm in j else norm_map.get(key_norm)
        if pick is not None and pick in j:
            all_syms = _symbols_from_override_value(j.get(pick))
            ksa, glob = _classify(all_syms)
            return {
                "all": all_syms,
                "ksa": ksa,
                "global": glob,
                "tickers": all_syms,
                "symbols": all_syms,
                "meta": {"mode": "symbols_json", "key": key_norm, "version": VERSION},
            }

    spec = PAGE_REGISTRY.get(key_norm)
    if spec is None:
        return {"all": [], "ksa": [], "global": [], "tickers": [], "symbols": [], "meta": {"error": f"unknown key: {key_norm}", "version": VERSION}}

    sid = (spreadsheet_id or "").strip() or _get_spreadsheet_id()
    if not sid:
        return {"all": [], "ksa": [], "global": [], "tickers": [], "symbols": [], "meta": {"error": "Spreadsheet ID missing", "version": VERSION}}

    cache_key = f"page::{sid}::{key_norm}"
    hit = _cache_get(cache_key)
    if isinstance(hit, dict) and "all" in hit:
        return hit

    syms, meta = _read_symbols_from_sheet(sid, spec)
    ksa, glob = _classify(syms)
    all_syms = _dedupe(syms)
    meta = dict(meta or {})
    meta["resolved_key"] = key_norm
    out = {"all": all_syms, "ksa": ksa, "global": glob, "tickers": all_syms, "symbols": all_syms, "meta": meta}
    _cache_set(cache_key, out)
    return out
