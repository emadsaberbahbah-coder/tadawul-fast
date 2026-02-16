# integrations/google_sheets_service.py  (FULL REPLACEMENT) — v3.16.0
"""
integrations/google_sheets_service.py
------------------------------------------------------------
Google Sheets helper for Tadawul Fast Bridge — v3.16.0 (PROD SAFE + ALIGNED + SAFE-MODE)

What this module does
- Reads/Writes/Clears ranges in Google Sheets using a Service Account.
- Calls Tadawul Fast Bridge backend endpoints that return:
    { headers: [...], rows: [...], status: "success|partial|error|skipped", error?: str, meta?: {...} }
- Writes data to Sheets in chunked mode to avoid request size limits.
- Enforces canonical header order per sheet when available (core.schemas.headers.get_headers_for_sheet).
- SAFE MODE: prevents destructive clears if backend returns empty headers / empty rows (configurable).

Key upgrades vs older versions
- ✅ SAFE MODE guards:
    - SHEETS_SAFE_MODE=1 blocks clear+write if backend returns empty headers
    - Optional SHEETS_BLOCK_ON_EMPTY_ROWS=1 blocks clear+write when backend returns 0 rows
- ✅ Robust header aliasing (ROI + Forecast keys):
    - expected_roi_1m / expected_roi_pct_1m -> "Expected ROI % (1M)"
    - forecast_price_1m -> "Forecast Price (1M)"
    - ... 3M / 12M variants supported
- ✅ Backend chunking + deterministic merge order by Symbol/Ticker when possible.
- ✅ BatchUpdate values write (optional) with fallback to chunked updates.
- ✅ Credentials parsing supports dict/raw/base64/file path; fixes private_key newlines.

ENV (selected)
- BACKEND_BASE_URL, APP_TOKEN, BACKUP_APP_TOKEN
- DEFAULT_SPREADSHEET_ID
- HTTP_TIMEOUT_SEC
- GOOGLE_SHEETS_CREDENTIALS (raw json or base64), GOOGLE_APPLICATION_CREDENTIALS (file path)
- SHEETS_BACKEND_TIMEOUT_SEC, SHEETS_BACKEND_RETRIES, SHEETS_BACKEND_RETRY_SLEEP
- SHEETS_BACKEND_MAX_SYMBOLS_PER_CALL (or SHEETS_BACKEND_BATCH_SIZE)
- SHEETS_API_RETRIES, SHEETS_API_RETRY_BASE_SLEEP
- SHEETS_MAX_ROWS_PER_WRITE, SHEETS_USE_BATCH_UPDATE, SHEETS_MAX_BATCH_RANGES
- SHEETS_CLEAR_END_COL, SHEETS_CLEAR_END_ROW, SHEETS_SMART_CLEAR
- SHEETS_SAFE_MODE (default true), SHEETS_BLOCK_ON_EMPTY_ROWS (default false)
- SHEETS_BACKEND_TOKEN_TRANSPORT="header,bearer,query" (any combo)
- SHEETS_BACKEND_TOKEN_QUERY_PARAM="token"
"""

from __future__ import annotations

import asyncio
import base64
import json
import logging
import os
import random
import re
import ssl
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
from typing import Any, Dict, List, Optional, Sequence, Tuple

logger = logging.getLogger("google_sheets_service")

SERVICE_VERSION = "3.16.0"
_TRUTHY = {"1", "true", "yes", "y", "on", "t"}


# =============================================================================
# OPTIONAL SAFE IMPORTS (NO HEAVY DEPENDENCIES)
# =============================================================================
get_headers_for_sheet = None  # type: ignore
try:
    from core.schemas.headers import get_headers_for_sheet as _gh  # type: ignore
    if callable(_gh):
        get_headers_for_sheet = _gh  # type: ignore
except Exception:
    pass

if get_headers_for_sheet is None:
    try:
        from core.schemas import get_headers_for_sheet as _gh2  # type: ignore
        if callable(_gh2):
            get_headers_for_sheet = _gh2  # type: ignore
    except Exception:
        pass


# Optional symbol normalization (best-effort; backend should normalize per provider)
def _fallback_normalize_symbol(raw: str) -> str:
    s = (raw or "").strip().upper()
    if not s:
        return ""
    if s.startswith("TADAWUL:"):
        s = s.split(":", 1)[1].strip()
    if s.endswith(".TADAWUL"):
        s = s.replace(".TADAWUL", "")
    # keep indices / special tickers as-is
    if any(ch in s for ch in ("^", "=", "/")):
        return s
    if s.isdigit():
        return f"{s}.SR"
    return s


_NORMALIZE_SYMBOL = _fallback_normalize_symbol
try:
    from core.symbols.normalize import normalize_symbol as _ns  # type: ignore

    if callable(_ns):
        _NORMALIZE_SYMBOL = lambda x: (str(_ns(x) or "")).strip().upper()  # type: ignore
except Exception:
    pass


# =============================================================================
# CONFIG IMPORT (env.py preferred -> core.config -> root config -> env vars)
# =============================================================================
def _safe_import(path: str) -> Optional[Any]:
    try:
        return __import__(path, fromlist=["*"])
    except Exception:
        return None


_env_mod = _safe_import("env")
_settings_from_env = getattr(_env_mod, "settings", None) if _env_mod else None

_core_cfg = _safe_import("core.config")
_core_get_settings = getattr(_core_cfg, "get_settings", None) if _core_cfg else None

_root_cfg = _safe_import("config")
_root_get_settings = getattr(_root_cfg, "get_settings", None) if _root_cfg else None


def _cfg_obj() -> Any:
    if _settings_from_env is not None:
        return _settings_from_env
    for getter in (_core_get_settings, _root_get_settings):
        if callable(getter):
            try:
                return getter()
            except Exception:
                pass
    return None


def _get_attr_any(obj: Any, names: Sequence[str]) -> Any:
    for n in names:
        try:
            if obj is not None and hasattr(obj, n):
                v = getattr(obj, n)
                if v is not None and (not isinstance(v, str) or v.strip()):
                    return v
        except Exception:
            pass
    return None


def _get_env_any(keys: Sequence[str], default: Any = None) -> Any:
    for k in keys:
        try:
            v = os.getenv(k) or os.getenv(k.upper()) or os.getenv(k.lower())
            if v is not None and str(v).strip() != "":
                return v
        except Exception:
            pass
    return default


def _get_str(names: Sequence[str], env_keys: Sequence[str], default: str = "") -> str:
    s = _cfg_obj()
    v = _get_attr_any(s, names)
    if isinstance(v, str) and v.strip():
        return v.strip()
    ev = _get_env_any(env_keys, default)
    return str(ev).strip() if ev is not None else default


def _get_float(names: Sequence[str], env_keys: Sequence[str], default: float) -> float:
    s = _cfg_obj()
    v = _get_attr_any(s, names)
    try:
        if v is not None:
            return float(v)
    except Exception:
        pass
    try:
        return float(str(_get_env_any(env_keys, default)).strip())
    except Exception:
        return float(default)


def _get_int(names: Sequence[str], env_keys: Sequence[str], default: int) -> int:
    s = _cfg_obj()
    v = _get_attr_any(s, names)
    try:
        if v is not None:
            return int(float(v))
    except Exception:
        pass
    try:
        return int(float(str(_get_env_any(env_keys, default)).strip()))
    except Exception:
        return int(default)


def _get_bool(names: Sequence[str], env_keys: Sequence[str], default: bool) -> bool:
    s = _cfg_obj()
    v = _get_attr_any(s, names)
    if isinstance(v, bool):
        return v
    if v is not None:
        sv = str(v).strip().lower()
        if sv in _TRUTHY:
            return True
        if sv in {"0", "false", "no", "n", "off", "f"}:
            return False

    ev = _get_env_any(env_keys, None)
    if ev is None:
        return default

    sv = str(ev).strip().lower()
    if sv in _TRUTHY:
        return True
    if sv in {"0", "false", "no", "n", "off", "f"}:
        return False
    return default


# Prefer env.py constants if present
_BACKEND_URL = (
    (getattr(_env_mod, "BACKEND_BASE_URL", None) if _env_mod else None)
    or _get_str(["backend_base_url"], ["BACKEND_BASE_URL", "TFB_BASE_URL", "BASE_URL"], "")
).rstrip("/")

_APP_TOKEN = (
    (getattr(_env_mod, "APP_TOKEN", None) if _env_mod else None)
    or _get_str(["app_token"], ["APP_TOKEN", "TFB_APP_TOKEN"], "")
).strip()

_BACKUP_TOKEN = (
    (getattr(_env_mod, "BACKUP_APP_TOKEN", None) if _env_mod else None)
    or _get_str(["backup_app_token"], ["BACKUP_APP_TOKEN"], "")
).strip()

_DEFAULT_SHEET_ID = (
    (getattr(_env_mod, "DEFAULT_SPREADSHEET_ID", None) if _env_mod else None)
    or _get_str(
        ["default_spreadsheet_id"],
        ["DEFAULT_SPREADSHEET_ID", "SPREADSHEET_ID", "GOOGLE_SHEETS_ID", "GOOGLE_SHEET_ID"],
        "",
    )
).strip()

_HTTP_TIMEOUT = max(5.0, _get_float(["http_timeout_sec"], ["HTTP_TIMEOUT_SEC", "HTTP_TIMEOUT"], 25.0))

# SAFE MODE (prevents destructive clears on bad backend response)
_SAFE_MODE = _get_bool([], ["SHEETS_SAFE_MODE"], True)
_BLOCK_ON_EMPTY_ROWS = _get_bool([], ["SHEETS_BLOCK_ON_EMPTY_ROWS"], False)


# =============================================================================
# Credentials handling
# =============================================================================
# Credentials (prefer parsed dict)
_CREDS_DICT: Optional[Dict[str, Any]] = None
try:
    s = _cfg_obj()
    cd = _get_attr_any(s, ["google_credentials_dict", "google_sheets_credentials"])
    if isinstance(cd, dict) and cd:
        _CREDS_DICT = dict(cd)
except Exception:
    _CREDS_DICT = None

# Raw credentials fallback
_CREDS_RAW = ""
try:
    s = _cfg_obj()
    raw = _get_attr_any(
        s,
        [
            "google_sheets_credentials_raw",
            "google_sheets_credentials",
            "google_sheets_credentials_json",
            "google_credentials",
        ],
    )
    if isinstance(raw, str) and raw.strip():
        _CREDS_RAW = raw.strip()
except Exception:
    _CREDS_RAW = ""

if not _CREDS_RAW:
    _CREDS_RAW = (os.getenv("GOOGLE_SHEETS_CREDENTIALS", "") or os.getenv("GOOGLE_CREDENTIALS", "") or "").strip()

# Support credentials file paths
_CREDS_FILE = (
    os.getenv("GOOGLE_SHEETS_CREDENTIALS_FILE", "")
    or os.getenv("GOOGLE_CREDENTIALS_FILE", "")
    or os.getenv("GOOGLE_APPLICATION_CREDENTIALS", "")
).strip()


# =============================================================================
# GOOGLE API CLIENT IMPORT
# =============================================================================
try:
    from google.oauth2.service_account import Credentials  # type: ignore
    from googleapiclient.discovery import build  # type: ignore
    from googleapiclient.errors import HttpError  # type: ignore
except Exception:
    Credentials = None  # type: ignore
    build = None  # type: ignore
    HttpError = None  # type: ignore


# =============================================================================
# SSL / CONSTANTS
# =============================================================================
_SSL_CONTEXT = ssl.create_default_context()
_SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]

_SHEETS_SERVICE = None  # singleton
_SHEETS_INIT_LOCK = threading.Lock()

_BACKEND_TIMEOUT = float(os.getenv("SHEETS_BACKEND_TIMEOUT_SEC", "120") or "120")
_BACKEND_TIMEOUT = max(_BACKEND_TIMEOUT, _HTTP_TIMEOUT)

_SHEETS_RETRIES = max(1, int(os.getenv("SHEETS_API_RETRIES", "3") or "3"))
_SHEETS_RETRY_BASE_SLEEP = float(os.getenv("SHEETS_API_RETRY_BASE_SLEEP", "1.0") or "1.0")

_BACKEND_RETRIES = max(0, int(os.getenv("SHEETS_BACKEND_RETRIES", "2") or "2"))
_BACKEND_RETRY_SLEEP = float(os.getenv("SHEETS_BACKEND_RETRY_SLEEP", "1.0") or "1.0")

_MAX_ROWS_PER_WRITE = max(50, int(os.getenv("SHEETS_MAX_ROWS_PER_WRITE", "500") or "500"))
_USE_BATCH_UPDATE = _get_bool([], ["SHEETS_USE_BATCH_UPDATE"], True)
_MAX_BATCH_RANGES = max(5, int(os.getenv("SHEETS_MAX_BATCH_RANGES", "25") or "25"))

_CLEAR_END_COL = (os.getenv("SHEETS_CLEAR_END_COL", "ZZ") or "ZZ").strip().upper()
_CLEAR_END_ROW = max(1000, int(os.getenv("SHEETS_CLEAR_END_ROW", "100000") or "100000"))
_SMART_CLEAR = _get_bool([], ["SHEETS_SMART_CLEAR"], True)

_BACKEND_MAX_SYMBOLS_PER_CALL = max(
    25,
    int(
        os.getenv("SHEETS_BACKEND_MAX_SYMBOLS_PER_CALL", "")
        or _get_int([], ["SHEETS_BACKEND_BATCH_SIZE"], 200)
    ),
)

_USER_AGENT = (os.getenv("SHEETS_USER_AGENT", "") or "").strip() or f"TadawulFastBridge-SheetsService/{SERVICE_VERSION}"

# Backend token transport alignment (header/bearer/query)
_BACKEND_TOKEN_TRANSPORT = (os.getenv("SHEETS_BACKEND_TOKEN_TRANSPORT", "header,bearer") or "header,bearer").lower()
_BACKEND_TOKEN_QUERY_PARAM = (os.getenv("SHEETS_BACKEND_TOKEN_QUERY_PARAM", "token") or "token").strip()

_A1_RE = re.compile(r"^\$?([A-Za-z]+)\$?(\d+)$")
_HKEY_RE = re.compile(r"[^a-z0-9]+")


def _hkey(name: Any) -> str:
    s = str(name or "").strip().lower()
    if not s:
        return ""
    return _HKEY_RE.sub("", s)


# =============================================================================
# Header aliasing (ROI + forecast key fix) — used for dict-row -> header generation
# =============================================================================
# Map common backend keys -> canonical sheet header labels
_HEADER_ALIAS_MAP: Dict[str, str] = {
    # Symbol
    "symbol": "Symbol",
    "ticker": "Symbol",
    "requested_symbol": "Symbol",
    # ROI %
    "expectedroi1m": "Expected ROI % (1M)",
    "expectedroipct1m": "Expected ROI % (1M)",
    "expectedroi%1m": "Expected ROI % (1M)",
    "expectedroi3m": "Expected ROI % (3M)",
    "expectedroipct3m": "Expected ROI % (3M)",
    "expectedroi12m": "Expected ROI % (12M)",
    "expectedroipct12m": "Expected ROI % (12M)",
    # Forecast price
    "forecastprice1m": "Forecast Price (1M)",
    "forecastprice3m": "Forecast Price (3M)",
    "forecastprice12m": "Forecast Price (12M)",
    # Forecast updated
    "forecastupdatedutc": "Forecast Updated (UTC)",
    "forecast_updated_utc": "Forecast Updated (UTC)",
    # Rec/score common variants
    "recommendation": "Recommendation",
    "rec": "Recommendation",
    "overallscore": "Overall Score",
    "advisorscore": "Advisor Score",
    "riskscore": "Risk Score",
}

# Normalize alias keys to hkey form
_HEADER_ALIAS_MAP = {_hkey(k): v for k, v in _HEADER_ALIAS_MAP.items() if _hkey(k) and str(v).strip()}


# =============================================================================
# LOW-LEVEL HELPERS
# =============================================================================
def _utc_iso() -> str:
    try:
        from datetime import datetime, timezone

        return datetime.now(timezone.utc).isoformat()
    except Exception:
        return ""


def _sleep_backoff(base: float, attempt: int, cap: float = 10.0) -> None:
    t = base * (2 ** attempt)
    t = min(t, cap)
    t = t * (0.85 + 0.3 * random.random())
    time.sleep(t)


def _safe_preview(text: str, limit: int = 400) -> str:
    t = (text or "").replace("\r", " ").replace("\n", " ").strip()
    return t[:limit]


def _safe_json_loads(raw: str) -> Optional[Any]:
    try:
        return json.loads(raw)
    except Exception:
        return None


def _coerce_private_key(creds_info: Dict[str, Any]) -> Dict[str, Any]:
    try:
        pk = creds_info.get("private_key")
        if isinstance(pk, str) and "\\n" in pk:
            creds_info["private_key"] = pk.replace("\\n", "\n")
    except Exception:
        pass
    return creds_info


def _looks_like_base64(s: str) -> bool:
    raw = (s or "").strip()
    if not raw or raw.startswith("{"):
        return False
    allowed = set("ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/=\n\r")
    if any(ch not in allowed for ch in raw):
        return False
    compact = raw.replace("\n", "").replace("\r", "")
    return len(compact) >= 40 and (len(compact) % 4 == 0)


def _maybe_b64_decode(s: str) -> str:
    raw = (s or "").strip()
    if not raw or raw.startswith("{"):
        return raw
    if not _looks_like_base64(raw):
        return raw
    try:
        decoded = base64.b64decode(raw).decode("utf-8", errors="strict").strip()
        if decoded.startswith("{") and '"private_key"' in decoded:
            return decoded
    except Exception:
        return raw
    return raw


def _strip_wrapping_quotes(s: str) -> str:
    t = (s or "").strip()
    if len(t) >= 2 and ((t[0] == t[-1] == '"') or (t[0] == t[-1] == "'")):
        return t[1:-1].strip()
    return t


def _parse_credentials(raw: str) -> Optional[Dict[str, Any]]:
    if not raw:
        return None
    cleaned = _strip_wrapping_quotes(str(raw).strip())
    cleaned = _maybe_b64_decode(cleaned)
    cleaned = _strip_wrapping_quotes(cleaned)
    if not cleaned.startswith("{"):
        return None
    data = _safe_json_loads(cleaned)
    if isinstance(data, dict) and data:
        return _coerce_private_key(data)
    return None


def _read_credentials_file(path: str) -> Optional[Dict[str, Any]]:
    p = (path or "").strip()
    if not p:
        return None
    try:
        with open(p, "r", encoding="utf-8") as f:
            raw = f.read().strip()
        return _parse_credentials(raw) or (_safe_json_loads(raw) if raw.startswith("{") else None)
    except Exception:
        return None


def _safe_sheet_name(sheet_name: str) -> str:
    name = (sheet_name or "").strip() or "Sheet1"
    name = name.replace("'", "''")
    return f"'{name}'"


def _parse_a1_cell(cell: str) -> Tuple[str, int]:
    s = (cell or "").strip()
    if ":" in s:
        s = s.split(":", 1)[0].strip()
    m = _A1_RE.match(s)
    if not m:
        return ("A", 1)
    col = m.group(1).upper()
    row = int(m.group(2))
    if row <= 0:
        row = 1
    return (col, row)


def _a1(col: str, row: int) -> str:
    return f"{col.upper()}{int(row)}"


def _chunk_rows(values: List[List[Any]], max_rows: int) -> List[List[List[Any]]]:
    if not values:
        return []
    if max_rows <= 0:
        return [values]
    return [values[i : i + max_rows] for i in range(0, len(values), max_rows)]


def _col_to_index(col: str) -> int:
    col = (col or "").strip().upper()
    if not col:
        return 1
    n = 0
    for ch in col:
        if "A" <= ch <= "Z":
            n = n * 26 + (ord(ch) - ord("A") + 1)
    return max(1, n)


def _index_to_col(idx: int) -> str:
    idx = int(idx)
    if idx <= 0:
        idx = 1
    s = ""
    while idx > 0:
        idx, rem = divmod(idx - 1, 26)
        s = chr(rem + ord("A")) + s
    return s or "A"


def _compute_clear_end_col(start_col: str, num_cols: int) -> str:
    if num_cols <= 0:
        return _CLEAR_END_COL
    start_idx = _col_to_index(start_col)
    end_idx = start_idx + num_cols - 1
    return _index_to_col(end_idx)


def _require_spreadsheet_id(spreadsheet_id: str) -> str:
    sid = (spreadsheet_id or _DEFAULT_SHEET_ID or "").strip()
    if not sid:
        raise ValueError("No Spreadsheet ID provided (DEFAULT_SPREADSHEET_ID missing).")
    return sid


def _safe_status_error(where: str, err: str, **extra) -> Dict[str, Any]:
    out: Dict[str, Any] = {"status": "error", "where": where, "error": err}
    out.update(extra)
    return out


def _chunk_list(items: List[str], size: int) -> List[List[str]]:
    if not items:
        return []
    size = max(1, int(size or 1))
    return [items[i : i + size] for i in range(0, len(items), size)]


def _append_missing_headers_ci(base: List[str], extra: Sequence[Any]) -> List[str]:
    seen = {_hkey(h) for h in (base or []) if _hkey(h)}
    out = list(base or [])
    for h in (extra or []):
        hs = str(h).strip()
        hk = _hkey(hs)
        if not hs or not hk:
            continue
        if hk in seen:
            continue
        seen.add(hk)
        out.append(hs)
    return out


def _safe_endpoint_join(base: str, endpoint: str) -> str:
    b = (base or "").rstrip("/")
    ep = (endpoint or "").strip()
    if not ep.startswith("/"):
        ep = "/" + ep
    return b + ep


def _add_query_params(endpoint: str, params: Optional[Dict[str, Any]]) -> str:
    if not params:
        return endpoint
    ep = (endpoint or "").strip()
    if not ep:
        return ep

    parsed = urllib.parse.urlsplit(ep)
    q = dict(urllib.parse.parse_qsl(parsed.query, keep_blank_values=True))
    for k, v in params.items():
        if v is None:
            continue
        s = str(v).strip()
        if s == "":
            continue
        q[str(k)] = s
    new_q = urllib.parse.urlencode(q, doseq=True)
    return urllib.parse.urlunsplit((parsed.scheme, parsed.netloc, parsed.path, new_q, parsed.fragment))


# =============================================================================
# CANONICAL HEADERS (per sheet) + output ordering
# =============================================================================
_CANONICAL_CACHE: Dict[str, List[str]] = {}


def _get_canonical_headers(sheet_name: str) -> List[str]:
    sh = (sheet_name or "").strip()
    if not sh:
        return []
    key = sh.upper()
    if key in _CANONICAL_CACHE:
        return list(_CANONICAL_CACHE[key])

    headers: List[str] = []
    if callable(get_headers_for_sheet):
        try:
            hh = get_headers_for_sheet(sh)  # type: ignore
            if isinstance(hh, list):
                headers = [str(x).strip() for x in hh if str(x).strip()]
        except Exception:
            headers = []

    _CANONICAL_CACHE[key] = list(headers)
    return list(headers)


def _reorder_to_canonical(sheet_name: str, headers: List[str], rows: List[List[Any]]) -> Tuple[List[str], List[List[Any]]]:
    canonical = _get_canonical_headers(sheet_name)
    if not canonical:
        return headers, rows

    canonical_keys = {_hkey(h) for h in canonical if _hkey(h)}
    extras = [h for h in (headers or []) if _hkey(h) and _hkey(h) not in canonical_keys]
    out_headers = list(canonical) + extras

    in_idx = {_hkey(h): i for i, h in enumerate(headers or []) if _hkey(h)}
    out_rows: List[List[Any]] = []
    for r in rows or []:
        rr = list(r) if isinstance(r, (list, tuple)) else [r]
        new_row: List[Any] = [None] * len(out_headers)
        for j, h in enumerate(out_headers):
            hk = _hkey(h)
            i = in_idx.get(hk)
            if i is None:
                continue
            if i < len(rr):
                new_row[j] = rr[i]
        out_rows.append(new_row)

    return out_headers, out_rows


# =============================================================================
# GOOGLE SHEETS SERVICE INITIALIZATION
# =============================================================================
def get_sheets_service():
    global _SHEETS_SERVICE
    if _SHEETS_SERVICE is not None:
        return _SHEETS_SERVICE

    with _SHEETS_INIT_LOCK:
        if _SHEETS_SERVICE is not None:
            return _SHEETS_SERVICE

        if not Credentials or not build:
            raise RuntimeError(
                "Google API client libraries not installed. "
                "Install: pip install google-api-python-client google-auth"
            )

        creds_info: Optional[Dict[str, Any]] = None

        if isinstance(_CREDS_DICT, dict) and _CREDS_DICT:
            creds_info = _coerce_private_key(dict(_CREDS_DICT))

        if creds_info is None:
            creds_info = _parse_credentials(_CREDS_RAW)

        if creds_info is None and _CREDS_FILE:
            creds_info = _read_credentials_file(_CREDS_FILE)

        if creds_info is None:
            creds_info = _parse_credentials(os.getenv("GOOGLE_SHEETS_CREDENTIALS", "") or os.getenv("GOOGLE_CREDENTIALS", "") or "")

        if not isinstance(creds_info, dict) or not creds_info:
            raise RuntimeError(
                "Missing/invalid Google service account credentials. "
                "Set GOOGLE_SHEETS_CREDENTIALS (json/base64) or GOOGLE_APPLICATION_CREDENTIALS (file path)."
            )

        creds = Credentials.from_service_account_info(creds_info, scopes=_SCOPES)
        _SHEETS_SERVICE = build("sheets", "v4", credentials=creds, cache_discovery=False)
        logger.info("[GoogleSheets] Service initialized.")
        return _SHEETS_SERVICE


# =============================================================================
# SHEETS API RETRY WRAPPER
# =============================================================================
def _http_error_status(e: Exception) -> Optional[int]:
    try:
        if HttpError is not None and isinstance(e, HttpError):  # type: ignore
            resp = getattr(e, "resp", None)
            st = getattr(resp, "status", None)
            if st is not None:
                return int(st)
    except Exception:
        pass
    return None


def _retry_sheet_op(operation_name: str, func, *args, **kwargs):
    last_exc: Optional[Exception] = None
    for i in range(_SHEETS_RETRIES):
        try:
            return func(*args, **kwargs)
        except Exception as e:
            last_exc = e
            status = _http_error_status(e)
            msg = str(e).lower()

            retryable = status in (429, 500, 502, 503, 504) or any(
                x in msg
                for x in (
                    "quota",
                    "rate limit",
                    "ratelimitexceeded",
                    "userratelimitexceeded",
                    "backenderror",
                    "internal error",
                    "the service is currently unavailable",
                )
            )

            if retryable and i < _SHEETS_RETRIES - 1:
                logger.warning("[GoogleSheets] %s retry (%s/%s): %s", operation_name, i + 1, _SHEETS_RETRIES, e)
                _sleep_backoff(_SHEETS_RETRY_BASE_SLEEP, i, cap=10.0)
                continue
            raise
    raise last_exc  # pragma: no cover


# =============================================================================
# SHEETS OPS: READ / WRITE / CLEAR
# =============================================================================
def read_range(spreadsheet_id: str, range_name: str) -> List[List[Any]]:
    service = get_sheets_service()
    sid = _require_spreadsheet_id(spreadsheet_id)

    def _do_read():
        result = service.spreadsheets().values().get(
            spreadsheetId=sid, range=range_name, majorDimension="ROWS"
        ).execute()
        return result.get("values", []) or []

    return _retry_sheet_op("Read Range", _do_read)


def write_range(
    spreadsheet_id: str,
    range_name: str,
    values: List[List[Any]],
    value_input: str = "RAW",
) -> int:
    service = get_sheets_service()
    sid = _require_spreadsheet_id(spreadsheet_id)
    body = {"values": values or [[]]}

    def _do_write():
        result = service.spreadsheets().values().update(
            spreadsheetId=sid,
            range=range_name,
            valueInputOption=value_input,
            body=body,
        ).execute()
        return int(result.get("updatedCells", 0) or 0)

    return _retry_sheet_op("Write Range", _do_write)


def clear_range(spreadsheet_id: str, range_name: str) -> None:
    service = get_sheets_service()
    sid = _require_spreadsheet_id(spreadsheet_id)

    def _do_clear():
        service.spreadsheets().values().clear(spreadsheetId=sid, range=range_name).execute()

    _retry_sheet_op("Clear Range", _do_clear)


def write_grid_chunked(
    spreadsheet_id: str,
    sheet_name: str,
    start_cell: str,
    grid: List[List[Any]],
    value_input: str = "RAW",
) -> int:
    """
    Writes [header_row + data_rows] starting at start_cell.
    - Pads/trims rows to header width
    - Uses values.batchUpdate (optional) then falls back to sequential updates
    """
    if not grid:
        return 0

    sid = _require_spreadsheet_id(spreadsheet_id)
    start_col, start_row = _parse_a1_cell(start_cell)
    sheet_a1 = _safe_sheet_name(sheet_name)

    header = grid[0] if grid else []
    header = list(header) if isinstance(header, (list, tuple)) else [header]
    hdr_len = len(header)

    data_rows = grid[1:] if len(grid) > 1 else []
    fixed_rows: List[List[Any]] = []
    for r in data_rows:
        rr = list(r) if isinstance(r, (list, tuple)) else [r]
        if hdr_len:
            if len(rr) < hdr_len:
                rr += [None] * (hdr_len - len(rr))
            elif len(rr) > hdr_len:
                rr = rr[:hdr_len]
        fixed_rows.append(rr)

    chunks = _chunk_rows(fixed_rows, _MAX_ROWS_PER_WRITE)

    # Header-only write
    if not chunks:
        rng = f"{sheet_a1}!{_a1(start_col, start_row)}"
        return write_range(sid, rng, [header], value_input=value_input)

    # Try batchUpdate
    if _USE_BATCH_UPDATE:
        try:
            service = get_sheets_service()
            data: List[Dict[str, Any]] = []

            rng0 = f"{sheet_a1}!{_a1(start_col, start_row)}"
            data.append({"range": rng0, "values": [header] + chunks[0]})

            current_row = start_row + 1 + len(chunks[0])
            for idx in range(1, len(chunks)):
                rng = f"{sheet_a1}!{_a1(start_col, current_row)}"
                data.append({"range": rng, "values": chunks[idx]})
                current_row += len(chunks[idx])

            def _do_batch(batch_data: List[Dict[str, Any]]) -> int:
                body = {"valueInputOption": value_input, "data": batch_data}
                result = service.spreadsheets().values().batchUpdate(spreadsheetId=sid, body=body).execute()
                total = 0
                for r in (result.get("responses") or []):
                    total += int(r.get("updatedCells", 0) or 0)
                return total

            total_cells = 0
            for i in range(0, len(data), _MAX_BATCH_RANGES):
                part = data[i : i + _MAX_BATCH_RANGES]
                total_cells += int(_retry_sheet_op("Batch Write Grid", _do_batch, part) or 0)

            return total_cells
        except Exception as e:
            logger.warning("[GoogleSheets] values.batchUpdate failed; falling back to sequential updates: %s", e)

    # Sequential fallback
    total_cells = 0
    rng0 = f"{sheet_a1}!{_a1(start_col, start_row)}"
    total_cells += write_range(sid, rng0, [header] + chunks[0], value_input=value_input)

    current_row = start_row + 1 + len(chunks[0])
    for idx in range(1, len(chunks)):
        rng = f"{sheet_a1}!{_a1(start_col, current_row)}"
        total_cells += write_range(sid, rng, chunks[idx], value_input=value_input)
        current_row += len(chunks[idx])

    return total_cells


# =============================================================================
# BACKEND API CLIENT (urllib)
# =============================================================================
def _backend_base_url() -> str:
    return (_BACKEND_URL or "http://127.0.0.1:8000").rstrip("/")


def _token_candidates() -> List[str]:
    toks: List[str] = []
    if _APP_TOKEN:
        toks.append(_APP_TOKEN)
    if _BACKUP_TOKEN and _BACKUP_TOKEN != _APP_TOKEN:
        toks.append(_BACKUP_TOKEN)
    if not toks:
        toks.append("")  # allow open-mode backend
    return toks


def _backend_headers(token: str) -> Dict[str, str]:
    h = {
        "Content-Type": "application/json; charset=utf-8",
        "User-Agent": _USER_AGENT,
        "Accept": "application/json, text/plain;q=0.9, */*;q=0.8",
    }
    if not token:
        return h

    if "header" in _BACKEND_TOKEN_TRANSPORT:
        h["X-APP-TOKEN"] = token
    if "bearer" in _BACKEND_TOKEN_TRANSPORT:
        h["Authorization"] = f"Bearer {token}"
    return h


def _backend_url_with_token(url: str, token: str) -> str:
    if not token or ("query" not in _BACKEND_TOKEN_TRANSPORT):
        return url
    try:
        parsed = urllib.parse.urlsplit(url)
        q = dict(urllib.parse.parse_qsl(parsed.query, keep_blank_values=True))
        q[_BACKEND_TOKEN_QUERY_PARAM] = token
        new_q = urllib.parse.urlencode(q, doseq=True)
        return urllib.parse.urlunsplit((parsed.scheme, parsed.netloc, parsed.path, new_q, parsed.fragment))
    except Exception:
        return url


def _sheets_safe_error_payload(symbols: List[str], err: str) -> Dict[str, Any]:
    return {
        "headers": ["Symbol", "Error"],
        "rows": [[t, err] for t in (symbols or [])],
        "status": "error",
        "error": err,
    }


def _extract_rows_candidate(resp: Dict[str, Any]) -> Any:
    """
    Some endpoints might return rows in:
      - rows
      - items
      - data.rows
    We normalize here.
    """
    if not isinstance(resp, dict):
        return []
    if isinstance(resp.get("rows"), list):
        return resp.get("rows")
    if isinstance(resp.get("items"), list):
        return resp.get("items")
    data = resp.get("data")
    if isinstance(data, dict) and isinstance(data.get("rows"), list):
        return data.get("rows")
    return []


def _extract_headers_candidate(resp: Dict[str, Any]) -> List[str]:
    if not isinstance(resp, dict):
        return []
    hh = resp.get("headers")
    if isinstance(hh, list) and hh:
        return [str(x).strip() for x in hh if str(x).strip()]
    data = resp.get("data")
    if isinstance(data, dict):
        hh2 = data.get("headers")
        if isinstance(hh2, list) and hh2:
            return [str(x).strip() for x in hh2 if str(x).strip()]
    return []


def _call_backend_api_once(endpoint: str, payload: Dict[str, Any]) -> Dict[str, Any]:
    """
    Single POST call with retries + token fallback.
    Returns dict always (sheets-safe error payload on failure).
    """
    base = _backend_base_url()
    url0 = _safe_endpoint_join(base, endpoint)

    syms_any = payload.get("symbols") or payload.get("tickers") or []
    symbols = [str(t).strip() for t in (syms_any or []) if str(t).strip()]
    if not symbols:
        return {"headers": ["Symbol", "Error"], "rows": [], "status": "skipped", "error": "No symbols provided"}

    try:
        body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
    except Exception as ex:
        return _sheets_safe_error_payload(symbols, f"Payload JSON encode error: {ex}")

    last_err: Optional[Exception] = None
    last_status: Optional[int] = None
    last_body_preview: Optional[str] = None

    attempts = max(1, int(_BACKEND_RETRIES) + 1)

    for attempt in range(attempts):
        for tok_idx, tok in enumerate(_token_candidates()):
            url = _backend_url_with_token(url0, tok)
            try:
                req = urllib.request.Request(url, data=body, headers=_backend_headers(tok), method="POST")
                with urllib.request.urlopen(req, timeout=_BACKEND_TIMEOUT, context=_SSL_CONTEXT) as resp:  # type: ignore
                    raw = resp.read().decode("utf-8", errors="replace")
                    last_status = int(getattr(resp, "status", 0) or 0)
                    last_body_preview = _safe_preview(raw, 400)

                    if 200 <= last_status < 300:
                        parsed = _safe_json_loads(raw)
                        if isinstance(parsed, dict):
                            parsed.setdefault("status", "success")
                            parsed.setdefault("headers", _extract_headers_candidate(parsed) or [])
                            parsed.setdefault("rows", _extract_rows_candidate(parsed) or [])
                            return parsed
                        return _sheets_safe_error_payload(symbols, "Backend returned non-JSON response")

                    raise RuntimeError(f"Backend HTTP {last_status}: {last_body_preview}")

            except urllib.error.HTTPError as e:
                last_err = e
                last_status = int(getattr(e, "code", 0) or 0)
                raw = ""
                try:
                    raw = e.read().decode("utf-8", errors="replace")  # type: ignore
                except Exception:
                    raw = str(e)
                last_body_preview = _safe_preview(raw, 400)

                # token fallback on auth failures
                if last_status in (401, 403) and tok_idx < len(_token_candidates()) - 1:
                    logger.warning("[SheetsService] Backend auth failed (HTTP %s) token#%s -> trying next token.", last_status, tok_idx + 1)
                    continue

                # retry on transient backend errors
                if last_status in (429, 500, 502, 503, 504) and attempt < attempts - 1:
                    ra = None
                    try:
                        ra = e.headers.get("Retry-After")  # type: ignore
                    except Exception:
                        ra = None
                    if ra and str(ra).strip().isdigit():
                        time.sleep(min(8.0, float(ra)))
                    else:
                        _sleep_backoff(_BACKEND_RETRY_SLEEP, attempt, cap=8.0)
                    continue

                logger.warning("[SheetsService] Backend HTTPError %s: %s", last_status, last_body_preview)

            except Exception as e:
                last_err = e
                logger.warning(
                    "[SheetsService] Backend call failed (%s/%s) endpoint=%s: %s",
                    attempt + 1,
                    attempts,
                    endpoint,
                    e,
                )

        if attempt < attempts - 1:
            _sleep_backoff(_BACKEND_RETRY_SLEEP, attempt, cap=8.0)

    err_msg = f"{last_err}" if last_err else "Unknown backend error"
    if last_status:
        err_msg = f"HTTP {last_status}: {err_msg}"
    if last_body_preview and last_status in (400, 401, 403, 404, 429, 500, 502, 503, 504):
        err_msg = f"{err_msg} | body: {last_body_preview}"

    return _sheets_safe_error_payload(symbols, err_msg)


def _find_symbol_col(headers: List[str]) -> int:
    for i, h in enumerate(headers or []):
        hk = _hkey(h)
        if hk in ("symbol", "ticker"):
            return i
    return -1


def _headers_from_dict_rows(rows: List[Dict[str, Any]]) -> List[str]:
    """
    Build a stable headers list from dict rows using alias mapping where possible.
    """
    if not rows:
        return []
    seen = set()
    out: List[str] = []

    # preference order: Symbol first if present
    keys = []
    for d in rows:
        for k in (d or {}).keys():
            if k not in keys:
                keys.append(k)

    # Put symbol-ish keys first
    symbol_keys = [k for k in keys if _hkey(k) in ("symbol", "ticker", "requested_symbol")]
    other_keys = [k for k in keys if k not in symbol_keys]
    ordered_keys = symbol_keys + other_keys

    for k in ordered_keys:
        hk = _hkey(k)
        label = _HEADER_ALIAS_MAP.get(hk) or str(k).strip()
        lk = _hkey(label)
        if not lk or lk in seen:
            continue
        seen.add(lk)
        out.append(label)

    return out


def _rows_to_grid(headers: List[str], rows: Any) -> Tuple[List[str], List[List[Any]]]:
    """
    Normalize backend rows:
    - list[list] -> keep (pad/trim to header length)
    - list[dict] -> ordered list rows by headers (header-key normalized + alias support)
    """
    # If rows is dict-list and headers missing, generate headers from dict keys
    if (not headers) and isinstance(rows, list) and rows and isinstance(rows[0], dict):
        headers = _headers_from_dict_rows(rows) or headers

    hdrs = [str(h).strip() for h in (headers or []) if str(h).strip()]
    if not hdrs:
        hdrs = ["Symbol", "Error"]

    fixed_rows: List[List[Any]] = []

    if isinstance(rows, list) and rows and isinstance(rows[0], dict):
        # Build a lookup per row using normalized keys of *both* original key and alias label key
        hdr_keys = [_hkey(h) for h in hdrs]
        for d in rows:
            dd = d if isinstance(d, dict) else {}
            km: Dict[str, Any] = {}
            for k, v in dd.items():
                hk = _hkey(k)
                km[hk] = v
                # also allow alias resolution
                alias_label = _HEADER_ALIAS_MAP.get(hk)
                if alias_label:
                    km[_hkey(alias_label)] = v

            fixed_rows.append([km.get(hk, None) for hk in hdr_keys])
        return hdrs, fixed_rows

    if isinstance(rows, list):
        for r in rows:
            rr = list(r) if isinstance(r, (list, tuple)) else [r]
            if len(rr) < len(hdrs):
                rr += [None] * (len(hdrs) - len(rr))
            elif len(rr) > len(hdrs):
                rr = rr[: len(hdrs)]
            fixed_rows.append(rr)
        return hdrs, fixed_rows

    fixed_rows.append([str(rows)])
    return hdrs, fixed_rows


def _merge_backend_responses(
    requested_symbols: List[str],
    first_headers: List[str],
    responses: List[Dict[str, Any]],
) -> Tuple[List[str], List[List[Any]], str, Optional[str]]:
    status = "success"
    error_msg: Optional[str] = None

    headers = [str(h).strip() for h in (first_headers or []) if str(h).strip()]

    if not headers:
        for r in responses:
            hh = _extract_headers_candidate(r) or []
            if hh:
                headers = hh
                break

    # union headers across responses
    for r in responses:
        hh = _extract_headers_candidate(r) or []
        if hh:
            headers = _append_missing_headers_ci(headers, hh)

    # if still no headers, but dict rows exist -> generate headers
    if not headers:
        for r in responses:
            rr = _extract_rows_candidate(r)
            if isinstance(rr, list) and rr and isinstance(rr[0], dict):
                headers = _headers_from_dict_rows(rr) or headers
                if headers:
                    break

    if not headers:
        headers = ["Symbol", "Error"]

    symbol_col = _find_symbol_col(headers)
    union_keys = [_hkey(h) for h in headers]

    row_map: Dict[str, List[Any]] = {}
    all_rows: List[List[Any]] = []

    for r in responses:
        st = str(r.get("status") or "success")
        if st in ("error", "partial"):
            status = "partial"
        if st == "error" and not error_msg:
            error_msg = str(r.get("error") or "Backend error")

        hh = _extract_headers_candidate(r) or headers
        rr = _extract_rows_candidate(r) or []
        hh2, rr2 = _rows_to_grid(hh if isinstance(hh, list) else headers, rr)
        sym_idx = _find_symbol_col(hh2)

        hh_key_to_pos = {_hkey(hh2[i]): i for i in range(len(hh2)) if _hkey(hh2[i])}

        if sym_idx >= 0:
            for row in rr2:
                sym = ""
                try:
                    sym = str(row[sym_idx] or "").strip().upper()
                except Exception:
                    sym = ""
                if not sym:
                    continue

                aligned = [None] * len(headers)
                for j, uk in enumerate(union_keys):
                    src_pos = hh_key_to_pos.get(uk)
                    if src_pos is None:
                        continue
                    if src_pos < len(row):
                        aligned[j] = row[src_pos]
                row_map.setdefault(sym, aligned)
        else:
            for row in rr2:
                aligned = list(row) if isinstance(row, (list, tuple)) else [row]
                if len(aligned) < len(headers):
                    aligned += [None] * (len(headers) - len(aligned))
                elif len(aligned) > len(headers):
                    aligned = aligned[: len(headers)]
                all_rows.append(aligned)

    if symbol_col >= 0 and requested_symbols:
        merged: List[List[Any]] = []
        for s in requested_symbols:
            key = str(s or "").strip().upper()
            row = row_map.get(key)
            if row is None:
                placeholder = [None] * len(headers)
                placeholder[symbol_col] = key
                # try to fill Error column if exists
                err_idx = None
                for i, h in enumerate(headers):
                    if _hkey(h) == "error":
                        err_idx = i
                        break
                if err_idx is not None:
                    placeholder[err_idx] = "Missing row from backend"
                merged.append(placeholder)
                status = "partial" if status == "success" else status
            else:
                if len(row) < len(headers):
                    row = row + [None] * (len(headers) - len(row))
                elif len(row) > len(headers):
                    row = row[: len(headers)]
                merged.append(row)
        return headers, merged, status, error_msg

    return headers, all_rows, status, error_msg


def _call_backend_api(endpoint: str, payload: Dict[str, Any], *, query_params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    syms_any = payload.get("symbols") or payload.get("tickers") or []
    symbols = [str(t).strip() for t in (syms_any or []) if str(t).strip()]
    if not symbols:
        return {"headers": ["Symbol", "Error"], "rows": [], "status": "skipped", "error": "No symbols provided"}

    endpoint2 = _add_query_params(endpoint, query_params)

    chunks = _chunk_list(symbols, _BACKEND_MAX_SYMBOLS_PER_CALL)
    if len(chunks) <= 1:
        return _call_backend_api_once(endpoint2, payload)

    responses: List[Dict[str, Any]] = []
    first_headers: List[str] = []

    for ch in chunks:
        p2 = dict(payload)
        p2["symbols"] = ch
        p2["tickers"] = ch
        res = _call_backend_api_once(endpoint2, p2)
        responses.append(res)

        if not first_headers:
            hh = _extract_headers_candidate(res) or []
            if hh:
                first_headers = hh

    headers, rows, st, err = _merge_backend_responses(symbols, first_headers, responses)
    out: Dict[str, Any] = {"headers": headers, "rows": rows, "status": st}
    if err:
        out["error"] = err
    return out


# =============================================================================
# HIGH-LEVEL ORCHESTRATION
# =============================================================================
def _normalize_tickers(tickers: Sequence[str]) -> List[str]:
    out: List[str] = []
    seen = set()
    for t in tickers or []:
        raw = str(t or "").strip()
        if not raw:
            continue
        sym = _NORMALIZE_SYMBOL(raw) or raw.strip().upper()
        if not sym:
            continue
        if sym in seen:
            continue
        seen.add(sym)
        out.append(sym)
    return out


def _payload_for_endpoint(symbols_list: List[str], sheet_name: str, *, extra: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    payload: Dict[str, Any] = {
        "symbols": symbols_list,
        "tickers": symbols_list,
        "sheet_name": sheet_name,
        "sheetName": sheet_name,
        "meta": {"ts_utc": _utc_iso(), "service_version": SERVICE_VERSION},
    }
    if isinstance(extra, dict) and extra:
        payload.update(extra)
        payload["symbols"] = symbols_list
        payload["tickers"] = symbols_list
        payload["sheet_name"] = sheet_name
        payload["sheetName"] = sheet_name
        payload.setdefault("meta", {})
        if isinstance(payload["meta"], dict):
            payload["meta"].setdefault("service_version", SERVICE_VERSION)
            payload["meta"].setdefault("ts_utc", _utc_iso())
    return payload


def _refresh_logic(
    endpoint: str,
    spreadsheet_id: str,
    sheet_name: str,
    tickers: Sequence[str],
    start_cell: str = "A5",
    clear: bool = False,
    value_input: str = "RAW",
    *,
    mode: Optional[str] = None,
    backend_query_params: Optional[Dict[str, Any]] = None,
    backend_payload_extra: Optional[Dict[str, Any]] = None,
    allow_empty_headers: bool = False,
    allow_empty_rows: bool = False,
) -> Dict[str, Any]:
    """
    Flow:
    1) Call backend -> {headers, rows, status}
    2) SAFE MODE checks (empty headers/rows)
    3) Enforce canonical header order per sheet (+ append backend extras)
    4) Optional clear then write grid (chunked)
    """
    try:
        symbols_list = _normalize_tickers(tickers)
        if not symbols_list:
            return {"status": "skipped", "reason": "No tickers provided", "endpoint": endpoint, "sheet": sheet_name}

        try:
            sid = _require_spreadsheet_id(spreadsheet_id)
        except Exception as e:
            return _safe_status_error("spreadsheet_id", str(e), sheet=sheet_name, endpoint=endpoint)

        sh = (sheet_name or "").strip()
        if not sh:
            return _safe_status_error("sheet_name", "sheet_name is required", endpoint=endpoint)

        qp = dict(backend_query_params or {})
        if mode and str(mode).strip():
            qp.setdefault("mode", str(mode).strip())

        # 1) backend
        response = _call_backend_api(
            endpoint,
            _payload_for_endpoint(symbols_list, sh, extra=backend_payload_extra),
            query_params=qp or None,
        )

        backend_status = str(response.get("status") or "success")
        backend_error = response.get("error")

        headers = _extract_headers_candidate(response) or []
        rows_raw = _extract_rows_candidate(response) or []

        # SAFE MODE: protect from destructive clears/writes
        if _SAFE_MODE and not allow_empty_headers and not headers:
            return {
                "status": "blocked",
                "reason": "SAFE MODE: backend returned EMPTY headers",
                "endpoint": endpoint,
                "sheet": sh,
                "backend_status": backend_status,
                "backend_error": backend_error,
                "service_version": SERVICE_VERSION,
            }

        if _SAFE_MODE and _BLOCK_ON_EMPTY_ROWS and not allow_empty_rows and isinstance(rows_raw, list) and len(rows_raw) == 0:
            return {
                "status": "blocked",
                "reason": "SAFE MODE: backend returned EMPTY rows",
                "endpoint": endpoint,
                "sheet": sh,
                "backend_status": backend_status,
                "backend_error": backend_error,
                "service_version": SERVICE_VERSION,
            }

        # If backend omitted headers, use canonical
        if not headers:
            headers = _get_canonical_headers(sh)

        headers2, fixed_rows = _rows_to_grid(headers, rows_raw)

        # canonical order
        headers3, fixed_rows2 = _reorder_to_canonical(sh, headers2, fixed_rows)

        if not headers3:
            headers3 = ["Symbol", "Error"]

        grid = [headers3] + fixed_rows2

        # 3) clear
        if clear:
            start_col, start_row = _parse_a1_cell(start_cell)
            end_col = _CLEAR_END_COL
            if _SMART_CLEAR:
                end_col = _compute_clear_end_col(start_col, len(headers3))
            clear_rng = f"{_safe_sheet_name(sh)}!{_a1(start_col, start_row)}:{end_col}{_CLEAR_END_ROW}"
            try:
                clear_range(sid, clear_rng)
            except Exception as e:
                logger.warning("[SheetsService] Clear failed (continuing): %s", e)

        # 4) write
        try:
            updated_cells = write_grid_chunked(sid, sh, start_cell, grid, value_input=value_input)
        except Exception as e:
            return _safe_status_error(
                "write_grid_chunked",
                str(e),
                sheet=sh,
                endpoint=endpoint,
                rows=len(fixed_rows2),
                headers_count=len(headers3),
                backend_status=backend_status,
                backend_error=backend_error,
            )

        status = "success"
        if backend_status in ("error", "partial"):
            status = "partial"
        elif backend_status == "skipped":
            status = "skipped"

        return {
            "status": status,
            "sheet": sh,
            "endpoint": endpoint,
            "mode": (mode or ""),
            "rows_written": len(fixed_rows2),
            "headers_count": len(headers3),
            "cells_updated": int(updated_cells or 0),
            "backend_status": backend_status,
            "backend_error": backend_error,
            "backend_chunk_size": _BACKEND_MAX_SYMBOLS_PER_CALL,
            "safe_mode": bool(_SAFE_MODE),
            "service_version": SERVICE_VERSION,
        }

    except Exception as fatal:
        return _safe_status_error("refresh_logic", str(fatal), endpoint=endpoint, sheet=sheet_name)


# =============================================================================
# PUBLIC API (Backward-Compatible)
# =============================================================================
def refresh_sheet_with_enriched_quotes(
    spreadsheet_id: str = "",
    sheet_name: str = "",
    tickers: Sequence[str] = (),
    sid: str = "",
    **kwargs,
) -> Dict[str, Any]:
    spreadsheet_id = (spreadsheet_id or sid or "").strip()
    return _refresh_logic("/v1/enriched/sheet-rows", spreadsheet_id, sheet_name, tickers, **kwargs)


def refresh_sheet_with_ai_analysis(
    spreadsheet_id: str = "",
    sheet_name: str = "",
    tickers: Sequence[str] = (),
    sid: str = "",
    **kwargs,
) -> Dict[str, Any]:
    spreadsheet_id = (spreadsheet_id or sid or "").strip()
    return _refresh_logic("/v1/analysis/sheet-rows", spreadsheet_id, sheet_name, tickers, **kwargs)


def refresh_sheet_with_advanced_analysis(
    spreadsheet_id: str = "",
    sheet_name: str = "",
    tickers: Sequence[str] = (),
    sid: str = "",
    **kwargs,
) -> Dict[str, Any]:
    spreadsheet_id = (spreadsheet_id or sid or "").strip()
    return _refresh_logic("/v1/advanced/sheet-rows", spreadsheet_id, sheet_name, tickers, **kwargs)


# Async wrappers (runs sync functions in thread pool)
async def _run_async(func, *args, **kwargs):
    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:
        loop = asyncio.get_event_loop()
    return await loop.run_in_executor(None, lambda: func(*args, **kwargs))


async def refresh_sheet_with_enriched_quotes_async(*args, **kwargs):
    return await _run_async(refresh_sheet_with_enriched_quotes, *args, **kwargs)


async def refresh_sheet_with_ai_analysis_async(*args, **kwargs):
    return await _run_async(refresh_sheet_with_ai_analysis, *args, **kwargs)


async def refresh_sheet_with_advanced_analysis_async(*args, **kwargs):
    return await _run_async(refresh_sheet_with_advanced_analysis, *args, **kwargs)


__all__ = [
    "get_sheets_service",
    "read_range",
    "write_range",
    "clear_range",
    "write_grid_chunked",
    "refresh_sheet_with_enriched_quotes",
    "refresh_sheet_with_ai_analysis",
    "refresh_sheet_with_advanced_analysis",
    "_refresh_logic",
    "refresh_sheet_with_enriched_quotes_async",
    "refresh_sheet_with_ai_analysis_async",
    "refresh_sheet_with_advanced_analysis_async",
]
