```python
# google_sheets_service.py  (FULL REPLACEMENT)
"""
google_sheets_service.py
------------------------------------------------------------
Google Sheets helper for Tadawul Fast Bridge – v3.9.1 (Aligned + production-hardened)

What this module does
- Reads/Writes/Clears ranges in Google Sheets using a Service Account.
- Calls Tadawul Fast Bridge backend endpoints that return {headers, rows, status}.
- Writes data to Sheets in chunked mode to avoid request size limits.

Key upgrades (v3.9.1)
- Safer config resolution: env.py constants -> settings attrs -> raw env vars.
- Credentials parsing hardened (dict / JSON / quoted JSON / base64 JSON / escaped \\n).
- Google API retries improved (handles googleapiclient HttpError status codes).
- Backend retries improved (handles 429/5xx + Retry-After when present).
- Batch update made safer (splits large batch payloads).
- Public refresh_* never raises; always returns a status dict.
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
import urllib.request
from typing import Any, Dict, List, Optional, Sequence, Tuple

logger = logging.getLogger("google_sheets_service")

SERVICE_VERSION = "3.9.1"

# =============================================================================
# CONFIG IMPORT (env.py preferred -> core.config -> env vars)
# =============================================================================
def _safe_import(path: str) -> Optional[Any]:
    try:
        return __import__(path, fromlist=["*"])
    except Exception:
        return None


_env_mod = _safe_import("env")
_settings_from_env = getattr(_env_mod, "settings", None) if _env_mod else None

_core_cfg = _safe_import("core.config")
_get_settings = getattr(_core_cfg, "get_settings", None) if _core_cfg else None


def _cfg_obj() -> Any:
    """
    Preferred config object:
      1) env.settings (already loaded)
      2) core.config.get_settings()
      3) None
    """
    if _settings_from_env is not None:
        return _settings_from_env
    if callable(_get_settings):
        try:
            return _get_settings()
        except Exception:
            return None
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


def _get_int(names: Sequence[str], env_keys: Sequence[str], default: int) -> int:
    s = _cfg_obj()
    v = _get_attr_any(s, names)
    try:
        if v is not None:
            n = int(v)
            return n
    except Exception:
        pass
    try:
        return int(str(_get_env_any(env_keys, default)).strip())
    except Exception:
        return int(default)


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


def _get_bool(names: Sequence[str], env_keys: Sequence[str], default: bool) -> bool:
    s = _cfg_obj()
    v = _get_attr_any(s, names)
    if isinstance(v, bool):
        return v
    if v is not None:
        sv = str(v).strip().lower()
        if sv in {"1", "true", "yes", "y", "on", "t"}:
            return True
        if sv in {"0", "false", "no", "n", "off", "f"}:
            return False
    ev = _get_env_any(env_keys, None)
    if ev is None:
        return default
    sv = str(ev).strip().lower()
    if sv in {"1", "true", "yes", "y", "on", "t"}:
        return True
    if sv in {"0", "false", "no", "n", "off", "f"}:
        return False
    return default


# Prefer env.py constants if present (no side effects here; env.py may print banner once)
_BACKEND_URL = (
    (getattr(_env_mod, "BACKEND_BASE_URL", None) if _env_mod else None)
    or _get_str(["backend_base_url"], ["BACKEND_BASE_URL"], "")
).rstrip("/")

_APP_TOKEN = (
    (getattr(_env_mod, "APP_TOKEN", None) if _env_mod else None)
    or _get_str(["app_token"], ["APP_TOKEN"], "")
).strip()

_BACKUP_TOKEN = (
    (getattr(_env_mod, "BACKUP_APP_TOKEN", None) if _env_mod else None)
    or _get_str(["backup_app_token"], ["BACKUP_APP_TOKEN"], "")
).strip()

_DEFAULT_SHEET_ID = (
    (getattr(_env_mod, "DEFAULT_SPREADSHEET_ID", None) if _env_mod else None)
    or _get_str(["default_spreadsheet_id"], ["DEFAULT_SPREADSHEET_ID", "GOOGLE_SHEET_ID"], "")
).strip()

_HTTP_TIMEOUT = max(5.0, _get_float(["http_timeout_sec"], ["HTTP_TIMEOUT_SEC", "HTTP_TIMEOUT"], 25.0))

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
    raw = _get_attr_any(s, ["google_sheets_credentials_raw", "google_sheets_credentials", "google_sheets_credentials_json"])
    if isinstance(raw, str) and raw.strip():
        _CREDS_RAW = raw.strip()
except Exception:
    _CREDS_RAW = ""

if not _CREDS_RAW:
    _CREDS_RAW = (os.getenv("GOOGLE_SHEETS_CREDENTIALS", "") or "").strip()

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

# Safety: split large batch payloads (data entries) into multiple batchUpdate calls
_MAX_BATCH_RANGES = max(5, int(os.getenv("SHEETS_MAX_BATCH_RANGES", "25") or "25"))

# Clear defaults
_CLEAR_END_COL = (os.getenv("SHEETS_CLEAR_END_COL", "ZZ") or "ZZ").strip().upper()
_CLEAR_END_ROW = max(1000, int(os.getenv("SHEETS_CLEAR_END_ROW", "100000") or "100000"))

# Smart clear (compute end col from header count)
_SMART_CLEAR = _get_bool([], ["SHEETS_SMART_CLEAR"], True)

_USER_AGENT = (
    (os.getenv("SHEETS_USER_AGENT", "") or "").strip()
    or f"TadawulFastBridge-SheetsService/{SERVICE_VERSION}"
)

# Accept A1 and $A$1 variants; also accept "A1:ZZ100" (we take start cell).
_A1_RE = re.compile(r"^\$?([A-Za-z]+)\$?(\d+)$")


# =============================================================================
# LOW-LEVEL HELPERS
# =============================================================================
def _sleep_backoff(base: float, attempt: int, cap: float = 10.0) -> None:
    """Exponential backoff with jitter (seconds)."""
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
    """Convert literal '\\n' inside private_key to real newlines."""
    try:
        pk = creds_info.get("private_key")
        if isinstance(pk, str) and "\\n" in pk:
            creds_info["private_key"] = pk.replace("\\n", "\n")
    except Exception:
        pass
    return creds_info


def _maybe_b64_decode(s: str) -> str:
    """
    If credentials are accidentally base64-encoded, decode them.
    Safe: only decodes when it very likely is base64 JSON.
    """
    raw = (s or "").strip()
    if not raw:
        return raw
    if raw.startswith("{"):
        return raw
    if len(raw) < 200:
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
    """
    Accepts:
      - JSON string
      - JSON wrapped in quotes
      - base64 JSON (heuristic)
    """
    if not raw:
        return None

    cleaned = _maybe_b64_decode(_strip_wrapping_quotes(str(raw).strip()))
    cleaned = _strip_wrapping_quotes(cleaned)

    if not cleaned.startswith("{"):
        return None

    data = _safe_json_loads(cleaned)
    if isinstance(data, dict) and data:
        return _coerce_private_key(data)
    return None


def _safe_sheet_name(sheet_name: str) -> str:
    """Sheets API ranges accept 'Sheet Name'!A1. Single quotes must be doubled."""
    name = (sheet_name or "").strip().replace("'", "''")
    return f"'{name}'"


def _parse_a1_cell(cell: str) -> Tuple[str, int]:
    """
    Parse A1 cell like 'A1' -> ('A', 1)
    Accepts 'A1:ZZ100' -> uses 'A1'
    Defaults to ('A', 1) if invalid.
    """
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


# =============================================================================
# GOOGLE SHEETS SERVICE INITIALIZATION
# =============================================================================
def get_sheets_service():
    """Singleton accessor for Google Sheets API service."""
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

        # 1) Prefer parsed dict from config/env
        if isinstance(_CREDS_DICT, dict) and _CREDS_DICT:
            creds_info = _coerce_private_key(dict(_CREDS_DICT))

        # 2) Else parse raw JSON
        if creds_info is None:
            creds_info = _parse_credentials(_CREDS_RAW)

        # 3) Else env var direct
        if creds_info is None:
            creds_info = _parse_credentials(os.getenv("GOOGLE_SHEETS_CREDENTIALS", "") or "")

        if not isinstance(creds_info, dict) or not creds_info:
            raise RuntimeError("Missing or invalid GOOGLE_SHEETS_CREDENTIALS (service account JSON).")

        creds = Credentials.from_service_account_info(creds_info, scopes=_SCOPES)
        _SHEETS_SERVICE = build("sheets", "v4", credentials=creds, cache_discovery=False)
        logger.info("[GoogleSheets] Service initialized successfully.")
        return _SHEETS_SERVICE


# =============================================================================
# SHEETS API RETRY WRAPPER
# =============================================================================
def _http_error_status(e: Exception) -> Optional[int]:
    # googleapiclient HttpError
    try:
        if HttpError is not None and isinstance(e, HttpError):  # type: ignore
            resp = getattr(e, "resp", None)
            st = getattr(resp, "status", None)
            if st is not None:
                return int(st)
    except Exception:
        pass

    # fallback: parse from message
    msg = str(e)
    for code in (429, 500, 502, 503, 504):
        if str(code) in msg:
            return code
    return None


def _retry_sheet_op(operation_name: str, func, *args, **kwargs):
    """
    Retries Google Sheets API calls with exponential backoff.
    Retries likely transient errors (429, 5xx, quota/rate limit).
    """
    last_exc: Optional[Exception] = None
    for i in range(_SHEETS_RETRIES):
        try:
            return func(*args, **kwargs)
        except Exception as e:
            last_exc = e
            status = _http_error_status(e)
            msg = str(e)
            retryable = False

            if status in (429, 500, 502, 503, 504):
                retryable = True
            if any(
                x.lower() in msg.lower()
                for x in (
                    "quota",
                    "rate limit",
                    "ratelimitexceeded",
                    "userratelimitexceeded",
                    "backenderror",
                    "internal error",
                    "the service is currently unavailable",
                )
            ):
                retryable = True

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
        result = (
            service.spreadsheets()
            .values()
            .get(spreadsheetId=sid, range=range_name, majorDimension="ROWS")
            .execute()
        )
        return result.get("values", []) or []

    return _retry_sheet_op("Read Range", _do_read)


def write_range(spreadsheet_id: str, range_name: str, values: List[List[Any]], value_input: str = "RAW") -> int:
    service = get_sheets_service()
    sid = _require_spreadsheet_id(spreadsheet_id)

    body = {"values": values or [[]]}

    def _do_write():
        result = (
            service.spreadsheets()
            .values()
            .update(
                spreadsheetId=sid,
                range=range_name,
                valueInputOption=value_input,
                body=body,
            )
            .execute()
        )
        return int(result.get("updatedCells", 0) or 0)

    return _retry_sheet_op("Write Range", _do_write)


def clear_range(spreadsheet_id: str, range_name: str) -> None:
    service = get_sheets_service()
    sid = _require_spreadsheet_id(spreadsheet_id)

    def _do_clear():
        service.spreadsheets().values().clear(spreadsheetId=sid, range=range_name).execute()

    _retry_sheet_op("Clear Range", _do_clear)


def write_grid_chunked(spreadsheet_id: str, sheet_name: str, start_cell: str, grid: List[List[Any]]) -> int:
    """
    Writes a grid possibly in multiple calls (row chunking).

    - Writes header once, then writes remaining chunks below.
    - Uses values.batchUpdate when enabled (best-effort).
    - Splits batchUpdate payload when too many ranges.
    """
    if not grid:
        return 0

    sid = _require_spreadsheet_id(spreadsheet_id)
    start_col, start_row = _parse_a1_cell(start_cell)
    sheet_a1 = _safe_sheet_name(sheet_name)

    header = grid[0] if grid else []
    data_rows = grid[1:] if len(grid) > 1 else []

    # Normalize rows lengths to header length (best-effort)
    hdr_len = len(header) if isinstance(header, list) else 0
    if hdr_len > 0:
        fixed: List[List[Any]] = []
        for r in data_rows:
            rr = list(r) if isinstance(r, (list, tuple)) else [r]
            if len(rr) < hdr_len:
                rr += [None] * (hdr_len - len(rr))
            elif len(rr) > hdr_len:
                rr = rr[:hdr_len]
            fixed.append(rr)
        data_rows = fixed

    chunks = _chunk_rows(data_rows, _MAX_ROWS_PER_WRITE)

    # Header-only
    if not chunks:
        rng = f"{sheet_a1}!{_a1(start_col, start_row)}"
        return write_range(sid, rng, [header])

    # Attempt batchUpdate (best-effort)
    if _USE_BATCH_UPDATE:
        try:
            service = get_sheets_service()

            # Build ranges list
            data: List[Dict[str, Any]] = []
            rng0 = f"{sheet_a1}!{_a1(start_col, start_row)}"
            data.append({"range": rng0, "values": [header] + chunks[0]})

            current_row = start_row + 1 + len(chunks[0])
            for idx in range(1, len(chunks)):
                rng = f"{sheet_a1}!{_a1(start_col, current_row)}"
                data.append({"range": rng, "values": chunks[idx]})
                current_row += len(chunks[idx])

            def _do_batch(batch_data: List[Dict[str, Any]]) -> int:
                body = {"valueInputOption": "RAW", "data": batch_data}
                result = service.spreadsheets().values().batchUpdate(spreadsheetId=sid, body=body).execute()
                total = 0
                for r in (result.get("responses") or []):
                    total += int(r.get("updatedCells", 0) or 0)
                return total

            total_cells = 0
            # Split if too many ranges
            for i in range(0, len(data), _MAX_BATCH_RANGES):
                part = data[i : i + _MAX_BATCH_RANGES]
                total_cells += int(_retry_sheet_op("Batch Write Grid", _do_batch, part) or 0)

            return total_cells
        except Exception as e:
            logger.warning("[GoogleSheets] batchUpdate failed, fallback to chunked updates: %s", e)

    # Fallback sequential updates
    total_cells = 0
    rng0 = f"{sheet_a1}!{_a1(start_col, start_row)}"
    total_cells += write_range(sid, rng0, [header] + chunks[0])

    current_row = start_row + 1 + len(chunks[0])
    for idx in range(1, len(chunks)):
        rng = f"{sheet_a1}!{_a1(start_col, current_row)}"
        total_cells += write_range(sid, rng, chunks[idx])
        current_row += len(chunks[idx])

    return total_cells


# =============================================================================
# BACKEND API CLIENT (urllib)
# =============================================================================
def _backend_base_url() -> str:
    return (_BACKEND_URL or "http://127.0.0.1:8000").rstrip("/")


def _backend_headers(token: str) -> Dict[str, str]:
    h = {
        "Content-Type": "application/json; charset=utf-8",
        "User-Agent": _USER_AGENT,
        "Accept": "application/json, text/plain;q=0.9, */*;q=0.8",
    }
    if token:
        # Primary header used by this project
        h["X-APP-TOKEN"] = token
        # Extra compatibility for gateways/proxies (harmless if ignored)
        h["Authorization"] = f"Bearer {token}"
    return h


def _sheets_safe_error_payload(tickers: List[str], err: str) -> Dict[str, Any]:
    return {
        "headers": ["Symbol", "Error"],
        "rows": [[t, err] for t in (tickers or [])],
        "status": "error",
        "error": err,
    }


def _call_backend_api(endpoint: str, payload: Dict[str, Any]) -> Dict[str, Any]:
    """
    Calls the backend API to compute sheet rows.
    Returns a dict ALWAYS containing at least: headers, rows, status.
    """
    url = f"{_backend_base_url()}{endpoint}"

    tickers_any = payload.get("tickers") or payload.get("symbols") or []
    tickers = [str(t).strip() for t in (tickers_any or []) if str(t).strip()]

    if not tickers:
        return {"headers": ["Symbol", "Error"], "rows": [], "status": "skipped", "error": "No tickers provided"}

    body = json.dumps(payload, ensure_ascii=False).encode("utf-8")

    # token order: primary -> backup -> open-mode
    tokens_to_try: List[str] = []
    if _APP_TOKEN:
        tokens_to_try.append(_APP_TOKEN)
    if _BACKUP_TOKEN and _BACKUP_TOKEN != _APP_TOKEN:
        tokens_to_try.append(_BACKUP_TOKEN)
    if not tokens_to_try:
        tokens_to_try.append("")

    last_err: Optional[Exception] = None
    last_status: Optional[int] = None
    last_body_preview: Optional[str] = None

    attempts = max(1, int(_BACKEND_RETRIES) + 1)

    for attempt in range(attempts):
        for tok_idx, tok in enumerate(tokens_to_try):
            try:
                req = urllib.request.Request(url, data=body, headers=_backend_headers(tok), method="POST")

                with urllib.request.urlopen(req, timeout=_BACKEND_TIMEOUT, context=_SSL_CONTEXT) as resp:  # type: ignore
                    raw = resp.read().decode("utf-8", errors="replace")
                    last_status = int(getattr(resp, "status", 0) or 0)
                    last_body_preview = _safe_preview(raw, 400)

                    if 200 <= last_status < 300:
                        parsed = _safe_json_loads(raw)
                        if isinstance(parsed, dict):
                            # Always ensure keys exist
                            parsed.setdefault("status", "success")
                            parsed.setdefault("headers", [])
                            parsed.setdefault("rows", [])
                            return parsed
                        return _sheets_safe_error_payload(tickers, "Backend returned non-JSON response")

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

                # If unauthorized on primary token, try next token
                if last_status in (401, 403) and tok_idx < len(tokens_to_try) - 1:
                    logger.warning(
                        "[GoogleSheets] Backend auth failed (HTTP %s) token#%s -> trying next token.",
                        last_status,
                        tok_idx + 1,
                    )
                    continue

                # Retry transient HTTP statuses
                if last_status in (429, 500, 502, 503, 504):
                    ra = None
                    try:
                        ra = e.headers.get("Retry-After")  # type: ignore
                    except Exception:
                        ra = None
                    if attempt < attempts - 1:
                        if ra and str(ra).isdigit():
                            time.sleep(min(8.0, float(ra)))
                        else:
                            _sleep_backoff(_BACKEND_RETRY_SLEEP, attempt, cap=8.0)
                        continue

                logger.warning("[GoogleSheets] Backend HTTPError %s: %s", last_status, last_body_preview)

            except Exception as e:
                last_err = e
                logger.warning(
                    "[GoogleSheets] Backend call failed (%s/%s) endpoint=%s: %s",
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

    return _sheets_safe_error_payload(tickers, err_msg)


# =============================================================================
# HIGH-LEVEL ORCHESTRATION
# =============================================================================
def _normalize_tickers(tickers: Sequence[str]) -> List[str]:
    out: List[str] = []
    seen = set()
    for t in tickers or []:
        s = str(t or "").strip()
        if not s:
            continue
        su = s.upper()
        if su in seen:
            continue
        seen.add(su)
        out.append(s)
    return out


def _refresh_logic(
    endpoint: str,
    spreadsheet_id: str,
    sheet_name: str,
    tickers: Sequence[str],
    start_cell: str = "A5",
    clear: bool = False,
) -> Dict[str, Any]:
    """
    Core logic:
    1) Call backend to get {headers, rows, status}
    2) Optional clear old values first
    3) Write to Google Sheet (chunked)

    Never raises: always returns a status dict.
    """
    try:
        tickers_list = _normalize_tickers(tickers)
        if not tickers_list:
            return {"status": "skipped", "reason": "No tickers provided", "endpoint": endpoint, "sheet": sheet_name}

        try:
            sid = _require_spreadsheet_id(spreadsheet_id)
        except Exception as e:
            return _safe_status_error("spreadsheet_id", str(e), sheet=sheet_name, endpoint=endpoint)

        sh = (sheet_name or "").strip()
        if not sh:
            return _safe_status_error("sheet_name", "sheet_name is required", endpoint=endpoint)

        # 1) backend call (never raise outward)
        try:
            response = _call_backend_api(endpoint, {"tickers": tickers_list, "sheet_name": sh})
        except Exception as e:
            response = _sheets_safe_error_payload(tickers_list, f"Backend client error: {e}")

        headers = response.get("headers") or ["Symbol", "Error"]
        rows = response.get("rows") or []
        backend_status = response.get("status")
        backend_error = response.get("error")

        # sanitize headers
        headers = [str(h).strip() for h in (headers or []) if h and str(h).strip()]
        if not headers:
            headers = ["Symbol", "Error"]

        # fix row lengths
        fixed_rows: List[List[Any]] = []
        for r in rows:
            rr = list(r) if isinstance(r, (list, tuple)) else [r]
            if len(rr) < len(headers):
                rr += [None] * (len(headers) - len(rr))
            elif len(rr) > len(headers):
                rr = rr[: len(headers)]
            fixed_rows.append(rr)

        grid = [headers] + fixed_rows

        # 2) optional clear
        if clear:
            start_col, start_row = _parse_a1_cell(start_cell)
            end_col = _CLEAR_END_COL
            if _SMART_CLEAR:
                end_col = _compute_clear_end_col(start_col, len(headers))

            clear_rng = f"{_safe_sheet_name(sh)}!{_a1(start_col, start_row)}:{end_col}{_CLEAR_END_ROW}"
            try:
                clear_range(sid, clear_rng)
            except Exception as e:
                logger.warning("[GoogleSheets] Clear failed (continuing): %s", e)

        # 3) write
        try:
            updated_cells = write_grid_chunked(sid, sh, start_cell, grid)
        except Exception as e:
            return _safe_status_error(
                "write_grid_chunked",
                str(e),
                sheet=sh,
                endpoint=endpoint,
                rows=len(fixed_rows),
                headers_count=len(headers),
                backend_status=backend_status,
                backend_error=backend_error,
            )

        # surface final status
        status = "success"
        if backend_status == "error":
            status = "partial"  # we still wrote error rows
        elif backend_status == "skipped":
            status = "skipped"
        elif backend_status == "partial":
            status = "partial"

        return {
            "status": status,
            "sheet": sh,
            "endpoint": endpoint,
            "rows_written": len(fixed_rows),
            "headers_count": len(headers),
            "cells_updated": int(updated_cells or 0),
            "backend_status": backend_status,
            "backend_error": backend_error,
        }

    except Exception as fatal:
        # absolute last line of defense
        return _safe_status_error("refresh_logic", str(fatal), endpoint=endpoint, sheet=sheet_name)


# =============================================================================
# PUBLIC API (Backward-Compatible)
# =============================================================================
def refresh_sheet_with_enriched_quotes(
    spreadsheet_id: str,
    sheet_name: str,
    tickers: Sequence[str],
    **kwargs,
) -> Dict[str, Any]:
    return _refresh_logic("/v1/enriched/sheet-rows", spreadsheet_id, sheet_name, tickers, **kwargs)


def refresh_sheet_with_ai_analysis(
    spreadsheet_id: str,
    sheet_name: str,
    tickers: Sequence[str],
    **kwargs,
) -> Dict[str, Any]:
    return _refresh_logic("/v1/analysis/sheet-rows", spreadsheet_id, sheet_name, tickers, **kwargs)


def refresh_sheet_with_advanced_analysis(
    spreadsheet_id: str,
    sheet_name: str,
    tickers: Sequence[str],
    **kwargs,
) -> Dict[str, Any]:
    return _refresh_logic("/v1/advanced/sheet-rows", spreadsheet_id, sheet_name, tickers, **kwargs)


# --- Async wrappers (runs sync functions in thread pool) ---
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
    "refresh_sheet_with_enriched_quotes_async",
    "refresh_sheet_with_ai_analysis_async",
    "refresh_sheet_with_advanced_analysis_async",
]
```
