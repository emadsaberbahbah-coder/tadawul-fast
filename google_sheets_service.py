# google_sheets_service.py
"""
google_sheets_service.py
------------------------------------------------------------
Google Sheets helper for Tadawul Fast Bridge – v3.8.0 (Aligned + production-hardened)

What this module does
- Reads/Writes/Clears ranges in Google Sheets using a Service Account.
- Calls Tadawul Fast Bridge backend endpoints that return {headers, rows}.
- Writes data to Sheets in chunked mode to avoid request size limits.

Key Improvements (v3.8.0)
- env.py alignment:
    • uses settings.google_credentials_dict / google_credentials_dict (new)
    • supports legacy settings.google_sheets_credentials / google_sheets_credentials_raw if present
    • respects settings.default_spreadsheet_id + settings.backend_base_url
- Credentials parsing hardened further:
    • accepts JSON, quoted JSON, extra whitespace, escaped \\n private_key
- Backend client hardening:
    • retries + exponential backoff + jitter
    • switches to BACKUP_APP_TOKEN on 401/403 automatically
    • safe body preview (never huge) + deterministic Sheets-safe payload on failures
- Range safety:
    • A1 parsing accepts "A5" or "A5:ZZ" (uses start cell)
    • safer sheet-name quoting (escapes single quotes)
    • configurable clear end-col/end-row
- Write performance:
    • optional values.batchUpdate (best-effort)
    • safe fallback to sequential updates
- Never crashes callers:
    • public refresh_* functions return status dict even on errors
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import random
import re
import ssl
import time
import urllib.error
import urllib.request
from typing import Any, Dict, List, Optional, Tuple

# =============================================================================
# CONFIG IMPORT (env.py preferred, then raw env vars)
# =============================================================================

try:
    # env.py exports "settings" sourced from config.get_settings()
    from env import settings  # type: ignore

    _BACKEND_URL = (getattr(settings, "backend_base_url", "") or "").rstrip("/")
    _APP_TOKEN = (getattr(settings, "app_token", None) or "") if settings else ""
    _BACKUP_TOKEN = (getattr(settings, "backup_app_token", None) or "") if settings else ""
    _DEFAULT_SHEET_ID = (getattr(settings, "default_spreadsheet_id", None) or "") if settings else ""
    _HTTP_TIMEOUT = float(getattr(settings, "http_timeout_sec", 25.0) or 25.0) if settings else 25.0

    # ✅ NEW (preferred): config/env exposes parsed dict
    _CREDS_DICT = getattr(settings, "google_credentials_dict", None)

    # Legacy fields (if user still has them in config.py)
    _CREDS_RAW = (
        getattr(settings, "google_sheets_credentials_raw", None)
        or getattr(settings, "google_sheets_credentials", None)
        or ""
    )
    # Sometimes older configs stored the dict under different names
    if _CREDS_DICT is None:
        _CREDS_DICT = getattr(settings, "google_sheets_credentials", None)

except Exception:
    settings = None  # type: ignore
    _BACKEND_URL = (os.getenv("BACKEND_BASE_URL", "") or "").rstrip("/")
    _APP_TOKEN = (os.getenv("APP_TOKEN", "") or "").strip()
    _BACKUP_TOKEN = (os.getenv("BACKUP_APP_TOKEN", "") or "").strip()
    _DEFAULT_SHEET_ID = (os.getenv("DEFAULT_SPREADSHEET_ID", "") or "").strip()
    _HTTP_TIMEOUT = float(os.getenv("HTTP_TIMEOUT_SEC", os.getenv("HTTP_TIMEOUT", "25")) or "25")
    _CREDS_RAW = (os.getenv("GOOGLE_SHEETS_CREDENTIALS", "") or "").strip()
    _CREDS_DICT = None

# If env var exists, allow it as raw override in non-config cases
if not _CREDS_RAW:
    _CREDS_RAW = (os.getenv("GOOGLE_SHEETS_CREDENTIALS", "") or "").strip()

# =============================================================================
# GOOGLE API CLIENT IMPORT
# =============================================================================

try:
    from google.oauth2.service_account import Credentials  # type: ignore
    from googleapiclient.discovery import build  # type: ignore
except Exception:
    Credentials = None  # type: ignore
    build = None  # type: ignore

# =============================================================================
# LOGGING / SSL
# =============================================================================

logger = logging.getLogger("google_sheets_service")
_SSL_CONTEXT = ssl.create_default_context()

# =============================================================================
# CONSTANTS
# =============================================================================

_SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]
_SHEETS_SERVICE = None  # singleton

# Backend calls can be longer for /sheet-rows
_BACKEND_TIMEOUT = float(os.getenv("SHEETS_BACKEND_TIMEOUT_SEC", "120") or "120")
_BACKEND_TIMEOUT = max(_BACKEND_TIMEOUT, _HTTP_TIMEOUT)

# Sheets API retries
_SHEETS_RETRIES = int(os.getenv("SHEETS_API_RETRIES", "3") or "3")
_SHEETS_RETRY_BASE_SLEEP = float(os.getenv("SHEETS_API_RETRY_BASE_SLEEP", "1.0") or "1.0")

# Backend retries
_BACKEND_RETRIES = int(os.getenv("SHEETS_BACKEND_RETRIES", "2") or "2")
_BACKEND_RETRY_SLEEP = float(os.getenv("SHEETS_BACKEND_RETRY_SLEEP", "1.0") or "1.0")

# Write chunking
_MAX_ROWS_PER_WRITE = int(os.getenv("SHEETS_MAX_ROWS_PER_WRITE", "500") or "500")
_USE_BATCH_UPDATE = str(os.getenv("SHEETS_USE_BATCH_UPDATE", "1") or "1").strip().lower() in {"1", "true", "yes", "on"}

# Clear range defaults
_CLEAR_END_COL = (os.getenv("SHEETS_CLEAR_END_COL", "ZZ") or "ZZ").strip()
_CLEAR_END_ROW = int(os.getenv("SHEETS_CLEAR_END_ROW", "100000") or "100000")

# Request headers
_USER_AGENT = os.getenv("SHEETS_USER_AGENT", "TadawulFastBridge-SheetsService/3.8").strip() or "TadawulFastBridge-SheetsService/3.8"

# =============================================================================
# LOW-LEVEL HELPERS
# =============================================================================

# Accept A1 and $A$1 variants; also accept "A1:ZZ100" (we take start cell).
_A1_RE = re.compile(r"^\$?([A-Za-z]+)\$?(\d+)$")


def _sleep_backoff(base: float, attempt: int, cap: float = 10.0) -> None:
    """Exponential backoff with jitter (seconds)."""
    t = base * (2 ** attempt)
    t = min(t, cap)
    t = t * (0.85 + 0.3 * random.random())
    time.sleep(t)


def _safe_json_loads(raw: str) -> Optional[Any]:
    try:
        return json.loads(raw)
    except Exception:
        return None


def _coerce_private_key(creds_info: Dict[str, Any]) -> Dict[str, Any]:
    """
    Google service account JSON often contains private_key with literal '\\n'.
    Convert to real newlines.
    """
    try:
        pk = creds_info.get("private_key")
        if isinstance(pk, str) and "\\n" in pk:
            creds_info["private_key"] = pk.replace("\\n", "\n")
    except Exception:
        pass
    return creds_info


def _parse_credentials_from_env(raw: str) -> Optional[Dict[str, Any]]:
    """
    Accepts:
      - raw JSON object string
      - same JSON wrapped in single/double quotes
      - accidental leading/trailing whitespace
    Rejects anything not starting with '{' after stripping.
    """
    if not raw:
        return None

    cleaned = str(raw).strip()

    # strip wrapping quotes if present
    if (cleaned.startswith("'") and cleaned.endswith("'")) or (cleaned.startswith('"') and cleaned.endswith('"')):
        cleaned = cleaned[1:-1].strip()

    if not cleaned.startswith("{"):
        return None

    data = _safe_json_loads(cleaned)
    if isinstance(data, dict) and data:
        return _coerce_private_key(data)
    return None


def _safe_sheet_name(sheet_name: str) -> str:
    """
    Sheets API ranges accept 'Sheet Name'!A1.
    Single quotes inside names must be doubled.
    """
    name = (sheet_name or "").strip()
    name = name.replace("'", "''")
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
    return [values[i: i + max_rows] for i in range(0, len(values), max_rows)]


def split_tickers_by_market(tickers: List[str]) -> Dict[str, List[str]]:
    """Split tickers for logging/diagnostics."""
    ksa, glob = [], []
    for t in tickers or []:
        clean = (t or "").strip().upper()
        if not clean:
            continue
        if clean.endswith(".SR") or clean.isdigit():
            ksa.append(clean)
        else:
            glob.append(clean)
    return {"ksa": ksa, "global": glob}


def _require_spreadsheet_id(spreadsheet_id: str) -> str:
    sid = (spreadsheet_id or _DEFAULT_SHEET_ID or "").strip()
    if not sid:
        raise ValueError("No Spreadsheet ID provided (DEFAULT_SPREADSHEET_ID missing).")
    return sid


def _safe_status_error(where: str, err: str, **extra) -> Dict[str, Any]:
    out = {"status": "error", "where": where, "error": err}
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

    if not Credentials or not build:
        raise RuntimeError(
            "Google API client libraries not installed. "
            "Install: pip install google-api-python-client google-auth"
        )

    creds_info: Optional[Dict[str, Any]] = None

    # 1) Prefer parsed dict from env/config if available
    if isinstance(_CREDS_DICT, dict) and _CREDS_DICT:
        creds_info = _coerce_private_key(dict(_CREDS_DICT))

    # 2) Else parse raw env var JSON
    if creds_info is None:
        creds_info = _parse_credentials_from_env(_CREDS_RAW)

    if not isinstance(creds_info, dict) or not creds_info:
        raise RuntimeError("Missing or invalid GOOGLE_SHEETS_CREDENTIALS (service account JSON).")

    creds = Credentials.from_service_account_info(creds_info, scopes=_SCOPES)
    _SHEETS_SERVICE = build("sheets", "v4", credentials=creds, cache_discovery=False)
    logger.info("[GoogleSheets] Service initialized successfully.")
    return _SHEETS_SERVICE


# =============================================================================
# SHEETS API RETRY WRAPPER
# =============================================================================

def _retry_sheet_op(operation_name: str, func, *args, **kwargs):
    """
    Retries Google Sheets API calls with exponential backoff.
    Only retries likely transient errors (429, 5xx, quota).
    """
    last_exc = None
    attempts = max(1, int(_SHEETS_RETRIES))

    for i in range(attempts):
        try:
            return func(*args, **kwargs)
        except Exception as e:
            last_exc = e
            msg = str(e)
            retryable = any(x in msg for x in ("429", "500", "502", "503", "quota", "Rate Limit", "rateLimitExceeded"))
            if retryable and i < attempts - 1:
                logger.warning("[GoogleSheets] %s retry (%s/%s): %s", operation_name, i + 1, attempts, e)
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
    Writes header once, then appends remaining chunks below.

    Uses values.batchUpdate when enabled (fewer API calls) – best-effort.
    """
    if not grid:
        return 0

    sid = _require_spreadsheet_id(spreadsheet_id)

    start_col, start_row = _parse_a1_cell(start_cell)
    sheet_a1 = _safe_sheet_name(sheet_name)

    header = grid[0] if grid else []
    data_rows = grid[1:] if len(grid) > 1 else []

    chunks = _chunk_rows(data_rows, _MAX_ROWS_PER_WRITE)

    # Only header
    if not chunks:
        rng = f"{sheet_a1}!{_a1(start_col, start_row)}"
        return write_range(sid, rng, [header])

    # If batchUpdate enabled, try one call
    if _USE_BATCH_UPDATE:
        try:
            service = get_sheets_service()

            data: List[Dict[str, Any]] = []

            # First block includes header + first chunk
            rng0 = f"{sheet_a1}!{_a1(start_col, start_row)}"
            data.append({"range": rng0, "values": [header] + chunks[0]})

            # Remaining blocks
            current_row = start_row + 1 + len(chunks[0])
            for idx in range(1, len(chunks)):
                rng = f"{sheet_a1}!{_a1(start_col, current_row)}"
                data.append({"range": rng, "values": chunks[idx]})
                current_row += len(chunks[idx])

            body = {"valueInputOption": "RAW", "data": data}

            def _do_batch():
                result = service.spreadsheets().values().batchUpdate(spreadsheetId=sid, body=body).execute()
                total = 0
                for r in (result.get("responses") or []):
                    total += int(r.get("updatedCells", 0) or 0)
                return total

            return int(_retry_sheet_op("Batch Write Grid", _do_batch) or 0)

        except Exception as e:
            logger.warning("[GoogleSheets] batchUpdate failed, fallback to chunked updates: %s", e)

    # Fallback: sequential updates
    total_cells = 0

    # Header + first chunk
    rng0 = f"{sheet_a1}!{_a1(start_col, start_row)}"
    total_cells += write_range(sid, rng0, [header] + chunks[0])

    # Remaining chunks
    current_row = start_row + 1 + len(chunks[0])
    for idx in range(1, len(chunks)):
        rng = f"{sheet_a1}!{_a1(start_col, current_row)}"
        total_cells += write_range(sid, rng, chunks[idx])
        current_row += len(chunks[idx])

    return total_cells


# =============================================================================
# BACKEND API CLIENT (urllib)
# =============================================================================

def _backend_headers(token: str) -> Dict[str, str]:
    h = {
        "Content-Type": "application/json; charset=utf-8",
        "User-Agent": _USER_AGENT,
        "Accept": "application/json",
    }
    if token:
        h["X-APP-TOKEN"] = token
    return h


def _backend_base_url() -> str:
    return (_BACKEND_URL or "http://127.0.0.1:8000").rstrip("/")


def _sheets_safe_error_payload(tickers: List[str], err: str) -> Dict[str, Any]:
    # Always Sheets-writeable: headers+rows exist
    return {
        "headers": ["Symbol", "Error"],
        "rows": [[t, err] for t in (tickers or [])],
        "status": "error",
        "error": err,
    }


def _call_backend_api(endpoint: str, payload: Dict[str, Any]) -> Dict[str, Any]:
    """
    Calls the backend API to compute sheet rows.
    Uses urllib + retries + token fallback.

    Returns dict ALWAYS containing at least:
      - headers: [...]
      - rows: [...]
      - status: success|error|skipped|partial
    """
    url = f"{_backend_base_url()}{endpoint}"

    tickers = payload.get("tickers") or payload.get("symbols") or []
    tickers = [str(t).strip() for t in (tickers or []) if str(t).strip()]

    if not tickers:
        return {"headers": ["Symbol", "Error"], "rows": [], "status": "skipped", "error": "No tickers provided"}

    body = json.dumps(payload, ensure_ascii=False).encode("utf-8")

    tokens_to_try: List[str] = []
    p = (_APP_TOKEN or "").strip()
    b = (_BACKUP_TOKEN or "").strip()
    if p:
        tokens_to_try.append(p)
    if b and b != p:
        tokens_to_try.append(b)
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
                with urllib.request.urlopen(req, timeout=_BACKEND_TIMEOUT, context=_SSL_CONTEXT) as resp:
                    raw = resp.read().decode("utf-8", errors="replace")
                    last_status = int(getattr(resp, "status", 0) or 0)
                    last_body_preview = raw[:400]

                    if 200 <= last_status < 300:
                        parsed = _safe_json_loads(raw)
                        if isinstance(parsed, dict):
                            parsed.setdefault("status", "success")
                            parsed.setdefault("headers", [])
                            parsed.setdefault("rows", [])
                            return parsed
                        return _sheets_safe_error_payload(tickers, "Backend returned non-JSON response")

                    raise RuntimeError(f"Backend HTTP {last_status}: {raw[:400]}")

            except urllib.error.HTTPError as e:
                last_err = e
                last_status = int(getattr(e, "code", 0) or 0)
                try:
                    raw = e.read().decode("utf-8", errors="replace")  # type: ignore
                except Exception:
                    raw = str(e)
                last_body_preview = raw[:400]

                # If unauthorized and this was primary token, try backup token next
                if last_status in (401, 403) and tok_idx < len(tokens_to_try) - 1:
                    logger.warning(
                        "[GoogleSheets] Backend auth failed (HTTP %s) on token #%s; trying next token.",
                        last_status,
                        tok_idx + 1,
                    )
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
    if last_body_preview and (last_status in (400, 401, 403, 404, 500, 502, 503)):
        err_msg = f"{err_msg} | body: {last_body_preview}"

    return _sheets_safe_error_payload(tickers, err_msg)


# =============================================================================
# HIGH-LEVEL ORCHESTRATION
# =============================================================================

def _normalize_tickers(tickers: List[str]) -> List[str]:
    out: List[str] = []
    for t in tickers or []:
        s = str(t or "").strip()
        if s:
            out.append(s)
    return out


def _refresh_logic(
    endpoint: str,
    spreadsheet_id: str,
    sheet_name: str,
    tickers: List[str],
    start_cell: str = "A5",
    clear: bool = False,
) -> Dict[str, Any]:
    """
    Core logic:
    1) Call backend to get {headers, rows}
    2) Write to Google Sheet (chunked)
    3) Optional clear old values first
    """
    tickers = _normalize_tickers(tickers)
    if not tickers:
        return {"status": "skipped", "reason": "No tickers provided"}

    try:
        sid = _require_spreadsheet_id(spreadsheet_id)
    except Exception as e:
        return _safe_status_error("spreadsheet_id", str(e), sheet=sheet_name, endpoint=endpoint)

    sh = (sheet_name or "").strip()
    if not sh:
        return _safe_status_error("sheet_name", "sheet_name is required", endpoint=endpoint)

    try:
        logger.info("[GoogleSheets] backend=%s tickers=%s sheet=%s endpoint=%s", _backend_base_url(), len(tickers), sh, endpoint)
        response = _call_backend_api(endpoint, {"tickers": tickers, "sheet_name": sh})
    except Exception as e:
        # Extremely defensive: even if backend client raises unexpectedly
        response = _sheets_safe_error_payload(tickers, f"Backend client error: {e}")

    headers = response.get("headers") or ["Symbol", "Error"]
    rows = response.get("rows") or []

    # Clean headers
    headers = [str(h).strip() for h in headers if h and str(h).strip()]
    if not headers:
        headers = ["Symbol", "Error"]

    # Ensure rows align with headers
    fixed_rows: List[List[Any]] = []
    for r in rows:
        rr = list(r) if isinstance(r, (list, tuple)) else [r]
        if len(rr) < len(headers):
            rr += [None] * (len(headers) - len(rr))
        elif len(rr) > len(headers):
            rr = rr[: len(headers)]
        fixed_rows.append(rr)

    grid = [headers] + fixed_rows

    # Optional clear old values first (large safe range)
    if clear:
        start_col, start_row = _parse_a1_cell(start_cell)
        clear_rng = f"{_safe_sheet_name(sh)}!{_a1(start_col, start_row)}:{_CLEAR_END_COL}{_CLEAR_END_ROW}"
        try:
            clear_range(sid, clear_rng)
        except Exception as e:
            logger.warning("[GoogleSheets] Clear failed (continuing): %s", e)

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
            backend_status=response.get("status"),
            backend_error=response.get("error"),
        )

    backend_status = response.get("status")
    status = "success"
    if backend_status == "error":
        status = "partial"  # wrote error rows
    elif backend_status == "skipped":
        status = "skipped"

    return {
        "status": status,
        "rows_written": len(fixed_rows),
        "cells_updated": int(updated_cells or 0),
        "sheet": sh,
        "headers_count": len(headers),
        "backend_status": backend_status,
        "backend_error": response.get("error"),
    }


# =============================================================================
# PUBLIC API (Backward-Compatible)
# =============================================================================

def refresh_sheet_with_enriched_quotes(sid: str, sheet_name: str, tickers: List[str], **kwargs):
    return _refresh_logic("/v1/enriched/sheet-rows", sid, sheet_name, tickers, **kwargs)


def refresh_sheet_with_ai_analysis(sid: str, sheet_name: str, tickers: List[str], **kwargs):
    return _refresh_logic("/v1/analysis/sheet-rows", sid, sheet_name, tickers, **kwargs)


def refresh_sheet_with_advanced_analysis(sid: str, sheet_name: str, tickers: List[str], **kwargs):
    return _refresh_logic("/v1/advanced/sheet-rows", sid, sheet_name, tickers, **kwargs)


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
    "split_tickers_by_market",
    "refresh_sheet_with_enriched_quotes",
    "refresh_sheet_with_ai_analysis",
    "refresh_sheet_with_advanced_analysis",
    "refresh_sheet_with_enriched_quotes_async",
    "refresh_sheet_with_ai_analysis_async",
    "refresh_sheet_with_advanced_analysis_async",
]
