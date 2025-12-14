"""
google_sheets_service.py
------------------------------------------------------------
Google Sheets helper for Tadawul Fast Bridge – v3.2.0

GOALS
- Centralize ALL direct Google Sheets access (Read/Write/Clear).
- Orchestrate data fetching from the Backend API (/sheet-rows) to Sheets.
- Defensive: retries, chunking, order preservation, schema padding/truncation.
- Zero heavy deps for backend calls (urllib), but uses Google API libs for Sheets.

CONFIGURATION
- Prefers `from env import settings` if available.
- Env var fallbacks:
    BACKEND_BASE_URL
    APP_TOKEN / BACKUP_APP_TOKEN
    DEFAULT_SPREADSHEET_ID
    GOOGLE_SHEETS_CREDENTIALS (JSON)
    BACKEND_VERIFY_SSL (true/false)
    SHEETS_BACKEND_TIMEOUT_SEC (float)
    SHEETS_BACKEND_CHUNK_SIZE (int)
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import ssl
import time
import urllib.error
import urllib.request
from typing import Any, Dict, List, Optional

# -----------------------------------------------------------------------------
# CONFIGURATION IMPORT
# -----------------------------------------------------------------------------
try:
    from env import settings  # type: ignore

    _BACKEND_URL = (getattr(settings, "backend_base_url", "") or "").strip()
    _APP_TOKEN = (getattr(settings, "app_token", "") or "").strip()
    _BACKUP_TOKEN = (getattr(settings, "backup_app_token", "") or "").strip()
    _DEFAULT_SHEET_ID = (getattr(settings, "default_spreadsheet_id", "") or "").strip()
    _CREDS_RAW = (getattr(settings, "google_sheets_credentials_raw", "") or "").strip()
    _CREDS_DICT = getattr(settings, "google_sheets_credentials", None)
except Exception:
    settings = None  # type: ignore
    _BACKEND_URL = (os.getenv("BACKEND_BASE_URL", "") or "").strip()
    _APP_TOKEN = (os.getenv("APP_TOKEN", "") or "").strip()
    _BACKUP_TOKEN = (os.getenv("BACKUP_APP_TOKEN", "") or "").strip()
    _DEFAULT_SHEET_ID = (os.getenv("DEFAULT_SPREADSHEET_ID", "") or "").strip()
    _CREDS_RAW = (os.getenv("GOOGLE_SHEETS_CREDENTIALS", "") or "").strip()
    _CREDS_DICT = None

# -----------------------------------------------------------------------------
# GOOGLE API CLIENT IMPORT
# -----------------------------------------------------------------------------
try:
    from google.oauth2.service_account import Credentials  # type: ignore
    from googleapiclient.discovery import build  # type: ignore
except Exception:
    Credentials = None  # type: ignore
    build = None  # type: ignore

# -----------------------------------------------------------------------------
# LOGGING
# -----------------------------------------------------------------------------
logger = logging.getLogger("google_sheets_service")

# -----------------------------------------------------------------------------
# CONSTANTS
# -----------------------------------------------------------------------------
_SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]
_SHEETS_SERVICE = None  # singleton

_BACKEND_TIMEOUT = float(os.getenv("SHEETS_BACKEND_TIMEOUT_SEC", "120") or 120.0)
_BACKEND_VERIFY_SSL = (os.getenv("BACKEND_VERIFY_SSL", "true") or "true").strip().lower() not in (
    "0",
    "false",
    "no",
    "off",
)

_BACKEND_CHUNK_SIZE = int(os.getenv("SHEETS_BACKEND_CHUNK_SIZE", "250") or 250)

_SHEETS_RETRIES = 3
_SHEETS_RETRY_BASE_SLEEP = 1.0


# =============================================================================
# Service Initialization
# =============================================================================
def _parse_creds_dict() -> Dict[str, Any]:
    """
    Parse GOOGLE_SHEETS_CREDENTIALS from either:
      - already-parsed dict (settings.google_sheets_credentials)
      - raw json string (settings.google_sheets_credentials_raw / env var)
    Also fixes private_key newline escaping.
    """
    creds_info = _CREDS_DICT if isinstance(_CREDS_DICT, dict) else None

    if not creds_info and _CREDS_RAW:
        cleaned = _CREDS_RAW.strip()

        # strip wrapping quotes (common in Render env)
        if (cleaned.startswith("'") and cleaned.endswith("'")) or (cleaned.startswith('"') and cleaned.endswith('"')):
            cleaned = cleaned[1:-1].strip()

        try:
            creds_info = json.loads(cleaned)
        except Exception as e:
            raise RuntimeError(f"Failed to parse GOOGLE_SHEETS_CREDENTIALS JSON: {e}") from e

    if not creds_info:
        raise RuntimeError("Missing GOOGLE_SHEETS_CREDENTIALS (dict or JSON string).")

    # Fix newline escaping in private_key if present
    pk = creds_info.get("private_key")
    if isinstance(pk, str) and "\\n" in pk:
        creds_info["private_key"] = pk.replace("\\n", "\n")

    return creds_info


def get_sheets_service():
    """Singleton accessor for Google Sheets API Service."""
    global _SHEETS_SERVICE
    if _SHEETS_SERVICE:
        return _SHEETS_SERVICE

    if Credentials is None or build is None:
        raise RuntimeError(
            "Google API client libraries not installed. "
            "Install: pip install google-api-python-client google-auth"
        )

    creds_info = _parse_creds_dict()
    creds = Credentials.from_service_account_info(creds_info, scopes=_SCOPES)
    _SHEETS_SERVICE = build("sheets", "v4", credentials=creds, cache_discovery=False)
    logger.info("[GoogleSheets] Service initialized successfully.")
    return _SHEETS_SERVICE


# =============================================================================
# Helper Utilities
# =============================================================================
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


def _chunk_list(items: List[str], chunk_size: int) -> List[List[str]]:
    if chunk_size <= 0:
        return [items]
    return [items[i : i + chunk_size] for i in range(0, len(items), chunk_size)]


def _pad_or_trim_row(row: List[Any], width: int) -> List[Any]:
    if width <= 0:
        return row
    if len(row) < width:
        return row + [None] * (width - len(row))
    if len(row) > width:
        return row[:width]
    return row


def _normalize_symbol_for_match(sym: str) -> str:
    s = (sym or "").strip().upper()
    if not s:
        return ""
    # allow KSA variations: "1120" <-> "1120.SR"
    if s.isdigit():
        return s
    if s.endswith(".SR") and s[:-3].isdigit():
        return s
    return s


def _reorder_rows(headers: List[str], rows: List[List[Any]], requested_tickers: List[str]) -> List[List[Any]]:
    """
    Reorders data rows to match requested_tickers order.
    Assumes column 0 is the Symbol.

    Also pads/truncates each row to match len(headers).
    """
    if not rows or not requested_tickers:
        return [_pad_or_trim_row(r or [], len(headers)) for r in (rows or [])]

    width = len(headers)
    row_map: Dict[str, List[Any]] = {}

    for r in rows or []:
        r = _pad_or_trim_row(list(r or []), width)
        if not r:
            continue
        sym = _normalize_symbol_for_match(str(r[0]))
        if not sym:
            continue

        row_map[sym] = r

        # add KSA alias keys
        if sym.endswith(".SR") and sym[:-3].isdigit():
            row_map[sym[:-3]] = r
        elif sym.isdigit():
            row_map[f"{sym}.SR"] = r

    ordered: List[List[Any]] = []
    for t in requested_tickers or []:
        t_norm = _normalize_symbol_for_match(t)
        if not t_norm:
            continue

        row = row_map.get(t_norm)
        if row is None:
            # missing placeholder
            row = _pad_or_trim_row([t_norm], width)
        ordered.append(row)

    return ordered


def _quote_sheet_name(sheet_name: str) -> str:
    """
    Quote sheet name safely for A1 notation.
    Handles single quotes by doubling them.
    """
    s = (sheet_name or "").replace("'", "''")
    return f"'{s}'"


# =============================================================================
# Backend API Client (Internal)
# =============================================================================
def _ssl_context() -> ssl.SSLContext:
    if _BACKEND_VERIFY_SSL:
        return ssl.create_default_context()
    # explicit opt-out (dev only)
    ctx = ssl.create_default_context()
    ctx.check_hostname = False
    ctx.verify_mode = ssl.CERT_NONE
    return ctx


def _pick_token() -> str:
    return _APP_TOKEN or _BACKUP_TOKEN or ""


def _call_backend_api(endpoint: str, payload: Dict[str, Any]) -> Dict[str, Any]:
    """
    Calls the backend API to fetch {headers, rows}.
    Uses urllib for minimal dependencies.

    Returns a safe fallback response on failure.
    """
    base = (_BACKEND_URL or "http://127.0.0.1:8000").rstrip("/")
    ep = (endpoint or "").strip()
    if not ep.startswith("/"):
        ep = "/" + ep
    url = f"{base}{ep}"

    headers = {
        "Content-Type": "application/json; charset=utf-8",
        "Accept": "application/json",
        "User-Agent": "TadawulFastBridge-SheetsService/3.2.0",
    }

    token = _pick_token()
    if token:
        headers["X-APP-TOKEN"] = token

    data_bytes = json.dumps(payload, ensure_ascii=False).encode("utf-8")
    req = urllib.request.Request(url, data=data_bytes, headers=headers, method="POST")

    retries = 2
    last_err: Optional[Exception] = None

    for attempt in range(retries + 1):
        try:
            with urllib.request.urlopen(req, timeout=_BACKEND_TIMEOUT, context=_ssl_context()) as resp:
                raw = resp.read().decode("utf-8", errors="replace")
                if 200 <= getattr(resp, "status", 200) < 300:
                    return json.loads(raw) if raw else {"headers": [], "rows": []}
                raise RuntimeError(f"Backend returned HTTP {getattr(resp, 'status', 'unknown')}: {raw[:300]}")
        except urllib.error.HTTPError as e:
            body = ""
            try:
                body = e.read().decode("utf-8", errors="replace")
            except Exception:
                body = ""
            last_err = RuntimeError(f"HTTPError {e.code}: {body[:400]}")
        except Exception as e:
            last_err = e

        logger.warning(
            "[GoogleSheets] Backend call %s attempt %d/%d failed: %s",
            endpoint,
            attempt + 1,
            retries + 1,
            last_err,
        )
        time.sleep(1.0 * (attempt + 1))

    # Safe fallback response (never crash caller)
    tickers = payload.get("tickers") or []
    err_msg = str(last_err) if last_err else "Unknown backend error"
    logger.error("[GoogleSheets] Backend failed after retries: %s", err_msg)

    return {
        "headers": ["Symbol", "Error"],
        "rows": [[(t or "").strip().upper(), err_msg] for t in tickers],
        "meta": {"status": "backend_failed"},
    }


def _call_backend_api_chunked(endpoint: str, tickers: List[str], sheet_name: str) -> Dict[str, Any]:
    """
    Chunk tickers to avoid oversized HTTP payloads.
    Merges rows while preserving requested order.
    """
    clean = [(t or "").strip().upper() for t in (tickers or []) if (t or "").strip()]
    if not clean:
        return {"headers": [], "rows": []}

    chunks = _chunk_list(clean, max(1, _BACKEND_CHUNK_SIZE))
    merged_headers: List[str] = []
    merged_rows: List[List[Any]] = []

    for i, ch in enumerate(chunks, start=1):
        resp = _call_backend_api(endpoint, {"tickers": ch, "sheet_name": sheet_name})

        headers = resp.get("headers") or []
        rows = resp.get("rows") or []

        if not merged_headers:
            merged_headers = list(headers)
        else:
            # If backend changes schema between chunks, keep first schema and log
            if list(headers) != merged_headers:
                logger.warning(
                    "[GoogleSheets] Schema mismatch between chunks. Keeping first schema. "
                    "chunk=%d headers_len=%d first_headers_len=%d",
                    i,
                    len(headers),
                    len(merged_headers),
                )

        merged_rows.extend(rows)

    return {"headers": merged_headers, "rows": merged_rows}


# =============================================================================
# Sheets Operations (Read/Write/Clear)
# =============================================================================
def _retry_sheet_op(operation_name: str, func, *args, **kwargs):
    """Retries Google Sheets API calls with exponential backoff on quota/5xx."""
    last_exc: Optional[Exception] = None
    for i in range(_SHEETS_RETRIES):
        try:
            return func(*args, **kwargs)
        except Exception as e:
            last_exc = e
            msg = str(e)
            is_retryable = any(code in msg for code in ("429", "500", "503", "Rate Limit", "quota"))
            if is_retryable:
                sleep_time = _SHEETS_RETRY_BASE_SLEEP * (2 ** i)
                logger.warning("[GoogleSheets] %s retryable error. Sleep %.1fs. Err=%s", operation_name, sleep_time, e)
                time.sleep(sleep_time)
            else:
                raise
    raise last_exc if last_exc else RuntimeError(f"{operation_name} failed (unknown error)")


def write_range(spreadsheet_id: str, range_name: str, values: List[List[Any]]) -> int:
    service = get_sheets_service()
    body = {"values": values}

    def _do_write():
        result = (
            service.spreadsheets()
            .values()
            .update(
                spreadsheetId=spreadsheet_id,
                range=range_name,
                valueInputOption="RAW",
                body=body,
            )
            .execute()
        )
        return int(result.get("updatedCells", 0) or 0)

    return int(_retry_sheet_op("Write Range", _do_write) or 0)


def clear_range(spreadsheet_id: str, range_name: str) -> None:
    service = get_sheets_service()

    def _do_clear():
        service.spreadsheets().values().clear(spreadsheetId=spreadsheet_id, range=range_name).execute()

    _retry_sheet_op("Clear Range", _do_clear)


# =============================================================================
# High-Level Orchestration
# =============================================================================
def _refresh_logic(
    endpoint: str,
    spreadsheet_id: str,
    sheet_name: str,
    tickers: List[str],
    start_cell: str = "A1",
    clear: bool = False,
    clear_end_col: str = "ZZ",
    clear_max_rows: int = 20000,
) -> Dict[str, Any]:
    """
    Core logic:
      1) Call backend to get data (headers + rows).
      2) Reorder rows to match ticker input.
      3) Optional clear (to prevent old rows remaining when list shrinks).
      4) Write headers + rows to sheet.

    Notes:
    - Writes starting at start_cell.
    - If clear=True, clears a broad area: start_cell:ZZ{max_rows}.
    """
    clean = [(t or "").strip().upper() for t in (tickers or []) if (t or "").strip()]
    if not clean:
        return {"status": "skipped", "reason": "No tickers provided"}

    sid = (spreadsheet_id or _DEFAULT_SHEET_ID or "").strip()
    if not sid:
        raise ValueError("No Spreadsheet ID provided (spreadsheet_id or DEFAULT_SPREADSHEET_ID).")

    sname = (sheet_name or "").strip()
    if not sname:
        raise ValueError("Sheet name is required.")

    # 1) Fetch
    logger.info("[GoogleSheets] Fetching %d tickers from %s -> %s", len(clean), endpoint, sname)
    response = _call_backend_api_chunked(endpoint, clean, sname)

    headers = list(response.get("headers") or [])
    rows = list(response.get("rows") or [])

    if not headers:
        # Safe minimal schema so sheets write doesn't fail
        headers = ["Symbol", "Error"]
        rows = [[t, "Backend returned empty headers"] for t in clean]

    # 2) Reorder + pad
    ordered_rows = _reorder_rows(headers, rows, clean)
    grid = [headers] + ordered_rows

    # 3) Clear
    quoted = _quote_sheet_name(sname)
    target_range = f"{quoted}!{start_cell}"

    if clear:
        # Clear a safe box that covers typical dashboard sizes
        clear_range_name = f"{quoted}!{start_cell}:{clear_end_col}{max(1, int(clear_max_rows))}"
        clear_range(sid, clear_range_name)

    # 4) Write
    updated_cells = write_range(sid, target_range, grid)

    return {
        "status": "success",
        "sheet": sname,
        "endpoint": endpoint,
        "tickers_requested": len(clean),
        "rows_written": len(ordered_rows),
        "cells_updated": updated_cells,
        "market_split": split_tickers_by_market(clean),
    }


# =============================================================================
# Public API (Sync)
# =============================================================================
def refresh_sheet_with_enriched_quotes(sid: str, sheet_name: str, tickers: List[str], **kwargs):
    return _refresh_logic("/v1/enriched/sheet-rows", sid, sheet_name, tickers, **kwargs)


def refresh_sheet_with_ai_analysis(sid: str, sheet_name: str, tickers: List[str], **kwargs):
    return _refresh_logic("/v1/analysis/sheet-rows", sid, sheet_name, tickers, **kwargs)


def refresh_sheet_with_advanced_analysis(sid: str, sheet_name: str, tickers: List[str], **kwargs):
    return _refresh_logic("/v1/advanced/sheet-rows", sid, sheet_name, tickers, **kwargs)


# =============================================================================
# Async wrappers (safe for FastAPI background tasks / scripts)
# =============================================================================
async def _run_async(func, *args, **kwargs):
    loop = asyncio.get_running_loop()
    return await loop.run_in_executor(None, lambda: func(*args, **kwargs))


async def refresh_sheet_with_enriched_quotes_async(*args, **kwargs):
    return await _run_async(refresh_sheet_with_enriched_quotes, *args, **kwargs)


async def refresh_sheet_with_ai_analysis_async(*args, **kwargs):
    return await _run_async(refresh_sheet_with_ai_analysis, *args, **kwargs)


async def refresh_sheet_with_advanced_analysis_async(*args, **kwargs):
    return await _run_async(refresh_sheet_with_advanced_analysis, *args, **kwargs)


__all__ = [
    "get_sheets_service",
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
