"""
google_sheets_service.py
------------------------------------------------------------
Google Sheets helper for Tadawul Fast Bridge – v3.2.0

Key Fixes / Enhancements
- Adds missing read_range() (symbols_reader depends on it).
- Safer clear_range() with proper A1 range.
- Chunked writes for large payloads (prevents request size issues).
- Stronger backend call retry/backoff + better error payloads.
- Keeps backward-compatible exports for your existing codebase.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import re
import ssl
import time
import urllib.error
import urllib.request
from typing import Any, Dict, List, Optional, Tuple

# --- CONFIGURATION IMPORT ---
try:
    from env import settings
    _BACKEND_URL = (settings.backend_base_url or "").rstrip("/")
    _APP_TOKEN = settings.app_token
    _BACKUP_TOKEN = settings.backup_app_token
    _DEFAULT_SHEET_ID = settings.default_spreadsheet_id
    _CREDS_RAW = settings.google_sheets_credentials_raw
    _CREDS_DICT = settings.google_sheets_credentials
    _HTTP_TIMEOUT = float(getattr(settings, "http_timeout_sec", 25.0) or 25.0)
except Exception:
    settings = None  # type: ignore
    _BACKEND_URL = (os.getenv("BACKEND_BASE_URL", "") or "").rstrip("/")
    _APP_TOKEN = os.getenv("APP_TOKEN", "")
    _BACKUP_TOKEN = os.getenv("BACKUP_APP_TOKEN", "")
    _DEFAULT_SHEET_ID = os.getenv("DEFAULT_SPREADSHEET_ID", "")
    _CREDS_RAW = os.getenv("GOOGLE_SHEETS_CREDENTIALS", "")
    _CREDS_DICT = None
    _HTTP_TIMEOUT = float(os.getenv("HTTP_TIMEOUT_SEC", os.getenv("HTTP_TIMEOUT", "25")))

# --- GOOGLE API CLIENT IMPORT ---
try:
    from google.oauth2.service_account import Credentials
    from googleapiclient.discovery import build
except ImportError:
    Credentials = None  # type: ignore
    build = None  # type: ignore

# --- LOGGING ---
logger = logging.getLogger("google_sheets_service")
_SSL_CONTEXT = ssl.create_default_context()

# --- CONSTANTS ---
_SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]
_SHEETS_SERVICE = None  # Singleton

# Backend calls can be longer for /sheet-rows
_BACKEND_TIMEOUT = float(os.getenv("SHEETS_BACKEND_TIMEOUT_SEC", "120") or "120")
_BACKEND_TIMEOUT = max(_BACKEND_TIMEOUT, _HTTP_TIMEOUT)

_SHEETS_RETRIES = int(os.getenv("SHEETS_API_RETRIES", "3") or "3")
_SHEETS_RETRY_BASE_SLEEP = float(os.getenv("SHEETS_API_RETRY_BASE_SLEEP", "1.0") or "1.0")

# Write chunking
_MAX_ROWS_PER_WRITE = int(os.getenv("SHEETS_MAX_ROWS_PER_WRITE", "500") or "500")

# =============================================================================
# Service Initialization
# =============================================================================

def get_sheets_service():
    """Singleton accessor for Google Sheets API Service."""
    global _SHEETS_SERVICE
    if _SHEETS_SERVICE:
        return _SHEETS_SERVICE

    if not Credentials or not build:
        raise RuntimeError(
            "Google API client libraries not installed. "
            "Run `pip install google-api-python-client google-auth`"
        )

    creds_info = _CREDS_DICT
    if not creds_info and _CREDS_RAW:
        try:
            cleaned = str(_CREDS_RAW).strip()
            if (cleaned.startswith("'") and cleaned.endswith("'")) or (cleaned.startswith('"') and cleaned.endswith('"')):
                cleaned = cleaned[1:-1].strip()
            creds_info = json.loads(cleaned)
            if isinstance(creds_info, dict) and "private_key" in creds_info:
                creds_info["private_key"] = str(creds_info["private_key"]).replace("\\n", "\n")
        except Exception as e:
            logger.error("Failed to parse GOOGLE_SHEETS_CREDENTIALS: %s", e)

    if not creds_info or not isinstance(creds_info, dict):
        raise RuntimeError("Missing or invalid GOOGLE_SHEETS_CREDENTIALS in env.")

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

def _chunk_rows(values: List[List[Any]], max_rows: int) -> List[List[List[Any]]]:
    if max_rows <= 0:
        return [values]
    return [values[i:i + max_rows] for i in range(0, len(values), max_rows)]

_A1_RE = re.compile(r"^([A-Za-z]+)(\d+)$")

def _parse_a1_cell(cell: str) -> Tuple[str, int]:
    """
    Parse A1 cell like 'A1' -> ('A', 1)
    Defaults to ('A', 1) if invalid.
    """
    m = _A1_RE.match((cell or "").strip())
    if not m:
        return ("A", 1)
    return (m.group(1).upper(), int(m.group(2)))

def _a1(col: str, row: int) -> str:
    return f"{col.upper()}{int(row)}"

# =============================================================================
# Backend API Client (Internal)
# =============================================================================

def _call_backend_api(endpoint: str, payload: Dict[str, Any]) -> Dict[str, Any]:
    """
    Calls the backend API to calculate data.
    Uses urllib (no extra dependency).
    """
    base = (_BACKEND_URL or "http://127.0.0.1:8000").rstrip("/")
    url = f"{base}{endpoint}"

    headers = {
        "Content-Type": "application/json; charset=utf-8",
        "User-Agent": "TadawulFastBridge-SheetsService/3.2",
    }

    token = (_APP_TOKEN or _BACKUP_TOKEN or "").strip()
    if token:
        headers["X-APP-TOKEN"] = token

    body = json.dumps(payload, ensure_ascii=False).encode("utf-8")

    retries = int(os.getenv("SHEETS_BACKEND_RETRIES", "2") or "2")
    base_sleep = float(os.getenv("SHEETS_BACKEND_RETRY_SLEEP", "1.0") or "1.0")

    last_err: Optional[Exception] = None

    for attempt in range(retries + 1):
        try:
            req = urllib.request.Request(url, data=body, headers=headers, method="POST")
            with urllib.request.urlopen(req, timeout=_BACKEND_TIMEOUT, context=_SSL_CONTEXT) as resp:
                raw = resp.read().decode("utf-8", errors="replace")
                if 200 <= resp.status < 300:
                    return json.loads(raw)
                raise RuntimeError(f"Backend returned HTTP {resp.status}: {raw[:300]}")
        except Exception as e:
            last_err = e
            sleep = base_sleep * (2 ** attempt)
            logger.warning("[GoogleSheets] Backend call %s attempt %s failed: %s", endpoint, attempt + 1, e)
            time.sleep(min(sleep, 8.0))

    # Safe fallback (never crash callers)
    tickers = payload.get("tickers") or payload.get("symbols") or []
    return {
        "headers": ["Symbol", "Error"],
        "rows": [[t, str(last_err)] for t in tickers],
        "status": "error",
        "error": str(last_err),
    }

# =============================================================================
# Sheets Operations (Read/Write/Clear)
# =============================================================================

def _retry_sheet_op(operation_name: str, func, *args, **kwargs):
    """Retries Google Sheets API calls with exponential backoff."""
    last_exc = None
    for i in range(_SHEETS_RETRIES):
        try:
            return func(*args, **kwargs)
        except Exception as e:
            last_exc = e
            msg = str(e)
            is_retryable = any(code in msg for code in ("429", "500", "503", "quota", "Rate Limit"))
            if is_retryable:
                sleep_time = _SHEETS_RETRY_BASE_SLEEP * (2 ** i)
                logger.warning("[GoogleSheets] %s retry in %ss due to: %s", operation_name, sleep_time, e)
                time.sleep(min(sleep_time, 10.0))
            else:
                raise
    raise last_exc  # type: ignore[misc]

def read_range(spreadsheet_id: str, range_name: str) -> List[List[Any]]:
    service = get_sheets_service()

    def _do_read():
        result = service.spreadsheets().values().get(
            spreadsheetId=spreadsheet_id,
            range=range_name,
            majorDimension="ROWS"
        ).execute()
        return result.get("values", [])

    return _retry_sheet_op("Read Range", _do_read)

def write_range(spreadsheet_id: str, range_name: str, values: List[List[Any]], value_input: str = "RAW") -> int:
    service = get_sheets_service()
    body = {"values": values}

    def _do_write():
        result = service.spreadsheets().values().update(
            spreadsheetId=spreadsheet_id,
            range=range_name,
            valueInputOption=value_input,
            body=body
        ).execute()
        return int(result.get("updatedCells", 0) or 0)

    return _retry_sheet_op("Write Range", _do_write)

def clear_range(spreadsheet_id: str, range_name: str) -> None:
    service = get_sheets_service()

    def _do_clear():
        service.spreadsheets().values().clear(
            spreadsheetId=spreadsheet_id,
            range=range_name
        ).execute()

    _retry_sheet_op("Clear Range", _do_clear)

def write_grid_chunked(spreadsheet_id: str, sheet_name: str, start_cell: str, grid: List[List[Any]]) -> int:
    """
    Writes a grid possibly in multiple calls (row chunking).
    Preserves header row by writing it only once.
    """
    if not grid:
        return 0

    start_col, start_row = _parse_a1_cell(start_cell)
    header = grid[0]
    data_rows = grid[1:]

    # 1) write header + first chunk
    total_cells = 0
    chunks = _chunk_rows(data_rows, _MAX_ROWS_PER_WRITE)
    if not chunks:
        # Only header
        rng = f"'{sheet_name}'!{_a1(start_col, start_row)}"
        total_cells += write_range(spreadsheet_id, rng, [header])
        return total_cells

    first_block = [header] + chunks[0]
    rng0 = f"'{sheet_name}'!{_a1(start_col, start_row)}"
    total_cells += write_range(spreadsheet_id, rng0, first_block)

    # 2) write remaining chunks below (no header)
    for idx in range(1, len(chunks)):
        offset_row = start_row + 1 + (idx * _MAX_ROWS_PER_WRITE)
        rng = f"'{sheet_name}'!{_a1(start_col, offset_row)}"
        total_cells += write_range(spreadsheet_id, rng, chunks[idx])

    return total_cells

# =============================================================================
# High-Level Orchestration
# =============================================================================

def _refresh_logic(
    endpoint: str,
    spreadsheet_id: str,
    sheet_name: str,
    tickers: List[str],
    start_cell: str = "A5",
    clear: bool = False
) -> Dict[str, Any]:
    """
    Core logic:
    1) Call backend to get {headers, rows}
    2) Write to Google Sheet (chunked)
    3) Optional clear old values first
    """
    tickers = [str(t).strip() for t in (tickers or []) if str(t).strip()]
    if not tickers:
        return {"status": "skipped", "reason": "No tickers provided"}

    sid = (spreadsheet_id or _DEFAULT_SHEET_ID or "").strip()
    if not sid:
        raise ValueError("No Spreadsheet ID provided (DEFAULT_SPREADSHEET_ID missing).")

    logger.info("[GoogleSheets] Fetching %s tickers from %s for sheet=%s", len(tickers), endpoint, sheet_name)

    response = _call_backend_api(endpoint, {"tickers": tickers, "sheet_name": sheet_name})
    headers = response.get("headers") or ["Symbol"]
    rows = response.get("rows") or []

    # Ensure row length == header length
    fixed_rows: List[List[Any]] = []
    for r in rows:
        r = list(r) if isinstance(r, (list, tuple)) else [r]
        if len(r) < len(headers):
            r = r + [None] * (len(headers) - len(r))
        elif len(r) > len(headers):
            r = r[:len(headers)]
        fixed_rows.append(r)

    grid = [headers] + fixed_rows

    # Clear (safe large range) if requested
    if clear:
        start_col, start_row = _parse_a1_cell(start_cell)
        clear_rng = f"'{sheet_name}'!{_a1(start_col, start_row)}:ZZ100000"
        clear_range(sid, clear_rng)

    updated_cells = write_grid_chunked(sid, sheet_name, start_cell, grid)

    return {
        "status": "success",
        "rows_written": len(fixed_rows),
        "cells_updated": updated_cells,
        "sheet": sheet_name,
        "headers": len(headers),
    }

# --- Synchronous Wrappers ---
def refresh_sheet_with_enriched_quotes(sid: str, sheet_name: str, tickers: List[str], **kwargs):
    return _refresh_logic("/v1/enriched/sheet-rows", sid, sheet_name, tickers, **kwargs)

def refresh_sheet_with_ai_analysis(sid: str, sheet_name: str, tickers: List[str], **kwargs):
    return _refresh_logic("/v1/analysis/sheet-rows", sid, sheet_name, tickers, **kwargs)

def refresh_sheet_with_advanced_analysis(sid: str, sheet_name: str, tickers: List[str], **kwargs):
    return _refresh_logic("/v1/advanced/sheet-rows", sid, sheet_name, tickers, **kwargs)

# --- Async Wrappers ---
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
