"""
google_sheets_service.py
------------------------------------------------------------
Google Sheets helper for Tadawul Fast Bridge – v3.0.0

GOALS
- Centralize ALL direct Google Sheets access (Read/Write/Clear).
- Orchestrate data fetching from the Backend API (/sheet-rows) to Sheets.
- Defensive: Retries, Chunking, Order Preservation.

CONFIGURATION
- Reads from `env.py`.
- Requires `GOOGLE_SHEETS_CREDENTIALS` and `DEFAULT_SPREADSHEET_ID`.
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
from typing import Any, Dict, List, Optional, Tuple

# --- CONFIGURATION IMPORT ---
try:
    from env import settings
    # Fallback constants
    _BACKEND_URL = settings.backend_base_url
    _APP_TOKEN = settings.app_token
    _BACKUP_TOKEN = settings.backup_app_token
    _DEFAULT_SHEET_ID = settings.default_spreadsheet_id
    _CREDS_RAW = settings.google_sheets_credentials_raw
    _CREDS_DICT = settings.google_sheets_credentials
except ImportError:
    # Fallback for environments without the full app stack
    import os
    settings = None # type: ignore
    _BACKEND_URL = os.getenv("BACKEND_BASE_URL", "")
    _APP_TOKEN = os.getenv("APP_TOKEN", "")
    _BACKUP_TOKEN = os.getenv("BACKUP_APP_TOKEN", "")
    _DEFAULT_SHEET_ID = os.getenv("DEFAULT_SPREADSHEET_ID", "")
    _CREDS_RAW = os.getenv("GOOGLE_SHEETS_CREDENTIALS", "")
    _CREDS_DICT = None

# --- GOOGLE API CLIENT IMPORT ---
try:
    from google.oauth2.service_account import Credentials
    from googleapiclient.discovery import build
    from googleapiclient.errors import HttpError
except ImportError:
    Credentials = None # type: ignore
    build = None # type: ignore
    class HttpError(Exception): # type: ignore
        pass

# --- LOGGING ---
logger = logging.getLogger("google_sheets_service")
_SSL_CONTEXT = ssl.create_default_context()

# --- CONSTANTS ---
_SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]
_SHEETS_SERVICE = None  # Singleton
_BACKEND_TIMEOUT = 120.0 # Match Procfile timeout
_SHEETS_RETRIES = 3
_SHEETS_RETRY_BASE_SLEEP = 1.0

# =============================================================================
# Service Initialization
# =============================================================================

def get_sheets_service():
    """Singleton accessor for Google Sheets API Service."""
    global _SHEETS_SERVICE
    if _SHEETS_SERVICE:
        return _SHEETS_SERVICE

    if not Credentials or not build:
        raise RuntimeError("Google API client libraries not installed. Run `pip install google-api-python-client google-auth`")

    creds_info = _CREDS_DICT
    if not creds_info and _CREDS_RAW:
        try:
            # Handle potential escaping issues in env vars
            cleaned = _CREDS_RAW.strip()
            if (cleaned.startswith("'") and cleaned.endswith("'")) or (cleaned.startswith('"') and cleaned.endswith('"')):
                cleaned = cleaned[1:-1]
            creds_info = json.loads(cleaned)
            # Fix newline escaping in private key
            if "private_key" in creds_info:
                creds_info["private_key"] = creds_info["private_key"].replace("\\n", "\n")
        except Exception as e:
            logger.error(f"Failed to parse GOOGLE_SHEETS_CREDENTIALS: {e}")

    if not creds_info:
        raise RuntimeError("Missing GOOGLE_SHEETS_CREDENTIALS in env.")

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
    for t in tickers:
        clean = (t or "").strip().upper()
        if not clean: continue
        if clean.endswith(".SR") or clean.isdigit():
            ksa.append(clean)
        else:
            glob.append(clean)
    return {"ksa": ksa, "global": glob}

def _chunk_list(items: List[str], chunk_size: int) -> List[List[str]]:
    if chunk_size <= 0: return [items]
    return [items[i : i + chunk_size] for i in range(0, len(items), chunk_size)]

def _reorder_rows(headers: List[str], rows: List[List[Any]], requested_tickers: List[str]) -> List[List[Any]]:
    """
    Reorders data rows to match the exact order of requested_tickers.
    Assumes column 0 is the Symbol.
    """
    if not rows or not requested_tickers:
        return rows

    # Normalize ticker map
    row_map = {}
    for r in rows:
        if r and len(r) > 0:
            sym = str(r[0]).strip().upper()
            # Handle potential suffix differences
            row_map[sym] = r
            if sym.endswith(".SR"):
                row_map[sym.replace(".SR", "")] = r
            elif sym.isdigit():
                row_map[f"{sym}.SR"] = r

    ordered_rows = []
    for t in requested_tickers:
        t_norm = t.strip().upper()
        # Try exact match, then suffix variations
        row = row_map.get(t_norm) or row_map.get(t_norm.replace(".SR", "")) or row_map.get(f"{t_norm}.SR")
        
        if row:
            ordered_rows.append(row)
        else:
            # Pad empty row if missing
            ordered_rows.append([t_norm] + [None] * (len(headers) - 1))
            
    return ordered_rows

# =============================================================================
# Backend API Client (Internal)
# =============================================================================

def _call_backend_api(endpoint: str, payload: Dict[str, Any]) -> Dict[str, Any]:
    """
    Calls the local/remote backend API to calculate data.
    Uses urllib for zero-dependency operation.
    """
    base = (_BACKEND_URL or "http://127.0.0.1:8000").rstrip("/")
    url = f"{base}{endpoint}"
    
    headers = {
        "Content-Type": "application/json; charset=utf-8",
        "User-Agent": "TadawulFastBridge-SheetsService/3.0"
    }
    
    token = _APP_TOKEN or _BACKUP_TOKEN
    if token:
        headers["X-APP-TOKEN"] = token

    data_bytes = json.dumps(payload).encode("utf-8")
    req = urllib.request.Request(url, data=data_bytes, headers=headers, method="POST")

    retries = 2
    last_err = None

    for attempt in range(retries + 1):
        try:
            with urllib.request.urlopen(req, timeout=_BACKEND_TIMEOUT, context=_SSL_CONTEXT) as resp:
                if 200 <= resp.status < 300:
                    return json.loads(resp.read().decode("utf-8"))
                else:
                    raise RuntimeError(f"Backend returned HTTP {resp.status}")
        except Exception as e:
            last_err = e
            logger.warning(f"[GoogleSheets] Backend call to {endpoint} attempt {attempt+1} failed: {e}")
            time.sleep(1.0 * (attempt + 1))

    # Construct a safe fallback response if backend is dead
    logger.error(f"[GoogleSheets] Backend failed after retries: {last_err}")
    return {"headers": ["Symbol", "Error"], "rows": [[t, str(last_err)] for t in payload.get("tickers", [])]}

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
            is_quota = "429" in str(e) or "500" in str(e) or "503" in str(e)
            if is_quota:
                sleep_time = _SHEETS_RETRY_BASE_SLEEP * (2 ** i)
                logger.warning(f"[GoogleSheets] {operation_name} hit rate limit. Retrying in {sleep_time}s...")
                time.sleep(sleep_time)
            else:
                raise e
    raise last_exc

def write_range(spreadsheet_id: str, range_name: str, values: List[List[Any]]) -> int:
    service = get_sheets_service()
    body = {"values": values}
    
    def _do_write():
        result = service.spreadsheets().values().update(
            spreadsheetId=spreadsheet_id,
            range=range_name,
            valueInputOption="RAW",
            body=body
        ).execute()
        return result.get("updatedCells", 0)

    return _retry_sheet_op("Write Range", _do_write)

def clear_range(spreadsheet_id: str, range_name: str) -> None:
    service = get_sheets_service()
    def _do_clear():
        service.spreadsheets().values().clear(
            spreadsheetId=spreadsheet_id, range=range_name
        ).execute()
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
    clear: bool = False
) -> Dict[str, Any]:
    """
    Core logic: 
    1. Call Backend to get data (headers + rows).
    2. Reorder rows to match ticker input.
    3. Clear sheet (optional).
    4. Write headers + rows to sheet.
    """
    if not tickers:
        return {"status": "skipped", "reason": "No tickers provided"}

    # 1. Fetch Data
    # Use chunking at this layer to avoid massive HTTP payloads if list is > 500
    # But for simplicity, we rely on backend chunking, sending all tickers at once 
    # (backend routers handle internal chunking).
    
    logger.info(f"[GoogleSheets] Fetching data for {len(tickers)} tickers from {endpoint}")
    response = _call_backend_api(endpoint, {
        "tickers": tickers,
        "sheet_name": sheet_name # Essential for backend to pick schema
    })
    
    headers = response.get("headers", [])
    rows = response.get("rows", [])
    
    # 2. Reorder
    ordered_rows = _reorder_rows(headers, rows, tickers)
    
    # Construct final grid
    grid = [headers] + ordered_rows
    
    # 3. Clear & Write
    sid = spreadsheet_id or _DEFAULT_SHEET_ID
    if not sid:
        raise ValueError("No Spreadsheet ID provided.")

    target_range = f"'{sheet_name}'!{start_cell}"
    
    if clear:
        # Clear everything below header row roughly, or specific range
        # Safer to just overwrite, but if list shrinks, old data remains.
        # We assume start_cell is top-left.
        # Let's clear from start_row+1 down to be safe, or just rely on overwrite.
        # Ideally, we clear the specific range we are about to write + extra.
        # For robustness, we won't clear explicitly to save quota, unless requested.
        clear_range(sid, f"'{sheet_name}'!{start_cell}:ZZ")

    updated = write_range(sid, target_range, grid)
    
    return {
        "status": "success",
        "rows_written": len(ordered_rows),
        "cells_updated": updated,
        "sheet": sheet_name
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
    "write_range",
    "clear_range",
    "refresh_sheet_with_enriched_quotes",
    "refresh_sheet_with_ai_analysis",
    "refresh_sheet_with_advanced_analysis",
    "refresh_sheet_with_enriched_quotes_async",
    "refresh_sheet_with_ai_analysis_async",
    "refresh_sheet_with_advanced_analysis_async"
]
