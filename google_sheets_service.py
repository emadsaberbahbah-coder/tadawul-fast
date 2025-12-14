"""
google_sheets_service.py
------------------------------------------------------------
Google Sheets helper for Tadawul Fast Bridge – v3.2.0

GOALS
- Centralize ALL Google Sheets access (Read/Write/Clear).
- Orchestrate backend /sheet-rows -> Google Sheets.
- Defensive: retries, chunking, order preservation.

Requires:
- GOOGLE_SHEETS_CREDENTIALS (service account JSON as string)
- DEFAULT_SPREADSHEET_ID
"""

from __future__ import annotations

import asyncio
import json
import logging
import ssl
import time
import urllib.request
from typing import Any, Dict, List, Optional, Tuple

from env import settings

try:
    from google.oauth2.service_account import Credentials
    from googleapiclient.discovery import build
except Exception:  # pragma: no cover
    Credentials = None  # type: ignore
    build = None  # type: ignore

logger = logging.getLogger("google_sheets_service")
_SSL_CONTEXT = ssl.create_default_context()

_SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]
_SHEETS_SERVICE = None

_BACKEND_TIMEOUT = 120.0
_SHEETS_RETRIES = 3
_SHEETS_RETRY_BASE_SLEEP = 1.0

# =============================================================================
# Service Initialization
# =============================================================================
def _parse_service_account_json(raw: Optional[str]) -> Optional[Dict[str, Any]]:
    if not raw:
        return None
    cleaned = str(raw).strip()
    if (cleaned.startswith("'") and cleaned.endswith("'")) or (cleaned.startswith('"') and cleaned.endswith('"')):
        cleaned = cleaned[1:-1].strip()
    try:
        creds_info = json.loads(cleaned)
        if isinstance(creds_info, dict) and "private_key" in creds_info:
            creds_info["private_key"] = creds_info["private_key"].replace("\\n", "\n")
        return creds_info if isinstance(creds_info, dict) else None
    except Exception as e:
        logger.error("Failed to parse GOOGLE_SHEETS_CREDENTIALS JSON: %s", e)
        return None

def get_sheets_service():
    """Singleton accessor for Google Sheets API Service."""
    global _SHEETS_SERVICE
    if _SHEETS_SERVICE:
        return _SHEETS_SERVICE

    if not Credentials or not build:
        raise RuntimeError("Google client libs missing. Install google-api-python-client + google-auth.")

    creds_info = settings.google_sheets_credentials or _parse_service_account_json(settings.google_sheets_credentials_raw)
    if not creds_info:
        raise RuntimeError("Missing/invalid GOOGLE_SHEETS_CREDENTIALS.")

    creds = Credentials.from_service_account_info(creds_info, scopes=_SCOPES)
    _SHEETS_SERVICE = build("sheets", "v4", credentials=creds, cache_discovery=False)
    logger.info("[GoogleSheets] Service initialized.")
    return _SHEETS_SERVICE

# =============================================================================
# Utilities
# =============================================================================
def split_tickers_by_market(tickers: List[str]) -> Dict[str, List[str]]:
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

def _retry_sheet_op(operation_name: str, func, *args, **kwargs):
    last_exc = None
    for i in range(_SHEETS_RETRIES):
        try:
            return func(*args, **kwargs)
        except Exception as e:
            last_exc = e
            msg = str(e)
            retryable = any(code in msg for code in ["429", "500", "503"])
            if retryable:
                sleep_time = _SHEETS_RETRY_BASE_SLEEP * (2 ** i)
                logger.warning("[GoogleSheets] %s retry in %ss: %s", operation_name, sleep_time, e)
                time.sleep(sleep_time)
            else:
                raise
    raise last_exc

# =============================================================================
# Sheets Operations
# =============================================================================
def read_range(spreadsheet_id: str, range_name: str) -> List[List[Any]]:
    service = get_sheets_service()

    def _do_read():
        resp = service.spreadsheets().values().get(spreadsheetId=spreadsheet_id, range=range_name).execute()
        return resp.get("values", [])

    return _retry_sheet_op("Read Range", _do_read)

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
        return int(result.get("updatedCells", 0) or 0)

    return _retry_sheet_op("Write Range", _do_write)

def clear_range(spreadsheet_id: str, range_name: str) -> None:
    service = get_sheets_service()

    def _do_clear():
        service.spreadsheets().values().clear(spreadsheetId=spreadsheet_id, range=range_name).execute()

    _retry_sheet_op("Clear Range", _do_clear)

# =============================================================================
# Backend API Client
# =============================================================================
def _call_backend_api(endpoint: str, payload: Dict[str, Any]) -> Dict[str, Any]:
    base = (settings.backend_base_url or "http://127.0.0.1:8000").rstrip("/")
    url = f"{base}{endpoint}"

    headers = {
        "Content-Type": "application/json; charset=utf-8",
        "User-Agent": "TadawulFastBridge-SheetsService/3.2",
    }

    token = settings.app_token or settings.backup_app_token
    if token:
        headers["X-APP-TOKEN"] = token

    data_bytes = json.dumps(payload).encode("utf-8")
    req = urllib.request.Request(url, data=data_bytes, headers=headers, method="POST")

    retries = 2
    last_err: Optional[Exception] = None

    for attempt in range(retries + 1):
        try:
            with urllib.request.urlopen(req, timeout=_BACKEND_TIMEOUT, context=_SSL_CONTEXT) as resp:
                raw = resp.read().decode("utf-8", errors="replace")
                if 200 <= resp.status < 300:
                    return json.loads(raw) if raw else {}
                raise RuntimeError(f"Backend HTTP {resp.status}: {raw[:200]}")
        except Exception as e:
            last_err = e
            logger.warning("[GoogleSheets] Backend call %s attempt %d failed: %s", endpoint, attempt + 1, e)
            time.sleep(1.0 * (attempt + 1))

    tickers = payload.get("tickers") or payload.get("symbols") or []
    err_txt = str(last_err) if last_err else "Unknown backend error"
    return {"headers": ["Symbol", "Error"], "rows": [[t, err_txt] for t in tickers]}

# =============================================================================
# Row Ordering (match request order)
# =============================================================================
def _reorder_rows(headers: List[str], rows: List[List[Any]], requested_tickers: List[str]) -> List[List[Any]]:
    if not rows or not requested_tickers:
        return rows

    row_map: Dict[str, List[Any]] = {}
    for r in rows:
        if not r:
            continue
        sym = str(r[0]).strip().upper() if len(r) > 0 else ""
        if not sym:
            continue
        row_map[sym] = r
        if sym.endswith(".SR"):
            row_map[sym.replace(".SR", "")] = r
        elif sym.isdigit():
            row_map[f"{sym}.SR"] = r

    ordered: List[List[Any]] = []
    for t in requested_tickers:
        t_norm = (t or "").strip().upper()
        if not t_norm:
            continue
        row = row_map.get(t_norm) or row_map.get(t_norm.replace(".SR", "")) or row_map.get(f"{t_norm}.SR")
        if row:
            ordered.append(row)
        else:
            ordered.append([t_norm] + [None] * (max(1, len(headers)) - 1))
    return ordered

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
    backend_chunk_size: int = 350,
) -> Dict[str, Any]:
    if not tickers:
        return {"status": "skipped", "reason": "No tickers provided"}

    sid = spreadsheet_id or settings.default_spreadsheet_id
    if not sid:
        raise ValueError("No Spreadsheet ID provided (DEFAULT_SPREADSHEET_ID missing).")

    # Backend chunking to avoid huge HTTP payloads
    chunks = _chunk_list([t for t in tickers if (t or "").strip()], backend_chunk_size)

    headers: List[str] = []
    all_rows: List[List[Any]] = []

    for chunk in chunks:
        resp = _call_backend_api(endpoint, {"tickers": chunk, "sheet_name": sheet_name})
        if not headers:
            headers = resp.get("headers", []) or []
        rows = resp.get("rows", []) or []
        all_rows.extend(rows)

    if not headers:
        headers = ["Symbol", "Error"]

    ordered_rows = _reorder_rows(headers, all_rows, tickers)
    grid = [headers] + ordered_rows

    target_range = f"'{sheet_name}'!{start_cell}"

    if clear:
        clear_range(sid, f"'{sheet_name}'!A1:ZZ")

    updated_cells = write_range(sid, target_range, grid)

    return {"status": "success", "rows_written": len(ordered_rows), "cells_updated": updated_cells, "sheet": sheet_name, "endpoint": endpoint}

# Sync wrappers
def refresh_sheet_with_enriched_quotes(sid: str, sheet_name: str, tickers: List[str], **kwargs):
    return _refresh_logic("/v1/enriched/sheet-rows", sid, sheet_name, tickers, **kwargs)

def refresh_sheet_with_ai_analysis(sid: str, sheet_name: str, tickers: List[str], **kwargs):
    return _refresh_logic("/v1/analysis/sheet-rows", sid, sheet_name, tickers, **kwargs)

def refresh_sheet_with_advanced_analysis(sid: str, sheet_name: str, tickers: List[str], **kwargs):
    return _refresh_logic("/v1/advanced/sheet-rows", sid, sheet_name, tickers, **kwargs)

# Async wrappers
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
