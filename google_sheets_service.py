"""
google_sheets_service.py
------------------------------------------------------------
Google Sheets helper for Tadawul Fast Bridge – v2.0

GOALS
- Centralize all direct Google Sheets access in one place.
- Integrate tightly with:
    • env.py      (GOOGLE_SHEETS_CREDENTIALS, BACKEND_BASE_URL, APP_TOKEN)
    • backend routes:
          /v1/enriched/sheet-rows
          /v1/analysis/sheet-rows
          /v1/advanced/sheet-rows
- Provide generic helpers to refresh ANY sheet/page with:
    • Enriched quotes (fundamentals style)
    • AI analysis scores
    • Advanced analysis & risk scores

IMPORTANT – KSA vs GLOBAL
- EODHD API is NOT used directly here.
- For all tickers, including KSA (.SR), this module calls your own backend
  endpoints (which internally use core.data_engine).
- core.data_engine is responsible for using Tadawul/Argaam providers
  for KSA tickers instead of EODHD. This module never calls EODHD itself.

USAGE EXAMPLES
--------------
    from google_sheets_service import (
        refresh_sheet_with_enriched_quotes,
        refresh_sheet_with_ai_analysis,
        refresh_sheet_with_advanced_analysis,
    )

    # Refresh a KSA Tadawul page
    refresh_sheet_with_enriched_quotes(
        spreadsheet_id="YOUR_SHEET_ID",
        sheet_name="KSA_Tadawul",
        tickers=["1120.SR", "1180.SR", "1211.SR"],
        start_cell="A5",
    )

    # Refresh Global Markets analysis
    refresh_sheet_with_ai_analysis(
        spreadsheet_id="YOUR_SHEET_ID",
        sheet_name="Global_Markets",
        tickers=["AAPL", "MSFT", "NVDA"],
        start_cell="A5",
    )
"""

from __future__ import annotations

import asyncio
import json
import logging
import ssl
import urllib.error
import urllib.parse
import urllib.request
from typing import Any, Dict, List, Optional

from env import (
    GOOGLE_SHEETS_CREDENTIALS,
    BACKEND_BASE_URL,
    APP_TOKEN,
)

logger = logging.getLogger(__name__)

# ----------------------------------------------------------------------
# Optional Google API imports (lazy-checked in get_sheets_service)
# ----------------------------------------------------------------------

try:  # pragma: no cover - optional dependency
    from google.oauth2.service_account import Credentials  # type: ignore
    from googleapiclient.discovery import build  # type: ignore
    from googleapiclient.errors import HttpError  # type: ignore
except Exception:  # pragma: no cover
    Credentials = None  # type: ignore
    build = None  # type: ignore

    class HttpError(Exception):  # fallback type
        pass


# ----------------------------------------------------------------------
# Constants
# ----------------------------------------------------------------------

_SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]
_SHEETS_SERVICE = None  # lazy singleton

_BACKEND_TIMEOUT = 30.0  # seconds
_SSL_CONTEXT = ssl.create_default_context()


# ----------------------------------------------------------------------
# Helpers: KSA vs GLOBAL tickers
# ----------------------------------------------------------------------


def split_tickers_by_market(tickers: List[str]) -> Dict[str, List[str]]:
    """
    Split mixed tickers into:
        - ksa:     ones ending with '.SR'
        - global:  all others

    NOTE:
    - This module NEVER calls EODHD directly.
    - The split is mainly for logging/diagnostics and future routing.
    - All tickers are still sent to backend endpoints; core.data_engine
      decides how to fetch data for KSA vs GLOBAL.
    """
    ksa: List[str] = []
    global_: List[str] = []

    for t in tickers or []:
        clean = (t or "").strip()
        if not clean:
            continue
        if clean.upper().endswith(".SR"):
            ksa.append(clean.upper())
        else:
            global_.append(clean.upper())

    return {"ksa": ksa, "global": global_}


# ----------------------------------------------------------------------
# Google Sheets client
# ----------------------------------------------------------------------


def get_sheets_service():
    """
    Build or reuse a Google Sheets API client.

    Uses GOOGLE_SHEETS_CREDENTIALS (dict) from env.py.
    """
    global _SHEETS_SERVICE

    if _SHEETS_SERVICE is not None:
        return _SHEETS_SERVICE

    if Credentials is None or build is None:
        raise RuntimeError(
            "google-api-python-client and google-auth are required to use "
            "google_sheets_service. Please install:\n"
            "  pip install google-api-python-client google-auth"
        )

    creds_dict = GOOGLE_SHEETS_CREDENTIALS
    if not creds_dict:
        raise RuntimeError(
            "GOOGLE_SHEETS_CREDENTIALS is not configured in environment. "
            "Set it to the full service account JSON."
        )

    creds = Credentials.from_service_account_info(creds_dict, scopes=_SCOPES)
    service = build("sheets", "v4", credentials=creds, cache_discovery=False)
    _SHEETS_SERVICE = service
    logger.info("[GoogleSheets] Sheets service initialized.")
    return service


# ----------------------------------------------------------------------
# Low-level Sheets helpers
# ----------------------------------------------------------------------


def read_range(spreadsheet_id: str, range_a1: str) -> List[List[Any]]:
    """
    Read values from the given spreadsheet + A1 range.
    """
    service = get_sheets_service()
    try:
        result = (
            service.spreadsheets()
            .values()
            .get(spreadsheetId=spreadsheet_id, range=range_a1)
            .execute()
        )
        return result.get("values", [])
    except HttpError as exc:
        logger.error(
            "[GoogleSheets] Error reading range %s!%s: %s",
            spreadsheet_id,
            range_a1,
            exc,
        )
        raise


def write_range(
    spreadsheet_id: str,
    range_a1: str,
    values: List[List[Any]],
    value_input_option: str = "RAW",
) -> Dict[str, Any]:
    """
    Write values to the given spreadsheet + A1 range.

    - If you specify only top-left cell (e.g. 'KSA_Tadawul!A5'),
      Sheets will expand to fit the values size.
    """
    service = get_sheets_service()
    body = {"values": values or []}
    try:
        result = (
            service.spreadsheets()
            .values()
            .update(
                spreadsheetId=spreadsheet_id,
                range=range_a1,
                valueInputOption=value_input_option,
                body=body,
            )
            .execute()
        )
        logger.info(
            "[GoogleSheets] Updated %s!%s (%s cells).",
            spreadsheet_id,
            range_a1,
            result.get("updatedCells", 0),
        )
        return result
    except HttpError as exc:
        logger.error(
            "[GoogleSheets] Error writing range %s!%s: %s",
            spreadsheet_id,
            range_a1,
            exc,
        )
        raise


def clear_range(spreadsheet_id: str, range_a1: str) -> Dict[str, Any]:
    """
    Clear (empty) a given range.
    """
    service = get_sheets_service()
    try:
        result = (
            service.spreadsheets()
            .values()
            .clear(
                spreadsheetId=spreadsheet_id,
                range=range_a1,
                body={},
            )
            .execute()
        )
        logger.info(
            "[GoogleSheets] Cleared %s!%s.",
            spreadsheet_id,
            range_a1,
        )
        return result
    except HttpError as exc:
        logger.error(
            "[GoogleSheets] Error clearing range %s!%s: %s",
            spreadsheet_id,
            range_a1,
            exc,
        )
        raise


# ----------------------------------------------------------------------
# Backend client for /sheet-rows endpoints
# ----------------------------------------------------------------------


def _call_backend_sheet_rows(
    endpoint: str,
    tickers: List[str],
    timeout: float = _BACKEND_TIMEOUT,
) -> Dict[str, Any]:
    """
    Call one of the backend's sheet-rows endpoints:

        /v1/enriched/sheet-rows
        /v1/analysis/sheet-rows
        /v1/advanced/sheet-rows

    BODY:
        { "tickers": [ ... ] }

    RETURNS:
        {
          "headers": [...],
          "rows": [[...], ...]
        }

    NOTE:
    - This function does NOT call EODHD directly.
    - It only calls your backend, which internally uses core.data_engine.
    """
    base = (BACKEND_BASE_URL or "").rstrip("/")
    if not base:
        raise RuntimeError(
            "BACKEND_BASE_URL is not configured in environment; "
            "cannot call backend /sheet-rows endpoints."
        )

    url = base + endpoint

    payload = {"tickers": tickers or []}
    headers: Dict[str, str] = {
        "Content-Type": "application/json; charset=utf-8",
    }
    if APP_TOKEN:
        headers["X-APP-TOKEN"] = APP_TOKEN

    data = json.dumps(payload).encode("utf-8")
    req = urllib.request.Request(url, data=data, headers=headers, method="POST")

    try:
        with urllib.request.urlopen(req, timeout=timeout, context=_SSL_CONTEXT) as resp:
            status = getattr(resp, "status", resp.getcode())
            raw = resp.read()
            text = raw.decode("utf-8", errors="replace")
            if status < 200 or status >= 300:
                logger.error(
                    "[Backend] HTTP %s calling %s: %s",
                    status,
                    endpoint,
                    text[:300],
                )
                raise RuntimeError(
                    f"Backend /sheet-rows error HTTP {status}: {text[:200]}"
                )
            try:
                parsed = json.loads(text)
            except Exception as exc:
                logger.error(
                    "[Backend] Error parsing JSON from %s: %s",
                    endpoint,
                    exc,
                )
                raise

            if not isinstance(parsed, dict):
                raise RuntimeError(f"Unexpected backend response type: {type(parsed)}")

            if "headers" not in parsed or "rows" not in parsed:
                raise RuntimeError(
                    f"Backend response from {endpoint} missing 'headers' or 'rows'."
                )

            return parsed

    except urllib.error.HTTPError as exc:
        body = exc.read().decode("utf-8", errors="replace") if exc.fp else ""
        logger.error(
            "[Backend] HTTPError %s calling %s: %s",
            exc.code,
            endpoint,
            body[:300],
        )
        raise RuntimeError(f"Backend HTTPError {exc.code}: {exc.reason}") from exc

    except urllib.error.URLError as exc:
        logger.error("[Backend] URLError calling %s: %s", endpoint, exc)
        raise RuntimeError(f"Backend URLError: {exc}") from exc

    except Exception as exc:
        logger.exception("[Backend] Unexpected error calling %s", endpoint)
        raise


async def _call_backend_sheet_rows_async(
    endpoint: str,
    tickers: List[str],
    timeout: float = _BACKEND_TIMEOUT,
) -> Dict[str, Any]:
    """
    Async wrapper around _call_backend_sheet_rows.
    """
    loop = asyncio.get_running_loop()
    return await loop.run_in_executor(
        None, lambda: _call_backend_sheet_rows(endpoint, tickers, timeout)
    )


# ----------------------------------------------------------------------
# HIGH-LEVEL REFRESH HELPERS (for ALL pages)
# ----------------------------------------------------------------------


def _build_sheet_values_from_backend_payload(
    backend_payload: Dict[str, Any],
) -> List[List[Any]]:
    """
    Combine headers + rows into a `values` list suitable for setValues()
    or Sheets API values.update().

    backend_payload:
        { "headers": [...], "rows": [[...], ...] }
    """
    headers = backend_payload.get("headers") or []
    rows = backend_payload.get("rows") or []
    values: List[List[Any]] = []
    if headers:
        values.append(list(headers))
    values.extend(rows)
    return values


def refresh_sheet_with_enriched_quotes(
    spreadsheet_id: str,
    sheet_name: str,
    tickers: List[str],
    start_cell: str = "A1",
    clear_existing: bool = False,
) -> Dict[str, Any]:
    """
    Refresh a sheet with ENRICHED QUOTE data from backend:

        POST {BACKEND_BASE_URL}/v1/enriched/sheet-rows

    - Suitable for KSA_Tadawul, Global_Markets, Mutual_Funds, Commodities_FX, etc.
    - KSA tickers (.SR) are handled by core.data_engine (Tadawul/Argaam, not EODHD).

    Parameters
    ----------
    spreadsheet_id : str
        Google Sheet ID.
    sheet_name : str
        Sheet/tab name (e.g. 'KSA_Tadawul').
    tickers : list[str]
        Mixed list of KSA (.SR) and Global tickers.
    start_cell : str
        Top-left cell to write values into (default 'A1', often 'A5' for your layouts).
    clear_existing : bool
        If True, clears the entire sheet range before writing (simple 'sheet_name'!A:ZZ).
    """
    split = split_tickers_by_market(tickers)
    logger.info(
        "[Sheets][Enriched] Refreshing %s!%s with %d tickers (KSA=%d, Global=%d)...",
        sheet_name,
        start_cell,
        len(split["ksa"]) + len(split["global"]),
        len(split["ksa"]),
        len(split["global"]),
    )

    backend_data = _call_backend_sheet_rows(
        endpoint="/v1/enriched/sheet-rows",
        tickers=tickers,
    )
    values = _build_sheet_values_from_backend_payload(backend_data)

    full_range = f"{sheet_name}!{start_cell}"
    if clear_existing:
        clear_range(spreadsheet_id, f"{sheet_name}!A:ZZ")
    return write_range(spreadsheet_id, full_range, values)


def refresh_sheet_with_ai_analysis(
    spreadsheet_id: str,
    sheet_name: str,
    tickers: List[str],
    start_cell: str = "A1",
    clear_existing: bool = False,
) -> Dict[str, Any]:
    """
    Refresh a sheet with AI ANALYSIS data from backend:

        POST {BACKEND_BASE_URL}/v1/analysis/sheet-rows

    - Good for "Insights & Analysis" type pages.
    """
    split = split_tickers_by_market(tickers)
    logger.info(
        "[Sheets][AI] Refreshing %s!%s with %d tickers (KSA=%d, Global=%d)...",
        sheet_name,
        start_cell,
        len(split["ksa"]) + len(split["global"]),
        len(split["ksa"]),
        len(split["global"]),
    )

    backend_data = _call_backend_sheet_rows(
        endpoint="/v1/analysis/sheet-rows",
        tickers=tickers,
    )
    values = _build_sheet_values_from_backend_payload(backend_data)

    full_range = f"{sheet_name}!{start_cell}"
    if clear_existing:
        clear_range(spreadsheet_id, f"{sheet_name}!A:ZZ")
    return write_range(spreadsheet_id, full_range, values)


def refresh_sheet_with_advanced_analysis(
    spreadsheet_id: str,
    sheet_name: str,
    tickers: List[str],
    start_cell: str = "A1",
    clear_existing: bool = False,
) -> Dict[str, Any]:
    """
    Refresh a sheet with ADVANCED ANALYSIS & RISK data from backend:

        POST {BACKEND_BASE_URL}/v1/advanced/sheet-rows

    - Good for Investment Advisor / Advanced Insights pages.
    """
    split = split_tickers_by_market(tickers)
    logger.info(
        "[Sheets][Advanced] Refreshing %s!%s with %d tickers (KSA=%d, Global=%d)...",
        sheet_name,
        start_cell,
        len(split["ksa"]) + len(split["global"]),
        len(split["ksa"]),
        len(split["global"]),
    )

    backend_data = _call_backend_sheet_rows(
        endpoint="/v1/advanced/sheet-rows",
        tickers=tickers,
    )
    values = _build_sheet_values_from_backend_payload(backend_data)

    full_range = f"{sheet_name}!{start_cell}"
    if clear_existing:
        clear_range(spreadsheet_id, f"{sheet_name}!A:ZZ")
    return write_range(spreadsheet_id, full_range, values)


# ----------------------------------------------------------------------
# ASYNC VERSIONS (optional for FastAPI background tasks)
# ----------------------------------------------------------------------


async def refresh_sheet_with_enriched_quotes_async(
    spreadsheet_id: str,
    sheet_name: str,
    tickers: List[str],
    start_cell: str = "A1",
    clear_existing: bool = False,
) -> Dict[str, Any]:
    loop = asyncio.get_running_loop()
    backend_data = await _call_backend_sheet_rows_async(
        endpoint="/v1/enriched/sheet-rows",
        tickers=tickers,
    )
    values = _build_sheet_values_from_backend_payload(backend_data)
    full_range = f"{sheet_name}!{start_cell}"

    if clear_existing:
        await loop.run_in_executor(
            None,
            lambda: clear_range(spreadsheet_id, f"{sheet_name}!A:ZZ"),
        )
    return await loop.run_in_executor(
        None,
        lambda: write_range(spreadsheet_id, full_range, values),
    )


async def refresh_sheet_with_ai_analysis_async(
    spreadsheet_id: str,
    sheet_name: str,
    tickers: List[str],
    start_cell: str = "A1",
    clear_existing: bool = False,
) -> Dict[str, Any]:
    loop = asyncio.get_running_loop()
    backend_data = await _call_backend_sheet_rows_async(
        endpoint="/v1/analysis/sheet-rows",
        tickers=tickers,
    )
    values = _build_sheet_values_from_backend_payload(backend_data)
    full_range = f"{sheet_name}!{start_cell}"

    if clear_existing:
        await loop.run_in_executor(
            None,
            lambda: clear_range(spreadsheet_id, f"{sheet_name}!A:ZZ"),
        )
    return await loop.run_in_executor(
        None,
        lambda: write_range(spreadsheet_id, full_range, values),
    )


async def refresh_sheet_with_advanced_analysis_async(
    spreadsheet_id: str,
    sheet_name: str,
    tickers: List[str],
    start_cell: str = "A1",
    clear_existing: bool = False,
) -> Dict[str, Any]:
    loop = asyncio.get_running_loop()
    backend_data = await _call_backend_sheet_rows_async(
        endpoint="/v1/advanced/sheet-rows",
        tickers=tickers,
    )
    values = _build_sheet_values_from_backend_payload(backend_data)
    full_range = f"{sheet_name}!{start_cell}"

    if clear_existing:
        await loop.run_in_executor(
            None,
            lambda: clear_range(spreadsheet_id, f"{sheet_name}!A:ZZ"),
        )
    return await loop.run_in_executor(
        None,
        lambda: write_range(spreadsheet_id, full_range, values),
    )
