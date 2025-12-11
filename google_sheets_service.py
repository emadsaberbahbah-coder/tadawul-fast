"""
google_sheets_service.py
------------------------------------------------------------
Google Sheets helper for Tadawul Fast Bridge – v2.4

GOALS
- Centralize all direct Google Sheets access in one place.
- Integrate tightly with:
    • env.py / environment variables:
          GOOGLE_SHEETS_CREDENTIALS
          GOOGLE_SHEETS_CREDENTIALS_RAW
          BACKEND_BASE_URL
          APP_TOKEN
    • backend routes (KSA-safe, NO direct EODHD calls here):
          /v1/enriched/sheet-rows
          /v1/analysis/sheet-rows
          /v1/advanced/sheet-rows

- Provide generic helpers to refresh ANY sheet/page with:
    • Enriched quotes (fundamentals-style, unified multi-page template)
    • AI analysis scores (Value / Quality / Momentum / Opportunity / Reco)
    • Advanced analysis & risk scores

IMPORTANT – KSA vs GLOBAL
- EODHD API is NOT used directly here.
- For all tickers (KSA .SR and Global), this module calls ONLY your backend
  endpoints, which internally use:
      core.data_engine_v2 (yfinance-based, KSA-safe routing)
      Tadawul / Argaam / other providers as configured.
- The unified engine is responsible for using Tadawul/Argaam providers
  for KSA tickers instead of EODHD. This module never calls EODHD itself.

SHEET-NAME AWARE
- The backend /v1/enriched/sheet-rows (and /v1/analysis, /v1/advanced)
  now accept an optional "sheet_name" in the JSON body.
- This file ALWAYS sends the current sheet_name to the backend so it can:
      • Select the correct column template (KSA, Global, Mutual Funds,
        Commodities_FX, My_Portfolio, Insights_Analysis, etc.).
      • Align headers with your MASTER_HEADERS / schemas.py contract.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import ssl
import urllib.error
import urllib.request
from typing import Any, Dict, List

logger = logging.getLogger("google_sheets_service")
if not logger.handlers:
    # Let main.py control root config; here we only set a sane default level.
    logger.setLevel(logging.INFO)

# ----------------------------------------------------------------------
# env.py INTEGRATION + SAFE FALLBACKS
# ----------------------------------------------------------------------
# We integrate with env.py if present, otherwise use pure environment vars.
#
# Supported inputs:
#   - BACKEND_BASE_URL, APP_TOKEN
#   - GOOGLE_SHEETS_CREDENTIALS         (dict in env.py, or JSON string in env)
#   - GOOGLE_SHEETS_CREDENTIALS_RAW     (JSON string, env or env.py)
# ----------------------------------------------------------------------

try:  # pragma: no cover - env.py is optional
    import env as _env_mod  # type: ignore
    logger.info("[GoogleSheets] env.py detected for configuration.")
except Exception:  # pragma: no cover - defensive
    _env_mod = None  # type: ignore
    logger.warning(
        "[GoogleSheets] env.py not available. Using OS environment variables only."
    )


def _get_env_attr(name: str, default: str = "") -> str:
    """
    Helper to read config from env.py if available, otherwise from OS env vars.
    Returns a string representation.
    """
    if _env_mod is not None and hasattr(_env_mod, name):
        value = getattr(_env_mod, name)
        if isinstance(value, str):
            return value
        try:
            return str(value)
        except Exception:
            return default
    return os.getenv(name, default)


BACKEND_BASE_URL: str = _get_env_attr("BACKEND_BASE_URL", "").strip()
APP_TOKEN: str = _get_env_attr("APP_TOKEN", "").strip()

# Credentials may come as:
#   - dict:   env.GOOGLE_SHEETS_CREDENTIALS
#   - JSON:   env.GOOGLE_SHEETS_CREDENTIALS_RAW
#   - JSON:   env var GOOGLE_SHEETS_CREDENTIALS_RAW
#   - JSON:   env var GOOGLE_SHEETS_CREDENTIALS
GOOGLE_SHEETS_CREDENTIALS: Any = {}
if _env_mod is not None and hasattr(_env_mod, "GOOGLE_SHEETS_CREDENTIALS"):
    GOOGLE_SHEETS_CREDENTIALS = getattr(_env_mod, "GOOGLE_SHEETS_CREDENTIALS")

_raw_from_env_mod = ""
if _env_mod is not None and hasattr(_env_mod, "GOOGLE_SHEETS_CREDENTIALS_RAW"):
    try:
        _raw_from_env_mod = str(
            getattr(_env_mod, "GOOGLE_SHEETS_CREDENTIALS_RAW") or ""
        ).strip()
    except Exception:
        _raw_from_env_mod = ""

GOOGLE_SHEETS_CREDENTIALS_RAW: str = (
    _raw_from_env_mod
    or os.getenv("GOOGLE_SHEETS_CREDENTIALS_RAW", "").strip()
    or os.getenv("GOOGLE_SHEETS_CREDENTIALS", "").strip()
)

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
        """Fallback HttpError when googleapiclient is not installed."""

        pass


# ----------------------------------------------------------------------
# Constants / Globals
# ----------------------------------------------------------------------

_SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]
_SHEETS_SERVICE = None  # lazy singleton

_BACKEND_TIMEOUT = 30.0  # seconds
_SSL_CONTEXT = ssl.create_default_context()


# ----------------------------------------------------------------------
# Helpers: KSA vs GLOBAL tickers (for logging / diagnostics only)
# ----------------------------------------------------------------------


def split_tickers_by_market(tickers: List[str]) -> Dict[str, List[str]]:
    """
    Split mixed tickers into:
        - ksa:     ones ending with '.SR'
        - global:  all others

    NOTE:
    - This module NEVER calls any market provider directly (no EODHD, no Yahoo).
    - The split is only for logging/diagnostics.
    - All tickers are still sent to backend endpoints; the unified engine
      decides how to fetch data for KSA vs GLOBAL.
    """
    ksa: List[str] = []
    global_: List[str] = []

    for t in tickers or []:
        clean = (t or "").strip()
        if not clean:
            continue
        up = clean.upper()
        if up.endswith(".SR"):
            ksa.append(up)
        else:
            global_.append(up)

    return {"ksa": ksa, "global": global_}


# ----------------------------------------------------------------------
# Google Sheets credentials handling
# ----------------------------------------------------------------------


def _get_creds_dict() -> Dict[str, Any]:
    """
    Resolve the Google service account credentials dict from either:
        - GOOGLE_SHEETS_CREDENTIALS (dict from env.py), or
        - GOOGLE_SHEETS_CREDENTIALS_RAW JSON string (env or env.py), or
        - GOOGLE_SHEETS_CREDENTIALS JSON string (env).

    Raises RuntimeError if we cannot produce a valid dict.

    IMPORTANT:
    - It is valid to paste the FULL service account JSON into the env var
      GOOGLE_SHEETS_CREDENTIALS (or GOOGLE_SHEETS_CREDENTIALS_RAW) exactly
      as you downloaded it from Google Cloud. This function will parse it.
    """
    # 1) If we already have a dict, use it directly
    if isinstance(GOOGLE_SHEETS_CREDENTIALS, dict) and GOOGLE_SHEETS_CREDENTIALS:
        return GOOGLE_SHEETS_CREDENTIALS

    # 2) Try RAW JSON string from env.py or env
    raw = GOOGLE_SHEETS_CREDENTIALS_RAW
    if not raw:
        # Fallback: try GOOGLE_SHEETS_CREDENTIALS again, just in case
        env_raw = os.getenv("GOOGLE_SHEETS_CREDENTIALS", "").strip()
        if env_raw:
            raw = env_raw

    if raw:
        try:
            parsed = json.loads(raw)
            if isinstance(parsed, dict):
                return parsed
            logger.error(
                "[GoogleSheets] GOOGLE_SHEETS_CREDENTIALS JSON did not parse to dict (got %s).",
                type(parsed),
            )
        except Exception as exc:
            logger.error(
                "[GoogleSheets] Failed to parse GOOGLE_SHEETS_CREDENTIALS JSON: %s",
                exc,
            )

    raise RuntimeError(
        "Google Sheets credentials not available. "
        "Ensure GOOGLE_SHEETS_CREDENTIALS (JSON) or GOOGLE_SHEETS_CREDENTIALS_RAW "
        "is set in environment or provided via env.py."
    )


def get_sheets_service():
    """
    Build or reuse a Google Sheets API client.

    Uses GOOGLE_SHEETS_CREDENTIALS (dict) or GOOGLE_SHEETS_CREDENTIALS_RAW (JSON)
    from env.py / environment variables.
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

    creds_dict = _get_creds_dict()
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
        values = result.get("values", [])
        logger.info(
            "[GoogleSheets] Read %d rows from %s!%s.",
            len(values),
            spreadsheet_id,
            range_a1,
        )
        return values
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
        logger.info("[GoogleSheets] Cleared %s!%s.", spreadsheet_id, range_a1)
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


def _normalize_endpoint(endpoint: str) -> str:
    """
    Ensure that the endpoint has a leading slash.
    """
    if not endpoint.startswith("/"):
        return "/" + endpoint
    return endpoint


def _ensure_backend_config() -> str:
    """
    Ensure BACKEND_BASE_URL is configured and normalized.
    Logs warning if APP_TOKEN is missing (non-fatal).
    Returns the normalized base URL without trailing slash.
    """
    base = (BACKEND_BASE_URL or "").strip()
    if not base:
        raise RuntimeError(
            "BACKEND_BASE_URL is not configured; "
            "cannot call backend /sheet-rows endpoints."
        )

    # If somebody accidentally set without scheme, try to help
    if not base.startswith("http://") and not base.startswith("https://"):
        logger.warning(
            "[Backend] BACKEND_BASE_URL has no scheme, "
            "assuming https://%s",
            base,
        )
        base = "https://" + base

    if not APP_TOKEN:
        logger.warning(
            "[Backend] APP_TOKEN is empty; backend may reject unauthorized "
            "sheet-rows calls."
        )

    return base.rstrip("/")


def _call_backend_sheet_rows(
    endpoint: str,
    tickers: List[str],
    sheet_name: str | None = None,
    timeout: float = _BACKEND_TIMEOUT,
) -> Dict[str, Any]:
    """
    Call one of the backend's sheet-rows endpoints:

        /v1/enriched/sheet-rows
        /v1/analysis/sheet-rows
        /v1/advanced/sheet-rows

    BODY:
        {
          "tickers": [...],
          "sheet_name": "KSA_Tadawul"   # optional, but we ALWAYS send it
        }

    RETURNS:
        {
          "headers": [...],
          "rows": [[...], ...]
        }

    NOTE:
    - This function does NOT call any external market provider (no EODHD).
    - It only calls your backend, which internally uses the unified data engine
      with KSA-safe routing (Tadawul/Argaam for .SR).
    """
    base = _ensure_backend_config()
    ep = _normalize_endpoint(endpoint)
    url = base + ep

    # Deduplicate while preserving order
    seen = set()
    clean_tickers: List[str] = []
    for t in tickers or []:
        tt = (t or "").strip()
        if not tt:
            continue
        if tt not in seen:
            seen.add(tt)
            clean_tickers.append(tt)

    payload: Dict[str, Any] = {"tickers": clean_tickers}
    # Always send sheet_name if we know it, so backend can pick correct template
    if sheet_name:
        payload["sheet_name"] = sheet_name

    headers: Dict[str, str] = {
        "Content-Type": "application/json; charset=utf-8",
    }
    if APP_TOKEN:
        headers["X-APP-TOKEN"] = APP_TOKEN

    data = json.dumps(payload).encode("utf-8")
    req = urllib.request.Request(url, data=data, headers=headers, method="POST")

    logger.info(
        "[Backend] Calling %s with %d tickers (sheet_name=%s).",
        ep,
        len(clean_tickers),
        sheet_name,
    )

    try:
        with urllib.request.urlopen(req, timeout=timeout, context=_SSL_CONTEXT) as resp:
            status_code = getattr(resp, "status", resp.getcode())
            raw = resp.read()
            text = raw.decode("utf-8", errors="replace")
            if status_code < 200 or status_code >= 300:
                logger.error(
                    "[Backend] HTTP %s calling %s: %s",
                    status_code,
                    ep,
                    text[:300],
                )
                raise RuntimeError(
                    f"Backend /sheet-rows error HTTP {status_code}: {text[:200]}"
                )

            try:
                parsed = json.loads(text)
            except Exception as exc:
                logger.error(
                    "[Backend] Error parsing JSON from %s: %s",
                    ep,
                    exc,
                )
                raise RuntimeError(
                    f"Error parsing backend JSON from {ep}: {exc}"
                ) from exc

            if not isinstance(parsed, dict):
                raise RuntimeError(
                    f"Unexpected backend response type from {ep}: {type(parsed)}"
                )

            if "headers" not in parsed or "rows" not in parsed:
                raise RuntimeError(
                    f"Backend response from {ep} missing 'headers' or 'rows'."
                )

            return parsed

    except urllib.error.HTTPError as exc:
        body = exc.read().decode("utf-8", errors="replace") if exc.fp else ""
        logger.error(
            "[Backend] HTTPError %s calling %s: %s",
            exc.code,
            ep,
            body[:300],
        )
        raise RuntimeError(f"Backend HTTPError {exc.code}: {exc.reason}") from exc

    except urllib.error.URLError as exc:
        logger.error("[Backend] URLError calling %s: %s", ep, exc)
        raise RuntimeError(f"Backend URLError: {exc}") from exc

    except Exception as exc:
        logger.exception("[Backend] Unexpected error calling %s", ep)
        raise RuntimeError(f"Backend unexpected error calling {ep}: {exc}") from exc


async def _call_backend_sheet_rows_async(
    endpoint: str,
    tickers: List[str],
    sheet_name: str | None = None,
    timeout: float = _BACKEND_TIMEOUT,
) -> Dict[str, Any]:
    """
    Async wrapper around _call_backend_sheet_rows.
    Offloads blocking IO to a thread pool.
    """
    loop = asyncio.get_running_loop()
    return await loop.run_in_executor(
        None,
        lambda: _call_backend_sheet_rows(
            endpoint=endpoint,
            tickers=tickers,
            sheet_name=sheet_name,
            timeout=timeout,
        ),
    )


# ----------------------------------------------------------------------
# HIGH-LEVEL REFRESH HELPERS (for ALL 9 pages)
# ----------------------------------------------------------------------


def _build_sheet_values_from_backend_payload(
    backend_payload: Dict[str, Any],
) -> List[List[Any]]:
    """
    Combine headers + rows into a `values` list suitable for setValues()
    or Sheets API values.update().

    backend_payload:
        { "headers": [...], "rows": [[...], ...] }

    NOTE:
    - We *always* prepend the headers row, so Apps Script / Sheets can safely:
          setValues(values)
      starting at the header row (e.g. 'A5').
    """
    headers = backend_payload.get("headers") or []
    rows = backend_payload.get("rows") or []
    values: List[List[Any]] = []
    if headers:
        values.append(list(headers))
    values.extend(rows)
    return values


def _refresh_sheet_generic(
    endpoint: str,
    spreadsheet_id: str,
    sheet_name: str,
    tickers: List[str],
    start_cell: str,
    clear_existing: bool,
    log_label: str,
) -> Dict[str, Any]:
    """
    Generic refresh used by enriched, AI, and advanced layers.

    sheet_name:
        - Used for:
            • Google Sheets tab (e.g., 'KSA_Tadawul', 'Global_Markets')
            • Backend 'sheet_name' parameter to pick correct column template.
    """
    split = split_tickers_by_market(tickers)
    logger.info(
        "%s Refreshing %s!%s with %d tickers (KSA=%d, Global=%d) via %s...",
        log_label,
        sheet_name,
        start_cell,
        len(split["ksa"]) + len(split["global"]),
        len(split["ksa"]),
        len(split["global"]),
        endpoint,
    )

    backend_data = _call_backend_sheet_rows(
        endpoint=endpoint,
        tickers=tickers,
        sheet_name=sheet_name,
    )
    values = _build_sheet_values_from_backend_payload(backend_data)

    full_range = f"{sheet_name}!{start_cell}"
    if clear_existing:
        clear_range(spreadsheet_id, f"{sheet_name}!A:ZZ")
    return write_range(spreadsheet_id, full_range, values)


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

    Suitable for:
        - KSA_Tadawul
        - Global_Markets
        - Mutual_Funds
        - Commodities_FX
        - My_Portfolio
        - Any fundamentals-style page

    KSA tickers (.SR) are handled by the unified engine (Tadawul/Argaam, NOT EODHD).
    The sheet_name is sent to the backend so it can use the right header template.
    """
    return _refresh_sheet_generic(
        endpoint="/v1/enriched/sheet-rows",
        spreadsheet_id=spreadsheet_id,
        sheet_name=sheet_name,
        tickers=tickers,
        start_cell=start_cell,
        clear_existing=clear_existing,
        log_label="[Sheets][Enriched]",
    )


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

    Ideal for:
        - Insights_Analysis page
        - Any scoring/AI layer on top of:
              KSA_Tadawul / Global_Markets / Mutual_Funds / Commodities_FX.

    The sheet_name is sent to the backend to allow different layouts per page.
    """
    return _refresh_sheet_generic(
        endpoint="/v1/analysis/sheet-rows",
        spreadsheet_id=spreadsheet_id,
        sheet_name=sheet_name,
        tickers=tickers,
        start_cell=start_cell,
        clear_existing=clear_existing,
        log_label="[Sheets][AI]",
    )


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

    Ideal for:
        - Investment_Advisor
        - Advanced_Insights / Risk_Buckets-style pages
        - Any page that needs deeper risk / factor breakdown.

    The sheet_name is sent to the backend to align output with the target sheet.
    """
    return _refresh_sheet_generic(
        endpoint="/v1/advanced/sheet-rows",
        spreadsheet_id=spreadsheet_id,
        sheet_name=sheet_name,
        tickers=tickers,
        start_cell=start_cell,
        clear_existing=clear_existing,
        log_label="[Sheets][Advanced]",
    )


# ----------------------------------------------------------------------
# ASYNC VERSIONS (for FastAPI background tasks / workers)
# ----------------------------------------------------------------------


async def _refresh_sheet_generic_async(
    endpoint: str,
    spreadsheet_id: str,
    sheet_name: str,
    tickers: List[str],
    start_cell: str,
    clear_existing: bool,
    log_label: str,
) -> Dict[str, Any]:
    """
    Async version of the generic refresh helper.
    """
    split = split_tickers_by_market(tickers)
    logger.info(
        "%s (async) Refreshing %s!%s with %d tickers (KSA=%d, Global=%d) via %s...",
        log_label,
        sheet_name,
        start_cell,
        len(split["ksa"]) + len(split["global"]),
        len(split["ksa"]),
        len(split["global"]),
        endpoint,
    )

    backend_data = await _call_backend_sheet_rows_async(
        endpoint=endpoint,
        tickers=tickers,
        sheet_name=sheet_name,
    )
    values = _build_sheet_values_from_backend_payload(backend_data)
    full_range = f"{sheet_name}!{start_cell}"

    loop = asyncio.get_running_loop()
    if clear_existing:
        await loop.run_in_executor(
            None,
            lambda: clear_range(spreadsheet_id, f"{sheet_name}!A:ZZ"),
        )
    return await loop.run_in_executor(
        None,
        lambda: write_range(spreadsheet_id, full_range, values),
    )


async def refresh_sheet_with_enriched_quotes_async(
    spreadsheet_id: str,
    sheet_name: str,
    tickers: List[str],
    start_cell: str = "A1",
    clear_existing: bool = False,
) -> Dict[str, Any]:
    """
    Async version of refresh_sheet_with_enriched_quotes.
    """
    return await _refresh_sheet_generic_async(
        endpoint="/v1/enriched/sheet-rows",
        spreadsheet_id=spreadsheet_id,
        sheet_name=sheet_name,
        tickers=tickers,
        start_cell=start_cell,
        clear_existing=clear_existing,
        log_label="[Sheets][Enriched]",
    )


async def refresh_sheet_with_ai_analysis_async(
    spreadsheet_id: str,
    sheet_name: str,
    tickers: List[str],
    start_cell: str = "A1",
    clear_existing: bool = False,
) -> Dict[str, Any]:
    """
    Async version of refresh_sheet_with_ai_analysis.
    """
    return await _refresh_sheet_generic_async(
        endpoint="/v1/analysis/sheet-rows",
        spreadsheet_id=spreadsheet_id,
        sheet_name=sheet_name,
        tickers=tickers,
        start_cell=start_cell,
        clear_existing=clear_existing,
        log_label="[Sheets][AI]",
    )


async def refresh_sheet_with_advanced_analysis_async(
    spreadsheet_id: str,
    sheet_name: str,
    tickers: List[str],
    start_cell: str = "A1",
    clear_existing: bool = False,
) -> Dict[str, Any]:
    """
    Async version of refresh_sheet_with_advanced_analysis.
    """
    return await _refresh_sheet_generic_async(
        endpoint="/v1/advanced/sheet-rows",
        spreadsheet_id=spreadsheet_id,
        sheet_name=sheet_name,
        tickers=tickers,
        start_cell=start_cell,
        clear_existing=clear_existing,
        log_label="[Sheets][Advanced]",
    )


__all__ = [
    "split_tickers_by_market",
    "get_sheets_service",
    "read_range",
    "write_range",
    "clear_range",
    "refresh_sheet_with_enriched_quotes",
    "refresh_sheet_with_ai_analysis",
    "refresh_sheet_with_advanced_analysis",
    "refresh_sheet_with_enriched_quotes_async",
    "refresh_sheet_with_ai_analysis_async",
    "refresh_sheet_with_advanced_analysis_async",
]
