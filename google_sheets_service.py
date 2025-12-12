"""
google_sheets_service.py
------------------------------------------------------------
Google Sheets helper for Tadawul Fast Bridge – v2.7.1 (Backend-first + Boot-safe)

GOALS
- Centralize ALL direct Google Sheets access here.
- Call ONLY the backend /sheet-rows endpoints (no direct providers).
- Always send `sheet_name` to backend so it can choose the right template/schema.
- Strongly defensive: safe env fallbacks, retries, chunking, ordering guarantees.

Backend endpoints (POST)
- /v1/enriched/sheet-rows
- /v1/analysis/sheet-rows
- /v1/advanced/sheet-rows
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

# ----------------------------------------------------------------------
# Logging
# ----------------------------------------------------------------------

logger = logging.getLogger("google_sheets_service")
if not logger.handlers:
    level_name = (os.getenv("LOG_LEVEL", "INFO") or "INFO").upper()
    level = getattr(logging, level_name, logging.INFO)
    logging.basicConfig(
        level=level,
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    )

# ----------------------------------------------------------------------
# env.py integration + safe fallbacks
# ----------------------------------------------------------------------

try:  # pragma: no cover
    import env as _env_mod  # type: ignore

    _SETTINGS = getattr(_env_mod, "settings", None)
    logger.info("[GoogleSheets] env.py detected for configuration.")
except Exception:  # pragma: no cover
    _env_mod = None  # type: ignore
    _SETTINGS = None
    logger.warning("[GoogleSheets] env.py not available. Using OS env vars only.")


def _env_get_str(name: str, default: str = "") -> str:
    """
    Prefer env.settings.<attr> when available, then env.<NAME>, then OS env vars.
    """
    # Prefer env.settings.<snake_case> if present
    if _SETTINGS is not None:
        mapping = {
            "BACKEND_BASE_URL": "backend_base_url",
            "APP_TOKEN": "app_token",
            "BACKUP_APP_TOKEN": "backup_app_token",
            "GOOGLE_SHEETS_CREDENTIALS_RAW": "google_sheets_credentials_raw",
            "DEFAULT_SPREADSHEET_ID": "default_spreadsheet_id",
        }
        attr = mapping.get(name)
        if attr and hasattr(_SETTINGS, attr):
            try:
                v = getattr(_SETTINGS, attr)
                if v is None:
                    return default
                return str(v).strip()
            except Exception:
                pass

    # Prefer env.<NAME> convenience constants
    if _env_mod is not None and hasattr(_env_mod, name):
        try:
            v = getattr(_env_mod, name)
            if v is None:
                return default
            return str(v).strip()
        except Exception:
            return default

    return (os.getenv(name, default) or default).strip()


def _env_get_dict(name: str) -> Optional[Dict[str, Any]]:
    if _env_mod is not None and hasattr(_env_mod, name):
        try:
            v = getattr(_env_mod, name)
            if isinstance(v, dict) and v:
                return v
        except Exception:
            pass
    return None


BACKEND_BASE_URL: str = _env_get_str("BACKEND_BASE_URL", "")
APP_TOKEN: str = _env_get_str("APP_TOKEN", "")
BACKUP_APP_TOKEN: str = _env_get_str("BACKUP_APP_TOKEN", "")
DEFAULT_SPREADSHEET_ID: str = _env_get_str("DEFAULT_SPREADSHEET_ID", "").strip()

# Credentials may come as:
# - env.GOOGLE_SHEETS_CREDENTIALS (dict)
# - env.GOOGLE_SHEETS_CREDENTIALS_RAW (string)
# - OS env GOOGLE_SHEETS_CREDENTIALS / GOOGLE_SHEETS_CREDENTIALS_RAW (string)
GOOGLE_SHEETS_CREDENTIALS: Any = _env_get_dict("GOOGLE_SHEETS_CREDENTIALS") or {}
GOOGLE_SHEETS_CREDENTIALS_RAW: str = (
    _env_get_str("GOOGLE_SHEETS_CREDENTIALS_RAW", "")
    or os.getenv("GOOGLE_SHEETS_CREDENTIALS_RAW", "").strip()
    or os.getenv("GOOGLE_SHEETS_CREDENTIALS", "").strip()
)

# ----------------------------------------------------------------------
# Optional Google API imports (lazy-checked in get_sheets_service)
# ----------------------------------------------------------------------

try:  # pragma: no cover
    from google.oauth2.service_account import Credentials  # type: ignore
    from googleapiclient.discovery import build  # type: ignore
    from googleapiclient.errors import HttpError  # type: ignore
except Exception:  # pragma: no cover
    Credentials = None  # type: ignore
    build = None  # type: ignore

    class HttpError(Exception):  # type: ignore
        pass


# ----------------------------------------------------------------------
# Module constants / globals
# ----------------------------------------------------------------------

_SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]
_SHEETS_SERVICE = None  # lazy singleton

_BACKEND_TIMEOUT = float(os.getenv("BACKEND_TIMEOUT_SECONDS", "30") or 30)
_BACKEND_MAX_TICKERS_PER_CALL = int(os.getenv("BACKEND_MAX_TICKERS_PER_CALL", "120") or 120)
_BACKEND_RETRIES = int(os.getenv("BACKEND_RETRIES", "2") or 2)

# Google Sheets API retries
_SHEETS_RETRIES = int(os.getenv("SHEETS_API_RETRIES", "3") or 3)
_SHEETS_RETRY_BASE_SLEEP = float(os.getenv("SHEETS_API_RETRY_BASE_SLEEP", "0.75") or 0.75)

_SSL_CONTEXT = ssl.create_default_context()

# ----------------------------------------------------------------------
# Helpers: market split (diagnostics only)
# ----------------------------------------------------------------------


def split_tickers_by_market(tickers: List[str]) -> Dict[str, List[str]]:
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
# Credentials handling
# ----------------------------------------------------------------------


def _parse_credentials_json(raw: str) -> Optional[Dict[str, Any]]:
    """
    Parse service-account JSON from env strings.
    Handles:
      - raw JSON
      - JSON wrapped in quotes
      - escaped newlines inside private_key
    """
    if not raw:
        return None

    s = str(raw).strip()

    # unwrap one layer of quotes
    if (s.startswith('"') and s.endswith('"')) or (s.startswith("'") and s.endswith("'")):
        s = s[1:-1].strip()

    # Attempt parse
    try:
        parsed = json.loads(s)
    except Exception:
        return None

    if not isinstance(parsed, dict) or not parsed:
        return None

    # Fix common Render env issue: private_key has literal "\n"
    try:
        pk = parsed.get("private_key")
        if isinstance(pk, str) and "\\n" in pk and "\n" not in pk:
            parsed["private_key"] = pk.replace("\\n", "\n")
    except Exception:
        pass

    return parsed


def _get_creds_dict() -> Dict[str, Any]:
    # 1) dict from env.py
    if isinstance(GOOGLE_SHEETS_CREDENTIALS, dict) and GOOGLE_SHEETS_CREDENTIALS:
        return GOOGLE_SHEETS_CREDENTIALS

    # 2) raw JSON from env.py / env vars
    raw = GOOGLE_SHEETS_CREDENTIALS_RAW or os.getenv("GOOGLE_SHEETS_CREDENTIALS", "").strip()
    parsed = _parse_credentials_json(raw)
    if isinstance(parsed, dict) and parsed:
        return parsed

    raise RuntimeError(
        "Google Sheets credentials not available. "
        "Set GOOGLE_SHEETS_CREDENTIALS (full service-account JSON) in Render env vars."
    )


def get_sheets_service():
    global _SHEETS_SERVICE
    if _SHEETS_SERVICE is not None:
        return _SHEETS_SERVICE

    if Credentials is None or build is None:
        raise RuntimeError(
            "google-api-python-client and google-auth are required. Install:\n"
            "  pip install google-api-python-client google-auth"
        )

    creds_dict = _get_creds_dict()
    creds = Credentials.from_service_account_info(creds_dict, scopes=_SCOPES)
    _SHEETS_SERVICE = build("sheets", "v4", credentials=creds, cache_discovery=False)
    logger.info("[GoogleSheets] Sheets service initialized.")
    return _SHEETS_SERVICE


# ----------------------------------------------------------------------
# Google Sheets API wrappers (retry-friendly)
# ----------------------------------------------------------------------


def _should_retry_http_error(exc: Exception) -> bool:
    # googleapiclient.errors.HttpError has .status_code in some versions; in others parse content
    try:
        if hasattr(exc, "status_code"):
            code = int(getattr(exc, "status_code"))
            return code in (429, 500, 502, 503, 504)
    except Exception:
        pass
    # fallback: string match
    msg = str(exc)
    return any(s in msg for s in ("429", "500", "502", "503", "504", "Rate Limit"))


def _with_retries(fn, *, label: str):
    last_exc: Optional[Exception] = None
    for attempt in range(1, _SHEETS_RETRIES + 2):
        try:
            return fn()
        except Exception as exc:  # noqa: BLE001
            last_exc = exc
            if attempt > _SHEETS_RETRIES + 1 or not _should_retry_http_error(exc):
                logger.error("[GoogleSheets] %s failed: %s", label, exc)
                raise
            sleep_s = _SHEETS_RETRY_BASE_SLEEP * attempt
            logger.warning("[GoogleSheets] %s retry %d (sleep %.2fs): %s", label, attempt, sleep_s, exc)
            time.sleep(sleep_s)
    raise last_exc or RuntimeError(f"{label} failed unexpectedly")


# ----------------------------------------------------------------------
# Low-level Sheets helpers
# ----------------------------------------------------------------------


def read_range(spreadsheet_id: str, range_a1: str) -> List[List[Any]]:
    service = get_sheets_service()

    def _call():
        result = (
            service.spreadsheets()
            .values()
            .get(spreadsheetId=spreadsheet_id, range=range_a1)
            .execute()
        )
        return result.get("values", [])

    values = _with_retries(_call, label=f"read_range {spreadsheet_id}!{range_a1}")
    logger.info("[GoogleSheets] Read %d rows from %s!%s.", len(values), spreadsheet_id, range_a1)
    return values


def write_range(
    spreadsheet_id: str,
    range_a1: str,
    values: List[List[Any]],
    value_input_option: str = "RAW",
) -> Dict[str, Any]:
    service = get_sheets_service()
    body = {"values": values or []}

    def _call():
        return (
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

    result = _with_retries(_call, label=f"write_range {spreadsheet_id}!{range_a1}")
    logger.info("[GoogleSheets] Updated %s!%s (%s cells).", spreadsheet_id, range_a1, result.get("updatedCells", 0))
    return result


def clear_range(spreadsheet_id: str, range_a1: str) -> Dict[str, Any]:
    service = get_sheets_service()

    def _call():
        return (
            service.spreadsheets()
            .values()
            .clear(
                spreadsheetId=spreadsheet_id,
                range=range_a1,
                body={},
            )
            .execute()
        )

    result = _with_retries(_call, label=f"clear_range {spreadsheet_id}!{range_a1}")
    logger.info("[GoogleSheets] Cleared %s!%s.", spreadsheet_id, range_a1)
    return result


# ----------------------------------------------------------------------
# Backend /sheet-rows client
# ----------------------------------------------------------------------


def _normalize_endpoint(endpoint: str) -> str:
    return endpoint if endpoint.startswith("/") else f"/{endpoint}"


def _ensure_backend_base_url() -> str:
    base = (BACKEND_BASE_URL or "").strip()
    if not base:
        raise RuntimeError("BACKEND_BASE_URL is not configured; cannot call backend /sheet-rows endpoints.")
    if not base.startswith("http://") and not base.startswith("https://"):
        logger.warning("[Backend] BACKEND_BASE_URL has no scheme; assuming https://%s", base)
        base = "https://" + base
    return base.rstrip("/")


def _normalize_tickers(tickers: List[str]) -> List[str]:
    """
    Deduplicate while preserving order; normalize to upper-case.
    """
    seen = set()
    out: List[str] = []
    for t in tickers or []:
        tt = (t or "").strip()
        if not tt:
            continue
        tt = tt.upper()
        if tt not in seen:
            seen.add(tt)
            out.append(tt)
    return out


def _chunk_list(items: List[str], chunk_size: int) -> List[List[str]]:
    if chunk_size <= 0:
        return [items]
    return [items[i : i + chunk_size] for i in range(0, len(items), chunk_size)]


def _auth_headers() -> Dict[str, str]:
    headers: Dict[str, str] = {"Content-Type": "application/json; charset=utf-8"}

    token = APP_TOKEN or BACKUP_APP_TOKEN
    if token:
        # Backend accepts X-APP-TOKEN
        headers["X-APP-TOKEN"] = token

    return headers


def _http_post_json(url: str, payload: Dict[str, Any], timeout: float) -> Tuple[int, str]:
    data = json.dumps(payload, ensure_ascii=False).encode("utf-8")
    req = urllib.request.Request(url, data=data, headers=_auth_headers(), method="POST")

    with urllib.request.urlopen(req, timeout=timeout, context=_SSL_CONTEXT) as resp:
        status_code = getattr(resp, "status", resp.getcode())
        raw = resp.read()
        text = raw.decode("utf-8", errors="replace")
        return status_code, text


def _find_symbol_col(headers: List[Any]) -> int:
    if not headers:
        return 0
    for i, h in enumerate(headers):
        s = str(h or "").strip().lower()
        if s in {"symbol", "ticker"}:
            return i
    # common templates start with symbol
    return 0


def _reorder_rows_by_tickers(headers: List[Any], rows: List[List[Any]], tickers: List[str]) -> List[List[Any]]:
    """
    Ensure rows are returned in the same order as the requested tickers when possible.
    Uses the 'Symbol'/'Ticker' column if present; falls back to col 0.
    """
    if not rows or not tickers:
        return rows or []

    sym_col = _find_symbol_col(headers)
    mapping: Dict[str, List[Any]] = {}
    extras: List[List[Any]] = []

    for r in rows:
        try:
            sym = str(r[sym_col]).strip().upper() if len(r) > sym_col else ""
        except Exception:
            sym = ""
        if sym and sym not in mapping:
            mapping[sym] = r
        else:
            extras.append(r)

    ordered: List[List[Any]] = []
    for t in tickers:
        rt = mapping.get(t.upper())
        if rt is not None:
            ordered.append(rt)

    # append anything we couldn't map
    for r in extras:
        ordered.append(r)

    return ordered


def _call_backend_sheet_rows_once(
    endpoint: str,
    tickers: List[str],
    sheet_name: str,
    timeout: float,
) -> Dict[str, Any]:
    base = _ensure_backend_base_url()
    ep = _normalize_endpoint(endpoint)
    url = base + ep

    if not sheet_name or not str(sheet_name).strip():
        raise RuntimeError("sheet_name is required (must always be sent to backend).")

    clean_tickers = _normalize_tickers(tickers)

    payload: Dict[str, Any] = {
        "tickers": clean_tickers,
        "sheet_name": sheet_name,  # ALWAYS send
    }

    status_code, text = _http_post_json(url, payload, timeout)

    if status_code < 200 or status_code >= 300:
        preview = text[:500]
        if status_code in (401, 403):
            raise RuntimeError(
                f"Backend auth error HTTP {status_code} at {ep}. "
                f"Check APP_TOKEN / BACKUP_APP_TOKEN / HAS_SECURE_TOKEN. Body: {preview}"
            )
        if status_code == 429:
            raise RuntimeError(f"Backend rate-limited HTTP 429 at {ep}. Body: {preview}")
        raise RuntimeError(f"Backend error HTTP {status_code} at {ep}. Body: {preview}")

    try:
        parsed = json.loads(text)
    except Exception as exc:
        raise RuntimeError(f"Error parsing backend JSON from {ep}: {exc}. Body: {text[:300]}") from exc

    if not isinstance(parsed, dict):
        raise RuntimeError(f"Unexpected backend response type from {ep}: {type(parsed)}")

    if "headers" not in parsed or "rows" not in parsed:
        raise RuntimeError(f"Backend response from {ep} missing 'headers' or 'rows'.")

    headers = parsed.get("headers") or []
    rows = parsed.get("rows") or []
    parsed["rows"] = _reorder_rows_by_tickers(headers, rows, clean_tickers)

    return parsed


def _call_backend_sheet_rows(
    endpoint: str,
    tickers: List[str],
    sheet_name: str,
    timeout: float = _BACKEND_TIMEOUT,
    max_tickers_per_call: int = _BACKEND_MAX_TICKERS_PER_CALL,
    retries: int = _BACKEND_RETRIES,
) -> Dict[str, Any]:
    """
    Chunk + retry wrapper.

    Returns payload:
      { "headers": [...], "rows": [...], "meta": {...} }

    Rows are concatenated in the same order as the (deduped) input tickers.
    """
    base_tickers = _normalize_tickers(tickers)
    chunks = _chunk_list(base_tickers, max_tickers_per_call)

    all_headers: Optional[List[Any]] = None
    all_rows: List[List[Any]] = []
    meta_list: List[Dict[str, Any]] = []

    t0 = time.time()
    ep_norm = _normalize_endpoint(endpoint)

    for idx, chunk in enumerate(chunks, start=1):
        attempt = 0
        while attempt <= retries:
            try:
                attempt += 1
                logger.info(
                    "[Backend] %s chunk %d/%d (tickers=%d, attempt=%d, sheet_name=%s)",
                    ep_norm,
                    idx,
                    len(chunks),
                    len(chunk),
                    attempt,
                    sheet_name,
                )
                payload = _call_backend_sheet_rows_once(
                    endpoint=endpoint,
                    tickers=chunk,
                    sheet_name=sheet_name,
                    timeout=timeout,
                )

                headers = payload.get("headers") or []
                rows = payload.get("rows") or []
                meta = payload.get("meta") or {}

                if all_headers is None:
                    all_headers = list(headers)
                else:
                    if list(headers) != list(all_headers):
                        logger.warning("[Backend] Headers differ across chunks; using first chunk headers.")

                # rows are already re-ordered per chunk
                all_rows.extend(rows)
                if isinstance(meta, dict) and meta:
                    meta_list.append(meta)

                break

            except (urllib.error.URLError, urllib.error.HTTPError) as exc:
                if attempt > retries:
                    raise RuntimeError(f"Backend network error after retries: {exc}") from exc
                sleep_s = 0.6 * attempt
                logger.warning("[Backend] Network error (retry %d, sleep %.2fs): %s", attempt, sleep_s, exc)
                time.sleep(sleep_s)

            except Exception as exc:
                # retry only for likely transient issues
                msg = str(exc)
                transient = ("timeout" in msg.lower()) or ("429" in msg) or ("502" in msg) or ("503" in msg) or ("504" in msg)
                if attempt > retries or not transient:
                    raise
                sleep_s = 0.6 * attempt
                logger.warning("[Backend] Transient error (retry %d, sleep %.2fs): %s", attempt, sleep_s, exc)
                time.sleep(sleep_s)

    dt = time.time() - t0
    logger.info("[Backend] Completed %s (tickers=%d, chunks=%d) in %.2fs", ep_norm, len(base_tickers), len(chunks), dt)

    # Final global reorder in case backend returned weird per-chunk ordering
    all_rows = _reorder_rows_by_tickers(all_headers or [], all_rows, base_tickers)

    return {
        "headers": all_headers or [],
        "rows": all_rows,
        "meta": {
            "sheet_name": sheet_name,
            "endpoint": ep_norm,
            "tickers_requested": len(base_tickers),
            "chunks": len(chunks),
            "duration_seconds": round(dt, 3),
            "chunk_meta": meta_list,
        },
    }


async def _call_backend_sheet_rows_async(
    endpoint: str,
    tickers: List[str],
    sheet_name: str,
    timeout: float = _BACKEND_TIMEOUT,
) -> Dict[str, Any]:
    loop = asyncio.get_running_loop()
    return await loop.run_in_executor(
        None,
        lambda: _call_backend_sheet_rows(endpoint=endpoint, tickers=tickers, sheet_name=sheet_name, timeout=timeout),
    )


# ----------------------------------------------------------------------
# Value builders + refresh helpers
# ----------------------------------------------------------------------


def _build_values_from_backend_payload(payload: Dict[str, Any]) -> List[List[Any]]:
    headers = payload.get("headers") or []
    rows = payload.get("rows") or []
    values: List[List[Any]] = []
    if headers:
        values.append(list(headers))
    values.extend(rows)
    return values


def _safe_clear_mode_range(sheet_name: str, start_cell: str) -> str:
    """
    Default clear range: from start cell row to ZZ, leaving rows above intact.
    Example: start_cell="A5" -> clear "Sheet!A5:ZZ"
    """
    start = (start_cell or "A1").strip().upper()
    col = "".join([c for c in start if c.isalpha()]) or "A"
    row = "".join([c for c in start if c.isdigit()]) or "1"
    return f"{sheet_name}!{col}{row}:ZZ"


def _refresh_sheet_generic(
    endpoint: str,
    spreadsheet_id: str,
    sheet_name: str,
    tickers: List[str],
    start_cell: str,
    clear_existing: bool,
    value_input_option: str,
    log_label: str,
) -> Dict[str, Any]:
    if not spreadsheet_id:
        raise RuntimeError("spreadsheet_id is required.")
    if not sheet_name or not str(sheet_name).strip():
        raise RuntimeError("sheet_name is required.")
    if not start_cell:
        start_cell = "A1"

    split = split_tickers_by_market(tickers)
    logger.info(
        "%s Refreshing %s!%s (tickers=%d | KSA=%d | Global=%d) via %s",
        log_label,
        sheet_name,
        start_cell,
        len(_normalize_tickers(tickers)),
        len(split["ksa"]),
        len(split["global"]),
        _normalize_endpoint(endpoint),
    )

    payload = _call_backend_sheet_rows(endpoint=endpoint, tickers=tickers, sheet_name=sheet_name)
    values = _build_values_from_backend_payload(payload)

    if clear_existing:
        clear_a1 = _safe_clear_mode_range(sheet_name, start_cell)
        clear_range(spreadsheet_id, clear_a1)

    target_a1 = f"{sheet_name}!{start_cell}"
    return write_range(spreadsheet_id, target_a1, values, value_input_option=value_input_option)


def refresh_sheet_with_enriched_quotes(
    spreadsheet_id: str,
    sheet_name: str,
    tickers: List[str],
    start_cell: str = "A1",
    clear_existing: bool = False,
    value_input_option: str = "RAW",
) -> Dict[str, Any]:
    return _refresh_sheet_generic(
        endpoint="/v1/enriched/sheet-rows",
        spreadsheet_id=spreadsheet_id,
        sheet_name=sheet_name,
        tickers=tickers,
        start_cell=start_cell,
        clear_existing=clear_existing,
        value_input_option=value_input_option,
        log_label="[Sheets][Enriched]",
    )


def refresh_sheet_with_ai_analysis(
    spreadsheet_id: str,
    sheet_name: str,
    tickers: List[str],
    start_cell: str = "A1",
    clear_existing: bool = False,
    value_input_option: str = "RAW",
) -> Dict[str, Any]:
    return _refresh_sheet_generic(
        endpoint="/v1/analysis/sheet-rows",
        spreadsheet_id=spreadsheet_id,
        sheet_name=sheet_name,
        tickers=tickers,
        start_cell=start_cell,
        clear_existing=clear_existing,
        value_input_option=value_input_option,
        log_label="[Sheets][AI]",
    )


def refresh_sheet_with_advanced_analysis(
    spreadsheet_id: str,
    sheet_name: str,
    tickers: List[str],
    start_cell: str = "A1",
    clear_existing: bool = False,
    value_input_option: str = "RAW",
) -> Dict[str, Any]:
    return _refresh_sheet_generic(
        endpoint="/v1/advanced/sheet-rows",
        spreadsheet_id=spreadsheet_id,
        sheet_name=sheet_name,
        tickers=tickers,
        start_cell=start_cell,
        clear_existing=clear_existing,
        value_input_option=value_input_option,
        log_label="[Sheets][Advanced]",
    )


# ----------------------------------------------------------------------
# Async refresh versions
# ----------------------------------------------------------------------


async def _refresh_sheet_generic_async(
    endpoint: str,
    spreadsheet_id: str,
    sheet_name: str,
    tickers: List[str],
    start_cell: str,
    clear_existing: bool,
    value_input_option: str,
    log_label: str,
) -> Dict[str, Any]:
    if not spreadsheet_id:
        raise RuntimeError("spreadsheet_id is required.")
    if not sheet_name or not str(sheet_name).strip():
        raise RuntimeError("sheet_name is required.")
    if not start_cell:
        start_cell = "A1"

    split = split_tickers_by_market(tickers)
    logger.info(
        "%s (async) Refreshing %s!%s (tickers=%d | KSA=%d | Global=%d) via %s",
        log_label,
        sheet_name,
        start_cell,
        len(_normalize_tickers(tickers)),
        len(split["ksa"]),
        len(split["global"]),
        _normalize_endpoint(endpoint),
    )

    payload = await _call_backend_sheet_rows_async(endpoint=endpoint, tickers=tickers, sheet_name=sheet_name)
    values = _build_values_from_backend_payload(payload)

    loop = asyncio.get_running_loop()

    if clear_existing:
        clear_a1 = _safe_clear_mode_range(sheet_name, start_cell)
        await loop.run_in_executor(None, lambda: clear_range(spreadsheet_id, clear_a1))

    target_a1 = f"{sheet_name}!{start_cell}"
    return await loop.run_in_executor(
        None,
        lambda: write_range(spreadsheet_id, target_a1, values, value_input_option=value_input_option),
    )


async def refresh_sheet_with_enriched_quotes_async(
    spreadsheet_id: str,
    sheet_name: str,
    tickers: List[str],
    start_cell: str = "A1",
    clear_existing: bool = False,
    value_input_option: str = "RAW",
) -> Dict[str, Any]:
    return await _refresh_sheet_generic_async(
        endpoint="/v1/enriched/sheet-rows",
        spreadsheet_id=spreadsheet_id,
        sheet_name=sheet_name,
        tickers=tickers,
        start_cell=start_cell,
        clear_existing=clear_existing,
        value_input_option=value_input_option,
        log_label="[Sheets][Enriched]",
    )


async def refresh_sheet_with_ai_analysis_async(
    spreadsheet_id: str,
    sheet_name: str,
    tickers: List[str],
    start_cell: str = "A1",
    clear_existing: bool = False,
    value_input_option: str = "RAW",
) -> Dict[str, Any]:
    return await _refresh_sheet_generic_async(
        endpoint="/v1/analysis/sheet-rows",
        spreadsheet_id=spreadsheet_id,
        sheet_name=sheet_name,
        tickers=tickers,
        start_cell=start_cell,
        clear_existing=clear_existing,
        value_input_option=value_input_option,
        log_label="[Sheets][AI]",
    )


async def refresh_sheet_with_advanced_analysis_async(
    spreadsheet_id: str,
    sheet_name: str,
    tickers: List[str],
    start_cell: str = "A1",
    clear_existing: bool = False,
    value_input_option: str = "RAW",
) -> Dict[str, Any]:
    return await _refresh_sheet_generic_async(
        endpoint="/v1/advanced/sheet-rows",
        spreadsheet_id=spreadsheet_id,
        sheet_name=sheet_name,
        tickers=tickers,
        start_cell=start_cell,
        clear_existing=clear_existing,
        value_input_option=value_input_option,
        log_label="[Sheets][Advanced]",
    )


__all__ = [
    "DEFAULT_SPREADSHEET_ID",
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
