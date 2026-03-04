#!/usr/bin/env python3
# routes/advanced_sheet_rows.py
"""
================================================================================
Advanced Sheet-Rows Router — v1.3.0 (PHASE 3/5 / SCHEMA-DRIVEN / SPECIAL-SAFE)
================================================================================

Endpoint:
  POST /v1/advanced/sheet-rows

Contract:
- Accept page aliases via page_catalog.normalize_page_name()
- Accept "sheet" or "page" (and common variants) from request body
- Reject forbidden/removed pages (e.g., KSA_Tadawul, Advisor_Criteria)
- Return FULL schema headers + keys from schema_registry for the requested page
- Stable ordering:
    - headers/keys follow schema_registry order
    - rows follow request symbols order (instrument pages)
- Optional rows_matrix for legacy clients (default true)

Special (FIXES your PowerShell evidence):
- ✅ Data_Dictionary is computed locally from schema_registry (no engine call)
- ✅ Insights_Analysis is computed via core.analysis.insights_builder (7 columns)
    (prevents accidental fallback to the 80-col instrument schema)

Startup-safe:
- No network I/O at import-time
- All heavy work is inside the request handler / engine calls
================================================================================
"""

from __future__ import annotations

import inspect
import os
import time
import uuid
from dataclasses import is_dataclass
from typing import Any, Dict, List, Optional, Sequence, Tuple

from fastapi import APIRouter, Body, Header, HTTPException, Query, Request, status

# -----------------------------------------------------------------------------
# Schema + pages
# -----------------------------------------------------------------------------
try:
    from core.sheets.schema_registry import get_sheet_spec  # type: ignore
except Exception as e:  # pragma: no cover
    get_sheet_spec = None  # type: ignore
    _SCHEMA_IMPORT_ERROR = repr(e)
else:
    _SCHEMA_IMPORT_ERROR = None

try:
    from core.sheets.page_catalog import CANONICAL_PAGES, normalize_page_name  # type: ignore
except Exception:  # pragma: no cover
    CANONICAL_PAGES = set()  # type: ignore

    def normalize_page_name(name: str, allow_output_pages: bool = True) -> str:  # type: ignore
        return (name or "").strip()

# Forbidden pages (optional, but we enforce a safe default)
try:
    from core.sheets.page_catalog import FORBIDDEN_PAGES  # type: ignore
except Exception:
    FORBIDDEN_PAGES = {"KSA_Tadawul", "Advisor_Criteria"}  # type: ignore

# Data Dictionary builder
try:
    from core.sheets.data_dictionary import build_data_dictionary_rows  # type: ignore
except Exception:  # pragma: no cover
    build_data_dictionary_rows = None  # type: ignore

# core.config is preferred for auth + flags, but router must be safe if unavailable
try:
    from core.config import auth_ok, get_settings_cached  # type: ignore
except Exception:  # pragma: no cover
    auth_ok = None  # type: ignore

    def get_settings_cached(*args, **kwargs):  # type: ignore
        return None


ADVANCED_SHEET_ROWS_VERSION = "1.3.0"
router = APIRouter(prefix="/v1/advanced", tags=["Advanced Sheet Rows"])


# -----------------------------------------------------------------------------
# Utilities
# -----------------------------------------------------------------------------
def _strip(v: Any) -> str:
    try:
        return str(v).strip()
    except Exception:
        return ""


def _to_plain_dict(obj: Any) -> Dict[str, Any]:
    if obj is None:
        return {}
    if isinstance(obj, dict):
        return obj
    try:
        # pydantic v2
        if hasattr(obj, "model_dump") and callable(getattr(obj, "model_dump")):
            return obj.model_dump(mode="python")  # type: ignore
        # pydantic v1
        if hasattr(obj, "dict") and callable(getattr(obj, "dict")):
            return obj.dict()  # type: ignore
        # dataclass
        if is_dataclass(obj):
            try:
                return {k: getattr(obj, k) for k in obj.__dict__.keys() if not str(k).startswith("_")}
            except Exception:
                return {}
        # plain object
        if hasattr(obj, "__dict__"):
            try:
                return dict(obj.__dict__)
            except Exception:
                return {}
    except Exception:
        return {}
    return {}


async def _maybe_await(x: Any) -> Any:
    try:
        if inspect.isawaitable(x):
            return await x
    except Exception:
        pass
    return x


def _get_list(body: Dict[str, Any], *keys: str) -> List[str]:
    for k in keys:
        v = body.get(k)
        if isinstance(v, list):
            out: List[str] = []
            for item in v:
                s = _strip(item)
                if s:
                    out.append(s)
            return out
    return []


def _get_int(body: Dict[str, Any], key: str, default: int) -> int:
    v = body.get(key)
    try:
        if isinstance(v, bool):
            return default
        if isinstance(v, (int, float)):
            return int(v)
        s = _strip(v)
        if s:
            return int(float(s))
    except Exception:
        pass
    return default


def _get_bool(body: Dict[str, Any], key: str, default: bool) -> bool:
    v = body.get(key)
    if isinstance(v, bool):
        return v
    if isinstance(v, (int, float)):
        try:
            return bool(int(v))
        except Exception:
            return default
    if isinstance(v, str):
        return v.strip().lower() in {"1", "true", "yes", "y", "on"}
    return default


def _allow_query_token(settings: Any, request: Request) -> bool:
    # Settings-first
    try:
        if settings is not None:
            return bool(getattr(settings, "allow_query_token", False))
    except Exception:
        pass

    # Env fallback
    v = (os.getenv("ALLOW_QUERY_TOKEN", "") or "").strip().lower()
    if v in {"1", "true", "yes", "y", "on"}:
        return True

    # Header override (optional; kept for debugging only)
    try:
        hv = _strip(request.headers.get("X-Allow-Query-Token"))
        if hv.lower() in {"1", "true", "yes"}:
            return True
    except Exception:
        pass

    return False


def _extract_auth_token(
    *,
    token_query: Optional[str],
    x_app_token: Optional[str],
    authorization: Optional[str],
    settings: Any,
    request: Request,
) -> str:
    # Header token
    auth_token = _strip(x_app_token)

    # Bearer token
    if authorization and authorization.strip().lower().startswith("bearer "):
        auth_token = authorization.strip().split(" ", 1)[1].strip()

    # Query token only if allowed
    if token_query and not auth_token and _allow_query_token(settings, request):
        auth_token = _strip(token_query)

    return auth_token


def _pick_page_from_body(body: Dict[str, Any]) -> str:
    """
    Accept both "page" and "sheet" and common variants.
    (Important: your PowerShell uses body @{ sheet=$sheet; limit=1 }.)
    """
    for k in ("page", "sheet", "sheet_name", "sheetName", "name"):
        v = body.get(k)
        s = _strip(v)
        if s:
            return s
    return ""


def _slice(rows: List[Dict[str, Any]], *, limit: int, offset: int) -> List[Dict[str, Any]]:
    if offset <= 0 and (limit <= 0 or limit >= len(rows)):
        return rows
    start = max(0, int(offset))
    if limit <= 0:
        return rows[start:]
    end = start + max(0, int(limit))
    return rows[start:end]


def _rows_matrix(rows: List[Dict[str, Any]], keys: Sequence[str]) -> List[List[Any]]:
    return [[r.get(k) for k in keys] for r in rows]


# -----------------------------------------------------------------------------
# Row normalization for Insights_Analysis (supports builder returning keys OR headers)
# -----------------------------------------------------------------------------
def _normalize_to_schema_keys(
    *,
    schema_keys: Sequence[str],
    schema_headers: Sequence[str],
    raw: Dict[str, Any],
) -> Dict[str, Any]:
    raw = raw or {}
    raw_ci = {str(k).strip().lower(): v for k, v in raw.items()}

    # Map key -> header (same ordering as schema)
    header_by_key: Dict[str, str] = {}
    for k, h in zip(schema_keys, schema_headers):
        header_by_key[str(k)] = str(h)

    out: Dict[str, Any] = {}
    for k in schema_keys:
        ks = str(k)
        v = None

        # Direct key match
        if ks in raw:
            v = raw.get(ks)
        else:
            # Case-insensitive key match
            v = raw_ci.get(ks.lower())

        # Header fallback (if builder used human headers)
        if v is None:
            h = header_by_key.get(ks, "")
            if h:
                if h in raw:
                    v = raw.get(h)
                else:
                    v = raw_ci.get(h.strip().lower())

        out[ks] = v

    return out


# -----------------------------------------------------------------------------
# Engine accessor (lazy)
# -----------------------------------------------------------------------------
async def _get_engine(request: Request) -> Optional[Any]:
    # Prefer app.state.engine
    try:
        st = getattr(request.app, "state", None)
        if st and getattr(st, "engine", None):
            return st.engine
    except Exception:
        pass

    # Fallback to core.data_engine_v2.get_engine()
    try:
        from core.data_engine_v2 import get_engine  # type: ignore

        eng = get_engine()
        eng = await _maybe_await(eng)
        return eng
    except Exception:
        return None


async def _call_engine(fn: Any, *args: Any, **kwargs: Any) -> Any:
    res = fn(*args, **kwargs)
    return await _maybe_await(res)


def _dict_is_symbol_map(d: Dict[str, Any], symbols: List[str]) -> bool:
    """
    Detect whether a dict is probably {symbol: row} rather than an envelope like:
      {"status": "...", "headers": [...], "rows": [...]}
    """
    if not isinstance(d, dict) or not symbols:
        return False

    symset = set(symbols)
    keys = [k for k in d.keys() if isinstance(k, str)]
    if not keys:
        return False

    hit = sum(1 for k in keys if k in symset)
    if hit == len(symset) and len(symset) > 0:
        return True

    if hit >= max(1, min(3, len(symset) // 3)):
        return True

    return False


async def _fetch_quotes(engine: Any, symbols: List[str], mode: str = "") -> Dict[str, Any]:
    """
    Best-effort enriched quote fetcher.
    Returns: {requested_symbol: row_obj_or_dict}
    """
    if not symbols:
        return {}

    # Best batch candidate (DataEngineV5)
    for method in ("get_enriched_quotes_batch",):
        fn = getattr(engine, method, None)
        if callable(fn):
            try:
                res = await _call_engine(fn, symbols, mode=mode) if mode else await _call_engine(fn, symbols)
                if isinstance(res, dict) and _dict_is_symbol_map(res, symbols):
                    return res
            except Exception:
                pass

    # Other batch candidates
    candidates = ("get_enriched_quotes", "get_quotes_batch", "quotes_batch")
    for method in candidates:
        fn = getattr(engine, method, None)
        if callable(fn):
            try:
                res = await _call_engine(fn, symbols, mode=mode) if mode else await _call_engine(fn, symbols)
                if isinstance(res, dict):
                    if _dict_is_symbol_map(res, symbols):
                        return res
                    data = res.get("data") or res.get("rows") or res.get("items")
                    if isinstance(data, dict) and _dict_is_symbol_map(data, symbols):
                        return data
                    if isinstance(data, list):
                        return {s: r for s, r in zip(symbols, data)}
                if isinstance(res, list):
                    return {s: r for s, r in zip(symbols, res)}
            except Exception:
                pass

    # Per-symbol fallback (prefer dict-returning method if available)
    out: Dict[str, Any] = {}
    per_dict_fn = getattr(engine, "get_enriched_quote_dict", None)
    per_fn = getattr(engine, "get_enriched_quote", None) or getattr(engine, "get_quote", None)

    for s in symbols:
        try:
            if callable(per_dict_fn):
                r = await _call_engine(per_dict_fn, s, mode=mode) if mode else await _call_engine(per_dict_fn, s)
                out[s] = r
            elif callable(per_fn):
                r = await _call_engine(per_fn, s, mode=mode) if mode else await _call_engine(per_fn, s)
                out[s] = r
            else:
                out[s] = {"symbol": s, "error": "engine_missing_get_enriched_quote"}
        except Exception as e:
            out[s] = {"symbol": s, "error": str(e)}

    return out


# -----------------------------------------------------------------------------
# Insights_Analysis builder adapter (lazy, best-effort)
# -----------------------------------------------------------------------------
async def _build_insights_analysis_rows(
    *,
    request: Request,
    settings: Any,
    mode: str,
    body: Dict[str, Any],
    schema_keys: Sequence[str],
    schema_headers: Sequence[str],
) -> List[Dict[str, Any]]:
    """
    Calls core.analysis.insights_builder to produce the Insights_Analysis rows.

    We support multiple possible builder names/signatures to avoid tight coupling.
    Expected output: list[dict] (keys should align to schema keys, but we normalize either way).
    """
    # Import inside call (startup-safe)
    try:
        mod = __import__("core.analysis.insights_builder", fromlist=["*"])
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail={"error": "Insights builder import failed", "detail": str(e)},
        )

    # Candidate function names (support future evolution)
    fn = None
    for name in (
        "build_insights_analysis_rows",
        "build_insights_rows",
        "build_insights_analysis",
        "get_insights_rows",
        "build_rows",
    ):
        cand = getattr(mod, name, None)
        if callable(cand):
            fn = cand
            break

    if fn is None:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail={"error": "Insights builder has no callable builder function"},
        )

    # Try calling with progressively simpler signatures
    last_err: Optional[Exception] = None
    candidates: List[Tuple[Tuple[Any, ...], Dict[str, Any]]] = [
        ((), {"request": request, "settings": settings, "mode": mode, "body": body}),
        ((), {"request": request, "settings": settings, "mode": mode}),
        ((), {"settings": settings, "mode": mode, "body": body}),
        ((), {"settings": settings, "mode": mode}),
        ((), {"mode": mode, "body": body}),
        ((), {"mode": mode}),
        ((), {"body": body}),
        ((), {}),
    ]

    out = None
    for args, kwargs in candidates:
        try:
            res = fn(*args, **kwargs)
            out = await _maybe_await(res)
            break
        except TypeError as e:
            last_err = e
            continue
        except Exception as e:
            last_err = e
            continue

    if out is None:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail={"error": "Insights builder call failed", "detail": str(last_err) if last_err else "unknown"},
        )

    # Coerce to rows list
    rows: List[Dict[str, Any]] = []
    if isinstance(out, list):
        rows = [r for r in out if isinstance(r, dict)]
    elif isinstance(out, dict):
        # allow envelope: {"rows": [...]}
        r2 = out.get("rows") or out.get("data") or out.get("items") or out.get("records")
        if isinstance(r2, list):
            rows = [r for r in r2 if isinstance(r, dict)]
        else:
            # single dict row
            rows = [out]
    else:
        rows = []

    # Normalize to schema keys (supports builder using headers instead of keys)
    norm: List[Dict[str, Any]] = []
    for r in rows:
        norm.append(_normalize_to_schema_keys(schema_keys=schema_keys, schema_headers=schema_headers, raw=r))

    return norm


# -----------------------------------------------------------------------------
# Route
# -----------------------------------------------------------------------------
@router.post("/sheet-rows")
async def advanced_sheet_rows(
    request: Request,
    body: Dict[str, Any] = Body(...),
    mode: str = Query(default="", description="Optional mode hint for engine/provider"),
    token: Optional[str] = Query(default=None, description="Auth token (query only if allowed)"),
    x_app_token: Optional[str] = Header(default=None, alias="X-APP-TOKEN"),
    authorization: Optional[str] = Header(default=None, alias="Authorization"),
    x_request_id: Optional[str] = Header(default=None, alias="X-Request-ID"),
) -> Dict[str, Any]:
    start = time.time()
    request_id = x_request_id or str(uuid.uuid4())

    # --- schema registry availability ---
    if get_sheet_spec is None:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail={
                "error": "Schema registry unavailable",
                "module": "routes.advanced_sheet_rows",
                "schema_import_error": _SCHEMA_IMPORT_ERROR,
            },
        )

    # --- settings (optional) ---
    settings = None
    try:
        settings = get_settings_cached()
    except Exception:
        settings = None

    # --- auth ---
    auth_token = _extract_auth_token(
        token_query=token,
        x_app_token=x_app_token,
        authorization=authorization,
        settings=settings,
        request=request,
    )

    if auth_ok is not None:
        if not auth_ok(
            token=auth_token,
            authorization=authorization,
            headers={"X-APP-TOKEN": x_app_token, "Authorization": authorization},
        ):
            raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid token")

    # --- parse page ---
    page_raw = _pick_page_from_body(body) or "Market_Leaders"
    try:
        page = normalize_page_name(page_raw, allow_output_pages=True)
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail={
                "error": f"Invalid page: {str(e)}",
                "allowed_pages": list(CANONICAL_PAGES) if CANONICAL_PAGES else None,
            },
        )

    forbidden = set(FORBIDDEN_PAGES or set())
    if page in forbidden:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail={"error": f"Forbidden/removed page: {page}", "forbidden_pages": sorted(list(forbidden))},
        )

    # --- common paging options (used for special pages too) ---
    limit = max(1, min(5000, int(_get_int(body, "limit", 2000))))
    offset = max(0, int(_get_int(body, "offset", 0)))
    include_matrix = _get_bool(body, "include_matrix", True)

    # --- schema ---
    try:
        spec = get_sheet_spec("Data_Dictionary" if page == "Data_Dictionary" else page)
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail={
                "error": f"Unknown page schema: {page}",
                "detail": str(e),
                "allowed_pages": list(CANONICAL_PAGES) if CANONICAL_PAGES else None,
            },
        )

    headers = [c.header for c in getattr(spec, "columns", [])]
    keys = [c.key for c in getattr(spec, "columns", [])]

    if not headers or not keys:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail={"error": f"Schema for page '{page}' is empty", "page": page},
        )

    # -----------------------------------------------------------------------------
    # ✅ SPECIAL: Insights_Analysis (7-col builder)
    # -----------------------------------------------------------------------------
    if page == "Insights_Analysis":
        rows = await _build_insights_analysis_rows(
            request=request,
            settings=settings,
            mode=mode or "",
            body=body,
            schema_keys=keys,
            schema_headers=headers,
        )
        rows = _slice(rows, limit=limit, offset=offset)

        return {
            "status": "success",
            "page": page,
            "headers": headers,
            "keys": keys,
            "rows": rows,
            "rows_matrix": _rows_matrix(rows, keys) if include_matrix else None,
            "version": ADVANCED_SHEET_ROWS_VERSION,
            "request_id": request_id,
            "meta": {
                "duration_ms": (time.time() - start) * 1000.0,
                "schema_mode": "insights_builder",
                "mode": mode,
                "limit": limit,
                "offset": offset,
                "count": len(rows),
            },
        }

    # -----------------------------------------------------------------------------
    # ✅ SPECIAL: Data_Dictionary (local compute)
    # -----------------------------------------------------------------------------
    if page == "Data_Dictionary":
        if build_data_dictionary_rows is None:
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail={"error": "Data_Dictionary builder unavailable"},
            )

        rows_dict = build_data_dictionary_rows(include_meta_sheet=True)
        # rows_dict should already be aligned to schema keys (from your v2.2.0 generator),
        # but we still normalize defensively.
        rows_norm = [_normalize_to_schema_keys(schema_keys=keys, schema_headers=headers, raw=(r or {})) for r in (rows_dict or [])]
        rows_norm = _slice(rows_norm, limit=limit, offset=offset)

        return {
            "status": "success",
            "page": page,
            "headers": headers,
            "keys": keys,
            "rows": rows_norm,
            "rows_matrix": _rows_matrix(rows_norm, keys) if include_matrix else None,
            "version": ADVANCED_SHEET_ROWS_VERSION,
            "request_id": request_id,
            "meta": {
                "duration_ms": (time.time() - start) * 1000.0,
                "schema_mode": "data_dictionary",
                "limit": limit,
                "offset": offset,
                "count": len(rows_norm),
            },
        }

    # -----------------------------------------------------------------------------
    # Instrument pages: fetch quotes and project to FULL schema
    # -----------------------------------------------------------------------------
    symbols = _get_list(body, "symbols", "tickers")
    top_n = max(1, min(2000, int(_get_int(body, "top_n", 50))))
    symbols = symbols[:top_n]

    # Always return schema even when symbols list is empty
    if not symbols:
        return {
            "status": "success",
            "page": page,
            "headers": headers,
            "keys": keys,
            "rows": [],
            "rows_matrix": [] if include_matrix else None,
            "version": ADVANCED_SHEET_ROWS_VERSION,
            "request_id": request_id,
            "meta": {
                "duration_ms": (time.time() - start) * 1000.0,
                "requested": 0,
                "errors": 0,
                "mode": mode,
            },
        }

    engine = await _get_engine(request)
    if not engine:
        raise HTTPException(status_code=status.HTTP_503_SERVICE_UNAVAILABLE, detail="Data engine unavailable")

    quotes = await _fetch_quotes(engine, symbols, mode=mode or "")
    normalized_rows: List[Dict[str, Any]] = []
    errors = 0

    # Ensure rows follow requested symbols order
    for sym in symbols:
        raw_obj = quotes.get(sym)
        raw = _to_plain_dict(raw_obj)
        if isinstance(raw, dict) and raw.get("error"):
            errors += 1
        normalized_rows.append(_normalize_to_schema_keys(schema_keys=keys, schema_headers=headers, raw=raw))

    status_out = "success" if errors == 0 else ("partial" if errors < len(symbols) else "error")

    return {
        "status": status_out,
        "page": page,
        "headers": headers,
        "keys": keys,
        "rows": normalized_rows,
        "rows_matrix": _rows_matrix(normalized_rows, keys) if include_matrix else None,
        "error": f"{errors} errors" if errors else None,
        "version": ADVANCED_SHEET_ROWS_VERSION,
        "request_id": request_id,
        "meta": {
            "duration_ms": (time.time() - start) * 1000.0,
            "requested": len(symbols),
            "errors": errors,
            "mode": mode,
        },
    }


__all__ = ["router", "ADVANCED_SHEET_ROWS_VERSION"]
