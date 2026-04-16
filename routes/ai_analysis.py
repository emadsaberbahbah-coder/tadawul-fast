#!/usr/bin/env python3
"""
routes/analysis.py
------------------------------------------------------------
TADAWUL ENTERPRISE ANALYSIS ROUTER — v8.9.0 (SCHEMA-ALIGNED)
Schema-driven /sheet-rows (FIXES special sheets) + Hardened Health/Metrics

Changes v8.8.0 → v8.9.0
------------------------
- FIX: _get_sheet_spec() hardened with multi-path fallback
       (core.sheets.schema_registry → core.schema_registry → schema_registry)
- FIX: _canonicalize_sheet_name() hardened with multi-path fallback
       (core.sheets.page_catalog → core.page_catalog → page_catalog)
- FIX: _get_all_sheet_names() hardened with multi-path fallback
- FIX: health() page_catalog import hardened with multi-path fallback
- FIX: Data_Dictionary fmt attribute lookup order corrected —
       canonical attr is 'fmt'; 'format' is a legacy alias
       (was: getattr(c, "format") or getattr(c, "fmt"),
        now: getattr(c, "fmt") or getattr(c, "format"))

Original v8.8.0 purpose
------------------------
- /v1/analysis/sheet-rows returns TRUE schema headers/keys/order for:
    Insights_Analysis (7), Top_10_Investments (83), Data_Dictionary (9)
- Startup-safe: no network/heavy imports at import-time (lazy inside handlers)
- Auth consistent (best-effort) via core.config.auth_ok when available

Mounted base router:
- prefix="/v1/analysis"
- originally: POST /sheet-rows (removed + replaced here)
"""

from __future__ import annotations

import logging
import os
from typing import Any, Dict, List, Optional, Tuple

from fastapi import Body, Header, HTTPException, Query, Request, Response, status

# ---------------------------------------------------------------------------
# Base router (prefix="/v1/analysis") from analysis_sheet_rows
# ---------------------------------------------------------------------------
from routes.analysis_sheet_rows import router  # noqa: F401

logger = logging.getLogger("routes.analysis")

ANALYSIS_VERSION = "8.9.0"


# ---------------------------------------------------------------------------
# Optional Prometheus (safe)
# ---------------------------------------------------------------------------
try:
    from prometheus_client import CONTENT_TYPE_LATEST, generate_latest  # type: ignore

    _PROMETHEUS_AVAILABLE = True
except Exception:
    generate_latest = None  # type: ignore
    CONTENT_TYPE_LATEST = "text/plain"
    _PROMETHEUS_AVAILABLE = False


# ---------------------------------------------------------------------------
# Optional core.config (safe)
# ---------------------------------------------------------------------------
try:
    from core.config import auth_ok, get_settings_cached  # type: ignore
except Exception:
    auth_ok = None  # type: ignore

    def get_settings_cached(*args, **kwargs):  # type: ignore
        return None


# ---------------------------------------------------------------------------
# Small safe helpers
# ---------------------------------------------------------------------------
def _safe_env_port() -> Optional[str]:
    p = (os.getenv("PORT") or "").strip()
    return p or None


def _safe_engine_type(engine: Any) -> str:
    try:
        return type(engine).__name__
    except Exception:
        return "unknown"


def _safe_bool_env(name: str, default: bool = False) -> bool:
    try:
        v = (os.getenv(name, str(default)) or "").strip().lower()
        return v in ("1", "true", "yes", "y", "on", "t")
    except Exception:
        return default


# ---------------------------------------------------------------------------
# Auth helper (best-effort; no startup dependency)
# ---------------------------------------------------------------------------
def _extract_auth_token(
    *,
    token_query: Optional[str],
    x_app_token: Optional[str],
    authorization: Optional[str],
) -> str:
    auth_token = (x_app_token or "").strip()
    if authorization and authorization.strip().lower().startswith("bearer "):
        auth_token = authorization.strip().split(" ", 1)[1].strip()

    if token_query and not auth_token:
        allow_query = False
        try:
            settings = get_settings_cached()
            allow_query = bool(getattr(settings, "allow_query_token", False))
        except Exception:
            allow_query = False
        if allow_query:
            auth_token = token_query.strip()

    return auth_token


def _require_auth_or_401(
    *,
    token_query: Optional[str],
    x_app_token: Optional[str],
    authorization: Optional[str],
) -> None:
    if auth_ok is None:
        return
    auth_token = _extract_auth_token(
        token_query=token_query,
        x_app_token=x_app_token,
        authorization=authorization,
    )
    if not auth_ok(
        token=auth_token,
        authorization=authorization,
        headers={"X-APP-TOKEN": x_app_token, "Authorization": authorization},
    ):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid token"
        )


# ---------------------------------------------------------------------------
# Engine accessor (lazy + safe)
# ---------------------------------------------------------------------------
async def _get_engine(request: Request) -> Optional[Any]:
    # Prefer app.state.engine (set by main.py lifespan)
    try:
        st = getattr(request.app, "state", None)
        if st and getattr(st, "engine", None):
            return st.engine
    except Exception:
        pass

    for modpath in ("core.data_engine_v2", "core.data_engine"):
        try:
            mod = __import__(modpath, fromlist=["get_engine"])
            get_engine = getattr(mod, "get_engine", None)
            if callable(get_engine):
                eng = get_engine()
                if hasattr(eng, "__await__"):
                    eng = await eng
                return eng
        except Exception:
            continue

    return None


async def _maybe_call(obj: Any, name: str) -> Optional[Any]:
    """Best-effort call obj.<name>(), supporting sync/async. Never raises."""
    try:
        fn = getattr(obj, name, None)
        if not callable(fn):
            return None
        out = fn()
        if hasattr(out, "__await__"):
            out = await out
        return out
    except Exception:
        return None


# ---------------------------------------------------------------------------
# Schema helpers (lazy) — FIX v8.9.0: multi-path imports throughout
# ---------------------------------------------------------------------------

def _canonicalize_sheet_name(sheet: str) -> str:
    s = (sheet or "").strip()
    if not s:
        return s

    # FIX v8.9.0: multi-path fallback for page_catalog
    for _pcat_path in ("core.sheets.page_catalog", "core.page_catalog", "page_catalog"):
        try:
            import importlib as _il
            _pcat = _il.import_module(_pcat_path)
            for fn_name in ("resolve_page", "canonicalize_page", "normalize_page_name",
                            "canonicalize_page_name"):
                fn = getattr(_pcat, fn_name, None)
                if callable(fn):
                    try:
                        out = fn(s)
                        if isinstance(out, str) and out.strip():
                            return out.strip()
                    except Exception:
                        continue
            aliases = getattr(_pcat, "PAGE_ALIASES", None) or getattr(_pcat, "ALIASES", None)
            if isinstance(aliases, dict):
                hit = aliases.get(s) or aliases.get(s.lower())
                if isinstance(hit, str) and hit.strip():
                    return hit.strip()
            canonical = getattr(_pcat, "CANONICAL_PAGES", None)
            if isinstance(canonical, (list, tuple, set)):
                lmap = {str(x).strip().lower(): str(x).strip() for x in canonical if x}
                if s.lower() in lmap:
                    return lmap[s.lower()]
            break  # found the module but no resolver matched — stop searching
        except Exception:
            continue

    return s.replace(" ", "_")


def _get_sheet_spec(sheet: str) -> Optional[Any]:
    # FIX v8.9.0: multi-path fallback for schema_registry
    for _sreg_path in ("core.sheets.schema_registry", "core.schema_registry", "schema_registry"):
        try:
            import importlib as _il
            _sreg = _il.import_module(_sreg_path)
            fn = getattr(_sreg, "get_sheet_spec", None)
            if callable(fn):
                return fn(sheet)
        except Exception:
            continue
    return None


def _schema_headers_keys(sheet: str) -> Tuple[List[str], List[str], str]:
    """Returns (headers, keys, source)."""
    spec = _get_sheet_spec(sheet)
    if spec and getattr(spec, "columns", None):
        headers = [str(c.header) for c in spec.columns]
        keys    = [str(c.key)    for c in spec.columns]
        return headers, keys, "schema_registry.get_sheet_spec"
    return [], [], "none"


def _get_all_sheet_names() -> List[str]:
    # FIX v8.9.0: multi-path fallback for schema_registry + page_catalog
    for _sreg_path in ("core.sheets.schema_registry", "core.schema_registry", "schema_registry"):
        try:
            import importlib as _il
            sr = _il.import_module(_sreg_path)
            for attr in ("SHEET_NAMES", "SHEETS", "CANONICAL_SHEETS", "CANONICAL_PAGES"):
                v = getattr(sr, attr, None)
                if isinstance(v, (list, tuple)) and v:
                    return [str(x) for x in v]
            fn = getattr(sr, "list_sheet_names", None)
            if callable(fn):
                out = fn()
                if isinstance(out, (list, tuple)):
                    return [str(x) for x in out]
        except Exception:
            continue

    for _pcat_path in ("core.sheets.page_catalog", "core.page_catalog", "page_catalog"):
        try:
            import importlib as _il2
            pc = _il2.import_module(_pcat_path)
            cp = getattr(pc, "CANONICAL_PAGES", None)
            if isinstance(cp, (list, tuple)) and cp:
                return [str(x) for x in cp]
        except Exception:
            continue

    return []


def _normalize_row_to_schema(sheet: str, row: Dict[str, Any]) -> Dict[str, Any]:
    """
    Best-effort normalization.
    If core.data_engine_v2.normalize_row_to_schema exists, use it.
    Otherwise project to schema keys with header-based fallback.
    """
    spec = _get_sheet_spec(sheet)
    if not spec or not getattr(spec, "columns", None):
        return row

    try:
        from core.data_engine_v2 import normalize_row_to_schema  # type: ignore

        return normalize_row_to_schema(sheet, row)
    except Exception:
        out: Dict[str, Any] = {}
        for c in spec.columns:
            k = str(c.key)
            h = str(c.header)
            if k in row:
                out[k] = row.get(k)
            elif h in row:
                out[k] = row.get(h)
            else:
                out[k] = None
        return out


def _rows_to_matrix(
    rows: List[Dict[str, Any]], keys: List[str]
) -> List[List[Any]]:
    return [[r.get(k) for k in keys] for r in rows]


def _coerce_rows_payload(
    payload: Any,
) -> Tuple[List[Dict[str, Any]], List[str], List[str]]:
    """Returns (rows, headers, keys) best-effort from engine payload."""
    if payload is None:
        return [], [], []

    if isinstance(payload, list):
        rows = [r for r in payload if isinstance(r, dict)]
        keys = list(rows[0].keys()) if rows else []
        return rows, keys[:], keys[:]

    if isinstance(payload, dict):
        headers = (
            payload.get("headers")
            or payload.get("columns")
            or payload.get("fields")
            or []
        )
        keys = (
            payload.get("keys")
            or payload.get("schema")
            or payload.get("fields")
            or []
        )
        rows = (
            payload.get("rows")
            or payload.get("data")
            or payload.get("items")
            or payload.get("records")
            or []
        )
        if not isinstance(rows, list):
            rows = []
        rows = [r for r in rows if isinstance(r, dict)]
        if not keys and rows:
            keys = list(rows[0].keys())
        if not headers and keys:
            headers = keys[:]
        return rows, [str(x) for x in headers], [str(x) for x in keys]

    try:
        if hasattr(payload, "model_dump"):
            return _coerce_rows_payload(payload.model_dump(mode="python"))  # type: ignore
        if hasattr(payload, "dict"):
            return _coerce_rows_payload(payload.dict())  # type: ignore
    except Exception:
        pass

    return [], [], []


async def _engine_sheet_rows(
    engine: Any,
    *,
    sheet: str,
    limit: int,
    offset: int,
    mode: str,
    body: Dict[str, Any],
) -> Any:
    """
    Best-effort call into engine to fetch sheet rows.
    Supports multiple possible method names/signatures without hard failing.
    """
    candidates = [
        ("get_sheet_rows",   dict(sheet=sheet, limit=limit, offset=offset, mode=mode, body=body)),
        ("get_sheet_rows",   dict(sheet=sheet, limit=limit, offset=offset, mode=mode)),
        ("sheet_rows",       dict(sheet=sheet, limit=limit, offset=offset, mode=mode, body=body)),
        ("sheet_rows",       dict(sheet=sheet, limit=limit, offset=offset, mode=mode)),
        ("build_sheet_rows", dict(sheet=sheet, limit=limit, offset=offset, mode=mode, body=body)),
        ("build_sheet_rows", dict(sheet=sheet, limit=limit, offset=offset, mode=mode)),
    ]

    last_err: Optional[Exception] = None

    for name, kwargs in candidates:
        fn = getattr(engine, name, None)
        if not callable(fn):
            continue
        try:
            out = fn(**kwargs)
            if hasattr(out, "__await__"):
                out = await out
            return out
        except TypeError as e:
            last_err = e
            try:
                out = fn(sheet, limit=limit, offset=offset, mode=mode)
                if hasattr(out, "__await__"):
                    out = await out
                return out
            except Exception as e2:
                last_err = e2
                continue
        except Exception as e:
            last_err = e
            continue

    if last_err is not None:
        raise last_err
    raise RuntimeError("Engine has no supported sheet-rows method")


# ---------------------------------------------------------------------------
# Remove base POST /sheet-rows; replace with schema-driven handler
# ---------------------------------------------------------------------------
def _remove_router_route(path: str, method: str) -> int:
    removed = 0
    try:
        new_routes = []
        for r in list(getattr(router, "routes", [])):
            try:
                rp = getattr(r, "path", None)
                rm = getattr(r, "methods", None) or set()
                if rp == path and method.upper() in set(rm):
                    removed += 1
                    continue
            except Exception:
                pass
            new_routes.append(r)
        router.routes = new_routes  # type: ignore
    except Exception:
        return removed
    return removed


_removed = _remove_router_route("/sheet-rows", "POST")
if _removed:
    logger.info(
        "Analysis: removed %s existing POST /sheet-rows route(s) "
        "to replace with schema-driven handler.",
        _removed,
    )


@router.post("/sheet-rows")
async def analysis_sheet_rows_schema_driven(
    request: Request,
    body: Dict[str, Any] = Body(...),
    mode: str = Query(default="", description="Optional mode hint for engine/provider"),
    include_matrix: Optional[bool] = Query(
        default=None, description="Return rows_matrix for legacy clients"
    ),
    token: Optional[str] = Query(default=None, description="Auth token (query only if allowed)"),
    x_app_token: Optional[str] = Header(default=None, alias="X-APP-TOKEN"),
    authorization: Optional[str] = Header(default=None, alias="Authorization"),
    x_request_id: Optional[str] = Header(default=None, alias="X-Request-ID"),
) -> Dict[str, Any]:
    """
    Schema-driven replacement for POST /v1/analysis/sheet-rows.

    Guarantees:
    - headers/keys/order match schema_registry for the requested sheet.
    - Special sheets fixed: Insights_Analysis, Top_10_Investments, Data_Dictionary.
    - Data_Dictionary is generated from schema_registry (no engine dependency).
    """
    _require_auth_or_401(
        token_query=token, x_app_token=x_app_token, authorization=authorization
    )
    request_id = x_request_id or (getattr(request.state, "request_id", None) or "")

    raw_sheet = str(
        body.get("sheet") or body.get("page") or body.get("name") or ""
    ).strip()
    if not raw_sheet:
        raise HTTPException(status_code=422, detail="Missing required field: sheet")

    sheet = _canonicalize_sheet_name(raw_sheet)

    limit  = max(1, min(5000, int(body.get("limit")  or 2000)))
    offset = max(0,           int(body.get("offset") or 0))

    include_matrix_final: bool
    if isinstance(include_matrix, bool):
        include_matrix_final = include_matrix
    elif isinstance(body.get("include_matrix"), (str, bool)):
        include_matrix_final = str(body.get("include_matrix", "")).strip().lower() in {
            "1", "true", "yes", "y", "on"
        }
    else:
        include_matrix_final = True

    schema_headers, schema_keys, schema_source = _schema_headers_keys(sheet)

    # ----------------------------------------------------------------
    # Data_Dictionary: generated from schema_registry (no engine needed)
    # ----------------------------------------------------------------
    if sheet == "Data_Dictionary":
        out_headers = schema_headers[:] if schema_headers else [
            "Sheet", "Group", "Header", "Key", "DType", "Format", "Required", "Source", "Notes"
        ]
        out_keys = schema_keys[:] if schema_keys else out_headers[:]

        rows: List[Dict[str, Any]] = []
        sheet_names = _get_all_sheet_names()
        if not sheet_names:
            sheet_names = [
                "Market_Leaders", "Global_Markets", "Commodities_FX", "Mutual_Funds",
                "My_Portfolio", "Insights_Analysis", "Top_10_Investments", "Data_Dictionary",
            ]

        for sn in sheet_names:
            spec = _get_sheet_spec(sn)
            cols = getattr(spec, "columns", None) if spec else None
            if not cols:
                continue

            for c in cols:
                group  = getattr(c, "group",  None) or getattr(spec, "group",  None) or ""
                source = getattr(c, "source", None) or getattr(spec, "source", None) or ""
                notes  = getattr(c, "notes",  None) or getattr(spec, "notes",  None) or ""
                dtype  = getattr(c, "dtype",  None) or getattr(c, "type",   None) or ""
                # FIX v8.9.0: canonical attr is 'fmt'; 'format' is legacy alias
                fmt    = getattr(c, "fmt",    None) or getattr(c, "format", None) or ""
                req    = getattr(c, "required", None)
                required = bool(req) if req is not None else False

                base = {
                    "Sheet":    sn,
                    "Group":    str(group)  if group   is not None else "",
                    "Header":   str(getattr(c, "header", "")),
                    "Key":      str(getattr(c, "key",    "")),
                    "DType":    str(dtype)  if dtype    is not None else "",
                    "Format":   str(fmt)    if fmt      is not None else "",  # display header
                    "Required": required,
                    "Source":   str(source) if source   is not None else "",
                    "Notes":    str(notes)  if notes    is not None else "",
                }

                row: Dict[str, Any] = {}
                for k in out_keys:
                    lk = k.strip().lower().replace(" ", "_")
                    if k in base:
                        row[k] = base[k]
                    elif lk in ("sheet", "page"):
                        row[k] = base["Sheet"]
                    elif lk == "group":
                        row[k] = base["Group"]
                    elif lk in ("header", "column"):
                        row[k] = base["Header"]
                    elif lk == "key":
                        row[k] = base["Key"]
                    elif lk in ("dtype", "type"):
                        row[k] = base["DType"]
                    elif lk in ("fmt", "format"):          # FIX v8.9.0: fmt first
                        row[k] = base["Format"]
                    elif lk in ("required", "is_required", "req"):
                        row[k] = base["Required"]
                    elif lk == "source":
                        row[k] = base["Source"]
                    elif lk in ("notes", "note", "description"):
                        row[k] = base["Notes"]
                    else:
                        row[k] = None

                rows.append(row)

        rows = rows[offset : offset + limit]

        return {
            "status":      "success",
            "page":        "Data_Dictionary",
            "sheet":       "Data_Dictionary",
            "headers":     out_headers,
            "keys":        out_keys,
            "rows":        rows,
            "rows_matrix": _rows_to_matrix(rows, out_keys) if include_matrix_final else None,
            "version":     ANALYSIS_VERSION,
            "request_id":  request_id or None,
            "meta": {
                "schema_source": schema_source,
                "generated":     True,
                "rows":          len(rows),
                "limit":         limit,
                "offset":        offset,
            },
        }

    # ----------------------------------------------------------------
    # All other sheets: engine + schema normalization + projection
    # ----------------------------------------------------------------
    engine = await _get_engine(request)
    if not engine:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Data engine unavailable",
        )

    try:
        payload = await _engine_sheet_rows(
            engine, sheet=sheet, limit=limit, offset=offset, mode=mode or "", body=body
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Engine sheet-rows failed: {e}")

    rows_raw, eng_headers, eng_keys = _coerce_rows_payload(payload)

    out_headers = schema_headers[:] if schema_headers else (eng_headers[:] if eng_headers else [])
    out_keys    = schema_keys[:]    if schema_keys    else (eng_keys[:]    if eng_keys    else [])

    if not out_keys and rows_raw:
        out_keys = list(rows_raw[0].keys())
    if not out_headers and out_keys:
        out_headers = out_keys[:]

    # Normalize to schema keys, then project strictly (drop extras)
    norm_rows: List[Dict[str, Any]] = []
    if schema_keys:
        for r in rows_raw:
            try:
                nr = _normalize_row_to_schema(sheet, r)
            except Exception:
                nr = r
            norm_rows.append({k: nr.get(k) for k in schema_keys})
    else:
        norm_rows = [dict(r) for r in rows_raw]

    return {
        "status":      "success",
        "page":        sheet,
        "sheet":       sheet,
        "headers":     out_headers,
        "keys":        out_keys,
        "rows":        norm_rows,
        "rows_matrix": (
            _rows_to_matrix(norm_rows, out_keys)
            if (include_matrix_final and out_keys)
            else None
        ),
        "version":    ANALYSIS_VERSION,
        "request_id": request_id or None,
        "meta": {
            "schema_source":          schema_source,
            "schema_cols":            len(schema_headers) if schema_headers else 0,
            "engine_cols":            len(eng_headers)    if eng_headers    else 0,
            "returned_cols":          len(out_headers)    if out_headers    else 0,
            "rows":                   len(norm_rows),
            "limit":                  limit,
            "offset":                 offset,
            "mode":                   mode,
            "sheet_rows_schema_driven": True,
        },
    }


# ---------------------------------------------------------------------------
# Health endpoint
# ---------------------------------------------------------------------------
@router.get("/health")
async def analysis_health(
    request: Request,
    token: Optional[str] = Query(default=None, description="Auth token (query only if allowed)"),
    x_app_token: Optional[str] = Header(default=None, alias="X-APP-TOKEN"),
    authorization: Optional[str] = Header(default=None, alias="Authorization"),
) -> Dict[str, Any]:
    """Lightweight health endpoint. Does not perform heavy computations."""
    _require_auth_or_401(
        token_query=token, x_app_token=x_app_token, authorization=authorization
    )

    engine = await _get_engine(request)

    engine_health: Optional[Dict[str, Any]] = None
    engine_stats:  Optional[Dict[str, Any]] = None

    if engine is not None:
        for attr in ("health", "health_check", "get_health"):
            r = await _maybe_call(engine, attr)
            if isinstance(r, dict):
                engine_health = r
                break
        for attr in ("get_stats", "stats", "metrics"):
            r = await _maybe_call(engine, attr)
            if isinstance(r, dict):
                engine_stats = r
                break

    settings = None
    try:
        settings = get_settings_cached()
    except Exception:
        settings = None

    # FIX v8.9.0: multi-path fallback for page_catalog in health()
    canonical_pages = None
    forbidden_pages = None
    for _pcat_path in ("core.sheets.page_catalog", "core.page_catalog", "page_catalog"):
        try:
            import importlib as _il
            _pcat = _il.import_module(_pcat_path)
            _cp = getattr(_pcat, "CANONICAL_PAGES", None)
            _fp = getattr(_pcat, "FORBIDDEN_PAGES", None)
            if _cp is not None:
                canonical_pages = list(_cp)
                forbidden_pages = list(_fp) if _fp is not None else []
                break
        except Exception:
            continue

    schema_version   = None
    schema_available = False
    try:
        schema_version = getattr(settings, "schema_version", None) if settings else None
        schema_available = bool(
            getattr(settings, "schema_enabled",      True)
            and getattr(settings, "schema_headers_always", True)
        ) if settings else False
    except Exception:
        pass

    return {
        "status":                   "ok" if engine else "degraded",
        "version":                  ANALYSIS_VERSION,
        "engine_available":         bool(engine),
        "engine_type":              _safe_engine_type(engine) if engine else "none",
        "engine_health":            engine_health,
        "engine_stats":             engine_stats,
        "schema_available":         bool(schema_available),
        "schema_version":           schema_version or "unknown",
        "schema_headers_always":    bool(getattr(settings, "schema_headers_always", True)) if settings else True,
        "computations_enabled":     bool(getattr(settings, "computations_enabled",  True)) if settings else True,
        "forecasting_enabled":      bool(getattr(settings, "forecasting_enabled",   True)) if settings else True,
        "scoring_enabled":          bool(getattr(settings, "scoring_enabled",       True)) if settings else True,
        "canonical_pages":          canonical_pages,
        "forbidden_pages":          forbidden_pages,
        "port":                     _safe_env_port(),
        "request_id":               getattr(request.state, "request_id", None),
        "require_auth":             _safe_bool_env("REQUIRE_AUTH", True),
        "sheet_rows_schema_driven": True,
        "replaced_base_sheet_rows_routes": _removed,
    }


# ---------------------------------------------------------------------------
# Prometheus metrics endpoint
# ---------------------------------------------------------------------------
@router.get("/metrics")
async def analysis_metrics() -> Response:
    """Prometheus metrics if available; otherwise 503."""
    if not _PROMETHEUS_AVAILABLE or generate_latest is None:
        return Response(
            content="Metrics not available", media_type="text/plain", status_code=503
        )
    return Response(content=generate_latest(), media_type=CONTENT_TYPE_LATEST)


__all__ = ["router"]
