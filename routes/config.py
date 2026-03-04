#!/usr/bin/env python3
"""
routes/config.py
================================================================================
TFB Config + Schema Routes — v5.6.0 (PROMETHEUS-SAFE / RENDER-SAFE / SCHEMA-AWARE)
================================================================================

Why this revision (your PowerShell evidence):
- ✅ FIX: Data_Dictionary was being treated like a quote sheet (80 cols) instead of 9.
- ✅ ADD: /v1/schema/data-dictionary endpoint lives here (as your system uses it):
      GET /v1/schema/data-dictionary?format=json&include_meta_sheet=true
  This endpoint now ALWAYS returns Data_Dictionary rows generated from
  core/sheets/data_dictionary.py (authoritative) and never touches engine quote
  builders.
- ✅ SAFE: No Prometheus metric creation here (prevents duplicate-timeseries crash)
- ✅ SAFE: No network calls at import time.

Existing endpoints (kept):
- GET  /v1/config/health
- GET  /v1/config/settings
- POST /v1/config/reload
- GET  /v1/config/versions

New endpoints (added, aligned):
- GET  /v1/schema/health
- GET  /v1/schema/pages
- GET  /v1/schema/data-dictionary
- GET  /v1/schema/sheet-spec?sheet=<name>

Auth policy:
- Uses core.config.is_open_mode/auth_ok when available.
- Supports:
    X-APP-TOKEN, X-API-KEY, Authorization: Bearer <token>
- Query token is allowed ONLY if settings.allow_query_token=True (or env ALLOW_QUERY_TOKEN=1)

Notes:
- This file intentionally contains BOTH routers:
    /v1/config/*
    /v1/schema/*
  because your deployment already calls /v1/schema/data-dictionary and we must
  guarantee it exists and is aligned.
"""

from __future__ import annotations

import logging
import os
import uuid
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, Query, Request
from fastapi.responses import JSONResponse

# Best JSON response (orjson if available)
try:
    from fastapi.responses import ORJSONResponse as BestJSONResponse  # type: ignore
except Exception:
    BestJSONResponse = JSONResponse  # type: ignore

logger = logging.getLogger("routes.config")

ROUTER_VERSION = "5.6.0"

# -----------------------------
# Routers
# -----------------------------
config_router = APIRouter(prefix="/v1/config", tags=["config"])
schema_router = APIRouter(prefix="/v1/schema", tags=["schema"])


# -----------------------------
# Internal helpers
# -----------------------------
def _now_utc() -> str:
    return datetime.now(timezone.utc).isoformat()


def _get_request_id(request: Request) -> str:
    return request.headers.get("X-Request-ID") or str(uuid.uuid4())[:8]


def _safe_bool_env(name: str, default: bool = False) -> bool:
    try:
        v = (os.getenv(name, str(default)) or "").strip().lower()
        return v in {"1", "true", "yes", "y", "on", "t"}
    except Exception:
        return default


def _get_settings_cached(force_reload: bool = False) -> Any:
    try:
        from core.config import get_settings_cached  # type: ignore

        if callable(get_settings_cached):
            return get_settings_cached(force_reload=bool(force_reload))
    except Exception:
        pass
    return None


def _allow_query_token(settings: Any) -> bool:
    try:
        if settings is not None:
            return bool(getattr(settings, "allow_query_token", False))
    except Exception:
        pass
    return _safe_bool_env("ALLOW_QUERY_TOKEN", False)


def _extract_token_from_request(request: Request, *, query_token: Optional[str] = None) -> Dict[str, Optional[str]]:
    """
    Returns dict with:
      token: token candidate (X-APP-TOKEN/X-API-KEY or query if allowed)
      authorization: raw Authorization header if present
    """
    token = (
        request.headers.get("X-APP-TOKEN")
        or request.headers.get("X-App-Token")
        or request.headers.get("X-API-KEY")
        or request.headers.get("X-Api-Key")
    )
    authorization = request.headers.get("Authorization")

    # Prefer bearer token if present
    if authorization and authorization.strip().lower().startswith("bearer "):
        token = authorization.strip().split(" ", 1)[1].strip()

    # Query token only if allowed
    settings = _get_settings_cached(force_reload=False)
    if (not token) and query_token and _allow_query_token(settings):
        token = (query_token or "").strip()

    return {"token": token, "authorization": authorization}


def _auth_ok(request: Request, *, query_token: Optional[str] = None) -> bool:
    """
    Uses core.config.auth_ok as the single auth gate when available.
    Open-mode bypass supported via core.config.is_open_mode.
    """
    try:
        from core.config import is_open_mode, auth_ok  # type: ignore

        if callable(is_open_mode) and is_open_mode():
            return True

        info = _extract_token_from_request(request, query_token=query_token)
        token = info.get("token")
        authz = info.get("authorization")

        if callable(auth_ok):
            return bool(auth_ok(token=token, authorization=authz, headers=dict(request.headers)))

    except Exception:
        return False

    # safest default
    return False


def _error(status_code: int, request_id: str, message: str) -> BestJSONResponse:
    return BestJSONResponse(
        status_code=status_code,
        content={
            "status": "error",
            "error": message,
            "request_id": request_id,
            "timestamp_utc": _now_utc(),
            "version": ROUTER_VERSION,
        },
    )


# =============================================================================
# /v1/config/*
# =============================================================================
@config_router.get("/health", include_in_schema=False)
async def config_health() -> Any:
    """
    Lightweight health for config subsystem (safe).
    """
    try:
        from core.config import config_health_check  # type: ignore

        if callable(config_health_check):
            data = config_health_check()
        else:
            data = {"status": "unknown", "checks": {}, "warnings": [], "errors": []}
    except Exception as e:
        data = {"status": "unhealthy", "checks": {}, "warnings": [], "errors": [f"{type(e).__name__}: {e}"]}

    data["module"] = "routes.config"
    data["version"] = ROUTER_VERSION
    data["timestamp_utc"] = _now_utc()
    return data


@config_router.get("/settings")
async def get_masked_settings(
    request: Request,
    force_reload: bool = Query(False, description="Force reload settings cache (no distributed calls unless enabled)"),
    token: Optional[str] = Query(None, description="Query token (only if allow_query_token enabled)"),
) -> BestJSONResponse:
    request_id = _get_request_id(request)
    if not _auth_ok(request, query_token=token):
        return _error(401, request_id, "unauthorized")

    try:
        from core.config import mask_settings  # type: ignore

        s = _get_settings_cached(force_reload=bool(force_reload))
        payload = mask_settings(s) if callable(mask_settings) else {}
        return BestJSONResponse(
            status_code=200,
            content={
                "status": "ok",
                "request_id": request_id,
                "timestamp_utc": _now_utc(),
                "settings": payload,
                "version": ROUTER_VERSION,
            },
        )
    except Exception as e:
        logger.warning("get_masked_settings failed: %s", e)
        return _error(500, request_id, f"{type(e).__name__}: {e}")


@config_router.post("/reload")
async def reload_config(
    request: Request,
    token: Optional[str] = Query(None, description="Query token (only if allow_query_token enabled)"),
    author: str = "",
    comment: str = "",
) -> BestJSONResponse:
    """
    Reload settings (merges distributed overrides ONLY if enabled in core.config).
    """
    request_id = _get_request_id(request)
    if not _auth_ok(request, query_token=token):
        return _error(401, request_id, "unauthorized")

    try:
        from core.config import reload_settings, mask_settings  # type: ignore

        if not callable(reload_settings):
            return _error(500, request_id, "reload_settings unavailable")

        s = reload_settings()
        masked = mask_settings(s) if callable(mask_settings) else {}

        # Best-effort validation if available
        errors: List[str] = []
        warnings: List[str] = []
        try:
            if hasattr(s, "validate") and callable(s.validate):
                errors, warnings = s.validate()
        except Exception:
            pass

        return BestJSONResponse(
            status_code=200,
            content={
                "status": "ok",
                "request_id": request_id,
                "timestamp_utc": _now_utc(),
                "version": ROUTER_VERSION,
                "meta": {"author": author, "comment": comment},
                "validation": {"errors": errors, "warnings": warnings},
                "settings": masked,
            },
        )
    except Exception as e:
        logger.warning("reload_config failed: %s", e)
        return _error(500, request_id, f"{type(e).__name__}: {e}")


@config_router.get("/versions", include_in_schema=False)
async def config_versions(
    request: Request,
    token: Optional[str] = Query(None, description="Query token (only if allow_query_token enabled)"),
    limit: int = Query(10, ge=1, le=50),
) -> BestJSONResponse:
    """
    Exposes recent config version history if core.config version manager is used.
    """
    request_id = _get_request_id(request)
    if not _auth_ok(request, query_token=token):
        return _error(401, request_id, "unauthorized")

    try:
        from core.config import get_version_manager  # type: ignore

        items: List[Dict[str, Any]] = []
        if callable(get_version_manager):
            vm = get_version_manager()
            if vm and hasattr(vm, "history") and callable(vm.history):
                hist = vm.history(limit=int(limit))
                for v in hist:
                    try:
                        items.append(v.to_dict() if hasattr(v, "to_dict") else dict(v))  # type: ignore[arg-type]
                    except Exception:
                        items.append({"value": str(v)})

        return BestJSONResponse(
            status_code=200,
            content={
                "status": "ok",
                "request_id": request_id,
                "timestamp_utc": _now_utc(),
                "version": ROUTER_VERSION,
                "count": len(items),
                "items": items,
            },
        )
    except Exception as e:
        logger.warning("config_versions failed: %s", e)
        return _error(500, request_id, f"{type(e).__name__}: {e}")


# =============================================================================
# /v1/schema/*
# =============================================================================
@schema_router.get("/health", include_in_schema=False)
async def schema_health() -> BestJSONResponse:
    """
    Lightweight schema health (no engine).
    """
    ok = True
    err = None

    schema_version = None
    sheets: List[str] = []
    try:
        from core.sheets.schema_registry import SCHEMA_VERSION, list_sheets  # type: ignore

        schema_version = SCHEMA_VERSION
        sheets = list_sheets()
    except Exception as e:
        ok = False
        err = f"{type(e).__name__}: {e}"

    return BestJSONResponse(
        status_code=200 if ok else 503,
        content={
            "status": "ok" if ok else "degraded",
            "version": ROUTER_VERSION,
            "schema_version": schema_version or "unknown",
            "sheets_count": len(sheets),
            "sheets": sheets,
            "error": err,
            "timestamp_utc": _now_utc(),
        },
    )


@schema_router.get("/pages")
async def schema_pages(
    request: Request,
    token: Optional[str] = Query(None, description="Query token (only if allow_query_token enabled)"),
) -> BestJSONResponse:
    """
    Returns canonical pages and forbidden pages (no engine).
    """
    request_id = _get_request_id(request)
    if not _auth_ok(request, query_token=token):
        return _error(401, request_id, "unauthorized")

    try:
        from core.sheets.page_catalog import CANONICAL_PAGES, FORBIDDEN_PAGES  # type: ignore

        return BestJSONResponse(
            status_code=200,
            content={
                "status": "ok",
                "request_id": request_id,
                "timestamp_utc": _now_utc(),
                "version": ROUTER_VERSION,
                "canonical_pages": list(CANONICAL_PAGES),
                "forbidden_pages": sorted(list(FORBIDDEN_PAGES)),
            },
        )
    except Exception as e:
        return _error(500, request_id, f"{type(e).__name__}: {e}")


@schema_router.get("/sheet-spec")
async def schema_sheet_spec(
    request: Request,
    sheet: str = Query(..., description="Canonical sheet name (or alias accepted by page_catalog)"),
    token: Optional[str] = Query(None, description="Query token (only if allow_query_token enabled)"),
) -> BestJSONResponse:
    """
    Returns a lightweight sheet spec:
      {sheet, kind, columns:[{group,header,key,dtype,fmt,required,source,notes}], criteria_fields:[...]}
    """
    request_id = _get_request_id(request)
    if not _auth_ok(request, query_token=token):
        return _error(401, request_id, "unauthorized")

    try:
        from core.sheets.page_catalog import normalize_page_name  # type: ignore
        from core.sheets.schema_registry import get_sheet_spec  # type: ignore

        canonical = normalize_page_name(sheet, allow_output_pages=True)
        spec = get_sheet_spec(canonical)

        cols = []
        for c in getattr(spec, "columns", []) or []:
            cols.append(
                {
                    "group": getattr(c, "group", ""),
                    "header": getattr(c, "header", ""),
                    "key": getattr(c, "key", ""),
                    "dtype": getattr(c, "dtype", "str"),
                    "fmt": getattr(c, "fmt", "text"),
                    "required": bool(getattr(c, "required", False)),
                    "source": getattr(c, "source", ""),
                    "notes": getattr(c, "notes", ""),
                }
            )

        cfields = []
        for cf in getattr(spec, "criteria_fields", []) or []:
            cfields.append(
                {
                    "key": getattr(cf, "key", ""),
                    "label": getattr(cf, "label", ""),
                    "dtype": getattr(cf, "dtype", "str"),
                    "default": getattr(cf, "default", ""),
                    "notes": getattr(cf, "notes", ""),
                }
            )

        return BestJSONResponse(
            status_code=200,
            content={
                "status": "ok",
                "request_id": request_id,
                "timestamp_utc": _now_utc(),
                "version": ROUTER_VERSION,
                "sheet": canonical,
                "kind": getattr(spec, "kind", ""),
                "columns": cols,
                "criteria_fields": cfields,
                "counts": {"columns": len(cols), "criteria_fields": len(cfields)},
            },
        )
    except ValueError as e:
        return _error(400, request_id, str(e))
    except Exception as e:
        return _error(500, request_id, f"{type(e).__name__}: {e}")


@schema_router.get("/data-dictionary")
async def schema_data_dictionary(
    request: Request,
    format: str = Query("json", description="json or values"),
    include_meta_sheet: bool = Query(True, description="Include Data_Dictionary itself in the output"),
    token: Optional[str] = Query(None, description="Query token (only if allow_query_token enabled)"),
) -> BestJSONResponse:
    """
    Authoritative Data Dictionary endpoint.

    - format=json:
        returns rows as list[dict] with keys matching Data_Dictionary schema keys.
    - format=values:
        returns a 2D array suitable for writing into Google Sheets.

    IMPORTANT:
    - This endpoint NEVER calls the quote engine.
    - It reads schema_registry and uses core/sheets/data_dictionary.py.
    """
    request_id = _get_request_id(request)
    if not _auth_ok(request, query_token=token):
        return _error(401, request_id, "unauthorized")

    fmt = (format or "json").strip().lower()
    if fmt not in {"json", "values"}:
        return _error(400, request_id, "format must be 'json' or 'values'")

    try:
        from core.sheets.schema_registry import get_sheet_headers  # type: ignore
        from core.sheets.data_dictionary import (  # type: ignore
            DATA_DICTIONARY_VERSION,
            build_data_dictionary_rows,
            build_data_dictionary_values,
            validate_data_dictionary_output,
        )

        headers = get_sheet_headers("Data_Dictionary")

        if fmt == "values":
            values = build_data_dictionary_values(include_header_row=True, include_meta_sheet=bool(include_meta_sheet))
            return BestJSONResponse(
                status_code=200,
                content={
                    "status": "ok",
                    "request_id": request_id,
                    "timestamp_utc": _now_utc(),
                    "version": ROUTER_VERSION,
                    "data_dictionary_version": DATA_DICTIONARY_VERSION,
                    "format": "values",
                    "headers": headers,
                    "values": values,
                    "counts": {"rows": max(0, len(values) - 1), "cols": len(headers)},
                },
            )

        # json
        rows = build_data_dictionary_rows(include_meta_sheet=bool(include_meta_sheet))
        # strict sanity check (fast)
        validate_data_dictionary_output(rows, enforce_unique_sheet_key=True)

        return BestJSONResponse(
            status_code=200,
            content={
                "status": "ok",
                "request_id": request_id,
                "timestamp_utc": _now_utc(),
                "version": ROUTER_VERSION,
                "data_dictionary_version": DATA_DICTIONARY_VERSION,
                "format": "json",
                "include_meta_sheet": bool(include_meta_sheet),
                "headers": headers,
                "rows": rows,
                "counts": {"rows": len(rows), "cols": len(headers)},
            },
        )
    except Exception as e:
        logger.warning("schema_data_dictionary failed: %s", e)
        return _error(500, request_id, f"{type(e).__name__}: {e}")


# ---------------------------------------------------------------------------
# Dynamic loader hook
# ---------------------------------------------------------------------------
def mount(app: Any) -> None:
    app.include_router(config_router)
    app.include_router(schema_router)


# ---------------------------------------------------------------------------
# Backward-compatible exports (some modules may import from routes.config)
# ---------------------------------------------------------------------------
def get_settings() -> Any:
    return _get_settings_cached(force_reload=False)


def allowed_tokens() -> List[str]:
    try:
        from core.config import allowed_tokens as _allowed  # type: ignore

        return list(_allowed()) if callable(_allowed) else []
    except Exception:
        return []


def is_open_mode() -> bool:
    try:
        from core.config import is_open_mode as _iom  # type: ignore

        return bool(_iom()) if callable(_iom) else False
    except Exception:
        # safest default
        req = _safe_bool_env("REQUIRE_AUTH", True)
        return not req


def auth_ok_request(
    *,
    x_app_token: Optional[str] = None,
    authorization: Optional[str] = None,
    query_token: Optional[str] = None,
) -> bool:
    try:
        from core.config import auth_ok as _auth_ok_fn  # type: ignore

        if not callable(_auth_ok_fn):
            return False

        token = (x_app_token or query_token) if (x_app_token or query_token) else None
        headers = {}
        if x_app_token:
            headers["X-APP-TOKEN"] = x_app_token
        if authorization:
            headers["Authorization"] = authorization

        return bool(_auth_ok_fn(token=token, authorization=authorization, headers=headers))
    except Exception:
        return False


__all__ = [
    "ROUTER_VERSION",
    "config_router",
    "schema_router",
    "mount",
    # compat exports
    "get_settings",
    "allowed_tokens",
    "is_open_mode",
    "auth_ok_request",
]
