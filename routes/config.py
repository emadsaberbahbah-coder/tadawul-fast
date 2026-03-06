#!/usr/bin/env python3
"""
routes/config.py
================================================================================
TFB Config + Schema Routes — v5.6.1 (PROMETHEUS-SAFE / RENDER-SAFE / SCHEMA-HARDENED)
================================================================================

What this revision fixes
- ✅ FIX: /v1/schema/sheet-spec is hardened to avoid 500s caused by:
      - normalize_page_name signature mismatch
      - dict-vs-object schema spec structures
      - missing optional criteria_fields
      - missing helper functions in page_catalog / schema_registry
- ✅ FIX: /v1/schema/data-dictionary is guaranteed to use the authoritative
      builder in core/sheets/data_dictionary.py and never the quote engine
- ✅ FIX: Data_Dictionary output is normalized and validated before response
- ✅ SAFE: no Prometheus metric creation here
- ✅ SAFE: no network calls at import time
- ✅ SAFE: import fallbacks + defensive compatibility for older/newer core modules

Endpoints kept
- GET  /v1/config/health
- GET  /v1/config/settings
- POST /v1/config/reload
- GET  /v1/config/versions

Schema endpoints
- GET  /v1/schema/health
- GET  /v1/schema/pages
- GET  /v1/schema/data-dictionary
- GET  /v1/schema/sheet-spec?sheet=<name>

Auth policy
- Uses core.config.is_open_mode/auth_ok when available
- Supports:
    X-APP-TOKEN, X-API-KEY, Authorization: Bearer <token>
- Query token is allowed only if settings.allow_query_token=True
  or env ALLOW_QUERY_TOKEN=1
"""

from __future__ import annotations

import logging
import os
import uuid
from datetime import datetime, timezone
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence

from fastapi import APIRouter, Query, Request
from fastapi.responses import JSONResponse

try:
    from fastapi.responses import ORJSONResponse as BestJSONResponse  # type: ignore
except Exception:  # pragma: no cover
    BestJSONResponse = JSONResponse  # type: ignore

logger = logging.getLogger("routes.config")

ROUTER_VERSION = "5.6.1"

config_router = APIRouter(prefix="/v1/config", tags=["config"])
schema_router = APIRouter(prefix="/v1/schema", tags=["schema"])


# =============================================================================
# Generic helpers
# =============================================================================
def _now_utc() -> str:
    return datetime.now(timezone.utc).isoformat()


def _get_request_id(request: Request) -> str:
    try:
        rid = request.headers.get("X-Request-ID")
        return str(rid).strip() if rid else str(uuid.uuid4())[:8]
    except Exception:
        return str(uuid.uuid4())[:8]


def _safe_bool_env(name: str, default: bool = False) -> bool:
    try:
        value = (os.getenv(name, str(default)) or "").strip().lower()
        return value in {"1", "true", "yes", "y", "on", "t"}
    except Exception:
        return default


def _safe_int(value: Any, default: int = 0) -> int:
    try:
        return int(value)
    except Exception:
        return default


def _error(status_code: int, request_id: str, message: str) -> BestJSONResponse:
    return BestJSONResponse(
        status_code=status_code,
        content={
            "status": "error",
            "error": str(message),
            "request_id": request_id,
            "timestamp_utc": _now_utc(),
            "version": ROUTER_VERSION,
        },
    )


def _obj_get(obj: Any, name: str, default: Any = None) -> Any:
    if obj is None:
        return default
    if isinstance(obj, Mapping):
        return obj.get(name, default)
    return getattr(obj, name, default)


def _call_if_callable(fn: Any, *args: Any, **kwargs: Any) -> Any:
    if callable(fn):
        return fn(*args, **kwargs)
    return None


def _as_list(value: Any) -> List[Any]:
    if value is None:
        return []
    if isinstance(value, list):
        return value
    if isinstance(value, tuple):
        return list(value)
    if isinstance(value, set):
        return list(value)
    if isinstance(value, str):
        return [value]
    if isinstance(value, Iterable):
        try:
            return list(value)
        except Exception:
            return [value]
    return [value]


def _normalize_bool(value: Any, default: bool = False) -> bool:
    if isinstance(value, bool):
        return value
    if value is None:
        return default
    s = str(value).strip().lower()
    return s in {"1", "true", "yes", "y", "on", "t"}


# =============================================================================
# Auth/config helpers
# =============================================================================
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


def _extract_token_from_request(
    request: Request,
    *,
    query_token: Optional[str] = None,
) -> Dict[str, Optional[str]]:
    token = (
        request.headers.get("X-APP-TOKEN")
        or request.headers.get("X-App-Token")
        or request.headers.get("X-API-KEY")
        or request.headers.get("X-Api-Key")
    )
    authorization = request.headers.get("Authorization")

    if authorization and authorization.strip().lower().startswith("bearer "):
        token = authorization.strip().split(" ", 1)[1].strip()

    settings = _get_settings_cached(force_reload=False)
    if (not token) and query_token and _allow_query_token(settings):
        token = (query_token or "").strip()

    return {"token": token, "authorization": authorization}


def _auth_ok(request: Request, *, query_token: Optional[str] = None) -> bool:
    try:
        from core.config import auth_ok, is_open_mode  # type: ignore

        if callable(is_open_mode) and bool(is_open_mode()):
            return True

        info = _extract_token_from_request(request, query_token=query_token)
        token = info.get("token")
        authz = info.get("authorization")

        if callable(auth_ok):
            return bool(auth_ok(token=token, authorization=authz, headers=dict(request.headers)))
    except Exception:
        return False

    return False


# =============================================================================
# Schema/page helpers
# =============================================================================
def _page_catalog_module() -> Any:
    from core.sheets import page_catalog  # type: ignore

    return page_catalog


def _schema_registry_module() -> Any:
    from core.sheets import schema_registry  # type: ignore

    return schema_registry


def _normalize_sheet_name(raw_sheet: str) -> str:
    if not raw_sheet or not str(raw_sheet).strip():
        raise ValueError("sheet is required")

    page_catalog = _page_catalog_module()
    raw = str(raw_sheet).strip()

    normalize_fn = getattr(page_catalog, "normalize_page_name", None)
    if callable(normalize_fn):
        # Try most specific signature first, then degrade safely.
        for kwargs in (
            {"allow_output_pages": True},
            {"allow_special_pages": True},
            {},
        ):
            try:
                return str(normalize_fn(raw, **kwargs))
            except TypeError:
                continue
            except Exception:
                break

        try:
            return str(normalize_fn(raw))
        except Exception:
            pass

    # Fallbacks from common catalog patterns.
    canonical_pages = _as_list(getattr(page_catalog, "CANONICAL_PAGES", []))
    aliases = getattr(page_catalog, "PAGE_ALIASES", None) or getattr(page_catalog, "ALIASES", None) or {}

    if isinstance(aliases, Mapping):
        aliased = aliases.get(raw) or aliases.get(raw.lower()) or aliases.get(raw.upper())
        if aliased:
            return str(aliased)

    raw_lower = raw.lower()
    for page in canonical_pages:
        if str(page).lower() == raw_lower:
            return str(page)

    # Gentle fallback: preserve caller input.
    return raw


def _get_all_pages() -> Dict[str, List[str]]:
    page_catalog = _page_catalog_module()

    canonical_pages = [str(x) for x in _as_list(getattr(page_catalog, "CANONICAL_PAGES", []))]
    forbidden_pages = [str(x) for x in _as_list(getattr(page_catalog, "FORBIDDEN_PAGES", []))]

    if not canonical_pages:
        try:
            registry = _schema_registry_module()
            list_sheets_fn = getattr(registry, "list_sheets", None)
            canonical_pages = [str(x) for x in _as_list(_call_if_callable(list_sheets_fn))]
        except Exception:
            canonical_pages = []

    return {
        "canonical_pages": canonical_pages,
        "forbidden_pages": sorted(set(forbidden_pages)),
    }


def _get_sheet_spec_compat(canonical_sheet: str) -> Any:
    registry = _schema_registry_module()

    # Preferred exact helper.
    get_sheet_spec = getattr(registry, "get_sheet_spec", None)
    if callable(get_sheet_spec):
        return get_sheet_spec(canonical_sheet)

    # Fallback registry containers seen in many codebases.
    for attr_name in ("SHEET_SPECS", "SCHEMA_REGISTRY", "SHEETS", "REGISTRY"):
        container = getattr(registry, attr_name, None)
        if isinstance(container, Mapping) and canonical_sheet in container:
            return container[canonical_sheet]

    raise ValueError(f"unknown sheet: {canonical_sheet}")


def _normalize_column_item(item: Any) -> Dict[str, Any]:
    return {
        "group": _obj_get(item, "group", ""),
        "header": _obj_get(item, "header", ""),
        "key": _obj_get(item, "key", ""),
        "dtype": _obj_get(item, "dtype", "str"),
        "fmt": _obj_get(item, "fmt", "text"),
        "required": bool(_obj_get(item, "required", False)),
        "source": _obj_get(item, "source", ""),
        "notes": _obj_get(item, "notes", ""),
    }


def _normalize_criteria_item(item: Any) -> Dict[str, Any]:
    return {
        "key": _obj_get(item, "key", ""),
        "label": _obj_get(item, "label", ""),
        "dtype": _obj_get(item, "dtype", "str"),
        "default": _obj_get(item, "default", None),
        "notes": _obj_get(item, "notes", ""),
    }


def _extract_spec_columns(spec: Any) -> List[Dict[str, Any]]:
    columns = _obj_get(spec, "columns", None)

    if columns is None and isinstance(spec, Mapping):
        columns = spec.get("headers") or spec.get("cols")

    return [_normalize_column_item(x) for x in _as_list(columns)]


def _extract_spec_criteria_fields(spec: Any) -> List[Dict[str, Any]]:
    criteria_fields = _obj_get(spec, "criteria_fields", None)
    if criteria_fields is None and isinstance(spec, Mapping):
        criteria_fields = spec.get("criteria") or spec.get("advisor_fields")
    return [_normalize_criteria_item(x) for x in _as_list(criteria_fields)]


def _extract_spec_kind(spec: Any) -> str:
    kind = _obj_get(spec, "kind", None)
    if kind is None and isinstance(spec, Mapping):
        kind = spec.get("sheet_kind") or spec.get("type") or ""
    return str(kind or "")


def _get_sheet_headers_compat(sheet_name: str) -> List[str]:
    registry = _schema_registry_module()

    get_sheet_headers = getattr(registry, "get_sheet_headers", None)
    if callable(get_sheet_headers):
        headers = get_sheet_headers(sheet_name)
        return [str(x) for x in _as_list(headers)]

    spec = _get_sheet_spec_compat(sheet_name)
    cols = _extract_spec_columns(spec)
    if cols:
        return [str(c.get("header", "")) for c in cols]

    return []


# =============================================================================
# /v1/config/*
# =============================================================================
@config_router.get("/health", include_in_schema=False)
async def config_health() -> Any:
    try:
        from core.config import config_health_check  # type: ignore

        if callable(config_health_check):
            data = config_health_check()
        else:
            data = {"status": "unknown", "checks": {}, "warnings": [], "errors": []}
    except Exception as e:
        data = {
            "status": "unhealthy",
            "checks": {},
            "warnings": [],
            "errors": [f"{type(e).__name__}: {e}"],
        }

    data["module"] = "routes.config"
    data["version"] = ROUTER_VERSION
    data["timestamp_utc"] = _now_utc()
    return data


@config_router.get("/settings")
async def get_masked_settings(
    request: Request,
    force_reload: bool = Query(False, description="Force reload settings cache"),
    token: Optional[str] = Query(None, description="Query token if allow_query_token is enabled"),
) -> BestJSONResponse:
    request_id = _get_request_id(request)
    if not _auth_ok(request, query_token=token):
        return _error(401, request_id, "unauthorized")

    try:
        from core.config import mask_settings  # type: ignore

        settings = _get_settings_cached(force_reload=bool(force_reload))
        payload = mask_settings(settings) if callable(mask_settings) else {}
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
    token: Optional[str] = Query(None, description="Query token if allow_query_token is enabled"),
    author: str = "",
    comment: str = "",
) -> BestJSONResponse:
    request_id = _get_request_id(request)
    if not _auth_ok(request, query_token=token):
        return _error(401, request_id, "unauthorized")

    try:
        from core.config import mask_settings, reload_settings  # type: ignore

        if not callable(reload_settings):
            return _error(500, request_id, "reload_settings unavailable")

        settings = reload_settings()
        masked = mask_settings(settings) if callable(mask_settings) else {}

        errors: List[str] = []
        warnings: List[str] = []
        try:
            validate = getattr(settings, "validate", None)
            if callable(validate):
                result = validate()
                if isinstance(result, tuple) and len(result) >= 2:
                    errors = _as_list(result[0])
                    warnings = _as_list(result[1])
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
    token: Optional[str] = Query(None, description="Query token if allow_query_token is enabled"),
    limit: int = Query(10, ge=1, le=50),
) -> BestJSONResponse:
    request_id = _get_request_id(request)
    if not _auth_ok(request, query_token=token):
        return _error(401, request_id, "unauthorized")

    try:
        from core.config import get_version_manager  # type: ignore

        items: List[Dict[str, Any]] = []
        vm = _call_if_callable(get_version_manager)
        history = getattr(vm, "history", None)
        if callable(history):
            for item in _as_list(history(limit=int(limit))):
                try:
                    items.append(item.to_dict() if hasattr(item, "to_dict") else dict(item))  # type: ignore[arg-type]
                except Exception:
                    items.append({"value": str(item)})

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
    ok = True
    err = None
    schema_version = "unknown"
    sheets: List[str] = []

    try:
        registry = _schema_registry_module()
        schema_version = str(getattr(registry, "SCHEMA_VERSION", "unknown"))
        list_sheets = getattr(registry, "list_sheets", None)
        sheets = [str(x) for x in _as_list(_call_if_callable(list_sheets))]
        if not sheets:
            pages = _get_all_pages()
            sheets = pages["canonical_pages"]
    except Exception as e:
        ok = False
        err = f"{type(e).__name__}: {e}"

    return BestJSONResponse(
        status_code=200 if ok else 503,
        content={
            "status": "ok" if ok else "degraded",
            "version": ROUTER_VERSION,
            "schema_version": schema_version,
            "sheets_count": len(sheets),
            "sheets": sheets,
            "error": err,
            "timestamp_utc": _now_utc(),
        },
    )


@schema_router.get("/pages")
async def schema_pages(
    request: Request,
    token: Optional[str] = Query(None, description="Query token if allow_query_token is enabled"),
) -> BestJSONResponse:
    request_id = _get_request_id(request)
    if not _auth_ok(request, query_token=token):
        return _error(401, request_id, "unauthorized")

    try:
        pages = _get_all_pages()
        return BestJSONResponse(
            status_code=200,
            content={
                "status": "ok",
                "request_id": request_id,
                "timestamp_utc": _now_utc(),
                "version": ROUTER_VERSION,
                "canonical_pages": pages["canonical_pages"],
                "forbidden_pages": pages["forbidden_pages"],
            },
        )
    except Exception as e:
        logger.warning("schema_pages failed: %s", e)
        return _error(500, request_id, f"{type(e).__name__}: {e}")


@schema_router.get("/sheet-spec")
async def schema_sheet_spec(
    request: Request,
    sheet: str = Query(..., description="Canonical sheet name or accepted alias"),
    token: Optional[str] = Query(None, description="Query token if allow_query_token is enabled"),
) -> BestJSONResponse:
    """
    Returns a robust sheet spec response:
      {
        sheet,
        kind,
        columns: [{group,header,key,dtype,fmt,required,source,notes}],
        criteria_fields: [{key,label,dtype,default,notes}],
        counts: {columns, criteria_fields}
      }
    """
    request_id = _get_request_id(request)
    if not _auth_ok(request, query_token=token):
        return _error(401, request_id, "unauthorized")

    try:
        canonical = _normalize_sheet_name(sheet)
        spec = _get_sheet_spec_compat(canonical)

        cols = _extract_spec_columns(spec)
        if not cols:
            headers = _get_sheet_headers_compat(canonical)
            cols = [
                {
                    "group": "",
                    "header": header,
                    "key": "",
                    "dtype": "str",
                    "fmt": "text",
                    "required": False,
                    "source": "",
                    "notes": "",
                }
                for header in headers
            ]

        criteria_fields = _extract_spec_criteria_fields(spec)
        kind = _extract_spec_kind(spec)

        return BestJSONResponse(
            status_code=200,
            content={
                "status": "ok",
                "request_id": request_id,
                "timestamp_utc": _now_utc(),
                "version": ROUTER_VERSION,
                "sheet": canonical,
                "kind": kind,
                "columns": cols,
                "criteria_fields": criteria_fields,
                "counts": {
                    "columns": len(cols),
                    "criteria_fields": len(criteria_fields),
                },
            },
        )
    except ValueError as e:
        return _error(400, request_id, str(e))
    except Exception as e:
        logger.warning("schema_sheet_spec failed for sheet=%r: %s", sheet, e)
        return _error(500, request_id, f"{type(e).__name__}: {e}")


@schema_router.get("/data-dictionary")
async def schema_data_dictionary(
    request: Request,
    format: str = Query("json", description="json or values"),
    include_meta_sheet: bool = Query(True, description="Include Data_Dictionary meta sheet rows"),
    token: Optional[str] = Query(None, description="Query token if allow_query_token is enabled"),
) -> BestJSONResponse:
    """
    Authoritative Data Dictionary endpoint.

    - format=json:
        returns rows as list[dict]
    - format=values:
        returns 2D array suitable for Google Sheets writeback

    Important:
    - never calls the quote engine
    - always uses core/sheets/data_dictionary.py
    """
    request_id = _get_request_id(request)
    if not _auth_ok(request, query_token=token):
        return _error(401, request_id, "unauthorized")

    fmt = (format or "json").strip().lower()
    if fmt not in {"json", "values"}:
        return _error(400, request_id, "format must be 'json' or 'values'")

    try:
        from core.sheets.data_dictionary import (  # type: ignore
            DATA_DICTIONARY_VERSION,
            build_data_dictionary_rows,
            build_data_dictionary_values,
            validate_data_dictionary_output,
        )

        headers = _get_sheet_headers_compat("Data_Dictionary")

        if fmt == "values":
            values = build_data_dictionary_values(
                include_header_row=True,
                include_meta_sheet=bool(include_meta_sheet),
            )
            values = _as_list(values)
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
                    "counts": {
                        "rows": max(0, len(values) - 1),
                        "cols": len(headers),
                    },
                },
            )

        rows = build_data_dictionary_rows(include_meta_sheet=bool(include_meta_sheet))
        rows = _as_list(rows)

        validate = validate_data_dictionary_output if callable(validate_data_dictionary_output) else None
        if callable(validate):
            validate(rows, enforce_unique_sheet_key=True)

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
                "counts": {
                    "rows": len(rows),
                    "cols": len(headers),
                },
            },
        )
    except Exception as e:
        logger.warning("schema_data_dictionary failed: %s", e)
        return _error(500, request_id, f"{type(e).__name__}: {e}")


# =============================================================================
# Router mounting / compat exports
# =============================================================================
def mount(app: Any) -> None:
    app.include_router(config_router)
    app.include_router(schema_router)


def get_settings() -> Any:
    return _get_settings_cached(force_reload=False)


def allowed_tokens() -> List[str]:
    try:
        from core.config import allowed_tokens as _allowed_tokens  # type: ignore

        return list(_allowed_tokens()) if callable(_allowed_tokens) else []
    except Exception:
        return []


def is_open_mode() -> bool:
    try:
        from core.config import is_open_mode as _is_open_mode  # type: ignore

        return bool(_is_open_mode()) if callable(_is_open_mode) else False
    except Exception:
        require_auth = _safe_bool_env("REQUIRE_AUTH", True)
        return not require_auth


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
        headers: Dict[str, str] = {}
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
    "get_settings",
    "allowed_tokens",
    "is_open_mode",
    "auth_ok_request",
]
