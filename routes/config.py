#!/usr/bin/env python3
"""
routes/config.py
================================================================================
TFB Config Routes — v5.7.0
================================================================================
CONFIG-ONLY • SCHEMA-DECOUPLED • PROMETHEUS-SAFE • RENDER-SAFE • AUTH-COMPATIBLE

Why this revision
-----------------
- FIX: restores this module to a true config-only router.
- FIX: removes ownership of `/v1/schema/*` from `routes.config` so canonical
       schema ownership can live in `routes.advanced_analysis`.
- FIX: keeps auth/config helpers stable for other routers without mounting
       schema endpoints from this module.
- SAFE: no Prometheus metric creation here.
- SAFE: no network calls at import time.
- SAFE: defensive compatibility with older/newer `core.config` variants.

Endpoints kept
--------------
- GET  /v1/config/health
- GET  /v1/config/settings
- POST /v1/config/reload
- GET  /v1/config/versions

Important note
--------------
This file intentionally does NOT mount `/v1/schema/*`.
Those endpoints should be owned and mounted by `routes.advanced_analysis`.
"""

from __future__ import annotations

import logging
import os
import uuid
from datetime import datetime, timezone
from typing import Any, Dict, Iterable, List, Mapping, Optional

from fastapi import APIRouter, Query, Request
from fastapi.responses import JSONResponse

try:
    from fastapi.responses import ORJSONResponse as BestJSONResponse  # type: ignore
except Exception:  # pragma: no cover
    BestJSONResponse = JSONResponse  # type: ignore

logger = logging.getLogger("routes.config")

ROUTER_VERSION = "5.7.0"
ROUTE_OWNER_NAME = "config"
ROUTE_FAMILY_NAME = "config"

config_router = APIRouter(prefix="/v1/config", tags=["config"])


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


def _error(status_code: int, request_id: str, message: str) -> BestJSONResponse:
    return BestJSONResponse(
        status_code=status_code,
        content={
            "status": "error",
            "error": str(message),
            "request_id": request_id,
            "timestamp_utc": _now_utc(),
            "version": ROUTER_VERSION,
            "route_owner": ROUTE_OWNER_NAME,
            "route_family": ROUTE_FAMILY_NAME,
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

    try:
        settings = _get_settings_cached(force_reload=False)
        open_mode = bool(_obj_get(settings, "open_mode", False))
        allow_query_token = _allow_query_token(settings)
    except Exception:
        open_mode = False
        allow_query_token = _safe_bool_env("ALLOW_QUERY_TOKEN", False)

    data["module"] = "routes.config"
    data["version"] = ROUTER_VERSION
    data["route_owner"] = ROUTE_OWNER_NAME
    data["route_family"] = ROUTE_FAMILY_NAME
    data["open_mode"] = open_mode
    data["allow_query_token"] = allow_query_token
    data["timestamp_utc"] = _now_utc()
    return data


@config_router.get("")
@config_router.get("/", include_in_schema=False)
async def config_root() -> Dict[str, Any]:
    return {
        "status": "ok",
        "service": "config",
        "version": ROUTER_VERSION,
        "route_owner": ROUTE_OWNER_NAME,
        "route_family": ROUTE_FAMILY_NAME,
        "root_path": "/v1/config",
        "supported_paths": [
            "/v1/config",
            "/v1/config/health",
            "/v1/config/settings",
            "/v1/config/reload",
            "/v1/config/versions",
        ],
        "timestamp_utc": _now_utc(),
    }


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
                "route_owner": ROUTE_OWNER_NAME,
                "route_family": ROUTE_FAMILY_NAME,
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
                "route_owner": ROUTE_OWNER_NAME,
                "route_family": ROUTE_FAMILY_NAME,
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
                "route_owner": ROUTE_OWNER_NAME,
                "route_family": ROUTE_FAMILY_NAME,
                "count": len(items),
                "items": items,
            },
        )
    except Exception as e:
        logger.warning("config_versions failed: %s", e)
        return _error(500, request_id, f"{type(e).__name__}: {e}")


# =============================================================================
# Router mounting / compat exports
# =============================================================================
def mount(app: Any) -> None:
    app.include_router(config_router)


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
    "ROUTE_OWNER_NAME",
    "ROUTE_FAMILY_NAME",
    "config_router",
    "mount",
    "get_settings",
    "allowed_tokens",
    "is_open_mode",
    "auth_ok_request",
]
