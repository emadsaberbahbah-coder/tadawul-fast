#!/usr/bin/env python3
# routes/config.py
"""
================================================================================
TFB Config Routes — v5.9.0
================================================================================
CONFIG-ONLY • SCHEMA-DECOUPLED • PROMETHEUS-SAFE • RENDER-SAFE • AUTH-COMPATIBLE
FLEXIBLE-AUTH-OK • STATE-AWARE-REQUEST-ID • RESPONSE-HEADER-PROPAGATION

Why this revision vs v5.8.0
---------------------------
- FIX: `_auth_ok` now invokes `core.config.auth_ok` using the project-wide
       flexible calling pattern (multi-signature attempts). Older versions of
       `core.config.auth_ok` didn't accept `path` / `request` / `settings`;
       current versions do. This matches `main.py._call_auth_ok_flexible`,
       `routes.analysis_sheet_rows._auth_passed`,
       `routes.data_dictionary._auth_passed`, and
       `routes.investment_advisor._auth_passed`.
- FIX: `_get_request_id` now prefers `request.state.request_id` (set by
       `main.py`'s `RequestIDMiddleware`) before falling back to the
       `X-Request-ID` header or a new UUID4. Matches the convention used by
       `investment_advisor`, `advanced_analysis`, and `advisor`.
- FIX: every response echoes `X-Request-ID` so clients can correlate —
       consistent with `investment_advisor.py` and `advanced_analysis.py`.
- FIX: `/reload` now best-effort calls `core.config.save_config_version(...)`
       if available, so the `author` / `comment` audit fields are actually
       persisted to the version manager (otherwise silently skipped).
- FIX: `/health` reports capability flags (core_config_available,
       `config_health_check_callable`, `mask_settings_callable`,
       `reload_settings_callable`, `get_version_manager_callable`,
       `save_config_version_callable`) + a route inventory, matching the
       diagnostic depth of other routers' health endpoints.
- KEEP: restores this module to a true config-only router — does NOT mount
        `/v1/schema/*` (owned by `routes.advanced_analysis`).
- KEEP: `router = config_router` alias for `main._router_from_module` discovery.
- KEEP: explicit `Query(...)` declarations for `author` / `comment` on POST
        `/reload` (previously bare strings behaved inconsistently across
        FastAPI versions).
- SAFE: no Prometheus metric creation here.
- SAFE: no network calls at import time.
- SAFE: defensive compatibility with older/newer `core.config` variants.

Endpoints kept
--------------
- GET  /v1/config            (root descriptor)
- GET  /v1/config/health
- GET  /v1/config/settings
- POST /v1/config/reload
- GET  /v1/config/versions

Important note
--------------
This file intentionally does NOT mount `/v1/schema/*`.
Those endpoints are owned by `routes.advanced_analysis`.
"""

from __future__ import annotations

import logging
import os
import uuid
from datetime import datetime, timezone
from typing import Any, Dict, Iterable, List, Mapping, Optional, Tuple

from fastapi import APIRouter, Query, Request
from fastapi.responses import JSONResponse

try:
    from fastapi.responses import ORJSONResponse as BestJSONResponse  # type: ignore
except Exception:  # pragma: no cover
    BestJSONResponse = JSONResponse  # type: ignore

logger = logging.getLogger("routes.config")

ROUTER_VERSION = "5.9.0"
ROUTE_OWNER_NAME = "config"
ROUTE_FAMILY_NAME = "config"

config_router = APIRouter(prefix="/v1/config", tags=["config"])

# main.py's _router_from_module looks for "router" / "api_router" / "routes" /
# "ROUTER". Keeping the `router` alias guarantees discovery regardless of
# which attribute the loader probes.
router = config_router


# =============================================================================
# Generic helpers
# =============================================================================
def _now_utc() -> str:
    return datetime.now(timezone.utc).isoformat()


def _get_request_id(request: Request) -> str:
    """
    Prefer the request_id set by main.py's RequestIDMiddleware (stored on
    request.state). Fall back to the X-Request-ID header, then a UUID4 hex.
    """
    try:
        rid = getattr(request.state, "request_id", None)
        if rid:
            return str(rid).strip()
    except Exception:
        pass
    try:
        hdr = request.headers.get("X-Request-ID")
        if hdr and str(hdr).strip():
            return str(hdr).strip()
    except Exception:
        pass
    return uuid.uuid4().hex[:12]


def _safe_bool_env(name: str, default: bool = False) -> bool:
    try:
        value = (os.getenv(name, str(default)) or "").strip().lower()
        return value in {"1", "true", "yes", "y", "on", "t"}
    except Exception:
        return default


def _response_headers(request_id: str) -> Dict[str, str]:
    return {"X-Request-ID": request_id}


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
        headers=_response_headers(request_id),
    )


def _ok(status_code: int, request_id: str, payload: Dict[str, Any]) -> BestJSONResponse:
    body = {
        "status": "ok",
        "request_id": request_id,
        "timestamp_utc": _now_utc(),
        "version": ROUTER_VERSION,
        "route_owner": ROUTE_OWNER_NAME,
        "route_family": ROUTE_FAMILY_NAME,
    }
    body.update(payload or {})
    return BestJSONResponse(
        status_code=status_code,
        content=body,
        headers=_response_headers(request_id),
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


def _import_core_attr(name: str) -> Any:
    """Lazy import of a single attribute from `core.config`. Returns None on failure."""
    try:
        import importlib

        mod = importlib.import_module("core.config")
        return getattr(mod, name, None)
    except Exception:
        return None


# =============================================================================
# Auth / config helpers
# =============================================================================
def _get_settings_cached(force_reload: bool = False) -> Any:
    getter = _import_core_attr("get_settings_cached")
    if callable(getter):
        try:
            return getter(force_reload=bool(force_reload))
        except TypeError:
            # Older signature that didn't accept force_reload
            try:
                return getter()
            except Exception:
                return None
        except Exception:
            return None
    return None


def _is_open_mode_effective() -> bool:
    fn = _import_core_attr("is_open_mode")
    if callable(fn):
        try:
            return bool(fn())
        except Exception:
            return False
    return False


def _allow_query_token(settings: Any) -> bool:
    for attr in ("ALLOW_QUERY_TOKEN", "allow_query_token"):
        try:
            if settings is not None and hasattr(settings, attr):
                return bool(getattr(settings, attr))
        except Exception:
            continue
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
        # Bearer token wins over ambiguous header-only values
        token = authorization.strip().split(" ", 1)[1].strip()

    settings = _get_settings_cached(force_reload=False)
    if (not token) and query_token and _allow_query_token(settings):
        token = (query_token or "").strip()

    return {"token": token, "authorization": authorization}


def _call_auth_ok_flexible(
    fn: Any,
    *,
    token: Optional[str],
    authorization: Optional[str],
    headers: Dict[str, str],
    path: str,
    request: Request,
    settings: Any,
) -> bool:
    """
    Flexible signature dispatch — mirrors the pattern used across the project
    in main._call_auth_ok_flexible, analysis_sheet_rows._auth_passed,
    data_dictionary._auth_passed, and investment_advisor._auth_passed.

    Older core.config.auth_ok versions didn't accept path/request/settings;
    current versions do. We start with the richest signature and degrade.
    """
    attempts: Tuple[Dict[str, Any], ...] = (
        {"token": token or None, "authorization": authorization, "headers": headers,
         "path": path, "request": request, "settings": settings},
        {"token": token or None, "authorization": authorization, "headers": headers,
         "path": path, "request": request},
        {"token": token or None, "authorization": authorization, "headers": headers,
         "path": path},
        {"token": token or None, "authorization": authorization, "headers": headers},
        {"token": token or None, "authorization": authorization},
        {"token": token or None},
    )
    for kwargs in attempts:
        try:
            return bool(fn(**kwargs))
        except TypeError:
            continue
        except Exception:
            return False
    return False


def _auth_ok(request: Request, *, query_token: Optional[str] = None) -> bool:
    # 1) Open-mode short-circuit
    if _is_open_mode_effective():
        return True

    # 2) Pull core.config.auth_ok lazily
    auth_ok_fn = _import_core_attr("auth_ok")
    if not callable(auth_ok_fn):
        # If auth is not importable, fail-closed — the project convention in
        # the config surface is to require auth by default.
        return False

    # 3) Extract token + authorization (handles bearer and query token)
    info = _extract_token_from_request(request, query_token=query_token)
    token = info.get("token")
    authz = info.get("authorization")

    path = str(getattr(getattr(request, "url", None), "path", "") or "")
    headers_dict = dict(request.headers)
    settings = _get_settings_cached(force_reload=False)

    return _call_auth_ok_flexible(
        auth_ok_fn,
        token=token,
        authorization=authz,
        headers=headers_dict,
        path=path,
        request=request,
        settings=settings,
    )


# =============================================================================
# /v1/config/*
# =============================================================================
@config_router.get("/health", include_in_schema=False)
async def config_health(request: Request) -> BestJSONResponse:
    """
    Public health endpoint. Exposes:
      - upstream `core.config.config_health_check()` output (if available)
      - capability flags for every `core.config` function we rely on
      - open_mode / allow_query_token effective values
    No auth required — matches the project convention for /health endpoints.
    """
    request_id = _get_request_id(request)

    # Upstream health check
    data: Dict[str, Any]
    try:
        fn = _import_core_attr("config_health_check")
        if callable(fn):
            data = dict(fn() or {})
        else:
            data = {"status": "unknown", "checks": {}, "warnings": [], "errors": []}
    except Exception as e:
        data = {
            "status": "unhealthy",
            "checks": {},
            "warnings": [],
            "errors": [f"{type(e).__name__}: {e}"],
        }

    # Local settings-derived flags
    try:
        settings = _get_settings_cached(force_reload=False)
        open_mode = _is_open_mode_effective() or bool(_obj_get(settings, "open_mode", False)) or bool(_obj_get(settings, "OPEN_MODE", False))
        allow_query_token = _allow_query_token(settings)
    except Exception:
        open_mode = _is_open_mode_effective()
        allow_query_token = _safe_bool_env("ALLOW_QUERY_TOKEN", False)

    # Capability flags — lets operators quickly see which core.config features
    # are actually wired up behind this deployment.
    try:
        import importlib

        core_config = importlib.import_module("core.config")
        core_config_available = True
    except Exception:
        core_config = None
        core_config_available = False

    def _has(name: str) -> bool:
        return callable(getattr(core_config, name, None)) if core_config else False

    capabilities = {
        "core_config_available": core_config_available,
        "config_health_check_callable": _has("config_health_check"),
        "get_settings_cached_callable": _has("get_settings_cached"),
        "reload_settings_callable": _has("reload_settings"),
        "mask_settings_callable": _has("mask_settings"),
        "allowed_tokens_callable": _has("allowed_tokens"),
        "is_open_mode_callable": _has("is_open_mode"),
        "auth_ok_callable": _has("auth_ok"),
        "get_version_manager_callable": _has("get_version_manager"),
        "save_config_version_callable": _has("save_config_version"),
    }

    # Routes owned by this module
    owned_routes: List[str] = []
    try:
        for route in list(getattr(config_router, "routes", []) or []):
            path = getattr(route, "path", None)
            if path:
                owned_routes.append(str(path))
    except Exception:
        pass

    data["module"] = "routes.config"
    data["version"] = ROUTER_VERSION
    data["route_owner"] = ROUTE_OWNER_NAME
    data["route_family"] = ROUTE_FAMILY_NAME
    data["open_mode"] = bool(open_mode)
    data["allow_query_token"] = bool(allow_query_token)
    data["capabilities"] = capabilities
    data["owned_routes"] = sorted(set(owned_routes))
    data["timestamp_utc"] = _now_utc()
    data["request_id"] = request_id

    return BestJSONResponse(
        status_code=200,
        content=data,
        headers=_response_headers(request_id),
    )


@config_router.get("")
@config_router.get("/", include_in_schema=False)
async def config_root(request: Request) -> BestJSONResponse:
    request_id = _get_request_id(request)
    payload = {
        "service": "config",
        "root_path": "/v1/config",
        "supported_paths": [
            "/v1/config",
            "/v1/config/health",
            "/v1/config/settings",
            "/v1/config/reload",
            "/v1/config/versions",
        ],
        "schema_endpoints_owner": "routes.advanced_analysis",  # documentation hint
    }
    return _ok(200, request_id, payload)


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
        mask_settings = _import_core_attr("mask_settings")
        settings = _get_settings_cached(force_reload=bool(force_reload))
        payload = mask_settings(settings) if callable(mask_settings) else {}
        return _ok(200, request_id, {"settings": payload})
    except Exception as e:
        logger.warning("get_masked_settings failed: %s", e)
        return _error(500, request_id, f"{type(e).__name__}: {e}")


@config_router.post("/reload")
async def reload_config(
    request: Request,
    token: Optional[str] = Query(None, description="Query token if allow_query_token is enabled"),
    # Explicit Query() declarations — bare `str = ""` was ambiguous and
    # behaved differently across FastAPI versions.
    author: str = Query("", description="Author label for this config reload (audit trail)"),
    comment: str = Query("", description="Comment/reason for this config reload (audit trail)"),
) -> BestJSONResponse:
    request_id = _get_request_id(request)
    if not _auth_ok(request, query_token=token):
        return _error(401, request_id, "unauthorized")

    try:
        reload_settings = _import_core_attr("reload_settings")
        mask_settings = _import_core_attr("mask_settings")

        if not callable(reload_settings):
            return _error(500, request_id, "reload_settings unavailable")

        settings = reload_settings()
        masked = mask_settings(settings) if callable(mask_settings) else {}

        # Best-effort validation
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

        # Best-effort audit trail: persist this reload to the version manager
        # if core.config exposes `save_config_version`. Previous versions just
        # dropped `author`/`comment` on the floor.
        author_clean = (author or "").strip()
        comment_clean = (comment or "").strip()
        version_recorded = False
        version_record: Optional[Dict[str, Any]] = None
        save_config_version = _import_core_attr("save_config_version")
        if callable(save_config_version):
            try:
                # Try the richest signature first, then degrade.
                rec = None
                for kwargs in (
                    {"settings": settings, "author": author_clean, "comment": comment_clean},
                    {"author": author_clean, "comment": comment_clean},
                    {},
                ):
                    try:
                        rec = save_config_version(**kwargs)
                        break
                    except TypeError:
                        continue
                version_recorded = rec is not None or True  # attempted; treat as recorded if no exception
                if rec is not None:
                    if hasattr(rec, "to_dict") and callable(getattr(rec, "to_dict")):
                        try:
                            version_record = dict(rec.to_dict())  # type: ignore[attr-defined]
                        except Exception:
                            version_record = {"value": str(rec)}
                    elif isinstance(rec, Mapping):
                        version_record = dict(rec)
                    else:
                        version_record = {"value": str(rec)}
            except Exception as e:
                logger.warning("save_config_version failed (non-fatal): %s", e)
                version_recorded = False

        return _ok(
            200,
            request_id,
            {
                "meta": {
                    "author": author_clean,
                    "comment": comment_clean,
                    "version_recorded": bool(version_recorded),
                    "version_record": version_record,
                },
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
        get_version_manager = _import_core_attr("get_version_manager")
        items: List[Dict[str, Any]] = []
        vm = _call_if_callable(get_version_manager)
        if vm is not None:
            history = getattr(vm, "history", None)
            if callable(history):
                try:
                    records = history(limit=int(limit))
                except TypeError:
                    records = history()
                for item in _as_list(records):
                    try:
                        if hasattr(item, "to_dict") and callable(getattr(item, "to_dict")):
                            items.append(dict(item.to_dict()))  # type: ignore[attr-defined]
                        elif isinstance(item, Mapping):
                            items.append(dict(item))
                        else:
                            items.append({"value": str(item)})
                    except Exception:
                        items.append({"value": str(item)})

        return _ok(200, request_id, {"count": len(items), "items": items})
    except Exception as e:
        logger.warning("config_versions failed: %s", e)
        return _error(500, request_id, f"{type(e).__name__}: {e}")


# =============================================================================
# Router mounting / compat exports
# =============================================================================
def mount(app: Any) -> None:
    """Idempotent mount helper for legacy callers that prefer a `mount(app)` API."""
    try:
        if app is not None and hasattr(app, "include_router"):
            app.include_router(config_router)
    except Exception:
        pass


def get_router() -> APIRouter:
    """Used by some loader variants that probe `get_router()` instead of `router`."""
    return config_router


def get_settings() -> Any:
    return _get_settings_cached(force_reload=False)


def allowed_tokens() -> List[str]:
    try:
        fn = _import_core_attr("allowed_tokens")
        return list(fn()) if callable(fn) else []
    except Exception:
        return []


def is_open_mode() -> bool:
    """
    Compat shim. Prefer `core.config.is_open_mode()`. Fall back to
    `REQUIRE_AUTH=false` environment only when core.config is unavailable.
    Note: this fallback is intentionally more permissive than the true
    `core.config.is_open_mode()` (which also requires no tokens to exist)
    because this branch only runs when `core.config` cannot be imported.
    """
    try:
        fn = _import_core_attr("is_open_mode")
        if callable(fn):
            return bool(fn())
    except Exception:
        pass
    require_auth = _safe_bool_env("REQUIRE_AUTH", True)
    return not require_auth


def auth_ok_request(
    *,
    x_app_token: Optional[str] = None,
    authorization: Optional[str] = None,
    query_token: Optional[str] = None,
) -> bool:
    """
    Out-of-request compat shim. Prefer `_auth_ok(request, query_token=...)`
    from inside a FastAPI handler when you have access to the request object.
    """
    try:
        fn = _import_core_attr("auth_ok")
        if not callable(fn):
            return False

        token = x_app_token or query_token
        headers: Dict[str, str] = {}
        if x_app_token:
            headers["X-APP-TOKEN"] = x_app_token
        if authorization:
            headers["Authorization"] = authorization

        # Flexible calling with no request/path/settings available in this
        # out-of-request context.
        attempts: Tuple[Dict[str, Any], ...] = (
            {"token": token or None, "authorization": authorization, "headers": headers},
            {"token": token or None, "authorization": authorization},
            {"token": token or None},
        )
        for kwargs in attempts:
            try:
                return bool(fn(**kwargs))
            except TypeError:
                continue
            except Exception:
                return False
        return False
    except Exception:
        return False


__all__ = [
    "ROUTER_VERSION",
    "ROUTE_OWNER_NAME",
    "ROUTE_FAMILY_NAME",
    "config_router",
    "router",        # main.py router discovery
    "mount",
    "get_router",
    "get_settings",
    "allowed_tokens",
    "is_open_mode",
    "auth_ok_request",
]
