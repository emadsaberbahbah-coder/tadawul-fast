#!/usr/bin/env python3
"""
routes/config.py
================================================================================
TFB Config Routes — v5.5.2 (PROMETHEUS-SAFE / RENDER-SAFE / CORE-CONFIG-BRIDGE)
================================================================================

Fixes your deployment blocker:
- ✅ FIX: NO Prometheus metric creation here (prevents duplicate-timeseries crash)
  Error you saw:
    ValueError: Duplicated timeseries in CollectorRegistry: {'config_requests_total', ...}

Alignment goals:
- ✅ Uses core.config as the single source of truth (tokens, open mode, reload, masking)
- ✅ Provides a real FastAPI router AND mount(app) so dynamic route loader shows "green"
- ✅ Import-safe: no network calls, no SDK init, no heavy background tasks at import-time
- ✅ Backward compatibility: exports allowed_tokens/is_open_mode/get_settings/auth_ok_request

Endpoints:
- GET  /v1/config/health
- GET  /v1/config/settings
- POST /v1/config/reload
- GET  /v1/config/versions
"""

from __future__ import annotations

import logging
import os
import uuid
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, Body, Query, Request

# Best JSON response (orjson if available)
try:
    from fastapi.responses import ORJSONResponse as BestJSONResponse  # type: ignore
except Exception:
    from starlette.responses import JSONResponse as BestJSONResponse  # type: ignore

logger = logging.getLogger("routes.config")

CONFIG_VERSION = "5.5.2"
ROUTER_VERSION = CONFIG_VERSION

router = APIRouter(prefix="/v1/config", tags=["config"])


# -----------------------------------------------------------------------------
# Internal helpers
# -----------------------------------------------------------------------------
def _now_utc() -> str:
    return datetime.now(timezone.utc).isoformat()


def _get_request_id(request: Request) -> str:
    return request.headers.get("X-Request-ID") or str(uuid.uuid4())[:8]


def _auth_ok(request: Request) -> bool:
    """
    Uses core.config.auth_ok as the single auth gate.
    Accepts:
      - X-APP-TOKEN
      - X-API-KEY
      - Authorization: Bearer <token>
    """
    try:
        from core.config import is_open_mode, auth_ok  # type: ignore

        if callable(is_open_mode) and is_open_mode():
            return True

        token = (
            request.headers.get("X-APP-TOKEN")
            or request.headers.get("X-App-Token")
            or request.headers.get("X-API-KEY")
            or request.headers.get("X-Api-Key")
        )
        authz = request.headers.get("Authorization")

        if callable(auth_ok):
            return bool(auth_ok(token=token, authorization=authz, headers=dict(request.headers)))

    except Exception:
        # If core.config is unavailable, do NOT accidentally expose config endpoints
        return False

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


# -----------------------------------------------------------------------------
# Routes
# -----------------------------------------------------------------------------
@router.get("/health", include_in_schema=False)
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


@router.get("/settings")
async def get_masked_settings(
    request: Request,
    force_reload: bool = Query(False, description="Force reload settings cache (no distributed calls unless enabled)"),
) -> BestJSONResponse:
    request_id = _get_request_id(request)
    if not _auth_ok(request):
        return _error(401, request_id, "unauthorized")

    try:
        from core.config import get_settings_cached, mask_settings  # type: ignore

        s = get_settings_cached(force_reload=bool(force_reload)) if callable(get_settings_cached) else None
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


@router.post("/reload")
async def reload_config(
    request: Request,
    author: str = Body(default="", embed=True),
    comment: str = Body(default="", embed=True),
) -> BestJSONResponse:
    """
    Reload settings (merges distributed overrides ONLY if enabled in core.config).
    """
    request_id = _get_request_id(request)
    if not _auth_ok(request):
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
                "validation": {"errors": errors, "warnings": warnings},
                "settings": masked,
            },
        )
    except Exception as e:
        logger.warning("reload_config failed: %s", e)
        return _error(500, request_id, f"{type(e).__name__}: {e}")


@router.get("/versions", include_in_schema=False)
async def config_versions(
    request: Request,
    limit: int = Query(10, ge=1, le=50),
) -> BestJSONResponse:
    """
    Exposes recent config version history if core.config version manager is used.
    """
    request_id = _get_request_id(request)
    if not _auth_ok(request):
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


# -----------------------------------------------------------------------------
# Dynamic loader hook
# -----------------------------------------------------------------------------
def mount(app: Any) -> None:
    app.include_router(router)


# -----------------------------------------------------------------------------
# Backward-compatible exports (some modules may import from routes.config)
# -----------------------------------------------------------------------------
def get_settings() -> Any:
    try:
        from core.config import get_settings_cached  # type: ignore

        return get_settings_cached()
    except Exception:
        return None


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
        req = (os.getenv("REQUIRE_AUTH") or "").strip().lower() in {"1", "true", "yes", "y", "on"}
        return not req


def auth_ok_request(
    *,
    x_app_token: Optional[str] = None,
    authorization: Optional[str] = None,
    query_token: Optional[str] = None,
) -> bool:
    """
    Compatibility shim for older callers.
    """
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
    "CONFIG_VERSION",
    "ROUTER_VERSION",
    "router",
    "mount",
    # compat exports
    "get_settings",
    "allowed_tokens",
    "is_open_mode",
    "auth_ok_request",
]
