#!/usr/bin/env python3
"""
routes/config.py
================================================================================
TFB Config Router — v5.4.2 (RENDER-SAFE / ROUTER-AWARE / PROMETHEUS-SAFE)
================================================================================

Why this revision:
- ✅ FIX (your deploy error): Prometheus duplicate metrics crash
    - This module NO LONGER registers metrics named:
      config_requests_total / config_requests_created / config_requests
    - Uses unique metric names: tfb_config_router_*
    - Uses "get-or-create" guards to avoid CollectorRegistry duplication
- ✅ FIX: Provides `router = APIRouter(...)` AND `mount(app)` so it mounts correctly
- ✅ ALIGN: Uses core.config as the single source of truth (no second config system here)
- ✅ IMPORT-SAFE: No network calls, no heavy SDK init at import-time

Endpoints:
- GET  /v1/config/health
- GET  /v1/config/settings   (masked; auth required unless OPEN_MODE)
- POST /v1/config/reload     (auth required unless OPEN_MODE)
"""

from __future__ import annotations

import logging
import os
import time
from typing import Any, Dict, Optional

from fastapi import APIRouter, Request
from fastapi.encoders import jsonable_encoder

try:
    from fastapi.responses import ORJSONResponse as BestJSONResponse
except Exception:
    from starlette.responses import JSONResponse as BestJSONResponse  # type: ignore

logger = logging.getLogger("routes.config")

ROUTER_VERSION = "5.4.2"
router = APIRouter(prefix="/v1/config", tags=["config"])

# =============================================================================
# Prometheus (SAFE: unique metric names + get-or-create guards)
# =============================================================================
try:
    from prometheus_client import Counter, Histogram, Gauge, REGISTRY  # type: ignore

    _PROM_AVAILABLE = True
except Exception:
    Counter = Histogram = Gauge = None  # type: ignore
    REGISTRY = None  # type: ignore
    _PROM_AVAILABLE = False


class _DummyMetric:
    def labels(self, *args, **kwargs):  # noqa: ANN001
        return self

    def inc(self, *args, **kwargs):  # noqa: ANN001
        return None

    def observe(self, *args, **kwargs):  # noqa: ANN001
        return None

    def set(self, *args, **kwargs):  # noqa: ANN001
        return None


def _registry_get(name: str) -> Optional[Any]:
    if not _PROM_AVAILABLE or REGISTRY is None:
        return None
    try:
        return getattr(REGISTRY, "_names_to_collectors", {}).get(name)
    except Exception:
        return None


def _counter(name: str, doc: str, labels: Optional[list[str]] = None) -> Any:
    if not _PROM_AVAILABLE or Counter is None:
        return _DummyMetric()
    existing = _registry_get(name)
    if existing is not None:
        return existing
    try:
        return Counter(name, doc, labels or [])
    except ValueError:
        existing = _registry_get(name)
        return existing if existing is not None else _DummyMetric()
    except Exception:
        return _DummyMetric()


def _hist(name: str, doc: str, labels: Optional[list[str]] = None) -> Any:
    if not _PROM_AVAILABLE or Histogram is None:
        return _DummyMetric()
    existing = _registry_get(name)
    if existing is not None:
        return existing
    try:
        return Histogram(name, doc, labels or [])
    except ValueError:
        existing = _registry_get(name)
        return existing if existing is not None else _DummyMetric()
    except Exception:
        return _DummyMetric()


def _gauge(name: str, doc: str, labels: Optional[list[str]] = None) -> Any:
    if not _PROM_AVAILABLE or Gauge is None:
        return _DummyMetric()
    existing = _registry_get(name)
    if existing is not None:
        return existing
    try:
        return Gauge(name, doc, labels or [])
    except ValueError:
        existing = _registry_get(name)
        return existing if existing is not None else _DummyMetric()
    except Exception:
        return _DummyMetric()


cfg_requests_total = _counter(
    "tfb_config_router_requests_total",
    "TFB config router requests",
    ["endpoint", "status"],
)
cfg_duration = _hist(
    "tfb_config_router_duration_seconds",
    "TFB config router request duration (seconds)",
    ["endpoint"],
)
cfg_reload_total = _counter(
    "tfb_config_router_reload_total",
    "TFB config router reload attempts",
    ["status"],
)
cfg_last_reload_epoch = _gauge(
    "tfb_config_router_last_reload_epoch",
    "TFB config router last reload time (epoch seconds)",
)


# =============================================================================
# core.config bridge (single source of truth)
# =============================================================================
_TRUTHY = {"1", "true", "yes", "y", "on", "t", "enabled"}


def _core() -> Optional[Any]:
    try:
        import core.config as cc  # type: ignore
        return cc
    except Exception:
        return None


def _is_authorized(request: Request) -> bool:
    """
    Prefer core.config auth.
    Accepts:
      - X-APP-TOKEN / X-API-Key
      - Authorization: Bearer <token>
      - token query param (only if core config allows it; otherwise best-effort)
    """
    cc = _core()

    # open-mode shortcut
    try:
        if cc and callable(getattr(cc, "is_open_mode", None)) and cc.is_open_mode():
            return True
    except Exception:
        pass

    x_token = (
        request.headers.get("X-APP-TOKEN")
        or request.headers.get("X-App-Token")
        or request.headers.get("X-API-Key")
        or request.headers.get("X-Api-Key")
    )
    authz = request.headers.get("Authorization") or ""
    q_token = request.query_params.get("token") if request.query_params else None

    # core.config.auth_ok
    try:
        if cc and callable(getattr(cc, "auth_ok", None)):
            return bool(cc.auth_ok(token=x_token or q_token, authorization=authz, headers=dict(request.headers)))
    except Exception:
        pass

    # fallback policy: if REQUIRE_AUTH is truthy, deny unless token matches APP_TOKEN/BACKUP/ALLOWED list
    require_auth = str(os.getenv("REQUIRE_AUTH", "")).strip().lower() in _TRUTHY
    allowed = set()

    for k in ("ALLOWED_TOKENS", "TFB_ALLOWED_TOKENS", "APP_TOKENS"):
        raw = (os.getenv(k) or "").strip()
        if raw:
            allowed.update([t.strip() for t in raw.split(",") if t.strip()])
    for k in ("APP_TOKEN", "TFB_APP_TOKEN", "BACKEND_TOKEN", "BACKUP_APP_TOKEN"):
        v = (os.getenv(k) or "").strip()
        if v:
            allowed.add(v)

    if not allowed:
        return not require_auth  # if auth required but no tokens configured => deny

    if x_token and x_token.strip() in allowed:
        return True
    if q_token and q_token.strip() in allowed:
        return True
    if authz.lower().startswith("bearer "):
        b = authz.split(" ", 1)[1].strip()
        return b in allowed
    return False


def _ok(payload: Dict[str, Any], *, status_code: int = 200) -> BestJSONResponse:
    return BestJSONResponse(status_code=status_code, content=jsonable_encoder(payload))


def _err(msg: str, *, status_code: int = 400, extra: Optional[Dict[str, Any]] = None) -> BestJSONResponse:
    p: Dict[str, Any] = {"status": "error", "error": msg, "router_version": ROUTER_VERSION}
    if extra:
        p.update(extra)
    return BestJSONResponse(status_code=status_code, content=jsonable_encoder(p))


# =============================================================================
# Routes
# =============================================================================
@router.get("/health", include_in_schema=False)
async def health(_request: Request):
    t0 = time.time()
    try:
        cc = _core()
        core_health: Dict[str, Any]
        if cc and callable(getattr(cc, "config_health_check", None)):
            core_health = cc.config_health_check()
        else:
            core_health = {
                "status": "degraded",
                "checks": {"core_config_import": False},
                "warnings": ["core.config not importable"],
                "errors": [],
            }

        cfg_requests_total.labels(endpoint="health", status="ok").inc()
        return _ok(
            {
                "status": "ok",
                "router": "config",
                "router_version": ROUTER_VERSION,
                "core_config": core_health,
            }
        )
    except Exception as e:
        logger.exception("config.health failed")
        cfg_requests_total.labels(endpoint="health", status="error").inc()
        return _err("health_failed", status_code=500, extra={"detail": str(e)})
    finally:
        cfg_duration.labels(endpoint="health").observe(max(0.0, time.time() - t0))


@router.get("/settings", include_in_schema=False)
async def settings(request: Request):
    t0 = time.time()
    try:
        if not _is_authorized(request):
            cfg_requests_total.labels(endpoint="settings", status="unauthorized").inc()
            return _err("unauthorized", status_code=401)

        cc = _core()
        if not cc:
            cfg_requests_total.labels(endpoint="settings", status="degraded").inc()
            return _err("core_config_missing", status_code=503)

        mask_fn = getattr(cc, "mask_settings", None)
        if not callable(mask_fn):
            cfg_requests_total.labels(endpoint="settings", status="degraded").inc()
            return _err("mask_settings_unavailable", status_code=503)

        cfg_requests_total.labels(endpoint="settings", status="ok").inc()
        return _ok({"status": "ok", "settings": mask_fn(), "router_version": ROUTER_VERSION})
    except Exception as e:
        logger.exception("config.settings failed")
        cfg_requests_total.labels(endpoint="settings", status="error").inc()
        return _err("settings_failed", status_code=500, extra={"detail": str(e)})
    finally:
        cfg_duration.labels(endpoint="settings").observe(max(0.0, time.time() - t0))


@router.post("/reload", include_in_schema=False)
async def reload_config(request: Request):
    t0 = time.time()
    try:
        if not _is_authorized(request):
            cfg_requests_total.labels(endpoint="reload", status="unauthorized").inc()
            cfg_reload_total.labels(status="unauthorized").inc()
            return _err("unauthorized", status_code=401)

        cc = _core()
        if not cc:
            cfg_requests_total.labels(endpoint="reload", status="degraded").inc()
            cfg_reload_total.labels(status="degraded").inc()
            return _err("core_config_missing", status_code=503)

        reload_fn = getattr(cc, "reload_settings", None)
        mask_fn = getattr(cc, "mask_settings", None)
        if not callable(reload_fn) or not callable(mask_fn):
            cfg_requests_total.labels(endpoint="reload", status="degraded").inc()
            cfg_reload_total.labels(status="degraded").inc()
            return _err("reload_not_supported", status_code=503)

        _ = reload_fn()
        cfg_last_reload_epoch.set(int(time.time()))
        cfg_requests_total.labels(endpoint="reload", status="ok").inc()
        cfg_reload_total.labels(status="ok").inc()
        return _ok({"status": "ok", "settings": mask_fn(), "router_version": ROUTER_VERSION})
    except Exception as e:
        logger.exception("config.reload failed")
        cfg_requests_total.labels(endpoint="reload", status="error").inc()
        cfg_reload_total.labels(status="error").inc()
        return _err("reload_failed", status_code=500, extra={"detail": str(e)})
    finally:
        cfg_duration.labels(endpoint="reload").observe(max(0.0, time.time() - t0))


# =============================================================================
# Mount helper for your dynamic router loader
# =============================================================================
def mount(app) -> None:  # noqa: ANN001
    app.include_router(router)


# =============================================================================
# Compatibility exports (so other modules can import routes.config safely)
# =============================================================================
def get_settings():  # noqa: ANN001
    cc = _core()
    if cc and callable(getattr(cc, "get_settings_cached", None)):
        return cc.get_settings_cached()
    if cc and callable(getattr(cc, "get_settings", None)):
        return cc.get_settings()
    return {}


def allowed_tokens() -> list[str]:
    cc = _core()
    if cc and callable(getattr(cc, "allowed_tokens", None)):
        return list(cc.allowed_tokens())
    raw = (os.getenv("ALLOWED_TOKENS") or os.getenv("APP_TOKENS") or "").strip()
    return [t.strip() for t in raw.split(",") if t.strip()]


def is_open_mode() -> bool:
    cc = _core()
    if cc and callable(getattr(cc, "is_open_mode", None)):
        return bool(cc.is_open_mode())
    require_auth = str(os.getenv("REQUIRE_AUTH", "")).strip().lower() in _TRUTHY
    toks = allowed_tokens()
    if require_auth and not toks:
        return False
    return len(toks) == 0


__all__ = [
    "router",
    "mount",
    "ROUTER_VERSION",
    "get_settings",
    "allowed_tokens",
    "is_open_mode",
]
