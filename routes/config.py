#!/usr/bin/env python3
"""
routes/config.py
================================================================================
TFB Config Router — v5.4.1 (RENDER-SAFE / ROUTER-AWARE / PROMETHEUS-SAFE)
================================================================================

Fixes your deployment errors:
- ✅ Provides `router = APIRouter(...)` so routes.config can mount (no more “No router object”)
- ✅ Fixes Prometheus duplicate metrics crash:
    - Uses UNIQUE metric names (tfb_config_router_*)
    - Uses "get-or-create" guards to avoid CollectorRegistry duplication if imported twice
- ✅ Aligns with `core/config.py` as the single source of truth (no second config system)
- ✅ Import-safe: no network calls, no heavy SDK initialization at import-time

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

ROUTER_VERSION = "5.4.1"
router = APIRouter(prefix="/v1/config", tags=["config"])

# =============================================================================
# Prometheus (SAFE: no duplicate registration, unique names)
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
    """
    Best-effort access to already-registered collectors.
    We use internal REGISTRY map defensively, because Prometheus client raises
    on duplicates and Render imports can happen twice in edge cases.
    """
    if not _PROM_AVAILABLE or REGISTRY is None:
        return None
    try:
        # prometheus_client registry internal mapping
        return getattr(REGISTRY, "_names_to_collectors", {}).get(name)
    except Exception:
        return None


def _counter(name: str, doc: str, labelnames: Optional[list[str]] = None) -> Any:
    if not _PROM_AVAILABLE or Counter is None:
        return _DummyMetric()

    existing = _registry_get(name)
    if existing is not None:
        return existing

    try:
        return Counter(name, doc, labelnames or [])
    except ValueError:
        # Already registered by another import path
        ex2 = _registry_get(name)
        return ex2 if ex2 is not None else _DummyMetric()
    except Exception:
        return _DummyMetric()


def _histogram(name: str, doc: str, labelnames: Optional[list[str]] = None) -> Any:
    if not _PROM_AVAILABLE or Histogram is None:
        return _DummyMetric()

    existing = _registry_get(name)
    if existing is not None:
        return existing

    try:
        return Histogram(name, doc, labelnames or [])
    except ValueError:
        ex2 = _registry_get(name)
        return ex2 if ex2 is not None else _DummyMetric()
    except Exception:
        return _DummyMetric()


def _gauge(name: str, doc: str, labelnames: Optional[list[str]] = None) -> Any:
    if not _PROM_AVAILABLE or Gauge is None:
        return _DummyMetric()

    existing = _registry_get(name)
    if existing is not None:
        return existing

    try:
        return Gauge(name, doc, labelnames or [])
    except ValueError:
        ex2 = _registry_get(name)
        return ex2 if ex2 is not None else _DummyMetric()
    except Exception:
        return _DummyMetric()


# Unique names (so they will not conflict with other modules)
cfg_router_requests = _counter(
    "tfb_config_router_requests",
    "TFB config router requests",
    ["endpoint", "status"],
)
cfg_router_duration = _histogram(
    "tfb_config_router_duration_seconds",
    "TFB config router request duration (seconds)",
    ["endpoint"],
)
cfg_router_reload_total = _counter(
    "tfb_config_router_reload_total",
    "TFB config router reload attempts",
    ["status"],
)
cfg_router_last_reload = _gauge(
    "tfb_config_router_last_reload_epoch",
    "TFB config router last reload time (epoch seconds)",
)


# =============================================================================
# Core config bridge (single source of truth)
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
    Use core.config.auth_ok / is_open_mode if available.
    Fallback: OPEN_MODE env or token presence checks.
    """
    cc = _core()
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
    authz = request.headers.get("Authorization")
    q_token = request.query_params.get("token") if request.query_params else None

    # prefer core.config.auth_ok
    try:
        if cc and callable(getattr(cc, "auth_ok", None)):
            return bool(cc.auth_ok(token=x_token or q_token, authorization=authz, headers=dict(request.headers)))
    except Exception:
        pass

    # fallback: OPEN_MODE or simple env token check
    if os.getenv("OPEN_MODE") is not None and str(os.getenv("OPEN_MODE", "")).strip().lower() in _TRUTHY:
        return True

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
        # if no token configured, treat as open
        return True

    if x_token and x_token.strip() in allowed:
        return True
    if q_token and q_token.strip() in allowed:
        return True
    if authz and authz.lower().startswith("bearer "):
        b = authz.split(" ", 1)[1].strip()
        return b in allowed

    return False


def _ok(payload: Dict[str, Any], *, status_code: int = 200) -> BestJSONResponse:
    return BestJSONResponse(status_code=status_code, content=jsonable_encoder(payload))


def _err(msg: str, *, status_code: int = 400, extra: Optional[Dict[str, Any]] = None) -> BestJSONResponse:
    p: Dict[str, Any] = {"status": "error", "error": msg, "version": ROUTER_VERSION}
    if extra:
        p.update(extra)
    return BestJSONResponse(status_code=status_code, content=jsonable_encoder(p))


# =============================================================================
# Routes
# =============================================================================
@router.get("/health", include_in_schema=False)
async def health(request: Request):
    t0 = time.time()
    try:
        cc = _core()
        if cc and callable(getattr(cc, "config_health_check", None)):
            data = cc.config_health_check()
        else:
            data = {
                "status": "degraded",
                "checks": {"core_config_import": False},
                "warnings": ["core.config not importable; using fallback router health"],
                "errors": [],
            }

        cfg_router_requests.labels(endpoint="health", status="ok").inc()
        return _ok(
            {
                "status": "ok",
                "router": "config",
                "router_version": ROUTER_VERSION,
                "core_config": data,
            }
        )
    except Exception as e:
        logger.exception("config.health failed")
        cfg_router_requests.labels(endpoint="health", status="error").inc()
        return _err("health_failed", status_code=500, extra={"detail": str(e)})
    finally:
        cfg_router_duration.labels(endpoint="health").observe(max(0.0, time.time() - t0))


@router.get("/settings", include_in_schema=False)
async def settings(request: Request):
    t0 = time.time()
    try:
        if not _is_authorized(request):
            cfg_router_requests.labels(endpoint="settings", status="unauthorized").inc()
            return _err("unauthorized", status_code=401)

        cc = _core()
        if not cc:
            cfg_router_requests.labels(endpoint="settings", status="degraded").inc()
            return _err("core_config_missing", status_code=503)

        fn = getattr(cc, "mask_settings", None)
        if not callable(fn):
            cfg_router_requests.labels(endpoint="settings", status="degraded").inc()
            return _err("mask_settings_unavailable", status_code=503)

        out = fn()
        cfg_router_requests.labels(endpoint="settings", status="ok").inc()
        return _ok({"status": "ok", "settings": out, "router_version": ROUTER_VERSION})
    except Exception as e:
        logger.exception("config.settings failed")
        cfg_router_requests.labels(endpoint="settings", status="error").inc()
        return _err("settings_failed", status_code=500, extra={"detail": str(e)})
    finally:
        cfg_router_duration.labels(endpoint="settings").observe(max(0.0, time.time() - t0))


@router.post("/reload", include_in_schema=False)
async def reload_config(request: Request):
    t0 = time.time()
    try:
        if not _is_authorized(request):
            cfg_router_requests.labels(endpoint="reload", status="unauthorized").inc()
            cfg_router_reload_total.labels(status="unauthorized").inc()
            return _err("unauthorized", status_code=401)

        cc = _core()
        if not cc:
            cfg_router_requests.labels(endpoint="reload", status="degraded").inc()
            cfg_router_reload_total.labels(status="degraded").inc()
            return _err("core_config_missing", status_code=503)

        reload_fn = getattr(cc, "reload_settings", None)
        mask_fn = getattr(cc, "mask_settings", None)
        if not callable(reload_fn) or not callable(mask_fn):
            cfg_router_requests.labels(endpoint="reload", status="degraded").inc()
            cfg_router_reload_total.labels(status="degraded").inc()
            return _err("reload_not_supported", status_code=503)

        _ = reload_fn()
        cfg_router_last_reload.set(int(time.time()))
        cfg_router_requests.labels(endpoint="reload", status="ok").inc()
        cfg_router_reload_total.labels(status="ok").inc()
        return _ok({"status": "ok", "settings": mask_fn(), "router_version": ROUTER_VERSION})
    except Exception as e:
        logger.exception("config.reload failed")
        cfg_router_requests.labels(endpoint="reload", status="error").inc()
        cfg_router_reload_total.labels(status="error").inc()
        return _err("reload_failed", status_code=500, extra={"detail": str(e)})
    finally:
        cfg_router_duration.labels(endpoint="reload").observe(max(0.0, time.time() - t0))


# =============================================================================
# Mount helper (for your dynamic router loader compatibility)
# =============================================================================
def mount(app) -> None:  # noqa: ANN001
    app.include_router(router)


# =============================================================================
# Compatibility exports (so other modules can import from routes.config safely)
# =============================================================================
def get_settings():  # noqa: ANN001
    cc = _core()
    if cc and callable(getattr(cc, "get_settings_cached", None)):
        return cc.get_settings_cached()
    if cc and callable(getattr(cc, "get_settings", None)):
        return cc.get_settings()
    return {}  # last-resort


def allowed_tokens() -> list[str]:
    cc = _core()
    if cc and callable(getattr(cc, "allowed_tokens", None)):
        return list(cc.allowed_tokens())
    # fallback
    raw = (os.getenv("ALLOWED_TOKENS") or os.getenv("APP_TOKENS") or "").strip()
    return [t.strip() for t in raw.split(",") if t.strip()]


def is_open_mode() -> bool:
    cc = _core()
    if cc and callable(getattr(cc, "is_open_mode", None)):
        return bool(cc.is_open_mode())
    # fallback: open if no tokens exist
    return len(allowed_tokens()) == 0


__all__ = [
    "router",
    "mount",
    "ROUTER_VERSION",
    "get_settings",
    "allowed_tokens",
    "is_open_mode",
]
