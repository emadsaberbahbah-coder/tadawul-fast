"""
main.py
------------------------------------------------------------
Tadawul Fast Bridge – FastAPI Entry Point (PROD SAFE)

Render runs:
  uvicorn main:app --host 0.0.0.0 --port $PORT

This module MUST expose:
  app = FastAPI(...)
"""

from __future__ import annotations

import json
import logging
import os
import uuid
from datetime import datetime, timezone
from importlib import import_module
from typing import Any, List, Optional

from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from starlette.middleware.gzip import GZipMiddleware
from starlette.responses import JSONResponse

# ------------------------------------------------------------
# Logging (bootstrap)
# ------------------------------------------------------------
LOG_FORMAT = "%(asctime)s | %(levelname)s | %(name)s | %(message)s"
logging.basicConfig(level=os.getenv("LOG_LEVEL", "info").upper(), format=LOG_FORMAT)
logger = logging.getLogger("main")

# ------------------------------------------------------------
# Settings loader (prefer root config.py; fallback to core.config; env.py remains supported)
# ------------------------------------------------------------
settings = None
env_mod = None

def _load_settings():
    # 1) root config.py
    try:
        from config import get_settings  # type: ignore
        return get_settings()
    except Exception:
        pass

    # 2) core.config
    try:
        from core.config import get_settings  # type: ignore
        return get_settings()
    except Exception:
        pass

    return None

try:
    settings = _load_settings()
    if settings is not None:
        try:
            logging.getLogger().setLevel(str(getattr(settings, "log_level", "info")).upper())
        except Exception:
            pass
        logger.info("Settings loaded successfully (config/core.config).")
    else:
        logger.warning("Settings not available (config/core.config). Falling back to env.py / OS env.")
except Exception as e:
    logger.warning("Settings loader failed. Falling back to env.py / OS env. Error: %s", e)

try:
    import env as env_mod  # backward-compatible exports
except Exception:
    env_mod = None


def _get(name: str, default: Any = None) -> Any:
    """
    Lookup order:
      1) settings.<name>
      2) env.<NAME> export (legacy constants)
      3) os.getenv(NAME)
    """
    if settings is not None and hasattr(settings, name):
        return getattr(settings, name)
    if env_mod is not None and hasattr(env_mod, name):
        return getattr(env_mod, name)
    return os.getenv(name, default)


def _parse_providers(v: Any) -> List[str]:
    """Accept list, CSV, or JSON list (string). Returns lower-case list."""
    if v is None:
        return []
    if isinstance(v, list):
        return [str(x).strip().lower() for x in v if str(x).strip()]
    s = str(v).strip()
    if not s:
        return []
    if s.startswith("[") and s.endswith("]"):
        try:
            arr = json.loads(s)
            if isinstance(arr, list):
                return [str(x).strip().lower() for x in arr if str(x).strip()]
        except Exception:
            pass
    return [p.strip().lower() for p in s.split(",") if p.strip()]


def _mount_router(app_: FastAPI, module_path: str, attr: str = "router") -> None:
    """Defensive router mounting: never crash app startup if a module is missing."""
    try:
        mod = import_module(module_path)
        router = getattr(mod, attr)
        app_.include_router(router)
        logger.info("Mounted router: %s.%s", module_path, attr)
    except Exception as e:
        logger.warning("Router not mounted (%s): %s", module_path, e)


def _cors_allow_origins() -> List[str]:
    # Prefer settings.cors_origins_list if available
    try:
        if settings is not None and hasattr(settings, "cors_origins_list"):
            lst = getattr(settings, "cors_origins_list")
            if isinstance(lst, list) and lst:
                return lst
    except Exception:
        pass

    cors_all = str(_get("ENABLE_CORS_ALL_ORIGINS", _get("CORS_ALL_ORIGINS", "true"))).strip().lower() in (
        "1",
        "true",
        "yes",
        "y",
        "on",
    )
    if cors_all:
        return ["*"]

    raw = str(_get("CORS_ORIGINS", "")).strip()
    return [o.strip() for o in raw.split(",") if o.strip()] or []


def _rate_limit_default() -> str:
    # Prefer settings.rate_limit_per_minute if present
    try:
        rpm = int(getattr(settings, "rate_limit_per_minute"))
        if rpm > 0:
            return f"{rpm}/minute"
    except Exception:
        pass
    return "240/minute"


def create_app() -> FastAPI:
    title = _get("app_name", _get("APP_NAME", "Tadawul Fast Bridge"))
    version = _get("version", _get("APP_VERSION", "4.6.0"))
    app_env = _get("env", _get("APP_ENV", "production"))

    app_ = FastAPI(title=str(title), version=str(version))
    app_.state.settings = settings
    app_.state.app_env = str(app_env)

    # ------------------------------------------------------------
    # Middleware: GZip + Request ID
    # ------------------------------------------------------------
    app_.add_middleware(GZipMiddleware, minimum_size=800)

    @app_.middleware("http")
    async def add_request_id(request: Request, call_next):  # noqa: ANN001
        rid = request.headers.get("X-Request-ID") or str(uuid.uuid4())
        request.state.request_id = rid
        resp = await call_next(request)
        resp.headers["X-Request-ID"] = rid
        return resp

    # ------------------------------------------------------------
    # CORS
    # ------------------------------------------------------------
    allow_origins = _cors_allow_origins()
    app_.add_middleware(
        CORSMiddleware,
        allow_origins=allow_origins if allow_origins else [],
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )

    # ------------------------------------------------------------
    # Global exception handler (safe JSON)
    # ------------------------------------------------------------
    @app_.exception_handler(Exception)
    async def unhandled_exception_handler(request: Request, exc: Exception):
        rid = getattr(getattr(request, "state", None), "request_id", None)
        logger.exception("Unhandled exception (request_id=%s): %s", rid, exc)
        return JSONResponse(
            status_code=500,
            content={
                "detail": "Internal server error",
                "request_id": rid,
            },
        )

    # ------------------------------------------------------------
    # SlowAPI rate limiting (optional, defensive)
    # ------------------------------------------------------------
    try:
        from slowapi import Limiter
        from slowapi.errors import RateLimitExceeded
        from slowapi.middleware import SlowAPIMiddleware
        from slowapi.util import get_remote_address

        limiter = Limiter(key_func=get_remote_address, default_limits=[_rate_limit_default()])
        app_.state.limiter = limiter
        app_.add_middleware(SlowAPIMiddleware)

        @app_.exception_handler(RateLimitExceeded)
        async def _rate_limit_handler(request: Request, exc):  # noqa: ANN001
            rid = getattr(getattr(request, "state", None), "request_id", None)
            return JSONResponse(status_code=429, content={"detail": "Rate limit exceeded", "request_id": rid})

        logger.info("SlowAPI limiter enabled (default %s).", _rate_limit_default())
    except Exception as e:
        logger.warning("SlowAPI not enabled: %s", e)

    # ------------------------------------------------------------
    # Routes (defensive mounting)
    # ------------------------------------------------------------
    _mount_router(app_, "routes.enriched_quote")
    _mount_router(app_, "routes.ai_analysis")
    _mount_router(app_, "routes.advanced_analysis")
    _mount_router(app_, "routes.price_history")  # NEW
    _mount_router(app_, "routes_argaam")
    _mount_router(app_, "legacy_service")

    # ------------------------------------------------------------
    # Engine bootstrap (optional but recommended)
    # - Allows routes to reuse app.state.engine (faster + consistent)
    # ------------------------------------------------------------
    @app_.on_event("startup")
    async def _startup():  # noqa: ANN001
        # Initialize DataEngine once (if available)
        try:
            from core.data_engine_v2 import DataEngine  # type: ignore

            app_.state.engine = DataEngine()
            logger.info("[startup] DataEngine initialized and stored in app.state.engine")
        except Exception as e:
            app_.state.engine = None
            logger.warning("[startup] DataEngine not initialized: %s", e)

        # Initialize history store once (if available)
        try:
            from core.price_history_store import get_price_history_store  # type: ignore

            app_.state.price_history_store = get_price_history_store()
            logger.info("[startup] PriceHistoryStore initialized and stored in app.state.price_history_store")
        except Exception as e:
            app_.state.price_history_store = None
            logger.warning("[startup] PriceHistoryStore not initialized: %s", e)

        logger.info("[startup] Completed.")

    # ------------------------------------------------------------
    # System endpoints
    # ------------------------------------------------------------
    @app_.api_route("/", methods=["GET", "HEAD"], tags=["system"])
    async def root():
        return {
            "status": "ok",
            "app": app_.title,
            "version": app_.version,
            "env": app_.state.app_env,
        }

    @app_.get("/health", tags=["system"])
    async def health():
        # Prefer config.py computed lists if available
        try:
            enabled = list(getattr(settings, "enabled_providers", [])) if settings is not None else []
        except Exception:
            enabled = _parse_providers(_get("ENABLED_PROVIDERS", _get("PROVIDERS", "")))

        try:
            ksa = list(getattr(settings, "enabled_ksa_providers", [])) if settings is not None else []
        except Exception:
            ksa = _parse_providers(_get("KSA_PROVIDERS", ""))

        engine_name = _get("ENGINE", "DataEngineV2")

        return {
            "status": "ok",
            "app": app_.title,
            "version": app_.version,
            "env": app_.state.app_env,
            "engine": engine_name,
            "engine_ready": bool(getattr(app_.state, "engine", None)),
            "history_store_ready": bool(getattr(app_.state, "price_history_store", None)),
            "providers": enabled,
            "ksa_providers": ksa,
            "time_utc": datetime.now(timezone.utc).isoformat(),
        }

    # Startup banner (log-only)
    try:
        enabled = list(getattr(settings, "enabled_providers", [])) if settings is not None else []
    except Exception:
        enabled = _parse_providers(_get("ENABLED_PROVIDERS", _get("PROVIDERS", "")))

    try:
        ksa = list(getattr(settings, "enabled_ksa_providers", [])) if settings is not None else []
    except Exception:
        ksa = _parse_providers(_get("KSA_PROVIDERS", ""))

    logger.info("==============================================")
    logger.info("🚀 Tadawul Fast Bridge starting")
    logger.info("   Env: %s | Version: %s", app_.state.app_env, app_.version)
    logger.info("   Providers: %s", ",".join(enabled) if enabled else "(not set)")
    logger.info("   KSA Providers: %s", ",".join(ksa) if ksa else "(not set)")
    logger.info("   CORS allow origins: %s", "ALL (*)" if allow_origins == ["*"] else str(allow_origins))
    logger.info("==============================================")

    return app_


# REQUIRED BY RENDER: uvicorn main:app
app = create_app()
