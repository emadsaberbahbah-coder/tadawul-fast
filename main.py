"""
main.py
------------------------------------------------------------
Tadawul Fast Bridge – FastAPI Entry Point (PROD SAFE)

IMPORTANT:
- Render runs: uvicorn main:app --host 0.0.0.0 --port $PORT
- Therefore this module MUST expose: `app = FastAPI(...)`

This file:
- Loads settings (config.get_settings)
- Enables CORS (optionally all origins)
- Enables SlowAPI rate limiting (if available)
- Mounts routers defensively (won’t crash if optional modules missing)
- Exposes / and /health endpoints
"""

from __future__ import annotations

import logging
import os
from typing import Optional

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

# -------------------------
# Logging
# -------------------------
LOG_FORMAT = "%(asctime)s | %(levelname)s | %(name)s | %(message)s"
logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"), format=LOG_FORMAT)
logger = logging.getLogger("main")

# -------------------------
# Settings
# -------------------------
try:
    from config import get_settings
except Exception as e:  # ultra-safe: never crash import path
    get_settings = None
    logger.warning("Settings loader not available (config.py). Using env only. Error: %s", e)


def _mount_router(app_: FastAPI, module_path: str, attr: str = "router", prefix: str = "") -> None:
    """
    Import and mount a FastAPI router without crashing the app if missing.
    """
    try:
        mod = __import__(module_path, fromlist=[attr])
        router = getattr(mod, attr)
        app_.include_router(router, prefix=prefix)
        logger.info("Mounted router: %s.%s", module_path, attr)
    except Exception as e:
        logger.warning("Router not mounted (%s): %s", module_path, e)


def create_app() -> FastAPI:
    settings = get_settings() if get_settings else None

    app_ = FastAPI(
        title=(settings.APP_NAME if settings else os.getenv("APP_NAME", "Tadawul Fast Bridge")),
        version=(settings.APP_VERSION if settings else os.getenv("APP_VERSION", "0.0.0")),
    )

    # Keep settings accessible to routers/services
    app_.state.settings = settings

    # -------------------------
    # CORS
    # -------------------------
    cors_all = True
    if settings:
        cors_all = bool(settings.ENABLE_CORS_ALL_ORIGINS)

    if cors_all:
        allow_origins = ["*"]
    else:
        # You can later restrict by setting CORS_ORIGINS env var to comma-separated values
        allow_origins = [o.strip() for o in os.getenv("CORS_ORIGINS", "").split(",") if o.strip()] or ["*"]

    app_.add_middleware(
        CORSMiddleware,
        allow_origins=allow_origins,
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )

    # -------------------------
    # SlowAPI (rate limit)
    # -------------------------
    try:
        from slowapi import Limiter
        from slowapi.errors import RateLimitExceeded
        from slowapi.middleware import SlowAPIMiddleware
        from slowapi.util import get_remote_address
        from starlette.responses import JSONResponse

        limiter = Limiter(key_func=get_remote_address, default_limits=["240/minute"])
        app_.state.limiter = limiter
        app_.add_middleware(SlowAPIMiddleware)

        @app_.exception_handler(RateLimitExceeded)
        async def _rate_limit_handler(request, exc):  # noqa: ANN001
            return JSONResponse(status_code=429, content={"detail": "Rate limit exceeded"})

        logger.info("SlowAPI limiter enabled (default 240/minute).")
    except Exception as e:
        logger.warning("SlowAPI not enabled: %s", e)

    # -------------------------
    # Routes
    # -------------------------
    _mount_router(app_, "routes.enriched_quote", "router")
    _mount_router(app_, "routes.ai_analysis", "router")
    _mount_router(app_, "routes.advanced_analysis", "router")
    _mount_router(app_, "routes_argaam", "router")

    # Optional legacy route module path (some projects used routes.legacy_service)
    _mount_router(app_, "routes.legacy_service", "router")
    # Current legacy router (based on your logs)
    _mount_router(app_, "legacy_service", "router")

    # -------------------------
    # Health & Root
    # -------------------------
    @app_.get("/", tags=["system"])
    async def root():
        return {"status": "ok", "app": app_.title, "version": app_.version}

    @app_.get("/health", tags=["system"])
    async def health():
        providers = None
        engine = os.getenv("ENGINE", "DataEngineV2")
        if settings:
            providers = ",".join(settings.ENABLED_PROVIDERS) if isinstance(settings.ENABLED_PROVIDERS, list) else str(settings.ENABLED_PROVIDERS)

        return {
            "status": "ok",
            "app": app_.title,
            "version": app_.version,
            "env": (settings.APP_ENV if settings else os.getenv("APP_ENV", "production")),
            "engine": engine,
            "providers": providers,
        }

    # Startup banner
    try:
        env = settings.APP_ENV if settings else os.getenv("APP_ENV", "production")
        prov = (
            ",".join(settings.ENABLED_PROVIDERS)
            if settings and isinstance(settings.ENABLED_PROVIDERS, list)
            else (str(settings.ENABLED_PROVIDERS) if settings else os.getenv("ENABLED_PROVIDERS", ""))
        )
        logger.info("==============================================")
        logger.info("🚀 Tadawul Fast Bridge starting")
        logger.info("   Env: %s | Version: %s", env, app_.version)
        logger.info("   Providers: %s", prov)
        logger.info("   CORS all origins: %s", cors_all)
        logger.info("==============================================")
    except Exception:
        pass

    return app_


# ✅ REQUIRED BY RENDER START COMMAND: uvicorn main:app
app = create_app()
