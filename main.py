"""
main.py
------------------------------------------------------------
Tadawul Fast Bridge – FastAPI Entry Point (PROD SAFE)

Render runs:
  uvicorn main:app --host 0.0.0.0 --port $PORT

So this module MUST expose:
  app = FastAPI(...)
"""

from __future__ import annotations

import logging
import os
import json
from typing import Any, Optional, List

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

# -------------------------
# Logging
# -------------------------
LOG_FORMAT = "%(asctime)s | %(levelname)s | %(name)s | %(message)s"
logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"), format=LOG_FORMAT)
logger = logging.getLogger("main")

# -------------------------
# Optional Settings Sources
# Priority: config.py (typed) -> env.py (project defaults) -> OS env
# -------------------------
settings = None
env_mod = None

try:
    from config import get_settings  # type: ignore
    settings = get_settings()
    logger.info("Settings loaded from config.py")
except Exception as e:
    logger.warning("Settings loader not available (config.py). Using env.py / OS env. Error: %s", e)

try:
    import env as env_mod  # type: ignore
except Exception:
    env_mod = None


def _get(name: str, default: Any = None) -> Any:
    if settings is not None and hasattr(settings, name):
        return getattr(settings, name)
    if env_mod is not None and hasattr(env_mod, name):
        return getattr(env_mod, name)
    return os.getenv(name, default)


def _parse_providers(v: Any) -> List[str]:
    if v is None:
        return []
    if isinstance(v, list):
        return [str(x).strip().lower() for x in v if str(x).strip()]
    s = str(v).strip()
    if not s:
        return []
    # JSON array?
    if s.startswith("[") and s.endswith("]"):
        try:
            arr = json.loads(s)
            if isinstance(arr, list):
                return [str(x).strip().lower() for x in arr if str(x).strip()]
        except Exception:
            pass
    return [p.strip().lower() for p in s.split(",") if p.strip()]


def _mount_router(app_: FastAPI, module_path: str, attr: str = "router") -> None:
    try:
        mod = __import__(module_path, fromlist=[attr])
        router = getattr(mod, attr)
        app_.include_router(router)
        logger.info("Mounted router: %s.%s", module_path, attr)
    except Exception as e:
        logger.warning("Router not mounted (%s): %s", module_path, e)


def create_app() -> FastAPI:
    title = _get("APP_NAME", "Tadawul Fast Bridge")
    version = _get("APP_VERSION", "4.6.0")  # ✅ fallback avoids 0.0.0
    app_env = _get("APP_ENV", "production")

    app_ = FastAPI(title=title, version=version)
    app_.state.settings = settings
    app_.state.app_env = app_env

    # -------------------------
    # CORS
    # -------------------------
    cors_all = str(_get("ENABLE_CORS_ALL_ORIGINS", "true")).strip().lower() in ("1", "true", "yes", "y", "on")
    allow_origins = ["*"] if cors_all else [o.strip() for o in str(_get("CORS_ORIGINS", "")).split(",") if o.strip()] or ["*"]

    app_.add_middleware(
        CORSMiddleware,
        allow_origins=allow_origins,
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )

    # -------------------------
    # SlowAPI rate limiting
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
    # Routes (defensive mounting)
    # -------------------------
    _mount_router(app_, "routes.enriched_quote")
    _mount_router(app_, "routes.ai_analysis")
    _mount_router(app_, "routes.advanced_analysis")
    _mount_router(app_, "routes_argaam")
    _mount_router(app_, "legacy_service")  # your log confirms this exists

    # -------------------------
    # System endpoints
    # (explicit HEAD avoids the Render internal HEAD / 405 noise)
    # -------------------------
    @app_.api_route("/", methods=["GET", "HEAD"], tags=["system"])
    async def root():
        return {"status": "ok", "app": app_.title, "version": app_.version, "env": app_env}

    @app_.get("/health", tags=["system"])
    async def health():
        providers = _parse_providers(_get("ENABLED_PROVIDERS", ""))  # informational only
        engine = _get("ENGINE", "DataEngineV2")
        return {
            "status": "ok",
            "app": app_.title,
            "version": app_.version,
            "env": app_env,
            "engine": engine,
            "providers": providers,
        }

    # Startup banner
    providers = _parse_providers(_get("ENABLED_PROVIDERS", ""))
    logger.info("==============================================")
    logger.info("🚀 Tadawul Fast Bridge starting")
    logger.info("   Env: %s | Version: %s", app_env, app_.version)
    logger.info("   Providers: %s", ",".join(providers) if providers else "(not set)")
    logger.info("   CORS all origins: %s", cors_all)
    logger.info("==============================================")

    return app_


# ✅ REQUIRED BY RENDER: uvicorn main:app
app = create_app()
