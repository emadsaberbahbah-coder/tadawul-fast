"""
main.py
===========================================================
TADAWUL FAST BRIDGE – FastAPI App Entrypoint (v4.5.1)

Fixes vs v4.5.0
- Accept HEAD / (Render port detection uses HEAD)
- Mount legacy router from either:
    • routes.legacy_service (if you created it)
    • legacy_service (root module in your repo)
- Router mounting stays defensive (never crashes deploy)
"""

from __future__ import annotations

import logging
import os
from datetime import datetime, timezone
from typing import Any, Dict, Optional, Iterable

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse, FileResponse, Response
from fastapi.middleware.cors import CORSMiddleware
from fastapi import APIRouter

from env import settings

# ---------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------
LOG_LEVEL = (settings.log_level or "INFO").upper()
logging.basicConfig(
    level=LOG_LEVEL,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)
logger = logging.getLogger("main")

# ---------------------------------------------------------------------
# Optional Rate Limiting (slowapi)
# ---------------------------------------------------------------------
limiter = None
RateLimitExceeded = None
try:
    from slowapi import Limiter  # type: ignore
    from slowapi.util import get_remote_address  # type: ignore
    from slowapi.errors import RateLimitExceeded as _RLE  # type: ignore

    RateLimitExceeded = _RLE
    limiter = Limiter(key_func=get_remote_address, default_limits=["240/minute"])
    logger.info("SlowAPI limiter enabled (default 240/minute).")
except Exception:
    logger.info("SlowAPI limiter not enabled (slowapi not available or import failed).")

# ---------------------------------------------------------------------
# App
# ---------------------------------------------------------------------
app = FastAPI(
    title=settings.app_name or "Tadawul Fast Bridge",
    version=settings.app_version or "0.0.0",
    description="Unified KSA + Global market data + Google Sheets integration (Engine v2).",
)

if limiter is not None:
    app.state.limiter = limiter  # slowapi convention

    @app.exception_handler(RateLimitExceeded)  # type: ignore[arg-type]
    async def _rate_limit_handler(request: Request, exc: Exception):
        return JSONResponse(status_code=429, content={"detail": "Rate limit exceeded"})

# ---------------------------------------------------------------------
# CORS
# ---------------------------------------------------------------------
if settings.enable_cors_all_origins:
    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],
        allow_credentials=False,
        allow_methods=["*"],
        allow_headers=["*"],
        expose_headers=["*"],
    )
else:
    app.add_middleware(
        CORSMiddleware,
        allow_origins=[],
        allow_credentials=False,
        allow_methods=["*"],
        allow_headers=["*"],
    )

# ---------------------------------------------------------------------
# Auth Middleware (X-APP-TOKEN)
# ---------------------------------------------------------------------
PUBLIC_PATH_PREFIXES = ("/health", "/docs", "/redoc", "/openapi.json", "/favicon.ico", "/ui")
PUBLIC_EXACT_PATHS = {
    "/",
    "/health",
    "/v1/enriched/health",
    "/v1/analysis/health",
    "/v1/advanced/health",
    "/v1/argaam/health",
    "/v1/legacy/health",
    "/v1/analysis/ping",
    "/v1/advanced/ping",
    "/v1/enriched/ping",
    "/v1/argaam/ping",
}

def _is_public_path(path: str) -> bool:
    if path in PUBLIC_EXACT_PATHS:
        return True
    return any(path.startswith(p) for p in PUBLIC_PATH_PREFIXES)

def _valid_token(token: Optional[str]) -> bool:
    if not token:
        return False
    token = token.strip()
    if not token:
        return False
    if settings.app_token and token == settings.app_token.strip():
        return True
    if settings.backup_app_token and token == settings.backup_app_token.strip():
        return True
    return False

@app.middleware("http")
async def auth_middleware(request: Request, call_next):
    path = request.url.path or "/"
    if _is_public_path(path):
        return await call_next(request)

    token = request.headers.get("X-APP-TOKEN") or request.headers.get("x-app-token")
    if not _valid_token(token):
        return JSONResponse(status_code=401, content={"detail": "Unauthorized: missing or invalid X-APP-TOKEN"})

    return await call_next(request)

# ---------------------------------------------------------------------
# Global Exception Handler (Sheets-safe)
# ---------------------------------------------------------------------
@app.exception_handler(Exception)
async def global_exception_handler(request: Request, exc: Exception):
    logger.exception("Unhandled exception at %s: %s", request.url.path, exc)
    return JSONResponse(
        status_code=200,  # Sheets-safe
        content={
            "status": "error",
            "error": str(exc),
            "path": request.url.path,
            "time_utc": datetime.now(timezone.utc).isoformat(),
        },
    )

# ---------------------------------------------------------------------
# Routers (defensive imports)
# ---------------------------------------------------------------------
def _include_router_safely(import_path: str, attrs: Iterable[str] = ("router",)) -> bool:
    """
    Tries importing a module and including its router.
    Returns True if mounted.
    """
    try:
        module = __import__(import_path, fromlist=list(attrs))
    except Exception as e:
        logger.warning("Router not mounted (%s): %s", import_path, e)
        return False

    for attr in attrs:
        try:
            candidate = getattr(module, attr, None)
            if isinstance(candidate, APIRouter):
                app.include_router(candidate)
                logger.info("Mounted router: %s.%s", import_path, attr)
                return True
        except Exception as e:
            logger.warning("Failed mounting %s.%s: %s", import_path, attr, e)

    # Optional pattern: module.register(app)
    try:
        reg = getattr(module, "register", None)
        if callable(reg):
            reg(app)
            logger.info("Mounted via register(): %s.register(app)", import_path)
            return True
    except Exception as e:
        logger.warning("Failed register() in %s: %s", import_path, e)

    logger.warning("Router not mounted (%s): no router found", import_path)
    return False

# Preferred routers
_include_router_safely("routes.enriched_quote")
_include_router_safely("routes.ai_analysis")
_include_router_safely("routes.advanced_analysis")
_include_router_safely("routes_argaam")

# Legacy router: support BOTH module locations
# 1) routes/legacy_service.py
# 2) legacy_service.py (repo root)
if not _include_router_safely("routes.legacy_service"):
    _include_router_safely("legacy_service")

# ---------------------------------------------------------------------
# Root + Health + UI
# ---------------------------------------------------------------------
@app.get("/")
async def root() -> Dict[str, Any]:
    return {
        "status": "ok",
        "app": settings.app_name,
        "version": settings.app_version,
        "env": settings.app_env,
        "engine": "DataEngineV2",
        "time_utc": datetime.now(timezone.utc).isoformat(),
    }

# IMPORTANT: Render sends HEAD / sometimes -> avoid 405 noise
@app.head("/")
async def root_head() -> Response:
    return Response(status_code=200)

@app.get("/health")
async def health() -> Dict[str, Any]:
    return {
        "status": "ok",
        "app": settings.app_name,
        "version": settings.app_version,
        "env": settings.app_env,
        "time_utc": datetime.now(timezone.utc).isoformat(),
    }

@app.head("/health")
async def health_head() -> Response:
    return Response(status_code=200)

@app.get("/ui")
async def ui():
    path = os.path.join(os.path.dirname(__file__), "index.html")
    if os.path.exists(path):
        return FileResponse(path, media_type="text/html")
    return JSONResponse(status_code=404, content={"detail": "index.html not found in service root"})

# ---------------------------------------------------------------------
# Startup log
# ---------------------------------------------------------------------
@app.on_event("startup")
async def startup_event():
    logger.info("==============================================")
    logger.info("🚀 %s starting", settings.app_name)
    logger.info("   Env: %s | Version: %s", settings.app_env, settings.app_version)
    logger.info("   Providers: %s", ",".join(settings.enabled_providers or []))
    logger.info("   CORS all origins: %s", settings.enable_cors_all_origins)
    logger.info("==============================================")
