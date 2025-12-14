"""
main.py
===========================================================
TADAWUL FAST BRIDGE – FastAPI App Entrypoint (v4.5.0)

Goals
- Deploy-safe for Render (uvicorn main:app).
- Centralized:
    • CORS
    • Token auth (X-APP-TOKEN) with public health endpoints
    • Rate limiting (slowapi) if installed
    • Router mounting (Enriched, AI, Advanced, Argaam, Legacy)
- Never hard-crash if an optional router is missing (logs + continues).

Author: Emad Bahbah (with GPT-5.2 Thinking)
"""

from __future__ import annotations

import logging
import os
from datetime import datetime, timezone
from typing import Any, Dict, Optional

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse, FileResponse
from fastapi.middleware.cors import CORSMiddleware

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
        return JSONResponse(
            status_code=429,
            content={"detail": "Rate limit exceeded"},
        )

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
    # If you later want allowlist mode, add env var parsing here.
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
PUBLIC_PATH_PREFIXES = (
    "/health",
    "/docs",
    "/redoc",
    "/openapi.json",
    "/favicon.ico",
    "/ui",
)
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
    for p in PUBLIC_PATH_PREFIXES:
        if path.startswith(p):
            return True
    return False

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
    """
    Protect all non-public endpoints using X-APP-TOKEN.
    """
    path = request.url.path or "/"
    if _is_public_path(path):
        return await call_next(request)

    token = request.headers.get("X-APP-TOKEN") or request.headers.get("x-app-token")
    if not _valid_token(token):
        return JSONResponse(
            status_code=401,
            content={"detail": "Unauthorized: missing or invalid X-APP-TOKEN"},
        )

    return await call_next(request)

# ---------------------------------------------------------------------
# Global Exception Handler (defensive for Sheets callers)
# ---------------------------------------------------------------------
@app.exception_handler(Exception)
async def global_exception_handler(request: Request, exc: Exception):
    logger.exception("Unhandled exception at %s: %s", request.url.path, exc)
    return JSONResponse(
        status_code=200,  # Sheets-safe: avoid breaking refresh pipelines
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
def _include_router_safely(import_path: str, attr: str = "router"):
    try:
        module = __import__(import_path, fromlist=[attr])
        router = getattr(module, attr)
        app.include_router(router)
        logger.info("Mounted router: %s.%s", import_path, attr)
    except Exception as e:
        logger.warning("Router not mounted (%s): %s", import_path, e)

# Enriched routes
_include_router_safely("routes.enriched_quote")
# AI routes
_include_router_safely("routes.ai_analysis")
# Advanced routes
_include_router_safely("routes.advanced_analysis")
# KSA Argaam gateway
_include_router_safely("routes_argaam")
# Legacy routes (has absolute paths inside)
_include_router_safely("routes.legacy_service")

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

@app.get("/health")
async def health() -> Dict[str, Any]:
    return {
        "status": "ok",
        "app": settings.app_name,
        "version": settings.app_version,
        "env": settings.app_env,
        "time_utc": datetime.now(timezone.utc).isoformat(),
    }

@app.get("/ui")
async def ui():
    """
    Serves local index.html if present (Render will include it if in repo root).
    """
    path = os.path.join(os.path.dirname(__file__), "index.html")
    if os.path.exists(path):
        return FileResponse(path, media_type="text/html")
    return JSONResponse(
        status_code=404,
        content={"detail": "index.html not found in service root"},
    )

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
