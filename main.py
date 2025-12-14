# main.py
from __future__ import annotations

import logging
import os
from contextlib import asynccontextmanager
from typing import Any, Dict, Optional

import uvicorn
from fastapi import Depends, FastAPI, HTTPException, Request, Response
from fastapi.middleware.cors import CORSMiddleware
from slowapi import Limiter, _rate_limit_exceeded_handler
from slowapi.errors import RateLimitExceeded
from slowapi.util import get_remote_address

# =============================================================================
# Safe imports (support both "core.*" layout and flat-file layout)
# =============================================================================
try:
    from core.config import get_settings  # type: ignore
except Exception:
    from config import get_settings  # type: ignore

try:
    from core.data_engine_v2 import DataEngine, UnifiedQuote  # type: ignore
except Exception:
    from data_engine_v2 import DataEngine, UnifiedQuote  # type: ignore

settings = get_settings()

# =============================================================================
# Logging
# =============================================================================
logging.basicConfig(
    level=getattr(logging, str(getattr(settings, "LOG_LEVEL", "INFO")).upper(), logging.INFO),
    format="%(asctime)s - [%(name)s] - %(levelname)s - %(message)s",
)
logger = logging.getLogger("tadawul_fast_bridge")

# =============================================================================
# Rate limiting
# =============================================================================
limiter = Limiter(key_func=get_remote_address)

# =============================================================================
# Security (optional)
# =============================================================================
def require_app_token(request: Request) -> None:
    """
    Enforce X-APP-TOKEN (or Authorization: Bearer <token>) if APP_TOKEN is set.
    We intentionally DO NOT protect:
      - /health
      - /
      - /docs (non-prod only)
    """
    token_expected = (getattr(settings, "APP_TOKEN", None) or "").strip()
    if not token_expected:
        return

    path = request.url.path or ""
    if path in {"/", "/health"} or path.startswith("/docs"):
        return

    token = (request.headers.get("X-APP-TOKEN") or "").strip()
    if not token:
        auth = (request.headers.get("Authorization") or "").strip()
        if auth.lower().startswith("bearer "):
            token = auth.split(" ", 1)[1].strip()

    if not token or token != token_expected:
        raise HTTPException(status_code=401, detail="Unauthorized (missing/invalid token)")

# =============================================================================
# Lifespan (startup/shutdown)
# =============================================================================
@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info(
        "Starting %s v%s [%s]",
        getattr(settings, "APP_NAME", "Tadawul Fast Bridge"),
        getattr(settings, "APP_VERSION", "0.0.0"),
        getattr(settings, "APP_ENV", "unknown"),
    )

    # Initialize the data engine (never crash startup)
    try:
        app.state.engine = DataEngine()
        app.state.engine_error = None
        logger.info("Data Engine initialized successfully.")
    except Exception as exc:
        app.state.engine = None
        app.state.engine_error = str(exc)
        logger.exception("Data Engine failed to initialize: %s", exc)

    yield

    # Shutdown: close engine connections
    try:
        engine = getattr(app.state, "engine", None)
        if engine is not None and hasattr(engine, "aclose"):
            await engine.aclose()
            logger.info("Data Engine connections closed.")
    except Exception as exc:
        logger.warning("Error during engine shutdown: %s", exc)

# =============================================================================
# App
# =============================================================================
app = FastAPI(
    title=getattr(settings, "APP_NAME", "Tadawul Fast Bridge"),
    version=getattr(settings, "APP_VERSION", "0.0.0"),
    lifespan=lifespan,
    docs_url="/docs" if getattr(settings, "APP_ENV", "production") != "production" else None,
    redoc_url=None,
)

app.state.limiter = limiter
app.add_exception_handler(RateLimitExceeded, _rate_limit_exceeded_handler)

# =============================================================================
# CORS (safe)
# IMPORTANT: wildcard origins cannot be used with allow_credentials=True in browsers.
# =============================================================================
if getattr(settings, "ENABLE_CORS_ALL_ORIGINS", True):
    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],
        allow_credentials=False,  # critical fix
        allow_methods=["*"],
        allow_headers=["*"],
    )
    logger.info("CORS enabled for all origins (credentials disabled).")

# =============================================================================
# Dependencies
# =============================================================================
def get_engine(request: Request) -> DataEngine:
    engine = getattr(request.app.state, "engine", None)
    if engine is None:
        err = getattr(request.app.state, "engine_error", None) or "Engine unavailable"
        raise HTTPException(status_code=503, detail=err)
    return engine

# =============================================================================
# System routes
# =============================================================================
@app.get("/", tags=["System"])
@limiter.limit("10/minute")
async def root(request: Request):
    return {
        "service": getattr(settings, "APP_NAME", "Tadawul Fast Bridge"),
        "version": getattr(settings, "APP_VERSION", "0.0.0"),
        "status": "online",
        "docs": "disabled" if getattr(settings, "APP_ENV", "production") == "production" else "/docs",
    }

@app.head("/", tags=["System"])
async def root_head():
    return Response(status_code=200)

@app.get("/health", tags=["System"])
async def health_check():
    return {
        "status": "ok" if getattr(app.state, "engine", None) is not None else "degraded",
        "env": getattr(settings, "APP_ENV", "unknown"),
        "version": getattr(settings, "APP_VERSION", "0.0.0"),
        "provider": getattr(settings, "PRIMARY_PROVIDER", None),
        "engine_ready": getattr(app.state, "engine", None) is not None,
        "engine_error": getattr(app.state, "engine_error", None),
        "docs": "disabled" if getattr(settings, "APP_ENV", "production") == "production" else "/docs",
    }

@app.head("/health", tags=["System"])
async def health_head():
    return Response(status_code=200)

@app.get("/health/deep", tags=["System"])
@limiter.limit("10/minute")
async def deep_health(
    request: Request,
    probe: int = 0,
    _: None = Depends(require_app_token),
):
    """
    Deep health: returns config + engine readiness.
    If probe=1, it will attempt tiny upstream calls (safe / best-effort).
    """
    engine = getattr(app.state, "engine", None)
    out: Dict[str, Any] = {
        "status": "ok" if engine is not None else "degraded",
        "env": getattr(settings, "APP_ENV", "unknown"),
        "version": getattr(settings, "APP_VERSION", "0.0.0"),
        "token_required": bool((getattr(settings, "APP_TOKEN", None) or "").strip()),
        "enabled_providers": getattr(settings, "ENABLED_PROVIDERS", None),
        "primary_provider": getattr(settings, "PRIMARY_PROVIDER", None),
        "keys_present": {
            "EODHD_API_KEY": bool((getattr(settings, "EODHD_API_KEY", None) or "").strip()),
            "FMP_API_KEY": bool((getattr(settings, "FMP_API_KEY", None) or "").strip()),
        },
        "engine_ready": engine is not None,
        "engine_error": getattr(app.state, "engine_error", None),
        "probe": {"enabled": bool(probe), "results": {}},
    }

    if probe and engine is not None:
        # Best-effort probes; never crash
        results: Dict[str, Any] = {}
        try:
            q = await engine.get_quote("AAPL.US")
            results["AAPL.US"] = {"data_quality": getattr(q, "data_quality", None), "data_source": getattr(q, "data_source", None)}
        except Exception as exc:
            results["AAPL.US"] = {"error": str(exc)}

        try:
            q = await engine.get_quote("1120.SR")
            results["1120.SR"] = {"data_quality": getattr(q, "data_quality", None), "data_source": getattr(q, "data_source", None)}
        except Exception as exc:
            results["1120.SR"] = {"error": str(exc)}

        out["probe"]["results"] = results

    return out

# =============================================================================
# Minimal direct endpoint (kept for backward compatibility)
# =============================================================================
@app.get("/api/v1/market-data", response_model=UnifiedQuote, tags=["Market Data"])
@limiter.limit("60/minute")
async def get_market_data(
    request: Request,
    ticker: str,
    engine: DataEngine = Depends(get_engine),
    _: None = Depends(require_app_token),
):
    if not ticker:
        raise HTTPException(status_code=400, detail="Ticker symbol is required")
    try:
        quote = await engine.get_quote(ticker)
        if getattr(quote, "data_quality", None) == "MISSING":
            logger.warning("No data found for %s", ticker)
        return quote
    except HTTPException:
        raise
    except Exception as exc:
        logger.exception("Error fetching data for %s: %s", ticker, exc)
        raise HTTPException(status_code=500, detail=str(exc))

# =============================================================================
# Include routers (v1 API)
# - We protect them with APP_TOKEN (if set).
# - Safe-import to support both `routes.*` and flat-file module layout.
# =============================================================================
def _include_router_safely(module_candidates: list[str]) -> Optional[str]:
    for mod in module_candidates:
        try:
            m = __import__(mod, fromlist=["router"])
            r = getattr(m, "router", None)
            if r is None:
                continue
            app.include_router(r, dependencies=[Depends(require_app_token)])
            logger.info("Mounted router from %s", mod)
            return mod
        except Exception as exc:
            logger.warning("Router import failed for %s: %s", mod, exc)
    return None

_include_router_safely(["routes.enriched_quote", "enriched_quote"])
_include_router_safely(["routes.routes_argaam", "routes_argaam"])
_include_router_safely(["routes.ai_analysis", "ai_analysis"])
_include_router_safely(["routes.advanced_analysis", "advanced_analysis"])
_include_router_safely(["routes.legacy_service", "legacy_service"])

# =============================================================================
# Local execution
# =============================================================================
if __name__ == "__main__":
    uvicorn.run(
        "main:app",
        host="0.0.0.0",
        port=int(os.getenv("PORT", "10000")),
        log_level=str(getattr(settings, "LOG_LEVEL", "INFO")).lower(),
        reload=True,
    )
