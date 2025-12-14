import logging
import os
from contextlib import asynccontextmanager
from importlib import import_module
from typing import Any, Dict, Optional

import uvicorn
from fastapi import Depends, FastAPI, Header, HTTPException, Request, Response
from fastapi.middleware.cors import CORSMiddleware
from slowapi import Limiter, _rate_limit_exceeded_handler
from slowapi.errors import RateLimitExceeded
from slowapi.util import get_remote_address

from core.config import get_settings
from core.data_engine_v2 import DataEngine, UnifiedQuote

settings = get_settings()

# -----------------------------------------------------------------------------
# Logging
# -----------------------------------------------------------------------------
logging.basicConfig(
    level=settings.LOG_LEVEL,
    format="%(asctime)s - [%(name)s] - %(levelname)s - %(message)s",
)
logger = logging.getLogger("tadawul_fast_bridge")

# -----------------------------------------------------------------------------
# Lifespan: create one shared engine (used by /api/v1/market-data).
# Routers may also create their own engine singletons; that's okay.
# -----------------------------------------------------------------------------
@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info(f"Starting {settings.APP_NAME} v{settings.APP_VERSION} [{settings.APP_ENV}]")
    app.state.engine = DataEngine()
    logger.info("Data Engine initialized successfully.")
    try:
        yield
    finally:
        logger.info("Shutting down application...")
        if hasattr(app.state, "engine"):
            try:
                await app.state.engine.aclose()
                logger.info("Data Engine connections closed.")
            except Exception as exc:
                logger.warning(f"Error closing Data Engine: {exc}")

# -----------------------------------------------------------------------------
# App
# -----------------------------------------------------------------------------
app = FastAPI(
    title=settings.APP_NAME,
    version=settings.APP_VERSION,
    lifespan=lifespan,
    docs_url="/docs" if settings.APP_ENV != "production" else None,
    redoc_url=None,
)

# -----------------------------------------------------------------------------
# Rate limiting
# -----------------------------------------------------------------------------
limiter = Limiter(key_func=get_remote_address)
app.state.limiter = limiter
app.add_exception_handler(RateLimitExceeded, _rate_limit_exceeded_handler)

# -----------------------------------------------------------------------------
# CORS
# -----------------------------------------------------------------------------
if settings.ENABLE_CORS_ALL_ORIGINS:
    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )
    logger.info("CORS enabled for all origins.")

# -----------------------------------------------------------------------------
# Simple API token protection (optional)
# If APP_TOKEN is set, require header X-APP-TOKEN on all non-system endpoints.
# -----------------------------------------------------------------------------
def require_app_token(x_app_token: Optional[str] = Header(default=None, alias="X-APP-TOKEN")) -> None:
    if not settings.APP_TOKEN:
        return
    if not x_app_token or x_app_token != settings.APP_TOKEN:
        raise HTTPException(status_code=401, detail="Unauthorized (missing/invalid X-APP-TOKEN)")

# -----------------------------------------------------------------------------
# Dependency: engine from app state
# -----------------------------------------------------------------------------
def get_engine(request: Request) -> DataEngine:
    return request.app.state.engine

# -----------------------------------------------------------------------------
# System endpoints
# -----------------------------------------------------------------------------
@app.get("/health", tags=["System"])
@app.head("/health", tags=["System"])
async def health_check() -> Dict[str, Any]:
    engine = getattr(app.state, "engine", None)
    enabled = getattr(engine, "enabled_providers", None)
    return {
        "status": "ok",
        "app": settings.APP_NAME,
        "version": settings.APP_VERSION,
        "env": settings.APP_ENV,
        "primary_provider": settings.PRIMARY_PROVIDER,
        "enabled_providers": enabled or settings.ENABLED_PROVIDERS,
    }

@app.get("/", tags=["System"])
@limiter.limit("10/minute")
async def root(request: Request) -> Dict[str, Any]:
    return {
        "service": settings.APP_NAME,
        "version": settings.APP_VERSION,
        "status": "online",
        "docs": "disabled" if settings.APP_ENV == "production" else "/docs",
    }

@app.head("/", tags=["System"])
async def root_head() -> Response:
    # Render may send HEAD / to verify service
    return Response(status_code=200)

# -----------------------------------------------------------------------------
# Main market data endpoint (backward compatible)
# -----------------------------------------------------------------------------
@app.get("/api/v1/market-data", response_model=UnifiedQuote, tags=["Market Data"])
@limiter.limit("60/minute")
async def get_market_data(
    request: Request,
    ticker: str,
    engine: DataEngine = Depends(get_engine),
    _auth: None = Depends(require_app_token),
) -> UnifiedQuote:
    if not ticker or not str(ticker).strip():
        raise HTTPException(status_code=400, detail="Ticker symbol is required")

    sym = str(ticker).strip()

    try:
        q = await engine.get_enriched_quote(sym)
        if q.data_quality == "MISSING":
            logger.warning(f"No data found for {sym}: {q.error or 'unknown'}")
        return q
    except Exception as exc:
        logger.exception("Error fetching data for %s", sym, exc_info=exc)
        raise HTTPException(status_code=500, detail=str(exc))

# -----------------------------------------------------------------------------
# Include modular routers (if present)
# -----------------------------------------------------------------------------
def _try_include(module_paths: list[str]) -> None:
    for mp in module_paths:
        try:
            mod = import_module(mp)
            r = getattr(mod, "router", None)
            if r is not None:
                app.include_router(r, dependencies=[Depends(require_app_token)])
                logger.info(f"Router included: {mp}")
            return
        except Exception:
            continue

_try_include(["routes.enriched_quote", "enriched_quote"])
_try_include(["routes_argaam", "routes.routes_argaam"])
_try_include(["routes.advanced_analysis", "advanced_analysis"])
_try_include(["routes.ai_analysis", "ai_analysis"])
_try_include(["legacy_service", "routes.legacy_service"])

# -----------------------------------------------------------------------------
# Local entry point
# -----------------------------------------------------------------------------
if __name__ == "__main__":
    uvicorn.run(
        "main:app",
        host="0.0.0.0",
        port=int(os.getenv("PORT", "10000")),
        log_level=settings.LOG_LEVEL.lower(),
        reload=True,
    )
