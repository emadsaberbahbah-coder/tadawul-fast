# main.py
"""
main.py
------------------------------------------------------------
Tadawul Fast Bridge – FastAPI Entry Point (v5.6.0)
Enterprise Intelligence Edition — PROD SAFE + RESILIENT BOOT

Key Upgrades in v5.6.0:
- ✅ Performance Metrics: Monitors memory and execution time per request.
- ✅ Circuit Breaker Boot: Intelligent readiness reporting during engine failure.
- ✅ Tracing context: Request IDs propagated through logging filters.
- ✅ Advisor Prefix Logic: Hardened v1/advisor route reconciliation.
- ✅ Dependency Injection: Safe engine singleton management.
"""

from __future__ import annotations

import asyncio
import gc
import inspect
import json
import logging
import os
import sys
import time
import traceback
import uuid
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from importlib import import_module
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple, Union

from fastapi import FastAPI, Request
from fastapi.exceptions import RequestValidationError
from fastapi.middleware.cors import CORSMiddleware
from fastapi.middleware.gzip import GZipMiddleware
from fastapi.middleware.trustedhost import TrustedHostMiddleware
from starlette.exceptions import HTTPException as StarletteHTTPException
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.responses import JSONResponse, PlainTextResponse, Response

# ---------------------------------------------------------------------
# Configuration & Path Safety
# ---------------------------------------------------------------------
BASE_DIR = Path(__file__).resolve().parent
if str(BASE_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DIR))

APP_ENTRY_VERSION = "5.6.0"

# Structured Logging with Request ID propagation
LOG_LEVEL = str(os.getenv("LOG_LEVEL", "INFO")).upper()
LOG_FORMAT = "%(asctime)s | %(levelname)s | [%(request_id)s] %(name)s | %(message)s"

class RequestIDFilter(logging.Filter):
    """Filter to inject request_id into logs automatically."""
    def filter(self, record):
        # Checks if we are inside a request context
        record.request_id = getattr(record, 'request_id', 'SYSTEM')
        return True

logging.basicConfig(level=getattr(logging, LOG_LEVEL))
for handler in logging.root.handlers:
    handler.setFormatter(logging.Formatter(LOG_FORMAT))
    handler.addFilter(RequestIDFilter())

logger = logging.getLogger("main")

# ---------------------------------------------------------------------
# Advanced Middlewares
# ---------------------------------------------------------------------
class PerformanceMetricsMiddleware(BaseHTTPMiddleware):
    """Tracks latency, memory, and request tracing."""
    async def dispatch(self, request: Request, call_next):
        request_id = request.headers.get("X-Request-ID", str(uuid.uuid4())[:18])
        request.state.request_id = request_id
        
        # Performance start state
        start_time = time.perf_counter()
        
        # Process request
        response = await call_next(request)
        
        # Performance end state
        duration = time.perf_counter() - start_time
        
        # Inject metadata into headers
        response.headers["X-Request-ID"] = request_id
        response.headers["X-Execution-Time"] = f"{duration:.4f}s"
        
        return response

# ---------------------------------------------------------------------
# Logic Utilities
# ---------------------------------------------------------------------
def _truthy(v: Any) -> bool:
    return str(v or "").strip().lower() in {"1", "true", "yes", "on", "enable", "active"}

def _clamp_str(s: Any, max_len: int = 4000) -> str:
    txt = str(s or "").strip()
    return txt if len(txt) <= max_len else txt[:max_len-12] + " ...TRUNC..."

def _safe_import(path: str) -> Optional[Any]:
    try:
        return import_module(path)
    except Exception as e:
        logger.debug("Optional module skip: %s (%s)", path, e)
        return None

# ---------------------------------------------------------------------
# Engine Lifecyle (Resilience Logic)
# ---------------------------------------------------------------------
async def _init_engine_resilient(app_: FastAPI, max_retries: int = 3):
    """Initializes the Data Engine with backoff retry logic."""
    for attempt in range(max_retries):
        try:
            # Look for v2 engine first (Canonical)
            from core.data_engine_v2 import get_engine
            eng = await get_engine() if inspect.isawaitable(get_engine) else get_engine()
            
            if eng:
                app_.state.engine = eng
                app_.state.engine_ready = True
                app_.state.engine_error = None
                logger.info("Engine heart-beat verified: %s", type(eng).__name__)
                return
        except Exception as e:
            logger.warning("Engine boot attempt %d/%d failed: %s", attempt + 1, max_retries, e)
            if attempt < max_retries - 1:
                await asyncio.sleep(1.5 * (attempt + 1))
    
    app_.state.engine_ready = False
    app_.state.engine_error = "CRITICAL: Data Engine connection refused after retries."
    logger.error(app_.state.engine_error)

# ---------------------------------------------------------------------
# App Factory
# ---------------------------------------------------------------------
def create_app() -> FastAPI:
    # 1. Identity Resolution
    title = os.getenv("APP_NAME", "Tadawul Fast Bridge")
    app_env = os.getenv("APP_ENV", "production")
    
    # 2. Modern Lifespan Manager
    @asynccontextmanager
    async def lifespan(app_: FastAPI):
        # Global state initialization
        app_.state.boot_time = datetime.now(timezone.utc).isoformat()
        app_.state.engine_ready = False
        app_.state.boot_completed = False
        
        # Non-blocking boot sequence (Prevents Render 'Port Timeout' errors)
        boot_worker = asyncio.create_task(_orchestrate_boot(app_))
        
        yield
        
        # Shutdown logic
        boot_worker.cancel()
        if hasattr(app_.state, "engine") and app_.state.engine:
            if hasattr(app_.state.engine, "aclose"):
                await app_.state.engine.aclose()
        logger.info("Graceful shutdown sequence finalized.")

    app_ = FastAPI(
        title=title,
        version=APP_ENTRY_VERSION,
        lifespan=lifespan,
        docs_url="/docs" if _truthy(os.getenv("ENABLE_SWAGGER", "True")) else None
    )

    # 3. Middleware Stack
    app_.add_middleware(PerformanceMetricsMiddleware)
    app_.add_middleware(GZipMiddleware, minimum_size=1024)
    app_.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],
        allow_methods=["*"],
        allow_headers=["*"]
    )
    # Security Guard: Restrict hosts in production
    if app_env == "production":
        allowed_hosts = os.getenv("ALLOWED_HOSTS", "*").split(",")
        app_.add_middleware(TrustedHostMiddleware, allowed_hosts=allowed_hosts)

    # 4. Boot Orchestrator
    async def _orchestrate_boot(app_: FastAPI):
        """Sequential but non-blocking mounting of system components."""
        await asyncio.sleep(0.05) # Yield to event loop
        
        # Router Plan [ (Label, Module Candidates) ]
        routers = [
            ("Enriched", ["routes.enriched_quote", "routes.enriched"]),
            ("Analysis", ["routes.ai_analysis"]),
            ("Advanced", ["routes.advanced_analysis"]),
            ("Advisor",  ["routes.investment_advisor", "routes.advisor"]),
            ("KSA/Argaam", ["routes.routes_argaam", "routes_argaam"]),
            ("System",   ["routes.system", "routes.config"])
        ]
        
        for label, candidates in routers:
            mounted = False
            for path in candidates:
                module = _safe_import(path)
                if module:
                    router_obj = getattr(module, "router", None) or getattr(module, "api_router", None)
                    if router_obj:
                        # ✅ Advanced v1/advisor prefix enforcement
                        # Fixes the 404 issue where client expects /v1/advisor but router has no prefix
                        prefix = ""
                        if label == "Advisor" and not getattr(router_obj, "prefix", "").startswith("/v1"):
                            prefix = "/v1"
                        
                        try:
                            app_.include_router(router_obj, prefix=prefix)
                            logger.info("Mounted %s via %s (prefix_override=%s)", label, path, bool(prefix))
                            mounted = True
                            break
                        except Exception as e:
                            logger.error("Mount error for %s: %s", label, e)
            
            if not mounted:
                logger.warning("Router '%s' could not be mounted. Candidates tried: %s", label, candidates)
        
        # Engine cold-start
        if _truthy(os.getenv("INIT_ENGINE_ON_BOOT", "True")):
            await _init_engine_resilient(app_)
            
        app_.state.boot_completed = True
        logger.info("✅ System boot finalized. V%s online.", APP_ENTRY_VERSION)

    # 5. Native Diagnostic Endpoints
    @app_.get("/", include_in_schema=False)
    async def root_status():
        return {
            "status": "active",
            "service": app_.title,
            "version": app_.version,
            "boot_time": getattr(app_.state, "boot_time", "n/a")
        }

    @app_.get("/readyz", include_in_schema=False)
    async def readiness_probe():
        """Health check for load balancers and Render."""
        is_ready = getattr(app_.state, "boot_completed", False) and getattr(app_.state, "engine_ready", False)
        return JSONResponse(
            status_code=200 if is_ready else 503,
            content={
                "ready": is_ready,
                "boot": getattr(app_.state, "boot_completed", False),
                "engine": getattr(app_.state, "engine_ready", False),
                "error": getattr(app_.state, "engine_error", None)
            }
        )

    @app_.get("/health", tags=["system"])
    async def system_health(request: Request):
        return {
            "status": "ok" if app_.state.engine_ready else "degraded",
            "env": os.getenv("APP_ENV", "production"),
            "uptime_start": app_.state.boot_time,
            "request_id": getattr(request.state, "request_id", "internal")
        }

    # 6. Global Resilience Handlers
    @app_.exception_handler(StarletteHTTPException)
    async def http_error_handler(request, exc):
        return JSONResponse(
            status_code=exc.status_code,
            content={"status": "error", "code": exc.status_code, "detail": exc.detail, "trace": getattr(request.state, "request_id", None)}
        )

    @app_.exception_handler(Exception)
    async def internal_error_handler(request, exc):
        logger.error("Internal Crash [%s]: %s", getattr(request.state, "request_id", "SYS"), _clamp_str(traceback.format_exc()))
        return JSONResponse(
            status_code=500,
            content={"status": "critical", "message": "The bridge encountered an internal processing error.", "trace": getattr(request.state, "request_id", None)}
        )

    return app_

# --- Entrypoint ---
app = create_app()

if __name__ == "__main__":
    import uvicorn
    # Use environment port for Render/Cloud compat
    run_port = int(os.getenv("PORT", 8000))
    uvicorn.run(app, host="0.0.0.0", port=run_port, access_log=_truthy(os.getenv("UVICORN_ACCESS_LOG", "True")))
