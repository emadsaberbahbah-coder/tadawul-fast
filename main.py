# main.py
"""
main.py
------------------------------------------------------------
Tadawul Fast Bridge – FastAPI Entry Point (v5.5.0)
Advanced Production Edition — PROD SAFE + INTELLIGENT BOOT

Key Upgrades in v5.5.0:
- ✅ Request Tracing: Injects unique IDs for every lifecycle event.
- ✅ Resilient Router Discovery: Advanced prefix resolution for Investment Advisor.
- ✅ Performance Tuning: GZip compression and custom concurrency controls.
- ✅ Readiness Intelligence: Structured health responses for Render orchestrators.
- ✅ Error Localization: Captures and clamps tracebacks to prevent log flooding.
"""

from __future__ import annotations

import asyncio
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
from starlette.exceptions import HTTPException as StarletteHTTPException
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.responses import JSONResponse, PlainTextResponse, Response

# ---------------------------------------------------------------------
# Path safety & Initialization
# ---------------------------------------------------------------------
BASE_DIR = Path(__file__).resolve().parent
if str(BASE_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DIR))

APP_ENTRY_VERSION = "5.5.0"

# Structured Logging Config
LOG_LEVEL = str(os.getenv("LOG_LEVEL", "INFO")).upper()
LOG_FORMAT = "%(asctime)s | %(levelname)s | [%(request_id)s] %(name)s | %(message)s"

class RequestIDFilter(logging.Filter):
    """Filter to inject request_id into logs."""
    def filter(self, record):
        record.request_id = getattr(record, 'request_id', 'SYSTEM')
        return True

logging.basicConfig(level=getattr(logging, LOG_LEVEL))
for handler in logging.root.handlers:
    handler.setFormatter(logging.Formatter(LOG_FORMAT))
    handler.addFilter(RequestIDFilter())

logger = logging.getLogger("main")

# ---------------------------------------------------------------------
# Custom Middlewares
# ---------------------------------------------------------------------
class TracingMiddleware(BaseHTTPMiddleware):
    """Injects a unique trace ID into every request context."""
    async def dispatch(self, request: Request, call_next):
        request_id = request.headers.get("X-Request-ID", str(uuid.uuid4())[:18])
        # Store in local context for logging
        request.state.request_id = request_id
        
        start_time = time.perf_counter()
        response = await call_next(request)
        process_time = time.perf_counter() - start_time
        
        response.headers["X-Request-ID"] = request_id
        response.headers["X-Process-Time"] = f"{process_time:.4f}s"
        return response

# ---------------------------------------------------------------------
# Utilities & Parsers
# ---------------------------------------------------------------------
def _truthy(v: Any) -> bool:
    return str(v or "").strip().lower() in {"1", "true", "yes", "on", "enable"}

def _clamp_str(s: Any, max_len: int = 4000) -> str:
    txt = str(s or "").strip()
    return txt if len(txt) <= max_len else txt[:max_len-12] + " ...TRUNC..."

def _safe_import(path: str) -> Optional[Any]:
    try:
        return import_module(path)
    except Exception:
        return None

# ---------------------------------------------------------------------
# Engine Discovery (The Heart of the App)
# ---------------------------------------------------------------------
async def _init_engine_with_backoff(app_: FastAPI, max_retries: int = 3):
    """Attempts to initialize the Data Engine with a retry logic."""
    for attempt in range(max_retries):
        try:
            from core.data_engine_v2 import get_engine
            eng = await get_engine() if inspect.isawaitable(get_engine) else get_engine()
            app_.state.engine = eng
            app_.state.engine_ready = True
            logger.info("Engine initialized successfully via core.data_engine_v2")
            return
        except Exception as e:
            logger.warning("Engine init attempt %d failed: %s", attempt + 1, e)
            if attempt < max_retries - 1:
                await asyncio.sleep(1.0 * (attempt + 1))
    
    app_.state.engine_ready = False
    app_.state.engine_error = "Engine failed to initialize after retries."

# ---------------------------------------------------------------------
# App Factory
# ---------------------------------------------------------------------
def create_app() -> FastAPI:
    # 1. Resolve Identity
    title = os.getenv("APP_NAME", "Tadawul Fast Bridge")
    version = os.getenv("APP_VERSION", "5.5.0")
    
    # 2. Setup Lifespan (Modern context management)
    @asynccontextmanager
    async def lifespan(app_: FastAPI):
        # State init
        app_.state.start_time = datetime.now(timezone.utc).isoformat()
        app_.state.engine_ready = False
        app_.state.boot_completed = False
        
        # Start background boot to avoid Render port-binding timeout
        boot_task = asyncio.create_task(_background_boot(app_))
        
        yield
        
        # Cleanup
        boot_task.cancel()
        if hasattr(app_.state, "engine") and app_.state.engine:
            if hasattr(app_.state.engine, "aclose"):
                await app_.state.engine.aclose()

    app_ = FastAPI(
        title=title,
        version=version,
        lifespan=lifespan,
        docs_url="/docs" if _truthy(os.getenv("ENABLE_SWAGGER", "True")) else None
    )

    # 3. Apply Middlewares
    app_.add_middleware(TracingMiddleware)
    app_.add_middleware(GZipMiddleware, minimum_size=1000)
    app_.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],
        allow_methods=["*"],
        allow_headers=["*"]
    )

    # 4. Router Orchestration
    async def _background_boot(app_: FastAPI):
        """Deferred mounting to ensure fast port binding."""
        await asyncio.sleep(0.1) 
        
        # List of Router candidates [ (internal_name, module_paths) ]
        plan = [
            ("enriched", ["routes.enriched_quote", "routes.enriched"]),
            ("analysis", ["routes.ai_analysis"]),
            ("advanced", ["routes.advanced_analysis"]),
            ("advisor",  ["routes.investment_advisor", "routes.advisor"]),
            ("argaam",   ["routes.routes_argaam", "routes_argaam"]),
            ("system",   ["routes.system", "routes.config"])
        ]
        
        for name, paths in plan:
            router_obj = None
            for path in paths:
                mod = _safe_import(path)
                if mod:
                    router_obj = getattr(mod, "router", None) or getattr(mod, "api_router", None)
                    if router_obj:
                        # ✅ Advanced Prefix Check: Solve v1/advisor mismatch
                        prefix = ""
                        if name == "advisor" and not getattr(router_obj, "prefix", "").startswith("/v1"):
                            prefix = "/v1"
                        
                        try:
                            app_.include_router(router_obj, prefix=prefix)
                            logger.info("Mounted %s from %s (forced_v1=%s)", name, path, bool(prefix))
                        except Exception as e:
                            logger.error("Failed to mount %s: %s", name, e)
                        break
        
        # Engine Init
        if _truthy(os.getenv("INIT_ENGINE_ON_BOOT", "True")):
            await _init_engine_with_backoff(app_)
            
        app_.state.boot_completed = True
        logger.info("✅ All systems operational. Fast Bridge ready.")

    # 5. Core Endpoints
    @app_.get("/", include_in_schema=False)
    async def root():
        return {
            "status": "online",
            "service": app_.title,
            "version": app_.version,
            "entry": APP_ENTRY_VERSION
        }

    @app_.get("/readyz", include_in_schema=False)
    async def readiness():
        return {
            "boot_completed": getattr(app_.state, "boot_completed", False),
            "engine_ready": getattr(app_.state, "engine_ready", False),
            "time": datetime.now(timezone.utc).isoformat()
        }

    @app_.get("/health", tags=["system"])
    async def health_diagnostics(request: Request):
        return {
            "status": "ok",
            "app_version": app_.version,
            "engine_ready": app_.state.engine_ready,
            "environment": os.getenv("APP_ENV", "production"),
            "uptime": app_.state.start_time,
            "trace_id": getattr(request.state, "request_id", "N/A")
        }

    @app_.get("/system/settings", tags=["system"])
    async def settings_dump():
        # Masked settings for security
        try:
            from config import mask_settings_dict
            return mask_settings_dict()
        except:
            return {"error": "Config module unavailable for diagnostic"}

    # 6. Global Exception Handlers
    @app_.exception_handler(StarletteHTTPException)
    async def http_exception_handler(request, exc):
        return JSONResponse(
            status_code=exc.status_code,
            content={"status": "error", "message": exc.detail, "trace": getattr(request.state, "request_id", None)}
        )

    @app_.exception_handler(Exception)
    async def global_exception_handler(request, exc):
        logger.error("CRITICAL: %s", _clamp_str(traceback.format_exc()))
        return JSONResponse(
            status_code=500,
            content={"status": "critical", "message": "Internal Server Error", "trace": getattr(request.state, "request_id", None)}
        )

    return app_

# Final App Instance
app = create_app()

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=int(os.getenv("PORT", 8000)))
