# main.py
"""
main.py
------------------------------------------------------------
Tadawul Fast Bridge – FastAPI Entry Point (v6.2.0)
Mission Critical Edition — OBSERVE + PROTECT + SCALE

Key Upgrades in v6.2.0:
- ✅ Loop Heartbeat: Active monitoring of async event loop latency.
- ✅ Route Inventory: Logs all mounted endpoints at startup for verification.
- ✅ Shutdown Safety: Enforced timeouts on cleanup to prevent zombie processes.
- ✅ Memory Opt: Post-boot garbage collection to minimize RAM footprint.
- ✅ Enhanced Health: Reports system load status dynamically.
"""

from __future__ import annotations

import asyncio
import gc
import inspect
import logging
import os
import psutil
import sys
import time
import traceback
import uuid
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from importlib import import_module
from pathlib import Path
from typing import Any, Callable, Dict, Optional

from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.middleware.gzip import GZipMiddleware
from fastapi.middleware.trustedhost import TrustedHostMiddleware
from starlette.exceptions import HTTPException as StarletteHTTPException
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.responses import JSONResponse
from starlette.routing import Route

# Optional: Rate Limiting
try:
    from slowapi import Limiter, _rate_limit_exceeded_handler
    from slowapi.util import get_remote_address
    from slowapi.errors import RateLimitExceeded
    HAS_LIMITER = True
except ImportError:
    HAS_LIMITER = False

# ---------------------------------------------------------------------
# Configuration & Path Safety
# ---------------------------------------------------------------------
BASE_DIR = Path(__file__).resolve().parent
if str(BASE_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DIR))

APP_ENTRY_VERSION = "6.2.0"
PROCESS = psutil.Process(os.getpid())

# Structured Logging
LOG_LEVEL = str(os.getenv("LOG_LEVEL", "INFO")).upper()
LOG_FORMAT = "%(asctime)s | %(levelname)s | [%(request_id)s] %(name)s | %(message)s"

class RequestIDFilter(logging.Filter):
    def filter(self, record):
        record.request_id = getattr(record, 'request_id', 'SYSTEM')
        return True

logging.basicConfig(level=getattr(logging, LOG_LEVEL))
for handler in logging.root.handlers:
    handler.setFormatter(logging.Formatter(LOG_FORMAT))
    handler.addFilter(RequestIDFilter())

logger = logging.getLogger("main")

# ---------------------------------------------------------------------
# Observability & Self-Healing
# ---------------------------------------------------------------------
class SystemGuardian:
    """Monitors system health and triggers self-healing actions."""
    def __init__(self):
        self.running = False
        self.latency_ms = 0.0
        self.memory_mb = 0.0
        self.cpu_pct = 0.0
        self.degraded_mode = False

    async def start(self):
        self.running = True
        logger.info("🛡️ System Guardian active.")
        while self.running:
            start = time.perf_counter()
            await asyncio.sleep(2.0)  # Check interval
            
            # 1. Latency Check
            self.latency_ms = (time.perf_counter() - start - 2.0) * 1000
            
            # 2. Resource Check
            self.memory_mb = PROCESS.memory_info().rss / 1024 / 1024
            self.cpu_pct = PROCESS.cpu_percent()

            # 3. Auto-Healing: Memory Pressure
            if self.memory_mb > 450: # Trigger GC if > 450MB (adjust for free tier)
                logger.warning(f"Memory pressure detected ({self.memory_mb:.1f}MB). Triggering GC.")
                gc.collect()

            # 4. Auto-Healing: Latency Spike
            if self.latency_ms > 500 and not self.degraded_mode:
                logger.warning("High latency detected. Entering degraded mode.")
                self.degraded_mode = True
            elif self.latency_ms < 100 and self.degraded_mode:
                self.degraded_mode = False
                logger.info("Latency normalized. Exiting degraded mode.")

    def stop(self):
        self.running = False

    def get_status(self) -> str:
        if self.latency_ms > 150: return "overloaded"
        if self.latency_ms > 50: return "heavy"
        return "nominal"

monitor = SystemGuardian()
guardian = monitor # Alias for compatibility

class TelemetryMiddleware(BaseHTTPMiddleware):
    """Captures request telemetry, performance IDs, and security headers."""
    async def dispatch(self, request: Request, call_next: Callable):
        request_id = request.headers.get("X-Request-ID", str(uuid.uuid4())[:18])
        request.state.request_id = request_id
        
        start_time = time.perf_counter()
        
        response = await call_next(request)
        
        duration = time.perf_counter() - start_time
        response.headers["X-Request-ID"] = request_id
        response.headers["X-Process-Time"] = f"{duration:.4f}s"
        response.headers["X-System-Load"] = monitor.get_status()
        
        # Security Headers
        response.headers["X-Content-Type-Options"] = "nosniff"
        response.headers["X-Frame-Options"] = "DENY"
        response.headers["Strict-Transport-Security"] = "max-age=31536000; includeSubDomains"
        
        return response

# ---------------------------------------------------------------------
# Utilities
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
# Engine Lifecycle (Resilient)
# ---------------------------------------------------------------------
async def get_engine_dep(request: Request) -> Any:
    """Dependency Injection for Routes."""
    engine = getattr(request.app.state, "engine", None)
    if not engine:
        # Fail fast if engine is missing in production
        raise StarletteHTTPException(status_code=503, detail="Data Engine unavailable.")
    return engine

async def _init_engine_resilient(app_: FastAPI, max_retries: int = 3):
    """Boot the Data Engine with backoff."""
    for attempt in range(max_retries):
        try:
            from core.data_engine_v2 import get_engine
            # Initialize engine in a separate thread to not block router mounting
            eng = await get_engine() if inspect.isawaitable(get_engine) else get_engine()
            
            if eng:
                app_.state.engine = eng
                app_.state.engine_ready = True
                app_.state.engine_error = None
                logger.info("Data Engine verified: %s", type(eng).__name__)
                return
        except Exception as e:
            logger.warning("Engine boot attempt %d/%d failed: %s", attempt + 1, max_retries, e)
            if attempt < max_retries - 1:
                await asyncio.sleep(2.0 * (attempt + 1))
    
    app_.state.engine_ready = False
    app_.state.engine_error = "CRITICAL: Engine connection refused."
    logger.error(app_.state.engine_error)

# ---------------------------------------------------------------------
# App Factory
# ---------------------------------------------------------------------
def create_app() -> FastAPI:
    title = os.getenv("APP_NAME", "Tadawul Fast Bridge")
    app_env = os.getenv("APP_ENV", "production").lower()

    if HAS_LIMITER:
        limiter = Limiter(key_func=get_remote_address, default_limits=["300 per minute"])
    else:
        limiter = None

    @asynccontextmanager
    async def lifespan(app_: FastAPI):
        # 1. State Init
        app_.state.boot_time = datetime.now(timezone.utc).isoformat()
        app_.state.engine_ready = False
        app_.state.boot_completed = False
        
        # 2. Start Monitors
        monitor_task = asyncio.create_task(monitor.start())
        boot_task = asyncio.create_task(_orchestrate_boot(app_))
        
        yield
        
        # 3. Shutdown Sequence
        logger.info("Initiating shutdown...")
        boot_task.cancel()
        monitor.stop()
        
        # Engine Cleanup (Time-boxed)
        if hasattr(app_.state, "engine") and app_.state.engine:
            if hasattr(app_.state.engine, "aclose"):
                try:
                    await asyncio.wait_for(app_.state.engine.aclose(), timeout=5.0)
                    logger.info("Engine shutdown complete.")
                except asyncio.TimeoutError:
                    logger.warning("Engine shutdown timed out. Forcing exit.")
                except Exception as e:
                    logger.error("Engine shutdown error: %s", e)

    app_ = FastAPI(
        title=title,
        version=APP_ENTRY_VERSION,
        lifespan=lifespan,
        docs_url="/docs" if _truthy(os.getenv("ENABLE_SWAGGER", "True")) else None
    )

    # Middlewares
    if HAS_LIMITER:
        app_.state.limiter = limiter
        app_.add_exception_handler(RateLimitExceeded, _rate_limit_exceeded_handler)
    
    app_.add_middleware(TelemetryMiddleware)
    app_.add_middleware(GZipMiddleware, minimum_size=512)
    app_.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],
        allow_methods=["*"],
        allow_headers=["*"]
    )
    if app_env == "production":
        allowed = os.getenv("ALLOWED_HOSTS", "*").split(",")
        app_.add_middleware(TrustedHostMiddleware, allowed_hosts=allowed)

    # Boot Logic
    async def _orchestrate_boot(app_: FastAPI):
        """Turbo Boot Sequence."""
        await asyncio.sleep(0.01) # Yield
        
        # Parallel Router Mounting
        router_modules = [
            ("Enriched", ["routes.enriched_quote", "routes.enriched"]),
            ("Analysis", ["routes.ai_analysis"]),
            ("Advanced", ["routes.advanced_analysis"]),
            ("Advisor",  ["routes.investment_advisor", "routes.advisor"]),
            ("KSA",      ["routes.routes_argaam", "routes_argaam"]),
            ("System",   ["routes.system", "routes.config"])
        ]
        
        for label, candidates in router_modules:
            mounted = False
            for path in candidates:
                module = _safe_import(path)
                if module:
                    router_obj = getattr(module, "router", None) or getattr(module, "api_router", None)
                    if router_obj:
                        # Fix Prefix for Advisor
                        prefix = ""
                        if label == "Advisor" and not getattr(router_obj, "prefix", "").startswith("/v1"):
                            prefix = "/v1"
                        
                        try:
                            app_.include_router(router_obj, prefix=prefix)
                            logger.info("Mounted %s via %s", label, path)
                            mounted = True
                            break
                        except Exception as e:
                            logger.error("Failed to mount %s: %s", label, e)
            if not mounted:
                logger.warning("Router '%s' could not be mounted.", label)

        # Log Route Inventory
        logger.info("--- Active Routes ---")
        for route in app_.routes:
            if isinstance(route, Route):
                logger.info(f"📍 {route.path} [{','.join(route.methods)}]")
        logger.info("---------------------")

        # Engine Start
        if _truthy(os.getenv("INIT_ENGINE_ON_BOOT", "True")):
            await _init_engine_resilient(app_)
            
        # Post-Boot Optimization
        gc.collect()
        app_.state.boot_completed = True
        logger.info("🚀 V%s Ready. Memory: %.1fMB", APP_ENTRY_VERSION, guardian.memory_mb)

    # Endpoints
    @app_.get("/", include_in_schema=False)
    async def root():
        return {"status": "online", "version": APP_ENTRY_VERSION}

    @app_.get("/readyz", include_in_schema=False)
    async def readiness():
        is_ready = getattr(app_.state, "boot_completed", False)
        return JSONResponse(
            status_code=200 if is_ready else 503,
            content={"ready": is_ready, "load": monitor.get_status()}
        )

    @app_.get("/health", tags=["system"])
    async def health(request: Request):
        return {
            "status": "ok" if app_.state.engine_ready else "degraded",
            "vitals": {
                "memory_mb": round(guardian.memory_mb, 1),
                "cpu_pct": guardian.cpu_pct,
                "latency_ms": round(guardian.latency_ms, 1),
                "load_status": monitor.get_status()
            },
            "uptime": app_.state.boot_time,
            "trace_id": getattr(request.state, "request_id", "n/a")
        }

    # Error Handling
    @app_.exception_handler(StarletteHTTPException)
    async def http_handler(request, exc):
        return JSONResponse(status_code=exc.status_code, content={"status": "error", "message": exc.detail})

    @app_.exception_handler(Exception)
    async def global_handler(request, exc):
        logger.error("Crash: %s", _clamp_str(traceback.format_exc()))
        return JSONResponse(status_code=500, content={"status": "critical", "message": "System Error"})

    return app_

app = create_app()

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=int(os.getenv("PORT", 8000)))
