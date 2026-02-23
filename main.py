#!/usr/bin/env python3
"""
main.py
===========================================================
TADAWUL FAST BRIDGE – ENTERPRISE FASTAPI ENTRY POINT (v8.5.0)
===========================================================
QUANTUM EDITION | MISSION CRITICAL | DIRECT MOUNT

What's new in v8.5.0:
- ✅ Smart Router Discovery: Scans modules for ANY APIRouter instance, preventing failures if variables are renamed (e.g., `advanced_router`).
- ✅ Auto-Prefixing: Automatically applies strict API prefixes (e.g., `/v1/advanced`) if missing from the child router, fixing 404 Not Found errors.
- ✅ Route Debugging: Added `/health` enhancements to show exactly which endpoints are actively listening.
- ✅ Engine Boot Fix: Maintained asyncio.Lock patch for core.data_engine.
"""

from __future__ import annotations

import asyncio
import contextvars
import gc
import hashlib
import inspect
import logging
import logging.config
import logging.handlers
import os
import random
import socket
import sys
import time
import traceback
import uuid
import zlib
import pickle
from urllib.parse import urlparse
from collections import defaultdict, deque
from contextlib import asynccontextmanager, contextmanager
from dataclasses import asdict, dataclass, field
from datetime import datetime, timedelta, timezone
from enum import Enum
from functools import lru_cache, wraps
from importlib import import_module
from pathlib import Path
from threading import Lock, Thread
from typing import (Any, AsyncGenerator, Awaitable, Callable, Dict, List,
                    Optional, Set, Tuple, Type, TypeVar, Union, cast)

import fastapi
from fastapi import (BackgroundTasks, Depends, FastAPI, Header,
                     HTTPException, Query, Request, Response,
                     WebSocket, WebSocketDisconnect, status, APIRouter)
from fastapi.datastructures import State
from fastapi.exceptions import RequestValidationError
from fastapi.middleware.cors import CORSMiddleware
from fastapi.middleware.gzip import GZipMiddleware
from fastapi.middleware.trustedhost import TrustedHostMiddleware
from fastapi.routing import APIRoute
from starlette.exceptions import HTTPException as StarletteHTTPException
from starlette.middleware.base import BaseHTTPMiddleware, RequestResponseEndpoint
from starlette.responses import PlainTextResponse, JSONResponse

# ---------------------------------------------------------------------------
# High-Performance JSON fallback
# ---------------------------------------------------------------------------
try:
    import orjson
    from fastapi.responses import ORJSONResponse as BestJSONResponse
    def json_dumps(v, *, default=None): return orjson.dumps(v, default=default).decode('utf-8')
    def json_loads(v): return orjson.loads(v)
    _HAS_ORJSON = True
except ImportError:
    import json
    from fastapi.responses import JSONResponse as BestJSONResponse
    def json_dumps(v, *, default=None): return json.dumps(v, default=default)
    def json_loads(v): return json.loads(v)
    _HAS_ORJSON = False

# =============================================================================
# Optional Dependencies with Graceful Degradation
# =============================================================================

# Rate limiting
try:
    from slowapi import Limiter, _rate_limit_exceeded_handler
    from slowapi.errors import RateLimitExceeded
    from slowapi.util import get_remote_address
    from slowapi.middleware import SlowAPIMiddleware
    SLOWAPI_AVAILABLE = True
except ImportError:
    SLOWAPI_AVAILABLE = False

# Prometheus metrics & Dynamic Patch for Duplicate Timeseries
try:
    from prometheus_client import Counter, Gauge, Histogram, Info, generate_latest
    from prometheus_client.core import REGISTRY
    from prometheus_client.registry import CollectorRegistry
    PROMETHEUS_AVAILABLE = True
    
    # Monkeypatch Prometheus to ignore duplicate metric registrations
    _original_register = CollectorRegistry.register
    def _safe_register(self, collector):
        try:
            _original_register(self, collector)
        except ValueError as e:
            if "Duplicated timeseries" not in str(e):
                raise
    CollectorRegistry.register = _safe_register
except ImportError:
    PROMETHEUS_AVAILABLE = False

# OpenTelemetry tracing
try:
    from opentelemetry import trace
    from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
    from opentelemetry.instrumentation.fastapi import FastAPIInstrumentor
    from opentelemetry.sdk.trace import TracerProvider
    from opentelemetry.sdk.trace.export import BatchSpanProcessor
    from opentelemetry.sdk.resources import SERVICE_NAME, Resource
    from opentelemetry.trace import Status, StatusCode
    TRACING_AVAILABLE = True
except ImportError:
    TRACING_AVAILABLE = False
    trace = None

# Redis
try:
    import redis.asyncio as aioredis
    REDIS_AVAILABLE = True
except ImportError:
    REDIS_AVAILABLE = False

# Sentry
try:
    import sentry_sdk
    from sentry_sdk.integrations.logging import LoggingIntegration
    try:
        from sentry_sdk.integrations.asgi import SentryAsgiMiddleware
    except ImportError:
        SentryAsgiMiddleware = None
    SENTRY_AVAILABLE = True
except ImportError:
    SENTRY_AVAILABLE = False

# Psutil for system metrics
try:
    import psutil
    PSUTIL_AVAILABLE = True
except ImportError:
    PSUTIL_AVAILABLE = False

try:
    import yaml
    YAML_AVAILABLE = True
except ImportError:
    YAML_AVAILABLE = False

# =============================================================================
# Global Patches
# =============================================================================

# Pre-emptively fix the `_thread.RLock` bug in core.data_engine
try:
    import core.data_engine
    if hasattr(core.data_engine, "_ENGINE_MANAGER"):
        # Replace the synchronous threading.RLock with an asyncio.Lock so 'async with' doesn't crash
        core.data_engine._ENGINE_MANAGER._lock = asyncio.Lock()
except Exception:
    pass

# =============================================================================
# Path & Environment Setup
# =============================================================================
BASE_DIR = Path(__file__).resolve().parent
if str(BASE_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DIR))

APP_ENTRY_VERSION = "8.5.0"

_TRUTHY = {"1", "true", "yes", "y", "on", "t", "enable", "enabled", "ok", "active"}
_FALSY = {"0", "false", "no", "n", "off", "f", "disable", "disabled"}

class LogLevel(str, Enum):
    DEBUG = "DEBUG"
    INFO = "INFO"
    WARNING = "WARNING"
    ERROR = "ERROR"
    CRITICAL = "CRITICAL"

# =============================================================================
# Utility Functions
# =============================================================================

def strip_value(v: Any) -> str:
    try: return str(v).strip()
    except Exception: return ""

def coerce_bool(v: Any, default: bool = False) -> bool:
    if isinstance(v, bool): return v
    s = strip_value(v).lower()
    if not s: return default
    if s in _TRUTHY: return True
    if s in _FALSY: return False
    return default

def get_env_bool(key: str, default: bool = False) -> bool:
    return coerce_bool(os.getenv(key), default)

def get_env_int(key: str, default: int) -> int:
    try: return int(float(os.getenv(key, str(default)).strip()))
    except (ValueError, TypeError): return default

def get_env_float(key: str, default: float) -> float:
    try: return float(os.getenv(key, str(default)).strip())
    except (ValueError, TypeError): return default

def get_env_str(key: str, default: str = "") -> str:
    v = os.getenv(key)
    return str(v).strip() if v is not None and str(v).strip() else default

def get_env_list(key: str, default: Optional[List[str]] = None) -> List[str]:
    v = os.getenv(key)
    if not v: return default or []
    return [x.strip() for x in v.split(",") if x.strip()]

def now_utc_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds")

def now_riyadh_iso() -> str:
    tz = timezone(timedelta(hours=3))
    return datetime.now(tz).isoformat(timespec="milliseconds")

def get_rss_mb() -> float:
    if PSUTIL_AVAILABLE:
        try: return psutil.Process(os.getpid()).memory_info().rss / 1024 / 1024
        except Exception: pass
    try:
        with open("/proc/self/status", "r") as f:
            for line in f:
                if line.startswith("VmRSS:"): return float(line.split()[1]) / 1024
    except Exception: pass
    return 0.0

def get_cpu_percent() -> float:
    if PSUTIL_AVAILABLE:
        try: return psutil.Process(os.getpid()).cpu_percent(interval=None)
        except Exception: pass
    return 0.0

def safe_import(module_path: str) -> Optional[Any]:
    try: 
        return import_module(module_path)
    except ModuleNotFoundError as e:
        # Suppress noisy logs for expected fallback candidates
        if e.name == module_path or e.name in module_path:
            logging.getLogger("boot").debug(f"Router candidate skipped (not found): {module_path}")
        else:
            logging.getLogger("boot").error(f"Missing dependency inside '{module_path}': {e}")
        return None
    except Exception as e:
        logging.getLogger("boot").error(f"❌ Failed to load '{module_path}': {e}\n{traceback.format_exc()}")
        return None

def is_valid_uri(uri: str) -> bool:
    try:
        result = urlparse(uri)
        return all([result.scheme, result.netloc])
    except Exception:
        return False

# =============================================================================
# Request Correlation (ContextVars)
# =============================================================================
_request_id_ctx: contextvars.ContextVar[str] = contextvars.ContextVar("request_id", default="SYSTEM")

def get_request_id() -> str:
    try: return _request_id_ctx.get()
    except Exception: return "SYSTEM"

# =============================================================================
# Advanced Logging Configuration
# =============================================================================
class RequestIDFilter(logging.Filter):
    def filter(self, record: logging.LogRecord) -> bool:
        record.request_id = getattr(record, "request_id", None) or get_request_id()
        return True

class JSONFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        log_entry = {
            "timestamp": datetime.now(timezone.utc).isoformat(timespec="milliseconds"),
            "level": record.levelname,
            "logger": record.name,
            "request_id": getattr(record, "request_id", "SYSTEM"),
            "message": record.getMessage(),
            "module": record.module,
            "function": record.funcName,
        }
        if record.exc_info: log_entry["exception"] = self.formatException(record.exc_info)
        if hasattr(record, "extra"): log_entry.update(record.extra)
        return json_dumps(log_entry)

def configure_logging() -> None:
    log_level = get_env_str("LOG_LEVEL", "INFO").upper()
    log_json = get_env_bool("LOG_JSON", False)
    
    if log_level not in ["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"]: log_level = "INFO"

    root_logger = logging.getLogger()
    root_logger.setLevel(log_level)

    for handler in root_logger.handlers[:]:
        root_logger.removeHandler(handler)

    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(log_level)
    console_handler.addFilter(RequestIDFilter())

    if log_json or _HAS_ORJSON:
        console_handler.setFormatter(JSONFormatter())
    else:
        fmt = "%(asctime)s | %(levelname)8s | [%(request_id)s] | %(name)s | %(message)s"
        console_handler.setFormatter(logging.Formatter(fmt, datefmt="%H:%M:%S"))

    root_logger.addHandler(console_handler)
    logging.getLogger("uvicorn.access").setLevel(logging.WARNING)
    logging.getLogger("asyncio").setLevel(logging.WARNING)

configure_logging()
logger = logging.getLogger("main")
boot_logger = logging.getLogger("boot")

# =============================================================================
# OpenTelemetry Tracing
# =============================================================================
class TraceContext:
    def __init__(self, name: str, attributes: Optional[Dict[str, Any]] = None):
        self.name = name
        self.attributes = attributes or {}
        self.tracer = trace.get_tracer(__name__) if TRACING_AVAILABLE and get_env_bool("ENABLE_TRACING", False) else None
        self.span = None
    
    async def __aenter__(self):
        if self.tracer:
            self.span = self.tracer.start_as_current_span(self.name)
            if self.attributes: self.span.set_attributes(self.attributes)
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self.span and TRACING_AVAILABLE:
            if exc_val:
                self.span.record_exception(exc_val)
                self.span.set_status(Status(StatusCode.ERROR, str(exc_val)))
            self.span.end()

# =============================================================================
# System Guardian (Resource Monitoring)
# =============================================================================
class SystemGuardian:
    def __init__(self):
        self.running = False
        self.degraded_mode = False
        self.shutting_down = False
        self.latency_ms: float = 0.0
        self.memory_mb: float = 0.0
        self.cpu_percent: float = 0.0
        self.request_count: int = 0
        self.error_count: int = 0
        self.active_connections: int = 0
        self.background_tasks: int = 0

        self.last_gc_time: float = 0.0
        self.gc_pressure_mb = get_env_int("GC_PRESSURE_MB", 420)
        self.gc_cooldown = get_env_float("GC_COOLDOWN_SEC", 60)
        self.degraded_enter_lag = get_env_float("DEGRADED_ENTER_LAG_MS", 500)
        self.degraded_exit_lag = get_env_float("DEGRADED_EXIT_LAG_MS", 150)
        self.degraded_memory_mb = get_env_int("DEGRADED_MEMORY_MB", 1024)
        
        self.boot_time_utc = now_utc_iso()
        self.boot_time_riyadh = now_riyadh_iso()

    def get_status(self) -> str:
        if self.shutting_down: return "shutting_down"
        if self.degraded_mode: return "degraded"
        if self.latency_ms > self.degraded_enter_lag * 0.5: return "busy"
        return "healthy"

    def get_load_level(self) -> str:
        if self.latency_ms > self.degraded_enter_lag: return "critical"
        if self.latency_ms > self.degraded_enter_lag * 0.7: return "high"
        if self.latency_ms > self.degraded_enter_lag * 0.4: return "medium"
        return "low"

    def to_dict(self) -> Dict[str, Any]:
        return {
            "status": self.get_status(),
            "load_level": self.get_load_level(),
            "metrics": {
                "latency_ms": round(self.latency_ms, 2),
                "memory_mb": round(self.memory_mb, 2),
                "cpu_percent": round(self.cpu_percent, 2),
                "request_count": self.request_count,
                "error_count": self.error_count,
                "active_connections": self.active_connections,
                "background_tasks": self.background_tasks,
            },
            "boot_time_utc": self.boot_time_utc,
            "boot_time_riyadh": self.boot_time_riyadh,
        }

    async def should_shed_request(self, path: str) -> bool:
        if not self.degraded_mode or not get_env_bool("DEGRADED_SHED_ENABLED", True): return False
        shed_paths = get_env_list("DEGRADED_SHED_PATHS", ["/v1/analysis", "/v1/advanced", "/v1/advisor"])
        return any(path.startswith(p) for p in shed_paths if p)

    async def monitor_loop(self):
        self.running = True
        while self.running and not self.shutting_down:
            try:
                self.memory_mb = get_rss_mb()
                self.cpu_percent = get_cpu_percent()

                now = time.time()
                if self.memory_mb > self.gc_pressure_mb and (now - self.last_gc_time > self.gc_cooldown):
                    boot_logger.info(f"Memory pressure: {self.memory_mb:.1f}MB, forcing GC")
                    gc.collect()
                    self.last_gc_time = now

                should_degrade = (self.latency_ms >= self.degraded_enter_lag or self.memory_mb >= self.degraded_memory_mb)
                if should_degrade and not self.degraded_mode:
                    self.degraded_mode = True
                    boot_logger.warning(f"Entering degraded mode: lag={self.latency_ms:.1f}ms, mem={self.memory_mb:.1f}MB")
                elif not should_degrade and self.degraded_mode and self.latency_ms <= self.degraded_exit_lag:
                    self.degraded_mode = False
                    boot_logger.info("Exiting degraded mode")

            except asyncio.CancelledError:
                break
            except Exception as e:
                boot_logger.error(f"Guardian monitor error: {e}")
            await asyncio.sleep(2.0)

    def stop(self):
        self.running = False
        self.shutting_down = True

guardian = SystemGuardian()

# =============================================================================
# Metrics Collection (Prometheus)
# =============================================================================
class MetricsCollector:
    def __init__(self):
        self.enabled = PROMETHEUS_AVAILABLE and get_env_bool("ENABLE_METRICS", True)
        if self.enabled:
            self.request_count = Counter("http_requests_total", "Total HTTP requests", ["method", "endpoint", "status"])
            self.request_duration = Histogram("http_request_duration_seconds", "HTTP request duration", ["method", "endpoint"], buckets=(0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10))
            self.active_requests = Gauge("http_requests_active", "Active HTTP requests")
            self.error_count = Counter("http_errors_total", "Total HTTP errors", ["method", "endpoint", "error_type"])
            self.cache_hits = Counter("cache_hits_total", "Total cache hits", ["cache_backend"])
            self.cache_misses = Counter("cache_misses_total", "Total cache misses", ["cache_backend"])
            
    def record_request(self, method: str, endpoint: str, status: int, duration: float):
        if not self.enabled: return
        endpoint = endpoint or "root"
        self.request_count.labels(method=method, endpoint=endpoint, status=str(status)).inc()
        self.request_duration.labels(method=method, endpoint=endpoint).observe(duration)

    def record_error(self, method: str, endpoint: str, error_type: str):
        if not self.enabled: return
        self.error_count.labels(method=method, endpoint=endpoint, error_type=error_type).inc()

metrics = MetricsCollector()

# =============================================================================
# Middleware Pipeline
# =============================================================================
class TelemetryMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next: RequestResponseEndpoint) -> Response:
        request_id = request.headers.get("X-Request-ID") or str(uuid.uuid4())[:18]
        token = _request_id_ctx.set(request_id)
        request.state.request_id = request_id
        request.state.start_time = time.time()
        
        guardian.request_count += 1
        if metrics.enabled: metrics.active_requests.inc()

        if await guardian.should_shed_request(request.url.path):
            if metrics.enabled: metrics.active_requests.dec()
            return BestJSONResponse(
                status_code=503,
                content={
                    "status": "degraded",
                    "message": "System under heavy load",
                    "request_id": request_id,
                    "time_riyadh": now_riyadh_iso()
                }
            )

        try:
            response = await call_next(request)
            
            duration = time.time() - request.state.start_time
            guardian.latency_ms = guardian.latency_ms * 0.9 + duration * 1000 * 0.1
            metrics.record_request(method=request.method, endpoint=request.url.path, status=response.status_code, duration=duration)
            
            response.headers["X-Request-ID"] = request_id
            response.headers["X-Process-Time"] = f"{duration:.4f}s"
            response.headers["X-System-Load"] = guardian.get_load_level()
            response.headers["X-Time-Riyadh"] = now_riyadh_iso()
            
            response.headers["Server-Timing"] = f"app;desc=\"FastAPI Processing\";dur={duration*1000:.2f}"
            
            response.headers["X-Content-Type-Options"] = "nosniff"
            response.headers["X-Frame-Options"] = "DENY"
            response.headers["X-XSS-Protection"] = "1; mode=block"
            response.headers["Referrer-Policy"] = "strict-origin-when-cross-origin"
            response.headers["Content-Security-Policy"] = "default-src 'self'; frame-ancestors 'none';"
            
            if get_env_str("APP_ENV") == "production":
                response.headers["Strict-Transport-Security"] = "max-age=31536000; includeSubDomains"
            
            return response
        except Exception as e:
            guardian.error_count += 1
            metrics.record_error(method=request.method, endpoint=request.url.path, error_type=type(e).__name__)
            raise
        finally:
            if metrics.enabled: metrics.active_requests.dec()
            _request_id_ctx.reset(token)

class TracingMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next: RequestResponseEndpoint) -> Response:
        if not TRACING_AVAILABLE or not get_env_bool("ENABLE_TRACING", False): 
            return await call_next(request)
            
        tracer = trace.get_tracer(__name__)
        with tracer.start_as_current_span(
            f"{request.method} {request.url.path}", 
            kind=trace.SpanKind.SERVER, 
            attributes={"http.method": request.method, "http.url": str(request.url), "request.id": get_request_id()}
        ) as span:
            response = await call_next(request)
            span.set_attribute("http.status_code", response.status_code)
            return response

class CompressionMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next: RequestResponseEndpoint) -> Response:
        response = await call_next(request)
        content_type = response.headers.get("content-type", "")
        if not content_type.startswith(("text/", "application/json", "application/javascript")): 
            return response
        if "gzip" not in request.headers.get("accept-encoding", ""): 
            return response
        
        if hasattr(response, 'body') and len(response.body) > 512:
            import gzip
            compressed = gzip.compress(response.body)
            return Response(
                content=compressed, 
                status_code=response.status_code, 
                headers=dict(response.headers), 
                media_type=response.media_type
            )
        return response

# =============================================================================
# Full Jitter Bootstrap
# =============================================================================
async def init_engine_resilient(app: FastAPI, max_retries: int = 3) -> None:
    app.state.engine_ready = False
    app.state.engine_error = None
    
    for attempt in range(max_retries):
        try:
            engine_module = safe_import("core.data_engine") or safe_import("core.data_engine_v2")
            if not engine_module or not hasattr(engine_module, "get_engine"):
                app.state.engine_error = "Engine module not available"
                boot_logger.warning(app.state.engine_error)
                return
                
            get_engine_fn = getattr(engine_module, "get_engine")
            engine = await get_engine_fn() if asyncio.iscoroutinefunction(get_engine_fn) else get_engine_fn()
            
            if engine:
                app.state.engine = engine
                app.state.engine_ready = True
                app.state.engine_error = None
                boot_logger.info(f"Data Engine ready: {type(engine).__name__}")
                return
            raise RuntimeError("Engine returned None")
        except Exception as e:
            boot_logger.warning(f"Engine boot attempt {attempt+1}/{max_retries} failed: {e}")
            if attempt < max_retries - 1:
                base_wait = 2 ** attempt
                jitter = random.uniform(0, base_wait)
                await asyncio.sleep(min(10.0, base_wait + jitter))
                
    app.state.engine_ready = False
    app.state.engine_error = "CRITICAL: Engine failed to initialize after retries"
    boot_logger.error(app.state.engine_error)

# =============================================================================
# Dynamic Router Mounting (V8.5 FIX FOR 404 ERRORS)
# =============================================================================

# Tuple format: (Label, Expected API Prefix, List of possible module paths)
# Enforcing prefixes here guarantees that the tests hit the correct endpoints.
ROUTER_PLAN: List[Tuple[str, str, List[str]]] = [
    ("Advanced", "/v1/advanced", ["routes.advanced_analysis", "routes.advanced", "api.advanced"]),
    ("Advisor", "/v1/advisor", ["routes.advisor", "routes.investment_advisor", "core.investment_advisor"]),
    ("KSA", "/v1/ksa", ["routes.routes_argaam", "routes.argaam"]),
    ("Enriched", "/v1/enriched", ["routes.enriched_quote", "routes.enriched", "core.enriched_quote"]),
    ("Analysis", "/v1/analysis", ["routes.ai_analysis", "routes.analysis"]),
    ("System", "", ["routes.config", "routes.system", "core.legacy_service"]),
    ("WebSocket", "", ["routes.websocket"]),
]

def route_inventory(app: FastAPI) -> List[Dict[str, Any]]:
    return [{"path": route.path, "name": route.name, "methods": sorted(route.methods)} 
            for route in app.routes if isinstance(route, APIRoute)]

def mount_routers_once(app: FastAPI) -> Dict[str, Any]:
    if not hasattr(app.state, "mounted_router_modules"): app.state.mounted_router_modules = set()
    report = {"mounted": [], "failed": []}
    
    for label, expected_prefix, candidates in ROUTER_PLAN:
        mounted, last_error = False, ""
        for module_path in candidates:
            if module_path in app.state.mounted_router_modules:
                mounted = True; break
            
            if module := safe_import(module_path):
                # V8.5 FIX: Dynamically scan for ANY APIRouter instance inside the module
                # This fixes failures where the variable was named `advanced_router` instead of `router`
                found_router = None
                for obj_name in dir(module):
                    obj = getattr(module, obj_name)
                    if isinstance(obj, APIRouter):
                        found_router = obj
                        break
                
                if found_router:
                    try:
                        # V8.5 FIX: Auto-Prefixing. If the developer forgot the prefix, enforce it here!
                        if expected_prefix and not found_router.prefix:
                            app.include_router(found_router, prefix=expected_prefix)
                            applied_prefix = expected_prefix
                        else:
                            # Use existing prefix (or empty if it's meant to be root)
                            app.include_router(found_router)
                            applied_prefix = found_router.prefix or '/'

                        app.state.mounted_router_modules.add(module_path)
                        boot_logger.info(f"✅ Mounted {label} via {module_path} (Prefix: {applied_prefix})")
                        report["mounted"].append({"label": label, "module": module_path, "prefix": applied_prefix})
                        mounted = True; break
                    except Exception as e:
                        last_error = str(e)
                        boot_logger.error(f"Failed to mount {label} via {module_path}: {e}")
                else:
                    last_error = f"Module found, but no APIRouter instance detected inside {module_path}"
                    
        if not mounted:
            boot_logger.warning(f"⚠️ Router '{label}' not mounted: {last_error or 'no module found'}")
            report["failed"].append({"label": label, "error": last_error or "not found"})
            
    app.state.router_report = report
    app.state.routes_snapshot = route_inventory(app)
    app.state.routers_mounted = True
    app.state.routers_mounted_at_utc = now_utc_iso()
    return report

# =============================================================================
# App Factory & Event Handlers
# =============================================================================

def create_app() -> FastAPI:
    app_env = get_env_str("APP_ENV", "production")
    app_name = get_env_str("APP_NAME", "Tadawul Fast Bridge")
    app_version = get_env_str("APP_VERSION", APP_ENTRY_VERSION)

    limiter = None
    if SLOWAPI_AVAILABLE and get_env_bool("ENABLE_RATE_LIMITING", True):
        rpm = get_env_int("MAX_REQUESTS_PER_MINUTE", 240)
        limiter = Limiter(
            key_func=get_remote_address, 
            default_limits=[f"{rpm} per minute"], 
            storage_uri=get_env_str("REDIS_URL") if get_env_str("RATE_LIMIT_BACKEND") == "redis" else "memory://"
        )

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        app.state.boot_time_utc = now_utc_iso()
        app.state.boot_completed = False
        app.state.engine_ready = False
        app.state.routers_mounted = False
        
        guardian_task = asyncio.create_task(guardian.monitor_loop())
        
        async def boot():
            await asyncio.sleep(0) # Yield control
            if get_env_bool("INIT_ENGINE_ON_BOOT", True):
                await init_engine_resilient(app, max_retries=get_env_int("MAX_RETRIES", 3))
            gc.collect()
            app.state.boot_completed = True
            boot_logger.info(f"🚀 Ready v{app_version} | env={app_env} | mem={guardian.memory_mb:.1f}MB")
            
        boot_task = asyncio.create_task(boot())
        
        yield
        
        boot_logger.info("Shutdown initiated. Draining connections...")
        guardian.stop()
        for task in (boot_task, guardian_task):
            task.cancel()
        await asyncio.gather(boot_task, guardian_task, return_exceptions=True)
        
        if engine := getattr(app.state, "engine", None):
            try:
                if hasattr(engine, "aclose") and asyncio.iscoroutinefunction(engine.aclose): 
                    await asyncio.wait_for(engine.aclose(), timeout=5.0)
                elif hasattr(engine, "close"): 
                    engine.close()
            except Exception as e: 
                boot_logger.warning(f"Engine shutdown error: {e}")
        boot_logger.info("Shutdown sequence complete")

    app = FastAPI(
        title=app_name, 
        version=app_version, 
        description="Tadawul Fast Bridge API - Enterprise Market Data Platform",
        lifespan=lifespan, 
        docs_url="/docs" if get_env_bool("ENABLE_SWAGGER", True) else None,
        redoc_url="/redoc" if get_env_bool("ENABLE_REDOC", True) else None,
        default_response_class=BestJSONResponse
    )

    if limiter is not None:
        app.state.limiter = limiter
        app.add_exception_handler(RateLimitExceeded, _rate_limit_exceeded_handler)
        if SLOWAPI_AVAILABLE: 
            app.add_middleware(SlowAPIMiddleware)

    app.add_middleware(TelemetryMiddleware)
    app.add_middleware(TracingMiddleware)
    app.add_middleware(GZipMiddleware, minimum_size=512)
    app.add_middleware(CompressionMiddleware)

    allow_origins = ["*"] if get_env_bool("ENABLE_CORS_ALL_ORIGINS", True) else get_env_list("CORS_ORIGINS", ["*"])
    app.add_middleware(
        CORSMiddleware, 
        allow_origins=allow_origins, 
        allow_methods=["*"], 
        allow_headers=["*"],
        allow_credentials=False, 
        expose_headers=["X-Request-ID", "X-Process-Time", "X-Time-Riyadh", "Server-Timing"], 
        max_age=600,
    )
    if app_env == "production": 
        app.add_middleware(TrustedHostMiddleware, allowed_hosts=get_env_list("ALLOWED_HOSTS", ["*"]))

    sentry_dsn = get_env_str("SENTRY_DSN").strip()
    if SENTRY_AVAILABLE and sentry_dsn and is_valid_uri(sentry_dsn):
        try:
            sentry_sdk.init(
                dsn=sentry_dsn,
                environment=app_env,
                release=app_version,
                traces_sample_rate=get_env_float("SENTRY_TRACES_SAMPLE_RATE", 0.1),
                integrations=[LoggingIntegration(level=logging.INFO, event_level=logging.ERROR)]
            )
            if SentryAsgiMiddleware:
                app.add_middleware(SentryAsgiMiddleware)
        except Exception as e:
            boot_logger.error(f"Failed to initialize Sentry: {e}")

    if TRACING_AVAILABLE and get_env_bool("ENABLE_TRACING", False):
        try:
            provider = TracerProvider(resource=Resource(attributes={SERVICE_NAME: app_name}))
            exporter = OTLPSpanExporter(endpoint=get_env_str("OTLP_ENDPOINT")) if get_env_str("OTLP_ENDPOINT") else None
            if exporter: provider.add_span_processor(BatchSpanProcessor(exporter))
            trace.set_tracer_provider(provider)
            FastAPIInstrumentor.instrument_app(app)
        except Exception as e:
            boot_logger.error(f"Failed to initialize OpenTelemetry: {e}")

    # FORCE MOUNT ROUTERS SYNCHRONOUSLY
    mount_routers_once(app)

    if PROMETHEUS_AVAILABLE and get_env_bool("ENABLE_METRICS", True):
        @app.get("/metrics", include_in_schema=False)
        async def metrics_endpoint():
            return Response(content=generate_latest(), media_type="text/plain")

    @app.api_route("/", methods=["GET", "HEAD"], include_in_schema=False)
    async def root():
        return {
            "status": "online", 
            "service": app_name, 
            "version": app_version, 
            "environment": app_env, 
            "time_riyadh": now_riyadh_iso(), 
            "documentation": "/docs"
        }

    @app.get("/health", tags=["system"])
    async def health(request: Request):
        return {
            "status": "healthy" if getattr(app.state, "engine_ready", False) and not guardian.degraded_mode else "degraded",
            "service": {"name": app_name, "version": app_version, "environment": app_env},
            "vitals": guardian.to_dict()["metrics"],
            "boot": {"engine_ready": getattr(app.state, "engine_ready", False)},
            "routers": getattr(app.state, "router_report", {"mounted": [], "failed": []}),
            "trace_id": get_request_id()
        }

    # V8.5 FEATURE: Debug tool to see exactly what routes the app actually knows about
    @app.get("/debug/routes", tags=["system"])
    async def debug_routes(request: Request):
        return {
            "total_routes": len(app.routes),
            "paths": [route.path for route in app.routes if isinstance(route, APIRoute)]
        }

    @app.exception_handler(StarletteHTTPException)
    async def http_exception_handler(request: Request, exc: StarletteHTTPException):
        return BestJSONResponse(
            status_code=exc.status_code, 
            content={"status": "error", "message": exc.detail, "request_id": get_request_id(), "time_riyadh": now_riyadh_iso()}
        )

    @app.exception_handler(RequestValidationError)
    async def validation_exception_handler(request: Request, exc: RequestValidationError):
        return BestJSONResponse(
            status_code=422, 
            content={"status": "error", "message": "Validation error", "details": exc.errors(), "request_id": get_request_id()}
        )

    @app.exception_handler(Exception)
    async def unhandled_exception_handler(request: Request, exc: Exception):
        logger.error(f"Unhandled exception on {request.url.path}: {traceback.format_exc()}")
        return BestJSONResponse(
            status_code=500, 
            content={"status": "critical", "message": "Internal server error", "request_id": get_request_id()}
        )

    return app


app = create_app()


if __name__ == "__main__":
    import uvicorn
    try:
        import uvloop
        asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
    except ImportError:
        pass
        
    port = get_env_int("PORT", 8000)
    host = get_env_str("HOST", "0.0.0.0")
    workers = get_env_int("WORKER_COUNT", 1)

    uvicorn.run(
        "main:app",
        host=host,
        port=port,
        workers=workers,
        log_level=get_env_str("LOG_LEVEL", "info").lower(),
        proxy_headers=True,
        forwarded_allow_ips="*",
        timeout_keep_alive=get_env_int("KEEPALIVE_TIMEOUT", 65),
        timeout_graceful_shutdown=get_env_int("GRACEFUL_TIMEOUT", 30),
        access_log=get_env_bool("UVICORN_ACCESS_LOG", False),
        server_header=False,
        date_header=True,
    )
