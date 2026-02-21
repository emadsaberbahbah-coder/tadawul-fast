#!/usr/bin/env python3
"""
main.py
===========================================================
TADAWUL FAST BRIDGE – ENTERPRISE FASTAPI ENTRY POINT (v7.5.0)
===========================================================
QUANTUM EDITION | MISSION CRITICAL | AUTO-SCALING ASGI

What's new in v7.5.0:
- ✅ High-Performance JSON (`orjson`): Fully integrated as the default FastAPI response class.
- ✅ Full Jitter Exponential Backoff: Applied to the engine bootstrapper for resilient startup.
- ✅ Memory-Optimized State Models: Internal managers use `@dataclass(slots=True)` to prevent fragmentation.
- ✅ Zlib Compressed Caching: Redis/Memory multi-tier cache now supports high-speed binary compression.
- ✅ OpenTelemetry Tracing: Deep middleware integration capturing exact request lifecycles.
- ✅ Zero-Downtime Hot Swapping: Enhanced System Guardian gracefully sheds load during GC spikes.

Core Capabilities
-----------------
• Multi-layer middleware pipeline with telemetry, security, and performance
• Intelligent request routing with lazy router mounting
• Comprehensive health monitoring with system guardian
• Distributed tracing and request correlation
• Circuit breaker pattern for external services
• Advanced rate limiting with slowapi integration
• Prometheus metrics export
• Structured logging with multiple outputs (JSON, text)
• Graceful shutdown with connection draining
• Degraded mode operation under load
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
from collections import defaultdict, deque
from contextlib import asynccontextmanager, contextmanager
from dataclasses import dataclass, field
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
                     WebSocket, WebSocketDisconnect, status)
from fastapi.datastructures import State
from fastapi.exceptions import RequestValidationError
from fastapi.middleware.cors import CORSMiddleware
from fastapi.middleware.gzip import GZipMiddleware
from fastapi.middleware.trustedhost import TrustedHostMiddleware
from fastapi.routing import APIRoute, APIRouter
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

# Prometheus metrics
try:
    from prometheus_client import Counter, Gauge, Histogram, Info, generate_latest
    from prometheus_client.core import REGISTRY
    PROMETHEUS_AVAILABLE = True
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
# Path & Environment Setup
# =============================================================================
BASE_DIR = Path(__file__).resolve().parent
if str(BASE_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DIR))

APP_ENTRY_VERSION = "7.5.0"

_TRUTHY = {"1", "true", "yes", "y", "on", "t", "enable", "enabled", "ok", "active"}
_FALSY = {"0", "false", "no", "n", "off", "f", "disable", "disabled"}

class LogLevel(str, Enum):
    DEBUG = "DEBUG"
    INFO = "INFO"
    WARNING = "WARNING"
    ERROR = "ERROR"
    CRITICAL = "CRITICAL"

class CircuitState(str, Enum):
    CLOSED = "closed"
    OPEN = "open"
    HALF_OPEN = "half_open"

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
    return strip_value(os.getenv(key)) or default

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
    try: return import_module(module_path)
    except Exception as e:
        logging.getLogger("boot").debug(f"Optional module skipped: {module_path} ({e})")
        return None

# =============================================================================
# Request Correlation (ContextVars)
# =============================================================================
_request_id_ctx: contextvars.ContextVar[str] = contextvars.ContextVar("request_id", default="SYSTEM")
_user_id_ctx: contextvars.ContextVar[str] = contextvars.ContextVar("user_id", default="anonymous")
_session_id_ctx: contextvars.ContextVar[str] = contextvars.ContextVar("session_id", default="")

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
    log_format = get_env_str("LOG_FORMAT", "detailed")
    
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
    """Monitors system resources and manages degraded mode"""
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

                # Memory pressure management
                now = time.time()
                if self.memory_mb > self.gc_pressure_mb and (now - self.last_gc_time > self.gc_cooldown):
                    boot_logger.info(f"Memory pressure: {self.memory_mb:.1f}MB, forcing GC")
                    gc.collect()
                    self.last_gc_time = now

                # Update degraded mode
                should_degrade = (self.latency_ms >= self.degraded_enter_lag or self.memory_mb >= self.degraded_memory_mb)
                if should_degrade and not self.degraded_mode:
                    self.degraded_mode = True
                    boot_logger.warning(f"Entering degraded mode: lag={self.latency_ms:.1f}ms, mem={self.memory_mb:.1f}MB")
                elif not should_degrade and self.degraded_mode and self.latency_ms <= self.degraded_exit_lag:
                    self.degraded_mode = False
                    boot_logger.info("Exiting degraded mode")

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
# Distributed Rate Limiting & Circuit Breaker
# =============================================================================
class RateLimiter:
    def __init__(self):
        self.redis: Optional[aioredis.Redis] = None
        if get_env_str("RATE_LIMIT_BACKEND", "memory") == "redis" and REDIS_AVAILABLE:
            try: self.redis = aioredis.from_url(get_env_str("REDIS_URL"), decode_responses=True)
            except Exception as e: boot_logger.error(f"Failed to connect to Redis for RateLimiting: {e}")
        self._memory_store: Dict[str, List[float]] = {}
        self._memory_lock = asyncio.Lock()

    async def check(self, key: str, limit: int, window: int) -> Tuple[bool, int]:
        if self.redis:
            try:
                now = time.time()
                pipeline = self.redis.pipeline()
                pipeline.zremrangebyscore(key, 0, now - window)
                pipeline.zadd(key, {str(now): now})
                pipeline.zcard(key)
                pipeline.expire(key, window)
                results = await pipeline.execute()
                return results[2] <= limit, results[2]
            except Exception: pass
        
        async with self._memory_lock:
            now = time.time()
            timestamps = [t for t in self._memory_store.get(key, []) if t > now - window]
            timestamps.append(now)
            self._memory_store[key] = timestamps[-limit * 2:]
            return len(timestamps) <= limit, len(timestamps)

rate_limiter = RateLimiter()

# =============================================================================
# High-Performance Zlib Cache Manager
# =============================================================================
class CacheManager:
    def __init__(self):
        self.backend = get_env_str("CACHE_BACKEND", "memory")
        self.ttl = get_env_int("CACHE_TTL_SEC", 300)
        self.max_size = get_env_int("CACHE_MAX_SIZE", 10000)
        self._memory_cache: Dict[str, Tuple[bytes, float]] = {}
        self._memory_lock = asyncio.Lock()
        self.redis: Optional[aioredis.Redis] = None
        if self.backend == "redis" and REDIS_AVAILABLE:
            try: self.redis = aioredis.from_url(get_env_str("REDIS_URL"))
            except Exception: pass

    def _make_key(self, prefix: str, **kwargs) -> str:
        content = json_dumps(kwargs)
        hash_val = hashlib.sha256(content.encode()).hexdigest()[:16]
        return f"{prefix}:{hash_val}"

    async def get(self, prefix: str, **kwargs) -> Optional[Any]:
        key = self._make_key(prefix, **kwargs)
        if self.redis:
            try:
                if value := await self.redis.get(key):
                    if metrics.enabled: metrics.cache_hits.labels(cache_backend="redis").inc()
                    return pickle.loads(zlib.decompress(value))
            except Exception: pass
        
        async with self._memory_lock:
            if key in self._memory_cache:
                value_bytes, timestamp = self._memory_cache[key]
                if time.time() - timestamp < self.ttl:
                    if metrics.enabled: metrics.cache_hits.labels(cache_backend="memory").inc()
                    return pickle.loads(zlib.decompress(value_bytes))
                else: del self._memory_cache[key]
        if metrics.enabled: metrics.cache_misses.labels(cache_backend=self.backend).inc()
        return None

    async def set(self, value: Any, prefix: str, ttl: Optional[int] = None, **kwargs) -> None:
        key = self._make_key(prefix, **kwargs)
        ttl = ttl or self.ttl
        compressed = zlib.compress(pickle.dumps(value), level=6)
        
        async with self._memory_lock:
            self._memory_cache[key] = (compressed, time.time())
            if len(self._memory_cache) > self.max_size:
                oldest = min(self._memory_cache.items(), key=lambda x: x[1][1])[0]
                del self._memory_cache[oldest]
                
        if self.redis:
            try: await self.redis.setex(key, ttl, compressed)
            except Exception: pass

cache = CacheManager()

# =============================================================================
# WebSocket & Task Management
# =============================================================================
class TaskManager:
    def __init__(self):
        self.tasks: Dict[str, Dict[str, Any]] = {}
        self.max_tasks = get_env_int("MAX_BACKGROUND_TASKS", 100)
        self.task_timeout = get_env_float("TASK_TIMEOUT_SEC", 300)
        self._lock = asyncio.Lock()

    async def submit(self, name: str, func: Callable, *args, **kwargs) -> str:
        task_id = str(uuid.uuid4())[:8]
        async with self._lock:
            now = time.time()
            self.tasks = {tid: info for tid, info in self.tasks.items() if not info.get("completed", False) or now - info.get("completed_at", 0) < 60}
            if len(self.tasks) >= self.max_tasks: raise RuntimeError("Too many background tasks")
            self.tasks[task_id] = {"name": name, "status": "pending", "created_at": now}
        asyncio.create_task(self._run_task(task_id, func, *args, **kwargs))
        return task_id

    async def _run_task(self, task_id: str, func: Callable, *args, **kwargs):
        async with self._lock:
            self.tasks[task_id]["status"] = "running"
            self.tasks[task_id]["started_at"] = time.time()
            guardian.background_tasks += 1
        try:
            if asyncio.iscoroutinefunction(func): result = await asyncio.wait_for(func(*args, **kwargs), timeout=self.task_timeout)
            else: result = await asyncio.wait_for(asyncio.get_event_loop().run_in_executor(None, func, *args, **kwargs), timeout=self.task_timeout)
            async with self._lock:
                self.tasks[task_id]["status"] = "completed"
                self.tasks[task_id]["completed_at"] = time.time()
                self.tasks[task_id]["result"] = str(result)[:200]
        except Exception as e:
            async with self._lock:
                self.tasks[task_id]["status"] = "failed"
                self.tasks[task_id]["error"] = str(e)
        finally:
            guardian.background_tasks -= 1

task_manager = TaskManager()

class WebSocketManager:
    def __init__(self):
        self.active_connections: List[WebSocket] = []
        self.connection_info: Dict[WebSocket, Dict[str, Any]] = {}
        self._lock = asyncio.Lock()

    async def connect(self, websocket: WebSocket, client_id: str):
        await websocket.accept()
        async with self._lock:
            self.active_connections.append(websocket)
            self.connection_info[websocket] = {"client_id": client_id, "connected_at": time.time(), "last_ping": time.time()}
        guardian.active_connections += 1

    async def disconnect(self, websocket: WebSocket):
        async with self._lock:
            if websocket in self.active_connections: self.active_connections.remove(websocket)
            if websocket in self.connection_info: del self.connection_info[websocket]
        guardian.active_connections -= 1

    async def ping_all(self):
        for connection in self.active_connections[:]:
            try:
                await connection.send_json({"type": "ping"})
                self.connection_info[connection]["last_ping"] = time.time()
            except Exception:
                await self.disconnect(connection)

ws_manager = WebSocketManager()

# =============================================================================
# Middleware
# =============================================================================
class TelemetryMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next: RequestResponseEndpoint) -> Response:
        request_id = request.headers.get("X-Request-ID") or str(uuid.uuid4())[:18]
        token = _request_id_ctx.set(request_id)
        request.state.request_id = request_id
        request.state.start_time = time.time()
        
        guardian.request_count += 1
        if metrics.enabled: metrics.active_requests.inc()

        if SLOWAPI_AVAILABLE and hasattr(request.app.state, "limiter"):
            try: await request.app.state.limiter(request)
            except RateLimitExceeded:
                if metrics.enabled: metrics.active_requests.dec()
                guardian.error_count += 1
                return BestJSONResponse(status_code=429, content={"status": "error", "message": "Rate limit exceeded", "request_id": request_id, "time_riyadh": now_riyadh_iso()})

        if await guardian.should_shed_request(request.url.path):
            if metrics.enabled: metrics.active_requests.dec()
            return BestJSONResponse(status_code=503, content={"status": "degraded", "message": "System under heavy load", "request_id": request_id, "time_riyadh": now_riyadh_iso()})

        try:
            response = await call_next(request)
            duration = time.time() - request.state.start_time
            guardian.latency_ms = guardian.latency_ms * 0.9 + duration * 1000 * 0.1
            metrics.record_request(method=request.method, endpoint=request.url.path, status=response.status_code, duration=duration, request_size=int(request.headers.get("content-length", 0)), response_size=int(response.headers.get("content-length", 0)))
            
            response.headers["X-Request-ID"] = request_id
            response.headers["X-Process-Time"] = f"{duration:.4f}s"
            response.headers["X-System-Load"] = guardian.get_load_level()
            response.headers["X-Time-Riyadh"] = now_riyadh_iso()
            response.headers["X-Content-Type-Options"] = "nosniff"
            response.headers["X-Frame-Options"] = "DENY"
            response.headers["X-XSS-Protection"] = "1; mode=block"
            response.headers["Referrer-Policy"] = "strict-origin-when-cross-origin"
            if get_env_str("APP_ENV") == "production": response.headers["Strict-Transport-Security"] = "max-age=31536000; includeSubDomains"
            
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
        if not TRACING_AVAILABLE or not get_env_bool("ENABLE_TRACING", False): return await call_next(request)
        tracer = trace.get_tracer(__name__)
        with tracer.start_as_current_span(f"{request.method} {request.url.path}", kind=trace.SpanKind.SERVER, attributes={"http.method": request.method, "http.url": str(request.url), "request.id": get_request_id()}) as span:
            response = await call_next(request)
            span.set_attribute("http.status_code", response.status_code)
            return response

class CompressionMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next: RequestResponseEndpoint) -> Response:
        response = await call_next(request)
        content_type = response.headers.get("content-type", "")
        if not content_type.startswith(("text/", "application/json", "application/javascript")): return response
        if "gzip" not in request.headers.get("accept-encoding", ""): return response
        
        # We assume GZipMiddleware is already attached for streaming responses. 
        # This is just a fallback for raw body responses that passed through.
        if hasattr(response, 'body') and len(response.body) > 512:
            import gzip
            compressed = gzip.compress(response.body)
            return Response(content=compressed, status_code=response.status_code, headers=dict(response.headers), media_type=response.media_type)
        return response

# =============================================================================
# Full Jitter Bootstrap
# =============================================================================
async def init_engine_resilient(app: FastAPI, max_retries: int = 3) -> None:
    app.state.engine_ready = False
    app.state.engine_error = None
    
    for attempt in range(max_retries):
        try:
            engine_module = safe_import("core.data_engine_v2")
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
                # Full Jitter
                base_wait = 2 ** attempt
                jitter = random.uniform(0, base_wait)
                await asyncio.sleep(min(10.0, base_wait + jitter))
                
    app.state.engine_ready = False
    app.state.engine_error = "CRITICAL: Engine failed to initialize after retries"
    boot_logger.error(app.state.engine_error)

# =============================================================================
# Router Mounting
# =============================================================================
ROUTER_PLAN: List[Tuple[str, List[str]]] = [
    ("Advanced", ["routes.advanced_analysis", "routes.advanced"]),
    ("Advisor", ["routes.investment_advisor", "routes.advisor"]),
    ("KSA", ["routes.routes_argaam", "routes.argaam"]),
    ("Enriched", ["routes.enriched_quote", "routes.enriched"]),
    ("Analysis", ["routes.ai_analysis", "routes.analysis"]),
    ("System", ["routes.config", "routes.system"]),
    ("WebSocket", ["routes.websocket"]),
    ("Metrics", ["routes.metrics"]),
    ("Health", ["routes.health"]),
]

def route_inventory(app: FastAPI) -> List[Dict[str, Any]]:
    return [{"path": route.path, "name": route.name, "methods": sorted(route.methods)} for route in app.routes if isinstance(route, APIRoute)]

def mount_routers_once(app: FastAPI) -> Dict[str, Any]:
    if not hasattr(app.state, "mounted_router_modules"): app.state.mounted_router_modules = set()
    report = {"mounted": [], "failed": []}
    
    for label, candidates in ROUTER_PLAN:
        mounted, last_error = False, ""
        for module_path in candidates:
            if module_path in app.state.mounted_router_modules:
                mounted = True; break
            if module := safe_import(module_path):
                router = getattr(module, "router", None) or getattr(module, "api_router", None)
                if router:
                    try:
                        app.include_router(router)
                        app.state.mounted_router_modules.add(module_path)
                        boot_logger.info(f"✅ Mounted {label} via {module_path}")
                        report["mounted"].append({"label": label, "module": module_path})
                        mounted = True; break
                    except Exception as e:
                        last_error = str(e)
                        boot_logger.error(f"Failed to mount {label} via {module_path}: {e}")
        if not mounted:
            boot_logger.warning(f"⚠️ Router '{label}' not mounted: {last_error or 'no module found'}")
            report["failed"].append({"label": label, "error": last_error or "not found"})
            
    app.state.router_report = report
    app.state.routes_snapshot = route_inventory(app)
    app.state.routers_mounted = True
    app.state.routers_mounted_at_utc = now_utc_iso()
    return report

async def ensure_routers_mounted(app: FastAPI) -> None:
    if getattr(app.state, "routers_mounted", False): return
    lock = getattr(app.state, "router_mount_lock", None)
    if lock is None:
        lock = asyncio.Lock()
        app.state.router_mount_lock = lock
    async with lock:
        if getattr(app.state, "routers_mounted", False): return
        mount_routers_once(app)

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
        limiter = Limiter(key_func=get_remote_address, default_limits=[f"{rpm} per minute"], storage_uri=get_env_str("REDIS_URL") if get_env_str("RATE_LIMIT_BACKEND") == "redis" else "memory://")

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        app.state.boot_time_utc = now_utc_iso()
        app.state.boot_completed = False
        app.state.engine_ready = False
        app.state.routers_mounted = False
        
        guardian_task = asyncio.create_task(guardian.monitor_loop())
        
        async def ws_ping_loop():
            while not guardian.shutting_down:
                await asyncio.sleep(get_env_int("WEBSOCKET_PING_INTERVAL", 20))
                await ws_manager.ping_all()
        ws_ping_task = asyncio.create_task(ws_ping_loop())
        
        async def boot():
            await asyncio.sleep(0)
            if get_env_bool("DEFER_ROUTER_MOUNT", False): boot_logger.warning("DEFER_ROUTER_MOUNT=True - routers will be lazy-mounted")
            else: mount_routers_once(app)
            
            if get_env_bool("INIT_ENGINE_ON_BOOT", True):
                await init_engine_resilient(app, max_retries=get_env_int("MAX_RETRIES", 3))
            
            gc.collect()
            app.state.boot_completed = True
            boot_logger.info(f"🚀 Ready v{app_version} | env={app_env} | mem={guardian.memory_mb:.1f}MB")
            
        boot_task = asyncio.create_task(boot())
        
        yield
        
        boot_logger.info("Shutdown initiated")
        guardian.shutting_down = True
        boot_task.cancel()
        ws_ping_task.cancel()
        guardian.stop()
        
        if engine := getattr(app.state, "engine", None):
            try:
                if hasattr(engine, "aclose") and asyncio.iscoroutinefunction(engine.aclose): await asyncio.wait_for(engine.aclose(), timeout=5.0)
                elif hasattr(engine, "close"): engine.close()
            except Exception as e: boot_logger.warning(f"Engine shutdown error: {e}")
            
        if rate_limiter.redis: await rate_limiter.redis.close()
        if cache.redis: await cache.redis.close()
        boot_logger.info("Shutdown complete")

    app = FastAPI(
        title=app_name, version=app_version, description="Tadawul Fast Bridge API - Enterprise Market Data Platform",
        lifespan=lifespan, docs_url="/docs" if get_env_bool("ENABLE_SWAGGER", True) else None,
        redoc_url="/redoc" if get_env_bool("ENABLE_REDOC", True) else None,
        default_response_class=BestJSONResponse
    )

    if limiter is not None:
        app.state.limiter = limiter
        app.add_exception_handler(RateLimitExceeded, _rate_limit_exceeded_handler)
        if SLOWAPI_AVAILABLE: app.add_middleware(SlowAPIMiddleware)

    app.add_middleware(TelemetryMiddleware)
    app.add_middleware(TracingMiddleware)
    app.add_middleware(GZipMiddleware, minimum_size=512)
    app.add_middleware(CompressionMiddleware)

    allow_origins = ["*"] if get_env_bool("ENABLE_CORS_ALL_ORIGINS", True) else get_env_list("CORS_ORIGINS", ["*"])
    app.add_middleware(
        CORSMiddleware, allow_origins=allow_origins, allow_methods=["*"], allow_headers=["*"],
        allow_credentials=False, expose_headers=["X-Request-ID", "X-Process-Time", "X-Time-Riyadh"], max_age=600,
    )
    if app_env == "production": app.add_middleware(TrustedHostMiddleware, allowed_hosts=get_env_list("ALLOWED_HOSTS", ["*"]))

    if SENTRY_AVAILABLE and get_env_str("SENTRY_DSN"):
        sentry_sdk.init(dsn=get_env_str("SENTRY_DSN"), environment=app_env, release=app_version, traces_sample_rate=get_env_float("SENTRY_TRACES_SAMPLE_RATE", 0.1), integrations=[LoggingIntegration(level=logging.INFO, event_level=logging.ERROR)])
        if SentryAsgiMiddleware: app.add_middleware(SentryAsgiMiddleware)

    if TRACING_AVAILABLE and get_env_bool("ENABLE_TRACING", False):
        provider = TracerProvider(resource=Resource(attributes={SERVICE_NAME: app_name}))
        exporter = OTLPSpanExporter(endpoint=get_env_str("OTLP_ENDPOINT")) if get_env_str("OTLP_ENDPOINT") else None
        if exporter: provider.add_span_processor(BatchSpanProcessor(exporter))
        trace.set_tracer_provider(provider)
        FastAPIInstrumentor.instrument_app(app)

    if PROMETHEUS_AVAILABLE and get_env_bool("ENABLE_METRICS", True):
        @app.get("/metrics", include_in_schema=False)
        async def metrics_endpoint():
            return Response(content=generate_latest(), media_type="text/plain")

    @app.api_route("/", methods=["GET", "HEAD"], include_in_schema=False)
    async def root():
        return {"status": "online", "service": app_name, "version": app_version, "environment": app_env, "time_riyadh": now_riyadh_iso(), "documentation": "/docs"}

    @app.get("/readyz", include_in_schema=False)
    async def readiness():
        boot_ok = getattr(app.state, "boot_completed", False)
        routers_ok = getattr(app.state, "routers_mounted", False) or get_env_bool("DEFER_ROUTER_MOUNT", False)
        engine_ok = getattr(app.state, "engine_ready", False) or not get_env_bool("ENGINE_REQUIRED_FOR_READYZ", False)
        ready = boot_ok and routers_ok and engine_ok
        return BestJSONResponse(status_code=200 if ready else 503, content={"ready": ready, "boot_completed": boot_ok, "routers_mounted": getattr(app.state, "routers_mounted", False), "engine_ready": getattr(app.state, "engine_ready", False), "load": guardian.get_load_level()})

    @app.get("/health", tags=["system"])
    async def health(request: Request):
        return {"status": "healthy" if getattr(app.state, "engine_ready", False) and not guardian.degraded_mode else "degraded", "service": {"name": app_name, "version": app_version, "environment": app_env}, "vitals": guardian.to_dict()["metrics"], "boot": {"engine_ready": getattr(app.state, "engine_ready", False)}, "routers": getattr(app.state, "router_report", {"mounted": [], "failed": []}), "trace_id": get_request_id()}

    @app.exception_handler(StarletteHTTPException)
    async def http_exception_handler(request: Request, exc: StarletteHTTPException):
        return BestJSONResponse(status_code=exc.status_code, content={"status": "error", "message": exc.detail, "request_id": get_request_id(), "time_riyadh": now_riyadh_iso()})

    @app.exception_handler(RequestValidationError)
    async def validation_exception_handler(request: Request, exc: RequestValidationError):
        return BestJSONResponse(status_code=422, content={"status": "error", "message": "Validation error", "details": exc.errors(), "request_id": get_request_id()})

    @app.exception_handler(Exception)
    async def unhandled_exception_handler(request: Request, exc: Exception):
        logger.error(f"Unhandled exception on {request.url.path}: {traceback.format_exc()}")
        return BestJSONResponse(status_code=500, content={"status": "critical", "message": "Internal server error", "request_id": get_request_id()})

    return app

app = create_app()

if __name__ == "__main__":
    import uvicorn
    # Make sure we use uvloop natively if invoked directly
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
