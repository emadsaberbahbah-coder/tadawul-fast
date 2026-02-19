#!/usr/bin/env python3
"""
main.py
===========================================================
TADAWUL FAST BRIDGE – ENTERPRISE FASTAPI ENTRY POINT (v7.5.0)
===========================================================
Mission Critical Production Server with Advanced Orchestration

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
• Memory pressure management with automatic GC
• Degraded mode operation under load
• Comprehensive error handling with request IDs
• API versioning and deprecation management
• WebSocket support for real-time updates
• Background task management
• Cache warming and preloading strategies
• A/B testing framework integration
• Feature flags with gradual rollout

Architecture
------------
┌─────────────────────────────────────────────────────────┐
│                     FastAPI Application                  │
├─────────────────────────────────────────────────────────┤
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐    │
│  │   CORS      │  │    GZip     │  │   Trusted   │    │
│  │ Middleware  │  │  Middleware │  │    Hosts    │    │
│  └─────────────┘  └─────────────┘  └─────────────┘    │
├─────────────────────────────────────────────────────────┤
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐    │
│  │  Telemetry  │  │  Rate Limit │  │   Circuit   │    │
│  │  Middleware │  │  Middleware │  │   Breaker   │    │
│  └─────────────┘  └─────────────┘  └─────────────┘    │
├─────────────────────────────────────────────────────────┤
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐    │
│  │  Prometheus │  │    Tracing  │  │  Compression│    │
│  │   Metrics   │  │  Middleware │  │  Middleware │    │
│  └─────────────┘  └─────────────┘  └─────────────┘    │
├─────────────────────────────────────────────────────────┤
│  ┌─────────────────────────────────────────────────┐   │
│  │            System Guardian (Background)         │   │
│  │   • Memory Monitoring  • GC Management          │   │
│  │   • CPU Tracking      • Degraded Mode           │   │
│  └─────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────┘

Exit Codes
----------
0: Clean shutdown
1: Configuration error
2: Runtime error
3: Worker timeout
4: Health check failed
5: Resource exhaustion
130: Interrupted by user

Environment Variables
---------------------
# Core
APP_NAME: str = "Tadawul Fast Bridge"
APP_VERSION: str = "dev"
APP_ENV: str = "production"
PORT: int = 8000

# Logging
LOG_LEVEL: str = "INFO"
LOG_JSON: bool = False
LOG_FORMAT: str = "detailed"
LOG_DATEFMT: str = "%H:%M:%S"

# Security
AUTH_HEADER_NAME: str = "X-APP-TOKEN"
APP_TOKEN: Optional[str] = None
BACKUP_APP_TOKEN: Optional[str] = None
ALLOW_QUERY_TOKEN: bool = False
ENABLE_CORS_ALL_ORIGINS: bool = True
CORS_ORIGINS: str = ""
ALLOWED_HOSTS: str = "*"

# Rate Limiting
ENABLE_RATE_LIMITING: bool = True
MAX_REQUESTS_PER_MINUTE: int = 240
RATE_LIMIT_STRATEGY: str = "fixed-window"

# Performance
WORKER_COUNT: int = 1
MAX_RETRIES: int = 3
HTTP_TIMEOUT_SEC: float = 45.0
KEEPALIVE_TIMEOUT: int = 65
GRACEFUL_TIMEOUT: int = 30

# Memory Management
GC_PRESSURE_MB: int = 420
GC_COOLDOWN_SEC: int = 60
MEMORY_WARNING_PERCENT: float = 0.8
MEMORY_CRITICAL_PERCENT: float = 0.95

# Degraded Mode
DEGRADED_ENTER_LAG_MS: int = 500
DEGRADED_EXIT_LAG_MS: int = 150
DEGRADED_SHED_ENABLED: bool = True
DEGRADED_SHED_PATHS: str = "/v1/analysis,/v1/advanced,/v1/advisor"

# Feature Flags
ENABLE_SWAGGER: bool = True
ENABLE_REDOC: bool = True
ENABLE_METRICS: bool = True
ENABLE_TRACING: bool = False
ENABLE_PROFILING: bool = False
DEFER_ROUTER_MOUNT: bool = False
INIT_ENGINE_ON_BOOT: bool = True
ENGINE_REQUIRED_FOR_READYZ: bool = False

# Tracing
TRACING_SAMPLE_RATE: float = 0.1
TRACING_EXPORTER: str = "console"
OTLP_ENDPOINT: Optional[str] = None

# Prometheus
PROMETHEUS_MULTIPROC_DIR: Optional[str] = None
PROMETHEUS_NAMESPACE: str = "tfb"

# Cache
CACHE_TTL_SEC: int = 300
CACHE_MAX_SIZE: int = 10000
CACHE_BACKEND: str = "memory"

# Background Tasks
MAX_BACKGROUND_TASKS: int = 100
TASK_TIMEOUT_SEC: int = 300

# A/B Testing
AB_TESTING_ENABLED: bool = False
AB_TESTING_CONFIG_PATH: Optional[str] = None

# Feature Flags
FEATURE_FLAGS_PATH: Optional[str] = None
FEATURE_FLAGS_REFRESH_SEC: int = 60

# WebSocket
WEBSOCKET_MAX_SIZE: int = 1024 * 1024  # 1MB
WEBSOCKET_MAX_QUEUE: int = 32
WEBSOCKET_PING_INTERVAL: int = 20
WEBSOCKET_PING_TIMEOUT: int = 30
"""

from __future__ import annotations

import asyncio
import contextlib
import contextvars
import functools
import gc
import hashlib
import hmac
import inspect
import json
import logging
import logging.config
import logging.handlers
import os
import random
import signal
import socket
import sys
import time
import traceback
import uuid
import warnings
from collections import defaultdict, deque
from contextlib import asynccontextmanager, contextmanager
from datetime import datetime, timedelta, timezone
from enum import Enum
from functools import lru_cache, wraps
from importlib import import_module
from pathlib import Path
from threading import Lock, Thread
from typing import (Any, AsyncGenerator, Awaitable, Callable, Dict, List,
                    Optional, Set, Tuple, Type, TypeVar, Union, cast)

import fastapi
from fastapi import (BackgroundTasks, Depends, FastAPI, File, Form, Header,
                     HTTPException, Query, Request, Response, UploadFile,
                     WebSocket, WebSocketDisconnect)
from fastapi.datastructures import State
from fastapi.exceptions import RequestValidationError
from fastapi.middleware.cors import CORSMiddleware
from fastapi.middleware.gzip import GZipMiddleware
from fastapi.middleware.trustedhost import TrustedHostMiddleware
from fastapi.openapi.docs import get_redoc_html, get_swagger_ui_html
from fastapi.openapi.utils import get_openapi
from fastapi.responses import (FileResponse, HTMLResponse, JSONResponse,
                               PlainTextResponse, RedirectResponse,
                               StreamingResponse)
from fastapi.routing import APIRoute, APIRouter
from starlette.exceptions import HTTPException as StarletteHTTPException
from starlette.middleware.base import BaseHTTPMiddleware, RequestResponseEndpoint
from starlette.requests import Request as StarletteRequest
from starlette.responses import Response as StarletteResponse
from starlette.types import ASGIApp, Receive, Scope, Send

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
    from prometheus_client import (Counter, Gauge, Histogram, Info,
                                   generate_latest, multiprocess)
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
    TRACING_AVAILABLE = True
except ImportError:
    TRACING_AVAILABLE = False

# Redis for distributed rate limiting
try:
    import aioredis
    REDIS_AVAILABLE = True
except ImportError:
    REDIS_AVAILABLE = False

# Sentry for error tracking
try:
    import sentry_sdk
    from sentry_sdk.integrations.asgi import SentryAsgiMiddleware
    from sentry_sdk.integrations.logging import LoggingIntegration
    SENTRY_AVAILABLE = True
except ImportError:
    SENTRY_AVAILABLE = False

# Psutil for system metrics
try:
    import psutil
    PSUTIL_AVAILABLE = True
except ImportError:
    PSUTIL_AVAILABLE = False

# Pydantic for validation
try:
    from pydantic import BaseModel, Field, validator
    PYDANTIC_AVAILABLE = True
except ImportError:
    PYDANTIC_AVAILABLE = False

# Cache backends
try:
    import aiocache
    from aiocache import Cache
    AIOCACHE_AVAILABLE = True
except ImportError:
    AIOCACHE_AVAILABLE = False

# YAML for configs
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

# =============================================================================
# Constants & Enums
# =============================================================================
_TRUTHY = {"1", "true", "yes", "y", "on", "t", "enable", "enabled", "ok", "active"}
_FALSY = {"0", "false", "no", "n", "off", "f", "disable", "disabled"}


class LogLevel(str, Enum):
    """Log levels"""
    DEBUG = "DEBUG"
    INFO = "INFO"
    WARNING = "WARNING"
    ERROR = "ERROR"
    CRITICAL = "CRITICAL"


class Environment(str, Enum):
    """Deployment environments"""
    DEVELOPMENT = "development"
    TESTING = "testing"
    STAGING = "staging"
    PRODUCTION = "production"
    LOCAL = "local"


class ServiceStatus(str, Enum):
    """Service status"""
    STARTING = "starting"
    HEALTHY = "healthy"
    DEGRADED = "degraded"
    UNHEALTHY = "unhealthy"
    SHUTTING_DOWN = "shutting_down"


class CacheBackend(str, Enum):
    """Cache backends"""
    MEMORY = "memory"
    REDIS = "redis"
    MEMCACHED = "memcached"
    NONE = "none"


# =============================================================================
# Utility Functions
# =============================================================================
def strip_value(v: Any) -> str:
    """Strip whitespace from value"""
    try:
        return str(v).strip()
    except Exception:
        return ""


def coerce_bool(v: Any, default: bool = False) -> bool:
    """Coerce value to boolean"""
    if isinstance(v, bool):
        return v
    s = strip_value(v).lower()
    if not s:
        return default
    if s in _TRUTHY:
        return True
    if s in _FALSY:
        return False
    return default


def coerce_int(v: Any, default: int, *, lo: Optional[int] = None, hi: Optional[int] = None) -> int:
    """Coerce value to integer with bounds"""
    try:
        if isinstance(v, (int, float)):
            x = int(v)
        else:
            x = int(float(strip_value(v)))
    except (ValueError, TypeError):
        x = default

    if lo is not None and x < lo:
        x = lo
    if hi is not None and x > hi:
        x = hi
    return x


def coerce_float(v: Any, default: float, *, lo: Optional[float] = None, hi: Optional[float] = None) -> float:
    """Coerce value to float with bounds"""
    try:
        if isinstance(v, (int, float)):
            x = float(v)
        else:
            x = float(strip_value(v))
    except (ValueError, TypeError):
        x = default

    if lo is not None and x < lo:
        x = lo
    if hi is not None and x > hi:
        x = hi
    return x


def coerce_list(v: Any, default: Optional[List[str]] = None) -> List[str]:
    """Coerce value to list"""
    if v is None:
        return default or []

    if isinstance(v, list):
        return [strip_value(x) for x in v if strip_value(x)]

    s = strip_value(v)
    if not s:
        return default or []

    # JSON array
    if s.startswith(("[", "(")):
        try:
            s_clean = s.replace("'", '"')
            parsed = json.loads(s_clean)
            if isinstance(parsed, list):
                return [strip_value(x) for x in parsed if strip_value(x)]
        except:
            pass

    # CSV
    return [x.strip() for x in s.split(",") if x.strip()]


def get_env_bool(key: str, default: bool = False) -> bool:
    """Get boolean from environment"""
    return coerce_bool(os.getenv(key), default)


def get_env_int(key: str, default: int, **kwargs) -> int:
    """Get integer from environment"""
    return coerce_int(os.getenv(key), default, **kwargs)


def get_env_float(key: str, default: float, **kwargs) -> float:
    """Get float from environment"""
    return coerce_float(os.getenv(key), default, **kwargs)


def get_env_str(key: str, default: str = "") -> str:
    """Get string from environment"""
    return strip_value(os.getenv(key)) or default


def get_env_list(key: str, default: Optional[List[str]] = None) -> List[str]:
    """Get list from environment"""
    return coerce_list(os.getenv(key), default)


def now_utc_iso() -> str:
    """Get current UTC time in ISO format"""
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds")


def now_riyadh_iso() -> str:
    """Get current Riyadh time in ISO format"""
    tz = timezone(timedelta(hours=3))
    return datetime.now(tz).isoformat(timespec="milliseconds")


def mask_secret(s: Optional[str], reveal_first: int = 3, reveal_last: int = 3) -> str:
    """Mask secret string for logging"""
    if s is None:
        return "MISSING"
    x = strip_value(s)
    if not x:
        return "EMPTY"
    if len(x) < reveal_first + reveal_last + 4:
        return "***"
    return f"{x[:reveal_first]}...{x[-reveal_last:]}"


def truncate_string(s: str, max_len: int = 5000) -> str:
    """Truncate string to maximum length"""
    if not s or len(s) <= max_len:
        return s
    return s[: max_len - 20] + " ... (truncated)"


def get_rss_mb() -> float:
    """Get RSS memory usage in MB"""
    if PSUTIL_AVAILABLE:
        try:
            process = psutil.Process(os.getpid())
            return process.memory_info().rss / 1024 / 1024
        except Exception:
            pass

    # Linux fallback
    try:
        with open("/proc/self/status", "r") as f:
            for line in f:
                if line.startswith("VmRSS:"):
                    kb = float(line.split()[1])
                    return kb / 1024
    except Exception:
        pass

    return 0.0


def get_cpu_percent() -> float:
    """Get CPU usage percentage"""
    if PSUTIL_AVAILABLE:
        try:
            process = psutil.Process(os.getpid())
            return process.cpu_percent(interval=None)
        except Exception:
            pass
    return 0.0


def get_system_load() -> float:
    """Get system load average"""
    if PSUTIL_AVAILABLE:
        try:
            return psutil.getloadavg()[0]
        except Exception:
            pass

    # Linux fallback
    try:
        with open("/proc/loadavg", "r") as f:
            return float(f.read().split()[0])
    except Exception:
        pass

    return 0.0


def safe_import(module_path: str) -> Optional[Any]:
    """Safely import module without raising"""
    try:
        return import_module(module_path)
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
    """Get current request ID"""
    try:
        return _request_id_ctx.get()
    except Exception:
        return "SYSTEM"


def get_user_id() -> str:
    """Get current user ID"""
    try:
        return _user_id_ctx.get()
    except Exception:
        return "anonymous"


def get_session_id() -> str:
    """Get current session ID"""
    try:
        return _session_id_ctx.get()
    except Exception:
        return ""


@contextmanager
def set_correlation_ids(request_id: str, user_id: str = "anonymous", session_id: str = ""):
    """Context manager for setting correlation IDs"""
    req_token = _request_id_ctx.set(request_id)
    user_token = _user_id_ctx.set(user_id)
    sess_token = _session_id_ctx.set(session_id)
    try:
        yield
    finally:
        _request_id_ctx.reset(req_token)
        _user_id_ctx.reset(user_token)
        _session_id_ctx.reset(sess_token)


# =============================================================================
# Advanced Logging Configuration
# =============================================================================
class RequestIDFilter(logging.Filter):
    """Add request ID to log records"""

    def filter(self, record: logging.LogRecord) -> bool:
        record.request_id = getattr(record, "request_id", None) or get_request_id()
        record.user_id = getattr(record, "user_id", None) or get_user_id()
        return True


class JSONFormatter(logging.Formatter):
    """JSON log formatter"""

    def format(self, record: logging.LogRecord) -> str:
        log_entry = {
            "timestamp": datetime.now(timezone.utc).isoformat(timespec="milliseconds"),
            "level": record.levelname,
            "logger": record.name,
            "request_id": getattr(record, "request_id", "SYSTEM"),
            "user_id": getattr(record, "user_id", "anonymous"),
            "message": record.getMessage(),
            "module": record.module,
            "function": record.funcName,
            "line": record.lineno,
        }

        if record.exc_info:
            log_entry["exception"] = self.formatException(record.exc_info)

        # Add extra attributes
        if hasattr(record, "extra"):
            log_entry.update(record.extra)

        return json.dumps(log_entry, ensure_ascii=False)


class StructuredLogger:
    """Structured logger with context"""

    def __init__(self, name: str):
        self.logger = logging.getLogger(name)

    def _log(self, level: int, msg: str, **kwargs):
        extra = {"extra": kwargs} if kwargs else None
        self.logger.log(level, msg, extra=extra)

    def debug(self, msg: str, **kwargs):
        self._log(logging.DEBUG, msg, **kwargs)

    def info(self, msg: str, **kwargs):
        self._log(logging.INFO, msg, **kwargs)

    def warning(self, msg: str, **kwargs):
        self._log(logging.WARNING, msg, **kwargs)

    def error(self, msg: str, **kwargs):
        self._log(logging.ERROR, msg, **kwargs)

    def critical(self, msg: str, **kwargs):
        self._log(logging.CRITICAL, msg, **kwargs)

    def metric(self, name: str, value: float, tags: Optional[Dict[str, str]] = None):
        """Log a metric"""
        self.info(f"METRIC|{name}|{value}|{json.dumps(tags or {})}")


def configure_logging() -> None:
    """Configure logging based on environment"""
    log_level = get_env_str("LOG_LEVEL", "INFO").upper()
    log_json = get_env_bool("LOG_JSON", False)
    log_format = get_env_str("LOG_FORMAT", "detailed")
    log_datefmt = get_env_str("LOG_DATEFMT", "%H:%M:%S")

    # Ensure valid log level
    if log_level not in ["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"]:
        log_level = "INFO"

    # Configure root logger
    root_logger = logging.getLogger()
    root_logger.setLevel(log_level)

    # Remove existing handlers
    for handler in root_logger.handlers[:]:
        root_logger.removeHandler(handler)

    # Console handler
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(log_level)

    # Add request ID filter
    console_handler.addFilter(RequestIDFilter())

    # Set formatter
    if log_json:
        console_handler.setFormatter(JSONFormatter())
    else:
        # Text formatter with presets
        if log_format in {"detailed", "detail", "default", "prod"}:
            fmt = "%(asctime)s | %(levelname)8s | [%(request_id)s] | %(name)s | %(message)s"
        elif log_format in {"compact", "short"}:
            fmt = "%(asctime)s | %(levelname)s | %(message)s"
        elif log_format in {"simple"}:
            fmt = "%(levelname)s | %(message)s"
        else:
            fmt = log_format

        console_handler.setFormatter(logging.Formatter(fmt, datefmt=log_datefmt))

    root_logger.addHandler(console_handler)

    # File handler if LOG_FILE is set
    log_file = get_env_str("LOG_FILE")
    if log_file:
        try:
            file_handler = logging.handlers.RotatingFileHandler(
                log_file,
                maxBytes=get_env_int("LOG_MAX_BYTES", 100 * 1024 * 1024),
                backupCount=get_env_int("LOG_BACKUP_COUNT", 10)
            )
            file_handler.setLevel(log_level)
            file_handler.addFilter(RequestIDFilter())
            file_handler.setFormatter(console_handler.formatter)
            root_logger.addHandler(file_handler)
        except Exception as e:
            logging.getLogger("boot").error(f"Failed to create log file: {e}")

    # Set levels for noisy libraries
    logging.getLogger("uvicorn.access").setLevel(logging.WARNING)
    logging.getLogger("uvicorn.error").setLevel(logging.ERROR)
    logging.getLogger("asyncio").setLevel(logging.WARNING)


# Initialize logging
configure_logging()
logger = logging.getLogger("main")
boot_logger = logging.getLogger("boot")


# =============================================================================
# System Guardian (Resource Monitoring)
# =============================================================================
class SystemGuardian:
    """Monitors system resources and manages degraded mode"""

    def __init__(self):
        self.running = False
        self.degraded_mode = False
        self.shutting_down = False

        # Metrics
        self.latency_ms: float = 0.0
        self.memory_mb: float = 0.0
        self.cpu_percent: float = 0.0
        self.system_load: float = 0.0
        self.request_count: int = 0
        self.error_count: int = 0
        self.active_connections: int = 0
        self.background_tasks: int = 0

        # Memory management
        self.last_gc_time: float = 0.0
        self.gc_pressure_mb = get_env_int("GC_PRESSURE_MB", 420)
        self.gc_cooldown = get_env_float("GC_COOLDOWN_SEC", 60)

        # Degraded mode thresholds
        self.degraded_enter_lag = get_env_float("DEGRADED_ENTER_LAG_MS", 500)
        self.degraded_exit_lag = get_env_float("DEGRADED_EXIT_LAG_MS", 150)
        self.degraded_memory_mb = get_env_int("DEGRADED_MEMORY_MB", 1024)
        self.degraded_cpu_percent = get_env_float("DEGRADED_CPU_PERCENT", 80)

        # Boot timestamps
        self.boot_time_utc = now_utc_iso()
        self.boot_time_riyadh = now_riyadh_iso()

        # Locks
        self._lock = asyncio.Lock()

    def get_status(self) -> str:
        """Get current system status"""
        if self.shutting_down:
            return "shutting_down"
        if self.degraded_mode:
            return "degraded"
        if self.latency_ms > self.degraded_enter_lag * 0.5:
            return "busy"
        return "healthy"

    def get_load_level(self) -> str:
        """Get load level description"""
        if self.latency_ms > self.degraded_enter_lag:
            return "critical"
        if self.latency_ms > self.degraded_enter_lag * 0.7:
            return "high"
        if self.latency_ms > self.degraded_enter_lag * 0.4:
            return "medium"
        return "low"

    async def should_shed_request(self, path: str) -> bool:
        """Check if request should be shed due to load"""
        if not self.degraded_mode:
            return False

        shed_enabled = get_env_bool("DEGRADED_SHED_ENABLED", True)
        if not shed_enabled:
            return False

        shed_paths = get_env_list("DEGRADED_SHED_PATHS", [
            "/v1/analysis",
            "/v1/advanced",
            "/v1/advisor",
            "/v1/compute"
        ])

        return any(path.startswith(p) for p in shed_paths if p)

    async def check_memory_pressure(self) -> bool:
        """Check if memory pressure requires GC"""
        if self.memory_mb < self.gc_pressure_mb:
            return False

        now = time.time()
        if now - self.last_gc_time < self.gc_cooldown:
            return False

        self.last_gc_time = now
        return True

    async def update_degraded_mode(self):
        """Update degraded mode based on metrics"""
        if self.shutting_down:
            self.degraded_mode = True
            return

        should_degrade = (
            self.latency_ms >= self.degraded_enter_lag or
            self.memory_mb >= self.degraded_memory_mb or
            self.cpu_percent >= self.degraded_cpu_percent
        )

        if should_degrade and not self.degraded_mode:
            self.degraded_mode = True
            boot_logger.warning(
                f"Entering degraded mode: "
                f"lag={self.latency_ms:.1f}ms, "
                f"mem={self.memory_mb:.1f}MB, "
                f"cpu={self.cpu_percent:.1f}%"
            )
        elif not should_degrade and self.degraded_mode:
            if self.latency_ms <= self.degraded_exit_lag:
                self.degraded_mode = False
                boot_logger.info("Exiting degraded mode")

    async def monitor_loop(self):
        """Background monitoring loop"""
        self.running = True

        while self.running and not self.shutting_down:
            try:
                # Update metrics
                self.memory_mb = get_rss_mb()
                self.cpu_percent = get_cpu_percent()
                self.system_load = get_system_load()

                # Check memory pressure
                if await self.check_memory_pressure():
                    boot_logger.info(f"Memory pressure: {self.memory_mb:.1f}MB, running GC")
                    gc.collect()

                # Update degraded mode
                await self.update_degraded_mode()

            except Exception as e:
                boot_logger.error(f"Guardian monitor error: {e}")

            await asyncio.sleep(2.0)

    def stop(self):
        """Stop monitoring"""
        self.running = False
        self.shutting_down = True

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary"""
        return {
            "status": self.get_status(),
            "load_level": self.get_load_level(),
            "degraded_mode": self.degraded_mode,
            "metrics": {
                "latency_ms": round(self.latency_ms, 2),
                "memory_mb": round(self.memory_mb, 2),
                "cpu_percent": round(self.cpu_percent, 2),
                "system_load": round(self.system_load, 2),
                "request_count": self.request_count,
                "error_count": self.error_count,
                "active_connections": self.active_connections,
                "background_tasks": self.background_tasks,
            },
            "boot_time": {
                "utc": self.boot_time_utc,
                "riyadh": self.boot_time_riyadh,
            }
        }


guardian = SystemGuardian()


# =============================================================================
# Metrics Collection (Prometheus)
# =============================================================================
class MetricsCollector:
    """Prometheus metrics collector"""

    def __init__(self):
        self.enabled = PROMETHEUS_AVAILABLE and get_env_bool("ENABLE_METRICS", True)

        if self.enabled:
            # Create metrics
            self.request_count = Counter(
                "http_requests_total",
                "Total HTTP requests",
                ["method", "endpoint", "status"]
            )
            self.request_duration = Histogram(
                "http_request_duration_seconds",
                "HTTP request duration",
                ["method", "endpoint"],
                buckets=(0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10)
            )
            self.request_size = Histogram(
                "http_request_size_bytes",
                "HTTP request size",
                ["method", "endpoint"]
            )
            self.response_size = Histogram(
                "http_response_size_bytes",
                "HTTP response size",
                ["method", "endpoint"]
            )
            self.active_requests = Gauge(
                "http_requests_active",
                "Active HTTP requests"
            )
            self.error_count = Counter(
                "http_errors_total",
                "Total HTTP errors",
                ["method", "endpoint", "error_type"]
            )
            self.cache_hits = Counter(
                "cache_hits_total",
                "Total cache hits",
                ["cache_backend"]
            )
            self.cache_misses = Counter(
                "cache_misses_total",
                "Total cache misses",
                ["cache_backend"]
            )
            self.background_tasks = Gauge(
                "background_tasks_active",
                "Active background tasks"
            )
            self.websocket_connections = Gauge(
                "websocket_connections_active",
                "Active WebSocket connections"
            )
            self.system_info = Info(
                "system_info",
                "System information"
            )
            self.system_info.info({
                "version": APP_ENTRY_VERSION,
                "python_version": sys.version.split()[0],
                "hostname": socket.gethostname(),
            })

    def record_request(self, method: str, endpoint: str, status: int, duration: float,
                       request_size: int = 0, response_size: int = 0):
        """Record request metrics"""
        if not self.enabled:
            return

        endpoint = endpoint or "root"
        self.request_count.labels(method=method, endpoint=endpoint, status=str(status)).inc()
        self.request_duration.labels(method=method, endpoint=endpoint).observe(duration)

        if request_size:
            self.request_size.labels(method=method, endpoint=endpoint).observe(request_size)
        if response_size:
            self.response_size.labels(method=method, endpoint=endpoint).observe(response_size)

    def record_error(self, method: str, endpoint: str, error_type: str):
        """Record error metrics"""
        if not self.enabled:
            return
        self.error_count.labels(method=method, endpoint=endpoint, error_type=error_type).inc()

    def record_cache(self, hit: bool, backend: str = "memory"):
        """Record cache metrics"""
        if not self.enabled:
            return
        if hit:
            self.cache_hits.labels(cache_backend=backend).inc()
        else:
            self.cache_misses.labels(cache_backend=backend).inc()

    async def metrics_endpoint(self) -> Response:
        """Prometheus metrics endpoint"""
        if not self.enabled:
            return Response("Metrics disabled", status_code=404)

        from prometheus_client import generate_latest
        return Response(
            content=generate_latest(),
            media_type="text/plain"
        )


metrics = MetricsCollector()


# =============================================================================
# Circuit Breaker Pattern
# =============================================================================
class CircuitBreaker:
    """Circuit breaker for external service calls"""

    def __init__(self, name: str, failure_threshold: int = 5, recovery_timeout: float = 60.0):
        self.name = name
        self.failure_threshold = failure_threshold
        self.recovery_timeout = recovery_timeout

        self.state = "CLOSED"  # CLOSED, OPEN, HALF_OPEN
        self.failure_count = 0
        self.last_failure_time: Optional[float] = None
        self._lock = asyncio.Lock()

    async def call(self, func: Callable, *args, **kwargs) -> Any:
        """Call function with circuit breaker protection"""
        async with self._lock:
            if self.state == "OPEN":
                if time.time() - (self.last_failure_time or 0) > self.recovery_timeout:
                    self.state = "HALF_OPEN"
                    boot_logger.info(f"Circuit {self.name} moving to HALF_OPEN")
                else:
                    raise Exception(f"Circuit {self.name} is OPEN")

        try:
            result = await func(*args, **kwargs) if asyncio.iscoroutinefunction(func) else func(*args, **kwargs)

            async with self._lock:
                if self.state == "HALF_OPEN":
                    self.state = "CLOSED"
                    self.failure_count = 0
                    boot_logger.info(f"Circuit {self.name} recovered to CLOSED")

            return result

        except Exception as e:
            async with self._lock:
                self.failure_count += 1
                self.last_failure_time = time.time()

                if self.failure_count >= self.failure_threshold:
                    self.state = "OPEN"
                    boot_logger.error(f"Circuit {self.name} OPEN after {self.failure_count} failures")

            raise e


# Circuit breaker registry
_circuit_breakers: Dict[str, CircuitBreaker] = {}


def get_circuit_breaker(name: str) -> CircuitBreaker:
    """Get or create circuit breaker"""
    if name not in _circuit_breakers:
        _circuit_breakers[name] = CircuitBreaker(
            name,
            failure_threshold=get_env_int("CIRCUIT_BREAKER_FAILURES", 5),
            recovery_timeout=get_env_float("CIRCUIT_BREAKER_TIMEOUT", 60.0)
        )
    return _circuit_breakers[name]


# =============================================================================
# Distributed Rate Limiting
# =============================================================================
class RateLimiter:
    """Distributed rate limiter with Redis support"""

    def __init__(self):
        self.redis: Optional[aioredis.Redis] = None
        self.backend = get_env_str("RATE_LIMIT_BACKEND", "memory")
        self.redis_url = get_env_str("REDIS_URL")

        if self.backend == "redis" and self.redis_url and REDIS_AVAILABLE:
            self._init_redis()

    def _init_redis(self):
        """Initialize Redis connection"""
        try:
            self.redis = aioredis.from_url(
                self.redis_url,
                encoding="utf-8",
                decode_responses=True
            )
        except Exception as e:
            boot_logger.error(f"Failed to connect to Redis: {e}")

    async def check_rate_limit(self, key: str, limit: int, window: int) -> Tuple[bool, int]:
        """Check rate limit for key"""
        if self.redis:
            return await self._check_redis(key, limit, window)
        return await self._check_memory(key, limit, window)

    async def _check_redis(self, key: str, limit: int, window: int) -> Tuple[bool, int]:
        """Redis-based rate limiting"""
        if not self.redis:
            return await self._check_memory(key, limit, window)

        try:
            now = time.time()
            pipeline = self.redis.pipeline()
            pipeline.zremrangebyscore(key, 0, now - window)
            pipeline.zadd(key, {str(now): now})
            pipeline.zcard(key)
            pipeline.expire(key, window)
            results = await pipeline.execute()

            current = results[2]
            return current <= limit, current

        except Exception as e:
            boot_logger.error(f"Redis rate limit error: {e}")
            return await self._check_memory(key, limit, window)

    async def _check_memory(self, key: str, limit: int, window: int) -> Tuple[bool, int]:
        """In-memory rate limiting (fallback)"""
        if not hasattr(self, "_memory_store"):
            self._memory_store: Dict[str, List[float]] = {}
            self._memory_lock = asyncio.Lock()

        async with self._memory_lock:
            now = time.time()
            timestamps = self._memory_store.get(key, [])
            timestamps = [t for t in timestamps if t > now - window]
            timestamps.append(now)
            self._memory_store[key] = timestamps[-limit * 2:]  # Keep reasonable size
            return len(timestamps) <= limit, len(timestamps)


rate_limiter = RateLimiter()


# =============================================================================
# Cache Manager
# =============================================================================
class CacheManager:
    """Multi-backend cache manager"""

    def __init__(self):
        self.backend = get_env_str("CACHE_BACKEND", "memory")
        self.ttl = get_env_int("CACHE_TTL_SEC", 300)
        self.max_size = get_env_int("CACHE_MAX_SIZE", 10000)

        self._memory_cache: Dict[str, Tuple[Any, float]] = {}
        self._memory_lock = asyncio.Lock()
        self.redis: Optional[aioredis.Redis] = None

        if self.backend == "redis" and REDIS_AVAILABLE:
            self._init_redis()

    def _init_redis(self):
        """Initialize Redis connection"""
        redis_url = get_env_str("REDIS_URL")
        if redis_url:
            try:
                self.redis = aioredis.from_url(redis_url, encoding="utf-8")
            except Exception as e:
                boot_logger.error(f"Failed to connect to Redis: {e}")

    def _make_key(self, prefix: str, **kwargs) -> str:
        """Generate cache key"""
        content = json.dumps(kwargs, sort_keys=True)
        hash_val = hashlib.sha256(content.encode()).hexdigest()[:16]
        return f"{prefix}:{hash_val}"

    async def get(self, prefix: str, **kwargs) -> Optional[Any]:
        """Get from cache"""
        key = self._make_key(prefix, **kwargs)

        if self.redis:
            try:
                value = await self.redis.get(key)
                if value:
                    metrics.record_cache(True, "redis")
                    return pickle.loads(value) if value else None
            except Exception as e:
                boot_logger.debug(f"Redis get failed: {e}")

        # Memory cache fallback
        async with self._memory_lock:
            if key in self._memory_cache:
                value, timestamp = self._memory_cache[key]
                if time.time() - timestamp < self.ttl:
                    metrics.record_cache(True, "memory")
                    return value
                else:
                    del self._memory_cache[key]

        metrics.record_cache(False, self.backend)
        return None

    async def set(self, value: Any, prefix: str, ttl: Optional[int] = None, **kwargs) -> None:
        """Set in cache"""
        key = self._make_key(prefix, **kwargs)
        ttl = ttl or self.ttl

        # Memory cache
        async with self._memory_lock:
            self._memory_cache[key] = (value, time.time())
            # Prune if too large
            if len(self._memory_cache) > self.max_size:
                oldest = min(self._memory_cache.items(), key=lambda x: x[1][1])[0]
                del self._memory_cache[oldest]

        # Redis cache
        if self.redis:
            try:
                pickled = pickle.dumps(value)
                await self.redis.setex(key, ttl, pickled)
            except Exception as e:
                boot_logger.debug(f"Redis set failed: {e}")

    async def delete(self, prefix: str, **kwargs) -> None:
        """Delete from cache"""
        key = self._make_key(prefix, **kwargs)

        async with self._memory_lock:
            if key in self._memory_cache:
                del self._memory_cache[key]

        if self.redis:
            try:
                await self.redis.delete(key)
            except Exception:
                pass

    async def clear(self, prefix: Optional[str] = None) -> None:
        """Clear cache"""
        async with self._memory_lock:
            if prefix:
                keys = [k for k in self._memory_cache if k.startswith(f"{prefix}:")]
                for k in keys:
                    del self._memory_cache[k]
            else:
                self._memory_cache.clear()

        if self.redis and prefix:
            # Redis pattern delete not implemented for simplicity
            pass


cache = CacheManager()


# =============================================================================
# Background Task Manager
# =============================================================================
class TaskManager:
    """Background task manager"""

    def __init__(self):
        self.tasks: Dict[str, Dict[str, Any]] = {}
        self.max_tasks = get_env_int("MAX_BACKGROUND_TASKS", 100)
        self.task_timeout = get_env_float("TASK_TIMEOUT_SEC", 300)
        self._lock = asyncio.Lock()

    async def submit(self, name: str, func: Callable, *args, **kwargs) -> str:
        """Submit background task"""
        task_id = str(uuid.uuid4())[:8]

        async with self._lock:
            # Clean up completed tasks
            now = time.time()
            self.tasks = {
                tid: info for tid, info in self.tasks.items()
                if not info.get("completed", False) or now - info.get("completed_at", 0) < 60
            }

            if len(self.tasks) >= self.max_tasks:
                raise RuntimeError("Too many background tasks")

            self.tasks[task_id] = {
                "name": name,
                "status": "pending",
                "created_at": time.time(),
            }

        # Create task
        asyncio.create_task(self._run_task(task_id, func, *args, **kwargs))

        return task_id

    async def _run_task(self, task_id: str, func: Callable, *args, **kwargs):
        """Run background task"""
        async with self._lock:
            self.tasks[task_id]["status"] = "running"
            self.tasks[task_id]["started_at"] = time.time()
            guardian.background_tasks += 1

        try:
            # Run with timeout
            if asyncio.iscoroutinefunction(func):
                result = await asyncio.wait_for(func(*args, **kwargs), timeout=self.task_timeout)
            else:
                result = await asyncio.wait_for(
                    asyncio.get_event_loop().run_in_executor(None, func, *args, **kwargs),
                    timeout=self.task_timeout
                )

            async with self._lock:
                self.tasks[task_id]["status"] = "completed"
                self.tasks[task_id]["completed_at"] = time.time()
                self.tasks[task_id]["result"] = str(result)[:200]

        except asyncio.TimeoutError:
            async with self._lock:
                self.tasks[task_id]["status"] = "timeout"
                self.tasks[task_id]["error"] = f"Task timed out after {self.task_timeout}s"

        except Exception as e:
            async with self._lock:
                self.tasks[task_id]["status"] = "failed"
                self.tasks[task_id]["error"] = str(e)

        finally:
            guardian.background_tasks -= 1

    async def get_status(self, task_id: str) -> Optional[Dict[str, Any]]:
        """Get task status"""
        async with self._lock:
            return self.tasks.get(task_id)


task_manager = TaskManager()


# =============================================================================
# A/B Testing Framework
# =============================================================================
class ABTesting:
    """A/B testing framework"""

    def __init__(self):
        self.enabled = get_env_bool("AB_TESTING_ENABLED", False)
        self.config_path = get_env_str("AB_TESTING_CONFIG_PATH")
        self.variants: Dict[str, List[Dict[str, Any]]] = {}
        self._lock = asyncio.Lock()

        if self.enabled and self.config_path:
            self._load_config()

    def _load_config(self):
        """Load A/B test configuration"""
        try:
            path = Path(self.config_path)
            if path.exists():
                if path.suffix in (".yaml", ".yml"):
                    with open(path) as f:
                        config = yaml.safe_load(f)
                elif path.suffix == ".json":
                    with open(path) as f:
                        config = json.load(f)
                else:
                    return

                self.variants = config.get("tests", {})
                boot_logger.info(f"Loaded {len(self.variants)} A/B tests")
        except Exception as e:
            boot_logger.error(f"Failed to load A/B test config: {e}")

    def get_variant(self, test_name: str, user_id: str) -> str:
        """Get variant for user"""
        if not self.enabled or test_name not in self.variants:
            return "control"

        # Deterministic assignment based on user_id
        hash_val = int(hashlib.md5(f"{test_name}:{user_id}".encode()).hexdigest(), 16)
        bucket = hash_val % 100

        # Find variant based on percentages
        cumulative = 0
        for variant in self.variants[test_name]:
            cumulative += variant.get("percentage", 0)
            if bucket < cumulative:
                return variant["name"]

        return "control"

    def get_config(self, test_name: str, variant: str) -> Dict[str, Any]:
        """Get configuration for test variant"""
        if not self.enabled or test_name not in self.variants:
            return {}

        for v in self.variants[test_name]:
            if v["name"] == variant:
                return v.get("config", {})

        return {}


ab_testing = ABTesting()


# =============================================================================
# Feature Flags
# =============================================================================
class FeatureFlags:
    """Feature flag management"""

    def __init__(self):
        self.flags: Dict[str, Dict[str, Any]] = {}
        self.config_path = get_env_str("FEATURE_FLAGS_PATH")
        self.refresh_interval = get_env_int("FEATURE_FLAGS_REFRESH_SEC", 60)
        self.last_refresh: float = 0
        self._lock = asyncio.Lock()

        if self.config_path:
            self._load_flags()

    def _load_flags(self):
        """Load feature flags"""
        try:
            path = Path(self.config_path)
            if path.exists():
                if path.suffix in (".yaml", ".yml"):
                    with open(path) as f:
                        self.flags = yaml.safe_load(f)
                elif path.suffix == ".json":
                    with open(path) as f:
                        self.flags = json.load(f)
                else:
                    return

                boot_logger.info(f"Loaded {len(self.flags)} feature flags")
                self.last_refresh = time.time()
        except Exception as e:
            boot_logger.error(f"Failed to load feature flags: {e}")

    async def refresh(self):
        """Refresh feature flags"""
        if time.time() - self.last_refresh > self.refresh_interval:
            async with self._lock:
                self._load_flags()

    def is_enabled(self, flag: str, user_id: Optional[str] = None) -> bool:
        """Check if feature flag is enabled"""
        if flag not in self.flags:
            return False

        config = self.flags[flag]

        # Global toggle
        if not config.get("enabled", False):
            return False

        # Percentage rollout
        percentage = config.get("percentage", 100)
        if percentage < 100 and user_id:
            hash_val = int(hashlib.md5(f"{flag}:{user_id}".encode()).hexdigest(), 16)
            if hash_val % 100 >= percentage:
                return False

        # Environment restrictions
        env_restrictions = config.get("environments", [])
        if env_restrictions:
            current_env = get_env_str("APP_ENV", "production")
            if current_env not in env_restrictions:
                return False

        return True

    def get_config(self, flag: str) -> Dict[str, Any]:
        """Get feature flag configuration"""
        return self.flags.get(flag, {})


feature_flags = FeatureFlags()


# =============================================================================
# WebSocket Manager
# =============================================================================
class WebSocketManager:
    """WebSocket connection manager"""

    def __init__(self):
        self.active_connections: List[WebSocket] = []
        self.connection_info: Dict[WebSocket, Dict[str, Any]] = {}
        self._lock = asyncio.Lock()

    async def connect(self, websocket: WebSocket, client_id: str):
        """Accept WebSocket connection"""
        await websocket.accept()
        async with self._lock:
            self.active_connections.append(websocket)
            self.connection_info[websocket] = {
                "client_id": client_id,
                "connected_at": time.time(),
                "last_ping": time.time(),
            }
        guardian.active_connections += 1
        metrics.websocket_connections.inc()

    async def disconnect(self, websocket: WebSocket):
        """Disconnect WebSocket"""
        async with self._lock:
            if websocket in self.active_connections:
                self.active_connections.remove(websocket)
            if websocket in self.connection_info:
                del self.connection_info[websocket]
        guardian.active_connections -= 1
        metrics.websocket_connections.dec()

    async def send_personal_message(self, message: dict, websocket: WebSocket):
        """Send message to specific client"""
        try:
            await websocket.send_json(message)
        except Exception:
            await self.disconnect(websocket)

    async def broadcast(self, message: dict):
        """Broadcast to all connected clients"""
        disconnected = []
        for connection in self.active_connections:
            try:
                await connection.send_json(message)
            except Exception:
                disconnected.append(connection)

        for connection in disconnected:
            await self.disconnect(connection)

    async def ping_all(self):
        """Send ping to all connections"""
        for connection in self.active_connections[:]:
            try:
                await connection.send_json({"type": "ping"})
                self.connection_info[connection]["last_ping"] = time.time()
            except Exception:
                await self.disconnect(connection)

    def get_stats(self) -> Dict[str, Any]:
        """Get connection statistics"""
        return {
            "total_connections": len(self.active_connections),
            "connections": [
                {
                    "client_id": info["client_id"],
                    "connected_seconds": time.time() - info["connected_at"],
                    "last_ping_seconds": time.time() - info["last_ping"],
                }
                for info in self.connection_info.values()
            ],
        }


ws_manager = WebSocketManager()


# =============================================================================
# Middleware
# =============================================================================
class TelemetryMiddleware(BaseHTTPMiddleware):
    """Enhanced telemetry middleware"""

    async def dispatch(self, request: Request, call_next: RequestResponseEndpoint) -> Response:
        # Generate request ID
        request_id = request.headers.get("X-Request-ID") or str(uuid.uuid4())[:18]
        user_id = request.headers.get("X-User-ID", "anonymous")
        session_id = request.headers.get("X-Session-ID", "")

        # Set correlation IDs
        token = _request_id_ctx.set(request_id)
        user_token = _user_id_ctx.set(user_id)
        sess_token = _session_id_ctx.set(session_id)

        request.state.request_id = request_id
        request.state.user_id = user_id
        request.state.start_time = time.time()

        # Track metrics
        guardian.request_count += 1
        metrics.active_requests.inc()

        # Check rate limiting
        if SLOWAPI_AVAILABLE and hasattr(request.app.state, "limiter"):
            try:
                await request.app.state.limiter(request)
            except RateLimitExceeded:
                metrics.active_requests.dec()
                guardian.error_count += 1
                return JSONResponse(
                    status_code=429,
                    content={
                        "status": "error",
                        "message": "Rate limit exceeded",
                        "request_id": request_id,
                        "time_riyadh": now_riyadh_iso(),
                    }
                )

        # Check if we should shed this request
        if await guardian.should_shed_request(request.url.path):
            metrics.active_requests.dec()
            return JSONResponse(
                status_code=503,
                content={
                    "status": "degraded",
                    "message": "System under heavy load",
                    "request_id": request_id,
                    "time_riyadh": now_riyadh_iso(),
                }
            )

        # Process request
        try:
            response = await call_next(request)

            # Record metrics
            duration = time.time() - request.state.start_time
            guardian.latency_ms = guardian.latency_ms * 0.9 + duration * 1000 * 0.1

            metrics.record_request(
                method=request.method,
                endpoint=request.url.path,
                status=response.status_code,
                duration=duration,
                request_size=request.headers.get("content-length", 0),
                response_size=response.headers.get("content-length", 0)
            )

            # Add headers
            response.headers["X-Request-ID"] = request_id
            response.headers["X-Process-Time"] = f"{duration:.4f}s"
            response.headers["X-System-Load"] = guardian.get_load_level()
            response.headers["X-Time-UTC"] = now_utc_iso()
            response.headers["X-Time-Riyadh"] = now_riyadh_iso()

            # Security headers
            response.headers["X-Content-Type-Options"] = "nosniff"
            response.headers["X-Frame-Options"] = "DENY"
            response.headers["X-XSS-Protection"] = "1; mode=block"
            response.headers["Referrer-Policy"] = "strict-origin-when-cross-origin"

            if get_env_str("APP_ENV") == "production":
                response.headers["Strict-Transport-Security"] = "max-age=31536000; includeSubDomains"

            return response

        except Exception as e:
            guardian.error_count += 1
            metrics.record_error(
                method=request.method,
                endpoint=request.url.path,
                error_type=type(e).__name__
            )
            raise

        finally:
            metrics.active_requests.dec()
            _request_id_ctx.reset(token)
            _user_id_ctx.reset(user_token)
            _session_id_ctx.reset(sess_token)


class TracingMiddleware(BaseHTTPMiddleware):
    """OpenTelemetry tracing middleware"""

    async def dispatch(self, request: Request, call_next: RequestResponseEndpoint) -> Response:
        if not TRACING_AVAILABLE or not get_env_bool("ENABLE_TRACING", False):
            return await call_next(request)

        tracer = trace.get_tracer(__name__)
        with tracer.start_as_current_span(
            f"{request.method} {request.url.path}",
            kind=trace.SpanKind.SERVER,
            attributes={
                "http.method": request.method,
                "http.url": str(request.url),
                "http.user_agent": request.headers.get("user-agent", ""),
                "request.id": get_request_id(),
            }
        ) as span:
            response = await call_next(request)
            span.set_attribute("http.status_code", response.status_code)
            return response


class CompressionMiddleware(BaseHTTPMiddleware):
    """Enhanced compression middleware"""

    async def dispatch(self, request: Request, call_next: RequestResponseEndpoint) -> Response:
        response = await call_next(request)

        # Only compress text responses
        content_type = response.headers.get("content-type", "")
        if not content_type.startswith(("text/", "application/json", "application/javascript")):
            return response

        # Check if client accepts compression
        accept_encoding = request.headers.get("accept-encoding", "")
        if "gzip" not in accept_encoding:
            return response

        # Check size threshold
        if len(response.body) < 512:  # Don't compress tiny responses
            return response

        # Apply gzip compression
        import gzip
        compressed = gzip.compress(response.body)
        return Response(
            content=compressed,
            status_code=response.status_code,
            headers=dict(response.headers),
            media_type=response.media_type
        )


# =============================================================================
# Engine Bootstrap
# =============================================================================
async def init_engine_resilient(app: FastAPI, max_retries: int = 3) -> None:
    """Initialize data engine with retries"""
    app.state.engine_ready = False
    app.state.engine_error = None

    for attempt in range(1, max_retries + 1):
        try:
            engine_module = safe_import("core.data_engine_v2")
            if not engine_module or not hasattr(engine_module, "get_engine"):
                app.state.engine_error = "Engine module not available"
                boot_logger.warning(app.state.engine_error)
                return

            get_engine = getattr(engine_module, "get_engine")
            engine = await get_engine() if asyncio.iscoroutinefunction(get_engine) else get_engine()

            if engine:
                app.state.engine = engine
                app.state.engine_ready = True
                app.state.engine_error = None
                boot_logger.info(f"Data Engine ready: {type(engine).__name__}")
                return

            raise RuntimeError("Engine returned None")

        except Exception as e:
            boot_logger.warning(f"Engine boot attempt {attempt}/{max_retries} failed: {e}")
            if attempt < max_retries:
                await asyncio.sleep(1.5 * attempt)

    app.state.engine_ready = False
    app.state.engine_error = "CRITICAL: Engine failed to initialize"
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
    """Get route inventory"""
    routes = []
    for route in app.routes:
        if isinstance(route, APIRoute):
            routes.append({
                "path": route.path,
                "name": route.name,
                "methods": sorted(route.methods),
            })
    return routes


def mount_routers_once(app: FastAPI) -> Dict[str, Any]:
    """Mount routers idempotently"""
    if not hasattr(app.state, "mounted_router_modules"):
        app.state.mounted_router_modules = set()

    report = {"mounted": [], "failed": []}

    for label, candidates in ROUTER_PLAN:
        mounted = False
        last_error = ""

        for module_path in candidates:
            if module_path in app.state.mounted_router_modules:
                mounted = True
                break

            module = safe_import(module_path)
            if not module:
                continue

            router = getattr(module, "router", None) or getattr(module, "api_router", None)
            if not router:
                continue

            try:
                app.include_router(router)
                app.state.mounted_router_modules.add(module_path)
                boot_logger.info(f"✅ Mounted {label} via {module_path}")
                report["mounted"].append({"label": label, "module": module_path})
                mounted = True
                break
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
    """Lazy router mounting"""
    if getattr(app.state, "routers_mounted", False):
        return

    lock = getattr(app.state, "router_mount_lock", None)
    if lock is None:
        lock = asyncio.Lock()
        app.state.router_mount_lock = lock

    async with lock:
        if getattr(app.state, "routers_mounted", False):
            return
        mount_routers_once(app)


# =============================================================================
# Sentry Integration
# =============================================================================
def setup_sentry(app: FastAPI) -> None:
    """Setup Sentry error tracking"""
    sentry_dsn = get_env_str("SENTRY_DSN")
    if not SENTRY_AVAILABLE or not sentry_dsn:
        return

    try:
        sentry_sdk.init(
            dsn=sentry_dsn,
            environment=get_env_str("APP_ENV", "production"),
            release=get_env_str("APP_VERSION", "dev"),
            traces_sample_rate=get_env_float("SENTRY_TRACES_SAMPLE_RATE", 0.1),
            integrations=[
                LoggingIntegration(
                    level=logging.INFO,
                    event_level=logging.ERROR
                ),
            ],
        )
        app.add_middleware(SentryAsgiMiddleware)
        boot_logger.info("Sentry initialized")
    except Exception as e:
        boot_logger.error(f"Failed to initialize Sentry: {e}")


# =============================================================================
# OpenTelemetry Tracing
# =============================================================================
def setup_tracing(app: FastAPI) -> None:
    """Setup OpenTelemetry tracing"""
    if not TRACING_AVAILABLE or not get_env_bool("ENABLE_TRACING", False):
        return

    try:
        resource = Resource(attributes={
            SERVICE_NAME: get_env_str("APP_NAME", "tadawul-fast-bridge"),
        })

        provider = TracerProvider(resource=resource)

        # Configure exporter
        exporter_type = get_env_str("TRACING_EXPORTER", "console")
        if exporter_type == "otlp" and get_env_str("OTLP_ENDPOINT"):
            exporter = OTLPSpanExporter(endpoint=get_env_str("OTLP_ENDPOINT"))
        else:
            from opentelemetry.sdk.trace.export import ConsoleSpanExporter
            exporter = ConsoleSpanExporter()

        processor = BatchSpanProcessor(exporter)
        provider.add_span_processor(processor)
        trace.set_tracer_provider(provider)

        # Instrument FastAPI
        FastAPIInstrumentor.instrument_app(app)

        boot_logger.info("OpenTelemetry tracing initialized")
    except Exception as e:
        boot_logger.error(f"Failed to initialize tracing: {e}")


# =============================================================================
# Prometheus Metrics
# =============================================================================
def setup_metrics(app: FastAPI) -> None:
    """Setup Prometheus metrics endpoint"""
    if not PROMETHEUS_AVAILABLE or not get_env_bool("ENABLE_METRICS", True):
        return

    @app.get("/metrics", include_in_schema=False)
    async def metrics_endpoint():
        return await metrics.metrics_endpoint()

    boot_logger.info("Prometheus metrics endpoint mounted at /metrics")


# =============================================================================
# Configuration Loading
# =============================================================================
def load_config() -> Dict[str, Any]:
    """Load configuration from various sources"""
    config = {}

    # Try config file
    config_path = get_env_str("CONFIG_PATH")
    if config_path:
        path = Path(config_path)
        if path.exists():
            try:
                if path.suffix in (".yaml", ".yml"):
                    with open(path) as f:
                        config = yaml.safe_load(f)
                elif path.suffix == ".json":
                    with open(path) as f:
                        config = json.load(f)
                boot_logger.info(f"Loaded config from {config_path}")
            except Exception as e:
                boot_logger.error(f"Failed to load config: {e}")

    return config


# =============================================================================
# App Factory
# =============================================================================
def create_app() -> FastAPI:
    """Create FastAPI application"""
    app_env = get_env_str("APP_ENV", "production")
    app_name = get_env_str("APP_NAME", "Tadawul Fast Bridge")
    app_version = get_env_str("APP_VERSION", APP_ENTRY_VERSION)

    enable_docs = get_env_bool("ENABLE_SWAGGER", True)
    enable_redoc = get_env_bool("ENABLE_REDOC", True)

    # Load configuration
    config = load_config()

    # Rate limiter
    limiter = None
    rpm = None
    if SLOWAPI_AVAILABLE and get_env_bool("ENABLE_RATE_LIMITING", True):
        rpm = get_env_int("MAX_REQUESTS_PER_MINUTE", 240)
        limiter = Limiter(
            key_func=get_remote_address,
            default_limits=[f"{rpm} per minute"],
            storage_uri=get_env_str("REDIS_URL") if get_env_str("RATE_LIMIT_BACKEND") == "redis" else "memory://"
        )

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        # Initialize app state
        app.state.boot_time_utc = now_utc_iso()
        app.state.boot_time_riyadh = now_riyadh_iso()
        app.state.boot_completed = False
        app.state.engine_ready = False
        app.state.engine_error = None
        app.state.routers_mounted = False
        app.state.router_report = {"mounted": [], "failed": []}
        app.state.routes_snapshot = []
        app.state.config = config

        # Start guardian
        guardian_task = asyncio.create_task(guardian.monitor_loop())

        # Background task for WebSocket pings
        async def ws_ping_loop():
            while not guardian.shutting_down:
                await asyncio.sleep(get_env_int("WEBSOCKET_PING_INTERVAL", 20))
                await ws_manager.ping_all()

        ws_ping_task = asyncio.create_task(ws_ping_loop())

        # Background task for feature flag refresh
        async def flag_refresh_loop():
            while not guardian.shutting_down:
                await asyncio.sleep(feature_flags.refresh_interval)
                await feature_flags.refresh()

        flag_refresh_task = asyncio.create_task(flag_refresh_loop())

        # Boot tasks
        async def boot():
            await asyncio.sleep(0)  # Yield control

            defer_mount = get_env_bool("DEFER_ROUTER_MOUNT", False)
            if defer_mount:
                boot_logger.warning("DEFER_ROUTER_MOUNT=True - routers will be lazy-mounted")
            else:
                mount_routers_once(app)
                boot_logger.info(f"Route inventory loaded ({len(app.state.routes_snapshot)} routes)")

            if get_env_bool("INIT_ENGINE_ON_BOOT", True):
                max_retries = get_env_int("MAX_RETRIES", 3)
                await init_engine_resilient(app, max_retries=max_retries)

            gc.collect()
            app.state.boot_completed = True

            boot_logger.info(
                f"🚀 Ready v{app_version} | env={app_env} | "
                f"mem={guardian.memory_mb:.1f}MB | "
                f"routers={'mounted' if app.state.routers_mounted else 'deferred'} | "
                f"engine={'ready' if app.state.engine_ready else 'not-ready'}"
            )

        boot_task = asyncio.create_task(boot())

        yield

        # Shutdown
        boot_logger.info("Shutdown initiated")
        guardian.shutting_down = True

        boot_task.cancel()
        ws_ping_task.cancel()
        flag_refresh_task.cancel()
        guardian.stop()

        try:
            await asyncio.wait_for(guardian_task, timeout=5.0)
        except asyncio.TimeoutError:
            boot_logger.warning("Guardian shutdown timeout")

        # Close engine
        engine = getattr(app.state, "engine", None)
        if engine:
            try:
                if hasattr(engine, "aclose") and asyncio.iscoroutinefunction(engine.aclose):
                    await asyncio.wait_for(engine.aclose(), timeout=5.0)
                elif hasattr(engine, "close"):
                    engine.close()
                boot_logger.info("Engine shutdown complete")
            except Exception as e:
                boot_logger.warning(f"Engine shutdown error: {e}")

        # Close Redis connections
        if rate_limiter.redis:
            await rate_limiter.redis.close()
        if cache.redis:
            await cache.redis.close()

        boot_logger.info("Shutdown complete")

    # Create app
    app = FastAPI(
        title=app_name,
        version=app_version,
        description="Tadawul Fast Bridge API - Enterprise Market Data Platform",
        lifespan=lifespan,
        docs_url="/docs" if enable_docs else None,
        redoc_url="/redoc" if enable_redoc else None,
        openapi_url="/openapi.json" if enable_docs else None,
    )

    # Rate limiting
    if limiter is not None:
        app.state.limiter = limiter
        app.add_exception_handler(RateLimitExceeded, _rate_limit_exceeded_handler)
        if SLOWAPI_AVAILABLE:
            app.add_middleware(SlowAPIMiddleware)

    # Core middleware
    app.add_middleware(TelemetryMiddleware)
    app.add_middleware(TracingMiddleware)
    app.add_middleware(GZipMiddleware, minimum_size=512)
    app.add_middleware(CompressionMiddleware)

    # CORS
    cors_all = get_env_bool("ENABLE_CORS_ALL_ORIGINS", True)
    cors_origins = get_env_list("CORS_ORIGINS")
    if cors_all or not cors_origins:
        allow_origins = ["*"]
    else:
        allow_origins = cors_origins

    app.add_middleware(
        CORSMiddleware,
        allow_origins=allow_origins,
        allow_methods=["*"],
        allow_headers=["*"],
        allow_credentials=False,
        expose_headers=["X-Request-ID", "X-Process-Time", "X-Time-Riyadh"],
        max_age=600,
    )

    # Trusted hosts in production
    if app_env == "production":
        allowed_hosts = get_env_list("ALLOWED_HOSTS", ["*"])
        app.add_middleware(TrustedHostMiddleware, allowed_hosts=allowed_hosts)

    # Setup integrations
    setup_sentry(app)
    setup_tracing(app)
    setup_metrics(app)

    # =========================================================================
    # System Endpoints
    # =========================================================================
    @app.api_route("/", methods=["GET", "HEAD"], include_in_schema=False)
    async def root():
        """Root endpoint"""
        return {
            "status": "online",
            "service": app_name,
            "version": app_version,
            "environment": app_env,
            "time_riyadh": now_riyadh_iso(),
            "documentation": "/docs",
        }

    @app.get("/robots.txt", include_in_schema=False)
    async def robots():
        """Robots.txt"""
        return PlainTextResponse("User-agent: *\nDisallow: /\n")

    @app.get("/favicon.ico", include_in_schema=False)
    async def favicon():
        """Favicon"""
        return PlainTextResponse("", status_code=204)

    @app.get("/readyz", include_in_schema=False)
    async def readiness():
        """Readiness probe"""
        boot_ok = getattr(app.state, "boot_completed", False)
        routers_ok = getattr(app.state, "routers_mounted", False) or get_env_bool("DEFER_ROUTER_MOUNT", False)
        engine_required = get_env_bool("ENGINE_REQUIRED_FOR_READYZ", False)
        engine_ok = getattr(app.state, "engine_ready", False) or not engine_required

        ready = boot_ok and routers_ok and engine_ok

        return JSONResponse(
            status_code=200 if ready else 503,
            content={
                "ready": ready,
                "boot_completed": boot_ok,
                "routers_mounted": getattr(app.state, "routers_mounted", False),
                "engine_ready": getattr(app.state, "engine_ready", False),
                "engine_error": getattr(app.state, "engine_error", None),
                "load": guardian.get_load_level(),
                "time_riyadh": now_riyadh_iso(),
            }
        )

    @app.get("/healthz", include_in_schema=False)
    async def healthz():
        """Health check"""
        return {
            "status": "ok",
            "time_utc": now_utc_iso(),
            "time_riyadh": now_riyadh_iso(),
        }

    @app.get("/health", tags=["system"])
    async def health(request: Request):
        """Detailed health check"""
        engine_ready = getattr(app.state, "engine_ready", False)
        guardian_dict = guardian.to_dict()

        return {
            "status": "healthy" if engine_ready and not guardian.degraded_mode else "degraded",
            "service": {
                "name": app_name,
                "version": app_version,
                "environment": app_env,
            },
            "vitals": guardian_dict["metrics"],
            "system": {
                "status": guardian_dict["status"],
                "load_level": guardian_dict["load_level"],
                "degraded_mode": guardian_dict["degraded_mode"],
            },
            "boot": {
                "boot_time_utc": getattr(app.state, "boot_time_utc", None),
                "boot_time_riyadh": getattr(app.state, "boot_time_riyadh", None),
                "boot_completed": getattr(app.state, "boot_completed", False),
                "routers_mounted": getattr(app.state, "routers_mounted", False),
                "routers_mounted_at_utc": getattr(app.state, "routers_mounted_at_utc", None),
                "engine_ready": engine_ready,
                "engine_error": getattr(app.state, "engine_error", None),
            },
            "routers": getattr(app.state, "router_report", {"mounted": [], "failed": []}),
            "routes_count": len(getattr(app.state, "routes_snapshot", [])),
            "rate_limit": {
                "enabled": limiter is not None,
                "rpm": rpm,
            },
            "cache": {
                "backend": cache.backend,
                "ttl": cache.ttl,
            },
            "websocket": ws_manager.get_stats(),
            "background_tasks": {
                "active": guardian.background_tasks,
                "max": task_manager.max_tasks,
            },
            "time": {
                "utc": now_utc_iso(),
                "riyadh": now_riyadh_iso(),
            },
            "trace_id": get_request_id(),
        }

    @app.get("/system/info", tags=["system"])
    async def system_info():
        """System information"""
        return {
            "status": "ok",
            "settings": {
                "app": {
                    "name": app_name,
                    "version": app_version,
                    "environment": app_env,
                },
                "auth": {
                    "tokens_present": bool(get_env_str("APP_TOKEN") or get_env_str("BACKUP_APP_TOKEN")),
                    "allow_query_token": get_env_bool("ALLOW_QUERY_TOKEN", False),
                },
                "logging": {
                    "level": get_env_str("LOG_LEVEL", "INFO"),
                    "json": get_env_bool("LOG_JSON", False),
                    "format": get_env_str("LOG_FORMAT", "detailed"),
                },
                "features": {
                    "rate_limiting": get_env_bool("ENABLE_RATE_LIMITING", True),
                    "swagger": get_env_bool("ENABLE_SWAGGER", True),
                    "metrics": get_env_bool("ENABLE_METRICS", True),
                    "tracing": get_env_bool("ENABLE_TRACING", False),
                    "defer_router_mount": get_env_bool("DEFER_ROUTER_MOUNT", False),
                },
            },
            "time_riyadh": now_riyadh_iso(),
        }

    @app.get("/system/routes", tags=["system"])
    async def system_routes():
        """List all routes"""
        routes = route_inventory(app)
        return {
            "status": "ok",
            "count": len(routes),
            "routes": routes,
            "time_riyadh": now_riyadh_iso(),
        }

    @app.post("/system/mount-routers", tags=["system"])
    async def mount_routers_now():
        """Mount routers immediately"""
        await ensure_routers_mounted(app)
        return {
            "status": "success",
            "mounted": app.state.router_report.get("mounted", []),
            "failed": app.state.router_report.get("failed", []),
            "routes": len(app.state.routes_snapshot or []),
            "time_riyadh": now_riyadh_iso(),
        }

    @app.get("/system/cache/stats", tags=["system"])
    async def cache_stats():
        """Get cache statistics"""
        return {
            "backend": cache.backend,
            "ttl": cache.ttl,
            "max_size": cache.max_size,
            "memory_size": len(cache._memory_cache) if hasattr(cache, "_memory_cache") else 0,
            "redis_connected": cache.redis is not None,
        }

    @app.delete("/system/cache", tags=["system"])
    async def clear_cache(prefix: Optional[str] = None):
        """Clear cache"""
        await cache.clear(prefix)
        return {"status": "ok", "cleared": True, "prefix": prefix}

    @app.post("/system/tasks", tags=["system"])
    async def submit_task(name: str = Body(...), payload: Dict[str, Any] = Body(...)):
        """Submit background task"""
        try:
            task_id = await task_manager.submit(name, lambda: process_task(name, payload))
            return {"status": "ok", "task_id": task_id}
        except Exception as e:
            raise HTTPException(status_code=400, detail=str(e))

    @app.get("/system/tasks/{task_id}", tags=["system"])
    async def get_task_status(task_id: str):
        """Get task status"""
        status = await task_manager.get_status(task_id)
        if not status:
            raise HTTPException(status_code=404, detail="Task not found")
        return status

    # =========================================================================
    # WebSocket Endpoints
    # =========================================================================
    @app.websocket("/ws")
    async def websocket_endpoint(websocket: WebSocket):
        """WebSocket connection"""
        client_id = websocket.query_params.get("client_id", str(uuid.uuid4())[:8])
        await ws_manager.connect(websocket, client_id)

        try:
            while True:
                data = await websocket.receive_json()
                await ws_manager.send_personal_message({
                    "type": "echo",
                    "data": data,
                    "time_riyadh": now_riyadh_iso(),
                }, websocket)

        except WebSocketDisconnect:
            await ws_manager.disconnect(websocket)

    @app.websocket("/ws/subscribe/{symbol}")
    async def websocket_subscribe(websocket: WebSocket, symbol: str):
        """Subscribe to symbol updates"""
        await ws_manager.connect(websocket, f"subscriber:{symbol}")

        try:
            # Send initial data
            await websocket.send_json({
                "type": "subscribed",
                "symbol": symbol,
                "time_riyadh": now_riyadh_iso(),
            })

            # Keep connection alive
            while True:
                data = await websocket.receive_text()
                # Handle subscription messages
                if data == "ping":
                    await websocket.send_json({"type": "pong"})

        except WebSocketDisconnect:
            await ws_manager.disconnect(websocket)

    # =========================================================================
    # Error Handlers
    # =========================================================================
    @app.exception_handler(StarletteHTTPException)
    async def http_exception_handler(request: Request, exc: StarletteHTTPException):
        """Handle HTTP exceptions"""
        logger.warning(
            f"HTTP {exc.status_code} on {request.url.path}: {exc.detail}",
            extra={
                "request_id": get_request_id(),
                "status_code": exc.status_code,
                "path": request.url.path,
            }
        )

        return JSONResponse(
            status_code=exc.status_code,
            content={
                "status": "error",
                "message": exc.detail,
                "request_id": get_request_id(),
                "time_riyadh": now_riyadh_iso(),
            },
        )

    @app.exception_handler(RequestValidationError)
    async def validation_exception_handler(request: Request, exc: RequestValidationError):
        """Handle validation errors"""
        logger.warning(
            f"Validation error on {request.url.path}: {exc.errors()}",
            extra={
                "request_id": get_request_id(),
                "path": request.url.path,
                "errors": exc.errors(),
            }
        )

        return JSONResponse(
            status_code=422,
            content={
                "status": "error",
                "message": "Validation error",
                "details": exc.errors(),
                "request_id": get_request_id(),
                "time_riyadh": now_riyadh_iso(),
            },
        )

    @app.exception_handler(Exception)
    async def unhandled_exception_handler(request: Request, exc: Exception):
        """Handle unhandled exceptions"""
        logger.error(
            f"Unhandled exception on {request.url.path}: {traceback.format_exc()}",
            extra={
                "request_id": get_request_id(),
                "path": request.url.path,
                "error_type": type(exc).__name__,
            }
        )

        return JSONResponse(
            status_code=500,
            content={
                "status": "critical",
                "message": "Internal server error",
                "request_id": get_request_id(),
                "time_riyadh": now_riyadh_iso(),
            },
        )

    return app


# Create app instance
app = create_app()

# =============================================================================
# Main Entry Point
# =============================================================================
if __name__ == "__main__":
    import uvicorn

    port = get_env_int("PORT", 8000, lo=1024, hi=65535)
    host = get_env_str("HOST", "0.0.0.0")
    workers = get_env_int("WORKER_COUNT", 1)

    log_level = get_env_str("LOG_LEVEL", "info").lower()

    uvicorn.run(
        "main:app",
        host=host,
        port=port,
        workers=workers,
        log_level=log_level,
        proxy_headers=True,
        forwarded_allow_ips="*",
        timeout_keep_alive=get_env_int("KEEPALIVE_TIMEOUT", 65),
        timeout_graceful_shutdown=get_env_int("GRACEFUL_TIMEOUT", 30),
        access_log=get_env_bool("UVICORN_ACCESS_LOG", False),
        server_header=False,
        date_header=True,
    )
