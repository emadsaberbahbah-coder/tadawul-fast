#!/usr/bin/env python3
"""
main.py
================================================================================
TADAWUL FAST BRIDGE – ENTERPRISE FASTAPI ENTRY POINT (v8.5.3)
================================================================================
QUANTUM EDITION | MISSION CRITICAL | DIRECT MOUNT

v8.5.3 FIXES
- ✅ Fix TraceContext: start_as_current_span returns a context manager; we now enter it correctly
- ✅ Keep Auth Bridge: accept X-APP-TOKEN by mapping to Authorization: Bearer
- ✅ Fix /docs blank: serve Swagger UI from unpkg + relax CSP ONLY for docs/redoc
- ✅ Error responses keep request_id consistent (request.state.request_id)
- ✅ Fix metrics gauge: no double active_requests.dec() on shed path
"""

from __future__ import annotations

import asyncio
import contextvars
import gc
import logging
import os
import random
import sys
import time
import traceback
import uuid
from urllib.parse import urlparse
from contextlib import asynccontextmanager, contextmanager
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from enum import Enum
from importlib import import_module
from pathlib import Path
from typing import Any, Dict, List, Optional

from fastapi import FastAPI, Request, Response, APIRouter
from fastapi.exceptions import RequestValidationError
from fastapi.middleware.cors import CORSMiddleware
from fastapi.middleware.gzip import GZipMiddleware
from fastapi.middleware.trustedhost import TrustedHostMiddleware
from fastapi.routing import APIRoute
from fastapi.openapi.docs import (
    get_swagger_ui_html,
    get_swagger_ui_oauth2_redirect_html,
    get_redoc_html,
)
from starlette.exceptions import HTTPException as StarletteHTTPException
from starlette.middleware.base import BaseHTTPMiddleware, RequestResponseEndpoint
from starlette.responses import JSONResponse, HTMLResponse

# ---------------------------------------------------------------------------
# High-Performance JSON fallback
# ---------------------------------------------------------------------------
try:
    import orjson
    from fastapi.responses import ORJSONResponse as BestJSONResponse

    def json_dumps(v, *, default=None) -> str:
        return orjson.dumps(v, default=default).decode("utf-8")

    _HAS_ORJSON = True
except ImportError:
    import json
    from fastapi.responses import JSONResponse as BestJSONResponse

    def json_dumps(v, *, default=None) -> str:
        return json.dumps(v, default=default)

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
    from prometheus_client import Counter, Gauge, Histogram, generate_latest
    from prometheus_client.registry import CollectorRegistry
    PROMETHEUS_AVAILABLE = True

    # Ignore duplicate registrations
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
    Status = None
    StatusCode = None

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
    SentryAsgiMiddleware = None

# psutil
try:
    import psutil
    PSUTIL_AVAILABLE = True
except ImportError:
    PSUTIL_AVAILABLE = False

# =============================================================================
# Global Patches
# =============================================================================
# Fix possible async lock misuse in core.data_engine (if present)
try:
    import core.data_engine
    if hasattr(core.data_engine, "_ENGINE_MANAGER"):
        core.data_engine._ENGINE_MANAGER._lock = asyncio.Lock()
except Exception:
    pass

# =============================================================================
# Path & Environment Setup
# =============================================================================
BASE_DIR = Path(__file__).resolve().parent
if str(BASE_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DIR))

APP_ENTRY_VERSION = "8.5.3"

_TRUTHY = {"1", "true", "yes", "y", "on", "t", "enable", "enabled", "ok", "active"}
_FALSY = {"0", "false", "no", "n", "off", "f", "disable", "disabled"}


class LogLevel(str, Enum):
    DEBUG = "DEBUG"
    INFO = "INFO"
    WARNING = "WARNING"
    ERROR = "ERROR"
    CRITICAL = "CRITICAL"


def strip_value(v: Any) -> str:
    try:
        return str(v).strip()
    except Exception:
        return ""


def coerce_bool(v: Any, default: bool = False) -> bool:
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


def get_env_bool(key: str, default: bool = False) -> bool:
    return coerce_bool(os.getenv(key), default)


def get_env_int(key: str, default: int) -> int:
    try:
        return int(float(os.getenv(key, str(default)).strip()))
    except (ValueError, TypeError):
        return default


def get_env_float(key: str, default: float) -> float:
    try:
        return float(os.getenv(key, str(default)).strip())
    except (ValueError, TypeError):
        return default


def get_env_str(key: str, default: str = "") -> str:
    v = os.getenv(key)
    return str(v).strip() if v is not None and str(v).strip() else default


def get_env_list(key: str, default: Optional[List[str]] = None) -> List[str]:
    v = os.getenv(key)
    if not v:
        return default or []
    return [x.strip() for x in v.split(",") if x.strip()]


def now_utc_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds")


def now_riyadh_iso() -> str:
    tz = timezone(timedelta(hours=3))
    return datetime.now(tz).isoformat(timespec="milliseconds")


def get_rss_mb() -> float:
    if PSUTIL_AVAILABLE:
        try:
            return psutil.Process(os.getpid()).memory_info().rss / 1024 / 1024
        except Exception:
            pass
    try:
        with open("/proc/self/status", "r") as f:
            for line in f:
                if line.startswith("VmRSS:"):
                    return float(line.split()[1]) / 1024
    except Exception:
        pass
    return 0.0


def safe_import(module_path: str) -> Optional[Any]:
    try:
        return import_module(module_path)
    except ModuleNotFoundError:
        logging.getLogger("boot").debug(f"Router candidate skipped (not found): {module_path}")
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
# Request Correlation
# =============================================================================
_request_id_ctx: contextvars.ContextVar[str] = contextvars.ContextVar("request_id", default="SYSTEM")


def get_request_id() -> str:
    try:
        return _request_id_ctx.get()
    except Exception:
        return "SYSTEM"


# =============================================================================
# Logging
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
        if record.exc_info:
            log_entry["exception"] = self.formatException(record.exc_info)
        return json_dumps(log_entry)


def configure_logging() -> None:
    log_level = get_env_str("LOG_LEVEL", "INFO").upper()
    log_json = get_env_bool("LOG_JSON", False)

    if log_level not in ["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"]:
        log_level = "INFO"

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
# TraceContext (FIXED)
# =============================================================================
class TraceContext:
    """
    Usage:
        async with TraceContext("name", {"k":"v"}):
            ...
    Fix: OpenTelemetry start_as_current_span returns a context manager, not a span.
    """

    def __init__(self, name: str, attributes: Optional[Dict[str, Any]] = None):
        self.name = name
        self.attributes = attributes or {}
        self.enabled = TRACING_AVAILABLE and get_env_bool("ENABLE_TRACING", False)
        self.tracer = trace.get_tracer(__name__) if self.enabled else None
        self._cm = None
        self.span = None

    async def __aenter__(self):
        if not self.tracer:
            return self
        try:
            # context manager -> enter to get the actual span
            self._cm = self.tracer.start_as_current_span(self.name)
            self.span = self._cm.__enter__()
            if self.attributes and self.span:
                for k, v in self.attributes.items():
                    try:
                        self.span.set_attribute(str(k), v)
                    except Exception:
                        pass
        except Exception:
            # never break request due to tracing
            self._cm = None
            self.span = None
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if not self._cm:
            return False
        try:
            if exc_val and self.span and TRACING_AVAILABLE and Status is not None and StatusCode is not None:
                try:
                    self.span.record_exception(exc_val)
                except Exception:
                    pass
                try:
                    self.span.set_status(Status(StatusCode.ERROR, str(exc_val)))
                except Exception:
                    pass
        finally:
            try:
                self._cm.__exit__(exc_type, exc_val, exc_tb)
            except Exception:
                pass
        return False


# =============================================================================
# System Guardian
# =============================================================================
class SystemGuardian:
    def __init__(self):
        self.running = False
        self.degraded_mode = False
        self.shutting_down = False
        self.latency_ms: float = 0.0
        self.memory_mb: float = 0.0
        self.request_count: int = 0
        self.error_count: int = 0

        self.last_gc_time: float = 0.0
        self.gc_pressure_mb = get_env_int("GC_PRESSURE_MB", 420)
        self.gc_cooldown = get_env_float("GC_COOLDOWN_SEC", 60)
        self.degraded_enter_lag = get_env_float("DEGRADED_ENTER_LAG_MS", 500)
        self.degraded_exit_lag = get_env_float("DEGRADED_EXIT_LAG_MS", 150)
        self.degraded_memory_mb = get_env_int("DEGRADED_MEMORY_MB", 1024)

        self.boot_time_utc = now_utc_iso()
        self.boot_time_riyadh = now_riyadh_iso()

    def get_load_level(self) -> str:
        if self.latency_ms > self.degraded_enter_lag:
            return "critical"
        if self.latency_ms > self.degraded_enter_lag * 0.7:
            return "high"
        if self.latency_ms > self.degraded_enter_lag * 0.4:
            return "medium"
        return "low"

    def to_dict(self) -> Dict[str, Any]:
        return {
            "metrics": {
                "latency_ms": round(self.latency_ms, 2),
                "memory_mb": round(self.memory_mb, 2),
                "request_count": self.request_count,
                "error_count": self.error_count,
            },
            "boot_time_utc": self.boot_time_utc,
            "boot_time_riyadh": self.boot_time_riyadh,
        }

    async def should_shed_request(self, path: str) -> bool:
        if not self.degraded_mode or not get_env_bool("DEGRADED_SHED_ENABLED", True):
            return False
        shed_paths = get_env_list("DEGRADED_SHED_PATHS", ["/v1/analysis", "/v1/advanced", "/v1/advisor"])
        return any(path.startswith(p) for p in shed_paths if p)

    async def monitor_loop(self):
        self.running = True
        while self.running and not self.shutting_down:
            try:
                self.memory_mb = get_rss_mb()
                now = time.time()
                if self.memory_mb > self.gc_pressure_mb and (now - self.last_gc_time > self.gc_cooldown):
                    boot_logger.info(f"Memory pressure: {self.memory_mb:.1f}MB, forcing GC")
                    gc.collect()
                    self.last_gc_time = now
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
# Metrics
# =============================================================================
class MetricsCollector:
    def __init__(self):
        self.enabled = PROMETHEUS_AVAILABLE and get_env_bool("ENABLE_METRICS", True)
        if self.enabled:
            self.active_requests = Gauge("http_requests_active", "Active HTTP requests")
            self.request_count = Counter("http_requests_total", "Total HTTP requests", ["method", "endpoint", "status"])
            self.request_duration = Histogram(
                "http_request_duration_seconds",
                "HTTP request duration",
                ["method", "endpoint"],
                buckets=(0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10),
            )

    def record(self, method: str, endpoint: str, status_code: int, duration: float):
        if not self.enabled:
            return
        self.request_count.labels(method=method, endpoint=endpoint or "root", status=str(status_code)).inc()
        self.request_duration.labels(method=method, endpoint=endpoint or "root").observe(duration)


metrics = MetricsCollector()

# =============================================================================
# Docs/CSP helpers
# =============================================================================
def _is_docs_path(path: str) -> bool:
    return path == "/docs" or path.startswith("/docs/") or path == "/redoc" or path.startswith("/redoc")


def _csp_for_docs() -> str:
    # Swagger UI / ReDoc need CDN scripts/styles + inline scripts
    return (
        "default-src 'self'; "
        "base-uri 'self'; "
        "frame-ancestors 'none'; "
        "img-src 'self' https: data:; "
        "style-src 'self' 'unsafe-inline' https://unpkg.com https://cdn.jsdelivr.net; "
        "script-src 'self' 'unsafe-inline' 'unsafe-eval' https://unpkg.com https://cdn.jsdelivr.net; "
        "connect-src 'self'; "
        "font-src 'self' https://unpkg.com https://cdn.jsdelivr.net data:;"
    )


def _csp_strict() -> str:
    return "default-src 'none'; base-uri 'none'; frame-ancestors 'none'"


# =============================================================================
# Middleware: Auth Bridge
# =============================================================================
class AuthHeaderBridgeMiddleware(BaseHTTPMiddleware):
    """
    If Authorization is missing, map:
      - query param token=...
      - header X-APP-TOKEN
    into: Authorization: Bearer <token>
    """

    @staticmethod
    def _get(scope_headers: List[tuple], key: bytes) -> Optional[bytes]:
        for k, v in scope_headers:
            if k == key:
                return v
        return None

    async def dispatch(self, request: Request, call_next: RequestResponseEndpoint) -> Response:
        try:
            scope = request.scope
            hdrs = list(scope.get("headers") or [])
            if self._get(hdrs, b"authorization"):
                return await call_next(request)

            token_q = (request.query_params.get("token") or "").strip()
            token_h = self._get(hdrs, b"x-app-token")
            token = token_q or ((token_h.decode("utf-8").strip()) if token_h else "")

            if token:
                hdrs.append((b"authorization", f"Bearer {token}".encode("utf-8")))
                scope["headers"] = hdrs
        except Exception:
            pass

        return await call_next(request)

# =============================================================================
# Middleware: Telemetry
# =============================================================================
class TelemetryMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next: RequestResponseEndpoint) -> Response:
        request_id = request.headers.get("X-Request-ID") or str(uuid.uuid4())[:18]
        ctx_token = _request_id_ctx.set(request_id)
        request.state.request_id = request_id
        request.state.start_time = time.time()

        guardian.request_count += 1
        if metrics.enabled:
            metrics.active_requests.inc()

        try:
            if await guardian.should_shed_request(request.url.path):
                # do NOT dec here; finally will handle it once
                resp = BestJSONResponse(
                    status_code=503,
                    content={
                        "status": "degraded",
                        "message": "System under heavy load",
                        "request_id": request_id,
                        "time_riyadh": now_riyadh_iso(),
                    },
                )
                resp.headers["X-Request-ID"] = request_id
                resp.headers["X-Time-Riyadh"] = now_riyadh_iso()
                return resp

            response = await call_next(request)

            duration = time.time() - request.state.start_time
            guardian.latency_ms = guardian.latency_ms * 0.9 + duration * 1000 * 0.1
            if metrics.enabled:
                metrics.record(request.method, request.url.path, response.status_code, duration)

            response.headers["X-Request-ID"] = request_id
            response.headers["X-Process-Time"] = f"{duration:.4f}s"
            response.headers["X-System-Load"] = guardian.get_load_level()
            response.headers["X-Time-Riyadh"] = now_riyadh_iso()

            response.headers["X-Content-Type-Options"] = "nosniff"
            response.headers["X-Frame-Options"] = "DENY"
            response.headers["Referrer-Policy"] = "strict-origin-when-cross-origin"

            response.headers["Content-Security-Policy"] = _csp_for_docs() if _is_docs_path(request.url.path) else _csp_strict()
            if get_env_str("APP_ENV", "production") == "production":
                response.headers["Strict-Transport-Security"] = "max-age=31536000; includeSubDomains"

            return response

        finally:
            if metrics.enabled:
                metrics.active_requests.dec()
            _request_id_ctx.reset(ctx_token)

# =============================================================================
# Compression (safe)
# =============================================================================
class CompressionMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next: RequestResponseEndpoint) -> Response:
        response = await call_next(request)
        if response.headers.get("content-encoding"):
            return response
        # Let GZipMiddleware do most work; keep this conservative.
        return response

# =============================================================================
# Router Mounting
# =============================================================================
@dataclass(frozen=True)
class RouterSpec:
    label: str
    expected_prefix: str
    candidates: List[str]
    required: bool = True

ROUTER_PLAN: List[RouterSpec] = [
    RouterSpec("Advanced", "/v1/advanced", ["routes.advanced_analysis", "routes.advanced", "api.advanced"], True),
    RouterSpec("Advisor", "/v1/advisor", ["routes.advisor", "routes.investment_advisor", "core.investment_advisor"], True),
    RouterSpec("KSA", "/v1/ksa", ["routes.routes_argaam", "routes.argaam"], True),
    RouterSpec("Enriched", "/v1/enriched", ["routes.enriched_quote", "routes.enriched", "core.enriched_quote"], True),
    RouterSpec("Analysis", "/v1/analysis", ["routes.ai_analysis", "routes.analysis"], True),
    RouterSpec("System", "", ["routes.config", "routes.system", "core.legacy_service"], True),
    RouterSpec("WebSocket", "", ["routes.websocket"], False),
]

def _find_any_apirouter(module: Any) -> Optional[APIRouter]:
    for name in dir(module):
        try:
            obj = getattr(module, name)
        except Exception:
            continue
        if isinstance(obj, APIRouter):
            return obj
    return None

@contextmanager
def _temp_router_prefix(router: APIRouter, new_prefix: str):
    old = getattr(router, "prefix", "")
    try:
        router.prefix = new_prefix or ""
        yield
    finally:
        try:
            router.prefix = old or ""
        except Exception:
            pass

def _mount_with_prefix_normalization(app: FastAPI, child: APIRouter, expected_prefix: str) -> str:
    child_prefix = (getattr(child, "prefix", "") or "").strip()

    if not expected_prefix:
        app.include_router(child)
        return child_prefix or "/"

    if not child_prefix or child_prefix == "/":
        app.include_router(child, prefix=expected_prefix)
        return expected_prefix

    if child_prefix == expected_prefix or child_prefix.startswith(expected_prefix + "/"):
        app.include_router(child)
        return child_prefix

    if expected_prefix.endswith(child_prefix) and expected_prefix.startswith("/v1/"):
        with _temp_router_prefix(child, ""):
            app.include_router(child, prefix=expected_prefix)
        return expected_prefix

    boot_logger.warning(
        f"Router prefix mismatch: expected='{expected_prefix}' but router.prefix='{child_prefix}'. Mounting as-is."
    )
    app.include_router(child)
    return child_prefix or "/"

def mount_routers_once(app: FastAPI) -> Dict[str, Any]:
    if not hasattr(app.state, "mounted_router_modules"):
        app.state.mounted_router_modules = set()

    report: Dict[str, Any] = {"mounted": [], "failed": []}

    for spec in ROUTER_PLAN:
        mounted, last_error = False, ""
        for module_path in spec.candidates:
            if module_path in app.state.mounted_router_modules:
                mounted = True
                break

            module = safe_import(module_path)
            if not module:
                last_error = "no module found"
                continue

            router = _find_any_apirouter(module)
            if not router:
                last_error = f"no APIRouter found in {module_path}"
                continue

            try:
                applied_prefix = _mount_with_prefix_normalization(app, router, spec.expected_prefix)
                app.state.mounted_router_modules.add(module_path)
                boot_logger.info(f"✅ Mounted {spec.label} via {module_path} (Prefix: {applied_prefix})")
                report["mounted"].append({"label": spec.label, "module": module_path, "prefix": applied_prefix})
                mounted = True
                break
            except Exception as e:
                last_error = str(e)

        if not mounted:
            if spec.required:
                boot_logger.warning(f"⚠️ Router '{spec.label}' not mounted: {last_error or 'not found'}")
            else:
                boot_logger.info(f"ℹ️ Optional router '{spec.label}' not mounted: {last_error or 'not found'}")
            report["failed"].append({"label": spec.label, "error": last_error or "not found"})

    app.state.router_report = report
    return report

# =============================================================================
# Engine bootstrap
# =============================================================================
async def init_engine_resilient(app: FastAPI, max_retries: int = 3) -> None:
    app.state.engine_ready = False
    app.state.engine_error = None

    for attempt in range(max_retries):
        try:
            engine_module = safe_import("core.data_engine_v2") or safe_import("core.data_engine")
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
                await asyncio.sleep(min(10.0, base_wait + random.uniform(0, base_wait)))

    app.state.engine_ready = False
    app.state.engine_error = "CRITICAL: Engine failed to initialize after retries"
    boot_logger.error(app.state.engine_error)

# =============================================================================
# App Factory
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
            storage_uri=get_env_str("REDIS_URL") if get_env_str("RATE_LIMIT_BACKEND") == "redis" else "memory://",
        )

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        guardian_task = asyncio.create_task(guardian.monitor_loop())

        async def boot():
            await asyncio.sleep(0)
            if get_env_bool("INIT_ENGINE_ON_BOOT", True):
                await init_engine_resilient(app, max_retries=get_env_int("MAX_RETRIES", 3))
            gc.collect()
            boot_logger.info(f"🚀 Ready v{app_version} | entry={APP_ENTRY_VERSION} | env={app_env} | mem={guardian.memory_mb:.1f}MB")

        boot_task = asyncio.create_task(boot())
        yield
        guardian.stop()
        for t in (boot_task, guardian_task):
            t.cancel()
        await asyncio.gather(boot_task, guardian_task, return_exceptions=True)

    # Disable FastAPI built-in docs; we provide custom unpkg docs
    app = FastAPI(
        title=app_name,
        version=app_version,
        description="Tadawul Fast Bridge API - Enterprise Market Data Platform",
        lifespan=lifespan,
        docs_url=None,
        redoc_url=None,
        default_response_class=BestJSONResponse,
    )

    # Rate limiting
    if limiter is not None:
        app.state.limiter = limiter
        app.add_exception_handler(RateLimitExceeded, _rate_limit_exceeded_handler)
        app.add_middleware(SlowAPIMiddleware)

    # Middlewares (auth bridge added LAST to run FIRST)
    app.add_middleware(TelemetryMiddleware)
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

    # Sentry (optional)
    sentry_dsn = get_env_str("SENTRY_DSN").strip()
    if SENTRY_AVAILABLE and sentry_dsn and is_valid_uri(sentry_dsn):
        try:
            sentry_sdk.init(
                dsn=sentry_dsn,
                environment=app_env,
                release=app_version,
                traces_sample_rate=get_env_float("SENTRY_TRACES_SAMPLE_RATE", 0.1),
                integrations=[LoggingIntegration(level=logging.INFO, event_level=logging.ERROR)],
            )
            if SentryAsgiMiddleware:
                app.add_middleware(SentryAsgiMiddleware)
        except Exception as e:
            boot_logger.error(f"Failed to initialize Sentry: {e}")

    # OpenTelemetry (optional)
    if TRACING_AVAILABLE and get_env_bool("ENABLE_TRACING", False):
        try:
            provider = TracerProvider(resource=Resource(attributes={SERVICE_NAME: app_name}))
            endpoint = get_env_str("OTLP_ENDPOINT")
            if endpoint:
                exporter = OTLPSpanExporter(endpoint=endpoint)
                provider.add_span_processor(BatchSpanProcessor(exporter))
            trace.set_tracer_provider(provider)
            FastAPIInstrumentor.instrument_app(app)
        except Exception as e:
            boot_logger.error(f"Failed to initialize OpenTelemetry: {e}")

    # Custom Swagger UI (unpkg)
    if get_env_bool("ENABLE_SWAGGER", True):
        @app.get("/docs", include_in_schema=False)
        async def custom_docs() -> HTMLResponse:
            return get_swagger_ui_html(
                openapi_url=app.openapi_url,
                title=f"{app.title} - Swagger UI",
                oauth2_redirect_url="/docs/oauth2-redirect",
                swagger_js_url="https://unpkg.com/swagger-ui-dist@5/swagger-ui-bundle.js",
                swagger_css_url="https://unpkg.com/swagger-ui-dist@5/swagger-ui.css",
            )

        @app.get("/docs/oauth2-redirect", include_in_schema=False)
        async def docs_redirect() -> HTMLResponse:
            return get_swagger_ui_oauth2_redirect_html()

    if get_env_bool("ENABLE_REDOC", True):
        @app.get("/redoc", include_in_schema=False)
        async def custom_redoc() -> HTMLResponse:
            return get_redoc_html(
                openapi_url=app.openapi_url,
                title=f"{app.title} - ReDoc",
                redoc_js_url="https://unpkg.com/redoc@next/bundles/redoc.standalone.js",
            )

    # Mount routers
    mount_routers_once(app)

    # Prometheus
    if PROMETHEUS_AVAILABLE and get_env_bool("ENABLE_METRICS", True):
        @app.get("/metrics", include_in_schema=False)
        async def metrics_endpoint():
            return Response(content=generate_latest(), media_type="text/plain; version=0.0.4; charset=utf-8")

    @app.api_route("/", methods=["GET", "HEAD"], include_in_schema=False)
    async def root():
        return {
            "status": "online",
            "service": app_name,
            "version": app_version,
            "entry_version": APP_ENTRY_VERSION,
            "environment": app_env,
            "time_riyadh": now_riyadh_iso(),
            "documentation": "/docs",
        }

    @app.get("/health", tags=["system"])
    async def health(request: Request):
        return {
            "status": "healthy" if getattr(app.state, "engine_ready", False) and not guardian.degraded_mode else "degraded",
            "service": {"name": app_name, "version": app_version, "entry_version": APP_ENTRY_VERSION, "environment": app_env},
            "vitals": guardian.to_dict()["metrics"],
            "boot": {"engine_ready": getattr(app.state, "engine_ready", False)},
            "routers": getattr(app.state, "router_report", {"mounted": [], "failed": []}),
            "request_id": getattr(request.state, "request_id", get_request_id()),
        }

    @app.get("/debug/routes", tags=["system"])
    async def debug_routes(request: Request):
        return {
            "total_routes": len(app.routes),
            "paths": [route.path for route in app.routes if isinstance(route, APIRoute)],
        }

    # Exception handlers (keep request_id stable)
    @app.exception_handler(StarletteHTTPException)
    async def http_exception_handler(request: Request, exc: StarletteHTTPException):
        rid = getattr(request.state, "request_id", None) or get_request_id()
        resp = BestJSONResponse(
            status_code=exc.status_code,
            content={"status": "error", "message": exc.detail, "request_id": rid, "time_riyadh": now_riyadh_iso()},
        )
        resp.headers["X-Request-ID"] = rid
        return resp

    @app.exception_handler(RequestValidationError)
    async def validation_exception_handler(request: Request, exc: RequestValidationError):
        rid = getattr(request.state, "request_id", None) or get_request_id()
        resp = BestJSONResponse(
            status_code=422,
            content={"status": "error", "message": "Validation error", "details": exc.errors(), "request_id": rid},
        )
        resp.headers["X-Request-ID"] = rid
        return resp

    @app.exception_handler(Exception)
    async def unhandled_exception_handler(request: Request, exc: Exception):
        rid = getattr(request.state, "request_id", None) or get_request_id()
        logger.error(f"Unhandled exception on {request.url.path}: {traceback.format_exc()}")
        resp = BestJSONResponse(
            status_code=500,
            content={"status": "error", "message": f"Internal Server Error: {exc}", "request_id": rid},
        )
        resp.headers["X-Request-ID"] = rid
        return resp

    # Add Auth Bridge LAST so it runs FIRST
    app.add_middleware(AuthHeaderBridgeMiddleware)

    return app


app = create_app()

if __name__ == "__main__":
    import uvicorn
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
