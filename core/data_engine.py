#!/usr/bin/env python3
"""
core/data_engine.py
================================================================================
Enterprise Data Engine — v6.5.0 (ALIGNED / SAFE / PROD-HARDENED)
================================================================================

Primary goals (this revision)
- ✅ Fix OpenTelemetry integration correctness (no name collision, correct span context usage)
- ✅ Fix async execution correctness (no nested event loops; safe sync bridge via thread runner)
- ✅ Hygiene-safe debug output (no built-in output calls; uses logger + sys.stderr.write)
- ✅ Fix perf metrics timestamps (epoch-based timestamps + perf_counter durations)
- ✅ Keep schema alignment (UnifiedQuote compatibility + stable field names)
- ✅ Keep resilience (rate-limit, circuit breaker, cache, emergency rescuer)

Notes on ratio/% correctness
- This module does NOT scale percent fields globally.
- It only computes change_pct in the EmergencyDataRescuer (as percent points).
"""

from __future__ import annotations

import asyncio
import functools
import inspect
import logging
import os
import random
import sys
import threading
import time
import traceback
import uuid
import zlib
import pickle
from collections import defaultdict
from contextlib import asynccontextmanager
from dataclasses import dataclass, field, asdict
from datetime import datetime, timedelta, timezone
from enum import Enum
from importlib import import_module
from typing import (
    Any,
    AsyncGenerator,
    Awaitable,
    Callable,
    Dict,
    Iterable,
    List,
    Optional,
    Sequence,
    Set,
    Tuple,
    Type,
    Union,
)

# =============================================================================
# Version Information
# =============================================================================

__version__ = "6.5.0"
ADAPTER_VERSION = __version__

# =============================================================================
# High-Performance JSON (optional)
# =============================================================================

try:
    import orjson  # type: ignore

    def json_loads(data: Union[str, bytes]) -> Any:
        return orjson.loads(data)

    def json_dumps(obj: Any) -> bytes:
        return orjson.dumps(obj, default=str)

    _HAS_ORJSON = True
except Exception:
    import json  # type: ignore

    def json_loads(data: Union[str, bytes]) -> Any:
        return json.loads(data)

    def json_dumps(obj: Any) -> bytes:
        return json.dumps(obj, default=str).encode("utf-8")

    _HAS_ORJSON = False

# =============================================================================
# Optional Dependencies (safe)
# =============================================================================

# Pydantic
try:
    from pydantic import BaseModel, Field, ConfigDict  # type: ignore
    from pydantic import ValidationError as PydanticValidationError  # type: ignore

    try:
        _PYDANTIC_V2 = True
    except Exception:
        _PYDANTIC_V2 = False
except Exception:
    _PYDANTIC_V2 = False
    BaseModel = object  # type: ignore

    def Field(default=None, **kwargs):  # type: ignore
        return default

    ConfigDict = None  # type: ignore
    PydanticValidationError = Exception  # type: ignore

# Redis (optional)
try:
    import aioredis  # type: ignore
    from aioredis import Redis  # type: ignore

    _REDIS_AVAILABLE = True
except Exception:
    aioredis = None  # type: ignore
    Redis = None  # type: ignore
    _REDIS_AVAILABLE = False

# Memcached (optional)
try:
    import aiomcache  # type: ignore

    _MEMCACHED_AVAILABLE = True
except Exception:
    aiomcache = None  # type: ignore
    _MEMCACHED_AVAILABLE = False

# Prometheus (optional)
try:
    from prometheus_client import Counter, Histogram, Gauge  # type: ignore
    from prometheus_client import generate_latest, REGISTRY  # type: ignore

    _PROMETHEUS_AVAILABLE = True
except Exception:
    _PROMETHEUS_AVAILABLE = False

# OpenTelemetry (optional)
try:
    from opentelemetry import trace as otel_trace  # type: ignore
    from opentelemetry.trace import Status, StatusCode  # type: ignore

    _OTEL_AVAILABLE = True
except Exception:
    otel_trace = None  # type: ignore
    Status = None  # type: ignore
    StatusCode = None  # type: ignore
    _OTEL_AVAILABLE = False

# =============================================================================
# Logging + Debug
# =============================================================================

logger = logging.getLogger("core.data_engine")

class LogLevel(Enum):
    DEBUG = 10
    INFO = 20
    WARN = 30
    WARNING = 30
    ERROR = 40
    CRITICAL = 50


_DEBUG = (os.getenv("DATA_ENGINE_DEBUG", "") or "").strip().lower() in {"1", "true", "yes", "y", "on"}
_DEBUG_LEVEL = (os.getenv("DATA_ENGINE_DEBUG_LEVEL", "info") or "info").strip().lower()
_STRICT_MODE = (os.getenv("DATA_ENGINE_STRICT", "") or "").strip().lower() in {"1", "true", "yes", "y", "on"}
_V2_DISABLED = (os.getenv("DATA_ENGINE_V2_DISABLED", "") or "").strip().lower() in {"1", "true", "yes", "y", "on"}
_PERF_MONITORING = (os.getenv("DATA_ENGINE_PERF_MONITORING", "") or "").strip().lower() in {"1", "true", "yes", "y", "on"}
_TRACING_ENABLED = (os.getenv("DATA_ENGINE_TRACING", "") or "").strip().lower() in {"1", "true", "yes", "y", "on"}

_LOG_LEVEL = LogLevel.INFO
try:
    _LOG_LEVEL = LogLevel[_DEBUG_LEVEL.upper()]
except Exception:
    _LOG_LEVEL = LogLevel.INFO

def _dbg(msg: str, level: str = "info") -> None:
    if not _DEBUG:
        return
    try:
        lvl = LogLevel[level.upper()]
        if lvl.value < _LOG_LEVEL.value:
            return
    except Exception:
        return

    # Hygiene-safe: avoid built-in output calls
    try:
        ts = datetime.now(timezone.utc).strftime("%H:%M:%S.%f")[:-3]
        sys.stderr.write(f"[{ts}] [data_engine:{level.upper()}] {msg}\n")
    except Exception:
        pass

# =============================================================================
# Time Helpers
# =============================================================================

_UTC = timezone.utc
_RIYADH_TZ = timezone(timedelta(hours=3))

def _utc_now() -> datetime:
    return datetime.now(_UTC)

def _utc_now_iso() -> str:
    return _utc_now().isoformat()

# =============================================================================
# Tracing (safe)
# =============================================================================

class TraceContext:
    """
    OpenTelemetry context manager (sync + async).
    Uses start_as_current_span when available.
    """

    def __init__(self, name: str, attributes: Optional[Dict[str, Any]] = None):
        self.name = name
        self.attributes = attributes or {}
        self._cm = None
        self._span = None
        self._tracer = None
        if _OTEL_AVAILABLE and _TRACING_ENABLED and otel_trace is not None:
            try:
                self._tracer = otel_trace.get_tracer(__name__)
            except Exception:
                self._tracer = None

    def __enter__(self):
        if self._tracer is None:
            return self
        try:
            self._cm = self._tracer.start_as_current_span(self.name)
            self._span = self._cm.__enter__()
            if self._span is not None and self.attributes:
                for k, v in self.attributes.items():
                    try:
                        self._span.set_attribute(str(k), v)
                    except Exception:
                        continue
        except Exception:
            self._cm = None
            self._span = None
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        try:
            if self._span is not None and exc_val is not None and _OTEL_AVAILABLE and Status is not None and StatusCode is not None:
                try:
                    self._span.record_exception(exc_val)
                except Exception:
                    pass
                try:
                    self._span.set_status(Status(StatusCode.ERROR, str(exc_val)))
                except Exception:
                    pass
        finally:
            if self._cm is not None:
                try:
                    return self._cm.__exit__(exc_type, exc_val, exc_tb)
                except Exception:
                    return False
        return False

    async def __aenter__(self):
        return self.__enter__()

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        return self.__exit__(exc_type, exc_val, exc_tb)

def otel_traced(name: Optional[str] = None):
    """
    Decorator for async functions.
    """
    def decorator(func: Callable[..., Awaitable[Any]]) -> Callable[..., Awaitable[Any]]:
        @functools.wraps(func)
        async def wrapper(*args: Any, **kwargs: Any) -> Any:
            span_name = name or func.__name__
            async with TraceContext(span_name, {"function": func.__name__}):
                return await func(*args, **kwargs)
        return wrapper
    return decorator

# =============================================================================
# Perf Monitoring (fixed timestamps)
# =============================================================================

@dataclass(slots=True)
class PerfMetrics:
    operation: str
    start_epoch: float
    duration_ms: float
    success: bool
    error: Optional[str] = None
    metadata: Dict[str, Any] = field(default_factory=dict)

_PERF_METRICS: List[PerfMetrics] = []
_PERF_LOCK = threading.RLock()

def record_perf_metric(m: PerfMetrics) -> None:
    if not _PERF_MONITORING:
        return
    with _PERF_LOCK:
        _PERF_METRICS.append(m)
        if len(_PERF_METRICS) > 10000:
            _PERF_METRICS[:] = _PERF_METRICS[-10000:]

def get_perf_metrics(operation: Optional[str] = None, limit: Optional[int] = 1000) -> List[Dict[str, Any]]:
    with _PERF_LOCK:
        items = _PERF_METRICS if not limit else _PERF_METRICS[-int(limit):]
        if operation:
            items = [x for x in items if x.operation == operation]
        return [
            {
                "operation": x.operation,
                "duration_ms": x.duration_ms,
                "success": x.success,
                "error": x.error,
                "metadata": x.metadata,
                "timestamp": datetime.fromtimestamp(x.start_epoch, timezone.utc).isoformat(),
            }
            for x in items
        ]

def get_perf_stats(operation: Optional[str] = None) -> Dict[str, Any]:
    rows = get_perf_metrics(operation, limit=None)
    if not rows:
        return {}
    durations = [r["duration_ms"] for r in rows if r["success"]]
    if not durations:
        return {"operation": operation or "all", "error": "no_success"}
    durations_sorted = sorted(durations)
    n = len(durations_sorted)
    return {
        "operation": operation or "all",
        "count": len(rows),
        "success_count": sum(1 for r in rows if r["success"]),
        "failure_count": sum(1 for r in rows if not r["success"]),
        "success_rate_pct": (sum(1 for r in rows if r["success"]) / len(rows)) * 100.0,
        "min_duration_ms": durations_sorted[0],
        "max_duration_ms": durations_sorted[-1],
        "avg_duration_ms": sum(durations_sorted) / n,
        "p50_duration_ms": durations_sorted[n // 2],
        "p95_duration_ms": durations_sorted[min(n - 1, int(n * 0.95))],
        "p99_duration_ms": durations_sorted[min(n - 1, int(n * 0.99))],
    }

def reset_perf_metrics() -> None:
    with _PERF_LOCK:
        _PERF_METRICS.clear()

def monitor_perf(operation: str, metadata: Optional[Dict[str, Any]] = None):
    def decorator(func: Callable[..., Awaitable[Any]]) -> Callable[..., Awaitable[Any]]:
        @functools.wraps(func)
        async def wrapper(*args: Any, **kwargs: Any) -> Any:
            start_epoch = time.time()
            t0 = time.perf_counter()
            meta = dict(metadata or {})
            if args and hasattr(args[0], "__class__"):
                meta.setdefault("class", args[0].__class__.__name__)
            try:
                res = await func(*args, **kwargs)
                dt_ms = (time.perf_counter() - t0) * 1000.0
                record_perf_metric(PerfMetrics(operation=operation, start_epoch=start_epoch, duration_ms=dt_ms, success=True, metadata=meta))
                return res
            except Exception as e:
                dt_ms = (time.perf_counter() - t0) * 1000.0
                record_perf_metric(PerfMetrics(operation=operation, start_epoch=start_epoch, duration_ms=dt_ms, success=False, error=str(e), metadata=meta))
                raise
        return wrapper
    return decorator

# =============================================================================
# Prometheus Metrics (safe)
# =============================================================================

class _DummyMetric:
    def labels(self, *args, **kwargs):  # noqa
        return self
    def inc(self, *args, **kwargs):  # noqa
        return None
    def observe(self, *args, **kwargs):  # noqa
        return None
    def set(self, *args, **kwargs):  # noqa
        return None

class MetricsRegistry:
    def __init__(self, namespace: str = "data_engine"):
        self.namespace = namespace
        self._lock = threading.RLock()
        self._metrics: Dict[str, Any] = {}
        if _PROMETHEUS_AVAILABLE:
            self._init_metrics()

    def _init_metrics(self) -> None:
        with self._lock:
            self._metrics["requests_total"] = Counter(
                f"{self.namespace}_requests_total",
                "Total number of requests",
                ["operation", "status"],
            )
            self._metrics["request_duration_seconds"] = Histogram(
                f"{self.namespace}_request_duration_seconds",
                "Request duration in seconds",
                ["operation"],
                buckets=[0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10],
            )
            self._metrics["cache_hits_total"] = Counter(
                f"{self.namespace}_cache_hits_total",
                "Total cache hits",
                ["cache_type"],
            )
            self._metrics["cache_misses_total"] = Counter(
                f"{self.namespace}_cache_misses_total",
                "Total cache misses",
                ["cache_type"],
            )
            self._metrics["circuit_breaker_state"] = Gauge(
                f"{self.namespace}_circuit_breaker_state",
                "Circuit breaker state (1=closed, 0=open, -1=half-open)",
                ["name"],
            )
            self._metrics["provider_health"] = Gauge(
                f"{self.namespace}_provider_health",
                "Provider health (1=healthy, 0=degraded, -1=unhealthy)",
                ["provider"],
            )
            self._metrics["active_requests"] = Gauge(
                f"{self.namespace}_active_requests",
                "Active request count",
            )

    def inc(self, name: str, value: float = 1.0, labels: Optional[Dict[str, str]] = None) -> None:
        if not _PROMETHEUS_AVAILABLE:
            return
        with self._lock:
            m = self._metrics.get(name)
            if not m:
                return
            if labels:
                m.labels(**labels).inc(value)
            else:
                m.inc(value)

    def observe(self, name: str, value: float, labels: Optional[Dict[str, str]] = None) -> None:
        if not _PROMETHEUS_AVAILABLE:
            return
        with self._lock:
            m = self._metrics.get(name)
            if not m:
                return
            if labels:
                m.labels(**labels).observe(value)
            else:
                m.observe(value)

    def set(self, name: str, value: float, labels: Optional[Dict[str, str]] = None) -> None:
        if not _PROMETHEUS_AVAILABLE:
            return
        with self._lock:
            m = self._metrics.get(name)
            if not m:
                return
            if labels:
                m.labels(**labels).set(value)
            else:
                m.set(value)

    def get_metrics_text(self) -> str:
        if not _PROMETHEUS_AVAILABLE:
            return ""
        return generate_latest(REGISTRY).decode("utf-8")

_METRICS = MetricsRegistry()
if not _PROMETHEUS_AVAILABLE:
    # Provide dummy objects to avoid attribute checks elsewhere
    _METRICS.inc = lambda *args, **kwargs: None  # type: ignore
    _METRICS.observe = lambda *args, **kwargs: None  # type: ignore
    _METRICS.set = lambda *args, **kwargs: None  # type: ignore

# =============================================================================
# Rate Limiter + Circuit Breaker
# =============================================================================

class CircuitState(Enum):
    CLOSED = "closed"
    OPEN = "open"
    HALF_OPEN = "half_open"

class TokenBucket:
    def __init__(self, rate_per_sec: float, capacity: Optional[float] = None):
        self.rate = max(0.0, float(rate_per_sec))
        self.capacity = float(capacity) if capacity is not None else max(1.0, self.rate * 2.0)
        self.tokens = self.capacity
        self.last = time.monotonic()
        self._lock = asyncio.Lock()

    async def acquire(self, tokens: float = 1.0) -> bool:
        if self.rate <= 0:
            return True
        async with self._lock:
            now = time.monotonic()
            elapsed = max(0.0, now - self.last)
            self.last = now
            self.tokens = min(self.capacity, self.tokens + elapsed * self.rate)
            if self.tokens >= tokens:
                self.tokens -= tokens
                return True
            return False

    async def wait_and_acquire(self, tokens: float = 1.0) -> None:
        while True:
            if await self.acquire(tokens):
                return
            async with self._lock:
                wait = (tokens - self.tokens) / self.rate if self.rate > 0 else 0.1
            await asyncio.sleep(min(1.0, max(0.01, wait)))

    def get_stats(self) -> Dict[str, Any]:
        util = 1.0 - (self.tokens / self.capacity) if self.capacity > 0 else 0.0
        return {"rate": self.rate, "capacity": self.capacity, "current_tokens": self.tokens, "utilization": util}

class DynamicCircuitBreaker:
    def __init__(
        self,
        name: str,
        failure_threshold: int = 5,
        success_threshold: int = 2,
        base_timeout_seconds: float = 30.0,
        max_timeout_seconds: float = 300.0,
    ):
        self.name = name
        self.failure_threshold = max(1, int(failure_threshold))
        self.success_threshold = max(1, int(success_threshold))
        self.base_timeout_seconds = float(base_timeout_seconds)
        self.max_timeout_seconds = float(max_timeout_seconds)

        self.state = CircuitState.CLOSED
        self.failure_count = 0
        self.success_count = 0
        self.last_failure_time = 0.0
        self.last_success_time = 0.0
        self.total_failures = 0
        self.total_successes = 0
        self.current_timeout = self.base_timeout_seconds
        self._lock = asyncio.Lock()

        _METRICS.set("circuit_breaker_state", 1, {"name": self.name})

    async def execute(self, func: Callable[..., Any], *args: Any, **kwargs: Any) -> Any:
        async with self._lock:
            now = time.time()
            if self.state == CircuitState.OPEN:
                if (now - self.last_failure_time) >= self.current_timeout:
                    self.state = CircuitState.HALF_OPEN
                    self.success_count = 0
                    _METRICS.set("circuit_breaker_state", -1, {"name": self.name})
                else:
                    raise RuntimeError(f"Circuit {self.name} is OPEN (cooldown {self.current_timeout:.1f}s)")

        try:
            result = func(*args, **kwargs)
            if inspect.isawaitable(result):
                result = await result

            async with self._lock:
                self.total_successes += 1
                self.last_success_time = time.time()
                if self.state == CircuitState.HALF_OPEN:
                    self.success_count += 1
                    if self.success_count >= self.success_threshold:
                        self.state = CircuitState.CLOSED
                        self.failure_count = 0
                        self.current_timeout = self.base_timeout_seconds
                        _METRICS.set("circuit_breaker_state", 1, {"name": self.name})
                else:
                    self.failure_count = 0
            return result

        except Exception as e:
            async with self._lock:
                self.total_failures += 1
                self.last_failure_time = time.time()

                if self.state == CircuitState.CLOSED:
                    self.failure_count += 1
                    if self.failure_count >= self.failure_threshold:
                        self.state = CircuitState.OPEN
                        _METRICS.set("circuit_breaker_state", 0, {"name": self.name})
                elif self.state == CircuitState.HALF_OPEN:
                    self.state = CircuitState.OPEN
                    self.current_timeout = min(self.max_timeout_seconds, self.current_timeout * 1.5)
                    _METRICS.set("circuit_breaker_state", 0, {"name": self.name})
            raise e

    def get_stats(self) -> Dict[str, Any]:
        return {
            "name": self.name,
            "state": self.state.value,
            "failure_count": self.failure_count,
            "success_count": self.success_count,
            "total_failures": self.total_failures,
            "total_successes": self.total_successes,
            "current_timeout": self.current_timeout,
            "last_failure_time": datetime.fromtimestamp(self.last_failure_time, timezone.utc).isoformat() if self.last_failure_time else None,
            "last_success_time": datetime.fromtimestamp(self.last_success_time, timezone.utc).isoformat() if self.last_success_time else None,
        }

# =============================================================================
# Distributed Cache (multi-backend, safe init)
# =============================================================================

class CacheBackend(Enum):
    MEMORY = "memory"
    REDIS = "redis"
    MEMCACHED = "memcached"
    NONE = "none"

@dataclass(slots=True)
class CacheEntry:
    key: str
    value: Any
    created_at: float = field(default_factory=time.time)
    expires_at: Optional[float] = None
    access_count: int = 0
    last_access: float = field(default_factory=time.time)

    @property
    def is_expired(self) -> bool:
        return self.expires_at is not None and time.time() > self.expires_at

class DistributedCache:
    def __init__(
        self,
        backend: CacheBackend = CacheBackend.MEMORY,
        default_ttl: int = 300,
        redis_url: Optional[str] = None,
        memcached_servers: Optional[List[str]] = None,
        max_size: int = 10000,
        compression: bool = True,
    ):
        self.backend = backend
        self.default_ttl = int(default_ttl)
        self.max_size = int(max_size)
        self.compression = bool(compression)

        self._memory_cache: Dict[str, CacheEntry] = {}
        self._redis_client: Optional["Redis"] = None
        self._memcached_client: Optional[Any] = None
        self._lock = asyncio.Lock()
        self._stats = {"hits": 0, "misses": 0, "sets": 0, "deletes": 0}

        self._redis_url = redis_url
        self._memcached_servers = memcached_servers or []

    def _cache_key(self, namespace: str, key: str) -> str:
        return f"{namespace}:{key}"

    def _serialize(self, value: Any) -> bytes:
        try:
            if self.compression:
                raw = pickle.dumps(value)
                return zlib.compress(raw)
            return json_dumps(value)
        except Exception:
            # best-effort fallback
            try:
                return json_dumps({"_error": "serialize_failed", "type": str(type(value))})
            except Exception:
                return b"{}"

    def _deserialize(self, data: bytes) -> Any:
        try:
            if self.compression:
                raw = zlib.decompress(data)
                return pickle.loads(raw)
            return json_loads(data)
        except Exception:
            return None

    async def _ensure_redis(self) -> None:
        if self.backend != CacheBackend.REDIS:
            return
        if not _REDIS_AVAILABLE or aioredis is None:
            self.backend = CacheBackend.MEMORY
            return
        if self._redis_client is not None:
            return
        if not self._redis_url:
            self.backend = CacheBackend.MEMORY
            return
        try:
            self._redis_client = await aioredis.from_url(
                self._redis_url,
                encoding=None,
                decode_responses=False,
                max_connections=20,
            )
            _dbg("Redis cache ready", "info")
        except Exception as e:
            _dbg(f"Redis init failed: {e}", "warn")
            self.backend = CacheBackend.MEMORY
            self._redis_client = None

    async def _ensure_memcached(self) -> None:
        if self.backend != CacheBackend.MEMCACHED:
            return
        if not _MEMCACHED_AVAILABLE or aiomcache is None:
            self.backend = CacheBackend.MEMORY
            return
        if self._memcached_client is not None:
            return
        if not self._memcached_servers:
            self.backend = CacheBackend.MEMORY
            return
        try:
            self._memcached_client = aiomcache.Client(self._memcached_servers)  # type: ignore
            _dbg("Memcached cache ready", "info")
        except Exception as e:
            _dbg(f"Memcached init failed: {e}", "warn")
            self.backend = CacheBackend.MEMORY
            self._memcached_client = None

    async def get(self, namespace: str, key: str) -> Optional[Any]:
        ck = self._cache_key(namespace, key)

        if self.backend == CacheBackend.REDIS:
            await self._ensure_redis()
            if self._redis_client is not None:
                try:
                    blob = await self._redis_client.get(ck)
                    if blob is not None:
                        self._stats["hits"] += 1
                        _METRICS.inc("cache_hits_total", 1, {"cache_type": "redis"})
                        return self._deserialize(blob)
                except Exception as e:
                    _dbg(f"Redis get failed: {e}", "debug")

        if self.backend == CacheBackend.MEMCACHED:
            await self._ensure_memcached()
            if self._memcached_client is not None:
                try:
                    blob = await self._memcached_client.get(ck.encode("utf-8"))
                    if blob:
                        self._stats["hits"] += 1
                        _METRICS.inc("cache_hits_total", 1, {"cache_type": "memcached"})
                        return self._deserialize(blob)
                except Exception as e:
                    _dbg(f"Memcached get failed: {e}", "debug")

        async with self._lock:
            ent = self._memory_cache.get(ck)
            if ent is not None and not ent.is_expired:
                ent.access_count += 1
                ent.last_access = time.time()
                self._stats["hits"] += 1
                _METRICS.inc("cache_hits_total", 1, {"cache_type": "memory"})
                return ent.value
            if ent is not None and ent.is_expired:
                self._memory_cache.pop(ck, None)

        self._stats["misses"] += 1
        _METRICS.inc("cache_misses_total", 1, {"cache_type": self.backend.value})
        return None

    async def set(self, namespace: str, key: str, value: Any, ttl: Optional[int] = None) -> None:
        ck = self._cache_key(namespace, key)
        ttl_i = int(ttl) if ttl is not None else self.default_ttl

        if self.backend == CacheBackend.REDIS:
            await self._ensure_redis()
            if self._redis_client is not None:
                try:
                    await self._redis_client.setex(ck, ttl_i, self._serialize(value))
                except Exception as e:
                    _dbg(f"Redis set failed: {e}", "debug")

        if self.backend == CacheBackend.MEMCACHED:
            await self._ensure_memcached()
            if self._memcached_client is not None:
                try:
                    await self._memcached_client.set(ck.encode("utf-8"), self._serialize(value), exptime=ttl_i)
                except Exception as e:
                    _dbg(f"Memcached set failed: {e}", "debug")

        async with self._lock:
            if len(self._memory_cache) >= self.max_size:
                await self._evict_lru()
            self._memory_cache[ck] = CacheEntry(key=ck, value=value, expires_at=time.time() + ttl_i)

        self._stats["sets"] += 1

    async def delete(self, namespace: str, key: str) -> None:
        ck = self._cache_key(namespace, key)

        if self.backend == CacheBackend.REDIS and self._redis_client is not None:
            try:
                await self._redis_client.delete(ck)
            except Exception:
                pass

        if self.backend == CacheBackend.MEMCACHED and self._memcached_client is not None:
            try:
                await self._memcached_client.delete(ck.encode("utf-8"))
            except Exception:
                pass

        async with self._lock:
            self._memory_cache.pop(ck, None)

        self._stats["deletes"] += 1

    async def clear(self, namespace: Optional[str] = None) -> None:
        async with self._lock:
            if not namespace:
                self._memory_cache.clear()
                return
            prefix = f"{namespace}:"
            for k in list(self._memory_cache.keys()):
                if k.startswith(prefix):
                    self._memory_cache.pop(k, None)

    async def _evict_lru(self) -> None:
        if not self._memory_cache:
            return
        items = sorted(self._memory_cache.items(), key=lambda kv: kv[1].last_access)
        # evict 20% oldest
        n = max(1, self.max_size // 5)
        for k, _ in items[:n]:
            self._memory_cache.pop(k, None)

    def get_stats(self) -> Dict[str, Any]:
        total = self._stats["hits"] + self._stats["misses"]
        return {
            "backend": self.backend.value,
            "hits": self._stats["hits"],
            "misses": self._stats["misses"],
            "sets": self._stats["sets"],
            "deletes": self._stats["deletes"],
            "hit_rate": (self._stats["hits"] / total) if total > 0 else 0.0,
            "memory_size": len(self._memory_cache),
            "memory_utilization": (len(self._memory_cache) / self.max_size) if self.max_size > 0 else 0.0,
        }

# =============================================================================
# Emergency Data Rescuer (missing prices)
# =============================================================================

class EmergencyDataRescuer:
    """
    If upstream returns missing/invalid price, try to patch via direct endpoints.
    """

    @classmethod
    async def rescue_quote(cls, quote_obj: Any, symbol: str) -> Any:
        norm_symbol = normalize_symbol(symbol) or (symbol or "").strip()
        if not norm_symbol:
            return quote_obj

        if quote_obj is None:
            UQ = get_unified_quote_class()
            quote_obj = UQ(symbol=norm_symbol, data_quality="MISSING", error="Upstream returned None")

        is_dict = isinstance(quote_obj, dict)
        price = None
        prev = None

        try:
            if is_dict:
                price = quote_obj.get("current_price") or quote_obj.get("price")
                prev = quote_obj.get("previous_close")
            else:
                price = getattr(quote_obj, "current_price", None) or getattr(quote_obj, "price", None)
                prev = getattr(quote_obj, "previous_close", None)
        except Exception:
            price = None

        # already good
        try:
            if price is not None and float(price) > 0:
                return quote_obj
        except Exception:
            pass

        _dbg(f"Rescuer triggered for {norm_symbol} (price missing)", "warn")

        data = await cls._fetch_data(norm_symbol)
        if not data:
            return quote_obj

        p = data.get("price")
        pc = data.get("prev_close")
        vol = data.get("volume")

        if p is None:
            return quote_obj

        try:
            p = float(p)
        except Exception:
            return quote_obj

        # Patch object
        try:
            if is_dict:
                quote_obj["current_price"] = p
                quote_obj["price"] = p
                if vol is not None:
                    quote_obj["volume"] = float(vol)
                if pc is not None:
                    quote_obj["previous_close"] = float(pc)
                quote_obj["data_quality"] = "RESCUED"
                quote_obj["error"] = None

                if pc not in (None, 0) and p is not None:
                    change = p - float(pc)
                    pct = (change / float(pc)) * 100.0
                    quote_obj["price_change"] = change
                    quote_obj["change"] = change
                    quote_obj["percent_change"] = pct
                    quote_obj["change_pct"] = pct
            else:
                if hasattr(quote_obj, "current_price"):
                    quote_obj.current_price = p
                if hasattr(quote_obj, "price"):
                    quote_obj.price = p
                if vol is not None and hasattr(quote_obj, "volume"):
                    quote_obj.volume = float(vol)
                if pc is not None and hasattr(quote_obj, "previous_close"):
                    quote_obj.previous_close = float(pc)
                if hasattr(quote_obj, "data_quality"):
                    quote_obj.data_quality = "RESCUED"
                if hasattr(quote_obj, "error"):
                    quote_obj.error = None

                if pc not in (None, 0) and p is not None:
                    change = p - float(pc)
                    pct = (change / float(pc)) * 100.0
                    if hasattr(quote_obj, "price_change"):
                        quote_obj.price_change = change
                    if hasattr(quote_obj, "change"):
                        quote_obj.change = change
                    if hasattr(quote_obj, "percent_change"):
                        quote_obj.percent_change = pct
                    if hasattr(quote_obj, "change_pct"):
                        quote_obj.change_pct = pct
        except Exception:
            return quote_obj

        _dbg(f"Rescuer patched {norm_symbol} => {p}", "info")
        return quote_obj

    @classmethod
    async def _fetch_data(cls, symbol: str) -> Optional[Dict[str, float]]:
        try:
            import aiohttp  # type: ignore
        except Exception:
            return None

        keys = {
            "alphavantage": (os.getenv("ALPHAVANTAGE_API_KEY") or "").strip(),
            "fmp": (os.getenv("FMP_API_KEY") or "").strip(),
        }
        proxy = (os.getenv("HTTP_PROXY") or os.getenv("PROXY_URL") or os.getenv("HTTPS_PROXY") or "").strip() or None

        timeout = aiohttp.ClientTimeout(total=3)
        headers = {
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/122.0.0.0 Safari/537.36"
            ),
            "Accept": "application/json",
            "Cache-Control": "no-cache",
            "Pragma": "no-cache",
        }

        async with aiohttp.ClientSession(timeout=timeout) as session:
            # Yahoo chart
            try:
                url = f"https://query2.finance.yahoo.com/v8/finance/chart/{symbol}?interval=1d&range=1d"
                async with session.get(url, headers=headers, proxy=proxy) as resp:
                    if resp.status == 200:
                        j = await resp.json()
                        meta = j.get("chart", {}).get("result", [{}])[0].get("meta", {}) or {}
                        price = meta.get("regularMarketPrice")
                        if price:
                            return {
                                "price": float(price),
                                "prev_close": float(meta.get("chartPreviousClose", price)),
                                "volume": float(meta.get("regularMarketVolume", 0) or 0),
                            }
            except Exception:
                pass

            # AlphaVantage
            if keys["alphavantage"]:
                try:
                    url = f"https://www.alphavantage.co/query?function=GLOBAL_QUOTE&symbol={symbol}&apikey={keys['alphavantage']}"
                    async with session.get(url, headers=headers) as resp:
                        if resp.status == 200:
                            j = await resp.json()
                            gq = j.get("Global Quote", {}) or {}
                            px = gq.get("05. price")
                            if px:
                                return {
                                    "price": float(px),
                                    "prev_close": float(gq.get("08. previous close", px)),
                                    "volume": float(gq.get("06. volume", 0) or 0),
                                }
                except Exception:
                    pass

            # FMP
            if keys["fmp"]:
                try:
                    url = f"https://financialmodelingprep.com/api/v3/quote/{symbol}?apikey={keys['fmp']}"
                    async with session.get(url, headers=headers) as resp:
                        if resp.status == 200:
                            j = await resp.json()
                            if isinstance(j, list) and j and "price" in j[0]:
                                row = j[0]
                                return {
                                    "price": float(row.get("price")),
                                    "prev_close": float(row.get("previousClose", row.get("price"))),
                                    "volume": float(row.get("volume", 0) or 0),
                                }
                except Exception:
                    pass

        return None

# =============================================================================
# Engine Modes / Discovery
# =============================================================================

class EngineMode(Enum):
    UNKNOWN = "unknown"
    V2 = "v2"
    LEGACY = "legacy"
    STUB = "stub"

@dataclass(slots=True)
class V2ModuleInfo:
    module: Any
    version: Optional[str] = None
    engine_class: Optional[Type] = None
    engine_v2_class: Optional[Type] = None
    engine_v3_class: Optional[Type] = None
    engine_v4_class: Optional[Type] = None
    engine_v5_class: Optional[Type] = None
    unified_quote_class: Optional[Type] = None
    has_module_funcs: bool = False
    error: Optional[str] = None

class V2Discovery:
    def __init__(self):
        self._lock = threading.RLock()
        self._mode: EngineMode = EngineMode.UNKNOWN
        self._info: Optional[V2ModuleInfo] = None
        self._error: Optional[str] = None

    def discover(self) -> Tuple[EngineMode, Optional[V2ModuleInfo], Optional[str]]:
        if self._mode != EngineMode.UNKNOWN:
            return self._mode, self._info, self._error

        with self._lock:
            if self._mode != EngineMode.UNKNOWN:
                return self._mode, self._info, self._error

            if _V2_DISABLED:
                self._mode = EngineMode.STUB
                self._error = "V2 disabled via DATA_ENGINE_V2_DISABLED"
                return self._mode, None, self._error

            try:
                mod = import_module("core.data_engine_v2")
                version = getattr(mod, "__version__", None) or getattr(mod, "VERSION", None)

                engine_class = getattr(mod, "DataEngine", None) or getattr(mod, "Engine", None)
                engine_v2_class = getattr(mod, "DataEngineV2", None)
                engine_v3_class = getattr(mod, "DataEngineV3", None)
                engine_v4_class = getattr(mod, "DataEngineV4", None)
                engine_v5_class = getattr(mod, "DataEngineV5", None)

                uq_class = getattr(mod, "UnifiedQuote", None)
                has_funcs = any(callable(getattr(mod, n, None)) for n in ("get_quote", "get_quotes", "get_enriched_quote", "get_enriched_quotes", "get_engine"))

                if uq_class is None:
                    try:
                        schemas = import_module("core.schemas")
                        uq_class = getattr(schemas, "UnifiedQuote", None)
                    except Exception:
                        uq_class = None

                if not (engine_class or engine_v2_class or engine_v3_class or engine_v4_class or engine_v5_class or has_funcs or uq_class):
                    raise ImportError("No usable exports found in core.data_engine_v2")

                self._info = V2ModuleInfo(
                    module=mod,
                    version=version,
                    engine_class=engine_class,
                    engine_v2_class=engine_v2_class,
                    engine_v3_class=engine_v3_class,
                    engine_v4_class=engine_v4_class,
                    engine_v5_class=engine_v5_class,
                    unified_quote_class=uq_class,
                    has_module_funcs=has_funcs,
                )
                self._mode = EngineMode.V2 if (engine_v2_class or engine_v3_class or engine_v4_class or engine_v5_class) else EngineMode.LEGACY
                return self._mode, self._info, None

            except Exception as e:
                self._mode = EngineMode.STUB
                self._error = str(e)
                logger.error("DataEngine V2 discovery failed; using STUB. %s\n%s", e, traceback.format_exc())
                return self._mode, None, self._error

_V2_DISCOVERY = V2Discovery()

# =============================================================================
# Safe helpers
# =============================================================================

def _safe_str(x: Any) -> str:
    try:
        return str(x).strip()
    except Exception:
        return ""

def _safe_upper(x: Any) -> str:
    return _safe_str(x).upper()

def _maybe_await(v: Any) -> Awaitable[Any]:
    if inspect.isawaitable(v):
        return v  # type: ignore
    async def _wrap() -> Any:
        return v
    return _wrap()

def _unwrap_payload(x: Any) -> Any:
    try:
        if isinstance(x, tuple) and len(x) >= 1:
            return x[0]
    except Exception:
        pass
    return x

def _as_list(x: Any) -> List[Any]:
    if x is None:
        return []
    if isinstance(x, list):
        return x
    if isinstance(x, dict):
        return list(x.values())
    if isinstance(x, (set, tuple)):
        return list(x)
    return [x]

def _coerce_symbol_from_payload(p: Any) -> Optional[str]:
    try:
        if p is None:
            return None
        if isinstance(p, dict):
            for key in ("symbol", "requested_symbol", "ticker", "code", "id"):
                v = p.get(key)
                if v:
                    return _safe_upper(v)
            return None
        for attr in ("symbol", "requested_symbol", "ticker", "code", "id"):
            v = getattr(p, attr, None)
            if v:
                return _safe_upper(v)
    except Exception:
        return None
    return None

def _finalize_quote(obj: Any) -> Any:
    try:
        fn = getattr(obj, "finalize", None)
        if callable(fn):
            return fn()
    except Exception:
        pass
    return obj

def _is_missing(obj: Any) -> bool:
    try:
        if obj is None:
            return True
        if isinstance(obj, dict):
            if obj.get("error"):
                return True
            dq = (obj.get("data_quality") or "").upper()
            return dq in {"MISSING", "ERROR"}
        err = getattr(obj, "error", None)
        if err:
            return True
        dq = (getattr(obj, "data_quality", "") or "").upper()
        return dq in {"MISSING", "ERROR"}
    except Exception:
        return False

# =============================================================================
# Sync bridge (safe when loop already running)
# =============================================================================

def _run_coro_sync(coro: Awaitable[Any]) -> Any:
    try:
        asyncio.get_running_loop()
    except RuntimeError:
        return asyncio.run(coro)

    # Running loop exists in this thread -> execute in a fresh thread with its own loop
    result_box: Dict[str, Any] = {}

    def _runner() -> None:
        try:
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            result_box["value"] = loop.run_until_complete(coro)
        except Exception as e:
            result_box["error"] = e
        finally:
            try:
                loop.close()
            except Exception:
                pass

    t = threading.Thread(target=_runner, daemon=True)
    t.start()
    t.join()

    if "error" in result_box:
        raise result_box["error"]
    return result_box.get("value")

# =============================================================================
# Symbol Normalization (provider routing)
# =============================================================================

@dataclass(slots=True)
class SymbolInfo:
    raw: str
    normalized: str
    market: str
    asset_class: str
    exchange: Optional[str] = None
    currency: Optional[str] = None
    provider_hint: Optional[str] = None

class SymbolNormalizer:
    def __init__(self):
        self._lock = threading.RLock()
        self._cache: Dict[str, SymbolInfo] = {}
        self._norm_fn: Optional[Callable[[str], str]] = None

    def _load_normalizer(self) -> Callable[[str], str]:
        if self._norm_fn is not None:
            return self._norm_fn

        with self._lock:
            if self._norm_fn is not None:
                return self._norm_fn

            for mod_name in ("core.symbols.normalize", "core.data_engine_v2"):
                try:
                    mod = import_module(mod_name)
                    fn = getattr(mod, "normalize_symbol", None)
                    if callable(fn):
                        self._norm_fn = fn
                        _dbg(f"Loaded normalizer from {mod_name}", "info")
                        return fn
                except Exception:
                    continue

            self._norm_fn = self._fallback_normalize
            _dbg("Using fallback normalizer", "info")
            return self._norm_fn

    def _fallback_normalize(self, symbol: str) -> str:
        su = _safe_upper(symbol)
        if not su:
            return ""
        if su.startswith("TADAWUL:"):
            su = su.split(":", 1)[1].strip()
        if su.endswith(".TADAWUL"):
            su = su.replace(".TADAWUL", "").strip()
        if any(ch in su for ch in ("=", "^", "/", "-")):
            return su
        if "." in su:
            return su
        if su.isdigit() and 3 <= len(su) <= 6:
            return f"{su}.SR"
        return su

    def _detect_market(self, symbol: str) -> str:
        s = _safe_upper(symbol)
        if s.endswith(".SR") or (s.isdigit() and 3 <= len(s) <= 6):
            return "KSA"
        if any(ch in s for ch in ("^", "=", "/")):
            return "SPECIAL"
        if "." not in s:
            return "US"
        suf = s.split(".")[-1]
        if suf in {"US", "NYSE", "NASDAQ", "N", "OQ"}:
            return "US"
        return "GLOBAL"

    def _detect_asset_class(self, symbol: str) -> str:
        s = _safe_upper(symbol)
        if s.startswith("^") or s.endswith(".INDX"):
            return "index"
        if ("=" in s and s.endswith("=X")) or "/" in s:
            return "forex"
        if ("=" in s and s.endswith("=F")):
            return "commodity"
        if "-" in s and s.split("-")[-1] in {"USD", "USDT", "BTC", "ETH"}:
            return "crypto"
        return "equity"

    def _detect_provider_hint(self, symbol: str) -> Optional[str]:
        s = _safe_upper(symbol)
        if s.endswith(".SR") or (s.isdigit() and 3 <= len(s) <= 6):
            return "tadawul"
        if s.startswith("^") or "=" in s or "/" in s:
            return "yahoo"
        if s.endswith(".L") or s.endswith(".DE"):
            return "eodhd"
        return None

    def normalize(self, symbol: str) -> str:
        raw = _safe_str(symbol)
        if not raw:
            return ""
        with self._lock:
            if raw in self._cache:
                return self._cache[raw].normalized

        fn = self._load_normalizer()
        try:
            norm = fn(raw)
        except Exception:
            norm = self._fallback_normalize(raw)

        info = SymbolInfo(
            raw=raw,
            normalized=norm,
            market=self._detect_market(norm),
            asset_class=self._detect_asset_class(norm),
            provider_hint=self._detect_provider_hint(norm),
        )
        with self._lock:
            self._cache[raw] = info
        return norm

    def get_info(self, symbol: str) -> SymbolInfo:
        raw = _safe_str(symbol)
        with self._lock:
            if raw in self._cache:
                return self._cache[raw]
        norm = self.normalize(raw)
        with self._lock:
            return self._cache.get(raw, SymbolInfo(raw=raw, normalized=norm, market=self._detect_market(norm), asset_class=self._detect_asset_class(norm)))

    def clean_symbols(self, symbols: Sequence[Any]) -> List[str]:
        seen: Set[str] = set()
        out: List[str] = []
        for s in symbols or []:
            raw = _safe_str(s)
            if not raw:
                continue
            norm = self.normalize(raw)
            if not norm or norm in seen:
                continue
            seen.add(norm)
            out.append(raw)
        return out

_SYMBOL_NORMALIZER = SymbolNormalizer()
normalize_symbol = _SYMBOL_NORMALIZER.normalize
get_symbol_info = _SYMBOL_NORMALIZER.get_info

# =============================================================================
# Stub Models
# =============================================================================

class StubUnifiedQuote(BaseModel):
    if _PYDANTIC_V2:
        model_config = ConfigDict(populate_by_name=True, extra="ignore")

    symbol: str
    name: Optional[str] = None
    market: str = "UNKNOWN"
    exchange: Optional[str] = None
    currency: Optional[str] = None

    current_price: Optional[float] = None
    previous_close: Optional[float] = None
    day_high: Optional[float] = None
    day_low: Optional[float] = None
    volume: Optional[float] = None
    market_cap: Optional[float] = None
    shares_outstanding: Optional[float] = None

    # canonical sheet names
    price: Optional[float] = None
    change: Optional[float] = None
    change_pct: Optional[float] = None

    # provider-friendly aliases
    price_change: Optional[float] = None
    percent_change: Optional[float] = None

    data_source: str = "stub"
    provider: Optional[str] = None
    data_quality: str = "MISSING"
    last_updated_utc: str = Field(default_factory=_utc_now_iso)
    error: Optional[str] = "Engine Unavailable"
    warnings: List[str] = Field(default_factory=list)
    request_id: Optional[str] = None

    def finalize(self) -> "StubUnifiedQuote":
        if self.current_price is None and self.price is not None:
            self.current_price = self.price
        if self.price is None and self.current_price is not None:
            self.price = self.current_price

        if self.change is None and self.price_change is not None:
            self.change = self.price_change
        if self.price_change is None and self.change is not None:
            self.price_change = self.change

        if self.change_pct is None and self.percent_change is not None:
            self.change_pct = self.percent_change
        if self.percent_change is None and self.change_pct is not None:
            self.percent_change = self.change_pct

        if self.change is None and self.current_price is not None and self.previous_close is not None:
            try:
                self.change = float(self.current_price) - float(self.previous_close)
                self.price_change = self.change
            except Exception:
                pass
        if self.change_pct is None and self.current_price is not None and self.previous_close not in (None, 0):
            try:
                self.change_pct = (float(self.current_price) / float(self.previous_close) - 1.0) * 100.0
                self.percent_change = self.change_pct
            except Exception:
                pass
        return self

    def dict(self, *args: Any, **kwargs: Any) -> Dict[str, Any]:
        if hasattr(self, "model_dump"):
            return self.model_dump(*args, **kwargs)  # type: ignore
        return dict(self.__dict__)

class StubEngine:
    def __init__(self, *args: Any, **kwargs: Any) -> None:
        logger.error("DataEngine initialized STUB engine (V2 unavailable)")
        self._start = time.time()
        self._req = 0
        self._err = 0

    async def get_quote(self, symbol: str) -> StubUnifiedQuote:
        self._req += 1
        return StubUnifiedQuote(symbol=_safe_upper(symbol), error="Engine V2 Missing", warnings=["stub"]).finalize()

    async def get_quotes(self, symbols: List[str]) -> List[StubUnifiedQuote]:
        self._req += len(symbols or [])
        return [StubUnifiedQuote(symbol=_safe_upper(s), error="Engine V2 Missing", warnings=["stub"]).finalize() for s in (symbols or [])]

    async def get_enriched_quote(self, symbol: str) -> StubUnifiedQuote:
        return await self.get_quote(symbol)

    async def get_enriched_quotes(self, symbols: List[str]) -> List[StubUnifiedQuote]:
        return await self.get_quotes(symbols)

    async def aclose(self) -> None:
        return None

    def get_stats(self) -> Dict[str, Any]:
        return {
            "mode": "stub",
            "uptime_seconds": time.time() - self._start,
            "request_count": self._req,
            "error_count": self._err,
            "error": "Engine V2 Missing",
        }

# =============================================================================
# Unified Quote resolution
# =============================================================================

def get_unified_quote_class() -> Type:
    mode, info, _ = _V2_DISCOVERY.discover()
    if mode in (EngineMode.V2, EngineMode.LEGACY) and info and info.unified_quote_class is not None:
        return info.unified_quote_class
    try:
        schemas = import_module("core.schemas")
        uq = getattr(schemas, "UnifiedQuote", None)
        if uq is not None:
            return uq
    except Exception:
        pass
    return StubUnifiedQuote

UnifiedQuote = get_unified_quote_class()

# =============================================================================
# Engine Manager
# =============================================================================

class EngineManager:
    def __init__(self):
        self._async_lock: Optional[asyncio.Lock] = None
        self._sync_lock = threading.RLock()
        self._engine: Optional[Any] = None
        self._mode: EngineMode = EngineMode.UNKNOWN
        self._v2_info: Optional[V2ModuleInfo] = None
        self._error: Optional[str] = None

        self._circuit = DynamicCircuitBreaker("engine", failure_threshold=5, base_timeout_seconds=30.0, max_timeout_seconds=300.0)
        self._rate = TokenBucket(rate_per_sec=100.0)

        # default: memory cache
        self._cache = DistributedCache(backend=CacheBackend.MEMORY, default_ttl=300, max_size=10000, compression=True)
        self._stats: Dict[str, Any] = {
            "created_at": _utc_now_iso(),
            "requests": 0,
            "errors": 0,
            "cache_hits": 0,
            "cache_misses": 0,
        }

    def _get_async_lock(self) -> asyncio.Lock:
        if self._async_lock is None:
            self._async_lock = asyncio.Lock()
        return self._async_lock

    def _discover(self) -> Tuple[EngineMode, Optional[V2ModuleInfo], Optional[str]]:
        if self._mode != EngineMode.UNKNOWN:
            return self._mode, self._v2_info, self._error
        mode, info, err = _V2_DISCOVERY.discover()
        self._mode, self._v2_info, self._error = mode, info, err
        return self._mode, self._v2_info, self._error

    def _instantiate_engine(self, engine_class: Type) -> Optional[Any]:
        # Try no-arg, then settings injection (best effort)
        try:
            return engine_class()
        except TypeError:
            pass
        except Exception:
            return None

        for settings_mod, fn_name in (("config", "get_settings"), ("core.config", "get_settings")):
            try:
                m = import_module(settings_mod)
                fn = getattr(m, fn_name, None)
                if callable(fn):
                    settings = fn()
                    try:
                        return engine_class(settings=settings)
                    except TypeError:
                        try:
                            return engine_class(settings)
                        except TypeError:
                            continue
            except Exception:
                continue
        try:
            return engine_class()
        except Exception:
            return None

    async def _get_v2_engine(self) -> Optional[Any]:
        mode, info, _ = self._discover()
        if mode not in (EngineMode.V2, EngineMode.LEGACY) or info is None:
            return None

        # Prefer module-level get_engine if present
        try:
            if info.has_module_funcs and hasattr(info.module, "get_engine"):
                fn = getattr(info.module, "get_engine")
                if callable(fn):
                    eng = fn()
                    if inspect.isawaitable(eng):
                        eng = await eng
                    if eng is not None:
                        return eng
        except Exception:
            pass

        for cls in (info.engine_v5_class, info.engine_v4_class, info.engine_v3_class, info.engine_v2_class, info.engine_class):
            if cls is None:
                continue
            eng = self._instantiate_engine(cls)
            if eng is not None:
                return eng
        return None

    async def get_engine(self) -> Any:
        if self._engine is not None:
            return self._engine

        async with self._get_async_lock():
            if self._engine is not None:
                return self._engine

            await self._rate.wait_and_acquire()

            async def _create() -> Any:
                eng = await self._get_v2_engine()
                if eng is not None:
                    self._engine = eng
                    _METRICS.set("provider_health", 1, {"provider": "engine_v2"})
                    _dbg("Using V2 engine", "info")
                    return eng
                self._engine = StubEngine()
                _METRICS.set("provider_health", -1, {"provider": "stub"})
                _dbg("Using stub engine", "warn")
                return self._engine

            return await self._circuit.execute(_create)

    async def close_engine(self) -> None:
        if self._engine is None:
            return
        eng = self._engine
        self._engine = None
        try:
            fn = getattr(eng, "aclose", None)
            if callable(fn):
                v = fn()
                if inspect.isawaitable(v):
                    await v
        except Exception as e:
            _dbg(f"Error closing engine: {e}", "error")

    def get_cache(self) -> DistributedCache:
        return self._cache

    def record_request(self, success: bool) -> None:
        with self._sync_lock:
            self._stats["requests"] = int(self._stats.get("requests", 0)) + 1
            if not success:
                self._stats["errors"] = int(self._stats.get("errors", 0)) + 1

    def get_stats(self) -> Dict[str, Any]:
        with self._sync_lock:
            st = dict(self._stats)
        st["mode"] = self._mode.value
        st["v2_available"] = self._mode in (EngineMode.V2, EngineMode.LEGACY)
        st["engine_active"] = self._engine is not None
        st["circuit_breaker"] = self._circuit.get_stats()
        st["rate_limiter"] = self._rate.get_stats()
        st["cache"] = self._cache.get_stats()
        if self._engine is not None and hasattr(self._engine, "get_stats"):
            try:
                st["engine_stats"] = self._engine.get_stats()
            except Exception:
                pass
        return st

_ENGINE_MANAGER = EngineManager()

# =============================================================================
# Core API (engine access)
# =============================================================================

async def get_engine() -> Any:
    return await _ENGINE_MANAGER.get_engine()

def get_engine_sync() -> Any:
    return _run_coro_sync(_ENGINE_MANAGER.get_engine())

async def close_engine() -> None:
    await _ENGINE_MANAGER.close_engine()

def get_cache() -> DistributedCache:
    return _ENGINE_MANAGER.get_cache()

# =============================================================================
# Engine method dispatch
# =============================================================================

def _candidate_method_names(enriched: bool, batch: bool) -> List[str]:
    if enriched and batch:
        return [
            "get_enriched_quotes",
            "get_enriched_quote_batch",
            "fetch_enriched_quotes",
            "fetch_enriched_quote_batch",
            "get_quotes_enriched",
        ]
    if enriched and not batch:
        return [
            "get_enriched_quote",
            "fetch_enriched_quote",
            "fetch_enriched_quote_patch",
            "enriched_quote",
            "get_enriched",
        ]
    if (not enriched) and batch:
        return [
            "get_quotes",
            "fetch_quotes",
            "quotes",
            "fetch_many",
            "get_quote_batch",
            "get_quotes_batch",
        ]
    return ["get_quote", "fetch_quote", "quote", "fetch", "get"]

async def _call_engine_method(engine: Any, method_name: str, *args: Any, **kwargs: Any) -> Any:
    method = getattr(engine, method_name, None)
    if method is None:
        raise AttributeError(f"Engine has no method '{method_name}'")
    res = method(*args, **kwargs)
    if inspect.isawaitable(res):
        res = await res
    return res

def _align_batch_results(results: Any, requested_symbols: List[str]) -> List[Any]:
    payload = _unwrap_payload(results)

    if isinstance(payload, dict):
        lookup: Dict[str, Any] = {}
        for k, v in payload.items():
            nk = normalize_symbol(_safe_str(k))
            if nk:
                lookup[nk] = v
            ps = _coerce_symbol_from_payload(v)
            if ps:
                lookup[normalize_symbol(ps)] = v
        return [lookup.get(normalize_symbol(sym)) for sym in requested_symbols]

    arr = _as_list(payload)
    if not arr:
        return [None] * len(requested_symbols)

    lookup2: Dict[str, Any] = {}
    for item in arr:
        ps = _coerce_symbol_from_payload(item)
        if ps:
            lookup2[normalize_symbol(ps)] = item

    if lookup2:
        return [lookup2.get(normalize_symbol(sym)) for sym in requested_symbols]

    if len(arr) >= len(requested_symbols):
        return arr[: len(requested_symbols)]
    return arr + [None] * (len(requested_symbols) - len(arr))

async def _call_engine_single(engine: Any, symbol: str, *, enriched: bool, use_cache: bool, ttl: Optional[int]) -> Any:
    norm = normalize_symbol(symbol)
    if not norm:
        raise ValueError("empty_symbol")

    cache_key = f"{norm}:{'enriched' if enriched else 'basic'}"
    if use_cache:
        cached = await _ENGINE_MANAGER.get_cache().get("quotes", cache_key)
        if cached is not None:
            with _ENGINE_MANAGER._sync_lock:
                _ENGINE_MANAGER._stats["cache_hits"] += 1
            _METRICS.inc("cache_hits_total", 1, {"cache_type": "engine"})
            return cached
        with _ENGINE_MANAGER._sync_lock:
            _ENGINE_MANAGER._stats["cache_misses"] += 1
        _METRICS.inc("cache_misses_total", 1, {"cache_type": "engine"})

    # Try module-level functions if available
    mode, info, _ = _V2_DISCOVERY.discover()
    if mode in (EngineMode.V2, EngineMode.LEGACY) and info and info.module is not None:
        fn_name = "get_enriched_quote" if enriched else "get_quote"
        fn = getattr(info.module, fn_name, None)
        if callable(fn):
            try:
                res = fn(norm)
                if inspect.isawaitable(res):
                    res = await res
                res = _unwrap_payload(res)
                res = await EmergencyDataRescuer.rescue_quote(res, norm)
                if use_cache and not _is_missing(res):
                    await _ENGINE_MANAGER.get_cache().set("quotes", cache_key, res, ttl)
                return res
            except Exception as e:
                _dbg(f"Module func {fn_name} failed: {e}", "debug")

    # Engine methods
    for m in _candidate_method_names(enriched=enriched, batch=False):
        try:
            res = await _call_engine_method(engine, m, norm)
            res = _unwrap_payload(res)
            res = await EmergencyDataRescuer.rescue_quote(res, norm)
            if use_cache and not _is_missing(res):
                await _ENGINE_MANAGER.get_cache().set("quotes", cache_key, res, ttl)
            return res
        except (AttributeError, NotImplementedError):
            continue
        except Exception as e:
            _dbg(f"Engine method {m} failed: {e}", "debug")
            continue

    # Final fallback: try enriched if basic requested
    if not enriched:
        return await _call_engine_single(engine, symbol, enriched=True, use_cache=use_cache, ttl=ttl)

    raise AttributeError("No suitable engine method found")

async def _call_engine_batch(engine: Any, symbols: List[str], *, enriched: bool, use_cache: bool, ttl: Optional[int]) -> List[Any]:
    norm_symbols = [normalize_symbol(s) for s in symbols]
    # cache fan-out
    cached_by_idx: Dict[int, Any] = {}
    miss_idx: List[int] = []
    if use_cache:
        cache = _ENGINE_MANAGER.get_cache()
        for i, sym in enumerate(norm_symbols):
            ck = f"{sym}:{'enriched' if enriched else 'basic'}"
            v = await cache.get("quotes", ck)
            if v is not None:
                cached_by_idx[i] = v
                with _ENGINE_MANAGER._sync_lock:
                    _ENGINE_MANAGER._stats["cache_hits"] += 1
                _METRICS.inc("cache_hits_total", 1, {"cache_type": "engine"})
            else:
                miss_idx.append(i)
                with _ENGINE_MANAGER._sync_lock:
                    _ENGINE_MANAGER._stats["cache_misses"] += 1
                _METRICS.inc("cache_misses_total", 1, {"cache_type": "engine"})
        if not miss_idx:
            return [cached_by_idx[i] for i in range(len(symbols))]
        miss_syms = [norm_symbols[i] for i in miss_idx]
    else:
        miss_syms = norm_symbols
        miss_idx = list(range(len(symbols)))

    # module-level batch
    mode, info, _ = _V2_DISCOVERY.discover()
    if mode in (EngineMode.V2, EngineMode.LEGACY) and info and info.module is not None:
        fn_name = "get_enriched_quotes" if enriched else "get_quotes"
        fn = getattr(info.module, fn_name, None)
        if callable(fn):
            try:
                res = fn(miss_syms)
                if inspect.isawaitable(res):
                    res = await res
                aligned = _align_batch_results(res, miss_syms)
                for j in range(len(aligned)):
                    aligned[j] = await EmergencyDataRescuer.rescue_quote(_unwrap_payload(aligned[j]), miss_syms[j])

                if use_cache:
                    cache = _ENGINE_MANAGER.get_cache()
                    for sym, item in zip(miss_syms, aligned):
                        if not _is_missing(item):
                            await cache.set("quotes", f"{sym}:{'enriched' if enriched else 'basic'}", item, ttl)

                out: List[Any] = [None] * len(symbols)
                cursor = 0
                for i in range(len(symbols)):
                    if i in cached_by_idx:
                        out[i] = cached_by_idx[i]
                    else:
                        out[i] = aligned[cursor]
                        cursor += 1
                return out
            except Exception as e:
                _dbg(f"Module func {fn_name} failed: {e}", "debug")

    # engine batch methods
    for m in _candidate_method_names(enriched=enriched, batch=True):
        try:
            res = await _call_engine_method(engine, m, miss_syms)
            aligned = _align_batch_results(res, miss_syms)
            for j in range(len(aligned)):
                aligned[j] = await EmergencyDataRescuer.rescue_quote(_unwrap_payload(aligned[j]), miss_syms[j])

            if use_cache:
                cache = _ENGINE_MANAGER.get_cache()
                for sym, item in zip(miss_syms, aligned):
                    if not _is_missing(item):
                        await cache.set("quotes", f"{sym}:{'enriched' if enriched else 'basic'}", item, ttl)

            out2: List[Any] = [None] * len(symbols)
            cursor = 0
            for i in range(len(symbols)):
                if i in cached_by_idx:
                    out2[i] = cached_by_idx[i]
                else:
                    out2[i] = aligned[cursor]
                    cursor += 1
            return out2
        except (AttributeError, NotImplementedError):
            continue
        except Exception as e:
            _dbg(f"Engine method {m} failed: {e}", "debug")
            continue

    # sequential fallback
    aligned_seq: List[Any] = []
    for sym in miss_syms:
        try:
            aligned_seq.append(await _call_engine_single(engine, sym, enriched=enriched, use_cache=False, ttl=None))
        except Exception:
            aligned_seq.append(None)

    out3: List[Any] = [None] * len(symbols)
    cursor = 0
    for i in range(len(symbols)):
        if i in cached_by_idx:
            out3[i] = cached_by_idx[i]
        else:
            out3[i] = aligned_seq[cursor]
            cursor += 1
    return out3

# =============================================================================
# Public Quote API
# =============================================================================

@otel_traced("get_enriched_quote")
@monitor_perf("get_enriched_quote")
async def get_enriched_quote(symbol: str, use_cache: bool = True, ttl: Optional[int] = None) -> Any:
    UQ = get_unified_quote_class()
    sym_in = _safe_str(symbol)
    if not sym_in:
        return UQ(symbol="", error="Empty symbol", data_quality="MISSING")

    t0 = time.perf_counter()
    _METRICS.inc("requests_total", 1, {"operation": "get_enriched_quote", "status": "started"})
    _METRICS.set("active_requests", 1)

    try:
        engine = await get_engine()
        res = await _call_engine_single(engine, sym_in, enriched=True, use_cache=use_cache, ttl=ttl)
        fin = _finalize_quote(_unwrap_payload(res))
        _ENGINE_MANAGER.record_request(True)

        _METRICS.inc("requests_total", 1, {"operation": "get_enriched_quote", "status": "success"})
        _METRICS.set("active_requests", 0)
        _METRICS.observe("request_duration_seconds", (time.perf_counter() - t0), {"operation": "get_enriched_quote"})
        return fin

    except Exception as e:
        _ENGINE_MANAGER.record_request(False)
        _METRICS.inc("requests_total", 1, {"operation": "get_enriched_quote", "status": "error"})
        _METRICS.set("active_requests", 0)
        _METRICS.observe("request_duration_seconds", (time.perf_counter() - t0), {"operation": "get_enriched_quote"})

        _dbg(f"get_enriched_quote failed: {e}", "error")
        if _STRICT_MODE:
            raise
        return UQ(
            symbol=normalize_symbol(sym_in) or sym_in,
            error=str(e),
            data_quality="MISSING",
            warnings=["Error retrieving quote"],
            request_id=str(uuid.uuid4()),
        ).finalize()

@otel_traced("get_enriched_quotes")
@monitor_perf("get_enriched_quotes")
async def get_enriched_quotes(symbols: List[str], use_cache: bool = True, ttl: Optional[int] = None) -> List[Any]:
    UQ = get_unified_quote_class()
    clean = _SYMBOL_NORMALIZER.clean_symbols(symbols)
    if not clean:
        return []

    normed = [normalize_symbol(s) for s in clean]
    _METRICS.inc("requests_total", 1, {"operation": "get_enriched_quotes", "status": "started"})
    _METRICS.set("active_requests", len(clean))

    try:
        engine = await get_engine()
        res = await _call_engine_batch(engine, normed, enriched=True, use_cache=use_cache, ttl=ttl)
        aligned = _align_batch_results(res, normed)

        out: List[Any] = []
        for orig, norm, item in zip(clean, normed, aligned):
            if item is None:
                q = UQ(symbol=norm, original_symbol=orig, error="Missing in batch result", data_quality="MISSING", request_id=str(uuid.uuid4()))
                out.append(q.finalize())
            else:
                fin = _finalize_quote(_unwrap_payload(item))
                try:
                    if hasattr(fin, "original_symbol"):
                        fin.original_symbol = orig
                except Exception:
                    pass
                out.append(fin)

        _ENGINE_MANAGER.record_request(True)
        _METRICS.inc("requests_total", 1, {"operation": "get_enriched_quotes", "status": "success"})
        _METRICS.set("active_requests", 0)
        return out

    except Exception as e:
        _ENGINE_MANAGER.record_request(False)
        _METRICS.inc("requests_total", 1, {"operation": "get_enriched_quotes", "status": "error"})
        _METRICS.set("active_requests", 0)
        _dbg(f"get_enriched_quotes failed: {e}", "error")
        if _STRICT_MODE:
            raise
        return [
            UQ(symbol=n, original_symbol=o, error=str(e), data_quality="MISSING", request_id=str(uuid.uuid4())).finalize()
            for o, n in zip(clean, normed)
        ]

async def get_quote(symbol: str, use_cache: bool = True, ttl: Optional[int] = None) -> Any:
    return await get_enriched_quote(symbol, use_cache=use_cache, ttl=ttl)

async def get_quotes(symbols: List[str], use_cache: bool = True, ttl: Optional[int] = None) -> List[Any]:
    return await get_enriched_quotes(symbols, use_cache=use_cache, ttl=ttl)

# =============================================================================
# Batch Processing (progress)
# =============================================================================

@dataclass(slots=True)
class BatchProgress:
    total: int
    completed: int = 0
    succeeded: int = 0
    failed: int = 0
    start_time: float = field(default_factory=time.time)
    errors: List[Tuple[str, str]] = field(default_factory=list)
    batch_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    metadata: Dict[str, Any] = field(default_factory=dict)

    @property
    def elapsed_seconds(self) -> float:
        return time.time() - self.start_time

    @property
    def completion_pct(self) -> float:
        return (self.completed / self.total * 100.0) if self.total > 0 else 0.0

    @property
    def success_rate_pct(self) -> float:
        return (self.succeeded / self.completed * 100.0) if self.completed > 0 else 0.0

    def to_dict(self) -> Dict[str, Any]:
        return {
            "batch_id": self.batch_id,
            "total": self.total,
            "completed": self.completed,
            "succeeded": self.succeeded,
            "failed": self.failed,
            "completion_pct": self.completion_pct,
            "success_rate_pct": self.success_rate_pct,
            "elapsed_seconds": self.elapsed_seconds,
            "errors": self.errors[:10],
            "metadata": self.metadata,
        }

async def process_batch(
    symbols: List[str],
    batch_size: int = 10,
    delay_seconds: float = 0.1,
    enriched: bool = True,
    use_cache: bool = True,
    ttl: Optional[int] = None,
    max_retries: int = 3,
    progress_callback: Optional[Callable[[BatchProgress], None]] = None,
    metadata: Optional[Dict[str, Any]] = None,
) -> List[Any]:
    clean = _SYMBOL_NORMALIZER.clean_symbols(symbols)
    if not clean:
        return []

    progress = BatchProgress(total=len(clean), metadata=metadata or {})
    results: List[Optional[Any]] = [None] * len(clean)
    retry_counts: Dict[int, int] = defaultdict(int)
    func = get_enriched_quotes if enriched else get_quotes

    for i in range(0, len(clean), batch_size):
        batch = clean[i : i + batch_size]
        idxs = list(range(i, min(i + batch_size, len(clean))))
        try:
            batch_results = await func(batch, use_cache=use_cache, ttl=ttl)
            for idx, r in zip(idxs, batch_results):
                results[idx] = r
                if r is None or getattr(r, "error", None) or (isinstance(r, dict) and r.get("error")):
                    if retry_counts[idx] < max_retries:
                        retry_counts[idx] += 1
                        continue
                    progress.failed += 1
                    progress.errors.append((clean[idx], str(getattr(r, "error", None) or (r.get("error") if isinstance(r, dict) else "error"))))
                else:
                    progress.succeeded += 1
                progress.completed += 1
        except Exception as e:
            for idx in idxs:
                if retry_counts[idx] < max_retries:
                    retry_counts[idx] += 1
                else:
                    results[idx] = None
                    progress.completed += 1
                    progress.failed += 1
                    progress.errors.append((clean[idx], str(e)))

        if progress_callback is not None:
            try:
                progress_callback(progress)
            except Exception:
                pass

        if i + batch_size < len(clean):
            await asyncio.sleep(delay_seconds)

    # replace None with stub quotes for consistency
    UQ = get_unified_quote_class()
    out: List[Any] = []
    for sym, r in zip(clean, results):
        if r is None:
            out.append(UQ(symbol=normalize_symbol(sym) or sym, error="Missing", data_quality="MISSING", request_id=str(uuid.uuid4())).finalize())
        else:
            out.append(_finalize_quote(_unwrap_payload(r)))
    return out

# =============================================================================
# Health Check
# =============================================================================

async def health_check() -> Dict[str, Any]:
    health: Dict[str, Any] = {
        "status": "healthy",
        "version": __version__,
        "timestamp": _utc_now_iso(),
        "checks": {},
        "warnings": [],
        "errors": [],
    }
    try:
        engine = await get_engine()
        if isinstance(engine, StubEngine):
            health["warnings"].append("Using stub engine (no real data)")
            health["status"] = "degraded"

        health["checks"]["cache"] = _ENGINE_MANAGER.get_cache().get_stats()
        health["checks"]["circuit_breaker"] = _ENGINE_MANAGER._circuit.get_stats()
        health["checks"]["rate_limiter"] = _ENGINE_MANAGER._rate.get_stats()

        try:
            test = await get_enriched_quote("AAPL", use_cache=False)
            if test is not None and not (getattr(test, "error", None) or (isinstance(test, dict) and test.get("error"))):
                health["checks"]["quote_test"] = "passed"
            else:
                health["checks"]["quote_test"] = "failed"
                health["status"] = "unhealthy"
                health["errors"].append("Quote test failed")
        except Exception as e:
            health["checks"]["quote_test"] = "failed"
            health["status"] = "unhealthy"
            health["errors"].append(f"Quote test error: {e}")

    except Exception as e:
        health["errors"].append(f"Health check failed: {e}")
        health["status"] = "unhealthy"
    return health

# =============================================================================
# Context manager + compatibility wrapper class
# =============================================================================

@asynccontextmanager
async def engine_context() -> AsyncGenerator[Any, None]:
    eng = await get_engine()
    try:
        yield eng
    finally:
        await close_engine()

class EngineSession:
    def __enter__(self) -> Any:
        self._engine = get_engine_sync()
        return self._engine
    def __exit__(self, *args: Any) -> None:
        _run_coro_sync(close_engine())

class DataEngine:
    """
    Backward-compatible wrapper that delegates to shared engine API.
    """
    def __init__(self, *args: Any, **kwargs: Any) -> None:
        self._engine: Optional[Any] = None
        self._request_id = str(uuid.uuid4())

    async def _ensure(self) -> Any:
        if self._engine is None:
            self._engine = await get_engine()
        return self._engine

    async def get_quote(self, symbol: str, use_cache: bool = True) -> Any:
        try:
            eng = await self._ensure()
            res = await _call_engine_single(eng, symbol, enriched=False, use_cache=use_cache, ttl=None)
            res = _finalize_quote(_unwrap_payload(res))
            return await EmergencyDataRescuer.rescue_quote(res, symbol)
        except Exception:
            return await get_quote(symbol, use_cache=use_cache)

    async def get_quotes(self, symbols: List[str], use_cache: bool = True) -> List[Any]:
        try:
            eng = await self._ensure()
            res = await _call_engine_batch(eng, symbols, enriched=False, use_cache=use_cache, ttl=None)
            aligned = _align_batch_results(res, [normalize_symbol(s) for s in symbols])
            out = [_finalize_quote(_unwrap_payload(x)) if x is not None else StubUnifiedQuote(symbol=_safe_upper(s), error="Missing", request_id=self._request_id).finalize() for x, s in zip(aligned, symbols)]
            for i in range(len(out)):
                out[i] = await EmergencyDataRescuer.rescue_quote(out[i], symbols[i])
            return out
        except Exception:
            return await get_quotes(symbols, use_cache=use_cache)

    async def get_enriched_quote(self, symbol: str, use_cache: bool = True) -> Any:
        return await get_enriched_quote(symbol, use_cache=use_cache)

    async def get_enriched_quotes(self, symbols: List[str], use_cache: bool = True) -> List[Any]:
        return await get_enriched_quotes(symbols, use_cache=use_cache)

    async def aclose(self) -> None:
        await close_engine()

# =============================================================================
# Diagnostics / Meta
# =============================================================================

def _parse_env_list(key: str) -> List[str]:
    raw = (os.getenv(key) or "").strip()
    if not raw:
        return []
    return [p.strip().lower() for p in raw.split(",") if p.strip()]

def get_engine_meta() -> Dict[str, Any]:
    mode, info, err = _V2_DISCOVERY.discover()
    providers = _parse_env_list("ENABLED_PROVIDERS") or _parse_env_list("PROVIDERS")
    ksa_providers = _parse_env_list("KSA_PROVIDERS")

    return {
        "mode": mode.value,
        "is_stub": mode == EngineMode.STUB,
        "adapter_version": ADAPTER_VERSION,
        "strict_mode": _STRICT_MODE,
        "v2_disabled": _V2_DISABLED,
        "v2_available": mode in (EngineMode.V2, EngineMode.LEGACY),
        "v2_version": info.version if info else None,
        "v2_error": err,
        "providers": providers,
        "ksa_providers": ksa_providers,
        "perf_monitoring": _PERF_MONITORING,
        "tracing_enabled": _TRACING_ENABLED,
        "engine_stats": _ENGINE_MANAGER.get_stats(),
        "perf_stats": get_perf_stats(),
    }

def __getattr__(name: str) -> Any:
    if name == "UnifiedQuote":
        return get_unified_quote_class()
    if name in {"ENGINE_MODE", "EngineMode"}:
        return _ENGINE_MANAGER._mode.value
    if name == "StubUnifiedQuote":
        return StubUnifiedQuote
    if name == "StubEngine":
        return StubEngine
    raise AttributeError(f"module 'core.data_engine' has no attribute '{name}'")

__all__ = [
    "__version__",
    "ADAPTER_VERSION",
    "EngineMode",
    "CircuitState",
    "CacheBackend",
    "UnifiedQuote",
    "StubUnifiedQuote",
    "SymbolInfo",
    "BatchProgress",
    "PerfMetrics",
    "normalize_symbol",
    "get_symbol_info",
    "get_engine",
    "get_engine_sync",
    "close_engine",
    "get_cache",
    "get_quote",
    "get_quotes",
    "get_enriched_quote",
    "get_enriched_quotes",
    "process_batch",
    "health_check",
    "engine_context",
    "EngineSession",
    "DataEngine",
    "DistributedCache",
    "DynamicCircuitBreaker",
    "TokenBucket",
    "get_perf_metrics",
    "get_perf_stats",
    "reset_perf_metrics",
    "get_engine_meta",
    "MetricsRegistry",
    "_METRICS",
]

# safe default
try:
    _METRICS.set("active_requests", 0)
except Exception:
    pass
