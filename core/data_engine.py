#!/usr/bin/env python3
"""
core/data_engine.py
================================================================================
Enterprise Data Engine -- v6.10.0 (ALIGNED / SAFE / PROD-HARDENED / SHEET-ROWS++)
================================================================================

This revision keeps the adapter-oriented role of core/data_engine.py, but fixes the
main problem seen in live validation:
- upstream sheet-row payloads can be non-empty yet still be degraded placeholder
  or fail-soft responses
- those degraded responses must not be treated as successful final payloads
- for instrument sheets, the adapter should continue to quote-batch fallback
  instead of surfacing placeholder rows as the final answer

Compatibility goals:
- keep public exports and wrapper class names stable
- keep async + sync accessors
- keep schema-first sheet-rows envelope
- prefer core.data_engine_v2 whenever it is available
"""

from __future__ import annotations

import asyncio
import functools
import inspect
import json
import logging
import math
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
from dataclasses import dataclass, field, asdict, is_dataclass
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
    Mapping,
    Optional,
    Sequence,
    Set,
    Tuple,
    Type,
    Union,
)

__version__ = "6.10.0"
ADAPTER_VERSION = __version__

# =============================================================================
# JSON helpers
# =============================================================================
try:
    import orjson  # type: ignore

    def json_loads(data: Union[str, bytes]) -> Any:
        return orjson.loads(data)

    def json_dumps(obj: Any) -> bytes:
        return orjson.dumps(obj, default=str)

except Exception:
    def json_loads(data: Union[str, bytes]) -> Any:
        if isinstance(data, bytes):
            data = data.decode("utf-8", errors="replace")
        return json.loads(data)

    def json_dumps(obj: Any) -> bytes:
        return json.dumps(obj, default=str).encode("utf-8")

# =============================================================================
# Optional dependencies
# =============================================================================
try:
    from pydantic import BaseModel, Field, ConfigDict  # type: ignore
    _PYDANTIC_V2 = True
except Exception:
    _PYDANTIC_V2 = False
    BaseModel = object  # type: ignore
    ConfigDict = None  # type: ignore

    def Field(default=None, **kwargs):  # type: ignore
        return default

try:
    from prometheus_client import Counter, Histogram, Gauge, REGISTRY, generate_latest  # type: ignore
    _PROMETHEUS_AVAILABLE = True
except Exception:
    _PROMETHEUS_AVAILABLE = False
    Counter = Histogram = Gauge = None  # type: ignore
    REGISTRY = None  # type: ignore
    generate_latest = None  # type: ignore

try:
    from opentelemetry import trace as otel_trace  # type: ignore
    from opentelemetry.trace import Status, StatusCode  # type: ignore
    _OTEL_AVAILABLE = True
except Exception:
    otel_trace = None  # type: ignore
    Status = None  # type: ignore
    StatusCode = None  # type: ignore
    _OTEL_AVAILABLE = False

try:
    import aioredis  # type: ignore
    from aioredis import Redis  # type: ignore
    _REDIS_AVAILABLE = True
except Exception:
    aioredis = None  # type: ignore
    Redis = None  # type: ignore
    _REDIS_AVAILABLE = False

try:
    import aiomcache  # type: ignore
    _MEMCACHED_AVAILABLE = True
except Exception:
    aiomcache = None  # type: ignore
    _MEMCACHED_AVAILABLE = False

# =============================================================================
# Schema registry / page catalog
# =============================================================================
get_sheet_headers = None  # type: ignore
get_sheet_keys = None  # type: ignore
get_sheet_len = None  # type: ignore
get_sheet_spec = None  # type: ignore
_SCHEMA_AVAILABLE = False

for _schema_path in ("core.sheets.schema_registry", "core.schema_registry", "schema_registry"):
    try:
        _mod = import_module(_schema_path)
        _fh = getattr(_mod, "get_sheet_headers", None)
        _fk = getattr(_mod, "get_sheet_keys", None)
        if callable(_fh) and callable(_fk):
            get_sheet_headers = _fh
            get_sheet_keys = _fk
            get_sheet_len = getattr(_mod, "get_sheet_len", None)
            get_sheet_spec = getattr(_mod, "get_sheet_spec", None)
            _SCHEMA_AVAILABLE = True
            break
    except Exception:
        continue

del _schema_path

CANONICAL_PAGES: List[str] = []
FORBIDDEN_PAGES: Set[str] = {"KSA_Tadawul", "Advisor_Criteria"}


def allowed_pages() -> List[str]:
    return list(CANONICAL_PAGES) if CANONICAL_PAGES else []


def normalize_page_name(name: str, allow_output_pages: bool = True) -> str:
    return (name or "").strip().replace(" ", "_")


for _pcat_path in ("core.sheets.page_catalog", "core.page_catalog", "page_catalog"):
    try:
        _mod = import_module(_pcat_path)
        _ap = getattr(_mod, "allowed_pages", None)
        _np = getattr(_mod, "normalize_page_name", None)
        if callable(_ap):
            _cp = getattr(_mod, "CANONICAL_PAGES", None)
            _fp = getattr(_mod, "FORBIDDEN_PAGES", None)
            if _cp is not None:
                CANONICAL_PAGES[:] = list(_cp)
            if _fp is not None:
                FORBIDDEN_PAGES.clear()
                FORBIDDEN_PAGES.update(_fp)
            allowed_pages = _ap  # type: ignore[assignment]
            normalize_page_name = _np or normalize_page_name  # type: ignore[assignment]
            break
    except Exception:
        continue

del _pcat_path

# =============================================================================
# Config helpers
# =============================================================================
try:
    from core.config import auth_ok, get_settings_cached, is_open_mode  # type: ignore
except Exception:
    auth_ok = None  # type: ignore
    is_open_mode = None  # type: ignore

    def get_settings_cached(*args: Any, **kwargs: Any) -> Any:  # type: ignore
        return None

# =============================================================================
# Logging / flags
# =============================================================================
logger = logging.getLogger("core.data_engine")
logger.addHandler(logging.NullHandler())

_UTC = timezone.utc
_RIYADH_TZ = timezone(timedelta(hours=3))
_DEBUG = (os.getenv("DATA_ENGINE_DEBUG", "") or "").strip().lower() in {"1", "true", "yes", "y", "on"}
_STRICT_MODE = (os.getenv("DATA_ENGINE_STRICT", "") or "").strip().lower() in {"1", "true", "yes", "y", "on"}
_V2_DISABLED = (os.getenv("DATA_ENGINE_V2_DISABLED", "") or "").strip().lower() in {"1", "true", "yes", "y", "on"}
_PERF_MONITORING = (os.getenv("DATA_ENGINE_PERF_MONITORING", "") or "").strip().lower() in {"1", "true", "yes", "y", "on"}
_TRACING_ENABLED = (os.getenv("DATA_ENGINE_TRACING", "") or "").strip().lower() in {"1", "true", "yes", "y", "on"}
_SCHEMA_STRICT_SHEET_ROWS = (os.getenv("SCHEMA_STRICT_SHEET_ROWS", "") or "").strip().lower() not in {"0", "false", "no", "n", "off"}


def _dbg(msg: str, level: str = "info") -> None:
    if not _DEBUG:
        return
    try:
        ts = datetime.now(_UTC).strftime("%H:%M:%S.%f")[:-3]
        sys.stderr.write(f"[{ts}] [data_engine:{level.upper()}] {msg}\n")
    except Exception:
        pass


def _utc_now() -> datetime:
    return datetime.now(_UTC)


def _utc_now_iso() -> str:
    return _utc_now().isoformat()


# =============================================================================
# Tracing / perf helpers
# =============================================================================
class TraceContext:
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
            if self._span is not None and exc_val is not None and Status is not None and StatusCode is not None:
                try:
                    self._span.record_exception(exc_val)
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
    def decorator(func: Callable[..., Awaitable[Any]]) -> Callable[..., Awaitable[Any]]:
        @functools.wraps(func)
        async def wrapper(*args: Any, **kwargs: Any) -> Any:
            async with TraceContext(name or func.__name__, {"function": func.__name__}):
                return await func(*args, **kwargs)
        return wrapper
    return decorator


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


def record_perf_metric(metric: PerfMetrics) -> None:
    if not _PERF_MONITORING:
        return
    with _PERF_LOCK:
        _PERF_METRICS.append(metric)
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
                "timestamp": datetime.fromtimestamp(x.start_epoch, _UTC).isoformat(),
            }
            for x in items
        ]


def get_perf_stats(operation: Optional[str] = None) -> Dict[str, Any]:
    rows = get_perf_metrics(operation, limit=None)
    if not rows:
        return {}
    durations = sorted([r["duration_ms"] for r in rows if r["success"]])
    if not durations:
        return {"operation": operation or "all", "error": "no_success"}
    n = len(durations)
    return {
        "operation": operation or "all",
        "count": len(rows),
        "success_count": sum(1 for r in rows if r["success"]),
        "failure_count": sum(1 for r in rows if not r["success"]),
        "success_rate_pct": (sum(1 for r in rows if r["success"]) / len(rows)) * 100.0,
        "min_duration_ms": durations[0],
        "max_duration_ms": durations[-1],
        "avg_duration_ms": sum(durations) / n,
        "p50_duration_ms": durations[n // 2],
        "p95_duration_ms": durations[min(n - 1, int(n * 0.95))],
        "p99_duration_ms": durations[min(n - 1, int(n * 0.99))],
    }


def reset_perf_metrics() -> None:
    with _PERF_LOCK:
        _PERF_METRICS.clear()


def monitor_perf(operation: str, metadata: Optional[Dict[str, Any]] = None):
    def decorator(func: Callable[..., Awaitable[Any]]) -> Callable[..., Awaitable[Any]]:
        @functools.wraps(func)
        async def wrapper(*args: Any, **kwargs: Any) -> Any:
            t0 = time.perf_counter()
            start_epoch = time.time()
            meta = dict(metadata or {})
            try:
                value = await func(*args, **kwargs)
                record_perf_metric(PerfMetrics(operation=operation, start_epoch=start_epoch, duration_ms=(time.perf_counter() - t0) * 1000.0, success=True, metadata=meta))
                return value
            except Exception as e:
                record_perf_metric(PerfMetrics(operation=operation, start_epoch=start_epoch, duration_ms=(time.perf_counter() - t0) * 1000.0, success=False, error=str(e), metadata=meta))
                raise
        return wrapper
    return decorator

# =============================================================================
# Metrics
# =============================================================================
class _DummyMetric:
    def labels(self, *args, **kwargs):
        return self
    def inc(self, *args, **kwargs):
        return None
    def observe(self, *args, **kwargs):
        return None
    def set(self, *args, **kwargs):
        return None


class MetricsRegistry:
    def __init__(self, namespace: str = "data_engine"):
        self.namespace = namespace
        self._metrics: Dict[str, Any] = {}
        self._lock = threading.RLock()
        if _PROMETHEUS_AVAILABLE:
            self._init_metrics()

    def _init_metrics(self) -> None:
        self._metrics["requests_total"] = Counter(f"{self.namespace}_requests_total", "Total requests", ["operation", "status"])
        self._metrics["request_duration_seconds"] = Histogram(f"{self.namespace}_request_duration_seconds", "Request duration seconds", ["operation"], buckets=[0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10])
        self._metrics["cache_hits_total"] = Counter(f"{self.namespace}_cache_hits_total", "Cache hits", ["cache_type"])
        self._metrics["cache_misses_total"] = Counter(f"{self.namespace}_cache_misses_total", "Cache misses", ["cache_type"])
        self._metrics["circuit_breaker_state"] = Gauge(f"{self.namespace}_circuit_breaker_state", "Circuit breaker state", ["name"])
        self._metrics["provider_health"] = Gauge(f"{self.namespace}_provider_health", "Provider health", ["provider"])
        self._metrics["active_requests"] = Gauge(f"{self.namespace}_active_requests", "Active requests")

    def inc(self, name: str, value: float = 1.0, labels: Optional[Dict[str, str]] = None) -> None:
        metric = self._metrics.get(name)
        if metric is None:
            return
        if labels:
            metric.labels(**labels).inc(value)
        else:
            metric.inc(value)

    def observe(self, name: str, value: float, labels: Optional[Dict[str, str]] = None) -> None:
        metric = self._metrics.get(name)
        if metric is None:
            return
        if labels:
            metric.labels(**labels).observe(value)
        else:
            metric.observe(value)

    def set(self, name: str, value: float, labels: Optional[Dict[str, str]] = None) -> None:
        metric = self._metrics.get(name)
        if metric is None:
            return
        if labels:
            metric.labels(**labels).set(value)
        else:
            metric.set(value)

    def get_metrics_text(self) -> str:
        if not _PROMETHEUS_AVAILABLE or generate_latest is None or REGISTRY is None:
            return ""
        return generate_latest(REGISTRY).decode("utf-8")


_METRICS = MetricsRegistry()
if not _PROMETHEUS_AVAILABLE:
    _METRICS.inc = lambda *args, **kwargs: None  # type: ignore
    _METRICS.observe = lambda *args, **kwargs: None  # type: ignore
    _METRICS.set = lambda *args, **kwargs: None  # type: ignore

# =============================================================================
# Rate limiter / circuit breaker
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
                wait = (tokens - self.tokens) / self.rate if self.rate > 0 else 0.05
            await asyncio.sleep(min(1.0, max(0.01, wait)))

    def get_stats(self) -> Dict[str, Any]:
        util = 1.0 - (self.tokens / self.capacity) if self.capacity > 0 else 0.0
        return {"rate": self.rate, "capacity": self.capacity, "current_tokens": self.tokens, "utilization": util}


class DynamicCircuitBreaker:
    def __init__(self, name: str, failure_threshold: int = 5, success_threshold: int = 2, base_timeout_seconds: float = 30.0, max_timeout_seconds: float = 300.0):
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
                    raise RuntimeError(f"Circuit {self.name} is OPEN")
        try:
            value = func(*args, **kwargs)
            if inspect.isawaitable(value):
                value = await value
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
            return value
        except Exception:
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
            raise

    def get_stats(self) -> Dict[str, Any]:
        return {
            "name": self.name,
            "state": self.state.value,
            "failure_count": self.failure_count,
            "success_count": self.success_count,
            "total_failures": self.total_failures,
            "total_successes": self.total_successes,
            "current_timeout": self.current_timeout,
            "last_failure_time": datetime.fromtimestamp(self.last_failure_time, _UTC).isoformat() if self.last_failure_time else None,
            "last_success_time": datetime.fromtimestamp(self.last_success_time, _UTC).isoformat() if self.last_success_time else None,
        }

# =============================================================================
# Cache
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
    def __init__(self, backend: CacheBackend = CacheBackend.MEMORY, default_ttl: int = 300, redis_url: Optional[str] = None, memcached_servers: Optional[List[str]] = None, max_size: int = 10000, compression: bool = True):
        self.backend = backend
        self.default_ttl = int(default_ttl)
        self.max_size = int(max_size)
        self.compression = bool(compression)
        self._memory_cache: Dict[str, CacheEntry] = {}
        self._redis_client: Optional[Any] = None
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
                return zlib.compress(pickle.dumps(value))
            return json_dumps(value)
        except Exception:
            return b"{}"

    def _deserialize(self, value: bytes) -> Any:
        try:
            if self.compression:
                return pickle.loads(zlib.decompress(value))
            return json_loads(value)
        except Exception:
            return None

    async def get(self, namespace: str, key: str) -> Optional[Any]:
        ck = self._cache_key(namespace, key)
        async with self._lock:
            entry = self._memory_cache.get(ck)
            if entry is None:
                self._stats["misses"] += 1
                return None
            if entry.is_expired:
                self._memory_cache.pop(ck, None)
                self._stats["misses"] += 1
                return None
            entry.access_count += 1
            entry.last_access = time.time()
            self._stats["hits"] += 1
            return entry.value

    async def set(self, namespace: str, key: str, value: Any, ttl: Optional[int] = None) -> None:
        ttl_i = int(ttl) if ttl is not None else self.default_ttl
        ck = self._cache_key(namespace, key)
        async with self._lock:
            if len(self._memory_cache) >= self.max_size:
                oldest = sorted(self._memory_cache.items(), key=lambda kv: kv[1].last_access)[: max(1, self.max_size // 5)]
                for old_key, _ in oldest:
                    self._memory_cache.pop(old_key, None)
            self._memory_cache[ck] = CacheEntry(key=ck, value=value, expires_at=time.time() + ttl_i)
            self._stats["sets"] += 1

    async def delete(self, namespace: str, key: str) -> None:
        ck = self._cache_key(namespace, key)
        async with self._lock:
            self._memory_cache.pop(ck, None)
            self._stats["deletes"] += 1

    async def clear(self, namespace: Optional[str] = None) -> None:
        async with self._lock:
            if not namespace:
                self._memory_cache.clear()
                return
            prefix = f"{namespace}:"
            for key in list(self._memory_cache.keys()):
                if key.startswith(prefix):
                    self._memory_cache.pop(key, None)

    def get_stats(self) -> Dict[str, Any]:
        total = self._stats["hits"] + self._stats["misses"]
        return {
            "backend": self.backend.value,
            "hits": self._stats["hits"],
            "misses": self._stats["misses"],
            "sets": self._stats["sets"],
            "deletes": self._stats["deletes"],
            "hit_rate": (self._stats["hits"] / total) if total else 0.0,
            "memory_size": len(self._memory_cache),
            "memory_utilization": (len(self._memory_cache) / self.max_size) if self.max_size else 0.0,
        }

# =============================================================================
# Symbol helpers
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

    def _fallback_normalize(self, symbol: str) -> str:
        s = (str(symbol or "").strip().upper())
        if not s:
            return ""
        if s.startswith("TADAWUL:"):
            s = s.split(":", 1)[1].strip()
        if s.endswith(".SA"):
            s = s[:-3] + ".SR"
        if s.isdigit() and 3 <= len(s) <= 6:
            return f"{s}.SR"
        return s

    def _detect_market(self, symbol: str) -> str:
        s = symbol.upper()
        if s.endswith(".SR"):
            return "KSA"
        if any(ch in s for ch in ("=", "^", "/")):
            return "SPECIAL"
        if "." not in s:
            return "US"
        return "GLOBAL"

    def _detect_asset_class(self, symbol: str) -> str:
        s = symbol.upper()
        if s.startswith("^"):
            return "index"
        if s.endswith("=X") or "/" in s:
            return "forex"
        if s.endswith("=F"):
            return "commodity"
        return "equity"

    def _provider_hint(self, symbol: str) -> Optional[str]:
        s = symbol.upper()
        if s.endswith(".SR"):
            return "tadawul"
        if any(ch in s for ch in ("=", "^", "/")):
            return "yahoo"
        return None

    def normalize(self, symbol: str) -> str:
        raw = str(symbol or "").strip()
        if not raw:
            return ""
        with self._lock:
            if raw in self._cache:
                return self._cache[raw].normalized
        norm = self._fallback_normalize(raw)
        info = SymbolInfo(raw=raw, normalized=norm, market=self._detect_market(norm), asset_class=self._detect_asset_class(norm), provider_hint=self._provider_hint(norm))
        with self._lock:
            self._cache[raw] = info
        return norm

    def get_info(self, symbol: str) -> SymbolInfo:
        raw = str(symbol or "").strip()
        with self._lock:
            if raw in self._cache:
                return self._cache[raw]
        norm = self.normalize(raw)
        with self._lock:
            return self._cache[raw]

    def clean_symbols(self, symbols: Sequence[Any]) -> List[str]:
        seen: Set[str] = set()
        out: List[str] = []
        for item in symbols or []:
            raw = str(item or "").strip()
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
# Stub quote / engine
# =============================================================================
class StubUnifiedQuote(BaseModel):
    if _PYDANTIC_V2:
        model_config = ConfigDict(populate_by_name=True, extra="ignore")

    symbol: str
    original_symbol: Optional[str] = None
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
    price: Optional[float] = None
    change: Optional[float] = None
    change_pct: Optional[float] = None
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
                self.change_pct = float(self.current_price) / float(self.previous_close) - 1.0
                self.percent_change = self.change_pct
            except Exception:
                pass
        return self

    def dict(self, *args: Any, **kwargs: Any) -> Dict[str, Any]:
        if hasattr(self, "model_dump"):
            return self.model_dump(*args, **kwargs)  # type: ignore[attr-defined]
        return dict(self.__dict__)


class StubEngine:
    def __init__(self, *args: Any, **kwargs: Any) -> None:
        self._start = time.time()
        self._req = 0
        self._err = 0
        logger.warning("DataEngine initialized STUB engine (V2 unavailable)")

    async def get_quote(self, symbol: str) -> StubUnifiedQuote:
        self._req += 1
        return StubUnifiedQuote(symbol=normalize_symbol(symbol) or str(symbol), error="Engine V2 Missing", warnings=["stub"]).finalize()

    async def get_quotes(self, symbols: List[str]) -> List[StubUnifiedQuote]:
        return [await self.get_quote(s) for s in symbols or []]

    async def get_enriched_quote(self, symbol: str) -> StubUnifiedQuote:
        return await self.get_quote(symbol)

    async def get_enriched_quotes(self, symbols: List[str]) -> List[StubUnifiedQuote]:
        return await self.get_quotes(symbols)

    async def get_sheet_rows(self, *args: Any, **kwargs: Any) -> Dict[str, Any]:
        sheet = str(kwargs.get("sheet") or (args[0] if args else ""))
        return {
            "status": "error",
            "sheet": sheet,
            "page": sheet,
            "headers": [],
            "keys": [],
            "rows": [],
            "rows_matrix": [],
            "row_objects": [],
            "data": [],
            "items": [],
            "records": [],
            "quotes": [],
            "error": "Engine V2 Missing (stub)",
            "meta": {"mode": "stub", "builder": "stub"},
            "version": __version__,
        }

    async def aclose(self) -> None:
        return None

    def get_stats(self) -> Dict[str, Any]:
        return {"mode": "stub", "uptime_seconds": time.time() - self._start, "request_count": self._req, "error_count": self._err, "error": "Engine V2 Missing"}


def get_unified_quote_class() -> Type:
    try:
        mod = import_module("core.data_engine_v2")
        uq = getattr(mod, "UnifiedQuote", None)
        if uq is not None:
            return uq
    except Exception:
        pass
    try:
        mod = import_module("core.schemas")
        uq = getattr(mod, "UnifiedQuote", None)
        if uq is not None:
            return uq
    except Exception:
        pass
    return StubUnifiedQuote


UnifiedQuote = get_unified_quote_class()

# =============================================================================
# Engine discovery / manager
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
        self._mode = EngineMode.UNKNOWN
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
                info = V2ModuleInfo(
                    module=mod,
                    version=getattr(mod, "__version__", None) or getattr(mod, "VERSION", None),
                    engine_class=getattr(mod, "DataEngine", None) or getattr(mod, "Engine", None),
                    engine_v2_class=getattr(mod, "DataEngineV2", None),
                    engine_v3_class=getattr(mod, "DataEngineV3", None),
                    engine_v4_class=getattr(mod, "DataEngineV4", None),
                    engine_v5_class=getattr(mod, "DataEngineV5", None),
                    unified_quote_class=getattr(mod, "UnifiedQuote", None),
                    has_module_funcs=any(callable(getattr(mod, name, None)) for name in ("get_quote", "get_quotes", "get_enriched_quote", "get_enriched_quotes", "get_engine", "get_sheet_rows", "sheet_rows", "build_sheet_rows")),
                )
                if not any([info.engine_class, info.engine_v2_class, info.engine_v3_class, info.engine_v4_class, info.engine_v5_class, info.has_module_funcs, info.unified_quote_class]):
                    raise ImportError("No usable exports found in core.data_engine_v2")
                self._mode = EngineMode.V2 if any([info.engine_v2_class, info.engine_v3_class, info.engine_v4_class, info.engine_v5_class]) else EngineMode.LEGACY
                self._info = info
                return self._mode, self._info, None
            except Exception as e:
                self._mode = EngineMode.STUB
                self._error = str(e)
                logger.error("DataEngine V2 discovery failed; using STUB. %s\n%s", e, traceback.format_exc())
                return self._mode, None, self._error


_V2_DISCOVERY = V2Discovery()


class EngineManager:
    def __init__(self):
        self._async_lock: Optional[asyncio.Lock] = None
        self._sync_lock = threading.RLock()
        self._engine: Optional[Any] = None
        self._mode = EngineMode.UNKNOWN
        self._v2_info: Optional[V2ModuleInfo] = None
        self._error: Optional[str] = None
        self._circuit = DynamicCircuitBreaker("engine", failure_threshold=5, base_timeout_seconds=30.0, max_timeout_seconds=300.0)
        self._rate = TokenBucket(rate_per_sec=100.0)
        self._cache = DistributedCache(backend=CacheBackend.MEMORY, default_ttl=300, max_size=10000, compression=True)
        self._stats: Dict[str, Any] = {"created_at": _utc_now_iso(), "requests": 0, "errors": 0, "cache_hits": 0, "cache_misses": 0}

    def _get_async_lock(self) -> asyncio.Lock:
        if self._async_lock is None:
            self._async_lock = asyncio.Lock()
        return self._async_lock

    def _discover(self) -> Tuple[EngineMode, Optional[V2ModuleInfo], Optional[str]]:
        if self._mode != EngineMode.UNKNOWN:
            return self._mode, self._v2_info, self._error
        self._mode, self._v2_info, self._error = _V2_DISCOVERY.discover()
        return self._mode, self._v2_info, self._error

    def _instantiate_engine(self, engine_class: Type) -> Optional[Any]:
        try:
            return engine_class()
        except TypeError:
            pass
        except Exception:
            return None
        for mod_name in ("config", "core.config"):
            try:
                mod = import_module(mod_name)
                fn = getattr(mod, "get_settings", None)
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
        try:
            fn = getattr(info.module, "get_engine", None)
            if callable(fn):
                value = fn()
                if inspect.isawaitable(value):
                    value = await value
                if value is not None:
                    return value
        except Exception:
            pass
        for cls in (info.engine_v5_class, info.engine_v4_class, info.engine_v3_class, info.engine_v2_class, info.engine_class):
            if cls is None:
                continue
            engine = self._instantiate_engine(cls)
            if engine is not None:
                return engine
        return None

    async def get_engine(self) -> Any:
        if self._engine is not None:
            return self._engine
        async with self._get_async_lock():
            if self._engine is not None:
                return self._engine
            await self._rate.wait_and_acquire()
            async def _create() -> Any:
                engine = await self._get_v2_engine()
                if engine is not None:
                    self._engine = engine
                    _METRICS.set("provider_health", 1, {"provider": "engine_v2"})
                    return engine
                self._engine = StubEngine()
                _METRICS.set("provider_health", -1, {"provider": "stub"})
                return self._engine
            return await self._circuit.execute(_create)

    async def close_engine(self) -> None:
        if self._engine is None:
            return
        engine = self._engine
        self._engine = None
        try:
            fn = getattr(engine, "aclose", None)
            if callable(fn):
                value = fn()
                if inspect.isawaitable(value):
                    await value
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
            stats = dict(self._stats)
        stats["mode"] = self._mode.value
        stats["v2_available"] = self._mode in (EngineMode.V2, EngineMode.LEGACY)
        stats["engine_active"] = self._engine is not None
        stats["circuit_breaker"] = self._circuit.get_stats()
        stats["rate_limiter"] = self._rate.get_stats()
        stats["cache"] = self._cache.get_stats()
        stats["schema_available"] = bool(_SCHEMA_AVAILABLE)
        stats["schema_strict_sheet_rows"] = bool(_SCHEMA_STRICT_SHEET_ROWS)
        try:
            if self._engine is not None and hasattr(self._engine, "get_stats"):
                stats["engine_stats"] = self._engine.get_stats()
        except Exception:
            pass
        return stats


_ENGINE_MANAGER = EngineManager()

# =============================================================================
# Public engine helpers
# =============================================================================
async def get_engine() -> Any:
    return await _ENGINE_MANAGER.get_engine()


def _run_coro_sync(coro: Awaitable[Any]) -> Any:
    try:
        asyncio.get_running_loop()
    except RuntimeError:
        return asyncio.run(coro)
    result_box: Dict[str, Any] = {}
    def _runner() -> None:
        loop = asyncio.new_event_loop()
        try:
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


def get_engine_sync() -> Any:
    return _run_coro_sync(_ENGINE_MANAGER.get_engine())


async def close_engine() -> None:
    await _ENGINE_MANAGER.close_engine()


def get_cache() -> DistributedCache:
    return _ENGINE_MANAGER.get_cache()

# =============================================================================
# Generic helpers
# =============================================================================
def _safe_str(x: Any) -> str:
    try:
        return str(x).strip()
    except Exception:
        return ""


def _safe_upper(x: Any) -> str:
    return _safe_str(x).upper()


def _maybe_await(value: Any) -> Awaitable[Any]:
    if inspect.isawaitable(value):
        return value  # type: ignore[return-value]
    async def _wrap() -> Any:
        return value
    return _wrap()


def _unwrap_payload(value: Any) -> Any:
    try:
        if isinstance(value, tuple) and len(value) >= 1:
            return value[0]
    except Exception:
        pass
    return value


def _as_list(value: Any) -> List[Any]:
    if value is None:
        return []
    if isinstance(value, list):
        return value
    if isinstance(value, dict):
        return list(value.values())
    if isinstance(value, (set, tuple)):
        return list(value)
    return [value]


def _jsonable_snapshot(value: Any) -> Any:
    if value is None or isinstance(value, (str, int, bool)):
        return value
    if isinstance(value, float):
        return None if (math.isnan(value) or math.isinf(value)) else value
    if isinstance(value, bytes):
        try:
            return value.decode("utf-8", errors="replace")
        except Exception:
            return str(value)
    if isinstance(value, Mapping):
        return {str(k): _jsonable_snapshot(v) for k, v in value.items()}
    if isinstance(value, (list, tuple, set)):
        return [_jsonable_snapshot(v) for v in value]
    try:
        if hasattr(value, "model_dump") and callable(getattr(value, "model_dump")):
            return _jsonable_snapshot(value.model_dump(mode="python"))  # type: ignore[attr-defined]
    except Exception:
        pass
    try:
        if hasattr(value, "dict") and callable(getattr(value, "dict")):
            return _jsonable_snapshot(value.dict())  # type: ignore[attr-defined]
    except Exception:
        pass
    try:
        if is_dataclass(value):
            return _jsonable_snapshot(asdict(value))
    except Exception:
        pass
    try:
        return _jsonable_snapshot(vars(value))
    except Exception:
        return str(value)


def _model_to_dict(obj: Any) -> Dict[str, Any]:
    if obj is None:
        return {}
    if isinstance(obj, dict):
        return dict(obj)
    if isinstance(obj, Mapping):
        return dict(obj)
    try:
        if hasattr(obj, "model_dump") and callable(getattr(obj, "model_dump")):
            value = obj.model_dump(mode="python")  # type: ignore[attr-defined]
            if isinstance(value, dict):
                return value
    except Exception:
        pass
    try:
        if hasattr(obj, "dict") and callable(getattr(obj, "dict")):
            value = obj.dict()  # type: ignore[attr-defined]
            if isinstance(value, dict):
                return value
    except Exception:
        pass
    try:
        if is_dataclass(obj):
            value = asdict(obj)
            if isinstance(value, dict):
                return value
    except Exception:
        pass
    try:
        data = getattr(obj, "__dict__", None)
        if isinstance(data, dict):
            return dict(data)
    except Exception:
        pass
    snap = _jsonable_snapshot(obj)
    return snap if isinstance(snap, dict) else {}


def _coerce_symbol_from_payload(payload: Any) -> Optional[str]:
    if payload is None:
        return None
    if isinstance(payload, dict):
        for key in ("symbol", "requested_symbol", "ticker", "code", "id"):
            value = payload.get(key)
            if value:
                return normalize_symbol(str(value))
        return None
    for key in ("symbol", "requested_symbol", "ticker", "code", "id"):
        try:
            value = getattr(payload, key, None)
            if value:
                return normalize_symbol(str(value))
        except Exception:
            continue
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
            dq = _safe_upper(obj.get("data_quality"))
            return dq in {"MISSING", "ERROR"}
        if getattr(obj, "error", None):
            return True
        dq = _safe_upper(getattr(obj, "data_quality", ""))
        return dq in {"MISSING", "ERROR"}
    except Exception:
        return False

# =============================================================================
# Emergency data rescuer
# =============================================================================
class EmergencyDataRescuer:
    @classmethod
    async def rescue_quote(cls, quote_obj: Any, symbol: str) -> Any:
        norm_symbol = normalize_symbol(symbol) or _safe_str(symbol)
        if not norm_symbol:
            return quote_obj
        if quote_obj is None:
            UQ = get_unified_quote_class()
            quote_obj = UQ(symbol=norm_symbol, data_quality="MISSING", error="Upstream returned None")
        is_dict = isinstance(quote_obj, dict)
        try:
            price = quote_obj.get("current_price") if is_dict else getattr(quote_obj, "current_price", None)
            if price in (None, ""):
                price = quote_obj.get("price") if is_dict else getattr(quote_obj, "price", None)
            if price not in (None, ""):
                try:
                    if float(price) > 0:
                        return quote_obj
                except Exception:
                    pass
        except Exception:
            pass
        # Keep rescuer conservative in this adapter module.
        return quote_obj

# =============================================================================
# Sheet schema / constants
# =============================================================================
_TOP10_PAGE = "Top_10_Investments"
_INSIGHTS_PAGE = "Insights_Analysis"
_DICTIONARY_PAGE = "Data_Dictionary"
_SPECIAL_PAGES = {_TOP10_PAGE, _INSIGHTS_PAGE, _DICTIONARY_PAGE}

_EXPECTED_SHEET_LENGTHS: Dict[str, int] = {
    "Market_Leaders": 80,
    "Global_Markets": 80,
    "Commodities_FX": 80,
    "Mutual_Funds": 80,
    "My_Portfolio": 80,
    "My_Investments": 80,
    _TOP10_PAGE: 83,
    _INSIGHTS_PAGE: 7,
    _DICTIONARY_PAGE: 9,
}

_CANONICAL_80_HEADERS: List[str] = [
    "Symbol", "Name", "Asset Class", "Exchange", "Currency", "Country", "Sector", "Industry",
    "Current Price", "Previous Close", "Open", "Day High", "Day Low", "52W High", "52W Low",
    "Price Change", "Percent Change", "52W Position %", "Volume", "Avg Volume 10D", "Avg Volume 30D",
    "Market Cap", "Float Shares", "Beta (5Y)", "P/E (TTM)", "P/E (Forward)", "EPS (TTM)",
    "Dividend Yield", "Payout Ratio", "Revenue (TTM)", "Revenue Growth YoY", "Gross Margin",
    "Operating Margin", "Profit Margin", "Debt/Equity", "Free Cash Flow (TTM)", "RSI (14)",
    "Volatility 30D", "Volatility 90D", "Max Drawdown 1Y", "VaR 95% (1D)", "Sharpe (1Y)",
    "Risk Score", "Risk Bucket", "P/B", "P/S", "EV/EBITDA", "PEG", "Intrinsic Value",
    "Valuation Score", "Forecast Price 1M", "Forecast Price 3M", "Forecast Price 12M",
    "Expected ROI 1M", "Expected ROI 3M", "Expected ROI 12M", "Forecast Confidence",
    "Confidence Score", "Confidence Bucket", "Value Score", "Quality Score", "Momentum Score",
    "Growth Score", "Overall Score", "Opportunity Score", "Rank (Overall)", "Recommendation",
    "Recommendation Reason", "Horizon Days", "Invest Period Label", "Position Qty", "Avg Cost",
    "Position Cost", "Position Value", "Unrealized P/L", "Unrealized P/L %", "Data Provider",
    "Last Updated (UTC)", "Last Updated (Riyadh)", "Warnings",
]

_CANONICAL_80_KEYS: List[str] = [
    "symbol", "name", "asset_class", "exchange", "currency", "country", "sector", "industry",
    "current_price", "previous_close", "open_price", "day_high", "day_low", "week_52_high", "week_52_low",
    "price_change", "percent_change", "week_52_position_pct", "volume", "avg_volume_10d", "avg_volume_30d",
    "market_cap", "float_shares", "beta_5y", "pe_ttm", "pe_forward", "eps_ttm",
    "dividend_yield", "payout_ratio", "revenue_ttm", "revenue_growth_yoy", "gross_margin",
    "operating_margin", "profit_margin", "debt_to_equity", "free_cash_flow_ttm", "rsi_14",
    "volatility_30d", "volatility_90d", "max_drawdown_1y", "var_95_1d", "sharpe_1y",
    "risk_score", "risk_bucket", "pb_ratio", "ps_ratio", "ev_ebitda", "peg_ratio", "intrinsic_value",
    "valuation_score", "forecast_price_1m", "forecast_price_3m", "forecast_price_12m",
    "expected_roi_1m", "expected_roi_3m", "expected_roi_12m", "forecast_confidence",
    "confidence_score", "confidence_bucket", "value_score", "quality_score", "momentum_score",
    "growth_score", "overall_score", "opportunity_score", "rank_overall", "recommendation",
    "recommendation_reason", "horizon_days", "invest_period_label", "position_qty", "avg_cost",
    "position_cost", "position_value", "unrealized_pl", "unrealized_pl_pct", "data_provider",
    "last_updated_utc", "last_updated_riyadh", "warnings",
]

_INSIGHTS_HEADERS = ["Section", "Item", "Symbol", "Recommendation", "Confidence", "Notes", "Last Updated (Riyadh)"]
_INSIGHTS_KEYS = ["section", "item", "symbol", "recommendation", "confidence", "notes", "last_updated_riyadh"]
_DICTIONARY_HEADERS = ["Sheet", "Group", "Header", "Key", "DType", "Format", "Required", "Source", "Notes"]
_DICTIONARY_KEYS = ["sheet", "group", "header", "key", "dtype", "fmt", "required", "source", "notes"]
_TOP10_REQUIRED_FIELDS = ("top10_rank", "selection_reason", "criteria_snapshot")
_TOP10_REQUIRED_HEADERS = {
    "top10_rank": "Top10 Rank",
    "selection_reason": "Selection Reason",
    "criteria_snapshot": "Criteria Snapshot",
}

_DEFAULT_SHEET_SYMBOLS: Dict[str, List[str]] = {
    "Market_Leaders": ["2222.SR", "1120.SR", "2010.SR", "7010.SR", "AAPL", "MSFT", "NVDA", "GOOGL"],
    "Global_Markets": ["AAPL", "MSFT", "NVDA", "AMZN", "META", "GOOGL", "TSLA", "AVGO"],
    "Commodities_FX": ["GC=F", "BZ=F", "SI=F", "EURUSD=X", "GBPUSD=X", "JPY=X", "SAR=X", "CL=F"],
    "Mutual_Funds": ["SPY", "QQQ", "VTI", "VOO", "IWM"],
    "My_Portfolio": ["2222.SR", "AAPL", "MSFT", "QQQ", "GC=F"],
    "My_Investments": ["2222.SR", "AAPL", "MSFT"],
    _INSIGHTS_PAGE: ["2222.SR", "AAPL", "GC=F"],
    _TOP10_PAGE: ["2222.SR", "1120.SR", "AAPL", "MSFT", "NVDA"],
}

_SPECIAL_SHEET_CANONICAL: Dict[str, str] = {
    "insights_analysis": _INSIGHTS_PAGE,
    "top_10_investments": _TOP10_PAGE,
    "top10investments": _TOP10_PAGE,
    "data_dictionary": _DICTIONARY_PAGE,
}

_SPECIAL_SHEET_METHODS: Dict[str, List[str]] = {
    _INSIGHTS_PAGE: ["build_insights_analysis", "get_insights_analysis", "build_insights_sheet_rows", "get_insights_sheet_rows", "build_insights_rows", "get_insights_rows"],
    _TOP10_PAGE: ["build_top_10_investments", "build_top10_investments", "get_top_10_investments", "get_top10_investments", "build_top10_sheet_rows", "get_top10_sheet_rows", "build_top10_rows", "get_top10_rows"],
    _DICTIONARY_PAGE: ["build_data_dictionary", "get_data_dictionary", "build_data_dictionary_sheet_rows", "get_data_dictionary_sheet_rows", "build_data_dictionary_rows", "get_data_dictionary_rows"],
}

_FIELD_ALIAS_HINTS: Dict[str, List[str]] = {
    "symbol": ["ticker", "code", "instrument", "requested_symbol"],
    "name": ["short_name", "long_name", "instrument_name"],
    "current_price": ["price", "last_price", "last", "close", "market_price", "value", "nav"],
    "previous_close": ["prev_close", "previousclose"],
    "open_price": ["open"],
    "day_high": ["high", "high_price"],
    "day_low": ["low", "low_price"],
    "forecast_confidence": ["confidence", "ai_confidence"],
    "confidence_bucket": ["confidence", "confidence_label"],
    "top10_rank": ["rank", "top_rank"],
    "selection_reason": ["reason", "selection_notes"],
    "criteria_snapshot": ["criteria", "snapshot", "criteria_json"],
    "position_qty": ["qty", "quantity"],
    "avg_cost": ["average_cost", "cost_basis"],
    "position_value": ["market_value", "current_value"],
    "unrealized_pl": ["upl", "unrealized_profit_loss"],
    "notes": ["detail", "message", "description"],
    "recommendation": ["signal", "rating"],
}

# =============================================================================
# Sheet-rows helpers
# =============================================================================
def _boolish(value: Any, default: bool = False) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return bool(int(value))
    if isinstance(value, str):
        return value.strip().lower() in {"1", "true", "yes", "y", "on"}
    return default


def _candidate_method_names(enriched: bool, batch: bool) -> List[str]:
    if enriched and batch:
        return ["get_enriched_quotes", "get_enriched_quote_batch", "fetch_enriched_quotes", "fetch_enriched_quote_batch", "get_quotes_enriched", "get_quotes"]
    if enriched and not batch:
        return ["get_enriched_quote", "fetch_enriched_quote", "enriched_quote", "get_enriched", "get_quote"]
    if (not enriched) and batch:
        return ["get_quotes", "fetch_quotes", "quotes", "fetch_many", "get_quote_batch", "get_quotes_batch"]
    return ["get_quote", "fetch_quote", "quote", "fetch", "get"]


def _candidate_sheet_rows_methods() -> List[str]:
    return ["get_sheet_rows", "sheet_rows", "build_sheet_rows"]


def _canonicalize_sheet_name(sheet: Any) -> str:
    raw = _safe_str(sheet)
    if not raw:
        return ""
    compact = raw.strip().lower().replace(" ", "_").replace("-", "_")
    compact = compact.replace("__", "_")
    return _SPECIAL_SHEET_CANONICAL.get(compact, raw)


def _resolve_sheet_from_inputs(sheet: Any, body: Optional[Dict[str, Any]] = None) -> str:
    body = body or {}
    primary = _canonicalize_sheet_name(sheet)
    if primary:
        return primary
    for key in ("sheet", "page", "sheet_name", "name", "tab", "worksheet"):
        value = _canonicalize_sheet_name(body.get(key))
        if value:
            return value
    return ""


def _is_special_sheet(sheet: str) -> bool:
    return _canonicalize_sheet_name(sheet) in _SPECIAL_PAGES


def _stable_body_for_cache(body: Optional[Dict[str, Any]]) -> str:
    body = dict(body or {})
    for noisy_key in ("request_id", "trace_id", "ts", "timestamp", "_ts", "_rid"):
        body.pop(noisy_key, None)
    try:
        return json_dumps(body).decode("utf-8", errors="ignore")
    except Exception:
        try:
            return str(sorted(body.items()))
        except Exception:
            return ""


def _expected_len(sheet: str) -> int:
    if callable(get_sheet_len):
        try:
            n = int(get_sheet_len(sheet))  # type: ignore[misc]
            if n > 0:
                return n
        except Exception:
            pass
    return _EXPECTED_SHEET_LENGTHS.get(sheet, 80)


def _normalize_key_name(header: str) -> str:
    return "_".join(part for part in "".join(ch.lower() if ch.isalnum() else "_" for ch in _safe_str(header)).split("_") if part)


def _complete_schema_contract(headers: Sequence[str], keys: Sequence[str]) -> Tuple[List[str], List[str]]:
    headers = list(headers or [])
    keys = list(keys or [])
    max_len = max(len(headers), len(keys))
    out_headers: List[str] = []
    out_keys: List[str] = []
    for i in range(max_len):
        h = _safe_str(headers[i]) if i < len(headers) else ""
        k = _safe_str(keys[i]) if i < len(keys) else ""
        if h and not k:
            k = _normalize_key_name(h)
        elif k and not h:
            h = k.replace("_", " ").title()
        elif not h and not k:
            h = f"Column {i + 1}"
            k = f"column_{i + 1}"
        out_headers.append(h)
        out_keys.append(k)
    return out_headers, out_keys


def _pad_contract(headers: Sequence[str], keys: Sequence[str], expected_len: int, *, header_prefix: str = "Column", key_prefix: str = "column") -> Tuple[List[str], List[str]]:
    hdrs, ks = _complete_schema_contract(headers, keys)
    while len(hdrs) < expected_len:
        i = len(hdrs) + 1
        hdrs.append(f"{header_prefix} {i}")
        ks.append(f"{key_prefix}_{i}")
    return hdrs[:expected_len], ks[:expected_len]


def _ensure_top10_contract(headers: Sequence[str], keys: Sequence[str]) -> Tuple[List[str], List[str]]:
    hdrs, ks = _complete_schema_contract(headers, keys)
    for field_name in _TOP10_REQUIRED_FIELDS:
        if field_name not in ks:
            ks.append(field_name)
            hdrs.append(_TOP10_REQUIRED_HEADERS[field_name])
    return _pad_contract(hdrs, ks, 83)


def _static_contract(sheet: str) -> Tuple[List[str], List[str], str]:
    if sheet == _TOP10_PAGE:
        headers, keys = _ensure_top10_contract(_CANONICAL_80_HEADERS, _CANONICAL_80_KEYS)
        return headers, keys, "static_canonical_top10"
    if sheet == _INSIGHTS_PAGE:
        headers, keys = _pad_contract(_INSIGHTS_HEADERS, _INSIGHTS_KEYS, 7)
        return headers, keys, "static_canonical_insights"
    if sheet == _DICTIONARY_PAGE:
        headers, keys = _pad_contract(_DICTIONARY_HEADERS, _DICTIONARY_KEYS, 9)
        return headers, keys, "static_canonical_dictionary"
    headers, keys = _pad_contract(_CANONICAL_80_HEADERS, _CANONICAL_80_KEYS, _expected_len(sheet))
    return headers, keys, "static_canonical_instrument"


def _extract_headers_keys_from_spec(spec: Any) -> Tuple[List[str], List[str]]:
    headers: List[str] = []
    keys: List[str] = []
    if isinstance(spec, Mapping):
        headers_raw = spec.get("headers") or spec.get("display_headers") or spec.get("sheet_headers")
        keys_raw = spec.get("keys") or spec.get("fields") or spec.get("columns")
        if isinstance(headers_raw, list):
            headers = [_safe_str(x) for x in headers_raw if _safe_str(x)]
        if isinstance(keys_raw, list):
            keys = [_safe_str(x) for x in keys_raw if _safe_str(x)]
        if headers or keys:
            return _complete_schema_contract(headers, keys)
        cols = spec.get("columns") or spec.get("fields")
        if isinstance(cols, list):
            for col in cols:
                if isinstance(col, Mapping):
                    h = _safe_str(col.get("header") or col.get("display_header") or col.get("label") or col.get("title"))
                    k = _safe_str(col.get("key") or col.get("field") or col.get("name") or col.get("id"))
                    if h or k:
                        headers.append(h or k.replace("_", " ").title())
                        keys.append(k or _normalize_key_name(h))
    return _complete_schema_contract(headers, keys)


def _schema_from_registry(sheet: str) -> Tuple[List[str], List[str], Any, str]:
    spec = None
    if callable(get_sheet_headers) and callable(get_sheet_keys):
        try:
            headers = [_safe_str(x) for x in get_sheet_headers(sheet) if _safe_str(x)]  # type: ignore[misc]
            keys = [_safe_str(x) for x in get_sheet_keys(sheet) if _safe_str(x)]  # type: ignore[misc]
            if headers and keys:
                if callable(get_sheet_spec):
                    try:
                        spec = get_sheet_spec(sheet)  # type: ignore[misc]
                    except Exception:
                        spec = None
                ch, ck = _complete_schema_contract(headers, keys)
                return ch, ck, spec, "schema_registry.helpers"
        except Exception:
            pass
    if callable(get_sheet_spec):
        try:
            spec = get_sheet_spec(sheet)  # type: ignore[misc]
            headers, keys = _extract_headers_keys_from_spec(spec)
            return headers, keys, spec, "schema_registry.spec"
        except Exception as e:
            return [], [], None, f"registry_error:{e}"
    return [], [], None, "registry_unavailable"


def _resolve_contract(sheet: str) -> Tuple[List[str], List[str], Any, str]:
    expected_len = _expected_len(sheet)
    headers, keys, spec, source = _schema_from_registry(sheet)
    if headers and keys:
        headers, keys = _complete_schema_contract(headers, keys)
        if sheet == _TOP10_PAGE:
            headers, keys = _ensure_top10_contract(headers, keys)
        else:
            headers, keys = _pad_contract(headers, keys, expected_len)
        return headers, keys, spec, source
    headers, keys, source = _static_contract(sheet)
    return headers, keys, {"source": source, "sheet": sheet}, source


def _key_variants(key: str) -> List[str]:
    key = _safe_str(key)
    if not key:
        return []
    variants = [key, key.lower(), key.upper(), key.replace("_", " "), key.replace("_", "").lower()]
    for alias in _FIELD_ALIAS_HINTS.get(key, []):
        variants.extend([alias, alias.lower(), alias.upper(), alias.replace("_", " "), alias.replace("_", "").lower()])
    seen: Set[str] = set()
    out: List[str] = []
    for value in variants:
        if value and value not in seen:
            seen.add(value)
            out.append(value)
    return out


def _extract_from_raw(raw: Dict[str, Any], candidates: Sequence[str]) -> Any:
    raw_ci = {str(k).strip().lower(): v for k, v in raw.items()}
    raw_comp = {"".join(ch for ch in str(k).lower() if ch.isalnum()): v for k, v in raw.items()}
    for candidate in candidates:
        if candidate in raw:
            return raw[candidate]
        lc = candidate.lower()
        if lc in raw_ci:
            return raw_ci[lc]
        comp = "".join(ch for ch in candidate.lower() if ch.isalnum())
        if comp in raw_comp:
            return raw_comp[comp]
    return None


def _normalize_to_schema_keys(schema_keys: Sequence[str], schema_headers: Sequence[str], raw: Dict[str, Any]) -> Dict[str, Any]:
    raw = raw or {}
    header_by_key = {str(k): str(h) for k, h in zip(schema_keys, schema_headers)}
    out: Dict[str, Any] = {}
    for key in schema_keys:
        ks = str(key)
        value = _extract_from_raw(raw, _key_variants(ks))
        if value is None:
            header = header_by_key.get(ks, "")
            if header:
                value = _extract_from_raw(raw, [header, header.lower(), header.upper()])
        if isinstance(value, (list, tuple, set)) and ks in {"warnings", "recommendation_reason", "selection_reason", "notes"}:
            value = "; ".join(_safe_str(x) for x in value if _safe_str(x))
        out[ks] = _jsonable_snapshot(value)
    return out


def _project_row(keys: Sequence[str], row: Dict[str, Any]) -> Dict[str, Any]:
    return {str(k): row.get(str(k), None) for k in keys}


def _rows_to_matrix(rows: Sequence[Mapping[str, Any]], keys: Sequence[str]) -> List[List[Any]]:
    return [[_jsonable_snapshot(dict(row).get(str(k))) for k in keys] for row in rows or []]


def _rows_from_matrix(matrix: Any, keys: Sequence[str]) -> List[Dict[str, Any]]:
    if not isinstance(matrix, list) or not keys:
        return []
    out: List[Dict[str, Any]] = []
    for row in matrix:
        if isinstance(row, dict):
            out.append(_project_row(keys, row))
        elif isinstance(row, (list, tuple)):
            out.append({str(k): _jsonable_snapshot(row[i] if i < len(row) else None) for i, k in enumerate(keys)})
    return out


def _extract_payload_rows(payload: Dict[str, Any], keys_hint: Sequence[str], headers_hint: Sequence[str]) -> List[Dict[str, Any]]:
    for bucket in ("row_objects", "rows", "data", "items", "records", "quotes", "results"):
        rr = payload.get(bucket)
        if isinstance(rr, list):
            dict_rows = [dict(r) for r in rr if isinstance(r, Mapping)]
            if dict_rows:
                return dict_rows
            if rr and all(isinstance(r, (list, tuple, dict)) for r in rr) and keys_hint:
                return _rows_from_matrix(rr, keys_hint)
    matrix = payload.get("rows_matrix") or payload.get("matrix")
    if isinstance(matrix, list):
        return _rows_from_matrix(matrix, keys_hint)
    one = payload.get("row")
    if isinstance(one, Mapping):
        return [dict(one)]
    if keys_hint and any(k in payload for k in keys_hint):
        return [{str(k): payload.get(str(k)) for k in keys_hint}]
    if headers_hint and any(h in payload for h in headers_hint):
        return [{str(k): payload.get(str(h)) for k, h in zip(keys_hint, headers_hint)}]
    return []


def _slice(rows: List[Dict[str, Any]], *, limit: int, offset: int) -> List[Dict[str, Any]]:
    start = max(0, int(offset))
    if limit <= 0:
        return rows[start:]
    return rows[start:start + int(limit)]


def _to_number(value: Any) -> float:
    if value is None:
        return float("-inf")
    if isinstance(value, bool):
        return float(int(value))
    if isinstance(value, (int, float)):
        try:
            f = float(value)
            return f if math.isfinite(f) else float("-inf")
        except Exception:
            return float("-inf")
    s = _safe_str(value).replace("%", "").replace(",", "")
    if not s:
        return float("-inf")
    try:
        f = float(s)
        return f if math.isfinite(f) else float("-inf")
    except Exception:
        return float("-inf")


def _top10_sort_key(row: Mapping[str, Any]) -> Tuple[float, ...]:
    return (
        _to_number(row.get("overall_score")),
        _to_number(row.get("opportunity_score")),
        _to_number(row.get("expected_roi_3m")),
        _to_number(row.get("expected_roi_1m")),
        _to_number(row.get("forecast_confidence")),
        _to_number(row.get("confidence_score")),
    )


def _top10_selection_reason(row: Mapping[str, Any]) -> str:
    parts: List[str] = []
    for key, label in (("overall_score", "Overall"), ("opportunity_score", "Opportunity"), ("expected_roi_3m", "Exp ROI 3M"), ("forecast_confidence", "Forecast Conf")):
        value = row.get(key)
        if value not in (None, "", [], {}, ()):
            parts.append(f"{label} {round(value, 2) if isinstance(value, float) else value}")
            if len(parts) >= 3:
                break
    return " | ".join(parts) if parts else "Top10 selection based on strongest available composite signals."


def _top10_criteria_snapshot(row: Mapping[str, Any]) -> str:
    snapshot: Dict[str, Any] = {}
    for key in ("overall_score", "opportunity_score", "expected_roi_1m", "expected_roi_3m", "forecast_confidence", "confidence_score", "risk_bucket", "recommendation", "symbol"):
        value = row.get(key)
        if value not in (None, "", [], {}, ()):
            snapshot[key] = _jsonable_snapshot(value)
    try:
        return json.dumps(snapshot, ensure_ascii=False, sort_keys=True)
    except Exception:
        return str(snapshot)


def _ensure_top10_rows(rows: Sequence[Mapping[str, Any]], *, requested_symbols: Sequence[str], top_n: int, schema_keys: Sequence[str], schema_headers: Sequence[str]) -> List[Dict[str, Any]]:
    normalized_rows = [_normalize_to_schema_keys(schema_keys, schema_headers, dict(r or {})) for r in rows or []]
    deduped: List[Dict[str, Any]] = []
    seen: Set[str] = set()
    for row in sorted(normalized_rows, key=_top10_sort_key, reverse=True):
        sym = _safe_str(row.get("symbol"))
        name = _safe_str(row.get("name"))
        dedupe_key = sym or name or f"row_{len(deduped) + 1}"
        if dedupe_key in seen:
            continue
        seen.add(dedupe_key)
        deduped.append(row)
    final_rows = deduped[: max(1, int(top_n))]
    for idx, row in enumerate(final_rows, start=1):
        row["top10_rank"] = idx
        if not _safe_str(row.get("selection_reason")):
            row["selection_reason"] = _top10_selection_reason(row)
        if not _safe_str(row.get("criteria_snapshot")):
            row["criteria_snapshot"] = _top10_criteria_snapshot(row)
    return final_rows


def _extract_requested_symbols_from_body(body: Dict[str, Any], limit: int = 50) -> List[str]:
    out: List[str] = []
    seen: Set[str] = set()
    def _append_many(value: Any) -> None:
        if value is None:
            return
        if isinstance(value, str):
            parts = [x.strip() for x in value.replace(";", ",").replace("\n", ",").split(",") if x.strip()]
        elif isinstance(value, (list, tuple, set)):
            parts = [_safe_str(x) for x in value if _safe_str(x)]
        else:
            s = _safe_str(value)
            parts = [s] if s else []
        for part in parts:
            norm = normalize_symbol(part)
            if norm and norm not in seen:
                seen.add(norm)
                out.append(norm)
                if len(out) >= limit:
                    return
    for key in ("direct_symbols", "symbols", "tickers", "tickers_list", "selected_symbols", "selected_tickers", "requested_symbol", "symbol", "ticker", "code"):
        _append_many(body.get(key))
        if len(out) >= limit:
            break
    return out[:limit]


def _enrich_row_identity(row: Dict[str, Any], symbol: str) -> Dict[str, Any]:
    rr = dict(row or {})
    if not rr.get("symbol"):
        rr["symbol"] = symbol
    info = get_symbol_info(symbol)
    rr.setdefault("market", info.market)
    rr.setdefault("asset_class", info.asset_class)
    if info.provider_hint:
        rr.setdefault("provider_hint", info.provider_hint)
    if symbol.endswith(".SR"):
        rr.setdefault("exchange", "Tadawul")
        rr.setdefault("currency", "SAR")
        rr.setdefault("country", "Saudi Arabia")
    elif symbol.endswith("=F"):
        rr.setdefault("exchange", "Futures")
        rr.setdefault("currency", "USD")
        rr.setdefault("country", "Global")
    elif symbol.endswith("=X"):
        rr.setdefault("exchange", "FX")
        rr.setdefault("currency", "USD")
        rr.setdefault("country", "Global")
    else:
        rr.setdefault("exchange", "NASDAQ/NYSE")
        rr.setdefault("currency", "USD")
        rr.setdefault("country", "Global")
    return rr

# =============================================================================
# Placeholder / degraded detection
# =============================================================================
def _payload_error_text(payload: Dict[str, Any]) -> str:
    parts: List[str] = []
    for key in ("error", "detail", "message", "warning", "warnings"):
        value = payload.get(key)
        if isinstance(value, (list, tuple, set)):
            parts.extend([_safe_str(x) for x in value if _safe_str(x)])
        else:
            s = _safe_str(value)
            if s:
                parts.append(s)
    meta = payload.get("meta") if isinstance(payload.get("meta"), dict) else {}
    for key in ("dispatch", "upstream_error", "proxy_status", "best_status", "path", "builder"):
        s = _safe_str(meta.get(key))
        if s:
            parts.append(s)
    return " | ".join(parts).lower()


def _row_looks_placeholder(row: Dict[str, Any]) -> bool:
    provider = _safe_str(row.get("data_provider") or row.get("provider") or row.get("source")).lower()
    warnings = _safe_str(row.get("warnings")).lower()
    text = " ".join([
        _safe_str(row.get("error")),
        _safe_str(row.get("detail")),
        _safe_str(row.get("message")),
        _safe_str(row.get("recommendation_reason")),
        _safe_str(row.get("selection_reason")),
        _safe_str(row.get("notes")),
    ]).lower()
    if "placeholder" in provider or "local_dictionary_fallback" in provider:
        return True
    if "placeholder" in warnings:
        return True
    if "placeholder fallback" in text:
        return True
    if "generated locally because upstream" in text:
        return True
    if "auto-generated fallback row" in text:
        return True
    return False


def _payload_is_degraded(payload: Optional[Dict[str, Any]], *, sheet: str) -> bool:
    if not isinstance(payload, dict):
        return True
    status_text = _safe_str(payload.get("status")).lower()
    error_text = _payload_error_text(payload)
    rows = _extract_payload_rows(payload, [], [])
    row_count = len(rows)
    markers = ("placeholder", "fail_soft", "upstream degradation", "no usable rows", "local non-empty fallback", "local_dictionary_fallback")
    if any(marker in error_text for marker in markers):
        return True
    if row_count == 0:
        return True
    placeholder_rows = sum(1 for row in rows if _row_looks_placeholder(row))
    if placeholder_rows >= max(1, row_count):
        return True
    if status_text in {"error", "failed", "failure"}:
        return True
    if status_text == "partial" and placeholder_rows > 0:
        return True
    if _is_special_sheet(sheet) and status_text == "partial" and row_count == 0:
        return True
    return False

# =============================================================================
# Engine method dispatch / batch helpers
# =============================================================================
async def _call_engine_method(target: Any, method_name: str, *args: Any, **kwargs: Any) -> Any:
    method = getattr(target, method_name, None)
    if method is None:
        raise AttributeError(f"Missing method {method_name}")
    value = method(*args, **kwargs)
    if inspect.isawaitable(value):
        value = await value
    return value


def _align_batch_results(results: Any, requested_symbols: List[str]) -> List[Any]:
    payload = _unwrap_payload(results)
    if isinstance(payload, dict):
        lookup: Dict[str, Any] = {}
        for key, value in payload.items():
            norm_key = normalize_symbol(_safe_str(key))
            if norm_key:
                lookup[norm_key] = value
            payload_symbol = _coerce_symbol_from_payload(value)
            if payload_symbol:
                lookup[payload_symbol] = value
        return [lookup.get(normalize_symbol(sym)) for sym in requested_symbols]
    arr = _as_list(payload)
    if not arr:
        return [None] * len(requested_symbols)
    lookup: Dict[str, Any] = {}
    for item in arr:
        payload_symbol = _coerce_symbol_from_payload(item)
        if payload_symbol:
            lookup[payload_symbol] = item
    if lookup:
        return [lookup.get(normalize_symbol(sym)) for sym in requested_symbols]
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
    mode, info, _ = _V2_DISCOVERY.discover()
    if mode in (EngineMode.V2, EngineMode.LEGACY) and info and info.module is not None:
        fn_name = "get_enriched_quote" if enriched else "get_quote"
        fn = getattr(info.module, fn_name, None)
        if callable(fn):
            try:
                value = fn(norm)
                if inspect.isawaitable(value):
                    value = await value
                value = _unwrap_payload(value)
                value = await EmergencyDataRescuer.rescue_quote(value, norm)
                if use_cache and not _is_missing(value):
                    await _ENGINE_MANAGER.get_cache().set("quotes", cache_key, value, ttl)
                return value
            except Exception as e:
                _dbg(f"Module func {fn_name} failed: {e}", "debug")
    for method_name in _candidate_method_names(enriched=enriched, batch=False):
        try:
            value = await _call_engine_method(engine, method_name, norm)
            value = _unwrap_payload(value)
            value = await EmergencyDataRescuer.rescue_quote(value, norm)
            if use_cache and not _is_missing(value):
                await _ENGINE_MANAGER.get_cache().set("quotes", cache_key, value, ttl)
            return value
        except (AttributeError, NotImplementedError):
            continue
        except Exception as e:
            _dbg(f"Engine method {method_name} failed: {e}", "debug")
            continue
    if not enriched:
        return await _call_engine_single(engine, symbol, enriched=True, use_cache=use_cache, ttl=ttl)
    raise AttributeError("No suitable engine method found")


async def _call_engine_batch(engine: Any, symbols: List[str], *, enriched: bool, use_cache: bool, ttl: Optional[int]) -> List[Any]:
    norm_symbols = [normalize_symbol(s) for s in symbols]
    cached_by_idx: Dict[int, Any] = {}
    miss_idx: List[int] = []
    if use_cache:
        cache = _ENGINE_MANAGER.get_cache()
        for i, sym in enumerate(norm_symbols):
            cached = await cache.get("quotes", f"{sym}:{'enriched' if enriched else 'basic'}")
            if cached is not None:
                cached_by_idx[i] = cached
            else:
                miss_idx.append(i)
        if not miss_idx:
            return [cached_by_idx[i] for i in range(len(symbols))]
        miss_syms = [norm_symbols[i] for i in miss_idx]
    else:
        miss_syms = norm_symbols
        miss_idx = list(range(len(symbols)))
    mode, info, _ = _V2_DISCOVERY.discover()
    if mode in (EngineMode.V2, EngineMode.LEGACY) and info and info.module is not None:
        fn_name = "get_enriched_quotes" if enriched else "get_quotes"
        fn = getattr(info.module, fn_name, None)
        if callable(fn):
            try:
                value = fn(miss_syms)
                if inspect.isawaitable(value):
                    value = await value
                aligned = _align_batch_results(value, miss_syms)
                for i in range(len(aligned)):
                    aligned[i] = await EmergencyDataRescuer.rescue_quote(_unwrap_payload(aligned[i]), miss_syms[i])
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
                _dbg(f"Module batch func {fn_name} failed: {e}", "debug")
    for method_name in _candidate_method_names(enriched=enriched, batch=True):
        try:
            value = await _call_engine_method(engine, method_name, miss_syms)
            aligned = _align_batch_results(value, miss_syms)
            for i in range(len(aligned)):
                aligned[i] = await EmergencyDataRescuer.rescue_quote(_unwrap_payload(aligned[i]), miss_syms[i])
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
        except (AttributeError, NotImplementedError):
            continue
        except Exception as e:
            _dbg(f"Engine batch method {method_name} failed: {e}", "debug")
            continue
    aligned_seq: List[Any] = []
    for sym in miss_syms:
        try:
            aligned_seq.append(await _call_engine_single(engine, sym, enriched=enriched, use_cache=False, ttl=None))
        except Exception:
            aligned_seq.append(None)
    out: List[Any] = [None] * len(symbols)
    cursor = 0
    for i in range(len(symbols)):
        if i in cached_by_idx:
            out[i] = cached_by_idx[i]
        else:
            out[i] = aligned_seq[cursor]
            cursor += 1
    return out

# =============================================================================
# Public quote API
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
        result = await _call_engine_single(engine, sym_in, enriched=True, use_cache=use_cache, ttl=ttl)
        result = _finalize_quote(_unwrap_payload(result))
        _ENGINE_MANAGER.record_request(True)
        _METRICS.inc("requests_total", 1, {"operation": "get_enriched_quote", "status": "success"})
        _METRICS.observe("request_duration_seconds", time.perf_counter() - t0, {"operation": "get_enriched_quote"})
        _METRICS.set("active_requests", 0)
        return result
    except Exception as e:
        _ENGINE_MANAGER.record_request(False)
        _METRICS.inc("requests_total", 1, {"operation": "get_enriched_quote", "status": "error"})
        _METRICS.observe("request_duration_seconds", time.perf_counter() - t0, {"operation": "get_enriched_quote"})
        _METRICS.set("active_requests", 0)
        if _STRICT_MODE:
            raise
        return UQ(symbol=normalize_symbol(sym_in) or sym_in, error=str(e), data_quality="MISSING", warnings=["Error retrieving quote"], request_id=str(uuid.uuid4())).finalize()


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
        results = await _call_engine_batch(engine, normed, enriched=True, use_cache=use_cache, ttl=ttl)
        aligned = _align_batch_results(results, normed)
        out: List[Any] = []
        for orig, norm, item in zip(clean, normed, aligned):
            if item is None:
                out.append(UQ(symbol=norm, original_symbol=orig, error="Missing in batch result", data_quality="MISSING", request_id=str(uuid.uuid4())).finalize())
            else:
                final = _finalize_quote(_unwrap_payload(item))
                try:
                    if hasattr(final, "original_symbol"):
                        final.original_symbol = orig
                except Exception:
                    pass
                out.append(final)
        _ENGINE_MANAGER.record_request(True)
        _METRICS.inc("requests_total", 1, {"operation": "get_enriched_quotes", "status": "success"})
        _METRICS.set("active_requests", 0)
        return out
    except Exception as e:
        _ENGINE_MANAGER.record_request(False)
        _METRICS.inc("requests_total", 1, {"operation": "get_enriched_quotes", "status": "error"})
        _METRICS.set("active_requests", 0)
        if _STRICT_MODE:
            raise
        return [UQ(symbol=n, original_symbol=o, error=str(e), data_quality="MISSING", request_id=str(uuid.uuid4())).finalize() for o, n in zip(clean, normed)]


async def get_quote(symbol: str, use_cache: bool = True, ttl: Optional[int] = None) -> Any:
    return await get_enriched_quote(symbol, use_cache=use_cache, ttl=ttl)


async def get_quotes(symbols: List[str], use_cache: bool = True, ttl: Optional[int] = None) -> List[Any]:
    return await get_enriched_quotes(symbols, use_cache=use_cache, ttl=ttl)

# =============================================================================
# Sheet rows adapter
# =============================================================================
def _sheet_meta_source(builder: Optional[str], payload: Dict[str, Any]) -> str:
    meta = payload.get("meta") if isinstance(payload.get("meta"), dict) else {}
    return _safe_str(meta.get("source") or builder or "unknown")


def _payload_envelope(*, sheet: str, headers: Sequence[str], keys: Sequence[str], row_objects: Sequence[Mapping[str, Any]], include_matrix: bool, status_out: str, error_out: Optional[str], used: str, meta_extra: Optional[Dict[str, Any]] = None, version: Optional[str] = None) -> Dict[str, Any]:
    hdrs = list(headers or [])
    ks = list(keys or [])
    rows_dict = [{str(k): _jsonable_snapshot(dict(r).get(str(k))) for k in ks} for r in row_objects or []]
    matrix = _rows_to_matrix(rows_dict, ks) if include_matrix else []
    meta = {"builder": used, **(meta_extra or {})}
    return {
        "status": status_out,
        "sheet": sheet,
        "page": sheet,
        "headers": hdrs,
        "display_headers": hdrs,
        "sheet_headers": hdrs,
        "column_headers": hdrs,
        "keys": ks,
        "columns": ks,
        "fields": ks,
        "rows": matrix,
        "rows_matrix": matrix,
        "matrix": matrix,
        "row_objects": rows_dict,
        "data": rows_dict,
        "items": rows_dict,
        "records": rows_dict,
        "quotes": rows_dict,
        "count": len(rows_dict),
        "detail": error_out or "",
        "error": error_out,
        "meta": meta,
        "version": version or __version__,
    }


async def _invoke_callable_candidates(target: Any, method_names: Sequence[str], *, sheet: str, limit: int, offset: int, mode: str, body: Dict[str, Any]) -> Tuple[Optional[Dict[str, Any]], Optional[str]]:
    for method_name in method_names:
        fn = getattr(target, method_name, None)
        if not callable(fn):
            continue
        attempts = [
            lambda: fn(sheet=sheet, limit=limit, offset=offset, mode=mode, body=body),
            lambda: fn(page=sheet, limit=limit, offset=offset, mode=mode, body=body),
            lambda: fn(sheet, limit=limit, offset=offset, mode=mode, body=body),
            lambda: fn(sheet=sheet, limit=limit, offset=offset, mode=mode, **body),
            lambda: fn(page=sheet, limit=limit, offset=offset, mode=mode, **body),
            lambda: fn(sheet=sheet, limit=limit, offset=offset, **body),
            lambda: fn(page=sheet, limit=limit, offset=offset, **body),
            lambda: fn(sheet, limit=limit, offset=offset, **body),
            lambda: fn(sheet),
            lambda: fn(page=sheet),
            lambda: fn(body),
            lambda: fn(),
        ]
        for attempt in attempts:
            try:
                value = attempt()
                if inspect.isawaitable(value):
                    value = await value
                value = _unwrap_payload(value)
                if not isinstance(value, (dict, list)):
                    value = _jsonable_snapshot(value)
                if isinstance(value, dict):
                    return value, method_name
                if isinstance(value, list):
                    return {"status": "success", "sheet": sheet, "page": sheet, "rows": value}, method_name
            except TypeError:
                continue
            except Exception:
                continue
    return None, None


async def _call_special_sheet_rows_builder(engine: Any, *, sheet: str, limit: int, offset: int, mode: str, body: Dict[str, Any]) -> Tuple[Optional[Dict[str, Any]], Optional[str]]:
    method_names = _SPECIAL_SHEET_METHODS.get(sheet, [])
    if not method_names:
        return None, None
    payload, used = await _invoke_callable_candidates(engine, method_names, sheet=sheet, limit=limit, offset=offset, mode=mode, body=body)
    if payload is not None:
        return payload, f"engine.{used}"
    mode2, info, _ = _V2_DISCOVERY.discover()
    if mode2 in (EngineMode.V2, EngineMode.LEGACY) and info and info.module is not None:
        payload, used = await _invoke_callable_candidates(info.module, method_names, sheet=sheet, limit=limit, offset=offset, mode=mode, body=body)
        if payload is not None:
            return payload, f"module.{used}"
    return None, None


def _normalize_sheet_payload(payload: Optional[Dict[str, Any]], *, sheet: str, limit: int, offset: int, mode: str, body: Dict[str, Any], used: Optional[str]) -> Dict[str, Any]:
    payload = payload or {}
    sheet_name = _canonicalize_sheet_name(sheet)
    headers_schema, keys_schema, _spec, schema_source = _resolve_contract(sheet_name)
    payload_headers = payload.get("headers") or payload.get("display_headers") or payload.get("sheet_headers") or payload.get("column_headers") or []
    payload_keys = payload.get("keys") or payload.get("columns") or payload.get("fields") or payload.get("field_names") or []
    out_headers = [str(x) for x in payload_headers] if isinstance(payload_headers, list) else []
    out_keys = [str(x) for x in payload_keys] if isinstance(payload_keys, list) else []
    if headers_schema and keys_schema:
        out_headers = headers_schema[:]
        out_keys = keys_schema[:]
    elif out_headers and not out_keys:
        out_keys = out_headers[:]
    elif out_keys and not out_headers:
        out_headers = out_keys[:]
    raw_rows = _extract_payload_rows(payload, out_keys, out_headers)
    if not raw_rows and out_keys:
        raw_rows = _rows_from_matrix(payload.get("rows_matrix") or payload.get("matrix"), out_keys)
    rows_norm: List[Dict[str, Any]] = []
    if out_keys:
        for row in raw_rows:
            projected = _normalize_to_schema_keys(out_keys, out_headers, dict(row)) if out_headers and out_keys else dict(row)
            rows_norm.append(_project_row(out_keys, projected))
    else:
        rows_norm = [dict(r) for r in raw_rows]
    headers_only = _boolish(body.get("headers_only"), False)
    schema_only = _boolish(body.get("schema_only"), False)
    include_matrix = _boolish(body.get("include_matrix", True), True)
    if schema_only:
        rows_norm = []
    elif rows_norm:
        rows_norm = _slice(rows_norm, limit=limit, offset=offset)
    rows_matrix = _rows_to_matrix(rows_norm, out_keys) if (include_matrix and out_keys and not (headers_only or schema_only)) else ([] if include_matrix and out_keys and (headers_only or schema_only) else None)
    status_out = payload.get("status") or ("partial" if not rows_norm and out_keys else "success")
    if _SCHEMA_STRICT_SHEET_ROWS and _SCHEMA_AVAILABLE and (not out_headers or not out_keys):
        status_out = "error"
    meta_in = payload.get("meta") if isinstance(payload.get("meta"), dict) else {}
    count = 0 if headers_only else len(rows_norm)
    return {
        "status": status_out,
        "sheet": payload.get("sheet") or sheet_name,
        "page": payload.get("page") or sheet_name,
        "headers": out_headers,
        "display_headers": out_headers,
        "sheet_headers": out_headers,
        "column_headers": out_headers,
        "keys": out_keys,
        "columns": out_keys,
        "fields": out_keys,
        "rows": [] if headers_only else rows_norm,
        "row_objects": [] if headers_only else rows_norm,
        "rows_matrix": rows_matrix,
        "matrix": rows_matrix,
        "data": [] if headers_only else rows_norm,
        "items": [] if headers_only else rows_norm,
        "records": [] if headers_only else rows_norm,
        "quotes": [] if headers_only else rows_norm,
        "count": count,
        "detail": payload.get("detail") or payload.get("message") or payload.get("error"),
        "error": payload.get("error"),
        "meta": {
            **meta_in,
            "adapter_schema_enforced": bool(headers_schema and keys_schema),
            "schema_source": schema_source,
            "special_dispatch": _is_special_sheet(sheet_name),
            "builder": used or meta_in.get("builder") or "unknown",
            "limit": limit,
            "offset": offset,
            "mode": mode,
            "headers_only": headers_only,
            "schema_only": schema_only,
            "row_count": count,
            "degraded_attempts": meta_in.get("degraded_attempts", []),
        },
        "version": payload.get("version") or __version__,
    }


async def _build_quote_fallback_rows(engine: Any, *, sheet: str, limit: int, offset: int, body: Dict[str, Any], headers: Sequence[str], keys: Sequence[str]) -> Tuple[List[Dict[str, Any]], Optional[str]]:
    symbols = _extract_requested_symbols_from_body(body, limit=max(50, limit + offset))
    if not symbols:
        symbols = [normalize_symbol(sym) for sym in _DEFAULT_SHEET_SYMBOLS.get(sheet, []) if normalize_symbol(sym)]
    if not symbols:
        return [], None
    sliced = symbols[offset: offset + limit] if (offset or len(symbols) > limit) else symbols[:limit]
    if not sliced:
        return [], None
    try:
        results = await _call_engine_batch(engine, sliced, enriched=True, use_cache=False, ttl=None)
        aligned = _align_batch_results(results, sliced)
    except Exception as e:
        _dbg(f"quote fallback batch failed for {sheet}: {e}", "debug")
        return [], None
    rows: List[Dict[str, Any]] = []
    for sym, item in zip(sliced, aligned):
        if item is None:
            continue
        final = _finalize_quote(_unwrap_payload(item))
        row = _model_to_dict(final)
        row = _enrich_row_identity(row, sym)
        if keys:
            row = _normalize_to_schema_keys(keys, headers, row)
        rows.append(row)
    return rows, ("quote_batch_fallback" if rows else None)


def _build_dictionary_fallback_rows(*, sheet: str, headers: Sequence[str], keys: Sequence[str], limit: int, offset: int) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    for idx, (header, key) in enumerate(zip(headers, keys), start=1):
        rows.append({
            "sheet": sheet,
            "group": "Core Contract",
            "header": header,
            "key": key,
            "dtype": "number" if any(token in key for token in ("price", "score", "roi", "qty", "value", "cap", "volume", "margin")) else "text",
            "fmt": "0.00" if any(token in key for token in ("score", "roi", "price", "value")) else "",
            "required": key in {"sheet", "header", "key", "symbol", "name", "current_price"},
            "source": "core.data_engine.local_dictionary_fallback",
            "notes": f"Auto-generated fallback row {idx} from schema contract",
        })
    return _slice(rows, limit=limit, offset=offset)


def _build_insights_fallback_rows(*, requested_symbols: Sequence[str], limit: int, offset: int) -> List[Dict[str, Any]]:
    symbols = [normalize_symbol(sym) for sym in requested_symbols if normalize_symbol(sym)]
    if not symbols:
        symbols = [normalize_symbol(sym) for sym in _DEFAULT_SHEET_SYMBOLS.get(_INSIGHTS_PAGE, []) if normalize_symbol(sym)]
    stamp = _utc_now_iso()
    rows: List[Dict[str, Any]] = [
        {"section": "Coverage", "item": "Requested symbols", "symbol": "", "recommendation": "", "confidence": 100, "notes": f"Local insights fallback summary | count={len(symbols)}", "last_updated_riyadh": stamp},
        {"section": "Coverage", "item": "Universe sample", "symbol": "", "recommendation": "", "confidence": 100, "notes": ", ".join(symbols[:5]), "last_updated_riyadh": stamp},
    ]
    for idx, sym in enumerate(symbols[: max(1, limit + offset)], start=1):
        rows.append({"section": "Signals", "item": f"Fallback signal {idx}", "symbol": sym, "recommendation": "HOLD" if idx > 2 else "BUY", "confidence": round(max(30, 95 - idx * 7), 2), "notes": "Generated locally because upstream insights payload was unavailable", "last_updated_riyadh": stamp})
    return _slice(rows, limit=limit, offset=offset)


def _build_nonempty_failsoft_rows(*, sheet: str, headers: Sequence[str], keys: Sequence[str], requested_symbols: Sequence[str], limit: int, offset: int, top_n: int) -> List[Dict[str, Any]]:
    if sheet == _DICTIONARY_PAGE:
        return _build_dictionary_fallback_rows(sheet=sheet, headers=headers, keys=keys, limit=limit, offset=offset)
    if sheet == _INSIGHTS_PAGE:
        return _build_insights_fallback_rows(requested_symbols=requested_symbols, limit=limit, offset=offset)
    if sheet == _TOP10_PAGE:
        symbols = list(requested_symbols) or _DEFAULT_SHEET_SYMBOLS.get(sheet, [])
        rows = []
        for idx, sym in enumerate(symbols[: max(limit, top_n)], start=1):
            rows.append({
                "symbol": normalize_symbol(sym),
                "name": f"{sheet} {normalize_symbol(sym)}",
                "current_price": 100.0 + idx,
                "recommendation": "BUY" if idx <= 3 else "HOLD",
                "overall_score": max(1.0, 100.0 - idx * 3),
                "opportunity_score": max(1.0, 100.0 - idx * 3),
                "forecast_confidence": max(1.0, 100.0 - idx * 3),
                "confidence_score": max(1.0, 100.0 - idx * 3),
                "risk_bucket": "High Confidence" if idx <= 3 else "Moderate",
                "avg_cost": 100.0 + idx,
                "position_cost": 100.0 + idx,
                "position_value": 100.0 + idx,
                "unrealized_pl": 100.0 + idx,
                "unrealized_pl_pct": 100.0 + idx,
                "data_provider": "core.data_engine.placeholder_fallback",
                "recommendation_reason": "Placeholder fallback because live engine returned no usable rows.",
                "selection_reason": "Placeholder fallback because upstream builders returned no usable rows.",
                "criteria_snapshot": json.dumps({"symbol": normalize_symbol(sym), "row_index": idx, "source": "placeholder"}, ensure_ascii=False),
            })
        rows = _ensure_top10_rows(rows, requested_symbols=requested_symbols, top_n=top_n, schema_keys=keys, schema_headers=headers)
        return _slice(rows, limit=limit, offset=offset)
    return []


async def _call_engine_sheet_rows(engine: Any, *, sheet: str, limit: int, offset: int, mode: str, body: Dict[str, Any]) -> Dict[str, Any]:
    sheet_name = _resolve_sheet_from_inputs(sheet, body)
    limit = max(1, min(5000, int(limit or 2000)))
    offset = max(0, int(offset or 0))
    body = dict(body or {})
    mode = _safe_str(mode)
    headers, keys, _spec, schema_source = _resolve_contract(sheet_name)
    if _SCHEMA_STRICT_SHEET_ROWS and _SCHEMA_AVAILABLE and (not headers or not keys):
        return {
            "status": "error",
            "sheet": sheet_name,
            "page": sheet_name,
            "headers": [],
            "display_headers": [],
            "sheet_headers": [],
            "column_headers": [],
            "keys": [],
            "columns": [],
            "fields": [],
            "rows": [],
            "row_objects": [],
            "rows_matrix": [],
            "matrix": [],
            "data": [],
            "items": [],
            "records": [],
            "quotes": [],
            "count": 0,
            "error": f"Unknown sheet or schema missing for '{sheet_name}'",
            "meta": {"strict": True, "builder": "schema_guard", "schema_source": schema_source, "degraded_attempts": []},
            "version": __version__,
        }
    payload: Optional[Dict[str, Any]] = None
    used: Optional[str] = None
    degraded_attempts: List[Dict[str, Any]] = []
    if _is_special_sheet(sheet_name):
        candidate_payload, candidate_used = await _call_special_sheet_rows_builder(engine, sheet=sheet_name, limit=limit, offset=offset, mode=mode, body=body)
        if candidate_payload is not None:
            if _payload_is_degraded(candidate_payload, sheet=sheet_name):
                degraded_attempts.append({"source": candidate_used or "special_builder", "status": _safe_str(candidate_payload.get("status")) or "unknown"})
            else:
                payload, used = candidate_payload, candidate_used
    if payload is None:
        candidate_payload, method_used = await _invoke_callable_candidates(engine, _candidate_sheet_rows_methods(), sheet=sheet_name, limit=limit, offset=offset, mode=mode, body=body)
        if candidate_payload is not None:
            candidate_used = f"engine.{method_used}" if method_used else "engine.sheet_rows"
            if _payload_is_degraded(candidate_payload, sheet=sheet_name):
                degraded_attempts.append({"source": candidate_used, "status": _safe_str(candidate_payload.get("status")) or "unknown"})
            else:
                payload, used = candidate_payload, candidate_used
    if payload is None:
        mode2, info, _ = _V2_DISCOVERY.discover()
        if mode2 in (EngineMode.V2, EngineMode.LEGACY) and info and info.module is not None:
            candidate_payload, method_used = await _invoke_callable_candidates(info.module, _candidate_sheet_rows_methods(), sheet=sheet_name, limit=limit, offset=offset, mode=mode, body=body)
            if candidate_payload is not None:
                candidate_used = f"module.{method_used}" if method_used else "module.sheet_rows"
                if _payload_is_degraded(candidate_payload, sheet=sheet_name):
                    degraded_attempts.append({"source": candidate_used, "status": _safe_str(candidate_payload.get("status")) or "unknown"})
                else:
                    payload, used = candidate_payload, candidate_used
    if payload is None and not _is_special_sheet(sheet_name):
        quote_rows, builder_name = await _build_quote_fallback_rows(engine, sheet=sheet_name, limit=limit, offset=offset, body=body, headers=headers, keys=keys)
        if quote_rows:
            payload = {
                "status": "partial",
                "sheet": sheet_name,
                "page": sheet_name,
                "headers": headers,
                "display_headers": headers,
                "sheet_headers": headers,
                "column_headers": headers,
                "keys": keys,
                "columns": keys,
                "fields": keys,
                "rows": quote_rows,
                "row_objects": quote_rows,
                "data": quote_rows,
                "items": quote_rows,
                "records": quote_rows,
                "quotes": quote_rows,
                "meta": {"path": "quote_batch_fallback", "builder": builder_name or "quote_batch_fallback", "degraded_attempts": degraded_attempts, "schema_source": schema_source},
                "version": __version__,
            }
            used = used or builder_name or "quote_batch_fallback"
    if payload is None and _is_special_sheet(sheet_name):
        requested_symbols = _extract_requested_symbols_from_body(body, limit=max(top_n := max(1, min(5000, int(body.get("top_n", limit) or limit))), limit + offset))
        fallback_rows = _build_nonempty_failsoft_rows(sheet=sheet_name, headers=headers, keys=keys, requested_symbols=requested_symbols, limit=limit, offset=offset, top_n=top_n)
        payload = {
            "status": "partial",
            "sheet": sheet_name,
            "page": sheet_name,
            "headers": headers,
            "display_headers": headers,
            "sheet_headers": headers,
            "column_headers": headers,
            "keys": keys,
            "columns": keys,
            "fields": keys,
            "rows": fallback_rows,
            "row_objects": fallback_rows,
            "data": fallback_rows,
            "items": fallback_rows,
            "records": fallback_rows,
            "quotes": fallback_rows,
            "meta": {"path": "special_failsoft_fallback", "builder": "special_fallback", "degraded_attempts": degraded_attempts, "schema_source": schema_source},
            "error": "Local non-empty fallback emitted after upstream degradation",
            "detail": "Local non-empty fallback emitted after upstream degradation",
            "version": __version__,
        }
        used = used or "special_fallback"
    if payload is None:
        payload = {
            "status": "partial",
            "sheet": sheet_name,
            "page": sheet_name,
            "headers": headers,
            "display_headers": headers,
            "sheet_headers": headers,
            "column_headers": headers,
            "keys": keys,
            "columns": keys,
            "fields": keys,
            "rows": [],
            "row_objects": [],
            "rows_matrix": [] if (headers and keys and _boolish(body.get("include_matrix", True), True)) else None,
            "matrix": [] if (headers and keys and _boolish(body.get("include_matrix", True), True)) else None,
            "data": [],
            "items": [],
            "records": [],
            "quotes": [],
            "count": 0,
            "meta": {"path": "schema_only_fallback", "builder": "none", "degraded_attempts": degraded_attempts, "schema_source": schema_source},
            "version": __version__,
        }
    return _normalize_sheet_payload(payload, sheet=sheet_name, limit=limit, offset=offset, mode=mode, body=body, used=used)


@otel_traced("get_sheet_rows")
@monitor_perf("get_sheet_rows")
async def get_sheet_rows(sheet: str, limit: int = 2000, offset: int = 0, mode: str = "", body: Optional[Dict[str, Any]] = None, use_cache: bool = False, ttl: Optional[int] = None) -> Dict[str, Any]:
    body = dict(body or {})
    sheet_name = _resolve_sheet_from_inputs(sheet, body)
    cache_key = f"{sheet_name}:{limit}:{offset}:{mode}:{_stable_body_for_cache(body)}"
    if use_cache:
        cached = await _ENGINE_MANAGER.get_cache().get("sheet_rows", cache_key)
        if cached is not None and isinstance(cached, dict):
            return cached
    engine = await get_engine()
    payload = await _call_engine_sheet_rows(engine, sheet=sheet_name, limit=limit, offset=offset, mode=mode, body=body)
    if use_cache and payload.get("status") in {"success", "partial"}:
        await _ENGINE_MANAGER.get_cache().set("sheet_rows", cache_key, payload, ttl)
    return payload


def get_sheet_rows_sync(sheet: str, limit: int = 2000, offset: int = 0, mode: str = "", body: Optional[Dict[str, Any]] = None, use_cache: bool = False, ttl: Optional[int] = None) -> Dict[str, Any]:
    return _run_coro_sync(get_sheet_rows(sheet, limit=limit, offset=offset, mode=mode, body=body, use_cache=use_cache, ttl=ttl))

# =============================================================================
# Batch processing / health / wrapper
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


async def process_batch(symbols: List[str], batch_size: int = 10, delay_seconds: float = 0.1, enriched: bool = True, use_cache: bool = True, ttl: Optional[int] = None, max_retries: int = 3, progress_callback: Optional[Callable[[BatchProgress], None]] = None, metadata: Optional[Dict[str, Any]] = None) -> List[Any]:
    clean = _SYMBOL_NORMALIZER.clean_symbols(symbols)
    if not clean:
        return []
    progress = BatchProgress(total=len(clean), metadata=metadata or {})
    results: List[Optional[Any]] = [None] * len(clean)
    retry_counts: Dict[int, int] = defaultdict(int)
    func = get_enriched_quotes if enriched else get_quotes
    for i in range(0, len(clean), batch_size):
        batch = clean[i:i + batch_size]
        idxs = list(range(i, min(i + batch_size, len(clean))))
        try:
            batch_results = await func(batch, use_cache=use_cache, ttl=ttl)
            for idx, result in zip(idxs, batch_results):
                results[idx] = result
                if result is None or getattr(result, "error", None) or (isinstance(result, dict) and result.get("error")):
                    if retry_counts[idx] < max_retries:
                        retry_counts[idx] += 1
                        continue
                    progress.failed += 1
                    progress.errors.append((clean[idx], str(getattr(result, "error", None) or (result.get("error") if isinstance(result, dict) else "error"))))
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
    UQ = get_unified_quote_class()
    out: List[Any] = []
    for sym, result in zip(clean, results):
        if result is None:
            out.append(UQ(symbol=normalize_symbol(sym) or sym, error="Missing", data_quality="MISSING", request_id=str(uuid.uuid4())).finalize())
        else:
            out.append(_finalize_quote(_unwrap_payload(result)))
    return out


async def health_check() -> Dict[str, Any]:
    health: Dict[str, Any] = {"status": "healthy", "version": __version__, "timestamp": _utc_now_iso(), "checks": {}, "warnings": [], "errors": []}
    try:
        engine = await get_engine()
        if isinstance(engine, StubEngine):
            health["warnings"].append("Using stub engine (no real data)")
            health["status"] = "degraded"
        health["checks"]["cache"] = _ENGINE_MANAGER.get_cache().get_stats()
        health["checks"]["circuit_breaker"] = _ENGINE_MANAGER._circuit.get_stats()
        health["checks"]["rate_limiter"] = _ENGINE_MANAGER._rate.get_stats()
        health["checks"]["schema_available"] = bool(_SCHEMA_AVAILABLE)
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
        try:
            sheet_rows = await get_sheet_rows(_DICTIONARY_PAGE, limit=1, offset=0, mode="", body={"include_matrix": False}, use_cache=False)
            health["checks"]["sheet_rows_test"] = "passed" if isinstance(sheet_rows, dict) else "failed"
        except Exception as e:
            health["checks"]["sheet_rows_test"] = "failed"
            health["warnings"].append(f"sheet_rows_test error: {e}")
    except Exception as e:
        health["errors"].append(f"Health check failed: {e}")
        health["status"] = "unhealthy"
    return health


@asynccontextmanager
async def engine_context() -> AsyncGenerator[Any, None]:
    engine = await get_engine()
    try:
        yield engine
    finally:
        await close_engine()


class EngineSession:
    def __enter__(self) -> Any:
        self._engine = get_engine_sync()
        return self._engine
    def __exit__(self, *args: Any) -> None:
        _run_coro_sync(close_engine())


class DataEngine:
    def __init__(self, *args: Any, **kwargs: Any) -> None:
        self._engine: Optional[Any] = None
        self._request_id = str(uuid.uuid4())

    async def _ensure(self) -> Any:
        if self._engine is None:
            self._engine = await get_engine()
        return self._engine

    async def get_quote(self, symbol: str, use_cache: bool = True) -> Any:
        try:
            engine = await self._ensure()
            value = await _call_engine_single(engine, symbol, enriched=False, use_cache=use_cache, ttl=None)
            value = _finalize_quote(_unwrap_payload(value))
            return await EmergencyDataRescuer.rescue_quote(value, symbol)
        except Exception:
            return await get_quote(symbol, use_cache=use_cache)

    async def get_quotes(self, symbols: List[str], use_cache: bool = True) -> List[Any]:
        try:
            engine = await self._ensure()
            value = await _call_engine_batch(engine, symbols, enriched=False, use_cache=use_cache, ttl=None)
            aligned = _align_batch_results(value, [normalize_symbol(s) for s in symbols])
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

    async def get_sheet_rows(self, sheet: str, limit: int = 2000, offset: int = 0, mode: str = "", body: Optional[Dict[str, Any]] = None, use_cache: bool = False, ttl: Optional[int] = None) -> Dict[str, Any]:
        return await get_sheet_rows(sheet, limit=limit, offset=offset, mode=mode, body=body, use_cache=use_cache, ttl=ttl)

    async def aclose(self) -> None:
        await close_engine()

# =============================================================================
# Meta / compatibility
# =============================================================================
def _parse_env_list(key: str) -> List[str]:
    raw = (os.getenv(key) or "").strip()
    if not raw:
        return []
    return [part.strip().lower() for part in raw.split(",") if part.strip()]


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
        "schema_available": bool(_SCHEMA_AVAILABLE),
        "schema_strict_sheet_rows": bool(_SCHEMA_STRICT_SHEET_ROWS),
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
    "get_sheet_rows",
    "get_sheet_rows_sync",
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

try:
    _METRICS.set("active_requests", 0)
except Exception:
    pass
