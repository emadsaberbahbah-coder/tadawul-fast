#!/usr/bin/env python3
"""
core/data_engine.py
================================================================================
Enterprise Data Engine — v6.2.0 (ADVANCED ENTERPRISE)
================================================================================
Financial Data Platform — Core Data Engine with Multi-Version Support

What's new in v6.2.0:
- ✅ High-performance JSON parsing via `orjson` (if available)
- ✅ Memory-optimized state models using `@dataclass(slots=True)`
- ✅ OpenTelemetry Tracing integration for sync and async execution
- ✅ Full Jitter Exponential Backoff for Distributed Cache cluster connections
- ✅ Dynamic Circuit Breaker with progressive timeout scaling (up to 300s)
- ✅ Advanced singleflight execution to prevent cache stampedes
- ✅ Zlib compression automatically applied to Redis and Memcached payloads
- ✅ Fully aligned with the v5.x provider ecosystem and core configuration

Core Capabilities:
- Multi-version engine support (v1, v2, v3) with automatic detection
- Advanced lazy loading with circuit breaker pattern
- Thread-safe singleton management with double-checked locking
- Smart caching with TTL, LRU eviction, and payload compression
- Comprehensive error handling with retry strategies
- Full async/await support with timeout controls
- Type-safe responses with Pydantic v2 compatibility
- Detailed telemetry and distributed tracing
- Never crashes startup: all functions are defensive
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
import uuid
import zlib
import pickle
from collections import defaultdict, deque
from contextlib import asynccontextmanager, contextmanager
from dataclasses import dataclass, field, asdict
from datetime import datetime, timedelta, timezone
from enum import Enum
from importlib import import_module
from typing import (
    Any, AsyncGenerator, AsyncIterator, Awaitable, Callable, Dict, Iterable, List,
    Optional, Sequence, Set, Tuple, Type, TypeVar, Union, cast
)

# ============================================================================
# Version Information
# ============================================================================

__version__ = "6.2.0"
ADAPTER_VERSION = __version__

# ============================================================================
# High-Performance JSON fallback
# ============================================================================
try:
    import orjson
    def json_loads(data: Union[str, bytes]) -> Any:
        return orjson.loads(data)
    def json_dumps(obj: Any) -> str:
        return orjson.dumps(obj).decode('utf-8')
except ImportError:
    import json
    def json_loads(data: Union[str, bytes]) -> Any:
        return json.loads(data)
    def json_dumps(obj: Any) -> str:
        return json.dumps(obj, default=str)


# ============================================================================
# Optional Dependencies with Graceful Fallback
# ============================================================================

# Pydantic
try:
    from pydantic import BaseModel, Field, ConfigDict
    from pydantic import ValidationError as PydanticValidationError
    try:
        _PYDANTIC_V2 = True
    except Exception:
        _PYDANTIC_V2 = False
except ImportError:
    _PYDANTIC_V2 = False
    BaseModel = object
    Field = lambda default=None, **kwargs: default
    ConfigDict = None
    PydanticValidationError = Exception

# Redis
try:
    import aioredis
    from aioredis import Redis
    _REDIS_AVAILABLE = True
except ImportError:
    aioredis = None
    Redis = None
    _REDIS_AVAILABLE = False

# Memcached
try:
    import aiomcache
    _MEMCACHED_AVAILABLE = True
except ImportError:
    aiomcache = None
    _MEMCACHED_AVAILABLE = False

# Prometheus
try:
    from prometheus_client import Counter, Histogram, Gauge, Summary
    from prometheus_client import generate_latest, REGISTRY
    _PROMETHEUS_AVAILABLE = True
except ImportError:
    _PROMETHEUS_AVAILABLE = False

# OpenTelemetry
try:
    from opentelemetry import trace
    from opentelemetry.trace import Span, Tracer, Status, StatusCode
    from opentelemetry.context import attach, detach
    _OTEL_AVAILABLE = True
except ImportError:
    _OTEL_AVAILABLE = False
    trace = None

# ============================================================================
# Debug Configuration
# ============================================================================

class LogLevel(Enum):
    DEBUG = 10
    INFO = 20
    WARN = 30
    WARNING = 30
    ERROR = 40
    CRITICAL = 50


_DEBUG = os.getenv("DATA_ENGINE_DEBUG", "").strip().lower() in {"1", "true", "yes", "y", "on"}
_DEBUG_LEVEL = os.getenv("DATA_ENGINE_DEBUG_LEVEL", "info").strip().lower() or "info"
_LOG_LEVEL = LogLevel.INFO
try:
    _LOG_LEVEL = LogLevel[_DEBUG_LEVEL.upper()]
except (KeyError, AttributeError):
    _LOG_LEVEL = LogLevel.INFO

_STRICT_MODE = os.getenv("DATA_ENGINE_STRICT", "").strip().lower() in {"1", "true", "yes", "y", "on"}
_V2_DISABLED = os.getenv("DATA_ENGINE_V2_DISABLED", "").strip().lower() in {"1", "true", "yes", "y", "on"}
_PERF_MONITORING = os.getenv("DATA_ENGINE_PERF_MONITORING", "").strip().lower() in {"1", "true", "yes", "y", "on"}
_TRACING_ENABLED = os.getenv("DATA_ENGINE_TRACING", "").strip().lower() in {"1", "true", "yes", "y", "on"}

logger = logging.getLogger("core.data_engine")


def _dbg(msg: str, level: str = "info") -> None:
    """Debug logging with level filtering."""
    if not _DEBUG:
        return

    try:
        msg_level = LogLevel[level.upper()]
        if msg_level.value >= _LOG_LEVEL.value:
            timestamp = time.strftime("%H:%M:%S.%f")[:-3]
            print(f"[{timestamp}] [data_engine:{level.upper()}] {msg}", file=sys.stderr)
    except Exception:
        pass


# ============================================================================
# Tracing Integration
# ============================================================================

class TraceContext:
    """OpenTelemetry trace context manager (Sync and Async compatible)."""
    
    def __init__(self, name: str, attributes: Optional[Dict[str, Any]] = None):
        self.name = name
        self.attributes = attributes or {}
        self.tracer = trace.get_tracer(__name__) if _OTEL_AVAILABLE and _TRACING_ENABLED else None
        self.span = None
    
    def __enter__(self):
        if self.tracer:
            self.span = self.tracer.start_span(self.name)
            if self.attributes:
                self.span.set_attributes(self.attributes)
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.span and _OTEL_AVAILABLE:
            if exc_val:
                self.span.record_exception(exc_val)
                self.span.set_status(Status(StatusCode.ERROR, str(exc_val)))
            self.span.end()

    async def __aenter__(self):
        return self.__enter__()
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        return self.__exit__(exc_type, exc_val, exc_tb)


def trace(name: Optional[str] = None):
    """Decorator to trace async functions."""
    def decorator(func: Callable[..., Awaitable[Any]]) -> Callable[..., Awaitable[Any]]:
        @functools.wraps(func)
        async def wrapper(*args: Any, **kwargs: Any) -> Any:
            trace_name = name or func.__name__
            async with TraceContext(trace_name, {"function": func.__name__}):
                return await func(*args, **kwargs)
        return wrapper
    return decorator


# ============================================================================
# Performance Monitoring
# ============================================================================

@dataclass(slots=True)
class PerfMetrics:
    """Performance metrics for operations."""
    operation: str
    start_time: float
    end_time: float
    duration_ms: float
    success: bool
    error: Optional[str] = None
    metadata: Dict[str, Any] = field(default_factory=dict)


_PERF_METRICS: List[PerfMetrics] = []
_PERF_LOCK = threading.RLock()


def record_perf_metric(metrics: PerfMetrics) -> None:
    """Record performance metric."""
    if not _PERF_MONITORING:
        return
    with _PERF_LOCK:
        _PERF_METRICS.append(metrics)
        if len(_PERF_METRICS) > 10000:
            _PERF_METRICS[:] = _PERF_METRICS[-10000:]


def get_perf_metrics(
    operation: Optional[str] = None,
    limit: int = 1000
) -> List[Dict[str, Any]]:
    """Get recorded performance metrics."""
    with _PERF_LOCK:
        metrics = _PERF_METRICS[-limit:] if limit else _PERF_METRICS
        if operation:
            metrics = [m for m in metrics if m.operation == operation]
        return [
            {
                "operation": m.operation,
                "duration_ms": m.duration_ms,
                "success": m.success,
                "error": m.error,
                "metadata": m.metadata,
                "timestamp": datetime.fromtimestamp(m.start_time, timezone.utc).isoformat(),
            }
            for m in metrics
        ]


def get_perf_stats(operation: Optional[str] = None) -> Dict[str, Any]:
    """Get performance statistics."""
    metrics = get_perf_metrics(operation, limit=None)
    if not metrics:
        return {}
    
    durations = [m["duration_ms"] for m in metrics if m["success"]]
    if not durations:
        return {"error": "No successful operations"}
    
    return {
        "operation": operation or "all",
        "count": len(metrics),
        "success_count": sum(1 for m in metrics if m["success"]),
        "failure_count": sum(1 for m in metrics if not m["success"]),
        "success_rate": sum(1 for m in metrics if m["success"]) / len(metrics) * 100,
        "min_duration_ms": min(durations),
        "max_duration_ms": max(durations),
        "avg_duration_ms": sum(durations) / len(durations),
        "p50_duration_ms": sorted(durations)[len(durations) // 2],
        "p95_duration_ms": sorted(durations)[int(len(durations) * 0.95)],
        "p99_duration_ms": sorted(durations)[int(len(durations) * 0.99)],
    }


def reset_perf_metrics() -> None:
    """Reset performance metrics."""
    with _PERF_LOCK:
        _PERF_METRICS.clear()


def monitor_perf(operation: str, metadata: Optional[Dict[str, Any]] = None):
    """Decorator to monitor performance of async functions."""
    def decorator(func: Callable[..., Awaitable[Any]]) -> Callable[..., Awaitable[Any]]:
        @functools.wraps(func)
        async def wrapper(*args: Any, **kwargs: Any) -> Any:
            start = time.perf_counter()
            meta = metadata or {}
            if args and hasattr(args[0], "__class__"):
                meta["class"] = args[0].__class__.__name__
            
            try:
                result = await func(*args, **kwargs)
                end = time.perf_counter()
                record_perf_metric(PerfMetrics(
                    operation=operation,
                    start_time=start,
                    end_time=end,
                    duration_ms=(end - start) * 1000,
                    success=True,
                    metadata=meta,
                ))
                return result
            except Exception as e:
                end = time.perf_counter()
                record_perf_metric(PerfMetrics(
                    operation=operation,
                    start_time=start,
                    end_time=end,
                    duration_ms=(end - start) * 1000,
                    success=False,
                    error=str(e),
                    metadata=meta,
                ))
                raise
        return wrapper
    return decorator


# ============================================================================
# Prometheus Metrics
# ============================================================================

class MetricsRegistry:
    """Prometheus metrics registry."""
    
    def __init__(self, namespace: str = "data_engine"):
        self.namespace = namespace
        self._metrics: Dict[str, Any] = {}
        self._lock = threading.RLock()
        
        if _PROMETHEUS_AVAILABLE:
            self._init_metrics()
    
    def _init_metrics(self):
        """Initialize Prometheus metrics."""
        with self._lock:
            self._metrics["requests_total"] = Counter(
                f"{self.namespace}_requests_total",
                "Total number of requests",
                ["operation", "status"]
            )
            self._metrics["request_duration_seconds"] = Histogram(
                f"{self.namespace}_request_duration_seconds",
                "Request duration in seconds",
                ["operation"],
                buckets=[0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0]
            )
            self._metrics["cache_hits_total"] = Counter(
                f"{self.namespace}_cache_hits_total",
                "Total cache hits",
                ["cache_type"]
            )
            self._metrics["cache_misses_total"] = Counter(
                f"{self.namespace}_cache_misses_total",
                "Total cache misses",
                ["cache_type"]
            )
            self._metrics["circuit_breaker_state"] = Gauge(
                f"{self.namespace}_circuit_breaker_state",
                "Circuit breaker state (1=closed, 0=open, -1=half-open)",
                ["name"]
            )
            self._metrics["provider_health"] = Gauge(
                f"{self.namespace}_provider_health",
                "Provider health (1=healthy, 0=degraded, -1=unhealthy)",
                ["provider"]
            )
            self._metrics["active_requests"] = Gauge(
                f"{self.namespace}_active_requests",
                "Number of active requests"
            )
    
    def inc(self, name: str, value: float = 1.0, labels: Optional[Dict[str, str]] = None):
        """Increment a counter metric."""
        if not _PROMETHEUS_AVAILABLE:
            return
        with self._lock:
            metric = self._metrics.get(name)
            if metric:
                if labels:
                    metric.labels(**labels).inc(value)
                else:
                    metric.inc(value)
    
    def observe(self, name: str, value: float, labels: Optional[Dict[str, str]] = None):
        """Observe a histogram metric."""
        if not _PROMETHEUS_AVAILABLE:
            return
        with self._lock:
            metric = self._metrics.get(name)
            if metric:
                if labels:
                    metric.labels(**labels).observe(value)
                else:
                    metric.observe(value)
    
    def set(self, name: str, value: float, labels: Optional[Dict[str, str]] = None):
        """Set a gauge metric."""
        if not _PROMETHEUS_AVAILABLE:
            return
        with self._lock:
            metric = self._metrics.get(name)
            if metric:
                if labels:
                    metric.labels(**labels).set(value)
                else:
                    metric.set(value)
    
    def get_metrics(self) -> str:
        """Get Prometheus metrics in text format."""
        if not _PROMETHEUS_AVAILABLE:
            return ""
        return generate_latest(REGISTRY).decode('utf-8')


_METRICS = MetricsRegistry()


# ============================================================================
# Rate Limiter & Circuit Breaker (Dynamic)
# ============================================================================

class CircuitState(Enum):
    """Circuit breaker state."""
    CLOSED = "closed"
    OPEN = "open"
    HALF_OPEN = "half_open"

class TokenBucket:
    """Async token bucket rate limiter."""
    
    def __init__(self, rate_per_sec: float, capacity: Optional[float] = None):
        self.rate = max(0.0, rate_per_sec)
        self.capacity = capacity or max(1.0, self.rate * 2.0)
        self.tokens = self.capacity
        self.last = time.monotonic()
        self._lock = asyncio.Lock()
    
    async def acquire(self, tokens: float = 1.0) -> bool:
        """Acquire tokens, returns True if successful."""
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
        """Wait until tokens available."""
        while True:
            if await self.acquire(tokens):
                return
            wait = (tokens - self.tokens) / self.rate if self.rate > 0 else 0.1
            await asyncio.sleep(min(1.0, max(0.01, wait)))
    
    def get_stats(self) -> Dict[str, Any]:
        """Get rate limiter statistics."""
        return {
            "rate": self.rate,
            "capacity": self.capacity,
            "current_tokens": self.tokens,
            "utilization": 1.0 - (self.tokens / self.capacity) if self.capacity > 0 else 0,
        }


class DynamicCircuitBreaker:
    """Advanced circuit breaker with progressive timeout scaling and health monitoring."""
    
    def __init__(
        self,
        name: str,
        failure_threshold: int = 5,
        success_threshold: int = 2,
        base_timeout_seconds: float = 30.0,
        max_timeout_seconds: float = 300.0
    ):
        self.name = name
        self.failure_threshold = failure_threshold
        self.success_threshold = success_threshold
        self.base_timeout_seconds = base_timeout_seconds
        self.max_timeout_seconds = max_timeout_seconds
        
        self.state = CircuitState.CLOSED
        self.failure_count = 0
        self.success_count = 0
        self.last_failure_time = 0.0
        self.last_success_time = 0.0
        self.total_failures = 0
        self.total_successes = 0
        self.current_timeout = base_timeout_seconds
        self._lock = asyncio.Lock()
        
        _METRICS.set("circuit_breaker_state", 1, {"name": name})
    
    async def execute(self, func: Callable, *args, **kwargs) -> Any:
        """Execute function with dynamic circuit breaker protection."""
        async with self._lock:
            now = time.time()
            if self.state == CircuitState.OPEN:
                if now - self.last_failure_time > self.current_timeout:
                    self.state = CircuitState.HALF_OPEN
                    self.success_count = 0
                    _METRICS.set("circuit_breaker_state", -1, {"name": self.name})
                    _dbg(f"Circuit {self.name} entering half-open state", "info")
                else:
                    raise Exception(f"Circuit {self.name} is OPEN (cooldown: {self.current_timeout}s)")
            
            # In half-open, we allow 1 request to pass through for testing (enforced by success threshold checks logic elsewhere or concurrency limits)
            pass
        
        try:
            result = await func(*args, **kwargs) if asyncio.iscoroutinefunction(func) else func(*args, **kwargs)
            
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
                        _dbg(f"Circuit {self.name} closed. Cooldown reset.", "info")
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
                        _dbg(f"Circuit {self.name} opened after {self.failure_count} failures", "warning")
                elif self.state == CircuitState.HALF_OPEN:
                    self.state = CircuitState.OPEN
                    # Progressive backoff
                    self.current_timeout = min(self.max_timeout_seconds, self.current_timeout * 1.5)
                    _METRICS.set("circuit_breaker_state", 0, {"name": self.name})
                    _dbg(f"Circuit {self.name} re-opened. Cooldown scaled to {self.current_timeout}s", "warning")
            
            raise e
    
    def get_stats(self) -> Dict[str, Any]:
        """Get circuit breaker statistics."""
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


# ============================================================================
# Distributed Cache with Compression
# ============================================================================

class CacheBackend(Enum):
    """Cache backend type."""
    MEMORY = "memory"
    REDIS = "redis"
    MEMCACHED = "memcached"
    NONE = "none"

@dataclass(slots=True)
class CacheEntry:
    """Cache entry with metadata."""
    key: str
    value: Any
    created_at: float = field(default_factory=time.time)
    expires_at: Optional[float] = None
    access_count: int = 0
    last_access: float = field(default_factory=time.time)
    
    @property
    def is_expired(self) -> bool:
        """Check if entry is expired."""
        return self.expires_at is not None and time.time() > self.expires_at
    
    @property
    def age_seconds(self) -> float:
        """Get age in seconds."""
        return time.time() - self.created_at

class DistributedCache:
    """Multi-backend distributed cache with Zlib compression and Full Jitter."""
    
    def __init__(
        self,
        backend: CacheBackend = CacheBackend.MEMORY,
        default_ttl: int = 300,
        redis_url: Optional[str] = None,
        memcached_servers: Optional[List[str]] = None,
        max_size: int = 10000,
        compression: bool = True
    ):
        self.backend = backend
        self.default_ttl = default_ttl
        self.max_size = max_size
        self.compression = compression
        self._memory_cache: Dict[str, CacheEntry] = {}
        self._redis_client: Optional[Redis] = None
        self._memcached_client = None
        self._lock = asyncio.Lock()
        self._stats = {"hits": 0, "misses": 0, "sets": 0, "deletes": 0}
        
        if backend == CacheBackend.REDIS and redis_url and _REDIS_AVAILABLE:
            asyncio.create_task(self._init_redis(redis_url))
        elif backend == CacheBackend.MEMCACHED and memcached_servers and _MEMCACHED_AVAILABLE:
            asyncio.create_task(self._init_memcached(memcached_servers))
    
    async def _init_redis(self, redis_url: str):
        """Initialize Redis client with Full Jitter backoff."""
        for attempt in range(4):
            try:
                self._redis_client = await aioredis.from_url(
                    redis_url,
                    encoding=None if self.compression else "utf-8",
                    decode_responses=not self.compression,
                    max_connections=20
                )
                _dbg(f"Redis cache initialized: {redis_url}", "info")
                return
            except Exception as e:
                base_wait = 2 ** attempt
                jitter = random.uniform(0, base_wait)
                await asyncio.sleep(min(15.0, base_wait + jitter))
                _dbg(f"Redis init failed (attempt {attempt+1}): {e}", "warning")
        self.backend = CacheBackend.MEMORY
    
    async def _init_memcached(self, servers: List[str]):
        """Initialize Memcached client."""
        try:
            self._memcached_client = aiomcache.Client(servers)
            _dbg(f"Memcached cache initialized: {servers}", "info")
        except Exception as e:
            _dbg(f"Memcached initialization failed: {e}", "error")
            self.backend = CacheBackend.MEMORY
    
    def _get_cache_key(self, namespace: str, key: str) -> str:
        return f"{namespace}:{key}"

    def _serialize(self, value: Any) -> bytes:
        data = pickle.dumps(value) if self.compression else json_dumps(value).encode()
        return zlib.compress(data) if self.compression else data

    def _deserialize(self, data: bytes) -> Any:
        try:
            decompressed = zlib.decompress(data) if self.compression else data
            return pickle.loads(decompressed) if self.compression else json_loads(decompressed)
        except Exception:
            return pickle.loads(data) if self.compression else json_loads(data)
    
    async def get(self, namespace: str, key: str) -> Optional[Any]:
        cache_key = self._get_cache_key(namespace, key)
        
        if self.backend == CacheBackend.REDIS and self._redis_client:
            try:
                value = await self._redis_client.get(cache_key)
                if value is not None:
                    parsed = self._deserialize(value)
                    self._stats["hits"] += 1
                    _METRICS.inc("cache_hits_total", 1, {"cache_type": "redis"})
                    return parsed
            except Exception as e:
                _dbg(f"Redis get failed: {e}", "debug")
        
        if self.backend == CacheBackend.MEMCACHED and self._memcached_client:
            try:
                value = await self._memcached_client.get(cache_key.encode())
                if value:
                    parsed = self._deserialize(value)
                    self._stats["hits"] += 1
                    _METRICS.inc("cache_hits_total", 1, {"cache_type": "memcached"})
                    return parsed
            except Exception as e:
                _dbg(f"Memcached get failed: {e}", "debug")
        
        async with self._lock:
            if cache_key in self._memory_cache:
                entry = self._memory_cache[cache_key]
                if not entry.is_expired:
                    entry.access_count += 1
                    entry.last_access = time.time()
                    self._stats["hits"] += 1
                    _METRICS.inc("cache_hits_total", 1, {"cache_type": "memory"})
                    return entry.value
                else:
                    del self._memory_cache[cache_key]
        
        self._stats["misses"] += 1
        _METRICS.inc("cache_misses_total", 1, {"cache_type": self.backend.value})
        return None
    
    async def set(self, namespace: str, key: str, value: Any, ttl: Optional[int] = None) -> None:
        cache_key = self._get_cache_key(namespace, key)
        ttl = ttl or self.default_ttl
        
        if self.backend == CacheBackend.REDIS and self._redis_client:
            try:
                data = self._serialize(value)
                await self._redis_client.setex(cache_key, ttl, data)
            except Exception as e:
                _dbg(f"Redis set failed: {e}", "debug")
        
        if self.backend == CacheBackend.MEMCACHED and self._memcached_client:
            try:
                data = self._serialize(value)
                await self._memcached_client.set(cache_key.encode(), data, exptime=ttl)
            except Exception as e:
                _dbg(f"Memcached set failed: {e}", "debug")
        
        async with self._lock:
            if len(self._memory_cache) >= self.max_size:
                await self._evict_lru()
            self._memory_cache[cache_key] = CacheEntry(cache_key, value, expires_at=time.time() + ttl)
        
        self._stats["sets"] += 1
    
    async def delete(self, namespace: str, key: str) -> None:
        cache_key = self._get_cache_key(namespace, key)
        if self.backend == CacheBackend.REDIS and self._redis_client:
            try: await self._redis_client.delete(cache_key)
            except Exception: pass
        
        if self.backend == CacheBackend.MEMCACHED and self._memcached_client:
            try: await self._memcached_client.delete(cache_key.encode())
            except Exception: pass
        
        async with self._lock:
            self._memory_cache.pop(cache_key, None)
        
        self._stats["deletes"] += 1
    
    async def clear(self, namespace: Optional[str] = None) -> None:
        if namespace:
            prefix = f"{namespace}:"
            async with self._lock:
                keys_to_delete = [k for k in self._memory_cache.keys() if k.startswith(prefix)]
                for k in keys_to_delete:
                    del self._memory_cache[k]
        else:
            async with self._lock:
                self._memory_cache.clear()
            if self.backend == CacheBackend.REDIS and self._redis_client:
                try: await self._redis_client.flushdb()
                except Exception: pass
    
    async def _evict_lru(self) -> None:
        if not self._memory_cache: return
        items = sorted(self._memory_cache.items(), key=lambda x: x[1].last_access)
        for key, _ in items[:self.max_size // 5]:
            del self._memory_cache[key]
    
    def get_stats(self) -> Dict[str, Any]:
        total = self._stats["hits"] + self._stats["misses"]
        return {
            "backend": self.backend.value,
            "hits": self._stats["hits"],
            "misses": self._stats["misses"],
            "sets": self._stats["sets"],
            "deletes": self._stats["deletes"],
            "hit_rate": self._stats["hits"] / total if total > 0 else 0,
            "memory_size": len(self._memory_cache),
            "memory_utilization": len(self._memory_cache) / self.max_size if self.max_size > 0 else 0,
        }


# ============================================================================
# Singleflight Pattern (Prevent Cache Stampede)
# ============================================================================

class SingleFlight:
    """Deduplicate concurrent requests for the same key."""

    def __init__(self) -> None:
        self._lock = asyncio.Lock()
        self._futures: Dict[str, asyncio.Future] = {}
        self._stats: Dict[str, int] = defaultdict(int)

    async def run(self, key: str, coro_fn):
        async with self._lock:
            fut = self._futures.get(key)
            if fut is not None:
                self._stats[key] += 1
                return await fut
            fut = asyncio.get_event_loop().create_future()
            self._futures[key] = fut

        try:
            result = await coro_fn()
            if not fut.done():
                fut.set_result(result)
            return result
        except Exception as e:
            if not fut.done():
                fut.set_exception(e)
            raise
        finally:
            async with self._lock:
                self._futures.pop(key, None)

    def get_stats(self) -> Dict[str, int]:
        return dict(self._stats)


# ============================================================================
# Legacy Type Definitions
# ============================================================================

class EngineMode(Enum):
    """Engine operating mode."""
    UNKNOWN = "unknown"
    V2 = "v2"
    V3 = "v3"
    STUB = "stub"
    LEGACY = "legacy"


class QuoteQuality(Enum):
    """Quality of quote data."""
    EXCELLENT = "excellent"
    GOOD = "good"
    FAIR = "fair"
    POOR = "poor"
    MISSING = "missing"


class QuoteSource(BaseModel):
    """Legacy provider metadata model."""
    if _PYDANTIC_V2:
        model_config = ConfigDict(extra="ignore", populate_by_name=True)

    provider: str
    latency_ms: Optional[float] = None
    timestamp_utc: Optional[datetime] = None
    raw: Optional[Dict[str, Any]] = None


# ============================================================================
# Thread-Safe Helpers
# ============================================================================

def _truthy_env(name: str, default: bool = False) -> bool:
    raw = str(os.getenv(name, "")).strip().lower()
    if not raw: return default
    return raw in {"1", "true", "yes", "y", "on", "enabled", "active"}

def _safe_str(x: Any) -> str:
    try: return str(x).strip()
    except Exception: return ""

def _safe_upper(x: Any) -> str:
    return _safe_str(x).upper()

def _safe_lower(x: Any) -> str:
    return _safe_str(x).lower()

def _safe_bool(x: Any, default: bool = False) -> bool:
    if x is None: return default
    if isinstance(x, bool): return x
    if isinstance(x, (int, float)): return bool(x)
    s = _safe_lower(x)
    if s in {"1", "true", "yes", "y", "on", "enabled", "active"}: return True
    if s in {"0", "false", "no", "n", "off", "disabled"}: return False
    return default

def _safe_int(x: Any, default: Optional[int] = None) -> Optional[int]:
    if x is None: return default
    try: return int(float(str(x).strip()))
    except Exception: return default

def _safe_float(x: Any, default: Optional[float] = None) -> Optional[float]:
    if x is None: return default
    try: return float(str(x).strip())
    except Exception: return default

def _utc_now() -> datetime:
    return datetime.now(timezone.utc)

def _utc_now_iso() -> str:
    return _utc_now().isoformat()

def _async_run(coro: Awaitable[Any]) -> Any:
    try:
        loop = asyncio.get_event_loop()
        if loop.is_running():
            new_loop = asyncio.new_event_loop()
            asyncio.set_event_loop(new_loop)
            try: return new_loop.run_until_complete(coro)
            finally: new_loop.close()
        else: return loop.run_until_complete(coro)
    except RuntimeError:
        new_loop = asyncio.new_event_loop()
        asyncio.set_event_loop(new_loop)
        try: return new_loop.run_until_complete(coro)
        finally: new_loop.close()

def _maybe_await(v: Any) -> Awaitable[Any]:
    if inspect.isawaitable(v): return v
    async def _wrap() -> Any: return v
    return _wrap()

def _unwrap_payload(x: Any) -> Any:
    try:
        if isinstance(x, tuple) and len(x) >= 1: return x[0]
    except Exception: pass
    return x

def _as_list(x: Any) -> List[Any]:
    if x is None: return []
    if isinstance(x, list): return x
    if isinstance(x, dict): return list(x.values())
    if isinstance(x, (set, tuple)): return list(x)
    return [x]

def _coerce_symbol_from_payload(p: Any) -> Optional[str]:
    try:
        if p is None: return None
        if isinstance(p, dict):
            for key in ("symbol", "requested_symbol", "ticker", "code", "id"):
                if key in p and p.get(key): return _safe_upper(p.get(key))
            return None
        for attr in ("symbol", "requested_symbol", "ticker", "code", "id"):
            if hasattr(p, attr) and getattr(p, attr): return _safe_upper(getattr(p, attr))
    except Exception: pass
    return None

def _finalize_quote(obj: Any) -> Any:
    try:
        fn = getattr(obj, "finalize", None)
        if callable(fn): return fn()
    except Exception: pass
    return obj


# ============================================================================
# Symbol Normalization (with provider-specific routing)
# ============================================================================

@dataclass(slots=True)
class SymbolInfo:
    """Information about a symbol."""
    raw: str
    normalized: str
    market: str  # "KSA", "US", "GLOBAL", "SPECIAL"
    asset_class: str  # "equity", "index", "forex", "crypto", "commodity"
    exchange: Optional[str] = None
    currency: Optional[str] = None
    provider_hint: Optional[str] = None


class SymbolNormalizer:
    """Thread-safe symbol normalizer with provider-specific rules."""

    def __init__(self):
        self._lock = threading.RLock()
        self._cache: Dict[str, SymbolInfo] = {}
        self._norm_fn: Optional[Callable[[str], str]] = None

    def _load_normalizer(self) -> Optional[Callable[[str], str]]:
        if self._norm_fn is not None:
            return self._norm_fn

        with self._lock:
            if self._norm_fn is not None:
                return self._norm_fn

            try:
                mod = import_module("core.symbols.normalize")
                fn = getattr(mod, "normalize_symbol", None)
                if callable(fn):
                    self._norm_fn = fn
                    _dbg("Loaded normalizer from core.symbols.normalize", "info")
                    return fn
            except Exception:
                pass

            try:
                mod = import_module("core.data_engine_v2")
                fn = getattr(mod, "normalize_symbol", None)
                if callable(fn):
                    self._norm_fn = fn
                    _dbg("Loaded normalizer from core.data_engine_v2", "info")
                    return fn
            except Exception:
                pass

            self._norm_fn = self._fallback_normalize
            _dbg("Using fallback normalizer", "info")
            return self._norm_fn

    def _fallback_normalize(self, symbol: str) -> str:
        raw = _safe_str(symbol)
        if not raw: return ""
        su = _safe_upper(raw)

        if su.startswith("TADAWUL:"): su = su.split(":", 1)[1].strip()
        if su.endswith(".TADAWUL"): su = su.replace(".TADAWUL", "").strip()
        if any(ch in su for ch in ("=", "^", "/", "-")): return su
        if "." in su: return su
        if su.isdigit() and 3 <= len(su) <= 6: return f"{su}.SR"
        return su

    def _detect_market(self, symbol: str) -> str:
        s = _safe_upper(symbol)
        if s.endswith(".SR") or (s.isdigit() and 3 <= len(s) <= 6): return "KSA"
        if any(ch in s for ch in ("^", "=", "/")): return "SPECIAL"
        if "." not in s: return "US"

        parts = s.split(".")
        if len(parts) >= 2:
            suffix = parts[-1]
            if suffix in {"US", "NYSE", "NASDAQ", "N", "OQ"}: return "US"
            if suffix in {"L", "LSE", "LN"}: return "UK"
            if suffix in {"T", "TYO", "F"}: return "JP"
            if suffix in {"HK", "HKG"}: return "HK"

        return "GLOBAL"

    def _detect_asset_class(self, symbol: str) -> str:
        s = _safe_upper(symbol)
        if s.startswith("^") or s.endswith(".INDX"): return "index"
        if ("=" in s and s.endswith("=X")) or "/" in s: return "forex"
        if ("=" in s and s.endswith("=F")) or s.endswith(".COMM") or s.endswith(".COM"): return "commodity"
        if "-" in s and s.split("-")[1] in {"USD", "USDT", "BTC", "ETH"}: return "crypto"
        return "equity"

    def _detect_provider_hint(self, symbol: str) -> Optional[str]:
        s = _safe_upper(symbol)
        if s.endswith(".SR") or (s.isdigit() and 3 <= len(s) <= 6): return "tadawul"
        if s.startswith("^") or "=" in s or "/" in s: return "yahoo"
        if s.endswith(".L") or s.endswith(".DE"): return "eodhd"
        return None

    def normalize(self, symbol: str) -> str:
        raw = _safe_str(symbol)
        if not raw: return ""
        if raw in self._cache: return self._cache[raw].normalized

        fn = self._load_normalizer()
        try: norm = fn(raw) if callable(fn) else self._fallback_normalize(raw)
        except Exception: norm = self._fallback_normalize(raw)

        info = SymbolInfo(
            raw=raw, normalized=norm, market=self._detect_market(norm),
            asset_class=self._detect_asset_class(norm), provider_hint=self._detect_provider_hint(norm),
        )
        with self._lock: self._cache[raw] = info
        return norm

    def get_info(self, symbol: str) -> SymbolInfo:
        raw = _safe_str(symbol)
        if raw in self._cache: return self._cache[raw]
        norm = self.normalize(raw)
        return self._cache.get(raw, SymbolInfo(
            raw=raw, normalized=norm, market=self._detect_market(norm), asset_class=self._detect_asset_class(norm),
        ))

    def clean_symbols(self, symbols: Sequence[Any]) -> List[str]:
        seen: Set[str] = set()
        out: List[str] = []
        for s in symbols or []:
            raw = _safe_str(s)
            if not raw: continue
            norm = self.normalize(raw)
            if norm in seen: continue
            seen.add(norm)
            out.append(raw)
        return out


_SYMBOL_NORMALIZER = SymbolNormalizer()
normalize_symbol = _SYMBOL_NORMALIZER.normalize
get_symbol_info = _SYMBOL_NORMALIZER.get_info


# ============================================================================
# V2 Engine Discovery (True Lazy)
# ============================================================================

@dataclass(slots=True)
class V2ModuleInfo:
    """Information about discovered V2 module."""
    module: Any
    version: Optional[str] = None
    engine_class: Optional[Type] = None
    engine_v2_class: Optional[Type] = None
    engine_v3_class: Optional[Type] = None
    unified_quote_class: Optional[Type] = None
    has_module_funcs: bool = False
    error: Optional[str] = None


class V2Discovery:
    """Thread-safe V2 module discovery with caching."""

    def __init__(self):
        self._lock = threading.RLock()
        self._info: Optional[V2ModuleInfo] = None
        self._error: Optional[str] = None
        self._mode = EngineMode.UNKNOWN

    def discover(self) -> Tuple[EngineMode, Optional[V2ModuleInfo], Optional[str]]:
        if self._mode != EngineMode.UNKNOWN: return self._mode, self._info, self._error

        with self._lock:
            if self._mode != EngineMode.UNKNOWN: return self._mode, self._info, self._error

            if _V2_DISABLED:
                self._mode, self._error = EngineMode.STUB, "V2 disabled by env: DATA_ENGINE_V2_DISABLED=true"
                _dbg(self._error, "warn")
                return self._mode, None, self._error

            try:
                mod = import_module("core.data_engine_v2")
                version = getattr(mod, "__version__", None) or getattr(mod, "VERSION", None)
                engine_class = getattr(mod, "DataEngine", None) or getattr(mod, "Engine", None)
                engine_v2_class = getattr(mod, "DataEngineV2", None)
                engine_v3_class = getattr(mod, "DataEngineV3", None)
                uq_class = getattr(mod, "UnifiedQuote", None)
                has_funcs = any(callable(getattr(mod, name, None)) for name in ["get_quote", "get_quotes", "get_enriched_quote", "get_enriched_quotes"])

                if not (engine_class or engine_v2_class or engine_v3_class or has_funcs or uq_class):
                    raise ImportError("No usable exports found in core.data_engine_v2")

                if uq_class is None:
                    try:
                        schemas = import_module("core.schemas")
                        uq_class = getattr(schemas, "UnifiedQuote", None)
                    except Exception: pass

                self._info = V2ModuleInfo(
                    module=mod, version=version, engine_class=engine_class,
                    engine_v2_class=engine_v2_class, engine_v3_class=engine_v3_class,
                    unified_quote_class=uq_class, has_module_funcs=has_funcs,
                )
                self._mode = EngineMode.V2 if engine_v2_class else EngineMode.LEGACY
                _dbg(f"Discovered V2 engine (version={version}, mode={self._mode.value})", "info")
                return self._mode, self._info, None

            except Exception as e:
                self._mode, self._error = EngineMode.STUB, str(e)
                _dbg(f"V2 discovery failed: {e}", "warn")
                return self._mode, None, self._error


_V2_DISCOVERY = V2Discovery()


# ============================================================================
# Stub Models (when V2 unavailable)
# ============================================================================

class StubUnifiedQuote(BaseModel):
    """Stub quote model when V2 is unavailable."""
    if _PYDANTIC_V2:
        model_config = ConfigDict(populate_by_name=True, extra="ignore")

    symbol: str
    name: Optional[str] = None
    market: str = "UNKNOWN"
    exchange: Optional[str] = None
    currency: Optional[str] = None
    current_price: Optional[float] = None
    previous_close: Optional[float] = None
    open: Optional[float] = None
    day_high: Optional[float] = None
    day_low: Optional[float] = None
    week_52_high: Optional[float] = None
    week_52_low: Optional[float] = None
    volume: Optional[float] = None
    avg_volume_30d: Optional[float] = None
    value_traded: Optional[float] = None
    price_change: Optional[float] = None
    percent_change: Optional[float] = None
    market_cap: Optional[float] = None
    shares_outstanding: Optional[float] = None
    free_float_pct: Optional[float] = None
    pe_ttm: Optional[float] = None
    pe_forward: Optional[float] = None
    pb: Optional[float] = None
    ps: Optional[float] = None
    eps_ttm: Optional[float] = None
    eps_forward: Optional[float] = None
    dividend_yield: Optional[float] = None
    beta: Optional[float] = None
    quality_score: Optional[float] = None
    value_score: Optional[float] = None
    momentum_score: Optional[float] = None
    risk_score: Optional[float] = None
    opportunity_score: Optional[float] = None
    overall_score: Optional[float] = None
    forecast_price_1m: Optional[float] = None
    expected_roi_1m: Optional[float] = None
    forecast_price_3m: Optional[float] = None
    expected_roi_3m: Optional[float] = None
    forecast_price_12m: Optional[float] = None
    expected_roi_12m: Optional[float] = None
    forecast_confidence: Optional[float] = None
    forecast_method: Optional[str] = None
    forecast_updated_utc: Optional[str] = None
    forecast_updated_riyadh: Optional[str] = None
    data_source: str = "stub"
    provider: Optional[str] = None
    data_quality: str = "MISSING"
    last_updated_utc: str = Field(default_factory=_utc_now_iso)
    error: Optional[str] = "Engine Unavailable"
    warnings: List[str] = Field(default_factory=list)
    request_id: Optional[str] = None
    price: Optional[float] = None
    change: Optional[float] = None

    def finalize(self) -> "StubUnifiedQuote":
        if self.current_price is None and self.price is not None: self.current_price = self.price
        if self.price_change is None and self.change is not None: self.price_change = self.change
        if self.percent_change is None and self.price_change is not None and self.previous_close not in (None, 0):
            try: self.percent_change = (self.price_change / self.previous_close) * 100.0
            except Exception: pass
        if self.forecast_updated_utc is None: self.forecast_updated_utc = self.last_updated_utc
        return self

    def dict(self, *args: Any, **kwargs: Any) -> Dict[str, Any]:
        if hasattr(self, "model_dump"): return self.model_dump(*args, **kwargs)
        return dict(self.__dict__)


class StubEngine:
    """Stub engine when V2 is unavailable."""
    def __init__(self, *args: Any, **kwargs: Any) -> None:
        logger.error("DataEngine: initialized STUB engine (V2 missing/unavailable)")
        self._start_time = time.time()
        self._request_count = 0
        self._error_count = 0

    async def get_quote(self, symbol: str) -> StubUnifiedQuote:
        self._request_count += 1
        return StubUnifiedQuote(symbol=_safe_str(symbol), error="Engine V2 Missing", warnings=["Using stub engine"]).finalize()

    async def get_quotes(self, symbols: List[str]) -> List[StubUnifiedQuote]:
        self._request_count += len(symbols)
        return [StubUnifiedQuote(symbol=_safe_str(s), error="Engine V2 Missing", warnings=["Using stub engine"]).finalize() for s in symbols or []]

    async def get_enriched_quote(self, symbol: str) -> StubUnifiedQuote:
        return await self.get_quote(symbol)

    async def get_enriched_quotes(self, symbols: List[str]) -> List[StubUnifiedQuote]:
        return await self.get_quotes(symbols)

    async def aclose(self) -> None: pass

    def get_stats(self) -> Dict[str, Any]:
        return {"mode": "stub", "uptime_seconds": time.time() - self._start_time, "request_count": self._request_count, "error_count": self._error_count, "error": "Engine V2 Missing"}


# ============================================================================
# Engine Singleton Management
# ============================================================================

class EngineManager:
    """Thread-safe engine manager with singleton pattern."""

    def __init__(self):
        self._lock = threading.RLock()
        self._engine: Optional[Any] = None
        self._v2_info: Optional[V2ModuleInfo] = None
        self._mode = EngineMode.UNKNOWN
        self._error: Optional[str] = None
        self._circuit_breaker = DynamicCircuitBreaker("engine", failure_threshold=5, base_timeout_seconds=30.0, max_timeout_seconds=300.0)
        self._rate_limiter = TokenBucket(rate_per_sec=100.0)
        self._cache = DistributedCache(backend=CacheBackend.MEMORY, default_ttl=300, max_size=10000, compression=True)
        self._stats: Dict[str, Any] = {"created_at": _utc_now_iso(), "requests": 0, "errors": 0, "cache_hits": 0, "cache_misses": 0}

    def _discover(self) -> Tuple[EngineMode, Optional[V2ModuleInfo], Optional[str]]:
        if self._mode != EngineMode.UNKNOWN: return self._mode, self._v2_info, self._error
        mode, info, error = _V2_DISCOVERY.discover()
        self._mode, self._v2_info, self._error = mode, info, error
        return mode, info, error

    def _instantiate_engine(self, engine_class: Type) -> Optional[Any]:
        try: return engine_class()
        except TypeError: pass
        try:
            from config import get_settings as get_config_settings
            settings = get_config_settings()
            try: return engine_class(settings=settings)
            except TypeError:
                try: return engine_class(settings)
                except TypeError: pass
        except Exception: pass
        try:
            from core.config import get_settings as get_core_settings
            settings = get_core_settings()
            try: return engine_class(settings=settings)
            except TypeError:
                try: return engine_class(settings)
                except TypeError: pass
        except Exception: pass
        try: return engine_class()
        except Exception: return None

    def _get_v2_engine(self) -> Optional[Any]:
        mode, info, _ = self._discover()
        if mode not in (EngineMode.V2, EngineMode.LEGACY) or info is None: return None
        if info.has_module_funcs and hasattr(info.module, "get_engine"):
            try:
                engine = _async_run(_maybe_await(getattr(info.module, "get_engine")()))
                if engine is not None: return engine
            except Exception: pass
        for cls in [info.engine_v3_class, info.engine_v2_class, info.engine_class]:
            if cls is not None:
                engine = self._instantiate_engine(cls)
                if engine is not None: return engine
        return None

    async def get_engine(self) -> Any:
        if self._engine is not None: return self._engine
        async with self._lock:
            if self._engine is not None: return self._engine
            await self._rate_limiter.wait_and_acquire()
            async def _create_engine():
                v2_engine = self._get_v2_engine()
                if v2_engine is not None:
                    self._engine = v2_engine
                    _dbg("Using V2 engine", "info")
                    _METRICS.set("provider_health", 1, {"provider": "engine_v2"})
                    return v2_engine
                self._engine = StubEngine()
                _dbg("Using stub engine (V2 unavailable)", "warn")
                _METRICS.set("provider_health", -1, {"provider": "stub"})
                return self._engine
            return await self._circuit_breaker.execute(_create_engine)

    async def get_engine_async(self) -> Any:
        return await self.get_engine()

    async def close_engine(self) -> None:
        if self._engine is None: return
        engine = self._engine
        self._engine = None
        try:
            if hasattr(engine, "aclose") and callable(engine.aclose):
                await _maybe_await(engine.aclose())
        except Exception as e:
            _dbg(f"Error closing engine: {e}", "error")

    def get_stats(self) -> Dict[str, Any]:
        stats = dict(self._stats)
        stats["mode"] = self._mode.value
        stats["v2_available"] = (self._mode in (EngineMode.V2, EngineMode.LEGACY))
        stats["engine_active"] = self._engine is not None
        stats["circuit_breaker"] = self._circuit_breaker.get_stats()
        stats["rate_limiter"] = self._rate_limiter.get_stats()
        stats["cache"] = self._cache.get_stats()
        if self._engine is not None and hasattr(self._engine, "get_stats"):
            try: stats["engine_stats"] = self._engine.get_stats()
            except Exception: pass
        return stats

    def record_request(self, success: bool = True) -> None:
        with self._lock:
            self._stats["requests"] = self._stats.get("requests", 0) + 1
            if not success: self._stats["errors"] = self._stats.get("errors", 0) + 1

    def get_cache(self) -> DistributedCache:
        return self._cache


_ENGINE_MANAGER = EngineManager()


# ============================================================================
# Unified Quote Class Resolution
# ============================================================================

def get_unified_quote_class() -> Type:
    mode, info, _ = _V2_DISCOVERY.discover()
    if mode in (EngineMode.V2, EngineMode.LEGACY) and info is not None and info.unified_quote_class is not None:
        return info.unified_quote_class
    return StubUnifiedQuote


UnifiedQuote = get_unified_quote_class()
DataEngineV2 = _ENGINE_MANAGER._v2_info.engine_v2_class if _ENGINE_MANAGER._v2_info else None
DataEngineV3 = _ENGINE_MANAGER._v2_info.engine_v3_class if _ENGINE_MANAGER._v2_info else None


# ============================================================================
# Core API Functions
# ============================================================================

async def get_engine() -> Any:
    return await _ENGINE_MANAGER.get_engine_async()

def get_engine_sync() -> Any:
    return _async_run(_ENGINE_MANAGER.get_engine_async())

async def close_engine() -> None:
    await _ENGINE_MANAGER.close_engine()

def get_cache() -> DistributedCache:
    return _ENGINE_MANAGER.get_cache()

def _get_v2_module_func(name: str) -> Optional[Callable]:
    mode, info, _ = _V2_DISCOVERY.discover()
    if mode in (EngineMode.V2, EngineMode.LEGACY) and info is not None and info.module is not None:
        return getattr(info.module, name, None)
    return None

def _candidate_method_names(enriched: bool, batch: bool) -> List[str]:
    if enriched and batch: return ["get_enriched_quotes", "get_enriched_quote_batch", "fetch_enriched_quotes", "fetch_enriched_quote_batch", "get_quotes_enriched", "get_enriched_quotes_batch"]
    if enriched and not batch: return ["get_enriched_quote", "fetch_enriched_quote", "fetch_enriched_quote_patch", "enriched_quote", "get_enriched"]
    if not enriched and batch: return ["get_quotes", "fetch_quotes", "quotes", "fetch_many", "get_quote_batch", "get_quotes_batch"]
    return ["get_quote", "fetch_quote", "quote", "fetch", "get"]

async def _call_engine_method(engine: Any, method_name: str, *args: Any, **kwargs: Any) -> Any:
    method = getattr(engine, method_name, None)
    if method is None: raise AttributeError(f"Engine has no method '{method_name}'")
    try:
        result = method(*args, **kwargs)
        if inspect.isawaitable(result): result = await result
        return result
    except Exception as e:
        _dbg(f"Engine method '{method_name}' failed: {e}", "error")
        raise

async def _call_engine_single(engine: Any, symbol: str, *, enriched: bool, use_cache: bool = True, ttl: Optional[int] = None) -> Any:
    norm_symbol = normalize_symbol(symbol)
    if use_cache:
        cache_key = f"quote:{norm_symbol}:{'enriched' if enriched else 'basic'}"
        cached = await _ENGINE_MANAGER.get_cache().get("quotes", cache_key)
        if cached is not None:
            _ENGINE_MANAGER._stats["cache_hits"] += 1
            _METRICS.inc("cache_hits_total", 1, {"cache_type": "engine"})
            return cached
        _ENGINE_MANAGER._stats["cache_misses"] += 1
        _METRICS.inc("cache_misses_total", 1, {"cache_type": "engine"})

    func_name = "get_enriched_quote" if enriched else "get_quote"
    module_func = _get_v2_module_func(func_name)
    if module_func is not None:
        try:
            result = await _maybe_await(module_func(norm_symbol))
            result = _unwrap_payload(result)
            if use_cache and result and not getattr(result, 'error', None):
                await _ENGINE_MANAGER.get_cache().set("quotes", cache_key, result, ttl)
            return result
        except Exception as e:
            _dbg(f"Module func '{func_name}' failed: {e}", "debug")

    for method_name in _candidate_method_names(enriched=enriched, batch=False):
        try:
            result = await _call_engine_method(engine, method_name, norm_symbol)
            result = _unwrap_payload(result)
            if use_cache and result and not getattr(result, 'error', None):
                await _ENGINE_MANAGER.get_cache().set("quotes", cache_key, result, ttl)
            return result
        except (AttributeError, NotImplementedError): continue
        except Exception as e:
            _dbg(f"Engine method '{method_name}' failed: {e}", "debug")
            continue

    if not enriched:
        try: return await _call_engine_single(engine, symbol, enriched=True, use_cache=use_cache, ttl=ttl)
        except Exception: pass
    raise AttributeError("No suitable engine method found")


async def _call_engine_batch(engine: Any, symbols: List[str], *, enriched: bool, use_cache: bool = True, ttl: Optional[int] = None) -> Any:
    norm_symbols = [normalize_symbol(s) for s in symbols]
    cached_results = {}
    missing_indices = []
    
    if use_cache:
        cache = _ENGINE_MANAGER.get_cache()
        for i, sym in enumerate(norm_symbols):
            cache_key = f"quote:{sym}:{'enriched' if enriched else 'basic'}"
            cached = await cache.get("quotes", cache_key)
            if cached is not None:
                cached_results[i] = cached
                _ENGINE_MANAGER._stats["cache_hits"] += 1
                _METRICS.inc("cache_hits_total", 1, {"cache_type": "engine"})
            else:
                missing_indices.append(i)
                _ENGINE_MANAGER._stats["cache_misses"] += 1
                _METRICS.inc("cache_misses_total", 1, {"cache_type": "engine"})
        
        if not missing_indices: return [cached_results[i] for i in range(len(symbols))]
        missing_symbols = [norm_symbols[i] for i in missing_indices]
    else:
        missing_symbols = norm_symbols
        missing_indices = list(range(len(symbols)))

    func_name = "get_enriched_quotes" if enriched else "get_quotes"
    module_func = _get_v2_module_func(func_name)
    if module_func is not None:
        try:
            result = await _maybe_await(module_func(missing_symbols))
            aligned = _align_batch_results(_unwrap_payload(result), missing_symbols)
            if use_cache:
                cache = _ENGINE_MANAGER.get_cache()
                for sym, res in zip(missing_symbols, aligned):
                    if res and not getattr(res, 'error', None):
                        await cache.set("quotes", f"quote:{sym}:{'enriched' if enriched else 'basic'}", res, ttl)
            
            final_results, cache_idx = [], 0
            for i in range(len(symbols)):
                if i in cached_results: final_results.append(cached_results[i])
                else:
                    final_results.append(aligned[cache_idx])
                    cache_idx += 1
            return final_results
        except Exception as e:
            _dbg(f"Module func '{func_name}' failed: {e}", "debug")

    for method_name in _candidate_method_names(enriched=enriched, batch=True):
        try:
            result = await _call_engine_method(engine, method_name, missing_symbols)
            aligned = _align_batch_results(_unwrap_payload(result), missing_symbols)
            if use_cache:
                cache = _ENGINE_MANAGER.get_cache()
                for sym, res in zip(missing_symbols, aligned):
                    if res and not getattr(res, 'error', None):
                        await cache.set("quotes", f"quote:{sym}:{'enriched' if enriched else 'basic'}", res, ttl)
            
            final_results, cache_idx = [], 0
            for i in range(len(symbols)):
                if i in cached_results: final_results.append(cached_results[i])
                else:
                    final_results.append(aligned[cache_idx])
                    cache_idx += 1
            return final_results
        except (AttributeError, NotImplementedError): continue
        except Exception as e:
            _dbg(f"Engine method '{method_name}' failed: {e}", "debug")
            continue

    results = []
    for symbol in missing_symbols:
        try: results.append(await _call_engine_single(engine, symbol, enriched=enriched, use_cache=False))
        except Exception as e:
            _dbg(f"Sequential call for '{symbol}' failed: {e}", "error")
            results.append(None)
    
    aligned = _align_batch_results(results, missing_symbols)
    final_results, cache_idx = [], 0
    for i in range(len(symbols)):
        if i in cached_results: final_results.append(cached_results[i])
        else:
            final_results.append(aligned[cache_idx])
            cache_idx += 1
    return final_results


def _align_batch_results(results: Any, requested_symbols: List[str], strict: bool = False) -> List[Any]:
    payload = _unwrap_payload(results)
    if isinstance(payload, dict):
        lookup: Dict[str, Any] = {}
        for k, v in payload.items():
            norm_key = normalize_symbol(_safe_str(k))
            if norm_key: lookup[norm_key] = v
            ps = _coerce_symbol_from_payload(v)
            if ps: lookup[normalize_symbol(ps)] = v
        return [lookup.get(normalize_symbol(sym)) for sym in requested_symbols]

    arr = _as_list(payload)
    if not arr: return [None] * len(requested_symbols)

    lookup2: Dict[str, Any] = {}
    for item in arr:
        ps = _coerce_symbol_from_payload(item)
        if ps: lookup2[normalize_symbol(ps)] = item

    if lookup2: return [lookup2.get(normalize_symbol(sym)) for sym in requested_symbols]
    if len(arr) >= len(requested_symbols): return arr[:len(requested_symbols)]
    return arr + [None] * (len(requested_symbols) - len(arr))


@trace("get_enriched_quote")
@monitor_perf("get_enriched_quote")
async def get_enriched_quote(symbol: str, use_cache: bool = True, ttl: Optional[int] = None) -> Any:
    UQ = get_unified_quote_class()
    sym_in = _safe_str(symbol)
    if not sym_in: return UQ(symbol="", error="Empty symbol", data_quality="MISSING")

    start = time.perf_counter()
    _METRICS.inc("requests_total", 1, {"operation": "get_enriched_quote", "status": "started"})
    _METRICS.set("active_requests", 1)

    try:
        engine = await get_engine()
        result = await _call_engine_single(engine, sym_in, enriched=True, use_cache=use_cache, ttl=ttl)
        finalized = _finalize_quote(_unwrap_payload(result))
        _ENGINE_MANAGER.record_request(success=True)
        _METRICS.inc("requests_total", 1, {"operation": "get_enriched_quote", "status": "success"})
        _METRICS.set("active_requests", 0)

        if _PERF_MONITORING:
            elapsed = (time.perf_counter() - start) * 1000
            _METRICS.observe("request_duration_seconds", elapsed / 1000, {"operation": "get_enriched_quote"})
        return finalized

    except Exception as e:
        _ENGINE_MANAGER.record_request(success=False)
        _METRICS.inc("requests_total", 1, {"operation": "get_enriched_quote", "status": "error"})
        _METRICS.set("active_requests", 0)
        _dbg(f"get_enriched_quote('{symbol}') failed: {e}", "error")

        if _PERF_MONITORING:
            elapsed = (time.perf_counter() - start) * 1000
            _METRICS.observe("request_duration_seconds", elapsed / 1000, {"operation": "get_enriched_quote"})

        if _STRICT_MODE: raise
        return UQ(symbol=normalize_symbol(sym_in) or sym_in, error=str(e), data_quality="MISSING", warnings=["Error retrieving quote"], request_id=str(uuid.uuid4())).finalize()


@trace("get_enriched_quotes")
@monitor_perf("get_enriched_quotes")
async def get_enriched_quotes(symbols: List[str], use_cache: bool = True, ttl: Optional[int] = None) -> List[Any]:
    UQ = get_unified_quote_class()
    clean = _SYMBOL_NORMALIZER.clean_symbols(symbols)
    if not clean: return []

    normed = [normalize_symbol(s) for s in clean]
    _METRICS.inc("requests_total", 1, {"operation": "get_enriched_quotes", "status": "started"})
    _METRICS.set("active_requests", len(clean))

    try:
        engine = await get_engine()
        result = await _call_engine_batch(engine, normed, enriched=True, use_cache=use_cache, ttl=ttl)
        aligned = _align_batch_results(result, normed)

        output = []
        for orig, norm, item in zip(clean, normed, aligned):
            if item is None:
                output.append(UQ(symbol=norm, original_symbol=orig, error="Missing in batch result", data_quality="MISSING", request_id=str(uuid.uuid4())).finalize())
            else:
                finalized = _finalize_quote(_unwrap_payload(item))
                if hasattr(finalized, "original_symbol"): finalized.original_symbol = orig
                output.append(finalized)

        _ENGINE_MANAGER.record_request(success=True)
        _METRICS.inc("requests_total", 1, {"operation": "get_enriched_quotes", "status": "success"})
        _METRICS.set("active_requests", 0)
        return output

    except Exception as e:
        _ENGINE_MANAGER.record_request(success=False)
        _METRICS.inc("requests_total", 1, {"operation": "get_enriched_quotes", "status": "error"})
        _METRICS.set("active_requests", 0)
        _dbg(f"get_enriched_quotes failed: {e}", "error")

        if _STRICT_MODE: raise
        return [UQ(symbol=n, original_symbol=o, error=str(e), data_quality="MISSING", request_id=str(uuid.uuid4())).finalize() for o, n in zip(clean, normed)]


async def get_quote(symbol: str, use_cache: bool = True, ttl: Optional[int] = None) -> Any:
    return await get_enriched_quote(symbol, use_cache=use_cache, ttl=ttl)


async def get_quotes(symbols: List[str], use_cache: bool = True, ttl: Optional[int] = None) -> List[Any]:
    return await get_enriched_quotes(symbols, use_cache=use_cache, ttl=ttl)


# ============================================================================
# Batch Processing with Progress Tracking
# ============================================================================

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
    def elapsed_seconds(self) -> float: return time.time() - self.start_time
    @property
    def completion_pct(self) -> float: return (self.completed / self.total * 100) if self.total > 0 else 0
    @property
    def success_rate(self) -> float: return (self.succeeded / self.completed * 100) if self.completed > 0 else 0
    
    def to_dict(self) -> Dict[str, Any]:
        return {"batch_id": self.batch_id, "total": self.total, "completed": self.completed, "succeeded": self.succeeded, "failed": self.failed, "completion_pct": self.completion_pct, "success_rate": self.success_rate, "elapsed_seconds": self.elapsed_seconds, "errors": self.errors[:10], "metadata": self.metadata}


async def process_batch(
    symbols: List[str], batch_size: int = 10, delay_seconds: float = 0.1,
    enriched: bool = True, use_cache: bool = True, ttl: Optional[int] = None,
    max_retries: int = 3, progress_callback: Optional[Callable[[BatchProgress], None]] = None,
    metadata: Optional[Dict[str, Any]] = None
) -> List[Any]:
    clean = _SYMBOL_NORMALIZER.clean_symbols(symbols)
    if not clean: return []

    progress = BatchProgress(total=len(clean), metadata=metadata or {})
    results: List[Optional[Any]] = [None] * len(clean)
    retry_counts: Dict[int, int] = defaultdict(int)
    func = get_enriched_quotes if enriched else get_quotes

    for i in range(0, len(clean), batch_size):
        batch = clean[i:i + batch_size]
        batch_indices = list(range(i, min(i + batch_size, len(clean))))

        try:
            batch_results = await func(batch, use_cache=use_cache, ttl=ttl)
            for idx, result in zip(batch_indices, batch_results):
                results[idx] = result
                progress.completed += 1
                if hasattr(result, "error") and result.error:
                    if retry_counts[idx] < max_retries:
                        retry_counts[idx] += 1
                        progress.completed -= 1
                    else:
                        progress.failed += 1
                        progress.errors.append((clean[idx], str(result.error)))
                else:
                    progress.succeeded += 1
        except Exception as e:
            _dbg(f"Batch {i//batch_size + 1} failed: {e}", "error")
            for idx in batch_indices:
                if retry_counts[idx] < max_retries: retry_counts[idx] += 1
                else:
                    results[idx] = None
                    progress.completed += 1
                    progress.failed += 1
                    progress.errors.append((clean[idx], str(e)))

        if progress_callback is not None: progress_callback(progress)
        if i + batch_size < len(clean): await asyncio.sleep(delay_seconds)

    return results


# ============================================================================
# Health Check
# ============================================================================

async def health_check() -> Dict[str, Any]:
    health = {"status": "healthy", "version": __version__, "timestamp": _utc_now_iso(), "checks": {}, "warnings": [], "errors": []}
    try:
        engine = await get_engine()
        if isinstance(engine, StubEngine):
            health["warnings"].append("Using stub engine - no real data")
            health["status"] = "degraded"
        
        health["checks"]["cache"] = _ENGINE_MANAGER.get_cache().get_stats()
        cb_stats = _ENGINE_MANAGER._circuit_breaker.get_stats()
        health["checks"]["circuit_breaker"] = cb_stats
        if cb_stats["state"] != "closed":
            health["warnings"].append(f"Circuit breaker is {cb_stats['state']}")
            health["status"] = "degraded"
        
        rl_stats = _ENGINE_MANAGER._rate_limiter.get_stats()
        health["checks"]["rate_limiter"] = rl_stats
        if rl_stats["utilization"] > 0.9: health["warnings"].append("Rate limiter near capacity")
        
        try:
            test_result = await get_enriched_quote("AAPL", use_cache=False)
            if test_result and not test_result.error: health["checks"]["quote_test"] = "passed"
            else:
                health["checks"]["quote_test"], health["status"] = "failed", "unhealthy"
                health["errors"].append("Quote test failed")
        except Exception as e:
            health["checks"]["quote_test"], health["status"] = "failed", "unhealthy"
            health["errors"].append(f"Quote test error: {e}")
            
    except Exception as e:
        health["errors"].append(f"Health check failed: {e}")
        health["status"] = "unhealthy"
    return health


# ============================================================================
# Context Managers & Class Wrappers
# ============================================================================

@asynccontextmanager
async def engine_context() -> AsyncGenerator[Any, None]:
    engine = await get_engine()
    try: yield engine
    finally: await close_engine()

class EngineSession:
    def __enter__(self) -> Any:
        self._engine = get_engine_sync()
        return self._engine
    def __exit__(self, *args: Any) -> None:
        _async_run(close_engine())


class DataEngine:
    """Lightweight wrapper for backward compatibility. Delegates to the shared engine instance."""
    def __init__(self, *args: Any, **kwargs: Any) -> None:
        self._engine: Optional[Any] = None
        self._args, self._kwargs = args, kwargs
        self._request_id = str(uuid.uuid4())

    async def _ensure(self) -> Any:
        if self._engine is None: self._engine = await get_engine()
        return self._engine

    async def get_quote(self, symbol: str, use_cache: bool = True) -> Any:
        engine = await self._ensure()
        try: return _finalize_quote(_unwrap_payload(await _call_engine_single(engine, symbol, enriched=False, use_cache=use_cache)))
        except Exception: return await get_quote(symbol, use_cache=use_cache)

    async def get_quotes(self, symbols: List[str], use_cache: bool = True) -> List[Any]:
        engine = await self._ensure()
        try:
            aligned = _align_batch_results(await _call_engine_batch(engine, symbols, enriched=False, use_cache=use_cache), symbols)
            return [_finalize_quote(_unwrap_payload(x)) if x is not None else StubUnifiedQuote(symbol=_safe_str(s), error="Missing", request_id=self._request_id).finalize() for x, s in zip(aligned, symbols)]
        except Exception: return await get_quotes(symbols, use_cache=use_cache)

    async def get_enriched_quote(self, symbol: str, use_cache: bool = True) -> Any:
        engine = await self._ensure()
        try: return _finalize_quote(_unwrap_payload(await _call_engine_single(engine, symbol, enriched=True, use_cache=use_cache)))
        except Exception: return await get_enriched_quote(symbol, use_cache=use_cache)

    async def get_enriched_quotes(self, symbols: List[str], use_cache: bool = True) -> List[Any]:
        engine = await self._ensure()
        try:
            aligned = _align_batch_results(await _call_engine_batch(engine, symbols, enriched=True, use_cache=use_cache), symbols)
            return [_finalize_quote(_unwrap_payload(x)) if x is not None else StubUnifiedQuote(symbol=_safe_str(s), error="Missing", request_id=self._request_id).finalize() for x, s in zip(aligned, symbols)]
        except Exception: return await get_enriched_quotes(symbols, use_cache=use_cache)

    async def aclose(self) -> None:
        await close_engine()


# ============================================================================
# Diagnostics and Metadata
# ============================================================================

def _get_settings_object() -> Optional[Any]:
    try:
        from config import get_settings as get_config_settings
        return get_config_settings()
    except Exception: pass
    try:
        from core.config import get_settings as get_core_settings
        return get_core_settings()
    except Exception: pass
    return None

def _parse_env_list(key: str) -> List[str]:
    raw = (os.getenv(key) or "").strip()
    return [p.strip().lower() for p in raw.split(",") if p.strip()] if raw else []

def get_engine_meta() -> Dict[str, Any]:
    mode, info, error = _V2_DISCOVERY.discover()
    settings = _get_settings_object()
    providers, ksa_providers = [], []

    if settings is not None:
        try:
            p = getattr(settings, "enabled_providers", None) or getattr(settings, "providers", None)
            if isinstance(p, (list, tuple, set)): providers = [str(x).strip().lower() for x in p if str(x).strip()]
        except Exception: pass
        try:
            k = getattr(settings, "ksa_providers", None) or getattr(settings, "providers_ksa", None)
            if isinstance(k, (list, tuple, set)): ksa_providers = [str(x).strip().lower() for x in k if str(x).strip()]
        except Exception: pass

    if not providers: providers = _parse_env_list("ENABLED_PROVIDERS") or _parse_env_list("PROVIDERS")
    if not ksa_providers: ksa_providers = _parse_env_list("KSA_PROVIDERS")

    return {
        "mode": mode.value, "is_stub": mode == EngineMode.STUB, "adapter_version": ADAPTER_VERSION,
        "strict_mode": _STRICT_MODE, "v2_disabled": _V2_DISABLED, "v2_available": (mode in (EngineMode.V2, EngineMode.LEGACY)),
        "v2_version": info.version if info else None, "v2_error": error, "providers": providers, "ksa_providers": ksa_providers,
        "perf_monitoring": _PERF_MONITORING, "tracing_enabled": _TRACING_ENABLED, "engine_stats": _ENGINE_MANAGER.get_stats(),
        "perf_stats": get_perf_stats(),
    }


def __getattr__(name: str) -> Any:
    if name == "UnifiedQuote": return get_unified_quote_class()
    if name == "DataEngineV2": return DataEngineV2
    if name == "DataEngineV3": return DataEngineV3
    if name == "ENGINE_MODE": return _ENGINE_MANAGER._mode.value
    if name == "StubUnifiedQuote": return StubUnifiedQuote
    if name == "StubEngine": return StubEngine
    raise AttributeError(f"module 'core.data_engine' has no attribute '{name}'")


def __dir__() -> List[str]:
    return sorted(__all__)


__all__ = [
    "__version__", "ADAPTER_VERSION", "EngineMode", "QuoteQuality", "CircuitState", "CacheBackend",
    "QuoteSource", "UnifiedQuote", "StubUnifiedQuote", "SymbolInfo", "BatchProgress", "PerfMetrics",
    "normalize_symbol", "get_symbol_info", "get_engine", "get_engine_sync", "close_engine", "get_cache",
    "get_quote", "get_quotes", "get_enriched_quote", "get_enriched_quotes", "health_check",
    "process_batch", "engine_context", "EngineSession", "DataEngine", "DataEngineV2", "DataEngineV3",
    "StubEngine", "DynamicCircuitBreaker", "TokenBucket", "DistributedCache", "get_perf_metrics",
    "get_perf_stats", "reset_perf_metrics", "get_engine_meta", "MetricsRegistry", "_METRICS",
]

_METRICS.set("active_requests", 0)
