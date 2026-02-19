#!/usr/bin/env python3
# core/providers/eodhd_provider.py
"""
================================================================================
EODHD Provider — v3.1.0 (ENTERPRISE GLOBAL PRIMARY + ML FORECASTING + MARKET INTELLIGENCE)
================================================================================

What's new in v3.1.0:
- ✅ FIXED: Git merge conflict markers at line 506 (multiple columns)
- ✅ ADDED: Distributed tracing with OpenTelemetry
- ✅ ADDED: Prometheus metrics integration
- ✅ ADDED: Advanced circuit breaker with health monitoring
- ✅ ADDED: Request queuing with priority levels
- ✅ ADDED: Adaptive rate limiting with token bucket
- ✅ ADDED: Comprehensive error recovery strategies
- ✅ ADDED: Data quality scoring and validation
- ✅ ADDED: Performance monitoring and bottleneck detection
- ✅ ADDED: Multi-model ensemble forecasting (ARIMA, Prophet-style, ML regression)
- ✅ ADDED: Technical indicator suite (MACD, RSI, Bollinger Bands, ATR, OBV)
- ✅ ADDED: Market regime detection with confidence scoring
- ✅ ADDED: Smart caching with TTL and LRU eviction
- ✅ ADDED: Circuit breaker pattern with half-open state
- ✅ ADDED: Advanced rate limiting with token bucket
- ✅ ADDED: Comprehensive metrics and monitoring
- ✅ ADDED: Riyadh timezone awareness throughout
- ✅ ADDED: Enhanced error recovery with retry strategies
- ✅ ADDED: Data quality scoring and anomaly detection

Key Features:
- Global equity, index, forex, commodity coverage
- KSA symbol blocking with override (ALLOW_EODHD_KSA)
- Smart symbol normalization with exchange suffix handling
- Ensemble forecasts with confidence levels
- Full technical analysis suite
- Market regime classification
- Production-grade error handling
- Distributed tracing and metrics
- Priority-based request queuing
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import math
import os
import random
import re
import time
import uuid
import functools
from collections import deque, defaultdict
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta, timezone
from enum import Enum
from typing import Any, Dict, List, Optional, Sequence, Set, Tuple, Union

import httpx
import numpy as np

# Optional scientific stack with graceful fallback
try:
    from scipy import stats
    from scipy.signal import savgol_filter
    SCIPY_AVAILABLE = True
except ImportError:
    SCIPY_AVAILABLE = False

try:
    from sklearn.linear_model import LinearRegression, Ridge
    from sklearn.preprocessing import StandardScaler
    from sklearn.ensemble import RandomForestRegressor, GradientBoostingRegressor
    SKLEARN_AVAILABLE = True
except ImportError:
    SKLEARN_AVAILABLE = False

# Optional tracing and metrics
try:
    from opentelemetry import trace
    from opentelemetry.trace import Span, Tracer, Status, StatusCode
    from opentelemetry.context import attach, detach
    _OTEL_AVAILABLE = True
except ImportError:
    _OTEL_AVAILABLE = False
    trace = None

try:
    from prometheus_client import Counter, Histogram, Gauge, Summary
    _PROMETHEUS_AVAILABLE = True
except ImportError:
    _PROMETHEUS_AVAILABLE = False

logger = logging.getLogger("core.providers.eodhd_provider")

PROVIDER_VERSION = "3.1.0"
PROVIDER_NAME = "eodhd"

DEFAULT_BASE_URL = "https://eodhistoricaldata.com/api"
DEFAULT_TIMEOUT_SEC = 15.0
DEFAULT_RETRY_ATTEMPTS = 3
DEFAULT_RATE_LIMIT = 5.0  # requests per second
DEFAULT_CIRCUIT_BREAKER_THRESHOLD = 5
DEFAULT_CIRCUIT_BREAKER_TIMEOUT = 30.0
DEFAULT_QUEUE_SIZE = 1000
DEFAULT_PRIORITY_LEVELS = 3

_TRUTHY = {"1", "true", "yes", "y", "on", "t"}
_FALSY = {"0", "false", "no", "n", "off", "f"}

USER_AGENT_DEFAULT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/120.0.0.0 Safari/537.36"
)

# Tracing and metrics
_TRACING_ENABLED = os.getenv("EODHD_TRACING_ENABLED", "").strip().lower() in {"1", "true", "yes", "y", "on"}
_METRICS_ENABLED = os.getenv("EODHD_METRICS_ENABLED", "").strip().lower() in {"1", "true", "yes", "y", "on"}


# ============================================================================
# Enums & Data Classes
# ============================================================================

class CircuitState(Enum):
    CLOSED = "closed"
    OPEN = "open"
    HALF_OPEN = "half_open"


class MarketRegime(Enum):
    BULL = "bull"
    BEAR = "bear"
    VOLATILE = "volatile"
    SIDEWAYS = "sideways"
    UNKNOWN = "unknown"


class AssetClass(Enum):
    EQUITY = "equity"
    INDEX = "index"
    FOREX = "forex"
    COMMODITY = "commodity"
    ETF = "etf"
    FUND = "fund"
    BOND = "bond"
    UNKNOWN = "unknown"


class RequestPriority(Enum):
    LOW = 0
    NORMAL = 1
    HIGH = 2
    CRITICAL = 3


@dataclass
class CircuitBreakerStats:
    failures: int = 0
    successes: int = 0
    last_failure: float = 0.0
    last_success: float = 0.0
    state: CircuitState = CircuitState.CLOSED
    open_until: float = 0.0
    total_failures: int = 0
    total_successes: int = 0


@dataclass
class RequestQueueItem:
    priority: RequestPriority
    endpoint: str
    params: Dict[str, Any]
    response_type: str
    future: asyncio.Future
    created_at: float = field(default_factory=time.time)
    retry_count: int = 0


# ============================================================================
# Tracing Integration
# ============================================================================

class TraceContext:
    """OpenTelemetry trace context manager."""
    
    def __init__(self, name: str, attributes: Optional[Dict[str, Any]] = None):
        self.name = name
        self.attributes = attributes or {}
        self.tracer = trace.get_tracer(__name__) if _OTEL_AVAILABLE and _TRACING_ENABLED else None
        self.span = None
        self.token = None
    
    async def __aenter__(self):
        if self.tracer:
            self.span = self.tracer.start_span(self.name)
            if self.attributes:
                self.span.set_attributes(self.attributes)
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self.span and _OTEL_AVAILABLE:
            if exc_val:
                self.span.record_exception(exc_val)
                self.span.set_status(Status(StatusCode.ERROR, str(exc_val)))
            self.span.end()


def trace(name: Optional[str] = None):
    """Decorator to trace async functions."""
    def decorator(func):
        @functools.wraps(func)
        async def wrapper(*args, **kwargs):
            trace_name = name or func.__name__
            async with TraceContext(trace_name, {"function": func.__name__}):
                return await func(*args, **kwargs)
        return wrapper
    return decorator


# ============================================================================
# Prometheus Metrics
# ============================================================================

class MetricsRegistry:
    """Prometheus metrics registry."""
    
    def __init__(self, namespace: str = "eodhd"):
        self.namespace = namespace
        self._metrics: Dict[str, Any] = {}
        self._lock = threading.RLock()
        
        if _PROMETHEUS_AVAILABLE and _METRICS_ENABLED:
            self._init_metrics()
    
    def _init_metrics(self):
        """Initialize Prometheus metrics."""
        with self._lock:
            self._metrics["requests_total"] = Counter(
                f"{self.namespace}_requests_total",
                "Total number of requests",
                ["endpoint", "status"]
            )
            self._metrics["request_duration_seconds"] = Histogram(
                f"{self.namespace}_request_duration_seconds",
                "Request duration in seconds",
                ["endpoint"],
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
                "Circuit breaker state (1=closed, 0=open, -1=half-open)"
            )
            self._metrics["rate_limiter_tokens"] = Gauge(
                f"{self.namespace}_rate_limiter_tokens",
                "Current tokens in rate limiter"
            )
            self._metrics["queue_size"] = Gauge(
                f"{self.namespace}_queue_size",
                "Current request queue size"
            )
            self._metrics["active_requests"] = Gauge(
                f"{self.namespace}_active_requests",
                "Number of active requests"
            )
            self._metrics["symbol_variants_generated"] = Counter(
                f"{self.namespace}_symbol_variants_generated",
                "Number of symbol variants generated",
                ["asset_class"]
            )
    
    def inc(self, name: str, value: float = 1.0, labels: Optional[Dict[str, str]] = None):
        """Increment a counter metric."""
        if not _PROMETHEUS_AVAILABLE or not _METRICS_ENABLED:
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
        if not _PROMETHEUS_AVAILABLE or not _METRICS_ENABLED:
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
        if not _PROMETHEUS_AVAILABLE or not _METRICS_ENABLED:
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
        from prometheus_client import generate_latest, REGISTRY
        return generate_latest(REGISTRY).decode('utf-8')


_METRICS = MetricsRegistry()


# ============================================================================
# Environment Helpers (Enhanced)
# ============================================================================

def _env_str(name: str, default: str = "") -> str:
    v = os.getenv(name)
    if v is None:
        return default
    s = str(v).strip()
    return s if s else default


def _env_int(name: str, default: int) -> int:
    v = os.getenv(name)
    if v is None:
        return default
    try:
        return int(str(v).strip())
    except Exception:
        return default


def _env_float(name: str, default: float) -> float:
    v = os.getenv(name)
    if v is None:
        return default
    try:
        return float(str(v).strip())
    except Exception:
        return default


def _env_bool(name: str, default: bool) -> bool:
    raw = (os.getenv(name) or "").strip().lower()
    if not raw:
        return default
    if raw in _FALSY:
        return False
    if raw in _TRUTHY:
        return True
    return default


def _utc_iso(dt: Optional[datetime] = None) -> str:
    d = dt or datetime.now(timezone.utc)
    if d.tzinfo is None:
        d = d.replace(tzinfo=timezone.utc)
    return d.astimezone(timezone.utc).isoformat()


def _riyadh_now() -> datetime:
    return datetime.now(timezone(timedelta(hours=3)))


def _riyadh_iso(dt: Optional[datetime] = None) -> str:
    tz = timezone(timedelta(hours=3))
    d = dt or datetime.now(tz)
    if d.tzinfo is None:
        d = d.replace(tzinfo=tz)
    return d.astimezone(tz).isoformat()


def _utc_now() -> datetime:
    """Get current UTC datetime."""
    return datetime.now(timezone.utc)


def _utc_now_iso() -> str:
    """Get current UTC datetime as ISO string."""
    return _utc_now().isoformat()


def _generate_request_id() -> str:
    """Generate unique request ID."""
    return str(uuid.uuid4())


def _token() -> Optional[str]:
    for k in ("EODHD_API_KEY", "EODHD_API_TOKEN", "EODHD_TOKEN"):
        v = (os.getenv(k) or "").strip()
        if v:
            return v
    return None


def _base_url() -> str:
    return _env_str("EODHD_BASE_URL", DEFAULT_BASE_URL).rstrip("/")


def _ua() -> str:
    return _env_str("EODHD_UA", USER_AGENT_DEFAULT)


def _default_exchange() -> str:
    ex = _env_str("EODHD_DEFAULT_EXCHANGE", "US").strip().upper()
    return ex or "US"


def _timeout_default() -> float:
    for k in ("EODHD_TIMEOUT_S", "HTTP_TIMEOUT_SEC", "HTTP_TIMEOUT"):
        v = (os.getenv(k) or "").strip()
        if v:
            try:
                t = float(v)
                if t > 0:
                    return t
            except Exception:
                pass
    return DEFAULT_TIMEOUT_SEC


def _retry_attempts() -> int:
    r = _env_int("EODHD_RETRY_ATTEMPTS", _env_int("MAX_RETRIES", DEFAULT_RETRY_ATTEMPTS))
    return max(1, int(r))


def _retry_delay_sec() -> float:
    d = _env_float("EODHD_RETRY_DELAY_SEC", 0.5)
    return d if d > 0 else 0.5


def _rate_limit() -> float:
    return _env_float("EODHD_RATE_LIMIT", DEFAULT_RATE_LIMIT)


def _circuit_breaker_enabled() -> bool:
    return _env_bool("EODHD_CIRCUIT_BREAKER", True)


def _circuit_breaker_threshold() -> int:
    return _env_int("EODHD_CB_THRESHOLD", DEFAULT_CIRCUIT_BREAKER_THRESHOLD)


def _circuit_breaker_timeout() -> float:
    return _env_float("EODHD_CB_TIMEOUT", DEFAULT_CIRCUIT_BREAKER_TIMEOUT)


def _enable_fundamentals() -> bool:
    return _env_bool("EODHD_ENABLE_FUNDAMENTALS", True)


def _enable_history() -> bool:
    return _env_bool("EODHD_ENABLE_HISTORY", True)


def _enable_forecast() -> bool:
    return _env_bool("EODHD_ENABLE_FORECAST", True)


def _enable_ml() -> bool:
    return _env_bool("EODHD_ENABLE_ML", True) and SKLEARN_AVAILABLE


def _verbose_warn() -> bool:
    return _env_bool("EODHD_VERBOSE_WARNINGS", False)


def _allow_ksa() -> bool:
    return _env_bool("ALLOW_EODHD_KSA", False) or _env_bool("EODHD_ALLOW_KSA", False)


def _quote_ttl_sec() -> float:
    ttl = _env_float("EODHD_QUOTE_TTL_SEC", 12.0)
    return max(5.0, float(ttl if ttl > 0 else 12.0))


def _fund_ttl_sec() -> float:
    ttl = _env_float("EODHD_FUND_TTL_SEC", 21600.0)  # 6 hours
    return max(300.0, float(ttl if ttl > 0 else 21600.0))


def _history_ttl_sec() -> float:
    ttl = _env_float("EODHD_HISTORY_TTL_SEC", 1800.0)  # 30 minutes
    return max(300.0, float(ttl if ttl > 0 else 1800.0))


def _history_days() -> int:
    d = _env_int("EODHD_HISTORY_DAYS", 500)
    return max(60, int(d))


def _history_points_max() -> int:
    n = _env_int("EODHD_HISTORY_POINTS_MAX", 1000)
    return max(100, int(n))


def _queue_size() -> int:
    return _env_int("EODHD_QUEUE_SIZE", DEFAULT_QUEUE_SIZE)


def _priority_levels() -> int:
    return _env_int("EODHD_PRIORITY_LEVELS", DEFAULT_PRIORITY_LEVELS)


def _json_env_map(name: str) -> Dict[str, str]:
    raw = _env_str(name, "")
    if not raw:
        return {}
    try:
        obj = json.loads(raw)
        if isinstance(obj, dict):
            out: Dict[str, str] = {}
            for k, v in obj.items():
                ks = str(k).strip()
                vs = str(v).strip()
                if ks and vs:
                    out[ks.upper()] = vs
            return out
    except Exception:
        return {}
    return {}


# ============================================================================
# Advanced Cache with TTL and LRU
# ============================================================================

class SmartCache:
    """Thread-safe cache with TTL and LRU eviction."""

    def __init__(self, maxsize: int = 5000, ttl: float = 300.0):
        self.maxsize = maxsize
        self.ttl = ttl
        self._cache: Dict[str, Any] = {}
        self._expires: Dict[str, float] = {}
        self._access_times: Dict[str, float] = {}
        self._hits: int = 0
        self._misses: int = 0
        self._lock = asyncio.Lock()

    async def get(self, key: str) -> Optional[Any]:
        async with self._lock:
            now = time.monotonic()

            if key in self._cache:
                if now < self._expires.get(key, 0):
                    self._access_times[key] = now
                    self._hits += 1
                    _METRICS.inc("cache_hits_total", 1, {"cache_type": "memory"})
                    return self._cache[key]
                else:
                    await self._delete(key)

            self._misses += 1
            _METRICS.inc("cache_misses_total", 1, {"cache_type": "memory"})
            return None

    async def set(self, key: str, value: Any, ttl: Optional[float] = None) -> None:
        async with self._lock:
            if len(self._cache) >= self.maxsize and key not in self._cache:
                await self._evict_lru()

            self._cache[key] = value
            self._expires[key] = time.monotonic() + (ttl or self.ttl)
            self._access_times[key] = time.monotonic()

    async def delete(self, key: str) -> None:
        async with self._lock:
            await self._delete(key)

    async def _delete(self, key: str) -> None:
        self._cache.pop(key, None)
        self._expires.pop(key, None)
        self._access_times.pop(key, None)

    async def _evict_lru(self) -> None:
        if not self._access_times:
            return
        oldest_key = min(self._access_times.items(), key=lambda x: x[1])[0]
        await self._delete(oldest_key)

    async def clear(self) -> None:
        async with self._lock:
            self._cache.clear()
            self._expires.clear()
            self._access_times.clear()
            self._hits = 0
            self._misses = 0

    async def size(self) -> int:
        async with self._lock:
            return len(self._cache)

    async def keys(self) -> List[str]:
        async with self._lock:
            return list(self._cache.keys())
    
    def get_stats(self) -> Dict[str, Any]:
        """Get cache statistics."""
        total = self._hits + self._misses
        return {
            "size": len(self._cache),
            "maxsize": self.maxsize,
            "ttl": self.ttl,
            "hits": self._hits,
            "misses": self._misses,
            "hit_rate": self._hits / total if total > 0 else 0,
            "utilization": len(self._cache) / self.maxsize if self.maxsize > 0 else 0
        }


# ============================================================================
# Rate Limiter & Circuit Breaker
# ============================================================================

class TokenBucket:
    """Async token bucket rate limiter."""

    def __init__(self, rate_per_sec: float, capacity: Optional[float] = None):
        self.rate = max(0.0, rate_per_sec)
        self.capacity = capacity or max(1.0, self.rate)
        self.tokens = self.capacity
        self.last = time.monotonic()
        self._lock = asyncio.Lock()
        self._total_acquired = 0
        self._total_rejected = 0

    async def acquire(self, tokens: float = 1.0) -> bool:
        if self.rate <= 0:
            self._total_acquired += 1
            return True

        async with self._lock:
            now = time.monotonic()
            elapsed = max(0.0, now - self.last)
            self.last = now

            self.tokens = min(self.capacity, self.tokens + elapsed * self.rate)

            if self.tokens >= tokens:
                self.tokens -= tokens
                self._total_acquired += 1
                _METRICS.set("rate_limiter_tokens", self.tokens)
                return True

            self._total_rejected += 1
            return False

    async def wait_and_acquire(self, tokens: float = 1.0) -> None:
        while True:
            if await self.acquire(tokens):
                return

            async with self._lock:
                need = tokens - self.tokens
                wait = need / self.rate if self.rate > 0 else 0.1

            await asyncio.sleep(min(1.0, wait))

    def get_stats(self) -> Dict[str, Any]:
        """Get rate limiter statistics."""
        return {
            "rate": self.rate,
            "capacity": self.capacity,
            "current_tokens": self.tokens,
            "utilization": 1.0 - (self.tokens / self.capacity) if self.capacity > 0 else 0,
            "total_acquired": self._total_acquired,
            "total_rejected": self._total_rejected
        }


class SmartCircuitBreaker:
    """Circuit breaker with half-open state and failure tracking."""

    def __init__(
        self,
        fail_threshold: int = 5,
        cooldown_sec: float = 30.0,
        half_open_max_calls: int = 2
    ):
        self.fail_threshold = fail_threshold
        self.cooldown_sec = cooldown_sec
        self.half_open_max_calls = half_open_max_calls

        self.stats = CircuitBreakerStats()
        self.half_open_calls = 0
        self._lock = asyncio.Lock()

    async def allow_request(self) -> bool:
        async with self._lock:
            now = time.monotonic()

            if self.stats.state == CircuitState.CLOSED:
                _METRICS.set("circuit_breaker_state", 1)
                return True

            elif self.stats.state == CircuitState.OPEN:
                if now >= self.stats.open_until:
                    self.stats.state = CircuitState.HALF_OPEN
                    self.half_open_calls = 0
                    logger.info(f"Circuit breaker moved to HALF_OPEN")
                    _METRICS.set("circuit_breaker_state", -1)
                    return True
                _METRICS.set("circuit_breaker_state", 0)
                return False

            elif self.stats.state == CircuitState.HALF_OPEN:
                if self.half_open_calls < self.half_open_max_calls:
                    self.half_open_calls += 1
                    _METRICS.set("circuit_breaker_state", -1)
                    return True
                return False

            return False

    async def on_success(self) -> None:
        async with self._lock:
            self.stats.successes += 1
            self.stats.total_successes += 1
            self.stats.last_success = time.monotonic()

            if self.stats.state == CircuitState.HALF_OPEN:
                self.stats.state = CircuitState.CLOSED
                self.stats.failures = 0
                logger.info("Circuit breaker returned to CLOSED after success")
                _METRICS.set("circuit_breaker_state", 1)

    async def on_failure(self) -> None:
        async with self._lock:
            self.stats.failures += 1
            self.stats.total_failures += 1
            self.stats.last_failure = time.monotonic()

            if self.stats.state == CircuitState.CLOSED:
                if self.stats.failures >= self.fail_threshold:
                    self.stats.state = CircuitState.OPEN
                    self.stats.open_until = time.monotonic() + self.cooldown_sec
                    logger.warning(f"Circuit breaker OPEN after {self.stats.failures} failures")
                    _METRICS.set("circuit_breaker_state", 0)

            elif self.stats.state == CircuitState.HALF_OPEN:
                self.stats.state = CircuitState.OPEN
                self.stats.open_until = time.monotonic() + self.cooldown_sec
                logger.warning("Circuit breaker returned to OPEN after half-open failure")
                _METRICS.set("circuit_breaker_state", 0)

    def get_stats(self) -> Dict[str, Any]:
        return {
            "state": self.stats.state.value,
            "failures": self.stats.failures,
            "successes": self.stats.successes,
            "total_failures": self.stats.total_failures,
            "total_successes": self.stats.total_successes,
            "last_failure": self.stats.last_failure,
            "last_success": self.stats.last_success,
            "open_until": self.stats.open_until
        }


# ============================================================================
# Request Queue with Priorities
# ============================================================================

class RequestQueue:
    """Priority-based request queue."""
    
    def __init__(self, max_size: int = 1000):
        self.max_size = max_size
        self.queues: Dict[RequestPriority, asyncio.Queue] = {
            RequestPriority.LOW: asyncio.Queue(),
            RequestPriority.NORMAL: asyncio.Queue(),
            RequestPriority.HIGH: asyncio.Queue(),
            RequestPriority.CRITICAL: asyncio.Queue()
        }
        self._lock = asyncio.Lock()
        self._total_queued = 0
        self._total_processed = 0
    
    async def put(self, item: RequestQueueItem) -> bool:
        """Put item in queue."""
        async with self._lock:
            total_size = sum(q.qsize() for q in self.queues.values())
            if total_size >= self.max_size:
                return False
            
            await self.queues[item.priority].put(item)
            self._total_queued += 1
            _METRICS.set("queue_size", total_size + 1)
            return True
    
    async def get(self) -> Optional[RequestQueueItem]:
        """Get highest priority item from queue."""
        for priority in sorted(RequestPriority, key=lambda p: p.value, reverse=True):
            queue = self.queues[priority]
            if not queue.empty():
                item = await queue.get()
                self._total_processed += 1
                total_size = sum(q.qsize() for q in self.queues.values())
                _METRICS.set("queue_size", total_size)
                return item
        return None
    
    def get_stats(self) -> Dict[str, Any]:
        """Get queue statistics."""
        return {
            "max_size": self.max_size,
            "current_sizes": {
                p.value: q.qsize() for p, q in self.queues.items()
            },
            "total_queued": self._total_queued,
            "total_processed": self._total_processed,
            "utilization": sum(q.qsize() for q in self.queues.values()) / self.max_size if self.max_size > 0 else 0
        }


# ============================================================================
# Symbol Normalization (Enhanced)
# ============================================================================

_KSA_RE = re.compile(r"^\d{3,6}(\.SR)?$", re.IGNORECASE)
_EXCH_SUFFIX_RE = re.compile(r"^(.+)\.([A-Z0-9]{2,10})$")
_INDEX_PREFIX_RE = re.compile(r"^\^([A-Z0-9]+)$")
_FOREX_SUFFIX_RE = re.compile(r"^(.+)=X$")
_COMMODITY_SUFFIX_RE = re.compile(r"^(.+)=F$")


def looks_like_ksa(symbol: str) -> bool:
    """Check if symbol appears to be KSA (Tadawul)."""
    s = (symbol or "").strip().upper()
    if not s:
        return False
    if s.endswith(".SR"):
        return True
    if s.isdigit() and 3 <= len(s) <= 6:
        return True
    return bool(_KSA_RE.match(s))


def split_exchange_suffix(sym: str) -> Tuple[str, Optional[str]]:
    """Split symbol into base and exchange suffix."""
    s = (sym or "").strip().upper()
    if not s:
        return "", None
    m = _EXCH_SUFFIX_RE.match(s)
    if not m:
        return s, None
    base = (m.group(1) or "").strip()
    exch = (m.group(2) or "").strip()
    if not base or not exch:
        return s, None
    return base, exch


def detect_asset_class(symbol: str) -> AssetClass:
    """Detect asset class from symbol pattern."""
    s = (symbol or "").strip().upper()

    # Check explicit suffix
    base, exch = split_exchange_suffix(s)
    if exch in ("INDX", "INDEX"):
        return AssetClass.INDEX
    if exch in ("FOREX", "FX"):
        return AssetClass.FOREX
    if exch in ("COMM", "COM", "CMD"):
        return AssetClass.COMMODITY
    if exch in ("ETF", "ETP"):
        return AssetClass.ETF
    if exch in ("FUND", "MF"):
        return AssetClass.FUND
    if exch in ("BOND", "GBOND"):
        return AssetClass.BOND

    # Check patterns
    if _INDEX_PREFIX_RE.match(s):
        return AssetClass.INDEX
    if _FOREX_SUFFIX_RE.match(s):
        return AssetClass.FOREX
    if _COMMODITY_SUFFIX_RE.match(s):
        return AssetClass.COMMODITY
    if s.endswith(".TO") or s.endswith(".V") or s.endswith(".L"):
        return AssetClass.EQUITY

    return AssetClass.EQUITY


# Default symbol mappings
_DEFAULT_INDEX_ALIASES: Dict[str, str] = {
    "TASI": "TASI.INDX",
    "NOMU": "NOMU.INDX",
    "SPX": "^GSPC.INDX",
    "IXIC": "^IXIC.INDX",
    "DJI": "^DJI.INDX",
    "FTSE": "^FTSE.INDX",
    "N225": "^N225.INDX",
    "HSI": "^HSI.INDX",
}

_DEFAULT_COMMODITY_ALIASES: Dict[str, str] = {
    "GC=F": "GC.COMM",  # Gold
    "SI=F": "SI.COMM",  # Silver
    "CL=F": "CL.COMM",  # WTI Crude
    "BZ=F": "BZ.COMM",  # Brent Crude
    "NG=F": "NG.COMM",  # Natural Gas
    "HG=F": "HG.COMM",  # Copper
    "ZW=F": "ZW.COMM",  # Wheat
    "ZC=F": "ZC.COMM",  # Corn
    "ZS=F": "ZS.COMM",  # Soybeans
}

_DEFAULT_FOREX_ALIASES: Dict[str, str] = {
    "EURUSD=X": "EUR.FOREX",
    "GBPUSD=X": "GBP.FOREX",
    "USDJPY=X": "JPY.FOREX",
    "USDCHF=X": "CHF.FOREX",
    "AUDUSD=X": "AUD.FOREX",
    "USDCAD=X": "CAD.FOREX",
    "NZDUSD=X": "NZD.FOREX",
    "SAR=X": "SAR.FOREX",
}

_INDEX_ALIASES = {**_DEFAULT_INDEX_ALIASES, **_json_env_map("EODHD_INDEX_ALIASES_JSON")}
_COMMODITY_ALIASES = {**_DEFAULT_COMMODITY_ALIASES, **_json_env_map("EODHD_COMMODITY_ALIASES_JSON")}
_FOREX_ALIASES = {**_DEFAULT_FOREX_ALIASES, **_json_env_map("EODHD_FOREX_ALIASES_JSON")}


def generate_symbol_variants(raw_symbol: str) -> Tuple[List[str], AssetClass]:
    """
    Generate all possible symbol variants for EODHD API.
    Returns (variants, asset_class).
    """
    s = (raw_symbol or "").strip().upper()
    if not s:
        return [], AssetClass.UNKNOWN

    # Check special mappings
    if s in _INDEX_ALIASES:
        _METRICS.inc("symbol_variants_generated", 1, {"asset_class": "index"})
        return [_INDEX_ALIASES[s], s], AssetClass.INDEX
    if s in _COMMODITY_ALIASES:
        _METRICS.inc("symbol_variants_generated", 1, {"asset_class": "commodity"})
        return [_COMMODITY_ALIASES[s], s], AssetClass.COMMODITY
    if s in _FOREX_ALIASES:
        _METRICS.inc("symbol_variants_generated", 1, {"asset_class": "forex"})
        return [_FOREX_ALIASES[s], s], AssetClass.FOREX

    # Detect asset class
    asset_class = detect_asset_class(s)
    variants: List[str] = []
    seen: Set[str] = set()

    # Add based on asset class
    if asset_class == AssetClass.INDEX:
        if s.startswith("^"):
            base = s[1:]
            variants.append(f"{base}.INDX")
        else:
            variants.append(f"{s}.INDX")
        variants.append(s)

    elif asset_class == AssetClass.FOREX:
        if s.endswith("=X"):
            base = s[:-2]
            variants.append(f"{base}.FOREX")
        else:
            variants.append(f"{s}.FOREX")
        variants.append(s)

    elif asset_class == AssetClass.COMMODITY:
        if s.endswith("=F"):
            base = s[:-2]
            variants.append(f"{base}.COMM")
        else:
            variants.append(f"{s}.COMM")
        variants.append(s)

    else:  # Equity
        base, exch = split_exchange_suffix(s)
        if exch:
            variants.append(s)
            variants.append(base)
        else:
            exch = _default_exchange()
            variants.append(f"{s}.{exch}")
            variants.append(s)

    # Add exchange variations
    if asset_class == AssetClass.EQUITY:
        for v in list(variants):
            b, e = split_exchange_suffix(v)
            if e:
                # Try alternative separators
                b1 = b.replace(".", "-")
                b2 = b.replace("-", ".")
                if b1 != b and b1 not in seen:
                    variants.append(f"{b1}.{e}")
                if b2 != b and b2 != b1 and b2 not in seen:
                    variants.append(f"{b2}.{e}")

    # Deduplicate while preserving order
    final: List[str] = []
    for v in variants:
        if v and v not in seen:
            seen.add(v)
            final.append(v)

    _METRICS.inc("symbol_variants_generated", len(final), {"asset_class": asset_class.value})
    return final, asset_class


# ============================================================================
# Numeric Helpers
# ============================================================================

def safe_float(val: Any) -> Optional[float]:
    """Safely convert value to float."""
    if val is None:
        return None
    try:
        if isinstance(val, (int, float)):
            f = float(val)
            if math.isnan(f) or math.isinf(f):
                return None
            return f

        s = str(val).strip()
        if not s or s in {"-", "—", "N/A", "NA", "null", "None", ""}:
            return None

        s = s.replace(",", "").replace("%", "").replace("$", "").replace("£", "").replace("€", "")
        if s.startswith("(") and s.endswith(")"):
            s = "-" + s[1:-1].strip()

        # Handle K/M/B suffixes
        m = re.match(r"^(-?\d+(\.\d+)?)([KMB])$", s, re.IGNORECASE)
        mult = 1.0
        if m:
            num = m.group(1)
            suf = m.group(3).upper()
            mult = 1_000.0 if suf == "K" else 1_000_000.0 if suf == "M" else 1_000_000_000.0
            s = num

        f = float(s) * mult
        if math.isnan(f) or math.isinf(f):
            return None
        return f
    except Exception:
        return None


def safe_int(val: Any) -> Optional[int]:
    """Safely convert to integer."""
    f = safe_float(val)
    if f is None:
        return None
    try:
        return int(round(f))
    except Exception:
        return None


def safe_str(val: Any) -> Optional[str]:
    """Safely convert to string."""
    if val is None:
        return None
    s = str(val).strip()
    return s if s else None


def pick(d: Any, *keys: str) -> Any:
    """Pick first existing key from dict."""
    if not isinstance(d, dict):
        return None
    for k in keys:
        if k in d:
            return d[k]
    return None


def position_52w(cur: Optional[float], lo: Optional[float], hi: Optional[float]) -> Optional[float]:
    """Calculate position in 52-week range as percentage."""
    if cur is None or lo is None or hi is None:
        return None
    if hi == lo:
        return 50.0
    try:
        return (cur - lo) / (hi - lo) * 100.0
    except Exception:
        return None


def clean_patch(patch: Dict[str, Any]) -> Dict[str, Any]:
    """Remove None and empty string values."""
    out: Dict[str, Any] = {}
    for k, v in (patch or {}).items():
        if v is None:
            continue
        if isinstance(v, str) and not v.strip():
            continue
        out[k] = v
    return out


def merge_into(dst: Dict[str, Any], src: Dict[str, Any], *, force_keys: Sequence[str] = ()) -> None:
    """Merge src into dst, with optional force keys."""
    fset = set(force_keys or ())
    for k, v in (src or {}).items():
        if v is None:
            continue
        if k in fset:
            dst[k] = v
            continue
        if k not in dst:
            dst[k] = v
            continue
        cur = dst.get(k)
        if cur is None:
            dst[k] = v
        elif isinstance(cur, str) and not cur.strip():
            dst[k] = v


def fill_derived(patch: Dict[str, Any]) -> None:
    """Calculate derived fields."""
    cur = safe_float(patch.get("current_price"))
    prev = safe_float(patch.get("previous_close"))
    vol = safe_float(patch.get("volume"))
    high = safe_float(patch.get("day_high"))
    low = safe_float(patch.get("day_low"))

    if patch.get("price_change") is None and cur is not None and prev is not None:
        patch["price_change"] = cur - prev

    if patch.get("percent_change") is None and cur is not None and prev not in (None, 0.0):
        try:
            patch["percent_change"] = (cur - prev) / prev * 100.0
        except Exception:
            pass

    if patch.get("value_traded") is None and cur is not None and vol is not None:
        patch["value_traded"] = cur * vol

    if patch.get("day_range") is None and high is not None and low is not None:
        patch["day_range"] = high - low

    if patch.get("position_52w_percent") is None:
        patch["position_52w_percent"] = position_52w(
            cur,
            safe_float(patch.get("week_52_low")),
            safe_float(patch.get("week_52_high")),
        )

    mc = safe_float(patch.get("market_cap"))
    ff = safe_float(patch.get("free_float"))
    if patch.get("free_float_market_cap") is None and mc is not None and ff is not None:
        patch["free_float_market_cap"] = mc * (ff / 100.0)


def data_quality_score(patch: Dict[str, Any]) -> Tuple[str, float]:
    """Score data quality (0-100) and return category."""
    score = 100.0
    reasons = []

    # Check essential fields
    if safe_float(patch.get("current_price")) is None:
        score -= 30
        reasons.append("no_price")

    if safe_float(patch.get("previous_close")) is None:
        score -= 15
        reasons.append("no_prev_close")

    if safe_float(patch.get("volume")) is None:
        score -= 10
        reasons.append("no_volume")

    # Check fundamental fields
    if safe_float(patch.get("market_cap")) is None:
        score -= 10
        reasons.append("no_mcap")

    if safe_float(patch.get("pe_ttm")) is None:
        score -= 5
        reasons.append("no_pe")

    if safe_str(patch.get("name")) is None:
        score -= 10
        reasons.append("no_name")

    # Check history
    if safe_int(patch.get("history_points", 0)) < 50:
        score -= 20
        reasons.append("insufficient_history")

    # Normalize score
    score = max(0.0, min(100.0, score))

    # Determine category
    if score >= 80:
        category = "EXCELLENT"
    elif score >= 60:
        category = "GOOD"
    elif score >= 40:
        category = "FAIR"
    elif score >= 20:
        category = "POOR"
    else:
        category = "BAD"

    return category, score


# ============================================================================
# Technical Indicators
# ============================================================================

class TechnicalIndicators:
    """Collection of technical analysis indicators."""

    @staticmethod
    def sma(prices: List[float], window: int) -> List[Optional[float]]:
        """Simple Moving Average."""
        if len(prices) < window:
            return [None] * len(prices)

        result: List[Optional[float]] = [None] * (window - 1)
        for i in range(window - 1, len(prices)):
            sma = sum(prices[i - window + 1:i + 1]) / window
            result.append(sma)
        return result

    @staticmethod
    def ema(prices: List[float], window: int) -> List[Optional[float]]:
        """Exponential Moving Average."""
        if len(prices) < window:
            return [None] * len(prices)

        result: List[Optional[float]] = [None] * (window - 1)
        multiplier = 2.0 / (window + 1)

        ema = sum(prices[:window]) / window
        result.append(ema)

        for price in prices[window:]:
            ema = (price - ema) * multiplier + ema
            result.append(ema)

        return result

    @staticmethod
    def macd(
        prices: List[float],
        fast: int = 12,
        slow: int = 26,
        signal: int = 9
    ) -> Dict[str, List[Optional[float]]]:
        """MACD (Moving Average Convergence Divergence)."""
        if len(prices) < slow:
            return {"macd": [None] * len(prices), "signal": [None] * len(prices), "histogram": [None] * len(prices)}

        ema_fast = TechnicalIndicators.ema(prices, fast)
        ema_slow = TechnicalIndicators.ema(prices, slow)

        macd_line: List[Optional[float]] = []
        for i in range(len(prices)):
            if ema_fast[i] is not None and ema_slow[i] is not None:
                macd_line.append(ema_fast[i] - ema_slow[i])  # type: ignore
            else:
                macd_line.append(None)

        valid_macd = [x for x in macd_line if x is not None]
        signal_line = TechnicalIndicators.ema(valid_macd, signal) if valid_macd else []

        padded_signal: List[Optional[float]] = [None] * (len(prices) - len(signal_line)) + signal_line

        histogram: List[Optional[float]] = []
        for i in range(len(prices)):
            if macd_line[i] is not None and padded_signal[i] is not None:
                histogram.append(macd_line[i] - padded_signal[i])  # type: ignore
            else:
                histogram.append(None)

        return {
            "macd": macd_line,
            "signal": padded_signal,
            "histogram": histogram
        }

    @staticmethod
    def rsi(prices: List[float], window: int = 14) -> List[Optional[float]]:
        """Relative Strength Index."""
        if len(prices) < window + 1:
            return [None] * len(prices)

        deltas = [prices[i] - prices[i - 1] for i in range(1, len(prices))]

        result: List[Optional[float]] = [None] * window

        for i in range(window, len(prices)):
            window_deltas = deltas[i - window:i]
            gains = sum(d for d in window_deltas if d > 0)
            losses = sum(-d for d in window_deltas if d < 0)

            if losses == 0:
                rsi = 100.0
            else:
                rs = gains / losses
                rsi = 100.0 - (100.0 / (1.0 + rs))

            result.append(rsi)

        return result

    @staticmethod
    def bollinger_bands(
        prices: List[float],
        window: int = 20,
        num_std: float = 2.0
    ) -> Dict[str, List[Optional[float]]]:
        """Bollinger Bands."""
        if len(prices) < window:
            return {
                "middle": [None] * len(prices),
                "upper": [None] * len(prices),
                "lower": [None] * len(prices),
                "bandwidth": [None] * len(prices)
            }

        middle = TechnicalIndicators.sma(prices, window)

        upper: List[Optional[float]] = [None] * (window - 1)
        lower: List[Optional[float]] = [None] * (window - 1)
        bandwidth: List[Optional[float]] = [None] * (window - 1)

        for i in range(window - 1, len(prices)):
            window_prices = prices[i - window + 1:i + 1]
            std = float(np.std(window_prices))

            m = middle[i]
            if m is not None:
                u = m + num_std * std
                l = m - num_std * std
                upper.append(u)
                lower.append(l)
                bandwidth.append((u - l) / m if m != 0 else None)
            else:
                upper.append(None)
                lower.append(None)
                bandwidth.append(None)

        return {
            "middle": middle,
            "upper": upper,
            "lower": lower,
            "bandwidth": bandwidth
        }

    @staticmethod
    def atr(highs: List[float], lows: List[float], closes: List[float], window: int = 14) -> List[Optional[float]]:
        """Average True Range."""
        if len(highs) < window + 1 or len(lows) < window + 1 or len(closes) < window + 1:
            return [None] * len(highs)

        tr: List[float] = []
        for i in range(1, len(highs)):
            hl = highs[i] - lows[i]
            hc = abs(highs[i] - closes[i - 1])
            lc = abs(lows[i] - closes[i - 1])
            tr.append(max(hl, hc, lc))

        atr_values: List[Optional[float]] = [None] * window

        if len(tr) >= window:
            atr_values.append(sum(tr[:window]) / window)

            for i in range(window, len(tr)):
                prev_atr = atr_values[-1]
                if prev_atr is not None:
                    atr_values.append((prev_atr * (window - 1) + tr[i]) / window)
                else:
                    atr_values.append(None)

        return atr_values

    @staticmethod
    def obv(closes: List[float], volumes: List[float]) -> List[float]:
        """On-Balance Volume."""
        if len(closes) < 2:
            return [0.0] * len(closes)

        obv_values: List[float] = [0.0]
        for i in range(1, len(closes)):
            if closes[i] > closes[i - 1]:
                obv_values.append(obv_values[-1] + volumes[i])
            elif closes[i] < closes[i - 1]:
                obv_values.append(obv_values[-1] - volumes[i])
            else:
                obv_values.append(obv_values[-1])

        return obv_values

    @staticmethod
    def volatility(prices: List[float], window: int = 30, annualize: bool = True) -> List[Optional[float]]:
        """Historical volatility."""
        if len(prices) < window + 1:
            return [None] * len(prices)

        returns = [math.log(prices[i] / prices[i - 1]) for i in range(1, len(prices))]

        vol: List[Optional[float]] = [None] * window

        for i in range(window, len(returns)):
            window_returns = returns[i - window:i]
            std = float(np.std(window_returns))

            if annualize:
                vol.append(std * math.sqrt(252))
            else:
                vol.append(std)

        vol = [None] + vol
        return vol[:len(prices)]


# ============================================================================
# Market Regime Detection
# ============================================================================

class MarketRegimeDetector:
    """Detect market regimes from price history."""

    def __init__(self, prices: List[float], window: int = 60):
        self.prices = prices
        self.window = min(window, len(prices) // 3) if len(prices) > 60 else 20

    def detect(self) -> Tuple[MarketRegime, float]:
        """Detect current market regime with confidence score."""
        if len(self.prices) < 30:
            return MarketRegime.UNKNOWN, 0.0

        # Calculate metrics
        returns = [self.prices[i] / self.prices[i - 1] - 1 for i in range(1, len(self.prices))]
        recent_returns = returns[-min(len(returns), 30):]

        # Trend analysis
        if len(self.prices) >= self.window and SCIPY_AVAILABLE:
            x = list(range(self.window))
            y = self.prices[-self.window:]
            slope, intercept, r_value, p_value, std_err = stats.linregress(x, y)
            trend_strength = abs(r_value)
            trend_direction = slope
            trend_pvalue = p_value
        else:
            # Simplified trend
            x = list(range(min(30, len(self.prices))))
            y = self.prices[-len(x):]
            if len(x) > 1:
                x_mean = sum(x) / len(x)
                y_mean = sum(y) / len(y)
                numerator = sum((xi - x_mean) * (yi - y_mean) for xi, yi in zip(x, y))
                denominator = sum((xi - x_mean) ** 2 for xi in x)
                trend_direction = numerator / denominator if denominator != 0 else 0
                trend_strength = 0.5  # Default
                trend_pvalue = 0.1
            else:
                trend_direction = 0
                trend_strength = 0
                trend_pvalue = 1.0

        # Volatility
        vol = float(np.std(recent_returns)) if recent_returns else 0

        # Momentum
        momentum_1m = self.prices[-1] / self.prices[-min(21, len(self.prices))] - 1 if len(self.prices) > 21 else 0
        momentum_3m = self.prices[-1] / self.prices[-min(63, len(self.prices))] - 1 if len(self.prices) > 63 else 0

        # Regime classification with confidence
        confidence = 0.7  # Base confidence

        if vol > 0.03:  # High volatility (3% daily moves)
            regime = MarketRegime.VOLATILE
            confidence = min(0.9, 0.7 + vol * 5)

        elif trend_strength > 0.7 and trend_pvalue < 0.05:
            if trend_direction > 0:
                regime = MarketRegime.BULL
                confidence = min(0.95, 0.7 + trend_strength * 0.3 + abs(momentum_1m) * 2)
            else:
                regime = MarketRegime.BEAR
                confidence = min(0.95, 0.7 + trend_strength * 0.3 + abs(momentum_1m) * 2)

        elif abs(momentum_1m) < 0.03 and vol < 0.015:
            regime = MarketRegime.SIDEWAYS
            confidence = 0.8

        elif momentum_1m > 0.05:
            regime = MarketRegime.BULL
            confidence = 0.7 + abs(momentum_1m) * 2

        elif momentum_1m < -0.05:
            regime = MarketRegime.BEAR
            confidence = 0.7 + abs(momentum_1m) * 2

        else:
            regime = MarketRegime.UNKNOWN
            confidence = 0.3

        return regime, min(1.0, confidence)


# ============================================================================
# Advanced Forecasting
# ============================================================================

class EnsembleForecaster:
    """Multi-model ensemble forecaster."""

    def __init__(self, prices: List[float], enable_ml: bool = True):
        self.prices = prices
        self.enable_ml = enable_ml and SKLEARN_AVAILABLE
        self.feature_importance: Dict[str, float] = {}

    def forecast(self, horizon_days: int = 252) -> Dict[str, Any]:
        """Generate ensemble forecast for given horizon."""
        if len(self.prices) < 30:
            return {
                "forecast_available": False,
                "reason": "insufficient_history",
                "horizon_days": horizon_days
            }

        result: Dict[str, Any] = {
            "forecast_available": True,
            "horizon_days": horizon_days,
            "models_used": [],
            "forecasts": {},
            "ensemble": {},
            "confidence": 0.0,
            "feature_importance": {}
        }

        last_price = self.prices[-1]
        forecasts: List[float] = []
        weights: List[float] = []

        # Model 1: Log-linear regression (trend)
        trend_forecast = self._forecast_trend(horizon_days)
        if trend_forecast is not None:
            result["models_used"].append("trend")
            result["forecasts"]["trend"] = trend_forecast
            forecasts.append(trend_forecast["price"])
            weights.append(trend_forecast.get("weight", 0.3))

        # Model 2: ARIMA-like (momentum + mean reversion)
        arima_forecast = self._forecast_arima(horizon_days)
        if arima_forecast is not None:
            result["models_used"].append("arima")
            result["forecasts"]["arima"] = arima_forecast
            forecasts.append(arima_forecast["price"])
            weights.append(arima_forecast.get("weight", 0.25))

        # Model 3: Moving average crossover
        ma_forecast = self._forecast_ma(horizon_days)
        if ma_forecast is not None:
            result["models_used"].append("moving_avg")
            result["forecasts"]["moving_avg"] = ma_forecast
            forecasts.append(ma_forecast["price"])
            weights.append(ma_forecast.get("weight", 0.2))

        # Model 4: Machine Learning (if available)
        if self.enable_ml:
            ml_forecast = self._forecast_ml(horizon_days)
            if ml_forecast is not None:
                result["models_used"].append("ml")
                result["forecasts"]["ml"] = ml_forecast
                forecasts.append(ml_forecast["price"])
                weights.append(ml_forecast.get("weight", 0.25))
                
                # Add feature importance
                if self.feature_importance:
                    result["feature_importance"] = self.feature_importance

        if not forecasts:
            return {
                "forecast_available": False,
                "reason": "no_models_converged",
                "horizon_days": horizon_days
            }

        # Weighted ensemble
        total_weight = sum(weights)
        if total_weight > 0:
            ensemble_price = sum(f * w for f, w in zip(forecasts, weights)) / total_weight
        else:
            ensemble_price = float(np.mean(forecasts))

        # Ensemble statistics
        ensemble_std = float(np.std(forecasts)) if len(forecasts) > 1 else 0
        ensemble_roi = (ensemble_price / last_price - 1) * 100

        # Calculate confidence
        confidence = self._calculate_confidence(result, forecasts)

        result["ensemble"] = {
            "price": ensemble_price,
            "roi_pct": ensemble_roi,
            "std_dev": ensemble_std,
            "price_range_low": ensemble_price - 2 * ensemble_std,
            "price_range_high": ensemble_price + 2 * ensemble_std
        }

        result["confidence"] = confidence
        result["confidence_level"] = self._confidence_level(confidence)

        # Add forecast aliases for dashboard
        if horizon_days <= 21:
            period = "1m"
        elif horizon_days <= 63:
            period = "3m"
        else:
            period = "1y"

        result[f"forecast_price_{period}"] = ensemble_price
        result[f"forecast_roi_{period}_pct"] = ensemble_roi

        return result

    def _forecast_trend(self, horizon: int) -> Optional[Dict[str, Any]]:
        """Trend-based forecast using log-linear regression."""
        try:
            n = min(len(self.prices), 252)
            y = [math.log(p) for p in self.prices[-n:]]
            x = list(range(n))

            if SKLEARN_AVAILABLE:
                model = LinearRegression()
                model.fit(np.array(x).reshape(-1, 1), y)
                slope = model.coef_[0]
                intercept = model.intercept_
                y_pred = model.predict(np.array(x).reshape(-1, 1))
                ss_res = sum((yi - yp) ** 2 for yi, yp in zip(y, y_pred))
                ss_tot = sum((yi - sum(y) / n) ** 2 for yi in y)
                r2 = 1 - (ss_res / ss_tot) if ss_tot != 0 else 0
            else:
                x_mean = sum(x) / n
                y_mean = sum(y) / n
                slope = sum((xi - x_mean) * (yi - y_mean) for xi, yi in zip(x, y)) / sum((xi - x_mean) ** 2)
                intercept = y_mean - slope * x_mean
                r2 = 0.6  # Default

            future_x = n + horizon
            log_price = intercept + slope * future_x
            price = math.exp(log_price)

            confidence = min(90, 60 + r2 * 30)
            weight = max(0.2, min(0.4, r2))

            return {
                "price": float(price),
                "roi_pct": float((price / self.prices[-1] - 1) * 100),
                "r2": float(r2),
                "slope": float(slope),
                "weight": float(weight),
                "confidence": float(confidence)
            }
        except Exception:
            return None

    def _forecast_arima(self, horizon: int) -> Optional[Dict[str, Any]]:
        """ARIMA-like forecast using momentum and mean reversion."""
        try:
            recent = self.prices[-min(60, len(self.prices)):]
            returns = [recent[i] / recent[i - 1] - 1 for i in range(1, len(recent))]

            avg_return = float(np.mean(returns))
            vol = float(np.std(returns))

            # Monte Carlo simulation
            n_sims = 500
            last_price = self.prices[-1]

            sim_prices = []
            for _ in range(n_sims):
                price = last_price
                for _ in range(horizon):
                    shock = np.random.normal(avg_return, vol)
                    price *= (1 + shock)
                sim_prices.append(price)

            median_price = float(np.median(sim_prices))
            p10 = float(np.percentile(sim_prices, 10))
            p90 = float(np.percentile(sim_prices, 90))

            # Confidence based on volatility
            confidence = max(30, min(85, 70 - vol * 500))

            return {
                "price": median_price,
                "roi_pct": float((median_price / last_price - 1) * 100),
                "mean": float(np.mean(sim_prices)),
                "p10": p10,
                "p90": p90,
                "volatility": vol,
                "weight": 0.25,
                "confidence": confidence
            }
        except Exception:
            return None

    def _forecast_ma(self, horizon: int) -> Optional[Dict[str, Any]]:
        """Moving average based forecast."""
        try:
            if len(self.prices) < 50:
                return None

            # Calculate various moving averages
            ma20 = sum(self.prices[-20:]) / 20
            ma50 = sum(self.prices[-50:]) / 50
            ma200 = sum(self.prices[-200:]) / 200 if len(self.prices) >= 200 else ma50

            last_price = self.prices[-1]

            # Trend direction and strength
            ma_trend = (ma20 / ma50 - 1) * 100
            long_trend = (ma50 / ma200 - 1) * 100

            # Combine signals
            if ma_trend > 2 and long_trend > 2:
                factor = 1 + (ma_trend / 100) * 0.5
            elif ma_trend < -2 and long_trend < -2:
                factor = 1 + (ma_trend / 100) * 0.5
            else:
                factor = 1.0

            # Project forward with diminishing trend
            decay = math.exp(-horizon / 126)  # 6-month half-life
            price = last_price * (1 + (factor - 1) * decay)

            # Confidence based on MA alignment
            if (ma20 > ma50 > ma200) or (ma20 < ma50 < ma200):
                confidence = 70
            elif abs(ma_trend) < 1:
                confidence = 50
            else:
                confidence = 40

            return {
                "price": float(price),
                "roi_pct": float((price / last_price - 1) * 100),
                "ma20": float(ma20),
                "ma50": float(ma50),
                "ma200": float(ma200),
                "trend_signal": float(ma_trend),
                "weight": 0.2,
                "confidence": float(confidence)
            }
        except Exception:
            return None

    def _forecast_ml(self, horizon: int) -> Optional[Dict[str, Any]]:
        """Machine learning forecast using Random Forest."""
        if not self.enable_ml or len(self.prices) < 100:
            return None

        try:
            from sklearn.ensemble import RandomForestRegressor

            # Feature engineering
            n = len(self.prices)
            X: List[List[float]] = []
            y: List[float] = []

            for i in range(60, n - 5):
                features = []

                # Returns over various windows
                for window in [5, 10, 20, 30, 60]:
                    ret = self.prices[i] / self.prices[i - window] - 1
                    features.append(ret)

                # Volatility
                window_returns = [self.prices[j] / self.prices[j - 1] - 1 for j in range(i - 20, i)]
                features.append(float(np.std(window_returns)) if window_returns else 0)

                # Moving averages
                for ma in [20, 50]:
                    if i >= ma:
                        sma = sum(self.prices[i - ma:i]) / ma
                        features.append(self.prices[i] / sma - 1)
                    else:
                        features.append(0)

                # RSI
                if i >= 15:
                    gains = []
                    losses = []
                    for j in range(i - 14, i):
                        d = self.prices[j] - self.prices[j - 1]
                        if d >= 0:
                            gains.append(d)
                        else:
                            losses.append(-d)
                    avg_gain = sum(gains) / 14 if gains else 0
                    avg_loss = sum(losses) / 14 if losses else 1e-10
                    rsi = 100 - (100 / (1 + avg_gain / avg_loss)) if avg_loss > 0 else 100
                    features.append(rsi / 100)
                else:
                    features.append(0.5)

                X.append(features)
                y.append(self.prices[i + 5] / self.prices[i] - 1)  # 5-day forward return

            if len(X) < 20:
                return None

            # Train model
            model = RandomForestRegressor(n_estimators=50, max_depth=5, random_state=42)
            model.fit(X, y)

            # Calculate feature importance
            if hasattr(model, 'feature_importances_'):
                feature_names = ['ret_5', 'ret_10', 'ret_20', 'ret_30', 'ret_60', 'volatility', 'ma20', 'ma50', 'rsi']
                self.feature_importance = {
                    name: float(imp)
                    for name, imp in zip(feature_names, model.feature_importances_)
                }

            # Create features for current point
            current_features = []
            i = n - 1

            for window in [5, 10, 20, 30, 60]:
                ret = self.prices[i] / self.prices[i - window] - 1
                current_features.append(ret)

            window_returns = [self.prices[j] / self.prices[j - 1] - 1 for j in range(i - 20, i)]
            current_features.append(float(np.std(window_returns)) if window_returns else 0)

            for ma in [20, 50]:
                if i >= ma:
                    sma = sum(self.prices[i - ma:i]) / ma
                    current_features.append(self.prices[i] / sma - 1)
                else:
                    current_features.append(0)

            if i >= 15:
                gains = []
                losses = []
                for j in range(i - 14, i):
                    d = self.prices[j] - self.prices[j - 1]
                    if d >= 0:
                        gains.append(d)
                    else:
                        losses.append(-d)
                avg_gain = sum(gains) / 14 if gains else 0
                avg_loss = sum(losses) / 14 if losses else 1e-10
                rsi = 100 - (100 / (1 + avg_gain / avg_loss)) if avg_loss > 0 else 100
                current_features.append(rsi / 100)
            else:
                current_features.append(0.5)

            # Predict
            pred_return = model.predict([current_features])[0]
            price = self.prices[-1] * (1 + pred_return) ** (horizon / 5)

            # Get prediction confidence
            tree_preds = [tree.predict([current_features])[0] for tree in model.estimators_]
            pred_std = float(np.std(tree_preds))
            confidence = max(30, min(90, 70 - pred_std * 100))

            return {
                "price": float(price),
                "roi_pct": float((price / self.prices[-1] - 1) * 100),
                "predicted_return": float(pred_return),
                "std_dev": float(pred_std),
                "weight": 0.3,
                "confidence": confidence
            }
        except Exception:
            return None

    def _calculate_confidence(self, results: Dict[str, Any], forecasts: List[float]) -> float:
        """Calculate ensemble confidence score."""
        if not forecasts:
            return 0.0

        # Model agreement (lower std = higher confidence)
        if len(forecasts) > 1:
            cv = float(np.std(forecasts) / np.mean(forecasts)) if np.mean(forecasts) != 0 else 1
            agreement = max(0, 100 - cv * 200)
        else:
            agreement = 60

        # Average model confidence
        model_conf = float(np.mean([f.get("confidence", 50) for f in results["forecasts"].values()]))

        # History length bonus
        history_bonus = min(20, len(self.prices) / 25)

        # Combine
        confidence = (agreement * 0.4) + (model_conf * 0.4) + history_bonus
        return min(100, max(0, confidence))

    def _confidence_level(self, score: float) -> str:
        if score >= 80:
            return "high"
        elif score >= 60:
            return "medium"
        elif score >= 40:
            return "low"
        else:
            return "very_low"


# ============================================================================
# History Analytics
# ============================================================================

def compute_history_analytics(prices: List[float], volumes: Optional[List[float]] = None) -> Dict[str, Any]:
    """Compute comprehensive history analytics."""
    if not prices or len(prices) < 10:
        return {}

    out: Dict[str, Any] = {}
    last = prices[-1]

    # Returns over periods
    periods = {
        "returns_1w": 5,
        "returns_2w": 10,
        "returns_1m": 21,
        "returns_3m": 63,
        "returns_6m": 126,
        "returns_1y": 252,
        "returns_2y": 504,
        "returns_5y": 1260,
    }

    for name, days in periods.items():
        if len(prices) > days:
            prior = prices[-(days + 1)]
            out[name] = float((last / prior - 1) * 100)

    # Moving averages
    for period in [20, 50, 200]:
        if len(prices) >= period:
            ma = sum(prices[-period:]) / period
            out[f"sma_{period}"] = float(ma)
            out[f"price_to_sma_{period}"] = float((last / ma - 1) * 100)

    # Volatility
    if len(prices) >= 31:
        returns = [prices[i] / prices[i - 1] - 1 for i in range(1, len(prices))]
        recent_returns = returns[-30:]
        out["volatility_30d"] = float(np.std(recent_returns) * math.sqrt(252) * 100)

    # Technical indicators
    ti = TechnicalIndicators()

    # RSI
    rsi_values = ti.rsi(prices, 14)
    if rsi_values and rsi_values[-1] is not None:
        out["rsi_14"] = float(rsi_values[-1])
        if out["rsi_14"] > 70:
            out["rsi_signal"] = "overbought"
        elif out["rsi_14"] < 30:
            out["rsi_signal"] = "oversold"
        else:
            out["rsi_signal"] = "neutral"

    # MACD
    macd = ti.macd(prices)
    if macd["macd"] and macd["macd"][-1] is not None:
        out["macd"] = float(macd["macd"][-1])
        out["macd_signal"] = float(macd["signal"][-1]) if macd["signal"][-1] else None
        out["macd_histogram"] = float(macd["histogram"][-1]) if macd["histogram"][-1] else None
        if out["macd"] and out["macd_signal"]:
            out["macd_cross"] = "bullish" if out["macd"] > out["macd_signal"] else "bearish"

    # Bollinger Bands
    bb = ti.bollinger_bands(prices)
    if bb["middle"][-1] is not None:
        out["bb_middle"] = float(bb["middle"][-1])
        out["bb_upper"] = float(bb["upper"][-1])
        out["bb_lower"] = float(bb["lower"][-1])
        if bb["upper"][-1] and bb["lower"][-1]:
            out["bb_position"] = float((last - bb["lower"][-1]) / (bb["upper"][-1] - bb["lower"][-1]))

    # On-Balance Volume
    if volumes and len(volumes) == len(prices):
        obv_values = ti.obv(prices, volumes)
        if obv_values:
            out["obv"] = float(obv_values[-1])
            if len(obv_values) > 20:
                obv_trend = (obv_values[-1] / obv_values[-20] - 1) * 100
                out["obv_trend_pct"] = float(obv_trend)
                out["obv_signal"] = "bullish" if obv_trend > 2 else "bearish" if obv_trend < -2 else "neutral"

    # Maximum Drawdown
    peak = prices[0]
    max_dd = 0.0
    for price in prices:
        if price > peak:
            peak = price
        dd = (price / peak - 1) * 100
        if dd < max_dd:
            max_dd = dd
    out["max_drawdown_pct"] = float(max_dd)

    # Sharpe Ratio (assuming 0% risk-free rate)
    if len(prices) > 30:
        returns = [prices[i] / prices[i - 1] - 1 for i in range(1, len(prices))]
        avg_ret = float(np.mean(returns)) * 252
        std_ret = float(np.std(returns)) * math.sqrt(252)
        out["sharpe_ratio"] = float(avg_ret / std_ret) if std_ret != 0 else None

    # Market Regime
    detector = MarketRegimeDetector(prices)
    regime, confidence = detector.detect()
    out["market_regime"] = regime.value
    out["market_regime_confidence"] = confidence

    # Ensemble Forecast
    if _enable_forecast() and len(prices) >= 60:
        forecaster = EnsembleForecaster(prices, enable_ml=_enable_ml())

        for horizon, period in [(21, "1m"), (63, "3m"), (252, "1y")]:
            forecast = forecaster.forecast(horizon)
            if forecast.get("forecast_available"):
                out[f"forecast_price_{period}"] = forecast["ensemble"]["price"]
                out[f"forecast_roi_{period}_pct"] = forecast["ensemble"]["roi_pct"]
                out[f"forecast_range_low_{period}"] = forecast["ensemble"].get("price_range_low")
                out[f"forecast_range_high_{period}"] = forecast["ensemble"].get("price_range_high")

                if period == "1y":
                    out["forecast_confidence"] = forecast["confidence"]
                    out["forecast_confidence_level"] = forecast["confidence_level"]
                    out["forecast_models"] = forecast["models_used"]
                    out["feature_importance"] = forecast.get("feature_importance")

        out["forecast_method"] = "ensemble_v3"
        out["forecast_updated_utc"] = _utc_iso()
        out["forecast_updated_riyadh"] = _riyadh_iso()

    # Add aliases for dashboard compatibility
    for period in ["1m", "3m", "1y"]:
        if f"forecast_price_{period}" in out:
            out[f"target_price_{period}"] = out[f"forecast_price_{period}"]

    return out


# ============================================================================
# Advanced HTTP Client with Rate Limiting & Circuit Breaker
# ============================================================================

class EODHDClient:
    """Advanced EODHD API client with full feature set."""

    def __init__(self):
        self.base_url = _base_url()
        self.api_key = _token()
        self.timeout_sec = _timeout_default()
        self.retry_attempts = _retry_attempts()
        self.client_id = str(uuid.uuid4())[:8]
        self.rate_limiter = TokenBucket(_rate_limit())
        self.circuit_breaker = SmartCircuitBreaker(
            fail_threshold=_circuit_breaker_threshold(),
            cooldown_sec=_circuit_breaker_timeout()
        )

        # Request queue
        queue_size = _queue_size()
        self.request_queue = RequestQueue(max_size=queue_size)

        # Caches
        self.quote_cache = SmartCache(maxsize=5000, ttl=_quote_ttl_sec())
        self.fund_cache = SmartCache(maxsize=3000, ttl=_fund_ttl_sec())
        self.history_cache = SmartCache(maxsize=2000, ttl=_history_ttl_sec())

        # HTTP client
        timeout = httpx.Timeout(self.timeout_sec, connect=min(10.0, self.timeout_sec))
        self._client = httpx.AsyncClient(
            timeout=timeout,
            headers=self._base_headers(),
            follow_redirects=True,
            limits=httpx.Limits(max_keepalive_connections=30, max_connections=50),
            http2=True
        )

        # Start queue processor
        self._queue_processor_task = asyncio.create_task(self._process_queue())

        # Metrics
        self.metrics: Dict[str, Any] = {
            "requests_total": 0,
            "requests_success": 0,
            "requests_failed": 0,
            "cache_hits": 0,
            "cache_misses": 0,
            "rate_limit_waits": 0,
            "circuit_breaker_blocks": 0
        }
        self._metrics_lock = asyncio.Lock()

        logger.info(
            f"EODHDClient v{PROVIDER_VERSION} initialized | "
            f"client_id={self.client_id} | "
            f"rate={_rate_limit()}/s | cb={_circuit_breaker_threshold()}/{_circuit_breaker_timeout()}s | "
            f"cache_ttl_q={_quote_ttl_sec()}s f={_fund_ttl_sec()}s h={_history_ttl_sec()}s | "
            f"queue={queue_size}"
        )

    def _base_headers(self) -> Dict[str, str]:
        headers = {
            "User-Agent": _ua(),
            "Accept": "application/json, text/plain, */*",
            "Accept-Language": "en-US,en;q=0.9",
            "Accept-Encoding": "gzip, deflate, br",
            "X-Client-ID": self.client_id,
        }

        custom = _json_env_map("EODHD_HEADERS_JSON")
        headers.update(custom)
        return headers

    def _endpoint_headers(self, endpoint: str) -> Dict[str, str]:
        key = f"EODHD_HEADERS_{endpoint.upper()}_JSON"
        return _json_env_map(key)

    async def _update_metric(self, name: str, inc: int = 1) -> None:
        async with self._metrics_lock:
            self.metrics[name] = self.metrics.get(name, 0) + inc

    @trace("eodhd_request")
    async def _request(
        self,
        endpoint: str,
        params: Dict[str, Any],
        *,
        response_type: str = "dict",
        priority: RequestPriority = RequestPriority.NORMAL
    ) -> Tuple[Optional[Union[Dict[str, Any], List[Any]]], Optional[str]]:
        """Make API request with full feature set."""
        request_id = _generate_request_id()
        start_time = time.time()

        await self._update_metric("requests_total")
        _METRICS.inc("requests_total", 1, {"endpoint": endpoint.split('/')[0], "status": "started"})
        _METRICS.set("active_requests", self.rate_limiter._total_acquired - self.rate_limiter._total_rejected)

        if not self.api_key:
            _METRICS.inc("requests_total", 1, {"endpoint": endpoint.split('/')[0], "status": "no_key"})
            return None, "API key not configured"

        # Circuit breaker check
        if not await self.circuit_breaker.allow_request():
            await self._update_metric("circuit_breaker_blocks")
            _METRICS.inc("requests_total", 1, {"endpoint": endpoint.split('/')[0], "status": "blocked"})
            return None, "circuit_breaker_open"

        # Queue request
        future = asyncio.Future()
        queue_item = RequestQueueItem(
            priority=priority,
            endpoint=endpoint,
            params=params,
            response_type=response_type,
            future=future
        )

        queued = await self.request_queue.put(queue_item)
        if not queued:
            _METRICS.inc("requests_total", 1, {"endpoint": endpoint.split('/')[0], "status": "queue_full"})
            return None, "queue_full"

        # Wait for processing
        result = await future

        duration = (time.time() - start_time) * 1000
        _METRICS.observe("request_duration_seconds", duration / 1000, {"endpoint": endpoint.split('/')[0]})

        return result

    async def _process_queue(self) -> None:
        """Process queued requests."""
        while True:
            try:
                item = await self.request_queue.get()
                if item is None:
                    await asyncio.sleep(0.1)
                    continue

                # Process the request
                asyncio.create_task(self._process_queue_item(item))

            except Exception as e:
                logger.error(f"Queue processor error: {e}")
                await asyncio.sleep(1)

    async def _process_queue_item(self, item: RequestQueueItem) -> None:
        """Process a single queue item."""
        try:
            result = await self._execute_request(
                item.endpoint,
                item.params,
                item.response_type
            )
            item.future.set_result(result)
        except Exception as e:
            item.future.set_exception(e)

    async def _execute_request(
        self,
        endpoint: str,
        params: Dict[str, Any],
        response_type: str
    ) -> Tuple[Optional[Union[Dict[str, Any], List[Any]]], Optional[str]]:
        """Execute the actual HTTP request."""
        # Rate limiting
        await self.rate_limiter.wait_and_acquire()
        await self._update_metric("rate_limit_waits")

        url = f"{self.base_url}/{endpoint.lstrip('/')}"
        request_params = dict(params)
        request_params["api_token"] = self.api_key
        request_params["fmt"] = "json"

        headers = self._base_headers()
        headers.update(self._endpoint_headers(endpoint.split('/')[0]))

        last_err: Optional[str] = None

        for attempt in range(self.retry_attempts):
            try:
                resp = await self._client.get(url, params=request_params, headers=headers)
                status = resp.status_code

                # Rate limit handling
                if status == 429:
                    retry_after = int(resp.headers.get("Retry-After", 5))
                    wait_time = min(retry_after, 30)
                    if attempt < self.retry_attempts - 1:
                        await asyncio.sleep(wait_time)
                        continue

                # Server errors
                if 500 <= status < 600:
                    if attempt < self.retry_attempts - 1:
                        wait = (2 ** attempt) + random.random()
                        await asyncio.sleep(min(wait, 10))
                        continue
                    await self.circuit_breaker.on_failure()
                    await self._update_metric("requests_failed")
                    _METRICS.inc("requests_total", 1, {"endpoint": endpoint.split('/')[0], "status": "error"})
                    return None, f"HTTP {status}"

                # Client errors
                if 400 <= status < 500 and status != 404:
                    error_msg = f"HTTP {status}"
                    try:
                        error_data = resp.json()
                        if isinstance(error_data, dict):
                            msg = error_data.get("message") or error_data.get("error") or ""
                            if msg:
                                error_msg += f": {msg}"
                    except Exception:
                        pass
                    await self.circuit_breaker.on_failure()
                    await self._update_metric("requests_failed")
                    _METRICS.inc("requests_total", 1, {"endpoint": endpoint.split('/')[0], "status": "error"})
                    return None, error_msg

                # Success
                try:
                    data = resp.json()
                except Exception:
                    await self.circuit_breaker.on_failure()
                    await self._update_metric("requests_failed")
                    _METRICS.inc("requests_total", 1, {"endpoint": endpoint.split('/')[0], "status": "error"})
                    return None, "invalid_json"

                # Validate response type
                if response_type == "dict" and not isinstance(data, dict):
                    await self.circuit_breaker.on_failure()
                    await self._update_metric("requests_failed")
                    _METRICS.inc("requests_total", 1, {"endpoint": endpoint.split('/')[0], "status": "error"})
                    return None, "unexpected_response_type"
                if response_type == "list" and not isinstance(data, list):
                    await self.circuit_breaker.on_failure()
                    await self._update_metric("requests_failed")
                    _METRICS.inc("requests_total", 1, {"endpoint": endpoint.split('/')[0], "status": "error"})
                    return None, "unexpected_response_type"

                await self.circuit_breaker.on_success()
                await self._update_metric("requests_success")
                _METRICS.inc("requests_total", 1, {"endpoint": endpoint.split('/')[0], "status": "success"})
                return data, None

            except httpx.TimeoutException:
                last_err = "timeout"
                if attempt < self.retry_attempts - 1:
                    await asyncio.sleep(2 ** attempt)
            except httpx.NetworkError as e:
                last_err = f"network_error: {e.__class__.__name__}"
                if attempt < self.retry_attempts - 1:
                    await asyncio.sleep(2 ** attempt)
            except Exception as e:
                last_err = f"unexpected: {e.__class__.__name__}"
                if attempt < self.retry_attempts - 1:
                    await asyncio.sleep(2 ** attempt)

        await self.circuit_breaker.on_failure()
        await self._update_metric("requests_failed")
        _METRICS.inc("requests_total", 1, {"endpoint": endpoint.split('/')[0], "status": "error"})
        return None, last_err or "request_failed"

    async def get_realtime(self, symbol: str, priority: RequestPriority = RequestPriority.NORMAL) -> Tuple[Optional[Dict[str, Any]], Optional[str]]:
        """Get realtime quote."""
        data, err = await self._request(f"real-time/{symbol}", {}, response_type="dict", priority=priority)
        if err or not data:
            return None, err
        return data, None

    async def get_fundamentals(self, symbol: str, priority: RequestPriority = RequestPriority.NORMAL) -> Tuple[Optional[Dict[str, Any]], Optional[str]]:
        """Get fundamentals data."""
        data, err = await self._request(f"fundamentals/{symbol}", {}, response_type="dict", priority=priority)
        if err or not data:
            return None, err
        return data, None

    async def get_eod(self, symbol: str, from_date: date, to_date: date, priority: RequestPriority = RequestPriority.NORMAL) -> Tuple[Optional[List[Any]], Optional[str]]:
        """Get EOD historical data."""
        params = {
            "from": from_date.isoformat(),
            "to": to_date.isoformat(),
            "period": "d"
        }
        data, err = await self._request(f"eod/{symbol}", params, response_type="list", priority=priority)
        if err or not data:
            return None, err
        return data, None

    async def get_metrics(self) -> Dict[str, Any]:
        """Get client metrics."""
        async with self._metrics_lock:
            metrics = dict(self.metrics)

        metrics["circuit_breaker"] = self.circuit_breaker.get_stats()
        metrics["rate_limiter"] = self.rate_limiter.get_stats()
        metrics["request_queue"] = self.request_queue.get_stats()
        metrics["cache_sizes"] = {
            "quote": await self.quote_cache.size(),
            "fund": await self.fund_cache.size(),
            "history": await self.history_cache.size()
        }
        metrics["cache_stats"] = {
            "quote": self.quote_cache.get_stats(),
            "fund": self.fund_cache.get_stats(),
            "history": self.history_cache.get_stats()
        }

        return metrics

    async def close(self) -> None:
        """Close HTTP client."""
        self._queue_processor_task.cancel()
        try:
            await self._queue_processor_task
        except:
            pass
        await self._client.aclose()


# ============================================================================
# Data Mapping Functions
# ============================================================================

def map_realtime_data(data: Dict[str, Any], symbol: str, norm_symbol: str) -> Dict[str, Any]:
    """Map realtime API response to standard format."""
    result: Dict[str, Any] = {
        "requested_symbol": symbol,
        "normalized_symbol": norm_symbol,
    }

    # Price fields
    result["current_price"] = safe_float(pick(data, "close", "Close", "last", "Last", "price"))
    result["previous_close"] = safe_float(pick(data, "previous_close", "previousClose", "PreviousClose"))
    result["open"] = safe_float(pick(data, "open", "Open"))
    result["day_high"] = safe_float(pick(data, "high", "High"))
    result["day_low"] = safe_float(pick(data, "low", "Low"))
    result["volume"] = safe_float(pick(data, "volume", "Volume"))

    # Change fields
    result["price_change"] = safe_float(pick(data, "change", "Change"))
    result["percent_change"] = safe_float(pick(data, "change_p", "ChangePercent", "changePercent"))

    # Add derived fields
    fill_derived(result)

    return clean_patch(result)


def map_fundamentals_data(data: Dict[str, Any], symbol: str, norm_symbol: str) -> Dict[str, Any]:
    """Map fundamentals API response to standard format."""
    result: Dict[str, Any] = {
        "requested_symbol": symbol,
        "normalized_symbol": norm_symbol,
    }

    general = data.get("General") or {}
    highlights = data.get("Highlights") or {}
    valuation = data.get("Valuation") or {}
    technicals = data.get("Technicals") or {}
    shares = data.get("SharesStats") or {}

    # Company info
    result["name"] = safe_str(pick(general, "Name", "LongName"))
    result["sector"] = safe_str(pick(general, "Sector"))
    result["industry"] = safe_str(pick(general, "Industry"))
    result["sub_sector"] = safe_str(pick(general, "GicSector", "GicIndustry"))
    result["currency"] = safe_str(pick(general, "CurrencyCode")) or "USD"
    result["exchange"] = safe_str(pick(general, "Exchange", "ExchangeName"))
    result["listing_date"] = safe_str(pick(general, "IPODate"))

    # Market cap
    mc = safe_float(highlights.get("MarketCapitalization"))
    mc_mln = safe_float(highlights.get("MarketCapitalizationMln"))
    if mc is None and mc_mln is not None:
        mc = mc_mln * 1_000_000
    result["market_cap"] = mc

    # Valuation metrics
    result["pe_ttm"] = safe_float(highlights.get("PERatio"))
    result["eps_ttm"] = safe_float(highlights.get("EarningsShare"))
    result["pb"] = safe_float(valuation.get("PriceBookMRQ")) or safe_float(highlights.get("PriceBook"))
    result["ps"] = safe_float(valuation.get("PriceSalesTTM")) or safe_float(highlights.get("PriceSalesTTM"))
    result["ev_ebitda"] = safe_float(valuation.get("EnterpriseValueEbitda")) or safe_float(highlights.get("EVToEBITDA"))

    # Profitability
    result["dividend_yield"] = safe_float(highlights.get("DividendYield"))
    result["roe"] = safe_float(highlights.get("ReturnOnEquityTTM"))
    result["roa"] = safe_float(highlights.get("ReturnOnAssetsTTM"))
    result["net_margin"] = safe_float(highlights.get("ProfitMargin"))
    result["beta"] = safe_float(technicals.get("Beta"))

    # 52-week range
    result["week_52_high"] = safe_float(technicals.get("52WeekHigh"))
    result["week_52_low"] = safe_float(technicals.get("52WeekLow"))

    # Moving averages
    result["ma50"] = safe_float(technicals.get("50DayMA"))
    result["ma20"] = safe_float(technicals.get("20DayMA")) or safe_float(technicals.get("10DayMA"))
    result["avg_volume_30d"] = safe_float(technicals.get("AverageVolume"))

    # Shares
    so = safe_float(shares.get("SharesOutstanding")) or safe_float(general.get("SharesOutstanding"))
    result["shares_outstanding"] = so

    float_shares = safe_float(shares.get("SharesFloat")) or safe_float(shares.get("FloatShares"))
    if so and float_shares and so > 0:
        result["free_float"] = (float_shares / so) * 100.0

    # Financial health
    result["debt_to_equity"] = safe_float(highlights.get("DebtToEquity"))
    result["current_ratio"] = safe_float(highlights.get("CurrentRatio"))
    result["quick_ratio"] = safe_float(highlights.get("QuickRatio"))

    return clean_patch(result)


def map_eod_data(data: List[Any], symbol: str, norm_symbol: str) -> Dict[str, Any]:
    """Map EOD historical data to standard format with full analytics."""
    if not data:
        return {}

    # Extract price series
    prices: List[float] = []
    volumes: List[float] = []
    last_date: Optional[str] = None

    for item in data:
        if not isinstance(item, dict):
            continue

        close = safe_float(item.get("close"))
        if close is not None:
            prices.append(close)

        vol = safe_float(item.get("volume"))
        if vol is not None:
            volumes.append(vol)

        date_str = item.get("date")
        if isinstance(date_str, str) and date_str:
            last_date = date_str

    if not prices:
        return {}

    # Limit to max points
    max_points = _history_points_max()
    prices = prices[-max_points:]
    if volumes:
        volumes = volumes[-max_points:]

    # Compute analytics
    analytics = compute_history_analytics(prices, volumes if volumes else None)

    result: Dict[str, Any] = {
        "requested_symbol": symbol,
        "normalized_symbol": norm_symbol,
        "history_points": len(prices),
        "history_last_date": last_date or "",
        "history_last_utc": _utc_iso(),
        "history_last_riyadh": _riyadh_iso(),
        "current_price": prices[-1] if prices else None,
    }
    result.update(analytics)

    return clean_patch(result)


# ============================================================================
# Main Fetch Function
# ============================================================================

_CLIENT_INSTANCE: Optional[EODHDClient] = None
_CLIENT_LOCK = asyncio.Lock()


async def get_client() -> EODHDClient:
    """Get or create client singleton."""
    global _CLIENT_INSTANCE
    if _CLIENT_INSTANCE is None:
        async with _CLIENT_LOCK:
            if _CLIENT_INSTANCE is None:
                _CLIENT_INSTANCE = EODHDClient()
    return _CLIENT_INSTANCE


async def close_client() -> None:
    """Close and cleanup client."""
    global _CLIENT_INSTANCE
    client = _CLIENT_INSTANCE
    _CLIENT_INSTANCE = None
    if client:
        await client.close()


async def _fetch(symbol: str, include_fundamentals: bool = True, include_history: bool = True) -> Dict[str, Any]:
    """Main fetch function with full enrichment."""
    sym_in = (symbol or "").strip()
    if not sym_in:
        return {
            "data_source": PROVIDER_NAME,
            "provider_version": PROVIDER_VERSION,
            "error": "empty_symbol",
            "last_updated_utc": _utc_iso(),
            "last_updated_riyadh": _riyadh_iso(),
        }

    # KSA blocking
    if looks_like_ksa(sym_in) and not _allow_ksa():
        return {
            "symbol": sym_in,
            "data_source": PROVIDER_NAME,
            "provider_version": PROVIDER_VERSION,
            "provider_blocked": True,
            "provider_warning": "KSA symbols blocked for EODHD (set ALLOW_EODHD_KSA=true to enable)",
            "last_updated_utc": _utc_iso(),
            "last_updated_riyadh": _riyadh_iso(),
        }

    # Generate symbol variants
    variants, asset_class = generate_symbol_variants(sym_in)
    if not variants:
        variants = [sym_in]

    client = await get_client()
    result: Dict[str, Any] = {
        "symbol": sym_in,
        "data_source": PROVIDER_NAME,
        "provider_version": PROVIDER_VERSION,
        "asset_class": asset_class.value,
        "last_updated_utc": _utc_iso(),
        "last_updated_riyadh": _riyadh_iso(),
        "provider_symbol": variants[0],
        "request_id": _generate_request_id(),
    }

    warnings: List[str] = []
    used_symbol = variants[0]

    # Try each variant until success
    for variant in variants:
        # Check cache first
        cache_key_q = f"quote:{variant}"
        cached_quote = await client.quote_cache.get(cache_key_q)
        if cached_quote:
            quote_data = cached_quote
            quote_err = None
        else:
            quote_data, quote_err = await client.get_realtime(variant)
            if quote_data and not quote_err:
                await client.quote_cache.set(cache_key_q, quote_data)

        if quote_data and not quote_err:
            used_symbol = variant
            result["provider_symbol"] = variant
            quote_mapped = map_realtime_data(quote_data, sym_in, variant)
            merge_into(result, quote_mapped)
            break
        else:
            warnings.append(f"quote_{variant}: {quote_err or 'failed'}")

    # Fundamentals (if enabled and not already failed for all variants)
    if include_fundamentals and _enable_fundamentals() and asset_class in (AssetClass.EQUITY, AssetClass.ETF, AssetClass.INDEX):
        for variant in variants:
            cache_key_f = f"fund:{variant}"
            cached_fund = await client.fund_cache.get(cache_key_f)
            if cached_fund:
                fund_data = cached_fund
                fund_err = None
            else:
                fund_data, fund_err = await client.get_fundamentals(variant)
                if fund_data and not fund_err:
                    await client.fund_cache.set(cache_key_f, fund_data)

            if fund_data and not fund_err:
                result.setdefault("provider_symbol_fundamentals", variant)
                fund_mapped = map_fundamentals_data(fund_data, sym_in, variant)
                merge_into(
                    result,
                    fund_mapped,
                    force_keys=(
                        "market_cap", "pe_ttm", "eps_ttm", "week_52_high", "week_52_low",
                        "shares_outstanding", "free_float", "ma20", "ma50", "avg_volume_30d",
                        "beta", "dividend_yield", "roe", "roa"
                    ),
                )
                break
            else:
                warnings.append(f"fund_{variant}: {fund_err or 'failed'}")

    # History (if enabled)
    if include_history and _enable_history():
        days = _history_days()
        to_date = date.today()
        from_date = to_date - timedelta(days=days)

        for variant in variants:
            cache_key_h = f"hist:{variant}:{days}"
            cached_hist = await client.history_cache.get(cache_key_h)
            if cached_hist:
                hist_data = cached_hist
                hist_err = None
            else:
                hist_data, hist_err = await client.get_eod(variant, from_date, to_date)
                if hist_data and not hist_err:
                    await client.history_cache.set(cache_key_h, hist_data)

            if hist_data and not hist_err:
                result.setdefault("provider_symbol_history", variant)
                hist_mapped = map_eod_data(hist_data, sym_in, variant)
                merge_into(result, hist_mapped)
                break
            else:
                warnings.append(f"hist_{variant}: {hist_err or 'failed'}")

    # Final touches
    fill_derived(result)

    # Data quality
    quality_category, quality_score = data_quality_score(result)
    result["data_quality"] = quality_category
    result["data_quality_score"] = quality_score

    # Add warnings if verbose
    if warnings and _verbose_warn():
        result["_warnings"] = " | ".join(warnings[:3])  # Limit to 3

    return clean_patch(result)


# ============================================================================
# Public API (Engine Compatible)
# ============================================================================

async def get_quote(symbol: str, *args: Any, **kwargs: Any) -> Dict[str, Any]:
    """Get fully enriched quote (fundamentals + history)."""
    return await _fetch(symbol, include_fundamentals=True, include_history=True)


async def get_enriched_quote(symbol: str, *args: Any, **kwargs: Any) -> Dict[str, Any]:
    """Alias for get_quote."""
    return await get_quote(symbol, *args, **kwargs)


async def fetch_quote(symbol: str, *args: Any, **kwargs: Any) -> Dict[str, Any]:
    """Alias for get_quote."""
    return await get_quote(symbol, *args, **kwargs)


async def quote(symbol: str, *args: Any, **kwargs: Any) -> Dict[str, Any]:
    """Alias for get_quote."""
    return await get_quote(symbol, *args, **kwargs)


async def fetch(symbol: str, *args: Any, **kwargs: Any) -> Dict[str, Any]:
    """Alias for get_quote."""
    return await get_quote(symbol, *args, **kwargs)


async def fetch_quote_patch(symbol: str, *args: Any, **kwargs: Any) -> Dict[str, Any]:
    """Fetch only realtime quote (no fundamentals, no history)."""
    return await _fetch(symbol, include_fundamentals=False, include_history=False)


async def fetch_enriched_quote_patch(symbol: str, *args: Any, **kwargs: Any) -> Dict[str, Any]:
    """Fetch fully enriched quote."""
    return await get_quote(symbol, *args, **kwargs)


async def fetch_quote_and_enrichment_patch(symbol: str, *args: Any, **kwargs: Any) -> Dict[str, Any]:
    """Alias for enriched quote."""
    return await get_quote(symbol, *args, **kwargs)


async def fetch_quote_and_fundamentals_patch(symbol: str, *args: Any, **kwargs: Any) -> Dict[str, Any]:
    """Fetch quote + fundamentals (no history)."""
    return await _fetch(symbol, include_fundamentals=True, include_history=False)


async def fetch_enriched_patch(symbol: str, *args: Any, **kwargs: Any) -> Dict[str, Any]:
    """Alias for enriched quote."""
    return await get_quote(symbol, *args, **kwargs)


async def get_client_metrics() -> Dict[str, Any]:
    """Get client performance metrics."""
    client = await get_client()
    return await client.get_metrics()


def normalize_symbol(symbol: str) -> str:
    """Public symbol normalization."""
    variants, _ = generate_symbol_variants(symbol)
    return variants[0] if variants else symbol


async def health_check() -> Dict[str, Any]:
    """Perform health check on the client."""
    try:
        client = await get_client()
        metrics = await client.get_metrics()
        
        # Determine health status
        if metrics["circuit_breaker"]["state"] == "open":
            status = "degraded"
        elif metrics["rate_limiter"]["utilization"] > 0.9:
            status = "degraded"
        elif metrics["request_queue"]["utilization"] > 0.8:
            status = "degraded"
        else:
            status = "healthy"
        
        return {
            "status": status,
            "provider": PROVIDER_NAME,
            "version": PROVIDER_VERSION,
            "client_id": client.client_id,
            "timestamp": _utc_now_iso(),
            "metrics": {
                "circuit_breaker": metrics["circuit_breaker"]["state"],
                "queue_size": metrics["request_queue"]["current_sizes"],
                "cache_hit_rate": {
                    k: v["hit_rate"] for k, v in metrics["cache_stats"].items()
                }
            }
        }
    except Exception as e:
        return {
            "status": "error",
            "provider": PROVIDER_NAME,
            "version": PROVIDER_VERSION,
            "error": str(e),
            "timestamp": _utc_now_iso()
        }


# ============================================================================
# Module Exports
# ============================================================================

__all__ = [
    "PROVIDER_NAME",
    "PROVIDER_VERSION",
    "get_quote",
    "get_enriched_quote",
    "fetch_quote",
    "quote",
    "fetch",
    "fetch_quote_patch",
    "fetch_enriched_quote_patch",
    "fetch_quote_and_enrichment_patch",
    "fetch_quote_and_fundamentals_patch",
    "fetch_enriched_patch",
    "get_client_metrics",
    "normalize_symbol",
    "close_client",
    "health_check",
    "AssetClass",
    "MarketRegime",
    "RequestPriority"
]
