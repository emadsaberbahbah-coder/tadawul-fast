#!/usr/bin/env python3
# core/providers/eodhd_provider.py
"""
================================================================================
EODHD Provider — v4.0.0 (Next-Gen Enterprise Global + ML)
================================================================================

What's new in v4.0.0:
- ✅ ADDED: XGBoost integration for superior ML forecasting accuracy.
- ✅ ADDED: Actual statsmodels ARIMA integration (with random-walk fallback).
- ✅ ADDED: High-performance JSON parsing via `orjson` (if available).
- ✅ ADDED: Exponential Backoff with 'Full Jitter' to prevent thundering herds.
- ✅ ADDED: Dynamic Circuit Breaker with progressive timeout scaling.
- ✅ ADDED: Ichimoku Cloud and Stochastic Oscillator indicators.
- ✅ ADDED: Strict Type Coercion for NaN/Inf anomalies.
- ✅ ENHANCED: Memory management using zero-copy slicing where possible.
- ✅ ENHANCED: Market Regime Detection using statistical rolling distributions.
- ✅ ENHANCED: Concurrency model with strict Semaphore bounds.

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
import functools
import hashlib
import json
import logging
import math
import os
import random
import re
import threading
import time
import uuid
from collections import deque, defaultdict
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta, timezone
from enum import Enum
from typing import Any, Dict, List, Optional, Sequence, Set, Tuple, Union

import httpx
import numpy as np

# ---------------------------------------------------------------------------
# High-Performance JSON fallback
# ---------------------------------------------------------------------------
try:
    import orjson
    def json_loads(data: Union[str, bytes]) -> Any:
        return orjson.loads(data)
except ImportError:
    def json_loads(data: Union[str, bytes]) -> Any:
        return json.loads(data)

# ---------------------------------------------------------------------------
# Optional Scientific & ML Stack
# ---------------------------------------------------------------------------
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

try:
    import xgboost as xgb
    XGB_AVAILABLE = True
except ImportError:
    XGB_AVAILABLE = False

try:
    from statsmodels.tsa.arima.model import ARIMA as StatsARIMA
    import warnings
    from statsmodels.tools.sm_exceptions import ConvergenceWarning
    warnings.simplefilter('ignore', ConvergenceWarning)
    STATSMODELS_AVAILABLE = True
except ImportError:
    STATSMODELS_AVAILABLE = False

# ---------------------------------------------------------------------------
# Tracing and Metrics Stack
# ---------------------------------------------------------------------------
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

PROVIDER_VERSION = "4.0.0"
PROVIDER_NAME = "eodhd"

DEFAULT_BASE_URL = "https://eodhistoricaldata.com/api"
DEFAULT_TIMEOUT_SEC = 15.0
DEFAULT_RETRY_ATTEMPTS = 4
DEFAULT_MAX_CONCURRENCY = 40
DEFAULT_RATE_LIMIT = 5.0  # requests per second
DEFAULT_CIRCUIT_BREAKER_THRESHOLD = 5
DEFAULT_CIRCUIT_BREAKER_TIMEOUT = 30.0
DEFAULT_QUEUE_SIZE = 2000
DEFAULT_PRIORITY_LEVELS = 4

_TRUTHY = {"1", "true", "yes", "y", "on", "t"}
_FALSY = {"0", "false", "no", "n", "off", "f"}

USER_AGENT_DEFAULT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/124.0.0.0 Safari/537.36"
)

# Tracing and metrics
_TRACING_ENABLED = os.getenv("EODHD_TRACING_ENABLED", "").strip().lower() in _TRUTHY
_METRICS_ENABLED = os.getenv("EODHD_METRICS_ENABLED", "").strip().lower() in _TRUTHY


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
    current_cooldown: float = DEFAULT_CIRCUIT_BREAKER_TIMEOUT


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


def _trace(name: Optional[str] = None):
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
        if not _PROMETHEUS_AVAILABLE:
            return ""
        from prometheus_client import generate_latest, REGISTRY
        return generate_latest(REGISTRY).decode('utf-8')


_METRICS = MetricsRegistry()


# ============================================================================
# Environment Helpers
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
    return datetime.now(timezone.utc)


def _utc_now_iso() -> str:
    return _utc_now().isoformat()


def _generate_request_id() -> str:
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


def _rate_limit() -> float:
    return _env_float("EODHD_RATE_LIMIT", DEFAULT_RATE_LIMIT)


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
    return _env_bool("EODHD_ENABLE_ML", True)


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
# Cache with TTL & LRU Eviction
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

    def get_stats(self) -> Dict[str, Any]:
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
# Rate Limiter & Circuit Breaker (Dynamic)
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
        return {
            "rate": self.rate, "capacity": self.capacity,
            "current_tokens": self.tokens,
            "utilization": 1.0 - (self.tokens / self.capacity) if self.capacity > 0 else 0,
            "total_acquired": self._total_acquired, "total_rejected": self._total_rejected
        }


class DynamicCircuitBreaker:
    """Circuit breaker that progressively scales timeouts for severe degradation."""
    
    def __init__(self, fail_threshold: int = 5, base_cooldown: float = 30.0):
        self.fail_threshold = fail_threshold
        self.base_cooldown = base_cooldown
        self.stats = CircuitBreakerStats(current_cooldown=base_cooldown)
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
                    _METRICS.set("circuit_breaker_state", -1)
                    return True
                _METRICS.set("circuit_breaker_state", 0)
                return False
            elif self.stats.state == CircuitState.HALF_OPEN:
                if self.half_open_calls < 2:
                    self.half_open_calls += 1
                    _METRICS.set("circuit_breaker_state", -1)
                    return True
                return False
            return False

    async def on_success(self) -> None:
        async with self._lock:
            self.stats.successes += 1
            self.stats.total_successes += 1
            if self.stats.state == CircuitState.HALF_OPEN:
                self.stats.state = CircuitState.CLOSED
                self.stats.failures = 0
                self.stats.current_cooldown = self.base_cooldown
                _METRICS.set("circuit_breaker_state", 1)

    async def on_failure(self, status_code: int = 500) -> None:
        async with self._lock:
            self.stats.failures += 1
            self.stats.total_failures += 1
            
            # Progressive backoff for extreme failures
            if status_code in (401, 403, 429):
                self.stats.current_cooldown = min(300.0, self.stats.current_cooldown * 1.5)
            
            if self.stats.state == CircuitState.CLOSED and self.stats.failures >= self.fail_threshold:
                self.stats.state = CircuitState.OPEN
                self.stats.open_until = time.monotonic() + self.stats.current_cooldown
                _METRICS.set("circuit_breaker_state", 0)
            elif self.stats.state == CircuitState.HALF_OPEN:
                self.stats.state = CircuitState.OPEN
                self.stats.current_cooldown = min(300.0, self.stats.current_cooldown * 2)
                self.stats.open_until = time.monotonic() + self.stats.current_cooldown
                _METRICS.set("circuit_breaker_state", 0)

    def get_stats(self) -> Dict[str, Any]:
        return {
            "state": self.stats.state.value,
            "failures": self.stats.failures,
            "successes": self.stats.successes,
            "total_failures": self.stats.total_failures,
            "total_successes": self.stats.total_successes,
            "open_until": self.stats.open_until,
            "current_cooldown": self.stats.current_cooldown
        }


# ============================================================================
# Request Queue
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
        async with self._lock:
            total_size = sum(q.qsize() for q in self.queues.values())
            if total_size >= self.max_size:
                return False
            
            await self.queues[item.priority].put(item)
            self._total_queued += 1
            _METRICS.set("queue_size", total_size + 1)
            return True
    
    async def get(self) -> Optional[RequestQueueItem]:
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
        return {
            "max_size": self.max_size,
            "current_sizes": {p.value: q.qsize() for p, q in self.queues.items()},
            "total_queued": self._total_queued,
            "total_processed": self._total_processed,
            "utilization": sum(q.qsize() for q in self.queues.values()) / self.max_size if self.max_size > 0 else 0
        }


# ============================================================================
# Symbol Normalization
# ============================================================================

_KSA_RE = re.compile(r"^\d{3,6}(\.SR)?$", re.IGNORECASE)
_EXCH_SUFFIX_RE = re.compile(r"^(.+)\.([A-Z0-9]{2,10})$")
_INDEX_PREFIX_RE = re.compile(r"^\^([A-Z0-9]+)$")
_FOREX_SUFFIX_RE = re.compile(r"^(.+)=X$")
_COMMODITY_SUFFIX_RE = re.compile(r"^(.+)=F$")


def looks_like_ksa(symbol: str) -> bool:
    s = (symbol or "").strip().upper()
    if not s: return False
    if s.endswith(".SR"): return True
    if s.isdigit() and 3 <= len(s) <= 6: return True
    return bool(_KSA_RE.match(s))


def split_exchange_suffix(sym: str) -> Tuple[str, Optional[str]]:
    s = (sym or "").strip().upper()
    if not s: return "", None
    m = _EXCH_SUFFIX_RE.match(s)
    if not m: return s, None
    base = (m.group(1) or "").strip()
    exch = (m.group(2) or "").strip()
    if not base or not exch: return s, None
    return base, exch


def detect_asset_class(symbol: str) -> AssetClass:
    s = (symbol or "").strip().upper()
    base, exch = split_exchange_suffix(s)
    if exch in ("INDX", "INDEX"): return AssetClass.INDEX
    if exch in ("FOREX", "FX"): return AssetClass.FOREX
    if exch in ("COMM", "COM", "CMD"): return AssetClass.COMMODITY
    if exch in ("ETF", "ETP"): return AssetClass.ETF
    if exch in ("FUND", "MF"): return AssetClass.FUND
    if exch in ("BOND", "GBOND"): return AssetClass.BOND

    if _INDEX_PREFIX_RE.match(s): return AssetClass.INDEX
    if _FOREX_SUFFIX_RE.match(s): return AssetClass.FOREX
    if _COMMODITY_SUFFIX_RE.match(s): return AssetClass.COMMODITY
    if s.endswith(".TO") or s.endswith(".V") or s.endswith(".L"): return AssetClass.EQUITY
    return AssetClass.EQUITY


_DEFAULT_INDEX_ALIASES: Dict[str, str] = {
    "TASI": "TASI.INDX", "NOMU": "NOMU.INDX", "SPX": "^GSPC.INDX",
    "IXIC": "^IXIC.INDX", "DJI": "^DJI.INDX", "FTSE": "^FTSE.INDX",
    "N225": "^N225.INDX", "HSI": "^HSI.INDX",
}

_DEFAULT_COMMODITY_ALIASES: Dict[str, str] = {
    "GC=F": "GC.COMM", "SI=F": "SI.COMM", "CL=F": "CL.COMM",
    "BZ=F": "BZ.COMM", "NG=F": "NG.COMM", "HG=F": "HG.COMM",
    "ZW=F": "ZW.COMM", "ZC=F": "ZC.COMM", "ZS=F": "ZS.COMM",
}

_DEFAULT_FOREX_ALIASES: Dict[str, str] = {
    "EURUSD=X": "EUR.FOREX", "GBPUSD=X": "GBP.FOREX", "USDJPY=X": "JPY.FOREX",
    "USDCHF=X": "CHF.FOREX", "AUDUSD=X": "AUD.FOREX", "USDCAD=X": "CAD.FOREX",
    "NZDUSD=X": "NZD.FOREX", "SAR=X": "SAR.FOREX",
}

_INDEX_ALIASES = {**_DEFAULT_INDEX_ALIASES, **_json_env_map("EODHD_INDEX_ALIASES_JSON")}
_COMMODITY_ALIASES = {**_DEFAULT_COMMODITY_ALIASES, **_json_env_map("EODHD_COMMODITY_ALIASES_JSON")}
_FOREX_ALIASES = {**_DEFAULT_FOREX_ALIASES, **_json_env_map("EODHD_FOREX_ALIASES_JSON")}


def generate_symbol_variants(raw_symbol: str) -> Tuple[List[str], AssetClass]:
    s = (raw_symbol or "").strip().upper()
    if not s: return [], AssetClass.UNKNOWN

    if s in _INDEX_ALIASES:
        _METRICS.inc("symbol_variants_generated", 1, {"asset_class": "index"})
        return [_INDEX_ALIASES[s], s], AssetClass.INDEX
    if s in _COMMODITY_ALIASES:
        _METRICS.inc("symbol_variants_generated", 1, {"asset_class": "commodity"})
        return [_COMMODITY_ALIASES[s], s], AssetClass.COMMODITY
    if s in _FOREX_ALIASES:
        _METRICS.inc("symbol_variants_generated", 1, {"asset_class": "forex"})
        return [_FOREX_ALIASES[s], s], AssetClass.FOREX

    asset_class = detect_asset_class(s)
    variants: List[str] = []
    seen: Set[str] = set()

    if asset_class == AssetClass.INDEX:
        variants.append(f"{s[1:]}.INDX" if s.startswith("^") else f"{s}.INDX")
        variants.append(s)
    elif asset_class == AssetClass.FOREX:
        variants.append(f"{s[:-2]}.FOREX" if s.endswith("=X") else f"{s}.FOREX")
        variants.append(s)
    elif asset_class == AssetClass.COMMODITY:
        variants.append(f"{s[:-2]}.COMM" if s.endswith("=F") else f"{s}.COMM")
        variants.append(s)
    else:
        base, exch = split_exchange_suffix(s)
        if exch:
            variants.extend([s, base])
        else:
            variants.extend([f"{s}.{_default_exchange()}", s])

    if asset_class == AssetClass.EQUITY:
        for v in list(variants):
            b, e = split_exchange_suffix(v)
            if e:
                b1, b2 = b.replace(".", "-"), b.replace("-", ".")
                if b1 != b and b1 not in seen: variants.append(f"{b1}.{e}")
                if b2 != b and b2 != b1 and b2 not in seen: variants.append(f"{b2}.{e}")

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
    if val is None: return None
    try:
        if isinstance(val, (int, float)):
            f = float(val)
            if math.isnan(f) or math.isinf(f): return None
            return f
        s = str(val).strip()
        if not s or s in {"-", "—", "N/A", "NA", "null", "None", ""}: return None
        s = s.replace(",", "").replace("%", "").replace("$", "").replace("£", "").replace("€", "")
        if s.startswith("(") and s.endswith(")"): s = "-" + s[1:-1].strip()
        m = re.match(r"^(-?\d+(\.\d+)?)([KMB])$", s, re.IGNORECASE)
        mult = 1.0
        if m:
            num = m.group(1)
            suf = m.group(3).upper()
            mult = 1_000.0 if suf == "K" else 1_000_000.0 if suf == "M" else 1_000_000_000.0
            s = num
        f = float(s) * mult
        if math.isnan(f) or math.isinf(f): return None
        return f
    except Exception:
        return None

def safe_int(val: Any) -> Optional[int]:
    f = safe_float(val)
    return int(round(f)) if f is not None else None

def safe_str(val: Any) -> Optional[str]:
    if val is None: return None
    s = str(val).strip()
    return s if s else None

def pick(d: Any, *keys: str) -> Any:
    if not isinstance(d, dict): return None
    for k in keys:
        if k in d: return d[k]
    return None

def position_52w(cur: Optional[float], lo: Optional[float
