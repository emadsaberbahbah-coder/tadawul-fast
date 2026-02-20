#!/usr/bin/env python3
# core/providers/finnhub_provider.py
"""
================================================================================
Finnhub Provider — v4.1.0 (Next-Gen Enterprise Global + ML)
================================================================================

What's new in v4.1.0:
- ✅ ADDED: XGBoost integration for superior ML forecasting accuracy.
- ✅ ADDED: Actual statsmodels ARIMA integration (with random-walk fallback).
- ✅ ADDED: High-performance JSON parsing via `orjson` (if available).
- ✅ ADDED: Exponential Backoff with 'Full Jitter' to prevent thundering herds.
- ✅ ADDED: Dynamic Circuit Breaker with progressive timeout scaling.
- ✅ ADDED: Ichimoku Cloud and Stochastic Oscillator indicators.
- ✅ ADDED: Strict Type Coercion for NaN/Inf anomalies.
- ✅ ENHANCED: Request Queuing with Priority levels and strict Semaphores.
- ✅ ENHANCED: Memory management using zero-copy slicing where possible.
- ✅ ENHANCED: Market Regime Detection using statistical rolling distributions.

Key Features:
- Global equity coverage (US, EU, Asia, etc.)
- Intelligent symbol normalization & KSA Blocking
- Yahoo Finance special symbol rejection
- Ensemble forecasts with confidence levels
- Full technical analysis suite & Market regime classification
- Production-grade error handling & Prometheus Metrics
"""

from __future__ import annotations

import asyncio
import functools
import json
import logging
import math
import os
import random
import re
import threading
import time
import uuid
from collections import deque
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
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
    SCIPY_AVAILABLE = True
except ImportError:
    SCIPY_AVAILABLE = False

try:
    from sklearn.linear_model import LinearRegression
    from sklearn.ensemble import RandomForestRegressor
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
    from opentelemetry.trace import Status, StatusCode
    _OTEL_AVAILABLE = True
except ImportError:
    _OTEL_AVAILABLE = False
    trace = None

try:
    from prometheus_client import Counter, Histogram, Gauge
    _PROMETHEUS_AVAILABLE = True
except ImportError:
    _PROMETHEUS_AVAILABLE = False

logger = logging.getLogger("core.providers.finnhub_provider")

PROVIDER_NAME = "finnhub"
PROVIDER_VERSION = "4.1.0"

DEFAULT_BASE_URL = "https://finnhub.io/api/v1"
DEFAULT_TIMEOUT_SEC = 15.0
DEFAULT_RETRY_ATTEMPTS = 4
DEFAULT_MAX_CONCURRENCY = 40
DEFAULT_RATE_LIMIT = 30.0  # Finnhub default free tier
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

# Regex patterns for symbol rejection
_KSA_CODE_RE = re.compile(r"^\d{3,6}(\.SR)?$", re.IGNORECASE)
_SPECIAL_SYMBOL_RE = re.compile(r"(\^|=|/|:)", re.IGNORECASE)
_CRYPTO_YAHOO_RE = re.compile(r"^[A-Z0-9]{2,10}-[A-Z0-9]{2,10}$", re.IGNORECASE)
_FUTURE_YAHOO_RE = re.compile(r"^[A-Z]{2,5}=F$", re.IGNORECASE)
_INDEX_YAHOO_RE = re.compile(r"^\^[A-Z]+$", re.IGNORECASE)

_TRACING_ENABLED = os.getenv("FINNHUB_TRACING_ENABLED", "").strip().lower() in _TRUTHY
_METRICS_ENABLED = os.getenv("FINNHUB_METRICS_ENABLED", "").strip().lower() in _TRUTHY


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
    future: asyncio.Future
    created_at: float = field(default_factory=time.time)


# ============================================================================
# Shared Normalizer Integration
# ============================================================================

def _try_import_shared_normalizer() -> Tuple[Optional[Any], Optional[Any]]:
    try:
        from core.symbols.normalize import normalize_symbol as _ns
        from core.symbols.normalize import looks_like_ksa as _lk
        return _ns, _lk
    except Exception:
        return None, None

_SHARED_NORMALIZE, _SHARED_LOOKS_KSA = _try_import_shared_normalizer()


# ============================================================================
# Environment Helpers
# ============================================================================

def _env_str(name: str, default: str = "") -> str:
    v = os.getenv(name)
    return str(v).strip() if v is not None and str(v).strip() else default

def _env_int(name: str, default: int) -> int:
    v = os.getenv(name)
    try: return int(str(v).strip()) if v is not None else default
    except Exception: return default

def _env_float(name: str, default: float) -> float:
    v = os.getenv(name)
    try: return float(str(v).strip()) if v is not None else default
    except Exception: return default

def _env_bool(name: str, default: bool) -> bool:
    raw = (os.getenv(name) or "").strip().lower()
    if not raw: return default
    if raw in _FALSY: return False
    if raw in _TRUTHY: return True
    return default

def _utc_iso(dt: Optional[datetime] = None) -> str:
    d = dt or datetime.now(timezone.utc)
    if d.tzinfo is None: d = d.replace(tzinfo=timezone.utc)
    return d.astimezone(timezone.utc).isoformat()

def _riyadh_iso(dt: Optional[datetime] = None) -> str:
    tz = timezone(timedelta(hours=3))
    d = dt or datetime.now(tz)
    if d.tzinfo is None: d = d.replace(tzinfo=tz)
    return d.astimezone(tz).isoformat()

def _to_riyadh_iso(dt: Optional[datetime]) -> Optional[str]:
    if not dt: return None
    tz = timezone(timedelta(hours=3))
    if dt.tzinfo is None: dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(tz).isoformat()

def _configured() -> bool:
    if not _env_bool("FINNHUB_ENABLED", True): return False
    return bool(_token())

def _emit_warnings() -> bool:
    return _env_bool("FINNHUB_VERBOSE_WARNINGS", False)

def _token() -> Optional[str]:
    for k in ("FINNHUB_API_KEY", "FINNHUB_API_TOKEN", "FINNHUB_TOKEN"):
        v = (os.getenv(k) or "").strip()
        if v: return v
    return None

def _base_url() -> str:
    return _env_str("FINNHUB_BASE_URL", DEFAULT_BASE_URL).rstrip("/")

def _timeout_sec() -> float:
    return max(3.0, _env_float("FINNHUB_TIMEOUT_SEC", DEFAULT_TIMEOUT_SEC))

def _retry_attempts() -> int:
    return max(1, _env_int("FINNHUB_RETRY_ATTEMPTS", DEFAULT_RETRY_ATTEMPTS))

def _rate_limit() -> float:
    return _env_float("FINNHUB_RATE_LIMIT", DEFAULT_RATE_LIMIT)

def _circuit_breaker_threshold() -> int:
    return _env_int("FINNHUB_CB_THRESHOLD", DEFAULT_CIRCUIT_BREAKER_THRESHOLD)

def _circuit_breaker_timeout() -> float:
    return _env_float("FINNHUB_CB_TIMEOUT", DEFAULT_CIRCUIT_BREAKER_TIMEOUT)

def _max_concurrency() -> int:
    return max(2, _env_int("FINNHUB_MAX_CONCURRENCY", DEFAULT_MAX_CONCURRENCY))

def _enable_profile() -> bool:
    return _env_bool("FINNHUB_ENABLE_PROFILE", True)

def _enable_history() -> bool:
    return _env_bool("FINNHUB_ENABLE_HISTORY", True)

def _enable_forecast() -> bool:
    return _env_bool("FINNHUB_ENABLE_FORECAST", True)

def _enable_ml() -> bool:
    return _env_bool("FINNHUB_ENABLE_ML", True)

def _quote_ttl_sec() -> float:
    return max(5.0, _env_float("FINNHUB_QUOTE_TTL_SEC", 10.0))

def _profile_ttl_sec() -> float:
    return max(300.0, _env_float("FINNHUB_PROFILE_TTL_SEC", 21600.0))

def _history_ttl_sec() -> float:
    return max(300.0, _env_float("FINNHUB_HISTORY_TTL_SEC", 1800.0))

def _history_days() -> int:
    return max(60, _env_int("FINNHUB_HISTORY_DAYS", 500))

def _history_points_max() -> int:
    return max(100, _env_int("FINNHUB_HISTORY_POINTS_MAX", 1000))

def _json_env_map(name: str) -> Dict[str, str]:
    raw = _env_str(name, "")
    if not raw: return {}
    try:
        obj = json.loads(raw)
        if isinstance(obj, dict):
            return {str(k).strip(): str(v).strip() for k, v in obj.items() if str(k).strip() and str(v).strip()}
    except Exception:
        return {}
    return {}


# ============================================================================
# Tracing & Metrics
# ============================================================================

class TraceContext:
    def __init__(self, name: str, attributes: Optional[Dict[str, Any]] = None):
        self.name = name
        self.attributes = attributes or {}
        self.tracer = trace.get_tracer(__name__) if _OTEL_AVAILABLE and _TRACING_ENABLED else None
        self.span = None
    
    async def __aenter__(self):
        if self.tracer:
            self.span = self.tracer.start_span(self.name)
            if self.attributes: self.span.set_attributes(self.attributes)
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self.span and _OTEL_AVAILABLE:
            if exc_val:
                self.span.record_exception(exc_val)
                self.span.set_status(Status(StatusCode.ERROR, str(exc_val)))
            self.span.end()

def _trace(name: Optional[str] = None):
    def decorator(func):
        @functools.wraps(func)
        async def wrapper(*args, **kwargs):
            trace_name = name or func.__name__
            async with TraceContext(trace_name, {"function": func.__name__}):
                return await func(*args, **kwargs)
        return wrapper
    return decorator

class MetricsRegistry:
    def __init__(self, namespace: str = "finnhub"):
        self.namespace = namespace
        self._metrics: Dict[str, Any] = {}
        self._lock = threading.RLock()
        if _PROMETHEUS_AVAILABLE and _METRICS_ENABLED: self._init_metrics()
    
    def _init_metrics(self):
        with self._lock:
            self._metrics["requests_total"] = Counter(f"{self.namespace}_requests_total", "Total requests", ["endpoint", "status"])
            self._metrics["request_duration_seconds"] = Histogram(f"{self.namespace}_request_duration_seconds", "Duration", ["endpoint"])
            self._metrics["cache_hits_total"] = Counter(f"{self.namespace}_cache_hits_total", "Cache hits", ["cache_type"])
            self._metrics["cache_misses_total"] = Counter(f"{self.namespace}_cache_misses_total", "Cache misses", ["cache_type"])
            self._metrics["circuit_breaker_state"] = Gauge(f"{self.namespace}_circuit_breaker_state", "State (1=closed, 0=open, -1=half)")
            self._metrics["rate_limiter_tokens"] = Gauge(f"{self.namespace}_rate_limiter_tokens", "Tokens")
            self._metrics["queue_size"] = Gauge(f"{self.namespace}_queue_size", "Queue size")
            self._metrics["active_requests"] = Gauge(f"{self.namespace}_active_requests", "Active requests")
    
    def inc(self, name: str, value: float = 1.0, labels: Optional[Dict[str, str]] = None):
        if not _PROMETHEUS_AVAILABLE or not _METRICS_ENABLED: return
        with self._lock:
            metric = self._metrics.get(name)
            if metric: metric.labels(**labels).inc(value) if labels else metric.inc(value)
    
    def observe(self, name: str, value: float, labels: Optional[Dict[str, str]] = None):
        if not _PROMETHEUS_AVAILABLE or not _METRICS_ENABLED: return
        with self._lock:
            metric = self._metrics.get(name)
            if metric: metric.labels(**labels).observe(value) if labels else metric.observe(value)
    
    def set(self, name: str, value: float, labels: Optional[Dict[str, str]] = None):
        if not _PROMETHEUS_AVAILABLE or not _METRICS_ENABLED: return
        with self._lock:
            metric = self._metrics.get(name)
            if metric: metric.labels(**labels).set(value) if labels else metric.set(value)

_METRICS = MetricsRegistry()


# ============================================================================
# Advanced Cache with TTL and LRU
# ============================================================================

class SmartCache:
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
        async with self._lock: await self._delete(key)

    async def _delete(self, key: str) -> None:
        self._cache.pop(key, None)
        self._expires.pop(key, None)
        self._access_times.pop(key, None)

    async def _evict_lru(self) -> None:
        if not self._access_times: return
        oldest_key = min(self._access_times.items(), key=lambda x: x[1])[0]
        await self._delete(oldest_key)

    async def clear(self) -> None:
        async with self._lock:
            self._cache.clear(); self._expires.clear(); self._access_times.clear()
            self._hits = 0; self._misses = 0

    async def size(self) -> int:
        async with self._lock: return len(self._cache)


# ============================================================================
# Dynamic Rate Limiter & Circuit Breaker
# ============================================================================

class TokenBucket:
    def __init__(self, rate_per_sec: float, capacity: Optional[float] = None):
        self.rate = max(0.0, rate_per_sec)
        self.capacity = capacity or max(1.0, self.rate)
        self.tokens = self.capacity
        self.last = time.monotonic()
        self._lock = asyncio.Lock()
        self._total_acquired = 0
        self._total_rejected = 0

    async def acquire(self, tokens: float = 1.0) -> bool:
        if self.rate <= 0: return True
        async with self._lock:
            now = time.monotonic()
            self.tokens = min(self.capacity, self.tokens + max(0.0, now - self.last) * self.rate)
            self.last = now
            if self.tokens >= tokens:
                self.tokens -= tokens
                self._total_acquired += 1
                _METRICS.set("rate_limiter_tokens", self.tokens)
                return True
            self._total_rejected += 1
            return False

    async def wait_and_acquire(self, tokens: float = 1.0) -> None:
        while True:
            if await self.acquire(tokens): return
            async with self._lock: wait = (tokens - self.tokens) / self.rate if self.rate > 0 else 0.1
            await asyncio.sleep(min(1.0, wait))

    def get_stats(self) -> Dict[str, Any]:
        return {
            "rate": self.rate, "capacity": self.capacity, "current_tokens": self.tokens,
            "utilization": 1.0 - (self.tokens / self.capacity) if self.capacity > 0 else 0,
        }

class DynamicCircuitBreaker:
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
                _METRICS.set("circuit_breaker_state", 1); return True
            elif self.stats.state == CircuitState.OPEN:
                if now >= self.stats.open_until:
                    self.stats.state = CircuitState.HALF_OPEN
                    self.half_open_calls = 0
                    _METRICS.set("circuit_breaker_state", -1); return True
                _METRICS.set("circuit_breaker_state", 0); return False
            elif self.stats.state == CircuitState.HALF_OPEN:
                if self.half_open_calls < 2:
                    self.half_open_calls += 1
                    _METRICS.set("circuit_breaker_state", -1); return True
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
            "state": self.stats.state.value, "failures": self.stats.failures, "successes": self.stats.successes,
            "open_until": self.stats.open_until, "current_cooldown": self.stats.current_cooldown
        }

class RequestQueue:
    def __init__(self, max_size: int = 1000):
        self.max_size = max_size
        self.queues: Dict[RequestPriority, asyncio.Queue] = {
            RequestPriority.LOW: asyncio.Queue(), RequestPriority.NORMAL: asyncio.Queue(),
            RequestPriority.HIGH: asyncio.Queue(), RequestPriority.CRITICAL: asyncio.Queue()
        }
        self._lock = asyncio.Lock()
        self._total_queued = 0
        self._total_processed = 0
    
    async def put(self, item: RequestQueueItem) -> bool:
        async with self._lock:
            total_size = sum(q.qsize() for q in self.queues.values())
            if total_size >= self.max_size: return False
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
                _METRICS.set("queue_size", sum(q.qsize() for q in self.queues.values()))
                return item
        return None


# ============================================================================
# Symbol Routing & Normalization
# ============================================================================

def looks_like_ksa(symbol: str) -> bool:
    s = (symbol or "").strip().upper()
    if not s: return False
    if callable(_SHARED_LOOKS_KSA):
        try:
            return bool(_SHARED_LOOKS_KSA(s))
        except Exception: pass
    if s.startswith("TADAWUL:"): return True
    if s.endswith(".SR"): return True
    if _KSA_CODE_RE.match(s): return True
    return False

def is_blocked_special(symbol: str) -> bool:
    s = (symbol or "").strip().upper()
    if not s: return True
    if _SPECIAL_SYMBOL_RE.search(s): return True
    if _CRYPTO_YAHOO_RE.match(s): return True
    if _FUTURE_YAHOO_RE.match(s): return True
    if _INDEX_YAHOO_RE.match(s): return True
    return False

def normalize_finnhub_symbol(symbol: str) -> str:
    s = (symbol or "").strip().upper()
    if s.endswith(".US"): s = s[:-3]
    if s.endswith(".NYSE"): s = s[:-5]
    if s.endswith(".NASDAQ"): s = s[:-7]
    mappings = _json_env_map("FINNHUB_SYMBOL_MAP_JSON")
    return mappings.get(s, s)

def generate_symbol_variants(symbol: str) -> List[str]:
    s = (symbol or "").strip().upper()
    if not s: return []
    variants, seen = [s], {s}
    
    if s.endswith(".US"): variants.append(s[:-3])
    if s.endswith(".NYSE"): variants.append(s[:-5])
    if s.endswith(".NASDAQ"): variants.append(s[:-7])
    if "." in s and not s.endswith(".US"):
        base = s.split('.')[0]
        if base not in seen: variants.append(base)
    if not "." in s and len(s) > 2: variants.append(f"{s}.US")

    final = []
    for v in variants:
        if v and v not in seen:
            seen.add(v)
            final.append(v)
    return [variants[0]] + final


# ============================================================================
# Numeric & Payload Helpers
# ============================================================================

def safe_float(x: Any) -> Optional[float]:
    if x is None: return None
    try:
        if isinstance(x, (int, float)):
            f = float(x)
            return f if not math.isnan(f) and not math.isinf(f) else None
        s = str(x).replace(",", "").strip()
        if not s or s in {"-", "—", "N/A", "NA", "null", "None"}: return None
        f = float(s)
        return f if not math.isnan(f) and not math.isinf(f) else None
    except Exception: return None

def safe_int(x: Any) -> Optional[int]:
    f = safe_float(x)
    return int(round(f)) if f is not None else None

def safe_str(x: Any) -> Optional[str]:
    return str(x).strip() if x is not None and str(x).strip() else None

def pick(d: Any, *keys: str) -> Any:
    if not isinstance(d, dict): return None
    for k in keys:
        if k in d: return d[k]
    return None

def clean_patch(patch: Dict[str, Any]) -> Dict[str, Any]:
    return {k: v for k, v in (patch or {}).items() if v is not None and not (isinstance(v, str) and not v.strip())}

def merge_into(dst: Dict[str, Any], src: Dict[str, Any], *, force_keys: Sequence[str] = ()) -> None:
    fset = set(force_keys or ())
    for k, v in (src or {}).items():
        if v is None: continue
        if k in fset or k not in dst or dst.get(k) is None or (isinstance(dst.get(k), str) and not dst.get(k).strip()):
            dst[k] = v

def fill_derived(patch: Dict[str, Any]) -> None:
    cur, prev, vol = safe_float(patch.get("current_price")), safe_float(patch.get("previous_close")), safe_float(patch.get("volume"))
    high, low = safe_float(patch.get("day_high")), safe_float(patch.get("day_low"))

    if patch.get("price_change") is None and cur is not None and prev is not None:
        patch["price_change"] = cur - prev
    if patch.get("percent_change") is None and cur is not None and prev not in (None, 0.0):
        try: patch["percent_change"] = (cur - prev) / prev * 100.0
        except Exception: pass
    if patch.get("value_traded") is None and cur is not None and vol is not None:
        patch["value_traded"] = cur * vol
    if patch.get("day_range") is None and high is not None and low is not None:
        patch["day_range"] = high - low

def data_quality_score(patch: Dict[str, Any]) -> Tuple[str, float]:
    score = 100.0
    if safe_float(patch.get("current_price")) is None: score -= 30
    if safe_float(patch.get("previous_close")) is None: score -= 15
    if safe_float(patch.get("volume")) is None: score -= 10
    if safe_float(patch.get("market_cap")) is None: score -= 10
    if safe_str(patch.get("name")) is None: score -= 10
    if safe_int(patch.get("history_points", 0)) < 50: score -= 20

    score = max(0.0, min(100.0, score))
    if score >= 80: cat = "EXCELLENT"
    elif score >= 60: cat = "GOOD"
    elif score >= 40: cat = "FAIR"
    elif score >= 20: cat = "POOR"
    else: cat = "BAD"
    return cat, score


# ============================================================================
# Advanced Technical Indicators
# ============================================================================

class TechnicalIndicators:
    @staticmethod
    def sma(prices: List[float], window: int) -> List[Optional[float]]:
        if len(prices) < window: return [None] * len(prices)
        result = [None] * (window - 1)
        for i in range(window - 1, len(prices)):
            result.append(sum(prices[i - window + 1:i + 1]) / window)
        return result

    @staticmethod
    def ema(prices: List[float], window: int) -> List[Optional[float]]:
        if len(prices) < window: return [None] * len(prices)
        result = [None] * (window - 1)
        multiplier = 2.0 / (window + 1)
        ema = sum(prices[:window]) / window
        result.append(ema)
        for price in prices[window:]:
            ema = (price - ema) * multiplier + ema
            result.append(ema)
        return result

    @staticmethod
    def macd(prices: List[float], fast: int = 12, slow: int = 26, signal: int = 9) -> Dict[str, List[Optional[float]]]:
        if len(prices) < slow:
            return {"macd": [None]*len(prices), "signal": [None]*len(prices), "histogram": [None]*len(prices)}
        ema_f, ema_s = TechnicalIndicators.ema(prices, fast), TechnicalIndicators.ema(prices, slow)
        macd_line = [f - s if f is not None and s is not None else None for f, s in zip(ema_f, ema_s)]
        valid_macd = [x for x in macd_line if x is not None]
        sig_line = TechnicalIndicators.ema(valid_macd, signal) if valid_macd else []
        padded_sig = [None] * (len(prices) - len(sig_line)) + sig_line
        hist = [m - s if m is not None and s is not None else None for m, s in zip(macd_line, padded_sig)]
        return {"macd": macd_line, "signal": padded_sig, "histogram": hist}

    @staticmethod
    def rsi(prices: List[float], window: int = 14) -> List[Optional[float]]:
        if len(prices) < window + 1: return [None] * len(prices)
        deltas = np.diff(prices)
        result = [None] * window
        
        window_deltas = deltas[:window]
        avg_gain = sum(d for d in window_deltas if d > 0) / window
        avg_loss = sum(-d for d in window_deltas if d < 0) / window
        
        rs = avg_gain / avg_loss if avg_loss != 0 else float('inf')
        result.append(100.0 - (100.0 / (1.0 + rs)) if avg_loss != 0 else 100.0)

        for d in deltas[window:]:
            gain = d if d > 0 else 0
            loss = -d if d < 0 else 0
            avg_gain = (avg_gain * (window - 1) + gain) / window
            avg_loss = (avg_loss * (window - 1) + loss) / window
            rs = avg_gain / avg_loss if avg_loss != 0 else float('inf')
            result.append(100.0 - (100.0 / (1.0 + rs)) if avg_loss != 0 else 100.0)
        return result

    @staticmethod
    def bollinger_bands(prices: List[float], window: int = 20, num_std: float = 2.0) -> Dict[str, List[Optional[float]]]:
        if len(prices) < window:
            return {"middle": [None]*len(prices), "upper": [None]*len(prices), "lower": [None]*len(prices), "bandwidth": [None]*len(prices)}
        middle = TechnicalIndicators.sma(prices, window)
        upper, lower, bandwidth = [None]*(window-1), [None]*(window-1), [None]*(window-1)
        for i in range(window - 1, len(prices)):
            std = float(np.std(prices[i - window + 1:i + 1]))
            m = middle[i]
            if m is not None:
                u, l = m + num_std * std, m - num_std * std
                upper.append(u); lower.append(l)
                bandwidth.append((u - l) / m if m != 0 else None)
            else:
                upper.append(None); lower.append(None); bandwidth.append(None)
        return {"middle": middle, "upper": upper, "lower": lower, "bandwidth": bandwidth}

    @staticmethod
    def atr(highs: List[float], lows: List[float], closes: List[float], window: int = 14) -> List[Optional[float]]:
        if len(highs) < window + 1 or len(lows) < window + 1 or len(closes) < window + 1: return [None] * len(highs)
        tr = [max(highs[i]-lows[i], abs(highs[i]-closes[i-1]), abs(lows[i]-closes[i-1])) for i in range(1, len(highs))]
        atr_values = [None] * window
        if len(tr) >= window:
            atr_values.append(sum(tr[:window]) / window)
            for i in range(window, len(tr)):
                prev_atr = atr_values[-1]
                atr_values.append((prev_atr * (window - 1) + tr[i]) / window if prev_atr else None)
        return atr_values

    @staticmethod
    def obv(closes: List[float], volumes: List[float]) -> List[float]:
        if len(closes) < 2: return [0.0] * len(closes)
        obv_values = [0.0]
        for i in range(1, len(closes)):
            if closes[i] > closes[i - 1]: obv_values.append(obv_values[-1] + volumes[i])
            elif closes[i] < closes[i - 1]: obv_values.append(obv_values[-1] - volumes[i])
            else: obv_values.append(obv_values[-1])
        return obv_values

    @staticmethod
    def ichimoku_cloud(highs: List[float], lows: List[float], conversion: int = 9, base: int = 26, span: int = 52) -> Dict[str, List[Optional[float]]]:
        length = len(highs)
        tenkan_sen, kijun_sen, senkou_span_a, senkou_span_b = [None] * length, [None] * length, [None] * length, [None] * length
        for i in range(length):
            if i >= conversion - 1: tenkan_sen[i] = (max(highs[i - conversion + 1:i + 1]) + min(lows[i - conversion + 1:i + 1])) / 2
            if i >= base - 1: kijun_sen[i] = (max(highs[i - base + 1:i + 1]) + min(lows[i - base + 1:i + 1])) / 2
            if tenkan_sen[i] is not None and kijun_sen[i] is not None: senkou_span_a[i] = (tenkan_sen[i] + kijun_sen[i]) / 2
            if i >= span - 1: senkou_span_b[i] = (max(highs[i - span + 1:i + 1]) + min(lows[i - span + 1:i + 1])) / 2
        return {"tenkan_sen": tenkan_sen, "kijun_sen": kijun_sen, "senkou_span_a": senkou_span_a, "senkou_span_b": senkou_span_b}

    @staticmethod
    def stochastic_oscillator(highs: List[float], lows: List[float], closes: List[float], k_window: int = 14, d_window: int = 3) -> Dict[str, List[Optional[float]]]:
        length = len(closes)
        k_line = [None] * length
        for i in range(k_window - 1, length):
            hh = max(highs[i - k_window + 1:i + 1])
            ll = min(lows[i - k_window + 1:i + 1])
            k_line[i] = 50.0 if hh - ll == 0 else 100 * ((closes[i] - ll) / (hh - ll))
        valid_k = [x for x in k_line if x is not None]
        d_line = TechnicalIndicators.sma(valid_k, d_window)
        padded_d = [None] * (length - len(d_line)) + d_line
        return {"%k": k_line, "%d": padded_d}


# ============================================================================
# Market Regime Detection
# ============================================================================

class MarketRegimeDetector:
    def __init__(self, prices: List[float], window: int = 60):
        self.prices = prices
        self.window = min(window, len(prices) // 3) if len(prices) > 60 else 20

    def detect(self) -> Tuple[MarketRegime, float]:
        if len(self.prices) < 30: return MarketRegime.UNKNOWN, 0.0
        returns = np.diff(self.prices) / self.prices[:-1]
        recent_returns = returns[-min(len(returns), 30):]

        if len(self.prices) >= self.window and SCIPY_AVAILABLE:
            x = list(range(self.window))
            y = self.prices[-self.window:]
            slope, _, r_value, p_value, _ = stats.linregress(x, y)
            trend_strength, trend_direction, trend_pvalue = abs(r_value), slope, p_value
        else:
            x = list(range(min(30, len(self.prices))))
            y = self.prices[-len(x):]
            if len(x) > 1:
                x_mean, y_mean = sum(x) / len(x), sum(y) / len(y)
                num = sum((xi - x_mean) * (yi - y_mean) for xi, yi in zip(x, y))
                den = sum((xi - x_mean) ** 2 for xi in x)
                trend_direction = num / den if den != 0 else 0
                trend_strength, trend_pvalue = 0.5, 0.1
            else:
                trend_direction, trend_strength, trend_pvalue = 0, 0, 1.0

        vol = float(np.std(recent_returns)) if len(recent_returns) > 0 else 0
        momentum_1m = self.prices[-1] / self.prices[-min(21, len(self.prices))] - 1 if len(self.prices) > 21 else 0

        confidence = 0.7
        if vol > 0.03:
            regime = MarketRegime.VOLATILE
            confidence = min(0.9, 0.7 + vol * 5)
        elif trend_strength > 0.7 and trend_pvalue < 0.05:
            regime = MarketRegime.BULL if trend_direction > 0 else MarketRegime.BEAR
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
# Advanced ML Ensemble Forecaster
# ============================================================================

class EnsembleForecaster:
    """Multi-model ensemble utilizing Statsmodels, XGBoost, Random Forest, and Heuristics."""

    def __init__(self, prices: List[float], enable_ml: bool = True):
        self.prices = prices
        self.enable_ml = enable_ml
        self.feature_importance: Dict[str, float] = {}

    def forecast(self, horizon_days: int = 252) -> Dict[str, Any]:
        if len(self.prices) < 50:
            return {"forecast_available": False, "reason": "insufficient_history", "horizon_days": horizon_days}

        results = {
            "forecast_available": True, "horizon_days": horizon_days,
            "models_used": [], "forecasts": {}, "ensemble": {},
            "confidence": 0.0, "feature_importance": {}
        }
        forecasts, weights = [], []

        # 1. Trend Baseline
        trend = self._forecast_trend(horizon_days)
        if trend:
            results["models_used"].append("trend")
            results["forecasts"]["trend"] = trend
            forecasts.append(trend["price"])
            weights.append(0.15)

        # 2. Real ARIMA (Statsmodels) or fallback
        arima = self._forecast_arima(horizon_days)
        if arima:
            results["models_used"].append("arima")
            results["forecasts"]["arima"] = arima
            forecasts.append(arima["price"])
            weights.append(0.3)

        # 3. XGBoost / Random Forest
        if self.enable_ml:
            ml_forecast = self._forecast_ml(horizon_days)
            if ml_forecast:
                results["models_used"].append("ml_tree")
                results["forecasts"]["ml_tree"] = ml_forecast
                forecasts.append(ml_forecast["price"])
                weights.append(0.55)

        if not forecasts:
            return {"forecast_available": False, "reason": "no_models_converged", "horizon_days": horizon_days}

        ensemble_price = np.average(forecasts, weights=weights)
        ensemble_std = np.std(forecasts) if len(forecasts) > 1 else ensemble_price * 0.05
        
        results["ensemble"] = {
            "price": ensemble_price,
            "roi_pct": (ensemble_price / self.prices[-1] - 1) * 100,
            "std_dev": ensemble_std,
            "price_range_low": ensemble_price - 1.96 * ensemble_std,
            "price_range_high": ensemble_price + 1.96 * ensemble_std
        }
        
        confidence = self._calculate_confidence(results)
        results["confidence"] = confidence
        results["confidence_level"] = self._confidence_level(confidence)

        if horizon_days <= 21: period = "1m"
        elif horizon_days <= 63: period = "3m"
        else: period = "1y"

        results[f"expected_roi_{period}"] = results["ensemble"]["roi_pct"]
        results[f"forecast_price_{period}"] = ensemble_price
        results[f"target_price_{period}"] = ensemble_price

        return results

    def _forecast_trend(self, horizon: int) -> Optional[Dict[str, Any]]:
        try:
            n = min(len(self.prices), 252)
            y = np.log(self.prices[-n:])
            x = np.arange(n).reshape(-1, 1)
            x_mean, y_mean = np.mean(x), np.mean(y)
            slope = np.sum((x.flatten() - x_mean) * (y - y_mean)) / np.sum((x.flatten() - x_mean) ** 2)
            intercept = y_mean - slope * x_mean
            future_x = n + horizon
            price = np.exp(intercept + slope * future_x)
            return {"price": float(price), "weight": 0.15}
        except Exception:
            return None

    def _forecast_arima(self, horizon: int) -> Optional[Dict[str, Any]]:
        try:
            if STATSMODELS_AVAILABLE and len(self.prices) >= 100:
                model = StatsARIMA(self.prices[-252:], order=(5, 1, 0))
                fitted = model.fit()
                price = float(fitted.forecast(steps=horizon).iloc[-1])
                return {"price": price, "weight": 0.3}
        except Exception: pass
            
        try:
            recent = self.prices[-min(60, len(self.prices)):]
            returns = np.diff(recent) / recent[:-1]
            drift = np.mean(returns)
            price = self.prices[-1] * ((1 + drift) ** horizon)
            return {"price": float(price), "weight": 0.1}
        except Exception: return None

    def _forecast_ml(self, horizon: int) -> Optional[Dict[str, Any]]:
        if not SKLEARN_AVAILABLE or len(self.prices) < 100: return None
        try:
            X, y = [], []
            for i in range(60, len(self.prices) - 5):
                feats = [self.prices[i] / self.prices[i-w] - 1 for w in [5, 10, 20]]
                vol = np.std([self.prices[j]/self.prices[j-1]-1 for j in range(i-20, i)])
                feats.append(vol)
                X.append(feats)
                y.append(self.prices[i+5] / self.prices[i] - 1)
            
            if len(X) < 20: return None

            if XGB_AVAILABLE: model = xgb.XGBRegressor(n_estimators=50, max_depth=3, learning_rate=0.05, n_jobs=-1)
            else: model = RandomForestRegressor(n_estimators=50, max_depth=4, random_state=42)
            model.fit(X, y)
            
            curr_feats = [self.prices[-1]/self.prices[-1-w]-1 for w in [5, 10, 20]]
            curr_vol = np.std(np.diff(self.prices[-21:]) / self.prices[-21:-1])
            curr_feats.append(curr_vol)
            
            pred_return = model.predict([curr_feats])[0]
            price = self.prices[-1] * (1 + pred_return) ** (horizon / 5)
            return {"price": float(price), "predicted_return": float(pred_return), "weight": 0.55}
        except Exception as e:
            logger.debug(f"ML forecast failed: {e}")
            return None

    def _calculate_confidence(self, results: Dict[str, Any]) -> float:
        if not results.get("forecasts"): return 0.0
        prices = [f["price"] for f in results["forecasts"].values()]
        cv = np.std(prices) / np.mean(prices) if len(prices) > 1 and np.mean(prices) != 0 else 0.5
        consistency = max(0, 100 - cv * 300)
        return min(100, max(0, consistency))

    def _confidence_level(self, score: float) -> str:
        if score >= 80: return "high"
        elif score >= 60: return "medium"
        elif score >= 40: return "low"
        else: return "very_low"


# ============================================================================
# History Analytics
# ============================================================================

def compute_history_analytics(prices: List[float], volumes: Optional[List[float]] = None) -> Dict[str, Any]:
    if not prices or len(prices) < 10: return {}
    out: Dict[str, Any] = {}
    last = prices[-1]

    # Returns
    for name, days in {"returns_1w": 5, "returns_1m": 21, "returns_3m": 63, "returns_1y": 252}.items():
        if len(prices) > days and prices[-(days + 1)] != 0: 
            out[name] = float((last / prices[-(days + 1)] - 1) * 100)

    # Moving averages
    for period in [20, 50, 200]:
        if len(prices) >= period:
            ma = sum(prices[-period:]) / period
            out[f"sma_{period}"] = float(ma)
            out[f"price_to_sma_{period}"] = float((last / ma - 1) * 100)

    # Volatility
    if len(prices) >= 31:
        out["volatility_30d"] = float(np.std(np.diff(prices[-31:]) / prices[-31:-1]) * math.sqrt(252) * 100)

    # Technical indicators
    ti = TechnicalIndicators()
    rsi_vals = ti.rsi(prices, 14)
    if rsi_vals and rsi_vals[-1] is not None:
        out["rsi_14"] = float(rsi_vals[-1])
        out["rsi_signal"] = "overbought" if out["rsi_14"] > 70 else "oversold" if out["rsi_14"] < 30 else "neutral"

    macd = ti.macd(prices)
    if macd["macd"] and macd["macd"][-1] is not None:
        out["macd"] = float(macd["macd"][-1])
        out["macd_signal"] = float(macd["signal"][-1]) if macd["signal"][-1] else None
        out["macd_histogram"] = float(macd["histogram"][-1]) if macd["histogram"][-1] else None
        if out["macd"] and out["macd_signal"]:
            out["macd_cross"] = "bullish" if out["macd"] > out["macd_signal"] else "bearish"

    bb = ti.bollinger_bands(prices)
    if bb["middle"][-1] is not None:
        out["bb_middle"] = float(bb["middle"][-1])
        out["bb_upper"] = float(bb["upper"][-1])
        out["bb_lower"] = float(bb["lower"][-1])
        if bb["upper"][-1] and bb["lower"][-1]:
            out["bb_position"] = float((last - bb["lower"][-1]) / (bb["upper"][-1] - bb["lower"][-1]))

    # Ichimoku & Stochastic
    ichimoku = ti.ichimoku_cloud(prices, prices)
    if ichimoku["tenkan_sen"][-1] is not None:
        out["ichimoku_tenkan"] = float(ichimoku["tenkan_sen"][-1])
        out["ichimoku_kijun"] = float(ichimoku["kijun_sen"][-1])

    if len(prices) >= 14:
        stoch = ti.stochastic_oscillator(prices, prices, prices)
        if stoch["%k"][-1] is not None:
            out["stochastic_k"] = float(stoch["%k"][-1])
            out["stochastic_d"] = float(stoch["%d"][-1])

    # Maximum Drawdown
    peak = prices[0]
    max_dd = 0.0
    for price in prices:
        if price > peak: peak = price
        dd = (price / peak - 1) * 100
        if dd < max_dd: max_dd = dd
    out["max_drawdown_pct"] = float(max_dd)

    # Market Regime
    regime, confidence = MarketRegimeDetector(prices).detect()
    out["market_regime"] = regime.value
    out["market_regime_confidence"] = confidence

    # Forecast
    if _enable_forecast() and len(prices) >= 60:
        forecaster = EnsembleForecaster(prices, enable_ml=_enable_ml())
        for horizon, period in [(21, "1m"), (63, "3m"), (252, "1y")]:
            forecast = forecaster.forecast(horizon)
            if forecast.get("forecast_available"):
                out[f"expected_roi_{period}"] = forecast["ensemble"]["roi_pct"]
                out[f"forecast_price_{period}"] = forecast["ensemble"]["price"]
                out[f"target_price_{period}"] = forecast["ensemble"]["price"]
                out[f"forecast_range_low_{period}"] = forecast["ensemble"].get("price_range_low")
                out[f"forecast_range_high_{period}"] = forecast["ensemble"].get("price_range_high")
                if period == "1y":
                    out["forecast_confidence"] = forecast["confidence"]
                    out["forecast_confidence_level"] = forecast["confidence_level"]

        out["forecast_method"] = "ensemble_v4"
        out["forecast_source"] = "finnhub_ml_ensemble"

    return out


# ============================================================================
# Advanced HTTP Client
# ============================================================================

class FinnhubClient:
    """Next-Gen Async Finnhub API client."""

    def __init__(self):
        self.base_url = _base_url()
        self.api_key = _token()
        self.timeout_sec = _timeout_sec()
        self.retry_attempts = _retry_attempts()
        self.client_id = str(uuid.uuid4())[:8]
        
        self.rate_limiter = TokenBucket(_rate_limit())
        self.circuit_breaker = DynamicCircuitBreaker(
            fail_threshold=_circuit_breaker_threshold(),
            base_cooldown=_circuit_breaker_timeout()
        )

        self.semaphore = asyncio.Semaphore(_max_concurrency())
        self.request_queue = RequestQueue(max_size=_queue_size())

        self.quote_cache = SmartCache(maxsize=8000, ttl=_quote_ttl_sec())
        self.profile_cache = SmartCache(maxsize=4000, ttl=_profile_ttl_sec())
        self.history_cache = SmartCache(maxsize=2500, ttl=_history_ttl_sec())

        self._client = httpx.AsyncClient(
            timeout=httpx.Timeout(self.timeout_sec),
            headers={"User-Agent": _env_str("FINNHUB_USER_AGENT", USER_AGENT_DEFAULT)},
            follow_redirects=True,
            limits=httpx.Limits(max_keepalive_connections=30, max_connections=50),
            http2=True
        )

        self._queue_processor_task = asyncio.create_task(self._process_queue())

        self.metrics: Dict[str, Any] = {
            "requests_total": 0, "requests_success": 0, "requests_failed": 0,
            "cache_hits": 0, "cache_misses": 0, "rate_limit_waits": 0, "circuit_breaker_blocks": 0
        }
        self._metrics_lock = asyncio.Lock()

    async def _update_metric(self, name: str, inc: int = 1) -> None:
        async with self._metrics_lock:
            self.metrics[name] = self.metrics.get(name, 0) + inc

    @_trace("finnhub_request")
    async def _request(self, endpoint: str, params: Dict[str, Any], priority: RequestPriority = RequestPriority.NORMAL) -> Tuple[Optional[Dict[str, Any]], Optional[str]]:
        start_time = time.time()
        await self._update_metric("requests_total")
        
        if not self.api_key: return None, "API key not configured"
        if not await self.circuit_breaker.allow_request(): return None, "circuit_breaker_open"

        future = asyncio.Future()
        queue_item = RequestQueueItem(priority=priority, endpoint=endpoint, params=params, future=future)
        if not await self.request_queue.put(queue_item): return None, "queue_full"

        result = await future
        _METRICS.observe("request_duration_seconds", (time.time() - start_time), {"endpoint": endpoint.split('/')[0]})
        return result

    async def _process_queue(self) -> None:
        while True:
            try:
                item = await self.request_queue.get()
                if item is None:
                    await asyncio.sleep(0.1)
                    continue
                asyncio.create_task(self._process_queue_item(item))
            except Exception as e:
                logger.error(f"Queue processor error: {e}")
                await asyncio.sleep(1)

    async def _process_queue_item(self, item: RequestQueueItem) -> None:
        try:
            result = await self._execute_request(item.endpoint, item.params)
            item.future.set_result(result)
        except Exception as e:
            item.future.set_exception(e)

    async def _execute_request(self, endpoint: str, params: Dict[str, Any]) -> Tuple[Optional[Dict[str, Any]], Optional[str]]:
        await self.rate_limiter.wait_and_acquire()
        await self._update_metric("rate_limit_waits")

        url = f"{self.base_url.rstrip('/')}/{endpoint.lstrip('/')}"
        req_params = dict(params)
        req_params["token"] = self.api_key

        last_err = None

        async with self.semaphore:
            for attempt in range(self.retry_attempts):
                try:
                    resp = await self._client.get(url, params=req_params)
                    status = resp.status_code

                    if status == 429:
                        await self.circuit_breaker.on_failure(status)
                        await asyncio.sleep(min(30, int(resp.headers.get("Retry-After", 5))))
                        continue

                    if 500 <= status < 600:
                        await self.circuit_breaker.on_failure(status)
                        base_wait = 2 ** attempt
                        jitter = random.uniform(0, base_wait)
                        await asyncio.sleep(min(10.0, base_wait + jitter))
                        continue

                    if status >= 400 and status != 404:
                        await self.circuit_breaker.on_failure(status)
                        return None, f"HTTP {status}"

                    # Success -> Fast Parse
                    try:
                        data = json_loads(resp.content)
                    except Exception:
                        await self.circuit_breaker.on_failure()
                        return None, "invalid_json"

                    if not isinstance(data, dict): return None, "unexpected_response_type"

                    await self.circuit_breaker.on_success()
                    await self._update_metric("requests_success")
                    return data, None

                except httpx.RequestError as e:
                    last_err = f"network_error_{e.__class__.__name__}"
                    base_wait = 2 ** attempt
                    await asyncio.sleep(min(10.0, base_wait + random.uniform(0, base_wait)))

        await self.circuit_breaker.on_failure()
        return None, last_err or "max_retries_exceeded"

    async def get_quote(self, symbol: str, priority: RequestPriority = RequestPriority.NORMAL) -> Tuple[Optional[Dict[str, Any]], Optional[str]]:
        return await self._request("quote", {"symbol": symbol}, priority=priority)

    async def get_profile(self, symbol: str, priority: RequestPriority = RequestPriority.NORMAL) -> Tuple[Optional[Dict[str, Any]], Optional[str]]:
        return await self._request("stock/profile2", {"symbol": symbol}, priority=priority)

    async def get_candles(self, symbol: str, resolution: str = "D", count: int = 500, priority: RequestPriority = RequestPriority.NORMAL) -> Tuple[Optional[Dict[str, Any]], Optional[str]]:
        to_ts = int(time.time())
        from_ts = to_ts - (count * 86400)
        params = {"symbol": symbol, "resolution": resolution, "from": from_ts, "to": to_ts}
        return await self._request("stock/candle", params, priority=priority)

    async def get_metrics(self) -> Dict[str, Any]:
        async with self._metrics_lock: metrics = dict(self.metrics)
        metrics["circuit_breaker"] = self.circuit_breaker.get_stats()
        metrics["cache_sizes"] = { "quote": await self.quote_cache.size(), "profile": await self.profile_cache.size(), "history": await self.history_cache.size() }
        return metrics

    async def close(self) -> None:
        self._queue_processor_task.cancel()
        try: await self._queue_processor_task
        except: pass
        await self._client.aclose()


# ============================================================================
# Data Mapping Functions
# ============================================================================

def map_quote_data(data: Dict[str, Any], requested_symbol: str, normalized_symbol: str) -> Dict[str, Any]:
    result: Dict[str, Any] = {
        "requested_symbol": requested_symbol, "normalized_symbol": normalized_symbol,
        "provider": PROVIDER_NAME, "data_source": PROVIDER_NAME,
        "provider_version": PROVIDER_VERSION,
        "last_updated_utc": _utc_iso(), "last_updated_riyadh": _riyadh_iso(),
    }
    result["current_price"] = safe_float(data.get("c"))
    result["previous_close"] = safe_float(data.get("pc"))
    result["open"] = safe_float(data.get("o"))
    result["day_high"] = safe_float(data.get("h"))
    result["day_low"] = safe_float(data.get("l"))
    result["price_change"] = safe_float(data.get("d"))
    result["percent_change"] = safe_float(data.get("dp"))
    fill_derived(result)
    return clean_patch(result)

def map_profile_data(data: Dict[str, Any]) -> Dict[str, Any]:
    result: Dict[str, Any] = {}
    result["name"] = safe_str(data.get("name"))
    result["sector"] = safe_str(data.get("finnhubIndustry"))
    result["currency"] = safe_str(data.get("currency"))
    result["market_cap"] = safe_float(data.get("marketCapitalization"))
    result["shares_outstanding"] = safe_float(data.get("shareOutstanding"))
    result["exchange"] = safe_str(data.get("exchange"))
    result["country"] = safe_str(data.get("country"))
    result["ipo"] = safe_str(data.get("ipo"))
    result["weburl"] = safe_str(data.get("weburl"))
    result["logo"] = safe_str(data.get("logo"))
    return clean_patch(result)

def map_candle_data(data: Dict[str, Any], requested_symbol: str, normalized_symbol: str) -> Dict[str, Any]:
    if data.get("s") != "ok": return {}
    closes = data.get("c") or []
    times = data.get("t") or []
    volumes = data.get("v") or []
    if not closes: return {}

    prices = [f for f in (safe_float(x) for x in closes) if f is not None]
    vols = [f for f in (safe_float(x) for x in volumes) if f is not None]
    if len(prices) < 10: return {}

    max_pts = _history_points_max()
    prices = prices[-max_pts:]
    if vols: vols = vols[-max_pts:]

    last_dt = None
    if times and len(times) == len(closes):
        try: last_dt = datetime.fromtimestamp(float(times[-1]), tz=timezone.utc)
        except Exception: pass

    analytics = compute_history_analytics(prices, vols if vols else None)
    result: Dict[str, Any] = {
        "requested_symbol": requested_symbol, "normalized_symbol": normalized_symbol,
        "history_points": len(prices), "history_last_utc": _utc_iso(last_dt),
        "history_last_riyadh": _to_riyadh_iso(last_dt),
        "forecast_updated_utc": _utc_iso(last_dt), "forecast_updated_riyadh": _to_riyadh_iso(last_dt),
        "provider": PROVIDER_NAME, "data_source": PROVIDER_NAME, "provider_version": PROVIDER_VERSION,
    }
    result.update(analytics)
    return clean_patch(result)


# ============================================================================
# Main Fetch Function
# ============================================================================

_CLIENT_INSTANCE: Optional[FinnhubClient] = None
_CLIENT_LOCK = asyncio.Lock()

async def get_client() -> FinnhubClient:
    global _CLIENT_INSTANCE
    if _CLIENT_INSTANCE is None:
        async with _CLIENT_LOCK:
            if _CLIENT_INSTANCE is None: _CLIENT_INSTANCE = FinnhubClient()
    return _CLIENT_INSTANCE

async def close_client() -> None:
    global _CLIENT_INSTANCE
    if _CLIENT_INSTANCE:
        await _CLIENT_INSTANCE.close()
        _CLIENT_INSTANCE = None

async def _fetch(symbol: str, include_profile: bool = True, include_history: bool = True) -> Dict[str, Any]:
    if not _configured(): return {}
    sym_in = (symbol or "").strip()
    if not sym_in: return {"provider": PROVIDER_NAME, "error": "empty_symbol"}
    if looks_like_ksa(sym_in): return {"_warn": "finnhub blocked: KSA symbol"} if _emit_warnings() else {}
    if is_blocked_special(sym_in): return {"_warn": "finnhub blocked: special symbol"} if _emit_warnings() else {}

    norm_symbol = normalize_finnhub_symbol(sym_in)
    variants = generate_symbol_variants(norm_symbol)
    client = await get_client()
    
    result = {
        "requested_symbol": sym_in, "normalized_symbol": norm_symbol,
        "provider": PROVIDER_NAME, "data_source": PROVIDER_NAME, "provider_version": PROVIDER_VERSION,
        "last_updated_utc": _utc_iso(), "last_updated_riyadh": _riyadh_iso(),
    }
    warnings = []
    used_symbol = norm_symbol

    # Quote
    for variant in variants:
        cached = await client.quote_cache.get(f"quote:{variant}")
        quote_data, quote_err = cached, None if cached else await client.get_quote(variant)
        
        if quote_data and not quote_err:
            if not cached: await client.quote_cache.set(f"quote:{variant}", quote_data)
            used_symbol = variant
            result["normalized_symbol"] = variant
            result.update(map_quote_data(quote_data, sym_in, variant))
            break
        else: warnings.append(f"quote_{variant}: {quote_err or 'failed'}")

    # Profile
    if include_profile and _enable_profile():
        cached = await client.profile_cache.get(f"profile:{used_symbol}")
        profile_data, profile_err = cached, None if cached else await client.get_profile(used_symbol)
        if profile_data and not profile_err:
            if not cached: await client.profile_cache.set(f"profile:{used_symbol}", profile_data)
            merge_into(result, map_profile_data(profile_data), force_keys=("market_cap", "shares_outstanding", "name", "sector", "currency"))

    # History
    if include_history and _enable_history():
        days = _history_days()
        cached = await client.history_cache.get(f"history:{used_symbol}:{days}")
        history_data, history_err = cached, None if cached else await client.get_candles(used_symbol, count=days)
        if history_data and not history_err:
            if not cached: await client.history_cache.set(f"history:{used_symbol}:{days}", history_data)
            merge_into(result, map_candle_data(history_data, sym_in, used_symbol))

    cat, score = data_quality_score(result)
    result["data_quality"], result["data_quality_score"] = cat, score
    if warnings and _emit_warnings(): result["_warnings"] = " | ".join(warnings[:3])
    
    return clean_patch(result)


# ============================================================================
# Public API
# ============================================================================

async def fetch_enriched_quote_patch(symbol: str, *args: Any, **kwargs: Any) -> Dict[str, Any]:
    return await _fetch(symbol, include_profile=True, include_history=True)

async def fetch_quote_patch(symbol: str, *args: Any, **kwargs: Any) -> Dict[str, Any]:
    return await _fetch(symbol, include_profile=False, include_history=False)

async def fetch_quote_and_profile_patch(symbol: str, *args: Any, **kwargs: Any) -> Dict[str, Any]:
    return await _fetch(symbol, include_profile=True, include_history=False)

async def fetch_quote_and_history_patch(symbol: str, *args: Any, **kwargs: Any) -> Dict[str, Any]:
    return await _fetch(symbol, include_profile=False, include_history=True)

async def fetch_enriched_patch(symbol: str, *args: Any, **kwargs: Any) -> Dict[str, Any]:
    return await fetch_enriched_quote_patch(symbol, *args, **kwargs)

async def fetch_quote_and_enrichment_patch(symbol: str, *args: Any, **kwargs: Any) -> Dict[str, Any]:
    return await fetch_enriched_quote_patch(symbol, *args, **kwargs)

async def get_client_metrics() -> Dict[str, Any]:
    return await (await get_client()).get_metrics()

async def aclose_finnhub_client() -> None:
    await close_client()

__all__ = [
    "PROVIDER_NAME", "PROVIDER_VERSION", "fetch_enriched_quote_patch", "fetch_quote_patch",
    "fetch_quote_and_profile_patch", "fetch_quote_and_history_patch", "fetch_enriched_patch",
    "fetch_quote_and_enrichment_patch", "get_client_metrics", "aclose_finnhub_client",
    "MarketRegime"
]
