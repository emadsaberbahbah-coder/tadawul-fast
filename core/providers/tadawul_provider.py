#!/usr/bin/env python3
# core/providers/tadawul_provider.py
"""
================================================================================
Tadawul Provider (KSA Market Data) — v4.1.0 (Next-Gen Enterprise + ML)
================================================================================

What's new in v4.1.0:
- ✅ ADDED: XGBoost integration for superior ML forecasting accuracy.
- ✅ ADDED: Actual statsmodels ARIMA integration (with random-walk fallback).
- ✅ ADDED: High-performance JSON parsing via `orjson` (if available).
- ✅ ADDED: Exponential Backoff with 'Full Jitter' to prevent thundering herds.
- ✅ ADDED: Dynamic Circuit Breaker with progressive timeout scaling.
- ✅ ADDED: Ichimoku Cloud and Stochastic Oscillator indicators.
- ✅ ENHANCED: Request Queuing with Priority levels and strict Semaphores.
- ✅ ENHANCED: Memory management using zero-copy slicing where possible.
- ✅ ENHANCED: Market Regime Detection using statistical rolling distributions.
- ✅ ENHANCED: Singleflight pattern integrated seamlessly with Priority Queues.

Key Features:
- KSA market data (Tadawul)
- Real-time quotes with derived fields
- Fundamentals (market cap, P/E, dividend yield, etc.)
- Historical data with full technical analysis
- Ensemble forecasts with confidence levels
- Production-grade error handling
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
    from scipy.signal import savgol_filter
    SCIPY_AVAILABLE = True
except ImportError:
    SCIPY_AVAILABLE = False

try:
    from sklearn.linear_model import LinearRegression, Ridge
    from sklearn.preprocessing import StandardScaler
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

logger = logging.getLogger("core.providers.tadawul_provider")

PROVIDER_NAME = "tadawul"
PROVIDER_VERSION = "4.1.0"

USER_AGENT_DEFAULT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/124.0.0.0 Safari/537.36"
)

DEFAULT_TIMEOUT_SEC = 25.0
DEFAULT_RETRY_ATTEMPTS = 4
DEFAULT_MAX_CONCURRENCY = 40
DEFAULT_RATE_LIMIT = 15.0  # requests per second
DEFAULT_CIRCUIT_BREAKER_THRESHOLD = 5
DEFAULT_CIRCUIT_BREAKER_TIMEOUT = 30.0
DEFAULT_QUEUE_SIZE = 2000

_ARABIC_DIGITS = str.maketrans("٠١٢٣٤٥٦٧٨٩", "0123456789")
_TRUTHY = {"1", "true", "yes", "y", "on", "t"}
_FALSY = {"0", "false", "no", "n", "off", "f"}

_KSA_CODE_RE = re.compile(r"^\d{3,6}$", re.IGNORECASE)

_TRACING_ENABLED = os.getenv("TADAWUL_TRACING_ENABLED", "").strip().lower() in _TRUTHY
_METRICS_ENABLED = os.getenv("TADAWUL_METRICS_ENABLED", "").strip().lower() in _TRUTHY


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

class DataQuality(Enum):
    EXCELLENT = "excellent"
    GOOD = "good"
    FAIR = "fair"
    POOR = "poor"
    BAD = "bad"

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
    url: str
    future: asyncio.Future
    created_at: float = field(default_factory=time.time)


# ============================================================================
# Shared Normalizer Integration
# ============================================================================

def _try_import_shared_ksa_helpers() -> Tuple[Optional[Any], Optional[Any]]:
    try:
        from core.symbols.normalize import normalize_ksa_symbol as _nksa
        from core.symbols.normalize import looks_like_ksa as _lk
        return _nksa, _lk
    except Exception:
        return None, None

_SHARED_NORM_KSA, _SHARED_LOOKS_KSA = _try_import_shared_ksa_helpers()


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

def _safe_str(x: Any) -> Optional[str]:
    if x is None: return None
    s = str(x).strip()
    return s if s else None

def _configured() -> bool:
    if not _env_bool("TADAWUL_ENABLED", True): return False
    return bool(_safe_str(_env_str("TADAWUL_QUOTE_URL", "")))

def _emit_warnings() -> bool:
    return _env_bool("TADAWUL_VERBOSE_WARNINGS", False)

def _enable_fundamentals() -> bool:
    return _env_bool("TADAWUL_ENABLE_FUNDAMENTALS", True)

def _enable_history() -> bool:
    return _env_bool("TADAWUL_ENABLE_HISTORY", True)

def _enable_forecast() -> bool:
    return _env_bool("TADAWUL_ENABLE_FORECAST", True)

def _enable_ml() -> bool:
    return _env_bool("TADAWUL_ENABLE_ML", True)

def _timeout_sec() -> float:
    return max(5.0, _env_float("TADAWUL_TIMEOUT_SEC", DEFAULT_TIMEOUT_SEC))

def _retry_attempts() -> int:
    return max(1, _env_int("TADAWUL_RETRY_ATTEMPTS", DEFAULT_RETRY_ATTEMPTS))

def _rate_limit() -> float:
    return _env_float("TADAWUL_RATE_LIMIT", DEFAULT_RATE_LIMIT)

def _circuit_breaker_threshold() -> int:
    return _env_int("TADAWUL_CB_THRESHOLD", DEFAULT_CIRCUIT_BREAKER_THRESHOLD)

def _circuit_breaker_timeout() -> float:
    return _env_float("TADAWUL_CB_TIMEOUT", DEFAULT_CIRCUIT_BREAKER_TIMEOUT)

def _max_concurrency() -> int:
    return max(2, _env_int("TADAWUL_MAX_CONCURRENCY", DEFAULT_MAX_CONCURRENCY))

def _queue_size() -> int:
    return _env_int("TADAWUL_QUEUE_SIZE", DEFAULT_QUEUE_SIZE)

def _quote_ttl_sec() -> float:
    return max(5.0, _env_float("TADAWUL_QUOTE_TTL_SEC", 15.0))

def _fund_ttl_sec() -> float:
    return max(300.0, _env_float("TADAWUL_FUNDAMENTALS_TTL_SEC", 21600.0))

def _hist_ttl_sec() -> float:
    return max(300.0, _env_float("TADAWUL_HISTORY_TTL_SEC", 1800.0))

def _err_ttl_sec() -> float:
    return max(5.0, _env_float("TADAWUL_ERROR_TTL_SEC", 10.0))

def _history_days() -> int:
    return max(60, _env_int("TADAWUL_HISTORY_DAYS", 500))

def _history_points_max() -> int:
    return max(100, _env_int("TADAWUL_HISTORY_POINTS_MAX", 1000))

def _extra_headers() -> Dict[str, str]:
    raw = _env_str("TADAWUL_HEADERS_JSON", "")
    if not raw: return {}
    try:
        js = json.loads(raw)
        if isinstance(js, dict):
            return {str(k).strip(): str(v).strip() for k, v in js.items() if str(k).strip() and str(v).strip()}
    except Exception: pass
    return {}

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


# ============================================================================
# KSA Symbol Normalization (Strict)
# ============================================================================

def normalize_ksa_symbol(symbol: str) -> str:
    raw = (symbol or "").strip()
    if not raw: return ""
    raw = raw.translate(_ARABIC_DIGITS).strip()

    if callable(_SHARED_NORM_KSA):
        try:
            s = (_SHARED_NORM_KSA(raw) or "").strip().upper()
            if s.endswith(".SR"):
                code = s[:-3].strip()
                return f"{code}.SR" if _KSA_CODE_RE.match(code) else ""
            return ""
        except Exception: pass

    s = raw.strip().upper()
    if s.startswith("TADAWUL:"): s = s.split(":", 1)[1].strip()
    if s.endswith(".SR"):
        code = s[:-3].strip()
        return f"{code}.SR" if _KSA_CODE_RE.match(code) else ""
    if _KSA_CODE_RE.match(s):
        return f"{s}.SR"
    return ""

def format_url(tpl: str, symbol: str, *, days: Optional[int] = None) -> str:
    sym = normalize_ksa_symbol(symbol)
    if not sym: return tpl
    code = sym[:-3] if sym.endswith(".SR") else sym
    url = tpl.replace("{symbol}", sym).replace("{code}", code)
    if days is not None: url = url.replace("{days}", str(int(days)))
    return url


# ============================================================================
# Numeric & Type Helpers
# ============================================================================

def safe_float(val: Any) -> Optional[float]:
    if val is None: return None
    try:
        if isinstance(val, (int, float)):
            f = float(val)
            return f if not math.isnan(f) and not math.isinf(f) else None
        s = str(val).strip()
        if not s or s in {"-", "—", "N/A", "NA", "null", "None", ""}: return None
        s = s.translate(_ARABIC_DIGITS).replace("٬", ",").replace("٫", ".").replace("−", "-")
        s = s.replace("SAR", "").replace("ريال", "").strip()
        s = s.replace("%", "").replace(",", "").replace("+", "").strip()
        if s.startswith("(") and s.endswith(")"): s = "-" + s[1:-1].strip()
        m = re.match(r"^(-?\d+(\.\d+)?)([KMB])$", s, re.IGNORECASE)
        mult = 1.0
        if m:
            num = m.group(1)
            suf = m.group(3).upper()
            mult = 1_000.0 if suf == "K" else 1_000_000.0 if suf == "M" else 1_000_000_000.0
            s = num
        f = float(s) * mult
        return f if not math.isnan(f) and not math.isinf(f) else None
    except Exception: return None

def safe_int(val: Any) -> Optional[int]:
    f = safe_float(val)
    return int(round(f)) if f is not None else None

def safe_str(val: Any) -> Optional[str]:
    return str(val).strip() if val is not None and str(val).strip() else None

def pick(d: Any, *keys: str) -> Any:
    if not isinstance(d, dict): return None
    for k in keys:
        if k in d: return d[k]
    return None

def unwrap_common_envelopes(js: Union[dict, list]) -> Union[dict, list]:
    current = js
    for _ in range(3):
        if isinstance(current, dict):
            for key in ("data", "result", "payload", "quote", "profile", "response"):
                if key in current and isinstance(current[key], (dict, list)):
                    current = current[key]
                    break
            else: break
        else: break
    return current

def coerce_dict(data: Union[dict, list]) -> dict:
    if isinstance(data, dict): return data
    if isinstance(data, list) and data and isinstance(data[0], dict): return data[0]
    return {}

def find_first_value(obj: Any, keys: Sequence[str], max_depth: int = 7, max_nodes: int = 3000) -> Any:
    if obj is None: return None
    keys_lower = {str(k).strip().lower() for k in keys if k}
    if not keys_lower: return None

    queue: List[Tuple[Any, int]] = [(obj, 0)]
    seen: Set[int] = set()
    nodes = 0

    while queue:
        current, depth = queue.pop(0)
        if current is None or depth > max_depth: continue

        current_id = id(current)
        if current_id in seen: continue
        seen.add(current_id)

        nodes += 1
        if nodes > max_nodes: return None

        if isinstance(current, dict):
            for k, v in current.items():
                if str(k).strip().lower() in keys_lower: return v
                queue.append((v, depth + 1))
        elif isinstance(current, list):
            for item in current:
                queue.append((item, depth + 1))
    return None

def pick_num(obj: Any, *keys: str) -> Optional[float]:
    return safe_float(find_first_value(obj, keys))

def pick_str(obj: Any, *keys: str) -> Optional[str]:
    return safe_str(find_first_value(obj, keys))

def pick_pct(obj: Any, *keys: str) -> Optional[float]:
    v = safe_float(find_first_value(obj, keys))
    return v * 100.0 if v is not None and abs(v) <= 1.0 else v

def clean_patch(p: Dict[str, Any]) -> Dict[str, Any]:
    return {k: v for k, v in (p or {}).items() if v is not None and not (isinstance(v, str) and not v.strip())}

def merge_into(dst: Dict[str, Any], src: Dict[str, Any], *, force_keys: Sequence[str] = ()) -> None:
    fset = set(force_keys or ())
    for k, v in (src or {}).items():
        if v is None: continue
        if k in fset or k not in dst or dst.get(k) is None or (isinstance(dst.get(k), str) and not dst.get(k).strip()):
            dst[k] = v

def fill_derived_quote_fields(patch: Dict[str, Any]) -> None:
    cur, prev = safe_float(patch.get("current_price")), safe_float(patch.get("previous_close"))
    vol, high, low = safe_float(patch.get("volume")), safe_float(patch.get("day_high")), safe_float(patch.get("day_low"))
    w52h, w52l = safe_float(patch.get("week_52_high")), safe_float(patch.get("week_52_low"))

    if patch.get("price_change") is None and cur is not None and prev is not None:
        patch["price_change"] = cur - prev
    if patch.get("percent_change") is None and cur is not None and prev not in (None, 0.0):
        try: patch["percent_change"] = (cur - prev) / prev * 100.0
        except Exception: pass
    if patch.get("value_traded") is None and cur is not None and vol is not None:
        patch["value_traded"] = cur * vol
    if cur is not None and high is not None and low is not None and cur != 0:
        patch["day_range_pct"] = ((high - low) / cur) * 100.0
    if cur is not None and w52h is not None and w52l is not None and w52h != w52l:
        patch["week_52_position_pct"] = ((cur - w52l) / (w52h - w52l)) * 100.0
    if patch.get("currency") is None:
        patch["currency"] = "SAR"

def data_quality_score(patch: Dict[str, Any]) -> Tuple[DataQuality, float]:
    score = 100.0
    if safe_float(patch.get("current_price")) is None: score -= 30
    if safe_float(patch.get("previous_close")) is None: score -= 15
    if safe_float(patch.get("volume")) is None: score -= 10
    if safe_float(patch.get("market_cap")) is None: score -= 10
    if safe_str(patch.get("name")) is None: score -= 10
    if safe_int(patch.get("history_points", 0)) < 50: score -= 20

    score = max(0.0, min(100.0, score))
    if score >= 80: cat = DataQuality.EXCELLENT
    elif score >= 60: cat = DataQuality.GOOD
    elif score >= 40: cat = DataQuality.FAIR
    elif score >= 20: cat = DataQuality.POOR
    else: cat = DataQuality.BAD
    return cat, score


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
    def __init__(self, namespace: str = "tadawul"):
        self.namespace = namespace
        self._metrics: Dict[str, Any] = {}
        self._lock = threading.RLock()
        if _PROMETHEUS_AVAILABLE and _METRICS_ENABLED: self._init_metrics()
    
    def _init_metrics(self):
        with self._lock:
            self._metrics["requests_total"] = Counter(f"{self.namespace}_requests_total", "Total requests", ["status"])
            self._metrics["request_duration_seconds"] = Histogram(f"{self.namespace}_request_duration_seconds", "Duration", buckets=[0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0])
            self._metrics["cache_hits_total"] = Counter(f"{self.namespace}_cache_hits_total", "Cache hits", ["cache_type"])
            self._metrics["cache_misses_total"] = Counter(f"{self.namespace}_cache_misses_total", "Cache misses", ["cache_type"])
            self._metrics["circuit_breaker_state"] = Gauge(f"{self.namespace}_circuit_breaker_state", "State (1=closed, 0=open, -1=half)")
            self._metrics["rate_limiter_tokens"] = Gauge(f"{self.namespace}_rate_limiter_tokens", "Tokens")
            self._metrics["queue_size"] = Gauge(f"{self.namespace}_queue_size", "Queue size")
    
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
# Core Engine Tools (Cache, Limiter, Circuit Breaker, Queue)
# ============================================================================

class SmartCache:
    def __init__(self, maxsize: int = 5000, ttl: float = 300.0):
        self.maxsize = maxsize
        self.ttl = ttl
        self._cache: Dict[str, Any] = {}
        self._expires: Dict[str, float] = {}
        self._access_times: Dict[str, float] = {}
        self._lock = asyncio.Lock()

    async def get(self, key: str) -> Optional[Any]:
        async with self._lock:
            now = time.monotonic()
            if key in self._cache:
                if now < self._expires.get(key, 0):
                    self._access_times[key] = now
                    _METRICS.inc("cache_hits_total", 1, {"cache_type": "memory"})
                    return self._cache[key]
                else:
                    await self._delete(key)
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

    async def size(self) -> int:
        async with self._lock: return len(self._cache)

class TokenBucket:
    def __init__(self, rate_per_sec: float, capacity: Optional[float] = None):
        self.rate = max(0.0, rate_per_sec)
        self.capacity = capacity or max(1.0, self.rate)
        self.tokens = self.capacity
        self.last = time.monotonic()
        self._lock = asyncio.Lock()

    async def acquire(self, tokens: float = 1.0) -> bool:
        if self.rate <= 0: return True
        async with self._lock:
            now = time.monotonic()
            self.tokens = min(self.capacity, self.tokens + max(0.0, now - self.last) * self.rate)
            self.last = now
            if self.tokens >= tokens:
                self.tokens -= tokens
                _METRICS.set("rate_limiter_tokens", self.tokens)
                return True
            return False

    async def wait_and_acquire(self, tokens: float = 1.0) -> None:
        while True:
            if await self.acquire(tokens): return
            async with self._lock: wait = (tokens - self.tokens) / self.rate if self.rate > 0 else 0.1
            await asyncio.sleep(min(1.0, wait))

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
    
    async def put(self, item: RequestQueueItem) -> bool:
        async with self._lock:
            total_size = sum(q.qsize() for q in self.queues.values())
            if total_size >= self.max_size: return False
            await self.queues[item.priority].put(item)
            _METRICS.set("queue_size", total_size + 1)
            return True
    
    async def get(self) -> Optional[RequestQueueItem]:
        for priority in sorted(RequestPriority, key=lambda p: p.value, reverse=True):
            queue = self.queues[priority]
            if not queue.empty():
                item = await queue.get()
                _METRICS.set("queue_size", sum(q.qsize() for q in self.queues.values()))
                return item
        return None

class SingleFlight:
    def __init__(self) -> None:
        self._lock = asyncio.Lock()
        self._futures: Dict[str, asyncio.Future] = {}

    async def run(self, key: str, coro_fn):
        async with self._lock:
            if key in self._futures:
                return await self._futures[key]
            fut = asyncio.get_event_loop().create_future()
            self._futures[key] = fut
        try:
            result = await coro_fn()
            if not fut.done(): fut.set_result(result)
            return result
        except Exception as e:
            if not fut.done(): fut.set_exception(e)
            raise
        finally:
            async with self._lock: self._futures.pop(key, None)


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
        else: period = "12m"

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

def compute_history_analytics(
    closes: List[float],
    highs: Optional[List[float]] = None,
    lows: Optional[List[float]] = None,
    volumes: Optional[List[float]] = None
) -> Dict[str, Any]:
    if not closes or len(closes) < 10: return {}
    out: Dict[str, Any] = {}
    last = closes[-1]

    for name, days in {"returns_1w": 5, "returns_1m": 21, "returns_3m": 63, "returns_12m": 252}.items():
        if len(closes) > days and closes[-(days + 1)] != 0: 
            out[name] = float((last / closes[-(days + 1)] - 1) * 100)

    for period in [20, 50, 200]:
        if len(closes) >= period:
            ma = sum(closes[-period:]) / period
            out[f"sma_{period}"] = float(ma)
            out[f"price_to_sma_{period}"] = float((last / ma - 1) * 100)

    if len(closes) >= 31:
        out["volatility_30d"] = float(np.std(np.diff(closes[-31:]) / closes[-31:-1]) * math.sqrt(252) * 100)

    ti = TechnicalIndicators()
    rsi_vals = ti.rsi(closes, 14)
    if rsi_vals and rsi_vals[-1] is not None:
        out["rsi_14"] = float(rsi_vals[-1])
        out["rsi_signal"] = "overbought" if out["rsi_14"] > 70 else "oversold" if out["rsi_14"] < 30 else "neutral"

    macd = ti.macd(closes)
    if macd["macd"] and macd["macd"][-1] is not None:
        out["macd"] = float(macd["macd"][-1])
        out["macd_signal"] = float(macd["signal"][-1]) if macd["signal"][-1] else None
        out["macd_histogram"] = float(macd["histogram"][-1]) if macd["histogram"][-1] else None
        if out["macd"] and out["macd_signal"]:
            out["macd_cross"] = "bullish" if out["macd"] > out["macd_signal"] else "bearish"

    bb = ti.bollinger_bands(closes)
    if bb["middle"][-1] is not None:
        out["bb_middle"] = float(bb["middle"][-1])
        out["bb_upper"] = float(bb["upper"][-1])
        out["bb_lower"] = float(bb["lower"][-1])
        if bb["upper"][-1] and bb["lower"][-1]:
            out["bb_position"] = float((last - bb["lower"][-1]) / (bb["upper"][-1] - bb["lower"][-1]))

    if highs and lows and len(highs) == len(closes) and len(lows) == len(closes):
        atr_values = ti.atr(highs, lows, closes, 14)
        if atr_values and atr_values[-1] is not None:
            out["atr_14"] = float(atr_values[-1])
            
        ichimoku = ti.ichimoku_cloud(highs, lows)
        if ichimoku["tenkan_sen"][-1] is not None:
            out["ichimoku_tenkan"] = float(ichimoku["tenkan_sen"][-1])
            out["ichimoku_kijun"] = float(ichimoku["kijun_sen"][-1])

    if volumes and len(volumes) == len(closes):
        obv_values = ti.obv(closes, volumes)
        if obv_values:
            out["obv"] = float(obv_values[-1])
            if len(obv_values) > 20:
                obv_trend = (obv_values[-1] / obv_values[-20] - 1) * 100
                out["obv_trend_pct"] = float(obv_trend)
                out["obv_signal"] = "bullish" if obv_trend > 2 else "bearish" if obv_trend < -2 else "neutral"

    if len(closes) >= 14 and highs and lows:
        stoch = ti.stochastic_oscillator(highs, lows, closes)
        if stoch["%k"][-1] is not None:
            out["stochastic_k"] = float(stoch["%k"][-1])
            out["stochastic_d"] = float(stoch["%d"][-1])

    peak = closes[0]
    max_dd = 0.0
    for price in closes:
        if price > peak: peak = price
        dd = (price / peak - 1) * 100
        if dd < max_dd: max_dd = dd
    out["max_drawdown_pct"] = float(max_dd)

    regime, confidence = MarketRegimeDetector(closes).detect()
    out["market_regime"] = regime.value
    out["market_regime_confidence"] = confidence

    if _enable_forecast() and len(closes) >= 60:
        forecaster = EnsembleForecaster(closes, enable_ml=_enable_ml())
        for horizon, period in [(21, "1m"), (63, "3m"), (252, "12m")]:
            forecast = forecaster.forecast(horizon)
            if forecast.get("forecast_available"):
                out[f"expected_roi_{period}"] = forecast["ensemble"]["roi_pct"]
                out[f"forecast_price_{period}"] = forecast["ensemble"]["price"]
                out[f"target_price_{period}"] = forecast["ensemble"]["price"]
                out[f"forecast_range_low_{period}"] = forecast["ensemble"].get("price_range_low")
                out[f"forecast_range_high_{period}"] = forecast["ensemble"].get("price_range_high")

                if period == "12m":
                    out["forecast_confidence"] = forecast["confidence"] / 100.0  # 0..1 scale
                    out["forecast_confidence_pct"] = forecast["confidence"]      # 0..100 scale
                    out["forecast_confidence_level"] = forecast["confidence_level"]
                    out["forecast_models"] = forecast["models_used"]

        out["forecast_method"] = "ensemble_v4"
        out["forecast_source"] = "tadawul_ml_ensemble"

    return out


# ============================================================================
# Advanced Tadawul Client
# ============================================================================

class TadawulClient:
    """Next-Gen Async Tadawul API client."""

    def __init__(self) -> None:
        self.timeout_sec = _timeout_sec()
        self.retry_attempts = _retry_attempts()

        self.quote_url = _safe_str(_env_str("TADAWUL_QUOTE_URL", ""))
        self.fundamentals_url = _safe_str(_env_str("TADAWUL_FUNDAMENTALS_URL", ""))
        self.history_url = _safe_str(_env_str("TADAWUL_HISTORY_URL", "")) or _safe_str(_env_str("TADAWUL_CANDLES_URL", ""))

        headers = {
            "User-Agent": _env_str("TADAWUL_USER_AGENT", USER_AGENT_DEFAULT),
            "Accept": "application/json",
            "Accept-Language": "ar,en;q=0.9",
        }
        headers.update(_extra_headers())

        self._client = httpx.AsyncClient(
            timeout=httpx.Timeout(self.timeout_sec),
            follow_redirects=True,
            headers=headers,
            limits=httpx.Limits(max_keepalive_connections=25, max_connections=50),
            http2=True
        )

        self.quote_cache = SmartCache(maxsize=7000, ttl=_quote_ttl_sec())
        self.fund_cache = SmartCache(maxsize=4000, ttl=_fund_ttl_sec())
        self.history_cache = SmartCache(maxsize=2500, ttl=_hist_ttl_sec())
        self.error_cache = SmartCache(maxsize=4000, ttl=_err_ttl_sec())

        self.semaphore = asyncio.Semaphore(_max_concurrency())
        self.rate_limiter = TokenBucket(_rate_limit())
        self.circuit_breaker = DynamicCircuitBreaker(
            fail_threshold=_circuit_breaker_threshold(),
            base_cooldown=_circuit_breaker_timeout()
        )

        self.request_queue = RequestQueue(max_size=_queue_size())
        self.singleflight = SingleFlight()
        
        self._queue_processor_task = asyncio.create_task(self._process_queue())

        logger.info(
            f"TadawulClient v{PROVIDER_VERSION} initialized | "
            f"rate={_rate_limit()}/s | cb={_circuit_breaker_threshold()}/{_circuit_breaker_timeout()}s"
        )

    @_trace("tadawul_request")
    async def _request(self, url: str, priority: RequestPriority = RequestPriority.NORMAL) -> Tuple[Optional[Union[dict, list]], Optional[str]]:
        _METRICS.inc("requests_total", 1, {"status": "started"})
        
        if not await self.circuit_breaker.allow_request(): return None, "circuit_breaker_open"

        future = asyncio.Future()
        queue_item = RequestQueueItem(priority=priority, url=url, future=future)
        if not await self.request_queue.put(queue_item): return None, "queue_full"

        result = await future
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
            result = await self._execute_request(item.url)
            item.future.set_result(result)
        except Exception as e:
            item.future.set_exception(e)

    async def _execute_request(self, url: str) -> Tuple[Optional[Union[dict, list]], Optional[str]]:
        await self.rate_limiter.wait_and_acquire()
        last_err = None

        async with self.semaphore:
            for attempt in range(self.retry_attempts):
                try:
                    resp = await self._client.get(url)
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

                    try:
                        data = json_loads(resp.content)
                        data = unwrap_common_envelopes(data)
                    except Exception:
                        await self.circuit_breaker.on_failure()
                        return None, "invalid_json"

                    if not isinstance(data, (dict, list)):
                        await self.circuit_breaker.on_failure()
                        return None, "unexpected_response_type"

                    await self.circuit_breaker.on_success()
                    _METRICS.inc("requests_total", 1, {"status": "success"})
                    return data, None

                except httpx.RequestError as e:
                    last_err = f"network_error_{e.__class__.__name__}"
                    base_wait = 2 ** attempt
                    await asyncio.sleep(min(10.0, base_wait + random.uniform(0, base_wait)))

        await self.circuit_breaker.on_failure()
        _METRICS.inc("requests_total", 1, {"status": "error"})
        return None, last_err or "max_retries_exceeded"

    def _map_quote(self, root: Any) -> Dict[str, Any]:
        patch: Dict[str, Any] = {}
        patch["current_price"] = pick_num(root, "last", "last_price", "price", "close", "c", "tradingPrice", "regularMarketPrice")
        patch["previous_close"] = pick_num(root, "previous_close", "prev_close", "pc", "PreviousClose", "prevClose")
        patch["open"] = pick_num(root, "open", "o", "Open", "openPrice")
        patch["day_high"] = pick_num(root, "high", "day_high", "h", "High", "dayHigh", "sessionHigh")
        patch["day_low"] = pick_num(root, "low", "day_low", "l", "Low", "dayLow", "sessionLow")
        patch["volume"] = pick_num(root, "volume", "v", "Volume", "tradedVolume", "qty", "quantity")
        patch["value_traded"] = pick_num(root, "value_traded", "tradedValue", "turnover", "value", "tradeValue")
        patch["week_52_high"] = pick_num(root, "week_52_high", "fiftyTwoWeekHigh", "52w_high", "yearHigh")
        patch["week_52_low"] = pick_num(root, "week_52_low", "fiftyTwoWeekLow", "52w_low", "yearLow")
        patch["price_change"] = pick_num(root, "change", "d", "price_change", "Change", "diff", "delta")
        patch["percent_change"] = pick_pct(root, "change_pct", "change_percent", "dp", "percent_change", "pctChange", "changePercent")
        fill_derived_quote_fields(patch)
        return clean_patch(patch)

    def _map_fundamentals(self, root: Any) -> Dict[str, Any]:
        patch: Dict[str, Any] = {}
        patch["name"] = pick_str(root, "name", "company", "company_name", "CompanyName", "shortName", "longName")
        patch["sector"] = pick_str(root, "sector", "Sector", "sectorName")
        patch["industry"] = pick_str(root, "industry", "Industry", "industryName")
        patch["sub_sector"] = pick_str(root, "sub_sector", "subSector", "SubSector", "subSectorName", "subIndustry")
        patch["market_cap"] = pick_num(root, "market_cap", "marketCap", "marketCapitalization", "MarketCap")
        patch["shares_outstanding"] = pick_num(root, "shares", "shares_outstanding", "shareOutstanding", "SharesOutstanding")
        patch["pe_ttm"] = pick_num(root, "pe", "pe_ttm", "trailingPE", "PE", "priceEarnings")
        patch["pb"] = pick_num(root, "pb", "priceToBook", "PBR", "price_book")
        patch["eps_ttm"] = pick_num(root, "eps", "eps_ttm", "trailingEps", "EPS")
        patch["dividend_yield"] = pick_pct(root, "dividend_yield", "divYield", "yield", "DividendYield")
        patch["beta"] = pick_num(root, "beta", "Beta")
        if patch.get("currency") is None: patch["currency"] = "SAR"
        return clean_patch(patch)

    def _parse_candles(self, js: Any) -> Tuple[List[float], List[float], List[float], List[float], Optional[datetime]]:
        closes, highs, lows, volumes = [], [], [], []
        last_dt = None

        if isinstance(js, dict):
            c_raw, h_raw, l_raw, v_raw = js.get("c") or js.get("close"), js.get("h") or js.get("high"), js.get("l") or js.get("low"), js.get("v") or js.get("volume")
            t_raw = js.get("t") or js.get("time") or js.get("timestamp")
            
            if isinstance(c_raw, list): closes = [f for f in (safe_float(x) for x in c_raw) if f is not None]
            if isinstance(h_raw, list): highs = [f for f in (safe_float(x) for x in h_raw) if f is not None]
            if isinstance(l_raw, list): lows = [f for f in (safe_float(x) for x in l_raw) if f is not None]
            if isinstance(v_raw, list): volumes = [f for f in (safe_float(x) for x in v_raw) if f is not None]

            if isinstance(t_raw, list) and t_raw:
                try: last_dt = datetime.fromtimestamp(float(t_raw[-1]), tz=timezone.utc)
                except Exception: pass

            if not closes:
                candles = js.get("candles") or js.get("data") or js.get("items") or js.get("prices") or js.get("history")
                if isinstance(candles, list) and candles and isinstance(candles[0], dict):
                    for item in candles:
                        c, h, l, v = safe_float(item.get("c") or item.get("close")), safe_float(item.get("h") or item.get("high")), safe_float(item.get("l") or item.get("low")), safe_float(item.get("v") or item.get("volume"))
                        if c is not None: closes.append(c)
                        if h is not None: highs.append(h)
                        if l is not None: lows.append(l)
                        if v is not None: volumes.append(v)
                        t = item.get("t") or item.get("time") or item.get("date")
                        if last_dt is None and t is not None:
                            try:
                                if isinstance(t, (int, float)): last_dt = datetime.fromtimestamp(float(t), tz=timezone.utc)
                                elif isinstance(t, str): last_dt = datetime.fromisoformat(t.replace('Z', '+00:00'))
                            except Exception: pass

        maxp = _history_points_max()
        return closes[-maxp:], highs[-maxp:] if highs else [], lows[-maxp:] if lows else [], volumes[-maxp:] if volumes else [], last_dt

    async def fetch_quote_patch(self, symbol: str) -> Dict[str, Any]:
        if not _configured(): return {}
        sym = normalize_ksa_symbol(symbol)
        if not sym or not self.quote_url: return {} if not _emit_warnings() else {"_warn": "invalid_symbol_or_url"}

        cache_key = f"quote:{sym}"
        if await self.error_cache.get(cache_key): return {}

        async def _fetch():
            js, err = await self._request(format_url(self.quote_url, sym))
            if js is None:
                await self.error_cache.set(cache_key, True)
                return {} if not _emit_warnings() else {"_warn": f"quote_failed: {err}"}
            
            mapped = self._map_quote(coerce_dict(js))
            if safe_float(mapped.get("current_price")) is None:
                await self.error_cache.set(cache_key, True)
                return {}
            
            result = {"requested_symbol": symbol, "symbol": sym, "provider": PROVIDER_NAME, "data_source": PROVIDER_NAME, "provider_version": PROVIDER_VERSION, "last_updated_utc": _utc_iso(), "last_updated_riyadh": _riyadh_iso(), **mapped}
            await self.quote_cache.set(cache_key, result)
            return result

        cached = await self.quote_cache.get(cache_key)
        return dict(cached) if cached else await self.singleflight.run(cache_key, _fetch)

    async def fetch_fundamentals_patch(self, symbol: str) -> Dict[str, Any]:
        if not _configured() or not _enable_fundamentals(): return {}
        sym = normalize_ksa_symbol(symbol)
        if not sym or not self.fundamentals_url: return {}

        cache_key = f"fund:{sym}"
        if await self.error_cache.get(cache_key): return {}

        async def _fetch():
            js, err = await self._request(format_url(self.fundamentals_url, sym))
            if js is None:
                await self.error_cache.set(cache_key, True)
                return {}
            mapped = self._map_fundamentals(coerce_dict(js))
            if not mapped:
                await self.error_cache.set(cache_key, True)
                return {}
            result = {"provider": PROVIDER_NAME, "data_source": PROVIDER_NAME, "provider_version": PROVIDER_VERSION, **mapped}
            await self.fund_cache.set(cache_key, result)
            return result

        cached = await self.fund_cache.get(cache_key)
        return dict(cached) if cached else await self.singleflight.run(cache_key, _fetch)

    async def fetch_history_patch(self, symbol: str) -> Dict[str, Any]:
        if not _configured() or not _enable_history(): return {}
        sym = normalize_ksa_symbol(symbol)
        if not sym or not self.history_url: return {}

        days = _history_days()
        cache_key = f"history:{sym}:{days}"
        if await self.error_cache.get(cache_key): return {}

        async def _fetch():
            js, err = await self._request(format_url(self.history_url, sym, days=days))
            if js is None:
                await self.error_cache.set(cache_key, True)
                return {}
            closes, highs, lows, volumes, last_dt = self._parse_candles(js)
            if not closes or len(closes) < 10:
                await self.error_cache.set(cache_key, True)
                return {}
            
            analytics = compute_history_analytics(closes, highs, lows, volumes)
            result = {
                "requested_symbol": symbol, "normalized_symbol": sym, "history_points": len(closes),
                "history_last_utc": _utc_iso(last_dt), "history_last_riyadh": _to_riyadh_iso(last_dt),
                "provider": PROVIDER_NAME, "data_source": PROVIDER_NAME, "provider_version": PROVIDER_VERSION,
                **analytics
            }
            if "forecast_method" in analytics:
                result["forecast_updated_utc"] = _utc_iso(last_dt)
                result["forecast_updated_riyadh"] = _to_riyadh_iso(last_dt)
            await self.history_cache.set(cache_key, result)
            return result

        cached = await self.history_cache.get(cache_key)
        return dict(cached) if cached else await self.singleflight.run(cache_key, _fetch)

    async def fetch_enriched_quote_patch(self, symbol: str) -> Dict[str, Any]:
        if not _configured(): return {}
        sym = normalize_ksa_symbol(symbol)
        if not sym: return {} if not _emit_warnings() else {"_warn": "invalid_ksa_symbol"}

        quote_data, fund_data, hist_data = await asyncio.gather(
            self.fetch_quote_patch(symbol), self.fetch_fundamentals_patch(symbol), self.fetch_history_patch(symbol), return_exceptions=True
        )

        quote_data = quote_data if isinstance(quote_data, dict) else {}
        fund_data = fund_data if isinstance(fund_data, dict) else {}
        hist_data = hist_data if isinstance(hist_data, dict) else {}

        result = dict(quote_data) if quote_data else {"requested_symbol": symbol, "symbol": sym, "provider": PROVIDER_NAME, "data_source": PROVIDER_NAME, "provider_version": PROVIDER_VERSION, "last_updated_utc": _utc_iso(), "last_updated_riyadh": _riyadh_iso()}
        if fund_data: merge_into(result, fund_data, force_keys=("market_cap", "pe_ttm", "eps_ttm", "shares_outstanding", "name", "sector", "industry"))
        if hist_data:
            for k, v in hist_data.items():
                if k not in result and v is not None: result[k] = v

        cat, score = data_quality_score(result)
        result["data_quality"], result["data_quality_score"] = cat.value, score

        if _emit_warnings():
            warnings = [src["_warn"] for src in (quote_data, fund_data, hist_data) if isinstance(src, dict) and "_warn" in src]
            if warnings: result["_warnings"] = " | ".join(warnings[:3])

        return clean_patch(result)

    async def get_metrics(self) -> Dict[str, Any]:
        async with self._metrics_lock: metrics = dict(self.metrics)
        metrics["circuit_breaker"] = self.circuit_breaker.get_stats()
        metrics["cache_sizes"] = { "quote": await self.quote_cache.size(), "fund": await self.fund_cache.size(), "history": await self.history_cache.size(), "error": await self.error_cache.size() }
        return metrics

    async def close(self) -> None:
        self._queue_processor_task.cancel()
        try: await self._queue_processor_task
        except: pass
        await self._client.aclose()


# ============================================================================
# Public API
# ============================================================================

_CLIENT_INSTANCE: Optional[TadawulClient] = None
_CLIENT_LOCK = asyncio.Lock()

async def get_client() -> TadawulClient:
    global _CLIENT_INSTANCE
    if _CLIENT_INSTANCE is None:
        async with _CLIENT_LOCK:
            if _CLIENT_INSTANCE is None: _CLIENT_INSTANCE = TadawulClient()
    return _CLIENT_INSTANCE

async def close_client() -> None:
    global _CLIENT_INSTANCE
    if _CLIENT_INSTANCE:
        await _CLIENT_INSTANCE.close()
        _CLIENT_INSTANCE = None

async def fetch_enriched_quote_patch(symbol: str, *args: Any, **kwargs: Any) -> Dict[str, Any]:
    return await (await get_client()).fetch_enriched_quote_patch(symbol)

async def fetch_quote_patch(symbol: str, *args: Any, **kwargs: Any) -> Dict[str, Any]:
    return await (await get_client()).fetch_quote_patch(symbol)

async def fetch_fundamentals_patch(symbol: str, *args: Any, **kwargs: Any) -> Dict[str, Any]:
    return await (await get_client()).fetch_fundamentals_patch(symbol)

async def fetch_history_patch(symbol: str, *args: Any, **kwargs: Any) -> Dict[str, Any]:
    return await (await get_client()).fetch_history_patch(symbol)

async def fetch_quote_and_fundamentals_patch(symbol: str, *args: Any, **kwargs: Any) -> Dict[str, Any]:
    client = await get_client()
    result = dict(await client.fetch_quote_patch(symbol))
    merge_into(result, await client.fetch_fundamentals_patch(symbol))
    return clean_patch(result)

async def fetch_quote_and_history_patch(symbol: str, *args: Any, **kwargs: Any) -> Dict[str, Any]:
    client = await get_client()
    result = dict(await client.fetch_quote_patch(symbol))
    merge_into(result, await client.fetch_history_patch(symbol))
    return clean_patch(result)

async def get_client_metrics() -> Dict[str, Any]:
    return await (await get_client()).get_metrics()

async def aclose_tadawul_client() -> None:
    await close_client()

__all__ = [
    "PROVIDER_NAME", "PROVIDER_VERSION", "fetch_enriched_quote_patch", "fetch_quote_patch",
    "fetch_fundamentals_patch", "fetch_history_patch", "fetch_quote_and_fundamentals_patch",
    "fetch_quote_and_history_patch", "get_client_metrics", "aclose_tadawul_client",
    "normalize_ksa_symbol", "MarketRegime", "DataQuality",
]
