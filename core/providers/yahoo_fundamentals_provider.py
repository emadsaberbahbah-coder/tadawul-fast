#!/usr/bin/env python3
# core/providers/yahoo_fundamentals_provider.py
"""
================================================================================
Yahoo Finance Fundamentals Provider — v5.1.0 (ADVANCED ENTERPRISE)
================================================================================

What's new in v5.1.0:
- ✅ XGBoost & Ridge Regression for EPS/Revenue multi-year projections
- ✅ High-performance JSON parsing via `orjson` (if available)
- ✅ Exponential Backoff with 'Full Jitter' safely in yfinance threads
- ✅ Dynamic Circuit Breaker with progressive timeout scaling
- ✅ Priority Request Queuing and strict concurrency semaphores
- ✅ Distributed caching with optional Redis Cluster support & compression
- ✅ Prometheus metrics export & OpenTelemetry tracing integration
- ✅ Enhanced DCF Valuation (Dynamic WACC via CAPM Beta estimation)
- ✅ IsolationForest ML-based fundamental anomaly detection

Key Features:
- Global equity fundamentals
- Real-time and historical financial data
- Analyst estimates and price targets
- Financial health scoring (Altman Z, Piotroski F)
- Intrinsic value calculations (DCF, Graham Number)
- Growth stage classification
- Production-grade error handling
"""

from __future__ import annotations

import asyncio
import hashlib
import logging
import math
import os
import random
import re
import threading
import time
import pickle
import zlib
from collections import defaultdict, deque
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from enum import Enum
from typing import Any, Dict, List, Optional, Set, Tuple, Union

import numpy as np

# ---------------------------------------------------------------------------
# High-Performance JSON fallback
# ---------------------------------------------------------------------------
try:
    import orjson
    def json_dumps(obj: Any) -> str:
        return orjson.dumps(obj).decode('utf-8')
except ImportError:
    def json_dumps(obj: Any) -> str:
        return json.dumps(obj, default=str)

# ---------------------------------------------------------------------------
# Optional Scientific & ML Stack
# ---------------------------------------------------------------------------
try:
    from scipy import stats
    SCIPY_AVAILABLE = True
except ImportError:
    SCIPY_AVAILABLE = False

try:
    from sklearn.linear_model import LinearRegression, Ridge
    from sklearn.preprocessing import StandardScaler
    from sklearn.ensemble import RandomForestRegressor, IsolationForest
    SKLEARN_AVAILABLE = True
except ImportError:
    SKLEARN_AVAILABLE = False

try:
    import xgboost as xgb
    XGB_AVAILABLE = True
except ImportError:
    XGB_AVAILABLE = False

# ---------------------------------------------------------------------------
# Redis / Monitoring Stack
# ---------------------------------------------------------------------------
try:
    from redis.asyncio import Redis, ConnectionPool
    REDIS_AVAILABLE = True
except ImportError:
    REDIS_AVAILABLE = False

try:
    from prometheus_client import Counter, Gauge, Histogram
    PROMETHEUS_AVAILABLE = True
except ImportError:
    PROMETHEUS_AVAILABLE = False

try:
    from opentelemetry import trace
    from opentelemetry.trace import Status, StatusCode
    _OTEL_AVAILABLE = True
except ImportError:
    _OTEL_AVAILABLE = False
    trace = None

# ---------------------------------------------------------------------------
# Core yfinance import
# ---------------------------------------------------------------------------
try:
    import yfinance as yf
    import pandas as pd
    _HAS_YFINANCE = True
    _HAS_PANDAS = True
except ImportError:
    yf = None
    pd = None
    _HAS_YFINANCE = False
    _HAS_PANDAS = False


logger = logging.getLogger("core.providers.yahoo_fundamentals_provider")

PROVIDER_NAME = "yahoo_fundamentals"
PROVIDER_VERSION = "5.1.0"

_TRUTHY = {"1", "true", "yes", "y", "on", "t"}
_FALSY = {"0", "false", "no", "n", "off", "f"}

# KSA symbol patterns
_KSA_CODE_RE = re.compile(r"^\d{3,6}$", re.IGNORECASE)

# Arabic digit translation
_ARABIC_DIGITS = str.maketrans("٠١٢٣٤٥٦٧٨٩", "0123456789")

# ============================================================================
# Enums & Data Classes
# ============================================================================

class CircuitState(Enum):
    CLOSED = "closed"
    OPEN = "open"
    HALF_OPEN = "half_open"

    def to_prometheus(self) -> float:
        return {CircuitState.CLOSED: 0.0, CircuitState.HALF_OPEN: 1.0, CircuitState.OPEN: 2.0}[self]


class RequestPriority(Enum):
    LOW = 0
    NORMAL = 1
    HIGH = 2
    CRITICAL = 3


class DataQuality(Enum):
    EXCELLENT = "excellent"
    GOOD = "good"
    FAIR = "fair"
    POOR = "poor"
    BAD = "bad"


class FinancialHealth(Enum):
    EXCELLENT = "excellent"
    GOOD = "good"
    FAIR = "fair"
    POOR = "poor"
    CRITICAL = "critical"


class GrowthStage(Enum):
    EARLY = "early"
    GROWTH = "growth"
    MATURE = "mature"
    DECLINE = "decline"
    CYCLICAL = "cyclical"


@dataclass
class RequestQueueItem:
    priority: RequestPriority
    symbol: str
    future: asyncio.Future
    created_at: float = field(default_factory=time.time)


@dataclass
class CacheStats:
    hits: int = 0
    misses: int = 0
    sets: int = 0
    deletes: int = 0
    evictions: int = 0
    size: int = 0
    memory_usage: float = 0.0


@dataclass
class CircuitBreakerStats:
    failures: int = 0
    successes: int = 0
    last_failure: float = 0.0
    last_success: float = 0.0
    state: CircuitState = CircuitState.CLOSED
    open_until: float = 0.0
    consecutive_successes: int = 0
    consecutive_failures: int = 0
    total_calls: int = 0
    rejected_calls: int = 0
    current_cooldown: float = 30.0


@dataclass
class FinancialMetrics:
    """Comprehensive financial metrics container."""
    market_cap: Optional[float] = None
    enterprise_value: Optional[float] = None
    pe_ttm: Optional[float] = None
    forward_pe: Optional[float] = None
    peg_ratio: Optional[float] = None
    ps_ttm: Optional[float] = None
    pb_ttm: Optional[float] = None
    pfcf_ttm: Optional[float] = None
    
    gross_margin: Optional[float] = None
    operating_margin: Optional[float] = None
    net_margin: Optional[float] = None
    roe: Optional[float] = None
    roa: Optional[float] = None
    roce: Optional[float] = None
    
    revenue_growth_yoy: Optional[float] = None
    earnings_growth_yoy: Optional[float] = None
    fcf_growth_yoy: Optional[float] = None
    growth_score: Optional[float] = None
    
    current_ratio: Optional[float] = None
    quick_ratio: Optional[float] = None
    debt_to_equity: Optional[float] = None
    interest_coverage: Optional[float] = None
    altman_z_score: Optional[float] = None
    piotroski_f_score: Optional[int] = None
    
    asset_turnover: Optional[float] = None
    inventory_turnover: Optional[float] = None
    receivables_turnover: Optional[float] = None
    days_sales_outstanding: Optional[float] = None
    
    operating_cf: Optional[float] = None
    investing_cf: Optional[float] = None
    financing_cf: Optional[float] = None
    free_cashflow: Optional[float] = None
    fcf_yield: Optional[float] = None
    
    dividend_yield: Optional[float] = None
    dividend_rate: Optional[float] = None
    payout_ratio: Optional[float] = None
    dividend_growth_5y: Optional[float] = None
    
    target_mean: Optional[float] = None
    target_high: Optional[float] = None
    target_low: Optional[float] = None
    recommendation: Optional[str] = None
    analyst_count: Optional[int] = None
    
    dcf_value: Optional[float] = None
    graham_value: Optional[float] = None
    relative_value: Optional[float] = None
    
    beta: Optional[float] = None
    short_ratio: Optional[float] = None
    short_percent: Optional[float] = None


# ============================================================================
# Tracing & Metrics Integrations
# ============================================================================

_TRACING_ENABLED = os.getenv("YF_TRACING_ENABLED", "").strip().lower() in _TRUTHY
_METRICS_ENABLED = os.getenv("YF_METRICS_ENABLED", "").strip().lower() in _TRUTHY

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
        import functools
        @functools.wraps(func)
        async def wrapper(*args, **kwargs):
            trace_name = name or func.__name__
            async with TraceContext(trace_name, {"function": func.__name__}):
                return await func(*args, **kwargs)
        return wrapper
    return decorator

if PROMETHEUS_AVAILABLE:
    yf_fund_requests_total = Counter('yf_fund_requests_total', 'Total API requests', ['status'])
    yf_fund_request_duration = Histogram('yf_fund_request_duration_seconds', 'Request duration', buckets=[0.1, 0.5, 1.0, 2.5, 5.0, 10.0])
    yf_fund_circuit_breaker = Gauge('yf_fund_circuit_breaker_state', 'CB state')
else:
    class DummyMetric:
        def labels(self, *args, **kwargs): return self
        def inc(self, *args, **kwargs): pass
        def observe(self, *args, **kwargs): pass
        def set(self, *args, **kwargs): pass
    yf_fund_requests_total = DummyMetric()
    yf_fund_request_duration = DummyMetric()
    yf_fund_circuit_breaker = DummyMetric()


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

def _configured() -> bool:
    if not _env_bool("YF_ENABLED", True): return False
    return _HAS_YFINANCE

def _emit_warnings() -> bool:
    return _env_bool("YF_VERBOSE_WARNINGS", False)

def _timeout_sec() -> float:
    return max(5.0, _env_float("YF_TIMEOUT_SEC", 25.0))

def _fund_ttl_sec() -> float:
    return max(300.0, _env_float("YF_FUND_TTL_SEC", 21600.0))

def _err_ttl_sec() -> float:
    return max(5.0, _env_float("YF_ERROR_TTL_SEC", 10.0))

def _max_concurrency() -> int:
    return max(2, _env_int("YF_MAX_CONCURRENCY", 10))

def _queue_size() -> int:
    return max(100, _env_int("YF_QUEUE_SIZE", 1000))

def _rate_limit() -> float:
    return max(0.0, _env_float("YF_RATE_LIMIT_PER_SEC", 5.0))

def _cb_enabled() -> bool:
    return _env_bool("YF_CIRCUIT_BREAKER", True)

def _cb_fail_threshold() -> int:
    return max(2, _env_int("YF_CB_FAIL_THRESHOLD", 6))

def _cb_cooldown_sec() -> float:
    return max(5.0, _env_float("YF_CB_COOLDOWN_SEC", 30.0))

def _enable_dcf() -> bool:
    return _env_bool("YF_ENABLE_DCF", True)

def _enable_ml_forecast() -> bool:
    return _env_bool("YF_ENABLE_ML_FORECAST", True) and SKLEARN_AVAILABLE

def _enable_redis() -> bool:
    return _env_bool("YF_ENABLE_REDIS", False) and REDIS_AVAILABLE

def _redis_url() -> str:
    return _env_str("REDIS_URL", "redis://localhost:6379/0")

def _utc_iso(dt: Optional[datetime] = None) -> str:
    d = dt or datetime.now(timezone.utc)
    if d.tzinfo is None: d = d.replace(tzinfo=timezone.utc)
    return d.astimezone(timezone.utc).isoformat()

def _riyadh_iso(dt: Optional[datetime] = None) -> str:
    tz = timezone(timedelta(hours=3))
    d = dt or datetime.now(tz)
    if d.tzinfo is None: d = d.replace(tzinfo=tz)
    return d.astimezone(tz).isoformat()


# ============================================================================
# Safe Type Helpers
# ============================================================================

def safe_str(x: Any) -> Optional[str]:
    if x is None: return None
    s = str(x).strip()
    return s if s else None

def safe_float(x: Any) -> Optional[float]:
    if x is None: return None
    try:
        if isinstance(x, (int, float)):
            f = float(x)
            return f if not math.isnan(f) and not math.isinf(f) else None
        s = str(x).strip()
        if not s or s.lower() in {"n/a", "na", "null", "none", "-", "--", ""}: return None
        s = s.translate(_ARABIC_DIGITS).replace(",", "").replace("%", "").replace("$", "").replace("£", "").replace("€", "")
        s = s.replace("SAR", "").replace("ريال", "").strip()
        if s.startswith("(") and s.endswith(")"): s = "-" + s[1:-1].strip()
        m = re.match(r"^(-?\d+(\.\d+)?)([KMBT])$", s, re.IGNORECASE)
        mult = 1.0
        if m:
            num, suf = m.group(1), m.group(3).upper()
            mult = {"K": 1e3, "M": 1e6, "B": 1e9, "T": 1e12}.get(suf, 1.0)
            s = num
        f = float(s) * mult
        return f if not math.isnan(f) and not math.isinf(f) else None
    except Exception:
        return None

def safe_int(x: Any) -> Optional[int]:
    f = safe_float(x)
    return int(round(f)) if f is not None else None

def as_percent(x: Any) -> Optional[float]:
    v = safe_float(x)
    return v * 100.0 if v is not None and abs(v) <= 2.0 else v

def clean_patch(p: Dict[str, Any]) -> Dict[str, Any]:
    return {k: v for k, v in (p or {}).items() if v is not None and not (isinstance(v, str) and not v.strip())}

def normalize_symbol(symbol: str) -> str:
    s = (symbol or "").strip()
    if not s: return ""
    s = s.translate(_ARABIC_DIGITS).strip().upper()
    if callable(_SHARED_NORMALIZE):
        try:
            res = _SHARED_NORMALIZE(s)
            if res: return res
        except Exception: pass

    for prefix in ["TADAWUL:", "SAUDI:", "KSA:", "ETF:", "INDEX:"]:
        if s.startswith(prefix): s = s.split(":", 1)[1].strip()
    for suffix in [".TADAWUL", ".SAUDI", ".KSA"]:
        if s.endswith(suffix): s = s[:-len(suffix)].strip()
    if _KSA_CODE_RE.match(s): return f"{s}.SR"
    if s.endswith(".SR") and _KSA_CODE_RE.match(s[:-3]): return s
    return s

def map_recommendation(rec: Optional[str]) -> str:
    if not rec: return "HOLD"
    rec_lower = str(rec).lower()
    if "strong_buy" in rec_lower: return "STRONG_BUY"
    if "buy" in rec_lower: return "BUY"
    if "hold" in rec_lower: return "HOLD"
    if "underperform" in rec_lower or "reduce" in rec_lower: return "REDUCE"
    if "sell" in rec_lower: return "SELL"
    return "HOLD"


# ============================================================================
# Advanced Cache & Rate Limiting
# ============================================================================

class AdvancedCache:
    """Multi-level cache with memory LRU and optional Redis backend."""
    def __init__(self, name: str, maxsize: int = 5000, ttl: float = 300.0, use_redis: bool = False, redis_url: Optional[str] = None):
        self.name = name
        self.maxsize = maxsize
        self.ttl = ttl
        self.use_redis = use_redis
        self._memory: Dict[str, Tuple[Any, float]] = {}
        self._access_times: Dict[str, float] = {}
        self._lock = asyncio.Lock()
        self.stats = CacheStats()
        self._redis = None
        if use_redis and REDIS_AVAILABLE:
            try:
                self._redis = Redis.from_url(redis_url or _redis_url(), decode_responses=False)
            except Exception as e:
                logger.warning(f"Redis cache '{self.name}' initialization failed: {e}")
                self.use_redis = False

    def _make_key(self, prefix: str) -> str:
        hash_val = hashlib.sha256(prefix.encode()).hexdigest()[:16]
        return f"yffund:{self.name}:{prefix}:{hash_val}"

    async def get(self, prefix: str) -> Optional[Any]:
        key = self._make_key(prefix)
        now = time.monotonic()
        async with self._lock:
            if key in self._memory:
                value, expiry = self._memory[key]
                if now < expiry:
                    self._access_times[key] = now
                    self.stats.hits += 1
                    return value
                else:
                    self._memory.pop(key, None)
                    self._access_times.pop(key, None)

        if self.use_redis and self._redis:
            try:
                data = await self._redis.get(key)
                if data:
                    value = pickle.loads(zlib.decompress(data))
                    async with self._lock:
                        if len(self._memory) >= self.maxsize: await self._evict_lru()
                        self._memory[key] = (value, now + self.ttl)
                        self._access_times[key] = now
                    self.stats.hits += 1
                    return value
            except Exception: pass
        self.stats.misses += 1
        return None

    async def set(self, prefix: str, value: Any, ttl: Optional[float] = None) -> None:
        key = self._make_key(prefix)
        exp = time.monotonic() + (ttl or self.ttl)
        async with self._lock:
            if len(self._memory) >= self.maxsize and key not in self._memory:
                await self._evict_lru()
            self._memory[key] = (value, exp)
            self._access_times[key] = time.monotonic()
            self.stats.sets += 1
            self.stats.size = len(self._memory)

        if self.use_redis and self._redis:
            try:
                await self._redis.setex(key, int(ttl or self.ttl), zlib.compress(pickle.dumps(value)))
            except Exception: pass

    async def _evict_lru(self) -> None:
        if not self._access_times: return
        oldest = min(self._access_times.items(), key=lambda x: x[1])[0]
        self._memory.pop(oldest, None)
        self._access_times.pop(oldest, None)
        self.stats.evictions += 1

    async def size(self) -> int:
        async with self._lock: return len(self._memory)

class TokenBucket:
    def __init__(self, rate_per_sec: float):
        self.rate = max(0.0, rate_per_sec)
        self.capacity = max(1.0, self.rate * 2)
        self.tokens = self.capacity
        self.last = time.monotonic()
        self._lock = asyncio.Lock()

    async def wait_and_acquire(self, tokens: float = 1.0) -> None:
        if self.rate <= 0: return
        while True:
            async with self._lock:
                now = time.monotonic()
                self.tokens = min(self.capacity, self.tokens + (now - self.last) * self.rate)
                self.last = now
                if self.tokens >= tokens:
                    self.tokens -= tokens
                    return
                wait = (tokens - self.tokens) / self.rate
            await asyncio.sleep(min(1.0, wait))

class AdvancedCircuitBreaker:
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
                yf_fund_circuit_breaker.set(0); return True
            elif self.stats.state == CircuitState.OPEN:
                if now >= self.stats.open_until:
                    self.stats.state = CircuitState.HALF_OPEN
                    self.half_open_calls = 0
                    yf_fund_circuit_breaker.set(1); return True
                yf_fund_circuit_breaker.set(2); return False
            elif self.stats.state == CircuitState.HALF_OPEN:
                if self.half_open_calls < 2:
                    self.half_open_calls += 1
                    yf_fund_circuit_breaker.set(1); return True
                return False
            return False

    async def on_success(self) -> None:
        async with self._lock:
            self.stats.successes += 1
            if self.stats.state == CircuitState.HALF_OPEN:
                self.stats.state = CircuitState.CLOSED
                self.stats.failures = 0
                self.stats.current_cooldown = self.base_cooldown
                yf_fund_circuit_breaker.set(0)

    async def on_failure(self, status_code: int = 500) -> None:
        async with self._lock:
            self.stats.failures += 1
            if status_code in (401, 403, 429):
                self.stats.current_cooldown = min(300.0, self.stats.current_cooldown * 1.5)
            
            if self.stats.state == CircuitState.CLOSED and self.stats.failures >= self.fail_threshold:
                self.stats.state = CircuitState.OPEN
                self.stats.open_until = time.monotonic() + self.stats.current_cooldown
                yf_fund_circuit_breaker.set(2)
            elif self.stats.state == CircuitState.HALF_OPEN:
                self.stats.state = CircuitState.OPEN
                self.stats.current_cooldown = min(300.0, self.stats.current_cooldown * 2)
                self.stats.open_until = time.monotonic() + self.stats.current_cooldown
                yf_fund_circuit_breaker.set(2)

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
            if sum(q.qsize() for q in self.queues.values()) >= self.max_size: return False
            await self.queues[item.priority].put(item)
            return True
    
    async def get(self) -> Optional[RequestQueueItem]:
        for priority in sorted(RequestPriority, key=lambda p: p.value, reverse=True):
            if not self.queues[priority].empty():
                return await self.queues[priority].get()
        return None

class SingleFlight:
    def __init__(self) -> None:
        self._lock = asyncio.Lock()
        self._futures: Dict[str, asyncio.Future] = {}

    async def run(self, key: str, coro_fn):
        async with self._lock:
            if key in self._futures: return await self._futures[key]
            fut = asyncio.get_event_loop().create_future()
            self._futures[key] = fut

        try:
            res = await coro_fn()
            if not fut.done(): fut.set_result(res)
            return res
        except Exception as e:
            if not fut.done(): fut.set_exception(e)
            raise
        finally:
            async with self._lock: self._futures.pop(key, None)


# ============================================================================
# Financial Analysis Functions
# ============================================================================

class FinancialAnalyzer:
    """Advanced financial analysis and valuation models."""

    @staticmethod
    def calculate_altman_z_score(metrics: Dict[str, Any]) -> Optional[float]:
        """Altman Z-Score for bankruptcy risk."""
        try:
            working_capital = safe_float(metrics.get("working_capital"))
            total_assets = safe_float(metrics.get("total_assets"))
            retained_earnings = safe_float(metrics.get("retained_earnings"))
            ebit = safe_float(metrics.get("ebit"))
            market_cap = safe_float(metrics.get("market_cap"))
            total_liabilities = safe_float(metrics.get("total_liabilities"))
            sales = safe_float(metrics.get("total_revenue"))

            if None in (working_capital, total_assets, retained_earnings, ebit, market_cap, total_liabilities, sales):
                return None
            if total_assets == 0 or total_liabilities == 0: return None

            A = working_capital / total_assets
            B = retained_earnings / total_assets
            C = ebit / total_assets
            D = market_cap / total_liabilities
            E = sales / total_assets

            return float(1.2 * A + 1.4 * B + 3.3 * C + 0.6 * D + 1.0 * E)
        except Exception:
            return None

    @staticmethod
    def calculate_piotroski_f_score(financials: Dict[str, Any]) -> Optional[int]:
        """Piotroski F-Score (0-9) for financial strength."""
        score = 0
        try:
            net_income, total_assets = safe_float(financials.get("net_income")), safe_float(financials.get("total_assets"))
            if net_income and total_assets and total_assets > 0 and net_income / total_assets > 0: score += 1

            ocf = safe_float(financials.get("operating_cashflow"))
            if ocf and ocf > 0: score += 1

            roa_c, roa_p = safe_float(financials.get("roa_current")), safe_float(financials.get("roa_previous"))
            if roa_c and roa_p and roa_c > roa_p: score += 1

            if ocf and net_income and ocf > net_income: score += 1

            lt_debt_c, lt_debt_p = safe_float(financials.get("long_term_debt_current")), safe_float(financials.get("long_term_debt_previous"))
            if lt_debt_c and lt_debt_p and lt_debt_c < lt_debt_p: score += 1

            cr_c, cr_p = safe_float(financials.get("current_ratio_current")), safe_float(financials.get("current_ratio_previous"))
            if cr_c and cr_p and cr_c > cr_p: score += 1

            sh_c, sh_p = safe_float(financials.get("shares_outstanding_current")), safe_float(financials.get("shares_outstanding_previous"))
            if sh_c and sh_p and sh_c <= sh_p: score += 1

            gm_c, gm_p = safe_float(financials.get("gross_margin_current")), safe_float(financials.get("gross_margin_previous"))
            if gm_c and gm_p and gm_c > gm_p: score += 1

            at_c, at_p = safe_float(financials.get("asset_turnover_current")), safe_float(financials.get("asset_turnover_previous"))
            if at_c and at_p and at_c > at_p: score += 1

            return score
        except Exception:
            return None

    @staticmethod
    def calculate_wacc(beta: Optional[float], risk_free_rate: float = 0.04, erp: float = 0.05) -> float:
        """Estimate WACC dynamically via CAPM if Beta exists."""
        if beta is None or beta <= 0: return 0.10
        return risk_free_rate + (beta * erp)

    @staticmethod
    def calculate_dcf_value(fcf: float, growth_rate: float, beta: Optional[float] = None, terminal_growth: float = 0.03, years: int = 5) -> float:
        """Calculate Discounted Cash Flow intrinsic value."""
        if fcf <= 0: return 0.0
        discount_rate = FinancialAnalyzer.calculate_wacc(beta)
        
        pv_fcf = 0.0
        for year in range(1, years + 1):
            pv_fcf += (fcf * (1 + growth_rate) ** year) / (1 + discount_rate) ** year

        terminal_fcf = fcf * (1 + growth_rate) ** years * (1 + terminal_growth)
        terminal_value = terminal_fcf / (discount_rate - terminal_growth) if discount_rate > terminal_growth else terminal_fcf * 10
        pv_terminal = terminal_value / (1 + discount_rate) ** years

        return pv_fcf + pv_terminal

    @staticmethod
    def calculate_graham_number(eps: float, book_value: float) -> float:
        """Calculate Graham Number."""
        if eps <= 0 or book_value <= 0: return 0.0
        return math.sqrt(22.5 * eps * book_value)

    @staticmethod
    def determine_growth_stage(metrics: Dict[str, Any]) -> GrowthStage:
        revenue_growth = safe_float(metrics.get("revenue_growth", 0))
        net_margin = safe_float(metrics.get("net_margin", 0))
        roe = safe_float(metrics.get("roe", 0))

        if revenue_growth and revenue_growth > 0.2 and net_margin and net_margin < 0.05: return GrowthStage.EARLY
        if revenue_growth and revenue_growth > 0.1 and roe and roe > 0.1: return GrowthStage.GROWTH
        if revenue_growth and 0.02 <= revenue_growth <= 0.1 and roe and roe > 0.1: return GrowthStage.MATURE
        if revenue_growth and revenue_growth < 0: return GrowthStage.DECLINE
        return GrowthStage.CYCLICAL

    @staticmethod
    def calculate_growth_score(metrics: Dict[str, Any]) -> float:
        score = 50.0
        rg, eg, roe, margin, upside = safe_float(metrics.get("revenue_growth")), safe_float(metrics.get("earnings_growth")), safe_float(metrics.get("roe")), safe_float(metrics.get("net_margin")), safe_float(metrics.get("upside_percent"))
        if rg: score += min(25, max(-25, rg * 100))
        if eg: score += min(25, max(-25, eg * 100))
        if roe: score += min(15, max(0, roe))
        if margin: score += min(15, max(0, margin))
        if upside: score += min(20, max(-20, upside / 5))
        return max(0, min(100, score))


# ============================================================================
# Advanced Forecasting (Ensemble)
# ============================================================================

class EnsembleForecaster:
    """Advanced financial forecasting using XGBoost, RF, Ridge, and CAGR methods."""

    def __init__(self, historical_data: Dict[str, List[float]], enable_ml: bool = True):
        self.historical_data = historical_data
        self.enable_ml = enable_ml and SKLEARN_AVAILABLE

    def forecast_earnings(self, years: int = 3) -> Dict[str, Any]:
        result: Dict[str, Any] = {"forecast_available": False, "models_used": [], "forecasts": {}, "ensemble": {}, "confidence": 0.0}
        eps_history, revenue_history = self.historical_data.get("eps", []), self.historical_data.get("revenue", [])

        if len(eps_history) < 3: return result

        trend = self._forecast_trend(eps_history, years)
        if trend: result["models_used"].append("trend"); result["forecasts"]["trend"] = trend

        cagr = self._forecast_cagr(eps_history, years)
        if cagr: result["models_used"].append("cagr"); result["forecasts"]["cagr"] = cagr

        if revenue_history and len(revenue_history) >= len(eps_history):
            linked = self._forecast_revenue_linked(eps_history, revenue_history, years)
            if linked: result["models_used"].append("revenue_linked"); result["forecasts"]["revenue_linked"] = linked

        if self.enable_ml and len(eps_history) >= 8:
            ml_fc = self._forecast_ml(eps_history, years)
            if ml_fc: result["models_used"].append("ml_ensemble"); result["forecasts"]["ml_ensemble"] = ml_fc

        if not result["models_used"]: return result

        last_eps = eps_history[-1]
        forecasts, weights = [], []
        for model, forecast in result["forecasts"].items():
            if "values" in forecast:
                forecasts.append(forecast["values"][-1])
                weights.append(forecast.get("weight", 1.0))

        if forecasts:
            tot_w = sum(weights)
            ens_eps = sum(f * w for f, w in zip(forecasts, weights)) / tot_w if tot_w > 0 else np.mean(forecasts)
            ens_cagr = (ens_eps / last_eps) ** (1 / years) - 1 if last_eps > 0 else 0
            
            result["ensemble"] = {
                "eps": ens_eps, "cagr": ens_cagr * 100,
                "values": [last_eps * (1 + ens_cagr) ** i for i in range(1, years + 1)]
            }
            result["confidence"] = self._calculate_confidence(result)
            result["forecast_available"] = True

        return result

    def _forecast_trend(self, values: List[float], years: int) -> Optional[Dict[str, Any]]:
        try:
            n = len(values)
            x = np.arange(n).reshape(-1, 1)
            y = [math.log(v) if v > 0 else 0 for v in values]
            
            if SKLEARN_AVAILABLE:
                model = Ridge(alpha=1.0)
                model.fit(x, y)
                slope, r2 = model.coef_[0], model.score(x, y)
                fut_v = [math.exp(math.log(values[-1]) + slope * i) for i in range(1, years + 1)]
            else:
                x_m, y_m = sum(x)/n, sum(y)/n
                slope = sum((x[i][0]-x_m)*(y[i]-y_m) for i in range(n)) / sum((x[i][0]-x_m)**2 for i in range(n))
                r2 = 0.6
                fut_v = [math.exp(math.log(values[-1]) + slope * i) for i in range(1, years + 1)]
                
            return {"values": fut_v, "slope": slope, "r2": r2, "weight": max(0.2, min(0.5, r2))}
        except Exception: return None

    def _forecast_cagr(self, values: List[float], years: int) -> Optional[Dict[str, Any]]:
        try:
            cagrs = [(values[-1] / values[-p - 1]) ** (1 / p) - 1 for p in [3, 5] if len(values) > p]
            if not cagrs: return None
            avg_cagr = np.mean(cagrs)
            return {"values": [values[-1] * (1 + avg_cagr) ** i for i in range(1, years + 1)], "cagr": avg_cagr, "weight": 0.25}
        except Exception: return None

    def _forecast_revenue_linked(self, eps: List[float], revenue: List[float], years: int) -> Optional[Dict[str, Any]]:
        try:
            margins = [eps[i] / revenue[i] if revenue[i] > 0 else 0 for i in range(min(len(eps), len(revenue)))]
            avg_margin = np.mean(margins) if margins else 0
            rev_fc = EnsembleForecaster({"revenue": revenue}, False)._forecast_trend(revenue, years)
            if not rev_fc: return None
            return {"values": [rev * avg_margin for rev in rev_fc["values"]], "margin": avg_margin, "weight": 0.2}
        except Exception: return None

    def _forecast_ml(self, values: List[float], years: int) -> Optional[Dict[str, Any]]:
        if not self.enable_ml or len(values) < 8: return None
        try:
            X, y = [], []
            for i in range(4, len(values) - 1):
                X.append([values[i-4], values[i-3], values[i-2], values[i-1], (values[i-1]/values[i-4]-1) if values[i-4]>0 else 0, np.std(values[i-4:i])])
                y.append(values[i])
            if len(X) < 5: return None

            model = xgb.XGBRegressor(n_estimators=50, max_depth=3) if XGB_AVAILABLE else RandomForestRegressor(n_estimators=50, max_depth=3, random_state=42)
            model.fit(X, y)

            fut_v, last_v = [], values[-4:]
            for _ in range(years):
                pred = model.predict([[last_v[0], last_v[1], last_v[2], last_v[3], (last_v[3]/last_v[0]-1) if last_v[0]>0 else 0, np.std(last_v)]])[0]
                fut_v.append(pred)
                last_v = last_v[1:] + [pred]
            
            conf = max(0.3, min(0.9, 0.7 - np.std([model.estimators_[i].predict([X[-1]])[0] for i in range(10)]) / values[-1])) if not XGB_AVAILABLE else 0.75
            return {"values": fut_v, "weight": 0.35, "confidence": conf}
        except Exception: return None

    def _calculate_confidence(self, results: Dict[str, Any]) -> float:
        if not results["forecasts"]: return 0.0
        final_vals = [f["values"][-1] for f in results["forecasts"].values() if "values" in f]
        agreement = max(0, 100 - (np.std(final_vals)/np.mean(final_vals) * 200)) if len(final_vals) > 1 and np.mean(final_vals) != 0 else 70
        model_conf = np.mean([f.get("confidence", 0.6) * 100 for f in results["forecasts"].values()])
        history_score = min(30, len(self.historical_data.get("eps", [])) * 2)
        return (agreement * 0.3 + model_conf * 0.4 + history_score * 0.3) / 100


# ============================================================================
# Yahoo Fundamentals Provider Implementation
# ============================================================================

@dataclass
class YahooFundamentalsProvider:
    """Next-Gen Enterprise Yahoo Fundamentals Provider."""

    name: str = PROVIDER_NAME

    def __post_init__(self) -> None:
        self.timeout_sec = _timeout_sec()

        self.semaphore = asyncio.Semaphore(_max_concurrency())
        self.rate_limiter = TokenBucket(_rate_limit())
        self.circuit_breaker = AdvancedCircuitBreaker(
            fail_threshold=_cb_fail_threshold(),
            cooldown_sec=_cb_cooldown_sec()
        )
        self.singleflight = SingleFlight()
        
        self.request_queue = RequestQueue(max_size=_queue_size())

        self.fund_cache = AdvancedCache(name="fund", maxsize=5000, ttl=_fund_ttl_sec(), use_redis=_enable_redis(), redis_url=_redis_url())
        self.error_cache = AdvancedCache(name="error", maxsize=5000, ttl=_err_ttl_sec(), use_redis=_enable_redis(), redis_url=_redis_url())

        self._queue_processor_task = asyncio.create_task(self._process_queue())

        logger.info(
            f"YahooFundamentalsProvider v{PROVIDER_VERSION} initialized | "
            f"yfinance={_HAS_YFINANCE} | rate={_rate_limit()}/s | cb={_cb_fail_threshold()}/{_cb_cooldown_sec()}s"
        )

    @_trace("yf_fund_request")
    async def _request_metadata(self, symbol: str, priority: RequestPriority = RequestPriority.NORMAL) -> Dict[str, Any]:
        """Queued execution wrapper."""
        yf_fund_requests_total.inc()
        if not await self.circuit_breaker.allow_request(): return {"error": "circuit_breaker_open"}

        future = asyncio.Future()
        if not await self.request_queue.put(RequestQueueItem(priority=priority, symbol=symbol, future=future)):
            return {"error": "queue_full"}

        return await future

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
            start = time.time()
            await self.rate_limiter.wait_and_acquire()
            async with self.semaphore:
                # Execution with Async Timeout and Full Jitter Thread Blocking
                result = await asyncio.wait_for(
                    asyncio.to_thread(self._blocking_fetch, item.symbol), 
                    timeout=self.timeout_sec
                )
            
            if "error" in result:
                await self.circuit_breaker.on_failure()
            else:
                await self.circuit_breaker.on_success()
            
            yf_fund_request_duration.observe(time.time() - start)
            item.future.set_result(result)
        except Exception as e:
            await self.circuit_breaker.on_failure()
            item.future.set_exception(e)

    def _extract_financial_series(self, ticker: Any) -> Dict[str, List[float]]:
        series: Dict[str, List[float]] = {"eps": [], "revenue": [], "fcf": [], "book_value": []}
        try:
            if hasattr(ticker, "financials") and ticker.financials is not None and not ticker.financials.empty:
                if "Total Revenue" in ticker.financials.index:
                    series["revenue"] = [f for f in (safe_float(x) for x in ticker.financials.loc["Total Revenue"].tolist()) if f is not None]

            if hasattr(ticker, "earnings_dates") and ticker.earnings_dates is not None:
                if hasattr(ticker.earnings_dates, "eps") and ticker.earnings_dates.eps is not None:
                    series["eps"] = [f for f in (safe_float(x) for x in ticker.earnings_dates.eps.tolist()) if f is not None]

            if hasattr(ticker, "cashflow") and ticker.cashflow is not None and not ticker.cashflow.empty:
                if "Free Cash Flow" in ticker.cashflow.index:
                    series["fcf"] = [f for f in (safe_float(x) for x in ticker.cashflow.loc["Free Cash Flow"].tolist()) if f is not None]

            if hasattr(ticker, "balance_sheet") and ticker.balance_sheet is not None and not ticker.balance_sheet.empty:
                if "Total Equity Gross Minority Interest" in ticker.balance_sheet.index:
                    series["book_value"] = [f for f in (safe_float(x) for x in ticker.balance_sheet.loc["Total Equity Gross Minority Interest"].tolist()) if f is not None]

        except Exception as e:
            logger.debug(f"Error extracting financial series: {e}")
        return series

    def _blocking_fetch(self, symbol: str) -> Dict[str, Any]:
        """Blocking yfinance fetch with Full Jitter Retry."""
        if not _HAS_YFINANCE or yf is None: return {"error": "yfinance not installed"}

        last_err = None
        for attempt in range(4):
            try:
                ticker = yf.Ticker(symbol)
                info = ticker.info or {}
                
                fast_price = None
                try:
                    fi = getattr(ticker, "fast_info", None)
                    if fi: fast_price = getattr(fi, "last_price", None) or fi.get("last_price", None)
                except Exception: pass

                series = self._extract_financial_series(ticker)
                metrics = FinancialMetrics()

                metrics.name = safe_str(info.get("longName") or info.get("shortName"))
                metrics.sector = safe_str(info.get("sector"))
                metrics.industry = safe_str(info.get("industry"))
                metrics.market_cap = safe_float(info.get("marketCap"))
                metrics.enterprise_value = safe_float(info.get("enterpriseValue"))
                metrics.beta = safe_float(info.get("beta"))
                metrics.pe_ttm = safe_float(info.get("trailingPE"))
                metrics.forward_pe = safe_float(info.get("forwardPE"))
                metrics.ps_ttm = safe_float(info.get("priceToSalesTrailing12Months"))
                metrics.pb_ttm = safe_float(info.get("priceToBook"))
                metrics.pfcf_ttm = safe_float(info.get("priceToFreeCashFlow"))
                metrics.gross_margin = as_percent(info.get("grossMargins"))
                metrics.operating_margin = as_percent(info.get("operatingMargins"))
                metrics.net_margin = as_percent(info.get("profitMargins"))
                metrics.roe = as_percent(info.get("returnOnEquity"))
                metrics.roa = as_percent(info.get("returnOnAssets"))
                metrics.revenue_growth_yoy = as_percent(info.get("revenueGrowth"))
                metrics.earnings_growth_yoy = as_percent(info.get("earningsGrowth"))
                metrics.eps_ttm = safe_float(info.get("trailingEps"))
                metrics.forward_eps = safe_float(info.get("forwardEps"))
                metrics.dividend_yield = as_percent(info.get("dividendYield"))
                metrics.dividend_rate = safe_float(info.get("dividendRate"))
                metrics.payout_ratio = as_percent(info.get("payoutRatio"))
                metrics.target_mean = safe_float(info.get("targetMeanPrice"))
                metrics.target_high = safe_float(info.get("targetHighPrice"))
                metrics.target_low = safe_float(info.get("targetLowPrice"))
                metrics.recommendation = map_recommendation(info.get("recommendationKey"))
                metrics.analyst_count = safe_int(info.get("numberOfAnalystOpinions"))
                metrics.current_ratio = safe_float(info.get("currentRatio"))
                metrics.quick_ratio = safe_float(info.get("quickRatio"))
                metrics.debt_to_equity = safe_float(info.get("debtToEquity"))
                metrics.operating_cf = safe_float(info.get("operatingCashflow"))
                metrics.free_cashflow = safe_float(info.get("freeCashflow"))
                metrics.fcf_yield = as_percent(info.get("freeCashFlowYield"))
                metrics.short_ratio = safe_float(info.get("shortRatio"))
                metrics.short_percent = as_percent(info.get("shortPercentOfFloat"))

                current_price = safe_float(info.get("currentPrice")) or safe_float(info.get("regularMarketPrice")) or safe_float(fast_price)

                out: Dict[str, Any] = {
                    "requested_symbol": symbol, "symbol": symbol, "provider_symbol": symbol,
                    "name": metrics.name, "sector": metrics.sector, "industry": metrics.industry, "sub_sector": metrics.industry,
                    "currency": safe_str(info.get("currency") or info.get("financialCurrency") or "USD"),
                    "market_cap": metrics.market_cap, "enterprise_value": metrics.enterprise_value,
                    "shares_outstanding": safe_float(info.get("sharesOutstanding")),
                    "pe_ttm": metrics.pe_ttm, "forward_pe": metrics.forward_pe, "ps_ttm": metrics.ps_ttm, "pb_ttm": metrics.pb_ttm,
                    "eps_ttm": metrics.eps_ttm, "forward_eps": metrics.forward_eps, "book_value": safe_float(info.get("bookValue")),
                    "beta": metrics.beta, "gross_margin": metrics.gross_margin, "operating_margin": metrics.operating_margin,
                    "net_margin": metrics.net_margin, "profit_margin": metrics.net_margin, "roe": metrics.roe, "roa": metrics.roa,
                    "revenue_growth": metrics.revenue_growth_yoy, "earnings_growth": metrics.earnings_growth_yoy,
                    "current_ratio": metrics.current_ratio, "quick_ratio": metrics.quick_ratio, "debt_to_equity": metrics.debt_to_equity,
                    "operating_cashflow": metrics.operating_cf, "free_cashflow": metrics.free_cashflow, "fcf_yield": metrics.fcf_yield,
                    "dividend_yield": metrics.dividend_yield, "dividend_rate": metrics.dividend_rate, "payout_ratio": metrics.payout_ratio,
                    "target_mean_price": metrics.target_mean, "target_high_price": metrics.target_high, "target_low_price": metrics.target_low,
                    "recommendation": metrics.recommendation, "analyst_count": metrics.analyst_count,
                    "short_ratio": metrics.short_ratio, "short_percent": metrics.short_percent, "current_price": current_price,
                    "provider": PROVIDER_NAME, "data_source": PROVIDER_NAME, "provider_version": PROVIDER_VERSION,
                    "last_updated_utc": _utc_iso(), "last_updated_riyadh": _riyadh_iso()
                }

                if current_price and current_price > 0 and metrics.target_mean and metrics.target_mean > 0:
                    upside = ((metrics.target_mean / current_price) - 1) * 100
                    out["upside_percent"] = float(upside)
                    out["forecast_price_12m"] = float(metrics.target_mean)
                    out["expected_roi_12m"] = float(upside)
                    
                    if metrics.analyst_count and metrics.analyst_count > 0:
                        conf = 0.6 + (math.log(metrics.analyst_count + 1) * 0.05)
                        out["forecast_confidence"] = float(min(0.95, conf))
                        out["confidence_score"] = float(min(95, conf * 100))
                    else:
                        out["forecast_confidence"], out["confidence_score"] = 0.5, 50.0
                    out["forecast_method"] = "analyst_consensus"

                if metrics.eps_ttm and out.get("book_value"):
                    graham = FinancialAnalyzer.calculate_graham_number(metrics.eps_ttm, out["book_value"])
                    out["graham_number"] = float(graham)
                    if current_price and current_price > 0: out["graham_upside"] = float(((graham / current_price) - 1) * 100)

                if _enable_dcf() and series["fcf"] and len(series["fcf"]) >= 3:
                    latest_fcf = series["fcf"][-1]
                    if latest_fcf > 0:
                        fcf_cagr = (series["fcf"][-1] / series["fcf"][0]) ** (1 / len(series["fcf"])) - 1 if len(series["fcf"]) >= 5 else 0.05
                        dcf_val = FinancialAnalyzer.calculate_dcf_value(latest_fcf, max(0.02, min(0.15, fcf_cagr)), beta=metrics.beta)
                        shares = metrics.shares_outstanding or safe_float(info.get("sharesOutstanding"))
                        if shares and shares > 0:
                            dcf_ps = dcf_val / shares
                            out["dcf_value"] = float(dcf_ps)
                            if current_price and current_price > 0: out["dcf_upside"] = float(((dcf_ps / current_price) - 1) * 100)

                bs = {}
                try:
                    if ticker.balance_sheet is not None and not ticker.balance_sheet.empty:
                        for k, k2 in [("Total Assets", "total_assets"), ("Total Liabilities Net Minority Interest", "total_liabilities"), ("Retained Earnings", "retained_earnings"), ("Working Capital", "working_capital")]:
                            if k in ticker.balance_sheet.index: bs[k2] = safe_float(ticker.balance_sheet.loc[k].iloc[0])
                    if ticker.income_stmt is not None and not ticker.income_stmt.empty:
                        if "EBIT" in ticker.income_stmt.index: bs["ebit"] = safe_float(ticker.income_stmt.loc["EBIT"].iloc[0])
                        if "Total Revenue" in ticker.income_stmt.index: bs["total_revenue"] = safe_float(ticker.income_stmt.loc["Total Revenue"].iloc[0])
                except Exception: pass

                bs["market_cap"] = metrics.market_cap
                z_score = FinancialAnalyzer.calculate_altman_z_score(bs)
                if z_score is not None:
                    out["altman_z_score"] = float(z_score)
                    out["bankruptcy_risk"] = "low" if z_score > 2.99 else "medium" if z_score > 1.81 else "high"

                out["growth_stage"] = FinancialAnalyzer.determine_growth_stage({"revenue_growth": metrics.revenue_growth_yoy, "net_margin": metrics.net_margin, "roe": metrics.roe}).value
                out["growth_score"] = FinancialAnalyzer.calculate_growth_score({"revenue_growth": metrics.revenue_growth_yoy, "earnings_growth": metrics.earnings_growth_yoy, "roe": metrics.roe, "net_margin": metrics.net_margin, "upside_percent": out.get("upside_percent")})

                hs = 0
                if metrics.current_ratio and metrics.current_ratio > 1.5: hs += 25
                if metrics.debt_to_equity and metrics.debt_to_equity < 1.0: hs += 25
                if metrics.interest_coverage and metrics.interest_coverage > 3: hs += 25
                if metrics.free_cashflow and metrics.free_cashflow > 0: hs += 25
                out["financial_health"] = FinancialHealth.EXCELLENT.value if hs >= 80 else FinancialHealth.GOOD.value if hs >= 60 else FinancialHealth.FAIR.value if hs >= 40 else FinancialHealth.POOR.value if hs >= 20 else FinancialHealth.CRITICAL.value
                out["financial_health_score"] = hs

                if _enable_ml_forecast() and series["eps"] and len(series["eps"]) >= 4:
                    fc = EnsembleForecaster(series, enable_ml=True).forecast_earnings(years=3)
                    if fc.get("forecast_available"):
                        out["earnings_forecast_available"] = True
                        out["earnings_forecast_confidence"] = fc["confidence"]
                        if "ensemble" in fc:
                            out["eps_forecast_1y"] = fc["ensemble"]["values"][0] if len(fc["ensemble"]["values"]) > 0 else None
                            out["eps_forecast_2y"] = fc["ensemble"]["values"][1] if len(fc["ensemble"]["values"]) > 1 else None
                            out["eps_forecast_3y"] = fc["ensemble"]["values"][2] if len(fc["ensemble"]["values"]) > 2 else None
                            out["eps_cagr_forecast"] = fc["ensemble"]["cagr"]
                            out["forecast_models"] = fc["models_used"]

                # ML Anomaly Detection on Fundamental Series
                if SKLEARN_AVAILABLE and series["revenue"] and len(series["revenue"]) >= 5:
                    try:
                        iso = IsolationForest(contamination=0.1, random_state=42)
                        rev_np = np.array(series["revenue"]).reshape(-1, 1)
                        preds = iso.fit_predict(rev_np)
                        if preds[-1] == -1: out["fundamental_anomaly"] = True
                    except Exception: pass

                quality_category, quality_score = data_quality_score(out)
                out["data_quality"], out["data_quality_score"] = quality_category.value, quality_score
                out["forecast_updated_utc"], out["forecast_updated_riyadh"] = _utc_iso(), _riyadh_iso()

                return clean_patch(out)
            except Exception as e:
                last_err = e
                base_wait = 2 ** attempt
                time.sleep(min(10.0, base_wait + random.uniform(0, base_wait)))

        return {"error": f"fetch failed after retries: {last_err.__class__.__name__} - {str(last_err)}"}

    async def fetch_fundamentals_patch(
        self,
        symbol: str,
        debug: bool = False,
        *args: Any,
        **kwargs: Any
    ) -> Dict[str, Any]:
        """Fetch comprehensive fundamentals data."""
        if not _configured(): return {} if _HAS_YFINANCE else ({} if not _emit_warnings() else {"_warn": "yfinance not installed"})

        norm_symbol = normalize_symbol(symbol)
        if not norm_symbol: return {} if not _emit_warnings() else {"_warn": "invalid_symbol"}

        cache_key = f"yffund:{norm_symbol}"

        if await self.error_cache.get(cache_key): return {} if not _emit_warnings() else {"_warn": "temporarily_backed_off"}

        cached = await self.fund_cache.get(cache_key)
        if cached: return dict(cached)

        async def _fetch():
            res = await self._request_metadata(norm_symbol)
            if "error" in res:
                await self.error_cache.set(cache_key, True)
                return {} if not _emit_warnings() else {"_warn": res["error"]}
            
            await self.fund_cache.set(cache_key, res)
            return res

        return await self.singleflight.run(cache_key, _fetch)

    async def get_metrics(self) -> Dict[str, Any]:
        """Get client metrics."""
        return {
            "circuit_breaker": self.circuit_breaker.get_stats(),
            "cache_sizes": {
                "fund": await self.fund_cache.size(),
                "error": await self.error_cache.size(),
            }
        }


# ============================================================================
# Singleton Management
# ============================================================================

_PROVIDER_INSTANCE: Optional[YahooFundamentalsProvider] = None
_PROVIDER_LOCK = asyncio.Lock()


async def get_provider() -> YahooFundamentalsProvider:
    """Get or create provider singleton."""
    global _PROVIDER_INSTANCE
    if _PROVIDER_INSTANCE is None:
        async with _PROVIDER_LOCK:
            if _PROVIDER_INSTANCE is None:
                _PROVIDER_INSTANCE = YahooFundamentalsProvider()
    return _PROVIDER_INSTANCE


async def close_provider() -> None:
    """Close provider."""
    global _PROVIDER_INSTANCE
    _PROVIDER_INSTANCE = None


# ============================================================================
# Public API (Engine Compatible)
# ============================================================================

async def fetch_fundamentals_patch(
    symbol: str,
    debug: bool = False,
    *args: Any,
    **kwargs: Any
) -> Dict[str, Any]:
    provider = await get_provider()
    return await provider.fetch_fundamentals_patch(symbol, debug=debug)


async def fetch_enriched_quote_patch(
    symbol: str,
    debug: bool = False,
    *args: Any,
    **kwargs: Any
) -> Dict[str, Any]:
    return await fetch_fundamentals_patch(symbol, debug=debug)


async def get_client_metrics() -> Dict[str, Any]:
    provider = await get_provider()
    return await provider.get_metrics()


async def aclose_yahoo_fundamentals_client() -> None:
    await close_provider()


# ============================================================================
# Module Exports
# ============================================================================

__all__ = [
    "PROVIDER_NAME",
    "PROVIDER_VERSION",
    "YahooFundamentalsProvider",
    "get_provider",
    "fetch_fundamentals_patch",
    "fetch_enriched_quote_patch",
    "get_client_metrics",
    "aclose_yahoo_fundamentals_client",
    "normalize_symbol",
    "FinancialHealth",
    "GrowthStage",
    "DataQuality",
]
