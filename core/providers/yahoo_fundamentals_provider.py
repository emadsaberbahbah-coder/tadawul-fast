#!/usr/bin/env python3
# core/providers/yahoo_fundamentals_provider.py
"""
================================================================================
Yahoo Finance Fundamentals Provider — v5.2.0 (PROD-HARDENED, ALIGNED)
================================================================================
(Emad Bahbah – Enterprise Integration Architecture)

Key upgrades vs v5.1.2
- ✅ Alignment: outputs data_quality as UPPERCASE (EXCELLENT/HIGH/MEDIUM/LOW/ERROR/MISSING/STALE)
- ✅ Optional deps: removed hard numpy import (no startup crash); yfinance/pandas remain optional
- ✅ Stronger symbol normalization (Arabic digits + KSA codes -> .SR)
- ✅ Better patch shape: adds common aliases (pb/ps, dividend_yield_percent, etc.) for downstream scorers
- ✅ Safer SingleFlight (clean owner logic; no awaiting under lock)
- ✅ Correct OpenTelemetry usage (start_as_current_span CM, set_attribute per key; safe fallbacks)
- ✅ Better cache + CB behavior (monotonic time, progressive cooldown scaling on 401/403/429)
- ✅ Engine-compatible public API preserved:
      - fetch_fundamentals_patch()
      - fetch_enriched_quote_patch()
      - get_client_metrics()
      - aclose_yahoo_fundamentals_client()
================================================================================
"""

from __future__ import annotations

import asyncio
import functools
import hashlib
import json
import logging
import math
import os
import pickle
import random
import re
import time
import zlib
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from enum import Enum
from typing import Any, Awaitable, Callable, Dict, List, Optional, Tuple

logger = logging.getLogger("core.providers.yahoo_fundamentals_provider")

PROVIDER_NAME = "yahoo_fundamentals"
PROVIDER_VERSION = "5.2.0"

_TRUTHY = {"1", "true", "yes", "y", "on", "t", "enabled", "enable"}
_FALSY = {"0", "false", "no", "n", "off", "f", "disabled", "disable"}

# KSA numeric symbols (1120 -> 1120.SR)
_KSA_CODE_RE = re.compile(r"^\d{3,6}$", re.IGNORECASE)

# Arabic digit translation
_ARABIC_DIGITS = str.maketrans("٠١٢٣٤٥٦٧٨٩", "0123456789")

# ---------------------------------------------------------------------------
# High-Performance JSON fallback
# ---------------------------------------------------------------------------
try:
    import orjson  # type: ignore

    def json_dumps(obj: Any) -> str:
        return orjson.dumps(obj, default=str).decode("utf-8")

except Exception:

    def json_dumps(obj: Any) -> str:
        return json.dumps(obj, default=str, ensure_ascii=False)

# ---------------------------------------------------------------------------
# Optional stacks
# ---------------------------------------------------------------------------
try:
    from redis.asyncio import Redis  # type: ignore

    _REDIS_AVAILABLE = True
except Exception:
    Redis = None  # type: ignore
    _REDIS_AVAILABLE = False

try:
    from prometheus_client import Counter, Gauge, Histogram  # type: ignore

    _PROMETHEUS_AVAILABLE = True
except Exception:
    Counter = Gauge = Histogram = None  # type: ignore
    _PROMETHEUS_AVAILABLE = False

try:
    from opentelemetry import trace  # type: ignore
    from opentelemetry.trace import Status, StatusCode  # type: ignore

    _OTEL_AVAILABLE = True
except Exception:
    trace = None  # type: ignore
    Status = StatusCode = None  # type: ignore
    _OTEL_AVAILABLE = False

try:
    import yfinance as yf  # type: ignore
    import pandas as pd  # type: ignore  # noqa: F401

    _HAS_YFINANCE = True
except Exception:
    yf = None  # type: ignore
    _HAS_YFINANCE = False

# ---------------------------------------------------------------------------
# Metrics (safe)
# ---------------------------------------------------------------------------
if _PROMETHEUS_AVAILABLE and Counter and Gauge and Histogram:
    yf_fund_requests_total = Counter(
        "yf_fund_requests_total",
        "Total Yahoo fundamentals provider requests",
        ["status"],
    )
    yf_fund_request_duration = Histogram(
        "yf_fund_request_duration_seconds",
        "Yahoo fundamentals provider request duration (seconds)",
        buckets=[0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0],
    )
    yf_fund_circuit_breaker_state = Gauge(
        "yf_fund_circuit_breaker_state",
        "Circuit breaker state (0=closed,1=half_open,2=open)",
    )
else:

    class _DummyMetric:
        def labels(self, *args, **kwargs):
            return self

        def inc(self, *args, **kwargs):
            return None

        def observe(self, *args, **kwargs):
            return None

        def set(self, *args, **kwargs):
            return None

    yf_fund_requests_total = _DummyMetric()
    yf_fund_request_duration = _DummyMetric()
    yf_fund_circuit_breaker_state = _DummyMetric()

# ---------------------------------------------------------------------------
# Tracing helpers (safe + correct)
# ---------------------------------------------------------------------------
_TRACING_ENABLED = (os.getenv("YF_TRACING_ENABLED", "").strip().lower() in _TRUTHY) and _OTEL_AVAILABLE


class TraceContext:
    """
    Correct OTEL handling:
    tracer.start_as_current_span() returns a context manager.
    Enter it to get span (maybe None).
    """

    def __init__(self, name: str, attrs: Optional[Dict[str, Any]] = None):
        self.name = name
        self.attrs = attrs or {}
        self._cm = None
        self.span = None
        self.tracer = trace.get_tracer(__name__) if (_TRACING_ENABLED and trace) else None

    async def __aenter__(self):
        if self.tracer:
            try:
                self._cm = self.tracer.start_as_current_span(self.name)
                self.span = self._cm.__enter__()
                if self.span and self.attrs:
                    for k, v in self.attrs.items():
                        try:
                            self.span.set_attribute(str(k), v)
                        except Exception:
                            pass
            except Exception:
                self._cm = None
                self.span = None
        return self

    async def __aexit__(self, exc_type, exc, tb):
        if self.span and _OTEL_AVAILABLE and exc and Status and StatusCode:
            try:
                if hasattr(self.span, "record_exception"):
                    self.span.record_exception(exc)
                if hasattr(self.span, "set_status"):
                    self.span.set_status(Status(StatusCode.ERROR, str(exc)))
            except Exception:
                pass
        if self._cm:
            try:
                self._cm.__exit__(exc_type, exc, tb)
            except Exception:
                pass
        return False


def _trace(name: Optional[str] = None):
    def deco(func):
        @functools.wraps(func)
        async def wrapper(*args, **kwargs):
            if not _TRACING_ENABLED:
                return await func(*args, **kwargs)
            async with TraceContext(name or func.__name__, {"function": func.__name__}):
                return await func(*args, **kwargs)

        return wrapper

    return deco


# ---------------------------------------------------------------------------
# Env helpers
# ---------------------------------------------------------------------------
def _env_str(name: str, default: str = "") -> str:
    v = os.getenv(name)
    v = str(v).strip() if v is not None else ""
    return v if v else default


def _env_int(name: str, default: int) -> int:
    try:
        v = os.getenv(name)
        return int(str(v).strip()) if v is not None else default
    except Exception:
        return default


def _env_float(name: str, default: float) -> float:
    try:
        v = os.getenv(name)
        return float(str(v).strip()) if v is not None else default
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


def _configured() -> bool:
    if not _env_bool("YF_ENABLED", True):
        return False
    return _HAS_YFINANCE


def _emit_warnings() -> bool:
    return _env_bool("YF_VERBOSE_WARNINGS", False)


def _timeout_sec() -> float:
    return max(5.0, _env_float("YF_TIMEOUT_SEC", 25.0))


def _fund_ttl_sec() -> float:
    return max(300.0, _env_float("YF_FUND_TTL_SEC", 21600.0))  # default 6h


def _err_ttl_sec() -> float:
    return max(5.0, _env_float("YF_ERROR_TTL_SEC", 15.0))


def _max_concurrency() -> int:
    return max(1, _env_int("YF_MAX_CONCURRENCY", 6))


def _rate_limit() -> float:
    return max(0.0, _env_float("YF_RATE_LIMIT_PER_SEC", 4.0))


def _cb_enabled() -> bool:
    return _env_bool("YF_CIRCUIT_BREAKER", True)


def _cb_fail_threshold() -> int:
    return max(2, _env_int("YF_CB_FAIL_THRESHOLD", 6))


def _cb_cooldown_sec() -> float:
    return max(5.0, _env_float("YF_CB_COOLDOWN_SEC", 30.0))


def _enable_redis() -> bool:
    return _env_bool("YF_ENABLE_REDIS", False) and _REDIS_AVAILABLE


def _redis_url() -> str:
    return _env_str("REDIS_URL", "redis://localhost:6379/0")


def _utc_iso(dt: Optional[datetime] = None) -> str:
    d = dt or datetime.now(timezone.utc)
    if d.tzinfo is None:
        d = d.replace(tzinfo=timezone.utc)
    return d.astimezone(timezone.utc).isoformat()


def _riyadh_iso(dt: Optional[datetime] = None) -> str:
    tz = timezone(timedelta(hours=3))
    d = dt or datetime.now(tz)
    if d.tzinfo is None:
        d = d.replace(tzinfo=tz)
    return d.astimezone(tz).isoformat()


# ---------------------------------------------------------------------------
# Safe type helpers
# ---------------------------------------------------------------------------
def safe_str(x: Any) -> Optional[str]:
    if x is None:
        return None
    s = str(x).strip()
    return s if s else None


def safe_float(x: Any) -> Optional[float]:
    if x is None:
        return None
    try:
        if isinstance(x, bool):
            return None
        if isinstance(x, (int, float)):
            f = float(x)
            if math.isnan(f) or math.isinf(f):
                return None
            return f

        s = str(x).strip()
        if not s:
            return None
        if s.lower() in {"n/a", "na", "null", "none", "-", "--"}:
            return None

        s = s.translate(_ARABIC_DIGITS)
        s = s.replace(",", "").replace("%", "").replace("$", "").replace("£", "").replace("€", "")
        s = s.replace("SAR", "").replace("ريال", "").strip()

        if s.startswith("(") and s.endswith(")"):
            s = "-" + s[1:-1].strip()

        m = re.match(r"^(-?\d+(\.\d+)?)([KMBT])$", s, re.IGNORECASE)
        mult = 1.0
        if m:
            num, suf = m.group(1), m.group(3).upper()
            mult = {"K": 1e3, "M": 1e6, "B": 1e9, "T": 1e12}.get(suf, 1.0)
            s = num

        f = float(s) * mult
        if math.isnan(f) or math.isinf(f):
            return None
        return f
    except Exception:
        return None


def safe_int(x: Any) -> Optional[int]:
    f = safe_float(x)
    return int(round(f)) if f is not None else None


def as_percent(x: Any) -> Optional[float]:
    """
    If x is a ratio 0.12 -> 12.0.
    If x already looks like percent 12 -> keep as 12.
    """
    v = safe_float(x)
    if v is None:
        return None
    return v * 100.0 if abs(v) <= 2.0 else v


def clean_patch(p: Dict[str, Any]) -> Dict[str, Any]:
    return {
        k: v
        for k, v in (p or {}).items()
        if v is not None and not (isinstance(v, str) and not v.strip())
    }


def normalize_symbol(symbol: str) -> str:
    s = (symbol or "").strip()
    if not s:
        return ""
    s = s.translate(_ARABIC_DIGITS).strip().upper()

    for prefix in ("TADAWUL:", "SAUDI:", "KSA:", "ETF:", "INDEX:"):
        if s.startswith(prefix):
            s = s.split(":", 1)[1].strip()

    for suffix in (".TADAWUL", ".SAUDI", ".KSA"):
        if s.endswith(suffix):
            s = s[: -len(suffix)].strip()

    if _KSA_CODE_RE.match(s):
        return f"{s}.SR"

    if s.endswith(".SR") and _KSA_CODE_RE.match(s[:-3]):
        return s

    return s


def map_recommendation(rec: Optional[str]) -> str:
    if not rec:
        return "HOLD"
    r = str(rec).lower()
    if "strong_buy" in r or "strongbuy" in r:
        return "STRONG_BUY"
    if "buy" in r:
        return "BUY"
    if "hold" in r or "neutral" in r:
        return "HOLD"
    if "underperform" in r or "reduce" in r:
        return "REDUCE"
    if "sell" in r:
        return "SELL"
    return "HOLD"


# ---------------------------------------------------------------------------
# Data quality alignment (UPPERCASE, system-wide)
# ---------------------------------------------------------------------------
class DataQuality(str, Enum):
    EXCELLENT = "EXCELLENT"
    HIGH = "HIGH"
    MEDIUM = "MEDIUM"
    LOW = "LOW"
    STALE = "STALE"
    ERROR = "ERROR"
    MISSING = "MISSING"


def data_quality_score(patch: Dict[str, Any]) -> Tuple[DataQuality, float]:
    """
    Score 0..100 based on presence/validity of key fundamentals.
    Maps to aligned DataQuality tiers.
    """
    score = 0.0

    if safe_str(patch.get("symbol")):
        score += 10
    if safe_str(patch.get("name")):
        score += 5
    if safe_str(patch.get("currency")):
        score += 3

    cp = safe_float(patch.get("current_price"))
    if cp is not None and cp > 0:
        score += 12
    else:
        score -= 10

    if safe_float(patch.get("market_cap")) is not None:
        score += 10

    # valuation
    if safe_float(patch.get("pe_ttm")) is not None:
        score += 7
    if safe_float(patch.get("pb")) is not None:
        score += 6
    if safe_float(patch.get("ps")) is not None:
        score += 6

    # profitability
    if safe_float(patch.get("net_margin")) is not None:
        score += 8
    if safe_float(patch.get("roe")) is not None:
        score += 8

    # growth
    if safe_float(patch.get("revenue_growth")) is not None:
        score += 7
    if safe_float(patch.get("earnings_growth")) is not None:
        score += 6

    # analyst target
    if safe_float(patch.get("target_mean_price")) is not None:
        score += 6
    if safe_int(patch.get("analyst_count")) is not None:
        score += 6

    score = max(0.0, min(100.0, score))

    if score >= 85:
        return DataQuality.EXCELLENT, score
    if score >= 70:
        return DataQuality.HIGH, score
    if score >= 55:
        return DataQuality.MEDIUM, score
    if score >= 35:
        return DataQuality.LOW, score
    return DataQuality.MISSING, score


# ---------------------------------------------------------------------------
# Cache / Rate limit / Circuit breaker
# ---------------------------------------------------------------------------
@dataclass(slots=True)
class CacheStats:
    hits: int = 0
    misses: int = 0
    sets: int = 0
    evictions: int = 0
    size: int = 0


class AdvancedCache:
    """Memory TTL cache + optional Redis (compressed pickle)."""

    def __init__(self, name: str, maxsize: int, ttl: float, use_redis: bool, redis_url: str):
        self.name = name
        self.maxsize = max(50, int(maxsize))
        self.ttl = float(ttl)
        self.use_redis = bool(use_redis and _REDIS_AVAILABLE and Redis)
        self.redis_url = redis_url

        self._mem: Dict[str, Tuple[Any, float]] = {}
        self._touch: Dict[str, float] = {}
        self._lock = asyncio.Lock()
        self.stats = CacheStats()

        self._redis = None
        if self.use_redis:
            try:
                self._redis = Redis.from_url(self.redis_url, decode_responses=False)  # type: ignore
            except Exception as e:
                logger.warning("Redis cache init failed (%s): %s", self.name, e)
                self._redis = None
                self.use_redis = False

    def _key(self, prefix: str) -> str:
        h = hashlib.sha256(prefix.encode("utf-8")).hexdigest()[:16]
        return f"yffund:{self.name}:{h}:{prefix}"

    async def _evict_lru(self) -> None:
        if not self._touch:
            return
        oldest = min(self._touch.items(), key=lambda kv: kv[1])[0]
        self._mem.pop(oldest, None)
        self._touch.pop(oldest, None)
        self.stats.evictions += 1

    async def get(self, prefix: str) -> Optional[Any]:
        k = self._key(prefix)
        now = time.monotonic()

        async with self._lock:
            if k in self._mem:
                v, exp = self._mem[k]
                if now < exp:
                    self._touch[k] = now
                    self.stats.hits += 1
                    return v
                self._mem.pop(k, None)
                self._touch.pop(k, None)

        if self.use_redis and self._redis:
            try:
                blob = await self._redis.get(k)
                if blob:
                    val = pickle.loads(zlib.decompress(blob))
                    async with self._lock:
                        if len(self._mem) >= self.maxsize:
                            await self._evict_lru()
                        self._mem[k] = (val, now + self.ttl)
                        self._touch[k] = now
                        self.stats.hits += 1
                        self.stats.size = len(self._mem)
                    return val
            except Exception:
                pass

        self.stats.misses += 1
        return None

    async def set(self, prefix: str, value: Any, ttl: Optional[float] = None) -> None:
        k = self._key(prefix)
        exp = time.monotonic() + float(ttl or self.ttl)
        now = time.monotonic()

        async with self._lock:
            if len(self._mem) >= self.maxsize and k not in self._mem:
                await self._evict_lru()
            self._mem[k] = (value, exp)
            self._touch[k] = now
            self.stats.sets += 1
            self.stats.size = len(self._mem)

        if self.use_redis and self._redis:
            try:
                blob = zlib.compress(pickle.dumps(value))
                await self._redis.setex(k, int(ttl or self.ttl), blob)
            except Exception:
                pass

    async def close(self) -> None:
        if self.use_redis and self._redis:
            try:
                await self._redis.close()
            except Exception:
                pass
        self._redis = None

    async def size(self) -> int:
        async with self._lock:
            return len(self._mem)


class TokenBucket:
    def __init__(self, rate_per_sec: float):
        self.rate = max(0.0, float(rate_per_sec))
        self.capacity = max(1.0, self.rate * 2.0) if self.rate > 0 else 1.0
        self.tokens = self.capacity
        self.last = time.monotonic()
        self._lock = asyncio.Lock()

    async def wait_and_acquire(self, tokens: float = 1.0) -> None:
        if self.rate <= 0:
            return
        need = float(tokens)
        while True:
            async with self._lock:
                now = time.monotonic()
                self.tokens = min(self.capacity, self.tokens + (now - self.last) * self.rate)
                self.last = now
                if self.tokens >= need:
                    self.tokens -= need
                    return
                wait = (need - self.tokens) / self.rate
            await asyncio.sleep(min(1.0, max(0.01, wait)))


class CircuitState(Enum):
    CLOSED = "closed"
    HALF_OPEN = "half_open"
    OPEN = "open"

    def to_numeric(self) -> float:
        return {CircuitState.CLOSED: 0.0, CircuitState.HALF_OPEN: 1.0, CircuitState.OPEN: 2.0}[self]


@dataclass(slots=True)
class CircuitBreakerStats:
    state: CircuitState = CircuitState.CLOSED
    failures: int = 0
    successes: int = 0
    last_failure_ts: float = 0.0
    open_until_ts: float = 0.0
    cooldown_sec: float = 30.0


class AdvancedCircuitBreaker:
    """Simple, correct circuit breaker (CLOSED/OPEN/HALF_OPEN)."""

    def __init__(self, fail_threshold: int, cooldown_sec: float):
        self.fail_threshold = max(1, int(fail_threshold))
        self.stats = CircuitBreakerStats(cooldown_sec=float(cooldown_sec))
        self._lock = asyncio.Lock()
        self._half_open_probe_used = False

    async def allow_request(self) -> bool:
        if not _cb_enabled():
            return True
        async with self._lock:
            now = time.monotonic()

            if self.stats.state == CircuitState.CLOSED:
                yf_fund_circuit_breaker_state.set(self.stats.state.to_numeric())
                return True

            if self.stats.state == CircuitState.OPEN:
                if now >= self.stats.open_until_ts:
                    self.stats.state = CircuitState.HALF_OPEN
                    self._half_open_probe_used = False
                    yf_fund_circuit_breaker_state.set(self.stats.state.to_numeric())
                    return True
                yf_fund_circuit_breaker_state.set(self.stats.state.to_numeric())
                return False

            # HALF_OPEN
            if not self._half_open_probe_used:
                self._half_open_probe_used = True
                yf_fund_circuit_breaker_state.set(self.stats.state.to_numeric())
                return True

            yf_fund_circuit_breaker_state.set(self.stats.state.to_numeric())
            return False

    async def on_success(self) -> None:
        if not _cb_enabled():
            return
        async with self._lock:
            self.stats.successes += 1
            self.stats.state = CircuitState.CLOSED
            self.stats.failures = 0
            self._half_open_probe_used = False
            yf_fund_circuit_breaker_state.set(self.stats.state.to_numeric())

    async def on_failure(self, status_code: int = 500) -> None:
        if not _cb_enabled():
            return
        async with self._lock:
            now = time.monotonic()
            self.stats.failures += 1
            self.stats.last_failure_ts = now

            cooldown = self.stats.cooldown_sec
            if status_code in (401, 403, 429):
                cooldown = min(300.0, cooldown * 1.5)

            if self.stats.state == CircuitState.HALF_OPEN:
                self.stats.state = CircuitState.OPEN
                self.stats.open_until_ts = now + min(300.0, cooldown * 2)
            elif self.stats.failures >= self.fail_threshold:
                self.stats.state = CircuitState.OPEN
                self.stats.open_until_ts = now + cooldown

            yf_fund_circuit_breaker_state.set(self.stats.state.to_numeric())

    def get_stats(self) -> Dict[str, Any]:
        s = self.stats
        return {
            "state": s.state.value,
            "fail_threshold": self.fail_threshold,
            "failures": s.failures,
            "successes": s.successes,
            "last_failure_ts": s.last_failure_ts,
            "open_until_ts": s.open_until_ts,
            "cooldown_sec": s.cooldown_sec,
        }


class SingleFlight:
    """Ensure only one in-flight fetch per key (no await under lock)."""

    def __init__(self) -> None:
        self._lock = asyncio.Lock()
        self._futs: Dict[str, asyncio.Future] = {}

    async def run(self, key: str, coro_fn: Callable[[], Awaitable[Any]]) -> Any:
        owner = False
        async with self._lock:
            fut = self._futs.get(key)
            if fut is None:
                fut = asyncio.get_running_loop().create_future()
                self._futs[key] = fut
                owner = True

        if not owner:
            return await fut  # type: ignore[return-value]

        try:
            res = await coro_fn()
            if not fut.done():
                fut.set_result(res)
            return res
        except Exception as e:
            if not fut.done():
                fut.set_exception(e)
            raise
        finally:
            async with self._lock:
                self._futs.pop(key, None)


# ---------------------------------------------------------------------------
# Provider
# ---------------------------------------------------------------------------
@dataclass(slots=True)
class YahooFundamentalsProvider:
    name: str = PROVIDER_NAME

    timeout_sec: float = field(init=False)
    semaphore: asyncio.Semaphore = field(init=False)
    rate_limiter: TokenBucket = field(init=False)
    circuit_breaker: AdvancedCircuitBreaker = field(init=False)
    singleflight: SingleFlight = field(init=False)
    fund_cache: AdvancedCache = field(init=False)
    err_cache: AdvancedCache = field(init=False)

    def __post_init__(self) -> None:
        self.timeout_sec = _timeout_sec()
        self.semaphore = asyncio.Semaphore(_max_concurrency())
        self.rate_limiter = TokenBucket(_rate_limit())
        self.circuit_breaker = AdvancedCircuitBreaker(
            fail_threshold=_cb_fail_threshold(),
            cooldown_sec=_cb_cooldown_sec(),
        )
        self.singleflight = SingleFlight()

        self.fund_cache = AdvancedCache(
            name="fund",
            maxsize=5000,
            ttl=_fund_ttl_sec(),
            use_redis=_enable_redis(),
            redis_url=_redis_url(),
        )
        self.err_cache = AdvancedCache(
            name="error",
            maxsize=2000,
            ttl=_err_ttl_sec(),
            use_redis=_enable_redis(),
            redis_url=_redis_url(),
        )

        logger.info(
            "YahooFundamentalsProvider v%s initialized | yfinance=%s | concurrency=%s | rate=%s/s | cb=%s/%ss",
            PROVIDER_VERSION,
            _HAS_YFINANCE,
            _max_concurrency(),
            _rate_limit(),
            _cb_fail_threshold(),
            _cb_cooldown_sec(),
        )

    def _blocking_fetch(self, symbol: str) -> Dict[str, Any]:
        """
        Blocking fetch with full-jitter retry. Runs in asyncio.to_thread().
        NOTE: yfinance internally may be slow; we keep retries small and bounded.
        """
        if not _HAS_YFINANCE or yf is None:
            return {"error": "yfinance_not_installed"}

        last_err: Optional[Exception] = None

        for attempt in range(4):
            try:
                t = yf.Ticker(symbol)

                info = {}
                try:
                    info = t.info or {}
                except Exception:
                    info = {}

                # Fast info for price (optional)
                fast_price = None
                try:
                    fi = getattr(t, "fast_info", None)
                    if fi:
                        fast_price = getattr(fi, "last_price", None)
                        if fast_price is None and hasattr(fi, "get"):
                            fast_price = fi.get("last_price")
                except Exception:
                    pass

                current_price = (
                    safe_float(info.get("currentPrice"))
                    or safe_float(info.get("regularMarketPrice"))
                    or safe_float(fast_price)
                )

                # Yahoo fields
                trailing_pe = safe_float(info.get("trailingPE"))
                forward_pe = safe_float(info.get("forwardPE"))
                pb = safe_float(info.get("priceToBook"))
                ps = safe_float(info.get("priceToSalesTrailing12Months"))

                # Build patch (aligned keys + a few compatibility aliases)
                now_utc = _utc_iso()
                now_riy = _riyadh_iso()

                out: Dict[str, Any] = {
                    "requested_symbol": symbol,
                    "symbol": symbol,
                    "provider_symbol": symbol,
                    "provider": PROVIDER_NAME,
                    "data_source": PROVIDER_NAME,
                    "provider_version": PROVIDER_VERSION,
                    "last_updated_utc": now_utc,
                    "last_updated_riyadh": now_riy,
                    # identity
                    "currency": safe_str(info.get("currency") or info.get("financialCurrency") or "USD"),
                    "name": safe_str(info.get("longName") or info.get("shortName")),
                    "sector": safe_str(info.get("sector")),
                    "industry": safe_str(info.get("industry")),
                    "sub_sector": safe_str(info.get("industry")),
                    # market
                    "current_price": current_price,
                    "price": current_price,  # alias used in some modules
                    "market_cap": safe_float(info.get("marketCap")),
                    "enterprise_value": safe_float(info.get("enterpriseValue")),
                    "shares_outstanding": safe_float(info.get("sharesOutstanding")),
                    "beta": safe_float(info.get("beta")),
                    # valuation
                    "pe_ttm": trailing_pe,
                    "forward_pe": forward_pe,
                    "pb": pb,
                    "ps": ps,
                    "pb_ttm": pb,  # legacy alias
                    "ps_ttm": ps,  # legacy alias
                    "peg": safe_float(info.get("pegRatio")) or safe_float(info.get("trailingPegRatio")),
                    # margins / returns (percent)
                    "gross_margin": as_percent(info.get("grossMargins")),
                    "operating_margin": as_percent(info.get("operatingMargins")),
                    "net_margin": as_percent(info.get("profitMargins")),
                    "roe": as_percent(info.get("returnOnEquity")),
                    "roa": as_percent(info.get("returnOnAssets")),
                    # growth (percent)
                    "revenue_growth": as_percent(info.get("revenueGrowth")),
                    "earnings_growth": as_percent(info.get("earningsGrowth")),
                    "eps_growth_yoy": as_percent(info.get("earningsGrowth")),  # compatibility hint
                    # earnings / book
                    "eps_ttm": safe_float(info.get("trailingEps")),
                    "eps_forward": safe_float(info.get("forwardEps")),
                    "book_value": safe_float(info.get("bookValue")),
                    # dividends (percent)
                    "dividend_yield": as_percent(info.get("dividendYield")),
                    "dividend_yield_percent": as_percent(info.get("dividendYield")),  # alias
                    "dividend_rate": safe_float(info.get("dividendRate")),
                    "payout_ratio": as_percent(info.get("payoutRatio")),
                    # analyst targets
                    "target_mean_price": safe_float(info.get("targetMeanPrice")),
                    "target_high_price": safe_float(info.get("targetHighPrice")),
                    "target_low_price": safe_float(info.get("targetLowPrice")),
                    "recommendation": map_recommendation(info.get("recommendationKey")),
                    "analyst_count": safe_int(info.get("numberOfAnalystOpinions")),
                    # balance/liquidity
                    "current_ratio": safe_float(info.get("currentRatio")),
                    "quick_ratio": safe_float(info.get("quickRatio")),
                    "debt_to_equity": safe_float(info.get("debtToEquity")),
                    # cashflow (might be absent)
                    "operating_cashflow": safe_float(info.get("operatingCashflow")),
                    "free_cashflow": safe_float(info.get("freeCashflow")),
                    "short_ratio": safe_float(info.get("shortRatio")),
                    "short_percent": as_percent(info.get("shortPercentOfFloat")),
                }

                # Derived upside + forecast (aligned with scoring_engine usage)
                cp = safe_float(out.get("current_price"))
                tm = safe_float(out.get("target_mean_price"))
                if cp is not None and cp > 0 and tm is not None and tm > 0:
                    upside = ((tm / cp) - 1.0) * 100.0
                    out["upside_percent"] = float(upside)
                    out["upside"] = float(upside)  # alias
                    out["forecast_price_12m"] = float(tm)
                    out["expected_roi_12m"] = float(upside)
                    out["forecast_method"] = "analyst_consensus"
                    out["forecast_updated_utc"] = now_utc
                    out["forecast_updated_riyadh"] = now_riy

                    ac = safe_int(out.get("analyst_count"))
                    if ac and ac > 0:
                        conf = 0.55 + (math.log(ac + 1) * 0.06)
                        conf = float(min(0.95, max(0.35, conf)))
                    else:
                        conf = 0.50
                    out["forecast_confidence"] = conf
                    out["confidence_score"] = float(conf * 100.0)

                dq, dq_score = data_quality_score(out)
                out["data_quality"] = dq.value
                out["data_quality_score"] = dq_score

                # For engines expecting status (optional, harmless)
                out["status"] = "ok"

                return clean_patch(out)

            except Exception as e:
                last_err = e
                base = 2 ** attempt
                time.sleep(min(8.0, base + random.uniform(0.0, base)))

        return {"error": f"fetch_failed: {type(last_err).__name__ if last_err else 'Unknown'}: {last_err}"}

    @_trace("yf_fund_fetch")
    async def fetch_fundamentals_patch(self, symbol: str, debug: bool = False, *args: Any, **kwargs: Any) -> Dict[str, Any]:
        if not _configured():
            if _HAS_YFINANCE:
                return {}
            return {} if not _emit_warnings() else {"_warn": "yfinance_not_installed"}

        norm = normalize_symbol(symbol)
        if not norm:
            return {} if not _emit_warnings() else {"_warn": "invalid_symbol"}

        cache_key = f"fund:{norm}"

        # short-lived error backoff
        if await self.err_cache.get(cache_key):
            return {} if not _emit_warnings() else {"_warn": "temporarily_backed_off"}

        cached = await self.fund_cache.get(cache_key)
        if cached:
            try:
                return dict(cached)
            except Exception:
                return cached  # type: ignore[return-value]

        async def _do_fetch() -> Dict[str, Any]:
            t0 = time.time()

            if not await self.circuit_breaker.allow_request():
                yf_fund_requests_total.labels(status="cb_open").inc()
                return {} if not _emit_warnings() else {"_warn": "circuit_breaker_open"}

            await self.rate_limiter.wait_and_acquire()

            try:
                async with self.semaphore:
                    res = await asyncio.wait_for(
                        asyncio.to_thread(self._blocking_fetch, norm),
                        timeout=self.timeout_sec,
                    )

                if isinstance(res, dict) and res.get("error"):
                    yf_fund_requests_total.labels(status="error").inc()
                    await self.circuit_breaker.on_failure(status_code=500)
                    await self.err_cache.set(cache_key, True, ttl=_err_ttl_sec())
                    return {} if not _emit_warnings() else {"_warn": str(res.get("error"))}

                yf_fund_requests_total.labels(status="ok").inc()
                await self.circuit_breaker.on_success()
                await self.fund_cache.set(cache_key, res, ttl=_fund_ttl_sec())

                if debug and isinstance(res, dict):
                    res = dict(res)
                    res["_debug"] = {
                        "provider": PROVIDER_NAME,
                        "provider_version": PROVIDER_VERSION,
                        "norm_symbol": norm,
                        "elapsed_ms": int((time.time() - t0) * 1000),
                        "cache_key": cache_key,
                    }
                return res if isinstance(res, dict) else {}  # type: ignore[return-value]

            except asyncio.TimeoutError:
                yf_fund_requests_total.labels(status="timeout").inc()
                await self.circuit_breaker.on_failure(status_code=504)
                await self.err_cache.set(cache_key, True, ttl=_err_ttl_sec())
                return {} if not _emit_warnings() else {"_warn": "timeout"}

            except Exception as e:
                yf_fund_requests_total.labels(status="exception").inc()
                await self.circuit_breaker.on_failure(status_code=500)
                await self.err_cache.set(cache_key, True, ttl=_err_ttl_sec())
                return {} if not _emit_warnings() else {"_warn": f"exception: {type(e).__name__}"}

            finally:
                try:
                    yf_fund_request_duration.observe(max(0.0, time.time() - t0))
                except Exception:
                    pass

        return await self.singleflight.run(cache_key, _do_fetch)

    async def get_metrics(self) -> Dict[str, Any]:
        return {
            "provider": PROVIDER_NAME,
            "provider_version": PROVIDER_VERSION,
            "configured": _configured(),
            "yfinance_available": _HAS_YFINANCE,
            "concurrency": _max_concurrency(),
            "rate_limit_per_sec": _rate_limit(),
            "timeout_sec": self.timeout_sec,
            "circuit_breaker": self.circuit_breaker.get_stats(),
            "cache_sizes": {"fund": await self.fund_cache.size(), "error": await self.err_cache.size()},
            "cache_stats": {"fund": self.fund_cache.stats.__dict__, "error": self.err_cache.stats.__dict__},
        }

    async def aclose(self) -> None:
        try:
            await self.fund_cache.close()
        except Exception:
            pass
        try:
            await self.err_cache.close()
        except Exception:
            pass


# ---------------------------------------------------------------------------
# Singleton management
# ---------------------------------------------------------------------------
_PROVIDER_INSTANCE: Optional[YahooFundamentalsProvider] = None
_PROVIDER_LOCK = asyncio.Lock()


async def get_provider() -> YahooFundamentalsProvider:
    global _PROVIDER_INSTANCE
    if _PROVIDER_INSTANCE is None:
        async with _PROVIDER_LOCK:
            if _PROVIDER_INSTANCE is None:
                _PROVIDER_INSTANCE = YahooFundamentalsProvider()
    return _PROVIDER_INSTANCE


async def close_provider() -> None:
    global _PROVIDER_INSTANCE
    if _PROVIDER_INSTANCE is not None:
        try:
            await _PROVIDER_INSTANCE.aclose()
        except Exception:
            pass
    _PROVIDER_INSTANCE = None


# ---------------------------------------------------------------------------
# Public API (Engine compatible)
# ---------------------------------------------------------------------------
async def fetch_fundamentals_patch(symbol: str, debug: bool = False, *args: Any, **kwargs: Any) -> Dict[str, Any]:
    provider = await get_provider()
    return await provider.fetch_fundamentals_patch(symbol, debug=debug)


async def fetch_enriched_quote_patch(symbol: str, debug: bool = False, *args: Any, **kwargs: Any) -> Dict[str, Any]:
    # compatibility alias used by some pipelines
    return await fetch_fundamentals_patch(symbol, debug=debug)


async def get_client_metrics() -> Dict[str, Any]:
    provider = await get_provider()
    return await provider.get_metrics()


async def aclose_yahoo_fundamentals_client() -> None:
    await close_provider()


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
    "DataQuality",
]
