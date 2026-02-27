#!/usr/bin/env python3
# core/providers/yahoo_chart_provider.py
"""
================================================================================
Yahoo Chart Provider (Global Market Data) — v6.5.0 (Loader-Compatible + Prod Safe)
================================================================================

What this provider guarantees
- ✅ No startup crashes (all optional deps safe)
- ✅ Async-friendly: threadpool for yfinance (blocking)
- ✅ Deterministic + safe fallbacks (no heavy ML required)
- ✅ Canonical output keys aligned with your engine:
    current_price, previous_close, day_high, day_low, week_52_high, week_52_low,
    volume, market_cap, currency, name, exchange,
    price_change, percent_change, week_52_position_pct,
    rsi_14, volatility_30d,
    forecast_price_1m/3m/12m, expected_roi_1m/3m/12m, forecast_confidence (0..1),
    last_updated_utc, last_updated_riyadh, history_last_utc

- ✅ Keeps backward-compatible aliases:
    price, prev_close, 52w_high, 52w_low, change, change_pct

Loader compatibility (important)
- Some loaders only accept a provider instance / factory / Provider class without awaiting:
  This module exports:
    provider, PROVIDER, Provider, get_provider(), build_provider(), create_provider(), provider_factory()

================================================================================
"""

from __future__ import annotations

import asyncio
import concurrent.futures
import json
import logging
import math
import os
import random
import re
import time
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Any, Awaitable, Callable, Dict, List, Optional, Tuple

# =============================================================================
# Optional deps (safe)
# =============================================================================
try:
    import numpy as np  # type: ignore

    _HAS_NUMPY = True
except Exception:
    np = None  # type: ignore
    _HAS_NUMPY = False

try:
    import pandas as pd  # type: ignore

    _HAS_PANDAS = True
except Exception:
    pd = None  # type: ignore
    _HAS_PANDAS = False

try:
    import yfinance as yf  # type: ignore

    _HAS_YFINANCE = True
except Exception:
    yf = None  # type: ignore
    _HAS_YFINANCE = False

# =============================================================================
# Prometheus (optional) — SAFE dummy that supports .labels()
# =============================================================================
try:
    from prometheus_client import Counter, Histogram, Gauge  # type: ignore

    _HAS_PROM = True
except Exception:
    _HAS_PROM = False

    class _DummyMetric:
        def __init__(self, *args, **kwargs):
            pass

        def labels(self, *args, **kwargs):
            return self

        def inc(self, *args, **kwargs):
            return None

        def observe(self, *args, **kwargs):
            return None

        def set(self, *args, **kwargs):
            return None

    Counter = Histogram = Gauge = _DummyMetric  # type: ignore

# =============================================================================
# OpenTelemetry (optional) — SAFE
# =============================================================================
try:
    from opentelemetry import trace  # type: ignore

    _HAS_OTEL = True
except Exception:
    trace = None  # type: ignore
    _HAS_OTEL = False

# Prefer core TraceContext if available (aligned with core/config.py)
try:
    from core.config import TraceContext as CoreTraceContext  # type: ignore

    TraceContext = CoreTraceContext  # type: ignore
except Exception:

    class TraceContext:  # type: ignore
        def __init__(self, name: str, attributes: Optional[Dict[str, Any]] = None):
            self.name = name
            self.attributes = attributes or {}

        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc_val, exc_tb):
            return False


# =============================================================================
# JSON helper (orjson optional, safe)
# =============================================================================
try:
    import orjson  # type: ignore

    def json_dumps(v: Any) -> str:
        return orjson.dumps(v, default=str).decode("utf-8")

    _HAS_ORJSON = True
except Exception:

    def json_dumps(v: Any) -> str:
        return json.dumps(v, default=str, ensure_ascii=False)

    _HAS_ORJSON = False

# =============================================================================
# Logging
# =============================================================================
logger = logging.getLogger("core.providers.yahoo_chart_provider")
logger.addHandler(logging.NullHandler())

PROVIDER_NAME = "yahoo_chart"
PROVIDER_VERSION = "6.5.0"

# =============================================================================
# Env helpers (safe)
# =============================================================================
_TRUTHY = {"1", "true", "yes", "y", "on", "t", "enable", "enabled"}
_FALSY = {"0", "false", "no", "n", "off", "f", "disable", "disabled"}

_KSA_CODE_RE = re.compile(r"^\d{3,6}$", re.IGNORECASE)
_ARABIC_DIGITS = str.maketrans("٠١٢٣٤٥٦٧٨٩", "0123456789")


def _env_str(name: str, default: str = "") -> str:
    v = os.getenv(name)
    if v is None:
        return default
    s = str(v).strip()
    return s if s else default


def _env_int(name: str, default: int) -> int:
    try:
        return int(float(str(os.getenv(name, str(default))).strip()))
    except Exception:
        return default


def _env_float(name: str, default: float) -> float:
    try:
        return float(str(os.getenv(name, str(default))).strip())
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
    if not _env_bool("YAHOO_ENABLED", True):
        return False
    return bool(_HAS_YFINANCE)


def _timeout_sec() -> float:
    return max(5.0, _env_float("YAHOO_TIMEOUT_SEC", 20.0))


def _quote_ttl_sec() -> float:
    return max(5.0, _env_float("YAHOO_QUOTE_TTL_SEC", 15.0))


def _history_period() -> str:
    return _env_str("YAHOO_HISTORY_PERIOD", "2y")


def _history_interval() -> str:
    return _env_str("YAHOO_HISTORY_INTERVAL", "1d")


def _max_concurrency() -> int:
    return max(2, _env_int("YAHOO_MAX_CONCURRENCY", 24))


def _rate_limit_per_sec() -> float:
    return max(0.0, _env_float("YAHOO_RATE_LIMIT_PER_SEC", 10.0))


def _rate_limit_burst() -> int:
    return max(1, _env_int("YAHOO_RATE_LIMIT_BURST", 20))


def _cb_fail_threshold() -> int:
    return max(2, _env_int("YAHOO_CB_FAIL_THRESHOLD", 6))


def _cb_cooldown_sec() -> float:
    return max(5.0, _env_float("YAHOO_CB_COOLDOWN_SEC", 30.0))


def _cb_success_threshold() -> int:
    return max(1, _env_int("YAHOO_CB_SUCCESS_THRESHOLD", 3))


def _tracing_enabled() -> bool:
    return (os.getenv("CORE_TRACING_ENABLED", "") or os.getenv("TRACING_ENABLED", "")).strip().lower() in _TRUTHY


# =============================================================================
# Time helpers (UTC + Riyadh)
# =============================================================================
_RIYADH_TZ = timezone(timedelta(hours=3))


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _utc_iso(dt: Optional[datetime] = None) -> str:
    d = dt or _utc_now()
    if d.tzinfo is None:
        d = d.replace(tzinfo=timezone.utc)
    return d.astimezone(timezone.utc).isoformat()


def _riyadh_iso(dt: Optional[datetime] = None) -> str:
    d = dt or datetime.now(_RIYADH_TZ)
    if d.tzinfo is None:
        d = d.replace(tzinfo=_RIYADH_TZ)
    return d.astimezone(_RIYADH_TZ).isoformat()


# =============================================================================
# Safe type helpers
# =============================================================================
def safe_str(x: Any) -> Optional[str]:
    if x is None:
        return None
    s = str(x).strip()
    return s if s else None


def safe_float(x: Any) -> Optional[float]:
    try:
        if x is None:
            return None
        if isinstance(x, (int, float)):
            f = float(x)
            if math.isnan(f) or math.isinf(f):
                return None
            return f
        s = str(x).strip()
        if not s or s.lower() in {"-", "—", "n/a", "na", "null", "none", "nan"}:
            return None
        s = s.translate(_ARABIC_DIGITS)
        s = s.replace(",", "").replace("%", "").replace("+", "").replace("$", "").replace("£", "").replace("€", "")
        s = s.replace("SAR", "").replace("USD", "").replace("EUR", "").strip()
        if s.startswith("(") and s.endswith(")"):
            s = "-" + s[1:-1].strip()
        m = re.match(r"^(-?\d+(\.\d+)?)([KMBT])$", s, re.IGNORECASE)
        mult = 1.0
        if m:
            num = m.group(1)
            suf = m.group(3).upper()
            mult = {"K": 1_000, "M": 1_000_000, "B": 1_000_000_000, "T": 1_000_000_000_000}.get(suf, 1.0)
            s = num
        f = float(s) * mult
        if math.isnan(f) or math.isinf(f):
            return None
        return f
    except Exception:
        return None


def safe_int(x: Any) -> Optional[int]:
    f = safe_float(x)
    if f is None:
        return None
    try:
        return int(round(f))
    except Exception:
        return None


def normalize_symbol(symbol: str) -> str:
    s = (symbol or "").strip()
    if not s:
        return ""
    s = s.translate(_ARABIC_DIGITS).strip().upper()
    # if numeric, treat as KSA
    if _KSA_CODE_RE.match(s):
        return f"{s}.SR"
    return s


def clean_dict(d: Dict[str, Any]) -> Dict[str, Any]:
    out: Dict[str, Any] = {}
    for k, v in (d or {}).items():
        if v is None:
            continue
        if isinstance(v, str) and not v.strip():
            continue
        if isinstance(v, dict):
            vv = clean_dict(v)
            if vv:
                out[k] = vv
        elif isinstance(v, (list, tuple)):
            lst = []
            for item in v:
                if isinstance(item, dict):
                    it = clean_dict(item)
                    if it:
                        lst.append(it)
                elif item is not None:
                    lst.append(item)
            if lst:
                out[k] = lst
        else:
            out[k] = v
    return out


# =============================================================================
# Metrics (safe even without Prometheus)
# =============================================================================
yahoo_requests_total = Counter("yahoo_requests_total", "Total Yahoo requests", ["symbol", "op", "status"])
yahoo_request_duration = Histogram("yahoo_request_duration_seconds", "Yahoo request duration", ["symbol", "op"])
yahoo_cache_hits_total = Counter("yahoo_cache_hits_total", "Yahoo cache hits", ["symbol"])
yahoo_cache_misses_total = Counter("yahoo_cache_misses_total", "Yahoo cache misses", ["symbol"])
yahoo_circuit_state = Gauge("yahoo_circuit_state", "Yahoo circuit breaker state", ["state"])


# =============================================================================
# Rate limiter (token bucket)
# =============================================================================
@dataclass(slots=True)
class TokenBucket:
    rate_per_sec: float
    burst: float
    tokens: float = 0.0
    last: float = 0.0
    lock: asyncio.Lock = field(default_factory=asyncio.Lock)

    async def acquire(self, n: float = 1.0) -> None:
        if self.rate_per_sec <= 0:
            return
        async with self.lock:
            now = time.monotonic()
            if self.last <= 0:
                self.last = now
                self.tokens = float(self.burst)
            elapsed = max(0.0, now - self.last)
            self.tokens = min(float(self.burst), self.tokens + elapsed * float(self.rate_per_sec))
            self.last = now
            if self.tokens >= n:
                self.tokens -= n
                return
            need = n - self.tokens
            wait = need / float(self.rate_per_sec)
            self.tokens = 0.0
        await asyncio.sleep(min(5.0, max(0.0, wait)))


# =============================================================================
# Circuit breaker
# =============================================================================
@dataclass(slots=True)
class CircuitBreaker:
    fail_threshold: int
    cooldown_sec: float
    success_threshold: int
    failures: int = 0
    successes: int = 0
    opened_at: float = 0.0
    state: str = "closed"  # closed/open/half_open
    lock: asyncio.Lock = field(default_factory=asyncio.Lock)

    async def allow(self) -> bool:
        async with self.lock:
            if self.state == "closed":
                yahoo_circuit_state.labels(state="closed").set(0.0)
                return True
            if self.state == "open":
                now = time.monotonic()
                if now - self.opened_at >= self.cooldown_sec:
                    self.state = "half_open"
                    self.successes = 0
                    yahoo_circuit_state.labels(state="half_open").set(1.0)
                    return True
                yahoo_circuit_state.labels(state="open").set(2.0)
                return False
            yahoo_circuit_state.labels(state="half_open").set(1.0)
            return True

    async def record_success(self) -> None:
        async with self.lock:
            if self.state == "half_open":
                self.successes += 1
                if self.successes >= self.success_threshold:
                    self.state = "closed"
                    self.failures = 0
                    self.successes = 0
            else:
                self.failures = max(0, self.failures - 1)

    async def record_failure(self) -> None:
        async with self.lock:
            self.failures += 1
            if self.state == "half_open":
                self.state = "open"
                self.opened_at = time.monotonic()
                return
            if self.failures >= self.fail_threshold:
                self.state = "open"
                self.opened_at = time.monotonic()


# =============================================================================
# SingleFlight (hardened)
# =============================================================================
class SingleFlight:
    def __init__(self) -> None:
        self._lock = asyncio.Lock()
        self._futures: Dict[str, asyncio.Future] = {}

    async def run(self, key: str, coro_fn: Callable[[], Awaitable[Any]]) -> Any:
        async with self._lock:
            fut = self._futures.get(key)
            if fut is not None:
                return await fut
            loop = asyncio.get_running_loop()
            fut = loop.create_future()
            self._futures[key] = fut

        try:
            res = await coro_fn()
            if not fut.cancelled() and not fut.done():
                fut.set_result(res)
            return res
        except Exception as e:
            if not fut.cancelled() and not fut.done():
                fut.set_exception(e)
            raise
        finally:
            async with self._lock:
                self._futures.pop(key, None)


# =============================================================================
# AdvancedCache (async TTL + clear)
# =============================================================================
@dataclass(slots=True)
class AdvancedCache:
    name: str
    ttl_sec: float
    maxsize: int = 5000
    _data: Dict[str, Tuple[Any, float]] = field(default_factory=dict)
    _lock: asyncio.Lock = field(default_factory=asyncio.Lock)

    def _make_key(self, symbol: str, kind: str) -> str:
        return f"{self.name}:{kind}:{symbol}"

    async def get(self, symbol: str, kind: str = "quote") -> Optional[Any]:
        key = self._make_key(symbol, kind)
        now = time.monotonic()
        async with self._lock:
            item = self._data.get(key)
            if not item:
                return None
            value, exp = item
            if now <= exp:
                return value
            self._data.pop(key, None)
            return None

    async def set(self, symbol: str, value: Any, kind: str = "quote", ttl_sec: Optional[float] = None) -> None:
        key = self._make_key(symbol, kind)
        ttl = float(ttl_sec if ttl_sec is not None else self.ttl_sec)
        exp = time.monotonic() + max(0.5, ttl)
        async with self._lock:
            if len(self._data) >= self.maxsize and key not in self._data:
                n = max(1, self.maxsize // 10)
                for k in list(self._data.keys())[:n]:
                    self._data.pop(k, None)
            self._data[key] = (value, exp)

    async def clear(self) -> None:
        async with self._lock:
            self._data.clear()

    async def size(self) -> int:
        async with self._lock:
            return len(self._data)


# =============================================================================
# Indicators
# =============================================================================
def _rsi_14(closes: List[float]) -> Optional[float]:
    if len(closes) < 15:
        return None
    try:
        if _HAS_NUMPY and np is not None:
            arr = np.asarray(closes, dtype=float)
            diff = np.diff(arr)
            gains = np.where(diff > 0, diff, 0.0)
            losses = np.where(diff < 0, -diff, 0.0)
            n = 14
            avg_gain = float(gains[:n].mean())
            avg_loss = float(losses[:n].mean())
            for i in range(n, len(gains)):
                avg_gain = (avg_gain * (n - 1) + float(gains[i])) / n
                avg_loss = (avg_loss * (n - 1) + float(losses[i])) / n
            if avg_loss == 0:
                return 100.0
            rs = avg_gain / avg_loss
            return float(100.0 - (100.0 / (1.0 + rs)))

        # pure python fallback
        n = 14
        diffs = [closes[i] - closes[i - 1] for i in range(1, len(closes))]
        gains = [d if d > 0 else 0.0 for d in diffs]
        losses = [-d if d < 0 else 0.0 for d in diffs]
        avg_gain = sum(gains[:n]) / n
        avg_loss = sum(losses[:n]) / n
        for i in range(n, len(gains)):
            avg_gain = (avg_gain * (n - 1) + gains[i]) / n
            avg_loss = (avg_loss * (n - 1) + losses[i]) / n
        if avg_loss == 0:
            return 100.0
        rs = avg_gain / avg_loss
        return 100.0 - (100.0 / (1.0 + rs))
    except Exception:
        return None


def _volatility_30d(closes: List[float]) -> Optional[float]:
    # annualized volatility based on last 30 trading days log returns
    if len(closes) < 31:
        return None
    try:
        window = closes[-31:]
        if _HAS_NUMPY and np is not None:
            arr = np.asarray(window, dtype=float)
            rets = np.diff(np.log(arr))
            if len(rets) < 2:
                return None
            vol = float(np.std(rets, ddof=1) * math.sqrt(252))
            if math.isnan(vol) or math.isinf(vol):
                return None
            return vol

        rets = []
        for i in range(1, len(window)):
            if window[i - 1] > 0 and window[i] > 0:
                rets.append(math.log(window[i] / window[i - 1]))
        if len(rets) < 2:
            return None
        mean = sum(rets) / len(rets)
        var = sum((r - mean) ** 2 for r in rets) / (len(rets) - 1)
        return math.sqrt(var) * math.sqrt(252)
    except Exception:
        return None


def _simple_forecast(closes: List[float], horizon_days: int) -> Tuple[Optional[float], Optional[float], float]:
    """
    Simple log-linear forecast:
    returns (forecast_price, expected_roi_pct, confidence_0_1)
    """
    if len(closes) < 60 or horizon_days <= 0:
        return None, None, 0.0
    try:
        n = min(len(closes), 252)
        y = closes[-n:]
        if any(p <= 0 for p in y):
            return None, None, 0.0

        if _HAS_NUMPY and np is not None:
            arr = np.asarray(y, dtype=float)
            x = np.arange(len(arr), dtype=float)
            logy = np.log(arr)
            slope, intercept = np.polyfit(x, logy, 1)
            pred = slope * x + intercept
            ss_res = float(np.sum((logy - pred) ** 2))
            ss_tot = float(np.sum((logy - float(np.mean(logy))) ** 2))
            r2 = 1.0 - ss_res / ss_tot if ss_tot > 0 else 0.0
            future_x = float(len(arr) - 1 + horizon_days)
            forecast_price = float(math.exp(float(slope) * future_x + float(intercept)))
        else:
            start = y[0]
            end = y[-1]
            years = max(1e-6, len(y) / 252.0)
            cagr = (end / start) ** (1 / years) - 1
            forecast_price = end * ((1 + cagr) ** (horizon_days / 252.0))
            r2 = 0.25

        last = float(closes[-1])
        roi_pct = (forecast_price / last - 1.0) * 100.0

        vol = _volatility_30d(closes) or 0.25
        conf = max(0.05, min(0.95, (max(0.0, min(1.0, r2)) * 0.8 + 0.2) * (1.0 / (1.0 + max(0.0, vol - 0.25)))))
        return forecast_price, roi_pct, conf
    except Exception:
        return None, None, 0.0


# =============================================================================
# Provider Implementation
# =============================================================================
_CPU_WORKERS = max(2, _env_int("YAHOO_THREADPOOL_WORKERS", 6))
_CPU_EXECUTOR = concurrent.futures.ThreadPoolExecutor(max_workers=_CPU_WORKERS, thread_name_prefix="YahooWorker")


def _get_attr(obj: Any, *names: str) -> Any:
    for n in names:
        try:
            if obj is None:
                return None
            # dict-like
            if isinstance(obj, dict):
                if n in obj:
                    return obj.get(n)
            # object-like
            if hasattr(obj, n):
                return getattr(obj, n)
        except Exception:
            continue
    return None


@dataclass(slots=True)
class YahooChartProvider:
    name: str = PROVIDER_NAME
    version: str = PROVIDER_VERSION

    timeout_sec: float = field(default_factory=_timeout_sec)
    retry_attempts: int = 3
    max_concurrency: int = field(default_factory=_max_concurrency)

    quote_cache: AdvancedCache = field(default_factory=lambda: AdvancedCache("yahoo", _quote_ttl_sec()))
    singleflight: SingleFlight = field(default_factory=SingleFlight)

    _sem: asyncio.Semaphore = field(default_factory=lambda: asyncio.Semaphore(_max_concurrency()))
    _rate: TokenBucket = field(default_factory=lambda: TokenBucket(_rate_limit_per_sec(), float(_rate_limit_burst())))
    _cb: CircuitBreaker = field(default_factory=lambda: CircuitBreaker(_cb_fail_threshold(), _cb_cooldown_sec(), _cb_success_threshold()))

    # ----------------------------
    # Core blocking fetch (yfinance)
    # ----------------------------
    def _blocking_fetch(self, symbol: str) -> Dict[str, Any]:
        if not _HAS_YFINANCE or yf is None:
            return {
                "symbol": symbol,
                "error": "yfinance_not_installed",
                "provider": PROVIDER_NAME,
                "provider_version": PROVIDER_VERSION,
                "data_sources": [PROVIDER_NAME],
            }

        last_err: Optional[Exception] = None

        for attempt in range(max(1, int(self.retry_attempts))):
            try:
                t0 = time.time()
                ticker = yf.Ticker(symbol)

                # fast_info (dict-like or object-like depending on yfinance version)
                try:
                    fast_info = ticker.fast_info
                except Exception:
                    fast_info = None

                # info can be slow; tolerate failures
                try:
                    info = ticker.info
                except Exception:
                    info = None

                out: Dict[str, Any] = {
                    "symbol": symbol,
                    "provider": PROVIDER_NAME,
                    "provider_version": PROVIDER_VERSION,
                    "data_sources": [PROVIDER_NAME],
                }

                # Prices
                price = safe_float(_get_attr(fast_info, "last_price", "lastPrice", "regularMarketPrice"))
                prev_close = safe_float(_get_attr(fast_info, "previous_close", "previousClose", "regularMarketPreviousClose"))

                if price is None and isinstance(info, dict):
                    price = safe_float(info.get("regularMarketPrice"))
                if prev_close is None and isinstance(info, dict):
                    prev_close = safe_float(info.get("regularMarketPreviousClose"))

                # Day range
                day_high = safe_float(_get_attr(fast_info, "day_high", "dayHigh"))
                day_low = safe_float(_get_attr(fast_info, "day_low", "dayLow"))
                if isinstance(info, dict):
                    day_high = day_high or safe_float(info.get("dayHigh"))
                    day_low = day_low or safe_float(info.get("dayLow"))

                # 52W
                w52_high = safe_float(_get_attr(fast_info, "fifty_two_week_high", "fiftyTwoWeekHigh", "week52High"))
                w52_low = safe_float(_get_attr(fast_info, "fifty_two_week_low", "fiftyTwoWeekLow", "week52Low"))
                if isinstance(info, dict):
                    w52_high = w52_high or safe_float(info.get("fiftyTwoWeekHigh"))
                    w52_low = w52_low or safe_float(info.get("fiftyTwoWeekLow"))

                # Volume / market cap
                volume = safe_float(_get_attr(fast_info, "last_volume", "lastVolume", "regularMarketVolume"))
                market_cap = safe_float(_get_attr(fast_info, "market_cap", "marketCap"))
                if isinstance(info, dict):
                    volume = volume or safe_float(info.get("regularMarketVolume"))
                    market_cap = market_cap or safe_float(info.get("marketCap"))

                currency = None
                name = None
                exchange = None
                if isinstance(info, dict):
                    currency = safe_str(info.get("currency"))
                    name = safe_str(info.get("shortName") or info.get("longName"))
                    exchange = safe_str(info.get("exchange") or info.get("fullExchangeName"))

                # History (for indicators + fallback price + fallback 52w + prev_close)
                closes: List[float] = []
                hist_last_dt: Optional[datetime] = None
                last_hist_row = None

                try:
                    hist = ticker.history(period=_history_period(), interval=_history_interval(), auto_adjust=False)
                    if hist is not None and not getattr(hist, "empty", True):
                        try:
                            last_hist_row = hist.iloc[-1] if hasattr(hist, "iloc") else None
                        except Exception:
                            last_hist_row = None

                        # closes
                        try:
                            if _HAS_PANDAS and pd is not None:
                                c = hist["Close"].dropna().astype(float).tolist()
                            else:
                                c = [safe_float(x) for x in list(hist["Close"])]
                                c = [x for x in c if x is not None]
                            closes = [float(x) for x in c if x is not None]
                        except Exception:
                            closes = []

                        # last timestamp
                        try:
                            idx = hist.index[-1]
                            hist_last_dt = idx.to_pydatetime() if hasattr(idx, "to_pydatetime") else idx
                        except Exception:
                            hist_last_dt = None

                        # fallback price
                        if price is None and closes:
                            price = float(closes[-1])

                        # fallback prev close
                        if prev_close is None and len(closes) >= 2:
                            prev_close = float(closes[-2])

                        # fallback 52w from history if missing
                        if (w52_high is None or w52_low is None) and closes:
                            try:
                                window = closes[-252:] if len(closes) > 252 else closes
                                w52_high = w52_high or max(window)
                                w52_low = w52_low or min(window)
                            except Exception:
                                pass

                        # fallback day high/low from last row if missing
                        if last_hist_row is not None:
                            try:
                                if day_high is None:
                                    day_high = safe_float(last_hist_row.get("High") if hasattr(last_hist_row, "get") else last_hist_row["High"])
                                if day_low is None:
                                    day_low = safe_float(last_hist_row.get("Low") if hasattr(last_hist_row, "get") else last_hist_row["Low"])
                                if volume is None:
                                    volume = safe_float(last_hist_row.get("Volume") if hasattr(last_hist_row, "get") else last_hist_row["Volume"])
                            except Exception:
                                pass
                except Exception:
                    closes = []

                if price is None:
                    raise ValueError("no_price_from_yahoo")

                # Canonical keys
                out["current_price"] = float(price)
                out["previous_close"] = float(prev_close) if prev_close is not None else None
                out["day_high"] = float(day_high) if day_high is not None else None
                out["day_low"] = float(day_low) if day_low is not None else None
                out["week_52_high"] = float(w52_high) if w52_high is not None else None
                out["week_52_low"] = float(w52_low) if w52_low is not None else None
                out["volume"] = float(volume) if volume is not None else None
                out["market_cap"] = float(market_cap) if market_cap is not None else None
                out["currency"] = currency
                out["name"] = name
                out["exchange"] = exchange

                # Derived canonical
                if out.get("previous_close") not in (None, 0.0):
                    chg = out["current_price"] - float(out["previous_close"])
                    out["price_change"] = float(chg)
                    out["percent_change"] = float((chg / float(out["previous_close"])) * 100.0)

                if out.get("week_52_high") is not None and out.get("week_52_low") is not None and out["week_52_high"] != out["week_52_low"]:
                    out["week_52_position_pct"] = float(
                        ((out["current_price"] - out["week_52_low"]) / (out["week_52_high"] - out["week_52_low"])) * 100.0
                    )

                # Indicators + Forecasts
                if closes:
                    rsi = _rsi_14(closes)
                    vol = _volatility_30d(closes)
                    if rsi is not None:
                        out["rsi_14"] = float(rsi)
                    if vol is not None:
                        out["volatility_30d"] = float(vol)  # 0..1-ish (annualized)

                    f1, roi1, c1 = _simple_forecast(closes, horizon_days=30)
                    f3, roi3, c3 = _simple_forecast(closes, horizon_days=90)
                    f12, roi12, c12 = _simple_forecast(closes, horizon_days=365)

                    if f1 is not None:
                        out["forecast_price_1m"] = float(f1)
                        out["expected_roi_1m"] = float(roi1) if roi1 is not None else None
                    if f3 is not None:
                        out["forecast_price_3m"] = float(f3)
                        out["expected_roi_3m"] = float(roi3) if roi3 is not None else None
                    if f12 is not None:
                        out["forecast_price_12m"] = float(f12)
                        out["expected_roi_12m"] = float(roi12) if roi12 is not None else None

                    conf = max(0.0, min(1.0, min([x for x in [c1, c3, c12] if x is not None] or [0.0])))
                    out["forecast_confidence"] = float(conf)  # 0..1

                # Timestamps
                out["last_updated_utc"] = _utc_iso()
                out["last_updated_riyadh"] = _riyadh_iso()
                if hist_last_dt is not None:
                    out["history_last_utc"] = _utc_iso(hist_last_dt)

                # Data quality score (0..1)
                filled = 0
                for k in ("current_price", "previous_close", "volume", "week_52_high", "week_52_low"):
                    if out.get(k) is not None:
                        filled += 1
                out["data_quality_score"] = float(filled / 5.0)

                out["_fetch_ms"] = int((time.time() - t0) * 1000)

                # Backward-compatible aliases
                out["price"] = out.get("current_price")
                out["prev_close"] = out.get("previous_close")
                out["change"] = out.get("price_change")
                out["change_pct"] = out.get("percent_change")
                out["52w_high"] = out.get("week_52_high")
                out["52w_low"] = out.get("week_52_low")

                return clean_dict(out)

            except Exception as e:
                last_err = e
                base = min(5.0, 0.5 * (2 ** attempt))
                time.sleep(random.uniform(0.0, base))

        return {
            "symbol": symbol,
            "error": f"fetch_failed: {str(last_err)}",
            "provider": PROVIDER_NAME,
            "provider_version": PROVIDER_VERSION,
            "data_sources": [PROVIDER_NAME],
        }

    # ----------------------------
    # Public async API
    # ----------------------------
    async def fetch_enriched_quote_patch(self, symbol: str, debug: bool = False, *args: Any, **kwargs: Any) -> Dict[str, Any]:
        if not _configured():
            return {
                "symbol": symbol,
                "error": "provider_disabled_or_yfinance_missing",
                "provider": PROVIDER_NAME,
                "provider_version": PROVIDER_VERSION,
                "data_sources": [PROVIDER_NAME],
            }

        sym = normalize_symbol(symbol)
        if not sym:
            return {
                "symbol": symbol,
                "error": "invalid_symbol",
                "provider": PROVIDER_NAME,
                "provider_version": PROVIDER_VERSION,
                "data_sources": [PROVIDER_NAME],
            }

        # Circuit breaker
        if not await self._cb.allow():
            return {
                "symbol": sym,
                "error": "circuit_open",
                "provider": PROVIDER_NAME,
                "provider_version": PROVIDER_VERSION,
                "data_sources": [PROVIDER_NAME],
            }

        # Cache
        cached = await self.quote_cache.get(sym, kind="quote")
        if cached is not None:
            yahoo_cache_hits_total.labels(symbol=sym).inc()
            return cached
        yahoo_cache_misses_total.labels(symbol=sym).inc()

        async def _do_fetch() -> Dict[str, Any]:
            async with self._sem:
                await self._rate.acquire(1.0)
                t0 = time.time()
                try:
                    loop = asyncio.get_running_loop()
                    with_trace = _tracing_enabled()
                    if with_trace:
                        async with TraceContext("yahoo_chart_fetch", {"symbol": sym}):
                            res = await loop.run_in_executor(_CPU_EXECUTOR, self._blocking_fetch, sym)
                    else:
                        res = await loop.run_in_executor(_CPU_EXECUTOR, self._blocking_fetch, sym)

                    ok = isinstance(res, dict) and not res.get("error")
                    if ok:
                        await self._cb.record_success()
                    else:
                        await self._cb.record_failure()

                    status = "success" if ok else "error"
                    yahoo_requests_total.labels(symbol=sym, op="quote", status=status).inc()
                    yahoo_request_duration.labels(symbol=sym, op="quote").observe(max(0.0, time.time() - t0))

                    if ok:
                        await self.quote_cache.set(sym, res, kind="quote", ttl_sec=_quote_ttl_sec())
                    return res

                except Exception as e:
                    await self._cb.record_failure()
                    yahoo_requests_total.labels(symbol=sym, op="quote", status="exception").inc()
                    yahoo_request_duration.labels(symbol=sym, op="quote").observe(max(0.0, time.time() - t0))
                    return {
                        "symbol": sym,
                        "error": f"exception: {e}",
                        "provider": PROVIDER_NAME,
                        "provider_version": PROVIDER_VERSION,
                        "data_sources": [PROVIDER_NAME],
                    }

        return await self.singleflight.run(f"yahoo:{sym}", _do_fetch)

    async def fetch_batch(self, symbols: List[str], debug: bool = False) -> Dict[str, Dict[str, Any]]:
        syms = [normalize_symbol(s) for s in (symbols or [])]
        syms = [s for s in syms if s]
        if not syms:
            return {}
        tasks = [self.fetch_enriched_quote_patch(s, debug=debug) for s in syms]
        res = await asyncio.gather(*tasks, return_exceptions=True)
        out: Dict[str, Dict[str, Any]] = {}
        for s, r in zip(syms, res):
            if isinstance(r, dict):
                out[s] = r
            else:
                out[s] = {
                    "symbol": s,
                    "error": str(r),
                    "provider": PROVIDER_NAME,
                    "provider_version": PROVIDER_VERSION,
                    "data_sources": [PROVIDER_NAME],
                }
        return out

    async def clear_caches(self) -> None:
        await self.quote_cache.clear()

    async def get_metrics(self) -> Dict[str, Any]:
        return {
            "provider": self.name,
            "version": self.version,
            "cache_size": await self.quote_cache.size(),
            "configured": _configured(),
            "has_numpy": _HAS_NUMPY,
            "has_pandas": _HAS_PANDAS,
            "has_orjson": _HAS_ORJSON,
            "has_yfinance": _HAS_YFINANCE,
            "threadpool_workers": _CPU_WORKERS,
        }

    async def health_check(self) -> Dict[str, Any]:
        sample = os.getenv("YAHOO_HEALTH_SYMBOL", "AAPL")
        res = await self.fetch_enriched_quote_patch(sample, debug=False)
        ok = isinstance(res, dict) and not res.get("error") and res.get("current_price") is not None
        return {
            "status": "healthy" if ok else "unhealthy",
            "sample": sample,
            "error": res.get("error") if isinstance(res, dict) else str(res),
        }


# =============================================================================
# Async singleton (engine-friendly)
# =============================================================================
_PROVIDER_INSTANCE: Optional[YahooChartProvider] = None
_PROVIDER_LOCK = asyncio.Lock()


async def aget_provider() -> YahooChartProvider:
    global _PROVIDER_INSTANCE
    if _PROVIDER_INSTANCE is None:
        async with _PROVIDER_LOCK:
            if _PROVIDER_INSTANCE is None:
                _PROVIDER_INSTANCE = YahooChartProvider()
    return _PROVIDER_INSTANCE


async def fetch_enriched_quote_patch(symbol: str, debug: bool = False, *args: Any, **kwargs: Any) -> Dict[str, Any]:
    return await (await aget_provider()).fetch_enriched_quote_patch(symbol, debug=debug, *args, **kwargs)


# Compatibility alias
fetch_quote_patch = fetch_enriched_quote_patch


async def fetch_batch_patch(symbols: List[str], debug: bool = False) -> Dict[str, Dict[str, Any]]:
    return await (await aget_provider()).fetch_batch(symbols, debug=debug)


async def get_client_metrics() -> Dict[str, Any]:
    return await (await aget_provider()).get_metrics()


async def health_check() -> Dict[str, Any]:
    return await (await aget_provider()).health_check()


async def clear_caches() -> None:
    await (await aget_provider()).clear_caches()


async def aclose_yahoo_client() -> None:
    global _PROVIDER_INSTANCE
    _PROVIDER_INSTANCE = None


# =============================================================================
# Loader-friendly adapter (NO await required to obtain provider)
# =============================================================================
class YahooProviderAdapter:
    async def get_quote(self, symbol: str) -> Dict[str, Any]:
        return await fetch_enriched_quote_patch(symbol)

    async def quote(self, symbol: str) -> Dict[str, Any]:
        return await fetch_enriched_quote_patch(symbol)

    async def fetch_quote(self, symbol: str) -> Dict[str, Any]:
        return await fetch_enriched_quote_patch(symbol)

    async def close(self) -> None:
        await aclose_yahoo_client()


# Export loader-friendly names
provider = YahooProviderAdapter()
PROVIDER = provider
Provider = YahooProviderAdapter


def get_provider():
    return provider


def build_provider():
    return provider


def create_provider():
    return provider


def provider_factory():
    return provider


# =============================================================================
# CLI test
# =============================================================================
if __name__ == "__main__":
    import sys

    async def _test():
        sys.stdout.write(f"\n🔧 Yahoo Chart Provider v{PROVIDER_VERSION}\n")
        sys.stdout.write("=" * 70 + "\n")
        r = await fetch_enriched_quote_patch("AAPL")
        sys.stdout.write(json_dumps(r) + "\n")
        sys.stdout.write("=" * 70 + "\n")

    asyncio.run(_test())


__all__ = [
    "PROVIDER_NAME",
    "PROVIDER_VERSION",
    # engine async exports
    "aget_provider",
    "fetch_enriched_quote_patch",
    "fetch_quote_patch",
    "fetch_batch_patch",
    "get_client_metrics",
    "health_check",
    "clear_caches",
    "aclose_yahoo_client",
    # loader-friendly exports
    "provider",
    "PROVIDER",
    "Provider",
    "get_provider",
    "build_provider",
    "create_provider",
    "provider_factory",
]
