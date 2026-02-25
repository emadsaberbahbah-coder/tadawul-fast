#!/usr/bin/env python3
# core/providers/yahoo_chart_provider.py
"""
================================================================================
Yahoo Chart Provider (Global Market Data) — v6.3.0 (QUANTUM EDITION)
================================================================================

Design Goals (Project-Aligned)
- ✅ Stable in production: no startup crashes (all optional deps are safe)
- ✅ Fast + async-friendly: threadpool execution for yfinance blocking calls
- ✅ Deterministic + safe fallbacks (no heavy ML stacks required)
- ✅ Aligned with:
    - core/config.py (Settings + TraceContext if available)
    - integrations/google_sheets_service.py (sheet-rows enrichment usage)
- ✅ Fixes:
    - No NameError for tracing flags
    - orjson.dumps always uses default=str
    - AdvancedCache includes clear()
    - SingleFlight hardened to avoid deadlocks/hanging futures
    - Duplicate __all__ removed

What this provider returns
- A compact “enriched quote patch” dict suitable for merging into UnifiedQuote / sheet rows:
  price, prev_close, change, change_pct, day_high/low, 52w high/low, vol, rsi, volatility, basic forecasts.

NOTE
- This file intentionally does NOT require torch/scipy/statsmodels/prophet/etc.
- If numpy/pandas are available, indicators are faster; otherwise pure-python fallbacks run.

================================================================================
"""

from __future__ import annotations

import asyncio
import concurrent.futures
import logging
import math
import os
import random
import re
import time
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Any, Awaitable, Callable, Dict, List, Optional, Tuple, Union

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

# Prometheus (optional)
try:
    from prometheus_client import Counter, Histogram, Gauge  # type: ignore

    _HAS_PROM = True
except Exception:
    _HAS_PROM = False

    class _DummyMetric:
        def labels(self, *args, **kwargs):  # noqa: D401
            return self

        def inc(self, *args, **kwargs):
            pass

        def observe(self, *args, **kwargs):
            pass

        def set(self, *args, **kwargs):
            pass

    Counter = Histogram = Gauge = _DummyMetric  # type: ignore

# OpenTelemetry (optional)
try:
    from opentelemetry import trace  # type: ignore
    from opentelemetry.trace import Status, StatusCode  # type: ignore

    _HAS_OTEL = True
except Exception:
    trace = None  # type: ignore
    Status = None  # type: ignore
    StatusCode = None  # type: ignore
    _HAS_OTEL = False

# Prefer core TraceContext if available (aligned with core/config.py v6.1.0+)
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


# JSON helper (orjson safe default=str)
try:
    import orjson  # type: ignore

    def json_dumps(v: Any) -> str:
        return orjson.dumps(v, default=str).decode("utf-8")

    _HAS_ORJSON = True
except Exception:
    import json

    def json_dumps(v: Any) -> str:
        return json.dumps(v, default=str, ensure_ascii=False)

    _HAS_ORJSON = False

# =============================================================================
# Logging
# =============================================================================
logger = logging.getLogger("core.providers.yahoo_chart_provider")
logger.addHandler(logging.NullHandler())

PROVIDER_NAME = "yahoo_chart"
PROVIDER_VERSION = "6.3.0"

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
    return _HAS_YFINANCE


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
def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _riyadh_tz() -> timezone:
    return timezone(timedelta(hours=3))


def _riyadh_now() -> datetime:
    return datetime.now(_riyadh_tz())


def _utc_iso(dt: Optional[datetime] = None) -> str:
    d = dt or _utc_now()
    if d.tzinfo is None:
        d = d.replace(tzinfo=timezone.utc)
    return d.astimezone(timezone.utc).isoformat()


def _riyadh_iso(dt: Optional[datetime] = None) -> str:
    tz = _riyadh_tz()
    d = dt or datetime.now(tz)
    if d.tzinfo is None:
        d = d.replace(tzinfo=timezone.utc)
    return d.astimezone(tz).isoformat()


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
    # keep existing suffixes
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
# Metrics (optional)
# =============================================================================
if _HAS_PROM:
    yahoo_requests_total = Counter(
        "yahoo_requests_total",
        "Total Yahoo requests",
        ["symbol", "op", "status"],
    )
    yahoo_request_duration = Histogram(
        "yahoo_request_duration_seconds",
        "Yahoo request duration",
        ["symbol", "op"],
    )
    yahoo_cache_hits_total = Counter("yahoo_cache_hits_total", "Yahoo cache hits", ["symbol"])
    yahoo_cache_misses_total = Counter("yahoo_cache_misses_total", "Yahoo cache misses", ["symbol"])
    yahoo_circuit_state = Gauge("yahoo_circuit_state", "Yahoo circuit breaker state", ["state"])
else:
    yahoo_requests_total = Counter()
    yahoo_request_duration = Histogram()
    yahoo_cache_hits_total = Counter()
    yahoo_cache_misses_total = Counter()
    yahoo_circuit_state = Gauge()

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
            return  # disabled
        async with self.lock:
            now = time.monotonic()
            if self.last <= 0:
                self.last = now
                self.tokens = self.burst
            # refill
            elapsed = max(0.0, now - self.last)
            self.tokens = min(self.burst, self.tokens + elapsed * self.rate_per_sec)
            self.last = now
            if self.tokens >= n:
                self.tokens -= n
                return
            # need wait
            need = n - self.tokens
            wait = need / self.rate_per_sec
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
            # half_open
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
                self.failures = 0

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
        # fast path: reuse existing future
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
            # simple eviction: drop oldest ~10%
            if len(self._data) >= self.maxsize and key not in self._data:
                # deterministic eviction (first N keys)
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
# Indicators (fast + safe)
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
            # Wilder smoothing
            n = 14
            avg_gain = gains[:n].mean()
            avg_loss = losses[:n].mean()
            for i in range(n, len(gains)):
                avg_gain = (avg_gain * (n - 1) + gains[i]) / n
                avg_loss = (avg_loss * (n - 1) + losses[i]) / n
            if avg_loss == 0:
                return 100.0
            rs = avg_gain / avg_loss
            return float(100.0 - (100.0 / (1.0 + rs)))
        # pure python
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
            vol = float(np.std(rets, ddof=1) * math.sqrt(252))
            if math.isnan(vol) or math.isinf(vol):
                return None
            return vol
        # pure python
        rets = []
        for i in range(1, len(window)):
            if window[i - 1] > 0 and window[i] > 0:
                rets.append(math.log(window[i] / window[i - 1]))
        if len(rets) < 2:
            return None
        mean = sum(rets) / len(rets)
        var = sum((r - mean) ** 2 for r in rets) / (len(rets) - 1)
        vol = math.sqrt(var) * math.sqrt(252)
        return vol
    except Exception:
        return None


def _simple_forecast(closes: List[float], horizon_days: int) -> Tuple[Optional[float], Optional[float], float]:
    """
    Simple log-linear forecast:
    - returns (forecast_price, expected_roi_pct, confidence_0_1)
    Confidence = f(R^2, volatility)
    """
    if len(closes) < 60 or horizon_days <= 0:
        return None, None, 0.0
    try:
        n = min(len(closes), 252)  # limit training window
        y = closes[-n:]
        if any(p is None or p <= 0 for p in y):
            return None, None, 0.0

        if _HAS_NUMPY and np is not None:
            arr = np.asarray(y, dtype=float)
            x = np.arange(len(arr), dtype=float)
            logy = np.log(arr)

            # fit line
            coeff = np.polyfit(x, logy, 1)
            slope, intercept = float(coeff[0]), float(coeff[1])

            # r^2
            pred = slope * x + intercept
            ss_res = float(np.sum((logy - pred) ** 2))
            ss_tot = float(np.sum((logy - float(np.mean(logy))) ** 2))
            r2 = 1.0 - ss_res / ss_tot if ss_tot > 0 else 0.0

            future_x = float(len(arr) - 1 + horizon_days)
            future_log = slope * future_x + intercept
            forecast_price = float(math.exp(future_log))
        else:
            # fallback: use simple CAGR
            start = y[0]
            end = y[-1]
            if start <= 0:
                return None, None, 0.0
            years = max(1e-6, len(y) / 252.0)
            cagr = (end / start) ** (1 / years) - 1
            forecast_price = end * ((1 + cagr) ** (horizon_days / 252.0))
            r2 = 0.25  # conservative fallback

        last = float(closes[-1])
        roi_pct = (forecast_price / last - 1.0) * 100.0

        vol = _volatility_30d(closes) or 0.25
        # confidence: r2 (0..1) penalized by high vol
        conf = max(0.05, min(0.95, (max(0.0, min(1.0, r2)) * 0.8 + 0.2) * (1.0 / (1.0 + max(0.0, vol - 0.25)))))
        return forecast_price, roi_pct, conf
    except Exception:
        return None, None, 0.0


# =============================================================================
# Provider Implementation
# =============================================================================
_CPU_EXECUTOR = concurrent.futures.ThreadPoolExecutor(max_workers=6, thread_name_prefix="YahooWorker")


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
    _cb: CircuitBreaker = field(
        default_factory=lambda: CircuitBreaker(_cb_fail_threshold(), _cb_cooldown_sec(), _cb_success_threshold())
    )

    # ----------------------------
    # Core blocking fetch
    # ----------------------------
    def _blocking_fetch(self, symbol: str) -> Dict[str, Any]:
        if not _HAS_YFINANCE or yf is None:
            return {"symbol": symbol, "error": "yfinance_not_installed"}

        last_err: Optional[Exception] = None
        for attempt in range(max(1, int(self.retry_attempts))):
            try:
                t0 = time.time()
                ticker = yf.Ticker(symbol)

                # Prefer fast_info when available
                fast_info = None
                try:
                    fast_info = ticker.fast_info
                except Exception:
                    fast_info = None

                info = None
                try:
                    # info can be slow; tolerate failures
                    info = ticker.info
                except Exception:
                    info = None

                out: Dict[str, Any] = {
                    "symbol": symbol,
                    "data_source": PROVIDER_NAME,
                    "provider_version": PROVIDER_VERSION,
                }

                # Prices
                price = safe_float(getattr(fast_info, "last_price", None)) if fast_info is not None else None
                prev_close = safe_float(getattr(fast_info, "previous_close", None)) if fast_info is not None else None

                if price is None and isinstance(info, dict):
                    price = safe_float(info.get("regularMarketPrice"))
                if prev_close is None and isinstance(info, dict):
                    prev_close = safe_float(info.get("regularMarketPreviousClose"))

                # Day range
                day_high = safe_float(getattr(fast_info, "day_high", None)) if fast_info is not None else None
                day_low = safe_float(getattr(fast_info, "day_low", None)) if fast_info is not None else None
                if (day_high is None or day_low is None) and isinstance(info, dict):
                    day_high = day_high or safe_float(info.get("dayHigh"))
                    day_low = day_low or safe_float(info.get("dayLow"))

                # 52W
                w52_high = safe_float(getattr(fast_info, "fifty_two_week_high", None)) if fast_info is not None else None
                w52_low = safe_float(getattr(fast_info, "fifty_two_week_low", None)) if fast_info is not None else None
                if (w52_high is None or w52_low is None) and isinstance(info, dict):
                    w52_high = w52_high or safe_float(info.get("fiftyTwoWeekHigh"))
                    w52_low = w52_low or safe_float(info.get("fiftyTwoWeekLow"))

                volume = safe_float(getattr(fast_info, "last_volume", None)) if fast_info is not None else None
                market_cap = safe_float(getattr(fast_info, "market_cap", None)) if fast_info is not None else None
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

                # History (for indicators + fallback price)
                closes: List[float] = []
                hist_last_dt: Optional[datetime] = None
                try:
                    hist = ticker.history(period=_history_period(), interval=_history_interval(), auto_adjust=False)
                    if hist is not None and not getattr(hist, "empty", True):
                        if _HAS_PANDAS and pd is not None:
                            c = hist["Close"].dropna().astype(float).tolist()
                        else:
                            c = [safe_float(x) for x in list(hist["Close"])]
                            c = [x for x in c if x is not None]
                        closes = [float(x) for x in c if x is not None]
                        # last timestamp
                        try:
                            idx = hist.index[-1]
                            hist_last_dt = idx.to_pydatetime() if hasattr(idx, "to_pydatetime") else idx
                        except Exception:
                            hist_last_dt = None

                        if price is None and closes:
                            price = float(closes[-1])
                except Exception:
                    # history not mandatory
                    closes = []

                if price is None:
                    raise ValueError("no_price_from_yahoo")

                out["price"] = price
                out["prev_close"] = prev_close
                out["day_high"] = day_high
                out["day_low"] = day_low
                out["52w_high"] = w52_high
                out["52w_low"] = w52_low
                out["volume"] = volume
                out["market_cap"] = market_cap
                out["currency"] = currency
                out["name"] = name
                out["exchange"] = exchange

                # Derived
                if prev_close is not None and prev_close != 0 and price is not None:
                    chg = price - prev_close
                    out["change"] = chg
                    out["change_pct"] = (chg / prev_close) * 100.0

                if w52_high is not None and w52_low is not None and w52_high != w52_low and price is not None:
                    out["52w_position_pct"] = ((price - w52_low) / (w52_high - w52_low)) * 100.0

                # Indicators
                if closes:
                    out["rsi_14"] = _rsi_14(closes)
                    out["volatility_30d"] = _volatility_30d(closes)

                    # Forecasts (simple, deterministic)
                    f1, roi1, c1 = _simple_forecast(closes, horizon_days=30)
                    f3, roi3, c3 = _simple_forecast(closes, horizon_days=90)
                    f12, roi12, c12 = _simple_forecast(closes, horizon_days=365)

                    if f1 is not None:
                        out["forecast_price_1m"] = f1
                        out["expected_roi_1m"] = roi1
                    if f3 is not None:
                        out["forecast_price_3m"] = f3
                        out["expected_roi_3m"] = roi3
                    if f12 is not None:
                        out["forecast_price_12m"] = f12
                        out["expected_roi_12m"] = roi12

                    # single confidence (min is conservative)
                    conf = max(0.0, min(1.0, min([x for x in [c1, c3, c12] if x is not None] or [0.0])))
                    out["forecast_confidence"] = conf

                # timestamps
                out["last_updated_utc"] = _utc_iso()
                out["last_updated_riyadh"] = _riyadh_iso()
                if hist_last_dt is not None:
                    out["history_last_ts_utc"] = _utc_iso(hist_last_dt)

                # data quality (simple score)
                score = 0
                for k in ("price", "prev_close", "volume", "52w_high", "52w_low"):
                    if out.get(k) is not None:
                        score += 1
                out["data_quality_score"] = score / 5.0

                # duration (debug)
                out["_fetch_ms"] = int((time.time() - t0) * 1000)

                return clean_dict(out)

            except Exception as e:
                last_err = e
                # full jitter backoff
                base = min(5.0, 0.5 * (2 ** attempt))
                time.sleep(random.uniform(0.0, base))

        return {"symbol": symbol, "error": f"fetch_failed: {str(last_err)}", "data_source": PROVIDER_NAME, "provider_version": PROVIDER_VERSION}

    # ----------------------------
    # Public async API
    # ----------------------------
    async def fetch_enriched_quote_patch(self, symbol: str, debug: bool = False, *args: Any, **kwargs: Any) -> Dict[str, Any]:
        """
        Async entry point used by the backend.
        Returns dict with 'error' on failures.
        """
        if not _configured():
            return {"symbol": symbol, "error": "provider_disabled_or_yfinance_missing", "data_source": PROVIDER_NAME, "provider_version": PROVIDER_VERSION}

        sym = normalize_symbol(symbol)
        if not sym:
            return {"symbol": symbol, "error": "invalid_symbol", "data_source": PROVIDER_NAME, "provider_version": PROVIDER_VERSION}

        # Circuit breaker
        if not await self._cb.allow():
            return {"symbol": sym, "error": "circuit_open", "data_source": PROVIDER_NAME, "provider_version": PROVIDER_VERSION}

        # Cache
        cached = await self.quote_cache.get(sym, kind="quote")
        if cached is not None:
            yahoo_cache_hits_total.labels(symbol=sym).inc()
            return cached
        yahoo_cache_misses_total.labels(symbol=sym).inc()

        # SingleFlight wrapper
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

                    ok = "error" not in res
                    await (self._cb.record_success() if ok else self._cb.record_failure())

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
                    return {"symbol": sym, "error": f"exception: {e}", "data_source": PROVIDER_NAME, "provider_version": PROVIDER_VERSION}

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
                out[s] = {"symbol": s, "error": str(r), "data_source": PROVIDER_NAME, "provider_version": PROVIDER_VERSION}
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
        }

    async def health_check(self) -> Dict[str, Any]:
        # lightweight health check (do not explode)
        sample = os.getenv("YAHOO_HEALTH_SYMBOL", "AAPL")
        res = await self.fetch_enriched_quote_patch(sample, debug=False)
        return {"status": "healthy" if isinstance(res, dict) and not res.get("error") else "unhealthy", "sample": sample, "error": res.get("error")}


# =============================================================================
# Singleton + Exports
# =============================================================================
_PROVIDER_INSTANCE: Optional[YahooChartProvider] = None
_PROVIDER_LOCK = asyncio.Lock()


async def get_provider() -> YahooChartProvider:
    global _PROVIDER_INSTANCE
    if _PROVIDER_INSTANCE is None:
        async with _PROVIDER_LOCK:
            if _PROVIDER_INSTANCE is None:
                _PROVIDER_INSTANCE = YahooChartProvider()
    return _PROVIDER_INSTANCE


async def fetch_enriched_quote_patch(symbol: str, debug: bool = False, *args: Any, **kwargs: Any) -> Dict[str, Any]:
    return await (await get_provider()).fetch_enriched_quote_patch(symbol, debug=debug, *args, **kwargs)


# Compatibility alias
fetch_quote_patch = fetch_enriched_quote_patch


async def fetch_batch_patch(symbols: List[str], debug: bool = False) -> Dict[str, Dict[str, Any]]:
    return await (await get_provider()).fetch_batch(symbols, debug=debug)


async def get_client_metrics() -> Dict[str, Any]:
    return await (await get_provider()).get_metrics()


async def health_check() -> Dict[str, Any]:
    return await (await get_provider()).health_check()


async def clear_caches() -> None:
    await (await get_provider()).clear_caches()


async def aclose_yahoo_client() -> None:
    global _PROVIDER_INSTANCE
    _PROVIDER_INSTANCE = None


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
    "get_provider",
    "fetch_enriched_quote_patch",
    "fetch_quote_patch",
    "fetch_batch_patch",
    "get_client_metrics",
    "health_check",
    "clear_caches",
    "aclose_yahoo_client",
]
