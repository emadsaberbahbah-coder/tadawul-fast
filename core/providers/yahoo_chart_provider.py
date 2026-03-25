#!/usr/bin/env python3
# core/providers/yahoo_chart_provider.py
"""
================================================================================
Yahoo Chart Provider (Global + KSA History) — v6.8.0
================================================================================
QUOTE/HISTORY HARDENED • COMMODITIES/FX FALLBACKS • CANONICAL FIELD ENRICHMENT
FUNDAMENTALS-LIGHT • HISTORY-DERIVED AVG VOLUME / 52W / RISK STATS • IMPORT-SAFE

Why this revision
- ✅ FIX: strengthen quote extraction for commodities / FX / futures by using
      more Yahoo aliases and metadata fallbacks.
- ✅ FIX: fill history-derived core fields more consistently:
      current_price, previous_close, open_price, day_high, day_low, volume,
      week_52_high, week_52_low, week_52_position_pct, avg_volume_10d,
      avg_volume_30d.
- ✅ FIX: opportunistically return light fundamentals/profile fields when Yahoo
      info exposes them, so the engine can hydrate more canonical columns.
- ✅ FIX: expand compatibility aliases / methods for legacy callers.

What this provider guarantees
- ✅ Startup-safe: no network calls at import-time
- ✅ Async-friendly: yfinance is blocking → executed in threadpool
- ✅ Deterministic + safe fallbacks (no heavy ML required)
- ✅ Canonical schema-aligned keys (engine-friendly)
- ✅ History API: fetch_history(symbol, period?, interval?) → list[{date, close}]
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
import statistics
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
# Prometheus (optional) — LAZY + DUPLICATE-SAFE
# =============================================================================
try:
    from prometheus_client import Counter, Gauge, Histogram, REGISTRY  # type: ignore

    _HAS_PROM = True
except Exception:
    Counter = Gauge = Histogram = None  # type: ignore
    REGISTRY = None  # type: ignore
    _HAS_PROM = False


class _DummyMetric:
    def __init__(self, *args: Any, **kwargs: Any) -> None:
        pass

    def labels(self, *args: Any, **kwargs: Any) -> "_DummyMetric":
        return self

    def inc(self, *args: Any, **kwargs: Any) -> None:
        return None

    def observe(self, *args: Any, **kwargs: Any) -> None:
        return None

    def set(self, *args: Any, **kwargs: Any) -> None:
        return None


def _prom_enabled() -> bool:
    raw = (os.getenv("PROMETHEUS_ENABLED") or os.getenv("METRICS_ENABLED") or "").strip().lower()
    if raw in {"0", "false", "no", "n", "off", "disabled"}:
        return False
    return bool(_HAS_PROM)


def _safe_get_existing_collector(name: str) -> Optional[Any]:
    try:
        if REGISTRY is None:
            return None
        m = getattr(REGISTRY, "_names_to_collectors", None)
        if isinstance(m, dict):
            return m.get(name)
    except Exception:
        return None
    return None


def _safe_create_metric(ctor: Any, name: str, doc: str, labelnames: List[str]) -> Any:
    if not _prom_enabled() or ctor is None:
        return _DummyMetric()
    try:
        return ctor(name, doc, labelnames)  # type: ignore[misc]
    except ValueError:
        existing = _safe_get_existing_collector(name)
        return existing if existing is not None else _DummyMetric()
    except Exception:
        return _DummyMetric()


@dataclass(slots=True)
class _Metrics:
    requests_total: Any
    request_duration: Any
    cache_hits_total: Any
    cache_misses_total: Any
    circuit_state: Any


_METRICS: Optional[_Metrics] = None


def metrics() -> _Metrics:
    global _METRICS
    if _METRICS is not None:
        return _METRICS

    _METRICS = _Metrics(
        requests_total=_safe_create_metric(
            Counter,
            "yahoo_requests_total",
            "Total Yahoo requests",
            ["symbol", "op", "status"],
        ),
        request_duration=_safe_create_metric(
            Histogram,
            "yahoo_request_duration_seconds",
            "Yahoo request duration",
            ["symbol", "op"],
        ),
        cache_hits_total=_safe_create_metric(
            Counter,
            "yahoo_cache_hits_total",
            "Yahoo cache hits",
            ["symbol"],
        ),
        cache_misses_total=_safe_create_metric(
            Counter,
            "yahoo_cache_misses_total",
            "Yahoo cache misses",
            ["symbol"],
        ),
        circuit_state=_safe_create_metric(
            Gauge,
            "yahoo_circuit_state",
            "Yahoo circuit breaker state",
            ["state"],
        ),
    )
    return _METRICS


# =============================================================================
# OpenTelemetry (optional) — SAFE
# =============================================================================
try:
    from opentelemetry import trace  # type: ignore

    _HAS_OTEL = True
except Exception:
    trace = None  # type: ignore
    _HAS_OTEL = False

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
PROVIDER_VERSION = "6.8.0"
VERSION = PROVIDER_VERSION

# =============================================================================
# Env helpers (safe)
# =============================================================================
_TRUTHY = {"1", "true", "yes", "y", "on", "t", "enable", "enabled"}
_FALSY = {"0", "false", "no", "n", "off", "f", "disable", "disabled"}

_KSA_CODE_RE = re.compile(r"^\d{3,6}$", re.IGNORECASE)
_FX_PAIR_RE = re.compile(r"^([A-Z]{6})=X$")
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


def _history_ttl_sec() -> float:
    return max(30.0, _env_float("YAHOO_HISTORY_TTL_SEC", 300.0))


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
        if isinstance(x, (int, float)) and not isinstance(x, bool):
            f = float(x)
            if math.isnan(f) or math.isinf(f):
                return None
            return f

        s = str(x).strip()
        if not s or s.lower() in {"-", "—", "n/a", "na", "null", "none", "nan"}:
            return None

        s = s.translate(_ARABIC_DIGITS)
        s = s.replace(",", "").replace("%", "").replace("+", "")
        s = s.replace("$", "").replace("£", "").replace("€", "")
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


def _percentish(x: Any) -> Optional[float]:
    f = safe_float(x)
    if f is None:
        return None
    if -1.0 <= f <= 1.0:
        return float(f * 100.0)
    return float(f)


def _first_number(*values: Any) -> Optional[float]:
    for v in values:
        f = safe_float(v)
        if f is not None:
            return f
    return None


def _first_percentish(*values: Any) -> Optional[float]:
    for v in values:
        f = _percentish(v)
        if f is not None:
            return f
    return None


def _first_str(*values: Any) -> Optional[str]:
    for v in values:
        s = safe_str(v)
        if s is not None:
            return s
    return None


def normalize_symbol(symbol: str) -> str:
    s = (symbol or "").strip()
    if not s:
        return ""
    s = s.translate(_ARABIC_DIGITS).strip().upper()
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
    state: str = "closed"
    lock: asyncio.Lock = field(default_factory=asyncio.Lock)

    async def allow(self) -> bool:
        m = metrics()
        async with self.lock:
            if self.state == "closed":
                m.circuit_state.labels(state="closed").set(0.0)
                return True
            if self.state == "open":
                now = time.monotonic()
                if now - self.opened_at >= self.cooldown_sec:
                    self.state = "half_open"
                    self.successes = 0
                    m.circuit_state.labels(state="half_open").set(1.0)
                    return True
                m.circuit_state.labels(state="open").set(2.0)
                return False
            m.circuit_state.labels(state="half_open").set(1.0)
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
# SingleFlight
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
# Risk / Stats helpers
# =============================================================================
def _simple_returns(closes: List[float]) -> List[float]:
    out: List[float] = []
    for i in range(1, len(closes)):
        p0 = closes[i - 1]
        p1 = closes[i]
        if p0 and p0 > 0:
            out.append((p1 / p0) - 1.0)
    return out


def _log_returns(closes: List[float]) -> List[float]:
    out: List[float] = []
    for i in range(1, len(closes)):
        p0 = closes[i - 1]
        p1 = closes[i]
        if p0 and p0 > 0 and p1 and p1 > 0:
            out.append(math.log(p1 / p0))
    return out


def _annualized_vol_from_logrets(logrets: List[float]) -> Optional[float]:
    if len(logrets) < 2:
        return None
    try:
        if _HAS_NUMPY and np is not None:
            v = float(np.std(np.asarray(logrets, dtype=float), ddof=1) * math.sqrt(252.0))
            if math.isnan(v) or math.isinf(v):
                return None
            return v
        std = statistics.stdev(logrets)
        return float(std * math.sqrt(252.0))
    except Exception:
        return None


def _volatility_nd(closes: List[float], days: int) -> Optional[float]:
    if len(closes) < days + 1:
        return None
    window = closes[-(days + 1) :]
    return _annualized_vol_from_logrets(_log_returns(window))


def _max_drawdown_1y(closes: List[float]) -> Optional[float]:
    if len(closes) < 20:
        return None
    window = closes[-252:] if len(closes) >= 252 else closes[:]
    peak = None
    max_dd = 0.0
    for p in window:
        if p is None or p <= 0:
            continue
        if peak is None or p > peak:
            peak = p
        if peak and peak > 0:
            dd = (p / peak) - 1.0
            if dd < max_dd:
                max_dd = dd
    return float(abs(max_dd)) if max_dd < 0 else 0.0


def _var_95_1d(closes: List[float]) -> Optional[float]:
    rets = _simple_returns(closes)
    if len(rets) < 30:
        return None
    try:
        if _HAS_NUMPY and np is not None:
            q = float(np.quantile(np.asarray(rets, dtype=float), 0.05))
        else:
            s = sorted(rets)
            idx = max(0, int(0.05 * (len(s) - 1)))
            q = float(s[idx])
        return float(abs(q)) if q < 0 else 0.0
    except Exception:
        return None


def _sharpe_1y(closes: List[float]) -> Optional[float]:
    rets = _simple_returns(closes)
    if len(rets) < 60:
        return None
    w = rets[-252:] if len(rets) >= 252 else rets[:]
    if len(w) < 30:
        return None
    try:
        if _HAS_NUMPY and np is not None:
            arr = np.asarray(w, dtype=float)
            mu = float(np.mean(arr))
            sd = float(np.std(arr, ddof=1))
        else:
            mu = float(sum(w) / len(w))
            sd = float(statistics.stdev(w))
        if sd <= 0 or math.isnan(sd) or math.isinf(sd):
            return None
        return float((mu * 252.0) / (sd * math.sqrt(252.0)))
    except Exception:
        return None


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


def _simple_forecast(closes: List[float], horizon_days: int) -> Tuple[Optional[float], Optional[float], float]:
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

        vol30 = _volatility_nd(closes, 30) or 0.25
        conf = max(
            0.05,
            min(
                0.95,
                (max(0.0, min(1.0, r2)) * 0.8 + 0.2) * (1.0 / (1.0 + max(0.0, vol30 - 0.25))),
            ),
        )
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
                continue
            if isinstance(obj, dict) and n in obj:
                return obj.get(n)
            if hasattr(obj, n):
                return getattr(obj, n)
        except Exception:
            continue
    return None


def _safe_history_metadata(ticker: Any) -> Dict[str, Any]:
    for attr_name in ("history_metadata",):
        try:
            meta = getattr(ticker, attr_name)
            if isinstance(meta, dict) and meta:
                return meta
        except Exception:
            pass
    for fn_name in ("get_history_metadata",):
        try:
            fn = getattr(ticker, fn_name, None)
            if callable(fn):
                meta = fn()
                if isinstance(meta, dict) and meta:
                    return meta
        except Exception:
            pass
    return {}


def _infer_asset_class(symbol: str, info: Any = None, meta: Optional[Dict[str, Any]] = None) -> Optional[str]:
    qt = None
    if isinstance(info, dict):
        qt = safe_str(info.get("quoteType") or info.get("quote_type") or info.get("type"))
    if qt is None and isinstance(meta, dict):
        qt = safe_str(meta.get("instrumentType") or meta.get("quoteType"))
    if qt:
        qtu = qt.upper()
        if qtu in {"CURRENCY", "FOREX", "FX"}:
            return "FX"
        if qtu in {"ETF", "MUTUALFUND", "MUTUAL FUND"}:
            return qtu
        return qtu

    s = symbol.upper()
    if s.endswith(".SR"):
        return "EQUITY"
    if s.endswith("=F"):
        return "FUTURE"
    if s.endswith("=X"):
        return "FX"
    if s.startswith("^"):
        return "INDEX"
    return None


def _infer_exchange(symbol: str, info: Any = None, meta: Optional[Dict[str, Any]] = None) -> Optional[str]:
    if isinstance(info, dict):
        ex = _first_str(info.get("exchange"), info.get("fullExchangeName"), info.get("exchangeName"))
        if ex:
            return ex
    if isinstance(meta, dict):
        ex = _first_str(meta.get("exchangeName"), meta.get("exchangeTimezoneName"))
        if ex:
            return ex

    s = symbol.upper()
    if s.endswith(".SR"):
        return "Tadawul"
    if s.endswith("=F"):
        return "Yahoo Futures"
    if s.endswith("=X"):
        return "Yahoo FX"
    return None


def _infer_currency(symbol: str, info: Any = None, meta: Optional[Dict[str, Any]] = None) -> Optional[str]:
    if isinstance(info, dict):
        cur = _first_str(info.get("currency"), info.get("financialCurrency"))
        if cur:
            return cur
    if isinstance(meta, dict):
        cur = _first_str(meta.get("currency"), meta.get("financialCurrency"))
        if cur:
            return cur

    s = symbol.upper()
    if s.endswith(".SR"):
        return "SAR"
    m = _FX_PAIR_RE.match(s)
    if m:
        pair = m.group(1)
        if len(pair) == 6:
            return pair[3:]
    return None


def _infer_country(symbol: str, info: Any = None) -> Optional[str]:
    if isinstance(info, dict):
        c = safe_str(info.get("country"))
        if c:
            return c
    if symbol.upper().endswith(".SR"):
        return "Saudi Arabia"
    return None


def _history_df_extract(hist: Any) -> Dict[str, Any]:
    bundle: Dict[str, Any] = {
        "closes": [],
        "opens": [],
        "highs": [],
        "lows": [],
        "volumes": [],
        "last_row": None,
        "last_dt": None,
    }
    if hist is None:
        return bundle

    try:
        if getattr(hist, "empty", None) is True:
            return bundle
    except Exception:
        pass

    try:
        if hasattr(hist, "iloc"):
            try:
                bundle["last_row"] = hist.iloc[-1]
            except Exception:
                bundle["last_row"] = None
        try:
            idx = hist.index[-1]
            bundle["last_dt"] = idx.to_pydatetime() if hasattr(idx, "to_pydatetime") else idx
        except Exception:
            bundle["last_dt"] = None

        def _series(col: str) -> List[float]:
            try:
                if _HAS_PANDAS and pd is not None:
                    vals = hist[col].dropna().astype(float).tolist()
                else:
                    vals = [safe_float(x) for x in list(hist[col])]
                    vals = [x for x in vals if x is not None]
                return [float(x) for x in vals if x is not None]
            except Exception:
                return []

        bundle["closes"] = [x for x in _series("Close") if x > 0]
        bundle["opens"] = [x for x in _series("Open") if x > 0]
        bundle["highs"] = [x for x in _series("High") if x > 0]
        bundle["lows"] = [x for x in _series("Low") if x > 0]
        bundle["volumes"] = [x for x in _series("Volume") if x >= 0]
    except Exception:
        return {
            "closes": [],
            "opens": [],
            "highs": [],
            "lows": [],
            "volumes": [],
            "last_row": None,
            "last_dt": None,
        }
    return bundle


def _history_df_to_list(hist: Any) -> List[Dict[str, Any]]:
    if hist is None:
        return []
    try:
        if getattr(hist, "empty", None) is True:
            return []
    except Exception:
        pass

    out: List[Dict[str, Any]] = []
    try:
        idx = list(hist.index)
        closes = list(hist["Close"])
        for i in range(min(len(idx), len(closes))):
            dt = idx[i]
            c = safe_float(closes[i])
            if c is None:
                continue
            if hasattr(dt, "to_pydatetime"):
                dt = dt.to_pydatetime()
            sdt = dt.isoformat() if hasattr(dt, "isoformat") else str(dt)
            out.append({"date": sdt, "close": float(c)})
    except Exception:
        return []
    return out


def _avg_last_n(values: List[float], n: int) -> Optional[float]:
    if not values:
        return None
    window = [float(v) for v in values[-n:] if v is not None]
    if not window:
        return None
    return float(sum(window) / len(window))


def _row_get(row: Any, key: str) -> Any:
    try:
        if row is None:
            return None
        if hasattr(row, "get"):
            return row.get(key)
        return row[key]
    except Exception:
        return None


@dataclass(slots=True)
class YahooChartProvider:
    name: str = PROVIDER_NAME
    version: str = PROVIDER_VERSION

    timeout_sec: float = field(default_factory=_timeout_sec)
    retry_attempts: int = 3
    max_concurrency: int = field(default_factory=_max_concurrency)

    quote_cache: AdvancedCache = field(default_factory=lambda: AdvancedCache("yahoo", _quote_ttl_sec()))
    history_cache: AdvancedCache = field(default_factory=lambda: AdvancedCache("yahoo", _history_ttl_sec()))
    singleflight: SingleFlight = field(default_factory=SingleFlight)

    _sem: asyncio.Semaphore = field(default_factory=lambda: asyncio.Semaphore(_max_concurrency()))
    _rate: TokenBucket = field(default_factory=lambda: TokenBucket(_rate_limit_per_sec(), float(_rate_limit_burst())))
    _cb: CircuitBreaker = field(
        default_factory=lambda: CircuitBreaker(_cb_fail_threshold(), _cb_cooldown_sec(), _cb_success_threshold())
    )

    def _blocking_fetch_history(self, symbol: str, period: str, interval: str) -> Dict[str, Any]:
        if not _HAS_YFINANCE or yf is None:
            return {"symbol": symbol, "error": "yfinance_not_installed", "provider": PROVIDER_NAME}

        last_err: Optional[Exception] = None
        for attempt in range(max(1, int(self.retry_attempts))):
            try:
                t0 = time.time()
                ticker = yf.Ticker(symbol)
                hist = ticker.history(period=period, interval=interval, auto_adjust=False)
                rows = _history_df_to_list(hist)
                last_dt = None
                try:
                    if hist is not None and not getattr(hist, "empty", True):
                        idx = hist.index[-1]
                        last_dt = idx.to_pydatetime() if hasattr(idx, "to_pydatetime") else idx
                except Exception:
                    last_dt = None

                return clean_dict(
                    {
                        "symbol": symbol,
                        "provider": PROVIDER_NAME,
                        "provider_version": PROVIDER_VERSION,
                        "history": rows,
                        "history_last_utc": _utc_iso(last_dt) if last_dt else None,
                        "_fetch_ms": int((time.time() - t0) * 1000),
                    }
                )
            except Exception as e:
                last_err = e
                base = min(5.0, 0.5 * (2**attempt))
                time.sleep(random.uniform(0.0, base))
        return {"symbol": symbol, "error": f"history_fetch_failed: {last_err}", "provider": PROVIDER_NAME}

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

                try:
                    fast_info = ticker.fast_info
                except Exception:
                    fast_info = None

                try:
                    info = ticker.info
                except Exception:
                    info = None

                meta: Dict[str, Any] = {}
                out: Dict[str, Any] = {
                    "symbol": symbol,
                    "requested_symbol": symbol,
                    "provider": PROVIDER_NAME,
                    "provider_version": PROVIDER_VERSION,
                    "data_sources": [PROVIDER_NAME],
                }

                hist = None
                history_bundle = {
                    "closes": [],
                    "opens": [],
                    "highs": [],
                    "lows": [],
                    "volumes": [],
                    "last_row": None,
                    "last_dt": None,
                }
                try:
                    hist = ticker.history(period=_history_period(), interval=_history_interval(), auto_adjust=False)
                    history_bundle = _history_df_extract(hist)
                    meta = _safe_history_metadata(ticker)
                except Exception:
                    hist = None
                    history_bundle = {
                        "closes": [],
                        "opens": [],
                        "highs": [],
                        "lows": [],
                        "volumes": [],
                        "last_row": None,
                        "last_dt": None,
                    }
                    meta = _safe_history_metadata(ticker)

                closes: List[float] = history_bundle["closes"]
                opens: List[float] = history_bundle["opens"]
                highs: List[float] = history_bundle["highs"]
                lows: List[float] = history_bundle["lows"]
                volumes: List[float] = history_bundle["volumes"]
                last_hist_row = history_bundle["last_row"]
                hist_last_dt: Optional[datetime] = history_bundle["last_dt"]

                if isinstance(info, dict):
                    out["raw_quote_type"] = safe_str(info.get("quoteType"))
                if isinstance(meta, dict):
                    out["raw_instrument_type"] = safe_str(meta.get("instrumentType"))

                price = _first_number(
                    _get_attr(fast_info, "last_price", "lastPrice", "regularMarketPrice"),
                    info.get("regularMarketPrice") if isinstance(info, dict) else None,
                    info.get("currentPrice") if isinstance(info, dict) else None,
                    info.get("navPrice") if isinstance(info, dict) else None,
                    meta.get("regularMarketPrice") if isinstance(meta, dict) else None,
                    _row_get(last_hist_row, "Close"),
                    closes[-1] if closes else None,
                )

                prev_close = _first_number(
                    _get_attr(fast_info, "previous_close", "previousClose", "regularMarketPreviousClose"),
                    info.get("regularMarketPreviousClose") if isinstance(info, dict) else None,
                    info.get("previousClose") if isinstance(info, dict) else None,
                    meta.get("previousClose") if isinstance(meta, dict) else None,
                    meta.get("chartPreviousClose") if isinstance(meta, dict) else None,
                    closes[-2] if len(closes) >= 2 else None,
                )

                open_price = _first_number(
                    _get_attr(fast_info, "open", "open_price", "regularMarketOpen"),
                    info.get("regularMarketOpen") if isinstance(info, dict) else None,
                    info.get("open") if isinstance(info, dict) else None,
                    _row_get(last_hist_row, "Open"),
                    opens[-1] if opens else None,
                )

                day_high = _first_number(
                    _get_attr(fast_info, "day_high", "dayHigh", "regularMarketDayHigh"),
                    info.get("regularMarketDayHigh") if isinstance(info, dict) else None,
                    info.get("dayHigh") if isinstance(info, dict) else None,
                    _row_get(last_hist_row, "High"),
                    highs[-1] if highs else None,
                )

                day_low = _first_number(
                    _get_attr(fast_info, "day_low", "dayLow", "regularMarketDayLow"),
                    info.get("regularMarketDayLow") if isinstance(info, dict) else None,
                    info.get("dayLow") if isinstance(info, dict) else None,
                    _row_get(last_hist_row, "Low"),
                    lows[-1] if lows else None,
                )

                w52_high = _first_number(
                    _get_attr(fast_info, "fifty_two_week_high", "fiftyTwoWeekHigh", "week52High", "year_high", "yearHigh"),
                    info.get("fiftyTwoWeekHigh") if isinstance(info, dict) else None,
                    info.get("regularMarketDayHigh") if isinstance(info, dict) and symbol.endswith("=F") and not highs else None,
                    max(highs[-252:]) if highs else None,
                    max(closes[-252:]) if closes else None,
                )

                w52_low = _first_number(
                    _get_attr(fast_info, "fifty_two_week_low", "fiftyTwoWeekLow", "week52Low", "year_low", "yearLow"),
                    info.get("fiftyTwoWeekLow") if isinstance(info, dict) else None,
                    min(lows[-252:]) if lows else None,
                    min(closes[-252:]) if closes else None,
                )

                volume = _first_number(
                    _get_attr(fast_info, "last_volume", "lastVolume", "regularMarketVolume", "volume"),
                    info.get("regularMarketVolume") if isinstance(info, dict) else None,
                    info.get("volume") if isinstance(info, dict) else None,
                    meta.get("regularMarketVolume") if isinstance(meta, dict) else None,
                    _row_get(last_hist_row, "Volume"),
                    volumes[-1] if volumes else None,
                )

                avg_volume_10d = _first_number(
                    info.get("averageDailyVolume10Day") if isinstance(info, dict) else None,
                    info.get("averageVolume10days") if isinstance(info, dict) else None,
                    info.get("averageVolume10Day") if isinstance(info, dict) else None,
                    _avg_last_n(volumes, 10),
                )

                avg_volume_30d = _first_number(
                    info.get("averageVolume") if isinstance(info, dict) else None,
                    info.get("averageVolume3Month") if isinstance(info, dict) else None,
                    _avg_last_n(volumes, 30),
                )

                market_cap = _first_number(
                    _get_attr(fast_info, "market_cap", "marketCap"),
                    info.get("marketCap") if isinstance(info, dict) else None,
                )

                currency = _infer_currency(symbol, info=info, meta=meta)
                name = _first_str(
                    info.get("shortName") if isinstance(info, dict) else None,
                    info.get("longName") if isinstance(info, dict) else None,
                    meta.get("shortName") if isinstance(meta, dict) else None,
                    meta.get("longName") if isinstance(meta, dict) else None,
                    symbol,
                )
                exchange = _infer_exchange(symbol, info=info, meta=meta)
                asset_class = _infer_asset_class(symbol, info=info, meta=meta)
                country = _infer_country(symbol, info=info)
                sector = _first_str(info.get("sector") if isinstance(info, dict) else None)
                industry = _first_str(info.get("industry") if isinstance(info, dict) else None)

                if price is None:
                    raise ValueError("no_price_from_yahoo")

                out["current_price"] = float(price)
                out["previous_close"] = float(prev_close) if prev_close is not None else None
                out["open_price"] = float(open_price) if open_price is not None else None
                out["day_high"] = float(day_high) if day_high is not None else None
                out["day_low"] = float(day_low) if day_low is not None else None
                out["week_52_high"] = float(w52_high) if w52_high is not None else None
                out["week_52_low"] = float(w52_low) if w52_low is not None else None
                out["volume"] = float(volume) if volume is not None else None
                out["avg_volume_10d"] = float(avg_volume_10d) if avg_volume_10d is not None else None
                out["avg_volume_30d"] = float(avg_volume_30d) if avg_volume_30d is not None else None
                out["market_cap"] = float(market_cap) if market_cap is not None else None
                out["currency"] = currency
                out["name"] = name
                out["exchange"] = exchange
                out["asset_class"] = asset_class
                out["country"] = country
                out["sector"] = sector
                out["industry"] = industry

                if out.get("previous_close") not in (None, 0.0):
                    chg = out["current_price"] - float(out["previous_close"])
                    out["price_change"] = float(chg)
                    out["percent_change"] = float((chg / float(out["previous_close"])) * 100.0)

                if (
                    out.get("week_52_high") is not None
                    and out.get("week_52_low") is not None
                    and out["week_52_high"] != out["week_52_low"]
                ):
                    out["week_52_position_pct"] = float(
                        ((out["current_price"] - out["week_52_low"]) / (out["week_52_high"] - out["week_52_low"])) * 100.0
                    )

                # Light fundamentals / profile from Yahoo info when available.
                if isinstance(info, dict):
                    out["float_shares"] = _first_number(info.get("floatShares"), info.get("sharesOutstanding"))
                    out["beta_5y"] = _first_number(info.get("beta"), info.get("beta3Year"))
                    out["pe_ttm"] = _first_number(info.get("trailingPE"), info.get("currentPrice") and info.get("trailingPE"))
                    out["pe_forward"] = _first_number(info.get("forwardPE"))
                    out["eps_ttm"] = _first_number(info.get("trailingEps"), info.get("epsTrailingTwelveMonths"))
                    out["dividend_yield"] = _first_percentish(info.get("dividendYield"), info.get("trailingAnnualDividendYield"))
                    out["payout_ratio"] = _first_percentish(info.get("payoutRatio"))
                    out["pb_ratio"] = _first_number(info.get("priceToBook"))
                    out["ps_ratio"] = _first_number(info.get("priceToSalesTrailing12Months"))
                    out["peg_ratio"] = _first_number(info.get("pegRatio"))
                    out["ev_ebitda"] = _first_number(info.get("enterpriseToEbitda"))
                    out["revenue_ttm"] = _first_number(info.get("totalRevenue"), info.get("revenuePerShare"))
                    out["revenue_growth_yoy"] = _first_percentish(info.get("revenueGrowth"))
                    out["gross_margin"] = _first_percentish(info.get("grossMargins"))
                    out["operating_margin"] = _first_percentish(info.get("operatingMargins"))
                    out["profit_margin"] = _first_percentish(info.get("profitMargins"))
                    out["debt_to_equity"] = _first_number(info.get("debtToEquity"))
                    out["free_cash_flow_ttm"] = _first_number(info.get("freeCashflow"), info.get("operatingCashflow"))

                # Indicators + Forecasts + Risk Stats
                if closes:
                    rsi = _rsi_14(closes)
                    if rsi is not None:
                        out["rsi_14"] = float(rsi)

                    vol30 = _volatility_nd(closes, 30)
                    vol90 = _volatility_nd(closes, 90)
                    if vol30 is not None:
                        out["volatility_30d"] = float(vol30)
                    if vol90 is not None:
                        out["volatility_90d"] = float(vol90)

                    dd1y = _max_drawdown_1y(closes)
                    v95 = _var_95_1d(closes)
                    sh = _sharpe_1y(closes)
                    if dd1y is not None:
                        out["max_drawdown_1y"] = float(dd1y)
                    if v95 is not None:
                        out["var_95_1d"] = float(v95)
                    if sh is not None:
                        out["sharpe_1y"] = float(sh)

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

                    conf_candidates = [x for x in [c1, c3, c12] if isinstance(x, (int, float))]
                    conf = max(0.0, min(1.0, float(min(conf_candidates) if conf_candidates else 0.0)))
                    out["forecast_confidence"] = float(conf)

                out["last_updated_utc"] = _utc_iso()
                out["last_updated_riyadh"] = _riyadh_iso()
                if hist_last_dt is not None:
                    out["history_last_utc"] = _utc_iso(hist_last_dt)

                filled = 0
                for k in (
                    "current_price",
                    "previous_close",
                    "open_price",
                    "day_high",
                    "day_low",
                    "volume",
                    "week_52_high",
                    "week_52_low",
                    "avg_volume_10d",
                    "avg_volume_30d",
                ):
                    if out.get(k) is not None:
                        filled += 1
                out["data_quality_score"] = float(filled / 10.0)
                out["_fetch_ms"] = int((time.time() - t0) * 1000)

                # Backward-compatible aliases
                out["price"] = out.get("current_price")
                out["prev_close"] = out.get("previous_close")
                out["open"] = out.get("open_price")
                out["change"] = out.get("price_change")
                out["change_pct"] = out.get("percent_change")
                out["52w_high"] = out.get("week_52_high")
                out["52w_low"] = out.get("week_52_low")
                out["avg_volume_10_day"] = out.get("avg_volume_10d")
                out["avg_volume_30_day"] = out.get("avg_volume_30d")

                return clean_dict(out)

            except Exception as e:
                last_err = e
                base = min(5.0, 0.5 * (2**attempt))
                time.sleep(random.uniform(0.0, base))

        return {
            "symbol": symbol,
            "requested_symbol": symbol,
            "error": f"fetch_failed: {str(last_err)}",
            "provider": PROVIDER_NAME,
            "provider_version": PROVIDER_VERSION,
            "data_sources": [PROVIDER_NAME],
        }

    async def fetch_enriched_quote_patch(self, symbol: str, debug: bool = False, *args: Any, **kwargs: Any) -> Dict[str, Any]:
        if not _configured():
            return {
                "symbol": symbol,
                "requested_symbol": symbol,
                "error": "provider_disabled_or_yfinance_missing",
                "provider": PROVIDER_NAME,
                "provider_version": PROVIDER_VERSION,
                "data_sources": [PROVIDER_NAME],
            }

        sym = normalize_symbol(symbol)
        if not sym:
            return {
                "symbol": symbol,
                "requested_symbol": symbol,
                "error": "invalid_symbol",
                "provider": PROVIDER_NAME,
                "provider_version": PROVIDER_VERSION,
                "data_sources": [PROVIDER_NAME],
            }

        if not await self._cb.allow():
            return {
                "symbol": sym,
                "requested_symbol": symbol,
                "error": "circuit_open",
                "provider": PROVIDER_NAME,
                "provider_version": PROVIDER_VERSION,
                "data_sources": [PROVIDER_NAME],
            }

        m = metrics()
        cached = await self.quote_cache.get(sym, kind="quote")
        if cached is not None:
            m.cache_hits_total.labels(symbol=sym).inc()
            return cached
        m.cache_misses_total.labels(symbol=sym).inc()

        async def _do_fetch() -> Dict[str, Any]:
            async with self._sem:
                await self._rate.acquire(1.0)
                t0 = time.time()
                try:
                    loop = asyncio.get_running_loop()
                    if _tracing_enabled():
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
                    m.requests_total.labels(symbol=sym, op="quote", status=status).inc()
                    m.request_duration.labels(symbol=sym, op="quote").observe(max(0.0, time.time() - t0))

                    if ok:
                        await self.quote_cache.set(sym, res, kind="quote", ttl_sec=_quote_ttl_sec())
                    return res
                except Exception as e:
                    await self._cb.record_failure()
                    m.requests_total.labels(symbol=sym, op="quote", status="exception").inc()
                    m.request_duration.labels(symbol=sym, op="quote").observe(max(0.0, time.time() - t0))
                    return {
                        "symbol": sym,
                        "requested_symbol": symbol,
                        "error": f"exception: {e}",
                        "provider": PROVIDER_NAME,
                        "provider_version": PROVIDER_VERSION,
                        "data_sources": [PROVIDER_NAME],
                    }

        return await self.singleflight.run(f"yahoo:quote:{sym}", _do_fetch)

    # compatibility aliases on the class
    async def fetch_quote_patch(self, symbol: str, debug: bool = False, *args: Any, **kwargs: Any) -> Dict[str, Any]:
        return await self.fetch_enriched_quote_patch(symbol, debug=debug, *args, **kwargs)

    async def fetch_quote(self, symbol: str, debug: bool = False, *args: Any, **kwargs: Any) -> Dict[str, Any]:
        return await self.fetch_enriched_quote_patch(symbol, debug=debug, *args, **kwargs)

    async def get_quote(self, symbol: str, debug: bool = False, *args: Any, **kwargs: Any) -> Dict[str, Any]:
        return await self.fetch_enriched_quote_patch(symbol, debug=debug, *args, **kwargs)

    async def fetch_history(self, symbol: str, period: Optional[str] = None, interval: Optional[str] = None) -> List[Dict[str, Any]]:
        if not _configured():
            return []

        sym = normalize_symbol(symbol)
        if not sym:
            return []

        per = (period or _history_period()).strip() or _history_period()
        itv = (interval or _history_interval()).strip() or _history_interval()

        cached = await self.history_cache.get(sym, kind=f"history:{per}:{itv}")
        if isinstance(cached, list):
            return cached

        async def _do_fetch() -> List[Dict[str, Any]]:
            async with self._sem:
                await self._rate.acquire(1.0)
                loop = asyncio.get_running_loop()
                payload = await loop.run_in_executor(_CPU_EXECUTOR, self._blocking_fetch_history, sym, per, itv)
                if isinstance(payload, dict) and isinstance(payload.get("history"), list):
                    rows = [r for r in payload["history"] if isinstance(r, dict) and r.get("close") is not None]
                    await self.history_cache.set(sym, rows, kind=f"history:{per}:{itv}", ttl_sec=_history_ttl_sec())
                    return rows
                return []

        return await self.singleflight.run(f"yahoo:history:{sym}:{per}:{itv}", _do_fetch)

    async def get_history(self, symbol: str, period: Optional[str] = None, interval: Optional[str] = None) -> List[Dict[str, Any]]:
        return await self.fetch_history(symbol, period=period, interval=interval)

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
                    "requested_symbol": s,
                    "error": str(r),
                    "provider": PROVIDER_NAME,
                    "provider_version": PROVIDER_VERSION,
                    "data_sources": [PROVIDER_NAME],
                }
        return out

    async def fetch_prices(self, symbols: List[str], debug: bool = False) -> Dict[str, Dict[str, Any]]:
        return await self.fetch_batch(symbols, debug=debug)

    async def clear_caches(self) -> None:
        await self.quote_cache.clear()
        await self.history_cache.clear()

    async def get_metrics(self) -> Dict[str, Any]:
        return {
            "provider": self.name,
            "version": self.version,
            "quote_cache_size": await self.quote_cache.size(),
            "history_cache_size": await self.history_cache.size(),
            "configured": _configured(),
            "prometheus_enabled": _prom_enabled(),
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


async def fetch_quote_patch(symbol: str, debug: bool = False, *args: Any, **kwargs: Any) -> Dict[str, Any]:
    return await (await aget_provider()).fetch_quote_patch(symbol, debug=debug, *args, **kwargs)


async def fetch_quote(symbol: str, debug: bool = False, *args: Any, **kwargs: Any) -> Dict[str, Any]:
    return await (await aget_provider()).fetch_quote(symbol, debug=debug, *args, **kwargs)


async def get_quote(symbol: str, debug: bool = False, *args: Any, **kwargs: Any) -> Dict[str, Any]:
    return await (await aget_provider()).get_quote(symbol, debug=debug, *args, **kwargs)


async def fetch_batch_patch(symbols: List[str], debug: bool = False) -> Dict[str, Dict[str, Any]]:
    return await (await aget_provider()).fetch_batch(symbols, debug=debug)


async def fetch_prices(symbols: List[str], debug: bool = False) -> Dict[str, Dict[str, Any]]:
    return await (await aget_provider()).fetch_prices(symbols, debug=debug)


async def fetch_history(symbol: str, period: Optional[str] = None, interval: Optional[str] = None) -> List[Dict[str, Any]]:
    return await (await aget_provider()).fetch_history(symbol, period=period, interval=interval)


async def get_history(symbol: str, period: Optional[str] = None, interval: Optional[str] = None) -> List[Dict[str, Any]]:
    return await (await aget_provider()).get_history(symbol, period=period, interval=interval)


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
# Loader-friendly adapter
# =============================================================================
class YahooProviderAdapter:
    async def get_quote(self, symbol: str) -> Dict[str, Any]:
        return await fetch_enriched_quote_patch(symbol)

    async def quote(self, symbol: str) -> Dict[str, Any]:
        return await fetch_enriched_quote_patch(symbol)

    async def fetch_quote(self, symbol: str) -> Dict[str, Any]:
        return await fetch_enriched_quote_patch(symbol)

    async def fetch_quote_patch(self, symbol: str) -> Dict[str, Any]:
        return await fetch_enriched_quote_patch(symbol)

    async def fetch_prices(self, symbols: List[str]) -> Dict[str, Dict[str, Any]]:
        return await fetch_batch_patch(symbols)

    async def history(self, symbol: str, period: Optional[str] = None, interval: Optional[str] = None) -> List[Dict[str, Any]]:
        return await fetch_history(symbol, period=period, interval=interval)

    async def close(self) -> None:
        await aclose_yahoo_client()


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
        h = await fetch_history("AAPL", period="6mo", interval="1d")
        sys.stdout.write(f"history_rows={len(h)}\n")

    asyncio.run(_test())


__all__ = [
    "PROVIDER_NAME",
    "PROVIDER_VERSION",
    "VERSION",
    "YahooChartProvider",
    "aget_provider",
    "fetch_enriched_quote_patch",
    "fetch_quote_patch",
    "fetch_quote",
    "get_quote",
    "fetch_batch_patch",
    "fetch_prices",
    "fetch_history",
    "get_history",
    "get_client_metrics",
    "health_check",
    "clear_caches",
    "aclose_yahoo_client",
    "provider",
    "PROVIDER",
    "Provider",
    "get_provider",
    "build_provider",
    "create_provider",
    "provider_factory",
]
