#!/usr/bin/env python3
# core/providers/yahoo_chart_provider.py
"""
================================================================================
Yahoo Chart Provider (Global + KSA History) -- v6.9.0
================================================================================
QUOTE/HISTORY HARDENED * COMMODITIES/FX FALLBACKS * CANONICAL FIELD ENRICHMENT
FUNDAMENTALS-LIGHT * HISTORY-DERIVED AVG VOLUME / 52W / RISK STATS * IMPORT-SAFE
SCHEMA-ALIGNED * BATCH-CAPABLE

v6.9.0 changes vs v6.8.0 (schema_registry v3.0.0 alignment)
-------------------------------------------------------------
FIX CRITICAL: _percentish() converted fractions -> percent-points (wrong direction).
  Yahoo returns dtype=pct values (dividendYield, grossMargins, etc.) as fractions
  (0.02 = 2%). The old helper multiplied fractions <= 1.0 by 100, turning 0.02
  into 2.0, causing all margin/yield fields to be stored as percent-points instead
  of fractions. Schema dtype=pct expects fractions.
  Fixed: _percentish() removed. New _as_fraction() uses abs(f)>1.5 -> /100 rule.
         _first_percentish() renamed to _first_fraction() with correct logic.

FIX: percent_change stored as fraction (was percent-points).
  Old: (chg / prev) * 100.0 -> 1.42 (1.42% stored as percent-points)
  New: chg / prev -> 0.0142 (1.42% as fraction, dtype=pct)

FIX: week_52_position_pct stored as fraction (was percent-points).
  Old: ((cur-lo)/(hi-lo)) * 100.0
  New: (cur-lo)/(hi-lo)

FIX: _simple_forecast() expected_roi_1m/3m/12m stored as fraction (was pct-points).
  Old: (price/last - 1.0) * 100.0
  New: (price/last) - 1.0

ENH: PROVIDER_BATCH_SUPPORTED = True exported.
ENH: fetch_enriched_quotes_batch() added (module-level).
ENH: __all__ updated with new exports.

Preserved from v6.8.0:
  All quote/history extraction logic, commodity/FX fallbacks.
  All risk stats helpers (volatility, drawdown, VaR, Sharpe -- already correct fractions).
  All async infrastructure (cache, circuit breaker, rate limiter, singleflight).
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
# Prometheus (optional) -- LAZY + DUPLICATE-SAFE
# =============================================================================
try:
    from prometheus_client import Counter, Gauge, Histogram, REGISTRY  # type: ignore
    _HAS_PROM = True
except Exception:
    Counter = Gauge = Histogram = None  # type: ignore
    REGISTRY = None  # type: ignore
    _HAS_PROM = False


class _DummyMetric:
    def __init__(self, *args: Any, **kwargs: Any) -> None: pass
    def labels(self, *args: Any, **kwargs: Any) -> "_DummyMetric": return self
    def inc(self, *args: Any, **kwargs: Any) -> None: return None
    def observe(self, *args: Any, **kwargs: Any) -> None: return None
    def set(self, *args: Any, **kwargs: Any) -> None: return None


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
        requests_total     = _safe_create_metric(Counter,   "yahoo_requests_total",           "Total Yahoo requests",     ["symbol", "op", "status"]),
        request_duration   = _safe_create_metric(Histogram, "yahoo_request_duration_seconds", "Yahoo request duration",   ["symbol", "op"]),
        cache_hits_total   = _safe_create_metric(Counter,   "yahoo_cache_hits_total",         "Yahoo cache hits",         ["symbol"]),
        cache_misses_total = _safe_create_metric(Counter,   "yahoo_cache_misses_total",       "Yahoo cache misses",       ["symbol"]),
        circuit_state      = _safe_create_metric(Gauge,     "yahoo_circuit_state",            "Yahoo circuit breaker",    ["state"]),
    )
    return _METRICS


# =============================================================================
# OpenTelemetry (optional) -- SAFE
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
        async def __aenter__(self): return self
        async def __aexit__(self, exc_type, exc_val, exc_tb): return False


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
# Logging + constants
# =============================================================================
logger = logging.getLogger("core.providers.yahoo_chart_provider")
logger.addHandler(logging.NullHandler())

PROVIDER_NAME            = "yahoo_chart"
PROVIDER_VERSION         = "6.9.0"
VERSION                  = PROVIDER_VERSION
PROVIDER_BATCH_SUPPORTED = True   # ENH v6.9.0

# =============================================================================
# Env helpers (safe)
# =============================================================================
_TRUTHY = {"1", "true", "yes", "y", "on", "t", "enable", "enabled"}
_FALSY  = {"0", "false", "no", "n", "off", "f", "disable", "disabled"}

_KSA_CODE_RE   = re.compile(r"^\d{3,6}$", re.IGNORECASE)
_FX_PAIR_RE    = re.compile(r"^([A-Z]{6})=X$")
_ARABIC_DIGITS = str.maketrans("٠١٢٣٤٥٦٧٨٩", "0123456789")


def _env_str(name: str, default: str = "") -> str:
    v = os.getenv(name)
    if v is None: return default
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
    if not raw:   return default
    if raw in _FALSY:  return False
    if raw in _TRUTHY: return True
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
    if d.tzinfo is None: d = d.replace(tzinfo=timezone.utc)
    return d.astimezone(timezone.utc).isoformat()


def _riyadh_iso(dt: Optional[datetime] = None) -> str:
    d = dt or datetime.now(_RIYADH_TZ)
    if d.tzinfo is None: d = d.replace(tzinfo=_RIYADH_TZ)
    return d.astimezone(_RIYADH_TZ).isoformat()


# =============================================================================
# Safe type helpers
# =============================================================================

def safe_str(x: Any) -> Optional[str]:
    if x is None: return None
    s = str(x).strip()
    return s if s else None


def safe_float(x: Any) -> Optional[float]:
    try:
        if x is None: return None
        if isinstance(x, (int, float)) and not isinstance(x, bool):
            f = float(x)
            if math.isnan(f) or math.isinf(f): return None
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
        if math.isnan(f) or math.isinf(f): return None
        return f
    except Exception:
        return None


def _as_fraction(x: Any) -> Optional[float]:
    """
    FIX v6.9.0: Normalise any percent-like value to a fraction (0.0142 = 1.42%).
    Yahoo returns dtype=pct fields (dividendYield, grossMargins, etc.) as fractions
    (0.02 = 2%). The old _percentish() helper wrongly multiplied these by 100.
    Rule: abs(f) > 1.5 -> divide by 100. Else keep as-is (already a fraction).
    """
    f = safe_float(x)
    if f is None:
        return None
    if abs(f) > 1.5:
        return f / 100.0
    return f


def _first_number(*values: Any) -> Optional[float]:
    for v in values:
        f = safe_float(v)
        if f is not None:
            return f
    return None


def _first_fraction(*values: Any) -> Optional[float]:
    """
    FIX v6.9.0: Return the first non-None value as a fraction.
    Replaces _first_percentish() which was converting fractions to percent-points.
    """
    for v in values:
        f = _as_fraction(v)
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
    if not s: return ""
    s = s.translate(_ARABIC_DIGITS).strip().upper()
    if _KSA_CODE_RE.match(s):
        return f"{s}.SR"
    return s


def clean_dict(d: Dict[str, Any]) -> Dict[str, Any]:
    out: Dict[str, Any] = {}
    for k, v in (d or {}).items():
        if v is None: continue
        if isinstance(v, str) and not v.strip(): continue
        if isinstance(v, dict):
            vv = clean_dict(v)
            if vv: out[k] = vv
        elif isinstance(v, (list, tuple)):
            lst = []
            for item in v:
                if isinstance(item, dict):
                    it = clean_dict(item)
                    if it: lst.append(it)
                elif item is not None: lst.append(item)
            if lst: out[k] = lst
        else:
            out[k] = v
    return out


# =============================================================================
# Rate limiter (token bucket)
# =============================================================================
@dataclass(slots=True)
class TokenBucket:
    rate_per_sec: float
    burst:        float
    tokens:       float = 0.0
    last:         float = 0.0
    lock:         asyncio.Lock = field(default_factory=asyncio.Lock)

    async def acquire(self, n: float = 1.0) -> None:
        if self.rate_per_sec <= 0: return
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
    fail_threshold:    int
    cooldown_sec:      float
    success_threshold: int
    failures:          int   = 0
    successes:         int   = 0
    opened_at:         float = 0.0
    state:             str   = "closed"
    lock:              asyncio.Lock = field(default_factory=asyncio.Lock)

    async def allow(self) -> bool:
        m = metrics()
        async with self.lock:
            if self.state == "closed":
                m.circuit_state.labels(state="closed").set(0.0)
                return True
            if self.state == "open":
                if time.monotonic() - self.opened_at >= self.cooldown_sec:
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
        self._lock    = asyncio.Lock()
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
    name:     str
    ttl_sec:  float
    maxsize:  int = 5000
    _data:    Dict[str, Tuple[Any, float]] = field(default_factory=dict)
    _lock:    asyncio.Lock = field(default_factory=asyncio.Lock)

    def _make_key(self, symbol: str, kind: str) -> str:
        return f"{self.name}:{kind}:{symbol}"

    async def get(self, symbol: str, kind: str = "quote") -> Optional[Any]:
        key = self._make_key(symbol, kind)
        now = time.monotonic()
        async with self._lock:
            item = self._data.get(key)
            if not item: return None
            value, exp = item
            if now <= exp: return value
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
# Risk / Stats helpers (all return fractions -- dtype=pct)
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
    if len(logrets) < 2: return None
    try:
        if _HAS_NUMPY and np is not None:
            v = float(np.std(np.asarray(logrets, dtype=float), ddof=1) * math.sqrt(252.0))
            if math.isnan(v) or math.isinf(v): return None
            return v
        std = statistics.stdev(logrets)
        return float(std * math.sqrt(252.0))
    except Exception:
        return None


def _volatility_nd(closes: List[float], days: int) -> Optional[float]:
    """Returns annualized volatility as fraction (0.18 = 18%). dtype=pct correct."""
    if len(closes) < days + 1: return None
    window = closes[-(days + 1):]
    return _annualized_vol_from_logrets(_log_returns(window))


def _max_drawdown_1y(closes: List[float]) -> Optional[float]:
    """Returns fraction (0.25 = 25% drawdown). dtype=pct correct."""
    if len(closes) < 20: return None
    window = closes[-252:] if len(closes) >= 252 else closes[:]
    peak   = None
    max_dd = 0.0
    for p in window:
        if p is None or p <= 0: continue
        if peak is None or p > peak: peak = p
        if peak and peak > 0:
            dd = (p / peak) - 1.0
            if dd < max_dd: max_dd = dd
    return float(abs(max_dd)) if max_dd < 0 else 0.0


def _var_95_1d(closes: List[float]) -> Optional[float]:
    """Returns fraction (0.02 = 2% VaR). dtype=pct correct."""
    rets = _simple_returns(closes)
    if len(rets) < 30: return None
    try:
        if _HAS_NUMPY and np is not None:
            q = float(np.quantile(np.asarray(rets, dtype=float), 0.05))
        else:
            s   = sorted(rets)
            idx = max(0, int(0.05 * (len(s) - 1)))
            q   = float(s[idx])
        return float(abs(q)) if q < 0 else 0.0
    except Exception:
        return None


def _sharpe_1y(closes: List[float]) -> Optional[float]:
    """Returns dimensionless Sharpe ratio (not pct)."""
    rets = _simple_returns(closes)
    if len(rets) < 60: return None
    w = rets[-252:] if len(rets) >= 252 else rets[:]
    if len(w) < 30: return None
    try:
        if _HAS_NUMPY and np is not None:
            arr = np.asarray(w, dtype=float)
            mu  = float(np.mean(arr))
            sd  = float(np.std(arr, ddof=1))
        else:
            mu = float(sum(w) / len(w))
            sd = float(statistics.stdev(w))
        if sd <= 0 or math.isnan(sd) or math.isinf(sd): return None
        return float((mu * 252.0) / (sd * math.sqrt(252.0)))
    except Exception:
        return None


def _rsi_14(closes: List[float]) -> Optional[float]:
    if len(closes) < 15: return None
    try:
        if _HAS_NUMPY and np is not None:
            arr   = np.asarray(closes, dtype=float)
            diff  = np.diff(arr)
            gains  = np.where(diff > 0, diff, 0.0)
            losses = np.where(diff < 0, -diff, 0.0)
            n = 14
            avg_gain = float(gains[:n].mean())
            avg_loss = float(losses[:n].mean())
            for i in range(n, len(gains)):
                avg_gain = (avg_gain * (n - 1) + float(gains[i])) / n
                avg_loss = (avg_loss * (n - 1) + float(losses[i])) / n
            if avg_loss == 0: return 100.0
            rs = avg_gain / avg_loss
            return float(100.0 - (100.0 / (1.0 + rs)))
        n = 14
        diffs  = [closes[i] - closes[i - 1] for i in range(1, len(closes))]
        gains  = [d if d > 0 else 0.0 for d in diffs]
        losses = [-d if d < 0 else 0.0 for d in diffs]
        avg_gain = sum(gains[:n])  / n
        avg_loss = sum(losses[:n]) / n
        for i in range(n, len(gains)):
            avg_gain = (avg_gain * (n - 1) + gains[i])  / n
            avg_loss = (avg_loss * (n - 1) + losses[i]) / n
        if avg_loss == 0: return 100.0
        rs = avg_gain / avg_loss
        return 100.0 - (100.0 / (1.0 + rs))
    except Exception:
        return None


def _simple_forecast(closes: List[float], horizon_days: int) -> Tuple[Optional[float], Optional[float], float]:
    """
    Returns (forecast_price, expected_roi, confidence).
    FIX v6.9.0: expected_roi returned as FRACTION (0.15 = 15%).
    Was: (price/last - 1.0) * 100.0 -> percent-points
    Now: (price/last) - 1.0 -> fraction (dtype=pct)
    """
    if len(closes) < 60 or horizon_days <= 0:
        return None, None, 0.0
    try:
        n = min(len(closes), 252)
        y = closes[-n:]
        if any(p <= 0 for p in y):
            return None, None, 0.0

        if _HAS_NUMPY and np is not None:
            arr  = np.asarray(y, dtype=float)
            x    = np.arange(len(arr), dtype=float)
            logy = np.log(arr)
            slope, intercept = np.polyfit(x, logy, 1)
            pred   = slope * x + intercept
            ss_res = float(np.sum((logy - pred) ** 2))
            ss_tot = float(np.sum((logy - float(np.mean(logy))) ** 2))
            r2     = 1.0 - ss_res / ss_tot if ss_tot > 0 else 0.0
            future_x = float(len(arr) - 1 + horizon_days)
            forecast_price = float(math.exp(float(slope) * future_x + float(intercept)))
        else:
            start = y[0]
            end   = y[-1]
            years = max(1e-6, len(y) / 252.0)
            cagr  = (end / start) ** (1 / years) - 1
            forecast_price = end * ((1 + cagr) ** (horizon_days / 252.0))
            r2 = 0.25

        last = float(closes[-1])
        # FIX v6.9.0: fraction not percent-points (was * 100.0)
        roi_frac = (forecast_price / last) - 1.0

        vol30 = _volatility_nd(closes, 30) or 0.25
        conf  = max(0.05, min(0.95,
            (max(0.0, min(1.0, r2)) * 0.8 + 0.2) *
            (1.0 / (1.0 + max(0.0, vol30 - 0.25)))
        ))
        return forecast_price, roi_frac, conf
    except Exception:
        return None, None, 0.0


# =============================================================================
# Provider implementation
# =============================================================================
_CPU_WORKERS  = max(2, _env_int("YAHOO_THREADPOOL_WORKERS", 6))
_CPU_EXECUTOR = concurrent.futures.ThreadPoolExecutor(max_workers=_CPU_WORKERS, thread_name_prefix="YahooWorker")


def _get_attr(obj: Any, *names: str) -> Any:
    for n in names:
        try:
            if obj is None: continue
            if isinstance(obj, dict) and n in obj: return obj.get(n)
            if hasattr(obj, n): return getattr(obj, n)
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
        if qtu in {"CURRENCY", "FOREX", "FX"}: return "FX"
        if qtu in {"ETF", "MUTUALFUND", "MUTUAL FUND"}: return qtu
        return qtu
    s = symbol.upper()
    if s.endswith(".SR"): return "EQUITY"
    if s.endswith("=F"):  return "FUTURE"
    if s.endswith("=X"):  return "FX"
    if s.startswith("^"): return "INDEX"
    return None


def _infer_exchange(symbol: str, info: Any = None, meta: Optional[Dict[str, Any]] = None) -> Optional[str]:
    if isinstance(info, dict):
        ex = _first_str(info.get("exchange"), info.get("fullExchangeName"), info.get("exchangeName"))
        if ex: return ex
    if isinstance(meta, dict):
        ex = _first_str(meta.get("exchangeName"), meta.get("exchangeTimezoneName"))
        if ex: return ex
    s = symbol.upper()
    if s.endswith(".SR"): return "Tadawul"
    if s
