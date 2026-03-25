#!/usr/bin/env python3
"""
core/yahoo_chart_provider.py
===========================================================
YAHOO CHART SHIM — v3.3.0 (SAFE · FAST · FULL COMPATIBILITY)
===========================================================
Emad Bahbah – Production Architecture

Goals
- Zero downtime compatibility layer for legacy imports -> canonical provider
- Render-safe import behavior (no event-loop work at import time)
- Strict hygiene: no stdout helpers except sys.stdout.write in __main__
- Fast hot-path: cached signature inspection + singleflight provider load
- Resiliency: circuit breaker + full-jitter retry
- Observability: OpenTelemetry spans + Prometheus metrics (optional)
- Broader legacy compatibility surface for class-based callers
"""

from __future__ import annotations

import asyncio
import inspect
import logging
import os
import random
import re
import sys
import threading
import time
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from enum import Enum
from functools import lru_cache
from typing import Any, Awaitable, Callable, Dict, List, Optional, Tuple, TypeVar, Union

# =============================================================================
# Versioning / constants
# =============================================================================

SHIM_VERSION = "3.3.0"
VERSION = SHIM_VERSION
PROVIDER_VERSION = SHIM_VERSION
DATA_SOURCE = "yahoo_chart"
CANONICAL_IMPORT_PATHS = (
    "core.providers.yahoo_chart_provider",
    "providers.yahoo_chart_provider",
)
MIN_CANONICAL_VERSION = "0.4.0"

UTC = timezone.utc
RIYADH = timezone(timedelta(hours=3))

_T = TypeVar("_T")

_TRUTHY = {"1", "true", "yes", "y", "on", "t"}
_FALSY = {"0", "false", "no", "n", "off", "f"}


# =============================================================================
# Small helpers
# =============================================================================
def _env_bool(name: str, default: bool = False) -> bool:
    raw = (os.getenv(name) or "").strip().lower()
    if not raw:
        return default
    if raw in _TRUTHY:
        return True
    if raw in _FALSY:
        return False
    return default


def _env_int(name: str, default: int, lo: Optional[int] = None, hi: Optional[int] = None) -> int:
    try:
        value = int(float((os.getenv(name) or str(default)).strip()))
    except Exception:
        value = default
    if lo is not None and value < lo:
        value = lo
    if hi is not None and value > hi:
        value = hi
    return value


def _env_float(name: str, default: float, lo: Optional[float] = None, hi: Optional[float] = None) -> float:
    try:
        value = float((os.getenv(name) or str(default)).strip())
    except Exception:
        value = default
    if lo is not None and value < lo:
        value = lo
    if hi is not None and value > hi:
        value = hi
    return value


def _utc_iso(dt: Optional[datetime] = None) -> str:
    d = dt or datetime.now(UTC)
    if d.tzinfo is None:
        d = d.replace(tzinfo=UTC)
    return d.astimezone(UTC).isoformat()


def _riyadh_iso(dt: Optional[datetime] = None) -> str:
    d = dt or datetime.now(UTC)
    if d.tzinfo is None:
        d = d.replace(tzinfo=UTC)
    return d.astimezone(RIYADH).isoformat()


def _safe_symbol(value: Any) -> str:
    try:
        return str(value or "").strip().upper()
    except Exception:
        return ""


def _nonempty_count(obj: Any) -> int:
    if not isinstance(obj, dict):
        return 0
    total = 0
    for v in obj.values():
        if v in (None, "", [], {}, ()):
            continue
        total += 1
    return total


def _extract_version_parts(value: str) -> Tuple[int, int, int]:
    raw = str(value or "").strip()
    nums = re.findall(r"\d+", raw)
    parts = [int(x) for x in nums[:3]]
    while len(parts) < 3:
        parts.append(0)
    return (parts[0], parts[1], parts[2])


def _version_tuple(value: str) -> Tuple[int, int, int]:
    try:
        return _extract_version_parts(value)
    except Exception:
        return (0, 0, 0)


# =============================================================================
# JSON helpers (optional orjson)
# =============================================================================
try:
    import orjson  # type: ignore

    def json_dumps(v: Any, *, default: Any = str) -> str:
        return orjson.dumps(v, default=default).decode("utf-8")

    def json_loads(v: Union[str, bytes]) -> Any:
        if isinstance(v, str):
            v = v.encode("utf-8")
        return orjson.loads(v)

except Exception:
    import json

    def json_dumps(v: Any, *, default: Any = str) -> str:
        return json.dumps(v, default=default, ensure_ascii=False)

    def json_loads(v: Union[str, bytes]) -> Any:
        if isinstance(v, (bytes, bytearray)):
            v = v.decode("utf-8", errors="replace")
        return json.loads(v)


# =============================================================================
# Logging
# =============================================================================
_LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").strip().upper()
logging.basicConfig(
    level=getattr(logging, _LOG_LEVEL, logging.INFO),
    format="%(asctime)s | %(levelname)8s | %(name)s | %(message)s",
)
logger = logging.getLogger("core.yahoo_chart_shim")


# =============================================================================
# Prometheus (optional)
# =============================================================================
try:
    from prometheus_client import Counter, Gauge, Histogram  # type: ignore

    shim_requests_total = Counter(
        "tfb_yahoo_shim_requests_total",
        "Total requests handled by yahoo shim",
        ["fn", "status"],
    )
    shim_request_seconds = Histogram(
        "tfb_yahoo_shim_request_seconds",
        "Yahoo shim request duration (seconds)",
        ["fn"],
    )
    shim_provider_available = Gauge(
        "tfb_yahoo_shim_provider_available",
        "Canonical provider availability (1/0)",
    )
    shim_cb_state = Gauge(
        "tfb_yahoo_shim_circuit_state",
        "Circuit state (0=closed,1=half,2=open)",
    )
except Exception:
    class _DummyMetric:
        def labels(self, *args, **kwargs):
            return self

        def inc(self, *args, **kwargs):
            return None

        def observe(self, *args, **kwargs):
            return None

        def set(self, *args, **kwargs):
            return None

    shim_requests_total = _DummyMetric()
    shim_request_seconds = _DummyMetric()
    shim_provider_available = _DummyMetric()
    shim_cb_state = _DummyMetric()


# =============================================================================
# OpenTelemetry (optional)
# =============================================================================
try:
    from opentelemetry import trace  # type: ignore
    from opentelemetry.trace import Status, StatusCode  # type: ignore

    _TRACER = trace.get_tracer(__name__)
    _OTEL_AVAILABLE = True
except Exception:
    Status = None  # type: ignore
    StatusCode = None  # type: ignore
    _TRACER = None
    _OTEL_AVAILABLE = False

_TRACING_ENABLED = _env_bool("CORE_TRACING_ENABLED", False)


class TraceContext:
    def __init__(self, name: str, attributes: Optional[Dict[str, Any]] = None):
        self.name = name
        self.attributes = attributes or {}
        self._cm = None
        self._span = None

    def __enter__(self):
        if _OTEL_AVAILABLE and _TRACING_ENABLED and _TRACER is not None:
            try:
                self._cm = _TRACER.start_as_current_span(self.name)
                self._span = self._cm.__enter__()
                for k, v in self.attributes.items():
                    try:
                        self._span.set_attribute(str(k), v)
                    except Exception:
                        pass
            except Exception:
                self._cm = None
                self._span = None
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        try:
            if self._span is not None and exc_val is not None and Status is not None and StatusCode is not None:
                try:
                    self._span.record_exception(exc_val)
                except Exception:
                    pass
                try:
                    self._span.set_status(Status(StatusCode.ERROR, str(exc_val)))
                except Exception:
                    pass
        finally:
            if self._cm is not None:
                try:
                    return self._cm.__exit__(exc_type, exc_val, exc_tb)
                except Exception:
                    return False
        return False

    async def __aenter__(self):
        return self.__enter__()

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        return self.__exit__(exc_type, exc_val, exc_tb)


# =============================================================================
# Data Quality
# =============================================================================
class DataQuality(str, Enum):
    EXCELLENT = "EXCELLENT"
    HIGH = "HIGH"
    MEDIUM = "MEDIUM"
    LOW = "LOW"
    STALE = "STALE"
    OK = "OK"
    PARTIAL = "PARTIAL"
    MISSING = "MISSING"
    ERROR = "ERROR"


# =============================================================================
# Telemetry (in-memory, thread-safe)
# =============================================================================
@dataclass(slots=True)
class CallMetrics:
    fn: str
    start_mono: float
    end_mono: float
    ok: bool
    error_type: Optional[str] = None
    duration_ms: float = field(init=False)

    def __post_init__(self):
        self.duration_ms = max(0.0, (self.end_mono - self.start_mono) * 1000.0)


class TelemetryCollector:
    def __init__(self, max_items: int = 1500):
        self._lock = threading.RLock()
        self._max = max(200, int(max_items))
        self._calls: List[CallMetrics] = []

    def record(self, metric: CallMetrics) -> None:
        if not _env_bool("SHIM_YAHOO_TELEMETRY", True):
            return
        with self._lock:
            self._calls.append(metric)
            if len(self._calls) > self._max:
                self._calls = self._calls[-self._max :]

    def snapshot(self) -> Dict[str, Any]:
        with self._lock:
            calls = list(self._calls)

        if not calls:
            return {}

        total = len(calls)
        ok = sum(1 for c in calls if c.ok)
        durations = sorted(c.duration_ms for c in calls)
        p50 = durations[total // 2]
        p95 = durations[min(total - 1, int(total * 0.95))]
        p99 = durations[min(total - 1, int(total * 0.99))]

        by_fn: Dict[str, Dict[str, Any]] = {}
        for c in calls:
            row = by_fn.setdefault(c.fn, {"calls": 0, "ok": 0, "fail": 0, "dur_ms_sum": 0.0})
            row["calls"] += 1
            row["ok"] += 1 if c.ok else 0
            row["fail"] += 0 if c.ok else 1
            row["dur_ms_sum"] += c.duration_ms

        for row in by_fn.values():
            row["avg_duration_ms"] = row["dur_ms_sum"] / max(1, row["calls"])
            row["success_rate"] = row["ok"] / max(1, row["calls"])
            row.pop("dur_ms_sum", None)

        return {
            "total_calls": total,
            "success_rate": ok / max(1, total),
            "avg_duration_ms": sum(durations) / max(1, total),
            "p50_duration_ms": p50,
            "p95_duration_ms": p95,
            "p99_duration_ms": p99,
            "by_fn": by_fn,
        }


_TELEMETRY = TelemetryCollector()


def _track(fn_name: str, start_mono: float, ok: bool, err: Optional[BaseException] = None) -> None:
    _TELEMETRY.record(
        CallMetrics(
            fn=fn_name,
            start_mono=start_mono,
            end_mono=time.monotonic(),
            ok=ok,
            error_type=(err.__class__.__name__ if err else None),
        )
    )


# =============================================================================
# SingleFlight (async)
# =============================================================================
class SingleFlight:
    def __init__(self):
        self._lock = asyncio.Lock()
        self._futs: Dict[str, asyncio.Future] = {}

    async def do(self, key: str, coro_fn: Callable[[], Awaitable[_T]]) -> _T:
        fut: Optional[asyncio.Future] = None
        leader = False

        async with self._lock:
            fut = self._futs.get(key)
            if fut is None:
                fut = asyncio.get_running_loop().create_future()
                self._futs[key] = fut
                leader = True

        if not leader:
            return await fut  # type: ignore[return-value]

        try:
            res = await coro_fn()
            if not fut.done():
                fut.set_result(res)
            return res
        except Exception as exc:
            if not fut.done():
                fut.set_exception(exc)
            raise
        finally:
            async with self._lock:
                self._futs.pop(key, None)


# =============================================================================
# Full Jitter retry (async)
# =============================================================================
class FullJitterBackoff:
    def __init__(self, attempts: int = 2, base: float = 0.4, cap: float = 4.0):
        self.attempts = max(0, int(attempts))
        self.base = max(0.05, float(base))
        self.cap = max(self.base, float(cap))

    async def run(self, fn: Callable[[], Awaitable[_T]]) -> _T:
        last: Optional[BaseException] = None
        total_tries = max(1, self.attempts + 1)

        for i in range(total_tries):
            try:
                return await fn()
            except Exception as exc:
                last = exc
                if i >= total_tries - 1:
                    break
                temp = min(self.cap, self.base * (2 ** i))
                await asyncio.sleep(random.uniform(0.0, temp))

        raise last if last else RuntimeError("retry_exhausted")


# =============================================================================
# Circuit breaker (async)
# =============================================================================
class CircuitState(str, Enum):
    CLOSED = "closed"
    HALF = "half_open"
    OPEN = "open"


class CircuitBreakerOpenError(RuntimeError):
    pass


class CircuitBreaker:
    def __init__(self, threshold: int, timeout_sec: float, half_open_calls: int):
        self.threshold = max(1, int(threshold))
        self.timeout_sec = max(1.0, float(timeout_sec))
        self.half_open_calls = max(1, int(half_open_calls))

        self._lock = asyncio.Lock()
        self.state = CircuitState.CLOSED
        self.failures = 0
        self.opened_mono: Optional[float] = None
        self.half_used = 0
        self.half_success = 0

        shim_cb_state.set(0)

    async def allow(self) -> None:
        async with self._lock:
            if self.state == CircuitState.CLOSED:
                shim_cb_state.set(0)
                return

            if self.state == CircuitState.OPEN:
                if self.opened_mono is None:
                    self.opened_mono = time.monotonic()
                if (time.monotonic() - self.opened_mono) >= self.timeout_sec:
                    self.state = CircuitState.HALF
                    self.half_used = 0
                    self.half_success = 0
                    shim_cb_state.set(1)
                    return
                shim_cb_state.set(2)
                raise CircuitBreakerOpenError("circuit_open")

            if self.state == CircuitState.HALF:
                shim_cb_state.set(1)
                if self.half_used >= self.half_open_calls:
                    raise CircuitBreakerOpenError("circuit_half_open_limit")
                self.half_used += 1

    async def on_ok(self) -> None:
        async with self._lock:
            if self.state == CircuitState.HALF:
                self.half_success += 1
                if self.half_success >= 2:
                    self.state = CircuitState.CLOSED
                    self.failures = 0
                    self.opened_mono = None
                    shim_cb_state.set(0)
            else:
                self.failures = 0

    async def on_fail(self) -> None:
        async with self._lock:
            if self.state == CircuitState.HALF:
                self.state = CircuitState.OPEN
                self.opened_mono = time.monotonic()
                self.failures = self.threshold
                shim_cb_state.set(2)
                return

            self.failures += 1
            if self.failures >= self.threshold:
                self.state = CircuitState.OPEN
                self.opened_mono = time.monotonic()
                shim_cb_state.set(2)


# =============================================================================
# Canonical provider cache (TTL + singleflight)
# =============================================================================
@dataclass(slots=True)
class ProviderInfo:
    module: Any
    version: str
    funcs: Dict[str, Callable[..., Any]]
    available: bool
    checked_mono: float
    origin_path: Optional[str] = None
    error: Optional[str] = None


class ProviderCache:
    def __init__(self):
        self.ttl = _env_float("SHIM_YAHOO_PROVIDER_TTL_SEC", 300.0, lo=10.0, hi=86400.0)
        self._lock = asyncio.Lock()
        self._info: Optional[ProviderInfo] = None
        self._sf = SingleFlight()
        self._cb = CircuitBreaker(
            threshold=_env_int("SHIM_YAHOO_CB_THRESHOLD", 3, lo=1, hi=20),
            timeout_sec=_env_float("SHIM_YAHOO_CB_TIMEOUT_SEC", 60.0, lo=5.0, hi=600.0),
            half_open_calls=_env_int("SHIM_YAHOO_CB_HALF_CALLS", 2, lo=1, hi=10),
        )

    def _valid(self) -> bool:
        return self._info is not None and (time.monotonic() - self._info.checked_mono) < self.ttl

    async def get(self) -> ProviderInfo:
        async with self._lock:
            if self._valid():
                shim_provider_available.set(1 if self._info and self._info.available else 0)
                return self._info  # type: ignore[return-value]

        info = await self._sf.do("provider_load", self._load)
        async with self._lock:
            self._info = info
        shim_provider_available.set(1 if info.available else 0)
        return info

    async def _load(self) -> ProviderInfo:
        async def _do_import() -> ProviderInfo:
            await self._cb.allow()
            try:
                mod = None
                last_err = None
                origin_path = None
                for path in CANONICAL_IMPORT_PATHS:
                    try:
                        mod = __import__(path, fromlist=["__all__"])
                        origin_path = path
                        last_err = None
                        break
                    except Exception as exc:
                        last_err = exc
                        continue

                if mod is None:
                    raise ImportError(f"canonical_missing:{last_err.__class__.__name__ if last_err else 'unknown'}")

                ver = str(getattr(mod, "PROVIDER_VERSION", getattr(mod, "VERSION", getattr(mod, "SHIM_VERSION", "unknown"))) or "unknown")
                if _version_tuple(ver) < _version_tuple(MIN_CANONICAL_VERSION):
                    raise RuntimeError(f"canonical_version_too_old:{ver}")

                funcs: Dict[str, Callable[..., Any]] = {}
                for name in dir(mod):
                    if name.startswith("_"):
                        continue
                    try:
                        obj = getattr(mod, name)
                    except Exception:
                        continue
                    if callable(obj):
                        funcs[name] = obj

                await self._cb.on_ok()
                return ProviderInfo(
                    module=mod,
                    version=ver,
                    funcs=funcs,
                    available=True,
                    checked_mono=time.monotonic(),
                    origin_path=origin_path,
                    error=None,
                )
            except Exception as exc:
                await self._cb.on_fail()
                return ProviderInfo(
                    module=None,
                    version="unknown",
                    funcs={},
                    available=False,
                    checked_mono=time.monotonic(),
                    origin_path=None,
                    error=str(exc),
                )

        with TraceContext("yahoo_shim.provider_load"):
            return await _do_import()


_PROVIDER = ProviderCache()


# =============================================================================
# Signature cache
# =============================================================================
@lru_cache(maxsize=256)
def _sig_params(fn: Callable[..., Any]) -> Dict[str, inspect.Parameter]:
    try:
        return dict(inspect.signature(fn).parameters)
    except Exception:
        return {}


def _adapt_kwargs(fn: Callable[..., Any], kwargs: Dict[str, Any]) -> Dict[str, Any]:
    try:
        params = _sig_params(fn)
        if not params:
            return dict(kwargs)
        if any(p.kind == inspect.Parameter.VAR_KEYWORD for p in params.values()):
            return dict(kwargs)
        return {k: v for k, v in kwargs.items() if k in params}
    except Exception:
        return dict(kwargs)


async def _maybe_await(value: Any) -> Any:
    if inspect.isawaitable(value):
        return await value
    return value


# =============================================================================
# Response normalization
# =============================================================================
def _ensure_shape(payload: Any, *, symbol: str, provider_version: Optional[str], fn_name: str) -> Any:
    if not isinstance(payload, dict):
        return payload

    out = dict(payload)
    sym = _safe_symbol(out.get("symbol") or symbol)
    out["symbol"] = sym
    out.setdefault("symbol_normalized", sym)
    out.setdefault("requested_symbol", sym)
    out.setdefault("data_source", DATA_SOURCE)
    out.setdefault("provider", DATA_SOURCE)
    out.setdefault("shim_version", SHIM_VERSION)
    if provider_version:
        out.setdefault("provider_version", provider_version)

    out.setdefault("last_updated_utc", _utc_iso())
    out.setdefault("last_updated_riyadh", _riyadh_iso())

    if out.get("error"):
        out.setdefault("status", "error")
        out.setdefault("data_quality", DataQuality.ERROR.value)
        out.setdefault("where", fn_name)
    else:
        out.setdefault("status", "ok")
        out.setdefault("data_quality", out.get("data_quality") or DataQuality.OK.value)

    return out


def _error_payload(symbol: str, err: str, *, fn_name: str) -> Dict[str, Any]:
    sym = _safe_symbol(symbol)
    return {
        "status": "error",
        "symbol": sym,
        "symbol_normalized": sym,
        "requested_symbol": sym,
        "data_source": DATA_SOURCE,
        "provider": DATA_SOURCE,
        "data_quality": DataQuality.ERROR.value,
        "error": err,
        "where": fn_name,
        "shim_version": SHIM_VERSION,
        "last_updated_utc": _utc_iso(),
        "last_updated_riyadh": _riyadh_iso(),
    }


# =============================================================================
# Default handlers (used only if canonical missing)
# =============================================================================
async def _default_quote(symbol: str, *args, **kwargs) -> Dict[str, Any]:
    sym = _safe_symbol(symbol)
    return {
        "status": "error",
        "symbol": sym,
        "symbol_normalized": sym,
        "requested_symbol": sym,
        "data_source": DATA_SOURCE,
        "provider": DATA_SOURCE,
        "data_quality": DataQuality.MISSING.value,
        "error": "canonical_unavailable",
        "shim_version": SHIM_VERSION,
        "last_updated_utc": _utc_iso(),
        "last_updated_riyadh": _riyadh_iso(),
    }


async def _default_patch(symbol: str, base: Optional[Dict[str, Any]] = None, *args, **kwargs) -> Dict[str, Any]:
    out = dict(base or {})
    out.update(await _default_quote(symbol, *args, **kwargs))
    return out


async def _default_history(*args, **kwargs) -> List[Dict[str, Any]]:
    return []


# =============================================================================
# Shim callable
# =============================================================================
class ShimFunction:
    def __init__(
        self,
        name: str,
        *,
        canonical_name: Optional[str] = None,
        fallback_factory: Optional[Callable[..., Any]] = None,
    ):
        self.name = name
        self.canonical_name = canonical_name or name
        self.fallback_factory = fallback_factory
        self._backoff = FullJitterBackoff(
            attempts=_env_int("SHIM_YAHOO_RETRY_ATTEMPTS", 2, lo=0, hi=6),
            base=_env_float("SHIM_YAHOO_RETRY_BASE_SEC", 0.35, lo=0.05, hi=5.0),
            cap=_env_float("SHIM_YAHOO_RETRY_CAP_SEC", 3.0, lo=0.2, hi=20.0),
        )

    async def __call__(self, *args, **kwargs) -> Any:
        start_mono = time.monotonic()
        sym = _safe_symbol(kwargs.get("symbol") or kwargs.get("ticker") or (args[0] if args else ""))

        async def _call_canonical() -> Any:
            info = await _PROVIDER.get()
            if not info.available:
                raise RuntimeError(info.error or "canonical_unavailable")
            fn = info.funcs.get(self.canonical_name)
            if fn is None:
                raise AttributeError(f"canonical_missing_fn:{self.canonical_name}")
            adapted_kwargs = _adapt_kwargs(fn, kwargs)
            result = fn(*args, **adapted_kwargs)
            result = await _maybe_await(result)
            return _ensure_shape(result, symbol=sym, provider_version=info.version, fn_name=self.name)

        async def _call_fallback() -> Any:
            if self.fallback_factory is None:
                raise RuntimeError("fallback_missing")
            result = self.fallback_factory(*args, **kwargs)
            result = await _maybe_await(result)
            return _ensure_shape(result, symbol=sym, provider_version=None, fn_name=self.name)

        with TraceContext(f"yahoo_shim.{self.name}", {"symbol": sym, "fn": self.name}):
            try:
                t0 = time.monotonic()
                out = await self._backoff.run(_call_canonical)
                shim_requests_total.labels(fn=self.name, status="ok").inc()
                shim_request_seconds.labels(fn=self.name).observe(max(0.0, time.monotonic() - t0))
                _track(self.name, start_mono, True)
                return out
            except Exception as exc:
                shim_requests_total.labels(fn=self.name, status="err").inc()
                _track(self.name, start_mono, False, exc)

                if _env_bool("SHIM_YAHOO_FALLBACK_ON_ERROR", True) and self.fallback_factory is not None:
                    try:
                        t0 = time.monotonic()
                        out = await self._backoff.run(_call_fallback)
                        shim_requests_total.labels(fn=self.name, status="fallback").inc()
                        shim_request_seconds.labels(fn=self.name).observe(max(0.0, time.monotonic() - t0))
                        return out
                    except Exception as fallback_exc:
                        return _error_payload(
                            sym,
                            f"{exc.__class__.__name__}:{exc}; fallback:{fallback_exc.__class__.__name__}:{fallback_exc}",
                            fn_name=self.name,
                        )

                return _error_payload(sym, f"{exc.__class__.__name__}:{exc}", fn_name=self.name)


# =============================================================================
# Public shim callables (legacy API surface)
# =============================================================================
fetch_quote = ShimFunction("fetch_quote", canonical_name="fetch_quote", fallback_factory=_default_quote)
get_quote = ShimFunction("get_quote", canonical_name="get_quote", fallback_factory=_default_quote)

get_quote_patch = ShimFunction("get_quote_patch", canonical_name="get_quote_patch", fallback_factory=_default_patch)
fetch_quote_patch = ShimFunction("fetch_quote_patch", canonical_name="fetch_quote_patch", fallback_factory=_default_patch)
fetch_enriched_quote_patch = ShimFunction("fetch_enriched_quote_patch", canonical_name="fetch_enriched_quote_patch", fallback_factory=_default_patch)
fetch_quote_and_enrichment_patch = ShimFunction(
    "fetch_quote_and_enrichment_patch",
    canonical_name="fetch_quote_and_enrichment_patch",
    fallback_factory=_default_patch,
)
fetch_quote_and_fundamentals_patch = ShimFunction(
    "fetch_quote_and_fundamentals_patch",
    canonical_name="fetch_quote_and_fundamentals_patch",
    fallback_factory=_default_patch,
)

yahoo_chart_quote = ShimFunction("yahoo_chart_quote", canonical_name="yahoo_chart_quote", fallback_factory=_default_quote)

fetch_price_history = ShimFunction("fetch_price_history", canonical_name="fetch_price_history", fallback_factory=_default_history)
fetch_history = ShimFunction("fetch_history", canonical_name="fetch_history", fallback_factory=_default_history)
fetch_ohlc_history = ShimFunction("fetch_ohlc_history", canonical_name="fetch_ohlc_history", fallback_factory=_default_history)
fetch_history_patch = ShimFunction("fetch_history_patch", canonical_name="fetch_history_patch", fallback_factory=_default_history)
fetch_prices = ShimFunction("fetch_prices", canonical_name="fetch_prices", fallback_factory=_default_history)


# =============================================================================
# Class compatibility wrapper
# =============================================================================
class YahooChartProvider:
    """
    Backward compatible provider class.
    Delegates to canonical class if available, otherwise to shim callables.
    """

    def __init__(self, *args, **kwargs):
        self._args = args
        self._kwargs = kwargs
        self._inner = None
        self._lock = asyncio.Lock()

    async def _ensure_inner(self) -> None:
        if self._inner is not None:
            return
        async with self._lock:
            if self._inner is not None:
                return
            info = await _PROVIDER.get()
            if info.available and hasattr(info.module, "YahooChartProvider"):
                try:
                    self._inner = info.module.YahooChartProvider(*self._args, **self._kwargs)
                except Exception:
                    self._inner = None

    async def __aenter__(self):
        await self._ensure_inner()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.aclose()

    async def aclose(self) -> None:
        await self._ensure_inner()
        if self._inner is not None and hasattr(self._inner, "aclose"):
            try:
                value = self._inner.aclose()
                if inspect.isawaitable(value):
                    await value
            except Exception:
                pass
        self._inner = None

    async def _dispatch(self, method_name: str, shim_fn: Callable[..., Awaitable[Any]], *args, **kwargs) -> Any:
        await self._ensure_inner()
        if self._inner is not None and hasattr(self._inner, method_name):
            try:
                value = getattr(self._inner, method_name)(*args, **kwargs)
                value = await _maybe_await(value)
                symbol = _safe_symbol(kwargs.get("symbol") or kwargs.get("ticker") or (args[0] if args else ""))
                return _ensure_shape(value, symbol=symbol, provider_version=None, fn_name=method_name)
            except Exception:
                pass
        return await shim_fn(*args, **kwargs)

    async def fetch_quote(self, symbol: str, *args, **kwargs) -> Dict[str, Any]:
        return await self._dispatch("fetch_quote", fetch_quote, symbol, *args, **kwargs)

    async def get_quote(self, symbol: str, *args, **kwargs) -> Dict[str, Any]:
        return await self._dispatch("get_quote", get_quote, symbol, *args, **kwargs)

    async def get_quote_patch(self, symbol: str, base: Optional[Dict[str, Any]] = None, *args, **kwargs) -> Dict[str, Any]:
        return await self._dispatch("get_quote_patch", get_quote_patch, symbol, base, *args, **kwargs)

    async def fetch_quote_patch(self, symbol: str, base: Optional[Dict[str, Any]] = None, *args, **kwargs) -> Dict[str, Any]:
        return await self._dispatch("fetch_quote_patch", fetch_quote_patch, symbol, base, *args, **kwargs)

    async def fetch_enriched_quote_patch(self, symbol: str, base: Optional[Dict[str, Any]] = None, *args, **kwargs) -> Dict[str, Any]:
        return await self._dispatch("fetch_enriched_quote_patch", fetch_enriched_quote_patch, symbol, base, *args, **kwargs)

    async def fetch_quote_and_enrichment_patch(self, symbol: str, base: Optional[Dict[str, Any]] = None, *args, **kwargs) -> Dict[str, Any]:
        return await self._dispatch("fetch_quote_and_enrichment_patch", fetch_quote_and_enrichment_patch, symbol, base, *args, **kwargs)

    async def fetch_quote_and_fundamentals_patch(self, symbol: str, base: Optional[Dict[str, Any]] = None, *args, **kwargs) -> Dict[str, Any]:
        return await self._dispatch("fetch_quote_and_fundamentals_patch", fetch_quote_and_fundamentals_patch, symbol, base, *args, **kwargs)

    async def fetch_price_history(self, *args, **kwargs) -> List[Dict[str, Any]]:
        return await self._dispatch("fetch_price_history", fetch_price_history, *args, **kwargs)

    async def fetch_history(self, *args, **kwargs) -> List[Dict[str, Any]]:
        return await self._dispatch("fetch_history", fetch_history, *args, **kwargs)

    async def fetch_ohlc_history(self, *args, **kwargs) -> List[Dict[str, Any]]:
        return await self._dispatch("fetch_ohlc_history", fetch_ohlc_history, *args, **kwargs)

    async def fetch_history_patch(self, *args, **kwargs) -> List[Dict[str, Any]]:
        return await self._dispatch("fetch_history_patch", fetch_history_patch, *args, **kwargs)

    async def fetch_prices(self, *args, **kwargs) -> List[Dict[str, Any]]:
        return await self._dispatch("fetch_prices", fetch_prices, *args, **kwargs)


# =============================================================================
# Utilities
# =============================================================================
async def get_provider_status() -> Dict[str, Any]:
    info = await _PROVIDER.get()
    return {
        "shim_version": SHIM_VERSION,
        "provider_version": PROVIDER_VERSION,
        "data_source": DATA_SOURCE,
        "canonical_available": bool(info.available),
        "canonical_version": info.version if info.available else None,
        "canonical_origin_path": info.origin_path if info.available else None,
        "canonical_error": info.error if not info.available else None,
        "ttl_sec": _PROVIDER.ttl,
        "telemetry": _TELEMETRY.snapshot(),
        "timestamp_utc": _utc_iso(),
        "timestamp_riyadh": _riyadh_iso(),
    }


async def clear_cache() -> None:
    global _PROVIDER
    _PROVIDER = ProviderCache()
    shim_provider_available.set(0)


def get_version() -> str:
    return SHIM_VERSION


__all__ = [
    "SHIM_VERSION",
    "VERSION",
    "PROVIDER_VERSION",
    "DATA_SOURCE",
    "DataQuality",
    "YahooChartProvider",
    "fetch_quote",
    "get_quote",
    "get_quote_patch",
    "fetch_quote_patch",
    "fetch_enriched_quote_patch",
    "fetch_quote_and_enrichment_patch",
    "fetch_quote_and_fundamentals_patch",
    "yahoo_chart_quote",
    "fetch_price_history",
    "fetch_history",
    "fetch_ohlc_history",
    "fetch_history_patch",
    "fetch_prices",
    "get_provider_status",
    "clear_cache",
    "get_version",
]


if __name__ == "__main__":
    async def _diag() -> None:
        sys.stdout.write("Yahoo shim diagnostics\n")
        sys.stdout.write("=" * 60 + "\n")
        status = await get_provider_status()
        sys.stdout.write(json_dumps(status, default=str) + "\n")
        sys.stdout.write("=" * 60 + "\n")
        res = await fetch_quote(symbol="AAPL")
        sys.stdout.write(json_dumps(res, default=str) + "\n")

    asyncio.run(_diag())
