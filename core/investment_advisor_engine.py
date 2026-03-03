#!/usr/bin/env python3
# core/investment_advisor_engine.py
"""
================================================================================
Investment Advisor Engine — MASTER ORCHESTRATOR — v4.0.0 (PHASE 4 ALIGNED)
================================================================================
Purpose:
- Provide a universal adapter layer so ANY engine implementation (v1/v2/v3) can be
  consumed by core/investment_advisor.py (Phase 4 aligned).

Phase 4 Alignments:
- ✅ KSA_TADAWUL removed completely
- ✅ No startup network I/O (capabilities discovery is introspection-only)
- ✅ Snapshot adapter preserves schema-aware shapes:
    - dict rows (rows: List[Dict])
    - matrix rows + keys (rows_matrix + keys)
    - legacy headers + rows (headers + rows)
- ✅ Safe sync/async bridging via _safe_call (timeouts, no deadlocks)
- ✅ TraceContext fixed (no set_attributes misuse)

Public entry:
- run_investment_advisor(payload: dict, engine: Any=None, ...) -> dict
- EngineSnapshotAdapter (get_cached_sheet_snapshot / get_cached_multi_sheet_snapshots)
================================================================================
"""

from __future__ import annotations

import asyncio
import hashlib
import inspect
import logging
import os
import threading
import time
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, Tuple, Union

# ---------------------------------------------------------------------------
# High-Performance JSON fallback
# ---------------------------------------------------------------------------
try:
    import orjson  # type: ignore

    def json_loads(data: Union[str, bytes]) -> Any:
        if isinstance(data, str):
            data = data.encode("utf-8")
        return orjson.loads(data)

    def json_dumps(obj: Any) -> str:
        return orjson.dumps(obj, default=str).decode("utf-8")

except Exception:
    import json  # type: ignore

    def json_loads(data: Union[str, bytes]) -> Any:
        if isinstance(data, (bytes, bytearray)):
            data = data.decode("utf-8", errors="replace")
        return json.loads(data)

    def json_dumps(obj: Any) -> str:
        return json.dumps(obj, default=str, ensure_ascii=False)

# ---------------------------------------------------------------------------
# Monitoring & Tracing (safe)
# ---------------------------------------------------------------------------
try:
    from prometheus_client import Counter, Histogram  # type: ignore

    _PROMETHEUS_AVAILABLE = True
    advisor_engine_requests = Counter(
        "advisor_engine_requests_total",
        "Total requests to advisor engine adapter",
        ["status"],
    )
    advisor_engine_duration = Histogram(
        "advisor_engine_duration_seconds",
        "Time spent in advisor engine adapter",
        buckets=[0.1, 0.5, 1.0, 2.5, 5.0, 10.0],
    )
except Exception:
    _PROMETHEUS_AVAILABLE = False

    class _DummyMetric:
        def labels(self, *args, **kwargs):  # noqa
            return self

        def inc(self, *args, **kwargs):  # noqa
            return None

        def observe(self, *args, **kwargs):  # noqa
            return None

    advisor_engine_requests = _DummyMetric()
    advisor_engine_duration = _DummyMetric()

try:
    from opentelemetry import trace  # type: ignore
    from opentelemetry.trace import Status, StatusCode  # type: ignore

    _OTEL_AVAILABLE = True
    _TRACER = trace.get_tracer(__name__)
except Exception:
    _OTEL_AVAILABLE = False
    Status = None  # type: ignore
    StatusCode = None  # type: ignore
    _TRACER = None  # type: ignore

_TRACING_ENABLED = str(os.getenv("CORE_TRACING_ENABLED", "")).strip().lower() in {"1", "true", "yes", "y", "on"}

class TraceContext:
    """
    Correct OTel usage:
      tracer.start_as_current_span(...) returns a context manager
      we enter it, set attributes one-by-one, exit closes span
    """
    def __init__(self, name: str, attributes: Optional[Dict[str, Any]] = None):
        self.name = name
        self.attributes = attributes or {}
        self._cm = None
        self._span = None
        self._enabled = bool(_OTEL_AVAILABLE and _TRACING_ENABLED and _TRACER is not None)

    def __enter__(self):
        if not self._enabled:
            return self
        try:
            self._cm = _TRACER.start_as_current_span(self.name)  # type: ignore
            self._span = self._cm.__enter__()
            for k, v in (self.attributes or {}).items():
                try:
                    if self._span is not None and hasattr(self._span, "set_attribute"):
                        self._span.set_attribute(str(k), v)
                except Exception:
                    pass
        except Exception:
            self._cm = None
            self._span = None
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if not self._cm:
            return False
        try:
            if exc_val and self._span is not None and _OTEL_AVAILABLE and Status is not None and StatusCode is not None:
                try:
                    if hasattr(self._span, "record_exception"):
                        self._span.record_exception(exc_val)
                except Exception:
                    pass
                try:
                    if hasattr(self._span, "set_status"):
                        self._span.set_status(Status(StatusCode.ERROR, str(exc_val)))
                except Exception:
                    pass
        finally:
            try:
                self._cm.__exit__(exc_type, exc_val, exc_tb)
            except Exception:
                pass
        return False


# =============================================================================
# Version
# =============================================================================
__version__ = "4.0.0"
ENGINE_VERSION = __version__

logger = logging.getLogger("core.investment_advisor_engine")

# =============================================================================
# Defaults (PHASE 4 aligned)
# =============================================================================
DEFAULT_SOURCES = [
    "Market_Leaders",
    "Global_Markets",
    "Mutual_Funds",
    "Commodities_FX",
    "My_Portfolio",
]

# Fallback headers (match core/investment_advisor.py v4 output)
DEFAULT_HEADERS: List[str] = [
    "Rank",
    "Symbol",
    "Origin",
    "Name",
    "Market",
    "Sector",
    "Currency",
    "Price",
    "Advisor Score",
    "Recommendation",
    "Allocation %",
    "Allocation Amount",
    "Forecast Price (1M)",
    "Expected ROI % (1M)",
    "Forecast Price (3M)",
    "Expected ROI % (3M)",
    "Forecast Price (12M)",
    "Expected ROI % (12M)",
    "Risk Bucket",
    "Confidence Bucket",
    "Liquidity Score",
    "Data Quality",
    "Reason",
    "Last Updated (UTC)",
    "Last Updated (Riyadh)",
]

# Cache TTL defaults (seconds)
CACHE_TTL_DEFAULT = 300  # 5 minutes
CACHE_TTL_SHEET = 600    # 10 minutes

# Method priority groups for engine discovery/calls
METHOD_GROUPS: Dict[str, List[str]] = {
    "snapshot_multi": [
        "get_cached_multi_sheet_snapshots",
        "get_multi_sheet_snapshots",
        "get_sheet_snapshots_batch",
        "fetch_multi_sheet_snapshots",
    ],
    "snapshot_single": [
        "get_cached_sheet_snapshot",
        "get_sheet_snapshot",
        "get_cached_sheet",
        "get_sheet_cached",
    ],
    "legacy_single": [
        "get_cached_sheet_rows",
        "get_sheet_rows_cached",
        "get_sheet_rows",
        "get_sheet",
        "sheet_rows",
        "fetch_sheet",
        "get_rows",
    ],
    "news": [
        "get_news_score",
        "get_cached_news_score",
        "news_get_score",
        "fetch_news_score",
        "get_news_sentiment",
    ],
    "technical": [
        "get_technical_signals",
        "get_cached_technical_signals",
        "fetch_technical_signals",
        "get_technicals",
    ],
    "historical": [
        "get_historical_returns",
        "get_cached_historical_returns",
        "fetch_historical_returns",
        "get_returns",
    ],
}


# =============================================================================
# Enums and Data Classes
# =============================================================================
class AdapterMode(Enum):
    NATIVE = "native"     # Engine already supports snapshot methods
    WRAPPED = "wrapped"   # Engine wrapped with adapter
    LEGACY = "legacy"     # Legacy engine methods only
    STUB = "stub"         # No sheet capability


class CacheStrategy(Enum):
    NONE = "none"
    MEMORY = "memory"
    ENGINE = "engine"


class MethodResult(Enum):
    SUCCESS = "success"
    FAILED = "failed"
    NOT_FOUND = "not_found"
    TIMEOUT = "timeout"
    EXCEPTION = "exception"


@dataclass(slots=True)
class MethodCall:
    method_name: str
    result: MethodResult
    duration_ms: float
    error: Optional[str] = None
    timestamp: datetime = field(default_factory=lambda: datetime.now(timezone.utc))


@dataclass(slots=True)
class EngineCapabilities:
    has_snapshot_multi: bool = False
    has_snapshot_single: bool = False
    has_legacy_methods: bool = False
    has_news: bool = False
    has_technical: bool = False
    has_historical: bool = False
    method_calls: List[MethodCall] = field(default_factory=list)

    def add_call(self, method: str, result: MethodResult, duration_ms: float, error: Optional[str] = None) -> None:
        self.method_calls.append(MethodCall(method_name=method, result=result, duration_ms=duration_ms, error=error))

    @property
    def can_fetch_sheets(self) -> bool:
        return self.has_snapshot_multi or self.has_snapshot_single or self.has_legacy_methods


@dataclass(slots=True)
class SnapshotEntry:
    """
    Cached snapshot entry.
    We store a normalized snapshot dict so we don't lose:
      - keys
      - rows_matrix
      - dict rows
    """
    sheet_name: str
    snapshot: Dict[str, Any]
    cached_at: datetime
    expires_at: datetime
    source_method: str
    last_access: float = field(default_factory=time.time)

    @property
    def is_expired(self) -> bool:
        return datetime.now(timezone.utc) > self.expires_at

    def to_dict(self) -> Dict[str, Any]:
        return {
            "sheet_name": self.sheet_name,
            "cached_at_utc": self.cached_at.isoformat(),
            "expires_at_utc": self.expires_at.isoformat(),
            "source_method": self.source_method,
        }


@dataclass(slots=True)
class EngineMetrics:
    total_requests: int = 0
    cache_hits: int = 0
    cache_misses: int = 0
    method_calls: Dict[str, int] = field(default_factory=dict)
    method_errors: Dict[str, int] = field(default_factory=dict)
    avg_response_time_ms: float = 0.0
    start_time: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    last_request_time: Optional[datetime] = None

    def record_call(self, method: str, duration_ms: float, success: bool = True) -> None:
        self.total_requests += 1
        self.method_calls[method] = self.method_calls.get(method, 0) + 1
        if not success:
            self.method_errors[method] = self.method_errors.get(method, 0) + 1

        # incremental avg over total requests
        n = max(1, self.total_requests)
        self.avg_response_time_ms = ((self.avg_response_time_ms * (n - 1)) + float(duration_ms)) / n
        self.last_request_time = datetime.now(timezone.utc)

    def to_dict(self) -> Dict[str, Any]:
        uptime = (datetime.now(timezone.utc) - self.start_time).total_seconds()
        return {
            "total_requests": self.total_requests,
            "cache_hits": self.cache_hits,
            "cache_misses": self.cache_misses,
            "method_calls": dict(self.method_calls),
            "method_errors": dict(self.method_errors),
            "avg_response_time_ms": round(self.avg_response_time_ms, 2),
            "uptime_seconds": round(uptime, 2),
            "last_request_utc": self.last_request_time.isoformat() if self.last_request_time else None,
        }


# =============================================================================
# Cache Implementation (thread-safe)
# =============================================================================
class SnapshotCache:
    """
    Thread-safe in-memory cache with true LRU eviction.
    """

    def __init__(self, strategy: CacheStrategy = CacheStrategy.MEMORY, default_ttl: int = CACHE_TTL_DEFAULT, max_size: int = 500):
        self.strategy = strategy
        self.default_ttl = int(default_ttl)
        self.max_size = int(max_size)
        self._cache: Dict[str, SnapshotEntry] = {}
        self._lock = threading.RLock()
        self._metrics = EngineMetrics()

    def get(self, sheet_name: str) -> Optional[SnapshotEntry]:
        if self.strategy == CacheStrategy.NONE:
            return None

        with self._lock:
            ent = self._cache.get(sheet_name)
            if ent is None:
                self._metrics.cache_misses += 1
                return None
            if ent.is_expired:
                self._cache.pop(sheet_name, None)
                self._metrics.cache_misses += 1
                return None
            ent.last_access = time.time()
            self._metrics.cache_hits += 1
            return ent

    def set(self, sheet_name: str, snapshot: Dict[str, Any], source_method: str, ttl: Optional[int] = None) -> SnapshotEntry:
        if self.strategy == CacheStrategy.NONE:
            # still return a computed entry (not stored)
            now = datetime.now(timezone.utc)
            return SnapshotEntry(
                sheet_name=sheet_name,
                snapshot=snapshot,
                cached_at=now,
                expires_at=now,
                source_method=source_method,
            )

        with self._lock:
            # LRU eviction
            if len(self._cache) >= self.max_size and sheet_name not in self._cache:
                oldest_key = min(self._cache.items(), key=lambda kv: kv[1].last_access)[0]
                self._cache.pop(oldest_key, None)

            now = datetime.now(timezone.utc)
            ttl_seconds = int(ttl or self.default_ttl)
            ent = SnapshotEntry(
                sheet_name=sheet_name,
                snapshot=snapshot,
                cached_at=now,
                expires_at=now + timedelta(seconds=ttl_seconds),
                source_method=source_method,
                last_access=time.time(),
            )
            self._cache[sheet_name] = ent
            return ent

    def clear(self) -> None:
        with self._lock:
            self._cache.clear()

    def remove(self, sheet_name: str) -> None:
        with self._lock:
            self._cache.pop(sheet_name, None)

    def get_metrics(self) -> Dict[str, Any]:
        with self._lock:
            m = self._metrics.to_dict()
            m.update(
                {
                    "cache_size": len(self._cache),
                    "cache_strategy": self.strategy.value,
                    "cache_ttl_sec": self.default_ttl,
                    "cache_max_size": self.max_size,
                }
            )
            return m


# =============================================================================
# Safe calling helpers
# =============================================================================
def _filter_kwargs_for_callable(fn: Callable, kwargs: Dict[str, Any]) -> Dict[str, Any]:
    if not kwargs:
        return {}
    try:
        sig = inspect.signature(fn)
        params = sig.parameters
        if any(p.kind == inspect.Parameter.VAR_KEYWORD for p in params.values()):
            return kwargs
        allowed = set(params.keys())
        return {k: v for k, v in kwargs.items() if k in allowed}
    except Exception:
        return {}


def _run_coro_in_thread(coro: Any, timeout: float) -> Tuple[Optional[Any], Optional[BaseException], bool]:
    """
    Run coroutine in a separate thread with its own loop.
    Returns (result, error, timed_out)
    """
    box: Dict[str, Any] = {"result": None, "error": None}

    def _worker():
        try:
            box["result"] = asyncio.run(asyncio.wait_for(coro, timeout=timeout))
        except BaseException as e:  # noqa
            box["error"] = e

    t = threading.Thread(target=_worker, daemon=True)
    t.start()
    t.join(timeout + 0.25)
    if t.is_alive():
        return None, None, True
    return box["result"], box["error"], False


def _safe_call(
    engine: Any,
    method_name: str,
    *args: Any,
    timeout: float = 2.0,
    **kwargs: Any,
) -> Tuple[Optional[Any], MethodResult, float, Optional[str]]:
    """
    Synchronous safe-call that supports:
    - sync functions
    - async functions
    - sync functions returning awaitables
    """
    start = time.time()

    if engine is None:
        return None, MethodResult.NOT_FOUND, 0.0, "Engine is None"

    fn = getattr(engine, method_name, None)
    if not callable(fn):
        return None, MethodResult.NOT_FOUND, 0.0, f"Method {method_name} not found"

    kw = _filter_kwargs_for_callable(fn, kwargs)

    try:
        out = fn(*args, **kw)

        if inspect.isawaitable(out):
            # if we're already in a running loop, run in thread to avoid deadlock
            try:
                loop = asyncio.get_running_loop()
                in_loop = loop.is_running()
            except RuntimeError:
                in_loop = False

            if in_loop:
                res, err, timed_out = _run_coro_in_thread(out, timeout)
                dur = (time.time() - start) * 1000
                if timed_out:
                    return None, MethodResult.TIMEOUT, dur, f"Timeout after {timeout}s"
                if err is not None:
                    return None, MethodResult.EXCEPTION, dur, str(err)
                return res, MethodResult.SUCCESS, dur, None

            # no running loop: safe to run directly
            try:
                res = asyncio.run(asyncio.wait_for(out, timeout=timeout))
                dur = (time.time() - start) * 1000
                return res, MethodResult.SUCCESS, dur, None
            except asyncio.TimeoutError:
                dur = (time.time() - start) * 1000
                return None, MethodResult.TIMEOUT, dur, f"Timeout after {timeout}s"

        dur = (time.time() - start) * 1000
        return out, MethodResult.SUCCESS, dur, None

    except asyncio.TimeoutError:
        dur = (time.time() - start) * 1000
        return None, MethodResult.TIMEOUT, dur, f"Timeout after {timeout}s"
    except Exception as e:
        dur = (time.time() - start) * 1000
        return None, MethodResult.EXCEPTION, dur, str(e)


# =============================================================================
# Capability Discovery (INTROSPECTION ONLY — no calls)
# =============================================================================
def _has_any(engine: Any, names: List[str]) -> bool:
    for n in names:
        if callable(getattr(engine, n, None)):
            return True
    return False


def _discover_engine_capabilities(engine: Any) -> EngineCapabilities:
    caps = EngineCapabilities()
    if engine is None:
        return caps

    caps.has_snapshot_multi = _has_any(engine, METHOD_GROUPS["snapshot_multi"])
    caps.has_snapshot_single = _has_any(engine, METHOD_GROUPS["snapshot_single"])
    caps.has_legacy_methods = _has_any(engine, METHOD_GROUPS["legacy_single"])
    caps.has_news = _has_any(engine, METHOD_GROUPS["news"])
    caps.has_technical = _has_any(engine, METHOD_GROUPS["technical"])
    caps.has_historical = _has_any(engine, METHOD_GROUPS["historical"])
    return caps


# =============================================================================
# Snapshot normalization
# =============================================================================
def _normalize_snapshot(sheet_name: str, result: Any, source_method: str) -> Optional[Dict[str, Any]]:
    """
    Accept multiple shapes and output a snapshot dict that preserves schema-aware fields:
      - headers + rows (legacy matrix)
      - keys + rows_matrix
      - rows as list[dict]
    """
    if result is None:
        return None

    # Already a snapshot dict
    if isinstance(result, dict):
        snap = dict(result)

        # common names normalization
        if "rows_matrix" not in snap and isinstance(snap.get("rows"), list) and snap.get("rows") and isinstance(snap["rows"][0], (list, tuple)):
            # rows is matrix
            snap.setdefault("rows_matrix", snap.get("rows"))

        # ensure basics exist
        snap.setdefault("sheet", sheet_name)
        snap.setdefault("source_method", source_method)
        return snap

    # Tuple/list (headers, rows)
    if isinstance(result, (tuple, list)) and len(result) == 2:
        h, r = result[0], result[1]
        if isinstance(h, (list, tuple)) and isinstance(r, (list, tuple)):
            headers = [str(x) for x in h]
            rows = [list(x) if isinstance(x, (list, tuple)) else [x] for x in r]
            return {
                "sheet": sheet_name,
                "headers": headers,
                "rows": rows,
                "rows_matrix": rows,
                "source_method": source_method,
            }

    # Unknown: best effort
    return None


# =============================================================================
# Engine Snapshot Adapter
# =============================================================================
class EngineSnapshotAdapter:
    """
    Universal adapter that makes any engine compatible with core/investment_advisor.py.

    Provides:
      - get_cached_sheet_snapshot(sheet_name) -> dict snapshot
      - get_cached_multi_sheet_snapshots(list) -> {sheet: snapshot}
    """

    def __init__(
        self,
        base_engine: Any,
        cache_strategy: CacheStrategy = CacheStrategy.MEMORY,
        cache_ttl: int = CACHE_TTL_DEFAULT,
        discover: bool = True,
    ):
        self._base = base_engine
        self._cache = SnapshotCache(strategy=cache_strategy, default_ttl=int(cache_ttl))
        self._capabilities = _discover_engine_capabilities(base_engine) if discover else EngineCapabilities()
        self._created_at = datetime.now(timezone.utc)
        self._metrics = EngineMetrics()

        if self._capabilities.has_snapshot_multi or self._capabilities.has_snapshot_single:
            self._mode = AdapterMode.NATIVE
        elif self._capabilities.has_legacy_methods:
            self._mode = AdapterMode.LEGACY
        else:
            self._mode = AdapterMode.STUB

        logger.info(
            "EngineSnapshotAdapter init | mode=%s | multi=%s | single=%s | legacy=%s",
            self._mode.value,
            self._capabilities.has_snapshot_multi,
            self._capabilities.has_snapshot_single,
            self._capabilities.has_legacy_methods,
        )

    def __repr__(self) -> str:
        return f"<EngineSnapshotAdapter mode={self._mode.value} base={type(self._base).__name__}>"

    # -------------------------
    # Snapshot getters
    # -------------------------
    def get_cached_sheet_snapshot(self, sheet_name: str) -> Optional[Dict[str, Any]]:
        start = time.time()

        # cache first
        ent = self._cache.get(sheet_name)
        if ent is not None:
            self._metrics.record_call("cache_hit", 0.0, True)
            snap = dict(ent.snapshot)
            snap.setdefault("cached_at_utc", ent.cached_at.isoformat())
            snap.setdefault("cached", True)
            return snap

        self._metrics.cache_misses += 1

        # native snapshot single
        if self._capabilities.has_snapshot_single:
            for method in METHOD_GROUPS["snapshot_single"]:
                res, status, dur, err = _safe_call(self._base, method, sheet_name, timeout=2.5)
                self._metrics.record_call(method, dur, status == MethodResult.SUCCESS)
                if status == MethodResult.SUCCESS:
                    snap = _normalize_snapshot(sheet_name, res, source_method=method)
                    if snap is not None:
                        self._cache.set(sheet_name, snap, method, ttl=CACHE_TTL_SHEET)
                        snap2 = dict(snap)
                        snap2["cached_at_utc"] = datetime.now(timezone.utc).isoformat()
                        snap2["cached"] = False
                        snap2["adapter_mode"] = self._mode.value
                        return snap2

        # legacy methods: try to produce headers+rows
        if self._capabilities.has_legacy_methods:
            for method in METHOD_GROUPS["legacy_single"]:
                res, status, dur, err = _safe_call(self._base, method, sheet_name, timeout=3.0)
                self._metrics.record_call(method, dur, status == MethodResult.SUCCESS)
                if status == MethodResult.SUCCESS:
                    snap = _normalize_snapshot(sheet_name, res, source_method=method)
                    if snap is not None:
                        self._cache.set(sheet_name, snap, method, ttl=CACHE_TTL_SHEET)
                        snap2 = dict(snap)
                        snap2["cached_at_utc"] = datetime.now(timezone.utc).isoformat()
                        snap2["cached"] = False
                        snap2["adapter_mode"] = "legacy"
                        return snap2

        self._metrics.record_call("get_cached_sheet_snapshot", (time.time() - start) * 1000, False)
        return None

    def get_cached_multi_sheet_snapshots(self, sheet_names: List[str]) -> Dict[str, Dict[str, Any]]:
        start = time.time()
        out: Dict[str, Dict[str, Any]] = {}

        # native multi
        if self._capabilities.has_snapshot_multi:
            for method in METHOD_GROUPS["snapshot_multi"]:
                res, status, dur, err = _safe_call(self._base, method, sheet_names, timeout=4.0)
                self._metrics.record_call(method, dur, status == MethodResult.SUCCESS)
                if status == MethodResult.SUCCESS and isinstance(res, dict):
                    for sheet, snap_obj in res.items():
                        sname = str(sheet)
                        snap = _normalize_snapshot(sname, snap_obj, source_method=method)
                        if snap is None:
                            continue
                        self._cache.set(sname, snap, method, ttl=CACHE_TTL_SHEET)
                        snap2 = dict(snap)
                        snap2["cached_at_utc"] = datetime.now(timezone.utc).isoformat()
                        snap2["cached"] = False
                        snap2["adapter_mode"] = self._mode.value
                        out[sname] = snap2
                    if out:
                        self._metrics.record_call("get_cached_multi_sheet_snapshots", (time.time() - start) * 1000, True)
                        return out

        # fallback single
        for s in sheet_names or []:
            snap = self.get_cached_sheet_snapshot(s)
            if snap is not None:
                out[str(s)] = snap

        self._metrics.record_call("get_cached_multi_sheet_snapshots", (time.time() - start) * 1000, bool(out))
        return out

    # -------------------------
    # Optional signals (pass-through)
    # -------------------------
    def get_news_score(self, symbol: str) -> Optional[float]:
        if not self._capabilities.has_news:
            return None
        for method in METHOD_GROUPS["news"]:
            res, status, dur, err = _safe_call(self._base, method, symbol, timeout=1.5)
            self._metrics.record_call(method, dur, status == MethodResult.SUCCESS)
            if status == MethodResult.SUCCESS:
                try:
                    return float(res)
                except Exception:
                    return None
        return None

    def get_cached_news_score(self, symbol: str) -> Optional[float]:
        return self.get_news_score(symbol)

    def news_get_score(self, symbol: str) -> Optional[float]:
        return self.get_news_score(symbol)

    def get_technical_signals(self, symbol: str) -> Dict[str, Any]:
        if not self._capabilities.has_technical:
            return {}
        for method in METHOD_GROUPS["technical"]:
            res, status, dur, err = _safe_call(self._base, method, symbol, timeout=1.5)
            self._metrics.record_call(method, dur, status == MethodResult.SUCCESS)
            if status == MethodResult.SUCCESS and isinstance(res, dict):
                return res
        return {}

    def get_cached_technical_signals(self, symbol: str) -> Dict[str, Any]:
        return self.get_technical_signals(symbol)

    def get_historical_returns(self, symbol: str, periods: List[str]) -> Dict[str, Optional[float]]:
        if not self._capabilities.has_historical:
            return {}
        for method in METHOD_GROUPS["historical"]:
            res, status, dur, err = _safe_call(self._base, method, symbol, periods, timeout=2.5)
            self._metrics.record_call(method, dur, status == MethodResult.SUCCESS)
            if status == MethodResult.SUCCESS and isinstance(res, dict):
                return {str(k): (float(v) if v is not None else None) for k, v in res.items()}
        return {}

    # -------------------------
    # Manual snapshot set (optional)
    # -------------------------
    def set_cached_sheet_snapshot(self, sheet_name: str, headers: List[Any], rows: List[Any], meta: Dict[str, Any]) -> None:
        snap = {
            "sheet": sheet_name,
            "headers": [str(h) for h in (headers or [])],
            "rows": [list(r) if isinstance(r, (list, tuple)) else [r] for r in (rows or [])],
            "rows_matrix": [list(r) if isinstance(r, (list, tuple)) else [r] for r in (rows or [])],
            "keys": meta.get("keys") if isinstance(meta, dict) else None,
            "source_method": "manual_set",
        }
        self._cache.set(sheet_name, snap, "manual_set", ttl=CACHE_TTL_SHEET)

    def clear_cache(self) -> None:
        self._cache.clear()

    def warm_cache(self, sheet_names: List[str]) -> Dict[str, bool]:
        results: Dict[str, bool] = {}
        for s in sheet_names or []:
            try:
                results[str(s)] = self.get_cached_sheet_snapshot(str(s)) is not None
            except Exception:
                results[str(s)] = False
        return results

    def get_metrics(self) -> Dict[str, Any]:
        return {
            "adapter": {
                "mode": self._mode.value,
                "created_at_utc": self._created_at.isoformat(),
                "uptime_seconds": round((datetime.now(timezone.utc) - self._created_at).total_seconds(), 2),
            },
            "capabilities": {
                "snapshot_multi": self._capabilities.has_snapshot_multi,
                "snapshot_single": self._capabilities.has_snapshot_single,
                "legacy": self._capabilities.has_legacy_methods,
                "news": self._capabilities.has_news,
                "technical": self._capabilities.has_technical,
                "historical": self._capabilities.has_historical,
            },
            "metrics": self._metrics.to_dict(),
            "cache": self._cache.get_metrics(),
        }

    def health_check(self) -> Dict[str, Any]:
        # must not trigger any engine calls
        total_errors = sum(self._metrics.method_errors.values())
        status = "healthy"
        issues: List[str] = []
        if total_errors > 100:
            status = "degraded"
            issues.append(f"High method error count: {total_errors}")
        total_cache = self._metrics.cache_hits + self._metrics.cache_misses
        if total_cache > 20:
            hit_rate = self._metrics.cache_hits / max(1, total_cache)
            if hit_rate < 0.35:
                status = "degraded"
                issues.append(f"Low cache hit rate: {hit_rate:.0%}")
        return {
            "status": status,
            "issues": issues,
            "timestamp_utc": datetime.now(timezone.utc).isoformat(),
            "metrics": self.get_metrics(),
        }


# =============================================================================
# Main Entry Point
# =============================================================================
def run_investment_advisor(
    payload: Dict[str, Any],
    *,
    engine: Any = None,
    cache_strategy: str = "memory",
    cache_ttl: int = CACHE_TTL_DEFAULT,
    debug: bool = False,
) -> Dict[str, Any]:
    """
    Engine-level entry that:
    - wraps engine if snapshot methods are missing
    - delegates to core.investment_advisor.run_investment_advisor(payload, engine=...)
    """
    with TraceContext("run_investment_advisor_engine", {"debug": bool(debug)}):
        start_time = time.time()
        request_id = hashlib.md5(str(time.time()).encode("utf-8")).hexdigest()[:10]

        if debug:
            try:
                logger.setLevel(logging.DEBUG)
            except Exception:
                pass

        response: Dict[str, Any] = {
            "headers": DEFAULT_HEADERS.copy(),
            "rows": [],
            "items": [],
            "meta": {
                "ok": False,
                "engine_version": ENGINE_VERSION,
                "request_id": request_id,
                "error": None,
                "runtime_ms": 0,
            },
        }

        try:
            # cache strategy
            try:
                strat = CacheStrategy(str(cache_strategy or "memory").lower())
            except Exception:
                strat = CacheStrategy.MEMORY

            eng_obj = engine
            adapter_mode = "none"

            if engine is not None:
                has_native = callable(getattr(engine, "get_cached_sheet_snapshot", None)) or callable(
                    getattr(engine, "get_cached_multi_sheet_snapshots", None)
                )
                if not has_native:
                    eng_obj = EngineSnapshotAdapter(engine, cache_strategy=strat, cache_ttl=int(cache_ttl))
                    adapter_mode = "wrapped"
                else:
                    adapter_mode = "native"
            else:
                adapter_mode = "none"

            # import core advisor
            try:
                from core.investment_advisor import run_investment_advisor as core_run  # type: ignore
                from core.investment_advisor import ADVISOR_VERSION as CORE_VERSION  # type: ignore
            except Exception:
                # relative fallback
                from .investment_advisor import run_investment_advisor as core_run  # type: ignore
                from .investment_advisor import ADVISOR_VERSION as CORE_VERSION  # type: ignore

            core_result = core_run(payload or {}, engine=eng_obj)

            if not isinstance(core_result, dict):
                raise ValueError(f"Core advisor returned non-dict: {type(core_result)}")

            headers = core_result.get("headers")
            rows = core_result.get("rows")
            items = core_result.get("items")
            meta = core_result.get("meta")

            if not isinstance(headers, list) or not headers:
                headers = DEFAULT_HEADERS.copy()
            if not isinstance(rows, list):
                rows = []
            if not isinstance(items, list):
                items = []
            if not isinstance(meta, dict):
                meta = {}

            meta.update(
                {
                    "ok": bool(meta.get("ok", True)),
                    "engine_version": ENGINE_VERSION,
                    "core_version": meta.get("core_version") or CORE_VERSION,
                    "adapter_mode": adapter_mode,
                    "cache_strategy": strat.value,
                    "cache_ttl_sec": int(cache_ttl),
                    "request_id": request_id,
                    "runtime_ms_engine_layer": int((time.time() - start_time) * 1000),
                }
            )

            if adapter_mode == "wrapped" and hasattr(eng_obj, "get_metrics"):
                try:
                    meta["adapter_metrics"] = eng_obj.get_metrics()
                except Exception:
                    pass

            response.update({"headers": headers, "rows": rows, "items": items, "meta": meta})

            advisor_engine_requests.labels(status="success").inc()
            advisor_engine_duration.observe(time.time() - start_time)

            return response

        except Exception as e:
            # safe debug traceback without import-time dependency
            if debug:
                try:
                    import traceback  # noqa

                    response["meta"]["traceback"] = traceback.format_exc()
                except Exception:
                    pass

            response["meta"].update(
                {
                    "ok": False,
                    "error": str(e),
                    "runtime_ms": int((time.time() - start_time) * 1000),
                }
            )

            advisor_engine_requests.labels(status="error").inc()
            advisor_engine_duration.observe(time.time() - start_time)
            return response


# =============================================================================
# Compatibility aliases
# =============================================================================
def run_investment_advisor_engine(payload: Dict[str, Any], *, engine: Any = None) -> Dict[str, Any]:
    return run_investment_advisor(payload, engine=engine)


def create_engine_adapter(engine: Any, cache_strategy: str = "memory", cache_ttl: int = CACHE_TTL_DEFAULT) -> EngineSnapshotAdapter:
    try:
        strat = CacheStrategy(str(cache_strategy or "memory").lower())
    except Exception:
        strat = CacheStrategy.MEMORY
    return EngineSnapshotAdapter(engine, cache_strategy=strat, cache_ttl=int(cache_ttl))


# =============================================================================
# Module exports
# =============================================================================
__all__ = [
    "run_investment_advisor",
    "run_investment_advisor_engine",
    "create_engine_adapter",
    "EngineSnapshotAdapter",
    "ENGINE_VERSION",
    "DEFAULT_HEADERS",
    "DEFAULT_SOURCES",
    "CacheStrategy",
    "AdapterMode",
    "SnapshotEntry",
]
