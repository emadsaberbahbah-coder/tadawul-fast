#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# core/investment_advisor_engine.py
"""
================================================================================
Investment Advisor Engine — MASTER ORCHESTRATOR — v4.2.0 (LIVE MODE FIX)
================================================================================

Fixes "No cached sheet snapshots found" + adds LIVE MODE.

Root cause (your current production error)
- Your DataEngineV2/V5 get_sheet_rows signature is keyword-only:
    get_sheet_rows(*, sheet="Market_Leaders", limit=..., ...)
  But the old adapter called legacy methods positionally:
    engine.get_sheet_rows("Market_Leaders")  -> TypeError -> no snapshots -> advisor empty

What v4.2.0 adds
1) ✅ Robust calling: tries keyword-only and positional signatures safely.
2) ✅ Live mode (AUTO by default):
   - If cached snapshots are missing/empty, it will fetch *live sheet rows* via engine.get_sheet_rows
   - Optionally enrich those sheet rows with engine quotes (engine.get_enriched_quotes_batch / get_quotes_batch)
     to avoid snapshot dependency and ensure Advisor has real data.
3) ✅ Snapshot shapes preserved:
   - rows: List[Dict] (preferred)
   - rows_matrix + keys (matrix support)
   - headers + rows (legacy support)
4) ✅ Startup-safe: no network I/O at import time; all engine calls happen at request time only.
5) ✅ Keeps compatibility exports:
   - run_investment_advisor(payload, engine=...)
   - EngineSnapshotAdapter (now supports live modes)

Advisor data modes
- "snapshot"      : ONLY use engine cached snapshots (old behavior)
- "live_sheet"    : build snapshots from engine.get_sheet_rows
- "live_quotes"   : live_sheet + enrich rows with engine quotes (recommended)
- "auto"          : try snapshot; if empty -> live_quotes; if that fails -> live_sheet

Controls
- payload["data_mode"] or payload["advisor_data_mode"] or payload["mode"] can set it.
- env ADVISOR_DATA_MODE can set global default ("auto" by default).

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
import uuid
from collections import OrderedDict
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, Tuple, Union

# ---------------------------------------------------------------------------
# High-Performance JSON fallback (safe)
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
      tracer.start_as_current_span(...) returns a context manager.
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
__version__ = "4.2.0"
ENGINE_VERSION = __version__

logger = logging.getLogger("core.investment_advisor_engine")

# =============================================================================
# Defaults (PHASE aligned)
# =============================================================================
DEFAULT_SOURCES = [
    "Market_Leaders",
    "Global_Markets",
    "Mutual_Funds",
    "Commodities_FX",
    "My_Portfolio",
]

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

CACHE_TTL_DEFAULT = 300  # 5 minutes
CACHE_TTL_SHEET = 600  # 10 minutes

# Engine discovery
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
    # NOTE: get_sheet_rows is typically keyword-only in your DataEngine
    "sheet_rows": [
        "get_sheet_rows",
        "sheet_rows",
        "build_sheet_rows",
        "get_rows",
        "get_sheet",
        "fetch_sheet",
    ],
    "quotes_batch": [
        "get_enriched_quotes_batch",
        "get_quotes_batch",
        "fetch_quotes_batch",
    ],
    "quote_single": [
        "get_enriched_quote_dict",
        "get_quote_dict",
        "get_enriched_quote",
        "get_quote",
        "fetch_quote",
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
    NATIVE = "native"
    WRAPPED = "wrapped"
    LEGACY = "legacy"
    STUB = "stub"


class CacheStrategy(Enum):
    NONE = "none"
    MEMORY = "memory"
    ENGINE = "engine"  # reserved


class MethodResult(Enum):
    SUCCESS = "success"
    FAILED = "failed"
    NOT_FOUND = "not_found"
    TIMEOUT = "timeout"
    EXCEPTION = "exception"


class AdvisorDataMode(Enum):
    SNAPSHOT = "snapshot"
    LIVE_SHEET = "live_sheet"
    LIVE_QUOTES = "live_quotes"
    AUTO = "auto"


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
    has_sheet_rows: bool = False
    has_quotes_batch: bool = False
    has_quote_single: bool = False
    has_news: bool = False
    has_technical: bool = False
    has_historical: bool = False
    method_calls: List[MethodCall] = field(default_factory=list)

    def add_call(self, method: str, result: MethodResult, duration_ms: float, error: Optional[str] = None) -> None:
        self.method_calls.append(MethodCall(method_name=method, result=result, duration_ms=duration_ms, error=error))

    @property
    def can_fetch_sheets(self) -> bool:
        return self.has_snapshot_multi or self.has_snapshot_single or self.has_sheet_rows


@dataclass(slots=True)
class SnapshotEntry:
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
# Cache Implementation (thread-safe, true LRU)
# =============================================================================
class SnapshotCache:
    def __init__(self, strategy: CacheStrategy = CacheStrategy.MEMORY, default_ttl: int = CACHE_TTL_DEFAULT, max_size: int = 500):
        self.strategy = strategy
        self.default_ttl = int(default_ttl)
        self.max_size = int(max_size)
        self._cache: "OrderedDict[str, SnapshotEntry]" = OrderedDict()
        self._lock = threading.RLock()
        self._metrics = EngineMetrics()

    def get(self, sheet_name: str) -> Optional[SnapshotEntry]:
        if self.strategy == CacheStrategy.NONE:
            return None
        key = str(sheet_name)
        with self._lock:
            ent = self._cache.get(key)
            if ent is None:
                self._metrics.cache_misses += 1
                return None
            if ent.is_expired:
                self._cache.pop(key, None)
                self._metrics.cache_misses += 1
                return None
            ent.last_access = time.time()
            self._cache.move_to_end(key, last=True)
            self._metrics.cache_hits += 1
            return ent

    def set(self, sheet_name: str, snapshot: Dict[str, Any], source_method: str, ttl: Optional[int] = None) -> SnapshotEntry:
        now = datetime.now(timezone.utc)
        if self.strategy == CacheStrategy.NONE:
            return SnapshotEntry(
                sheet_name=str(sheet_name),
                snapshot=snapshot,
                cached_at=now,
                expires_at=now,
                source_method=source_method,
            )
        ttl_seconds = int(ttl or self.default_ttl)
        key = str(sheet_name)
        with self._lock:
            ent = SnapshotEntry(
                sheet_name=key,
                snapshot=snapshot,
                cached_at=now,
                expires_at=now + timedelta(seconds=ttl_seconds),
                source_method=source_method,
                last_access=time.time(),
            )
            self._cache[key] = ent
            self._cache.move_to_end(key, last=True)
            while len(self._cache) > self.max_size:
                self._cache.popitem(last=False)
            return ent

    def clear(self) -> None:
        with self._lock:
            self._cache.clear()

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
# Safe calling helpers (sync/async bridging) + signature variants
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
            try:
                loop = asyncio.get_running_loop()
                in_loop = loop.is_running()
            except RuntimeError:
                in_loop = False

            if in_loop:
                res, err, timed_out = _run_coro_in_thread(out, timeout)
                dur = (time.time() - start) * 1000.0
                if timed_out:
                    return None, MethodResult.TIMEOUT, dur, f"Timeout after {timeout}s"
                if err is not None:
                    return None, MethodResult.EXCEPTION, dur, str(err)
                return res, MethodResult.SUCCESS, dur, None

            try:
                res = asyncio.run(asyncio.wait_for(out, timeout=timeout))
                dur = (time.time() - start) * 1000.0
                return res, MethodResult.SUCCESS, dur, None
            except asyncio.TimeoutError:
                dur = (time.time() - start) * 1000.0
                return None, MethodResult.TIMEOUT, dur, f"Timeout after {timeout}s"

        dur = (time.time() - start) * 1000.0
        return out, MethodResult.SUCCESS, dur, None

    except asyncio.TimeoutError:
        dur = (time.time() - start) * 1000.0
        return None, MethodResult.TIMEOUT, dur, f"Timeout after {timeout}s"
    except Exception as e:
        dur = (time.time() - start) * 1000.0
        return None, MethodResult.EXCEPTION, dur, str(e)


def _safe_call_variants(
    engine: Any,
    method_name: str,
    variants: List[Tuple[Tuple[Any, ...], Dict[str, Any]]],
    *,
    timeout: float,
) -> Tuple[Optional[Any], MethodResult, float, Optional[str], int]:
    """
    Try multiple signatures; return first SUCCESS.
    """
    last_err: Optional[str] = None
    last_status: MethodResult = MethodResult.FAILED
    last_dur: float = 0.0

    for i, (args, kwargs) in enumerate(variants):
        res, status, dur, err = _safe_call(engine, method_name, *args, timeout=timeout, **kwargs)
        if status == MethodResult.SUCCESS:
            return res, status, dur, None, i
        last_err = err
        last_status = status
        last_dur = dur

    return None, last_status, last_dur, last_err or "no_variants_succeeded", -1


# =============================================================================
# Capability discovery (INTROSPECTION ONLY — no calls)
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
    caps.has_sheet_rows = _has_any(engine, METHOD_GROUPS["sheet_rows"])
    caps.has_quotes_batch = _has_any(engine, METHOD_GROUPS["quotes_batch"])
    caps.has_quote_single = _has_any(engine, METHOD_GROUPS["quote_single"])
    caps.has_news = _has_any(engine, METHOD_GROUPS["news"])
    caps.has_technical = _has_any(engine, METHOD_GROUPS["technical"])
    caps.has_historical = _has_any(engine, METHOD_GROUPS["historical"])
    return caps


# =============================================================================
# Snapshot normalization (schema-aware)
# =============================================================================
def _rows_dicts_from_matrix(keys: List[str], rows_matrix: List[List[Any]]) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    if not keys or not isinstance(rows_matrix, list):
        return out
    for r in rows_matrix:
        if not isinstance(r, (list, tuple)):
            continue
        d = {k: (r[i] if i < len(r) else None) for i, k in enumerate(keys)}
        out.append(d)
    return out


def _normalize_sheet_rows_payload(sheet_name: str, payload: Dict[str, Any], source_method: str) -> Optional[Dict[str, Any]]:
    """
    Normalize engine.get_sheet_rows output to a snapshot dict.
    Accepts envelopes like:
      {headers, keys, rows, rows_matrix, meta, ...}
    """
    if not isinstance(payload, dict):
        return None

    headers = payload.get("headers") or []
    keys = payload.get("keys") or []
    rows = payload.get("rows") or []
    rows_matrix = payload.get("rows_matrix")

    # Sometimes engines return rows as matrix but don't use rows_matrix
    if rows_matrix is None and isinstance(rows, list) and rows and isinstance(rows[0], (list, tuple)):
        rows_matrix = rows

    # Ensure keys list
    if not isinstance(keys, list):
        keys = []
    keys = [str(k) for k in keys if str(k).strip()]

    # Ensure headers list
    if not isinstance(headers, list):
        headers = []
    headers = [str(h) for h in headers if str(h).strip()]

    # Prefer dict rows
    rows_dicts: List[Dict[str, Any]] = []
    if isinstance(rows, list) and rows and isinstance(rows[0], dict):
        rows_dicts = [r for r in rows if isinstance(r, dict)]
    elif rows_matrix is not None and isinstance(rows_matrix, list) and keys:
        rows_dicts = _rows_dicts_from_matrix(keys, rows_matrix)  # build dict rows

    snap: Dict[str, Any] = {
        "sheet": sheet_name,
        "source_method": source_method,
    }
    if headers:
        snap["headers"] = headers
    if keys:
        snap["keys"] = keys
    if rows_dicts:
        snap["rows"] = rows_dicts
    elif isinstance(rows_matrix, list):
        snap["rows_matrix"] = rows_matrix
        snap["rows"] = rows_matrix  # keep legacy consumers alive

    # Preserve useful meta
    meta = payload.get("meta")
    if isinstance(meta, dict) and meta:
        snap["meta"] = meta

    # If still empty, return None
    if not snap.get("rows") and not snap.get("rows_matrix"):
        return None
    return snap


def _normalize_snapshot(sheet_name: str, result: Any, source_method: str) -> Optional[Dict[str, Any]]:
    if result is None:
        return None

    if isinstance(result, dict):
        # If it's a sheet_rows payload, normalize it properly
        if "rows" in result or "rows_matrix" in result or "keys" in result or "headers" in result:
            snap2 = _normalize_sheet_rows_payload(sheet_name, result, source_method)
            if snap2 is not None:
                return snap2

        snap = dict(result)
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

    return None


# =============================================================================
# Live enrichment (quotes) helpers
# =============================================================================
def _safe_str(x: Any) -> str:
    try:
        s = str(x).strip()
        return s
    except Exception:
        return ""


def _extract_symbols_from_snapshot(snapshot: Dict[str, Any]) -> List[str]:
    """
    Extract symbols from:
      - rows: List[Dict]  (symbol/ticker/code)
      - rows_matrix + keys
    """
    out: List[str] = []
    if not isinstance(snapshot, dict):
        return out

    # dict rows
    rows = snapshot.get("rows")
    if isinstance(rows, list) and rows and isinstance(rows[0], dict):
        for r in rows:
            if not isinstance(r, dict):
                continue
            s = r.get("symbol") or r.get("ticker") or r.get("code") or r.get("Symbol") or r.get("Ticker") or r.get("Code")
            s = _safe_str(s)
            if s:
                out.append(s)
        return out

    # matrix rows
    keys = snapshot.get("keys") or []
    rows_matrix = snapshot.get("rows_matrix") or snapshot.get("rows")
    if isinstance(keys, list) and isinstance(rows_matrix, list) and keys:
        idx = None
        for i, k in enumerate(keys):
            lk = _safe_str(k).lower().strip()
            if lk in {"symbol", "ticker", "code"}:
                idx = i
                break
        if idx is not None:
            for r in rows_matrix:
                if isinstance(r, (list, tuple)) and idx < len(r):
                    s = _safe_str(r[idx])
                    if s:
                        out.append(s)
    return out


async def _enrich_symbols_best_effort(engine: Any, symbols: List[str]) -> Dict[str, Dict[str, Any]]:
    """
    Fetch live quotes via engine methods (preferred: batch).
    Returns map symbol -> quote dict
    """
    symbols = [s for s in (_safe_str(x) for x in (symbols or [])) if s]
    if not symbols or engine is None:
        return {}

    # Batch methods first
    for m in METHOD_GROUPS["quotes_batch"]:
        fn = getattr(engine, m, None)
        if not callable(fn):
            continue
        # Try common variants
        variants = [
            ((), {"symbols": symbols}),
            ((symbols,), {}),
            ((), {"tickers": symbols}),
        ]
        res, status, dur, err, _ = _safe_call_variants(engine, m, variants, timeout=10.0)
        if status == MethodResult.SUCCESS and isinstance(res, dict):
            # expected: {symbol: dict}
            out: Dict[str, Dict[str, Any]] = {}
            for k, v in res.items():
                if isinstance(v, dict):
                    out[_safe_str(k)] = dict(v)
            if out:
                return out

    # Fallback: per-symbol
    for m in METHOD_GROUPS["quote_single"]:
        fn = getattr(engine, m, None)
        if not callable(fn):
            continue

        async def one(sym: str) -> Tuple[str, Dict[str, Any]]:
            variants = [
                ((sym,), {}),
                ((), {"symbol": sym}),
                ((), {"ticker": sym}),
            ]
            r, st, _dur, _err, _ = _safe_call_variants(engine, m, variants, timeout=6.0)
            if st == MethodResult.SUCCESS:
                if isinstance(r, dict):
                    return sym, dict(r)
                # if model object, try dict-like
                try:
                    md = getattr(r, "model_dump", None)
                    if callable(md):
                        return sym, md(mode="python")  # type: ignore
                except Exception:
                    pass
                try:
                    return sym, dict(getattr(r, "__dict__", {}) or {})
                except Exception:
                    return sym, {}
            return sym, {}

        # Run concurrently (but bounded)
        sem = asyncio.Semaphore(12)

        async def guarded(sym: str):
            async with sem:
                return await one(sym)

        pairs = await asyncio.gather(*(guarded(s) for s in symbols), return_exceptions=True)
        out2: Dict[str, Dict[str, Any]] = {}
        for p in pairs:
            if isinstance(p, tuple) and len(p) == 2 and isinstance(p[1], dict):
                out2[p[0]] = p[1]
        if out2:
            return out2

    return {}


def _merge_quote_into_row(row: Dict[str, Any], quote: Dict[str, Any]) -> Dict[str, Any]:
    """
    Enrichment wins only for missing/blank values (non-destructive by default).
    """
    out = dict(row or {})
    q = quote or {}
    for k, v in q.items():
        if v is None:
            continue
        if k not in out or out.get(k) in (None, "", [], {}):
            out[k] = v
    return out


# =============================================================================
# Engine Advisor Adapter (snapshots + live modes)
# =============================================================================
class EngineSnapshotAdapter:
    """
    Universal adapter that makes any engine compatible with core/investment_advisor.py.

    NEW in v4.2.0:
    - supports live modes (auto/live_sheet/live_quotes)
    - robust signature calling for keyword-only engines (DataEngineV2/V5)
    """

    def __init__(
        self,
        base_engine: Any,
        *,
        cache_strategy: CacheStrategy = CacheStrategy.MEMORY,
        cache_ttl: int = CACHE_TTL_DEFAULT,
        discover: bool = True,
        data_mode: AdvisorDataMode = AdvisorDataMode.AUTO,
    ):
        self._base = base_engine
        self._cache = SnapshotCache(strategy=cache_strategy, default_ttl=int(cache_ttl))
        self._capabilities = _discover_engine_capabilities(base_engine) if discover else EngineCapabilities()
        self._created_at = datetime.now(timezone.utc)
        self._metrics = EngineMetrics()
        self._data_mode = data_mode

        if self._capabilities.has_snapshot_multi or self._capabilities.has_snapshot_single:
            self._mode = AdapterMode.NATIVE
        elif self._capabilities.has_sheet_rows:
            self._mode = AdapterMode.LEGACY
        else:
            self._mode = AdapterMode.STUB

        logger.info(
            "EngineSnapshotAdapter init | mode=%s | data_mode=%s | multi=%s | single=%s | sheet_rows=%s | quotes_batch=%s",
            self._mode.value,
            self._data_mode.value,
            self._capabilities.has_snapshot_multi,
            self._capabilities.has_snapshot_single,
            self._capabilities.has_sheet_rows,
            self._capabilities.has_quotes_batch,
        )

    def __repr__(self) -> str:
        return f"<EngineSnapshotAdapter mode={self._mode.value} data_mode={self._data_mode.value} base={type(self._base).__name__}>"

    def __getattr__(self, name: str) -> Any:
        return getattr(self._base, name)

    def set_data_mode(self, mode: AdvisorDataMode) -> None:
        self._data_mode = mode

    # -------------------------
    # Internal: build snapshot from live sheet rows
    # -------------------------
    def _build_snapshot_from_sheet_rows(self, sheet_name: str) -> Optional[Dict[str, Any]]:
        if not self._capabilities.has_sheet_rows:
            return None

        sname = str(sheet_name)

        # Most important: keyword-only signature for your engine
        body = {"include_matrix": True, "mode": "advisor"}

        variants = [
            ((), {"sheet": sname, "limit": 5000, "offset": 0, "mode": "advisor", "body": body}),
            ((), {"sheet": sname, "limit": 5000, "offset": 0, "body": body}),
            ((), {"sheet": sname}),
            ((), {"page": sname, "limit": 5000, "offset": 0, "mode": "advisor", "body": body}),
            ((sname,), {}),  # legacy positional fallback
        ]

        for method in METHOD_GROUPS["sheet_rows"]:
            res, status, dur, err, _ = _safe_call_variants(self._base, method, variants, timeout=6.0)
            self._metrics.record_call(method, dur, status == MethodResult.SUCCESS)
            if status == MethodResult.SUCCESS:
                snap = _normalize_snapshot(sname, res, source_method=method)
                if snap is not None:
                    return snap

        return None

    # -------------------------
    # Internal: apply live quote enrichment
    # -------------------------
    def _enrich_snapshot_live_quotes(self, snapshot: Dict[str, Any]) -> Dict[str, Any]:
        """
        Enrich rows with quotes so Advisor doesn't depend on cached snapshots content.
        This function is SYNC wrapper that safely runs async enrichment.
        """
        if not isinstance(snapshot, dict):
            return snapshot

        rows = snapshot.get("rows")
        if not (isinstance(rows, list) and rows and isinstance(rows[0], dict)):
            # Only enrich dict rows; if matrix only, keep as-is.
            return snapshot

        symbols = _extract_symbols_from_snapshot(snapshot)
        if not symbols:
            return snapshot

        # run async enrichment safely even if we are inside an event loop
        async def _do() -> Dict[str, Any]:
            quotes = await _enrich_symbols_best_effort(self._base, symbols)
            if not quotes:
                return snapshot

            new_rows: List[Dict[str, Any]] = []
            for r in rows:
                if not isinstance(r, dict):
                    continue
                sym = _safe_str(r.get("symbol") or r.get("ticker") or r.get("code") or "")
                q = quotes.get(sym) or quotes.get(_safe_str(r.get("symbol_normalized") or "")) or {}
                new_rows.append(_merge_quote_into_row(r, q if isinstance(q, dict) else {}))

            snap2 = dict(snapshot)
            snap2["rows"] = new_rows
            snap2.setdefault("meta", {})
            if isinstance(snap2["meta"], dict):
                snap2["meta"]["live_quotes_enriched"] = True
                snap2["meta"]["live_quotes_count"] = len(quotes)
            return snap2

        # execute
        try:
            loop = asyncio.get_running_loop()
            in_loop = loop.is_running()
        except RuntimeError:
            in_loop = False

        if in_loop:
            res, err, timed_out = _run_coro_in_thread(_do(), timeout=12.0)
            if timed_out or err is not None or not isinstance(res, dict):
                return snapshot
            return res

        try:
            return asyncio.run(asyncio.wait_for(_do(), timeout=12.0))
        except Exception:
            return snapshot

    # -------------------------
    # Public: snapshot getters used by core/investment_advisor.py
    # -------------------------
    def get_cached_sheet_snapshot(self, sheet_name: str) -> Optional[Dict[str, Any]]:
        sname = str(sheet_name)

        # 1) cache
        ent = self._cache.get(sname)
        if ent is not None:
            snap = dict(ent.snapshot)
            snap.setdefault("cached_at_utc", ent.cached_at.isoformat())
            snap.setdefault("cached", True)
            snap.setdefault("adapter_mode", self._mode.value)
            snap.setdefault("advisor_data_mode", self._data_mode.value)
            return snap

        self._metrics.cache_misses += 1

        # Helper to store & return
        def _store_and_return(snap: Dict[str, Any], source: str) -> Dict[str, Any]:
            ent2 = self._cache.set(sname, snap, source, ttl=CACHE_TTL_SHEET)
            out = dict(snap)
            out["cached_at_utc"] = ent2.cached_at.isoformat()
            out["cached"] = False
            out["adapter_mode"] = self._mode.value
            out["advisor_data_mode"] = self._data_mode.value
            return out

        # Mode decision
        mode = self._data_mode

        # 2) snapshot-only
        if mode in (AdvisorDataMode.SNAPSHOT, AdvisorDataMode.AUTO) and self._capabilities.has_snapshot_single:
            for method in METHOD_GROUPS["snapshot_single"]:
                variants = [
                    ((sname,), {}),
                    ((), {"sheet": sname}),
                    ((), {"page": sname}),
                    ((), {"sheet_name": sname}),
                ]
                res, status, dur, err, _ = _safe_call_variants(self._base, method, variants, timeout=3.5)
                self._metrics.record_call(method, dur, status == MethodResult.SUCCESS)
                if status == MethodResult.SUCCESS:
                    snap = _normalize_snapshot(sname, res, source_method=method)
                    if snap is not None:
                        if mode == AdvisorDataMode.AUTO:
                            # If snapshot is empty, fall through to live
                            if not snap.get("rows") and not snap.get("rows_matrix"):
                                break
                        return _store_and_return(snap, method)

        # 3) live_sheet / live_quotes / auto fallback
        if mode in (AdvisorDataMode.LIVE_SHEET, AdvisorDataMode.LIVE_QUOTES, AdvisorDataMode.AUTO):
            snap_live = self._build_snapshot_from_sheet_rows(sname)
            if snap_live is not None:
                if mode in (AdvisorDataMode.LIVE_QUOTES, AdvisorDataMode.AUTO):
                    snap_live = self._enrich_snapshot_live_quotes(snap_live)
                return _store_and_return(snap_live, snap_live.get("source_method") or "sheet_rows")

        return None

    def get_cached_multi_sheet_snapshots(self, sheet_names: List[str]) -> Dict[str, Dict[str, Any]]:
        names = [str(x) for x in (sheet_names or []) if str(x).strip()]
        out: Dict[str, Dict[str, Any]] = {}
        if not names:
            return out

        # Try native multi snapshots if allowed
        if self._data_mode in (AdvisorDataMode.SNAPSHOT, AdvisorDataMode.AUTO) and self._capabilities.has_snapshot_multi:
            for method in METHOD_GROUPS["snapshot_multi"]:
                variants = [
                    ((names,), {}),
                    ((), {"sheets": names}),
                    ((), {"pages": names}),
                    ((), {"sheet_names": names}),
                ]
                res, status, dur, err, _ = _safe_call_variants(self._base, method, variants, timeout=6.0)
                self._metrics.record_call(method, dur, status == MethodResult.SUCCESS)
                if status == MethodResult.SUCCESS and isinstance(res, dict):
                    for sheet, snap_obj in res.items():
                        sname = str(sheet)
                        snap = _normalize_snapshot(sname, snap_obj, source_method=method)
                        if snap is None:
                            continue
                        ent2 = self._cache.set(sname, snap, method, ttl=CACHE_TTL_SHEET)
                        snap2 = dict(snap)
                        snap2["cached_at_utc"] = ent2.cached_at.isoformat()
                        snap2["cached"] = False
                        snap2["adapter_mode"] = self._mode.value
                        snap2["advisor_data_mode"] = self._data_mode.value
                        out[sname] = snap2

                    # In AUTO mode: if we got some snapshots but not all, we'll fill the rest live.
                    if out and self._data_mode != AdvisorDataMode.AUTO:
                        return out

        # Fallback: per-sheet getter (handles live modes)
        for s in names:
            snap = self.get_cached_sheet_snapshot(s)
            if snap is not None:
                out[s] = snap

        return out

    # Pass-through helper methods (optional)
    def get_news_score(self, symbol: str) -> Optional[float]:
        if not self._capabilities.has_news:
            return None
        for method in METHOD_GROUPS["news"]:
            res, status, dur, err = _safe_call(self._base, method, symbol, timeout=1.8)
            self._metrics.record_call(method, dur, status == MethodResult.SUCCESS)
            if status == MethodResult.SUCCESS:
                try:
                    return float(res)
                except Exception:
                    return None
        return None

    def get_technical_signals(self, symbol: str) -> Dict[str, Any]:
        if not self._capabilities.has_technical:
            return {}
        for method in METHOD_GROUPS["technical"]:
            res, status, dur, err = _safe_call(self._base, method, symbol, timeout=2.0)
            self._metrics.record_call(method, dur, status == MethodResult.SUCCESS)
            if status == MethodResult.SUCCESS and isinstance(res, dict):
                return res
        return {}

    def get_historical_returns(self, symbol: str, periods: List[str]) -> Dict[str, Optional[float]]:
        if not self._capabilities.has_historical:
            return {}
        for method in METHOD_GROUPS["historical"]:
            res, status, dur, err = _safe_call(self._base, method, symbol, periods, timeout=3.0)
            self._metrics.record_call(method, dur, status == MethodResult.SUCCESS)
            if status == MethodResult.SUCCESS and isinstance(res, dict):
                out: Dict[str, Optional[float]] = {}
                for k, v in res.items():
                    try:
                        out[str(k)] = float(v) if v is not None else None
                    except Exception:
                        out[str(k)] = None
                return out
        return {}

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
                "advisor_data_mode": self._data_mode.value,
                "created_at_utc": self._created_at.isoformat(),
                "uptime_seconds": round((datetime.now(timezone.utc) - self._created_at).total_seconds(), 2),
            },
            "capabilities": {
                "snapshot_multi": self._capabilities.has_snapshot_multi,
                "snapshot_single": self._capabilities.has_snapshot_single,
                "sheet_rows": self._capabilities.has_sheet_rows,
                "quotes_batch": self._capabilities.has_quotes_batch,
                "quote_single": self._capabilities.has_quote_single,
                "news": self._capabilities.has_news,
                "technical": self._capabilities.has_technical,
                "historical": self._capabilities.has_historical,
            },
            "metrics": self._metrics.to_dict(),
            "cache": self._cache.get_metrics(),
        }

    def health_check(self) -> Dict[str, Any]:
        total_errors = sum(self._metrics.method_errors.values())
        status = "healthy"
        issues: List[str] = []

        if self._mode == AdapterMode.STUB:
            status = "unhealthy"
            issues.append("No usable engine methods detected (no snapshots and no sheet_rows).")

        if total_errors > 50:
            status = "degraded"
            issues.append(f"High method error count: {total_errors}")

        return {
            "status": status,
            "issues": issues,
            "timestamp_utc": datetime.now(timezone.utc).isoformat(),
            "metrics": self.get_metrics(),
        }


# =============================================================================
# Main Entry Point
# =============================================================================
def _parse_data_mode(payload: Dict[str, Any]) -> AdvisorDataMode:
    # payload override
    raw = (
        (payload or {}).get("data_mode")
        or (payload or {}).get("advisor_data_mode")
        or (payload or {}).get("mode")
        or ""
    )
    raw = str(raw).strip().lower()

    # env default
    if not raw:
        raw = str(os.getenv("ADVISOR_DATA_MODE", "auto") or "auto").strip().lower()

    mapping = {
        "snapshot": AdvisorDataMode.SNAPSHOT,
        "snapshots": AdvisorDataMode.SNAPSHOT,
        "live": AdvisorDataMode.LIVE_QUOTES,
        "live_quotes": AdvisorDataMode.LIVE_QUOTES,
        "quotes": AdvisorDataMode.LIVE_QUOTES,
        "live_sheet": AdvisorDataMode.LIVE_SHEET,
        "sheet": AdvisorDataMode.LIVE_SHEET,
        "auto": AdvisorDataMode.AUTO,
    }
    return mapping.get(raw, AdvisorDataMode.AUTO)


def run_investment_advisor(
    payload: Dict[str, Any],
    *,
    engine: Any = None,
    cache_strategy: str = "memory",
    cache_ttl: int = CACHE_TTL_DEFAULT,
    debug: bool = False,
) -> Dict[str, Any]:
    """
    Engine-level entry that ALWAYS passes an advisor-compatible engine object to core advisor.

    Key change vs v4.1.0:
    - We wrap the engine with EngineSnapshotAdapter even if it has some snapshot methods,
      because AUTO live fallback is required to prevent empty advisor results.
    """
    with TraceContext("run_investment_advisor_engine", {"debug": bool(debug)}):
        start_time = time.time()
        request_id = uuid.uuid4().hex[:12]

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

            data_mode = _parse_data_mode(payload or {})

            eng_obj = engine
            adapter_mode = "none"

            if engine is not None:
                # Always wrap to ensure live fallback works and keyword-only calls are handled.
                eng_obj = EngineSnapshotAdapter(
                    engine,
                    cache_strategy=strat,
                    cache_ttl=int(cache_ttl),
                    discover=True,
                    data_mode=data_mode,
                )
                adapter_mode = "wrapped"
            else:
                adapter_mode = "none"

            # import core advisor
            try:
                from core.investment_advisor import run_investment_advisor as core_run  # type: ignore
                from core.investment_advisor import ADVISOR_VERSION as CORE_VERSION  # type: ignore
            except Exception:
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

            runtime_ms = int((time.time() - start_time) * 1000)

            meta.update(
                {
                    "ok": bool(meta.get("ok", True)),
                    "engine_version": ENGINE_VERSION,
                    "core_version": meta.get("core_version") or CORE_VERSION,
                    "adapter_mode": adapter_mode,
                    "advisor_data_mode": data_mode.value,
                    "cache_strategy": strat.value,
                    "cache_ttl_sec": int(cache_ttl),
                    "request_id": request_id,
                    "runtime_ms_engine_layer": runtime_ms,
                    "runtime_ms": int(meta.get("runtime_ms") or runtime_ms),
                }
            )

            # Helpful diagnostics when empty
            if adapter_mode == "wrapped" and hasattr(eng_obj, "get_metrics"):
                try:
                    meta["adapter_metrics"] = eng_obj.get_metrics()  # type: ignore[attr-defined]
                except Exception:
                    pass

            response.update({"headers": headers, "rows": rows, "items": items, "meta": meta})

            advisor_engine_requests.labels(status="success").inc()
            advisor_engine_duration.observe(time.time() - start_time)

            return response

        except Exception as e:
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


def create_engine_adapter(
    engine: Any,
    cache_strategy: str = "memory",
    cache_ttl: int = CACHE_TTL_DEFAULT,
    data_mode: str = "auto",
) -> EngineSnapshotAdapter:
    try:
        strat = CacheStrategy(str(cache_strategy or "memory").lower())
    except Exception:
        strat = CacheStrategy.MEMORY
    mode = _parse_data_mode({"data_mode": data_mode})
    return EngineSnapshotAdapter(engine, cache_strategy=strat, cache_ttl=int(cache_ttl), data_mode=mode)


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
    "AdvisorDataMode",
    "SnapshotEntry",
]
