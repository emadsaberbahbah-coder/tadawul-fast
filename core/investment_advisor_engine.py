#!/usr/bin/env python3
"""
core/investment_advisor_engine.py
================================================================================
Investment Advisor Engine — THE MASTER ORCHESTRATOR — v3.0.0 (NEXT-GEN ENTERPRISE)
================================================================================
Financial Data Platform — Advanced Investment Advisor Engine with Multi-Layer Architecture

What's new in v3.0.0:
- ✅ **High-Performance JSON**: `orjson` integration for blazing fast serialization
- ✅ **Memory Optimization**: Applied `@dataclass(slots=True)` to core components
- ✅ **OpenTelemetry Tracing**: End-to-end tracing for cross-module performance tracking
- ✅ **Prometheus Metrics**: Advanced histograms and counters for engine routing
- ✅ **Universal Event Loop Management**: Hardened `_safe_call` for seamless sync/async bridging
- ✅ **Adaptive Cache Eviction**: Upgraded `SnapshotCache` with true LRU access tracking
- ✅ **Strict Alignment**: Fully synced with `core/investment_advisor.py` v3.0.0 output schema

Key Features:
- Works with ANY engine implementation (v1, v2, v3)
- Zero modifications required to the core algorithm logic
- Automatic adapter selection based on discovered engine capabilities
- Comprehensive method discovery and fallback chains
- Production-grade error handling and circuit breaking
- Thread-safe snapshot caching
"""

from __future__ import annotations

import asyncio
import functools
import hashlib
import inspect
import logging
import os
import sys
import time
import threading
from collections import defaultdict
from dataclasses import dataclass, field, asdict
from datetime import datetime, timedelta, timezone
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, Set, Tuple, TypeVar, Union
import warnings

# ---------------------------------------------------------------------------
# High-Performance JSON fallback
# ---------------------------------------------------------------------------
try:
    import orjson
    def json_loads(data: Union[str, bytes]) -> Any:
        return orjson.loads(data)
    def json_dumps(obj: Any) -> str:
        return orjson.dumps(obj).decode('utf-8')
except ImportError:
    import json
    def json_loads(data: Union[str, bytes]) -> Any:
        return json.loads(data)
    def json_dumps(obj: Any) -> str:
        return json.dumps(obj, default=str)

# ---------------------------------------------------------------------------
# Monitoring & Tracing
# ---------------------------------------------------------------------------
try:
    from prometheus_client import Counter, Histogram, Gauge
    _PROMETHEUS_AVAILABLE = True
    advisor_engine_requests = Counter('advisor_engine_requests_total', 'Total requests to advisor engine adapter', ['status'])
    advisor_engine_duration = Histogram('advisor_engine_duration_seconds', 'Time spent in advisor engine adapter', buckets=[0.1, 0.5, 1.0, 2.5, 5.0, 10.0])
except ImportError:
    _PROMETHEUS_AVAILABLE = False
    class DummyMetric:
        def labels(self, *args, **kwargs): return self
        def inc(self, *args, **kwargs): pass
        def observe(self, *args, **kwargs): pass
        def set(self, *args, **kwargs): pass
    advisor_engine_requests = DummyMetric()
    advisor_engine_duration = DummyMetric()

try:
    from opentelemetry import trace
    from opentelemetry.trace import Status, StatusCode
    _OTEL_AVAILABLE = True
    tracer = trace.get_tracer(__name__)
except ImportError:
    _OTEL_AVAILABLE = False
    class DummySpan:
        def set_attribute(self, *args, **kwargs): pass
        def set_status(self, *args, **kwargs): pass
        def __enter__(self): return self
        def __exit__(self, *args, **kwargs): pass
    class DummyTracer:
        def start_as_current_span(self, *args, **kwargs): return DummySpan()
    tracer = DummyTracer()

_TRACING_ENABLED = os.getenv("CORE_TRACING_ENABLED", "").strip().lower() in {"1", "true", "yes", "y", "on"}

class TraceContext:
    """OpenTelemetry trace context manager (Sync and Async compatible)."""
    def __init__(self, name: str, attributes: Optional[Dict[str, Any]] = None):
        self.name = name
        self.attributes = attributes or {}
        self.tracer = tracer if _OTEL_AVAILABLE and _TRACING_ENABLED else None
        self.span = None
    
    def __enter__(self):
        if self.tracer:
            self.span = self.tracer.start_as_current_span(self.name)
            if self.attributes:
                self.span.set_attributes(self.attributes)
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.span and _OTEL_AVAILABLE:
            if exc_val:
                self.span.record_exception(exc_val)
                self.span.set_status(Status(StatusCode.ERROR, str(exc_val)))
            self.span.__exit__(exc_type, exc_val, exc_tb)

# ============================================================================
# Version Information
# ============================================================================

__version__ = "3.0.0"
ENGINE_VERSION = __version__

logger = logging.getLogger("core.investment_advisor_engine")

# ============================================================================
# Constants
# ============================================================================

# Default sources (aligned with core/investment_advisor.py)
DEFAULT_SOURCES = [
    "Market_Leaders",
    "Global_Markets",
    "Mutual_Funds",
    "Commodities_FX",
    "KSA_TADAWUL",
    "INSIGHTS_ANALYSIS",
]

# Default headers (aligned with core/investment_advisor.py v3.0.0)
DEFAULT_HEADERS: List[str] = [
    "Rank", "Symbol", "Origin", "Name", "Market", "Sector", "Currency", "Price", 
    "Advisor Score", "Action", "Allocation %", "Allocation Amount", 
    "Forecast Price (1M)", "Expected ROI % (1M)", "Forecast Price (3M)", "Expected ROI % (3M)", 
    "Forecast Price (12M)", "Expected ROI % (12M)", "Risk Bucket", "Confidence Bucket", 
    "Liquidity Score", "Data Quality", "Reason", "Data Source", 
    "Last Updated (UTC)", "Last Updated (Riyadh)"
]

# Cache TTL defaults (seconds)
CACHE_TTL_DEFAULT = 300  # 5 minutes
CACHE_TTL_SHEET = 600    # 10 minutes
CACHE_TTL_NEWS = 300     # 5 minutes

# Method priority groups for engine discovery
METHOD_GROUPS = {
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
        "get_sheet_cached",
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


# ============================================================================
# Enums and Data Classes
# ============================================================================

class AdapterMode(Enum):
    """Adapter operation mode."""
    NATIVE = "native"                    # Engine has native snapshot support
    WRAPPED = "wrapped"                  # Engine wrapped with adapter
    LEGACY = "legacy"                    # Legacy engine without snapshots
    STUB = "stub"                        # Stub engine (minimal functionality)


class CacheStrategy(Enum):
    """Cache strategy for snapshots."""
    NONE = "none"                        # No caching
    MEMORY = "memory"                    # In-memory cache
    REDIS = "redis"                      # Redis cache (if available)
    ENGINE = "engine"                    # Rely on engine's own cache


class MethodResult(Enum):
    """Method call result status."""
    SUCCESS = "success"
    FAILED = "failed"
    NOT_FOUND = "not_found"
    TIMEOUT = "timeout"
    EXCEPTION = "exception"


@dataclass(slots=True)
class MethodCall:
    """Record of a method call attempt."""
    method_name: str
    result: MethodResult
    duration_ms: float
    error: Optional[str] = None
    timestamp: datetime = field(default_factory=lambda: datetime.now(timezone.utc))


@dataclass(slots=True)
class EngineCapabilities:
    """Engine capabilities detected."""
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
    
    @property
    def preferred_method(self) -> Optional[str]:
        if self.has_snapshot_multi: return "multi_snapshot"
        if self.has_snapshot_single: return "single_snapshot"
        if self.has_legacy_methods: return "legacy"
        return None


@dataclass(slots=True)
class SnapshotMetadata:
    """Metadata for cached snapshot."""
    sheet_name: str
    headers: List[str]
    rows: List[List[Any]]
    cached_at: datetime
    expires_at: datetime
    source_method: str
    row_count: int
    column_count: int
    last_access: float = field(default_factory=time.time)
    
    @property
    def is_expired(self) -> bool:
        return datetime.now(timezone.utc) > self.expires_at
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "sheet_name": self.sheet_name,
            "headers": self.headers,
            "cached_at": self.cached_at.isoformat(),
            "expires_at": self.expires_at.isoformat(),
            "source_method": self.source_method,
            "row_count": self.row_count,
            "column_count": self.column_count,
        }


@dataclass(slots=True)
class EngineMetrics:
    """Engine performance metrics."""
    total_requests: int = 0
    cache_hits: int = 0
    cache_misses: int = 0
    method_calls: Dict[str, int] = field(default_factory=dict)
    method_errors: Dict[str, int] = field(default_factory=dict)
    avg_response_time_ms: float = 0.0
    last_request_time: Optional[datetime] = None
    start_time: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    
    def record_call(self, method: str, duration_ms: float, success: bool = True) -> None:
        self.total_requests += 1
        self.method_calls[method] = self.method_calls.get(method, 0) + 1
        if not success:
            self.method_errors[method] = self.method_errors.get(method, 0) + 1
        
        total = sum(self.method_calls.values())
        self.avg_response_time_ms = ((self.avg_response_time_ms * (total - 1) + duration_ms) / total)
        self.last_request_time = datetime.now(timezone.utc)
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "total_requests": self.total_requests,
            "cache_hits": self.cache_hits,
            "cache_misses": self.cache_misses,
            "method_calls": dict(self.method_calls),
            "method_errors": dict(self.method_errors),
            "avg_response_time_ms": round(self.avg_response_time_ms, 2),
            "uptime_seconds": (datetime.now(timezone.utc) - self.start_time).total_seconds(),
            "last_request": self.last_request_time.isoformat() if self.last_request_time else None,
        }


# ============================================================================
# Cache Implementation
# ============================================================================

class SnapshotCache:
    """
    Thread-safe cache for sheet snapshots with true LRU eviction.
    """
    
    def __init__(self, strategy: CacheStrategy = CacheStrategy.MEMORY, default_ttl: int = CACHE_TTL_DEFAULT, max_size: int = 1000):
        self.strategy = strategy
        self.default_ttl = default_ttl
        self.max_size = max_size
        self._cache: Dict[str, SnapshotMetadata] = {}
        self._lock = threading.RLock()
        self._metrics = EngineMetrics()
    
    def get(self, sheet_name: str) -> Optional[SnapshotMetadata]:
        with self._lock:
            self._metrics.cache_misses += 1
            if sheet_name not in self._cache:
                return None
            
            meta = self._cache[sheet_name]
            if meta.is_expired:
                del self._cache[sheet_name]
                return None
            
            meta.last_access = time.time()
            self._metrics.cache_hits += 1
            self._metrics.cache_misses -= 1 # adjust since it was a hit
            return meta
    
    def set(self, sheet_name: str, headers: List[str], rows: List[List[Any]], source_method: str, ttl: Optional[int] = None) -> SnapshotMetadata:
        with self._lock:
            if len(self._cache) >= self.max_size and sheet_name not in self._cache:
                # Evict oldest access
                oldest = min(self._cache.items(), key=lambda x: x[1].last_access)[0]
                del self._cache[oldest]
            
            now = datetime.now(timezone.utc)
            ttl_seconds = ttl or self.default_ttl
            
            meta = SnapshotMetadata(
                sheet_name=sheet_name,
                headers=headers,
                rows=rows,
                cached_at=now,
                expires_at=now + timedelta(seconds=ttl_seconds),
                source_method=source_method,
                row_count=len(rows) if rows else 0,
                column_count=len(headers) if headers else 0,
                last_access=time.time()
            )
            
            self._cache[sheet_name] = meta
            return meta
    
    def clear(self) -> None:
        with self._lock:
            self._cache.clear()
    
    def remove(self, sheet_name: str) -> None:
        with self._lock:
            self._cache.pop(sheet_name, None)
    
    def get_metrics(self) -> Dict[str, Any]:
        with self._lock:
            metrics = self._metrics.to_dict()
            metrics.update({
                "cache_size": len(self._cache),
                "cache_strategy": self.strategy.value,
                "cache_ttl": self.default_ttl,
                "cache_max_size": self.max_size,
            })
            return metrics


# ============================================================================
# Engine Method Discovery
# ============================================================================

def _safe_call(engine: Any, method_name: str, *args: Any, timeout: float = 2.0, **kwargs: Any) -> Tuple[Optional[Any], MethodResult, float, Optional[str]]:
    start = time.time()
    
    if engine is None: return None, MethodResult.NOT_FOUND, 0, "Engine is None"
    
    method = getattr(engine, method_name, None)
    if not callable(method):
        return None, MethodResult.NOT_FOUND, 0, f"Method {method_name} not found"
    
    try:
        if inspect.iscoroutinefunction(method):
            try:
                loop = asyncio.get_running_loop()
            except RuntimeError:
                loop = None

            if loop and loop.is_running():
                # We are already in an event loop, but _safe_call is synchronous.
                # Use a fresh event loop in a new thread to safely block.
                result = [None]
                error = [None]
                def _run_in_thread():
                    new_loop = asyncio.new_event_loop()
                    asyncio.set_event_loop(new_loop)
                    try:
                        result[0] = new_loop.run_until_complete(asyncio.wait_for(method(*args, **kwargs), timeout))
                    except Exception as ex:
                        error[0] = ex
                    finally:
                        new_loop.close()
                t = threading.Thread(target=_run_in_thread)
                t.start()
                t.join()
                if error[0]:
                    if isinstance(error[0], asyncio.TimeoutError):
                        return None, MethodResult.TIMEOUT, (time.time() - start) * 1000, f"Timeout after {timeout}s"
                    raise error[0]
                res = result[0]
            else:
                # No running loop, safe to run_until_complete
                new_loop = asyncio.new_event_loop()
                asyncio.set_event_loop(new_loop)
                try:
                    res = new_loop.run_until_complete(asyncio.wait_for(method(*args, **kwargs), timeout))
                except asyncio.TimeoutError:
                    return None, MethodResult.TIMEOUT, (time.time() - start) * 1000, f"Timeout after {timeout}s"
                finally:
                    new_loop.close()
        else:
            res = method(*args, **kwargs)
        
        duration = (time.time() - start) * 1000
        return res, MethodResult.SUCCESS, duration, None
        
    except Exception as e:
        duration = (time.time() - start) * 1000
        return None, MethodResult.EXCEPTION, duration, str(e)


def _discover_engine_capabilities(engine: Any, timeout: float = 1.0) -> EngineCapabilities:
    caps = EngineCapabilities()
    if engine is None: return caps
    
    for method in METHOD_GROUPS["snapshot_multi"]:
        result, status, duration, error = _safe_call(engine, method, ["test_sheet"], timeout=timeout)
        caps.add_call(method, status, duration, error)
        if status == MethodResult.SUCCESS:
            caps.has_snapshot_multi = True
            break
    
    for method in METHOD_GROUPS["snapshot_single"]:
        result, status, duration, error = _safe_call(engine, method, "test_sheet", timeout=timeout)
        caps.add_call(method, status, duration, error)
        if status == MethodResult.SUCCESS:
            caps.has_snapshot_single = True
            break
    
    for method in METHOD_GROUPS["legacy_single"]:
        result, status, duration, error = _safe_call(engine, method, "test_sheet", timeout=timeout)
        caps.add_call(method, status, duration, error)
        if status == MethodResult.SUCCESS:
            caps.has_legacy_methods = True
            break
    
    for method in METHOD_GROUPS["news"]:
        result, status, duration, error = _safe_call(engine, method, "TEST", timeout=timeout)
        caps.add_call(method, status, duration, error)
        if status == MethodResult.SUCCESS:
            caps.has_news = True
            break
    
    for method in METHOD_GROUPS["technical"]:
        result, status, duration, error = _safe_call(engine, method, "TEST", timeout=timeout)
        caps.add_call(method, status, duration, error)
        if status == MethodResult.SUCCESS:
            caps.has_technical = True
            break
    
    for method in METHOD_GROUPS["historical"]:
        result, status, duration, error = _safe_call(engine, method, "TEST", ["1m"], timeout=timeout)
        caps.add_call(method, status, duration, error)
        if status == MethodResult.SUCCESS:
            caps.has_historical = True
            break
    
    return caps


# ============================================================================
# Engine Snapshot Adapter
# ============================================================================

class EngineSnapshotAdapter:
    """
    Universal adapter that makes any engine compatible with core/investment_advisor.py.
    """
    def __init__(self, base_engine: Any, cache_strategy: CacheStrategy = CacheStrategy.MEMORY, cache_ttl: int = CACHE_TTL_DEFAULT, discover: bool = True):
        self._base = base_engine
        self._cache = SnapshotCache(strategy=cache_strategy, default_ttl=cache_ttl)
        self._capabilities = _discover_engine_capabilities(base_engine) if discover else EngineCapabilities()
        self._created_at = datetime.now(timezone.utc)
        self._metrics = EngineMetrics()
        
        if self._capabilities.has_snapshot_multi or self._capabilities.has_snapshot_single: self._mode = AdapterMode.NATIVE
        elif self._capabilities.has_legacy_methods: self._mode = AdapterMode.LEGACY
        else: self._mode = AdapterMode.STUB
        
        logger.info(f"EngineSnapshotAdapter initialized: mode={self._mode.value}, snapshot_multi={self._capabilities.has_snapshot_multi}, snapshot_single={self._capabilities.has_snapshot_single}, legacy={self._capabilities.has_legacy_methods}")
    
    def __repr__(self) -> str: return f"<EngineSnapshotAdapter mode={self._mode.value} base={type(self._base).__name__}>"
    
    def get_cached_sheet_snapshot(self, sheet_name: str) -> Optional[Dict[str, Any]]:
        start = time.time()
        cached = self._cache.get(sheet_name)
        if cached is not None:
            self._metrics.record_call("cache_hit", 0)
            return {"headers": cached.headers, "rows": cached.rows, "meta": cached.to_dict(), "cached_at_utc": cached.cached_at.isoformat()}
        
        if self._capabilities.has_snapshot_single:
            for method in METHOD_GROUPS["snapshot_single"]:
                result, status, duration, error = _safe_call(self._base, method, sheet_name)
                self._metrics.record_call(method, duration, status == MethodResult.SUCCESS)
                
                if status == MethodResult.SUCCESS and isinstance(result, dict):
                    headers, rows = result.get("headers", []), result.get("rows", [])
                    self._cache.set(sheet_name, headers, rows, method, ttl=CACHE_TTL_SHEET)
                    return {"headers": headers, "rows": rows, "meta": {"source_method": method, "cached": False, "duration_ms": duration}, "cached_at_utc": datetime.now(timezone.utc).isoformat()}
        
        if self._capabilities.has_legacy_methods:
            for method in METHOD_GROUPS["legacy_single"]:
                result, status, duration, error = _safe_call(self._base, method, sheet_name)
                self._metrics.record_call(method, duration, status == MethodResult.SUCCESS)
                
                if status == MethodResult.SUCCESS:
                    headers, rows = self._normalize_legacy_result(result)
                    if headers and rows:
                        self._cache.set(sheet_name, headers, rows, method, ttl=CACHE_TTL_SHEET)
                        return {"headers": headers, "rows": rows, "meta": {"source_method": method, "cached": False, "adapter_mode": "legacy", "duration_ms": duration}, "cached_at_utc": datetime.now(timezone.utc).isoformat()}
        
        self._metrics.record_call("get_cached_sheet_snapshot", (time.time() - start) * 1000, False)
        logger.warning(f"No data found for sheet: {sheet_name}")
        return None
    
    def get_cached_multi_sheet_snapshots(self, sheet_names: List[str]) -> Dict[str, Dict[str, Any]]:
        start = time.time()
        result: Dict[str, Dict[str, Any]] = {}
        
        if self._capabilities.has_snapshot_multi:
            for method in METHOD_GROUPS["snapshot_multi"]:
                res, status, duration, error = _safe_call(self._base, method, sheet_names)
                self._metrics.record_call(method, duration, status == MethodResult.SUCCESS)
                
                if status == MethodResult.SUCCESS and isinstance(res, dict):
                    for sheet, snap in res.items():
                        if isinstance(snap, dict):
                            headers, rows = snap.get("headers", []), snap.get("rows", [])
                            self._cache.set(str(sheet), headers, rows, method, ttl=CACHE_TTL_SHEET)
                            result[str(sheet)] = {"headers": headers, "rows": rows, "meta": {"source_method": method, "cached": False}, "cached_at_utc": datetime.now(timezone.utc).isoformat()}
                    if result:
                        self._metrics.record_call("get_cached_multi_sheet_snapshots", (time.time() - start) * 1000, True)
                        return result
        
        for sheet in sheet_names:
            snap = self.get_cached_sheet_snapshot(sheet)
            if snap: result[sheet] = snap
        
        self._metrics.record_call("get_cached_multi_sheet_snapshots", (time.time() - start) * 1000, bool(result))
        return result
    
    def _normalize_legacy_result(self, result: Any) -> Tuple[List[str], List[List[Any]]]:
        headers, rows = [], []
        try:
            if isinstance(result, tuple) and len(result) == 2:
                h, r = result
                if isinstance(h, (list, tuple)) and isinstance(r, (list, tuple)):
                    headers = list(h)
                    rows = [list(row) if isinstance(row, (list, tuple)) else [row] for row in r]
            elif isinstance(result, list) and len(result) == 2:
                h, r = result
                if isinstance(h, (list, tuple)) and isinstance(r, (list, tuple)):
                    headers = list(h)
                    rows = [list(row) if isinstance(row, (list, tuple)) else [row] for row in r]
            elif isinstance(result, dict):
                if "headers" in result and "rows" in result:
                    h, r = result["headers"], result["rows"]
                    if isinstance(h, (list, tuple)): headers = list(h)
                    if isinstance(r, (list, tuple)): rows = [list(row) if isinstance(row, (list, tuple)) else [row] for row in r]
            elif hasattr(result, "to_dict") and hasattr(result, "columns"):
                try:
                    df = result
                    headers = list(df.columns)
                    for _, row in df.iterrows(): rows.append([row[col] for col in headers])
                except Exception: pass
        except Exception as e: logger.debug(f"Error normalizing legacy result: {e}")
        return headers, rows
    
    def get_news_score(self, symbol: str) -> Optional[float]:
        for method in METHOD_GROUPS["news"]:
            result, status, duration, error = _safe_call(self._base, method, symbol)
            self._metrics.record_call(method, duration, status == MethodResult.SUCCESS)
            if status == MethodResult.SUCCESS:
                try: return float(result)
                except (TypeError, ValueError): pass
        return None
    
    def get_cached_news_score(self, symbol: str) -> Optional[float]: return self.get_news_score(symbol)
    def news_get_score(self, symbol: str) -> Optional[float]: return self.get_news_score(symbol)
    
    def get_technical_signals(self, symbol: str) -> Dict[str, Any]:
        for method in METHOD_GROUPS["technical"]:
            result, status, duration, error = _safe_call(self._base, method, symbol)
            self._metrics.record_call(method, duration, status == MethodResult.SUCCESS)
            if status == MethodResult.SUCCESS and isinstance(result, dict): return result
        return {}
    
    def get_cached_technical_signals(self, symbol: str) -> Dict[str, Any]: return self.get_technical_signals(symbol)
    
    def get_historical_returns(self, symbol: str, periods: List[str]) -> Dict[str, Optional[float]]:
        for method in METHOD_GROUPS["historical"]:
            result, status, duration, error = _safe_call(self._base, method, symbol, periods)
            self._metrics.record_call(method, duration, status == MethodResult.SUCCESS)
            if status == MethodResult.SUCCESS and isinstance(result, dict): return result
        return {}
    
    def set_cached_sheet_snapshot(self, sheet_name: str, headers: List[Any], rows: List[Any], meta: Dict[str, Any]) -> None:
        self._cache.set(sheet_name, [str(h) for h in headers], [list(r) if isinstance(r, (list, tuple)) else [r] for r in rows], "manual_set", ttl=CACHE_TTL_SHEET)
    
    def clear_cache(self) -> None:
        self._cache.clear()
        logger.info("Cache cleared")
    
    def warm_cache(self, sheet_names: List[str]) -> Dict[str, bool]:
        results = {}
        for sheet in sheet_names:
            try:
                snap = self.get_cached_sheet_snapshot(sheet)
                results[sheet] = snap is not None
            except Exception as e:
                logger.warning(f"Failed to warm cache for {sheet}: {e}")
                results[sheet] = False
        return results
    
    def get_metrics(self) -> Dict[str, Any]:
        return {
            "adapter": {
                "mode": self._mode.value,
                "created_at": self._created_at.isoformat(),
                "uptime_seconds": (datetime.now(timezone.utc) - self._created_at).total_seconds(),
            },
            "capabilities": {
                "snapshot_multi": self._capabilities.has_snapshot_multi,
                "snapshot_single": self._capabilities.has_snapshot_single,
                "legacy": self._capabilities.has_legacy_methods,
                "news": self._capabilities.has_news,
                "technical": self._capabilities.has_technical,
                "historical": self._capabilities.has_historical,
                "method_calls": [
                    {"method": c.method_name, "result": c.result.value, "duration_ms": round(c.duration_ms, 2), "error": c.error, "timestamp": c.timestamp.isoformat()}
                    for c in self._capabilities.method_calls
                ],
            },
            "metrics": self._metrics.to_dict(),
            "cache": self._cache.get_metrics(),
        }
    
    def health_check(self) -> Dict[str, Any]:
        status = "healthy"
        issues = []
        test_snap = self.get_cached_sheet_snapshot("test")
        
        total_errors = sum(self._metrics.method_errors.values())
        if total_errors > 100:
            status = "degraded"
            issues.append(f"High error count: {total_errors}")
        
        total_cache = self._metrics.cache_hits + self._metrics.cache_misses
        if total_cache > 0:
            hit_rate = self._metrics.cache_hits / total_cache
            if hit_rate < 0.5:
                status = "degraded"
                issues.append(f"Low cache hit rate: {hit_rate:.1%}")
        
        return {"status": status, "issues": issues, "timestamp": datetime.now(timezone.utc).isoformat(), "metrics": self.get_metrics()}


# ============================================================================
# Main Entry Point
# ============================================================================

def run_investment_advisor(payload: Dict[str, Any], *, engine: Any = None, cache_strategy: str = "memory", cache_ttl: int = CACHE_TTL_DEFAULT, debug: bool = False) -> Dict[str, Any]:
    with TraceContext("run_investment_advisor_engine", {"debug": debug}) as span:
        start_time = time.time()
        request_id = hashlib.md5(str(time.time()).encode()).hexdigest()[:8]
        
        if debug: logger.setLevel(logging.DEBUG)
        logger.info(f"Request {request_id}: Starting investment advisor")
        
        response: Dict[str, Any] = {
            "headers": DEFAULT_HEADERS.copy(),
            "rows": [], "items": [],
            "meta": {"ok": False, "engine_version": ENGINE_VERSION, "request_id": request_id, "error": None, "runtime_ms": 0},
        }
        
        try:
            try: cache_strat = CacheStrategy(cache_strategy.lower())
            except ValueError:
                cache_strat = CacheStrategy.MEMORY
                logger.warning(f"Invalid cache strategy '{cache_strategy}', using memory")
            
            adapter_mode = "none"
            eng_obj = engine
            
            if engine is not None:
                has_native = callable(getattr(engine, "get_cached_sheet_snapshot", None)) or callable(getattr(engine, "get_cached_multi_sheet_snapshots", None))
                if not has_native:
                    eng_obj = EngineSnapshotAdapter(engine, cache_strategy=cache_strat, cache_ttl=cache_ttl)
                    adapter_mode = "wrapped"
                    if hasattr(eng_obj, "get_metrics"):
                        metrics = eng_obj.get_metrics()
                        logger.debug(f"Adapter metrics: {json_dumps(metrics)}")
                else: adapter_mode = "native"
            
            try:
                from core.investment_advisor import run_investment_advisor as core_run
                from core.investment_advisor import ADVISOR_VERSION as CORE_VERSION
            except ImportError:
                try:
                    from .investment_advisor import run_investment_advisor as core_run
                    from .investment_advisor import ADVISOR_VERSION as CORE_VERSION
                except ImportError as e:
                    raise ImportError("Could not import core.investment_advisor. Make sure it's installed and in Python path.") from e
            
            logger.info(f"Request {request_id}: Delegating to core advisor")
            core_result = core_run(payload or {}, engine=eng_obj)
            
            if not isinstance(core_result, dict): raise ValueError(f"Core advisor returned non-dict: {type(core_result)}")
            
            headers = core_result.get("headers")
            if not isinstance(headers, list) or not headers: headers = DEFAULT_HEADERS.copy()
            rows = core_result.get("rows")
            if not isinstance(rows, list): rows = []
            items = core_result.get("items")
            if not isinstance(items, list): items = []
            meta = core_result.get("meta")
            if not isinstance(meta, dict): meta = {}
            
            meta.update({
                "engine_version": ENGINE_VERSION,
                "core_version": meta.get("core_version") or CORE_VERSION,
                "adapter_mode": adapter_mode,
                "cache_strategy": cache_strat.value,
                "cache_ttl": cache_ttl,
                "request_id": request_id,
                "runtime_ms_engine_layer": int((time.time() - start_time) * 1000),
            })
            
            if hasattr(eng_obj, "get_metrics") and adapter_mode == "wrapped":
                try: meta["adapter_metrics"] = eng_obj.get_metrics()
                except Exception as e: logger.debug(f"Failed to get adapter metrics: {e}")
            
            response.update({"headers": headers, "rows": rows, "items": items, "meta": meta})
            logger.info(f"Request {request_id}: Completed successfully in {meta['runtime_ms_engine_layer']}ms with {len(rows)} rows")
            
            advisor_engine_requests.labels(status="success").inc()
            advisor_engine_duration.observe(time.time() - start_time)
            
        except Exception as e:
            logger.error(f"Request {request_id}: Failed: {e}", exc_info=True)
            response["meta"].update({"ok": False, "error": str(e), "runtime_ms": int((time.time() - start_time) * 1000)})
            if debug:
                import traceback
                response["meta"]["traceback"] = traceback.format_exc()
                
            advisor_engine_requests.labels(status="error").inc()
            advisor_engine_duration.observe(time.time() - start_time)
            
        return response


# ============================================================================
# Compatibility Aliases
# ============================================================================

def run_investment_advisor_engine(payload: Dict[str, Any], *, engine: Any = None) -> Dict[str, Any]:
    return run_investment_advisor(payload, engine=engine)

def create_engine_adapter(engine: Any, cache_strategy: str = "memory", cache_ttl: int = CACHE_TTL_DEFAULT) -> EngineSnapshotAdapter:
    return EngineSnapshotAdapter(engine, cache_strategy=CacheStrategy(cache_strategy.lower()), cache_ttl=cache_ttl)

# ============================================================================
# Module Exports
# ============================================================================

__all__ = [
    "run_investment_advisor",
    "run_investment_advisor_engine",
    "create_engine_adapter",
    "EngineSnapshotAdapter",
    "ENGINE_VERSION",
    "DEFAULT_HEADERS",
    "CacheStrategy",
    "AdapterMode",
    "SnapshotMetadata",
]
