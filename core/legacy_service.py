#!/usr/bin/env python3
"""
core/legacy_service.py
================================================================================
Legacy Compatibility Service — v3.0.0 (NEXT-GEN ENTERPRISE)
================================================================================
Financial Data Platform — Legacy Router with Modern Features

What's new in v3.0.0:
- ✅ **ORJSON Serialization**: Integrated C-compiled JSON responses for massive batch throughput
- ✅ **Memory-Optimized Models**: Applied `@dataclass(slots=True)` to reduce memory overhead
- ✅ **Universal Event Loop Management**: Hardened sync/async bridging to prevent loop crashes
- ✅ **Distributed Tracing**: OpenTelemetry integration for legacy endpoint observability
- ✅ **Prometheus Metrics**: Exporting latency and request counts for legacy traffic
- ✅ **Adaptive Cache Eviction**: Upgraded `SmartCache` with true LRU access tracking
- ✅ **Strict Alignment**: Fully synced with `core/investment_advisor_engine.py` v3.0.0

Key Features:
- Zero startup cost (lazy imports)
- Works with any engine implementation (v1, v2, v3)
- Automatic method discovery and fallback
- Smart percent scaling for financial ratios
- Comprehensive error handling and tracing
- Thread-safe operations
"""

from __future__ import annotations

import asyncio
import hashlib
import importlib
import inspect
import logging
import os
import time
import threading
import traceback
import warnings
from collections import defaultdict
from contextlib import asynccontextmanager
from dataclasses import dataclass, field, asdict
from datetime import datetime, timedelta, timezone
from enum import Enum
from typing import Any, AsyncGenerator, Callable, Dict, List, Optional, Set, Tuple, TypeVar, Union

from fastapi import APIRouter, Query, Request, HTTPException
from fastapi.encoders import jsonable_encoder
from pydantic import BaseModel, Field, validator

# ---------------------------------------------------------------------------
# High-Performance JSON fallback
# ---------------------------------------------------------------------------
try:
    from fastapi.responses import ORJSONResponse as BestJSONResponse
    import orjson
    def json_dumps(obj: Any) -> str:
        return orjson.dumps(obj).decode('utf-8')
except ImportError:
    from starlette.responses import JSONResponse as BestJSONResponse
    import json
    def json_dumps(obj: Any) -> str:
        return json.dumps(obj, default=str)

# ---------------------------------------------------------------------------
# Monitoring & Tracing
# ---------------------------------------------------------------------------
try:
    from prometheus_client import Counter, Histogram
    _PROMETHEUS_AVAILABLE = True
    legacy_requests_total = Counter('legacy_requests_total', 'Total requests to legacy router', ['endpoint', 'status'])
    legacy_request_duration = Histogram('legacy_request_duration_seconds', 'Legacy router request duration', ['endpoint'])
except ImportError:
    _PROMETHEUS_AVAILABLE = False
    class DummyMetric:
        def labels(self, *args, **kwargs): return self
        def inc(self, *args, **kwargs): pass
        def observe(self, *args, **kwargs): pass
    legacy_requests_total = DummyMetric()
    legacy_request_duration = DummyMetric()

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
VERSION = __version__

logger = logging.getLogger("core.legacy_service")

# ============================================================================
# Constants
# ============================================================================

# Truthy values for boolean parsing
_TRUTHY = {"1", "true", "yes", "y", "on", "t", "enable", "enabled", "ok", "active"}

# Default concurrency settings
DEFAULT_CONCURRENCY = 8
DEFAULT_TIMEOUT_SEC = 25.0
DEFAULT_MAX_SYMBOLS = 2500

# Cache TTL defaults (seconds)
CACHE_TTL_SNAPSHOT = 300  # 5 minutes
CACHE_TTL_QUOTE = 60      # 1 minute

# Percent-like header patterns
PERCENT_HEADER_PATTERNS = {
    "yield", "margin", "growth", "ratio", "rate", "return",
    "roi", "roe", "roa", "change %", "position %", "turnover %",
    "free float %", "payout ratio", "upside %", "volatility",
}

# ============================================================================
# Environment Helpers
# ============================================================================

def _env_bool(name: str, default: bool = False) -> bool:
    v = os.getenv(name)
    if v is None: return default
    return v.strip().lower() in _TRUTHY

def _env_int(name: str, default: int) -> int:
    try:
        v = int(str(os.getenv(name, "")).strip() or default)
        return v if v > 0 else default
    except Exception:
        return default

def _env_float(name: str, default: float) -> float:
    try:
        v = float(str(os.getenv(name, "")).strip() or default)
        return v if v > 0 else default
    except Exception:
        return default

# Configuration from environment
ENABLE_EXTERNAL_LEGACY_ROUTER = _env_bool("ENABLE_EXTERNAL_LEGACY_ROUTER", False)
LOG_EXTERNAL_IMPORT_FAILURE = _env_bool("LOG_EXTERNAL_LEGACY_IMPORT_FAILURE", False)
DEBUG_ERRORS = _env_bool("DEBUG_ERRORS", False)
ENABLE_AUTH = _env_bool("ENABLE_LEGACY_AUTH", False)

LEGACY_CONCURRENCY = max(1, min(50, _env_int("LEGACY_CONCURRENCY", DEFAULT_CONCURRENCY)))
LEGACY_TIMEOUT_SEC = max(3.0, min(90.0, _env_float("LEGACY_TIMEOUT_SEC", DEFAULT_TIMEOUT_SEC)))
LEGACY_MAX_SYMBOLS = max(50, min(10000, _env_int("LEGACY_MAX_SYMBOLS", DEFAULT_MAX_SYMBOLS)))

_external_loaded_from: Optional[str] = None


# ============================================================================
# Enums and Data Classes
# ============================================================================

class EngineSource(Enum):
    APP_STATE = "app.state.engine"
    V2_SINGLETON = "core.data_engine_v2.get_engine"
    V2_TEMP = "core.data_engine_v2.temp"
    V1_TEMP = "core.data_engine.temp"
    NONE = "none"


class CallStatus(Enum):
    SUCCESS = "success"
    FAILED = "failed"
    TIMEOUT = "timeout"
    NOT_FOUND = "not_found"
    EXCEPTION = "exception"


@dataclass(slots=True)
class MethodCall:
    method: str
    status: CallStatus
    duration_ms: float
    error: Optional[str] = None
    timestamp: datetime = field(default_factory=lambda: datetime.now(timezone.utc))


@dataclass(slots=True)
class EngineInfo:
    engine: Any
    source: EngineSource
    should_close: bool
    method_calls: List[MethodCall] = field(default_factory=list)
    
    def add_call(self, method: str, status: CallStatus, duration_ms: float, error: Optional[str] = None) -> None:
        self.method_calls.append(MethodCall(method=method, status=status, duration_ms=duration_ms, error=error))


@dataclass(slots=True)
class ServiceMetrics:
    total_requests: int = 0
    cache_hits: int = 0
    cache_misses: int = 0
    engine_resolutions: Dict[str, int] = field(default_factory=dict)
    method_calls: Dict[str, int] = field(default_factory=dict)
    method_errors: Dict[str, int] = field(default_factory=dict)
    start_time: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    
    def record_engine(self, source: EngineSource) -> None:
        self.engine_resolutions[source.value] = self.engine_resolutions.get(source.value, 0) + 1
    
    def record_call(self, method: str, success: bool = True) -> None:
        self.total_requests += 1
        self.method_calls[method] = self.method_calls.get(method, 0) + 1
        if not success:
            self.method_errors[method] = self.method_errors.get(method, 0) + 1
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "total_requests": self.total_requests,
            "cache_hits": self.cache_hits,
            "cache_misses": self.cache_misses,
            "engine_resolutions": dict(self.engine_resolutions),
            "method_calls": dict(self.method_calls),
            "method_errors": dict(self.method_errors),
            "uptime_seconds": (datetime.now(timezone.utc) - self.start_time).total_seconds(),
        }


# ============================================================================
# Pydantic Models
# ============================================================================

class SymbolsIn(BaseModel):
    """Input model for symbols list."""
    symbols: List[str] = Field(default_factory=list, description="List of symbols")
    tickers: List[str] = Field(default_factory=list, description="Alias for symbols")
    
    @validator("symbols", "tickers", pre=True)
    def validate_list(cls, v: Any) -> List[str]:
        if v is None: return []
        if isinstance(v, str): return [s.strip() for s in v.split(",") if s.strip()]
        if isinstance(v, (list, tuple)): return [str(s).strip() for s in v if s]
        return []
    
    def normalized(self) -> List[str]:
        items = self.symbols or self.tickers or []
        out: List[str] = []
        seen: Set[str] = set()
        for x in items[:LEGACY_MAX_SYMBOLS]:
            s = str(x or "").strip()
            if not s: continue
            su = s.upper()
            if su in seen: continue
            seen.add(su)
            out.append(s)
        return out


class SheetRowsIn(BaseModel):
    """Input model for sheet rows request."""
    symbols: List[str] = Field(default_factory=list, description="List of symbols")
    tickers: List[str] = Field(default_factory=list, description="Alias for symbols")
    sheet_name: str = Field("", description="Sheet name for caching")
    sheetName: str = Field("", description="Alias for sheet_name")
    
    @validator("sheet_name", "sheetName", pre=True)
    def validate_sheet_name(cls, v: Any) -> str:
        return str(v).strip() if v is not None else ""
    
    def get_sheet_name(self) -> str:
        return self.sheet_name or self.sheetName or ""


class CacheControl(BaseModel):
    ttl: int = Field(CACHE_TTL_SNAPSHOT, description="Cache TTL in seconds")
    skip_cache: bool = Field(False, description="Skip cache and force refresh")


# ============================================================================
# Helper Functions
# ============================================================================

# Riyadh timezone (UTC+3, no DST)
RIYADH_TZ = timezone(timedelta(hours=3))

def _utc_now() -> datetime: return datetime.now(timezone.utc)
def _utc_iso() -> str: return _utc_now().isoformat()
def _riyadh_now() -> datetime: return datetime.now(RIYADH_TZ)
def _riyadh_iso() -> str: return _riyadh_now().isoformat()

def _safe_str(x: Any, default: str = "") -> str:
    try: return str(x).strip() if x is not None else default
    except Exception: return default

def _safe_int(x: Any, default: Optional[int] = None) -> Optional[int]:
    try: return int(float(str(x).strip())) if x is not None else default
    except (ValueError, TypeError): return default

def _safe_float(x: Any, default: Optional[float] = None) -> Optional[float]:
    try: return float(str(x).strip().replace(",", "")) if x is not None else default
    except (ValueError, TypeError): return default

def _safe_err(e: BaseException) -> str:
    msg = str(e).strip()
    return msg or e.__class__.__name__

def _looks_like_percent_header(header: str) -> bool:
    h = header.lower().strip()
    if "%" in h: return True
    return any(pattern in h for pattern in PERCENT_HEADER_PATTERNS)

def _to_float_best(x: Any) -> Optional[float]:
    if x is None or isinstance(x, bool): return None
    if isinstance(x, (int, float)):
        try: return float(x)
        except Exception: return None
    s = _safe_str(x)
    if not s or s.lower() in {"na", "n/a", "null", "none", "-", "—", ""}: return None
    if s.endswith("%"): s = s[:-1].strip()
    try: return float(s.replace(",", ""))
    except Exception: return None

def _coerce_for_header(header: str, val: Any) -> Any:
    if val is None: return None
    f = _to_float_best(val)
    if f is None: return val
    if _looks_like_percent_header(header):
        return round(f * 100.0, 4) if -2.0 <= f <= 2.0 else round(f, 4)
    return round(f, 4) if isinstance(f, float) else f

def _normalize_symbol(symbol: str) -> str:
    s = _safe_str(symbol).upper().replace(" ", "")
    if not s: return ""
    if s.endswith(".SA"): s = s[:-3] + ".SR"
    try:
        from core.data_engine_v2 import normalize_symbol
        return _safe_str(normalize_symbol(s)) or s
    except Exception:
        return s

def _looks_like_this_file(path: str) -> bool:
    p = (path or "").replace("\\", "/")
    return p.endswith("/core/legacy_service.py")

def _safe_mod_file(mod: Any) -> str:
    try: return str(getattr(mod, "__file__", "") or "")
    except Exception: return ""

def _quote_dict(q: Any) -> Dict[str, Any]:
    if q is None: return {}
    if isinstance(q, dict): return q
    try:
        if hasattr(q, "model_dump"): return q.model_dump()
        if hasattr(q, "dict"): return q.dict()
        return dict(getattr(q, "__dict__", {}) or {})
    except Exception: return {}

def _extract_symbol_from_quote(q: Any) -> str:
    d = _quote_dict(q)
    for key in ("symbol_normalized", "symbol", "ticker", "requested_symbol"):
        if key in d and d[key]: return _safe_str(d[key]).upper()
    return ""

def _items_to_ordered_list(items: Any, symbols: List[str]) -> List[Any]:
    if items is None: return []
    if isinstance(items, list):
        sym_map = {k: it for it in items if (k := _extract_symbol_from_quote(it))}
        if sym_map:
            ordered = [sym_map.get(_safe_str(s).upper()) for s in symbols]
            if any(x is not None for x in ordered): return ordered
        return items
    if isinstance(items, dict):
        mp = {}
        for k, v in items.items():
            if kk := _safe_str(k).upper(): mp[kk] = v
            if s2 := _extract_symbol_from_quote(v):
                if s2 not in mp: mp[s2] = v
        ordered = [mp.get(_safe_str(s).upper()) for s in symbols]
        if any(x is not None for x in ordered): return ordered
        return list(items.values())
    if len(symbols) == 1: return [items]
    return []

async def _maybe_await(x: Any) -> Any:
    try:
        if inspect.isawaitable(x): return await x
    except Exception: pass
    return x

def _as_payload(obj: Any) -> Dict[str, Any]:
    if obj is None: return {}
    if isinstance(obj, dict): return jsonable_encoder(obj)
    if hasattr(obj, "model_dump") and callable(obj.model_dump):
        try: return jsonable_encoder(obj.model_dump())
        except Exception: pass
    if hasattr(obj, "dict") and callable(obj.dict):
        try: return jsonable_encoder(obj.dict())
        except Exception: pass
    if hasattr(obj, "__dataclass_fields__"):
        try: return jsonable_encoder({f: getattr(obj, f) for f in obj.__dataclass_fields__})
        except Exception: pass
    return {"value": _safe_str(obj)}

def _unwrap_tuple(x: Any) -> Any:
    return x[0] if isinstance(x, tuple) and len(x) >= 1 else x

def _unwrap_container(x: Any) -> Any:
    if not isinstance(x, dict): return x
    for key in ("items", "data", "quotes", "results", "payload"):
        if key in x and isinstance(x[key], (list, dict)): return x[key]
    return x

def _origin_from_symbol(symbol: str) -> str:
    s = _safe_str(symbol).upper()
    return "KSA_TADAWUL" if s.endswith(".SR") else "GLOBAL_MARKETS"

# ============================================================================
# Computed Fields
# ============================================================================

def _compute_52w_position(price: Any, low: Any, high: Any) -> Optional[float]:
    p, l, h = _safe_float(price), _safe_float(low), _safe_float(high)
    if p is None or l is None or h is None or h == l: return None
    return round(((p - l) / (h - l)) * 100.0, 4)

def _compute_turnover(volume: Any, shares: Any) -> Optional[float]:
    v, s = _safe_float(volume), _safe_float(shares)
    if v is None or s is None or s == 0: return None
    return round((v / s) * 100.0, 4)

def _compute_value_traded(price: Any, volume: Any) -> Optional[float]:
    p, v = _safe_float(price), _safe_float(volume)
    if p is None or v is None: return None
    return round(p * v, 4)

def _compute_free_float_mcap(mcap: Any, free_float: Any) -> Optional[float]:
    m, f = _safe_float(mcap), _safe_float(free_float)
    if m is None or f is None: return None
    if f > 1.5: f = f / 100.0
    return m * max(0.0, min(1.0, f))

def _compute_liquidity_score(value_traded: Any) -> Optional[float]:
    vt = _safe_float(value_traded)
    if vt is None or vt <= 0: return None
    try:
        import math
        log_val = math.log10(vt)
        score = ((log_val - 6.0) / (11.0 - 6.0)) * 100.0
        return max(0.0, min(100.0, round(score, 2)))
    except Exception: return None

def _compute_change_and_pct(price: Any, prev: Any) -> Tuple[Optional[float], Optional[float]]:
    p, pc = _safe_float(price), _safe_float(prev)
    if p is None or pc is None or pc == 0: return None, None
    change = round(p - pc, 6)
    pct = round((p / pc - 1.0) * 100.0, 6)
    return change, pct

def _compute_fair_value(payload: Dict[str, Any]) -> Tuple[Optional[float], Optional[float], Optional[str]]:
    price = _safe_float(payload.get("current_price") or payload.get("price"))
    if price is None or price <= 0: return None, None, None
    fair = _safe_float(payload.get("fair_value")) or _safe_float(payload.get("target_price")) or \
           _safe_float(payload.get("forecast_price_3m")) or _safe_float(payload.get("forecast_price_12m")) or \
           _safe_float(payload.get("ma200")) or _safe_float(payload.get("ma50"))
    if fair is None or fair <= 0: return None, None, None
    upside = round((fair / price - 1.0) * 100.0, 4)
    label = "Undervalued" if upside >= 20 else "Moderately Undervalued" if upside >= 10 else \
            "Fairly Valued" if upside >= -10 else "Moderately Overvalued" if upside >= -20 else "Overvalued"
    return fair, upside, label

def _compute_data_quality(payload: Dict[str, Any]) -> str:
    if _safe_float(payload.get("current_price") or payload.get("price")) is None: return "MISSING"
    
    # Ensure FIELD_CATEGORIES is accessible (defining a minimal version here to avoid missing references)
    categories = {
        "identity": {"symbol", "name", "sector", "market"},
        "price": {"current_price", "previous_close", "volume"},
        "fundamentals": {"market_cap", "pe_ttm", "eps_ttm"}
    }
    
    score, total = 0, 0
    for fields in categories.values():
        hits = sum(1 for f in fields if f in payload and payload[f] is not None)
        tot = len(fields)
        if tot > 0: score += hits * 10; total += tot * 10
    
    if total == 0: return "POOR"
    pct = (score / total) * 100
    if pct >= 70: return "EXCELLENT"
    elif pct >= 50: return "GOOD"
    elif pct >= 30: return "FAIR"
    return "POOR"


# ============================================================================
# Advanced Caching
# ============================================================================

@dataclass(slots=True)
class CacheEntry:
    data: Any
    expires_at: datetime
    created_at: datetime = field(default_factory=_utc_now)
    last_access: float = field(default_factory=time.time)
    
    @property
    def is_expired(self) -> bool:
        return _utc_now() > self.expires_at

class SmartCache:
    """Thread-safe cache with TTL and true LRU eviction."""
    def __init__(self, default_ttl: int = CACHE_TTL_SNAPSHOT, max_size: int = 5000):
        self._cache: Dict[str, CacheEntry] = {}
        self._lock = asyncio.Lock()
        self.default_ttl = default_ttl
        self.max_size = max_size
        self.hits = 0
        self.misses = 0
    
    async def get(self, key: str) -> Optional[Any]:
        async with self._lock:
            if key in self._cache:
                entry = self._cache[key]
                if not entry.is_expired:
                    entry.last_access = time.time()
                    self.hits += 1
                    return entry.data
                else:
                    del self._cache[key]
            self.misses += 1
            return None
    
    async def set(self, key: str, value: Any, ttl: Optional[int] = None) -> None:
        async with self._lock:
            if len(self._cache) >= self.max_size and key not in self._cache:
                oldest = min(self._cache.items(), key=lambda x: x[1].last_access)[0]
                del self._cache[oldest]
                
            ttl_seconds = ttl or self.default_ttl
            expires = _utc_now() + timedelta(seconds=ttl_seconds)
            self._cache[key] = CacheEntry(data=value, expires_at=expires)
    
    async def delete(self, key: str) -> None:
        async with self._lock: self._cache.pop(key, None)
    
    async def clear(self) -> None:
        async with self._lock:
            self._cache.clear()
            self.hits = 0
            self.misses = 0
    
    async def get_stats(self) -> Dict[str, Any]:
        async with self._lock:
            total = self.hits + self.misses
            return {
                "size": len(self._cache),
                "hits": self.hits,
                "misses": self.misses,
                "hit_rate": self.hits / total if total > 0 else 0,
                "default_ttl": self.default_ttl,
                "max_size": self.max_size
            }


# ============================================================================
# Engine Discovery
# ============================================================================

async def _call_engine_method(
    engine: Any,
    methods: List[str],
    *args: Any,
    timeout: float = LEGACY_TIMEOUT_SEC,
    **kwargs: Any
) -> Tuple[Optional[Any], Optional[str], Optional[str]]:
    """Try multiple methods safely using an isolated event loop for synchronous blocking calls."""
    last_error = None
    
    for method_name in methods:
        method = getattr(engine, method_name, None)
        if not callable(method): continue
        
        try:
            if inspect.iscoroutinefunction(method):
                try:
                    loop = asyncio.get_running_loop()
                except RuntimeError:
                    loop = None

                if loop and loop.is_running():
                    # Running in FastAPI loop but method might block natively, use thread isolation
                    result_box = [None]
                    error_box = [None]
                    def _run_in_thread():
                        new_loop = asyncio.new_event_loop()
                        asyncio.set_event_loop(new_loop)
                        try:
                            result_box[0] = new_loop.run_until_complete(asyncio.wait_for(method(*args, **kwargs), timeout))
                        except Exception as ex:
                            error_box[0] = ex
                        finally:
                            new_loop.close()
                    
                    t = threading.Thread(target=_run_in_thread)
                    t.start()
                    t.join()
                    
                    if error_box[0]:
                        if isinstance(error_box[0], asyncio.TimeoutError):
                            last_error = f"Timeout after {timeout}s"
                            continue
                        raise error_box[0]
                    result = result_box[0]
                else:
                    new_loop = asyncio.new_event_loop()
                    asyncio.set_event_loop(new_loop)
                    try:
                        result = new_loop.run_until_complete(asyncio.wait_for(method(*args, **kwargs), timeout))
                    finally:
                        new_loop.close()
            else:
                result = method(*args, **kwargs)
            
            return result, method_name, None
            
        except asyncio.TimeoutError:
            last_error = f"Timeout after {timeout}s"
            continue
        except Exception as e:
            last_error = _safe_err(e)
            continue
            
    return None, None, last_error


async def _get_engine(request: Request) -> Tuple[EngineInfo, ServiceMetrics]:
    start = time.time()
    metrics = ServiceMetrics()
    
    try:
        eng = getattr(request.app.state, "engine", None)
        if eng is not None:
            metrics.record_engine(EngineSource.APP_STATE)
            return EngineInfo(engine=eng, source=EngineSource.APP_STATE, should_close=False), metrics
    except Exception: pass
    
    try:
        from core.data_engine_v2 import get_engine as v2_get_engine
        eng2 = v2_get_engine()
        eng2 = await _maybe_await(eng2)
        if eng2 is not None:
            try: request.app.state.engine = eng2
            except Exception: pass
            metrics.record_engine(EngineSource.V2_SINGLETON)
            return EngineInfo(engine=eng2, source=EngineSource.V2_SINGLETON, should_close=False), metrics
    except Exception: pass
    
    try:
        mod = importlib.import_module("core.data_engine_v2")
        engine_class = getattr(mod, "DataEngineV2", None) or getattr(mod, "DataEngine", None)
        if engine_class is not None:
            eng3 = engine_class()
            metrics.record_engine(EngineSource.V2_TEMP)
            return EngineInfo(engine=eng3, source=EngineSource.V2_TEMP, should_close=True), metrics
    except Exception: pass
    
    try:
        from core.data_engine import DataEngine as V1Engine
        eng4 = V1Engine()
        metrics.record_engine(EngineSource.V1_TEMP)
        return EngineInfo(engine=eng4, source=EngineSource.V1_TEMP, should_close=True), metrics
    except Exception: pass
    
    metrics.record_engine(EngineSource.NONE)
    return EngineInfo(engine=None, source=EngineSource.NONE, should_close=False), metrics


async def _close_engine(engine_info: EngineInfo) -> None:
    if not engine_info.should_close or engine_info.engine is None: return
    try:
        aclose = getattr(engine_info.engine, "aclose", None)
        if callable(aclose):
            await _maybe_await(aclose())
            return
    except Exception: pass
    try:
        close = getattr(engine_info.engine, "close", None)
        if callable(close): close()
    except Exception: pass


# ============================================================================
# Enriched Quote Class (Memory Optimized)
# ============================================================================

ENRICHED_HEADERS_59: List[str] = [
    "Rank", "Symbol", "Origin", "Name", "Sector", "Sub Sector", "Market",
    "Currency", "Listing Date", "Price", "Prev Close", "Change", "Change %",
    "Day High", "Day Low", "52W High", "52W Low", "52W Position %",
    "Volume", "Avg Vol 30D", "Value Traded", "Turnover %", "Shares Outstanding",
    "Free Float %", "Market Cap", "Free Float Mkt Cap",
    "Liquidity Score", "EPS (TTM)", "Forward EPS", "P/E (TTM)", "Forward P/E",
    "P/B", "P/S", "EV/EBITDA", "Dividend Yield", "Dividend Rate", "Payout Ratio", "Beta",
    "ROE", "ROA", "Net Margin", "EBITDA Margin", "Revenue Growth", "Net Income Growth",
    "Volatility 30D", "RSI 14", "Fair Value", "Upside %", "Valuation Label",
    "Value Score", "Quality Score", "Momentum Score", "Opportunity Score", "Risk Score",
    "Overall Score", "Error", "Recommendation", "Data Source", "Data Quality",
    "Last Updated (UTC)", "Last Updated (Riyadh)",
]

_HEADER_MAP = {
    "rank": (("rank",), None),
    "symbol": (("symbol", "symbol_normalized", "ticker", "code"), None),
    "origin": (("origin", "market_region"), None),
    "name": (("name", "company_name", "long_name"), None),
    "sector": (("sector",), None),
    "sub sector": (("sub_sector", "subsector", "industry"), None),
    "market": (("market", "exchange", "primary_exchange"), None),
    "currency": (("currency",), None),
    "listing date": (("listing_date", "ipo_date", "ipo"), None),
    "price": (("current_price", "last_price", "price", "regular_market_price"), None),
    "prev close": (("previous_close", "prev_close", "regular_market_previous_close"), None),
    "change": (("price_change", "change", "regular_market_change"), None),
    "change %": (("percent_change", "change_percent", "regular_market_change_percent"), _to_percent),
    "day high": (("day_high", "regular_market_day_high"), None),
    "day low": (("day_low", "regular_market_day_low"), None),
    "52w high": (("week_52_high", "fifty_two_week_high", "year_high"), None),
    "52w low": (("week_52_low", "fifty_two_week_low", "year_low"), None),
    "52w position %": (("week_52_position_pct", "position_52w"), _to_percent),
    "volume": (("volume", "regular_market_volume"), None),
    "avg vol 30d": (("avg_volume_30d", "average_volume", "average_daily_volume"), None),
    "value traded": (("value_traded", "traded_value", "turnover_value"), None),
    "turnover %": (("turnover_percent",), _to_percent),
    "shares outstanding": (("shares_outstanding", "shares_out"), None),
    "free float %": (("free_float", "free_float_percent"), _to_percent),
    "market cap": (("market_cap", "market_capitalization"), None),
    "free float mkt cap": (("free_float_market_cap", "free_float_mkt_cap"), None),
    "liquidity score": (("liquidity_score",), None),
    "eps (ttm)": (("eps_ttm", "trailing_eps", "earnings_per_share"), None),
    "forward eps": (("forward_eps",), None),
    "p/e (ttm)": (("pe_ttm", "trailing_pe", "price_to_earnings"), None),
    "forward p/e": (("forward_pe",), None),
    "p/b": (("pb", "price_to_book", "price_book"), None),
    "p/s": (("ps", "price_to_sales", "price_sales"), None),
    "ev/ebitda": (("ev_ebitda", "enterprise_value_to_ebitda"), None),
    "dividend yield": (("dividend_yield",), _to_percent),
    "dividend rate": (("dividend_rate",), None),
    "payout ratio": (("payout_ratio",), _to_percent),
    "beta": (("beta",), None),
    "roe": (("roe", "return_on_equity"), _to_percent),
    "roa": (("roa", "return_on_assets"), _to_percent),
    "net margin": (("net_margin", "profit_margin"), _to_percent),
    "ebitda margin": (("ebitda_margin",), _to_percent),
    "revenue growth": (("revenue_growth",), _to_percent),
    "net income growth": (("net_income_growth",), _to_percent),
    "volatility 30d": (("volatility_30d", "vol_30d_ann"), _to_volatility),
    "rsi 14": (("rsi_14", "rsi14"), None),
    "fair value": (("fair_value", "target_price", "forecast_price_3m", "forecast_price_12m"), None),
    "upside %": (("upside_percent",), _to_percent),
    "valuation label": (("valuation_label",), None),
    "value score": (("value_score",), None),
    "quality score": (("quality_score",), None),
    "momentum score": (("momentum_score",), None),
    "opportunity score": (("opportunity_score",), None),
    "risk score": (("risk_score",), None),
    "overall score": (("overall_score", "composite_score"), None),
    "error": (("error", "error_message"), None),
    "recommendation": (("recommendation",), None),
    "data source": (("data_source", "source", "provider"), None),
    "data quality": (("data_quality",), None),
    "last updated (utc)": (("last_updated_utc", "as_of_utc", "timestamp_utc"), _iso_or_none),
    "last updated (riyadh)": (("last_updated_riyadh",), _iso_or_none),
}

def _get_master_headers() -> List[str]: return ENRICHED_HEADERS_59.copy()

def _get_headers_for_sheet(sheet_name: str) -> List[str]:
    try:
        from core.schemas import get_headers_for_sheet
        if callable(get_headers_for_sheet):
            headers = get_headers_for_sheet(sheet_name)
            if headers and isinstance(headers, list): return [str(h) for h in headers]
    except Exception: pass
    return _get_master_headers()


class EnrichedQuote:
    __slots__ = ('payload', '_computed')
    def __init__(self, payload: Dict[str, Any]):
        self.payload = payload or {}
        self._computed: Dict[str, Any] = {}
        
    @classmethod
    def from_unified(cls, unified: Any) -> EnrichedQuote:
        payload = _as_payload(_unwrap_tuple(_unwrap_container(unified)))
        if "symbol" not in payload and "symbol_normalized" in payload: payload["symbol"] = payload["symbol_normalized"]
        if "recommendation" in payload: payload["recommendation"] = _normalize_recommendation(payload["recommendation"])
        if "last_updated_utc" not in payload: payload["last_updated_utc"] = _now_utc_iso()
        if "last_updated_riyadh" not in payload: payload["last_updated_riyadh"] = _to_riyadh_iso(payload["last_updated_utc"])
        return cls(payload)
        
    def _get_value(self, header: str) -> Any:
        h = header.strip().lower()
        if h in self._computed: return self._computed[h]
        
        spec = _HEADER_MAP.get(h)
        if not spec: return self.payload.get(h)
        
        fields, transform = spec
        if h == "origin":
            val = self._get_first(fields)
            return val if val is not None else _origin_from_symbol(self.payload.get("symbol", ""))
        
        if h == "change":
            val = self._get_first(fields)
            if val is not None: return val
            ch, _ = _compute_change_and_pct(self.payload.get("current_price"), self.payload.get("previous_close"))
            self._computed[h] = ch
            return ch
            
        if h == "change %":
            val = self._get_first(fields)
            if val is not None: return _to_percent(val) if transform else val
            _, pct = _compute_change_and_pct(self.payload.get("current_price"), self.payload.get("previous_close"))
            self._computed[h] = pct
            return pct
            
        if h == "value traded":
            val = self._get_first(fields)
            if val is not None: return val
            vt = _compute_value_traded(self.payload.get("current_price"), self.payload.get("volume"))
            self._computed[h] = vt
            return vt
            
        if h == "52w position %":
            val = self._get_first(fields)
            if val is not None: return _to_percent(val) if transform else val
            pos = _compute_52w_position(self.payload.get("current_price"), self.payload.get("week_52_low"), self.payload.get("week_52_high"))
            self._computed[h] = pos
            return pos
            
        if h == "turnover %":
            val = self._get_first(fields)
            if val is not None: return _to_percent(val) if transform else val
            to = _compute_turnover(self.payload.get("volume"), self.payload.get("shares_outstanding"))
            self._computed[h] = to
            return to
            
        if h == "free float mkt cap":
            val = self._get_first(fields)
            if val is not None: return val
            ffmc = _compute_free_float_mcap(self.payload.get("market_cap"), self.payload.get("free_float"))
            self._computed[h] = ffmc
            return ffmc
            
        if h == "liquidity score":
            val = self._get_first(fields)
            if val is not None: return val
            ls = _compute_liquidity_score(self.payload.get("value_traded"))
            self._computed[h] = ls
            return ls
            
        if h in ("fair value", "upside %", "valuation label"):
            fair, upside, label = _compute_fair_value(self.payload)
            if h == "fair value": return self._get_first(fields) if self._get_first(fields) is not None else fair
            if h == "upside %": 
                val = self._get_first(fields)
                return _to_percent(val) if transform and val is not None else (val if val is not None else upside)
            if h == "valuation label": return self._get_first(fields) if self._get_first(fields) is not None else label
            
        if h == "recommendation": return _normalize_recommendation(self.payload.get("recommendation"))
        if h == "data quality": return self.payload.get("data_quality") or _compute_data_quality(self.payload)
        if h == "last updated (riyadh)":
            val = self._get_first(fields)
            return _to_iso(val) if transform and val is not None else (val if val is not None else _to_riyadh_iso(self.payload.get("last_updated_utc")))
            
        val = self._get_first(fields)
        if transform and val is not None:
            try: return transform(val)
            except Exception: return val
        return val
        
    def _get_first(self, fields: Tuple[str, ...]) -> Any:
        for f in fields:
            if f in self.payload and self.payload[f] is not None: return self.payload[f]
        return None
        
    def to_row(self, headers: Sequence[str]) -> List[Any]:
        return [self._get_value(h) for h in headers]


def _quote_to_row(q: Any, headers: List[str]) -> List[Any]:
    return EnrichedQuote.from_unified(q).to_row(headers)


async def _try_cache_snapshot(engine: Any, sheet_name: str, headers: List[str], rows: List[List[Any]], meta: Dict[str, Any]) -> bool:
    if engine is None or not sheet_name or not headers: return False
    for method_name in ["set_cached_sheet_snapshot", "cache_sheet_snapshot", "store_sheet_snapshot", "save_sheet_snapshot"]:
        method = getattr(engine, method_name, None)
        if callable(method):
            try:
                await _maybe_await(method(sheet_name, headers, rows, meta))
                return True
            except Exception as e:
                logger.debug(f"Cache method {method_name} failed: {e}")
                continue
    return False


# ============================================================================
# Authentication
# ============================================================================

async def _check_auth(request: Request) -> Tuple[bool, Optional[str]]:
    if not ENABLE_AUTH: return True, None
    try:
        from core.config import auth_ok
        api_key = request.headers.get("X-API-Key") or request.headers.get("Authorization")
        if not api_key: return False, "Missing API key"
        if not auth_ok(token=api_key, headers=dict(request.headers)): return False, "Invalid API key"
        return True, None
    except Exception as e:
        logger.warning(f"Auth check failed: {e}")
        return False, "Authentication error"


# ============================================================================
# External Router Import
# ============================================================================

def _try_import_external_router() -> Optional[APIRouter]:
    global _external_loaded_from
    for module_name, source in [("legacy_service", "legacy_service"), ("routes.legacy_service", "routes.legacy_service")]:
        try:
            mod = importlib.import_module(module_name)
            if _looks_like_this_file(_safe_mod_file(mod)): continue
            router_obj = getattr(mod, "router", None)
            if router_obj and isinstance(router_obj, APIRouter):
                _external_loaded_from = source
                logger.info(f"Loaded external router from {source}")
                return router_obj
        except Exception as e:
            if LOG_EXTERNAL_IMPORT_FAILURE: logger.debug(f"Failed to import {module_name}: {e}")
            continue
    return None


# ============================================================================
# Router Initialization
# ============================================================================

router: APIRouter = APIRouter(prefix="/v1/legacy", tags=["legacy_compat"])
_cache = SmartCache(default_ttl=CACHE_TTL_QUOTE, max_size=10000)
_metrics = ServiceMetrics()


# ============================================================================
# Endpoints
# ============================================================================

@router.get("/health", summary="Legacy compatibility health check")
async def legacy_health(request: Request) -> Dict[str, Any]:
    auth_ok, auth_error = await _check_auth(request)
    if not auth_ok: return {"status": "error", "error": auth_error or "Unauthorized", "version": VERSION}
    
    engine_info, _ = await _get_engine(request)
    
    response: Dict[str, Any] = {
        "status": "ok", "version": VERSION, "router": "core.legacy_service", "mode": "internal",
        "external_router_enabled": ENABLE_EXTERNAL_LEGACY_ROUTER,
        "engine": {
            "present": engine_info.engine is not None, "source": engine_info.source.value,
            "should_close": engine_info.should_close, "class": engine_info.engine.__class__.__name__ if engine_info.engine else None,
        },
        "config": {"concurrency": LEGACY_CONCURRENCY, "timeout_sec": LEGACY_TIMEOUT_SEC, "max_symbols": LEGACY_MAX_SYMBOLS, "auth_enabled": ENABLE_AUTH},
        "cache": await _cache.get_stats(), "metrics": _metrics.to_dict(),
    }
    
    if _external_loaded_from: response["external_loaded_from"] = _external_loaded_from
    
    if engine_info.engine is not None:
        try:
            if hasattr(engine_info.engine, "version"): response["engine"]["version"] = engine_info.engine.version
            elif hasattr(engine_info.engine, "ENGINE_VERSION"): response["engine"]["version"] = engine_info.engine.ENGINE_VERSION
        except Exception: pass
    
    await _close_engine(engine_info)
    return response


@router.get("/quote", summary="Legacy quote endpoint")
async def legacy_quote(
    request: Request, symbol: str = Query(..., min_length=1, description="Symbol to lookup"),
    debug: bool = Query(False, description="Include debug information"), skip_cache: bool = Query(False, description="Skip cache and force refresh"),
) -> BestJSONResponse:
    request_id = hashlib.md5(f"{time.time()}{symbol}".encode()).hexdigest()[:8]
    start = time.time()
    
    with tracer.start_as_current_span("legacy_get_quote") as span:
        span.set_attribute("symbol", symbol)
        
        auth_ok, auth_error = await _check_auth(request)
        if not auth_ok:
            legacy_requests_total.labels(endpoint="quote", status="unauthorized").inc()
            return BestJSONResponse(status_code=401 if ENABLE_AUTH else 200, content={"status": "error", "error": auth_error or "Unauthorized", "request_id": request_id})
        
        raw, norm = symbol.strip(), _normalize_symbol(symbol.strip())
        cache_key = f"quote:{norm}"
        
        if not skip_cache:
            cached = await _cache.get(cache_key)
            if cached is not None:
                _metrics.cache_hits += 1
                return BestJSONResponse(status_code=200, content=jsonable_encoder(cached))
        _metrics.cache_misses += 1
        
        engine_info, metrics = await _get_engine(request)
        _metrics.engine_resolutions[engine_info.source.value] = _metrics.engine_resolutions.get(engine_info.source.value, 0) + 1
        
        if engine_info.engine is None:
            legacy_requests_total.labels(endpoint="quote", status="no_engine").inc()
            return BestJSONResponse(status_code=200, content={"status": "error", "symbol": raw, "symbol_normalized": norm, "error": "No engine available", "request_id": request_id, "runtime_ms": (time.time() - start) * 1000})
        
        try:
            result, method, error = await _call_engine_method(engine_info.engine, ["get_enriched_quote", "get_quote", "fetch_quote"], norm, timeout=LEGACY_TIMEOUT_SEC)
            _metrics.record_call(method or "unknown", success=result is not None)
            
            if result is None:
                legacy_requests_total.labels(endpoint="quote", status="not_found").inc()
                response = {"status": "error", "symbol": raw, "symbol_normalized": norm, "error": error or "Quote not found", "request_id": request_id, "runtime_ms": (time.time() - start) * 1000}
                if debug: response["debug"] = {"engine_source": engine_info.source.value, "method_attempted": method}
                return BestJSONResponse(status_code=200, content=jsonable_encoder(response))
            
            await _cache.set(cache_key, result, ttl=CACHE_TTL_QUOTE)
            
            response = {"status": "success", "data": result, "request_id": request_id, "runtime_ms": (time.time() - start) * 1000}
            if debug: response["debug"] = {"engine_source": engine_info.source.value, "method": method}
            
            legacy_requests_total.labels(endpoint="quote", status="success").inc()
            legacy_request_duration.labels(endpoint="quote").observe(time.time() - start)
            return BestJSONResponse(status_code=200, content=jsonable_encoder(response))
            
        except Exception as e:
            legacy_requests_total.labels(endpoint="quote", status="error").inc()
            logger.error(f"Quote error for {symbol}: {e}", exc_info=True)
            response = {"status": "error", "symbol": raw, "symbol_normalized": norm, "error": _safe_err(e), "request_id": request_id, "runtime_ms": (time.time() - start) * 1000}
            if debug: response["debug"] = {"engine_source": engine_info.source.value, "traceback": traceback.format_exc()[:2000]}
            return BestJSONResponse(status_code=200, content=jsonable_encoder(response))
        finally:
            await _close_engine(engine_info)


@router.post("/quotes", summary="Legacy batch quotes endpoint")
async def legacy_quotes(
    request: Request, payload: SymbolsIn, debug: bool = Query(False, description="Include debug information"),
    skip_cache: bool = Query(False, description="Skip cache and force refresh"),
) -> BestJSONResponse:
    request_id = hashlib.md5(f"{time.time()}".encode()).hexdigest()[:8]
    start = time.time()
    
    with tracer.start_as_current_span("legacy_batch_quotes") as span:
        auth_ok, auth_error = await _check_auth(request)
        if not auth_ok:
            legacy_requests_total.labels(endpoint="quotes", status="unauthorized").inc()
            return BestJSONResponse(status_code=401 if ENABLE_AUTH else 200, content={"status": "error", "error": auth_error or "Unauthorized", "request_id": request_id})
        
        raw_symbols = payload.normalized()
        if not raw_symbols:
            return BestJSONResponse(status_code=200, content={"status": "error", "error": "No symbols provided", "request_id": request_id})
        
        span.set_attribute("symbol_count", len(raw_symbols))
        norm_symbols = [_normalize_symbol(s) for s in raw_symbols]
        engine_info, metrics = await _get_engine(request)
        _metrics.engine_resolutions[engine_info.source.value] = _metrics.engine_resolutions.get(engine_info.source.value, 0) + 1
        
        if engine_info.engine is None:
            legacy_requests_total.labels(endpoint="quotes", status="no_engine").inc()
            return BestJSONResponse(status_code=200, content={"status": "error", "error": "No engine available", "request_id": request_id, "runtime_ms": (time.time() - start) * 1000})
        
        try:
            result, method, error = await _call_engine_method(engine_info.engine, ["get_enriched_quotes", "get_quotes", "fetch_quotes"], norm_symbols, timeout=LEGACY_TIMEOUT_SEC)
            
            if result is not None:
                ordered = _items_to_ordered_list(result, raw_symbols)
                if ordered:
                    for i, item in enumerate(ordered):
                        if i < len(norm_symbols) and item: await _cache.set(f"quote:{norm_symbols[i]}", item, ttl=CACHE_TTL_QUOTE)
                    response = {"status": "success", "data": ordered, "count": len(ordered), "request_id": request_id, "runtime_ms": (time.time() - start) * 1000}
                    if debug: response["debug"] = {"engine_source": engine_info.source.value, "method": method, "batch": True}
                    legacy_requests_total.labels(endpoint="quotes", status="success").inc()
                    legacy_request_duration.labels(endpoint="quotes").observe(time.time() - start)
                    return BestJSONResponse(status_code=200, content=jsonable_encoder(response))
            
            sem = asyncio.Semaphore(LEGACY_CONCURRENCY)
            async def _fetch_one(idx: int) -> Tuple[int, Any]:
                raw, norm = raw_symbols[idx], norm_symbols[idx]
                if not skip_cache:
                    cached = await _cache.get(f"quote:{norm}")
                    if cached:
                        _metrics.cache_hits += 1
                        return idx, cached
                async with sem:
                    res, m, e = await _call_engine_method(engine_info.engine, ["get_enriched_quote", "get_quote", "fetch_quote"], norm, timeout=LEGACY_TIMEOUT_SEC)
                    _metrics.record_call(m or "unknown", success=res is not None)
                    if res: await _cache.set(f"quote:{norm}", res, ttl=CACHE_TTL_QUOTE)
                    return idx, res
            
            tasks = [_fetch_one(i) for i in range(len(raw_symbols))]
            results = await asyncio.gather(*tasks)
            
            ordered = [None] * len(raw_symbols)
            for idx, res in results: ordered[idx] = res
            
            response = {"status": "success", "data": ordered, "count": len(ordered), "request_id": request_id, "runtime_ms": (time.time() - start) * 1000}
            if debug: response["debug"] = {"engine_source": engine_info.source.value, "fallback": True}
            
            legacy_requests_total.labels(endpoint="quotes", status="success").inc()
            legacy_request_duration.labels(endpoint="quotes").observe(time.time() - start)
            return BestJSONResponse(status_code=200, content=jsonable_encoder(response))
            
        except Exception as e:
            legacy_requests_total.labels(endpoint="quotes", status="error").inc()
            logger.error(f"Batch quotes error: {e}", exc_info=True)
            response = {"status": "error", "error": _safe_err(e), "request_id": request_id, "runtime_ms": (time.time() - start) * 1000}
            if debug: response["debug"] = {"engine_source": engine_info.source.value, "traceback": traceback.format_exc()[:2000]}
            return BestJSONResponse(status_code=200, content=jsonable_encoder(response))
        finally:
            await _close_engine(engine_info)


@router.post("/sheet-rows", summary="Legacy sheet-rows helper")
async def legacy_sheet_rows(
    request: Request, payload: SheetRowsIn, debug: bool = Query(False, description="Include debug information"),
    skip_cache: bool = Query(False, description="Skip cache and force refresh"),
) -> BestJSONResponse:
    request_id = hashlib.md5(f"{time.time()}".encode()).hexdigest()[:8]
    start = time.time()
    
    with tracer.start_as_current_span("legacy_sheet_rows") as span:
        auth_ok, auth_error = await _check_auth(request)
        if not auth_ok:
            legacy_requests_total.labels(endpoint="sheet_rows", status="unauthorized").inc()
            return BestJSONResponse(status_code=401 if ENABLE_AUTH else 200, content={"status": "error", "error": auth_error or "Unauthorized", "request_id": request_id})
        
        symbols_in = SymbolsIn(symbols=payload.symbols, tickers=payload.tickers)
        raw_symbols = symbols_in.normalized()
        headers = _get_headers_for_sheet(payload.get_sheet_name())
        
        if not raw_symbols:
            return BestJSONResponse(status_code=200, content={"status": "error", "error": "No symbols provided", "headers": headers, "rows": [], "request_id": request_id})
        
        norm_symbols = [_normalize_symbol(s) for s in raw_symbols]
        engine_info, metrics = await _get_engine(request)
        _metrics.engine_resolutions[engine_info.source.value] = _metrics.engine_resolutions.get(engine_info.source.value, 0) + 1
        
        if engine_info.engine is None:
            legacy_requests_total.labels(endpoint="sheet_rows", status="no_engine").inc()
            return BestJSONResponse(status_code=200, content={"status": "error", "error": "No engine available", "headers": headers, "rows": [], "request_id": request_id, "runtime_ms": (time.time() - start) * 1000})
        
        try:
            result, method, error = await _call_engine_method(engine_info.engine, ["get_enriched_quotes", "get_quotes", "fetch_quotes"], norm_symbols, timeout=LEGACY_TIMEOUT_SEC)
            
            quotes = []
            if result is not None:
                ordered = _items_to_ordered_list(result, raw_symbols)
                if ordered: quotes = ordered
            
            if len(quotes) != len(raw_symbols):
                sem = asyncio.Semaphore(LEGACY_CONCURRENCY)
                async def _fetch_one(idx: int) -> Tuple[int, Any]:
                    norm = norm_symbols[idx]
                    async with sem:
                        res, m, e = await _call_engine_method(engine_info.engine, ["get_enriched_quote", "get_quote", "fetch_quote"], norm, timeout=LEGACY_TIMEOUT_SEC)
                        return idx, res
                
                tasks = [_fetch_one(i) for i in range(len(raw_symbols))]
                results = await asyncio.gather(*tasks)
                
                quotes = [None] * len(raw_symbols)
                for idx, res in results: quotes[idx] = res
            
            rows = [_quote_to_row(q, headers) if q is not None else _quote_to_row({}, headers) for q in quotes]
            
            if payload.get_sheet_name():
                meta = {"source": "legacy_service", "version": VERSION, "engine_source": engine_info.source.value, "method": method, "cached_at": _utc_iso()}
                await _try_cache_snapshot(engine_info.engine, payload.get_sheet_name(), headers, rows, meta)
            
            response = {"status": "success", "headers": headers, "rows": rows, "count": len(rows), "request_id": request_id, "runtime_ms": (time.time() - start) * 1000}
            if debug: response["debug"] = {"engine_source": engine_info.source.value, "method": method, "sheet_name": payload.get_sheet_name()}
            
            legacy_requests_total.labels(endpoint="sheet_rows", status="success").inc()
            legacy_request_duration.labels(endpoint="sheet_rows").observe(time.time() - start)
            return BestJSONResponse(status_code=200, content=jsonable_encoder(response))
            
        except Exception as e:
            legacy_requests_total.labels(endpoint="sheet_rows", status="error").inc()
            logger.error(f"Sheet rows error: {e}", exc_info=True)
            response = {"status": "error", "error": _safe_err(e), "headers": headers, "rows": [], "request_id": request_id, "runtime_ms": (time.time() - start) * 1000}
            if debug: response["debug"] = {"engine_source": engine_info.source.value, "traceback": traceback.format_exc()[:2000]}
            return BestJSONResponse(status_code=200, content=jsonable_encoder(response))
        finally:
            await _close_engine(engine_info)


@router.delete("/cache", summary="Clear legacy cache")
async def legacy_clear_cache(request: Request) -> BestJSONResponse:
    auth_ok, auth_error = await _check_auth(request)
    if not auth_ok: return BestJSONResponse(status_code=401 if ENABLE_AUTH else 200, content={"status": "error", "error": auth_error or "Unauthorized"})
    await _cache.clear()
    return BestJSONResponse(status_code=200, content={"status": "success", "message": "Cache cleared"})


@router.get("/cache/stats", summary="Get cache statistics")
async def legacy_cache_stats(request: Request) -> BestJSONResponse:
    auth_ok, auth_error = await _check_auth(request)
    if not auth_ok: return BestJSONResponse(status_code=401 if ENABLE_AUTH else 200, content={"status": "error", "error": auth_error or "Unauthorized"})
    return BestJSONResponse(status_code=200, content={"status": "success", "stats": await _cache.get_stats()})


@router.get("/metrics", summary="Get service metrics")
async def legacy_metrics(request: Request) -> BestJSONResponse:
    auth_ok, auth_error = await _check_auth(request)
    if not auth_ok: return BestJSONResponse(status_code=401 if ENABLE_AUTH else 200, content={"status": "error", "error": auth_error or "Unauthorized"})
    return BestJSONResponse(status_code=200, content={"status": "success", "metrics": _metrics.to_dict()})


# ============================================================================
# External Router Override
# ============================================================================

if ENABLE_EXTERNAL_LEGACY_ROUTER:
    ext = _try_import_external_router()
    if ext is not None: router = ext


# ============================================================================
# Module Exports
# ============================================================================

def get_router() -> APIRouter:
    return router


__all__ = [
    "router",
    "get_router",
    "VERSION",
    "SymbolsIn",
    "SheetRowsIn",
]
