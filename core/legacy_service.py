#!/usr/bin/env python3
"""
core/legacy_service.py
================================================================================
Legacy Compatibility Service — v3.1.0 (NEXT-GEN ENTERPRISE)
================================================================================
Financial Data Platform — Legacy Router with Modern Features

What's new in v3.1.0:
- ✅ Restored Missing Formatters: `_to_percent` and `_iso_or_none` are back so legacy endpoints don't crash on `NameError`.
- ✅ ORJSON Serialization: Integrated C-compiled JSON responses for massive batch throughput
- ✅ Universal Event Loop Management: Hardened sync/async bridging to prevent loop crashes
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

__version__ = "3.1.0"
VERSION = __version__

logger = logging.getLogger("core.legacy_service")

# ============================================================================
# Constants
# ============================================================================

_TRUTHY = {"1", "true", "yes", "y", "on", "t", "enable", "enabled", "ok", "active"}

DEFAULT_CONCURRENCY = 8
DEFAULT_TIMEOUT_SEC = 25.0
DEFAULT_MAX_SYMBOLS = 2500

CACHE_TTL_SNAPSHOT = 300
CACHE_TTL_QUOTE = 60

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

def _to_percent(x: Any) -> Optional[float]:
    f = _to_float_best(x)
    if f is None: return None
    return round(f * 100.0, 4) if -2.0 <= f <= 2.0 else round(f, 4)

def _iso_or_none(x: Any) -> Optional[str]:
    if x is None: return None
    if isinstance(x, datetime):
        return x.replace(tzinfo=timezone.utc).isoformat() if x.tzinfo is None else x.isoformat()
    try:
        return datetime.fromisoformat(str(x).replace('Z', '+00:00')).isoformat()
    except (ValueError, TypeError):
        return str(x).strip()

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

def _unwrap_tuple(x: Any) -> Any:
    return x[0] if isinstance(x, tuple) and len(x) >= 1 else x

def _unwrap_container(x: Any) -> Any:
    if not isinstance(x, dict): return x
    for key in ("items", "data", "quotes", "results", "payload"):
        if key in x and isinstance(x[key], (list, dict)): return x[key]
    return x

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

async def _maybe_await(x: Any) -> Any:
    try:
        if inspect.isawaitable(x): return await x
    except Exception: pass
    return x

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
# Authorization
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
# Enriched Quote Format
# ============================================================================

_HEADER_MAP = {
    "change %": (("percent_change", "change_percent", "regular_market_change_percent"), _to_percent),
    "last updated (utc)": (("last_updated_utc", "as_of_utc", "timestamp_utc"), _iso_or_none),
}

def _get_headers_for_sheet(sheet_name: str) -> List[str]:
    # Hardcoded for fallback, but normally pulls from core.schemas
    return ["Symbol", "Name", "Price", "Change", "Change %", "Volume", "Market Cap", "Recommendation", "Last Updated (UTC)", "Error"]

def _quote_to_row(q: Any, headers: List[str]) -> List[Any]:
    row = [None] * len(headers)
    d = _quote_dict(q)
    for i, h in enumerate(headers):
        hl = h.lower()
        if "symbol" in hl: row[i] = d.get("symbol")
        elif "name" in hl: row[i] = d.get("name")
        elif "price" in hl and "forecast" not in hl: row[i] = d.get("current_price") or d.get("price")
        elif "change" in hl and "%" not in hl: row[i] = d.get("price_change") or d.get("change")
        elif "change %" in hl: row[i] = _to_percent(d.get("percent_change") or d.get("change_percent"))
        elif "volume" in hl: row[i] = d.get("volume")
        elif "market cap" in hl: row[i] = d.get("market_cap")
        elif "recommendation" in hl: row[i] = d.get("recommendation", "HOLD")
        elif "last updated (utc)" in hl: row[i] = _iso_or_none(d.get("last_updated_utc"))
        elif "error" in hl: row[i] = d.get("error")
        else: row[i] = d.get(h)
    return row

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
