#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
core/legacy_service.py
================================================================================
Legacy Compatibility Service -- v4.1.0 (EVENT-LOOP-SAFE / SCHEMA-ALIGNED)
================================================================================
Financial Data Platform — Legacy Router with Modern Features

v4.1.0 changes (what moved from v4.0.0)
---------------------------------------
- CRITICAL FIX: `_call_engine_method` no longer spawns a worker thread per
  async call. v4.0.0 was inside an `async def`, already on a running event
  loop, yet called `threading.Thread(...).start(); t.join()` for each
  coroutine method — which (a) blocked the caller's event loop (so one
  request stalls every other request on the same worker), and (b) ran the
  engine coroutine on a *foreign* event loop, which breaks any engine that
  holds loop-bound state (aiohttp sessions, asyncio locks — `data_engine_v2`
  has both). v4.1.0 just `await`s the coroutine, with `asyncio.wait_for`
  for the timeout.
- ALIGN: `_get_headers_for_sheet` now pulls the authoritative headers from
  `core.sheets.schema_registry` and returns a paired `(headers, keys)`
  tuple. v4.0.0 hard-coded a 10-column fallback and never consulted the
  registry for any sheet — Market_Leaders calls returned 10 cols instead
  of the registry's 80, Insights_Analysis got the same 10 cols instead of 7,
  Top_10 got 10 instead of 83, etc.
- ALIGN: `_quote_to_row` now accepts `keys` (not headers) and uses
  canonical-key lookup with alias fallback. v4.0.0 substring-matched header
  names (`"price" in hl`, `"change" in hl`, …) for a hardcoded set of 10
  fields, with `else: d.get(header)` for the remainder — but quote dicts use
  snake_case keys, so any non-matching header (most of the canonical 80)
  returned `None`.
- FIX: `_get_engine` no longer allocates and returns a throwaway
  `ServiceMetrics`. It updates the module-level `_metrics` directly, which
  is what callers observe. v4.0.0's returned metrics object was dead code.
- FIX: cache-miss counter only increments when we actually performed a
  lookup. v4.0.0 bumped `_metrics.cache_misses` even when `skip_cache=True`,
  producing phantom misses.
- FIX: request IDs now use `uuid4().hex[:8]` instead of
  `md5(str(time.time()))[:8]`, eliminating collisions between requests
  arriving in the same millisecond.
- FIX: external-router self-detection uses `os.path.samefile` against
  `__file__` instead of path-suffix string match, so relocating the module
  doesn't break it.
- CLEANUP: dead `start` variable in `_get_engine` removed.

Public API is preserved: `router`, `get_router`, `VERSION`, `SymbolsIn`,
`SheetRowsIn`. All endpoint paths and shapes are unchanged.
================================================================================
"""

from __future__ import annotations

import asyncio
import hashlib
import importlib
import inspect
import json
import logging
import math
import os
import threading
import time
import traceback
import uuid
from contextlib import asynccontextmanager
from dataclasses import asdict, dataclass, field
from datetime import datetime, timedelta, timezone
from enum import Enum
from typing import (
    Any,
    AsyncGenerator,
    Callable,
    Dict,
    List,
    Optional,
    Sequence,
    Set,
    Tuple,
    Type,
    TypeVar,
    Union,
    cast,
)

from fastapi import APIRouter, HTTPException, Query, Request
from fastapi.encoders import jsonable_encoder
from pydantic import BaseModel, Field, validator


# ---------------------------------------------------------------------------
# High-Performance JSON Support
# ---------------------------------------------------------------------------

try:
    from fastapi.responses import ORJSONResponse as BestJSONResponse
    import orjson

    def _json_dumps(obj: Any) -> str:
        return orjson.dumps(obj).decode("utf-8")

    _HAS_ORJSON = True
except ImportError:
    from starlette.responses import JSONResponse as BestJSONResponse

    def _json_dumps(obj: Any) -> str:
        return json.dumps(obj, default=str)

    _HAS_ORJSON = False


# ---------------------------------------------------------------------------
# Logging Setup
# ---------------------------------------------------------------------------

logger = logging.getLogger(__name__)
logger.addHandler(logging.NullHandler())


# ---------------------------------------------------------------------------
# Version
# ---------------------------------------------------------------------------

__version__ = "4.1.0"
VERSION = __version__


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

_TRUTHY = {"1", "true", "yes", "y", "on", "t", "enable", "enabled", "ok", "active"}

DEFAULT_CONCURRENCY = 8
DEFAULT_TIMEOUT_SEC = 25.0
DEFAULT_MAX_SYMBOLS = 2500

CACHE_TTL_SNAPSHOT = 300
CACHE_TTL_QUOTE = 60

# Riyadh timezone (UTC+3)
_RIYADH_TZ = timezone(timedelta(hours=3))


# ---------------------------------------------------------------------------
# Custom Exceptions
# ---------------------------------------------------------------------------

class LegacyServiceError(Exception):
    """Base exception for legacy service."""
    pass


class EngineResolutionError(LegacyServiceError):
    """Raised when engine cannot be resolved."""
    pass


class AuthenticationError(LegacyServiceError):
    """Raised when authentication fails."""
    pass


# ---------------------------------------------------------------------------
# Enums
# ---------------------------------------------------------------------------

class EngineSource(Enum):
    """Engine source types."""
    APP_STATE = "app.state.engine"
    V2_SINGLETON = "core.data_engine_v2.get_engine"
    V2_TEMP = "core.data_engine_v2.temp"
    V1_TEMP = "core.data_engine.temp"
    NONE = "none"


class CallStatus(Enum):
    """Call status types."""
    SUCCESS = "success"
    FAILED = "failed"
    TIMEOUT = "timeout"
    NOT_FOUND = "not_found"
    EXCEPTION = "exception"


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class LegacyConfig:
    """Configuration for legacy service."""
    enabled: bool = True
    concurrency: int = DEFAULT_CONCURRENCY
    timeout_sec: float = DEFAULT_TIMEOUT_SEC
    max_symbols: int = DEFAULT_MAX_SYMBOLS
    enable_auth: bool = False
    debug_errors: bool = False
    log_external_import_failure: bool = False

    @classmethod
    def from_env(cls) -> "LegacyConfig":
        """Load configuration from environment."""
        def _env_bool(name: str, default: bool = False) -> bool:
            v = os.getenv(name)
            if v is None:
                return default
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

        return cls(
            enabled=_env_bool("ENABLE_EXTERNAL_LEGACY_ROUTER", False),
            concurrency=max(1, min(50, _env_int("LEGACY_CONCURRENCY", DEFAULT_CONCURRENCY))),
            timeout_sec=max(3.0, min(90.0, _env_float("LEGACY_TIMEOUT_SEC", DEFAULT_TIMEOUT_SEC))),
            max_symbols=max(50, min(10000, _env_int("LEGACY_MAX_SYMBOLS", DEFAULT_MAX_SYMBOLS))),
            enable_auth=_env_bool("ENABLE_LEGACY_AUTH", False),
            debug_errors=_env_bool("DEBUG_ERRORS", False),
            log_external_import_failure=_env_bool("LOG_EXTERNAL_LEGACY_IMPORT_FAILURE", False),
        )


_CONFIG = LegacyConfig.from_env()


# ---------------------------------------------------------------------------
# Data Classes
# ---------------------------------------------------------------------------

@dataclass(slots=True)
class MethodCall:
    """Record of a method call."""
    method: str
    status: CallStatus
    duration_ms: float
    error: Optional[str] = None
    timestamp: datetime = field(default_factory=lambda: datetime.now(timezone.utc))


@dataclass(slots=True)
class EngineInfo:
    """Information about an engine instance."""
    engine: Any
    source: EngineSource
    should_close: bool
    method_calls: List[MethodCall] = field(default_factory=list)

    def add_call(
        self,
        method: str,
        status: CallStatus,
        duration_ms: float,
        error: Optional[str] = None,
    ) -> None:
        """Record a method call."""
        self.method_calls.append(MethodCall(
            method=method,
            status=status,
            duration_ms=duration_ms,
            error=error,
        ))


@dataclass(slots=True)
class ServiceMetrics:
    """Service metrics for monitoring."""
    total_requests: int = 0
    cache_hits: int = 0
    cache_misses: int = 0
    engine_resolutions: Dict[str, int] = field(default_factory=dict)
    method_calls: Dict[str, int] = field(default_factory=dict)
    method_errors: Dict[str, int] = field(default_factory=dict)
    start_time: datetime = field(default_factory=lambda: datetime.now(timezone.utc))

    def record_engine(self, source: EngineSource) -> None:
        """Record an engine resolution."""
        key = source.value
        self.engine_resolutions[key] = self.engine_resolutions.get(key, 0) + 1

    def record_call(self, method: str, success: bool = True) -> None:
        """Record a method call."""
        self.total_requests += 1
        self.method_calls[method] = self.method_calls.get(method, 0) + 1
        if not success:
            self.method_errors[method] = self.method_errors.get(method, 0) + 1

    def to_dict(self) -> Dict[str, Any]:
        """Convert metrics to dictionary."""
        uptime = (datetime.now(timezone.utc) - self.start_time).total_seconds()
        total = self.cache_hits + self.cache_misses
        return {
            "total_requests": self.total_requests,
            "cache_hits": self.cache_hits,
            "cache_misses": self.cache_misses,
            "hit_rate": (self.cache_hits / total) if total > 0 else 0,
            "engine_resolutions": dict(self.engine_resolutions),
            "method_calls": dict(self.method_calls),
            "method_errors": dict(self.method_errors),
            "uptime_seconds": round(uptime, 2),
        }


# ---------------------------------------------------------------------------
# Pydantic Models
# ---------------------------------------------------------------------------

class SymbolsIn(BaseModel):
    """Symbols input model."""
    symbols: List[str] = Field(default_factory=list, description="List of symbols")
    tickers: List[str] = Field(default_factory=list, description="Alias for symbols")

    @validator("symbols", "tickers", pre=True)
    def _validate_list(cls, v: Any) -> List[str]:
        if v is None:
            return []
        if isinstance(v, str):
            return [s.strip() for s in v.split(",") if s.strip()]
        if isinstance(v, (list, tuple)):
            return [str(s).strip() for s in v if s]
        return []

    def normalized(self) -> List[str]:
        """Get normalized symbols."""
        items = self.symbols or self.tickers or []
        result: List[str] = []
        seen: Set[str] = set()
        for x in items[:_CONFIG.max_symbols]:
            s = str(x or "").strip()
            if not s:
                continue
            su = s.upper()
            if su in seen:
                continue
            seen.add(su)
            result.append(s)
        return result


class SheetRowsIn(BaseModel):
    """Sheet rows input model."""
    symbols: List[str] = Field(default_factory=list, description="List of symbols")
    tickers: List[str] = Field(default_factory=list, description="Alias for symbols")
    sheet_name: str = Field("", description="Sheet name for caching")
    sheetName: str = Field("", description="Alias for sheet_name")

    @validator("sheet_name", "sheetName", pre=True)
    def _validate_sheet_name(cls, v: Any) -> str:
        return str(v).strip() if v is not None else ""

    def get_sheet_name(self) -> str:
        """Get effective sheet name."""
        return self.sheet_name or self.sheetName or ""


class CacheControl(BaseModel):
    """Cache control model."""
    ttl: int = Field(CACHE_TTL_SNAPSHOT, description="Cache TTL in seconds")
    skip_cache: bool = Field(False, description="Skip cache and force refresh")


# ---------------------------------------------------------------------------
# Pure Utility Functions
# ---------------------------------------------------------------------------

def _utc_now() -> datetime:
    """Get current UTC time."""
    return datetime.now(timezone.utc)


def _utc_iso() -> str:
    """Get current UTC time in ISO format."""
    return _utc_now().isoformat()


def _riyadh_now() -> datetime:
    """Get current Riyadh time."""
    return datetime.now(_RIYADH_TZ)


def _riyadh_iso() -> str:
    """Get current Riyadh time in ISO format."""
    return _riyadh_now().isoformat()


def _new_request_id() -> str:
    """Collision-safe short request id."""
    return uuid.uuid4().hex[:8]


def _safe_str(value: Any, default: str = "") -> str:
    """Safely convert to string."""
    try:
        return str(value).strip() if value is not None else default
    except Exception:
        return default


def _safe_int(value: Any, default: Optional[int] = None) -> Optional[int]:
    """Safely convert to integer."""
    try:
        return int(float(str(value).strip())) if value is not None else default
    except (ValueError, TypeError):
        return default


def _safe_float(value: Any, default: Optional[float] = None) -> Optional[float]:
    """Safely convert to float."""
    try:
        if value is None:
            return default
        return float(str(value).strip().replace(",", ""))
    except (ValueError, TypeError):
        return default


def _safe_err(exc: BaseException) -> str:
    """Safely convert exception to string."""
    msg = str(exc).strip()
    return msg or exc.__class__.__name__


def _to_float_best(value: Any) -> Optional[float]:
    """Best-effort conversion to float."""
    if value is None or isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        try:
            return float(value)
        except Exception:
            return None
    s = _safe_str(value)
    if not s or s.lower() in {"na", "n/a", "null", "none", "-", "—", ""}:
        return None
    if s.endswith("%"):
        s = s[:-1].strip()
    try:
        return float(s.replace(",", ""))
    except Exception:
        return None


def _to_percent(value: Any) -> Optional[float]:
    """Convert value to percentage points."""
    f = _to_float_best(value)
    if f is None:
        return None
    if -2.0 <= f <= 2.0:
        return round(f * 100.0, 4)
    return round(f, 4)


def _iso_or_none(value: Any) -> Optional[str]:
    """Convert to ISO format or None."""
    if value is None:
        return None
    if isinstance(value, datetime):
        if value.tzinfo is None:
            value = value.replace(tzinfo=timezone.utc)
        return value.isoformat()
    try:
        dt = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
        return dt.isoformat()
    except (ValueError, TypeError):
        return _safe_str(value)


def _normalize_symbol(symbol: str) -> str:
    """Normalize symbol to canonical format."""
    s = _safe_str(symbol).upper().replace(" ", "")
    if not s:
        return ""
    if s.endswith(".SA"):
        s = s[:-3] + ".SR"
    try:
        from core.data_engine_v2 import normalize_symbol as _norm
        try:
            return _safe_str(_norm(s)) or s
        except Exception:
            return s
    except ImportError:
        return s


def _quote_dict(quote: Any) -> Dict[str, Any]:
    """Convert quote to dictionary."""
    if quote is None:
        return {}
    if isinstance(quote, dict):
        return quote
    try:
        if hasattr(quote, "model_dump"):
            return quote.model_dump()
        if hasattr(quote, "dict"):
            return quote.dict()
        return dict(getattr(quote, "__dict__", {}) or {})
    except Exception:
        return {}


def _extract_symbol_from_quote(quote: Any) -> str:
    """Extract symbol from quote."""
    d = _quote_dict(quote)
    for key in ("symbol_normalized", "symbol", "ticker", "requested_symbol"):
        if key in d and d[key]:
            return _safe_str(d[key]).upper()
    return ""


def _items_to_ordered_list(items: Any, symbols: List[str]) -> List[Any]:
    """Convert items to ordered list matching symbols order."""
    if items is None:
        return []

    if isinstance(items, list):
        sym_map: Dict[str, Any] = {}
        for it in items:
            sym = _extract_symbol_from_quote(it)
            if sym:
                sym_map[sym] = it
        if sym_map:
            ordered = [sym_map.get(_safe_str(s).upper()) for s in symbols]
            if any(x is not None for x in ordered):
                return ordered
        return items

    if isinstance(items, dict):
        mapping: Dict[str, Any] = {}
        for k, v in items.items():
            key = _safe_str(k).upper()
            if key:
                mapping[key] = v
            sym = _extract_symbol_from_quote(v)
            if sym and sym not in mapping:
                mapping[sym] = v
        ordered = [mapping.get(_safe_str(s).upper()) for s in symbols]
        if any(x is not None for x in ordered):
            return ordered
        return list(items.values())

    if len(symbols) == 1:
        return [items]

    return []


def _unwrap_tuple(value: Any) -> Any:
    """Unwrap tuple if needed."""
    return value[0] if isinstance(value, tuple) and len(value) >= 1 else value


def _unwrap_container(value: Any) -> Any:
    """Unwrap container if needed."""
    if not isinstance(value, dict):
        return value
    for key in ("items", "data", "quotes", "results", "payload"):
        if key in value and isinstance(value[key], (list, dict)):
            return value[key]
    return value


# ---------------------------------------------------------------------------
# Computed Fields
# ---------------------------------------------------------------------------

def _compute_52w_position(price: Any, low: Any, high: Any) -> Optional[float]:
    """Compute 52-week position percentage."""
    p = _safe_float(price)
    l = _safe_float(low)
    h = _safe_float(high)
    if p is None or l is None or h is None or h == l:
        return None
    return round(((p - l) / (h - l)) * 100.0, 4)


def _compute_turnover(volume: Any, shares: Any) -> Optional[float]:
    """Compute turnover percentage."""
    v = _safe_float(volume)
    s = _safe_float(shares)
    if v is None or s is None or s == 0:
        return None
    return round((v / s) * 100.0, 4)


def _compute_value_traded(price: Any, volume: Any) -> Optional[float]:
    """Compute value traded."""
    p = _safe_float(price)
    v = _safe_float(volume)
    if p is None or v is None:
        return None
    return round(p * v, 4)


def _compute_free_float_mcap(mcap: Any, free_float: Any) -> Optional[float]:
    """Compute free float market cap."""
    m = _safe_float(mcap)
    f = _safe_float(free_float)
    if m is None or f is None:
        return None
    if f > 1.5:
        f = f / 100.0
    return m * max(0.0, min(1.0, f))


def _compute_liquidity_score(value_traded: Any) -> Optional[float]:
    """Compute liquidity score."""
    vt = _safe_float(value_traded)
    if vt is None or vt <= 0:
        return None
    try:
        log_val = math.log10(vt)
        score = ((log_val - 6.0) / (11.0 - 6.0)) * 100.0
        return max(0.0, min(100.0, round(score, 2)))
    except Exception:
        return None


def _compute_change_and_pct(price: Any, prev: Any) -> Tuple[Optional[float], Optional[float]]:
    """Compute price change and percentage change."""
    p = _safe_float(price)
    pc = _safe_float(prev)
    if p is None or pc is None or pc == 0:
        return None, None
    change = round(p - pc, 6)
    pct = round((p / pc - 1.0) * 100.0, 6)
    return change, pct


def _compute_fair_value(payload: Dict[str, Any]) -> Tuple[Optional[float], Optional[float], Optional[str]]:
    """Compute fair value and label."""
    price = _safe_float(payload.get("current_price") or payload.get("price"))
    if price is None or price <= 0:
        return None, None, None

    fair = (
        _safe_float(payload.get("fair_value")) or
        _safe_float(payload.get("target_price")) or
        _safe_float(payload.get("forecast_price_3m")) or
        _safe_float(payload.get("forecast_price_12m")) or
        _safe_float(payload.get("ma200")) or
        _safe_float(payload.get("ma50"))
    )
    if fair is None or fair <= 0:
        return None, None, None

    upside = round((fair / price - 1.0) * 100.0, 4)
    if upside >= 20:
        label = "Undervalued"
    elif upside >= 10:
        label = "Moderately Undervalued"
    elif upside >= -10:
        label = "Fairly Valued"
    elif upside >= -20:
        label = "Moderately Overvalued"
    else:
        label = "Overvalued"

    return fair, upside, label


def _compute_data_quality(payload: Dict[str, Any]) -> str:
    """Compute data quality score."""
    if _safe_float(payload.get("current_price") or payload.get("price")) is None:
        return "MISSING"

    categories = {
        "identity": {"symbol", "name", "sector", "market"},
        "price": {"current_price", "previous_close", "volume"},
        "fundamentals": {"market_cap", "pe_ttm", "eps_ttm"},
    }

    score = 0
    total = 0

    for fields in categories.values():
        hits = sum(1 for f in fields if f in payload and payload[f] is not None)
        tot = len(fields)
        if tot > 0:
            score += hits * 10
            total += tot * 10

    if total == 0:
        return "POOR"

    pct = (score / total) * 100
    if pct >= 70:
        return "EXCELLENT"
    if pct >= 50:
        return "GOOD"
    if pct >= 30:
        return "FAIR"
    return "POOR"


# ---------------------------------------------------------------------------
# Thread-Safe Cache
# ---------------------------------------------------------------------------

@dataclass(slots=True)
class CacheEntry:
    """Cache entry with metadata."""
    data: Any
    expires_at: datetime
    created_at: datetime = field(default_factory=_utc_now)
    last_access: float = field(default_factory=time.time)

    @property
    def is_expired(self) -> bool:
        """Check if cache entry is expired."""
        return _utc_now() > self.expires_at


class SmartCache:
    """Async cache with TTL and LRU eviction.

    Note: `asyncio.Lock` is task-cooperative within a single event loop, not
    cross-thread safe. This is correct for the intended single-event-loop
    FastAPI usage. The v4.0.0 `_call_engine_method` path that ran methods on
    foreign event loops would have corrupted this cache under concurrency;
    v4.1.0 removes that path entirely.
    """

    def __init__(self, default_ttl: int = CACHE_TTL_SNAPSHOT, max_size: int = 5000):
        self._cache: Dict[str, CacheEntry] = {}
        self._lock = asyncio.Lock()
        self.default_ttl = default_ttl
        self.max_size = max_size
        self.hits = 0
        self.misses = 0

    async def get(self, key: str) -> Optional[Any]:
        """Get value from cache."""
        async with self._lock:
            if key in self._cache:
                entry = self._cache[key]
                if not entry.is_expired:
                    entry.last_access = time.time()
                    self.hits += 1
                    return entry.data
                del self._cache[key]
            self.misses += 1
            return None

    async def set(self, key: str, value: Any, ttl: Optional[int] = None) -> None:
        """Set value in cache."""
        async with self._lock:
            # Evict oldest if cache is full
            if len(self._cache) >= self.max_size and key not in self._cache:
                oldest = min(self._cache.items(), key=lambda x: x[1].last_access)[0]
                del self._cache[oldest]

            ttl_sec = ttl or self.default_ttl
            expires_at = _utc_now() + timedelta(seconds=ttl_sec)
            self._cache[key] = CacheEntry(data=value, expires_at=expires_at)

    async def delete(self, key: str) -> None:
        """Delete value from cache."""
        async with self._lock:
            self._cache.pop(key, None)

    async def clear(self) -> None:
        """Clear all cache entries."""
        async with self._lock:
            self._cache.clear()
            self.hits = 0
            self.misses = 0

    async def get_stats(self) -> Dict[str, Any]:
        """Get cache statistics."""
        async with self._lock:
            total = self.hits + self.misses
            return {
                "size": len(self._cache),
                "hits": self.hits,
                "misses": self.misses,
                "hit_rate": self.hits / total if total > 0 else 0,
                "default_ttl": self.default_ttl,
                "max_size": self.max_size,
            }


# ---------------------------------------------------------------------------
# Engine Discovery
# ---------------------------------------------------------------------------

async def _maybe_await(value: Any) -> Any:
    """Await if value is awaitable."""
    try:
        if inspect.isawaitable(value):
            return await value
    except Exception:
        pass
    return value


async def _call_engine_method(
    engine: Any,
    methods: List[str],
    *args: Any,
    timeout: float = DEFAULT_TIMEOUT_SEC,
    **kwargs: Any,
) -> Tuple[Optional[Any], Optional[str], Optional[str]]:
    """Call engine method with multiple attempts.

    v4.1.0: `await`s coroutine methods directly. v4.0.0 used a
    `threading.Thread + new event loop + t.join()` pattern which (a) blocked
    the caller's event loop, defeating async concurrency, and (b) ran engine
    coroutines on a foreign loop, breaking any engine with loop-bound state.
    """
    last_error: Optional[str] = None

    for method_name in methods:
        method = getattr(engine, method_name, None)
        if not callable(method):
            continue

        try:
            if inspect.iscoroutinefunction(method):
                result = await asyncio.wait_for(
                    method(*args, **kwargs), timeout=timeout
                )
            else:
                result = method(*args, **kwargs)
                # Defensive: a sync-looking method may still return an awaitable
                # (e.g., a @property exposing a Task). Await if so.
                if inspect.isawaitable(result):
                    result = await asyncio.wait_for(result, timeout=timeout)

            return result, method_name, None

        except asyncio.TimeoutError:
            last_error = f"Timeout after {timeout}s"
            continue
        except Exception as e:
            last_error = _safe_err(e)
            continue

    return None, None, last_error


async def _get_engine(request: Request) -> EngineInfo:
    """Resolve the data engine for this request.

    Updates the module-level `_metrics` directly rather than returning a
    throwaway ServiceMetrics. v4.0.0 allocated a fresh ServiceMetrics, called
    `record_engine()` on it, returned it to the caller, and the caller
    discarded it — so those records were never observed.
    """
    # Check app state
    try:
        eng = getattr(request.app.state, "engine", None)
        if eng is not None:
            _metrics.record_engine(EngineSource.APP_STATE)
            return EngineInfo(engine=eng, source=EngineSource.APP_STATE, should_close=False)
    except Exception:
        pass

    # Try V2 singleton
    try:
        from core.data_engine_v2 import get_engine as v2_get_engine
        eng2 = v2_get_engine()
        eng2 = await _maybe_await(eng2)
        if eng2 is not None:
            try:
                request.app.state.engine = eng2
            except Exception:
                pass
            _metrics.record_engine(EngineSource.V2_SINGLETON)
            return EngineInfo(engine=eng2, source=EngineSource.V2_SINGLETON, should_close=False)
    except Exception:
        pass

    # Try V2 class instantiation (last-resort fallback; bypasses the singleton)
    try:
        mod = importlib.import_module("core.data_engine_v2")
        engine_class = getattr(mod, "DataEngineV2", None) or getattr(mod, "DataEngine", None)
        if engine_class is not None:
            eng3 = engine_class()
            _metrics.record_engine(EngineSource.V2_TEMP)
            return EngineInfo(engine=eng3, source=EngineSource.V2_TEMP, should_close=True)
    except Exception:
        pass

    # Try V1 class
    try:
        from core.data_engine import DataEngine as V1Engine
        eng4 = V1Engine()
        _metrics.record_engine(EngineSource.V1_TEMP)
        return EngineInfo(engine=eng4, source=EngineSource.V1_TEMP, should_close=True)
    except Exception:
        pass

    _metrics.record_engine(EngineSource.NONE)
    return EngineInfo(engine=None, source=EngineSource.NONE, should_close=False)


async def _close_engine(engine_info: EngineInfo) -> None:
    """Close engine if needed."""
    if not engine_info.should_close or engine_info.engine is None:
        return

    try:
        aclose = getattr(engine_info.engine, "aclose", None)
        if callable(aclose):
            await _maybe_await(aclose())
            return
    except Exception:
        pass

    try:
        close = getattr(engine_info.engine, "close", None)
        if callable(close):
            close()
    except Exception:
        pass


# ---------------------------------------------------------------------------
# Authentication
# ---------------------------------------------------------------------------

async def _check_auth(request: Request) -> Tuple[bool, Optional[str]]:
    """Check authentication."""
    if not _CONFIG.enable_auth:
        return True, None

    try:
        from core.config import auth_ok
        api_key = request.headers.get("X-API-Key") or request.headers.get("Authorization")
        if not api_key:
            return False, "Missing API key"

        if not auth_ok(token=api_key, headers=dict(request.headers)):
            return False, "Invalid API key"

        return True, None
    except Exception as e:
        logger.warning("Auth check failed: %s", e)
        return False, "Authentication error"


# ---------------------------------------------------------------------------
# Sheet Rows Helpers (schema-aware)
# ---------------------------------------------------------------------------

# Minimal last-resort fallback, used only if the schema registry can't be
# imported AND the sheet name doesn't match a known sheet.
_FALLBACK_LEGACY_HEADERS: List[str] = [
    "Symbol", "Name", "Current Price", "Price Change", "Percent Change",
    "Volume", "Market Cap", "Recommendation", "Last Updated (UTC)", "Warnings",
]
_FALLBACK_LEGACY_KEYS: List[str] = [
    "symbol", "name", "current_price", "price_change", "percent_change",
    "volume", "market_cap", "recommendation", "last_updated_utc", "warnings",
]

# Alias hints for key-based row lookup. The authoritative key is the one
# coming from `schema_registry`; these are tried only when that key is
# missing from the quote dict.
# Legacy sheet-name aliases. The registry / page_catalog is the authoritative
# source; this map lets older clients keep using pre-rename names.
_LEGACY_SHEET_ALIASES: Dict[str, str] = {
    "my_investments":   "My_Portfolio",
    "my investments":   "My_Portfolio",
    "myinvestments":    "My_Portfolio",
    "myportfolio":      "My_Portfolio",
    "portfolio":        "My_Portfolio",
}


_FIELD_ALIAS_HINTS: Dict[str, Tuple[str, ...]] = {
    "symbol": ("ticker", "code", "symbol_normalized", "requested_symbol"),
    "name": ("long_name", "company_name", "instrument_name", "title"),
    "current_price": ("price", "last_price", "last", "close", "market_price", "nav"),
    "price_change": ("change", "net_change"),
    "percent_change": ("change_percent", "change_pct", "pct_change"),
    "volume": ("vol",),
    "market_cap": ("mcap", "market_capitalization"),
    "recommendation": ("rating", "signal", "action"),
    "last_updated_utc": ("updated_at_utc", "timestamp_utc", "last_updated"),
    "last_updated_riyadh": ("updated_at_riyadh", "timestamp_riyadh", "as_of_riyadh"),
    "warnings": ("errors", "error"),
}


def _get_headers_for_sheet(sheet_name: str) -> Tuple[List[str], List[str]]:
    """Return paired `(headers, keys)` for a sheet from the schema registry.

    v4.0.0 hard-coded a 10-column fallback regardless of `sheet_name`.
    v4.1.0 consults `core.sheets.schema_registry` so Market_Leaders gets its
    80 cols, Top_10_Investments gets 83, Insights gets 7, etc.
    """
    sheet = _safe_str(sheet_name)

    # Apply legacy aliases first (case-insensitive match)
    if sheet:
        aliased = _LEGACY_SHEET_ALIASES.get(sheet.lower())
        if aliased:
            sheet = aliased

    # Try page catalog normalization
    try:
        from core.sheets.page_catalog import normalize_page_name  # type: ignore
        try:
            sheet = normalize_page_name(sheet) or sheet
        except Exception:
            pass
    except ImportError:
        pass

    # Try the registry
    if sheet:
        for module_name in (
            "core.sheets.schema_registry",
            "core.schema_registry",
        ):
            try:
                mod = importlib.import_module(module_name)
            except ImportError:
                continue
            except Exception:
                continue

            getter = (
                getattr(mod, "get_sheet_spec", None)
                or getattr(mod, "get_page_spec", None)
            )
            if not callable(getter):
                continue

            try:
                spec = getter(sheet)
            except Exception:
                continue

            cols = getattr(spec, "columns", None)
            if not cols:
                continue

            headers: List[str] = []
            keys: List[str] = []
            for col in cols:
                h = _safe_str(
                    getattr(col, "display_header", None)
                    or getattr(col, "header", None)
                    or getattr(col, "name", None)
                )
                k = _safe_str(
                    getattr(col, "key", None)
                    or getattr(col, "field", None)
                    or getattr(col, "name", None)
                )
                if h or k:
                    headers.append(h or k)
                    keys.append(k or h.lower().replace(" ", "_"))

            if headers and keys and len(headers) == len(keys):
                return headers, keys

    # Minimal fallback
    return list(_FALLBACK_LEGACY_HEADERS), list(_FALLBACK_LEGACY_KEYS)


def _row_value_for_key(quote_dict: Dict[str, Any], key: str) -> Any:
    """Fetch a value from a quote dict by canonical key, then by aliases."""
    if not key:
        return None

    if key in quote_dict and quote_dict[key] is not None:
        return quote_dict[key]

    # Try aliases
    for alias in _FIELD_ALIAS_HINTS.get(key, ()):
        if alias in quote_dict and quote_dict[alias] is not None:
            return quote_dict[alias]

    # Try case-insensitive lookup
    key_lower = key.lower()
    for k, v in quote_dict.items():
        if _safe_str(k).lower() == key_lower and v is not None:
            return v

    return None


def _quote_to_row(
    quote: Any,
    headers_or_keys: Sequence[str],
    keys: Optional[Sequence[str]] = None,
) -> List[Any]:
    """Convert a quote to a row aligned with `keys` (registry-canonical).

    Signature note: the first positional was previously called `headers`.
    To preserve backward compatibility, if called with a single sequence
    argument we treat it as keys (best-effort header->key normalization).
    Prefer the 2-arg form `_quote_to_row(quote, headers, keys)`.
    """
    if keys is None:
        # Single-arg back-compat: assume the passed sequence is either
        # registry keys (snake_case) or display headers that need
        # normalization.
        lookup_keys = [
            k if (k and "_" in k and k == k.lower())
            else _safe_str(k).lower().replace(" ", "_").replace("-", "_")
            for k in headers_or_keys
        ]
    else:
        lookup_keys = [_safe_str(k) for k in keys]

    d = _quote_dict(quote)
    row: List[Any] = []
    for k in lookup_keys:
        val = _row_value_for_key(d, k)
        # Post-process common types
        if k == "percent_change":
            val = _to_percent(val)
        elif k in ("last_updated_utc", "last_updated_riyadh") and val is not None:
            val = _iso_or_none(val)
        row.append(val)
    return row


async def _try_cache_snapshot(
    engine: Any,
    sheet_name: str,
    headers: List[str],
    rows: List[List[Any]],
    meta: Dict[str, Any],
) -> bool:
    """Try to cache sheet snapshot."""
    if engine is None or not sheet_name or not headers:
        return False

    methods = [
        "set_cached_sheet_snapshot",
        "cache_sheet_snapshot",
        "store_sheet_snapshot",
        "save_sheet_snapshot",
    ]

    for method_name in methods:
        method = getattr(engine, method_name, None)
        if callable(method):
            try:
                await _maybe_await(method(sheet_name, headers, rows, meta))
                return True
            except Exception as e:
                logger.debug("Cache method %s failed: %s", method_name, e)
                continue

    return False


# ---------------------------------------------------------------------------
# Router Initialization
# ---------------------------------------------------------------------------

router = APIRouter(prefix="/v1/legacy", tags=["legacy_compat"])
_cache = SmartCache(default_ttl=CACHE_TTL_QUOTE, max_size=10000)
_metrics = ServiceMetrics()
_external_loaded_from: Optional[str] = None


# ---------------------------------------------------------------------------
# Health Check Endpoint
# ---------------------------------------------------------------------------

@router.get("/health", summary="Legacy compatibility health check")
async def legacy_health(request: Request) -> Dict[str, Any]:
    """Health check endpoint."""
    auth_ok, auth_error = await _check_auth(request)
    if not auth_ok:
        return {"status": "error", "error": auth_error or "Unauthorized", "version": VERSION}

    engine_info = await _get_engine(request)

    response: Dict[str, Any] = {
        "status": "ok",
        "version": VERSION,
        "router": "core.legacy_service",
        "mode": "internal",
        "external_router_enabled": _CONFIG.enabled,
        "engine": {
            "present": engine_info.engine is not None,
            "source": engine_info.source.value,
            "should_close": engine_info.should_close,
            "class": engine_info.engine.__class__.__name__ if engine_info.engine else None,
        },
        "config": {
            "concurrency": _CONFIG.concurrency,
            "timeout_sec": _CONFIG.timeout_sec,
            "max_symbols": _CONFIG.max_symbols,
            "auth_enabled": _CONFIG.enable_auth,
        },
        "cache": await _cache.get_stats(),
        "metrics": _metrics.to_dict(),
    }

    if _external_loaded_from:
        response["external_loaded_from"] = _external_loaded_from

    if engine_info.engine is not None:
        try:
            if hasattr(engine_info.engine, "version"):
                response["engine"]["version"] = engine_info.engine.version
            elif hasattr(engine_info.engine, "ENGINE_VERSION"):
                response["engine"]["version"] = engine_info.engine.ENGINE_VERSION
        except Exception:
            pass

    await _close_engine(engine_info)
    return response


# ---------------------------------------------------------------------------
# Quote Endpoint
# ---------------------------------------------------------------------------

@router.get("/quote", summary="Legacy quote endpoint")
async def legacy_quote(
    request: Request,
    symbol: str = Query(..., min_length=1, description="Symbol to lookup"),
    debug: bool = Query(False, description="Include debug information"),
    skip_cache: bool = Query(False, description="Skip cache and force refresh"),
) -> BestJSONResponse:
    """Get quote for a single symbol."""
    request_id = _new_request_id()
    start = time.time()

    # Authentication
    auth_ok, auth_error = await _check_auth(request)
    if not auth_ok:
        return BestJSONResponse(
            status_code=401 if _CONFIG.enable_auth else 200,
            content={
                "status": "error",
                "error": auth_error or "Unauthorized",
                "request_id": request_id,
            }
        )

    # Normalize symbol
    raw_symbol = symbol.strip()
    norm_symbol = _normalize_symbol(raw_symbol)
    cache_key = f"quote:{norm_symbol}"

    # Check cache — only count misses when we actually looked
    if not skip_cache:
        cached = await _cache.get(cache_key)
        if cached is not None:
            _metrics.cache_hits += 1
            return BestJSONResponse(status_code=200, content=jsonable_encoder(cached))
        _metrics.cache_misses += 1

    # Get engine
    engine_info = await _get_engine(request)

    if engine_info.engine is None:
        return BestJSONResponse(
            status_code=200,
            content={
                "status": "error",
                "symbol": raw_symbol,
                "symbol_normalized": norm_symbol,
                "error": "No engine available",
                "request_id": request_id,
                "runtime_ms": (time.time() - start) * 1000,
            }
        )

    try:
        result, method, error = await _call_engine_method(
            engine_info.engine,
            ["get_enriched_quote", "get_quote", "fetch_quote"],
            norm_symbol,
            timeout=_CONFIG.timeout_sec,
        )
        _metrics.record_call(method or "unknown", success=result is not None)

        if result is None:
            response = {
                "status": "error",
                "symbol": raw_symbol,
                "symbol_normalized": norm_symbol,
                "error": error or "Quote not found",
                "request_id": request_id,
                "runtime_ms": (time.time() - start) * 1000,
            }
            if debug:
                response["debug"] = {
                    "engine_source": engine_info.source.value,
                    "method_attempted": method,
                }
            return BestJSONResponse(status_code=200, content=jsonable_encoder(response))

        # Cache result
        await _cache.set(cache_key, result, ttl=CACHE_TTL_QUOTE)

        response = {
            "status": "success",
            "data": result,
            "request_id": request_id,
            "runtime_ms": (time.time() - start) * 1000,
        }
        if debug:
            response["debug"] = {
                "engine_source": engine_info.source.value,
                "method": method,
            }

        return BestJSONResponse(status_code=200, content=jsonable_encoder(response))

    except Exception as e:
        logger.error("Quote error for %s: %s", symbol, e, exc_info=True)
        response = {
            "status": "error",
            "symbol": raw_symbol,
            "symbol_normalized": norm_symbol,
            "error": _safe_err(e),
            "request_id": request_id,
            "runtime_ms": (time.time() - start) * 1000,
        }
        if debug:
            response["debug"] = {
                "engine_source": engine_info.source.value,
                "traceback": traceback.format_exc()[:2000],
            }
        return BestJSONResponse(status_code=200, content=jsonable_encoder(response))

    finally:
        await _close_engine(engine_info)


# ---------------------------------------------------------------------------
# Batch Quotes Endpoint
# ---------------------------------------------------------------------------

@router.post("/quotes", summary="Legacy batch quotes endpoint")
async def legacy_quotes(
    request: Request,
    payload: SymbolsIn,
    debug: bool = Query(False, description="Include debug information"),
    skip_cache: bool = Query(False, description="Skip cache and force refresh"),
) -> BestJSONResponse:
    """Get quotes for multiple symbols."""
    request_id = _new_request_id()
    start = time.time()

    # Authentication
    auth_ok, auth_error = await _check_auth(request)
    if not auth_ok:
        return BestJSONResponse(
            status_code=401 if _CONFIG.enable_auth else 200,
            content={
                "status": "error",
                "error": auth_error or "Unauthorized",
                "request_id": request_id,
            }
        )

    # Normalize symbols
    raw_symbols = payload.normalized()
    if not raw_symbols:
        return BestJSONResponse(
            status_code=200,
            content={
                "status": "error",
                "error": "No symbols provided",
                "request_id": request_id,
            }
        )

    norm_symbols = [_normalize_symbol(s) for s in raw_symbols]

    # Get engine
    engine_info = await _get_engine(request)

    if engine_info.engine is None:
        return BestJSONResponse(
            status_code=200,
            content={
                "status": "error",
                "error": "No engine available",
                "request_id": request_id,
                "runtime_ms": (time.time() - start) * 1000,
            }
        )

    try:
        result, method, error = await _call_engine_method(
            engine_info.engine,
            ["get_enriched_quotes", "get_quotes", "fetch_quotes"],
            norm_symbols,
            timeout=_CONFIG.timeout_sec,
        )

        if result is not None:
            ordered = _items_to_ordered_list(result, raw_symbols)
            if ordered:
                # Cache individual quotes
                for i, item in enumerate(ordered):
                    if i < len(norm_symbols) and item:
                        await _cache.set(f"quote:{norm_symbols[i]}", item, ttl=CACHE_TTL_QUOTE)

                response = {
                    "status": "success",
                    "data": ordered,
                    "count": len(ordered),
                    "request_id": request_id,
                    "runtime_ms": (time.time() - start) * 1000,
                }
                if debug:
                    response["debug"] = {
                        "engine_source": engine_info.source.value,
                        "method": method,
                        "batch": True,
                    }
                return BestJSONResponse(status_code=200, content=jsonable_encoder(response))

        # Fallback to individual requests
        semaphore = asyncio.Semaphore(_CONFIG.concurrency)

        async def _fetch_one(idx: int) -> Tuple[int, Any]:
            norm = norm_symbols[idx]

            if not skip_cache:
                cached = await _cache.get(f"quote:{norm}")
                if cached is not None:
                    _metrics.cache_hits += 1
                    return idx, cached
                _metrics.cache_misses += 1

            async with semaphore:
                res, m, _err = await _call_engine_method(
                    engine_info.engine,
                    ["get_enriched_quote", "get_quote", "fetch_quote"],
                    norm,
                    timeout=_CONFIG.timeout_sec,
                )
                _metrics.record_call(m or "unknown", success=res is not None)
                if res:
                    await _cache.set(f"quote:{norm}", res, ttl=CACHE_TTL_QUOTE)
                return idx, res

        tasks = [_fetch_one(i) for i in range(len(raw_symbols))]
        results = await asyncio.gather(*tasks)

        ordered = [None] * len(raw_symbols)
        for idx, res in results:
            ordered[idx] = res

        response = {
            "status": "success",
            "data": ordered,
            "count": len(ordered),
            "request_id": request_id,
            "runtime_ms": (time.time() - start) * 1000,
        }
        if debug:
            response["debug"] = {
                "engine_source": engine_info.source.value,
                "fallback": True,
            }

        return BestJSONResponse(status_code=200, content=jsonable_encoder(response))

    except Exception as e:
        logger.error("Batch quotes error: %s", e, exc_info=True)
        response = {
            "status": "error",
            "error": _safe_err(e),
            "request_id": request_id,
            "runtime_ms": (time.time() - start) * 1000,
        }
        if debug:
            response["debug"] = {
                "engine_source": engine_info.source.value,
                "traceback": traceback.format_exc()[:2000],
            }
        return BestJSONResponse(status_code=200, content=jsonable_encoder(response))

    finally:
        await _close_engine(engine_info)


# ---------------------------------------------------------------------------
# Sheet Rows Endpoint
# ---------------------------------------------------------------------------

@router.post("/sheet-rows", summary="Legacy sheet-rows helper")
async def legacy_sheet_rows(
    request: Request,
    payload: SheetRowsIn,
    debug: bool = Query(False, description="Include debug information"),
    skip_cache: bool = Query(False, description="Skip cache and force refresh"),
) -> BestJSONResponse:
    """Get sheet rows for symbols."""
    request_id = _new_request_id()
    start = time.time()

    # Authentication
    auth_ok, auth_error = await _check_auth(request)
    if not auth_ok:
        return BestJSONResponse(
            status_code=401 if _CONFIG.enable_auth else 200,
            content={
                "status": "error",
                "error": auth_error or "Unauthorized",
                "request_id": request_id,
            }
        )

    # Normalize symbols
    symbols_in = SymbolsIn(symbols=payload.symbols, tickers=payload.tickers)
    raw_symbols = symbols_in.normalized()
    headers, keys = _get_headers_for_sheet(payload.get_sheet_name())

    if not raw_symbols:
        return BestJSONResponse(
            status_code=200,
            content={
                "status": "error",
                "error": "No symbols provided",
                "headers": headers,
                "keys": keys,
                "rows": [],
                "request_id": request_id,
            }
        )

    norm_symbols = [_normalize_symbol(s) for s in raw_symbols]

    # Get engine
    engine_info = await _get_engine(request)

    if engine_info.engine is None:
        return BestJSONResponse(
            status_code=200,
            content={
                "status": "error",
                "error": "No engine available",
                "headers": headers,
                "keys": keys,
                "rows": [],
                "request_id": request_id,
                "runtime_ms": (time.time() - start) * 1000,
            }
        )

    try:
        # Try batch first
        result, method, error = await _call_engine_method(
            engine_info.engine,
            ["get_enriched_quotes", "get_quotes", "fetch_quotes"],
            norm_symbols,
            timeout=_CONFIG.timeout_sec,
        )

        quotes: List[Any] = []
        if result is not None:
            ordered = _items_to_ordered_list(result, raw_symbols)
            if ordered:
                quotes = ordered

        # Fallback to individual if needed
        if len(quotes) != len(raw_symbols):
            semaphore = asyncio.Semaphore(_CONFIG.concurrency)

            async def _fetch_one(idx: int) -> Tuple[int, Any]:
                norm = norm_symbols[idx]
                async with semaphore:
                    res, _m, _e = await _call_engine_method(
                        engine_info.engine,
                        ["get_enriched_quote", "get_quote", "fetch_quote"],
                        norm,
                        timeout=_CONFIG.timeout_sec,
                    )
                    return idx, res

            tasks = [_fetch_one(i) for i in range(len(raw_symbols))]
            results = await asyncio.gather(*tasks)

            quotes = [None] * len(raw_symbols)
            for idx, res in results:
                quotes[idx] = res

        # Convert to rows using registry-canonical keys
        rows = [
            _quote_to_row(q if q is not None else {}, headers, keys)
            for q in quotes
        ]

        # Cache snapshot if sheet name provided
        if payload.get_sheet_name():
            meta = {
                "source": "legacy_service",
                "version": VERSION,
                "engine_source": engine_info.source.value,
                "method": method,
                "cached_at": _utc_iso(),
                "keys": keys,
            }
            await _try_cache_snapshot(engine_info.engine, payload.get_sheet_name(), headers, rows, meta)

        response = {
            "status": "success",
            "headers": headers,
            "keys": keys,
            "rows": rows,
            "count": len(rows),
            "request_id": request_id,
            "runtime_ms": (time.time() - start) * 1000,
        }
        if debug:
            response["debug"] = {
                "engine_source": engine_info.source.value,
                "method": method,
                "sheet_name": payload.get_sheet_name(),
            }

        return BestJSONResponse(status_code=200, content=jsonable_encoder(response))

    except Exception as e:
        logger.error("Sheet rows error: %s", e, exc_info=True)
        response = {
            "status": "error",
            "error": _safe_err(e),
            "headers": headers,
            "keys": keys,
            "rows": [],
            "request_id": request_id,
            "runtime_ms": (time.time() - start) * 1000,
        }
        if debug:
            response["debug"] = {
                "engine_source": engine_info.source.value,
                "traceback": traceback.format_exc()[:2000],
            }
        return BestJSONResponse(status_code=200, content=jsonable_encoder(response))

    finally:
        await _close_engine(engine_info)


# ---------------------------------------------------------------------------
# Cache Management Endpoints
# ---------------------------------------------------------------------------

@router.delete("/cache", summary="Clear legacy cache")
async def legacy_clear_cache(request: Request) -> BestJSONResponse:
    """Clear the legacy cache."""
    auth_ok, auth_error = await _check_auth(request)
    if not auth_ok:
        return BestJSONResponse(
            status_code=401 if _CONFIG.enable_auth else 200,
            content={"status": "error", "error": auth_error or "Unauthorized"}
        )

    await _cache.clear()
    return BestJSONResponse(
        status_code=200,
        content={"status": "success", "message": "Cache cleared"}
    )


@router.get("/cache/stats", summary="Get cache statistics")
async def legacy_cache_stats(request: Request) -> BestJSONResponse:
    """Get cache statistics."""
    auth_ok, auth_error = await _check_auth(request)
    if not auth_ok:
        return BestJSONResponse(
            status_code=401 if _CONFIG.enable_auth else 200,
            content={"status": "error", "error": auth_error or "Unauthorized"}
        )

    return BestJSONResponse(
        status_code=200,
        content={"status": "success", "stats": await _cache.get_stats()}
    )


@router.get("/metrics", summary="Get service metrics")
async def legacy_metrics(request: Request) -> BestJSONResponse:
    """Get service metrics."""
    auth_ok, auth_error = await _check_auth(request)
    if not auth_ok:
        return BestJSONResponse(
            status_code=401 if _CONFIG.enable_auth else 200,
            content={"status": "error", "error": auth_error or "Unauthorized"}
        )

    return BestJSONResponse(
        status_code=200,
        content={"status": "success", "metrics": _metrics.to_dict()}
    )


# ---------------------------------------------------------------------------
# External Router Override
# ---------------------------------------------------------------------------

if _CONFIG.enabled:
    def _is_this_module_file(path: str) -> bool:
        """Check if a module path resolves to this very file."""
        if not path:
            return False
        try:
            return os.path.samefile(path, __file__)
        except (OSError, ValueError):
            # Fallback: basename comparison
            try:
                return os.path.basename(path) == os.path.basename(__file__)
            except Exception:
                return False

    def _safe_mod_file(mod: Any) -> str:
        """Get module file path safely."""
        try:
            return str(getattr(mod, "__file__", "") or "")
        except Exception:
            return ""

    def _try_import_external_router() -> Optional[APIRouter]:
        """Try to import external router."""
        global _external_loaded_from

        for module_name, source in [
            ("legacy_service", "legacy_service"),
            ("routes.legacy_service", "routes.legacy_service"),
        ]:
            try:
                mod = importlib.import_module(module_name)
                if _is_this_module_file(_safe_mod_file(mod)):
                    continue
                router_obj = getattr(mod, "router", None)
                if router_obj and isinstance(router_obj, APIRouter):
                    _external_loaded_from = source
                    logger.info("Loaded external router from %s", source)
                    return router_obj
            except Exception as e:
                if _CONFIG.log_external_import_failure:
                    logger.debug("Failed to import %s: %s", module_name, e)
                continue
        return None

    ext = _try_import_external_router()
    if ext is not None:
        router = ext


# ---------------------------------------------------------------------------
# Module Exports
# ---------------------------------------------------------------------------

def get_router() -> APIRouter:
    """Get the legacy router."""
    return router


__all__ = [
    "router",
    "get_router",
    "VERSION",
    "SymbolsIn",
    "SheetRowsIn",
]
