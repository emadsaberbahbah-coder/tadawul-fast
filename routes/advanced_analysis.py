#!/usr/bin/env python3
"""
routes/advanced_analysis.py
------------------------------------------------------------
TADAWUL ADVANCED ANALYSIS ENGINE — v5.3.0 (PHASE 3 SCHEMA-ROWS)
PROD HARDENED + SCHEMA-DRIVEN SHEET-ROWS

PHASE 3 REQUIREMENTS (DONE)
- ✅ Accept page names + aliases (page_catalog.normalize_page_name)
- ✅ Return FULL schema headers from schema_registry (not 27 cols)
- ✅ For each row: emit dict with all schema keys, missing => null
- ✅ Stable ordering:
     - headers/keys follow schema order
     - rows follow requested symbols order
- ✅ Optional: include rows_matrix for legacy clients

Notes:
- No startup network I/O.
- Engine access is lazy via request.app.state.engine or core.data_engine_v2.get_engine().
"""

from __future__ import annotations

import asyncio
import logging
import os
import random
import time
import uuid
import zlib
import pickle
from dataclasses import dataclass, is_dataclass
from datetime import datetime, timedelta, timezone
from enum import Enum
from typing import Any, Awaitable, Callable, Dict, List, Optional, Sequence, Set, Tuple, Union

from fastapi import APIRouter, Body, Header, HTTPException, Query, Request, Response, status
from urllib.parse import urlparse

# ---------------------------------------------------------------------------
# Phase 1 / 3: Schema + Page Catalog
# ---------------------------------------------------------------------------
from core.sheets.schema_registry import SCHEMA_REGISTRY, get_sheet_spec
from core.sheets.page_catalog import normalize_page_name, CANONICAL_PAGES
from core.sheets.data_dictionary import build_data_dictionary_rows

# Core config (preferred for auth & schema flags)
try:
    from core.config import auth_ok, get_settings_cached  # type: ignore
except Exception:
    auth_ok = None  # type: ignore

    def get_settings_cached(*args, **kwargs):  # type: ignore
        return None

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logger = logging.getLogger("routes.advanced_analysis")

ADVANCED_ANALYSIS_VERSION = "5.3.0"
router = APIRouter(prefix="/v1/advanced", tags=["Advanced Analysis"])

# ---------------------------------------------------------------------------
# Helpers: NaN/Inf cleaning (prevents JSON crashes)
# ---------------------------------------------------------------------------
def clean_nans(obj: Any) -> Any:
    try:
        import math

        if isinstance(obj, dict):
            return {k: clean_nans(v) for k, v in obj.items()}
        if isinstance(obj, list):
            return [clean_nans(v) for v in obj]
        if isinstance(obj, float) and (math.isnan(obj) or math.isinf(obj)):
            return None
    except Exception:
        pass
    return obj


# ---------------------------------------------------------------------------
# High-Performance JSON fallback
# ---------------------------------------------------------------------------
try:
    import orjson
    from fastapi.responses import ORJSONResponse as BestJSONResponse

    def json_dumps(v: Any, *, default=str) -> str:
        return orjson.dumps(clean_nans(v), default=default).decode("utf-8")

    def json_loads(v: Union[str, bytes]) -> Any:
        return orjson.loads(v)

    _HAS_ORJSON = True
except ImportError:
    import json
    from fastapi.responses import JSONResponse as BestJSONResponse

    def json_dumps(v: Any, *, default=str) -> str:
        return json.dumps(clean_nans(v), default=default, ensure_ascii=False)

    def json_loads(v: Union[str, bytes]) -> Any:
        return json.loads(v)

    _HAS_ORJSON = False


# ---------------------------------------------------------------------------
# Optional enterprise integrations
# ---------------------------------------------------------------------------
# OpenTelemetry (optional)
try:
    from opentelemetry import trace
    from opentelemetry.trace import Status, StatusCode

    OPENTELEMETRY_AVAILABLE = True
    tracer = trace.get_tracer(__name__)
except ImportError:
    OPENTELEMETRY_AVAILABLE = False

    class DummySpan:
        def set_attribute(self, *args, **kwargs):
            return None

        def record_exception(self, *args, **kwargs):
            return None

        def set_status(self, *args, **kwargs):
            return None

        def end(self):
            return None

    class DummyContextManager:
        def __init__(self, span):
            self._span = span

        def __enter__(self):
            return self._span

        def __exit__(self, *args, **kwargs):
            return False

    class DummyTracer:
        def start_as_current_span(self, *args, **kwargs):
            return DummyContextManager(DummySpan())

    tracer = DummyTracer()


# aiocache (optional)
try:
    from aiocache import Cache  # type: ignore

    AIOCACHE_AVAILABLE = True
except ImportError:
    AIOCACHE_AVAILABLE = False
    Cache = None  # type: ignore

# Prometheus (optional)
try:
    from prometheus_client import Counter, Histogram, Gauge  # noqa

    PROMETHEUS_AVAILABLE = True
except ImportError:
    PROMETHEUS_AVAILABLE = False

# Pydantic imports
try:
    from pydantic import BaseModel, ConfigDict, Field, model_validator  # type: ignore

    _PYDANTIC_V2 = True
except ImportError:
    from pydantic import BaseModel, Field  # type: ignore

    _PYDANTIC_V2 = False


# =============================================================================
# Configuration
# =============================================================================

class CacheStrategy(str, Enum):
    LOCAL_MEMORY = "local_memory"
    REDIS = "redis"
    DISTRIBUTED = "distributed"


@dataclass(slots=True)
class AdvancedConfig:
    # concurrency/batching
    max_concurrency: int = 18
    adaptive_concurrency: bool = True
    concurrency_floor: int = 4
    concurrency_ceiling: int = 80
    batch_size: int = 250
    batch_timeout: float = 40.0
    total_timeout: float = 110.0

    # cache
    cache_strategy: CacheStrategy = CacheStrategy.LOCAL_MEMORY
    cache_ttl_seconds: int = 300
    cache_max_size: int = 10000
    cache_compression: bool = True

    # auth/rate limit
    allow_query_token: bool = False
    require_auth: bool = True
    rate_limit_enabled: bool = True
    rate_limit_requests: int = 100
    rate_limit_window: int = 60

    # tracing/metrics
    enable_tracing: bool = True
    trace_sample_rate: float = 1.0

    # compliance/audit
    retain_audit_log: bool = True


_CONFIG = AdvancedConfig()


def _load_config_from_env() -> None:
    def env_int(name: str, default: int) -> int:
        try:
            return int(os.getenv(name, str(default)))
        except Exception:
            return default

    def env_float(name: str, default: float) -> float:
        try:
            return float(os.getenv(name, str(default)))
        except Exception:
            return default

    def env_bool(name: str, default: bool) -> bool:
        return os.getenv(name, str(default)).strip().lower() in ("true", "1", "yes", "on", "y")

    _CONFIG.max_concurrency = env_int("ADVANCED_MAX_CONCURRENCY", _CONFIG.max_concurrency)
    _CONFIG.adaptive_concurrency = env_bool("ADVANCED_ADAPTIVE_CONCURRENCY", _CONFIG.adaptive_concurrency)

    _CONFIG.batch_size = env_int("ADVANCED_BATCH_SIZE", _CONFIG.batch_size)
    _CONFIG.batch_timeout = env_float("ADVANCED_BATCH_TIMEOUT_SEC", _CONFIG.batch_timeout)
    _CONFIG.total_timeout = env_float("ADVANCED_TOTAL_TIMEOUT_SEC", _CONFIG.total_timeout)

    _CONFIG.allow_query_token = env_bool("ALLOW_QUERY_TOKEN", _CONFIG.allow_query_token)
    _CONFIG.enable_tracing = env_bool("CORE_TRACING_ENABLED", _CONFIG.enable_tracing)

    _CONFIG.require_auth = env_bool("REQUIRE_AUTH", _CONFIG.require_auth)

    _CONFIG.rate_limit_enabled = env_bool("RATE_LIMIT_ENABLED", _CONFIG.rate_limit_enabled)
    _CONFIG.rate_limit_requests = env_int("MAX_REQUESTS_PER_MINUTE", _CONFIG.rate_limit_requests)
    _CONFIG.rate_limit_window = env_int("RATE_LIMIT_WINDOW_SEC", _CONFIG.rate_limit_window)


_load_config_from_env()


# =============================================================================
# Trace Context (safe)
# =============================================================================

class TraceContext:
    """
    Correct OTel usage:
      tracer.start_as_current_span(...) -> context manager
      enter to get span, set attributes via set_attribute
      exit closes span
    """

    def __init__(self, name: str, attributes: Optional[Dict[str, Any]] = None):
        self.name = name
        self.attributes = attributes or {}
        self._cm = None
        self.span = None
        self._enabled = OPENTELEMETRY_AVAILABLE and _CONFIG.enable_tracing and (random.random() <= _CONFIG.trace_sample_rate)

    async def __aenter__(self):
        if self._enabled:
            try:
                self._cm = tracer.start_as_current_span(self.name)
                self.span = self._cm.__enter__()
                for k, v in self.attributes.items():
                    try:
                        if hasattr(self.span, "set_attribute"):
                            self.span.set_attribute(k, v)
                    except Exception:
                        pass
            except Exception:
                self._cm = None
                self.span = None
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self._cm and self.span:
            try:
                if exc_val and hasattr(self.span, "record_exception"):
                    self.span.record_exception(exc_val)
                if exc_val and hasattr(self.span, "set_status") and OPENTELEMETRY_AVAILABLE:
                    self.span.set_status(Status(StatusCode.ERROR, str(exc_val)))
            except Exception:
                pass
            try:
                return self._cm.__exit__(exc_type, exc_val, exc_tb)
            except Exception:
                return False
        return False


# =============================================================================
# Riyadh Time / business day
# =============================================================================

class RiyadhTime:
    def __init__(self):
        self._tz = timezone(timedelta(hours=3))
        self._weekend_days = {4, 5}  # Fri/Sat

    def now(self) -> datetime:
        return datetime.now(self._tz)

    def is_business_day(self, dt: Optional[datetime] = None) -> bool:
        dt = dt or self.now()
        return dt.weekday() not in self._weekend_days


_riyadh_time = RiyadhTime()


# =============================================================================
# Rate limiter (kept)
# =============================================================================

class RateLimiter:
    def __init__(self):
        self._buckets: Dict[str, Tuple[int, float]] = {}
        self._lock = asyncio.Lock()

    async def check(self, key: str, requests: int, window: int) -> bool:
        if not _CONFIG.rate_limit_enabled or requests <= 0:
            return True
        now = time.time()
        async with self._lock:
            count, reset = self._buckets.get(key, (0, now + window))
            if now > reset:
                count, reset = 0, now + window
            if count < requests:
                self._buckets[key] = (count + 1, reset)
                return True
            return False


_rate_limiter = RateLimiter()


# =============================================================================
# Cache Manager (local + optional Redis via aiocache)
# =============================================================================

class CacheManager:
    """Multi-tier cache manager with optional Redis and zlib+pickle compression."""

    def __init__(self):
        self._local: Dict[str, Tuple[Any, float]] = {}
        self._lock = asyncio.Lock()
        self._stats = {"hits": 0, "misses": 0, "redis_hits": 0}
        self._redis = None

        if AIOCACHE_AVAILABLE and _CONFIG.cache_strategy in (CacheStrategy.REDIS, CacheStrategy.DISTRIBUTED):
            try:
                redis_url = os.getenv("REDIS_URL", "").strip()
                if redis_url:
                    u = urlparse(redis_url)
                    endpoint = u.hostname or "localhost"
                    port = int(u.port or 6379)
                    password = u.password
                    db = int((u.path or "/0").lstrip("/") or 0)
                else:
                    endpoint = os.getenv("REDIS_HOST", "localhost")
                    port = int(os.getenv("REDIS_PORT", "6379"))
                    password = os.getenv("REDIS_PASSWORD") or None
                    db = int(os.getenv("REDIS_DB", "0"))

                self._redis = Cache.REDIS(  # type: ignore
                    endpoint=endpoint,
                    port=port,
                    password=password,
                    db=db,
                    namespace="tadawul_advanced",
                )
            except Exception as e:
                logger.warning("Redis cache unavailable: %s", e)
                self._redis = None

    def _compress(self, data: Any) -> bytes:
        raw = pickle.dumps(data, protocol=pickle.HIGHEST_PROTOCOL)
        if not _CONFIG.cache_compression:
            return raw
        return zlib.compress(raw, level=6)

    def _decompress(self, data: bytes) -> Any:
        try:
            if _CONFIG.cache_compression:
                return pickle.loads(zlib.decompress(data))
            return pickle.loads(data)
        except Exception:
            return pickle.loads(data)

    async def get(self, key: str) -> Optional[Any]:
        now = time.time()
        async with self._lock:
            item = self._local.get(key)
            if item:
                value, expiry = item
                if expiry > now:
                    self._stats["hits"] += 1
                    return value
                self._local.pop(key, None)

        if self._redis:
            try:
                raw = await self._redis.get(key)
                if raw is not None:
                    value = self._decompress(raw)
                    self._stats["redis_hits"] += 1
                    self._stats["hits"] += 1
                    async with self._lock:
                        self._local[key] = (value, time.time() + (_CONFIG.cache_ttl_seconds // 2))
                    return value
            except Exception:
                pass

        self._stats["misses"] += 1
        return None

    async def set(self, key: str, value: Any, ttl: Optional[int] = None) -> None:
        ttl = int(ttl or _CONFIG.cache_ttl_seconds)
        expiry = time.time() + ttl

        async with self._lock:
            if len(self._local) >= _CONFIG.cache_max_size:
                items = sorted(self._local.items(), key=lambda x: x[1][1])
                for k, _ in items[: max(1, _CONFIG.cache_max_size // 10)]:
                    self._local.pop(k, None)
            self._local[key] = (value, expiry)

        if self._redis:
            try:
                await self._redis.set(key, self._compress(value), ttl=ttl)
            except Exception:
                pass

    def get_stats(self) -> Dict[str, Any]:
        return {
            "hits": self._stats["hits"],
            "misses": self._stats["misses"],
            "redis_hits": self._stats["redis_hits"],
            "local_cache_size": len(self._local),
        }


_cache_manager = CacheManager()


# =============================================================================
# Core engine wrapper
# =============================================================================

class AdvancedDataEngine:
    def __init__(self):
        self._engine = None
        self._lock = asyncio.Lock()

    async def get_engine(self, request: Request) -> Optional[Any]:
        try:
            st = getattr(request.app, "state", None)
            if st and getattr(st, "engine", None):
                return st.engine
        except Exception:
            pass

        if self._engine is not None:
            return self._engine

        async with self._lock:
            if self._engine is not None:
                return self._engine
            try:
                from core.data_engine_v2 import get_engine  # expected in your repo

                self._engine = await get_engine()
                return self._engine
            except Exception as e:
                logger.error("Engine initialization failed: %s", e)
                return None

    async def get_quotes(self, engine: Any, symbols: List[str], mode: str = "") -> Dict[str, Any]:
        cache_keys = {s: f"quote:{s}:{mode}" for s in symbols}
        cached: Dict[str, Any] = {}
        for s, ck in cache_keys.items():
            v = await _cache_manager.get(ck)
            if v is not None:
                cached[s] = v

        to_fetch = [s for s in symbols if s not in cached]
        fetched: Dict[str, Any] = {}

        if to_fetch:
            # try batch methods if present
            for method in ("get_enriched_quotes_batch", "get_enriched_quotes", "get_quotes_batch"):
                fn = getattr(engine, method, None)
                if callable(fn):
                    try:
                        res = await fn(to_fetch, mode=mode) if mode else await fn(to_fetch)
                        if isinstance(res, dict):
                            fetched = res
                        elif isinstance(res, list):
                            fetched = {s: r for s, r in zip(to_fetch, res)}
                        break
                    except Exception:
                        continue

            if not fetched:
                # fallback per-symbol
                sem = asyncio.Semaphore(_CONFIG.max_concurrency)

                async def fetch_one(sym: str):
                    async with sem:
                        try:
                            q = await engine.get_enriched_quote(sym, mode=mode) if mode else await engine.get_enriched_quote(sym)
                            return sym, q
                        except Exception as e:
                            return sym, {"symbol": sym, "error": str(e)}

                pairs = await asyncio.gather(*(fetch_one(s) for s in to_fetch), return_exceptions=False)
                fetched = {k: v for k, v in pairs}

        for s, v in fetched.items():
            if isinstance(v, dict) and v.get("error"):
                continue
            await _cache_manager.set(cache_keys[s], v, ttl=_CONFIG.cache_ttl_seconds)

        out = dict(cached)
        out.update(fetched)
        return out


_data_engine = AdvancedDataEngine()


# =============================================================================
# Phase 3: Schema normalization
# =============================================================================

_CANONICAL_ALIASES: Dict[str, Tuple[str, ...]] = {
    # identity
    "symbol": ("symbol", "ticker"),
    "name": ("name", "company_name"),
    "exchange": ("exchange",),
    "currency": ("currency",),
    "asset_class": ("asset_class", "type", "instrument_type"),

    # price
    "current_price": ("current_price", "price", "last_price"),
    "previous_close": ("previous_close", "prev_close"),
    "open_price": ("open_price", "open"),
    "day_high": ("day_high", "high"),
    "day_low": ("day_low", "low"),
    "week_52_high": ("week_52_high", "52w_high", "high_52w"),
    "week_52_low": ("week_52_low", "52w_low", "low_52w"),
    "price_change": ("price_change", "change"),
    "percent_change": ("percent_change", "change_pct", "change_percent"),

    # liquidity
    "volume": ("volume",),
    "market_cap": ("market_cap",),

    # technical/risk
    "rsi_14": ("rsi_14", "rsi14", "rsi_14d", "rsi"),
    "volatility_30d": ("volatility_30d", "vol_30d"),
    "risk_score": ("risk_score",),

    # forecast/roi
    "forecast_price_1m": ("forecast_price_1m",),
    "forecast_price_3m": ("forecast_price_3m",),
    "forecast_price_12m": ("forecast_price_12m",),
    "expected_roi_1m": ("expected_roi_1m",),
    "expected_roi_3m": ("expected_roi_3m",),
    "expected_roi_12m": ("expected_roi_12m",),

    # scores
    "overall_score": ("overall_score",),
    "confidence_score": ("confidence_score",),
    "forecast_confidence": ("forecast_confidence",),
    "recommendation": ("recommendation",),
    "recommendation_reason": ("recommendation_reason",),

    # provenance
    "last_updated_utc": ("last_updated_utc",),
    "last_updated_riyadh": ("last_updated_riyadh",),
    "warnings": ("warnings",),
    "data_provider": ("data_provider",),
}


def _to_plain_dict(obj: Any) -> Dict[str, Any]:
    if obj is None:
        return {}
    if isinstance(obj, dict):
        return obj
    if hasattr(obj, "model_dump"):
        try:
            return obj.model_dump(mode="python")
        except Exception:
            return {}
    if hasattr(obj, "dict"):
        try:
            return obj.dict()
        except Exception:
            return {}
    if is_dataclass(obj):
        try:
            return {k: getattr(obj, k) for k in obj.__dict__.keys() if not str(k).startswith("_")}
        except Exception:
            return {}
    return {}


def _normalize_row_to_schema(schema_keys: Sequence[str], raw: Dict[str, Any], *, symbol_fallback: str) -> Dict[str, Any]:
    """
    Returns a dict with ALL schema_keys in schema order.
    Missing keys -> None.
    """
    raw_lc = {str(k).lower(): v for k, v in (raw or {}).items()}
    out: Dict[str, Any] = {}

    for k in schema_keys:
        v = None
        # direct
        if k in raw:
            v = raw.get(k)
        else:
            # case-insensitive match
            if k.lower() in raw_lc:
                v = raw_lc.get(k.lower())
            else:
                # alias scan
                aliases = _CANONICAL_ALIASES.get(k, ())
                for a in aliases:
                    if a in raw:
                        v = raw.get(a)
                        break
                    if a.lower() in raw_lc:
                        v = raw_lc.get(a.lower())
                        break
        out[k] = v

    # enforce symbol
    if "symbol" in out and not out.get("symbol"):
        out["symbol"] = symbol_fallback

    return out


# =============================================================================
# Request model (Phase 3)
# =============================================================================

class AdvancedSheetRowsRequest(BaseModel):
    page: str = Field(default="Market_Leaders", description="Sheet/page name or alias (e.g., 'Market Leaders').")
    symbols: List[str] = Field(default_factory=list)
    tickers: List[str] = Field(default_factory=list)  # backward compat
    top_n: int = Field(default=50, ge=1, le=2000)
    include_matrix: bool = Field(default=True, description="Include rows_matrix (legacy compatibility).")

    if _PYDANTIC_V2:
        model_config = ConfigDict(extra="ignore")

        @model_validator(mode="after")
        def _combine(self) -> "AdvancedSheetRowsRequest":
            all_syms = list(self.symbols or []) + list(self.tickers or [])
            out: List[str] = []
            seen = set()
            for s in all_syms:
                n = str(s).strip()
                if not n:
                    continue
                n = n.upper()
                if n.startswith("TADAWUL:"):
                    n = n.split(":", 1)[1]
                if n.endswith(".TADAWUL"):
                    n = n.replace(".TADAWUL", "")
                if n not in seen:
                    seen.add(n)
                    out.append(n)
            self.symbols = out
            self.tickers = []
            return self


# =============================================================================
# Routes
# =============================================================================

@router.get("/health")
async def advanced_health(request: Request) -> Dict[str, Any]:
    engine = await _data_engine.get_engine(request)
    return {
        "status": "ok",
        "version": ADVANCED_ANALYSIS_VERSION,
        "timestamp": _riyadh_time.now().isoformat(),
        "business_day": _riyadh_time.is_business_day(),
        "engine_available": bool(engine),
        "cache": _cache_manager.get_stats(),
        "schema_pages": list(CANONICAL_PAGES),
    }


@router.get("/metrics")
async def advanced_metrics() -> Response:
    if not PROMETHEUS_AVAILABLE:
        return BestJSONResponse(status_code=503, content={"error": "Metrics not available"})
    from prometheus_client import generate_latest, CONTENT_TYPE_LATEST  # type: ignore

    return Response(content=generate_latest(), media_type=CONTENT_TYPE_LATEST)


@router.post("/sheet-rows")
async def advanced_sheet_rows(
    request: Request,
    body: Dict[str, Any] = Body(...),
    mode: str = Query(default="", description="Optional mode hint for engine/provider"),
    token: Optional[str] = Query(default=None, description="Auth token (only if query token allowed)"),
    x_app_token: Optional[str] = Header(default=None, alias="X-APP-TOKEN"),
    authorization: Optional[str] = Header(default=None, alias="Authorization"),
    x_request_id: Optional[str] = Header(default=None, alias="X-Request-ID"),
) -> BestJSONResponse:
    """
    Phase 3 schema-driven sheet rows:
    - page aliases accepted
    - full schema headers/keys returned
    - rows are list of dicts with all schema keys (missing => null)
    """
    request_id = x_request_id or str(uuid.uuid4())
    start_time = time.time()

    # Rate limiting
    client_ip = request.client.host if request.client else "unknown"
    if not await _rate_limiter.check(client_ip, _CONFIG.rate_limit_requests, _CONFIG.rate_limit_window):
        raise HTTPException(status_code=status.HTTP_429_TOO_MANY_REQUESTS, detail="Rate limit exceeded")

    # ---- Auth (align to core.config if present) ----
    auth_token = (x_app_token or "").strip()
    if authorization and authorization.startswith("Bearer "):
        auth_token = authorization[7:].strip()
    if _CONFIG.allow_query_token and token and not auth_token:
        auth_token = token.strip()

    if auth_ok is not None:
        if not auth_ok(token=auth_token, authorization=authorization, headers={"X-APP-TOKEN": x_app_token, "Authorization": authorization}):
            raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid token")
    else:
        # fallback: if require auth and no token => deny
        if _CONFIG.require_auth and not auth_token:
            raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid token")

    # Parse request
    async with TraceContext("parse_request", {"request_id": request_id}):
        try:
            req = AdvancedSheetRowsRequest.model_validate(body) if _PYDANTIC_V2 else AdvancedSheetRowsRequest.parse_obj(body)  # type: ignore
        except Exception as e:
            return BestJSONResponse(
                status_code=400,
                content={
                    "status": "error",
                    "page": None,
                    "headers": [],
                    "keys": [],
                    "rows": [],
                    "error": f"Invalid request: {str(e)}",
                    "version": ADVANCED_ANALYSIS_VERSION,
                    "request_id": request_id,
                    "meta": {"duration_ms": (time.time() - start_time) * 1000},
                },
            )

    # Normalize page name (aliases -> canonical)
    try:
        page = normalize_page_name(req.page, allow_output_pages=True)
    except Exception as e:
        return BestJSONResponse(
            status_code=400,
            content={
                "status": "error",
                "page": req.page,
                "headers": [],
                "keys": [],
                "rows": [],
                "error": f"Invalid page: {str(e)}",
                "allowed_pages": list(CANONICAL_PAGES),
                "version": ADVANCED_ANALYSIS_VERSION,
                "request_id": request_id,
                "meta": {"duration_ms": (time.time() - start_time) * 1000},
            },
        )

    # Handle Data_Dictionary without engine calls
    if page == "Data_Dictionary":
        spec = get_sheet_spec("Data_Dictionary")
        headers = [c.header for c in spec.columns]
        keys = [c.key for c in spec.columns]

        rows_dict = build_data_dictionary_rows(include_meta_sheet=True)
        # Ensure dict rows have all keys in schema order
        normalized_rows: List[Dict[str, Any]] = []
        for r in rows_dict:
            normalized_rows.append(_normalize_row_to_schema(keys, r, symbol_fallback=""))

        payload: Dict[str, Any] = {
            "status": "success",
            "page": page,
            "headers": headers,
            "keys": keys,
            "rows": normalized_rows,
            "rows_matrix": [[row.get(k) for k in keys] for row in normalized_rows] if req.include_matrix else None,
            "version": ADVANCED_ANALYSIS_VERSION,
            "request_id": request_id,
            "meta": {
                "duration_ms": (time.time() - start_time) * 1000,
                "count": len(normalized_rows),
                "schema_mode": "data_dictionary",
            },
        }
        return BestJSONResponse(content=clean_nans(payload))

    # Schema for requested page
    try:
        sheet_spec = get_sheet_spec(page)
    except Exception as e:
        return BestJSONResponse(
            status_code=400,
            content={
                "status": "error",
                "page": page,
                "headers": [],
                "keys": [],
                "rows": [],
                "error": f"Schema not found for page '{page}': {str(e)}",
                "allowed_pages": list(CANONICAL_PAGES),
                "version": ADVANCED_ANALYSIS_VERSION,
                "request_id": request_id,
                "meta": {"duration_ms": (time.time() - start_time) * 1000},
            },
        )

    headers = [c.header for c in sheet_spec.columns]
    keys = [c.key for c in sheet_spec.columns]

    # Engine required for instrument-like pages
    engine = await _data_engine.get_engine(request)
    if not engine:
        return BestJSONResponse(
            status_code=503,
            content={
                "status": "error",
                "page": page,
                "headers": headers,
                "keys": keys,
                "rows": [],
                "error": "Data engine unavailable",
                "version": ADVANCED_ANALYSIS_VERSION,
                "request_id": request_id,
                "meta": {"duration_ms": (time.time() - start_time) * 1000},
            },
        )

    # Symbols (stable order)
    symbols = (req.symbols or [])[: int(req.top_n or 50)]

    async with TraceContext("fetch_quotes", {"page": page, "symbol_count": len(symbols), "mode": mode}):
        quotes = await _data_engine.get_quotes(engine, symbols, mode=mode)

    # Normalize each row to schema
    normalized_rows: List[Dict[str, Any]] = []
    for sym in symbols:
        raw = _to_plain_dict(quotes.get(sym))
        row = _normalize_row_to_schema(keys, raw, symbol_fallback=sym)
        normalized_rows.append(row)

    payload: Dict[str, Any] = {
        "status": "success",
        "page": page,
        "headers": headers,
        "keys": keys,
        "rows": normalized_rows,
        "rows_matrix": [[row.get(k) for k in keys] for row in normalized_rows] if req.include_matrix else None,
        "version": ADVANCED_ANALYSIS_VERSION,
        "request_id": request_id,
        "meta": {
            "duration_ms": (time.time() - start_time) * 1000,
            "requested": len(symbols),
            "cache_stats": _cache_manager.get_stats(),
            "riyadh_time": _riyadh_time.now().isoformat(),
            "business_day": _riyadh_time.is_business_day(),
        },
    }

    return BestJSONResponse(content=clean_nans(payload))


__all__ = ["router"]
