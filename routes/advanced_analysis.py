#!/usr/bin/env python3
"""
routes/advanced_analysis.py
------------------------------------------------------------
TADAWUL ADVANCED ANALYSIS ENGINE — v5.2.1 (ENTERPRISE PATCH)
PROD HARDENED + DISTRIBUTED CACHING + ML READY

v5.2.1 Fixes (production blockers / correctness)
- ✅ FIX: OpenTelemetry TraceContext corrected (start_as_current_span is a context manager; no attach/detach misuse).
- ✅ FIX: Token validation now respects REQUIRE_AUTH (no more accidental public “open mode”).
- ✅ FIX: Query token only honored when ALLOW_QUERY_TOKEN = true.
- ✅ FIX: Riyadh business day logic corrected for Saudi weekend (Fri/Sat) + holidays now configurable via SAUDI_MARKET_HOLIDAYS.
- ✅ FIX: Redis cache can use REDIS_URL (Render Key Value) in addition to REDIS_HOST/PORT.
- ✅ IMPROVE: JSON safety (NaN/Inf → None) to prevent orjson serialization issues.
- ✅ IMPROVE: Response building avoids unnecessary json_dumps/json_loads roundtrip.
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
# Logging
# ---------------------------------------------------------------------------
logger = logging.getLogger("routes.advanced_analysis")

ADVANCED_ANALYSIS_VERSION = "5.2.1"
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
        def set_attribute(self, *args, **kwargs):  # noqa
            return None

        def record_exception(self, *args, **kwargs):  # noqa
            return None

        def set_status(self, *args, **kwargs):  # noqa
            return None

        def end(self):  # noqa
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
    import aiocache
    from aiocache import Cache

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

class FallbackStrategy(str, Enum):
    CACHE_ONLY = "cache_only"
    STALE_WHILE_REVALIDATE = "stale_while_revalidate"
    CIRCUIT_BREAKER = "circuit_breaker"
    THROTTLED = "throttled"
    GRACEFUL = "graceful"

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
# Trace Context (FIXED)
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
# Riyadh Time / business day (FIXED weekend + configurable holidays)
# =============================================================================

class RiyadhTime:
    def __init__(self):
        self._tz = timezone(timedelta(hours=3))
        # Saudi weekend is Friday (4) + Saturday (5)
        self._weekend_days = {4, 5}

        env_h = os.getenv("SAUDI_MARKET_HOLIDAYS", "").strip()
        if env_h:
            self._holidays = {x.strip() for x in env_h.split(",") if x.strip()}
        else:
            # legacy fallback (keep, but you should set SAUDI_MARKET_HOLIDAYS in production)
            self._holidays = {
                "2024-02-22",
                "2024-04-10", "2024-04-11", "2024-04-12",
                "2024-06-16", "2024-06-17", "2024-06-18",
                "2024-09-23",
            }

    def now(self) -> datetime:
        return datetime.now(self._tz)

    def is_business_day(self, dt: Optional[datetime] = None) -> bool:
        dt = dt or self.now()
        if dt.weekday() in self._weekend_days:
            return False
        if dt.strftime("%Y-%m-%d") in self._holidays:
            return False
        return True

_riyadh_time = RiyadhTime()

# =============================================================================
# Audit logger
# =============================================================================

class AuditLogger:
    def __init__(self):
        self._buf: List[Dict[str, Any]] = []
        self._lock = asyncio.Lock()

    async def log(self, event: str, details: Dict[str, Any]) -> None:
        if not _CONFIG.retain_audit_log:
            return
        entry = {
            "timestamp": _riyadh_time.now().isoformat(),
            "event": event,
            "details": details,
            "version": ADVANCED_ANALYSIS_VERSION,
        }
        flush = False
        async with self._lock:
            self._buf.append(entry)
            if len(self._buf) >= 100:
                flush = True
        if flush:
            asyncio.create_task(self._flush())

    async def _flush(self) -> None:
        async with self._lock:
            batch = list(self._buf)
            self._buf.clear()
        try:
            logger.info("Audit flush: %s entries", len(batch))
        except Exception:
            pass

_audit_logger = AuditLogger()

# =============================================================================
# Auth + rate limiter
# =============================================================================

class TokenManager:
    def __init__(self):
        self._tokens: Set[str] = set()
        for key in ("APP_TOKEN", "BACKUP_APP_TOKEN", "TFB_APP_TOKEN"):
            t = os.getenv(key, "").strip()
            if t:
                self._tokens.add(t)

    def validate_token(self, token: str) -> bool:
        token = (token or "").strip()

        # ✅ FIX: if auth required but no tokens configured -> DENY
        if _CONFIG.require_auth:
            if not self._tokens:
                return False
            return token in self._tokens

        # auth not required -> allow unless tokens exist (then enforce)
        if not self._tokens:
            return True
        return token in self._tokens

_token_manager = TokenManager()

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
                # Prefer REDIS_URL (Render)
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
            # last resort
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
                # evict ~10% oldest by expiry
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
# Circuit breaker + coalescing (kept)
# =============================================================================

class CircuitState(str, Enum):
    CLOSED = "closed"
    OPEN = "open"
    HALF_OPEN = "half_open"

class CircuitBreaker:
    def __init__(self, name: str, threshold: int = 5, timeout: float = 60.0):
        self.name = name
        self.threshold = threshold
        self.timeout = timeout
        self.state = CircuitState.CLOSED
        self.failure_count = 0
        self.last_failure_time = 0.0
        self.success_count = 0
        self._lock = asyncio.Lock()

    async def call(self, func: Callable, *args, **kwargs) -> Any:
        async with self._lock:
            if self.state == CircuitState.OPEN:
                if time.time() - self.last_failure_time > self.timeout:
                    self.state = CircuitState.HALF_OPEN
                    self.success_count = 0
                else:
                    raise Exception(f"Circuit {self.name} is OPEN")

        try:
            result = await func(*args, **kwargs) if asyncio.iscoroutinefunction(func) else func(*args, **kwargs)
            async with self._lock:
                if self.state == CircuitState.HALF_OPEN:
                    self.success_count += 1
                    if self.success_count >= 2:
                        self.state = CircuitState.CLOSED
                        self.failure_count = 0
                else:
                    self.failure_count = 0
            return result
        except Exception as e:
            async with self._lock:
                self.failure_count += 1
                self.last_failure_time = time.time()
                if self.state == CircuitState.CLOSED and self.failure_count >= self.threshold:
                    self.state = CircuitState.OPEN
                elif self.state == CircuitState.HALF_OPEN:
                    self.state = CircuitState.OPEN
            raise e

class RequestCoalescer:
    def __init__(self):
        self._pending: Dict[str, asyncio.Future] = {}
        self._lock = asyncio.Lock()

    async def execute(self, key: str, coro: Awaitable) -> Any:
        async with self._lock:
            fut = self._pending.get(key)
            if fut:
                return await fut
            fut = asyncio.get_running_loop().create_future()
            self._pending[key] = fut

        try:
            res = await coro
            if not fut.done():
                fut.set_result(res)
        except Exception as e:
            if not fut.done():
                fut.set_exception(e)
        finally:
            async with self._lock:
                self._pending.pop(key, None)

        return await fut

_coalescer = RequestCoalescer()

# =============================================================================
# ML-ready models (kept minimal)
# =============================================================================

class MLFeatures(BaseModel):
    symbol: str
    price: Optional[float] = None
    volume: Optional[int] = None
    volatility_30d: Optional[float] = None
    momentum_14d: Optional[float] = None
    rsi_14d: Optional[float] = None
    macd: Optional[float] = None
    macd_signal: Optional[float] = None
    bb_upper: Optional[float] = None
    bb_lower: Optional[float] = None
    bb_middle: Optional[float] = None
    market_cap: Optional[float] = None
    pe_ratio: Optional[float] = None
    dividend_yield: Optional[float] = None
    beta: Optional[float] = None
    sharpe_ratio: Optional[float] = None
    max_drawdown_30d: Optional[float] = None
    correlation_tasi: Optional[float] = None

    if _PYDANTIC_V2:
        model_config = ConfigDict(arbitrary_types_allowed=True, extra="ignore")

class MLPrediction(BaseModel):
    symbol: str
    predicted_return_1d: Optional[float] = None
    predicted_return_1w: Optional[float] = None
    predicted_return_1m: Optional[float] = None
    confidence_1d: Optional[float] = None
    confidence_1w: Optional[float] = None
    confidence_1m: Optional[float] = None
    risk_level: Optional[str] = None
    factors: Dict[str, float] = Field(default_factory=dict)
    model_version: str = "5.2.x"

    if _PYDANTIC_V2:
        model_config = ConfigDict(arbitrary_types_allowed=True, extra="ignore")

class AdvancedSheetRequest(BaseModel):
    tickers: List[str] = Field(default_factory=list)
    symbols: List[str] = Field(default_factory=list)
    top_n: Optional[int] = Field(default=50, ge=1, le=1000)
    headers: Optional[List[str]] = None
    include_features: bool = False
    include_predictions: bool = False
    cache_ttl: Optional[int] = None

    if _PYDANTIC_V2:
        model_config = ConfigDict(extra="ignore")

        @model_validator(mode="after")
        def combine(self) -> "AdvancedSheetRequest":
            all_syms = list(self.tickers or []) + list(self.symbols or [])
            seen = set()
            out: List[str] = []
            for s in all_syms:
                n = str(s).strip().upper()
                if n.startswith("TADAWUL:"):
                    n = n.split(":", 1)[1]
                if n.endswith(".TADAWUL"):
                    n = n.replace(".TADAWUL", "")
                if n and n not in seen:
                    seen.add(n)
                    out.append(n)
            self.symbols = out
            self.tickers = []
            return self

# =============================================================================
# ML model manager (safe fallback)
# =============================================================================

class MLModelManager:
    def __init__(self):
        self._initialized = False
        self._lock = asyncio.Lock()

    async def load_models(self) -> None:
        if self._initialized:
            return
        async with self._lock:
            if self._initialized:
                return
            # project may not ship ml package; keep safe fallback
            self._initialized = True

    async def predict(self, features: MLFeatures) -> Optional[MLPrediction]:
        if not self._initialized:
            return None
        # fallback: no real ML available here
        return None

_ml_manager = MLModelManager()

# =============================================================================
# Core engine wrapper
# =============================================================================

class AdvancedDataEngine:
    def __init__(self):
        self._engine = None
        self._lock = asyncio.Lock()
        self._breaker = CircuitBreaker("data_engine", threshold=5, timeout=60.0)

    async def get_engine(self, request: Request) -> Optional[Any]:
        try:
            st = getattr(request.app, "state", None)
            if st and getattr(st, "engine", None):
                return st.engine
        except Exception:
            pass

        if self._engine is not None:
            return self._engine

        async def _init():
            async with self._lock:
                if self._engine is not None:
                    return self._engine
                try:
                    from core.data_engine_v2 import get_engine  # project expected
                    self._engine = await get_engine()
                    return self._engine
                except Exception as e:
                    logger.error("Engine initialization failed: %s", e)
                    return None

        return await self._breaker.call(_init)

    async def get_quotes(self, engine: Any, symbols: List[str], mode: str = "") -> Dict[str, Any]:
        # Cache lookup
        cache_keys = {s: f"quote:{s}:{mode}" for s in symbols}
        cached: Dict[str, Any] = {}
        for s, ck in cache_keys.items():
            v = await _cache_manager.get(ck)
            if v is not None:
                cached[s] = v

        to_fetch = [s for s in symbols if s not in cached]
        fetched: Dict[str, Any] = {}

        async def _fetch_all():
            if not to_fetch:
                return {}

            # try batch methods if present
            for method in ("get_enriched_quotes_batch", "get_enriched_quotes", "get_quotes_batch"):
                fn = getattr(engine, method, None)
                if callable(fn):
                    try:
                        res = await fn(to_fetch, mode=mode) if mode else await fn(to_fetch)
                        if isinstance(res, dict):
                            return res
                        if isinstance(res, list):
                            # best effort zip
                            return {s: r for s, r in zip(to_fetch, res)}
                    except Exception:
                        pass

            # fallback per-symbol
            sem = asyncio.Semaphore(_CONFIG.max_concurrency)
            async def fetch_one(sym: str):
                async with sem:
                    try:
                        q = await engine.get_enriched_quote(sym, mode=mode) if mode else await engine.get_enriched_quote(sym)
                        return sym, q
                    except Exception as e:
                        return sym, {"symbol": sym, "error": str(e), "data_quality": "MISSING"}

            pairs = await asyncio.gather(*(fetch_one(s) for s in to_fetch), return_exceptions=False)
            return {k: v for k, v in pairs}

        # Coalesce identical requests (important under load)
        coalesce_key = hashlib_key("quotes", to_fetch, mode)
        try:
            fetched = await _coalescer.execute(coalesce_key, _fetch_all())
        except Exception as e:
            logger.error("Fetch failed: %s", e)
            fetched = {s: {"symbol": s, "error": str(e), "data_quality": "MISSING"} for s in to_fetch}

        # write fetched to cache
        for s, v in fetched.items():
            if isinstance(v, dict) and "error" in v:
                continue
            await _cache_manager.set(cache_keys[s], v, ttl=_CONFIG.cache_ttl_seconds)

        out = dict(cached)
        out.update(fetched)
        return out

_data_engine = AdvancedDataEngine()

def hashlib_key(prefix: str, symbols: Sequence[str], mode: str) -> str:
    import hashlib
    raw = f"{prefix}|{mode}|{' '.join(symbols)}"
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()

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
        "require_auth": _CONFIG.require_auth,
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
    mode: str = Query(default="", description="Mode hint"),
    token: Optional[str] = Query(default=None, description="Auth token"),
    x_app_token: Optional[str] = Header(default=None, alias="X-APP-TOKEN"),
    authorization: Optional[str] = Header(default=None, alias="Authorization"),
    x_request_id: Optional[str] = Header(default=None, alias="X-Request-ID"),
) -> BestJSONResponse:
    request_id = x_request_id or str(uuid.uuid4())
    start_time = time.time()

    # Rate limiting
    client_ip = request.client.host if request.client else "unknown"
    if not await _rate_limiter.check(client_ip, _CONFIG.rate_limit_requests, _CONFIG.rate_limit_window):
        raise HTTPException(status_code=status.HTTP_429_TOO_MANY_REQUESTS, detail="Rate limit exceeded")

    # Auth token selection
    auth_token = (x_app_token or "").strip()

    if authorization and authorization.startswith("Bearer "):
        auth_token = authorization[7:].strip()

    # Query token only allowed if explicitly enabled
    if _CONFIG.allow_query_token and token and not auth_token:
        auth_token = token.strip()

    if not _token_manager.validate_token(auth_token):
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid token")

    # Parse request
    async with TraceContext("parse_request", {"request_id": request_id}):
        try:
            req = AdvancedSheetRequest.model_validate(body) if _PYDANTIC_V2 else AdvancedSheetRequest.parse_obj(body)  # type: ignore
        except Exception as e:
            return BestJSONResponse(
                status_code=400,
                content={
                    "status": "error",
                    "headers": [],
                    "rows": [],
                    "error": f"Invalid request: {str(e)}",
                    "version": ADVANCED_ANALYSIS_VERSION,
                    "request_id": request_id,
                    "meta": {"duration_ms": (time.time() - start_time) * 1000},
                },
            )

    if req.include_predictions:
        asyncio.create_task(_ml_manager.load_models())

    engine = await _data_engine.get_engine(request)
    if not engine:
        return BestJSONResponse(
            status_code=503,
            content={
                "status": "error",
                "headers": [],
                "rows": [],
                "error": "Data engine unavailable",
                "version": ADVANCED_ANALYSIS_VERSION,
                "request_id": request_id,
                "meta": {"duration_ms": (time.time() - start_time) * 1000},
            },
        )

    symbols = (req.symbols or req.tickers or [])[: int(req.top_n or 50)]

    async with TraceContext("fetch_quotes", {"symbol_count": len(symbols), "mode": mode}):
        quotes = await _data_engine.get_quotes(engine, symbols, mode=mode)

    headers = req.headers or [
        "Symbol", "Name", "Price", "Change", "Change %", "Volume", "Market Cap",
        "P/E Ratio", "Dividend Yield", "Beta", "RSI (14)", "MACD",
        "Volatility (30d)", "Momentum (14d)", "Sharpe Ratio",
        "Forecast Price (1M)", "Expected ROI (1M)",
        "Forecast Price (3M)", "Expected ROI (3M)",
        "Forecast Price (12M)", "Expected ROI (12M)",
        "Risk Score", "Overall Score", "Recommendation", "Data Quality",
        "Last Updated (UTC)", "Last Updated (Riyadh)",
    ]

    rows: List[List[Any]] = []
    features_dict: Optional[Dict[str, Any]] = {} if req.include_features else None
    predictions_dict: Optional[Dict[str, Any]] = {} if req.include_predictions else None

    def normalize_quote(q: Any) -> Dict[str, Any]:
        if q is None:
            return {}
        if isinstance(q, dict):
            return q
        if hasattr(q, "model_dump"):
            return q.model_dump(mode="python")
        if hasattr(q, "dict"):
            return q.dict()
        if is_dataclass(q):
            return {k: getattr(q, k) for k in q.__dict__.keys() if not str(k).startswith("_")}
        return {}

    for sym in symbols:
        q = normalize_quote(quotes.get(sym))
        lookup = {str(k).lower(): v for k, v in q.items()} if isinstance(q, dict) else {}

        def get_any(*keys: str) -> Any:
            for k in keys:
                if k in lookup:
                    return lookup.get(k)
            return None

        row: List[Any] = []
        for h in headers:
            hl = h.lower()
            if "symbol" == hl:
                row.append(get_any("symbol", "ticker") or sym)
            elif "name" in hl:
                row.append(get_any("name", "company_name") or "")
            elif hl.startswith("price") and "forecast" not in hl:
                row.append(get_any("price", "current_price"))
            elif hl.startswith("change %") or "change%" in hl:
                row.append(get_any("change_percent", "change_pct"))
            elif hl.startswith("change"):
                row.append(get_any("change", "price_change"))
            elif "volume" in hl:
                row.append(get_any("volume"))
            elif "market cap" in hl:
                row.append(get_any("market_cap"))
            elif "p/e" in hl or "pe ratio" in hl:
                row.append(get_any("pe_ratio", "pe_ttm"))
            elif "dividend yield" in hl:
                row.append(get_any("dividend_yield"))
            elif "beta" in hl:
                row.append(get_any("beta"))
            elif "rsi" in hl:
                row.append(get_any("rsi_14d", "rsi_14"))
            elif "macd" in hl and "signal" not in hl:
                row.append(get_any("macd", "macd_line"))
            elif "volatility" in hl:
                row.append(get_any("volatility_30d"))
            elif "momentum" in hl:
                row.append(get_any("momentum_14d", "returns_1m"))
            elif "sharpe" in hl:
                row.append(get_any("sharpe_ratio"))
            elif "forecast price" in hl and "1m" in hl:
                row.append(get_any("forecast_price_1m"))
            elif "expected roi" in hl and "1m" in hl:
                row.append(get_any("expected_roi_1m"))
            elif "forecast price" in hl and "3m" in hl:
                row.append(get_any("forecast_price_3m"))
            elif "expected roi" in hl and "3m" in hl:
                row.append(get_any("expected_roi_3m"))
            elif "forecast price" in hl and "12m" in hl:
                row.append(get_any("forecast_price_12m"))
            elif "expected roi" in hl and "12m" in hl:
                row.append(get_any("expected_roi_12m"))
            elif "risk score" in hl:
                row.append(get_any("risk_score"))
            elif "overall score" in hl:
                row.append(get_any("overall_score"))
            elif "recommendation" in hl:
                row.append(get_any("recommendation") or "HOLD")
            elif "data quality" in hl:
                row.append(get_any("data_quality") or ("GOOD" if q else "MISSING"))
            elif "last updated (utc)" in hl:
                row.append(get_any("last_updated_utc"))
            elif "last updated (riyadh)" in hl:
                row.append(get_any("last_updated_riyadh") or _riyadh_time.now().isoformat())
            else:
                row.append(get_any(hl))
        rows.append(row)

        if req.include_features and features_dict is not None:
            features_dict[sym] = MLFeatures(
                symbol=sym,
                price=get_any("price", "current_price"),
                volume=get_any("volume"),
                volatility_30d=get_any("volatility_30d"),
                momentum_14d=get_any("momentum_14d", "returns_1m"),
                rsi_14d=get_any("rsi_14d", "rsi_14"),
                macd=get_any("macd", "macd_line"),
                macd_signal=get_any("macd_signal"),
                bb_upper=get_any("bb_upper"),
                bb_lower=get_any("bb_lower"),
                bb_middle=get_any("bb_middle"),
                market_cap=get_any("market_cap"),
                pe_ratio=get_any("pe_ratio", "pe_ttm"),
                dividend_yield=get_any("dividend_yield"),
                beta=get_any("beta"),
                sharpe_ratio=get_any("sharpe_ratio"),
                max_drawdown_30d=get_any("max_drawdown_30d"),
                correlation_tasi=get_any("correlation_tasi"),
            )

        if req.include_predictions and predictions_dict is not None and features_dict is not None:
            pred = await _ml_manager.predict(features_dict[sym])
            if pred:
                predictions_dict[sym] = pred

    error_count = 0
    for q in quotes.values():
        if isinstance(q, dict) and q.get("error"):
            error_count += 1

    status_str = "success" if error_count == 0 else ("partial" if error_count < len(symbols) else "error")

    asyncio.create_task(_audit_logger.log(
        "sheet_rows_request",
        {"request_id": request_id, "symbols": len(symbols), "status": status_str, "duration_ms": (time.time() - start_time) * 1000},
    ))

    payload: Dict[str, Any] = {
        "status": status_str,
        "headers": headers,
        "rows": rows,
        "features": features_dict,
        "predictions": predictions_dict,
        "error": f"{error_count} errors" if error_count else None,
        "version": ADVANCED_ANALYSIS_VERSION,
        "request_id": request_id,
        "meta": {
            "duration_ms": (time.time() - start_time) * 1000,
            "requested": len(symbols),
            "errors": error_count,
            "cache_stats": _cache_manager.get_stats(),
            "riyadh_time": _riyadh_time.now().isoformat(),
            "business_day": _riyadh_time.is_business_day(),
        },
    }

    return BestJSONResponse(content=clean_nans(payload))


__all__ = ["router"]
