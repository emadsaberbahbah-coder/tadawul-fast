#!/usr/bin/env python3
"""
routes/ai_analysis.py
------------------------------------------------------------
TADAWUL ENTERPRISE AI ANALYSIS ENGINE — v8.5.3 (STABLE + FALLBACK)
SAMA Compliant | Resilient Routes | Safe Tracing | No Raw 500s

v8.5.3 Fixes
- ✅ Adds fallback to /v1/enriched/quote when engine returns "No providers available"
- ✅ Tries both symbol forms for Saudi tickers: 1120 and 1120.SR
- ✅ Keeps TraceContext fix (no set_attributes crash)
"""

from __future__ import annotations

import asyncio
import hashlib
import logging
import math
import os
import random
import time
import traceback
import uuid
import zlib
import pickle
from dataclasses import dataclass, field, is_dataclass
from datetime import datetime, timedelta, timezone
from enum import Enum
from typing import Any, Dict, List, Optional, Set, Tuple, Union

from fastapi import APIRouter, Body, Header, HTTPException, Query, Request, Response, WebSocket, WebSocketDisconnect, status

# -----------------------------------------------------------------------------
# Utility: NaN/Inf safe serialization
# -----------------------------------------------------------------------------
def clean_nans(obj: Any) -> Any:
    if isinstance(obj, dict):
        return {k: clean_nans(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [clean_nans(v) for v in obj]
    if isinstance(obj, float) and (math.isnan(obj) or math.isinf(obj)):
        return None
    return obj

# -----------------------------------------------------------------------------
# JSON response (orjson if available)
# -----------------------------------------------------------------------------
try:
    import orjson
    from fastapi.responses import Response as StarletteResponse

    class BestJSONResponse(StarletteResponse):
        media_type = "application/json"
        def render(self, content: Any) -> bytes:
            return orjson.dumps(clean_nans(content), default=str)

    _HAS_ORJSON = True
except Exception:
    import json
    from fastapi.responses import JSONResponse as _JSONResponse

    class BestJSONResponse(_JSONResponse):
        def render(self, content: Any) -> bytes:
            return json.dumps(clean_nans(content), default=str, ensure_ascii=False).encode("utf-8")

    _HAS_ORJSON = False

logger = logging.getLogger("routes.ai_analysis")

AI_ANALYSIS_VERSION = "8.5.3"
router = APIRouter(prefix="/v1/analysis", tags=["AI & Analysis"])

# -----------------------------------------------------------------------------
# Optional HTTP client for internal fallback
# -----------------------------------------------------------------------------
try:
    import httpx  # type: ignore
    _HTTPX_AVAILABLE = True
except Exception:
    httpx = None
    _HTTPX_AVAILABLE = False

# -----------------------------------------------------------------------------
# Optional OpenTelemetry (safe)
# -----------------------------------------------------------------------------
try:
    from opentelemetry import trace
    from opentelemetry.trace import Status, StatusCode
    _OTEL_AVAILABLE = True
    _TRACER = trace.get_tracer(__name__)
except Exception:
    trace = None
    Status = None
    StatusCode = None
    _OTEL_AVAILABLE = False
    _TRACER = None

# -----------------------------------------------------------------------------
# Optional Prometheus
# -----------------------------------------------------------------------------
try:
    from prometheus_client import Counter, Histogram, Gauge, generate_latest, CONTENT_TYPE_LATEST
    _PROMETHEUS_AVAILABLE = True
except Exception:
    _PROMETHEUS_AVAILABLE = False

# -----------------------------------------------------------------------------
# Optional Redis (use redis.asyncio if available)
# -----------------------------------------------------------------------------
try:
    import redis.asyncio as redis_async
    _REDIS_AVAILABLE = True
except Exception:
    redis_async = None
    _REDIS_AVAILABLE = False

# -----------------------------------------------------------------------------
# Pydantic (v2 preferred)
# -----------------------------------------------------------------------------
try:
    from pydantic import BaseModel, ConfigDict, Field, model_validator
    _PYDANTIC_V2 = True
except Exception:
    from pydantic import BaseModel, Field  # type: ignore
    _PYDANTIC_V2 = False

# =============================================================================
# Enums (Flexible)
# =============================================================================
class FlexibleEnum(str, Enum):
    @classmethod
    def _missing_(cls, value: object) -> Any:
        val = str(value).upper().replace(" ", "_")
        for member in cls:
            if member.name == val or str(member.value).upper() == val:
                return member
        return None

class AssetClass(FlexibleEnum):
    EQUITY = "EQUITY"
    SUKUK = "SUKUK"
    REIT = "REIT"
    ETF = "ETF"
    COMMODITY = "COMMODITY"
    CURRENCY = "CURRENCY"
    CRYPTO = "CRYPTO"
    DERIVATIVE = "DERIVATIVE"

class MarketRegion(FlexibleEnum):
    SAUDI = "SAUDI"
    UAE = "UAE"
    QATAR = "QATAR"
    KUWAIT = "KUWAIT"
    BAHRAIN = "BAHRAIN"
    OMAN = "OMAN"
    EGYPT = "EGYPT"
    USA = "USA"
    GLOBAL = "GLOBAL"

class DataQuality(FlexibleEnum):
    EXCELLENT = "EXCELLENT"
    GOOD = "GOOD"
    FAIR = "FAIR"
    POOR = "POOR"
    MISSING = "MISSING"
    STALE = "STALE"
    HIGH = "HIGH"
    MEDIUM = "MEDIUM"
    LOW = "LOW"
    ERROR = "ERROR"

class Recommendation(FlexibleEnum):
    STRONG_BUY = "STRONG_BUY"
    BUY = "BUY"
    HOLD = "HOLD"
    REDUCE = "REDUCE"
    SELL = "SELL"
    STRONG_SELL = "STRONG_SELL"
    UNDER_REVIEW = "UNDER_REVIEW"

class ConfidenceLevel(FlexibleEnum):
    VERY_HIGH = "VERY_HIGH"
    HIGH = "HIGH"
    MEDIUM = "MEDIUM"
    LOW = "LOW"
    VERY_LOW = "VERY_LOW"

class SignalType(FlexibleEnum):
    BULLISH = "BULLISH"
    BEARISH = "BEARISH"
    NEUTRAL = "NEUTRAL"
    OVERBOUGHT = "OVERBOUGHT"
    OVERSOLD = "OVERSOLD"
    DIVERGENCE = "DIVERGENCE"

class ShariahCompliance(FlexibleEnum):
    COMPLIANT = "COMPLIANT"
    NON_COMPLIANT = "NON_COMPLIANT"
    PENDING = "PENDING"
    NA = "NA"

# =============================================================================
# Time (Riyadh)
# =============================================================================
class SaudiTime:
    def __init__(self):
        self._tz = timezone(timedelta(hours=3))
        self._weekend_days = [4, 5]  # Fri=4, Sat=5 (Mon=0)
        self._trading_hours = {"start": "10:00", "end": "15:00"}

    def now(self) -> datetime:
        return datetime.now(self._tz)

    def now_iso(self) -> str:
        return self.now().isoformat(timespec="milliseconds")

    def now_utc_iso(self) -> str:
        return datetime.now(timezone.utc).isoformat(timespec="milliseconds")

    def is_trading_day(self, dt: Optional[datetime] = None) -> bool:
        dt = dt or self.now()
        return dt.weekday() not in self._weekend_days

_saudi_time = SaudiTime()

# =============================================================================
# Config
# =============================================================================
@dataclass(slots=True)
class AIConfig:
    batch_concurrency: int = 6
    max_tickers: int = 1200
    cache_ttl_seconds: int = 300
    cache_max_size: int = 10000
    enable_redis_cache: bool = True
    redis_url: str = "redis://localhost:6379"

    adaptive_concurrency: bool = True

    allow_query_token: bool = False
    require_api_key: bool = True
    enable_tracing: bool = True
    trace_sample_rate: float = 1.0

    enable_scoreboard: bool = True
    enable_websocket: bool = False

_CONFIG = AIConfig()

def _load_config_from_env() -> None:
    def env_int(name: str, default: int) -> int:
        try: return int(os.getenv(name, str(default)).strip())
        except Exception: return default

    def env_float(name: str, default: float) -> float:
        try: return float(os.getenv(name, str(default)).strip())
        except Exception: return default

    def env_bool(name: str, default: bool) -> bool:
        v = os.getenv(name, str(default)).strip().lower()
        return v in ("true", "1", "yes", "on", "y")

    _CONFIG.batch_concurrency = env_int("AI_BATCH_CONCURRENCY", _CONFIG.batch_concurrency)
    _CONFIG.max_tickers = env_int("AI_MAX_TICKERS", _CONFIG.max_tickers)
    _CONFIG.cache_ttl_seconds = env_int("AI_CACHE_TTL", _CONFIG.cache_ttl_seconds)
    _CONFIG.cache_max_size = env_int("AI_CACHE_MAX_SIZE", _CONFIG.cache_max_size)
    _CONFIG.enable_redis_cache = env_bool("AI_ENABLE_REDIS", _CONFIG.enable_redis_cache)
    _CONFIG.redis_url = os.getenv("REDIS_URL", _CONFIG.redis_url) or _CONFIG.redis_url

    _CONFIG.allow_query_token = env_bool("ALLOW_QUERY_TOKEN", _CONFIG.allow_query_token)
    _CONFIG.require_api_key = env_bool("REQUIRE_API_KEY", _CONFIG.require_api_key)

    _CONFIG.enable_tracing = env_bool("CORE_TRACING_ENABLED", _CONFIG.enable_tracing)
    _CONFIG.trace_sample_rate = env_float("TRACE_SAMPLE_RATE", _CONFIG.trace_sample_rate)

    _CONFIG.adaptive_concurrency = env_bool("ADAPTIVE_CONCURRENCY", _CONFIG.adaptive_concurrency)

_load_config_from_env()

# =============================================================================
# Metrics
# =============================================================================
class MetricsRegistry:
    def __init__(self, namespace: str = "tadawul_ai"):
        self.namespace = namespace
        self._counters: Dict[str, Any] = {}

    def counter(self, name: str, description: str, labels: Optional[List[str]] = None) -> Any:
        if not _PROMETHEUS_AVAILABLE:
            return None
        if name not in self._counters:
            self._counters[name] = Counter(f"{self.namespace}_{name}", description, labelnames=labels or [])
        return self._counters.get(name)

_metrics = MetricsRegistry()

# =============================================================================
# ✅ TraceContext (FIXED)
# =============================================================================
class TraceContext:
    def __init__(self, name: str, attributes: Optional[Dict[str, Any]] = None):
        self.name = name
        self.attributes = attributes or {}
        self.enabled = bool(_OTEL_AVAILABLE and _CONFIG.enable_tracing and _TRACER is not None)
        self._cm = None
        self.span = None

    async def __aenter__(self):
        if not self.enabled:
            return self
        if random.random() > float(_CONFIG.trace_sample_rate or 1.0):
            return self
        try:
            self._cm = _TRACER.start_as_current_span(self.name)
            self.span = self._cm.__enter__()
            for k, v in self.attributes.items():
                try:
                    if self.span:
                        self.span.set_attribute(str(k), v)
                except Exception:
                    pass
        except Exception:
            self._cm = None
            self.span = None
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if not self._cm:
            return False
        try:
            if exc_val and self.span and Status is not None and StatusCode is not None:
                try: self.span.record_exception(exc_val)
                except Exception: pass
                try: self.span.set_status(Status(StatusCode.ERROR, str(exc_val)))
                except Exception: pass
        finally:
            try: self._cm.__exit__(exc_type, exc_val, exc_tb)
            except Exception: pass
        return False

# =============================================================================
# Auth & Rate limiter
# =============================================================================
class TokenManager:
    def __init__(self):
        self._tokens: Set[str] = set()
        for key in ("APP_TOKEN", "BACKUP_APP_TOKEN", "TFB_APP_TOKEN"):
            t = (os.getenv(key, "") or "").strip()
            if t:
                self._tokens.add(t)

    def validate_token(self, token: str) -> bool:
        if not _CONFIG.require_api_key:
            return True
        if not self._tokens:
            return True
        return (token or "").strip() in self._tokens

_token_manager = TokenManager()

class RateLimiter:
    def __init__(self):
        self._buckets: Dict[str, Tuple[int, float]] = {}
        self._lock = asyncio.Lock()
        self.requests = int(os.getenv("RATE_LIMIT_REQUESTS", "120"))
        self.window = int(os.getenv("RATE_LIMIT_WINDOW", "60"))

    async def check(self, key: str) -> bool:
        if self.requests <= 0:
            return True
        async with self._lock:
            now = time.time()
            count, reset = self._buckets.get(key, (0, now + self.window))
            if now > reset:
                count, reset = 0, now + self.window
            if count < self.requests:
                self._buckets[key] = (count + 1, reset)
                return True
            return False

_rate_limiter = RateLimiter()

def _extract_auth_token(token_q: Optional[str], x_app_token: Optional[str], authorization: Optional[str]) -> str:
    if authorization:
        a = authorization.strip()
        if a.lower().startswith("bearer "):
            return a.split(" ", 1)[1].strip()
        return a
    if token_q and token_q.strip():
        return token_q.strip()
    if x_app_token and x_app_token.strip():
        return x_app_token.strip()
    return ""

# =============================================================================
# Cache Manager
# =============================================================================
class CacheManager:
    def __init__(self):
        self._local: Dict[str, Tuple[Any, float]] = {}
        self._lock = asyncio.Lock()
        self._redis = None
        self._redis_inited = False
        self._stats = {"hits": 0, "misses": 0, "redis_hits": 0, "redis_misses": 0}

    def _key(self, key: str, namespace: str = "ai") -> str:
        return f"{namespace}:{hashlib.md5(key.encode()).hexdigest()[:32]}"

    def _compress(self, data: Any) -> bytes:
        return zlib.compress(pickle.dumps(data), level=6)

    def _decompress(self, data: bytes) -> Any:
        try:
            return pickle.loads(zlib.decompress(data))
        except Exception:
            return pickle.loads(data)

    async def _ensure_redis(self) -> None:
        if self._redis_inited:
            return
        self._redis_inited = True
        if not (_CONFIG.enable_redis_cache and _REDIS_AVAILABLE and redis_async):
            return
        try:
            self._redis = redis_async.from_url(_CONFIG.redis_url, encoding=None, decode_responses=False)
            logger.info(f"Redis cache enabled: {_CONFIG.redis_url}")
        except Exception as e:
            logger.warning(f"Redis init failed: {e}")
            self._redis = None

    async def get(self, key: str, namespace: str = "ai") -> Optional[Any]:
        ck = self._key(key, namespace)
        now = time.time()

        async with self._lock:
            if ck in self._local:
                v, exp = self._local[ck]
                if exp > now:
                    self._stats["hits"] += 1
                    return v
                del self._local[ck]

        await self._ensure_redis()
        if self._redis:
            try:
                raw = await self._redis.get(ck)
                if raw is not None:
                    self._stats["redis_hits"] += 1
                    self._stats["hits"] += 1
                    v = self._decompress(raw)
                    async with self._lock:
                        self._local[ck] = (v, now + (_CONFIG.cache_ttl_seconds // 2))
                    return v
                self._stats["redis_misses"] += 1
            except Exception:
                self._stats["redis_misses"] += 1

        self._stats["misses"] += 1
        return None

    async def set(self, key: str, value: Any, ttl: Optional[int] = None, namespace: str = "ai") -> None:
        ck = self._key(key, namespace)
        ttl = int(ttl or _CONFIG.cache_ttl_seconds)
        exp = time.time() + ttl

        async with self._lock:
            if len(self._local) >= _CONFIG.cache_max_size:
                items = sorted(self._local.items(), key=lambda x: x[1][1])
                for k, _ in items[: max(1, _CONFIG.cache_max_size // 5)]:
                    self._local.pop(k, None)
            self._local[ck] = (value, exp)

        await self._ensure_redis()
        if self._redis:
            try:
                await self._redis.setex(ck, ttl, self._compress(value))
            except Exception:
                pass

    def stats(self) -> Dict[str, Any]:
        total = self._stats["hits"] + self._stats["misses"]
        return {
            **self._stats,
            "hit_rate": (self._stats["hits"] / total) if total else 0.0,
            "local_cache_size": len(self._local),
            "redis_enabled": bool(self._redis),
        }

_cache = CacheManager()

# =============================================================================
# Pydantic Models (extra="ignore")
# =============================================================================
def _pydantic_config():
    if _PYDANTIC_V2:
        return ConfigDict(arbitrary_types_allowed=True, use_enum_values=True, extra="ignore")
    class _Cfg:
        arbitrary_types_allowed = True
        use_enum_values = True
        extra = "ignore"
    return _Cfg

class SingleAnalysisResponse(BaseModel):
    symbol: str
    name: Optional[str] = None
    price: Optional[float] = None
    change_pct: Optional[float] = None
    recommendation: Recommendation = Recommendation.HOLD
    data_quality: DataQuality = DataQuality.MISSING
    error: Optional[str] = None
    last_updated_riyadh: Optional[str] = None

    if _PYDANTIC_V2:
        model_config = _pydantic_config()
    else:
        class Config(_pydantic_config()):  # type: ignore
            pass

class BatchAnalysisResponse(BaseModel):
    results: List[SingleAnalysisResponse] = Field(default_factory=list)
    status: str = "success"
    error: Optional[str] = None
    version: str = AI_ANALYSIS_VERSION
    meta: Dict[str, Any] = Field(default_factory=dict)

    if _PYDANTIC_V2:
        model_config = _pydantic_config()
    else:
        class Config(_pydantic_config()):  # type: ignore
            pass

class SheetAnalysisResponse(BaseModel):
    status: str = "success"
    headers: List[str] = Field(default_factory=list)
    rows: List[List[Any]] = Field(default_factory=list)
    error: Optional[str] = None
    version: str = AI_ANALYSIS_VERSION
    request_id: str
    meta: Dict[str, Any] = Field(default_factory=dict)

    if _PYDANTIC_V2:
        model_config = _pydantic_config()
    else:
        class Config(_pydantic_config()):  # type: ignore
            pass

class AdvancedSheetRequest(BaseModel):
    tickers: List[str] = Field(default_factory=list)
    symbols: List[str] = Field(default_factory=list)
    top_n: Optional[int] = Field(default=50, ge=1, le=1000)
    sheet_name: Optional[str] = None
    headers: Optional[List[str]] = None

    if _PYDANTIC_V2:
        model_config = ConfigDict(extra="ignore")

        @model_validator(mode="after")
        def combine_tickers(self) -> "AdvancedSheetRequest":
            all_symbols = list(self.tickers or []) + list(self.symbols or [])
            seen: Set[str] = set()
            out: List[str] = []
            for s in all_symbols:
                x = str(s).strip().upper()
                if x.startswith("TADAWUL:"):
                    x = x.split(":", 1)[1]
                if x.endswith(".TADAWUL"):
                    x = x.replace(".TADAWUL", "")
                if x and x not in seen:
                    seen.add(x)
                    out.append(x)
            self.symbols = out
            self.tickers = []
            return self

# =============================================================================
# Engine Access + Enriched Fallback
# =============================================================================
def _to_dict(obj: Any) -> Dict[str, Any]:
    if obj is None:
        return {}
    if isinstance(obj, dict):
        return obj
    try:
        if hasattr(obj, "model_dump"):
            return obj.model_dump()
        if hasattr(obj, "dict"):
            return obj.dict()
        if is_dataclass(obj):
            return {k: v for k, v in obj.__dict__.items() if not k.startswith("_")}
    except Exception:
        pass
    return {"raw": str(obj)}

def _internal_base_url() -> str:
    port = os.getenv("PORT", "10000").strip() or "10000"
    return f"http://127.0.0.1:{port}"

def _needs_provider_fallback(d: Dict[str, Any]) -> bool:
    err = str(d.get("error") or "").lower()
    if not err:
        return False
    return ("no providers available" in err) or ("providers available" in err)

async def _fetch_enriched_via_local_http(symbol: str, auth_token: str) -> Optional[Dict[str, Any]]:
    if not _HTTPX_AVAILABLE or httpx is None:
        return None
    base = _internal_base_url()
    url = f"{base}/v1/enriched/quote"
    headers: Dict[str, str] = {}
    if auth_token:
        headers["Authorization"] = f"Bearer {auth_token}"
        headers["X-APP-TOKEN"] = auth_token

    try:
        async with httpx.AsyncClient(timeout=10.0) as client:
            r = await client.get(url, params={"symbol": symbol, "debug": False, "include_headers": False, "include_rows": False}, headers=headers)
            if r.status_code != 200:
                return None
            data = r.json()
            if isinstance(data, dict):
                return data
            return None
    except Exception:
        return None

class AnalysisEngine:
    def __init__(self):
        self._engine_lock = asyncio.Lock()

    async def get_engine(self, request: Request) -> Optional[Any]:
        try:
            st = getattr(request.app, "state", None)
            if st and getattr(st, "engine", None):
                return st.engine
        except Exception:
            pass

        async with self._engine_lock:
            try:
                from core.data_engine_v2 import get_engine  # type: ignore
                eng = get_engine()
                if hasattr(eng, "__await__"):
                    eng = await eng
                return eng
            except Exception:
                pass
            try:
                from core.data_engine import get_engine  # type: ignore
                eng = get_engine()
                if hasattr(eng, "__await__"):
                    eng = await eng
                return eng
            except Exception:
                return None

    async def fetch_enriched(self, request: Request, engine: Any, symbols: List[str], auth_token: str) -> Dict[str, Dict[str, Any]]:
        """
        Fetch enriched quotes (best-effort). If engine returns "No providers available",
        fallback to local /v1/enriched/quote and try .SR if needed.
        """
        out: Dict[str, Dict[str, Any]] = {}

        # Batch methods first
        for method in ("get_enriched_quotes_batch", "get_enriched_quotes", "enriched_quotes", "get_quotes_batch"):
            fn = getattr(engine, method, None)
            if callable(fn):
                try:
                    res = fn(symbols)
                    if hasattr(res, "__await__"):
                        res = await res
                    if isinstance(res, dict):
                        for s in symbols:
                            out[s] = _to_dict(res.get(s))
                        break
                except Exception:
                    pass

        # Fallback: per symbol via engine
        if not out:
            per_methods = ("get_enriched_quote", "enriched_quote", "get_quote", "quote")
            sem = asyncio.Semaphore(max(1, int(_CONFIG.batch_concurrency)))

            async def one(s: str):
                async with sem:
                    last_err = None
                    for m in per_methods:
                        fn = getattr(engine, m, None)
                        if callable(fn):
                            try:
                                r = fn(s)
                                if hasattr(r, "__await__"):
                                    r = await r
                                return s, _to_dict(r)
                            except Exception as e:
                                last_err = str(e)
                                continue
                    return s, {"symbol": s, "error": last_err or "No engine method for enriched quote", "data_quality": "MISSING"}

            results = await asyncio.gather(*(one(s) for s in symbols), return_exceptions=True)
            for r in results:
                if isinstance(r, tuple) and len(r) == 2:
                    out[r[0]] = r[1]

        # ✅ Fallback to /v1/enriched/quote for provider failures
        for s in list(symbols):
            d = out.get(s) or {"symbol": s, "error": "Empty result", "data_quality": "MISSING"}
            if _needs_provider_fallback(d):
                candidates = [s]
                # if it's a pure numeric Saudi ticker, also try ".SR"
                if s.isdigit() and len(s) in (3, 4, 5):
                    candidates.append(f"{s}.SR")
                # if ".SR" given, also try numeric
                if s.upper().endswith(".SR"):
                    candidates.append(s.split(".", 1)[0])

                fixed = None
                for c in candidates:
                    fixed = await _fetch_enriched_via_local_http(c, auth_token=auth_token)
                    if isinstance(fixed, dict) and not _needs_provider_fallback(fixed) and not fixed.get("error"):
                        break
                    fixed = None

                if fixed:
                    out[s] = fixed

        # normalize timestamps
        for s in symbols:
            d = out.get(s) or {}
            d.setdefault("symbol", s)
            d.setdefault("last_updated_utc", _saudi_time.now_utc_iso())
            d.setdefault("last_updated_riyadh", _saudi_time.now_iso())
            out[s] = d

        return out

_analysis_engine = AnalysisEngine()

# =============================================================================
# Helpers
# =============================================================================
def _default_headers() -> List[str]:
    return ["Symbol", "Name", "Price", "Recommendation", "Data Quality", "Error", "Last Updated Riyadh"]

def _row_from_enriched(d: Dict[str, Any]) -> List[Any]:
    return [
        d.get("symbol") or d.get("ticker"),
        d.get("name") or "",
        d.get("price") or d.get("current_price"),
        d.get("recommendation") or "HOLD",
        d.get("data_quality") or ("MISSING" if d.get("error") else "GOOD"),
        d.get("error"),
        d.get("last_updated_riyadh") or _saudi_time.now_iso(),
    ]

# =============================================================================
# Routes
# =============================================================================
@router.get("/health")
async def health(request: Request) -> Dict[str, Any]:
    engine = await _analysis_engine.get_engine(request)
    return {
        "status": "ok" if engine else "degraded",
        "version": AI_ANALYSIS_VERSION,
        "timestamp_riyadh": _saudi_time.now_iso(),
        "engine": {"available": bool(engine), "type": type(engine).__name__ if engine else "none"},
        "cache": _cache.stats(),
        "request_id": getattr(request.state, "request_id", None),
    }

@router.post("/sheet-rows", response_model=SheetAnalysisResponse)
async def sheet_rows(
    request: Request,
    body: Dict[str, Any] = Body(...),
    mode: str = Query(default="", description="Mode hint"),
    token: Optional[str] = Query(default=None, description="Auth token"),
    x_app_token: Optional[str] = Header(default=None, alias="X-APP-TOKEN"),
    authorization: Optional[str] = Header(default=None, alias="Authorization"),
    x_request_id: Optional[str] = Header(default=None, alias="X-Request-ID"),
) -> BestJSONResponse:
    start = time.time()
    request_id = x_request_id or getattr(request.state, "request_id", None) or str(uuid.uuid4())[:18]

    try:
        client_ip = request.client.host if request.client else "unknown"
        if not await _rate_limiter.check(client_ip):
            raise HTTPException(status_code=status.HTTP_429_TOO_MANY_REQUESTS, detail="Rate limit exceeded")

        auth_token = _extract_auth_token(token if _CONFIG.allow_query_token else None, x_app_token, authorization)
        if not _token_manager.validate_token(auth_token):
            raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid token")

        async with TraceContext("analysis.sheet_rows.parse", {"request_id": request_id}):
            try:
                req = AdvancedSheetRequest.model_validate(body) if _PYDANTIC_V2 else AdvancedSheetRequest.parse_obj(body)  # type: ignore
            except Exception as e:
                err_resp = SheetAnalysisResponse(
                    status="error",
                    headers=[],
                    rows=[],
                    error=f"Invalid request: {e}",
                    version=AI_ANALYSIS_VERSION,
                    request_id=request_id,
                    meta={"duration_ms": (time.time() - start) * 1000},
                )
                return BestJSONResponse(status_code=400, content=err_resp.model_dump(exclude_none=True) if _PYDANTIC_V2 else err_resp.dict(exclude_none=True))  # type: ignore

        symbols = (req.symbols or [])[: (req.top_n or 50)]
        headers = req.headers or _default_headers()

        if not symbols:
            resp = SheetAnalysisResponse(
                status="skipped",
                headers=headers,
                rows=[],
                error="No symbols provided",
                version=AI_ANALYSIS_VERSION,
                request_id=request_id,
                meta={"duration_ms": (time.time() - start) * 1000},
            )
            return BestJSONResponse(content=resp.model_dump(exclude_none=True) if _PYDANTIC_V2 else resp.dict(exclude_none=True))  # type: ignore

        engine = await _analysis_engine.get_engine(request)
        if not engine:
            err_resp = SheetAnalysisResponse(
                status="error",
                headers=[],
                rows=[],
                error="Data engine unavailable",
                version=AI_ANALYSIS_VERSION,
                request_id=request_id,
                meta={"duration_ms": (time.time() - start) * 1000},
            )
            return BestJSONResponse(status_code=503, content=err_resp.model_dump(exclude_none=True) if _PYDANTIC_V2 else err_resp.dict(exclude_none=True))  # type: ignore

        async with TraceContext("analysis.sheet_rows.fetch", {"count": len(symbols), "request_id": request_id, "mode": mode or ""}):
            data_map = await _analysis_engine.fetch_enriched(request, engine, symbols, auth_token=auth_token)

        rows: List[List[Any]] = []
        errors = 0
        for s in symbols:
            d = data_map.get(s) or {"symbol": s, "error": "Not found", "data_quality": "MISSING", "last_updated_riyadh": _saudi_time.now_iso()}
            if d.get("error"):
                errors += 1
            rows.append(_row_from_enriched(d))

        status_out = "success" if errors == 0 else ("partial" if errors < len(symbols) else "error")

        if _PROMETHEUS_AVAILABLE:
            c = _metrics.counter("requests_total", "Requests", ["status"])
            if c:
                c.labels(status=status_out).inc()

        resp = SheetAnalysisResponse(
            status=status_out,
            headers=headers,
            rows=rows,
            error=f"{errors} errors" if errors else None,
            version=AI_ANALYSIS_VERSION,
            request_id=request_id,
            meta={
                "duration_ms": (time.time() - start) * 1000,
                "requested": len(symbols),
                "errors": errors,
                "cache_stats": _cache.stats(),
                "riyadh_time": _saudi_time.now_iso(),
                "business_day": _saudi_time.is_trading_day(),
                "mode": mode,
                "sheet_name": req.sheet_name,
            },
        )
        return BestJSONResponse(content=resp.model_dump(exclude_none=True) if _PYDANTIC_V2 else resp.dict(exclude_none=True))  # type: ignore

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"sheet_rows failed: {e}\n{traceback.format_exc()}")
        err_resp = SheetAnalysisResponse(
            status="error",
            headers=[],
            rows=[],
            error=f"Internal Server Error: {e}",
            version=AI_ANALYSIS_VERSION,
            request_id=request_id,
            meta={"duration_ms": (time.time() - start) * 1000},
        )
        return BestJSONResponse(status_code=500, content=err_resp.model_dump(exclude_none=True) if _PYDANTIC_V2 else err_resp.dict(exclude_none=True))  # type: ignore

__all__ = ["router"]
