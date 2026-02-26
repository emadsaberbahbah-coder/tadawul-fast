#!/usr/bin/env python3
"""
routes/ai_analysis.py
------------------------------------------------------------
TADAWUL ENTERPRISE AI ANALYSIS ENGINE — v8.5.2 (STABLE ENTERPRISE)
SAMA Compliant | Resilient Routes | Safe Tracing | Safe Pydantic | No Raw 500s

v8.5.2 Fixes
- ✅ TraceContext fixed: start_as_current_span() is a context manager; now entered correctly.
- ✅ Removed invalid set_attributes() call; uses set_attribute() safely.
- ✅ Removed invalid attach/detach(span) usage (OpenTelemetry context handled by the CM).
- ✅ Added missing _CONFIG.adaptive_concurrency flag.
- ✅ Pydantic extra fields will not crash: models use extra="ignore".
- ✅ /sheet-rows never hard-fails; always returns SheetAnalysisResponse with status + meta.
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

AI_ANALYSIS_VERSION = "8.5.2"
router = APIRouter(prefix="/v1/analysis", tags=["AI & Analysis"])

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
    from prometheus_client import Counter, Histogram, Gauge, Summary, generate_latest, CONTENT_TYPE_LATEST
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

    def is_trading_hours(self, dt: Optional[datetime] = None) -> bool:
        if not self.is_trading_day(dt):
            return False
        dt = dt or self.now()
        current = dt.time()
        start = datetime.strptime(self._trading_hours["start"], "%H:%M").time()
        end = datetime.strptime(self._trading_hours["end"], "%H:%M").time()
        return start <= current <= end

    def to_riyadh(self, utc_dt: Union[str, datetime, None]) -> str:
        if not utc_dt:
            return ""
        try:
            dt = datetime.fromisoformat(str(utc_dt).replace("Z", "+00:00")) if isinstance(utc_dt, str) else utc_dt
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return dt.astimezone(self._tz).isoformat(timespec="milliseconds")
        except Exception:
            return ""

_saudi_time = SaudiTime()

# =============================================================================
# Config
# =============================================================================
@dataclass(slots=True)
class AIConfig:
    batch_size: int = 25
    batch_timeout_sec: float = 60.0
    batch_concurrency: int = 6
    max_tickers: int = 1200
    route_timeout_sec: float = 120.0

    enable_ml_predictions: bool = True
    trace_sample_rate: float = 1.0
    enable_tracing: bool = True

    cache_ttl_seconds: int = 300
    cache_max_size: int = 10000
    enable_redis_cache: bool = True
    redis_url: str = "redis://localhost:6379"

    # ✅ was missing in v8.5.1 (caused AttributeError)
    adaptive_concurrency: bool = True

    allow_query_token: bool = False
    require_api_key: bool = True

    enable_scoreboard: bool = True
    enable_websocket: bool = False  # keep off by default (safe)

_CONFIG = AIConfig()

def _load_config_from_env() -> None:
    def env_int(name: str, default: int) -> int:
        try:
            return int(os.getenv(name, str(default)).strip())
        except Exception:
            return default

    def env_float(name: str, default: float) -> float:
        try:
            return float(os.getenv(name, str(default)).strip())
        except Exception:
            return default

    def env_bool(name: str, default: bool) -> bool:
        v = os.getenv(name, str(default)).strip().lower()
        return v in ("true", "1", "yes", "on", "y")

    _CONFIG.batch_size = env_int("AI_BATCH_SIZE", _CONFIG.batch_size)
    _CONFIG.batch_timeout_sec = env_float("AI_BATCH_TIMEOUT_SEC", _CONFIG.batch_timeout_sec)
    _CONFIG.batch_concurrency = env_int("AI_BATCH_CONCURRENCY", _CONFIG.batch_concurrency)
    _CONFIG.max_tickers = env_int("AI_MAX_TICKERS", _CONFIG.max_tickers)
    _CONFIG.route_timeout_sec = env_float("AI_ROUTE_TIMEOUT_SEC", _CONFIG.route_timeout_sec)

    _CONFIG.enable_ml_predictions = env_bool("AI_ENABLE_ML", _CONFIG.enable_ml_predictions)
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
        self._histograms: Dict[str, Any] = {}
        self._gauges: Dict[str, Any] = {}
        self._summaries: Dict[str, Any] = {}

    def counter(self, name: str, description: str, labels: Optional[List[str]] = None) -> Any:
        if name not in self._counters and _PROMETHEUS_AVAILABLE:
            self._counters[name] = Counter(f"{self.namespace}_{name}", description, labelnames=labels or [])
        return self._counters.get(name)

    def histogram(self, name: str, description: str, buckets: Optional[List[float]] = None) -> Any:
        if name not in self._histograms and _PROMETHEUS_AVAILABLE:
            self._histograms[name] = Histogram(
                f"{self.namespace}_{name}",
                description,
                buckets=buckets or Histogram.DEFAULT_BUCKETS
            )
        return self._histograms.get(name)

    def gauge(self, name: str, description: str) -> Any:
        if name not in self._gauges and _PROMETHEUS_AVAILABLE:
            self._gauges[name] = Gauge(f"{self.namespace}_{name}", description)
        return self._gauges.get(name)

_metrics = MetricsRegistry()

# =============================================================================
# ✅ TraceContext (FIXED)
# =============================================================================
class TraceContext:
    """
    Safe OpenTelemetry wrapper.
    - start_as_current_span returns a context manager.
    - span supports set_attribute(), not set_attributes().
    - No attach/detach misuse.
    """
    def __init__(self, name: str, attributes: Optional[Dict[str, Any]] = None):
        self.name = name
        self.attributes = attributes or {}
        self.enabled = bool(_OTEL_AVAILABLE and _CONFIG.enable_tracing and _TRACER is not None)
        self._cm = None
        self.span = None

    async def __aenter__(self):
        if not self.enabled:
            return self
        # sampling
        if random.random() > float(_CONFIG.trace_sample_rate or 1.0):
            return self
        try:
            self._cm = _TRACER.start_as_current_span(self.name)
            self.span = self._cm.__enter__()  # actual span
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
                try:
                    self.span.record_exception(exc_val)
                except Exception:
                    pass
                try:
                    self.span.set_status(Status(StatusCode.ERROR, str(exc_val)))
                except Exception:
                    pass
        finally:
            try:
                self._cm.__exit__(exc_type, exc_val, exc_tb)
            except Exception:
                pass
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
            # no configured token => allow (dev mode)
            return True
        token = (token or "").strip()
        return token in self._tokens

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
# Cache Manager (safe, no create_task at import-time)
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
                # evict oldest 20%
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
# Pydantic Models (extra="ignore" to prevent crashes)
# =============================================================================
def _pydantic_config():
    if _PYDANTIC_V2:
        return ConfigDict(arbitrary_types_allowed=True, use_enum_values=True, extra="ignore")
    class _Cfg:  # pydantic v1
        arbitrary_types_allowed = True
        use_enum_values = True
        extra = "ignore"
    return _Cfg

class MLFeatures(BaseModel):
    symbol: str
    asset_class: AssetClass = AssetClass.EQUITY
    market_region: MarketRegion = MarketRegion.SAUDI
    price: Optional[float] = None
    open: Optional[float] = None
    high: Optional[float] = None
    low: Optional[float] = None
    close: Optional[float] = None
    volume: Optional[int] = None
    rsi_14d: Optional[float] = None
    macd: Optional[float] = None
    macd_signal: Optional[float] = None
    bb_position: Optional[float] = None
    volatility_30d: Optional[float] = None
    momentum_20d: Optional[float] = None
    return_20d: Optional[float] = None
    beta: Optional[float] = None
    market_cap: Optional[float] = None
    pe_ratio: Optional[float] = None
    dividend_yield: Optional[float] = None
    data_quality: DataQuality = DataQuality.GOOD
    timestamp: str = Field(default_factory=_saudi_time.now_iso)

    if _PYDANTIC_V2:
        model_config = _pydantic_config()
    else:
        class Config(_pydantic_config()):  # type: ignore
            pass

class MLPrediction(BaseModel):
    symbol: str
    prediction_time: str = Field(default_factory=_saudi_time.now_iso)
    predicted_return_1m: Optional[float] = None
    predicted_return_3m: Optional[float] = None
    predicted_return_1y: Optional[float] = None
    confidence_1m: float = 0.5
    confidence_3m: float = 0.5
    confidence_1y: float = 0.5
    risk_score: float = 50.0
    target_price_1m: Optional[float] = None
    target_price_3m: Optional[float] = None
    target_price_1y: Optional[float] = None
    signal_1m: SignalType = SignalType.NEUTRAL
    model_version: str = AI_ANALYSIS_VERSION
    recommendation: Recommendation = Recommendation.HOLD
    recommendation_strength: float = 0.0

    if _PYDANTIC_V2:
        model_config = _pydantic_config()
    else:
        class Config(_pydantic_config()):  # type: ignore
            pass

class SingleAnalysisResponse(BaseModel):
    symbol: str
    name: Optional[str] = None
    asset_class: AssetClass = AssetClass.EQUITY
    market_region: MarketRegion = MarketRegion.SAUDI

    price: Optional[float] = None
    open: Optional[float] = None
    high: Optional[float] = None
    low: Optional[float] = None
    close: Optional[float] = None
    volume: Optional[int] = None
    change: Optional[float] = None
    change_pct: Optional[float] = None

    market_cap: Optional[float] = None
    pe_ratio: Optional[float] = None
    pb_ratio: Optional[float] = None
    ps_ratio: Optional[float] = None
    dividend_yield: Optional[float] = None
    eps: Optional[float] = None
    revenue_growth: Optional[float] = None
    profit_margin: Optional[float] = None

    rsi_14d: Optional[float] = None
    macd: Optional[float] = None
    macd_signal: Optional[float] = None
    macd_histogram: Optional[float] = None
    bb_upper: Optional[float] = None
    bb_lower: Optional[float] = None
    bb_middle: Optional[float] = None
    bb_width: Optional[float] = None
    bb_position: Optional[float] = None

    volatility_30d: Optional[float] = None
    atr_14d: Optional[float] = None
    beta: Optional[float] = None

    momentum_1m: Optional[float] = None
    momentum_3m: Optional[float] = None
    momentum_12m: Optional[float] = None

    sharpe_ratio: Optional[float] = None
    sortino_ratio: Optional[float] = None
    max_drawdown_30d: Optional[float] = None
    var_95: Optional[float] = None

    ml_prediction: Optional[MLPrediction] = None

    value_score: Optional[float] = None
    quality_score: Optional[float] = None
    momentum_score: Optional[float] = None
    risk_score: Optional[float] = None
    overall_score: Optional[float] = None

    recommendation: Recommendation = Recommendation.HOLD
    recommendation_strength: float = 0.0
    confidence_level: ConfidenceLevel = ConfidenceLevel.MEDIUM

    expected_roi_1m: Optional[float] = None
    expected_roi_3m: Optional[float] = None
    expected_roi_12m: Optional[float] = None
    forecast_price_1m: Optional[float] = None
    forecast_price_3m: Optional[float] = None
    forecast_price_12m: Optional[float] = None
    forecast_confidence: Optional[float] = None

    shariah_compliant: ShariahCompliance = ShariahCompliance.PENDING
    forecast_updated_utc: Optional[str] = None
    forecast_updated_riyadh: Optional[str] = None
    last_updated_utc: Optional[str] = None
    last_updated_riyadh: Optional[str] = None

    data_quality: DataQuality = DataQuality.MISSING
    error: Optional[str] = None

    if _PYDANTIC_V2:
        model_config = _pydantic_config()
    else:
        class Config(_pydantic_config()):  # type: ignore
            pass

class BatchAnalysisResponse(BaseModel):
    results: List[SingleAnalysisResponse] = Field(default_factory=list)
    status: str = "success"
    error: Optional[str] = None
    warnings: List[str] = Field(default_factory=list)
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
    features: Optional[Dict[str, MLFeatures]] = None
    predictions: Optional[Dict[str, MLPrediction]] = None
    error: Optional[str] = None
    version: str = AI_ANALYSIS_VERSION
    request_id: str
    meta: Dict[str, Any] = Field(default_factory=dict)

    if _PYDANTIC_V2:
        model_config = _pydantic_config()
    else:
        class Config(_pydantic_config()):  # type: ignore
            pass

class ScoreboardResponse(BaseModel):
    headers: List[str] = Field(default_factory=list)
    rows: List[List[Any]] = Field(default_factory=list)
    status: str = "success"
    error: Optional[str] = None
    version: str = AI_ANALYSIS_VERSION
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
    mode: Optional[str] = None
    headers: Optional[List[str]] = None
    include_features: bool = False
    include_predictions: bool = False
    cache_ttl: Optional[int] = None
    priority: Optional[int] = Field(default=0, ge=0, le=10)

    if _PYDANTIC_V2:
        model_config = ConfigDict(extra="ignore")

        @model_validator(mode="after")
        def combine_tickers(self) -> "AdvancedSheetRequest":
            all_symbols = list(self.tickers or []) + list(self.symbols or [])
            seen: Set[str] = set()
            result: List[str] = []
            for s in all_symbols:
                normalized = str(s).strip().upper()
                if normalized.startswith("TADAWUL:"):
                    normalized = normalized.split(":", 1)[1]
                if normalized.endswith(".TADAWUL"):
                    normalized = normalized.replace(".TADAWUL", "")
                if normalized and normalized not in seen:
                    seen.add(normalized)
                    result.append(normalized)
            self.symbols = result
            self.tickers = []
            return self

# =============================================================================
# Core Engine Access
# =============================================================================
class AnalysisEngine:
    def __init__(self):
        self._engine_lock = asyncio.Lock()

    async def get_engine(self, request: Request) -> Optional[Any]:
        # prefer app.state.engine (created by main)
        try:
            st = getattr(request.app, "state", None)
            if st and getattr(st, "engine", None):
                return st.engine
        except Exception:
            pass

        # fallback: try core.data_engine_v2.get_engine
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

    async def fetch_enriched(self, engine: Any, symbols: List[str]) -> Dict[str, Dict[str, Any]]:
        """
        Best-effort fetch of enriched quotes using whatever engine method exists.
        Always returns a dict[symbol] -> dict(payload).
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
                        return out
                except Exception:
                    pass

        # Fallback: per symbol
        per_methods = ("get_enriched_quote", "enriched_quote", "get_quote", "quote")
        sem = asyncio.Semaphore(max(1, int(_CONFIG.batch_concurrency)))

        async def one(s: str):
            async with sem:
                for m in per_methods:
                    fn = getattr(engine, m, None)
                    if callable(fn):
                        try:
                            r = fn(s)
                            if hasattr(r, "__await__"):
                                r = await r
                            return s, _to_dict(r)
                        except Exception as e:
                            last = str(e)
                            continue
                return s, {"symbol": s, "error": "No engine method for enriched quote", "data_quality": "MISSING"}

        results = await asyncio.gather(*(one(s) for s in symbols), return_exceptions=True)
        for r in results:
            if isinstance(r, tuple) and len(r) == 2:
                out[r[0]] = r[1]
        return out

_analysis_engine = AnalysisEngine()

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

# =============================================================================
# Helpers: parsing, mapping, headers/rows
# =============================================================================
def _parse_tickers(tickers_str: str) -> List[str]:
    if not tickers_str:
        return []
    symbols = [p.strip().upper() for p in tickers_str.replace(",", " ").replace(";", " ").replace("|", " ").split() if p.strip()]
    seen: Set[str] = set()
    out: List[str] = []
    for s in symbols:
        if s not in seen:
            seen.add(s)
            out.append(s)
    return out

def _default_headers() -> List[str]:
    return [
        "Symbol", "Name", "Price", "Change", "Change %", "Volume", "Market Cap",
        "P/E", "P/B", "Dividend Yield", "RSI (14)", "MACD", "BB Position",
        "Volatility 30D", "Overall Score", "Risk Score", "Recommendation",
        "Expected ROI 1M", "Forecast Price 1M",
        "Expected ROI 3M", "Forecast Price 3M",
        "Expected ROI 12M", "Forecast Price 12M",
        "Confidence", "Signal", "Data Quality", "Error", "Last Updated Riyadh"
    ]

def _quote_to_response(data: Dict[str, Any]) -> SingleAnalysisResponse:
    sym = (data.get("symbol") or data.get("ticker") or "UNKNOWN").strip()

    price = data.get("current_price") or data.get("price")
    change = data.get("price_change") or data.get("change")
    change_pct = data.get("percent_change") or data.get("change_pct")

    ml_pred = data.get("ml_prediction")
    ml_prediction: Optional[MLPrediction] = None
    if isinstance(ml_pred, dict):
        try:
            ml_prediction = MLPrediction(**ml_pred)
        except Exception:
            ml_prediction = None

    # map confidence level from ML prediction if present
    conf = ConfidenceLevel.MEDIUM
    if ml_prediction:
        c = float(getattr(ml_prediction, "confidence_1m", 0.5) or 0.5)
        if c >= 0.8:
            conf = ConfidenceLevel.VERY_HIGH
        elif c >= 0.6:
            conf = ConfidenceLevel.HIGH
        elif c >= 0.4:
            conf = ConfidenceLevel.MEDIUM
        elif c >= 0.2:
            conf = ConfidenceLevel.LOW
        else:
            conf = ConfidenceLevel.VERY_LOW

    # recommendation fallback
    rec = data.get("recommendation")
    if not rec and ml_prediction:
        rec = ml_prediction.recommendation
    rec = rec or Recommendation.HOLD

    dq = data.get("data_quality") or ("MISSING" if data.get("error") else "GOOD")
    try:
        dq_enum = DataQuality(dq)
    except Exception:
        dq_enum = DataQuality.MISSING

    region = MarketRegion.SAUDI if sym.endswith(".SR") or "SR" in sym else MarketRegion.GLOBAL

    return SingleAnalysisResponse(
        symbol=sym,
        name=data.get("name"),
        asset_class=AssetClass.EQUITY,
        market_region=region,
        price=price,
        open=data.get("open") or data.get("day_open"),
        high=data.get("day_high") or data.get("high"),
        low=data.get("day_low") or data.get("low"),
        close=data.get("previous_close") or data.get("close"),
        volume=data.get("volume"),
        change=change,
        change_pct=change_pct,
        market_cap=data.get("market_cap"),
        pe_ratio=data.get("pe_ttm") or data.get("pe_ratio"),
        pb_ratio=data.get("pb") or data.get("pb_ratio"),
        ps_ratio=data.get("ps") or data.get("ps_ratio"),
        dividend_yield=data.get("dividend_yield"),
        eps=data.get("eps_ttm") or data.get("eps"),
        revenue_growth=data.get("revenue_growth_yoy") or data.get("revenue_growth"),
        profit_margin=data.get("net_margin") or data.get("profit_margin"),
        rsi_14d=data.get("rsi_14") or data.get("rsi_14d"),
        macd=data.get("macd") or data.get("macd_line"),
        macd_signal=data.get("macd_signal"),
        macd_histogram=data.get("macd_histogram"),
        bb_upper=data.get("bb_upper"),
        bb_lower=data.get("bb_lower"),
        bb_middle=data.get("bb_middle"),
        bb_position=data.get("bb_position") or data.get("week_52_position_pct"),
        volatility_30d=data.get("volatility_30d"),
        beta=data.get("beta"),
        momentum_1m=data.get("returns_1m") or data.get("momentum_1m"),
        momentum_3m=data.get("returns_3m") or data.get("momentum_3m"),
        momentum_12m=data.get("returns_12m") or data.get("momentum_12m"),
        overall_score=data.get("overall_score") or data.get("score"),
        risk_score=data.get("risk_score"),
        recommendation=rec,
        confidence_level=conf,
        expected_roi_1m=data.get("expected_roi_1m"),
        expected_roi_3m=data.get("expected_roi_3m"),
        expected_roi_12m=data.get("expected_roi_12m"),
        forecast_price_1m=data.get("forecast_price_1m"),
        forecast_price_3m=data.get("forecast_price_3m"),
        forecast_price_12m=data.get("forecast_price_12m"),
        ml_prediction=ml_prediction,
        data_quality=dq_enum,
        error=data.get("error"),
        last_updated_utc=data.get("last_updated_utc") or _saudi_time.now_utc_iso(),
        last_updated_riyadh=data.get("last_updated_riyadh") or _saudi_time.now_iso(),
    )

def _headers_to_row(headers: List[str], data: Dict[str, Any], ml_pred: Optional[Dict[str, Any]] = None) -> List[Any]:
    lookup = {str(k).lower(): v for k, v in (data or {}).items()}
    ml_lookup = {str(k).lower(): v for k, v in (ml_pred or {}).items()}
    row: List[Any] = []

    for h in headers:
        hl = h.lower()
        if "symbol" == hl or "symbol" in hl:
            row.append(lookup.get("symbol") or lookup.get("ticker"))
        elif "name" in hl:
            row.append(lookup.get("name") or lookup.get("company_name"))
        elif hl == "price" or ("price" in hl and "forecast" not in hl):
            row.append(lookup.get("price") or lookup.get("current_price"))
        elif hl == "change":
            row.append(lookup.get("change") or lookup.get("price_change"))
        elif "change %" in hl or "change%" in hl:
            row.append(lookup.get("change_pct") or lookup.get("percent_change"))
        elif "volume" in hl:
            row.append(lookup.get("volume"))
        elif "market cap" in hl:
            row.append(lookup.get("market_cap"))
        elif hl in ("p/e", "pe", "p/e ratio") or "p/e" in hl or "pe" == hl:
            row.append(lookup.get("pe_ratio") or lookup.get("pe_ttm"))
        elif "p/b" in hl or "pb" == hl:
            row.append(lookup.get("pb_ratio") or lookup.get("pb"))
        elif "dividend" in hl:
            row.append(lookup.get("dividend_yield"))
        elif "rsi" in hl:
            row.append(lookup.get("rsi_14d") or lookup.get("rsi_14"))
        elif "macd" in hl and "signal" not in hl:
            row.append(lookup.get("macd") or lookup.get("macd_line"))
        elif "bb position" in hl or "bb" in hl:
            row.append(lookup.get("bb_position") or lookup.get("week_52_position_pct"))
        elif "volatility" in hl:
            row.append(lookup.get("volatility_30d"))
        elif "overall score" in hl:
            row.append(lookup.get("overall_score") or lookup.get("score"))
        elif "risk score" in hl:
            row.append(lookup.get("risk_score") or ml_lookup.get("risk_score"))
        elif "recommendation" in hl:
            row.append(lookup.get("recommendation") or ml_lookup.get("recommendation") or "HOLD")
        elif "expected roi" in hl and "1m" in hl:
            row.append(lookup.get("expected_roi_1m"))
        elif "forecast price" in hl and "1m" in hl:
            row.append(lookup.get("forecast_price_1m") or ml_lookup.get("target_price_1m"))
        elif "expected roi" in hl and "3m" in hl:
            row.append(lookup.get("expected_roi_3m"))
        elif "forecast price" in hl and "3m" in hl:
            row.append(lookup.get("forecast_price_3m") or ml_lookup.get("target_price_3m"))
        elif "expected roi" in hl and "12m" in hl:
            row.append(lookup.get("expected_roi_12m"))
        elif "forecast price" in hl and "12m" in hl:
            row.append(lookup.get("forecast_price_12m") or ml_lookup.get("target_price_1y"))
        elif "confidence" in hl:
            row.append(ml_lookup.get("confidence_1m"))
        elif "signal" in hl:
            row.append(ml_lookup.get("signal_1m"))
        elif "data quality" in hl:
            row.append(lookup.get("data_quality"))
        elif hl == "error":
            row.append(lookup.get("error"))
        elif "last updated" in hl:
            row.append(lookup.get("last_updated_riyadh") or _saudi_time.now_iso())
        else:
            row.append(lookup.get(hl))

    return row

# =============================================================================
# Routes
# =============================================================================
@router.get("/health")
async def health(request: Request) -> Dict[str, Any]:
    engine = await _analysis_engine.get_engine(request)
    return {
        "status": "ok" if engine else "degraded",
        "version": AI_ANALYSIS_VERSION,
        "timestamp_utc": _saudi_time.now_utc_iso(),
        "timestamp_riyadh": _saudi_time.now_iso(),
        "trading_day": _saudi_time.is_trading_day(),
        "trading_hours": _saudi_time.is_trading_hours(),
        "engine": {"available": engine is not None, "type": type(engine).__name__ if engine else "none"},
        "cache": _cache.stats(),
        "config": {
            "batch_size": _CONFIG.batch_size,
            "batch_concurrency": _CONFIG.batch_concurrency,
            "max_tickers": _CONFIG.max_tickers,
            "enable_tracing": _CONFIG.enable_tracing,
            "trace_sample_rate": _CONFIG.trace_sample_rate,
        },
        "request_id": getattr(request.state, "request_id", None),
    }

@router.get("/metrics")
async def metrics() -> Response:
    if not _PROMETHEUS_AVAILABLE:
        return BestJSONResponse(status_code=503, content={"status": "error", "error": "Metrics not available"})
    return Response(content=generate_latest(), media_type=CONTENT_TYPE_LATEST)

@router.get("/quote", response_model=SingleAnalysisResponse)
async def get_quote(
    request: Request,
    symbol: str = Query(..., description="Ticker/Symbol"),
    include_ml: bool = Query(True, description="Include ML predictions"),
    token: Optional[str] = Query(default=None),
    x_app_token: Optional[str] = Header(None, alias="X-APP-TOKEN"),
    authorization: Optional[str] = Header(None, alias="Authorization"),
) -> BestJSONResponse:
    start = time.time()
    rid = getattr(request.state, "request_id", None) or str(uuid.uuid4())[:18]
    sym = (symbol or "").strip().upper()

    try:
        client_ip = request.client.host if request.client else "unknown"
        if not await _rate_limiter.check(client_ip):
            raise HTTPException(status_code=status.HTTP_429_TOO_MANY_REQUESTS, detail="Rate limit exceeded")

        auth_token = _extract_auth_token(token if _CONFIG.allow_query_token else None, x_app_token, authorization)
        if not _token_manager.validate_token(auth_token):
            raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid token")

        if not sym:
            resp = SingleAnalysisResponse(symbol="UNKNOWN", error="No symbol provided", data_quality=DataQuality.MISSING)
            return BestJSONResponse(content=resp.model_dump(exclude_none=True) if _PYDANTIC_V2 else resp.dict(exclude_none=True))  # type: ignore

        engine = await _analysis_engine.get_engine(request)
        if not engine:
            resp = SingleAnalysisResponse(symbol=sym, error="Engine unavailable", data_quality=DataQuality.MISSING)
            return BestJSONResponse(content=resp.model_dump(exclude_none=True) if _PYDANTIC_V2 else resp.dict(exclude_none=True))  # type: ignore

        async with TraceContext("analysis.get_quote", {"symbol": sym, "request_id": rid}):
            # cached?
            ck = f"analysis:quote:{sym}"
            cached = await _cache.get(ck)
            if isinstance(cached, dict) and cached.get("symbol"):
                data = cached
            else:
                data_map = await _analysis_engine.fetch_enriched(engine, [sym])
                data = data_map.get(sym) or {"symbol": sym, "error": "No data", "data_quality": "MISSING"}
                data["last_updated_utc"] = _saudi_time.now_utc_iso()
                data["last_updated_riyadh"] = _saudi_time.now_iso()
                await _cache.set(ck, data)

            response = _quote_to_response(data)
            return BestJSONResponse(content=response.model_dump(exclude_none=True) if _PYDANTIC_V2 else response.dict(exclude_none=True))  # type: ignore

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"get_quote failed: {e}\n{traceback.format_exc()}")
        err = SingleAnalysisResponse(symbol=sym or "UNKNOWN", error=f"Internal Server Error: {e}", data_quality=DataQuality.ERROR)
        return BestJSONResponse(status_code=500, content=err.model_dump(exclude_none=True) if _PYDANTIC_V2 else err.dict(exclude_none=True))  # type: ignore

@router.post("/quote", response_model=SingleAnalysisResponse)
async def post_quote(
    request: Request,
    body: Dict[str, Any] = Body(default_factory=dict),
    include_ml: bool = Query(True, description="Include ML predictions"),
    token: Optional[str] = Query(default=None),
    x_app_token: Optional[str] = Header(None, alias="X-APP-TOKEN"),
    authorization: Optional[str] = Header(None, alias="Authorization"),
) -> BestJSONResponse:
    symbol = (body.get("symbol") or body.get("ticker") or body.get("Symbol") or "").strip()
    return await get_quote(
        request=request,
        symbol=symbol,
        include_ml=include_ml,
        token=token,
        x_app_token=x_app_token,
        authorization=authorization,
    )

@router.get("/quotes", response_model=BatchAnalysisResponse)
async def get_quotes(
    request: Request,
    tickers: str = Query(default="", description="Tickers: 'AAPL,MSFT 1120.SR'"),
    top_n: int = Query(default=0, ge=0, le=5000),
    include_ml: bool = Query(True, description="Include ML predictions"),
    token: Optional[str] = Query(default=None),
    x_app_token: Optional[str] = Header(None, alias="X-APP-TOKEN"),
    authorization: Optional[str] = Header(None, alias="Authorization"),
) -> BestJSONResponse:
    start = time.time()
    rid = getattr(request.state, "request_id", None) or str(uuid.uuid4())[:18]

    try:
        client_ip = request.client.host if request.client else "unknown"
        if not await _rate_limiter.check(client_ip):
            raise HTTPException(status_code=status.HTTP_429_TOO_MANY_REQUESTS, detail="Rate limit exceeded")

        auth_token = _extract_auth_token(token if _CONFIG.allow_query_token else None, x_app_token, authorization)
        if not _token_manager.validate_token(auth_token):
            raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid token")

        symbols = _parse_tickers(tickers)
        if not symbols:
            resp = BatchAnalysisResponse(status="skipped", results=[], version=AI_ANALYSIS_VERSION, meta={"reason": "No tickers provided"})
            return BestJSONResponse(content=resp.model_dump(exclude_none=True) if _PYDANTIC_V2 else resp.dict(exclude_none=True))  # type: ignore

        if top_n and top_n > 0:
            symbols = symbols[:min(top_n, _CONFIG.max_tickers)]
        else:
            symbols = symbols[:_CONFIG.max_tickers]

        engine = await _analysis_engine.get_engine(request)
        if not engine:
            resp = BatchAnalysisResponse(status="error", error="Engine unavailable", results=[], version=AI_ANALYSIS_VERSION)
            return BestJSONResponse(content=resp.model_dump(exclude_none=True) if _PYDANTIC_V2 else resp.dict(exclude_none=True))  # type: ignore

        async with TraceContext("analysis.get_quotes", {"count": len(symbols), "request_id": rid}):
            data_map = await _analysis_engine.fetch_enriched(engine, symbols)

        results: List[SingleAnalysisResponse] = []
        err_count = 0
        for s in symbols:
            d = data_map.get(s) or {"symbol": s, "error": "No data", "data_quality": "MISSING"}
            d["last_updated_utc"] = d.get("last_updated_utc") or _saudi_time.now_utc_iso()
            d["last_updated_riyadh"] = d.get("last_updated_riyadh") or _saudi_time.now_iso()
            if d.get("error"):
                err_count += 1
            results.append(_quote_to_response(d))

        status_out = "success" if err_count == 0 else ("partial" if err_count < len(symbols) else "error")

        resp = BatchAnalysisResponse(
            status=status_out,
            results=results,
            error=f"{err_count} errors" if err_count else None,
            version=AI_ANALYSIS_VERSION,
            meta={
                "requested": len(symbols),
                "errors": err_count,
                "duration_ms": (time.time() - start) * 1000,
                "timestamp_riyadh": _saudi_time.now_iso(),
            },
        )
        return BestJSONResponse(content=resp.model_dump(exclude_none=True) if _PYDANTIC_V2 else resp.dict(exclude_none=True))  # type: ignore

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"get_quotes failed: {e}\n{traceback.format_exc()}")
        resp = BatchAnalysisResponse(status="error", error=f"Internal Server Error: {e}", results=[], version=AI_ANALYSIS_VERSION)
        return BestJSONResponse(status_code=500, content=resp.model_dump(exclude_none=True) if _PYDANTIC_V2 else resp.dict(exclude_none=True))  # type: ignore

@router.post("/quotes", response_model=BatchAnalysisResponse)
async def batch_quotes(
    request: Request,
    body: Dict[str, Any] = Body(default_factory=dict),
    include_ml: bool = Query(True, description="Include ML predictions"),
    token: Optional[str] = Query(default=None),
    x_app_token: Optional[str] = Header(None, alias="X-APP-TOKEN"),
    authorization: Optional[str] = Header(None, alias="Authorization"),
) -> BestJSONResponse:
    tickers = body.get("tickers") or body.get("symbols") or ""
    if isinstance(tickers, list):
        tickers = ",".join(str(t) for t in tickers)
    return await get_quotes(
        request=request,
        tickers=str(tickers),
        include_ml=include_ml,
        token=token,
        x_app_token=x_app_token,
        authorization=authorization,
    )

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

        # Parse request safely
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
        if not symbols:
            resp = SheetAnalysisResponse(
                status="skipped",
                headers=req.headers or _default_headers(),
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
            data_map = await _analysis_engine.fetch_enriched(engine, symbols)

        headers = req.headers or _default_headers()
        rows: List[List[Any]] = []
        features_dict: Optional[Dict[str, MLFeatures]] = {} if req.include_features else None
        predictions_dict: Optional[Dict[str, MLPrediction]] = {} if req.include_predictions else None

        errors = 0
        for s in symbols:
            d = data_map.get(s) or {"symbol": s, "error": "Not found", "data_quality": "MISSING"}
            if d.get("error"):
                errors += 1

            # Extract ml_prediction dict if exists
            ml_pred = d.get("ml_prediction")
            ml_pred_dict = ml_pred if isinstance(ml_pred, dict) else None

            rows.append(_headers_to_row(headers, d, ml_pred_dict))

            if req.include_features:
                try:
                    # Keep it minimal & safe; extra="ignore" tolerates extra keys
                    feat = MLFeatures(
                        symbol=s,
                        price=d.get("price") or d.get("current_price"),
                        volume=d.get("volume"),
                        volatility_30d=d.get("volatility_30d"),
                        rsi_14d=d.get("rsi_14d") or d.get("rsi_14"),
                        macd=d.get("macd"),
                        macd_signal=d.get("macd_signal"),
                        bb_position=d.get("bb_position"),
                        beta=d.get("beta"),
                        market_cap=d.get("market_cap"),
                        pe_ratio=d.get("pe_ratio") or d.get("pe_ttm"),
                        dividend_yield=d.get("dividend_yield"),
                        data_quality=DataQuality(d.get("data_quality") or "GOOD"),
                    )
                    features_dict[s] = feat  # type: ignore
                except Exception:
                    pass

            if req.include_predictions and predictions_dict is not None:
                # keep prediction non-blocking: if already present use it, otherwise skip (no heavy training here)
                if isinstance(ml_pred_dict, dict):
                    try:
                        predictions_dict[s] = MLPrediction(**ml_pred_dict)
                    except Exception:
                        pass

        status_out = "success" if errors == 0 else ("partial" if errors < len(symbols) else "error")

        # Prometheus updates (safe)
        if _PROMETHEUS_AVAILABLE:
            c = _metrics.counter("requests_total", "Requests", ["status"])
            if c:
                c.labels(status=status_out).inc()

        response = SheetAnalysisResponse(
            status=status_out,
            headers=headers,
            rows=rows,
            features=features_dict,
            predictions=predictions_dict,
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
        return BestJSONResponse(content=response.model_dump(exclude_none=True) if _PYDANTIC_V2 else response.dict(exclude_none=True))  # type: ignore

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

@router.get("/scoreboard", response_model=ScoreboardResponse)
async def scoreboard(
    request: Request,
    tickers: str = Query(default="", description="Tickers: 'AAPL,MSFT 1120.SR'"),
    top_n: int = Query(default=20, ge=1, le=200),
    token: Optional[str] = Query(default=None),
    x_app_token: Optional[str] = Header(None, alias="X-APP-TOKEN"),
    authorization: Optional[str] = Header(None, alias="Authorization"),
) -> BestJSONResponse:
    start = time.time()
    rid = getattr(request.state, "request_id", None) or str(uuid.uuid4())[:18]

    try:
        auth_token = _extract_auth_token(token if _CONFIG.allow_query_token else None, x_app_token, authorization)
        if not _token_manager.validate_token(auth_token):
            raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid token")

        if not _CONFIG.enable_scoreboard:
            resp = ScoreboardResponse(status="disabled", error="Scoreboard disabled", headers=[], rows=[], version=AI_ANALYSIS_VERSION)
            return BestJSONResponse(content=resp.model_dump(exclude_none=True) if _PYDANTIC_V2 else resp.dict(exclude_none=True))  # type: ignore

        symbols = _parse_tickers(tickers)[:top_n]
        if not symbols:
            resp = ScoreboardResponse(status="skipped", headers=[], rows=[], version=AI_ANALYSIS_VERSION, meta={"reason": "No tickers provided"})
            return BestJSONResponse(content=resp.model_dump(exclude_none=True) if _PYDANTIC_V2 else resp.dict(exclude_none=True))  # type: ignore

        engine = await _analysis_engine.get_engine(request)
        if not engine:
            resp = ScoreboardResponse(status="error", error="Engine unavailable", headers=[], rows=[], version=AI_ANALYSIS_VERSION)
            return BestJSONResponse(content=resp.model_dump(exclude_none=True) if _PYDANTIC_V2 else resp.dict(exclude_none=True))  # type: ignore

        async with TraceContext("analysis.scoreboard", {"count": len(symbols), "request_id": rid}):
            data_map = await _analysis_engine.fetch_enriched(engine, symbols)

        items = []
        for s in symbols:
            d = data_map.get(s) or {"symbol": s, "error": "No data", "data_quality": "MISSING"}
            ml_pred = d.get("ml_prediction") if isinstance(d.get("ml_prediction"), dict) else {}
            overall = d.get("overall_score") or d.get("score")
            risk = d.get("risk_score") or ml_pred.get("risk_score")
            if overall is None and risk is not None:
                try:
                    overall = 100.0 - float(risk)
                except Exception:
                    overall = 50.0
            items.append({
                "symbol": s,
                "name": d.get("name", ""),
                "price": d.get("price") or d.get("current_price"),
                "change_pct": d.get("change_pct") or d.get("percent_change"),
                "overall_score": overall if overall is not None else 50.0,
                "risk_score": risk if risk is not None else 50.0,
                "recommendation": d.get("recommendation") or ml_pred.get("recommendation") or "HOLD",
                "last_updated": d.get("last_updated_riyadh") or _saudi_time.now_iso(),
            })

        items.sort(key=lambda x: float(x["overall_score"] or 0.0), reverse=True)

        headers = ["Rank", "Symbol", "Name", "Price", "Change %", "Overall Score", "Risk Score", "Recommendation", "Last Updated"]
        rows = []
        for i, it in enumerate(items[:top_n], 1):
            rows.append([i, it["symbol"], it["name"], it["price"], it["change_pct"], it["overall_score"], it["risk_score"], it["recommendation"], it["last_updated"]])

        resp = ScoreboardResponse(
            status="success",
            headers=headers,
            rows=rows,
            version=AI_ANALYSIS_VERSION,
            meta={
                "total": len(items),
                "displayed": min(len(items), top_n),
                "duration_ms": (time.time() - start) * 1000,
                "timestamp_riyadh": _saudi_time.now_iso(),
            },
        )
        return BestJSONResponse(content=resp.model_dump(exclude_none=True) if _PYDANTIC_V2 else resp.dict(exclude_none=True))  # type: ignore

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"scoreboard failed: {e}\n{traceback.format_exc()}")
        resp = ScoreboardResponse(status="error", error=f"Internal Server Error: {e}", headers=[], rows=[], version=AI_ANALYSIS_VERSION)
        return BestJSONResponse(status_code=500, content=resp.model_dump(exclude_none=True) if _PYDANTIC_V2 else resp.dict(exclude_none=True))  # type: ignore

# Optional WebSocket (off by default)
@router.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket, token: Optional[str] = Query(None)):
    if not _CONFIG.enable_websocket:
        await websocket.close(code=1000, reason="WebSocket disabled")
        return
    if not token or not _token_manager.validate_token(token):
        await websocket.close(code=1008, reason="Invalid token")
        return

    await websocket.accept()
    try:
        await websocket.send_json({"type": "connection", "status": "connected", "timestamp": _saudi_time.now_iso(), "version": AI_ANALYSIS_VERSION})
        while True:
            data = await websocket.receive_json()
            if data.get("type") == "ping":
                await websocket.send_json({"type": "pong", "timestamp": _saudi_time.now_iso()})
    except WebSocketDisconnect:
        return
    except Exception:
        return

__all__ = ["router"]
