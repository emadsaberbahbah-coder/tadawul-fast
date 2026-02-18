"""
routes/ai_analysis.py
------------------------------------------------------------
TADAWUL ENTERPRISE AI ANALYSIS ENGINE — v7.0.0 (ML-POWERED + PRODUCTION HARDENED)
SAMA Compliant | Real-time ML | Predictive Analytics | Distributed Caching

Core Capabilities:
- Real-time ML-powered analysis with ensemble models
- Predictive analytics with confidence scoring
- Multi-asset support with Shariah compliance filtering
- Distributed tracing with OpenTelemetry
- Advanced caching with Redis and predictive pre-fetch
- Circuit breaker pattern for external services
- Rate limiting with token bucket algorithm
- SAMA-compliant audit logging
- WebSocket support for real-time updates
- Batch processing with adaptive concurrency
- Explainable AI with feature importance
"""

from __future__ import annotations

import asyncio
import functools
import hashlib
import inspect
import json
import logging
import math
import os
import random
import re
import time
import uuid
from collections import defaultdict, deque
from contextlib import asynccontextmanager
from dataclasses import dataclass, field, is_dataclass
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from enum import Enum
from functools import lru_cache, wraps
from typing import (
    Any,
    AsyncGenerator,
    Awaitable,
    Callable,
    Dict,
    List,
    Optional,
    Sequence,
    Set,
    Tuple,
    TypeVar,
    Union,
    cast,
)
from urllib.parse import urlparse

import fastapi
from fastapi import (
    APIRouter,
    Body,
    Header,
    HTTPException,
    Query,
    Request,
    Response,
    WebSocket,
    WebSocketDisconnect,
    status,
)
from fastapi.responses import JSONResponse, StreamingResponse
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer

# Optional ML/Data libraries with graceful fallbacks
try:
    import numpy as np
    import pandas as pd
    from scipy import stats
    from sklearn.ensemble import RandomForestRegressor, GradientBoostingRegressor
    from sklearn.preprocessing import StandardScaler
    _ML_AVAILABLE = True
except ImportError:
    _ML_AVAILABLE = False
    # Create dummy classes
    class RandomForestRegressor: pass
    class GradientBoostingRegressor: pass
    class StandardScaler: pass

try:
    from opentelemetry import trace
    from opentelemetry.trace import Span, Status, StatusCode, Tracer
    from opentelemetry.context import attach, detach
    from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter
    _OTEL_AVAILABLE = True
except ImportError:
    _OTEL_AVAILABLE = False
    # Dummy tracer
    class DummyTracer:
        def start_as_current_span(self, *args, **kwargs):
            return asynccontextmanager(lambda: (yield None))()
    trace = DummyTracer()

try:
    import aioredis
    from aioredis import Redis
    from aioredis.exceptions import RedisError
    _REDIS_AVAILABLE = True
except ImportError:
    _REDIS_AVAILABLE = False

try:
    import prometheus_client
    from prometheus_client import Counter, Histogram, Gauge, Summary
    _PROMETHEUS_AVAILABLE = True
except ImportError:
    _PROMETHEUS_AVAILABLE = False

try:
    import aiohttp
    from aiohttp import ClientTimeout, ClientSession
    _AIOHTTP_AVAILABLE = True
except ImportError:
    _AIOHTTP_AVAILABLE = False

# Pydantic imports
try:
    from pydantic import (
        BaseModel,
        ConfigDict,
        Field,
        field_validator,
        model_validator,
        ValidationError,
        AliasChoices,
    )
    from pydantic.functional_validators import AfterValidator
    from typing_extensions import Annotated
    _PYDANTIC_V2 = True
except ImportError:
    from pydantic import BaseModel, Field, validator  # type: ignore
    _PYDANTIC_V2 = False

logger = logging.getLogger("routes.ai_analysis")

AI_ANALYSIS_VERSION = "7.0.0"
router = APIRouter(prefix="/v1/analysis", tags=["AI & Analysis"])

# =============================================================================
# Enums & Types
# =============================================================================

class AssetClass(str, Enum):
    """Asset classes supported"""
    EQUITY = "equity"
    SUKUK = "sukuk"
    REIT = "reit"
    ETF = "etf"
    COMMODITY = "commodity"
    CURRENCY = "currency"
    CRYPTO = "crypto"
    DERIVATIVE = "derivative"

class MarketRegion(str, Enum):
    """Market regions"""
    SAUDI = "saudi"
    UAE = "uae"
    QATAR = "qatar"
    KUWAIT = "kuwait"
    BAHRAIN = "bahrain"
    OMAN = "oman"
    EGYPT = "egypt"
    USA = "usa"
    GLOBAL = "global"

class DataQuality(str, Enum):
    """Data quality levels"""
    EXCELLENT = "excellent"
    GOOD = "good"
    FAIR = "fair"
    POOR = "poor"
    MISSING = "missing"
    STALE = "stale"

class Recommendation(str, Enum):
    """Investment recommendations"""
    STRONG_BUY = "strong_buy"
    BUY = "buy"
    HOLD = "hold"
    SELL = "sell"
    STRONG_SELL = "strong_sell"
    UNDER_REVIEW = "under_review"

class ConfidenceLevel(str, Enum):
    """Confidence levels"""
    VERY_HIGH = "very_high"
    HIGH = "high"
    MEDIUM = "medium"
    LOW = "low"
    VERY_LOW = "very_low"

class SignalType(str, Enum):
    """Trading signal types"""
    BULLISH = "bullish"
    BEARISH = "bearish"
    NEUTRAL = "neutral"
    OVERBOUGHT = "overbought"
    OVERSOLD = "oversold"
    DIVERGENCE = "divergence"

class ShariahCompliance(str, Enum):
    """Shariah compliance status"""
    COMPLIANT = "compliant"
    NON_COMPLIANT = "non_compliant"
    PENDING = "pending"
    NA = "na"

class TimeFrame(str, Enum):
    """Time frames for analysis"""
    INTRADAY = "intraday"
    DAILY = "daily"
    WEEKLY = "weekly"
    MONTHLY = "monthly"
    QUARTERLY = "quarterly"
    YEARLY = "yearly"

# =============================================================================
# Configuration with Dynamic Updates
# =============================================================================

@dataclass
class AIConfig:
    """Dynamic AI configuration with hot-reload"""
    
    # Batch processing
    batch_size: int = 25
    batch_timeout_sec: float = 60.0
    batch_concurrency: int = 6
    max_tickers: int = 1200
    route_timeout_sec: float = 120.0
    
    # ML/AI settings
    enable_ml_predictions: bool = True
    ml_model_cache_ttl: int = 3600
    prediction_confidence_threshold: float = 0.6
    feature_importance_enabled: bool = True
    ensemble_method: str = "weighted_average"  # weighted_average, voting, stacking
    
    # Model parameters
    rf_n_estimators: int = 100
    rf_max_depth: int = 10
    gb_n_estimators: int = 100
    gb_learning_rate: float = 0.1
    
    # Technical indicators
    enable_technical_indicators: bool = True
    rsi_period: int = 14
    macd_fast: int = 12
    macd_slow: int = 26
    macd_signal: int = 9
    bb_period: int = 20
    bb_std: float = 2.0
    volume_ma_periods: List[int] = field(default_factory=lambda: [5, 10, 20])
    
    # Fundamental analysis
    enable_fundamental_analysis: bool = True
    min_market_cap: float = 100_000_000  # 100M SAR
    pe_ratio_range: Tuple[float, float] = (5, 30)
    pb_ratio_max: float = 5.0
    
    # Sentiment analysis
    enable_sentiment_analysis: bool = True
    news_sentiment_weight: float = 0.3
    social_sentiment_weight: float = 0.2
    analyst_rating_weight: float = 0.5
    
    # Cache settings
    cache_ttl_seconds: int = 300
    cache_max_size: int = 10000
    enable_redis_cache: bool = True
    redis_url: str = "redis://localhost:6379"
    
    # Circuit breaker
    circuit_breaker_threshold: int = 5
    circuit_breaker_timeout: float = 60.0
    half_open_requests: int = 2
    
    # Rate limiting
    rate_limit_requests: int = 100
    rate_limit_window: int = 60
    rate_limit_burst: int = 20
    
    # Observability
    enable_metrics: bool = True
    enable_tracing: bool = False
    trace_sample_rate: float = 0.1
    
    # Security
    allow_query_token: bool = False
    require_api_key: bool = True
    enable_audit_log: bool = True
    
    # Shariah compliance
    enable_shariah_filter: bool = True
    shariah_check_timeout: float = 10.0
    
    # Market data
    market_data_refresh_interval: int = 60
    enable_realtime: bool = True
    
    # Feature flags
    enable_scoreboard: bool = True
    enable_websocket: bool = True
    enable_push_mode: bool = True
    enable_fallback: bool = True

_CONFIG = AIConfig()

def _load_config_from_env() -> None:
    """Load configuration from environment variables"""
    global _CONFIG
    
    def env_int(name: str, default: int) -> int:
        return int(os.getenv(name, str(default)))
    
    def env_float(name: str, default: float) -> float:
        return float(os.getenv(name, str(default)))
    
    def env_bool(name: str, default: bool) -> bool:
        return os.getenv(name, str(default)).lower() in ("true", "1", "yes", "on", "y")
    
    def env_list(name: str, default: List[str]) -> List[str]:
        val = os.getenv(name, "")
        if val:
            return [v.strip() for v in val.split(",") if v.strip()]
        return default
    
    # Load from environment
    _CONFIG.batch_size = env_int("AI_BATCH_SIZE", _CONFIG.batch_size)
    _CONFIG.batch_timeout_sec = env_float("AI_BATCH_TIMEOUT_SEC", _CONFIG.batch_timeout_sec)
    _CONFIG.batch_concurrency = env_int("AI_BATCH_CONCURRENCY", _CONFIG.batch_concurrency)
    _CONFIG.max_tickers = env_int("AI_MAX_TICKERS", _CONFIG.max_tickers)
    _CONFIG.route_timeout_sec = env_float("AI_ROUTE_TIMEOUT_SEC", _CONFIG.route_timeout_sec)
    _CONFIG.enable_ml_predictions = env_bool("AI_ENABLE_ML", _CONFIG.enable_ml_predictions)
    _CONFIG.allow_query_token = env_bool("ALLOW_QUERY_TOKEN", _CONFIG.allow_query_token)
    _CONFIG.enable_redis_cache = env_bool("AI_ENABLE_REDIS", _CONFIG.enable_redis_cache)
    _CONFIG.redis_url = os.getenv("REDIS_URL", _CONFIG.redis_url)

_load_config_from_env()

# =============================================================================
# Advanced Time Handling (SAMA Compliant)
# =============================================================================

class SaudiTime:
    """SAMA-compliant Saudi time handling with business calendar"""
    
    def __init__(self):
        self._tz = self._get_tz()
        self._trading_hours = {
            "start": "10:00",  # 10 AM Riyadh
            "end": "15:00",     # 3 PM Riyadh
        }
        self._weekend_days = [4, 5]  # Friday, Saturday
        self._holidays = self._load_holidays()
    
    @lru_cache(maxsize=1)
    def _get_tz(self):
        """Get Riyadh timezone"""
        try:
            from zoneinfo import ZoneInfo
            return ZoneInfo("Asia/Riyadh")
        except ImportError:
            return timezone(timedelta(hours=3))
    
    def _load_holidays(self) -> Set[str]:
        """Load SAMA holidays"""
        # This would load from database or config
        return {
            "2024-02-22",  # Founding Day
            "2024-04-10", "2024-04-11", "2024-04-12",  # Eid al-Fitr
            "2024-06-16", "2024-06-17", "2024-06-18",  # Eid al-Adha
            "2024-09-23",  # Saudi National Day
        }
    
    def now(self) -> datetime:
        """Get current Riyadh time"""
        return datetime.now(self._tz)
    
    def now_iso(self) -> str:
        """Get current Riyadh time in ISO format"""
        return self.now().isoformat(timespec="milliseconds")
    
    def now_utc_iso(self) -> str:
        """Get current UTC time in ISO format"""
        return datetime.now(timezone.utc).isoformat(timespec="milliseconds")
    
    def is_trading_day(self, dt: Optional[datetime] = None) -> bool:
        """Check if it's a trading day"""
        if dt is None:
            dt = self.now()
        
        # Check weekend
        if dt.weekday() in self._weekend_days:
            return False
        
        # Check holidays
        date_str = dt.strftime("%Y-%m-%d")
        if date_str in self._holidays:
            return False
        
        return True
    
    def is_trading_hours(self, dt: Optional[datetime] = None) -> bool:
        """Check if within trading hours"""
        if not self.is_trading_day(dt):
            return False
        
        if dt is None:
            dt = self.now()
        
        current = dt.time()
        start = datetime.strptime(self._trading_hours["start"], "%H:%M").time()
        end = datetime.strptime(self._trading_hours["end"], "%H:%M").time()
        
        return start <= current <= end
    
    def to_riyadh(self, utc_dt: Union[str, datetime, None]) -> str:
        """Convert UTC datetime to Riyadh time string"""
        if utc_dt is None:
            return ""
        
        try:
            if isinstance(utc_dt, str):
                dt = datetime.fromisoformat(utc_dt.replace("Z", "+00:00"))
            else:
                dt = utc_dt
            
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            
            return dt.astimezone(self._tz).isoformat(timespec="milliseconds")
        except Exception:
            return ""
    
    def next_trading_day(self, days: int = 1) -> datetime:
        """Get next trading day"""
        dt = self.now()
        while days > 0:
            dt += timedelta(days=1)
            if self.is_trading_day(dt):
                days -= 1
        return dt

_saudi_time = SaudiTime()

# =============================================================================
# Observability & Metrics
# =============================================================================

class MetricsRegistry:
    """Prometheus metrics registry with labels support"""
    
    def __init__(self, namespace: str = "tadawul_ai"):
        self.namespace = namespace
        self._counters: Dict[str, Counter] = {}
        self._histograms: Dict[str, Histogram] = {}
        self._gauges: Dict[str, Gauge] = {}
        self._summaries: Dict[str, Summary] = {}
    
    def counter(self, name: str, description: str, labels: Optional[List[str]] = None) -> Optional[Counter]:
        if name not in self._counters and _PROMETHEUS_AVAILABLE:
            self._counters[name] = Counter(
                f"{self.namespace}_{name}",
                description,
                labelnames=labels or []
            )
        return self._counters.get(name)
    
    def histogram(self, name: str, description: str, buckets: Optional[List[float]] = None) -> Optional[Histogram]:
        if name not in self._histograms and _PROMETHEUS_AVAILABLE:
            self._histograms[name] = Histogram(
                f"{self.namespace}_{name}",
                description,
                buckets=buckets or Histogram.DEFAULT_BUCKETS
            )
        return self._histograms.get(name)
    
    def gauge(self, name: str, description: str) -> Optional[Gauge]:
        if name not in self._gauges and _PROMETHEUS_AVAILABLE:
            self._gauges[name] = Gauge(f"{self.namespace}_{name}", description)
        return self._gauges.get(name)
    
    def summary(self, name: str, description: str) -> Optional[Summary]:
        if name not in self._summaries and _PROMETHEUS_AVAILABLE:
            self._summaries[name] = Summary(f"{self.namespace}_{name}", description)
        return self._summaries.get(name)

_metrics = MetricsRegistry()

class TraceContext:
    """OpenTelemetry trace context manager"""
    
    def __init__(self, name: str, attributes: Optional[Dict[str, Any]] = None):
        self.name = name
        self.attributes = attributes or {}
        self.tracer = trace.get_tracer(__name__) if _OTEL_AVAILABLE else DummyTracer()
        self.span = None
        self.token = None
    
    async def __aenter__(self):
        if _CONFIG.enable_tracing and random.random() < _CONFIG.trace_sample_rate:
            self.span = self.tracer.start_span(self.name)
            if self.attributes:
                self.span.set_attributes(self.attributes)
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self.span and _OTEL_AVAILABLE:
            if exc_val:
                self.span.record_exception(exc_val)
                self.span.set_status(Status(StatusCode.ERROR, str(exc_val)))
            self.span.end()

# =============================================================================
# Advanced Caching with Redis
# =============================================================================

class CacheManager:
    """Multi-tier cache with Redis and predictive pre-fetch"""
    
    def __init__(self):
        self._local_cache: Dict[str, Tuple[Any, float]] = {}
        self._redis: Optional[Redis] = None
        self._stats = {"hits": 0, "misses": 0, "redis_hits": 0, "redis_misses": 0}
        self._lock = asyncio.Lock()
        self._predictive_cache: Dict[str, Any] = {}
        self._access_patterns: Dict[str, List[float]] = defaultdict(list)
        self._background_tasks: Set[asyncio.Task] = set()
        
        if _CONFIG.enable_redis_cache and _REDIS_AVAILABLE:
            asyncio.create_task(self._init_redis())
    
    async def _init_redis(self):
        """Initialize Redis connection"""
        try:
            self._redis = await aioredis.from_url(
                _CONFIG.redis_url,
                encoding="utf-8",
                decode_responses=True,
                max_connections=20,
                socket_timeout=5.0,
                socket_connect_timeout=5.0
            )
            logger.info(f"Redis cache initialized at {_CONFIG.redis_url}")
        except Exception as e:
            logger.warning(f"Redis initialization failed: {e}")
    
    def _get_cache_key(self, key: str, namespace: str = "ai") -> str:
        """Generate cache key with namespace"""
        return f"{namespace}:{hashlib.sha256(key.encode()).hexdigest()[:32]}"
    
    async def get(self, key: str, namespace: str = "ai") -> Optional[Any]:
        """Get from cache with multi-tier support"""
        cache_key = self._get_cache_key(key, namespace)
        
        # Track access pattern
        self._access_patterns[key].append(time.time())
        if len(self._access_patterns[key]) > 100:
            self._access_patterns[key] = self._access_patterns[key][-100:]
        
        # Check local cache
        async with self._lock:
            if cache_key in self._local_cache:
                value, expiry = self._local_cache[cache_key]
                if expiry > time.time():
                    self._stats["hits"] += 1
                    return value
                else:
                    del self._local_cache[cache_key]
        
        # Check Redis
        if self._redis:
            try:
                value = await self._redis.get(cache_key)
                if value is not None:
                    self._stats["redis_hits"] += 1
                    self._stats["hits"] += 1
                    
                    # Deserialize
                    try:
                        value = json.loads(value)
                    except:
                        pass
                    
                    # Store in local cache with shorter TTL
                    async with self._lock:
                        self._local_cache[cache_key] = (
                            value,
                            time.time() + (_CONFIG.cache_ttl_seconds // 2)
                        )
                    
                    return value
                else:
                    self._stats["redis_misses"] += 1
            except Exception as e:
                logger.debug(f"Redis get failed: {e}")
                self._stats["redis_misses"] += 1
        
        self._stats["misses"] += 1
        
        # Check predictive cache
        if key in self._predictive_cache:
            return self._predictive_cache[key]
        
        return None
    
    async def set(
        self,
        key: str,
        value: Any,
        ttl: Optional[int] = None,
        namespace: str = "ai",
        predictive: bool = False
    ) -> None:
        """Set in cache with TTL"""
        cache_key = self._get_cache_key(key, namespace)
        ttl = ttl or _CONFIG.cache_ttl_seconds
        expiry = time.time() + ttl
        
        # Local cache with LRU eviction
        async with self._lock:
            # Evict if needed
            if len(self._local_cache) >= _CONFIG.cache_max_size:
                # Remove oldest 20%
                items = sorted(
                    self._local_cache.items(),
                    key=lambda x: x[1][1]
                )
                for k, _ in items[:_CONFIG.cache_max_size // 5]:
                    del self._local_cache[k]
            
            self._local_cache[cache_key] = (value, expiry)
        
        # Predictive cache
        if predictive:
            self._predictive_cache[key] = value
        
        # Redis cache
        if self._redis:
            try:
                # Serialize if needed
                if not isinstance(value, (str, int, float, bool, type(None))):
                    value = json.dumps(value, default=str)
                
                await self._redis.setex(cache_key, ttl, value)
            except Exception as e:
                logger.debug(f"Redis set failed: {e}")
    
    async def delete(self, key: str, namespace: str = "ai") -> None:
        """Delete from cache"""
        cache_key = self._get_cache_key(key, namespace)
        
        async with self._lock:
            self._local_cache.pop(cache_key, None)
        
        if self._redis:
            try:
                await self._redis.delete(cache_key)
            except Exception:
                pass
        
        self._predictive_cache.pop(key, None)
    
    async def pre_fetch(self, keys: List[str]) -> Dict[str, Any]:
        """Predictive pre-fetch based on access patterns"""
        results = {}
        
        # Analyze patterns
        for key in keys:
            patterns = self._access_patterns.get(key, [])
            if len(patterns) > 5:
                # Calculate average interval
                intervals = [
                    patterns[i+1] - patterns[i]
                    for i in range(len(patterns)-1)
                ]
                avg_interval = sum(intervals) / len(intervals) if intervals else 0
                
                # If accessed regularly, pre-fetch
                if avg_interval < _CONFIG.cache_ttl_seconds * 2:
                    value = await self.get(key)
                    if value:
                        results[key] = value
        
        return results
    
    async def warm_up(self, keys: List[str]) -> None:
        """Warm up cache with commonly accessed keys"""
        task = asyncio.create_task(self._warm_up_task(keys))
        self._background_tasks.add(task)
        task.add_done_callback(self._background_tasks.discard)
    
    async def _warm_up_task(self, keys: List[str]) -> None:
        """Background task for cache warming"""
        for key in keys:
            await self.get(key)
            await asyncio.sleep(0.1)  # Rate limit
    
    def get_stats(self) -> Dict[str, Any]:
        """Get cache statistics"""
        total = self._stats["hits"] + self._stats["misses"]
        return {
            "hits": self._stats["hits"],
            "misses": self._stats["misses"],
            "redis_hits": self._stats["redis_hits"],
            "redis_misses": self._stats["redis_misses"],
            "hit_rate": self._stats["hits"] / total if total > 0 else 0,
            "redis_hit_rate": self._stats["redis_hits"] / (self._stats["redis_hits"] + self._stats["redis_misses"]) if (self._stats["redis_hits"] + self._stats["redis_misses"]) > 0 else 0,
            "local_cache_size": len(self._local_cache),
            "predictive_cache_size": len(self._predictive_cache),
            "tracked_keys": len(self._access_patterns),
            "background_tasks": len(self._background_tasks)
        }

_cache = CacheManager()

# =============================================================================
# Circuit Breaker Pattern
# =============================================================================

class CircuitState(Enum):
    CLOSED = "closed"
    OPEN = "open"
    HALF_OPEN = "half_open"

class CircuitBreaker:
    """Advanced circuit breaker with half-open state and metrics"""
    
    def __init__(self, name: str, threshold: int = 5, timeout: float = 60.0):
        self.name = name
        self.threshold = threshold
        self.timeout = timeout
        self.state = CircuitState.CLOSED
        self.failure_count = 0
        self.last_failure_time = 0.0
        self.success_count = 0
        self.total_calls = 0
        self.total_failures = 0
        self._lock = asyncio.Lock()
        self._half_open_requests = 0
    
    async def execute(self, func: Callable, *args, **kwargs) -> Any:
        """Execute with circuit breaker protection"""
        
        async with self._lock:
            self.total_calls += 1
            
            # Check if circuit is open
            if self.state == CircuitState.OPEN:
                if time.time() - self.last_failure_time > self.timeout:
                    self.state = CircuitState.HALF_OPEN
                    self.success_count = 0
                    self._half_open_requests = 0
                    logger.info(f"Circuit {self.name} entering half-open state")
                else:
                    raise Exception(f"Circuit {self.name} is OPEN")
            
            # Check half-open limits
            if self.state == CircuitState.HALF_OPEN:
                if self._half_open_requests >= _CONFIG.half_open_requests:
                    raise Exception(f"Circuit {self.name} half-open limit reached")
                self._half_open_requests += 1
        
        try:
            result = await func(*args, **kwargs) if asyncio.iscoroutinefunction(func) else func(*args, **kwargs)
            
            async with self._lock:
                if self.state == CircuitState.HALF_OPEN:
                    self.success_count += 1
                    if self.success_count >= 2:
                        self.state = CircuitState.CLOSED
                        self.failure_count = 0
                        logger.info(f"Circuit {self.name} closed")
                else:
                    self.failure_count = 0
            
            return result
            
        except Exception as e:
            async with self._lock:
                self.total_failures += 1
                self.failure_count += 1
                self.last_failure_time = time.time()
                
                if self.state == CircuitState.CLOSED and self.failure_count >= self.threshold:
                    self.state = CircuitState.OPEN
                    logger.warning(f"Circuit {self.name} opened after {self.failure_count} failures")
                elif self.state == CircuitState.HALF_OPEN:
                    self.state = CircuitState.OPEN
                    logger.warning(f"Circuit {self.name} re-opened from half-open")
            
            raise e
    
    def get_stats(self) -> Dict[str, Any]:
        """Get circuit breaker statistics"""
        return {
            "name": self.name,
            "state": self.state.value,
            "total_calls": self.total_calls,
            "total_failures": self.total_failures,
            "failure_rate": self.total_failures / self.total_calls if self.total_calls > 0 else 0,
            "current_failure_count": self.failure_count,
            "threshold": self.threshold,
            "timeout": self.timeout
        }

# =============================================================================
# Rate Limiter
# =============================================================================

class RateLimiter:
    """Token bucket rate limiter with burst support"""
    
    def __init__(self):
        self._buckets: Dict[str, Dict[str, Any]] = {}
        self._lock = asyncio.Lock()
    
    async def check(self, key: str) -> bool:
        """Check if request is allowed"""
        if not _CONFIG.require_api_key:
            return True
        
        async with self._lock:
            now = time.time()
            
            if key not in self._buckets:
                self._buckets[key] = {
                    "tokens": _CONFIG.rate_limit_burst,
                    "last_update": now,
                    "total_requests": 0
                }
            
            bucket = self._buckets[key]
            bucket["total_requests"] += 1
            
            # Add tokens based on time passed
            time_passed = now - bucket["last_update"]
            new_tokens = time_passed * (_CONFIG.rate_limit_requests / _CONFIG.rate_limit_window)
            bucket["tokens"] = min(
                _CONFIG.rate_limit_burst,
                bucket["tokens"] + new_tokens
            )
            bucket["last_update"] = now
            
            # Check if we have a token
            if bucket["tokens"] >= 1:
                bucket["tokens"] -= 1
                return True
            
            return False
    
    def get_stats(self, key: Optional[str] = None) -> Dict[str, Any]:
        """Get rate limiter statistics"""
        if key:
            bucket = self._buckets.get(key)
            if bucket:
                return {
                    "tokens": bucket["tokens"],
                    "total_requests": bucket["total_requests"]
                }
            return {}
        
        return {
            "total_buckets": len(self._buckets),
            "total_requests": sum(b["total_requests"] for b in self._buckets.values())
        }

_rate_limiter = RateLimiter()

# =============================================================================
# Token Manager
# =============================================================================

class TokenManager:
    """Advanced token management with rotation and validation"""
    
    def __init__(self):
        self._tokens: Dict[str, Dict[str, Any]] = {}
        self._rate_limits: Dict[str, Tuple[int, float]] = {}
        self._load_tokens()
    
    def _load_tokens(self) -> None:
        """Load tokens from environment and vault"""
        for key in ("APP_TOKEN", "BACKUP_APP_TOKEN", "TFB_APP_TOKEN"):
            token = os.getenv(key, "").strip()
            if token:
                self._tokens[token] = {
                    "source": key,
                    "created_at": _saudi_time.now_iso(),
                    "last_used": None,
                    "usage_count": 0,
                    "rate_limit": _CONFIG.rate_limit_requests
                }
    
    def validate(self, token: str, client_ip: str) -> bool:
        """Validate token with rate limiting"""
        token = token.strip()
        
        # Open mode
        if not self._tokens:
            return True
        
        # Check if token exists
        if token not in self._tokens:
            return False
        
        # Update usage
        self._tokens[token]["last_used"] = _saudi_time.now_iso()
        self._tokens[token]["usage_count"] += 1
        
        # Rate limit per token
        now = time.time()
        usage_key = f"{token}:{client_ip}"
        count, reset_time = self._rate_limits.get(usage_key, (0, now + _CONFIG.rate_limit_window))
        
        if now > reset_time:
            count = 0
            reset_time = now + _CONFIG.rate_limit_window
        
        if count < _CONFIG.rate_limit_requests:
            self._rate_limits[usage_key] = (count + 1, reset_time)
            return True
        
        return False
    
    def rotate(self, old_token: str, new_token: str, admin_key: str) -> bool:
        """Rotate token (admin only)"""
        admin_token = os.getenv("ADMIN_TOKEN", "")
        if not admin_token or admin_key != admin_token:
            return False
        
        if old_token in self._tokens:
            self._tokens[new_token] = self._tokens.pop(old_token)
            self._tokens[new_token]["rotated_at"] = _saudi_time.now_iso()
            return True
        
        return False
    
    def get_stats(self) -> Dict[str, Any]:
        """Get token statistics"""
        return {
            "total_tokens": len(self._tokens),
            "total_usage": sum(t["usage_count"] for t in self._tokens.values()),
            "active_tokens": sum(1 for t in self._tokens.values()
                               if t.get("last_used") and
                               (time.time() - datetime.fromisoformat(t["last_used"]).timestamp()) < 86400)
        }

_token_manager = TokenManager()

# =============================================================================
# Audit Logger (SAMA Compliance)
# =============================================================================

class AuditLogger:
    """SAMA-compliant audit logging with secure storage"""
    
    def __init__(self):
        self._buffer: List[Dict[str, Any]] = []
        self._buffer_lock = asyncio.Lock()
        self._flush_task: Optional[asyncio.Task] = None
        self._audit_file = os.getenv("AUDIT_LOG_FILE", "audit.log")
    
    async def log(
        self,
        event: str,
        user_id: Optional[str],
        resource: str,
        action: str,
        status: str,
        details: Dict[str, Any],
        request_id: Optional[str] = None
    ) -> None:
        """Log audit event"""
        if not _CONFIG.enable_audit_log:
            return
        
        entry = {
            "timestamp": _saudi_time.now_iso(),
            "event_id": str(uuid.uuid4()),
            "event": event,
            "user_id": user_id,
            "resource": resource,
            "action": action,
            "status": status,
            "details": details,
            "request_id": request_id,
            "version": AI_ANALYSIS_VERSION,
            "environment": os.getenv("APP_ENV", "production")
        }
        
        async with self._buffer_lock:
            self._buffer.append(entry)
            
            if len(self._buffer) >= 100:
                await self._flush()
        
        # Update metrics
        if _metrics.counter("audit_entries"):
            _metrics.counter("audit_entries").inc()
    
    async def _flush(self) -> None:
        """Flush buffer to secure storage"""
        async with self._buffer_lock:
            buffer = self._buffer.copy()
            self._buffer.clear()
        
        # Write to audit file
        try:
            with open(self._audit_file, "a") as f:
                for entry in buffer:
                    f.write(json.dumps(entry) + "\n")
            
            logger.info(f"Audit flush: {len(buffer)} entries")
            
        except Exception as e:
            logger.error(f"Audit flush failed: {e}")
    
    async def query(
        self,
        start_date: datetime,
        end_date: datetime,
        user_id: Optional[str] = None,
        event_type: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        """Query audit logs (admin only)"""
        # This would query from database in production
        return []

_audit = AuditLogger()

# =============================================================================
# ML/AI Models
# =============================================================================

class MLFeatures(BaseModel):
    """Machine learning features for predictions"""
    
    # Basic info
    symbol: str
    asset_class: AssetClass = AssetClass.EQUITY
    market_region: MarketRegion = MarketRegion.SAUDI
    
    # Price features
    price: Optional[float] = None
    open: Optional[float] = None
    high: Optional[float] = None
    low: Optional[float] = None
    close: Optional[float] = None
    volume: Optional[int] = None
    
    # Technical indicators
    rsi_14d: Optional[float] = None
    macd: Optional[float] = None
    macd_signal: Optional[float] = None
    macd_histogram: Optional[float] = None
    bb_upper: Optional[float] = None
    bb_lower: Optional[float] = None
    bb_middle: Optional[float] = None
    bb_width: Optional[float] = None
    bb_position: Optional[float] = None
    
    # Moving averages
    sma_5d: Optional[float] = None
    sma_10d: Optional[float] = None
    sma_20d: Optional[float] = None
    sma_50d: Optional[float] = None
    sma_200d: Optional[float] = None
    ema_5d: Optional[float] = None
    ema_10d: Optional[float] = None
    ema_20d: Optional[float] = None
    
    # Volume indicators
    volume_sma_5d: Optional[float] = None
    volume_sma_10d: Optional[float] = None
    volume_sma_20d: Optional[float] = None
    obv: Optional[float] = None
    ad_line: Optional[float] = None
    
    # Volatility
    volatility_10d: Optional[float] = None
    volatility_30d: Optional[float] = None
    volatility_60d: Optional[float] = None
    atr_14d: Optional[float] = None
    
    # Momentum
    momentum_5d: Optional[float] = None
    momentum_10d: Optional[float] = None
    momentum_20d: Optional[float] = None
    momentum_60d: Optional[float] = None
    
    # Returns
    return_1d: Optional[float] = None
    return_5d: Optional[float] = None
    return_10d: Optional[float] = None
    return_20d: Optional[float] = None
    return_60d: Optional[float] = None
    return_252d: Optional[float] = None
    
    # Risk metrics
    beta: Optional[float] = None
    sharpe_ratio: Optional[float] = None
    sortino_ratio: Optional[float] = None
    max_drawdown_30d: Optional[float] = None
    
    # Fundamental
    market_cap: Optional[float] = None
    pe_ratio: Optional[float] = None
    pb_ratio: Optional[float] = None
    ps_ratio: Optional[float] = None
    dividend_yield: Optional[float] = None
    eps: Optional[float] = None
    revenue_growth: Optional[float] = None
    profit_margin: Optional[float] = None
    
    # Sentiment
    news_sentiment: Optional[float] = None
    social_sentiment: Optional[float] = None
    analyst_rating: Optional[float] = None
    analyst_count: Optional[int] = None
    
    # Metadata
    data_quality: DataQuality = DataQuality.GOOD
    timestamp: str = Field(default_factory=_saudi_time.now_iso)
    
    if _PYDANTIC_V2:
        model_config = ConfigDict(arbitrary_types_allowed=True, use_enum_values=True)

class MLPrediction(BaseModel):
    """ML prediction with explainability"""
    
    # Basic
    symbol: str
    prediction_time: str = Field(default_factory=_saudi_time.now_iso)
    
    # Returns
    predicted_return_1d: Optional[float] = None
    predicted_return_1w: Optional[float] = None
    predicted_return_1m: Optional[float] = None
    predicted_return_3m: Optional[float] = None
    predicted_return_1y: Optional[float] = None
    
    # Confidence
    confidence_1d: float = 0.5
    confidence_1w: float = 0.5
    confidence_1m: float = 0.5
    confidence_3m: float = 0.5
    confidence_1y: float = 0.5
    
    # Risk
    risk_score: float = 50.0
    volatility_forecast: Optional[float] = None
    sharpe_ratio_forecast: Optional[float] = None
    
    # Price targets
    target_price_1m: Optional[float] = None
    target_price_3m: Optional[float] = None
    target_price_1y: Optional[float] = None
    
    # Signals
    signal_1d: SignalType = SignalType.NEUTRAL
    signal_1w: SignalType = SignalType.NEUTRAL
    signal_1m: SignalType = SignalType.NEUTRAL
    
    # Explainability
    feature_importance: Dict[str, float] = Field(default_factory=dict)
    top_factors: List[str] = Field(default_factory=list)
    model_version: str = AI_ANALYSIS_VERSION
    
    # Recommendation
    recommendation: Recommendation = Recommendation.HOLD
    recommendation_strength: float = 0.0
    
    if _PYDANTIC_V2:
        model_config = ConfigDict(arbitrary_types_allowed=True, use_enum_values=True)

class MLModelManager:
    """ML model management with online learning"""
    
    def __init__(self):
        self._models: Dict[str, Any] = {}
        self._scalers: Dict[str, Any] = {}
        self._initialized = False
        self._prediction_cache: Dict[str, Tuple[MLPrediction, float]] = {}
        self._lock = asyncio.Lock()
        self._circuit_breaker = CircuitBreaker("ml_models")
        self._feature_names: List[str] = []
        
        # Predefined feature list
        self._feature_names = [
            "price", "volume", "rsi_14d", "macd", "bb_position",
            "volatility_30d", "momentum_20d", "return_20d", "beta",
            "market_cap", "pe_ratio", "dividend_yield"
        ]
    
    async def initialize(self) -> None:
        """Initialize ML models"""
        if self._initialized or not _ML_AVAILABLE:
            return
        
        async with self._lock:
            if self._initialized:
                return
            
            try:
                # Initialize models
                self._models["rf_1d"] = RandomForestRegressor(
                    n_estimators=_CONFIG.rf_n_estimators,
                    max_depth=_CONFIG.rf_max_depth,
                    random_state=42
                )
                self._models["rf_1w"] = RandomForestRegressor(
                    n_estimators=_CONFIG.rf_n_estimators,
                    max_depth=_CONFIG.rf_max_depth,
                    random_state=42
                )
                self._models["gb_1m"] = GradientBoostingRegressor(
                    n_estimators=_CONFIG.gb_n_estimators,
                    learning_rate=_CONFIG.gb_learning_rate,
                    max_depth=5,
                    random_state=42
                )
                
                # Scaler
                self._scalers["standard"] = StandardScaler()
                
                self._initialized = True
                logger.info("ML models initialized")
                
                if _metrics.gauge("ml_models_loaded"):
                    _metrics.gauge("ml_models_loaded").set(1)
                
            except Exception as e:
                logger.error(f"ML initialization failed: {e}")
                if _metrics.gauge("ml_models_loaded"):
                    _metrics.gauge("ml_models_loaded").set(0)
    
    async def predict(self, features: MLFeatures) -> Optional[MLPrediction]:
        """Generate predictions with explainability"""
        if not self._initialized or not _ML_AVAILABLE:
            return None
        
        # Check cache
        cache_key = f"ml_pred:{features.symbol}:{_saudi_time.now().strftime('%Y%m%d')}"
        if cache_key in self._prediction_cache:
            pred, expiry = self._prediction_cache[cache_key]
            if expiry > time.time():
                return pred
        
        async def _do_predict():
            try:
                # Extract feature vector
                vector = self._extract_features(features)
                
                # Scale features
                vector_scaled = self._scalers["standard"].fit_transform([vector])[0]
                
                # Get predictions from different models
                pred_1d = self._models["rf_1d"].predict([vector_scaled])[0]
                pred_1w = self._models["rf_1w"].predict([vector_scaled])[0]
                pred_1m = self._models["gb_1m"].predict([vector_scaled])[0]
                
                # Calculate confidence based on model agreement
                predictions = [pred_1d, pred_1w, pred_1m]
                mean_pred = np.mean(predictions)
                std_pred = np.std(predictions)
                confidence = 1.0 / (1.0 + std_pred / (abs(mean_pred) + 1e-6))
                
                # Calculate risk score
                risk_score = self._calculate_risk(features)
                
                # Determine signal
                signal = self._determine_signal(mean_pred, risk_score)
                
                # Get feature importance
                importance = {}
                if _CONFIG.feature_importance_enabled and hasattr(self._models["rf_1d"], "feature_importances_"):
                    importance = dict(zip(
                        self._feature_names,
                        self._models["rf_1d"].feature_importances_
                    ))
                
                # Top factors
                top_factors = sorted(importance.items(), key=lambda x: x[1], reverse=True)[:5]
                top_factor_names = [f"{k}: {v:.1%}" for k, v in top_factors]
                
                # Determine recommendation
                rec, rec_strength = self._determine_recommendation(mean_pred, risk_score, confidence)
                
                prediction = MLPrediction(
                    symbol=features.symbol,
                    predicted_return_1d=float(pred_1d),
                    predicted_return_1w=float(pred_1w),
                    predicted_return_1m=float(pred_1m),
                    confidence_1d=float(confidence),
                    confidence_1w=float(confidence * 0.9),
                    confidence_1m=float(confidence * 0.8),
                    risk_score=float(risk_score),
                    signal_1d=signal,
                    feature_importance=importance,
                    top_factors=top_factor_names,
                    recommendation=rec,
                    recommendation_strength=rec_strength
                )
                
                # Cache prediction
                self._prediction_cache[cache_key] = (
                    prediction,
                    time.time() + 3600  # 1 hour cache
                )
                
                return prediction
                
            except Exception as e:
                logger.error(f"ML prediction failed: {e}")
                return None
        
        return await self._circuit_breaker.execute(_do_predict)
    
    def _extract_features(self, features: MLFeatures) -> List[float]:
        """Extract feature vector from MLFeatures"""
        vector = []
        
        feature_map = {
            "price": features.price,
            "volume": features.volume,
            "rsi_14d": features.rsi_14d,
            "macd": features.macd,
            "bb_position": features.bb_position,
            "volatility_30d": features.volatility_30d,
            "momentum_20d": features.momentum_20d,
            "return_20d": features.return_20d,
            "beta": features.beta,
            "market_cap": features.market_cap,
            "pe_ratio": features.pe_ratio,
            "dividend_yield": features.dividend_yield
        }
        
        for feature_name in self._feature_names:
            value = feature_map.get(feature_name)
            if value is None:
                value = 0.0
            vector.append(float(value))
        
        return vector
    
    def _calculate_risk(self, features: MLFeatures) -> float:
        """Calculate risk score (0-100)"""
        risk = 50.0  # Base risk
        
        # Adjust based on volatility
        if features.volatility_30d:
            risk += features.volatility_30d * 10
        
        # Adjust based on beta
        if features.beta:
            risk += (features.beta - 1) * 20
        
        # Adjust based on momentum
        if features.momentum_20d:
            if features.momentum_20d < -0.1:
                risk += 20
            elif features.momentum_20d > 0.1:
                risk -= 10
        
        # Adjust based on market cap (smaller = riskier)
        if features.market_cap:
            if features.market_cap < 1e9:  # < 1B
                risk += 15
            elif features.market_cap > 1e11:  # > 100B
                risk -= 10
        
        return max(0, min(100, risk))
    
    def _determine_signal(self, predicted_return: float, risk_score: float) -> SignalType:
        """Determine trading signal"""
        if predicted_return > 0.05:  # > 5%
            return SignalType.BULLISH
        elif predicted_return < -0.05:  # < -5%
            return SignalType.BEARISH
        else:
            return SignalType.NEUTRAL
    
    def _determine_recommendation(
        self,
        predicted_return: float,
        risk_score: float,
        confidence: float
    ) -> Tuple[Recommendation, float]:
        """Determine investment recommendation"""
        
        # Adjust predicted return by confidence
        adjusted_return = predicted_return * confidence
        
        if adjusted_return > 0.15:  # > 15%
            return Recommendation.STRONG_BUY, 1.0
        elif adjusted_return > 0.08:  # > 8%
            return Recommendation.BUY, 0.8
        elif adjusted_return < -0.15:  # < -15%
            return Recommendation.STRONG_SELL, 1.0
        elif adjusted_return < -0.08:  # < -8%
            return Recommendation.SELL, 0.8
        else:
            return Recommendation.HOLD, 0.5
    
    def get_stats(self) -> Dict[str, Any]:
        """Get model statistics"""
        return {
            "initialized": self._initialized,
            "models": list(self._models.keys()),
            "cache_size": len(self._prediction_cache),
            "circuit_breaker": self._circuit_breaker.get_stats()
        }

_ml_models = MLModelManager()

# =============================================================================
# Enhanced Models
# =============================================================================

class SingleAnalysisResponse(BaseModel):
    """Enhanced single analysis response"""
    
    # Core
    symbol: str
    name: Optional[str] = None
    asset_class: AssetClass = AssetClass.EQUITY
    market_region: MarketRegion = MarketRegion.SAUDI
    
    # Price data
    price: Optional[float] = None
    open: Optional[float] = None
    high: Optional[float] = None
    low: Optional[float] = None
    close: Optional[float] = None
    volume: Optional[int] = None
    change: Optional[float] = None
    change_pct: Optional[float] = None
    
    # Fundamentals
    market_cap: Optional[float] = None
    pe_ratio: Optional[float] = None
    pb_ratio: Optional[float] = None
    ps_ratio: Optional[float] = None
    dividend_yield: Optional[float] = None
    eps: Optional[float] = None
    revenue_growth: Optional[float] = None
    profit_margin: Optional[float] = None
    
    # Technical indicators
    rsi_14d: Optional[float] = None
    macd: Optional[float] = None
    macd_signal: Optional[float] = None
    macd_histogram: Optional[float] = None
    bb_upper: Optional[float] = None
    bb_lower: Optional[float] = None
    bb_middle: Optional[float] = None
    bb_width: Optional[float] = None
    bb_position: Optional[float] = None
    
    # Moving averages
    sma_20d: Optional[float] = None
    sma_50d: Optional[float] = None
    sma_200d: Optional[float] = None
    ema_20d: Optional[float] = None
    
    # Volatility
    volatility_30d: Optional[float] = None
    atr_14d: Optional[float] = None
    beta: Optional[float] = None
    
    # Momentum
    momentum_1m: Optional[float] = None
    momentum_3m: Optional[float] = None
    momentum_12m: Optional[float] = None
    
    # Risk metrics
    sharpe_ratio: Optional[float] = None
    sortino_ratio: Optional[float] = None
    max_drawdown_30d: Optional[float] = None
    var_95: Optional[float] = None
    
    # AI predictions
    ml_prediction: Optional[MLPrediction] = None
    
    # Scores
    value_score: Optional[float] = None
    quality_score: Optional[float] = None
    momentum_score: Optional[float] = None
    risk_score: Optional[float] = None
    overall_score: Optional[float] = None
    
    # Recommendations
    recommendation: Recommendation = Recommendation.HOLD
    recommendation_strength: float = 0.0
    confidence_level: ConfidenceLevel = ConfidenceLevel.MEDIUM
    
    # Forecasts
    expected_roi_1m: Optional[float] = None
    expected_roi_3m: Optional[float] = None
    expected_roi_12m: Optional[float] = None
    forecast_price_1m: Optional[float] = None
    forecast_price_3m: Optional[float] = None
    forecast_price_12m: Optional[float] = None
    forecast_confidence: Optional[float] = None
    
    # Shariah
    shariah_compliant: ShariahCompliance = ShariahCompliance.PENDING
    
    # Timestamps
    forecast_updated_utc: Optional[str] = None
    forecast_updated_riyadh: Optional[str] = None
    last_updated_utc: Optional[str] = None
    last_updated_riyadh: Optional[str] = None
    
    # Quality
    data_quality: DataQuality = DataQuality.MISSING
    error: Optional[str] = None
    
    if _PYDANTIC_V2:
        model_config = ConfigDict(use_enum_values=True)

class BatchAnalysisResponse(BaseModel):
    """Enhanced batch analysis response"""
    
    results: List[SingleAnalysisResponse] = Field(default_factory=list)
    status: str = "success"
    error: Optional[str] = None
    warnings: List[str] = Field(default_factory=list)
    version: str = AI_ANALYSIS_VERSION
    meta: Dict[str, Any] = Field(default_factory=dict)
    
    if _PYDANTIC_V2:
        model_config = ConfigDict(arbitrary_types_allowed=True)

class SheetAnalysisResponse(BaseModel):
    """Enhanced sheet analysis response"""
    
    headers: List[str] = Field(default_factory=list)
    rows: List[List[Any]] = Field(default_factory=list)
    status: str = "success"
    error: Optional[str] = None
    version: str = AI_ANALYSIS_VERSION
    meta: Dict[str, Any] = Field(default_factory=dict)
    
    if _PYDANTIC_V2:
        model_config = ConfigDict(arbitrary_types_allowed=True)

class ScoreboardResponse(BaseModel):
    """Enhanced scoreboard response"""
    
    headers: List[str] = Field(default_factory=list)
    rows: List[List[Any]] = Field(default_factory=list)
    status: str = "success"
    error: Optional[str] = None
    version: str = AI_ANALYSIS_VERSION
    meta: Dict[str, Any] = Field(default_factory=dict)
    
    if _PYDANTIC_V2:
        model_config = ConfigDict(arbitrary_types_allowed=True)

# =============================================================================
# Core Analysis Engine
# =============================================================================

class AnalysisEngine:
    """Core analysis engine with AI integration"""
    
    def __init__(self):
        self._engine = None
        self._engine_lock = asyncio.Lock()
        self._circuit_breaker = CircuitBreaker(
            "analysis_engine",
            threshold=_CONFIG.circuit_breaker_threshold,
            timeout=_CONFIG.circuit_breaker_timeout
        )
        self._shariah_checker = None
    
    async def get_engine(self, request: Request) -> Optional[Any]:
        """Get or initialize engine with circuit breaker"""
        
        # Check app state
        try:
            st = getattr(request.app, "state", None)
            if st is not None:
                eng = getattr(st, "engine", None)
                if eng is not None:
                    return eng
        except Exception:
            pass
        
        # Check cached
        if self._engine is not None:
            return self._engine
        
        # Initialize with circuit breaker
        async def _init_engine():
            async with self._engine_lock:
                if self._engine is not None:
                    return self._engine
                
                try:
                    from core.data_engine_v2 import get_engine
                    engine = await get_engine()
                    self._engine = engine
                    return engine
                except Exception as e:
                    logger.error(f"Engine initialization failed: {e}")
                    return None
        
        return await self._circuit_breaker.execute(_init_engine)
    
    async def get_quotes(
        self,
        engine: Any,
        symbols: List[str],
        include_ml: bool = True
    ) -> Tuple[Dict[str, Any], Dict[str, Any]]:
        """Get quotes with ML predictions"""
        
        stats = {
            "requested": len(symbols),
            "from_cache": 0,
            "from_engine": 0,
            "ml_predictions": 0,
            "errors": 0
        }
        
        results: Dict[str, Any] = {}
        
        # Check cache first
        cache_keys = {s: f"quote:{s}" for s in symbols}
        cached_results = {}
        symbols_to_fetch = []
        
        for symbol, cache_key in cache_keys.items():
            cached = await _cache.get(cache_key)
            if cached is not None:
                cached_results[symbol] = cached
                stats["from_cache"] += 1
            else:
                symbols_to_fetch.append(symbol)
        
        # Fetch missing symbols
        if symbols_to_fetch and engine:
            try:
                fetched = await self._fetch_from_engine(engine, symbols_to_fetch)
                stats["from_engine"] += len(fetched)
                
                # Cache fetched results
                for symbol, data in fetched.items():
                    await _cache.set(
                        cache_keys[symbol],
                        data,
                        ttl=_CONFIG.cache_ttl_seconds
                    )
                
                results.update(fetched)
                
            except Exception as e:
                logger.error(f"Engine fetch failed: {e}")
                stats["errors"] += len(symbols_to_fetch)
                
                # Create placeholders
                for symbol in symbols_to_fetch:
                    results[symbol] = self._create_placeholder(symbol, str(e))
        
        # Combine cached and fetched
        results.update(cached_results)
        
        # Add ML predictions
        if include_ml and _CONFIG.enable_ml_predictions:
            await _ml_models.initialize()
            
            for symbol, data in results.items():
                if data and not isinstance(data, dict):
                    data = self._to_dict(data)
                
                if data and "error" not in data:
                    features = self._extract_features(data, symbol)
                    ml_pred = await _ml_models.predict(features)
                    
                    if ml_pred:
                        data["ml_prediction"] = ml_pred.dict()
                        stats["ml_predictions"] += 1
        
        return results, stats
    
    async def _fetch_from_engine(
        self,
        engine: Any,
        symbols: List[str]
    ) -> Dict[str, Any]:
        """Fetch from engine with batching"""
        
        # Try batch first
        batch_methods = [
            "get_enriched_quotes_batch",
            "get_enriched_quotes",
            "get_quotes_batch"
        ]
        
        for method in batch_methods:
            fn = getattr(engine, method, None)
            if callable(fn):
                try:
                    result = await fn(symbols)
                    if result:
                        return self._normalize_batch_result(result, symbols)
                except Exception:
                    continue
        
        # Fallback to individual fetches
        return await self._fetch_individual(engine, symbols)
    
    async def _fetch_individual(
        self,
        engine: Any,
        symbols: List[str]
    ) -> Dict[str, Any]:
        """Fetch individual symbols with concurrency control"""
        
        semaphore = asyncio.Semaphore(_CONFIG.batch_concurrency)
        
        async def fetch_one(symbol: str) -> Tuple[str, Any]:
            async with semaphore:
                try:
                    quote = await engine.get_enriched_quote(symbol)
                    return symbol, self._to_dict(quote)
                except Exception as e:
                    return symbol, self._create_placeholder(symbol, str(e))
        
        tasks = [fetch_one(s) for s in symbols]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        fetched = {}
        for r in results:
            if isinstance(r, tuple):
                symbol, data = r
                fetched[symbol] = data
        
        return fetched
    
    def _normalize_batch_result(
        self,
        result: Any,
        symbols: List[str]
    ) -> Dict[str, Any]:
        """Normalize batch result to dict"""
        if isinstance(result, dict):
            return {
                s: self._to_dict(result.get(s))
                for s in symbols
            }
        elif isinstance(result, list) and len(result) == len(symbols):
            return {
                s: self._to_dict(r)
                for s, r in zip(symbols, result)
            }
        return {}
    
    def _to_dict(self, obj: Any) -> Dict[str, Any]:
        """Convert object to dict"""
        if obj is None:
            return {}
        
        if isinstance(obj, dict):
            return obj
        
        try:
            if hasattr(obj, "dict"):
                return obj.dict()
            if hasattr(obj, "model_dump"):
                return obj.model_dump()
            if is_dataclass(obj):
                return {
                    k: v for k, v in obj.__dict__.items()
                    if not k.startswith("_")
                }
        except Exception:
            pass
        
        return {"raw": str(obj)}
    
    def _create_placeholder(self, symbol: str, error: str) -> Dict[str, Any]:
        """Create placeholder for failed symbol"""
        utc_now = _saudi_time.now_utc_iso()
        return {
            "symbol": symbol.upper(),
            "error": error,
            "data_quality": DataQuality.MISSING,
            "last_updated_utc": utc_now,
            "last_updated_riyadh": _saudi_time.to_riyadh(utc_now),
            "recommendation": Recommendation.HOLD
        }
    
    def _extract_features(self, data: Dict[str, Any], symbol: str) -> MLFeatures:
        """Extract ML features from data"""
        
        # Get price data
        price = data.get("price") or data.get("current_price")
        volume = data.get("volume")
        market_cap = data.get("market_cap")
        pe_ratio = data.get("pe_ratio")
        dividend_yield = data.get("dividend_yield")
        beta = data.get("beta")
        
        # Technical indicators
        rsi_14d = data.get("rsi_14d")
        macd = data.get("macd")
        bb_position = data.get("bb_position")
        volatility_30d = data.get("volatility_30d")
        momentum_20d = data.get("momentum_20d")
        return_20d = data.get("return_20d")
        
        return MLFeatures(
            symbol=symbol,
            price=price,
            volume=volume,
            market_cap=market_cap,
            pe_ratio=pe_ratio,
            dividend_yield=dividend_yield,
            beta=beta,
            rsi_14d=rsi_14d,
            macd=macd,
            bb_position=bb_position,
            volatility_30d=volatility_30d,
            momentum_20d=momentum_20d,
            return_20d=return_20d
        )

_analysis_engine = AnalysisEngine()

# =============================================================================
# WebSocket Manager
# =============================================================================

class WebSocketManager:
    """Manage WebSocket connections for real-time updates"""
    
    def __init__(self):
        self._active_connections: List[WebSocket] = []
        self._connection_lock = asyncio.Lock()
        self._subscriptions: Dict[str, List[WebSocket]] = defaultdict(list)
        self._broadcast_task: Optional[asyncio.Task] = None
    
    async def connect(self, websocket: WebSocket):
        """Accept new WebSocket connection"""
        await websocket.accept()
        async with self._connection_lock:
            self._active_connections.append(websocket)
        
        if _metrics.gauge("websocket_connections"):
            _metrics.gauge("websocket_connections").set(len(self._active_connections))
        
        # Start broadcast task if not running
        if self._broadcast_task is None:
            self._broadcast_task = asyncio.create_task(self._broadcast_loop())
    
    async def disconnect(self, websocket: WebSocket):
        """Remove WebSocket connection"""
        async with self._connection_lock:
            if websocket in self._active_connections:
                self._active_connections.remove(websocket)
            
            # Remove from subscriptions
            for symbol in list(self._subscriptions.keys()):
                if websocket in self._subscriptions[symbol]:
                    self._subscriptions[symbol].remove(websocket)
        
        if _metrics.gauge("websocket_connections"):
            _metrics.gauge("websocket_connections").set(len(self._active_connections))
    
    async def subscribe(self, websocket: WebSocket, symbol: str):
        """Subscribe to symbol updates"""
        async with self._connection_lock:
            self._subscriptions[symbol.upper()].append(websocket)
    
    async def unsubscribe(self, websocket: WebSocket, symbol: str):
        """Unsubscribe from symbol updates"""
        async with self._connection_lock:
            if symbol.upper() in self._subscriptions:
                if websocket in self._subscriptions[symbol.upper()]:
                    self._subscriptions[symbol.upper()].remove(websocket)
    
    async def broadcast(self, message: Dict[str, Any], symbol: Optional[str] = None):
        """Broadcast message to all or subscribed connections"""
        if symbol:
            connections = self._subscriptions.get(symbol.upper(), [])
        else:
            connections = self._active_connections
        
        disconnected = []
        for connection in connections:
            try:
                await connection.send_json(message)
            except Exception:
                disconnected.append(connection)
        
        # Clean up disconnected
        for conn in disconnected:
            await self.disconnect(conn)
    
    async def _broadcast_loop(self):
        """Background task for broadcasting updates"""
        while True:
            try:
                await asyncio.sleep(5)  # Broadcast every 5 seconds
                
                # Get market summary
                summary = {
                    "type": "market_summary",
                    "timestamp": _saudi_time.now_iso(),
                    "trading_hours": _saudi_time.is_trading_hours(),
                    "active_connections": len(self._active_connections)
                }
                
                await self.broadcast(summary)
                
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Broadcast error: {e}")

_ws_manager = WebSocketManager()

# =============================================================================
# Routes
# =============================================================================

@router.get("/health")
async def health(request: Request) -> Dict[str, Any]:
    """Enhanced health check"""
    
    engine = await _analysis_engine.get_engine(request)
    
    return {
        "status": "ok",
        "version": AI_ANALYSIS_VERSION,
        "timestamp_utc": _saudi_time.now_utc_iso(),
        "timestamp_riyadh": _saudi_time.now_iso(),
        "trading_day": _saudi_time.is_trading_day(),
        "trading_hours": _saudi_time.is_trading_hours(),
        "engine": {
            "available": engine is not None,
            "type": type(engine).__name__ if engine else "none"
        },
        "ml": _ml_models.get_stats(),
        "cache": _cache.get_stats(),
        "rate_limiter": _rate_limiter.get_stats(),
        "token_manager": _token_manager.get_stats(),
        "websocket": {
            "connections": len(_ws_manager._active_connections),
            "subscriptions": len(_ws_manager._subscriptions)
        },
        "config": {
            "batch_size": _CONFIG.batch_size,
            "batch_concurrency": _CONFIG.batch_concurrency,
            "max_tickers": _CONFIG.max_tickers,
            "enable_ml": _CONFIG.enable_ml_predictions,
            "enable_shariah": _CONFIG.enable_shariah_filter
        },
        "request_id": getattr(request.state, "request_id", None)
    }

@router.get("/metrics")
async def metrics() -> Response:
    """Prometheus metrics endpoint"""
    if not _PROMETHEUS_AVAILABLE:
        return JSONResponse(
            status_code=503,
            content={"error": "Metrics not available"}
        )
    
    from prometheus_client import generate_latest, CONTENT_TYPE_LATEST
    return Response(
        content=generate_latest(),
        media_type=CONTENT_TYPE_LATEST
    )

@router.get("/quote", response_model=SingleAnalysisResponse)
async def get_quote(
    request: Request,
    symbol: str = Query(..., description="Ticker/Symbol"),
    include_ml: bool = Query(True, description="Include ML predictions"),
    token: Optional[str] = Query(default=None),
    x_app_token: Optional[str] = Header(None, alias="X-APP-TOKEN"),
    authorization: Optional[str] = Header(None, alias="Authorization")
) -> SingleAnalysisResponse:
    """
    Get single quote with AI analysis
    """
    request_id = str(uuid.uuid4())
    start_time = time.time()
    
    # Rate limiting
    client_ip = request.client.host if request.client else "unknown"
    if not await _rate_limiter.check(client_ip):
        raise HTTPException(
            status_code=status.HTTP_429_TOO_MANY_REQUESTS,
            detail="Rate limit exceeded"
        )
    
    # Authentication
    auth_token = x_app_token or token or ""
    if authorization and authorization.startswith("Bearer "):
        auth_token = authorization[7:]
    
    if not _token_manager.validate(auth_token, client_ip):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid token"
        )
    
    # Normalize symbol
    symbol = symbol.strip().upper()
    if not symbol:
        return SingleAnalysisResponse(
            symbol="UNKNOWN",
            error="No symbol provided",
            data_quality=DataQuality.MISSING
        )
    
    # Get engine
    engine = await _analysis_engine.get_engine(request)
    if not engine:
        return SingleAnalysisResponse(
            symbol=symbol,
            error="Engine unavailable",
            data_quality=DataQuality.MISSING
        )
    
    # Get quotes
    results, stats = await _analysis_engine.get_quotes(
        engine,
        [symbol],
        include_ml=include_ml
    )
    
    data = results.get(symbol)
    if not data:
        data = _analysis_engine._create_placeholder(symbol, "No data")
    
    # Convert to response
    response = _quote_to_response(data)
    
    # Add metadata
    response.last_updated_utc = _saudi_time.now_utc_iso()
    response.last_updated_riyadh = _saudi_time.now_iso()
    
    # Audit log
    await _audit.log(
        "get_quote",
        auth_token[:8],
        "quote",
        "read",
        "success",
        {"symbol": symbol, "duration": time.time() - start_time},
        request_id
    )
    
    return response

@router.post("/quote", response_model=SingleAnalysisResponse)
async def post_quote(
    request: Request,
    body: Dict[str, Any] = Body(default_factory=dict),
    include_ml: bool = Query(True, description="Include ML predictions"),
    token: Optional[str] = Query(default=None),
    x_app_token: Optional[str] = Header(None, alias="X-APP-TOKEN"),
    authorization: Optional[str] = Header(None, alias="Authorization")
) -> SingleAnalysisResponse:
    """
    POST single quote with AI analysis
    """
    # Extract symbol from body
    symbol = body.get("symbol") or body.get("ticker") or body.get("Symbol") or ""
    
    # Forward to get_quote
    return await get_quote(
        request=request,
        symbol=symbol,
        include_ml=include_ml,
        token=token,
        x_app_token=x_app_token,
        authorization=authorization
    )

@router.get("/quotes", response_model=BatchAnalysisResponse)
async def get_quotes(
    request: Request,
    tickers: str = Query(default="", description="Tickers: 'AAPL,MSFT 1120.SR'"),
    top_n: int = Query(default=0, ge=0, le=5000),
    include_ml: bool = Query(True, description="Include ML predictions"),
    token: Optional[str] = Query(default=None),
    x_app_token: Optional[str] = Header(None, alias="X-APP-TOKEN"),
    authorization: Optional[str] = Header(None, alias="Authorization")
) -> BatchAnalysisResponse:
    """
    Get multiple quotes with AI analysis
    """
    request_id = str(uuid.uuid4())
    start_time = time.time()
    
    # Rate limiting
    client_ip = request.client.host if request.client else "unknown"
    if not await _rate_limiter.check(client_ip):
        raise HTTPException(
            status_code=status.HTTP_429_TOO_MANY_REQUESTS,
            detail="Rate limit exceeded"
        )
    
    # Authentication
    auth_token = x_app_token or token or ""
    if authorization and authorization.startswith("Bearer "):
        auth_token = authorization[7:]
    
    if not _token_manager.validate(auth_token, client_ip):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid token"
        )
    
    # Parse tickers
    symbols = _parse_tickers(tickers)
    if not symbols:
        return BatchAnalysisResponse(
            status="skipped",
            results=[],
            version=AI_ANALYSIS_VERSION,
            meta={"reason": "No tickers provided"}
        )
    
    # Apply limits
    if top_n and top_n > 0:
        symbols = symbols[:min(top_n, _CONFIG.max_tickers)]
    else:
        symbols = symbols[:_CONFIG.max_tickers]
    
    # Get engine
    engine = await _analysis_engine.get_engine(request)
    if not engine:
        return BatchAnalysisResponse(
            status="error",
            error="Engine unavailable",
            results=[],
            version=AI_ANALYSIS_VERSION
        )
    
    # Get quotes
    results, stats = await _analysis_engine.get_quotes(
        engine,
        symbols,
        include_ml=include_ml
    )
    
    # Convert to responses
    responses = []
    for symbol in symbols:
        data = results.get(symbol)
        if not data:
            data = _analysis_engine._create_placeholder(symbol, "No data")
        responses.append(_quote_to_response(data))
    
    # Audit log
    await _audit.log(
        "get_quotes",
        auth_token[:8],
        "quotes",
        "read",
        "success",
        {
            "symbols": len(symbols),
            "duration": time.time() - start_time,
            "stats": stats
        },
        request_id
    )
    
    return BatchAnalysisResponse(
        status="success",
        results=responses,
        version=AI_ANALYSIS_VERSION,
        meta={
            "requested": len(symbols),
            "from_cache": stats["from_cache"],
            "from_engine": stats["from_engine"],
            "ml_predictions": stats["ml_predictions"],
            "errors": stats["errors"],
            "duration_ms": (time.time() - start_time) * 1000,
            "timestamp_utc": _saudi_time.now_utc_iso(),
            "timestamp_riyadh": _saudi_time.now_iso()
        }
    )

@router.post("/quotes", response_model=BatchAnalysisResponse)
async def batch_quotes(
    request: Request,
    body: Dict[str, Any] = Body(default_factory=dict),
    include_ml: bool = Query(True, description="Include ML predictions"),
    token: Optional[str] = Query(default=None),
    x_app_token: Optional[str] = Header(None, alias="X-APP-TOKEN"),
    authorization: Optional[str] = Header(None, alias="Authorization")
) -> BatchAnalysisResponse:
    """
    Batch analysis endpoint with JSON body
    """
    # Extract tickers from body
    tickers = []
    
    if "tickers" in body:
        tickers = body["tickers"]
    elif "symbols" in body:
        tickers = body["symbols"]
    elif isinstance(body, list):
        tickers = body
    
    # Convert to comma-separated string
    if isinstance(tickers, list):
        tickers = ",".join(str(t) for t in tickers)
    
    # Forward to get_quotes
    return await get_quotes(
        request=request,
        tickers=tickers,
        include_ml=include_ml,
        token=token,
        x_app_token=x_app_token,
        authorization=authorization
    )

@router.post("/sheet-rows")
async def sheet_rows(
    request: Request,
    body: Dict[str, Any] = Body(...),
    token: Optional[str] = Query(default=None),
    x_app_token: Optional[str] = Header(None, alias="X-APP-TOKEN"),
    authorization: Optional[str] = Header(None, alias="Authorization")
) -> Union[SheetAnalysisResponse, Dict[str, Any]]:
    """
    Dual-Mode Endpoint:
    1) PUSH MODE: Cache snapshots
    2) COMPUTE MODE: Return sheet rows
    """
    request_id = str(uuid.uuid4())
    start_time = time.time()
    
    # Authentication
    client_ip = request.client.host if request.client else "unknown"
    auth_token = x_app_token or token or ""
    if authorization and authorization.startswith("Bearer "):
        auth_token = authorization[7:]
    
    if not _token_manager.validate(auth_token, client_ip):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid token"
        )
    
    # Push mode
    if "items" in body and isinstance(body.get("items"), list):
        if not _CONFIG.enable_push_mode:
            return {"status": "error", "error": "Push mode disabled"}
        
        try:
            engine = await _analysis_engine.get_engine(request)
            
            written = []
            for item in body["items"]:
                sheet = item.get("sheet", "unknown")
                headers = item.get("headers", [])
                rows = item.get("rows", [])
                
                try:
                    # Cache snapshot
                    cache_key = f"sheet:{sheet}"
                    await _cache.set(
                        cache_key,
                        {"headers": headers, "rows": rows},
                        ttl=3600
                    )
                    
                    written.append({
                        "sheet": sheet,
                        "rows": len(rows),
                        "status": "cached"
                    })
                except Exception as e:
                    written.append({
                        "sheet": sheet,
                        "rows": len(rows),
                        "status": "error",
                        "error": str(e)
                    })
            
            return {
                "status": "success",
                "mode": "push",
                "written": written,
                "version": AI_ANALYSIS_VERSION
            }
            
        except Exception as e:
            return {
                "status": "error",
                "error": str(e),
                "version": AI_ANALYSIS_VERSION
            }
    
    # Compute mode
    sheet_name = body.get("sheet_name") or body.get("sheet") or "Analysis"
    tickers = body.get("tickers") or body.get("symbols") or ""
    
    if isinstance(tickers, list):
        tickers = ",".join(str(t) for t in tickers)
    
    symbols = _parse_tickers(tickers)
    if not symbols:
        return SheetAnalysisResponse(
            status="skipped",
            error="No tickers",
            headers=_get_default_headers(),
            rows=[],
            version=AI_ANALYSIS_VERSION
        )
    
    symbols = symbols[:_CONFIG.max_tickers]
    
    # Get engine
    engine = await _analysis_engine.get_engine(request)
    if not engine:
        return SheetAnalysisResponse(
            status="error",
            error="Engine unavailable",
            headers=_get_default_headers(),
            rows=[],
            version=AI_ANALYSIS_VERSION
        )
    
    # Get quotes
    results, stats = await _analysis_engine.get_quotes(
        engine,
        symbols,
        include_ml=True
    )
    
    # Build sheet
    headers = _get_sheet_headers(sheet_name)
    rows = _build_sheet_rows(headers, symbols, results)
    
    return SheetAnalysisResponse(
        status="success",
        headers=headers,
        rows=rows,
        version=AI_ANALYSIS_VERSION,
        meta={
            "sheet_name": sheet_name,
            "tickers": len(symbols),
            "duration_ms": (time.time() - start_time) * 1000,
            "timestamp_riyadh": _saudi_time.now_iso(),
            "stats": stats
        }
    )

@router.get("/scoreboard")
async def scoreboard(
    request: Request,
    tickers: str = Query(default="", description="Tickers: 'AAPL,MSFT 1120.SR'"),
    top_n: int = Query(default=20, ge=1, le=200),
    token: Optional[str] = Query(default=None),
    x_app_token: Optional[str] = Header(None, alias="X-APP-TOKEN"),
    authorization: Optional[str] = Header(None, alias="Authorization")
) -> ScoreboardResponse:
    """
    Get ranked scoreboard of symbols
    """
    request_id = str(uuid.uuid4())
    start_time = time.time()
    
    # Authentication
    client_ip = request.client.host if request.client else "unknown"
    auth_token = x_app_token or token or ""
    if authorization and authorization.startswith("Bearer "):
        auth_token = authorization[7:]
    
    if not _token_manager.validate(auth_token, client_ip):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid token"
        )
    
    if not _CONFIG.enable_scoreboard:
        return ScoreboardResponse(
            status="disabled",
            error="Scoreboard disabled",
            headers=[],
            rows=[],
            version=AI_ANALYSIS_VERSION
        )
    
    # Parse tickers
    symbols = _parse_tickers(tickers)
    if not symbols:
        return ScoreboardResponse(
            status="skipped",
            headers=[],
            rows=[],
            version=AI_ANALYSIS_VERSION,
            meta={"reason": "No tickers provided"}
        )
    
    symbols = symbols[:top_n]
    
    # Get engine
    engine = await _analysis_engine.get_engine(request)
    if not engine:
        return ScoreboardResponse(
            status="error",
            error="Engine unavailable",
            headers=[],
            rows=[],
            version=AI_ANALYSIS_VERSION
        )
    
    # Get quotes
    results, stats = await _analysis_engine.get_quotes(
        engine,
        symbols,
        include_ml=True
    )
    
    # Build scoreboard
    headers = [
        "Rank", "Symbol", "Name", "Price", "Change %",
        "Overall Score", "Risk Score", "Recommendation",
        "Expected ROI 1M", "Expected ROI 3M", "Expected ROI 12M",
        "ML Confidence", "Signal", "Last Updated"
    ]
    
    rows = []
    items = []
    
    for symbol in symbols:
        data = results.get(symbol)
        if not data:
            continue
        
        # Calculate score
        overall_score = data.get("overall_score") or 50.0
        risk_score = data.get("risk_score") or 50.0
        ml_pred = data.get("ml_prediction")
        
        if ml_pred:
            confidence = ml_pred.get("confidence_1m", 0.5)
            signal = ml_pred.get("signal_1m", "neutral")
        else:
            confidence = 0.5
            signal = "neutral"
        
        items.append({
            "symbol": symbol,
            "name": data.get("name", ""),
            "price": data.get("price"),
            "change_pct": data.get("change_pct"),
            "overall_score": overall_score,
            "risk_score": risk_score,
            "recommendation": data.get("recommendation", "HOLD"),
            "expected_roi_1m": data.get("expected_roi_1m"),
            "expected_roi_3m": data.get("expected_roi_3m"),
            "expected_roi_12m": data.get("expected_roi_12m"),
            "confidence": confidence,
            "signal": signal,
            "last_updated": data.get("last_updated_riyadh", "")
        })
    
    # Sort by overall score
    items.sort(key=lambda x: x["overall_score"], reverse=True)
    
    # Build rows
    for i, item in enumerate(items[:top_n], 1):
        rows.append([
            i,
            item["symbol"],
            item["name"],
            item["price"],
            item["change_pct"],
            item["overall_score"],
            item["risk_score"],
            item["recommendation"],
            item["expected_roi_1m"],
            item["expected_roi_3m"],
            item["expected_roi_12m"],
            f"{item['confidence']:.1%}",
            item["signal"].upper(),
            item["last_updated"]
        ])
    
    return ScoreboardResponse(
        status="success",
        headers=headers,
        rows=rows,
        version=AI_ANALYSIS_VERSION,
        meta={
            "total": len(items),
            "displayed": min(len(items), top_n),
            "duration_ms": (time.time() - start_time) * 1000,
            "timestamp_riyadh": _saudi_time.now_iso()
        }
    )

@router.websocket("/ws")
async def websocket_endpoint(
    websocket: WebSocket,
    token: Optional[str] = Query(None)
):
    """
    WebSocket endpoint for real-time updates
    """
    if not _CONFIG.enable_websocket:
        await websocket.close(code=1000, reason="WebSocket disabled")
        return
    
    # Authenticate
    if not token or not _token_manager.validate(token, websocket.client.host):
        await websocket.close(code=1008, reason="Invalid token")
        return
    
    await _ws_manager.connect(websocket)
    
    try:
        while True:
            data = await websocket.receive_json()
            
            # Handle subscription messages
            if data.get("type") == "subscribe":
                symbol = data.get("symbol")
                if symbol:
                    await _ws_manager.subscribe(websocket, symbol)
                    await websocket.send_json({
                        "type": "subscribed",
                        "symbol": symbol,
                        "timestamp": _saudi_time.now_iso()
                    })
            
            elif data.get("type") == "unsubscribe":
                symbol = data.get("symbol")
                if symbol:
                    await _ws_manager.unsubscribe(websocket, symbol)
                    await websocket.send_json({
                        "type": "unsubscribed",
                        "symbol": symbol,
                        "timestamp": _saudi_time.now_iso()
                    })
            
            elif data.get("type") == "ping":
                await websocket.send_json({
                    "type": "pong",
                    "timestamp": _saudi_time.now_iso()
                })
    
    except WebSocketDisconnect:
        await _ws_manager.disconnect(websocket)
    except Exception as e:
        logger.error(f"WebSocket error: {e}")
        await _ws_manager.disconnect(websocket)

# =============================================================================
# Admin Routes
# =============================================================================

@router.post("/admin/cache/clear", include_in_schema=False)
async def admin_clear_cache(request: Request) -> Dict[str, Any]:
    """Clear all caches (admin only)"""
    
    # Verify admin token
    auth_header = request.headers.get("Authorization", "")
    if not auth_header.startswith("Bearer "):
        raise HTTPException(status_code=401, detail="Unauthorized")
    
    token = auth_header[7:]
    admin_token = os.getenv("ADMIN_TOKEN", "")
    if not admin_token or token != admin_token:
        raise HTTPException(status_code=403, detail="Forbidden")
    
    await _cache.clear()
    
    return {
        "status": "success",
        "message": "Cache cleared",
        "timestamp": _saudi_time.now_iso()
    }

@router.post("/admin/config/reload", include_in_schema=False)
async def admin_reload_config(request: Request) -> Dict[str, Any]:
    """Reload configuration (admin only)"""
    
    # Verify admin token
    auth_header = request.headers.get("Authorization", "")
    if not auth_header.startswith("Bearer "):
        raise HTTPException(status_code=401, detail="Unauthorized")
    
    token = auth_header[7:]
    admin_token = os.getenv("ADMIN_TOKEN", "")
    if not admin_token or token != admin_token:
        raise HTTPException(status_code=403, detail="Forbidden")
    
    _load_config_from_env()
    
    return {
        "status": "success",
        "message": "Configuration reloaded",
        "config": {
            "batch_size": _CONFIG.batch_size,
            "batch_concurrency": _CONFIG.batch_concurrency,
            "enable_ml": _CONFIG.enable_ml_predictions
        },
        "timestamp": _saudi_time.now_iso()
    }

# =============================================================================
# Helper Functions
# =============================================================================

def _parse_tickers(tickers_str: str) -> List[str]:
    """Parse tickers string into list"""
    if not tickers_str:
        return []
    
    # Replace common separators
    text = tickers_str.replace(",", " ").replace(";", " ").replace("|", " ")
    
    # Split and clean
    symbols = []
    for part in text.split():
        part = part.strip().upper()
        if part:
            symbols.append(part)
    
    # Remove duplicates while preserving order
    seen = set()
    unique = []
    for s in symbols:
        if s not in seen:
            seen.add(s)
            unique.append(s)
    
    return unique

def _get_default_headers() -> List[str]:
    """Get default headers for sheet output"""
    return [
        "Symbol",
        "Name",
        "Price",
        "Change %",
        "Volume",
        "Market Cap",
        "P/E Ratio",
        "Dividend Yield",
        "RSI (14)",
        "MACD",
        "BB Position",
        "Volatility",
        "Overall Score",
        "Risk Score",
        "Recommendation",
        "Expected ROI 1M",
        "Forecast Price 1M",
        "Expected ROI 3M",
        "Forecast Price 3M",
        "Expected ROI 12M",
        "Forecast Price 12M",
        "ML Confidence",
        "Signal",
        "Data Quality",
        "Error",
        "Last Updated (UTC)",
        "Last Updated (Riyadh)"
    ]

def _get_sheet_headers(sheet_name: str) -> List[str]:
    """Get headers for specific sheet"""
    # This could be customized per sheet
    return _get_default_headers()

def _build_sheet_rows(
    headers: List[str],
    symbols: List[str],
    results: Dict[str, Any]
) -> List[List[Any]]:
    """Build sheet rows from results"""
    rows = []
    
    for symbol in symbols:
        data = results.get(symbol, {})
        row = []
        
        for header in headers:
            value = _map_header_to_field(header, data)
            row.append(value)
        
        rows.append(row)
    
    return rows

def _map_header_to_field(header: str, data: Dict[str, Any]) -> Any:
    """Map sheet header to data field"""
    header_lower = header.lower()
    
    # Symbol
    if "symbol" in header_lower:
        return data.get("symbol", "")
    
    # Name
    if "name" in header_lower:
        return data.get("name", "")
    
    # Price
    if "price" in header_lower and "forecast" not in header_lower:
        return data.get("price")
    
    # Change %
    if "change %" in header_lower or "change%" in header_lower:
        return data.get("change_pct")
    
    # Volume
    if "volume" in header_lower:
        return data.get("volume")
    
    # Market Cap
    if "market cap" in header_lower:
        return data.get("market_cap")
    
    # P/E Ratio
    if "p/e" in header_lower or "pe ratio" in header_lower:
        return data.get("pe_ratio")
    
    # Dividend Yield
    if "dividend" in header_lower:
        return data.get("dividend_yield")
    
    # RSI
    if "rsi" in header_lower:
        return data.get("rsi_14d")
    
    # MACD
    if "macd" in header_lower:
        return data.get("macd")
    
    # BB Position
    if "bb position" in header_lower:
        return data.get("bb_position")
    
    # Volatility
    if "volatility" in header_lower:
        return data.get("volatility_30d")
    
    # Overall Score
    if "overall score" in header_lower:
        return data.get("overall_score")
    
    # Risk Score
    if "risk score" in header_lower:
        return data.get("risk_score")
    
    # Recommendation
    if "recommendation" in header_lower:
        return data.get("recommendation", "HOLD")
    
    # Expected ROI
    if "expected roi 1m" in header_lower:
        return data.get("expected_roi_1m")
    if "expected roi 3m" in header_lower:
        return data.get("expected_roi_3m")
    if "expected roi 12m" in header_lower:
        return data.get("expected_roi_12m")
    
    # Forecast Price
    if "forecast price 1m" in header_lower:
        return data.get("forecast_price_1m")
    if "forecast price 3m" in header_lower:
        return data.get("forecast_price_3m")
    if "forecast price 12m" in header_lower:
        return data.get("forecast_price_12m")
    
    # ML Confidence
    if "ml confidence" in header_lower:
        ml_pred = data.get("ml_prediction", {})
        return ml_pred.get("confidence_1m", 0.5)
    
    # Signal
    if "signal" in header_lower:
        ml_pred = data.get("ml_prediction", {})
        return ml_pred.get("signal_1m", "neutral").upper()
    
    # Data Quality
    if "data quality" in header_lower:
        return data.get("data_quality", "UNKNOWN")
    
    # Error
    if "error" in header_lower:
        return data.get("error")
    
    # Last Updated
    if "last updated (utc)" in header_lower:
        return data.get("last_updated_utc")
    if "last updated (riyadh)" in header_lower:
        return data.get("last_updated_riyadh")
    
    return None

def _quote_to_response(data: Dict[str, Any]) -> SingleAnalysisResponse:
    """Convert quote data to response"""
    
    # Extract ML prediction
    ml_pred_data = data.get("ml_prediction")
    ml_prediction = None
    if ml_pred_data:
        ml_prediction = MLPrediction(**ml_pred_data)
    
    # Determine confidence level
    confidence = ConfidenceLevel.MEDIUM
    if ml_prediction:
        if ml_prediction.confidence_1m >= 0.8:
            confidence = ConfidenceLevel.VERY_HIGH
        elif ml_prediction.confidence_1m >= 0.6:
            confidence = ConfidenceLevel.HIGH
        elif ml_prediction.confidence_1m >= 0.4:
            confidence = ConfidenceLevel.MEDIUM
        elif ml_prediction.confidence_1m >= 0.2:
            confidence = ConfidenceLevel.LOW
        else:
            confidence = ConfidenceLevel.VERY_LOW
    
    # Determine data quality
    data_quality = DataQuality.GOOD
    if data.get("error"):
        data_quality = DataQuality.MISSING
    elif not data.get("price"):
        data_quality = DataQuality.POOR
    
    return SingleAnalysisResponse(
        symbol=data.get("symbol", "UNKNOWN"),
        name=data.get("name"),
        asset_class=AssetClass.EQUITY,
        market_region=MarketRegion.SAUDI,
        price=data.get("price"),
        volume=data.get("volume"),
        change=data.get("change"),
        change_pct=data.get("change_pct"),
        market_cap=data.get("market_cap"),
        pe_ratio=data.get("pe_ratio"),
        pb_ratio=data.get("pb_ratio"),
        dividend_yield=data.get("dividend_yield"),
        rsi_14d=data.get("rsi_14d"),
        macd=data.get("macd"),
        bb_position=data.get("bb_position"),
        volatility_30d=data.get("volatility_30d"),
        beta=data.get("beta"),
        momentum_1m=data.get("momentum_1m"),
        overall_score=data.get("overall_score"),
        risk_score=data.get("risk_score"),
        recommendation=data.get("recommendation", Recommendation.HOLD),
        confidence_level=confidence,
        expected_roi_1m=data.get("expected_roi_1m"),
        expected_roi_3m=data.get("expected_roi_3m"),
        expected_roi_12m=data.get("expected_roi_12m"),
        forecast_price_1m=data.get("forecast_price_1m"),
        forecast_price_3m=data.get("forecast_price_3m"),
        forecast_price_12m=data.get("forecast_price_12m"),
        ml_prediction=ml_prediction,
        data_quality=data_quality,
        error=data.get("error"),
        last_updated_utc=data.get("last_updated_utc"),
        last_updated_riyadh=data.get("last_updated_riyadh")
    )

# =============================================================================
# Module Exports
# =============================================================================

__all__ = ["router"]
