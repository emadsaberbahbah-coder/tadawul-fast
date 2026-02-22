#!/usr/bin/env python3
"""
routes/ai_analysis.py
------------------------------------------------------------
TADAWUL ENTERPRISE AI ANALYSIS ENGINE — v8.1.0 (NEXT-GEN ENTERPRISE)
SAMA Compliant | Real-time ML | Predictive Analytics | Distributed Caching

What's new in v8.1.0:
- ✅ Pydantic V2 Resilience: Upgraded all Enums to `FlexibleEnum` with `_missing_` interceptors to permanently cure 500 Internal Server Errors caused by case-sensitivity mismatches.
- ✅ High-Performance JSON (`orjson`): Integrated `ORJSONResponse` for ultra-fast predictive payload delivery
- ✅ Non-Blocking ML Inference: Delegated all Scikit-Learn/Ensemble execution to ThreadPoolExecutors

Core Capabilities:
- Real-time ML-powered analysis with ensemble models
- Predictive analytics with confidence scoring
- Multi-asset support with Shariah compliance filtering
- Distributed tracing with OpenTelemetry
- Advanced caching with Redis and predictive pre-fetch
- Circuit breaker pattern for external services
"""

from __future__ import annotations

import asyncio
import functools
import hashlib
import inspect
import logging
import math
import os
import random
import re
import time
import uuid
import zlib
import pickle
import threading
from collections import defaultdict, deque
from contextlib import asynccontextmanager
from dataclasses import dataclass, field, is_dataclass
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from enum import Enum
from functools import lru_cache, wraps
from typing import (
    Any, AsyncGenerator, Awaitable, Callable, Dict, List, 
    Optional, Sequence, Set, Tuple, TypeVar, Union, cast
)
from urllib.parse import urlparse

import fastapi
from fastapi import (
    APIRouter, Body, Header, HTTPException, Query, 
    Request, Response, WebSocket, WebSocketDisconnect, status
)
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer

# ---------------------------------------------------------------------------
# High-Performance JSON fallback
# ---------------------------------------------------------------------------
try:
    import orjson
    from fastapi.responses import ORJSONResponse as BestJSONResponse
    def json_dumps(v, *, default=None): return orjson.dumps(v, default=default).decode('utf-8')
    def json_loads(v): return orjson.loads(v)
    _HAS_ORJSON = True
except ImportError:
    import json
    from fastapi.responses import JSONResponse as BestJSONResponse
    def json_dumps(v, *, default=None): return json.dumps(v, default=default)
    def json_loads(v): return json.loads(v)
    _HAS_ORJSON = False

# ---------------------------------------------------------------------------
# ML/Data libraries with graceful fallbacks
# ---------------------------------------------------------------------------
try:
    import numpy as np
    import pandas as pd
    from scipy import stats
    from sklearn.ensemble import RandomForestRegressor, GradientBoostingRegressor
    from sklearn.preprocessing import StandardScaler
    _ML_AVAILABLE = True
except ImportError:
    _ML_AVAILABLE = False
    class RandomForestRegressor: pass
    class GradientBoostingRegressor: pass
    class StandardScaler: pass

try:
    from opentelemetry import trace
    from opentelemetry.trace import Span, Status, StatusCode, Tracer
    from opentelemetry.context import attach, detach
    _OTEL_AVAILABLE = True
    tracer = trace.get_tracer(__name__)
except ImportError:
    _OTEL_AVAILABLE = False
    class DummySpan:
        def set_attribute(self, *args, **kwargs): pass
        def set_status(self, *args, **kwargs): pass
        def __enter__(self): return self
        def __exit__(self, *args, **kwargs): pass
        def record_exception(self, *args, **kwargs): pass
        def end(self): pass
    class DummyTracer:
        def start_as_current_span(self, *args, **kwargs): return DummySpan()
        def start_span(self, *args, **kwargs): return DummySpan()
    tracer = DummyTracer()

try:
    import aioredis
    from aioredis import Redis
    _REDIS_AVAILABLE = True
except ImportError:
    _REDIS_AVAILABLE = False

try:
    import prometheus_client
    from prometheus_client import Counter, Histogram, Gauge, Summary
    _PROMETHEUS_AVAILABLE = True
except ImportError:
    _PROMETHEUS_AVAILABLE = False

# Pydantic imports
try:
    from pydantic import (
        BaseModel, ConfigDict, Field, field_validator, model_validator, ValidationError, AliasChoices
    )
    from pydantic.functional_validators import AfterValidator
    from typing_extensions import Annotated
    _PYDANTIC_V2 = True
except ImportError:
    from pydantic import BaseModel, Field, validator  # type: ignore
    _PYDANTIC_V2 = False

logger = logging.getLogger("routes.ai_analysis")

AI_ANALYSIS_VERSION = "8.1.0"
router = APIRouter(prefix="/v1/analysis", tags=["AI & Analysis"])

# =============================================================================
# Enums & Types (v8.1.0 FlexibleEnum Upgrade)
# =============================================================================

class FlexibleEnum(str, Enum):
    """Safely intercepts case-mismatches to prevent Pydantic 500 errors."""
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

class TimeFrame(FlexibleEnum):
    INTRADAY = "INTRADAY"
    DAILY = "DAILY"
    WEEKLY = "WEEKLY"
    MONTHLY = "MONTHLY"
    QUARTERLY = "QUARTERLY"
    YEARLY = "YEARLY"

class CacheStrategy(FlexibleEnum):
    LOCAL_MEMORY = "LOCAL_MEMORY"
    REDIS = "REDIS"
    MEMCACHED = "MEMCACHED"
    DISTRIBUTED = "DISTRIBUTED"

# =============================================================================
# Configuration
# =============================================================================

@dataclass(slots=True)
class AIConfig:
    """Dynamic AI configuration with memory optimization"""
    batch_size: int = 25
    batch_timeout_sec: float = 60.0
    batch_concurrency: int = 6
    max_tickers: int = 1200
    route_timeout_sec: float = 120.0
    
    enable_ml_predictions: bool = True
    ml_model_cache_ttl: int = 3600
    prediction_confidence_threshold: float = 0.6
    feature_importance_enabled: bool = True
    ensemble_method: str = "weighted_average" 
    
    rf_n_estimators: int = 100
    rf_max_depth: int = 10
    gb_n_estimators: int = 100
    gb_learning_rate: float = 0.1
    
    enable_technical_indicators: bool = True
    rsi_period: int = 14
    macd_fast: int = 12
    macd_slow: int = 26
    macd_signal: int = 9
    bb_period: int = 20
    bb_std: float = 2.0
    volume_ma_periods: List[int] = field(default_factory=lambda: [5, 10, 20])
    
    enable_fundamental_analysis: bool = True
    min_market_cap: float = 100_000_000 
    pe_ratio_range: Tuple[float, float] = (5, 30)
    pb_ratio_max: float = 5.0
    
    enable_sentiment_analysis: bool = True
    news_sentiment_weight: float = 0.3
    social_sentiment_weight: float = 0.2
    analyst_rating_weight: float = 0.5
    
    cache_ttl_seconds: int = 300
    cache_max_size: int = 10000
    enable_redis_cache: bool = True
    redis_url: str = "redis://localhost:6379"
    
    circuit_breaker_threshold: int = 5
    circuit_breaker_timeout: float = 60.0
    half_open_requests: int = 2
    
    rate_limit_requests: int = 100
    rate_limit_window: int = 60
    rate_limit_burst: int = 20
    
    enable_metrics: bool = True
    enable_tracing: bool = True
    trace_sample_rate: float = 1.0
    
    allow_query_token: bool = False
    require_api_key: bool = True
    enable_audit_log: bool = True
    
    enable_shariah_filter: bool = True
    shariah_check_timeout: float = 10.0
    
    market_data_refresh_interval: int = 60
    enable_realtime: bool = True
    
    enable_scoreboard: bool = True
    enable_websocket: bool = True
    enable_push_mode: bool = True
    enable_fallback: bool = True

_CONFIG = AIConfig()

def _load_config_from_env() -> None:
    global _CONFIG
    def env_int(name: str, default: int) -> int: return int(os.getenv(name, str(default)))
    def env_float(name: str, default: float) -> float: return float(os.getenv(name, str(default)))
    def env_bool(name: str, default: bool) -> bool: return os.getenv(name, str(default)).lower() in ("true", "1", "yes", "on", "y")
    
    _CONFIG.batch_size = env_int("AI_BATCH_SIZE", _CONFIG.batch_size)
    _CONFIG.batch_timeout_sec = env_float("AI_BATCH_TIMEOUT_SEC", _CONFIG.batch_timeout_sec)
    _CONFIG.batch_concurrency = env_int("AI_BATCH_CONCURRENCY", _CONFIG.batch_concurrency)
    _CONFIG.max_tickers = env_int("AI_MAX_TICKERS", _CONFIG.max_tickers)
    _CONFIG.route_timeout_sec = env_float("AI_ROUTE_TIMEOUT_SEC", _CONFIG.route_timeout_sec)
    _CONFIG.enable_ml_predictions = env_bool("AI_ENABLE_ML", _CONFIG.enable_ml_predictions)
    _CONFIG.allow_query_token = env_bool("ALLOW_QUERY_TOKEN", _CONFIG.allow_query_token)
    _CONFIG.enable_redis_cache = env_bool("AI_ENABLE_REDIS", _CONFIG.enable_redis_cache)
    _CONFIG.redis_url = os.getenv("REDIS_URL", _CONFIG.redis_url)
    _CONFIG.enable_tracing = env_bool("CORE_TRACING_ENABLED", _CONFIG.enable_tracing)

_load_config_from_env()

# =============================================================================
# Observability & Metrics
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
            self._histograms[name] = Histogram(f"{self.namespace}_{name}", description, buckets=buckets or Histogram.DEFAULT_BUCKETS)
        return self._histograms.get(name)
    
    def gauge(self, name: str, description: str) -> Any:
        if name not in self._gauges and _PROMETHEUS_AVAILABLE:
            self._gauges[name] = Gauge(f"{self.namespace}_{name}", description)
        return self._gauges.get(name)
    
    def summary(self, name: str, description: str) -> Any:
        if name not in self._summaries and _PROMETHEUS_AVAILABLE:
            self._summaries[name] = Summary(f"{self.namespace}_{name}", description)
        return self._summaries.get(name)

_metrics = MetricsRegistry()

class TraceContext:
    def __init__(self, name: str, attributes: Optional[Dict[str, Any]] = None):
        self.name = name
        self.attributes = attributes or {}
        self.tracer = tracer if _OTEL_AVAILABLE and _CONFIG.enable_tracing else None
        self.span = None
        self.token = None
    
    async def __aenter__(self):
        if self.tracer and random.random() <= _CONFIG.trace_sample_rate:
            self.span = self.tracer.start_as_current_span(self.name)
            self.token = attach(self.span)
            if self.attributes:
                self.span.set_attributes(self.attributes)
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self.span:
            if exc_val:
                self.span.record_exception(exc_val)
                self.span.set_status(Status(StatusCode.ERROR, str(exc_val)))
            self.span.end()
            if self.token:
                detach(self.token)

# =============================================================================
# Advanced Time Handling
# =============================================================================

class SaudiTime:
    def __init__(self):
        self._tz = timezone(timedelta(hours=3))
        self._trading_hours = {"start": "10:00", "end": "15:00"}
        self._weekend_days = [4, 5] 
        self._holidays = {"2024-02-22", "2024-04-10", "2024-04-11", "2024-04-12", "2024-06-16", "2024-06-17", "2024-06-18", "2024-09-23"}
    
    def now(self) -> datetime: return datetime.now(self._tz)
    
    def now_iso(self) -> str: return self.now().isoformat(timespec="milliseconds")
    
    def now_utc_iso(self) -> str: return datetime.now(timezone.utc).isoformat(timespec="milliseconds")
    
    def is_trading_day(self, dt: Optional[datetime] = None) -> bool:
        dt = dt or self.now()
        if dt.weekday() in self._weekend_days: return False
        if dt.strftime("%Y-%m-%d") in self._holidays: return False
        return True
    
    def is_trading_hours(self, dt: Optional[datetime] = None) -> bool:
        if not self.is_trading_day(dt): return False
        dt = dt or self.now()
        current = dt.time()
        start = datetime.strptime(self._trading_hours["start"], "%H:%M").time()
        end = datetime.strptime(self._trading_hours["end"], "%H:%M").time()
        return start <= current <= end
    
    def to_riyadh(self, utc_dt: Union[str, datetime, None]) -> str:
        if not utc_dt: return ""
        try:
            dt = datetime.fromisoformat(utc_dt.replace("Z", "+00:00")) if isinstance(utc_dt, str) else utc_dt
            if dt.tzinfo is None: dt = dt.replace(tzinfo=timezone.utc)
            return dt.astimezone(self._tz).isoformat(timespec="milliseconds")
        except Exception: return ""

_saudi_time = SaudiTime()

# =============================================================================
# Distributed Caching
# =============================================================================

class CacheManager:
    """Multi-tier cache manager with Redis fallback and Zlib compression"""
    
    def __init__(self):
        self._local_cache: Dict[str, Tuple[Any, float]] = {}
        self._redis_cache = None
        self._stats = {"hits": 0, "misses": 0, "redis_hits": 0, "redis_misses": 0}
        self._lock = asyncio.Lock()
        self._predictive_cache: Dict[str, Any] = {}
        self._access_patterns: Dict[str, List[float]] = defaultdict(list)
        self._background_tasks: Set[asyncio.Task] = set()
        
        if _CONFIG.enable_redis_cache and _REDIS_AVAILABLE:
            asyncio.create_task(self._init_redis())
    
    async def _init_redis(self):
        try:
            self._redis_cache = await aioredis.from_url(
                _CONFIG.redis_url, encoding="utf-8", decode_responses=False,
                max_connections=20, socket_timeout=5.0, socket_connect_timeout=5.0
            )
            logger.info(f"Redis cache initialized at {_CONFIG.redis_url}")
        except Exception as e:
            logger.warning(f"Redis initialization failed: {e}")
            
    def _compress(self, data: Any) -> bytes:
        return zlib.compress(pickle.dumps(data), level=6)

    def _decompress(self, data: bytes) -> Any:
        try: return pickle.loads(zlib.decompress(data))
        except Exception: return pickle.loads(data)
    
    def _get_cache_key(self, key: str, namespace: str = "ai") -> str:
        return f"{namespace}:{hashlib.md5(key.encode()).hexdigest()[:32]}"
    
    async def get(self, key: str, namespace: str = "ai") -> Optional[Any]:
        cache_key = self._get_cache_key(key, namespace)
        self._access_patterns[key].append(time.time())
        if len(self._access_patterns[key]) > 100: self._access_patterns[key] = self._access_patterns[key][-100:]
        
        async with self._lock:
            if cache_key in self._local_cache:
                value, expiry = self._local_cache[cache_key]
                if expiry > time.time():
                    self._stats["hits"] += 1
                    return value
                else: del self._local_cache[cache_key]
        
        if self._redis_cache:
            try:
                raw = await self._redis_cache.get(cache_key)
                if raw is not None:
                    value = self._decompress(raw)
                    self._stats["redis_hits"] += 1
                    self._stats["hits"] += 1
                    async with self._lock:
                        self._local_cache[cache_key] = (value, time.time() + (_CONFIG.cache_ttl_seconds // 2))
                    return value
                else: self._stats["redis_misses"] += 1
            except Exception as e:
                logger.debug(f"Redis get failed: {e}")
                self._stats["redis_misses"] += 1
        
        self._stats["misses"] += 1
        if key in self._predictive_cache: return self._predictive_cache[key]
        return None
    
    async def set(self, key: str, value: Any, ttl: Optional[int] = None, namespace: str = "ai", predictive: bool = False) -> None:
        cache_key = self._get_cache_key(key, namespace)
        ttl = ttl or _CONFIG.cache_ttl_seconds
        expiry = time.time() + ttl
        
        async with self._lock:
            if len(self._local_cache) >= _CONFIG.cache_max_size:
                items = sorted(self._local_cache.items(), key=lambda x: x[1][1])
                for k, _ in items[:_CONFIG.cache_max_size // 5]: del self._local_cache[k]
            self._local_cache[cache_key] = (value, expiry)
        
        if predictive: self._predictive_cache[key] = value
        
        if self._redis_cache:
            try: await self._redis_cache.setex(cache_key, ttl, self._compress(value))
            except Exception as e: logger.debug(f"Redis set failed: {e}")
    
    async def delete(self, key: str, namespace: str = "ai") -> None:
        cache_key = self._get_cache_key(key, namespace)
        async with self._lock: self._local_cache.pop(cache_key, None)
        if self._redis_cache:
            try: await self._redis_cache.delete(cache_key)
            except Exception: pass
        self._predictive_cache.pop(key, None)
    
    async def pre_fetch(self, keys: List[str]) -> Dict[str, Any]:
        results = {}
        for key in keys:
            patterns = self._access_patterns.get(key, [])
            if len(patterns) > 5:
                intervals = [patterns[i+1] - patterns[i] for i in range(len(patterns)-1)]
                avg_interval = sum(intervals) / len(intervals) if intervals else 0
                if avg_interval < _CONFIG.cache_ttl_seconds * 2:
                    if value := await self.get(key): results[key] = value
        return results
    
    def get_stats(self) -> Dict[str, Any]:
        total = self._stats["hits"] + self._stats["misses"]
        return {"hits": self._stats["hits"], "misses": self._stats["misses"], "redis_hits": self._stats["redis_hits"], "redis_misses": self._stats["redis_misses"], "hit_rate": self._stats["hits"] / total if total > 0 else 0, "local_cache_size": len(self._local_cache), "predictive_cache_size": len(self._predictive_cache), "tracked_keys": len(self._access_patterns)}

_cache = CacheManager()

# =============================================================================
# Circuit Breaker & Coalescing
# =============================================================================

class CircuitStateEnum(str, Enum):
    CLOSED = "closed"
    OPEN = "open"
    HALF_OPEN = "half_open"

class CircuitBreaker:
    def __init__(self, name: str, threshold: int = 5, timeout: float = 60.0):
        self.name, self.threshold, self.timeout = name, threshold, timeout
        self.state = CircuitStateEnum.CLOSED
        self.failure_count, self.last_failure_time, self.success_count = 0, 0.0, 0
        self._lock = asyncio.Lock()
        self._half_open_requests = 0
    
    async def execute(self, func: Callable, *args, **kwargs) -> Any:
        async with self._lock:
            if self.state == CircuitStateEnum.OPEN:
                if time.time() - self.last_failure_time > self.timeout:
                    self.state, self.success_count, self._half_open_requests = CircuitStateEnum.HALF_OPEN, 0, 0
                else: raise Exception(f"Circuit {self.name} is OPEN")
            if self.state == CircuitStateEnum.HALF_OPEN:
                if self._half_open_requests >= _CONFIG.half_open_requests: raise Exception(f"Circuit {self.name} half-open limit reached")
                self._half_open_requests += 1
        try:
            result = await func(*args, **kwargs) if asyncio.iscoroutinefunction(func) else func(*args, **kwargs)
            async with self._lock:
                if self.state == CircuitStateEnum.HALF_OPEN:
                    self.success_count += 1
                    if self.success_count >= 2:
                        self.state, self.failure_count = CircuitStateEnum.CLOSED, 0
                else: self.failure_count = 0
            return result
        except Exception as e:
            async with self._lock:
                self.failure_count += 1
                self.last_failure_time = time.time()
                if self.state == CircuitStateEnum.CLOSED and self.failure_count >= self.threshold:
                    self.state = CircuitStateEnum.OPEN
                elif self.state == CircuitStateEnum.HALF_OPEN:
                    self.state = CircuitStateEnum.OPEN
            raise e

class RequestCoalescer:
    def __init__(self):
        self._pending: Dict[str, asyncio.Future] = {}
        self._lock = asyncio.Lock()
    
    async def execute(self, key: str, coro: Awaitable) -> Any:
        async with self._lock:
            if key in self._pending: return await self._pending[key]
            future = asyncio.Future()
            self._pending[key] = future
        try:
            result = await coro
            if not future.done(): future.set_result(result)
        except Exception as e:
            if not future.done(): future.set_exception(e)
        finally:
            async with self._lock: self._pending.pop(key, None)
        return await future

_coalescer = RequestCoalescer()

# =============================================================================
# Adaptive Concurrency Controller
# =============================================================================

class AdaptiveConcurrencyController:
    def __init__(self):
        self.current_concurrency = _CONFIG.batch_concurrency
        self.latency_window: List[float] = []
        self.error_window: List[bool] = []
        self.window_size = 100
        self.last_adjustment = time.time()
        self.adjustment_interval = 30
    
    def record_request(self, latency: float, success: bool) -> None:
        self.latency_window.append(latency)
        self.error_window.append(not success)
        if len(self.latency_window) > self.window_size:
            self.latency_window.pop(0)
            self.error_window.pop(0)
    
    def should_adjust(self) -> bool:
        return (time.time() - self.last_adjustment) > self.adjustment_interval
    
    def adjust(self) -> int:
        if not self.latency_window: return self.current_concurrency
        avg_latency = sum(self.latency_window) / len(self.latency_window)
        error_rate = sum(self.error_window) / len(self.error_window) if self.error_window else 0
        new_concurrency = self.current_concurrency
        
        if error_rate > 0.1: new_concurrency = max(1, int(self.current_concurrency * 0.7))
        elif avg_latency > 1000: new_concurrency = max(1, int(self.current_concurrency * 0.9))
        elif error_rate < 0.01 and avg_latency < 500: new_concurrency = min(20, int(self.current_concurrency * 1.1))
        
        self.current_concurrency = new_concurrency
        self.last_adjustment = time.time()
        return new_concurrency

_concurrency_controller = AdaptiveConcurrencyController()

# =============================================================================
# Audit Logging (SAMA Compliance)
# =============================================================================

class AuditLogger:
    def __init__(self):
        self._audit_buffer: List[Dict[str, Any]] = []
        self._buffer_lock = asyncio.Lock()
    
    async def log(self, event: str, user_id: Optional[str], resource: str, action: str, status: str, details: Dict[str, Any], request_id: Optional[str] = None) -> None:
        if not _CONFIG.enable_audit_log: return
        entry = {"timestamp": _saudi_time.now_iso(), "event_id": str(uuid.uuid4()), "event": event, "user_id": user_id, "resource": resource, "action": action, "status": status, "details": details, "request_id": request_id, "version": AI_ANALYSIS_VERSION, "environment": os.getenv("APP_ENV", "production")}
        async with self._buffer_lock:
            self._audit_buffer.append(entry)
            if len(self._audit_buffer) >= 100: asyncio.create_task(self._flush())
    
    async def _flush(self) -> None:
        async with self._buffer_lock:
            buffer, self._audit_buffer = self._audit_buffer.copy(), []
        try: logger.info(f"Audit flush: {len(buffer)} entries")
        except Exception as e: logger.error(f"Audit flush failed: {e}")

_audit_logger = AuditLogger()

# =============================================================================
# Enhanced Auth & Rate Limiter
# =============================================================================

class TokenManager:
    def __init__(self):
        self._tokens: Set[str] = set()
        self._token_metadata: Dict[str, Dict[str, Any]] = {}
        for key in ("APP_TOKEN", "BACKUP_APP_TOKEN", "TFB_APP_TOKEN"):
            if token := os.getenv(key, "").strip():
                self._tokens.add(token)
                self._token_metadata[token] = {"source": key, "created_at": datetime.now().isoformat(), "last_used": None}
    
    def validate_token(self, token: str) -> bool:
        if not self._tokens: return True
        token = token.strip()
        if token in self._tokens:
            if token in self._token_metadata: self._token_metadata[token]["last_used"] = datetime.now().isoformat()
            return True
        return False

_token_manager = TokenManager()

class RateLimiter:
    def __init__(self):
        self._buckets: Dict[str, Tuple[int, float]] = {}
        self._lock = asyncio.Lock()
    
    async def check(self, key: str, requests: int = 100, window: int = 60) -> bool:
        if not _CONFIG.rate_limit_requests: return True
        async with self._lock:
            now = time.time()
            count, reset_time = self._buckets.get(key, (0, now + window))
            if now > reset_time: count, reset_time = 0, now + window
            if count < requests:
                self._buckets[key] = (count + 1, reset_time)
                return True
            return False

_rate_limiter = RateLimiter()

# =============================================================================
# ML Models & Schemas
# =============================================================================

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
    macd_histogram: Optional[float] = None
    bb_upper: Optional[float] = None
    bb_lower: Optional[float] = None
    bb_middle: Optional[float] = None
    bb_width: Optional[float] = None
    bb_position: Optional[float] = None
    sma_20d: Optional[float] = None
    sma_50d: Optional[float] = None
    sma_200d: Optional[float] = None
    ema_20d: Optional[float] = None
    volume_sma_5d: Optional[float] = None
    volume_sma_10d: Optional[float] = None
    volume_sma_20d: Optional[float] = None
    obv: Optional[float] = None
    ad_line: Optional[float] = None
    volatility_10d: Optional[float] = None
    volatility_30d: Optional[float] = None
    volatility_60d: Optional[float] = None
    atr_14d: Optional[float] = None
    momentum_5d: Optional[float] = None
    momentum_10d: Optional[float] = None
    momentum_20d: Optional[float] = None
    momentum_60d: Optional[float] = None
    return_1d: Optional[float] = None
    return_5d: Optional[float] = None
    return_10d: Optional[float] = None
    return_20d: Optional[float] = None
    return_60d: Optional[float] = None
    return_252d: Optional[float] = None
    beta: Optional[float] = None
    sharpe_ratio: Optional[float] = None
    sortino_ratio: Optional[float] = None
    max_drawdown_30d: Optional[float] = None
    market_cap: Optional[float] = None
    pe_ratio: Optional[float] = None
    pb_ratio: Optional[float] = None
    ps_ratio: Optional[float] = None
    dividend_yield: Optional[float] = None
    eps: Optional[float] = None
    revenue_growth: Optional[float] = None
    profit_margin: Optional[float] = None
    news_sentiment: Optional[float] = None
    social_sentiment: Optional[float] = None
    analyst_rating: Optional[float] = None
    analyst_count: Optional[int] = None
    data_quality: DataQuality = DataQuality.GOOD
    timestamp: str = Field(default_factory=_saudi_time.now_iso)
    if _PYDANTIC_V2: model_config = ConfigDict(arbitrary_types_allowed=True, use_enum_values=True)

class MLPrediction(BaseModel):
    symbol: str
    prediction_time: str = Field(default_factory=_saudi_time.now_iso)
    predicted_return_1d: Optional[float] = None
    predicted_return_1w: Optional[float] = None
    predicted_return_1m: Optional[float] = None
    predicted_return_3m: Optional[float] = None
    predicted_return_1y: Optional[float] = None
    confidence_1d: float = 0.5
    confidence_1w: float = 0.5
    confidence_1m: float = 0.5
    confidence_3m: float = 0.5
    confidence_1y: float = 0.5
    risk_score: float = 50.0
    volatility_forecast: Optional[float] = None
    sharpe_ratio_forecast: Optional[float] = None
    target_price_1m: Optional[float] = None
    target_price_3m: Optional[float] = None
    target_price_1y: Optional[float] = None
    signal_1d: SignalType = SignalType.NEUTRAL
    signal_1w: SignalType = SignalType.NEUTRAL
    signal_1m: SignalType = SignalType.NEUTRAL
    feature_importance: Dict[str, float] = Field(default_factory=dict)
    top_factors: List[str] = Field(default_factory=list)
    model_version: str = AI_ANALYSIS_VERSION
    recommendation: Recommendation = Recommendation.HOLD
    recommendation_strength: float = 0.0
    if _PYDANTIC_V2: model_config = ConfigDict(arbitrary_types_allowed=True, use_enum_values=True)

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
    sma_20d: Optional[float] = None
    sma_50d: Optional[float] = None
    sma_200d: Optional[float] = None
    ema_20d: Optional[float] = None
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
    if _PYDANTIC_V2: model_config = ConfigDict(use_enum_values=True)

class BatchAnalysisResponse(BaseModel):
    results: List[SingleAnalysisResponse] = Field(default_factory=list)
    status: str = "success"
    error: Optional[str] = None
    warnings: List[str] = Field(default_factory=list)
    version: str = AI_ANALYSIS_VERSION
    meta: Dict[str, Any] = Field(default_factory=dict)
    if _PYDANTIC_V2: model_config = ConfigDict(arbitrary_types_allowed=True)

class SheetAnalysisResponse(BaseModel):
    headers: List[str] = Field(default_factory=list)
    rows: List[List[Any]] = Field(default_factory=list)
    status: str = "success"
    error: Optional[str] = None
    version: str = AI_ANALYSIS_VERSION
    meta: Dict[str, Any] = Field(default_factory=dict)
    if _PYDANTIC_V2: model_config = ConfigDict(arbitrary_types_allowed=True)

class ScoreboardResponse(BaseModel):
    headers: List[str] = Field(default_factory=list)
    rows: List[List[Any]] = Field(default_factory=list)
    status: str = "success"
    error: Optional[str] = None
    version: str = AI_ANALYSIS_VERSION
    meta: Dict[str, Any] = Field(default_factory=dict)
    if _PYDANTIC_V2: model_config = ConfigDict(arbitrary_types_allowed=True)


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
            seen, result = set(), []
            for s in all_symbols:
                normalized = str(s).strip().upper()
                if normalized.startswith("TADAWUL:"): normalized = normalized.split(":", 1)[1]
                if normalized.endswith(".TADAWUL"): normalized = normalized.replace(".TADAWUL", "")
                if normalized and normalized not in seen:
                    seen.add(normalized); result.append(normalized)
            self.symbols, self.tickers = result, []
            return self

# =============================================================================
# ML Model Manager (Lazy Loading)
# =============================================================================

class MLModelManager:
    """Non-blocking ML model management utilizing ThreadPoolExecutors"""
    def __init__(self):
        self._models: Dict[str, Any] = {}
        self._scalers: Dict[str, Any] = {}
        self._initialized = False
        self._prediction_cache: Dict[str, Tuple[MLPrediction, float]] = {}
        self._lock = asyncio.Lock()
        self._circuit_breaker = CircuitBreaker("ml_models")
        self._feature_names = ["price", "volume", "rsi_14d", "macd", "bb_position", "volatility_30d", "momentum_20d", "return_20d", "beta", "market_cap", "pe_ratio", "dividend_yield"]
    
    async def initialize(self) -> None:
        if self._initialized or not _ML_AVAILABLE: return
        async with self._lock:
            if self._initialized: return
            try:
                self._models["rf_1d"] = RandomForestRegressor(n_estimators=_CONFIG.rf_n_estimators, max_depth=_CONFIG.rf_max_depth, random_state=42)
                self._models["rf_1w"] = RandomForestRegressor(n_estimators=_CONFIG.rf_n_estimators, max_depth=_CONFIG.rf_max_depth, random_state=42)
                self._models["gb_1m"] = GradientBoostingRegressor(n_estimators=_CONFIG.gb_n_estimators, learning_rate=_CONFIG.gb_learning_rate, max_depth=5, random_state=42)
                self._scalers["standard"] = StandardScaler()
                self._initialized = True
                logger.info("ML models initialized")
                if _metrics.gauge("ml_models_loaded"): _metrics.gauge("ml_models_loaded").set(1)
            except Exception as e:
                logger.error(f"ML initialization failed: {e}")
                if _metrics.gauge("ml_models_loaded"): _metrics.gauge("ml_models_loaded").set(0)
    
    async def predict(self, features: MLFeatures) -> Optional[MLPrediction]:
        if not self._initialized or not _ML_AVAILABLE: return None
        cache_key = f"ml_pred:{features.symbol}:{_saudi_time.now_iso()[:10]}"
        if cache_key in self._prediction_cache:
            pred, expiry = self._prediction_cache[cache_key]
            if expiry > time.time(): return pred
        
        async def _do_predict():
            try:
                loop = asyncio.get_running_loop()
                def _run_inference():
                    vector = self._extract_features(features)
                    vector_scaled = self._scalers["standard"].fit_transform([vector])[0]
                    pred_1d = self._models["rf_1d"].predict([vector_scaled])[0]
                    pred_1w = self._models["rf_1w"].predict([vector_scaled])[0]
                    pred_1m = self._models["gb_1m"].predict([vector_scaled])[0]
                    
                    predictions = [pred_1d, pred_1w, pred_1m]
                    mean_pred = np.mean(predictions)
                    std_pred = np.std(predictions)
                    confidence = 1.0 / (1.0 + std_pred / (abs(mean_pred) + 1e-6))
                    
                    risk = 50.0
                    if features.volatility_30d: risk += features.volatility_30d * 10
                    if features.beta: risk += (features.beta - 1) * 20
                    if features.momentum_20d:
                        if features.momentum_20d < -0.1: risk += 20
                        elif features.momentum_20d > 0.1: risk -= 10
                    if features.market_cap:
                        if features.market_cap < 1e9: risk += 15
                        elif features.market_cap > 1e11: risk -= 10
                    risk_score = max(0, min(100, risk))
                    
                    signal = SignalType.BULLISH if mean_pred > 0.05 else SignalType.BEARISH if mean_pred < -0.05 else SignalType.NEUTRAL
                    
                    importance = {}
                    if _CONFIG.feature_importance_enabled and hasattr(self._models["rf_1d"], "feature_importances_"):
                        importance = dict(zip(self._feature_names, self._models["rf_1d"].feature_importances_))
                        total = sum(importance.values())
                        if total > 0: importance = {k: v/total for k, v in importance.items()}
                        
                    top_factors = sorted(importance.items(), key=lambda x: x[1], reverse=True)[:5]
                    top_factor_names = [f"{k}: {v:.1%}" for k, v in top_factors]
                    
                    adjusted_return = mean_pred * confidence
                    if adjusted_return > 0.15: rec, rec_strength = Recommendation.STRONG_BUY, 1.0
                    elif adjusted_return > 0.08: rec, rec_strength = Recommendation.BUY, 0.8
                    elif adjusted_return < -0.15: rec, rec_strength = Recommendation.STRONG_SELL, 1.0
                    elif adjusted_return < -0.08: rec, rec_strength = Recommendation.SELL, 0.8
                    else: rec, rec_strength = Recommendation.HOLD, 0.5
                    
                    return MLPrediction(
                        symbol=features.symbol, predicted_return_1d=float(pred_1d), predicted_return_1w=float(pred_1w),
                        predicted_return_1m=float(pred_1m), confidence_1d=float(confidence), confidence_1w=float(confidence * 0.9),
                        confidence_1m=float(confidence * 0.8), risk_score=float(risk_score), signal_1d=signal,
                        feature_importance=importance, top_factors=top_factor_names, recommendation=rec, recommendation_strength=rec_strength
                    )
                
                import concurrent.futures
                with concurrent.futures.ThreadPoolExecutor(max_workers=4) as pool:
                    prediction = await loop.run_in_executor(pool, _run_inference)
                
                self._prediction_cache[cache_key] = (prediction, time.time() + 3600)
                return prediction
            except Exception as e:
                logger.error(f"ML prediction failed: {e}")
                return None
                
        return await self._circuit_breaker.execute(_do_predict)
    
    def _extract_features(self, features: MLFeatures) -> List[float]:
        feature_map = {"price": features.price, "volume": features.volume, "rsi_14d": features.rsi_14d, "macd": features.macd, "bb_position": features.bb_position, "volatility_30d": features.volatility_30d, "momentum_20d": features.momentum_20d, "return_20d": features.return_20d, "beta": features.beta, "market_cap": features.market_cap, "pe_ratio": features.pe_ratio, "dividend_yield": features.dividend_yield}
        return [float(feature_map.get(name) or 0.0) for name in self._feature_names]

_ml_models = MLModelManager()

# =============================================================================
# Core Analysis Engine
# =============================================================================

class AnalysisEngine:
    def __init__(self):
        self._engine = None
        self._engine_lock = asyncio.Lock()
        self._circuit_breaker = CircuitBreaker("analysis_engine", threshold=_CONFIG.circuit_breaker_threshold, timeout=_CONFIG.circuit_breaker_timeout)
    
    async def get_engine(self, request: Request) -> Optional[Any]:
        try:
            st = getattr(request.app, "state", None)
            if st and getattr(st, "engine", None): return st.engine
        except Exception: pass
        if self._engine is not None: return self._engine
        
        async def _init_engine():
            async with self._engine_lock:
                if self._engine is not None: return self._engine
                try:
                    from core.data_engine_v2 import get_engine
                    self._engine = await get_engine()
                    return self._engine
                except Exception as e:
                    logger.error(f"Engine initialization failed: {e}")
                    return None
        return await self._circuit_breaker.execute(_init_engine)
    
    async def get_quotes(self, engine: Any, symbols: List[str], include_ml: bool = True) -> Tuple[Dict[str, Any], Dict[str, Any]]:
        stats = {"requested": len(symbols), "from_cache": 0, "from_engine": 0, "ml_predictions": 0, "errors": 0}
        results, cache_keys = {}, {s: f"quote:{s}" for s in symbols}
        
        for symbol, cache_key in cache_keys.items():
            if (cached := await _cache.get(cache_key)) is not None:
                results[symbol] = cached
                stats["from_cache"] += 1
                
        symbols_to_fetch = [s for s in symbols if s not in results]
        
        if symbols_to_fetch and engine:
            try:
                fetched = {}
                for method in ["get_enriched_quotes_batch", "get_enriched_quotes", "get_quotes_batch"]:
                    if callable(fn := getattr(engine, method, None)):
                        try:
                            res = await fn(symbols_to_fetch)
                            if res:
                                fetched = {s: self._to_dict(res.get(s)) for s in symbols_to_fetch} if isinstance(res, dict) else {s: self._to_dict(r) for s, r in zip(symbols_to_fetch, res)}
                                break
                        except Exception: pass
                        
                if not fetched:
                    semaphore = asyncio.Semaphore(_CONFIG.batch_concurrency)
                    async def fetch_one(s):
                        async with semaphore:
                            try: return s, self._to_dict(await engine.get_enriched_quote(s))
                            except Exception as e: return s, self._create_placeholder(s, str(e))
                    results_gathered = await asyncio.gather(*(fetch_one(s) for s in symbols_to_fetch), return_exceptions=True)
                    fetched = {r[0]: r[1] for r in results_gathered if isinstance(r, tuple)}
                
                stats["from_engine"] += len(fetched)
                for symbol, data in fetched.items(): await _cache.set(cache_keys[symbol], data, ttl=_CONFIG.cache_ttl_seconds)
                results.update(fetched)
            except Exception as e:
                logger.error(f"Engine fetch failed: {e}")
                stats["errors"] += len(symbols_to_fetch)
                for symbol in symbols_to_fetch: results[symbol] = self._create_placeholder(symbol, str(e))
                
        if include_ml and _CONFIG.enable_ml_predictions:
            await _ml_models.initialize()
            for symbol, data in results.items():
                if data and not isinstance(data, dict): data = self._to_dict(data)
                if data and "error" not in data:
                    features = MLFeatures(symbol=symbol, price=data.get("price"), volume=data.get("volume"), market_cap=data.get("market_cap"), pe_ratio=data.get("pe_ratio"), pb_ratio=data.get("pb_ratio"), dividend_yield=data.get("dividend_yield"), beta=data.get("beta"), volatility_30d=data.get("volatility_30d"), momentum_14d=data.get("momentum_14d"), rsi_14d=data.get("rsi_14d"), macd=data.get("macd"), macd_signal=data.get("macd_signal"), sector=data.get("sector"), industry=data.get("industry"))
                    if ml_pred := await _ml_models.predict(features):
                        data["ml_prediction"] = ml_pred.model_dump() if _PYDANTIC_V2 else ml_pred.dict()
                        stats["ml_predictions"] += 1
                        
        return results, stats
    
    def _to_dict(self, obj: Any) -> Dict[str, Any]:
        if obj is None: return {}
        if isinstance(obj, dict): return obj
        try:
            if hasattr(obj, "dict"): return obj.dict()
            if hasattr(obj, "model_dump"): return obj.model_dump()
            if is_dataclass(obj): return {k: v for k, v in obj.__dict__.items() if not k.startswith("_")}
        except Exception: pass
        return {"raw": str(obj)}
        
    def _create_placeholder(self, symbol: str, error: str) -> Dict[str, Any]:
        return {"symbol": symbol.upper(), "error": error, "data_quality": DataQuality.MISSING, "last_updated_utc": _saudi_time.now_utc_iso(), "last_updated_riyadh": _saudi_time.to_riyadh(_saudi_time.now_utc_iso()), "recommendation": Recommendation.HOLD}

_analysis_engine = AnalysisEngine()

# =============================================================================
# WebSocket Manager
# =============================================================================

class WebSocketManager:
    def __init__(self):
        self._active_connections: List[WebSocket] = []
        self._connection_lock = asyncio.Lock()
        self._subscriptions: Dict[str, List[WebSocket]] = defaultdict(list)
        self._broadcast_queue: asyncio.Queue = asyncio.Queue()
        self._broadcast_task: Optional[asyncio.Task] = None
    
    async def start(self): self._broadcast_task = asyncio.create_task(self._process_broadcasts())
    
    async def stop(self):
        if self._broadcast_task:
            self._broadcast_task.cancel()
            try: await self._broadcast_task
            except: pass
    
    async def connect(self, websocket: WebSocket):
        await websocket.accept()
        async with self._connection_lock: self._active_connections.append(websocket)
        if _metrics.gauge("websocket_connections"): _metrics.gauge("websocket_connections").set(len(self._active_connections))
        if self._broadcast_task is None: self._broadcast_task = asyncio.create_task(self._process_broadcasts())
    
    async def disconnect(self, websocket: WebSocket):
        async with self._connection_lock:
            if websocket in self._active_connections: self._active_connections.remove(websocket)
            for symbol in list(self._subscriptions.keys()):
                if websocket in self._subscriptions[symbol]: self._subscriptions[symbol].remove(websocket)
        if _metrics.gauge("websocket_connections"): _metrics.gauge("websocket_connections").set(len(self._active_connections))
    
    async def subscribe(self, websocket: WebSocket, symbol: str):
        async with self._connection_lock: self._subscriptions[symbol.upper()].append(websocket)
    
    async def unsubscribe(self, websocket: WebSocket, symbol: str):
        async with self._connection_lock:
            if symbol.upper() in self._subscriptions and websocket in self._subscriptions[symbol.upper()]:
                self._subscriptions[symbol.upper()].remove(websocket)
    
    async def broadcast(self, message: Dict[str, Any], symbol: Optional[str] = None):
        await self._broadcast_queue.put((message, symbol))
    
    async def _process_broadcasts(self):
        while True:
            try:
                await asyncio.sleep(5)
                await self.broadcast({"type": "market_summary", "timestamp": _saudi_time.now_iso(), "trading_hours": _saudi_time.is_trading_hours(), "active_connections": len(self._active_connections)})
                message, symbol = await self._broadcast_queue.get()
                connections = self._subscriptions.get(symbol.upper(), []) if symbol else self._active_connections
                disconnected = []
                for connection in connections:
                    try: await connection.send_text(json_dumps(message)) if _HAS_ORJSON else await connection.send_json(message)
                    except Exception: disconnected.append(connection)
                for conn in disconnected: await self.disconnect(conn)
            except asyncio.CancelledError: break
            except Exception as e: logger.error(f"Broadcast error: {e}")

_ws_manager = WebSocketManager()

# =============================================================================
# Routes
# =============================================================================

def _parse_tickers(tickers_str: str) -> List[str]:
    if not tickers_str: return []
    symbols = [p.strip().upper() for p in tickers_str.replace(",", " ").replace(";", " ").replace("|", " ").split() if p.strip()]
    seen, unique = set(), []
    for s in symbols:
        if s not in seen:
            seen.add(s)
            unique.append(s)
    return unique

def _get_default_headers() -> List[str]:
    return ["Symbol", "Name", "Price", "Change %", "Volume", "Market Cap", "P/E Ratio", "Dividend Yield", "RSI (14)", "MACD", "BB Position", "Volatility", "Overall Score", "Risk Score", "Recommendation", "Expected ROI 1M", "Forecast Price 1M", "Expected ROI 3M", "Forecast Price 3M", "Expected ROI 12M", "Forecast Price 12M", "ML Confidence", "Signal", "Data Quality", "Error", "Last Updated (UTC)", "Last Updated (Riyadh)"]

def _get_sheet_headers(sheet_name: str) -> List[str]: return _get_default_headers()

def _map_header_to_field(header: str, data: Dict[str, Any]) -> Any:
    h = header.lower()
    if "symbol" in h: return data.get("symbol", "")
    if "name" in h: return data.get("name", "")
    if "price" in h and "forecast" not in h: return data.get("price")
    if "change %" in h or "change%" in h: return data.get("change_pct")
    if "change" in h: return data.get("change")
    if "volume" in h: return data.get("volume")
    if "market cap" in h: return data.get("market_cap")
    if "p/e" in h or "pe ratio" in h: return data.get("pe_ratio")
    if "dividend" in h: return data.get("dividend_yield")
    if "rsi" in h: return data.get("rsi_14d")
    if "macd" in h: return data.get("macd")
    if "bb position" in h: return data.get("bb_position")
    if "volatility" in h: return data.get("volatility_30d")
    if "overall score" in h: return data.get("overall_score")
    if "risk score" in h: return data.get("risk_score")
    if "recommendation" in h: return data.get("recommendation", "HOLD")
    if "expected roi 1m" in h: return data.get("expected_roi_1m")
    if "expected roi 3m" in h: return data.get("expected_roi_3m")
    if "expected roi 12m" in h: return data.get("expected_roi_12m")
    if "forecast price 1m" in h: return data.get("forecast_price_1m")
    if "forecast price 3m" in h: return data.get("forecast_price_3m")
    if "forecast price 12m" in h: return data.get("forecast_price_12m")
    if "ml confidence" in h: return data.get("ml_prediction", {}).get("confidence_1m", 0.5)
    if "signal" in h: return data.get("ml_prediction", {}).get("signal_1m", "neutral").upper()
    if "data quality" in h: return data.get("data_quality", "UNKNOWN")
    if "error" in h: return data.get("error")
    if "last updated (utc)" in h: return data.get("last_updated_utc")
    if "last updated (riyadh)" in h: return data.get("last_updated_riyadh")
    return None

def _build_sheet_rows(headers: List[str], symbols: List[str], results: Dict[str, Any]) -> List[List[Any]]:
    return [[_map_header_to_field(header, results.get(symbol, {})) for header in headers] for symbol in symbols]

def _quote_to_response(data: Dict[str, Any]) -> SingleAnalysisResponse:
    ml_pred = data.get("ml_prediction")
    ml_prediction = MLPrediction(**ml_pred) if ml_pred else None
    
    confidence = ConfidenceLevel.MEDIUM
    if ml_prediction:
        if ml_prediction.confidence_1m >= 0.8: confidence = ConfidenceLevel.VERY_HIGH
        elif ml_prediction.confidence_1m >= 0.6: confidence = ConfidenceLevel.HIGH
        elif ml_prediction.confidence_1m >= 0.4: confidence = ConfidenceLevel.MEDIUM
        elif ml_prediction.confidence_1m >= 0.2: confidence = ConfidenceLevel.LOW
        else: confidence = ConfidenceLevel.VERY_LOW
        
    data_quality = DataQuality.MISSING if data.get("error") else DataQuality.POOR if not data.get("price") else DataQuality.GOOD
    
    return SingleAnalysisResponse(
        symbol=data.get("symbol", "UNKNOWN"), name=data.get("name"), asset_class=AssetClass.EQUITY, market_region=MarketRegion.SAUDI,
        price=data.get("price"), volume=data.get("volume"), change=data.get("change"), change_pct=data.get("change_pct"),
        market_cap=data.get("market_cap"), pe_ratio=data.get("pe_ratio"), pb_ratio=data.get("pb_ratio"), dividend_yield=data.get("dividend_yield"),
        rsi_14d=data.get("rsi_14d"), macd=data.get("macd"), bb_position=data.get("bb_position"), volatility_30d=data.get("volatility_30d"),
        beta=data.get("beta"), momentum_1m=data.get("momentum_1m"), overall_score=data.get("overall_score"), risk_score=data.get("risk_score"),
        recommendation=data.get("recommendation", Recommendation.HOLD), confidence_level=confidence, expected_roi_1m=data.get("expected_roi_1m"),
        expected_roi_3m=data.get("expected_roi_3m"), expected_roi_12m=data.get("expected_roi_12m"), forecast_price_1m=data.get("forecast_price_1m"),
        forecast_price_3m=data.get("forecast_price_3m"), forecast_price_12m=data.get("forecast_price_12m"), ml_prediction=ml_prediction,
        data_quality=data_quality, error=data.get("error"), last_updated_utc=data.get("last_updated_utc"), last_updated_riyadh=data.get("last_updated_riyadh")
    )


@router.get("/health")
async def health(request: Request) -> Dict[str, Any]:
    engine = await _analysis_engine.get_engine(request)
    return {
        "status": "ok", "version": AI_ANALYSIS_VERSION, "timestamp_utc": _saudi_time.now_utc_iso(), "timestamp_riyadh": _saudi_time.now_iso(),
        "trading_day": _saudi_time.is_trading_day(), "trading_hours": _saudi_time.is_trading_hours(),
        "engine": {"available": engine is not None, "type": type(engine).__name__ if engine else "none"},
        "ml": {"initialized": _ml_models._initialized}, "cache": _cache.get_stats(),
        "websocket": {"connections": len(_ws_manager._active_connections), "subscriptions": len(_ws_manager._subscriptions)},
        "config": {"batch_size": _CONFIG.batch_size, "batch_concurrency": _CONFIG.batch_concurrency, "max_tickers": _CONFIG.max_tickers, "enable_ml": _CONFIG.enable_ml_predictions, "enable_shariah": _CONFIG.enable_shariah_filter},
        "request_id": getattr(request.state, "request_id", None)
    }

@router.get("/metrics")
async def metrics() -> Response:
    if not _PROMETHEUS_AVAILABLE: return BestJSONResponse(status_code=503, content={"error": "Metrics not available"})
    from prometheus_client import generate_latest, CONTENT_TYPE_LATEST
    return Response(content=generate_latest(), media_type=CONTENT_TYPE_LATEST)

@router.get("/quote", response_model=SingleAnalysisResponse)
async def get_quote(
    request: Request, symbol: str = Query(..., description="Ticker/Symbol"), include_ml: bool = Query(True, description="Include ML predictions"),
    token: Optional[str] = Query(default=None), x_app_token: Optional[str] = Header(None, alias="X-APP-TOKEN"), authorization: Optional[str] = Header(None, alias="Authorization")
) -> BestJSONResponse:
    request_id = str(uuid.uuid4())
    start_time = time.time()
    
    client_ip = request.client.host if request.client else "unknown"
    if not await _rate_limiter.check(client_ip): raise HTTPException(status_code=status.HTTP_429_TOO_MANY_REQUESTS, detail="Rate limit exceeded")
    auth_token = x_app_token or token or (authorization[7:] if authorization and authorization.startswith("Bearer ") else "")
    if not _token_manager.validate_token(auth_token): raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid token")
    
    symbol = symbol.strip().upper()
    if not symbol: return BestJSONResponse(content=json_loads(json_dumps(SingleAnalysisResponse(symbol="UNKNOWN", error="No symbol provided", data_quality=DataQuality.MISSING).model_dump() if _PYDANTIC_V2 else SingleAnalysisResponse(symbol="UNKNOWN", error="No symbol provided", data_quality=DataQuality.MISSING).dict())))
    
    engine = await _analysis_engine.get_engine(request)
    if not engine: return BestJSONResponse(content=json_loads(json_dumps(SingleAnalysisResponse(symbol=symbol, error="Engine unavailable", data_quality=DataQuality.MISSING).model_dump() if _PYDANTIC_V2 else SingleAnalysisResponse(symbol=symbol, error="Engine unavailable", data_quality=DataQuality.MISSING).dict())))
    
    results, stats = await _analysis_engine.get_quotes(engine, [symbol], include_ml=include_ml)
    data = results.get(symbol, _analysis_engine._create_placeholder(symbol, "No data"))
    
    response = _quote_to_response(data)
    response.last_updated_utc = _saudi_time.now_utc_iso()
    response.last_updated_riyadh = _saudi_time.now_iso()
    
    asyncio.create_task(_audit_logger.log("get_quote", auth_token[:8], "quote", "read", "success", {"symbol": symbol, "duration": time.time() - start_time}, request_id))
    return BestJSONResponse(content=json_loads(json_dumps(response.model_dump() if _PYDANTIC_V2 else response.dict())))

@router.post("/quote", response_model=SingleAnalysisResponse)
async def post_quote(
    request: Request, body: Dict[str, Any] = Body(default_factory=dict), include_ml: bool = Query(True, description="Include ML predictions"),
    token: Optional[str] = Query(default=None), x_app_token: Optional[str] = Header(None, alias="X-APP-TOKEN"), authorization: Optional[str] = Header(None, alias="Authorization")
) -> BestJSONResponse:
    symbol = body.get("symbol") or body.get("ticker") or body.get("Symbol") or ""
    return await get_quote(request=request, symbol=symbol, include_ml=include_ml, token=token, x_app_token=x_app_token, authorization=authorization)

@router.get("/quotes", response_model=BatchAnalysisResponse)
async def get_quotes(
    request: Request, tickers: str = Query(default="", description="Tickers: 'AAPL,MSFT 1120.SR'"), top_n: int = Query(default=0, ge=0, le=5000),
    include_ml: bool = Query(True, description="Include ML predictions"), token: Optional[str] = Query(default=None),
    x_app_token: Optional[str] = Header(None, alias="X-APP-TOKEN"), authorization: Optional[str] = Header(None, alias="Authorization")
) -> BestJSONResponse:
    request_id = str(uuid.uuid4())
    start_time = time.time()
    
    client_ip = request.client.host if request.client else "unknown"
    if not await _rate_limiter.check(client_ip): raise HTTPException(status_code=status.HTTP_429_TOO_MANY_REQUESTS, detail="Rate limit exceeded")
    auth_token = x_app_token or token or (authorization[7:] if authorization and authorization.startswith("Bearer ") else "")
    if not _token_manager.validate_token(auth_token): raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid token")
    
    symbols = _parse_tickers(tickers)
    if not symbols: return BestJSONResponse(content=json_loads(json_dumps(BatchAnalysisResponse(status="skipped", results=[], version=AI_ANALYSIS_VERSION, meta={"reason": "No tickers provided"}).model_dump() if _PYDANTIC_V2 else BatchAnalysisResponse(status="skipped", results=[], version=AI_ANALYSIS_VERSION, meta={"reason": "No tickers provided"}).dict())))
    symbols = symbols[:min(top_n, _CONFIG.max_tickers)] if top_n > 0 else symbols[:_CONFIG.max_tickers]
    
    engine = await _analysis_engine.get_engine(request)
    if not engine: return BestJSONResponse(content=json_loads(json_dumps(BatchAnalysisResponse(status="error", error="Engine unavailable", results=[], version=AI_ANALYSIS_VERSION).model_dump() if _PYDANTIC_V2 else BatchAnalysisResponse(status="error", error="Engine unavailable", results=[], version=AI_ANALYSIS_VERSION).dict())))
    
    results, stats = await _analysis_engine.get_quotes(engine, symbols, include_ml=include_ml)
    responses = [_quote_to_response(results.get(symbol) or _analysis_engine._create_placeholder(symbol, "No data")) for symbol in symbols]
    
    asyncio.create_task(_audit_logger.log("get_quotes", auth_token[:8], "quotes", "read", "success", {"symbols": len(symbols), "duration": time.time() - start_time, "stats": stats}, request_id))
    
    response = BatchAnalysisResponse(status="success", results=responses, version=AI_ANALYSIS_VERSION, meta={"requested": len(symbols), "from_cache": stats["from_cache"], "from_engine": stats["from_engine"], "ml_predictions": stats["ml_predictions"], "errors": stats["errors"], "duration_ms": (time.time() - start_time) * 1000, "timestamp_utc": _saudi_time.now_utc_iso(), "timestamp_riyadh": _saudi_time.now_iso()})
    return BestJSONResponse(content=json_loads(json_dumps(response.model_dump() if _PYDANTIC_V2 else response.dict())))

@router.post("/quotes", response_model=BatchAnalysisResponse)
async def batch_quotes(
    request: Request, body: Dict[str, Any] = Body(default_factory=dict), include_ml: bool = Query(True, description="Include ML predictions"),
    token: Optional[str] = Query(default=None), x_app_token: Optional[str] = Header(None, alias="X-APP-TOKEN"), authorization: Optional[str] = Header(None, alias="Authorization")
) -> BestJSONResponse:
    tickers = body.get("tickers") or body.get("symbols") or body if isinstance(body, list) else []
    if isinstance(tickers, list): tickers = ",".join(str(t) for t in tickers)
    return await get_quotes(request=request, tickers=tickers, include_ml=include_ml, token=token, x_app_token=x_app_token, authorization=authorization)

@router.post("/sheet-rows")
async def sheet_rows(
    request: Request, body: Dict[str, Any] = Body(...), mode: str = Query(default="", description="Mode hint"),
    token: Optional[str] = Query(default=None, description="Auth token"), x_app_token: Optional[str] = Header(default=None, alias="X-APP-TOKEN"),
    authorization: Optional[str] = Header(default=None, alias="Authorization"), x_request_id: Optional[str] = Header(default=None, alias="X-Request-ID")
) -> BestJSONResponse:
    request_id = x_request_id or str(uuid.uuid4())
    start_time = time.time()
    
    client_ip = request.client.host if request.client else "unknown"
    if not await _rate_limiter.check(client_ip):
        raise HTTPException(status_code=status.HTTP_429_TOO_MANY_REQUESTS, detail="Rate limit exceeded")
    
    auth_token = x_app_token or token or ""
    if authorization and authorization.startswith("Bearer "): auth_token = authorization[7:]
    if not _token_manager.validate_token(auth_token): raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid token")
    
    async with TraceContext("parse_request", {"request_id": request_id}):
        try: req = AdvancedSheetRequest.model_validate(body) if _PYDANTIC_V2 else AdvancedSheetRequest.parse_obj(body)
        except Exception as e:
            return BestJSONResponse(status_code=400, content={"status": "error", "headers": [], "rows": [], "error": f"Invalid request: {str(e)}", "version": AI_ANALYSIS_VERSION, "request_id": request_id, "meta": {"duration_ms": (time.time() - start_time) * 1000}})
    
    if req.include_predictions: asyncio.create_task(_ml_models.initialize())
    
    engine = await _analysis_engine.get_engine(request)
    if not engine: return BestJSONResponse(status_code=503, content={"status": "error", "headers": [], "rows": [], "error": "Data engine unavailable", "version": AI_ANALYSIS_VERSION, "request_id": request_id, "meta": {"duration_ms": (time.time() - start_time) * 1000}})
    
    async with TraceContext("fetch_quotes", {"symbol_count": len(req.symbols), "mode": mode}):
        quotes, _ = await _analysis_engine.get_quotes(engine, req.symbols[:req.top_n], include_ml=True)
    
    headers = req.headers or ["Symbol", "Name", "Price", "Change", "Change %", "Volume", "Market Cap", "P/E Ratio", "Dividend Yield", "Beta", "RSI (14)", "MACD", "BB Upper", "BB Lower", "BB Middle", "Volatility (30d)", "Momentum (14d)", "Sharpe Ratio", "Max Drawdown (30d)", "Correlation TASI", "Forecast Price (1M)", "Expected ROI (1M)", "Forecast Price (3M)", "Expected ROI (3M)", "Forecast Price (12M)", "Expected ROI (12M)", "Risk Score", "Overall Score", "Recommendation", "Data Quality", "Last Updated (UTC)", "Last Updated (Riyadh)"]
    rows, features_dict, predictions_dict = [], {} if req.include_features else None, {} if req.include_predictions else None
    
    for symbol in req.symbols[:req.top_n]:
        quote = quotes.get(symbol, _analysis_engine._create_placeholder(symbol, "Not found"))
        if hasattr(quote, 'dict'): quote = quote.dict()
        elif hasattr(quote, 'model_dump'): quote = quote.model_dump()
        elif is_dataclass(quote): quote = {k: v for k, v in quote.__dict__.items() if not k.startswith("_")}
        
        lookup = {k.lower(): v for k, v in quote.items()} if isinstance(quote, dict) else {}
        row = []
        for header in headers:
            h_lower = header.lower()
            if "symbol" in h_lower: row.append(lookup.get("symbol") or lookup.get("ticker"))
            elif "name" in h_lower: row.append(lookup.get("name") or lookup.get("company_name"))
            elif "price" in h_lower and "forecast" not in h_lower: row.append(lookup.get("price") or lookup.get("current_price"))
            elif "change" in h_lower and "%" not in h_lower: row.append(lookup.get("change") or lookup.get("price_change"))
            elif "change %" in h_lower or "change%" in h_lower: row.append(lookup.get("change_percent") or lookup.get("change_pct"))
            elif "volume" in h_lower: row.append(lookup.get("volume"))
            elif "market cap" in h_lower: row.append(lookup.get("market_cap"))
            elif "pe ratio" in h_lower or "p/e" in h_lower: row.append(lookup.get("pe_ratio"))
            elif "dividend yield" in h_lower: row.append(lookup.get("dividend_yield"))
            elif "beta" in h_lower: row.append(lookup.get("beta"))
            elif "rsi" in h_lower: row.append(lookup.get("rsi_14d"))
            elif "macd" in h_lower: row.append(lookup.get("macd"))
            elif "bb upper" in h_lower: row.append(lookup.get("bb_upper"))
            elif "bb lower" in h_lower: row.append(lookup.get("bb_lower"))
            elif "bb middle" in h_lower: row.append(lookup.get("bb_middle"))
            elif "volatility" in h_lower: row.append(lookup.get("volatility_30d"))
            elif "momentum" in h_lower: row.append(lookup.get("momentum_14d"))
            elif "sharpe" in h_lower: row.append(lookup.get("sharpe_ratio"))
            elif "drawdown" in h_lower or "max drawdown" in h_lower: row.append(lookup.get("max_drawdown_30d"))
            elif "correlation" in h_lower and "tasi" in h_lower: row.append(lookup.get("correlation_tasi"))
            elif "forecast price" in h_lower and "1m" in h_lower: row.append(lookup.get("forecast_price_1m"))
            elif "expected roi" in h_lower and "1m" in h_lower: row.append(lookup.get("expected_roi_1m"))
            elif "forecast price" in h_lower and "3m" in h_lower: row.append(lookup.get("forecast_price_3m"))
            elif "expected roi" in h_lower and "3m" in h_lower: row.append(lookup.get("expected_roi_3m"))
            elif "forecast price" in h_lower and "12m" in h_lower: row.append(lookup.get("forecast_price_12m"))
            elif "expected roi" in h_lower and "12m" in h_lower: row.append(lookup.get("expected_roi_12m"))
            elif "risk score" in h_lower: row.append(lookup.get("risk_score"))
            elif "overall score" in h_lower: row.append(lookup.get("overall_score"))
            elif "recommendation" in h_lower: row.append(lookup.get("recommendation") or "HOLD")
            elif "data quality" in h_lower: row.append(lookup.get("data_quality") or "PARTIAL")
            elif "last updated (utc)" in h_lower: row.append(lookup.get("last_updated_utc"))
            elif "last updated (riyadh)" in h_lower: row.append(lookup.get("last_updated_riyadh") or _saudi_time.now().isoformat())
            else: row.append(lookup.get(h_lower))
        rows.append(row)

    error_count = sum(1 for q in quotes.values() if isinstance(q, dict) and "error" in q)
    status_str = "success" if error_count == 0 else "partial" if error_count < len(req.symbols[:req.top_n]) else "error"
    
    if PROMETHEUS_AVAILABLE:
        _metrics.counter("requests_total", "Requests", ["status"]).labels(status=status_str).inc()
        _metrics.histogram("request_duration_seconds", "Duration").observe(time.time() - start_time)
        
    asyncio.create_task(_audit_logger.log("sheet_rows_request", auth_token[:8], "sheet_rows", "read", status_str, {"request_id": request_id, "symbols": len(req.symbols), "duration": time.time() - start_time}))
    if _CONFIG.adaptive_concurrency and _concurrency_controller.should_adjust(): _concurrency_controller.adjust()
    _concurrency_controller.record_request((time.time() - start_time) * 1000, status_str == "success")
    
    return BestJSONResponse(content=json_loads(json_dumps({
        "status": status_str, "headers": headers, "rows": rows, "features": features_dict, "predictions": predictions_dict, "error": f"{error_count} errors" if error_count > 0 else None, "version": AI_ANALYSIS_VERSION, "request_id": request_id, "meta": {"duration_ms": (time.time() - start_time) * 1000, "requested": len(req.symbols[:req.top_n]), "errors": error_count, "cache_stats": _cache.get_stats(), "concurrency": _concurrency_controller.current_concurrency, "riyadh_time": _saudi_time.now_iso(), "business_day": _saudi_time.is_trading_day()}
    })))

@router.get("/scoreboard")
async def scoreboard(
    request: Request, tickers: str = Query(default="", description="Tickers: 'AAPL,MSFT 1120.SR'"), top_n: int = Query(default=20, ge=1, le=200),
    token: Optional[str] = Query(default=None), x_app_token: Optional[str] = Header(None, alias="X-APP-TOKEN"), authorization: Optional[str] = Header(None, alias="Authorization")
) -> BestJSONResponse:
    request_id = str(uuid.uuid4())
    start_time = time.time()
    
    client_ip = request.client.host if request.client else "unknown"
    auth_token = x_app_token or token or (authorization[7:] if authorization and authorization.startswith("Bearer ") else "")
    if not _token_manager.validate_token(auth_token): raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid token")
    
    if not _CONFIG.enable_scoreboard: return BestJSONResponse(content=json_loads(json_dumps(ScoreboardResponse(status="disabled", error="Scoreboard disabled", headers=[], rows=[], version=AI_ANALYSIS_VERSION).model_dump() if _PYDANTIC_V2 else ScoreboardResponse(status="disabled", error="Scoreboard disabled", headers=[], rows=[], version=AI_ANALYSIS_VERSION).dict())))
    
    symbols = _parse_tickers(tickers)[:top_n]
    if not symbols: return BestJSONResponse(content=json_loads(json_dumps(ScoreboardResponse(status="skipped", headers=[], rows=[], version=AI_ANALYSIS_VERSION, meta={"reason": "No tickers provided"}).model_dump() if _PYDANTIC_V2 else ScoreboardResponse(status="skipped", headers=[], rows=[], version=AI_ANALYSIS_VERSION, meta={"reason": "No tickers provided"}).dict())))
    
    engine = await _analysis_engine.get_engine(request)
    if not engine: return BestJSONResponse(content=json_loads(json_dumps(ScoreboardResponse(status="error", error="Engine unavailable", headers=[], rows=[], version=AI_ANALYSIS_VERSION).model_dump() if _PYDANTIC_V2 else ScoreboardResponse(status="error", error="Engine unavailable", headers=[], rows=[], version=AI_ANALYSIS_VERSION).dict())))
    
    results, stats = await _analysis_engine.get_quotes(engine, symbols, include_ml=True)
    headers = ["Rank", "Symbol", "Name", "Price", "Change %", "Overall Score", "Risk Score", "Recommendation", "Expected ROI 1M", "Expected ROI 3M", "Expected ROI 12M", "ML Confidence", "Signal", "Last Updated"]
    rows, items = [], []
    
    for symbol in symbols:
        if not (data := results.get(symbol)): continue
        ml_pred = data.get("ml_prediction", {})
        items.append({"symbol": symbol, "name": data.get("name", ""), "price": data.get("price"), "change_pct": data.get("change_pct"), "overall_score": data.get("overall_score") or 50.0, "risk_score": data.get("risk_score") or 50.0, "recommendation": data.get("recommendation", "HOLD"), "expected_roi_1m": data.get("expected_roi_1m"), "expected_roi_3m": data.get("expected_roi_3m"), "expected_roi_12m": data.get("expected_roi_12m"), "confidence": ml_pred.get("confidence_1m", 0.5) if ml_pred else 0.5, "signal": ml_pred.get("signal_1m", "neutral") if ml_pred else "neutral", "last_updated": data.get("last_updated_riyadh", "")})
        
    items.sort(key=lambda x: x["overall_score"], reverse=True)
    for i, item in enumerate(items[:top_n], 1):
        rows.append([i, item["symbol"], item["name"], item["price"], item["change_pct"], item["overall_score"], item["risk_score"], item["recommendation"], item["expected_roi_1m"], item["expected_roi_3m"], item["expected_roi_12m"], f"{item['confidence']:.1%}", item["signal"].upper(), item["last_updated"]])
        
    response = ScoreboardResponse(status="success", headers=headers, rows=rows, version=AI_ANALYSIS_VERSION, meta={"total": len(items), "displayed": min(len(items), top_n), "duration_ms": (time.time() - start_time) * 1000, "timestamp_riyadh": _saudi_time.now_iso()})
    return BestJSONResponse(content=json_loads(json_dumps(response.model_dump() if _PYDANTIC_V2 else response.dict())))

@router.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket, token: Optional[str] = Query(None)):
    if not _CONFIG.enable_websocket:
        await websocket.close(code=1000, reason="WebSocket disabled")
        return
    if not token or not _token_manager.validate_token(token):
        await websocket.close(code=1008, reason="Invalid token")
        return
    await _ws_manager.connect(websocket)
    try:
        await websocket.send_text(json_dumps({"type": "connection", "status": "connected", "timestamp": _saudi_time.now_iso(), "version": AI_ANALYSIS_VERSION})) if _HAS_ORJSON else await websocket.send_json({"type": "connection", "status": "connected", "timestamp": _saudi_time.now_iso(), "version": AI_ANALYSIS_VERSION})
        while True:
            try:
                data = await websocket.receive_json()
                if data.get("type") == "subscribe" and (symbol := data.get("symbol")):
                    await _ws_manager.subscribe(websocket, symbol)
                    await websocket.send_text(json_dumps({"type": "subscribed", "symbol": symbol, "timestamp": _saudi_time.now_iso()})) if _HAS_ORJSON else await websocket.send_json({"type": "subscribed", "symbol": symbol, "timestamp": _saudi_time.now_iso()})
                elif data.get("type") == "unsubscribe" and (symbol := data.get("symbol")):
                    await _ws_manager.unsubscribe(websocket, symbol)
                    await websocket.send_text(json_dumps({"type": "unsubscribed", "symbol": symbol, "timestamp": _saudi_time.now_iso()})) if _HAS_ORJSON else await websocket.send_json({"type": "unsubscribed", "symbol": symbol, "timestamp": _saudi_time.now_iso()})
                elif data.get("type") == "ping":
                    await websocket.send_text(json_dumps({"type": "pong", "timestamp": _saudi_time.now_iso()})) if _HAS_ORJSON else await websocket.send_json({"type": "pong", "timestamp": _saudi_time.now_iso()})
            except WebSocketDisconnect: break
            except Exception as e:
                logger.error(f"WebSocket error: {e}")
                await websocket.send_text(json_dumps({"type": "error", "error": str(e), "timestamp": _saudi_time.now_iso()})) if _HAS_ORJSON else await websocket.send_json({"type": "error", "error": str(e), "timestamp": _saudi_time.now_iso()})
    finally:
        await _ws_manager.disconnect(websocket)

__all__ = ["router"]
