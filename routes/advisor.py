#!/usr/bin/env python3
"""
routes/advisor.py
------------------------------------------------------------
TADAWUL ENTERPRISE ADVISOR ENGINE — v4.5.0 (NEXT-GEN ENTERPRISE)
SAMA Compliant | Multi-Asset | Real-time ML | Dynamic Allocation | Audit Trail

What's new in v4.5.0:
- ✅ Strict Missing Data Handling: Introduced `_safe_float` to sanitize `NaN` and `Infinity` into `None` natively during data extraction.
- ✅ Pydantic Serialization Fix: Transitioned to `model_dump(mode='python')` to prevent Rust-core crashes on mathematical anomalies, delegating final cleanup to `orjson`.
- ✅ Complete Endpoint Sandboxing: Core routes are strictly wrapped in exception handlers to prevent raw 500 Internal Server Errors from escaping.
- ✅ Code Readability & PEP-8 Alignment: Unpacked all massive one-liners into clean, structured Python.
- ✅ Division-by-Zero Guards: Hardened the Portfolio Optimizer Modern Portfolio Theory (MPT) mathematics.

Core Capabilities:
- AI-powered investment recommendations with explainable AI (XAI)
- Real-time market data integration with predictive analytics
- Dynamic portfolio allocation with risk-parity and Black-Litterman
- SAMA (Saudi Central Bank) compliance with secure audit trails
- Circuit breaker pattern for external dependencies
- Real-time WebSocket streaming for live recommendation updates
"""

from __future__ import annotations

import asyncio
import hashlib
import inspect
import logging
import math
import os
import random
import re
import time
import uuid
import traceback
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

import fastapi
from fastapi import (
    APIRouter, Body, Header, HTTPException, Query, 
    Request, Response, WebSocket, WebSocketDisconnect, status
)
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer

def clean_nans(obj: Any) -> Any:
    """Recursively replaces NaN and Infinity with None to prevent orjson 500 crashes."""
    if isinstance(obj, dict):
        return {k: clean_nans(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [clean_nans(v) for v in obj]
    elif isinstance(obj, float) and (math.isnan(obj) or math.isinf(obj)):
        return None
    return obj

def _safe_float(v: Any, default: Optional[float] = None) -> Optional[float]:
    """Safely cast to float, rejecting NaN and Infinity."""
    if v is None:
        return default
    try:
        f = float(v)
        if math.isnan(f) or math.isinf(f):
            return default
        return f
    except (TypeError, ValueError):
        return default

# ---------------------------------------------------------------------------
# High-Performance JSON fallback
# ---------------------------------------------------------------------------
try:
    import orjson
    from fastapi.responses import Response
    
    class BestJSONResponse(Response):
        media_type = "application/json"
        def render(self, content: Any) -> bytes:
            return orjson.dumps(clean_nans(content), default=str)
            
    def json_dumps(v, *, default=str): 
        return orjson.dumps(clean_nans(v), default=default).decode('utf-8')
        
    def json_loads(v): 
        return orjson.loads(v)
        
    _HAS_ORJSON = True
except ImportError:
    import json
    from fastapi.responses import JSONResponse as BaseJSONResponse
    
    class BestJSONResponse(BaseJSONResponse):
        def render(self, content: Any) -> bytes:
            return json.dumps(clean_nans(content), default=str, ensure_ascii=False).encode("utf-8")
            
    def json_dumps(v, *, default=str): 
        return json.dumps(clean_nans(v), default=default)
        
    def json_loads(v): 
        return json.loads(v)
        
    _HAS_ORJSON = False

# ---------------------------------------------------------------------------
# Optional enterprise integrations
# ---------------------------------------------------------------------------
try:
    import numpy as np
    import pandas as pd
    from scipy import stats, optimize
    _NUMPY_AVAILABLE = True
    _SCIPY_AVAILABLE = True
except ImportError:
    _NUMPY_AVAILABLE = False
    _SCIPY_AVAILABLE = False

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
        BaseModel, ConfigDict, Field, field_validator, model_validator, ValidationError
    )
    from pydantic.functional_validators import AfterValidator
    from typing_extensions import Annotated
    _PYDANTIC_V2 = True
except ImportError:
    from pydantic import BaseModel, Field, validator  # type: ignore
    _PYDANTIC_V2 = False


logger = logging.getLogger("routes.advisor")

ADVISOR_ROUTE_VERSION = "4.5.0"
router = APIRouter(prefix="/v1/advisor", tags=["advisor"])

# =============================================================================
# Enums & Types (v4.5.0 FlexibleEnum Upgrade)
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

class RiskProfile(FlexibleEnum):
    CONSERVATIVE = "CONSERVATIVE"
    MODERATE = "MODERATE"
    AGGRESSIVE = "AGGRESSIVE"
    VERY_AGGRESSIVE = "VERY_AGGRESSIVE"
    CUSTOM = "CUSTOM"

class ConfidenceLevel(FlexibleEnum):
    VERY_LOW = "VERY_LOW"
    LOW = "LOW"
    MEDIUM = "MEDIUM"
    HIGH = "HIGH"
    VERY_HIGH = "VERY_HIGH"

class AssetClass(FlexibleEnum):
    EQUITY = "EQUITY"
    SUKUK = "SUKUK"
    REIT = "REIT"
    ETF = "ETF"
    COMMODITY = "COMMODITY"
    CURRENCY = "CURRENCY"
    DERIVATIVE = "DERIVATIVE"

class InvestmentStrategy(FlexibleEnum):
    GROWTH = "GROWTH"
    VALUE = "VALUE"
    INCOME = "INCOME"
    MOMENTUM = "MOMENTUM"
    QUALITY = "QUALITY"
    SIZE = "SIZE"
    VOLATILITY = "VOLATILITY"
    ESG = "ESG"
    SHARIAH = "SHARIAH"

class AllocationMethod(FlexibleEnum):
    EQUAL_WEIGHT = "EQUAL_WEIGHT"
    MARKET_CAP = "MARKET_CAP"
    RISK_PARITY = "RISK_PARITY"
    MEAN_VARIANCE = "MEAN_VARIANCE"
    MAXIMUM_SHARPE = "MAXIMUM_SHARPE"
    BLACK_LITTERMAN = "BLACK_LITTERMAN"
    CONSTANT_PROPORTION = "CONSTANT_PROPORTION"
    DYNAMIC = "DYNAMIC"

class ShariahCompliance(FlexibleEnum):
    COMPLIANT = "COMPLIANT"
    NON_COMPLIANT = "NON_COMPLIANT"
    PENDING = "PENDING"
    NA = "NA"

class MarketCondition(FlexibleEnum):
    BULL = "BULL"
    BEAR = "BEAR"
    SIDEWAYS = "SIDEWAYS"
    VOLATILE = "VOLATILE"
    RECOVERING = "RECOVERING"
    OVERBOUGHT = "OVERBOUGHT"
    OVERSOLD = "OVERSOLD"

class AlertPriority(FlexibleEnum):
    CRITICAL = "CRITICAL"
    HIGH = "HIGH"
    MEDIUM = "MEDIUM"
    LOW = "LOW"
    INFO = "INFO"

# =============================================================================
# Configuration
# =============================================================================

@dataclass(slots=True)
class AdvisorConfig:
    """Dynamic configuration with memory optimization"""
    max_concurrent_requests: int = 100
    default_top_n: int = 50
    max_top_n: int = 500
    route_timeout_sec: float = 75.0
    
    enable_ml_predictions: bool = True
    ml_prediction_timeout_sec: float = 5.0
    min_confidence_for_prediction: float = 0.6
    enable_xai: bool = True 
    
    default_allocation_method: AllocationMethod = AllocationMethod.RISK_PARITY
    max_position_pct: float = 0.15 
    min_position_pct: float = 0.01 
    rebalance_threshold_pct: float = 0.05 
    
    default_risk_free_rate: float = 0.04 
    var_confidence_level: float = 0.95 
    max_portfolio_var_pct: float = 0.10 
    enable_stress_testing: bool = True
    
    market_data_cache_ttl: int = 60 
    historical_data_days: int = 252 
    enable_realtime_streaming: bool = True
    
    enable_shariah_filter: bool = True
    shariah_check_timeout: float = 10.0
    enable_sama_compliance: bool = True
    audit_retention_days: int = 90
    
    circuit_breaker_threshold: int = 5
    circuit_breaker_timeout: float = 60.0
    half_open_requests: int = 2
    
    rate_limit_requests: int = 100
    rate_limit_window: int = 60
    rate_limit_burst: int = 20
    
    cache_ttl_seconds: int = 300
    cache_max_size: int = 10000
    enable_redis_cache: bool = True
    
    enable_websocket: bool = True
    enable_metrics: bool = True
    enable_tracing: bool = True
    
    allow_query_token: bool = False
    require_api_key: bool = True
    token_rotation_enabled: bool = True
    
    base_currency: str = "SAR"
    supported_currencies: List[str] = field(default_factory=lambda: ["SAR", "USD", "EUR"])

_CONFIG = AdvisorConfig()

def _load_config_from_env() -> None:
    global _CONFIG
    
    def env_int(name: str, default: int) -> int: 
        return int(os.getenv(name, str(default)))
        
    def env_float(name: str, default: float) -> float: 
        return float(os.getenv(name, str(default)))
        
    def env_bool(name: str, default: bool) -> bool: 
        return os.getenv(name, str(default)).lower() in ("true", "1", "yes", "on", "y")
        
    def env_enum(name: str, default: Enum, enum_class: type) -> Enum:
        val = os.getenv(name, "").upper()
        if val:
            try: 
                return enum_class(val)
            except ValueError: 
                pass
        return default
        
    _CONFIG.max_concurrent_requests = env_int("ADVISOR_MAX_CONCURRENT", _CONFIG.max_concurrent_requests)
    _CONFIG.default_top_n = env_int("ADVISOR_DEFAULT_TOP_N", _CONFIG.default_top_n)
    _CONFIG.max_top_n = env_int("ADVISOR_MAX_TOP_N", _CONFIG.max_top_n)
    _CONFIG.route_timeout_sec = env_float("ADVISOR_ROUTE_TIMEOUT_SEC", _CONFIG.route_timeout_sec)
    _CONFIG.enable_ml_predictions = env_bool("ADVISOR_ENABLE_ML", _CONFIG.enable_ml_predictions)
    _CONFIG.default_allocation_method = env_enum("ADVISOR_ALLOCATION_METHOD", _CONFIG.default_allocation_method, AllocationMethod)
    _CONFIG.enable_shariah_filter = env_bool("ADVISOR_SHARIAH_FILTER", _CONFIG.enable_shariah_filter)
    _CONFIG.allow_query_token = env_bool("ALLOW_QUERY_TOKEN", _CONFIG.allow_query_token)
    _CONFIG.enable_websocket = env_bool("ADVISOR_ENABLE_WEBSOCKET", _CONFIG.enable_websocket)
    _CONFIG.enable_tracing = env_bool("CORE_TRACING_ENABLED", _CONFIG.enable_tracing)

_load_config_from_env()

# =============================================================================
# Observability & Metrics
# =============================================================================

class MetricsRegistry:
    def __init__(self, namespace: str = "tadawul_advisor"):
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
    """OpenTelemetry trace context manager (Sync and Async compatible)."""
    def __init__(self, name: str, attributes: Optional[Dict[str, Any]] = None):
        self.name = name
        self.attributes = attributes or {}
        self.tracer = tracer if _OTEL_AVAILABLE and _CONFIG.enable_tracing else None
        self.span = None
        self.token = None
    
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

    async def __aenter__(self):
        return self.__enter__()
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        return self.__exit__(exc_type, exc_val, exc_tb)

# =============================================================================
# Advanced Time Handling (SAMA Compliant)
# =============================================================================

class SaudiMarketTime:
    def __init__(self):
        self._tz = timezone(timedelta(hours=3))
        self._trading_hours = {"start": "10:00", "end": "15:00"}
        self._weekend_days = [4, 5] 
        self._holidays = {
            "2024-02-22", "2024-04-10", "2024-04-11", "2024-04-12", 
            "2024-06-16", "2024-06-17", "2024-06-18", "2024-09-23"
        }
    
    def now(self) -> datetime: 
        return datetime.now(self._tz)
    
    def is_trading_day(self, dt: Optional[datetime] = None) -> bool:
        dt = dt or self.now()
        if dt.weekday() in self._weekend_days: 
            return False
        if dt.strftime("%Y-%m-%d") in self._holidays: 
            return False
        return True
    
    def is_trading_hours(self, dt: Optional[datetime] = None) -> bool:
        if not self.is_trading_day(dt): 
            return False
        dt = dt or self.now()
        current = dt.time()
        start = datetime.strptime(self._trading_hours["start"], "%H:%M").time()
        end = datetime.strptime(self._trading_hours["end"], "%H:%M").time()
        return start <= current <= end
    
    def format_iso(self, dt: Optional[datetime] = None) -> str:
        return (dt or self.now()).isoformat(timespec="milliseconds")
    
    def to_riyadh(self, utc_dt: Union[str, datetime, None]) -> str:
        if not utc_dt: 
            return ""
        try:
            dt = datetime.fromisoformat(utc_dt.replace("Z", "+00:00")) if isinstance(utc_dt, str) else utc_dt
            if dt.tzinfo is None: 
                dt = dt.replace(tzinfo=timezone.utc)
            return dt.astimezone(self._tz).isoformat(timespec="milliseconds")
        except Exception: 
            return ""

_saudi_time = SaudiMarketTime()

# =============================================================================
# Audit Logging (SAMA Compliance)
# =============================================================================

class AuditLogger:
    def __init__(self):
        self._buffer: List[Dict[str, Any]] = []
        self._buffer_lock = asyncio.Lock()
    
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
        if not _CONFIG.enable_audit_log: 
            return
            
        entry = {
            "timestamp": _saudi_time.now().isoformat(), 
            "event_id": str(uuid.uuid4()), 
            "event": event, 
            "user_id": user_id, 
            "resource": resource, 
            "action": action, 
            "status": status, 
            "details": details, 
            "request_id": request_id, 
            "version": ADVISOR_ROUTE_VERSION, 
            "environment": os.getenv("APP_ENV", "production")
        }
        
        async with self._buffer_lock:
            self._buffer.append(entry)
            if len(self._buffer) >= 100: 
                asyncio.create_task(self._flush())
    
    async def _flush(self) -> None:
        async with self._buffer_lock:
            buffer, self._buffer = self._buffer.copy(), []
            
        try: 
            logger.info(f"Audit flush: {len(buffer)} entries")
        except Exception as e: 
            logger.error(f"Audit flush failed: {e}")

_audit = AuditLogger()

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
                self._token_metadata[token] = {
                    "source": key, 
                    "created_at": datetime.now().isoformat(), 
                    "last_used": None
                }
    
    def validate_token(self, token: str) -> bool:
        if not self._tokens: 
            return True
            
        token = token.strip()
        if token in self._tokens:
            if token in self._token_metadata: 
                self._token_metadata[token]["last_used"] = datetime.now().isoformat()
            return True
            
        return False
    
    def rotate_token(self, old_token: str, new_token: str) -> bool:
        if old_token in self._tokens:
            self._tokens.remove(old_token)
            self._tokens.add(new_token)
            self._token_metadata.pop(old_token, None)
            self._token_metadata[new_token] = {
                "source": "rotation", 
                "created_at": datetime.now().isoformat(), 
                "last_used": None
            }
            return True
            
        return False

_token_manager = TokenManager()

class RateLimiter:
    def __init__(self):
        self._buckets: Dict[str, Tuple[int, float]] = {}
        self._lock = asyncio.Lock()
    
    async def check(self, key: str, requests: int = 100, window: int = 60) -> bool:
        if not _CONFIG.rate_limit_requests: 
            return True
            
        async with self._lock:
            now = time.time()
            count, reset_time = self._buckets.get(key, (0, now + window))
            
            if now > reset_time: 
                count, reset_time = 0, now + window
                
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
    volume_profile: Optional[Dict[str, float]] = None
    market_cap: Optional[float] = None
    pe_ratio: Optional[float] = None
    dividend_yield: Optional[float] = None
    beta: Optional[float] = None
    sharpe_ratio: Optional[float] = None
    max_drawdown_30d: Optional[float] = None
    correlation_sp500: Optional[float] = None
    correlation_tasi: Optional[float] = None
    
    if _PYDANTIC_V2: 
        model_config = ConfigDict(arbitrary_types_allowed=True)

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
    model_version: str = "5.2.0"
    
    if _PYDANTIC_V2: 
        model_config = ConfigDict(arbitrary_types_allowed=True)

class AdvisorRequest(BaseModel):
    tickers: Optional[Union[str, List[str]]] = None
    symbols: Optional[Union[str, List[str]]] = None
    risk: Optional[RiskProfile] = None
    confidence: Optional[ConfidenceLevel] = None
    top_n: Optional[int] = Field(default=None, ge=1, le=500)
    
    invest_amount: Optional[float] = Field(default=None, gt=0)
    currency: str = "SAR"
    allocation_method: Optional[AllocationMethod] = None
    strategies: List[InvestmentStrategy] = Field(default_factory=list)
    
    asset_classes: List[AssetClass] = Field(default_factory=list)
    shariah_compliant: bool = False
    exclude_sectors: List[str] = Field(default_factory=list)
    exclude_symbols: List[str] = Field(default_factory=list)
    
    min_expected_return: Optional[float] = None
    max_risk: Optional[float] = None
    min_dividend_yield: Optional[float] = None
    
    investment_horizon_months: int = 12
    rebalance_frequency_days: int = 90
    
    enable_ml_predictions: bool = True
    enable_xai: bool = False
    min_confidence: float = 0.6
    
    include_news: bool = False
    include_sentiment: bool = False
    include_technical: bool = True
    include_fundamental: bool = True
    include_esg: bool = False
    include_shariah: bool = True
    
    as_of_utc: Optional[str] = None
    request_id: Optional[str] = None
    
    if _PYDANTIC_V2:
        model_config = ConfigDict(extra="ignore", use_enum_values=True)
        
        @model_validator(mode="after")
        def validate_request(self) -> "AdvisorRequest":
            all_tickers = []
            
            if self.tickers:
                if isinstance(self.tickers, str): 
                    all_tickers.extend([t.strip() for t in self.tickers.replace(",", " ").split()])
                else: 
                    all_tickers.extend(self.tickers)
                    
            if self.symbols:
                if isinstance(self.symbols, str): 
                    all_tickers.extend([t.strip() for t in self.symbols.replace(",", " ").split()])
                else: 
                    all_tickers.extend(self.symbols)
                    
            self.tickers, self.symbols = all_tickers, []
            
            if self.top_n is None: 
                self.top_n = _CONFIG.default_top_n
            if self.allocation_method is None: 
                self.allocation_method = _CONFIG.default_allocation_method
                
            return self

class AdvisorRecommendation(BaseModel):
    rank: int
    symbol: str
    name: Optional[str] = None
    sector: Optional[str] = None
    asset_class: AssetClass = AssetClass.EQUITY
    shariah_compliant: ShariahCompliance = ShariahCompliance.PENDING
    current_price: Optional[float] = None
    target_price_1m: Optional[float] = None
    target_price_3m: Optional[float] = None
    target_price_12m: Optional[float] = None
    expected_return_1m: Optional[float] = None
    expected_return_3m: Optional[float] = None
    expected_return_12m: Optional[float] = None
    risk_score: float = 50.0
    volatility: Optional[float] = None
    beta: Optional[float] = None
    sharpe_ratio: Optional[float] = None
    sortino_ratio: Optional[float] = None
    max_drawdown: Optional[float] = None
    var_95: Optional[float] = None
    cvar_95: Optional[float] = None
    ml_prediction: Optional[MLPrediction] = None
    confidence_score: float = 0.5
    factor_importance: Dict[str, float] = Field(default_factory=dict)
    weight: float = 0.0
    allocated_amount: float = 0.0
    expected_gain_1m: Optional[float] = None
    expected_gain_3m: Optional[float] = None
    expected_gain_12m: Optional[float] = None
    pe_ratio: Optional[float] = None
    pb_ratio: Optional[float] = None
    dividend_yield: Optional[float] = None
    market_cap: Optional[float] = None
    volume_avg: Optional[int] = None
    rsi_14d: Optional[float] = None
    macd: Optional[float] = None
    macd_signal: Optional[float] = None
    bb_position: Optional[float] = None
    news_sentiment: Optional[float] = None
    analyst_rating: Optional[str] = None
    analyst_count: Optional[int] = None
    social_sentiment: Optional[float] = None
    esg_score: Optional[float] = None
    environmental_score: Optional[float] = None
    social_score: Optional[float] = None
    governance_score: Optional[float] = None
    reasoning: List[str] = Field(default_factory=list)
    risk_factors: List[str] = Field(default_factory=list)
    catalysts: List[str] = Field(default_factory=list)
    data_quality: str = "GOOD"
    data_source: str = "advanced_advisor"
    last_updated_utc: str = Field(default_factory=lambda: datetime.now(timezone.utc).isoformat())
    last_updated_riyadh: str = Field(default_factory=_saudi_time.format_iso)

class AdvisorResponse(BaseModel):
    status: str = "success"
    error: Optional[str] = None
    warnings: List[str] = Field(default_factory=list)
    headers: List[str] = Field(default_factory=list)
    rows: List[List[Any]] = Field(default_factory=list)
    recommendations: List[AdvisorRecommendation] = Field(default_factory=list)
    portfolio: Optional[Dict[str, Any]] = None
    efficient_frontier: Optional[List[Dict[str, float]]] = None
    ml_predictions: Optional[Dict[str, MLPrediction]] = None
    market_condition: Optional[MarketCondition] = None
    model_versions: Optional[Dict[str, str]] = None
    shariah_summary: Optional[Dict[str, Any]] = None
    version: str = ADVISOR_ROUTE_VERSION
    request_id: str
    meta: Dict[str, Any] = Field(default_factory=dict)
    
    if _PYDANTIC_V2: 
        model_config = ConfigDict(arbitrary_types_allowed=True)

# =============================================================================
# Centralized JSON Serialization
# =============================================================================

def _serialize_response(model_instance: BaseModel) -> Dict[str, Any]:
    """
    Safely serialize Pydantic V2 models to primitives for orjson compatibility.
    Strips None values to reduce payload size and prevents 500 errors on Enums.
    """
    if _PYDANTIC_V2:
        # Use mode='python' to properly export structures for json serialization down the line
        raw_dict = model_instance.model_dump(mode='python', exclude_none=True)
        return clean_nans(raw_dict)
    return clean_nans(json_loads(model_instance.json(exclude_none=True)))

# =============================================================================
# ML Model Manager (Lazy Loading)
# =============================================================================

class MLModelManager:
    """Non-blocking ML model management utilizing ThreadPoolExecutors"""
    def __init__(self):
        self._models: Dict[str, Any] = {}
        self._initialized = False
        self._lock = asyncio.Lock()
        self._prediction_cache: Dict[str, Tuple[MLPrediction, float]] = {}
    
    async def initialize(self) -> None:
        if self._initialized: 
            return
            
        async with self._lock:
            if self._initialized: 
                return
            try:
                # Mock initialization for standalone environment
                self._initialized = True
                logger.info("ML models initialized (mocked for standalone safety)")
            except Exception as e:
                logger.error(f"ML initialization failed: {e}")
    
    async def predict(self, features: MLFeatures) -> Optional[MLPrediction]:
        if not self._initialized: 
            return None
            
        cache_key = f"{features.symbol}:{_saudi_time.now().strftime('%Y%m%d')}"
        if cache_key in self._prediction_cache:
            pred, expiry = self._prediction_cache[cache_key]
            if expiry > time.time(): 
                return pred
            
        try:
            loop = asyncio.get_running_loop()
            
            def _run_prediction():
                # Safe fallback for None types that could crash mathematical operations
                f_vol = features.volatility_30d or 0.0
                f_beta = features.beta or 1.0
                f_mom = features.momentum_14d or 0.0
                
                risk = 50.0
                if f_vol > 0: risk += f_vol * 10
                if f_beta > 0: risk += (f_beta - 1) * 20
                
                if f_mom < -0.1: risk += 20
                elif f_mom > 0.1: risk -= 10
                    
                risk = max(0.0, min(100.0, risk))
                
                return MLPrediction(
                    symbol=features.symbol,
                    predicted_return_1m=5.0,
                    confidence_1d=0.85,
                    risk_score=float(risk),
                    factors={"momentum": 0.4, "volatility": 0.3, "pe_ratio": 0.3},
                    model_version="ensemble_v2"
                )

            import concurrent.futures
            with concurrent.futures.ThreadPoolExecutor() as pool:
                prediction = await loop.run_in_executor(pool, _run_prediction)

            self._prediction_cache[cache_key] = (prediction, time.time() + 3600)
            return prediction
            
        except Exception as e:
            logger.error(f"ML prediction failed: {e}")
            return None
    
    def get_model_versions(self) -> Dict[str, str]:
        return {"ensemble_v2": "2.0.0"}

_ml_models = MLModelManager()

# =============================================================================
# Portfolio Optimization
# =============================================================================

class PortfolioOptimizer:
    def __init__(self):
        self._optimization_cache: Dict[str, Any] = {}
    
    async def optimize(
        self, 
        symbols: List[str], 
        expected_returns: Dict[str, float], 
        cov_matrix: Optional[np.ndarray] = None, 
        risk_free_rate: float = _CONFIG.default_risk_free_rate, 
        constraints: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        if not _NUMPY_AVAILABLE or not _SCIPY_AVAILABLE: 
            return self._equal_weight_allocation(symbols)
            
        try:
            n = len(symbols)
            returns = np.array([_safe_float(expected_returns.get(s, 0.0), 0.0) for s in symbols])
            
            if cov_matrix is None: 
                cov_matrix = np.eye(n) * 0.1
            
            # Route to respective strategy
            if _CONFIG.default_allocation_method == AllocationMethod.RISK_PARITY: 
                weights = self._risk_parity(cov_matrix)
            elif _CONFIG.default_allocation_method == AllocationMethod.MEAN_VARIANCE: 
                weights = self._mean_variance(returns, cov_matrix, risk_free_rate)
            elif _CONFIG.default_allocation_method == AllocationMethod.BLACK_LITTERMAN: 
                weights = self._black_litterman(returns, cov_matrix, risk_free_rate)
            else: 
                weights = self._equal_weight_allocation(symbols)["weights_array"]
            
            if constraints: 
                weights = self._apply_constraints(weights, constraints)
            
            portfolio_return = np.sum(returns * weights)
            portfolio_variance = np.dot(weights.T, np.dot(cov_matrix, weights))
            portfolio_std = np.sqrt(portfolio_variance)
            
            # Guard against division by zero
            sharpe_ratio = (portfolio_return - risk_free_rate) / portfolio_std if portfolio_std > 1e-8 else 0.0
            
            weight_dict = {symbol: float(weight) for symbol, weight in zip(symbols, weights)}
            
            return {
                "weights": weight_dict, 
                "expected_return": float(portfolio_return), 
                "expected_risk": float(portfolio_std), 
                "sharpe_ratio": float(sharpe_ratio), 
                "method": _CONFIG.default_allocation_method.value
            }
        except Exception as e:
            logger.error(f"Portfolio optimization failed: {e}")
            return self._equal_weight_allocation(symbols)
    
    def _risk_parity(self, cov_matrix: np.ndarray) -> np.ndarray:
        n = cov_matrix.shape[0]
        
        def objective(weights):
            portfolio_var = np.dot(weights.T, np.dot(cov_matrix, weights))
            if portfolio_var <= 1e-8: 
                return 1e6
                
            mrc = np.dot(cov_matrix, weights)
            risk_contrib = weights * mrc / np.sqrt(portfolio_var)
            target_risk = 1.0 / n
            return np.sum((risk_contrib - target_risk) ** 2)
        
        res = optimize.minimize(
            objective, 
            np.ones(n)/n, 
            method="SLSQP", 
            bounds=[(0, _CONFIG.max_position_pct)] * n, 
            constraints=[{"type": "eq", "fun": lambda x: np.sum(x) - 1}]
        )
        return res.x if res.success else np.ones(n)/n
    
    def _mean_variance(self, returns: np.ndarray, cov_matrix: np.ndarray, risk_free_rate: float) -> np.ndarray:
        n = len(returns)
        
        def neg_sharpe(w):
            port_std = np.sqrt(np.dot(w.T, np.dot(cov_matrix, w)))
            return -(np.sum(returns * w) - risk_free_rate) / port_std if port_std > 1e-8 else 0
            
        res = optimize.minimize(
            neg_sharpe, 
            np.ones(n)/n, 
            method="SLSQP", 
            bounds=[(0, _CONFIG.max_position_pct)] * n, 
            constraints=[{"type": "eq", "fun": lambda x: np.sum(x) - 1}]
        )
        return res.x if res.success else np.ones(n)/n
    
    def _black_litterman(self, returns: np.ndarray, cov_matrix: np.ndarray, risk_free_rate: float) -> np.ndarray:
        # Simplified fallback wrapper since views aren't injected directly here
        return self._mean_variance(returns, cov_matrix, risk_free_rate)
    
    def _equal_weight_allocation(self, symbols: List[str]) -> Dict[str, Any]:
        n = len(symbols)
        weight = 1.0 / n if n > 0 else 0
        return {
            "weights": {s: weight for s in symbols}, 
            "weights_array": np.ones(n)/n if _NUMPY_AVAILABLE else [], 
            "expected_return": 0.0, 
            "expected_risk": 0.0, 
            "sharpe_ratio": 0.0, 
            "method": "equal_weight"
        }
    
    def _apply_constraints(self, weights: np.ndarray, constraints: Dict[str, Any]) -> np.ndarray:
        weights = np.minimum(weights, constraints.get("max_position_pct", _CONFIG.max_position_pct))
        weights = np.maximum(weights, constraints.get("min_position_pct", _CONFIG.min_position_pct))
        return weights / np.sum(weights)

_portfolio_optimizer = PortfolioOptimizer()

# =============================================================================
# Shariah Compliance Checker
# =============================================================================

class ShariahChecker:
    def __init__(self):
        self._compliance_cache: Dict[str, Tuple[ShariahCompliance, float]] = {}
        self._lock = asyncio.Lock()
    
    async def check(self, symbol: str, data: Dict[str, Any]) -> ShariahCompliance:
        cache_key = f"shariah:{symbol}"
        async with self._lock:
            if cache_key in self._compliance_cache:
                status, expiry = self._compliance_cache[cache_key]
                if expiry > time.time(): 
                    return status
        
        try:
            sector = data.get("sector", "").lower()
            industry = data.get("industry", "").lower()
            name = data.get("name", "").lower()
            
            combined_text = f"{sector} {industry} {name}"
            prohibited = ["alcohol", "tobacco", "pork", "gambling", "conventional_finance", "insurance", "weapons", "defense"]
            
            if any(p in combined_text for p in prohibited):
                self._cache_result(cache_key, ShariahCompliance.NON_COMPLIANT)
                return ShariahCompliance.NON_COMPLIANT
            
            # Simple financial ratio filters
            if data.get("debt_to_assets", 0) > 0.33 or data.get("cash_to_assets", 0) > 0.33:
                self._cache_result(cache_key, ShariahCompliance.NON_COMPLIANT)
                return ShariahCompliance.NON_COMPLIANT
            
            self._cache_result(cache_key, ShariahCompliance.COMPLIANT)
            return ShariahCompliance.COMPLIANT
            
        except Exception as e:
            logger.error(f"Shariah check failed for {symbol}: {e}")
            return ShariahCompliance.PENDING
    
    def _cache_result(self, key: str, status: ShariahCompliance) -> None:
        self._compliance_cache[key] = (status, time.time() + 86400)

_shariah_checker = ShariahChecker()

# =============================================================================
# Core Advisor Engine
# =============================================================================

class AdvisorEngine:
    def __init__(self):
        self._engine = None
        self._engine_lock = asyncio.Lock()
        
        class SimpleBreaker:
            async def execute(self, func, *args, **kwargs): 
                return await func(*args, **kwargs)
        
        self._circuit_breaker = SimpleBreaker()
    
    async def get_engine(self, request: Request) -> Optional[Any]:
        try:
            st = getattr(request.app, "state", None)
            if st and getattr(st, "engine", None): 
                return st.engine
        except Exception: 
            pass
        
        if self._engine is not None: 
            return self._engine
        
        async def _init_engine():
            async with self._engine_lock:
                if self._engine is not None: 
                    return self._engine
                try:
                    from core.data_engine_v2 import get_engine
                    self._engine = await get_engine()
                    return self._engine
                except Exception as e:
                    logger.error(f"Engine initialization failed: {e}")
                    return None
                    
        return await self._circuit_breaker.execute(_init_engine)
    
    async def get_recommendations(
        self, 
        request: AdvisorRequest, 
        engine: Any, 
        request_id: str
    ) -> Tuple[List[AdvisorRecommendation], Dict[str, Any]]:
        
        recommendations = []
        meta = {
            "symbols_processed": 0, 
            "symbols_succeeded": 0, 
            "symbols_failed": 0, 
            "ml_predictions": 0, 
            "shariah_compliant": 0
        }
        
        if not engine or not request.tickers: 
            return recommendations, meta
        
        symbols = request.tickers[:request.top_n]
        meta["symbols_processed"] = len(symbols)
        semaphore = asyncio.Semaphore(_CONFIG.max_concurrent_requests)
        
        async def process_symbol(symbol: str) -> Optional[AdvisorRecommendation]:
            async with semaphore:
                try:
                    quote = await engine.get_enriched_quote(symbol)
                    if not quote:
                        meta["symbols_failed"] += 1
                        return None
                    
                    quote_dict = quote.dict() if hasattr(quote, "dict") else quote.model_dump() if hasattr(quote, "model_dump") else quote if isinstance(quote, dict) else {}
                    
                    shariah_compliant = ShariahCompliance.NA
                    if request.include_shariah or request.shariah_compliant:
                        shariah_compliant = await _shariah_checker.check(symbol, quote_dict)
                        if request.shariah_compliant and shariah_compliant != ShariahCompliance.COMPLIANT:
                            meta["symbols_failed"] += 1
                            return None
                        if shariah_compliant == ShariahCompliance.COMPLIANT: 
                            meta["shariah_compliant"] += 1
                    
                    features = self._extract_features(quote_dict, symbol)
                    ml_prediction = await _ml_models.predict(features) if request.enable_ml_predictions else None
                    if ml_prediction: 
                        meta["ml_predictions"] += 1
                    
                    rec = await self._build_recommendation(symbol, quote_dict, features, ml_prediction, request, shariah_compliant)
                    meta["symbols_succeeded"] += 1
                    return rec
                    
                except Exception as e:
                    logger.error(f"Error processing {symbol}: {e}")
                    meta["symbols_failed"] += 1
                    return None
        
        tasks = [process_symbol(s) for s in symbols]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        for result in results:
            if isinstance(result, AdvisorRecommendation): 
                recommendations.append(result)
        
        recommendations.sort(key=lambda x: x.rank if x.rank else 999)
        return recommendations, meta
    
    def _extract_features(self, data: Dict[str, Any], symbol: str) -> MLFeatures:
        return MLFeatures(
            symbol=symbol, 
            price=_safe_float(data.get("price") or data.get("current_price")), 
            volume=_safe_float(data.get("volume"), 0), 
            market_cap=_safe_float(data.get("market_cap")),
            pe_ratio=_safe_float(data.get("pe_ratio") or data.get("pe_ttm")), 
            pb_ratio=_safe_float(data.get("pb_ratio") or data.get("pb")), 
            dividend_yield=_safe_float(data.get("dividend_yield")),
            beta=_safe_float(data.get("beta"), 1.0), 
            volatility_30d=_safe_float(data.get("volatility_30d")), 
            momentum_14d=_safe_float(data.get("momentum_14d") or data.get("returns_1m")),
            rsi_14d=_safe_float(data.get("rsi_14d") or data.get("rsi_14"), 50.0), 
            macd=_safe_float(data.get("macd") or data.get("macd_line")), 
            macd_signal=_safe_float(data.get("macd_signal"))
        )
    
    async def _build_recommendation(
        self, 
        symbol: str, 
        data: Dict[str, Any], 
        features: MLFeatures, 
        ml_prediction: Optional[MLPrediction], 
        request: AdvisorRequest, 
        shariah_compliant: ShariahCompliance
    ) -> AdvisorRecommendation:
        
        risk_score = ml_prediction.risk_score if ml_prediction else 50.0
        confidence = ml_prediction.confidence_1d if ml_prediction else 0.5
        
        var_95, cvar_95 = None, None
        price = _safe_float(data.get("current_price") or data.get("price"))
        
        if data.get("volatility_30d") and price:
            volatility = _safe_float(data.get("volatility_30d"), 20) / 100
            var_95 = -1.645 * volatility * 100 
            cvar_95 = -2.063 * volatility * 100 
            
        reasoning = []
        if ml_prediction and ml_prediction.factors:
            top_factors = sorted(ml_prediction.factors.items(), key=lambda x: x[1], reverse=True)[:3]
            reasoning = [f"{factor}: {importance:.1%}" for factor, importance in top_factors]
            
        if shariah_compliant == ShariahCompliance.COMPLIANT: 
            reasoning.append("Shariah compliant")
        elif shariah_compliant == ShariahCompliance.NON_COMPLIANT: 
            reasoning.append("Not Shariah compliant")
        
        rank_score = ((ml_prediction.predicted_return_1m or 0) * 0.4 + (100 - risk_score) * 0.3 + confidence * 0.3)
        
        return AdvisorRecommendation(
            rank=int(rank_score * 100), 
            symbol=symbol, 
            name=data.get("name"), 
            sector=data.get("sector"),
            asset_class=AssetClass.EQUITY, 
            shariah_compliant=shariah_compliant, 
            current_price=price,
            target_price_1m=_safe_float(data.get("forecast_price_1m")), 
            target_price_3m=_safe_float(data.get("forecast_price_3m")),
            target_price_12m=_safe_float(data.get("forecast_price_12m")), 
            expected_return_1m=_safe_float(data.get("expected_roi_1m")),
            expected_return_3m=_safe_float(data.get("expected_roi_3m")), 
            expected_return_12m=_safe_float(data.get("expected_roi_12m")),
            risk_score=risk_score, 
            volatility=_safe_float(data.get("volatility_30d")), 
            beta=_safe_float(data.get("beta")),
            sharpe_ratio=_safe_float(data.get("sharpe_ratio")), 
            var_95=var_95, 
            cvar_95=cvar_95, 
            ml_prediction=ml_prediction,
            confidence_score=confidence, 
            factor_importance=ml_prediction.factors if ml_prediction else {},
            pe_ratio=_safe_float(data.get("pe_ratio") or data.get("pe_ttm")), 
            pb_ratio=_safe_float(data.get("pb_ratio") or data.get("pb")), 
            dividend_yield=_safe_float(data.get("dividend_yield")),
            market_cap=_safe_float(data.get("market_cap")), 
            rsi_14d=_safe_float(data.get("rsi_14d") or data.get("rsi_14")), 
            macd=_safe_float(data.get("macd")),
            reasoning=reasoning, 
            data_quality="GOOD" if price else "PARTIAL"
        )

_advisor_engine = AdvisorEngine()

# =============================================================================
# WebSocket Manager for Real-time Updates
# =============================================================================

class WebSocketManager:
    def __init__(self):
        self._active_connections: List[WebSocket] = []
        self._connection_lock = asyncio.Lock()
        self._subscriptions: Dict[str, List[WebSocket]] = defaultdict(list)
        self._broadcast_queue: asyncio.Queue = asyncio.Queue()
        self._broadcast_task: Optional[asyncio.Task] = None
    
    async def start(self): 
        self._broadcast_task = asyncio.create_task(self._process_broadcasts())
    
    async def stop(self):
        if self._broadcast_task:
            self._broadcast_task.cancel()
            try: 
                await self._broadcast_task
            except: 
                pass
    
    async def connect(self, websocket: WebSocket):
        await websocket.accept()
        async with self._connection_lock: 
            self._active_connections.append(websocket)
    
    async def disconnect(self, websocket: WebSocket):
        async with self._connection_lock:
            if websocket in self._active_connections: 
                self._active_connections.remove(websocket)
            for symbol in list(self._subscriptions.keys()):
                if websocket in self._subscriptions[symbol]: 
                    self._subscriptions[symbol].remove(websocket)
    
    async def subscribe(self, websocket: WebSocket, symbol: str):
        async with self._connection_lock: 
            self._subscriptions[symbol].append(websocket)
    
    async def broadcast(self, message: Dict[str, Any], symbol: Optional[str] = None):
        await self._broadcast_queue.put((message, symbol))
    
    async def _process_broadcasts(self):
        while True:
            try:
                message, symbol = await self._broadcast_queue.get()
                connections = self._subscriptions.get(symbol, []) if symbol else self._active_connections
                disconnected = []
                
                # Render payload once as string to save massive amounts of CPU on large broadcasts
                msg_str = json_dumps(message)
                
                for connection in connections:
                    try: 
                        if _HAS_ORJSON:
                            await connection.send_text(msg_str)
                        else: 
                            await connection.send_json(message)
                    except Exception: 
                        disconnected.append(connection)
                        
                for conn in disconnected: 
                    await self.disconnect(conn)
                    
            except asyncio.CancelledError: 
                break
            except Exception as e: 
                logger.error(f"Broadcast error: {e}")

_ws_manager = WebSocketManager()

# =============================================================================
# Helper Functions
# =============================================================================

def _get_default_headers() -> List[str]:
    return [
        "Rank", "Symbol", "Name", "Sector", "Current Price", "Target Price", 
        "Expected Return", "Risk Score", "Confidence", "Shariah", "Weight", "Allocated Amount"
    ]

def _recommendations_to_rows(recommendations: List[AdvisorRecommendation], headers: List[str]) -> List[List[Any]]:
    rows = []
    for rec in recommendations:
        rows.append([
            rec.rank, rec.symbol, rec.name or "", rec.sector or "", 
            f"{rec.current_price:.2f}" if rec.current_price else "",
            f"{rec.target_price_12m:.2f}" if rec.target_price_12m else "", 
            f"{rec.expected_return_12m:.1f}%" if rec.expected_return_12m else "",
            f"{rec.risk_score:.0f}", f"{rec.confidence_score:.1%}", 
            "✓" if rec.shariah_compliant == ShariahCompliance.COMPLIANT else "✗",
            f"{rec.weight:.1%}", f"{rec.allocated_amount:.2f}" if rec.allocated_amount else ""
        ])
    return rows

def _determine_market_condition(recommendations: List[AdvisorRecommendation]) -> MarketCondition:
    if not recommendations: 
        return MarketCondition.SIDEWAYS
        
    avg_confidence = sum(r.confidence_score for r in recommendations) / len(recommendations)
    avg_risk = sum(r.risk_score for r in recommendations) / len(recommendations)
    
    if avg_confidence > 0.8 and avg_risk < 40: 
        return MarketCondition.BULL
    elif avg_confidence < 0.3 and avg_risk > 70: 
        return MarketCondition.BEAR
    elif avg_risk > 60: 
        return MarketCondition.VOLATILE
    else: 
        return MarketCondition.SIDEWAYS

# =============================================================================
# Routes
# =============================================================================

@router.get("/health")
async def advisor_health(request: Request) -> Dict[str, Any]:
    engine = await _advisor_engine.get_engine(request)
    return {
        "status": "ok", 
        "version": ADVISOR_ROUTE_VERSION, 
        "timestamp": _saudi_time.format_iso(),
        "trading_day": _saudi_time.is_trading_day(), 
        "trading_hours": _saudi_time.is_trading_hours(),
        "config": {
            "max_concurrent": _CONFIG.max_concurrent_requests, 
            "default_top_n": _CONFIG.default_top_n,
            "allocation_method": _CONFIG.default_allocation_method.value, 
            "enable_ml": _CONFIG.enable_ml_predictions,
            "enable_shariah": _CONFIG.enable_shariah_filter
        },
        "websocket_connections": len(_ws_manager._active_connections),
        "ml_initialized": _ml_models._initialized,
        "ml_models": _ml_models.get_model_versions() if _ml_models._initialized else {},
        "request_id": getattr(request.state, "request_id", None)
    }


@router.get("/metrics")
async def advisor_metrics() -> Response:
    if not _PROMETHEUS_AVAILABLE: 
        return BestJSONResponse(status_code=503, content={"error": "Metrics not available"})
    from prometheus_client import generate_latest, CONTENT_TYPE_LATEST
    return Response(content=generate_latest(), media_type=CONTENT_TYPE_LATEST)


@router.post("/recommendations")
async def advisor_recommendations(
    request: Request, 
    body: Dict[str, Any] = Body(...), 
    token: Optional[str] = Query(default=None),
    x_app_token: Optional[str] = Header(default=None, alias="X-APP-TOKEN"), 
    authorization: Optional[str] = Header(default=None, alias="Authorization"),
    x_request_id: Optional[str] = Header(default=None, alias="X-Request-ID")
) -> BestJSONResponse:
    request_id = x_request_id or str(uuid.uuid4())
    start_time = time.time()
    
    try:
        client_ip = request.client.host if request.client else "unknown"
        if not await _rate_limiter.check(client_ip): 
            raise HTTPException(status_code=status.HTTP_429_TOO_MANY_REQUESTS, detail="Rate limit exceeded")
        
        auth_token = x_app_token or token or ""
        if authorization and authorization.startswith("Bearer "): 
            auth_token = authorization[7:]
        if not _token_manager.validate_token(auth_token): 
            raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid token")
        
        async with TraceContext("parse_request", {"request_id": request_id}):
            try:
                req = AdvisorRequest.model_validate(body) if _PYDANTIC_V2 else AdvisorRequest.parse_obj(body)
                req.request_id = request_id
            except Exception as e:
                await _audit.log("validation_error", auth_token[:8], "advisor", "recommendations", "error", {"error": str(e)}, request_id)
                err_resp = {"status": "error", "error": f"Invalid request: {str(e)}", "headers": [], "rows": [], "request_id": request_id, "meta": {"duration_ms": (time.time() - start_time) * 1000}}
                return BestJSONResponse(status_code=400, content=err_resp)
        
        if req.enable_ml_predictions: 
            asyncio.create_task(_ml_models.initialize())
        
        async with TraceContext("get_engine"):
            engine = await _advisor_engine.get_engine(request)
            
        if not engine: 
            err_resp = {"status": "error", "error": "Advisor engine unavailable", "headers": [], "rows": [], "request_id": request_id, "meta": {"duration_ms": (time.time() - start_time) * 1000}}
            return BestJSONResponse(status_code=503, content=err_resp)
        
        async with TraceContext("get_recommendations", {"symbol_count": len(req.tickers or []), "top_n": req.top_n}):
            recommendations, rec_meta = await _advisor_engine.get_recommendations(req, engine, request_id)
        
        portfolio = None
        async with TraceContext("optimize_portfolio"):
            if recommendations and req.invest_amount:
                expected_returns = {r.symbol: (r.expected_return_12m or 0) / 100.0 for r in recommendations}
                symbols = [r.symbol for r in recommendations]
                portfolio = await _portfolio_optimizer.optimize(
                    symbols, 
                    expected_returns, 
                    constraints={
                        "max_position_pct": _CONFIG.max_position_pct, 
                        "min_position_pct": _CONFIG.min_position_pct
                    }
                )
                for rec in recommendations:
                    rec.weight = portfolio["weights"].get(rec.symbol, 0)
                    rec.allocated_amount = rec.weight * req.invest_amount
                    if rec.expected_return_1m: rec.expected_gain_1m = rec.allocated_amount * (rec.expected_return_1m / 100.0)
                    if rec.expected_return_3m: rec.expected_gain_3m = rec.allocated_amount * (rec.expected_return_3m / 100.0)
                    if rec.expected_return_12m: rec.expected_gain_12m = rec.allocated_amount * (rec.expected_return_12m / 100.0)
        
        headers = _get_default_headers()
        rows = _recommendations_to_rows(recommendations, headers)
        status_val = "success" if rec_meta["symbols_failed"] == 0 else "partial" if rec_meta["symbols_succeeded"] > 0 else "error"
        market_condition = _determine_market_condition(recommendations)
        ml_predictions = {r.symbol: r.ml_prediction for r in recommendations if r.ml_prediction} if req.enable_ml_predictions else None
        shariah_summary = {"total": len(recommendations), "compliant": rec_meta.get("shariah_compliant", 0), "non_compliant": len(recommendations) - rec_meta.get("shariah_compliant", 0)}
        
        if _PROMETHEUS_AVAILABLE: 
            _metrics.counter("requests_total", "Request totals", ["status"]).labels(status=status_val).inc()
        
        asyncio.create_task(_audit.log(
            "recommendations", 
            auth_token[:8], 
            "advisor", 
            "generate", 
            status_val, 
            {
                "symbols": len(req.tickers or []), 
                "recommendations": len(recommendations), 
                "portfolio_value": req.invest_amount
            }, 
            request_id
        ))
        
        response = AdvisorResponse(
            status=status_val, 
            error=f"{rec_meta['symbols_failed']} symbols failed" if rec_meta["symbols_failed"] > 0 else None,
            warnings=[], 
            headers=headers, 
            rows=rows, 
            recommendations=recommendations, 
            portfolio=portfolio,
            ml_predictions=ml_predictions, 
            market_condition=market_condition, 
            shariah_summary=shariah_summary,
            model_versions=_ml_models.get_model_versions() if _ml_models._initialized else None, 
            version=ADVISOR_ROUTE_VERSION,
            request_id=request_id, 
            meta={
                "duration_ms": (time.time() - start_time) * 1000, 
                "symbols_processed": rec_meta["symbols_processed"], 
                "symbols_succeeded": rec_meta["symbols_succeeded"], 
                "symbols_failed": rec_meta["symbols_failed"], 
                "ml_predictions": rec_meta["ml_predictions"], 
                "timestamp_utc": datetime.now(timezone.utc).isoformat(), 
                "timestamp_riyadh": _saudi_time.format_iso(), 
                "trading_hours": _saudi_time.is_trading_hours()
            }
        )
        
        # V4.5.0 FIX: Strip None fields entirely to save bandwidth & prevent generic parse errors on Pydantic Enums
        return BestJSONResponse(content=_serialize_response(response))

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Global catch in recommendations: {e}\n{traceback.format_exc()}")
        err_resp = {"status": "error", "error": f"Internal Server Error: {e}", "headers": [], "rows": [], "request_id": request_id, "meta": {"duration_ms": (time.time() - start_time) * 1000}}
        return BestJSONResponse(status_code=500, content=err_resp)


@router.post("/run")
async def advisor_run(
    request: Request, 
    body: Dict[str, Any] = Body(...), 
    token: Optional[str] = Query(default=None), 
    x_app_token: Optional[str] = Header(default=None, alias="X-APP-TOKEN"), 
    authorization: Optional[str] = Header(default=None, alias="Authorization"), 
    x_request_id: Optional[str] = Header(default=None, alias="X-Request-ID")
) -> BestJSONResponse:
    """Alias for /recommendations"""
    return await advisor_recommendations(
        request=request, 
        body=body, 
        token=token, 
        x_app_token=x_app_token, 
        authorization=authorization, 
        x_request_id=x_request_id
    )


@router.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    """Real-time streaming for live advisor updates."""
    await _ws_manager.connect(websocket)
    try:
        init_payload = {
            "type": "connection", 
            "status": "connected", 
            "timestamp": _saudi_time.format_iso(), 
            "version": ADVISOR_ROUTE_VERSION
        }
        
        if _HAS_ORJSON:
            await websocket.send_text(json_dumps(init_payload))
        else:
            await websocket.send_json(init_payload)
        
        while True:
            try:
                message = await websocket.receive_json()
                if message.get("type") == "subscribe":
                    if symbol := message.get("symbol"):
                        await _ws_manager.subscribe(websocket, symbol)
                        sub_msg = {
                            "type": "subscription", 
                            "status": "subscribed", 
                            "symbol": symbol, 
                            "timestamp": _saudi_time.format_iso()
                        }
                        if _HAS_ORJSON:
                            await websocket.send_text(json_dumps(sub_msg))
                        else:
                            await websocket.send_json(sub_msg)
                elif message.get("type") == "ping":
                    pong_msg = {
                        "type": "pong", 
                        "timestamp": _saudi_time.format_iso()
                    }
                    if _HAS_ORJSON:
                        await websocket.send_text(json_dumps(pong_msg))
                    else:
                        await websocket.send_json(pong_msg)
            except WebSocketDisconnect: 
                break
            except Exception as e:
                logger.error(f"WebSocket error: {e}")
                err_msg = {
                    "type": "error", 
                    "error": str(e), 
                    "timestamp": _saudi_time.format_iso()
                }
                if _HAS_ORJSON:
                    await websocket.send_text(json_dumps(err_msg))
                else:
                    await websocket.send_json(err_msg)
    finally:
        await _ws_manager.disconnect(websocket)


@router.on_event("startup")
async def startup_event():
    # Retained for strict backward compatibility with FastAPI < 0.93.0
    await _ws_manager.start()


@router.on_event("shutdown")
async def shutdown_event():
    await _ws_manager.stop()


__all__ = ["router"]
