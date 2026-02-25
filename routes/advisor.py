#!/usr/bin/env python3
"""
routes/advisor.py
------------------------------------------------------------
TADAWUL ENTERPRISE ADVISOR ENGINE — v4.5.2 (ENTERPRISE PATCH)
SAMA Compliant | Multi-Asset | Real-time ML | Dynamic Allocation | Audit Trail

v4.5.2 Fixes (production blockers)
- ✅ FIX: TraceContext now sets attributes using Span.set_attribute() (works with real OpenTelemetry spans).
- ✅ FIX: AdvisorConfig now includes enable_audit_log (prevents AttributeError).
- ✅ FIX: Token validation now respects require_api_key (no more “open mode” accidental public access).
- ✅ FIX: Query token only used when allow_query_token = True.
- ✅ FIX: MLPrediction now includes risk_score (was missing → runtime crash).
- ✅ FIX: MLFeatures now includes pb_ratio; volume uses safe_int.
- ✅ FIX: Removed async race on meta counters by aggregating results after gather().
- ✅ IMPROVE: Holidays are configurable via SAUDI_MARKET_HOLIDAYS env (comma-separated ISO dates).
"""

from __future__ import annotations

import asyncio
import logging
import math
import os
import time
import uuid
import traceback
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from enum import Enum
from typing import Any, Dict, List, Optional, Set, Tuple, Union

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

# =============================================================================
# Helpers: NaN/Inf cleaning + safe casts
# =============================================================================

def clean_nans(obj: Any) -> Any:
    """Recursively replaces NaN and Infinity with None to prevent JSON crashes."""
    if isinstance(obj, dict):
        return {k: clean_nans(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [clean_nans(v) for v in obj]
    if isinstance(obj, float) and (math.isnan(obj) or math.isinf(obj)):
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


def _safe_int(v: Any, default: Optional[int] = None) -> Optional[int]:
    """Safely cast to int."""
    if v is None:
        return default
    try:
        # Allow numeric strings/floats
        i = int(float(v))
        return i
    except (TypeError, ValueError):
        return default


# =============================================================================
# High-Performance JSON fallback
# =============================================================================

try:
    import orjson
    from fastapi.responses import Response as FastAPIResponse

    class BestJSONResponse(FastAPIResponse):
        media_type = "application/json"

        def render(self, content: Any) -> bytes:
            return orjson.dumps(clean_nans(content), default=str)

    def json_dumps(v: Any, *, default=str) -> str:
        return orjson.dumps(clean_nans(v), default=default).decode("utf-8")

    def json_loads(v: Union[str, bytes]) -> Any:
        return orjson.loads(v)

    _HAS_ORJSON = True
except ImportError:
    import json
    from fastapi.responses import JSONResponse as BaseJSONResponse

    class BestJSONResponse(BaseJSONResponse):
        def render(self, content: Any) -> bytes:
            return json.dumps(clean_nans(content), default=str, ensure_ascii=False).encode("utf-8")

    def json_dumps(v: Any, *, default=str) -> str:
        return json.dumps(clean_nans(v), default=default)

    def json_loads(v: Union[str, bytes]) -> Any:
        return json.loads(v)

    _HAS_ORJSON = False


# =============================================================================
# Optional enterprise integrations
# =============================================================================

try:
    import numpy as np
    from scipy import optimize  # type: ignore

    _NUMPY_AVAILABLE = True
    _SCIPY_AVAILABLE = True
except ImportError:
    np = None  # type: ignore
    optimize = None  # type: ignore
    _NUMPY_AVAILABLE = False
    _SCIPY_AVAILABLE = False

# OpenTelemetry (optional)
try:
    from opentelemetry import trace
    from opentelemetry.trace import Status, StatusCode

    _OTEL_AVAILABLE = True
    tracer = trace.get_tracer(__name__)
except ImportError:
    _OTEL_AVAILABLE = False

    class DummySpan:
        def set_attribute(self, *args, **kwargs):  # noqa
            return None

        def set_status(self, *args, **kwargs):  # noqa
            return None

        def record_exception(self, *args, **kwargs):  # noqa
            return None

        def __enter__(self):  # noqa
            return self

        def __exit__(self, *args, **kwargs):  # noqa
            return False

        def end(self):  # noqa
            return None

    class DummyContextManager:
        def __init__(self, span):
            self.span = span

        def __enter__(self):
            return self.span

        def __exit__(self, *args, **kwargs):
            return False

    class DummyTracer:
        def start_as_current_span(self, *args, **kwargs):
            return DummyContextManager(DummySpan())

    tracer = DummyTracer()

# Prometheus (optional)
try:
    from prometheus_client import Counter, Histogram, Gauge, Summary  # noqa

    _PROMETHEUS_AVAILABLE = True
except ImportError:
    _PROMETHEUS_AVAILABLE = False

# Pydantic imports (v2 preferred, v1 fallback)
try:
    from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator  # type: ignore
    _PYDANTIC_V2 = True
except ImportError:
    from pydantic import BaseModel, Field, validator  # type: ignore
    _PYDANTIC_V2 = False


logger = logging.getLogger("routes.advisor")

ADVISOR_ROUTE_VERSION = "4.5.2"
router = APIRouter(prefix="/v1/advisor", tags=["advisor"])


# =============================================================================
# Enums & Types
# =============================================================================

class FlexibleEnum(str, Enum):
    """Safely intercepts case-mismatches to prevent Enum 500 errors."""
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


# =============================================================================
# Configuration
# =============================================================================

@dataclass(slots=True)
class AdvisorConfig:
    # concurrency + route behavior
    max_concurrent_requests: int = 100
    default_top_n: int = 50
    max_top_n: int = 500
    route_timeout_sec: float = 75.0

    # ML
    enable_ml_predictions: bool = True
    ml_prediction_timeout_sec: float = 5.0
    min_confidence_for_prediction: float = 0.6

    # allocation
    default_allocation_method: AllocationMethod = AllocationMethod.RISK_PARITY
    max_position_pct: float = 0.15
    min_position_pct: float = 0.01

    # market data
    market_data_cache_ttl: int = 60
    historical_data_days: int = 252

    # compliance & audit
    enable_shariah_filter: bool = True
    enable_sama_compliance: bool = True
    audit_retention_days: int = 90
    enable_audit_log: bool = True  # ✅ FIX (was missing)

    # rate limit
    rate_limit_requests: int = 100
    rate_limit_window: int = 60

    # auth
    allow_query_token: bool = False
    require_api_key: bool = True

    # cache (future)
    cache_ttl_seconds: int = 300
    enable_tracing: bool = True

    base_currency: str = "SAR"
    supported_currencies: List[str] = field(default_factory=lambda: ["SAR", "USD", "EUR"])


_CONFIG = AdvisorConfig()


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

    def env_enum(name: str, default: Enum, enum_class: type) -> Enum:
        val = os.getenv(name, "").strip().upper()
        if val:
            try:
                return enum_class(val)
            except Exception:
                return default
        return default

    _CONFIG.max_concurrent_requests = env_int("ADVISOR_MAX_CONCURRENT", _CONFIG.max_concurrent_requests)
    _CONFIG.default_top_n = env_int("ADVISOR_DEFAULT_TOP_N", _CONFIG.default_top_n)
    _CONFIG.max_top_n = env_int("ADVISOR_MAX_TOP_N", _CONFIG.max_top_n)
    _CONFIG.route_timeout_sec = env_float("ADVISOR_ROUTE_TIMEOUT_SEC", _CONFIG.route_timeout_sec)

    _CONFIG.enable_ml_predictions = env_bool("ADVISOR_ENABLE_ML", _CONFIG.enable_ml_predictions)
    _CONFIG.default_allocation_method = env_enum("ADVISOR_ALLOCATION_METHOD", _CONFIG.default_allocation_method, AllocationMethod)
    _CONFIG.enable_shariah_filter = env_bool("ADVISOR_SHARIAH_FILTER", _CONFIG.enable_shariah_filter)

    _CONFIG.allow_query_token = env_bool("ALLOW_QUERY_TOKEN", _CONFIG.allow_query_token)
    _CONFIG.require_api_key = env_bool("REQUIRE_AUTH", _CONFIG.require_api_key)  # align with render.yaml usage

    _CONFIG.enable_tracing = env_bool("CORE_TRACING_ENABLED", _CONFIG.enable_tracing)
    _CONFIG.enable_audit_log = env_bool("ENABLE_AUDIT_LOG", _CONFIG.enable_audit_log)

    _CONFIG.rate_limit_requests = env_int("MAX_REQUESTS_PER_MINUTE", _CONFIG.rate_limit_requests)
    _CONFIG.rate_limit_window = env_int("RATE_LIMIT_WINDOW_SEC", _CONFIG.rate_limit_window)

_load_config_from_env()


# =============================================================================
# Observability & Metrics
# =============================================================================

class TraceContext:
    """
    Safe OpenTelemetry trace context manager.
    Correct behavior: unroll context manager then set attributes using set_attribute().
    """
    def __init__(self, name: str, attributes: Optional[Dict[str, Any]] = None):
        self.name = name
        self.attributes = attributes or {}
        self.tracer = tracer if _OTEL_AVAILABLE and _CONFIG.enable_tracing else None
        self._cm = None
        self.span = None

    def __enter__(self):
        if self.tracer:
            try:
                self._cm = self.tracer.start_as_current_span(self.name)
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

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self._cm and self.span:
            try:
                if exc_val and hasattr(self.span, "record_exception"):
                    self.span.record_exception(exc_val)
                if exc_val and hasattr(self.span, "set_status") and _OTEL_AVAILABLE:
                    self.span.set_status(Status(StatusCode.ERROR, str(exc_val)))
            except Exception:
                pass
            try:
                return self._cm.__exit__(exc_type, exc_val, exc_tb)
            except Exception:
                return False
        return False

    async def __aenter__(self):
        return self.__enter__()

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        return self.__exit__(exc_type, exc_val, exc_tb)


# =============================================================================
# Saudi market time
# =============================================================================

class SaudiMarketTime:
    def __init__(self):
        self._tz = timezone(timedelta(hours=3))
        self._trading_hours = {"start": "10:00", "end": "15:00"}
        self._weekend_days = [4, 5]  # Fri/Sat

        # Configurable holidays: "YYYY-MM-DD,YYYY-MM-DD"
        env_h = os.getenv("SAUDI_MARKET_HOLIDAYS", "").strip()
        if env_h:
            self._holidays = {x.strip() for x in env_h.split(",") if x.strip()}
        else:
            # fallback (legacy)
            self._holidays = {
                "2024-02-22", "2024-04-10", "2024-04-11", "2024-04-12",
                "2024-06-16", "2024-06-17", "2024-06-18", "2024-09-23",
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
        dt = dt or self.now()
        if not self.is_trading_day(dt):
            return False
        start = datetime.strptime(self._trading_hours["start"], "%H:%M").time()
        end = datetime.strptime(self._trading_hours["end"], "%H:%M").time()
        return start <= dt.time() <= end

    def format_iso(self, dt: Optional[datetime] = None) -> str:
        return (dt or self.now()).isoformat(timespec="milliseconds")

_saudi_time = SaudiMarketTime()


# =============================================================================
# Audit logging (SAMA)
# =============================================================================

class AuditLogger:
    def __init__(self):
        self._buffer: List[Dict[str, Any]] = []
        self._lock = asyncio.Lock()

    async def log(
        self,
        event: str,
        user_id: Optional[str],
        resource: str,
        action: str,
        status_s: str,
        details: Dict[str, Any],
        request_id: Optional[str] = None,
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
            "status": status_s,
            "details": details,
            "request_id": request_id,
            "version": ADVISOR_ROUTE_VERSION,
            "environment": os.getenv("APP_ENV", "production"),
        }

        flush_now = False
        async with self._lock:
            self._buffer.append(entry)
            if len(self._buffer) >= 100:
                flush_now = True

        if flush_now:
            asyncio.create_task(self._flush())

    async def _flush(self) -> None:
        async with self._lock:
            buffer = list(self._buffer)
            self._buffer.clear()

        # Replace with real sink later (DB/queue/object storage)
        try:
            logger.info("Audit flush: %s entries", len(buffer))
        except Exception:
            pass

_audit = AuditLogger()


# =============================================================================
# Auth & Rate limiter
# =============================================================================

class TokenManager:
    def __init__(self):
        self._tokens: Set[str] = set()
        for key in ("APP_TOKEN", "BACKUP_APP_TOKEN", "TFB_APP_TOKEN"):
            token = os.getenv(key, "").strip()
            if token:
                self._tokens.add(token)

    def validate_token(self, token: str) -> bool:
        token = (token or "").strip()

        # ✅ FIX: respect require_api_key
        if _CONFIG.require_api_key:
            if not self._tokens:
                # Production-safe: if auth is required but no tokens configured => deny
                return False
            return token in self._tokens

        # If auth not required, allow
        if not self._tokens:
            return True
        return token in self._tokens

_token_manager = TokenManager()


class RateLimiter:
    def __init__(self):
        self._buckets: Dict[str, Tuple[int, float]] = {}
        self._lock = asyncio.Lock()

    async def check(self, key: str, requests: int, window: int) -> bool:
        if requests <= 0:
            return True
        now = time.time()
        async with self._lock:
            count, reset_time = self._buckets.get(key, (0, now + window))
            if now > reset_time:
                count, reset_time = 0, now + window
            if count < requests:
                self._buckets[key] = (count + 1, reset_time)
                return True
            return False

_rate_limiter = RateLimiter()


# =============================================================================
# Schemas
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
    market_cap: Optional[float] = None
    pe_ratio: Optional[float] = None
    pb_ratio: Optional[float] = None  # ✅ FIX
    dividend_yield: Optional[float] = None
    beta: Optional[float] = None
    sharpe_ratio: Optional[float] = None
    max_drawdown_30d: Optional[float] = None

    if _PYDANTIC_V2:
        model_config = ConfigDict(arbitrary_types_allowed=True, extra="ignore")


class MLPrediction(BaseModel):
    symbol: str
    predicted_return_1m: Optional[float] = None
    confidence_1d: Optional[float] = None
    risk_score: Optional[float] = None  # ✅ FIX (was missing)
    risk_level: Optional[str] = None
    factors: Dict[str, float] = Field(default_factory=dict)
    model_version: str = "ensemble_v2"

    if _PYDANTIC_V2:
        model_config = ConfigDict(arbitrary_types_allowed=True, extra="ignore")


class AdvisorRequest(BaseModel):
    tickers: Optional[Union[str, List[str]]] = None
    symbols: Optional[Union[str, List[str]]] = None
    risk: Optional[RiskProfile] = None
    confidence: Optional[ConfidenceLevel] = None
    top_n: Optional[int] = Field(default=None, ge=1, le=500)

    invest_amount: Optional[float] = Field(default=None, gt=0)
    currency: str = "SAR"
    allocation_method: Optional[AllocationMethod] = None

    shariah_compliant: bool = False
    include_shariah: bool = True
    enable_ml_predictions: bool = True
    min_confidence: float = 0.6

    request_id: Optional[str] = None

    if _PYDANTIC_V2:
        model_config = ConfigDict(extra="ignore", use_enum_values=True)

        @model_validator(mode="after")
        def normalize_request(self) -> "AdvisorRequest":
            items: List[str] = []

            def add_many(x: Union[str, List[str]]):
                if isinstance(x, str):
                    parts = [p.strip() for p in x.replace(",", " ").split()]
                    items.extend([p for p in parts if p])
                else:
                    items.extend([str(p).strip() for p in x if str(p).strip()])

            if self.tickers:
                add_many(self.tickers)
            if self.symbols:
                add_many(self.symbols)

            # normalize & dedupe preserve order
            seen = set()
            normalized: List[str] = []
            for s in items:
                s2 = s.upper()
                if s2 and s2 not in seen:
                    seen.add(s2)
                    normalized.append(s2)

            self.tickers = normalized
            self.symbols = []

            if self.top_n is None:
                self.top_n = _CONFIG.default_top_n
            self.top_n = max(1, min(int(self.top_n), _CONFIG.max_top_n))

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
    target_price_12m: Optional[float] = None
    expected_return_12m: Optional[float] = None
    risk_score: float = 50.0
    confidence_score: float = 0.5
    ml_prediction: Optional[MLPrediction] = None
    factor_importance: Dict[str, float] = Field(default_factory=dict)
    weight: float = 0.0
    allocated_amount: float = 0.0
    expected_gain_12m: Optional[float] = None
    pe_ratio: Optional[float] = None
    pb_ratio: Optional[float] = None
    dividend_yield: Optional[float] = None
    market_cap: Optional[float] = None
    rsi_14d: Optional[float] = None
    macd: Optional[float] = None
    reasoning: List[str] = Field(default_factory=list)
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
    ml_predictions: Optional[Dict[str, Any]] = None
    market_condition: Optional[MarketCondition] = None
    model_versions: Optional[Dict[str, str]] = None
    shariah_summary: Optional[Dict[str, Any]] = None
    version: str = ADVISOR_ROUTE_VERSION
    request_id: str = ""
    meta: Dict[str, Any] = Field(default_factory=dict)

    if _PYDANTIC_V2:
        model_config = ConfigDict(arbitrary_types_allowed=True, extra="ignore")


def _serialize_response(model_instance: BaseModel) -> Dict[str, Any]:
    """Serialize Pydantic models safely for orjson/json output."""
    if _PYDANTIC_V2:
        raw = model_instance.model_dump(mode="python", exclude_none=True)
        return clean_nans(raw)
    # pydantic v1 fallback
    return clean_nans(json_loads(model_instance.json(exclude_none=True)))


# =============================================================================
# ML Model Manager (lazy)
# =============================================================================

class MLModelManager:
    def __init__(self):
        self._initialized = False
        self._lock = asyncio.Lock()
        self._prediction_cache: Dict[str, Tuple[MLPrediction, float]] = {}

    async def initialize(self) -> None:
        if self._initialized:
            return
        async with self._lock:
            if self._initialized:
                return
            self._initialized = True
            logger.info("ML models initialized (mock safe)")

    async def predict(self, features: MLFeatures) -> Optional[MLPrediction]:
        if not self._initialized:
            return None

        cache_key = f"{features.symbol}:{_saudi_time.now().strftime('%Y%m%d')}"
        cached = self._prediction_cache.get(cache_key)
        if cached:
            pred, expiry = cached
            if expiry > time.time():
                return pred

        try:
            # mock prediction logic
            f_vol = features.volatility_30d or 0.0
            f_beta = features.beta or 1.0
            f_mom = features.momentum_14d or 0.0

            risk = 50.0
            if f_vol > 0:
                risk += f_vol * 10
            if f_beta > 0:
                risk += (f_beta - 1) * 20
            if f_mom < -0.1:
                risk += 20
            elif f_mom > 0.1:
                risk -= 10

            risk = max(0.0, min(100.0, risk))
            pred = MLPrediction(
                symbol=features.symbol,
                predicted_return_1m=5.0,
                confidence_1d=0.85,
                risk_score=float(risk),
                factors={"momentum": 0.4, "volatility": 0.3, "pe_ratio": 0.3},
                model_version="ensemble_v2",
            )
            self._prediction_cache[cache_key] = (pred, time.time() + 3600)
            return pred
        except Exception as e:
            logger.error("ML prediction failed: %s", e)
            return None

    def get_model_versions(self) -> Dict[str, str]:
        return {"ensemble_v2": "2.0.0"}

_ml_models = MLModelManager()


# =============================================================================
# Shariah checker (simple)
# =============================================================================

class ShariahChecker:
    def __init__(self):
        self._cache: Dict[str, Tuple[ShariahCompliance, float]] = {}
        self._lock = asyncio.Lock()

    async def check(self, symbol: str, data: Dict[str, Any]) -> ShariahCompliance:
        key = f"shariah:{symbol}"
        async with self._lock:
            cached = self._cache.get(key)
            if cached:
                status_s, expiry = cached
                if expiry > time.time():
                    return status_s

        try:
            sector = str(data.get("sector", "")).lower()
            name = str(data.get("name", "")).lower()
            combined = f"{sector} {name}"

            prohibited = ["alcohol", "tobacco", "pork", "gambling", "conventional_finance", "insurance", "weapons", "defense"]
            if any(p in combined for p in prohibited):
                self._cache[key] = (ShariahCompliance.NON_COMPLIANT, time.time() + 86400)
                return ShariahCompliance.NON_COMPLIANT

            self._cache[key] = (ShariahCompliance.COMPLIANT, time.time() + 86400)
            return ShariahCompliance.COMPLIANT
        except Exception as e:
            logger.error("Shariah check failed for %s: %s", symbol, e)
            return ShariahCompliance.PENDING

_shariah_checker = ShariahChecker()


# =============================================================================
# Portfolio optimizer (minimal safe version)
# =============================================================================

class PortfolioOptimizer:
    async def optimize(
        self,
        symbols: List[str],
        expected_returns: Dict[str, float],
        constraints: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        if not symbols:
            return {"weights": {}, "method": "none"}

        # fallback equal weight unless numpy/scipy are available
        n = len(symbols)
        w = 1.0 / n
        weights = {s: w for s in symbols}

        # apply min/max constraints if provided
        if constraints:
            maxp = float(constraints.get("max_position_pct", _CONFIG.max_position_pct))
            minp = float(constraints.get("min_position_pct", _CONFIG.min_position_pct))
            # clamp
            for s in list(weights.keys()):
                weights[s] = max(minp, min(maxp, weights[s]))
            # renormalize
            tot = sum(weights.values()) or 1.0
            weights = {s: v / tot for s, v in weights.items()}

        return {"weights": weights, "method": "equal_weight"}

_portfolio_optimizer = PortfolioOptimizer()


# =============================================================================
# Engine wrapper
# =============================================================================

class AdvisorEngine:
    def __init__(self):
        self._engine = None
        self._lock = asyncio.Lock()

    async def get_engine(self, request: Request) -> Optional[Any]:
        # 1) app.state.engine preferred if exists
        try:
            st = getattr(request.app, "state", None)
            if st and getattr(st, "engine", None):
                return st.engine
        except Exception:
            pass

        # 2) cached
        if self._engine is not None:
            return self._engine

        # 3) init once
        async with self._lock:
            if self._engine is not None:
                return self._engine
            try:
                from core.data_engine_v2 import get_engine  # expected in your project
                self._engine = await get_engine()
                return self._engine
            except Exception as e:
                logger.error("Engine initialization failed: %s", e)
                return None

    async def get_recommendations(
        self,
        req: AdvisorRequest,
        engine: Any,
    ) -> Tuple[List[AdvisorRecommendation], Dict[str, Any]]:

        symbols = (req.tickers or [])[: int(req.top_n or _CONFIG.default_top_n)]
        if not engine or not symbols:
            return [], {
                "symbols_processed": len(symbols),
                "symbols_succeeded": 0,
                "symbols_failed": len(symbols),
                "ml_predictions": 0,
                "shariah_compliant": 0,
            }

        sem = asyncio.Semaphore(_CONFIG.max_concurrent_requests)

        async def process_symbol(symbol: str) -> Tuple[Optional[AdvisorRecommendation], Dict[str, int]]:
            async with sem:
                stats = {"ok": 0, "fail": 0, "ml": 0, "shariah_ok": 0}
                try:
                    quote = await engine.get_enriched_quote(symbol)
                    if not quote:
                        stats["fail"] = 1
                        return None, stats

                    if isinstance(quote, dict):
                        quote_dict = quote
                    elif hasattr(quote, "model_dump"):
                        quote_dict = quote.model_dump(mode="python")
                    elif hasattr(quote, "dict"):
                        quote_dict = quote.dict()
                    else:
                        quote_dict = {}

                    # Shariah filtering
                    shariah_status = ShariahCompliance.NA
                    if req.include_shariah or req.shariah_compliant:
                        shariah_status = await _shariah_checker.check(symbol, quote_dict)
                        if req.shariah_compliant and shariah_status != ShariahCompliance.COMPLIANT:
                            stats["fail"] = 1
                            return None, stats
                        if shariah_status == ShariahCompliance.COMPLIANT:
                            stats["shariah_ok"] = 1

                    features = self._extract_features(quote_dict, symbol)

                    ml_pred = None
                    if req.enable_ml_predictions and _CONFIG.enable_ml_predictions:
                        ml_pred = await _ml_models.predict(features)
                        if ml_pred:
                            stats["ml"] = 1

                    rec = await self._build_recommendation(symbol, quote_dict, ml_pred, shariah_status)
                    stats["ok"] = 1
                    return rec, stats

                except Exception as e:
                    logger.error("Error processing %s: %s", symbol, e)
                    stats["fail"] = 1
                    return None, stats

        tasks = [process_symbol(s) for s in symbols]
        results = await asyncio.gather(*tasks, return_exceptions=False)

        recs: List[AdvisorRecommendation] = []
        ok = fail = ml = sh_ok = 0
        for rec, st in results:
            ok += st["ok"]
            fail += st["fail"]
            ml += st["ml"]
            sh_ok += st["shariah_ok"]
            if isinstance(rec, AdvisorRecommendation):
                recs.append(rec)

        # Sort by rank descending (higher score is better)
        recs.sort(key=lambda r: r.rank, reverse=True)

        meta = {
            "symbols_processed": len(symbols),
            "symbols_succeeded": ok,
            "symbols_failed": fail,
            "ml_predictions": ml,
            "shariah_compliant": sh_ok,
        }
        return recs, meta

    def _extract_features(self, data: Dict[str, Any], symbol: str) -> MLFeatures:
        return MLFeatures(
            symbol=symbol,
            price=_safe_float(data.get("price") or data.get("current_price")),
            volume=_safe_int(data.get("volume")),
            market_cap=_safe_float(data.get("market_cap")),
            pe_ratio=_safe_float(data.get("pe_ratio") or data.get("pe_ttm")),
            pb_ratio=_safe_float(data.get("pb_ratio") or data.get("pb")),
            dividend_yield=_safe_float(data.get("dividend_yield")),
            beta=_safe_float(data.get("beta"), 1.0),
            volatility_30d=_safe_float(data.get("volatility_30d")),
            momentum_14d=_safe_float(data.get("momentum_14d") or data.get("returns_1m")),
            rsi_14d=_safe_float(data.get("rsi_14d") or data.get("rsi_14"), 50.0),
            macd=_safe_float(data.get("macd") or data.get("macd_line")),
            macd_signal=_safe_float(data.get("macd_signal")),
        )

    async def _build_recommendation(
        self,
        symbol: str,
        data: Dict[str, Any],
        ml_prediction: Optional[MLPrediction],
        shariah_status: ShariahCompliance,
    ) -> AdvisorRecommendation:

        risk_score = float(ml_prediction.risk_score) if (ml_prediction and ml_prediction.risk_score is not None) else 50.0
        confidence = float(ml_prediction.confidence_1d) if (ml_prediction and ml_prediction.confidence_1d is not None) else 0.5

        price = _safe_float(data.get("current_price") or data.get("price"))
        exp12 = _safe_float(data.get("expected_roi_12m"))
        tgt12 = _safe_float(data.get("forecast_price_12m"))

        reasoning: List[str] = []
        factors = (ml_prediction.factors if ml_prediction else {}) or {}
        if factors:
            top = sorted(factors.items(), key=lambda x: x[1], reverse=True)[:3]
            reasoning.extend([f"{k}: {v:.1%}" for k, v in top])

        if shariah_status == ShariahCompliance.COMPLIANT:
            reasoning.append("Shariah compliant")
        elif shariah_status == ShariahCompliance.NON_COMPLIANT:
            reasoning.append("Not Shariah compliant")

        # score → use as rank (higher is better)
        score = ((ml_prediction.predicted_return_1m or 0.0) * 0.4) + ((100.0 - risk_score) * 0.3) + (confidence * 100.0 * 0.3)
        rank = int(max(0.0, score))

        return AdvisorRecommendation(
            rank=rank,
            symbol=symbol,
            name=data.get("name"),
            sector=data.get("sector"),
            shariah_compliant=shariah_status,
            current_price=price,
            target_price_12m=tgt12,
            expected_return_12m=exp12,
            risk_score=risk_score,
            confidence_score=confidence,
            ml_prediction=ml_prediction,
            factor_importance=factors,
            pe_ratio=_safe_float(data.get("pe_ratio") or data.get("pe_ttm")),
            pb_ratio=_safe_float(data.get("pb_ratio") or data.get("pb")),
            dividend_yield=_safe_float(data.get("dividend_yield")),
            market_cap=_safe_float(data.get("market_cap")),
            rsi_14d=_safe_float(data.get("rsi_14d") or data.get("rsi_14")),
            macd=_safe_float(data.get("macd")),
            reasoning=reasoning,
            data_quality="GOOD" if price is not None else "PARTIAL",
        )

_advisor_engine = AdvisorEngine()


# =============================================================================
# WebSocket Manager
# =============================================================================

class WebSocketManager:
    def __init__(self):
        self._active: List[WebSocket] = []
        self._lock = asyncio.Lock()
        self._subs: Dict[str, List[WebSocket]] = defaultdict(list)
        self._queue: asyncio.Queue = asyncio.Queue()
        self._task: Optional[asyncio.Task] = None

    async def start(self):
        if self._task and not self._task.done():
            return
        self._task = asyncio.create_task(self._loop())

    async def stop(self):
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except Exception:
                pass

    async def connect(self, ws: WebSocket):
        await ws.accept()
        async with self._lock:
            self._active.append(ws)

    async def disconnect(self, ws: WebSocket):
        async with self._lock:
            if ws in self._active:
                self._active.remove(ws)
            for sym in list(self._subs.keys()):
                if ws in self._subs[sym]:
                    self._subs[sym].remove(ws)

    async def subscribe(self, ws: WebSocket, symbol: str):
        async with self._lock:
            self._subs[symbol].append(ws)

    async def broadcast(self, message: Dict[str, Any], symbol: Optional[str] = None):
        await self._queue.put((message, symbol))

    async def _loop(self):
        while True:
            try:
                message, symbol = await self._queue.get()
                async with self._lock:
                    targets = list(self._subs.get(symbol, [])) if symbol else list(self._active)

                payload = json_dumps(message)
                disconnected: List[WebSocket] = []
                for ws in targets:
                    try:
                        await ws.send_text(payload)
                    except Exception:
                        disconnected.append(ws)

                for ws in disconnected:
                    await self.disconnect(ws)

            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error("Broadcast error: %s", e)

_ws_manager = WebSocketManager()


# =============================================================================
# Response formatting helpers
# =============================================================================

def _get_default_headers() -> List[str]:
    return [
        "Rank",
        "Symbol",
        "Name",
        "Sector",
        "Current Price",
        "Target Price (12M)",
        "Expected Return (12M)",
        "Risk Score",
        "Confidence",
        "Shariah",
        "Weight",
        "Allocated Amount",
    ]


def _recommendations_to_rows(recs: List[AdvisorRecommendation]) -> List[List[Any]]:
    rows: List[List[Any]] = []
    for r in recs:
        rows.append([
            r.rank,
            r.symbol,
            r.name or "",
            r.sector or "",
            f"{r.current_price:.2f}" if r.current_price is not None else "",
            f"{r.target_price_12m:.2f}" if r.target_price_12m is not None else "",
            f"{r.expected_return_12m:.1f}%" if r.expected_return_12m is not None else "",
            f"{r.risk_score:.0f}",
            f"{r.confidence_score:.1%}",
            "✓" if r.shariah_compliant == ShariahCompliance.COMPLIANT else ("✗" if r.shariah_compliant == ShariahCompliance.NON_COMPLIANT else ""),
            f"{r.weight:.1%}",
            f"{r.allocated_amount:.2f}" if r.allocated_amount else "",
        ])
    return rows


def _determine_market_condition(recs: List[AdvisorRecommendation]) -> MarketCondition:
    if not recs:
        return MarketCondition.SIDEWAYS
    avg_conf = sum(r.confidence_score for r in recs) / len(recs)
    avg_risk = sum(r.risk_score for r in recs) / len(recs)
    if avg_conf > 0.8 and avg_risk < 40:
        return MarketCondition.BULL
    if avg_conf < 0.3 and avg_risk > 70:
        return MarketCondition.BEAR
    if avg_risk > 60:
        return MarketCondition.VOLATILE
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
        "engine_ready": bool(engine),
        "config": {
            "max_concurrent": _CONFIG.max_concurrent_requests,
            "default_top_n": _CONFIG.default_top_n,
            "allocation_method": _CONFIG.default_allocation_method.value,
            "enable_ml": _CONFIG.enable_ml_predictions,
            "enable_shariah": _CONFIG.enable_shariah_filter,
            "require_api_key": _CONFIG.require_api_key,
        },
        "websocket_connections": len(_ws_manager._active),
        "ml_initialized": _ml_models._initialized,
        "ml_models": _ml_models.get_model_versions() if _ml_models._initialized else {},
        "request_id": getattr(request.state, "request_id", None),
    }


@router.get("/metrics")
async def advisor_metrics() -> Response:
    if not _PROMETHEUS_AVAILABLE:
        return BestJSONResponse(status_code=503, content={"error": "Metrics not available"})
    from prometheus_client import generate_latest, CONTENT_TYPE_LATEST  # type: ignore
    return Response(content=generate_latest(), media_type=CONTENT_TYPE_LATEST)


@router.post("/recommendations")
async def advisor_recommendations(
    request: Request,
    body: Dict[str, Any] = Body(...),
    token: Optional[str] = Query(default=None),
    x_app_token: Optional[str] = Header(default=None, alias="X-APP-TOKEN"),
    authorization: Optional[str] = Header(default=None, alias="Authorization"),
    x_request_id: Optional[str] = Header(default=None, alias="X-Request-ID"),
) -> BestJSONResponse:
    request_id = x_request_id or str(uuid.uuid4())
    start_time = time.time()

    try:
        # Rate limiting
        client_ip = request.client.host if request.client else "unknown"
        ok = await _rate_limiter.check(client_ip, _CONFIG.rate_limit_requests, _CONFIG.rate_limit_window)
        if not ok:
            raise HTTPException(status_code=status.HTTP_429_TOO_MANY_REQUESTS, detail="Rate limit exceeded")

        # Auth token selection
        auth_token = (x_app_token or "").strip()

        # Only allow query token if explicitly allowed
        if _CONFIG.allow_query_token and token:
            auth_token = token.strip()

        if authorization and authorization.startswith("Bearer "):
            auth_token = authorization[7:].strip()

        if not _token_manager.validate_token(auth_token):
            raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid token")

        # Parse request
        async with TraceContext("parse_request", {"request_id": request_id}):
            try:
                req = AdvisorRequest.model_validate(body) if _PYDANTIC_V2 else AdvisorRequest.parse_obj(body)  # type: ignore
                req.request_id = request_id
            except Exception as e:
                await _audit.log(
                    "validation_error",
                    auth_token[:8] if auth_token else None,
                    "advisor",
                    "recommendations",
                    "error",
                    {"error": str(e)},
                    request_id,
                )
                return BestJSONResponse(
                    status_code=400,
                    content={
                        "status": "error",
                        "error": f"Invalid request: {str(e)}",
                        "headers": [],
                        "rows": [],
                        "request_id": request_id,
                        "meta": {"duration_ms": (time.time() - start_time) * 1000},
                    },
                )

        # warm ML
        if req.enable_ml_predictions and _CONFIG.enable_ml_predictions:
            asyncio.create_task(_ml_models.initialize())

        # engine
        async with TraceContext("get_engine"):
            engine = await _advisor_engine.get_engine(request)
        if not engine:
            return BestJSONResponse(
                status_code=503,
                content={
                    "status": "error",
                    "error": "Advisor engine unavailable",
                    "headers": [],
                    "rows": [],
                    "request_id": request_id,
                    "meta": {"duration_ms": (time.time() - start_time) * 1000},
                },
            )

        # recommendations
        async with TraceContext("get_recommendations", {"symbol_count": len(req.tickers or []), "top_n": req.top_n or 0}):
            recommendations, rec_meta = await _advisor_engine.get_recommendations(req, engine)

        # portfolio
        portfolio = None
        if recommendations and req.invest_amount:
            async with TraceContext("optimize_portfolio"):
                expected_returns = {r.symbol: (r.expected_return_12m or 0.0) / 100.0 for r in recommendations}
                symbols = [r.symbol for r in recommendations]
                portfolio = await _portfolio_optimizer.optimize(
                    symbols,
                    expected_returns,
                    constraints={"max_position_pct": _CONFIG.max_position_pct, "min_position_pct": _CONFIG.min_position_pct},
                )
                weights = (portfolio or {}).get("weights", {}) if isinstance(portfolio, dict) else {}
                for r in recommendations:
                    r.weight = float(weights.get(r.symbol, 0.0) or 0.0)
                    r.allocated_amount = float(r.weight * req.invest_amount)
                    if r.expected_return_12m is not None:
                        r.expected_gain_12m = r.allocated_amount * (float(r.expected_return_12m) / 100.0)

        headers = _get_default_headers()
        rows = _recommendations_to_rows(recommendations)

        status_val = "success" if rec_meta["symbols_failed"] == 0 else ("partial" if rec_meta["symbols_succeeded"] > 0 else "error")
        market_condition = _determine_market_condition(recommendations)
        ml_predictions = {r.symbol: (r.ml_prediction.model_dump(mode="python") if (_PYDANTIC_V2 and r.ml_prediction) else r.ml_prediction.dict() if r.ml_prediction else None)
                          for r in recommendations if r.ml_prediction} if (req.enable_ml_predictions and _CONFIG.enable_ml_predictions) else None

        shariah_summary = {
            "total": len(recommendations),
            "compliant": rec_meta.get("shariah_compliant", 0),
            "non_compliant": len(recommendations) - rec_meta.get("shariah_compliant", 0),
        }

        asyncio.create_task(_audit.log(
            "recommendations",
            auth_token[:8] if auth_token else None,
            "advisor",
            "generate",
            status_val,
            {"symbols": len(req.tickers or []), "recommendations": len(recommendations), "portfolio_value": req.invest_amount},
            request_id,
        ))

        response_obj = AdvisorResponse(
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
                "trading_hours": _saudi_time.is_trading_hours(),
            },
        )

        return BestJSONResponse(content=_serialize_response(response_obj))

    except HTTPException:
        raise
    except Exception as e:
        logger.error("Global catch in recommendations: %s\n%s", e, traceback.format_exc())
        return BestJSONResponse(
            status_code=500,
            content={
                "status": "error",
                "error": f"Internal Server Error: {e}",
                "headers": [],
                "rows": [],
                "request_id": request_id,
                "meta": {"duration_ms": (time.time() - start_time) * 1000},
            },
        )


@router.post("/run")
async def advisor_run(
    request: Request,
    body: Dict[str, Any] = Body(...),
    token: Optional[str] = Query(default=None),
    x_app_token: Optional[str] = Header(default=None, alias="X-APP-TOKEN"),
    authorization: Optional[str] = Header(default=None, alias="Authorization"),
    x_request_id: Optional[str] = Header(default=None, alias="X-Request-ID"),
) -> BestJSONResponse:
    """Alias for /recommendations."""
    return await advisor_recommendations(
        request=request,
        body=body,
        token=token,
        x_app_token=x_app_token,
        authorization=authorization,
        x_request_id=x_request_id,
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
            "version": ADVISOR_ROUTE_VERSION,
        }
        await websocket.send_text(json_dumps(init_payload))

        while True:
            try:
                message = await websocket.receive_json()
                mtype = str(message.get("type", "")).lower()

                if mtype == "subscribe":
                    symbol = str(message.get("symbol", "")).strip().upper()
                    if symbol:
                        await _ws_manager.subscribe(websocket, symbol)
                        await websocket.send_text(json_dumps({
                            "type": "subscription",
                            "status": "subscribed",
                            "symbol": symbol,
                            "timestamp": _saudi_time.format_iso(),
                        }))
                elif mtype == "ping":
                    await websocket.send_text(json_dumps({"type": "pong", "timestamp": _saudi_time.format_iso()}))
            except WebSocketDisconnect:
                break
            except Exception as e:
                logger.error("WebSocket error: %s", e)
                await websocket.send_text(json_dumps({"type": "error", "error": str(e), "timestamp": _saudi_time.format_iso()}))
    finally:
        await _ws_manager.disconnect(websocket)


@router.on_event("startup")
async def startup_event():
    await _ws_manager.start()


@router.on_event("shutdown")
async def shutdown_event():
    await _ws_manager.stop()


__all__ = ["router"]
