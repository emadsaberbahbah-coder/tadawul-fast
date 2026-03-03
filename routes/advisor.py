#!/usr/bin/env python3
"""
routes/advisor.py
------------------------------------------------------------
TADAWUL ENTERPRISE ADVISOR ENGINE — v4.6.0 (PHASE-ALIGNED + STARTUP-SAFE)
SAMA Compliant | Multi-Asset | Real-time ML (optional) | Dynamic Allocation | Audit Trail

What this revision improves (vs your v4.5.2)
- ✅ Auth alignment:
    - Prefer core.config.auth_ok / is_open_mode when available
    - Otherwise secure fallback TokenManager (REQUIRE_AUTH=true + no tokens => DENY)
    - Query token only honored when ALLOW_QUERY_TOKEN=true
- ✅ Ranking correctness:
    - Uses an internal score, then assigns rank = 1..N after sorting (stable + deterministic)
- ✅ Engine compatibility:
    - Tries batch + single methods safely (get_enriched_quotes_batch / get_enriched_quotes / get_enriched_quote)
- ✅ Output stability:
    - Adds `items` (dict rows) alongside legacy headers/rows
    - Keeps rows formatting, but also returns raw numeric fields in items for Apps Script
- ✅ Safer defaults:
    - ML predicted_return_1m is FRACTION (0.05 = 5%) consistent with ROI fields
- ✅ No import-time network I/O, no startup heavy work
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
from typing import Any, Dict, List, Optional, Set, Tuple, Union, Callable

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


def _safe_str(v: Any, default: str = "") -> str:
    try:
        return str(v).strip() if v is not None else default
    except Exception:
        return default


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
        return int(float(v))
    except (TypeError, ValueError):
        return default


def _as_ratio(v: Any) -> Optional[float]:
    """
    Convert percent-points to fraction if needed.
      12   -> 0.12
      0.12 -> 0.12
      "12%" -> 0.12
    """
    if v is None:
        return None
    if isinstance(v, str):
        s = v.strip()
        if not s:
            return None
        if s.endswith("%"):
            f = _safe_float(s[:-1].strip())
            return None if f is None else f / 100.0
        f = _safe_float(s)
    else:
        f = _safe_float(v)
    if f is None:
        return None
    return f / 100.0 if abs(f) > 1.5 else f


# =============================================================================
# High-Performance JSON response
# =============================================================================

try:
    import orjson  # type: ignore
    from fastapi.responses import Response as FastAPIResponse

    class BestJSONResponse(FastAPIResponse):
        media_type = "application/json"

        def render(self, content: Any) -> bytes:
            return orjson.dumps(clean_nans(content), default=str)

    def json_dumps(v: Any, *, default=str) -> str:
        return orjson.dumps(clean_nans(v), default=default).decode("utf-8")

    _HAS_ORJSON = True
except Exception:
    import json  # type: ignore
    from fastapi.responses import JSONResponse as BaseJSONResponse

    class BestJSONResponse(BaseJSONResponse):
        def render(self, content: Any) -> bytes:
            return json.dumps(clean_nans(content), default=str, ensure_ascii=False).encode("utf-8")

    def json_dumps(v: Any, *, default=str) -> str:
        return json.dumps(clean_nans(v), default=default, ensure_ascii=False)

    _HAS_ORJSON = False


# =============================================================================
# Optional enterprise integrations
# =============================================================================

# OpenTelemetry (optional)
try:
    from opentelemetry import trace  # type: ignore
    from opentelemetry.trace import Status, StatusCode  # type: ignore

    _OTEL_AVAILABLE = True
    tracer = trace.get_tracer(__name__)
except Exception:
    _OTEL_AVAILABLE = False
    tracer = None  # type: ignore
    Status = None  # type: ignore
    StatusCode = None  # type: ignore

    class _DummySpan:
        def set_attribute(self, *args, **kwargs):  # noqa
            return None
        def set_status(self, *args, **kwargs):  # noqa
            return None
        def record_exception(self, *args, **kwargs):  # noqa
            return None

    class _DummyCM:
        def __enter__(self):  # noqa
            return _DummySpan()
        def __exit__(self, *args, **kwargs):  # noqa
            return False

    class _DummyTracer:
        def start_as_current_span(self, *args, **kwargs):  # noqa
            return _DummyCM()

    tracer = _DummyTracer()  # type: ignore

# Prometheus (optional)
try:
    from prometheus_client import Counter, Histogram  # type: ignore
    _PROM_AVAILABLE = True
    advisor_requests_total = Counter("tfb_advisor_requests_total", "Advisor requests", ["endpoint", "status"])
    advisor_duration_seconds = Histogram("tfb_advisor_duration_seconds", "Advisor request duration", ["endpoint"])
except Exception:
    _PROM_AVAILABLE = False

    class _DummyMetric:
        def labels(self, *args, **kwargs):
            return self
        def inc(self, *args, **kwargs):
            return None
        def observe(self, *args, **kwargs):
            return None

    advisor_requests_total = _DummyMetric()
    advisor_duration_seconds = _DummyMetric()

# Pydantic (v2 preferred, v1 fallback)
try:
    from pydantic import BaseModel, ConfigDict, Field, model_validator  # type: ignore
    _PYDANTIC_V2 = True
except Exception:
    from pydantic import BaseModel, Field  # type: ignore
    _PYDANTIC_V2 = False


logger = logging.getLogger("routes.advisor")

ADVISOR_ROUTE_VERSION = "4.6.0"
router = APIRouter(prefix="/v1/advisor", tags=["advisor"])

_TRUTHY = {"1", "true", "yes", "y", "on", "t"}


# =============================================================================
# TraceContext (safe)
# =============================================================================

class TraceContext:
    """
    Safe OpenTelemetry trace context manager.
    Uses start_as_current_span correctly, then set_attribute per attribute.
    """
    def __init__(self, name: str, attributes: Optional[Dict[str, Any]] = None):
        self.name = name
        self.attributes = attributes or {}
        self._cm = None
        self.span = None
        self._enabled = bool(_OTEL_AVAILABLE and str(os.getenv("CORE_TRACING_ENABLED", "1")).strip().lower() in _TRUTHY)

    def __enter__(self):
        if not self._enabled:
            return self
        try:
            self._cm = tracer.start_as_current_span(self.name)  # type: ignore
            self.span = self._cm.__enter__()
            for k, v in (self.attributes or {}).items():
                try:
                    if hasattr(self.span, "set_attribute"):
                        self.span.set_attribute(str(k), v)
                except Exception:
                    pass
        except Exception:
            self._cm = None
            self.span = None
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self._cm is None:
            return False
        try:
            if exc_val and self.span is not None and Status is not None and StatusCode is not None:
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
                return self._cm.__exit__(exc_type, exc_val, exc_tb)
            except Exception:
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
        self._weekend_days = {4, 5}  # Fri/Sat

        env_h = os.getenv("SAUDI_MARKET_HOLIDAYS", "").strip()
        self._holidays = {x.strip() for x in env_h.split(",") if x.strip()} if env_h else set()

    def now(self) -> datetime:
        return datetime.now(self._tz)

    def format_iso(self, dt: Optional[datetime] = None) -> str:
        return (dt or self.now()).isoformat(timespec="milliseconds")

    def is_trading_day(self, dt: Optional[datetime] = None) -> bool:
        dt = dt or self.now()
        if dt.weekday() in self._weekend_days:
            return False
        if dt.strftime("%Y-%m-%d") in self._holidays:
            return False
        return True

_saudi_time = SaudiMarketTime()


# =============================================================================
# Enums & Types
# =============================================================================

class FlexibleEnum(str, Enum):
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

class AllocationMethod(FlexibleEnum):
    EQUAL_WEIGHT = "EQUAL_WEIGHT"
    MARKET_CAP = "MARKET_CAP"
    RISK_PARITY = "RISK_PARITY"
    MEAN_VARIANCE = "MEAN_VARIANCE"
    MAXIMUM_SHARPE = "MAXIMUM_SHARPE"
    BLACK_LITTERMAN = "BLACK_LITTERMAN"
    DYNAMIC = "DYNAMIC"

class ShariahCompliance(FlexibleEnum):
    COMPLIANT = "COMPLIANT"
    NON_COMPLIANT = "NON_COMPLIANT"
    PENDING = "PENDING"
    NA = "NA"


# =============================================================================
# Configuration
# =============================================================================

@dataclass(slots=True)
class AdvisorConfig:
    max_concurrent_requests: int = 60
    default_top_n: int = 50
    max_top_n: int = 500
    route_timeout_sec: float = 75.0

    enable_ml_predictions: bool = True
    ml_prediction_timeout_sec: float = 5.0
    min_confidence_for_prediction: float = 0.6

    default_allocation_method: AllocationMethod = AllocationMethod.EQUAL_WEIGHT
    max_position_pct: float = 0.15
    min_position_pct: float = 0.01

    enable_shariah_filter: bool = True
    enable_audit_log: bool = True

    rate_limit_requests: int = 120
    rate_limit_window: int = 60

    allow_query_token: bool = False
    require_auth: bool = True  # aligns to REQUIRE_AUTH

    base_currency: str = "SAR"

_CONFIG = AdvisorConfig()

def _load_config_from_env() -> None:
    def env_int(name: str, default: int) -> int:
        try:
            return int(float(os.getenv(name, str(default)).strip()))
        except Exception:
            return default

    def env_float(name: str, default: float) -> float:
        try:
            return float(os.getenv(name, str(default)).strip())
        except Exception:
            return default

    def env_bool(name: str, default: bool) -> bool:
        return (os.getenv(name, str(default)).strip().lower() in _TRUTHY)

    def env_enum(name: str, default: Enum, enum_class: type) -> Enum:
        raw = os.getenv(name, "").strip().upper()
        if not raw:
            return default
        try:
            return enum_class(raw)
        except Exception:
            return default

    _CONFIG.max_concurrent_requests = env_int("ADVISOR_MAX_CONCURRENT", _CONFIG.max_concurrent_requests)
    _CONFIG.default_top_n = env_int("ADVISOR_DEFAULT_TOP_N", _CONFIG.default_top_n)
    _CONFIG.max_top_n = env_int("ADVISOR_MAX_TOP_N", _CONFIG.max_top_n)
    _CONFIG.route_timeout_sec = env_float("ADVISOR_ROUTE_TIMEOUT_SEC", _CONFIG.route_timeout_sec)

    _CONFIG.enable_ml_predictions = env_bool("ADVISOR_ENABLE_ML", _CONFIG.enable_ml_predictions)
    _CONFIG.ml_prediction_timeout_sec = env_float("ADVISOR_ML_TIMEOUT_SEC", _CONFIG.ml_prediction_timeout_sec)
    _CONFIG.min_confidence_for_prediction = env_float("ADVISOR_MIN_CONFIDENCE", _CONFIG.min_confidence_for_prediction)

    _CONFIG.default_allocation_method = env_enum("ADVISOR_ALLOCATION_METHOD", _CONFIG.default_allocation_method, AllocationMethod)
    _CONFIG.enable_shariah_filter = env_bool("ADVISOR_SHARIAH_FILTER", _CONFIG.enable_shariah_filter)

    _CONFIG.allow_query_token = env_bool("ALLOW_QUERY_TOKEN", _CONFIG.allow_query_token)
    _CONFIG.require_auth = env_bool("REQUIRE_AUTH", _CONFIG.require_auth)

    _CONFIG.enable_audit_log = env_bool("ENABLE_AUDIT_LOG", _CONFIG.enable_audit_log)

    _CONFIG.rate_limit_requests = env_int("MAX_REQUESTS_PER_MINUTE", _CONFIG.rate_limit_requests)
    _CONFIG.rate_limit_window = env_int("RATE_LIMIT_WINDOW_SEC", _CONFIG.rate_limit_window)

_load_config_from_env()


# =============================================================================
# Auth & Rate limiter
# =============================================================================

# Prefer core.config auth if available
try:
    from core.config import auth_ok as _core_auth_ok  # type: ignore
    from core.config import is_open_mode as _core_is_open_mode  # type: ignore
except Exception:
    _core_auth_ok = None  # type: ignore
    _core_is_open_mode = None  # type: ignore

class TokenManager:
    """
    Secure fallback:
      - if REQUIRE_AUTH=true and no tokens configured => DENY
      - else validate against configured tokens
    """
    def __init__(self):
        self._tokens: Set[str] = set()
        for key in ("APP_TOKEN", "BACKUP_APP_TOKEN", "TFB_APP_TOKEN", "BACKEND_TOKEN"):
            t = (os.getenv(key, "") or "").strip()
            if t:
                self._tokens.add(t)

    def validate(self, token: str) -> bool:
        token = (token or "").strip()
        if not _CONFIG.require_auth:
            return True if (not self._tokens) else (token in self._tokens)
        if not self._tokens:
            return False
        return token in self._tokens

_token_manager = TokenManager()

def _extract_auth_token(token_q: Optional[str], x_app_token: Optional[str], authorization: Optional[str]) -> str:
    if authorization:
        a = authorization.strip()
        if a.lower().startswith("bearer "):
            return a.split(" ", 1)[1].strip()
        return a
    if x_app_token and x_app_token.strip():
        return x_app_token.strip()
    if token_q and token_q.strip():
        return token_q.strip()
    return ""

def _auth_allowed(request: Request, token_q: Optional[str], x_app_token: Optional[str], authorization: Optional[str]) -> bool:
    # open mode shortcut
    if callable(_core_is_open_mode):
        try:
            if _core_is_open_mode():
                return True
        except Exception:
            pass

    token = _extract_auth_token(token_q if _CONFIG.allow_query_token else None, x_app_token, authorization)
    if callable(_core_auth_ok):
        try:
            return bool(_core_auth_ok(token=token, authorization=authorization, headers=dict(request.headers)))
        except Exception:
            # if core auth fails unexpectedly, fall back to local manager
            return _token_manager.validate(token)
    return _token_manager.validate(token)

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
# Audit logging (buffered)
# =============================================================================

class AuditLogger:
    def __init__(self):
        self._buffer: List[Dict[str, Any]] = []
        self._lock = asyncio.Lock()

    async def log(self, event: str, action: str, status_s: str, details: Dict[str, Any], request_id: str, user_hint: Optional[str] = None) -> None:
        if not _CONFIG.enable_audit_log:
            return
        entry = {
            "timestamp": _saudi_time.format_iso(),
            "event_id": str(uuid.uuid4()),
            "event": event,
            "action": action,
            "status": status_s,
            "details": details,
            "request_id": request_id,
            "user_hint": user_hint,
            "version": ADVISOR_ROUTE_VERSION,
            "env": os.getenv("APP_ENV", "production"),
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
            batch = list(self._buffer)
            self._buffer.clear()
        try:
            logger.info("Advisor audit flush: %s entries", len(batch))
        except Exception:
            pass

_audit = AuditLogger()


# =============================================================================
# Models
# =============================================================================

class MLFeatures(BaseModel):
    symbol: str
    price: Optional[float] = None
    previous_close: Optional[float] = None
    volume: Optional[int] = None
    market_cap: Optional[float] = None
    pe_ttm: Optional[float] = None
    pb: Optional[float] = None
    dividend_yield: Optional[float] = None
    beta: Optional[float] = None
    volatility_30d: Optional[float] = None
    momentum_14d: Optional[float] = None
    rsi_14: Optional[float] = None

    if _PYDANTIC_V2:
        model_config = ConfigDict(extra="ignore", arbitrary_types_allowed=True)

class MLPrediction(BaseModel):
    symbol: str
    predicted_return_1m: Optional[float] = None  # FRACTION (0.05 = 5%)
    confidence: Optional[float] = None           # 0..1
    risk_score: Optional[float] = None           # 0..100
    factors: Dict[str, float] = Field(default_factory=dict)
    model_version: str = "advisor_mock_v1"

    if _PYDANTIC_V2:
        model_config = ConfigDict(extra="ignore", arbitrary_types_allowed=True)

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
                elif isinstance(x, list):
                    items.extend([_safe_str(p).strip() for p in x if _safe_str(p).strip()])

            if self.tickers:
                add_many(self.tickers)
            if self.symbols:
                add_many(self.symbols)

            # normalize & dedupe preserve order
            seen = set()
            out: List[str] = []
            for s in items:
                u = _safe_str(s).upper()
                if u and u not in seen:
                    seen.add(u)
                    out.append(u)

            self.tickers = out
            self.symbols = []

            if self.top_n is None:
                self.top_n = _CONFIG.default_top_n
            self.top_n = max(1, min(int(self.top_n), _CONFIG.max_top_n))

            if self.allocation_method is None:
                self.allocation_method = _CONFIG.default_allocation_method

            # clamp min_confidence
            try:
                self.min_confidence = float(self.min_confidence)
            except Exception:
                self.min_confidence = _CONFIG.min_confidence_for_prediction
            self.min_confidence = max(0.0, min(1.0, self.min_confidence))

            return self

class AdvisorRecommendation(BaseModel):
    rank: int
    symbol: str
    name: Optional[str] = None
    sector: Optional[str] = None
    current_price: Optional[float] = None

    forecast_price_12m: Optional[float] = None
    expected_roi_12m: Optional[float] = None  # FRACTION

    risk_score: float = 50.0
    confidence_score: float = 0.5

    shariah: ShariahCompliance = ShariahCompliance.NA

    recommendation: str = "HOLD"
    advisor_score: float = 0.0

    weight: float = 0.0
    allocated_amount: float = 0.0
    expected_gain_12m: Optional[float] = None

    pe_ttm: Optional[float] = None
    pb: Optional[float] = None
    dividend_yield: Optional[float] = None
    market_cap: Optional[float] = None
    rsi_14: Optional[float] = None
    volatility_30d: Optional[float] = None

    ml_prediction: Optional[MLPrediction] = None
    reasoning: List[str] = Field(default_factory=list)

    data_quality: str = "GOOD"
    data_source: str = "advisor"
    last_updated_utc: str = Field(default_factory=lambda: datetime.now(timezone.utc).isoformat())
    last_updated_riyadh: str = Field(default_factory=_saudi_time.format_iso)

    if _PYDANTIC_V2:
        model_config = ConfigDict(extra="ignore", arbitrary_types_allowed=True)

class AdvisorResponse(BaseModel):
    status: str = "success"
    error: Optional[str] = None

    headers: List[str] = Field(default_factory=list)
    rows: List[List[Any]] = Field(default_factory=list)

    # richer output
    recommendations: List[AdvisorRecommendation] = Field(default_factory=list)
    items: List[Dict[str, Any]] = Field(default_factory=list)  # dict rows for Apps Script
    portfolio: Optional[Dict[str, Any]] = None

    version: str = ADVISOR_ROUTE_VERSION
    request_id: str = ""
    meta: Dict[str, Any] = Field(default_factory=dict)

    if _PYDANTIC_V2:
        model_config = ConfigDict(extra="ignore", arbitrary_types_allowed=True)

def _serialize_model(m: BaseModel) -> Dict[str, Any]:
    if _PYDANTIC_V2:
        return clean_nans(m.model_dump(mode="python", exclude_none=True))  # type: ignore
    # pydantic v1
    try:
        return clean_nans(m.dict(exclude_none=True))  # type: ignore
    except Exception:
        return clean_nans({"status": "error", "error": "serialization_failed"})


# =============================================================================
# ML Model Manager (lazy + safe)
# =============================================================================

class MLModelManager:
    def __init__(self):
        self._initialized = False
        self._lock = asyncio.Lock()

    async def initialize(self) -> None:
        if self._initialized:
            return
        async with self._lock:
            if self._initialized:
                return
            # NOTE: keep startup-safe (no external loads here)
            self._initialized = True
            logger.info("Advisor ML initialized (safe stub)")

    async def predict(self, f: MLFeatures, *, min_conf: float) -> Optional[MLPrediction]:
        if not self._initialized:
            return None

        # Lightweight deterministic heuristics (no heavy ML deps)
        price = f.price
        prev = f.previous_close
        vol = f.volatility_30d or 0.0
        beta = f.beta if f.beta is not None else 1.0
        rsi = f.rsi_14 if f.rsi_14 is not None else 50.0
        mom = f.momentum_14d if f.momentum_14d is not None else 0.0

        # predicted return (fraction)
        base_ret = 0.01
        if price is not None and prev is not None and prev != 0:
            base_ret += max(-0.05, min(0.05, (price / prev) - 1.0))
        base_ret += max(-0.03, min(0.03, mom))

        # confidence 0..1
        conf = 0.55
        if f.market_cap:
            conf += 0.10
        if f.volume:
            conf += 0.05
        if 30 <= rsi <= 70:
            conf += 0.05
        conf = max(0.0, min(1.0, conf))

        if conf < min_conf:
            return None

        # risk score 0..100
        risk = 50.0
        if vol:
            risk += min(35.0, max(0.0, vol) * 1.2)
        if beta:
            risk += (beta - 1.0) * 15.0
        if rsi >= 75:
            risk += 10.0
        if rsi <= 25:
            risk += 6.0
        risk = max(0.0, min(100.0, risk))

        factors = {
            "momentum": float(mom),
            "volatility": float(vol),
            "beta": float(beta),
            "rsi": float(rsi),
        }

        return MLPrediction(
            symbol=f.symbol,
            predicted_return_1m=float(base_ret),
            confidence=float(conf),
            risk_score=float(risk),
            factors=factors,
            model_version="advisor_mock_v1",
        )

_ml_models = MLModelManager()


# =============================================================================
# Shariah checker (simple safe filter)
# =============================================================================

class ShariahChecker:
    def __init__(self):
        self._cache: Dict[str, Tuple[ShariahCompliance, float]] = {}
        self._lock = asyncio.Lock()

    async def check(self, symbol: str, data: Dict[str, Any]) -> ShariahCompliance:
        key = f"shariah:{symbol}"
        now = time.time()
        async with self._lock:
            cached = self._cache.get(key)
            if cached and cached[1] > now:
                return cached[0]

        # simple keyword screening (replace with real dataset later)
        sector = _safe_str(data.get("sector")).lower()
        name = _safe_str(data.get("name")).lower()
        combined = f"{sector} {name}"

        prohibited = ["alcohol", "tobacco", "pork", "gambling", "casino", "conventional finance", "insurance", "weapons"]
        status_s = ShariahCompliance.NON_COMPLIANT if any(p in combined for p in prohibited) else ShariahCompliance.COMPLIANT

        async with self._lock:
            self._cache[key] = (status_s, now + 86400)
        return status_s

_shariah_checker = ShariahChecker()


# =============================================================================
# Portfolio optimizer (safe minimal: equal weight + constraints)
# =============================================================================

class PortfolioOptimizer:
    async def optimize(self, symbols: List[str], *, max_pos: float, min_pos: float) -> Dict[str, Any]:
        if not symbols:
            return {"weights": {}, "method": "none"}
        n = len(symbols)
        w = 1.0 / max(1, n)
        weights = {s: w for s in symbols}

        # clamp then renormalize
        max_pos = float(max_pos)
        min_pos = float(min_pos)
        for s in list(weights.keys()):
            weights[s] = max(min_pos, min(max_pos, weights[s]))
        tot = sum(weights.values()) or 1.0
        weights = {s: v / tot for s, v in weights.items()}
        return {"weights": weights, "method": "equal_weight"}

_portfolio_optimizer = PortfolioOptimizer()


# =============================================================================
# Engine wrapper
# =============================================================================

def _to_dict(obj: Any) -> Dict[str, Any]:
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
    if hasattr(obj, "__dict__"):
        try:
            return dict(obj.__dict__)
        except Exception:
            return {}
    return {}

class AdvisorEngine:
    def __init__(self):
        self._engine = None
        self._lock = asyncio.Lock()

    async def get_engine(self, request: Request) -> Optional[Any]:
        # prefer app.state.engine
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
            # Prefer v2
            for modpath in ("core.data_engine_v2", "data_engine_v2", "core.data_engine"):
                try:
                    mod = __import__(modpath, fromlist=["get_engine"])
                    get_engine = getattr(mod, "get_engine", None)
                    if callable(get_engine):
                        eng = get_engine()
                        if hasattr(eng, "__await__"):
                            eng = await eng
                        self._engine = eng
                        return self._engine
                except Exception:
                    continue
        return None

    async def fetch_enriched_map(self, engine: Any, symbols: List[str]) -> Dict[str, Dict[str, Any]]:
        """
        Best effort:
          - get_enriched_quotes_batch(symbols) -> dict
          - get_enriched_quotes(symbols) -> list/dict
          - fallback per-symbol get_enriched_quote
        """
        if not engine or not symbols:
            return {}

        # batch dict
        for method in ("get_enriched_quotes_batch", "get_enriched_quotes", "get_quotes_batch"):
            fn = getattr(engine, method, None)
            if callable(fn):
                try:
                    res = fn(symbols)
                    if hasattr(res, "__await__"):
                        res = await res
                    if isinstance(res, dict):
                        return {str(k).upper(): _to_dict(v) for k, v in res.items()}
                    if isinstance(res, list):
                        out: Dict[str, Dict[str, Any]] = {}
                        for sym, item in zip(symbols, res):
                            out[str(sym).upper()] = _to_dict(item)
                        return out
                except Exception:
                    pass

        # per-symbol
        fn1 = getattr(engine, "get_enriched_quote", None) or getattr(engine, "get_quote", None)
        if not callable(fn1):
            return {}

        sem = asyncio.Semaphore(max(1, int(_CONFIG.max_concurrent_requests)))

        async def one(sym: str) -> Tuple[str, Dict[str, Any]]:
            async with sem:
                try:
                    r = fn1(sym)
                    if hasattr(r, "__await__"):
                        r = await r
                    return str(sym).upper(), _to_dict(r)
                except Exception as e:
                    return str(sym).upper(), {"symbol": sym, "error": str(e), "data_quality": "MISSING"}

        pairs = await asyncio.gather(*(one(s) for s in symbols), return_exceptions=False)
        return {k: v for k, v in pairs}

_advisor_engine = AdvisorEngine()


# =============================================================================
# Scoring + recommendation logic
# =============================================================================

def _compute_advisor_score(
    *,
    ml: Optional[MLPrediction],
    expected_roi_12m: Optional[float],
    risk_score: float,
    confidence: float,
) -> float:
    """
    Returns 0..100
    - ROI contributes (up to 40)
    - risk contributes (up to 30) (lower risk => higher score)
    - confidence contributes (up to 30)
    """
    roi = expected_roi_12m if expected_roi_12m is not None else (ml.predicted_return_1m if ml and ml.predicted_return_1m is not None else 0.0)
    roi = float(roi or 0.0)
    roi = max(-0.5, min(0.8, roi))  # clamp

    roi_score = max(0.0, min(40.0, (roi + 0.10) * 200.0))  # -10% -> 0, +10% -> 40
    risk_score = float(risk_score)
    risk_component = max(0.0, min(30.0, (100.0 - risk_score) * 0.30))
    conf_component = max(0.0, min(30.0, float(confidence) * 30.0))
    return max(0.0, min(100.0, roi_score + risk_component + conf_component))

def _label_recommendation(score: float) -> str:
    if score >= 80:
        return "STRONG_BUY"
    if score >= 65:
        return "BUY"
    if score >= 50:
        return "HOLD"
    if score >= 35:
        return "REDUCE"
    return "SELL"


# =============================================================================
# Request parsing helpers
# =============================================================================

def _parse_symbols(req: AdvisorRequest) -> List[str]:
    raw: List[str] = []
    if req.tickers:
        if isinstance(req.tickers, str):
            raw.extend([p.strip() for p in req.tickers.replace(",", " ").split() if p.strip()])
        elif isinstance(req.tickers, list):
            raw.extend([_safe_str(x).strip() for x in req.tickers if _safe_str(x).strip()])

    # (symbols field is normalized away in v2 validator, but keep fallback)
    if req.symbols:
        if isinstance(req.symbols, str):
            raw.extend([p.strip() for p in req.symbols.replace(",", " ").split() if p.strip()])
        elif isinstance(req.symbols, list):
            raw.extend([_safe_str(x).strip() for x in req.symbols if _safe_str(x).strip()])

    seen: Set[str] = set()
    out: List[str] = []
    for s in raw:
        u = s.upper()
        if u and u not in seen:
            seen.add(u)
            out.append(u)
    return out


# =============================================================================
# Response formatting helpers (legacy rows)
# =============================================================================

def _headers() -> List[str]:
    return [
        "Rank",
        "Symbol",
        "Name",
        "Sector",
        "Current Price",
        "Forecast Price (12M)",
        "Expected ROI (12M)%",
        "Risk Score",
        "Confidence%",
        "Shariah",
        "Advisor Score",
        "Recommendation",
        "Weight%",
        "Allocated Amount",
    ]

def _rows_from_recs(recs: List[AdvisorRecommendation]) -> List[List[Any]]:
    rows: List[List[Any]] = []
    for r in recs:
        rows.append([
            r.rank,
            r.symbol,
            r.name or "",
            r.sector or "",
            round(r.current_price, 4) if r.current_price is not None else "",
            round(r.forecast_price_12m, 4) if r.forecast_price_12m is not None else "",
            round((r.expected_roi_12m or 0.0) * 100.0, 2) if r.expected_roi_12m is not None else "",
            round(r.risk_score, 1),
            round((r.confidence_score or 0.0) * 100.0, 1),
            "✓" if r.shariah == ShariahCompliance.COMPLIANT else ("✗" if r.shariah == ShariahCompliance.NON_COMPLIANT else ""),
            round(r.advisor_score, 1),
            r.recommendation,
            round(r.weight * 100.0, 2) if r.weight else "",
            round(r.allocated_amount, 2) if r.allocated_amount else "",
        ])
    return rows


# =============================================================================
# WebSocket Manager (kept simple)
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
                logger.error("Advisor WS broadcast error: %s", e)

_ws_manager = WebSocketManager()


# =============================================================================
# Routes
# =============================================================================

@router.get("/health")
async def advisor_health(request: Request) -> Dict[str, Any]:
    engine = await _advisor_engine.get_engine(request)
    return {
        "status": "ok",
        "version": ADVISOR_ROUTE_VERSION,
        "timestamp_riyadh": _saudi_time.format_iso(),
        "trading_day": _saudi_time.is_trading_day(),
        "engine_ready": bool(engine),
        "config": {
            "max_concurrent": _CONFIG.max_concurrent_requests,
            "default_top_n": _CONFIG.default_top_n,
            "allocation_method": _CONFIG.default_allocation_method.value,
            "enable_ml": _CONFIG.enable_ml_predictions,
            "enable_shariah": _CONFIG.enable_shariah_filter,
            "require_auth": _CONFIG.require_auth,
            "allow_query_token": _CONFIG.allow_query_token,
        },
        "websocket_connections": len(_ws_manager._active),
        "ml_initialized": _ml_models._initialized,
        "request_id": getattr(request.state, "request_id", None),
    }


@router.get("/metrics")
async def advisor_metrics() -> Response:
    if not _PROM_AVAILABLE:
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
    endpoint = "recommendations"
    request_id = x_request_id or getattr(request.state, "request_id", None) or str(uuid.uuid4())[:18]
    t0 = time.time()

    advisor_requests_total.labels(endpoint=endpoint, status="start").inc()

    try:
        # rate limit
        client_ip = request.client.host if request.client else "unknown"
        if not await _rate_limiter.check(client_ip, _CONFIG.rate_limit_requests, _CONFIG.rate_limit_window):
            advisor_requests_total.labels(endpoint=endpoint, status="rate_limited").inc()
            raise HTTPException(status_code=status.HTTP_429_TOO_MANY_REQUESTS, detail="Rate limit exceeded")

        # auth
        if not _auth_allowed(request, token, x_app_token, authorization):
            advisor_requests_total.labels(endpoint=endpoint, status="unauthorized").inc()
            raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid token")

        # parse
        async with TraceContext("advisor.parse", {"request_id": request_id}):
            try:
                req = AdvisorRequest.model_validate(body) if _PYDANTIC_V2 else AdvisorRequest.parse_obj(body)  # type: ignore
                req.request_id = request_id
            except Exception as e:
                asyncio.create_task(_audit.log(
                    event="advisor_validation_error",
                    action="validate",
                    status_s="error",
                    details={"error": str(e)},
                    request_id=request_id,
                ))
                advisor_requests_total.labels(endpoint=endpoint, status="bad_request").inc()
                return BestJSONResponse(
                    status_code=400,
                    content={
                        "status": "error",
                        "error": f"Invalid request: {e}",
                        "headers": [],
                        "rows": [],
                        "recommendations": [],
                        "items": [],
                        "request_id": request_id,
                        "version": ADVISOR_ROUTE_VERSION,
                        "meta": {"duration_ms": (time.time() - t0) * 1000},
                    },
                )

        symbols = _parse_symbols(req)
        top_n = int(req.top_n or _CONFIG.default_top_n)
        symbols = symbols[: top_n]

        if not symbols:
            advisor_requests_total.labels(endpoint=endpoint, status="skipped").inc()
            return BestJSONResponse(
                content={
                    "status": "skipped",
                    "error": "No symbols provided",
                    "headers": _headers(),
                    "rows": [],
                    "recommendations": [],
                    "items": [],
                    "request_id": request_id,
                    "version": ADVISOR_ROUTE_VERSION,
                    "meta": {"duration_ms": (time.time() - t0) * 1000},
                }
            )

        # warm ML
        if req.enable_ml_predictions and _CONFIG.enable_ml_predictions:
            asyncio.create_task(_ml_models.initialize())

        # engine
        async with TraceContext("advisor.get_engine", {"request_id": request_id}):
            engine = await _advisor_engine.get_engine(request)
        if not engine:
            advisor_requests_total.labels(endpoint=endpoint, status="engine_unavailable").inc()
            return BestJSONResponse(
                status_code=503,
                content={
                    "status": "error",
                    "error": "Advisor engine unavailable",
                    "headers": _headers(),
                    "rows": [],
                    "recommendations": [],
                    "items": [],
                    "request_id": request_id,
                    "version": ADVISOR_ROUTE_VERSION,
                    "meta": {"duration_ms": (time.time() - t0) * 1000},
                },
            )

        # fetch enriched
        async with TraceContext("advisor.fetch_enriched", {"count": len(symbols), "request_id": request_id}):
            data_map = await _advisor_engine.fetch_enriched_map(engine, symbols)

        # compute recs (no race on counters: aggregate after)
        sem = asyncio.Semaphore(max(1, int(_CONFIG.max_concurrent_requests)))

        async def build_one(sym: str) -> Tuple[Optional[AdvisorRecommendation], Dict[str, int]]:
            async with sem:
                st = {"ok": 0, "fail": 0, "ml": 0, "sh_ok": 0}
                raw = data_map.get(sym.upper()) or {"symbol": sym, "error": "not_found", "data_quality": "MISSING"}

                # normalize fields
                d = _to_dict(raw)
                symbol_out = _safe_str(d.get("symbol") or sym).upper()

                price = _safe_float(d.get("current_price") or d.get("price"))
                prev_close = _safe_float(d.get("previous_close") or d.get("prev_close"))
                expected_roi_12m = _as_ratio(d.get("expected_roi_12m"))
                forecast_price_12m = _safe_float(d.get("forecast_price_12m"))

                # shariah
                shariah = ShariahCompliance.NA
                if _CONFIG.enable_shariah_filter and (req.include_shariah or req.shariah_compliant):
                    shariah = await _shariah_checker.check(symbol_out, d)
                    if req.shariah_compliant and shariah != ShariahCompliance.COMPLIANT:
                        st["fail"] = 1
                        return None, st
                    if shariah == ShariahCompliance.COMPLIANT:
                        st["sh_ok"] = 1

                # features + ML
                features = MLFeatures(
                    symbol=symbol_out,
                    price=price,
                    previous_close=prev_close,
                    volume=_safe_int(d.get("volume")),
                    market_cap=_safe_float(d.get("market_cap")),
                    pe_ttm=_safe_float(d.get("pe_ttm") or d.get("pe_ratio")),
                    pb=_safe_float(d.get("pb") or d.get("pb_ratio")),
                    dividend_yield=_as_ratio(d.get("dividend_yield")),
                    beta=_safe_float(d.get("beta")),
                    volatility_30d=_as_ratio(d.get("volatility_30d")) or _safe_float(d.get("volatility_30d")),
                    momentum_14d=_as_ratio(d.get("momentum_14d")) or _as_ratio(d.get("returns_1m")),
                    rsi_14=_safe_float(d.get("rsi_14") or d.get("rsi_14d") or d.get("rsi_14d")),
                )

                ml_pred: Optional[MLPrediction] = None
                if req.enable_ml_predictions and _CONFIG.enable_ml_predictions:
                    try:
                        ml_pred = await asyncio.wait_for(_ml_models.predict(features, min_conf=req.min_confidence), timeout=float(_CONFIG.ml_prediction_timeout_sec))
                        if ml_pred:
                            st["ml"] = 1
                    except Exception:
                        ml_pred = None

                # risk/confidence
                risk = float(ml_pred.risk_score) if (ml_pred and ml_pred.risk_score is not None) else float(_safe_float(d.get("risk_score"), 50.0) or 50.0)
                conf = float(ml_pred.confidence) if (ml_pred and ml_pred.confidence is not None) else float(_safe_float(d.get("confidence_score"), 0.5) or 0.5)

                advisor_score = _compute_advisor_score(
                    ml=ml_pred,
                    expected_roi_12m=expected_roi_12m,
                    risk_score=risk,
                    confidence=conf,
                )

                recommendation = _label_recommendation(advisor_score)

                reasoning: List[str] = []
                if ml_pred and ml_pred.factors:
                    top = sorted(ml_pred.factors.items(), key=lambda x: abs(x[1]), reverse=True)[:3]
                    reasoning.extend([f"{k}={v:.3f}" for k, v in top])
                if shariah == ShariahCompliance.COMPLIANT:
                    reasoning.append("Shariah:COMPLIANT")
                elif shariah == ShariahCompliance.NON_COMPLIANT:
                    reasoning.append("Shariah:NON_COMPLIANT")

                # build recommendation object (rank assigned later)
                rec = AdvisorRecommendation(
                    rank=0,
                    symbol=symbol_out,
                    name=_safe_str(d.get("name") or d.get("company_name")) or None,
                    sector=_safe_str(d.get("sector")) or None,
                    current_price=price,
                    forecast_price_12m=forecast_price_12m,
                    expected_roi_12m=expected_roi_12m,
                    risk_score=max(0.0, min(100.0, risk)),
                    confidence_score=max(0.0, min(1.0, conf)),
                    shariah=shariah,
                    recommendation=recommendation,
                    advisor_score=float(advisor_score),
                    pe_ttm=_safe_float(d.get("pe_ttm") or d.get("pe_ratio")),
                    pb=_safe_float(d.get("pb") or d.get("pb_ratio")),
                    dividend_yield=_as_ratio(d.get("dividend_yield")),
                    market_cap=_safe_float(d.get("market_cap")),
                    rsi_14=_safe_float(d.get("rsi_14") or d.get("rsi_14d")),
                    volatility_30d=_as_ratio(d.get("volatility_30d")) or _safe_float(d.get("volatility_30d")),
                    ml_prediction=ml_pred,
                    reasoning=reasoning,
                    data_quality=_safe_str(d.get("data_quality"), "GOOD"),
                    data_source=_safe_str(d.get("data_source") or d.get("engine_source") or d.get("provider") or "engine"),
                    last_updated_utc=_safe_str(d.get("last_updated_utc"), datetime.now(timezone.utc).isoformat()),
                    last_updated_riyadh=_safe_str(d.get("last_updated_riyadh"), _saudi_time.format_iso()),
                )
                st["ok"] = 1
                return rec, st

        results = await asyncio.gather(*(build_one(s) for s in symbols), return_exceptions=False)

        recs: List[AdvisorRecommendation] = []
        ok = fail = ml = sh_ok = 0
        for rec, st in results:
            ok += st["ok"]
            fail += st["fail"]
            ml += st["ml"]
            sh_ok += st["sh_ok"]
            if isinstance(rec, AdvisorRecommendation):
                recs.append(rec)

        # sort by advisor_score desc, then by symbol for stability
        recs.sort(key=lambda r: (-float(r.advisor_score or 0.0), r.symbol))

        # assign rank 1..N (stable)
        for i, r in enumerate(recs, 1):
            r.rank = i

        # portfolio weights (optional)
        portfolio = None
        if recs and req.invest_amount:
            async with TraceContext("advisor.optimize", {"count": len(recs), "request_id": request_id}):
                portfolio = await _portfolio_optimizer.optimize(
                    [r.symbol for r in recs],
                    max_pos=_CONFIG.max_position_pct,
                    min_pos=_CONFIG.min_position_pct,
                )
            weights = (portfolio or {}).get("weights", {}) if isinstance(portfolio, dict) else {}
            for r in recs:
                r.weight = float(weights.get(r.symbol, 0.0) or 0.0)
                r.allocated_amount = float(r.weight * float(req.invest_amount))
                if r.expected_roi_12m is not None:
                    r.expected_gain_12m = r.allocated_amount * float(r.expected_roi_12m)

        # items (dict rows)
        items: List[Dict[str, Any]] = []
        for r in recs:
            items.append({
                "rank": r.rank,
                "symbol": r.symbol,
                "name": r.name,
                "sector": r.sector,
                "current_price": r.current_price,
                "forecast_price_12m": r.forecast_price_12m,
                "expected_roi_12m": r.expected_roi_12m,     # fraction
                "risk_score": r.risk_score,
                "confidence_score": r.confidence_score,    # 0..1
                "shariah": r.shariah,
                "advisor_score": r.advisor_score,
                "recommendation": r.recommendation,
                "weight": r.weight,
                "allocated_amount": r.allocated_amount,
                "expected_gain_12m": r.expected_gain_12m,
                "pe_ttm": r.pe_ttm,
                "pb": r.pb,
                "dividend_yield": r.dividend_yield,        # fraction
                "market_cap": r.market_cap,
                "rsi_14": r.rsi_14,
                "volatility_30d": r.volatility_30d,
                "data_quality": r.data_quality,
                "data_source": r.data_source,
                "last_updated_utc": r.last_updated_utc,
                "last_updated_riyadh": r.last_updated_riyadh,
                "ml_prediction": (_serialize_model(r.ml_prediction) if (r.ml_prediction is not None and isinstance(r.ml_prediction, BaseModel)) else (_to_dict(r.ml_prediction) if r.ml_prediction else None)),
                "reasoning": list(r.reasoning or []),
            })

        status_val = "success" if fail == 0 else ("partial" if ok > 0 else "error")

        asyncio.create_task(_audit.log(
            event="advisor_recommendations",
            action="generate",
            status_s=status_val,
            details={"requested": len(symbols), "returned": len(recs), "failed": fail, "ml": ml, "shariah_ok": sh_ok},
            request_id=request_id,
        ))

        resp = AdvisorResponse(
            status=status_val,
            error=f"{fail} symbols failed" if fail else None,
            headers=_headers(),
            rows=_rows_from_recs(recs),
            recommendations=recs,
            items=items,
            portfolio=portfolio,
            version=ADVISOR_ROUTE_VERSION,
            request_id=request_id,
            meta={
                "duration_ms": (time.time() - t0) * 1000,
                "requested": len(symbols),
                "returned": len(recs),
                "ok": ok,
                "fail": fail,
                "ml_predictions": ml,
                "shariah_compliant": sh_ok,
                "timestamp_utc": datetime.now(timezone.utc).isoformat(),
                "timestamp_riyadh": _saudi_time.format_iso(),
            },
        )

        advisor_requests_total.labels(endpoint=endpoint, status=status_val).inc()
        advisor_duration_seconds.labels(endpoint=endpoint).observe(max(0.0, time.time() - t0))

        return BestJSONResponse(content=_serialize_model(resp))

    except HTTPException:
        raise
    except Exception as e:
        logger.error("Advisor recommendations failed: %s\n%s", e, traceback.format_exc())
        advisor_requests_total.labels(endpoint=endpoint, status="error").inc()
        advisor_duration_seconds.labels(endpoint=endpoint).observe(max(0.0, time.time() - t0))

        return BestJSONResponse(
            status_code=500,
            content={
                "status": "error",
                "error": f"Internal Server Error: {e}",
                "headers": _headers(),
                "rows": [],
                "recommendations": [],
                "items": [],
                "request_id": request_id,
                "version": ADVISOR_ROUTE_VERSION,
                "meta": {"duration_ms": (time.time() - t0) * 1000},
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
    """Real-time streaming for live advisor updates (optional)."""
    await _ws_manager.connect(websocket)
    try:
        await websocket.send_text(json_dumps({
            "type": "connection",
            "status": "connected",
            "timestamp": _saudi_time.format_iso(),
            "version": ADVISOR_ROUTE_VERSION,
        }))

        while True:
            try:
                message = await websocket.receive_json()
                mtype = _safe_str(message.get("type")).lower()

                if mtype == "subscribe":
                    symbol = _safe_str(message.get("symbol")).upper()
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
                logger.error("Advisor WS error: %s", e)
                await websocket.send_text(json_dumps({"type": "error", "error": str(e), "timestamp": _saudi_time.format_iso()}))
    finally:
        await _ws_manager.disconnect(websocket)


@router.on_event("startup")
async def _startup_event():
    await _ws_manager.start()


@router.on_event("shutdown")
async def _shutdown_event():
    await _ws_manager.stop()


__all__ = ["router"]
