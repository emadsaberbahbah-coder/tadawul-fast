#!/usr/bin/env python3
"""
core/scoring_engine.py
===========================================================
ADVANCED SCORING & FORECASTING ENGINE v3.0.1 (PROD-HARDENED)
===========================================================
(Emad Bahbah – Institutional Grade Quantitative Analysis)

v3.0.1 Fix Pack (critical correctness / runtime)
- ✅ FIX: removed accidental duplicated file content (your paste had the full file repeated twice)
- ✅ FIX: missing imports (re, time) and missing helper methods (MetricExtractor.get_int)
- ✅ FIX: undefined symbols (HAS_NUMPY) and safe NumPy detection
- ✅ FIX: forecast ROI clamping now supports negative ranges correctly (no accidental 0..100 clamp)
- ✅ FIX: __all__ list was corrupted at EOF; now clean and valid
- ✅ SAFE: optional libs are truly optional (module imports without sklearn/xgb/lgb/shap/optuna/joblib)

Core Capabilities:
- Multi-factor scoring (Value, Quality, Momentum, Risk, Opportunity)
- Forecast generation with confidence calibration + trend/risk/volatility multipliers
- News sentiment delta (if provided)
- Badges + recommendation
- Explainability hooks (SHAP if available)
- Never raises from public API wrappers
"""

from __future__ import annotations

import asyncio
import logging
import math
import re
import statistics
import time
from collections import defaultdict
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta, timezone
from enum import Enum
from typing import Any, Dict, List, Optional, Set, Tuple

# ---------------------------------------------------------------------------
# High-Performance JSON fallback
# ---------------------------------------------------------------------------
try:
    import orjson

    def json_dumps(v: Any, *, default=str) -> str:
        return orjson.dumps(v, default=default).decode("utf-8")

    def json_loads(v: Any) -> Any:
        return orjson.loads(v)

    _HAS_ORJSON = True
except Exception:
    import json

    def json_dumps(v: Any, *, default=str) -> str:
        return json.dumps(v, default=default, ensure_ascii=False)

    def json_loads(v: Any) -> Any:
        return json.loads(v)

    _HAS_ORJSON = False

# ---------------------------------------------------------------------------
# Optional numeric + ML stack
#   (keep numpy separate; sklearn may be missing)
# ---------------------------------------------------------------------------
try:
    import numpy as np  # type: ignore

    _NUMPY_AVAILABLE = True
except Exception:
    np = None  # type: ignore
    _NUMPY_AVAILABLE = False

try:
    from sklearn.ensemble import GradientBoostingRegressor, RandomForestRegressor  # type: ignore
    from sklearn.metrics import mean_squared_error, r2_score  # type: ignore
    from sklearn.preprocessing import StandardScaler  # type: ignore

    _SKLEARN_AVAILABLE = True
except Exception:
    GradientBoostingRegressor = None  # type: ignore
    RandomForestRegressor = None  # type: ignore
    StandardScaler = None  # type: ignore
    mean_squared_error = None  # type: ignore
    r2_score = None  # type: ignore
    _SKLEARN_AVAILABLE = False

try:
    import xgboost as xgb  # type: ignore

    _XGB_AVAILABLE = True
except Exception:
    xgb = None  # type: ignore
    _XGB_AVAILABLE = False

try:
    import lightgbm as lgb  # type: ignore

    _LGB_AVAILABLE = True
except Exception:
    lgb = None  # type: ignore
    _LGB_AVAILABLE = False

try:
    import shap  # type: ignore

    _SHAP_AVAILABLE = True
except Exception:
    shap = None  # type: ignore
    _SHAP_AVAILABLE = False

try:
    import optuna  # type: ignore

    _OPTUNA_AVAILABLE = True
except Exception:
    optuna = None  # type: ignore
    _OPTUNA_AVAILABLE = False

try:
    import joblib  # type: ignore

    _JOBLIB_AVAILABLE = True
except Exception:
    joblib = None  # type: ignore
    _JOBLIB_AVAILABLE = False

# ---------------------------------------------------------------------------
# Pydantic (v2 preferred, v1 fallback)
# ---------------------------------------------------------------------------
try:
    from pydantic import BaseModel, ConfigDict, Field, field_validator  # type: ignore

    _PYDANTIC_V2 = True
except Exception:
    from pydantic import BaseModel, Field, validator  # type: ignore

    ConfigDict = None  # type: ignore
    field_validator = None  # type: ignore
    _PYDANTIC_V2 = False

logger = logging.getLogger("core.scoring_engine")

SCORING_ENGINE_VERSION = "3.0.1"

_UTC = timezone.utc
_RIYADH_TZ = timezone(timedelta(hours=3))


# =============================================================================
# Enums
# =============================================================================
class Recommendation(str, Enum):
    STRONG_BUY = "STRONG_BUY"
    BUY = "BUY"
    HOLD = "HOLD"
    REDUCE = "REDUCE"
    SELL = "SELL"
    STRONG_SELL = "STRONG_SELL"

    @classmethod
    def from_score(cls, score: float) -> "Recommendation":
        if score >= 85:
            return cls.STRONG_BUY
        if score >= 70:
            return cls.BUY
        if score >= 45:
            return cls.HOLD
        if score >= 30:
            return cls.REDUCE
        if score >= 15:
            return cls.SELL
        return cls.STRONG_SELL


class BadgeLevel(str, Enum):
    ELITE = "ELITE"
    EXCELLENT = "EXCELLENT"
    GOOD = "GOOD"
    NEUTRAL = "NEUTRAL"
    CAUTION = "CAUTION"
    DANGER = "DANGER"
    NONE = "NONE"


class TrendDirection(str, Enum):
    STRONG_UPTREND = "STRONG_UPTREND"
    UPTREND = "UPTREND"
    SIDEWAYS = "SIDEWAYS"
    DOWNTREND = "DOWNTREND"
    STRONG_DOWNTREND = "STRONG_DOWNTREND"
    NEUTRAL = "NEUTRAL"


class DataQuality(str, Enum):
    EXCELLENT = "EXCELLENT"
    HIGH = "HIGH"
    MEDIUM = "MEDIUM"
    LOW = "LOW"
    STALE = "STALE"
    ERROR = "ERROR"
    NONE = "NONE"


class ModelType(str, Enum):
    GRADIENT_BOOSTING = "gradient_boosting"
    RANDOM_FOREST = "random_forest"
    XGBOOST = "xgboost"
    LIGHTGBM = "lightgbm"
    ENSEMBLE = "ensemble"
    LINEAR = "linear"
    NEURAL = "neural"


# =============================================================================
# Helpers (safe coercions)
# =============================================================================
def now_utc() -> datetime:
    return datetime.now(_UTC)


def now_riyadh() -> datetime:
    return datetime.now(_RIYADH_TZ)


def utc_to_riyadh(dt: Optional[datetime]) -> Optional[datetime]:
    if dt is None:
        return None
    try:
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=_UTC)
        return dt.astimezone(_RIYADH_TZ)
    except Exception:
        return None


def clamp(value: float, min_val: float = 0.0, max_val: float = 100.0) -> float:
    return max(min_val, min(max_val, value))


def safe_str(value: Any, default: str = "") -> str:
    try:
        return str(value).strip() if value is not None else default
    except Exception:
        return default


def safe_float(value: Any, default: Optional[float] = None) -> Optional[float]:
    if value is None:
        return default
    try:
        if isinstance(value, bool):
            return default
        if isinstance(value, (int, float)):
            f = float(value)
            if math.isnan(f) or math.isinf(f):
                return default
            return f
        if isinstance(value, str):
            s = value.strip()
            if not s:
                return default
            # keep digits, minus, dot, comma, exp, percent
            cleaned = re.sub(r"[^\d.,\-eE%]", "", s)
            if not cleaned or cleaned in ("-", ".", ","):
                return default
            if "%" in cleaned:
                cleaned = cleaned.replace("%", "")
            # fix thousands separators
            if "," in cleaned and "." in cleaned:
                if cleaned.rindex(",") > cleaned.rindex("."):
                    cleaned = cleaned.replace(".", "").replace(",", ".")
                else:
                    cleaned = cleaned.replace(",", "")
            elif "," in cleaned:
                cleaned = cleaned.replace(",", ".")
            # handle multiple dots
            if cleaned.count(".") > 1:
                cleaned = cleaned.replace(".", "", cleaned.count(".") - 1)
            f = float(cleaned)
            if math.isnan(f) or math.isinf(f):
                return default
            return f
    except Exception:
        return default
    return default


def safe_int(value: Any, default: Optional[int] = None) -> Optional[int]:
    f = safe_float(value, None)
    return int(round(f)) if f is not None else default


def safe_bool(value: Any, default: bool = False) -> bool:
    if value is None:
        return default
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return bool(value)
    if isinstance(value, str):
        s = value.strip().upper()
        if s in ("TRUE", "YES", "Y", "1", "ON", "ENABLED"):
            return True
        if s in ("FALSE", "NO", "N", "0", "OFF", "DISABLED"):
            return False
    return default


def safe_percent(value: Any, default: Optional[float] = None) -> Optional[float]:
    """
    If value looks like 0.12 -> treat as 12%.
    If value looks like 12 -> treat as 12%.
    """
    v = safe_float(value, None)
    if v is None:
        return default
    if v == 0.0:
        return 0.0
    return v * 100.0 if abs(v) <= 1.5 else v


def safe_datetime(value: Any) -> Optional[datetime]:
    if value is None:
        return None
    try:
        if isinstance(value, datetime):
            return value.replace(tzinfo=_UTC) if value.tzinfo is None else value
        if isinstance(value, str):
            s = value.strip()
            if not s:
                return None
            try:
                dt = datetime.fromisoformat(s.replace("Z", "+00:00"))
                return dt.replace(tzinfo=_UTC) if dt.tzinfo is None else dt
            except Exception:
                pass
            for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S", "%d/%m/%Y %H:%M:%S"):
                try:
                    return datetime.strptime(s, fmt).replace(tzinfo=_UTC)
                except Exception:
                    continue
    except Exception:
        return None
    return None


# =============================================================================
# Scoring utilities
# =============================================================================
def percentile_rank(value: float, distribution: List[float]) -> float:
    if not distribution:
        return 50.0
    try:
        count_less = sum(1 for x in distribution if x < value)
        count_equal = sum(1 for x in distribution if x == value)
        return ((count_less + 0.5 * count_equal) / len(distribution)) * 100.0
    except Exception:
        return 50.0


def score_linear(value: Optional[float], thresholds: List[Tuple[float, float]], default: float = 50.0) -> float:
    if value is None or not thresholds:
        return default
    thresholds = sorted(thresholds, key=lambda t: t[0])
    if value <= thresholds[0][0]:
        return thresholds[0][1]
    if value >= thresholds[-1][0]:
        return thresholds[-1][1]
    for i in range(1, len(thresholds)):
        x1, s1 = thresholds[i - 1]
        x2, s2 = thresholds[i]
        if x1 <= value <= x2:
            if x2 == x1:
                return s2
            return s1 + ((value - x1) / (x2 - x1)) * (s2 - s1)
    return default


def calculate_badge(score: float, thresholds: Dict[BadgeLevel, Tuple[float, float]], default: BadgeLevel = BadgeLevel.NEUTRAL) -> BadgeLevel:
    for badge, (mn, mx) in thresholds.items():
        if mn <= score <= mx:
            return badge
    return default


# =============================================================================
# Data Models
# =============================================================================
@dataclass(slots=True)
class ScoringWeights:
    value: float = 0.28
    quality: float = 0.26
    momentum: float = 0.22
    risk: float = 0.14
    opportunity: float = 0.10

    def validate(self) -> "ScoringWeights":
        total = self.value + self.quality + self.momentum + self.risk + self.opportunity
        if total <= 0:
            return self
        if abs(total - 1.0) > 0.01:
            scale = 1.0 / total
            self.value *= scale
            self.quality *= scale
            self.momentum *= scale
            self.risk *= scale
            self.opportunity *= scale
        return self

    def to_dict(self) -> Dict[str, float]:
        return asdict(self)


@dataclass(slots=True)
class ForecastParameters:
    horizon_1m: float = 30.0
    horizon_3m: float = 90.0
    horizon_6m: float = 180.0
    horizon_12m: float = 365.0

    max_roi_1m: float = 35.0
    max_roi_3m: float = 45.0
    max_roi_6m: float = 55.0
    max_roi_12m: float = 65.0

    min_roi_1m: float = -35.0
    min_roi_3m: float = -45.0
    min_roi_6m: float = -55.0
    min_roi_12m: float = -65.0

    enable_ml_forecast: bool = True
    ml_model_version: Optional[str] = None


class AssetScores(BaseModel):
    value_score: float = Field(50.0, ge=0, le=100)
    quality_score: float = Field(50.0, ge=0, le=100)
    momentum_score: float = Field(50.0, ge=0, le=100)
    risk_score: float = Field(50.0, ge=0, le=100)
    opportunity_score: float = Field(50.0, ge=0, le=100)
    overall_score: float = Field(50.0, ge=0, le=100)

    recommendation: Recommendation = Field(Recommendation.HOLD)
    recommendation_raw: Optional[str] = None
    recommendation_confidence: float = Field(50.0, ge=0, le=100)
    recommendation_strength: Optional[float] = None

    rec_badge: Optional[BadgeLevel] = None
    value_badge: Optional[BadgeLevel] = None
    quality_badge: Optional[BadgeLevel] = None
    momentum_badge: Optional[BadgeLevel] = None
    risk_badge: Optional[BadgeLevel] = None
    opportunity_badge: Optional[BadgeLevel] = None

    data_confidence: float = Field(50.0, ge=0, le=100)
    data_quality: Optional[DataQuality] = None
    data_completeness: float = Field(0.0, ge=0, le=100)
    data_freshness: Optional[float] = None

    forecast_price_1m: Optional[float] = None
    forecast_price_3m: Optional[float] = None
    forecast_price_6m: Optional[float] = None
    forecast_price_12m: Optional[float] = None

    expected_roi_1m: Optional[float] = None
    expected_roi_3m: Optional[float] = None
    expected_roi_6m: Optional[float] = None
    expected_roi_12m: Optional[float] = None

    forecast_confidence: Optional[float] = None
    forecast_method: Optional[str] = None
    forecast_model: Optional[str] = None
    forecast_parameters: Optional[Dict[str, Any]] = None
    forecast_ensemble: Optional[Dict[str, Any]] = None

    ml_explanations: Optional[Dict[str, Any]] = None
    feature_importance: Optional[Dict[str, float]] = None
    shap_values: Optional[Dict[str, Any]] = None

    forecast_updated_utc: Optional[datetime] = None
    forecast_updated_riyadh: Optional[datetime] = None
    scoring_updated_utc: Optional[datetime] = None
    scoring_updated_riyadh: Optional[datetime] = None

    scoring_reason: List[str] = Field(default_factory=list)
    scoring_warnings: List[str] = Field(default_factory=list)
    scoring_errors: List[str] = Field(default_factory=list)
    scoring_version: str = SCORING_ENGINE_VERSION
    scoring_model_version: Optional[str] = None

    component_scores: Dict[str, float] = Field(default_factory=dict)
    input_metrics: Dict[str, Any] = Field(default_factory=dict)
    normalized_metrics: Dict[str, float] = Field(default_factory=dict)

    performance_metrics: Optional[Dict[str, float]] = None

    # Compatibility aliases (used by some routes)
    expected_return_1m: Optional[float] = None
    expected_return_3m: Optional[float] = None
    expected_return_6m: Optional[float] = None
    expected_return_12m: Optional[float] = None
    expected_price_1m: Optional[float] = None
    expected_price_3m: Optional[float] = None
    expected_price_6m: Optional[float] = None
    expected_price_12m: Optional[float] = None
    confidence_score: Optional[float] = None

    if _PYDANTIC_V2:
        model_config = ConfigDict(
            extra="ignore",
            validate_assignment=True,
            json_encoders={
                datetime: lambda v: v.isoformat() if v else None,
                Enum: lambda v: v.value if v else None,
            },
        )

        @field_validator("recommendation", mode="before")
        @classmethod
        def _validate_rec(cls, v: Any) -> Recommendation:
            if isinstance(v, Recommendation):
                return v
            s = safe_str(v).upper().replace(" ", "_")
            for r in Recommendation:
                if r.value == s or r.name == s:
                    return r
            return Recommendation.HOLD
    else:
        class Config:
            extra = "ignore"
            validate_assignment = True
            json_encoders = {
                datetime: lambda v: v.isoformat() if v else None,
                Enum: lambda v: v.value if v else None,
            }

        @validator("recommendation", pre=True)
        def _validate_rec_v1(cls, v):
            if isinstance(v, Recommendation):
                return v
            s = safe_str(v).upper().replace(" ", "_")
            for r in Recommendation:
                if r.value == s or r.name == s:
                    return r
            return Recommendation.HOLD


# =============================================================================
# Metric extractor
# =============================================================================
class MetricExtractor:
    """Pull metrics from dict/object safely (cached)."""

    def __init__(self, source: Any):
        self.source = source
        self._cache: Dict[str, Any] = {}

    def _extract(self, name: str) -> Any:
        if self.source is None:
            return None
        if isinstance(self.source, dict):
            return self.source.get(name)
        try:
            return getattr(self.source, name, None)
        except Exception:
            return None

    def get(self, *names: str, default: Any = None) -> Any:
        for name in names:
            if name in self._cache:
                return self._cache[name]
            value = self._extract(name)
            if value is not None:
                self._cache[name] = value
                return value
        return default

    def get_float(self, *names: str, default: Optional[float] = None) -> Optional[float]:
        return safe_float(self.get(*names), default)

    def get_int(self, *names: str, default: Optional[int] = None) -> Optional[int]:
        return safe_int(self.get(*names), default)

    def get_percent(self, *names: str, default: Optional[float] = None) -> Optional[float]:
        return safe_percent(self.get(*names), default)

    def get_str(self, *names: str, default: str = "") -> str:
        return safe_str(self.get(*names), default)

    def get_bool(self, *names: str, default: bool = False) -> bool:
        return safe_bool(self.get(*names), default)

    def get_datetime(self, *names: str, default: Optional[datetime] = None) -> Optional[datetime]:
        return safe_datetime(self.get(*names)) or default


# =============================================================================
# ML Model wrapper (optional)
# =============================================================================
class MLModel:
    def __init__(self, model_type: ModelType, version: str = "1.0.0", parameters: Optional[Dict[str, Any]] = None):
        self.model_type = model_type
        self.version = version
        self.parameters = parameters or {}
        self.model = None
        self.scaler = None
        self.feature_importance: Dict[str, float] = {}
        self.training_metrics: Dict[str, float] = {}
        self.created_at = now_utc()
        self.updated_at = now_utc()

    def build(self) -> None:
        if not _SKLEARN_AVAILABLE:
            return

        params = dict(self.parameters)
        if self.model_type == ModelType.GRADIENT_BOOSTING:
            self.model = GradientBoostingRegressor(
                n_estimators=params.pop("n_estimators", 100),
                learning_rate=params.pop("learning_rate", 0.1),
                max_depth=params.pop("max_depth", 5),
                random_state=42,
                **params,
            )
        elif self.model_type == ModelType.RANDOM_FOREST:
            self.model = RandomForestRegressor(
                n_estimators=params.pop("n_estimators", 100),
                max_depth=params.pop("max_depth", 10),
                random_state=42,
                **params,
            )
        elif self.model_type == ModelType.XGBOOST and _XGB_AVAILABLE:
            self.model = xgb.XGBRegressor(
                n_estimators=params.pop("n_estimators", 100),
                max_depth=params.pop("max_depth", 5),
                learning_rate=params.pop("learning_rate", 0.1),
                random_state=42,
                **params,
            )
        elif self.model_type == ModelType.LIGHTGBM and _LGB_AVAILABLE:
            self.model = lgb.LGBMRegressor(
                n_estimators=params.pop("n_estimators", 100),
                max_depth=params.pop("max_depth", 5),
                learning_rate=params.pop("learning_rate", 0.1),
                random_state=42,
                **params,
            )

        if self.model is not None:
            self.scaler = StandardScaler()
            self.updated_at = now_utc()

    def train(self, X, y, feature_names: List[str]) -> Dict[str, float]:
        if not _SKLEARN_AVAILABLE:
            return {}
        if self.model is None:
            self.build()
        if self.model is None or self.scaler is None:
            return {}

        X_scaled = self.scaler.fit_transform(X)
        self.model.fit(X_scaled, y)

        if hasattr(self.model, "feature_importances_"):
            try:
                self.feature_importance = {n: float(v) for n, v in zip(feature_names, self.model.feature_importances_)}
            except Exception:
                self.feature_importance = {}

        try:
            y_pred = self.model.predict(X_scaled)
            self.training_metrics = {
                "mse": float(mean_squared_error(y, y_pred)),
                "rmse": float(math.sqrt(mean_squared_error(y, y_pred))),
                "r2": float(r2_score(y, y_pred)),
            }
        except Exception:
            self.training_metrics = {}

        self.updated_at = now_utc()
        return self.training_metrics

    def predict(self, X):
        if not _SKLEARN_AVAILABLE or self.model is None or self.scaler is None:
            if _NUMPY_AVAILABLE:
                return np.zeros(X.shape[0])
            return [0.0 for _ in range(len(X))]
        X_scaled = self.scaler.transform(X)
        return self.model.predict(X_scaled)

    def save(self, path: str) -> bool:
        if not _JOBLIB_AVAILABLE:
            return False
        try:
            joblib.dump(
                {
                    "model_type": self.model_type.value,
                    "version": self.version,
                    "parameters": self.parameters,
                    "model": self.model,
                    "scaler": self.scaler,
                    "feature_importance": self.feature_importance,
                    "training_metrics": self.training_metrics,
                    "created_at": self.created_at.isoformat(),
                    "updated_at": self.updated_at.isoformat(),
                },
                path,
            )
            return True
        except Exception:
            return False

    @classmethod
    def load(cls, path: str) -> Optional["MLModel"]:
        if not _JOBLIB_AVAILABLE:
            return None
        try:
            data = joblib.load(path)
            m = cls(ModelType(data["model_type"]), data.get("version", "1.0.0"), data.get("parameters", {}))
            m.model = data.get("model")
            m.scaler = data.get("scaler")
            m.feature_importance = data.get("feature_importance", {}) or {}
            m.training_metrics = data.get("training_metrics", {}) or {}
            m.created_at = safe_datetime(data.get("created_at")) or now_utc()
            m.updated_at = safe_datetime(data.get("updated_at")) or now_utc()
            return m
        except Exception:
            return None


# =============================================================================
# WebSocket Manager (generic)
# =============================================================================
class WebSocketManager:
    def __init__(self):
        self.connections: Set[Any] = set()
        self.subscriptions: Dict[str, Set[Any]] = defaultdict(set)
        self._lock = asyncio.Lock()

    async def connect(self, websocket: Any) -> None:
        async with self._lock:
            self.connections.add(websocket)

    async def disconnect(self, websocket: Any) -> None:
        async with self._lock:
            self.connections.discard(websocket)
            for symbol in list(self.subscriptions.keys()):
                self.subscriptions[symbol].discard(websocket)

    async def subscribe(self, websocket: Any, symbol: str) -> None:
        async with self._lock:
            self.subscriptions[symbol].add(websocket)

    async def broadcast(self, data: Dict[str, Any], symbol: Optional[str] = None) -> None:
        connections = self.subscriptions.get(symbol, set()) if symbol else self.connections
        if not connections:
            return
        payload = json_dumps(data, default=str)
        dead: List[Any] = []
        for ws in connections:
            try:
                # generic .send(text) pattern (works for websockets lib)
                await ws.send(payload)
            except Exception:
                dead.append(ws)
        for ws in dead:
            await self.disconnect(ws)


# =============================================================================
# Explainability (optional)
# =============================================================================
class SHAPExplainer:
    def __init__(self, model: Any, feature_names: List[str]):
        self.model = model
        self.feature_names = feature_names
        self.explainer = None
        self.initialized = False
        if _SHAP_AVAILABLE:
            try:
                self.explainer = shap.TreeExplainer(model)
                self.initialized = True
            except Exception:
                self.initialized = False

    def explain(self, X) -> Dict[str, Any]:
        if not self.initialized or not _SHAP_AVAILABLE:
            return {}
        try:
            shap_values = self.explainer.shap_values(X)
            if _NUMPY_AVAILABLE:
                mean_shap = np.abs(shap_values).mean(axis=0)
                explanations = {n: float(v) for n, v in zip(self.feature_names, mean_shap)}
                total = sum(explanations.values())
                if total > 0:
                    explanations = {k: v / total for k, v in explanations.items()}
            else:
                explanations = {}

            return {
                "feature_importance": explanations,
                "base_value": float(getattr(self.explainer, "expected_value", 0.0)),
            }
        except Exception:
            return {}


# =============================================================================
# Core Engine
# =============================================================================
class ScoringEngine:
    def __init__(
        self,
        weights: Optional[ScoringWeights] = None,
        ml_model: Optional[MLModel] = None,
        forecast_params: Optional[ForecastParameters] = None,
    ):
        self.weights = (weights or ScoringWeights()).validate()
        self.ml_model = ml_model
        self.forecast_params = forecast_params or ForecastParameters()
        self.ws_manager = WebSocketManager()
        self.performance_metrics: Dict[str, List[float]] = defaultdict(list)
        self.model_version = SCORING_ENGINE_VERSION

        self.badge_thresholds = {
            BadgeLevel.ELITE: (95, 100),
            BadgeLevel.EXCELLENT: (80, 95),
            BadgeLevel.GOOD: (60, 80),
            BadgeLevel.NEUTRAL: (40, 60),
            BadgeLevel.CAUTION: (20, 40),
            BadgeLevel.DANGER: (0, 20),
        }
        self.risk_badge_thresholds = {
            BadgeLevel.ELITE: (0, 5),
            BadgeLevel.EXCELLENT: (5, 20),
            BadgeLevel.GOOD: (20, 40),
            BadgeLevel.NEUTRAL: (40, 60),
            BadgeLevel.CAUTION: (60, 80),
            BadgeLevel.DANGER: (80, 100),
        }

    def compute_scores(self, source: Any) -> AssetScores:
        t0 = time.time()
        ex = MetricExtractor(source)

        reasons: List[str] = []
        warnings_list: List[str] = []
        components: Dict[str, float] = {}
        normalized: Dict[str, float] = {}

        # -------- Extract (minimal set used by downstream routes) --------
        pe = ex.get_float("pe_ttm", "pe", "price_earnings")
        forward_pe = ex.get_float("forward_pe", "pe_forward", "forward_price_earnings")
        pb = ex.get_float("pb", "pb_ratio", "price_book")
        ev_ebitda = ex.get_float("ev_ebitda", "ev_to_ebitda", "evebitda")
        peg = ex.get_float("peg", "peg_ratio")

        dividend_yield = ex.get_percent("dividend_yield", "div_yield", "dividend_yield_percent")

        roe = ex.get_percent("roe", "return_on_equity")
        roa = ex.get_percent("roa", "return_on_assets")
        gross_margin = ex.get_percent("gross_margin", "gross_profit_margin")
        operating_margin = ex.get_percent("operating_margin", "operating_profit_margin")
        net_margin = ex.get_percent("net_margin", "profit_margin")

        revenue_growth_yoy = ex.get_percent("revenue_growth_yoy", "revenue_growth", "rev_growth")
        eps_growth_yoy = ex.get_percent("eps_growth_yoy", "eps_growth", "earnings_growth")

        debt_to_equity = ex.get_float("debt_to_equity", "debt_equity", "dte")
        current_ratio = ex.get_float("current_ratio")
        interest_coverage = ex.get_float("interest_coverage", "int_coverage")

        beta = ex.get_float("beta", "beta_5y")
        volatility = ex.get_percent("volatility_30d", "vol_30d", "volatility")
        max_drawdown = ex.get_percent("max_drawdown_90d", "max_drawdown")

        price = ex.get_float("price", "current_price", "last_price", "close")
        percent_change = ex.get_percent("percent_change", "change_percent", "pct_change")

        rsi = ex.get_float("rsi_14", "rsi14", "rsi")
        macd = ex.get_float("macd_histogram", "macd_hist", "macd")
        ma20 = ex.get_float("ma20", "sma20")
        ma50 = ex.get_float("ma50", "sma50")
        ma200 = ex.get_float("ma200", "sma200")

        high_52w = ex.get_float("week_52_high", "high_52w", "fifty_two_week_high")
        low_52w = ex.get_float("week_52_low", "low_52w", "fifty_two_week_low")
        pos_52w = ex.get_percent("position_52w_percent", "week_52_position", "pos_52w_pct")

        volume = ex.get_float("volume", "vol")
        avg_volume = ex.get_float("avg_volume_30d", "avg_volume", "average_volume")
        market_cap = ex.get_float("market_cap", "mkt_cap")
        free_float = ex.get_percent("free_float", "free_float_percent")
        liquidity_score = ex.get_float("liquidity_score", "liq_score")

        fair_value = ex.get_float("fair_value", "intrinsic_value", "target_mean_price")
        upside = ex.get_percent("upside_percent", "upside", "upside_pct")

        news_score = ex.get_float("news_score", "news_sentiment", "sentiment")

        dq_str = ex.get_str("data_quality", "dq", "quality").upper()
        data_quality = self._parse_data_quality(dq_str)

        last_updated = ex.get_datetime("last_updated_utc", "updated_utc")
        data_freshness = (now_utc() - last_updated).total_seconds() / 3600.0 if last_updated else None

        # -------- Value score --------
        value_parts: List[float] = []
        pe_used = forward_pe or pe
        if pe_used is not None and pe_used > 0:
            pe_score = score_linear(pe_used, [(5, 90), (10, 80), (15, 70), (20, 60), (25, 50), (30, 40), (40, 30), (50, 20)])
            value_parts.append(pe_score)
            normalized["pe_normalized"] = pe_score / 100.0
            reasons.append(f"P/E: {pe_used:.1f}")

        if pb is not None and pb > 0:
            pb_score = score_linear(pb, [(0.5, 90), (1, 80), (1.5, 70), (2, 60), (3, 50), (4, 40), (5, 30)])
            value_parts.append(pb_score)
            normalized["pb_normalized"] = pb_score / 100.0
            reasons.append(f"P/B: {pb:.2f}")

        if ev_ebitda is not None and ev_ebitda > 0:
            ev_score = score_linear(ev_ebitda, [(4, 90), (6, 80), (8, 70), (10, 60), (12, 50), (15, 40), (20, 30)])
            value_parts.append(ev_score)
            normalized["ev_normalized"] = ev_score / 100.0
            reasons.append(f"EV/EBITDA: {ev_ebitda:.1f}")

        if dividend_yield is not None:
            dy_score = score_linear(dividend_yield, [(0, 30), (1, 50), (2, 70), (3, 85), (4, 90), (5, 85), (6, 80), (8, 70)])
            value_parts.append(dy_score)
            normalized["dy_normalized"] = dy_score / 100.0
            reasons.append(f"Div Yield: {dividend_yield:.1f}%")

        upside_value = upside
        if upside_value is None and fair_value and price and price > 0:
            upside_value = ((fair_value / price) - 1.0) * 100.0

        if upside_value is not None:
            up_score = score_linear(upside_value, [(-20, 20), (-10, 30), (0, 40), (10, 60), (20, 75), (30, 85), (50, 95)])
            value_parts.append(up_score * 0.8)
            normalized["upside_normalized"] = up_score / 100.0
            reasons.append(f"Upside: {upside_value:.1f}%")

        if peg is not None and peg > 0:
            peg_score = score_linear(peg, [(0.5, 95), (1, 80), (1.5, 65), (2, 50), (2.5, 35)])
            value_parts.append(peg_score)
            normalized["peg_normalized"] = peg_score / 100.0
            reasons.append(f"PEG: {peg:.2f}")

        value_score = sum(value_parts) / len(value_parts) if value_parts else 50.0
        value_score = clamp(value_score)
        components["value"] = value_score

        # -------- Quality score --------
        quality_parts: List[float] = []
        if roe is not None:
            roe_score = score_linear(roe, [(-10, 20), (0, 35), (5, 50), (10, 65), (15, 75), (20, 85), (25, 90), (30, 95)])
            quality_parts.append(roe_score)
            normalized["roe_normalized"] = roe_score / 100.0
            reasons.append(f"ROE: {roe:.1f}%")

        if roa is not None:
            roa_score = score_linear(roa, [(-5, 25), (0, 40), (3, 50), (5, 60), (8, 70), (12, 80), (15, 85), (20, 90)])
            quality_parts.append(roa_score)
            normalized["roa_normalized"] = roa_score / 100.0
            reasons.append(f"ROA: {roa:.1f}%")

        margin_scores: List[float] = []
        if gross_margin is not None:
            margin_scores.append(score_linear(gross_margin, [(10, 30), (20, 45), (30, 60), (40, 75), (50, 85), (60, 90)]))
        if operating_margin is not None:
            margin_scores.append(score_linear(operating_margin, [(5, 30), (10, 45), (15, 60), (20, 75), (25, 85), (30, 90)]))
        if net_margin is not None:
            margin_scores.append(score_linear(net_margin, [(2, 30), (5, 45), (10, 60), (15, 75), (20, 85), (25, 90)]))
        if margin_scores:
            quality_parts.append(sum(margin_scores) / len(margin_scores))

        growth_scores: List[float] = []
        if revenue_growth_yoy is not None:
            growth_scores.append(score_linear(revenue_growth_yoy, [(-20, 20), (-10, 30), (0, 40), (10, 60), (20, 75), (30, 85), (50, 95)]))
        if eps_growth_yoy is not None:
            growth_scores.append(score_linear(eps_growth_yoy, [(-30, 15), (-15, 25), (0, 40), (10, 60), (20, 75), (30, 85), (50, 95)]))
        if growth_scores:
            quality_parts.append(sum(growth_scores) / len(growth_scores))

        health_penalty = 0.0
        if debt_to_equity is not None:
            if debt_to_equity > 2.0:
                health_penalty += 15
                warnings_list.append(f"High D/E: {debt_to_equity:.2f}")
            elif debt_to_equity > 1.0:
                health_penalty += 7
        if current_ratio is not None:
            if current_ratio < 1.0:
                health_penalty += 15
                warnings_list.append(f"Low Current Ratio: {current_ratio:.2f}")
            elif current_ratio < 1.5:
                health_penalty += 7
        if interest_coverage is not None:
            if interest_coverage < 1.5:
                health_penalty += 20
                warnings_list.append(f"Low Interest Coverage: {interest_coverage:.2f}")
            elif interest_coverage < 3:
                health_penalty += 10

        quality_score = (sum(quality_parts) / len(quality_parts)) if quality_parts else 50.0
        quality_score = clamp(quality_score - health_penalty)
        components["quality"] = quality_score

        # -------- Momentum score --------
        mom_parts: List[float] = []
        if percent_change is not None:
            pc_score = score_linear(percent_change, [(-10, 25), (-5, 35), (0, 50), (2, 60), (5, 70), (8, 80), (12, 85), (15, 90)])
            mom_parts.append(pc_score * 1.5)
            normalized["momentum_normalized"] = pc_score / 100.0

        if price is not None:
            ma_score = 0.0
            ma_count = 0
            if ma20 is not None:
                ma_score += 15 if price > ma20 else -15
                ma_count += 1
            if ma50 is not None:
                ma_score += 10 if price > ma50 else -10
                ma_count += 1
            if ma200 is not None:
                ma_score += 5 if price > ma200 else -5
                ma_count += 1
            if ma_count > 0:
                mom_parts.append(50 + (ma_score / ma_count))

        pos = pos_52w
        if pos is None and price is not None and high_52w and low_52w and high_52w > low_52w:
            pos = ((price - low_52w) / (high_52w - low_52w)) * 100.0
        if pos is not None:
            pos_score = score_linear(pos, [(0, 20), (20, 35), (40, 50), (60, 65), (80, 80), (90, 90), (100, 95)])
            mom_parts.append(pos_score)
            normalized["pos_52w_normalized"] = pos_score / 100.0
            reasons.append(f"52W Pos: {pos:.1f}%")

        if rsi is not None:
            # mid-zone keep near RSI; extremes penalize
            rsi_score = 70 - (rsi - 70) * 2 if rsi > 70 else 30 + (30 - rsi) * 2 if rsi < 30 else rsi
            mom_parts.append(clamp(rsi_score))
            normalized["rsi_normalized"] = clamp(rsi_score) / 100.0

        if macd is not None and price is not None and price > 0:
            macd_score = 50 + (macd / price) * 100
            mom_parts.append(clamp(macd_score))
            normalized["macd_normalized"] = clamp(macd_score) / 100.0

        if volume is not None and avg_volume and avg_volume > 0:
            vr = volume / avg_volume
            if vr > 2.0:
                mom_parts.append(70)
                reasons.append("Very High Volume")
            elif vr > 1.5:
                mom_parts.append(60)
                reasons.append("High Volume")
            elif vr > 1.0:
                mom_parts.append(55)

        momentum_score = sum(mom_parts) / len(mom_parts) if mom_parts else 50.0
        momentum_score = clamp(momentum_score)
        components["momentum"] = momentum_score

        # -------- Risk score (higher = riskier) --------
        risk_parts: List[float] = []
        risk_base = 50.0

        if beta is not None:
            beta_score = score_linear(beta, [(0.2, 20), (0.5, 30), (0.8, 40), (1.0, 50), (1.3, 60), (1.6, 70), (2.0, 80), (2.5, 90)])
            risk_parts.append(beta_score)
            normalized["beta_normalized"] = beta_score / 100.0
            reasons.append(f"Beta: {beta:.2f}")

        if volatility is not None:
            vol_score = score_linear(volatility, [(10, 20), (15, 30), (20, 40), (25, 50), (30, 60), (40, 70), (50, 80), (60, 90)])
            risk_parts.append(vol_score)
            normalized["volatility_normalized"] = vol_score / 100.0
            reasons.append(f"Vol: {volatility:.1f}%")

        if max_drawdown is not None:
            risk_parts.append(score_linear(abs(max_drawdown), [(5, 20), (10, 30), (15, 40), (20, 50), (25, 60), (30, 70), (40, 80), (50, 90)]))

        if debt_to_equity is not None:
            if debt_to_equity > 2.0:
                risk_base += 20
            elif debt_to_equity > 1.0:
                risk_base += 10

        if liquidity_score is not None:
            if liquidity_score < 30:
                risk_base += 20
                warnings_list.append("Very Low Liquidity")
            elif liquidity_score < 50:
                risk_base += 10
                warnings_list.append("Low Liquidity")

        if free_float is not None:
            if free_float < 20:
                risk_base += 15
                warnings_list.append("Very Low Free Float")
            elif free_float < 30:
                risk_base += 7
                warnings_list.append("Low Free Float")

        if market_cap is not None:
            if market_cap < 100_000_000:
                risk_base += 25
            elif market_cap < 500_000_000:
                risk_base += 15
            elif market_cap < 2_000_000_000:
                risk_base += 7
            elif market_cap < 10_000_000_000:
                risk_base += 3

        if risk_parts:
            risk_score_val = (risk_base + sum(risk_parts)) / (len(risk_parts) + 1)
        else:
            risk_score_val = risk_base
        risk_score_val = clamp(risk_score_val)
        components["risk"] = risk_score_val

        # -------- News delta -> opportunity adjustment --------
        news_delta = 0.0
        if news_score is not None:
            if -1 <= news_score <= 1:
                news_delta = news_score * 20
            elif 0 <= news_score <= 100:
                news_delta = (news_score - 50) * 0.4
            news_delta = clamp(news_delta, -20, 20)
            if abs(news_delta) > 10:
                reasons.append(f"News: {'Very Positive' if news_delta > 0 else 'Very Negative'}")
            elif abs(news_delta) > 5:
                reasons.append(f"News: {'Positive' if news_delta > 0 else 'Negative'}")

        opportunity_score = clamp(
            0.40 * value_score + 0.30 * momentum_score + 0.20 * quality_score - (risk_score_val - 50) * 0.4 + news_delta
        )
        components["opportunity"] = opportunity_score

        overall_score = clamp(
            self.weights.value * value_score
            + self.weights.quality * quality_score
            + self.weights.momentum * momentum_score
            + self.weights.opportunity * opportunity_score
            - self.weights.risk * (risk_score_val - 50)
        )
        components["overall"] = overall_score

        # -------- Confidence --------
        metric_groups = [
            [pe, pb, dividend_yield],
            [roe, roa, net_margin],
            [beta, volatility],
            [price, volume, market_cap],
            [revenue_growth_yoy, eps_growth_yoy],
        ]
        completeness = sum((sum(1 for m in g if m is not None) / len(g)) * 100.0 for g in metric_groups) / len(metric_groups)

        quality_mult = {
            DataQuality.EXCELLENT: 1.0,
            DataQuality.HIGH: 0.95,
            DataQuality.MEDIUM: 0.85,
            DataQuality.LOW: 0.60,
            DataQuality.STALE: 0.40,
            DataQuality.ERROR: 0.20,
            DataQuality.NONE: 0.50,
        }.get(data_quality, 0.50)
        data_confidence = clamp(completeness * quality_mult)

        # -------- Recommendation + badges --------
        recommendation = Recommendation.from_score(overall_score)
        rec_badge = calculate_badge(overall_score, self.badge_thresholds)
        if recommendation == Recommendation.STRONG_BUY and risk_score_val <= 20:
            rec_badge = BadgeLevel.ELITE

        # strong news overrides (bounded)
        if news_delta > 15 and recommendation in (Recommendation.HOLD, Recommendation.REDUCE):
            recommendation = Recommendation.BUY
            rec_badge = BadgeLevel.GOOD
            reasons.append("Strong Positive News Override")
        elif news_delta < -15 and recommendation in (Recommendation.HOLD, Recommendation.BUY):
            recommendation = Recommendation.REDUCE
            rec_badge = BadgeLevel.CAUTION
            reasons.append("Strong Negative News Override")

        # -------- Forecast --------
        trend = self._determine_trend(price, ma20, ma50, ma200)
        forecast_data = self._generate_forecast(
            price=price,
            fair_value=fair_value,
            upside=upside_value,
            risk_score=risk_score_val,
            confidence=data_confidence,
            trend=trend,
            volatility=volatility,
            extractor=ex,
            normalized_features=normalized,
        )

        # -------- Final model --------
        utc_now = now_utc()
        riy_now = now_riyadh()

        scores = AssetScores(
            value_score=value_score,
            quality_score=quality_score,
            momentum_score=momentum_score,
            risk_score=risk_score_val,
            opportunity_score=opportunity_score,
            overall_score=overall_score,
            recommendation=recommendation,
            recommendation_confidence=data_confidence,
            recommendation_strength=overall_score - (risk_score_val * 0.3),
            rec_badge=rec_badge,
            value_badge=calculate_badge(value_score, self.badge_thresholds),
            quality_badge=calculate_badge(quality_score, self.badge_thresholds),
            momentum_badge=calculate_badge(momentum_score, self.badge_thresholds),
            risk_badge=calculate_badge(risk_score_val, self.risk_badge_thresholds),
            opportunity_badge=calculate_badge(opportunity_score, self.badge_thresholds),
            data_confidence=data_confidence,
            data_quality=data_quality,
            data_completeness=completeness,
            data_freshness=data_freshness,
            scoring_updated_utc=utc_now,
            scoring_updated_riyadh=riy_now,
            scoring_reason=reasons,
            scoring_warnings=warnings_list,
            scoring_version=SCORING_ENGINE_VERSION,
            scoring_model_version=self.model_version,
            component_scores=components,
            normalized_metrics=normalized,
            input_metrics={
                "pe": pe,
                "pb": pb,
                "roe": roe,
                "roa": roa,
                "beta": beta,
                "volatility": volatility,
                "price": price,
                "volume": volume,
                "market_cap": market_cap,
            },
            **forecast_data,
        )

        # compatibility aliases
        scores.expected_return_1m = scores.expected_roi_1m
        scores.expected_return_3m = scores.expected_roi_3m
        scores.expected_return_6m = scores.expected_roi_6m
        scores.expected_return_12m = scores.expected_roi_12m
        scores.expected_price_1m = scores.forecast_price_1m
        scores.expected_price_3m = scores.forecast_price_3m
        scores.expected_price_6m = scores.forecast_price_6m
        scores.expected_price_12m = scores.forecast_price_12m
        scores.confidence_score = scores.forecast_confidence

        self.performance_metrics["scoring_duration"].append(time.time() - t0)
        if len(self.performance_metrics["scoring_duration"]) > 1000:
            self.performance_metrics["scoring_duration"] = self.performance_metrics["scoring_duration"][-1000:]

        return scores

    def _prepare_feature_vector(self, normalized: Dict[str, float]) -> Dict[str, float]:
        keys = [
            "pe_normalized",
            "pb_normalized",
            "dy_normalized",
            "roe_normalized",
            "roa_normalized",
            "momentum_normalized",
            "pos_52w_normalized",
            "rsi_normalized",
            "beta_normalized",
            "volatility_normalized",
        ]
        return {k: float(normalized.get(k, 0.5)) for k in keys}

    def _generate_forecast(
        self,
        *,
        price: Optional[float],
        fair_value: Optional[float],
        upside: Optional[float],
        risk_score: float,
        confidence: float,
        trend: TrendDirection,
        volatility: Optional[float],
        extractor: MetricExtractor,
        normalized_features: Optional[Dict[str, float]] = None,
    ) -> Dict[str, Any]:
        if price is None or price <= 0:
            return {}

        # base return 12m
        if fair_value is not None and fair_value > 0:
            base_return_12m = ((fair_value / price) - 1.0) * 100.0
        elif upside is not None:
            base_return_12m = float(upside)
        else:
            base_return_12m = 5.0

        forecast_conf = extractor.get_float("forecast_confidence", "confidence_score", "confidence") or confidence
        forecast_conf = clamp(float(forecast_conf), 0, 100)

        forecast_method = extractor.get_str("forecast_method", "forecast_model", default="weighted_v3")
        forecast_updated = extractor.get_datetime("forecast_updated_utc", "forecast_last_utc", "last_updated_utc") or now_utc()

        risk_multiplier = clamp(1.0 - (risk_score / 150.0), 0.33, 1.0)
        trend_multiplier = (
            1.20 if trend == TrendDirection.STRONG_UPTREND else
            1.10 if trend == TrendDirection.UPTREND else
            0.90 if trend == TrendDirection.DOWNTREND else
            0.80 if trend == TrendDirection.STRONG_DOWNTREND else
            1.00
        )
        vol_multiplier = clamp(1.0 - ((volatility or 0.0) / 150.0), 0.5, 1.0) if volatility is not None else 1.0
        conf_multiplier = forecast_conf / 100.0

        base_adjusted = base_return_12m * risk_multiplier * trend_multiplier * vol_multiplier

        # NOTE: clamp ROI ranges with their correct negative bounds (do NOT use 0..100 clamp)
        r1 = clamp(base_adjusted * math.sqrt(30 / 365) * conf_multiplier, self.forecast_params.min_roi_1m, self.forecast_params.max_roi_1m)
        r3 = clamp(base_adjusted * math.sqrt(90 / 365) * conf_multiplier, self.forecast_params.min_roi_3m, self.forecast_params.max_roi_3m)
        r6 = clamp(base_adjusted * math.sqrt(180 / 365) * conf_multiplier, self.forecast_params.min_roi_6m, self.forecast_params.max_roi_6m)
        r12 = clamp(base_adjusted * conf_multiplier, self.forecast_params.min_roi_12m, self.forecast_params.max_roi_12m)

        return {
            "forecast_price_1m": price * (1.0 + r1 / 100.0),
            "forecast_price_3m": price * (1.0 + r3 / 100.0),
            "forecast_price_6m": price * (1.0 + r6 / 100.0),
            "forecast_price_12m": price * (1.0 + r12 / 100.0),
            "expected_roi_1m": r1,
            "expected_roi_3m": r3,
            "expected_roi_6m": r6,
            "expected_roi_12m": r12,
            "forecast_confidence": forecast_conf,
            "forecast_method": forecast_method,
            "forecast_model": self.ml_model.model_type.value if self.ml_model else None,
            "forecast_updated_utc": forecast_updated,
            "forecast_updated_riyadh": utc_to_riyadh(forecast_updated),
            "forecast_parameters": {
                "risk_multiplier": risk_multiplier,
                "trend_multiplier": trend_multiplier,
                "vol_multiplier": vol_multiplier,
                "confidence_multiplier": conf_multiplier,
                "base_return_12m": base_return_12m,
            },
        }

    def _determine_trend(self, price: Optional[float], ma20: Optional[float], ma50: Optional[float], ma200: Optional[float]) -> TrendDirection:
        if price is None:
            return TrendDirection.NEUTRAL

        strong_up, up, down, strong_down = 0, 0, 0, 0

        def _vote(ma: Optional[float], up_mult: float, down_mult: float, up_w: int, down_w: int):
            nonlocal strong_up, up, down, strong_down
            if ma is None or ma <= 0:
                return
            if price > ma * up_mult:
                strong_up += up_w
            elif price > ma:
                up += up_w
            elif price < ma * down_mult:
                strong_down += down_w
            elif price < ma:
                down += down_w

        _vote(ma20, 1.10, 0.90, 1, 1)
        _vote(ma50, 1.15, 0.85, 1, 1)
        _vote(ma200, 1.20, 0.80, 1, 1)

        if strong_up >= 2:
            return TrendDirection.STRONG_UPTREND
        if up >= 2:
            return TrendDirection.UPTREND
        if strong_down >= 2:
            return TrendDirection.STRONG_DOWNTREND
        if down >= 2:
            return TrendDirection.DOWNTREND
        return TrendDirection.SIDEWAYS

    def _parse_data_quality(self, quality_str: str) -> DataQuality:
        q = (quality_str or "").strip().upper()
        if q in ("EXCELLENT", "PERFECT"):
            return DataQuality.EXCELLENT
        if q in ("HIGH", "GOOD"):
            return DataQuality.HIGH
        if q in ("MEDIUM", "MED", "AVERAGE", "OK"):
            return DataQuality.MEDIUM
        if q in ("LOW", "POOR", "BAD"):
            return DataQuality.LOW
        if q in ("STALE", "OLD", "EXPIRED"):
            return DataQuality.STALE
        if q in ("NONE", ""):
            return DataQuality.NONE
        if q in ("ERROR", "MISSING"):
            return DataQuality.ERROR
        return DataQuality.MEDIUM

    async def stream_scores(self, symbol: str, websocket: Any) -> None:
        await self.ws_manager.connect(websocket)
        await self.ws_manager.subscribe(websocket, symbol)
        try:
            while True:
                await asyncio.sleep(1)
        finally:
            await self.ws_manager.disconnect(websocket)


# =============================================================================
# A/B Testing + Weight Optimizer (optional)
# =============================================================================
class ABTest:
    def __init__(self, name: str, variants: Dict[str, ScoringEngine]):
        self.name = name
        self.variants = variants
        self.results: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
        self.start_time = now_utc()

    async def run_test(self, test_data: List[Any], iterations: int = 100) -> Dict[str, Any]:
        for variant_name, engine in self.variants.items():
            start = time.time()
            rows: List[Dict[str, Any]] = []
            for data in test_data[:iterations]:
                try:
                    r = engine.compute_scores(data)
                    rows.append(
                        {
                            "overall_score": r.overall_score,
                            "recommendation": r.recommendation.value,
                            "confidence": r.data_confidence,
                            "duration": time.time() - start,
                        }
                    )
                except Exception as e:
                    rows.append({"error": str(e)})
            self.results[variant_name] = rows
        return self.analyze_results()

    def analyze_results(self) -> Dict[str, Any]:
        analysis: Dict[str, Any] = {}
        for variant_name, rows in self.results.items():
            valid = [x for x in rows if "error" not in x]
            if not valid:
                analysis[variant_name] = {"error": "No valid scores"}
                continue
            overall_scores = [x["overall_score"] for x in valid]
            confidences = [x["confidence"] for x in valid]
            durations = [x["duration"] for x in valid]
            analysis[variant_name] = {
                "mean_score": statistics.mean(overall_scores),
                "std_score": statistics.stdev(overall_scores) if len(overall_scores) > 1 else 0.0,
                "mean_confidence": statistics.mean(confidences),
                "mean_duration_ms": statistics.mean(durations) * 1000.0,
                "sample_size": len(valid),
                "error_rate": (len(rows) - len(valid)) / max(1, len(rows)) * 100.0,
            }

        winner = None
        winner_score = -1.0
        for k, v in analysis.items():
            if "mean_score" not in v:
                continue
            score = (v["mean_score"] * v["mean_confidence"]) / 100.0
            if score > winner_score:
                winner_score = score
                winner = k
        analysis["winner"] = winner
        analysis["test_duration_seconds"] = (now_utc() - self.start_time).total_seconds()
        return analysis


class WeightOptimizer:
    def __init__(self, engine: ScoringEngine):
        self.engine = engine
        self.historical_data: List[Tuple[Any, float]] = []

    def add_training_point(self, data: Any, actual_return: float) -> None:
        self.historical_data.append((data, actual_return))

    def optimize(self, n_trials: int = 100) -> ScoringWeights:
        if not _OPTUNA_AVAILABLE or not _NUMPY_AVAILABLE or len(self.historical_data) < 10:
            return self.engine.weights

        def objective(trial):
            w = ScoringWeights(
                value=trial.suggest_float("value", 0.1, 0.4),
                quality=trial.suggest_float("quality", 0.1, 0.4),
                momentum=trial.suggest_float("momentum", 0.1, 0.4),
                risk=trial.suggest_float("risk", 0.05, 0.25),
                opportunity=trial.suggest_float("opportunity", 0.05, 0.25),
            ).validate()

            temp = ScoringEngine(weights=w)
            errors: List[float] = []
            for data, actual in self.historical_data:
                s = temp.compute_scores(data)
                pred = (s.expected_roi_12m or 0.0)
                errors.append((pred - actual) ** 2)
            return float(np.mean(np.array(errors)))

        study = optuna.create_study(direction="minimize")
        study.optimize(objective, n_trials=n_trials)
        return ScoringWeights(**study.best_params).validate()


# =============================================================================
# Public API (stable)
# =============================================================================
_ENGINE = ScoringEngine()


def compute_scores(source: Any) -> AssetScores:
    try:
        return _ENGINE.compute_scores(source)
    except Exception as e:
        dt = now_utc()
        return AssetScores(
            scoring_errors=[str(e)],
            scoring_updated_utc=dt,
            scoring_updated_riyadh=utc_to_riyadh(dt),
        )


def enrich_with_scores(target: Any, *, prefer_existing_risk_score: bool = True, in_place: bool = False) -> Any:
    scores = compute_scores(target)

    update_data: Dict[str, Any] = {
        "value_score": scores.value_score,
        "quality_score": scores.quality_score,
        "momentum_score": scores.momentum_score,
        "opportunity_score": scores.opportunity_score,
        "overall_score": scores.overall_score,
        "recommendation": scores.recommendation.value,
        "recommendation_raw": scores.recommendation_raw,
        "recommendation_strength": scores.recommendation_strength,
        "rec_badge": scores.rec_badge.value if scores.rec_badge else None,
        "value_badge": scores.value_badge.value if scores.value_badge else None,
        "quality_badge": scores.quality_badge.value if scores.quality_badge else None,
        "momentum_badge": scores.momentum_badge.value if scores.momentum_badge else None,
        "risk_badge": scores.risk_badge.value if scores.risk_badge else None,
        "opportunity_badge": scores.opportunity_badge.value if scores.opportunity_badge else None,
        "data_confidence": scores.data_confidence,
        "data_quality": scores.data_quality.value if scores.data_quality else None,
        "data_completeness": scores.data_completeness,
        "data_freshness": scores.data_freshness,
        "forecast_price_1m": scores.forecast_price_1m,
        "forecast_price_3m": scores.forecast_price_3m,
        "forecast_price_6m": scores.forecast_price_6m,
        "forecast_price_12m": scores.forecast_price_12m,
        "expected_roi_1m": scores.expected_roi_1m,
        "expected_roi_3m": scores.expected_roi_3m,
        "expected_roi_6m": scores.expected_roi_6m,
        "expected_roi_12m": scores.expected_roi_12m,
        "forecast_confidence": scores.forecast_confidence,
        "forecast_method": scores.forecast_method,
        "forecast_model": scores.forecast_model,
        "forecast_parameters": scores.forecast_parameters,
        "forecast_ensemble": scores.forecast_ensemble,
        "forecast_updated_utc": scores.forecast_updated_utc.isoformat() if scores.forecast_updated_utc else None,
        "forecast_updated_riyadh": scores.forecast_updated_riyadh.isoformat() if scores.forecast_updated_riyadh else None,
        "ml_explanations": scores.ml_explanations,
        "feature_importance": scores.feature_importance,
        "shap_values": scores.shap_values,
        "scoring_reason": ", ".join(scores.scoring_reason) if scores.scoring_reason else "",
        "scoring_warnings": scores.scoring_warnings,
        "scoring_errors": scores.scoring_errors,
        "scoring_version": scores.scoring_version,
        "scoring_model_version": scores.scoring_model_version,
        "scoring_updated_utc": scores.scoring_updated_utc.isoformat() if scores.scoring_updated_utc else None,
        "scoring_updated_riyadh": scores.scoring_updated_riyadh.isoformat() if scores.scoring_updated_riyadh else None,
        # compatibility aliases
        "expected_return_1m": scores.expected_roi_1m,
        "expected_return_3m": scores.expected_roi_3m,
        "expected_return_6m": scores.expected_roi_6m,
        "expected_return_12m": scores.expected_roi_12m,
        "expected_price_1m": scores.forecast_price_1m,
        "expected_price_3m": scores.forecast_price_3m,
        "expected_price_6m": scores.forecast_price_6m,
        "expected_price_12m": scores.forecast_price_12m,
        "confidence_score": scores.forecast_confidence,
    }

    # risk score preference
    if not (prefer_existing_risk_score and hasattr(target, "risk_score") and getattr(target, "risk_score") is not None):
        update_data["risk_score"] = scores.risk_score

    if in_place:
        if isinstance(target, dict):
            target.update(update_data)
            return target
        for k, v in update_data.items():
            try:
                if hasattr(target, k):
                    setattr(target, k, v)
            except Exception:
                pass
        return target

    if isinstance(target, dict):
        out = dict(target)
        out.update(update_data)
        return out

    # pydantic model copy support
    if hasattr(target, "model_copy"):
        try:
            return target.model_copy(update=update_data)
        except Exception:
            pass
    if hasattr(target, "copy"):
        try:
            return target.copy(update=update_data)
        except Exception:
            pass

    return update_data


def batch_compute(sources: List[Any]) -> List[AssetScores]:
    return [compute_scores(s) for s in sources]


def get_engine() -> ScoringEngine:
    return _ENGINE


def set_weights(weights: ScoringWeights) -> None:
    global _ENGINE
    _ENGINE = ScoringEngine(weights=weights)


def set_ml_model(model: MLModel) -> None:
    global _ENGINE
    _ENGINE.ml_model = model


def create_ab_test(variants: Dict[str, Dict[str, Any]]) -> ABTest:
    engines: Dict[str, ScoringEngine] = {}
    for name, cfg in variants.items():
        w = ScoringWeights(**(cfg.get("weights") or {})).validate()
        fp = ForecastParameters(**(cfg.get("forecast_params") or {}))
        engines[name] = ScoringEngine(weights=w, forecast_params=fp)
    return ABTest("scoring_ab_test", engines)


def optimize_weights(historical_data: List[Tuple[Any, float]], n_trials: int = 100) -> ScoringWeights:
    opt = WeightOptimizer(get_engine())
    for data, ret in historical_data:
        opt.add_training_point(data, ret)
    return opt.optimize(n_trials)


__all__ = [
    "SCORING_ENGINE_VERSION",
    "Recommendation",
    "BadgeLevel",
    "TrendDirection",
    "DataQuality",
    "ModelType",
    "ScoringWeights",
    "ForecastParameters",
    "AssetScores",
    "ScoringEngine",
    "MLModel",
    "ABTest",
    "WeightOptimizer",
    "compute_scores",
    "enrich_with_scores",
    "batch_compute",
    "get_engine",
    "set_weights",
    "set_ml_model",
    "create_ab_test",
    "optimize_weights",
    "safe_float",
    "safe_percent",
    "safe_datetime",
    "now_utc",
    "now_riyadh",
]
