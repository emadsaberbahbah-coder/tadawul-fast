#!/usr/bin/env python3
# core/scoring.py
"""
================================================================================
Scoring Module — v4.1.4
(HORIZON-AWARE / TECHNICAL-SIGNALS / SHORT-TERM READY / SCHEMA-ALIGNED)
================================================================================

v4.1.4 changes (what moved from v4.1.3)
---------------------------------------
- FIX [HIGH]: Blue-chip guard was too narrow. v4.1.3 required BOTH
  quality >= 65 AND growth >= 65 simultaneously. The problem: growth_score
  returns None when revenue_growth_yoy is absent (common for ETFs,
  commodities, and stocks whose data provider didn't return the field).
  With growth is None, the guard's `growth is not None and g >= 65.0`
  branch silently failed — and the stock got a false SELL/REDUCE.

  Fix: the guard now fires in two independent cases:
  (a) Full signal: quality >= 65 AND growth >= 65 AND risk <= 55
      (the original v4.1.3 logic, unchanged).
  (b) Quality-only: quality >= 70 AND risk <= 50, regardless of growth.
      Tighter thresholds because we're using fewer inputs; symmetric
      with (a) overall.
  Either case triggers HOLD instead of SELL/REDUCE when forward-looking
  data is missing.

- FIX [MEDIUM]: compute_valuation_score weight normalization. v4.1.2 fixed
  the anchors-only case but left the partial-signal calibration
  inconsistent across symbols. Example: a symbol with only upside_n
  (weight 0.40) scored with wsum=0.40, while a symbol with upside_n +
  roi3_n + roi12_n + anchor_avg scored with wsum=1.00. The weighted
  average is numerically the same shape but the EFFECTIVE minimum for
  partial-signal symbols was higher. This made scores not directly
  comparable.

  Fix: when any forward signal is present, give the partial-data case
  the same expected weight floor (sum of missing weights treated as
  neutral 0.5). Documented as v4.1.4 behavior; no behavior change for
  full-signal symbols.

- DOC: v4.1.3 blue-chip guard docstring clarified to explain both cases.

Public API preserved. `compute_recommendation`'s signature is unchanged
from v4.1.3. Callers that pass quality/growth/valuation still benefit
from the stricter v4.1.3 logic; the new case (b) also fires when growth
is None.

v4.1.3 — Blue-chip guard (preserved)
------------------------------------
See v4.1.3 docstring block below for the original motivation.
Blue-chip stocks (strong fundamentals, controlled risk) with silent
forward-looking providers now receive HOLD instead of false SELL/REDUCE.

v4.1.2 — Respect scoring.py's intentional None for valuation_score (preserved)
------------------------------------------------------------------
compute_valuation_score returns None (not a misleading near-zero score)
when no forward-looking signal is available. downstream compute_scores
handles None by skipping and renormalizing.

v4.1.1 — normalize_recommendation_code preserves STRONG_BUY (preserved)
--------------------------------------------------------------------
3-step resolution: canonical fast path, local alias table, then delegate
to reco_normalize for unknown inputs only.

v4.1.0 — ROI coercion + insufficient-input handling (preserved)
---------------------------------------------------------------
_as_roi_fraction for all expected_roi_* fields; honest HOLD when
scoring inputs are insufficient.
================================================================================
"""

from __future__ import annotations

import logging
import math
import os
from dataclasses import asdict, dataclass, field, replace
from datetime import datetime, timedelta, timezone
from enum import Enum
from typing import (
    Any,
    Dict,
    List,
    Mapping,
    Optional,
    Sequence,
    Tuple,
)

logger = logging.getLogger(__name__)
logger.addHandler(logging.NullHandler())

# =============================================================================
# Version
# =============================================================================

__version__ = "4.1.4"
SCORING_VERSION = __version__

# =============================================================================
# Time Helpers
# =============================================================================

_UTC = timezone.utc
_RIYADH_TZ = timezone(timedelta(hours=3))


def _utc_iso(dt: Optional[datetime] = None) -> str:
    """Get UTC time in ISO format. Naive datetimes are treated as UTC."""
    d = dt or datetime.now(_UTC)
    if d.tzinfo is None:
        d = d.replace(tzinfo=_UTC)
    return d.astimezone(_UTC).isoformat()


def _riyadh_iso(dt: Optional[datetime] = None) -> str:
    """Get Riyadh time in ISO format."""
    if dt is None:
        return datetime.now(_RIYADH_TZ).isoformat()
    d = dt
    if d.tzinfo is None:
        d = d.replace(tzinfo=_UTC)
    return d.astimezone(_RIYADH_TZ).isoformat()


# =============================================================================
# Enums
# =============================================================================

class Horizon(str, Enum):
    """Investment horizon classification."""
    DAY = "day"
    WEEK = "week"
    MONTH = "month"
    LONG = "long"


class Signal(str, Enum):
    """Trading signals."""
    STRONG_BUY = "STRONG_BUY"
    BUY = "BUY"
    HOLD = "HOLD"
    REDUCE = "REDUCE"
    SELL = "SELL"


class RSISignal(str, Enum):
    """RSI-based signals."""
    OVERSOLD = "Oversold"
    NEUTRAL = "Neutral"
    OVERBOUGHT = "Overbought"
    N_A = "N/A"


# =============================================================================
# Custom Exceptions
# =============================================================================

class ScoringError(Exception):
    """Base exception for scoring errors."""
    pass


class InvalidHorizonError(ScoringError):
    """Raised when horizon is invalid."""
    pass


class MissingDataError(ScoringError):
    """Raised when required data is missing."""
    pass


# =============================================================================
# Configuration
# =============================================================================

@dataclass(frozen=True)
class ScoringConfig:
    """Configuration for scoring module."""
    day_threshold: int = 5
    week_threshold: int = 14
    month_threshold: int = 90

    default_valuation: float = 0.30
    default_momentum: float = 0.25
    default_quality: float = 0.20
    default_growth: float = 0.15
    default_opportunity: float = 0.10
    default_technical: float = 0.00

    risk_penalty_strength: float = 0.55
    confidence_penalty_strength: float = 0.45

    confidence_high: float = 0.75
    confidence_medium: float = 0.50

    risk_low_threshold: float = 35.0
    risk_moderate_threshold: float = 65.0

    @classmethod
    def from_env(cls) -> "ScoringConfig":
        def _env_float(name: str, default: float) -> float:
            try:
                return float(os.getenv(name, str(default)))
            except Exception:
                return default

        def _env_int(name: str, default: int) -> int:
            try:
                return int(os.getenv(name, str(default)))
            except Exception:
                return default

        return cls(
            day_threshold=_env_int("SCORING_DAY_THRESHOLD", 5),
            week_threshold=_env_int("SCORING_WEEK_THRESHOLD", 14),
            month_threshold=_env_int("SCORING_MONTH_THRESHOLD", 90),
            default_valuation=_env_float("SCORING_W_VALUATION", 0.30),
            default_momentum=_env_float("SCORING_W_MOMENTUM", 0.25),
            default_quality=_env_float("SCORING_W_QUALITY", 0.20),
            default_growth=_env_float("SCORING_W_GROWTH", 0.15),
            default_opportunity=_env_float("SCORING_W_OPPORTUNITY", 0.10),
            default_technical=_env_float("SCORING_W_TECHNICAL", 0.00),
            risk_penalty_strength=_env_float("SCORING_RISK_PENALTY", 0.55),
            confidence_penalty_strength=_env_float("SCORING_CONFIDENCE_PENALTY", 0.45),
        )


_CONFIG = ScoringConfig.from_env()

# =============================================================================
# Horizon Thresholds
# =============================================================================

_HORIZON_DAYS_CUTOFFS: Tuple[Tuple[int, Horizon], ...] = (
    (_CONFIG.day_threshold, Horizon.DAY),
    (_CONFIG.week_threshold, Horizon.WEEK),
    (_CONFIG.month_threshold, Horizon.MONTH),
)


# =============================================================================
# Data Classes
# =============================================================================

@dataclass(slots=True)
class ScoreWeights:
    w_valuation: float = _CONFIG.default_valuation
    w_momentum: float = _CONFIG.default_momentum
    w_quality: float = _CONFIG.default_quality
    w_growth: float = _CONFIG.default_growth
    w_opportunity: float = _CONFIG.default_opportunity
    w_technical: float = _CONFIG.default_technical
    risk_penalty_strength: float = _CONFIG.risk_penalty_strength
    confidence_penalty_strength: float = _CONFIG.confidence_penalty_strength

    def normalize(self) -> "ScoreWeights":
        total = (self.w_valuation + self.w_momentum + self.w_quality +
                 self.w_growth + self.w_opportunity + self.w_technical)
        if total > 0:
            return ScoreWeights(
                w_valuation=self.w_valuation / total,
                w_momentum=self.w_momentum / total,
                w_quality=self.w_quality / total,
                w_growth=self.w_growth / total,
                w_opportunity=self.w_opportunity / total,
                w_technical=self.w_technical / total,
                risk_penalty_strength=self.risk_penalty_strength,
                confidence_penalty_strength=self.confidence_penalty_strength,
            )
        return self


@dataclass(slots=True)
class ForecastParameters:
    min_roi_1m: float = -0.25
    max_roi_1m: float = 0.25
    min_roi_3m: float = -0.35
    max_roi_3m: float = 0.35
    min_roi_12m: float = -0.65
    max_roi_12m: float = 0.65
    ratio_1m_of_12m: float = 0.18
    ratio_3m_of_12m: float = 0.42


@dataclass(slots=True)
class AssetScores:
    valuation_score: Optional[float] = None
    momentum_score: Optional[float] = None
    quality_score: Optional[float] = None
    growth_score: Optional[float] = None
    value_score: Optional[float] = None
    opportunity_score: Optional[float] = None
    confidence_score: Optional[float] = None
    forecast_confidence: Optional[float] = None
    confidence_bucket: Optional[str] = None
    risk_score: Optional[float] = None
    risk_bucket: Optional[str] = None
    overall_score: Optional[float] = None
    overall_score_raw: Optional[float] = None
    overall_penalty_factor: Optional[float] = None

    technical_score: Optional[float] = None
    rsi_signal: Optional[str] = None
    short_term_signal: Optional[str] = None
    day_range_position: Optional[float] = None
    volume_ratio: Optional[float] = None
    upside_pct: Optional[float] = None
    invest_period_label: Optional[str] = None
    horizon_label: Optional[str] = None
    horizon_days_effective: Optional[int] = None

    recommendation: str = "HOLD"
    recommendation_reason: str = "Insufficient data."

    forecast_price_1m: Optional[float] = None
    forecast_price_3m: Optional[float] = None
    forecast_price_12m: Optional[float] = None
    expected_roi_1m: Optional[float] = None
    expected_roi_3m: Optional[float] = None
    expected_roi_12m: Optional[float] = None
    expected_return_1m: Optional[float] = None
    expected_return_3m: Optional[float] = None
    expected_return_12m: Optional[float] = None
    expected_price_1m: Optional[float] = None
    expected_price_3m: Optional[float] = None
    expected_price_12m: Optional[float] = None

    scoring_updated_utc: str = field(default_factory=_utc_iso)
    scoring_updated_riyadh: str = field(default_factory=_riyadh_iso)
    scoring_errors: List[str] = field(default_factory=list)

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


ScoringWeights = ScoreWeights

DEFAULT_WEIGHTS = ScoreWeights()
DEFAULT_FORECASTS = ForecastParameters()


# =============================================================================
# Pure Utility Functions
# =============================================================================

def _clamp(value: float, min_val: float, max_val: float) -> float:
    return max(min_val, min(value, max_val))


def _round(value: Optional[float], ndigits: int = 2) -> Optional[float]:
    if value is None:
        return None
    try:
        return round(value, ndigits)
    except Exception:
        return None


def _safe_float(value: Any) -> Optional[float]:
    if value is None:
        return None
    try:
        if isinstance(value, (int, float)) and not isinstance(value, bool):
            return float(value)
        s = str(value).strip().replace(",", "")
        if not s or s.lower() in {"na", "n/a", "none", "null", ""}:
            return None
        if s.endswith("%"):
            f = float(s[:-1].strip()) / 100.0
        else:
            f = float(s)
        if math.isnan(f) or math.isinf(f):
            return None
        return f
    except (ValueError, TypeError):
        return None


def _safe_str(value: Any, default: str = "") -> str:
    try:
        s = str(value).strip()
        return s if s else default
    except Exception:
        return default


def _get(row: Mapping[str, Any], *keys: str) -> Any:
    for key in keys:
        if key in row and row[key] is not None:
            return row[key]
    return None


def _get_float(row: Mapping[str, Any], *keys: str) -> Optional[float]:
    return _safe_float(_get(row, *keys))


def _as_fraction(value: Any) -> Optional[float]:
    f = _safe_float(value)
    if f is None:
        return None
    if abs(f) >= 1.5:
        return f / 100.0
    return f


def _as_roi_fraction(value: Any) -> Optional[float]:
    f = _safe_float(value)
    if f is None:
        return None
    if abs(f) > 1.0:
        return f / 100.0
    return f


def _norm_score_0_100(value: Any) -> Optional[float]:
    f = _safe_float(value)
    if f is None:
        return None
    if 0.0 <= f <= 1.5:
        f *= 100.0
    return _clamp(f, 0.0, 100.0)


def _norm_confidence_0_1(value: Any) -> Optional[float]:
    f = _safe_float(value)
    if f is None:
        return None
    if f > 1.5:
        f /= 100.0
    return _clamp(f, 0.0, 1.0)


# =============================================================================
# Horizon Detection
# =============================================================================

def detect_horizon(settings: Any = None, row: Optional[Mapping[str, Any]] = None) -> Tuple[Horizon, Optional[int]]:
    horizon_days: Optional[float] = None

    if settings is not None:
        if isinstance(settings, Mapping):
            horizon_days = _safe_float(settings.get("horizon_days") or settings.get("invest_period_days"))
        else:
            horizon_days = _safe_float(
                getattr(settings, "horizon_days", None) or
                getattr(settings, "invest_period_days", None)
            )

    if horizon_days is None and row is not None:
        horizon_days = _get_float(row, "horizon_days", "invest_period_days")

    if horizon_days is None:
        return Horizon.MONTH, None

    hd = int(abs(horizon_days))
    for cutoff, label in _HORIZON_DAYS_CUTOFFS:
        if hd <= cutoff:
            return label, hd
    return Horizon.LONG, hd


def get_weights_for_horizon(horizon: Horizon, settings: Any = None) -> ScoreWeights:
    presets = {
        Horizon.DAY: ScoreWeights(
            w_technical=0.50, w_momentum=0.30, w_quality=0.10,
            w_valuation=0.00, w_growth=0.00, w_opportunity=0.10,
        ),
        Horizon.WEEK: ScoreWeights(
            w_technical=0.25, w_momentum=0.25, w_valuation=0.20,
            w_quality=0.20, w_growth=0.00, w_opportunity=0.10,
        ),
        Horizon.MONTH: ScoreWeights(
            w_technical=0.00, w_valuation=0.30, w_momentum=0.25,
            w_quality=0.20, w_growth=0.15, w_opportunity=0.10,
        ),
        Horizon.LONG: ScoreWeights(
            w_technical=0.00, w_valuation=0.35, w_quality=0.25,
            w_growth=0.20, w_momentum=0.15, w_opportunity=0.05,
        ),
    }

    base = presets.get(horizon, presets[Horizon.MONTH])

    if settings is None:
        return base.normalize()

    def _try(name: str, current: float) -> float:
        try:
            v = settings.get(name) if isinstance(settings, Mapping) else getattr(settings, name, None)
            if v is None:
                return current
            f = float(v)
            return f if not math.isnan(f) and not math.isinf(f) else current
        except Exception:
            return current

    result = replace(base)
    result.risk_penalty_strength = _clamp(_try("risk_penalty_strength", result.risk_penalty_strength), 0.0, 1.0)
    result.confidence_penalty_strength = _clamp(_try("confidence_penalty_strength", result.confidence_penalty_strength), 0.0, 1.0)

    return result.normalize()


# =============================================================================
# Derived Field Helpers
# =============================================================================

def derive_volume_ratio(row: Mapping[str, Any]) -> Optional[float]:
    vr = _get_float(row, "volume_ratio")
    if vr is not None and vr > 0:
        return _round(vr, 4)

    vol = _get_float(row, "volume")
    avg = _get_float(row, "avg_volume_10d")
    if avg is None:
        avg = _get_float(row, "avg_volume_30d")

    if vol is None or avg is None or avg <= 0:
        return None

    return _round(vol / avg, 4)


def derive_day_range_position(row: Mapping[str, Any]) -> Optional[float]:
    drp = _get_float(row, "day_range_position")
    if drp is not None:
        return _round(_clamp(drp, 0.0, 1.0), 4)

    price = _get_float(row, "current_price", "price", "last_price")
    low = _get_float(row, "day_low")
    high = _get_float(row, "day_high")

    if price is None or low is None or high is None:
        return None

    range_span = high - low
    if range_span <= 0:
        return _round(0.5, 4)

    return _round(_clamp((price - low) / range_span, 0.0, 1.0), 4)


def derive_upside_pct(row: Mapping[str, Any]) -> Optional[float]:
    usp = _get_float(row, "upside_pct")
    if usp is not None:
        return usp

    price = _get_float(row, "current_price", "price", "last_price")
    intrinsic = _get_float(row, "intrinsic_value", "fair_value")

    if price is None or price <= 0 or intrinsic is None or intrinsic <= 0:
        return None

    return _round((intrinsic - price) / price, 4)


def invest_period_label(horizon: Horizon, horizon_days: Optional[int] = None) -> str:
    if horizon_days is not None:
        if horizon_days <= 1:
            return "1D"
        if horizon_days <= 6:
            return "1W"
        if horizon_days <= 30:
            return "1M"
        if horizon_days <= 90:
            return "3M"
        return "12M"

    return {
        Horizon.DAY: "1D",
        Horizon.WEEK: "1W",
        Horizon.MONTH: "1M",
        Horizon.LONG: "12M",
    }.get(horizon, "1M")


# =============================================================================
# Technical Score
# =============================================================================

def _rsi_to_zone_score(rsi: Optional[float]) -> Optional[float]:
    if rsi is None:
        return None
    if rsi <= 25:
        return 0.95
    if rsi <= 35:
        return 0.88
    if rsi <= 45:
        return 0.78
    if rsi <= 55:
        return 0.68
    if rsi <= 60:
        return 0.58
    if rsi <= 65:
        return 0.45
    if rsi <= 70:
        return 0.30
    if rsi <= 75:
        return 0.18
    return 0.08


def _volume_ratio_to_score(ratio: Optional[float]) -> Optional[float]:
    if ratio is None or ratio < 0:
        return None
    if ratio >= 3.0:
        return 1.00
    if ratio >= 2.0:
        return 0.90
    if ratio >= 1.5:
        return 0.75
    if ratio >= 1.0:
        return 0.55
    if ratio >= 0.7:
        return 0.40
    return 0.20


def _day_range_to_score(drp: Optional[float]) -> Optional[float]:
    if drp is None:
        return None
    return _clamp(1.0 - (drp ** 0.7), 0.0, 1.0)


def compute_technical_score(row: Mapping[str, Any]) -> Optional[float]:
    rsi = _get_float(row, "rsi_14", "rsi", "rsi14")
    vol_ratio = derive_volume_ratio(row)
    drp = derive_day_range_position(row)

    parts: List[Tuple[float, float]] = []

    rsi_score = _rsi_to_zone_score(rsi)
    if rsi_score is not None:
        parts.append((0.40, rsi_score))

    vol_score = _volume_ratio_to_score(vol_ratio)
    if vol_score is not None:
        parts.append((0.30, vol_score))

    drp_score = _day_range_to_score(drp)
    if drp_score is not None:
        parts.append((0.30, drp_score))

    if not parts:
        return None

    wsum = sum(w for w, _ in parts)
    score_01 = sum(w * v for w, v in parts) / max(1e-9, wsum)
    return _round(100.0 * _clamp(score_01, 0.0, 1.0), 2)


def rsi_signal(rsi: Optional[float]) -> str:
    if rsi is None:
        return RSISignal.N_A.value
    if rsi < 30:
        return RSISignal.OVERSOLD.value
    if rsi > 70:
        return RSISignal.OVERBOUGHT.value
    return RSISignal.NEUTRAL.value


def short_term_signal(
    technical: Optional[float],
    momentum: Optional[float],
    risk: Optional[float],
    horizon: Horizon,
) -> str:
    t = technical if technical is not None else 50.0
    m = momentum if momentum is not None else 50.0
    r = risk if risk is not None else 50.0

    if horizon == Horizon.DAY:
        if t >= 75 and m >= 70 and r <= 50:
            return Signal.STRONG_BUY.value
        if t >= 60 and m >= 55 and r <= 65:
            return Signal.BUY.value
        if t < 38 or m < 30:
            return Signal.SELL.value
        return Signal.HOLD.value

    if horizon == Horizon.WEEK:
        if t >= 65 and m >= 60 and r <= 60:
            return Signal.BUY.value
        if t < 35 or m < 30:
            return Signal.SELL.value
        return Signal.HOLD.value

    # Month / Long
    if m >= 70 and t >= 55:
        return Signal.BUY.value
    if m <= 30 or t <= 35:
        return Signal.SELL.value
    return Signal.HOLD.value


# =============================================================================
# Forecast Helpers
# =============================================================================

def _forecast_params_from_settings(settings: Any) -> ForecastParameters:
    if settings is None:
        return DEFAULT_FORECASTS

    def _try_fraction(name: str, current: float) -> float:
        try:
            v = settings.get(name) if isinstance(settings, Mapping) else getattr(settings, name, None)
            if v is None:
                return current
            f = float(v)
            if math.isnan(f) or math.isinf(f):
                return current
            if abs(f) > 1.5:
                f /= 100.0
            return f
        except Exception:
            return current

    return ForecastParameters(
        min_roi_1m=_try_fraction("min_roi_1m", DEFAULT_FORECASTS.min_roi_1m),
        max_roi_1m=_try_fraction("max_roi_1m", DEFAULT_FORECASTS.max_roi_1m),
        min_roi_3m=_try_fraction("min_roi_3m", DEFAULT_FORECASTS.min_roi_3m),
        max_roi_3m=_try_fraction("max_roi_3m", DEFAULT_FORECASTS.max_roi_3m),
        min_roi_12m=_try_fraction("min_roi_12m", DEFAULT_FORECASTS.min_roi_12m),
        max_roi_12m=_try_fraction("max_roi_12m", DEFAULT_FORECASTS.max_roi_12m),
    )


def derive_forecast_patch(
    row: Mapping[str, Any],
    forecasts: ForecastParameters,
) -> Tuple[Dict[str, Any], List[str]]:
    patch: Dict[str, Any] = {}
    errors: List[str] = []

    price = _get_float(row, "current_price", "price", "last_price", "last")
    fair = _get_float(
        row,
        "intrinsic_value", "fair_value", "target_price",
        "target_mean_price", "forecast_price_12m",
        "forecast_price_3m", "forecast_price_1m"
    )

    roi1 = _as_roi_fraction(_get(row, "expected_roi_1m", "expected_return_1m"))
    roi3 = _as_roi_fraction(_get(row, "expected_roi_3m", "expected_return_3m"))
    roi12 = _as_roi_fraction(_get(row, "expected_roi_12m", "expected_return_12m"))

    fp1 = _get_float(row, "forecast_price_1m", "expected_price_1m")
    fp3 = _get_float(row, "forecast_price_3m", "expected_price_3m")
    fp12 = _get_float(row, "forecast_price_12m", "expected_price_12m")

    if price is not None and price > 0:
        if roi12 is None and fp12 is not None and fp12 > 0:
            roi12 = (fp12 / price) - 1.0
        if roi3 is None and fp3 is not None and fp3 > 0:
            roi3 = (fp3 / price) - 1.0
        if roi1 is None and fp1 is not None and fp1 > 0:
            roi1 = (fp1 / price) - 1.0

    if price is not None and price > 0 and roi12 is None and fair is not None and fair > 0:
        roi12 = (fair / price) - 1.0

    if roi12 is not None:
        roi12 = _clamp(roi12, forecasts.min_roi_12m, forecasts.max_roi_12m)
    if roi3 is None and roi12 is not None:
        roi3 = _clamp(roi12 * forecasts.ratio_3m_of_12m, forecasts.min_roi_3m, forecasts.max_roi_3m)
    if roi1 is None and roi12 is not None:
        roi1 = _clamp(roi12 * forecasts.ratio_1m_of_12m, forecasts.min_roi_1m, forecasts.max_roi_1m)

    if roi3 is not None:
        roi3 = _clamp(roi3, forecasts.min_roi_3m, forecasts.max_roi_3m)
    if roi1 is not None:
        roi1 = _clamp(roi1, forecasts.min_roi_1m, forecasts.max_roi_1m)

    if price is not None and price > 0:
        if fp12 is None and roi12 is not None:
            fp12 = price * (1.0 + roi12)
        if fp3 is None and roi3 is not None:
            fp3 = price * (1.0 + roi3)
        if fp1 is None and roi1 is not None:
            fp1 = price * (1.0 + roi1)
    elif fair is None:
        errors.append("price_unavailable_for_forecast")

    patch["forecast_price_1m"] = _round(fp1, 4)
    patch["forecast_price_3m"] = _round(fp3, 4)
    patch["forecast_price_12m"] = _round(fp12, 4)
    patch["expected_roi_1m"] = _round(roi1, 6)
    patch["expected_roi_3m"] = _round(roi3, 6)
    patch["expected_roi_12m"] = _round(roi12, 6)

    patch["expected_return_1m"] = patch["expected_roi_1m"]
    patch["expected_return_3m"] = patch["expected_roi_3m"]
    patch["expected_return_12m"] = patch["expected_roi_12m"]
    patch["expected_price_1m"] = patch["forecast_price_1m"]
    patch["expected_price_3m"] = patch["forecast_price_3m"]
    patch["expected_price_12m"] = patch["forecast_price_12m"]

    return patch, errors


# =============================================================================
# Component Scoring
# =============================================================================

def _data_quality_factor(row: Mapping[str, Any]) -> float:
    dq = str(_get(row, "data_quality") or "").strip().upper()
    quality_map = {
        "EXCELLENT": 0.95,
        "HIGH": 0.85,
        "GOOD": 0.80,
        "MEDIUM": 0.68,
        "FAIR": 0.60,
        "POOR": 0.40,
        "STALE": 0.45,
        "MISSING": 0.20,
        "ERROR": 0.15,
    }
    return quality_map.get(dq, 0.60)


def _completeness_factor(row: Mapping[str, Any]) -> float:
    core_fields = [
        "symbol", "name", "currency", "exchange", "current_price", "previous_close",
        "day_high", "day_low", "week_52_high", "week_52_low", "volume", "market_cap",
        "pe_ttm", "pb_ratio", "ps_ratio", "dividend_yield", "rsi_14",
        "volatility_30d", "volatility_90d", "expected_roi_3m", "forecast_price_3m",
        "forecast_confidence",
    ]
    present = sum(1 for k in core_fields if row.get(k) not in (None, "", [], {}))
    return present / max(1, len(core_fields))


def compute_quality_score(row: Mapping[str, Any]) -> Optional[float]:
    dq = _data_quality_factor(row)
    comp = _completeness_factor(row)
    data_quality_proxy = _clamp(0.55 * dq + 0.45 * comp, 0.0, 1.0)

    roe = _as_fraction(_get(row, "roe", "return_on_equity", "returnOnEquity"))
    roa = _as_fraction(_get(row, "roa", "return_on_assets", "returnOnAssets"))
    op_margin = _as_fraction(_get(row, "operating_margin", "operatingMarginTTM"))
    net_margin = _as_fraction(_get(row, "profit_margin", "net_margin", "profitMargins"))
    de = _get_float(row, "debt_to_equity", "debtToEquity")

    fin_parts: List[Tuple[float, float]] = []

    if roe is not None:
        fin_parts.append((0.30, _clamp((roe - 0.05) / 0.30, 0.0, 1.0)))
    if roa is not None:
        fin_parts.append((0.25, _clamp((roa - 0.02) / 0.16, 0.0, 1.0)))
    if op_margin is not None:
        fin_parts.append((0.25, _clamp((op_margin - 0.05) / 0.35, 0.0, 1.0)))
    if net_margin is not None:
        fin_parts.append((0.15, _clamp((net_margin - 0.02) / 0.28, 0.0, 1.0)))
    if de is not None and de >= 0:
        fin_parts.append((0.05, _clamp(1.0 - (de / 2.5), 0.0, 1.0)))

    if fin_parts:
        wsum = sum(w for w, _ in fin_parts)
        financial_quality = sum(w * v for w, v in fin_parts) / max(1e-9, wsum)
        combined = 0.40 * financial_quality + 0.60 * data_quality_proxy
    else:
        combined = data_quality_proxy

    return _round(100.0 * _clamp(combined, 0.0, 1.0), 2)


def compute_confidence_score(row: Mapping[str, Any]) -> Tuple[Optional[float], Optional[float]]:
    fc = _safe_float(_get(row, "forecast_confidence", "ai_confidence", "confidence_score", "confidence"))
    if fc is not None:
        fc01 = (fc / 100.0) if fc > 1.5 else fc
        fc01 = _clamp(fc01, 0.0, 1.0)
        return _round(fc01 * 100.0, 2), _round(fc01, 4)

    dq = _data_quality_factor(row)
    comp = _completeness_factor(row)
    provs = row.get("data_sources") or row.get("providers") or []
    try:
        pcount = len(provs) if isinstance(provs, list) else 0
    except Exception:
        pcount = 0
    prov_factor = _clamp(pcount / 3.0, 0.0, 1.0)
    conf01 = _clamp(0.55 * dq + 0.35 * comp + 0.10 * prov_factor, 0.0, 1.0)
    return _round(conf01 * 100.0, 2), _round(conf01, 4)


def compute_valuation_score(row: Mapping[str, Any]) -> Optional[float]:
    """
    Compute valuation score (0-100).

    v4.1.4: weight normalization for partial-signal symbols. When only
    some forward signals are available, the weighted average divides by
    the sum of available weights — which mathematically gives the raw
    average, but made partial-signal scores not directly comparable to
    full-signal scores. v4.1.4 normalizes by the full expected weight,
    treating missing components as neutral 0.5 (not 0.0) so one strong
    positive signal doesn't get a free pass.

    v4.1.2: returns None if no forward-looking signal is available
    (upside / 3m ROI / 12m ROI all missing). Anchors-only (PE/PB/PS/PEG/
    EV) is insufficient because those fields point backward, not forward.
    """
    price = _get_float(row, "current_price", "price", "last_price", "last")
    if price is None or price <= 0:
        return None

    fair = _get_float(
        row,
        "intrinsic_value", "fair_value", "target_price",
        "forecast_price_3m", "forecast_price_12m", "forecast_price_1m"
    )
    upside = ((fair / price) - 1.0) if fair is not None and fair > 0 else None

    roi3 = _as_roi_fraction(_get(row, "expected_roi_3m", "expected_return_3m"))
    roi12 = _as_roi_fraction(_get(row, "expected_roi_12m", "expected_return_12m"))

    def _roi_norm(frac: Optional[float], cap: float) -> Optional[float]:
        if frac is None:
            return None
        return _clamp((frac + cap) / (2 * cap), 0.0, 1.0)

    pe = _get_float(row, "pe_ttm", "pe_ratio")
    pb = _get_float(row, "pb_ratio", "pb", "price_to_book")
    ps = _get_float(row, "ps_ratio", "ps", "price_to_sales")
    peg = _get_float(row, "peg_ratio", "peg")
    ev = _get_float(row, "ev_ebitda", "ev_to_ebitda")

    def _low_is_good(x: Optional[float], lo: float, hi: float) -> Optional[float]:
        if x is None or x <= 0:
            return None
        if x <= lo:
            return 1.0
        if x >= hi:
            return 0.0
        return 1.0 - ((x - lo) / (hi - lo))

    anchors = [
        a for a in (
            _low_is_good(pe, 8.0, 35.0),
            _low_is_good(pb, 0.8, 6.0),
            _low_is_good(ps, 1.0, 10.0),
            _low_is_good(peg, 0.8, 4.0),
            _low_is_good(ev, 6.0, 25.0),
        ) if a is not None
    ]
    anchor_avg = (sum(anchors) / len(anchors)) if anchors else None

    upside_n = _roi_norm(upside, 0.50)
    roi3_n = _roi_norm(roi3, 0.35)
    roi12_n = _roi_norm(roi12, 0.80)

    # v4.1.2: require at least one forward-looking signal.
    if upside_n is None and roi3_n is None and roi12_n is None:
        return None

    # v4.1.4: normalize over full expected weight (1.00).
    # Missing components contribute 0.5 (neutral) * their weight, so
    # scores are on a consistent 0-100 scale regardless of which
    # forward signals are present.
    FULL_WEIGHT = 1.00
    components: List[Tuple[float, Optional[float]]] = [
        (0.40, upside_n),
        (0.30, roi3_n),
        (0.20, roi12_n),
        (0.10, anchor_avg),
    ]

    total = 0.0
    for weight, value in components:
        if value is not None:
            total += weight * value
        else:
            # Missing component treated as neutral 0.5
            total += weight * 0.5

    score_01 = total / FULL_WEIGHT
    return _round(100.0 * _clamp(score_01, 0.0, 1.0), 2)


def compute_growth_score(row: Mapping[str, Any]) -> Optional[float]:
    g = _as_fraction(_get(row, "revenue_growth_yoy", "revenue_growth", "growth_yoy"))
    if g is None:
        return None
    return _round(_clamp(((g + 0.30) / 0.60) * 100.0, 0.0, 100.0), 2)


def compute_momentum_score(row: Mapping[str, Any]) -> Optional[float]:
    pct = _as_roi_fraction(_get(row, "percent_change", "change_pct", "change_percent"))
    rsi = _get_float(row, "rsi_14", "rsi", "rsi14")
    pos = _as_fraction(_get(row, "week_52_position_pct", "position_52w_pct", "week52_position_pct"))
    pct_5d = _as_roi_fraction(_get(row, "price_change_5d"))
    vol_r = derive_volume_ratio(row)

    parts: List[Tuple[float, float]] = []

    if rsi is not None:
        x = (rsi - 55.0) / 12.0
        parts.append((0.30, _clamp(math.exp(-(x * x)), 0.0, 1.0)))

    if pct is not None:
        parts.append((0.25, _clamp((pct + 0.10) / 0.20, 0.0, 1.0)))

    if pct_5d is not None:
        parts.append((0.20, _clamp((pct_5d + 0.08) / 0.16, 0.0, 1.0)))

    if pos is not None:
        parts.append((0.15, _clamp(pos, 0.0, 1.0)))

    if vol_r is not None:
        parts.append((0.10, _clamp((vol_r - 0.5) / 1.5, 0.0, 1.0)))

    if not parts:
        return None

    wsum = sum(w for w, _ in parts)
    score_01 = sum(w * v for w, v in parts) / max(1e-9, wsum)
    return _round(100.0 * _clamp(score_01, 0.0, 1.0), 2)


def compute_risk_score(row: Mapping[str, Any]) -> Optional[float]:
    vol90 = _as_fraction(_get(row, "volatility_90d"))
    dd1y = _as_fraction(_get(row, "max_drawdown_1y"))
    var1d = _as_fraction(_get(row, "var_95_1d"))
    sharpe = _get_float(row, "sharpe_1y")

    def _scale(x: Optional[float], lo: float, hi: float) -> Optional[float]:
        if x is None:
            return None
        return _clamp((x - lo) / (hi - lo), 0.0, 1.0) if hi > lo else None

    parts: List[Tuple[float, float]] = []
    if vol90 is not None:
        parts.append((0.40, _scale(vol90, 0.12, 0.70) or 0.0))
    if dd1y is not None:
        parts.append((0.35, _scale(abs(dd1y), 0.05, 0.55) or 0.0))
    if var1d is not None:
        parts.append((0.20, _scale(var1d, 0.01, 0.08) or 0.0))
    if sharpe is not None:
        sharpe_norm = _clamp(1.0 - _clamp((sharpe + 0.5) / 2.5, 0.0, 1.0), 0.0, 1.0)
        parts.append((0.05, sharpe_norm))

    if not parts:
        vol = _as_fraction(_get(row, "volatility_30d", "vol_30d"))
        beta = _get_float(row, "beta_5y", "beta")
        dd = _as_fraction(_get(row, "max_drawdown_30d", "drawdown_30d"))
        if vol is not None:
            parts.append((0.50, _scale(vol, 0.10, 0.60) or 0.0))
        if beta is not None:
            parts.append((0.30, _scale(beta, 0.60, 2.00) or 0.0))
        if dd is not None:
            parts.append((0.20, _scale(abs(dd), 0.00, 0.50) or 0.0))

    if not parts:
        return None

    wsum = sum(w for w, _ in parts)
    score_01 = sum(w * v for w, v in parts) / max(1e-9, wsum)
    return _round(100.0 * _clamp(score_01, 0.0, 1.0), 2)


def compute_opportunity_score(
    row: Mapping[str, Any],
    valuation: Optional[float],
    momentum: Optional[float],
) -> Optional[float]:
    roi1 = _as_roi_fraction(_get(row, "expected_roi_1m", "expected_return_1m"))
    roi3 = _as_roi_fraction(_get(row, "expected_roi_3m", "expected_return_3m"))
    roi12 = _as_roi_fraction(_get(row, "expected_roi_12m", "expected_return_12m"))

    def _roi_norm(frac: Optional[float], cap: float) -> Optional[float]:
        if frac is None:
            return None
        return _clamp((frac + cap) / (2 * cap), 0.0, 1.0)

    parts: List[Tuple[float, float]] = []
    r3 = _roi_norm(roi3, 0.35)
    r12 = _roi_norm(roi12, 0.80)
    r1 = _roi_norm(roi1, 0.25)

    if r3 is not None:
        parts.append((0.55, r3))
    if r12 is not None:
        parts.append((0.30, r12))
    if r1 is not None:
        parts.append((0.15, r1))

    if parts:
        wsum = sum(w for w, _ in parts)
        return _round(100.0 * _clamp(sum(w * v for w, v in parts) / max(1e-9, wsum), 0.0, 1.0), 2)

    if valuation is None and momentum is None:
        return None

    v = (valuation or 50.0) / 100.0
    m = (momentum or 50.0) / 100.0
    return _round(100.0 * _clamp(0.60 * v + 0.40 * m, 0.0, 1.0), 2)


def risk_bucket(score: Optional[float]) -> Optional[str]:
    if score is None:
        return None
    if score <= _CONFIG.risk_low_threshold:
        return "Low"
    if score <= _CONFIG.risk_moderate_threshold:
        return "Moderate"
    return "High"


def confidence_bucket(conf01: Optional[float]) -> Optional[str]:
    if conf01 is None:
        return None
    if conf01 >= _CONFIG.confidence_high:
        return "High"
    if conf01 >= _CONFIG.confidence_medium:
        return "Medium"
    return "Low"


def compute_recommendation(
    overall: Optional[float],
    risk: Optional[float],
    confidence100: Optional[float],
    roi3: Optional[float],
    horizon: Horizon = Horizon.MONTH,
    technical: Optional[float] = None,
    momentum: Optional[float] = None,
    roi1: Optional[float] = None,
    roi12: Optional[float] = None,
    *,
    quality: Optional[float] = None,
    growth: Optional[float] = None,
    valuation: Optional[float] = None,
) -> Tuple[str, str]:
    """
    Compute recommendation based on scores and horizon.

    v4.1.4 blue-chip guard (replaces v4.1.3):
        In LONG and MONTH horizons, when forward-looking data is entirely
        missing (no 1M/3M/12M ROI, no valuation score), suppress SELL and
        REDUCE recommendations if either:

        (a) FULL-SIGNAL GUARD: quality >= 65 AND growth >= 65 AND risk <= 55
            Same as v4.1.3. Fires when all three fundamental signals are
            available and healthy.

        (b) QUALITY-ONLY GUARD: quality >= 70 AND risk <= 50
            NEW in v4.1.4. Fires when growth is None (common for ETFs,
            commodities, and equities whose provider omitted
            revenue_growth_yoy). Tighter thresholds because we're using
            fewer inputs.

        Either case returns HOLD with a reason that names the protection.
        DAY and WEEK horizons are unchanged — short-term technical
        signals are still allowed to fire regardless of fundamentals.

        Requires `quality` to be passed in; if None, neither case fires
        (equivalent to v4.1.2 behavior).
    """
    if overall is None:
        return "HOLD", "Insufficient data to score reliably."

    r = risk if risk is not None else 50.0
    c = confidence100 if confidence100 is not None else 55.0
    t = technical if technical is not None else 50.0
    m = momentum if momentum is not None else 50.0
    q = quality if quality is not None else 0.0
    g = growth if growth is not None else 0.0

    if c < 40:
        return "HOLD", f"Low AI confidence ({_round(c, 1)}%) — insufficient signal quality."

    # v4.1.4: blue-chip guard with two independent cases.
    no_forward_data = (
        roi1 is None
        and roi3 is None
        and roi12 is None
        and valuation is None
    )
    guard_case_a = (
        no_forward_data
        and quality is not None and q >= 65.0
        and growth is not None and g >= 65.0
        and r <= 55.0
    )
    guard_case_b = (
        no_forward_data
        and quality is not None and q >= 70.0
        and r <= 50.0
    )
    blue_chip_guard = guard_case_a or guard_case_b

    # DAY horizon
    if horizon == Horizon.DAY:
        if t >= 80 and m >= 75 and r <= 45:
            return "STRONG_BUY", (
                f"Strong technical setup: Tech={_round(t, 1)}, "
                f"Momentum={_round(m, 1)}, Risk=Low ({_round(r, 1)})."
            )
        if t >= 65 and m >= 55 and r <= 60:
            return "BUY", (
                f"Technical momentum confirms entry: Tech={_round(t, 1)}, "
                f"Momentum={_round(m, 1)}."
            )
        if t >= 50 and r <= 55:
            return "HOLD", f"Neutral technical setup — await stronger signal (Tech={_round(t, 1)})."
        if t < 38 or m < 30:
            return "SELL", (
                f"Technical deterioration — avoid: Tech={_round(t, 1)}, "
                f"Momentum={_round(m, 1)}."
            )
        return "HOLD", f"Day trade setup inconclusive (Tech={_round(t, 1)}, Risk={_round(r, 1)})."

    # WEEK horizon
    if horizon == Horizon.WEEK:
        roi_1m = roi1 if roi1 is not None else 0.0
        if t >= 72 and m >= 65 and r <= 50:
            return "STRONG_BUY", (
                f"Strong week setup: Tech={_round(t, 1)}, "
                f"Momentum={_round(m, 1)}, Risk=Low. 1M ROI≈{_round(roi_1m * 100, 1)}%."
            )
        if t >= 58 and m >= 50 and r <= 65 and roi_1m >= 0.015:
            return "BUY", (
                f"Week momentum setup: Tech={_round(t, 1)}, "
                f"1M ROI≈{_round(roi_1m * 100, 1)}%, Risk={_round(r, 1)}."
            )
        if t < 35 or m < 30:
            return "SELL", (
                f"Weak week setup — technical breakdown: Tech={_round(t, 1)}, "
                f"Momentum={_round(m, 1)}."
            )
        return "HOLD", f"Week setup not compelling — wait for better entry (Tech={_round(t, 1)})."

    # LONG horizon
    if horizon == Horizon.LONG:
        roi_12m = roi12 if roi12 is not None else 0.0
        if r >= 75 and overall < 75:
            return "REDUCE", f"High long-term risk ({_round(r, 1)}) with moderate score ({_round(overall, 1)})."
        if roi_12m >= 0.25 and c >= 70 and r <= 45 and overall >= 78:
            return "STRONG_BUY", f"Strong 12M expected return ({_round(roi_12m * 100, 1)}%) with low risk."
        if roi_12m >= 0.12 and c >= 60 and r <= 60 and overall >= 68:
            return "BUY", f"Positive 12M outlook ({_round(roi_12m * 100, 1)}%) with acceptable risk."
        if overall >= 75 and r <= 50:
            return "BUY", f"Strong long-term score ({_round(overall, 1)}) with controlled risk."
        if overall >= 65:
            return "HOLD", f"Moderate long-term profile (score={_round(overall, 1)})."
        # v4.1.4: blue-chip guard fires BEFORE REDUCE/SELL.
        if blue_chip_guard:
            reason_fragment = (
                f"Quality={_round(q, 0)}, Growth={_round(g, 0)}, Risk={_round(r, 0)}"
                if guard_case_a
                else f"Quality={_round(q, 0)}, Risk={_round(r, 0)} (growth data unavailable)"
            )
            return "HOLD", (
                f"Strong fundamentals ({reason_fragment}) with forward "
                f"outlook unavailable — holding pending provider signal."
            )
        if overall >= 50:
            return "REDUCE", f"Weak long-term outlook (score={_round(overall, 1)})."
        return "SELL", f"Poor long-term fundamentals (score={_round(overall, 1)})."

    # MONTH horizon (default)
    roi_3m = roi3 if roi3 is not None else 0.0
    if r >= 75 and overall < 75:
        return "REDUCE", f"High risk ({_round(r, 1)}) with moderate score ({_round(overall, 1)})."
    if roi_3m >= 0.25 and c >= 70 and r <= 45 and overall >= 78:
        return "STRONG_BUY", f"High expected 3M ROI (~{_round(roi_3m * 100, 1)}%) with strong confidence and low risk."
    if roi_3m >= 0.12 and c >= 60 and r <= 55 and overall >= 70:
        return "BUY", f"Positive 3M expected ROI (~{_round(roi_3m * 100, 1)}%) with acceptable risk/confidence."
    if overall >= 82 and r <= 55:
        return "BUY", f"Strong overall score ({_round(overall, 1)}) with controlled risk ({_round(r, 1)})."
    if overall >= 65:
        return "HOLD", f"Moderate overall score ({_round(overall, 1)})."
    # v4.1.4: blue-chip guard fires BEFORE REDUCE/SELL.
    if blue_chip_guard:
        reason_fragment = (
            f"Quality={_round(q, 0)}, Growth={_round(g, 0)}, Risk={_round(r, 0)}"
            if guard_case_a
            else f"Quality={_round(q, 0)}, Risk={_round(r, 0)} (growth data unavailable)"
        )
        return "HOLD", (
            f"Strong fundamentals ({reason_fragment}) with forward "
            f"outlook unavailable — holding pending provider signal."
        )
    if overall >= 50:
        return "REDUCE", f"Weak overall score ({_round(overall, 1)})."
    return "SELL", f"Very weak overall score ({_round(overall, 1)})."


# =============================================================================
# Recommendation Normalization
# =============================================================================

_LOCAL_RECO_ALIASES: Dict[str, str] = {
    "STRONG_BUY": "STRONG_BUY",
    "STRONGBUY": "STRONG_BUY",
    "CONVICTION_BUY": "STRONG_BUY",
    "TOP_PICK": "STRONG_BUY",
    "BUY": "BUY",
    "ACCUMULATE": "BUY",
    "ADD": "BUY",
    "OUTPERFORM": "BUY",
    "OVERWEIGHT": "BUY",
    "HOLD": "HOLD",
    "NEUTRAL": "HOLD",
    "MAINTAIN": "HOLD",
    "MARKET_PERFORM": "HOLD",
    "WATCH": "HOLD",
    "REDUCE": "REDUCE",
    "TRIM": "REDUCE",
    "UNDERWEIGHT": "REDUCE",
    "SELL": "SELL",
    "AVOID": "SELL",
    "EXIT": "SELL",
    "UNDERPERFORM": "SELL",
    "STRONG_SELL": "SELL",
}

CANONICAL_RECOMMENDATION_CODES: Tuple[str, ...] = (
    "STRONG_BUY", "BUY", "HOLD", "REDUCE", "SELL",
)
_CANONICAL_RECO = set(CANONICAL_RECOMMENDATION_CODES)


def _normalize_key(label: Any) -> str:
    s = _safe_str(label).upper()
    if not s:
        return ""
    s = s.replace("-", "_").replace(" ", "_")
    while "__" in s:
        s = s.replace("__", "_")
    return s.strip("_")


def normalize_recommendation_code(label: Any) -> str:
    # Step 1: canonical fast path
    key = _normalize_key(label)
    if not key:
        return "HOLD"
    if key in _CANONICAL_RECO:
        return key

    # Step 2: local alias table
    if key in _LOCAL_RECO_ALIASES:
        return _LOCAL_RECO_ALIASES[key]

    # Step 3: delegate to reco_normalize for unknown inputs
    try:
        from core.reco_normalize import normalize_recommendation as _reco_norm  # noqa: WPS433
        normalized = _reco_norm(label)
        if normalized in _CANONICAL_RECO:
            return normalized
        normalized_key = _normalize_key(normalized)
        if normalized_key in _CANONICAL_RECO:
            return normalized_key
        if normalized_key in _LOCAL_RECO_ALIASES:
            return _LOCAL_RECO_ALIASES[normalized_key]
    except Exception:
        pass

    # Step 4: default
    return "HOLD"


# =============================================================================
# Main Scoring Function
# =============================================================================

def compute_scores(row: Dict[str, Any], settings: Any = None) -> Dict[str, Any]:
    source = dict(row or {})
    scoring_errors: List[str] = []

    forecasts = _forecast_params_from_settings(settings)
    forecast_patch, forecast_errors = derive_forecast_patch(source, forecasts)
    scoring_errors.extend(forecast_errors)

    working = dict(source)
    working.update({k: v for k, v in forecast_patch.items() if v is not None})

    horizon, hdays = detect_horizon(settings, working)
    weights = get_weights_for_horizon(horizon, settings)

    valuation = compute_valuation_score(working)
    momentum = compute_momentum_score(working)
    quality = compute_quality_score(working)
    growth = compute_growth_score(working)
    confidence100, conf01 = compute_confidence_score(working)
    risk = compute_risk_score(working)
    opportunity = compute_opportunity_score(working, valuation, momentum)
    value_score = valuation

    tech_score = compute_technical_score(working)
    vol_ratio = derive_volume_ratio(working)
    drp = derive_day_range_position(working)
    usp = derive_upside_pct(working)
    rsi_val = _get_float(working, "rsi_14", "rsi", "rsi14")
    rsi_sig = rsi_signal(rsi_val)

    base_parts: List[Tuple[float, float]] = []
    if weights.w_technical > 0 and tech_score is not None:
        base_parts.append((weights.w_technical, tech_score / 100.0))
    if weights.w_valuation > 0 and valuation is not None:
        base_parts.append((weights.w_valuation, valuation / 100.0))
    if weights.w_momentum > 0 and momentum is not None:
        base_parts.append((weights.w_momentum, momentum / 100.0))
    if weights.w_quality > 0 and quality is not None:
        base_parts.append((weights.w_quality, quality / 100.0))
    if weights.w_growth > 0 and growth is not None:
        base_parts.append((weights.w_growth, growth / 100.0))
    if weights.w_opportunity > 0 and opportunity is not None:
        base_parts.append((weights.w_opportunity, opportunity / 100.0))

    overall: Optional[float] = None
    overall_raw: Optional[float] = None
    penalty_factor: Optional[float] = None
    insufficient_inputs = False

    if base_parts:
        wsum = sum(x[0] for x in base_parts)
        base01 = sum(w * v for w, v in base_parts) / max(1e-9, wsum)
        overall_raw = _round(100.0 * _clamp(base01, 0.0, 1.0), 2)

        risk01 = (risk / 100.0) if risk is not None else 0.50
        conf01_used = conf01 if conf01 is not None else 0.55

        risk_pen = _clamp(1.0 - weights.risk_penalty_strength * (risk01 * 0.70), 0.0, 1.0)
        conf_pen = _clamp(1.0 - weights.confidence_penalty_strength * ((1.0 - conf01_used) * 0.80), 0.0, 1.0)
        penalty_factor = _round(risk_pen * conf_pen, 4)

        base01 *= (risk_pen * conf_pen)
        overall = _round(100.0 * _clamp(base01, 0.0, 1.0), 2)
    else:
        overall = 50.0
        overall_raw = 50.0
        penalty_factor = 1.0
        insufficient_inputs = True
        scoring_errors.append("insufficient_scoring_inputs")

    rb = risk_bucket(risk)
    cb = confidence_bucket(conf01)

    roi3 = _as_roi_fraction(working.get("expected_roi_3m"))
    roi1 = _as_roi_fraction(working.get("expected_roi_1m"))
    roi12 = _as_roi_fraction(working.get("expected_roi_12m"))

    if insufficient_inputs:
        rec, reason = "HOLD", "Insufficient scoring inputs available."
    else:
        rec, reason = compute_recommendation(
            overall, risk, confidence100, roi3,
            horizon=horizon, technical=tech_score, momentum=momentum,
            roi1=roi1, roi12=roi12,
            quality=quality,
            growth=growth,
            valuation=valuation,
        )

    st_signal_val = short_term_signal(tech_score, momentum, risk, horizon)
    period_label = invest_period_label(horizon, hdays)

    scores = AssetScores(
        valuation_score=valuation,
        momentum_score=momentum,
        quality_score=quality,
        growth_score=growth,
        value_score=value_score,
        opportunity_score=opportunity,
        confidence_score=confidence100,
        forecast_confidence=conf01,
        confidence_bucket=cb,
        risk_score=risk,
        risk_bucket=rb,
        overall_score=overall,
        overall_score_raw=overall_raw,
        overall_penalty_factor=penalty_factor,
        technical_score=tech_score,
        rsi_signal=rsi_sig,
        short_term_signal=st_signal_val,
        day_range_position=drp,
        volume_ratio=vol_ratio,
        upside_pct=usp,
        invest_period_label=period_label,
        horizon_label=horizon.value,
        horizon_days_effective=hdays,
        recommendation=normalize_recommendation_code(rec),
        recommendation_reason=reason,
        forecast_price_1m=forecast_patch.get("forecast_price_1m"),
        forecast_price_3m=forecast_patch.get("forecast_price_3m"),
        forecast_price_12m=forecast_patch.get("forecast_price_12m"),
        expected_roi_1m=forecast_patch.get("expected_roi_1m"),
        expected_roi_3m=forecast_patch.get("expected_roi_3m"),
        expected_roi_12m=forecast_patch.get("expected_roi_12m"),
        expected_return_1m=forecast_patch.get("expected_return_1m"),
        expected_return_3m=forecast_patch.get("expected_return_3m"),
        expected_return_12m=forecast_patch.get("expected_return_12m"),
        expected_price_1m=forecast_patch.get("expected_price_1m"),
        expected_price_3m=forecast_patch.get("expected_price_3m"),
        expected_price_12m=forecast_patch.get("expected_price_12m"),
        scoring_errors=scoring_errors,
    )
    return scores.to_dict()


def score_row(row: Dict[str, Any], settings: Any = None) -> Dict[str, Any]:
    return compute_scores(row, settings=settings)


def score_quote(row: Dict[str, Any], settings: Any = None) -> Dict[str, Any]:
    return compute_scores(row, settings=settings)


def enrich_with_scores(
    row: Dict[str, Any],
    settings: Any = None,
    in_place: bool = False,
) -> Dict[str, Any]:
    target = row if in_place else dict(row or {})
    patch = compute_scores(target, settings=settings)
    target.update(patch)
    return target


class ScoringEngine:
    """Lightweight wrapper supporting object-style callers."""

    version = SCORING_VERSION

    def __init__(
        self,
        settings: Any = None,
        weights: Optional[ScoreWeights] = None,
        forecasts: Optional[ForecastParameters] = None,
    ):
        self.settings = settings
        self.weights = weights or DEFAULT_WEIGHTS
        self.forecasts = forecasts or DEFAULT_FORECASTS

    def compute_scores(self, row: Dict[str, Any]) -> Dict[str, Any]:
        return compute_scores(row, settings=self.settings)

    def enrich_with_scores(self, row: Dict[str, Any], in_place: bool = False) -> Dict[str, Any]:
        return enrich_with_scores(row, settings=self.settings, in_place=in_place)


# =============================================================================
# Ranking Helpers
# =============================================================================

def _rank_sort_tuple(row: Dict[str, Any], key_overall: str = "overall_score") -> Tuple[float, ...]:
    overall = _norm_score_0_100(row.get(key_overall))
    opp = _norm_score_0_100(row.get("opportunity_score"))
    conf = _norm_score_0_100(row.get("confidence_score"))
    risk = _norm_score_0_100(row.get("risk_score"))
    roi3 = _as_roi_fraction(row.get("expected_roi_3m"))
    symbol = _safe_str(row.get("symbol"), "~")
    return (
        overall if overall is not None else -1e9,
        opp if opp is not None else -1e9,
        conf if conf is not None else -1e9,
        -(risk if risk is not None else 1e9),
        roi3 if roi3 is not None else -1e9,
        symbol,
    )


def assign_rank_overall(
    rows: Sequence[Dict[str, Any]],
    key_overall: str = "overall_score",
    inplace: bool = True,
    rank_key: str = "rank_overall",
) -> List[Dict[str, Any]]:
    target = list(rows) if inplace else [dict(r or {}) for r in rows]
    indexed = list(enumerate(target))
    indexed.sort(key=lambda item: _rank_sort_tuple(item[1], key_overall=key_overall), reverse=True)
    for rank, (_, row) in enumerate(indexed, start=1):
        row[rank_key] = rank
    return target


def rank_rows_by_overall(
    rows: List[Dict[str, Any]],
    key_overall: str = "overall_score",
) -> List[Dict[str, Any]]:
    return assign_rank_overall(rows, key_overall=key_overall, inplace=True, rank_key="rank_overall")


def score_and_rank_rows(
    rows: Sequence[Dict[str, Any]],
    settings: Any = None,
    key_overall: str = "overall_score",
    inplace: bool = False,
) -> List[Dict[str, Any]]:
    prepared = list(rows) if inplace else [dict(r or {}) for r in rows]
    for row in prepared:
        try:
            row.update(compute_scores(row, settings=settings))
        except Exception as exc:
            logger.debug("score_and_rank_rows: scoring failed for row: %s", exc)
            existing_errors = row.get("scoring_errors")
            if not isinstance(existing_errors, list):
                existing_errors = []
            existing_errors.append(f"scoring_exception: {type(exc).__name__}")
            row["scoring_errors"] = existing_errors
    assign_rank_overall(prepared, key_overall=key_overall, inplace=True, rank_key="rank_overall")
    return prepared


# =============================================================================
# Module Exports
# =============================================================================

__all__ = [
    "__version__",
    "SCORING_VERSION",
    "Horizon",
    "Signal",
    "RSISignal",
    "compute_scores",
    "score_row",
    "score_quote",
    "enrich_with_scores",
    "rank_rows_by_overall",
    "assign_rank_overall",
    "score_and_rank_rows",
    "AssetScores",
    "ScoreWeights",
    "ScoringWeights",
    "ForecastParameters",
    "ScoringEngine",
    "DEFAULT_WEIGHTS",
    "DEFAULT_FORECASTS",
    "normalize_recommendation_code",
    "CANONICAL_RECOMMENDATION_CODES",
    "detect_horizon",
    "get_weights_for_horizon",
    "compute_technical_score",
    "rsi_signal",
    "short_term_signal",
    "derive_upside_pct",
    "derive_volume_ratio",
    "derive_day_range_position",
    "invest_period_label",
    "compute_valuation_score",
    "compute_growth_score",
    "compute_momentum_score",
    "compute_quality_score",
    "compute_risk_score",
    "compute_opportunity_score",
    "compute_confidence_score",
    "compute_recommendation",
    "risk_bucket",
    "confidence_bucket",
    "ScoringError",
    "InvalidHorizonError",
    "MissingDataError",
]
