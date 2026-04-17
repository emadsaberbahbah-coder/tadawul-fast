#!/usr/bin/env python3
# core/scoring.py
"""
================================================================================
Scoring Module — v3.0.0
(HORIZON-AWARE / TECHNICAL-SIGNALS / SHORT-TERM READY / SCHEMA-ALIGNED)
================================================================================

v3.0.0 changes vs v2.4.0
--------------------------
HORIZON-AWARE SCORING
  compute_scores() now detects the investment horizon (day / week / month /
  long) from settings.horizon_days or settings.invest_period_days, and applies
  a different weight set for each horizon:

  HORIZON_DAY   (≤5 days):  technical=50%, momentum=30%, quality=10%, opp=10%
  HORIZON_WEEK  (6-14 days): technical=25%, momentum=25%, valuation=20%,
                              quality=20%, opportunity=10%
  HORIZON_MONTH (15-90 days): valuation=30%, momentum=25%, quality=20%,
                              growth=15%, opportunity=10%  (unchanged from v2.4.0)
  HORIZON_LONG  (>90 days): valuation=35%, quality=25%, growth=20%,
                              momentum=15%, opportunity=5%

NEW: _technical_score()
  Composite short-term technical signal 0-100.
  Formula: 0.40 × RSI_zone_score
         + 0.30 × volume_ratio_score
         + 0.30 × day_range_position_score (near bottom = buy signal)
  Source fields: rsi_14, volume_ratio (or computed volume/avg_volume_10d),
                 day_range_position (or computed from day_high/day_low/price)

NEW: _rsi_signal()  →  Oversold / Neutral / Overbought
NEW: _short_term_signal()  →  BUY / HOLD / SELL per horizon
NEW: _derive_upside_pct()  →  (intrinsic_value - price) / price
NEW: _derive_volume_ratio()  →  volume / avg_volume_10d
NEW: _derive_day_range_position()  →  (price - day_low) / (day_high - day_low)
NEW: _invest_period_label()  →  1D / 1W / 1M / 3M / 12M
NEW: _detect_horizon()  →  day / week / month / long
NEW: _weights_for_horizon()  →  ScoreWeights with correct w_technical

ENHANCED: _momentum_score()
  Old: 0.40×pct_1d + 0.35×RSI_zone + 0.25×52w_position
  New: 0.30×RSI + 0.25×pct_1d + 0.20×pct_5d + 0.15×52w_pos + 0.10×vol_ratio
  5D price change and volume surge are strong short-term momentum confirmations.

ENHANCED: _quality_score()
  Old: 0.55×data_quality_flag + 0.45×field_completeness
  New: if ROE/ROA/margins available:
         0.40×financial_quality + 0.60×data_quality_proxy
       else: existing data quality proxy
  ROE, ROA, operating margin, net margin, D/E ratio = real business quality.

ENHANCED: _recommendation()
  Now accepts horizon, technical_score, momentum_score, roi1 kwargs.
  Short-term path (day/week): uses technical_score as primary signal.
  Medium-term path (month): existing roi_3m + overall logic.
  Long-term path (long): roi_12m becomes primary.

UPDATED: AssetScores
  9 new fields: technical_score, rsi_signal, short_term_signal,
                day_range_position, volume_ratio, upside_pct,
                invest_period_label, horizon_label, horizon_days_effective

UPDATED: ScoreWeights
  New field: w_technical = 0.0 (default — activated only for short horizons)

BACKWARD COMPAT
  All existing public function signatures unchanged.
  ScoringWeights alias preserved.
  DEFAULT_WEIGHTS, DEFAULT_FORECASTS preserved.
  compute_scores() returns all previous fields plus new ones.
================================================================================
"""

from __future__ import annotations

import math
import os
from dataclasses import asdict, dataclass, field, replace
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Mapping, Optional, Sequence, Tuple

__version__ = "3.0.0"
SCORING_VERSION = __version__

_UTC = timezone.utc
_RIYADH = timezone(timedelta(hours=3))


# =============================================================================
# Horizon constants
# =============================================================================
HORIZON_DAY   = "day"    # ≤5 days   — pure technical / momentum play
HORIZON_WEEK  = "week"   # 6-14 days — technical + value blend
HORIZON_MONTH = "month"  # 15-90 days — balanced (original TFB default)
HORIZON_LONG  = "long"   # >90 days  — fundamentals / valuation primary

_HORIZON_DAYS_CUTOFFS: Tuple[Tuple[int, str], ...] = (
    (5,  HORIZON_DAY),
    (14, HORIZON_WEEK),
    (90, HORIZON_MONTH),
)


# =============================================================================
# Utilities
# =============================================================================

def _utc_iso(dt: Optional[datetime] = None) -> str:
    d = dt or datetime.now(_UTC)
    if d.tzinfo is None:
        d = d.replace(tzinfo=_UTC)
    return d.astimezone(_UTC).isoformat()


def _riyadh_iso(dt: Optional[datetime] = None) -> str:
    d = dt or datetime.now(_RIYADH)
    if d.tzinfo is None:
        d = d.replace(tzinfo=_RIYADH)
    return d.astimezone(_RIYADH).isoformat()


def _is_nan(x: float) -> bool:
    try:
        return math.isnan(x) or math.isinf(x)
    except Exception:
        return True


def _clamp(x: float, lo: float, hi: float) -> float:
    if x < lo:
        return lo
    if x > hi:
        return hi
    return x


def _round(x: Optional[float], nd: int = 2) -> Optional[float]:
    if x is None:
        return None
    try:
        y = float(x)
        if _is_nan(y):
            return None
        return round(y, nd)
    except Exception:
        return None


def _safe_float(v: Any) -> Optional[float]:
    if v is None:
        return None
    try:
        if isinstance(v, (int, float)) and not isinstance(v, bool):
            f = float(v)
        else:
            s = str(v).strip().replace(",", "")
            if not s or s.lower() in {"na", "n/a", "none", "null"}:
                return None
            if s.endswith("%"):
                f = float(s[:-1].strip()) / 100.0
            else:
                f = float(s)
        if _is_nan(f):
            return None
        return f
    except Exception:
        return None


def _safe_str(v: Any, default: str = "") -> str:
    try:
        s = str(v).strip()
        return s if s else default
    except Exception:
        return default


def _get(row: Mapping[str, Any], *keys: str) -> Any:
    for k in keys:
        if k in row and row[k] is not None:
            return row[k]
    return None


def _getf(row: Mapping[str, Any], *keys: str) -> Optional[float]:
    return _safe_float(_get(row, *keys))


def _as_fraction(x: Any) -> Optional[float]:
    """Convert percent-like values to fraction. Threshold: abs(f) >= 1.5."""
    f = _safe_float(x)
    if f is None:
        return None
    if abs(f) >= 1.5:
        return f / 100.0
    return f


def _as_roi_fraction(x: Any) -> Optional[float]:
    """Convert ROI / return values to fraction with tighter threshold > 1.0."""
    f = _safe_float(x)
    if f is None:
        return None
    if abs(f) > 1.0:
        return f / 100.0
    return f


def _norm_score_0_100(v: Any) -> Optional[float]:
    f = _safe_float(v)
    if f is None:
        return None
    if 0.0 <= f <= 1.5:
        f *= 100.0
    return _clamp(f, 0.0, 100.0)


def _norm_conf_0_1(v: Any) -> Optional[float]:
    f = _safe_float(v)
    if f is None:
        return None
    if f > 1.5:
        f /= 100.0
    return _clamp(f, 0.0, 1.0)


def _env_float(name: str, default: float) -> float:
    raw = (os.getenv(name) or "").strip()
    if not raw:
        return default
    try:
        return float(raw)
    except Exception:
        return default


# =============================================================================
# Recommendation label normalization
# =============================================================================
CANONICAL_RECOMMENDATION_CODES: Tuple[str, ...] = (
    "STRONG_BUY",
    "BUY",
    "HOLD",
    "REDUCE",
    "SELL",
)

RECOMMENDATION_LABEL_MAP: Dict[str, str] = {
    "STRONG_BUY": "Strong Buy",
    "BUY":        "Buy",
    "HOLD":       "Hold",
    "REDUCE":     "Reduce",
    "SELL":       "Sell",
}

_RECOMMENDATION_CODE_ALIASES: Dict[str, str] = {
    "STRONG_BUY": "STRONG_BUY",
    "STRONGBUY":  "STRONG_BUY",
    "STRONG-BUY": "STRONG_BUY",
    "STRONG BUY": "STRONG_BUY",
    "BUY":        "BUY",
    "ACCUMULATE": "BUY",
    "ADD":        "BUY",
    "HOLD":       "HOLD",
    "NEUTRAL":    "HOLD",
    "REDUCE":     "REDUCE",
    "TRIM":       "REDUCE",
    "SELL":       "SELL",
    "AVOID":      "SELL",
    "EXIT":       "SELL",
}


def _normalize_label_token(label: Any) -> str:
    s = _safe_str(label).upper()
    if not s:
        return ""
    s = s.replace("-", "_").replace(" ", "_")
    while "__" in s:
        s = s.replace("__", "_")
    return s.strip("_")


def normalize_recommendation_code(label: Any) -> str:
    """Normalize a recommendation label to canonical uppercase enum."""
    token = _normalize_label_token(label)
    if not token:
        return "HOLD"
    return _RECOMMENDATION_CODE_ALIASES.get(
        token, token if token in CANONICAL_RECOMMENDATION_CODES else "HOLD"
    )


def normalize_recommendation_label(label: Any) -> str:
    """Normalize a recommendation label to canonical Title Case display format."""
    code = normalize_recommendation_code(label)
    return RECOMMENDATION_LABEL_MAP.get(code, "Hold")


# =============================================================================
# Tunables / types
# =============================================================================

@dataclass(slots=True)
class ScoreWeights:
    # Component weights — must sum to 1.0 (normalized in _weights_from_settings)
    w_valuation:  float = 0.30
    w_momentum:   float = 0.25
    w_quality:    float = 0.20
    w_growth:     float = 0.15
    w_opportunity:float = 0.10
    w_technical:  float = 0.00  # NEW v3.0.0 — non-zero only for day/week horizons

    # Penalty multipliers applied to the raw overall_score
    risk_penalty_strength:       float = 0.55
    confidence_penalty_strength: float = 0.45


@dataclass(slots=True)
class ForecastParameters:
    """Stored internally as fractions. Env values accepted as percentage points."""
    min_roi_1m:  float = field(default_factory=lambda: _env_float("TFB_TEST_MIN_ROI_1M",  -25.0) / 100.0)
    max_roi_1m:  float = field(default_factory=lambda: _env_float("TFB_TEST_MAX_ROI_1M",   25.0) / 100.0)
    min_roi_3m:  float = field(default_factory=lambda: _env_float("TFB_TEST_MIN_ROI_3M",  -35.0) / 100.0)
    max_roi_3m:  float = field(default_factory=lambda: _env_float("TFB_TEST_MAX_ROI_3M",   35.0) / 100.0)
    min_roi_12m: float = field(default_factory=lambda: _env_float("TFB_TEST_MIN_ROI_12M", -65.0) / 100.0)
    max_roi_12m: float = field(default_factory=lambda: _env_float("TFB_TEST_MAX_ROI_12M",  65.0) / 100.0)
    ratio_1m_of_12m: float = 0.18
    ratio_3m_of_12m: float = 0.42


@dataclass(slots=True)
class AssetScores:
    # ── Component scores (0-100) ──────────────────────────────────────────────
    valuation_score:    Optional[float] = None
    momentum_score:     Optional[float] = None
    quality_score:      Optional[float] = None
    growth_score:       Optional[float] = None
    value_score:        Optional[float] = None
    opportunity_score:  Optional[float] = None
    confidence_score:   Optional[float] = None
    forecast_confidence:Optional[float] = None
    confidence_bucket:  Optional[str]   = None
    risk_score:         Optional[float] = None
    risk_bucket:        Optional[str]   = None
    overall_score:      Optional[float] = None
    overall_score_raw:  Optional[float] = None
    overall_penalty_factor: Optional[float] = None

    # ── NEW v3.0.0: technical + derived signals ───────────────────────────────
    technical_score:       Optional[float] = None  # 0-100 composite technical
    rsi_signal:            Optional[str]   = None  # Oversold / Neutral / Overbought
    short_term_signal:     Optional[str]   = None  # BUY / HOLD / SELL (day/week horizon)
    day_range_position:    Optional[float] = None  # 0-1: where price is in today's range
    volume_ratio:          Optional[float] = None  # volume / avg_volume_10d
    upside_pct:            Optional[float] = None  # (intrinsic - price) / price
    invest_period_label:   Optional[str]   = None  # 1D / 1W / 1M / 3M / 12M
    horizon_label:         Optional[str]   = None  # day / week / month / long
    horizon_days_effective:Optional[int]   = None  # resolved horizon in days

    # ── Recommendation ────────────────────────────────────────────────────────
    recommendation:         str = "HOLD"
    recommendation_reason:  str = "Insufficient data."

    # ── Forecasts ─────────────────────────────────────────────────────────────
    forecast_price_1m:   Optional[float] = None
    forecast_price_3m:   Optional[float] = None
    forecast_price_12m:  Optional[float] = None
    expected_roi_1m:     Optional[float] = None
    expected_roi_3m:     Optional[float] = None
    expected_roi_12m:    Optional[float] = None
    expected_return_1m:  Optional[float] = None
    expected_return_3m:  Optional[float] = None
    expected_return_12m: Optional[float] = None
    expected_price_1m:   Optional[float] = None
    expected_price_3m:   Optional[float] = None
    expected_price_12m:  Optional[float] = None

    # ── Metadata ──────────────────────────────────────────────────────────────
    scoring_updated_utc:    str       = field(default_factory=_utc_iso)
    scoring_updated_riyadh: str       = field(default_factory=_riyadh_iso)
    scoring_errors:         List[str] = field(default_factory=list)

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


ScoringWeights = ScoreWeights   # backward-compat alias

DEFAULT_WEIGHTS   = ScoreWeights()
DEFAULT_FORECASTS = ForecastParameters()


# =============================================================================
# Horizon detection & weight selection  (NEW v3.0.0)
# =============================================================================

def _detect_horizon(settings: Any, row: Mapping[str, Any]) -> Tuple[str, Optional[int]]:
    """
    Detect the investment horizon from settings or row data.
    Returns (horizon_label, horizon_days_int).

    Priority:
      1. settings.horizon_days / settings.invest_period_days
      2. row["horizon_days"] / row["invest_period_days"]
      3. Default: HORIZON_MONTH
    """
    horizon_days: Optional[float] = None

    if settings is not None:
        if isinstance(settings, Mapping):
            horizon_days = _safe_float(
                settings.get("horizon_days") or settings.get("invest_period_days")
            )
        else:
            horizon_days = _safe_float(
                getattr(settings, "horizon_days", None) or
                getattr(settings, "invest_period_days", None)
            )

    if horizon_days is None:
        horizon_days = _getf(row, "horizon_days", "invest_period_days")

    if horizon_days is None:
        return HORIZON_MONTH, None

    hd = int(abs(horizon_days))
    for cutoff, label in _HORIZON_DAYS_CUTOFFS:
        if hd <= cutoff:
            return label, hd
    return HORIZON_LONG, hd


def _weights_for_horizon(horizon: str, settings: Any = None) -> ScoreWeights:
    """
    Return a ScoreWeights instance tuned for the given horizon.
    If settings override individual weights, they are applied on top of the
    horizon-default set — but only if the override is for the matching horizon.

    Horizon weight philosophy:
      DAY:   Technical dominates — price action, volume, RSI zone are the only
             signals that matter for a 1-5 day trade.
      WEEK:  Technical still important but value starts to matter — oversold
             quality stocks with good technical setup are the ideal pick.
      MONTH: Balanced original TFB weights — unchanged from v2.4.0.
      LONG:  Fundamentals dominate — valuation + quality + growth compound over
             12+ month periods. Technical is noise at this horizon.
    """
    presets: Dict[str, ScoreWeights] = {
        HORIZON_DAY: ScoreWeights(
            w_technical=0.50, w_momentum=0.30, w_quality=0.10,
            w_valuation=0.00, w_growth=0.00, w_opportunity=0.10,
            risk_penalty_strength=0.60, confidence_penalty_strength=0.40,
        ),
        HORIZON_WEEK: ScoreWeights(
            w_technical=0.25, w_momentum=0.25, w_valuation=0.20,
            w_quality=0.20, w_growth=0.00, w_opportunity=0.10,
            risk_penalty_strength=0.55, confidence_penalty_strength=0.45,
        ),
        HORIZON_MONTH: ScoreWeights(
            w_technical=0.00, w_valuation=0.30, w_momentum=0.25,
            w_quality=0.20, w_growth=0.15, w_opportunity=0.10,
            risk_penalty_strength=0.55, confidence_penalty_strength=0.45,
        ),
        HORIZON_LONG: ScoreWeights(
            w_technical=0.00, w_valuation=0.35, w_quality=0.25,
            w_growth=0.20, w_momentum=0.15, w_opportunity=0.05,
            risk_penalty_strength=0.50, confidence_penalty_strength=0.50,
        ),
    }
    base = presets.get(horizon, presets[HORIZON_MONTH])
    if settings is None:
        return replace(base)

    # Allow settings to override specific weights (e.g. for custom criteria)
    def _try(name: str, current: float) -> float:
        try:
            v = settings.get(name) if isinstance(settings, Mapping) else getattr(settings, name, None)
            if v is None:
                return current
            f = float(v)
            return f if not _is_nan(f) else current
        except Exception:
            return current

    w = replace(base)
    w.risk_penalty_strength       = _clamp(_try("risk_penalty_strength", w.risk_penalty_strength), 0.0, 1.0)
    w.confidence_penalty_strength = _clamp(_try("confidence_penalty_strength", w.confidence_penalty_strength), 0.0, 1.0)
    return w


def _weights_from_settings(settings: Any) -> ScoreWeights:
    """
    Legacy weight loader — returns HORIZON_MONTH weights respecting any
    per-field overrides from settings. Called when horizon not specified.
    """
    w = replace(DEFAULT_WEIGHTS)
    if settings is None:
        return w

    def _try(name: str, default: float) -> float:
        try:
            v = settings.get(name) if isinstance(settings, Mapping) else getattr(settings, name, None)
            if v is None:
                return default
            f = float(v)
            return f if not _is_nan(f) else default
        except Exception:
            return default

    w.w_valuation  = _try("score_w_valuation",  w.w_valuation)
    w.w_momentum   = _try("score_w_momentum",   w.w_momentum)
    w.w_quality    = _try("score_w_quality",    w.w_quality)
    w.w_growth     = _try("score_w_growth",     w.w_growth)
    w.w_opportunity= _try("score_w_opportunity",w.w_opportunity)
    w.risk_penalty_strength       = _try("risk_penalty_strength",       w.risk_penalty_strength)
    w.confidence_penalty_strength = _try("confidence_penalty_strength", w.confidence_penalty_strength)

    total = w.w_valuation + w.w_momentum + w.w_quality + w.w_growth + w.w_opportunity
    if total > 0:
        w.w_valuation   /= total
        w.w_momentum    /= total
        w.w_quality     /= total
        w.w_growth      /= total
        w.w_opportunity /= total

    w.risk_penalty_strength       = _clamp(w.risk_penalty_strength, 0.0, 1.0)
    w.confidence_penalty_strength = _clamp(w.confidence_penalty_strength, 0.0, 1.0)
    return w


# =============================================================================
# Buckets
# =============================================================================

def _risk_bucket(score: Optional[float]) -> Optional[str]:
    if score is None:
        return None
    if score <= 35:
        return "Low"
    if score <= 65:
        return "Moderate"
    return "High"


def _confidence_bucket(conf01: Optional[float]) -> Optional[str]:
    if conf01 is None:
        return None
    if conf01 >= 0.75:
        return "High"
    if conf01 >= 0.50:
        return "Medium"
    return "Low"


# =============================================================================
# Horizon-derived labels  (NEW v3.0.0)
# =============================================================================

def _rsi_signal(rsi: Optional[float]) -> str:
    """
    Classify RSI into actionable text signal.
    Oversold (<30) = potential reversal upward.
    Overbought (>70) = potential reversal downward.
    """
    if rsi is None:
        return "N/A"
    if rsi < 30:
        return "Oversold"
    if rsi > 70:
        return "Overbought"
    return "Neutral"


def _invest_period_label(horizon: str, horizon_days: Optional[int] = None) -> str:
    """
    Convert horizon label or day count to user-facing period label.
    Returns: 1D / 1W / 1M / 3M / 12M
    """
    if horizon_days is not None:
        if horizon_days <= 1:   return "1D"
        if horizon_days <= 6:   return "1W"
        if horizon_days <= 30:  return "1M"
        if horizon_days <= 90:  return "3M"
        return "12M"
    return {
        HORIZON_DAY:   "1D",
        HORIZON_WEEK:  "1W",
        HORIZON_MONTH: "1M",
        HORIZON_LONG:  "12M",
    }.get(horizon, "1M")


def _short_term_signal(
    technical: Optional[float],
    momentum:  Optional[float],
    risk:      Optional[float],
    horizon:   str,
) -> str:
    """
    Generate a day / week horizon trading signal from technical + momentum.

    Day horizon thresholds (tight — only strong setups get BUY):
      STRONG_BUY: technical≥75 AND momentum≥70 AND risk≤50
      BUY:        technical≥60 AND momentum≥55 AND risk≤65
      SELL:       technical<38 OR momentum<30
      HOLD:       everything else

    Week horizon thresholds (slightly looser):
      BUY:        technical≥65 AND momentum≥60 AND risk≤60
      SELL:       technical<35 OR momentum<30
      HOLD:       everything else

    Month/Long: signal based on momentum alone (less reliable for these horizons).
    """
    t = technical if technical is not None else 50.0
    m = momentum  if momentum  is not None else 50.0
    r = risk      if risk      is not None else 50.0

    if horizon == HORIZON_DAY:
        if t >= 75 and m >= 70 and r <= 50:
            return "STRONG_BUY"
        if t >= 60 and m >= 55 and r <= 65:
            return "BUY"
        if t < 38 or m < 30:
            return "SELL"
        return "HOLD"

    if horizon == HORIZON_WEEK:
        if t >= 65 and m >= 60 and r <= 60:
            return "BUY"
        if t < 35 or m < 30:
            return "SELL"
        return "HOLD"

    # Month / Long — less reliable for ST signal, use gentle thresholds
    if m >= 70 and t >= 55:
        return "BUY"
    if m <= 30 or t <= 35:
        return "SELL"
    return "HOLD"


# =============================================================================
# Derived field helpers  (NEW v3.0.0)
# =============================================================================

def _derive_volume_ratio(row: Mapping[str, Any]) -> Optional[float]:
    """
    Compute volume_ratio = volume / avg_volume_10d.
    Returns existing value if already in row, otherwise derives it.
    """
    vr = _getf(row, "volume_ratio")
    if vr is not None and vr > 0:
        return _round(vr, 4)
    vol = _getf(row, "volume")
    avg = _getf(row, "avg_volume_10d")
    if avg is None:
        avg = _getf(row, "avg_volume_30d")
    if vol is None or avg is None or avg <= 0:
        return None
    return _round(vol / avg, 4)


def _derive_day_range_position(row: Mapping[str, Any]) -> Optional[float]:
    """
    Compute day_range_position = (price - day_low) / (day_high - day_low).
    Returns existing value if already in row.
    0 = price at bottom of today's range (buy signal).
    1 = price at top of today's range (caution).
    """
    drp = _getf(row, "day_range_position")
    if drp is not None:
        return _round(_clamp(drp, 0.0, 1.0), 4)
    price = _getf(row, "current_price", "price", "last_price")
    low   = _getf(row, "day_low")
    high  = _getf(row, "day_high")
    if price is None or low is None or high is None:
        return None
    rng = high - low
    if rng <= 0:
        return _round(0.5, 4)
    return _round(_clamp((price - low) / rng, 0.0, 1.0), 4)


def _derive_upside_pct(row: Mapping[str, Any]) -> Optional[float]:
    """
    Compute upside_pct = (intrinsic_value - current_price) / current_price.
    Positive = trading below intrinsic value (buy signal).
    Negative = trading above intrinsic value (caution).
    Returns existing value if already in row.
    """
    usp = _getf(row, "upside_pct")
    if usp is not None:
        return usp
    price = _getf(row, "current_price", "price", "last_price")
    intrinsic = _getf(row, "intrinsic_value", "fair_value")
    if price is None or price <= 0 or intrinsic is None or intrinsic <= 0:
        return None
    return _round((intrinsic - price) / price, 4)


# =============================================================================
# Technical score  (NEW v3.0.0)
# =============================================================================

def _volume_ratio_to_score(ratio: Optional[float]) -> Optional[float]:
    """
    Map volume_ratio (volume / avg_volume_10d) to a 0-1 score.
    High ratio = unusual activity = stronger signal.
    """
    if ratio is None or ratio < 0:
        return None
    if ratio >= 3.0:  return 1.00   # extreme surge
    if ratio >= 2.0:  return 0.90   # strong surge
    if ratio >= 1.5:  return 0.75   # moderate surge
    if ratio >= 1.0:  return 0.55   # normal activity
    if ratio >= 0.7:  return 0.40   # below average
    return 0.20                      # low volume = weak signal


def _rsi_to_zone_score(rsi: Optional[float]) -> Optional[float]:
    """
    Map RSI to a buy-signal strength score (0-1).
    Optimal buy zone: RSI 30-60 (slightly oversold to neutral).
    High RSI (>70) = overbought, low score.
    Very low RSI (<25) = deeply oversold, can be contrarian buy.
    """
    if rsi is None:
        return None
    if rsi <= 25:   return 0.95   # deeply oversold — strong contrarian buy
    if rsi <= 35:   return 0.88   # oversold
    if rsi <= 45:   return 0.78   # mildly oversold — good entry
    if rsi <= 55:   return 0.68   # neutral — acceptable
    if rsi <= 60:   return 0.58   # slightly elevated — hold
    if rsi <= 65:   return 0.45   # getting stretched
    if rsi <= 70:   return 0.30   # approaching overbought
    if rsi <= 75:   return 0.18   # overbought
    return 0.08                    # very overbought — avoid or short


def _day_range_to_score(drp: Optional[float]) -> Optional[float]:
    """
    Map day_range_position (0=bottom, 1=top) to buy-signal score.
    Near bottom of day's range = better entry point.
    """
    if drp is None:
        return None
    # Invert and apply a gentle curve: near bottom = high score
    return _clamp(1.0 - (drp ** 0.7), 0.0, 1.0)


def _technical_score(row: Mapping[str, Any]) -> Optional[float]:
    """
    Composite short-term technical signal 0-100.

    Formula (normalized weights):
      0.40 × RSI_zone_score         — trend exhaustion detector
      0.30 × volume_ratio_score     — institutional activity indicator
      0.30 × day_range_position_inv — entry quality within today's range

    A high technical_score (≥65) means: RSI is in a good buy zone,
    unusual volume is confirming the move, and price is near the lower end
    of today's range — all three aligning is a strong short-term setup.

    Sources:
      rsi_14 (provider/derived), volume_ratio (derived), day_range_position
      (derived or from day_high/day_low/current_price).
    """
    rsi      = _getf(row, "rsi_14", "rsi", "rsi14")
    vol_r    = _derive_volume_ratio(row)
    drp      = _derive_day_range_position(row)

    rsi_score = _rsi_to_zone_score(rsi)
    vol_score = _volume_ratio_to_score(vol_r)
    drp_score = _day_range_to_score(drp)

    parts: List[Tuple[float, float]] = []
    if rsi_score is not None:
        parts.append((0.40, rsi_score))
    if vol_score is not None:
        parts.append((0.30, vol_score))
    if drp_score is not None:
        parts.append((0.30, drp_score))

    if not parts:
        return None

    wsum = sum(w for w, _ in parts)
    score01 = sum(w * v for w, v in parts) / max(1e-9, wsum)
    return _round(100.0 * _clamp(score01, 0.0, 1.0), 2)


# =============================================================================
# Forecast helpers
# =============================================================================

def _forecast_params_from_settings(settings: Any) -> ForecastParameters:
    fp = replace(DEFAULT_FORECASTS)
    if settings is None:
        return fp

    def _try_fraction(name: str, current: float) -> float:
        try:
            v = settings.get(name) if isinstance(settings, Mapping) else getattr(settings, name, None)
            if v is None:
                return current
            f = float(v)
            if _is_nan(f):
                return current
            if abs(f) > 1.5:
                f /= 100.0
            return f
        except Exception:
            return current

    fp.min_roi_1m  = _try_fraction("min_roi_1m",  fp.min_roi_1m)
    fp.max_roi_1m  = _try_fraction("max_roi_1m",  fp.max_roi_1m)
    fp.min_roi_3m  = _try_fraction("min_roi_3m",  fp.min_roi_3m)
    fp.max_roi_3m  = _try_fraction("max_roi_3m",  fp.max_roi_3m)
    fp.min_roi_12m = _try_fraction("min_roi_12m", fp.min_roi_12m)
    fp.max_roi_12m = _try_fraction("max_roi_12m", fp.max_roi_12m)
    return fp


def _derive_forecast_patch(
    row: Mapping[str, Any],
    forecasts: ForecastParameters,
) -> Tuple[Dict[str, Any], List[str]]:
    patch: Dict[str, Any] = {}
    errors: List[str] = []

    price = _getf(row, "current_price", "price", "last_price", "last")
    fair  = _getf(row, "intrinsic_value", "fair_value", "target_price",
                  "target_mean_price", "forecast_price_12m",
                  "forecast_price_3m", "forecast_price_1m")

    roi1  = _as_fraction(_get(row, "expected_roi_1m",  "expected_return_1m"))
    roi3  = _as_roi_fraction(_get(row, "expected_roi_3m",  "expected_return_3m"))
    roi12 = _as_roi_fraction(_get(row, "expected_roi_12m", "expected_return_12m"))

    fp1   = _getf(row, "forecast_price_1m",  "expected_price_1m")
    fp3   = _getf(row, "forecast_price_3m",  "expected_price_3m")
    fp12  = _getf(row, "forecast_price_12m", "expected_price_12m")

    if price is not None and price > 0:
        if roi12 is None and fp12 is not None and fp12 > 0:
            roi12 = (fp12 / price) - 1.0
        if roi3  is None and fp3  is not None and fp3  > 0:
            roi3  = (fp3  / price) - 1.0
        if roi1  is None and fp1  is not None and fp1  > 0:
            roi1  = (fp1  / price) - 1.0

    if price is not None and price > 0 and roi12 is None and fair is not None and fair > 0:
        roi12 = (fair / price) - 1.0

    if roi12 is not None:
        roi12 = _clamp(roi12, forecasts.min_roi_12m, forecasts.max_roi_12m)
    if roi3  is None and roi12 is not None:
        roi3  = _clamp(roi12 * forecasts.ratio_3m_of_12m, forecasts.min_roi_3m, forecasts.max_roi_3m)
    if roi1  is None and roi12 is not None:
        roi1  = _clamp(roi12 * forecasts.ratio_1m_of_12m, forecasts.min_roi_1m, forecasts.max_roi_1m)

    if roi3  is not None:
        roi3  = _clamp(roi3,  forecasts.min_roi_3m,  forecasts.max_roi_3m)
    if roi1  is not None:
        roi1  = _clamp(roi1,  forecasts.min_roi_1m,  forecasts.max_roi_1m)

    if price is not None and price > 0:
        if fp12 is None and roi12 is not None:
            fp12 = price * (1.0 + roi12)
        if fp3  is None and roi3  is not None:
            fp3  = price * (1.0 + roi3)
        if fp1  is None and roi1  is not None:
            fp1  = price * (1.0 + roi1)
    elif fair is None:
        errors.append("price_unavailable_for_forecast")

    patch["forecast_price_1m"]  = _round(fp1,  4)
    patch["forecast_price_3m"]  = _round(fp3,  4)
    patch["forecast_price_12m"] = _round(fp12, 4)
    patch["expected_roi_1m"]    = _round(roi1,  6)
    patch["expected_roi_3m"]    = _round(roi3,  6)
    patch["expected_roi_12m"]   = _round(roi12, 6)

    # Aliases
    patch["expected_return_1m"]  = patch["expected_roi_1m"]
    patch["expected_return_3m"]  = patch["expected_roi_3m"]
    patch["expected_return_12m"] = patch["expected_roi_12m"]
    patch["expected_price_1m"]   = patch["forecast_price_1m"]
    patch["expected_price_3m"]   = patch["forecast_price_3m"]
    patch["expected_price_12m"]  = patch["forecast_price_12m"]
    return patch, errors


# =============================================================================
# Component scoring
# =============================================================================

def _data_quality_factor(row: Mapping[str, Any]) -> float:
    dq = str(_get(row, "data_quality") or "").strip().upper()
    return {
        "EXCELLENT": 0.95,
        "HIGH":      0.85,
        "GOOD":      0.80,
        "MEDIUM":    0.68,
        "FAIR":      0.60,
        "POOR":      0.40,
        "STALE":     0.45,
        "MISSING":   0.20,
        "ERROR":     0.15,
    }.get(dq, 0.60)


def _completeness_factor(row: Mapping[str, Any]) -> float:
    core_fields = [
        "symbol", "name", "currency", "exchange", "current_price", "previous_close",
        "day_high", "day_low", "week_52_high", "week_52_low", "volume", "market_cap",
        "pe_ttm", "pb_ratio", "ps_ratio", "dividend_yield", "rsi_14",
        "volatility_30d", "volatility_90d", "expected_roi_3m", "forecast_price_3m",
        "forecast_confidence",
    ]
    present = sum(
        1 for k in core_fields
        if row.get(k) not in (None, "", [], {})
    )
    return present / max(1, len(core_fields))


def _quality_score(row: Mapping[str, Any]) -> Optional[float]:
    """
    v3.0.0 enhancement: blends real financial quality metrics with data quality.

    Financial quality metrics (when available — sourced from yahoo_fundamentals / EODHD):
      ROE   (0.30 weight): >15% good, >25% excellent
      ROA   (0.25 weight): >5%  good, >15% excellent
      OpMgn (0.25 weight): >15% good, >30% excellent
      NtMgn (0.15 weight): >10% good, >20% excellent
      D/E   (0.05 weight): <0.5 good, >2.0 poor

    Data quality proxy (used when no financial data available):
      0.55 × data_quality_flag + 0.45 × completeness_factor

    Blend: 0.40 × financial_quality + 0.60 × data_quality_proxy
    Falls back to pure data quality proxy when no financial metrics are present.
    """
    dq   = _data_quality_factor(row)
    comp = _completeness_factor(row)
    data_quality_proxy = _clamp(0.55 * dq + 0.45 * comp, 0.0, 1.0)

    # Financial quality metrics
    roe       = _as_fraction(_get(row, "roe", "return_on_equity", "returnOnEquity"))
    roa       = _as_fraction(_get(row, "roa", "return_on_assets", "returnOnAssets"))
    op_margin = _as_fraction(_get(row, "operating_margin", "operatingMarginTTM"))
    net_margin= _as_fraction(_get(row, "profit_margin", "net_margin", "profitMargins"))
    de        = _getf(row, "debt_to_equity", "debtToEquity")

    fin_parts: List[Tuple[float, float]] = []

    if roe is not None:
        # ROE: <5% = weak (0), 15% = good (0.5), 25% = excellent (0.8), >35% = top (1.0)
        roe_score = _clamp((roe - 0.05) / 0.30, 0.0, 1.0)
        fin_parts.append((0.30, roe_score))

    if roa is not None:
        # ROA: <2% = weak (0), 5% = good (0.4), 12% = excellent (0.8), >18% = top (1.0)
        roa_score = _clamp((roa - 0.02) / 0.16, 0.0, 1.0)
        fin_parts.append((0.25, roa_score))

    if op_margin is not None:
        # OpMargin: <5% = weak (0), 15% = good (0.4), 30% = excellent (0.8), >40% = top (1.0)
        op_score = _clamp((op_margin - 0.05) / 0.35, 0.0, 1.0)
        fin_parts.append((0.25, op_score))

    if net_margin is not None:
        # NetMargin: <2% = weak (0), 10% = good (0.4), 20% = excellent (0.8), >30% = top (1.0)
        nm_score = _clamp((net_margin - 0.02) / 0.28, 0.0, 1.0)
        fin_parts.append((0.15, nm_score))

    if de is not None and de >= 0:
        # D/E: 0 = best (1.0), 0.5 = good (0.8), 1.5 = caution (0.4), >2.5 = poor (0)
        de_score = _clamp(1.0 - (de / 2.5), 0.0, 1.0)
        fin_parts.append((0.05, de_score))

    if fin_parts:
        wsum = sum(w for w, _ in fin_parts)
        financial_quality = sum(w * v for w, v in fin_parts) / max(1e-9, wsum)
        combined = 0.40 * financial_quality + 0.60 * data_quality_proxy
    else:
        # No financial metrics — fall back to pure data quality proxy
        combined = data_quality_proxy

    return _round(100.0 * _clamp(combined, 0.0, 1.0), 2)


def _confidence_score(row: Mapping[str, Any]) -> Tuple[Optional[float], Optional[float]]:
    fc = _safe_float(_get(row, "forecast_confidence", "ai_confidence", "confidence_score", "confidence"))
    if fc is not None:
        fc01 = (fc / 100.0) if fc > 1.5 else fc
        fc01 = _clamp(fc01, 0.0, 1.0)
        return _round(fc01 * 100.0, 2), _round(fc01, 4)

    dq    = _data_quality_factor(row)
    comp  = _completeness_factor(row)
    provs = row.get("data_sources") or row.get("providers") or []
    try:
        pcount = len(provs) if isinstance(provs, list) else 0
    except Exception:
        pcount = 0
    prov_factor = _clamp(pcount / 3.0, 0.0, 1.0)
    conf01 = _clamp(0.55 * dq + 0.35 * comp + 0.10 * prov_factor, 0.0, 1.0)
    return _round(conf01 * 100.0, 2), _round(conf01, 4)


def _valuation_score(row: Mapping[str, Any]) -> Optional[float]:
    price = _getf(row, "current_price", "price", "last_price", "last")
    if price is None or price <= 0:
        return None

    fair   = _getf(row, "intrinsic_value", "fair_value", "target_price",
                   "forecast_price_3m", "forecast_price_12m", "forecast_price_1m")
    upside = ((fair / price) - 1.0) if fair is not None and fair > 0 else None

    roi3  = _as_roi_fraction(_get(row, "expected_roi_3m",  "expected_return_3m"))
    roi12 = _as_roi_fraction(_get(row, "expected_roi_12m", "expected_return_12m"))

    def _roi_norm(frac: Optional[float], cap: float) -> Optional[float]:
        if frac is None:
            return None
        return _clamp((frac + cap) / (2 * cap), 0.0, 1.0)

    pe  = _getf(row, "pe_ttm", "pe_ratio")
    pb  = _getf(row, "pb_ratio", "pb", "price_to_book")
    ps  = _getf(row, "ps_ratio", "ps", "price_to_sales")
    peg = _getf(row, "peg_ratio", "peg")
    ev  = _getf(row, "ev_ebitda", "ev_to_ebitda")

    def _low_is_good(x: Optional[float], lo: float, hi: float) -> Optional[float]:
        if x is None or x <= 0:
            return None
        if x <= lo:  return 1.0
        if x >= hi:  return 0.0
        return 1.0 - ((x - lo) / (hi - lo))

    anchors = [a for a in (
        _low_is_good(pe,  8.0,  35.0),
        _low_is_good(pb,  0.8,   6.0),
        _low_is_good(ps,  1.0,  10.0),
        _low_is_good(peg, 0.8,   4.0),
        _low_is_good(ev,  6.0,  25.0),
    ) if a is not None]
    anchor_avg = (sum(anchors) / len(anchors)) if anchors else None

    parts: List[Tuple[float, float]] = []
    upside_n = _roi_norm(upside, 0.50)
    roi3_n   = _roi_norm(roi3,   0.35)
    roi12_n  = _roi_norm(roi12,  0.80)

    if upside_n  is not None: parts.append((0.40, upside_n))
    if roi3_n    is not None: parts.append((0.30, roi3_n))
    if roi12_n   is not None: parts.append((0.20, roi12_n))
    if anchor_avg is not None: parts.append((0.10, anchor_avg))

    if not parts:
        return None

    wsum = sum(w for w, _ in parts)
    return _round(100.0 * _clamp(sum(w * v for w, v in parts) / max(1e-9, wsum), 0.0, 1.0), 2)


def _growth_score(row: Mapping[str, Any]) -> Optional[float]:
    g = _as_fraction(_get(row, "revenue_growth_yoy", "revenue_growth", "growth_yoy"))
    if g is None:
        return None
    return _round(_clamp(((g + 0.30) / 0.60) * 100.0, 0.0, 100.0), 2)


def _momentum_score(row: Mapping[str, Any]) -> Optional[float]:
    """
    v3.0.0 enhancement: adds 5D price change and volume_ratio as inputs.

    Weights (normalized):
      0.30 — RSI zone score (trend exhaustion detector — most reliable)
      0.25 — 1D percent change (immediate price action)
      0.20 — 5D price change (short-term trend confirmation)
      0.15 — 52-week position (medium-term trend context)
      0.10 — volume ratio (institutional activity confirmation)
    """
    pct    = _as_roi_fraction(_get(row, "percent_change",   "change_pct", "change_percent"))
    rsi    = _getf(row, "rsi_14", "rsi", "rsi14")
    pos    = _as_fraction(_get(row, "week_52_position_pct", "position_52w_pct", "week52_position_pct"))
    pct_5d = _as_roi_fraction(_get(row, "price_change_5d"))
    vol_r  = _derive_volume_ratio(row)

    parts: List[Tuple[float, float]] = []

    if rsi is not None:
        # Bell curve centered at RSI 55 — optimal momentum zone
        x = (rsi - 55.0) / 12.0
        parts.append((0.30, _clamp(math.exp(-(x * x)), 0.0, 1.0)))

    if pct is not None:
        # 1D change: ±10% maps to 0-1
        parts.append((0.25, _clamp((pct + 0.10) / 0.20, 0.0, 1.0)))

    if pct_5d is not None:
        # 5D change: ±8% maps to 0-1 (slightly tighter than 1D)
        parts.append((0.20, _clamp((pct_5d + 0.08) / 0.16, 0.0, 1.0)))

    if pos is not None:
        # 52W position: 1.0 = near 52W high (strong uptrend)
        parts.append((0.15, _clamp(pos, 0.0, 1.0)))

    if vol_r is not None:
        # Volume ratio: >2.0 = maximum score
        parts.append((0.10, _clamp((vol_r - 0.5) / 1.5, 0.0, 1.0)))

    if not parts:
        return None

    wsum = sum(w for w, _ in parts)
    return _round(100.0 * _clamp(sum(w * v for w, v in parts) / max(1e-9, wsum), 0.0, 1.0), 2)


def _risk_score(row: Mapping[str, Any]) -> Optional[float]:
    """Unchanged from v2.4.0 — higher score = higher risk."""
    vol90  = _as_fraction(_get(row, "volatility_90d"))
    dd1y   = _as_fraction(_get(row, "max_drawdown_1y"))
    var1d  = _as_fraction(_get(row, "var_95_1d"))
    sharpe = _getf(row, "sharpe_1y")

    def _scale(x: Optional[float], lo: float, hi: float) -> Optional[float]:
        if x is None:
            return None
        return _clamp((x - lo) / (hi - lo), 0.0, 1.0) if hi > lo else None

    parts: List[Tuple[float, float]] = []
    if vol90  is not None: parts.append((0.40, _scale(vol90, 0.12, 0.70)       or 0.0))
    if dd1y   is not None: parts.append((0.35, _scale(abs(dd1y), 0.05, 0.55)   or 0.0))
    if var1d  is not None: parts.append((0.20, _scale(var1d, 0.01, 0.08)       or 0.0))
    if sharpe is not None: parts.append((0.05, _clamp(1.0 - _clamp((sharpe + 0.5) / 2.5, 0.0, 1.0), 0.0, 1.0)))

    if not parts:
        vol  = _as_fraction(_get(row, "volatility_30d", "vol_30d"))
        beta = _getf(row, "beta_5y", "beta")
        dd   = _as_fraction(_get(row, "max_drawdown_30d", "drawdown_30d"))
        if vol  is not None: parts.append((0.50, _scale(vol,      0.10, 0.60) or 0.0))
        if beta is not None: parts.append((0.30, _scale(beta,     0.60, 2.00) or 0.0))
        if dd   is not None: parts.append((0.20, _scale(abs(dd),  0.00, 0.50) or 0.0))

    if not parts:
        return None

    wsum = sum(w for w, _ in parts)
    return _round(100.0 * _clamp(sum(w * v for w, v in parts) / max(1e-9, wsum), 0.0, 1.0), 2)


def _opportunity_score(
    row: Mapping[str, Any],
    valuation: Optional[float],
    momentum:  Optional[float],
) -> Optional[float]:
    """Unchanged from v2.4.0."""
    roi1  = _as_fraction(_get(row, "expected_roi_1m",  "expected_return_1m"))
    roi3  = _as_roi_fraction(_get(row, "expected_roi_3m",  "expected_return_3m"))
    roi12 = _as_roi_fraction(_get(row, "expected_roi_12m", "expected_return_12m"))

    def _roi_norm(frac: Optional[float], cap: float) -> Optional[float]:
        if frac is None:
            return None
        return _clamp((frac + cap) / (2 * cap), 0.0, 1.0)

    parts: List[Tuple[float, float]] = []
    r3  = _roi_norm(roi3,  0.35)
    r12 = _roi_norm(roi12, 0.80)
    r1  = _roi_norm(roi1,  0.25)

    if r3  is not None: parts.append((0.55, r3))
    if r12 is not None: parts.append((0.30, r12))
    if r1  is not None: parts.append((0.15, r1))

    if parts:
        wsum = sum(w for w, _ in parts)
        return _round(100.0 * _clamp(sum(w * v for w, v in parts) / max(1e-9, wsum), 0.0, 1.0), 2)

    if valuation is None and momentum is None:
        return None

    v = (valuation or 50.0) / 100.0
    m = (momentum  or 50.0) / 100.0
    return _round(100.0 * _clamp(0.60 * v + 0.40 * m, 0.0, 1.0), 2)


def _recommendation(
    overall:    Optional[float],
    risk:       Optional[float],
    confidence100: Optional[float],
    roi3:       Optional[float],
    *,
    horizon:    str = HORIZON_MONTH,
    technical:  Optional[float] = None,
    momentum:   Optional[float] = None,
    roi1:       Optional[float] = None,
    roi12:      Optional[float] = None,
) -> Tuple[str, str]:
    """
    v3.0.0: horizon-aware recommendation.

    DAY horizon   — primary signal: technical_score (RSI + volume + range)
    WEEK horizon  — primary signal: technical_score + momentum
    MONTH horizon — primary signal: roi_3m + overall_score (original v2.4.0)
    LONG horizon  — primary signal: roi_12m + overall_score
    """
    if overall is None:
        return "HOLD", "Insufficient data to score reliably."

    r   = risk         if risk         is not None else 50.0
    c   = confidence100 if confidence100 is not None else 55.0
    t   = technical    if technical    is not None else 50.0
    m   = momentum     if momentum     is not None else 50.0

    # Low confidence → hold regardless of horizon
    if c < 40:
        return "HOLD", f"Low AI confidence ({_round(c, 1)}%) — insufficient signal quality."

    # ── DAY horizon ───────────────────────────────────────────────────────────
    if horizon == HORIZON_DAY:
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

    # ── WEEK horizon ──────────────────────────────────────────────────────────
    if horizon == HORIZON_WEEK:
        roi_1m = roi1 if roi1 is not None else 0.0
        if t >= 72 and m >= 65 and r <= 50:
            return "STRONG_BUY", (
                f"Strong week setup: Tech={_round(t, 1)}, "
                f"Momentum={_round(m, 1)}, Risk=Low. 1M ROI≈{_round(roi_1m*100, 1)}%."
            )
        if t >= 58 and m >= 50 and r <= 65 and roi_1m >= 0.015:
            return "BUY", (
                f"Week momentum setup: Tech={_round(t, 1)}, "
                f"1M ROI≈{_round(roi_1m*100, 1)}%, Risk={_round(r, 1)}."
            )
        if t < 35 or m < 30:
            return "SELL", (
                f"Weak week setup — technical breakdown: Tech={_round(t, 1)}, "
                f"Momentum={_round(m, 1)}."
            )
        return "HOLD", f"Week setup not compelling — wait for better entry (Tech={_round(t, 1)})."

    # ── LONG horizon ──────────────────────────────────────────────────────────
    if horizon == HORIZON_LONG:
        roi_12m = roi12 if roi12 is not None else 0.0
        if r >= 75 and overall < 75:
            return "REDUCE", f"High long-term risk ({_round(r, 1)}) with moderate score ({_round(overall, 1)})."
        if roi_12m >= 0.25 and c >= 70 and r <= 45 and overall >= 78:
            return "STRONG_BUY", f"Strong 12M expected return ({_round(roi_12m*100, 1)}%) with low risk."
        if roi_12m >= 0.12 and c >= 60 and r <= 60 and overall >= 68:
            return "BUY", f"Positive 12M outlook ({_round(roi_12m*100, 1)}%) with acceptable risk."
        if overall >= 75 and r <= 50:
            return "BUY", f"Strong long-term score ({_round(overall, 1)}) with controlled risk."
        if overall >= 65:
            return "HOLD", f"Moderate long-term profile (score={_round(overall, 1)})."
        if overall >= 50:
            return "REDUCE", f"Weak long-term outlook (score={_round(overall, 1)})."
        return "SELL", f"Poor long-term fundamentals (score={_round(overall, 1)})."

    # ── MONTH horizon (default — original v2.4.0 logic) ──────────────────────
    roi_3m = roi3 if roi3 is not None else 0.0
    if r >= 75 and overall < 75:
        return "REDUCE", f"High risk ({_round(r, 1)}) with moderate score ({_round(overall, 1)})."
    if roi3 is not None and roi_3m >= 0.25 and c >= 70 and r <= 45 and overall >= 78:
        return "STRONG_BUY", f"High expected 3M ROI (~{_round(roi_3m*100, 1)}%) with strong confidence and low risk."
    if roi3 is not None and roi_3m >= 0.12 and c >= 60 and r <= 55 and overall >= 70:
        return "BUY", f"Positive 3M expected ROI (~{_round(roi_3m*100, 1)}%) with acceptable risk/confidence."
    if overall >= 82 and r <= 55:
        return "BUY", f"Strong overall score ({_round(overall, 1)}) with controlled risk ({_round(r, 1)})."
    if overall >= 65:
        return "HOLD", f"Moderate overall score ({_round(overall, 1)})."
    if overall >= 50:
        return "REDUCE", f"Weak overall score ({_round(overall, 1)})."
    return "SELL", f"Very weak overall score ({_round(overall, 1)})."


# =============================================================================
# Public API
# =============================================================================

def compute_scores(row: Dict[str, Any], *, settings: Any = None) -> Dict[str, Any]:
    """
    Main entrypoint. Returns a patch dict to merge into the row.

    v3.0.0 additions to the returned dict:
      technical_score, rsi_signal, short_term_signal, day_range_position,
      volume_ratio, upside_pct, invest_period_label, horizon_label,
      horizon_days_effective

    All v2.4.0 fields are preserved unchanged.
    """
    source = dict(row or {})
    scoring_errors: List[str] = []

    # ── Forecasts ──────────────────────────────────────────────────────────────
    forecasts = _forecast_params_from_settings(settings)
    forecast_patch, forecast_errors = _derive_forecast_patch(source, forecasts)
    scoring_errors.extend(forecast_errors)

    working = dict(source)
    working.update({k: v for k, v in forecast_patch.items() if v is not None})

    # ── Horizon detection ─────────────────────────────────────────────────────
    horizon, hdays = _detect_horizon(settings, working)
    weights = _weights_for_horizon(horizon, settings)

    # ── Component scores ──────────────────────────────────────────────────────
    valuation  = _valuation_score(working)
    momentum   = _momentum_score(working)
    quality    = _quality_score(working)
    growth     = _growth_score(working)
    confidence100, conf01 = _confidence_score(working)
    risk       = _risk_score(working)
    opportunity= _opportunity_score(working, valuation, momentum)
    value_score= valuation

    # ── NEW v3.0.0: technical + derived signals ───────────────────────────────
    tech_score = _technical_score(working)
    vol_ratio  = _derive_volume_ratio(working)
    drp        = _derive_day_range_position(working)
    usp        = _derive_upside_pct(working)
    rsi_val    = _getf(working, "rsi_14", "rsi", "rsi14")
    rsi_sig    = _rsi_signal(rsi_val)

    # ── Weighted overall score ─────────────────────────────────────────────────
    base_parts: List[Tuple[float, float]] = []
    if weights.w_technical  > 0 and tech_score  is not None:
        base_parts.append((weights.w_technical,  tech_score  / 100.0))
    if weights.w_valuation  > 0 and valuation   is not None:
        base_parts.append((weights.w_valuation,  valuation   / 100.0))
    if weights.w_momentum   > 0 and momentum    is not None:
        base_parts.append((weights.w_momentum,   momentum    / 100.0))
    if weights.w_quality    > 0 and quality     is not None:
        base_parts.append((weights.w_quality,    quality     / 100.0))
    if weights.w_growth     > 0 and growth      is not None:
        base_parts.append((weights.w_growth,     growth      / 100.0))
    if weights.w_opportunity > 0 and opportunity is not None:
        base_parts.append((weights.w_opportunity, opportunity / 100.0))

    overall: Optional[float] = None
    overall_raw: Optional[float] = None
    penalty_factor: Optional[float] = None

    if base_parts:
        wsum   = sum(x[0] for x in base_parts)
        base01 = sum(w * v for w, v in base_parts) / max(1e-9, wsum)
        overall_raw = _round(100.0 * _clamp(base01, 0.0, 1.0), 2)

        risk01       = (risk   / 100.0) if risk   is not None else 0.50
        conf01_used  = conf01           if conf01 is not None else 0.55

        risk_pen = _clamp(1.0 - weights.risk_penalty_strength       * (risk01        * 0.70), 0.0, 1.0)
        conf_pen = _clamp(1.0 - weights.confidence_penalty_strength * ((1.0 - conf01_used) * 0.80), 0.0, 1.0)
        penalty_factor = _round(risk_pen * conf_pen, 4)

        base01  *= (risk_pen * conf_pen)
        overall  = _round(100.0 * _clamp(base01, 0.0, 1.0), 2)
    else:
        overall       = 50.0
        overall_raw   = 50.0
        penalty_factor= 1.0
        scoring_errors.append("insufficient_scoring_inputs")

    rb = _risk_bucket(risk)
    cb = _confidence_bucket(conf01)

    roi3  = _as_fraction(working.get("expected_roi_3m"))
    roi1  = _as_fraction(working.get("expected_roi_1m"))
    roi12 = _as_fraction(working.get("expected_roi_12m"))

    rec, reason = _recommendation(
        overall, risk, confidence100, roi3,
        horizon=horizon, technical=tech_score, momentum=momentum,
        roi1=roi1, roi12=roi12,
    )

    st_signal   = _short_term_signal(tech_score, momentum, risk, horizon)
    period_label= _invest_period_label(horizon, hdays)

    scores = AssetScores(
        # Component scores
        valuation_score    = valuation,
        momentum_score     = momentum,
        quality_score      = quality,
        growth_score       = growth,
        value_score        = value_score,
        opportunity_score  = opportunity,
        confidence_score   = confidence100,
        forecast_confidence= conf01,
        confidence_bucket  = cb,
        risk_score         = risk,
        risk_bucket        = rb,
        overall_score      = overall,
        overall_score_raw  = overall_raw,
        overall_penalty_factor = penalty_factor,
        # New v3.0.0 signals
        technical_score       = tech_score,
        rsi_signal            = rsi_sig,
        short_term_signal     = st_signal,
        day_range_position    = drp,
        volume_ratio          = vol_ratio,
        upside_pct            = usp,
        invest_period_label   = period_label,
        horizon_label         = horizon,
        horizon_days_effective= hdays,
        # Recommendation
        recommendation        = normalize_recommendation_code(rec),
        recommendation_reason = reason,
        # Forecasts
        forecast_price_1m    = forecast_patch.get("forecast_price_1m"),
        forecast_price_3m    = forecast_patch.get("forecast_price_3m"),
        forecast_price_12m   = forecast_patch.get("forecast_price_12m"),
        expected_roi_1m      = forecast_patch.get("expected_roi_1m"),
        expected_roi_3m      = forecast_patch.get("expected_roi_3m"),
        expected_roi_12m     = forecast_patch.get("expected_roi_12m"),
        expected_return_1m   = forecast_patch.get("expected_return_1m"),
        expected_return_3m   = forecast_patch.get("expected_return_3m"),
        expected_return_12m  = forecast_patch.get("expected_return_12m"),
        expected_price_1m    = forecast_patch.get("expected_price_1m"),
        expected_price_3m    = forecast_patch.get("expected_price_3m"),
        expected_price_12m   = forecast_patch.get("expected_price_12m"),
        scoring_errors       = scoring_errors,
    )
    return scores.to_dict()


def score_row(row: Dict[str, Any], *, settings: Any = None) -> Dict[str, Any]:
    return compute_scores(row, settings=settings)


def score_quote(row: Dict[str, Any], *, settings: Any = None) -> Dict[str, Any]:
    return compute_scores(row, settings=settings)


def enrich_with_scores(row: Dict[str, Any], *, settings: Any = None, in_place: bool = False) -> Dict[str, Any]:
    target = row if in_place else dict(row or {})
    patch  = compute_scores(target, settings=settings)
    target.update(patch)
    return target


class ScoringEngine:
    """Lightweight wrapper supporting object-style callers."""
    version = SCORING_VERSION

    def __init__(
        self,
        *,
        settings:  Any                       = None,
        weights:   Optional[ScoreWeights]    = None,
        forecasts: Optional[ForecastParameters] = None,
    ):
        self.settings  = settings
        self.weights   = weights  or replace(DEFAULT_WEIGHTS)
        self.forecasts = forecasts or replace(DEFAULT_FORECASTS)

    def compute_scores(self, row: Dict[str, Any]) -> Dict[str, Any]:
        return compute_scores(row, settings=self.settings)

    def enrich_with_scores(self, row: Dict[str, Any], *, in_place: bool = False) -> Dict[str, Any]:
        return enrich_with_scores(row, settings=self.settings, in_place=in_place)


# =============================================================================
# Ranking helpers  (unchanged from v2.4.0)
# =============================================================================

def _rank_sort_tuple(
    row: Dict[str, Any],
    *,
    key_overall: str = "overall_score",
) -> Tuple[float, float, float, float, float, str]:
    overall = _norm_score_0_100(row.get(key_overall))
    opp     = _norm_score_0_100(row.get("opportunity_score"))
    conf    = _norm_score_0_100(row.get("confidence_score"))
    risk    = _norm_score_0_100(row.get("risk_score"))
    roi3    = _as_fraction(row.get("expected_roi_3m"))
    return (
        overall if overall is not None else -1e9,
        opp     if opp     is not None else -1e9,
        conf    if conf    is not None else -1e9,
        -(risk  if risk    is not None else 1e9),
        roi3    if roi3    is not None else -1e9,
        _safe_str(row.get("symbol"), "~"),
    )


def assign_rank_overall(
    rows:        Sequence[Dict[str, Any]],
    *,
    key_overall: str  = "overall_score",
    inplace:     bool = True,
    rank_key:    str  = "rank_overall",
) -> List[Dict[str, Any]]:
    target  = list(rows) if inplace else [dict(r or {}) for r in rows]
    indexed = list(enumerate(target))
    indexed.sort(key=lambda item: _rank_sort_tuple(item[1], key_overall=key_overall), reverse=True)
    for rank, (_, row) in enumerate(indexed, start=1):
        row[rank_key] = rank
    return target


def rank_rows_by_overall(
    rows:        List[Dict[str, Any]],
    *,
    key_overall: str = "overall_score",
) -> List[Dict[str, Any]]:
    return assign_rank_overall(rows, key_overall=key_overall, inplace=True, rank_key="rank_overall")


def score_and_rank_rows(
    rows:        Sequence[Dict[str, Any]],
    *,
    settings:    Any  = None,
    key_overall: str  = "overall_score",
    inplace:     bool = False,
) -> List[Dict[str, Any]]:
    prepared = list(rows) if inplace else [dict(r or {}) for r in rows]
    for row in prepared:
        try:
            row.update(compute_scores(row, settings=settings))
        except Exception:
            pass
    assign_rank_overall(prepared, key_overall=key_overall, inplace=True, rank_key="rank_overall")
    return prepared


__all__ = [
    # Version
    "__version__",
    "SCORING_VERSION",
    # Horizon constants
    "HORIZON_DAY",
    "HORIZON_WEEK",
    "HORIZON_MONTH",
    "HORIZON_LONG",
    # Core API
    "compute_scores",
    "score_row",
    "score_quote",
    "enrich_with_scores",
    "rank_rows_by_overall",
    "assign_rank_overall",
    "score_and_rank_rows",
    # Types
    "AssetScores",
    "ScoreWeights",
    "ScoringWeights",           # backward-compat alias
    "ForecastParameters",
    "ScoringEngine",
    # Defaults
    "DEFAULT_WEIGHTS",
    "DEFAULT_FORECASTS",
    # Recommendation helpers
    "CANONICAL_RECOMMENDATION_CODES",
    "RECOMMENDATION_LABEL_MAP",
    "normalize_recommendation_code",
    "normalize_recommendation_label",
    # New v3.0.0 public helpers
    "_detect_horizon",
    "_weights_for_horizon",
    "_technical_score",
    "_rsi_signal",
    "_short_term_signal",
    "_derive_upside_pct",
    "_derive_volume_ratio",
    "_derive_day_range_position",
    "_invest_period_label",
]
