#!/usr/bin/env python3
# core/scoring.py
"""
================================================================================
Scoring Module — v5.5.0
FULL REVISED VERSION / OPS-TUNABLE
================================================================================
Purpose
-------
Canonical scoring and recommendation source for Tadawul Fast Bridge.

Key fixes versus v5.3.0
-----------------------
1. Bucket consistency: risk_bucket(), confidence_bucket(), derive_risk_view(),
   and derive_confidence_view-style outputs use one canonical threshold policy.
2. Actual canonical bucket integration: when core.buckets exists, wrappers call it;
   when unavailable, local fallback uses the same intended thresholds.
3. Horizon-aware recommendation: long horizon uses 12M ROI/upside more heavily;
   short horizon uses technical/momentum/1M ROI.
4. Forecast clearing: forecast_patch is merged including None values so invalid
   forecasts do not leave stale ROI/price values in the working row.
5. Valuation fallback: ratio-based valuation can produce a score even when
   forecast/fair-value inputs are missing.
6. Provider sanity checks: dividend yield, debt/equity, percentage-like fields,
   and suspicious currency-sensitive values are normalized/flagged defensively.
7. Conservative but usable recommendation ladder: final enum remains closed
   to STRONG_BUY/BUY/HOLD/REDUCE/SELL/STRONG_SELL, while detail/position hint
   can surface WATCH / ACCUMULATE CANDIDATE wording.
8. Ranking remains a full-dataset operation; do not rank partial refresh batches
   unless the caller intentionally wants local batch ranks.



v5.4.1 hotfix over v5.4.0
---------------------------
1. Rebuilds recommendation_detail after the batch insights pass every time, so
   Recommendation Detail cannot become stale when recommendation_reason changes.
2. Makes _active_roi_for_horizon labels truthful on fallback paths: DAY/WEEK/MONTH
   now report whether the active ROI came from 1M, 3M, 12M, UPSIDE, or fallback.
3. Prevents ScoringEngine instances from sharing mutable module-default weight and
   forecast objects by copying DEFAULT_SCORING_WEIGHTS / DEFAULT_FORECAST_PARAMETERS.
4. Restores ScoringConfig.risk_moderate_threshold as a compatibility property that
   aliases risk_high_threshold.
5. Tightens the 12M unit-mismatch guard to avoid clearing legitimate severe
   negative forecasts inside the configured min_roi_12m range.
6. Restores mid-text legacy recommendation label normalization in reasons, but
   uses a narrow safe alias list to avoid corrupting normal prose.

v5.4.2 hotfix over v5.4.1
---------------------------
1. Narrows mid-text recommendation-label substitution to finance-jargon terms only.
   This prevents normal prose such as "Watch / accumulate candidate" from being
   rewritten into misleading text like "HOLD / BUY candidate".
2. Keeps full standalone-label normalization unchanged for normalize_recommendation_code()
   and keeps trailing-arrow normalization for legacy reason endings.

v5.5.0 ops enhancements over v5.4.2
-----------------------------------
1. Bucket thresholds are now env-tunable so deploy-level reconfiguration is
   possible without forking the file. Supported env vars:
     SCORING_RISK_LOW                 (default 35.0,  range 0..100)
     SCORING_RISK_HIGH                (default 70.0,  range 0..100)
     SCORING_CONFIDENCE_MODERATE      (default 0.45,  range 0..1)
     SCORING_CONFIDENCE_HIGH          (default 0.75,  range 0..1)
   Invalid combinations (low >= high, out of range) are rejected at import time
   with a WARN log and the default pair is kept. derive_risk_view/risk_bucket/
   confidence_bucket/ScoringConfig all use these resolved values transparently.
2. scoring_errors are deduplicated at the end of compute_scores while preserving
   first-seen order, so a downstream consumer cannot see the same provider tag
   listed twice in the same row.
3. opportunity_source now distinguishes which side of the val/mom fallback was
   missing: "valuation_only_fallback", "momentum_only_fallback", and
   "both_present_fallback" replace the prior single "valuation_momentum_fallback"
   tag. "roi_based" and "insufficient" are unchanged.
4. New public diagnostic helpers:
     get_canonical_thresholds()   -> resolved risk/confidence threshold pairs
                                     plus the env-override flag for each
     get_canonical_state()        -> full ops snapshot: version, source tag,
                                     buckets-canonical availability, thresholds,
                                     and the active ScoringConfig
   These are intended for audit/diagnostic tooling so it can confirm what
   thresholds the running process is actually using without reading env or
   re-importing private symbols.

Public API compatibility
------------------------
Preserves the main names used by data_engine_v2 and routes:
compute_scores, score_row, score_quote, enrich_with_scores, score_and_rank_rows,
apply_canonical_recommendation, derive_canonical_recommendation,
_compute private wrappers, bucket wrappers, classes, constants, and __all__.
================================================================================
"""

from __future__ import annotations

import logging
import math
import os
import re
from dataclasses import asdict, dataclass, field, replace
from datetime import datetime, timedelta, timezone
from enum import Enum
from typing import Any, Dict, List, Mapping, Optional, Sequence, Set, Tuple

logger = logging.getLogger(__name__)
logger.addHandler(logging.NullHandler())

# =============================================================================
# Version / Canonical contract
# =============================================================================

__version__ = "5.5.0"
SCORING_VERSION = __version__
SCORING_SCHEMA_VERSION = __version__
RECOMMENDATION_SOURCE_TAG = f"scoring.py v{__version__}"

RECOMMENDATION_ENUM: Tuple[str, ...] = (
    "STRONG_BUY",
    "BUY",
    "HOLD",
    "REDUCE",
    "SELL",
    "STRONG_SELL",
)
CANONICAL_RECOMMENDATION_CODES: Tuple[str, ...] = RECOMMENDATION_ENUM
_CANONICAL_RECO: Set[str] = set(CANONICAL_RECOMMENDATION_CODES)

# Canonical bucket thresholds used by local fallback and by derive_*_view.
# Risk: lower is better. 0-35 LOW, 35-70 MODERATE, >=70 HIGH.
# Confidence: 0-45 LOW, 45-75 MODERATE, >=75 HIGH.
#
# v5.5.0: these are the COMPILED-IN DEFAULTS. The module-level values below
# may be overridden by environment variables at import time via
# _resolve_bucket_thresholds_from_env(), which runs after the env helpers are
# defined. The override flags _RISK_THRESHOLDS_FROM_ENV and
# _CONF_THRESHOLDS_FROM_ENV record whether the env override took effect (used
# by get_canonical_state() for ops diagnostics).
RISK_BUCKET_THRESHOLDS: Tuple[float, float] = (35.0, 70.0)
CONFIDENCE_BUCKET_THRESHOLDS: Tuple[float, float] = (0.45, 0.75)
_RISK_THRESHOLDS_FROM_ENV: bool = False
_CONF_THRESHOLDS_FROM_ENV: bool = False

SCORE_PRECISION = 2
FRACTION_PRECISION = 4
ROI_PRECISION = 6
PENALTY_PRECISION = 4

# =============================================================================
# Environment controls
# =============================================================================

def _env_bool(name: str, default: bool = False) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    s = str(raw).strip().lower()
    if s in {"1", "true", "yes", "y", "on", "t"}:
        return True
    if s in {"0", "false", "no", "n", "off", "f"}:
        return False
    return default


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


def _env_text(name: str, default: str = "") -> str:
    raw = os.getenv(name)
    if raw is None:
        return default
    text = str(raw).strip()
    return text if text else default


def _nullable_overall_enabled() -> bool:
    return _env_bool("TFB_SCORING_NULLABLE_OVERALL", True)


def _structured_reason_enabled() -> bool:
    return _env_text("TFB_SCORING_REASON_FORMAT", "structured").lower() != "legacy"


def _strict_bucket_logs_enabled() -> bool:
    return _env_bool("TFB_SCORING_STRICT_BUCKET_LOGS", True)


def _resolve_bucket_thresholds_from_env() -> None:
    """v5.5.0: allow ops to tune bucket thresholds via env without forking the file.

    Reads SCORING_RISK_LOW / SCORING_RISK_HIGH / SCORING_CONFIDENCE_MODERATE /
    SCORING_CONFIDENCE_HIGH. Each pair must satisfy low < high and lie in the
    valid range (risk 0-100, confidence 0-1). Invalid pairs are rejected with a
    WARN log and the compiled-in defaults are retained.

    Updates the module-level RISK_BUCKET_THRESHOLDS / CONFIDENCE_BUCKET_THRESHOLDS
    tuples in place and records whether an env override actually took effect on
    _RISK_THRESHOLDS_FROM_ENV / _CONF_THRESHOLDS_FROM_ENV. ScoringConfig field
    defaults (which read these constants at class-definition time, later in the
    file) will see the resolved values.
    """
    global RISK_BUCKET_THRESHOLDS, CONFIDENCE_BUCKET_THRESHOLDS
    global _RISK_THRESHOLDS_FROM_ENV, _CONF_THRESHOLDS_FROM_ENV

    # Detect whether the env actually carries values (vs falling back to default)
    risk_low_raw = os.getenv("SCORING_RISK_LOW")
    risk_high_raw = os.getenv("SCORING_RISK_HIGH")
    conf_mod_raw = os.getenv("SCORING_CONFIDENCE_MODERATE")
    conf_high_raw = os.getenv("SCORING_CONFIDENCE_HIGH")

    if risk_low_raw is not None or risk_high_raw is not None:
        rl = _env_float("SCORING_RISK_LOW", RISK_BUCKET_THRESHOLDS[0])
        rh = _env_float("SCORING_RISK_HIGH", RISK_BUCKET_THRESHOLDS[1])
        if 0.0 <= rl < rh <= 100.0:
            RISK_BUCKET_THRESHOLDS = (rl, rh)
            _RISK_THRESHOLDS_FROM_ENV = True
        else:
            logger.warning(
                "[v5.5.0 BUCKET_CFG] invalid SCORING_RISK_LOW/HIGH (%s, %s); "
                "keeping defaults %s",
                rl, rh, RISK_BUCKET_THRESHOLDS,
            )

    if conf_mod_raw is not None or conf_high_raw is not None:
        cm = _env_float("SCORING_CONFIDENCE_MODERATE", CONFIDENCE_BUCKET_THRESHOLDS[0])
        ch = _env_float("SCORING_CONFIDENCE_HIGH", CONFIDENCE_BUCKET_THRESHOLDS[1])
        if 0.0 <= cm < ch <= 1.0:
            CONFIDENCE_BUCKET_THRESHOLDS = (cm, ch)
            _CONF_THRESHOLDS_FROM_ENV = True
        else:
            logger.warning(
                "[v5.5.0 BUCKET_CFG] invalid SCORING_CONFIDENCE_MODERATE/HIGH "
                "(%s, %s); keeping defaults %s",
                cm, ch, CONFIDENCE_BUCKET_THRESHOLDS,
            )


_resolve_bucket_thresholds_from_env()


# =============================================================================
# Canonical bucket integration
# =============================================================================

try:  # preferred package import on Render
    from core.buckets import (  # type: ignore  # noqa: WPS433
        risk_bucket_from_score as _bk_risk_bucket_from_score,
        confidence_bucket_from_score as _bk_confidence_bucket_from_score,
        normalize_risk_bucket as _bk_normalize_risk_bucket,
        normalize_confidence_bucket as _bk_normalize_confidence_bucket,
    )
    _BUCKETS_AVAILABLE = True
except Exception:
    try:  # fallback for local script-style execution
        from buckets import (  # type: ignore  # noqa: WPS433
            risk_bucket_from_score as _bk_risk_bucket_from_score,
            confidence_bucket_from_score as _bk_confidence_bucket_from_score,
            normalize_risk_bucket as _bk_normalize_risk_bucket,
            normalize_confidence_bucket as _bk_normalize_confidence_bucket,
        )
        _BUCKETS_AVAILABLE = True
    except Exception:
        _bk_risk_bucket_from_score = None  # type: ignore
        _bk_confidence_bucket_from_score = None  # type: ignore
        _bk_normalize_risk_bucket = None  # type: ignore
        _bk_normalize_confidence_bucket = None  # type: ignore
        _BUCKETS_AVAILABLE = False

BUCKETS_CANONICAL: bool = _BUCKETS_AVAILABLE

# =============================================================================
# Time helpers
# =============================================================================

_UTC = timezone.utc
_RIYADH_TZ = timezone(timedelta(hours=3))


def _utc_iso(dt: Optional[datetime] = None) -> str:
    d = dt or datetime.now(_UTC)
    if d.tzinfo is None:
        d = d.replace(tzinfo=_UTC)
    return d.astimezone(_UTC).isoformat()


def _riyadh_iso(dt: Optional[datetime] = None) -> str:
    d = dt or datetime.now(_RIYADH_TZ)
    if d.tzinfo is None:
        d = d.replace(tzinfo=_UTC)
    return d.astimezone(_RIYADH_TZ).isoformat()


# =============================================================================
# Enums / priority
# =============================================================================

class Horizon(str, Enum):
    DAY = "day"
    WEEK = "week"
    MONTH = "month"
    LONG = "long"


class Signal(str, Enum):
    STRONG_BUY = "STRONG_BUY"
    BUY = "BUY"
    HOLD = "HOLD"
    REDUCE = "REDUCE"
    SELL = "SELL"
    STRONG_SELL = "STRONG_SELL"


class RSISignal(str, Enum):
    OVERSOLD = "Oversold"
    NEUTRAL = "Neutral"
    OVERBOUGHT = "Overbought"
    N_A = "N/A"


PRIO_P1 = "P1"  # Critical action
PRIO_P2 = "P2"  # Strong/high-conviction opportunity
PRIO_P3 = "P3"  # Normal BUY
PRIO_P4 = "P4"  # HOLD / watch
PRIO_P5 = "P5"  # Low-priority reduce/sell
CANONICAL_PRIORITIES: Tuple[str, ...] = (PRIO_P1, PRIO_P2, PRIO_P3, PRIO_P4, PRIO_P5)
_CANONICAL_PRIORITIES_SET: Set[str] = set(CANONICAL_PRIORITIES)


class ScoringError(Exception):
    pass


class InvalidHorizonError(ScoringError):
    pass


class MissingDataError(ScoringError):
    pass


# =============================================================================
# Configuration
# =============================================================================

@dataclass(frozen=True)
class ScoringConfig:
    day_threshold: int = 5
    week_threshold: int = 14
    month_threshold: int = 90

    default_valuation: float = 0.30
    default_momentum: float = 0.20
    default_quality: float = 0.20
    default_growth: float = 0.15
    default_opportunity: float = 0.15
    default_technical: float = 0.00

    risk_penalty_strength: float = 0.36
    confidence_penalty_strength: float = 0.38

    risk_low_threshold: float = RISK_BUCKET_THRESHOLDS[0]
    risk_high_threshold: float = RISK_BUCKET_THRESHOLDS[1]
    confidence_low_to_moderate: float = CONFIDENCE_BUCKET_THRESHOLDS[0]
    confidence_high: float = CONFIDENCE_BUCKET_THRESHOLDS[1]

    @property
    def risk_moderate_threshold(self) -> float:
        """Compatibility alias for callers that still read the old name.

        v5.4.0 renamed the second risk cutoff to risk_high_threshold;
        v5.4.1 restores this read-only alias so existing audit tooling and
        Google Apps Script bridge checks do not fail with AttributeError.
        """
        return self.risk_high_threshold

    @classmethod
    def from_env(cls) -> "ScoringConfig":
        return cls(
            day_threshold=_env_int("SCORING_DAY_THRESHOLD", 5),
            week_threshold=_env_int("SCORING_WEEK_THRESHOLD", 14),
            month_threshold=_env_int("SCORING_MONTH_THRESHOLD", 90),
            default_valuation=_env_float("SCORING_W_VALUATION", 0.30),
            default_momentum=_env_float("SCORING_W_MOMENTUM", 0.20),
            default_quality=_env_float("SCORING_W_QUALITY", 0.20),
            default_growth=_env_float("SCORING_W_GROWTH", 0.15),
            default_opportunity=_env_float("SCORING_W_OPPORTUNITY", 0.15),
            default_technical=_env_float("SCORING_W_TECHNICAL", 0.00),
            risk_penalty_strength=_env_float("SCORING_RISK_PENALTY", 0.36),
            confidence_penalty_strength=_env_float("SCORING_CONFIDENCE_PENALTY", 0.38),
        )


_CONFIG = ScoringConfig.from_env()
_HORIZON_DAYS_CUTOFFS: Tuple[Tuple[int, Horizon], ...] = (
    (_CONFIG.day_threshold, Horizon.DAY),
    (_CONFIG.week_threshold, Horizon.WEEK),
    (_CONFIG.month_threshold, Horizon.MONTH),
)


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
        total = (
            self.w_valuation + self.w_momentum + self.w_quality +
            self.w_growth + self.w_opportunity + self.w_technical
        )
        if total <= 0:
            return self
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

    def as_factor_weights_map(self) -> Dict[str, float]:
        return {
            "valuation_score": self.w_valuation,
            "momentum_score": self.w_momentum,
            "quality_score": self.w_quality,
            "growth_score": self.w_growth,
            "opportunity_score": self.w_opportunity,
            "technical_score": self.w_technical,
        }


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
    opportunity_source: str = ""
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

    fundamental_view: Optional[str] = None
    technical_view: Optional[str] = None
    risk_view: Optional[str] = None
    value_view: Optional[str] = None

    recommendation: str = "HOLD"
    recommendation_reason: str = "Insufficient data."
    recommendation_detail: str = ""

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

    sector_relative_score: Optional[float] = None
    conviction_score: Optional[float] = None
    top_factors: str = ""
    top_risks: str = ""
    position_size_hint: str = ""

    scoring_updated_utc: str = field(default_factory=_utc_iso)
    scoring_updated_riyadh: str = field(default_factory=_riyadh_iso)
    scoring_errors: List[str] = field(default_factory=list)

    recommendation_priority_band: Optional[str] = None
    recommendation_source: str = ""
    scoring_schema_version: str = ""

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


ScoringWeights = ScoreWeights
DEFAULT_SCORING_WEIGHTS = ScoreWeights()
DEFAULT_FORECAST_PARAMETERS = ForecastParameters()
DEFAULT_WEIGHTS = DEFAULT_SCORING_WEIGHTS
DEFAULT_FORECASTS = DEFAULT_FORECAST_PARAMETERS


# =============================================================================
# Utility helpers
# =============================================================================

def _clamp(value: float, min_val: float, max_val: float) -> float:
    return max(min_val, min(value, max_val))


def _round(value: Optional[float], ndigits: int = SCORE_PRECISION) -> Optional[float]:
    if value is None:
        return None
    try:
        if math.isnan(float(value)) or math.isinf(float(value)):
            return None
        return round(float(value), ndigits)
    except Exception:
        return None


def _safe_float(value: Any) -> Optional[float]:
    if value is None:
        return None
    try:
        if isinstance(value, bool):
            return None
        if isinstance(value, (int, float)):
            f = float(value)
        else:
            s = str(value).strip().replace(",", "")
            if not s or s.lower() in {"na", "n/a", "none", "null", "nan", "-"}:
                return None
            # Handle arrows and display prefixes sometimes returned to Sheets.
            s = s.replace("▲", "").replace("▼", "").strip()
            if s.endswith("%"):
                f = float(s[:-1].strip()) / 100.0
            else:
                f = float(s)
        if math.isnan(f) or math.isinf(f):
            return None
        return f
    except Exception:
        return None


def _safe_str(value: Any, default: str = "") -> str:
    try:
        s = str(value).strip()
        return s if s else default
    except Exception:
        return default


def _safe_bool(value: Any) -> bool:
    if value is None:
        return False
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return bool(value)
    s = str(value).strip().lower()
    if s in {"true", "t", "yes", "y", "1", "on"}:
        return True
    if s in {"false", "f", "no", "n", "0", "off", "none", "null", "na", "n/a"}:
        return False
    return False


def _dedupe_preserving_order(items: Sequence[Any]) -> List[Any]:
    """v5.5.0: dedupe while preserving first-seen order.

    Used to clean scoring_errors at the end of compute_scores so the same
    provider tag cannot appear twice in the row's error list. Tolerant of
    unhashable items: falls back to identity equality on a per-item basis.
    """
    if not items:
        return []
    seen: Set[Any] = set()
    out: List[Any] = []
    for item in items:
        try:
            if item in seen:
                continue
            seen.add(item)
        except TypeError:
            # Unhashable: linear scan
            if any(prev is item or prev == item for prev in out):
                continue
        out.append(item)
    return out


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
    # ROI can be displayed as 10.5 or 10.5% or 0.105. Treat absolute >1 as percent points.
    if abs(f) > 1.0:
        return f / 100.0
    return f


def _as_upside_fraction(value: Any) -> Optional[float]:
    f = _safe_float(value)
    if f is None:
        return None
    # Engine canonical is fraction; legacy Sheets can carry percent points.
    if abs(f) <= 2.5:
        return f
    return f / 100.0


def _as_pct_position_fraction(value: Any) -> Optional[float]:
    f = _safe_float(value)
    if f is None:
        return None
    if abs(f) <= 1.5:
        return _clamp(f, 0.0, 1.0)
    return _clamp(f / 100.0, 0.0, 1.0)


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


def _sanitize_dividend_yield(value: Any, errors: Optional[List[str]] = None) -> Optional[float]:
    """Return dividend yield as fraction. Suppress suspicious provider errors."""
    f = _safe_float(value)
    if f is None:
        return None
    # Percent strings already became fraction in _safe_float. Numeric 5.2 means 5.2%.
    if abs(f) > 1.5:
        f /= 100.0
    if f < 0:
        if errors is not None:
            errors.append("dividend_yield_negative_suppressed")
        return None
    if f > 0.50:
        if errors is not None:
            errors.append("dividend_yield_suspicious_suppressed")
        return None
    if f > 0.25:
        if errors is not None:
            errors.append("dividend_yield_high_flagged")
    return _round(f, 6)


def _normalize_debt_to_equity(value: Any, errors: Optional[List[str]] = None) -> Optional[float]:
    """Return debt/equity as ratio. Providers often send 195 for 195% = 1.95."""
    f = _safe_float(value)
    if f is None:
        return None
    if f < 0:
        if errors is not None:
            errors.append("debt_to_equity_negative_ignored")
        return None
    if f > 10.0:
        if f <= 1000.0:
            if errors is not None:
                errors.append("debt_to_equity_percent_normalized")
            f /= 100.0
        else:
            if errors is not None:
                errors.append("debt_to_equity_suspicious_suppressed")
            return None
    return f


def _warning_tags_from_row(row: Mapping[str, Any]) -> Set[str]:
    raw = row.get("warnings") if isinstance(row, Mapping) else None
    if raw is None:
        return set()
    parts: List[str] = []
    if isinstance(raw, str):
        pieces = re.split(r"[;|]", raw)
        parts.extend(p.strip() for p in pieces if p.strip())
    elif isinstance(raw, (list, tuple, set)):
        parts.extend(str(x).strip() for x in raw if str(x).strip())
    else:
        s = str(raw).strip()
        if s:
            parts.append(s)
    out: Set[str] = set()
    for p in parts:
        out.add(p)
        if ":" in p:
            out.add(p.split(":", 1)[0].strip())
    return {x for x in out if x}


# =============================================================================
# Horizon / weights
# =============================================================================

def detect_horizon(settings: Any = None, row: Optional[Mapping[str, Any]] = None) -> Tuple[Horizon, Optional[int]]:
    horizon_days: Optional[float] = None
    if settings is not None:
        if isinstance(settings, Mapping):
            horizon_days = _safe_float(settings.get("horizon_days") or settings.get("invest_period_days"))
        else:
            horizon_days = _safe_float(
                getattr(settings, "horizon_days", None) or getattr(settings, "invest_period_days", None)
            )
    if horizon_days is None and row is not None:
        horizon_days = _get_float(row, "horizon_days", "horizon_days_effective", "invest_period_days", "horizon")
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
            w_technical=0.45, w_momentum=0.30, w_quality=0.10,
            w_valuation=0.00, w_growth=0.00, w_opportunity=0.15,
        ),
        Horizon.WEEK: ScoreWeights(
            w_technical=0.25, w_momentum=0.25, w_valuation=0.20,
            w_quality=0.15, w_growth=0.00, w_opportunity=0.15,
        ),
        Horizon.MONTH: ScoreWeights(
            w_technical=0.00, w_valuation=0.30, w_momentum=0.20,
            w_quality=0.20, w_growth=0.15, w_opportunity=0.15,
        ),
        Horizon.LONG: ScoreWeights(
            w_technical=0.00, w_valuation=0.35, w_quality=0.25,
            w_growth=0.18, w_momentum=0.12, w_opportunity=0.10,
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
        return "1Y"
    return {
        Horizon.DAY: "1D",
        Horizon.WEEK: "1W",
        Horizon.MONTH: "3M",
        Horizon.LONG: "1Y",
    }.get(horizon, "3M")


# =============================================================================
# Bucket helpers
# =============================================================================

def _local_risk_bucket(score: Optional[float]) -> Optional[str]:
    s = _norm_score_0_100(score)
    if s is None:
        return None
    low, high = RISK_BUCKET_THRESHOLDS
    if s < low:
        return "LOW"
    if s < high:
        return "MODERATE"
    return "HIGH"


def _local_confidence_bucket(value: Optional[float]) -> Optional[str]:
    v = _norm_confidence_0_1(value)
    if v is None:
        return None
    low_to_moderate, high = CONFIDENCE_BUCKET_THRESHOLDS
    if v >= high:
        return "HIGH"
    if v >= low_to_moderate:
        return "MODERATE"
    return "LOW"


def _normalize_bucket_text(value: Any, kind: str) -> Optional[str]:
    if value is None:
        return None
    try:
        if kind == "risk" and _bk_normalize_risk_bucket is not None:
            b = _bk_normalize_risk_bucket(value)
        elif kind == "confidence" and _bk_normalize_confidence_bucket is not None:
            b = _bk_normalize_confidence_bucket(value)
        else:
            b = value
    except Exception:
        b = value
    s = _safe_str(b).upper().replace("MEDIUM", "MODERATE")
    if s in {"LOW", "MODERATE", "HIGH"}:
        return s
    return None


def risk_bucket(score: Optional[float]) -> Optional[str]:
    if score is None:
        return None
    s = _norm_score_0_100(score)
    if s is None:
        return None
    if _BUCKETS_AVAILABLE and _bk_risk_bucket_from_score is not None:
        try:
            b = _normalize_bucket_text(_bk_risk_bucket_from_score(s), "risk")
            if b:
                return b
        except Exception as exc:
            if _strict_bucket_logs_enabled():
                logger.debug("core.buckets risk fallback: %s", exc)
    return _local_risk_bucket(s)


def confidence_bucket(conf01: Optional[float]) -> Optional[str]:
    if conf01 is None:
        return None
    v = _norm_confidence_0_1(conf01)
    if v is None:
        return None
    if _BUCKETS_AVAILABLE and _bk_confidence_bucket_from_score is not None:
        try:
            # core.buckets is described as scale-aware. Pass fraction first.
            b = _normalize_bucket_text(_bk_confidence_bucket_from_score(v), "confidence")
            if b:
                return b
        except Exception as exc:
            if _strict_bucket_logs_enabled():
                logger.debug("core.buckets confidence fallback: %s", exc)
    return _local_confidence_bucket(v)


def _risk_bucket(score: Optional[float]) -> Optional[str]:
    return risk_bucket(score)


def _confidence_bucket(value: Optional[float]) -> Optional[str]:
    return confidence_bucket(value)


# =============================================================================
# Derived fields
# =============================================================================

_UPSIDE_PCT_SUSPECT_FLOOR = -0.90
_UPSIDE_PCT_SUSPECT_CEILING = 2.00
_FORECAST_ROI12_UNIT_MISMATCH_FLOOR = -0.70
_FORECAST_ROI12_SYNTHESIS_CEILING = 2.00
_ENGINE_UNFORECASTABLE_TAGS: Set[str] = {
    "forecast_unavailable",
    "forecast_unavailable_no_source",
    "forecast_cleared_consistency_sweep",
    "forecast_skipped_unavailable",
}
_ENGINE_DROPPED_VALUATION_TAGS: Set[str] = {
    "intrinsic_unit_mismatch_suspected",
    "upside_synthesis_suspect",
    "engine_52w_high_unit_mismatch_dropped",
    "engine_52w_low_unit_mismatch_dropped",
    "engine_52w_high_low_inverted",
}


def _is_upside_suspect(upside: Optional[float]) -> bool:
    if upside is None:
        return False
    return upside < _UPSIDE_PCT_SUSPECT_FLOOR or upside > _UPSIDE_PCT_SUSPECT_CEILING


def _row_engine_dropped_valuation(row: Mapping[str, Any]) -> bool:
    return bool(_warning_tags_from_row(row) & _ENGINE_DROPPED_VALUATION_TAGS)


def derive_volume_ratio(row: Mapping[str, Any]) -> Optional[float]:
    vr = _get_float(row, "volume_ratio")
    if vr is not None and vr > 0:
        return _round(vr, 4)
    vol = _get_float(row, "volume")
    avg = _get_float(row, "avg_volume_10d") or _get_float(row, "avg_volume_30d")
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
    span = high - low
    if span <= 0:
        return _round(0.5, 4)
    return _round(_clamp((price - low) / span, 0.0, 1.0), 4)


def derive_upside_pct(row: Mapping[str, Any]) -> Optional[float]:
    supplied = _as_upside_fraction(_get(row, "upside_pct"))
    if supplied is not None:
        if _is_upside_suspect(supplied):
            return None
        return _round(supplied, 4)
    price = _get_float(row, "current_price", "price", "last_price")
    intrinsic = _get_float(row, "intrinsic_value", "fair_value", "target_price", "target_mean_price")
    if price is None or price <= 0 or intrinsic is None or intrinsic <= 0:
        return None
    raw = (intrinsic - price) / price
    if _is_upside_suspect(raw):
        return None
    return _round(raw, 4)


# =============================================================================
# Technical score
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
    if m >= 70 and t >= 55 and r <= 65:
        return Signal.BUY.value
    if m <= 30 or t <= 35:
        return Signal.SELL.value
    return Signal.HOLD.value


# =============================================================================
# Forecast helpers
# =============================================================================

def _forecast_params_from_settings(settings: Any) -> ForecastParameters:
    if settings is None:
        return DEFAULT_FORECAST_PARAMETERS

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
        min_roi_1m=_try_fraction("min_roi_1m", DEFAULT_FORECAST_PARAMETERS.min_roi_1m),
        max_roi_1m=_try_fraction("max_roi_1m", DEFAULT_FORECAST_PARAMETERS.max_roi_1m),
        min_roi_3m=_try_fraction("min_roi_3m", DEFAULT_FORECAST_PARAMETERS.min_roi_3m),
        max_roi_3m=_try_fraction("max_roi_3m", DEFAULT_FORECAST_PARAMETERS.max_roi_3m),
        min_roi_12m=_try_fraction("min_roi_12m", DEFAULT_FORECAST_PARAMETERS.min_roi_12m),
        max_roi_12m=_try_fraction("max_roi_12m", DEFAULT_FORECAST_PARAMETERS.max_roi_12m),
        ratio_1m_of_12m=_try_fraction("ratio_1m_of_12m", DEFAULT_FORECAST_PARAMETERS.ratio_1m_of_12m),
        ratio_3m_of_12m=_try_fraction("ratio_3m_of_12m", DEFAULT_FORECAST_PARAMETERS.ratio_3m_of_12m),
    )


def _empty_forecast_patch() -> Dict[str, Any]:
    return {
        "forecast_price_1m": None,
        "forecast_price_3m": None,
        "forecast_price_12m": None,
        "expected_roi_1m": None,
        "expected_roi_3m": None,
        "expected_roi_12m": None,
        "expected_return_1m": None,
        "expected_return_3m": None,
        "expected_return_12m": None,
        "expected_price_1m": None,
        "expected_price_3m": None,
        "expected_price_12m": None,
    }


def _is_row_unforecastable(row: Mapping[str, Any]) -> bool:
    if _safe_bool(_get(row, "forecast_unavailable", "is_forecast_unavailable")):
        return True
    tags = _warning_tags_from_row(row)
    if tags and (tags & _ENGINE_UNFORECASTABLE_TAGS):
        return True
    dq_label = str(_get(row, "data_quality") or "").strip().upper()
    if dq_label not in {"STALE", "MISSING", "ERROR"}:
        return False
    fair = _get_float(row, "intrinsic_value", "fair_value", "target_price", "target_mean_price")
    api_roi3 = _as_roi_fraction(_get(row, "expected_roi_3m", "expected_return_3m"))
    api_roi12 = _as_roi_fraction(_get(row, "expected_roi_12m", "expected_return_12m"))
    api_fp12 = _get_float(row, "forecast_price_12m", "expected_price_12m")
    return fair is None and api_roi3 is None and api_roi12 is None and api_fp12 is None


def derive_forecast_patch(row: Mapping[str, Any], forecasts: ForecastParameters) -> Tuple[Dict[str, Any], List[str]]:
    errors: List[str] = []
    if _is_row_unforecastable(row):
        errors.append("forecast_skipped_unavailable")
        return _empty_forecast_patch(), errors

    price = _get_float(row, "current_price", "price", "last_price", "last")
    fair = _get_float(
        row,
        "intrinsic_value", "fair_value", "target_price", "target_mean_price",
        "forecast_price_12m", "forecast_price_3m", "forecast_price_1m",
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

    synthesized_from_fair = False
    if price is not None and price > 0 and roi12 is None and fair is not None and fair > 0:
        roi12 = (fair / price) - 1.0
        synthesized_from_fair = True

    if roi12 is not None and roi12 < _FORECAST_ROI12_UNIT_MISMATCH_FLOOR:
        errors.append("forecast_suspect_unit_mismatch")
        roi12 = None
        fp12 = None
        if synthesized_from_fair:
            roi1 = None
            roi3 = None
            fp1 = None
            fp3 = None

    if roi12 is not None and synthesized_from_fair and roi12 > _FORECAST_ROI12_SYNTHESIS_CEILING:
        errors.append("forecast_suspect_synthesis_overshoot")
        roi12 = None
        roi3 = None
        roi1 = None
        fp12 = None
        fp3 = None
        fp1 = None

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

    patch = _empty_forecast_patch()
    patch.update({
        "forecast_price_1m": _round(fp1, 4),
        "forecast_price_3m": _round(fp3, 4),
        "forecast_price_12m": _round(fp12, 4),
        "expected_roi_1m": _round(roi1, ROI_PRECISION),
        "expected_roi_3m": _round(roi3, ROI_PRECISION),
        "expected_roi_12m": _round(roi12, ROI_PRECISION),
    })
    patch["expected_return_1m"] = patch["expected_roi_1m"]
    patch["expected_return_3m"] = patch["expected_roi_3m"]
    patch["expected_return_12m"] = patch["expected_roi_12m"]
    patch["expected_price_1m"] = patch["forecast_price_1m"]
    patch["expected_price_3m"] = patch["forecast_price_3m"]
    patch["expected_price_12m"] = patch["forecast_price_12m"]
    return patch, errors


# =============================================================================
# Component scoring
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


def _revenue_collapse_haircut(revenue_growth: Optional[float]) -> float:
    if revenue_growth is None:
        return 1.0
    start = -0.30
    floor = -0.75
    min_multiplier = 0.55
    if revenue_growth >= start:
        return 1.0
    span = floor - start
    progress = _clamp((revenue_growth - start) / span, 0.0, 1.0) if span != 0 else 1.0
    return _clamp(1.0 - progress * (1.0 - min_multiplier), min_multiplier, 1.0)


def compute_quality_score(row: Mapping[str, Any]) -> Optional[float]:
    roe = _as_fraction(_get(row, "roe", "return_on_equity", "returnOnEquity"))
    roa = _as_fraction(_get(row, "roa", "return_on_assets", "returnOnAssets"))
    op_margin = _as_fraction(_get(row, "operating_margin", "operatingMarginTTM"))
    net_margin = _as_fraction(_get(row, "profit_margin", "net_margin", "profitMargins"))
    de = _normalize_debt_to_equity(_get(row, "debt_to_equity", "debt_equity", "debtToEquity"))

    has_any_financial = any(x is not None for x in (roe, roa, op_margin, net_margin, de))
    dq_label = str(_get(row, "data_quality") or "").strip().upper()
    dq_is_weak = dq_label in {"", "POOR", "STALE", "MISSING", "ERROR", "UNKNOWN"}
    completeness = _completeness_factor(row)
    if not has_any_financial and dq_is_weak and completeness < 0.30:
        return None

    dq = _data_quality_factor(row)
    data_quality_proxy = _clamp(0.55 * dq + 0.45 * completeness, 0.0, 1.0)
    fin_parts: List[Tuple[float, float]] = []
    if roe is not None:
        fin_parts.append((0.28, _clamp((roe - 0.05) / 0.30, 0.0, 1.0)))
    if roa is not None:
        fin_parts.append((0.22, _clamp((roa - 0.02) / 0.16, 0.0, 1.0)))
    if op_margin is not None:
        fin_parts.append((0.22, _clamp((op_margin - 0.05) / 0.35, 0.0, 1.0)))
    if net_margin is not None:
        fin_parts.append((0.18, _clamp((net_margin - 0.02) / 0.28, 0.0, 1.0)))
    if de is not None:
        fin_parts.append((0.10, _clamp(1.0 - (de / 3.0), 0.0, 1.0)))

    if fin_parts:
        wsum = sum(w for w, _ in fin_parts)
        financial_quality = sum(w * v for w, v in fin_parts) / max(1e-9, wsum)
        combined = 0.50 * financial_quality + 0.50 * data_quality_proxy
    else:
        combined = data_quality_proxy

    revenue_growth = _as_fraction(_get(row, "revenue_growth_yoy", "revenue_growth", "growth_yoy"))
    combined *= _revenue_collapse_haircut(revenue_growth)
    return _round(100.0 * _clamp(combined, 0.0, 1.0), 2)


def compute_confidence_score(row: Mapping[str, Any]) -> Tuple[Optional[float], Optional[float]]:
    fc = _safe_float(_get(row, "forecast_confidence", "ai_confidence", "confidence_score", "confidence"))
    if fc is not None:
        fc01 = (fc / 100.0) if fc > 1.5 else fc
        fc01 = _clamp(fc01, 0.0, 1.0)
        return _round(fc01 * 100.0, 2), _round(fc01, 4)

    has_dq_signal = bool(str(_get(row, "data_quality") or "").strip())
    completeness = _completeness_factor(row)
    providers = row.get("data_sources") or row.get("providers") or []
    try:
        pcount = len(providers) if isinstance(providers, list) else 0
    except Exception:
        pcount = 0
    if not (has_dq_signal or completeness >= 0.40 or pcount >= 1):
        return None, None
    dq = _data_quality_factor(row)
    prov_factor = _clamp(pcount / 3.0, 0.0, 1.0)
    conf01 = _clamp(0.55 * dq + 0.35 * completeness + 0.10 * prov_factor, 0.0, 1.0)
    return _round(conf01 * 100.0, 2), _round(conf01, 4)


def _low_is_good(x: Optional[float], lo: float, hi: float) -> Optional[float]:
    if x is None or x <= 0:
        return None
    if x <= lo:
        return 1.0
    if x >= hi:
        return 0.0
    return 1.0 - ((x - lo) / (hi - lo))


def compute_valuation_score(row: Mapping[str, Any]) -> Optional[float]:
    price = _get_float(row, "current_price", "price", "last_price", "last")
    if price is None or price <= 0:
        return None

    fair = _get_float(row, "intrinsic_value", "fair_value", "target_price", "forecast_price_12m")
    upside: Optional[float] = None
    if fair is not None and fair > 0:
        raw_upside = (fair / price) - 1.0
        if not _is_upside_suspect(raw_upside):
            upside = raw_upside

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

    anchors = [
        v for v in (
            _low_is_good(pe, 8.0, 35.0),
            _low_is_good(pb, 0.8, 6.0),
            _low_is_good(ps, 1.0, 10.0),
            _low_is_good(peg, 0.8, 4.0),
            _low_is_good(ev, 6.0, 25.0),
        ) if v is not None
    ]
    anchor_avg = (sum(anchors) / len(anchors)) if anchors else None

    upside_n = _roi_norm(upside, 0.50)
    roi3_n = _roi_norm(roi3, 0.35)
    roi12_n = _roi_norm(roi12, 0.80)

    components: List[Tuple[float, Optional[float]]] = [
        (0.35, upside_n),
        (0.25, roi3_n),
        (0.20, roi12_n),
        (0.20, anchor_avg),
    ]

    # Fix: ratio-only valuation fallback is valid, but lower confidence.
    if upside_n is None and roi3_n is None and roi12_n is None and anchor_avg is not None:
        return _round(100.0 * _clamp(anchor_avg, 0.0, 1.0), 2)
    if all(v is None for _, v in components):
        return None

    total = 0.0
    for weight, value in components:
        total += weight * (value if value is not None else 0.50)
    return _round(100.0 * _clamp(total, 0.0, 1.0), 2)


def compute_growth_score(row: Mapping[str, Any]) -> Optional[float]:
    g = _as_fraction(_get(row, "revenue_growth_yoy", "revenue_growth", "growth_yoy"))
    if g is None:
        return None
    return _round(_clamp(((g + 0.30) / 0.60) * 100.0, 0.0, 100.0), 2)


def compute_momentum_score(row: Mapping[str, Any]) -> Optional[float]:
    pct = _as_roi_fraction(_get(row, "percent_change", "change_pct", "change_percent"))
    rsi = _get_float(row, "rsi_14", "rsi", "rsi14")
    pos = _as_pct_position_fraction(_get(row, "week_52_position_pct", "position_52w_pct", "week52_position_pct", "52w_position_pct"))
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
        if x is None or hi <= lo:
            return None
        return _clamp((x - lo) / (hi - lo), 0.0, 1.0)

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


def compute_opportunity_score_with_source(
    row: Mapping[str, Any],
    valuation: Optional[float],
    momentum: Optional[float],
) -> Tuple[Optional[float], str]:
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
        parts.append((0.45, r3))
    if r12 is not None:
        parts.append((0.40, r12))
    if r1 is not None:
        parts.append((0.15, r1))
    if parts:
        wsum = sum(w for w, _ in parts)
        return _round(100.0 * _clamp(sum(w * v for w, v in parts) / max(1e-9, wsum), 0.0, 1.0), 2), "roi_based"
    if valuation is None and momentum is None:
        return None, "insufficient"
    # v5.5.0: distinguish which side(s) of the val/mom fallback are real, so audit
    # tooling can tell a partial fallback (one input fabricated to 50) apart from
    # a true valuation+momentum blended fallback.
    if valuation is not None and momentum is None:
        source_tag = "valuation_only_fallback"
    elif valuation is None and momentum is not None:
        source_tag = "momentum_only_fallback"
    else:
        source_tag = "both_present_fallback"
    v = (valuation if valuation is not None else 50.0) / 100.0
    m = (momentum if momentum is not None else 50.0) / 100.0
    return _round(100.0 * _clamp(0.65 * v + 0.35 * m, 0.0, 1.0), 2), source_tag


def compute_opportunity_score(row: Mapping[str, Any], valuation: Optional[float], momentum: Optional[float]) -> Optional[float]:
    score, _source = compute_opportunity_score_with_source(row, valuation, momentum)
    return score


# =============================================================================
# Views
# =============================================================================

def _view_or_na(view: Optional[str]) -> str:
    if view is None:
        return "N/A"
    s = str(view).strip()
    return s if s else "N/A"


def derive_fundamental_view(quality: Optional[float], growth: Optional[float]) -> Optional[str]:
    if quality is None and growth is None:
        return None
    q = quality if quality is not None else 50.0
    g = growth if growth is not None else 50.0
    if q < 40.0:
        return "BEARISH"
    if g < 25.0 and q < 55.0:
        return "BEARISH"
    if q >= 65.0 and g >= 60.0:
        return "BULLISH"
    if q >= 75.0 and growth is None:
        return "BULLISH"
    return "NEUTRAL"


def derive_technical_view(technical: Optional[float], momentum: Optional[float], rsi_label: Optional[str] = None) -> Optional[str]:
    if technical is None and momentum is None:
        return None
    t = technical if technical is not None else 50.0
    m = momentum if momentum is not None else 50.0
    label = (rsi_label or "").strip().lower()
    is_overbought = label.startswith("overbought")
    if t < 40.0 or m < 35.0:
        return "BEARISH"
    if t >= 65.0 and m >= 55.0:
        return "NEUTRAL" if is_overbought else "BULLISH"
    return "NEUTRAL"


def derive_risk_view(risk: Optional[float]) -> Optional[str]:
    return risk_bucket(risk)


def derive_value_view(valuation: Optional[float], upside_pct: Optional[float] = None) -> Optional[str]:
    if upside_pct is not None:
        if upside_pct > 0.20:
            return "CHEAP"
        if upside_pct < -0.10:
            return "EXPENSIVE"
        if valuation is None:
            return "FAIR"
    if valuation is None:
        return None
    if valuation >= 65.0:
        return "CHEAP"
    if valuation < 40.0:
        return "EXPENSIVE"
    return "FAIR"


def score_views_completeness(row_or_scores: Mapping[str, Any]) -> Dict[str, Any]:
    fields = ("fundamental_view", "technical_view", "risk_view", "value_view")
    present = 0
    missing: List[str] = []
    for f in fields:
        v = row_or_scores.get(f) if isinstance(row_or_scores, Mapping) else None
        s = str(v).strip().upper() if v is not None else ""
        if not s or s == "N/A":
            missing.append(f)
        else:
            present += 1
    total = len(fields)
    return {"present": present, "total": total, "ratio": round(present / total, 4), "missing": missing}


# =============================================================================
# Recommendation helpers
# =============================================================================

_LOCAL_RECO_ALIASES: Dict[str, str] = {
    "STRONG_BUY": "STRONG_BUY",
    "STRONGBUY": "STRONG_BUY",
    "STRONG BUY": "STRONG_BUY",
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
    "STRONG_SELL": "STRONG_SELL",
    "STRONGSELL": "STRONG_SELL",
    "STRONG SELL": "STRONG_SELL",
}

_TRAILING_ARROW_RE = re.compile(
    r"(\s*(?:\u2192|->|=>|\bTHEN\b)\s*)"
    r"(STRONG[\s_]?BUY|BUY|ACCUMULATE|ADD|OUTPERFORM|OVERWEIGHT|HOLD|NEUTRAL|MAINTAIN|WATCH|MARKET[\s_]?PERFORM|REDUCE|TRIM|UNDERWEIGHT|SELL|AVOID|EXIT|UNDERPERFORM|STRONG[\s_]?SELL)"
    r"\s*\.?\s*$",
    re.IGNORECASE,
)

# v5.4.2: normalize only SAFE finance-jargon recommendation labels inside
# arbitrary prose. Do NOT use the full _LOCAL_RECO_ALIASES here because common
# English words such as WATCH, ADD, TRIM, AVOID, EXIT, NEUTRAL, and MAINTAIN can
# appear naturally in explanatory text and should not be rewritten.
#
# Full alias normalization is still preserved in normalize_recommendation_code()
# where the input is an extracted/standalone label. Trailing-arrow legacy labels
# are still handled by _TRAILING_ARROW_RE in _align_reason_to_canonical_recommendation().
_MID_TEXT_SAFE_ALIASES: Dict[str, str] = {
    "STRONGBUY": "STRONG_BUY",
    "STRONG BUY": "STRONG_BUY",
    "STRONGSELL": "STRONG_SELL",
    "STRONG SELL": "STRONG_SELL",
    "CONVICTION_BUY": "STRONG_BUY",
    "CONVICTION BUY": "STRONG_BUY",
    "TOP_PICK": "STRONG_BUY",
    "TOP PICK": "STRONG_BUY",
    "OUTPERFORM": "BUY",
    "OVERWEIGHT": "BUY",
    "UNDERPERFORM": "SELL",
    "UNDERWEIGHT": "REDUCE",
    "MARKET_PERFORM": "HOLD",
    "MARKET PERFORM": "HOLD",
}

_LEGACY_LABEL_PATTERNS: List[Tuple[re.Pattern[str], str]] = []
for _legacy_label, _canonical_label in _MID_TEXT_SAFE_ALIASES.items():
    _LEGACY_LABEL_PATTERNS.append((
        re.compile(
            r"(?<![A-Za-z0-9_])" + re.escape(_legacy_label) + r"(?![A-Za-z0-9_])",
            re.IGNORECASE,
        ),
        _canonical_label,
    ))

def _normalize_key(label: Any) -> str:
    s = _safe_str(label).upper()
    if not s:
        return ""
    s = s.replace("-", "_").replace(" ", "_")
    while "__" in s:
        s = s.replace("__", "_")
    return s.strip("_")


def normalize_recommendation_code(label: Any) -> str:
    key = _normalize_key(label)
    if not key:
        return "HOLD"
    if key in _CANONICAL_RECO:
        return key
    if key in _LOCAL_RECO_ALIASES:
        return _LOCAL_RECO_ALIASES[key]
    try:
        from core.reco_normalize import normalize_recommendation as _reco_norm  # type: ignore  # noqa: WPS433
        normalized = _reco_norm(label)
        nkey = _normalize_key(normalized)
        if nkey in _CANONICAL_RECO:
            return nkey
        if nkey in _LOCAL_RECO_ALIASES:
            return _LOCAL_RECO_ALIASES[nkey]
    except Exception:
        pass
    return "HOLD"


def _fmt_score_component(value: Optional[float]) -> str:
    if value is None:
        return "NA"
    try:
        return f"{float(value):.1f}"
    except Exception:
        return "NA"


def _fmt_roi_component(value: Optional[float]) -> str:
    if value is None:
        return "NA"
    try:
        v = float(value)
        if abs(v) <= 1.5:
            v *= 100.0
        return f"{v:.1f}"
    except Exception:
        return "NA"


def _format_recommendation_reason(
    rec: str,
    prose: str,
    overall: Optional[float],
    risk: Optional[float],
    confidence100: Optional[float],
    roi1: Optional[float],
    roi3: Optional[float],
    roi12: Optional[float],
    horizon: Horizon,
) -> str:
    canonical = normalize_recommendation_code(rec)
    text = str(prose or "No explanation available.").strip()
    if not _structured_reason_enabled():
        return text
    return (
        f"{canonical}: {text} | "
        f"overall={_fmt_score_component(overall)} "
        f"risk={_fmt_score_component(risk)} "
        f"conf={_fmt_score_component(confidence100)} "
        f"roi1m={_fmt_roi_component(roi1)}% "
        f"roi3m={_fmt_roi_component(roi3)}% "
        f"roi12m={_fmt_roi_component(roi12)}% "
        f"horizon={horizon.value}"
    )


def _align_reason_to_canonical_recommendation(reason: Optional[str], canonical_rec: Optional[str]) -> str:
    if not reason:
        return reason or ""
    text = str(reason)
    canonical = normalize_recommendation_code(canonical_rec or "HOLD")

    # v5.4.2: normalize only safe finance-jargon labels inside prose. Common
    # instruction words (WATCH/ADD/TRIM/AVOID/EXIT/NEUTRAL/MAINTAIN) are
    # intentionally excluded to preserve narrative wording.
    for pattern, replacement in _LEGACY_LABEL_PATTERNS:
        text = pattern.sub(replacement, text)

    match = _TRAILING_ARROW_RE.search(text)
    if match:
        text = text[: match.start()] + match.group(1) + canonical
    return text


def _build_view_prefix(fundamental: Optional[str], technical: Optional[str], risk: Optional[str], value: Optional[str]) -> str:
    return f"Fund: {_view_or_na(fundamental)} | Tech: {_view_or_na(technical)} | Risk: {_view_or_na(risk)} | Val: {_view_or_na(value)}"


def _active_roi_for_horizon(
    horizon: Horizon,
    roi1: Optional[float],
    roi3: Optional[float],
    roi12: Optional[float],
    upside_pct: Optional[float],
) -> Tuple[Optional[float], str]:
    """Return the ROI used for the recommendation plus a truthful label.

    v5.4.1 fixes misleading labels on fallback paths. The label now describes
    the actual source of the active ROI used in recommendation prose.
    """
    if horizon == Horizon.DAY:
        if roi1 is not None:
            return roi1, "1M"
        if roi3 is not None:
            return roi3, "3M_FALLBACK"
        if roi12 is not None:
            return roi12, "12M_FALLBACK"
        if upside_pct is not None:
            return upside_pct, "UPSIDE_FALLBACK"
        return None, "1M"

    if horizon == Horizon.WEEK:
        if roi1 is not None:
            return roi1, "1M"
        if roi3 is not None:
            return roi3, "3M_FALLBACK"
        if roi12 is not None:
            return roi12, "12M_FALLBACK"
        if upside_pct is not None:
            return upside_pct, "UPSIDE_FALLBACK"
        return None, "1M/3M"

    if horizon == Horizon.MONTH:
        if roi3 is not None:
            return roi3, "3M"
        if roi12 is not None:
            return roi12, "12M_FALLBACK"
        if upside_pct is not None:
            return upside_pct, "UPSIDE_FALLBACK"
        if roi1 is not None:
            return roi1, "1M_FALLBACK"
        return None, "3M"

    # Long horizon: 12M ROI is primary, then intrinsic upside, then 3M ROI.
    if roi12 is not None:
        return roi12, "12M"
    if upside_pct is not None:
        return upside_pct, "UPSIDE"
    if roi3 is not None:
        return roi3, "3M_FALLBACK"
    if roi1 is not None:
        return roi1, "1M_FALLBACK"
    return None, "12M"


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
    fundamental_view: Optional[str] = None,
    technical_view: Optional[str] = None,
    risk_view: Optional[str] = None,
    value_view: Optional[str] = None,
    upside_pct: Optional[float] = None,
    rsi_label: Optional[str] = None,
    conviction: Optional[float] = None,
    sector_relative: Optional[float] = None,
) -> Tuple[str, str]:
    if fundamental_view is None:
        fundamental_view = derive_fundamental_view(quality, growth)
    if technical_view is None:
        technical_view = derive_technical_view(technical, momentum, rsi_label)
    if risk_view is None:
        risk_view = derive_risk_view(risk)
    if value_view is None:
        value_view = derive_value_view(valuation, upside_pct)

    view_prefix = _build_view_prefix(fundamental_view, technical_view, risk_view, value_view)

    def _reason(rec: str, prose: str) -> Tuple[str, str]:
        code = normalize_recommendation_code(rec)
        return code, _format_recommendation_reason(code, prose, overall, risk, confidence100, roi1, roi3, roi12, horizon)

    if overall is None:
        return _reason("HOLD", "Insufficient data to score reliably.")

    c = confidence100 if confidence100 is not None else 55.0
    r = risk if risk is not None else 50.0
    o = overall
    q = quality if quality is not None else 50.0
    v = valuation if valuation is not None else 50.0
    m = momentum if momentum is not None else 50.0
    t = technical if technical is not None else 50.0
    conv = conviction if conviction is not None else o
    active_roi, active_label = _active_roi_for_horizon(horizon, roi1, roi3, roi12, upside_pct)
    active = active_roi if active_roi is not None else 0.0

    # First: confidence/risk controls.
    if c < 35.0:
        return _reason("HOLD", f"Low confidence ({_round(c, 1)}%) prevents a reliable action signal.")
    if r >= 90.0 and (c < 45.0 or o < 45.0):
        return _reason("STRONG_SELL", "Extreme risk and weak support require urgent exit or avoidance.")
    if r >= 85.0 and o < 55.0:
        return _reason("SELL", "Very high risk overrides the score profile.")
    if r >= 75.0 and o < 70.0:
        return _reason("REDUCE", "High risk overrides otherwise acceptable score.")
    if r >= 70.0 and active < 0.0 and o < 75.0:
        return _reason("REDUCE", "High risk combined with negative expected return does not support exposure.")

    # Short-term technical routes.
    if horizon in {Horizon.DAY, Horizon.WEEK}:
        if t >= 80 and m >= 70 and r <= 55 and c >= 55 and active >= 0.01:
            return _reason("BUY", "Short-term setup is positive with controlled risk.")
        if t < 35 or m < 30:
            return _reason("SELL", "Short-term technical setup has broken down.")

    # Long-horizon route: use 12M ROI/upside/valuation, not only 3M ROI.
    if horizon == Horizon.LONG:
        if active >= 0.25 and c >= 70 and r <= 60 and o >= 76 and v >= 60:
            return _reason("STRONG_BUY", f"High {active_label} expected return with strong confidence, valuation, and controlled risk.")
        if active >= 0.10 and c >= 60 and r <= 65 and o >= 68 and (v >= 55 or q >= 70):
            return _reason("BUY", f"Positive {active_label} expected return with acceptable confidence and risk.")
        if active >= 0.06 and c >= 60 and r <= 70 and o >= 65:
            return _reason("HOLD", f"Watch / accumulate candidate, but {active_label} return or risk is not strong enough for BUY.")
    else:
        # Month/default route.
        if active >= 0.18 and c >= 70 and r <= 55 and o >= 76:
            return _reason("STRONG_BUY", f"High {active_label} expected return with strong confidence and controlled risk.")
        if active >= 0.07 and c >= 60 and r <= 60 and o >= 68:
            return _reason("BUY", f"Positive {active_label} expected return with acceptable confidence and risk.")
        if active >= 0.04 and c >= 60 and r <= 65 and o >= 65:
            return _reason("HOLD", f"Watch / accumulate candidate, but {active_label} return is below BUY threshold.")

    # Quality fallback: allows good companies to remain HOLD instead of REDUCE.
    if o >= 70 and c >= 60 and r <= 70:
        return _reason("HOLD", "Strong profile, but expected return does not justify adding exposure now.")
    if o >= 65:
        return _reason("HOLD", "Score is acceptable but risk, confidence, or ROI does not justify adding exposure.")
    if o >= 50:
        return _reason("REDUCE", "Score is below preferred quality threshold.")
    return _reason("SELL", "Weak score profile does not support holding the position.")


def _recommendation(
    overall: Optional[float],
    risk: Optional[float],
    confidence100: Optional[float],
    roi3: Optional[float],
) -> Tuple[str, str]:
    return compute_recommendation(overall, risk, confidence100, roi3)


def _compute_priority(
    reco: Optional[str],
    overall: Optional[float],
    risk: Optional[float],
    confidence100: Optional[float],
    roi3: Optional[float],
) -> str:
    o = overall if overall is not None else 50.0
    r = risk if risk is not None else 50.0
    c = confidence100 if confidence100 is not None else 55.0
    canon = normalize_recommendation_code(reco or "HOLD")
    if canon == "STRONG_SELL":
        return PRIO_P1
    if canon == "STRONG_BUY":
        return PRIO_P2
    if canon == "BUY":
        return PRIO_P2 if (o >= 78 and c >= 70 and r <= 60) else PRIO_P3
    if canon == "REDUCE":
        return PRIO_P1 if (r >= 90 and c < 45) else PRIO_P5
    if canon == "SELL":
        return PRIO_P1 if (r >= 85 and o < 35) else PRIO_P5
    return PRIO_P4


# =============================================================================
# Insights builder integration
# =============================================================================

def _import_insights_builder():
    try:
        from core import insights_builder as _ib  # type: ignore  # noqa: WPS433
        return _ib
    except ImportError:
        try:
            import insights_builder as _ib  # type: ignore  # noqa: WPS433
            return _ib
        except ImportError:
            return None


# =============================================================================
# Canonical recommendation helpers
# =============================================================================

def _coerce_view(v: Any) -> Optional[str]:
    s = _safe_str(v)
    if not s or s.upper() == "N/A":
        return None
    return s


def derive_canonical_recommendation(row: Mapping[str, Any], *, settings: Any = None) -> Tuple[str, str, str]:
    existing_tag = _safe_str(row.get("recommendation_source"))
    if existing_tag == RECOMMENDATION_SOURCE_TAG:
        reco = normalize_recommendation_code(row.get("recommendation"))
        reason = _safe_str(row.get("recommendation_reason"))
        priority = _safe_str(row.get("recommendation_priority_band"))
        if priority not in _CANONICAL_PRIORITIES_SET:
            priority = _compute_priority(
                reco,
                _norm_score_0_100(row.get("overall_score")),
                _norm_score_0_100(row.get("risk_score")),
                _norm_score_0_100(row.get("confidence_score")),
                _as_roi_fraction(row.get("expected_roi_3m")),
            )
        return reco, reason, priority

    overall = _norm_score_0_100(row.get("overall_score"))
    risk_s = _norm_score_0_100(row.get("risk_score"))
    conf100 = _norm_score_0_100(row.get("confidence_score"))
    if conf100 is None:
        fc01 = _norm_confidence_0_1(row.get("forecast_confidence"))
        if fc01 is not None:
            conf100 = fc01 * 100.0

    roi1 = _as_roi_fraction(row.get("expected_roi_1m"))
    roi3 = _as_roi_fraction(row.get("expected_roi_3m"))
    roi12 = _as_roi_fraction(row.get("expected_roi_12m"))
    valuation = _norm_score_0_100(row.get("valuation_score"))
    quality_s = _norm_score_0_100(row.get("quality_score"))
    growth_s = _norm_score_0_100(row.get("growth_score"))
    momentum_s = _norm_score_0_100(row.get("momentum_score"))
    technical_s = _norm_score_0_100(row.get("technical_score"))
    conviction = _norm_score_0_100(row.get("conviction_score"))
    sector_rel = _norm_score_0_100(row.get("sector_relative_score"))
    usp = _as_upside_fraction(row.get("upside_pct"))
    rsi_val = _get_float(row, "rsi_14", "rsi", "rsi14")
    rsi_sig = rsi_signal(rsi_val)
    horizon, _hdays = detect_horizon(settings, row)

    rec, reason = compute_recommendation(
        overall, risk_s, conf100, roi3,
        horizon=horizon,
        technical=technical_s,
        momentum=momentum_s,
        roi1=roi1,
        roi12=roi12,
        quality=quality_s,
        growth=growth_s,
        valuation=valuation,
        fundamental_view=_coerce_view(row.get("fundamental_view")),
        technical_view=_coerce_view(row.get("technical_view")),
        risk_view=_coerce_view(row.get("risk_view")),
        value_view=_coerce_view(row.get("value_view")),
        upside_pct=usp,
        rsi_label=rsi_sig,
        conviction=conviction,
        sector_relative=sector_rel,
    )
    canonical_rec = normalize_recommendation_code(rec)
    aligned = _align_reason_to_canonical_recommendation(reason, canonical_rec)
    priority = _compute_priority(canonical_rec, overall, risk_s, conf100, roi3)
    return canonical_rec, aligned, priority


def apply_canonical_recommendation(
    row: Dict[str, Any],
    *,
    settings: Any = None,
    overwrite: bool = False,
) -> Dict[str, Any]:
    existing_tag = _safe_str(row.get("recommendation_source"))
    if (not overwrite) and existing_tag == RECOMMENDATION_SOURCE_TAG:
        return {}
    reco, reason, priority = derive_canonical_recommendation(row, settings=settings)
    return {
        "recommendation": reco,
        "recommendation_reason": reason,
        "recommendation_priority_band": priority,
        "recommendation_source": RECOMMENDATION_SOURCE_TAG,
    }


# =============================================================================
# Main scoring function
# =============================================================================

def _prepare_sanitized_working_row(source: Dict[str, Any], errors: List[str]) -> Dict[str, Any]:
    working = dict(source)
    dy = _sanitize_dividend_yield(_get(working, "dividend_yield", "yield"), errors)
    if dy is not None:
        working["dividend_yield"] = dy
    elif "dividend_yield" in working:
        working["dividend_yield"] = None

    de = _normalize_debt_to_equity(_get(working, "debt_to_equity", "debt_equity", "debtToEquity"), errors)
    if de is not None:
        working["debt_to_equity"] = de
    elif any(k in working for k in ("debt_to_equity", "debt_equity", "debtToEquity")):
        working["debt_to_equity"] = None
    return working


def _clean_structured_reason(reason: str, canonical_rec: str) -> str:
    if not _structured_reason_enabled():
        return reason
    # Avoid duplicate nested structured strings from insights_builder.
    prefix = canonical_rec + ":"
    if reason.startswith(prefix) and " | overall=" in reason:
        return reason
    return reason


def compute_scores(row: Dict[str, Any], settings: Any = None) -> Dict[str, Any]:
    source = dict(row or {})
    scoring_errors: List[str] = []

    err_raw = _get(source, "last_error_class", "lastErrorClass", "errorClass", "error_class")
    if err_raw is not None:
        err_class = _safe_str(err_raw)
        if err_class and err_class.lower() not in {"none", "null", "nil", "nan", "n/a", "na"}:
            scoring_errors.append("provider_error:" + err_class)

    if _row_engine_dropped_valuation(source):
        src_intrinsic = _get_float(source, "intrinsic_value", "fair_value")
        if src_intrinsic is None or src_intrinsic <= 0:
            scoring_errors.append("engine_dropped_valuation")

    forecasts = _forecast_params_from_settings(settings)
    forecast_patch, forecast_errors = derive_forecast_patch(source, forecasts)
    scoring_errors.extend(forecast_errors)

    # FIX: overwrite with None values too. This prevents stale ROI/forecast values.
    working = dict(source)
    working.update(forecast_patch)
    working = _prepare_sanitized_working_row(working, scoring_errors)

    horizon, hdays = detect_horizon(settings, working)
    weights = get_weights_for_horizon(horizon, settings)

    valuation = compute_valuation_score(working)
    momentum = compute_momentum_score(working)
    quality = compute_quality_score(working)
    growth = compute_growth_score(working)
    confidence100, conf01 = compute_confidence_score(working)
    risk = compute_risk_score(working)
    opportunity, opportunity_source = compute_opportunity_score_with_source(working, valuation, momentum)
    value_score = valuation
    tech_score = compute_technical_score(working)
    vol_ratio = derive_volume_ratio(working)
    drp = derive_day_range_position(working)
    usp = derive_upside_pct(working)

    if usp is None:
        raw_intrinsic = _get_float(working, "intrinsic_value", "fair_value")
        raw_price = _get_float(working, "current_price", "price", "last_price")
        supplied_usp = _as_upside_fraction(_get(source, "upside_pct"))
        if (
            raw_intrinsic is not None and raw_intrinsic > 0 and
            raw_price is not None and raw_price > 0 and
            _is_upside_suspect((raw_intrinsic - raw_price) / raw_price)
        ) or (supplied_usp is not None and _is_upside_suspect(supplied_usp)):
            scoring_errors.append("upside_synthesis_suspect")

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

    overall: Optional[float]
    overall_raw: Optional[float]
    penalty_factor: Optional[float]
    insufficient_inputs = False
    sig_weight_total = sum(w for w, _ in base_parts)
    if base_parts and (len(base_parts) >= 2 or sig_weight_total >= 0.40):
        base01 = sum(w * v for w, v in base_parts) / max(1e-9, sig_weight_total)
        overall_raw = _round(100.0 * _clamp(base01, 0.0, 1.0), 2)
        risk01 = (risk / 100.0) if risk is not None else 0.50
        conf01_used = conf01 if conf01 is not None else 0.55
        risk_pen = _clamp(1.0 - weights.risk_penalty_strength * (risk01 * 0.65), 0.0, 1.0)
        conf_pen = _clamp(1.0 - weights.confidence_penalty_strength * ((1.0 - conf01_used) * 0.75), 0.0, 1.0)
        penalty_factor = _round(risk_pen * conf_pen, PENALTY_PRECISION)
        overall = _round(100.0 * _clamp(base01 * risk_pen * conf_pen, 0.0, 1.0), 2)
    else:
        insufficient_inputs = True
        scoring_errors.append("insufficient_scoring_inputs")
        if _nullable_overall_enabled():
            overall = None
            overall_raw = None
            penalty_factor = None
        else:
            overall = 50.0
            overall_raw = 50.0
            penalty_factor = 1.0
        missing_components = [
            name for name, val in (
                ("technical_score", tech_score),
                ("valuation_score", valuation),
                ("momentum_score", momentum),
                ("quality_score", quality),
                ("growth_score", growth),
                ("opportunity_score", opportunity),
            ) if val is None
        ]
        logger.warning(
            "[v5.5.0 INSUFFICIENT] symbol=%s missing=%s",
            _safe_str(source.get("symbol") or source.get("ticker") or source.get("requested_symbol"), "UNKNOWN"),
            ",".join(missing_components),
        )

    rb = risk_bucket(risk)
    cb = confidence_bucket(confidence100)
    fundamental_view_raw = derive_fundamental_view(quality, growth)
    technical_view_raw = derive_technical_view(tech_score, momentum, rsi_sig)
    risk_view_raw = derive_risk_view(risk)
    value_view_raw = derive_value_view(valuation, usp)

    roi1 = _as_roi_fraction(working.get("expected_roi_1m"))
    roi3 = _as_roi_fraction(working.get("expected_roi_3m"))
    roi12 = _as_roi_fraction(working.get("expected_roi_12m"))

    conviction: Optional[float] = None
    ib = _import_insights_builder()
    if ib is not None and overall is not None:
        try:
            conviction = ib.compute_conviction_score(
                overall_score=overall,
                fundamental_view=fundamental_view_raw,
                technical_view=technical_view_raw,
                risk_view=risk_view_raw,
                value_view=value_view_raw,
                forecast_confidence=conf01,
                completeness=_completeness_factor(working),
            )
        except Exception as exc:
            logger.debug("compute_conviction_score failed: %s", exc)
            scoring_errors.append(f"conviction_failed:{type(exc).__name__}")

    rec, reason = compute_recommendation(
        overall if not insufficient_inputs or not _nullable_overall_enabled() else None,
        risk,
        confidence100,
        roi3,
        horizon=horizon,
        technical=tech_score,
        momentum=momentum,
        roi1=roi1,
        roi12=roi12,
        quality=quality,
        growth=growth,
        valuation=valuation,
        fundamental_view=fundamental_view_raw,
        technical_view=technical_view_raw,
        risk_view=risk_view_raw,
        value_view=value_view_raw,
        upside_pct=usp,
        rsi_label=rsi_sig,
        conviction=conviction,
        sector_relative=None,
    )
    canonical_rec = normalize_recommendation_code(rec)
    reason = _align_reason_to_canonical_recommendation(reason, canonical_rec)
    priority = _compute_priority(canonical_rec, overall, risk, confidence100, roi3)
    st_signal_val = short_term_signal(tech_score, momentum, risk, horizon)
    period_label = invest_period_label(horizon, hdays)

    top_factors_str = ""
    top_risks_str = ""
    pos_hint = ""
    structured_reason = reason

    if ib is not None and not insufficient_inputs:
        try:
            synthetic_row = dict(working)
            synthetic_row.update({
                "overall_score": overall,
                "valuation_score": valuation,
                "momentum_score": momentum,
                "quality_score": quality,
                "growth_score": growth,
                "value_score": value_score,
                "opportunity_score": opportunity,
                "technical_score": tech_score,
                "risk_score": risk,
                "fundamental_view": fundamental_view_raw,
                "technical_view": technical_view_raw,
                "risk_view": risk_view_raw,
                "value_view": value_view_raw,
                "forecast_confidence": conf01,
                "recommendation": canonical_rec,
                "recommendation_reason": reason,
            })
            bundle = ib.build_insights(
                synthetic_row,
                sector_scores=None,
                weights=weights.as_factor_weights_map(),
                base_reason=reason,
            )
            top_factors_str = bundle.top_factors or ""
            top_risks_str = bundle.top_risks or ""
            pos_hint = bundle.position_size_hint or ""
            structured_reason = bundle.recommendation_reason or reason
        except Exception as exc:
            logger.debug("build_insights failed for row: %s", exc)
            scoring_errors.append(f"insights_failed:{type(exc).__name__}")

    # Ensure final reason remains parseable and synchronized after insights_builder.
    structured_reason = _align_reason_to_canonical_recommendation(structured_reason, canonical_rec)
    if _structured_reason_enabled() and " | overall=" not in structured_reason:
        structured_reason = _format_recommendation_reason(
            canonical_rec,
            structured_reason,
            overall,
            risk,
            confidence100,
            roi1,
            roi3,
            roi12,
            horizon,
        )

    # If no insights hint exists, produce a useful position hint here.
    if not pos_hint:
        if canonical_rec in {"STRONG_BUY", "BUY"}:
            pos_hint = "Add gradually / accumulate"
        elif canonical_rec == "HOLD" and overall is not None and overall >= 65:
            pos_hint = "Maintain / watch for better entry"
        elif canonical_rec == "REDUCE":
            pos_hint = "Avoid new exposure / reduce"
        elif canonical_rec in {"SELL", "STRONG_SELL"}:
            pos_hint = "Exit or avoid"
        else:
            pos_hint = "Maintain"

    recommendation_detail = f"{priority} [{canonical_rec}]: {structured_reason}"

    # v5.5.0: dedupe scoring_errors while preserving first-seen order so the
    # same provider tag cannot appear twice in the row's error list.
    scoring_errors = _dedupe_preserving_order(scoring_errors)

    logger.info(
        "[v5.5.0 SCORE] symbol=%s overall=%s risk=%s conf=%s rec=%s",
        _safe_str(source.get("symbol") or source.get("ticker") or source.get("requested_symbol"), "UNKNOWN"),
        overall,
        risk,
        confidence100,
        canonical_rec,
    )

    scores = AssetScores(
        valuation_score=valuation,
        momentum_score=momentum,
        quality_score=quality,
        growth_score=growth,
        value_score=value_score,
        opportunity_score=opportunity,
        opportunity_source=opportunity_source,
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
        fundamental_view=_view_or_na(fundamental_view_raw),
        technical_view=_view_or_na(technical_view_raw),
        risk_view=_view_or_na(risk_view_raw),
        value_view=_view_or_na(value_view_raw),
        recommendation=canonical_rec,
        recommendation_reason=structured_reason,
        recommendation_detail=recommendation_detail,
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
        sector_relative_score=None,
        conviction_score=conviction,
        top_factors=top_factors_str,
        top_risks=top_risks_str,
        position_size_hint=pos_hint,
        scoring_errors=scoring_errors,
        recommendation_priority_band=priority,
        recommendation_source=RECOMMENDATION_SOURCE_TAG,
        scoring_schema_version=SCORING_SCHEMA_VERSION,
    )
    return scores.to_dict()


def score_row(row: Dict[str, Any], settings: Any = None) -> Dict[str, Any]:
    return compute_scores(row, settings=settings)


def score_quote(row: Dict[str, Any], settings: Any = None) -> Dict[str, Any]:
    return compute_scores(row, settings=settings)


def enrich_with_scores(row: Dict[str, Any], settings: Any = None, in_place: bool = False) -> Dict[str, Any]:
    target = row if in_place else dict(row or {})
    patch = compute_scores(target, settings=settings)
    target.update(patch)
    return target


class ScoringEngine:
    version = SCORING_VERSION

    def __init__(
        self,
        settings: Any = None,
        weights: Optional[ScoreWeights] = None,
        forecasts: Optional[ForecastParameters] = None,
    ):
        self.settings = settings
        # v5.4.1: copy module defaults so one engine instance cannot mutate
        # global defaults used by future instances.
        self.weights = weights if weights is not None else replace(DEFAULT_SCORING_WEIGHTS)
        self.forecasts = forecasts if forecasts is not None else replace(DEFAULT_FORECAST_PARAMETERS)

    def compute_scores(self, row: Dict[str, Any]) -> Dict[str, Any]:
        return compute_scores(row, settings=self.settings)

    def enrich_with_scores(self, row: Dict[str, Any], in_place: bool = False) -> Dict[str, Any]:
        return enrich_with_scores(row, settings=self.settings, in_place=in_place)

    def apply_canonical_recommendation(self, row: Dict[str, Any], overwrite: bool = False) -> Dict[str, Any]:
        return apply_canonical_recommendation(row, settings=self.settings, overwrite=overwrite)


# =============================================================================
# Ranking + batch scoring
# =============================================================================

def _rank_sort_tuple(row: Dict[str, Any], key_overall: str = "overall_score") -> Tuple[float, float, float, float, float, str]:
    overall = _norm_score_0_100(row.get(key_overall))
    opp = _norm_score_0_100(row.get("opportunity_score"))
    conf = _norm_score_0_100(row.get("confidence_score"))
    risk = _norm_score_0_100(row.get("risk_score"))
    roi3 = _as_roi_fraction(row.get("expected_roi_3m"))
    roi12 = _as_roi_fraction(row.get("expected_roi_12m"))
    active_roi = roi12 if roi12 is not None else roi3
    symbol = _safe_str(row.get("symbol"), "~")
    return (
        overall if overall is not None else -1e9,
        opp if opp is not None else -1e9,
        conf if conf is not None else -1e9,
        -(risk if risk is not None else 1e9),
        active_roi if active_roi is not None else -1e9,
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
        # Keep common aliases for sheet mapping.
        if rank_key == "rank_overall":
            row.setdefault("rank", rank)
    return target


def rank_rows_by_overall(rows: List[Dict[str, Any]], key_overall: str = "overall_score") -> List[Dict[str, Any]]:
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
            logger.exception("score_and_rank_rows: scoring failed")
            existing_errors = row.get("scoring_errors")
            if not isinstance(existing_errors, list):
                existing_errors = []
            existing_errors.append(f"scoring_exception:{type(exc).__name__}")
            row["scoring_errors"] = existing_errors

    ib = _import_insights_builder()
    if ib is not None:
        try:
            horizon_for_weights = Horizon.MONTH
            for r in prepared:
                hl = r.get("horizon_label")
                if hl:
                    try:
                        horizon_for_weights = Horizon(hl)
                        break
                    except ValueError:
                        continue
            weights = get_weights_for_horizon(horizon_for_weights, settings)
            ib.enrich_rows_with_insights(
                prepared,
                weights=weights.as_factor_weights_map(),
                sector_key="sector",
                inplace=True,
            )
        except Exception as exc:
            logger.debug("score_and_rank_rows: batch insights failed: %s", exc)

    for row in prepared:
        rec = normalize_recommendation_code(row.get("recommendation"))
        row["recommendation"] = rec
        reason = row.get("recommendation_reason")
        if reason:
            row["recommendation_reason"] = _align_reason_to_canonical_recommendation(str(reason), rec)
        ov = _norm_score_0_100(row.get("overall_score"))
        rk = _norm_score_0_100(row.get("risk_score"))
        cf = _norm_score_0_100(row.get("confidence_score"))
        r3 = _as_roi_fraction(row.get("expected_roi_3m"))
        priority = _compute_priority(rec, ov, rk, cf, r3)
        row["recommendation_priority_band"] = priority
        row["recommendation_source"] = RECOMMENDATION_SOURCE_TAG
        row["scoring_schema_version"] = SCORING_SCHEMA_VERSION
        if row.get("recommendation_reason"):
            # v5.4.1: always rebuild after insights_builder; compute_scores may
            # have created an earlier detail using the pre-insights reason.
            row["recommendation_detail"] = f"{priority} [{rec}]: {row['recommendation_reason']}"
        for view_key in ("fundamental_view", "technical_view", "risk_view", "value_view"):
            if view_key in row:
                row[view_key] = _view_or_na(row.get(view_key))

    # IMPORTANT: this must be called on the full dataset, not partial batches,
    # when dashboard-level global rank is required.
    assign_rank_overall(prepared, key_overall=key_overall, inplace=True, rank_key="rank_overall")
    return prepared


# =============================================================================
# v5.5.0 — Public diagnostic helpers
# =============================================================================

def get_canonical_thresholds() -> Dict[str, Any]:
    """Return the bucket thresholds the running process is actually using.

    Useful for audit/diagnostic tooling to verify scoring.py is aligned with
    other modules in the stack (data_engine_v2, investment_advisor) without
    re-reading env vars or guessing.

    Returns
    -------
    Dict[str, Any]
        {
          "risk_thresholds":           (low, high),       # 0-100 scale
          "confidence_thresholds":     (moderate, high),  # 0-1 fraction
          "risk_thresholds_from_env":  bool,
          "confidence_thresholds_from_env": bool,
        }
    """
    return {
        "risk_thresholds": tuple(RISK_BUCKET_THRESHOLDS),
        "confidence_thresholds": tuple(CONFIDENCE_BUCKET_THRESHOLDS),
        "risk_thresholds_from_env": bool(_RISK_THRESHOLDS_FROM_ENV),
        "confidence_thresholds_from_env": bool(_CONF_THRESHOLDS_FROM_ENV),
    }


def get_canonical_state() -> Dict[str, Any]:
    """Return a full ops snapshot of scoring.py state.

    Includes version, recommendation source tag, canonical bucket availability,
    threshold settings, and the active ScoringConfig (as a plain dict via
    asdict for JSON-friendliness).

    This is the single entry-point audit/diagnostic tooling should call to
    verify what the running scoring process is doing.
    """
    try:
        config_snapshot: Dict[str, Any] = asdict(_CONFIG)
    except Exception:
        # _CONFIG is a frozen dataclass; asdict should always work, but defensive.
        config_snapshot = {}

    # Include the read-only compat alias explicitly so audit tooling can
    # confirm the property still resolves (it is not surfaced by asdict).
    try:
        config_snapshot["risk_moderate_threshold"] = _CONFIG.risk_moderate_threshold
    except Exception:
        pass

    thresholds = get_canonical_thresholds()
    return {
        "version": SCORING_VERSION,
        "schema_version": SCORING_SCHEMA_VERSION,
        "recommendation_source_tag": RECOMMENDATION_SOURCE_TAG,
        "buckets_canonical": bool(BUCKETS_CANONICAL),
        "risk_thresholds": thresholds["risk_thresholds"],
        "confidence_thresholds": thresholds["confidence_thresholds"],
        "risk_thresholds_from_env": thresholds["risk_thresholds_from_env"],
        "confidence_thresholds_from_env": thresholds["confidence_thresholds_from_env"],
        "recommendation_enum": list(RECOMMENDATION_ENUM),
        "canonical_priorities": list(CANONICAL_PRIORITIES),
        "config": config_snapshot,
    }


# =============================================================================
# Module exports
# =============================================================================

__all__ = [
    "__version__",
    "SCORING_VERSION",
    "SCORING_SCHEMA_VERSION",
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
    "DEFAULT_SCORING_WEIGHTS",
    "DEFAULT_FORECAST_PARAMETERS",
    "RECOMMENDATION_ENUM",
    "RISK_BUCKET_THRESHOLDS",
    "CONFIDENCE_BUCKET_THRESHOLDS",
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
    "compute_opportunity_score_with_source",
    "compute_confidence_score",
    "compute_recommendation",
    "risk_bucket",
    "confidence_bucket",
    "_recommendation",
    "_risk_bucket",
    "_confidence_bucket",
    "derive_fundamental_view",
    "derive_technical_view",
    "derive_risk_view",
    "derive_value_view",
    "ScoringError",
    "InvalidHorizonError",
    "MissingDataError",
    "score_views_completeness",
    "derive_canonical_recommendation",
    "apply_canonical_recommendation",
    "RECOMMENDATION_SOURCE_TAG",
    "CANONICAL_PRIORITIES",
    "PRIO_P1",
    "PRIO_P2",
    "PRIO_P3",
    "PRIO_P4",
    "PRIO_P5",
    "BUCKETS_CANONICAL",
    # v5.5.0 additions
    "get_canonical_thresholds",
    "get_canonical_state",
]
