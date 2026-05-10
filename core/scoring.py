#!/usr/bin/env python3
# core/scoring.py
"""
================================================================================
Scoring Module — v5.2.3
(AUDIT-DRIVEN HARDENING / NO-FABRICATED-CONFIDENCE /
 RECOMMENDATION-COHERENCE-GUARDED / RISK-PENALTY-REBALANCED /
 REVENUE-COLLAPSE-AWARE QUALITY / UNIT-MISMATCH-SAFE FORECASTS /
 ILLIQUID-AWARE FORECAST SKIP /
 [PRESERVED] VIEW-COMPLETENESS HARDENED / VIEW-TOKENS-IN-EVERY-REASON /
 ACCUMULATE-ALIGNED / 5-TIER + CONVICTION FLOORS /
 INSIGHTS-INTEGRATED / PHANTOM-ROW-SAFE / SCHEMA-ALIGNED /
 v5.53.0 ENGINE-UNIT COMPATIBLE)
================================================================================

v5.2.3 changes (vs v5.2.2)  —  AUDIT-DRIVEN HARDENING
-----------------------------------------------------
Closes six structural defects surfaced by the production audit of
the deployed Global_Markets sheet (1,707 rows × 46 cols). v5.2.2's
unit-conversion fix was correct but local; the audit revealed broader
pathological patterns in scoring outputs that v5.2.3 addresses.

Audit findings (with concrete examples from the snapshot):

  1. Forecast Confidence collapsed onto a ~60% baseline for >90% of
     rows. compute_confidence_score's fallback path was always
     synthesizing a value (~55) from _data_quality_factor's default
     of 0.60 + completeness ~0.50 + provider count = 0. The "60%
     wall" was therefore not a model output but a fabrication.

  2. BUY / STRONG_BUY recommendations attached to rows whose own
     forecast pointed DOWN (negative 1M and 3M ROI). Examples:
     STNE.US (BUY, roi3=-3.1%), CNX.US (BUY, roi3=-2.4%),
     COALINDIA.NSE (BUY, roi3=-1.9%). compute_recommendation
     trusted reco_normalize's output without cross-checking against
     the row's own forecasts.

  3. REDUCE-heavy recommendation distribution (~50% of rows).
     risk_penalty_strength=0.55 was making high-risk stocks lose
     ~27% of their composite score, mechanically pushing them into
     REDUCE/SELL even when the underlying fundamentals warranted
     HOLD/BUY.

  4. Quality Score = 95 on KROS.US despite revenue YoY = -87%.
     compute_quality_score weighted ROE / margins independently of
     top-line trajectory. A company can have 30% net margins this
     quarter and still be in terminal decline; the score didn't
     recognize that.

  5. LSE / JSE / TA tickers showing phantom -97% to -99% downside.
     Examples: AV.LSE (-97.45%), STAN.LSE (-99.0%), MTN.JSE
     (-98.62%), POLI.TA (-99.0%). Caused by intrinsic_value being
     in the major currency unit (GBP/ZAR/ILS) while current_price
     was in the subunit (GBX/ZAC/ILA). derive_forecast_patch
     happily computed (fair/price - 1) without checking unit
     consistency, producing roi12 values clamped to the most
     negative bound and flowing through to valuation_score.

  6. Placeholder forecasts emitted for delisted / illiquid rows.
     LSI.US (delisted), EXPGF.US / BABWF.US (PINK/OTC stale), and
     OOREDOO.KW (zero market cap) all had synthesized forecast
     fields even though their data was unusable. derive_forecast_patch
     had no early-exit for unrecoverable inputs.

v5.2.3 fixes, by audit item:

  A. compute_confidence_score — NO-FABRICATED-CONFIDENCE.
     When there is genuinely no signal (no explicit DQ marker AND
     completeness < 0.40 AND no listed providers), return
     (None, None) instead of the synthesized fallback. The
     downstream consumers (recommendation gates, confidence_bucket,
     compute_scores' penalty calc) already treat None correctly.

  B. compute_recommendation — RECOMMENDATION-COHERENCE-GUARD.
     After reco_normalize returns its label, verify the label is
     coherent with the row's own forecasts. Specifically:
       - if canonical_rec ∈ {BUY, STRONG_BUY}
       - AND roi3 < -0.02  (more than 2% expected loss in 3 months)
       - AND confidence100 < 65
     -> downgrade to HOLD with an explicit
        "coherence guard: <orig> → HOLD; 3M ROI <pct>% with
         AI confidence <conf>%" reason.
     Strong signals (high confidence) override the guard, since
     a high-conviction BUY with mildly negative 3M ROI may still
     be a legitimate accumulation call. Low-confidence BUYs in the
     face of negative forecasts are the audit pattern we're killing.

  C. ScoringConfig.risk_penalty_strength — RISK-PENALTY-REBALANCE.
     Default lowered from 0.55 to 0.40. With v5.2.2 weights, a
     high-risk stock (risk01 = 0.7) was losing 0.55 * 0.49 = 27%
     of its composite. v5.2.3 brings that to 0.40 * 0.49 = 20%,
     restoring REDUCE for genuinely-bad fundamentals while not
     punishing volatile-but-good companies. ScoreWeights and the
     env-var default ($SCORING_RISK_PENALTY) updated together so
     the change is centralised.

  D. compute_quality_score — REVENUE-COLLAPSE-AWARE.
     Final combined score is multiplied by a haircut factor when
     revenue_growth_yoy is severely negative. Linear ramp:
       at -30% YoY → haircut = 1.0  (no penalty; cyclical bottom)
       at -50% YoY → haircut = 0.80
       at -75% YoY → haircut = 0.55  (clamped floor)
     KROS-style cases (revenue -87%) cap at 0.55, bringing a
     pre-haircut quality of 95 down to ~52 — still positive (the
     margins are real today) but no longer aspirational. The
     haircut is multiplicative on the final 0-1 combined score
     before scaling, so the effect is consistent across rows.

  E. derive_forecast_patch — UNIT-MISMATCH SANITY GUARD.
     After roi12 has been derived (whether from API or synthesized
     from fair/price), if it falls below -50%, suppress it and
     emit "forecast_suspect_unit_mismatch" in errors. -50% is
     deliberately conservative: a genuine 50%+ overvaluation
     belief is rare and almost always implies a unit-conversion
     bug (subunit pricing, share-class confusion, or stale fair
     value vs ratio split).
     Trapped here, this prevents the bad roi12 from propagating
     to valuation_score (which calls _as_roi_fraction on
     expected_roi_12m from the working row) and from being
     written to the sheet's expected_roi_12m / forecast_price_12m
     columns. The 1m/3m derived values from the bad 12m are also
     suppressed.

  F. derive_forecast_patch — ILLIQUID-AWARE EARLY EXIT.
     Before any synthesis happens, check whether the row is
     unforecastable:
       - explicit forecast_unavailable=True flag, OR
       - data_quality ∈ {STALE, MISSING, ERROR} AND no fair value
         AND no API-supplied roi3/roi12
     -> return all-None forecast patch with
        "forecast_skipped_unavailable" error tag.
     Stops the placeholder-forecast leak for delisted /
     PINK-stale / zero-market-cap rows.

[PRESERVED — strictly] Every v5.2.2 / v5.2.1 / v5.2.0 / v5.1.0
helper, signature, dataclass field, and behaviour. The structural
changes are ALL in function bodies; public API surface is
unchanged. No removals from __all__.

Public API additions in v5.2.3:
  None at module level. The behavioural changes are all internal
  to the existing public functions. The new helper
  _coherence_guard_recommendation is private (leading underscore).

================================================================================

v5.2.2 changes (vs v5.2.1)
--------------------------
[PRESERVED] _as_pct_position_fraction helper for week_52_position_pct
in PERCENT POINTS storage (post data_engine_v2 v5.53.0). Threshold
1.0 instead of 1.5 fixes the [1.0, 1.5] edge case where stocks
sitting 1.0%-1.5% above their 52W low were being misread as
fractions. Single call-site swap in compute_momentum_score.

v5.2.1 changes (vs v5.2.0)
--------------------------
[PRESERVED] _build_view_prefix module helper. compute_recommendation
derives all four views at the TOP of the function (before any
early return), builds the view prefix once, and includes it in
every return path. compute_scores insufficient_inputs path also
includes the prefix.

v5.2.0 changes (vs v5.1.0)
--------------------------
[PRESERVED] _view_or_na, _CANONICAL_REC_ALIASES,
_align_reason_to_canonical_recommendation, score_views_completeness.
compute_scores applies the alignment helper twice (before and
after build_insights). compute_recommendation applies it to its
return value.

Public API (unchanged from v5.2.2):
  All exports preserved. No removals.
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
from typing import (
    Any,
    Dict,
    List,
    Mapping,
    Optional,
    Sequence,
    Set,
    Tuple,
)

logger = logging.getLogger(__name__)
logger.addHandler(logging.NullHandler())

# =============================================================================
# Version
# =============================================================================

__version__ = "5.2.3"
SCORING_VERSION = __version__

# =============================================================================
# Time Helpers
# =============================================================================

_UTC = timezone.utc
_RIYADH_TZ = timezone(timedelta(hours=3))


def _utc_iso(dt: Optional[datetime] = None) -> str:
    d = dt or datetime.now(_UTC)
    if d.tzinfo is None:
        d = d.replace(tzinfo=_UTC)
    return d.astimezone(_UTC).isoformat()


def _riyadh_iso(dt: Optional[datetime] = None) -> str:
    if dt is None:
        return datetime.now(_RIYADH_TZ).isoformat()
    d = dt
    if d.tzinfo is None:
        d = d.replace(tzinfo=_UTC)
    return d.astimezone(_RIYADH_TZ).isoformat()


# =============================================================================
# Enums (preserved)
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


class RSISignal(str, Enum):
    OVERSOLD = "Oversold"
    NEUTRAL = "Neutral"
    OVERBOUGHT = "Overbought"
    N_A = "N/A"


# =============================================================================
# Custom Exceptions (preserved)
# =============================================================================

class ScoringError(Exception):
    pass


class InvalidHorizonError(ScoringError):
    pass


class MissingDataError(ScoringError):
    pass


# =============================================================================
# v5.2.3 — Audit-driven thresholds
# =============================================================================
#
# Centralising these here so a future ops-side tuning round can move
# them via env vars without touching the function bodies.

# Recommendation-coherence guard. Rows where reco_normalize said
# BUY/STRONG_BUY but the row's own roi3 is below this threshold AND
# confidence is below the confidence threshold get downgraded to HOLD.
_COHERENCE_ROI3_FLOOR_FRACTION = -0.02   # -2% over 3 months
_COHERENCE_CONFIDENCE_FLOOR = 65.0       # 65% AI confidence

# Forecast unit-mismatch guard. roi12 values below this get suppressed
# as suspected unit-conversion bugs. -50% is deliberately conservative.
_FORECAST_ROI12_UNIT_MISMATCH_FLOOR = -0.50

# Quality revenue-collapse haircut. Linear ramp from "no penalty" at
# the start threshold to "max penalty" at the floor threshold. The
# haircut multiplies the final 0-1 combined quality score.
_QUALITY_REVENUE_COLLAPSE_START = -0.30   # Start applying haircut at -30% YoY
_QUALITY_REVENUE_COLLAPSE_FLOOR = -0.75   # Floor (max haircut) at -75% YoY
_QUALITY_REVENUE_COLLAPSE_MAX_HAIRCUT = 0.55  # Min multiplier (1.0 = no penalty)

# Confidence non-fabrication thresholds. Below all three, return None
# instead of synthesizing.
_CONFIDENCE_FALLBACK_MIN_COMPLETENESS = 0.40
_CONFIDENCE_FALLBACK_MIN_PROVIDERS = 1


# =============================================================================
# Configuration
# =============================================================================

@dataclass(frozen=True)
class ScoringConfig:
    day_threshold: int = 5
    week_threshold: int = 14
    month_threshold: int = 90

    default_valuation: float = 0.30
    default_momentum: float = 0.25
    default_quality: float = 0.20
    default_growth: float = 0.15
    default_opportunity: float = 0.10
    default_technical: float = 0.00

    # v5.2.3: lowered from 0.55 to 0.40 to address audit finding #3
    # (REDUCE-heavy distribution). With v5.2.2 weights a high-risk
    # stock lost ~27% of its composite score; v5.2.3 brings that to
    # ~20%, restoring HOLD/BUY for genuinely-good-but-volatile names.
    risk_penalty_strength: float = 0.40

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
            # v5.2.3: env default lowered from 0.55 to 0.40 (see above)
            risk_penalty_strength=_env_float("SCORING_RISK_PENALTY", 0.40),
            confidence_penalty_strength=_env_float("SCORING_CONFIDENCE_PENALTY", 0.45),
        )


_CONFIG = ScoringConfig.from_env()

# =============================================================================
# Horizon Thresholds (preserved)
# =============================================================================

_HORIZON_DAYS_CUTOFFS: Tuple[Tuple[int, Horizon], ...] = (
    (_CONFIG.day_threshold, Horizon.DAY),
    (_CONFIG.week_threshold, Horizon.WEEK),
    (_CONFIG.month_threshold, Horizon.MONTH),
)


# =============================================================================
# v5.2.0 — View / recommendation hardening constants (PRESERVED)
# =============================================================================

_CANONICAL_REC_LABELS_SET: Set[str] = {
    "STRONG_BUY", "BUY", "HOLD", "REDUCE", "SELL",
}

_CANONICAL_REC_ALIASES: Dict[str, str] = {
    "ACCUMULATE": "BUY",
    "ADD": "BUY",
    "OUTPERFORM": "BUY",
    "OVERWEIGHT": "BUY",
    "STRONGBUY": "STRONG_BUY",
    "MARKET_PERFORM": "HOLD",
    "MARKET PERFORM": "HOLD",
    "NEUTRAL": "HOLD",
    "WATCH": "HOLD",
    "MAINTAIN": "HOLD",
    "TRIM": "REDUCE",
    "UNDERWEIGHT": "REDUCE",
    "AVOID": "SELL",
    "EXIT": "SELL",
    "UNDERPERFORM": "SELL",
    "STRONG_SELL": "SELL",
    "STRONGSELL": "SELL",
    "STRONG SELL": "SELL",
}

_TRAILING_ARROW_RE = re.compile(
    r'(\s*(?:\u2192|->|=>|\bTHEN\b)\s*)'
    r'('
    r'STRONG[\s_]?BUY|BUY|ACCUMULATE|ADD|OUTPERFORM|OVERWEIGHT'
    r'|HOLD|NEUTRAL|MAINTAIN|WATCH|MARKET[\s_]?PERFORM'
    r'|REDUCE|TRIM|UNDERWEIGHT'
    r'|SELL|AVOID|EXIT|UNDERPERFORM|STRONG[\s_]?SELL'
    r')'
    r'\s*\.?\s*$',
    re.IGNORECASE,
)

_LEGACY_LABEL_PATTERNS: List[Tuple[re.Pattern, str]] = [
    (
        re.compile(
            r'(?<![A-Za-z0-9_])' + re.escape(legacy) + r'(?![A-Za-z0-9_])',
            re.IGNORECASE,
        ),
        canonical,
    )
    for legacy, canonical in _CANONICAL_REC_ALIASES.items()
]


def _view_or_na(view: Optional[str]) -> str:
    if view is None:
        return "N/A"
    s = str(view).strip()
    if not s:
        return "N/A"
    return s


def _build_view_prefix(
    fundamental: Optional[str],
    technical: Optional[str],
    risk: Optional[str],
    value: Optional[str],
) -> str:
    return (
        "Fund: " + _view_or_na(fundamental)
        + " | Tech: " + _view_or_na(technical)
        + " | Risk: " + _view_or_na(risk)
        + " | Val: " + _view_or_na(value)
    )


def _align_reason_to_canonical_recommendation(
    reason: Optional[str],
    canonical_rec: Optional[str],
) -> str:
    if not reason:
        return reason or ""

    text = str(reason)

    if not canonical_rec:
        canonical_rec = "HOLD"

    for pattern, canonical_label in _LEGACY_LABEL_PATTERNS:
        if canonical_label == "HOLD" and pattern.pattern.upper().find('NEUTRAL') > -1:
            continue
        text = pattern.sub(canonical_label, text)

    match = _TRAILING_ARROW_RE.search(text)
    if match:
        arrow_part = match.group(1)
        existing_label_raw = match.group(2).strip().upper().replace(' ', '_')

        if existing_label_raw in _CANONICAL_REC_LABELS_SET:
            existing_canonical = existing_label_raw
        else:
            existing_canonical = _CANONICAL_REC_ALIASES.get(
                existing_label_raw,
                _CANONICAL_REC_ALIASES.get(
                    existing_label_raw.replace('_', ' '),
                    None,
                ),
            )

        if (
            existing_canonical is not None
            and existing_canonical in _CANONICAL_REC_LABELS_SET
            and existing_label_raw != canonical_rec
        ):
            text = text[: match.start()] + arrow_part + canonical_rec

    return text


def score_views_completeness(
    row_or_scores: Mapping[str, Any],
) -> Dict[str, Any]:
    """
    v5.2.0: Per-row completeness summary for the four view fields.
    Treats None / "" / "N/A" as MISSING.
    """
    fields = (
        "fundamental_view",
        "technical_view",
        "risk_view",
        "value_view",
    )
    present = 0
    missing: List[str] = []
    for f in fields:
        v = row_or_scores.get(f) if isinstance(row_or_scores, Mapping) else None
        if v is None:
            missing.append(f)
            continue
        s = str(v).strip().upper()
        if not s or s == "N/A":
            missing.append(f)
            continue
        present += 1
    total = len(fields)
    ratio = present / total if total else 0.0
    return {
        "present": present,
        "total": total,
        "ratio": round(ratio, 4),
        "missing": missing,
    }


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
    # v5.2.3: default sources from _CONFIG.risk_penalty_strength,
    # which v5.2.3 lowered to 0.40.
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

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


ScoringWeights = ScoreWeights

DEFAULT_WEIGHTS = ScoreWeights()
DEFAULT_FORECASTS = ForecastParameters()


# =============================================================================
# Pure Utility Functions (preserved)
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


def _safe_bool(value: Any) -> bool:
    """v5.2.3: forgiving bool coercion. Recognises common truthy strings."""
    if value is None:
        return False
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return bool(value)
    s = str(value).strip().lower()
    if not s:
        return False
    if s in {"true", "t", "yes", "y", "1", "on"}:
        return True
    if s in {"false", "f", "no", "n", "0", "off", "none", "null", "na", "n/a"}:
        return False
    return False


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


def _as_pct_position_fraction(value: Any) -> Optional[float]:
    """
    v5.2.2: 52W-position-style fields, post data_engine_v2 v5.53.0.

    Engine v5.53.0 stores week_52_position_pct as PERCENT POINTS
    (0-100). This helper enforces the new contract: ALWAYS treats the
    input as percent points and divides by 100, with the call-site
    clamp to [0, 1] absorbing any legacy fraction-format leftovers.

    Only call on fields whose semantic range is bounded [0, 100] as
    percent points (or [0, 1] as a legacy fraction). Do NOT use this
    for unbounded percent fields.
    """
    f = _safe_float(value)
    if f is None:
        return None
    return f / 100.0


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
# Horizon Detection (preserved)
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
# Derived Field Helpers (preserved)
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
# Technical Score (preserved)
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

    if m >= 70 and t >= 55:
        return Signal.BUY.value
    if m <= 30 or t <= 35:
        return Signal.SELL.value
    return Signal.HOLD.value


# =============================================================================
# Forecast Helpers — v5.2.3: illiquid skip + unit-mismatch guard
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


def _empty_forecast_patch() -> Dict[str, Any]:
    """v5.2.3: helper for forecast-skip paths."""
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
    """
    v5.2.3: detect rows where forecast synthesis should be skipped
    entirely. Returns True when ANY of:
      - explicit forecast_unavailable=True flag
      - data_quality ∈ {STALE, MISSING, ERROR} AND no fair value
        AND no API-supplied roi3/roi12
    Other shapes of "missing forecast inputs" still go through the
    normal synthesis path; this is specifically for rows we know are
    structurally unscoreable (delisted, PINK-stale, zero-cap names).
    """
    if _safe_bool(_get(row, "forecast_unavailable", "is_forecast_unavailable")):
        return True

    dq_label = str(_get(row, "data_quality") or "").strip().upper()
    dq_is_unrecoverable = dq_label in {"STALE", "MISSING", "ERROR"}

    if not dq_is_unrecoverable:
        return False

    # When DQ is unrecoverable, only skip if there's truly nothing
    # to synthesize from. (Some stale rows still have a usable fair
    # value or API-supplied ROI; we let those through.)
    fair = _get_float(
        row,
        "intrinsic_value", "fair_value", "target_price",
        "target_mean_price",
    )
    api_roi3 = _as_roi_fraction(_get(row, "expected_roi_3m", "expected_return_3m"))
    api_roi12 = _as_roi_fraction(_get(row, "expected_roi_12m", "expected_return_12m"))
    api_fp12 = _get_float(row, "forecast_price_12m", "expected_price_12m")

    return fair is None and api_roi3 is None and api_roi12 is None and api_fp12 is None


def derive_forecast_patch(
    row: Mapping[str, Any],
    forecasts: ForecastParameters,
) -> Tuple[Dict[str, Any], List[str]]:
    """
    v5.2.3: hardened forecast derivation.

    Two new safety paths vs v5.2.2:
      1. ILLIQUID-AWARE EARLY EXIT — when _is_row_unforecastable
         returns True, return all-None patch immediately. Stops
         the placeholder-forecast leak for delisted / stale rows.
      2. UNIT-MISMATCH SANITY GUARD — after roi12 has been derived
         (whether from API or synthesized), if it's < -50%, treat as
         a unit-conversion bug (subunit pricing, share-class issue),
         suppress the forecast, and emit an error tag. roi1/roi3
         derived from this bad roi12 are also suppressed.
    """
    errors: List[str] = []

    # v5.2.3: ILLIQUID-AWARE EARLY EXIT
    if _is_row_unforecastable(row):
        errors.append("forecast_skipped_unavailable")
        return _empty_forecast_patch(), errors

    patch: Dict[str, Any] = {}

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

    # Derive ROIs from price + forecast prices when missing.
    if price is not None and price > 0:
        if roi12 is None and fp12 is not None and fp12 > 0:
            roi12 = (fp12 / price) - 1.0
        if roi3 is None and fp3 is not None and fp3 > 0:
            roi3 = (fp3 / price) - 1.0
        if roi1 is None and fp1 is not None and fp1 > 0:
            roi1 = (fp1 / price) - 1.0

    # Derive 12m ROI from fair value as last-resort.
    roi12_synthesized_from_fair = False
    if price is not None and price > 0 and roi12 is None and fair is not None and fair > 0:
        roi12 = (fair / price) - 1.0
        roi12_synthesized_from_fair = True

    # v5.2.3: UNIT-MISMATCH SANITY GUARD.
    # If roi12 is below the unit-mismatch floor (-50% by default),
    # something is almost certainly wrong with the unit conversion
    # (LSE/JSE/TA subunit pricing was the audit-flagged case).
    # Suppress the entire forecast chain rather than propagate
    # nonsensical values into valuation_score and the output sheet.
    if roi12 is not None and roi12 < _FORECAST_ROI12_UNIT_MISMATCH_FLOOR:
        errors.append("forecast_suspect_unit_mismatch")
        roi12 = None
        # Suppress derived 1m/3m too IF they came from this bad roi12
        # path (i.e. were not API-supplied independently of roi12).
        # Conservative rule: if the roi12 was synthesized from fair
        # value (not from API-supplied fp12), kill all three. If
        # roi12 came from API-supplied fp12, the API was already
        # internally consistent so 1m/3m can stay.
        if roi12_synthesized_from_fair:
            roi1 = None
            roi3 = None
            fp1 = None
            fp3 = None
            fp12 = None
        else:
            # API-supplied fp12 was the source — also suppress fp12
            # since it's the unit-mismatched value, but keep 1m/3m.
            fp12 = None

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
# Component Scoring — v5.2.3: revenue-collapse haircut on quality
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
    """
    v5.2.3: Multiplicative haircut for quality scores when revenue
    has collapsed YoY. Returns a multiplier in
    [_QUALITY_REVENUE_COLLAPSE_MAX_HAIRCUT, 1.0].

      revenue_growth >= -30%  -> 1.0   (no penalty)
      revenue_growth = -50%   -> 0.80
      revenue_growth = -75%   -> 0.55  (clamped floor)
      revenue_growth <= -75%  -> 0.55  (floor)

    None input or missing growth -> 1.0 (no penalty applied; we don't
    punish data we don't have).
    """
    if revenue_growth is None:
        return 1.0
    if revenue_growth >= _QUALITY_REVENUE_COLLAPSE_START:
        return 1.0

    # Linear ramp from start (1.0x) to floor (max_haircut).
    span = _QUALITY_REVENUE_COLLAPSE_FLOOR - _QUALITY_REVENUE_COLLAPSE_START
    if span >= 0:
        # Defensive: shouldn't happen given our constants
        return 1.0
    progress = (revenue_growth - _QUALITY_REVENUE_COLLAPSE_START) / span
    progress = _clamp(progress, 0.0, 1.0)
    haircut = 1.0 - progress * (1.0 - _QUALITY_REVENUE_COLLAPSE_MAX_HAIRCUT)
    return _clamp(haircut, _QUALITY_REVENUE_COLLAPSE_MAX_HAIRCUT, 1.0)


def compute_quality_score(row: Mapping[str, Any]) -> Optional[float]:
    """
    Compute quality score (0-100).

    v5.2.3 changes:
      - REVENUE-COLLAPSE-AWARE. After computing the standard
        margins/ROE-based combined score, apply a multiplicative
        haircut tied to revenue_growth_yoy. This addresses the
        audit-flagged case (KROS quality=95 with revenue -87%):
        margins can stay strong for a quarter or two after the top
        line collapses, but the company is structurally impaired
        and "95" is misleading. A 0.55x haircut at -75% growth
        brings KROS-like cases down to ~52, still positive but no
        longer aspirational.

    [PRESERVED] Phantom-row gate: when there's no financial signal
    AND the data_quality marker is weak AND completeness < 30%,
    return None so the row is excluded from HOLD/BUY recommendations.
    """
    roe = _as_fraction(_get(row, "roe", "return_on_equity", "returnOnEquity"))
    roa = _as_fraction(_get(row, "roa", "return_on_assets", "returnOnAssets"))
    op_margin = _as_fraction(_get(row, "operating_margin", "operatingMarginTTM"))
    net_margin = _as_fraction(_get(row, "profit_margin", "net_margin", "profitMargins"))
    de = _get_float(row, "debt_to_equity", "debtToEquity")

    has_any_financial = any(x is not None for x in (roe, roa, op_margin, net_margin, de))

    dq_label = str(_get(row, "data_quality") or "").strip().upper()
    dq_is_weak = dq_label in {"", "POOR", "STALE", "MISSING", "ERROR", "UNKNOWN"}

    completeness = _completeness_factor(row)

    # Phantom-row gate (preserved from v5.0.0)
    if not has_any_financial and dq_is_weak and completeness < 0.30:
        return None

    dq = _data_quality_factor(row)
    data_quality_proxy = _clamp(0.55 * dq + 0.45 * completeness, 0.0, 1.0)

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

    # v5.2.3: REVENUE-COLLAPSE-AWARE haircut applied to the final
    # combined 0-1 score before scaling to 100.
    revenue_growth = _as_fraction(
        _get(row, "revenue_growth_yoy", "revenue_growth", "growth_yoy")
    )
    haircut = _revenue_collapse_haircut(revenue_growth)
    combined *= haircut

    return _round(100.0 * _clamp(combined, 0.0, 1.0), 2)


def compute_confidence_score(row: Mapping[str, Any]) -> Tuple[Optional[float], Optional[float]]:
    """
    Compute (confidence_score_100, forecast_confidence_01).

    v5.2.3: NO-FABRICATED-CONFIDENCE.
      Path 1 (preferred, unchanged): use explicit forecast_confidence
        from the row when present.
      Path 2 (fallback, v5.2.3 hardened): only synthesize a confidence
        value when there's a meaningful data-quality signal. Specifically:
          - explicit data_quality marker present, OR
          - completeness >= 0.40, OR
          - >= 1 listed provider in data_sources / providers
        Otherwise return (None, None) so downstream consumers know
        confidence is genuinely unknown rather than the audit-flagged
        ~60% baseline that haunted >90% of rows in v5.2.2.

    Returns:
      (confidence_score_100, forecast_confidence_01)
      Both are None when no signal exists.
    """
    fc = _safe_float(
        _get(row, "forecast_confidence", "ai_confidence", "confidence_score", "confidence")
    )
    if fc is not None:
        fc01 = (fc / 100.0) if fc > 1.5 else fc
        fc01 = _clamp(fc01, 0.0, 1.0)
        return _round(fc01 * 100.0, 2), _round(fc01, 4)

    # v5.2.3: NO-FABRICATED-CONFIDENCE gate.
    has_dq_signal = bool(str(_get(row, "data_quality") or "").strip())
    completeness = _completeness_factor(row)
    provs = row.get("data_sources") or row.get("providers") or []
    try:
        pcount = len(provs) if isinstance(provs, list) else 0
    except Exception:
        pcount = 0

    has_any_signal = (
        has_dq_signal
        or completeness >= _CONFIDENCE_FALLBACK_MIN_COMPLETENESS
        or pcount >= _CONFIDENCE_FALLBACK_MIN_PROVIDERS
    )

    if not has_any_signal:
        return None, None

    dq = _data_quality_factor(row)
    prov_factor = _clamp(pcount / 3.0, 0.0, 1.0)
    conf01 = _clamp(0.55 * dq + 0.35 * completeness + 0.10 * prov_factor, 0.0, 1.0)
    return _round(conf01 * 100.0, 2), _round(conf01, 4)


def compute_valuation_score(row: Mapping[str, Any]) -> Optional[float]:
    """Compute valuation score (0-100). Preserved from v5.0.0."""
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

    if upside_n is None and roi3_n is None and roi12_n is None:
        return None

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
            total += weight * 0.5

    score_01 = total / FULL_WEIGHT
    return _round(100.0 * _clamp(score_01, 0.0, 1.0), 2)


def compute_growth_score(row: Mapping[str, Any]) -> Optional[float]:
    g = _as_fraction(_get(row, "revenue_growth_yoy", "revenue_growth", "growth_yoy"))
    if g is None:
        return None
    return _round(_clamp(((g + 0.30) / 0.60) * 100.0, 0.0, 100.0), 2)


def compute_momentum_score(row: Mapping[str, Any]) -> Optional[float]:
    """
    Compute momentum score (0-100). v5.2.2: switched
    week_52_position_pct read to _as_pct_position_fraction.
    """
    pct = _as_roi_fraction(_get(row, "percent_change", "change_pct", "change_percent"))
    rsi = _get_float(row, "rsi_14", "rsi", "rsi14")
    pos = _as_pct_position_fraction(_get(row, "week_52_position_pct", "position_52w_pct", "week52_position_pct"))
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


# =============================================================================
# View Derivation (preserved from v5.0.0)
# =============================================================================

def derive_fundamental_view(
    quality: Optional[float],
    growth: Optional[float],
) -> Optional[str]:
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
    if q >= 70.0 and growth is None:
        return "BULLISH"
    return "NEUTRAL"


def derive_technical_view(
    technical: Optional[float],
    momentum: Optional[float],
    rsi_label: Optional[str] = None,
) -> Optional[str]:
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
    if risk is None:
        return None
    if risk <= _CONFIG.risk_low_threshold:
        return "LOW"
    if risk <= _CONFIG.risk_moderate_threshold:
        return "MODERATE"
    return "HIGH"


def derive_value_view(
    valuation: Optional[float],
    upside_pct: Optional[float] = None,
) -> Optional[str]:
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


# =============================================================================
# v5.2.3 — Recommendation coherence guard
# =============================================================================

def _coherence_guard_recommendation(
    canonical_rec: str,
    roi3: Optional[float],
    confidence100: Optional[float],
    view_prefix: str,
) -> Tuple[Optional[str], Optional[str]]:
    """
    v5.2.3: Cross-check the recommendation label against the row's
    own forecasts. Returns (downgraded_rec, downgraded_reason) if
    a downgrade is warranted, or (None, None) to leave the original
    intact.

    Rule:
      if canonical_rec ∈ {BUY, STRONG_BUY}
         AND roi3 < _COHERENCE_ROI3_FLOOR_FRACTION
         AND confidence100 < _COHERENCE_CONFIDENCE_FLOOR
      -> downgrade to HOLD with explicit reason

    Strong signals (high confidence) override the guard, since a
    high-conviction BUY with mildly negative 3M ROI may be a
    legitimate accumulation call. Low-confidence BUYs in the face of
    negative forecasts are the audit-flagged pattern.
    """
    if canonical_rec not in ("BUY", "STRONG_BUY"):
        return None, None
    if roi3 is None:
        return None, None
    if roi3 >= _COHERENCE_ROI3_FLOOR_FRACTION:
        return None, None

    conf_for_guard = confidence100 if confidence100 is not None else 55.0
    if conf_for_guard >= _COHERENCE_CONFIDENCE_FLOOR:
        return None, None

    roi3_pct_disp = _round(roi3 * 100.0, 1)
    conf_pct_disp = _round(conf_for_guard, 0)

    reason = (
        f"{view_prefix} \u2192 HOLD "
        f"(coherence guard: {canonical_rec} \u2192 HOLD; "
        f"3M ROI {roi3_pct_disp}% with AI confidence {conf_pct_disp}%)"
    )
    return "HOLD", reason


# =============================================================================
# Recommendation
# =============================================================================

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
    """
    Compute view-aware 5-tier recommendation.

    v5.2.3 changes:
      - RECOMMENDATION-COHERENCE-GUARD applied after reco_normalize
        returns its label. See _coherence_guard_recommendation for
        the rule.

    v5.2.1 / v5.2.0 (preserved): all four views derived at the top so
    every return path carries the parseable view prefix. Final pass
    through _align_reason_to_canonical_recommendation before return.
    """
    if fundamental_view is None:
        fundamental_view = derive_fundamental_view(quality, growth)
    if technical_view is None:
        technical_view = derive_technical_view(technical, momentum, rsi_label)
    if risk_view is None:
        risk_view = derive_risk_view(risk)
    if value_view is None:
        value_view = derive_value_view(valuation, upside_pct)

    view_prefix = _build_view_prefix(
        fundamental_view, technical_view, risk_view, value_view
    )

    if overall is None and quality is None and valuation is None:
        return (
            "HOLD",
            f"{view_prefix} \u2192 HOLD (insufficient data to score reliably)",
        )

    c = confidence100 if confidence100 is not None else 55.0
    if c < 35.0:
        return (
            "HOLD",
            f"{view_prefix} \u2192 HOLD "
            f"(low AI confidence {_round(c, 1)}%)",
        )

    if horizon == Horizon.DAY:
        t = technical if technical is not None else 50.0
        m = momentum if momentum is not None else 50.0
        r = risk if risk is not None else 50.0
        if t >= 80 and m >= 75 and r <= 45:
            return (
                "STRONG_BUY",
                f"{view_prefix} \u2192 STRONG_BUY "
                f"(day-trade setup: Tech={_round(t, 1)}, "
                f"Momentum={_round(m, 1)}, Risk=Low ({_round(r, 1)}))",
            )
        if t < 35 or m < 30:
            return (
                "SELL",
                f"{view_prefix} \u2192 SELL "
                f"(day-trade breakdown: Tech={_round(t, 1)}, "
                f"Momentum={_round(m, 1)})",
            )

    try:
        from core.reco_normalize import recommendation_from_views  # noqa: WPS433
    except ImportError:
        try:
            from reco_normalize import recommendation_from_views  # noqa: WPS433
        except ImportError:
            logger.warning(
                "core.reco_normalize unavailable; defaulting to HOLD."
            )
            return (
                "HOLD",
                f"{view_prefix} \u2192 HOLD "
                "(recommendation engine unavailable)",
            )

    rec, reason = recommendation_from_views(
        fundamental=fundamental_view,
        technical=technical_view,
        risk=risk_view,
        value=value_view,
        score=overall,
        conviction=conviction,
        sector_relative=sector_relative,
    )

    canonical_rec = normalize_recommendation_code(rec)

    # v5.2.3: COHERENCE GUARD
    guarded_rec, guarded_reason = _coherence_guard_recommendation(
        canonical_rec, roi3, confidence100, view_prefix
    )
    if guarded_rec is not None and guarded_reason is not None:
        return guarded_rec, guarded_reason

    aligned_reason = _align_reason_to_canonical_recommendation(reason, canonical_rec)
    return canonical_rec, aligned_reason


# =============================================================================
# Recommendation Normalization (preserved)
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
    key = _normalize_key(label)
    if not key:
        return "HOLD"
    if key in _CANONICAL_RECO:
        return key

    if key in _LOCAL_RECO_ALIASES:
        return _LOCAL_RECO_ALIASES[key]

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

    return "HOLD"


# =============================================================================
# v5.1.0: insights_builder lazy import helper (preserved)
# =============================================================================

def _import_insights_builder():
    try:
        from core import insights_builder as _ib  # noqa: WPS433
        return _ib
    except ImportError:
        try:
            import insights_builder as _ib  # noqa: WPS433
            return _ib
        except ImportError:
            return None


# =============================================================================
# Main Scoring Function
# =============================================================================

def compute_scores(row: Dict[str, Any], settings: Any = None) -> Dict[str, Any]:
    """
    Score a single row.

    v5.2.3 changes:
      - compute_confidence_score may now return (None, None); the
        downstream penalty calc and bucket helpers already handle
        this correctly.
      - compute_quality_score may return a haircut-adjusted value.
      - compute_recommendation now applies the coherence guard.
      - derive_forecast_patch may early-exit (illiquid skip) or
        suppress unit-mismatched roi12/roi3/roi1.

    [PRESERVED] All v5.2.0 / v5.2.1 / v5.2.2 mechanics.
    """
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

    sig_weight_total = sum(w for w, _ in base_parts)
    if base_parts and (len(base_parts) >= 2 or sig_weight_total >= 0.40):
        wsum = sig_weight_total
        base01 = sum(w * v for w, v in base_parts) / max(1e-9, wsum)
        overall_raw = _round(100.0 * _clamp(base01, 0.0, 1.0), 2)

        risk01 = (risk / 100.0) if risk is not None else 0.50
        # v5.2.3: when conf01 is None (no signal), treat as
        # mid-confidence for penalty calc only. We don't penalize
        # rows for missing confidence — that would be double-counting
        # the same data gap.
        conf01_used = conf01 if conf01 is not None else 0.55

        # v5.2.3: with risk_penalty_strength=0.40 the penalty for a
        # high-risk stock (risk01=0.7) becomes:
        #   risk_pen = 1 - 0.40 * (0.7 * 0.70) = 1 - 0.196 = 0.80
        # was 0.73 in v5.2.2 with strength=0.55.
        risk_pen = _clamp(1.0 - weights.risk_penalty_strength * (risk01 * 0.70), 0.0, 1.0)
        conf_pen = _clamp(1.0 - weights.confidence_penalty_strength * ((1.0 - conf01_used) * 0.80), 0.0, 1.0)
        penalty_factor = _round(risk_pen * conf_pen, 4)

        base01 *= (risk_pen * conf_pen)
        overall = _round(100.0 * _clamp(base01, 0.0, 1.0), 2)
    else:
        overall = None
        overall_raw = None
        penalty_factor = None
        insufficient_inputs = True
        scoring_errors.append("insufficient_scoring_inputs")

    rb = risk_bucket(risk)
    cb = confidence_bucket(conf01)

    fundamental_view_raw = derive_fundamental_view(quality, growth)
    technical_view_raw = derive_technical_view(tech_score, momentum, rsi_sig)
    risk_view_raw = derive_risk_view(risk)
    value_view_raw = derive_value_view(valuation, usp)

    roi3 = _as_roi_fraction(working.get("expected_roi_3m"))
    roi1 = _as_roi_fraction(working.get("expected_roi_1m"))
    roi12 = _as_roi_fraction(working.get("expected_roi_12m"))

    # ---- v5.1.0: compute conviction BEFORE the recommendation -------
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
            scoring_errors.append(f"conviction_failed: {type(exc).__name__}")

    if insufficient_inputs:
        view_prefix_for_suppress = _build_view_prefix(
            fundamental_view_raw, technical_view_raw,
            risk_view_raw, value_view_raw,
        )
        rec, reason = "HOLD", (
            f"{view_prefix_for_suppress} \u2192 HOLD "
            "(insufficient scoring inputs)"
        )
    else:
        rec, reason = compute_recommendation(
            overall, risk, confidence100, roi3,
            horizon=horizon, technical=tech_score, momentum=momentum,
            roi1=roi1, roi12=roi12,
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

    st_signal_val = short_term_signal(tech_score, momentum, risk, horizon)
    period_label = invest_period_label(horizon, hdays)

    # ---- v5.1.0: build per-row insights ------------------------------
    top_factors_str: str = ""
    top_risks_str: str = ""
    pos_hint: str = ""
    structured_reason: str = reason

    if ib is not None and not insufficient_inputs:
        try:
            synthetic_row: Dict[str, Any] = dict(working)
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
            scoring_errors.append(f"insights_failed: {type(exc).__name__}")

    structured_reason = _align_reason_to_canonical_recommendation(
        structured_reason, canonical_rec
    )

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
        fundamental_view=_view_or_na(fundamental_view_raw),
        technical_view=_view_or_na(technical_view_raw),
        risk_view=_view_or_na(risk_view_raw),
        value_view=_view_or_na(value_view_raw),
        recommendation=canonical_rec,
        recommendation_reason=structured_reason,
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
# Ranking + Batch Scoring (preserved from v5.2.0)
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
    """
    Score every row and rank by overall_score.

    [PRESERVED] After per-row scoring, runs a batch-level pass via
    insights_builder.enrich_rows_with_insights() to compute
    sector_relative_score and rebuild recommendation_reason with
    sector-adjusted badges.

    [PRESERVED] After insights_builder returns, each row's
    recommendation_reason is re-aligned via
    _align_reason_to_canonical_recommendation, and view fields are
    coerced via _view_or_na.
    """
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
        rec = row.get("recommendation")
        reason = row.get("recommendation_reason")
        if rec and reason:
            canonical_rec = normalize_recommendation_code(rec)
            row["recommendation"] = canonical_rec
            row["recommendation_reason"] = _align_reason_to_canonical_recommendation(
                str(reason), canonical_rec
            )
        for view_key in ("fundamental_view", "technical_view", "risk_view", "value_view"):
            if view_key in row:
                row[view_key] = _view_or_na(row.get(view_key))

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
    "derive_fundamental_view",
    "derive_technical_view",
    "derive_risk_view",
    "derive_value_view",
    "ScoringError",
    "InvalidHorizonError",
    "MissingDataError",
    "score_views_completeness",
]
