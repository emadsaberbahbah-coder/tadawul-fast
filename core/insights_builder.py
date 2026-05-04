#!/usr/bin/env python3
# core/insights_builder.py
"""
================================================================================
Insights Builder — v1.0.0
================================================================================
PRODUCTION-SAFE • PURE-FUNCTION • NO-NETWORK • SCHEMA-AWARE • IMPORT-SAFE

Purpose
-------
Build derived "insights" fields on top of a scored row, beyond what the raw
component-scoring pipeline produces. These fields are the bridge between
numeric scoring and a useful, differentiated recommendation:

  - conviction_score (0-100): how strongly we believe in the recommendation.
    Distinct from `forecast_confidence` (which measures DATA quality) and
    `confidence_score` (the model's confidence in its own forecast).
    High conviction = views all aligned + score far from 50 (not borderline)
    + forecast confidence is itself solid + input data is reasonably complete.

  - sector_relative_score (0-100): percentile rank of overall_score within
    the row's sector cohort. A 70 in semiconductors may rank P40 within
    semis (mediocre); a 60 in utilities may rank P85 (excellent). Catches
    sector-rotation effects that absolute thresholds miss.

  - top_factors: top-N (default 3) component scores ranked by their actual
    contribution to overall_score (component_score * weight). Pipe-separated
    string for sheet display, e.g. "Quality 82 | Value 75 | Momentum 68".

  - top_risks: top-N (default 2) risk factors flagged from the row. Rule-
    driven: high vol, big drawdown, high leverage, overbought RSI, etc.

  - position_size_hint: heuristic position-size anchor as a percentage range,
    derived from (recommendation, conviction). NOT financial advice — these
    are heuristic anchors only, intended for risk-managed sizing.

  - structured recommendation_reason: a single string composed of the badge
    line ("Action: BUY, Conv 78/100, Sector-Adj 84"), the top factors line,
    the top risks line, and the underlying base reason. Pipe-joined so it
    fits in a Sheet cell.

Cross-module integration
------------------------
- Reads from rows produced by core.scoring v5.1.0+ (component scores, views,
  recommendation, recommendation_reason, forecast_confidence).
- Schema columns added in core.sheets.schema_registry v2.6.0 (Insights group):
    sector_relative_score, conviction_score, top_factors, top_risks,
    position_size_hint.
- Conviction is fed back into core.reco_normalize v7.1.0+ via the new
  `conviction` kwarg of Recommendation.from_views() to enforce conviction
  floors (STRONG_BUY -> BUY when conviction < 60; BUY -> HOLD when < 45).

Pure-function design
--------------------
Every function is deterministic given its inputs. No global mutable state.
No I/O. Safe to call from sync or async contexts. Safe to import at module
load (no side effects).

Public API
----------
- VERSION, INSIGHTS_BUILDER_VERSION
- InsightsBundle (dataclass)
- compute_conviction_score()
- compute_sector_relative_score()
- derive_top_factors()
- derive_top_risks()
- build_position_size_hint()
- build_recommendation_reason()
- build_insights()              -- per-row builder
- enrich_row_with_insights()    -- per-row enricher (in-place option)
- enrich_rows_with_insights()   -- batch enricher (computes sector cohorts)
================================================================================
"""

from __future__ import annotations

import math
from dataclasses import dataclass
from typing import Any, Dict, List, Mapping, Optional, Sequence, Tuple

# =============================================================================
# Version
# =============================================================================

VERSION = "1.0.0"
INSIGHTS_BUILDER_VERSION = VERSION


# =============================================================================
# Constants & lookup tables
# =============================================================================

# Pretty labels for top_factors output. Keep in sync with the component-score
# keys actually emitted by core.scoring.AssetScores.
_FACTOR_LABELS: Dict[str, str] = {
    "valuation_score": "Valuation",
    "momentum_score": "Momentum",
    "quality_score": "Quality",
    "growth_score": "Growth",
    "value_score": "Value",
    "opportunity_score": "Opportunity",
    "technical_score": "Technical",
}

# Default weights mirroring the MONTH-horizon profile in core.scoring. Used
# when the caller doesn't pass an explicit weights dict (e.g. ad-hoc usage).
# For batch runs the caller should pass the actual horizon-aware weights so
# top_factors reflects the real contribution to overall_score.
_DEFAULT_FACTOR_WEIGHTS: Dict[str, float] = {
    "valuation_score": 0.30,
    "momentum_score": 0.25,
    "quality_score": 0.20,
    "growth_score": 0.15,
    "opportunity_score": 0.10,
    "technical_score": 0.00,
}

# Risk-factor rules: (row_key, threshold, comparator, label, severity_unit).
# severity_unit lets us scale the "severity" used for ranking when multiple
# risks fire — keeps comparable units across vol, leverage, etc.
_RISK_FACTOR_RULES: Tuple[Tuple[str, float, str, str, float], ...] = (
    ("volatility_30d",   0.40, ">=", "High 30D volatility",         0.30),
    ("volatility_90d",   0.40, ">=", "Sustained 90D volatility",    0.30),
    ("max_drawdown_1y", -0.30, "<=", "Severe 1Y drawdown",          0.30),
    ("debt_to_equity",   2.0,  ">=", "High leverage",               2.0),
    ("beta_5y",          1.5,  ">=", "Elevated beta",               1.0),
    ("rsi_14",           75.0, ">=", "Overbought (RSI)",            10.0),
    ("rsi_14",           25.0, "<=", "Oversold (RSI)",              10.0),
    ("profit_margin",   -0.05, "<=", "Negative net margin",         0.10),
    ("var_95_1d",        0.05, ">=", "Elevated 1D VaR",             0.04),
)

# Position-size hint table: (rec, conviction_lo, conviction_hi) -> hint.
# These are heuristic anchors only — not financial advice. Each rec maps
# multiple conviction tiers because the right size depends on confidence
# in the call.
_POSITION_HINTS: Dict[str, Tuple[Tuple[float, float, str], ...]] = {
    "STRONG_BUY": (
        (75.0, 101.0, "4-6% of portfolio"),
        (60.0,  75.0, "3-5% of portfolio"),
        ( 0.0,  60.0, "2-3% of portfolio (low conviction)"),
    ),
    "BUY": (
        (75.0, 101.0, "3-5% of portfolio"),
        (60.0,  75.0, "2-4% of portfolio"),
        ( 0.0,  60.0, "1-3% of portfolio (low conviction)"),
    ),
    "HOLD": (
        (0.0, 101.0, "Hold existing position; no new capital"),
    ),
    "REDUCE": (
        (60.0, 101.0, "Trim to 50% of current position"),
        ( 0.0,  60.0, "Trim 25-50% of position"),
    ),
    "SELL": (
        (60.0, 101.0, "Exit position fully"),
        ( 0.0,  60.0, "Exit 75% of position"),
    ),
}

# Minimum cohort size for sector-relative ranking to be meaningful. Smaller
# cohorts produce noisy percentiles, so we return None instead of misleading
# the caller.
_MIN_SECTOR_COHORT = 3

# Core completeness fields used when caller doesn't pass completeness explicitly.
_COMPLETENESS_FIELDS: Tuple[str, ...] = (
    "symbol", "current_price", "previous_close", "volume", "market_cap",
    "pe_ttm", "rsi_14", "volatility_30d", "expected_roi_3m",
    "forecast_confidence",
)


# =============================================================================
# Pure utilities (private)
# =============================================================================

def _safe_float(v: Any) -> Optional[float]:
    """Coerce v to float, returning None for None/NaN/inf/non-numeric."""
    if v is None:
        return None
    try:
        if isinstance(v, bool):
            return None
        f = float(v)
        if math.isnan(f) or math.isinf(f):
            return None
        return f
    except (TypeError, ValueError):
        return None


def _clamp(v: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, v))


def _round(v: Optional[float], n: int = 2) -> Optional[float]:
    if v is None:
        return None
    try:
        return round(float(v), n)
    except (TypeError, ValueError):
        return None


def _norm_pct_or_fraction(v: Optional[float]) -> Optional[float]:
    """If v looks like a percent (>1.5 or <-1.5), convert to fraction."""
    if v is None:
        return None
    if abs(v) > 1.5:
        return v / 100.0
    return v


def _view_to_signed(v: Optional[str]) -> Optional[float]:
    """
    Map a view token to a signed alignment value:
      Bullish-axis (BULLISH / LOW / CHEAP)         -> +1.0
      Bearish-axis (BEARISH / HIGH / EXPENSIVE)    -> -1.0
      Mid-axis     (NEUTRAL / MODERATE / FAIR)     ->  0.0
      Anything else / missing                      -> None
    """
    if v is None:
        return None
    try:
        token = str(v).strip().upper()
    except Exception:
        return None
    if not token:
        return None
    if token in ("BULLISH", "LOW", "CHEAP"):
        return 1.0
    if token in ("BEARISH", "HIGH", "EXPENSIVE"):
        return -1.0
    if token in ("NEUTRAL", "MODERATE", "FAIR"):
        return 0.0
    return None


# =============================================================================
# Conviction Score
# =============================================================================

def _view_agreement_score(
    fundamental: Optional[str],
    technical: Optional[str],
    risk: Optional[str],
    value: Optional[str],
) -> float:
    """
    Compute view-agreement component (0..1).

    Logic: convert each view to its signed alignment, take the mean, and
    use |mean| as agreement. All four bullish -> mean=+1, |mean|=1.0.
    Two bullish + two bearish -> mean=0, |mean|=0.0 (max disagreement).
    All four neutral -> mean=0, |mean|=0.0 (true neutrality, treated as
    low agreement because there's no directional signal). To avoid
    penalizing the mid-case unfairly we add a baseline of 0.5 when any
    views are present.

    Returns 0.0 (no views) up to 1.0 (perfect alignment).
    """
    values = [
        x for x in (
            _view_to_signed(fundamental),
            _view_to_signed(technical),
            _view_to_signed(risk),
            _view_to_signed(value),
        ) if x is not None
    ]
    if not values:
        return 0.5  # no information — neutral

    mean = sum(values) / len(values)
    return _clamp(0.5 + 0.5 * abs(mean), 0.0, 1.0)


def compute_conviction_score(
    *,
    overall_score: Optional[float],
    fundamental_view: Optional[str] = None,
    technical_view: Optional[str] = None,
    risk_view: Optional[str] = None,
    value_view: Optional[str] = None,
    forecast_confidence: Optional[float] = None,
    completeness: Optional[float] = None,
) -> Optional[float]:
    """
    Compute conviction score (0-100): how strongly we believe in the call.

    Composition:
      40% — view agreement (all 4 aligned -> high)
      30% — score extremity (overall far from 50 -> high; borderline -> low)
      20% — forecast_confidence (model's own confidence in its forecast)
      10% — completeness (how much data was available for the score)

    Weights sum to 1.00 only when forecast_confidence is non-None. When
    forecast_confidence is missing, its 20% redistributes proportionally
    across the remaining components. Math: weighted_sum / total_weight.

    Returns None if overall_score is None — without an overall there's
    no recommendation to be confident about.
    """
    if overall_score is None:
        return None

    overall = _safe_float(overall_score)
    if overall is None:
        return None

    # 1) View agreement
    agree = _view_agreement_score(
        fundamental_view, technical_view, risk_view, value_view
    )

    # 2) Score extremity (distance from 50, normalized to 0..1)
    extremity = _clamp(abs(overall - 50.0) / 50.0, 0.0, 1.0)

    # 3) Forecast confidence
    fc = _safe_float(forecast_confidence)
    if fc is not None:
        if fc > 1.5:
            fc = fc / 100.0
        fc = _clamp(fc, 0.0, 1.0)

    # 4) Completeness
    comp = _safe_float(completeness)
    comp = _clamp(comp, 0.0, 1.0) if comp is not None else 0.5

    parts: List[Tuple[float, float]] = [
        (0.40, agree),
        (0.30, extremity),
        (0.10, comp),
    ]
    if fc is not None:
        parts.append((0.20, fc))

    total_weight = sum(w for w, _ in parts)
    weighted = sum(w * v for w, v in parts) / max(1e-9, total_weight)
    return _round(100.0 * _clamp(weighted, 0.0, 1.0), 2)


# =============================================================================
# Sector-Relative Score
# =============================================================================

def compute_sector_relative_score(
    overall_score: Optional[float],
    sector_scores: Sequence[Any],
) -> Optional[float]:
    """
    Compute percentile rank of overall_score within sector_scores (0-100).

    Returns:
        100 = highest in sector
         50 = median
          0 = lowest

    Returns None if:
        - overall_score is None
        - cohort has fewer than _MIN_SECTOR_COHORT (3) valid scores

    Uses the "midpoint" percentile method: rank_below + (rank_equal / 2),
    which avoids the off-by-one issue of strictly-less-than ranking.
    """
    overall = _safe_float(overall_score)
    if overall is None:
        return None

    valid: List[float] = []
    for s in sector_scores or ():
        f = _safe_float(s)
        if f is not None:
            valid.append(f)

    if len(valid) < _MIN_SECTOR_COHORT:
        return None

    valid.sort()
    n = len(valid)
    rank_below = sum(1 for s in valid if s < overall)
    rank_equal = sum(1 for s in valid if s == overall)
    pct = (rank_below + rank_equal / 2.0) / n
    return _round(100.0 * _clamp(pct, 0.0, 1.0), 2)


# =============================================================================
# Top Factors
# =============================================================================

def derive_top_factors(
    row: Mapping[str, Any],
    *,
    n: int = 3,
    weights: Optional[Mapping[str, float]] = None,
) -> str:
    """
    Identify top-N factors by contribution to overall_score.

    Contribution = component_score * weight. So a 90 in a 0.05-weight factor
    (contribution 4.5) ranks BELOW a 60 in a 0.30-weight factor
    (contribution 18.0). This is the only ranking that correctly answers
    "which factors moved the score the most".

    Returns a pipe-separated string like:
        "Quality 82 | Value 75 | Momentum 68"

    Returns "" if no component scores are present in row.
    """
    if not row:
        return ""

    w_map: Dict[str, float] = {}
    src = weights if weights is not None else _DEFAULT_FACTOR_WEIGHTS
    for k, v in src.items():
        wf = _safe_float(v)
        if wf is not None and wf > 0:
            w_map[k] = wf

    if not w_map:
        # Fall back to defaults if caller passed an empty/all-zero dict
        w_map = dict(_DEFAULT_FACTOR_WEIGHTS)

    contributions: List[Tuple[str, float, float]] = []
    for key, weight in w_map.items():
        val = _safe_float(row.get(key))
        if val is None:
            continue
        contributions.append((key, val, val * weight))

    if not contributions:
        return ""

    contributions.sort(key=lambda x: x[2], reverse=True)
    top = contributions[: max(1, n)]

    parts: List[str] = []
    for key, val, _ in top:
        label = _FACTOR_LABELS.get(key, key.replace("_score", "").title())
        parts.append(f"{label} {int(round(val))}")

    # Comma-separated so the outer pipe-joined recommendation_reason
    # has unambiguous structural separators.
    return ", ".join(parts)


# =============================================================================
# Top Risks
# =============================================================================

def derive_top_risks(
    row: Mapping[str, Any],
    *,
    n: int = 2,
) -> str:
    """
    Identify top-N risk factors fired by the row.

    Each rule in _RISK_FACTOR_RULES checks one field against a threshold.
    A fired rule contributes a "severity" measured in the rule's natural
    unit and rescaled to a comparable 0..1 by the per-rule severity_unit.
    The most severe risks win the top-N slots.

    Returns a comma-separated string like:
        "High 30D volatility, High leverage"

    The inner separator is a comma (not a pipe) so that this string can be
    safely embedded inside the outer pipe-joined recommendation_reason
    without creating ambiguity.

    Returns "" if no risks fire.
    """
    if not row:
        return ""

    risks: List[Tuple[str, float]] = []
    seen_labels = set()

    for key, threshold, comp, label, severity_unit in _RISK_FACTOR_RULES:
        val = _safe_float(row.get(key))
        if val is None:
            continue

        # For pct fields stored as percent (e.g. 35 instead of 0.35),
        # normalize to fraction so threshold comparisons work consistently.
        if key in ("volatility_30d", "volatility_90d", "max_drawdown_1y",
                   "profit_margin", "var_95_1d"):
            val = _norm_pct_or_fraction(val)
            if val is None:
                continue

        triggered = False
        if comp == ">=":
            triggered = val >= threshold
        elif comp == "<=":
            triggered = val <= threshold

        if triggered and label not in seen_labels:
            seen_labels.add(label)
            severity = abs(val - threshold) / max(1e-9, severity_unit)
            risks.append((label, severity))

    # Risk-score override: if the model's own risk score is very high, surface it
    risk_score = _safe_float(row.get("risk_score"))
    if risk_score is not None and risk_score >= 75.0:
        rs_label = f"High overall risk ({int(risk_score)})"
        if rs_label not in seen_labels:
            risks.append((rs_label, (risk_score - 75.0) / 25.0))

    if not risks:
        return ""

    risks.sort(key=lambda x: x[1], reverse=True)
    top = risks[: max(1, n)]
    return ", ".join(label for label, _ in top)


# =============================================================================
# Position Size Hint
# =============================================================================

def build_position_size_hint(
    recommendation: Any,
    conviction: Optional[float],
) -> str:
    """
    Build a heuristic position-size hint from (recommendation, conviction).

    NOT financial advice. These are anchor ranges for risk-managed sizing
    within a diversified portfolio. Real sizing depends on portfolio
    concentration, correlation, liquidity, mandate, etc.

    Returns "" if recommendation is unknown or empty.
    """
    if not recommendation:
        return ""

    rec = str(recommendation).strip().upper().replace("-", "_").replace(" ", "_")
    if rec not in _POSITION_HINTS:
        return ""

    conv = _safe_float(conviction)
    if conv is None:
        conv = 50.0  # neutral default
    conv = _clamp(conv, 0.0, 100.0)

    for lo, hi, hint in _POSITION_HINTS[rec]:
        if lo <= conv < hi:
            return hint

    # Fallback to first tier (shouldn't be reached if tables cover 0..101)
    tiers = _POSITION_HINTS[rec]
    return tiers[0][2] if tiers else ""


# =============================================================================
# Structured Recommendation Reason
# =============================================================================

import re as _re

# Pattern matches the structured-reason prefix produced by this function.
# Used to strip prior structured wrappers so the function is idempotent
# under repeated calls (the batch path may invoke it twice — once at
# scoring time, once at sector-cohort time).
_STRUCTURED_PREFIX_RE = _re.compile(
    r"^Action:\s*[A-Z_]+(?:,\s*Conv\s+\d+/100)?(?:,\s*Sector-Adj\s+-?\d+)?\s*",
    flags=_re.IGNORECASE,
)

# Pattern matches the "[Conv NN, Sector-Adj NN]" badge that
# core.reco_normalize._attach_metrics may append to the END of a reason.
# Stripped before wrapping so we don't end up with duplicate badges.
_RECO_METRICS_BADGE_RE = _re.compile(
    r"\s*\[(?:Conv\s+\d+|Sector-Adj\s+-?\d+)(?:,\s*(?:Conv\s+\d+|Sector-Adj\s+-?\d+))*\]\s*$",
    flags=_re.IGNORECASE,
)


def _strip_structured_wrappers(text: str) -> str:
    """
    Strip any prior structured-reason wrapping so we can rebuild cleanly.

    Removes:
      - leading "Action: REC, Conv NN/100, Sector-Adj NN | Top factors: ... | Top risks: ... | "
      - trailing "[Conv NN, Sector-Adj NN]" attached by reco_normalize

    Returns the underlying base-reason. Idempotent and safe on already-clean
    input (returns input unchanged).
    """
    if not text:
        return ""

    s = str(text).strip()

    # Drop leading structured prefix (Action / Conv / Sector-Adj line plus
    # any "Top factors:" and "Top risks:" segments) by walking pipe-joined
    # parts and keeping only those that don't match our prefix patterns.
    parts = [p.strip() for p in s.split("|")]
    kept: List[str] = []
    for p in parts:
        if not p:
            continue
        if p.upper().startswith("ACTION:"):
            continue
        if p.lower().startswith("top factors:"):
            continue
        if p.lower().startswith("top risks:"):
            continue
        kept.append(p)
    s = " | ".join(kept).strip()

    # Drop trailing reco_normalize metric badge if any
    s = _RECO_METRICS_BADGE_RE.sub("", s).strip()

    return s


def build_recommendation_reason(
    *,
    recommendation: Any,
    conviction: Optional[float] = None,
    sector_relative: Optional[float] = None,
    top_factors: Optional[str] = None,
    top_risks: Optional[str] = None,
    base_reason: Optional[str] = None,
) -> str:
    """
    Build a structured recommendation reason string.

    Format (pipe-joined to fit a single sheet cell):
        "Action: <REC>, Conv NN/100, Sector-Adj NN | Top factors: ... | Top risks: ... | <base_reason>"

    Components are added only when their data is available, so a row with
    no sector context and no risks fired produces a shorter (but still
    well-formed) string.

    IDEMPOTENT: if `base_reason` is already a structured reason produced by
    this function, the structured wrapping is stripped before re-building,
    so calling this twice never doubles up the badges. Same for any
    "[Conv NN]" / "[Sector-Adj NN]" tails that core.reco_normalize may have
    attached.
    """
    parts: List[str] = []

    rec_text = str(recommendation or "HOLD").strip().upper()
    badge_parts = [f"Action: {rec_text}"]
    if conviction is not None:
        cv = _safe_float(conviction)
        if cv is not None:
            badge_parts.append(f"Conv {int(round(cv))}/100")
    if sector_relative is not None:
        sr = _safe_float(sector_relative)
        if sr is not None:
            badge_parts.append(f"Sector-Adj {int(round(sr))}")
    parts.append(", ".join(badge_parts))

    if top_factors:
        tf = str(top_factors).strip()
        if tf:
            parts.append(f"Top factors: {tf}")

    if top_risks:
        tr = str(top_risks).strip()
        if tr:
            parts.append(f"Top risks: {tr}")

    if base_reason:
        clean = _strip_structured_wrappers(str(base_reason))
        if clean and not any(clean in p for p in parts):
            parts.append(clean)

    return " | ".join(p for p in parts if p)


# =============================================================================
# Bundle / batch builders
# =============================================================================

@dataclass(slots=True)
class InsightsBundle:
    """Container for the five insight fields produced by build_insights."""
    conviction_score: Optional[float] = None
    sector_relative_score: Optional[float] = None
    top_factors: str = ""
    top_risks: str = ""
    position_size_hint: str = ""
    recommendation_reason: str = ""

    def to_dict(self) -> Dict[str, Any]:
        return {
            "conviction_score": self.conviction_score,
            "sector_relative_score": self.sector_relative_score,
            "top_factors": self.top_factors,
            "top_risks": self.top_risks,
            "position_size_hint": self.position_size_hint,
            "recommendation_reason": self.recommendation_reason,
        }


def _completeness_from_row(row: Mapping[str, Any]) -> float:
    """Quick heuristic completeness score from core data fields."""
    if not row:
        return 0.0
    present = 0
    for k in _COMPLETENESS_FIELDS:
        v = row.get(k)
        if v not in (None, "", [], {}):
            present += 1
    return present / max(1, len(_COMPLETENESS_FIELDS))


def build_insights(
    row: Mapping[str, Any],
    *,
    sector_scores: Optional[Sequence[Any]] = None,
    weights: Optional[Mapping[str, float]] = None,
    base_reason: Optional[str] = None,
) -> InsightsBundle:
    """
    Build all 6 insights fields for a single row.

    Args:
        row:            scored row dict (must contain overall_score and views
                        for full conviction; missing fields are tolerated).
        sector_scores:  optional cohort of overall_scores from same sector.
                        If omitted or too small, sector_relative_score=None.
        weights:        actual horizon-aware weights used for the score; if
                        omitted, defaults to monthly weights. Used only by
                        derive_top_factors.
        base_reason:    optional base recommendation reason from upstream
                        (e.g. core.scoring's recommendation_reason). Will be
                        appended to the structured reason.

    Returns:
        InsightsBundle with populated fields. Each field is independently
        nullable — partial data produces a partial bundle, never a crash.
    """
    overall = _safe_float(row.get("overall_score"))
    fund_view = row.get("fundamental_view")
    tech_view = row.get("technical_view")
    risk_view = row.get("risk_view")
    value_view = row.get("value_view")
    fc = _safe_float(row.get("forecast_confidence"))

    completeness = _completeness_from_row(row)

    conviction = compute_conviction_score(
        overall_score=overall,
        fundamental_view=fund_view,
        technical_view=tech_view,
        risk_view=risk_view,
        value_view=value_view,
        forecast_confidence=fc,
        completeness=completeness,
    )

    sector_rel: Optional[float] = None
    if sector_scores is not None:
        sector_rel = compute_sector_relative_score(overall, sector_scores)

    top_f = derive_top_factors(row, weights=weights)
    top_r = derive_top_risks(row)

    rec = row.get("recommendation") or "HOLD"
    pos_hint = build_position_size_hint(rec, conviction)

    reason = build_recommendation_reason(
        recommendation=rec,
        conviction=conviction,
        sector_relative=sector_rel,
        top_factors=top_f,
        top_risks=top_r,
        base_reason=base_reason if base_reason is not None else row.get("recommendation_reason"),
    )

    return InsightsBundle(
        conviction_score=conviction,
        sector_relative_score=sector_rel,
        top_factors=top_f,
        top_risks=top_r,
        position_size_hint=pos_hint,
        recommendation_reason=reason,
    )


def enrich_row_with_insights(
    row: Dict[str, Any],
    *,
    sector_scores: Optional[Sequence[Any]] = None,
    weights: Optional[Mapping[str, float]] = None,
    in_place: bool = False,
) -> Dict[str, Any]:
    """
    Add the 6 insight fields to a row dict.

    The new `recommendation_reason` value will OVERWRITE any existing one,
    because the structured form is a superset of the base reason.
    """
    target = row if in_place else dict(row or {})
    base_reason = target.get("recommendation_reason")
    insights = build_insights(
        target,
        sector_scores=sector_scores,
        weights=weights,
        base_reason=base_reason,
    )
    target.update(insights.to_dict())
    return target


def enrich_rows_with_insights(
    rows: Sequence[Dict[str, Any]],
    *,
    weights: Optional[Mapping[str, float]] = None,
    sector_key: str = "sector",
    inplace: bool = False,
) -> List[Dict[str, Any]]:
    """
    Batch enricher that computes sector cohorts and adds insights to each row.

    Sector cohorts are derived from `sector_key` (default "sector") and used
    to compute `sector_relative_score` via percentile rank within the cohort.
    Rows without a sector are still enriched, just with sector_relative_score
    left as None.
    """
    target = list(rows) if inplace else [dict(r or {}) for r in rows]

    cohorts: Dict[str, List[Any]] = {}
    for r in target:
        sec = str(r.get(sector_key) or "").strip()
        if not sec:
            continue
        cohorts.setdefault(sec, []).append(r.get("overall_score"))

    for r in target:
        sec = str(r.get(sector_key) or "").strip()
        sector_scores = cohorts.get(sec) if sec else None
        enrich_row_with_insights(
            r,
            sector_scores=sector_scores,
            weights=weights,
            in_place=True,
        )

    return target


# =============================================================================
# Module exports
# =============================================================================

__all__ = [
    "VERSION",
    "INSIGHTS_BUILDER_VERSION",
    "InsightsBundle",
    "compute_conviction_score",
    "compute_sector_relative_score",
    "derive_top_factors",
    "derive_top_risks",
    "build_position_size_hint",
    "build_recommendation_reason",
    "build_insights",
    "enrich_row_with_insights",
    "enrich_rows_with_insights",
]
