#!/usr/bin/env python3
# core/scoring.py
"""
================================================================================
Scoring Module — v5.2.6
(DATA_ENGINE_V2 v5.67.0 FRACTION-CONTRACT ALIGNMENT /
 [PRESERVED v5.2.5] CROSS-PROVIDER CONTRACT ALIGNMENT WITH v4.7.3 / v6.1.0 /
 v8.2.0 / v4.3.0 PROVIDERS + DATA_ENGINE_V2 v5.60.0 /
 [PRESERVED v5.2.4] POST-DEPLOY SYNTHESIZER-OVER-AGGRESSION GUARD /
 [PRESERVED v5.2.3] AUDIT-DRIVEN HARDENING / NO-FABRICATED-CONFIDENCE /
 RECOMMENDATION-COHERENCE-GUARDED / RISK-PENALTY-REBALANCED /
 REVENUE-COLLAPSE-AWARE QUALITY / UNIT-MISMATCH-SAFE FORECASTS /
 ILLIQUID-AWARE FORECAST SKIP /
 [PRESERVED] VIEW-COMPLETENESS HARDENED / VIEW-TOKENS-IN-EVERY-REASON /
 ACCUMULATE-ALIGNED / 5-TIER + CONVICTION FLOORS /
 INSIGHTS-INTEGRATED / PHANTOM-ROW-SAFE / SCHEMA-ALIGNED /
 v5.53.0 / v5.60.0 / v5.67.0 ENGINE-UNIT COMPATIBLE)
================================================================================

v5.2.6 changes (vs v5.2.5)  —  DATA_ENGINE_V2 v5.67.0 FRACTION-CONTRACT ALIGNMENT
---------------------------------------------------------------------------------
Closes one defensive-shape-detection gap surfaced by the May 13 2026
data_engine_v2 v5.67.0 unit-contract alignment release. v5.67.0 switched
the engine's emit contract for four percent-unit fields from POINTS to
FRACTION to align with 04_Format.gs v2.7.0's
`_KNOWN_FRACTION_PERCENT_COLUMNS_` formatter expectation:

  - percent_change             (was ×100 points → now fraction)
  - upside_pct                 (was ×100 points → now fraction)
  - max_drawdown_1y            (was abs()×100 → now SIGNED fraction)
  - var_95_1d                  (was ×100 points → now fraction)

Audit of scoring.py v5.2.5 against the new contract:

  - `compute_momentum_score: percent_change`        OK via `_as_roi_fraction` (shape-aware, >1.0 → /100)
  - `compute_risk_score: max_drawdown_1y`           OK via `_as_fraction` + `abs()` (signed fraction safe)
  - `compute_risk_score: var_95_1d`                 OK via `_as_fraction`
  - `compute_momentum_score: week_52_position_pct`  OK via `_as_pct_position_fraction` (v5.2.5 PHASE-M)
  - `derive_upside_pct: upside_pct`                 GAP — no shape detection
  - `compute_scores: upside_pct tracking`           GAP — no shape detection

The first three are already defensively shape-aware (the helpers detect
POINTS vs FRACTION by magnitude and convert). The fourth was hardened in
v5.2.5 PHASE-M. The remaining gap is the `upside_pct` read path:

  `_is_upside_suspect` bounds are FRACTION-form (-0.90, +2.00) by
  intent — they match the engine v5.67.0 emit contract exactly. But
  `derive_upside_pct` reads the row's `upside_pct` via `_get_float`
  with NO shape conversion before the suspect check.

  For an engine v5.67.0 row (upside_pct = 0.25 fraction):
    _is_upside_suspect(0.25) = False → returned ✓
  For a pre-v5.67.0 engine row (upside_pct = 25.0 points):
    _is_upside_suspect(25.0) = True (25 > 2.0) → None ✗
  For a non-engine path (test fixture, snapshot replay, third-party
  importer) emitting points form:
    same suppression failure mode ✗

v5.2.6 fix — Phase Q:

  Q. Defensive shape detection for upside_pct (NEW).

     New private helper `_as_upside_fraction(value)` with shape
     detection by magnitude:
       |value| <= 2.5 → FRACTION (engine v5.67.0 canonical form;
                                  covers the entire legitimate
                                  range [-0.90, +2.00] with margin)
       |value|  > 2.5 → PERCENT POINTS (pre-v5.67.0 engine, legacy
                                       snapshots, non-engine paths;
                                       divide by 100)

     Threshold rationale: unlike `_as_pct_position_fraction` which
     uses 1.5 (52W position range is bounded by 1.0 fraction),
     `upside_pct` can legitimately reach 2.0 (200%) per the v5.67.0
     engine cap `_INTRINSIC_UPSIDE_MAX_PCT / 100.0`. Using 1.5 would
     misread legitimate fraction values in [1.5, 2.0]. Threshold 2.5
     preserves the entire v5.67.0 legitimate fraction range and
     catches all reasonable points values (≥2.5 points = ≥2.5%
     upside; anything smaller is essentially never the actual case
     in legacy data).

     Idempotent across both shapes.

     Applied at two call sites:
       1. `derive_upside_pct` — the row's supplied `upside_pct` is
          shape-normalized before `_is_upside_suspect` is evaluated.
          Result is always fraction-form on return.
       2. `compute_scores` — the v5.2.4 upside-suppression tracking
          block reads the source row's `upside_pct` defensively so a
          legacy POINTS value doesn't get falsely tagged as
          "upside_synthesis_suspect" when in fact it's a valid
          upside in disguise.

  R. `_is_upside_suspect` docstring update — clarifies that the
     bounds are FRACTION-form and aligned with the v5.67.0 engine
     emit contract. No behavior change.

  S. Version bump 5.2.5 → 5.2.6.

[PRESERVED — strictly] All v5.2.5 / v5.2.4 / v5.2.3 / v5.2.2 / v5.2.1 /
v5.2.0 helpers, signatures, dataclass fields, behaviors, constants,
and the entire prior narrative. The new helper is additive. Public API
surface unchanged. No removals from __all__.

================================================================================

v5.2.5 changes (vs v5.2.4)  —  CROSS-PROVIDER CONTRACT ALIGNMENT
-----------------------------------------------------------------
Aligns scoring.py with the May 10/11 2026 provider + engine revisions:

  - eodhd_provider v4.7.3, yahoo_fundamentals_provider v6.1.0,
    yahoo_chart_provider v8.2.0, and enriched_quote v4.3.0 emit
    `warnings` as List[str] (engine canonicalizes to "; "-joined
    string at the merge boundary) and `last_error_class` for
    diagnostic clustering on full-fail returns.
  - data_engine_v2 v5.60.0 Phase O canonicalizes
    week_52_position_pct to PERCENT POINTS (0-100). Phase P
    defensively validates 52W bounds (drops bounds out of [8x, 1/8x]
    of price). Phase Q preserves last_error_class through
    canonicalize via a new alias entry. Phase H/I/P guards may
    clear intrinsic_value AND/OR upside_pct when synthesizer
    artifacts or unit mismatches are detected upstream.

Four structural gaps in v5.2.4 vs the new cross-stack contract:

  GAP 1 — Engine-applied valuation clears not surfaced.
    Engine v5.60.0 may clear intrinsic_value AND upside_pct via
    Phase H (hard-kill unit mismatch), Phase I (upside synthesis
    suspect), or Phase P (52W bounds dropped). In those cases the
    row arrives at scoring with intrinsic=None, upside_pct=None,
    and the warnings string containing one of:
      - "intrinsic_unit_mismatch_suspected"
      - "upside_synthesis_suspect"
      - "engine_52w_high_unit_mismatch_dropped"
      - "engine_52w_low_unit_mismatch_dropped"
      - "engine_52w_high_low_inverted"
    v5.2.4's upside-suppression tracker only fires when scoring
    ITSELF detects a suspect upside, so engine-cleared rows
    produce no audit trail in scoring_errors.

  GAP 2 — last_error_class not surfaced.
    The new providers emit last_error_class (e.g., "RateLimited",
    "AuthError", "IpBlocked", "NotFound", "NetworkError",
    "InvalidPayload", "FetchError", "InvalidSymbol",
    "MissingApiKey", "KsaBlocked") on full-fail returns. Engine
    v5.60.0 Phase Q preserves the field through canonicalize.
    Scoring v5.2.4 doesn't read it, so the audit trail loses an
    important diagnostic signal — operators cannot distinguish a
    row that came from a healthy provider vs one that was
    constructed from a fallback after a primary-provider error.

  GAP 3 — week_52_position_pct shape assumption.
    v5.2.2's _as_pct_position_fraction assumed PERCENT POINTS
    (0-100) only and unconditionally divided by 100. After engine
    v5.60.0 Phase O the contract is enforced at the canonicalize
    boundary, but rows from non-engine paths (test fixtures,
    third-party importers, snapshot replays of pre-v5.60.0 data)
    may still arrive as FRACTION (0-1), in which case the v5.2.4
    form produced wrong results (0.156 -> 0.00156).

  GAP 4 — Warnings-string-aware unforecastable detection missing.
    v5.2.4's _is_row_unforecastable checks the forecast_unavailable
    bool. Engine v5.60.0 Phase B's consistency sweep sets BOTH the
    bool and the warning tag, but defense-in-depth says scoring
    should also detect the warning string in case a downstream
    merge drops the bool while preserving the warning (or a
    direct-test row supplies only the warning).

v5.2.5 fixes (preserved verbatim in v5.2.6):

  K. Engine-applied valuation clear surfacing (GAP 1).
     New helper _row_engine_dropped_valuation() reads the warnings
     string for engine-applied tags.

  L. last_error_class surfacing (GAP 2). compute_scores reads
     last_error_class and appends "provider_error:<class>" to
     scoring_errors.

  M. Defensive _as_pct_position_fraction (GAP 3). Shape detection.

  N. Warnings-string-aware unforecastable detection (GAP 4).
     _is_row_unforecastable also checks the warnings string for
     engine-emitted markers.

  O. New helper _warning_tags_from_row(row).

  P. Version bump to 5.2.5.

================================================================================

v5.2.4 changes (vs v5.2.3)  —  POST-DEPLOY SYNTHESIZER-OVER-AGGRESSION GUARD
----------------------------------------------------------------------------
Closes one structural defect surfaced by the post-deploy production
audit (127-row Global_Markets sample, May 10 2026). v5.2.3's
unit-mismatch FLOOR guard was correct and is preserved; v5.2.4 adds
the missing CEILING guard plus a two-sided bound on the displayed
upside_pct field.

v5.2.4 fixes (preserved verbatim in v5.2.5 / v5.2.6):

  G. derive_forecast_patch — SYNTHESIS CEILING guard.
  H. derive_upside_pct — TWO-SIDED SUSPECT BOUNDS.
  I. compute_valuation_score — GUARDED UPSIDE COMPUTATION.
  J. compute_scores — ERROR TRACKING.

[PRESERVED — strictly] All v5.2.3 / v5.2.2 / v5.2.1 / v5.2.0 / v5.1.0
helpers, signatures, dataclass fields, behaviors, and constants. The
existing _FORECAST_ROI12_UNIT_MISMATCH_FLOOR is preserved exactly.
Public API surface unchanged.

================================================================================

v5.2.3 changes (vs v5.2.2)  —  AUDIT-DRIVEN HARDENING
-----------------------------------------------------
Closes six structural defects surfaced by the production audit of
the deployed Global_Markets sheet (1,707 rows × 46 cols). v5.2.2's
unit-conversion fix was correct but local; the audit revealed broader
pathological patterns in scoring outputs that v5.2.3 addresses.

v5.2.3 fixes (preserved):
  A. compute_confidence_score — NO-FABRICATED-CONFIDENCE.
  B. compute_recommendation — RECOMMENDATION-COHERENCE-GUARD.
  C. ScoringConfig.risk_penalty_strength — RISK-PENALTY-REBALANCE.
  D. compute_quality_score — REVENUE-COLLAPSE-AWARE.
  E. derive_forecast_patch — UNIT-MISMATCH SANITY GUARD.
  F. derive_forecast_patch — ILLIQUID-AWARE EARLY EXIT.

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

Public API (unchanged from v5.2.5):
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

__version__ = "5.2.6"
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
# v5.2.3 — Audit-driven thresholds  (PRESERVED)
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
# v5.2.4 — Synthesizer-over-aggression guard  (PRESERVED)
# =============================================================================
#
# Mirror to v5.2.3's _FORECAST_ROI12_UNIT_MISMATCH_FLOOR, but on the
# POSITIVE side. Catches the +396% / +470% / +500% upside cases
# observed in the May 2026 post-deploy audit (SD.US, NPSNY.US, CRK.US,
# SBK.JSE post-v5.55.1 unit fix).
#
# Only fires when roi12 was synthesized from fair value (not when
# the provider supplied directly), because a provider-supplied
# roi12 above +200% may reflect a legitimate high-conviction call
# we don't want to silently suppress.

# Forecast-synthesis CEILING guard (POSITIVE side, v5.2.4).
_FORECAST_ROI12_SYNTHESIS_CEILING = 2.00   # +200% over 12 months

# Two-sided suspect bounds for the user-visible upside_pct field.
# v5.2.6 NOTE: These bounds are FRACTION-FORM. They are aligned with
# the data_engine_v2 v5.67.0 emit contract, which stores upside_pct
# as a fraction (e.g., 0.25 for +25% upside; cap range
# [-0.90, +2.00]). Callers that may receive POINTS-form values from
# non-engine paths or legacy snapshots should normalize via
# `_as_upside_fraction()` BEFORE calling `_is_upside_suspect`.
#
# Threshold rationale (against the production sample):
#   FLOOR -90% catches AV.LSE (-97%), SMIN.LSE (-99%), SBK.JSE (-94%),
#                      ANG.JSE (-96%); preserves -50% to -85% genuine.
#   CEIL +200% catches SD.US (+500%), NPSNY.US (+472%), CRK.US (+396%),
#                      SBK.JSE post-v5.55.1 (+470%); preserves
#                      ML.PA (+190%), RCI.US (+166%), BCE.TO (+162%),
#                      KEP.US (+153%).
_UPSIDE_PCT_SUSPECT_FLOOR = -0.90      # -90% (fraction form)
_UPSIDE_PCT_SUSPECT_CEILING = 2.00     # +200% (fraction form)


def _is_upside_suspect(upside: Optional[float]) -> bool:
    """
    v5.2.4: Returns True when an upside_pct value falls outside the
    data-quality sanity bounds.

    Use to guard:
      - The displayed upside_pct field (derive_upside_pct).
      - The ad-hoc upside computation in compute_valuation_score.

    None inputs return False (i.e. None is not "suspect" — it's just
    absent; callers handle absence separately).

    v5.2.6 NOTE: The bounds are FRACTION-FORM, aligned with the
    data_engine_v2 v5.67.0 emit contract (upside_pct stored as
    fraction). When the caller may have POINTS-form input (pre-v5.67.0
    engine row, legacy snapshot, test fixture, third-party importer),
    use `_as_upside_fraction()` to shape-normalize BEFORE calling
    this function. The two existing call sites in scoring.py
    (`derive_upside_pct` and `compute_valuation_score`) have been
    updated to do this in v5.2.6.
    """
    if upside is None:
        return False
    return (
        upside < _UPSIDE_PCT_SUSPECT_FLOOR
        or upside > _UPSIDE_PCT_SUSPECT_CEILING
    )


# =============================================================================
# v5.2.5 — Cross-provider contract alignment  (PRESERVED)
# =============================================================================
#
# Provider error-class values surfaced by eodhd_provider v4.7.3,
# yahoo_fundamentals_provider v6.1.0, and yahoo_chart_provider v8.2.0
# via the `last_error_class` field (preserved through engine
# canonicalization by data_engine_v2 v5.60.0 Phase Q alias entry).
#
# Surfaced to scoring_errors as "provider_error:<class>" so audit
# reports can correlate scoring outcomes against upstream provider
# health. Not used to factor into confidence — _data_quality_factor
# already accounts for data quality, so this would double-penalize.

_PROVIDER_ERROR_CLASSES_KNOWN: Set[str] = {
    "AuthError",
    "IpBlocked",
    "RateLimited",
    "NotFound",
    "NetworkError",
    "InvalidPayload",
    "FetchError",
    "InvalidSymbol",
    "MissingApiKey",
    "KsaBlocked",
}

# Engine-applied warning tags that indicate the engine (data_engine_v2
# v5.60.0 Phase H / Phase I / Phase P) dropped intrinsic_value and/or
# upside_pct because of unit-mismatch or synthesizer-overshoot
# detection. When scoring sees these tags AND intrinsic is missing,
# it surfaces "engine_dropped_valuation" to scoring_errors for audit
# correlation.
#
# v5.2.6 NOTE: data_engine_v2 v5.67.0 switched to a fraction-emit
# contract that eliminates the unit-mismatch class of bugs the
# original tags were designed to catch. v5.67.0 rows are unlikely to
# carry these tags, but the helper remains correct (returns False)
# and continues to surface tags from legacy snapshot rows or older
# engine versions still in production.
_ENGINE_DROPPED_VALUATION_TAGS: Set[str] = {
    "intrinsic_unit_mismatch_suspected",
    "upside_synthesis_suspect",
    "engine_52w_high_unit_mismatch_dropped",
    "engine_52w_low_unit_mismatch_dropped",
    "engine_52w_high_low_inverted",
}

# Engine-emitted warning tags that mark a row as unforecastable.
# v5.2.5 reads these in addition to the forecast_unavailable bool
# for defense-in-depth against state-drift in downstream merge code.
_ENGINE_UNFORECASTABLE_TAGS: Set[str] = {
    "forecast_unavailable",
    "forecast_unavailable_no_source",
    "forecast_cleared_consistency_sweep",
    "forecast_skipped_unavailable",
}


def _warning_tags_from_row(row: Mapping[str, Any]) -> Set[str]:
    """
    v5.2.5: Parse the row's `warnings` field into a Set[str] of
    normalized tags. Handles:
      - None / "" / missing -> empty set
      - "; "-joined string (engine v5.60.0 / v5.67.0 Phase N canonical shape)
      - Accidental List[str] / Tuple[str] input (defensive: scoring
        is called from many paths, some of which may bypass engine
        canonicalization)
    Each tag is stripped of whitespace. Empty parts after split are
    dropped. Used by _row_engine_dropped_valuation() and the
    warnings-aware branch of _is_row_unforecastable().
    """
    if row is None:
        return set()
    raw = row.get("warnings") if isinstance(row, Mapping) else None
    if raw is None:
        return set()

    parts: List[str] = []
    if isinstance(raw, str):
        for piece in raw.split(";"):
            s = piece.strip()
            if s:
                parts.append(s)
    elif isinstance(raw, (list, tuple, set)):
        for item in raw:
            if item is None:
                continue
            try:
                s = str(item).strip()
            except Exception:
                continue
            if s:
                parts.append(s)
    else:
        try:
            s = str(raw).strip()
        except Exception:
            s = ""
        if s:
            parts.append(s)

    # Strip "key:value" suffix when comparing against the tag set —
    # callers compare with the SET membership which uses bare keys.
    # We keep both forms so callers can pick either.
    out: Set[str] = set()
    for p in parts:
        out.add(p)
        # Also add the bare key (before any ":") so a tag like
        # "forecast_unavailable:no_price_or_market_cap" is matched
        # against the bare "forecast_unavailable" in the static sets.
        if ":" in p:
            bare = p.split(":", 1)[0].strip()
            if bare:
                out.add(bare)
    return out


def _row_engine_dropped_valuation(row: Mapping[str, Any]) -> bool:
    """
    v5.2.5: Returns True when the row's warnings string indicates
    the engine (data_engine_v2 v5.60.0 Phase H / I / P) dropped
    intrinsic_value or upside_pct due to unit-mismatch or
    synthesizer-overshoot detection upstream.

    Used by compute_scores to append "engine_dropped_valuation" to
    scoring_errors when intrinsic_value is missing AND one of these
    tags is present — closes the audit-trail gap where engine-
    cleared rows produced no scoring-layer diagnostic.

    v5.2.6 NOTE: data_engine_v2 v5.67.0 emits values directly in
    FRACTION form without the upstream-clear class of remediations,
    so v5.67.0 rows are unlikely to carry these tags. The helper
    remains useful for legacy snapshots and older engine versions
    coexisting in production.
    """
    if row is None:
        return False
    tags = _warning_tags_from_row(row)
    if not tags:
        return False
    return bool(tags & _ENGINE_DROPPED_VALUATION_TAGS)


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
# Pure Utility Functions (preserved + v5.2.5 Phase M / v5.2.6 Phase Q updates)
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
    v5.2.5: 52W-position-style fields with defensive shape detection.

    Engine v5.60.0 / v5.67.0 Phase O canonicalizes week_52_position_pct
    to PERCENT POINTS (0-100) at the canonicalize boundary, so rows
    flowing from the engine arrive in percent-point form. But scoring
    is called from many code paths, some of which may bypass engine
    canonicalization (test fixtures, direct provider patches being
    scored individually, snapshot replays of pre-v5.60.0 sheets).
    Those rows may still arrive as FRACTION (0-1).

    Shape detection (v5.2.5 — Phase M):
      |value| <= 1.5  ->  FRACTION; pass through with clamp to [0,1]
      |value|  > 1.5  ->  PERCENT POINTS; divide by 100 and clamp

    Idempotent across both shapes. Closes the gap where a fraction
    value (0.156) running through v5.2.4's unconditional /100 would
    have produced 0.00156, dropping the position effectively to 0.

    [PRESERVED v5.2.2] Public helper signature and return type
    unchanged. The clamp at the call site remains harmless and is
    kept for further defense.
    """
    f = _safe_float(value)
    if f is None:
        return None
    if abs(f) <= 1.5:
        # FRACTION shape (engine pre-v5.60.0, non-engine paths, providers
        # emitting raw fractions). Already in target [0, 1] range.
        return _clamp(f, 0.0, 1.0)
    # PERCENT POINTS shape (engine v5.60.0 / v5.67.0 Phase O canonical
    # form, eodhd_provider v4.7.3 native form, scoring.py v5.2.2 contract).
    return _clamp(f / 100.0, 0.0, 1.0)


def _as_upside_fraction(value: Any) -> Optional[float]:
    """
    v5.2.6 — Phase Q: upside_pct shape detection.

    The data_engine_v2 v5.67.0 release switched upside_pct from POINTS
    (e.g., 25.0 for +25% upside) to FRACTION (e.g., 0.25). The engine
    caps fraction output to the range [-0.90, +2.00] per
    `_INTRINSIC_UPSIDE_MIN_PCT / 100.0` and `_INTRINSIC_UPSIDE_MAX_PCT / 100.0`.

    Pre-v5.67.0 engines and many non-engine code paths (test fixtures,
    snapshot replays of older sheets, third-party row importers) may
    still emit POINTS. This helper detects shape by magnitude and
    normalizes to fraction so `_is_upside_suspect` and the downstream
    consumers (`derive_value_view`, `compute_recommendation`) see
    consistent fraction-form input.

    Shape detection:
      |value| <= 2.5  ->  FRACTION (engine v5.67.0 canonical form;
                                    covers the entire legitimate range
                                    [-0.90, +2.00] with comfortable
                                    margin for analyst-supplied
                                    high-conviction upsides up to +250%)
      |value|  > 2.5  ->  PERCENT POINTS (pre-v5.67.0 engine, legacy
                                          snapshots, non-engine paths;
                                          divide by 100)

    Threshold rationale: unlike `_as_pct_position_fraction` which uses
    1.5 (52W position is bounded by 1.0 fraction = 100%), upside_pct
    can legitimately reach 2.0 (200%) per the engine cap. A threshold
    of 1.5 would misread valid fraction values in [1.5, 2.0]. Threshold
    2.5 preserves the full v5.67.0 legitimate range, leaves headroom
    for occasional analyst-supplied values up to +250%, and reliably
    catches all reasonable points values (any |x| >= 2.5 points = ≥2.5%
    upside; smaller-than-2.5% points-form upsides are tolerated as
    "fraction" but the downstream suspect-bound check correctly accepts
    them either way since the bounds are symmetric in magnitude).

    Idempotent across both shapes for any value within the engine's
    cap range. None inputs return None.
    """
    f = _safe_float(value)
    if f is None:
        return None
    if abs(f) <= 2.5:
        # FRACTION shape (engine v5.67.0 canonical form). Pass through;
        # no clamp here since `_is_upside_suspect` is the sole reader
        # and applies its own asymmetric bounds (-0.90, +2.00).
        return f
    # POINTS shape (pre-v5.67.0 engine, legacy snapshots). Convert.
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
# Derived Field Helpers — v5.2.6 Phase Q applied to derive_upside_pct
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
    """
    Compute the displayed upside_pct field.

    v5.2.6 — Phase Q (NEW). When the row carries a supplied
    `upside_pct`, normalize its shape via `_as_upside_fraction` before
    evaluating the suspect bounds. Engine v5.67.0 stores upside_pct as
    fraction (e.g., 0.25 for +25%); pre-v5.67.0 engines and non-engine
    paths may emit POINTS (e.g., 25.0). The shape detection ensures
    `_is_upside_suspect` always sees fraction-form input, eliminating
    the false-positive suppression of valid POINTS-form upsides
    (anything ≥+2.5% points was being flagged as suspect pre-v5.2.6
    because 25.0 > +2.00 fraction ceiling).

    v5.2.4 — TWO-SIDED SUSPECT BOUNDS (preserved).
    Suppresses values outside [_UPSIDE_PCT_SUSPECT_FLOOR,
    _UPSIDE_PCT_SUSPECT_CEILING] (default -90% to +200% in fraction
    form) by returning None. Catches both the LSE/JSE/TASE phantom
    downsides (-94% to -99%, subunit pricing artifacts) and the
    Graham-formula synthesizer over-aggression (+396% to +500% on
    SD.US, NPSNY.US, CRK.US, SBK.JSE).

    The bounds apply to BOTH:
      - a row-supplied upside_pct value (shape-normalized via
        _as_upside_fraction; suppressed if suspect)
      - a locally-computed (intrinsic - price) / price (always
        fraction; suppressed if suspect)

    Returns None when:
      - upside is missing (no price or no intrinsic)
      - upside is suspect (outside bounds after shape normalization)
    Otherwise returns the rounded fraction.
    """
    # v5.2.6 Phase Q: shape-aware read. Handles both engine v5.67.0
    # fraction and pre-v5.67.0 / non-engine POINTS forms.
    usp = _as_upside_fraction(_get(row, "upside_pct"))
    if usp is not None:
        # v5.2.4 (preserved): guard against suspect provider-supplied upside.
        if _is_upside_suspect(usp):
            return None
        # v5.2.6: round consistently with the synthesized-path return.
        return _round(usp, 4)

    price = _get_float(row, "current_price", "price", "last_price")
    intrinsic = _get_float(row, "intrinsic_value", "fair_value")

    if price is None or price <= 0 or intrinsic is None or intrinsic <= 0:
        return None

    raw = (intrinsic - price) / price

    # v5.2.4 (preserved): guard against synthesizer-derived suspect upside.
    if _is_upside_suspect(raw):
        return None

    return _round(raw, 4)


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
# Forecast Helpers — v5.2.5 Phase N preserved
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
    v5.2.5 — WARNINGS-STRING-AWARE detection (Phase N).

    Returns True when forecast synthesis should be skipped entirely.
    Detection rules (any of):
      1. v5.2.3 explicit forecast_unavailable=True flag.
      2. v5.2.3 data_quality ∈ {STALE, MISSING, ERROR} AND no fair
         value AND no API-supplied roi3/roi12/fp12.
      3. v5.2.5 NEW: warnings string contains any of
         _ENGINE_UNFORECASTABLE_TAGS:
           - "forecast_unavailable"
           - "forecast_unavailable_no_source"
           - "forecast_cleared_consistency_sweep"
           - "forecast_skipped_unavailable"

    Rules 1 + 2 preserved verbatim from v5.2.4. Rule 3 is additive.
    """
    # Rule 1 — explicit bool (v5.2.3, preserved)
    if _safe_bool(_get(row, "forecast_unavailable", "is_forecast_unavailable")):
        return True

    # Rule 3 — engine-emitted warning tags (v5.2.5 Phase N)
    tags = _warning_tags_from_row(row)
    if tags and (tags & _ENGINE_UNFORECASTABLE_TAGS):
        return True

    # Rule 2 — DQ + no synthesizable inputs (v5.2.3, preserved)
    dq_label = str(_get(row, "data_quality") or "").strip().upper()
    dq_is_unrecoverable = dq_label in {"STALE", "MISSING", "ERROR"}

    if not dq_is_unrecoverable:
        return False

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
    v5.2.4: hardened forecast derivation.

    Three safety paths inherited from v5.2.3, plus one new in v5.2.4:
      1. ILLIQUID-AWARE EARLY EXIT (v5.2.3) — when _is_row_unforecastable
         returns True, return all-None patch immediately.
      2. UNIT-MISMATCH SANITY GUARD, FLOOR side (v5.2.3) — after roi12
         has been derived, if it's < -50%, treat as a unit-conversion
         bug, suppress, emit "forecast_suspect_unit_mismatch".
      3. SYNTHESIS-CEILING GUARD (v5.2.4) — symmetric to (2).
         When roi12 was synthesized from fair value AND is > +200%,
         treat as Graham-formula over-aggression, suppress, emit
         "forecast_suspect_synthesis_overshoot". Does NOT fire for
         provider-supplied roi12.
      4. ROI clamping to ForecastParameters bounds (preserved).

    [v5.2.5 NOTE] _is_row_unforecastable now also detects engine-
    emitted warning tags (Phase N). This function's behavior is
    unchanged — it just sees an earlier early-exit on a wider set
    of unforecastable rows.

    [v5.2.6 NOTE] expected_roi_3m / expected_roi_12m are read via
    `_as_roi_fraction` which is already shape-aware (>1.0 → /100), so
    the data_engine_v2 v5.67.0 fraction emit contract is transparent
    to this function. No behavioral change required.
    """
    errors: List[str] = []

    # v5.2.3: ILLIQUID-AWARE EARLY EXIT (extended in v5.2.5 to honor warnings tags)
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
    if roi12 is not None and roi12 < _FORECAST_ROI12_UNIT_MISMATCH_FLOOR:
        errors.append("forecast_suspect_unit_mismatch")
        roi12 = None
        if roi12_synthesized_from_fair:
            roi1 = None
            roi3 = None
            fp1 = None
            fp3 = None
            fp12 = None
        else:
            fp12 = None

    # v5.2.4: SYNTHESIS-CEILING GUARD (POSITIVE side / CEILING).
    if (
        roi12 is not None
        and roi12_synthesized_from_fair
        and roi12 > _FORECAST_ROI12_SYNTHESIS_CEILING
    ):
        errors.append("forecast_suspect_synthesis_overshoot")
        roi12 = None
        roi1 = None
        roi3 = None
        fp1 = None
        fp3 = None
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
# Component Scoring — preserved from v5.2.5
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

    span = _QUALITY_REVENUE_COLLAPSE_FLOOR - _QUALITY_REVENUE_COLLAPSE_START
    if span >= 0:
        return 1.0
    progress = (revenue_growth - _QUALITY_REVENUE_COLLAPSE_START) / span
    progress = _clamp(progress, 0.0, 1.0)
    haircut = 1.0 - progress * (1.0 - _QUALITY_REVENUE_COLLAPSE_MAX_HAIRCUT)
    return _clamp(haircut, _QUALITY_REVENUE_COLLAPSE_MAX_HAIRCUT, 1.0)


def compute_quality_score(row: Mapping[str, Any]) -> Optional[float]:
    """
    Compute quality score (0-100). v5.2.3: REVENUE-COLLAPSE-AWARE.
    [PRESERVED] Phantom-row gate.
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

    # v5.2.3: REVENUE-COLLAPSE-AWARE haircut
    revenue_growth = _as_fraction(
        _get(row, "revenue_growth_yoy", "revenue_growth", "growth_yoy")
    )
    haircut = _revenue_collapse_haircut(revenue_growth)
    combined *= haircut

    return _round(100.0 * _clamp(combined, 0.0, 1.0), 2)


def compute_confidence_score(row: Mapping[str, Any]) -> Tuple[Optional[float], Optional[float]]:
    """
    v5.2.3 NO-FABRICATED-CONFIDENCE (preserved).
    Returns (confidence_score_100, forecast_confidence_01).
    """
    fc = _safe_float(
        _get(row, "forecast_confidence", "ai_confidence", "confidence_score", "confidence")
    )
    if fc is not None:
        fc01 = (fc / 100.0) if fc > 1.5 else fc
        fc01 = _clamp(fc01, 0.0, 1.0)
        return _round(fc01 * 100.0, 2), _round(fc01, 4)

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
    """
    v5.2.4 — GUARDED UPSIDE COMPUTATION (preserved verbatim).

    The ad-hoc upside is computed locally as (fair / price) - 1.0
    which is always a FRACTION; the v5.2.4 `_is_upside_suspect` guard
    operates on it directly. No shape conversion needed here — that
    only applies to row-supplied `upside_pct` values which go through
    `derive_upside_pct` (which uses `_as_upside_fraction` in v5.2.6).
    """
    price = _get_float(row, "current_price", "price", "last_price", "last")
    if price is None or price <= 0:
        return None

    fair = _get_float(
        row,
        "intrinsic_value", "fair_value", "target_price",
        "forecast_price_3m", "forecast_price_12m", "forecast_price_1m"
    )

    # v5.2.4: GUARDED UPSIDE.
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
    Compute momentum score (0-100). v5.2.5: _as_pct_position_fraction
    detects shape (fraction vs percent points) defensively.

    [v5.2.6 NOTE] percent_change is read via `_as_roi_fraction`, which
    is already shape-aware (>1.0 → /100). The data_engine_v2 v5.67.0
    fraction emit contract is transparent here. No behavioral change.
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
    """
    Compute risk score (0-100). Higher = riskier.

    [v5.2.6 NOTE] max_drawdown_1y and var_95_1d are read via
    `_as_fraction` which is shape-aware (>1.5 → /100). The
    data_engine_v2 v5.67.0 fraction emit contract is transparent. The
    drawdown SIGN flip (v5.67.0 emits signed negative fraction; pre-
    v5.67.0 emitted abs() positive points) is handled by the existing
    `abs(dd1y)` call below — the absolute magnitude is what feeds the
    risk scale either way.
    """
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
# View Derivation (preserved)
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
    """
    v5.2.6 NOTE: `upside_pct` is expected to be a FRACTION here.
    Callers should obtain it via `derive_upside_pct` (which applies
    `_as_upside_fraction` for shape detection in v5.2.6) so the
    thresholds below (0.20 = 20%, -0.10 = -10%) are applied to
    consistently fraction-form data.
    """
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
# v5.2.3 — Recommendation coherence guard (preserved)
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
# Recommendation (preserved from v5.2.4)
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

    v5.2.3 changes (preserved):
      - RECOMMENDATION-COHERENCE-GUARD applied after reco_normalize
        returns its label.

    v5.2.1 / v5.2.0 (preserved): all four views derived at the top so
    every return path carries the parseable view prefix. Final pass
    through _align_reason_to_canonical_recommendation before return.

    v5.2.6 NOTE: `upside_pct` is expected to be a FRACTION (e.g., 0.25
    for +25%). Callers obtain it from `derive_upside_pct` which
    applies `_as_upside_fraction` shape detection.
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
# Main Scoring Function — v5.2.5 Phases K & L preserved + v5.2.6 Phase Q applied
# =============================================================================

def compute_scores(row: Dict[str, Any], settings: Any = None) -> Dict[str, Any]:
    """
    Score a single row.

    v5.2.6 changes (vs v5.2.5):
      - Q. `compute_scores` now reads the source row's `upside_pct`
        via `_as_upside_fraction` (shape-aware) in the v5.2.4 upside-
        suppression tracking block. Prevents false-positive
        "upside_synthesis_suspect" tags for legacy POINTS-form
        supplied upsides that the engine v5.67.0 fraction contract
        would otherwise legitimately accept.

    v5.2.5 changes (preserved):
      - K. Engine-applied valuation clears surfaced.
      - L. last_error_class surfaced as "provider_error:<class>".

    v5.2.4 changes (preserved):
      - derive_upside_pct returns None for suspect upsides (now
        shape-aware in v5.2.6).
      - compute_valuation_score guards its upside computation.
      - derive_forecast_patch has a synthesis-ceiling guard.
      - error tracking for suspect upsides (computed AND
        directly-supplied, even without intrinsic value).

    [PRESERVED] All v5.2.0 / v5.2.1 / v5.2.2 / v5.2.3 / v5.2.4 / v5.2.5
    mechanics.
    """
    source = dict(row or {})
    scoring_errors: List[str] = []

    # v5.2.5 PHASE-L: surface last_error_class from providers.
    _err_raw = _get(source, "last_error_class", "lastErrorClass", "errorClass", "error_class")
    if _err_raw is not None:
        _err_class = _safe_str(_err_raw)
        if _err_class and _err_class.lower() not in {"none", "null", "nil", "nan", "n/a", "na"}:
            scoring_errors.append("provider_error:" + _err_class)

    # v5.2.5 PHASE-K: surface engine-applied valuation clears.
    if _row_engine_dropped_valuation(source):
        _src_intrinsic = _get_float(source, "intrinsic_value", "fair_value")
        if _src_intrinsic is None or _src_intrinsic <= 0:
            if "engine_dropped_valuation" not in scoring_errors:
                scoring_errors.append("engine_dropped_valuation")

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

    # v5.2.4 + v5.2.6 Phase Q: Track scoring-layer upside suppression.
    # When derive_upside_pct returned None we want to attribute it to
    # either (a) a locally-suspect synthesized upside, or (b) a
    # directly-supplied suspect upside_pct in the source row.
    # v5.2.6: the source-supplied read uses _as_upside_fraction so a
    # legacy POINTS value (e.g., 25.0 = +25%) is correctly normalized
    # to fraction (0.25) before the bounds check; without this, valid
    # points-form upsides ≥ 2.5% would be falsely tagged as suspect.
    if usp is None:
        _raw_intrinsic = _get_float(working, "intrinsic_value", "fair_value")
        _raw_price = _get_float(working, "current_price", "price", "last_price")

        reason_for_suppress = False

        if (
            _raw_intrinsic is not None and _raw_intrinsic > 0
            and _raw_price is not None and _raw_price > 0
        ):
            _raw_upside = (_raw_intrinsic - _raw_price) / _raw_price
            if _is_upside_suspect(_raw_upside):
                reason_for_suppress = True

        if not reason_for_suppress:
            # v5.2.6 Phase Q: shape-aware read of the source upside_pct.
            # _as_upside_fraction handles both v5.67.0 fraction form
            # (0.25) and pre-v5.67.0 / legacy POINTS form (25.0).
            _supplied_usp = _as_upside_fraction(_get(source, "upside_pct"))
            if _supplied_usp is not None and _is_upside_suspect(_supplied_usp):
                reason_for_suppress = True

        if reason_for_suppress:
            if "upside_synthesis_suspect" not in scoring_errors:
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
        conf01_used = conf01 if conf01 is not None else 0.55

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
