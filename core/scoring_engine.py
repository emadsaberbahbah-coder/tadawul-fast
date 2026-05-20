#!/usr/bin/env python3
# core/scoring_engine.py
"""
================================================================================
Scoring Engine -- v3.6.0
(THIN COMPATIBILITY BRIDGE -- DELEGATES TO core.scoring v5.7.0+
 AND core.reco_normalize v8.0.0+)
================================================================================

This module is a pure compatibility shim. All scoring logic lives in
`core.scoring`. Older code (and some external integrations) imports
`ScoringEngine`, `compute_scores`, etc. from `core.scoring_engine` --
this module routes those imports to the canonical implementations in
`core.scoring`.

================================================================================
v3.6.0 changes (vs v3.5.0)  --  v5.6.0 / v5.7.0 / v8.0.0 ALIGNMENT
================================================================================

core.scoring has advanced through v5.6.0 -> v5.7.0 since v3.5.0 was
cut, and core.reco_normalize has advanced from v7.2.1 to v8.0.0. The
bridge was operating correctly against all of those versions thanks
to the getattr() fallback pattern, but was under-advertising the
new surface:

  - the v5.6.0 / v5.7.0 contract-version markers
    (_ENGINE_CONTRACT_VERSION, _RECO_NORMALIZE_CONTRACT_VERSION) are
    not re-exported, so audit tooling reading the bridge cannot
    reach them
  - the v5.6.0 row field `recommendation_detailed` is not in any
    field-contract tuple, so consumers introspecting the post-
    compute_scores schema via the bridge miss it
  - the v8.0.0 reco_normalize vocabulary expansion (ACCUMULATE,
    STRONG_SELL, AVOID added as canonical tiers; new RULE_ID_*
    constants; WORST_ORDINAL widened to 7) is invisible to the
    degradation report -- it shows 8-tier RECOMMENDATION_ENUM through
    the existing bucket-alignment check, but cannot distinguish a
    deployment with reco_normalize v8.0.0 (8-tier-aware) from one
    where scoring.py v5.7.0 emits 8 tiers but reco_normalize is
    still on v7.2.x (vocabulary mismatch -- ACCUMULATE / AVOID
    round-trip back through normalize_recommendation_code()
    incorrectly).

v3.6.0 closes those three gaps. No removals; strictly additive.

What advanced in core.scoring since v5.5.0
-------------------------------------------
  v5.6.0: data_engine_v2 v5.75.0 engine-contract alignment.
            derive_canonical_recommendation accepts overwrite=False
            and threads it through (was unconditionally honoring
            the inner idempotency-skip). _LOCAL_RECO_ALIASES["AVOID"]
            -> STRONG_SELL (cross-stack alignment for collapse).
            AssetScores gains recommendation_detailed (mirror of
            recommendation for field-name compat). Hard-normalize
            recommendation_priority_band -> P4 if any path returned
            a non-canonical value. get_canonical_state() reports
            engine_contract_version = "5.75.0". RECOMMENDATION_SOURCE_TAG
            auto-bumps via __version__ f-string.

  v5.7.0: core.reco_normalize v8.0.0 vocabulary expansion.
            RECOMMENDATION_ENUM widened 6 tiers -> 8 tiers, adding
            ACCUMULATE (between BUY and HOLD) and AVOID (after
            STRONG_SELL). Signal enum widened symmetrically.
            _LOCAL_RECO_ALIASES: ACCUMULATE -> ACCUMULATE (was BUY),
            AVOID -> AVOID (was STRONG_SELL); v5.6.0's collapse is
            obsolete now that AVOID is its own canonical tier. New
            scale-in synonyms (SCALE_IN, BUILD_POSITION, PHASE_IN,
            etc) routing to ACCUMULATE; new uninvestable synonyms
            (UNINVESTABLE, DO_NOT_BUY, BLACKLIST, STAY_AWAY, etc)
            routing to AVOID. compute_recommendation() ladder gains
            AVOID branches at the top (score < 15 OR
            bearish/bearish/expensive view consensus); the two
            former "Watch / accumulate candidate" HOLD branches
            (MONTH and LONG horizons) become ACCUMULATE proper,
            thresholds preserved verbatim. STRONG_SELL reason text
            tightened from "require urgent exit or avoidance" to
            "require urgent exit" -- AVOID is now distinct.
            _compute_priority() gains AVOID -> P1, ACCUMULATE -> P3.
            Position-size hint fallback gains ACCUMULATE / AVOID
            cases; SELL/STRONG_SELL hint tightened from "Exit or
            avoid" to "Exit position". New module constant
            _RECO_NORMALIZE_CONTRACT_VERSION = "8.0.0" surfaced via
            get_canonical_state() as reco_normalize_contract_version.

What advanced in core.reco_normalize since v7.2.1
--------------------------------------------------
  v8.0.0: 8-tier canonical vocabulary. The Recommendation enum gains
            ACCUMULATE (ordinal 2, between BUY and HOLD),
            STRONG_SELL (ordinal 6, between SELL and AVOID), and
            AVOID (ordinal 7, the new worst tier). to_score() range
            widened from 0-4 to 0-7; WORST_ORDINAL exported as a
            symbolic literal (== 7). from_score() / _parse_numeric_rating
            re-banded for 8 tiers on the 0-100 and 0-1 scales.
            _classify_views_with_rule_id() can emit all 8 tiers; four
            new RULE_ID_* constants cover the new branches
            (ACCUMULATE_BULLISH_MODERATE, DOWNGRADED_ACCUMULATE_TO_HOLD,
            HARD_STRONG_SELL_DOUBLE_BEARISH, AVOID_UNINVESTABLE).
            New multilingual sets (AR/FR/ES/DE/PT) for ACCUMULATE
            and AVOID idioms. _PAT_AVOID_NEGATED distinguishes a
            genuine "not avoid" from AVOID idioms like "DO NOT BUY"
            where the embedded NOT/NEVER is intrinsic to the AVOID
            meaning. Conviction floor cascade extended: ACCUMULATE
            with conviction < BUY floor downgrades to HOLD.

Bridge changes in v3.6.0
------------------------

  Phase A -- Header docstring sync: floor moves to core.scoring v5.7.0+
             and core.reco_normalize v8.0.0+. Older versions still load
             cleanly via the getattr() fallback pattern; the degradation
             report flags them.

  Phase B -- Re-export the v5.6.0 / v5.7.0 contract-version markers
             (all via getattr() fallback so a deployment on pre-v5.6.0
             core.scoring still loads cleanly):
               * _ENGINE_CONTRACT_VERSION         (v5.6.0+;
                                                   value "5.75.0" --
                                                   data_engine_v2 release
                                                   scoring.py aligns with)
               * _RECO_NORMALIZE_CONTRACT_VERSION (v5.7.0+;
                                                   value "8.0.0" --
                                                   reco_normalize release
                                                   scoring.py aligns with)
             Audit tooling can read these via the bridge to verify
             cross-stack alignment without separately importing scoring.

  Phase C -- NEW _V570_FIELDS tuple listing the row fields added in
             v5.6.0 / v5.7.0 that are not in _V525 / _V528 / _V540:
               * recommendation_detailed  (v5.6.0+; mirror of
                                            `recommendation` for
                                            field-name compat with
                                            data_engine_v2 v5.75.0)
             Mirror of the existing _V525 / _V528 / _V540 field-
             contract constants. No removals from prior tuples.

  Phase D -- NEW _CONTRACT_VERSION_SYMBOLS tuple tracking the v5.6.0+/
             v5.7.0+ contract-marker surface. Tracked separately from
             _BUCKET_ALIGNMENT_SYMBOLS so a deployment running v5.3.0-
             v5.5.x (full bucket alignment but no contract markers
             yet) still gets a working bridge with a partial-alignment
             flag.

  Phase E -- NEW _RECO_8TIER_SYMBOLS tuple tracking the v8.0.0
             vocabulary-expansion surface in core.reco_normalize:
               * RECO_ACCUMULATE / RECO_STRONG_SELL / RECO_AVOID
                 (module-level string constants)
               * WORST_ORDINAL                       (== 7 in v8.0.0)
               * RULE_ID_ACCUMULATE_BULLISH_MODERATE,
                 RULE_ID_DOWNGRADED_ACCUMULATE_TO_HOLD,
                 RULE_ID_HARD_STRONG_SELL_DOUBLE_BEARISH,
                 RULE_ID_AVOID_UNINVESTABLE        (the four new
                                                    v8.0.0 RULE_IDs)
             Tracked on the reco_normalize side, not core.scoring.

  Phase F -- NEW is_contract_versioned() helper: returns True when
             both contract markers are present on core.scoring.
             Mirror of is_view_aware, is_audit_hardened,
             is_reco_rule_id_aware, is_cascade_bridged,
             is_bucket_aligned.

             NEW is_reco_8tier_aware() helper: returns True when
             both:
               (a) core.scoring's RECOMMENDATION_ENUM contains all 8
                   canonical tiers (verifies the v5.7.0 vocabulary
                   expansion actually landed; a deployment can have
                   RECOMMENDATION_ENUM as a tuple but with only 6
                   members if still on v5.6.0 or earlier)
               (b) core.reco_normalize exposes the v8.0.0
                   vocabulary symbols (RECO_ACCUMULATE, RECO_AVOID,
                   WORST_ORDINAL, and the four new RULE_ID_* constants)
             Both must be true for the full stack to be 8-tier-aware
             in lockstep. A False from this helper while
             is_bucket_aligned() is True indicates a partial
             rollout -- scoring may emit ACCUMULATE/AVOID while
             reco_normalize cannot represent them, or vice versa.

  Phase G -- Extended get_degradation_report():
               * engine_contract_version: str or None  (the actual
                 value from _ENGINE_CONTRACT_VERSION marker on
                 core.scoring; not just symbol presence)
               * reco_normalize_contract_version: str or None  (same
                 for the v5.7.0 marker)
               * recommendation_enum: tuple  (the actual resolved
                 enum tuple, so audit tooling can verify the v5.7.0
                 expansion -- 6 tiers vs 8 tiers -- without
                 separately importing core.scoring)
               * contract_versioned_symbols: {symbol: present_bool}
               * missing_contract_versioned: [symbols]
               * contract_versioned_enabled: bool
               * reco_8tier_symbols: {symbol: present_bool}
               * missing_reco_8tier: [symbols]
               * reco_8tier_aware_enabled: bool
               * v570_fields: tuple  (mirror of v540_detail_fields)
             Existing report fields preserved verbatim.

  Phase H -- Version bump 3.5.0 -> 3.6.0. Minor bump because the
             public re-export surface widens; no breaking changes
             and no removals.

[PRESERVED -- strictly]
  - Every v3.5.0 public name remains exported.
  - is_view_aware, is_audit_hardened, is_reco_rule_id_aware,
    is_cascade_bridged, is_bucket_aligned all unchanged.
  - _V525_ENRICHMENT_FIELDS / _V528_CASCADE_BRIDGE_FIELDS /
    _V540_DETAIL_FIELDS unchanged.
  - _BUCKET_ALIGNMENT_SYMBOLS unchanged (additions in v3.6.0 go
    into separate _CONTRACT_VERSION_SYMBOLS and _RECO_8TIER_SYMBOLS
    tuples so a deployment on v5.3.0-v5.5.x still passes the
    is_bucket_aligned() check).
  - RECOMMENDATION_SOURCE_TAG, CANONICAL_PRIORITIES, PRIO_P1..PRIO_P5
    auto-track core.scoring's __version__ via its f-string.

API surface (additions in v3.6.0):
  - _ENGINE_CONTRACT_VERSION, _RECO_NORMALIZE_CONTRACT_VERSION
    (re-exports of the v5.6.0 / v5.7.0 contract markers)
  - _V570_FIELDS (private constant)
  - _CONTRACT_VERSION_SYMBOLS (private constant)
  - _RECO_8TIER_SYMBOLS (private constant)
  - is_contract_versioned (bridge-level helper)
  - is_reco_8tier_aware (bridge-level helper)

================================================================================
v3.5.0 changes (preserved verbatim)
================================================================================

core.scoring has advanced through v5.3.0 -> v5.4.0 -> v5.4.1 -> v5.4.2
-> v5.5.0 since v3.4.4 was cut. The bridge was operating correctly
against all of those versions thanks to the getattr() fallback
pattern, but was under-advertising the new surface (downstream
modules importing via the bridge could not reach symbols added since
v5.2.8). v3.5.0 closed that gap.

What advanced in core.scoring since v5.2.8
-------------------------------------------
  v5.3.0: Compatibility wrappers for data_engine_v2 v5.71.0
            (_recommendation, _risk_bucket, _confidence_bucket).
            Closed final recommendation enum
            (STRONG_BUY/BUY/HOLD/REDUCE/SELL/STRONG_SELL exported as
            RECOMMENDATION_ENUM). Module-level threshold constants
            (RISK_BUCKET_THRESHOLDS, CONFIDENCE_BUCKET_THRESHOLDS).
            Nullable overall_score for insufficient inputs.
            opportunity_source field with provenance. New
            compute_opportunity_score_with_source helper.
            scoring_schema_version field.

  v5.4.0: Horizon-aware ROI selection. Forecast patch full overwrite
            (clears stale ROI). Valuation ratio-only fallback.
            Provider sanity (dividend yield, debt/equity).
            recommendation_detail field. derive_risk_view now
            delegates to risk_bucket (single source of truth).

  v5.4.1: recommendation_detail rebuild after batch insights pass.
            _active_roi_for_horizon truthful FALLBACK labels.
            ScoringEngine clones DEFAULT_SCORING_WEIGHTS /
            DEFAULT_FORECAST_PARAMETERS. risk_moderate_threshold
            compat alias property. -0.70 unit-mismatch floor.

  v5.4.2: Mid-text legacy label substitution narrowed to finance
            jargon (no longer corrupts narrative containing common
            English words like WATCH / ACCUMULATE / NEUTRAL).

  v5.5.0: Env-tunable bucket thresholds (SCORING_RISK_LOW,
            SCORING_RISK_HIGH, SCORING_CONFIDENCE_MODERATE,
            SCORING_CONFIDENCE_HIGH). scoring_errors deduplication
            in compute_scores. opportunity_source granularity
            (valuation_only_fallback / momentum_only_fallback /
            both_present_fallback / roi_based / insufficient).
            Public diagnostic helpers get_canonical_thresholds() and
            get_canonical_state() for ops tooling.

Bridge changes in v3.5.0
------------------------

  Phase A -- Header docstring sync: reference core.scoring v5.4.2+
             (or v5.5.0+) as canonical floor. Enumerate the new
             public surface.

  Phase B -- Re-export new public symbols (all via getattr() fallback
             so a deployment still on a pre-v5.4.2 core.scoring gets
             a working bridge with degraded-mode report rather than
             ImportError):
               * RECOMMENDATION_ENUM           (v5.3.0+)
               * RISK_BUCKET_THRESHOLDS        (v5.3.0+)
               * CONFIDENCE_BUCKET_THRESHOLDS  (v5.3.0+)
               * BUCKETS_CANONICAL             (v5.2.9+)
               * compute_opportunity_score_with_source  (v5.3.0+)
               * get_canonical_thresholds      (v5.5.0+)
               * get_canonical_state           (v5.5.0+)
               * DEFAULT_SCORING_WEIGHTS       (alias completion of
                                                DEFAULT_WEIGHTS)
               * DEFAULT_FORECAST_PARAMETERS   (alias completion of
                                                DEFAULT_FORECASTS)

  Phase C -- Re-export the private compatibility wrappers added in
             v5.3.0 for data_engine_v2 v5.71.0, in case any consumer
             imports them via the bridge:
               * _recommendation
               * _risk_bucket
               * _confidence_bucket

  Phase D -- NEW _V540_DETAIL_FIELDS tuple listing the row fields
             added in v5.3.0 / v5.4.0 that are not already in
             _V525_ENRICHMENT_FIELDS or _V528_CASCADE_BRIDGE_FIELDS:
               * recommendation_detail   (v5.4.0+; "P# [VERDICT]: ...")
               * scoring_schema_version  (v5.3.0+; equals SCORING_VERSION)
               * opportunity_source      (v5.3.0+; provenance tag)
             Mirror of the existing _V525 / _V528 field-contract
             constants.

  Phase E -- NEW _BUCKET_ALIGNMENT_SYMBOLS tuple tracking the v5.3.0+
             bucket-canonical public surface. Tracked separately from
             the cascade-bridge symbols so a deployment running
             v5.2.7-v5.2.9 still gets a working bridge with a
             partial-alignment flag.

  Phase F -- NEW is_bucket_aligned() helper, mirror of is_view_aware,
             is_audit_hardened, is_reco_rule_id_aware,
             is_cascade_bridged. Quick boolean for
             "are we on core.scoring v5.3.0+ with the canonical bucket
             surface?"

  Phase G -- Extended get_degradation_report():
               * bucket_alignment_symbols: {symbol: present_bool}
               * missing_bucket_alignment: [symbols]
               * bucket_alignment_enabled: bool
               * risk_thresholds: tuple  (actual resolved values at
                                          core.scoring import time)
               * confidence_thresholds: tuple  (same)
               * buckets_canonical_enabled: bool  (mirrors
                                                   BUCKETS_CANONICAL)
               * v540_detail_fields: tuple
             Existing report fields preserved verbatim.

  Phase H -- Version bump 3.4.4 -> 3.5.0. Minor bump because the
             public re-export surface widens; no breaking changes
             and no removals.

[PRESERVED -- strictly]
  - Every v3.4.4 public name remains exported.
  - is_view_aware, is_audit_hardened, is_reco_rule_id_aware,
    is_cascade_bridged all unchanged.
  - _V525_ENRICHMENT_FIELDS unchanged.
  - _V528_CASCADE_BRIDGE_FIELDS unchanged (this is the v5.2.8 field-
    rename marker, not a generic "current contract" handle; renaming
    it to _V542_* would break callers that already read it).
  - RECOMMENDATION_SOURCE_TAG, CANONICAL_PRIORITIES, PRIO_P1..PRIO_P5
    auto-track core.scoring's __version__ via its f-string.

API surface (additions in v3.5.0):
  - RECOMMENDATION_ENUM, RISK_BUCKET_THRESHOLDS,
    CONFIDENCE_BUCKET_THRESHOLDS, BUCKETS_CANONICAL,
    compute_opportunity_score_with_source
  - get_canonical_thresholds, get_canonical_state (v5.5.0)
  - DEFAULT_SCORING_WEIGHTS, DEFAULT_FORECAST_PARAMETERS
    (alias completion)
  - _recommendation, _risk_bucket, _confidence_bucket (private compat)
  - _V540_DETAIL_FIELDS (private constant)
  - _BUCKET_ALIGNMENT_SYMBOLS (private constant)
  - is_bucket_aligned (bridge-level helper)

================================================================================
v3.4.4 changes (preserved verbatim)
================================================================================
Field-name collision fix sync: core.scoring v5.2.7 -> v5.2.8 renamed
the canonical-priority row field from `recommendation_priority` to
`recommendation_priority_band` to avoid collision with the schema
v2.7.0 / data_engine_v2 v5.60.0 Decision Matrix surface, which also
emits a row field named `recommendation_priority` but with different
semantics (int 1..8 for which 8-tier classifier rule fired, vs
string "P1".."P5" for the canonical priority band). The bridge
synced to this rename: _V527_CASCADE_BRIDGE_FIELDS ->
_V528_CASCADE_BRIDGE_FIELDS, element rename inside the tuple,
get_degradation_report() field rename.

================================================================================
v3.4.3 changes (preserved verbatim)
================================================================================
v5.2.7 cascade-bridge awareness: re-export of
derive_canonical_recommendation, apply_canonical_recommendation,
RECOMMENDATION_SOURCE_TAG, CANONICAL_PRIORITIES, PRIO_P1..PRIO_P5.
_V527/_V528_CASCADE_BRIDGE_FIELDS tuple. _CASCADE_BRIDGE_SYMBOLS
category and is_cascade_bridged() helper. Extended
get_degradation_report() with cascade-bridge tracking.

================================================================================
v3.4.2 and earlier (preserved verbatim)
================================================================================
- __version__ = VERSION alias (v3.4.2).
- _V525_ENRICHMENT_FIELDS, _RECO_RULE_ID_SYMBOLS,
  is_reco_rule_id_aware() (v3.4.2).
- score_views_completeness re-export, _AUDIT_HARDENING_SYMBOLS,
  is_audit_hardened() (v3.4.1).
- Four view-derivation symbols and view-awareness reporting (v3.4.0).
- Bridge contract for the v4.x scoring family (v3.3.x and earlier).

API surface (preserved exports):
- VERSION, __version__
- ScoringEngine, AssetScores, ScoreWeights, ScoringWeights,
  ForecastParameters, DEFAULT_WEIGHTS, DEFAULT_FORECASTS
- Horizon, Signal, RSISignal
- compute_scores, score_row, score_quote, enrich_with_scores
- rank_rows_by_overall, assign_rank_overall, score_and_rank_rows
- normalize_recommendation_code, CANONICAL_RECOMMENDATION_CODES
- detect_horizon, get_weights_for_horizon
- compute_technical_score, rsi_signal, short_term_signal
- derive_upside_pct, derive_volume_ratio, derive_day_range_position
- invest_period_label
- compute_valuation_score, compute_growth_score, compute_momentum_score
- compute_quality_score, compute_risk_score, compute_opportunity_score
- compute_confidence_score, compute_recommendation
- risk_bucket, confidence_bucket
- derive_fundamental_view, derive_technical_view,
  derive_risk_view, derive_value_view
- score_views_completeness
- ScoringError, InvalidHorizonError, MissingDataError
- get_degradation_report, is_view_aware, is_audit_hardened,
  is_reco_rule_id_aware, is_cascade_bridged, is_bucket_aligned [v3.5.0]
- _V525_ENRICHMENT_FIELDS
- _V528_CASCADE_BRIDGE_FIELDS
- _V540_DETAIL_FIELDS [v3.5.0]
- derive_canonical_recommendation, apply_canonical_recommendation
- RECOMMENDATION_SOURCE_TAG, CANONICAL_PRIORITIES
- PRIO_P1..PRIO_P5
- RECOMMENDATION_ENUM, RISK_BUCKET_THRESHOLDS,
  CONFIDENCE_BUCKET_THRESHOLDS, BUCKETS_CANONICAL [v3.5.0]
- compute_opportunity_score_with_source [v3.5.0]
- get_canonical_thresholds, get_canonical_state [v3.5.0]
- DEFAULT_SCORING_WEIGHTS, DEFAULT_FORECAST_PARAMETERS [v3.5.0]
- _recommendation, _risk_bucket, _confidence_bucket [v3.5.0]
================================================================================
"""

from __future__ import annotations

import logging
import warnings
from typing import Any, Dict, List, Optional, Sequence, Tuple

logger = logging.getLogger(__name__)
logger.addHandler(logging.NullHandler())

# ---------------------------------------------------------------------------
# Version
# ---------------------------------------------------------------------------

VERSION = "3.6.0"
# v3.4.2 Phase B (preserved): __version__ alias matches TFB module
# convention used by core.scoring v5.2.5+, core.reco_normalize v7.2.0+,
# and insights_builder v7.0.0.
__version__ = VERSION


# =============================================================================
# Resolve the canonical core.scoring module
# =============================================================================
#
# We try multiple import paths so this bridge works whether it's mounted
# at `core.scoring_engine`, top-level `scoring_engine`, or pulled in via
# a sys.path tweak in tests.

_core_scoring = None
_import_attempt_log: List[str] = []

for _module_path in ("core.scoring", "scoring"):
    try:
        _core_scoring = __import__(_module_path, fromlist=["*"])
        _import_attempt_log.append(f"OK: {_module_path}")
        break
    except ImportError as exc:
        _import_attempt_log.append(f"FAIL: {_module_path} ({exc})")
        continue

if _core_scoring is None:
    raise ImportError(
        "core.scoring is required by scoring_engine but could not be "
        "imported. Attempts: " + " | ".join(_import_attempt_log)
    )

# =============================================================================
# Resolve the canonical reco_normalize module (for the degradation manifest)
# =============================================================================

_reco_normalize = None
for _module_path in ("core.reco_normalize", "reco_normalize"):
    try:
        _reco_normalize = __import__(_module_path, fromlist=["*"])
        break
    except ImportError:
        continue

# =============================================================================
# Re-export everything the original scoring_engine exposed
# =============================================================================

# Versions
SCORING_VERSION = getattr(_core_scoring, "__version__", "0.0.0")

# Enums
Horizon = _core_scoring.Horizon
Signal = _core_scoring.Signal
RSISignal = _core_scoring.RSISignal

# Data classes
AssetScores = _core_scoring.AssetScores
ScoreWeights = _core_scoring.ScoreWeights
ScoringWeights = _core_scoring.ScoringWeights
ForecastParameters = _core_scoring.ForecastParameters

# Defaults (v3.4.x preserved short aliases)
DEFAULT_WEIGHTS = _core_scoring.DEFAULT_WEIGHTS
DEFAULT_FORECASTS = _core_scoring.DEFAULT_FORECASTS

# v3.5.0 Phase B: alias completion. core.scoring v5.3.0+ exposes both
# DEFAULT_SCORING_WEIGHTS (canonical name) and DEFAULT_WEIGHTS (alias)
# pointing at the same ScoreWeights() instance. We re-export both so
# downstream code that uses either name reaches the bridge symbol
# table successfully. getattr() fallback handles deployments on
# older core.scoring releases that only expose the short name.
DEFAULT_SCORING_WEIGHTS = getattr(_core_scoring, "DEFAULT_SCORING_WEIGHTS", DEFAULT_WEIGHTS)
DEFAULT_FORECAST_PARAMETERS = getattr(
    _core_scoring, "DEFAULT_FORECAST_PARAMETERS", DEFAULT_FORECASTS,
)

# Engine class (object-style API)
ScoringEngine = _core_scoring.ScoringEngine

# Exceptions
ScoringError = _core_scoring.ScoringError
InvalidHorizonError = _core_scoring.InvalidHorizonError
MissingDataError = _core_scoring.MissingDataError

# Horizon helpers
detect_horizon = _core_scoring.detect_horizon
get_weights_for_horizon = _core_scoring.get_weights_for_horizon

# Component scoring functions
compute_technical_score = _core_scoring.compute_technical_score
compute_valuation_score = _core_scoring.compute_valuation_score
compute_growth_score = _core_scoring.compute_growth_score
compute_momentum_score = _core_scoring.compute_momentum_score
compute_quality_score = _core_scoring.compute_quality_score
compute_risk_score = _core_scoring.compute_risk_score
compute_opportunity_score = _core_scoring.compute_opportunity_score
compute_confidence_score = _core_scoring.compute_confidence_score

# v3.5.0 Phase B: opportunity score with provenance source tag
# (v5.3.0+). Returns (score, source_tag) where source_tag is one of
# "roi_based" / "valuation_only_fallback" / "momentum_only_fallback" /
# "both_present_fallback" / "insufficient". The three "_fallback"
# variants are v5.5.0 granularity over the prior single
# "valuation_momentum_fallback" tag. Getattr fallback so pre-v5.3.0
# deployments still load this bridge cleanly.
compute_opportunity_score_with_source = getattr(
    _core_scoring, "compute_opportunity_score_with_source", None,
)

# Signal helpers
rsi_signal = _core_scoring.rsi_signal
short_term_signal = _core_scoring.short_term_signal

# Derived field helpers
derive_upside_pct = _core_scoring.derive_upside_pct
derive_volume_ratio = _core_scoring.derive_volume_ratio
derive_day_range_position = _core_scoring.derive_day_range_position

# View derivation helpers (v3.4.0, sourced from core.scoring v5.0.0+)
derive_fundamental_view = getattr(_core_scoring, "derive_fundamental_view", None)
derive_technical_view = getattr(_core_scoring, "derive_technical_view", None)
derive_risk_view = getattr(_core_scoring, "derive_risk_view", None)
derive_value_view = getattr(_core_scoring, "derive_value_view", None)

# v3.4.1: View completeness verification helper, sourced from
# core.scoring v5.2.0+. Falls back to None when the deployed scoring
# module predates v5.2.0; the degradation report flags this case.
score_views_completeness = getattr(_core_scoring, "score_views_completeness", None)

# Period / horizon labels
invest_period_label = _core_scoring.invest_period_label

# Recommendation
compute_recommendation = _core_scoring.compute_recommendation
normalize_recommendation_code = _core_scoring.normalize_recommendation_code
CANONICAL_RECOMMENDATION_CODES = _core_scoring.CANONICAL_RECOMMENDATION_CODES

# v3.5.0 Phase B: closed final recommendation enum (v5.3.0+). Same
# tuple as CANONICAL_RECOMMENDATION_CODES; surfaced under its public
# name for symmetry with the upstream contract. getattr() fallback
# for pre-v5.3.0 deployments.
RECOMMENDATION_ENUM = getattr(_core_scoring, "RECOMMENDATION_ENUM", CANONICAL_RECOMMENDATION_CODES)

# Buckets
risk_bucket = _core_scoring.risk_bucket
confidence_bucket = _core_scoring.confidence_bucket

# v3.5.0 Phase B: module-level threshold constants (v5.3.0+). Surfaced
# so audit / diagnostic tooling can read the canonical thresholds
# without parsing env vars or guessing. Each is a tuple (low, high)
# for risk (0-100 scale) and (moderate_floor, high_floor) for
# confidence (0-1 fraction). getattr() fallback returns None for
# pre-v5.3.0 deployments; consumers should treat None as
# "thresholds not exposed" and fall back to risk_bucket()/
# confidence_bucket() calls directly.
RISK_BUCKET_THRESHOLDS = getattr(_core_scoring, "RISK_BUCKET_THRESHOLDS", None)
CONFIDENCE_BUCKET_THRESHOLDS = getattr(_core_scoring, "CONFIDENCE_BUCKET_THRESHOLDS", None)

# v3.5.0 Phase B: canonical-bucket availability flag (v5.2.9+). True
# when core.scoring is routing its bucket helpers through core.buckets
# rather than the local fallback; False otherwise. None when the
# core.scoring deployment predates the flag entirely.
BUCKETS_CANONICAL = getattr(_core_scoring, "BUCKETS_CANONICAL", None)

# v3.5.0 Phase B: v5.5.0 ops diagnostic helpers. Both are no-arg
# functions returning a dict. get_canonical_thresholds() returns
# just the resolved threshold pairs + env-override flags;
# get_canonical_state() returns a full ops snapshot (version, source
# tag, BUCKETS_CANONICAL, thresholds, env-override flags,
# recommendation_enum, canonical_priorities, active ScoringConfig).
# Both are intended for diagnostic / audit tooling that needs to
# confirm what the running scoring process is actually using.
get_canonical_thresholds = getattr(_core_scoring, "get_canonical_thresholds", None)
get_canonical_state = getattr(_core_scoring, "get_canonical_state", None)

# Main scoring entry points
compute_scores = _core_scoring.compute_scores
score_row = _core_scoring.score_row
score_quote = _core_scoring.score_quote
enrich_with_scores = _core_scoring.enrich_with_scores

# Ranking helpers
rank_rows_by_overall = _core_scoring.rank_rows_by_overall
assign_rank_overall = _core_scoring.assign_rank_overall
score_and_rank_rows = _core_scoring.score_and_rank_rows


# =============================================================================
# v3.5.0 Phase C — Private compatibility wrappers (defensive re-export)
# =============================================================================
#
# core.scoring v5.3.0 introduced three private-prefixed wrappers
# specifically so data_engine_v2 v5.71.0 could import them without
# falling back to conservative HOLD defaults. The canonical import
# target is `from core.scoring import _recommendation, _risk_bucket,
# _confidence_bucket`, but we re-export them via the bridge too in
# case any module imports them from `core.scoring_engine` instead.
#
# All getattr() so pre-v5.3.0 deployments load this bridge cleanly
# with None values rather than AttributeError at import time. The
# bucket_alignment degradation report flags pre-v5.3.0 deployments.

_recommendation = getattr(_core_scoring, "_recommendation", None)
_risk_bucket = getattr(_core_scoring, "_risk_bucket", None)
_confidence_bucket = getattr(_core_scoring, "_confidence_bucket", None)


# =============================================================================
# v3.6.0 Phase B — Re-export core.scoring v5.6.0+/v5.7.0+ contract markers
# =============================================================================
#
# core.scoring v5.6.0 introduced _ENGINE_CONTRACT_VERSION ("5.75.0") to
# document which data_engine_v2 release scoring.py was built to align
# with. v5.7.0 introduced _RECO_NORMALIZE_CONTRACT_VERSION ("8.0.0") to
# document which core.reco_normalize release scoring.py was built to
# align with. Both are surfaced via core.scoring.get_canonical_state()
# under the keys engine_contract_version and reco_normalize_contract_version
# respectively.
#
# Re-exporting via the bridge so audit tooling that connects through
# scoring_engine (e.g. investment_advisor_engine, data_engine_v2) can
# inspect both markers without separately importing core.scoring.
#
# All getattr() with a None default. A None value here means
# "deployed core.scoring predates the marker"; consumers should treat
# None as "unknown / pre-marker era" and fall back to inspecting
# SCORING_VERSION directly. The contract_versioned degradation flag
# captures which markers are missing.

_ENGINE_CONTRACT_VERSION = getattr(
    _core_scoring, "_ENGINE_CONTRACT_VERSION", None,
)
_RECO_NORMALIZE_CONTRACT_VERSION = getattr(
    _core_scoring, "_RECO_NORMALIZE_CONTRACT_VERSION", None,
)


# =============================================================================
# v3.4.3 Phase B — Re-export core.scoring v5.2.7+ cascade-bridge symbols
# =============================================================================
#
# core.scoring v5.2.7 adds the canonical recommendation source-of-truth
# surface. The bridge re-exports these helpers + constants so downstream
# engines (investment_advisor_engine, data_engine_v2) can import either
# from core.scoring OR core.scoring_engine and get identical behavior.
#
# All getattr() fallbacks so a deployment still running core.scoring
# v5.2.0-v5.2.6 gets None values and a clean degradation report,
# rather than an ImportError that would brick the bridge entirely.

derive_canonical_recommendation = getattr(
    _core_scoring, "derive_canonical_recommendation", None,
)
apply_canonical_recommendation = getattr(
    _core_scoring, "apply_canonical_recommendation", None,
)

# v5.2.7 module constants. Each falls back to a sentinel so downstream
# code can detect "running pre-v5.2.7" by comparing against the
# sentinel value rather than catching AttributeError.
RECOMMENDATION_SOURCE_TAG = getattr(
    _core_scoring, "RECOMMENDATION_SOURCE_TAG", None,
)
CANONICAL_PRIORITIES = getattr(
    _core_scoring, "CANONICAL_PRIORITIES", tuple(),
)
PRIO_P1 = getattr(_core_scoring, "PRIO_P1", None)
PRIO_P2 = getattr(_core_scoring, "PRIO_P2", None)
PRIO_P3 = getattr(_core_scoring, "PRIO_P3", None)
PRIO_P4 = getattr(_core_scoring, "PRIO_P4", None)
PRIO_P5 = getattr(_core_scoring, "PRIO_P5", None)


# =============================================================================
# v3.4.2 Phase C — v5.2.5 enrichment row-field contract (PRESERVED)
# =============================================================================
#
# core.scoring v5.2.5 added four row fields produced by compute_scores
# / score_row / enrich_with_scores. They are dict entries on the
# returned row, not standalone functions, so they don't need
# re-export. But callers benefit from knowing what to expect.

_V525_ENRICHMENT_FIELDS: Tuple[str, ...] = (
    "conviction_score",     # float, 0-100
    "top_factors",          # str, "++"-prefixed list of contributing factors
    "top_risks",            # str, "--"-prefixed list of contributing risks
    "position_size_hint",   # str, sizing suggestion (e.g. "Standard 5%")
)


# =============================================================================
# v3.4.3 Phase C — v5.2.7/v5.2.8 cascade-bridge row-field contract (PRESERVED)
# =============================================================================
#
# core.scoring v5.2.7 adds two row fields produced by compute_scores
# / score_row / enrich_with_scores. v5.2.8 renamed the first one to
# resolve a collision with schema v2.7.0. Mirror to
# _V525_ENRICHMENT_FIELDS.

_V528_CASCADE_BRIDGE_FIELDS: Tuple[str, ...] = (
    "recommendation_priority_band",  # str; "P1".."P5" canonical priority bucket
    "recommendation_source",    # str; "scoring.py vX.Y.Z" provenance tag
)


# =============================================================================
# v3.5.0 Phase D — v5.3.0+/v5.4.0+ detail-field contract (NEW)
# =============================================================================
#
# core.scoring v5.3.0 and v5.4.0 added three additional row fields
# produced by compute_scores that are not in _V525 or _V528 above.
# Tracked separately so consumers can introspect "what fields does
# a row carry after v5.4.0+ scoring" without re-deriving from scratch.
#
#   recommendation_detail   (v5.4.0+):
#       Format: "P# [VERDICT]: <structured reason>"
#       Built from priority + canonical_rec + structured_reason at the
#       end of compute_scores. Rebuilt by score_and_rank_rows after
#       the batch insights pass so it cannot become stale.
#
#   scoring_schema_version  (v5.3.0+):
#       String equal to core.scoring.SCORING_VERSION. Useful for
#       cache invalidation: if a row's recorded version doesn't match
#       the running version, the row should be re-scored.
#
#   opportunity_source      (v5.3.0+; granularity added in v5.5.0):
#       Provenance tag on the opportunity_score field. One of:
#         - "roi_based"
#         - "valuation_only_fallback"   (v5.5.0+)
#         - "momentum_only_fallback"    (v5.5.0+)
#         - "both_present_fallback"     (v5.5.0+)
#         - "valuation_momentum_fallback"  (v5.3.0-v5.4.2, legacy)
#         - "insufficient"
#       Pre-v5.5.0 deployments emit the legacy single fallback tag.
#
# Consumers downstream:
#   - Apps Script 04_Format.gs: reads recommendation_detail directly
#     instead of reconstructing it from rec + priority + reason.
#   - audit_data_quality / Diagnostic_Report: groups by
#     opportunity_source for "where do opportunity scores come from"
#     audits.
#   - Any cache layer: invalidates rows whose scoring_schema_version
#     doesn't match the running SCORING_VERSION.

_V540_DETAIL_FIELDS: Tuple[str, ...] = (
    "recommendation_detail",      # str; "P# [VERDICT]: reason"  (v5.4.0+)
    "scoring_schema_version",     # str; equals SCORING_VERSION  (v5.3.0+)
    "opportunity_source",         # str; opportunity provenance  (v5.3.0+)
)


# =============================================================================
# v3.6.0 Phase C — v5.6.0+/v5.7.0+ row-field contract (NEW)
# =============================================================================
#
# core.scoring v5.6.0 added one additional row field produced by
# compute_scores that is not in _V525 / _V528 / _V540 above.
# Tracked separately so consumers can introspect "what fields does a
# row carry after v5.6.0+ scoring" without re-deriving from scratch.
#
#   recommendation_detailed  (v5.6.0+):
#       Mirror of `recommendation`. Always carries the same canonical
#       8-tier code as `recommendation` (STRONG_BUY / BUY / ACCUMULATE
#       / HOLD / REDUCE / SELL / STRONG_SELL / AVOID after v5.7.0).
#       data_engine_v2 v5.75.0 writes both atomically in
#       _classify_recommendation_8tier and treats them as a single
#       unit during the patch merge. The mirror exists for field-
#       name compatibility with consumers that read
#       `recommendation_detailed` directly (some Apps Script reads,
#       and the schema_registry v2.11.0+ documents both columns).
#
# Consumers downstream:
#   - schema_registry v2.11.0+: documents recommendation_detailed
#     alongside recommendation; both columns carry the same value
#     by invariant.
#   - data_engine_v2 v5.75.0+: writes the mirror atomically; the
#     owned-keys filter strips both during the patch merge so the
#     producer/consumer contract is preserved.
#   - Apps Script 04_Format.gs: may read either column; both render
#     the same canonical token.
#
# Note: this tuple lists only the v5.6.0+ row-field additions. The
# v5.7.0 release expanded the value domain of `recommendation` and
# `recommendation_detailed` from 6 tiers to 8 but did not introduce
# new row fields. The vocabulary expansion is captured by
# is_reco_8tier_aware() (a content check on RECOMMENDATION_ENUM), not
# by a field-contract tuple.

_V570_FIELDS: Tuple[str, ...] = (
    "recommendation_detailed",    # str; mirror of `recommendation`  (v5.6.0+)
)


# =============================================================================
# Degradation Report
# =============================================================================
#
# Returns a structured snapshot of which scoring symbols resolved and
# which fell back. Useful for diagnosing whether the deployed
# scoring_engine is properly wired up to the v5.0.0+ view-aware logic
# AND the v5.2.0+ audit-hardening surface AND the v5.2.7+ cascade-
# bridge surface AND the v5.3.0+ bucket-alignment surface, vs an
# older scoring module.

# v5.0.0-era contract: bridge requires these to function at all.
# Missing any of them is "degraded mode".
_REQUIRED_CORE_SYMBOLS: Tuple[str, ...] = (
    "compute_scores",
    "score_row",
    "compute_recommendation",
    "compute_quality_score",
    "compute_valuation_score",
    "compute_momentum_score",
    "compute_risk_score",
    "compute_opportunity_score",
    "compute_confidence_score",
    "compute_growth_score",
    "compute_technical_score",
    "AssetScores",
    "ScoreWeights",
    "ScoringEngine",
    "DEFAULT_WEIGHTS",
    "DEFAULT_FORECASTS",
    "Horizon",
    "Signal",
    "RSISignal",
)

# View-derivation symbols added in scoring v5.0.0 / scoring_engine v3.4.0.
_VIEW_SYMBOLS: Tuple[str, ...] = (
    "derive_fundamental_view",
    "derive_technical_view",
    "derive_risk_view",
    "derive_value_view",
)

# v3.4.1: Audit-hardening symbols added in scoring v5.2.0+.
_AUDIT_HARDENING_SYMBOLS: Tuple[str, ...] = (
    "score_views_completeness",
)

# v3.4.2 Phase D: reco_normalize v7.2.0+ rule_id introspection symbols.
_RECO_RULE_ID_SYMBOLS: Tuple[str, ...] = (
    "recommendation_from_views_with_rule_id",
    "RULE_ID_INSUFFICIENT_DATA_NO_SIGNALS",
    "RULE_ID_STRONG_BUY_ALL",
    "RULE_ID_HOLD_BULLISH_EXPENSIVE_VETO",
    "RULE_ID_HOLD_MIXED",
)

# v3.4.3 Phase D: core.scoring v5.2.7+ cascade-bridge symbols.
_CASCADE_BRIDGE_SYMBOLS: Tuple[str, ...] = (
    "derive_canonical_recommendation",
    "apply_canonical_recommendation",
    "RECOMMENDATION_SOURCE_TAG",
    "CANONICAL_PRIORITIES",
    "PRIO_P1",
    "PRIO_P2",
    "PRIO_P3",
    "PRIO_P4",
    "PRIO_P5",
)

# v3.5.0 Phase E: core.scoring v5.3.0+ bucket-alignment surface.
# Tracks the public bucket-canonical contract. v5.2.9 introduced
# BUCKETS_CANONICAL alone; v5.3.0 added the threshold constants, the
# closed recommendation enum, and the opportunity-source helper.
# v5.5.0 added the two diagnostic helpers. Missing any of these
# means "running pre-v5.5.0 -- bucket math may be inline rather than
# canonical, and ops cannot inspect resolved thresholds via the
# diagnostic helpers". Optional category: the bridge still functions,
# but downstream audit/diagnostic tooling loses introspection.
_BUCKET_ALIGNMENT_SYMBOLS: Tuple[str, ...] = (
    "RECOMMENDATION_ENUM",
    "RISK_BUCKET_THRESHOLDS",
    "CONFIDENCE_BUCKET_THRESHOLDS",
    "BUCKETS_CANONICAL",
    "compute_opportunity_score_with_source",
    "_recommendation",
    "_risk_bucket",
    "_confidence_bucket",
    "get_canonical_thresholds",
    "get_canonical_state",
)

# v3.6.0 Phase D: core.scoring v5.6.0+/v5.7.0+ contract-version surface.
# Tracks the two markers that document which downstream contracts
# scoring.py was built against. _ENGINE_CONTRACT_VERSION (v5.6.0+)
# names the data_engine_v2 release; _RECO_NORMALIZE_CONTRACT_VERSION
# (v5.7.0+) names the core.reco_normalize release.
#
# Tracked separately from _BUCKET_ALIGNMENT_SYMBOLS so a deployment
# on v5.3.0-v5.5.x (full bucket-canonical surface but no contract
# markers yet) still passes is_bucket_aligned() while honestly
# reporting that contract-version introspection is unavailable.
#
# Missing either marker means the deployed core.scoring is on
# pre-v5.6.0 (engine marker absent) or pre-v5.7.0 (reco_normalize
# marker absent). The bridge function is unaffected -- this is
# diagnostic surface only.
_CONTRACT_VERSION_SYMBOLS: Tuple[str, ...] = (
    "_ENGINE_CONTRACT_VERSION",
    "_RECO_NORMALIZE_CONTRACT_VERSION",
)

# v3.6.0 Phase E: core.reco_normalize v8.0.0+ vocabulary-expansion
# surface. Tracks the symbols added when the canonical recommendation
# enum was widened from 5 tiers to 8.
#
# RECO_ACCUMULATE / RECO_STRONG_SELL / RECO_AVOID are the new
# module-level string constants that complete the 8-tier value set
# (the existing RECO_BUY / RECO_HOLD / RECO_REDUCE / RECO_SELL /
# RECO_STRONG_BUY were preserved from v7.x). WORST_ORDINAL is a
# symbolic literal (== 7 in v8.0.0; was 4 in v7.x) so downstream
# code never has to hard-code the worst-tier ordinal.
#
# The four RULE_ID_* constants cover the new branches in
# _classify_views_with_rule_id(): ACCUMULATE_BULLISH_MODERATE
# (scale-in branch, between BUY and HOLD), DOWNGRADED_ACCUMULATE_TO_HOLD
# (conviction floor cascade for ACCUMULATE),
# HARD_STRONG_SELL_DOUBLE_BEARISH (escalation from SELL when
# double-bearish compounds with high risk or very weak score), and
# AVOID_UNINVESTABLE (the uninvestable branch -- score < 15 or
# bearish-bearish-expensive).
#
# Missing any of these symbols means the deployed core.reco_normalize
# is on pre-v8.0.0. A False from the resulting is_reco_8tier_aware()
# helper while is_bucket_aligned() is True indicates a partial
# rollout (scoring.py v5.7.0+ emits 8 tiers but reco_normalize is
# still on v7.2.x and cannot represent ACCUMULATE/AVOID) -- a
# vocabulary mismatch that would silently round-trip incorrectly
# through normalize_recommendation_code().
_RECO_8TIER_SYMBOLS: Tuple[str, ...] = (
    "RECO_ACCUMULATE",
    "RECO_STRONG_SELL",
    "RECO_AVOID",
    "WORST_ORDINAL",
    "RULE_ID_ACCUMULATE_BULLISH_MODERATE",
    "RULE_ID_DOWNGRADED_ACCUMULATE_TO_HOLD",
    "RULE_ID_HARD_STRONG_SELL_DOUBLE_BEARISH",
    "RULE_ID_AVOID_UNINVESTABLE",
)

_REQUIRED_RECO_SYMBOLS: Tuple[str, ...] = (
    "Recommendation",
    "normalize_recommendation",
    "is_valid_recommendation",
    "recommendation_from_score",
    "recommendation_from_views",  # Added in reco_normalize v7.0.0
)


def get_degradation_report() -> Dict[str, Any]:
    """
    Return a structured report of which expected symbols are present.

    The report includes:
      - bridge_version: this module's VERSION
      - scoring_version: the version of core.scoring that resolved
      - reco_normalize_version: the version of core.reco_normalize
      - engine_contract_version: str or None (the actual value of
        core.scoring._ENGINE_CONTRACT_VERSION; "5.75.0" in v5.6.0+;
        None when the marker is absent or set to None; added v3.6.0)
      - reco_normalize_contract_version: str or None (the actual
        value of core.scoring._RECO_NORMALIZE_CONTRACT_VERSION;
        "8.0.0" in v5.7.0+; None when absent; added v3.6.0)
      - recommendation_enum: tuple or None (the actual resolved
        RECOMMENDATION_ENUM tuple, so audit tooling can verify the
        v5.7.0 vocabulary expansion -- 6 tiers vs 8 tiers -- without
        re-importing core.scoring directly; added v3.6.0)
      - core_symbols: dict of {symbol: present_bool} for required scoring symbols
      - view_symbols: dict of {symbol: present_bool} for view derivers
        (added v3.4.0)
      - audit_hardening_symbols: dict of {symbol: present_bool} for
        v5.2.0+ verification helpers (added v3.4.1)
      - reco_symbols: dict of {symbol: present_bool} for required reco symbols
      - reco_rule_id_symbols: dict of {symbol: present_bool} for
        v7.2.0+ rule_id introspection (added v3.4.2)
      - cascade_bridge_symbols: dict of {symbol: present_bool} for
        v5.2.7+ canonical recommendation source-of-truth (added v3.4.3)
      - bucket_alignment_symbols: dict of {symbol: present_bool} for
        v5.3.0+ canonical bucket surface (added v3.5.0)
      - contract_versioned_symbols: dict of {symbol: present_bool}
        for v5.6.0+/v5.7.0+ contract-version markers (added v3.6.0)
      - reco_8tier_symbols: dict of {symbol: present_bool} for
        core.reco_normalize v8.0.0+ vocabulary expansion (added v3.6.0)
      - risk_thresholds: tuple or None (the actual values resolved at
        import time; surfaced for ops diagnostic; added v3.5.0)
      - confidence_thresholds: tuple or None (same; added v3.5.0)
      - buckets_canonical_enabled: bool or None (mirrors
        BUCKETS_CANONICAL; True when core.scoring is routing through
        core.buckets; added v3.5.0)
      - v525_enrichment_fields: tuple of row field names produced by
        compute_scores in v5.2.5+ (added v3.4.2)
      - v528_cascade_bridge_fields: tuple of row field names produced
        by compute_scores in v5.2.7+ / v5.2.8 (added v3.4.3)
      - v540_detail_fields: tuple of row field names produced by
        compute_scores in v5.3.0+ / v5.4.0+ (added v3.5.0)
      - v570_fields: tuple of row field names produced by
        compute_scores in v5.6.0+ (added v3.6.0)
      - import_log: list of successful/failed import attempts
      - missing_critical: list of missing CRITICAL symbols
      - missing_view: list of missing view derivers
      - missing_audit_hardening: list of missing v5.2.0+ helpers
      - missing_reco: list of missing reco symbols
      - missing_reco_rule_id: list of missing v7.2.0+ helpers
      - missing_cascade_bridge: list of missing v5.2.7+ helpers
      - missing_bucket_alignment: list of missing v5.3.0+ helpers
        (added v3.5.0)
      - missing_contract_versioned: list of missing v5.6.0+/v5.7.0+
        markers (added v3.6.0)
      - missing_reco_8tier: list of missing reco_normalize v8.0.0+
        vocabulary symbols (added v3.6.0)
      - view_aware_recommendation_enabled: bool (v3.4.0+)
      - audit_hardening_enabled: bool (v3.4.1+)
      - reco_rule_id_aware_enabled: bool (v3.4.2+)
      - cascade_bridge_enabled: bool (v3.4.3+)
      - bucket_alignment_enabled: bool (v3.5.0+)
      - contract_versioned_enabled: bool (v3.6.0+)
      - reco_8tier_aware_enabled: bool (v3.6.0+; content check on
        RECOMMENDATION_ENUM plus symbol check on reco_normalize side)

    Returns:
        Dict[str, Any]: The degradation report.
    """
    core_present = {sym: hasattr(_core_scoring, sym) for sym in _REQUIRED_CORE_SYMBOLS}
    view_present = {sym: hasattr(_core_scoring, sym) for sym in _VIEW_SYMBOLS}
    audit_present = {sym: hasattr(_core_scoring, sym) for sym in _AUDIT_HARDENING_SYMBOLS}
    cascade_present = {
        sym: hasattr(_core_scoring, sym) for sym in _CASCADE_BRIDGE_SYMBOLS
    }
    # v3.5.0 Phase G: track bucket-alignment surface.
    bucket_align_present = {
        sym: hasattr(_core_scoring, sym) for sym in _BUCKET_ALIGNMENT_SYMBOLS
    }
    # v3.6.0 Phase G: track contract-version-marker surface (core.scoring side).
    contract_version_present = {
        sym: hasattr(_core_scoring, sym) for sym in _CONTRACT_VERSION_SYMBOLS
    }

    if _reco_normalize is not None:
        reco_present = {sym: hasattr(_reco_normalize, sym) for sym in _REQUIRED_RECO_SYMBOLS}
        reco_rule_id_present = {
            sym: hasattr(_reco_normalize, sym) for sym in _RECO_RULE_ID_SYMBOLS
        }
        # v3.6.0 Phase G: track 8-tier vocabulary-expansion surface (reco_normalize side).
        reco_8tier_present = {
            sym: hasattr(_reco_normalize, sym) for sym in _RECO_8TIER_SYMBOLS
        }
        reco_version = getattr(_reco_normalize, "VERSION", "unknown")
    else:
        reco_present = {sym: False for sym in _REQUIRED_RECO_SYMBOLS}
        reco_rule_id_present = {sym: False for sym in _RECO_RULE_ID_SYMBOLS}
        reco_8tier_present = {sym: False for sym in _RECO_8TIER_SYMBOLS}
        reco_version = "missing"

    missing_critical = [sym for sym, ok in core_present.items() if not ok]
    missing_view = [sym for sym, ok in view_present.items() if not ok]
    missing_audit_hardening = [sym for sym, ok in audit_present.items() if not ok]
    missing_reco = [sym for sym, ok in reco_present.items() if not ok]
    missing_reco_rule_id = [sym for sym, ok in reco_rule_id_present.items() if not ok]
    missing_cascade_bridge = [sym for sym, ok in cascade_present.items() if not ok]
    # v3.5.0 Phase G: missing bucket-alignment list.
    missing_bucket_alignment = [
        sym for sym, ok in bucket_align_present.items() if not ok
    ]
    # v3.6.0 Phase G: missing contract-version-marker list.
    missing_contract_versioned = [
        sym for sym, ok in contract_version_present.items() if not ok
    ]
    # v3.6.0 Phase G: missing 8-tier-vocabulary list.
    missing_reco_8tier = [
        sym for sym, ok in reco_8tier_present.items() if not ok
    ]

    # v3.5.0 Phase G: surface actual resolved threshold values for ops
    # diagnostic. None means "deployed core.scoring predates v5.3.0".
    risk_thresholds_resolved: Optional[Tuple[float, ...]] = None
    confidence_thresholds_resolved: Optional[Tuple[float, ...]] = None
    if RISK_BUCKET_THRESHOLDS is not None:
        try:
            risk_thresholds_resolved = tuple(RISK_BUCKET_THRESHOLDS)
        except Exception:
            risk_thresholds_resolved = None
    if CONFIDENCE_BUCKET_THRESHOLDS is not None:
        try:
            confidence_thresholds_resolved = tuple(CONFIDENCE_BUCKET_THRESHOLDS)
        except Exception:
            confidence_thresholds_resolved = None

    # v3.6.0 Phase G: surface the actual resolved RECOMMENDATION_ENUM
    # tuple so audit tooling can verify the v5.7.0 vocabulary expansion
    # actually landed (6 tiers vs 8 tiers) without re-importing
    # core.scoring directly. None when the deployed core.scoring
    # predates v5.3.0 (the symbol itself is absent).
    recommendation_enum_resolved: Optional[Tuple[str, ...]] = None
    if RECOMMENDATION_ENUM is not None:
        try:
            recommendation_enum_resolved = tuple(RECOMMENDATION_ENUM)
        except Exception:
            recommendation_enum_resolved = None

    # v3.6.0 Phase G: surface the actual resolved contract-marker
    # values, not just symbol presence. _ENGINE_CONTRACT_VERSION is
    # "5.75.0" in scoring.py v5.6.0+; _RECO_NORMALIZE_CONTRACT_VERSION
    # is "8.0.0" in scoring.py v5.7.0+. None means the marker is
    # absent (or set to None) on the deployed core.scoring.
    engine_contract_version_resolved: Optional[str] = (
        _ENGINE_CONTRACT_VERSION if isinstance(_ENGINE_CONTRACT_VERSION, str)
        else None
    )
    reco_normalize_contract_version_resolved: Optional[str] = (
        _RECO_NORMALIZE_CONTRACT_VERSION
        if isinstance(_RECO_NORMALIZE_CONTRACT_VERSION, str)
        else None
    )

    # v3.6.0 Phase G: derive the 8-tier-vocabulary content check. This
    # is intentionally a CONTENT check on the resolved enum, not a
    # symbol-presence check on RECOMMENDATION_ENUM. A deployment on
    # core.scoring v5.6.0 has RECOMMENDATION_ENUM defined as a tuple
    # but with only 6 members; that should NOT pass the 8-tier check.
    # A deployment on core.scoring v5.7.0+ has all 8 canonical tiers
    # in the tuple.
    enum_has_accumulate_and_avoid = bool(
        recommendation_enum_resolved is not None
        and "ACCUMULATE" in recommendation_enum_resolved
        and "AVOID" in recommendation_enum_resolved
    )
    reco_8tier_aware_enabled = (
        enum_has_accumulate_and_avoid and len(missing_reco_8tier) == 0
    )

    return {
        "bridge_version": VERSION,
        "scoring_version": SCORING_VERSION,
        "reco_normalize_version": reco_version,
        # v3.6.0 Phase G: resolved contract-marker values for cross-stack audit.
        "engine_contract_version": engine_contract_version_resolved,
        "reco_normalize_contract_version": reco_normalize_contract_version_resolved,
        # v3.6.0 Phase G: resolved enum tuple so audit can verify 6-tier vs 8-tier.
        "recommendation_enum": recommendation_enum_resolved,
        "core_symbols": core_present,
        "view_symbols": view_present,
        "audit_hardening_symbols": audit_present,
        "reco_symbols": reco_present,
        "reco_rule_id_symbols": reco_rule_id_present,
        "cascade_bridge_symbols": cascade_present,
        # v3.5.0 Phase G: bucket-alignment report.
        "bucket_alignment_symbols": bucket_align_present,
        # v3.6.0 Phase G: contract-version-marker presence map.
        "contract_versioned_symbols": contract_version_present,
        # v3.6.0 Phase G: 8-tier-vocabulary presence map.
        "reco_8tier_symbols": reco_8tier_present,
        # v3.5.0 Phase G: actual resolved threshold values.
        "risk_thresholds": risk_thresholds_resolved,
        "confidence_thresholds": confidence_thresholds_resolved,
        "buckets_canonical_enabled": (
            None if BUCKETS_CANONICAL is None else bool(BUCKETS_CANONICAL)
        ),
        "v525_enrichment_fields": tuple(_V525_ENRICHMENT_FIELDS),
        "v528_cascade_bridge_fields": tuple(_V528_CASCADE_BRIDGE_FIELDS),
        # v3.5.0 Phase D: surface the v5.3.0+/v5.4.0+ detail field contract.
        "v540_detail_fields": tuple(_V540_DETAIL_FIELDS),
        # v3.6.0 Phase G: surface the v5.6.0+/v5.7.0+ field contract.
        "v570_fields": tuple(_V570_FIELDS),
        "import_log": list(_import_attempt_log),
        "missing_critical": missing_critical,
        "missing_view": missing_view,
        "missing_audit_hardening": missing_audit_hardening,
        "missing_reco": missing_reco,
        "missing_reco_rule_id": missing_reco_rule_id,
        "missing_cascade_bridge": missing_cascade_bridge,
        # v3.5.0 Phase G: missing bucket-alignment list.
        "missing_bucket_alignment": missing_bucket_alignment,
        # v3.6.0 Phase G: missing contract-version-marker list.
        "missing_contract_versioned": missing_contract_versioned,
        # v3.6.0 Phase G: missing 8-tier-vocabulary list.
        "missing_reco_8tier": missing_reco_8tier,
        "view_aware_recommendation_enabled": (
            len(missing_view) == 0
            and "recommendation_from_views" in reco_present
            and reco_present.get("recommendation_from_views", False)
        ),
        "audit_hardening_enabled": len(missing_audit_hardening) == 0,
        "reco_rule_id_aware_enabled": len(missing_reco_rule_id) == 0,
        "cascade_bridge_enabled": len(missing_cascade_bridge) == 0,
        # v3.5.0 Phase G: bucket-alignment enabled flag.
        "bucket_alignment_enabled": len(missing_bucket_alignment) == 0,
        # v3.6.0 Phase G: contract-versioned enabled flag.
        "contract_versioned_enabled": len(missing_contract_versioned) == 0,
        # v3.6.0 Phase G: 8-tier-vocabulary enabled flag (content + symbol check).
        "reco_8tier_aware_enabled": reco_8tier_aware_enabled,
    }


def is_view_aware() -> bool:
    """
    Quick boolean check: is the deployed pipeline running v5.0.0+
    view-aware recommendations?

    Returns False if any of:
      - core.scoring lacks the four derive_*_view functions
      - core.reco_normalize lacks Recommendation.from_views
      - the bridge can't reach either module
    """
    return get_degradation_report().get("view_aware_recommendation_enabled", False)


def is_audit_hardened() -> bool:
    """
    v3.4.1: Quick boolean check: is the deployed pipeline running
    core.scoring v5.2.0+ with the audit-driven verification surface?

    Returns False if core.scoring lacks any v5.2.0+ helpers
    (currently: score_views_completeness). Mirror of is_view_aware().
    """
    return get_degradation_report().get("audit_hardening_enabled", False)


def is_reco_rule_id_aware() -> bool:
    """
    v3.4.2 Phase E: Quick boolean check: is the deployed pipeline
    running core.reco_normalize v7.2.0+ with the rule_id introspection
    surface?

    Returns False if core.reco_normalize lacks any of:
      - recommendation_from_views_with_rule_id
      - the four representative RULE_ID_* constants
      - the bridge can't reach core.reco_normalize at all

    Mirror of is_view_aware() and is_audit_hardened().
    """
    return get_degradation_report().get("reco_rule_id_aware_enabled", False)


def is_cascade_bridged() -> bool:
    """
    v3.4.3 Phase E: Quick boolean check: is the deployed pipeline
    running core.scoring v5.2.7+ with the canonical recommendation
    source-of-truth surface?

    Returns False if core.scoring lacks any of:
      - derive_canonical_recommendation (function)
      - apply_canonical_recommendation (function)
      - RECOMMENDATION_SOURCE_TAG (str constant)
      - CANONICAL_PRIORITIES (tuple)
      - PRIO_P1..PRIO_P5 (str constants)
    """
    return get_degradation_report().get("cascade_bridge_enabled", False)


def is_bucket_aligned() -> bool:
    """
    v3.5.0 Phase F: Quick boolean check: is the deployed pipeline
    running core.scoring v5.3.0+/v5.5.0+ with the canonical bucket
    surface fully present?

    Returns True when core.scoring exposes ALL of:
      - RECOMMENDATION_ENUM (closed final recommendation tuple)
      - RISK_BUCKET_THRESHOLDS / CONFIDENCE_BUCKET_THRESHOLDS
      - BUCKETS_CANONICAL flag
      - compute_opportunity_score_with_source
      - _recommendation / _risk_bucket / _confidence_bucket (private
        compat wrappers required by data_engine_v2 v5.71.0)
      - get_canonical_thresholds / get_canonical_state (v5.5.0 ops
        diagnostic helpers)

    Mirror of is_view_aware(), is_audit_hardened(),
    is_reco_rule_id_aware(), is_cascade_bridged(). A False result is
    not a fatal degradation; the bridge function is unaffected. But
    audit / diagnostic tooling that relies on the bucket-alignment
    surface (e.g. reading RISK_BUCKET_THRESHOLDS directly to verify
    35/70 alignment across the stack, or calling
    get_canonical_state() to confirm env tuning took effect) needs
    this flag to be True.

    Note: BUCKETS_CANONICAL is a separate concept -- it indicates
    whether core.scoring is routing its bucket helpers through
    core.buckets at runtime. is_bucket_aligned() concerns the public
    SYMBOL SURFACE; BUCKETS_CANONICAL concerns the RUNTIME BEHAVIOR.
    Both can be True, both can be False, or one can be True without
    the other.
    """
    return get_degradation_report().get("bucket_alignment_enabled", False)


def is_contract_versioned() -> bool:
    """
    v3.6.0 Phase F: Quick boolean check: does the deployed
    core.scoring expose both contract-version markers?

    Returns True when core.scoring exposes ALL of:
      - _ENGINE_CONTRACT_VERSION   (added in core.scoring v5.6.0;
                                    documents which data_engine_v2
                                    release scoring.py was built to
                                    align with -- value "5.75.0")
      - _RECO_NORMALIZE_CONTRACT_VERSION  (added in core.scoring
                                           v5.7.0; documents which
                                           core.reco_normalize release
                                           scoring.py was built to
                                           align with -- value "8.0.0")

    Mirror of is_view_aware(), is_audit_hardened(),
    is_reco_rule_id_aware(), is_cascade_bridged(), is_bucket_aligned().
    A False result is not a fatal degradation; the bridge function is
    unaffected. But audit / diagnostic tooling that reads the resolved
    values via the degradation report (engine_contract_version,
    reco_normalize_contract_version) needs this flag to be True to
    trust those keys carry meaningful strings rather than None.

    Note: the markers themselves carry version *strings* that name the
    counterpart contract release. The degradation report surfaces both
    via top-level engine_contract_version / reco_normalize_contract_version
    keys -- read those, not just this boolean -- when full alignment
    verification is required.
    """
    return get_degradation_report().get("contract_versioned_enabled", False)


def is_reco_8tier_aware() -> bool:
    """
    v3.6.0 Phase F: Quick boolean check: is the full stack on the
    8-tier canonical vocabulary?

    Returns True when BOTH:
      (a) core.scoring's RECOMMENDATION_ENUM contains the v5.7.0
          additions ACCUMULATE and AVOID, AND
      (b) core.reco_normalize v8.0.0+ exposes the vocabulary-
          expansion symbols (RECO_ACCUMULATE, RECO_STRONG_SELL,
          RECO_AVOID, WORST_ORDINAL, and the four new RULE_ID_*
          constants).

    Both conditions must hold for the full stack to be 8-tier-aware
    in lockstep. A False from this helper while is_bucket_aligned()
    is True indicates a partial rollout:
      - scoring on v5.7.0+ (8-tier enum) but reco_normalize stuck on
        v7.2.x (cannot represent ACCUMULATE/AVOID) -- vocabulary
        round-trip through normalize_recommendation_code() will
        silently collapse the new tiers back to BUY/STRONG_SELL
      - scoring on pre-v5.7.0 (6-tier enum) but reco_normalize on
        v8.0.0+ -- reco_normalize emits ACCUMULATE/AVOID and scoring
        cannot accept them; normalize_recommendation_code() would
        collapse via its alias table.

    Either direction of mismatch is silent corruption of the
    recommendation pipeline at the contract boundary. Operators
    upgrading the stack should verify this flag flips True after
    deploying both modules.

    Note: this is a CONTENT check on RECOMMENDATION_ENUM (does it
    literally contain "ACCUMULATE" and "AVOID"?) plus a SYMBOL check
    on the reco_normalize side. It is not satisfied by simply having
    RECOMMENDATION_ENUM exposed -- that symbol exists in core.scoring
    v5.3.0+ but with only 6 tiers in v5.3.0-v5.6.0.
    """
    return get_degradation_report().get("reco_8tier_aware_enabled", False)


# =============================================================================
# Deprecation warnings (preserved from prior versions)
# =============================================================================

_LEGACY_WARNINGS_EMITTED: set = set()


def _warn_legacy(symbol: str, replacement: str) -> None:
    """Emit a one-shot deprecation warning for legacy callers."""
    if symbol in _LEGACY_WARNINGS_EMITTED:
        return
    _LEGACY_WARNINGS_EMITTED.add(symbol)
    warnings.warn(
        f"core.scoring_engine.{symbol} is deprecated; "
        f"use {replacement} instead.",
        DeprecationWarning,
        stacklevel=2,
    )


def _legacy_scoring_engine_init(*args: Any, **kwargs: Any) -> ScoringEngine:
    """Legacy-style init wrapper. Preserved from older API."""
    _warn_legacy(
        "_legacy_scoring_engine_init",
        "core.scoring.ScoringEngine(...)",
    )
    return ScoringEngine(*args, **kwargs)


# =============================================================================
# Module Exports
# =============================================================================

__all__ = [
    # Version
    "VERSION",
    "__version__",
    "SCORING_VERSION",
    # Enums
    "Horizon",
    "Signal",
    "RSISignal",
    # Data classes
    "AssetScores",
    "ScoreWeights",
    "ScoringWeights",
    "ForecastParameters",
    "ScoringEngine",
    # Defaults
    "DEFAULT_WEIGHTS",
    "DEFAULT_FORECASTS",
    # v3.5.0 Phase B: default alias completion
    "DEFAULT_SCORING_WEIGHTS",
    "DEFAULT_FORECAST_PARAMETERS",
    # Horizon
    "detect_horizon",
    "get_weights_for_horizon",
    # Component scores
    "compute_technical_score",
    "compute_valuation_score",
    "compute_growth_score",
    "compute_momentum_score",
    "compute_quality_score",
    "compute_risk_score",
    "compute_opportunity_score",
    "compute_confidence_score",
    # v3.5.0 Phase B: opportunity score with provenance source
    "compute_opportunity_score_with_source",
    # Signals
    "rsi_signal",
    "short_term_signal",
    # Derived fields
    "derive_upside_pct",
    "derive_volume_ratio",
    "derive_day_range_position",
    # View derivation (v3.4.0)
    "derive_fundamental_view",
    "derive_technical_view",
    "derive_risk_view",
    "derive_value_view",
    # Audit-hardening verification helpers (v3.4.1)
    "score_views_completeness",
    # v3.4.2 Phase C: v5.2.5 enrichment field contract
    "_V525_ENRICHMENT_FIELDS",
    # v3.4.3 Phase C: v5.2.7/v5.2.8 cascade-bridge field contract
    "_V528_CASCADE_BRIDGE_FIELDS",
    # v3.5.0 Phase D: v5.3.0+/v5.4.0+ detail field contract
    "_V540_DETAIL_FIELDS",
    # Periods
    "invest_period_label",
    # Recommendation
    "compute_recommendation",
    "normalize_recommendation_code",
    "CANONICAL_RECOMMENDATION_CODES",
    # v3.5.0 Phase B: closed final recommendation enum
    "RECOMMENDATION_ENUM",
    # Buckets
    "risk_bucket",
    "confidence_bucket",
    # v3.5.0 Phase B: bucket threshold constants + canonical flag
    "RISK_BUCKET_THRESHOLDS",
    "CONFIDENCE_BUCKET_THRESHOLDS",
    "BUCKETS_CANONICAL",
    # v3.5.0 Phase B: v5.5.0 ops diagnostic helpers
    "get_canonical_thresholds",
    "get_canonical_state",
    # v3.5.0 Phase C: private compat wrappers (data_engine_v2 v5.71.0)
    "_recommendation",
    "_risk_bucket",
    "_confidence_bucket",
    # Main entry points
    "compute_scores",
    "score_row",
    "score_quote",
    "enrich_with_scores",
    # Ranking
    "rank_rows_by_overall",
    "assign_rank_overall",
    "score_and_rank_rows",
    # v3.4.3 Phase B: canonical recommendation source-of-truth (v5.2.7+)
    "derive_canonical_recommendation",
    "apply_canonical_recommendation",
    "RECOMMENDATION_SOURCE_TAG",
    "CANONICAL_PRIORITIES",
    "PRIO_P1",
    "PRIO_P2",
    "PRIO_P3",
    "PRIO_P4",
    "PRIO_P5",
    # Diagnostics
    "get_degradation_report",
    "is_view_aware",
    "is_audit_hardened",
    "is_reco_rule_id_aware",
    "is_cascade_bridged",
    # v3.5.0 Phase F: bucket-alignment awareness helper
    "is_bucket_aligned",
    # v3.6.0 Phase F: contract-version + 8-tier-vocabulary helpers
    "is_contract_versioned",
    "is_reco_8tier_aware",
    # v3.6.0 Phase B: contract-version-marker re-exports
    "_ENGINE_CONTRACT_VERSION",
    "_RECO_NORMALIZE_CONTRACT_VERSION",
    # v3.6.0 Phase C: v5.6.0+/v5.7.0+ field contract
    "_V570_FIELDS",
    # Exceptions
    "ScoringError",
    "InvalidHorizonError",
    "MissingDataError",
]
