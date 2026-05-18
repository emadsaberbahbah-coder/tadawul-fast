#!/usr/bin/env python3
# core/scoring_engine.py
"""
================================================================================
Scoring Engine -- v3.5.0
(THIN COMPATIBILITY BRIDGE -- DELEGATES TO core.scoring v5.4.2+
 AND core.reco_normalize v7.2.0+)
================================================================================

This module is a pure compatibility shim. All scoring logic lives in
`core.scoring`. Older code (and some external integrations) imports
`ScoringEngine`, `compute_scores`, etc. from `core.scoring_engine` --
this module routes those imports to the canonical implementations in
`core.scoring`.

================================================================================
v3.5.0 changes (vs v3.4.4)  --  v5.4.2 / v5.5.0 ALIGNMENT
================================================================================

core.scoring has advanced through v5.3.0 -> v5.4.0 -> v5.4.1 -> v5.4.2
-> v5.5.0 since v3.4.4 was cut. The bridge was operating correctly
against all of those versions thanks to the getattr() fallback
pattern, but was under-advertising the new surface (downstream
modules importing via the bridge could not reach symbols added since
v5.2.8). v3.5.0 closes that gap.

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

VERSION = "3.5.0"
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
      - import_log: list of successful/failed import attempts
      - missing_critical: list of missing CRITICAL symbols
      - missing_view: list of missing view derivers
      - missing_audit_hardening: list of missing v5.2.0+ helpers
      - missing_reco: list of missing reco symbols
      - missing_reco_rule_id: list of missing v7.2.0+ helpers
      - missing_cascade_bridge: list of missing v5.2.7+ helpers
      - missing_bucket_alignment: list of missing v5.3.0+ helpers
        (added v3.5.0)
      - view_aware_recommendation_enabled: bool (v3.4.0+)
      - audit_hardening_enabled: bool (v3.4.1+)
      - reco_rule_id_aware_enabled: bool (v3.4.2+)
      - cascade_bridge_enabled: bool (v3.4.3+)
      - bucket_alignment_enabled: bool (v3.5.0+)

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

    if _reco_normalize is not None:
        reco_present = {sym: hasattr(_reco_normalize, sym) for sym in _REQUIRED_RECO_SYMBOLS}
        reco_rule_id_present = {
            sym: hasattr(_reco_normalize, sym) for sym in _RECO_RULE_ID_SYMBOLS
        }
        reco_version = getattr(_reco_normalize, "VERSION", "unknown")
    else:
        reco_present = {sym: False for sym in _REQUIRED_RECO_SYMBOLS}
        reco_rule_id_present = {sym: False for sym in _RECO_RULE_ID_SYMBOLS}
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

    return {
        "bridge_version": VERSION,
        "scoring_version": SCORING_VERSION,
        "reco_normalize_version": reco_version,
        "core_symbols": core_present,
        "view_symbols": view_present,
        "audit_hardening_symbols": audit_present,
        "reco_symbols": reco_present,
        "reco_rule_id_symbols": reco_rule_id_present,
        "cascade_bridge_symbols": cascade_present,
        # v3.5.0 Phase G: bucket-alignment report.
        "bucket_alignment_symbols": bucket_align_present,
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
        "import_log": list(_import_attempt_log),
        "missing_critical": missing_critical,
        "missing_view": missing_view,
        "missing_audit_hardening": missing_audit_hardening,
        "missing_reco": missing_reco,
        "missing_reco_rule_id": missing_reco_rule_id,
        "missing_cascade_bridge": missing_cascade_bridge,
        # v3.5.0 Phase G: missing bucket-alignment list.
        "missing_bucket_alignment": missing_bucket_alignment,
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
    # Exceptions
    "ScoringError",
    "InvalidHorizonError",
    "MissingDataError",
]
