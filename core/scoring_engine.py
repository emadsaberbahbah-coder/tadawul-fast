#!/usr/bin/env python3
# core/scoring_engine.py
"""
================================================================================
Scoring Engine -- v3.4.2
(THIN COMPATIBILITY BRIDGE -- DELEGATES TO core.scoring v5.2.5+
 AND core.reco_normalize v7.2.0+)
================================================================================

This module is a pure compatibility shim. All scoring logic lives in
`core.scoring`. Older code (and some external integrations) imports
`ScoringEngine`, `compute_scores`, etc. from `core.scoring_engine` --
this module routes those imports to the canonical implementations in
`core.scoring`.

================================================================================
v3.4.2 changes (vs v3.4.1)  --  CROSS-STACK SYNC FOR v5.2.4 / v5.2.5 / v7.2.0
================================================================================

v3.4.1 documented core.scoring v5.2.3 as the canonical floor. Since
then the cross-stack has advanced on two fronts:

  core.scoring v5.2.4 (May 10/11 2026 cross-stack diagnostic surfacing):
    - engine_dropped_valuation tag surfaced in scoring_errors when
      data_engine_v2 v5.60.0 Phase H/I/P clears intrinsic_value/upside_pct
      due to unit-mismatch or synthesizer-overshoot detection.
    - provider_error:<class> tag surfaced when providers (eodhd v4.7.3,
      yahoo_fundamentals v6.1.0, yahoo_chart v8.2.0, enriched_quote v4.3.0)
      report last_error_class via the engine's Phase Q preservation path.
    - week_52_position_pct shape defense: scoring's `_as_pct_position_fraction`
      now tolerates both fraction (0-1) and percent points (0-100) inputs
      so the v5.60.0 Phase O auto-normalization doesn't break older callers.
    - Warnings string canonicalization: `_warning_tags_from_row` parses
      "; "-joined strings, lists, sets, frozensets with bare-key
      extraction for "key:value" tags.

  core.scoring v5.2.5 (scoring enrichment field production):
    - compute_scores / score_row / enrich_with_scores now produce four
      additional row fields:
        * conviction_score    (0-100; view-conjunction + confidence + data
                               completeness)
        * top_factors         (str; "++"-prefixed list of contributing
                               bullish factors)
        * top_risks           (str; "--"-prefixed list of contributing
                               bearish factors)
        * position_size_hint  (str; sizing suggestion derived from
                               conviction + risk_bucket)
    - These flow through to insights_builder v7.0.0 (Phase B Top Picks
      enrichment) without further code changes.

  core.reco_normalize v7.2.0 (rule_id introspection + bug fix):
    - NEW recommendation_from_views_with_rule_id(...) returning
      (rec, reason, rule_id) where rule_id is one of 14 stable
      RULE_ID_* constants. Lets callers do branch-specific audits.
    - Env-tunable conviction floors via RECO_STRONG_BUY_CONVICTION_FLOOR
      and RECO_BUY_CONVICTION_FLOOR.
    - Fix: "AVOID" now correctly parses to SELL (was REDUCE in v7.1.0).
    - __version__ alias added.

v3.4.2 closes the bridge's awareness of all three rollouts. No new
public function signatures are introduced -- the cross-stack changes
are either behavioral (propagate automatically via passthrough) or
row field additions (visible in compute_scores output dicts). The
bridge exposes them via introspection helpers and the degradation
report.

  Phase A -- Header docstring sync: reference core.scoring v5.2.5+
             and reco_normalize v7.2.0+. Enumerate v5.2.4-v5.2.5
             hardenings and v7.2.0 additions.

  Phase B -- __version__ = VERSION alias (TFB module convention used
             by core.scoring v5.2.5, core.reco_normalize v7.2.0,
             insights_builder v7.0.0). Exported in __all__.

  Phase C -- NEW _V525_ENRICHMENT_FIELDS tuple listing the row fields
             that compute_scores / score_row / enrich_with_scores now
             produce in v5.2.5+ (conviction_score, top_factors,
             top_risks, position_size_hint). Callers can introspect
             via this constant to know what fields to expect from
             enriched rows. Added to __all__.

  Phase D -- NEW _RECO_RULE_ID_SYMBOLS tuple tracking the v7.2.0
             reco_normalize surface: recommendation_from_views_with_rule_id
             plus representative RULE_ID_* constants. Optional
             category (not required for bridge function); degradation
             report tracks separately so a deployment running
             reco_normalize v7.0.0-v7.1.x still gets a working bridge
             with a degraded-mode report rather than an ImportError.

  Phase E -- NEW is_reco_rule_id_aware() helper, mirror of
             is_view_aware() and is_audit_hardened(). Quick boolean
             for "are we on reco_normalize v7.2.0+?".

  Phase F -- Extended get_degradation_report():
               * reco_rule_id_symbols: {symbol: present_bool}
               * missing_reco_rule_id: [symbols]
               * reco_rule_id_aware_enabled: bool
             Existing report fields preserved verbatim.

  Phase G -- Version bump 3.4.1 -> 3.4.2.

[PRESERVED -- strictly]
  - All v3.4.1 features: score_views_completeness re-export,
    _AUDIT_HARDENING_SYMBOLS category, is_audit_hardened().
  - All v3.4.0 features: view-derivation symbol re-exports
    (derive_fundamental_view, derive_technical_view, derive_risk_view,
    derive_value_view) and view-awareness reporting.
  - All v3.3.x and earlier public API surface.
  - No removals from __all__. New optional helpers and tracking
    categories only.

API surface (additions in v3.4.2):
  - __version__ (alias of VERSION)
  - _V525_ENRICHMENT_FIELDS (private constant, in __all__ for
    introspection)
  - _RECO_RULE_ID_SYMBOLS (private constant)
  - is_reco_rule_id_aware (bridge-level helper)

================================================================================
v3.4.1 changes (preserved verbatim)
================================================================================

A. NEW import + re-export of score_views_completeness from
   core.scoring. Added to module-level namespace and __all__.

B. NEW _AUDIT_HARDENING_SYMBOLS tuple groups the v5.2.0+ additions
   (currently just score_views_completeness; future verification
   helpers will be added here). The degradation report now reports
   audit_hardening_symbols presence separately so callers can
   distinguish between "running pre-v5.0.0 (no view-aware logic)"
   and "running v5.0.0-v5.1.x (view-aware but missing audit
   hardenings)".

C. NEW is_audit_hardened() helper. Quick boolean for "are we on
   v5.2.0+ scoring?". Mirror of the existing is_view_aware().

D. _REQUIRED_CORE_SYMBOLS unchanged (those are the v5.0.0-era
   contract -- the bridge would be unusable without them). Audit-
   hardening symbols are tracked via the new optional category
   instead of being promoted to "required", so a deployment running
   pre-v5.2.0 scoring still gets a working bridge with a degraded-
   mode report rather than an ImportError.

E. Header docstring corrected to reference core.scoring v5.2.3+ and
   to enumerate the audit-driven hardenings the bridge passes
   through. [v3.4.2 brings this current to v5.2.5+ / v7.2.0+.]

================================================================================
v3.4.0 changes (preserved)
================================================================================
- Recognize the four view-derivation symbols added in core.scoring
  v5.0.0 (derive_fundamental_view, derive_technical_view,
  derive_risk_view, derive_value_view) and surface them via
  __all__ + the degradation report.

================================================================================
v3.3.0 and earlier
================================================================================
Bridge contract for the v4.x scoring family. Preserved unchanged.

API surface (preserved exports):
- VERSION, __version__ [v3.4.2]
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
- score_views_completeness [v3.4.1]
- ScoringError, InvalidHorizonError, MissingDataError
- get_degradation_report, is_view_aware, is_audit_hardened,
  is_reco_rule_id_aware [v3.4.2]
- _V525_ENRICHMENT_FIELDS [v3.4.2]
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

VERSION = "3.4.2"
# v3.4.2 Phase B: __version__ alias matches TFB module convention used
# by core.scoring v5.2.5, core.reco_normalize v7.2.0, and
# insights_builder v7.0.0.
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

# Defaults
DEFAULT_WEIGHTS = _core_scoring.DEFAULT_WEIGHTS
DEFAULT_FORECASTS = _core_scoring.DEFAULT_FORECASTS

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

# Buckets
risk_bucket = _core_scoring.risk_bucket
confidence_bucket = _core_scoring.confidence_bucket

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
# v3.4.2 Phase C — v5.2.5 enrichment row-field contract
# =============================================================================
#
# core.scoring v5.2.5 added four row fields produced by compute_scores
# / score_row / enrich_with_scores. They are dict entries on the
# returned row, not standalone functions, so they don't need
# re-export. But callers benefit from knowing what to expect.
# This constant lets downstream code introspect the field contract
# without re-deriving the names from scratch.
#
# Consumers downstream:
#   - insights_builder v7.0.0 Phase B (Top Picks enrichment)
#   - top10_selector (when it advances)
#   - any future advisor / portfolio-allocation layer

_V525_ENRICHMENT_FIELDS: Tuple[str, ...] = (
    "conviction_score",     # float, 0-100
    "top_factors",          # str, "++"-prefixed list of contributing factors
    "top_risks",            # str, "--"-prefixed list of contributing risks
    "position_size_hint",   # str, sizing suggestion (e.g. "Standard 5%")
)


# =============================================================================
# Degradation Report
# =============================================================================
#
# Returns a structured snapshot of which scoring symbols resolved and
# which fell back. Useful for diagnosing whether the deployed
# scoring_engine is properly wired up to the v5.0.0+ view-aware logic
# AND the v5.2.0+ audit-hardening surface, vs an older scoring module.

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
# Missing any of these means "running pre-v5.0.0 -- no view-aware
# recommendations". Bridge still works but capabilities are reduced.
_VIEW_SYMBOLS: Tuple[str, ...] = (
    "derive_fundamental_view",
    "derive_technical_view",
    "derive_risk_view",
    "derive_value_view",
)

# v3.4.1: Audit-hardening symbols added in scoring v5.2.0+.
# Missing any of these means "running v5.0.0-v5.1.x -- view-aware but
# without the audit-driven verification surface". Bridge still works
# but operators lose some diagnostic capabilities.
_AUDIT_HARDENING_SYMBOLS: Tuple[str, ...] = (
    "score_views_completeness",
)

# v3.4.2 Phase D: reco_normalize v7.2.0+ rule_id introspection symbols.
# Missing means "running reco_normalize v7.0.0-v7.1.x -- 5-tier
# vocabulary works but rule_id introspection isn't available".
# Optional category: bridge function not affected. We track a
# representative subset of the RULE_ID_* constants rather than all
# 14 (any one of them being present means all 14 are present since
# they're defined together in reco_normalize v7.2.0).
_RECO_RULE_ID_SYMBOLS: Tuple[str, ...] = (
    "recommendation_from_views_with_rule_id",
    "RULE_ID_INSUFFICIENT_DATA_NO_SIGNALS",
    "RULE_ID_STRONG_BUY_ALL",
    "RULE_ID_HOLD_BULLISH_EXPENSIVE_VETO",
    "RULE_ID_HOLD_MIXED",
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
      - v525_enrichment_fields: tuple of row field names produced by
        compute_scores in v5.2.5+ (added v3.4.2)
      - import_log: list of successful/failed import attempts
      - missing_critical: list of missing CRITICAL symbols (degraded mode)
      - missing_view: list of missing view derivers (running pre-v5.0.0
        scoring module without view-aware logic)
      - missing_audit_hardening: list of missing v5.2.0+ helpers
        (running v5.0.0-v5.1.x scoring without audit-driven
        verification surface)
      - missing_reco_rule_id: list of missing v7.2.0+ helpers (added v3.4.2)
      - view_aware_recommendation_enabled: bool (v3.4.0+)
      - audit_hardening_enabled: bool (v3.4.1+)
      - reco_rule_id_aware_enabled: bool (v3.4.2+)

    Returns:
        Dict[str, Any]: The degradation report.
    """
    core_present = {sym: hasattr(_core_scoring, sym) for sym in _REQUIRED_CORE_SYMBOLS}
    view_present = {sym: hasattr(_core_scoring, sym) for sym in _VIEW_SYMBOLS}
    audit_present = {sym: hasattr(_core_scoring, sym) for sym in _AUDIT_HARDENING_SYMBOLS}

    if _reco_normalize is not None:
        reco_present = {sym: hasattr(_reco_normalize, sym) for sym in _REQUIRED_RECO_SYMBOLS}
        # v3.4.2 Phase D: track rule_id surface separately so a v7.0.0-v7.1.x
        # deployment isn't reported as "missing critical".
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

    return {
        "bridge_version": VERSION,
        "scoring_version": SCORING_VERSION,
        "reco_normalize_version": reco_version,
        "core_symbols": core_present,
        "view_symbols": view_present,
        "audit_hardening_symbols": audit_present,
        "reco_symbols": reco_present,
        "reco_rule_id_symbols": reco_rule_id_present,
        # v3.4.2 Phase C: surface the v5.2.5 enrichment field contract so
        # downstream consumers (insights_builder, top10_selector, etc.)
        # can introspect what fields compute_scores produces.
        "v525_enrichment_fields": tuple(_V525_ENRICHMENT_FIELDS),
        "import_log": list(_import_attempt_log),
        "missing_critical": missing_critical,
        "missing_view": missing_view,
        "missing_audit_hardening": missing_audit_hardening,
        "missing_reco": missing_reco,
        "missing_reco_rule_id": missing_reco_rule_id,
        "view_aware_recommendation_enabled": (
            len(missing_view) == 0
            and "recommendation_from_views" in reco_present
            and reco_present.get("recommendation_from_views", False)
        ),
        "audit_hardening_enabled": len(missing_audit_hardening) == 0,
        "reco_rule_id_aware_enabled": len(missing_reco_rule_id) == 0,
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

    Note: this checks for the v5.2.0+ public API surface. The v5.2.3
    behavioural hardenings (no-fabricated-confidence, recommendation
    coherence guard, risk penalty rebalance, revenue-collapse
    haircut, forecast unit-mismatch guard, illiquid skip) plus the
    v5.2.4 cross-stack tags (engine_dropped_valuation, provider_error)
    plus the v5.2.5 enrichment field production (conviction_score,
    top_factors, top_risks, position_size_hint) are behaviour-only
    changes and cannot be detected by symbol presence alone. Use
    SCORING_VERSION for that.
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

    Mirror of is_view_aware() and is_audit_hardened(). Useful for
    audit pipelines that want to surface rule_id information when
    available without crashing on older reco_normalize deployments.

    Note: the v7.2.0 conviction floor env-vars
    (RECO_STRONG_BUY_CONVICTION_FLOOR / RECO_BUY_CONVICTION_FLOOR)
    are behaviour-only -- check RECO_NORMALIZE version directly via
    the degradation report's `reco_normalize_version` field.
    """
    return get_degradation_report().get("reco_rule_id_aware_enabled", False)


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
    # v3.4.2 Phase B: __version__ alias
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
    # Periods
    "invest_period_label",
    # Recommendation
    "compute_recommendation",
    "normalize_recommendation_code",
    "CANONICAL_RECOMMENDATION_CODES",
    # Buckets
    "risk_bucket",
    "confidence_bucket",
    # Main entry points
    "compute_scores",
    "score_row",
    "score_quote",
    "enrich_with_scores",
    # Ranking
    "rank_rows_by_overall",
    "assign_rank_overall",
    "score_and_rank_rows",
    # Diagnostics
    "get_degradation_report",
    "is_view_aware",
    "is_audit_hardened",
    # v3.4.2 Phase E: reco_normalize v7.2.0 awareness
    "is_reco_rule_id_aware",
    # Exceptions
    "ScoringError",
    "InvalidHorizonError",
    "MissingDataError",
]
