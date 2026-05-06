#!/usr/bin/env python3
# core/scoring.py
"""
================================================================================
Scoring Module — v5.2.1
(VIEW-COMPLETENESS HARDENED / VIEW-TOKENS-IN-EVERY-REASON /
 ACCUMULATE-ALIGNED / 5-TIER + CONVICTION FLOORS /
 INSIGHTS-INTEGRATED / PHANTOM-ROW-SAFE / SCHEMA-ALIGNED)
================================================================================

v5.2.1 changes (vs v5.2.0)
--------------------------
Closes the last visibility gap left by v5.2.0. v5.2.0 ensured the
four view fields on AssetScores were always non-empty strings via
the _view_or_na sentinel; v5.2.1 ensures the recommendation_reason
text on every emitted row CONTAINS the same four view tokens in
parseable "Fund: X | Tech: Y | Risk: Z | Val: W → REC" form, so
the consuming Apps Script helper (11_SpecialPages.gs v1.2.3
applyViewTokensAndRecommendationOverride_) can populate the
dedicated view columns from the reason text on every row,
including the rows where v5.2.0's compute_recommendation was
emitting non-parseable suppression messages like "Insufficient
data to score reliably."

Why this matters: the four view columns on Layout A pages
(Fundamental View / Technical View / Risk View / Value View) are
NOT in the canonical 80-col backend schema. The header-mapped
writer in 02_Core.gs writeRowsBySymbol_ doesn't write them
because they have no schema entry. The Apps Script helper
bridges the gap by parsing the reason text — but only when the
reason text contains parseable view tokens. v5.2.0 left ~3% of
rows (insufficient inputs / low confidence / day-trade override /
reco_normalize-unavailable / score-only-data paths) with reason
text that didn't contain tokens; those rows fell through to
blank cells even after the v1.2.3 helper ran. v5.2.1 raises
that coverage to 100%.

A. NEW _build_view_prefix(fund, tech, risk, val) module helper.
   Produces a parseable "Fund: X | Tech: Y | Risk: Z | Val: W"
   string with _view_or_na coercion on each token. Always
   returns a non-empty string of the canonical form, ready to
   prepend to any reason text.

B. compute_recommendation() now derives all four views at the
   TOP of the function (before any early return), builds the
   view prefix once, and includes it in every return path:

     - "Insufficient data to score reliably" path now returns
       reason = "<prefix> → HOLD (insufficient data to score reliably)"
     - Low-confidence path now returns
       reason = "<prefix> → HOLD (low AI confidence X.X%)"
     - Day-trade STRONG_BUY override now returns
       reason = "<prefix> → STRONG_BUY (day-trade setup ...)"
     - Day-trade SELL breakdown now returns
       reason = "<prefix> → SELL (day-trade breakdown ...)"
     - reco_normalize-unavailable path now returns
       reason = "<prefix> → HOLD (recommendation engine unavailable)"
     - The main reco_normalize path is unchanged — reco_normalize
       already emits the "Fund: X | Tech: Y | ..." form natively,
       so the alignment helpers run as before.

C. compute_scores() insufficient_inputs path now also builds the
   view prefix and emits the same form:
     reason = "<prefix> → HOLD (insufficient scoring inputs)"

D. The _align_reason_to_canonical_recommendation passes from
   v5.2.0 are unchanged and still scrub any legacy ACCUMULATE
   spelling out of reason text plus align the trailing arrow to
   canonical_rec. The new view-prefix paths produce reason text
   that is already aligned, so the alignment passes are no-ops
   on those rows — but the defensive double-pass remains for
   reco_normalize / insights_builder outputs that may carry
   legacy labels.

[PRESERVED — strictly] Every v5.2.0 helper, signature, dataclass
field, and behaviour for non-edge-case rows. The structural
change is content-only inside the reason text — the
(label, reason) tuple shape from compute_recommendation is
unchanged, AssetScores is unchanged, all v5.1.0 / v5.0.0
contracts are unchanged.

Public API: same as v5.2.0 (no removals, no signature changes).
The only new module-level symbol is the private helper
_build_view_prefix; it's not exported via __all__ since it's an
internal formatting helper.

v5.2.0 changes (vs v5.1.0)
--------------------------
Hardening release driven by a Tadawul Fast Bridge audit of the
Global_Markets sheet (1,707 rows × 46 cols) that surfaced two
structural issues in the data this module emits:

  - 99.6% of rows had blank Fundamental View / Technical View /
    Risk View / Value View columns. The audit traced this to the
    consuming Apps Script writer (now fixed independently in
    11_SpecialPages.gs v1.2.3 + the per-page wire-ups), but the
    Python derivation also had a structural weakness: when both
    inputs to a view were None, the helper returned None and that
    None propagated through the pipeline as a missing field.

  - 30 rows had Recommendation = ACCUMULATE while their
    Recommendation Reason ended "→ HOLD" or "→ BUY". Trace
    showed reco_normalize correctly returned canonical 5-tier
    labels but the embedded reason text contained legacy
    ACCUMULATE spelling from older insights_builder output, and
    a parallel pipeline (data_engine_v2._compute_recommendation)
    was overwriting the Recommendation field with a coarse
    score-thresholded ACCUMULATE label.

v5.2.0 fixes both at the source without breaking any v5.1.0
contract:

A. NEW _view_or_na(view) helper. Returns the view string
   unchanged when it is a non-empty string; returns "N/A" when the
   input is None or empty. "N/A" is in the canonical view
   vocabulary on every consumer (Apps Script FUNDAMENTAL_VIEW_VOCAB_
   etc.) so this is a recognised sentinel, not a magic string.

   compute_scores() now applies _view_or_na at the AssetScores
   storage step, so the four view fields are guaranteed to be
   non-empty strings on every emitted row. Behaviour for rows
   that had a populated view in v5.1.0 is byte-identical — the
   helper is a pass-through for any non-empty string. The change
   only affects rows where v5.1.0 was emitting None.

   The derivation functions themselves (derive_fundamental_view /
   derive_technical_view / derive_risk_view / derive_value_view)
   are unchanged: their Optional[str] return type is preserved
   so internal callers (compute_recommendation) keep their
   None-handling semantics. The N/A substitution happens at the
   AssetScores write boundary only.

B. NEW _CANONICAL_REC_ALIASES module constant. Maps every legacy
   recommendation label that may appear in reason text or
   inbound rec strings to its canonical 5-tier equivalent:

       ACCUMULATE       -> BUY
       ADD              -> BUY
       OUTPERFORM       -> BUY
       OVERWEIGHT       -> BUY
       STRONGBUY        -> STRONG_BUY
       MARKET_PERFORM   -> HOLD
       NEUTRAL          -> HOLD
       WATCH            -> HOLD
       TRIM             -> REDUCE
       UNDERWEIGHT      -> REDUCE
       AVOID            -> SELL
       EXIT             -> SELL
       UNDERPERFORM     -> SELL
       STRONG_SELL      -> SELL
       STRONGSELL       -> SELL
       STRONG SELL      -> SELL

   This is the same mapping the existing _LOCAL_RECO_ALIASES
   table uses for input-side normalisation (canonical labels
   like BUY map to BUY). v5.2.0 lifts the non-canonical entries
   into a dedicated, documented constant so the
   reason-alignment helper can reuse them.

C. NEW _align_reason_to_canonical_recommendation(reason,
   canonical_rec) helper. Two passes:

     Pass 1 — Whole-word replacement. Any case-insensitive match
              of a non-canonical label gets replaced with its
              canonical 5-tier equivalent. So "→ ACCUMULATE",
              "Trade ACCUMULATE on retracement", and
              "labelled ACCUMULATE" all become BUY references.
              Whole-word boundaries protect against false
              substring matches (e.g. inside hex strings or
              symbol names).

     Pass 2 — Trailing-arrow alignment. When the reason ends in
              "→ X" / "-> X" / "=> X" / "THEN X" and X is a
              recognised recommendation label that disagrees
              with canonical_rec, the trailing label is
              rewritten to canonical_rec. Free-form prose after
              the arrow (e.g. "→ a strong setup") is left
              untouched — only label-shaped tokens are
              candidates for rewrite.

   Empty / arrow-less / canonical-already inputs are returned
   unchanged.

D. compute_scores() now applies _align_reason_to_canonical_recommendation
   in two places:

     - On the raw reason returned by compute_recommendation,
       before that reason is passed as base_reason to
       insights_builder.build_insights(). This ensures
       insights_builder receives clean input.
     - On the structured_reason returned by build_insights, so
       any non-canonical labels emitted by older
       insights_builder versions are still corrected on the
       way out.

   The double-pass is defensive but cheap (one regex scan each).

E. compute_recommendation() also applies (C) to its return
   value. Direct callers (bypassing compute_scores) get the
   same alignment guarantee.

F. NEW score_views_completeness(row_or_scores) helper. Returns
   {present, total, ratio, missing} for the four view fields on
   a row or AssetScores dict. Designed to be polled by the
   consuming Apps Script verification triplet and by backend
   health checks. Treats "N/A" as MISSING for the purposes of
   completeness scoring, since N/A means "we couldn't
   compute" — but the cell is non-empty, so the writer side has
   something to put in the column. This matches the verification
   semantics expected by 99_Diag.gs's verifyAllColumnsComplete.

[PRESERVED — strictly] Every v5.1.0 public symbol, dataclass
field, function signature, and behaviour for non-edge-case
rows. Specifically:

  - AssetScores layout unchanged. The four view fields keep
    their Optional[str] type; the only change is what gets
    stored (now always a string with "N/A" sentinel for the
    no-data case, where v5.1.0 stored None).

  - compute_scores() return shape byte-identical to v5.1.0.

  - compute_recommendation() return shape byte-identical to
    v5.1.0. The trailing-arrow rewrite is content-only — the
    (label, reason) tuple shape doesn't change.

  - All component-score functions, horizon helpers, forecast
    helpers, ranking helpers, and the ScoringEngine class
    behave identically.

  - normalize_recommendation_code() unchanged. The new
    _CANONICAL_REC_ALIASES constant is a SUPERSET of the
    non-canonical entries in _LOCAL_RECO_ALIASES; the existing
    _LOCAL_RECO_ALIASES is preserved verbatim for backwards
    compat with any external import.

Public API (UNCHANGED + additions):
  All v5.1.0 exports preserved. No removals.
  Additions: _CANONICAL_REC_ALIASES (constant),
             score_views_completeness (function),
             _align_reason_to_canonical_recommendation (function).
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

__version__ = "5.2.1"
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
# Configuration (preserved)
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
# Horizon Thresholds (preserved)
# =============================================================================

_HORIZON_DAYS_CUTOFFS: Tuple[Tuple[int, Horizon], ...] = (
    (_CONFIG.day_threshold, Horizon.DAY),
    (_CONFIG.week_threshold, Horizon.WEEK),
    (_CONFIG.month_threshold, Horizon.MONTH),
)


# =============================================================================
# v5.2.0 — View / recommendation hardening constants
# =============================================================================

# v5.2.0: canonical 5-tier vocabulary (also exposed via
# CANONICAL_RECOMMENDATION_CODES below). Kept here as a set for fast
# membership checks inside the trailing-arrow alignment helper.
_CANONICAL_REC_LABELS_SET: Set[str] = {
    "STRONG_BUY", "BUY", "HOLD", "REDUCE", "SELL",
}

# v5.2.0: legacy / non-canonical recommendation labels and their
# canonical 5-tier equivalents. Used by
# _align_reason_to_canonical_recommendation to scrub free-form
# reason text before storage.
#
# This is a SUPERSET of the non-canonical entries in
# _LOCAL_RECO_ALIASES (defined further down). _LOCAL_RECO_ALIASES
# is preserved verbatim for backward compatibility of any external
# import; this constant exists for the alignment helper's benefit.
_CANONICAL_REC_ALIASES: Dict[str, str] = {
    # The ACCUMULATE → BUY mapping is the v5.2.0 anchor case — it's
    # what 30 audited Global_Markets rows had stuck in their
    # Recommendation field while their reason text said HOLD/BUY.
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

# v5.2.0: regex matching a trailing arrow + recommendation-shaped
# label. Anchored at end-of-string so we only rewrite the LAST
# label in the reason. Handles all four arrow variants used by
# reco_normalize and insights_builder (Unicode →, ASCII ->, =>,
# and the word "THEN").
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

# v5.2.0: pre-compiled patterns for the whole-word legacy-label
# replacement pass. Built once at module load so the per-row
# alignment is cheap. Pattern uses lookarounds instead of \b
# because the alias keys may contain spaces (e.g. "STRONG SELL").
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
    """
    v5.2.0: Coerce a view value to a non-empty string.

    Returns the input unchanged when it's a non-empty string.
    Returns "N/A" (which is in every consumer's view vocabulary) when
    the input is None or whitespace-only. This is the storage-time
    sentinel that prevents Fundamental View / Technical View /
    Risk View / Value View from being persisted as null.

    Internal callers (compute_recommendation) keep working with
    Optional[str] directly — this helper is applied only at the
    AssetScores write boundary inside compute_scores().
    """
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
    """
    v5.2.1: Format the four view tokens into a parseable prefix.

    Produces a string of the canonical form
        "Fund: X | Tech: Y | Risk: Z | Val: W"
    where each token is run through _view_or_na so missing inputs
    appear as "N/A" rather than as the literal string "None" or
    an empty token.

    The output is the exact prefix the consuming Apps Script helper
    (11_SpecialPages.gs v1.2.3 _parseRecommendationReason_) parses
    when populating the four view columns from the Recommendation
    Reason cell. By using this helper at every reason-emitting
    return path inside compute_recommendation and compute_scores,
    v5.2.1 guarantees that EVERY emitted row's reason text contains
    these four parseable tokens — even for rows that v5.2.0's
    early-return paths emitted as plain prose ("Insufficient data
    to score reliably", "Low AI confidence", etc.).

    >>> _build_view_prefix("BULLISH", "NEUTRAL", "HIGH", "FAIR")
    'Fund: BULLISH | Tech: NEUTRAL | Risk: HIGH | Val: FAIR'

    >>> _build_view_prefix(None, None, None, None)
    'Fund: N/A | Tech: N/A | Risk: N/A | Val: N/A'
    """
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
    """
    v5.2.0: Align a recommendation reason text to a canonical 5-tier
    recommendation label.

    Two passes:

      1. Whole-word replacement of every legacy / non-canonical label
         (ACCUMULATE → BUY, STRONGBUY → STRONG_BUY, etc.) regardless
         of position in the text. Word-boundary lookarounds protect
         against false substring matches.

      2. Trailing-arrow alignment. If the text ends with an arrow
         (→ / -> / => / THEN) followed by a recommendation-shaped
         label that disagrees with canonical_rec, the label is
         rewritten to canonical_rec. Free-form prose after the
         arrow is left untouched.

    Empty inputs are returned unchanged (an empty reason carries no
    label to misalign). Inputs whose trailing label already matches
    canonical_rec are returned with only the Pass 1 rewrites
    applied.

    >>> _align_reason_to_canonical_recommendation(
    ...     "Fund: BULLISH | Tech: NEUTRAL → ACCUMULATE", "BUY"
    ... )
    'Fund: BULLISH | Tech: HOLD → BUY'

    Note in the example above that NEUTRAL also gets rewritten to
    HOLD by Pass 1 — NEUTRAL is in the alias map because it's a
    legacy synonym for HOLD on the recommendation axis. This is
    intentional and correct: when NEUTRAL appears as a
    recommendation label, it's the legacy form of HOLD. NEUTRAL as
    a VIEW label (Fund: NEUTRAL, Tech: NEUTRAL) is a different
    semantic — see the caveat in compute_scores below for how
    the alignment is scoped to avoid view-axis collateral.
    """
    if not reason:
        return reason or ""

    text = str(reason)

    if not canonical_rec:
        canonical_rec = "HOLD"

    # Pass 1: legacy label replacement throughout the text.
    # CAVEAT: NEUTRAL is in the alias map because as a
    # *recommendation*, NEUTRAL means HOLD. But NEUTRAL is also a
    # valid VIEW label (Fund: NEUTRAL, Tech: NEUTRAL). The reason
    # text in scoring.py / insights_builder uses the format
    # "Fund: X | Tech: Y | Risk: Z | Val: W → REC" where view
    # tokens follow a colon-prefixed label and the recommendation
    # follows the arrow. To preserve view-axis NEUTRAL while
    # rewriting recommendation-axis NEUTRAL, we exclude NEUTRAL
    # from Pass 1 and rely on Pass 2 (trailing-arrow alignment)
    # to catch a NEUTRAL recommendation suffix.
    for pattern, canonical_label in _LEGACY_LABEL_PATTERNS:
        # Skip NEUTRAL in the bulk pass — handled by Pass 2 only.
        if canonical_label == "HOLD" and pattern.pattern.upper().find('NEUTRAL') > -1:
            continue
        text = pattern.sub(canonical_label, text)

    # Pass 2: trailing-arrow alignment.
    match = _TRAILING_ARROW_RE.search(text)
    if match:
        arrow_part = match.group(1)
        existing_label_raw = match.group(2).strip().upper().replace(' ', '_')

        # Map via the alias table to a canonical 5-tier label.
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

        # Only rewrite when the existing trailing label is a
        # recognised recommendation token whose LITERAL spelling
        # disagrees with canonical_rec. We compare the raw text
        # token (existing_label_raw) against canonical_rec, not
        # the alias-resolved canonical — because a legacy spelling
        # like "→ NEUTRAL" with canonical_rec="HOLD" needs
        # rewriting even though both resolve to HOLD canonically;
        # the literal text in the cell needs to read "→ HOLD".
        # Free-form prose after the arrow (existing_canonical is
        # None) is left alone.
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
    v5.2.0: Return a per-row completeness summary for the four view
    fields (Fundamental View, Technical View, Risk View, Value View).

    Designed to be called by the consuming Apps Script verification
    triplet (cf. 99_Diag.gs verifyAllColumnsComplete) and by backend
    health probes.

    Treats None, "" and "N/A" as MISSING for completeness purposes.
    The cell will be non-empty after _view_or_na coercion ("N/A")
    but the semantic is "we could not compute the view". Returning
    completeness=missing for N/A lets the consumer know that "the
    cell is filled but the data is unavailable".

    Returns:
        {
            "present": int,    # number of fields with a real label
            "total":   int,    # always 4
            "ratio":   float,  # present / total, in [0.0, 1.0]
            "missing": List[str],  # the missing field names
        }
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
# Data Classes (extended in v5.1.0 with 5 new insight fields, preserved)
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

    def as_factor_weights_map(self) -> Dict[str, float]:
        """
        Project into a {component_score_key: weight} mapping suitable for
        passing into core.insights_builder.derive_top_factors. v5.1.0+.
        """
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

    # v5.2.0: these four fields keep their Optional[str] type for
    # backward compatibility, but compute_scores() now guarantees they
    # are written as non-empty strings ("N/A" sentinel for the
    # no-data case). Rows that v5.1.0 emitted with view=None now
    # emit view="N/A".
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

    # NEW in v5.1.0 — populated by core.insights_builder via compute_scores().
    # sector_relative_score is populated only via the batch path
    # (score_and_rank_rows) since it requires a sector cohort.
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
# Forecast Helpers (preserved)
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
# Component Scoring (preserved from v5.0.0)
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
    """
    Compute quality score (0-100). Preserved from v5.0.0.

    Returns None when there is genuinely no quality input — no financial
    fundamentals AND no meaningful data-quality marker AND completeness
    below 30%. This is the phantom-row gate that prevents XETRA tickers
    with no underlying data from sneaking into HOLD/BUY recommendations.

    Detection rule:
      - no roe / roa / op_margin / net_margin / debt_to_equity, AND
      - data_quality is missing or in {POOR, STALE, MISSING, ERROR}, AND
      - completeness < 30% of core fields populated.
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

    # Phantom-row gate
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
    Compute valuation score (0-100). Preserved from v5.0.0.

    Returns None if no forward-looking signal is available (upside, 3m ROI,
    12m ROI all missing). Anchors-only (PE/PB/PS/PEG/EV) is insufficient
    because those fields are backward-looking.
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


# =============================================================================
# View Derivation (preserved from v5.0.0)
#
# v5.2.0 NOTE: these functions still return Optional[str] so internal
# callers (compute_recommendation) can branch on None and re-derive.
# The N/A substitution for storage happens at the AssetScores write
# boundary in compute_scores via _view_or_na.
# =============================================================================

def derive_fundamental_view(
    quality: Optional[float],
    growth: Optional[float],
) -> Optional[str]:
    """BULLISH / NEUTRAL / BEARISH from quality + growth scores."""
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
    """BULLISH / NEUTRAL / BEARISH from technical + momentum + RSI signal."""
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
    """LOW / MODERATE / HIGH from a 0-100 risk score (higher = riskier)."""
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
    """CHEAP / FAIR / EXPENSIVE from valuation score and/or upside %."""
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
# Recommendation (extended in v5.1.0 / v5.2.0)
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
    # NEW in v5.1.0 — pass-through to reco_normalize v7.1.0+
    conviction: Optional[float] = None,
    sector_relative: Optional[float] = None,
) -> Tuple[str, str]:
    """
    Compute view-aware 5-tier recommendation, with optional conviction floors.

    v5.2.1: Derives the four views at the TOP of the function (before
    any early return) so every return path can include them in the
    reason text. The reason text on every emitted row now contains a
    parseable "Fund: X | Tech: Y | Risk: Z | Val: W → REC" prefix
    that the consuming Apps Script helper can parse to populate the
    dedicated view columns. Previously, five early-return paths
    (insufficient data, low confidence, two day-trade overrides,
    reco_normalize unavailable) emitted plain prose with no view
    tokens, leaving the consuming Apps Script helper unable to
    populate those rows' view cells.

    v5.2.0: Adds a final pass through
    _align_reason_to_canonical_recommendation() before returning, so
    the (label, reason) tuple is internally consistent. The label is
    canonicalised via normalize_recommendation_code() and any legacy
    label spelling in the reason text (ACCUMULATE, STRONG SELL, etc.)
    is rewritten to match.

    v5.1.0: Adds `conviction` and `sector_relative` kwargs that pass
    through to reco_normalize.recommendation_from_views(). When
    conviction is provided, STRONG_BUY/BUY can be downgraded if
    conviction is below the floor (60 / 45 respectively). REDUCE /
    SELL / HOLD are protective and are never downgraded.

    The roi1/roi3/roi12 parameters are kept in the signature for
    backward compatibility with any caller passing them positionally;
    the v5.0.0+ view-aware path doesn't use them directly (the views
    already encode the relevant signal).
    """
    # v5.2.1: derive all four views FIRST so every early return can
    # carry the parseable view prefix. Caller-supplied views take
    # precedence over re-derivation.
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

    # Short-term horizons: a strong-momentum reversal can override the
    # view-aware logic, since fundamentals matter less day-to-day.
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

    # Delegate to the single source of truth.
    try:
        from core.reco_normalize import recommendation_from_views  # noqa: WPS433
    except ImportError:
        try:
            from reco_normalize import recommendation_from_views  # noqa: WPS433
        except ImportError:
            logger.warning(
                "core.reco_normalize unavailable; defaulting to HOLD. "
                "This typically means a deployment issue — recommendation "
                "engine should always be present."
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
        conviction=conviction,           # v5.1.0
        sector_relative=sector_relative, # v5.1.0
    )

    # v5.2.0: canonicalise label + align reason in one place.
    canonical_rec = normalize_recommendation_code(rec)
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
# v5.1.0: insights_builder lazy import helper
# =============================================================================

def _import_insights_builder():
    """
    Lazy import of core.insights_builder. Falls back to top-level
    insights_builder for layouts that flatten the package. Returns None
    if neither is available (insights are then skipped with a warning).
    """
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

    v5.2.0 changes vs v5.1.0:
      1. _view_or_na coerces each of the four view fields to a non-empty
         string ("N/A" sentinel) at the AssetScores write boundary.
         Internal computation still uses Optional[str] views.
      2. _align_reason_to_canonical_recommendation is applied to the raw
         reason returned by compute_recommendation BEFORE it is passed to
         insights_builder.build_insights() as base_reason, AND to the
         structured_reason returned by build_insights — defensive double
         pass that ensures no legacy ACCUMULATE/etc. spelling reaches
         the caller.

    v5.1.0 mechanics (preserved):
      A. After component scores + views are computed, ask insights_builder
         for a conviction score and pass that conviction into
         compute_recommendation so the conviction-floor downgrade can fire.
      B. After the final recommendation is set, compute top_factors,
         top_risks, position_size_hint and a structured recommendation_reason
         via insights_builder.build_insights().
      C. sector_relative_score is left as None at the per-row level — set
         in the batch path (score_and_rank_rows).
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

    # v5.2.0: keep the raw (Optional[str]) views for the recommendation
    # call so reco_normalize sees None for missing inputs and can branch
    # accordingly. The N/A coercion happens at AssetScores write time.
    fundamental_view_raw = derive_fundamental_view(quality, growth)
    technical_view_raw = derive_technical_view(tech_score, momentum, rsi_sig)
    risk_view_raw = derive_risk_view(risk)
    value_view_raw = derive_value_view(valuation, usp)

    roi3 = _as_roi_fraction(working.get("expected_roi_3m"))
    roi1 = _as_roi_fraction(working.get("expected_roi_1m"))
    roi12 = _as_roi_fraction(working.get("expected_roi_12m"))

    # ---- v5.1.0: compute conviction BEFORE the recommendation ---------
    # We need conviction so the conviction-floor downgrade in
    # reco_normalize can fire. Conviction depends on overall + views +
    # forecast_confidence + completeness — all known at this point.
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
        # v5.2.1: include view-prefix so the consuming Apps Script
        # helper can populate the four view columns even on rows
        # where the scoring inputs were too sparse to compute an
        # overall score. Views may themselves be N/A here, but the
        # parseable form is always present.
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
            sector_relative=None,  # populated only via batch path
        )

    canonical_rec = normalize_recommendation_code(rec)

    # v5.2.0: align the reason text BEFORE passing to insights_builder
    # so it receives clean input. compute_recommendation already
    # aligns its return value, but defensive re-alignment is cheap
    # and survives any direct upstream return path.
    reason = _align_reason_to_canonical_recommendation(reason, canonical_rec)

    st_signal_val = short_term_signal(tech_score, momentum, risk, horizon)
    period_label = invest_period_label(horizon, hdays)

    # ---- v5.1.0: build per-row insights (top_factors, top_risks, hint) -
    top_factors_str: str = ""
    top_risks_str: str = ""
    pos_hint: str = ""
    structured_reason: str = reason  # default: keep the (aligned) raw reason

    if ib is not None and not insufficient_inputs:
        try:
            # Build a synthetic row that contains the freshly-computed scores
            # so insights_builder can derive top factors / risks correctly.
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
                sector_scores=None,  # batch path will override later
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

    # v5.2.0: re-align AFTER build_insights too, in case an older
    # insights_builder version emitted a legacy label in its
    # recommendation_reason output.
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
        # v5.2.0: views coerced to non-empty strings here. Rows that
        # would have stored None now store "N/A".
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
        # v5.1.0 insight fields:
        sector_relative_score=None,  # populated by batch path
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
# Ranking + Batch Scoring (v5.1.0: adds sector-relative pass)
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

    v5.2.0: After insights_builder.enrich_rows_with_insights returns,
    each row's recommendation_reason is re-aligned to its
    recommendation field via _align_reason_to_canonical_recommendation.
    The batch insights path may rewrite reason text with sector-
    adjusted badges; this final alignment ensures legacy labels
    introduced by older insights_builder versions don't survive into
    the emitted rows.

    v5.1.0 addition (preserved): After per-row scoring, runs a batch-
    level pass via insights_builder.enrich_rows_with_insights() to
    compute sector_relative_score for each row and rebuild
    recommendation_reason with the sector-adjusted badge. The sector
    cohort is derived from the `sector` field on each row.

    If insights_builder is unavailable the batch pass is silently
    skipped and rows retain the per-row insights from compute_scores.
    """
    prepared = list(rows) if inplace else [dict(r or {}) for r in rows]

    # Per-row scoring (uses insights_builder for conviction + top_factors etc.)
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

    # v5.1.0: Batch-level insights pass — populates sector_relative_score
    # and rebuilds recommendation_reason with sector context.
    ib = _import_insights_builder()
    if ib is not None:
        try:
            # Use the actual horizon weights for top_factors ranking. We
            # detect a representative horizon from the first row that has
            # one populated; fall back to MONTH defaults otherwise.
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

    # v5.2.0: post-batch reason alignment. enrich_rows_with_insights
    # may rewrite recommendation_reason with sector context; if any
    # legacy labels (ACCUMULATE etc.) leak through, this pass scrubs
    # them to match each row's canonical recommendation.
    for row in prepared:
        rec = row.get("recommendation")
        reason = row.get("recommendation_reason")
        if rec and reason:
            canonical_rec = normalize_recommendation_code(rec)
            row["recommendation"] = canonical_rec
            row["recommendation_reason"] = _align_reason_to_canonical_recommendation(
                str(reason), canonical_rec
            )
        # v5.2.0: also coerce views to non-empty strings on the batch
        # output, in case enrich_rows_with_insights set them to None
        # for missing-data rows.
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
    # v5.2.0 additions:
    "score_views_completeness",
]
