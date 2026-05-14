#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
core/reco_normalize.py
================================================================================
Recommendation Normalization -- v8.0.0
================================================================================

v8.0.0 changes (vs v7.2.1) -- 8-TIER CANONICAL VOCABULARY
---------------------------------------------------------
MAJOR: the canonical recommendation enum is expanded from 5 tiers to 8 to
become the single source of truth for the whole TFB stack. `data_engine_v2.py`'s
`_classify_recommendation_8tier` already emits all eight tiers; previously
`reco_normalize` could only represent five of them, so routing the engine's
output through `normalize_recommendation()` silently collapsed `ACCUMULATE`
into `BUY` and `STRONG_SELL` / `AVOID` into `SELL`. v8.0.0 removes that lossy
collapse.

Canonical tiers (best -> worst), with their 0-7 ordinal:

    STRONG_BUY   ordinal 0
    BUY          ordinal 1
    ACCUMULATE   ordinal 2      <- NEW
    HOLD         ordinal 3
    REDUCE       ordinal 4
    SELL         ordinal 5
    STRONG_SELL  ordinal 6      <- NEW
    AVOID        ordinal 7      <- NEW

A.  ENUM EXPANSION.
    `Recommendation` gains `ACCUMULATE`, `STRONG_SELL`, `AVOID`. `_RECO_ENUM`,
    `RECO_*` module constants, and `__all__` are extended to match.

B.  RE-BANDED `from_score()` / `recommendation_from_score()`.
    The numeric->tier mapping is re-spaced for 8 tiers on every supported
    scale. The 0-100 banding (the scale the engine and vendors use most):
        >= 85  STRONG_BUY
        70-84  BUY
        60-69  ACCUMULATE
        45-59  HOLD
        35-44  REDUCE
        25-34  SELL
        15-24  STRONG_SELL
        < 15   AVOID
    The 1-5 and 1-3 scales are likewise subdivided; where a coarse scale
    cannot distinguish 8 tiers (1-3 in particular) it maps to the nearest
    representative tier rather than inventing false precision.

C.  RE-BANDED `to_score()`.
    Returns the 0-7 ordinal above (was 0-4). Callers that only compared
    ordinals for ranking keep working — the order is preserved and still
    monotonic. Callers that hard-coded "4 == worst" must move to "7 == worst";
    `WORST_ORDINAL` is exported so they can reference it symbolically.

D.  8-TIER VIEW RESOLVER.
    `_classify_views_with_rule_id()` can now emit all eight tiers:
      - STRONG_SELL: double-bearish (fund AND tech bearish) escalates to
        STRONG_SELL when ALSO high risk or score < 25 -- previously these
        cases were flattened to SELL.
      - AVOID: reserved for the genuinely uninvestable -- score < 15, or
        double-bearish on an EXPENSIVE valuation. AVOID is "do not hold
        and do not enter"; SELL is "exit an existing position".
      - ACCUMULATE: bullish fundamentals + cheap/fair value + non-bullish
        (neutral) technicals with a moderate score band [55,65). This is
        the "scale in, the thesis is good but the entry isn't urgent" call
        that previously had to round to either BUY or HOLD.
    Four new RULE_ID_* constants cover the new branches. All v7.x rule-ids
    are preserved verbatim.

E.  RE-BANDED `_parse_numeric_rating()`.
    The vendor-rating numeric parser now resolves into the 8-tier space on
    the 0-100 and 0-1 paths. The 1-5 / 1-3 star paths stay coarse (a 5-star
    scale genuinely cannot express 8 tiers) but use the wider vocabulary
    where the integer cleanly implies it.

F.  STRING PARSING -- 8-tier aware.
    New `_ACCUMULATE_LIKE` set so free text "ACCUMULATE" / "SCALE IN" /
    "START A POSITION" resolves to ACCUMULATE instead of BUY. `_AVOID_LIKE`
    so "AVOID" / "DO NOT BUY" / "UNINVESTABLE" resolves to AVOID instead of
    SELL. `_STRONG_SELL_LIKE` (already existed) now maps to the STRONG_SELL
    tier instead of collapsing to SELL. Multilingual sets (AR/FR/ES/DE/PT)
    gain accumulate/avoid/strong-sell entries. Resolution order is
    strongest-signal-first so "STRONG SELL" is never shadowed by "SELL".

G.  CONVICTION FLOORS extended.
    The conviction-floor cascade is unchanged for STRONG_BUY/BUY but now also
    covers ACCUMULATE: ACCUMULATE with conviction below `_BUY_CONVICTION_FLOOR`
    downgrades to HOLD (same floor as BUY -- a low-conviction "scale in" is
    just a HOLD). Protective tiers (REDUCE/SELL/STRONG_SELL/AVOID/HOLD) are
    still never downgraded.

H.  PHASE E NEGATION FIX preserved + extended.
    v7.2.1's removal of "AVOID"/"AGAINST" from `_PAT_NEGATE` is preserved.
    `_apply_negation` is updated for the wider vocabulary: negating a
    STRONG_BUY yields BUY (not HOLD); negating ACCUMULATE yields HOLD.

[PRESERVED -- strictly] All v7.2.1 / v7.2.0 / v7.1.0 / v7.0.0 / v6.1.0
behavior that is not explicitly re-banded above: multilingual pattern
dictionaries (EN/AR/FR/ES/DE/PT), vendor field mappings, the
NormalizedRecommendation dataclass, exceptions, deep-get extraction,
Arabic normalization, horizon helpers, conviction-floor env tunables,
rule-id introspection, and every public symbol from v7.2.1. The public
API is augmented (3 enum members + 4 rule-ids + new *_LIKE sets +
`WORST_ORDINAL`) -- nothing is removed from `__all__`.

MIGRATION NOTE for downstream callers
-------------------------------------
- `to_score()` range changed 0-4 -> 0-7. Ranking code that sorts on the
  ordinal is unaffected (order preserved). Code that hard-coded the literal
  4 as "worst" must use `WORST_ORDINAL` (== 7) instead.
- `normalize_recommendation()` may now return ACCUMULATE / STRONG_SELL /
  AVOID. Any consumer with a hard-coded 5-value allow-list must widen it.
  `is_valid_recommendation()` already reflects the full 8-tier set.

================================================================================
PRESERVED CHANGE HISTORY (v7.2.1 and earlier)
================================================================================

v7.2.1 changes (vs v7.2.0)
--------------------------
METADATA-ONLY PATCH -- May 2026 cross-stack family alignment.
- `investment_advisor` v5.3.1 uses rule-id-aware cascade via
  `recommendation_from_views_with_rule_id()`.
- `investment_advisor_engine` v4.4.1 delegates to `Recommendation.from_views()`.
- `criteria_model` v3.1.1 mirrors the env-tunable conviction floor variables.
All v7.2.0 behavior preserved verbatim.

v7.2.0 changes (vs v7.1.0)
--------------------------
A.  ENV-TUNABLE CONVICTION FLOORS via `RECO_STRONG_BUY_CONVICTION_FLOOR` /
    `RECO_BUY_CONVICTION_FLOOR` (defaults 60.0 / 45.0).
B.  RULE-ID INTROSPECTION: `recommendation_from_views_with_rule_id(...)`
    returning `(canonical_str, reason, rule_id)`.
C.  `__version__` alias alongside `VERSION`.
D.  Conviction-badge de-duplication in downgrade prose.
E.  REAL BUG FIX: "AVOID"/"AGAINST" removed from `_PAT_NEGATE` (they are
    recommendation words, not negators -- the defect downgraded a SELL to
    REDUCE).

v7.1.0 changes
--------------
- NEW optional kwargs on `Recommendation.from_views()`: `conviction`,
  `sector_relative`. Conviction floors: STRONG_BUY needs >= 60 else -> BUY;
  BUY needs >= 45 else -> HOLD. Protective calls never downgraded.
- Defensive `_to_float_strict()` guard; score-badge de-duplication.

v7.0.0 features
---------------
- View-aware 5-tier resolver with EXPENSIVE-veto / double-bearish-SELL /
  STRONG_BUY-all-conditions.
- All language pattern dictionaries and vendor field mappings.
- normalize_recommendation*, batch_normalize, NormalizedRecommendation.
================================================================================
"""

from __future__ import annotations

import os
import re
from dataclasses import dataclass, field as dc_field
from enum import Enum
from functools import lru_cache
from typing import (
    Any,
    Dict,
    Iterable,
    List,
    Mapping,
    Optional,
    Sequence,
    Set,
    Tuple,
    Union,
    cast,
)

# =============================================================================
# Version
# =============================================================================

VERSION = "8.0.0"
__version__ = VERSION  # v8.0.0: 8-tier canonical vocabulary


# =============================================================================
# v7.2.0 Phase B / v8.0.0 Phase D — Rule-ID constants for introspection
# =============================================================================
#
# Stable string identifiers for each branch of the view-aware classifier.
# Returned by recommendation_from_views_with_rule_id() so callers can
# audit which rule fired without parsing the reason text.

RULE_ID_INSUFFICIENT_DATA_NO_SIGNALS = "INSUFFICIENT_DATA_NO_VIEWS_NO_SCORE"
RULE_ID_INSUFFICIENT_DATA_TOO_FEW = "INSUFFICIENT_DATA_TOO_FEW_SIGNALS"

RULE_ID_HARD_SELL_DOUBLE_BEARISH = "HARD_SELL_DOUBLE_BEARISH"
RULE_ID_HARD_SELL_LOW_SCORE = "HARD_SELL_LOW_SCORE"

RULE_ID_REDUCE_BEARISH_FUND = "REDUCE_BEARISH_FUNDAMENTAL"
RULE_ID_REDUCE_BEARISH_TECH_EXPENSIVE = "REDUCE_BEARISH_TECH_EXPENSIVE_VALUE"
RULE_ID_REDUCE_WEAK_HIGH_RISK = "REDUCE_WEAK_SCORE_HIGH_RISK"

RULE_ID_STRONG_BUY_ALL = "STRONG_BUY_ALL_CONDITIONS"
RULE_ID_BUY_BULLISH_FUND = "BUY_BULLISH_FUND_ACCEPTABLE"

RULE_ID_HOLD_BULLISH_EXPENSIVE_VETO = "HOLD_VETO_BULLISH_EXPENSIVE"
RULE_ID_HOLD_MIXED = "HOLD_MIXED_SIGNALS"

RULE_ID_DOWNGRADED_STRONG_BUY_TO_BUY = "STRONG_BUY_DOWNGRADED_BUY_LOW_CONVICTION"
RULE_ID_DOWNGRADED_BUY_TO_HOLD = "BUY_DOWNGRADED_HOLD_LOW_CONVICTION"
RULE_ID_DOWNGRADED_STRONG_BUY_TO_HOLD = "STRONG_BUY_CASCADED_HOLD_LOW_CONVICTION"

# v8.0.0 Phase D — new rule-ids for the three additional tiers
RULE_ID_ACCUMULATE_BULLISH_MODERATE = "ACCUMULATE_BULLISH_FUND_MODERATE_SCORE"
RULE_ID_DOWNGRADED_ACCUMULATE_TO_HOLD = "ACCUMULATE_DOWNGRADED_HOLD_LOW_CONVICTION"
RULE_ID_HARD_STRONG_SELL_DOUBLE_BEARISH = "STRONG_SELL_DOUBLE_BEARISH_ESCALATED"
RULE_ID_AVOID_UNINVESTABLE = "AVOID_UNINVESTABLE"


# =============================================================================
# Enums and view constants
# =============================================================================

class Recommendation(str, Enum):
    """
    Canonical recommendation values — 8-tier as of v8.0.0.

    Ordering (best -> worst): STRONG_BUY, BUY, ACCUMULATE, HOLD,
    REDUCE, SELL, STRONG_SELL, AVOID.
    """
    STRONG_BUY = "STRONG_BUY"
    BUY = "BUY"
    ACCUMULATE = "ACCUMULATE"   # v8.0.0
    HOLD = "HOLD"
    REDUCE = "REDUCE"
    SELL = "SELL"
    STRONG_SELL = "STRONG_SELL"  # v8.0.0
    AVOID = "AVOID"              # v8.0.0

    # ---- numeric score -> recommendation (re-banded for 8 tiers in v8.0.0) -

    @classmethod
    def from_score(cls, score: Any, scale: str = "1-5") -> "Recommendation":
        """
        Convert a numeric score to a recommendation.

        v8.0.0: re-banded across 8 tiers. The 0-100 scale is the primary
        path (engine + most vendors); the 1-5 and 1-3 star scales are
        genuinely too coarse for 8 distinct tiers, so they map each integer
        to its nearest representative tier rather than fabricating
        precision the source data does not contain.

        0-100 banding:
            >= 85  STRONG_BUY
            70-84  BUY
            60-69  ACCUMULATE
            45-59  HOLD
            35-44  REDUCE
            25-34  SELL
            15-24  STRONG_SELL
            < 15   AVOID
        """
        try:
            if isinstance(score, bool):
                s = float(score)
            elif isinstance(score, (int, float)):
                s = float(score)
            else:
                s = float(str(score).strip().replace(",", "."))
            if s != s:  # NaN
                return cls.HOLD
        except (TypeError, ValueError):
            return cls.HOLD

        if scale == "1-5":
            # 5-point scale: 1 best .. 5 worst. Eight tiers cannot be
            # cleanly expressed on five integers, so we use half-step
            # boundaries to reach the most defensible representative tier.
            if s <= 1.25:
                return cls.STRONG_BUY
            if s <= 1.75:
                return cls.BUY
            if s <= 2.5:
                return cls.ACCUMULATE
            if s <= 3.25:
                return cls.HOLD
            if s <= 3.75:
                return cls.REDUCE
            if s <= 4.5:
                return cls.SELL
            if s <= 4.85:
                return cls.STRONG_SELL
            return cls.AVOID

        if scale == "1-3":
            # 3-point scale is too coarse for 8 tiers; map to the three
            # representative anchors plus the extreme tails.
            if s <= 1.15:
                return cls.STRONG_BUY
            if s <= 1.6:
                return cls.BUY
            if s <= 2.4:
                return cls.HOLD
            if s <= 2.85:
                return cls.SELL
            return cls.AVOID

        if scale == "0-100":
            if s >= 85:
                return cls.STRONG_BUY
            if s >= 70:
                return cls.BUY
            if s >= 60:
                return cls.ACCUMULATE
            if s >= 45:
                return cls.HOLD
            if s >= 35:
                return cls.REDUCE
            if s >= 25:
                return cls.SELL
            if s >= 15:
                return cls.STRONG_SELL
            return cls.AVOID

        return cls.HOLD

    def to_score(self) -> int:
        """
        Convert recommendation to a 0-7 ordinal (0 = best, 7 = worst).

        v8.0.0: range widened from 0-4 to 0-7. Ordering is preserved and
        still strictly monotonic, so ranking/sort consumers are unaffected.
        Code that hard-coded the literal 4 as "worst" must move to
        `WORST_ORDINAL` (== 7).
        """
        return {
            Recommendation.STRONG_BUY: 0,
            Recommendation.BUY: 1,
            Recommendation.ACCUMULATE: 2,
            Recommendation.HOLD: 3,
            Recommendation.REDUCE: 4,
            Recommendation.SELL: 5,
            Recommendation.STRONG_SELL: 6,
            Recommendation.AVOID: 7,
        }.get(self, 3)

    @classmethod
    def is_valid(cls, value: str) -> bool:
        """Check if value is a valid recommendation."""
        return value in cls._value2member_map_

    # ---- view-aware resolver (8-tier as of v8.0.0) ------------------------

    @classmethod
    def from_views(
        cls,
        *,
        fundamental: Any = None,
        technical: Any = None,
        risk: Any = None,
        value: Any = None,
        score: Any = None,
        conviction: Any = None,
        sector_relative: Any = None,
    ) -> Tuple["Recommendation", str]:
        """
        Map four View strings + Overall Score (+optional conviction +optional
        sector-relative) into a canonical recommendation.

        Args:
            fundamental:     Fundamental View   (BULLISH / NEUTRAL / BEARISH)
            technical:       Technical View     (BULLISH / NEUTRAL / BEARISH)
            risk:            Risk View          (LOW / MODERATE / HIGH)
            value:           Value View         (CHEAP / FAIR / EXPENSIVE)
            score:           Overall Score      (0-100, optional)
            conviction:      Conviction Score   (0-100, optional)
            sector_relative: Sector-Adj Score   (0-100, optional)

        Returns:
            (Recommendation enum, reason string).

        v8.0.0: the resolver can now emit all eight tiers. STRONG_SELL and
        AVOID escalate from the v7.x SELL branches under the harshest
        conditions; ACCUMULATE fills the "good thesis, non-urgent entry"
        band that previously had to round to BUY or HOLD.

        Conviction-floor downgrades (env-tunable):
            - STRONG_BUY  with conviction < _STRONG_BUY_CONVICTION_FLOOR (60)
              -> BUY
            - BUY         with conviction < _BUY_CONVICTION_FLOOR (45)  -> HOLD
            - ACCUMULATE  with conviction < _BUY_CONVICTION_FLOOR (45)  -> HOLD
            - REDUCE / SELL / STRONG_SELL / AVOID / HOLD are never downgraded.

        Examples:
            >>> Recommendation.from_views(
            ...     fundamental="BULLISH", technical="BULLISH",
            ...     risk="LOW", value="CHEAP", score=82, conviction=78
            ... )
            (<Recommendation.STRONG_BUY: 'STRONG_BUY'>, 'Strong fundamentals + ...')

            >>> # ACCUMULATE: good thesis, neutral technicals, moderate score
            >>> Recommendation.from_views(
            ...     fundamental="BULLISH", technical="NEUTRAL",
            ...     risk="MODERATE", value="FAIR", score=58
            ... )
            (<Recommendation.ACCUMULATE: 'ACCUMULATE'>, 'Bullish fundamentals ...')

            >>> # STRONG_SELL: double-bearish escalated by high risk
            >>> Recommendation.from_views(
            ...     fundamental="BEARISH", technical="BEARISH",
            ...     risk="HIGH", value="FAIR", score=30
            ... )
            (<Recommendation.STRONG_SELL: 'STRONG_SELL'>, 'Bearish on bearish ...')

            >>> # AVOID: uninvestable — double-bearish on expensive valuation
            >>> Recommendation.from_views(
            ...     fundamental="BEARISH", technical="BEARISH",
            ...     risk="HIGH", value="EXPENSIVE", score=20
            ... )
            (<Recommendation.AVOID: 'AVOID'>, 'Bearish on bearish, expensive ...')
        """
        rec, reason, _rule_id = _classify_views_with_rule_id(
            fundamental=fundamental,
            technical=technical,
            risk=risk,
            value=value,
            score=score,
            conviction=conviction,
            sector_relative=sector_relative,
        )
        return rec, reason


# Symbolic "worst ordinal" so downstream code never hard-codes the literal.
WORST_ORDINAL = Recommendation.AVOID.to_score()  # == 7 in v8.0.0


# =============================================================================
# v7.1.0 / v7.2.0 internals: conviction-floor enforcement
# =============================================================================

def _to_float_strict(value: Any) -> Optional[float]:
    """
    Strict numeric coercion for thresholds. Returns None for None / NaN /
    non-numeric. Defensive against numpy scalars / Decimal that survive
    bare float() but compare oddly downstream.
    """
    if value is None:
        return None
    try:
        if isinstance(value, bool):
            return None
        if isinstance(value, (int, float)):
            f = float(value)
        else:
            f = float(str(value).strip().replace(",", "."))
        if f != f or f in (float("inf"), float("-inf")):
            return None
        return f
    except (TypeError, ValueError):
        return None


def _env_float(name: str, default: float) -> float:
    """
    Best-effort env-var float load with default fallback on parse failure /
    unset / NaN. Used for the conviction-floor thresholds so ops can tune
    them without a code change.
    """
    raw = os.getenv(name)
    if raw is None:
        return default
    try:
        f = float(raw)
        if f != f or f in (float("inf"), float("-inf")):
            return default
        return f
    except (TypeError, ValueError):
        return default


def _attach_metrics(
    base_reason: str,
    conviction: Optional[float],
    sector_relative: Optional[float],
) -> str:
    """
    Append a metric badge "[Conv NN, Sector-Adj NN]" to base_reason if
    those metrics are available. Skipped fields are omitted, so a row
    with neither produces no badge at all.
    """
    parts: List[str] = []
    if conviction is not None:
        parts.append(f"Conv {conviction:.0f}")
    if sector_relative is not None:
        parts.append(f"Sector-Adj {sector_relative:.0f}")
    if not parts:
        return base_reason
    return f"{base_reason} [{', '.join(parts)}]"


# Env-tunable conviction floors. Defaults match v7.1.0 verbatim. Ops can
# override via:
#     export RECO_STRONG_BUY_CONVICTION_FLOOR=65
#     export RECO_BUY_CONVICTION_FLOOR=50
# Loaded once at module import.
_STRONG_BUY_CONVICTION_FLOOR = _env_float("RECO_STRONG_BUY_CONVICTION_FLOOR", 60.0)
_BUY_CONVICTION_FLOOR = _env_float("RECO_BUY_CONVICTION_FLOOR", 45.0)


def _apply_conviction_floor(
    initial: Recommendation,
    initial_reason: str,
    conviction: Optional[float],
    sector_relative: Optional[float],
) -> Tuple[Recommendation, str]:
    """
    Apply conviction floors to a STRONG_BUY / BUY / ACCUMULATE recommendation.

    Rules (thresholds env-tunable):
      - STRONG_BUY with conviction < _STRONG_BUY_CONVICTION_FLOOR (60)
        -> BUY (and note in reason)
      - BUY with conviction < _BUY_CONVICTION_FLOOR (45)
        -> HOLD (and note in reason)
      - ACCUMULATE with conviction < _BUY_CONVICTION_FLOOR (45)
        -> HOLD (v8.0.0: a low-conviction "scale in" is just a HOLD)
      - Any rec with conviction is None  -> unchanged (just attach badge)

    Protective calls (REDUCE / SELL / STRONG_SELL / AVOID / HOLD) are never
    passed here, so they are never downgraded.

    v7.2.0 Phase D: when a downgrade fires, the reason text already cites
    the conviction value in prose, so the `[Conv NN]` badge would be
    redundant — it is suppressed in the downgrade path; the
    `[Sector-Adj NN]` part is still appended when supplied.
    """
    if conviction is None:
        return initial, _attach_metrics(initial_reason, None, sector_relative)

    # Cascade: STRONG_BUY -> BUY -> HOLD, and ACCUMULATE -> HOLD, when
    # conviction is below the relevant floor(s). Iterate until stable so a
    # STRONG_BUY with conviction 40 does not stop at BUY (whose own floor
    # is 45) — that would be dishonest.
    current = initial
    downgrades: List[str] = []

    if current == Recommendation.STRONG_BUY and conviction < _STRONG_BUY_CONVICTION_FLOOR:
        downgrades.append(
            f"STRONG_BUY -> BUY (conviction {conviction:.0f} below floor "
            f"{_STRONG_BUY_CONVICTION_FLOOR:.0f})"
        )
        current = Recommendation.BUY

    if current == Recommendation.BUY and conviction < _BUY_CONVICTION_FLOOR:
        downgrades.append(
            f"BUY -> HOLD (conviction {conviction:.0f} below floor "
            f"{_BUY_CONVICTION_FLOOR:.0f})"
        )
        current = Recommendation.HOLD

    # v8.0.0: ACCUMULATE shares the BUY floor. ACCUMULATE only ever arrives
    # here as the *initial* value (the STRONG_BUY/BUY cascade above cannot
    # produce it), so this is an independent check, not part of the cascade.
    if current == Recommendation.ACCUMULATE and conviction < _BUY_CONVICTION_FLOOR:
        downgrades.append(
            f"ACCUMULATE -> HOLD (conviction {conviction:.0f} below floor "
            f"{_BUY_CONVICTION_FLOOR:.0f})"
        )
        current = Recommendation.HOLD

    if downgrades:
        current_reason = (
            f"{initial_reason} Downgraded: " + "; ".join(downgrades) + "."
        )
        # Skip the redundant Conv badge — conviction value is already in the
        # downgrade prose. Sector badge still applies.
        return current, _attach_metrics(current_reason, None, sector_relative)

    return current, _attach_metrics(initial_reason, conviction, sector_relative)


# =============================================================================
# View token canonical values
# =============================================================================

VIEW_FUND_BULLISH = "BULLISH"
VIEW_FUND_NEUTRAL = "NEUTRAL"
VIEW_FUND_BEARISH = "BEARISH"

VIEW_TECH_BULLISH = "BULLISH"
VIEW_TECH_NEUTRAL = "NEUTRAL"
VIEW_TECH_BEARISH = "BEARISH"

VIEW_RISK_LOW = "LOW"
VIEW_RISK_MODERATE = "MODERATE"
VIEW_RISK_HIGH = "HIGH"

VIEW_VALUE_CHEAP = "CHEAP"
VIEW_VALUE_FAIR = "FAIR"
VIEW_VALUE_EXPENSIVE = "EXPENSIVE"


# Tolerant view-token normalizers
_VIEW_FUND_ALIASES: Dict[str, str] = {
    "BULLISH": VIEW_FUND_BULLISH, "BULL": VIEW_FUND_BULLISH,
    "POSITIVE": VIEW_FUND_BULLISH, "STRONG": VIEW_FUND_BULLISH,
    "GOOD": VIEW_FUND_BULLISH, "HEALTHY": VIEW_FUND_BULLISH,
    "NEUTRAL": VIEW_FUND_NEUTRAL, "MIXED": VIEW_FUND_NEUTRAL,
    "MODERATE": VIEW_FUND_NEUTRAL, "FAIR": VIEW_FUND_NEUTRAL,
    "BEARISH": VIEW_FUND_BEARISH, "BEAR": VIEW_FUND_BEARISH,
    "NEGATIVE": VIEW_FUND_BEARISH, "WEAK": VIEW_FUND_BEARISH,
    "POOR": VIEW_FUND_BEARISH, "DETERIORATING": VIEW_FUND_BEARISH,
}

_VIEW_TECH_ALIASES: Dict[str, str] = {
    "BULLISH": VIEW_TECH_BULLISH, "BULL": VIEW_TECH_BULLISH,
    "UP": VIEW_TECH_BULLISH, "POSITIVE": VIEW_TECH_BULLISH,
    "UPTREND": VIEW_TECH_BULLISH, "STRONG": VIEW_TECH_BULLISH,
    "OVERSOLD": VIEW_TECH_BULLISH,
    "NEUTRAL": VIEW_TECH_NEUTRAL, "FLAT": VIEW_TECH_NEUTRAL,
    "SIDEWAYS": VIEW_TECH_NEUTRAL, "MIXED": VIEW_TECH_NEUTRAL,
    "BEARISH": VIEW_TECH_BEARISH, "BEAR": VIEW_TECH_BEARISH,
    "DOWN": VIEW_TECH_BEARISH, "NEGATIVE": VIEW_TECH_BEARISH,
    "DOWNTREND": VIEW_TECH_BEARISH, "WEAK": VIEW_TECH_BEARISH,
    "OVERBOUGHT": VIEW_TECH_BEARISH,
}

_VIEW_RISK_ALIASES: Dict[str, str] = {
    "LOW": VIEW_RISK_LOW, "VERY LOW": VIEW_RISK_LOW, "VERY_LOW": VIEW_RISK_LOW,
    "MINIMAL": VIEW_RISK_LOW, "SAFE": VIEW_RISK_LOW,
    "MODERATE": VIEW_RISK_MODERATE, "MEDIUM": VIEW_RISK_MODERATE,
    "MED": VIEW_RISK_MODERATE, "BALANCED": VIEW_RISK_MODERATE,
    "MID": VIEW_RISK_MODERATE,
    "HIGH": VIEW_RISK_HIGH, "VERY HIGH": VIEW_RISK_HIGH,
    "VERY_HIGH": VIEW_RISK_HIGH, "ELEVATED": VIEW_RISK_HIGH,
    "EXTREME": VIEW_RISK_HIGH, "DANGEROUS": VIEW_RISK_HIGH,
}

_VIEW_VALUE_ALIASES: Dict[str, str] = {
    "CHEAP": VIEW_VALUE_CHEAP, "UNDERVALUED": VIEW_VALUE_CHEAP,
    "BARGAIN": VIEW_VALUE_CHEAP, "DISCOUNTED": VIEW_VALUE_CHEAP,
    "ATTRACTIVE": VIEW_VALUE_CHEAP, "VALUE": VIEW_VALUE_CHEAP,
    "FAIR": VIEW_VALUE_FAIR, "FAIRLY VALUED": VIEW_VALUE_FAIR,
    "FAIRLY_VALUED": VIEW_VALUE_FAIR, "REASONABLE": VIEW_VALUE_FAIR,
    "NEUTRAL": VIEW_VALUE_FAIR, "FULL": VIEW_VALUE_FAIR,
    "EXPENSIVE": VIEW_VALUE_EXPENSIVE, "OVERVALUED": VIEW_VALUE_EXPENSIVE,
    "RICH": VIEW_VALUE_EXPENSIVE, "PREMIUM": VIEW_VALUE_EXPENSIVE,
    "STRETCHED": VIEW_VALUE_EXPENSIVE,
}


def normalize_view_token(value: Any, *, kind: str = "fundamental") -> str:
    """
    Normalize a view-token string to its canonical form.

    Returns the canonical token (e.g. 'BULLISH', 'EXPENSIVE') or '' if
    the input can't be parsed. Tolerant to case, whitespace, hyphens, and
    common synonyms.

    Args:
        value: The raw view value (string, None, or other).
        kind:  One of 'fundamental', 'technical', 'risk', 'value'.
    """
    if value is None:
        return ""
    try:
        s = str(value).strip().upper()
    except Exception:
        return ""
    if not s or s in {"N/A", "NA", "NONE", "NULL", "-", "—", ""}:
        return ""

    # Strip trailing tokens like "VIEW", "RATING", "BUCKET"
    for suffix in (" VIEW", " RATING", " BUCKET", " LEVEL"):
        if s.endswith(suffix):
            s = s[: -len(suffix)].strip()

    # Normalize separators
    s = s.replace("-", " ").replace("_", " ")
    s = " ".join(s.split())

    table = {
        "fundamental": _VIEW_FUND_ALIASES,
        "technical": _VIEW_TECH_ALIASES,
        "risk": _VIEW_RISK_ALIASES,
        "value": _VIEW_VALUE_ALIASES,
    }.get(kind.lower(), _VIEW_FUND_ALIASES)

    if s in table:
        return table[s]
    no_space = s.replace(" ", "_")
    if no_space in table:
        return table[no_space]
    return ""


# =============================================================================
# v7.2.0 Phase B / v8.0.0 Phase D — view classifier with rule_id introspection
# =============================================================================

def _classify_views_with_rule_id(
    *,
    fundamental: Any = None,
    technical: Any = None,
    risk: Any = None,
    value: Any = None,
    score: Any = None,
    conviction: Any = None,
    sector_relative: Any = None,
) -> Tuple[Recommendation, str, str]:
    """
    Core view-aware classifier returning rule_id.

    v8.0.0: extended from a 5-tier to an 8-tier resolver. The priority
    cascade preserves every v7.x branch verbatim; the changes are:

      - The v7.x "double-bearish -> SELL" branch now ESCALATES to
        STRONG_SELL when risk is HIGH or score < 25, and to AVOID when the
        valuation is also EXPENSIVE (uninvestable). Plain double-bearish
        with neither aggravator still returns SELL exactly as before.
      - The v7.x "very weak score -> SELL" branch now returns AVOID when
        score < 15.
      - A new ACCUMULATE branch sits between BUY and the EXPENSIVE veto:
        bullish fundamentals + cheap/fair value + neutral technicals with
        a moderate score band [55, 65).

    Returns (rec, reason, rule_id) where rule_id is one of the RULE_ID_*
    string constants.
    """
    f = normalize_view_token(fundamental, kind="fundamental")
    t = normalize_view_token(technical, kind="technical")
    r = normalize_view_token(risk, kind="risk")
    v = normalize_view_token(value, kind="value")

    s_val = _to_float_strict(score)
    conv_val = _to_float_strict(conviction)
    sect_val = _to_float_strict(sector_relative)

    # ----- Priority 1: NO DATA --------------------------------------
    # We need at least 2 views OR a score to make any call.
    views_present = sum(1 for x in (f, t, r, v) if x)
    if views_present == 0 and s_val is None:
        return (
            Recommendation.HOLD,
            "Insufficient data — no views and no score available.",
            RULE_ID_INSUFFICIENT_DATA_NO_SIGNALS,
        )
    if views_present < 2 and s_val is None:
        return (
            Recommendation.HOLD,
            "Insufficient data — too few signals to recommend.",
            RULE_ID_INSUFFICIENT_DATA_TOO_FEW,
        )

    # ----- Priority 2: HARD SELL / STRONG_SELL / AVOID --------------
    # v8.0.0: the v7.x double-bearish SELL branch now escalates.
    if f == VIEW_FUND_BEARISH and t == VIEW_TECH_BEARISH:
        score_text = f", Score={s_val:.0f}" if s_val is not None else ""
        # Worst case: double-bearish on an expensive valuation — uninvestable.
        if v == VIEW_VALUE_EXPENSIVE:
            return (
                Recommendation.AVOID,
                f"Bearish fundamentals AND bearish technicals on an "
                f"expensive valuation — uninvestable, do not hold or "
                f"enter{score_text}.",
                RULE_ID_AVOID_UNINVESTABLE,
            )
        # Escalate to STRONG_SELL when high risk or a very weak score
        # compounds the double-bearish signal.
        if r == VIEW_RISK_HIGH or (s_val is not None and s_val < 25.0):
            aggravator = (
                "high risk" if r == VIEW_RISK_HIGH
                else f"very weak score ({s_val:.1f})"
            )
            return (
                Recommendation.STRONG_SELL,
                f"Bearish fundamentals AND bearish technicals compounded "
                f"by {aggravator} — exit with urgency{score_text}.",
                RULE_ID_HARD_STRONG_SELL_DOUBLE_BEARISH,
            )
        # Plain double-bearish — unchanged from v7.x.
        return (
            Recommendation.SELL,
            f"Bearish fundamentals AND bearish technicals — exit{score_text}.",
            RULE_ID_HARD_SELL_DOUBLE_BEARISH,
        )

    # v8.0.0: a genuinely uninvestable score floors to AVOID, not SELL.
    if s_val is not None and s_val < 15.0:
        return (
            Recommendation.AVOID,
            f"Overall score critically low ({s_val:.1f}) — uninvestable.",
            RULE_ID_AVOID_UNINVESTABLE,
        )

    if s_val is not None and s_val < 35.0:
        return (
            Recommendation.SELL,
            f"Very weak overall score ({s_val:.1f}) — exit.",
            RULE_ID_HARD_SELL_LOW_SCORE,
        )

    # ----- Priority 3: REDUCE ---------------------------------------
    if f == VIEW_FUND_BEARISH:
        return (
            Recommendation.REDUCE,
            "Bearish fundamentals — reduce exposure even with mixed "
            "technical/value signals.",
            RULE_ID_REDUCE_BEARISH_FUND,
        )

    if t == VIEW_TECH_BEARISH and v == VIEW_VALUE_EXPENSIVE:
        return (
            Recommendation.REDUCE,
            "Bearish technicals on an expensive valuation — reduce.",
            RULE_ID_REDUCE_BEARISH_TECH_EXPENSIVE,
        )

    if s_val is not None and s_val < 50.0 and r == VIEW_RISK_HIGH:
        return (
            Recommendation.REDUCE,
            f"Weak score ({s_val:.1f}) combined with high risk — reduce.",
            RULE_ID_REDUCE_WEAK_HIGH_RISK,
        )

    # ----- Priority 4: STRONG_BUY (all conditions required) ---------
    is_strong_buy = (
        f == VIEW_FUND_BULLISH
        and t == VIEW_TECH_BULLISH
        and v == VIEW_VALUE_CHEAP
        and r != VIEW_RISK_HIGH
        and s_val is not None and s_val >= 75.0
    )
    if is_strong_buy:
        base_reason = (
            f"Strong fundamentals + technicals on cheap valuation, "
            f"risk under control (Score={s_val:.1f})."
        )
        final_rec, final_reason = _apply_conviction_floor(
            Recommendation.STRONG_BUY, base_reason, conv_val, sect_val
        )
        if final_rec == Recommendation.STRONG_BUY:
            return final_rec, final_reason, RULE_ID_STRONG_BUY_ALL
        elif final_rec == Recommendation.BUY:
            return final_rec, final_reason, RULE_ID_DOWNGRADED_STRONG_BUY_TO_BUY
        elif final_rec == Recommendation.HOLD:
            return final_rec, final_reason, RULE_ID_DOWNGRADED_STRONG_BUY_TO_HOLD
        else:  # defensive — shouldn't reach here
            return final_rec, final_reason, RULE_ID_STRONG_BUY_ALL

    # ----- Priority 5: BUY ------------------------------------------
    is_buy = (
        f == VIEW_FUND_BULLISH
        and v in (VIEW_VALUE_CHEAP, VIEW_VALUE_FAIR)
        and t in (VIEW_TECH_BULLISH, VIEW_TECH_NEUTRAL)
        and s_val is not None and s_val >= 65.0
    )
    if is_buy:
        base_reason = (
            f"Bullish fundamentals with acceptable valuation and "
            f"technicals (Score={s_val:.1f})."
        )
        final_rec, final_reason = _apply_conviction_floor(
            Recommendation.BUY, base_reason, conv_val, sect_val
        )
        if final_rec == Recommendation.BUY:
            return final_rec, final_reason, RULE_ID_BUY_BULLISH_FUND
        elif final_rec == Recommendation.HOLD:
            return final_rec, final_reason, RULE_ID_DOWNGRADED_BUY_TO_HOLD
        else:  # defensive
            return final_rec, final_reason, RULE_ID_BUY_BULLISH_FUND

    # ----- Priority 5b: ACCUMULATE (v8.0.0) -------------------------
    # Good thesis, non-urgent entry: bullish fundamentals + acceptable
    # (cheap/fair) valuation + non-bearish technicals, but the score sits
    # in the moderate [55, 65) band — not strong enough for a full BUY.
    # This is the "scale in" call that v7.x had to round to BUY or HOLD.
    is_accumulate = (
        f == VIEW_FUND_BULLISH
        and v in (VIEW_VALUE_CHEAP, VIEW_VALUE_FAIR)
        and t in (VIEW_TECH_BULLISH, VIEW_TECH_NEUTRAL)
        and s_val is not None and 55.0 <= s_val < 65.0
    )
    if is_accumulate:
        base_reason = (
            f"Bullish fundamentals on acceptable valuation, but a moderate "
            f"overall score (Score={s_val:.1f}) argues for scaling in "
            f"rather than a full position."
        )
        final_rec, final_reason = _apply_conviction_floor(
            Recommendation.ACCUMULATE, base_reason, conv_val, sect_val
        )
        if final_rec == Recommendation.ACCUMULATE:
            return final_rec, final_reason, RULE_ID_ACCUMULATE_BULLISH_MODERATE
        elif final_rec == Recommendation.HOLD:
            return final_rec, final_reason, RULE_ID_DOWNGRADED_ACCUMULATE_TO_HOLD
        else:  # defensive
            return final_rec, final_reason, RULE_ID_ACCUMULATE_BULLISH_MODERATE

    # Special-case BUY veto explanation: bullish fundamentals but
    # valuation says EXPENSIVE -> HOLD with clear reason.
    if (
        f == VIEW_FUND_BULLISH
        and t in (VIEW_TECH_BULLISH, VIEW_TECH_NEUTRAL)
        and v == VIEW_VALUE_EXPENSIVE
    ):
        return (
            Recommendation.HOLD,
            _attach_metrics(
                "Bullish fundamentals but valuation expensive — wait for "
                "a better entry.",
                conv_val, sect_val,
            ),
            RULE_ID_HOLD_BULLISH_EXPENSIVE_VETO,
        )

    # ----- Priority 6: HOLD (default) -------------------------------
    score_part = f" Score={s_val:.1f}." if s_val is not None else ""
    view_summary = (
        f"Fund={f or 'N/A'}, Tech={t or 'N/A'}, "
        f"Risk={r or 'N/A'}, Value={v or 'N/A'}."
    )
    return (
        Recommendation.HOLD,
        _attach_metrics(
            f"Mixed signals — holding. {view_summary}{score_part}",
            conv_val, sect_val,
        ),
        RULE_ID_HOLD_MIXED,
    )


def recommendation_from_views(
    fundamental: Any = None,
    technical: Any = None,
    risk: Any = None,
    value: Any = None,
    score: Any = None,
    conviction: Any = None,
    sector_relative: Any = None,
) -> Tuple[str, str]:
    """
    Free-function wrapper around `Recommendation.from_views()`.

    Returns:
        (canonical_string, reason_text). canonical_string is one of the
        eight canonical tiers (STRONG_BUY / BUY / ACCUMULATE / HOLD /
        REDUCE / SELL / STRONG_SELL / AVOID).

    Internally delegates to `_classify_views_with_rule_id()` and discards
    the rule_id. For audit-style callers that need the rule_id, use
    `recommendation_from_views_with_rule_id()` instead.
    """
    rec, reason, _rule_id = _classify_views_with_rule_id(
        fundamental=fundamental,
        technical=technical,
        risk=risk,
        value=value,
        score=score,
        conviction=conviction,
        sector_relative=sector_relative,
    )
    return rec.value, reason


def recommendation_from_views_with_rule_id(
    fundamental: Any = None,
    technical: Any = None,
    risk: Any = None,
    value: Any = None,
    score: Any = None,
    conviction: Any = None,
    sector_relative: Any = None,
) -> Tuple[str, str, str]:
    """
    As `recommendation_from_views()` but additionally returns a stable
    rule_id string identifying exactly which branch of the classifier fired.

    Returns:
        (canonical_string, reason_text, rule_id)
        - canonical_string ∈ the 8 canonical tiers
        - reason_text: human-readable explanation
        - rule_id: one of the RULE_ID_* constants.

    Useful for:
      - Audit logs: track the distribution of which rule fires across the
        universe to spot rule-coverage gaps.
      - Sheet-level diagnostics: surface the rule_id alongside the
        recommendation for operator review.
      - Debugging: distinguish "rule fired AND was downgraded" from
        "different rule fired entirely".
    """
    rec, reason, rule_id = _classify_views_with_rule_id(
        fundamental=fundamental,
        technical=technical,
        risk=risk,
        value=value,
        score=score,
        conviction=conviction,
        sector_relative=sector_relative,
    )
    return rec.value, reason, rule_id


# =============================================================================
# Constants (preserved from v7.2.1, extended for the 8-tier vocabulary)
# =============================================================================

_RECO_ENUM = frozenset({r.value for r in Recommendation})

_CONFIDENCE_HIGH = 0.8
_CONFIDENCE_MEDIUM = 0.5

_NO_RATING: Set[str] = {
    "NA", "N/A", "NONE", "NULL", "UNKNOWN", "UNRATED", "NOT RATED", "NO RATING",
    "NR", "N R", "SUSPENDED", "UNDER REVIEW", "NOT COVERED", "NO COVERAGE",
    "NOT APPLICABLE", "NIL", "—", "-", "WAITING", "PENDING", "TBA", "TBD",
    "NO RECOMMENDATION", "RATING WITHDRAWN", "WITHDRAWN", "NOT RANKED",
}

_STRONG_BUY_LIKE: Set[str] = {
    "STRONG BUY", "CONVICTION BUY", "HIGH CONVICTION BUY", "BUY STRONG",
    "TOP PICK", "BEST IDEA", "HIGHLY RECOMMENDED BUY", "BUY (STRONG)",
    "STRONG BUY RECOMMENDATION", "MUST BUY", "FOCUS BUY",
}

# v8.0.0: "ACCUMULATE"-family text now resolves to the ACCUMULATE tier
# instead of collapsing into BUY. "STRONG ACCUMULATE" stays in
# _STRONG_BUY_LIKE (it is a high-conviction call, not a scale-in).
_ACCUMULATE_LIKE: Set[str] = {
    "ACCUMULATE", "ACCUMULATION", "SCALE IN", "SCALE-IN", "SCALING IN",
    "START A POSITION", "START POSITION", "BUILD A POSITION",
    "BUILD POSITION", "BEGIN ACCUMULATING", "PHASE IN", "AVERAGE IN",
    "GRADUAL BUY", "STAGED BUY", "PARTIAL BUY", "NIBBLE",
}

_STRONG_SELL_LIKE: Set[str] = {
    "STRONG SELL", "SELL (STRONG)", "SELL STRONG", "EXIT IMMEDIATELY",
    "STRONG SELL RECOMMENDATION", "MUST SELL", "URGENT SELL", "FORCED EXIT",
    "LIQUIDATE ALL", "DUMP", "FULL EXIT",
}

# v8.0.0: "AVOID"-family text now resolves to the AVOID tier instead of
# collapsing into SELL. AVOID == "do not hold and do not enter".
_AVOID_LIKE: Set[str] = {
    "AVOID", "AVOIDANCE", "DO NOT BUY", "DO NOT ENTER", "DO NOT INVEST",
    "STAY AWAY", "UNINVESTABLE", "NOT INVESTABLE", "BLACKLIST",
    "BLACKLISTED", "NEVER BUY", "STEER CLEAR", "NO-GO", "NO GO",
}

_BUY_LIKE: Set[str] = {
    "BUY", "ADD", "OUTPERFORM", "MARKET OUTPERFORM",
    "SECTOR OUTPERFORM", "OVERWEIGHT", "INCREASE", "UPGRADE", "LONG",
    "BULLISH", "POSITIVE", "BUYING", "SPECULATIVE BUY", "RECOMMENDED BUY",
    "BUY ON DIPS", "BUY RECOMMENDATION", "BUILT", "POSITION",
}

_HOLD_LIKE: Set[str] = {
    "HOLD", "NEUTRAL", "MAINTAIN", "UNCHANGED", "MARKET PERFORM", "SECTOR PERFORM",
    "IN LINE", "EQUAL WEIGHT", "EQUALWEIGHT", "FAIR VALUE", "FAIRLY VALUED",
    "WAIT", "KEEP", "STABLE", "WATCH", "MONITOR", "NO ACTION", "PERFORM",
    "HOLD RECOMMENDATION", "HOLDING", "KEEP POSITION", "NO CHANGE",
}

_REDUCE_LIKE: Set[str] = {
    "REDUCE", "TRIM", "LIGHTEN", "PARTIAL SELL", "TAKE PROFIT", "TAKE PROFITS",
    "UNDERWEIGHT", "DECREASE", "WEAKEN", "CAUTIOUS", "PROFIT TAKING",
    "SELL STRENGTH", "TRIM POSITION", "REDUCE EXPOSURE", "PARTIAL EXIT",
    "REDUCE WEIGHT", "CUT POSITION", "LESSEN", "SCALE BACK",
}

_SELL_LIKE: Set[str] = {
    "SELL", "EXIT", "UNDERPERFORM", "MARKET UNDERPERFORM",
    "SECTOR UNDERPERFORM", "SHORT", "BEARISH", "NEGATIVE", "SELLING",
    "DOWNGRADE", "RED FLAG", "LIQUIDATE", "CLOSE POSITION", "UNWIND",
    "SELL RECOMMENDATION", "EXIT POSITION",
}

# --- Arabic --------------------------------------------------------------
_AR_BUY: Set[str] = {
    "شراء", "اشتر", "اشترى", "اشترِ", "زيادة", "ايجابي", "ايجابى",
    "فرصة شراء", "اداء متفوق", "زيادة المراكز", "توصية شراء", "توصية بالشراء",
    "احتفاظ مع ميل للشراء", "زيادة التعرض", "تعزيز المراكز",
    "مضاعفة", "تفوق", "متفوق", "زيادة وزن", "فرصة استثمارية",
}

_AR_STRONG_BUY: Set[str] = {
    "شراء قوي", "شراء قوى", "شراء مكثف", "توصية شراء قوية", "شراء بقوة",
}

_AR_ACCUMULATE: Set[str] = {
    "تجميع", "تراكم", "بناء مركز", "بناء المركز", "شراء تدريجي",
    "تجميع تدريجي", "دخول تدريجي", "شراء جزئي",
}

_AR_HOLD: Set[str] = {
    "احتفاظ", "محايد", "انتظار", "مراقبة", "ثبات", "استقرار", "تمسك", "اداء محايد",
    "حياد", "موقف محايد", "ابقاء المراكز", "متوازن", "بدون تغيير", "محافظة",
    "استمرارية", "لا تغيير", "محايد تماما", "وزن مساوي",
}

_AR_REDUCE: Set[str] = {
    "تقليص", "تخفيف", "جني ارباح", "جني ارباح جزئي", "تقليل", "تخفيض",
    "اداء ضعيف", "تخفيض المراكز", "بيع جزئي", "تخفيف المراكز",
    "تقليل التعرض", "تخفيف المخاطر", "تسييل جزئي", "اخذ ارباح", "خفف",
}

_AR_SELL: Set[str] = {
    "بيع", "تخارج", "سلبي", "سلبى", "خروج",
    "توصية بيع", "توصية بالبيع", "اداء اقل", "اداء أقل", "تصفية", "الخروج من السهم",
    "بيع كلي", "تسييل", "تخلص من",
}

_AR_STRONG_SELL: Set[str] = {
    "بيع قوي", "بيع قوى", "خروج فوري", "بيع بقوة", "تصفية كاملة",
    "توصية بيع قوية", "خروج عاجل",
}

_AR_AVOID: Set[str] = {
    "تجنب", "تجنب تماما", "ابتعد", "غير قابل للاستثمار", "لا تشتري",
    "تجنب الشراء", "ابتعد عن",
}

# --- French --------------------------------------------------------------
_FR_BUY: Set[str] = {
    "ACHETER", "ACHAT", "RENFORCER", "SURPERFORMER", "SURPERFORMANCE",
    "POSITIF", "BULLISH", "RECOMMANDATION D'ACHAT", "AJOUTER",
    "SURPONDÉRER", "HAUSSIER",
}
_FR_STRONG_BUY: Set[str] = {
    "ACHAT FORT", "ACHAT SOLIDE", "ACHETER FORTEMENT", "FORTE CONVICTION",
}
_FR_ACCUMULATE: Set[str] = {
    "ACCUMULER", "ACCUMULATION", "CONSTRUIRE UNE POSITION", "ENTRÉE PROGRESSIVE",
    "ACHAT PROGRESSIF", "ACHAT ÉCHELONNÉ",
}
_FR_HOLD: Set[str] = {
    "CONSERVER", "GARDER", "NEUTRE", "MAINTENIR", "STABLE", "EN LIGNE",
    "PERFORMANCE DU MARCHÉ", "ATTENDRE", "SURVEILLER", "PAS DE CHANGEMENT",
    "CONSERVATION", "POSITION NEUTRE", "À CONSERVER",
}
_FR_REDUCE: Set[str] = {
    "RÉDUIRE", "ALLEGER", "PRENDRE DES BÉNÉFICES", "PRISE DE BÉNÉFICES",
    "SOUS-PONDÉRER", "DIMINUER", "ATTÉNUER", "RÉDUCTION", "PRUDENCE",
    "VENDRE PARTIELLEMENT", "ALLÉGEMENT",
}
_FR_SELL: Set[str] = {
    "VENDRE", "CÉDER", "SOUS-PERFORMER", "SOUS-PERFORMANCE",
    "BAISSIER", "NÉGATIF", "SORTIR", "LIQUIDER",
    "RECOMMANDATION DE VENTE", "VENTE", "DÉGRADER",
}
_FR_STRONG_SELL: Set[str] = {
    "VENDRE FORT", "VENTE FORTE", "SORTIE IMMÉDIATE", "LIQUIDER TOUT",
}
_FR_AVOID: Set[str] = {
    "ÉVITER", "À ÉVITER", "NE PAS ACHETER", "RESTER À L'ÉCART", "PROSCRIRE",
}

# --- Spanish -------------------------------------------------------------
_ES_BUY: Set[str] = {
    "COMPRAR", "SOBREPONDERAR", "SOBRERENDIMIENTO", "POSITIVO",
    "ALCISTA", "COMPRA", "RECOMENDACIÓN DE COMPRA",
    "AÑADIR", "INCREMENTAR", "SUPERAR", "OUTPERFORM",
}
_ES_STRONG_BUY: Set[str] = {
    "COMPRA FUERTE", "COMPRAR FUERTE", "ALTA CONVICCIÓN", "COMPRA DECIDIDA",
}
_ES_ACCUMULATE: Set[str] = {
    "ACUMULAR", "ACUMULACIÓN", "CONSTRUIR POSICIÓN", "ENTRADA GRADUAL",
    "COMPRA ESCALONADA", "COMPRA GRADUAL",
}
_ES_HOLD: Set[str] = {
    "MANTENER", "CONSERVAR", "NEUTRAL", "NEUTRO", "ESPERAR", "MONITOREAR",
    "RENDIMIENTO DE MERCADO", "SIN CAMBIOS", "ESTABLE", "EN LÍNEA",
    "POSICIÓN NEUTRA", "MANTENIMIENTO",
}
_ES_REDUCE: Set[str] = {
    "REDUCIR", "ALIGERAR", "TOMAR GANANCIAS", "TOMA DE GANANCIAS",
    "INFRAPONDERAR", "DISMINUIR", "PRECAUCIÓN", "VENTA PARCIAL",
    "RECORTAR", "DISMINUCIÓN",
}
_ES_SELL: Set[str] = {
    "VENDER", "BAJISTA", "NEGATIVO", "SALIR", "LIQUIDAR",
    "BAJO RENDIMIENTO", "RENDIMIENTO INFERIOR", "VENTA", "DESHACERSE",
    "RECOMENDACIÓN DE VENTA",
}
_ES_STRONG_SELL: Set[str] = {
    "VENTA FUERTE", "VENDER FUERTE", "SALIDA INMEDIATA", "LIQUIDAR TODO",
}
_ES_AVOID: Set[str] = {
    "EVITAR", "A EVITAR", "NO COMPRAR", "MANTENERSE ALEJADO", "NO INVERTIR",
}

# --- German --------------------------------------------------------------
_DE_BUY: Set[str] = {
    "KAUFEN", "KAUF", "AUFSTOCKEN", "ÜBERGEWICHTEN",
    "OUTPERFORM", "POSITIV", "BULLISH", "KAUFEMPFEHLUNG",
    "HINZUFÜGEN", "ERHÖHEN", "ÜBERTREFFEN",
}
_DE_STRONG_BUY: Set[str] = {
    "STARKER KAUF", "STARK KAUFEN", "HOHE ÜBERZEUGUNG", "KLARER KAUF",
}
_DE_ACCUMULATE: Set[str] = {
    "AKKUMULIEREN", "AKKUMULATION", "POSITION AUFBAUEN", "SCHRITTWEISER EINSTIEG",
    "GESTAFFELTER KAUF", "SCHRITTWEISE KAUFEN",
}
_DE_HOLD: Set[str] = {
    "HALTEN", "BEHALTEN", "NEUTRAL", "MARKTGEWICHT", "ABWARTEN", "BEOBACHTEN",
    "STABIL", "UNVERÄNDERT", "IN LINE", "MARKTPERFORMANCE", "HALTEEMPFEHLUNG",
}
_DE_REDUCE: Set[str] = {
    "REDUZIEREN", "VERRINGERN", "GEWINNE MITNEHMEN", "GEWINN MITNAHME",
    "UNTERGEWICHTEN", "ABBAUEN", "VORSICHT", "TEILVERKAUF", "KÜRZEN",
    "VERMINDERN", "ZURÜCKFAHREN",
}
_DE_SELL: Set[str] = {
    "VERKAUFEN", "VERKAUF", "UNTERPERFORM", "BAISSIST", "NEGATIV",
    "AUSSTEIGEN", "LIQUIDIEREN", "VERKAUFSEMPFEHLUNG",
    "ABSTOSSEN",
}
_DE_STRONG_SELL: Set[str] = {
    "STARKER VERKAUF", "STARK VERKAUFEN", "SOFORTIGER AUSSTIEG",
    "ALLES LIQUIDIEREN",
}
_DE_AVOID: Set[str] = {
    "VERMEIDEN", "ZU VERMEIDEN", "NICHT KAUFEN", "FERNBLEIBEN", "MEIDEN",
}

# --- Portuguese ----------------------------------------------------------
_PT_BUY: Set[str] = {
    "COMPRAR", "SOBREPONDERAR", "OUTPERFORM", "POSITIVO",
    "ALCISTA", "COMPRA", "RECOMENDAÇÃO DE COMPRA",
    "ADICIONAR", "AUMENTAR", "SUPERAR",
}
_PT_STRONG_BUY: Set[str] = {
    "COMPRA FORTE", "COMPRAR FORTE", "ALTA CONVICÇÃO", "COMPRA DECIDIDA",
}
_PT_ACCUMULATE: Set[str] = {
    "ACUMULAR", "ACUMULAÇÃO", "CONSTRUIR POSIÇÃO", "ENTRADA GRADUAL",
    "COMPRA ESCALONADA", "COMPRA GRADUAL",
}
_PT_HOLD: Set[str] = {
    "MANTER", "CONSERVAR", "NEUTRO", "AGUARDAR", "MONITORAR",
    "DESEMPENHO DE MERCADO", "SEM ALTERAÇÕES", "ESTÁVEL", "EM LINHA",
    "POSIÇÃO NEUTRA", "MANUTENÇÃO",
}
_PT_REDUCE: Set[str] = {
    "REDUZIR", "ALIVIAR", "OBTER LUCROS", "OBTER LUCROS PARCIAIS",
    "SUB-PONDERAR", "DIMINUIR", "PRUDÊNCIA", "VENDA PARCIAL",
    "CORTAR", "REDUÇÃO", "COLHER LUCROS",
}
_PT_SELL: Set[str] = {
    "VENDER", "BAIXISTA", "NEGATIVO", "SAIR", "LIQUIDAR",
    "DESEMPENHO INFERIOR", "SUBPERFORM", "VENDA", "LIVRAR-SE",
    "RECOMENDAÇÃO DE VENTA",
}
_PT_STRONG_SELL: Set[str] = {
    "VENDA FORTE", "VENDER FORTE", "SAÍDA IMEDIATA", "LIQUIDAR TUDO",
    "LIQUIDAÇÃO",
}
_PT_AVOID: Set[str] = {
    "EVITAR", "A EVITAR", "NÃO COMPRAR", "MANTER DISTÂNCIA", "NÃO INVESTIR",
}

# Aggregated multilingual sets (v8.0.0: now 8 tiers wide).
_LANG_STRONG_BUY = _FR_STRONG_BUY | _ES_STRONG_BUY | _DE_STRONG_BUY | _PT_STRONG_BUY
_LANG_BUY = _FR_BUY | _ES_BUY | _DE_BUY | _PT_BUY
_LANG_ACCUMULATE = _FR_ACCUMULATE | _ES_ACCUMULATE | _DE_ACCUMULATE | _PT_ACCUMULATE
_LANG_HOLD = _FR_HOLD | _ES_HOLD | _DE_HOLD | _PT_HOLD
_LANG_REDUCE = _FR_REDUCE | _ES_REDUCE | _DE_REDUCE | _PT_REDUCE
_LANG_SELL = _FR_SELL | _ES_SELL | _DE_SELL | _PT_SELL
_LANG_STRONG_SELL = _FR_STRONG_SELL | _ES_STRONG_SELL | _DE_STRONG_SELL | _PT_STRONG_SELL
_LANG_AVOID = _FR_AVOID | _ES_AVOID | _DE_AVOID | _PT_AVOID

_VENDOR_FIELDS: Dict[str, List[str]] = {
    "default": [
        "recommendation", "reco", "rating", "action", "signal",
        "analyst_rating", "analystRecommendation", "analyst_recommendation",
        "consensus", "consensus_rating",
    ],
    "nested": [
        "meta.recommendation", "meta.rating", "data.recommendation",
        "result.recommendation", "payload.recommendation",
        "recommendation.value", "rating.value", "analyst.rating",
    ],
    "bloomberg": [
        "analyst_rating", "recommendation_mean", "analyst_consensus",
        "rating_trend", "analyst_summary.recommendation",
    ],
    "yahoo": [
        "recommendationKey", "recommendationMean", "analystRating",
        "targetConsensus", "rating",
    ],
    "reuters": [
        "analystRecommendations", "streetAccount.analystRating",
        "REC", "REC_AVG", "analystRatings.recommendation",
    ],
    "morningstar": [
        "starRating", "analystRating", "ratingSummary.recommendation",
    ],
    "zacks": [
        "zacksRank", "zacksRankText", "zacks_rank", "rank",
    ],
    "tipranks": [
        "consensus", "analystConsensus", "tipranksConsensus",
    ],
}

_REASON_FIELDS: List[str] = [
    "recommendation_reason",
    "reason",
    "recommendationReason",
    "recommendation_reason_text",
    "advisor_reason",
    "explanation",
    "rationale",
    "justification",
    "why",
    "meta.recommendation_reason",
    "data.recommendation_reason",
    "result.recommendation_reason",
]

_HORIZON_DAYS_FIELDS: List[str] = [
    "horizon_days",
    "invest_period_days",
    "investment_period_days",
    "period_days",
    "days",
    "meta.horizon_days",
    "data.horizon_days",
]

_HORIZON_LABEL_FIELDS: List[str] = [
    "invest_period_label",
    "investment_period_label",
    "period_label",
    "horizon_label",
    "meta.invest_period_label",
    "data.invest_period_label",
]

_CONFIDENCE_FIELDS: List[str] = [
    "forecast_confidence",
    "confidence_score",
    "confidence",
    "meta.confidence",
    "data.confidence",
]

_SCORE_FIELDS: List[str] = [
    "overall_score",
    "risk_score",
    "valuation_score",
    "momentum_score",
    "quality_score",
    "value_score",
    "opportunity_score",
]

# v7.2.0 Phase E (BUG FIX, preserved): "AVOID" and "AGAINST" are NOT in the
# negate pattern. They are recommendation/sentiment words, not negation
# operators. "AVOID" alone is an AVOID-tier signal (v8.0.0); with AVOID in
# NEGATE, _apply_negation would downgrade its own call. Phrases like
# "DO NOT AVOID THIS" still negate correctly because NOT is the canonical
# negator.
_PAT_NEGATE = re.compile(
    r"\b(?:NOT|NO|NEVER|WITHOUT|HARDLY|RARELY|UNLIKELY)\b",
    re.IGNORECASE,
)
_PAT_NEGATION_CONTEXT = re.compile(
    r"(?:NOT|NO|NEVER)\s+(?:A\s+|AN\s+)?(STRONG\s+)?(BUY|SELL|HOLD|REDUCE|ACCUMULATE|AVOID)",
    re.IGNORECASE,
)

_PAT_STRONG_BUY = re.compile(
    r"\b(?:STRONG\s+BUY|CONVICTION\s+BUY|TOP\s+PICK|BEST\s+IDEA|MUST\s+BUY|FOCUS\s+BUY)\b",
    re.IGNORECASE,
)
_PAT_STRONG_SELL = re.compile(
    r"\b(?:STRONG\s+SELL|EXIT\s+IMMEDIATELY|MUST\s+SELL|URGENT\s+SELL|LIQUIDATE\s+ALL|FULL\s+EXIT)\b",
    re.IGNORECASE,
)
# v8.0.0: dedicated patterns for the two new tiers.
_PAT_ACCUMULATE = re.compile(
    r"\b(?:ACCUMULATE|ACCUMULATION|SCALE[\s-]?IN|SCALING\s+IN|START\s+(?:A\s+)?POSITION|"
    r"BUILD\s+(?:A\s+)?POSITION|PHASE\s+IN|AVERAGE\s+IN|GRADUAL\s+BUY|STAGED\s+BUY|PARTIAL\s+BUY)\b",
    re.IGNORECASE,
)
_PAT_AVOID = re.compile(
    r"\b(?:AVOID|AVOIDANCE|DO\s+NOT\s+BUY|DO\s+NOT\s+ENTER|DO\s+NOT\s+INVEST|"
    r"STAY\s+AWAY|UNINVESTABLE|NOT\s+INVESTABLE|BLACKLIST(?:ED)?|NEVER\s+BUY|"
    r"STEER\s+CLEAR|NO[\s-]?GO)\b",
    re.IGNORECASE,
)
# v8.0.0: detects a genuine negation OF the AVOID call itself ("not avoid",
# "no longer avoid", "never avoid"). This is deliberately distinct from the
# AVOID idioms inside _PAT_AVOID ("DO NOT BUY" / "NEVER BUY" / "DO NOT
# ENTER" ...), where the embedded NOT/NEVER is intrinsic to the AVOID
# meaning and must NOT be treated as a negation. Without this distinction,
# _apply_negation reads the "NOT BUY" inside "DO NOT BUY" as a negated BUY
# and wrongly flattens a clear AVOID idiom to HOLD.
_PAT_AVOID_NEGATED = re.compile(
    r"\b(?:NOT|NO\s+LONGER|NEVER)\s+(?:TO\s+)?AVOID(?:ANCE|ING)?\b",
    re.IGNORECASE,
)

_PAT_SELL = re.compile(
    r"\b(?:SELL|EXIT|UNDERPERFORM|MARKET\s+UNDERPERFORM|SECTOR\s+UNDERPERFORM|SHORT|"
    r"BEARISH|NEGATIVE|SELLING|DOWNGRADE|RED\s+FLAG|LIQUIDATE|CLOSE\s+POSITION|UNWIND|"
    r"SELL\s+RECOMMENDATION|EXIT\s+POSITION)\b",
    re.IGNORECASE,
)
_PAT_REDUCE = re.compile(
    r"\b(?:REDUCE|TRIM|LIGHTEN|PARTIAL\s+SELL|TAKE\s+PROFIT|TAKE\s+PROFITS|UNDERWEIGHT|"
    r"DECREASE|WEAKEN|CAUTIOUS|PROFIT\s+TAKING|SELL\s+STRENGTH|TRIM\s+POSITION|"
    r"REDUCE\s+EXPOSURE|PARTIAL\s+EXIT|REDUCE\s+WEIGHT|CUT\s+POSITION|LESSEN|SCALE\s+BACK)\b",
    re.IGNORECASE,
)
_PAT_HOLD = re.compile(
    r"\b(?:HOLD|NEUTRAL|MAINTAIN|UNCHANGED|MARKET\s+PERFORM|SECTOR\s+PERFORM|IN\s+LINE|"
    r"EQUAL\s+WEIGHT|EQUALWEIGHT|FAIR\s+VALUE|FAIRLY\s+VALUED|WAIT|KEEP|STABLE|WATCH|"
    r"MONITOR|NO\s+ACTION|PERFORM|HOLD\s+RECOMMENDATION|HOLDING|KEEP\s+POSITION|NO\s+CHANGE)\b",
    re.IGNORECASE,
)
_PAT_BUY = re.compile(
    r"\b(?:BUY|ADD|OUTPERFORM|MARKET\s+OUTPERFORM|SECTOR\s+OUTPERFORM|OVERWEIGHT|INCREASE|"
    r"UPGRADE|LONG|BULLISH|POSITIVE|BUYING|SPECULATIVE\s+BUY|RECOMMENDED\s+BUY|"
    r"BUY\s+ON\s+DIPS|BUY\s+RECOMMENDATION|BUILT|POSITION)\b",
    re.IGNORECASE,
)

_PAT_RATING_HINT = re.compile(r"\b(?:RATING|SCORE|RANK|RECOMMENDATION|CONSENSUS|STARS|STAR)\b", re.IGNORECASE)
_PAT_RATIO = re.compile(r"(\d+(?:[.,]\d+)?)\s*(?:/|OF|OUT\s+OF)\s*(\d+(?:[.,]\d+)?)", re.IGNORECASE)
_PAT_NUMBER = re.compile(r"(-?\d+(?:[.,]\d+)?)")
_PAT_CONFIDENCE = re.compile(r"\b(?:CONFIDENCE|PROB|PROBABILITY|CHANCE|LIKELIHOOD|LIKELYHOOD)\b", re.IGNORECASE)

_PAT_SCALE_5 = re.compile(r"\b(?:[1-5]\s*(?:/|\|)?\s*[1-5]|5[- ]?POINT|5[- ]?STAR)\b")
_PAT_SCALE_3 = re.compile(r"\b(?:[1-3]\s*(?:/|\|)?\s*[1-3]|3[- ]?TIER|3[- ]?LEVEL)\b")

_PAT_SEP = re.compile(r"[\t\r\n]+")
_PAT_SPLIT = re.compile(r"[\-_/|]+")
_PAT_WS = re.compile(r"\s+")
_PAT_PUNCT = re.compile(r"[^\w\s\-]")


# =============================================================================
# Data Classes
# =============================================================================

@dataclass
class NormalizedRecommendation:
    """Normalized recommendation with context. (Preserved from v6.1.0.)"""
    recommendation: str = Recommendation.HOLD.value
    recommendation_reason: Optional[str] = None
    horizon_days: Optional[int] = None
    invest_period_days: Optional[int] = None
    invest_period_label: Optional[str] = None
    confidence: Optional[float] = None
    scores: Dict[str, float] = dc_field(default_factory=dict)
    source_fields: Dict[str, Any] = dc_field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "recommendation": self.recommendation,
            "recommendation_reason": self.recommendation_reason,
            "horizon_days": self.horizon_days,
            "invest_period_days": self.invest_period_days,
            "invest_period_label": self.invest_period_label,
            "confidence": self.confidence,
            "scores": self.scores,
            "source_fields": self.source_fields,
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "NormalizedRecommendation":
        return cls(
            recommendation=data.get("recommendation", Recommendation.HOLD.value),
            recommendation_reason=data.get("recommendation_reason"),
            horizon_days=data.get("horizon_days"),
            invest_period_days=data.get("invest_period_days"),
            invest_period_label=data.get("invest_period_label"),
            confidence=data.get("confidence"),
            scores=data.get("scores", {}),
            source_fields=data.get("source_fields", {}),
        )


# =============================================================================
# Custom Exceptions
# =============================================================================

class RecommendationError(Exception):
    """Base exception for recommendation normalization."""
    pass


class InvalidRecommendationError(RecommendationError):
    """Raised when recommendation is invalid."""
    pass


# =============================================================================
# Pure Utility Functions (preserved from v6.1.0)
# =============================================================================

def _safe_str(value: Any) -> str:
    try:
        return "" if value is None else str(value).strip()
    except Exception:
        return ""


def _clean_text(value: Any) -> str:
    s = _safe_str(value)
    if not s:
        return ""
    return " ".join(s.split()).strip(" |;,-.")


def _first_non_empty_str(items: Iterable[Any]) -> Optional[str]:
    for item in items:
        s = _safe_str(item)
        if s and s.lower() not in ("none", "null", "n/a", ""):
            return s
    return None


def _to_float(value: Any) -> Optional[float]:
    try:
        if value is None:
            return None
        if isinstance(value, (int, float)) and not isinstance(value, bool):
            f = float(value)
        else:
            s = str(value).strip().replace(",", "")
            if not s or s.lower() in {"none", "null", "n/a", "na"}:
                return None
            if s.endswith("%"):
                f = float(s[:-1].strip()) / 100.0
            else:
                f = float(s)
        if f != f:
            return None
        return f
    except Exception:
        return None


def _to_int(value: Any) -> Optional[int]:
    f = _to_float(value)
    return int(f) if f is not None else None


def _as_ratio(value: Any) -> Optional[float]:
    f = _to_float(value)
    if f is None:
        return None
    if abs(f) > 1.5:
        return f / 100.0
    return f


def _deep_get(data: Dict[str, Any], keys: Sequence[str]) -> Optional[Any]:
    for key in keys:
        if not key:
            continue

        if key in data:
            value = data.get(key)
            if value is not None and _safe_str(value):
                return value

        if "." in key:
            current: Any = data
            parts = key.split(".")
            valid = True

            for part in parts:
                if "[" in part and "]" in part:
                    base, idx_str = part.split("[", 1)
                    idx = idx_str.rstrip("]")
                    if isinstance(current, dict) and base in current:
                        current = current[base]
                        if isinstance(current, (list, tuple)) and idx.isdigit():
                            idx_num = int(idx)
                            if 0 <= idx_num < len(current):
                                current = current[idx_num]
                            else:
                                valid = False
                                break
                        else:
                            valid = False
                            break
                    else:
                        valid = False
                        break
                else:
                    if isinstance(current, dict) and part in current:
                        current = current[part]
                    else:
                        valid = False
                        break

            if valid and current is not None and _safe_str(current):
                return current

    return None


def _extract_candidate(value: Any) -> Optional[Any]:
    if value is None:
        return None

    if isinstance(value, str) and value.upper() in _RECO_ENUM:
        return value.upper()

    if isinstance(value, (int, float)) and not isinstance(value, bool):
        return value

    if isinstance(value, dict):
        for _, fields in _VENDOR_FIELDS.items():
            v = _deep_get(value, fields)
            if v is not None and _safe_str(v):
                return v

        for key in value.keys():
            if isinstance(key, str) and any(h in key.lower() for h in ["reco", "rating", "rank", "score", "consensus"]):
                v = value[key]
                if v is not None and _safe_str(v):
                    return v

        return _first_non_empty_str(value.values())

    if isinstance(value, (list, tuple)):
        if value and isinstance(value[0], dict):
            for item in value:
                extracted = _extract_candidate(item)
                if extracted is not None:
                    return extracted
        return _first_non_empty_str(value)

    return value


# =============================================================================
# Arabic / Text Normalization (preserved)
# =============================================================================

@lru_cache(maxsize=8192)
def _normalize_arabic(text: str) -> str:
    if not text:
        return ""

    s = _PAT_WS.sub(" ", text).strip()
    replacements = {
        "أ": "ا", "إ": "ا", "آ": "ا",
        "ي": "ي", "ى": "ي",
        "ة": "ه",
        "َ": "", "ِ": "", "ُ": "", "ْ": "", "ّ": "",
    }
    for old, new in replacements.items():
        s = s.replace(old, new)

    return s


@lru_cache(maxsize=8192)
def _normalize_text(text: str) -> str:
    if not text:
        return ""

    s = text.upper()
    s = _PAT_SEP.sub(" ", s)
    s = _PAT_SPLIT.sub(" ", s)
    s = _PAT_PUNCT.sub(" ", s)
    s = _PAT_WS.sub(" ", s).strip()
    return s


# =============================================================================
# Numeric Rating Parsing (re-banded for 8 tiers in v8.0.0)
# =============================================================================

def _parse_numeric_rating(text: str, context_hints: bool = True) -> Optional[Tuple[str, float]]:
    """
    Parse an embedded numeric rating into (canonical_tier, confidence).

    v8.0.0: the 0-100 and 0-1 paths resolve into the full 8-tier space.
    The 1-5 / 1-3 star paths stay coarse — a 5-star or 3-point scale
    genuinely cannot express 8 distinct tiers — but use the wider
    vocabulary where the integer cleanly implies it (e.g. a 1/5 is a
    STRONG_BUY, a 5/5 is a STRONG_SELL).
    """
    if not text:
        return None

    su = text.upper()

    if _PAT_CONFIDENCE.search(su) and not _PAT_RATING_HINT.search(su):
        return None

    match = _PAT_RATIO.search(su)
    if match:
        try:
            x = float(match.group(1).replace(",", "."))
            y = float(match.group(2).replace(",", "."))

            if y > 0:
                if y == 5:
                    normalized = x
                elif y == 3:
                    normalized = {1: 1.0, 2: 3.0, 3: 5.0}.get(int(round(x)), 3.0)
                elif y == 100:
                    normalized = 1 + (x / 100) * 4
                else:
                    normalized = x

                # normalized is on a 1 (best) .. 5 (worst) scale.
                if normalized <= 1.25:
                    return (Recommendation.STRONG_BUY.value, 0.9)
                if normalized <= 1.9:
                    return (Recommendation.BUY.value, 0.9)
                if normalized <= 2.5:
                    return (Recommendation.ACCUMULATE.value, 0.8)
                if normalized <= 3.25:
                    return (Recommendation.HOLD.value, 0.8)
                if normalized <= 3.9:
                    return (Recommendation.REDUCE.value, 0.8)
                if normalized <= 4.5:
                    return (Recommendation.SELL.value, 0.9)
                if normalized <= 4.85:
                    return (Recommendation.STRONG_SELL.value, 0.9)
                return (Recommendation.AVOID.value, 0.9)
        except Exception:
            pass

    match = _PAT_NUMBER.search(su)
    if not match:
        return None

    try:
        num = float(match.group(1).replace(",", "."))
    except Exception:
        return None

    scale_5 = _PAT_SCALE_5.search(su) or "STAR" in su or "STARS" in su
    scale_3 = _PAT_SCALE_3.search(su)
    has_hint = _PAT_RATING_HINT.search(su) if context_hints else True

    # 1-5 integer scale: coarse, but the extremes map to the strong tiers.
    if 1.0 <= num <= 5.0 and (has_hint or scale_5):
        n = int(round(num))
        if n == 1:
            return (Recommendation.STRONG_BUY.value, 0.85)
        if n == 2:
            return (Recommendation.BUY.value, 0.85)
        if n == 3:
            return (Recommendation.HOLD.value, 0.85)
        if n == 4:
            return (Recommendation.REDUCE.value, 0.85)
        return (Recommendation.STRONG_SELL.value, 0.85)  # n == 5

    # 1-3 integer scale: too coarse for 8 tiers — map to representative anchors.
    if 1.0 <= num <= 3.0 and (has_hint or scale_3):
        n = int(round(num))
        if n == 1:
            return (Recommendation.BUY.value, 0.8)
        if n == 2:
            return (Recommendation.HOLD.value, 0.8)
        return (Recommendation.SELL.value, 0.8)

    # 0-100 scale: full 8-tier resolution, matching Recommendation.from_score.
    if 0.0 <= num <= 100.0 and has_hint:
        if num >= 85.0:
            return (Recommendation.STRONG_BUY.value, 0.8)
        if num >= 70.0:
            return (Recommendation.BUY.value, 0.78)
        if num >= 60.0:
            return (Recommendation.ACCUMULATE.value, 0.72)
        if num >= 45.0:
            return (Recommendation.HOLD.value, 0.6)
        if num >= 35.0:
            return (Recommendation.REDUCE.value, 0.72)
        if num >= 25.0:
            return (Recommendation.SELL.value, 0.78)
        if num >= 15.0:
            return (Recommendation.STRONG_SELL.value, 0.8)
        return (Recommendation.AVOID.value, 0.8)

    # 0-1 scale: lower-confidence, but still 8-tier aware.
    if 0.0 <= num <= 1.0 and has_hint:
        if num >= 0.85:
            return (Recommendation.STRONG_BUY.value, 0.55)
        if num >= 0.7:
            return (Recommendation.BUY.value, 0.52)
        if num >= 0.6:
            return (Recommendation.ACCUMULATE.value, 0.48)
        if num >= 0.45:
            return (Recommendation.HOLD.value, 0.4)
        if num >= 0.35:
            return (Recommendation.REDUCE.value, 0.48)
        if num >= 0.25:
            return (Recommendation.SELL.value, 0.52)
        if num >= 0.15:
            return (Recommendation.STRONG_SELL.value, 0.55)
        return (Recommendation.AVOID.value, 0.55)

    return None


def _apply_negation(text: str, reco: str) -> str:
    """
    Apply negation logic to a parsed recommendation.

    v8.0.0: extended for the wider vocabulary.
      - An explicit negation-context match ("NOT A BUY", "NEVER SELL", ...)
        flattens to HOLD, as before.
      - A bare negator near a directional call softens it one step toward
        the centre: STRONG_BUY -> BUY, BUY/ACCUMULATE -> HOLD,
        SELL -> REDUCE, STRONG_SELL -> SELL, AVOID -> SELL.
      - REDUCE / HOLD are already central/protective and pass through.

    Preserves v7.2.0 Phase E: "AVOID"/"AGAINST" are not negators, so an
    AVOID-from-"AVOID" classification is never spuriously softened by its
    own keyword.
    """
    if not text or not reco:
        return reco

    if _PAT_NEGATION_CONTEXT.search(text):
        return Recommendation.HOLD.value

    if _PAT_NEGATE.search(text):
        if reco == Recommendation.STRONG_BUY.value:
            return Recommendation.BUY.value
        if reco in (Recommendation.BUY.value, Recommendation.ACCUMULATE.value):
            return Recommendation.HOLD.value
        if reco == Recommendation.SELL.value:
            return Recommendation.REDUCE.value
        if reco == Recommendation.STRONG_SELL.value:
            return Recommendation.SELL.value
        if reco == Recommendation.AVOID.value:
            return Recommendation.SELL.value

    return reco


def _sentiment_analysis(text: str) -> Optional[str]:
    if not text:
        return None

    positive_terms = ["BULLISH", "POSITIVE", "OPTIMISTIC", "UP", "UPSIDE", "GAIN"]
    negative_terms = ["BEARISH", "NEGATIVE", "PESSIMISTIC", "DOWN", "DOWNSIDE", "LOSS"]

    pos_count = sum(1 for term in positive_terms if term in text)
    neg_count = sum(1 for term in negative_terms if term in text)

    if pos_count > neg_count + 1:
        return Recommendation.BUY.value
    if neg_count > pos_count + 1:
        return Recommendation.SELL.value
    return None


# =============================================================================
# Core Recommendation Parsing (8-tier aware in v8.0.0)
# =============================================================================

@lru_cache(maxsize=8192)
def _parse_string_recommendation(raw: str) -> str:
    """
    Parse a raw recommendation string into a canonical 8-tier value.

    v8.0.0 resolution order is strongest-signal-first within each language
    so a longer, more specific phrase is never shadowed by a substring
    match — e.g. "STRONG SELL" must resolve before "SELL", "ACCUMULATE"
    before "BUY", "AVOID" before "SELL".
    """
    ar_norm = _normalize_arabic(raw)

    # --- Arabic exact-set matches (strongest signal first) -------------
    if ar_norm in _AR_STRONG_BUY:
        return Recommendation.STRONG_BUY.value
    if ar_norm in _AR_AVOID:
        return Recommendation.AVOID.value
    if ar_norm in _AR_STRONG_SELL:
        return Recommendation.STRONG_SELL.value
    if ar_norm in _AR_ACCUMULATE:
        return Recommendation.ACCUMULATE.value
    if ar_norm in _AR_BUY:
        return Recommendation.BUY.value
    if ar_norm in _AR_HOLD:
        return Recommendation.HOLD.value
    if ar_norm in _AR_REDUCE:
        return Recommendation.REDUCE.value
    if ar_norm in _AR_SELL:
        return Recommendation.SELL.value

    # --- Arabic fuzzy token containment (strongest signal first) -------
    if any(tok in ar_norm for tok in ["تجنب", "ابتعد", "غير قابل للاستثمار"]):
        return Recommendation.AVOID.value
    if any(tok in ar_norm for tok in ["بيع قوي", "خروج فوري", "تصفية كاملة"]):
        return Recommendation.STRONG_SELL.value
    if any(tok in ar_norm for tok in ["شراء قوي", "شراء مكثف"]):
        return Recommendation.STRONG_BUY.value
    if any(tok in ar_norm for tok in ["تجميع", "تراكم", "شراء تدريجي", "دخول تدريجي"]):
        return Recommendation.ACCUMULATE.value
    if any(tok in ar_norm for tok in ["شراء", "اشتر", "زيادة"]):
        return Recommendation.BUY.value
    if any(tok in ar_norm for tok in ["تخارج", "تصفية"]):
        return Recommendation.SELL.value
    if any(tok in ar_norm for tok in ["جني ارباح", "تخفيف", "تقليص"]):
        return Recommendation.REDUCE.value
    if any(tok in ar_norm for tok in ["احتفاظ", "محايد", "انتظار", "مراقبة"]):
        return Recommendation.HOLD.value

    norm = _normalize_text(raw)
    if not norm:
        return Recommendation.HOLD.value

    if norm in _NO_RATING:
        return Recommendation.HOLD.value

    # --- Multilingual (FR/ES/DE/PT) exact-set matches ------------------
    # Strongest signal first so "STRONG SELL" / "AVOID" / "ACCUMULATE"
    # are never shadowed by the plainer BUY/SELL sets.
    if norm in _LANG_STRONG_BUY:
        return Recommendation.STRONG_BUY.value
    if norm in _LANG_AVOID:
        return Recommendation.AVOID.value
    if norm in _LANG_STRONG_SELL:
        return Recommendation.STRONG_SELL.value
    if norm in _LANG_ACCUMULATE:
        return Recommendation.ACCUMULATE.value
    if norm in _LANG_BUY:
        return Recommendation.BUY.value
    if norm in _LANG_SELL:
        return Recommendation.SELL.value
    if norm in _LANG_HOLD:
        return Recommendation.HOLD.value
    if norm in _LANG_REDUCE:
        return Recommendation.REDUCE.value

    # --- English exact-set + regex (strongest signal first) ------------
    if norm in _AVOID_LIKE or _PAT_AVOID.search(norm):
        # The AVOID idioms ("DO NOT BUY", "NEVER BUY", "DO NOT ENTER" ...)
        # embed a negator as part of their meaning. They must NOT pass
        # through _apply_negation, which would misread that embedded
        # NOT/NEVER as negating the call and flatten a clear AVOID to HOLD.
        # Only a genuine negation of AVOID itself ("not avoid") flattens.
        if _PAT_AVOID_NEGATED.search(norm):
            return Recommendation.HOLD.value
        return Recommendation.AVOID.value
    if norm in _STRONG_SELL_LIKE or _PAT_STRONG_SELL.search(norm):
        return _apply_negation(norm, Recommendation.STRONG_SELL.value)
    if norm in _STRONG_BUY_LIKE or _PAT_STRONG_BUY.search(norm):
        return _apply_negation(norm, Recommendation.STRONG_BUY.value)
    if norm in _ACCUMULATE_LIKE or _PAT_ACCUMULATE.search(norm):
        return _apply_negation(norm, Recommendation.ACCUMULATE.value)

    if norm in _SELL_LIKE or _PAT_SELL.search(norm):
        return _apply_negation(norm, Recommendation.SELL.value)
    if norm in _REDUCE_LIKE or _PAT_REDUCE.search(norm):
        return _apply_negation(norm, Recommendation.REDUCE.value)
    if norm in _HOLD_LIKE or _PAT_HOLD.search(norm):
        return _apply_negation(norm, Recommendation.HOLD.value)
    if norm in _BUY_LIKE or _PAT_BUY.search(norm):
        return _apply_negation(norm, Recommendation.BUY.value)

    num_result = _parse_numeric_rating(norm, context_hints=True)
    if num_result and num_result[1] >= _CONFIDENCE_MEDIUM:
        return _apply_negation(norm, num_result[0])

    # --- regex fall-through (strongest signal first) -------------------
    if _PAT_AVOID.search(norm):
        # Same idiom-vs-genuine-negation handling as the exact-set branch.
        if _PAT_AVOID_NEGATED.search(norm):
            return Recommendation.HOLD.value
        return Recommendation.AVOID.value
    if _PAT_STRONG_SELL.search(norm):
        return _apply_negation(norm, Recommendation.STRONG_SELL.value)
    if _PAT_ACCUMULATE.search(norm):
        return _apply_negation(norm, Recommendation.ACCUMULATE.value)
    if _PAT_SELL.search(norm):
        return _apply_negation(norm, Recommendation.SELL.value)
    if _PAT_REDUCE.search(norm):
        return _apply_negation(norm, Recommendation.REDUCE.value)
    if _PAT_HOLD.search(norm):
        return _apply_negation(norm, Recommendation.HOLD.value)
    if _PAT_BUY.search(norm):
        return _apply_negation(norm, Recommendation.BUY.value)

    sentiment = _sentiment_analysis(norm)
    if sentiment:
        return sentiment

    return Recommendation.HOLD.value


# =============================================================================
# Horizon Helpers (preserved)
# =============================================================================

def _horizon_label_from_days(days: Optional[int]) -> Optional[str]:
    if days is None or days <= 0:
        return None
    if days <= 45:
        return "1M"
    if days <= 135:
        return "3M"
    if days <= 240:
        return "6M"
    return "12M"


def _infer_horizon_days_from_payload(data: Dict[str, Any]) -> Optional[int]:
    value = _deep_get(data, _HORIZON_DAYS_FIELDS)
    if value is not None:
        di = _to_int(value)
        if di is not None and di > 0:
            return di

    label = _clean_text(_deep_get(data, _HORIZON_LABEL_FIELDS)).upper()
    if label:
        label_map = {
            "1M": 30, "30D": 30, "30": 30,
            "3M": 90, "90D": 90, "90": 90,
            "6M": 180, "180D": 180, "180": 180,
            "12M": 365, "1Y": 365, "365D": 365, "365": 365,
        }
        if label in label_map:
            return label_map[label]

    if _deep_get(data, ["required_roi_1m"]) is not None:
        return 30
    if _deep_get(data, ["required_roi_3m"]) is not None:
        return 90
    if _deep_get(data, ["required_roi_12m"]) is not None:
        return 365

    return None


def _infer_horizon_label_from_payload(data: Dict[str, Any], horizon_days: Optional[int]) -> Optional[str]:
    label = _clean_text(_deep_get(data, _HORIZON_LABEL_FIELDS)).upper()
    if label:
        return label
    return _horizon_label_from_days(horizon_days)


# =============================================================================
# Public API (preserved + new)
# =============================================================================

def normalize_recommendation(value: Any) -> str:
    """
    Normalize a recommendation to canonical form.

    v8.0.0: may now return any of the eight canonical tiers
    (STRONG_BUY / BUY / ACCUMULATE / HOLD / REDUCE / SELL /
    STRONG_SELL / AVOID). Consumers with a hard-coded 5-value allow-list
    must widen it; `is_valid_recommendation()` reflects the full set.
    """
    try:
        candidate = _extract_candidate(value)
        if candidate is None:
            return Recommendation.HOLD.value

        if isinstance(candidate, str):
            s0 = candidate.strip()
            if not s0:
                return Recommendation.HOLD.value
            s0u = s0.upper()
            if s0u in _RECO_ENUM:
                return s0u
            return _parse_string_recommendation(s0)

        if isinstance(candidate, (int, float)) and not isinstance(candidate, bool):
            num_result = _parse_numeric_rating(str(candidate), context_hints=False)
            if num_result:
                return num_result[0]
            return Recommendation.HOLD.value

        s_raw = _safe_str(candidate)
        if not s_raw:
            return Recommendation.HOLD.value

        return _parse_string_recommendation(s_raw)
    except Exception:
        return Recommendation.HOLD.value


def normalize_recommendation_payload(data: Any) -> Dict[str, Any]:
    """Normalize a recommendation payload without stripping context. (Preserved.)"""
    try:
        if isinstance(data, dict):
            raw = dict(data)
        else:
            raw = {"recommendation": data}

        reco_value = _deep_get(raw, _VENDOR_FIELDS["default"] + _VENDOR_FIELDS["nested"])
        if reco_value is None:
            reco_value = raw.get("recommendation")
        recommendation = normalize_recommendation(reco_value if reco_value is not None else raw)

        reason = _clean_text(_deep_get(raw, _REASON_FIELDS))
        if not reason:
            reason = _clean_text(raw.get("recommendation_reason"))

        horizon_days = _infer_horizon_days_from_payload(raw)
        if horizon_days is None:
            horizon_days = _to_int(raw.get("horizon_days")) or _to_int(raw.get("invest_period_days"))

        invest_period_label = _infer_horizon_label_from_payload(raw, horizon_days)
        if not invest_period_label:
            invest_period_label = _clean_text(raw.get("invest_period_label")).upper() or None

        confidence = None
        for conf_field in _CONFIDENCE_FIELDS:
            val = _deep_get(raw, [conf_field])
            if val is not None:
                confidence = _as_ratio(val)
                if confidence is not None:
                    break

        scores: Dict[str, float] = {}
        for score_field in _SCORE_FIELDS:
            val = _deep_get(raw, [score_field])
            if val is not None:
                scores[score_field] = _to_float(val)

        return {
            "recommendation": recommendation,
            "recommendation_reason": reason or None,
            "horizon_days": horizon_days,
            "invest_period_days": _to_int(raw.get("invest_period_days")) or horizon_days,
            "invest_period_label": invest_period_label,
            "confidence": confidence,
            "scores": scores,
            "source_fields": {},
        }
    except Exception:
        return {
            "recommendation": Recommendation.HOLD.value,
            "recommendation_reason": None,
            "horizon_days": None,
            "invest_period_days": None,
            "invest_period_label": None,
            "confidence": None,
            "scores": {},
            "source_fields": {},
        }


def normalize_recommendation_row(row: Dict[str, Any]) -> Dict[str, Any]:
    """Normalize a recommendation row. (Preserved.)"""
    return normalize_recommendation_payload(row)


def normalize_recommendation_rows(rows: Sequence[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Normalize multiple recommendation rows. (Preserved.)"""
    result: List[Dict[str, Any]] = []
    for row in rows:
        try:
            result.append(normalize_recommendation_row(row))
        except Exception:
            result.append({
                "recommendation": Recommendation.HOLD.value,
                "recommendation_reason": None,
                "horizon_days": None,
                "invest_period_days": None,
                "invest_period_label": None,
                "confidence": None,
                "scores": {},
                "source_fields": {},
            })
    return result


def batch_normalize(recommendations: Sequence[Any]) -> List[str]:
    """Normalize a batch of recommendations. (Preserved.)"""
    return [normalize_recommendation(r) for r in recommendations]


def is_valid_recommendation(value: Any) -> bool:
    """
    Check if value is a valid canonical recommendation.

    v8.0.0: the valid set is the full 8-tier vocabulary.
    """
    try:
        return str(value) in _RECO_ENUM
    except Exception:
        return False


def get_recommendation_score(reco: Any) -> int:
    """
    Get numeric ordinal for a recommendation (0-7, 0 = STRONG_BUY best,
    7 = AVOID worst).

    v8.0.0: range widened from 0-4 to 0-7. Ordering is preserved and
    monotonic; ranking consumers are unaffected. Unknown values map to
    HOLD's ordinal (3), the neutral centre.
    """
    try:
        s = str(reco)
    except Exception:
        return Recommendation.HOLD.to_score()
    if s in _RECO_ENUM:
        try:
            return Recommendation(s).to_score()
        except Exception:
            return Recommendation.HOLD.to_score()
    return Recommendation.HOLD.to_score()


def recommendation_from_score(score: Any, scale: str = "1-5") -> str:
    """Convert score to recommendation. (Re-banded for 8 tiers in v8.0.0.)"""
    return Recommendation.from_score(score, scale).value


# =============================================================================
# Backward-compatibility constants
# =============================================================================

RECO_STRONG_BUY = Recommendation.STRONG_BUY.value
RECO_BUY = Recommendation.BUY.value
RECO_ACCUMULATE = Recommendation.ACCUMULATE.value   # v8.0.0
RECO_HOLD = Recommendation.HOLD.value
RECO_REDUCE = Recommendation.REDUCE.value
RECO_SELL = Recommendation.SELL.value
RECO_STRONG_SELL = Recommendation.STRONG_SELL.value  # v8.0.0
RECO_AVOID = Recommendation.AVOID.value              # v8.0.0


# =============================================================================
# Module Exports
# =============================================================================

__all__ = [
    # Version
    "VERSION",
    "__version__",
    # Enums
    "Recommendation",
    # Recommendation constants (v8.0.0: 8-tier)
    "RECO_STRONG_BUY",
    "RECO_BUY",
    "RECO_ACCUMULATE",
    "RECO_HOLD",
    "RECO_REDUCE",
    "RECO_SELL",
    "RECO_STRONG_SELL",
    "RECO_AVOID",
    # Ordinal helper (v8.0.0)
    "WORST_ORDINAL",
    # View-token constants
    "VIEW_FUND_BULLISH",
    "VIEW_FUND_NEUTRAL",
    "VIEW_FUND_BEARISH",
    "VIEW_TECH_BULLISH",
    "VIEW_TECH_NEUTRAL",
    "VIEW_TECH_BEARISH",
    "VIEW_RISK_LOW",
    "VIEW_RISK_MODERATE",
    "VIEW_RISK_HIGH",
    "VIEW_VALUE_CHEAP",
    "VIEW_VALUE_FAIR",
    "VIEW_VALUE_EXPENSIVE",
    # Rule-id constants
    "RULE_ID_INSUFFICIENT_DATA_NO_SIGNALS",
    "RULE_ID_INSUFFICIENT_DATA_TOO_FEW",
    "RULE_ID_HARD_SELL_DOUBLE_BEARISH",
    "RULE_ID_HARD_SELL_LOW_SCORE",
    "RULE_ID_REDUCE_BEARISH_FUND",
    "RULE_ID_REDUCE_BEARISH_TECH_EXPENSIVE",
    "RULE_ID_REDUCE_WEAK_HIGH_RISK",
    "RULE_ID_STRONG_BUY_ALL",
    "RULE_ID_BUY_BULLISH_FUND",
    "RULE_ID_HOLD_BULLISH_EXPENSIVE_VETO",
    "RULE_ID_HOLD_MIXED",
    "RULE_ID_DOWNGRADED_STRONG_BUY_TO_BUY",
    "RULE_ID_DOWNGRADED_BUY_TO_HOLD",
    "RULE_ID_DOWNGRADED_STRONG_BUY_TO_HOLD",
    # v8.0.0 rule-ids
    "RULE_ID_ACCUMULATE_BULLISH_MODERATE",
    "RULE_ID_DOWNGRADED_ACCUMULATE_TO_HOLD",
    "RULE_ID_HARD_STRONG_SELL_DOUBLE_BEARISH",
    "RULE_ID_AVOID_UNINVESTABLE",
    # View helpers
    "normalize_view_token",
    "recommendation_from_views",
    "recommendation_from_views_with_rule_id",
    # Data classes
    "NormalizedRecommendation",
    # Core functions
    "normalize_recommendation",
    "normalize_recommendation_payload",
    "normalize_recommendation_row",
    "normalize_recommendation_rows",
    "batch_normalize",
    "is_valid_recommendation",
    "get_recommendation_score",
    "recommendation_from_score",
    # Exceptions
    "RecommendationError",
    "InvalidRecommendationError",
]
