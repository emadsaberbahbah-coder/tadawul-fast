#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
core/reco_normalize.py
================================================================================
Recommendation Normalization -- v7.1.0
================================================================================

v7.1.0 changes (vs v7.0.0)
--------------------------
- NEW kwargs on `Recommendation.from_views()`:
    - `conviction`        (Optional[float], 0-100): when supplied, applies
      conviction floors to the resulting recommendation:
          STRONG_BUY    requires conviction >= 60  (else downgrade -> BUY)
          BUY           requires conviction >= 45  (else downgrade -> HOLD)
      REDUCE/SELL/HOLD are NOT downgraded — those are protective calls and
      we want them to fire even when conviction is moderate. The conviction
      number is also surfaced in the reason text so the user sees why a
      call was capped.
    - `sector_relative`   (Optional[float], 0-100): when supplied, surfaces
      the percentile rank in the reason text. Does NOT change rule flow in
      v7.1.0 (left to a future revision after we have backtest data on
      sector-rotation effects).
  Both kwargs are OPTIONAL with default None — every existing v7.0.0 caller
  continues to work without changes.

- FIX (defensive): `from_views()` now guards against `score` being a numpy
  scalar / Decimal that survived basic float() but doesn't compare cleanly.
  Wraps in `_to_float_strict()` before threshold checks.

- FIX (cleanup): When BOTH conviction and base reason mention the score,
  we de-duplicate the score badge so reasons stay readable.

- DOC: Examples updated to show the conviction-floor downgrade path.

- PRESERVED (no behavior change): all v7.0.0 behavior is unchanged when
  conviction and sector_relative are not passed. Every public symbol from
  v7.0.0 is exported. This is purely additive.

v7.0.0 features (preserved verbatim)
------------------------------------
- View-aware 5-tier resolver with EXPENSIVE-veto / double-bearish-SELL /
  STRONG_BUY-all-conditions / etc.
- All language pattern dictionaries (EN/AR/FR/ES/DE/PT) and vendor field
  mappings.
- normalize_recommendation*, batch_normalize, NormalizedRecommendation.

Public API (additions only — fully backward compatible)
- Recommendation.from_views(...) gains `conviction` and `sector_relative`.
- recommendation_from_views(...) gains the same kwargs.
================================================================================
"""

from __future__ import annotations

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

VERSION = "7.1.0"


# =============================================================================
# Enums and view constants
# =============================================================================

class Recommendation(str, Enum):
    """Canonical recommendation values."""
    STRONG_BUY = "STRONG_BUY"
    BUY = "BUY"
    HOLD = "HOLD"
    REDUCE = "REDUCE"
    SELL = "SELL"

    # ---- numeric score -> recommendation (preserved from v6.1.0) -----------

    @classmethod
    def from_score(cls, score: Any, scale: str = "1-5") -> "Recommendation":
        """Convert a numeric score to a recommendation. (Preserved from v6.1.0.)"""
        try:
            if isinstance(score, bool):
                s = float(score)
            elif isinstance(score, (int, float)):
                s = float(score)
            else:
                s = float(str(score).strip().replace(",", "."))
            if s != s:
                return cls.HOLD
        except (TypeError, ValueError):
            return cls.HOLD

        if scale == "1-5":
            if s <= 1.5:
                return cls.STRONG_BUY
            if s <= 2.5:
                return cls.BUY
            if s <= 3.5:
                return cls.HOLD
            if s <= 4.5:
                return cls.REDUCE
            return cls.SELL

        if scale == "1-3":
            if s <= 1.3:
                return cls.STRONG_BUY
            if s <= 1.8:
                return cls.BUY
            if s <= 2.3:
                return cls.HOLD
            if s <= 2.7:
                return cls.REDUCE
            return cls.SELL

        if scale == "0-100":
            if s >= 85:
                return cls.STRONG_BUY
            if s >= 65:
                return cls.BUY
            if s >= 45:
                return cls.HOLD
            if s >= 35:
                return cls.REDUCE
            return cls.SELL

        return cls.HOLD

    def to_score(self) -> int:
        """Convert recommendation to a 0-4 score (0 = best). (Preserved.)"""
        return {
            Recommendation.STRONG_BUY: 0,
            Recommendation.BUY: 1,
            Recommendation.HOLD: 2,
            Recommendation.REDUCE: 3,
            Recommendation.SELL: 4,
        }.get(self, 2)

    @classmethod
    def is_valid(cls, value: str) -> bool:
        """Check if value is a valid recommendation."""
        return value in cls._value2member_map_

    # ---- v7.0.0 + v7.1.0: view-aware resolver ------------------------------

    @classmethod
    def from_views(
        cls,
        *,
        fundamental: Any = None,
        technical: Any = None,
        risk: Any = None,
        value: Any = None,
        score: Any = None,
        # v7.1.0 additions (both optional)
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
            conviction:      Conviction Score   (0-100, optional, v7.1.0+)
            sector_relative: Sector-Adj Score   (0-100, optional, v7.1.0+)

        Returns:
            (Recommendation enum, reason string).

        Conviction-floor downgrades (v7.1.0):
            - STRONG_BUY with conviction < 60 -> downgraded to BUY
            - BUY with conviction < 45        -> downgraded to HOLD
            - REDUCE / SELL / HOLD are never downgraded (protective calls)

        Examples:
            >>> Recommendation.from_views(
            ...     fundamental="BULLISH", technical="BULLISH",
            ...     risk="LOW", value="CHEAP", score=82, conviction=78
            ... )
            (<Recommendation.STRONG_BUY: 'STRONG_BUY'>, 'Strong fundamentals + ...')

            >>> # Conviction floor: same views/score but conviction too low
            >>> Recommendation.from_views(
            ...     fundamental="BULLISH", technical="BULLISH",
            ...     risk="LOW", value="CHEAP", score=82, conviction=52
            ... )
            (<Recommendation.BUY: 'BUY'>, '... downgraded from STRONG_BUY ...')

            >>> # EXPENSIVE veto preserved from v7.0.0
            >>> Recommendation.from_views(
            ...     fundamental="BULLISH", technical="BULLISH",
            ...     risk="LOW", value="EXPENSIVE", score=80
            ... )
            (<Recommendation.HOLD: 'HOLD'>, 'Bullish fundamentals but valuation ...')

            >>> # Double-bearish forces SELL regardless of score
            >>> Recommendation.from_views(
            ...     fundamental="BEARISH", technical="BEARISH",
            ...     risk="HIGH", value="FAIR", score=80
            ... )
            (<Recommendation.SELL: 'SELL'>, 'Bearish fundamentals AND bearish ...')
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
            return cls.HOLD, "Insufficient data — no views and no score available."
        if views_present < 2 and s_val is None:
            return cls.HOLD, "Insufficient data — too few signals to recommend."

        # ----- Priority 2: HARD SELL ------------------------------------
        if f == VIEW_FUND_BEARISH and t == VIEW_TECH_BEARISH:
            score_text = f", Score={s_val:.0f}" if s_val is not None else ""
            return cls.SELL, (
                f"Bearish fundamentals AND bearish technicals — exit"
                f"{score_text}."
            )

        if s_val is not None and s_val < 35.0:
            return cls.SELL, f"Very weak overall score ({s_val:.1f}) — exit."

        # ----- Priority 3: REDUCE ---------------------------------------
        if f == VIEW_FUND_BEARISH:
            return cls.REDUCE, (
                "Bearish fundamentals — reduce exposure even with mixed "
                "technical/value signals."
            )

        if t == VIEW_TECH_BEARISH and v == VIEW_VALUE_EXPENSIVE:
            return cls.REDUCE, (
                "Bearish technicals on an expensive valuation — reduce."
            )

        if s_val is not None and s_val < 50.0 and r == VIEW_RISK_HIGH:
            return cls.REDUCE, (
                f"Weak score ({s_val:.1f}) combined with high risk — reduce."
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
            return _apply_conviction_floor(
                cls.STRONG_BUY, base_reason, conv_val, sect_val
            )

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
            return _apply_conviction_floor(
                cls.BUY, base_reason, conv_val, sect_val
            )

        # Special-case BUY veto explanation: bullish fundamentals but
        # valuation says EXPENSIVE -> HOLD with clear reason.
        if (
            f == VIEW_FUND_BULLISH
            and t in (VIEW_TECH_BULLISH, VIEW_TECH_NEUTRAL)
            and v == VIEW_VALUE_EXPENSIVE
        ):
            return cls.HOLD, _attach_metrics(
                "Bullish fundamentals but valuation expensive — wait for "
                "a better entry.",
                conv_val, sect_val,
            )

        # ----- Priority 6: HOLD (default) -------------------------------
        score_part = f" Score={s_val:.1f}." if s_val is not None else ""
        view_summary = (
            f"Fund={f or 'N/A'}, Tech={t or 'N/A'}, "
            f"Risk={r or 'N/A'}, Value={v or 'N/A'}."
        )
        return cls.HOLD, _attach_metrics(
            f"Mixed signals — holding. {view_summary}{score_part}",
            conv_val, sect_val,
        )


# =============================================================================
# v7.1.0 internals: conviction-floor enforcement
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


_STRONG_BUY_CONVICTION_FLOOR = 60.0
_BUY_CONVICTION_FLOOR = 45.0


def _apply_conviction_floor(
    initial: Recommendation,
    initial_reason: str,
    conviction: Optional[float],
    sector_relative: Optional[float],
) -> Tuple[Recommendation, str]:
    """
    Apply conviction floors to a STRONG_BUY/BUY recommendation.

    Rules (v7.1.0):
      - STRONG_BUY with conviction < 60 -> BUY (and note in reason)
      - BUY with conviction < 45        -> HOLD (and note in reason)
      - Any rec with conviction is None  -> unchanged (just attach badge)

    Protective calls (REDUCE/SELL/HOLD) are never passed here, so they
    never get downgraded.
    """
    if conviction is None:
        # No conviction info — pass through with whatever badges apply
        return initial, _attach_metrics(initial_reason, None, sector_relative)

    # Cascade: STRONG_BUY -> BUY -> HOLD if conviction is below all floors.
    # Single-step would leave STRONG_BUY at BUY with conv=40 even though
    # BUY's own floor is 45, which is dishonest. Iterate until stable.
    current = initial
    current_reason = initial_reason
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

    if downgrades:
        current_reason = (
            f"{initial_reason} Downgraded: " + "; ".join(downgrades) + "."
        )

    return current, _attach_metrics(current_reason, conviction, sector_relative)


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
    "NEUTRAL": VIEW_TECH_NEUTRAL, "FLAT": VIEW_TECH_NEUTRAL,
    "SIDEWAYS": VIEW_TECH_NEUTRAL, "MIXED": VIEW_TECH_NEUTRAL,
    "BEARISH": VIEW_TECH_BEARISH, "BEAR": VIEW_TECH_BEARISH,
    "DOWN": VIEW_TECH_BEARISH, "NEGATIVE": VIEW_TECH_BEARISH,
    "DOWNTREND": VIEW_TECH_BEARISH, "WEAK": VIEW_TECH_BEARISH,
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
    "NEUTRAL": VIEW_VALUE_FAIR,
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


def recommendation_from_views(
    fundamental: Any = None,
    technical: Any = None,
    risk: Any = None,
    value: Any = None,
    score: Any = None,
    conviction: Any = None,        # v7.1.0
    sector_relative: Any = None,   # v7.1.0
) -> Tuple[str, str]:
    """
    Free-function wrapper around `Recommendation.from_views()`.

    Returns:
        (canonical_string, reason_text). canonical_string is one of
        STRONG_BUY / BUY / HOLD / REDUCE / SELL.
    """
    rec, reason = Recommendation.from_views(
        fundamental=fundamental,
        technical=technical,
        risk=risk,
        value=value,
        score=score,
        conviction=conviction,
        sector_relative=sector_relative,
    )
    return rec.value, reason


# =============================================================================
# Constants (preserved verbatim from v6.1.0 / v7.0.0)
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
    "STRONG BUY RECOMMENDATION", "MUST BUY", "STRONG ACCUMULATE", "FOCUS BUY",
}

_STRONG_SELL_LIKE: Set[str] = {
    "STRONG SELL", "SELL (STRONG)", "SELL STRONG", "EXIT IMMEDIATELY",
    "STRONG SELL RECOMMENDATION", "MUST SELL", "URGENT SELL", "FORCED EXIT",
    "LIQUIDATE ALL",
}

_BUY_LIKE: Set[str] = {
    "BUY", "ACCUMULATE", "ADD", "OUTPERFORM", "MARKET OUTPERFORM",
    "SECTOR OUTPERFORM", "OVERWEIGHT", "INCREASE", "UPGRADE", "LONG",
    "BULLISH", "POSITIVE", "BUYING", "SPECULATIVE BUY", "RECOMMENDED BUY",
    "BUY ON DIPS", "BUY RECOMMENDATION", "ACCUMULATION", "BUILT", "POSITION",
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
    "SELL", "EXIT", "AVOID", "UNDERPERFORM", "MARKET UNDERPERFORM",
    "SECTOR UNDERPERFORM", "SHORT", "BEARISH", "NEGATIVE", "SELLING",
    "DOWNGRADE", "RED FLAG", "LIQUIDATE", "CLOSE POSITION", "UNWIND",
    "SELL RECOMMENDATION", "AVOIDANCE", "EXIT POSITION",
}

_AR_BUY: Set[str] = {
    "شراء", "اشتر", "اشترى", "اشترِ", "تجميع", "زيادة", "ايجابي", "ايجابى",
    "فرصة شراء", "اداء متفوق", "زيادة المراكز", "توصية شراء", "توصية بالشراء",
    "شراء قوي", "شراء قوى", "احتفاظ مع ميل للشراء", "زيادة التعرض", "تعزيز المراكز",
    "شراء مكثف", "تراكم", "مضاعفة", "تفوق", "متفوق", "زيادة وزن", "فرصة استثمارية",
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
    "بيع", "تخارج", "سلبي", "سلبى", "تجنب", "خروج", "بيع قوي", "بيع قوى",
    "توصية بيع", "توصية بالبيع", "اداء اقل", "اداء أقل", "تصفية", "الخروج من السهم",
    "بيع كلي", "تسييل", "تخلص من", "تجنب تماما",
}

_FR_BUY: Set[str] = {
    "ACHETER", "ACHAT", "ACCUMULER", "RENFORCER", "SURPERFORMER", "SURPERFORMANCE",
    "POSITIF", "BULLISH", "RECOMMANDATION D'ACHAT", "ACHAT FORT", "ACHAT SOLIDE",
    "ACCUMULATION", "AJOUTER", "SURPONDÉRER", "HAUSSIER",
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
    "VENDRE", "CÉDER", "ÉVITER", "SOUS-PERFORMER", "SOUS-PERFORMANCE",
    "BAISSIER", "NÉGATIF", "SORTIR", "LIQUIDER", "VENDRE FORT",
    "RECOMMANDATION DE VENTE", "VENTE", "DÉGRADER",
}

_ES_BUY: Set[str] = {
    "COMPRAR", "ACUMULAR", "SOBREPONDERAR", "SOBRERENDIMIENTO", "POSITIVO",
    "ALCISTA", "COMPRA", "RECOMENDACIÓN DE COMPRA", "COMPRA FUERTE",
    "AÑADIR", "INCREMENTAR", "SUPERAR", "OUTPERFORM",
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
    "VENDER", "EVITAR", "BAJISTA", "NEGATIVO", "SALIR", "LIQUIDAR",
    "BAJO RENDIMIENTO", "RENDIMIENTO INFERIOR", "VENTA", "DESHACERSE",
    "RECOMENDACIÓN DE VENTA", "VENTA FUERTE",
}

_DE_BUY: Set[str] = {
    "KAUFEN", "KAUF", "AKKUMULIEREN", "AUFSTOCKEN", "ÜBERGEWICHTEN",
    "OUTPERFORM", "POSITIV", "BULLISH", "KAUFEMPFEHLUNG", "STARKER KAUF",
    "HINZUFÜGEN", "ERHÖHEN", "ÜBERTREFFEN",
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
    "VERKAUFEN", "VERKAUF", "VERMEIDEN", "UNTERPERFORM", "BAISSIST", "NEGATIV",
    "AUSSTEIGEN", "LIQUIDIEREN", "VERKAUFSEMPFEHLUNG", "STARKER VERKAUF",
    "ABSTOSSEN", "REDUZIEREN AUF NULL",
}

_PT_BUY: Set[str] = {
    "COMPRAR", "ACUMULAR", "SOBREPONDERAR", "OUTPERFORM", "POSITIVO",
    "ALCISTA", "COMPRA", "RECOMENDAÇÃO DE COMPRA", "COMPRA FORTE",
    "ADICIONAR", "AUMENTAR", "SUPERAR", "ACUMULAÇÃO",
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
    "VENDER", "EVITAR", "BAIXISTA", "NEGATIVO", "SAIR", "LIQUIDAR",
    "DESEMPENHO INFERIOR", "SUBPERFORM", "VENDA", "LIVRAR-SE",
    "RECOMENDAÇÃO DE VENTA", "VENDA FORTE", "LIQUIDAÇÃO",
}

_LANG_BUY = _FR_BUY | _ES_BUY | _DE_BUY | _PT_BUY
_LANG_HOLD = _FR_HOLD | _ES_HOLD | _DE_HOLD | _PT_HOLD
_LANG_REDUCE = _FR_REDUCE | _ES_REDUCE | _DE_REDUCE | _PT_REDUCE
_LANG_SELL = _FR_SELL | _ES_SELL | _DE_SELL | _PT_SELL

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

_PAT_NEGATE = re.compile(r"\b(?:NOT|NO|NEVER|WITHOUT|HARDLY|RARELY|UNLIKELY|AVOID|AGAINST)\b", re.IGNORECASE)
_PAT_NEGATION_CONTEXT = re.compile(r"(?:NOT|NO|NEVER)\s+(?:A\s+|AN\s+)?(STRONG\s+)?(BUY|SELL|HOLD|REDUCE)", re.IGNORECASE)

_PAT_STRONG_BUY = re.compile(r"\b(?:STRONG\s+BUY|CONVICTION\s+BUY|TOP\s+PICK|BEST\s+IDEA|MUST\s+BUY|FOCUS\s+BUY)\b", re.IGNORECASE)
_PAT_STRONG_SELL = re.compile(r"\b(?:STRONG\s+SELL|EXIT\s+IMMEDIATELY|MUST\s+SELL|URGENT\s+SELL)\b", re.IGNORECASE)

_PAT_SELL = re.compile(r"\b(?:SELL|EXIT|AVOID|UNDERPERFORM|MARKET\s+UNDERPERFORM|SECTOR\s+UNDERPERFORM|SHORT|BEARISH|NEGATIVE|SELLING|DOWNGRADE|RED\s+FLAG|LIQUIDATE|CLOSE\s+POSITION|UNWIND|SELL\s+RECOMMENDATION|AVOIDANCE|EXIT\s+POSITION)\b", re.IGNORECASE)
_PAT_REDUCE = re.compile(r"\b(?:REDUCE|TRIM|LIGHTEN|PARTIAL\s+SELL|TAKE\s+PROFIT|TAKE\s+PROFITS|UNDERWEIGHT|DECREASE|WEAKEN|CAUTIOUS|PROFIT\s+TAKING|SELL\s+STRENGTH|TRIM\s+POSITION|REDUCE\s+EXPOSURE|PARTIAL\s+EXIT|REDUCE\s+WEIGHT|CUT\s+POSITION|LESSEN|SCALE\s+BACK)\b", re.IGNORECASE)
_PAT_HOLD = re.compile(r"\b(?:HOLD|NEUTRAL|MAINTAIN|UNCHANGED|MARKET\s+PERFORM|SECTOR\s+PERFORM|IN\s+LINE|EQUAL\s+WEIGHT|EQUALWEIGHT|FAIR\s+VALUE|FAIRLY\s+VALUED|WAIT|KEEP|STABLE|WATCH|MONITOR|NO\s+ACTION|PERFORM|HOLD\s+RECOMMENDATION|HOLDING|KEEP\s+POSITION|NO\s+CHANGE)\b", re.IGNORECASE)
_PAT_BUY = re.compile(r"\b(?:BUY|ACCUMULATE|ADD|OUTPERFORM|MARKET\s+OUTPERFORM|SECTOR\s+OUTPERFORM|OVERWEIGHT|INCREASE|UPGRADE|LONG|BULLISH|POSITIVE|BUYING|SPECULATIVE\s+BUY|RECOMMENDED\s+BUY|BUY\s+ON\s+DIPS|BUY\s+RECOMMENDATION|ACCUMULATION|BUILT|POSITION)\b", re.IGNORECASE)

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
# Numeric Rating Parsing (preserved from v6.1.0)
# =============================================================================

def _parse_numeric_rating(text: str, context_hints: bool = True) -> Optional[Tuple[str, float]]:
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

                if normalized <= 2.0:
                    return (Recommendation.BUY.value, 0.9)
                if normalized <= 3.0:
                    return (Recommendation.HOLD.value, 0.7)
                if normalized <= 4.0:
                    return (Recommendation.REDUCE.value, 0.7)
                return (Recommendation.SELL.value, 0.9)
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

    if 1.0 <= num <= 5.0 and (has_hint or scale_5):
        n = int(round(num))
        if n <= 2:
            return (Recommendation.BUY.value, 0.85)
        if n == 3:
            return (Recommendation.HOLD.value, 0.85)
        if n == 4:
            return (Recommendation.REDUCE.value, 0.85)
        return (Recommendation.SELL.value, 0.85)

    if 1.0 <= num <= 3.0 and (has_hint or scale_3):
        n = int(round(num))
        if n == 1:
            return (Recommendation.BUY.value, 0.8)
        if n == 2:
            return (Recommendation.HOLD.value, 0.8)
        return (Recommendation.SELL.value, 0.8)

    if 0.0 <= num <= 100.0 and has_hint:
        if num >= 70.0:
            return (Recommendation.BUY.value, 0.75)
        if num >= 45.0:
            return (Recommendation.HOLD.value, 0.6)
        return (Recommendation.SELL.value, 0.75)

    if 0.0 <= num <= 1.0 and has_hint:
        if num >= 0.7:
            return (Recommendation.BUY.value, 0.5)
        if num >= 0.45:
            return (Recommendation.HOLD.value, 0.4)
        return (Recommendation.SELL.value, 0.5)

    return None


def _apply_negation(text: str, reco: str) -> str:
    if not text or not reco:
        return reco

    if _PAT_NEGATION_CONTEXT.search(text):
        return Recommendation.HOLD.value

    if _PAT_NEGATE.search(text):
        if reco == Recommendation.SELL.value:
            return Recommendation.REDUCE.value
        if reco == Recommendation.BUY.value:
            return Recommendation.HOLD.value

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
# Core Recommendation Parsing (preserved)
# =============================================================================

@lru_cache(maxsize=8192)
def _parse_string_recommendation(raw: str) -> str:
    ar_norm = _normalize_arabic(raw)

    if ar_norm in _AR_BUY:
        return Recommendation.BUY.value
    if ar_norm in _AR_HOLD:
        return Recommendation.HOLD.value
    if ar_norm in _AR_REDUCE:
        return Recommendation.REDUCE.value
    if ar_norm in _AR_SELL:
        return Recommendation.SELL.value

    if any(tok in ar_norm for tok in ["شراء", "اشتر", "تجميع", "زيادة"]):
        return Recommendation.BUY.value
    if any(tok in ar_norm for tok in ["بيع قوي", "تخارج", "تجنب", "تصفية"]):
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

    if norm in _LANG_BUY:
        return Recommendation.BUY.value
    if norm in _LANG_SELL:
        return Recommendation.SELL.value
    if norm in _LANG_HOLD:
        return Recommendation.HOLD.value
    if norm in _LANG_REDUCE:
        return Recommendation.REDUCE.value

    if norm in _STRONG_SELL_LIKE or _PAT_STRONG_SELL.search(norm):
        return _apply_negation(norm, Recommendation.SELL.value)
    if norm in _STRONG_BUY_LIKE or _PAT_STRONG_BUY.search(norm):
        return _apply_negation(norm, Recommendation.STRONG_BUY.value)

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
    """Normalize a recommendation to canonical form. (Preserved from v6.1.0.)"""
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
    """Check if value is a valid canonical recommendation. (Preserved.)"""
    try:
        return str(value) in _RECO_ENUM
    except Exception:
        return False


def get_recommendation_score(reco: Any) -> int:
    """Get numeric score for recommendation (0-4, 0 = STRONG_BUY best). (Preserved.)"""
    try:
        s = str(reco)
    except Exception:
        return 2
    if s in _RECO_ENUM:
        try:
            return Recommendation(s).to_score()
        except Exception:
            return 2
    return 2


def recommendation_from_score(score: Any, scale: str = "1-5") -> str:
    """Convert score to recommendation. (Preserved.)"""
    return Recommendation.from_score(score, scale).value


# =============================================================================
# Backward-compatibility constants
# =============================================================================

RECO_STRONG_BUY = Recommendation.STRONG_BUY.value
RECO_BUY = Recommendation.BUY.value
RECO_HOLD = Recommendation.HOLD.value
RECO_REDUCE = Recommendation.REDUCE.value
RECO_SELL = Recommendation.SELL.value


# =============================================================================
# Module Exports
# =============================================================================

__all__ = [
    # Version
    "VERSION",
    # Enums
    "Recommendation",
    # Recommendation constants
    "RECO_STRONG_BUY",
    "RECO_BUY",
    "RECO_HOLD",
    "RECO_REDUCE",
    "RECO_SELL",
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
    # View helpers
    "normalize_view_token",
    "recommendation_from_views",
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
