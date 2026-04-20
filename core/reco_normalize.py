#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
core/reco_normalize.py
================================================================================
Recommendation Normalization -- v6.1.0
================================================================================

Purpose
-------
- Provide a single canonical recommendation enum across the application.
- Normalize upstream recommendation payloads without stripping advisory context.
- Preserve and standardize:
    - recommendation
    - recommendation_reason
    - horizon_days
    - invest_period_label
    - invest_period_days
    - confidence / score / source fields where present
- Remain deterministic, fast, import-safe, and exception-safe.

v6.1.0 changes (what moved from v6.0.0)
---------------------------------------
- FIX: `Recommendation.from_score` 1-5 scale band alignment. v6.0.0 used
  HOLD only for 2.5 < score <= 3.0, which meant score=3.1 → REDUCE even
  though 3.0 is the conventional analyst neutral midpoint. v6.1.0 widens
  HOLD to (2.5, 3.5], which also makes it consistent with how
  `_parse_numeric_rating` maps `int(round(num)) == 3 → HOLD` and
  `int(round(num)) == 4 → REDUCE` — previously the two code paths
  disagreed on the treatment of 3.2 (v6.0.0 from_score → REDUCE,
  v6.0.0 string-parse → HOLD).
- FIX: `Recommendation.from_score` 1-3 scale now returns REDUCE between
  HOLD and SELL bands and widens STRONG_BUY/BUY bands. v6.0.0's
  `score <= 1.2 → STRONG_BUY` was too tight to hit in practice, and the
  1-3 scale had no REDUCE bucket, forcing anything above 2.5 directly
  to SELL.
- FIX: `Recommendation.from_score` 0-100 scale now has a REDUCE bucket
  between HOLD and SELL (35 <= score < 45), matching the other scales
  that have a 5-level output range.
- ROBUSTNESS: `from_score` coerces non-numeric input to float instead
  of raising TypeError, returning HOLD on coercion failure.
- ROBUSTNESS: `get_recommendation_score` accepts any string and returns
  2 (HOLD) for invalid values rather than raising.
- LINT: removed `field` variable shadowing of `dataclasses.field` inside
  `normalize_recommendation_payload` (harmless at runtime but flagged by
  every linter).
- DOC: `__all__` ordering now places `RECO_*` constants after they are
  defined in the file, so star-imports work regardless of execution order
  during circular-import recovery.
- PRESERVES: all pattern dictionaries (English, Arabic, French, Spanish,
  German, Portuguese), all vendor field mappings, all regex patterns,
  lru_cache on `_normalize_text`/`_normalize_arabic`/`_parse_string_recommendation`,
  and the full public API surface.

Public API preserved: VERSION, Recommendation, RECO_STRONG_BUY, RECO_BUY,
RECO_HOLD, RECO_REDUCE, RECO_SELL, NormalizedRecommendation,
normalize_recommendation, normalize_recommendation_payload,
normalize_recommendation_row, normalize_recommendation_rows,
batch_normalize, is_valid_recommendation, get_recommendation_score,
recommendation_from_score, RecommendationError,
InvalidRecommendationError.
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

VERSION = "6.1.0"


# =============================================================================
# Enums
# =============================================================================

class Recommendation(str, Enum):
    """Canonical recommendation values."""
    STRONG_BUY = "STRONG_BUY"
    BUY = "BUY"
    HOLD = "HOLD"
    REDUCE = "REDUCE"
    SELL = "SELL"

    @classmethod
    def from_score(cls, score: Any, scale: str = "1-5") -> "Recommendation":
        """Convert a numeric score to a recommendation.

        Scale conventions (v6.1.0):

          1-5  (analyst convention, 1 = best):
            score <= 1.5         -> STRONG_BUY
            1.5 <  score <= 2.5  -> BUY
            2.5 <  score <= 3.5  -> HOLD     (v6.0.0 upper bound was 3.0)
            3.5 <  score <= 4.5  -> REDUCE
            score >  4.5         -> SELL

          1-3  (tiered, 1 = best):
            score <= 1.3         -> STRONG_BUY
            1.3 <  score <= 1.8  -> BUY
            1.8 <  score <= 2.3  -> HOLD
            2.3 <  score <= 2.7  -> REDUCE    (v6.0.0: missing, went straight to SELL)
            score >  2.7         -> SELL

          0-100 (score, higher = better):
            score >= 85          -> STRONG_BUY
            65 <= score <  85    -> BUY
            45 <= score <  65    -> HOLD
            35 <= score <  45    -> REDUCE    (v6.0.0: missing, went straight to SELL)
            score <  35          -> SELL
        """
        # Coerce to float safely; return HOLD on failure (v6.0.0 raised TypeError).
        try:
            if isinstance(score, bool):
                s = float(score)
            elif isinstance(score, (int, float)):
                s = float(score)
            else:
                s = float(str(score).strip().replace(",", "."))
            if s != s:  # NaN guard
                return cls.HOLD
        except (TypeError, ValueError):
            return cls.HOLD

        if scale == "1-5":
            if s <= 1.5:
                return cls.STRONG_BUY
            if s <= 2.5:
                return cls.BUY
            if s <= 3.5:  # v6.1.0: was 3.0 in v6.0.0
                return cls.HOLD
            if s <= 4.5:  # v6.1.0: was 4.0 in v6.0.0
                return cls.REDUCE
            return cls.SELL

        if scale == "1-3":
            if s <= 1.3:  # v6.1.0: was 1.2 in v6.0.0
                return cls.STRONG_BUY
            if s <= 1.8:
                return cls.BUY
            if s <= 2.3:  # v6.1.0: was 2.5 in v6.0.0
                return cls.HOLD
            if s <= 2.7:  # v6.1.0: REDUCE bucket added (absent in v6.0.0)
                return cls.REDUCE
            return cls.SELL

        if scale == "0-100":
            if s >= 85:
                return cls.STRONG_BUY
            if s >= 65:
                return cls.BUY
            if s >= 45:
                return cls.HOLD
            if s >= 35:  # v6.1.0: REDUCE bucket added (absent in v6.0.0)
                return cls.REDUCE
            return cls.SELL

        return cls.HOLD

    def to_score(self) -> int:
        """Convert recommendation to a 0-4 score (0 = best, 4 = worst)."""
        scores = {
            Recommendation.STRONG_BUY: 0,
            Recommendation.BUY: 1,
            Recommendation.HOLD: 2,
            Recommendation.REDUCE: 3,
            Recommendation.SELL: 4,
        }
        return scores.get(self, 2)

    @classmethod
    def is_valid(cls, value: str) -> bool:
        """Check if value is a valid recommendation."""
        return value in cls._value2member_map_


# =============================================================================
# Constants
# =============================================================================

_RECO_ENUM = frozenset({r.value for r in Recommendation})

_CONFIDENCE_HIGH = 0.8
_CONFIDENCE_MEDIUM = 0.5

# Null-ish / no-rating markers
_NO_RATING: Set[str] = {
    "NA", "N/A", "NONE", "NULL", "UNKNOWN", "UNRATED", "NOT RATED", "NO RATING",
    "NR", "N R", "SUSPENDED", "UNDER REVIEW", "NOT COVERED", "NO COVERAGE",
    "NOT APPLICABLE", "NIL", "—", "-", "WAITING", "PENDING", "TBA", "TBD",
    "NO RECOMMENDATION", "RATING WITHDRAWN", "WITHDRAWN", "NOT RANKED",
}

# Exact phrase mappings
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

# Arabic
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

# French
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

# Spanish
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

# German
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

# Portuguese
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

# Combined language sets
_LANG_BUY = _FR_BUY | _ES_BUY | _DE_BUY | _PT_BUY
_LANG_HOLD = _FR_HOLD | _ES_HOLD | _DE_HOLD | _PT_HOLD
_LANG_REDUCE = _FR_REDUCE | _ES_REDUCE | _DE_REDUCE | _PT_REDUCE
_LANG_SELL = _FR_SELL | _ES_SELL | _DE_SELL | _PT_SELL

# Vendor field mappings
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

# Context-preservation fields
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

# Compiled regex patterns
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
    """Normalized recommendation with context."""
    recommendation: str = Recommendation.HOLD.value
    recommendation_reason: Optional[str] = None
    horizon_days: Optional[int] = None
    invest_period_days: Optional[int] = None
    invest_period_label: Optional[str] = None
    confidence: Optional[float] = None
    scores: Dict[str, float] = dc_field(default_factory=dict)
    source_fields: Dict[str, Any] = dc_field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
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
        """Create from dictionary."""
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
# Pure Utility Functions
# =============================================================================

def _safe_str(value: Any) -> str:
    """Safely convert to string."""
    try:
        return "" if value is None else str(value).strip()
    except Exception:
        return ""


def _clean_text(value: Any) -> str:
    """Clean text by removing extra whitespace."""
    s = _safe_str(value)
    if not s:
        return ""
    return " ".join(s.split()).strip(" |;,-.")


def _first_non_empty_str(items: Iterable[Any]) -> Optional[str]:
    """Return first non-empty string from items."""
    for item in items:
        s = _safe_str(item)
        if s and s.lower() not in ("none", "null", "n/a", ""):
            return s
    return None


def _to_float(value: Any) -> Optional[float]:
    """Safely convert to float."""
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
    """Safely convert to integer."""
    f = _to_float(value)
    return int(f) if f is not None else None


def _as_ratio(value: Any) -> Optional[float]:
    """Convert percent-like value to ratio."""
    f = _to_float(value)
    if f is None:
        return None
    if abs(f) > 1.5:
        return f / 100.0
    return f


def _deep_get(data: Dict[str, Any], keys: Sequence[str]) -> Optional[Any]:
    """Deep get from nested dictionary."""
    for key in keys:
        if not key:
            continue

        # Direct key
        if key in data:
            value = data.get(key)
            if value is not None and _safe_str(value):
                return value

        # Nested path
        if "." in key:
            current: Any = data
            parts = key.split(".")
            valid = True

            for part in parts:
                # Handle array indexing (e.g., "items[0]")
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
    """Extract recommendation candidate from various formats."""
    if value is None:
        return None

    # Direct enum match
    if isinstance(value, str) and value.upper() in _RECO_ENUM:
        return value.upper()

    # Numeric
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        return value

    # Dictionary
    if isinstance(value, dict):
        for _, fields in _VENDOR_FIELDS.items():
            v = _deep_get(value, fields)
            if v is not None and _safe_str(v):
                return v

        # Check for rating-like keys
        for key in value.keys():
            if isinstance(key, str) and any(h in key.lower() for h in ["reco", "rating", "rank", "score", "consensus"]):
                v = value[key]
                if v is not None and _safe_str(v):
                    return v

        return _first_non_empty_str(value.values())

    # List or tuple
    if isinstance(value, (list, tuple)):
        if value and isinstance(value[0], dict):
            for item in value:
                extracted = _extract_candidate(item)
                if extracted is not None:
                    return extracted
        return _first_non_empty_str(value)

    return value


# =============================================================================
# Arabic Normalization
# =============================================================================

@lru_cache(maxsize=8192)
def _normalize_arabic(text: str) -> str:
    """Normalize Arabic text."""
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
    """Normalize text for pattern matching."""
    if not text:
        return ""

    s = text.upper()
    s = _PAT_SEP.sub(" ", s)
    s = _PAT_SPLIT.sub(" ", s)
    s = _PAT_PUNCT.sub(" ", s)
    s = _PAT_WS.sub(" ", s).strip()
    return s


# =============================================================================
# Numeric Rating Parsing
# =============================================================================

def _parse_numeric_rating(text: str, context_hints: bool = True) -> Optional[Tuple[str, float]]:
    """Parse numeric rating (e.g., 4/5, 3 stars)."""
    if not text:
        return None

    su = text.upper()

    # Skip confidence scores
    if _PAT_CONFIDENCE.search(su) and not _PAT_RATING_HINT.search(su):
        return None

    # Ratio pattern (e.g., 4/5, 8 out of 10)
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

    # Single number
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

    # 1-5 scale
    if 1.0 <= num <= 5.0 and (has_hint or scale_5):
        n = int(round(num))
        if n <= 2:
            return (Recommendation.BUY.value, 0.85)
        if n == 3:
            return (Recommendation.HOLD.value, 0.85)
        if n == 4:
            return (Recommendation.REDUCE.value, 0.85)
        return (Recommendation.SELL.value, 0.85)

    # 1-3 scale
    if 1.0 <= num <= 3.0 and (has_hint or scale_3):
        n = int(round(num))
        if n == 1:
            return (Recommendation.BUY.value, 0.8)
        if n == 2:
            return (Recommendation.HOLD.value, 0.8)
        return (Recommendation.SELL.value, 0.8)

    # 0-100 scale
    if 0.0 <= num <= 100.0 and has_hint:
        if num >= 70.0:
            return (Recommendation.BUY.value, 0.75)
        if num >= 45.0:
            return (Recommendation.HOLD.value, 0.6)
        return (Recommendation.SELL.value, 0.75)

    # 0-1 scale
    if 0.0 <= num <= 1.0 and has_hint:
        if num >= 0.7:
            return (Recommendation.BUY.value, 0.5)
        if num >= 0.45:
            return (Recommendation.HOLD.value, 0.4)
        return (Recommendation.SELL.value, 0.5)

    return None


def _apply_negation(text: str, reco: str) -> str:
    """Apply negation to recommendation."""
    if not text or not reco:
        return reco

    # Context-based negation
    if _PAT_NEGATION_CONTEXT.search(text):
        return Recommendation.HOLD.value

    # Simple negation
    if _PAT_NEGATE.search(text):
        if reco == Recommendation.SELL.value:
            return Recommendation.REDUCE.value
        if reco == Recommendation.BUY.value:
            return Recommendation.HOLD.value

    return reco


def _sentiment_analysis(text: str) -> Optional[str]:
    """Basic sentiment analysis fallback."""
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
# Core Recommendation Parsing
# =============================================================================

@lru_cache(maxsize=8192)
def _parse_string_recommendation(raw: str) -> str:
    """Parse recommendation from string."""
    # Arabic
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

    # Normalized text
    norm = _normalize_text(raw)
    if not norm:
        return Recommendation.HOLD.value

    # No rating markers
    if norm in _NO_RATING:
        return Recommendation.HOLD.value

    # Language-specific
    if norm in _LANG_BUY:
        return Recommendation.BUY.value
    if norm in _LANG_SELL:
        return Recommendation.SELL.value
    if norm in _LANG_HOLD:
        return Recommendation.HOLD.value
    if norm in _LANG_REDUCE:
        return Recommendation.REDUCE.value

    # Strong patterns
    if norm in _STRONG_SELL_LIKE or _PAT_STRONG_SELL.search(norm):
        return _apply_negation(norm, Recommendation.SELL.value)
    if norm in _STRONG_BUY_LIKE or _PAT_STRONG_BUY.search(norm):
        return _apply_negation(norm, Recommendation.STRONG_BUY.value)

    # Standard patterns
    if norm in _SELL_LIKE or _PAT_SELL.search(norm):
        return _apply_negation(norm, Recommendation.SELL.value)
    if norm in _REDUCE_LIKE or _PAT_REDUCE.search(norm):
        return _apply_negation(norm, Recommendation.REDUCE.value)
    if norm in _HOLD_LIKE or _PAT_HOLD.search(norm):
        return _apply_negation(norm, Recommendation.HOLD.value)
    if norm in _BUY_LIKE or _PAT_BUY.search(norm):
        return _apply_negation(norm, Recommendation.BUY.value)

    # Numeric rating
    num_result = _parse_numeric_rating(norm, context_hints=True)
    if num_result and num_result[1] >= _CONFIDENCE_MEDIUM:
        return _apply_negation(norm, num_result[0])

    # Fallback patterns
    if _PAT_SELL.search(norm):
        return _apply_negation(norm, Recommendation.SELL.value)
    if _PAT_REDUCE.search(norm):
        return _apply_negation(norm, Recommendation.REDUCE.value)
    if _PAT_HOLD.search(norm):
        return _apply_negation(norm, Recommendation.HOLD.value)
    if _PAT_BUY.search(norm):
        return _apply_negation(norm, Recommendation.BUY.value)

    # Sentiment analysis
    sentiment = _sentiment_analysis(norm)
    if sentiment:
        return sentiment

    return Recommendation.HOLD.value


# =============================================================================
# Horizon Helpers
# =============================================================================

def _horizon_label_from_days(days: Optional[int]) -> Optional[str]:
    """Convert days to horizon label."""
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
    """Infer horizon days from payload."""
    # Direct fields
    value = _deep_get(data, _HORIZON_DAYS_FIELDS)
    if value is not None:
        di = _to_int(value)
        if di is not None and di > 0:
            return di

    # From label
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

    # From ROI fields
    if _deep_get(data, ["required_roi_1m"]) is not None:
        return 30
    if _deep_get(data, ["required_roi_3m"]) is not None:
        return 90
    if _deep_get(data, ["required_roi_12m"]) is not None:
        return 365

    return None


def _infer_horizon_label_from_payload(data: Dict[str, Any], horizon_days: Optional[int]) -> Optional[str]:
    """Infer horizon label from payload."""
    label = _clean_text(_deep_get(data, _HORIZON_LABEL_FIELDS)).upper()
    if label:
        return label
    return _horizon_label_from_days(horizon_days)


# =============================================================================
# Public API
# =============================================================================

def normalize_recommendation(value: Any) -> str:
    """
    Normalize a recommendation to canonical form.

    Args:
        value: Raw recommendation value (string, number, dict, etc.)

    Returns:
        Canonical recommendation: STRONG_BUY, BUY, HOLD, REDUCE, or SELL

    Examples:
        >>> normalize_recommendation("Strong Buy")
        'STRONG_BUY'
        >>> normalize_recommendation(4)
        'REDUCE'
        >>> normalize_recommendation({"rating": "Outperform"})
        'BUY'
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
    """
    Normalize a recommendation payload without stripping context.

    Args:
        data: Raw payload (dict, string, or other)

    Returns:
        Normalized dict with recommendation, reason, horizon, etc.

    Examples:
        >>> normalize_recommendation_payload({
        ...     "rating": "Buy",
        ...     "recommendation_reason": "Strong fundamentals",
        ...     "horizon_days": 90
        ... })
        {
            'recommendation': 'BUY',
            'recommendation_reason': 'Strong fundamentals',
            'horizon_days': 90,
            'invest_period_days': 90,
            'invest_period_label': '3M',
            'confidence': None,
            'scores': {},
            'source_fields': {}
        }
    """
    try:
        if isinstance(data, dict):
            raw = dict(data)
        else:
            raw = {"recommendation": data}

        # Extract recommendation
        reco_value = _deep_get(raw, _VENDOR_FIELDS["default"] + _VENDOR_FIELDS["nested"])
        if reco_value is None:
            reco_value = raw.get("recommendation")
        recommendation = normalize_recommendation(reco_value if reco_value is not None else raw)

        # Extract reason
        reason = _clean_text(_deep_get(raw, _REASON_FIELDS))
        if not reason:
            reason = _clean_text(raw.get("recommendation_reason"))

        # Extract horizon days
        horizon_days = _infer_horizon_days_from_payload(raw)
        if horizon_days is None:
            horizon_days = _to_int(raw.get("horizon_days")) or _to_int(raw.get("invest_period_days"))

        # Extract horizon label
        invest_period_label = _infer_horizon_label_from_payload(raw, horizon_days)
        if not invest_period_label:
            invest_period_label = _clean_text(raw.get("invest_period_label")).upper() or None

        # Extract confidence
        # v6.1.0: local loop var renamed from `field` to avoid shadowing
        # `dataclasses.field` at function scope (lint-only; no runtime change).
        confidence = None
        for conf_field in _CONFIDENCE_FIELDS:
            val = _deep_get(raw, [conf_field])
            if val is not None:
                confidence = _as_ratio(val)
                if confidence is not None:
                    break

        # Extract scores
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
    """
    Normalize a recommendation row.

    Alias for normalize_recommendation_payload for row-level normalization.
    """
    return normalize_recommendation_payload(row)


def normalize_recommendation_rows(rows: Sequence[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Normalize multiple recommendation rows.

    Args:
        rows: List of row dicts

    Returns:
        List of normalized row dicts
    """
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
    """
    Normalize a batch of recommendations.

    Args:
        recommendations: List of raw recommendation values

    Returns:
        List of canonical recommendations
    """
    return [normalize_recommendation(r) for r in recommendations]


def is_valid_recommendation(value: Any) -> bool:
    """Check if value is a valid canonical recommendation."""
    try:
        return str(value) in _RECO_ENUM
    except Exception:
        return False


def get_recommendation_score(reco: Any) -> int:
    """Get numeric score for recommendation (0-4, 0 = STRONG_BUY best).

    v6.1.0: accepts Any and returns 2 (HOLD) for invalid or non-string
    inputs rather than raising.
    """
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
    """Convert score to recommendation."""
    return Recommendation.from_score(score, scale).value


# =============================================================================
# Backward compatibility constants
# =============================================================================
# (Defined BEFORE __all__ so star-imports work consistently regardless of
# whether another module reloads this module partway through initialization.)

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
    # Constants
    "RECO_STRONG_BUY",
    "RECO_BUY",
    "RECO_HOLD",
    "RECO_REDUCE",
    "RECO_SELL",
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
