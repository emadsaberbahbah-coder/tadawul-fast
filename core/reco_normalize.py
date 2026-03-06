#!/usr/bin/env python3
"""
core/reco_normalize.py
------------------------------------------------------------
Recommendation Normalization — v5.0.0
------------------------------------------------------------

Purpose
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

Canonical outputs
- BUY / HOLD / REDUCE / SELL

What changed in v5.0.0
- ✅ Keeps legacy normalize_recommendation(x) behavior
- ✅ Adds schema-safe row/payload normalization helpers
- ✅ Preserves recommendation_reason instead of dropping it
- ✅ Preserves and infers horizon_days / invest_period_label
- ✅ Supports dict rows, nested payloads, and mixed upstream formats
- ✅ Adds helper to normalize full advisor rows/canonical records
- ✅ Never throws exceptions by design

Notes
- Conservative by default: unknown/ambiguous => HOLD
- REDUCE = partial sell / lighten / take profit
- SELL = explicit exit / avoid / liquidation
"""

from __future__ import annotations

import re
from functools import lru_cache
from typing import Any, Dict, Iterable, List, Optional, Sequence, Set, Tuple, Union

VERSION = "5.0.0"

# ---------------------------------------------------------------------
# Canonical recommendation enum
# ---------------------------------------------------------------------
RECO_BUY = "BUY"
RECO_HOLD = "HOLD"
RECO_REDUCE = "REDUCE"
RECO_SELL = "SELL"
_RECO_ENUM = frozenset({RECO_BUY, RECO_HOLD, RECO_REDUCE, RECO_SELL})

_CONFIDENCE_HIGH = 0.8
_CONFIDENCE_MEDIUM = 0.5

# ---------------------------------------------------------------------
# Null-ish / no-rating markers
# ---------------------------------------------------------------------
_NO_RATING: Set[str] = {
    "NA", "N/A", "NONE", "NULL", "UNKNOWN", "UNRATED", "NOT RATED", "NO RATING",
    "NR", "N R", "SUSPENDED", "UNDER REVIEW", "NOT COVERED", "NO COVERAGE",
    "NOT APPLICABLE", "NIL", "—", "-", "WAITING", "PENDING", "TBA", "TBD",
    "NO RECOMMENDATION", "RATING WITHDRAWN", "WITHDRAWN", "NOT RANKED"
}

# ---------------------------------------------------------------------
# Exact phrase mappings
# ---------------------------------------------------------------------
_STRONG_BUY_LIKE: Set[str] = {
    "STRONG BUY", "CONVICTION BUY", "HIGH CONVICTION BUY", "BUY STRONG",
    "TOP PICK", "BEST IDEA", "HIGHLY RECOMMENDED BUY", "BUY (STRONG)",
    "STRONG BUY RECOMMENDATION", "MUST BUY", "STRONG ACCUMULATE", "FOCUS BUY"
}

_STRONG_SELL_LIKE: Set[str] = {
    "STRONG SELL", "SELL (STRONG)", "SELL STRONG", "EXIT IMMEDIATELY",
    "STRONG SELL RECOMMENDATION", "MUST SELL", "URGENT SELL", "FORCED EXIT",
    "LIQUIDATE ALL"
}

_BUY_LIKE: Set[str] = {
    "BUY", "ACCUMULATE", "ADD", "OUTPERFORM", "MARKET OUTPERFORM",
    "SECTOR OUTPERFORM", "OVERWEIGHT", "INCREASE", "UPGRADE", "LONG",
    "BULLISH", "POSITIVE", "BUYING", "SPECULATIVE BUY", "RECOMMENDED BUY",
    "BUY ON DIPS", "BUY RECOMMENDATION", "ACCUMULATION", "BUILT", "POSITION"
}

_HOLD_LIKE: Set[str] = {
    "HOLD", "NEUTRAL", "MAINTAIN", "UNCHANGED", "MARKET PERFORM", "SECTOR PERFORM",
    "IN LINE", "EQUAL WEIGHT", "EQUALWEIGHT", "FAIR VALUE", "FAIRLY VALUED",
    "WAIT", "KEEP", "STABLE", "WATCH", "MONITOR", "NO ACTION", "PERFORM",
    "HOLD RECOMMENDATION", "HOLDING", "KEEP POSITION", "NO CHANGE"
}

_REDUCE_LIKE: Set[str] = {
    "REDUCE", "TRIM", "LIGHTEN", "PARTIAL SELL", "TAKE PROFIT", "TAKE PROFITS",
    "UNDERWEIGHT", "DECREASE", "WEAKEN", "CAUTIOUS", "PROFIT TAKING",
    "SELL STRENGTH", "TRIM POSITION", "REDUCE EXPOSURE", "PARTIAL EXIT",
    "REDUCE WEIGHT", "CUT POSITION", "LESSEN", "SCALE BACK"
}

_SELL_LIKE: Set[str] = {
    "SELL", "EXIT", "AVOID", "UNDERPERFORM", "MARKET UNDERPERFORM",
    "SECTOR UNDERPERFORM", "SHORT", "BEARISH", "NEGATIVE", "SELLING",
    "DOWNGRADE", "RED FLAG", "LIQUIDATE", "CLOSE POSITION", "UNWIND",
    "SELL RECOMMENDATION", "AVOIDANCE", "EXIT POSITION"
}

# Arabic
_AR_BUY: Set[str] = {
    "شراء", "اشتر", "اشترى", "اشترِ", "تجميع", "زيادة", "ايجابي", "ايجابى",
    "فرصة شراء", "اداء متفوق", "زيادة المراكز", "توصية شراء", "توصية بالشراء",
    "شراء قوي", "شراء قوى", "احتفاظ مع ميل للشراء", "زيادة التعرض", "تعزيز المراكز",
    "شراء مكثف", "تراكم", "مضاعفة", "تفوق", "متفوق", "زيادة وزن", "فرصة استثمارية"
}

_AR_HOLD: Set[str] = {
    "احتفاظ", "محايد", "انتظار", "مراقبة", "ثبات", "استقرار", "تمسك", "اداء محايد",
    "حياد", "موقف محايد", "ابقاء المراكز", "متوازن", "بدون تغيير", "محافظة",
    "استمرارية", "لا تغيير", "محايد تماما", "وزن مساوي"
}

_AR_REDUCE: Set[str] = {
    "تقليص", "تخفيف", "جني ارباح", "جني ارباح جزئي", "تقليل", "تخفيض",
    "اداء ضعيف", "تخفيض المراكز", "بيع جزئي", "تخفيف المراكز",
    "تقليل التعرض", "تخفيف المخاطر", "تسييل جزئي", "اخذ ارباح", "خفف"
}

_AR_SELL: Set[str] = {
    "بيع", "تخارج", "سلبي", "سلبى", "تجنب", "خروج", "بيع قوي", "بيع قوى",
    "توصية بيع", "توصية بالبيع", "اداء اقل", "اداء أقل", "تصفية", "الخروج من السهم",
    "بيع كلي", "تسييل", "تخلص من", "تجنب تماما"
}

# FR / ES / DE / PT
_FR_BUY: Set[str] = {
    "ACHETER", "ACHAT", "ACCUMULER", "RENFORCER", "SURPERFORMER", "SURPERFORMANCE",
    "POSITIF", "BULLISH", "RECOMMANDATION D'ACHAT", "ACHAT FORT", "ACHAT SOLIDE",
    "ACCUMULATION", "AJOUTER", "SURPONDÉRER", "HAUSSIER"
}
_FR_HOLD: Set[str] = {
    "CONSERVER", "GARDER", "NEUTRE", "MAINTENIR", "STABLE", "EN LIGNE",
    "PERFORMANCE DU MARCHÉ", "ATTENDRE", "SURVEILLER", "PAS DE CHANGEMENT",
    "CONSERVATION", "POSITION NEUTRE", "À CONSERVER"
}
_FR_REDUCE: Set[str] = {
    "RÉDUIRE", "ALLEGER", "PRENDRE DES BÉNÉFICES", "PRISE DE BÉNÉFICES",
    "SOUS-PONDÉRER", "DIMINUER", "ATTÉNUER", "RÉDUCTION", "PRUDENCE",
    "VENDRE PARTIELLEMENT", "ALLÉGEMENT"
}
_FR_SELL: Set[str] = {
    "VENDRE", "CÉDER", "ÉVITER", "SOUS-PERFORMER", "SOUS-PERFORMANCE",
    "BAISSIER", "NÉGATIF", "SORTIR", "LIQUIDER", "VENDRE FORT",
    "RECOMMANDATION DE VENTE", "VENTE", "DÉGRADER"
}

_ES_BUY: Set[str] = {
    "COMPRAR", "ACUMULAR", "SOBREPONDERAR", "SOBRERENDIMIENTO", "POSITIVO",
    "ALCISTA", "COMPRA", "RECOMENDACIÓN DE COMPRA", "COMPRA FUERTE",
    "AÑADIR", "INCREMENTAR", "SUPERAR", "OUTPERFORM"
}
_ES_HOLD: Set[str] = {
    "MANTENER", "CONSERVAR", "NEUTRAL", "NEUTRO", "ESPERAR", "MONITOREAR",
    "RENDIMIENTO DE MERCADO", "SIN CAMBIOS", "ESTABLE", "EN LÍNEA",
    "POSICIÓN NEUTRA", "MANTENIMIENTO"
}
_ES_REDUCE: Set[str] = {
    "REDUCIR", "ALIGERAR", "TOMAR GANANCIAS", "TOMA DE GANANCIAS",
    "INFRAPONDERAR", "DISMINUIR", "PRECAUCIÓN", "VENTA PARCIAL",
    "RECORTAR", "DISMINUCIÓN"
}
_ES_SELL: Set[str] = {
    "VENDER", "EVITAR", "BAJISTA", "NEGATIVO", "SALIR", "LIQUIDAR",
    "BAJO RENDIMIENTO", "RENDIMIENTO INFERIOR", "VENTA", "DESHACERSE",
    "RECOMENDACIÓN DE VENTA", "VENTA FUERTE"
}

_DE_BUY: Set[str] = {
    "KAUFEN", "KAUF", "AKKUMULIEREN", "AUFSTOCKEN", "ÜBERGEWICHTEN",
    "OUTPERFORM", "POSITIV", "BULLISH", "KAUFEMPFEHLUNG", "STARKER KAUF",
    "HINZUFÜGEN", "ERHÖHEN", "ÜBERTREFFEN"
}
_DE_HOLD: Set[str] = {
    "HALTEN", "BEHALTEN", "NEUTRAL", "MARKTGEWICHT", "ABWARTEN", "BEOBACHTEN",
    "STABIL", "UNVERÄNDERT", "IN LINE", "MARKTPERFORMANCE", "HALTEEMPFEHLUNG"
}
_DE_REDUCE: Set[str] = {
    "REDUZIEREN", "VERRINGERN", "GEWINNE MITNEHMEN", "GEWINN MITNAHME",
    "UNTERGEWICHTEN", "ABBAUEN", "VORSICHT", "TEILVERKAUF", "KÜRZEN",
    "VERMINDERN", "ZURÜCKFAHREN"
}
_DE_SELL: Set[str] = {
    "VERKAUFEN", "VERKAUF", "VERMEIDEN", "UNTERPERFORM", "BAISSIST", "NEGATIV",
    "AUSSTEIGEN", "LIQUIDIEREN", "VERKAUFSEMPFEHLUNG", "STARKER VERKAUF",
    "ABSTOSSEN", "REDUZIEREN AUF NULL"
}

_PT_BUY: Set[str] = {
    "COMPRAR", "ACUMULAR", "SOBREPONDERAR", "OUTPERFORM", "POSITIVO",
    "ALCISTA", "COMPRA", "RECOMENDAÇÃO DE COMPRA", "COMPRA FORTE",
    "ADICIONAR", "AUMENTAR", "SUPERAR", "ACUMULAÇÃO"
}
_PT_HOLD: Set[str] = {
    "MANTER", "CONSERVAR", "NEUTRO", "AGUARDAR", "MONITORAR",
    "DESEMPENHO DE MERCADO", "SEM ALTERAÇÕES", "ESTÁVEL", "EM LINHA",
    "POSIÇÃO NEUTRA", "MANUTENÇÃO"
}
_PT_REDUCE: Set[str] = {
    "REDUZIR", "ALIVIAR", "OBTER LUCROS", "OBTER LUCROS PARCIAIS",
    "SUB-PONDERAR", "DIMINUIR", "PRUDÊNCIA", "VENDA PARCIAL",
    "CORTAR", "REDUÇÃO", "COLHER LUCROS"
}
_PT_SELL: Set[str] = {
    "VENDER", "EVITAR", "BAIXISTA", "NEGATIVO", "SAIR", "LIQUIDAR",
    "DESEMPENHO INFERIOR", "SUBPERFORM", "VENDA", "LIVRAR-SE",
    "RECOMENDAÇÃO DE VENDA", "VENDA FORTE", "LIQUIDAÇÃO"
}

_LANG_BUY = _FR_BUY | _ES_BUY | _DE_BUY | _PT_BUY
_LANG_HOLD = _FR_HOLD | _ES_HOLD | _DE_HOLD | _PT_HOLD
_LANG_REDUCE = _FR_REDUCE | _ES_REDUCE | _DE_REDUCE | _PT_REDUCE
_LANG_SELL = _FR_SELL | _ES_SELL | _DE_SELL | _PT_SELL

# ---------------------------------------------------------------------
# Vendor-ish field maps
# ---------------------------------------------------------------------
_VENDOR_FIELDS: Dict[str, List[str]] = {
    "default": [
        "recommendation", "reco", "rating", "action", "signal",
        "analyst_rating", "analystRecommendation", "analyst_recommendation",
        "consensus", "consensus_rating"
    ],
    "nested": [
        "meta.recommendation", "meta.rating", "data.recommendation",
        "result.recommendation", "payload.recommendation",
        "recommendation.value", "rating.value", "analyst.rating"
    ],
    "bloomberg": [
        "analyst_rating", "recommendation_mean", "analyst_consensus",
        "rating_trend", "analyst_summary.recommendation"
    ],
    "yahoo": [
        "recommendationKey", "recommendationMean", "analystRating",
        "targetConsensus", "rating"
    ],
    "reuters": [
        "analystRecommendations", "streetAccount.analystRating",
        "REC", "REC_AVG", "analystRatings.recommendation"
    ],
    "morningstar": [
        "starRating", "analystRating", "ratingSummary.recommendation"
    ],
    "zacks": [
        "zacksRank", "zacksRankText", "zacks_rank", "rank"
    ],
    "tipranks": [
        "consensus", "analystConsensus", "tipranksConsensus"
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

# ---------------------------------------------------------------------
# Compiled regex
# ---------------------------------------------------------------------
_PAT_NEGATE = re.compile(r"\b(?:NOT|NO|NEVER|WITHOUT|HARDLY|RARELY|UNLIKELY|AVOID|AGAINST)\b", re.IGNORECASE)
_PAT_NEGATION_CONTEXT = re.compile(r"(?:NOT|NO|NEVER)\s+(?:A\s+|AN\s+)?(STRONG\s+)?(BUY|SELL|HOLD|REDUCE)", re.IGNORECASE)

_PAT_STRONG_BUY = re.compile(r"\b(?:STRONG\s+BUY|CONVICTION\s+BUY|TOP\s+PICK|BEST\s+IDEA|MUST\s+BUY|FOCUS\s+BUY)\b", re.IGNORECASE)
_PAT_STRONG_SELL = re.compile(r"\b(?:STRONG\s+SELL|EXIT\s+IMMEDIATELY|MUST\s+SELL|URGENT\s+SELL)\b", re.IGNORECASE)

_PAT_SELL = re.compile(r"\b(?:SELL|EXIT|AVOID|UNDERPERFORM|SHORT|LIQUIDATE|CLOSE\s+POSITION|UNWIND)\b", re.IGNORECASE)
_PAT_REDUCE = re.compile(r"\b(?:REDUCE|TRIM|LIGHTEN|TAKE\s+PROFIT|TAKE\s+PROFITS|UNDERWEIGHT|DECREASE|CUT\s+POSITION|SCALE\s+BACK)\b", re.IGNORECASE)
_PAT_HOLD = re.compile(r"\b(?:HOLD|NEUTRAL|MAINTAIN|EQUAL\s+WEIGHT|MARKET\s+PERFORM|SECTOR\s+PERFORM|IN\s+LINE|FAIR\s+VALUE|WAIT|WATCH|MONITOR)\b", re.IGNORECASE)
_PAT_BUY = re.compile(r"\b(?:BUY|ACCUMULATE|ADD|OUTPERFORM|OVERWEIGHT|UPGRADE|LONG|BULLISH|POSITIVE)\b", re.IGNORECASE)

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

# ---------------------------------------------------------------------
# Safe helpers
# ---------------------------------------------------------------------
def _safe_str(x: Any) -> str:
    try:
        return "" if x is None else str(x).strip()
    except Exception:
        return ""


def _clean_text(x: Any) -> str:
    s = _safe_str(x)
    if not s:
        return ""
    return " ".join(s.split()).strip(" |;,-.")


def _first_non_empty_str(items: Iterable[Any]) -> Optional[str]:
    for it in items:
        s = _safe_str(it)
        if s and s.lower() not in ("none", "null", "n/a", ""):
            return s
    return None


def _to_float(x: Any) -> Optional[float]:
    try:
        if x is None:
            return None
        if isinstance(x, (int, float)) and not isinstance(x, bool):
            f = float(x)
        else:
            s = str(x).strip().replace(",", "")
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


def _to_int(x: Any) -> Optional[int]:
    f = _to_float(x)
    return int(f) if f is not None else None


def _as_ratio(x: Any) -> Optional[float]:
    f = _to_float(x)
    if f is None:
        return None
    if abs(f) > 1.5:
        return f / 100.0
    return f


def _deep_get(d: Dict[str, Any], keys: Sequence[str]) -> Optional[Any]:
    for key in keys:
        if not key:
            continue
        if key in d:
            v = d.get(key)
            if v is not None and _safe_str(v):
                return v

        if "." in key:
            current: Any = d
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


def _extract_candidate(x: Any) -> Optional[Any]:
    if x is None:
        return None

    if isinstance(x, str) and x.upper() in _RECO_ENUM:
        return x.upper()

    if isinstance(x, (int, float)) and not isinstance(x, bool):
        return x

    if isinstance(x, dict):
        for _, fields in _VENDOR_FIELDS.items():
            v = _deep_get(x, fields)
            if v is not None and _safe_str(v):
                return v

        for key in x.keys():
            if isinstance(key, str) and any(h in key.lower() for h in ["reco", "rating", "rank", "score", "consensus"]):
                v = x[key]
                if v is not None and _safe_str(v):
                    return v

        return _first_non_empty_str(x.values())

    if isinstance(x, (list, tuple)):
        if x and isinstance(x[0], dict):
            for item in x:
                extracted = _extract_candidate(item)
                if extracted is not None:
                    return extracted
        return _first_non_empty_str(x)

    return x


@lru_cache(maxsize=8192)
def _normalize_arabic(s: str) -> str:
    if not s:
        return ""
    s = _PAT_WS.sub(" ", s).strip()
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
def _normalize_text(s: str) -> str:
    if not s:
        return ""
    su = s.upper()
    su = _PAT_SEP.sub(" ", su)
    su = _PAT_SPLIT.sub(" ", su)
    su = _PAT_PUNCT.sub(" ", su)
    su = _PAT_WS.sub(" ", su).strip()
    return su


def _parse_numeric_rating(s: str, context_hints: bool = True) -> Optional[Tuple[str, float]]:
    if not s:
        return None

    su = s.upper()

    if _PAT_CONFIDENCE.search(su) and not _PAT_RATING_HINT.search(su):
        return None

    mratio = _PAT_RATIO.search(su)
    if mratio:
        try:
            x = float(mratio.group(1).replace(",", "."))
            y = float(mratio.group(2).replace(",", "."))

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
                    return (RECO_BUY, 0.9)
                if normalized <= 3.0:
                    return (RECO_HOLD, 0.7)
                if normalized <= 4.0:
                    return (RECO_REDUCE, 0.7)
                return (RECO_SELL, 0.9)
        except Exception:
            pass

    mnum = _PAT_NUMBER.search(su)
    if not mnum:
        return None

    try:
        num = float(mnum.group(1).replace(",", "."))
    except Exception:
        return None

    scale_5 = _PAT_SCALE_5.search(su) or "STAR" in su or "STARS" in su
    scale_3 = _PAT_SCALE_3.search(su)
    has_hint = _PAT_RATING_HINT.search(su) if context_hints else True

    if 1.0 <= num <= 5.0 and (has_hint or scale_5):
        n = int(round(num))
        if n <= 2:
            return (RECO_BUY, 0.85)
        if n == 3:
            return (RECO_HOLD, 0.85)
        if n == 4:
            return (RECO_REDUCE, 0.85)
        return (RECO_SELL, 0.85)

    if 1.0 <= num <= 3.0 and (has_hint or scale_3):
        n = int(round(num))
        if n == 1:
            return (RECO_BUY, 0.8)
        if n == 2:
            return (RECO_HOLD, 0.8)
        return (RECO_SELL, 0.8)

    if 0.0 <= num <= 100.0 and has_hint:
        if num >= 70.0:
            return (RECO_BUY, 0.75)
        if num >= 45.0:
            return (RECO_HOLD, 0.6)
        return (RECO_SELL, 0.75)

    if 0.0 <= num <= 1.0 and has_hint:
        if num >= 0.7:
            return (RECO_BUY, 0.5)
        if num >= 0.45:
            return (RECO_HOLD, 0.4)
        return (RECO_SELL, 0.5)

    return None


def _apply_negation(su: str, reco: str) -> str:
    if not su or not reco:
        return reco

    if _PAT_NEGATION_CONTEXT.search(su):
        return RECO_HOLD

    if _PAT_NEGATE.search(su):
        if reco == RECO_SELL:
            return RECO_REDUCE
        if reco == RECO_BUY:
            return RECO_HOLD

    return reco


def _sentiment_analysis(su: str) -> Optional[str]:
    if not su:
        return None

    positive_terms = ["BULLISH", "POSITIVE", "OPTIMISTIC", "UP", "UPSIDE", "GAIN"]
    negative_terms = ["BEARISH", "NEGATIVE", "PESSIMISTIC", "DOWN", "DOWNSIDE", "LOSS"]

    pos_count = sum(1 for term in positive_terms if term in su)
    neg_count = sum(1 for term in negative_terms if term in su)

    if pos_count > neg_count + 1:
        return RECO_BUY
    if neg_count > pos_count + 1:
        return RECO_SELL
    return None


@lru_cache(maxsize=8192)
def _parse_string_recommendation(s_raw: str) -> str:
    s_ar = _normalize_arabic(s_raw)

    if s_ar in _AR_BUY:
        return RECO_BUY
    if s_ar in _AR_HOLD:
        return RECO_HOLD
    if s_ar in _AR_REDUCE:
        return RECO_REDUCE
    if s_ar in _AR_SELL:
        return RECO_SELL

    if any(tok in s_ar for tok in ["شراء", "اشتر", "تجميع", "زيادة"]):
        return RECO_BUY
    if any(tok in s_ar for tok in ["بيع قوي", "تخارج", "تجنب", "تصفية"]):
        return RECO_SELL
    if any(tok in s_ar for tok in ["جني ارباح", "تخفيف", "تقليص"]):
        return RECO_REDUCE
    if any(tok in s_ar for tok in ["احتفاظ", "محايد", "انتظار", "مراقبة"]):
        return RECO_HOLD

    su = _normalize_text(s_raw)
    if not su:
        return RECO_HOLD

    if su in _NO_RATING:
        return RECO_HOLD

    if su in _LANG_BUY:
        return RECO_BUY
    if su in _LANG_SELL:
        return RECO_SELL
    if su in _LANG_HOLD:
        return RECO_HOLD
    if su in _LANG_REDUCE:
        return RECO_REDUCE

    if su in _STRONG_SELL_LIKE or _PAT_STRONG_SELL.search(su):
        return _apply_negation(su, RECO_SELL)
    if su in _STRONG_BUY_LIKE or _PAT_STRONG_BUY.search(su):
        return _apply_negation(su, RECO_BUY)

    if su in _SELL_LIKE:
        return _apply_negation(su, RECO_SELL)
    if su in _REDUCE_LIKE:
        return _apply_negation(su, RECO_REDUCE)
    if su in _HOLD_LIKE:
        return _apply_negation(su, RECO_HOLD)
    if su in _BUY_LIKE:
        return _apply_negation(su, RECO_BUY)

    num_result = _parse_numeric_rating(su, context_hints=True)
    if num_result and num_result[1] >= _CONFIDENCE_MEDIUM:
        return _apply_negation(su, num_result[0])

    if _PAT_SELL.search(su):
        return _apply_negation(su, RECO_SELL)
    if _PAT_REDUCE.search(su):
        return _apply_negation(su, RECO_REDUCE)
    if _PAT_HOLD.search(su):
        return _apply_negation(su, RECO_HOLD)
    if _PAT_BUY.search(su):
        return _apply_negation(su, RECO_BUY)

    sentiment = _sentiment_analysis(su)
    if sentiment:
        return sentiment

    return RECO_HOLD


# ---------------------------------------------------------------------
# Horizon / label helpers
# ---------------------------------------------------------------------
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


def _infer_horizon_days_from_payload(d: Dict[str, Any]) -> Optional[int]:
    v = _deep_get(d, _HORIZON_DAYS_FIELDS)
    if v is not None:
        di = _to_int(v)
        if di is not None and di > 0:
            return di

    label = _clean_text(_deep_get(d, _HORIZON_LABEL_FIELDS)).upper()
    if label:
        label_map = {
            "1M": 30, "30D": 30, "30": 30,
            "3M": 90, "90D": 90, "90": 90,
            "6M": 180, "180D": 180, "180": 180,
            "12M": 365, "1Y": 365, "365D": 365, "365": 365,
        }
        if label in label_map:
            return label_map[label]

    if _deep_get(d, ["required_roi_1m"]) is not None:
        return 30
    if _deep_get(d, ["required_roi_3m"]) is not None:
        return 90
    if _deep_get(d, ["required_roi_12m"]) is not None:
        return 365

    return None


def _infer_horizon_label_from_payload(d: Dict[str, Any], horizon_days: Optional[int]) -> Optional[str]:
    label = _clean_text(_deep_get(d, _HORIZON_LABEL_FIELDS)).upper()
    if label:
        return label
    return _horizon_label_from_days(horizon_days)


# ---------------------------------------------------------------------
# Public API: label normalization
# ---------------------------------------------------------------------
def normalize_recommendation(x: Any) -> str:
    """
    Return one canonical recommendation:
    BUY / HOLD / REDUCE / SELL
    Never raises.
    """
    try:
        candidate = _extract_candidate(x)
        if candidate is None:
            return RECO_HOLD

        if isinstance(candidate, str):
            s0 = candidate.strip()
            if not s0:
                return RECO_HOLD
            s0u = s0.upper()
            if s0u in _RECO_ENUM:
                return s0u
            return _parse_string_recommendation(s0)

        if isinstance(candidate, (int, float)) and not isinstance(candidate, bool):
            num_result = _parse_numeric_rating(str(candidate), context_hints=False)
            if num_result:
                return num_result[0]
            return RECO_HOLD

        s_raw = _safe_str(candidate)
        if not s_raw:
            return RECO_HOLD

        return _parse_string_recommendation(s_raw)
    except Exception:
        return RECO_HOLD


# ---------------------------------------------------------------------
# Public API: context-preserving normalization
# ---------------------------------------------------------------------
def normalize_recommendation_payload(x: Any) -> Dict[str, Any]:
    """
    Normalize a recommendation payload/row without stripping context.

    Guaranteed output keys:
    - recommendation
    - recommendation_reason
    - horizon_days
    - invest_period_label
    - invest_period_days

    Additional known fields are preserved when present.
    """
    try:
        if isinstance(x, dict):
            raw = dict(x)
        else:
            raw = {"recommendation": x}

        out: Dict[str, Any] = dict(raw)

        recommendation_value = _deep_get(raw, _VENDOR_FIELDS["default"] + _VENDOR_FIELDS["nested"])
        out["recommendation"] = normalize_recommendation(recommendation_value if recommendation_value is not None else raw)

        reason = _clean_text(_deep_get(raw, _REASON_FIELDS))
        if reason:
            out["recommendation_reason"] = reason
        else:
            out["recommendation_reason"] = _clean_text(out.get("recommendation_reason"))

        horizon_days = _infer_horizon_days_from_payload(raw)
        if horizon_days is None:
            horizon_days = _to_int(out.get("horizon_days")) or _to_int(out.get("invest_period_days"))
        out["horizon_days"] = horizon_days
        out["invest_period_days"] = _to_int(out.get("invest_period_days")) or horizon_days

        invest_period_label = _infer_horizon_label_from_payload(raw, horizon_days)
        if not invest_period_label:
            invest_period_label = _clean_text(out.get("invest_period_label")).upper()
        out["invest_period_label"] = invest_period_label or None

        # Preserve confidence-ish fields
        for key in _CONFIDENCE_FIELDS:
            val = _deep_get(raw, [key])
            if val is not None and _safe_str(val):
                out[key.split(".")[-1]] = val

        # Preserve scores when present
        for key in _SCORE_FIELDS:
            val = _deep_get(raw, [key])
            if val is not None:
                out[key] = val

        return out
    except Exception:
        return {
            "recommendation": RECO_HOLD,
            "recommendation_reason": "",
            "horizon_days": None,
            "invest_period_days": None,
            "invest_period_label": None,
        }


def normalize_recommendation_row(row: Dict[str, Any]) -> Dict[str, Any]:
    """
    Alias for row-level normalization with context preservation.
    """
    return normalize_recommendation_payload(row)


def normalize_recommendation_rows(rows: Sequence[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Normalize multiple rows while preserving context fields.
    """
    out: List[Dict[str, Any]] = []
    for row in rows:
        try:
            out.append(normalize_recommendation_row(row))
        except Exception:
            out.append({
                "recommendation": RECO_HOLD,
                "recommendation_reason": "",
                "horizon_days": None,
                "invest_period_days": None,
                "invest_period_label": None,
            })
    return out


# ---------------------------------------------------------------------
# Batch / utilities
# ---------------------------------------------------------------------
def batch_normalize(recommendations: Sequence[Any]) -> List[str]:
    return [normalize_recommendation(r) for r in recommendations]


def is_valid_recommendation(value: str) -> bool:
    return value in _RECO_ENUM


def get_recommendation_score(reco: str) -> int:
    scores = {
        RECO_BUY: 1,
        RECO_HOLD: 2,
        RECO_REDUCE: 3,
        RECO_SELL: 4,
    }
    return scores.get(reco, 2)


def recommendation_from_score(score: float, scale: str = "1-5") -> str:
    if scale == "1-5":
        if score <= 2.0:
            return RECO_BUY
        if score <= 3.0:
            return RECO_HOLD
        if score <= 4.0:
            return RECO_REDUCE
        return RECO_SELL
    if scale == "1-3":
        if score <= 1.5:
            return RECO_BUY
        if score <= 2.5:
            return RECO_HOLD
        return RECO_SELL
    if scale == "0-100":
        if score >= 70:
            return RECO_BUY
        if score >= 45:
            return RECO_HOLD
        return RECO_SELL
    return RECO_HOLD


__all__ = [
    "normalize_recommendation",
    "normalize_recommendation_payload",
    "normalize_recommendation_row",
    "normalize_recommendation_rows",
    "batch_normalize",
    "is_valid_recommendation",
    "get_recommendation_score",
    "recommendation_from_score",
    "VERSION",
    "RECO_BUY",
    "RECO_HOLD",
    "RECO_REDUCE",
    "RECO_SELL",
]
