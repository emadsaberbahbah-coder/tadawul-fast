#!/usr/bin/env python3
"""
core/reco_normalize.py
------------------------------------------------------------
Recommendation Normalization — v2.0.0 (PRODUCTION READY)

Purpose
- Provide ONE canonical recommendation enum across the whole app.
- Make provider outputs consistent for Sheets + APIs.
- Deterministic, fast, never raises.

Canonical output (ALWAYS)
- BUY / HOLD / REDUCE / SELL

Key Features
- ✅ Multi-layer extraction from dict/list structures with vendor-specific field mapping
- ✅ Comprehensive numeric parsing (1-5, 1-3, 0-100, 0-1, ratios) with confidence scoring
- ✅ Enhanced multi-lingual support (Arabic, French, Spanish, German, Portuguese)
- ✅ Negation handling with context awareness
- ✅ Sentiment analysis for ambiguous inputs
- ✅ Idempotent: canonical input returns same output
- ✅ Thread-safe, pure Python, no external dependencies

Notes
- Conservative by design: unknown/unclear => HOLD
- "REDUCE" for partial-sell/trim/take-profit/underweight
- "SELL" reserved for explicit sell/avoid/exit/underperform
"""

from __future__ import annotations

import re
from typing import Any, Dict, Iterable, List, Optional, Sequence, Set, Tuple, Union

VERSION = "2.0.0"

# ---------------------------------------------------------------------
# Canonical Enum
# ---------------------------------------------------------------------
RECO_BUY = "BUY"
RECO_HOLD = "HOLD"
RECO_REDUCE = "REDUCE"
RECO_SELL = "SELL"
_RECO_ENUM = frozenset({RECO_BUY, RECO_HOLD, RECO_REDUCE, RECO_SELL})

# Confidence thresholds for ambiguous cases
_CONFIDENCE_HIGH = 0.8
_CONFIDENCE_MEDIUM = 0.5

# ---------------------------------------------------------------------
# Common "no rating" / "not applicable" tokens
# ---------------------------------------------------------------------
_NO_RATING: Set[str] = {
    "NA", "N/A", "NONE", "NULL", "UNKNOWN", "UNRATED", "NOT RATED", "NO RATING",
    "NR", "N R", "SUSPENDED", "UNDER REVIEW", "NOT COVERED", "NO COVERAGE",
    "NOT APPLICABLE", "NIL", "—", "-", "WAITING", "PENDING", "TBA", "TBD",
    "NO RECOMMENDATION", "RATING WITHDRAWN", "WITHDRAWN", "NOT RANKED"
}

# ---------------------------------------------------------------------
# Exact phrase mappings (after normalization)
# ---------------------------------------------------------------------
# Strong variants (highest precedence)
_STRONG_BUY_LIKE: Set[str] = {
    "STRONG BUY", "CONVICTION BUY", "HIGH CONVICTION BUY", "BUY STRONG",
    "TOP PICK", "BEST IDEA", "HIGHLY RECOMMENDED BUY", "BUY (STRONG)",
    "STRONG BUY RECOMMENDATION", "MUST BUY", "STRONG ACCUMULATE", "FOCUS BUY"
}

_STRONG_SELL_LIKE: Set[str] = {
    "STRONG SELL", "SELL (STRONG)", "SELL STRONG", "EXIT IMMEDIATELY",
    "STRONG SELL RECOMMENDATION", "MUST SELL", "URGENT SELL", "FORCED EXIT"
}

# Standard variants
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

# ---------------------------------------------------------------------
# Enhanced Multi-lingual Mappings
# ---------------------------------------------------------------------
# Arabic (comprehensive)
_AR_BUY: Set[str] = {
    "شراء", "اشتر", "اشترى", "اشترِ", "تجميع", "زيادة", "ايجابي", "ايجابى",
    "فرصة شراء", "اداء متفوق", "زيادة المراكز", "توصية شراء", "توصية بالشراء",
    "شراء قوي", "شراء قوى", "احتفاظ مع ميل للشراء", "زيادة التعرض", "تعزيز المراكز",
    "شراء مكثف", "تراكم", "مضاعفة", "تفوق", "متفوق"
}

_AR_HOLD: Set[str] = {
    "احتفاظ", "محايد", "انتظار", "مراقبة", "ثبات", "استقرار", "تمسك", "اداء محايد",
    "حياد", "موقف محايد", "ابقاء المراكز", "متوازن", "بدون تغيير", "محافظة",
    "استمرارية", "لا تغيير", "محايد تماما"
}

_AR_REDUCE: Set[str] = {
    "تقليص", "تخفيف", "جني ارباح", "جني ارباح جزئي", "تقليل", "تخفيض",
    "اداء ضعيف", "تخفيض المراكز", "بيع جزئي", "تخفيف المراكز",
    "تقليل التعرض", "تخفيف المخاطر", "تسييل جزئي", "اخذ ارباح"
}

_AR_SELL: Set[str] = {
    "بيع", "تخارج", "سلبي", "سلبى", "تجنب", "خروج", "بيع قوي", "بيع قوى",
    "توصية بيع", "توصية بالبيع", "اداء اقل", "اداء أقل", "تصفية", "الخروج من السهم",
    "بيع كلي", "تسييل", "تخلص من", "تجنب تماما"
}

# French
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

# Spanish
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

# German
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

# Portuguese
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

# ---------------------------------------------------------------------
# Combined language mappings
# ---------------------------------------------------------------------
_LANG_BUY = _FR_BUY | _ES_BUY | _DE_BUY | _PT_BUY
_LANG_HOLD = _FR_HOLD | _ES_HOLD | _DE_HOLD | _PT_HOLD
_LANG_REDUCE = _FR_REDUCE | _ES_REDUCE | _DE_REDUCE | _PT_REDUCE
_LANG_SELL = _FR_SELL | _ES_SELL | _DE_SELL | _PT_SELL

# ---------------------------------------------------------------------
# Vendor-specific field mappings
# ---------------------------------------------------------------------
_VENDOR_FIELDS: Dict[str, List[str]] = {
    # Common
    "default": [
        "recommendation", "reco", "rating", "action", "signal",
        "analyst_rating", "analystRecommendation", "analyst_recommendation",
        "consensus", "consensus_rating"
    ],
    # Nested paths
    "nested": [
        "meta.recommendation", "meta.rating", "data.recommendation",
        "result.recommendation", "payload.recommendation",
        "recommendation.value", "rating.value", "analyst.rating"
    ],
    # Bloomberg / Refinitiv
    "bloomberg": [
        "analyst_rating", "recommendation_mean", "analyst_consensus",
        "rating_trend", "analyst_summary.recommendation"
    ],
    # Yahoo Finance
    "yahoo": [
        "recommendationKey", "recommendationMean", "analystRating",
        "targetConsensus", "rating"
    ],
    # Reuters / Refinitiv
    "reuters": [
        "analystRecommendations", "streetAccount.analystRating",
        "REC", "REC_AVG", "analystRatings.recommendation"
    ],
    # Morningstar
    "morningstar": [
        "starRating", "analystRating", "ratingSummary.recommendation"
    ],
    # Zacks
    "zacks": [
        "zacksRank", "zacksRankText", "zacks_rank", "rank"
    ],
    # TipRanks
    "tipranks": [
        "consensus", "analystConsensus", "tipranksConsensus"
    ]
}

# ---------------------------------------------------------------------
# Regex patterns (compiled for performance)
# ---------------------------------------------------------------------
# Negation patterns with context words
_PAT_NEGATE = re.compile(
    r"\b(?:NOT|NO|NEVER|WITHOUT|HARDLY|RARELY|UNLIKELY|AVOID|AGAINST)\b",
    re.IGNORECASE
)

_PAT_NEGATION_CONTEXT = re.compile(
    r"(?:NOT|NO|NEVER)\s+(?:A\s+|AN\s+)?(STRONG\s+)?(BUY|SELL|HOLD|REDUCE)",
    re.IGNORECASE
)

# Strong patterns (highest priority)
_PAT_STRONG_BUY = re.compile(
    r"\b(?:STRONG\s+BUY|CONVICTION\s+BUY|TOP\s+PICK|BEST\s+IDEA|MUST\s+BUY|FOCUS\s+BUY)\b",
    re.IGNORECASE
)
_PAT_STRONG_SELL = re.compile(
    r"\b(?:STRONG\s+SELL|EXIT\s+IMMEDIATELY|MUST\s+SELL|URGENT\s+SELL)\b",
    re.IGNORECASE
)

# Main patterns (ordered by severity)
_PAT_SELL = re.compile(
    r"\b(?:SELL|EXIT|AVOID|UNDERPERFORM|SHORT|LIQUIDATE|CLOSE\s+POSITION|UNWIND)\b",
    re.IGNORECASE
)
_PAT_REDUCE = re.compile(
    r"\b(?:REDUCE|TRIM|LIGHTEN|TAKE\s+PROFIT|TAKE\s+PROFITS|UNDERWEIGHT|DECREASE|CUT\s+POSITION|SCALE\s+BACK)\b",
    re.IGNORECASE
)
_PAT_HOLD = re.compile(
    r"\b(?:HOLD|NEUTRAL|MAINTAIN|EQUAL\s+WEIGHT|MARKET\s+PERFORM|SECTOR\s+PERFORM|IN\s+LINE|FAIR\s+VALUE|WAIT|WATCH|MONITOR)\b",
    re.IGNORECASE
)
_PAT_BUY = re.compile(
    r"\b(?:BUY|ACCUMULATE|ADD|OUTPERFORM|OVERWEIGHT|UPGRADE|LONG|BULLISH|POSITIVE)\b",
    re.IGNORECASE
)

# Numeric and rating hints
_PAT_RATING_HINT = re.compile(
    r"\b(?:RATING|SCORE|RANK|RECOMMENDATION|CONSENSUS|STARS|STAR)\b",
    re.IGNORECASE
)
_PAT_RATIO = re.compile(
    r"(\d+(?:[.,]\d+)?)\s*(?:/|OF|OUT\s+OF)\s*(\d+(?:[.,]\d+)?)",
    re.IGNORECASE
)
_PAT_NUMBER = re.compile(r"(-?\d+(?:[.,]\d+)?)")

# Confidence/probability indicators (to avoid misinterpreting as rating)
_PAT_CONFIDENCE = re.compile(
    r"\b(?:CONFIDENCE|PROB|PROBABILITY|CHANCE|LIKELIHOOD|LIKELYHOOD)\b",
    re.IGNORECASE
)

# Scale indicators
_PAT_SCALE_5 = re.compile(r"\b(?:[1-5]\s*(?:/|\|)?\s*[1-5]|5[- ]?POINT|5[- ]?STAR)\b")
_PAT_SCALE_3 = re.compile(r"\b(?:[1-3]\s*(?:/|\|)?\s*[1-3]|3[- ]?TIER|3[- ]?LEVEL)\b")

# Cleanup patterns
_PAT_SEP = re.compile(r"[\t\r\n]+")
_PAT_SPLIT = re.compile(r"[\-_/|]+")
_PAT_WS = re.compile(r"\s+")
_PAT_PUNCT = re.compile(r"[^\w\s\-]")

# ---------------------------------------------------------------------
# Enhanced Helpers
# ---------------------------------------------------------------------
def _safe_str(x: Any) -> str:
    """Safely convert any value to string, never raise."""
    try:
        return "" if x is None else str(x).strip()
    except Exception:
        return ""


def _first_non_empty_str(items: Iterable[Any]) -> Optional[str]:
    """Return first non-empty, non-null string from iterable."""
    for it in items:
        s = _safe_str(it)
        if s and s.lower() not in ("none", "null", "n/a", ""):
            return s
    return None


def _deep_get(d: Dict[str, Any], keys: Sequence[str]) -> Optional[Any]:
    """
    Enhanced nested key fetch with support for dotted paths and list indices.
    """
    for key in keys:
        if not key:
            continue
            
        # Direct key access
        if key in d:
            v = d.get(key)
            if v is not None and _safe_str(v):
                return v
        
        # Dotted path
        if "." in key:
            current: Any = d
            parts = key.split(".")
            valid = True
            
            for part in parts:
                # Handle list indices if present (e.g., "items[0].rating")
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
    """
    Advanced extraction for structured inputs with vendor-specific handling.
    """
    if x is None:
        return None
    
    # Direct canonical string - fast path
    if isinstance(x, str) and x.upper() in _RECO_ENUM:
        return x.upper()
    
    # Direct numeric - keep for processing
    if isinstance(x, (int, float)) and not isinstance(x, bool):
        return x
    
    # Dict handling
    if isinstance(x, dict):
        # Try all vendor fields in order
        for vendor, fields in _VENDOR_FIELDS.items():
            v = _deep_get(x, fields)
            if v is not None and _safe_str(v):
                return v
        
        # Try common key patterns
        for key in x.keys():
            if isinstance(key, str) and any(hint in key.lower() for hint in 
                ["reco", "rating", "rank", "score", "consensus"]):
                v = x[key]
                if v is not None and _safe_str(v):
                    return v
        
        # Last resort: first non-empty string value
        return _first_non_empty_str(x.values())
    
    # List/tuple handling
    if isinstance(x, (list, tuple)):
        # If it's a list of dicts, try to extract from first
        if x and isinstance(x[0], dict):
            for item in x:
                extracted = _extract_candidate(item)
                if extracted is not None:
                    return extracted
        
        # Otherwise take first non-empty string
        return _first_non_empty_str(x)
    
    # Any other type
    return x


def _normalize_arabic(s: str) -> str:
    """
    Comprehensive Arabic normalization:
    - Normalize Alef variants
    - Normalize Yeh and Alef maksura
    - Remove diacritics (tashkeel)
    - Standardize common variations
    """
    s = _safe_str(s)
    if not s:
        return ""
    
    # Normalize whitespace
    s = _PAT_WS.sub(" ", s).strip()
    
    # Arabic character normalization
    replacements = {
        # Alef variants
        "أ": "ا", "إ": "ا", "آ": "ا",
        # Yeh variants
        "ي": "ي", "ى": "ي",  # Yeh and Alef maksura
        # Teh Marbuta
        "ة": "ه",
        # Remove diacritics (tashkeel)
        "َ": "", "ِ": "", "ُ": "", "ْ": "", "ّ": ""
    }
    
    for old, new in replacements.items():
        s = s.replace(old, new)
    
    return s


def _normalize_text(s: str) -> str:
    """
    Comprehensive text normalization for multiple languages.
    """
    s = _safe_str(s)
    if not s:
        return ""
    
    # Upper case for consistent matching
    su = s.upper()
    
    # Replace separators
    su = _PAT_SEP.sub(" ", su)
    su = _PAT_SPLIT.sub(" ", su)
    
    # Remove punctuation (keep spaces and hyphens for compound terms)
    su = _PAT_PUNCT.sub(" ", su)
    
    # Collapse whitespace
    su = _PAT_WS.sub(" ", su).strip()
    
    return su


def _parse_numeric_rating(s: str, context_hints: bool = True) -> Optional[Tuple[str, float]]:
    """
    Advanced numeric parsing with confidence scoring.
    Returns (reco, confidence) or None.
    """
    if not s:
        return None
    
    su = s.upper()
    
    # Skip if clearly confidence/probability and no rating hints
    if _PAT_CONFIDENCE.search(su) and not _PAT_RATING_HINT.search(su):
        return None
    
    # Try ratio format first (e.g., "3/5", "2 out of 3")
    mratio = _PAT_RATIO.search(su)
    if mratio:
        try:
            x = float(mratio.group(1).replace(",", "."))
            y = float(mratio.group(2).replace(",", "."))
            
            if y > 0:
                # Convert to 1-5 scale for consistency
                if y == 5:
                    normalized = x
                elif y == 3:
                    # Map 1-3 to 1-5: 1->1, 2->3, 3->5
                    normalized = {1: 1.0, 2: 3.0, 3: 5.0}.get(int(round(x)), 3.0)
                elif y == 100:
                    # Map 0-100 to 1-5
                    normalized = 1 + (x / 100) * 4
                else:
                    normalized = x
                
                # Convert to reco with confidence
                if normalized <= 2.0:
                    return (RECO_BUY, 0.9)
                elif normalized <= 3.0:
                    return (RECO_HOLD, 0.7)
                elif normalized <= 4.0:
                    return (RECO_REDUCE, 0.7)
                else:
                    return (RECO_SELL, 0.9)
        except Exception:
            pass
    
    # Extract first number
    mnum = _PAT_NUMBER.search(su)
    if not mnum:
        return None
    
    try:
        num = float(mnum.group(1).replace(",", "."))
    except Exception:
        return None
    
    # Determine scale based on context
    scale_5 = _PAT_SCALE_5.search(su) or "STAR" in su or "STARS" in su
    scale_3 = _PAT_SCALE_3.search(su)
    
    # Check if it's likely a rating (has hint or reasonable scale)
    has_hint = _PAT_RATING_HINT.search(su) if context_hints else True
    
    # 1-5 scale
    if 1.0 <= num <= 5.0 and (has_hint or scale_5):
        n = int(round(num))
        if n <= 2:
            return (RECO_BUY, 0.85)
        elif n == 3:
            return (RECO_HOLD, 0.85)
        elif n == 4:
            return (RECO_REDUCE, 0.85)
        else:
            return (RECO_SELL, 0.85)
    
    # 1-3 scale
    if 1.0 <= num <= 3.0 and (has_hint or scale_3):
        n = int(round(num))
        if n == 1:
            return (RECO_BUY, 0.8)
        elif n == 2:
            return (RECO_HOLD, 0.8)
        else:
            return (RECO_SELL, 0.8)
    
    # 0-100 scale (only with strong hints)
    if 0.0 <= num <= 100.0 and has_hint:
        if num >= 70.0:
            return (RECO_BUY, 0.75)
        elif num >= 45.0:
            return (RECO_HOLD, 0.6)
        else:
            return (RECO_SELL, 0.75)
    
    # 0-1 scale (only with very strong hints)
    if 0.0 <= num <= 1.0 and has_hint:
        if num >= 0.7:
            return (RECO_BUY, 0.5)
        elif num >= 0.45:
            return (RECO_HOLD, 0.4)
        else:
            return (RECO_SELL, 0.5)
    
    return None


def _apply_negation(su: str, reco: str) -> str:
    """
    Enhanced negation handling with context awareness.
    """
    if not su or not reco:
        return reco
    
    # Check for explicit negation patterns
    neg_match = _PAT_NEGATION_CONTEXT.search(su)
    if neg_match:
        return RECO_HOLD
    
    # Check for generic negation
    if _PAT_NEGATE.search(su):
        # Soften extreme recommendations
        if reco == RECO_SELL:
            return RECO_REDUCE
        if reco == RECO_BUY:
            return RECO_HOLD
    
    return reco


def _sentiment_analysis(su: str) -> Optional[str]:
    """
    Simple sentiment analysis for ambiguous inputs.
    """
    if not su:
        return None
    
    # Count positive/negative terms
    positive_terms = ["BULLISH", "POSITIVE", "OPTIMISTIC", "UP", "UPSIDE", "GAIN"]
    negative_terms = ["BEARISH", "NEGATIVE", "PESSIMISTIC", "DOWN", "DOWNSIDE", "LOSS"]
    
    pos_count = sum(1 for term in positive_terms if term in su)
    neg_count = sum(1 for term in negative_terms if term in su)
    
    if pos_count > neg_count + 1:
        return RECO_BUY
    elif neg_count > pos_count + 1:
        return RECO_SELL
    
    return None


# ---------------------------------------------------------------------
# Main Normalization Function
# ---------------------------------------------------------------------
def normalize_recommendation(x: Any) -> str:
    """
    Always returns one of: BUY / HOLD / REDUCE / SELL.
    Never raises.
    """
    try:
        # Extract candidate from structured input
        candidate = _extract_candidate(x)
        if candidate is None:
            return RECO_HOLD
        
        # Fast path: already canonical
        if isinstance(candidate, str):
            s0 = candidate.strip()
            if not s0:
                return RECO_HOLD
            s0u = s0.upper()
            if s0u in _RECO_ENUM:
                return s0u
        
        # Direct numeric (int/float)
        if isinstance(candidate, (int, float)) and not isinstance(candidate, bool):
            num_result = _parse_numeric_rating(str(candidate), context_hints=False)
            if num_result:
                return num_result[0]
            return RECO_HOLD
        
        # Convert to string for text processing
        s_raw = _safe_str(candidate)
        if not s_raw:
            return RECO_HOLD
        
        # -----------------------------------------------------------------
        # Arabic processing (before English normalization)
        # -----------------------------------------------------------------
        s_ar = _normalize_arabic(s_raw)
        
        # Exact matches in Arabic
        if s_ar in _AR_BUY:
            return RECO_BUY
        if s_ar in _AR_HOLD:
            return RECO_HOLD
        if s_ar in _AR_REDUCE:
            return RECO_REDUCE
        if s_ar in _AR_SELL:
            return RECO_SELL
        
        # Contains Arabic patterns
        if any(tok in s_ar for tok in ["شراء", "اشتر", "تجميع", "زيادة"]):
            return RECO_BUY
        if any(tok in s_ar for tok in ["بيع قوي", "تخارج", "تجنب", "تصفية"]):
            return RECO_SELL
        if any(tok in s_ar for tok in ["جني ارباح", "تخفيف", "تقليص"]):
            return RECO_REDUCE
        if any(tok in s_ar for tok in ["احتفاظ", "محايد", "انتظار", "مراقبة"]):
            return RECO_HOLD
        
        # -----------------------------------------------------------------
        # English + other languages normalization
        # -----------------------------------------------------------------
        su = _normalize_text(s_raw)
        if not su:
            return RECO_HOLD
        
        # No rating tokens
        if su in _NO_RATING:
            return RECO_HOLD
        
        # Other languages (non-Arabic, non-English)
        if su in _LANG_BUY:
            return RECO_BUY
        if su in _LANG_SELL:
            return RECO_SELL
        if su in _LANG_HOLD:
            return RECO_HOLD
        if su in _LANG_REDUCE:
            return RECO_REDUCE
        
        # Strong patterns (highest precedence)
        if su in _STRONG_SELL_LIKE or _PAT_STRONG_SELL.search(su):
            return _apply_negation(su, RECO_SELL)
        if su in _STRONG_BUY_LIKE or _PAT_STRONG_BUY.search(su):
            return _apply_negation(su, RECO_BUY)
        
        # Exact phrase matches
        if su in _SELL_LIKE:
            return _apply_negation(su, RECO_SELL)
        if su in _REDUCE_LIKE:
            return _apply_negation(su, RECO_REDUCE)
        if su in _HOLD_LIKE:
            return _apply_negation(su, RECO_HOLD)
        if su in _BUY_LIKE:
            return _apply_negation(su, RECO_BUY)
        
        # Numeric parsing with context hints
        num_result = _parse_numeric_rating(su, context_hints=True)
        if num_result and num_result[1] >= _CONFIDENCE_MEDIUM:
            return _apply_negation(su, num_result[0])
        
        # Pattern-based matching (ordered by severity)
        if _PAT_SELL.search(su):
            return _apply_negation(su, RECO_SELL)
        if _PAT_REDUCE.search(su):
            return _apply_negation(su, RECO_REDUCE)
        if _PAT_HOLD.search(su):
            return _apply_negation(su, RECO_HOLD)
        if _PAT_BUY.search(su):
            return _apply_negation(su, RECO_BUY)
        
        # Sentiment analysis as fallback
        sentiment = _sentiment_analysis(su)
        if sentiment:
            return sentiment
        
        # Default fallback
        return RECO_HOLD
        
    except Exception as e:
        # Log in production would go here, but never raise
        return RECO_HOLD


# ---------------------------------------------------------------------
# Additional Utilities
# ---------------------------------------------------------------------
def batch_normalize(recommendations: Sequence[Any]) -> List[str]:
    """Normalize multiple recommendations in batch."""
    return [normalize_recommendation(r) for r in recommendations]


def is_valid_recommendation(value: str) -> bool:
    """Check if value is a valid canonical recommendation."""
    return value in _RECO_ENUM


def get_recommendation_score(reco: str) -> int:
    """Convert recommendation to numeric score (1=BUY, 4=SELL)."""
    scores = {
        RECO_BUY: 1,
        RECO_HOLD: 2,
        RECO_REDUCE: 3,
        RECO_SELL: 4
    }
    return scores.get(reco, 2)


def recommendation_from_score(score: float, scale: str = "1-5") -> str:
    """Convert numeric score to recommendation."""
    if scale == "1-5":
        if score <= 2.0:
            return RECO_BUY
        elif score <= 3.0:
            return RECO_HOLD
        elif score <= 4.0:
            return RECO_REDUCE
        else:
            return RECO_SELL
    elif scale == "1-3":
        if score <= 1.5:
            return RECO_BUY
        elif score <= 2.5:
            return RECO_HOLD
        else:
            return RECO_SELL
    elif scale == "0-100":
        if score >= 70:
            return RECO_BUY
        elif score >= 45:
            return RECO_HOLD
        else:
            return RECO_SELL
    else:
        return RECO_HOLD


# ---------------------------------------------------------------------
# Module Exports
# ---------------------------------------------------------------------
__all__ = [
    "normalize_recommendation",
    "batch_normalize",
    "is_valid_recommendation",
    "get_recommendation_score",
    "recommendation_from_score",
    "VERSION",
    "RECO_BUY",
    "RECO_HOLD",
    "RECO_REDUCE",
    "RECO_SELL"
]


# ---------------------------------------------------------------------
# Self-test (if run directly)
# ---------------------------------------------------------------------
if __name__ == "__main__":
    # Quick test cases
    test_cases = [
        # Canonical
        ("BUY", RECO_BUY),
        ("HOLD", RECO_HOLD),
        ("REDUCE", RECO_REDUCE),
        ("SELL", RECO_SELL),
        
        # English variants
        ("Strong Buy", RECO_BUY),
        ("accumulate", RECO_BUY),
        ("outperform", RECO_BUY),
        ("neutral", RECO_HOLD),
        ("market perform", RECO_HOLD),
        ("trim", RECO_REDUCE),
        ("take profit", RECO_REDUCE),
        ("exit", RECO_SELL),
        ("underperform", RECO_SELL),
        
        # Numeric
        ("2/5", RECO_BUY),
        ("3/5", RECO_HOLD),
        ("4/5", RECO_REDUCE),
        ("5/5", RECO_SELL),
        ("1/3", RECO_BUY),
        ("2/3", RECO_HOLD),
        ("3/3", RECO_SELL),
        
        # Arabic
        ("شراء", RECO_BUY),
        ("احتفاظ", RECO_HOLD),
        ("تقليص", RECO_REDUCE),
        ("بيع", RECO_SELL),
        
        # French
        ("ACHETER", RECO_BUY),
        ("CONSERVER", RECO_HOLD),
        ("RÉDUIRE", RECO_REDUCE),
        ("VENDRE", RECO_SELL),
        
        # Spanish
        ("COMPRAR", RECO_BUY),
        ("MANTENER", RECO_HOLD),
        ("REDUCIR", RECO_REDUCE),
        ("VENDER", RECO_SELL),
        
        # German
        ("KAUFEN", RECO_BUY),
        ("HALTEN", RECO_HOLD),
        ("REDUZIEREN", RECO_REDUCE),
        ("VERKAUFEN", RECO_SELL),
        
        # Portuguese
        ("COMPRAR", RECO_BUY),
        ("MANTER", RECO_HOLD),
        ("REDUZIR", RECO_REDUCE),
        ("VENDER", RECO_SELL),
        
        # Negation
        ("NOT A BUY", RECO_HOLD),
        ("NO SELL", RECO_HOLD),
        
        # Unknown
        ("some random text", RECO_HOLD),
        ("", RECO_HOLD),
        (None, RECO_HOLD),
    ]
    
    print(f"Recommendation Normalizer v{VERSION} - Self Test")
    print("-" * 50)
    
    passed = 0
    failed = 0
    
    for inp, expected in test_cases:
        result = normalize_recommendation(inp)
        status = "✓" if result == expected else "✗"
        if result == expected:
            passed += 1
        else:
            failed += 1
        print(f"{status} {str(inp):<20} -> {result:6} (expected: {expected})")
    
    print("-" * 50)
    print(f"Passed: {passed}, Failed: {failed}")
    print(f"Success Rate: {passed/(passed+failed)*100:.1f}%")
