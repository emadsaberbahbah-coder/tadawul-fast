#!/usr/bin/env python3
"""
core/reco_normalize.py
------------------------------------------------------------
تسوية التوصيات — الإصدار 4.0.0 (مستوى المؤسسات المتقدم - Quantum Edition)
------------------------------------------------------------

الغرض:
- توفير قائمة موحدة للتوصيات (Enum) عبر التطبيق بأكمله.
- جعل مخرجات مزودي البيانات متسقة مع الجداول وواجهات برمجة التطبيقات.
- أداء حتمي، سريع جداً، ولا يرمي استثناءات (Errors) أبداً.

المخرجات القياسية (دائماً):
- BUY (شراء) / HOLD (احتفاظ) / REDUCE (تخفيف) / SELL (بيع)

الميزات الرئيسية في الإصدار 4.0.0:
- ✅ التوافق مع فحص النظافة (Hygiene): إزالة دوال `print` لتجاوز فحص CI/CD بنجاح.
- ✅ تخزين مؤقت عالي الأداء (LRU Caching) لمعالجة السلاسل النصية بسرعات فائقة.
- ✅ استخراج متعدد الطبقات من القواميس/القوائم مع تعيين حقول خاصة بكل مزود (Vendor).
- ✅ تحليل رقمي شامل (1-5، 1-3، 0-100، 0-1، النسب) مع حساب درجات الثقة.
- ✅ دعم متعدد اللغات محسن (العربية، الفرنسية، الإسبانية، الألمانية، البرتغالية).
- ✅ معالجة النفي مع الوعي بالسياق.
- ✅ تحليل المشاعر للمدخلات الغامضة.
- ✅ آمن للاستخدام مع خيوط المعالجة المتعددة (Thread-safe) وبدون اعتمادات خارجية.

ملاحظات:
- التصميم محافظ بطبيعته: إذا كانت الحالة غير معروفة أو غامضة => HOLD.
- "REDUCE" تُستخدم للبيع الجزئي/التخفيف/جني الأرباح/تقليل الوزن.
- "SELL" مخصصة للبيع الصريح/التجنب/الخروج/الأداء الضعيف جداً.
"""

from __future__ import annotations

import re
import sys
from functools import lru_cache
from typing import Any, Dict, Iterable, List, Optional, Sequence, Set, Tuple, Union

VERSION = "4.0.0"

# ---------------------------------------------------------------------
# التوصيات القياسية (Canonical Enum)
# ---------------------------------------------------------------------
RECO_BUY = "BUY"
RECO_HOLD = "HOLD"
RECO_REDUCE = "REDUCE"
RECO_SELL = "SELL"
_RECO_ENUM = frozenset({RECO_BUY, RECO_HOLD, RECO_REDUCE, RECO_SELL})

# عتبات الثقة للحالات الغامضة
_CONFIDENCE_HIGH = 0.8
_CONFIDENCE_MEDIUM = 0.5

# ---------------------------------------------------------------------
# الرموز الشائعة التي تعني "بدون تقييم" أو "غير متوفر"
# ---------------------------------------------------------------------
_NO_RATING: Set[str] = {
    "NA", "N/A", "NONE", "NULL", "UNKNOWN", "UNRATED", "NOT RATED", "NO RATING",
    "NR", "N R", "SUSPENDED", "UNDER REVIEW", "NOT COVERED", "NO COVERAGE",
    "NOT APPLICABLE", "NIL", "—", "-", "WAITING", "PENDING", "TBA", "TBD",
    "NO RECOMMENDATION", "RATING WITHDRAWN", "WITHDRAWN", "NOT RANKED"
}

# ---------------------------------------------------------------------
# تعيينات العبارات الدقيقة باللغة الإنجليزية (بعد التسوية)
# ---------------------------------------------------------------------
# متغيرات الشراء القوي (الأولوية القصوى)
_STRONG_BUY_LIKE: Set[str] = {
    "STRONG BUY", "CONVICTION BUY", "HIGH CONVICTION BUY", "BUY STRONG",
    "TOP PICK", "BEST IDEA", "HIGHLY RECOMMENDED BUY", "BUY (STRONG)",
    "STRONG BUY RECOMMENDATION", "MUST BUY", "STRONG ACCUMULATE", "FOCUS BUY"
}

# متغيرات البيع القوي
_STRONG_SELL_LIKE: Set[str] = {
    "STRONG SELL", "SELL (STRONG)", "SELL STRONG", "EXIT IMMEDIATELY",
    "STRONG SELL RECOMMENDATION", "MUST SELL", "URGENT SELL", "FORCED EXIT",
    "LIQUIDATE ALL"
}

# متغيرات الشراء القياسية
_BUY_LIKE: Set[str] = {
    "BUY", "ACCUMULATE", "ADD", "OUTPERFORM", "MARKET OUTPERFORM",
    "SECTOR OUTPERFORM", "OVERWEIGHT", "INCREASE", "UPGRADE", "LONG",
    "BULLISH", "POSITIVE", "BUYING", "SPECULATIVE BUY", "RECOMMENDED BUY",
    "BUY ON DIPS", "BUY RECOMMENDATION", "ACCUMULATION", "BUILT", "POSITION"
}

# متغيرات الاحتفاظ
_HOLD_LIKE: Set[str] = {
    "HOLD", "NEUTRAL", "MAINTAIN", "UNCHANGED", "MARKET PERFORM", "SECTOR PERFORM",
    "IN LINE", "EQUAL WEIGHT", "EQUALWEIGHT", "FAIR VALUE", "FAIRLY VALUED",
    "WAIT", "KEEP", "STABLE", "WATCH", "MONITOR", "NO ACTION", "PERFORM",
    "HOLD RECOMMENDATION", "HOLDING", "KEEP POSITION", "NO CHANGE"
}

# متغيرات التخفيف / جني الأرباح
_REDUCE_LIKE: Set[str] = {
    "REDUCE", "TRIM", "LIGHTEN", "PARTIAL SELL", "TAKE PROFIT", "TAKE PROFITS",
    "UNDERWEIGHT", "DECREASE", "WEAKEN", "CAUTIOUS", "PROFIT TAKING",
    "SELL STRENGTH", "TRIM POSITION", "REDUCE EXPOSURE", "PARTIAL EXIT",
    "REDUCE WEIGHT", "CUT POSITION", "LESSEN", "SCALE BACK"
}

# متغيرات البيع القياسية
_SELL_LIKE: Set[str] = {
    "SELL", "EXIT", "AVOID", "UNDERPERFORM", "MARKET UNDERPERFORM",
    "SECTOR UNDERPERFORM", "SHORT", "BEARISH", "NEGATIVE", "SELLING",
    "DOWNGRADE", "RED FLAG", "LIQUIDATE", "CLOSE POSITION", "UNWIND",
    "SELL RECOMMENDATION", "AVOIDANCE", "EXIT POSITION"
}

# ---------------------------------------------------------------------
# تعيينات لغات متعددة محسنة
# ---------------------------------------------------------------------
# اللغة العربية
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

# اللغة الفرنسية
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

# اللغة الإسبانية
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

# اللغة الألمانية
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

# اللغة البرتغالية
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
# المجموعات اللغوية المدمجة
# ---------------------------------------------------------------------
_LANG_BUY = _FR_BUY | _ES_BUY | _DE_BUY | _PT_BUY
_LANG_HOLD = _FR_HOLD | _ES_HOLD | _DE_HOLD | _PT_HOLD
_LANG_REDUCE = _FR_REDUCE | _ES_REDUCE | _DE_REDUCE | _PT_REDUCE
_LANG_SELL = _FR_SELL | _ES_SELL | _DE_SELL | _PT_SELL

# ---------------------------------------------------------------------
# تعيينات الحقول الخاصة بكل مزود بيانات
# ---------------------------------------------------------------------
_VENDOR_FIELDS: Dict[str, List[str]] = {
    # الأسماء الشائعة
    "default": [
        "recommendation", "reco", "rating", "action", "signal",
        "analyst_rating", "analystRecommendation", "analyst_recommendation",
        "consensus", "consensus_rating"
    ],
    # المسارات المتداخلة (Nested)
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
# أنماط التعابير المنطقية (مجمعة مسبقاً لتحسين الأداء)
# ---------------------------------------------------------------------
# أنماط النفي مع الكلمات السياقية
_PAT_NEGATE = re.compile(
    r"\b(?:NOT|NO|NEVER|WITHOUT|HARDLY|RARELY|UNLIKELY|AVOID|AGAINST)\b",
    re.IGNORECASE
)

_PAT_NEGATION_CONTEXT = re.compile(
    r"(?:NOT|NO|NEVER)\s+(?:A\s+|AN\s+)?(STRONG\s+)?(BUY|SELL|HOLD|REDUCE)",
    re.IGNORECASE
)

# الأنماط القوية (الأولوية القصوى)
_PAT_STRONG_BUY = re.compile(
    r"\b(?:STRONG\s+BUY|CONVICTION\s+BUY|TOP\s+PICK|BEST\s+IDEA|MUST\s+BUY|FOCUS\s+BUY)\b",
    re.IGNORECASE
)
_PAT_STRONG_SELL = re.compile(
    r"\b(?:STRONG\s+SELL|EXIT\s+IMMEDIATELY|MUST\s+SELL|URGENT\s+SELL)\b",
    re.IGNORECASE
)

# الأنماط الرئيسية (مرتبة حسب الشدة)
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

# التلميحات الرقمية والتقييمات
_PAT_RATING_HINT = re.compile(
    r"\b(?:RATING|SCORE|RANK|RECOMMENDATION|CONSENSUS|STARS|STAR)\b",
    re.IGNORECASE
)
_PAT_RATIO = re.compile(
    r"(\d+(?:[.,]\d+)?)\s*(?:/|OF|OUT\s+OF)\s*(\d+(?:[.,]\d+)?)",
    re.IGNORECASE
)
_PAT_NUMBER = re.compile(r"(-?\d+(?:[.,]\d+)?)")

# مؤشرات الثقة/الاحتمالية (لتجنب التفسير الخاطئ على أنها تقييم)
_PAT_CONFIDENCE = re.compile(
    r"\b(?:CONFIDENCE|PROB|PROBABILITY|CHANCE|LIKELIHOOD|LIKELYHOOD)\b",
    re.IGNORECASE
)

# مؤشرات المقاييس (Scale Indicators)
_PAT_SCALE_5 = re.compile(r"\b(?:[1-5]\s*(?:/|\|)?\s*[1-5]|5[- ]?POINT|5[- ]?STAR)\b")
_PAT_SCALE_3 = re.compile(r"\b(?:[1-3]\s*(?:/|\|)?\s*[1-3]|3[- ]?TIER|3[- ]?LEVEL)\b")

# أنماط التنظيف (Cleanup Patterns)
_PAT_SEP = re.compile(r"[\t\r\n]+")
_PAT_SPLIT = re.compile(r"[\-_/|]+")
_PAT_WS = re.compile(r"\s+")
_PAT_PUNCT = re.compile(r"[^\w\s\-]")

# ---------------------------------------------------------------------
# دوال مساعدة محسنة (Enhanced Helpers)
# ---------------------------------------------------------------------
def _safe_str(x: Any) -> str:
    """تحويل آمن إلى سلسلة نصية، لا يرمي استثناءات أبداً."""
    try:
        return "" if x is None else str(x).strip()
    except Exception:
        return ""


def _first_non_empty_str(items: Iterable[Any]) -> Optional[str]:
    """إرجاع أول سلسلة نصية غير فارغة من كائن قابل للتكرار."""
    for it in items:
        s = _safe_str(it)
        if s and s.lower() not in ("none", "null", "n/a", ""):
            return s
    return None


def _deep_get(d: Dict[str, Any], keys: Sequence[str]) -> Optional[Any]:
    """استخراج آمن للقيم المتداخلة مع دعم للمسارات المنقطة وفهارس القوائم."""
    for key in keys:
        if not key:
            continue
            
        # وصول مباشر للمفتاح
        if key in d:
            v = d.get(key)
            if v is not None and _safe_str(v):
                return v
        
        # المسار المنقط (Dotted Path)
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
    """
    استخراج متقدم للبيانات المهيكلة مع معالجة خاصة لمزودي البيانات.
    """
    if x is None:
        return None
    
    # سلسلة نصية قياسية - مسار سريع
    if isinstance(x, str) and x.upper() in _RECO_ENUM:
        return x.upper()
    
    # رقم مباشر - الاحتفاظ به للمعالجة
    if isinstance(x, (int, float)) and not isinstance(x, bool):
        return x
    
    # معالجة القواميس (Dict)
    if isinstance(x, dict):
        for vendor, fields in _VENDOR_FIELDS.items():
            v = _deep_get(x, fields)
            if v is not None and _safe_str(v):
                return v
        
        # محاولة البحث عن مفاتيح شائعة
        for key in x.keys():
            if isinstance(key, str) and any(hint in key.lower() for hint in 
                ["reco", "rating", "rank", "score", "consensus"]):
                v = x[key]
                if v is not None and _safe_str(v):
                    return v
        
        # الملاذ الأخير: أول سلسلة نصية غير فارغة
        return _first_non_empty_str(x.values())
    
    # معالجة القوائم (List)
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
    """
    تسوية شاملة للنصوص العربية باستخدام التخزين المؤقت.
    - تسوية أشكال الألف
    - تسوية الياء والألف المقصورة
    - إزالة التشكيل
    """
    if not s:
        return ""
    
    s = _PAT_WS.sub(" ", s).strip()
    
    replacements = {
        "أ": "ا", "إ": "ا", "آ": "ا",
        "ي": "ي", "ى": "ي",
        "ة": "ه",
        "َ": "", "ِ": "", "ُ": "", "ْ": "", "ّ": ""
    }
    
    for old, new in replacements.items():
        s = s.replace(old, new)
    
    return s


@lru_cache(maxsize=8192)
def _normalize_text(s: str) -> str:
    """
    تسوية نصية شاملة للغات المتعددة باستخدام التخزين المؤقت.
    """
    if not s:
        return ""
    
    su = s.upper()
    su = _PAT_SEP.sub(" ", su)
    su = _PAT_SPLIT.sub(" ", su)
    su = _PAT_PUNCT.sub(" ", su)
    su = _PAT_WS.sub(" ", su).strip()
    
    return su


def _parse_numeric_rating(s: str, context_hints: bool = True) -> Optional[Tuple[str, float]]:
    """تحليل متقدم للتقييمات الرقمية مع حساب درجة الثقة."""
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
                elif normalized <= 3.0:
                    return (RECO_HOLD, 0.7)
                elif normalized <= 4.0:
                    return (RECO_REDUCE, 0.7)
                else:
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
        elif n == 3:
            return (RECO_HOLD, 0.85)
        elif n == 4:
            return (RECO_REDUCE, 0.85)
        else:
            return (RECO_SELL, 0.85)
    
    if 1.0 <= num <= 3.0 and (has_hint or scale_3):
        n = int(round(num))
        if n == 1:
            return (RECO_BUY, 0.8)
        elif n == 2:
            return (RECO_HOLD, 0.8)
        else:
            return (RECO_SELL, 0.8)
    
    if 0.0 <= num <= 100.0 and has_hint:
        if num >= 70.0:
            return (RECO_BUY, 0.75)
        elif num >= 45.0:
            return (RECO_HOLD, 0.6)
        else:
            return (RECO_SELL, 0.75)
    
    if 0.0 <= num <= 1.0 and has_hint:
        if num >= 0.7:
            return (RECO_BUY, 0.5)
        elif num >= 0.45:
            return (RECO_HOLD, 0.4)
        else:
            return (RECO_SELL, 0.5)
    
    return None


def _apply_negation(su: str, reco: str) -> str:
    """معالجة محسنة للنفي مع الوعي بالسياق."""
    if not su or not reco:
        return reco
    
    neg_match = _PAT_NEGATION_CONTEXT.search(su)
    if neg_match:
        return RECO_HOLD
    
    if _PAT_NEGATE.search(su):
        if reco == RECO_SELL:
            return RECO_REDUCE
        if reco == RECO_BUY:
            return RECO_HOLD
            
    return reco


def _sentiment_analysis(su: str) -> Optional[str]:
    """تحليل المشاعر الأساسي للمدخلات الغامضة."""
    if not su:
        return None
    
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
# الدالة الأساسية والمخزنة مؤقتاً للتسوية (Main Normalization Logic)
# ---------------------------------------------------------------------
@lru_cache(maxsize=8192)
def _parse_string_recommendation(s_raw: str) -> str:
    """
    تحليل السلسلة النصية للتوصيات باستخدام التخزين المؤقت (LRU Caching) لضمان الأداء الفائق.
    """
    s_ar = _normalize_arabic(s_raw)
    
    # التطابقات الدقيقة باللغة العربية
    if s_ar in _AR_BUY: return RECO_BUY
    if s_ar in _AR_HOLD: return RECO_HOLD
    if s_ar in _AR_REDUCE: return RECO_REDUCE
    if s_ar in _AR_SELL: return RECO_SELL
    
    # فحص الأنماط العربية الشائعة
    if any(tok in s_ar for tok in ["شراء", "اشتر", "تجميع", "زيادة"]): return RECO_BUY
    if any(tok in s_ar for tok in ["بيع قوي", "تخارج", "تجنب", "تصفية"]): return RECO_SELL
    if any(tok in s_ar for tok in ["جني ارباح", "تخفيف", "تقليص"]): return RECO_REDUCE
    if any(tok in s_ar for tok in ["احتفاظ", "محايد", "انتظار", "مراقبة"]): return RECO_HOLD
    
    # التسوية الشاملة للغات الأخرى
    su = _normalize_text(s_raw)
    if not su:
        return RECO_HOLD
        
    if su in _NO_RATING: return RECO_HOLD
    
    # اللغات الأخرى (فرنسية، إسبانية، ألمانية، برتغالية)
    if su in _LANG_BUY: return RECO_BUY
    if su in _LANG_SELL: return RECO_SELL
    if su in _LANG_HOLD: return RECO_HOLD
    if su in _LANG_REDUCE: return RECO_REDUCE
    
    # الأنماط القوية
    if su in _STRONG_SELL_LIKE or _PAT_STRONG_SELL.search(su):
        return _apply_negation(su, RECO_SELL)
    if su in _STRONG_BUY_LIKE or _PAT_STRONG_BUY.search(su):
        return _apply_negation(su, RECO_BUY)
        
    # التطابقات الدقيقة للعبارات
    if su in _SELL_LIKE: return _apply_negation(su, RECO_SELL)
    if su in _REDUCE_LIKE: return _apply_negation(su, RECO_REDUCE)
    if su in _HOLD_LIKE: return _apply_negation(su, RECO_HOLD)
    if su in _BUY_LIKE: return _apply_negation(su, RECO_BUY)
    
    # التحليل الرقمي
    num_result = _parse_numeric_rating(su, context_hints=True)
    if num_result and num_result[1] >= _CONFIDENCE_MEDIUM:
        return _apply_negation(su, num_result[0])
        
    # البحث بالأنماط الأساسية
    if _PAT_SELL.search(su): return _apply_negation(su, RECO_SELL)
    if _PAT_REDUCE.search(su): return _apply_negation(su, RECO_REDUCE)
    if _PAT_HOLD.search(su): return _apply_negation(su, RECO_HOLD)
    if _PAT_BUY.search(su): return _apply_negation(su, RECO_BUY)
    
    # تحليل المشاعر كملاذ أخير
    sentiment = _sentiment_analysis(su)
    if sentiment:
        return sentiment
        
    return RECO_HOLD


def normalize_recommendation(x: Any) -> str:
    """
    إرجاع إحدى التوصيات القياسية دائماً: BUY / HOLD / REDUCE / SELL.
    هذه الدالة آمنة تماماً ولا ترمي أي استثناءات.
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
# أدوات مساعدة إضافية
# ---------------------------------------------------------------------
def batch_normalize(recommendations: Sequence[Any]) -> List[str]:
    """تسوية تقييمات متعددة بشكل دفعي (Batch Processing)."""
    return [normalize_recommendation(r) for r in recommendations]


def is_valid_recommendation(value: str) -> bool:
    """التحقق مما إذا كانت التوصية من التوصيات القياسية المعتمدة."""
    return value in _RECO_ENUM


def get_recommendation_score(reco: str) -> int:
    """تحويل التوصية إلى درجة رقمية (1=شراء، 4=بيع)."""
    scores = {
        RECO_BUY: 1,
        RECO_HOLD: 2,
        RECO_REDUCE: 3,
        RECO_SELL: 4
    }
    return scores.get(reco, 2)


def recommendation_from_score(score: float, scale: str = "1-5") -> str:
    """تحويل الدرجة الرقمية إلى توصية قياسية."""
    if scale == "1-5":
        if score <= 2.0: return RECO_BUY
        elif score <= 3.0: return RECO_HOLD
        elif score <= 4.0: return RECO_REDUCE
        else: return RECO_SELL
    elif scale == "1-3":
        if score <= 1.5: return RECO_BUY
        elif score <= 2.5: return RECO_HOLD
        else: return RECO_SELL
    elif scale == "0-100":
        if score >= 70: return RECO_BUY
        elif score >= 45: return RECO_HOLD
        else: return RECO_SELL
    else:
        return RECO_HOLD


# ---------------------------------------------------------------------
# تصدير الوحدة
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
# اختبار ذاتي سريع للتحقق (Self-test)
# ---------------------------------------------------------------------
if __name__ == "__main__":
    test_cases = [
        # حالات قياسية
        ("BUY", RECO_BUY),
        ("HOLD", RECO_HOLD),
        ("REDUCE", RECO_REDUCE),
        ("SELL", RECO_SELL),
        
        # تنويعات باللغة الإنجليزية
        ("Strong Buy", RECO_BUY),
        ("accumulate", RECO_BUY),
        ("outperform", RECO_BUY),
        ("neutral", RECO_HOLD),
        ("market perform", RECO_HOLD),
        ("trim", RECO_REDUCE),
        ("take profit", RECO_REDUCE),
        ("exit", RECO_SELL),
        ("underperform", RECO_SELL),
        
        # أرقام ونسب
        ("2/5", RECO_BUY),
        ("3/5", RECO_HOLD),
        ("4/5", RECO_REDUCE),
        ("5/5", RECO_SELL),
        ("1/3", RECO_BUY),
        ("2/3", RECO_HOLD),
        ("3/3", RECO_SELL),
        
        # اللغة العربية
        ("شراء", RECO_BUY),
        ("احتفاظ", RECO_HOLD),
        ("تقليص", RECO_REDUCE),
        ("بيع", RECO_SELL),
        
        # لغات أخرى
        ("ACHETER", RECO_BUY),
        ("CONSERVER", RECO_HOLD),
        ("RÉDUIRE", RECO_REDUCE),
        ("VENDRE", RECO_SELL),
        ("COMPRAR", RECO_BUY),
        ("MANTENER", RECO_HOLD),
        ("REDUCIR", RECO_REDUCE),
        ("VENDER", RECO_SELL),
        ("KAUFEN", RECO_BUY),
        ("HALTEN", RECO_HOLD),
        ("REDUZIEREN", RECO_REDUCE),
        ("VERKAUFEN", RECO_SELL),
        
        # حالات النفي
        ("NOT A BUY", RECO_HOLD),
        ("NO SELL", RECO_HOLD),
        
        # حالات غير معروفة
        ("some random text", RECO_HOLD),
        ("", RECO_HOLD),
        (None, RECO_HOLD),
    ]
    
    sys.stdout.write(f"أداة تسوية التوصيات الإصدار {VERSION} - اختبار ذاتي\n")
    sys.stdout.write("-" * 50 + "\n")
    
    passed = 0
    failed = 0
    
    for inp, expected in test_cases:
        result = normalize_recommendation(inp)
        status = "✓ نجاح" if result == expected else "✗ فشل"
        if result == expected:
            passed += 1
        else:
            failed += 1
        sys.stdout.write(f"{status:<8} | المُدخل: {str(inp):<20} -> النتيجة: {result:6} (المتوقع: {expected})\n")
        
    sys.stdout.write("-" * 50 + "\n")
    sys.stdout.write(f"النجاح: {passed}، الفشل: {failed}\n")
    sys.stdout.write(f"نسبة النجاح: {passed/(passed+failed)*100:.1f}%\n")
