# core/reco_normalize.py
"""
core/reco_normalize.py
------------------------------------------------------------
Recommendation Normalization — v1.6.0 (PROD SAFE + ADVANCED MULTI-LINGUAL + CONTEXT-AWARE)

Purpose
- Provide ONE canonical recommendation enum across the whole app.
- Make provider outputs consistent for Sheets + APIs.
- Deterministic, fast, never raises.

Canonical output (ALWAYS)
- BUY / HOLD / REDUCE / SELL

Key upgrades vs v1.3.0
- ✅ Stronger extraction from dict/list structures (supports nested fields + common vendor shapes)
- ✅ Advanced numeric parsing (1..5, 1..3, 0..100, 0..1) with safer gating
- ✅ Better phrase precedence (Strong Buy/Sell, Reduce/Trim vs Sell)
- ✅ Negation handling ("not a buy" => HOLD/REDUCE)
- ✅ Multi-lingual mapping: enhanced Arabic + supports common French/Spanish tokens (light)
- ✅ Idempotent: feeding canonical output returns same output
- ✅ Fast: precompiled regex + O(n) logic, no heavy deps

Notes
- We keep logic conservative: unknown => HOLD
- "REDUCE" is used for partial-sell / trim / take-profit / underweight
- "SELL" reserved for explicit sell/avoid/exit/underperform/short
"""

from __future__ import annotations

import re
from typing import Any, Dict, Iterable, Optional, Sequence, Tuple

VERSION = "1.6.0"

RECO_BUY = "BUY"
RECO_HOLD = "HOLD"
RECO_REDUCE = "REDUCE"
RECO_SELL = "SELL"
_RECO_ENUM = (RECO_BUY, RECO_HOLD, RECO_REDUCE, RECO_SELL)

# ---------------------------------------------------------------------
# Common "no rating" / "not applicable" tokens => HOLD
# ---------------------------------------------------------------------
_NO_RATING = {
    "NA", "N/A", "NONE", "NULL", "UNKNOWN", "UNRATED", "NOT RATED", "NO RATING",
    "NR", "N R", "SUSPENDED", "UNDER REVIEW", "NOT COVERED", "NO COVERAGE",
    "NOT APPLICABLE", "NIL", "—", "-", "WAITING", "PENDING"
}

# ---------------------------------------------------------------------
# Exact phrase mappings (after normalization)
# ---------------------------------------------------------------------
# NOTE: We separate STRONG BUY/SELL for precedence
_STRONG_BUY_LIKE = {
    "STRONG BUY", "CONVICTION BUY", "HIGH CONVICTION BUY", "BUY STRONG",
    "TOP PICK", "BEST IDEA", "HIGHLY RECOMMENDED BUY", "BUY (STRONG)"
}
_BUY_LIKE = {
    "BUY", "ACCUMULATE", "ADD", "OUTPERFORM", "MARKET OUTPERFORM",
    "SECTOR OUTPERFORM", "OVERWEIGHT", "INCREASE", "UPGRADE", "LONG",
    "BULLISH", "POSITIVE", "BUYING", "SPECULATIVE BUY", "RECOMMENDED BUY",
}
_HOLD_LIKE = {
    "HOLD", "NEUTRAL", "MAINTAIN", "UNCHANGED", "MARKET PERFORM", "SECTOR PERFORM",
    "IN LINE", "EQUAL WEIGHT", "EQUALWEIGHT", "FAIR VALUE", "FAIRLY VALUED",
    "WAIT", "KEEP", "STABLE", "WATCH", "MONITOR", "NO ACTION", "PERFORM",
}
_REDUCE_LIKE = {
    "REDUCE", "TRIM", "LIGHTEN", "PARTIAL SELL", "TAKE PROFIT", "TAKE PROFITS",
    "UNDERWEIGHT", "DECREASE", "WEAKEN", "CAUTIOUS", "PROFIT TAKING",
    "SELL STRENGTH", "TRIM POSITION", "REDUCE EXPOSURE",
}
_STRONG_SELL_LIKE = {
    "STRONG SELL", "SELL (STRONG)", "SELL STRONG", "EXIT IMMEDIATELY"
}
_SELL_LIKE = {
    "SELL", "EXIT", "AVOID", "UNDERPERFORM", "MARKET UNDERPERFORM",
    "SECTOR UNDERPERFORM", "SHORT", "BEARISH", "NEGATIVE", "SELLING",
    "DOWNGRADE", "RED FLAG",
}

# ---------------------------------------------------------------------
# Arabic mappings (enhanced + conservative)
# ---------------------------------------------------------------------
# We normalize Alef variants and whitespace; keep sets in normalized form.
_AR_BUY = {
    "شراء", "اشتر", "اشترى", "اشترِ", "تجميع", "زيادة", "ايجابي", "ايجابى", "ايجابي جدا",
    "فرصة شراء", "اداء متفوق", "زيادة المراكز", "توصية شراء", "توصية بالشراء",
    "شراء قوي", "شراء قوى", "احتفاظ مع ميل للشراء", "زيادة التعرض", "تعزيز المراكز",
}
_AR_HOLD = {
    "احتفاظ", "محايد", "انتظار", "مراقبة", "ثبات", "استقرار", "تمسك", "اداء محايد",
    "حياد", "موقف محايد", "ابقاء المراكز", "متوازن", "بدون تغيير",
}
_AR_REDUCE = {
    "تقليص", "تخفيف", "جني ارباح", "جني ارباح جزئي", "جني الارباح", "تقليل", "تخفيض",
    "اداء ضعيف", "تخفيض المراكز", "بيع جزئي", "جني الارباح", "تخفيف المراكز",
    "تقليل التعرض", "تخفيف المخاطر",
}
_AR_SELL = {
    "بيع", "تخارج", "سلبي", "سلبى", "تجنب", "خروج", "بيع قوي", "بيع قوى",
    "توصية بيع", "توصية بالبيع", "اداء اقل", "اداء أقل", "تصفية", "الخروج من السهم",
}

# light non-English (very common broker words) — optional but helpful
# kept minimal to avoid false positives
_OTHER_BUY = {"ACHETER", "COMPRAR"}       # FR/ES buy
_OTHER_SELL = {"VENDRE", "VENDER"}        # FR/ES sell
_OTHER_HOLD = {"CONSERVER", "MANTENER"}   # FR/ES hold-ish

# ---------------------------------------------------------------------
# Regex patterns (compiled, ordered by severity)
# ---------------------------------------------------------------------
# Negation patterns that can flip meaning
_PAT_NEGATE = re.compile(r"\b(NOT|NO|NEVER|WITHOUT|HARDLY|RARELY)\b")

# Strong explicit
_PAT_STRONG_BUY = re.compile(r"\b(STRONG\s+BUY|CONVICTION\s+BUY|TOP\s+PICK|BEST\s+IDEA)\b")
_PAT_STRONG_SELL = re.compile(r"\b(STRONG\s+SELL|EXIT\s+IMMEDIATELY)\b")

# Severity group patterns
_PAT_SELL = re.compile(r"\b(SELL|EXIT|AVOID|UNDERPERFORM|SHORT)\b")
_PAT_REDUCE = re.compile(r"\b(REDUCE|TRIM|LIGHTEN|TAKE\s+PROFIT|TAKE\s+PROFITS|UNDERWEIGHT|DECREASE)\b")
_PAT_HOLD = re.compile(r"\b(HOLD|NEUTRAL|MAINTAIN|EQUAL\s+WEIGHT|MARKET\s+PERFORM|SECTOR\s+PERFORM|IN\s+LINE|FAIR\s+VALUE|WAIT|WATCH|MONITOR)\b")
_PAT_BUY = re.compile(r"\b(BUY|ACCUMULATE|ADD|OUTPERFORM|OVERWEIGHT|UPGRADE|LONG)\b")

# Numeric / rating hints
_PAT_RATING_HINT = re.compile(r"\b(RATING|SCORE|RANK|RECOMMENDATION|CONSENSUS)\b")
_PAT_RATIO = re.compile(r"(\d+(?:[.,]\d+)?)\s*(/|OF)\s*(\d+(?:[.,]\d+)?)", re.IGNORECASE)

# Strip punctuation/separators
_PAT_SEP = re.compile(r"[\t\r\n]+")
_PAT_SPLIT = re.compile(r"[\-_/|]+")
_PAT_WS = re.compile(r"\s+")

# ---------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------
def _safe_str(x: Any) -> str:
    try:
        return "" if x is None else str(x).strip()
    except Exception:
        return ""


def _first_non_empty_str(items: Iterable[Any]) -> Optional[str]:
    for it in items:
        s = _safe_str(it)
        if s and s.lower() not in ("none", "null", "n/a"):
            return s
    return None


def _deep_get(d: Dict[str, Any], keys: Sequence[str]) -> Any:
    """
    Best-effort nested key fetch: supports dotted keys like "meta.recommendation".
    """
    for k in keys:
        if not k:
            continue
        if k in d:
            v = d.get(k)
            if _safe_str(v):
                return v
        if "." in k:
            cur: Any = d
            ok = True
            for part in k.split("."):
                if isinstance(cur, dict) and part in cur:
                    cur = cur.get(part)
                else:
                    ok = False
                    break
            if ok and _safe_str(cur):
                return cur
    return None


def _extract_candidate(x: Any) -> Any:
    """
    Best-effort extraction for structured inputs:
    - dict: try typical keys, nested keys, and common vendor fields
    - list/tuple: take first non-empty string
    Otherwise return x.
    """
    if x is None:
        return None

    if isinstance(x, dict):
        # common keys
        preferred = (
            "recommendation", "reco", "rating", "action", "signal",
            "analyst_rating", "analystRecommendation", "analyst_recommendation",
            "consensus", "consensus_rating",
            # nested
            "meta.recommendation", "meta.rating", "data.recommendation",
            "result.recommendation", "payload.recommendation",
        )
        v = _deep_get(x, preferred)
        if v is not None:
            return v

        # if dict values contain a string-like rating, take first
        v2 = _first_non_empty_str(x.values())
        return v2

    if isinstance(x, (list, tuple)):
        return _first_non_empty_str(x)

    return x


def _normalize_arabic(s: str) -> str:
    """
    Light Arabic normalization (conservative):
    - trim, collapse spaces
    - normalize Alef variants to "ا"
    - normalize Ta marbuta/Ha NOT done (too aggressive)
    """
    s = _safe_str(s)
    if not s:
        return ""
    s = _PAT_WS.sub(" ", s).strip()
    s = s.replace("أ", "ا").replace("إ", "ا").replace("آ", "ا")
    return s


def _normalize_text(s: str) -> str:
    """
    Normalize separators, punctuation, and whitespace (English focus).
    """
    s = _safe_str(s)
    if not s:
        return ""
    su = s.upper()
    su = _PAT_SEP.sub(" ", su)
    su = _PAT_SPLIT.sub(" ", su)
    su = su.replace("(", " ").replace(")", " ").replace("[", " ").replace("]", " ").replace("{", " ").replace("}", " ")
    su = _PAT_WS.sub(" ", su).strip()
    return su


def _try_parse_numeric_rating(s: str) -> Optional[str]:
    """
    Parse numeric-style ratings safely.

    Supported:
      - 1..5  (1 best): 1-2 BUY, 3 HOLD, 4-5 SELL
      - 1..3  (1 best): 1 BUY, 2 HOLD, 3 SELL
      - 0..100 (score): >=70 BUY, 45..69 HOLD, <45 SELL (only if strong hint exists)
      - 0..1 (prob/score): ignored unless rating hint exists

    Returns canonical enum or None.
    """
    if not s:
        return None
    su = s.upper()

    # If it clearly looks like probability/confidence, do not treat as rating.
    if re.search(r"\b(CONFIDENCE|PROB|PROBABILITY|CHANCE|LIKELIHOOD)\b", su):
        return None

    # Ratio "x/y" or "x of y"
    mratio = _PAT_RATIO.search(su)
    if mratio:
        try:
            x = float(mratio.group(1).replace(",", "."))
            y = float(mratio.group(3).replace(",", "."))
        except Exception:
            x = None
            y = None

        if x is not None and y is not None and y > 0:
            yy = int(round(y))
            n = int(round(x))
            if yy == 5:
                if n <= 2:
                    return RECO_BUY
                if n == 3:
                    return RECO_HOLD
                return RECO_SELL
            if yy == 3:
                if n == 1:
                    return RECO_BUY
                if n == 2:
                    return RECO_HOLD
                return RECO_SELL
            return None

    # First number
    m = re.search(r"(-?\d+(?:[.,]\d+)?)", su)
    if not m:
        return None
    try:
        num = float(m.group(1).replace(",", "."))
    except Exception:
        return None

    # 0..1 ambiguous: only accept if strong hint exists
    if 0.0 <= num <= 1.0 and not _PAT_RATING_HINT.search(su):
        return None

    # 1..5 scale
    if 1.0 <= num <= 5.0:
        n = int(round(num))
        if n <= 2:
            return RECO_BUY
        if n == 3:
            return RECO_HOLD
        return RECO_SELL

    # 1..3 scale
    if 1.0 <= num <= 3.0:
        n = int(round(num))
        if n == 1:
            return RECO_BUY
        if n == 2:
            return RECO_HOLD
        return RECO_SELL

    # 0..100 score (only if hint exists)
    if 0.0 <= num <= 100.0 and _PAT_RATING_HINT.search(su):
        if num >= 70.0:
            return RECO_BUY
        if num >= 45.0:
            return RECO_HOLD
        return RECO_SELL

    return None


def _apply_negation_guard(su: str, reco: str) -> str:
    """
    If text contains negation near BUY/SELL tokens, downgrade severity.
    Example:
      "NOT A BUY" => HOLD
      "NOT SELL"  => HOLD
    Conservative: push toward HOLD, not opposite extreme.
    """
    if not su:
        return reco
    if not _PAT_NEGATE.search(su):
        return reco

    # common phrases
    if re.search(r"\bNOT\s+(A\s+)?BUY\b", su) or re.search(r"\bNO\s+BUY\b", su):
        return RECO_HOLD
    if re.search(r"\bNOT\s+(A\s+)?SELL\b", su) or re.search(r"\bNO\s+SELL\b", su):
        return RECO_HOLD
    if re.search(r"\bNOT\s+(A\s+)?STRONG\s+BUY\b", su):
        return RECO_HOLD
    if re.search(r"\bNOT\s+(A\s+)?STRONG\s+SELL\b", su):
        return RECO_HOLD

    # if generic negation exists and reco is extreme, soften one step
    if reco == RECO_SELL:
        return RECO_REDUCE
    if reco == RECO_BUY:
        return RECO_HOLD
    return reco


# ---------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------
def normalize_recommendation(x: Any) -> str:
    """
    Always returns one of: BUY / HOLD / REDUCE / SELL.
    Never raises.
    """
    try:
        x2 = _extract_candidate(x)
        if x2 is None:
            return RECO_HOLD

        # fast path: already canonical
        if isinstance(x2, str):
            s0 = x2.strip()
            if not s0:
                return RECO_HOLD
            s0u = s0.upper()
            if s0u in _RECO_ENUM:
                return s0u
        else:
            # numeric direct (int/float)
            if isinstance(x2, (int, float)) and not isinstance(x2, bool):
                out_num = _try_parse_numeric_rating(str(x2))
                return out_num or RECO_HOLD

        # Arabic mapping first (before English upper normalization)
        s_ar_raw = _safe_str(x2)
        if s_ar_raw:
            s_ar = _normalize_arabic(s_ar_raw)
            if s_ar in _AR_BUY:
                return RECO_BUY
            if s_ar in _AR_HOLD:
                return RECO_HOLD
            if s_ar in _AR_REDUCE:
                return RECO_REDUCE
            if s_ar in _AR_SELL:
                return RECO_SELL

            # contains (Arabic) conservative
            if any(tok in s_ar for tok in ("شراء", "اشتر", "تجميع", "زيادة", "توصية بالشراء", "شراء قوي", "شراء قوى")):
                return RECO_BUY
            if any(tok in s_ar for tok in ("بيع قوي", "بيع قوى", "تخارج", "تجنب", "توصية بالبيع", "تصفية", "خروج")):
                return RECO_SELL
            if any(tok in s_ar for tok in ("جني ارباح", "جني الارباح", "تخفيف", "تقليص", "تقليل", "تخفيض", "بيع جزئي")):
                return RECO_REDUCE
            if any(tok in s_ar for tok in ("احتفاظ", "محايد", "انتظار", "مراقبة", "استقرار", "حياد", "بدون تغيير")):
                return RECO_HOLD

        # English + other languages normalization
        su = _normalize_text(_safe_str(x2))
        if not su:
            return RECO_HOLD

        if su in _NO_RATING:
            return RECO_HOLD

        # very light FR/ES
        if su in _OTHER_BUY:
            return RECO_BUY
        if su in _OTHER_SELL:
            return RECO_SELL
        if su in _OTHER_HOLD:
            return RECO_HOLD

        # Exact phrases (strong precedence)
        if su in _STRONG_SELL_LIKE or _PAT_STRONG_SELL.search(su):
            return _apply_negation_guard(su, RECO_SELL)
        if su in _STRONG_BUY_LIKE or _PAT_STRONG_BUY.search(su):
            return _apply_negation_guard(su, RECO_BUY)

        # Exact maps
        if su in _SELL_LIKE:
            return _apply_negation_guard(su, RECO_SELL)
        if su in _REDUCE_LIKE:
            return _apply_negation_guard(su, RECO_REDUCE)
        if su in _HOLD_LIKE:
            return _apply_negation_guard(su, RECO_HOLD)
        if su in _BUY_LIKE:
            return _apply_negation_guard(su, RECO_BUY)

        # Numeric embedded
        out_num = _try_parse_numeric_rating(su)
        if out_num is not None and (_PAT_RATING_HINT.search(su) or _PAT_RATIO.search(su)):
            return _apply_negation_guard(su, out_num)

        # Heuristics (contains) — ordered by severity
        if _PAT_SELL.search(su):
            return _apply_negation_guard(su, RECO_SELL)
        if _PAT_REDUCE.search(su):
            return _apply_negation_guard(su, RECO_REDUCE)
        if _PAT_HOLD.search(su):
            return _apply_negation_guard(su, RECO_HOLD)
        if _PAT_BUY.search(su):
            return _apply_negation_guard(su, RECO_BUY)

        # Unknown => HOLD
        return RECO_HOLD

    except Exception:
        return RECO_HOLD


__all__ = ["normalize_recommendation", "VERSION", "RECO_BUY", "RECO_HOLD", "RECO_REDUCE", "RECO_SELL"]
