# core/reco_normalize.py  (FULL REPLACEMENT)
"""
core/reco_normalize.py
------------------------------------------------------------
Recommendation Normalization — v1.2.0 (PROD SAFE)

Purpose
- Provide ONE canonical recommendation enum across the whole app.
- Make provider outputs consistent for Sheets + APIs.
- Keep logic simple, deterministic, and safe.

Canonical output (ALWAYS)
- BUY / HOLD / REDUCE / SELL

Rules (high level)
- Missing/blank/unrecognized => HOLD
- Supports common broker/provider labels (incl. "Overweight/Underweight", etc.)
- Supports numeric rating scales (best-effort):
    * 1..5  => 1=BUY, 2=BUY, 3=HOLD, 4=SELL, 5=SELL
    * 1..3  => 1=BUY, 2=HOLD, 3=SELL
- Supports dict/list inputs (best-effort extraction)
- Includes minimal Arabic mappings (useful for KSA contexts)

Notes (v1.2.0)
- More robust Arabic matching (contains + normalization) while still conservative.
- Better numeric parsing: handles "3/5", "4 of 5", "2.5", "2,0", "rating 4" safely.
- Prevents misclassifying confidence/probability 0..1 as rating even without explicit keywords.
"""

from __future__ import annotations

import re
from typing import Any, Optional, Sequence

VERSION = "1.2.0"

_RECO_ENUM = ("BUY", "HOLD", "REDUCE", "SELL")

# Common "no rating" / "not applicable" tokens => HOLD
_NO_RATING = {
    "NA",
    "N/A",
    "NONE",
    "NULL",
    "UNKNOWN",
    "UNRATED",
    "NOT RATED",
    "NO RATING",
    "NR",
    "N R",
    "SUSPENDED",
    "UNDER REVIEW",
    "NOT COVERED",
    "NO COVERAGE",
    "NOT APPLICABLE",
    "NIL",
}

# Exact phrase mappings (after normalization)
_BUY_LIKE = {
    "STRONG BUY",
    "BUY",
    "ACCUMULATE",
    "ADD",
    "OUTPERFORM",
    "MARKET OUTPERFORM",
    "SECTOR OUTPERFORM",
    "OVERWEIGHT",
    "INCREASE",
    "UPGRADE",
    "TOP PICK",
    "LONG",
    "BULLISH",
    "POSITIVE",
    "BUYING",
    "SPECULATIVE BUY",
    "CONVICTION BUY",
    "RECOMMENDED BUY",
}

_HOLD_LIKE = {
    "HOLD",
    "NEUTRAL",
    "MAINTAIN",
    "UNCHANGED",
    "MARKET PERFORM",
    "SECTOR PERFORM",
    "IN LINE",
    "EQUAL WEIGHT",
    "EQUALWEIGHT",
    "FAIR VALUE",
    "FAIRLY VALUED",
    "WAIT",
    "KEEP",
    "STABLE",
    "WATCH",
    "MONITOR",
    "NO ACTION",
}

_REDUCE_LIKE = {
    "REDUCE",
    "TRIM",
    "LIGHTEN",
    "PARTIAL SELL",
    "TAKE PROFIT",
    "TAKE PROFITS",
    "UNDERWEIGHT",
    "DECREASE",
    "WEAKEN",
    "CAUTIOUS",
    "PROFIT TAKING",
}

_SELL_LIKE = {
    "SELL",
    "STRONG SELL",
    "EXIT",
    "AVOID",
    "UNDERPERFORM",
    "MARKET UNDERPERFORM",
    "SECTOR UNDERPERFORM",
    "SHORT",
    "BEARISH",
    "NEGATIVE",
    "SELLING",
    "DOWNGRADE",
    "RED FLAG",
}

# Minimal Arabic mappings (best-effort)
# Note: we intentionally keep this small and conservative.
_AR_BUY = {"شراء", "اشتر", "اشترِ", "تجميع", "زيادة", "ايجابي", "إيجابي", "فرصة شراء"}
_AR_HOLD = {"احتفاظ", "محايد", "انتظار", "مراقبة", "ثبات", "استقرار", "تمسك"}
_AR_REDUCE = {"تقليص", "تخفيف", "جني ارباح", "جني أرباح", "تقليل", "تخفيض"}
_AR_SELL = {"بيع", "تخارج", "سلبي", "سلبى", "تجنب", "خروج"}

# Patterns (contains) — ordered by severity
_PAT_SELL = re.compile(r"\b(SELL|EXIT|AVOID|UNDERPERFORM|SHORT|DOWNGRADE)\b")
_PAT_REDUCE = re.compile(r"\b(REDUCE|TRIM|TAKE PROFIT|TAKE PROFITS|UNDERWEIGHT|LIGHTEN|DECREASE)\b")
_PAT_HOLD = re.compile(
    r"\b(HOLD|NEUTRAL|MAINTAIN|EQUAL WEIGHT|MARKET PERFORM|SECTOR PERFORM|IN LINE|FAIR VALUE|WAIT|WATCH|MONITOR)\b"
)
_PAT_BUY = re.compile(r"\b(BUY|ACCUMULATE|ADD|OUTPERFORM|OVERWEIGHT|UPGRADE|TOP PICK|LONG)\b")

# Numeric / rating hints
_PAT_RATING_HINT = re.compile(r"\b(RATING|SCORE|RANK|RECOMMENDATION)\b")
_PAT_RATIO = re.compile(r"(\d+(?:[.,]\d+)?)\s*(/|OF)\s*(\d+(?:[.,]\d+)?)")


def _first_non_empty_str(items: Sequence[Any]) -> Optional[str]:
    for it in items:
        if it is None:
            continue
        try:
            s = str(it).strip()
        except Exception:
            continue
        if s:
            return s
    return None


def _extract_candidate(x: Any) -> Any:
    """
    Best-effort extraction for structured inputs:
    - dict: try typical keys
    - list/tuple: take first non-empty
    Otherwise return x.
    """
    if x is None:
        return None

    if isinstance(x, dict):
        for k in (
            "recommendation",
            "reco",
            "rating",
            "action",
            "signal",
            "analyst_rating",
            "analystRecommendation",
            "analyst_recommendation",
        ):
            v = x.get(k)
            if v is not None and str(v).strip():
                return v
        # fall back to any non-empty string value
        v2 = _first_non_empty_str(list(x.values()))
        return v2 if v2 is not None else None

    if isinstance(x, (list, tuple)):
        v = _first_non_empty_str(x)
        return v if v is not None else None

    return x


def _normalize_arabic(s: str) -> str:
    """
    Light Arabic normalization (conservative):
    - trim, collapse spaces
    - normalize Arabic alef variants minimally
    """
    s = (s or "").strip()
    if not s:
        return ""
    s = re.sub(r"\s+", " ", s).strip()
    # Normalize common alef forms to "ا" (very light)
    s = s.replace("أ", "ا").replace("إ", "ا").replace("آ", "ا")
    return s


def _try_parse_numeric_rating(s: str) -> Optional[str]:
    """
    Parse numeric-style ratings, e.g.:
      "1", "2", "3", "4", "5"
      "1/5", "4 of 5", "2.0", "3.0/5"
      "rating: 4"
    Returns canonical enum or None if not parsed.
    """
    if not s:
        return None

    su = s.upper()

    # If it clearly looks like a probability/confidence, do not treat as rating.
    # Covers cases like "0.73", "0.8" when accompanied by common context words.
    if re.search(r"\b(CONFIDENCE|PROB|PROBABILITY|CHANCE|LIKELIHOOD)\b", su):
        if re.search(r"\b0\.\d+|\b1\.0\b|\b0\b", su):
            return None

    # If it's a ratio "x/y" or "x of y", prefer that
    mratio = _PAT_RATIO.search(su)
    if mratio:
        try:
            x = float(mratio.group(1).replace(",", "."))
            y = float(mratio.group(3).replace(",", "."))
        except Exception:
            x = None
            y = None

        if x is not None and y is not None and y > 0:
            # Typical 1..5 or 1..3 scales (1 best)
            # We treat x as the rating value, y as max.
            # If y is 5 or 3, map directly.
            n = int(round(x))
            if int(round(y)) == 5:
                if n <= 2:
                    return "BUY"
                if n == 3:
                    return "HOLD"
                return "SELL"
            if int(round(y)) == 3:
                if n == 1:
                    return "BUY"
                if n == 2:
                    return "HOLD"
                return "SELL"
            # Unknown max: do not guess
            return None

    # Otherwise, grab first number
    m = re.search(r"(-?\d+(?:[.,]\d+)?)", su)
    if not m:
        return None

    try:
        num = float(m.group(1).replace(",", "."))
    except Exception:
        return None

    # Avoid treating 0..1 as rating unless strong hint exists
    if 0.0 <= num <= 1.0 and not _PAT_RATING_HINT.search(su):
        return None

    # Common broker scale 1..5 (1 best)
    if 1.0 <= num <= 5.0:
        n = int(round(num))
        if n <= 2:
            return "BUY"
        if n == 3:
            return "HOLD"
        return "SELL"

    # Some providers use 1..3 (1 best)
    if 1.0 <= num <= 3.0:
        n = int(round(num))
        if n == 1:
            return "BUY"
        if n == 2:
            return "HOLD"
        return "SELL"

    return None


def _normalize_text(s: str) -> str:
    """
    Normalize separators, punctuation, and whitespace (English focus).
    """
    s = (s or "").strip()
    if not s:
        return ""

    su = s.upper()

    # Replace typical separators with spaces
    su = re.sub(r"[\t\r\n]+", " ", su)
    su = re.sub(r"[\-_/]+", " ", su)

    # Remove brackets content markers but keep words
    su = su.replace("(", " ").replace(")", " ").replace("[", " ").replace("]", " ").replace("{", " ").replace("}", " ")

    # Collapse whitespace
    su = re.sub(r"\s+", " ", su).strip()

    return su


def normalize_recommendation(x: Any) -> str:
    """
    Always returns one of: BUY / HOLD / REDUCE / SELL (uppercase).
    Never raises.
    """
    try:
        x2 = _extract_candidate(x)
        if x2 is None:
            return "HOLD"

        # Fast path: already canonical
        if isinstance(x2, str):
            s_raw = x2.strip()
            if not s_raw:
                return "HOLD"
            if s_raw.upper() in _RECO_ENUM:
                return s_raw.upper()
        else:
            # numeric direct (int/float)
            if isinstance(x2, (int, float)) and not isinstance(x2, bool):
                out_num = _try_parse_numeric_rating(str(x2))
                return out_num or "HOLD"

        # Arabic mapping (check before English normalization)
        try:
            s_ar_raw = str(x2).strip()
        except Exception:
            s_ar_raw = ""

        if s_ar_raw:
            s_ar = _normalize_arabic(s_ar_raw)
            if s_ar in _AR_BUY:
                return "BUY"
            if s_ar in _AR_HOLD:
                return "HOLD"
            if s_ar in _AR_REDUCE:
                return "REDUCE"
            if s_ar in _AR_SELL:
                return "SELL"

            # Conservative contains (Arabic) — only if clearly present
            # (avoid over-mapping long Arabic sentences)
            if any(tok in s_ar for tok in ("شراء", "اشتر", "تجميع", "زيادة")):
                return "BUY"
            if any(tok in s_ar for tok in ("بيع", "تخارج", "تجنب")):
                return "SELL"
            if any(tok in s_ar for tok in ("جني ارباح", "جني ارباح", "تخفيف", "تقليص", "تقليل")):
                return "REDUCE"
            if any(tok in s_ar for tok in ("احتفاظ", "محايد", "انتظار", "مراقبة", "استقرار")):
                return "HOLD"

        s = _normalize_text(str(x2))
        if not s:
            return "HOLD"

        # No-rating bucket
        if s in _NO_RATING:
            return "HOLD"

        # Exact phrase maps
        if s in _BUY_LIKE:
            return "BUY"
        if s in _HOLD_LIKE:
            return "HOLD"
        if s in _REDUCE_LIKE:
            return "REDUCE"
        if s in _SELL_LIKE:
            return "SELL"

        # Numeric embedded in text (e.g., "Rating: 4/5")
        out_num = _try_parse_numeric_rating(s)
        if out_num is not None and (_PAT_RATING_HINT.search(s) or _PAT_RATIO.search(s)):
            return out_num

        # Heuristics (contains) — ordered by severity
        if _PAT_SELL.search(s):
            return "SELL"
        if _PAT_REDUCE.search(s):
            return "REDUCE"
        if _PAT_HOLD.search(s):
            return "HOLD"
        if _PAT_BUY.search(s):
            return "BUY"

        return "HOLD"
    except Exception:
        return "HOLD"


__all__ = ["normalize_recommendation", "VERSION"]
