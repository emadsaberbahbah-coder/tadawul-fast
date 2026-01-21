# core/reco_normalize.py  (FULL REPLACEMENT)
"""
core/reco_normalize.py
------------------------------------------------------------
Recommendation Normalization — v1.1.0 (PROD SAFE)

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
"""

from __future__ import annotations

import re
from typing import Any, Optional, Sequence

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
}

# Minimal Arabic mappings (best-effort)
# Note: we intentionally keep this small and conservative.
_AR_BUY = {"شراء", "اشتر", "اشترِ", "تجميع", "زيادة", "ايجابي", "إيجابي"}
_AR_HOLD = {"احتفاظ", "محايد", "انتظار", "مراقبة", "ثبات", "استقرار"}
_AR_REDUCE = {"تقليص", "تخفيف", "جني ارباح", "جني أرباح"}
_AR_SELL = {"بيع", "تخارج", "سلبي", "سلبى", "تجنب"}


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


def _try_parse_numeric_rating(s: str) -> Optional[str]:
    """
    Parse numeric-style ratings, e.g.:
      "1", "2", "3", "4", "5"
      "1/5", "4 of 5", "2.0", "3.0/5"
      "rating: 4"
    Returns canonical enum or None if not parsed.
    """
    # Grab the first number that looks like a rating
    m = re.search(r"(-?\d+(?:\.\d+)?)", s)
    if not m:
        return None

    try:
        num = float(m.group(1))
    except Exception:
        return None

    # If it clearly looks like a percent/confidence (0..1) => do not treat as reco rating
    # (avoid mapping 0.7 to HOLD/BUY, etc.)
    if 0.0 <= num <= 1.0 and re.search(r"\b(CONFIDENCE|PROB|PROBABILITY)\b", s):
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
    Normalize separators, punctuation, camel-ish blobs, and whitespace.
    """
    s = (s or "").strip()
    if not s:
        return ""

    # Keep Arabic text as-is for Arabic mapping checks
    # For English tokens, work in uppercase
    su = s.upper()

    # Replace typical separators with spaces
    su = re.sub(r"[\t\r\n]+", " ", su)
    su = re.sub(r"[\-_/]+", " ", su)

    # Remove brackets/parentheses content markers but keep words
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
                # Treat common numeric scales
                s_num = str(x2)
                out_num = _try_parse_numeric_rating(s_num)
                return out_num or "HOLD"

        # Arabic mapping (check before upper normalization destroys Arabic)
        try:
            s_ar = str(x2).strip()
        except Exception:
            s_ar = ""
        if s_ar:
            s_ar_clean = re.sub(r"\s+", " ", s_ar).strip()
            if s_ar_clean in _AR_BUY:
                return "BUY"
            if s_ar_clean in _AR_HOLD:
                return "HOLD"
            if s_ar_clean in _AR_REDUCE:
                return "REDUCE"
            if s_ar_clean in _AR_SELL:
                return "SELL"

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
        if out_num is not None and re.search(r"\b(RATING|SCORE|RANK)\b|\d+\s*(/|OF)\s*\d+", s):
            return out_num

        # Heuristics (contains) — ordered by severity
        # SELL signals
        if re.search(r"\b(SELL|EXIT|AVOID|UNDERPERFORM|SHORT|DOWNGRADE)\b", s):
            return "SELL"

        # REDUCE signals
        if re.search(r"\b(REDUCE|TRIM|TAKE PROFIT|TAKE PROFITS|UNDERWEIGHT|LIGHTEN|DECREASE)\b", s):
            return "REDUCE"

        # HOLD signals
        if re.search(r"\b(HOLD|NEUTRAL|MAINTAIN|EQUAL WEIGHT|MARKET PERFORM|SECTOR PERFORM|IN LINE|FAIR VALUE|WAIT)\b", s):
            return "HOLD"

        # BUY signals
        if re.search(r"\b(BUY|ACCUMULATE|ADD|OUTPERFORM|OVERWEIGHT|UPGRADE|TOP PICK|LONG)\b", s):
            return "BUY"

        return "HOLD"
    except Exception:
        return "HOLD"


__all__ = ["normalize_recommendation"]
