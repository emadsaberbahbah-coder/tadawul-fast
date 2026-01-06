# core/reco_normalize.py  (FULL REPLACEMENT)
"""
core/reco_normalize.py
------------------------------------------------------------
Recommendation Normalization Utility — v1.2.0 (PROD SAFE)

Central logic to standardize broker/analyst ratings into a
strict 4-state enum: BUY, HOLD, REDUCE, SELL.

Upgrades vs v1.1.0
- ✅ Handles common compact forms: BUY+, SELL-, B/U/S, etc. (best-effort).
- ✅ More synonyms: "Overweight/Underweight", "Perform", "Sector", "Speculative".
- ✅ Stronger cleaning (keeps + / - for heuristic, then strips safely).
- ✅ Guaranteed output always in ALLOWED_RECOS; default coerced to HOLD if invalid.

Zero deps, safe to import anywhere.

Usage:
    from core.reco_normalize import normalize_recommendation
    normalize_recommendation("Strong Buy")   -> "BUY"
    normalize_recommendation("Underweight")  -> "REDUCE"
    normalize_recommendation(None)           -> "HOLD"
"""

from __future__ import annotations

import re
from typing import Any, Set

# Canonical output set
ALLOWED_RECOS: Set[str] = {"BUY", "HOLD", "REDUCE", "SELL"}

# Cleaners
# 1) Normalize separators to spaces (keep + and - for heuristic stage)
_SEP_RE = re.compile(r"[\s_/\\|]+")
# 2) Collapse multiple spaces
_SPACE_RE = re.compile(r"\s+")
# 3) Remove non-word/non-space after heuristic stage (keeps letters/numbers/spaces)
_STRIP_RE = re.compile(r"[^A-Z0-9 ]+")

# Synonym mappings (normalized input -> canonical output)
_BUY_LIKE = {
    "STRONG BUY",
    "BUY",
    "ACCUMULATE",
    "ADD",
    "OUTPERFORM",
    "OVERWEIGHT",
    "LONG",
    "POSITIVE",
    "MARKET OUTPERFORM",
    "SECTOR OUTPERFORM",
    "TOP PICK",
    "CONVICTION BUY",
    "SPEC BUY",
    "SPECULATIVE BUY",
    "TRADING BUY",
    "BUY LIST",
    "RECOMMENDED BUY",
    "UPGRADE",
    "BULLISH",
    "INCREASE",
    "INCREASE POSITION",
}

_HOLD_LIKE = {
    "HOLD",
    "NEUTRAL",
    "MAINTAIN",
    "MARKET PERFORM",
    "SECTOR PERFORM",
    "EQUAL WEIGHT",
    "EQUALWEIGHT",
    "WAIT",
    "KEEP",
    "IN LINE",
    "INLINE",
    "PEER PERFORM",
    "PERFORM",
    "FAIR VALUE",
    "FAIRVALUE",
    "UNCHANGED",
    "NO ACTION",
    "NOACTION",
    "STABLE",
    "RANGE",
}

_REDUCE_LIKE = {
    "REDUCE",
    "TRIM",
    "LIGHTEN",
    "UNDERWEIGHT",
    "PARTIAL SELL",
    "PARTIALSELL",
    "TAKE PROFIT",
    "TAKE PROFITS",
    "TAKEPROFIT",
    "TAKEPROFITS",
    "WEAK HOLD",
    "UNDERPERFORM",
    "MODERATE SELL",
    "SELL SOME",
    "DECREASE",
    "DECREASE POSITION",
    "DOWNGRADE",
    "BEARISH",
}

_SELL_LIKE = {
    "SELL",
    "STRONG SELL",
    "EXIT",
    "AVOID",
    "SHORT",
    "NEGATIVE",
    "SELL OFF",
    "SELL ALL",
    "DUMP",
}

# Compact/abbrev signals (best-effort)
# Note: we keep these conservative to avoid misclassifying.
_ABBREV_BUY = {"B", "BUY+", "B+", "BUY PLUS"}
_ABBREV_SELL = {"S", "SELL-", "S-", "SELL MINUS"}
_ABBREV_HOLD = {"H"}
_ABBREV_REDUCE = {"R"}


def _safe_default(default: Any) -> str:
    d = str(default or "").strip().upper()
    return d if d in ALLOWED_RECOS else "HOLD"


def _normalize_spaces(s: str) -> str:
    s = _SEP_RE.sub(" ", s)
    s = _SPACE_RE.sub(" ", s).strip()
    return s


def normalize_recommendation(x: Any, default: str = "HOLD") -> str:
    """
    Normalize any recommendation string/object to one of:
    BUY, HOLD, REDUCE, SELL.

    If input is None or unrecognizable, returns `default` (coerced to HOLD if invalid).
    """
    dflt = _safe_default(default)

    if x is None:
        return dflt

    try:
        raw = str(x).strip().upper()
    except Exception:
        return dflt

    if not raw:
        return dflt

    # Fast path: already canonical
    if raw in ALLOWED_RECOS:
        return raw

    # Heuristic stage for + / - and compact forms (before stripping)
    raw2 = _normalize_spaces(raw)

    # Common compact forms
    if raw2 in _ABBREV_BUY:
        return "BUY"
    if raw2 in _ABBREV_SELL:
        return "SELL"
    if raw2 in _ABBREV_HOLD:
        return "HOLD"
    if raw2 in _ABBREV_REDUCE:
        return "REDUCE"

    # Handle e.g. "BUY+" / "SELL-" anywhere
    if "BUY+" in raw2 or raw2.endswith("+") and "BUY" in raw2:
        return "BUY"
    if "SELL-" in raw2 or raw2.endswith("-") and "SELL" in raw2:
        return "SELL"

    # Now strip punctuation safely and normalize spaces again
    norm = _STRIP_RE.sub(" ", raw2)
    norm = _normalize_spaces(norm)

    # Direct synonym lookup
    if norm in _BUY_LIKE:
        return "BUY"
    if norm in _HOLD_LIKE:
        return "HOLD"
    if norm in _REDUCE_LIKE:
        return "REDUCE"
    if norm in _SELL_LIKE:
        return "SELL"

    # Token-based heuristic fallbacks for longer strings
    # (Ordered from strongest sell to buy to reduce to hold)
    if "STRONG" in norm and "SELL" in norm:
        return "SELL"
    if "STRONG" in norm and "BUY" in norm:
        return "BUY"

    # Primary keyword hits
    if "SELL" in norm or "AVOID" in norm or "EXIT" in norm or "SHORT" in norm:
        return "SELL"
    if "UNDERWEIGHT" in norm or "UNDERPERFORM" in norm or "TRIM" in norm or "REDUCE" in norm:
        return "REDUCE"
    if "OVERWEIGHT" in norm or "OUTPERFORM" in norm or "ACCUMULATE" in norm or "ADD" in norm or "BUY" in norm:
        return "BUY"
    if "HOLD" in norm or "NEUTRAL" in norm or "MAINTAIN" in norm or "PERFORM" in norm or "IN LINE" in norm:
        return "HOLD"

    return dflt


__all__ = ["normalize_recommendation", "ALLOWED_RECOS"]
