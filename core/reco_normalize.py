# core/reco_normalize.py
"""
core/reco_normalize.py
------------------------------------------------------------
Recommendation Normalization Utility — v1.1.0 (PROD SAFE)

Central logic to standardize broker/analyst ratings into a
strict 4-state enum: BUY, HOLD, REDUCE, SELL.

Features:
- Robust normalization (handles case, whitespace, underscores, hyphens).
- Extensive synonym mapping (Accumulate, Neutral, Underweight, etc.).
- Safe fallback (defaults to HOLD if unknown/empty).
- Zero dependencies (pure Python).

Usage:
    from core.reco_normalize import normalize_recommendation
    reco = normalize_recommendation("Strong Buy")  # -> "BUY"
"""

from __future__ import annotations

import re
from typing import Any, Optional

# The canonical output set
ALLOWED_RECOS = {"BUY", "HOLD", "REDUCE", "SELL"}

# Regex to clean input: replaces multiple spaces/underscores/hyphens/slashes with a single space
_CLEANER_RE = re.compile(r"[\s\-_/]+")

# Synonym mappings (normalized uppercase input -> canonical output)
_BUY_LIKE = {
    "STRONG BUY", "BUY", "ACCUMULATE", "ADD", "OUTPERFORM", "OVERWEIGHT", "LONG",
    "POSITIVE", "MARKET OUTPERFORM", "STRONGBUY", "STRONG_BUY"
}

_HOLD_LIKE = {
    "HOLD", "NEUTRAL", "MAINTAIN", "MARKET PERFORM", "EQUAL WEIGHT", "WAIT", "KEEP",
    "IN LINE", "PEER PERFORM", "SECTOR PERFORM", "STALEMATE"
}

_REDUCE_LIKE = {
    "REDUCE", "TRIM", "LIGHTEN", "UNDERWEIGHT", "PARTIAL SELL", "TAKE PROFIT",
    "TAKE PROFITS", "WEAK HOLD", "UNDERPERFORM", "MODERATE SELL"
}

_SELL_LIKE = {
    "SELL", "STRONG SELL", "EXIT", "AVOID", "SHORT", "NEGATIVE",
    "STRONGSELL", "STRONG_SELL"
}


def normalize_recommendation(x: Any, default: str = "HOLD") -> str:
    """
    Normalize any recommendation string/object to one of:
    BUY, HOLD, REDUCE, SELL.

    If input is None or unrecognizable, returns `default` (usually "HOLD").
    """
    if x is None:
        return default

    try:
        s = str(x).strip().upper()
    except Exception:
        return default

    if not s:
        return default

    # Fast path: already canonical
    if s in ALLOWED_RECOS:
        return s

    # Normalization: "Strong-Buy" -> "STRONG BUY"
    norm = _CLEANER_RE.sub(" ", s).strip()

    if norm in _BUY_LIKE:
        return "BUY"
    if norm in _HOLD_LIKE:
        return "HOLD"
    if norm in _REDUCE_LIKE:
        return "REDUCE"
    if norm in _SELL_LIKE:
        return "SELL"

    # Heuristic fallbacks for complex strings
    if "SELL" in norm:
        return "SELL"
    if "BUY" in norm or "ACCUMULATE" in norm:
        return "BUY"
    if "HOLD" in norm or "NEUTRAL" in norm:
        return "HOLD"
    if "REDUCE" in norm or "UNDERWEIGHT" in norm:
        return "REDUCE"

    return default


__all__ = ["normalize_recommendation", "ALLOWED_RECOS"]
