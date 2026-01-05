# core/reco_normalize.py
"""
core/reco_normalize.py
------------------------------------------------------------
Recommendation Normalization Utility — v1.2.0 (PROD SAFE)

Central logic to standardize broker/analyst ratings into a
strict 4-state enum: BUY, HOLD, REDUCE, SELL.

What's New in v1.2.0:
- ✅ Expanded Synonyms: Added Argaam-specific and international broker ratings.
- ✅ Tiered Matching: Exact set match -> Normalized match -> Heuristic partial match.
- ✅ Case-Insensitive: Handles any variation of casing or punctuation.
- ✅ Zero Dependency: Pure Python, import-safe everywhere.
"""

from __future__ import annotations

import re
from typing import Any, Set

# The canonical output set (The only 4 values allowed)
ALLOWED_RECOS = {"BUY", "HOLD", "REDUCE", "SELL"}

# Cleaner: Replaces punctuation and extra whitespace with a single space
_CLEANER_RE = re.compile(r"[\s\-_/]+")

# -----------------------------------------------------------------------------
# Synonym Sets (Aggregated from Yahoo, Argaam, EODHD, and Finnhub)
# -----------------------------------------------------------------------------

_BUY_LIKE: Set[str] = {
    "STRONG BUY", "BUY", "ACCUMULATE", "ADD", "OUTPERFORM", "OVERWEIGHT", 
    "LONG", "POSITIVE", "MARKET OUTPERFORM", "STRONGBUY", "STRONG_BUY", 
    "OVER WEIGHT", "MODERATE BUY", "TOP PICK"
}

_HOLD_LIKE: Set[str] = {
    "HOLD", "NEUTRAL", "MAINTAIN", "MARKET PERFORM", "EQUAL WEIGHT", "WAIT", 
    "KEEP", "IN LINE", "PEER PERFORM", "SECTOR PERFORM", "STALEMATE", 
    "MARKET_PERFORM", "EQUAL_WEIGHT", "FAIR VALUE"
}

_REDUCE_LIKE: Set[str] = {
    "REDUCE", "TRIM", "LIGHTEN", "UNDERWEIGHT", "PARTIAL SELL", "TAKE PROFIT",
    "TAKE PROFITS", "WEAK HOLD", "UNDERPERFORM", "MODERATE SELL", "UNDER WEIGHT",
    "SECTOR UNDERPERFORM"
}

_SELL_LIKE: Set[str] = {
    "SELL", "STRONG SELL", "EXIT", "AVOID", "SHORT", "NEGATIVE",
    "STRONGSELL", "STRONG_SELL", "LIQUIDATE"
}

# -----------------------------------------------------------------------------
# Logic
# -----------------------------------------------------------------------------

def normalize_recommendation(x: Any, default: str = "HOLD") -> str:
    """
    Standardize any recommendation string to BUY, HOLD, REDUCE, or SELL.
    
    Args:
        x: The raw recommendation string from a provider.
        default: The fallback value if normalization fails (Default: HOLD).
    """
    if x is None:
        return default

    # 1. Basic Cleaning
    try:
        raw = str(x).strip().upper()
    except Exception:
        return default

    if not raw:
        return default

    # 2. Fast Path: Already Standardized
    if raw in ALLOWED_RECOS:
        return raw

    # 3. Tiered Normalization
    # Strip special chars and extra spaces: "Strong-Buy" -> "STRONG BUY"
    norm = _CLEANER_RE.sub(" ", raw).strip()

    if norm in _BUY_LIKE:
        return "BUY"
    if norm in _HOLD_LIKE:
        return "HOLD"
    if norm in _REDUCE_LIKE:
        return "REDUCE"
    if norm in _SELL_LIKE:
        return "SELL"

    # 4. Heuristic Partial Matching (Last Resort)
    # Checks if key indicators exist anywhere in the string
    if "SELL" in norm:
        return "SELL"
    if "BUY" in norm or "ACCUMULATE" in norm or "POSITIVE" in norm:
        return "BUY"
    if "REDUCE" in norm or "TRIM" in norm or "UNDERWEIGHT" in norm:
        return "REDUCE"
    if "HOLD" in norm or "NEUTRAL" in norm or "MAINTAIN" in norm:
        return "HOLD"

    # 5. Global Fallback
    return default

__all__ = ["normalize_recommendation", "ALLOWED_RECOS"]
