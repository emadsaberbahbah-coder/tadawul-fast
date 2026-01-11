# core/reco_normalize.py  (FULL REPLACEMENT)
"""
core/reco_normalize.py
------------------------------------------------------------
Recommendation Normalization — v1.0.0 (PROD SAFE)

Purpose
- Provide ONE canonical recommendation enum across the whole app.
- Make provider outputs consistent for Sheets + APIs.
- Keep logic simple, deterministic, and safe.

Canonical output (ALWAYS)
- BUY / HOLD / REDUCE / SELL

Notes
- If input is missing/blank/unrecognized => HOLD
- Handles common broker/provider labels and some heuristics.
"""

from __future__ import annotations

import re
from typing import Any

_RECO_ENUM = ("BUY", "HOLD", "REDUCE", "SELL")


def normalize_recommendation(x: Any) -> str:
    """
    Always returns one of: BUY / HOLD / REDUCE / SELL (uppercase).
    """
    if x is None:
        return "HOLD"

    try:
        s = str(x).strip().upper()
    except Exception:
        return "HOLD"

    if not s:
        return "HOLD"
    if s in _RECO_ENUM:
        return s

    # Normalize separators and whitespace
    s2 = re.sub(r"[\s\-_/]+", " ", s).strip()

    # Common mappings (exact)
    buy_like = {
        "STRONG BUY",
        "BUY",
        "ACCUMULATE",
        "ADD",
        "OUTPERFORM",
        "OVERWEIGHT",
        "LONG",
        "BULLISH",
        "POSITIVE",
    }
    hold_like = {
        "HOLD",
        "NEUTRAL",
        "MAINTAIN",
        "MARKET PERFORM",
        "EQUAL WEIGHT",
        "WAIT",
        "IN LINE",
        "UNCHANGED",
        "STABLE",
    }
    reduce_like = {
        "REDUCE",
        "TRIM",
        "LIGHTEN",
        "UNDERWEIGHT",
        "PARTIAL SELL",
        "TAKE PROFIT",
        "TAKE PROFITS",
        "WEAKEN",
        "CAUTIOUS",
    }
    sell_like = {
        "SELL",
        "STRONG SELL",
        "EXIT",
        "AVOID",
        "UNDERPERFORM",
        "SHORT",
        "BEARISH",
        "NEGATIVE",
    }

    if s2 in buy_like:
        return "BUY"
    if s2 in hold_like:
        return "HOLD"
    if s2 in reduce_like:
        return "REDUCE"
    if s2 in sell_like:
        return "SELL"

    # Heuristics (contains)
    if "SELL" in s2 or "EXIT" in s2 or "AVOID" in s2 or "UNDERPERFORM" in s2 or "SHORT" in s2:
        return "SELL"
    if "REDUCE" in s2 or "TRIM" in s2 or "TAKE PROFIT" in s2 or "UNDERWEIGHT" in s2:
        return "REDUCE"
    if "HOLD" in s2 or "NEUTRAL" in s2 or "MAINTAIN" in s2 or "EQUAL WEIGHT" in s2:
        return "HOLD"
    if "BUY" in s2 or "ACCUMULATE" in s2 or "OUTPERFORM" in s2 or "OVERWEIGHT" in s2 or "LONG" in s2:
        return "BUY"

    return "HOLD"


__all__ = ["normalize_recommendation"]
