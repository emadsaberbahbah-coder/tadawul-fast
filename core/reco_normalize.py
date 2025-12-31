from __future__ import annotations
from typing import Any, Optional

_ALLOWED = {"BUY", "HOLD", "REDUCE", "SELL"}

_STRONG_TO_BASE = {
    "STRONG_BUY": "BUY",
    "STRONGBUY": "BUY",
    "STRONG SELL": "SELL",
    "STRONG_SELL": "SELL",
    "STRONGSELL": "SELL",
}

_SYNONYMS = {
    "NEUTRAL": "HOLD",
    "KEEP": "HOLD",
    "WAIT": "HOLD",
    "TRIM": "REDUCE",
    "TAKE_PROFIT": "REDUCE",
    "PARTIAL_SELL": "REDUCE",
}

def normalize_recommendation(x: Any) -> Optional[str]:
    if x is None:
        return None
    s = str(x).strip()
    if not s:
        return None

    u = s.upper().strip()
    u2 = u.replace(" ", "_")

    if u in _ALLOWED:
        return u
    if u2 in _ALLOWED:
        return u2

    if u in _STRONG_TO_BASE:
        return _STRONG_TO_BASE[u]
    if u2 in _STRONG_TO_BASE:
        return _STRONG_TO_BASE[u2]

    if u in _SYNONYMS:
        return _SYNONYMS[u]
    if u2 in _SYNONYMS:
        return _SYNONYMS[u2]

    # safest fallback: HOLD (stable, not misleading like BUY/SELL)
    return "HOLD"
