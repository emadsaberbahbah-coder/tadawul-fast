# core/symbols/normalize.py  (NEW SCRIPT)
"""
core/symbols/normalize.py
------------------------------------------------------------
Symbol Normalization — v1.0.0 (PROD SAFE)

Purpose
- Provide ONE canonical symbol format inside the app.
- Enable provider-specific symbol formats without guessing.

Canonical rules (internal)
- KSA numeric -> 1120.SR
- If already ends with .SR keep it
- Global equity default -> TICKER.US (default exchange from DEFAULT_EXCHANGE env or US)
- Keep special symbols unchanged (indices/FX/commodities) e.g. ^GSPC, EURUSD=X, GC=F

Provider helpers
- to_eodhd_symbol(): ensure "TICKER.EXCHANGE" (AAPL -> AAPL.US)
- to_yahoo_symbol(): strip ".US" to "AAPL" (Yahoo usually wants AAPL)
"""

from __future__ import annotations

import os
import re
from typing import Any, List, Optional, Tuple

_TRUTHY = {"1", "true", "yes", "y", "on", "t"}

_KSA_RE = re.compile(r"^\d{3,6}(\.SR)?$", re.IGNORECASE)
_EXCH_SUFFIX_RE = re.compile(r"^(.+)\.([A-Z0-9]{2,6})$")


def _env_default_exchange() -> str:
    ex = (os.getenv("DEFAULT_EXCHANGE") or os.getenv("EODHD_DEFAULT_EXCHANGE") or "US").strip().upper()
    return ex or "US"


def _is_index_or_fx_or_future(s: str) -> bool:
    """
    Keep these as-is:
    - indices: ^GSPC, ^TASI, etc
    - FX: EURUSD=X
    - commodities/futures: GC=F, CL=F, BZ=F, etc
    """
    u = (s or "").strip().upper()
    if not u:
        return False
    if "^" in u:
        return True
    if "=" in u:
        return True
    return False


def _strip_wrappers(s: str) -> str:
    u = (s or "").strip().upper()
    if not u:
        return ""
    # Tadawul wrappers
    if u.startswith("TADAWUL:"):
        u = u.split(":", 1)[1].strip()
    if u.endswith(".TADAWUL"):
        u = u.replace(".TADAWUL", "")
    return u.strip()


def looks_like_ksa(symbol: str) -> bool:
    u = _strip_wrappers(symbol)
    if not u:
        return False
    if u.endswith(".SR"):
        return True
    if u.isdigit():
        return True
    return bool(_KSA_RE.match(u))


def split_exchange_suffix(symbol: str) -> Tuple[str, Optional[str]]:
    """
    If ends with .US/.L/.TO/... return (base, exch). Else (symbol, None).
    """
    u = _strip_wrappers(symbol)
    if not u:
        return "", None
    m = _EXCH_SUFFIX_RE.match(u)
    if not m:
        return u, None
    base = (m.group(1) or "").strip()
    exch = (m.group(2) or "").strip()
    if not base or not exch:
        return u, None
    return base, exch


def normalize_symbol(raw: Any, *, default_exchange: Optional[str] = None) -> str:
    """
    Canonical internal format:
    - KSA: 1120.SR
    - Global equities: AAPL.US (default exchange US unless provided)
    - Keep special symbols as-is (indices/FX/futures)
    """
    s = _strip_wrappers(str(raw or ""))
    if not s:
        return ""

    if _is_index_or_fx_or_future(s):
        return s

    # KSA normalize
    if looks_like_ksa(s):
        if s.endswith(".SR"):
            return s
        if s.isdigit():
            return f"{s}.SR"
        # if matches like 1120 (already handled) or 1120.SR (handled)
        return s if s.endswith(".SR") else f"{s}.SR"

    # Global normalize
    base, exch = split_exchange_suffix(s)
    if exch:
        return f"{base}.{exch}"

    ex = (default_exchange or _env_default_exchange()).strip().upper() or "US"
    return f"{s}.{ex}"


def to_eodhd_symbol(raw: Any, *, default_exchange: Optional[str] = None) -> str:
    """
    EODHD format typically expects: TICKER.EXCH (AAPL.US)
    """
    return normalize_symbol(raw, default_exchange=default_exchange)


def to_yahoo_symbol(raw: Any) -> str:
    """
    Yahoo typically expects:
    - US equities: AAPL (not AAPL.US)
    - KSA: 1120.SR (same)
    - indices/FX/futures: unchanged
    """
    s = normalize_symbol(raw)
    if not s:
        return ""
    if _is_index_or_fx_or_future(s):
        return s
    if s.endswith(".SR"):
        return s
    # strip .US (or any 2-6 suffix) ONLY if it looks like an exchange suffix
    base, exch = split_exchange_suffix(s)
    if exch:
        return base
    return s


def parse_symbols_list(raw: Any, *, max_symbols: int = 0) -> List[str]:
    """
    Accepts:
      "AAPL,MSFT,1120"
      "AAPL MSFT 1120.SR"
    Returns canonical normalized list (deduped, order preserved).
    """
    s = str(raw or "").strip()
    if not s:
        return []

    parts = re.split(r"[\s,]+", s)
    out: List[str] = []
    seen = set()

    for p in parts:
        if not p or not p.strip():
            continue
        sym = normalize_symbol(p)
        if not sym:
            continue
        key = sym.upper()
        if key in seen:
            continue
        seen.add(key)
        out.append(sym)

        if max_symbols and max_symbols > 0 and len(out) >= max_symbols:
            break

    return out


__all__ = [
    "normalize_symbol",
    "to_eodhd_symbol",
    "to_yahoo_symbol",
    "parse_symbols_list",
    "looks_like_ksa",
    "split_exchange_suffix",
]
