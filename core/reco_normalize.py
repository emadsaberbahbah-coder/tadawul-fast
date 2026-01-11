# core/symbols/normalize.py
"""
core/symbols/normalize.py
------------------------------------------------------------
Symbol Normalization — v1.1.0 (PROD SAFE)

Purpose
- Provide ONE canonical symbol format inside the app.
- Enable provider-specific symbol formats without guessing.

Canonical rules (internal)
- KSA numeric -> 1120.SR
- If already ends with .SR keep it
- Global equity default -> TICKER.US (default exchange from DEFAULT_EXCHANGE env or US)
- Keep special symbols unchanged (indices/FX/commodities/crypto pairs) e.g. ^GSPC, EURUSD=X, GC=F, BTC-USD

Provider helpers
- to_eodhd_symbol(): ensure "TICKER.EXCHANGE" (AAPL -> AAPL.US)
- to_yahoo_symbol(): strip ".US" to "AAPL" (Yahoo expects AAPL for US), but KEEP non-US suffix (e.g., BP.L)
- to_finnhub_symbol(): strip ".US" to "AAPL" (best-effort)
"""

from __future__ import annotations

import os
import re
from typing import Any, Iterable, List, Optional, Tuple

_ARABIC_DIGITS = str.maketrans("٠١٢٣٤٥٦٧٨٩", "0123456789")

# KSA: 3-6 digits, optional .SR
_KSA_RE = re.compile(r"^\d{3,6}(\.SR)?$", re.IGNORECASE)

# Generic exchange suffix: BASE.EXCH (EXCH 2-6 chars)
_EXCH_SUFFIX_RE = re.compile(r"^(.+)\.([A-Z0-9]{2,6})$")

# Crypto pairs like BTC-USD, ETH-USDT, etc (quote 3-10 chars)
_CRYPTO_PAIR_RE = re.compile(r"^[A-Z0-9]{2,10}-[A-Z0-9]{3,10}$")


def _env_default_exchange() -> str:
    ex = (os.getenv("DEFAULT_EXCHANGE") or os.getenv("EODHD_DEFAULT_EXCHANGE") or "US").strip().upper()
    return ex or "US"


def _coerce_str(x: Any) -> str:
    try:
        return (str(x) if x is not None else "").strip()
    except Exception:
        return ""


def _strip_wrappers(s: Any) -> str:
    """
    Normalize casing + remove known wrappers without changing meaning.
    - translates Arabic digits
    - strips: "TADAWUL:" and ".TADAWUL"
    """
    u = _coerce_str(s).translate(_ARABIC_DIGITS).strip().upper()
    if not u:
        return ""

    # Tadawul wrappers
    if u.startswith("TADAWUL:"):
        u = u.split(":", 1)[1].strip()
    if u.endswith(".TADAWUL"):
        u = u.replace(".TADAWUL", "")

    return u.strip()


def _is_special_symbol(u: str) -> bool:
    """
    Keep these unchanged (do NOT append .US / .SR):
    - indices: ^GSPC, ^TASI, etc
    - Yahoo FX/commodities/futures: EURUSD=X, GC=F, CL=F, etc
    - generic '=' symbols
    - slash pairs: EUR/USD
    - crypto pairs: BTC-USD, ETH-USDT, etc
    """
    s = (u or "").strip().upper()
    if not s:
        return False
    if "^" in s:
        return True
    if "=" in s:
        return True
    if "/" in s:
        return True
    if _CRYPTO_PAIR_RE.fullmatch(s or ""):
        return True
    return False


def looks_like_ksa(symbol: Any) -> bool:
    u = _strip_wrappers(symbol)
    if not u:
        return False
    if u.endswith(".SR"):
        return True
    if u.isdigit():
        return True
    return bool(_KSA_RE.fullmatch(u))


def split_exchange_suffix(symbol: Any) -> Tuple[str, Optional[str]]:
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
    exch = (m.group(2) or "").strip().upper()
    if not base or not exch:
        return u, None
    return base, exch


def normalize_symbol(raw: Any, *, default_exchange: Optional[str] = None) -> str:
    """
    Canonical internal format:
    - KSA: 1120.SR
    - Global equities: AAPL.US (default exchange US unless provided)
    - Keep special symbols as-is (indices/FX/futures/crypto pairs)
    """
    s = _strip_wrappers(raw)
    if not s:
        return ""

    if _is_special_symbol(s):
        return s

    # KSA normalize
    if looks_like_ksa(s):
        if s.endswith(".SR"):
            return s
        # digits-only
        if s.isdigit():
            return f"{s}.SR"
        # safety: if something like "1120" already handled; else force .SR
        base, exch = split_exchange_suffix(s)
        if exch == "SR":
            return f"{base}.SR"
        if base.isdigit():
            return f"{base}.SR"
        return f"{s}.SR"

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
    - Non-US equities: KEEP suffix (e.g., BP.L, SHOP.TO)
    - KSA: 1120.SR (same)
    - indices/FX/futures/crypto pairs: unchanged
    """
    s = normalize_symbol(raw)
    if not s:
        return ""
    if _is_special_symbol(s):
        return s
    if s.endswith(".SR"):
        return s

    base, exch = split_exchange_suffix(s)
    if not exch:
        return s

    # Strip only US
    if exch == "US":
        return base

    # Keep non-US suffix for Yahoo
    return f"{base}.{exch}"


def to_finnhub_symbol(raw: Any) -> str:
    """
    Finnhub often expects plain ticker for US equities (AAPL).
    Best-effort:
    - AAPL.US -> AAPL
    - keep others unchanged (including 1120.SR, BP.L, indices, FX, etc.)
    """
    s = normalize_symbol(raw)
    if not s:
        return ""
    if _is_special_symbol(s):
        return s
    base, exch = split_exchange_suffix(s)
    if exch == "US":
        return base
    return s


def parse_symbols_list(raw: Any, *, max_symbols: int = 0) -> List[str]:
    """
    Accepts:
      "AAPL,MSFT,1120"
      "AAPL MSFT 1120.SR"
      ["AAPL", "MSFT", "1120"]
    Returns canonical normalized list (deduped, order preserved).
    """
    out: List[str] = []
    seen = set()

    def _push(p: Any) -> None:
        sym = normalize_symbol(p)
        if not sym:
            return
        key = sym.upper()
        if key in seen:
            return
        seen.add(key)
        out.append(sym)

    if isinstance(raw, (list, tuple, set)):
        for p in raw:
            _push(p)
            if max_symbols and max_symbols > 0 and len(out) >= max_symbols:
                break
        return out

    s = _coerce_str(raw)
    if not s:
        return []

    parts = re.split(r"[\s,]+", s)
    for p in parts:
        if not p or not p.strip():
            continue
        _push(p)
        if max_symbols and max_symbols > 0 and len(out) >= max_symbols:
            break

    return out


__all__ = [
    "normalize_symbol",
    "to_eodhd_symbol",
    "to_yahoo_symbol",
    "to_finnhub_symbol",
    "parse_symbols_list",
    "looks_like_ksa",
    "split_exchange_suffix",
]
