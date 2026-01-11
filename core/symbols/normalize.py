# core/symbols/normalize.py
"""
core/symbols/normalize.py
============================================================
Symbol Normalization — v1.0.0 (PROD SAFE / NO NETWORK)

Purpose
- Provide ONE consistent normalization layer for:
  - routers (/enriched, /analysis)
  - engine selection (GLOBAL vs KSA)
  - provider formatting (esp. EODHD exchange suffix like .US)

Design rules (important)
- normalize_symbol() does NOT force ".US" for GLOBAL tickers.
  It returns a clean canonical *request* symbol:
    - "1120"      -> "1120.SR"
    - "1120.SR"   -> "1120.SR"
    - "AAPL"      -> "AAPL"
    - "AAPL.US"   -> "AAPL.US"
    - "^GSPC"     -> "^GSPC"
    - "EURUSD=X"  -> "EURUSD=X"
- Providers/routers can then try variants:
  - GLOBAL: [AAPL, AAPL.US] (and BRK.B/BRK-B swaps)
  - KSA:    [1120.SR, 1120]
  - FX/Indices/Commodities: [itself only]

This file must stay import-safe (no network, no heavy imports).
"""

from __future__ import annotations

import os
import re
from typing import Iterable, List, Optional, Sequence, Tuple

__all__ = [
    "normalize_symbol",
    "normalize_symbols_list",
    "symbol_variants",
    "market_hint_for",
    "is_ksa",
    "is_index_or_fx",
    "to_eodhd_symbol",
    "eodhd_symbol_variants",
]

_TRUTHY = {"1", "true", "yes", "y", "on", "t"}
_FALSY = {"0", "false", "no", "n", "off", "f"}

# KSA: naked digits or digits.SR
_KSA_RE = re.compile(r"^\d{3,6}(\.SR)?$", re.IGNORECASE)

# Exchange suffix like .US / .L / .TO ... (2..6 chars)
_EXCH_SUFFIX_RE = re.compile(r"^(.+)\.([A-Z0-9]{2,6})$")

# Common prefixes you may get from some feeds
_PREFIXES = ("TADAWUL:",)


def _env_str(name: str, default: str = "") -> str:
    v = os.getenv(name)
    if v is None:
        return default
    s = str(v).strip()
    return s if s else default


def _env_bool(name: str, default: bool = False) -> bool:
    raw = (os.getenv(name) or "").strip().lower()
    if not raw:
        return default
    if raw in _FALSY:
        return False
    if raw in _TRUTHY:
        return True
    return default


def is_index_or_fx(sym: str) -> bool:
    """
    Treat Yahoo-style indices/FX/commodities as passthrough:
      - indices: ^GSPC
      - FX: EURUSD=X
      - commodities: GC=F, BZ=F
    """
    s = (sym or "").strip().upper()
    if not s:
        return False
    return ("^" in s) or ("=" in s) or s.endswith("=X") or s.endswith("=F")


def is_ksa(sym: str) -> bool:
    s = (sym or "").strip().upper()
    if not s:
        return False
    if s.endswith(".SR"):
        return True
    if s.isdigit():
        return True
    return bool(_KSA_RE.match(s))


def _strip_known_prefixes(raw: str) -> str:
    s = (raw or "").strip()
    if not s:
        return ""
    u = s.upper()
    for p in _PREFIXES:
        if u.startswith(p):
            return s.split(":", 1)[1].strip()
    return s


def _strip_known_suffixes(raw: str) -> str:
    s = (raw or "").strip()
    if not s:
        return ""
    u = s.upper()
    # some feeds use ".TADAWUL"
    if u.endswith(".TADAWUL"):
        s = s[: -len(".TADAWUL")]
    return s.strip()


def _split_exchange_suffix(sym: str) -> Tuple[str, Optional[str]]:
    """
    If sym ends in .US/.L/.TO/... treat last segment as exchange suffix.
    Returns (base, exch_or_none).
    """
    s = (sym or "").strip().upper()
    if not s:
        return "", None
    m = _EXCH_SUFFIX_RE.match(s)
    if not m:
        return s, None
    base = (m.group(1) or "").strip()
    exch = (m.group(2) or "").strip()
    if not base or not exch:
        return s, None
    return base, exch


def normalize_symbol(raw: str) -> str:
    """
    Canonical router/engine normalization.
    - Keeps indices/FX/commodities unchanged (upper)
    - KSA digits -> digits.SR
    - Keeps explicit suffix (.SR/.US/...) unchanged (upper)
    - Plain tickers -> UPPER (no forced .US here)
    """
    s = _strip_known_prefixes(raw)
    s = _strip_known_suffixes(s)
    s = (s or "").strip()
    if not s:
        return ""

    u = s.upper()

    if is_index_or_fx(u):
        return u

    # KSA: digits -> digits.SR
    if u.isdigit():
        return f"{u}.SR"

    # KSA explicit .SR
    if u.endswith(".SR"):
        return u

    # Keep any explicit suffix like .US/.L/.TO etc as-is
    if "." in u:
        return u

    return u


def normalize_symbols_list(raw: str, *, limit: int = 0) -> List[str]:
    """
    Accepts:
      "AAPL,MSFT,1120.SR"
      "AAPL MSFT 1120.SR"
      "AAPL, MSFT  ,1120.SR"
    Returns a clean list (keeps order), normalized, de-duped (case-insensitive).
    """
    s = (raw or "").strip()
    if not s:
        return []
    parts = re.split(r"[\s,]+", s)
    out: List[str] = []
    for p in parts:
        if not p or not p.strip():
            continue
        out.append(normalize_symbol(p))

    seen = set()
    final: List[str] = []
    for x in out:
        if not x:
            continue
        k = x.upper()
        if k in seen:
            continue
        seen.add(k)
        final.append(x)
        if limit and limit > 0 and len(final) >= limit:
            break
    return final


def market_hint_for(sym_norm: str) -> str:
    """
    Minimal hint string used by routers/engines.
    """
    u = (sym_norm or "").strip().upper()
    if is_ksa(u):
        return "KSA"
    return "GLOBAL"


def symbol_variants(sym_norm: str) -> List[str]:
    """
    Safe router-level variants:
    - GLOBAL: AAPL -> [AAPL, AAPL.US]
             AAPL.US -> [AAPL.US, AAPL]
             BRK.B -> [BRK.B, BRK.B.US, BRK-B.US]
             BRK-B -> [BRK-B, BRK-B.US, BRK.B.US]
    - KSA: 1120.SR -> [1120.SR, 1120]
           1120 -> [1120.SR, 1120]
    - Indices/FX/Commodities: [itself]
    """
    s = (sym_norm or "").strip()
    if not s:
        return []

    u = s.upper()

    if is_index_or_fx(u):
        return [u]

    out: List[str] = []

    # KSA
    if is_ksa(u):
        if u.isdigit():
            out.extend([f"{u}.SR", u])
        elif u.endswith(".SR"):
            out.extend([u, u.replace(".SR", "")])
        else:
            out.append(u)
        return _dedupe_preserve(out)

    # GLOBAL
    out.append(u)

    # If has explicit exchange suffix, also try base
    base, exch = _split_exchange_suffix(u)
    if exch is not None:
        if base and base != u:
            out.append(base)
        return _dedupe_preserve(out)

    # Plain ticker: add .US variant (common for EODHD)
    if "." not in u:
        out.append(f"{u}.{_default_exchange()}")

    # Class share dot/dash swaps (BRK.B <-> BRK-B)
    # Only do this for BASE, not for the exchange separator.
    if "." in u and u.count(".") == 1:
        # e.g., BRK.B (class share style) -> also BRK-B
        out.append(u.replace(".", "-"))
        out.append(f"{u}.{_default_exchange()}")
        out.append(f"{u.replace('.', '-')}.{_default_exchange()}")
    elif "-" in u and "." not in u:
        # e.g., BRK-B -> also BRK.B
        out.append(u.replace("-", "."))
        out.append(f"{u}.{_default_exchange()}")
        out.append(f"{u.replace('-', '.')}.{_default_exchange()}")

    return _dedupe_preserve(out)


def _default_exchange() -> str:
    ex = _env_str("EODHD_DEFAULT_EXCHANGE", "US").strip().upper()
    return ex or "US"


def _dedupe_preserve(xs: Sequence[str]) -> List[str]:
    seen = set()
    out: List[str] = []
    for x in xs:
        if not x:
            continue
        k = x.upper()
        if k in seen:
            continue
        seen.add(k)
        out.append(x)
    return out


def to_eodhd_symbol(sym_norm: str, *, default_exchange: Optional[str] = None) -> str:
    """
    Convert a normalized *GLOBAL* symbol to EODHD "BASE.EXCH" format when needed.
    - If symbol already has exchange suffix => keep as-is.
    - If symbol is index/fx/commodity => keep as-is.
    - If symbol is KSA => keep as-is (EODHD KSA is blocked by default in provider anyway).
    - Else: AAPL -> AAPL.US (or default_exchange / EODHD_DEFAULT_EXCHANGE).
    """
    s = (sym_norm or "").strip().upper()
    if not s:
        return ""
    if is_index_or_fx(s) or is_ksa(s):
        return s
    if "." in s:
        return s
    ex = (default_exchange or _default_exchange()).strip().upper() or "US"
    return f"{s}.{ex}"


def eodhd_symbol_variants(sym_norm: str, *, default_exchange: Optional[str] = None) -> List[str]:
    """
    Provider-oriented variants for EODHD:
    - AAPL -> [AAPL.US, AAPL]
    - BRK.B -> [BRK.B.US, BRK-B.US, BRK.B]
    - BRK-B -> [BRK-B.US, BRK.B.US, BRK-B]
    """
    s = (sym_norm or "").strip().upper()
    if not s:
        return []
    if is_index_or_fx(s) or is_ksa(s):
        return [s]

    ex = (default_exchange or _default_exchange()).strip().upper() or "US"

    out: List[str] = []

    base, exch = _split_exchange_suffix(s)
    if exch is not None:
        # already in BASE.EXCH form -> try as-is, then base
        out.append(f"{base}.{exch}")
        if base:
            out.append(base)
        return _dedupe_preserve(out)

    # Plain ticker
    if "." not in s and "-" not in s:
        out.extend([f"{s}.{ex}", s])
        return _dedupe_preserve(out)

    # Class shares / special tickers
    # BRK.B
    if "." in s:
        out.append(f"{s}.{ex}")
        out.append(f"{s.replace('.', '-')}.{ex}")
        out.append(s)
        return _dedupe_preserve(out)

    # BRK-B
    if "-" in s:
        out.append(f"{s}.{ex}")
        out.append(f"{s.replace('-', '.')}.{ex}")
        out.append(s)
        return _dedupe_preserve(out)

    out.extend([f"{s}.{ex}", s])
    return _dedupe_preserve(out)
