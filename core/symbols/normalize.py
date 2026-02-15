#!/usr/bin/env python3
# core/symbols/normalize.py
"""
core/symbols/normalize.py
============================================================
Symbol Normalization — v3.0.0 (PROD SAFE / NO NETWORK)
Financial Leader Edition — Optimized for KSA + Global + Indices/FX/Crypto/Commodities

FULL REPLACEMENT (v3.0.0) — What’s improved vs v1.2.1
- ✅ Stronger Unicode cleanup (zero-width, bidi marks, BOM, NBSP, Arabic tatweel, etc.)
- ✅ Arabic/Indic digit normalization (Arabic-Indic + Eastern Arabic-Indic)
- ✅ More accurate special-symbol detection (indices, FX, commodities futures, crypto)
- ✅ Strict KSA normalization:
    - "1120" / "١١٢٠" / "1120.SR" / "tadawul:1120" -> "1120.SR"
    - Rejects invalid codes safely (returns "")
- ✅ Better global ticker sanitation:
    - Strips known prefixes/suffixes robustly
    - Standardizes share class separators conservatively (BRK-B -> BRK.B)
    - Preserves valid dotted tickers (e.g., "7203.T", "RDS.A") without over-normalizing
- ✅ Provider formatting helpers:
    - to_yahoo_symbol(): ensures KSA uses .SR, keeps indices/FX, handles class separator
    - to_finnhub_symbol(): strips .US and returns finnhub-ready equity symbol
    - to_eodhd_symbol(): ensures "TICKER.EXCH" when needed (configurable default)
- ✅ Variants tuned for routing:
    - symbol_variants(): generic
    - yahoo_symbol_variants(): Yahoo-first variants
    - eodhd_symbol_variants(): EODHD-first variants
    - finnhub_symbol_variants(): Finnhub-first variants
- ✅ Fast & deterministic:
    - No network, no heavy imports, stable outputs

Environment variables
- EODHD_DEFAULT_EXCHANGE (default "US")
- NORMALIZE_STRIP_SUFFIXES (optional, comma-separated; appended to built-ins)
- NORMALIZE_STRIP_PREFIXES (optional, comma-separated; appended to built-ins)
"""

from __future__ import annotations

import os
import re
from typing import List, Optional, Tuple

__all__ = [
    # Core
    "normalize_symbol",
    "normalize_ksa_symbol",
    "normalize_symbols_list",
    "symbol_variants",
    "market_hint_for",
    "is_ksa",
    "looks_like_ksa",
    "is_index_or_fx",
    "is_special_symbol",
    # Provider formatting
    "to_yahoo_symbol",
    "to_finnhub_symbol",
    "to_eodhd_symbol",
    # Provider-specific variants
    "yahoo_symbol_variants",
    "finnhub_symbol_variants",
    "eodhd_symbol_variants",
]

# ---------------------------------------------------------------------------
# Unicode + digit normalization
# ---------------------------------------------------------------------------
# Arabic-Indic digits (U+0660..0669) and Eastern Arabic-Indic digits (U+06F0..06F9)
_ARABIC_INDIC = "٠١٢٣٤٥٦٧٨٩"
_EASTERN_ARABIC_INDIC = "۰۱۲۳۴۵۶۷۸۹"
_ASCII_DIGITS = "0123456789"

_DIGIT_TRANS = str.maketrans(
    _ARABIC_INDIC + _EASTERN_ARABIC_INDIC,
    _ASCII_DIGITS + _ASCII_DIGITS,
)

# Hidden / zero-width / bidi / BOM / NBSP and similar artifacts
# Covers: ZWSP/ZWNJ/ZWJ, LRM/RLM, bidi overrides, BOM, NBSP, Arabic tatweel.
_HIDDEN_CHARS_RE = re.compile(
    r"[\u200b\u200c\u200d\u200e\u200f\u202a-\u202e\u2066-\u2069\ufeff\u00a0\u0640]"
)

# Some Arabic punctuation used in numbers: Arabic thousands separator (٬) and decimal (٫)
_ARABIC_THOUSANDS = "\u066c"  # ٬
_ARABIC_DECIMAL = "\u066b"    # ٫

# ---------------------------------------------------------------------------
# Patterns / routing
# ---------------------------------------------------------------------------
# KSA: naked digits or digits.SR (3 to 6 digits)
_KSA_RE = re.compile(r"^\d{3,6}(\.SR)?$", re.IGNORECASE)
_KSA_CODE_ONLY = re.compile(r"^\d{3,6}$", re.IGNORECASE)

# Yahoo special formats:
# - Indices: ^GSPC, ^TASI, ^N225
# - FX: EURUSD=X, USDJPY=X
# - Commodities futures: GC=F, CL=F, BZ=F, SI=F
# - Crypto: BTC-USD, ETH-USD
# - Some rates: ^IRX, ^TNX etc (handled by caret)
_RE_INDEX = re.compile(r"^\^.+")
_RE_FX = re.compile(r"^[A-Z]{3,6}=[X]$", re.IGNORECASE)  # e.g., EURUSD=X
_RE_FUT = re.compile(r"^[A-Z0-9]{1,6}=[F]$", re.IGNORECASE)  # e.g., GC=F, CL=F
_RE_CRYPTO = re.compile(r"^[A-Z0-9]{2,15}-[A-Z]{2,10}$", re.IGNORECASE)  # BTC-USD
_RE_SLASH_FX = re.compile(r"^[A-Z]{3,6}/[A-Z]{3,6}$", re.IGNORECASE)  # EUR/USD

# Common prefixes/suffixes noise to strip
_BUILTIN_PREFIXES = ("TADAWUL:", "STOCK:", "TICKER:", "INDEX:", "NYSE:", "NASDAQ:", "OTC:", "FOREX:")
_BUILTIN_SUFFIXES = (".TADAWUL",)

# Some platforms append these pseudo-suffixes; we keep them optional via env
# (We do NOT strip real exchange suffixes like .T, .L, .HK, etc.)
# Only strip the ones you explicitly want.
_DEFAULT_EXTRA_SUFFIXES = (".QA", ".JK")

# Global tickers allowed characters (conservative):
# - letters, digits
# - dot for class/exchange
# - dash for class
# - caret and equals only for special symbols
_RE_GLOBAL_ALLOWED = re.compile(r"^[A-Z0-9\.\-\^=\/]+$", re.IGNORECASE)


# ---------------------------------------------------------------------------
# Env helpers
# ---------------------------------------------------------------------------
def _env_str(name: str, default: str = "") -> str:
    v = os.getenv(name)
    return str(v).strip() if v else default


def _env_list(name: str) -> Tuple[str, ...]:
    raw = _env_str(name, "")
    if not raw:
        return tuple()
    parts = [p.strip() for p in raw.split(",") if p.strip()]
    return tuple(parts)


def _strip_hidden(s: str) -> str:
    if not s:
        return ""
    s = s.translate(_DIGIT_TRANS)
    s = _HIDDEN_CHARS_RE.sub("", s)
    # normalize Arabic separators inside numeric strings (only affects later parsing; harmless)
    s = s.replace(_ARABIC_THOUSANDS, ",").replace(_ARABIC_DECIMAL, ".")
    # collapse whitespace
    s = " ".join(s.split())
    return s.strip()


def _strip_noise_prefix_suffix(u: str) -> str:
    if not u:
        return ""
    prefixes = _BUILTIN_PREFIXES + _env_list("NORMALIZE_STRIP_PREFIXES")
    suffixes = _BUILTIN_SUFFIXES + _env_list("NORMALIZE_STRIP_SUFFIXES") + _DEFAULT_EXTRA_SUFFIXES

    for p in prefixes:
        if u.startswith(p):
            u = u.split(":", 1)[1].strip() if ":" in u else u[len(p):].strip()

    for suf in suffixes:
        if u.endswith(suf):
            u = u[: -len(suf)].strip()

    return u.strip()


# ---------------------------------------------------------------------------
# Public detection helpers
# ---------------------------------------------------------------------------
def is_special_symbol(sym: str) -> bool:
    """Detects Yahoo-style special symbols for indices, FX, commodities futures, crypto."""
    s = _strip_hidden((sym or "")).upper()
    if not s:
        return False
    if _RE_INDEX.match(s):
        return True
    if _RE_FX.match(s) or _RE_FUT.match(s):
        return True
    if _RE_CRYPTO.match(s):
        return True
    if _RE_SLASH_FX.match(s):
        return True
    # also treat any symbol containing '=' with X/F as special
    if "=" in s and (s.endswith("=X") or s.endswith("=F")):
        return True
    return False


def is_index_or_fx(sym: str) -> bool:
    """Backwards-compatible alias (kept for legacy imports)."""
    return is_special_symbol(sym)


def looks_like_ksa(sym: str) -> bool:
    """Cheap heuristic: Arabic/Latin digits of length 3..6, possibly with .SR suffix."""
    s = _strip_hidden((sym or "")).upper()
    if not s:
        return False
    if s.endswith(".SR"):
        s = s[:-3].strip()
    return bool(_KSA_CODE_ONLY.match(s))


def is_ksa(sym: str) -> bool:
    """Strict check if a symbol is a valid Saudi Tadawul code (digits or digits.SR)."""
    s = _strip_hidden((sym or "")).upper()
    if not s:
        return False
    s = _strip_noise_prefix_suffix(s)
    if s.endswith(".SR"):
        code = s[:-3].strip()
        return bool(_KSA_CODE_ONLY.match(code))
    return bool(_KSA_CODE_ONLY.match(s))


# ---------------------------------------------------------------------------
# KSA normalization
# ---------------------------------------------------------------------------
def normalize_ksa_symbol(symbol: str) -> str:
    """
    STRICT KSA Normalization:
    Ensures Saudi symbols always follow the '####.SR' format.
    Returns "" if not valid.
    """
    s = _strip_hidden(symbol).upper()
    if not s:
        return ""

    s = _strip_noise_prefix_suffix(s)

    if s.endswith(".SR"):
        code = s[:-3].strip()
        return f"{code}.SR" if _KSA_CODE_ONLY.match(code) else ""

    if _KSA_CODE_ONLY.match(s):
        return f"{s}.SR"

    return ""


# ---------------------------------------------------------------------------
# Canonical normalization
# ---------------------------------------------------------------------------
def normalize_symbol(raw: str) -> str:
    """
    Primary Canonical Normalizer:
    - Resolves Arabic/Indic digits & hidden Unicode artifacts.
    - Preserves special symbols (indices/FX/commodities/crypto).
    - Standardizes KSA to .SR.
    - Conservative global sanitation:
        - Strips known noise prefixes/suffixes
        - Share class separator: BRK-B -> BRK.B (only if looks like class)
    """
    if not raw:
        return ""

    s = _strip_hidden(str(raw))
    if not s:
        return ""

    u = s.upper()
    u = _strip_noise_prefix_suffix(u)

    if not u:
        return ""

    # Special symbols passthrough
    if is_special_symbol(u):
        return u

    # KSA handling
    if looks_like_ksa(u) or is_ksa(u):
        return normalize_ksa_symbol(u)

    # Validate allowed chars (drop weird artifacts early)
    if not _RE_GLOBAL_ALLOWED.match(u):
        # best-effort cleanup: keep only allowed set
        u = "".join(ch for ch in u if re.match(r"[A-Z0-9\.\-]", ch))
        u = u.strip(".-").strip()

    # Standardize share class separators, conservatively:
    # Only convert dash->dot if it looks like a class suffix (1-2 letters after dash)
    # Examples: BRK-B, RDS-A, GOOG-L? (less common)
    if "-" in u and "." not in u:
        parts = u.split("-")
        if len(parts) == 2 and parts[0] and 1 <= len(parts[1]) <= 2 and parts[1].isalpha():
            u = parts[0] + "." + parts[1]

    return u.strip()


# ---------------------------------------------------------------------------
# List parsing
# ---------------------------------------------------------------------------
def normalize_symbols_list(raw: str, *, limit: int = 0) -> List[str]:
    """Parses a string of tickers (comma/space separated) into a unique normalized list."""
    if not raw:
        return []
    parts = re.split(r"[\s,;]+", str(raw))
    out: List[str] = []
    seen = set()
    for p in parts:
        norm = normalize_symbol(p)
        if norm and norm not in seen:
            seen.add(norm)
            out.append(norm)
            if limit > 0 and len(out) >= limit:
                break
    return out


def market_hint_for(sym_norm: str) -> str:
    """Returns 'KSA' or 'GLOBAL' to help router selection."""
    return "KSA" if is_ksa(sym_norm) else "GLOBAL"


# ---------------------------------------------------------------------------
# Provider formatting helpers
# ---------------------------------------------------------------------------
def _default_exchange() -> str:
    return (_env_str("EODHD_DEFAULT_EXCHANGE", "US").upper() or "US").strip()


def to_yahoo_symbol(sym_norm: str) -> str:
    """
    Yahoo formatting:
    - KSA must be ####.SR
    - Keep special symbols as-is (^GSPC, GC=F, EURUSD=X, BTC-USD)
    - Keep global dotted tickers (7203.T) intact
    """
    s = normalize_symbol(sym_norm)
    return s


def to_finnhub_symbol(sym_norm: str) -> str:
    """
    Finnhub formatting:
    - Finnhub equities are usually plain tickers (AAPL), sometimes exchange-specific by endpoint.
    - For our engine fallback, we:
        - keep special symbols as-is (though you usually route them away from finnhub)
        - strip a trailing '.US' if present
        - do NOT convert 1120.SR (KSA) (your router should block KSA for finnhub)
    """
    s = normalize_symbol(sym_norm)
    if not s:
        return ""
    if is_special_symbol(s):
        return s
    if s.endswith(".US"):
        return s[:-3]
    return s


def to_eodhd_symbol(sym_norm: str, *, default_exchange: Optional[str] = None) -> str:
    """
    EODHD formatting:
    - Global equities generally require 'TICKER.EXCH' (AAPL.US)
    - If symbol already has a dot, we assume it's already exchange-qualified
      (except class tickers like BRK.B; we still treat '.' as class and may need EXCH).
    - KSA remains ####.SR (EODHD supports it)
    - Special symbols passthrough
    """
    s = normalize_symbol(sym_norm)
    if not s or is_special_symbol(s) or is_ksa(s):
        return s

    # If has exchange suffix already like 7203.T, AAPL.US, VOD.L, keep it.
    # But share class like BRK.B should still be suffixed for EODHD (BRK.B.US).
    # Heuristic: if last segment after dot is 1-3 chars and all letters -> likely exchange (US, L, T, HK)
    if "." in s:
        base, last = s.rsplit(".", 1)
        if last.isalpha() and 1 <= len(last) <= 3:
            return s
        # otherwise treat as class; add exchange
        ex = (default_exchange or _default_exchange()).upper()
        return f"{s}.{ex}"

    ex = (default_exchange or _default_exchange()).upper()
    return f"{s}.{ex}"


# ---------------------------------------------------------------------------
# Variants (routing/fallback)
# ---------------------------------------------------------------------------
def symbol_variants(sym_norm: str) -> List[str]:
    """Generic variant list for fallback attempts across providers."""
    u = normalize_symbol(sym_norm)
    if not u:
        return []
    if is_special_symbol(u):
        return [u]

    out: List[str] = [u]

    # KSA variants
    if is_ksa(u):
        if u.endswith(".SR"):
            out.append(u[:-3])  # naked code
        else:
            out.insert(0, f"{u}.SR")

    # Global class variants
    else:
        if "." in u:
            out.append(u.replace(".", "-"))
        if "-" in u:
            out.append(u.replace("-", "."))

    # De-dupe preserve order
    seen = set()
    return [x for x in out if not (x in seen or seen.add(x))]


def yahoo_symbol_variants(sym_norm: str) -> List[str]:
    """Yahoo-first variants."""
    s = normalize_symbol(sym_norm)
    if not s:
        return []
    if is_special_symbol(s):
        return [s]
    out = [to_yahoo_symbol(s)]
    # For class: try dash variant too (some sources accept both)
    if "." in s and not is_ksa(s):
        out.append(s.replace(".", "-"))
    if "-" in s and not is_ksa(s):
        out.append(s.replace("-", "."))
    if is_ksa(s) and s.endswith(".SR"):
        out.append(s[:-3])
    seen = set()
    return [x for x in out if not (x in seen or seen.add(x))]


def finnhub_symbol_variants(sym_norm: str) -> List[str]:
    """Finnhub-first variants (router should avoid special + KSA in finnhub)."""
    s = normalize_symbol(sym_norm)
    if not s:
        return []
    if is_special_symbol(s):
        return [s]
    out = [to_finnhub_symbol(s)]
    # Some engines may keep .US; allow both
    if out[0] and not out[0].endswith(".US") and out[0].isalpha():
        out.append(out[0] + ".US")
    seen = set()
    return [x for x in out if x and not (x in seen or seen.add(x))]


def eodhd_symbol_variants(sym_norm: str, *, default_exchange: Optional[str] = None) -> List[str]:
    """EODHD-first variants optimized for exchange suffix requirements."""
    s = normalize_symbol(sym_norm)
    if not s:
        return []
    if is_special_symbol(s):
        return [s]

    ex = (default_exchange or _default_exchange()).upper()
    out: List[str] = []

    if is_ksa(s):
        base = s.replace(".SR", "")
        out.extend([f"{base}.SR", base])
    else:
        # base without exchange suffix
        base = s
        if "." in s:
            # If already exchange-like, keep it first
            b, last = s.rsplit(".", 1)
            if last.isalpha() and 1 <= len(last) <= 3:
                out.append(s)
                base = b
        # Ensure exchange-qualified first for EODHD
        out.append(to_eodhd_symbol(base, default_exchange=ex))
        out.append(base)

        # Class separator alternates
        if "-" in base:
            dot_base = base.replace("-", ".")
            out.insert(1, to_eodhd_symbol(dot_base, default_exchange=ex))
        if "." in base:
            dash_base = base.replace(".", "-")
            out.insert(1, to_eodhd_symbol(dash_base, default_exchange=ex))

    seen = set()
    return [x for x in out if x and not (x in seen or seen.add(x))]
