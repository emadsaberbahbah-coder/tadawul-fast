#!/usr/bin/env python3
# core/symbols/normalize.py
"""
core/symbols/normalize.py
============================================================
Symbol Normalization — v1.2.0 (PROD SAFE / NO NETWORK)
Financial Leader Edition — Optimized for KSA & Global Markets

Purpose:
- Provide ONE consistent normalization layer for the entire system.
- Translates Arabic numerals and cleans non-printable artifacts.
- Manages exchange suffixes for EODHD, Finnhub, and Yahoo.
- Prevents "Symbol Not Found" errors due to minor formatting differences.

Standard Behavior:
- "1120"      -> "1120.SR" (Standard Saudi)
- "١١٢٠"      -> "1120.SR" (Arabic digits support)
- "AAPL"      -> "AAPL"    (Global/US Default)
- "BRK-B"     -> "BRK.B"   (Standardized class separator)
- "^GSPC"     -> "^GSPC"   (Passthrough for indices)
"""

from __future__ import annotations

import os
import re
from typing import Iterable, List, Optional, Sequence, Tuple

__all__ = [
    "normalize_symbol",
    "normalize_ksa_symbol",
    "normalize_symbols_list",
    "symbol_variants",
    "market_hint_for",
    "is_ksa",
    "is_index_or_fx",
    "to_eodhd_symbol",
    "eodhd_symbol_variants",
]

# Arabic to ASCII translation table
_ARABIC_DIGITS = str.maketrans("٠١٢٣٤٥٦٧٨٩", "0123456789")

# KSA: naked digits or digits.SR (3 to 6 digits)
_KSA_RE = re.compile(r"^\d{3,6}(\.SR)?$", re.IGNORECASE)
_KSA_CODE_ONLY = re.compile(r"^\d{3,6}$")

# Exchange suffix like .US / .L / .TO ... (2..6 chars)
_EXCH_SUFFIX_RE = re.compile(r"^(.+)\.([A-Z0-9]{2,6})$")

# Common noise patterns to strip
_PREFIXES = ("TADAWUL:", "STOCK:", "TICKER:", "INDEX:")
_SUFFIXES = (".TADAWUL", ".JK", ".QA")

def _env_str(name: str, default: str = "") -> str:
    v = os.getenv(name)
    return str(v).strip() if v else default

def is_index_or_fx(sym: str) -> bool:
    """Detects Yahoo-style special symbols for Indices, FX, and Commodities."""
    s = (sym or "").strip().upper()
    if not s: return False
    # Common markers: ^GSPC, EURUSD=X, GC=F, BTC-USD
    return ("^" in s) or ("=" in s) or s.endswith("=X") or s.endswith("=F") or ("/USD" in s)

def is_ksa(sym: str) -> bool:
    """Checks if a symbol is a valid Saudi Tadawul code."""
    s = (sym or "").strip().upper().translate(_ARABIC_DIGITS)
    if not s: return False
    if s.endswith(".SR"): return True
    if s.isdigit(): return True
    return bool(_KSA_RE.match(s))

def normalize_ksa_symbol(symbol: str) -> str:
    """
    STRICT KSA Normalization:
    Ensures Saudi symbols always follow the '####.SR' format.
    """
    s = (symbol or "").strip().upper().translate(_ARABIC_DIGITS)
    if not s: return ""
    
    # Strip common noise prefixes/suffixes
    for p in _PREFIXES:
        if s.startswith(p): s = s.split(":", 1)[1]
    for suf in _SUFFIXES:
        if s.endswith(suf): s = s.replace(suf, "")
        
    s = s.strip()
    
    # Handle already suffixed case
    if s.endswith(".SR"):
        code = s[:-3]
        return f"{code}.SR" if _KSA_CODE_RE_match := _KSA_CODE_ONLY.match(code) else ""
    
    # Handle naked numeric case
    if _KSA_CODE_ONLY.match(s):
        return f"{s}.SR"
    
    return ""

def normalize_symbol(raw: str) -> str:
    """
    Primary Canonical Normalizer:
    - Resolves Arabic digits.
    - Standardizes KSA to .SR.
    - Standardizes global share class separators (BRK-B -> BRK.B).
    - Preserves Index/FX formats.
    - Cleans zero-width spaces and hidden artifacts.
    """
    if not raw: return ""
    
    # 1. Strip hidden Unicode artifacts and normalize digits
    s = str(raw).translate(_ARABIC_DIGITS).strip()
    s = re.sub(r"[\u200b\u200e\u200f\u202a-\u202e]", "", s) 
    
    u = s.upper()
    if not u: return ""

    # 2. Passthrough for special types (Indices/Commodities)
    if is_index_or_fx(u):
        return u

    # 3. Handle KSA specifically
    if is_ksa(u):
        return normalize_ksa_symbol(u)

    # 4. Standardize Global Tickers
    # Strip known prefixes
    for p in _PREFIXES:
        if u.startswith(p): u = u.split(":", 1)[1]
    
    # Standardize share class separators: BRK-B -> BRK.B
    # Only if it's not a KSA stock and looks like a class indicator
    if "-" in u and not is_ksa(u):
        u = u.replace("-", ".")
        
    return u.strip()

def normalize_symbols_list(raw: str, *, limit: int = 0) -> List[str]:
    """Parses a string of tickers (comma or space separated) into a unique list."""
    if not raw: return []
    # Split by whitespace or commas
    parts = re.split(r"[\s,]+", str(raw))
    
    out: List[str] = []
    seen = set()
    
    for p in parts:
        norm = normalize_symbol(p)
        if norm and norm not in seen:
            seen.add(norm)
            out.append(norm)
            if limit > 0 and len(out) >= limit: break
            
    return out

def market_hint_for(sym_norm: str) -> str:
    """Returns 'KSA' or 'GLOBAL' to help router selection."""
    return "KSA" if is_ksa(sym_norm) else "GLOBAL"

def _default_exchange() -> str:
    return _env_str("EODHD_DEFAULT_EXCHANGE", "US").upper() or "US"

def symbol_variants(sym_norm: str) -> List[str]:
    """Generates a list of variant symbols for fallback provider attempts."""
    u = (sym_norm or "").upper()
    if not u: return []
    
    # Indices are usually fixed
    if is_index_or_fx(u): return [u]

    out = [u]
    
    # KSA Fallbacks
    if is_ksa(u):
        if u.endswith(".SR"): 
            out.append(u[:-3]) # naked code
        else: 
            out.insert(0, f"{u}.SR") # force .SR primary
    
    # Global Fallbacks (dot/dash swap for classes)
    elif "." in u:
        out.append(u.replace(".", "-"))
    elif "-" in u:
        out.append(u.replace("-", "."))
        
    # De-dupe keeping order
    seen = set()
    return [x for x in out if not (x in seen or seen.add(x))]

def to_eodhd_symbol(sym_norm: str, *, default_exchange: Optional[str] = None) -> str:
    """Formats a global ticker for EODHD's required 'TICKER.EXCH' format."""
    s = (sym_norm or "").upper()
    if not s or is_index_or_fx(s) or is_ksa(s) or "." in s:
        return s
    ex = (default_exchange or _default_exchange())
    return f"{s}.{ex}"

def eodhd_symbol_variants(sym_norm: str, *, default_exchange: Optional[str] = None) -> List[str]:
    """Specific variant generator optimized for EODHD routing."""
    s = (sym_norm or "").upper()
    if not s or is_index_or_fx(s):
        return [s]
        
    if is_ksa(s):
        base = s.replace(".SR", "")
        return [f"{base}.SR", base]

    ex = (default_exchange or _default_exchange())
    base = s.split(".")[0]
    
    # For global symbols, always try the exchange-suffixed version first
    variants = [f"{base}.{ex}", base]
    
    # Handle class share separators for EODHD (they often prefer dash in base)
    if "-" in base: 
        dot_base = base.replace("-", ".")
        variants.insert(1, f"{dot_base}.{ex}")
    if "." in base: 
        dash_base = base.replace(".", "-")
        variants.insert(1, f"{dash_base}.{ex}")
    
    seen = set()
    return [x for x in variants if not (x in seen or seen.add(x))]
