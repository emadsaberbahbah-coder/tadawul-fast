#!/usr/bin/env python3
# core/symbols/normalize.py
"""
================================================================================
Symbol Normalization — v5.2.0 (ENTERPRISE ALIGNED)
================================================================================
Comprehensive Symbol Normalization for KSA + Global Markets, with provider-safe
formatting helpers and robust handling of share-class tickers (e.g., BRK.B).

Key upgrades in v5.2.0
- ✅ Remove duplicate env helper definitions (single source of truth)
- ✅ Fix EODHD formatting bug: share-class tickers like BRK.B now correctly become BRK.B.US
   (previous logic treated ".B" as an exchange suffix and returned BRK.B without .US)
- ✅ extract_base_symbol no longer strips share class by mistake; strips only REAL exchange suffixes
- ✅ to_yahoo_symbol now strips ".US" (and other exchange suffixes) when needed (provider-safe)
- ✅ Stronger exchange-suffix detection via split_symbol_exchange()
- ✅ New normalize_symbol_for_provider() for consistent provider routing (optional usage)

Performance
- Pure Python + regex + lru_cache (microsecond latency)
- Optional orjson for fast JSON env parsing
"""

from __future__ import annotations

import os
import re
from datetime import datetime
from enum import Enum
from functools import lru_cache
from typing import Any, Dict, List, Optional, Set, Tuple, Union

# ---------------------------------------------------------------------------
# High-Performance JSON fallback
# ---------------------------------------------------------------------------
try:
    import orjson  # type: ignore

    def json_loads(data: Union[str, bytes]) -> Any:
        return orjson.loads(data)
except Exception:
    import json  # type: ignore

    def json_loads(data: Union[str, bytes]) -> Any:
        return json.loads(data)

__version__ = "5.2.0"

__all__ = [
    # Core enums
    "MarketType",
    "AssetClass",
    "SymbolQuality",
    # Core functions
    "normalize_symbol",
    "normalize_ksa_symbol",
    "normalize_symbols_list",
    "symbol_variants",
    "market_hint_for",
    "detect_market_type",
    "detect_asset_class",
    "validate_symbol",
    # Detection helpers
    "is_ksa",
    "looks_like_ksa",
    "is_index",
    "is_fx",
    "is_commodity_future",
    "is_crypto",
    "is_etf",
    "is_special_symbol",
    "is_isin",
    "is_cusip",
    "is_sedol",
    "is_option",
    # Options helpers
    "parse_occ_option",
    # Provider formatting
    "to_yahoo_symbol",
    "to_finnhub_symbol",
    "to_eodhd_symbol",
    "to_bloomberg_symbol",
    "to_reuters_symbol",
    "to_google_symbol",
    "to_tradingview_symbol",
    # Provider-specific variants
    "yahoo_symbol_variants",
    "finnhub_symbol_variants",
    "eodhd_symbol_variants",
    "bloomberg_symbol_variants",
    "reuters_symbol_variants",
    # Utility functions
    "extract_base_symbol",
    "extract_exchange_code",
    "split_symbol_exchange",
    "standardize_share_class",
    "get_primary_exchange",
    "get_currency_from_symbol",
    "get_mic_code",
    # Provider-aware normalization (optional)
    "normalize_symbol_for_provider",
    # Version
    "__version__",
]

# =============================================================================
# Enums
# =============================================================================


class MarketType(Enum):
    """Primary market classification."""
    KSA = "ksa"
    US = "us"
    UK = "uk"
    JP = "jp"
    HK = "hk"
    CN = "cn"
    IN = "in"
    DE = "de"
    FR = "fr"
    AU = "au"
    CA = "ca"
    BR = "br"
    ZA = "za"
    AE = "ae"
    KW = "kw"
    QA = "qa"
    GLOBAL = "global"
    SPECIAL = "special"


class AssetClass(Enum):
    """Asset class classification."""
    EQUITY = "equity"
    ETF = "etf"
    INDEX = "index"
    FOREX = "forex"
    COMMODITY = "commodity"
    CRYPTO = "crypto"
    BOND = "bond"
    FUND = "fund"
    OPTION = "option"
    FUTURE = "future"
    WARRANT = "warrant"
    REIT = "reit"
    ADR = "adr"
    PREFERRED = "preferred"
    UNKNOWN = "unknown"


class SymbolQuality(Enum):
    """Symbol validation quality."""
    EXCELLENT = "excellent"
    GOOD = "good"
    FAIR = "fair"
    POOR = "poor"
    INVALID = "invalid"


# =============================================================================
# Env helpers (single source of truth)
# =============================================================================

_TRUTHY = {"1", "true", "yes", "y", "on", "t", "enabled", "enable"}
_FALSY = {"0", "false", "no", "n", "off", "f", "disabled", "disable"}


def _env_str(name: str, default: str = "") -> str:
    v = os.getenv(name)
    return default if v is None else str(v).strip()


def _env_bool(name: str, default: bool = False) -> bool:
    raw = _env_str(name, "")
    if raw == "":
        return bool(default)
    s = raw.strip().lower()
    if s in _TRUTHY:
        return True
    if s in _FALSY:
        return False
    return bool(default)


def _env_list(name: str) -> Tuple[str, ...]:
    raw = _env_str(name, "")
    if not raw:
        return tuple()
    parts = [p.strip() for p in raw.split(",") if p.strip()]
    return tuple(parts)


def _env_dict(name: str) -> Dict[str, str]:
    raw = _env_str(name, "")
    if not raw:
        return {}
    try:
        data = json_loads(raw)
        if isinstance(data, dict):
            return {str(k).upper(): str(v) for k, v in data.items()}
    except Exception:
        pass
    return {}


# =============================================================================
# Unicode + Digit Normalization
# =============================================================================

_ARABIC_INDIC = "٠١٢٣٤٥٦٧٨٩"
_EASTERN_ARABIC_INDIC = "۰۱۲۳۴۵۶۷۸۹"
_ASCII_DIGITS = "0123456789"

_DIGIT_TRANS = str.maketrans(
    _ARABIC_INDIC + _EASTERN_ARABIC_INDIC,
    _ASCII_DIGITS + _ASCII_DIGITS,
)

_HIDDEN_CHARS_RE = re.compile(
    r"[\u200b\u200c\u200d\u200e\u200f\u202a\u202b\u202c\u202d\u202e"
    r"\u2066\u2067\u2068\u2069\ufeff\u00a0\u0640\u2000-\u200a\u202f\u205f]"
)

_ARABIC_THOUSANDS = "\u066c"
_ARABIC_DECIMAL = "\u066b"

_DASH_CHARS = "-\u2010\u2011\u2012\u2013\u2014\u2015\u2212"
_DASH_RE = re.compile(f"[{_DASH_CHARS}]")

_DOT_CHARS = ".\u2024\u2027\u2219\u22c5"
_DOT_RE = re.compile(f"[{_DOT_CHARS}]")

_SLASH_CHARS = "/\u2215\u2044"
_SLASH_RE = re.compile(f"[{_SLASH_CHARS}]")

_SPACE_RE = re.compile(r"\s+")

# =============================================================================
# Pattern Definitions
# =============================================================================

# KSA Tadawul patterns
KSA_CODE_ONLY_RE = re.compile(r"^\d{3,6}$", re.IGNORECASE)
KSA_SR_RE = re.compile(r"^\d{3,6}\.SR$", re.IGNORECASE)
KSA_TADAWUL_RE = re.compile(r"^TADAWUL:(\d{3,6})(\.SR)?$", re.IGNORECASE)

# Standard Identifiers
ISIN_RE = re.compile(r"^[A-Z]{2}[A-Z0-9]{9}\d$", re.IGNORECASE)
CUSIP_RE = re.compile(r"^[0-9A-Z]{9}$", re.IGNORECASE)
SEDOL_RE = re.compile(r"^[0-9BCDFGHJKLMNPQRSTVWXYZ]{7}$", re.IGNORECASE)

# OCC Options
OCC_OPTION_RE = re.compile(r"^([A-Z]{1,6})(\d{6})([CP])(\d{8})$", re.IGNORECASE)

# Index patterns
INDEX_CARET_RE = re.compile(r"^\^[A-Z0-9]+$", re.IGNORECASE)
INDEX_SUFFIX_RE = re.compile(r"^([A-Z0-9]+)\.INDX$", re.IGNORECASE)
INDEX_COMMON: Dict[str, str] = {
    "SPX": "^GSPC", "SP500": "^GSPC", "DJI": "^DJI", "DOW": "^DJI", "NDX": "^NDX",
    "NASDAQ": "^IXIC", "RUT": "^RUT", "FTSE": "^FTSE", "DAX": "^GDAXI", "CAC": "^FCHI",
    "NIKKEI": "^N225", "N225": "^N225", "HSI": "^HSI", "SSEC": "^SSEC", "TASI": "^TASI",
    "NOMU": "^NOMU", "VIX": "^VIX",
}

# Forex patterns
FX_EQUAL_RE = re.compile(r"^([A-Z]{3,6})=[X]$", re.IGNORECASE)
FX_SLASH_RE = re.compile(r"^([A-Z]{3})/([A-Z]{3})$", re.IGNORECASE)
FX_DASH_RE = re.compile(r"^([A-Z]{3})-([A-Z]{3})$", re.IGNORECASE)
FX_SUFFIX_RE = re.compile(r"^([A-Z]{3,6})\.FOREX$", re.IGNORECASE)
FX_COMMON_PAIRS: Set[str] = {
    "EURUSD", "GBPUSD", "USDJPY", "USDCHF", "AUDUSD", "USDCAD", "NZDUSD",
    "EURGBP", "EURJPY", "GBPJPY", "CHFJPY", "EURCHF", "GBPCHF", "AUDJPY",
}

# Commodity futures patterns
FUTURE_EQUAL_RE = re.compile(r"^([A-Z0-9]{1,6})=[F]$", re.IGNORECASE)
FUTURE_SUFFIX_RE = re.compile(r"^([A-Z0-9]{1,6})\.(COMM|COM|FUT)$", re.IGNORECASE)
COMMODITY_CODES: Dict[str, str] = {
    "GC": "gold", "SI": "silver", "PL": "platinum", "PA": "palladium",
    "CL": "wti_crude", "BZ": "brent_crude", "NG": "natural_gas", "HO": "heating_oil",
    "RB": "gasoline", "ZC": "corn", "ZW": "wheat", "ZS": "soybeans",
    "ZM": "soybean_meal", "ZL": "soybean_oil", "ZR": "rough_rice", "ZO": "oats",
    "KE": "kc_wheat", "MW": "spring_wheat", "CC": "cocoa", "KC": "coffee",
    "CT": "cotton", "OJ": "orange_juice", "SB": "sugar", "LB": "lumber",
    "HG": "copper", "ALI": "aluminium", "NICKEL": "nickel", "ZINC": "zinc",
    "LEAD": "lead", "TIN": "tin",
}

# Crypto patterns
CRYPTO_DASH_RE = re.compile(r"^([A-Z0-9]{2,15})-([A-Z]{2,10})$", re.IGNORECASE)
CRYPTO_SUFFIX_RE = re.compile(r"^([A-Z0-9]{2,15})\.(CRYPTO|CC|C)$", re.IGNORECASE)
CRYPTO_COMMON: Dict[str, str] = {
    "BTC": "bitcoin", "ETH": "ethereum", "XRP": "ripple", "LTC": "litecoin",
    "BCH": "bitcoin_cash", "ADA": "cardano", "DOT": "polkadot", "LINK": "chainlink",
    "BNB": "binance", "XLM": "stellar", "DOGE": "dogecoin", "UNI": "uniswap",
    "SOL": "solana", "MATIC": "polygon", "AVAX": "avalanche", "ATOM": "cosmos",
    "ALGO": "algorand", "VET": "vechain", "FIL": "filecoin", "TRX": "tron",
    "USDT": "tether", "USDC": "usd_coin", "SHIB": "shiba_inu", "LUNA": "terra",
}

# ETF patterns
ETF_SUFFIX_RE = re.compile(r"^([A-Z0-9]+)\.(ETF|ET)$", re.IGNORECASE)
ETF_COMMON_PREFIX: Set[str] = {"SPY", "QQQ", "IVV", "VTI", "VOO", "BND", "EFA", "IWM", "AGG", "GLD", "SLV"}

# Exchange suffixes mapping (NOTE: keys must be UPPER and include leading dot)
EXCHANGE_SUFFIXES: Dict[str, str] = {
    # Americas
    ".US": "US", ".NYSE": "US", ".N": "US", ".NASDAQ": "US", ".OQ": "US", ".NM": "US", ".NG": "US",
    ".TO": "CA", ".V": "CA", ".CNQ": "CA",
    ".MX": "MX", ".SA": "BR", ".BA": "AR",

    # EMEA
    ".L": "UK", ".LSE": "UK", ".LN": "UK",
    ".PA": "FR", ".FP": "FR",
    ".DE": "DE", ".F": "DE", ".BE": "DE", ".DU": "DE", ".HM": "DE",
    ".SW": "CH", ".VX": "CH",
    ".AS": "NL", ".BR": "BE",
    ".MC": "ES",
    ".MI": "IT", ".IM": "IT",
    ".CO": "DK", ".ST": "SE", ".OL": "NO", ".HE": "FI",
    ".WA": "PL", ".PR": "CZ", ".BU": "HU",
    ".AT": "AT", ".VI": "AT",
    ".IR": "IE", ".DUB": "IE",
    ".ZA": "ZA", ".JSE": "ZA",
    ".TA": "IL", ".TASE": "IL",
    ".SAU": "SA", ".SR": "SA", ".TADAWUL": "SA",
    ".AE": "AE", ".DFM": "AE", ".ADX": "AE",
    ".QA": "QA", ".QE": "QA",
    ".KW": "KW", ".KSE": "KW",
    ".EG": "EG", ".EGX": "EG",

    # Asia Pacific
    ".T": "JP", ".TYO": "JP",
    ".HK": "HK", ".HKG": "HK",
    ".SS": "CN", ".SHG": "CN",
    ".SZ": "CN", ".SHE": "CN",
    ".NS": "IN", ".NSE": "IN",
    ".BO": "IN", ".BSE": "IN",
    ".KS": "KR", ".KQ": "KR", ".KOSDAQ": "KR",
    ".TW": "TW", ".TWO": "TW",
    ".SI": "SG", ".SGX": "SG",
    ".KL": "MY", ".KLSE": "MY",
    ".JK": "ID", ".IDX": "ID",
    ".SET": "TH", ".BK": "TH",
    ".VN": "VN", ".HOSE": "VN",
    ".PS": "PH", ".PSE": "PH",
    ".AU": "AU", ".AX": "AU", ".ASX": "AU",
    ".NZ": "NZ", ".NZSE": "NZ",
}

# ISO 10383 MIC Code Mapping
MIC_MAPPINGS: Dict[str, str] = {
    "US": "XNYS", "NASDAQ": "XNAS", "CA": "XTSE", "MX": "XMEX", "BR": "BVMF",
    "UK": "XLON", "FR": "XPAR", "DE": "XETR", "CH": "XSWX", "SA": "XSAU",
    "AE": "XDFM", "JP": "XTKS", "HK": "XHKG", "CN": "XSHG", "IN": "XNSE",
    "AU": "XASX", "KR": "XKRX", "SG": "XSES",
}

# Share class patterns
CLASS_DASH_RE = re.compile(r"^([A-Z]+)-([A-Z])$", re.IGNORECASE)
CLASS_DOT_RE = re.compile(r"^([A-Z]+)\.([A-Z])$", re.IGNORECASE)

# Common prefixes to strip
COMMON_PREFIXES = (
    "TADAWUL:", "STOCK:", "TICKER:", "INDEX:", "NYSE:", "NASDAQ:", "OTC:",
    "LSE:", "TSX:", "ASX:", "HKEX:", "SGX:", "B3:", "JSE:", "DFM:", "ADX:",
    "FOREX:", "FX:", "CRYPTO:", "CC:", "FUT:", "COMM:", "INDX:", "ETF:",
)

# Common suffixes to strip (but not exchange suffixes)
COMMON_SUFFIXES = (
    ".TADAWUL", ".STOCK", ".TICKER", ".INDEX",
)

ALLOWED_CHARS_RE = re.compile(r"^[A-Z0-9\.\-\^=\/\@\#]+$", re.IGNORECASE)

# =============================================================================
# Custom mappings from environment
# =============================================================================

CUSTOM_EXCHANGE_MAP = _env_dict("SYMBOL_EXCHANGE_MAP_JSON")
CUSTOM_INDEX_MAP = _env_dict("SYMBOL_INDEX_MAP_JSON")
CUSTOM_FX_MAP = _env_dict("SYMBOL_FX_MAP_JSON")
CUSTOM_COMMODITY_MAP = _env_dict("SYMBOL_COMMODITY_MAP_JSON")
CUSTOM_CRYPTO_MAP = _env_dict("SYMBOL_CRYPTO_MAP_JSON")

if CUSTOM_EXCHANGE_MAP:
    EXCHANGE_SUFFIXES.update(CUSTOM_EXCHANGE_MAP)
if CUSTOM_INDEX_MAP:
    INDEX_COMMON.update({str(k).upper(): str(v) for k, v in CUSTOM_INDEX_MAP.items()})
if CUSTOM_FX_MAP:
    FX_COMMON_PAIRS.update({str(k).upper().replace("/", "").replace("-", "") for k in CUSTOM_FX_MAP.keys()})
if CUSTOM_COMMODITY_MAP:
    COMMODITY_CODES.update({str(k).upper(): str(v) for k, v in CUSTOM_COMMODITY_MAP.items()})
if CUSTOM_CRYPTO_MAP:
    CRYPTO_COMMON.update({str(k).upper(): str(v) for k, v in CUSTOM_CRYPTO_MAP.items()})

EXTRA_STRIP_SUFFIXES = _env_list("NORMALIZE_STRIP_SUFFIXES")
EXTRA_STRIP_PREFIXES = _env_list("NORMALIZE_STRIP_PREFIXES")

# =============================================================================
# Small utilities
# =============================================================================


def _unique_preserve_order(items: List[str]) -> List[str]:
    seen: Set[str] = set()
    out: List[str] = []
    for x in items:
        if not x:
            continue
        if x in seen:
            continue
        seen.add(x)
        out.append(x)
    return out


# =============================================================================
# Core Unicode Cleaning Functions
# =============================================================================

@lru_cache(maxsize=20000)
def clean_unicode(text: str) -> str:
    if not text:
        return ""
    s = str(text).strip()
    s = s.translate(_DIGIT_TRANS)
    s = _HIDDEN_CHARS_RE.sub("", s)
    s = s.replace(_ARABIC_THOUSANDS, ",").replace(_ARABIC_DECIMAL, ".")
    s = _DASH_RE.sub("-", s)
    s = _DOT_RE.sub(".", s)
    s = _SLASH_RE.sub("/", s)
    s = _SPACE_RE.sub(" ", s).strip()
    return s


@lru_cache(maxsize=20000)
def strip_noise_prefix_suffix(s: str) -> str:
    if not s:
        return ""
    u = s.upper().strip()
    for p in COMMON_PREFIXES + EXTRA_STRIP_PREFIXES:
        if u.startswith(p.upper()):
            u = u[len(p):].strip()
            break
    for suf in COMMON_SUFFIXES + EXTRA_STRIP_SUFFIXES:
        if u.endswith(suf.upper()):
            u = u[:-len(suf)].strip()
            break
    return u


# =============================================================================
# Detection Functions
# =============================================================================

@lru_cache(maxsize=20000)
def looks_like_ksa(symbol: str) -> bool:
    s = clean_unicode(symbol).upper()
    if not s:
        return False
    if s.startswith("TADAWUL:"):
        return True
    if s.endswith(".SR"):
        code = s[:-3].strip()
        return bool(KSA_CODE_ONLY_RE.match(code))
    return bool(KSA_CODE_ONLY_RE.match(s))


@lru_cache(maxsize=20000)
def is_ksa(symbol: str) -> bool:
    s = clean_unicode(symbol).upper()
    if not s:
        return False
    return bool(KSA_TADAWUL_RE.match(s) or KSA_SR_RE.match(s) or KSA_CODE_ONLY_RE.match(s))


@lru_cache(maxsize=20000)
def is_isin(symbol: str) -> bool:
    return bool(ISIN_RE.match(clean_unicode(symbol).upper()))


@lru_cache(maxsize=20000)
def is_cusip(symbol: str) -> bool:
    return bool(CUSIP_RE.match(clean_unicode(symbol).upper()))


@lru_cache(maxsize=20000)
def is_sedol(symbol: str) -> bool:
    return bool(SEDOL_RE.match(clean_unicode(symbol).upper()))


@lru_cache(maxsize=20000)
def is_option(symbol: str) -> bool:
    return bool(OCC_OPTION_RE.match(clean_unicode(symbol).upper()))


def parse_occ_option(symbol: str) -> Optional[Dict[str, Any]]:
    s = clean_unicode(symbol).upper()
    m = OCC_OPTION_RE.match(s)
    if not m:
        return None
    underlying, exp, right, strike_str = m.groups()
    try:
        exp_date = datetime.strptime(exp, "%y%m%d").date().isoformat()
    except Exception:
        return None
    return {
        "underlying": underlying,
        "expiration": exp_date,
        "type": "CALL" if right == "C" else "PUT",
        "strike": float(strike_str) / 1000.0,
    }


@lru_cache(maxsize=20000)
def is_index(symbol: str) -> bool:
    s = clean_unicode(symbol).upper()
    if not s:
        return False
    if INDEX_CARET_RE.match(s) or INDEX_SUFFIX_RE.match(s):
        return True
    return s in INDEX_COMMON or s in set(INDEX_COMMON.values())


@lru_cache(maxsize=20000)
def is_fx(symbol: str) -> bool:
    s = clean_unicode(symbol).upper()
    if not s:
        return False
    if FX_EQUAL_RE.match(s) or FX_SLASH_RE.match(s) or FX_DASH_RE.match(s) or FX_SUFFIX_RE.match(s):
        return True
    base = s.replace("=X", "").replace("/", "").replace("-", "")
    return base in FX_COMMON_PAIRS


@lru_cache(maxsize=20000)
def is_commodity_future(symbol: str) -> bool:
    s = clean_unicode(symbol).upper()
    if not s:
        return False
    if FUTURE_EQUAL_RE.match(s) or FUTURE_SUFFIX_RE.match(s):
        return True
    base = s.replace("=F", "").replace(".COMM", "").replace(".COM", "").replace(".FUT", "")
    return base in COMMODITY_CODES


@lru_cache(maxsize=20000)
def is_crypto(symbol: str) -> bool:
    s = clean_unicode(symbol).upper()
    if not s:
        return False
    if CRYPTO_DASH_RE.match(s) or CRYPTO_SUFFIX_RE.match(s):
        return True
    base = s.split("-")[0] if "-" in s else s
    return base in CRYPTO_COMMON


@lru_cache(maxsize=20000)
def is_etf(symbol: str) -> bool:
    s = clean_unicode(symbol).upper()
    if not s:
        return False
    if ETF_SUFFIX_RE.match(s):
        return True
    base = s.split(".")[0] if "." in s else s
    # if symbol is like SPY.US, base=SPY -> should detect
    return base in ETF_COMMON_PREFIX


@lru_cache(maxsize=20000)
def is_special_symbol(symbol: str) -> bool:
    return any([
        is_index(symbol),
        is_fx(symbol),
        is_commodity_future(symbol),
        is_crypto(symbol),
        is_isin(symbol),
        is_option(symbol),
    ])


# =============================================================================
# Exchange / suffix parsing (critical for correctness)
# =============================================================================

@lru_cache(maxsize=20000)
def split_symbol_exchange(symbol: str) -> Tuple[str, Optional[str]]:
    """
    Split (base, exchange_suffix) ONLY when the suffix is a REAL exchange suffix
    recognized in EXCHANGE_SUFFIXES, or KSA .SR.

    Examples:
      AAPL.US   -> ("AAPL", "US")
      2222.SR   -> ("2222", "SR")
      BRK.B     -> ("BRK.B", None)   (share class, NOT an exchange suffix)
      BRK.B.US  -> ("BRK.B", "US")
    """
    s = normalize_symbol(symbol)
    if not s or "." not in s:
        return s, None

    base, suf = s.rsplit(".", 1)
    key = f".{suf.upper()}"
    if key in EXCHANGE_SUFFIXES:
        return base, suf.upper()

    # Not a known exchange suffix -> keep as part of base
    return s, None


@lru_cache(maxsize=20000)
def extract_exchange_code(symbol: str) -> Optional[str]:
    _, ex = split_symbol_exchange(symbol)
    return ex


# =============================================================================
# Advanced Detection
# =============================================================================

@lru_cache(maxsize=20000)
def detect_market_type(symbol: str) -> MarketType:
    s = normalize_symbol(symbol)
    if not s:
        return MarketType.GLOBAL
    if is_special_symbol(s):
        return MarketType.SPECIAL
    if is_ksa(s):
        return MarketType.KSA

    _, ex = split_symbol_exchange(s)
    if ex:
        key = f".{ex.upper()}"
        market_code = EXCHANGE_SUFFIXES.get(key, "")
        for market in MarketType:
            if market.value.upper() == market_code.upper():
                return market

    # Heuristic for simple US tickers
    if "." not in s and s.isalpha() and len(s) <= 5:
        return MarketType.US

    return MarketType.GLOBAL


@lru_cache(maxsize=20000)
def detect_asset_class(symbol: str) -> AssetClass:
    s = normalize_symbol(symbol)
    if not s:
        return AssetClass.UNKNOWN

    if is_index(s):
        return AssetClass.INDEX
    if is_fx(s):
        return AssetClass.FOREX
    if is_commodity_future(s):
        return AssetClass.COMMODITY
    if is_crypto(s):
        return AssetClass.CRYPTO
    if is_etf(s):
        return AssetClass.ETF
    if is_option(s):
        return AssetClass.OPTION

    if s.endswith(".REIT") or "REIT" in s:
        return AssetClass.REIT
    if s.endswith(".ADR"):
        return AssetClass.ADR

    return AssetClass.EQUITY


def validate_symbol(symbol: str) -> Tuple[SymbolQuality, Optional[str]]:
    try:
        s = clean_unicode(symbol)
        if not s:
            return SymbolQuality.INVALID, None

        if not ALLOWED_CHARS_RE.match(s.upper()):
            return SymbolQuality.POOR, normalize_symbol(s)

        norm = normalize_symbol(s)
        if not norm:
            return SymbolQuality.INVALID, None

        if is_option(norm) or is_isin(norm) or is_cusip(norm):
            return SymbolQuality.EXCELLENT, norm

        if len(norm) < 1 or len(norm) > 35:
            return SymbolQuality.FAIR, norm

        if is_special_symbol(norm):
            return SymbolQuality.GOOD, norm

        if is_ksa(norm):
            if KSA_SR_RE.match(norm):
                return SymbolQuality.EXCELLENT, norm
            return SymbolQuality.GOOD, norm

        # If it has a known exchange suffix -> good
        _, ex = split_symbol_exchange(norm)
        if ex:
            return SymbolQuality.GOOD, norm

        # Plain US ticker heuristic
        if norm.isalpha() and 1 <= len(norm) <= 5:
            return SymbolQuality.EXCELLENT, norm

        return SymbolQuality.FAIR, norm
    except Exception:
        return SymbolQuality.INVALID, None


# =============================================================================
# Core Normalization
# =============================================================================

@lru_cache(maxsize=20000)
def normalize_ksa_symbol(symbol: str) -> str:
    s = clean_unicode(symbol).upper()
    if not s:
        return ""

    if s.startswith("TADAWUL:"):
        s = s.split(":", 1)[1].strip()

    for suf in (".TADAWUL", ".SA", ".SAU"):
        if s.endswith(suf):
            s = s[:-len(suf)].strip()
            break

    if s.endswith(".SR"):
        code = s[:-3].strip()
        return f"{code}.SR" if KSA_CODE_ONLY_RE.match(code) else ""

    if KSA_CODE_ONLY_RE.match(s):
        return f"{s}.SR"

    return ""


def _default_exchange() -> str:
    # Used mainly for EODHD formatting and variants
    return (_env_str("EODHD_DEFAULT_EXCHANGE", "US").upper() or "US").strip()


@lru_cache(maxsize=40000)
def normalize_symbol(symbol: str) -> str:
    """
    Canonical "neutral" symbol:
    - KSA -> always ####.SR
    - Special -> normalized forms (FX -> EURUSD=X, indices -> ^GSPC)
    - Equities -> keep as close as possible to user intent
      (NO forced .US unless you enable it explicitly).
    """
    if not symbol:
        return ""
    s = clean_unicode(symbol)
    if not s:
        return ""

    s = strip_noise_prefix_suffix(s)
    if not s:
        return ""
    u = s.upper()

    # Special symbols
    if is_special_symbol(u) and not is_isin(u) and not is_option(u):
        if "/" in u and is_fx(u):
            a, b = u.split("/", 1)
            return f"{a}{b}=X"
        if not u.startswith("^") and is_index(u) and u in INDEX_COMMON:
            return INDEX_COMMON[u]
        return u

    # KSA canonicalization (must be early and final)
    if looks_like_ksa(u) or is_ksa(u):
        k = normalize_ksa_symbol(u)
        return k or u

    # Share class BRK-B -> BRK.B
    if "-" in u:
        parts = u.split("-")
        if len(parts) == 2 and len(parts[1]) == 1 and parts[1].isalpha():
            u = f"{parts[0]}.{parts[1]}"

    # Keep exchange suffix only if it is a known exchange suffix
    if "." in u:
        base, suf = u.rsplit(".", 1)
        key = f".{suf.upper()}"
        if key in EXCHANGE_SUFFIXES:
            u = f"{base}.{suf.upper()}"

    # Final safe char filter
    u = "".join(c for c in u if c.isalnum() or c in ".-^=/")
    u = u.strip(".-")

    # Optional: force default exchange suffix for plain equities (OFF by default)
    # If you want canonical AAPL.US everywhere, set:
    #   NORMALIZE_DEFAULT_EQUITY_EXCHANGE_SUFFIX=US
    default_equity_ex = _env_str("NORMALIZE_DEFAULT_EQUITY_EXCHANGE_SUFFIX", "").upper().strip()
    if default_equity_ex:
        if "." not in u and not is_special_symbol(u) and not is_ksa(u):
            # only apply to plausible equity tickers
            if u.isalpha() and 1 <= len(u) <= 8:
                u = f"{u}.{default_equity_ex}"

    return u


def normalize_symbols_list(
    symbols: Union[str, List[str]],
    *,
    limit: int = 0,
    unique: bool = True,
    validate: bool = False,
) -> List[str]:
    if isinstance(symbols, str):
        parts = re.split(r"[\s,;|]+", symbols)
    else:
        parts = list(symbols)

    result: List[str] = []
    seen: Set[str] = set()

    for p in parts:
        if not p or not str(p).strip():
            continue
        norm = normalize_symbol(p)
        if not norm:
            continue

        if validate:
            quality, _ = validate_symbol(p)
            if quality in (SymbolQuality.INVALID, SymbolQuality.POOR):
                continue

        if unique:
            if norm not in seen:
                seen.add(norm)
                result.append(norm)
        else:
            result.append(norm)

        if limit > 0 and len(result) >= limit:
            break

    return result


# =============================================================================
# Utility Functions
# =============================================================================

def extract_base_symbol(symbol: str) -> str:
    """
    Base symbol with *exchange suffix removed only when it's a real exchange suffix*.
    Does NOT remove share-class (BRK.B stays BRK.B).
    """
    s = normalize_symbol(symbol)
    if not s:
        return ""

    # Preserve canonical KSA suffix for provider safety
    if is_ksa(s) and s.endswith(".SR"):
        return s

    base, ex = split_symbol_exchange(s)
    return base if ex else s


def standardize_share_class(symbol: str) -> str:
    """
    BRK-B -> BRK.B, BF-B -> BF.B
    """
    s = normalize_symbol(symbol)
    if not s:
        return ""
    if "-" in s:
        parts = s.split("-")
        if len(parts) == 2 and len(parts[1]) == 1:
            return f"{parts[0]}.{parts[1]}"
    return s


def get_primary_exchange(symbol: str) -> Optional[str]:
    s = normalize_symbol(symbol)
    if not s:
        return None
    if is_ksa(s):
        return "SAUDI"
    _, ex = split_symbol_exchange(s)
    if ex:
        key = f".{ex.upper()}"
        return EXCHANGE_SUFFIXES.get(key, None)  # e.g., "US", "UK", "JP"
    if is_special_symbol(s):
        return "SPECIAL"
    return "US" if (s.isalpha() and len(s) <= 5) else None


def get_mic_code(symbol: str) -> Optional[str]:
    exch = get_primary_exchange(symbol)
    if not exch:
        return None
    if exch == "SAUDI":
        return "XSAU"
    return MIC_MAPPINGS.get(exch, None)


def get_currency_from_symbol(symbol: str) -> Optional[str]:
    s = normalize_symbol(symbol)
    if not s:
        return None

    if is_fx(s):
        base = extract_base_symbol(s).replace("/", "").replace("-", "").replace("=X", "")
        return base[:3] if len(base) >= 3 else None

    if is_ksa(s):
        return "SAR"

    exch = get_primary_exchange(s)
    if exch == "US":
        return "USD"
    if exch == "UK":
        return "GBP"
    if exch == "JP":
        return "JPY"
    if exch in {"DE", "FR", "IT", "ES", "NL", "BE", "AT", "FI", "IE", "PT"}:
        return "EUR"
    return None


@lru_cache(maxsize=20000)
def market_hint_for(symbol: str) -> str:
    if is_ksa(symbol):
        return "KSA"
    if is_special_symbol(symbol):
        return "SPECIAL"
    return "GLOBAL"


# =============================================================================
# Provider Formatting Functions
# =============================================================================

@lru_cache(maxsize=20000)
def to_yahoo_symbol(symbol: str) -> str:
    """
    Yahoo (yfinance) generally expects:
      - US equities: AAPL (NOT AAPL.US)
      - KSA: 2222.SR
      - Indices: ^GSPC etc
      - FX: EURUSD=X
      - Futures: GC=F
      - Crypto: BTC-USD
    """
    s = normalize_symbol(symbol)
    if not s:
        return ""

    if is_ksa(s):
        return s if s.endswith(".SR") else f"{s}.SR"

    if is_index(s):
        if s in INDEX_COMMON:
            return INDEX_COMMON[s]
        return f"^{s}" if not s.startswith("^") else s

    if is_fx(s):
        base = extract_base_symbol(s).replace("/", "").replace("-", "")
        base = base.replace("=X", "")
        return f"{base}=X" if len(base) == 6 else s

    if is_commodity_future(s):
        return f"{extract_base_symbol(s)}=F"

    if is_crypto(s):
        return s if "-" in s else f"{s}-USD"

    # Strip known exchange suffix like .US for Yahoo equity
    base, ex = split_symbol_exchange(s)
    if ex == "US":
        return base

    return s


@lru_cache(maxsize=20000)
def to_finnhub_symbol(symbol: str) -> str:
    s = normalize_symbol(symbol)
    if not s:
        return ""
    if is_special_symbol(s):
        return s

    base, ex = split_symbol_exchange(s)
    # Finnhub often wants plain ticker for US
    if ex == "US":
        return base
    return s


@lru_cache(maxsize=20000)
def to_eodhd_symbol(symbol: str, *, default_exchange: Optional[str] = None) -> str:
    """
    EODHD typically expects Equity symbols as: TICKER.EXCHANGE (e.g., AAPL.US, 2222.SR)

    v5.2.0 FIX:
    - Share-class tickers like BRK.B are NOT an exchange suffix.
      We now append the default exchange: BRK.B -> BRK.B.US
    """
    s = normalize_symbol(symbol)
    if not s:
        return ""

    if is_special_symbol(s):
        return s

    if is_ksa(s):
        return s if s.endswith(".SR") else f"{s}.SR"

    base, ex = split_symbol_exchange(s)
    if ex:
        # already has a real exchange suffix
        return f"{base}.{ex}"

    ex2 = (default_exchange or _default_exchange()).upper()
    return f"{s}.{ex2}"


@lru_cache(maxsize=20000)
def to_bloomberg_symbol(symbol: str) -> str:
    s = normalize_symbol(symbol)
    if not s:
        return ""
    if is_special_symbol(s):
        return s
    if is_ksa(s):
        # Bloomberg KSA commonly uses "AB"
        return f"{s.replace('.SR', '')} AB"

    base, ex = split_symbol_exchange(s)
    if ex:
        bloomberg_ex = {
            "US": "US",
            "UK": "LN",
            "JP": "JP",
            "HK": "HK",
        }.get(ex.upper(), ex.upper())
        return f"{base} {bloomberg_ex}"

    return f"{s} US"


@lru_cache(maxsize=20000)
def to_reuters_symbol(symbol: str) -> str:
    s = normalize_symbol(symbol)
    if not s:
        return ""
    base, ex = split_symbol_exchange(s)
    if ex:
        if ex.upper() == "NASDAQ":
            return f"{base}.OQ"
        if ex.upper() == "NYSE":
            return f"{base}.N"
    return s


@lru_cache(maxsize=20000)
def to_google_symbol(symbol: str) -> str:
    s = normalize_symbol(symbol)
    if not s:
        return ""
    if is_ksa(s):
        return f"TADAWUL:{s.replace('.SR', '')}"

    base, ex = split_symbol_exchange(s)
    if ex:
        exchange_map = {"US": "NYSE", "NASDAQ": "NASDAQ", "UK": "LON", "JP": "TYO", "HK": "HKG"}.get(ex.upper(), ex.upper())
        return f"{exchange_map}:{base}"
    return s


@lru_cache(maxsize=20000)
def to_tradingview_symbol(symbol: str) -> str:
    s = normalize_symbol(symbol)
    if not s:
        return ""
    if is_ksa(s):
        return f"TADAWUL:{s.replace('.SR', '')}"
    if is_fx(s):
        return f"FX:{extract_base_symbol(s).replace('/', '').replace('-', '').replace('=X', '')}"
    if is_crypto(s):
        return f"BINANCE:{s.replace('-', '')}"

    base, ex = split_symbol_exchange(s)
    if ex:
        exchange_map = {
            "UK": "LSE", "FR": "EURONEXT", "DE": "XETR",
            "JP": "TSE", "HK": "HKEX", "CN": "SSE",
            "AU": "ASX", "CA": "TSX", "US": "NASDAQ",
        }.get(ex.upper(), ex.upper())
        return f"{exchange_map}:{base}"

    return f"NASDAQ:{s}"


# =============================================================================
# Symbol Variants Functions
# =============================================================================

def symbol_variants(symbol: str) -> List[str]:
    s = normalize_symbol(symbol)
    if not s:
        return []
    variants: List[str] = [s]

    if is_ksa(s):
        code = s.replace(".SR", "")
        variants.extend([f"{code}.SR", code, f"TADAWUL:{code}"])
        return _unique_preserve_order(variants)

    # Share class variations
    if "." in s:
        # if this is a share class (e.g., BRK.B) add dash version (BRK-B)
        base, ex = split_symbol_exchange(s)
        if ex is None and CLASS_DOT_RE.match(s):
            root, cls = s.rsplit(".", 1)
            variants.append(f"{root}-{cls}")
        if ex is not None and CLASS_DOT_RE.match(base):
            root, cls = base.rsplit(".", 1)
            variants.extend([f"{root}-{cls}.{ex}"])

    if "-" in s:
        m = CLASS_DASH_RE.match(s)
        if m:
            root, cls = m.groups()
            variants.append(f"{root}.{cls}")

    # If no exchange suffix and not special, add common guesses
    if extract_exchange_code(s) is None and not is_special_symbol(s):
        variants.extend([f"{s}.US", f"{s}.L"])

    return _unique_preserve_order(variants)


def yahoo_symbol_variants(symbol: str) -> List[str]:
    s = normalize_symbol(symbol)
    if not s:
        return []

    variants: List[str] = []
    y = to_yahoo_symbol(s)
    if y:
        variants.append(y)

    if is_ksa(s):
        code = s.replace(".SR", "")
        variants.extend([f"{code}.SR", code])

    # Share class conversions
    base, ex = split_symbol_exchange(s)
    if ex == "US":
        variants.append(base)  # AAPL.US -> AAPL for yahoo
    if CLASS_DOT_RE.match(base) and ex:
        # BRK.B.US -> yahoo expects BRK.B
        variants.append(base)

    if CLASS_DOT_RE.match(s):
        root, cls = s.rsplit(".", 1)
        variants.append(f"{root}-{cls}")

    if is_index(s) and not s.startswith("^"):
        variants.append(f"^{s}")
    if is_fx(s):
        b = extract_base_symbol(s).replace("=X", "")
        variants.extend([f"{b}=X", f"{b[:3]}/{b[3:]}"])
    if is_commodity_future(s):
        variants.append(f"{extract_base_symbol(s)}=F")
    if is_crypto(s) and "-" not in s:
        variants.append(f"{s}-USD")

    return _unique_preserve_order(variants)


def finnhub_symbol_variants(symbol: str) -> List[str]:
    s = normalize_symbol(symbol)
    if not s:
        return []

    variants: List[str] = []
    f = to_finnhub_symbol(s)
    if f:
        variants.append(f)

    base, ex = split_symbol_exchange(s)
    if ex and ex != "US":
        variants.append(base)

    if extract_exchange_code(s) is None and not is_ksa(s) and not is_special_symbol(s):
        variants.append(f"{s}.US")

    return _unique_preserve_order(variants)


def eodhd_symbol_variants(symbol: str, *, default_exchange: Optional[str] = None) -> List[str]:
    """
    EODHD variants are crucial when input is plain ticker:
      AAPL -> AAPL.US (default)
      BRK.B -> BRK.B.US and BRK-B.US
    """
    s = normalize_symbol(symbol)
    if not s:
        return []

    variants: List[str] = []
    ex = (default_exchange or _default_exchange()).upper()

    e = to_eodhd_symbol(s, default_exchange=ex)
    if e:
        variants.append(e)

    if is_ksa(s):
        code = s.replace(".SR", "")
        variants.extend([f"{code}.SR", code])
        return _unique_preserve_order(variants)

    # Include plain + base forms (useful if provider accepts)
    base = extract_base_symbol(s)
    if base and base != s:
        variants.append(base)

    # Ensure default exchange suffix exists for equity-like tickers
    if extract_exchange_code(s) is None and not is_special_symbol(s):
        variants.extend([f"{s}.{ex}"])

    # Share class: BRK.B -> BRK-B.US and BRK.B.US
    base2, ex2 = split_symbol_exchange(s)
    if ex2 is None and CLASS_DOT_RE.match(s):
        root, cls = s.rsplit(".", 1)
        variants.extend([f"{root}-{cls}.{ex}", f"{root}.{cls}.{ex}"])
    elif ex2 is not None and CLASS_DOT_RE.match(base2):
        root, cls = base2.rsplit(".", 1)
        variants.extend([f"{root}-{cls}.{ex2}", f"{root}.{cls}.{ex2}"])

    # If dash share class was input, add dot version + suffix
    if CLASS_DASH_RE.match(s):
        root, cls = s.split("-", 1)
        variants.extend([f"{root}.{cls}.{ex}", f"{root}.{cls}"])

    return _unique_preserve_order(variants)


def bloomberg_symbol_variants(symbol: str) -> List[str]:
    s = normalize_symbol(symbol)
    if not s:
        return []
    variants: List[str] = []
    b = to_bloomberg_symbol(s)
    if b:
        variants.append(b)

    if " " in b:
        variants.append(b.replace(" ", ""))
        parts = b.split(" ")
        if len(parts) >= 2:
            variants.append(f"{parts[0]}.{parts[1]}")

    return _unique_preserve_order(variants)


def reuters_symbol_variants(symbol: str) -> List[str]:
    s = normalize_symbol(symbol)
    if not s:
        return []
    variants: List[str] = []
    r = to_reuters_symbol(s)
    if r:
        variants.append(r)
    if "." in r:
        variants.append(r.replace(".OQ", ".O").replace(".N", ""))
    return _unique_preserve_order(variants)


# =============================================================================
# Provider-aware normalization (optional usage by engine)
# =============================================================================

@lru_cache(maxsize=40000)
def normalize_symbol_for_provider(symbol: str, provider: str, *, default_exchange: Optional[str] = None) -> str:
    """
    Returns a provider-ready symbol without the caller needing to remember rules.

    provider examples: "yahoo", "finnhub", "eodhd", "google", "tradingview"
    """
    p = (provider or "").strip().lower()
    if p in {"yahoo", "yfinance"}:
        return to_yahoo_symbol(symbol)
    if p == "finnhub":
        return to_finnhub_symbol(symbol)
    if p == "eodhd":
        return to_eodhd_symbol(symbol, default_exchange=default_exchange)
    if p in {"google", "goog"}:
        return to_google_symbol(symbol)
    if p in {"tv", "tradingview"}:
        return to_tradingview_symbol(symbol)
    # fallback: neutral
    return normalize_symbol(symbol)
