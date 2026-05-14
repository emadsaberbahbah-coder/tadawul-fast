#!/usr/bin/env python3
# core/symbols/normalize.py
"""
================================================================================
Symbol Normalization -- v7.2.0 (PROVIDER-SYMBOL-CORRECT / COUNTRY-AWARE)
================================================================================
Comprehensive Symbol Normalization for KSA + Global Markets, with provider-safe
formatting helpers and robust handling of share-class tickers (e.g., BRK.B).

v7.2.0 Changes (from v7.1.0)
----------------------------
CRITICAL (audit fix -- provider symbol formatting; the root cause of the
non-US data corruption seen in Market_Leaders / Global_Markets):
  - `to_yahoo_symbol` now translates the (possibly aliased) exchange suffix
    to the EXACT suffix Yahoo Finance expects, via the new
    `_YAHOO_SUFFIX_MAP` table. Previously any non-US suffix was passed
    through unchanged, so internal / EODHD-style suffixes that Yahoo does
    NOT recognise (`.LSE`, `.LN`, `.NSE`, `.JSE`, `.ZA`, `.HKG`, `.TYO`,
    `.KOSDAQ`, `.NYSE`, `.OQ`, `.N`, ...) were sent to Yahoo verbatim.
    Yahoo rejected them, enrichment fell through, and the data engine
    stamped those rows with US identity defaults -- `III.LSE`, `ANTO.LSE`,
    `RR.LSE`, `HDFCLIFE.NSE`, `FSR.JSE` were all mis-tagged USA / USD,
    which in turn inflated market caps ~100x (GBp priced as USD).
    Now: `.LSE`/`.L`/`.LN` -> `.L`, `.NSE`/`.NS` -> `.NS`,
    `.JSE`/`.ZA` -> `.JO`, `.TYO`/`.T` -> `.T`, `.HKG`/`.HK` -> `.HK`,
    `.NYSE`/`.OQ`/`.N`/`.NM`/`.NG` -> `` (US, no suffix), etc.
  - `to_yahoo_symbol` now converts share-class DOT tickers back to Yahoo's
    DASH form: canonical `BRK.B` / `BRK.B.US` -> `BRK-B`. `normalize_symbol`
    canonicalizes to the dot form, but Yahoo only accepts the dash form;
    previously only the variant generator knew this, the primary formatter
    returned the (rejected) dot form.

HIGH (audit fix -- provider symbol formatting):
  - `to_eodhd_symbol` now translates aliased suffixes to EODHD exchange
    codes via `_EODHD_SUFFIX_MAP`, instead of echoing whatever suffix the
    caller happened to pass. Yahoo-style short suffixes that EODHD rejects
    are now mapped (e.g. `.L` -> `.LSE`, `.NYSE`/`.N`/`.OQ` -> `.US`,
    `.TYO` -> `.TSE`). Inputs that already used EODHD-style suffixes are
    unaffected. Unknown / uncertain suffixes fall back to the raw suffix,
    so an unverified market degrades to pre-7.2.0 behaviour rather than
    breaking.
  - `to_finnhub_symbol` now strips ALL US exchange aliases (`.NYSE`, `.N`,
    `.OQ`, `.NM`, `.NG`), not just the literal `.US`.

NEW:
  - `.XETRA` / `.ETR` recognised as Deutsche Borse XETRA (-> DE). Live
    symbol universes use `.XETRA`; previously it matched no exchange
    suffix and fell through every country / currency / MIC / provider
    resolution path.
  - `_YAHOO_SUFFIX_MAP` -- raw-suffix -> Yahoo-suffix translation table,
    total over `_EXCHANGE_SUFFIXES`.
  - `_EODHD_SUFFIX_MAP` -- raw-suffix -> EODHD-exchange-code table
    (best-effort; see operator note).
  - `_share_class_dot_to_dash` -- internal helper, dot share class -> dash.

Preserved:
  - All v7.1.0 country/currency work (`get_country_from_symbol`,
    `_COUNTRY_BY_EXCHANGE`, `_CURRENCY_BY_EXCHANGE`, `_INDEX_COUNTRY`,
    share-class `get_primary_exchange` heuristic).
  - All v7.0.0 fixes (ETH-USD crypto vs FX, EUR-USD canonicalization,
    SPX.INDX -> ^GSPC, NASDAQ key removed from _INDEX_COMMON, dead code
    cleanup, _MARKET_BY_CODE precompute).
  - Complete `__all__` surface (no public API removed; none added).
  - All regex patterns (public and internal), all mapping tables, all
    `lru_cache` sizes, `SymbolNormalizationConfig` shape, all env var
    names, every provider formatter and variant generator.
  - KSA canonicalization (`####.SR`), TADAWUL: prefix handling, .SA/.SAU
    stripping, share-class conversion (BRK-B <-> BRK.B).
  - Exchange suffix table, MIC codes, default_exchange env handling.

Operator note (integration):
  - data_engine_v2 must (a) replace `_infer_country_from_symbol` with
    `get_country_from_symbol` (per the v7.1.0 note) AND (b) route ALL
    provider fetches through `to_yahoo_symbol` / `to_eodhd_symbol` /
    `normalize_symbol_for_provider`. The suffix-translation fix only
    takes effect if the formatters are actually on the request path;
    if the data engine hand-builds provider symbols anywhere, that path
    must be removed.
  - EODHD exchange codes for a few markets (Japan=TSE, China=SHG/SHE,
    Korea=KO/KQ, Singapore=SG, Switzerland=SW) follow EODHD's documented
    conventions but should be spot-checked against your EODHD
    `/exchanges-list` if you trade those venues. Anything not explicitly
    in `_EODHD_SUFFIX_MAP` falls back to the raw suffix.
================================================================================
"""

from __future__ import annotations

import os
import re
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from functools import lru_cache
from typing import (
    Any,
    Dict,
    List,
    Optional,
    Set,
    Tuple,
    Union,
)

# ---------------------------------------------------------------------------
# High-Performance JSON Support
# ---------------------------------------------------------------------------

try:
    import orjson  # type: ignore

    def _json_loads(data: Union[str, bytes]) -> Any:
        if isinstance(data, str):
            data = data.encode("utf-8")
        return orjson.loads(data)

    _HAS_ORJSON = True
except ImportError:
    import json  # type: ignore

    def _json_loads(data: Union[str, bytes]) -> Any:
        if isinstance(data, bytes):
            data = data.decode("utf-8", errors="replace")
        return json.loads(data)

    _HAS_ORJSON = False

# ---------------------------------------------------------------------------
# Version and Exports
# ---------------------------------------------------------------------------

__version__ = "7.2.0"

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
    "get_country_from_symbol",   # v7.1.0
    "get_mic_code",
    # Provider-aware normalization
    "normalize_symbol_for_provider",
    "__version__",
]

# =============================================================================
# Enums
# =============================================================================


class MarketType(str, Enum):
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


class AssetClass(str, Enum):
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


class SymbolQuality(str, Enum):
    """Symbol validation quality."""
    EXCELLENT = "excellent"
    GOOD = "good"
    FAIR = "fair"
    POOR = "poor"
    INVALID = "invalid"


# =============================================================================
# Configuration
# =============================================================================

@dataclass(frozen=True)
class SymbolNormalizationConfig:
    """Configuration for symbol normalization."""
    default_exchange: str = "US"
    default_equity_exchange_suffix: str = ""
    strip_prefixes: Tuple[str, ...] = ()
    strip_suffixes: Tuple[str, ...] = ()
    exchange_map: Dict[str, str] = field(default_factory=dict)
    index_map: Dict[str, str] = field(default_factory=dict)
    fx_map: Dict[str, str] = field(default_factory=dict)
    commodity_map: Dict[str, str] = field(default_factory=dict)
    crypto_map: Dict[str, str] = field(default_factory=dict)

    @classmethod
    def from_env(cls) -> "SymbolNormalizationConfig":
        """Load configuration from environment variables."""
        def _env_str(name: str, default: str = "") -> str:
            v = os.getenv(name)
            return default if v is None else str(v).strip()

        def _env_list(name: str) -> Tuple[str, ...]:
            raw = _env_str(name, "")
            if not raw:
                return tuple()
            return tuple(p.strip() for p in raw.split(",") if p.strip())

        def _env_dict(name: str) -> Dict[str, str]:
            raw = _env_str(name, "")
            if not raw:
                return {}
            try:
                data = _json_loads(raw)
                if isinstance(data, dict):
                    return {str(k).upper(): str(v) for k, v in data.items()}
            except Exception:
                pass
            return {}

        return cls(
            default_exchange=_env_str("EODHD_DEFAULT_EXCHANGE", "US").upper(),
            default_equity_exchange_suffix=_env_str(
                "NORMALIZE_DEFAULT_EQUITY_EXCHANGE_SUFFIX", "",
            ).upper(),
            strip_prefixes=_env_list("NORMALIZE_STRIP_PREFIXES"),
            strip_suffixes=_env_list("NORMALIZE_STRIP_SUFFIXES"),
            exchange_map=_env_dict("SYMBOL_EXCHANGE_MAP_JSON"),
            index_map=_env_dict("SYMBOL_INDEX_MAP_JSON"),
            fx_map=_env_dict("SYMBOL_FX_MAP_JSON"),
            commodity_map=_env_dict("SYMBOL_COMMODITY_MAP_JSON"),
            crypto_map=_env_dict("SYMBOL_CRYPTO_MAP_JSON"),
        )


_CONFIG = SymbolNormalizationConfig.from_env()

# =============================================================================
# Unicode and Digit Normalization
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
# Pattern Definitions (Compiled)
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

# Forex patterns
FX_EQUAL_RE = re.compile(r"^([A-Z]{3,6})=X$", re.IGNORECASE)
FX_SLASH_RE = re.compile(r"^([A-Z]{3})/([A-Z]{3})$", re.IGNORECASE)
FX_DASH_RE = re.compile(r"^([A-Z]{3})-([A-Z]{3})$", re.IGNORECASE)
FX_SUFFIX_RE = re.compile(r"^([A-Z]{3,6})\.FOREX$", re.IGNORECASE)

# Commodity futures patterns
FUTURE_EQUAL_RE = re.compile(r"^([A-Z0-9]{1,6})=F$", re.IGNORECASE)
FUTURE_SUFFIX_RE = re.compile(r"^([A-Z0-9]{1,6})\.(COMM|COM|FUT)$", re.IGNORECASE)

# Crypto patterns
CRYPTO_DASH_RE = re.compile(r"^([A-Z0-9]{2,15})-([A-Z]{2,10})$", re.IGNORECASE)
CRYPTO_SUFFIX_RE = re.compile(r"^([A-Z0-9]{2,15})\.(CRYPTO|CC|C)$", re.IGNORECASE)

# ETF patterns
ETF_SUFFIX_RE = re.compile(r"^([A-Z0-9]+)\.(ETF|ET)$", re.IGNORECASE)

# Share class patterns
CLASS_DASH_RE = re.compile(r"^([A-Z]+)-([A-Z])$", re.IGNORECASE)
CLASS_DOT_RE = re.compile(r"^([A-Z]+)\.([A-Z])$", re.IGNORECASE)

# Allowed characters
ALLOWED_CHARS_RE = re.compile(r"^[A-Z0-9\.\-\^=\/\@\#]+$", re.IGNORECASE)


# =============================================================================
# Known Mappings
# =============================================================================

# Exchange suffixes mapping (keys must be UPPER and include leading dot)
_EXCHANGE_SUFFIXES: Dict[str, str] = {
    # Americas
    ".US": "US", ".NYSE": "US", ".N": "US", ".NASDAQ": "US", ".OQ": "US", ".NM": "US", ".NG": "US",
    ".TO": "CA", ".V": "CA", ".CNQ": "CA",
    ".MX": "MX", ".SA": "BR", ".BA": "AR",
    # EMEA
    ".L": "UK", ".LSE": "UK", ".LN": "UK",
    ".PA": "FR", ".FP": "FR",
    ".DE": "DE", ".F": "DE", ".BE": "DE", ".DU": "DE", ".HM": "DE",
    ".XETRA": "DE", ".ETR": "DE",  # v7.2.0: Deutsche Borse XETRA
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
_MIC_MAPPINGS: Dict[str, str] = {
    "US": "XNYS", "NASDAQ": "XNAS", "CA": "XTSE", "MX": "XMEX", "BR": "BVMF",
    "UK": "XLON", "FR": "XPAR", "DE": "XETR", "CH": "XSWX", "SA": "XSAU",
    "AE": "XDFM", "JP": "XTKS", "HK": "XHKG", "CN": "XSHG", "IN": "XNSE",
    "AU": "XASX", "KR": "XKRX", "SG": "XSES",
}

# v7.2.0: Yahoo Finance suffix translation.
# -----------------------------------------------------------------------------
# Keys are the RAW suffix as returned by split_symbol_exchange (UPPER, no dot).
# Values are the suffix Yahoo Finance actually expects, INCLUDING the leading
# dot -- or "" (empty string) for US listings, which carry no Yahoo suffix.
#
# Why this exists: split_symbol_exchange returns whatever recognised alias the
# caller passed (".LSE", ".LN" and ".L" are all valid UK aliases internally),
# but Yahoo accepts exactly ONE suffix per venue. v7.1.0 and earlier passed any
# non-US suffix through unchanged, so ".LSE" / ".NSE" / ".JSE" / ".TYO" / ".OQ"
# went to Yahoo verbatim, were rejected, and the data engine fell back to US
# identity defaults. This table is TOTAL over _EXCHANGE_SUFFIXES so to_yahoo_
# symbol never has a gap; any caller-added env suffix not present here falls
# back to pass-through (pre-7.2.0 behaviour).
_YAHOO_SUFFIX_MAP: Dict[str, str] = {
    # --- Americas ---
    "US": "", "NYSE": "", "N": "", "NASDAQ": "", "OQ": "", "NM": "", "NG": "",
    "TO": ".TO", "V": ".V", "CNQ": ".CN",
    "MX": ".MX", "SA": ".SA", "BA": ".BA",
    # --- EMEA ---
    "L": ".L", "LSE": ".L", "LN": ".L",
    "PA": ".PA", "FP": ".PA",
    "DE": ".DE", "F": ".F", "BE": ".BE", "DU": ".DU", "HM": ".HM",
    "XETRA": ".DE", "ETR": ".DE",
    "SW": ".SW", "VX": ".SW",
    "AS": ".AS", "BR": ".BR",
    "MC": ".MC",
    "MI": ".MI", "IM": ".MI",
    "CO": ".CO", "ST": ".ST", "OL": ".OL", "HE": ".HE",
    "WA": ".WA", "PR": ".PR", "BU": ".BD",
    "AT": ".VI", "VI": ".VI",
    "IR": ".IR", "DUB": ".IR",
    "ZA": ".JO", "JSE": ".JO",
    "TA": ".TA", "TASE": ".TA",
    "SAU": ".SR", "SR": ".SR", "TADAWUL": ".SR",
    # Yahoo Gulf coverage is limited; best-effort below.
    "AE": ".AE", "DFM": ".AE", "ADX": ".AE",
    "QA": ".QA", "QE": ".QA",
    "KW": ".KW", "KSE": ".KW",
    "EG": ".CA", "EGX": ".CA",  # Yahoo uses .CA for Cairo (EGX)
    # --- Asia Pacific ---
    "T": ".T", "TYO": ".T",
    "HK": ".HK", "HKG": ".HK",
    "SS": ".SS", "SHG": ".SS",
    "SZ": ".SZ", "SHE": ".SZ",
    "NS": ".NS", "NSE": ".NS",
    "BO": ".BO", "BSE": ".BO",
    "KS": ".KS", "KQ": ".KQ", "KOSDAQ": ".KQ",
    "TW": ".TW", "TWO": ".TWO",
    "SI": ".SI", "SGX": ".SI",
    "KL": ".KL", "KLSE": ".KL",
    "JK": ".JK", "IDX": ".JK",
    "SET": ".BK", "BK": ".BK",
    "VN": ".VN", "HOSE": ".VN",
    "PS": ".PS", "PSE": ".PS",
    "AU": ".AX", "AX": ".AX", "ASX": ".AX",
    "NZ": ".NZ", "NZSE": ".NZ",
}

# v7.2.0: EODHD exchange-code translation.
# -----------------------------------------------------------------------------
# Keys are the RAW suffix as returned by split_symbol_exchange (UPPER, no dot).
# Values are the EODHD exchange code (no dot). EODHD expects TICKER.EXCHANGE.
#
# Only entries that DIFFER from the raw suffix, or that normalise a family of
# aliases onto a single EODHD code, need to be listed -- to_eodhd_symbol falls
# back to the raw suffix (current behaviour) for anything not present here, so
# an unverified market degrades gracefully rather than breaking.
#
# OPERATOR NOTE: the high-confidence entries are the Americas / Western Europe /
# UK / India / Australia / HK / South Africa rows. Japan (TSE), China (SHG/SHE),
# Korea (KO/KQ), Singapore (SG) and Switzerland (SW) follow EODHD's documented
# conventions but should be spot-checked against your EODHD /exchanges-list.
_EODHD_SUFFIX_MAP: Dict[str, str] = {
    # --- Americas (US aliases all collapse to US) ---
    "US": "US", "NYSE": "US", "N": "US", "NASDAQ": "US", "OQ": "US",
    "NM": "US", "NG": "US",
    "TO": "TO", "V": "V",
    "MX": "MX", "SA": "SA", "BA": "BA",
    # --- EMEA ---
    "L": "LSE", "LSE": "LSE", "LN": "LSE",
    "PA": "PA", "FP": "PA",
    "DE": "XETRA", "XETRA": "XETRA", "ETR": "XETRA", "F": "F",
    "SW": "SW", "VX": "SW",
    "AS": "AS", "BR": "BR",
    "MC": "MC",
    "MI": "MI", "IM": "MI",
    "CO": "CO", "ST": "ST", "OL": "OL", "HE": "HE",
    "WA": "WAR", "PR": "PR",
    "AT": "VI", "VI": "VI",
    "IR": "IR", "DUB": "IR",
    "ZA": "JSE", "JSE": "JSE",
    "TA": "TA", "TASE": "TA",
    "SAU": "SR", "SR": "SR", "TADAWUL": "SR",
    # --- Asia Pacific ---
    "T": "TSE", "TYO": "TSE",
    "HK": "HK", "HKG": "HK",
    "SS": "SHG", "SHG": "SHG",
    "SZ": "SHE", "SHE": "SHE",
    "NS": "NSE", "NSE": "NSE",
    "BO": "BSE", "BSE": "BSE",
    "KS": "KO", "KQ": "KQ", "KOSDAQ": "KQ",
    "TW": "TW", "TWO": "TW",
    "SI": "SG", "SGX": "SG",
    "KL": "KLSE", "KLSE": "KLSE",
    "JK": "JK", "IDX": "JK",
    "AU": "AU", "AX": "AU", "ASX": "AU",
    "NZ": "NZ", "NZSE": "NZ",
}

# v7.1.0: country lookup (display-friendly English names) keyed by the
# exchange code returned by get_primary_exchange. This is the table the
# new get_country_from_symbol consults. Bug being fixed: data engine
# was returning "USA" for every non-.SR / non-special symbol because
# its homegrown _infer_country_from_symbol had no exchange-suffix
# lookup -- BARC.L, SAP.DE, 7203.T were all "USA".
_COUNTRY_BY_EXCHANGE: Dict[str, str] = {
    # Americas
    "US": "United States",
    "CA": "Canada",
    "MX": "Mexico",
    "BR": "Brazil",
    "AR": "Argentina",
    # EMEA
    "UK": "United Kingdom",
    "FR": "France",
    "DE": "Germany",
    "CH": "Switzerland",
    "NL": "Netherlands",
    "BE": "Belgium",
    "ES": "Spain",
    "IT": "Italy",
    "DK": "Denmark",
    "SE": "Sweden",
    "NO": "Norway",
    "FI": "Finland",
    "PL": "Poland",
    "CZ": "Czechia",
    "HU": "Hungary",
    "AT": "Austria",
    "IE": "Ireland",
    "PT": "Portugal",
    "ZA": "South Africa",
    "IL": "Israel",
    "SA": "Saudi Arabia",
    "AE": "United Arab Emirates",
    "QA": "Qatar",
    "KW": "Kuwait",
    "EG": "Egypt",
    # Asia Pacific
    "JP": "Japan",
    "HK": "Hong Kong",
    "CN": "China",
    "IN": "India",
    "KR": "South Korea",
    "TW": "Taiwan",
    "SG": "Singapore",
    "MY": "Malaysia",
    "ID": "Indonesia",
    "TH": "Thailand",
    "VN": "Vietnam",
    "PH": "Philippines",
    "AU": "Australia",
    "NZ": "New Zealand",
    # Aliases that may surface from get_primary_exchange's KSA shortcut
    "SAUDI": "Saudi Arabia",
}

# v7.1.0: currency lookup keyed by the same exchange code. Replaces
# the inline if/elif chain in get_currency_from_symbol, which only
# covered US / UK / JP / EUR-zone and returned None for HK, CN, IN,
# AU, CA, CH, KR, SG, SA(non-KSA-shortcut), AE, QA, KW, EG, ZA, etc.
_CURRENCY_BY_EXCHANGE: Dict[str, str] = {
    # Americas
    "US": "USD",
    "CA": "CAD",
    "MX": "MXN",
    "BR": "BRL",
    "AR": "ARS",
    # EMEA - GBP
    "UK": "GBP",
    # EMEA - Euro zone
    "FR": "EUR",
    "DE": "EUR",
    "NL": "EUR",
    "BE": "EUR",
    "ES": "EUR",
    "IT": "EUR",
    "AT": "EUR",
    "IE": "EUR",
    "PT": "EUR",
    "FI": "EUR",
    # EMEA - other
    "CH": "CHF",
    "DK": "DKK",
    "SE": "SEK",
    "NO": "NOK",
    "PL": "PLN",
    "CZ": "CZK",
    "HU": "HUF",
    "ZA": "ZAR",
    "IL": "ILS",
    "SA": "SAR",
    "SAUDI": "SAR",
    "AE": "AED",
    "QA": "QAR",
    "KW": "KWD",
    "EG": "EGP",
    # Asia Pacific
    "JP": "JPY",
    "HK": "HKD",
    "CN": "CNY",
    "IN": "INR",
    "KR": "KRW",
    "TW": "TWD",
    "SG": "SGD",
    "MY": "MYR",
    "ID": "IDR",
    "TH": "THB",
    "VN": "VND",
    "PH": "PHP",
    "AU": "AUD",
    "NZ": "NZD",
}

# v7.1.0: per-index country override for major caret-prefixed indexes.
# Without this, ^GSPC / ^FTSE / ^N225 all resolved to "Global" because
# is_special_symbol short-circuits before exchange-suffix logic.
_INDEX_COUNTRY: Dict[str, str] = {
    "^GSPC": "United States",
    "^DJI": "United States",
    "^IXIC": "United States",
    "^NDX": "United States",
    "^RUT": "United States",
    "^VIX": "United States",
    "^FTSE": "United Kingdom",
    "^GDAXI": "Germany",
    "^FCHI": "France",
    "^N225": "Japan",
    "^HSI": "Hong Kong",
    "^SSEC": "China",
    "^TASI": "Saudi Arabia",
    "^NOMU": "Saudi Arabia",
    "^STOXX50E": "European Union",
    "^STOXX": "European Union",
    "^AXJO": "Australia",
    "^NSEI": "India",
    "^BSESN": "India",
    "^KS11": "South Korea",
    "^TWII": "Taiwan",
    "^STI": "Singapore",
    "^JKSE": "Indonesia",
    "^KLSE": "Malaysia",
    "^SET": "Thailand",
    "^MERV": "Argentina",
    "^BVSP": "Brazil",
    "^MXX": "Mexico",
    "^GSPTSE": "Canada",
    "^TA125": "Israel",
}

# Index mappings
# v7.0.0 fix: the bare 'NASDAQ' key was a footgun -- it made is_index('NASDAQ')
# return True, so detect_asset_class('NASDAQ') returned INDEX. But NASDAQ is
# primarily an exchange keyword (already in _EXCHANGE_SUFFIXES). Callers who
# want the NASDAQ Composite index should use 'IXIC' or '^IXIC'.
_INDEX_COMMON: Dict[str, str] = {
    "SPX": "^GSPC", "SP500": "^GSPC", "DJI": "^DJI", "DOW": "^DJI",
    "NDX": "^NDX", "IXIC": "^IXIC", "RUT": "^RUT", "FTSE": "^FTSE",
    "DAX": "^GDAXI", "CAC": "^FCHI", "NIKKEI": "^N225", "N225": "^N225",
    "HSI": "^HSI", "SSEC": "^SSEC", "TASI": "^TASI", "NOMU": "^NOMU", "VIX": "^VIX",
}

# Forex common pairs
_FX_COMMON_PAIRS: Set[str] = {
    "EURUSD", "GBPUSD", "USDJPY", "USDCHF", "AUDUSD", "USDCAD", "NZDUSD",
    "EURGBP", "EURJPY", "GBPJPY", "CHFJPY", "EURCHF", "GBPCHF", "AUDJPY",
}

# Commodity codes
_COMMODITY_CODES: Dict[str, str] = {
    "GC": "gold", "SI": "silver", "PL": "platinum", "PA": "palladium",
    "CL": "wti_crude", "BZ": "brent_crude", "NG": "natural_gas", "HO": "heating_oil",
    "RB": "gasoline", "ZC": "corn", "ZW": "wheat", "ZS": "soybeans",
    "ZM": "soybean_meal", "ZL": "soybean_oil", "ZR": "rough_rice", "ZO": "oats",
    "KE": "kc_wheat", "MW": "spring_wheat", "CC": "cocoa", "KC": "coffee",
    "CT": "cotton", "OJ": "orange_juice", "SB": "sugar", "LB": "lumber",
    "HG": "copper", "ALI": "aluminium", "NICKEL": "nickel", "ZINC": "zinc",
    "LEAD": "lead", "TIN": "tin",
}

# Crypto common
_CRYPTO_COMMON: Dict[str, str] = {
    "BTC": "bitcoin", "ETH": "ethereum", "XRP": "ripple", "LTC": "litecoin",
    "BCH": "bitcoin_cash", "ADA": "cardano", "DOT": "polkadot", "LINK": "chainlink",
    "BNB": "binance", "XLM": "stellar", "DOGE": "dogecoin", "UNI": "uniswap",
    "SOL": "solana", "MATIC": "polygon", "AVAX": "avalanche", "ATOM": "cosmos",
    "ALGO": "algorand", "VET": "vechain", "FIL": "filecoin", "TRX": "tron",
    "USDT": "tether", "USDC": "usd_coin", "SHIB": "shiba_inu", "LUNA": "terra",
    # v7.0.0: a few more for detection robustness
    "NEAR": "near", "APT": "aptos", "ARB": "arbitrum", "OP": "optimism",
    "INJ": "injective", "SUI": "sui", "TON": "ton", "PEPE": "pepe",
}

# ETF common prefixes
_ETF_COMMON_PREFIX: Set[str] = {
    "SPY", "QQQ", "IVV", "VTI", "VOO", "BND", "EFA", "IWM", "AGG", "GLD", "SLV",
}

# Common prefixes to strip
_COMMON_PREFIXES: Tuple[str, ...] = (
    "TADAWUL:", "STOCK:", "TICKER:", "INDEX:", "NYSE:", "NASDAQ:", "OTC:",
    "LSE:", "TSX:", "ASX:", "HKEX:", "SGX:", "B3:", "JSE:", "DFM:", "ADX:",
    "FOREX:", "FX:", "CRYPTO:", "CC:", "FUT:", "COMM:", "INDX:", "ETF:",
)

# Common suffixes to strip
_COMMON_SUFFIXES: Tuple[str, ...] = (
    ".TADAWUL", ".STOCK", ".TICKER", ".INDEX",
)


# =============================================================================
# Apply Custom Mappings from Config
# =============================================================================

_EXCHANGE_SUFFIXES.update(_CONFIG.exchange_map)
_INDEX_COMMON.update(_CONFIG.index_map)
_FX_COMMON_PAIRS.update({
    k.upper().replace("/", "").replace("-", "")
    for k in _CONFIG.fx_map.keys()
})
_COMMODITY_CODES.update({k.upper(): v for k, v in _CONFIG.commodity_map.items()})
_CRYPTO_COMMON.update({k.upper(): v for k, v in _CONFIG.crypto_map.items()})

_STRIP_PREFIXES = _COMMON_PREFIXES + _CONFIG.strip_prefixes
_STRIP_SUFFIXES = _COMMON_SUFFIXES + _CONFIG.strip_suffixes


# v7.0.0: precomputed reverse index for detect_market_type to avoid per-call
# linear scan over MarketType with case-insensitive string compares.
_MARKET_BY_CODE: Dict[str, MarketType] = {m.value.upper(): m for m in MarketType}


# =============================================================================
# Pure Utility Functions
# =============================================================================

def _unique_preserve_order(items: List[str]) -> List[str]:
    """Deduplicate items while preserving order."""
    seen: Set[str] = set()
    result: List[str] = []
    for item in items:
        if not item or item in seen:
            continue
        seen.add(item)
        result.append(item)
    return result


def _share_class_dot_to_dash(sym: str) -> str:
    """
    Convert a share-class DOT ticker to the DASH form (BRK.B -> BRK-B).

    v7.2.0 helper. `normalize_symbol` canonicalizes share classes to the
    dot form, but Yahoo Finance expects the dash form. Only fires when the
    whole string is a `<ALPHA>.<SINGLE-LETTER>` share-class pattern; plain
    tickers ("AAPL"), dash tickers ("EPI-A") and anything else are returned
    unchanged.
    """
    if not sym:
        return sym
    m = CLASS_DOT_RE.match(sym)
    if m:
        return f"{m.group(1)}-{m.group(2)}"
    return sym


# =============================================================================
# Core Unicode Cleaning Functions
# =============================================================================

@lru_cache(maxsize=20000)
def clean_unicode(text: str) -> str:
    """
    Clean Unicode text, normalize digits, remove hidden characters.

    Examples:
        clean_unicode("AAPL") -> "AAPL"
        clean_unicode("٢٢٢٢") -> "2222"
    """
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
def strip_noise_prefix_suffix(text: str) -> str:
    """Strip common noise prefixes and suffixes (one pass each)."""
    if not text:
        return ""
    s = text.upper().strip()
    for prefix in _STRIP_PREFIXES:
        if s.startswith(prefix.upper()):
            s = s[len(prefix):].strip()
            break
    for suffix in _STRIP_SUFFIXES:
        if s.endswith(suffix.upper()):
            s = s[:-len(suffix)].strip()
            break
    return s


# =============================================================================
# Detection Functions
# =============================================================================

@lru_cache(maxsize=20000)
def looks_like_ksa(symbol: str) -> bool:
    """
    Check if symbol looks like a KSA (Saudi) symbol.

    v7.1.0: also detects `.SAU` and `.SA` when the base is a pure 3-6
    digit code (Tadawul format). `normalize_ksa_symbol` already knows
    how to strip those suffixes, but `looks_like_ksa` previously only
    accepted `.SR`, so `normalize_symbol("2222.SAU")` skipped the KSA
    branch and returned the input unchanged. The pure-digit guard
    keeps Brazilian `.SA` tickers (PETR4.SA, VALE3.SA) out -- they
    have alphabetic prefixes and won't match `KSA_CODE_ONLY_RE`.
    """
    s = clean_unicode(symbol).upper()
    if not s:
        return False
    if s.startswith("TADAWUL:"):
        return True
    for suffix in (".SR", ".SAU", ".SA"):
        if s.endswith(suffix):
            code = s[:-len(suffix)].strip()
            if KSA_CODE_ONLY_RE.match(code):
                return True
    return bool(KSA_CODE_ONLY_RE.match(s))


@lru_cache(maxsize=20000)
def is_ksa(symbol: str) -> bool:
    """Check if symbol is a KSA (Saudi) symbol."""
    s = clean_unicode(symbol).upper()
    if not s:
        return False
    return bool(
        KSA_TADAWUL_RE.match(s)
        or KSA_SR_RE.match(s)
        or KSA_CODE_ONLY_RE.match(s)
    )


@lru_cache(maxsize=20000)
def is_isin(symbol: str) -> bool:
    """Check if symbol is an ISIN (International Securities Identification Number)."""
    return bool(ISIN_RE.match(clean_unicode(symbol).upper()))


@lru_cache(maxsize=20000)
def is_cusip(symbol: str) -> bool:
    """Check if symbol is a CUSIP (Committee on Uniform Securities Identification Procedures)."""
    return bool(CUSIP_RE.match(clean_unicode(symbol).upper()))


@lru_cache(maxsize=20000)
def is_sedol(symbol: str) -> bool:
    """Check if symbol is a SEDOL (Stock Exchange Daily Official List)."""
    return bool(SEDOL_RE.match(clean_unicode(symbol).upper()))


@lru_cache(maxsize=20000)
def is_option(symbol: str) -> bool:
    """Check if symbol is an OCC option."""
    return bool(OCC_OPTION_RE.match(clean_unicode(symbol).upper()))


def parse_occ_option(symbol: str) -> Optional[Dict[str, Any]]:
    """
    Parse OCC option symbol.

    Returns:
        Dict with underlying, expiration, type, strike or None
    """
    s = clean_unicode(symbol).upper()
    match = OCC_OPTION_RE.match(s)
    if not match:
        return None

    underlying, exp, right, strike_str = match.groups()
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
    """Check if symbol is an index."""
    s = clean_unicode(symbol).upper()
    if not s:
        return False
    if INDEX_CARET_RE.match(s) or INDEX_SUFFIX_RE.match(s):
        return True
    return s in _INDEX_COMMON or s in set(_INDEX_COMMON.values())


@lru_cache(maxsize=20000)
def is_crypto(symbol: str) -> bool:
    """Check if symbol is a cryptocurrency."""
    s = clean_unicode(symbol).upper()
    if not s:
        return False
    if CRYPTO_SUFFIX_RE.match(s):
        return True
    # Dash-form pairs (BTC-USD, ETH-USD): require the LEFT side to be a
    # known crypto ticker so we don't misclassify FX pairs like EUR-USD.
    dm = CRYPTO_DASH_RE.match(s)
    if dm and dm.group(1).upper() in _CRYPTO_COMMON:
        return True
    base = s.split("-")[0] if "-" in s else s
    return base in _CRYPTO_COMMON


@lru_cache(maxsize=20000)
def is_fx(symbol: str) -> bool:
    """
    Check if symbol is a forex pair.

    v7.0.0: if the symbol has the 3-letter dash form AND the left side
    is a known crypto ticker (like ETH-USD), treat as crypto, not FX.
    """
    s = clean_unicode(symbol).upper()
    if not s:
        return False
    if FX_EQUAL_RE.match(s) or FX_SLASH_RE.match(s) or FX_SUFFIX_RE.match(s):
        return True
    # Dash form: reject if left side is a known crypto
    dm = FX_DASH_RE.match(s)
    if dm:
        left = dm.group(1).upper()
        if left in _CRYPTO_COMMON:
            return False
        return True
    base = s.replace("=X", "").replace("/", "").replace("-", "")
    return base in _FX_COMMON_PAIRS


@lru_cache(maxsize=20000)
def is_commodity_future(symbol: str) -> bool:
    """Check if symbol is a commodity future."""
    s = clean_unicode(symbol).upper()
    if not s:
        return False
    if FUTURE_EQUAL_RE.match(s) or FUTURE_SUFFIX_RE.match(s):
        return True
    base = (
        s.replace("=F", "")
         .replace(".COMM", "")
         .replace(".COM", "")
         .replace(".FUT", "")
    )
    return base in _COMMODITY_CODES


@lru_cache(maxsize=20000)
def is_etf(symbol: str) -> bool:
    """Check if symbol is an ETF."""
    s = clean_unicode(symbol).upper()
    if not s:
        return False
    if ETF_SUFFIX_RE.match(s):
        return True
    base = s.split(".")[0] if "." in s else s
    return base in _ETF_COMMON_PREFIX


@lru_cache(maxsize=20000)
def is_special_symbol(symbol: str) -> bool:
    """Check if symbol is a special symbol (index, forex, commodity, crypto, etc.)."""
    return any([
        is_index(symbol),
        is_fx(symbol),
        is_commodity_future(symbol),
        is_crypto(symbol),
        is_isin(symbol),
        is_option(symbol),
    ])


# =============================================================================
# Exchange / Suffix Parsing
# =============================================================================

@lru_cache(maxsize=20000)
def split_symbol_exchange(symbol: str) -> Tuple[str, Optional[str]]:
    """
    Split symbol into (base, exchange_suffix) when the suffix is a real exchange suffix.

    Examples:
        "AAPL.US" -> ("AAPL", "US")
        "2222.SR" -> ("2222", "SR")
        "BRK.B" -> ("BRK.B", None)  # share class, NOT exchange suffix
        "BRK.B.US" -> ("BRK.B", "US")
    """
    s = normalize_symbol(symbol)
    if not s or "." not in s:
        return s, None

    base, suffix = s.rsplit(".", 1)
    key = f".{suffix.upper()}"
    if key in _EXCHANGE_SUFFIXES:
        return base, suffix.upper()

    # Not a known exchange suffix -> keep as part of base
    return s, None


@lru_cache(maxsize=20000)
def extract_exchange_code(symbol: str) -> Optional[str]:
    """Extract exchange code from symbol."""
    _, exchange = split_symbol_exchange(symbol)
    return exchange


# =============================================================================
# Advanced Detection
# =============================================================================

@lru_cache(maxsize=20000)
def detect_market_type(symbol: str) -> MarketType:
    """
    Detect market type for a symbol.

    v7.0.0: uses precomputed `_MARKET_BY_CODE` dict instead of scanning the
    full MarketType enum per call.
    """
    s = normalize_symbol(symbol)
    if not s:
        return MarketType.GLOBAL

    if is_special_symbol(s):
        return MarketType.SPECIAL
    if is_ksa(s):
        return MarketType.KSA

    _, exchange = split_symbol_exchange(s)
    if exchange:
        key = f".{exchange.upper()}"
        market_code = _EXCHANGE_SUFFIXES.get(key, "")
        if market_code:
            hit = _MARKET_BY_CODE.get(market_code.upper())
            if hit is not None:
                return hit

    # Heuristic for simple US tickers
    if "." not in s and s.isalpha() and 1 <= len(s) <= 5:
        return MarketType.US

    return MarketType.GLOBAL


@lru_cache(maxsize=20000)
def detect_asset_class(symbol: str) -> AssetClass:
    """
    Detect asset class for a symbol.

    v7.0.0 ordering: INDEX > CRYPTO > FX > COMMODITY > ETF > OPTION > REIT > ADR > EQUITY.
    Crypto is checked before FX because dashed pairs like ETH-USD are
    crypto, not forex (fixed via v7.0.0 is_fx/is_crypto refinement, but
    the ordering change makes the intent explicit).
    """
    s = normalize_symbol(symbol)
    if not s:
        return AssetClass.UNKNOWN

    if is_index(s):
        return AssetClass.INDEX
    if is_crypto(s):
        return AssetClass.CRYPTO
    if is_fx(s):
        return AssetClass.FOREX
    if is_commodity_future(s):
        return AssetClass.COMMODITY
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
    """
    Validate a symbol and return (quality, normalized_symbol).
    """
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

        if len(norm) > 35:
            return SymbolQuality.FAIR, norm

        if is_special_symbol(norm):
            return SymbolQuality.GOOD, norm

        if is_ksa(norm):
            if KSA_SR_RE.match(norm):
                return SymbolQuality.EXCELLENT, norm
            return SymbolQuality.GOOD, norm

        # If it has a known exchange suffix -> good
        _, exchange = split_symbol_exchange(norm)
        if exchange:
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
    """Normalize KSA symbol to canonical format (e.g., "2222.SR")."""
    s = clean_unicode(symbol).upper()
    if not s:
        return ""

    # Remove TADAWUL: prefix
    if s.startswith("TADAWUL:"):
        s = s.split(":", 1)[1].strip()

    # Remove common KSA-flavored suffixes
    for suffix in (".TADAWUL", ".SA", ".SAU"):
        if s.endswith(suffix):
            s = s[:-len(suffix)].strip()
            break

    # Already has .SR suffix
    if s.endswith(".SR"):
        code = s[:-3].strip()
        return f"{code}.SR" if KSA_CODE_ONLY_RE.match(code) else ""

    # Numeric code only
    if KSA_CODE_ONLY_RE.match(s):
        return f"{s}.SR"

    return ""


@lru_cache(maxsize=40000)
def normalize_symbol(symbol: str) -> str:
    """
    Canonical "neutral" symbol normalization.

    Rules:
        - KSA          -> always ####.SR
        - FX           -> EURUSD=X  (both slash AND dash forms canonicalized)
        - Index alias  -> ^GSPC etc. when in _INDEX_COMMON
        - INDX suffix  -> canonical ^ form when left side matches _INDEX_COMMON
        - Share class  -> BRK.B (dash form converted to dot form)
        - Equities     -> preserve as close to user intent as possible
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
        # FX: slash form -> EURUSD=X
        if "/" in u and is_fx(u):
            a, b = u.split("/", 1)
            return f"{a}{b}=X"
        # v7.0.0: FX dash form -> EURUSD=X  (v6 only handled slash)
        fx_dm = FX_DASH_RE.match(u)
        if fx_dm and is_fx(u):  # is_fx now rejects crypto dash pairs
            return f"{fx_dm.group(1)}{fx_dm.group(2)}=X"
        # Index alias: SPX -> ^GSPC
        if not u.startswith("^") and is_index(u) and u in _INDEX_COMMON:
            return _INDEX_COMMON[u]
        # v7.0.0: INDX-suffix canonicalization -> ^GSPC when left matches
        idx_sm = INDEX_SUFFIX_RE.match(u)
        if idx_sm:
            left = idx_sm.group(1).upper()
            if left in _INDEX_COMMON:
                return _INDEX_COMMON[left]
            # No common-index mapping -> carp form as fallback
            if not left.startswith("^"):
                return f"^{left}"
            return left
        return u

    # KSA canonicalization
    if looks_like_ksa(u) or is_ksa(u):
        k = normalize_ksa_symbol(u)
        return k or u

    # Share class: BRK-B -> BRK.B
    if "-" in u:
        parts = u.split("-")
        if len(parts) == 2 and len(parts[1]) == 1 and parts[1].isalpha():
            u = f"{parts[0]}.{parts[1]}"

    # Keep exchange suffix only if it is a known exchange suffix
    if "." in u:
        base, suffix = u.rsplit(".", 1)
        key = f".{suffix.upper()}"
        if key in _EXCHANGE_SUFFIXES:
            u = f"{base}.{suffix.upper()}"

    # Final safe character filter
    u = "".join(c for c in u if c.isalnum() or c in ".-^=/")
    u = u.strip(".-")

    # Optional: force default exchange suffix for plain equities
    if _CONFIG.default_equity_exchange_suffix:
        if "." not in u and not is_special_symbol(u) and not is_ksa(u):
            if u.isalpha() and 1 <= len(u) <= 8:
                u = f"{u}.{_CONFIG.default_equity_exchange_suffix}"

    return u


def normalize_symbols_list(
    symbols: Union[str, List[str]],
    limit: int = 0,
    unique: bool = True,
    validate: bool = False,
) -> List[str]:
    """
    Normalize a list of symbols.

    Args:
        symbols: String with delimiters or list of symbols
        limit: Maximum number of symbols to return (0 = no limit)
        unique: Whether to deduplicate symbols
        validate: Whether to skip symbols classified INVALID or POOR

    Returns:
        List of normalized symbols
    """
    if isinstance(symbols, str):
        parts = re.split(r"[\s,;|]+", symbols)
    else:
        parts = list(symbols)

    result: List[str] = []
    seen: Set[str] = set()

    for part in parts:
        if not part or not str(part).strip():
            continue
        norm = normalize_symbol(part)
        if not norm:
            continue

        if validate:
            quality, _ = validate_symbol(part)
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

@lru_cache(maxsize=20000)
def extract_base_symbol(symbol: str) -> str:
    """
    Extract base symbol without exchange suffix.

    Does NOT remove share-class (BRK.B stays BRK.B).
    """
    s = normalize_symbol(symbol)
    if not s:
        return ""

    # Preserve canonical KSA suffix for provider safety
    if is_ksa(s) and s.endswith(".SR"):
        return s

    base, exchange = split_symbol_exchange(s)
    return base if exchange else s


@lru_cache(maxsize=20000)
def standardize_share_class(symbol: str) -> str:
    """Standardize share class notation (BRK-B -> BRK.B)."""
    s = normalize_symbol(symbol)
    if not s:
        return ""
    if "-" in s:
        parts = s.split("-")
        if len(parts) == 2 and len(parts[1]) == 1:
            return f"{parts[0]}.{parts[1]}"
    return s


@lru_cache(maxsize=20000)
def get_primary_exchange(symbol: str) -> Optional[str]:
    """
    Get primary exchange for a symbol.

    v7.1.0: handles US share-class tickers (BRK.B, BF.B, RDS.A) which
    normalise to dot form, so split_symbol_exchange returns exchange=None
    (because '.B' is not a registered exchange suffix), and the
    `s.isalpha()` heuristic also fails (the dot makes isalpha() False).
    Now: if the dot is a share-class delimiter and the root is a
    plausible US ticker, route to "US".
    """
    s = normalize_symbol(symbol)
    if not s:
        return None

    if is_ksa(s):
        return "SAUDI"

    _, exchange = split_symbol_exchange(s)
    if exchange:
        key = f".{exchange.upper()}"
        return _EXCHANGE_SUFFIXES.get(key)

    if is_special_symbol(s):
        return "SPECIAL"

    # Plain US ticker heuristic
    if s.isalpha() and 1 <= len(s) <= 5:
        return "US"

    # v7.1.0: share-class US ticker heuristic (BRK.B etc.).
    cls_match = CLASS_DOT_RE.match(s)
    if cls_match:
        root = cls_match.group(1)
        if 1 <= len(root) <= 5:
            return "US"

    return None


@lru_cache(maxsize=20000)
def get_mic_code(symbol: str) -> Optional[str]:
    """Get MIC (Market Identifier Code) for a symbol."""
    exchange = get_primary_exchange(symbol)
    if not exchange:
        return None
    if exchange == "SAUDI":
        return "XSAU"
    return _MIC_MAPPINGS.get(exchange)


@lru_cache(maxsize=20000)
def get_currency_from_symbol(symbol: str) -> Optional[str]:
    """
    Get currency for a symbol.

    For FX pairs, returns the BASE currency (left side of the pair):
    e.g. USDJPY=X -> "USD", EURGBP=X -> "EUR".

    v7.1.0: replaced the inline US/UK/JP/EUR if-chain with a dict lookup
    against `_CURRENCY_BY_EXCHANGE`, picking up 30+ markets that v7.0.0
    silently returned None for: HK -> HKD, CN -> CNY, IN -> INR,
    AU -> AUD, CA -> CAD, CH -> CHF, KR -> KRW, SG -> SGD, ZA -> ZAR,
    AE -> AED, QA -> QAR, KW -> KWD, etc.

    NOTE: this returns the LISTING currency only. It does NOT solve the
    GBp-vs-GBP (pence vs pound) trap on UK listings -- that is a
    price-scaling concern for the data engine / currency-normalization
    layer, not a symbol concern.
    """
    s = normalize_symbol(symbol)
    if not s:
        return None

    if is_fx(s):
        base = (
            extract_base_symbol(s)
            .replace("/", "")
            .replace("-", "")
            .replace("=X", "")
        )
        return base[:3] if len(base) >= 3 else None

    if is_ksa(s):
        return "SAR"

    exchange = get_primary_exchange(s)
    if exchange:
        ccy = _CURRENCY_BY_EXCHANGE.get(exchange.upper())
        if ccy:
            return ccy

    return None


@lru_cache(maxsize=20000)
def get_country_from_symbol(symbol: str) -> Optional[str]:
    """
    Get the listing country for a symbol as a display-friendly name.

    NEW in v7.1.0. Closes the audit gap where the data engine returned
    "USA" for every non-`.SR` / non-`=X` / non-`=F` symbol -- mis-tagging
    `.L` (UK), `.DE` (Germany), `.PA` (France), `.T` (Japan), `.HK`,
    `.SS`, `.NS`, `.AX`, etc. as American.

    Resolution order:
        1. `_INDEX_COUNTRY` override for caret-prefixed major indexes
           (^GSPC -> United States, ^FTSE -> United Kingdom, ^N225 -> Japan,
           ^TASI -> Saudi Arabia, ...).
        2. KSA (Saudi Arabia) by `is_ksa`.
        3. "Global" for FX, crypto, commodity-future, ISIN, and option
           symbols (anything classified by `is_special_symbol`) once the
           index check has passed.
        4. `_COUNTRY_BY_EXCHANGE` lookup keyed by `get_primary_exchange`.
        5. Plain US ticker / US share-class heuristic.
        6. None when nothing matches.

    Returns:
        Display-friendly English country name (e.g. "United Kingdom",
        "Saudi Arabia", "United States", "Germany"), the literal string
        "Global" for asset-class symbols whose listing country is
        ambiguous or not meaningful, or None when the country cannot be
        determined.
    """
    s = normalize_symbol(symbol)
    if not s:
        return None

    # 1. Major indexes have a known listing country
    if s in _INDEX_COUNTRY:
        return _INDEX_COUNTRY[s]

    # 2. KSA shortcut (matches data engine's existing convention)
    if is_ksa(s):
        return "Saudi Arabia"

    # 3. Other special symbols: FX, crypto, commodity, ISIN, option
    if is_special_symbol(s):
        return "Global"

    # 4. Exchange-suffix lookup (the audit fix - no more silent USA fallback)
    exchange = get_primary_exchange(s)
    if exchange:
        country = _COUNTRY_BY_EXCHANGE.get(exchange.upper())
        if country:
            return country
        # exchange code recognised but no country mapping -> still better
        # than fabricating "USA"; surface the unknown rather than lie.
        return None

    # 5. Plain US heuristic mirrors get_primary_exchange's fallback
    if s.isalpha() and 1 <= len(s) <= 5:
        return "United States"

    cls_match = CLASS_DOT_RE.match(s)
    if cls_match and 1 <= len(cls_match.group(1)) <= 5:
        return "United States"

    return None


@lru_cache(maxsize=20000)
def market_hint_for(symbol: str) -> str:
    """Get market hint for a symbol."""
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
    Convert symbol to Yahoo Finance format.

    v7.2.0: equity exchange suffixes are now TRANSLATED to the exact
    suffix Yahoo expects (via `_YAHOO_SUFFIX_MAP`) instead of being
    echoed verbatim, and share-class DOT tickers are converted back to
    Yahoo's DASH form.

    Examples:
        "AAPL"      -> "AAPL"
        "AAPL.US"   -> "AAPL"
        "AAPL.NYSE" -> "AAPL"
        "III.LSE"   -> "III.L"
        "BARC.L"    -> "BARC.L"
        "7203.TYO"  -> "7203.T"
        "BRK.B"     -> "BRK-B"
        "BRK.B.US"  -> "BRK-B"
        "2222.SR"   -> "2222.SR"
        "EUR/USD"   -> "EURUSD=X"
        "^GSPC"     -> "^GSPC"
    """
    s = normalize_symbol(symbol)
    if not s:
        return ""

    if is_ksa(s):
        return s if s.endswith(".SR") else f"{s}.SR"

    if is_index(s):
        if s in _INDEX_COMMON:
            return _INDEX_COMMON[s]
        if s.startswith("^"):
            return s
        # Accept INDX-suffixed inputs as a safety net (normalize_symbol
        # already canonicalizes them, but downstream may call this fn
        # directly on a non-normalized input).
        idx_sm = INDEX_SUFFIX_RE.match(s)
        if idx_sm:
            left = idx_sm.group(1).upper()
            return _INDEX_COMMON.get(left, f"^{left}")
        return f"^{s}"

    if is_fx(s):
        base = extract_base_symbol(s).replace("/", "").replace("-", "")
        base = base.replace("=X", "")
        return f"{base}=X" if len(base) == 6 else s

    if is_commodity_future(s):
        return f"{extract_base_symbol(s)}=F"

    if is_crypto(s):
        return s if "-" in s else f"{s}-USD"

    # --- Equity ---------------------------------------------------------
    # v7.2.0: translate the (possibly aliased) exchange suffix to the
    # specific suffix Yahoo expects, and convert share-class DOT tickers
    # back to Yahoo's DASH form.
    base, raw_suffix = split_symbol_exchange(s)
    if raw_suffix is not None:
        yahoo_suffix = _YAHOO_SUFFIX_MAP.get(raw_suffix.upper())
        if yahoo_suffix is not None:
            base_out = _share_class_dot_to_dash(base)
            return f"{base_out}{yahoo_suffix}" if yahoo_suffix else base_out
        # Recognised by _EXCHANGE_SUFFIXES (e.g. a caller-added env
        # suffix) but with no Yahoo mapping -> pass through unchanged.
        return s

    # No exchange suffix. Could still be a share-class DOT ticker (BRK.B).
    return _share_class_dot_to_dash(s)


@lru_cache(maxsize=20000)
def to_finnhub_symbol(symbol: str) -> str:
    """
    Convert symbol to Finnhub format.

    v7.2.0: strips ALL US exchange aliases (.NYSE/.N/.OQ/.NM/.NG), not
    just the literal .US, so US-listed names always reach Finnhub as the
    bare ticker.
    """
    s = normalize_symbol(symbol)
    if not s:
        return ""

    if is_special_symbol(s):
        return s

    base, exchange = split_symbol_exchange(s)
    if exchange:
        # Any US-aliased suffix (.US/.NYSE/.N/.OQ/.NM/.NG) -> bare ticker.
        if _EXCHANGE_SUFFIXES.get(f".{exchange.upper()}") == "US":
            return base

    return s


@lru_cache(maxsize=20000)
def to_eodhd_symbol(symbol: str, default_exchange: Optional[str] = None) -> str:
    """
    Convert symbol to EODHD format (TICKER.EXCHANGE).

    v7.2.0: aliased exchange suffixes are now TRANSLATED to EODHD
    exchange codes (via `_EODHD_SUFFIX_MAP`) instead of being echoed
    verbatim. Yahoo-style short suffixes EODHD rejects (`.L`, `.NYSE`,
    `.N`, `.OQ`, `.TYO`, ...) are mapped to EODHD's codes. Suffixes not
    present in the map fall back to the raw suffix (pre-7.2.0 behaviour).

    Examples:
        "AAPL"      -> "AAPL.US"
        "AAPL.NYSE" -> "AAPL.US"
        "BARC.L"    -> "BARC.LSE"
        "III.LSE"   -> "III.LSE"
        "7203.TYO"  -> "7203.TSE"
        "BRK.B"     -> "BRK.B.US"
        "2222.SR"   -> "2222.SR"
    """
    s = normalize_symbol(symbol)
    if not s:
        return ""

    if is_special_symbol(s):
        return s

    if is_ksa(s):
        return s if s.endswith(".SR") else f"{s}.SR"

    base, raw_suffix = split_symbol_exchange(s)
    if raw_suffix is not None:
        # v7.2.0: map to EODHD's exchange code; unknown -> raw suffix.
        eodhd_code = _EODHD_SUFFIX_MAP.get(raw_suffix.upper(), raw_suffix.upper())
        return f"{base}.{eodhd_code}"

    exchange = (default_exchange or _CONFIG.default_exchange).upper()
    return f"{s}.{exchange}"


@lru_cache(maxsize=20000)
def to_bloomberg_symbol(symbol: str) -> str:
    """Convert symbol to Bloomberg format."""
    s = normalize_symbol(symbol)
    if not s:
        return ""

    if is_special_symbol(s):
        return s

    if is_ksa(s):
        return f"{s.replace('.SR', '')} AB"

    base, exchange = split_symbol_exchange(s)
    if exchange:
        bloomberg_ex = {
            "US": "US",
            "UK": "LN",
            "JP": "JP",
            "HK": "HK",
        }.get(exchange.upper(), exchange.upper())
        return f"{base} {bloomberg_ex}"

    return f"{s} US"


@lru_cache(maxsize=20000)
def to_reuters_symbol(symbol: str) -> str:
    """Convert symbol to Reuters format."""
    s = normalize_symbol(symbol)
    if not s:
        return ""

    base, exchange = split_symbol_exchange(s)
    if exchange:
        if exchange.upper() == "NASDAQ":
            return f"{base}.OQ"
        if exchange.upper() == "NYSE":
            return f"{base}.N"
    return s


@lru_cache(maxsize=20000)
def to_google_symbol(symbol: str) -> str:
    """Convert symbol to Google Finance format."""
    s = normalize_symbol(symbol)
    if not s:
        return ""

    if is_ksa(s):
        return f"TADAWUL:{s.replace('.SR', '')}"

    base, exchange = split_symbol_exchange(s)
    if exchange:
        exchange_map = {
            "US": "NYSE", "NASDAQ": "NASDAQ", "UK": "LON",
            "JP": "TYO", "HK": "HKG",
        }.get(exchange.upper(), exchange.upper())
        return f"{exchange_map}:{base}"
    return s


@lru_cache(maxsize=20000)
def to_tradingview_symbol(symbol: str) -> str:
    """Convert symbol to TradingView format."""
    s = normalize_symbol(symbol)
    if not s:
        return ""

    if is_ksa(s):
        return f"TADAWUL:{s.replace('.SR', '')}"

    if is_fx(s):
        return f"FX:{extract_base_symbol(s).replace('/', '').replace('-', '').replace('=X', '')}"

    if is_crypto(s):
        return f"BINANCE:{s.replace('-', '')}"

    base, exchange = split_symbol_exchange(s)
    if exchange:
        exchange_map = {
            "UK": "LSE", "FR": "EURONEXT", "DE": "XETR",
            "JP": "TSE", "HK": "HKEX", "CN": "SSE",
            "AU": "ASX", "CA": "TSX", "US": "NASDAQ",
        }.get(exchange.upper(), exchange.upper())
        return f"{exchange_map}:{base}"

    return f"NASDAQ:{s}"


# =============================================================================
# Symbol Variants Functions
# =============================================================================

def symbol_variants(symbol: str) -> List[str]:
    """Generate symbol variants for fallback lookups."""
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
        base, exchange = split_symbol_exchange(s)
        if exchange is None and CLASS_DOT_RE.match(s):
            root, cls = s.rsplit(".", 1)
            variants.append(f"{root}-{cls}")
        if exchange is not None and CLASS_DOT_RE.match(base):
            root, cls = base.rsplit(".", 1)
            variants.extend([f"{root}-{cls}.{exchange}"])

    if "-" in s:
        match = CLASS_DASH_RE.match(s)
        if match:
            root, cls = match.groups()
            variants.append(f"{root}.{cls}")

    # If no exchange suffix and not special, add common guesses
    if extract_exchange_code(s) is None and not is_special_symbol(s):
        variants.extend([f"{s}.US", f"{s}.L"])

    return _unique_preserve_order(variants)


def yahoo_symbol_variants(symbol: str) -> List[str]:
    """Generate Yahoo Finance symbol variants."""
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
    base, exchange = split_symbol_exchange(s)
    if exchange == "US":
        variants.append(base)
    if CLASS_DOT_RE.match(base) and exchange:
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
    """Generate Finnhub symbol variants."""
    s = normalize_symbol(symbol)
    if not s:
        return []

    variants: List[str] = []
    f = to_finnhub_symbol(s)
    if f:
        variants.append(f)

    base, exchange = split_symbol_exchange(s)
    if exchange and exchange != "US":
        variants.append(base)

    if extract_exchange_code(s) is None and not is_ksa(s) and not is_special_symbol(s):
        variants.append(f"{s}.US")

    return _unique_preserve_order(variants)


def eodhd_symbol_variants(symbol: str, default_exchange: Optional[str] = None) -> List[str]:
    """Generate EODHD symbol variants."""
    s = normalize_symbol(symbol)
    if not s:
        return []

    variants: List[str] = []
    exchange = (default_exchange or _CONFIG.default_exchange).upper()

    e = to_eodhd_symbol(s, default_exchange=exchange)
    if e:
        variants.append(e)

    if is_ksa(s):
        code = s.replace(".SR", "")
        variants.extend([f"{code}.SR", code])
        return _unique_preserve_order(variants)

    # Include plain + base forms
    base = extract_base_symbol(s)
    if base and base != s:
        variants.append(base)

    # Ensure default exchange suffix exists for equity-like tickers
    if extract_exchange_code(s) is None and not is_special_symbol(s):
        variants.extend([f"{s}.{exchange}"])

    # Share class: BRK.B -> BRK-B.US and BRK.B.US
    base2, exchange2 = split_symbol_exchange(s)
    if exchange2 is None and CLASS_DOT_RE.match(s):
        root, cls = s.rsplit(".", 1)
        variants.extend([f"{root}-{cls}.{exchange}", f"{root}.{cls}.{exchange}"])
    elif exchange2 is not None and CLASS_DOT_RE.match(base2):
        root, cls = base2.rsplit(".", 1)
        variants.extend([f"{root}-{cls}.{exchange2}", f"{root}.{cls}.{exchange2}"])

    # If dash share class was input, add dot version + suffix
    if CLASS_DASH_RE.match(s):
        root, cls = s.split("-", 1)
        variants.extend([f"{root}.{cls}.{exchange}", f"{root}.{cls}"])

    return _unique_preserve_order(variants)


def bloomberg_symbol_variants(symbol: str) -> List[str]:
    """Generate Bloomberg symbol variants."""
    s = normalize_symbol(symbol)
    if not s:
        return []

    variants: List[str] = []
    b = to_bloomberg_symbol(s)
    if b:
        variants.append(b)

    if b and " " in b:
        variants.append(b.replace(" ", ""))
        parts = b.split(" ")
        if len(parts) >= 2:
            variants.append(f"{parts[0]}.{parts[1]}")

    return _unique_preserve_order(variants)


def reuters_symbol_variants(symbol: str) -> List[str]:
    """Generate Reuters symbol variants."""
    s = normalize_symbol(symbol)
    if not s:
        return []

    variants: List[str] = []
    r = to_reuters_symbol(s)
    if r:
        variants.append(r)

    if r and "." in r:
        variants.append(r.replace(".OQ", ".O").replace(".N", ""))

    return _unique_preserve_order(variants)


# =============================================================================
# Provider-aware Normalization
# =============================================================================

@lru_cache(maxsize=40000)
def normalize_symbol_for_provider(
    symbol: str,
    provider: str,
    default_exchange: Optional[str] = None,
) -> str:
    """
    Normalize symbol for a specific provider.

    Args:
        symbol: Input symbol
        provider: Provider name (yahoo, finnhub, eodhd, google, tradingview)
        default_exchange: Default exchange for EODHD

    Returns:
        Provider-ready symbol
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
    # Fallback to neutral
    return normalize_symbol(symbol)
