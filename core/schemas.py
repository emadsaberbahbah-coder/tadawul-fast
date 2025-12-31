# core/schemas.py  (FULL REPLACEMENT)
"""
core/schemas.py
===========================================================
CANONICAL SHEET SCHEMAS + HEADERS — v3.4.1 (PROD SAFE)

Purpose
- Single source of truth for the canonical 59-column quote schema.
- Provide get_headers_for_sheet(sheet_name) used by:
    - core/enriched_quote.py
    - routes/ai_analysis.py
    - routes/advanced_analysis.py
    - Google Apps Script sheet builders
- Provide shared request models (BatchProcessRequest) used by routers.

Design rules
✅ Import-safe: no DataEngine imports, no heavy dependencies.
✅ Defensive: always returns a valid header list (never raises).
✅ Stable: DEFAULT_HEADERS_59 order must not change lightly.
✅ No mutation leaks: always returns COPIES of header lists.
✅ Better alignment: sheet aliases include your actual page names.
✅ Compatibility helpers:
    - HEADER_TO_FIELD remains for legacy callers
    - HEADER_FIELD_CANDIDATES adds multi-field fallbacks for engine/provider variations
    - FIELD_TO_HEADER recognizes common alias fields (week_52_high vs high_52w etc.)

✅ v3.4.1 enhancements (this revision)
- Header lookup is now tolerant to casing/punctuation variants:
  "Sub Sector" == "Sub-Sector", "Avg Volume (30d)" == "Avg Volume (30D)", etc.
- header_to_field() and header_field_candidates() normalize header labels before mapping.
- Adds a few practical alias fields seen in providers (safe, non-breaking).
"""

from __future__ import annotations

import re
from typing import Any, Dict, List, Optional, Sequence, Tuple

# Pydantic v2 preferred, v1 fallback
try:
    from pydantic import BaseModel, Field, ConfigDict, field_validator, model_validator  # type: ignore

    _PYDANTIC_V2 = True
except Exception:  # pragma: no cover
    from pydantic import BaseModel, Field, validator  # type: ignore

    ConfigDict = None  # type: ignore
    field_validator = None  # type: ignore
    model_validator = None  # type: ignore
    _PYDANTIC_V2 = False


SCHEMAS_VERSION = "3.4.1"

# =============================================================================
# Canonical 59-column schema (SOURCE OF TRUTH)
# =============================================================================

# Keep as a list for backward-compat, but NEVER return this object directly.
DEFAULT_HEADERS_59: List[str] = [
    # Identity
    "Symbol",
    "Company Name",
    "Sector",
    "Sub-Sector",
    "Market",
    "Currency",
    "Listing Date",
    # Prices
    "Last Price",
    "Previous Close",
    "Price Change",
    "Percent Change",
    "Day High",
    "Day Low",
    "52W High",
    "52W Low",
    "52W Position %",
    # Volume / Liquidity
    "Volume",
    "Avg Volume (30D)",
    "Value Traded",
    "Turnover %",
    # Shares / Cap
    "Shares Outstanding",
    "Free Float %",
    "Market Cap",
    "Free Float Market Cap",
    "Liquidity Score",
    # Fundamentals
    "EPS (TTM)",
    "Forward EPS",
    "P/E (TTM)",
    "Forward P/E",
    "P/B",
    "P/S",
    "EV/EBITDA",
    "Dividend Yield %",
    "Dividend Rate",
    "Payout Ratio %",
    "ROE %",
    "ROA %",
    "Net Margin %",
    "EBITDA Margin %",
    "Revenue Growth %",
    "Net Income Growth %",
    "Beta",
    # Technicals
    "Volatility (30D)",
    "RSI (14)",
    # Valuation / Targets
    "Fair Value",
    "Upside %",
    "Valuation Label",
    # Scores / Recommendation
    "Value Score",
    "Quality Score",
    "Momentum Score",
    "Opportunity Score",
    "Risk Score",
    "Overall Score",
    "Error",
    "Recommendation",
    # Meta
    "Data Source",
    "Data Quality",
    "Last Updated (UTC)",
    "Last Updated (Riyadh)",
]


def _ensure_len_59(headers: Sequence[str]) -> Tuple[str, ...]:
    """
    PROD-SAFE: never raises.
    If headers length != 59, returns the canonical DEFAULT_HEADERS_59 as tuple.
    """
    try:
        if isinstance(headers, (list, tuple)) and len(headers) == 59:
            return tuple(str(x) for x in headers)
    except Exception:
        pass
    return tuple(DEFAULT_HEADERS_59)


# Freeze the canonical list as a tuple for internal use (prevents accidental mutation).
_DEFAULT_59_TUPLE: Tuple[str, ...] = _ensure_len_59(DEFAULT_HEADERS_59)

# Normalize exported list to canonical (if someone edited it accidentally)
if len(DEFAULT_HEADERS_59) != 59:  # pragma: no cover
    DEFAULT_HEADERS_59 = list(_DEFAULT_59_TUPLE)


def is_canonical_headers(headers: Any) -> bool:
    """Returns True if headers is a 59-length sequence matching canonical labels exactly."""
    try:
        if not isinstance(headers, (list, tuple)):
            return False
        if len(headers) != 59:
            return False
        return list(map(str, headers)) == list(_DEFAULT_59_TUPLE)
    except Exception:
        return False


def coerce_headers_59(headers: Any) -> List[str]:
    """
    Returns a safe 59 header list.
    If given headers are invalid/wrong length => canonical headers.
    Never raises.
    """
    try:
        if isinstance(headers, (list, tuple)) and len(headers) == 59:
            return [str(x) for x in headers]
    except Exception:
        pass
    return list(_DEFAULT_59_TUPLE)


def validate_headers_59(headers: Any) -> Dict[str, Any]:
    """
    Debug-safe validation helper.
    Never raises.
    """
    try:
        ok = is_canonical_headers(headers)
        return {
            "ok": bool(ok),
            "expected_len": 59,
            "got_len": (len(headers) if isinstance(headers, (list, tuple)) else None),
        }
    except Exception:
        return {"ok": False, "expected_len": 59, "got_len": None}


# =============================================================================
# Header normalization (tolerant mapping)
# =============================================================================
def _norm_header_label(h: Optional[str]) -> str:
    """
    Normalize header labels for tolerant lookups.
    Example:
      "Avg Volume (30d)" -> "avg_volume_30d"
      "Sub Sector" -> "sub_sector"
      "Last Updated Riyadh" -> "last_updated_riyadh"
    """
    s = str(h or "").strip().lower()
    if not s:
        return ""
    # unify separators and remove noisy punctuation
    s = s.replace("%", " percent ")
    s = re.sub(r"[()\[\]{}]", " ", s)
    s = re.sub(r"[^a-z0-9]+", "_", s)
    s = re.sub(r"_+", "_", s).strip("_")
    return s


# Canonical header lookup by normalized key (auto-built)
_HEADER_CANON_BY_NORM: Dict[str, str] = {}
for _h in _DEFAULT_59_TUPLE:
    _HEADER_CANON_BY_NORM[_norm_header_label(_h)] = _h

# Common synonyms/variants that appear in code/sheets
_HEADER_SYNONYMS: Dict[str, str] = {
    "sub_sector": "Sub-Sector",
    "subsector": "Sub-Sector",
    "avg_volume_30d": "Avg Volume (30D)",
    "avg_volume_30d_": "Avg Volume (30D)",
    "volatility_30d": "Volatility (30D)",
    "last_updated_utc": "Last Updated (UTC)",
    "last_updated_riyadh": "Last Updated (Riyadh)",
    "last_updated_ksa": "Last Updated (Riyadh)",
    "free_float_percent": "Free Float %",
    "turnover_percent": "Turnover %",
    "dividend_yield_percent": "Dividend Yield %",
    "payout_ratio_percent": "Payout Ratio %",
    "roe_percent": "ROE %",
    "roa_percent": "ROA %",
}
for k, canon_header in list(_HEADER_SYNONYMS.items()):
    nk = _norm_header_label(k)
    if nk and canon_header in _DEFAULT_59_TUPLE:
        _HEADER_CANON_BY_NORM[nk] = canon_header


def _canonical_header_label(header: str) -> str:
    """
    Best-effort variant header -> canonical header label.
    Never raises.
    """
    h = str(header or "").strip()
    if not h:
        return ""
    if h in HEADER_TO_FIELD:  # exact canonical match
        return h
    nh = _norm_header_label(h)
    return _HEADER_CANON_BY_NORM.get(nh, h)


# =============================================================================
# Field aliases (engine/provider variations)
# =============================================================================
# Canonical field -> known aliases that might appear in payloads.
FIELD_ALIASES: Dict[str, Tuple[str, ...]] = {
    # Identity
    "symbol": ("symbol_normalized", "symbol_input", "ticker", "code"),
    "name": ("company_name", "long_name"),
    "sub_sector": ("subsector",),
    "market": ("market_region", "exchange", "listing_exchange"),
    # Prices
    "current_price": ("last_price", "price", "close", "last"),
    "day_high": ("high",),
    "day_low": ("low",),
    "previous_close": ("prev_close",),
    "price_change": ("change",),
    "percent_change": ("change_percent", "change_pct", "pct_change"),
    # 52W
    "week_52_high": ("high_52w", "52w_high"),
    "week_52_low": ("low_52w", "52w_low"),
    "position_52w_percent": ("position_52w",),
    # Liquidity / Shares
    "avg_volume_30d": ("avg_volume", "avg_vol_30d"),
    "value_traded": ("traded_value",),
    "turnover_percent": ("turnover", "turnover_pct"),
    "shares_outstanding": ("shares", "outstanding_shares"),
    "free_float": ("free_float_percent", "free_float_pct"),
    "market_cap": ("mkt_cap", "marketcapitalization"),
    "free_float_market_cap": ("ff_market_cap",),
    # Fundamentals
    "eps_ttm": ("eps",),
    "forward_eps": ("eps_forward",),
    "pe_ttm": ("pe",),
    "forward_pe": ("pe_forward",),
    "ev_ebitda": ("evebitda",),
    "dividend_yield": ("div_yield", "dividend_yield_pct"),
    "payout_ratio": ("payout", "payout_pct"),
    "roe": ("return_on_equity",),
    "roa": ("return_on_assets",),
    "net_margin": ("profit_margin",),
    "ebitda_margin": ("margin_ebitda",),
    "revenue_growth": ("rev_growth",),
    "net_income_growth": ("ni_growth",),
    "beta": ("beta_5y",),
    # Technicals
    "volatility_30d": ("vol_30d_ann", "vol30d"),
    "rsi_14": ("rsi14",),
    # Valuation / Targets
    "fair_value": ("expected_price_3m", "expected_price_12m", "ma200", "ma50"),
    "upside_percent": ("upside_pct",),
    # Scores/Rec
    "overall_score": ("score", "total_score"),
    # Meta
    "data_source": ("source", "provider"),
    "data_quality": ("dq",),
    "last_updated_utc": ("as_of_utc",),
    "last_updated_riyadh": ("as_of_riyadh", "last_updated_ksa"),
}

# Build reverse lookup: alias_field -> canonical_field
_ALIAS_TO_CANON: Dict[str, str] = {}
for canon, aliases in FIELD_ALIASES.items():
    for a in aliases:
        _ALIAS_TO_CANON[a] = canon


def canonical_field(field: str) -> str:
    """
    Best-effort alias -> canonical field.
    Example: high_52w -> week_52_high
    """
    f = str(field or "").strip()
    if not f:
        return ""
    return _ALIAS_TO_CANON.get(f, f)


# =============================================================================
# Header <-> Field mapping (UnifiedQuote alignment helper)
# =============================================================================

# Canonical (preferred) mapping
HEADER_TO_FIELD: Dict[str, str] = {
    # Identity
    "Symbol": "symbol",
    "Company Name": "name",
    "Sector": "sector",
    "Sub-Sector": "sub_sector",
    "Market": "market",
    "Currency": "currency",
    "Listing Date": "listing_date",
    # Prices
    "Last Price": "current_price",
    "Previous Close": "previous_close",
    "Price Change": "price_change",
    "Percent Change": "percent_change",
    "Day High": "day_high",
    "Day Low": "day_low",
    # 52W
    "52W High": "week_52_high",
    "52W Low": "week_52_low",
    "52W Position %": "position_52w_percent",
    # Volume/Liquidity
    "Volume": "volume",
    "Avg Volume (30D)": "avg_volume_30d",
    "Value Traded": "value_traded",
    "Turnover %": "turnover_percent",
    # Shares/Cap
    "Shares Outstanding": "shares_outstanding",
    "Free Float %": "free_float",
    "Market Cap": "market_cap",
    "Free Float Market Cap": "free_float_market_cap",
    "Liquidity Score": "liquidity_score",
    # Fundamentals
    "EPS (TTM)": "eps_ttm",
    "Forward EPS": "forward_eps",
    "P/E (TTM)": "pe_ttm",
    "Forward P/E": "forward_pe",
    "P/B": "pb",
    "P/S": "ps",
    "EV/EBITDA": "ev_ebitda",
    "Dividend Yield %": "dividend_yield",
    "Dividend Rate": "dividend_rate",
    "Payout Ratio %": "payout_ratio",
    "ROE %": "roe",
    "ROA %": "roa",
    "Net Margin %": "net_margin",
    "EBITDA Margin %": "ebitda_margin",
    "Revenue Growth %": "revenue_growth",
    "Net Income Growth %": "net_income_growth",
    "Beta": "beta",
    # Technicals
    "Volatility (30D)": "volatility_30d",
    "RSI (14)": "rsi_14",
    # Valuation
    "Fair Value": "fair_value",
    "Upside %": "upside_percent",
    "Valuation Label": "valuation_label",
    # Scores/Rec
    "Value Score": "value_score",
    "Quality Score": "quality_score",
    "Momentum Score": "momentum_score",
    "Opportunity Score": "opportunity_score",
    "Risk Score": "risk_score",
    "Overall Score": "overall_score",
    "Error": "error",
    "Recommendation": "recommendation",
    # Meta
    "Data Source": "data_source",
    "Data Quality": "data_quality",
    "Last Updated (UTC)": "last_updated_utc",
    "Last Updated (Riyadh)": "last_updated_riyadh",
}

# Multi-field fallback candidates per header (for robust mapping in routers)
# Ordered: preferred first, then aliases.
HEADER_FIELD_CANDIDATES: Dict[str, Tuple[str, ...]] = {}
for h, f in HEADER_TO_FIELD.items():
    canon = canonical_field(f)
    aliases = FIELD_ALIASES.get(canon, ())
    HEADER_FIELD_CANDIDATES[h] = (canon,) + tuple(a for a in aliases if a)

# Build FIELD_TO_HEADER that recognizes both canonical fields and known aliases.
_FIELD_TO_HEADER: Dict[str, str] = {}
for header, field in HEADER_TO_FIELD.items():
    canon = canonical_field(field)
    _FIELD_TO_HEADER[canon] = header
    for a in FIELD_ALIASES.get(canon, ()):
        _FIELD_TO_HEADER[a] = header

FIELD_TO_HEADER: Dict[str, str] = dict(_FIELD_TO_HEADER)


def header_to_field(header: str) -> str:
    """Best-effort header (any variant) -> canonical field name."""
    h = _canonical_header_label(header)
    return canonical_field(HEADER_TO_FIELD.get(h, str(header or "").strip()))


def header_field_candidates(header: str) -> Tuple[str, ...]:
    """
    Robust mapping helper:
    returns preferred + aliases (e.g. ("week_52_high","high_52w","52w_high")).
    Now tolerant to header variants.
    """
    h0 = str(header or "").strip()
    h = _canonical_header_label(h0)

    c = HEADER_FIELD_CANDIDATES.get(h)
    if c:
        return c

    # fallback: if caller passes unknown header, try using the header string itself
    f = canonical_field(h0)
    return (f,) if f else ()


def field_to_header(field: str) -> str:
    """Best-effort field (canonical or alias) -> header label."""
    f = str(field or "").strip()
    if not f:
        return ""
    return FIELD_TO_HEADER.get(f, FIELD_TO_HEADER.get(canonical_field(f), f))


# =============================================================================
# Sheet name normalization + mappings
# =============================================================================
def _norm_sheet_name(name: Optional[str]) -> str:
    """
    Normalizes sheet names from Google Sheets (spaces/case/punctuations).
    Examples:
      "Global_Markets" -> "global_markets"
      "Insights Analysis" -> "insights_analysis"
      "KSA-Tadawul (Market)" -> "ksa_tadawul_market"
      "My/Portfolio" -> "my_portfolio"
    """
    s = (name or "").strip().lower()
    if not s:
        return ""
    for ch in ["-", " ", ".", "/", "\\", "|", ":", ";", ","]:
        s = s.replace(ch, "_")
    s = s.replace("(", "_").replace(")", "_")
    while "__" in s:
        s = s.replace("__", "_")
    return s.strip("_")


# Store as tuples internally to prevent mutation.
_SHEET_HEADERS: Dict[str, Tuple[str, ...]] = {}


def _register(keys: List[str], headers: Tuple[str, ...]) -> None:
    for k in keys:
        kk = _norm_sheet_name(k)
        if kk:
            _SHEET_HEADERS[kk] = headers


# Canonical schema used everywhere for now
_register(
    keys=[
        # KSA / Tadawul
        "KSA_Tadawul",
        "KSA Tadawul",
        "ksa_tadawul",
        "ksa_tadawul_market",
        "KSA_Tadawul_Market",
        "KSA-Market",
        "Tadawul",
        "KSA",
        "Market_Leaders",
        "Market Leaders",
        "KSA_Market_Leaders",
        # Global
        "Global_Markets",
        "Global Markets",
        "Global",
        # Funds
        "Mutual_Funds",
        "Mutual Funds",
        "Funds",
        # Commodities & FX
        "Commodities_FX",
        "Commodities & FX",
        "FX",
        "Commodities",
        # Portfolio / Investment
        "My_Portfolio",
        "My Portfolio",
        "My_Portfolio_Investment",
        "My Portfolio Investment",
        "Investment_Income_Statement",
        "Investment Income Statement",
        "Portfolio",
        # Insights / Analysis / Advisor
        "Insights_Analysis",
        "Insights Analysis",
        "Insights",
        "Analysis",
        "Investment_Advisor",
        "Investment Advisor",
        "Advisor",
        # Additional pages
        "Economic_Calendar",
        "Economic Calendar",
        "Calendar",
        "Status",
    ],
    headers=_DEFAULT_59_TUPLE,
)


def resolve_sheet_key(sheet_name: Optional[str]) -> str:
    """Returns the normalized key used for lookups."""
    return _norm_sheet_name(sheet_name)


def get_headers_for_sheet(sheet_name: Optional[str] = None) -> List[str]:
    """
    Returns a safe headers list for the given sheet.
    - Always returns a list (never raises).
    - Returns a COPY to prevent accidental mutation by callers.
    """
    try:
        key = _norm_sheet_name(sheet_name)
        if not key:
            return list(_DEFAULT_59_TUPLE)

        v = _SHEET_HEADERS.get(key)
        if isinstance(v, tuple) and v:
            return list(v)

        # contains / prefix matching (best-effort)
        for k, vv in _SHEET_HEADERS.items():
            if key == k or key.startswith(k) or k in key:
                return list(vv)

        return list(_DEFAULT_59_TUPLE)
    except Exception:
        return list(_DEFAULT_59_TUPLE)


def get_supported_sheets() -> List[str]:
    """Useful for debugging / UI lists."""
    try:
        return sorted(list(_SHEET_HEADERS.keys()))
    except Exception:
        return []


# =============================================================================
# Shared request models
# =============================================================================
def _coerce_str_list(v: Any) -> List[str]:
    """
    Accept:
      - ["AAPL","MSFT"]
      - "AAPL,MSFT 1120.SR"
      - "AAPL MSFT,1120.SR"
      - None
    and return a clean list of strings.
    """
    if v is None:
        return []
    if isinstance(v, list):
        out: List[str] = []
        for x in v:
            if x is None:
                continue
            s = str(x).strip()
            if s:
                out.append(s)
        return out

    s = str(v).replace("\n", " ").replace("\t", " ").replace(",", " ").strip()
    if not s:
        return []
    return [p.strip() for p in s.split(" ") if p.strip()]


class _ExtraIgnore(BaseModel):
    if _PYDANTIC_V2:
        model_config = ConfigDict(extra="ignore")  # type: ignore
    else:  # pragma: no cover
        class Config:
            extra = "ignore"


class BatchProcessRequest(_ExtraIgnore):
    """
    Shared contract used by routers and sheet refresh endpoints.
    Supports both `symbols` and `tickers` (client robustness).
    """
    operation: str = Field(default="refresh")
    sheet_name: Optional[str] = Field(default=None)
    symbols: List[str] = Field(default_factory=list)
    tickers: List[str] = Field(default_factory=list)  # alias support

    if _PYDANTIC_V2:  # type: ignore
        @field_validator("symbols", mode="before")  # type: ignore
        def _v2_symbols(cls, v: Any) -> List[str]:
            return _coerce_str_list(v)

        @field_validator("tickers", mode="before")  # type: ignore
        def _v2_tickers(cls, v: Any) -> List[str]:
            return _coerce_str_list(v)

        @model_validator(mode="after")  # type: ignore
        def _v2_post(self) -> "BatchProcessRequest":
            self.symbols = _coerce_str_list(self.symbols)
            self.tickers = _coerce_str_list(self.tickers)
            return self

    else:  # pragma: no cover
        @validator("symbols", pre=True)  # type: ignore
        def _v1_symbols(cls, v: Any) -> List[str]:
            return _coerce_str_list(v)

        @validator("tickers", pre=True)  # type: ignore
        def _v1_tickers(cls, v: Any) -> List[str]:
            return _coerce_str_list(v)

    def all_symbols(self) -> List[str]:
        """
        Returns combined symbols (symbols + tickers), trimmed, in original order,
        without forcing uniqueness (caller may handle uniqueness/normalize).
        """
        out: List[str] = []
        for x in (self.symbols or []) + (self.tickers or []):
            if x is None:
                continue
            s = str(x).strip()
            if s:
                out.append(s)
        return out


__all__ = [
    "SCHEMAS_VERSION",
    "DEFAULT_HEADERS_59",
    "is_canonical_headers",
    "coerce_headers_59",
    "validate_headers_59",
    "FIELD_ALIASES",
    "canonical_field",
    "HEADER_TO_FIELD",
    "HEADER_FIELD_CANDIDATES",
    "FIELD_TO_HEADER",
    "header_to_field",
    "header_field_candidates",
    "field_to_header",
    "resolve_sheet_key",
    "get_headers_for_sheet",
    "get_supported_sheets",
    "BatchProcessRequest",
]
