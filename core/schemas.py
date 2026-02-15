# core/schemas.py
"""
core/schemas.py
===========================================================
CANONICAL SHEET SCHEMAS + HEADERS — v4.2.0 (PROD SAFE + ADVANCED NORMALIZATION)

Design Goals
- Import-safe: no network, no DataEngine imports, no heavy deps.
- Deterministic + defensive: never raises from public helpers.
- Backward compatible: keeps the legacy 59-column schema + public API from v3.x.
- vNext-first: customized per-page schemas by default (unless disabled / legacy selected).
- Strong tolerance: header synonyms + field aliases + fuzzy sheet name resolution.

v4.2.0 Enhancements
- ✅ Stronger sheet name normalization + richer aliases for common page names.
- ✅ Canonical header normalization upgraded (handles %/parentheses/underscores better).
- ✅ Synonym engine expanded (covers more real-world variants from Sheets + providers).
- ✅ Field alias mapping expanded and unified.
- ✅ New safe helpers: canonicalize_headers(), build_header_index(), validate_sheet_headers(),
  get_schema_info(), and safe mapping utilities (all optional; do not break old callers).
- ✅ Settings resolution hardened: ENV overrides + optional core.config.get_settings().

Public API preserved:
- get_headers_for_sheet(), get_supported_sheets(), get_header_groups()
- header_to_field(), header_field_candidates(), field_to_header(), canonical_field()
- BatchProcessRequest model compatible with Pydantic v1/v2

"""

from __future__ import annotations

import os
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


SCHEMAS_VERSION = "4.2.0"

# =============================================================================
# LEGACY: Canonical 59-column schema (kept for backward compatibility)
# =============================================================================
# NOTE: Do not change order lightly. Older routes/scripts assume this exact order.
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

# Legacy "analysis extras" (kept)
_LEGACY_ANALYSIS_EXTRAS: List[str] = [
    "Returns 1W %",
    "Returns 1M %",
    "Returns 3M %",
    "Returns 6M %",
    "Returns 12M %",
    "MA20",
    "MA50",
    "MA200",
    "Expected Return 1M %",
    "Expected Return 3M %",
    "Expected Return 12M %",
    "Expected Price 1M",
    "Expected Price 3M",
    "Expected Price 12M",
    "Confidence Score",
    "Forecast Method",
    "History Points",
    "History Source",
    "History Last (UTC)",
]
DEFAULT_HEADERS_ANALYSIS: List[str] = list(DEFAULT_HEADERS_59) + list(_LEGACY_ANALYSIS_EXTRAS)


def _ensure_len(headers: Sequence[str], expected: int, fallback: Sequence[str]) -> Tuple[str, ...]:
    """PROD-SAFE: never raises. If headers length != expected, returns fallback as tuple."""
    try:
        if isinstance(headers, (list, tuple)) and len(headers) == expected:
            return tuple(str(x) for x in headers)
    except Exception:
        pass
    return tuple(str(x) for x in fallback)


def _ensure_len_59(headers: Sequence[str]) -> Tuple[str, ...]:
    return _ensure_len(headers, 59, DEFAULT_HEADERS_59)


_DEFAULT_59_TUPLE: Tuple[str, ...] = _ensure_len_59(DEFAULT_HEADERS_59)
_DEFAULT_ANALYSIS_TUPLE: Tuple[str, ...] = tuple(str(x) for x in DEFAULT_HEADERS_ANALYSIS)

# Normalize exported lists to canonical if edited accidentally
if len(DEFAULT_HEADERS_59) != 59:  # pragma: no cover
    DEFAULT_HEADERS_59 = list(_DEFAULT_59_TUPLE)


def is_canonical_headers(headers: Any) -> bool:
    """True if headers is a 59-length sequence matching legacy canonical labels exactly."""
    try:
        if not isinstance(headers, (list, tuple)):
            return False
        if len(headers) != 59:
            return False
        return list(map(str, headers)) == list(_DEFAULT_59_TUPLE)
    except Exception:
        return False


def coerce_headers_59(headers: Any) -> List[str]:
    """Returns a safe 59 header list. Invalid/wrong length => legacy canonical headers. Never raises."""
    try:
        if isinstance(headers, (list, tuple)) and len(headers) == 59:
            return [str(x) for x in headers]
    except Exception:
        pass
    return list(_DEFAULT_59_TUPLE)


def validate_headers_59(headers: Any) -> Dict[str, Any]:
    """Debug-safe validation helper. Never raises."""
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
# vNext: Page-customized schemas (what your Sheets should use)
# =============================================================================

_VN_IDENTITY: List[str] = [
    "Rank",
    "Symbol",
    "Origin",
    "Name",
    "Sector",
    "Sub Sector",
    "Market",
    "Currency",
    "Listing Date",
]

_VN_PRICE: List[str] = [
    "Price",
    "Prev Close",
    "Change",
    "Change %",
    "Day High",
    "Day Low",
    "52W High",
    "52W Low",
    "52W Position %",
]

_VN_LIQUIDITY: List[str] = [
    "Volume",
    "Avg Vol 30D",
    "Value Traded",
    "Turnover %",
]

_VN_CAP: List[str] = [
    "Shares Outstanding",
    "Free Float %",
    "Market Cap",
    "Free Float Mkt Cap",
    "Liquidity Score",
]

_VN_FUNDAMENTALS: List[str] = [
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
]

_VN_TECHNICALS: List[str] = [
    "Volatility (30D)",
    "RSI (14)",
    "Trend Signal",
]

_VN_VALUATION: List[str] = [
    "Fair Value",
    "Upside %",
    "Valuation Label",
]

_VN_SCORES: List[str] = [
    "Value Score",
    "Quality Score",
    "Momentum Score",
    "Opportunity Score",
    "Risk Score",
    "Overall Score",
]

_VN_BADGES: List[str] = [
    "Rec Badge",
    "Momentum Badge",
    "Opportunity Badge",
    "Risk Badge",
]

# Forecast CORE (agreed)
_VN_FORECAST_CORE: List[str] = [
    "Forecast Price (1M)",
    "Expected ROI % (1M)",
    "Forecast Price (3M)",
    "Expected ROI % (3M)",
    "Forecast Price (12M)",
    "Expected ROI % (12M)",
    "Forecast Confidence",
    "Forecast Updated (UTC)",
    "Forecast Updated (Riyadh)",
]

_VN_FORECAST_EXTENDED: List[str] = [
    "Returns 1W %",
    "Returns 1M %",
    "Returns 3M %",
    "Returns 6M %",
    "Returns 12M %",
    "MA20",
    "MA50",
    "MA200",
    "Forecast Method",
    "History Points",
    "History Source",
    "History Last (UTC)",
]

_VN_FORECAST_FULL: List[str] = list(_VN_FORECAST_CORE) + list(_VN_FORECAST_EXTENDED)

_VN_META: List[str] = [
    "Error",
    "Recommendation",
    "Data Source",
    "Data Quality",
    "Last Updated (UTC)",
    "Last Updated (Riyadh)",
]

# --- Page Schemas (vNext) ---
_VN_HEADERS_KSA_TADAWUL: List[str] = (
    _VN_IDENTITY
    + _VN_PRICE
    + _VN_LIQUIDITY
    + _VN_CAP
    + _VN_FUNDAMENTALS
    + _VN_TECHNICALS
    + _VN_VALUATION
    + _VN_SCORES
    + _VN_BADGES
    + _VN_FORECAST_CORE
    + _VN_META
)

_VN_HEADERS_MARKET_LEADERS: List[str] = list(_VN_HEADERS_KSA_TADAWUL)

_VN_HEADERS_GLOBAL: List[str] = (
    _VN_IDENTITY
    + _VN_PRICE
    + _VN_LIQUIDITY
    + _VN_CAP
    + _VN_FUNDAMENTALS
    + _VN_TECHNICALS
    + _VN_VALUATION
    + _VN_SCORES
    + _VN_BADGES
    + _VN_FORECAST_FULL
    + _VN_META
)

_VN_HEADERS_FUNDS: List[str] = (
    [
        "Rank",
        "Symbol",
        "Origin",
        "Name",
        "Fund Type",
        "Fund Category",
        "Market",
        "Currency",
        "Inception Date",
    ]
    + [
        "Price",
        "Prev Close",
        "Change",
        "Change %",
        "Day High",
        "Day Low",
        "52W High",
        "52W Low",
        "52W Position %",
    ]
    + [
        "Volume",
        "Avg Vol 30D",
        "Value Traded",
        "Expense Ratio",
        "AUM",
        "Distribution Yield",
        "Beta",
        "Volatility (30D)",
        "RSI (14)",
        "Fair Value",
        "Upside %",
        "Valuation Label",
        "Momentum Score",
        "Risk Score",
        "Overall Score",
        "Rec Badge",
    ]
    + _VN_FORECAST_FULL
    + _VN_META
)

_VN_HEADERS_COMMODITIES_FX: List[str] = (
    [
        "Rank",
        "Symbol",
        "Origin",
        "Name",
        "Asset Class",
        "Sub Class",
        "Market",
        "Currency",
    ]
    + [
        "Price",
        "Prev Close",
        "Change",
        "Change %",
        "Day High",
        "Day Low",
        "52W High",
        "52W Low",
        "52W Position %",
        "Volume",
        "Value Traded",
        "Volatility (30D)",
        "RSI (14)",
        "Fair Value",
        "Upside %",
        "Valuation Label",
        "Momentum Score",
        "Risk Score",
        "Overall Score",
        "Rec Badge",
    ]
    + _VN_FORECAST_FULL
    + _VN_META
)

_VN_HEADERS_PORTFOLIO: List[str] = (
    [
        "Rank",
        "Symbol",
        "Origin",
        "Name",
        "Market",
        "Currency",
        "Asset Type",
        "Portfolio Group",
        "Broker/Account",
    ]
    + [
        "Quantity",
        "Avg Cost",
        "Cost Value",
        "Target Weight %",
        "Notes",
    ]
    + [
        "Price",
        "Prev Close",
        "Change",
        "Change %",
        "Day High",
        "Day Low",
        "52W High",
        "52W Low",
        "52W Position %",
        "Volume",
        "Value Traded",
    ]
    + [
        "Market Value",
        "Unrealized P/L",
        "Unrealized P/L %",
        "Weight %",
        "Rebalance Δ",
    ]
    + [
        "Fair Value",
        "Upside %",
        "Valuation Label",
        "Forecast Price (1M)",
        "Expected ROI % (1M)",
        "Forecast Price (3M)",
        "Expected ROI % (3M)",
        "Forecast Price (12M)",
        "Expected ROI % (12M)",
        "Forecast Confidence",
        "Forecast Updated (UTC)",
        "Recommendation",
        "Rec Badge",
    ]
    + [
        "Risk Score",
        "Overall Score",
        "Volatility (30D)",
        "RSI (14)",
    ]
    + [
        "Error",
        "Data Source",
        "Data Quality",
        "Last Updated (UTC)",
        "Last Updated (Riyadh)",
    ]
)

_VN_HEADERS_INSIGHTS_ANALYSIS: List[str] = list(_VN_HEADERS_GLOBAL)

# Freeze vNext schemas as tuples (prevent accidental mutation)
_VN_KSA_T: Tuple[str, ...] = tuple(_VN_HEADERS_KSA_TADAWUL)
_VN_ML_T: Tuple[str, ...] = tuple(_VN_HEADERS_MARKET_LEADERS)
_VN_GLOBAL_T: Tuple[str, ...] = tuple(_VN_HEADERS_GLOBAL)
_VN_FUNDS_T: Tuple[str, ...] = tuple(_VN_HEADERS_FUNDS)
_VN_COMFX_T: Tuple[str, ...] = tuple(_VN_HEADERS_COMMODITIES_FX)
_VN_PORTFOLIO_T: Tuple[str, ...] = tuple(_VN_HEADERS_PORTFOLIO)
_VN_INSIGHTS_T: Tuple[str, ...] = tuple(_VN_HEADERS_INSIGHTS_ANALYSIS)

VNEXT_SCHEMAS: Dict[str, Tuple[str, ...]] = {
    "KSA_TADAWUL": _VN_KSA_T,
    "MARKET_LEADERS": _VN_ML_T,
    "GLOBAL_MARKETS": _VN_GLOBAL_T,
    "MUTUAL_FUNDS": _VN_FUNDS_T,
    "COMMODITIES_FX": _VN_COMFX_T,
    "MY_PORTFOLIO": _VN_PORTFOLIO_T,
    "INSIGHTS_ANALYSIS": _VN_INSIGHTS_T,
}

# =============================================================================
# Header grouping (category-wise cross-check helper)
# =============================================================================
_HEADER_GROUPS_VNEXT: Dict[str, Tuple[str, ...]] = {
    "Identity": tuple(_VN_IDENTITY),
    "Price": tuple(_VN_PRICE),
    "Liquidity": tuple(_VN_LIQUIDITY),
    "Capitalization": tuple(_VN_CAP),
    "Fundamentals": tuple(_VN_FUNDAMENTALS),
    "Technicals": tuple(_VN_TECHNICALS),
    "Valuation": tuple(_VN_VALUATION),
    "Scores": tuple(_VN_SCORES),
    "Badges": tuple(_VN_BADGES),
    "Forecast Core": tuple(_VN_FORECAST_CORE),
    "Forecast Extended": tuple(_VN_FORECAST_EXTENDED),
    "Meta": tuple(_VN_META),
}

# =============================================================================
# Header normalization (tolerant mapping)
# =============================================================================
_PAT_SPACES = re.compile(r"\s+")
_PAT_NON_ALNUM = re.compile(r"[^a-z0-9]+")


def _norm_header_label(h: Optional[str]) -> str:
    """
    Normalize header labels for tolerant lookups.

    Examples:
      "Avg Vol 30D" -> "avg_vol_30d"
      "Sub Sector" -> "sub_sector"
      "Last Updated (Riyadh)" -> "last_updated_riyadh"
      "Unrealized P/L %" -> "unrealized_p_l_percent"
    """
    try:
        s = str(h or "").strip().lower()
    except Exception:
        return ""
    if not s:
        return ""

    # keep semantic tokens
    s = s.replace("%", " percent ")
    s = s.replace("Δ", " delta ")
    s = s.replace("/", " ")
    s = s.replace("&", " and ")

    # remove brackets
    s = re.sub(r"[()\[\]{}]", " ", s)

    # collapse/normalize
    s = _PAT_SPACES.sub(" ", s).strip()
    s = _PAT_NON_ALNUM.sub("_", s)
    s = re.sub(r"_+", "_", s).strip("_")
    return s


_HEADER_CANON_BY_NORM: Dict[str, str] = {}


def _seed_canon_headers(headers: Sequence[str]) -> None:
    for hh in headers:
        nh = _norm_header_label(hh)
        if nh:
            _HEADER_CANON_BY_NORM[nh] = str(hh)


_seed_canon_headers(_DEFAULT_59_TUPLE)
_seed_canon_headers(_DEFAULT_ANALYSIS_TUPLE)
_seed_canon_headers(_VN_KSA_T)
_seed_canon_headers(_VN_ML_T)
_seed_canon_headers(_VN_GLOBAL_T)
_seed_canon_headers(_VN_FUNDS_T)
_seed_canon_headers(_VN_COMFX_T)
_seed_canon_headers(_VN_PORTFOLIO_T)
_seed_canon_headers(_VN_INSIGHTS_T)

# Expanded header synonyms: normalized_key -> canonical header label
_HEADER_SYNONYMS: Dict[str, str] = {
    # Identity
    "company_name": "Name",
    "company": "Name",
    "long_name": "Name",
    "issuer": "Name",
    "subsector": "Sub Sector",
    "sub_sector": "Sub Sector",
    "sub_sector_name": "Sub Sector",
    "industry": "Sub Sector",
    "exchange": "Market",
    "market_region": "Market",
    "listing_exchange": "Market",
    "ccy": "Currency",
    "currency_code": "Currency",
    "ipo_date": "Listing Date",
    "listed_at": "Listing Date",

    # Prices
    "last_price": "Price",
    "last": "Price",
    "close": "Price",
    "current_price": "Price",
    "previous_close": "Prev Close",
    "prev_close": "Prev Close",
    "prior_close": "Prev Close",
    "price_change": "Change",
    "pct_change": "Change %",
    "percent_change": "Change %",
    "change_pct": "Change %",
    "change_percent": "Change %",
    "high": "Day High",
    "low": "Day Low",

    # 52W
    "high_52w": "52W High",
    "52w_high": "52W High",
    "low_52w": "52W Low",
    "52w_low": "52W Low",
    "position_52w": "52W Position %",
    "position_52w_percent": "52W Position %",
    "pos_52w_pct": "52W Position %",

    # Liquidity/cap
    "avg_volume_30d": "Avg Vol 30D",
    "avg_vol_30d": "Avg Vol 30D",
    "avg_volume": "Avg Vol 30D",
    "avg_volume_30day": "Avg Vol 30D",
    "traded_value": "Value Traded",
    "turnover_value": "Value Traded",
    "turnover": "Turnover %",
    "turnover_pct": "Turnover %",
    "free_float_percent": "Free Float %",
    "free_float_pct": "Free Float %",
    "mkt_cap": "Market Cap",
    "marketcapitalization": "Market Cap",
    "free_float_market_cap": "Free Float Mkt Cap",
    "free_float_mkt_cap": "Free Float Mkt Cap",
    "ff_market_cap": "Free Float Mkt Cap",
    "liq_score": "Liquidity Score",

    # Fundamentals (percent tolerant)
    "dividend_yield": "Dividend Yield %",
    "dividend_yield_pct": "Dividend Yield %",
    "dividend_yield_percent": "Dividend Yield %",
    "payout_ratio": "Payout Ratio %",
    "payout_pct": "Payout Ratio %",
    "return_on_equity": "ROE %",
    "roe": "ROE %",
    "return_on_assets": "ROA %",
    "roa": "ROA %",
    "profit_margin": "Net Margin %",
    "net_margin": "Net Margin %",
    "margin_ebitda": "EBITDA Margin %",
    "ebitda_margin": "EBITDA Margin %",
    "rev_growth": "Revenue Growth %",
    "revenue_growth": "Revenue Growth %",
    "ni_growth": "Net Income Growth %",
    "net_income_growth": "Net Income Growth %",

    # Technicals
    "volatility_30d": "Volatility (30D)",
    "vol30d": "Volatility (30D)",
    "vol_30d": "Volatility (30D)",
    "rsi_14": "RSI (14)",
    "rsi14": "RSI (14)",
    "trend_signal": "Trend Signal",

    # Forecast (aligned)
    "forecast_price_1m": "Forecast Price (1M)",
    "target_price_1m": "Forecast Price (1M)",
    "expected_price_1m": "Forecast Price (1M)",
    "forecast_price_3m": "Forecast Price (3M)",
    "target_price_3m": "Forecast Price (3M)",
    "expected_price_3m": "Forecast Price (3M)",
    "forecast_price_12m": "Forecast Price (12M)",
    "target_price_12m": "Forecast Price (12M)",
    "expected_price_12m": "Forecast Price (12M)",

    "expected_roi_1m": "Expected ROI % (1M)",
    "expected_return_1m": "Expected ROI % (1M)",
    "roi_1m": "Expected ROI % (1M)",
    "expected_roi_3m": "Expected ROI % (3M)",
    "expected_return_3m": "Expected ROI % (3M)",
    "roi_3m": "Expected ROI % (3M)",
    "expected_roi_12m": "Expected ROI % (12M)",
    "expected_return_12m": "Expected ROI % (12M)",
    "roi_12m": "Expected ROI % (12M)",

    "confidence_score": "Forecast Confidence",
    "forecast_confidence": "Forecast Confidence",
    "forecast_updated_utc": "Forecast Updated (UTC)",
    "forecast_updated_riyadh": "Forecast Updated (Riyadh)",

    "hist_last_utc": "History Last (UTC)",
    "history_last_utc": "History Last (UTC)",

    # Meta timestamps
    "as_of_utc": "Last Updated (UTC)",
    "last_updated_utc": "Last Updated (UTC)",
    "as_of_riyadh": "Last Updated (Riyadh)",
    "last_updated_ksa": "Last Updated (Riyadh)",
    "last_updated_riyadh": "Last Updated (Riyadh)",

    # Portfolio
    "broker_account": "Broker/Account",
    "account": "Broker/Account",
    "target_weight": "Target Weight %",
    "target_weight_percent": "Target Weight %",
    "unrealized_pnl": "Unrealized P/L",
    "unrealized_pl": "Unrealized P/L",
    "unrealized_pnl_percent": "Unrealized P/L %",
    "weight": "Weight %",

    # Legacy labels supported
    "sub_sector_legacy": "Sub-Sector",
    "avg_volume_30d_legacy": "Avg Volume (30D)",
    "free_float_market_cap_legacy": "Free Float Market Cap",
    "last_price_legacy": "Last Price",
    "previous_close_legacy": "Previous Close",
    "price_change_legacy": "Price Change",
    "percent_change_legacy": "Percent Change",
}

# Bind synonyms into canonical table
for k, canon_header in list(_HEADER_SYNONYMS.items()):
    nk = _norm_header_label(k)
    if nk and canon_header:
        _HEADER_CANON_BY_NORM[nk] = canon_header


def _canonical_header_label(header: str) -> str:
    """Best-effort variant header -> canonical header label. Never raises."""
    try:
        h = str(header or "").strip()
    except Exception:
        return ""
    if not h:
        return ""
    nh = _norm_header_label(h)
    return _HEADER_CANON_BY_NORM.get(nh, h)


# =============================================================================
# Field aliases (engine/provider variations)
# =============================================================================
FIELD_ALIASES: Dict[str, Tuple[str, ...]] = {
    # Identity
    "rank": ("row_rank", "position", "rank_num", "market_rank"),
    "symbol": ("symbol_normalized", "symbol_input", "ticker", "code", "instrument", "ric"),
    "origin": ("page_key", "sheet_key", "source_page", "exchange"),
    "name": ("company_name", "long_name", "issuer", "display_name"),
    "sector": ("sector_name", "industry_sector"),
    "sub_sector": ("subsector", "industry", "industry_group"),
    "market": ("market_region", "listing_exchange", "market_name"),
    "currency": ("ccy", "currency_code", "currency_symbol"),
    "listing_date": ("ipo_date", "listed_at", "listingDate"),

    # Prices
    "current_price": ("last_price", "price", "close", "last", "lastTradePrice", "regularMarketPrice"),
    "previous_close": ("prev_close", "prior_close", "previousClose", "regularMarketPreviousClose"),
    "price_change": ("change", "priceChange"),
    "percent_change": ("change_percent", "change_pct", "pct_change", "percentChange"),
    "day_high": ("high", "dayHigh", "regularMarketDayHigh"),
    "day_low": ("low", "dayLow", "regularMarketDayLow"),

    # 52W
    "week_52_high": ("high_52w", "52w_high", "fiftyTwoWeekHigh"),
    "week_52_low": ("low_52w", "52w_low", "fiftyTwoWeekLow"),
    "position_52w_percent": ("position_52w", "pos_52w_pct", "fiftyTwoWeekRangePosition"),

    # Liquidity / Shares
    "volume": ("vol", "regularMarketVolume"),
    "avg_volume_30d": ("avg_volume", "avg_vol_30d", "avg_volume_30day", "averageDailyVolume3Month"),
    "value_traded": ("traded_value", "turnover_value"),
    "turnover_percent": ("turnover", "turnover_pct"),
    "shares_outstanding": ("shares", "outstanding_shares", "sharesOutstanding"),
    "free_float": ("free_float_percent", "free_float_pct"),
    "market_cap": ("mkt_cap", "marketcapitalization", "marketCap"),
    "free_float_market_cap": ("ff_market_cap", "free_float_mkt_cap", "freeFloatMarketCap"),
    "liquidity_score": ("liq_score",),

    # Fundamentals
    "eps_ttm": ("eps", "trailingEps"),
    "forward_eps": ("eps_forward", "forwardEps"),
    "pe_ttm": ("pe", "trailingPE"),
    "forward_pe": ("pe_forward", "forwardPE"),
    "pb": ("p_b", "priceToBook"),
    "ps": ("p_s", "priceToSalesTrailing12Months"),
    "ev_ebitda": ("evebitda", "enterpriseToEbitda"),
    "dividend_yield": ("div_yield", "dividend_yield_pct", "dividend_yield_percent", "dividendYield"),
    "dividend_rate": ("div_rate", "dividendRate"),
    "payout_ratio": ("payout", "payout_pct", "payoutRatio"),
    "roe": ("return_on_equity",),
    "roa": ("return_on_assets",),
    "net_margin": ("profit_margin", "profitMargins"),
    "ebitda_margin": ("margin_ebitda",),
    "revenue_growth": ("rev_growth", "revenueGrowth"),
    "net_income_growth": ("ni_growth",),
    "beta": ("beta_5y", "beta3Year", "beta"),

    # Technicals
    "volatility_30d": ("vol_30d_ann", "vol30d", "vol_30d"),
    "rsi_14": ("rsi14",),
    "trend_signal": ("trend", "signal"),

    # Valuation / targets
    "fair_value": ("intrinsic_value",),
    "upside_percent": ("upside_pct",),
    "valuation_label": ("valuation",),

    # Scores / badges
    "value_score": ("score_value",),
    "quality_score": ("score_quality",),
    "momentum_score": ("score_momentum",),
    "opportunity_score": ("score_opportunity",),
    "risk_score": ("score_risk",),
    "overall_score": ("score", "total_score", "advisor_score"),
    "rec_badge": ("recommendation_badge",),
    "momentum_badge": ("mom_badge",),
    "opportunity_badge": ("opp_badge",),
    "risk_badge": ("rk_badge",),

    "error": ("err",),
    "recommendation": ("recommend", "action", "signal_text"),

    # History / technical extras
    "returns_1w": ("return_1w", "ret_1w"),
    "returns_1m": ("return_1m", "ret_1m"),
    "returns_3m": ("return_3m", "ret_3m"),
    "returns_6m": ("return_6m", "ret_6m"),
    "returns_12m": ("return_12m", "ret_12m"),
    "ma20": ("sma20",),
    "ma50": ("sma50",),
    "ma200": ("sma200",),

    # Forecast (aligned)
    "expected_roi_1m": ("expected_return_1m", "exp_return_1m", "roi_1m", "expected_roi_percent_1m"),
    "expected_roi_3m": ("expected_return_3m", "exp_return_3m", "roi_3m", "expected_roi_percent_3m"),
    "expected_roi_12m": ("expected_return_12m", "exp_return_12m", "roi_12m", "expected_roi_percent_12m"),

    "forecast_price_1m": ("expected_price_1m", "exp_price_1m", "target_price_1m"),
    "forecast_price_3m": ("expected_price_3m", "exp_price_3m", "target_price_3m"),
    "forecast_price_12m": ("expected_price_12m", "exp_price_12m", "target_price_12m"),

    "forecast_confidence": ("confidence_score", "conf_score"),
    "forecast_method": ("forecast_model",),

    "history_points": ("hist_points",),
    "history_source": ("hist_source",),
    "history_last_utc": ("hist_last_utc", "history_last"),

    "forecast_updated_utc": ("forecast_last_utc", "forecast_asof_utc", "forecast_time_utc", "forecast_updated"),
    "forecast_updated_riyadh": ("forecast_asof_riyadh", "forecast_time_riyadh"),

    # Funds
    "fund_type": ("etf_type",),
    "fund_category": ("category",),
    "inception_date": ("fund_inception_date",),
    "expense_ratio": ("expense_ratio_percent",),
    "aum": ("assets_under_management",),
    "distribution_yield": ("yield", "yield_percent"),

    # Commodities/FX
    "asset_class": ("class",),
    "sub_class": ("subclass",),

    # Meta
    "data_source": ("source", "provider"),
    "data_quality": ("dq",),
    "last_updated_utc": ("as_of_utc",),
    "last_updated_riyadh": ("as_of_riyadh", "last_updated_ksa"),

    # Portfolio
    "asset_type": ("instrument_type", "security_type"),
    "portfolio_group": ("group",),
    "broker_account": ("account",),
    "quantity": ("qty",),
    "avg_cost": ("avg_price", "cost_avg"),
    "cost_value": ("cost_basis",),
    "target_weight": ("target_weight_percent",),
    "notes": ("comment",),
    "market_value": ("position_value",),
    "unrealized_pl": ("unrealized_pnl",),
    "unrealized_pl_percent": ("unrealized_pnl_percent",),
    "weight_percent": ("weight",),
    "rebalance_delta": ("rebalance",),
}

_ALIAS_TO_CANON: Dict[str, str] = {}
for canon, aliases in FIELD_ALIASES.items():
    for a in aliases:
        _ALIAS_TO_CANON[str(a)] = canon


def canonical_field(field: str) -> str:
    """Best-effort alias -> canonical field. Example: high_52w -> week_52_high"""
    try:
        f = str(field or "").strip()
    except Exception:
        return ""
    if not f:
        return ""
    return _ALIAS_TO_CANON.get(f, f)


# =============================================================================
# Header <-> Field mapping (UnifiedQuote alignment helper)
# =============================================================================
HEADER_TO_FIELD: Dict[str, str] = {
    # --- vNext Identity ---
    "Rank": "rank",
    "Symbol": "symbol",
    "Origin": "origin",
    "Name": "name",
    "Sector": "sector",
    "Sub Sector": "sub_sector",
    "Market": "market",
    "Currency": "currency",
    "Listing Date": "listing_date",

    # --- vNext Prices ---
    "Price": "current_price",
    "Prev Close": "previous_close",
    "Change": "price_change",
    "Change %": "percent_change",
    "Day High": "day_high",
    "Day Low": "day_low",
    "52W High": "week_52_high",
    "52W Low": "week_52_low",
    "52W Position %": "position_52w_percent",

    # --- vNext Liquidity / Cap ---
    "Volume": "volume",
    "Avg Vol 30D": "avg_volume_30d",
    "Value Traded": "value_traded",
    "Turnover %": "turnover_percent",
    "Shares Outstanding": "shares_outstanding",
    "Free Float %": "free_float",
    "Market Cap": "market_cap",
    "Free Float Mkt Cap": "free_float_market_cap",
    "Liquidity Score": "liquidity_score",

    # --- vNext Fundamentals ---
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

    # --- vNext Technicals ---
    "Volatility (30D)": "volatility_30d",
    "RSI (14)": "rsi_14",
    "Trend Signal": "trend_signal",

    # --- vNext Valuation ---
    "Fair Value": "fair_value",
    "Upside %": "upside_percent",
    "Valuation Label": "valuation_label",

    # --- vNext Scores / badges ---
    "Value Score": "value_score",
    "Quality Score": "quality_score",
    "Momentum Score": "momentum_score",
    "Opportunity Score": "opportunity_score",
    "Risk Score": "risk_score",
    "Overall Score": "overall_score",
    "Rec Badge": "rec_badge",
    "Momentum Badge": "momentum_badge",
    "Opportunity Badge": "opportunity_badge",
    "Risk Badge": "risk_badge",

    # --- Forecast / history (FULL) ---
    "Returns 1W %": "returns_1w",
    "Returns 1M %": "returns_1m",
    "Returns 3M %": "returns_3m",
    "Returns 6M %": "returns_6m",
    "Returns 12M %": "returns_12m",
    "MA20": "ma20",
    "MA50": "ma50",
    "MA200": "ma200",
    "Forecast Method": "forecast_method",
    "History Points": "history_points",
    "History Source": "history_source",
    "History Last (UTC)": "history_last_utc",

    # --- Forecast CORE ---
    "Forecast Price (1M)": "forecast_price_1m",
    "Expected ROI % (1M)": "expected_roi_1m",
    "Forecast Price (3M)": "forecast_price_3m",
    "Expected ROI % (3M)": "expected_roi_3m",
    "Forecast Price (12M)": "forecast_price_12m",
    "Expected ROI % (12M)": "expected_roi_12m",
    "Forecast Confidence": "forecast_confidence",
    "Forecast Updated (UTC)": "forecast_updated_utc",
    "Forecast Updated (Riyadh)": "forecast_updated_riyadh",

    # --- Fund specific ---
    "Fund Type": "fund_type",
    "Fund Category": "fund_category",
    "Inception Date": "inception_date",
    "Expense Ratio": "expense_ratio",
    "AUM": "aum",
    "Distribution Yield": "distribution_yield",

    # --- Commodities/FX ---
    "Asset Class": "asset_class",
    "Sub Class": "sub_class",

    # --- Portfolio inputs / KPIs ---
    "Asset Type": "asset_type",
    "Portfolio Group": "portfolio_group",
    "Broker/Account": "broker_account",
    "Quantity": "quantity",
    "Avg Cost": "avg_cost",
    "Cost Value": "cost_value",
    "Target Weight %": "target_weight",
    "Notes": "notes",
    "Market Value": "market_value",
    "Unrealized P/L": "unrealized_pl",
    "Unrealized P/L %": "unrealized_pl_percent",
    "Weight %": "weight_percent",
    "Rebalance Δ": "rebalance_delta",

    # --- Meta ---
    "Error": "error",
    "Recommendation": "recommendation",
    "Data Source": "data_source",
    "Data Quality": "data_quality",
    "Last Updated (UTC)": "last_updated_utc",
    "Last Updated (Riyadh)": "last_updated_riyadh",

    # --- Legacy labels still supported ---
    "Company Name": "name",
    "Sub-Sector": "sub_sector",
    "Last Price": "current_price",
    "Previous Close": "previous_close",
    "Price Change": "price_change",
    "Percent Change": "percent_change",
    "Avg Volume (30D)": "avg_volume_30d",
    "Free Float Market Cap": "free_float_market_cap",

    # compat non-% variants
    "Dividend Yield": "dividend_yield",
    "Payout Ratio": "payout_ratio",
    "ROE": "roe",
    "ROA": "roa",
    "Net Margin": "net_margin",
    "EBITDA Margin": "ebitda_margin",
    "Revenue Growth": "revenue_growth",
    "Net Income Growth": "net_income_growth",
    "Volatility 30D": "volatility_30d",
    "RSI 14": "rsi_14",

    # ROI/Target legacy variants
    "Expected Return 1M %": "expected_roi_1m",
    "Expected Return 3M %": "expected_roi_3m",
    "Expected Return 12M %": "expected_roi_12m",
    "Expected Price 1M": "forecast_price_1m",
    "Expected Price 3M": "forecast_price_3m",
    "Expected Price 12M": "forecast_price_12m",
    "Confidence Score": "forecast_confidence",
}

HEADER_FIELD_CANDIDATES: Dict[str, Tuple[str, ...]] = {}
for h, f in HEADER_TO_FIELD.items():
    canon = canonical_field(f)
    aliases = FIELD_ALIASES.get(canon, ())
    HEADER_FIELD_CANDIDATES[h] = (canon,) + tuple(a for a in aliases if a)

# Prefer these headers if multiple could map to same field
_PREFERRED_HEADERS: Tuple[str, ...] = (
    "Forecast Price (1M)",
    "Expected ROI % (1M)",
    "Forecast Price (3M)",
    "Expected ROI % (3M)",
    "Forecast Price (12M)",
    "Expected ROI % (12M)",
    "Forecast Confidence",
    "Forecast Updated (UTC)",
    "Forecast Updated (Riyadh)",
)

_FIELD_TO_HEADER: Dict[str, str] = {}
for header, field in HEADER_TO_FIELD.items():
    canon = canonical_field(field)
    if (canon not in _FIELD_TO_HEADER) or (header in _PREFERRED_HEADERS):
        _FIELD_TO_HEADER[canon] = header
    for a in FIELD_ALIASES.get(canon, ()):
        if (a not in _FIELD_TO_HEADER) or (header in _PREFERRED_HEADERS):
            _FIELD_TO_HEADER[a] = header

FIELD_TO_HEADER: Dict[str, str] = dict(_FIELD_TO_HEADER)


def header_to_field(header: str) -> str:
    """
    Best-effort header (any variant) -> canonical field name.
    Returns "" when header is unknown (safer for callers).
    """
    h = _canonical_header_label(header)
    if not h:
        return ""
    f = HEADER_TO_FIELD.get(h)
    return canonical_field(f) if f else ""


def header_field_candidates(header: str) -> Tuple[str, ...]:
    """
    Robust mapping helper:
    returns preferred + aliases (e.g. ("week_52_high","high_52w","52w_high")).
    Tolerant to header variants.
    """
    h = _canonical_header_label(str(header or "").strip())
    if not h:
        return ()
    c = HEADER_FIELD_CANDIDATES.get(h)
    return c or ()


def field_to_header(field: str) -> str:
    """Best-effort field (canonical or alias) -> header label."""
    f = str(field or "").strip()
    if not f:
        return ""
    return FIELD_TO_HEADER.get(f, FIELD_TO_HEADER.get(canonical_field(f), f))


# =============================================================================
# Sheet name normalization + registry (legacy + vNext)
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
    # replace common separators
    for ch in ["-", " ", ".", "/", "\\", "|", ":", ";", ",", "&"]:
        s = s.replace(ch, "_")
    s = s.replace("(", "_").replace(")", "_")
    # collapse
    while "__" in s:
        s = s.replace("__", "_")
    return s.strip("_")


_SHEET_HEADERS_LEGACY: Dict[str, Tuple[str, ...]] = {}
_SHEET_HEADERS_VNEXT: Dict[str, Tuple[str, ...]] = {}


def _register(reg: Dict[str, Tuple[str, ...]], keys: List[str], headers: Tuple[str, ...]) -> None:
    for k in keys:
        kk = _norm_sheet_name(k)
        if kk:
            reg[kk] = headers


# ----- Legacy mapping (kept) -----
_register(
    _SHEET_HEADERS_LEGACY,
    keys=[
        "KSA_Tadawul",
        "ksa_tadawul",
        "tadawul",
        "ksa",
        "Market_Leaders",
        "market_leaders",
        "Mutual_Funds",
        "mutual_funds",
        "Commodities_FX",
        "commodities_fx",
        "My_Portfolio",
        "my_portfolio",
        "Global_Markets",
        "global_markets",
        "Insights_Analysis",
        "investment_advisor",
        "advisor",
    ],
    headers=_DEFAULT_59_TUPLE,
)
_register(
    _SHEET_HEADERS_LEGACY,
    keys=["Global_Markets", "Insights_Analysis", "Investment_Advisor", "advisor", "insights"],
    headers=_DEFAULT_ANALYSIS_TUPLE,
)

# ----- vNext mapping (custom per page) -----
_register(
    _SHEET_HEADERS_VNEXT,
    keys=[
        "KSA_Tadawul",
        "KSA Tadawul",
        "ksa_tadawul",
        "ksa_tadawul_market",
        "tadawul",
        "ksa",
        "tasi",
        "saudi",
    ],
    headers=_VN_KSA_T,
)
_register(
    _SHEET_HEADERS_VNEXT,
    keys=[
        "Market_Leaders",
        "Market Leaders",
        "market_leaders",
        "ksa_market_leaders",
        "leaders",
    ],
    headers=_VN_ML_T,
)
_register(
    _SHEET_HEADERS_VNEXT,
    keys=[
        "Global_Markets",
        "Global Markets",
        "global_markets",
        "global",
        "world",
    ],
    headers=_VN_GLOBAL_T,
)
_register(
    _SHEET_HEADERS_VNEXT,
    keys=[
        "Mutual_Funds",
        "Mutual Funds",
        "mutual_funds",
        "funds",
        "etf",
        "etfs",
    ],
    headers=_VN_FUNDS_T,
)
_register(
    _SHEET_HEADERS_VNEXT,
    keys=[
        "Commodities_FX",
        "Commodities & FX",
        "commodities_fx",
        "commodities",
        "fx",
        "forex",
        "commodities_and_fx",
    ],
    headers=_VN_COMFX_T,
)
_register(
    _SHEET_HEADERS_VNEXT,
    keys=[
        "My_Portfolio",
        "My Portfolio",
        "my_portfolio",
        "portfolio",
        "holdings",
        "positions",
    ],
    headers=_VN_PORTFOLIO_T,
)
_register(
    _SHEET_HEADERS_VNEXT,
    keys=[
        "Insights_Analysis",
        "Insights Analysis",
        "insights_analysis",
        "insights",
        "analysis",
        "advisor_output",
    ],
    headers=_VN_INSIGHTS_T,
)


def resolve_sheet_key(sheet_name: Optional[str]) -> str:
    """Returns the normalized key used for lookups."""
    return _norm_sheet_name(sheet_name)


# =============================================================================
# Settings resolution (ENV + optional config)
# =============================================================================
_TRUTHY = {"1", "true", "yes", "y", "on", "t"}


def _env_bool(name: str, default: bool) -> bool:
    v = os.getenv(name)
    if v is None:
        return default
    return str(v).strip().lower() in _TRUTHY


def _env_str(name: str, default: str) -> str:
    v = os.getenv(name)
    return str(v).strip() if v is not None and str(v).strip() else default


def _get_schema_mode_from_settings() -> Tuple[bool, str]:
    """
    Read schema settings (import-safe).
    Order:
      1) ENV overrides
      2) core.config.get_settings() if exists
      3) defaults
    Returns: (schemas_enabled, schema_version)
    """
    enabled_default = True
    version_default = "vNext"

    # ENV overrides (strongest)
    enabled = _env_bool("SHEET_SCHEMAS_ENABLED", enabled_default)
    version = _env_str("SHEET_SCHEMA_VERSION", "")

    # Optional config fallback
    if not version:
        try:
            from core.config import get_settings  # type: ignore

            s = get_settings()
            enabled = bool(getattr(s, "sheet_schemas_enabled", enabled))
            version = str(getattr(s, "sheet_schema_version", version_default) or version_default)
        except Exception:
            version = version_default

    version = (version or version_default).strip()
    return enabled, version


def _pick_registry(schema_version: Optional[str]) -> Dict[str, Tuple[str, ...]]:
    enabled, ver = _get_schema_mode_from_settings()
    v = (schema_version or ver or "vNext").strip().lower()

    # If disabled: keep legacy behavior
    if not enabled:
        return _SHEET_HEADERS_LEGACY

    # Explicit legacy switch
    if v in {"legacy", "v3", "3.9.0", "3.8.2", "3.8.1", "3.8", "3.6.2", "3.6", "3.5.0", "3.5", "3.0"}:
        return _SHEET_HEADERS_LEGACY

    # Default: vNext customized schemas
    return _SHEET_HEADERS_VNEXT


def get_headers_for_sheet(sheet_name: Optional[str] = None, schema_version: Optional[str] = None) -> List[str]:
    """
    Returns a safe headers list for the given sheet.
    - Always returns a list (never raises).
    - Returns a COPY to prevent accidental mutation by callers.
    - Uses vNext customized schemas by default (unless settings force legacy).
    """
    try:
        key = _norm_sheet_name(sheet_name)
        reg = _pick_registry(schema_version)

        if not key:
            # sensible defaults
            if reg is _SHEET_HEADERS_VNEXT:
                return list(_VN_KSA_T)
            return list(_DEFAULT_59_TUPLE)

        v = reg.get(key)
        if isinstance(v, tuple) and v:
            return list(v)

        # Fuzzy match (prefix/contains)
        for k, vv in reg.items():
            if key == k or key.startswith(k) or (k and k in key):
                return list(vv)

        # last resort default
        if reg is _SHEET_HEADERS_VNEXT:
            return list(_VN_KSA_T)
        return list(_DEFAULT_59_TUPLE)

    except Exception:
        return list(_DEFAULT_59_TUPLE)


def get_supported_sheets(schema_version: Optional[str] = None) -> List[str]:
    """Useful for debugging / UI lists."""
    try:
        reg = _pick_registry(schema_version)
        return sorted(list(reg.keys()))
    except Exception:
        return []


def get_header_groups() -> Dict[str, List[str]]:
    """
    Category-wise cross-check (vNext).
    Always returns a safe dict of lists.
    """
    try:
        return {k: list(v) for k, v in _HEADER_GROUPS_VNEXT.items()}
    except Exception:
        return {}


# =============================================================================
# New safe helpers (optional; do not break old code)
# =============================================================================
def canonicalize_headers(headers: Any) -> List[str]:
    """
    Converts a headers list to canonical labels using the synonym engine.
    Never raises. Returns [] if headers is not list-like.
    """
    try:
        if not isinstance(headers, (list, tuple)):
            return []
        out: List[str] = []
        for h in headers:
            out.append(_canonical_header_label(str(h)))
        return out
    except Exception:
        return []


def build_header_index(headers: Sequence[str]) -> Dict[str, int]:
    """
    Returns a tolerant index map for headers:
      - keys include both canonical label normalized forms and raw normalized forms
      - values are the column index
    Useful for fast row mapping.
    Never raises.
    """
    mp: Dict[str, int] = {}
    try:
        for i, h in enumerate(list(headers or [])):
            hh = str(h or "")
            mp[_norm_header_label(hh)] = i
            mp[_norm_header_label(_canonical_header_label(hh))] = i
        return mp
    except Exception:
        return {}


def validate_sheet_headers(
    headers: Any,
    sheet_name: Optional[str],
    *,
    schema_version: Optional[str] = None,
    strict_order: bool = True,
) -> Dict[str, Any]:
    """
    Validates that headers match the expected schema for the given sheet.
    Never raises.

    strict_order=True  -> exact order match required
    strict_order=False -> just checks membership + length
    """
    try:
        expected = get_headers_for_sheet(sheet_name, schema_version=schema_version)
        got = canonicalize_headers(headers)

        if not expected:
            return {"ok": False, "reason": "expected_empty", "expected_len": 0, "got_len": len(got)}

        if not got:
            return {"ok": False, "reason": "got_empty", "expected_len": len(expected), "got_len": 0}

        if strict_order:
            ok = (got == expected)
        else:
            ok = (len(got) == len(expected) and set(got) == set(expected))

        # small diff summary
        exp_set = set(expected)
        got_set = set(got)
        missing = [h for h in expected if h not in got_set]
        extra = [h for h in got if h not in exp_set]

        return {
            "ok": bool(ok),
            "strict_order": bool(strict_order),
            "expected_len": len(expected),
            "got_len": len(got),
            "missing_count": len(missing),
            "extra_count": len(extra),
            "missing": missing[:25],
            "extra": extra[:25],
        }
    except Exception as e:
        return {"ok": False, "reason": "error", "error": str(e)}


def get_schema_info(sheet_name: Optional[str] = None, schema_version: Optional[str] = None) -> Dict[str, Any]:
    """
    Returns debug-safe info describing which schema will be used and why.
    Never raises.
    """
    try:
        enabled, ver = _get_schema_mode_from_settings()
        reg = _pick_registry(schema_version)
        key = _norm_sheet_name(sheet_name)
        headers = get_headers_for_sheet(sheet_name, schema_version=schema_version)
        mode = "vNext" if reg is _SHEET_HEADERS_VNEXT else "legacy"
        return {
            "schemas_version": SCHEMAS_VERSION,
            "schemas_enabled": bool(enabled),
            "requested_schema_version": (schema_version or None),
            "resolved_schema_version": ver,
            "mode": mode,
            "sheet_name": sheet_name or "",
            "sheet_key": key,
            "headers_len": len(headers),
            "headers_preview": headers[:12],
            "supported_sheets_count": len(reg),
        }
    except Exception:
        return {"schemas_version": SCHEMAS_VERSION, "ok": False}


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
        """Returns combined symbols (symbols + tickers), trimmed, in original order."""
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
    # Legacy exports (kept)
    "DEFAULT_HEADERS_59",
    "DEFAULT_HEADERS_ANALYSIS",
    "is_canonical_headers",
    "coerce_headers_59",
    "validate_headers_59",
    # vNext
    "VNEXT_SCHEMAS",
    # Mapping exports
    "FIELD_ALIASES",
    "canonical_field",
    "HEADER_TO_FIELD",
    "HEADER_FIELD_CANDIDATES",
    "FIELD_TO_HEADER",
    "header_to_field",
    "header_field_candidates",
    "field_to_header",
    # Schemas helpers
    "resolve_sheet_key",
    "get_headers_for_sheet",
    "get_supported_sheets",
    "get_header_groups",
    # New helpers (safe)
    "canonicalize_headers",
    "build_header_index",
    "validate_sheet_headers",
    "get_schema_info",
    # Request models
    "BatchProcessRequest",
]
