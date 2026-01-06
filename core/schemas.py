# core/schemas.py  — FULL REPLACEMENT — v3.6.2
"""
core/schemas.py
===========================================================
CANONICAL SHEET SCHEMAS + HEADERS — v3.6.2 (PROD SAFE)

What changed in v3.6.2 (alignment release)
- ✅ vNext headers aligned to routes/ai_analysis.py expectations:
    • Percent columns explicitly include "%" (Dividend Yield %, ROE %, etc.)
    • Technicals use legacy-style labels for safer transforms: Volatility (30D), RSI (14)
    • Forecast block uses "Expected Return" + "Expected Price" (keeps ROI/Target as synonyms)
- ✅ Keeps backwards compatibility:
    • All prior v3.6.1 headers remain recognized via tolerant header normalization + synonyms
    • Legacy 59 + legacy Analysis headers preserved as-is
- ✅ Still import-safe: no DataEngine imports, no network, no heavy deps.
- ✅ Defensive: get_headers_for_sheet() never raises and always returns COPIES.
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


SCHEMAS_VERSION = "3.6.2"

# =============================================================================
# LEGACY: Canonical 59-column schema (kept for backward compatibility)
# =============================================================================
# NOTE: Do not change order lightly. Many older routes/scripts assume this.
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

# Legacy "analysis extras" (old names kept)
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

# Normalize exported lists to canonical (if someone edited accidentally)
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

# --- Common blocks (vNext labels aligned to routes/ai_analysis.py transforms) ---
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

# IMPORTANT: percent fields include "%" to allow robust ratio->percent transforms in routes.
_VN_FUNDAMENTALS: List[str] = [
    "EPS (TTM)",
    "Forward EPS",
    "P/E (TTM)",
    "Forward P/E",
    "P/B",
    "P/S",
    "EV/EBITDA",
    "Dividend Yield %",   # was "Dividend Yield"
    "Dividend Rate",
    "Payout Ratio %",     # was "Payout Ratio"
    "ROE %",              # was "ROE"
    "ROA %",              # was "ROA"
    "Net Margin %",       # was "Net Margin"
    "EBITDA Margin %",    # was "EBITDA Margin"
    "Revenue Growth %",   # was "Revenue Growth"
    "Net Income Growth %",# was "Net Income Growth"
    "Beta",
]

# Align technical labels with legacy-safe keys used across routes.
_VN_TECHNICALS: List[str] = [
    "Volatility (30D)",   # was "Volatility 30D"
    "RSI (14)",           # was "RSI 14"
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

# Forecasting / history / targets (use Expected Return / Expected Price to match analysis routes)
_VN_FORECAST: List[str] = [
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

_VN_META: List[str] = [
    "Error",
    "Recommendation",
    "Data Source",
    "Data Quality",
    "Last Updated (UTC)",
    "Last Updated (Riyadh)",
]

# --- Page Schemas (vNext) ---
# Market Leaders + KSA Tadawul: full equity superset (scores + badges)
_VN_HEADERS_EQUITY_FULL: List[str] = (
    _VN_IDENTITY
    + _VN_PRICE
    + _VN_LIQUIDITY
    + _VN_CAP
    + _VN_FUNDAMENTALS
    + _VN_TECHNICALS
    + _VN_VALUATION
    + _VN_SCORES
    + _VN_BADGES
    + _VN_META
)

# Global markets: equity superset + forecast/history/targets
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
    + _VN_FORECAST
    + _VN_META
)

# Mutual funds: simplified + fund attributes + forecast
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
        "Distribution Yield",  # keep as-is (providers vary)
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
    + _VN_FORECAST
    + _VN_META
)

# Commodities & FX: stripped fundamentals + forecast
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
    + _VN_FORECAST
    + _VN_META
)

# My Portfolio: inputs + market data + portfolio KPIs + forecast
_VN_HEADERS_PORTFOLIO: List[str] = (
    [
        # Identity / grouping
        "Rank",
        "Symbol",
        "Origin",
        "Name",
        "Market",
        "Currency",
        "Asset Type",          # Stock / ETF / Fund / Commodity / FX
        "Portfolio Group",     # user-defined (e.g., Core / Growth / Income)
        "Broker/Account",      # user-defined
    ]
    + [
        # User inputs (Sheet-entered)
        "Quantity",
        "Avg Cost",
        "Cost Value",
        "Target Weight %",
        "Notes",
    ]
    + [
        # Market snapshot (API)
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
        # Position metrics (can be sheet formulas OR API if you later implement)
        "Market Value",
        "Unrealized P/L",
        "Unrealized P/L %",
        "Weight %",
        "Rebalance Δ",
    ]
    + [
        # Analytics / forward-looking
        "Fair Value",
        "Upside %",
        "Valuation Label",
        "Expected Return 1M %",
        "Expected Return 3M %",
        "Expected Return 12M %",
        "Expected Price 1M",
        "Expected Price 3M",
        "Expected Price 12M",
        "Confidence Score",
        "Recommendation",
        "Rec Badge",
    ]
    + [
        # Quality/safety indicators
        "Risk Score",
        "Overall Score",
        "Volatility (30D)",
        "RSI (14)",
    ]
    + [
        # Provenance / meta
        "Error",
        "Data Source",
        "Data Quality",
        "Last Updated (UTC)",
        "Last Updated (Riyadh)",
    ]
)

# Freeze vNext schemas as tuples (prevent accidental mutation)
_VN_EQUITY_FULL_T: Tuple[str, ...] = tuple(_VN_HEADERS_EQUITY_FULL)
_VN_GLOBAL_T: Tuple[str, ...] = tuple(_VN_HEADERS_GLOBAL)
_VN_FUNDS_T: Tuple[str, ...] = tuple(_VN_HEADERS_FUNDS)
_VN_COMFX_T: Tuple[str, ...] = tuple(_VN_HEADERS_COMMODITIES_FX)
_VN_PORTFOLIO_T: Tuple[str, ...] = tuple(_VN_HEADERS_PORTFOLIO)


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
    "Forecast": tuple(_VN_FORECAST),
    "Meta": tuple(_VN_META),
}


# =============================================================================
# Header normalization (tolerant mapping)
# =============================================================================
def _norm_header_label(h: Optional[str]) -> str:
    """
    Normalize header labels for tolerant lookups.
    Example:
      "Avg Vol 30D" -> "avg_vol_30d"
      "Sub Sector" -> "sub_sector"
      "Last Updated (Riyadh)" -> "last_updated_riyadh"
    """
    s = str(h or "").strip().lower()
    if not s:
        return ""
    s = s.replace("%", " percent ")
    s = re.sub(r"[()\[\]{}]", " ", s)
    s = re.sub(r"[^a-z0-9]+", "_", s)
    s = re.sub(r"_+", "_", s).strip("_")
    return s


# Build canonical header lookup by normalized key
_HEADER_CANON_BY_NORM: Dict[str, str] = {}

# Include legacy + vNext headers in canon map
for _h in (
    list(_DEFAULT_59_TUPLE)
    + list(_DEFAULT_ANALYSIS_TUPLE)
    + list(_VN_GLOBAL_T)
    + list(_VN_EQUITY_FULL_T)
    + list(_VN_FUNDS_T)
    + list(_VN_COMFX_T)
    + list(_VN_PORTFOLIO_T)
):
    _HEADER_CANON_BY_NORM[_norm_header_label(_h)] = str(_h)

# Common synonyms/variants
_HEADER_SYNONYMS: Dict[str, str] = {
    # legacy vs vNext name alignment
    "company_name": "Name",
    "company": "Name",
    "sub_sector": "Sub Sector",
    "subsector": "Sub Sector",

    "last_price": "Price",
    "previous_close": "Prev Close",
    "price_change": "Change",
    "percent_change": "Change %",

    "avg_volume_30d": "Avg Vol 30D",
    "avg_vol_30d": "Avg Vol 30D",

    "free_float_market_cap": "Free Float Mkt Cap",
    "ff_market_cap": "Free Float Mkt Cap",

    # technical variants
    "volatility_30d": "Volatility (30D)",
    "volatility30d": "Volatility (30D)",
    "rsi_14": "RSI (14)",

    # percent label variants (accept non-% versions)
    "dividend_yield": "Dividend Yield %",
    "dividend_yield_percent": "Dividend Yield %",
    "dividend_yield_pct": "Dividend Yield %",

    "payout_ratio": "Payout Ratio %",
    "roe": "ROE %",
    "roa": "ROA %",
    "net_margin": "Net Margin %",
    "ebitda_margin": "EBITDA Margin %",
    "revenue_growth": "Revenue Growth %",
    "net_income_growth": "Net Income Growth %",

    # timestamps
    "last_updated_utc": "Last Updated (UTC)",
    "last_updated_riyadh": "Last Updated (Riyadh)",
    "last_updated_ksa": "Last Updated (Riyadh)",

    # portfolio header variants
    "broker_account": "Broker/Account",
    "portfolio_group": "Portfolio Group",
    "asset_type": "Asset Type",

    # forecast aliases (support ROI/Target labels too)
    "expected_roi_1m_percent": "Expected Return 1M %",
    "expected_roi_3m_percent": "Expected Return 3M %",
    "expected_roi_12m_percent": "Expected Return 12M %",
    "expected_return_1m_percent": "Expected Return 1M %",
    "expected_return_3m_percent": "Expected Return 3M %",
    "expected_return_12m_percent": "Expected Return 12M %",
    "target_price_1m": "Expected Price 1M",
    "target_price_3m": "Expected Price 3M",
    "target_price_12m": "Expected Price 12M",
}
for k, canon_header in list(_HEADER_SYNONYMS.items()):
    nk = _norm_header_label(k)
    if nk and canon_header:
        _HEADER_CANON_BY_NORM[nk] = canon_header


def _canonical_header_label(header: str) -> str:
    """Best-effort variant header -> canonical header label. Never raises."""
    h = str(header or "").strip()
    if not h:
        return ""
    nh = _norm_header_label(h)
    return _HEADER_CANON_BY_NORM.get(nh, h)


# =============================================================================
# Field aliases (engine/provider variations)
# =============================================================================
FIELD_ALIASES: Dict[str, Tuple[str, ...]] = {
    # Identity
    "rank": ("row_rank", "position", "rank_num"),
    "symbol": ("symbol_normalized", "symbol_input", "ticker", "code"),
    "origin": ("page_key", "sheet_key", "source_page"),
    "name": ("company_name", "long_name"),
    "sector": ("industry",),
    "sub_sector": ("subsector",),
    "market": ("market_region", "exchange", "listing_exchange"),
    "currency": ("ccy",),
    "listing_date": ("ipo_date", "listed_at"),

    # Prices
    "current_price": ("last_price", "price", "close", "last"),
    "previous_close": ("prev_close",),
    "price_change": ("change",),
    "percent_change": ("change_percent", "change_pct", "pct_change"),
    "day_high": ("high",),
    "day_low": ("low",),

    # 52W
    "week_52_high": ("high_52w", "52w_high"),
    "week_52_low": ("low_52w", "52w_low"),
    "position_52w_percent": ("position_52w", "pos_52w_pct"),

    # Liquidity / Shares
    "volume": ("vol",),
    "avg_volume_30d": ("avg_volume", "avg_vol_30d", "avg_volume_30day"),
    "value_traded": ("traded_value",),
    "turnover_percent": ("turnover", "turnover_pct"),
    "shares_outstanding": ("shares", "outstanding_shares"),
    "free_float": ("free_float_percent", "free_float_pct"),
    "market_cap": ("mkt_cap", "marketcapitalization"),
    "free_float_market_cap": ("ff_market_cap",),
    "liquidity_score": ("liq_score",),

    # Fundamentals
    "eps_ttm": ("eps",),
    "forward_eps": ("eps_forward",),
    "pe_ttm": ("pe",),
    "forward_pe": ("pe_forward",),
    "pb": ("p_b",),
    "ps": ("p_s",),
    "ev_ebitda": ("evebitda",),
    "dividend_yield": ("div_yield", "dividend_yield_pct", "dividend_yield_percent"),
    "dividend_rate": ("div_rate",),
    "payout_ratio": ("payout", "payout_pct"),
    "roe": ("return_on_equity",),
    "roa": ("return_on_assets",),
    "net_margin": ("profit_margin",),
    "ebitda_margin": ("margin_ebitda",),
    "revenue_growth": ("rev_growth",),
    "net_income_growth": ("ni_growth",),
    "beta": ("beta_5y",),

    # Technicals
    "volatility_30d": ("vol_30d_ann", "vol30d", "vol_30d"),
    "rsi_14": ("rsi14",),

    # Valuation / targets
    "fair_value": ("intrinsic_value",),
    "upside_percent": ("upside_pct",),
    "valuation_label": ("valuation",),

    # Scores / recommendation
    "value_score": ("score_value",),
    "quality_score": ("score_quality",),
    "momentum_score": ("score_momentum",),
    "opportunity_score": ("score_opportunity",),
    "risk_score": ("score_risk",),
    "overall_score": ("score", "total_score"),
    "rec_badge": ("recommendation_badge",),
    "momentum_badge": ("mom_badge",),
    "opportunity_badge": ("opp_badge",),
    "risk_badge": ("rk_badge",),

    "error": ("err",),
    "recommendation": ("recommend", "action"),

    # Forecast / history
    "returns_1w": ("return_1w", "ret_1w"),
    "returns_1m": ("return_1m", "ret_1m"),
    "returns_3m": ("return_3m", "ret_3m"),
    "returns_6m": ("return_6m", "ret_6m"),
    "returns_12m": ("return_12m", "ret_12m"),
    "ma20": ("sma20",),
    "ma50": ("sma50",),
    "ma200": ("sma200",),
    "expected_return_1m": ("exp_return_1m", "expected_roi_1m"),
    "expected_return_3m": ("exp_return_3m", "expected_roi_3m"),
    "expected_return_12m": ("exp_return_12m", "expected_roi_12m"),
    "expected_price_1m": ("exp_price_1m", "target_price_1m"),
    "expected_price_3m": ("exp_price_3m", "target_price_3m"),
    "expected_price_12m": ("exp_price_12m", "target_price_12m"),
    "confidence_score": ("conf_score",),
    "forecast_method": ("forecast_model",),
    "history_points": ("hist_points",),
    "history_source": ("hist_source",),
    "history_last_utc": ("hist_last_utc",),

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

    # Portfolio (sheet inputs / computed)
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
        _ALIAS_TO_CANON[a] = canon


def canonical_field(field: str) -> str:
    """Best-effort alias -> canonical field. Example: high_52w -> week_52_high"""
    f = str(field or "").strip()
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

    # --- vNext Fundamentals (percent labels explicit) ---
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

    # --- Forecast / history (Expected Return / Expected Price) ---
    "Returns 1W %": "returns_1w",
    "Returns 1M %": "returns_1m",
    "Returns 3M %": "returns_3m",
    "Returns 6M %": "returns_6m",
    "Returns 12M %": "returns_12m",
    "MA20": "ma20",
    "MA50": "ma50",
    "MA200": "ma200",
    "Expected Return 1M %": "expected_return_1m",
    "Expected Return 3M %": "expected_return_3m",
    "Expected Return 12M %": "expected_return_12m",
    "Expected Price 1M": "expected_price_1m",
    "Expected Price 3M": "expected_price_3m",
    "Expected Price 12M": "expected_price_12m",
    "Confidence Score": "confidence_score",
    "Forecast Method": "forecast_method",
    "History Points": "history_points",
    "History Source": "history_source",
    "History Last (UTC)": "history_last_utc",

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

    # --- Legacy labels still supported (do not remove) ---
    "Company Name": "name",
    "Sub-Sector": "sub_sector",
    "Last Price": "current_price",
    "Previous Close": "previous_close",
    "Price Change": "price_change",
    "Percent Change": "percent_change",
    "Avg Volume (30D)": "avg_volume_30d",
    "Free Float Market Cap": "free_float_market_cap",
    "Dividend Yield": "dividend_yield",          # legacy/no-% variant (tolerant)
    "Payout Ratio": "payout_ratio",
    "ROE": "roe",
    "ROA": "roa",
    "Net Margin": "net_margin",
    "EBITDA Margin": "ebitda_margin",
    "Revenue Growth": "revenue_growth",
    "Net Income Growth": "net_income_growth",
    "Volatility 30D": "volatility_30d",          # legacy v3.6.1 variant
    "RSI 14": "rsi_14",                          # legacy v3.6.1 variant

    # ROI/Target legacy variants (tolerant)
    "Expected ROI 1M %": "expected_return_1m",
    "Expected ROI 3M %": "expected_return_3m",
    "Expected ROI 12M %": "expected_return_12m",
    "Target Price 1M": "expected_price_1m",
    "Target Price 3M": "expected_price_3m",
    "Target Price 12M": "expected_price_12m",
}

# Multi-field fallback candidates per header (preferred first, then aliases)
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
    for ch in ["-", " ", ".", "/", "\\", "|", ":", ";", ","]:
        s = s.replace(ch, "_")
    s = s.replace("(", "_").replace(")", "_")
    while "__" in s:
        s = s.replace("__", "_")
    return s.strip("_")


# Internal registries stored as tuples to prevent mutation leaks
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
        "KSA_Tadawul", "ksa_tadawul", "tadawul", "ksa",
        "Market_Leaders", "market_leaders",
        "Mutual_Funds", "mutual_funds",
        "Commodities_FX", "commodities_fx",
        "My_Portfolio", "my_portfolio",
        "Global_Markets", "global_markets",
        "Insights_Analysis", "investment_advisor",
    ],
    headers=_DEFAULT_59_TUPLE,
)
_register(
    _SHEET_HEADERS_LEGACY,
    keys=["Global_Markets", "Insights_Analysis", "Investment_Advisor"],
    headers=_DEFAULT_ANALYSIS_TUPLE,
)

# ----- vNext mapping (custom per page) -----
_register(
    _SHEET_HEADERS_VNEXT,
    keys=[
        "KSA_Tadawul", "KSA Tadawul", "ksa_tadawul", "ksa_tadawul_market", "tadawul", "ksa",
    ],
    headers=_VN_EQUITY_FULL_T,
)
_register(
    _SHEET_HEADERS_VNEXT,
    keys=[
        "Market_Leaders", "Market Leaders", "market_leaders", "ksa_market_leaders",
    ],
    headers=_VN_EQUITY_FULL_T,
)
_register(
    _SHEET_HEADERS_VNEXT,
    keys=[
        "Global_Markets", "Global Markets", "global_markets", "global",
    ],
    headers=_VN_GLOBAL_T,
)
_register(
    _SHEET_HEADERS_VNEXT,
    keys=[
        "Mutual_Funds", "Mutual Funds", "mutual_funds", "funds",
    ],
    headers=_VN_FUNDS_T,
)
_register(
    _SHEET_HEADERS_VNEXT,
    keys=[
        "Commodities_FX", "Commodities & FX", "commodities_fx", "commodities", "fx",
    ],
    headers=_VN_COMFX_T,
)
_register(
    _SHEET_HEADERS_VNEXT,
    keys=[
        "My_Portfolio", "My Portfolio", "my_portfolio", "portfolio", "my_portfolio_investment",
    ],
    headers=_VN_PORTFOLIO_T,
)
# Explicit mapping for Insights_Analysis under vNext
_register(
    _SHEET_HEADERS_VNEXT,
    keys=[
        "Insights_Analysis", "Insights Analysis", "insights_analysis", "insights",
    ],
    headers=_VN_GLOBAL_T,
)


def resolve_sheet_key(sheet_name: Optional[str]) -> str:
    """Returns the normalized key used for lookups."""
    return _norm_sheet_name(sheet_name)


def _get_schema_mode_from_settings() -> Tuple[bool, str]:
    """
    Read schema settings (import-safe).
    Returns: (schemas_enabled, schema_version)
    """
    enabled = True
    version = "vNext"

    try:
        # Import inside function (avoid import-time coupling)
        from core.config import get_settings  # type: ignore

        s = get_settings()
        enabled = bool(getattr(s, "sheet_schemas_enabled", True))
        version = str(getattr(s, "sheet_schema_version", "vNext") or "vNext")
    except Exception:
        pass

    version = (version or "vNext").strip()
    return enabled, version


def _pick_registry(schema_version: Optional[str]) -> Dict[str, Tuple[str, ...]]:
    enabled, ver = _get_schema_mode_from_settings()
    v = (schema_version or ver or "vNext").strip().lower()

    # If disabled: keep legacy behavior
    if not enabled:
        return _SHEET_HEADERS_LEGACY

    # Explicit legacy switch
    if v in {"legacy", "v3", "3.5.0", "3.5", "3.0"}:
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
            # Default fallback: vNext equity full (most common), else legacy 59
            if reg is _SHEET_HEADERS_VNEXT:
                return list(_VN_EQUITY_FULL_T)
            return list(_DEFAULT_59_TUPLE)

        v = reg.get(key)
        if isinstance(v, tuple) and v:
            return list(v)

        # contains / prefix matching (best-effort)
        for k, vv in reg.items():
            if key == k or key.startswith(k) or k in key:
                return list(vv)

        # Final fallback
        if reg is _SHEET_HEADERS_VNEXT:
            return list(_VN_EQUITY_FULL_T)
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
    # Request models
    "BatchProcessRequest",
]
