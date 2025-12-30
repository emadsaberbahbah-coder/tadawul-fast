# core/schemas.py  (FULL REPLACEMENT)
"""
core/schemas.py
===========================================================
CANONICAL SHEET SCHEMAS + HEADERS — v3.3.3 (PROD SAFE)

Purpose
- Single source of truth for the canonical 59-column quote schema.
- Provide get_headers_for_sheet(sheet_name) used by:
    - routes/enriched_quote.py
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
✅ Convenience: provides HEADER<->FIELD mapping for UnifiedQuote-style payloads.

v3.3.3 changes vs v3.3.1
- ✅ Adds explicit alias keys for your exact sheet names (Market_Leaders, My_Portfolio, etc.)
- ✅ Stronger sheet normalization (handles dots, slashes, parentheses)
- ✅ Adds strict canonical header validation helper + debug-safe helpers
- ✅ BatchProcessRequest: supports symbols/tickers as list OR mixed-separator string
"""

from __future__ import annotations

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


SCHEMAS_VERSION = "3.3.3"

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


# =============================================================================
# Header <-> Field mapping (UnifiedQuote alignment helper)
# =============================================================================

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
    "52W High": "high_52w",
    "52W Low": "low_52w",
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

FIELD_TO_HEADER: Dict[str, str] = {v: k for k, v in HEADER_TO_FIELD.items()}


def header_to_field(header: str) -> str:
    """Best-effort header -> UnifiedQuote field name."""
    return HEADER_TO_FIELD.get(str(header or "").strip(), str(header or "").strip())


def field_to_header(field: str) -> str:
    """Best-effort UnifiedQuote field -> header label."""
    return FIELD_TO_HEADER.get(str(field or "").strip(), str(field or "").strip())


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
    # unify separators
    for ch in ["-", " ", ".", "/", "\\", "|", ":", ";", ","]:
        s = s.replace(ch, "_")
    # remove parentheses but keep content
    s = s.replace("(", "_").replace(")", "_")
    # collapse repeats
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

        # direct match
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

    # single string: support mixed separators
    s = str(v).replace("\n", " ").replace("\t", " ").replace(",", " ").strip()
    if not s:
        return []
    parts = [p.strip() for p in s.split(" ") if p.strip()]
    return parts


class _ExtraIgnore(BaseModel):
    if _PYDANTIC_V2:
        model_config = ConfigDict(extra="ignore")  # type: ignore
    else:  # pragma: no cover
        class Config:
            extra = "ignore"


class BatchProcessRequest(_ExtraIgnore):
    """
    Shared contract used by routes/enriched_quote.py (and reusable elsewhere).
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
    "HEADER_TO_FIELD",
    "FIELD_TO_HEADER",
    "header_to_field",
    "field_to_header",
    "resolve_sheet_key",
    "get_headers_for_sheet",
    "get_supported_sheets",
    "BatchProcessRequest",
]
