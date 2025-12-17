# core/schemas.py
"""
core/schemas.py
===========================================================
CANONICAL SHEETS SCHEMAS + HEADERS – v3.1.0 (PROD SAFE)

Purpose
- Single source of truth for sheet headers used by:
    - routes/enriched_quote.py
    - routes/ai_analysis.py
    - routes/advanced_analysis.py
    - Google Apps Script sheet builders
- Provide shared request models (BatchProcessRequest) used by routers.

Key update (v3.1.0)
- Keep DEFAULT_HEADERS_59 unchanged for backward compatibility (global/canonical).
- Introduce a KSA_TADAWUL_HEADERS_52 "future-looking" schema designed to minimize
  missing columns by relying on:
    • current market data (Argaam/Tadawul/etc.)
    • computed metrics (ROI horizons, 52W stats, scores)
    • master data (Company Name/Sector/Industry)
- get_headers_for_sheet(sheet_name) returns the correct list based on sheet.
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional

# Pydantic v2 preferred, v1 fallback
try:
    from pydantic import BaseModel, Field, ConfigDict  # type: ignore
    _PYDANTIC_V2 = True
except Exception:  # pragma: no cover
    from pydantic import BaseModel, Field  # type: ignore
    ConfigDict = None  # type: ignore
    _PYDANTIC_V2 = False


SCHEMAS_VERSION = "3.1.0"


# =============================================================================
# Canonical 59-column schema (SOURCE OF TRUTH – DO NOT BREAK)
# =============================================================================

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

# Hard guard (prevents accidental edits)
if len(DEFAULT_HEADERS_59) != 59:  # pragma: no cover
    raise RuntimeError(f"DEFAULT_HEADERS_59 must be 59 columns, got {len(DEFAULT_HEADERS_59)}")


# =============================================================================
# KSA Tadawul schema (future-looking, minimal-missing)
# =============================================================================
# Notes:
# - Company Name / Sector / Industry: should be filled from your master table.
# - ROI / 52W / Health/Rank/Expected ROI: computed by backend (history + scoring).
# - We intentionally remove rarely-available fields (e.g., Free Float Market Cap,
#   Avg Volume 30D, etc.) unless you later prove stable capture.

KSA_TADAWUL_HEADERS_52: List[str] = [
    # Identity (master-driven, stable)
    "Symbol",
    "Company Name",
    "Sector",
    "Industry",
    "Market",
    "Currency",
    # Current Market (provider-driven, stable)
    "Last Price",
    "Previous Close",
    "Price Change",
    "Percent Change",
    "Open",
    "Day High",
    "Day Low",
    "52W High",
    "52W Low",
    "52W Position %",
    "Volume",
    "Value Traded",
    "Turnover %",
    "Shares Outstanding",
    "Market Cap",
    "Liquidity Score",
    # Trailing Performance (computed from stored history)
    "ROI (3M) %",
    "ROI (12M) %",
    "Volatility (30D)",
    "RSI (14)",
    # Fundamentals / Health (prefer computed + master-friendly)
    "Revenue Growth (YoY) %",
    "Net Margin %",
    "ROE %",
    "Debt/Equity",
    "Financial Health Score",
    "Financial Health Rank (Sector)",
    # Valuation / Forward (computed / derived)
    "Fair Value",
    "Upside %",
    "Target Price (12M)",
    "Expected ROI (1M) %",
    "Expected ROI (3M) %",
    "Expected ROI (12M) %",
    "Expected Price Growth (12M) %",
    # Scores / Recommendation (computed)
    "Value Score",
    "Quality Score",
    "Momentum Score",
    "Opportunity Score",
    "Risk Score",
    "Overall Score",
    "Recommendation",
    "Confidence",
    # Meta (must exist)
    "Data Source",
    "Data Quality",
    "Last Updated (UTC)",
    "Last Updated (Riyadh)",
    "Error",
]

if len(KSA_TADAWUL_HEADERS_52) != 52:  # pragma: no cover
    raise RuntimeError(f"KSA_TADAWUL_HEADERS_52 must be 52 columns, got {len(KSA_TADAWUL_HEADERS_52)}")


# =============================================================================
# Sheet name normalization + mappings
# =============================================================================

def _norm_sheet_name(name: Optional[str]) -> str:
    s = (name or "").strip().lower()
    s = s.replace("-", "_").replace(" ", "_")
    while "__" in s:
        s = s.replace("__", "_")
    return s


# Sheet header mappings (aliases included)
_SHEET_HEADERS: Dict[str, List[str]] = {
    # KSA (use the new optimized schema)
    "ksa_tadawul": KSA_TADAWUL_HEADERS_52,
    "ksa_tadawul_market": KSA_TADAWUL_HEADERS_52,
    "ksa_market": KSA_TADAWUL_HEADERS_52,
    "tadawul": KSA_TADAWUL_HEADERS_52,
    "ksa": KSA_TADAWUL_HEADERS_52,

    # Global / Other pages keep canonical 59 unless explicitly changed later
    "global_markets": DEFAULT_HEADERS_59,
    "global_market": DEFAULT_HEADERS_59,

    "mutual_funds": DEFAULT_HEADERS_59,
    "mutualfunds": DEFAULT_HEADERS_59,

    "commodities_fx": DEFAULT_HEADERS_59,
    "commodities_and_fx": DEFAULT_HEADERS_59,
    "fx": DEFAULT_HEADERS_59,

    "my_portfolio": DEFAULT_HEADERS_59,
    "my_portfolio_investment": DEFAULT_HEADERS_59,
    "portfolio": DEFAULT_HEADERS_59,

    "insights_analysis": DEFAULT_HEADERS_59,
    "analysis": DEFAULT_HEADERS_59,
    "investment_advisor": DEFAULT_HEADERS_59,
}


def get_headers_for_sheet(sheet_name: Optional[str] = None) -> List[str]:
    """
    Returns a safe headers list for the given sheet.
    - Always returns a list (never raises).
    - Returns a COPY to prevent accidental mutation by callers.
    """
    try:
        key = _norm_sheet_name(sheet_name)
        if not key:
            return list(DEFAULT_HEADERS_59)

        # direct match
        if key in _SHEET_HEADERS:
            return list(_SHEET_HEADERS[key])

        # prefix / contains matching (defensive)
        for k, v in _SHEET_HEADERS.items():
            if key == k or key.startswith(k) or k in key:
                return list(v)

        return list(DEFAULT_HEADERS_59)
    except Exception:
        return list(DEFAULT_HEADERS_59)


def get_supported_sheets() -> List[str]:
    """Useful for debugging / UI lists."""
    try:
        return sorted(list(_SHEET_HEADERS.keys()))
    except Exception:
        return []


def get_schema_for_sheet(sheet_name: Optional[str] = None) -> Dict[str, Any]:
    """
    Convenience helper for debugging:
    returns {sheet_name_norm, version, headers, count}.
    """
    h = get_headers_for_sheet(sheet_name)
    return {
        "schema_version": SCHEMAS_VERSION,
        "sheet_name_norm": _norm_sheet_name(sheet_name),
        "count": len(h),
        "headers": h,
    }


# =============================================================================
# Shared request models
# =============================================================================

class _ExtraIgnore(BaseModel):
    if _PYDANTIC_V2:
        model_config = ConfigDict(extra="ignore")  # type: ignore
    else:  # pragma: no cover
        class Config:
            extra = "ignore"


class BatchProcessRequest(_ExtraIgnore):
    """
    Shared contract used by routes/enriched_quote.py (and can be reused elsewhere).
    Supports both `symbols` and `tickers` to be client-robust.
    """
    operation: str = Field(default="refresh")
    sheet_name: Optional[str] = Field(default=None)
    symbols: List[str] = Field(default_factory=list)
    tickers: List[str] = Field(default_factory=list)  # alias support

    def all_symbols(self) -> List[str]:
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
    "KSA_TADAWUL_HEADERS_52",
    "get_headers_for_sheet",
    "get_supported_sheets",
    "get_schema_for_sheet",
    "BatchProcessRequest",
]
