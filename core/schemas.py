"""
core/schemas.py
=================================================
Core Schemas + Google Sheets Header Templates – v4.0.1

Goals
- Keep a stable, dashboard-friendly header schema (59 columns).
- Provide lightweight Pydantic models used across routes/services.
- Keep `get_headers_for_sheet(sheet_name)` signature (required by routes).
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from pydantic import BaseModel, Field, ConfigDict


# =============================================================================
# Base
# =============================================================================

class BaseSchema(BaseModel):
    model_config = ConfigDict(
        populate_by_name=True,
        from_attributes=True,
        validate_assignment=True,
        extra="ignore",
    )


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


# =============================================================================
# Common Request / Response Models
# =============================================================================

class TickerRequest(BaseSchema):
    ticker: str = Field(..., description="The stock symbol (e.g., 1120.SR, AAPL)")
    provider: Optional[str] = Field(
        default=None,
        description="Optional provider hint (engine still auto-routes).",
    )


class MarketData(BaseSchema):
    symbol: str
    price: Optional[float] = None
    currency: Optional[str] = None
    change: Optional[float] = None
    change_percent: Optional[float] = None
    volume: Optional[float] = None
    timestamp_utc: datetime = Field(default_factory=utc_now)


class AIAnalysisResponse(BaseSchema):
    symbol: str
    recommendation: str = Field(..., description="STRONG BUY, BUY, HOLD, SELL")
    confidence_score: float = Field(..., ge=0, le=100)
    reasoning: str
    generated_at_utc: datetime = Field(default_factory=utc_now)


class ScoredQuote(BaseSchema):
    symbol: str
    market_data: MarketData
    value_score: Optional[float] = None
    quality_score: Optional[float] = None
    momentum_score: Optional[float] = None
    opportunity_score: Optional[float] = None
    overall_score: Optional[float] = None
    recommendation: Optional[str] = None
    confidence: Optional[float] = None


class BatchProcessRequest(BaseSchema):
    symbols: List[str] = Field(default_factory=list)
    operation: str = Field(default="full_scan", description="full_scan, quick_price, ai_analysis")
    sheet_name: Optional[str] = Field(default=None, description="Optional sheet name for header selection")


# =============================================================================
# Google Sheets Headers (59 columns)
# =============================================================================
# NOTE:
# - Keep this stable to avoid breaking Apps Script / dashboard formatting.
# - Use "Error" instead of "Rank" (more useful for debugging + data QA).

DEFAULT_HEADERS_59: List[str] = [
    "Symbol",
    "Company Name",
    "Sector",
    "Sub-Sector",
    "Market",
    "Currency",
    "Listing Date",
    "Last Price",
    "Previous Close",
    "Price Change",
    "Percent Change",
    "Day High",
    "Day Low",
    "52W High",
    "52W Low",
    "52W Position %",
    "Volume",
    "Avg Volume (30D)",
    "Value Traded",
    "Turnover %",
    "Shares Outstanding",
    "Free Float %",
    "Market Cap",
    "Free Float Market Cap",
    "Liquidity Score",
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
    "Volatility (30D)",
    "RSI (14)",
    "Fair Value",
    "Upside %",
    "Valuation Label",
    "Value Score",
    "Quality Score",
    "Momentum Score",
    "Opportunity Score",
    "Error",
    "Recommendation",
    "Data Source",
    "Data Quality",
    "Last Updated (UTC)",
    "Last Updated (Riyadh)",
]

# Sheet-specific variants (currently all share the same canonical template).
SHEET_HEADERS: Dict[str, List[str]] = {
    "KSA_Tadawul": DEFAULT_HEADERS_59,
    "KSA_Tadawul_Market": DEFAULT_HEADERS_59,
    "Global_Markets": DEFAULT_HEADERS_59,
    "Mutual_Funds": DEFAULT_HEADERS_59,
    "Commodities_FX": DEFAULT_HEADERS_59,
    "My_Portfolio": DEFAULT_HEADERS_59,
    "My_Portfolio_Investment": DEFAULT_HEADERS_59,
    "Market_Leaders": DEFAULT_HEADERS_59,
}


def get_headers_for_sheet(sheet_name: str | None = None) -> List[str]:
    """
    Returns the header list to be used for a given sheet.

    Required by:
    - routes_argaam.py
    - enriched_quote.py

    Unknown sheet names fall back to the canonical 59-column template.
    Returned list is a COPY (safe for callers to mutate).
    """
    if not sheet_name:
        return list(DEFAULT_HEADERS_59)

    key = str(sheet_name).strip()
    return list(SHEET_HEADERS.get(key, DEFAULT_HEADERS_59))


# Backward-compat alias (some older code may import this name)
get_sheet_headers = get_headers_for_sheet
