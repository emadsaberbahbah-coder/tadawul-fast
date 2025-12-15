"""
core/enriched_quote.py
===========================================================
EnrichedQuote (UnifiedQuote -> Google Sheets Row) – v2.2.0

Purpose
- Provide a stable conversion layer from core.data_engine_v2.UnifiedQuote
  into:
    1) API-friendly dict (same fields)
    2) Google Sheets row aligned to headers (59-column schema)

Design rules
- Import-safe and production defensive (never crash on missing fields).
- Header-driven row rendering:
    EnrichedQuote.to_row(headers) returns exactly len(headers) columns.
- Uses core.schemas.DEFAULT_HEADERS_59 / get_headers_for_sheet as source of truth.
"""

from __future__ import annotations

import math
from typing import Any, Dict, List, Optional

from pydantic import BaseModel, ConfigDict

from core.data_engine_v2 import UnifiedQuote


def _sf(x: Any) -> Optional[float]:
    """Safe float coercion (never raises)."""
    try:
        if x is None:
            return None
        if isinstance(x, (int, float)):
            f = float(x)
            if math.isnan(f) or math.isinf(f):
                return None
            return f
        s = str(x).strip()
        if not s or s in {"-", "—", "N/A", "NA", "null", "None"}:
            return None
        f = float(s)
        if math.isnan(f) or math.isinf(f):
            return None
        return f
    except Exception:
        return None


def _compute_upside_percent(fair_value: Any, current_price: Any) -> Optional[float]:
    fv = _sf(fair_value)
    cp = _sf(current_price)
    if fv is None or cp is None or cp == 0:
        return None
    return (fv / cp - 1.0) * 100.0


# Canonical header -> UnifiedQuote attribute
HEADER_TO_ATTR: Dict[str, str] = {
    "Symbol": "symbol",
    "Company Name": "name",
    "Sector": "sector",
    "Sub-Sector": "sub_sector",
    "Market": "market",
    "Currency": "currency",
    "Listing Date": "listing_date",
    "Last Price": "current_price",
    "Previous Close": "previous_close",
    "Price Change": "price_change",
    "Percent Change": "percent_change",
    "Day High": "day_high",
    "Day Low": "day_low",
    "52W High": "high_52w",
    "52W Low": "low_52w",
    "52W Position %": "position_52w_percent",
    "Volume": "volume",
    "Avg Volume (30D)": "avg_volume_30d",
    "Value Traded": "value_traded",
    "Turnover %": "turnover_percent",
    "Shares Outstanding": "shares_outstanding",
    "Free Float %": "free_float",
    "Market Cap": "market_cap",
    "Free Float Market Cap": "free_float_market_cap",
    "Liquidity Score": "liquidity_score",
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
    "Volatility (30D)": "volatility_30d",
    "RSI (14)": "rsi_14",
    "Fair Value": "fair_value",
    "Upside %": "upside_percent",
    "Valuation Label": "valuation_label",
    "Value Score": "value_score",
    "Quality Score": "quality_score",
    "Momentum Score": "momentum_score",
    "Opportunity Score": "opportunity_score",
    "Risk Score": "risk_score",
    "Overall Score": "overall_score",
    "Error": "error",
    "Recommendation": "recommendation",
    "Data Source": "data_source",
    "Data Quality": "data_quality",
    "Last Updated (UTC)": "last_updated_utc",
    "Last Updated (Riyadh)": "last_updated_riyadh",
}


class EnrichedQuote(BaseModel):
    model_config = ConfigDict(populate_by_name=True, from_attributes=True, extra="ignore")

    # Keep fields aligned with UnifiedQuote + sheet headers
    symbol: str

    name: Optional[str] = None
    sector: Optional[str] = None
    sub_sector: Optional[str] = None
    market: Optional[str] = None
    currency: Optional[str] = None
    listing_date: Optional[str] = None

    current_price: Optional[float] = None
    previous_close: Optional[float] = None
    price_change: Optional[float] = None
    percent_change: Optional[float] = None
    day_high: Optional[float] = None
    day_low: Optional[float] = None
    high_52w: Optional[float] = None
    low_52w: Optional[float] = None
    position_52w_percent: Optional[float] = None

    volume: Optional[float] = None
    avg_volume_30d: Optional[float] = None
    value_traded: Optional[float] = None
    turnover_percent: Optional[float] = None
    shares_outstanding: Optional[float] = None
    free_float: Optional[float] = None
    market_cap: Optional[float] = None
    free_float_market_cap: Optional[float] = None
    liquidity_score: Optional[float] = None

    eps_ttm: Optional[float] = None
    forward_eps: Optional[float] = None
    pe_ttm: Optional[float] = None
    forward_pe: Optional[float] = None
    pb: Optional[float] = None
    ps: Optional[float] = None
    ev_ebitda: Optional[float] = None
    dividend_yield: Optional[float] = None
    dividend_rate: Optional[float] = None
    payout_ratio: Optional[float] = None
    roe: Optional[float] = None
    roa: Optional[float] = None
    net_margin: Optional[float] = None
    ebitda_margin: Optional[float] = None
    revenue_growth: Optional[float] = None
    net_income_growth: Optional[float] = None
    beta: Optional[float] = None

    volatility_30d: Optional[float] = None
    rsi_14: Optional[float] = None

    fair_value: Optional[float] = None
    upside_percent: Optional[float] = None
    valuation_label: Optional[str] = None

    value_score: Optional[float] = None
    quality_score: Optional[float] = None
    momentum_score: Optional[float] = None
    opportunity_score: Optional[float] = None
    risk_score: Optional[float] = None
    overall_score: Optional[float] = None

    recommendation: Optional[str] = None
    data_source: Optional[str] = None
    data_quality: Optional[str] = None
    last_updated_utc: Optional[str] = None
    last_updated_riyadh: Optional[str] = None
    error: Optional[str] = None

    @classmethod
    def from_unified(cls, q: UnifiedQuote) -> "EnrichedQuote":
        # Copy what we can, then patch computed values safely.
        d = {}
        try:
            d = q.model_dump(exclude_none=False)  # pydantic v2
        except Exception:
            d = dict(getattr(q, "__dict__", {}) or {})

        # Compute upside if missing
        if d.get("upside_percent") is None:
            d["upside_percent"] = _compute_upside_percent(d.get("fair_value"), d.get("current_price"))

        return cls(**d)

    def to_row(self, headers: List[str]) -> List[Any]:
        """
        Return an exact-length row matching provided headers.
        Unknown headers -> blank cell (None).
        """
        # Build attr dict once
        data: Dict[str, Any] = {}
        try:
            data = self.model_dump(exclude_none=False)
        except Exception:
            data = dict(getattr(self, "__dict__", {}) or {})

        row: List[Any] = []
        for h in headers or []:
            attr = HEADER_TO_ATTR.get(h, None)
            if not attr:
                row.append(None)
                continue
            row.append(data.get(attr))
        return row


__all__ = ["EnrichedQuote", "HEADER_TO_ATTR"]
