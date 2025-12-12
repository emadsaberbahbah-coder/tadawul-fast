"""
core/enriched_quote.py
===============================================
Enriched Quote schema & Sheet Row Adapter - v1.0

Author: Emad Bahbah (with GPT-5.1 Thinking)

Purpose
-------
- Provide a unified, sheet-friendly EnrichedQuote model built on top of
  core.data_engine_v2.UnifiedQuote.
- Add helpers to:
    • Convert UnifiedQuote -> EnrichedQuote.
    • Convert EnrichedQuote -> Google Sheets row based on a header list.
- Keep ALL fields from UnifiedQuote so the backend, AI analysis, and
  9-page Google Sheets dashboard stay fully aligned.

Typical usage
-------------
    from core.data_engine_v2 import DataEngine
    from core.enriched_quote import EnrichedQuote

    engine = DataEngine()

    # 1) Get backend unified quote
    q_unified = await engine.get_enriched_quote("1120.SR")

    # 2) Wrap as EnrichedQuote (adds to_row helper)
    eq = EnrichedQuote.from_unified(q_unified)

    # 3) Convert to row for Sheets (headers from core.schemas)
    from core.schemas import get_headers_for_sheet
    headers = get_headers_for_sheet("KSA_Tadawul")
    row = eq.to_row(headers)
"""

from __future__ import annotations

import re
from datetime import date, datetime
from typing import Any, Dict, List, Optional, Sequence

from pydantic import BaseModel

from .data_engine_v2 import UnifiedQuote


# ============================================================================
# Header <-> Field mapping utilities
# ============================================================================


def _normalize_header_name(name: str) -> str:
    """
    Normalize a header string into a canonical key:
    - Lowercase
    - Remove (), [], %, etc.
    - Replace spaces, /, - with underscores
    - Collapse multiple underscores
    """
    s = name.strip().lower()
    # Remove brackets / percent signs
    s = re.sub(r"[\(\)\[\]%]", " ", s)
    # Replace separators with underscore
    s = s.replace("/", "_").replace("-", "_")
    # Collapse whitespace -> underscore
    s = re.sub(r"\s+", "_", s)
    # Collapse multiple underscores
    s = re.sub(r"_+", "_", s)
    return s.strip("_")


# Map *normalized* header names to UnifiedQuote / EnrichedQuote field names.
# You can extend this dict at any time when new columns are added.
_HEADER_FIELD_MAP: Dict[str, str] = {
    # Identity
    "symbol": "symbol",
    "ticker": "symbol",
    "company": "name",
    "company_name": "name",
    "name": "name",
    "sector": "sector",
    "sub_sector": "sub_sector",
    "sub_sector_name": "sub_sector",
    "sub_sector_desc": "sub_sector",
    "industry": "industry",
    "market": "market",
    "market_region": "market_region",
    "exchange": "exchange",
    "currency": "currency",
    "listing_date": "listing_date",

    # Capital structure / ownership / short interest
    "shares_outstanding": "shares_outstanding",
    "free_float": "free_float",
    "free_float_percent": "free_float",
    "insider_ownership": "insider_ownership_percent",
    "insider_ownership_percent": "insider_ownership_percent",
    "inst_ownership": "institutional_ownership_percent",
    "institutional_ownership": "institutional_ownership_percent",
    "institutional_ownership_percent": "institutional_ownership_percent",
    "short_ratio": "short_ratio",
    "short_interest": "short_percent_float",
    "short_percent_float": "short_percent_float",

    # Price / liquidity
    "last_price": "last_price",
    "price": "last_price",
    "current_price": "last_price",
    "previous_close": "previous_close",
    "prev_close": "previous_close",
    "open": "open",
    "open_price": "open",
    "day_high": "high",
    "high": "high",
    "day_low": "low",
    "low": "low",
    "change": "change",
    "price_change": "change",
    "change_percent": "change_percent",
    "percent_change": "change_percent",
    "change_": "change_percent",          # e.g. "Change %"

    "52w_high": "high_52w",
    "52_week_high": "high_52w",
    "52w_low": "low_52w",
    "52_week_low": "low_52w",
    "52w_position": "position_52w_percent",
    "52w_position_": "position_52w_percent",  # "52W Position %"
    "52w_position_percent": "position_52w_percent",
    "fifty_two_week_position": "position_52w_percent",

    "volume": "volume",
    "avg_volume": "avg_volume_30d",
    "avg_volume_30d": "avg_volume_30d",
    "average_volume_30d": "avg_volume_30d",
    "value_traded": "value_traded",
    "turnover": "turnover_rate",
    "turnover_": "turnover_rate",
    "turnover_percent": "turnover_rate",

    "bid": "bid_price",
    "bid_price": "bid_price",
    "ask": "ask_price",
    "ask_price": "ask_price",
    "bid_size": "bid_size",
    "ask_size": "ask_size",
    "spread": "spread_percent",
    "spread_": "spread_percent",
    "spread_percent": "spread_percent",
    "liquidity_score": "liquidity_score",

    # Fundamentals
    "eps": "eps_ttm",
    "eps_ttm": "eps_ttm",
    "p_e": "pe_ratio",
    "pe": "pe_ratio",
    "pe_ttm": "pe_ratio",
    "p_b": "pb_ratio",
    "pb": "pb_ratio",
    "pb_ratio": "pb_ratio",
    "dividend_yield": "dividend_yield_percent",
    "dividend_yield_": "dividend_yield_percent",
    "dividend_yield_percent": "dividend_yield_percent",
    "dividend_rate": "dividend_rate",
    "dividend_payout": "dividend_payout_ratio",
    "payout_ratio": "dividend_payout_ratio",
    "dividend_payout_ratio": "dividend_payout_ratio",
    "ex_dividend_date": "ex_dividend_date",

    "roe": "roe_percent",
    "roe_": "roe_percent",
    "roe_percent": "roe_percent",
    "roa": "roa_percent",
    "roa_": "roa_percent",
    "roa_percent": "roa_percent",
    "debt_to_equity": "debt_to_equity",
    "debt_equity": "debt_to_equity",
    "current_ratio": "current_ratio",
    "quick_ratio": "quick_ratio",
    "market_cap": "market_cap",

    # Growth / profitability
    "revenue_growth": "revenue_growth_percent",
    "revenue_growth_": "revenue_growth_percent",
    "revenue_growth_percent": "revenue_growth_percent",
    "net_income_growth": "net_income_growth_percent",
    "net_income_growth_": "net_income_growth_percent",
    "net_income_growth_percent": "net_income_growth_percent",
    "ebitda_margin": "ebitda_margin_percent",
    "ebitda_margin_": "ebitda_margin_percent",
    "ebitda_margin_percent": "ebitda_margin_percent",
    "operating_margin": "operating_margin_percent",
    "operating_margin_": "operating_margin_percent",
    "operating_margin_percent": "operating_margin_percent",
    "net_margin": "net_margin_percent",
    "net_margin_": "net_margin_percent",
    "net_margin_percent": "net_margin_percent",

    # Valuation / risk
    "ev_ebitda": "ev_to_ebitda",
    "ev_to_ebitda": "ev_to_ebitda",
    "price_to_sales": "price_to_sales",
    "p_s": "price_to_sales",
    "price_to_cash_flow": "price_to_cash_flow",
    "peg": "peg_ratio",
    "peg_ratio": "peg_ratio",
    "beta": "beta",
    "volatility_30d": "volatility_30d_percent",
    "volatility_30d_": "volatility_30d_percent",
    "volatility_30d_percent": "volatility_30d_percent",
    "target_price": "target_price",

    # AI valuation & scores
    "fair_value": "fair_value",
    "upside_": "upside_percent",
    "upside_percent": "upside_percent",
    "valuation_label": "valuation_label",
    "value_score": "value_score",
    "quality_score": "quality_score",
    "momentum_score": "momentum_score",
    "opportunity_score": "opportunity_score",
    "overall_score": "overall_score",
    "ai_score": "overall_score",
    "recommendation": "recommendation",
    "risk_label": "risk_label",
    "risk_bucket": "risk_bucket",
    "exp_roi_3m": "exp_roi_3m",
    "exp_roi_12m": "exp_roi_12m",

    # Technicals
    "rsi": "rsi_14",
    "rsi_14": "rsi_14",
    "macd": "macd",
    "ma20": "ma_20d",
    "ma20d": "ma_20d",
    "ma_20d": "ma_20d",
    "ma50": "ma_50d",
    "ma50d": "ma_50d",
    "ma_50d": "ma_50d",

    # Meta & providers
    "data_source": "data_source",
    "provider": "provider",
    "primary_provider": "primary_provider",
    "confidence": "data_quality",
    "data_quality": "data_quality",

    # Timestamps
    "last_updated_utc": "last_updated_utc",
    "last_updated": "last_updated_utc",
    "last_updated_local": "last_updated_riyadh",
    "last_updated_riyadh": "last_updated_riyadh",
    "as_of_utc": "as_of_utc",
    "as_of_local": "as_of_local",
    "timezone": "timezone",

    # Error
    "error": "error",
}


def _field_name_for_header(header: str) -> str:
    """
    Determine the UnifiedQuote/EnrichedQuote field name for a given header.
    - Try HEADER_FIELD_MAP first.
    - Fallback: use normalized header string itself as the field name.
    """
    norm = _normalize_header_name(header)
    if norm in _HEADER_FIELD_MAP:
        return _HEADER_FIELD_MAP[norm]
    return norm  # may still match e.g. 'ytd_return' if you add such a field later


def _serialize_value(value: Any) -> Any:
    """
    Prepare a value for sending to Google Sheets:
    - datetime/date -> ISO string
    - other types -> unchanged
    """
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, date):
        return value.isoformat()
    return value


# ============================================================================
# EnrichedQuote model
# ============================================================================


class EnrichedQuote(UnifiedQuote):
    """
    Sheet-friendly view of UnifiedQuote.

    - Inherits ALL fields from UnifiedQuote (symbol, last_price, AI scores, etc.).
    - Adds:
        • from_unified() -> create EnrichedQuote from a UnifiedQuote.
        • to_row(headers) -> map to Google Sheets row based on header list.
    """

    class Config:
        # Allow arbitrary types in 'raw' if needed
        arbitrary_types_allowed = True

    # ------------------------------------------------------------------ #
    # Constructors / converters
    # ------------------------------------------------------------------ #

    @classmethod
    def from_unified(cls, quote: UnifiedQuote) -> "EnrichedQuote":
        """
        Build an EnrichedQuote from a UnifiedQuote instance.

        If 'quote' is already EnrichedQuote, it is returned as-is.
        """
        if isinstance(quote, cls):
            return quote

        if hasattr(quote, "model_dump"):
            data = quote.model_dump()
        else:  # pydantic v1 fallback
            data = quote.dict()  # type: ignore[attr-defined]

        return cls(**data)

    # ------------------------------------------------------------------ #
    # Sheet row adapter
    # ------------------------------------------------------------------ #

    def to_row(self, headers: Sequence[str]) -> List[Any]:
        """
        Convert this EnrichedQuote into a row aligned with the given headers.

        Each header is mapped to a field via _field_name_for_header().
        For unknown headers, this method will:
          - Try to use the normalized header as a direct attribute name.
          - If the attribute does not exist, return None for that column.
        """
        row: List[Any] = []
        for header in headers:
            field_name = _field_name_for_header(header)
            value: Optional[Any] = getattr(self, field_name, None)
            row.append(_serialize_value(value))
        return row


__all__ = ["EnrichedQuote", "_field_name_for_header", "_normalize_header_name"]
