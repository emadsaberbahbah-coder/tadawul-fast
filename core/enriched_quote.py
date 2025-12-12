"""
core/enriched_quote.py
===============================================
Enriched Quote schema & Sheet Row Adapter - v1.2

Author: Emad Bahbah (with GPT-5.2 Thinking)

Purpose
-------
- Provide a unified, sheet-friendly EnrichedQuote model built on top of
  core.data_engine_v2.UnifiedQuote.
- Convert UnifiedQuote -> EnrichedQuote.
- Convert EnrichedQuote -> Google Sheets row based on a header list.
- Keep backend + AI analysis + 9-page Google Sheets dashboard aligned.

Notes
-----
- Designed for Pydantic v2 (your requirements.txt uses pydantic>=2.7).
- Defensive serialization: NaN/Inf => None, dict/list => JSON string.
"""

from __future__ import annotations

import json
import math
import re
from datetime import date, datetime
from decimal import Decimal
from functools import lru_cache
from typing import Any, Dict, List, Optional, Sequence

from pydantic import ConfigDict

from .data_engine_v2 import UnifiedQuote


# ============================================================================
# Header <-> Field mapping utilities
# ============================================================================

def _normalize_header_name(name: str) -> str:
    """
    Normalize a header string into a canonical key:
    - Lowercase
    - Remove (), [], %, etc.
    - Replace separators with underscores
    - Collapse multiple underscores
    """
    s = (name or "").strip().lower()
    if not s:
        return ""

    # remove common symbols but preserve meaning via whitespace
    s = re.sub(r"[\(\)\[\]%]", " ", s)
    s = s.replace("&", " and ")
    s = s.replace("/", " ").replace("-", " ")
    s = re.sub(r"\s+", "_", s)
    s = re.sub(r"_+", "_", s)
    return s.strip("_")


# Map *normalized* header names to UnifiedQuote / EnrichedQuote field names.
# Extend safely when you add new columns.
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
    "free_float_": "free_float",
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
    "day_high": "high",
    "high": "high",
    "day_low": "low",
    "low": "low",
    "change": "change",
    "price_change": "change",
    "change_percent": "change_percent",
    "percent_change": "change_percent",
    "change_percent_": "change_percent",

    "52w_high": "high_52w",
    "52_week_high": "high_52w",
    "52w_low": "low_52w",
    "52_week_low": "low_52w",
    "52w_position": "position_52w_percent",
    "52w_position_percent": "position_52w_percent",
    "52_week_position": "position_52w_percent",
    "52_week_position_percent": "position_52w_percent",

    "volume": "volume",
    "avg_volume": "avg_volume_30d",
    "avg_volume_30d": "avg_volume_30d",
    "avg_volume_30d_": "avg_volume_30d",
    "average_volume_30d": "avg_volume_30d",
    "value_traded": "value_traded",
    "turnover": "turnover_rate",
    "turnover_percent": "turnover_rate",
    "turnover_": "turnover_rate",

    "bid": "bid_price",
    "bid_price": "bid_price",
    "ask": "ask_price",
    "ask_price": "ask_price",
    "bid_size": "bid_size",
    "ask_size": "ask_size",
    "spread": "spread_percent",
    "spread_percent": "spread_percent",
    "liquidity_score": "liquidity_score",

    # Fundamentals
    "eps": "eps_ttm",
    "eps_ttm": "eps_ttm",
    "forward_eps": "forward_eps",
    "p_e": "pe_ratio",
    "pe": "pe_ratio",
    "pe_ttm": "pe_ratio",
    "forward_pe": "forward_pe_ratio",
    "p_b": "pb_ratio",
    "pb": "pb_ratio",
    "p_s": "price_to_sales",
    "price_to_sales": "price_to_sales",
    "ev_ebitda": "ev_to_ebitda",
    "ev_to_ebitda": "ev_to_ebitda",

    "dividend_yield": "dividend_yield_percent",
    "dividend_yield_percent": "dividend_yield_percent",
    "dividend_rate": "dividend_rate",
    "payout_ratio": "dividend_payout_ratio",
    "payout_ratio_percent": "dividend_payout_ratio",
    "dividend_payout_ratio": "dividend_payout_ratio",
    "ex_dividend_date": "ex_dividend_date",

    "roe": "roe_percent",
    "roe_percent": "roe_percent",
    "roa": "roa_percent",
    "roa_percent": "roa_percent",
    "debt_to_equity": "debt_to_equity",
    "debt_equity": "debt_to_equity",
    "current_ratio": "current_ratio",
    "quick_ratio": "quick_ratio",
    "market_cap": "market_cap",
    "free_float_market_cap": "free_float_market_cap",

    # Growth / profitability
    "revenue_growth": "revenue_growth_percent",
    "revenue_growth_percent": "revenue_growth_percent",
    "net_income_growth": "net_income_growth_percent",
    "net_income_growth_percent": "net_income_growth_percent",
    "ebitda_margin": "ebitda_margin_percent",
    "ebitda_margin_percent": "ebitda_margin_percent",
    "net_margin": "net_margin_percent",
    "net_margin_percent": "net_margin_percent",
    "operating_margin": "operating_margin_percent",
    "operating_margin_percent": "operating_margin_percent",

    # Risk / technical
    "beta": "beta",
    "volatility_30d": "volatility_30d_percent",
    "volatility_30d_percent": "volatility_30d_percent",
    "rsi": "rsi_14",
    "rsi_14": "rsi_14",
    "macd": "macd",
    "ma_20d": "ma_20d",
    "ma_50d": "ma_50d",

    # AI valuation & scores
    "target_price": "target_price",
    "fair_value": "fair_value",
    "upside": "upside_percent",
    "upside_percent": "upside_percent",
    "valuation_label": "valuation_label",
    "value_score": "value_score",
    "quality_score": "quality_score",
    "momentum_score": "momentum_score",
    "opportunity_score": "opportunity_score",
    "overall_score": "overall_score",
    "rank": "rank",
    "recommendation": "recommendation",
    "risk_label": "risk_label",
    "risk_bucket": "risk_bucket",
    "exp_roi_3m": "exp_roi_3m",
    "exp_roi_12m": "exp_roi_12m",

    # Meta & providers
    "data_source": "data_source",
    "provider": "provider",
    "primary_provider": "primary_provider",
    "data_quality": "data_quality",

    # Timestamps
    "last_updated_utc": "last_updated_utc",
    "last_updated_local": "last_updated_riyadh",
    "last_updated_riyadh": "last_updated_riyadh",
    "as_of_utc": "as_of_utc",
    "as_of_local": "as_of_local",
    "timezone": "timezone",

    # Error
    "error": "error",
}


@lru_cache(maxsize=1024)
def _field_name_for_header(header: str) -> str:
    """
    Determine the UnifiedQuote/EnrichedQuote field name for a given header.
    - Try HEADER_FIELD_MAP first.
    - Fallback: use normalized header string itself as the field name.
    """
    norm = _normalize_header_name(header)
    if not norm:
        return ""
    return _HEADER_FIELD_MAP.get(norm, norm)


def _serialize_value(value: Any) -> Any:
    """
    Prepare a value for sending to Google Sheets:
    - datetime/date -> ISO string
    - NaN/Inf -> None
    - Decimal -> float
    - dict/list/tuple -> JSON string
    """
    if value is None:
        return None

    if isinstance(value, (datetime, date)):
        return value.isoformat()

    if isinstance(value, Decimal):
        return float(value)

    if isinstance(value, float):
        if math.isnan(value) or math.isinf(value):
            return None
        return value

    if isinstance(value, (dict, list, tuple)):
        try:
            return json.dumps(value, ensure_ascii=False)
        except Exception:
            return str(value)

    return value


# ============================================================================
# EnrichedQuote model
# ============================================================================

class EnrichedQuote(UnifiedQuote):
    """
    Sheet-friendly view of UnifiedQuote.

    - Inherits ALL fields from UnifiedQuote.
    - Adds:
        • from_unified() -> create EnrichedQuote from a UnifiedQuote.
        • to_row(headers) -> map to Google Sheets row based on header list.
    """

    model_config = ConfigDict(arbitrary_types_allowed=True)

    @classmethod
    def from_unified(cls, quote: UnifiedQuote | Dict[str, Any]) -> "EnrichedQuote":
        """Build EnrichedQuote from UnifiedQuote (or dict)."""
        if isinstance(quote, cls):
            return quote

        if isinstance(quote, dict):
            return cls(**quote)

        # Pydantic v2 model
        if hasattr(quote, "model_dump"):
            data = quote.model_dump()
        else:
            # Safety fallback (should not happen with pydantic v2)
            data = dict(quote)  # type: ignore[arg-type]

        return cls(**data)

    def to_row(self, headers: Sequence[str]) -> List[Any]:
        """
        Convert this EnrichedQuote into a row aligned with the given headers.
        Unknown headers return None.
        """
        row: List[Any] = []
        for header in headers:
            field_name = _field_name_for_header(header)
            if not field_name:
                row.append(None)
                continue
            value: Optional[Any] = getattr(self, field_name, None)
            row.append(_serialize_value(value))
        return row


__all__ = ["EnrichedQuote", "_field_name_for_header", "_normalize_header_name"]
