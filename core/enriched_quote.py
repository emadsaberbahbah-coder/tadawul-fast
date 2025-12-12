"""
core/enriched_quote.py
===============================================
Enriched Quote schema & Sheet Row Adapter - v1.2

Author: Emad Bahbah (with GPT-5.2 Thinking)

Purpose
-------
- Provide a unified, sheet-friendly EnrichedQuote model built on top of
  core.data_engine_v2.UnifiedQuote.
- Add helpers to:
    • Convert UnifiedQuote / dict -> EnrichedQuote.
    • Convert EnrichedQuote -> Google Sheets row based on a header list.
- Keep ALL fields from UnifiedQuote so the backend, AI analysis, and
  9-page Google Sheets dashboard stay fully aligned.

Design notes
------------
- Header mapping is tolerant:
    • "P/E", "P E", "P-E" -> "pe_ratio"
    • "Dividend Yield %" -> "dividend_yield_percent"
    • "52W High" -> "high_52w"
- Unknown headers fall back to normalized header name (best-effort).
- Serialization is Sheets-safe:
    • datetime/date -> ISO
    • NaN/inf -> None
    • dict/list -> JSON string
"""

from __future__ import annotations

import json
import math
import re
from datetime import date, datetime
from functools import lru_cache
from typing import Any, Dict, List, Optional, Sequence

from .data_engine_v2 import UnifiedQuote

# Pydantic v2 config (fallback to v1 if needed)
try:
    from pydantic import ConfigDict  # type: ignore
except Exception:  # pragma: no cover
    ConfigDict = None  # type: ignore


# ============================================================================
# Header <-> Field mapping utilities
# ============================================================================

_NORMALIZE_RE_REMOVE = re.compile(r"[\(\)\[\]\{\}%]", re.UNICODE)
_NORMALIZE_RE_WS = re.compile(r"\s+", re.UNICODE)
_NORMALIZE_RE_MULTI_UNDERSCORE = re.compile(r"_+", re.UNICODE)


@lru_cache(maxsize=4096)
def _normalize_header_name(name: str) -> str:
    """
    Normalize a header string into a canonical key:
    - lowercase
    - remove (), [], {}, % etc
    - replace separators (/ - . :) with underscores
    - collapse whitespace to underscores
    - collapse multiple underscores
    """
    s = (name or "").strip().lower()

    # Remove brackets / percent signs
    s = _NORMALIZE_RE_REMOVE.sub(" ", s)

    # Common separators -> underscore
    for ch in ("/", "-", ".", ":", "|"):
        s = s.replace(ch, "_")

    # Whitespace -> underscore
    s = _NORMALIZE_RE_WS.sub("_", s)

    # Collapse multiple underscores
    s = _NORMALIZE_RE_MULTI_UNDERSCORE.sub("_", s)

    return s.strip("_")


# Map *normalized* header names to UnifiedQuote / EnrichedQuote field names.
# Extend freely as new columns are added.
_HEADER_FIELD_MAP: Dict[str, str] = {
    # Identity
    "symbol": "symbol",
    "ticker": "symbol",
    "company": "name",
    "company_name": "name",
    "name": "name",
    "sector": "sector",
    "sub_sector": "sub_sector",
    "subsector": "sub_sector",
    "industry": "industry",
    "market": "market",
    "market_region": "market_region",
    "exchange": "exchange",
    "currency": "currency",
    "listing_date": "listing_date",

    # Ownership / float / short
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
    "day_high": "high",
    "high": "high",
    "day_low": "low",
    "low": "low",
    "change": "change",
    "price_change": "change",
    "change_percent": "change_percent",
    "percent_change": "change_percent",

    "52w_high": "high_52w",
    "52_week_high": "high_52w",
    "high_52w": "high_52w",
    "52w_low": "low_52w",
    "52_week_low": "low_52w",
    "low_52w": "low_52w",
    "52w_position": "position_52w_percent",
    "52w_position_percent": "position_52w_percent",
    "position_52w_percent": "position_52w_percent",

    "volume": "volume",
    "avg_volume": "avg_volume_30d",
    "avg_volume_30d": "avg_volume_30d",
    "average_volume_30d": "avg_volume_30d",
    "value_traded": "value_traded",
    "turnover": "turnover_rate",
    "turnover_percent": "turnover_rate",

    "bid": "bid_price",
    "bid_price": "bid_price",
    "ask": "ask_price",
    "ask_price": "ask_price",
    "bid_size": "bid_size",
    "ask_size": "ask_size",
    "spread": "spread_percent",
    "spread_percent": "spread_percent",
    "liquidity_score": "liquidity_score",

    # Returns (optional – only if UnifiedQuote has these)
    "ytd_return": "ytd_return_percent",
    "1y_return": "return_1y_percent",
    "3m_return": "return_3m_percent",
    "6m_return": "return_6m_percent",
    "12m_return": "return_12m_percent",

    # Fundamentals
    "eps": "eps_ttm",
    "eps_ttm": "eps_ttm",
    "forward_eps": "forward_eps",
    "p_e": "pe_ratio",
    "pe": "pe_ratio",
    "pe_ttm": "pe_ratio",
    "forward_pe": "forward_pe",
    "p_b": "pb_ratio",
    "pb": "pb_ratio",
    "p_s": "price_to_sales",
    "ps": "price_to_sales",
    "dividend_yield": "dividend_yield_percent",
    "dividend_yield_percent": "dividend_yield_percent",
    "dividend_rate": "dividend_rate",
    "payout_ratio": "dividend_payout_ratio",
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

    # Growth / profitability
    "revenue_growth": "revenue_growth_percent",
    "revenue_growth_percent": "revenue_growth_percent",
    "net_income_growth": "net_income_growth_percent",
    "net_income_growth_percent": "net_income_growth_percent",
    "ebitda_margin": "ebitda_margin_percent",
    "ebitda_margin_percent": "ebitda_margin_percent",
    "operating_margin": "operating_margin_percent",
    "operating_margin_percent": "operating_margin_percent",
    "net_margin": "net_margin_percent",
    "net_margin_percent": "net_margin_percent",

    # Valuation / risk
    "ev_ebitda": "ev_to_ebitda",
    "ev_to_ebitda": "ev_to_ebitda",
    "price_to_sales": "price_to_sales",
    "price_to_cash_flow": "price_to_cash_flow",
    "peg": "peg_ratio",
    "peg_ratio": "peg_ratio",
    "beta": "beta",
    "volatility_30d": "volatility_30d_percent",
    "volatility_30d_percent": "volatility_30d_percent",
    "target_price": "target_price",

    # AI valuation & scores
    "fair_value": "fair_value",
    "upside": "upside_percent",
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

    # Meta / providers
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


def extend_header_field_map(new_map: Dict[str, str]) -> None:
    """
    Extend the global header->field map at runtime.
    Keys may be raw headers or already-normalized keys.
    """
    for k, v in (new_map or {}).items():
        nk = _normalize_header_name(k)
        if nk:
            _HEADER_FIELD_MAP[nk] = v


@lru_cache(maxsize=8192)
def _field_name_for_header(header: str) -> str:
    """
    Determine the UnifiedQuote/EnrichedQuote field name for a given header.
    - Try HEADER_FIELD_MAP first.
    - Fallback: normalized header itself (best effort).
    """
    norm = _normalize_header_name(header)
    return _HEADER_FIELD_MAP.get(norm, norm)


def _is_nan_or_inf(x: Any) -> bool:
    try:
        return isinstance(x, (float, int)) and (math.isnan(float(x)) or math.isinf(float(x)))
    except Exception:
        return False


def _serialize_value(value: Any) -> Any:
    """
    Prepare a value for sending to Google Sheets:
    - None -> None
    - NaN/Inf -> None
    - datetime/date -> ISO string
    - dict/list -> JSON string
    - everything else -> unchanged
    """
    if value is None:
        return None

    if _is_nan_or_inf(value):
        return None

    if isinstance(value, datetime):
        return value.isoformat()

    if isinstance(value, date):
        return value.isoformat()

    if isinstance(value, (dict, list)):
        try:
            return json.dumps(value, ensure_ascii=False, default=str)
        except Exception:
            return str(value)

    return value


# ============================================================================
# EnrichedQuote model
# ============================================================================

class EnrichedQuote(UnifiedQuote):
    """
    Sheet-friendly view of UnifiedQuote.

    Inherits ALL fields from UnifiedQuote (symbol, last_price, AI scores, etc.).
    Adds:
      • from_unified() / from_any()
      • to_row(headers)
      • to_dict_for_headers(headers)
    """

    # Pydantic v2 config
    if ConfigDict is not None:  # pragma: no cover
        model_config = ConfigDict(arbitrary_types_allowed=True)

    # ------------------------------------------------------------------ #
    # Constructors / converters
    # ------------------------------------------------------------------ #

    @classmethod
    def from_unified(cls, quote: UnifiedQuote) -> "EnrichedQuote":
        """
        Build an EnrichedQuote from a UnifiedQuote instance.
        If 'quote' is already EnrichedQuote, return as-is.
        """
        if isinstance(quote, cls):
            return quote

        if hasattr(quote, "model_dump"):
            data = quote.model_dump()
        else:  # pydantic v1 fallback
            data = quote.dict()  # type: ignore[attr-defined]

        return cls(**data)

    @classmethod
    def from_any(cls, obj: Any) -> "EnrichedQuote":
        """
        Accept UnifiedQuote / EnrichedQuote / dict-like and return EnrichedQuote.
        """
        if obj is None:
            raise ValueError("from_any() received None")

        if isinstance(obj, cls):
            return obj

        if isinstance(obj, UnifiedQuote):
            return cls.from_unified(obj)

        if isinstance(obj, dict):
            return cls(**obj)

        # last resort
        if hasattr(obj, "model_dump"):
            return cls(**obj.model_dump())
        if hasattr(obj, "__dict__"):
            return cls(**dict(obj.__dict__))

        raise TypeError(f"Unsupported type for from_any(): {type(obj)}")

    # ------------------------------------------------------------------ #
    # Sheet adapters
    # ------------------------------------------------------------------ #

    def to_row(self, headers: Sequence[str], default: Any = None) -> List[Any]:
        """
        Convert this EnrichedQuote into a row aligned with the given headers.

        - Each header is mapped to a field via _field_name_for_header().
        - If the attribute does not exist, 'default' is used (default None).
        """
        row: List[Any] = []
        for header in headers:
            field_name = _field_name_for_header(header)
            value = getattr(self, field_name, default)
            row.append(_serialize_value(value))
        return row

    def to_dict_for_headers(self, headers: Sequence[str], default: Any = None) -> Dict[str, Any]:
        """
        Return a dict keyed by the *original headers* with values taken from this quote.
        Useful for debugging mismatches between sheet headers and UnifiedQuote fields.
        """
        out: Dict[str, Any] = {}
        for header in headers:
            field_name = _field_name_for_header(header)
            out[header] = _serialize_value(getattr(self, field_name, default))
        return out


__all__ = [
    "EnrichedQuote",
    "extend_header_field_map",
    "_field_name_for_header",
    "_normalize_header_name",
]
