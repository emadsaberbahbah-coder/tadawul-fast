"""
core/enriched_quote.py
===============================================
Enriched Quote schema & Sheet Row Adapter - v1.3

Author: Emad Bahbah (with GPT-5.2 Thinking)

Purpose
-------
- Provide a unified, sheet-friendly EnrichedQuote model built on top of
  core.data_engine_v2.UnifiedQuote.
- Convert UnifiedQuote -> EnrichedQuote.
- Convert EnrichedQuote -> Google Sheets row based on a header list.
- Keep backend + AI analysis + 9-page Google Sheets dashboard aligned.

Key Improvements (v1.3)
-----------------------
- Correct handling of headers containing "%" (e.g., "Change %", "Dividend Yield %"):
  "%" is normalized into "_percent" so it maps to the correct *_percent fields.
- Header mapping keys are organized and sorted for readability/maintenance.
- More defensive serialization (NaN/Inf => None, dict/list => JSON).
- Cached normalization + header->field mapping for speed.

Notes
-----
- Designed for Pydantic v2 (your requirements.txt uses pydantic>=2.7).
- Unknown headers return None (never raises).
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
# Header normalization
# ============================================================================

@lru_cache(maxsize=2048)
def _normalize_header_name(name: str) -> str:
    """
    Normalize a header string into a canonical key:
    - Lowercase
    - Convert "%" into the word "percent" (critical for correct mapping)
    - Remove (), [], etc.
    - Replace separators with underscores
    - Collapse multiple underscores
    """
    s = (name or "").strip().lower()
    if not s:
        return ""

    # Preserve percent meaning (ex: "Change %" -> "change_percent")
    s = s.replace("%", " percent ")

    # Common replacements
    s = s.replace("&", " and ")
    s = re.sub(r"[\(\)\[\]\{\}]", " ", s)  # remove brackets
    s = re.sub(r"[,:;]", " ", s)

    # Convert separators to spaces then collapse to underscore
    s = s.replace("/", " ").replace("-", " ").replace("\\", " ")
    s = re.sub(r"\s+", "_", s)
    s = re.sub(r"_+", "_", s)
    return s.strip("_")


# ============================================================================
# Header -> Field mapping (sorted)
# ============================================================================

# NOTE:
# Keys MUST be normalized-form keys (output of _normalize_header_name).
# Values MUST be UnifiedQuote / EnrichedQuote attribute names.

_HEADER_FIELD_PAIRS: List[tuple[str, str]] = [
    # -------------------------
    # Identity
    # -------------------------
    ("company", "name"),
    ("company_name", "name"),
    ("currency", "currency"),
    ("exchange", "exchange"),
    ("industry", "industry"),
    ("listing_date", "listing_date"),
    ("market", "market"),
    ("market_region", "market_region"),
    ("name", "name"),
    ("sector", "sector"),
    ("sub_sector", "sub_sector"),
    ("sub_sector_name", "sub_sector"),
    ("symbol", "symbol"),
    ("ticker", "symbol"),

    # -------------------------
    # Capital structure / ownership / short interest
    # -------------------------
    ("free_float", "free_float"),
    ("free_float_percent", "free_float"),
    ("free_float_percent_", "free_float"),
    ("insider_ownership", "insider_ownership_percent"),
    ("insider_ownership_percent", "insider_ownership_percent"),
    ("inst_ownership", "institutional_ownership_percent"),
    ("institutional_ownership", "institutional_ownership_percent"),
    ("institutional_ownership_percent", "institutional_ownership_percent"),
    ("shares_outstanding", "shares_outstanding"),
    ("short_interest", "short_percent_float"),
    ("short_percent_float", "short_percent_float"),
    ("short_ratio", "short_ratio"),

    # -------------------------
    # Price / liquidity
    # -------------------------
    ("ask", "ask_price"),
    ("ask_price", "ask_price"),
    ("ask_size", "ask_size"),
    ("avg_volume", "avg_volume_30d"),
    ("avg_volume_30d", "avg_volume_30d"),
    ("average_volume_30d", "avg_volume_30d"),
    ("bid", "bid_price"),
    ("bid_price", "bid_price"),
    ("bid_size", "bid_size"),
    ("change", "change"),
    ("change_percent", "change_percent"),
    ("current_price", "last_price"),
    ("day_high", "high"),
    ("day_low", "low"),
    ("high", "high"),
    ("last_price", "last_price"),
    ("liquidity_score", "liquidity_score"),
    ("low", "low"),
    ("open", "open"),
    ("percent_change", "change_percent"),
    ("prev_close", "previous_close"),
    ("previous_close", "previous_close"),
    ("price", "last_price"),
    ("price_change", "change"),
    ("spread", "spread_percent"),
    ("spread_percent", "spread_percent"),
    ("turnover", "turnover_rate"),
    ("turnover_percent", "turnover_rate"),
    ("value_traded", "value_traded"),
    ("volume", "volume"),

    # 52-week
    ("52w_high", "high_52w"),
    ("52w_low", "low_52w"),
    ("52w_position", "position_52w_percent"),
    ("52w_position_percent", "position_52w_percent"),
    ("52_week_high", "high_52w"),
    ("52_week_low", "low_52w"),
    ("52_week_position", "position_52w_percent"),
    ("52_week_position_percent", "position_52w_percent"),

    # -------------------------
    # Fundamentals / valuation ratios
    # -------------------------
    ("current_ratio", "current_ratio"),
    ("debt_equity", "debt_to_equity"),
    ("debt_to_equity", "debt_to_equity"),
    ("dividend_payout_ratio", "dividend_payout_ratio"),
    ("dividend_rate", "dividend_rate"),
    ("dividend_yield", "dividend_yield_percent"),
    ("dividend_yield_percent", "dividend_yield_percent"),
    ("eps", "eps_ttm"),
    ("eps_ttm", "eps_ttm"),
    ("ev_ebitda", "ev_to_ebitda"),
    ("ev_to_ebitda", "ev_to_ebitda"),
    ("ex_dividend_date", "ex_dividend_date"),
    ("forward_eps", "forward_eps"),
    ("forward_pe", "forward_pe_ratio"),
    ("market_cap", "market_cap"),
    ("p_b", "pb_ratio"),
    ("p_e", "pe_ratio"),
    ("p_e_ttm", "pe_ratio"),  # e.g., "P/E (TTM)"
    ("p_s", "price_to_sales"),
    ("pb", "pb_ratio"),
    ("pb_ratio", "pb_ratio"),
    ("pe", "pe_ratio"),
    ("pe_ttm", "pe_ratio"),
    ("price_to_sales", "price_to_sales"),
    ("quick_ratio", "quick_ratio"),

    ("free_float_market_cap", "free_float_market_cap"),

    # -------------------------
    # Growth / profitability
    # -------------------------
    ("ebitda_margin", "ebitda_margin_percent"),
    ("ebitda_margin_percent", "ebitda_margin_percent"),
    ("net_income_growth", "net_income_growth_percent"),
    ("net_income_growth_percent", "net_income_growth_percent"),
    ("net_margin", "net_margin_percent"),
    ("net_margin_percent", "net_margin_percent"),
    ("operating_margin", "operating_margin_percent"),
    ("operating_margin_percent", "operating_margin_percent"),
    ("revenue_growth", "revenue_growth_percent"),
    ("revenue_growth_percent", "revenue_growth_percent"),
    ("roa", "roa_percent"),
    ("roa_percent", "roa_percent"),
    ("roe", "roe_percent"),
    ("roe_percent", "roe_percent"),

    # -------------------------
    # Risk / technical
    # -------------------------
    ("beta", "beta"),
    ("macd", "macd"),
    ("ma_20d", "ma_20d"),
    ("ma_50d", "ma_50d"),
    ("rsi", "rsi_14"),
    ("rsi_14", "rsi_14"),
    ("volatility_30d", "volatility_30d_percent"),
    ("volatility_30d_percent", "volatility_30d_percent"),

    # -------------------------
    # AI valuation & scores
    # -------------------------
    ("exp_roi_12m", "exp_roi_12m"),
    ("exp_roi_3m", "exp_roi_3m"),
    ("fair_value", "fair_value"),
    ("momentum_score", "momentum_score"),
    ("opportunity_score", "opportunity_score"),
    ("overall_score", "overall_score"),
    ("quality_score", "quality_score"),
    ("rank", "rank"),
    ("recommendation", "recommendation"),
    ("risk_bucket", "risk_bucket"),
    ("risk_label", "risk_label"),
    ("target_price", "target_price"),
    ("upside", "upside_percent"),
    ("upside_percent", "upside_percent"),
    ("valuation_label", "valuation_label"),
    ("value_score", "value_score"),

    # -------------------------
    # Meta & providers
    # -------------------------
    ("data_quality", "data_quality"),
    ("data_source", "data_source"),
    ("primary_provider", "primary_provider"),
    ("provider", "provider"),

    # -------------------------
    # Timestamps
    # -------------------------
    ("as_of_local", "as_of_local"),
    ("as_of_utc", "as_of_utc"),
    ("last_updated_local", "last_updated_riyadh"),
    ("last_updated_riyadh", "last_updated_riyadh"),
    ("last_updated_utc", "last_updated_utc"),
    ("timezone", "timezone"),

    # -------------------------
    # Error
    # -------------------------
    ("error", "error"),
]

# Build mapping dict (sorted by key, deterministic)
_HEADER_FIELD_MAP: Dict[str, str] = dict(sorted(_HEADER_FIELD_PAIRS, key=lambda kv: kv[0]))


# ============================================================================
# Header -> field resolver
# ============================================================================

@lru_cache(maxsize=4096)
def _field_name_for_header(header: str) -> str:
    """
    Determine the UnifiedQuote/EnrichedQuote field name for a given header.

    Resolution order:
    1) Normalized header -> mapped field (HEADER_FIELD_MAP)
    2) Fallback to normalized header itself (supports future columns that match field names)
    """
    norm = _normalize_header_name(header)
    if not norm:
        return ""
    return _HEADER_FIELD_MAP.get(norm, norm)


# ============================================================================
# Serialization to Google Sheets-safe cells
# ============================================================================

def _safe_json_dumps(value: Any) -> str:
    try:
        return json.dumps(value, ensure_ascii=False)
    except Exception:
        return str(value)


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
        return _safe_json_dumps(value)

    return value


# ============================================================================
# EnrichedQuote model
# ============================================================================

class EnrichedQuote(UnifiedQuote):
    """
    Sheet-friendly view of UnifiedQuote.

    - Inherits ALL fields from UnifiedQuote.
    - Adds:
        • from_unified() -> create EnrichedQuote from a UnifiedQuote (or dict).
        • to_row(headers) -> map to Google Sheets row based on header list.
    """

    model_config = ConfigDict(
        arbitrary_types_allowed=True,
        extra="allow",  # defensive: ignore new/extra provider keys without breaking
    )

    @classmethod
    def from_unified(cls, quote: UnifiedQuote | Dict[str, Any]) -> "EnrichedQuote":
        """Build EnrichedQuote from UnifiedQuote (or dict)."""
        if isinstance(quote, cls):
            return quote

        if isinstance(quote, dict):
            return cls(**quote)

        if hasattr(quote, "model_dump"):  # Pydantic v2
            data = quote.model_dump()
        else:
            # Defensive fallback
            try:
                data = dict(quote)  # type: ignore[arg-type]
            except Exception:
                data = {"error": "Unable to convert quote to dict"}

        return cls(**data)

    def to_row(self, headers: Sequence[str]) -> List[Any]:
        """
        Convert this EnrichedQuote into a row aligned with the given headers.
        Unknown headers return None. Never raises.
        """
        row: List[Any] = []
        for header in headers:
            field_name = _field_name_for_header(str(header))
            if not field_name:
                row.append(None)
                continue
            value: Optional[Any] = getattr(self, field_name, None)
            row.append(_serialize_value(value))
        return row


__all__ = ["EnrichedQuote", "_field_name_for_header", "_normalize_header_name"]
