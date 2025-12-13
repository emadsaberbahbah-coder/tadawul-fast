"""
core/enriched_quote.py
===============================================
Enriched Quote schema & Sheet Row Adapter - v1.4

Author: Emad Bahbah (with GPT-5.2 Thinking)

Purpose
-------
- Provide a unified, sheet-friendly EnrichedQuote model built on top of
  core.data_engine_v2.UnifiedQuote.
- Convert UnifiedQuote -> EnrichedQuote.
- Convert EnrichedQuote -> Google Sheets row based on a header list.
- Keep backend + AI analysis + 9-page Google Sheets dashboard aligned.

Key Improvements (v1.4)
-----------------------
- STRICT ALIGNMENT with core.data_engine_v2.UnifiedQuote fields.
- Correct handling of headers containing "%" (e.g., "Change %" -> "percent_change").
- More defensive serialization (NaN/Inf => None, dict/list => JSON).
- Cached normalization + header->field mapping for speed.

Notes
-----
- Designed for Pydantic v2 (inherits from UnifiedQuote).
- Unknown headers return None (never raises).
"""

from __future__ import annotations

import json
import math
import re
from datetime import date, datetime
from decimal import Decimal
from functools import lru_cache
from typing import Any, Dict, List, Optional, Sequence, Union

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
# Header -> Field mapping (Sorted & Aligned to data_engine_v2)
# ============================================================================

# NOTE:
# Keys MUST be normalized-form keys (output of _normalize_header_name).
# Values MUST be UnifiedQuote attribute names found in core/data_engine_v2.py.

_HEADER_FIELD_PAIRS: List[tuple[str, str]] = [
    # -------------------------
    # Identity
    # -------------------------
    ("company", "name"),
    ("company_name", "name"),
    ("currency", "currency"),
    ("exchange", "market"), # Map exchange to market if needed, or define exchange in v2
    ("industry", "industry"),
    ("market", "market"),
    ("market_region", "market"), # Loose mapping
    ("name", "name"),
    ("sector", "sector"),
    ("symbol", "symbol"),
    ("ticker", "symbol"),

    # -------------------------
    # Price
    # -------------------------
    ("ask", "current_price"), # Fallback
    ("bid", "current_price"), # Fallback
    ("change", "price_change"),
    ("change_percent", "percent_change"), # "Change %" -> "percent_change"
    ("current_price", "current_price"),
    ("day_high", "day_high"),
    ("day_low", "day_low"),
    ("high", "day_high"),
    ("last_price", "current_price"),
    ("low", "day_low"),
    ("open", "previous_close"), # Approximate if open missing
    ("percent_change", "percent_change"),
    ("prev_close", "previous_close"),
    ("previous_close", "previous_close"),
    ("price", "current_price"),
    ("price_change", "price_change"),

    # 52-week
    ("52w_high", "high_52w"),
    ("52w_low", "low_52w"),
    ("52_week_high", "high_52w"),
    ("52_week_low", "low_52w"),

    # -------------------------
    # Volume / Liquidity
    # -------------------------
    ("avg_volume", "avg_volume_30d"),
    ("avg_volume_30d", "avg_volume_30d"),
    ("average_volume_30d", "avg_volume_30d"),
    ("value_traded", "value_traded"),
    ("volume", "volume"),

    # -------------------------
    # Capital Structure
    # -------------------------
    ("free_float", "free_float"),
    ("free_float_percent", "free_float"),
    ("market_cap", "market_cap"),
    ("shares_outstanding", "shares_outstanding"),

    # -------------------------
    # Fundamentals / Ratios
    # -------------------------
    ("beta", "beta"),
    ("current_ratio", "current_ratio"),
    ("debt_equity", "debt_to_equity"),
    ("debt_to_equity", "debt_to_equity"),
    ("dividend_payout_ratio", "payout_ratio"),
    ("dividend_yield", "dividend_yield"),
    ("dividend_yield_percent", "dividend_yield"), # "Dividend Yield %" -> "dividend_yield"
    ("ebitda_margin", "ebitda_margin"),
    ("ebitda_margin_percent", "ebitda_margin"),
    ("eps", "eps_ttm"),
    ("eps_ttm", "eps_ttm"),
    ("ev_ebitda", "ev_ebitda"),
    ("ev_to_ebitda", "ev_ebitda"),
    ("forward_eps", "forward_eps"),
    ("forward_pe", "forward_pe"),
    ("net_income_growth", "net_income_growth"),
    ("net_margin", "net_margin"),
    ("net_margin_percent", "net_margin"),
    ("p_b", "pb"),
    ("p_e", "pe_ttm"),
    ("p_e_ttm", "pe_ttm"),
    ("p_s", "ps"),
    ("payout_ratio", "payout_ratio"),
    ("pb", "pb"),
    ("pe", "pe_ttm"),
    ("pe_ttm", "pe_ttm"),
    ("price_to_sales", "ps"),
    ("ps", "ps"),
    ("quick_ratio", "quick_ratio"),
    ("revenue_growth", "revenue_growth"),
    ("roa", "roa"),
    ("roa_percent", "roa"),
    ("roe", "roe"),
    ("roe_percent", "roe"),

    # -------------------------
    # Technicals
    # -------------------------
    ("ma20", "ma20"),
    ("ma50", "ma50"),
    ("macd", "macd"),
    ("rsi", "rsi_14"),
    ("rsi_14", "rsi_14"),
    ("volatility_30d", "volatility_30d"),

    # -------------------------
    # AI / Scoring / Valuation
    # -------------------------
    ("fair_value", "fair_value"),
    ("momentum_score", "momentum_score"),
    ("opportunity_score", "opportunity_score"),
    ("overall_score", "overall_score"),
    ("quality_score", "quality_score"),
    ("recommendation", "recommendation"),
    ("target_price", "target_price"),
    ("upside", "upside_percent"),
    ("upside_percent", "upside_percent"),
    ("valuation_label", "valuation_label"),
    ("value_score", "value_score"),

    # -------------------------
    # Meta / Time
    # -------------------------
    ("data_quality", "data_quality"),
    ("data_source", "data_source"),
    ("error", "error"),
    ("last_updated", "last_updated_utc"),
    ("last_updated_riyadh", "last_updated_riyadh"),
    ("last_updated_utc", "last_updated_utc"),
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
    # Check explicit map first, then check if normalization matches a field directly (rare but possible)
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
                data = {"symbol": getattr(quote, "symbol", "UNKNOWN"), "error": "Unable to convert quote to dict"}

        return cls(**data)

    def to_row(self, headers: Sequence[str]) -> List[Any]:
        """
        Convert this EnrichedQuote into a row aligned with the given headers.
        Unknown headers return None. Never raises.
        """
        row: List[Any] = []
        for header in headers:
            field_name = _field_name_for_header(str(header))
            
            # Explicitly check if the mapped field name exists on the model
            if not field_name or not hasattr(self, field_name):
                # Try fallback: maybe header IS the field name
                if hasattr(self, str(header)):
                     value = getattr(self, str(header), None)
                else:
                     value = None
            else:
                value = getattr(self, field_name, None)
            
            row.append(_serialize_value(value))
        return row


__all__ = ["EnrichedQuote", "_field_name_for_header", "_normalize_header_name"]
