"""
core/enriched_quote.py
===============================================
Enriched Quote schema & Sheet Row Adapter - v1.6

Purpose
-------
- Provide a unified, sheet-friendly EnrichedQuote model built on top of
  core.data_engine_v2.UnifiedQuote.
- Convert UnifiedQuote -> EnrichedQuote.
- Convert EnrichedQuote -> Google Sheets row based on a header list.
- Keep backend + AI analysis + 9-page Google Sheets dashboard aligned.

Key Improvements (v1.6)
-----------------------
- STRICT alignment with core.data_engine_v2.UnifiedQuote (Pydantic v2).
- Robust header normalization (%, (), 52W, 30D, Arabic digits).
- Deterministic header->field mapping + safe fallback to model fields.
- Defensive serialization for Google Sheets (NaN/Inf => None, dict/list => JSON).
- Cached field resolution for speed (supports 59-column template).
"""

from __future__ import annotations

import json
import math
import re
from datetime import date, datetime
from decimal import Decimal
from functools import lru_cache
from typing import Any, Dict, List, Optional, Sequence, Tuple, Union

from pydantic import ConfigDict

from .data_engine_v2 import UnifiedQuote


# =============================================================================
# Header normalization
# =============================================================================

_ARABIC_DIGITS = str.maketrans("٠١٢٣٤٥٦٧٨٩", "0123456789")

@lru_cache(maxsize=4096)
def _normalize_header_name(name: str) -> str:
    """
    Normalize a header string into a canonical key:
    - Lowercase
    - Convert "%" into "percent"
    - Normalize Arabic digits
    - Remove brackets (), [], {}
    - Replace separators with underscores
    - Collapse repeated underscores
    """
    s = (name or "").strip()
    if not s:
        return ""

    s = s.translate(_ARABIC_DIGITS).lower()

    # Preserve percent meaning
    s = s.replace("%", " percent ")

    # Common replacements
    s = s.replace("&", " and ")
    s = s.replace("–", "-").replace("—", "-")

    # Remove brackets
    s = re.sub(r"[\(\)\[\]\{\}]", " ", s)

    # Normalize common tokens
    s = s.replace("52w", "52_week")
    s = s.replace("30d", "30_day")
    s = s.replace("ttm", "ttm")

    # Punctuations
    s = re.sub(r"[,:;]", " ", s)
    s = s.replace("/", " ").replace("\\", " ").replace("-", " ")

    # Collapse spaces -> underscore
    s = re.sub(r"\s+", "_", s)
    s = re.sub(r"_+", "_", s)
    return s.strip("_")


# =============================================================================
# Header -> Field mapping
# =============================================================================

# IMPORTANT:
# Keys MUST be normalized-form keys (output of _normalize_header_name).
# Values MUST be UnifiedQuote attribute names found in core/data_engine_v2.py.

_HEADER_FIELD_PAIRS: List[Tuple[str, str]] = [
    # -------------------------
    # Identity (59-col template)
    # -------------------------
    ("symbol", "symbol"),
    ("ticker", "symbol"),
    ("company_name", "name"),
    ("company", "name"),
    ("name", "name"),
    ("sector", "sector"),
    ("sub_sector", "sub_sector"),
    ("subsector", "sub_sector"),
    ("industry", "industry"),
    ("market", "market"),
    ("exchange", "market"),
    ("currency", "currency"),
    ("listing_date", "listing_date"),

    # -------------------------
    # Prices (59-col template)
    # -------------------------
    ("last_price", "current_price"),
    ("current_price", "current_price"),
    ("price", "current_price"),
    ("previous_close", "previous_close"),
    ("prev_close", "previous_close"),
    ("open", "open"),
    ("day_high", "day_high"),
    ("day_low", "day_low"),
    ("high", "day_high"),
    ("low", "day_low"),
    ("price_change", "price_change"),
    ("change", "price_change"),
    ("percent_change", "percent_change"),
    ("change_percent", "percent_change"),
    ("change_percent_percent", "percent_change"),  # defensive (rare)
    ("high_52_week", "high_52w"),
    ("low_52_week", "low_52w"),
    ("52_week_high", "high_52w"),
    ("52_week_low", "low_52w"),
    ("52w_high", "high_52w"),
    ("52w_low", "low_52w"),
    ("52_week_position_percent", "position_52w_percent"),
    ("52w_position_percent", "position_52w_percent"),

    # -------------------------
    # Volume / Liquidity (59-col template)
    # -------------------------
    ("volume", "volume"),
    ("avg_volume_30_day", "avg_volume_30d"),
    ("avg_volume_30d", "avg_volume_30d"),
    ("avg_volume_30", "avg_volume_30d"),
    ("average_volume_30_day", "avg_volume_30d"),
    ("value_traded", "value_traded"),
    ("turnover_percent", "turnover_percent"),
    ("turnover", "turnover_percent"),
    ("liquidity_score", "liquidity_score"),

    # -------------------------
    # Capital structure (59-col template)
    # -------------------------
    ("shares_outstanding", "shares_outstanding"),
    ("free_float_percent", "free_float"),
    ("free_float", "free_float"),
    ("market_cap", "market_cap"),
    ("free_float_market_cap", "free_float_market_cap"),

    # -------------------------
    # Fundamentals / Ratios (59-col template)
    # -------------------------
    ("eps_ttm", "eps_ttm"),
    ("eps", "eps_ttm"),
    ("forward_eps", "forward_eps"),
    ("p_e_ttm", "pe_ttm"),
    ("pe_ttm", "pe_ttm"),
    ("p_e", "pe_ttm"),
    ("pe", "pe_ttm"),
    ("forward_p_e", "forward_pe"),
    ("forward_pe", "forward_pe"),
    ("p_b", "pb"),
    ("pb", "pb"),
    ("p_s", "ps"),
    ("ps", "ps"),
    ("ev_ebitda", "ev_ebitda"),
    ("ev_to_ebitda", "ev_ebitda"),
    ("dividend_yield_percent", "dividend_yield"),
    ("dividend_yield", "dividend_yield"),
    ("dividend_rate", "dividend_rate"),
    ("payout_ratio_percent", "payout_ratio"),
    ("payout_ratio", "payout_ratio"),
    ("roe_percent", "roe"),
    ("roe", "roe"),
    ("roa_percent", "roa"),
    ("roa", "roa"),
    ("net_margin_percent", "net_margin"),
    ("net_margin", "net_margin"),
    ("ebitda_margin_percent", "ebitda_margin"),
    ("ebitda_margin", "ebitda_margin"),
    ("revenue_growth_percent", "revenue_growth"),
    ("revenue_growth", "revenue_growth"),
    ("net_income_growth_percent", "net_income_growth"),
    ("net_income_growth", "net_income_growth"),
    ("beta", "beta"),
    ("volatility_30_day", "volatility_30d"),
    ("volatility_30d", "volatility_30d"),

    # -------------------------
    # Technicals
    # -------------------------
    ("rsi_14", "rsi_14"),
    ("rsi_14_day", "rsi_14"),
    ("rsi", "rsi_14"),
    ("macd", "macd"),
    ("ma20", "ma20"),
    ("ma50", "ma50"),

    # -------------------------
    # Valuation / Scores
    # -------------------------
    ("fair_value", "fair_value"),
    ("upside_percent", "upside_percent"),
    ("upside", "upside_percent"),
    ("valuation_label", "valuation_label"),
    ("target_price", "target_price"),
    ("analyst_rating", "analyst_rating"),
    ("value_score", "value_score"),
    ("quality_score", "quality_score"),
    ("momentum_score", "momentum_score"),
    ("opportunity_score", "opportunity_score"),
    ("overall_score", "overall_score"),
    ("recommendation", "recommendation"),
    ("risk_score", "risk_score"),
    ("confidence", "confidence"),

    # -------------------------
    # Meta
    # -------------------------
    ("data_source", "data_source"),
    ("data_quality", "data_quality"),
    ("last_updated_utc", "last_updated_utc"),
    ("last_updated_riyadh", "last_updated_riyadh"),
    ("last_updated_local", "last_updated_riyadh"),
    ("last_updated", "last_updated_utc"),
    ("error", "error"),
]

_HEADER_FIELD_MAP: Dict[str, str] = dict(sorted(_HEADER_FIELD_PAIRS, key=lambda kv: kv[0]))


# =============================================================================
# Header -> field resolver
# =============================================================================

@lru_cache(maxsize=4096)
def _known_fields() -> frozenset[str]:
    try:
        return frozenset(UnifiedQuote.model_fields.keys())  # Pydantic v2
    except Exception:
        # ultra-defensive fallback
        return frozenset(dir(UnifiedQuote))

@lru_cache(maxsize=8192)
def _field_name_for_header(header: str) -> str:
    """
    Determine the UnifiedQuote/EnrichedQuote field name for a given header.

    Resolution order:
    1) Normalized header -> explicit mapping (_HEADER_FIELD_MAP)
    2) Normalized header matches an actual model field
    3) Heuristic cleanup (remove common suffixes) then check again
    """
    norm = _normalize_header_name(header)
    if not norm:
        return ""

    # 1) explicit mapping
    mapped = _HEADER_FIELD_MAP.get(norm)
    if mapped:
        return mapped

    fields = _known_fields()

    # 2) direct match to field
    if norm in fields:
        return norm

    # 3) heuristics: remove units / tokens
    # Example: "avg_volume_30_day" -> "avg_volume_30d" (already mapped, but keep)
    heuristic = norm
    heuristic = heuristic.replace("_percent", "")
    heuristic = heuristic.replace("_pct", "")
    heuristic = heuristic.replace("_usd", "").replace("_sar", "")
    heuristic = re.sub(r"_\d+_day$", "", heuristic)  # drop trailing period tokens
    heuristic = re.sub(r"_\d+d$", "", heuristic)

    if heuristic in fields:
        return heuristic

    return ""


# =============================================================================
# Serialization to Google Sheets-safe cells
# =============================================================================

def _safe_json_dumps(value: Any) -> str:
    try:
        return json.dumps(value, ensure_ascii=False, default=str)
    except Exception:
        return str(value)

def _serialize_value(value: Any) -> Any:
    """
    Prepare a value for Google Sheets:
    - datetime/date -> ISO string
    - Decimal -> float
    - NaN/Inf -> None
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

    # bytes -> string
    if isinstance(value, (bytes, bytearray)):
        try:
            return value.decode("utf-8", errors="ignore")
        except Exception:
            return str(value)

    return value


# =============================================================================
# EnrichedQuote model
# =============================================================================

class EnrichedQuote(UnifiedQuote):
    """
    Sheet-friendly view of UnifiedQuote.

    - Inherits ALL fields from UnifiedQuote.
    - Adds:
        • from_unified() -> create EnrichedQuote from a UnifiedQuote (or dict).
        • to_row(headers) -> map to Google Sheets row based on a header list.
    """

    model_config = ConfigDict(
        arbitrary_types_allowed=True,
        extra="allow",  # defensive: tolerate new provider keys without breaking
    )

    @classmethod
    def from_unified(cls, quote: Union[UnifiedQuote, Dict[str, Any]]) -> "EnrichedQuote":
        """Build EnrichedQuote from UnifiedQuote (or dict)."""
        if isinstance(quote, cls):
            return quote

        if isinstance(quote, dict):
            return cls(**quote)

        # Pydantic v2 objects have model_dump
        try:
            data = quote.model_dump()  # type: ignore[attr-defined]
        except Exception:
            # very defensive fallback
            data = {"symbol": getattr(quote, "symbol", "UNKNOWN"), "error": "Unable to convert quote to dict"}

        return cls(**data)

    def to_row(self, headers: Sequence[str]) -> List[Any]:
        """
        Convert this EnrichedQuote into a row aligned with the given headers.
        Unknown headers return None. Never raises.
        """
        row: List[Any] = []
        for header in headers:
            h = "" if header is None else str(header)
            field_name = _field_name_for_header(h)

            if field_name and hasattr(self, field_name):
                value = getattr(self, field_name, None)
            else:
                # fallback: if someone passed the raw field name as header
                raw = _normalize_header_name(h)
                value = getattr(self, raw, None) if raw and hasattr(self, raw) else None

            row.append(_serialize_value(value))

        return row


__all__ = ["EnrichedQuote", "_field_name_for_header", "_normalize_header_name"]
