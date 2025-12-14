# core/enriched_quote.py
"""
core/enriched_quote.py
===============================================
Enriched Quote schema & Google Sheets Row Adapter - v1.8 (PROD SAFE)

Purpose
-------
- Keep backend + AI analysis + Google Sheets (59-col) perfectly aligned.
- Convert UnifiedQuote (or dict) -> EnrichedQuote
- Convert EnrichedQuote -> Google Sheets row based on a header list.

Key upgrades (v1.8)
-------------------
- Riyadh timezone fallback is UTC+3 (not UTC).
- Stronger ISO parsing (supports "Z", space separator, unix seconds).
- One-time model_dump() + merges Pydantic extras safely.
- Header normalization hardened for punctuation, Arabic digits, 52W/30D tokens.
- Alias lookup supports both engine naming styles (current_price vs last_price, etc.).
- Safe cell serialization for Sheets (NaN/Inf -> None, dict/list -> JSON, datetime -> ISO).
"""

from __future__ import annotations

import json
import math
import re
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from functools import lru_cache
from typing import Any, Dict, List, Optional, Sequence, Tuple, Union

from pydantic import ConfigDict

# ---- UnifiedQuote import (defensive) ----
try:
    from .data_engine_v2 import UnifiedQuote  # preferred
except Exception:  # pragma: no cover
    try:
        from pydantic import BaseModel as UnifiedQuote  # fallback
    except Exception:  # pragma: no cover
        class UnifiedQuote:  # type: ignore
            pass


# =============================================================================
# Timezone helpers
# =============================================================================

try:
    from zoneinfo import ZoneInfo  # py3.9+
    _RIYADH_TZ = ZoneInfo("Asia/Riyadh")
except Exception:  # pragma: no cover
    _RIYADH_TZ = timezone(timedelta(hours=3))  # ✅ last resort: UTC+3


def _parse_iso_dt(value: Any) -> Optional[datetime]:
    """Parse common ISO strings safely to datetime (UTC-aware if possible)."""
    if value is None:
        return None

    if isinstance(value, datetime):
        return value if value.tzinfo else value.replace(tzinfo=timezone.utc)

    # unix seconds (or ms if too large)
    if isinstance(value, (int, float)) and not math.isnan(float(value)) and not math.isinf(float(value)):
        try:
            v = float(value)
            # heuristic: ms timestamps
            if v > 2_000_000_000_000:
                v = v / 1000.0
            return datetime.fromtimestamp(v, tz=timezone.utc)
        except Exception:
            return None

    if not isinstance(value, str):
        return None

    s = value.strip()
    if not s:
        return None

    # normalize "Z"
    if s.endswith("Z"):
        s = s[:-1] + "+00:00"

    # support "YYYY-MM-DD HH:MM:SS" by converting space to "T"
    if " " in s and "T" not in s:
        s = s.replace(" ", "T", 1)

    try:
        dt = datetime.fromisoformat(s)
        return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)
    except Exception:
        return None


def _to_riyadh_iso(dt_utc: Optional[datetime]) -> Optional[str]:
    if not dt_utc:
        return None
    try:
        if dt_utc.tzinfo is None:
            dt_utc = dt_utc.replace(tzinfo=timezone.utc)
        return dt_utc.astimezone(_RIYADH_TZ).isoformat()
    except Exception:
        return None


# =============================================================================
# Header normalization
# =============================================================================

_ARABIC_DIGITS = str.maketrans("٠١٢٣٤٥٦٧٨٩", "0123456789")
_DASHES = {"–": "-", "—": "-", "−": "-"}

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
    for k, v in _DASHES.items():
        s = s.replace(k, v)

    s = s.replace("%", " percent ")
    s = s.replace("&", " and ")

    # remove brackets
    s = re.sub(r"[\(\)\[\]\{\}]", " ", s)

    # normalize common tokens
    s = s.replace("52w", "52_week")
    s = s.replace("30d", "30_day")
    s = s.replace("ttm", "ttm")

    # punctuation -> spaces
    s = re.sub(r"[,:;|]", " ", s)
    s = s.replace("/", " ").replace("\\", " ").replace("-", " ").replace(".", " ")

    # collapse spaces -> underscore
    s = re.sub(r"\s+", "_", s)
    s = re.sub(r"_+", "_", s)
    return s.strip("_")


# =============================================================================
# Header -> Field mapping
# =============================================================================

_HEADER_FIELD_PAIRS: List[Tuple[str, str]] = [
    # Identity
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

    # Prices
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
    ("high_52_week", "high_52w"),
    ("low_52_week", "low_52w"),
    ("52_week_high", "high_52w"),
    ("52_week_low", "low_52w"),
    ("52w_high", "high_52w"),
    ("52w_low", "low_52w"),
    ("52_week_position_percent", "position_52w_percent"),
    ("52w_position_percent", "position_52w_percent"),

    # Volume / Liquidity
    ("volume", "volume"),
    ("avg_volume_30_day", "avg_volume_30d"),
    ("avg_volume_30d", "avg_volume_30d"),
    ("avg_volume_30", "avg_volume_30d"),
    ("average_volume_30_day", "avg_volume_30d"),
    ("value_traded", "value_traded"),
    ("turnover_percent", "turnover_percent"),
    ("turnover", "turnover_percent"),
    ("liquidity_score", "liquidity_score"),

    # Capital structure
    ("shares_outstanding", "shares_outstanding"),
    ("free_float_percent", "free_float"),
    ("free_float", "free_float"),
    ("market_cap", "market_cap"),
    ("free_float_market_cap", "free_float_market_cap"),

    # Fundamentals / Ratios
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

    # Technicals
    ("rsi_14", "rsi_14"),
    ("rsi_14_day", "rsi_14"),
    ("rsi", "rsi_14"),
    ("macd", "macd"),
    ("ma20", "ma20"),
    ("ma50", "ma50"),

    # Valuation / Scores
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

    # Meta
    ("data_source", "data_source"),
    ("data_quality", "data_quality"),
    ("last_updated_utc", "last_updated_utc"),
    ("last_updated_riyadh", "last_updated_riyadh"),
    ("last_updated_local", "last_updated_riyadh"),
    ("last_updated", "last_updated_utc"),
    ("error", "error"),
]

_HEADER_FIELD_MAP: Dict[str, str] = dict(sorted(_HEADER_FIELD_PAIRS, key=lambda kv: kv[0]))

# Preferred field -> acceptable aliases in data dict
_FIELD_ALIASES: Dict[str, List[str]] = {
    "current_price": ["last_price", "price", "last", "close"],
    "price_change": ["change", "delta", "price_delta"],
    "percent_change": ["change_percent", "pct_change", "percent", "delta_percent"],
    "high_52w": ["high52w", "fifty_two_week_high", "52w_high"],
    "low_52w": ["low52w", "fifty_two_week_low", "52w_low"],
    "position_52w_percent": ["52w_position", "position_52w", "fifty_two_week_position_percent"],
    "last_updated_utc": ["updated_at_utc", "timestamp_utc", "last_update_utc", "last_updated"],
    "last_updated_riyadh": ["updated_at_riyadh", "timestamp_riyadh", "last_updated_local"],
}


@lru_cache(maxsize=4096)
def _field_name_for_header(header: str) -> str:
    norm = _normalize_header_name(header)
    if not norm:
        return ""
    return _HEADER_FIELD_MAP.get(norm) or norm


# =============================================================================
# Serialization to Google Sheets-safe cells
# =============================================================================

def _safe_json_dumps(value: Any) -> str:
    try:
        return json.dumps(value, ensure_ascii=False, default=str)
    except Exception:
        return str(value)


def _serialize_value(value: Any) -> Any:
    if value is None:
        return None

    if isinstance(value, bool):
        return True if value else False

    if isinstance(value, (datetime, date)):
        return value.isoformat()

    if isinstance(value, Decimal):
        try:
            return float(value)
        except Exception:
            return str(value)

    if isinstance(value, float):
        if math.isnan(value) or math.isinf(value):
            return None
        return value

    if isinstance(value, (dict, list, tuple)):
        return _safe_json_dumps(value)

    if isinstance(value, (bytes, bytearray)):
        try:
            return value.decode("utf-8", errors="ignore")
        except Exception:
            return str(value)

    return value


def _get_from_data(data: Dict[str, Any], key: str) -> Any:
    if not key:
        return None
    if key in data:
        return data.get(key)

    for alt in _FIELD_ALIASES.get(key, []):
        if alt in data:
            return data.get(alt)

    return None


# =============================================================================
# EnrichedQuote model
# =============================================================================

class EnrichedQuote(UnifiedQuote):
    """
    Sheet-friendly view of UnifiedQuote.
    """

    model_config = ConfigDict(
        arbitrary_types_allowed=True,
        extra="allow",
    )

    @classmethod
    def from_unified(cls, quote: Union["EnrichedQuote", UnifiedQuote, Dict[str, Any]]) -> "EnrichedQuote":
        if isinstance(quote, cls):
            return quote
        if isinstance(quote, dict):
            return cls(**quote)

        try:
            data = quote.model_dump()  # type: ignore[attr-defined]
        except Exception:
            data = dict(getattr(quote, "__dict__", {}) or {})
            data.setdefault("symbol", getattr(quote, "symbol", "UNKNOWN"))
            data.setdefault("error", "Unable to convert quote to dict")

        return cls(**data)

    def _dump_for_row(self) -> Dict[str, Any]:
        # One-time dump (includes standard fields)
        try:
            data: Dict[str, Any] = self.model_dump(exclude_none=False)  # type: ignore[attr-defined]
        except Exception:
            data = dict(getattr(self, "__dict__", {}) or {})

        # Merge Pydantic v2 extras if present (don’t overwrite explicit)
        extra = getattr(self, "__pydantic_extra__", None)
        if isinstance(extra, dict):
            for k, v in extra.items():
                data.setdefault(k, v)

        # Ensure symbol at least
        data.setdefault("symbol", getattr(self, "symbol", None) or "UNKNOWN")

        # Compute Riyadh timestamp if missing but UTC exists
        if not data.get("last_updated_riyadh"):
            dt_utc = _parse_iso_dt(
                data.get("last_updated_utc")
                or data.get("updated_at_utc")
                or data.get("timestamp_utc")
                or data.get("last_updated")
            )
            riy = _to_riyadh_iso(dt_utc)
            if riy:
                data["last_updated_riyadh"] = riy

        return data

    def to_row(self, headers: Sequence[str]) -> List[Any]:
        data = self._dump_for_row()
        row: List[Any] = []

        for header in headers:
            h = "" if header is None else str(header)
            field = _field_name_for_header(h)

            value = _get_from_data(data, field)

            if value is None:
                raw = _normalize_header_name(h)
                value = _get_from_data(data, raw)

            row.append(_serialize_value(value))

        return row


__all__ = ["EnrichedQuote", "_field_name_for_header", "_normalize_header_name"]
