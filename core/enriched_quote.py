# core/enriched_quote.py
"""
core/enriched_quote.py
===========================================================
EnrichedQuote Mapper + Sheets Row Builder – v2.3.0 (PROD SAFE)

Purpose
- Provide a stable, Sheets-friendly representation of UnifiedQuote.
- Convert UnifiedQuote -> EnrichedQuote (same field names).
- Convert EnrichedQuote -> Google Sheets row aligned to provided headers.

Design
- Defensive: never throws during row build.
- Header-driven: supports multiple header naming conventions.
- Sheets-safe values: None / NaN / inf -> "" (blank cell)
- No risky % conversions for price changes (providers already vary).
  Only conservative ratio->percent conversion is applied to:
    dividend_yield, payout_ratio, roe, roa, net_margin, ebitda_margin,
    revenue_growth, net_income_growth
"""

from __future__ import annotations

import math
import re
from typing import Any, Callable, Dict, List, Optional

from pydantic import BaseModel, ConfigDict

from core.data_engine_v2 import UnifiedQuote


ENRICHED_VERSION = "2.3.0"


# =============================================================================
# Helpers
# =============================================================================
def _is_bad_number(x: Any) -> bool:
    try:
        if isinstance(x, (int, float)):
            f = float(x)
            return math.isnan(f) or math.isinf(f)
    except Exception:
        return True
    return False


def _clean_value(x: Any) -> Any:
    """
    Google Sheets values API is happiest with:
      - str, int/float, bool, "" (blank)
    """
    if x is None:
        return ""
    if _is_bad_number(x):
        return ""
    return x


def _norm_header(h: str) -> str:
    """
    Normalize header labels to a compact key:
    - lowercase
    - remove non-alphanumeric
    Example: "Last Updated (Riyadh)" -> "lastupdatedriyadh"
    """
    s = (h or "").strip().lower()
    s = re.sub(r"[^a-z0-9]+", "", s)
    return s


def _snake_from_header(h: str) -> str:
    """
    Convert header label to snake_case candidate:
      "Last Updated (Riyadh)" -> "last_updated_riyadh"
    """
    raw = (h or "").strip().lower()
    raw = re.sub(r"[\(\)\[\]]", " ", raw)
    raw = re.sub(r"[^a-z0-9]+", "_", raw).strip("_")
    return raw


def _ratio_to_percent(v: Any) -> Optional[float]:
    """
    Conservative conversion for ratio-like fundamentals:
    - If provider returns 0.15 => 15.0
    - If provider returns 15.0 => 15.0
    """
    if v is None:
        return None
    try:
        f = float(v)
        if _is_bad_number(f):
            return None
        return f * 100.0 if -1.0 <= f <= 1.0 else f
    except Exception:
        return None


def _safe_upside(current: Any, target: Any) -> Optional[float]:
    try:
        c = float(current) if current is not None else None
        t = float(target) if target is not None else None
        if c is None or t is None or c == 0:
            return None
        if _is_bad_number(c) or _is_bad_number(t):
            return None
        return (t / c - 1.0) * 100.0
    except Exception:
        return None


# =============================================================================
# Model: EnrichedQuote
# =============================================================================
class EnrichedQuote(BaseModel):
    """
    Stable “public” quote shape for API + Google Sheets.

    Field names intentionally match UnifiedQuote (engine output).
    """
    model_config = ConfigDict(
        populate_by_name=True,
        from_attributes=True,
        validate_assignment=True,
        extra="ignore",
    )

    # Identity
    symbol: str
    name: Optional[str] = None
    sector: Optional[str] = None
    industry: Optional[str] = None
    sub_sector: Optional[str] = None
    market: str = "UNKNOWN"
    currency: Optional[str] = None
    listing_date: Optional[str] = None

    # Shares / Float / Cap
    shares_outstanding: Optional[float] = None
    free_float: Optional[float] = None
    market_cap: Optional[float] = None
    free_float_market_cap: Optional[float] = None

    # Prices
    current_price: Optional[float] = None
    previous_close: Optional[float] = None
    open: Optional[float] = None
    day_high: Optional[float] = None
    day_low: Optional[float] = None
    high_52w: Optional[float] = None
    low_52w: Optional[float] = None
    position_52w_percent: Optional[float] = None
    price_change: Optional[float] = None
    percent_change: Optional[float] = None

    # Volume / Liquidity
    volume: Optional[float] = None
    avg_volume_30d: Optional[float] = None
    value_traded: Optional[float] = None
    turnover_percent: Optional[float] = None
    liquidity_score: Optional[float] = None

    # Fundamentals
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
    debt_to_equity: Optional[float] = None
    current_ratio: Optional[float] = None
    quick_ratio: Optional[float] = None

    # Technicals
    rsi_14: Optional[float] = None
    volatility_30d: Optional[float] = None
    macd: Optional[float] = None
    ma20: Optional[float] = None
    ma50: Optional[float] = None

    # Valuation / Targets
    fair_value: Optional[float] = None
    target_price: Optional[float] = None
    upside_percent: Optional[float] = None
    valuation_label: Optional[str] = None
    analyst_rating: Optional[str] = None

    # Scores / Recommendation
    value_score: Optional[float] = None
    quality_score: Optional[float] = None
    momentum_score: Optional[float] = None
    opportunity_score: Optional[float] = None
    overall_score: Optional[float] = None
    recommendation: Optional[str] = None
    confidence: Optional[float] = None
    risk_score: Optional[float] = None

    # Meta
    data_source: str = "none"
    data_quality: str = "MISSING"
    last_updated_utc: str = ""
    last_updated_riyadh: str = ""
    error: Optional[str] = None

    # -------------------------------------------------------------------------
    # Constructors
    # -------------------------------------------------------------------------
    @classmethod
    def from_unified(cls, q: Any) -> "EnrichedQuote":
        """
        Accepts:
          - UnifiedQuote
          - EnrichedQuote
          - dict-like
          - object with attributes
        """
        if q is None:
            return cls(symbol="", data_quality="MISSING", error="Empty quote input")

        if isinstance(q, EnrichedQuote):
            return q

        data: Dict[str, Any] = {}
        try:
            if isinstance(q, UnifiedQuote):
                data = q.model_dump(exclude_none=False)
            elif isinstance(q, dict):
                data = dict(q)
            elif hasattr(q, "model_dump"):
                data = q.model_dump(exclude_none=False)  # type: ignore
            elif hasattr(q, "__dict__"):
                data = dict(getattr(q, "__dict__", {}) or {})
        except Exception:
            data = {}

        sym = (data.get("symbol") or data.get("ticker") or data.get("Symbol") or "").strip()
        if not sym and hasattr(q, "symbol"):
            try:
                sym = str(getattr(q, "symbol") or "").strip()
            except Exception:
                sym = ""

        obj = cls(**{**data, "symbol": sym})

        # Compute upside if missing (prefer target_price, else fair_value)
        if obj.upside_percent is None:
            tgt = obj.target_price if obj.target_price is not None else obj.fair_value
            obj.upside_percent = _safe_upside(obj.current_price, tgt)

        return obj

    # -------------------------------------------------------------------------
    # Sheets Row Builder
    # -------------------------------------------------------------------------
    def to_row(self, headers: List[str]) -> List[Any]:
        """
        Build a row aligned to an arbitrary headers list.
        Unknown headers => blank.
        Never raises.
        """
        try:
            d = self.model_dump(exclude_none=False)
        except Exception:
            d = dict(getattr(self, "__dict__", {}) or {})

        def g(*keys: str) -> Any:
            for k in keys:
                if k in d:
                    return d.get(k)
            return None

        # Conservative ratio->% conversions for specific fundamentals only
        def pct_field(key: str) -> Any:
            v = g(key)
            return _ratio_to_percent(v)

        mapping: Dict[str, Callable[[], Any]] = {
            # Identity
            "symbol": lambda: g("symbol"),
            "ticker": lambda: g("symbol"),
            "companyname": lambda: g("name"),
            "name": lambda: g("name"),
            "sector": lambda: g("sector"),
            "industry": lambda: g("industry"),
            "subsector": lambda: g("sub_sector"),
            "market": lambda: g("market"),
            "currency": lambda: g("currency"),
            "listingdate": lambda: g("listing_date"),

            # Shares / Float / Cap
            "sharesoutstanding": lambda: g("shares_outstanding"),
            "freefloat": lambda: g("free_float"),
            "marketcap": lambda: g("market_cap"),
            "freefloatmarketcap": lambda: g("free_float_market_cap"),

            # Prices
            "currentprice": lambda: g("current_price"),
            "lastprice": lambda: g("current_price"),
            "price": lambda: g("current_price"),
            "previousclose": lambda: g("previous_close"),
            "open": lambda: g("open"),
            "dayhigh": lambda: g("day_high"),
            "daylow": lambda: g("day_low"),
            "52whigh": lambda: g("high_52w"),
            "52wlow": lambda: g("low_52w"),
            "52wposition": lambda: g("position_52w_percent"),
            "52wpositionpercent": lambda: g("position_52w_percent"),
            "pricechange": lambda: g("price_change"),
            "percentchange": lambda: g("percent_change"),

            # Volume / Liquidity
            "volume": lambda: g("volume"),
            "avgvolume30d": lambda: g("avg_volume_30d"),
            "valuetraded": lambda: g("value_traded"),
            "turnover": lambda: g("turnover_percent"),
            "turnoverpercent": lambda: g("turnover_percent"),
            "liquidityscore": lambda: g("liquidity_score"),

            # Fundamentals
            "epsttm": lambda: g("eps_ttm"),
            "forwardeps": lambda: g("forward_eps"),
            "pettm": lambda: g("pe_ttm"),
            "forwardpe": lambda: g("forward_pe"),
            "pb": lambda: g("pb"),
            "ps": lambda: g("ps"),
            "evebitda": lambda: g("ev_ebitda"),
            "dividendyield": lambda: pct_field("dividend_yield"),
            "dividendrate": lambda: g("dividend_rate"),
            "payoutratio": lambda: pct_field("payout_ratio"),
            "roe": lambda: pct_field("roe"),
            "roa": lambda: pct_field("roa"),
            "netmargin": lambda: pct_field("net_margin"),
            "ebitdamargin": lambda: pct_field("ebitda_margin"),
            "revenuegrowth": lambda: pct_field("revenue_growth"),
            "netincomegrowth": lambda: pct_field("net_income_growth"),
            "beta": lambda: g("beta"),
            "debttoequity": lambda: g("debt_to_equity"),
            "currentratio": lambda: g("current_ratio"),
            "quickratio": lambda: g("quick_ratio"),

            # Technicals
            "rsi14": lambda: g("rsi_14"),
            "volatility30d": lambda: g("volatility_30d"),
            "macd": lambda: g("macd"),
            "ma20": lambda: g("ma20"),
            "ma50": lambda: g("ma50"),

            # Valuation / targets
            "fairvalue": lambda: g("fair_value"),
            "targetprice": lambda: g("target_price"),
            "upside": lambda: g("upside_percent") if g("upside_percent") is not None else _safe_upside(g("current_price"), g("target_price") or g("fair_value")),
            "upsidepercent": lambda: g("upside_percent") if g("upside_percent") is not None else _safe_upside(g("current_price"), g("target_price") or g("fair_value")),
            "valuationlabel": lambda: g("valuation_label"),
            "analystrating": lambda: g("analyst_rating"),

            # Scores / reco
            "valuescore": lambda: g("value_score"),
            "qualityscore": lambda: g("quality_score"),
            "momentumscore": lambda: g("momentum_score"),
            "opportunityscore": lambda: g("opportunity_score"),
            "overallscore": lambda: g("overall_score"),
            "recommendation": lambda: g("recommendation"),
            "confidence": lambda: g("confidence"),
            "riskscore": lambda: g("risk_score"),

            # Meta
            "datasource": lambda: g("data_source"),
            "dataquality": lambda: g("data_quality"),
            "lastupdatedutc": lambda: g("last_updated_utc"),
            "lastupdatedriyadh": lambda: g("last_updated_riyadh"),
            "error": lambda: g("error"),
        }

        row: List[Any] = []
        for h in (headers or []):
            key = _norm_header(h)

            # Small compatibility shims
            if key == "52wposition":
                key = "52wpositionpercent"
            if key == "turnover":
                key = "turnoverpercent"
            if key == "lastupdatedlocal":
                key = "lastupdatedriyadh"

            val: Any = ""
            try:
                fn = mapping.get(key)
                if fn is not None:
                    val = fn()
                else:
                    # Fallback: attempt snake_case lookup
                    val = d.get(_snake_from_header(h), "")
            except Exception:
                val = ""

            row.append(_clean_value(val))

        return row


__all__ = ["EnrichedQuote", "ENRICHED_VERSION"]
