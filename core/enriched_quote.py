# core/enriched_quote.py
"""
core/enriched_quote.py
===========================================================
EnrichedQuote Mapper + Sheets Row Builder – v2.2 (PROD SAFE)

Purpose
- Provide a stable, Sheets-friendly representation of UnifiedQuote.
- Convert UnifiedQuote -> EnrichedQuote (same field names).
- Convert EnrichedQuote -> Google Sheets row aligned to headers returned by:
    core.schemas.get_headers_for_sheet(sheet_name)

Design
- Defensive: never throws during row build.
- Header-driven: supports multiple header naming conventions (Symbol vs Ticker,
  Company Name vs Name, Last Price vs Current Price, etc.)
- Sheets-safe values: None / NaN / inf -> "" (blank cell)
"""

from __future__ import annotations

import math
import re
from typing import Any, Callable, Dict, List, Optional

from pydantic import BaseModel, ConfigDict, Field

from core.data_engine_v2 import UnifiedQuote


ENRICHED_VERSION = "2.2.0"


# =============================================================================
# Helpers
# =============================================================================
def _is_bad_number(x: Any) -> bool:
    try:
        if isinstance(x, (int, float)):
            return math.isnan(float(x)) or math.isinf(float(x))
    except Exception:
        return True
    return False


def _clean_value(x: Any) -> Any:
    """
    Google Sheets values API is happiest with:
      - str, int/float, bool, ""
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


def _pct(x: Any) -> Optional[float]:
    try:
        if x is None:
            return None
        v = float(x)
        if _is_bad_number(v):
            return None
        return v
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
    A stable “public” quote shape for API + Google Sheets.

    NOTE: Field names intentionally match UnifiedQuote (engine output).
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
    last_updated_utc: str = Field(default_factory=lambda: "")
    last_updated_riyadh: str = Field(default_factory=lambda: "")
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

        # Ensure symbol
        sym = (data.get("symbol") or data.get("ticker") or data.get("Symbol") or "").strip()
        if not sym and hasattr(q, "symbol"):
            try:
                sym = str(getattr(q, "symbol") or "").strip()
            except Exception:
                sym = ""

        obj = cls(**{**data, "symbol": sym})

        # Compute upside if missing (prefer fair_value, else target_price)
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
        """
        # Convenience locals (avoid getattr overhead)
        d = self.model_dump(exclude_none=False)

        # Header-key -> extractor
        def g(*keys: str) -> Any:
            for k in keys:
                if k in d:
                    return d.get(k)
            return None

        mapping: Dict[str, Callable[[], Any]] = {
            # Identity
            "symbol": lambda: g("symbol"),
            "ticker": lambda: g("symbol"),
            "companyname": lambda: g("name"),
            "name": lambda: g("name"),
            "sector": lambda: g("sector"),
            "industry": lambda: g("industry"),
            "subsector": lambda: g("sub_sector"),
            "subsectorname": lambda: g("sub_sector"),
            "market": lambda: g("market"),
            "currency": lambda: g("currency"),
            "listingdate": lambda: g("listing_date"),

            # Shares / float / cap
            "sharesoutstanding": lambda: g("shares_outstanding"),
            "freefloat": lambda: g("free_float"),
            "freefloatpercent": lambda: g("free_float"),
            "marketcap": lambda: g("market_cap"),
            "freefloatmarketcap": lambda: g("free_float_market_cap"),

            # Prices
            "currentprice": lambda: g("current_price"),
            "lastprice": lambda: g("current_price"),
            "price": lambda: g("current_price"),
            "previousclose": lambda: g("previous_close"),
            "open": lambda: g("open"),
            "dayhigh": lambda: g("day_high"),
            "high": lambda: g("day_high"),
            "daylow": lambda: g("day_low"),
            "low": lambda: g("day_low"),
            "52whigh": lambda: g("high_52w"),
            "yearhigh": lambda: g("high_52w"),
            "52wlow": lambda: g("low_52w"),
            "yearlow": lambda: g("low_52w"),
            "52wpositionpercent": lambda: g("position_52w_percent"),
            "position52wpercent": lambda: g("position_52w_percent"),
            "pricechange": lambda: g("price_change"),
            "change": lambda: g("price_change"),
            "percentchange": lambda: g("percent_change"),
            "changepercent": lambda: g("percent_change"),
            "change%": lambda: g("percent_change"),

            # Volume / Liquidity
            "volume": lambda: g("volume"),
            "avgvolume30d": lambda: g("avg_volume_30d"),
            "avgvolume": lambda: g("avg_volume_30d"),
            "valuetraded": lambda: g("value_traded"),
            "turnoverpercent": lambda: g("turnover_percent"),
            "turnover%": lambda: g("turnover_percent"),
            "liquidityscore": lambda: g("liquidity_score"),

            # Fundamentals
            "epsttm": lambda: g("eps_ttm"),
            "eps": lambda: g("eps_ttm"),
            "forwardeps": lambda: g("forward_eps"),
            "pettm": lambda: g("pe_ttm"),
            "pe": lambda: g("pe_ttm"),
            "forwardpe": lambda: g("forward_pe"),
            "pb": lambda: g("pb"),
            "ps": lambda: g("ps"),
            "evebitda": lambda: g("ev_ebitda"),
            "dividendyield": lambda: g("dividend_yield"),
            "dividendyieldpercent": lambda: g("dividend_yield"),
            "dividendrate": lambda: g("dividend_rate"),
            "payoutratio": lambda: g("payout_ratio"),
            "payoutratiopercent": lambda: g("payout_ratio"),
            "roe": lambda: g("roe"),
            "roepercent": lambda: g("roe"),
            "roa": lambda: g("roa"),
            "roapercent": lambda: g("roa"),
            "netmargin": lambda: g("net_margin"),
            "netmarginpercent": lambda: g("net_margin"),
            "ebitdamargin": lambda: g("ebitda_margin"),
            "ebitdamarginpercent": lambda: g("ebitda_margin"),
            "revenuegrowth": lambda: g("revenue_growth"),
            "revenuegrowthpercent": lambda: g("revenue_growth"),
            "netincomegrowth": lambda: g("net_income_growth"),
            "netincomegrowthpercent": lambda: g("net_income_growth"),
            "beta": lambda: g("beta"),
            "debtequity": lambda: g("debt_to_equity"),
            "debttoequity": lambda: g("debt_to_equity"),
            "currentratio": lambda: g("current_ratio"),
            "quickratio": lambda: g("quick_ratio"),

            # Technicals
            "rsi14": lambda: g("rsi_14"),
            "rsi": lambda: g("rsi_14"),
            "volatility30d": lambda: g("volatility_30d"),
            "volatility": lambda: g("volatility_30d"),
            "macd": lambda: g("macd"),
            "ma20": lambda: g("ma20"),
            "ma50": lambda: g("ma50"),

            # Valuation / targets
            "fairvalue": lambda: g("fair_value"),
            "targetprice": lambda: g("target_price"),
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
            "lastupdated": lambda: g("last_updated_utc"),
            "lastupdatedutc": lambda: g("last_updated_utc"),
            "lastupdatedlocal": lambda: g("last_updated_riyadh"),
            "lastupdatedriyadh": lambda: g("last_updated_riyadh"),
            "error": lambda: g("error"),
        }

        row: List[Any] = []
        for h in (headers or []):
            key = _norm_header(h)

            # Also support patterns like "Change %" where normalization removes %
            if key not in mapping:
                # Try another normalization that keeps % removed but keyword preserved
                if "change" in key and "percent" in key:
                    key = "percentchange"

            val = ""
            try:
                fn = mapping.get(key)
                if fn is not None:
                    val = fn()
                else:
                    # Fallback: attempt snake_case attr from header
                    # e.g. "Last Updated (Riyadh)" -> last_updated_riyadh
                    raw = (h or "").strip().lower()
                    raw = re.sub(r"[\(\)\[\]]", " ", raw)
                    raw = re.sub(r"[^a-z0-9]+", "_", raw).strip("_")
                    val = d.get(raw, "")
            except Exception:
                val = ""

            row.append(_clean_value(val))

        return row


__all__ = ["EnrichedQuote", "ENRICHED_VERSION"]
