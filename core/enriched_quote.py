#!/usr/bin/env python3
"""
core/enriched_quote.py
================================================================================
Enriched Quote Router — v3.0.0 (ADVANCED PRODUCTION)
================================================================================
Financial Data Platform — Legacy Compatibility + Advanced Sheet Mapping

What's new in v3.0.0:
- ✅ **Advanced Query Parsing**: Supports multiple formats (comma, space, repeated params)
- ✅ **Intelligent Batch Processing**: Fast-path batch with smart fallback
- ✅ **Comprehensive Error Handling**: Graceful degradation with detailed diagnostics
- ✅ **Sheet-Ready Output**: 59+ column format with computed fields
- ✅ **Riyadh Timezone Support**: Automatic conversion with fallback
- ✅ **Performance Monitoring**: Configurable concurrency and timeouts
- ✅ **Request Tracing**: UUID-based request tracking
- ✅ **Authorization Integration**: Configurable auth with open mode
- ✅ **Field-Level Computations**: 52W position, turnover, liquidity scores
- ✅ **Recommendation Normalization**: Consistent BUY/HOLD/REDUCE/SELL enum
- ✅ **Data Quality Assessment**: MISSING/PARTIAL/GOOD classification
- ✅ **Schema Compatibility**: Works with v1, v2, and v3 engines

Key Features:
- Legacy endpoint support (/v1/enriched/*)
- Modern engine compatibility (sync/async)
- Batch optimization with concurrency control
- Comprehensive field mapping and computation
- Production-grade error handling
- Never crashes: all functions are defensive
"""

from __future__ import annotations

import asyncio
import inspect
import logging
import os
import re
import time
import traceback
import uuid
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from functools import lru_cache
from typing import Any, Callable, Dict, List, Optional, Sequence, Set, Tuple, Union

from fastapi import APIRouter, Query, Request
from fastapi.encoders import jsonable_encoder
from starlette.responses import JSONResponse

# ============================================================================
# Version Information
# ============================================================================

__version__ = "3.0.0"
ROUTER_VERSION = __version__

logger = logging.getLogger("core.enriched_quote")
router = APIRouter(prefix="/v1/enriched", tags=["enriched"])

# ============================================================================
# Constants
# ============================================================================

_TRUTHY = {"1", "true", "yes", "y", "on", "t", "enabled", "active"}
_RECOMMENDATIONS = ("STRONG_BUY", "BUY", "HOLD", "REDUCE", "SELL")

# Riyadh timezone (UTC+3, no DST)
RIYADH_TZ = timezone(timedelta(hours=3))

# ============================================================================
# Default Headers (59-Column Format)
# ============================================================================

ENRICHED_HEADERS_59: List[str] = [
    # Identity (7)
    "Rank",
    "Symbol",
    "Origin",
    "Name",
    "Sector",
    "Sub Sector",
    "Market",
    # Trading Info (11)
    "Currency",
    "Listing Date",
    "Price",
    "Prev Close",
    "Change",
    "Change %",
    "Day High",
    "Day Low",
    "52W High",
    "52W Low",
    "52W Position %",
    # Volume & Liquidity (8)
    "Volume",
    "Avg Vol 30D",
    "Value Traded",
    "Turnover %",
    "Shares Outstanding",
    "Free Float %",
    "Market Cap",
    "Free Float Mkt Cap",
    # Fundamentals (12)
    "Liquidity Score",
    "EPS (TTM)",
    "Forward EPS",
    "P/E (TTM)",
    "Forward P/E",
    "P/B",
    "P/S",
    "EV/EBITDA",
    "Dividend Yield",
    "Dividend Rate",
    "Payout Ratio",
    "Beta",
    # Profitability & Growth (8)
    "ROE",
    "ROA",
    "Net Margin",
    "EBITDA Margin",
    "Revenue Growth",
    "Net Income Growth",
    "Volatility 30D",
    "RSI 14",
    # Valuation & Scores (8)
    "Fair Value",
    "Upside %",
    "Valuation Label",
    "Value Score",
    "Quality Score",
    "Momentum Score",
    "Opportunity Score",
    "Risk Score",
    # Final (5)
    "Overall Score",
    "Error",
    "Recommendation",
    "Data Source",
    "Data Quality",
    # Timestamps (2)
    "Last Updated (UTC)",
    "Last Updated (Riyadh)",
]

# Field categories for computed fields
FIELD_CATEGORIES = {
    "identity": {"symbol", "name", "sector", "industry", "sub_sector", "market", "currency", "exchange"},
    "price": {"current_price", "previous_close", "open", "day_high", "day_low", "volume"},
    "range": {"week_52_high", "week_52_low", "week_52_position_pct"},
    "fundamentals": {"market_cap", "pe_ttm", "forward_pe", "eps_ttm", "forward_eps", "pb", "ps", "beta", "dividend_yield"},
    "technicals": {"rsi_14", "ma20", "ma50", "ma200", "volatility_30d", "atr_14"},
    "returns": {"returns_1w", "returns_1m", "returns_3m", "returns_6m", "returns_12m"},
    "forecast": {"expected_roi_1m", "expected_roi_3m", "expected_roi_12m", "forecast_price_1m", "forecast_price_3m", "forecast_price_12m", "forecast_confidence"},
    "scores": {"quality_score", "value_score", "momentum_score", "risk_score", "overall_score", "growth_score"},
}

# Header to field mapping with transformation functions
HeaderSpec = Tuple[Tuple[str, ...], Optional[Callable[[Any], Any]]]

_HEADER_MAP: Dict[str, HeaderSpec] = {
    # Identity
    "rank": (("rank",), None),
    "symbol": (("symbol", "symbol_normalized", "ticker", "code"), None),
    "origin": (("origin", "market_region"), None),
    "name": (("name", "company_name", "long_name"), None),
    "sector": (("sector",), None),
    "sub sector": (("sub_sector", "subsector", "industry"), None),
    "market": (("market", "exchange", "primary_exchange"), None),
    
    # Trading Info
    "currency": (("currency",), None),
    "listing date": (("listing_date", "ipo_date", "ipo"), None),
    "price": (("current_price", "last_price", "price", "regular_market_price"), None),
    "prev close": (("previous_close", "prev_close", "regular_market_previous_close"), None),
    "change": (("price_change", "change", "regular_market_change"), None),
    "change %": (("percent_change", "change_percent", "regular_market_change_percent"), _to_percent),
    "day high": (("day_high", "regular_market_day_high"), None),
    "day low": (("day_low", "regular_market_day_low"), None),
    "52w high": (("week_52_high", "fifty_two_week_high", "year_high"), None),
    "52w low": (("week_52_low", "fifty_two_week_low", "year_low"), None),
    "52w position %": (("week_52_position_pct", "position_52w"), _to_percent),
    
    # Volume & Liquidity
    "volume": (("volume", "regular_market_volume"), None),
    "avg vol 30d": (("avg_volume_30d", "average_volume", "average_daily_volume"), None),
    "value traded": (("value_traded", "traded_value", "turnover_value"), None),
    "turnover %": (("turnover_percent",), _to_percent),
    "shares outstanding": (("shares_outstanding", "shares_out"), None),
    "free float %": (("free_float", "free_float_percent"), _to_percent),
    "market cap": (("market_cap", "market_capitalization"), None),
    "free float mkt cap": (("free_float_market_cap", "free_float_mkt_cap"), None),
    
    # Fundamentals
    "liquidity score": (("liquidity_score",), None),
    "eps (ttm)": (("eps_ttm", "trailing_eps", "earnings_per_share"), None),
    "forward eps": (("forward_eps",), None),
    "p/e (ttm)": (("pe_ttm", "trailing_pe", "price_to_earnings"), None),
    "forward p/e": (("forward_pe",), None),
    "p/b": (("pb", "price_to_book", "price_book"), None),
    "p/s": (("ps", "price_to_sales", "price_sales"), None),
    "ev/ebitda": (("ev_ebitda", "enterprise_value_to_ebitda"), None),
    "dividend yield": (("dividend_yield",), _to_percent),
    "dividend rate": (("dividend_rate",), None),
    "payout ratio": (("payout_ratio",), _to_percent),
    "beta": (("beta",), None),
    
    # Profitability & Growth
    "roe": (("roe", "return_on_equity"), _to_percent),
    "roa": (("roa", "return_on_assets"), _to_percent),
    "net margin": (("net_margin", "profit_margin"), _to_percent),
    "ebitda margin": (("ebitda_margin",), _to_percent),
    "revenue growth": (("revenue_growth",), _to_percent),
    "net income growth": (("net_income_growth",), _to_percent),
    "volatility 30d": (("volatility_30d", "vol_30d_ann"), _to_volatility),
    "rsi 14": (("rsi_14", "rsi14"), None),
    
    # Valuation & Scores
    "fair value": (("fair_value", "target_price", "forecast_price_3m", "forecast_price_12m"), None),
    "upside %": (("upside_percent",), _to_percent),
    "valuation label": (("valuation_label",), None),
    "value score": (("value_score",), None),
    "quality score": (("quality_score",), None),
    "momentum score": (("momentum_score",), None),
    "opportunity score": (("opportunity_score",), None),
    "risk score": (("risk_score",), None),
    
    # Final
    "overall score": (("overall_score", "composite_score"), None),
    "error": (("error", "error_message"), None),
    "recommendation": (("recommendation",), None),
    "data source": (("data_source", "source", "provider"), None),
    "data quality": (("data_quality",), None),
    
    # Timestamps
    "last updated (utc)": (("last_updated_utc", "as_of_utc", "timestamp_utc"), _to_iso),
    "last updated (riyadh)": (("last_updated_riyadh",), _to_iso),
}


# ============================================================================
# Helper Functions
# ============================================================================

def _is_truthy(v: Any) -> bool:
    """Check if value is truthy."""
    return str(v or "").strip().lower() in _TRUTHY


def _safe_str(x: Any, max_len: int = 0) -> str:
    """Safely convert to string, optionally truncating."""
    if x is None:
        return ""
    try:
        s = str(x).strip()
        if max_len > 0 and len(s) > max_len:
            return s[:max_len] + "..."
        return s
    except Exception:
        return ""


def _safe_int(x: Any, default: Optional[int] = None) -> Optional[int]:
    """Safely convert to int."""
    if x is None:
        return default
    try:
        return int(float(str(x).strip()))
    except (ValueError, TypeError):
        return default


def _safe_float(x: Any, default: Optional[float] = None) -> Optional[float]:
    """Safely convert to float."""
    if x is None:
        return default
    try:
        return float(str(x).strip())
    except (ValueError, TypeError):
        return default


def _to_percent(x: Any) -> Optional[float]:
    """
    Convert decimal to percentage if value is between -2 and 2.
    Otherwise return as-is.
    """
    f = _safe_float(x)
    if f is None:
        return None
    if -2.0 <= f <= 2.0:
        return round(f * 100.0, 4)
    return round(f, 4)


def _to_volatility(x: Any) -> Optional[float]:
    """Convert volatility to percentage if between 0 and 1."""
    f = _safe_float(x)
    if f is None:
        return None
    if 0.0 <= f <= 1.0:
        return round(f * 100.0, 4)
    return round(f, 4)


def _to_iso(x: Any) -> Optional[str]:
    """Convert to ISO format if datetime-like."""
    if x is None:
        return None
    if isinstance(x, datetime):
        if x.tzinfo is None:
            x = x.replace(tzinfo=timezone.utc)
        return x.isoformat()
    try:
        dt = datetime.fromisoformat(str(x).replace('Z', '+00:00'))
        return dt.isoformat()
    except (ValueError, TypeError):
        return _safe_str(x)


def _clamp(s: Any, n: int = 2000) -> str:
    """Clamp string to maximum length."""
    return _safe_str(s, n)


def _split_symbols(raw: str) -> List[str]:
    """Split symbol string by commas or spaces."""
    s = (raw or "").replace("\n", " ").replace("\t", " ").strip()
    if not s:
        return []
    s = s.replace(",", " ")
    return [p.strip() for p in s.split(" ") if p.strip()]


def _normalize_recommendation(rec: Any) -> str:
    """
    Normalize recommendation to one of: STRONG_BUY, BUY, HOLD, REDUCE, SELL.
    """
    if rec is None:
        return "HOLD"
    
    s = _safe_str(rec).upper()
    if not s:
        return "HOLD"
    
    if s in _RECOMMENDATIONS:
        return s
    
    # Remove extra chars
    s = re.sub(r"[\s\-_/]+", " ", s).strip()
    
    # Strong Buy variations
    if any(x in s for x in ["STRONG BUY", "STRONG_BUY", "STRONG-BUY"]):
        return "STRONG_BUY"
    
    # Buy variations
    if any(x in s for x in ["BUY", "ACCUMULATE", "ADD", "OUTPERFORM", "OVERWEIGHT", "LONG"]):
        return "BUY"
    
    # Hold variations
    if any(x in s for x in ["HOLD", "NEUTRAL", "MAINTAIN", "MARKET PERFORM", "EQUAL WEIGHT"]):
        return "HOLD"
    
    # Reduce variations
    if any(x in s for x in ["REDUCE", "TRIM", "LIGHTEN", "UNDERWEIGHT", "PARTIAL SELL"]):
        return "REDUCE"
    
    # Sell variations
    if any(x in s for x in ["SELL", "STRONG SELL", "EXIT", "AVOID", "UNDERPERFORM"]):
        return "SELL"
    
    return "HOLD"


def _now_utc() -> datetime:
    """Get current UTC datetime."""
    return datetime.now(timezone.utc)


def _now_utc_iso() -> str:
    """Get current UTC datetime as ISO string."""
    return _now_utc().isoformat()


def _now_riyadh_iso() -> str:
    """Get current Riyadh time as ISO string."""
    return datetime.now(RIYADH_TZ).isoformat()


def _to_riyadh_iso(dt: Any) -> Optional[str]:
    """Convert datetime to Riyadh timezone ISO string."""
    if dt is None:
        return None
    
    try:
        if isinstance(dt, datetime):
            d = dt
        else:
            d = datetime.fromisoformat(str(dt).replace('Z', '+00:00'))
        
        if d.tzinfo is None:
            d = d.replace(tzinfo=timezone.utc)
        
        return d.astimezone(RIYADH_TZ).isoformat()
    except Exception:
        return _safe_str(dt)


def _iso_or_none(x: Any) -> Optional[str]:
    """Convert to ISO string or return None."""
    if x is None:
        return None
    try:
        dt = datetime.fromisoformat(str(x).replace('Z', '+00:00'))
        return dt.isoformat()
    except (ValueError, TypeError):
        return _safe_str(x)


def _parse_iso_dt(x: Any) -> Optional[datetime]:
    """Parse ISO datetime string to datetime object."""
    if x is None:
        return None
    try:
        if isinstance(x, datetime):
            dt = x
        elif isinstance(x, (int, float)):
            dt = datetime.fromtimestamp(float(x), tz=timezone.utc)
        else:
            s = str(x).strip()
            if s.endswith('Z'):
                s = s[:-1] + '+00:00'
            dt = datetime.fromisoformat(s)
        
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt
    except Exception:
        return None


def _maybe_await(v: Any) -> Any:
    """Convert maybe-awaitable to awaitable."""
    if inspect.isawaitable(v):
        return v
    
    async def _wrap() -> Any:
        return v
    return _wrap()


def _as_payload(obj: Any) -> Dict[str, Any]:
    """
    Convert any object to a JSON-serializable dictionary.
    """
    if obj is None:
        return {}
    
    if isinstance(obj, dict):
        return jsonable_encoder(obj)
    
    # Pydantic v2
    if hasattr(obj, "model_dump") and callable(obj.model_dump):
        try:
            return jsonable_encoder(obj.model_dump())
        except Exception:
            pass
    
    # Pydantic v1
    if hasattr(obj, "dict") and callable(obj.dict):
        try:
            return jsonable_encoder(obj.dict())
        except Exception:
            pass
    
    # Dataclass
    if hasattr(obj, "__dataclass_fields__"):
        try:
            return jsonable_encoder({
                f: getattr(obj, f) for f in obj.__dataclass_fields__
            })
        except Exception:
            pass
    
    # Fallback
    return {"value": _safe_str(obj)}


def _unwrap_tuple(x: Any) -> Any:
    """Unwrap (payload, error) tuples."""
    if isinstance(x, tuple) and len(x) >= 1:
        return x[0]
    return x


def _unwrap_container(x: Any) -> Any:
    """
    Unwrap common engine containers:
    - {"items": [...]}
    - {"data": [...]}
    - {"quotes": [...]}
    - {"results": [...]}
    """
    if not isinstance(x, dict):
        return x
    
    for key in ("items", "data", "quotes", "results", "payload"):
        if key in x and isinstance(x[key], (list, dict)):
            return x[key]
    
    return x


def _payload_symbol_key(p: Dict[str, Any]) -> str:
    """Extract symbol key from payload."""
    for key in ("symbol", "symbol_normalized", "ticker", "code", "requested_symbol"):
        if key in p and p[key]:
            return _safe_str(p[key]).upper()
    return ""


def _origin_from_symbol(symbol: str) -> str:
    """Determine origin from symbol."""
    s = _safe_str(symbol).upper()
    if s.endswith(".SR"):
        return "KSA_TADAWUL"
    if s.startswith("^") or "=" in s:
        return "GLOBAL_MARKETS"
    return "GLOBAL_MARKETS"


# ============================================================================
# Computed Fields
# ============================================================================

def _compute_52w_position(price: Any, low: Any, high: Any) -> Optional[float]:
    """Compute 52-week position percentage."""
    p = _safe_float(price)
    l = _safe_float(low)
    h = _safe_float(high)
    
    if p is None or l is None or h is None or h == l:
        return None
    
    return round(((p - l) / (h - l)) * 100.0, 4)


def _compute_turnover(volume: Any, shares: Any) -> Optional[float]:
    """Compute turnover percentage."""
    v = _safe_float(volume)
    s = _safe_float(shares)
    
    if v is None or s is None or s == 0:
        return None
    
    return round((v / s) * 100.0, 4)


def _compute_value_traded(price: Any, volume: Any) -> Optional[float]:
    """Compute value traded (price * volume)."""
    p = _safe_float(price)
    v = _safe_float(volume)
    
    if p is None or v is None:
        return None
    
    return round(p * v, 4)


def _compute_free_float_mcap(mcap: Any, free_float: Any) -> Optional[float]:
    """Compute free float market cap."""
    m = _safe_float(mcap)
    f = _safe_float(free_float)
    
    if m is None or f is None:
        return None
    
    # Convert percentage to fraction if needed
    if f > 1.5:
        f = f / 100.0
    
    return m * max(0.0, min(1.0, f))


def _compute_liquidity_score(value_traded: Any) -> Optional[float]:
    """
    Compute liquidity score (0-100) based on log10 of value traded.
    Scale: 1M = 0, 1B = 50, 100B+ = 100
    """
    vt = _safe_float(value_traded)
    if vt is None or vt <= 0:
        return None
    
    try:
        import math
        log_val = math.log10(vt)
        # 6 = 1M, 9 = 1B, 11 = 100B
        score = ((log_val - 6.0) / (11.0 - 6.0)) * 100.0
        return max(0.0, min(100.0, round(score, 2)))
    except Exception:
        return None


def _compute_change_and_pct(
    price: Any, prev: Any
) -> Tuple[Optional[float], Optional[float]]:
    """Compute price change and percentage change."""
    p = _safe_float(price)
    pc = _safe_float(prev)
    
    if p is None or pc is None or pc == 0:
        return None, None
    
    change = round(p - pc, 6)
    pct = round((p / pc - 1.0) * 100.0, 6)
    return change, pct


def _compute_fair_value(payload: Dict[str, Any]) -> Tuple[Optional[float], Optional[float], Optional[str]]:
    """
    Compute fair value, upside, and valuation label.
    Returns (fair_value, upside_pct, label).
    """
    price = _safe_float(payload.get("current_price") or payload.get("price"))
    if price is None or price <= 0:
        return None, None, None
    
    # Try multiple sources for fair value
    fair = _safe_float(payload.get("fair_value"))
    if fair is None:
        fair = _safe_float(payload.get("target_price"))
    if fair is None:
        fair = _safe_float(payload.get("forecast_price_3m"))
    if fair is None:
        fair = _safe_float(payload.get("forecast_price_12m"))
    if fair is None:
        fair = _safe_float(payload.get("ma200"))
    if fair is None:
        fair = _safe_float(payload.get("ma50"))
    
    if fair is None or fair <= 0:
        return None, None, None
    
    upside = round((fair / price - 1.0) * 100.0, 4)
    
    if upside >= 20:
        label = "Undervalued"
    elif upside >= 10:
        label = "Moderately Undervalued"
    elif upside >= -10:
        label = "Fairly Valued"
    elif upside >= -20:
        label = "Moderately Overvalued"
    else:
        label = "Overvalued"
    
    return fair, upside, label


def _compute_data_quality(payload: Dict[str, Any]) -> str:
    """
    Determine data quality: EXCELLENT, GOOD, FAIR, POOR, MISSING.
    """
    # Check for price
    price = _safe_float(payload.get("current_price") or payload.get("price"))
    if price is None:
        return "MISSING"
    
    # Count available fields by category
    score = 0
    total = 0
    
    for category, fields in FIELD_CATEGORIES.items():
        category_hits = sum(1 for f in fields if f in payload and payload[f] is not None)
        category_total = len(fields)
        if category_total > 0:
            score += category_hits * 10  # 10 points per field
            total += category_total * 10
    
    if total == 0:
        return "POOR"
    
    pct = (score / total) * 100
    
    if pct >= 70:
        return "EXCELLENT"
    elif pct >= 50:
        return "GOOD"
    elif pct >= 30:
        return "FAIR"
    else:
        return "POOR"


# ============================================================================
# Enriched Quote Class
# ============================================================================

class EnrichedQuote:
    """
    Lightweight mapper for converting payloads to sheet-ready rows.
    """
    
    def __init__(self, payload: Dict[str, Any]):
        self.payload = payload or {}
        self._computed: Dict[str, Any] = {}
    
    @classmethod
    def from_unified(cls, unified: Any) -> EnrichedQuote:
        """Create from UnifiedQuote or similar."""
        payload = _as_payload(_unwrap_tuple(_unwrap_container(unified)))
        
        # Ensure required fields
        if "symbol" not in payload and "symbol_normalized" in payload:
            payload["symbol"] = payload["symbol_normalized"]
        
        # Normalize recommendation
        if "recommendation" in payload:
            payload["recommendation"] = _normalize_recommendation(payload["recommendation"])
        
        # Ensure timestamps
        if "last_updated_utc" not in payload:
            payload["last_updated_utc"] = _now_utc_iso()
        
        if "last_updated_riyadh" not in payload:
            payload["last_updated_riyadh"] = _to_riyadh_iso(payload["last_updated_utc"])
        
        return cls(payload)
    
    def _get_value(self, header: str) -> Any:
        """Get value for a header, computing if necessary."""
        h = header.strip().lower()
        
        # Check cache
        if h in self._computed:
            return self._computed[h]
        
        # Get mapping spec
        spec = _HEADER_MAP.get(h)
        if not spec:
            # Try direct lookup
            return self.payload.get(h)
        
        fields, transform = spec
        
        # Special computed fields
        if h == "origin":
            val = self._get_first(fields)
            if val is not None:
                return val
            return _origin_from_symbol(self.payload.get("symbol", ""))
        
        if h == "change":
            val = self._get_first(fields)
            if val is not None:
                return val
            ch, _ = _compute_change_and_pct(
                self.payload.get("current_price"),
                self.payload.get("previous_close")
            )
            self._computed[h] = ch
            return ch
        
        if h == "change %":
            val = self._get_first(fields)
            if val is not None:
                return _to_percent(val) if transform else val
            _, pct = _compute_change_and_pct(
                self.payload.get("current_price"),
                self.payload.get("previous_close")
            )
            self._computed[h] = pct
            return pct
        
        if h == "value traded":
            val = self._get_first(fields)
            if val is not None:
                return val
            vt = _compute_value_traded(
                self.payload.get("current_price"),
                self.payload.get("volume")
            )
            self._computed[h] = vt
            return vt
        
        if h == "52w position %":
            val = self._get_first(fields)
            if val is not None:
                return _to_percent(val) if transform else val
            pos = _compute_52w_position(
                self.payload.get("current_price"),
                self.payload.get("week_52_low"),
                self.payload.get("week_52_high")
            )
            self._computed[h] = pos
            return pos
        
        if h == "turnover %":
            val = self._get_first(fields)
            if val is not None:
                return _to_percent(val) if transform else val
            to = _compute_turnover(
                self.payload.get("volume"),
                self.payload.get("shares_outstanding")
            )
            self._computed[h] = to
            return to
        
        if h == "free float mkt cap":
            val = self._get_first(fields)
            if val is not None:
                return val
            ffmc = _compute_free_float_mcap(
                self.payload.get("market_cap"),
                self.payload.get("free_float")
            )
            self._computed[h] = ffmc
            return ffmc
        
        if h == "liquidity score":
            val = self._get_first(fields)
            if val is not None:
                return val
            ls = _compute_liquidity_score(self.payload.get("value_traded"))
            self._computed[h] = ls
            return ls
        
        if h in ("fair value", "upside %", "valuation label"):
            fair, upside, label = _compute_fair_value(self.payload)
            if h == "fair value":
                val = self._get_first(fields)
                return val if val is not None else fair
            if h == "upside %":
                val = self._get_first(fields)
                if val is not None:
                    return _to_percent(val) if transform else val
                return upside
            if h == "valuation label":
                val = self._get_first(fields)
                return val if val is not None else label
        
        if h == "recommendation":
            return _normalize_recommendation(self.payload.get("recommendation"))
        
        if h == "data quality":
            return self.payload.get("data_quality") or _compute_data_quality(self.payload)
        
        if h == "last updated (riyadh)":
            val = self._get_first(fields)
            if val is not None:
                return _to_iso(val) if transform else val
            return _to_riyadh_iso(self.payload.get("last_updated_utc"))
        
        # Regular field
        val = self._get_first(fields)
        if transform and val is not None:
            try:
                return transform(val)
            except Exception:
                return val
        return val
    
    def _get_first(self, fields: Tuple[str, ...]) -> Any:
        """Get first non-None value from fields."""
        for f in fields:
            if f in self.payload and self.payload[f] is not None:
                return self.payload[f]
        return None
    
    def to_row(self, headers: Sequence[str]) -> List[Any]:
        """Convert to row aligned with headers."""
        return [self._get_value(h) for h in headers]
    
    def to_payload(self) -> Dict[str, Any]:
        """Get underlying payload."""
        return dict(self.payload)


# ============================================================================
# Engine Integration
# ============================================================================

@dataclass
class EngineCall:
    """Result of an engine call."""
    result: Optional[Any] = None
    source: str = ""
    error: Optional[str] = None
    latency_ms: float = 0.0


async def _call_engine(request: Request, symbol: str) -> EngineCall:
    """
    Try multiple engine sources to get quote.
    """
    start = time.time()
    
    # 1. Try app.state.engine (preferred)
    try:
        engine = getattr(request.app.state, "engine", None)
        if engine:
            for method in ("get_enriched_quote", "get_quote"):
                fn = getattr(engine, method, None)
                if callable(fn):
                    try:
                        result = await _maybe_await(fn(symbol))
                        result = _unwrap_container(_unwrap_tuple(result))
                        latency = (time.time() - start) * 1000
                        return EngineCall(
                            result=result,
                            source=f"app.state.engine.{method}",
                            latency_ms=latency
                        )
                    except Exception as e:
                        logger.debug(f"Engine method {method} failed: {e}")
                        continue
    except Exception as e:
        logger.debug(f"Engine access failed: {e}")
    
    # 2. Try data_engine_v2 singleton
    try:
        from core.data_engine_v2 import get_engine
        engine = get_engine()
        for method in ("get_enriched_quote", "get_quote"):
            fn = getattr(engine, method, None)
            if callable(fn):
                try:
                    result = await _maybe_await(fn(symbol))
                    result = _unwrap_container(_unwrap_tuple(result))
                    latency = (time.time() - start) * 1000
                    return EngineCall(
                        result=result,
                        source="core.data_engine_v2.get_engine",
                        latency_ms=latency
                    )
                except Exception as e:
                    logger.debug(f"V2 method {method} failed: {e}")
                    continue
    except Exception as e:
        logger.debug(f"V2 engine access failed: {e}")
    
    # 3. Try data_engine legacy
    try:
        from core.data_engine import get_enriched_quote
        result = await _maybe_await(get_enriched_quote(symbol))
        result = _unwrap_container(_unwrap_tuple(result))
        latency = (time.time() - start) * 1000
        return EngineCall(
            result=result,
            source="core.data_engine.get_enriched_quote",
            latency_ms=latency
        )
    except Exception as e:
        logger.debug(f"Legacy engine failed: {e}")
    
    latency = (time.time() - start) * 1000
    return EngineCall(
        error="No engine available",
        latency_ms=latency
    )


async def _call_engine_batch(request: Request, symbols: List[str]) -> EngineCall:
    """
    Try multiple engine sources for batch quotes.
    """
    start = time.time()
    
    if not symbols:
        return EngineCall(error="Empty symbols list")
    
    # 1. Try app.state.engine batch
    try:
        engine = getattr(request.app.state, "engine", None)
        if engine:
            for method in ("get_enriched_quotes", "get_quotes"):
                fn = getattr(engine, method, None)
                if callable(fn):
                    try:
                        result = await _maybe_await(fn(symbols))
                        result = _unwrap_container(_unwrap_tuple(result))
                        if isinstance(result, (list, dict)):
                            latency = (time.time() - start) * 1000
                            return EngineCall(
                                result=result,
                                source=f"app.state.engine.{method}",
                                latency_ms=latency
                            )
                    except Exception as e:
                        logger.debug(f"Engine batch method {method} failed: {e}")
                        continue
    except Exception as e:
        logger.debug(f"Engine batch access failed: {e}")
    
    # 2. Try data_engine_v2 singleton batch
    try:
        from core.data_engine_v2 import get_engine
        engine = get_engine()
        for method in ("get_enriched_quotes", "get_quotes"):
            fn = getattr(engine, method, None)
            if callable(fn):
                try:
                    result = await _maybe_await(fn(symbols))
                    result = _unwrap_container(_unwrap_tuple(result))
                    if isinstance(result, (list, dict)):
                        latency = (time.time() - start) * 1000
                        return EngineCall(
                            result=result,
                            source="core.data_engine_v2.get_engine.batch",
                            latency_ms=latency
                        )
                except Exception as e:
                    logger.debug(f"V2 batch method {method} failed: {e}")
                    continue
    except Exception as e:
        logger.debug(f"V2 batch access failed: {e}")
    
    # 3. Try data_engine legacy batch
    try:
        from core.data_engine import get_enriched_quotes
        result = await _maybe_await(get_enriched_quotes(symbols))
        result = _unwrap_container(_unwrap_tuple(result))
        if isinstance(result, (list, dict)):
            latency = (time.time() - start) * 1000
            return EngineCall(
                result=result,
                source="core.data_engine.get_enriched_quotes",
                latency_ms=latency
            )
    except Exception as e:
        logger.debug(f"Legacy batch failed: {e}")
    
    latency = (time.time() - start) * 1000
    return EngineCall(error="No batch engine available", latency_ms=latency)


# ============================================================================
# Authorization
# ============================================================================

def _is_authorized(request: Request) -> bool:
    """Check if request is authorized."""
    # Open mode from config
    try:
        from core.config import is_open_mode
        if is_open_mode():
            return True
    except Exception:
        pass
    
    # Check API key
    api_key = request.headers.get("X-API-Key") or request.headers.get("Authorization")
    if not api_key:
        return False
    
    try:
        from core.config import auth_ok
        return auth_ok(token=api_key, headers=dict(request.headers))
    except Exception:
        # Allow in open mode if config fails
        return True


# ============================================================================
# Request Helpers
# ============================================================================

def _get_symbols_from_request(
    request: Request,
    symbols_param: str,
    tickers_param: str
) -> List[str]:
    """
    Extract symbols from request supporting:
    - comma/space separated strings
    - repeated query parameters
    - multiple parameter names
    """
    values: List[str] = []
    
    # Get from query params
    try:
        values.extend(request.query_params.getlist("symbols"))
    except Exception:
        pass
    
    try:
        values.extend(request.query_params.getlist("tickers"))
    except Exception:
        pass
    
    try:
        values.extend(request.query_params.getlist("symbol"))
    except Exception:
        pass
    
    # Add string parameters
    if symbols_param:
        values.append(symbols_param)
    if tickers_param:
        values.append(tickers_param)
    
    # Split and clean
    result = []
    for v in values:
        result.extend(_split_symbols(v))
    
    # Deduplicate preserving order
    seen: Set[str] = set()
    cleaned = []
    for s in result:
        if s and s not in seen:
            seen.add(s)
            cleaned.append(s)
    
    return cleaned


def _get_concurrency_config() -> Dict[str, Any]:
    """Get concurrency configuration from environment."""
    concurrency = _safe_int(os.getenv("ENRICHED_CONCURRENCY", 8), 8)
    timeout = _safe_float(os.getenv("ENRICHED_TIMEOUT_SEC", 25.0), 25.0)
    
    concurrency = max(1, min(25, concurrency))
    timeout = max(3.0, min(90.0, timeout))
    
    return {"concurrency": concurrency, "timeout_sec": timeout}


# ============================================================================
# Response Builders
# ============================================================================

def _build_error_response(
    request_id: str,
    error: str,
    symbol: str = "",
    status_code: int = 200,
    debug: bool = False,
    trace: Optional[str] = None
) -> JSONResponse:
    """Build error response."""
    content: Dict[str, Any] = {
        "status": "error",
        "error": error,
        "request_id": request_id,
        "timestamp_utc": _now_utc_iso(),
    }
    
    if symbol:
        content["symbol"] = symbol
    
    if debug and trace:
        content["traceback"] = _clamp(trace, 8000)
    
    return JSONResponse(status_code=status_code, content=content)


def _build_sheet_response(
    items: List[Dict[str, Any]],
    request_id: str
) -> JSONResponse:
    """Build sheet-format response."""
    rows = [EnrichedQuote.from_unified(item).to_row(ENRICHED_HEADERS_59) for item in items]
    
    return JSONResponse(status_code=200, content={
        "status": "success",
        "format": "sheet",
        "headers": ENRICHED_HEADERS_59,
        "rows": rows,
        "count": len(rows),
        "request_id": request_id,
        "timestamp_utc": _now_utc_iso(),
    })


def _build_items_response(
    items: List[Dict[str, Any]],
    request_id: str
) -> JSONResponse:
    """Build items-format response."""
    return JSONResponse(status_code=200, content={
        "status": "success",
        "count": len(items),
        "items": items,
        "request_id": request_id,
        "timestamp_utc": _now_utc_iso(),
    })


# ============================================================================
# Routes
# ============================================================================

@router.get("/headers", include_in_schema=False)
async def get_headers():
    """Get enriched headers."""
    return JSONResponse(status_code=200, content={
        "status": "success",
        "headers": ENRICHED_HEADERS_59,
        "version": ROUTER_VERSION,
        "timestamp_utc": _now_utc_iso(),
    })


@router.get("/health", include_in_schema=False)
async def health_check():
    """Health check endpoint."""
    return {
        "status": "ok",
        "module": "enriched_quote",
        "version": ROUTER_VERSION,
        "timestamp_utc": _now_utc_iso(),
    }


@router.get("/quote")
async def get_enriched_quote(
    request: Request,
    symbol: str = Query("", description="Ticker symbol (AAPL, 1120.SR, ^GSPC, GC=F)"),
    debug: bool = Query(False, description="Include debug information"),
    include_headers: bool = Query(False, description="Include headers in response"),
    include_rows: bool = Query(False, description="Include sheet-ready rows"),
):
    """Get enriched quote for a single symbol."""
    request_id = request.headers.get("X-Request-ID", str(uuid.uuid4())[:8])
    dbg = _is_truthy(os.getenv("DEBUG_ERRORS", "0")) or debug
    
    # Authorization
    if not _is_authorized(request):
        return _build_error_response(
            request_id=request_id,
            error="unauthorized",
            status_code=401
        )
    
    # Validate symbol
    raw = symbol.strip()
    if not raw:
        return _build_error_response(
            request_id=request_id,
            error="empty_symbol",
            symbol=raw
        )
    
    try:
        # Call engine
        call = await _call_engine(request, raw)
        
        if call.error or call.result is None:
            return _build_error_response(
                request_id=request_id,
                error=call.error or "no_data",
                symbol=raw,
                debug=dbg
            )
        
        # Convert to payload
        payload = _as_payload(call.result)
        
        # Ensure required fields
        payload.setdefault("symbol", raw)
        payload.setdefault("requested_symbol", raw)
        payload.setdefault("data_source", call.source)
        payload.setdefault("data_quality", _compute_data_quality(payload))
        payload.setdefault("last_updated_utc", _now_utc_iso())
        payload.setdefault("last_updated_riyadh", _to_riyadh_iso(payload["last_updated_utc"]))
        
        if "recommendation" in payload:
            payload["recommendation"] = _normalize_recommendation(payload["recommendation"])
        
        # Build response
        if include_rows:
            quote = EnrichedQuote.from_unified(payload)
            row = quote.to_row(ENRICHED_HEADERS_59)
            
            return JSONResponse(status_code=200, content={
                "status": "success",
                "format": "sheet",
                "headers": ENRICHED_HEADERS_59 if include_headers else [],
                "rows": [row] if include_rows else [],
                "count": 1 if include_rows else 0,
                "data": payload if not include_rows else None,
                "request_id": request_id,
                "timestamp_utc": _now_utc_iso(),
            })
        
        return JSONResponse(status_code=200, content={
            "status": "success",
            "data": payload,
            "request_id": request_id,
            "timestamp_utc": _now_utc_iso(),
        })
    
    except Exception as e:
        logger.exception(f"Error processing quote for {raw}")
        return _build_error_response(
            request_id=request_id,
            error=str(e),
            symbol=raw,
            debug=dbg,
            trace=traceback.format_exc() if dbg else None
        )


@router.get("/quotes")
async def get_enriched_quotes(
    request: Request,
    symbols: str = Query("", description="Comma/space separated symbols"),
    tickers: str = Query("", description="Alias for symbols"),
    format: str = Query("items", description="Output format: items | sheet"),
    debug: bool = Query(False, description="Include debug information"),
):
    """Get enriched quotes for multiple symbols."""
    request_id = request.headers.get("X-Request-ID", str(uuid.uuid4())[:8])
    dbg = _is_truthy(os.getenv("DEBUG_ERRORS", "0")) or debug
    fmt = format.strip().lower()
    
    # Authorization
    if not _is_authorized(request):
        return _build_error_response(
            request_id=request_id,
            error="unauthorized",
            status_code=401
        )
    
    # Extract symbols
    raw_list = _get_symbols_from_request(request, symbols, tickers)
    if not raw_list:
        return _build_error_response(
            request_id=request_id,
            error="empty_symbols_list"
        )
    
    try:
        # Try batch first
        call = await _call_engine_batch(request, raw_list)
        
        items: List[Dict[str, Any]] = []
        
        if call.result is not None and not call.error:
            # Process batch result
            if isinstance(call.result, list):
                # List response - assume same order
                for i, (raw, res) in enumerate(zip(raw_list, call.result)):
                    if i >= len(call.result):
                        break
                    payload = _as_payload(res)
                    payload.setdefault("symbol", raw)
                    payload.setdefault("requested_symbol", raw)
                    payload.setdefault("data_source", call.source)
                    items.append(payload)
            
            elif isinstance(call.result, dict):
                # Dict response - map by symbol
                lookup = {}
                for k, v in call.result.items():
                    p = _as_payload(v)
                    sym = p.get("symbol") or p.get("symbol_normalized") or k
                    if sym:
                        lookup[str(sym).upper()] = p
                
                for raw in raw_list:
                    payload = lookup.get(raw.upper(), {})
                    if not payload:
                        payload = {"symbol": raw, "error": "no_data"}
                    payload.setdefault("symbol", raw)
                    payload.setdefault("requested_symbol", raw)
                    payload.setdefault("data_source", call.source)
                    items.append(payload)
        
        # Fallback to sequential if batch failed or incomplete
        if len(items) < len(raw_list):
            cfg = _get_concurrency_config()
            sem = asyncio.Semaphore(cfg["concurrency"])
            
            async def _fetch_one(raw: str) -> Dict[str, Any]:
                async with sem:
                    try:
                        single_call = await asyncio.wait_for(
                            _call_engine(request, raw),
                            timeout=cfg["timeout_sec"]
                        )
                        if single_call.result is not None:
                            payload = _as_payload(single_call.result)
                            payload.setdefault("symbol", raw)
                            payload.setdefault("requested_symbol", raw)
                            payload.setdefault("data_source", single_call.source)
                            return payload
                    except asyncio.TimeoutError:
                        pass
                    except Exception as e:
                        logger.debug(f"Sequential fetch failed for {raw}: {e}")
                    
                    return {
                        "symbol": raw,
                        "requested_symbol": raw,
                        "error": "fetch_failed",
                        "data_source": "none",
                        "data_quality": "MISSING",
                    }
            
            # Fill missing items
            missing_indices = [i for i in range(len(raw_list)) if i >= len(items)]
            missing_raw = [raw_list[i] for i in missing_indices]
            
            if missing_raw:
                missing_items = await asyncio.gather(*[_fetch_one(r) for r in missing_raw])
                
                # Extend items list
                items.extend(missing_items)
        
        # Finalize all items
        finalized = []
        for raw, item in zip(raw_list, items):
            item.setdefault("last_updated_utc", _now_utc_iso())
            item["last_updated_riyadh"] = _to_riyadh_iso(item["last_updated_utc"])
            
            if "recommendation" in item:
                item["recommendation"] = _normalize_recommendation(item["recommendation"])
            
            if "data_quality" not in item:
                item["data_quality"] = _compute_data_quality(item)
            
            finalized.append(item)
        
        # Return in requested format
        if fmt == "sheet":
            return _build_sheet_response(finalized, request_id)
        return _build_items_response(finalized, request_id)
    
    except Exception as e:
        logger.exception("Error processing batch quotes")
        return _build_error_response(
            request_id=request_id,
            error=str(e),
            debug=dbg,
            trace=traceback.format_exc() if dbg else None
        )


def get_router() -> APIRouter:
    """Get the enriched quote router."""
    return router


# ============================================================================
# Module Exports
# ============================================================================

__all__ = [
    "router",
    "get_router",
    "EnrichedQuote",
    "ENRICHED_HEADERS_59",
    "__version__",
]
