#!/usr/bin/env python3
# core/enriched_quote.py
"""
================================================================================
Enriched Quote Router — v5.1.0 (GLOBAL-FIRST / ENGINE-LAZY / SHEET-STABLE)
================================================================================
Financial Data Platform — High-Performance Aggregation and Sheet Mapping

Aligned fixes for your current GLOBAL plan (EODHD as PRIMARY):
- ✅ Fixes “No engine available” by robust lazy engine init + caching in app.state.engine
- ✅ Strong canonical symbol output:
     - If request is "AAPL" => output stays "AAPL" (even if upstream uses "AAPL.US")
     - If KSA => always keeps ".SR"
- ✅ Sheet mapping updated to support new EODHD provider keys:
     - high_52w / low_52w / position_52w_pct
     - avg_vol_30d / volatility_30d
     - change / change_pct
- ✅ Smarter percent handling (avoids multiplying already-percent values like 1.2%)
- ✅ Better batch mapping and fallback logic
- ✅ Auth header compatibility: X-APP-TOKEN, X-API-Key, Authorization Bearer
- ✅ Production-safe error handling + optional debug traceback

Endpoints:
- /v1/enriched/quote
- /v1/enriched/quotes?format=items|sheet
- /v1/enriched/headers
- /v1/enriched/health
"""

from __future__ import annotations

import asyncio
import inspect
import logging
import math
import os
import re
import time
import traceback
import uuid
from datetime import datetime, timedelta, timezone
from typing import Any, Callable, Dict, List, Optional, Sequence, Set, Tuple

from fastapi import APIRouter, Query, Request
from fastapi.encoders import jsonable_encoder

# ---------------------------------------------------------------------------
# High-Performance JSON response
# ---------------------------------------------------------------------------
try:
    from fastapi.responses import ORJSONResponse as BestJSONResponse
except Exception:
    from starlette.responses import JSONResponse as BestJSONResponse  # type: ignore

# ---------------------------------------------------------------------------
# Monitoring & Tracing (optional)
# ---------------------------------------------------------------------------
try:
    from prometheus_client import Counter, Histogram  # type: ignore

    _PROMETHEUS_AVAILABLE = True
    router_requests_total = Counter(
        "router_enriched_requests_total",
        "Total requests to enriched router",
        ["endpoint", "status"],
    )
    router_request_duration = Histogram(
        "router_enriched_duration_seconds",
        "Router request duration",
        ["endpoint"],
    )
except Exception:
    _PROMETHEUS_AVAILABLE = False

    class DummyMetric:
        def labels(self, *args, **kwargs):  # noqa
            return self

        def inc(self, *args, **kwargs):  # noqa
            pass

        def observe(self, *args, **kwargs):  # noqa
            pass

    router_requests_total = DummyMetric()
    router_request_duration = DummyMetric()

try:
    from opentelemetry import trace  # type: ignore
    from opentelemetry.trace import Status, StatusCode  # type: ignore

    _OTEL_AVAILABLE = True
    tracer = trace.get_tracer(__name__)
except Exception:
    _OTEL_AVAILABLE = False

    class DummySpan:
        def set_attribute(self, *args, **kwargs):  # noqa
            pass

        def set_status(self, *args, **kwargs):  # noqa
            pass

        def __enter__(self):  # noqa
            return self

        def __exit__(self, *args, **kwargs):  # noqa
            pass

    class DummyTracer:
        def start_as_current_span(self, *args, **kwargs):  # noqa
            return DummySpan()

    tracer = DummyTracer()

# ---------------------------------------------------------------------------
# Symbol normalization (supports both repo layouts)
# ---------------------------------------------------------------------------
def _fallback_normalize_symbol(s: str) -> str:
    return (s or "").strip().upper()

try:
    from symbols.normalize import normalize_symbol as _normalize_symbol  # type: ignore
except Exception:
    try:
        from core.symbols.normalize import normalize_symbol as _normalize_symbol  # type: ignore
    except Exception:
        _normalize_symbol = _fallback_normalize_symbol  # type: ignore

__version__ = "5.1.0"
ROUTER_VERSION = __version__

logger = logging.getLogger("core.enriched_quote")
router = APIRouter(prefix="/v1/enriched", tags=["enriched"])

_TRUTHY = {"1", "true", "yes", "y", "on", "t", "enabled", "active"}
_RECOMMENDATIONS = ("STRONG_BUY", "BUY", "HOLD", "REDUCE", "SELL")
RIYADH_TZ = timezone(timedelta(hours=3))

# ============================================================================

ENRICHED_HEADERS_61: List[str] = [
    "Rank", "Symbol", "Origin", "Name", "Sector", "Sub Sector", "Market",
    "Currency", "Listing Date", "Price", "Prev Close", "Change", "Change %", "Day High", "Day Low",
    "52W High", "52W Low", "52W Position %",
    "Volume", "Avg Vol 30D", "Value Traded", "Turnover %", "Shares Outstanding", "Free Float %", "Market Cap", "Free Float Mkt Cap",
    "Liquidity Score", "EPS (TTM)", "Forward EPS", "P/E (TTM)", "Forward P/E", "P/B", "P/S", "EV/EBITDA",
    "Dividend Yield", "Dividend Rate", "Payout Ratio", "Beta",
    "ROE", "ROA", "Net Margin", "EBITDA Margin", "Revenue Growth", "Net Income Growth", "Volatility 30D", "RSI 14",
    "Fair Value", "Upside %", "Valuation Label", "Value Score", "Quality Score", "Momentum Score", "Opportunity Score", "Risk Score",
    "Overall Score", "Error", "Recommendation", "Data Source", "Data Quality",
    "Last Updated (UTC)", "Last Updated (Riyadh)",
]

# Backward alias (your older name)
ENRICHED_HEADERS_59 = ENRICHED_HEADERS_61

FIELD_CATEGORIES = {
    "identity": {"symbol", "name", "sector", "industry", "sub_sector", "market", "currency", "exchange"},
    "price": {"current_price", "previous_close", "day_open", "day_high", "day_low", "volume", "vwap"},
    # Support both old + new names
    "range": {"week_52_high", "week_52_low", "week_52_position_pct", "high_52w", "low_52w", "position_52w_pct"},
    "fundamentals": {"market_cap", "pe_ttm", "forward_pe", "eps_ttm", "forward_eps", "pb", "ps", "beta", "dividend_yield"},
    "technicals": {"rsi_14", "volatility_30d"},
    "forecast": {"expected_roi_1m", "expected_roi_3m", "expected_roi_12m", "forecast_price_1m", "forecast_price_3m", "forecast_price_12m", "forecast_confidence"},
    "scores": {"quality_score", "value_score", "momentum_score", "risk_score", "overall_score", "opportunity_score"},
}

HeaderSpec = Tuple[Tuple[str, ...], Optional[Callable[[Any], Any]]]

# ============================================================================

def _safe_str(x: Any, max_len: int = 0) -> str:
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
    if x is None:
        return default
    try:
        return int(float(str(x).strip()))
    except Exception:
        return default


def _safe_float(x: Any, default: Optional[float] = None) -> Optional[float]:
    if x is None:
        return default
    try:
        return float(str(x).strip())
    except Exception:
        return default


def _is_truthy(v: Any) -> bool:
    return str(v or "").strip().lower() in _TRUTHY


def _now_utc() -> datetime:
    return datetime.now(timezone.utc)


def _now_utc_iso() -> str:
    return _now_utc().isoformat()


def _now_riyadh_iso() -> str:
    return datetime.now(RIYADH_TZ).isoformat()


def _to_iso(x: Any) -> Optional[str]:
    if x is None:
        return None
    if isinstance(x, datetime):
        return x.replace(tzinfo=timezone.utc).isoformat() if x.tzinfo is None else x.isoformat()
    try:
        return datetime.fromisoformat(str(x).replace("Z", "+00:00")).isoformat()
    except Exception:
        return _safe_str(x)


def _to_riyadh_iso(dt: Any) -> Optional[str]:
    if dt is None:
        return None
    try:
        d = dt if isinstance(dt, datetime) else datetime.fromisoformat(str(dt).replace("Z", "+00:00"))
        if d.tzinfo is None:
            d = d.replace(tzinfo=timezone.utc)
        return d.astimezone(RIYADH_TZ).isoformat()
    except Exception:
        return _safe_str(dt)


def _to_percent_smart(x: Any) -> Optional[float]:
    """
    Smart percent:
    - if value looks like fraction (<=1 in abs) => *100
    - else treat as already-percent
    Works well for:
      - dividend_yield: 0.02 => 2.0
      - ROE: 0.15 => 15.0
      - change_pct: 1.2 => 1.2 (already percent)
    """
    f = _safe_float(x)
    if f is None:
        return None
    if -1.0 <= f <= 1.0:
        return round(f * 100.0, 6)
    return round(f, 6)


def _to_volatility_smart(x: Any) -> Optional[float]:
    f = _safe_float(x)
    if f is None:
        return None
    # If it looks like fraction volatility, convert to %
    if 0.0 <= f <= 1.0:
        return round(f * 100.0, 6)
    return round(f, 6)


def _split_symbols(raw: str) -> List[str]:
    s = (raw or "").replace("\n", " ").replace("\t", " ").strip().replace(",", " ")
    return [p.strip() for p in s.split(" ") if p.strip()]


def _normalize_recommendation(rec: Any) -> str:
    if rec is None:
        return "HOLD"
    s = _safe_str(rec).upper()
    if not s:
        return "HOLD"
    if s in _RECOMMENDATIONS:
        return s
    s2 = re.sub(r"[\s\-_/]+", " ", s).strip()
    if "STRONG BUY" in s2:
        return "STRONG_BUY"
    if any(x in s2 for x in ["BUY", "ACCUMULATE", "ADD", "OUTPERFORM", "OVERWEIGHT", "LONG"]):
        return "BUY"
    if any(x in s2 for x in ["HOLD", "NEUTRAL", "MAINTAIN", "MARKET PERFORM", "EQUAL WEIGHT"]):
        return "HOLD"
    if any(x in s2 for x in ["REDUCE", "TRIM", "LIGHTEN", "UNDERWEIGHT", "PARTIAL SELL"]):
        return "REDUCE"
    if any(x in s2 for x in ["SELL", "STRONG SELL", "EXIT", "AVOID", "UNDERPERFORM"]):
        return "SELL"
    return "HOLD"


def _as_payload(obj: Any) -> Dict[str, Any]:
    if obj is None:
        return {}
    if isinstance(obj, dict):
        return jsonable_encoder(obj)
    # pydantic v2
    if hasattr(obj, "model_dump") and callable(obj.model_dump):
        try:
            return jsonable_encoder(obj.model_dump())
        except Exception:
            pass
    # pydantic v1
    if hasattr(obj, "dict") and callable(obj.dict):
        try:
            return jsonable_encoder(obj.dict())
        except Exception:
            pass
    return jsonable_encoder({"value": _safe_str(obj)})


def _unwrap_tuple(x: Any) -> Any:
    return x[0] if isinstance(x, tuple) and len(x) >= 1 else x


def _unwrap_container(x: Any) -> Any:
    if not isinstance(x, dict):
        return x
    for key in ("items", "data", "quotes", "results", "payload"):
        if key in x and isinstance(x[key], (list, dict)):
            return x[key]
    return x


def _is_ksa_symbol(s: str) -> bool:
    u = _safe_str(s).upper()
    if not u:
        return False
    if u.startswith("TADAWUL:"):
        u = u.split(":", 1)[1].strip()
    if u.endswith(".SR"):
        code = u[:-3].strip()
        return code.isdigit() and 3 <= len(code) <= 6
    return u.isdigit() and 3 <= len(u) <= 6


def _canonical_output_symbol(raw_symbol: str, payload: Dict[str, Any]) -> str:
    """
    Strong canonical output rules:
    - If request is KSA code => output as NNNN.SR
    - If request is GLOBAL plain ticker => output stays raw upper (AAPL), even if upstream uses AAPL.US
    - If request includes dot (except SR rules) => keep normalized of raw
    """
    raw = _safe_str(raw_symbol).upper()
    if not raw:
        # fallback to payload
        return _safe_str(payload.get("symbol_normalized") or payload.get("symbol") or "").upper()

    # KSA normalization (force .SR)
    if _is_ksa_symbol(raw):
        if raw.endswith(".SR"):
            return raw
        if raw.isdigit():
            return f"{raw}.SR"
        # if it came like TADAWUL:1120
        if raw.startswith("TADAWUL:"):
            code = raw.split(":", 1)[1].strip()
            if code.isdigit():
                return f"{code}.SR"

    # GLOBAL: if raw has no dot => keep raw (AAPL) even if payload has AAPL.US
    if "." not in raw:
        return raw

    # Otherwise, normalize raw
    try:
        return _normalize_symbol(raw)
    except Exception:
        return raw


def _origin_from_symbol(symbol: str) -> str:
    s = _safe_str(symbol).upper()
    if s.endswith(".SR"):
        return "KSA_TADAWUL"
    return "GLOBAL_MARKETS"


def _compute_52w_position(price: Any, low: Any, high: Any) -> Optional[float]:
    p, l, h = _safe_float(price), _safe_float(low), _safe_float(high)
    if p is None or l is None or h is None or h == l:
        return None
    return round(((p - l) / (h - l)) * 100.0, 6)


def _compute_turnover(volume: Any, shares: Any) -> Optional[float]:
    v, s = _safe_float(volume), _safe_float(shares)
    if v is None or s is None or s == 0:
        return None
    return round((v / s) * 100.0, 6)


def _compute_value_traded(price: Any, volume: Any) -> Optional[float]:
    p, v = _safe_float(price), _safe_float(volume)
    if p is None or v is None:
        return None
    return round(p * v, 6)


def _compute_free_float_mcap(mcap: Any, free_float: Any) -> Optional[float]:
    m, f = _safe_float(mcap), _safe_float(free_float)
    if m is None or f is None:
        return None
    if f > 1.5:  # percent
        f = f / 100.0
    return m * max(0.0, min(1.0, f))


def _compute_liquidity_score(value_traded: Any) -> Optional[float]:
    vt = _safe_float(value_traded)
    if vt is None or vt <= 0:
        return None
    try:
        log_val = math.log10(vt)
        score = ((log_val - 6.0) / (11.0 - 6.0)) * 100.0
        return max(0.0, min(100.0, round(score, 2)))
    except Exception:
        return None


def _compute_change_and_pct(price: Any, prev: Any) -> Tuple[Optional[float], Optional[float]]:
    p, pc = _safe_float(price), _safe_float(prev)
    if p is None or pc is None or pc == 0:
        return None, None
    return round(p - pc, 8), round((p / pc - 1.0) * 100.0, 8)


def _compute_fair_value(payload: Dict[str, Any]) -> Tuple[Optional[float], Optional[float], Optional[str]]:
    price = _safe_float(payload.get("current_price") or payload.get("price"))
    if price is None or price <= 0:
        return None, None, None

    fair = (
        _safe_float(payload.get("fair_value"))
        or _safe_float(payload.get("target_price"))
        or _safe_float(payload.get("forecast_price_3m"))
        or _safe_float(payload.get("forecast_price_12m"))
        or _safe_float(payload.get("ma200"))
        or _safe_float(payload.get("ma50"))
    )
    if fair is None or fair <= 0:
        return None, None, None

    upside = round((fair / price - 1.0) * 100.0, 6)
    label = (
        "Undervalued" if upside >= 20 else
        "Moderately Undervalued" if upside >= 10 else
        "Fairly Valued" if upside >= -10 else
        "Moderately Overvalued" if upside >= -20 else
        "Overvalued"
    )
    return fair, upside, label


def _compute_data_quality(payload: Dict[str, Any]) -> str:
    if _safe_float(payload.get("current_price") or payload.get("price")) is None:
        return "MISSING"
    score, total = 0, 0
    for _, fields in FIELD_CATEGORIES.items():
        hits = sum(1 for f in fields if f in payload and payload[f] is not None)
        tot = len(fields)
        if tot > 0:
            score += hits
            total += tot
    if total == 0:
        return "POOR"
    pct = (score / total) * 100.0
    if pct >= 70:
        return "EXCELLENT"
    if pct >= 50:
        return "GOOD"
    if pct >= 30:
        return "FAIR"
    return "POOR"


# ---------------------------------------------------------------------------
# Header mapping (UPDATED for EODHD v4.1.0 keys)
# ---------------------------------------------------------------------------
_HEADER_MAP: Dict[str, HeaderSpec] = {
    "rank": (("rank",), None),
    "symbol": (("symbol", "symbol_normalized", "ticker", "code"), None),
    "origin": (("origin", "market_region"), None),
    "name": (("name", "company_name", "long_name"), None),
    "sector": (("sector",), None),
    "sub sector": (("sub_sector", "subsector", "industry"), None),
    "market": (("market", "exchange", "primary_exchange"), None),

    "currency": (("currency",), None),
    "listing date": (("listing_date", "ipo_date", "ipo"), None),

    "price": (("current_price", "last_price", "price", "regular_market_price"), None),
    "prev close": (("previous_close", "prev_close", "regular_market_previous_close"), None),

    # change: prefer provider change; else compute from price/prev_close
    "change": (("change", "price_change", "regular_market_change"), None),

    # change %: accept change_pct first
    "change %": (("change_pct", "percent_change", "change_percent", "regular_market_change_percent"), _to_percent_smart),

    "day high": (("day_high", "regular_market_day_high"), None),
    "day low": (("day_low", "regular_market_day_low"), None),

    # 52W (support both old + new)
    "52w high": (("high_52w", "week_52_high", "fifty_two_week_high", "year_high"), None),
    "52w low": (("low_52w", "week_52_low", "fifty_two_week_low", "year_low"), None),
    "52w position %": (("position_52w_pct", "week_52_position_pct", "position_52w"), _to_percent_smart),

    "volume": (("volume", "regular_market_volume"), None),
    "avg vol 30d": (("avg_vol_30d", "avg_volume_30d", "average_volume", "average_daily_volume"), None),
    "value traded": (("value_traded", "traded_value", "turnover_value"), None),
    "turnover %": (("turnover_percent", "turnover_pct"), _to_percent_smart),
    "shares outstanding": (("shares_outstanding", "shares_out"), None),
    "free float %": (("free_float", "free_float_percent"), _to_percent_smart),
    "market cap": (("market_cap", "market_capitalization"), None),
    "free float mkt cap": (("free_float_market_cap", "free_float_mkt_cap"), None),

    "liquidity score": (("liquidity_score",), None),

    "eps (ttm)": (("eps_ttm", "trailing_eps", "earnings_per_share"), None),
    "forward eps": (("forward_eps",), None),
    "p/e (ttm)": (("pe_ttm", "trailing_pe", "price_to_earnings"), None),
    "forward p/e": (("forward_pe",), None),
    "p/b": (("pb", "price_to_book", "price_book"), None),
    "p/s": (("ps", "price_to_sales", "price_sales"), None),
    "ev/ebitda": (("ev_ebitda", "enterprise_value_to_ebitda"), None),

    "dividend yield": (("dividend_yield",), _to_percent_smart),
    "dividend rate": (("dividend_rate",), None),
    "payout ratio": (("payout_ratio",), _to_percent_smart),
    "beta": (("beta",), None),

    "roe": (("roe", "return_on_equity"), _to_percent_smart),
    "roa": (("roa", "return_on_assets"), _to_percent_smart),
    "net margin": (("net_margin", "profit_margin"), _to_percent_smart),
    "ebitda margin": (("ebitda_margin",), _to_percent_smart),
    "revenue growth": (("revenue_growth",), _to_percent_smart),
    "net income growth": (("net_income_growth",), _to_percent_smart),
    "volatility 30d": (("volatility_30d", "vol_30d_ann"), _to_volatility_smart),
    "rsi 14": (("rsi_14", "rsi14"), None),

    "fair value": (("fair_value", "target_price", "forecast_price_3m", "forecast_price_12m"), None),
    "upside %": (("upside_percent", "upside_pct"), _to_percent_smart),
    "valuation label": (("valuation_label",), None),

    "value score": (("value_score",), None),
    "quality score": (("quality_score",), None),
    "momentum score": (("momentum_score",), None),
    "opportunity score": (("opportunity_score",), None),
    "risk score": (("risk_score",), None),
    "overall score": (("overall_score", "composite_score"), None),

    "error": (("error", "error_message", "error_detail"), None),
    "recommendation": (("recommendation",), None),
    "data source": (("data_source", "source", "provider"), None),
    "data quality": (("data_quality",), None),

    "last updated (utc)": (("last_updated_utc", "as_of_utc", "timestamp_utc"), _to_iso),
    "last updated (riyadh)": (("last_updated_riyadh",), _to_iso),
}

# ============================================================================

class EnrichedQuote:
    __slots__ = ("payload", "_computed")

    def __init__(self, payload: Dict[str, Any]):
        self.payload = payload or {}
        self._computed: Dict[str, Any] = {}

    @classmethod
    def from_unified(cls, unified: Any) -> "EnrichedQuote":
        payload = _as_payload(_unwrap_tuple(_unwrap_container(unified)))

        # Ensure canonical symbol behavior (router-level)
        requested = payload.get("requested_symbol") or payload.get("symbol") or ""
        output_symbol = _canonical_output_symbol(str(requested), payload)
        if output_symbol:
            payload["symbol_normalized"] = output_symbol
            payload["symbol"] = output_symbol

        if "recommendation" in payload:
            payload["recommendation"] = _normalize_recommendation(payload["recommendation"])

        payload.setdefault("last_updated_utc", _now_utc_iso())
        payload.setdefault("last_updated_riyadh", _to_riyadh_iso(payload["last_updated_utc"]))
        payload.setdefault("data_quality", _compute_data_quality(payload))
        return cls(payload)

    def _get_first(self, fields: Tuple[str, ...]) -> Any:
        for f in fields:
            if f in self.payload and self.payload[f] is not None:
                return self.payload[f]
        return None

    def _get_value(self, header: str) -> Any:
        h = header.strip().lower()
        if h in self._computed:
            return self._computed[h]

        spec = _HEADER_MAP.get(h)
        if not spec:
            return self.payload.get(h)

        fields, transform = spec

        if h == "origin":
            val = self._get_first(fields)
            return val if val is not None else _origin_from_symbol(self.payload.get("symbol", ""))

        # computed: change / pct
        if h == "change":
            val = self._get_first(fields)
            if val is not None:
                return val
            ch, _ = _compute_change_and_pct(
                self.payload.get("current_price") or self.payload.get("price"),
                self.payload.get("previous_close"),
            )
            self._computed[h] = ch
            return ch

        if h == "change %":
            val = self._get_first(fields)
            if val is not None:
                return transform(val) if transform else val
            _, pct = _compute_change_and_pct(
                self.payload.get("current_price") or self.payload.get("price"),
                self.payload.get("previous_close"),
            )
            self._computed[h] = pct
            return pct

        if h == "value traded":
            val = self._get_first(fields)
            if val is not None:
                return val
            vt = _compute_value_traded(
                self.payload.get("current_price") or self.payload.get("price"),
                self.payload.get("volume"),
            )
            if vt is not None:
                self.payload["value_traded"] = vt
            self._computed[h] = vt
            return vt

        if h == "52w position %":
            val = self._get_first(fields)
            if val is not None:
                return transform(val) if transform else val
            pos = _compute_52w_position(
                self.payload.get("current_price") or self.payload.get("price"),
                self.payload.get("low_52w") or self.payload.get("week_52_low"),
                self.payload.get("high_52w") or self.payload.get("week_52_high"),
            )
            self._computed[h] = pos
            return pos

        if h == "turnover %":
            val = self._get_first(fields)
            if val is not None:
                return transform(val) if transform else val
            to = _compute_turnover(self.payload.get("volume"), self.payload.get("shares_outstanding"))
            self._computed[h] = to
            return to

        if h == "free float mkt cap":
            val = self._get_first(fields)
            if val is not None:
                return val
            ffmc = _compute_free_float_mcap(self.payload.get("market_cap"), self.payload.get("free_float"))
            self._computed[h] = ffmc
            return ffmc

        if h == "liquidity score":
            val = self._get_first(fields)
            if val is not None:
                return val
            if self.payload.get("value_traded") is None:
                self.payload["value_traded"] = _compute_value_traded(
                    self.payload.get("current_price") or self.payload.get("price"),
                    self.payload.get("volume"),
                )
            ls = _compute_liquidity_score(self.payload.get("value_traded"))
            self._computed[h] = ls
            return ls

        if h in ("fair value", "upside %", "valuation label"):
            fair, upside, label = _compute_fair_value(self.payload)
            if h == "fair value":
                v = self._get_first(fields)
                return v if v is not None else fair
            if h == "upside %":
                v = self._get_first(fields)
                if v is not None:
                    return transform(v) if transform else v
                return upside
            if h == "valuation label":
                v = self._get_first(fields)
                return v if v is not None else label

        if h == "recommendation":
            return _normalize_recommendation(self.payload.get("recommendation"))

        if h == "data quality":
            return self.payload.get("data_quality") or _compute_data_quality(self.payload)

        if h == "last updated (riyadh)":
            val = self._get_first(fields)
            if val is not None:
                return transform(val) if transform else val
            return _to_riyadh_iso(self.payload.get("last_updated_utc"))

        val = self._get_first(fields)
        if transform and val is not None:
            try:
                return transform(val)
            except Exception:
                return val
        return val

    def to_row(self, headers: Sequence[str]) -> List[Any]:
        return [self._get_value(h) for h in headers]

# ============================================================================

class EngineCall:
    __slots__ = ("result", "source", "error", "latency_ms")

    def __init__(self, result: Optional[Any] = None, source: str = "", error: Optional[str] = None, latency_ms: float = 0.0):
        self.result = result
        self.source = source
        self.error = error
        self.latency_ms = latency_ms


async def _maybe_await(v: Any) -> Any:
    if inspect.isawaitable(v):
        return await v
    return v


async def _get_or_init_engine(request: Request) -> Tuple[Optional[Any], str, Optional[str]]:
    """
    Returns (engine, source, error).
    Caches engine in request.app.state.engine to prevent repeated imports.
    """
    # 1) already set on app.state
    try:
        engine = getattr(request.app.state, "engine", None)
        if engine is not None:
            return engine, "app.state.engine", None
    except Exception:
        pass

    # 2) load via core.data_engine_v2.get_engine
    try:
        from core.data_engine_v2 import get_engine  # type: ignore

        eng = get_engine()
        engine = await eng if inspect.isawaitable(eng) else eng
        try:
            request.app.state.engine = engine
        except Exception:
            pass
        return engine, "core.data_engine_v2.get_engine", None
    except Exception as e:
        return None, "core.data_engine_v2.get_engine", repr(e)


async def _call_engine(request: Request, symbol: str) -> EngineCall:
    start = time.time()

    engine, src, err = await _get_or_init_engine(request)
    if engine is None:
        return EngineCall(error="No engine available", source=src, latency_ms=(time.time() - start) * 1000)

    # Try standard methods
    for method in ("get_enriched_quote", "get_quote"):
        fn = getattr(engine, method, None)
        if callable(fn):
            try:
                res = await _maybe_await(fn(symbol))
                res = _unwrap_container(_unwrap_tuple(res))
                return EngineCall(result=res, source=f"{src}.{method}", latency_ms=(time.time() - start) * 1000)
            except Exception as e:
                err = repr(e)

    return EngineCall(error=err or "engine_call_failed", source=src, latency_ms=(time.time() - start) * 1000)


async def _call_engine_batch(request: Request, symbols: List[str]) -> EngineCall:
    start = time.time()

    engine, src, err = await _get_or_init_engine(request)
    if engine is None:
        return EngineCall(error="No batch engine available", source=src, latency_ms=(time.time() - start) * 1000)

    for method in ("get_enriched_quotes", "get_quotes"):
        fn = getattr(engine, method, None)
        if callable(fn):
            try:
                res = await _maybe_await(fn(symbols))
                res = _unwrap_container(_unwrap_tuple(res))
                if isinstance(res, (list, dict)):
                    return EngineCall(result=res, source=f"{src}.{method}", latency_ms=(time.time() - start) * 1000)
            except Exception as e:
                err = repr(e)

    return EngineCall(error=err or "engine_batch_call_failed", source=src, latency_ms=(time.time() - start) * 1000)

# ============================================================================

def _is_authorized(request: Request) -> bool:
    # Open mode shortcut (preferred)
    try:
        from core.config import is_open_mode  # type: ignore

        if callable(is_open_mode) and is_open_mode():
            return True
    except Exception:
        pass

    # Accept your header + common variants
    token = (
        request.headers.get("X-APP-TOKEN")
        or request.headers.get("X-App-Token")
        or request.headers.get("X-API-Key")
        or request.headers.get("X-Api-Key")
        or request.headers.get("Authorization")
    )
    if not token:
        return False

    # Use core.config.auth_ok if available
    try:
        from core.config import auth_ok  # type: ignore

        if callable(auth_ok):
            return bool(auth_ok(token=token, headers=dict(request.headers)))
    except Exception:
        # If REQUIRE_AUTH explicitly true and auth_ok missing => deny
        req = str(os.getenv("REQUIRE_AUTH", "")).strip().lower() in _TRUTHY
        return not req

    return True


def _get_symbols_from_request(request: Request, symbols_param: str, tickers_param: str) -> List[str]:
    values: List[str] = []
    for param in ("symbols", "tickers", "symbol"):
        try:
            values.extend(request.query_params.getlist(param))
        except Exception:
            pass
    if symbols_param:
        values.append(symbols_param)
    if tickers_param:
        values.append(tickers_param)

    out: List[str] = []
    for v in values:
        out.extend(_split_symbols(v))

    seen: Set[str] = set()
    dedup: List[str] = []
    for s in out:
        s = s.strip()
        if not s:
            continue
        if s in seen:
            continue
        seen.add(s)
        dedup.append(s)
    return dedup


def _get_concurrency_config() -> Dict[str, Any]:
    concurrency = max(1, min(50, _safe_int(os.getenv("ENRICHED_CONCURRENCY", 10), 10) or 10))
    timeout = max(3.0, min(90.0, _safe_float(os.getenv("ENRICHED_TIMEOUT_SEC", 30.0), 30.0) or 30.0))
    return {"concurrency": concurrency, "timeout_sec": timeout}


def _build_error_response(
    request_id: str,
    error: str,
    symbol: str = "",
    status_code: int = 200,
    debug: bool = False,
    trace: Optional[str] = None,
) -> BestJSONResponse:
    content: Dict[str, Any] = {
        "status": "error",
        "error": error,
        "request_id": request_id,
        "timestamp_utc": _now_utc_iso(),
        "timestamp_riyadh": _now_riyadh_iso(),
    }
    if symbol:
        content["symbol"] = symbol
    if debug and trace:
        content["traceback"] = _safe_str(trace, 8000)
    return BestJSONResponse(status_code=status_code, content=content)


def _build_sheet_response(items: List[Dict[str, Any]], request_id: str) -> BestJSONResponse:
    rows = [EnrichedQuote.from_unified(item).to_row(ENRICHED_HEADERS_61) for item in items]
    return BestJSONResponse(
        status_code=200,
        content={
            "status": "success",
            "format": "sheet",
            "headers": ENRICHED_HEADERS_61,
            "rows": rows,
            "count": len(rows),
            "request_id": request_id,
            "timestamp_utc": _now_utc_iso(),
        },
    )


def _build_items_response(items: List[Dict[str, Any]], request_id: str) -> BestJSONResponse:
    return BestJSONResponse(
        status_code=200,
        content={
            "status": "success",
            "count": len(items),
            "items": items,
            "request_id": request_id,
            "timestamp_utc": _now_utc_iso(),
        },
    )


def _canonicalize_payload(raw_symbol: str, payload: Dict[str, Any], source: str) -> Dict[str, Any]:
    payload = payload or {}

    raw = (raw_symbol or "").strip()
    payload["requested_symbol"] = raw

    out_symbol = _canonical_output_symbol(raw, payload)
    if out_symbol:
        payload["symbol_normalized"] = out_symbol
        payload["symbol"] = out_symbol

    # Source & timestamps
    payload.setdefault("data_source", source)
    payload.setdefault("last_updated_utc", _now_utc_iso())
    payload.setdefault("last_updated_riyadh", _to_riyadh_iso(payload["last_updated_utc"]))

    # Recommendation
    if "recommendation" in payload:
        payload["recommendation"] = _normalize_recommendation(payload.get("recommendation"))

    # Ensure origin field exists (useful for sheets)
    payload.setdefault("origin", _origin_from_symbol(payload.get("symbol", "")))

    # Ensure data_quality exists
    if not payload.get("data_quality"):
        payload["data_quality"] = _compute_data_quality(payload)

    return payload


# ============================================================================

@router.get("/headers", include_in_schema=False)
async def get_headers():
    return BestJSONResponse(
        status_code=200,
        content={
            "status": "success",
            "headers": ENRICHED_HEADERS_61,
            "version": ROUTER_VERSION,
            "timestamp_utc": _now_utc_iso(),
        },
    )


@router.get("/health", include_in_schema=False)
async def health_check():
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
    include_headers: bool = Query(False, description="Include headers in response (sheet mode)"),
    include_rows: bool = Query(False, description="Include sheet-ready rows"),
):
    request_id = request.headers.get("X-Request-ID", str(uuid.uuid4())[:8])
    dbg = _is_truthy(os.getenv("DEBUG_ERRORS", "0")) or debug

    with tracer.start_as_current_span("router_get_quote") as span:
        span.set_attribute("symbol", symbol)

        if not _is_authorized(request):
            router_requests_total.labels(endpoint="quote", status="unauthorized").inc()
            return _build_error_response(request_id=request_id, error="unauthorized", status_code=401)

        raw = (symbol or "").strip()
        if not raw:
            router_requests_total.labels(endpoint="quote", status="empty_symbol").inc()
            return _build_error_response(request_id=request_id, error="empty_symbol", symbol=raw)

        start_time = time.time()
        try:
            call = await _call_engine(request, raw)
            if call.error or call.result is None:
                router_requests_total.labels(endpoint="quote", status="no_data").inc()
                return _build_error_response(
                    request_id=request_id,
                    error=call.error or "no_data",
                    symbol=raw,
                    debug=dbg,
                    trace=traceback.format_exc() if dbg else None,
                )

            payload = _as_payload(call.result)
            payload = _canonicalize_payload(raw, payload, call.source)

            router_requests_total.labels(endpoint="quote", status="success").inc()
            router_request_duration.labels(endpoint="quote").observe(time.time() - start_time)

            if include_rows:
                q = EnrichedQuote.from_unified(payload)
                row = q.to_row(ENRICHED_HEADERS_61)
                return BestJSONResponse(
                    status_code=200,
                    content={
                        "status": "success",
                        "format": "sheet",
                        "headers": ENRICHED_HEADERS_61 if include_headers else [],
                        "rows": [row],
                        "count": 1,
                        "request_id": request_id,
                        "timestamp_utc": _now_utc_iso(),
                    },
                )

            return BestJSONResponse(
                status_code=200,
                content={
                    "status": "success",
                    "data": payload,
                    "request_id": request_id,
                    "timestamp_utc": _now_utc_iso(),
                },
            )

        except Exception as e:
            router_requests_total.labels(endpoint="quote", status="error").inc()
            logger.exception("Error processing /quote")
            return _build_error_response(
                request_id=request_id,
                error=str(e),
                symbol=raw,
                debug=dbg,
                trace=traceback.format_exc() if dbg else None,
            )


@router.get("/quotes")
async def get_enriched_quotes(
    request: Request,
    symbols: str = Query("", description="Comma/space separated symbols"),
    tickers: str = Query("", description="Alias for symbols"),
    format: str = Query("items", description="Output format: items | sheet"),
    debug: bool = Query(False, description="Include debug information"),
):
    request_id = request.headers.get("X-Request-ID", str(uuid.uuid4())[:8])
    dbg = _is_truthy(os.getenv("DEBUG_ERRORS", "0")) or debug
    fmt = (format or "").strip().lower()

    with tracer.start_as_current_span("router_get_quotes") as span:
        if not _is_authorized(request):
            router_requests_total.labels(endpoint="quotes", status="unauthorized").inc()
            return _build_error_response(request_id=request_id, error="unauthorized", status_code=401)

        raw_list = _get_symbols_from_request(request, symbols, tickers)
        if not raw_list:
            router_requests_total.labels(endpoint="quotes", status="empty_symbols_list").inc()
            return _build_error_response(request_id=request_id, error="empty_symbols_list")

        span.set_attribute("symbol_count", len(raw_list))
        start_time = time.time()

        try:
            call = await _call_engine_batch(request, raw_list)
            items: List[Dict[str, Any]] = []

            if call.result is not None and not call.error:
                if isinstance(call.result, list):
                    # Engine likely returns list in same order
                    for raw, res in zip(raw_list, call.result):
                        payload = _as_payload(res)
                        payload = _canonicalize_payload(raw, payload, call.source)
                        items.append(payload)

                elif isinstance(call.result, dict):
                    # Symbol-keyed dict (best-effort)
                    lookup: Dict[str, Dict[str, Any]] = {}
                    for k, v in call.result.items():
                        p = _as_payload(v)
                        sym = p.get("requested_symbol") or p.get("symbol_normalized") or p.get("symbol") or p.get("normalized_symbol") or k
                        if sym:
                            lookup[_canonical_output_symbol(str(sym), p)] = p
                    for raw in raw_list:
                        key = _canonical_output_symbol(raw, {})
                        payload = lookup.get(key, {"error": "no_data", "data_quality": "MISSING", "data_source": "none"})
                        payload = _canonicalize_payload(raw, payload, call.source)
                        items.append(payload)

            # Fallback per-symbol for missing items
            if len(items) < len(raw_list):
                cfg = _get_concurrency_config()
                sem = asyncio.Semaphore(cfg["concurrency"])

                async def _fetch_one(raw: str) -> Dict[str, Any]:
                    async with sem:
                        try:
                            single = await asyncio.wait_for(_call_engine(request, raw), timeout=cfg["timeout_sec"])
                            if single.result is not None and not single.error:
                                payload = _as_payload(single.result)
                                return _canonicalize_payload(raw, payload, single.source)
                        except asyncio.TimeoutError:
                            pass
                        except Exception as e:
                            logger.debug(f"Sequential fetch failed for {raw}: {e}")
                        return _canonicalize_payload(raw, {"error": "fetch_failed", "data_quality": "MISSING"}, "none")

                missing = raw_list[len(items):]
                if missing:
                    items.extend(await asyncio.gather(*[_fetch_one(r) for r in missing]))

            router_requests_total.labels(endpoint="quotes", status="success").inc()
            router_request_duration.labels(endpoint="quotes").observe(time.time() - start_time)

            if fmt == "sheet":
                return _build_sheet_response(items, request_id)
            return _build_items_response(items, request_id)

        except Exception as e:
            router_requests_total.labels(endpoint="quotes", status="error").inc()
            logger.exception("Error processing /quotes")
            return _build_error_response(
                request_id=request_id,
                error=str(e),
                debug=dbg,
                trace=traceback.format_exc() if dbg else None,
            )


def get_router() -> APIRouter:
    return router


__all__ = ["router", "get_router", "EnrichedQuote", "ENRICHED_HEADERS_61", "ENRICHED_HEADERS_59", "__version__"]
