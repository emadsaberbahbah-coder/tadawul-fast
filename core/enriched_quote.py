# core/enriched_quote.py  (FULL REPLACEMENT)
"""
core/enriched_quote.py
------------------------------------------------------------
Compatibility Router + Row Mapper: Enriched Quote (PROD SAFE) — v2.6.0
Emad Bahbah — Financial Leader Edition

What this module is for
- Legacy/compat endpoints under /v1/enriched/* (kept because some clients still call it)
- Always returns HTTP 200 with a `status` field (client simplicity)
- Works with BOTH async and sync engines
- Attempts batch fast-path first; falls back per-symbol safely (bounded concurrency + timeout)

✅ v2.6.0 enhancements
- Custom “Emad 59-column” headers support:
    • Stronger header synonym mapping (Price/Prev Close/Change/Change %, etc.)
    • Adds default ENRICHED_HEADERS_59 list (sheet-ready)
- Forecast fields compatibility:
    • Uses fair_value / expected_price_* / upside_percent when present
    • Auto-computes Fair Value / Upside % / Valuation Label if missing
- Computes missing sheet KPIs:
    • turnover_percent, free_float_market_cap, liquidity_score, overall_score
- Recommendation standardized everywhere to: BUY / HOLD / REDUCE / SELL (UPPERCASE)
- ISO parsing supports trailing 'Z'
- Riyadh timestamp fill from UTC if missing
- 52W Position % computed + stored in payload if missing
- Defensive per-symbol concurrency + timeout for /quotes slow path

NOTE
- Import-safe: no hard DataEngine dependency at import time.
"""

from __future__ import annotations

import asyncio
import inspect
import logging
import os
import re
import traceback
from datetime import datetime, timezone
from functools import lru_cache
from typing import Any, Dict, List, Optional, Sequence, Tuple, Union

from fastapi import APIRouter, Query, Request
from fastapi.encoders import jsonable_encoder
from starlette.responses import JSONResponse

logger = logging.getLogger("core.enriched_quote")

ROUTER_VERSION = "2.6.0"
router = APIRouter(prefix="/v1/enriched", tags=["enriched"])

_TRUTHY = {"1", "true", "yes", "y", "on", "t"}
_UQ_KEYS: Optional[List[str]] = None

_RECO_ENUM = ("BUY", "HOLD", "REDUCE", "SELL")

# -----------------------------------------------------------------------------
# Default “Emad 59-column” headers (sheet-ready)
# -----------------------------------------------------------------------------
ENRICHED_HEADERS_59: List[str] = [
    "Rank",
    "Symbol",
    "Origin",
    "Name",
    "Sector",
    "Sub Sector",
    "Market",
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
    "Volume",
    "Avg Vol 30D",
    "Value Traded",
    "Turnover %",
    "Shares Outstanding",
    "Free Float %",
    "Market Cap",
    "Free Float Mkt Cap",
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
    "ROE",
    "ROA",
    "Net Margin",
    "EBITDA Margin",
    "Revenue Growth",
    "Net Income Growth",
    "Beta",
    "Volatility 30D",
    "RSI 14",
    "Fair Value",
    "Upside %",
    "Valuation Label",
    "Value Score",
    "Quality Score",
    "Momentum Score",
    "Opportunity Score",
    "Risk Score",
    "Overall Score",
    "Error",
    "Recommendation",
    "Data Source",
    "Data Quality",
    "Last Updated (UTC)",
    "Last Updated (Riyadh)",
]


# =============================================================================
# Small helpers
# =============================================================================
def _truthy(v: Any) -> bool:
    return str(v or "").strip().lower() in _TRUTHY


def _clamp(s: Any, n: int = 2000) -> str:
    t = (str(s) if s is not None else "").strip()
    if not t:
        return ""
    return t if len(t) <= n else (t[: n - 12] + " ...TRUNC...")


def _split_symbols(raw: str) -> List[str]:
    s = (raw or "").replace("\n", " ").replace("\t", " ").strip()
    if not s:
        return []
    s = s.replace(",", " ")
    return [p.strip() for p in s.split(" ") if p.strip()]


def _safe_error_message(e: BaseException) -> str:
    msg = str(e).strip()
    return msg or e.__class__.__name__


async def _maybe_await(v: Any) -> Any:
    if inspect.isawaitable(v):
        return await v
    return v


def _safe_get(obj: Any, *names: str) -> Any:
    if obj is None:
        return None
    if isinstance(obj, dict):
        for n in names:
            if n in obj and obj[n] is not None:
                return obj[n]
        return None
    for n in names:
        try:
            v = getattr(obj, n, None)
            if v is not None:
                return v
        except Exception:
            pass
    return None


def _as_payload(obj: Any) -> Dict[str, Any]:
    """
    Convert engine return types into a JSON-safe dict.
    - dict: passthrough
    - pydantic: model_dump()/dict()
    - dataclass-ish: __dict__
    - fallback: {"value": "..."}
    """
    if obj is None:
        return {}
    if isinstance(obj, dict):
        return jsonable_encoder(obj)

    md = getattr(obj, "model_dump", None)
    if callable(md):
        try:
            return jsonable_encoder(md())
        except Exception:
            pass

    d = getattr(obj, "dict", None)
    if callable(d):
        try:
            return jsonable_encoder(d())
        except Exception:
            pass

    od = getattr(obj, "__dict__", None)
    if isinstance(od, dict) and od:
        try:
            return jsonable_encoder(dict(od))
        except Exception:
            pass

    return {"value": str(obj)}


def _unwrap_tuple_payload(x: Any) -> Any:
    # support (payload, err)
    if isinstance(x, tuple) and len(x) == 2:
        return x[0]
    return x


# =============================================================================
# Recommendation normalization (ONE ENUM everywhere)
# =============================================================================
def _normalize_recommendation(x: Any) -> str:
    """
    Always returns one of: BUY/HOLD/REDUCE/SELL
    """
    if x is None:
        return "HOLD"
    try:
        s = str(x).strip().upper()
    except Exception:
        return "HOLD"
    if not s:
        return "HOLD"
    if s in _RECO_ENUM:
        return s

    s2 = re.sub(r"[\s\-_/]+", " ", s).strip()

    buy_like = {"STRONG BUY", "BUY", "ACCUMULATE", "ADD", "OUTPERFORM", "OVERWEIGHT", "LONG"}
    hold_like = {"HOLD", "NEUTRAL", "MAINTAIN", "MARKET PERFORM", "EQUAL WEIGHT", "WAIT"}
    reduce_like = {"REDUCE", "TRIM", "LIGHTEN", "UNDERWEIGHT", "PARTIAL SELL", "TAKE PROFIT", "TAKE PROFITS"}
    sell_like = {"SELL", "STRONG SELL", "EXIT", "AVOID", "UNDERPERFORM", "SHORT"}

    if s2 in buy_like:
        return "BUY"
    if s2 in hold_like:
        return "HOLD"
    if s2 in reduce_like:
        return "REDUCE"
    if s2 in sell_like:
        return "SELL"

    # heuristic contains
    if "SELL" in s2:
        return "SELL"
    if "REDUCE" in s2 or "TRIM" in s2 or "UNDERWEIGHT" in s2:
        return "REDUCE"
    if "HOLD" in s2 or "NEUTRAL" in s2 or "MAINTAIN" in s2:
        return "HOLD"
    if "BUY" in s2 or "ACCUMULATE" in s2 or "OVERWEIGHT" in s2:
        return "BUY"

    return "HOLD"


# =============================================================================
# Symbol normalization (PROD SAFE)
# =============================================================================
def _fallback_normalize(raw: str) -> str:
    s = (raw or "").strip().upper()
    if not s:
        return ""
    if s.startswith("TADAWUL:"):
        s = s.split(":", 1)[1].strip()
    if s.endswith(".TADAWUL"):
        s = s.replace(".TADAWUL", "")
    if any(ch in s for ch in ("=", "^")):
        return s
    if s.isdigit():
        return f"{s}.SR"
    if s.endswith(".SR") or s.endswith(".US"):
        return s
    if "." in s:
        return s
    return f"{s}.US"


@lru_cache(maxsize=1)
def _try_import_v2_normalizer() -> Any:
    try:
        from core.data_engine_v2 import normalize_symbol as _NS  # type: ignore

        return _NS
    except Exception:
        return _fallback_normalize


def _normalize_symbol_safe(raw: str) -> str:
    s = (raw or "").strip()
    if not s:
        return ""
    try:
        ns = _try_import_v2_normalizer()
        out = ns(s)
        return (out or "").strip().upper()
    except Exception:
        return _fallback_normalize(s)


# =============================================================================
# Canonical schema fill (best-effort)
# =============================================================================
@lru_cache(maxsize=1)
def _try_import_schemas():
    try:
        from core import schemas as _schemas  # type: ignore

        return _schemas
    except Exception:
        return None


def _get_uq_keys() -> List[str]:
    global _UQ_KEYS
    if isinstance(_UQ_KEYS, list):
        return _UQ_KEYS

    try:
        from core.data_engine_v2 import UnifiedQuote as UQ  # type: ignore

        mf = getattr(UQ, "model_fields", None)
        if isinstance(mf, dict) and mf:
            _UQ_KEYS = list(mf.keys())
            return _UQ_KEYS
    except Exception:
        pass

    _UQ_KEYS = []
    return _UQ_KEYS


def _schema_fill_best_effort(payload: Dict[str, Any]) -> Dict[str, Any]:
    try:
        keys = _get_uq_keys()
        if keys:
            for k in keys:
                payload.setdefault(k, None)
            return payload

        sch = _try_import_schemas()
        if sch is not None:
            try:
                for f in set(str(v) for v in getattr(sch, "HEADER_TO_FIELD", {}).values()):
                    if f:
                        payload.setdefault(f, None)
            except Exception:
                pass

        return payload
    except Exception:
        return payload


# =============================================================================
# Time helpers (Z-safe)
# =============================================================================
def _now_utc() -> datetime:
    return datetime.now(timezone.utc)


def _parse_iso_dt(x: Any) -> Optional[datetime]:
    if x is None or x == "":
        return None
    try:
        if isinstance(x, datetime):
            dt = x
        else:
            s = str(x).strip()
            if s.endswith("Z"):
                s = s[:-1] + "+00:00"
            dt = datetime.fromisoformat(s)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt
    except Exception:
        return None


def _iso_or_none(x: Any) -> Optional[str]:
    dt = _parse_iso_dt(x)
    if dt is not None:
        return dt.isoformat()
    try:
        return str(x) if x is not None else None
    except Exception:
        return None


def _to_riyadh_iso(utc_any: Any) -> Optional[str]:
    dt = _parse_iso_dt(utc_any)
    if dt is None:
        return None
    try:
        from zoneinfo import ZoneInfo  # py3.9+

        tz = ZoneInfo("Asia/Riyadh")
        return dt.astimezone(tz).isoformat()
    except Exception:
        return None


# =============================================================================
# Computations / transforms
# =============================================================================
def _safe_float_or_none(x: Any) -> Optional[float]:
    try:
        if x is None or x == "":
            return None
        return float(x)
    except Exception:
        return None


def _compute_52w_position_pct(cp: Any, low_52w: Any, high_52w: Any) -> Optional[float]:
    try:
        cp_f = float(cp)
        lo = float(low_52w)
        hi = float(high_52w)
        if hi == lo:
            return None
        return round(((cp_f - lo) / (hi - lo)) * 100.0, 2)
    except Exception:
        return None


def _ratio_to_percent(v: Any) -> Any:
    if v is None:
        return None
    try:
        f = float(v)
        return (f * 100.0) if -1.0 <= f <= 1.0 else f
    except Exception:
        return v


def _vol_to_percent(v: Any) -> Any:
    if v is None:
        return None
    try:
        f = float(v)
        return f * 100.0 if 0.0 <= f <= 1.0 else f
    except Exception:
        return v


def _ff_to_fraction(x: Any) -> Optional[float]:
    """
    Accepts free_float as:
    - 0..1 fraction OR 0..100 percent
    returns fraction 0..1
    """
    f = _safe_float_or_none(x)
    if f is None:
        return None
    if f > 1.5:
        return max(0.0, min(1.0, f / 100.0))
    return max(0.0, min(1.0, f))


def _compute_turnover_percent(volume: Any, shares_outstanding: Any) -> Optional[float]:
    v = _safe_float_or_none(volume)
    sh = _safe_float_or_none(shares_outstanding)
    if v is None or sh in (None, 0.0):
        return None
    try:
        return round((v / sh) * 100.0, 4)
    except Exception:
        return None


def _compute_free_float_mkt_cap(market_cap: Any, free_float: Any) -> Optional[float]:
    mc = _safe_float_or_none(market_cap)
    ff = _ff_to_fraction(free_float)
    if mc is None or ff is None:
        return None
    try:
        return mc * ff
    except Exception:
        return None


def _compute_liquidity_score(value_traded: Any) -> Optional[float]:
    """
    Simple, stable liquidity score 0..100 using log10(value_traded).
    Intended only as a sheet helper (not “financial advice”).
    """
    vt = _safe_float_or_none(value_traded)
    if vt is None or vt <= 0:
        return None
    try:
        import math

        x = math.log10(vt)
        # 6 -> ~1M, 9 -> ~1B
        score = (x - 6.0) / (9.0 - 6.0) * 100.0
        return float(max(0.0, min(100.0, round(score, 2))))
    except Exception:
        return None


def _compute_fair_value_and_upside(payload: Dict[str, Any]) -> Tuple[Optional[float], Optional[float], Optional[str]]:
    price = _safe_float_or_none(_safe_get(payload, "current_price", "last_price", "price"))
    if price is None or price <= 0:
        return None, None, None

    fair = _safe_float_or_none(_safe_get(payload, "fair_value"))
    if fair is None:
        fair = _safe_float_or_none(_safe_get(payload, "expected_price_3m"))
    if fair is None:
        fair = _safe_float_or_none(_safe_get(payload, "expected_price_12m"))
    if fair is None:
        fair = _safe_float_or_none(_safe_get(payload, "ma200"))
    if fair is None:
        fair = _safe_float_or_none(_safe_get(payload, "ma50"))

    if fair is None or fair <= 0:
        return None, None, None

    upside = (fair / price - 1.0) * 100.0
    label = "Fairly Valued"
    if upside >= 15:
        label = "Undervalued"
    elif upside <= -15:
        label = "Overvalued"

    return fair, round(upside, 2), label


def _hkey(h: str) -> str:
    s = str(h or "").strip().lower()
    s = re.sub(r"\s+", " ", s)
    return s


# =============================================================================
# Header mapping (robust synonyms for your 59 columns)
# =============================================================================
_HEADER_MAP: Dict[str, Tuple[Tuple[str, ...], Optional[Any]]] = {
    # Identity
    "rank": (("rank",), None),
    "symbol": (("symbol", "symbol_normalized", "ticker", "code"), None),
    "origin": (("origin",), None),
    "name": (("name", "company_name"), None),
    "company name": (("name", "company_name"), None),
    "sector": (("sector",), None),
    "sub sector": (("sub_sector", "subsector"), None),
    "sub-sector": (("sub_sector", "subsector"), None),
    "market": (("market", "market_region"), None),
    "currency": (("currency",), None),
    "listing date": (("listing_date", "ipo"), None),

    # Prices
    "price": (("current_price", "last_price", "price"), None),
    "last price": (("current_price", "last_price", "price"), None),
    "prev close": (("previous_close",), None),
    "previous close": (("previous_close",), None),
    "change": (("price_change", "change"), None),
    "price change": (("price_change", "change"), None),
    "change %": (("percent_change", "change_percent", "change_pct"), None),
    "percent change": (("percent_change", "change_percent", "change_pct"), None),
    "day high": (("day_high",), None),
    "day low": (("day_low",), None),

    # 52W
    "52w high": (("week_52_high", "high_52w", "52w_high"), None),
    "52w low": (("week_52_low", "low_52w", "52w_low"), None),
    "52w position %": (("position_52w_percent", "position_52w"), None),

    # Volume/Liquidity
    "volume": (("volume",), None),
    "avg vol 30d": (("avg_volume_30d", "avg_volume"), None),
    "avg volume (30d)": (("avg_volume_30d", "avg_volume"), None),
    "value traded": (("value_traded",), None),
    "turnover %": (("turnover_percent", "turnover"), _ratio_to_percent),
    "shares outstanding": (("shares_outstanding",), None),
    "free float %": (("free_float", "free_float_percent"), _ratio_to_percent),
    "market cap": (("market_cap",), None),
    "free float mkt cap": (("free_float_market_cap", "free_float_mkt_cap"), None),
    "free float market cap": (("free_float_market_cap", "free_float_mkt_cap"), None),
    "liquidity score": (("liquidity_score",), None),

    # Fundamentals / ratios
    "eps (ttm)": (("eps_ttm", "eps"), None),
    "forward eps": (("forward_eps",), None),
    "p/e (ttm)": (("pe_ttm",), None),
    "forward p/e": (("forward_pe",), None),
    "p/b": (("pb",), None),
    "p/s": (("ps",), None),
    "ev/ebitda": (("ev_ebitda",), None),

    # Dividends / profitability / growth
    "dividend yield": (("dividend_yield",), _ratio_to_percent),
    "dividend yield %": (("dividend_yield",), _ratio_to_percent),
    "dividend rate": (("dividend_rate",), None),
    "payout ratio": (("payout_ratio",), _ratio_to_percent),
    "payout ratio %": (("payout_ratio",), _ratio_to_percent),
    "roe": (("roe",), _ratio_to_percent),
    "roe %": (("roe",), _ratio_to_percent),
    "roa": (("roa",), _ratio_to_percent),
    "roa %": (("roa",), _ratio_to_percent),
    "net margin": (("net_margin",), _ratio_to_percent),
    "net margin %": (("net_margin",), _ratio_to_percent),
    "ebitda margin": (("ebitda_margin",), _ratio_to_percent),
    "ebitda margin %": (("ebitda_margin",), _ratio_to_percent),
    "revenue growth": (("revenue_growth",), _ratio_to_percent),
    "revenue growth %": (("revenue_growth",), _ratio_to_percent),
    "net income growth": (("net_income_growth",), _ratio_to_percent),
    "net income growth %": (("net_income_growth",), _ratio_to_percent),

    # Risk / technical
    "beta": (("beta",), None),
    "volatility 30d": (("volatility_30d", "vol_30d_ann", "volatility_30d_ann"), _vol_to_percent),
    "volatility (30d)": (("volatility_30d", "vol_30d_ann", "volatility_30d_ann"), _vol_to_percent),
    "rsi 14": (("rsi_14", "rsi14"), None),
    "rsi (14)": (("rsi_14", "rsi14"), None),

    # Valuation / scores
    "fair value": (("fair_value", "expected_price_3m", "expected_price_12m", "ma200", "ma50"), None),
    "upside %": (("upside_percent",), _ratio_to_percent),
    "valuation label": (("valuation_label",), None),

    "value score": (("value_score",), None),
    "quality score": (("quality_score",), None),
    "momentum score": (("momentum_score",), None),
    "opportunity score": (("opportunity_score",), None),
    "risk score": (("risk_score",), None),
    "overall score": (("overall_score",), None),

    # Metadata
    "error": (("error",), None),
    "recommendation": (("recommendation",), None),
    "data source": (("data_source", "source"), None),
    "data quality": (("data_quality",), None),
    "last updated (utc)": (("last_updated_utc", "as_of_utc"), _iso_or_none),
    "last updated (riyadh)": (("last_updated_riyadh",), _iso_or_none),
}


# =============================================================================
# EnrichedQuote helper (used by routes/advanced_analysis.py)
# =============================================================================
class EnrichedQuote:
    """
    Lightweight mapper that can convert a UnifiedQuote-like payload into sheet rows.

    - from_unified(payload): accepts dict or attribute object
    - to_row(headers): returns values aligned to provided headers
    """

    def __init__(self, payload: Dict[str, Any]):
        self.payload = payload or {}

    @classmethod
    def from_unified(cls, uq: Any) -> "EnrichedQuote":
        p = _as_payload(_unwrap_tuple_payload(uq))

        # Recommendation enum
        try:
            p["recommendation"] = _normalize_recommendation(p.get("recommendation"))
        except Exception:
            p["recommendation"] = "HOLD"

        # Ensure timestamps
        try:
            if not p.get("last_updated_utc") and p.get("as_of_utc"):
                p["last_updated_utc"] = p.get("as_of_utc")
        except Exception:
            pass

        # Fill Riyadh timestamp if possible
        try:
            if not p.get("last_updated_riyadh"):
                p["last_updated_riyadh"] = _to_riyadh_iso(p.get("last_updated_utc")) or ""
        except Exception:
            pass

        # Compute 52W position if missing
        try:
            if p.get("position_52w_percent") is None:
                cp = _safe_get(p, "current_price", "last_price", "price")
                lo = _safe_get(p, "week_52_low", "low_52w", "52w_low")
                hi = _safe_get(p, "week_52_high", "high_52w", "52w_high")
                pos = _compute_52w_position_pct(cp, lo, hi)
                if pos is not None:
                    p["position_52w_percent"] = pos
        except Exception:
            pass

        # Compute turnover/free-float mkt cap/liquidity score if missing
        try:
            if p.get("turnover_percent") is None:
                p["turnover_percent"] = _compute_turnover_percent(p.get("volume"), p.get("shares_outstanding"))
        except Exception:
            pass

        try:
            if p.get("free_float_market_cap") is None:
                p["free_float_market_cap"] = _compute_free_float_mkt_cap(p.get("market_cap"), p.get("free_float"))
        except Exception:
            pass

        try:
            if p.get("liquidity_score") is None:
                p["liquidity_score"] = _compute_liquidity_score(p.get("value_traded"))
        except Exception:
            pass

        # overall_score default
        try:
            if p.get("overall_score") is None:
                p["overall_score"] = p.get("opportunity_score")
        except Exception:
            pass

        return cls(p)

    def _value_for_header(self, header: str) -> Any:
        sch = _try_import_schemas()
        hk = _hkey(header)

        # Schema-driven candidates (preferred if available)
        candidates: Tuple[str, ...] = ()
        if sch is not None:
            try:
                candidates = tuple(getattr(sch, "header_field_candidates")(header))  # type: ignore
            except Exception:
                candidates = ()

        # Internal robust mapping
        spec = _HEADER_MAP.get(hk)
        if spec:
            fields, transform = spec

            # computed: origin
            if hk == "origin":
                v = _safe_get(self.payload, *fields)
                if v is not None:
                    return v
                sym = str(self.payload.get("symbol_normalized") or self.payload.get("symbol") or "").upper()
                mkt = str(self.payload.get("market") or "").upper()
                if sym.endswith(".SR") or mkt == "KSA":
                    return "KSA_TADAWUL"
                if sym.startswith("^") or "=" in sym:
                    return "GLOBAL_MARKETS"
                return "GLOBAL_MARKETS"

            # computed: 52W position %
            if hk in ("52w position %", "52w position"):
                v = _safe_get(self.payload, *fields)
                if v is not None:
                    return v
                cp = _safe_get(self.payload, "current_price", "last_price", "price")
                lo = _safe_get(self.payload, "week_52_low", "low_52w", "52w_low")
                hi = _safe_get(self.payload, "week_52_high", "high_52w", "52w_high")
                v2 = _compute_52w_position_pct(cp, lo, hi)
                if v2 is not None:
                    try:
                        self.payload["position_52w_percent"] = v2
                    except Exception:
                        pass
                return v2

            # computed: turnover %
            if hk == "turnover %":
                v = _safe_get(self.payload, *fields)
                if v is not None:
                    return transform(v) if transform else v
                v2 = _compute_turnover_percent(self.payload.get("volume"), self.payload.get("shares_outstanding"))
                if v2 is not None:
                    try:
                        self.payload["turnover_percent"] = v2
                    except Exception:
                        pass
                return v2

            # computed: free float mkt cap
            if hk in ("free float mkt cap", "free float market cap"):
                v = _safe_get(self.payload, *fields)
                if v is not None:
                    return v
                v2 = _compute_free_float_mkt_cap(self.payload.get("market_cap"), self.payload.get("free_float"))
                if v2 is not None:
                    try:
                        self.payload["free_float_market_cap"] = v2
                    except Exception:
                        pass
                return v2

            # computed: liquidity score
            if hk == "liquidity score":
                v = _safe_get(self.payload, *fields)
                if v is not None:
                    return v
                v2 = _compute_liquidity_score(self.payload.get("value_traded"))
                if v2 is not None:
                    try:
                        self.payload["liquidity_score"] = v2
                    except Exception:
                        pass
                return v2

            # computed: last updated (riyadh)
            if hk == "last updated (riyadh)":
                v = self.payload.get("last_updated_riyadh")
                if not v:
                    u = self.payload.get("last_updated_utc") or self.payload.get("as_of_utc")
                    v = _to_riyadh_iso(u) or ""
                    try:
                        self.payload["last_updated_riyadh"] = v
                    except Exception:
                        pass
                return _iso_or_none(v) or v

            # computed fair/upside/label if missing
            if hk in ("fair value", "upside %", "valuation label"):
                fair, upside, label = _compute_fair_value_and_upside(self.payload)
                if hk == "fair value":
                    v = _safe_get(self.payload, *fields)
                    return v if v is not None else fair
                if hk == "upside %":
                    v = self.payload.get("upside_percent")
                    return _ratio_to_percent(v) if v is not None else upside
                if hk == "valuation label":
                    v = self.payload.get("valuation_label")
                    return v if v is not None else label

            # recommendation forced enum
            if hk == "recommendation":
                return _normalize_recommendation(self.payload.get("recommendation"))

            # overall score fallback
            if hk == "overall score":
                v = _safe_get(self.payload, *fields)
                if v is not None:
                    return v
                return self.payload.get("opportunity_score")

            val = _safe_get(self.payload, *fields)
            if transform and val is not None:
                try:
                    return transform(val)
                except Exception:
                    return val
            return val

        # If schema candidates exist, use them
        if candidates:
            for f in candidates:
                if not f:
                    continue
                if f in self.payload and self.payload.get(f) is not None:
                    val = self.payload.get(f)

                    if hk in {
                        "turnover %",
                        "free float %",
                        "dividend yield",
                        "dividend yield %",
                        "payout ratio",
                        "payout ratio %",
                        "roe",
                        "roe %",
                        "roa",
                        "roa %",
                        "net margin",
                        "net margin %",
                        "ebitda margin",
                        "ebitda margin %",
                        "revenue growth",
                        "revenue growth %",
                        "net income growth",
                        "net income growth %",
                        "upside %",
                        "change %",
                        "percent change",
                    }:
                        return _ratio_to_percent(val)

                    if hk in ("volatility 30d", "volatility (30d)"):
                        return _vol_to_percent(val)

                    if hk == "last updated (utc)":
                        return _iso_or_none(val) or val

                    return val

        # Absolute last fallback
        guess = str(header or "").strip()
        return self.payload.get(guess)

    def to_row(self, headers: Sequence[str]) -> List[Any]:
        hs = [str(h) for h in (headers or [])]
        row = [self._value_for_header(h) for h in hs]
        if len(row) < len(hs):
            row += [None] * (len(hs) - len(row))
        return row[: len(hs)]

    def to_payload(self) -> Dict[str, Any]:
        return dict(self.payload)


# =============================================================================
# Engine calls (best-effort)
# =============================================================================
async def _call_engine_best_effort(request: Request, symbol: str) -> Tuple[Optional[Any], Optional[str], Optional[str]]:
    """
    Returns: (result, source, error)
    """
    # 1) app.state.engine
    try:
        eng = getattr(request.app.state, "engine", None)
    except Exception:
        eng = None

    if eng is not None:
        last_err: Optional[str] = None
        for fn_name in ("get_enriched_quote", "get_quote"):
            fn = getattr(eng, fn_name, None)
            if callable(fn):
                try:
                    res = await _maybe_await(fn(symbol))
                    return _unwrap_tuple_payload(res), f"app.state.engine.{fn_name}", None
                except Exception as e:
                    last_err = _safe_error_message(e)
                    continue
        return None, "app.state.engine", last_err or "engine call failed"

    # 2) v2 module singleton function (if present)
    try:
        from core.data_engine_v2 import get_enriched_quote as v2_get  # type: ignore

        res2 = await _maybe_await(v2_get(symbol))
        return _unwrap_tuple_payload(res2), "core.data_engine_v2.get_enriched_quote(singleton)", None
    except Exception:
        pass

    # 3) v2 temp engine
    try:
        from core.data_engine_v2 import DataEngine as V2Engine  # type: ignore

        tmp = V2Engine()
        try:
            fn = getattr(tmp, "get_enriched_quote", None) or getattr(tmp, "get_quote", None)
            if callable(fn):
                res3 = await _maybe_await(fn(symbol))
                return _unwrap_tuple_payload(res3), "core.data_engine_v2.DataEngine(temp)", None
        finally:
            aclose = getattr(tmp, "aclose", None)
            if callable(aclose):
                try:
                    await _maybe_await(aclose())
                except Exception:
                    pass
            close = getattr(tmp, "close", None)
            if callable(close):
                try:
                    close()
                except Exception:
                    pass
    except Exception:
        pass

    # 4) legacy module-level singleton
    try:
        from core.data_engine import get_enriched_quote as v1_get  # type: ignore

        res4 = await _maybe_await(v1_get(symbol))
        return _unwrap_tuple_payload(res4), "core.data_engine.get_enriched_quote", None
    except Exception:
        pass

    return None, None, "no provider/engine available"


async def _call_engine_batch_best_effort(
    request: Request, symbols_norm: List[str]
) -> Tuple[Optional[Union[List[Any], Dict[str, Any]]], Optional[str], Optional[str]]:
    """
    Returns: (batch_result, source, error)
    batch_result may be list[payload] OR dict[symbol->payload]
    """
    if not symbols_norm:
        return None, None, "empty"

    # 1) app.state.engine batch
    try:
        eng = getattr(request.app.state, "engine", None)
    except Exception:
        eng = None

    if eng is not None:
        last_err: Optional[str] = None
        for fn_name in ("get_enriched_quotes", "get_quotes"):
            fn = getattr(eng, fn_name, None)
            if callable(fn):
                try:
                    res = await _maybe_await(fn(symbols_norm))
                    res = _unwrap_tuple_payload(res)
                    if isinstance(res, (list, dict)):
                        return res, f"app.state.engine.{fn_name}", None
                    last_err = "batch returned non-list/non-dict"
                except Exception as e:
                    last_err = _safe_error_message(e)
        return None, "app.state.engine(batch)", last_err or "batch call failed"

    # 2) v2 singleton batch
    try:
        from core.data_engine_v2 import get_enriched_quotes as v2_batch  # type: ignore

        res2 = await _maybe_await(v2_batch(symbols_norm))
        res2 = _unwrap_tuple_payload(res2)
        if isinstance(res2, (list, dict)):
            return res2, "core.data_engine_v2.get_enriched_quotes(singleton)", None
        return None, "core.data_engine_v2.get_enriched_quotes(singleton)", "batch returned non-list/non-dict"
    except Exception:
        pass

    return None, None, None


# =============================================================================
# Finalization rules for API payloads
# =============================================================================
def _finalize_payload(payload: Dict[str, Any], *, raw: str, norm: str, source: str) -> Dict[str, Any]:
    sym = (norm or raw or "").strip().upper() or ""

    try:
        payload.setdefault("symbol", sym)
        payload["symbol_input"] = payload.get("symbol_input") or raw
        payload["symbol_normalized"] = payload.get("symbol_normalized") or sym

        # recommendation enum
        payload["recommendation"] = _normalize_recommendation(payload.get("recommendation"))

        # normalize error field to string
        if payload.get("error") is None:
            payload["error"] = ""
        if not isinstance(payload.get("error"), str):
            payload["error"] = str(payload.get("error") or "")

        # set status consistently
        if str(payload.get("error") or "").strip():
            payload["status"] = "error"
        else:
            if not str(payload.get("status") or "").strip():
                payload["status"] = "success"

        # data_source/data_quality defaults
        if not payload.get("data_source"):
            payload["data_source"] = source or "unknown"
        if not payload.get("data_quality"):
            payload["data_quality"] = "MISSING" if payload.get("current_price") is None else "PARTIAL"

        # timestamps
        if not payload.get("last_updated_utc"):
            payload["last_updated_utc"] = payload.get("as_of_utc") or _now_utc().isoformat()
        else:
            payload["last_updated_utc"] = _iso_or_none(payload.get("last_updated_utc")) or payload.get("last_updated_utc")

        if not payload.get("last_updated_riyadh"):
            payload["last_updated_riyadh"] = _to_riyadh_iso(payload.get("last_updated_utc")) or ""

        # compute 52w position if missing
        if payload.get("position_52w_percent") is None:
            cp = _safe_get(payload, "current_price", "last_price", "price")
            lo = _safe_get(payload, "week_52_low", "low_52w", "52w_low")
            hi = _safe_get(payload, "week_52_high", "high_52w", "52w_high")
            pos = _compute_52w_position_pct(cp, lo, hi)
            if pos is not None:
                payload["position_52w_percent"] = pos

        # turnover / free float mkt cap / liquidity score
        if payload.get("turnover_percent") is None:
            payload["turnover_percent"] = _compute_turnover_percent(payload.get("volume"), payload.get("shares_outstanding"))

        if payload.get("free_float_market_cap") is None:
            payload["free_float_market_cap"] = _compute_free_float_mkt_cap(payload.get("market_cap"), payload.get("free_float"))

        if payload.get("liquidity_score") is None:
            payload["liquidity_score"] = _compute_liquidity_score(payload.get("value_traded"))

        # overall_score fallback
        if payload.get("overall_score") is None:
            payload["overall_score"] = payload.get("opportunity_score")

        return _schema_fill_best_effort(payload)
    except Exception:
        return _schema_fill_best_effort(
            {
                "status": "error",
                "symbol": sym,
                "symbol_input": raw,
                "symbol_normalized": sym,
                "recommendation": "HOLD",
                "data_quality": "MISSING",
                "data_source": source or "unknown",
                "error": "payload_finalize_failed",
                "last_updated_utc": _now_utc().isoformat(),
                "last_updated_riyadh": "",
            }
        )


def _payload_symbol_key(p: Dict[str, Any]) -> str:
    try:
        for k in ("symbol_normalized", "symbol", "Symbol", "ticker", "code"):
            v = p.get(k)
            if v is not None:
                s = str(v).strip().upper()
                if s:
                    return s
    except Exception:
        pass
    return ""


# =============================================================================
# Concurrency config for slow path
# =============================================================================
def _safe_int(x: Any, default: int) -> int:
    try:
        v = int(str(x).strip())
        return v if v > 0 else default
    except Exception:
        return default


def _safe_float(x: Any, default: float) -> float:
    try:
        v = float(str(x).strip())
        return v if v > 0 else default
    except Exception:
        return default


def _cfg() -> Dict[str, Any]:
    concurrency = _safe_int(os.getenv("ENRICHED_CONCURRENCY", 8), 8)
    timeout_sec = _safe_float(os.getenv("ENRICHED_TIMEOUT_SEC", 25), 25.0)

    concurrency = max(1, min(25, concurrency))
    timeout_sec = max(3.0, min(90.0, timeout_sec))

    return {"concurrency": concurrency, "timeout_sec": timeout_sec}


# =============================================================================
# Routes
# =============================================================================
@router.get("/quote")
async def enriched_quote(
    request: Request,
    symbol: str = Query(..., description="Ticker symbol (AAPL, MSFT.US, 1120.SR, ^GSPC, GC=F)"),
    debug: int = Query(0, description="Set 1 to include traceback (or enable DEBUG_ERRORS=1)"),
):
    dbg = _truthy(os.getenv("DEBUG_ERRORS", "0")) or bool(debug)

    raw = (symbol or "").strip()
    norm = _normalize_symbol_safe(raw)

    if not raw:
        out = _schema_fill_best_effort(
            {
                "status": "error",
                "symbol": "",
                "symbol_input": "",
                "symbol_normalized": "",
                "recommendation": "HOLD",
                "data_quality": "MISSING",
                "data_source": "none",
                "error": "Empty symbol",
                "last_updated_utc": _now_utc().isoformat(),
                "last_updated_riyadh": "",
            }
        )
        return JSONResponse(status_code=200, content=out)

    try:
        result, source, err = await _call_engine_best_effort(request, norm or raw)
        if result is None:
            out = _schema_fill_best_effort(
                {
                    "status": "error",
                    "symbol": norm or raw,
                    "symbol_input": raw,
                    "symbol_normalized": norm or raw,
                    "recommendation": "HOLD",
                    "data_quality": "MISSING",
                    "data_source": "none",
                    "error": err or "Enriched quote engine not available (no working provider).",
                    "last_updated_utc": _now_utc().isoformat(),
                    "last_updated_riyadh": "",
                }
            )
            return JSONResponse(status_code=200, content=out)

        payload = _as_payload(result)
        payload = _finalize_payload(payload, raw=raw, norm=norm, source=source or "unknown")
        return JSONResponse(status_code=200, content=payload)

    except Exception as e:
        out: Dict[str, Any] = {
            "status": "error",
            "symbol": norm or raw,
            "symbol_input": raw,
            "symbol_normalized": norm or raw,
            "recommendation": "HOLD",
            "data_quality": "MISSING",
            "data_source": "none",
            "error": _safe_error_message(e),
            "last_updated_utc": _now_utc().isoformat(),
            "last_updated_riyadh": "",
        }
        if dbg:
            out["traceback"] = _clamp(traceback.format_exc(), 8000)
        return JSONResponse(status_code=200, content=_schema_fill_best_effort(out))


@router.get("/quotes")
async def enriched_quotes(
    request: Request,
    symbols: str = Query(..., description="Comma/space-separated symbols, e.g. AAPL,MSFT,1120.SR"),
    debug: int = Query(0, description="Set 1 to include traceback (or enable DEBUG_ERRORS=1)"),
):
    dbg = _truthy(os.getenv("DEBUG_ERRORS", "0")) or bool(debug)

    raw_list = _split_symbols(symbols)
    if not raw_list:
        return JSONResponse(status_code=200, content={"status": "error", "error": "Empty symbols list", "items": []})

    norms = [_normalize_symbol_safe(r) or (r or "").strip() for r in raw_list]
    norms = [n for n in norms if n]

    batch_res, batch_source, batch_err = await _call_engine_batch_best_effort(request, norms)

    items: List[Dict[str, Any]] = []

    # FAST PATH: batch returned list or dict
    if isinstance(batch_res, (list, dict)) and batch_res:
        if isinstance(batch_res, dict):
            mp: Dict[str, Dict[str, Any]] = {}
            for k, v in batch_res.items():
                kk = str(k or "").strip().upper()
                pv = _as_payload(_unwrap_tuple_payload(v))
                if kk and kk not in mp:
                    mp[kk] = pv

            for raw in raw_list:
                norm = (_normalize_symbol_safe(raw) or raw).strip().upper()
                p = mp.get(norm)
                if p is None:
                    out = {
                        "status": "error",
                        "symbol": norm,
                        "symbol_input": raw,
                        "symbol_normalized": norm,
                        "recommendation": "HOLD",
                        "data_quality": "MISSING",
                        "data_source": "none",
                        "error": "Engine returned no item for this symbol.",
                        "last_updated_utc": _now_utc().isoformat(),
                        "last_updated_riyadh": "",
                    }
                    if dbg and batch_err:
                        out["batch_error_hint"] = _clamp(batch_err, 1200)
                    items.append(_schema_fill_best_effort(out))
                else:
                    items.append(_finalize_payload(p, raw=raw, norm=norm, source=batch_source or "unknown"))

            return JSONResponse(status_code=200, content={"status": "success", "count": len(items), "items": items})

        # list case
        payloads: List[Dict[str, Any]] = [_as_payload(_unwrap_tuple_payload(x)) for x in list(batch_res)]

        if len(payloads) == len(raw_list):
            for i, raw in enumerate(raw_list):
                norm = _normalize_symbol_safe(raw) or raw
                p = payloads[i] if i < len(payloads) else {}
                items.append(_finalize_payload(p, raw=raw, norm=norm, source=batch_source or "unknown"))
            return JSONResponse(status_code=200, content={"status": "success", "count": len(items), "items": items})

        mp2: Dict[str, Dict[str, Any]] = {}
        for p in payloads:
            k = _payload_symbol_key(p)
            if k and k not in mp2:
                mp2[k] = p

        for raw in raw_list:
            norm = (_normalize_symbol_safe(raw) or raw).strip().upper()
            p = mp2.get(norm)
            if p is None:
                out = {
                    "status": "error",
                    "symbol": norm,
                    "symbol_input": raw,
                    "symbol_normalized": norm,
                    "recommendation": "HOLD",
                    "data_quality": "MISSING",
                    "data_source": "none",
                    "error": "Engine returned no item for this symbol.",
                    "last_updated_utc": _now_utc().isoformat(),
                    "last_updated_riyadh": "",
                }
                if dbg and batch_err:
                    out["batch_error_hint"] = _clamp(batch_err, 1200)
                items.append(_schema_fill_best_effort(out))
            else:
                items.append(_finalize_payload(p, raw=raw, norm=norm, source=batch_source or "unknown"))

        return JSONResponse(status_code=200, content={"status": "success", "count": len(items), "items": items})

    # SLOW PATH: per-symbol (bounded concurrency + timeout)
    cfg = _cfg()
    sem = asyncio.Semaphore(cfg["concurrency"])

    async def _one(raw: str) -> Dict[str, Any]:
        norm = _normalize_symbol_safe(raw)
        async with sem:
            try:
                result, source, err = await asyncio.wait_for(
                    _call_engine_best_effort(request, norm or raw), timeout=cfg["timeout_sec"]
                )
                if result is None:
                    out = {
                        "status": "error",
                        "symbol": norm or raw,
                        "symbol_input": raw,
                        "symbol_normalized": norm or raw,
                        "recommendation": "HOLD",
                        "data_quality": "MISSING",
                        "data_source": "none",
                        "error": err or "Engine not available for this symbol.",
                        "last_updated_utc": _now_utc().isoformat(),
                        "last_updated_riyadh": "",
                    }
                    if dbg and batch_err:
                        out["batch_error_hint"] = _clamp(batch_err, 1200)
                    return _schema_fill_best_effort(out)

                payload = _as_payload(result)
                return _finalize_payload(payload, raw=raw, norm=norm, source=source or "unknown")

            except asyncio.TimeoutError:
                out = {
                    "status": "error",
                    "symbol": norm or raw,
                    "symbol_input": raw,
                    "symbol_normalized": norm or raw,
                    "recommendation": "HOLD",
                    "data_quality": "MISSING",
                    "data_source": "none",
                    "error": "timeout",
                    "last_updated_utc": _now_utc().isoformat(),
                    "last_updated_riyadh": "",
                }
                return _schema_fill_best_effort(out)

            except Exception as e:
                out2: Dict[str, Any] = {
                    "status": "error",
                    "symbol": norm or raw,
                    "symbol_input": raw,
                    "symbol_normalized": norm or raw,
                    "recommendation": "HOLD",
                    "data_quality": "MISSING",
                    "data_source": "none",
                    "error": _safe_error_message(e),
                    "last_updated_utc": _now_utc().isoformat(),
                    "last_updated_riyadh": "",
                }
                if dbg:
                    out2["traceback"] = _clamp(traceback.format_exc(), 8000)
                    if batch_err:
                        out2["batch_error_hint"] = _clamp(batch_err, 1200)
                return _schema_fill_best_effort(out2)

    items = await asyncio.gather(*[_one(r) for r in raw_list])
    return JSONResponse(status_code=200, content={"status": "success", "count": len(items), "items": items})


@router.get("/health", include_in_schema=False)
async def enriched_health():
    return {"status": "ok", "module": "core.enriched_quote", "version": ROUTER_VERSION}


def get_router() -> APIRouter:
    return router


__all__ = ["router", "get_router", "EnrichedQuote", "ENRICHED_HEADERS_59"]
