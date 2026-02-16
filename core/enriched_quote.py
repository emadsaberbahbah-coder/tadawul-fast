"""
core/enriched_quote.py
------------------------------------------------------------
Compatibility Router + Row Mapper: Enriched Quote (PROD SAFE) — v2.9.0
Emad Bahbah — Financial Leader Edition

What this module is for
- Legacy/compat endpoints under /v1/enriched/* (kept because some clients still call it)
- Always returns HTTP 200 with a `status` field (client simplicity)
- Works with BOTH async and sync engines
- Attempts batch fast-path first; falls back per-symbol safely (bounded concurrency + timeout)

✅ v2.9.0 enhancements (this revision)
- ✅ Query parsing is more robust:
    * Supports comma/space strings AND repeated query params:
      /v1/enriched/quotes?symbols=AAPL&symbols=MSFT
      /v1/enriched/quotes?tickers=1120.SR&tickers=2222.SR
    * Accepts aliases: symbols/tickers/symbol (best-effort)
- ✅ Batch unwrapping is more robust:
    * Handles engine batch returns as list, dict, or dict with {"items":[...]}
- ✅ Optional sheet-ready output:
    * include_headers=1 to return headers
    * include_rows=1 to return rows aligned to ENRICHED_HEADERS_59
- ✅ Stronger payload finalization:
    * Fills origin default (KSA_TADAWUL / GLOBAL_MARKETS)
    * Better data_quality heuristic (MISSING/PARTIAL/GOOD)
    * Adds requested_symbol to each item (stable mapping for Sheets)
- ✅ Riyadh localization hardened with fallback to +03:00 if ZoneInfo not available

NOTE
- This module is intentionally “compat + sheet-mapper”. Your newer sheet endpoints should use:
    routes/advanced_analysis.py  (engine-driven + schema-driven)
"""

from __future__ import annotations

import asyncio
import inspect
import logging
import os
import re
import traceback
import uuid
from datetime import datetime, timezone, timedelta
from functools import lru_cache
from typing import Any, Dict, List, Optional, Sequence, Tuple, Union

from fastapi import APIRouter, Query, Request
from fastapi.encoders import jsonable_encoder
from starlette.responses import JSONResponse

logger = logging.getLogger("core.enriched_quote")

ROUTER_VERSION = "2.9.0"
router = APIRouter(prefix="/v1/enriched", tags=["enriched"])

_TRUTHY = {"1", "true", "yes", "y", "on", "t"}
_UQ_KEYS: Optional[List[str]] = None

_RECO_ENUM = ("BUY", "HOLD", "REDUCE", "SELL")


# ---------------------------------------------------------------------------
# Default “Emad 59-column” headers (sheet-ready)
# ---------------------------------------------------------------------------
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
    """
    Safe getter for dict-like or object-like payloads.
    """
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


def _unwrap_engine_container(x: Any) -> Any:
    """
    Engines sometimes return:
      - list[payload]
      - dict[symbol->payload]
      - dict with {"items":[payload], ...}
      - dict with {"data":[...], ...}
    This helper unwraps common containers safely.
    """
    x = _unwrap_tuple_payload(x)
    if isinstance(x, dict):
        for k in ("items", "data", "quotes", "results"):
            v = x.get(k)
            if isinstance(v, (list, dict)):
                return v
    return x


def _getlist_any(request: Request, *names: str) -> List[str]:
    out: List[str] = []
    try:
        qp = request.query_params
        for n in names:
            try:
                vals = qp.getlist(n)  # type: ignore[attr-defined]
            except Exception:
                vals = []
            for v in vals or []:
                if v is not None and str(v).strip():
                    out.append(str(v).strip())
    except Exception:
        return out
    return out


def _extract_symbols_from_request(request: Request, symbols_fallback: Optional[str]) -> List[str]:
    """
    Supports:
      - symbols=comma/space list
      - repeated: ?symbols=AAPL&symbols=MSFT
      - tickers alias: ?tickers=1120.SR&tickers=2222.SR
      - also accepts 'symbol' (single or repeated) best-effort
    """
    raw_chunks: List[str] = []
    raw_chunks.extend(_getlist_any(request, "symbols", "tickers", "symbol"))
    if symbols_fallback:
        raw_chunks.append(symbols_fallback)

    out: List[str] = []
    for chunk in raw_chunks:
        out.extend(_split_symbols(chunk))
    # de-empty + preserve order
    cleaned: List[str] = []
    seen: set = set()
    for s in out:
        s2 = (s or "").strip()
        if not s2:
            continue
        if s2 not in seen:
            seen.add(s2)
            cleaned.append(s2)
    return cleaned


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
    """
    Tries to ensure a stable key-set for older clients.
    Never raises.
    """
    try:
        keys = _get_uq_keys()
        if keys:
            for k in keys:
                payload.setdefault(k, None)
            return payload

        sch = _try_import_schemas()
        if sch is not None:
            try:
                hdr_to_field = getattr(sch, "HEADER_TO_FIELD", {})
                if isinstance(hdr_to_field, dict):
                    for f in set(str(v) for v in hdr_to_field.values()):
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
        elif isinstance(x, (int, float)):
            # epoch seconds (best-effort)
            dt = datetime.fromtimestamp(float(x), tz=timezone.utc)
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
        # fallback fixed offset (+03:00) – Saudi has no DST
        try:
            tz = timezone(timedelta(hours=3))
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
    """
    If value looks like a ratio (between -1 and 1), convert to percent.
    Otherwise keep as-is.
    """
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


def _compute_value_traded(price: Any, volume: Any) -> Optional[float]:
    p = _safe_float_or_none(price)
    v = _safe_float_or_none(volume)
    if p is None or v is None:
        return None
    try:
        return round(p * v, 4)
    except Exception:
        return None


def _compute_change_and_pct(price: Any, prev_close: Any) -> Tuple[Optional[float], Optional[float]]:
    p = _safe_float_or_none(price)
    pc = _safe_float_or_none(prev_close)
    if p is None or pc is None or pc == 0:
        return None, None
    ch = p - pc
    pct = (p / pc - 1.0) * 100.0
    return round(ch, 6), round(pct, 6)


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
    # v2 canonical forecast fields
    if fair is None:
        fair = _safe_float_or_none(_safe_get(payload, "forecast_price_3m"))
    if fair is None:
        fair = _safe_float_or_none(_safe_get(payload, "forecast_price_12m"))
    # legacy expected_price fields (some providers may still send them)
    if fair is None:
        fair = _safe_float_or_none(_safe_get(payload, "expected_price_3m"))
    if fair is None:
        fair = _safe_float_or_none(_safe_get(payload, "expected_price_12m"))
    # technical proxies
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


def _payload_symbol_key(p: Dict[str, Any]) -> str:
    try:
        for k in ("symbol_normalized", "symbol", "ticker", "code"):
            v = p.get(k)
            if v is not None:
                s = str(v).strip().upper()
                if s:
                    return s
    except Exception:
        pass
    return ""


def _origin_default_from_payload(p: Dict[str, Any]) -> str:
    sym = str(p.get("symbol_normalized") or p.get("symbol") or "").upper()
    mkt = str(p.get("market") or p.get("market_region") or "").upper()
    if sym.endswith(".SR") or mkt == "KSA" or mkt == "SA" or mkt == "TADAWUL":
        return "KSA_TADAWUL"
    if sym.startswith("^") or "=" in sym:
        return "GLOBAL_MARKETS"
    return "GLOBAL_MARKETS"


def _has_price(p: Dict[str, Any]) -> bool:
    return _safe_get(p, "current_price", "last_price", "price") is not None


def _compute_data_quality(p: Dict[str, Any]) -> str:
    """
    Minimal, stable heuristic (do not overfit):
      - MISSING: no price
      - GOOD: price + (market_cap OR volume) + (prev_close OR day_high/day_low)
      - PARTIAL: otherwise (but has price)
    """
    if not _has_price(p):
        return "MISSING"
    has_liq = _safe_get(p, "market_cap") is not None or _safe_get(p, "volume", "vol") is not None
    has_ref = _safe_get(p, "previous_close", "prev_close", "prior_close") is not None or (
        _safe_get(p, "day_high") is not None and _safe_get(p, "day_low") is not None
    )
    return "GOOD" if (has_liq and has_ref) else "PARTIAL"


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
    "listing date": (("listing_date", "ipo_date", "ipo"), None),
    # Prices
    "price": (("current_price", "last_price", "price"), None),
    "last price": (("current_price", "last_price", "price"), None),
    "prev close": (("previous_close", "prev_close", "prior_close"), None),
    "previous close": (("previous_close", "prev_close", "prior_close"), None),
    "change": (("price_change", "change"), None),
    "price change": (("price_change", "change"), None),
    "change %": (("percent_change", "change_percent", "change_pct", "pct_change"), _ratio_to_percent),
    "percent change": (("percent_change", "change_percent", "change_pct", "pct_change"), _ratio_to_percent),
    "day high": (("day_high",), None),
    "day low": (("day_low",), None),
    # 52W
    "52w high": (("week_52_high", "high_52w", "52w_high"), None),
    "52w low": (("week_52_low", "low_52w", "52w_low"), None),
    "52w position %": (("position_52w_percent", "position_52w"), _ratio_to_percent),
    # Volume/Liquidity
    "volume": (("volume", "vol"), None),
    "avg vol 30d": (("avg_volume_30d", "avg_volume"), None),
    "avg volume (30d)": (("avg_volume_30d", "avg_volume"), None),
    "value traded": (("value_traded", "traded_value", "turnover_value"), None),
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
    "fair value": (
        ("fair_value", "forecast_price_3m", "forecast_price_12m", "expected_price_3m", "expected_price_12m", "ma200", "ma50"),
        None,
    ),
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
    "data source": (("data_source", "source", "provider"), None),
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

        # --- Normalize core identity
        try:
            if not p.get("symbol") and p.get("symbol_normalized"):
                p["symbol"] = p.get("symbol_normalized")
        except Exception:
            pass

        # --- Recommendation enum
        try:
            p["recommendation"] = _normalize_recommendation(p.get("recommendation"))
        except Exception:
            p["recommendation"] = "HOLD"

        # --- Ensure timestamps
        try:
            if not p.get("last_updated_utc") and p.get("as_of_utc"):
                p["last_updated_utc"] = p.get("as_of_utc")
            if p.get("last_updated_utc"):
                p["last_updated_utc"] = _iso_or_none(p.get("last_updated_utc")) or p.get("last_updated_utc")
        except Exception:
            pass

        # Fill Riyadh timestamp if possible
        try:
            if not p.get("last_updated_riyadh"):
                p["last_updated_riyadh"] = _to_riyadh_iso(p.get("last_updated_utc")) or ""
        except Exception:
            pass

        # --- Compute Change / Change % if missing (prefer provider values if present)
        try:
            price = _safe_get(p, "current_price", "last_price", "price")
            prev = _safe_get(p, "previous_close", "prev_close", "prior_close")
            ch = _safe_get(p, "price_change", "change")
            pct = _safe_get(p, "percent_change", "change_percent", "change_pct", "pct_change")

            if ch is None or pct is None:
                ch2, pct2 = _compute_change_and_pct(price, prev)
                if ch is None and ch2 is not None:
                    p["price_change"] = ch2
                if pct is None and pct2 is not None:
                    p["percent_change"] = pct2  # store as percent (0..100)
        except Exception:
            pass

        # --- Compute Value Traded if missing (price * volume)
        try:
            if p.get("value_traded") is None:
                price = _safe_get(p, "current_price", "last_price", "price")
                vol = _safe_get(p, "volume", "vol")
                vt = _compute_value_traded(price, vol)
                if vt is not None:
                    p["value_traded"] = vt
        except Exception:
            pass

        # --- Compute 52W position if missing
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

        # --- Compute turnover/free-float mkt cap/liquidity score if missing
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

        # --- Fair value / upside / label if missing
        try:
            if p.get("fair_value") is None or p.get("upside_percent") is None or p.get("valuation_label") is None:
                fair, upside, label = _compute_fair_value_and_upside(p)
                if p.get("fair_value") is None and fair is not None:
                    p["fair_value"] = fair
                if p.get("upside_percent") is None and upside is not None:
                    p["upside_percent"] = upside  # store as percent
                if p.get("valuation_label") is None and label is not None:
                    p["valuation_label"] = label
        except Exception:
            pass

        # --- origin default
        try:
            if not p.get("origin"):
                p["origin"] = _origin_default_from_payload(p)
        except Exception:
            pass

        # --- data_quality default
        try:
            if not p.get("data_quality"):
                p["data_quality"] = _compute_data_quality(p)
        except Exception:
            pass

        # --- overall_score default
        try:
            if p.get("overall_score") is None:
                p["overall_score"] = p.get("opportunity_score")
        except Exception:
            pass

        return cls(p)

    def _origin_default(self) -> str:
        return _origin_default_from_payload(self.payload)

    def _value_for_header(self, header: str) -> Any:
        sch = _try_import_schemas()
        hk = _hkey(header)

        # Schema-driven candidates (preferred if available)
        candidates: Tuple[str, ...] = ()
        if sch is not None:
            try:
                fn = getattr(sch, "header_field_candidates", None)
                if callable(fn):
                    candidates = tuple(fn(header))  # type: ignore[misc]
            except Exception:
                candidates = ()

        # Internal robust mapping
        spec = _HEADER_MAP.get(hk)
        if spec:
            fields, transform = spec

            # computed: origin
            if hk == "origin":
                v = _safe_get(self.payload, *fields)
                return v if v is not None else self._origin_default()

            # computed: change / change % (always safe)
            if hk in ("change", "price change"):
                v = _safe_get(self.payload, *fields)
                if v is not None:
                    return v
                ch2, _pct2 = _compute_change_and_pct(
                    _safe_get(self.payload, "current_price", "last_price", "price"),
                    _safe_get(self.payload, "previous_close", "prev_close", "prior_close"),
                )
                if ch2 is not None:
                    try:
                        self.payload["price_change"] = ch2
                    except Exception:
                        pass
                return ch2

            if hk in ("change %", "percent change"):
                v = _safe_get(self.payload, *fields)
                if v is not None:
                    return transform(v) if transform else v
                _ch2, pct2 = _compute_change_and_pct(
                    _safe_get(self.payload, "current_price", "last_price", "price"),
                    _safe_get(self.payload, "previous_close", "prev_close", "prior_close"),
                )
                if pct2 is not None:
                    try:
                        self.payload["percent_change"] = pct2
                    except Exception:
                        pass
                return pct2

            # computed: value traded
            if hk == "value traded":
                v = _safe_get(self.payload, *fields)
                if v is not None:
                    return v
                vt = _compute_value_traded(
                    _safe_get(self.payload, "current_price", "last_price", "price"),
                    _safe_get(self.payload, "volume", "vol"),
                )
                if vt is not None:
                    try:
                        self.payload["value_traded"] = vt
                    except Exception:
                        pass
                return vt

            # computed: 52W position %
            if hk in ("52w position %", "52w position"):
                v = _safe_get(self.payload, *fields)
                if v is not None:
                    return transform(v) if transform else v
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

            # computed: last updated (utc) iso
            if hk == "last updated (utc)":
                v = self.payload.get("last_updated_utc") or self.payload.get("as_of_utc")
                if not v:
                    v = _now_utc().isoformat()
                    try:
                        self.payload["last_updated_utc"] = v
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

        # If schema candidates exist, use them (best-effort transforms for known pct/vol/time headers)
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
                        "52w position %",
                    }:
                        return _ratio_to_percent(val)

                    if hk in ("volatility 30d", "volatility (30d)"):
                        return _vol_to_percent(val)

                    if hk == "last updated (utc)":
                        return _iso_or_none(val) or val

                    return val

        # Absolute last fallback: snake-ish guess
        guess = str(header or "").strip()
        return self.payload.get(guess)

    def to_row(self, headers: Sequence[str]) -> List[Any]:
        hs = [str(h) for h in (headers or [])]
        row = [self._value_for_header(h) for h in hs]

        # Ensure recommendation column is enum if present
        try:
            for i, h in enumerate(hs):
                if str(h).strip().lower() == "recommendation":
                    row[i] = _normalize_recommendation(row[i])
                    break
        except Exception:
            pass

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
    # 1) app.state.engine (preferred)
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
                    res = _unwrap_engine_container(res)
                    return _unwrap_tuple_payload(res), f"app.state.engine.{fn_name}", None
                except Exception as e:
                    last_err = _safe_error_message(e)
                    continue
        return None, "app.state.engine", last_err or "engine call failed"

    # 2) v2 singleton accessor (CORRECT for v2.10.x): get_engine().get_enriched_quote()
    try:
        from core.data_engine_v2 import get_engine  # type: ignore

        eng2 = get_engine()
        fn = getattr(eng2, "get_enriched_quote", None) or getattr(eng2, "get_quote", None)
        if callable(fn):
            res2 = await _maybe_await(fn(symbol))
            res2 = _unwrap_engine_container(res2)
            return _unwrap_tuple_payload(res2), "core.data_engine_v2.get_engine().get_enriched_quote", None
    except Exception:
        pass

    # 3) v2 temp engine (fallback)
    try:
        from core.data_engine_v2 import DataEngine as V2Engine  # type: ignore

        tmp = V2Engine()
        try:
            fn = getattr(tmp, "get_enriched_quote", None) or getattr(tmp, "get_quote", None)
            if callable(fn):
                res3 = await _maybe_await(fn(symbol))
                res3 = _unwrap_engine_container(res3)
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

    # 4) legacy module-level singleton (v1)
    try:
        from core.data_engine import get_enriched_quote as v1_get  # type: ignore

        res4 = await _maybe_await(v1_get(symbol))
        res4 = _unwrap_engine_container(res4)
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

    # 1) app.state.engine batch (preferred)
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
                    res = _unwrap_engine_container(res)
                    res = _unwrap_tuple_payload(res)
                    if isinstance(res, (list, dict)):
                        return res, f"app.state.engine.{fn_name}", None
                    last_err = "batch returned non-list/non-dict"
                except Exception as e:
                    last_err = _safe_error_message(e)
        return None, "app.state.engine(batch)", last_err or "batch call failed"

    # 2) v2 singleton accessor batch (CORRECT for v2.10.x)
    try:
        from core.data_engine_v2 import get_engine  # type: ignore

        eng2 = get_engine()
        fn = getattr(eng2, "get_enriched_quotes", None) or getattr(eng2, "get_quotes", None)
        if callable(fn):
            res2 = await _maybe_await(fn(symbols_norm))
            res2 = _unwrap_engine_container(res2)
            res2 = _unwrap_tuple_payload(res2)
            if isinstance(res2, (list, dict)):
                return res2, "core.data_engine_v2.get_engine().get_enriched_quotes", None
            return None, "core.data_engine_v2.get_engine().get_enriched_quotes", "batch returned non-list/non-dict"
    except Exception:
        pass

    return None, None, None


# =============================================================================
# Finalization rules for API payloads
# =============================================================================
def _finalize_payload(payload: Dict[str, Any], *, raw: str, norm: str, source: str, request_id: str) -> Dict[str, Any]:
    sym = (norm or raw or "").strip().upper() or ""

    try:
        payload.setdefault("symbol", sym)
        payload["requested_symbol"] = payload.get("requested_symbol") or raw
        payload["symbol_input"] = payload.get("symbol_input") or raw
        payload["symbol_normalized"] = payload.get("symbol_normalized") or sym
        payload["request_id"] = payload.get("request_id") or request_id

        # origin default
        if not payload.get("origin"):
            payload["origin"] = _origin_default_from_payload(payload)

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
            payload["data_quality"] = _compute_data_quality(payload)

        # timestamps
        if not payload.get("last_updated_utc"):
            payload["last_updated_utc"] = payload.get("as_of_utc") or _now_utc().isoformat()
        else:
            payload["last_updated_utc"] = _iso_or_none(payload.get("last_updated_utc")) or payload.get("last_updated_utc")

        if not payload.get("last_updated_riyadh"):
            payload["last_updated_riyadh"] = _to_riyadh_iso(payload.get("last_updated_utc")) or ""

        # compute change/change% if missing
        try:
            price = _safe_get(payload, "current_price", "last_price", "price")
            prev = _safe_get(payload, "previous_close", "prev_close", "prior_close")
            if payload.get("price_change") is None or payload.get("percent_change") is None:
                ch, pct = _compute_change_and_pct(price, prev)
                if payload.get("price_change") is None and ch is not None:
                    payload["price_change"] = ch
                if payload.get("percent_change") is None and pct is not None:
                    payload["percent_change"] = pct
        except Exception:
            pass

        # compute value_traded if missing
        try:
            if payload.get("value_traded") is None:
                payload["value_traded"] = _compute_value_traded(
                    _safe_get(payload, "current_price", "last_price", "price"),
                    _safe_get(payload, "volume", "vol"),
                )
        except Exception:
            pass

        # 52w position if missing
        if payload.get("position_52w_percent") is None:
            payload["position_52w_percent"] = _compute_52w_position_pct(
                _safe_get(payload, "current_price", "last_price", "price"),
                _safe_get(payload, "week_52_low", "low_52w", "52w_low"),
                _safe_get(payload, "week_52_high", "high_52w", "52w_high"),
            )

        # turnover / free float mkt cap / liquidity score
        if payload.get("turnover_percent") is None:
            payload["turnover_percent"] = _compute_turnover_percent(payload.get("volume"), payload.get("shares_outstanding"))

        if payload.get("free_float_market_cap") is None:
            payload["free_float_market_cap"] = _compute_free_float_mkt_cap(payload.get("market_cap"), payload.get("free_float"))

        if payload.get("liquidity_score") is None:
            payload["liquidity_score"] = _compute_liquidity_score(payload.get("value_traded"))

        # fair/upside/label if missing
        try:
            if payload.get("fair_value") is None or payload.get("upside_percent") is None or payload.get("valuation_label") is None:
                fair, upside, label = _compute_fair_value_and_upside(payload)
                if payload.get("fair_value") is None and fair is not None:
                    payload["fair_value"] = fair
                if payload.get("upside_percent") is None and upside is not None:
                    payload["upside_percent"] = upside
                if payload.get("valuation_label") is None and label is not None:
                    payload["valuation_label"] = label
        except Exception:
            pass

        # overall_score fallback
        if payload.get("overall_score") is None:
            payload["overall_score"] = payload.get("opportunity_score")

        return _schema_fill_best_effort(payload)
    except Exception:
        return _schema_fill_best_effort(
            {
                "status": "error",
                "symbol": sym,
                "requested_symbol": raw,
                "symbol_input": raw,
                "symbol_normalized": sym,
                "recommendation": "HOLD",
                "data_quality": "MISSING",
                "data_source": source or "unknown",
                "error": "payload_finalize_failed",
                "last_updated_utc": _now_utc().isoformat(),
                "last_updated_riyadh": "",
                "request_id": request_id,
            }
        )


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
# Query parsing (supports repeated params)
# =============================================================================
def _extract_symbols_from_request(req: Request, symbols_param: str, tickers_param: str) -> List[str]:
    vals: List[str] = []
    try:
        vals.extend([v for v in req.query_params.getlist("symbols") if v])
    except Exception:
        pass
    try:
        vals.extend([v for v in req.query_params.getlist("tickers") if v])
    except Exception:
        pass

    if not vals:
        if symbols_param:
            vals.append(symbols_param)
        if tickers_param:
            vals.append(tickers_param)

    out: List[str] = []
    for v in vals:
        out.extend(_split_symbols(v))
    # de-dupe while preserving order
    seen = set()
    final: List[str] = []
    for s in out:
        k = s.strip()
        if not k:
            continue
        if k not in seen:
            seen.add(k)
            final.append(k)
    return final


# =============================================================================
# Routes
# =============================================================================
@router.get("/headers", include_in_schema=False)
async def enriched_headers():
    return JSONResponse(status_code=200, content={"status": "success", "headers": ENRICHED_HEADERS_59, "version": ROUTER_VERSION})


@router.get("/quote")
async def enriched_quote(
    request: Request,
    symbol: str = Query("", description="Ticker symbol (AAPL, MSFT.US, 1120.SR, ^GSPC, GC=F)"),
    debug: int = Query(0, description="Set 1 to include traceback (or enable DEBUG_ERRORS=1)"),
    include_headers: int = Query(0, description="Set 1 to include headers in response"),
    include_rows: int = Query(0, description="Set 1 to include sheet-ready row aligned to headers"),
):
    request_id = request.headers.get("X-Request-ID") or str(uuid.uuid4())[:18]
    dbg = _truthy(os.getenv("DEBUG_ERRORS", "0")) or bool(debug)

    if not _is_authorized(request):
        return JSONResponse(
            status_code=200,
            content=_schema_fill_best_effort(
                {
                    "status": "error",
                    "message": "unauthorized",
                    "error": "unauthorized",
                    "request_id": request_id,
                    "time_utc": _now_utc().isoformat(),
                }
            ),
        )

    raw = (symbol or "").strip()
    norm = _normalize_symbol_safe(raw)

    if not raw:
        return JSONResponse(
            status_code=200,
            content=_schema_fill_best_effort(
                {
                    "status": "error",
                    "symbol": "",
                    "requested_symbol": "",
                    "symbol_input": "",
                    "symbol_normalized": "",
                    "recommendation": "HOLD",
                    "data_quality": "MISSING",
                    "data_source": "none",
                    "error": "Empty symbol",
                    "last_updated_utc": _now_utc().isoformat(),
                    "last_updated_riyadh": "",
                    "request_id": request_id,
                }
            ),
        )

    try:
        result, source, err = await _call_engine_best_effort(request, norm or raw)
        if result is None:
            out = _schema_fill_best_effort(
                {
                    "status": "error",
                    "symbol": norm or raw,
                    "requested_symbol": raw,
                    "symbol_input": raw,
                    "symbol_normalized": norm or raw,
                    "recommendation": "HOLD",
                    "data_quality": "MISSING",
                    "data_source": "none",
                    "error": err or "Enriched quote engine not available (no working provider).",
                    "last_updated_utc": _now_utc().isoformat(),
                    "last_updated_riyadh": "",
                    "request_id": request_id,
                }
            )
            return JSONResponse(status_code=200, content=out)

        payload = _as_payload(result)
        payload = _finalize_payload(payload, raw=raw, norm=norm, source=source or "unknown", request_id=request_id)
        return JSONResponse(status_code=200, content=payload)

    except Exception as e:
        out2: Dict[str, Any] = {
            "status": "error",
            "symbol": norm or raw,
            "requested_symbol": raw,
            "symbol_input": raw,
            "symbol_normalized": norm or raw,
            "recommendation": "HOLD",
            "data_quality": "MISSING",
            "data_source": "none",
            "error": _safe_error_message(e),
            "last_updated_utc": _now_utc().isoformat(),
            "last_updated_riyadh": "",
            "request_id": request_id,
        }
        if dbg:
            out2["traceback"] = _clamp(traceback.format_exc(), 8000)
        return JSONResponse(status_code=200, content=_schema_fill_best_effort(out2))


@router.get("/quotes")
async def enriched_quotes(
    request: Request,
    symbols: str = Query("", description="Comma/space-separated symbols OR repeated ?symbols=..."),
    tickers: str = Query("", description="Alias of symbols (supports repeated ?tickers=...)"),
    format: str = Query("items", description="items | sheet"),
    debug: int = Query(0, description="Set 1 to include traceback (or enable DEBUG_ERRORS=1)"),
):
    request_id = request.headers.get("X-Request-ID") or str(uuid.uuid4())[:18]
    dbg = _truthy(os.getenv("DEBUG_ERRORS", "0")) or bool(debug)
    fmt = (format or "items").strip().lower()

    if not _is_authorized(request):
        return JSONResponse(
            status_code=200,
            content={
                "status": "error",
                "message": "unauthorized",
                "error": "unauthorized",
                "request_id": request_id,
                "items": [],
                "count": 0,
            },
        )

    raw_list = _extract_symbols_from_request(request, symbols, tickers)
    if not raw_list:
        return JSONResponse(status_code=200, content={"status": "error", "error": "Empty symbols list", "items": [], "count": 0, "request_id": request_id})

    norms = [(_normalize_symbol_safe(r) or r).strip().upper() for r in raw_list]
    norms = [n for n in norms if n]

    batch_res, batch_source, batch_err = await _call_engine_batch_best_effort(request, norms)

    def _to_sheet(items_payload: List[Dict[str, Any]]) -> Dict[str, Any]:
        rows = [EnrichedQuote.from_unified(p).to_row(ENRICHED_HEADERS_59) for p in items_payload]
        return {
            "status": "success",
            "format": "sheet",
            "headers": ENRICHED_HEADERS_59,
            "rows": rows,
            "count": len(rows),
            "request_id": request_id,
        }

    items: List[Dict[str, Any]] = []

    # FAST PATH: batch returned dict
    if isinstance(batch_res, dict) and batch_res:
        mp: Dict[str, Dict[str, Any]] = {}
        for k, v in batch_res.items():
            pv = _as_payload(_unwrap_tuple_payload(v))
            kk = str(k or "").strip().upper()
            if kk:
                mp.setdefault(kk, pv)
            symk = _payload_symbol_key(pv)
            if symk:
                mp.setdefault(symk, pv)

        for raw, norm in zip(raw_list, norms):
            p = mp.get(norm)
            if p is None:
                out = {
                    "status": "error",
                    "symbol": norm,
                    "requested_symbol": raw,
                    "symbol_input": raw,
                    "symbol_normalized": norm,
                    "recommendation": "HOLD",
                    "data_quality": "MISSING",
                    "data_source": "none",
                    "error": "Engine returned no item for this symbol.",
                    "last_updated_utc": _now_utc().isoformat(),
                    "last_updated_riyadh": "",
                    "request_id": request_id,
                }
                if dbg and batch_err:
                    out["batch_error_hint"] = _clamp(batch_err, 1200)
                items.append(_schema_fill_best_effort(out))
            else:
                items.append(_finalize_payload(p, raw=raw, norm=norm, source=batch_source or "unknown", request_id=request_id))

        if fmt == "sheet":
            return JSONResponse(status_code=200, content=_to_sheet(items))
        return JSONResponse(status_code=200, content={"status": "success", "count": len(items), "items": items, "request_id": request_id})

    # FAST PATH: batch returned list
    if isinstance(batch_res, list) and batch_res:
        payloads: List[Dict[str, Any]] = [_as_payload(_unwrap_tuple_payload(x)) for x in batch_res]

        # if order matches
        if len(payloads) == len(raw_list):
            for i, (raw, norm) in enumerate(zip(raw_list, norms)):
                p = payloads[i] if i < len(payloads) else {}
                items.append(_finalize_payload(p, raw=raw, norm=norm, source=batch_source or "unknown", request_id=request_id))
            if fmt == "sheet":
                return JSONResponse(status_code=200, content=_to_sheet(items))
            return JSONResponse(status_code=200, content={"status": "success", "count": len(items), "items": items, "request_id": request_id})

        # else map by symbol key
        mp2: Dict[str, Dict[str, Any]] = {}
        for p in payloads:
            k = _payload_symbol_key(p)
            if k and k not in mp2:
                mp2[k] = p

        for raw, norm in zip(raw_list, norms):
            p = mp2.get(norm)
            if p is None:
                out = {
                    "status": "error",
                    "symbol": norm,
                    "requested_symbol": raw,
                    "symbol_input": raw,
                    "symbol_normalized": norm,
                    "recommendation": "HOLD",
                    "data_quality": "MISSING",
                    "data_source": "none",
                    "error": "Engine returned no item for this symbol.",
                    "last_updated_utc": _now_utc().isoformat(),
                    "last_updated_riyadh": "",
                    "request_id": request_id,
                }
                if dbg and batch_err:
                    out["batch_error_hint"] = _clamp(batch_err, 1200)
                items.append(_schema_fill_best_effort(out))
            else:
                items.append(_finalize_payload(p, raw=raw, norm=norm, source=batch_source or "unknown", request_id=request_id))

        if fmt == "sheet":
            return JSONResponse(status_code=200, content=_to_sheet(items))
        return JSONResponse(status_code=200, content={"status": "success", "count": len(items), "items": items, "request_id": request_id})

    # SLOW PATH: per-symbol (bounded concurrency + timeout)
    cfg = _cfg()
    sem = asyncio.Semaphore(cfg["concurrency"])

    async def _one(raw: str, norm: str) -> Dict[str, Any]:
        async with sem:
            try:
                result, source, err = await asyncio.wait_for(
                    _call_engine_best_effort(request, norm or raw), timeout=cfg["timeout_sec"]
                )
                if result is None:
                    out = {
                        "status": "error",
                        "symbol": norm or raw,
                        "requested_symbol": raw,
                        "symbol_input": raw,
                        "symbol_normalized": norm or raw,
                        "recommendation": "HOLD",
                        "data_quality": "MISSING",
                        "data_source": "none",
                        "error": err or "Engine not available for this symbol.",
                        "last_updated_utc": _now_utc().isoformat(),
                        "last_updated_riyadh": "",
                        "request_id": request_id,
                    }
                    if dbg and batch_err:
                        out["batch_error_hint"] = _clamp(batch_err, 1200)
                    return _schema_fill_best_effort(out)

                payload = _as_payload(result)
                return _finalize_payload(payload, raw=raw, norm=norm, source=source or "unknown", request_id=request_id)

            except asyncio.TimeoutError:
                return _schema_fill_best_effort(
                    {
                        "status": "error",
                        "symbol": norm or raw,
                        "requested_symbol": raw,
                        "symbol_input": raw,
                        "symbol_normalized": norm or raw,
                        "recommendation": "HOLD",
                        "data_quality": "MISSING",
                        "data_source": "none",
                        "error": "timeout",
                        "last_updated_utc": _now_utc().isoformat(),
                        "last_updated_riyadh": "",
                        "request_id": request_id,
                    }
                )

            except Exception as e:
                out2: Dict[str, Any] = {
                    "status": "error",
                    "symbol": norm or raw,
                    "requested_symbol": raw,
                    "symbol_input": raw,
                    "symbol_normalized": norm or raw,
                    "recommendation": "HOLD",
                    "data_quality": "MISSING",
                    "data_source": "none",
                    "error": _safe_error_message(e),
                    "last_updated_utc": _now_utc().isoformat(),
                    "last_updated_riyadh": "",
                    "request_id": request_id,
                }
                if dbg:
                    out2["traceback"] = _clamp(traceback.format_exc(), 8000)
                    if batch_err:
                        out2["batch_error_hint"] = _clamp(batch_err, 1200)
                return _schema_fill_best_effort(out2)

    items = await asyncio.gather(*[_one(r, n) for r, n in zip(raw_list, norms)])

    if fmt == "sheet":
        return JSONResponse(status_code=200, content=_to_sheet(items))
    return JSONResponse(status_code=200, content={"status": "success", "count": len(items), "items": items, "request_id": request_id})


@router.get("/health", include_in_schema=False)
async def enriched_health():
    return {"status": "ok", "module": "enriched_quote", "version": ROUTER_VERSION, "time_utc": _now_utc().isoformat()}


def get_router() -> APIRouter:
    return router


__all__ = ["router", "get_router", "EnrichedQuote", "ENRICHED_HEADERS_59"]
