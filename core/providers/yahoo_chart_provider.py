"""
core/providers/yahoo_chart_provider.py
------------------------------------------------------------
Yahoo Quote/Chart Provider (KSA-safe) — v1.2.0

Goals
- Provide a reliable quote source for KSA (.SR) without yfinance.
- Prefer Yahoo v7 quote endpoint (fast, rich fields).
- Fallback to Yahoo v8 chart endpoint (robust when quote is missing).
- Never do network at import time.
- Return stable dict keys used by DataEngineV2.

Exports (compat)
- fetch_quote(symbol, debug=False)
- get_quote(symbol, debug=False)
- YahooChartProvider (class wrapper)
"""

from __future__ import annotations

import os
import math
from dataclasses import dataclass
from typing import Any, Dict, Optional, Tuple

import httpx

PROVIDER_VERSION = "1.2.0"

UA = os.getenv(
    "YAHOO_UA",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
)
TIMEOUT_S = float(os.getenv("YAHOO_TIMEOUT_S", "8.5") or "8.5")

ENABLE_SUPPLEMENT = str(os.getenv("ENABLE_YAHOO_CHART_SUPPLEMENT", "true")).strip().lower() in {
    "1",
    "true",
    "yes",
    "y",
    "on",
}

QUOTE_URL = "https://query1.finance.yahoo.com/v7/finance/quote"
CHART_URL = "https://query1.finance.yahoo.com/v8/finance/chart/{symbol}"

TRUTHY = {"1", "true", "yes", "y", "on", "t"}


def _is_nan(x: Any) -> bool:
    try:
        return x is None or (isinstance(x, float) and math.isnan(x))
    except Exception:
        return False


def _to_float(x: Any) -> Optional[float]:
    try:
        if _is_nan(x):
            return None
        if x is None:
            return None
        return float(x)
    except Exception:
        return None


def _safe_div(a: Optional[float], b: Optional[float]) -> Optional[float]:
    if a is None or b in (None, 0.0):
        return None
    try:
        return a / b
    except Exception:
        return None


def _position_52w(cur: Optional[float], lo: Optional[float], hi: Optional[float]) -> Optional[float]:
    if cur is None or lo is None or hi is None:
        return None
    if hi == lo:
        return None
    return (cur - lo) / (hi - lo) * 100.0


def _normalize_symbol(symbol: str) -> str:
    s = (symbol or "").strip().upper()
    # allow user to pass "1120" -> treat as "1120.SR" only if you want that behavior
    # (engine often already normalizes). We keep as-is to avoid surprises.
    return s


async def _http_get_json(url: str, params: Optional[Dict[str, Any]] = None) -> Tuple[Optional[dict], Optional[str]]:
    headers = {"User-Agent": UA, "Accept": "application/json,text/plain,*/*"}
    try:
        async with httpx.AsyncClient(timeout=TIMEOUT_S, headers=headers) as client:
            r = await client.get(url, params=params)
            if r.status_code != 200:
                return None, f"HTTP {r.status_code}"
            return r.json(), None
    except Exception as e:
        return None, f"{e.__class__.__name__}: {e}"


def _base_payload(symbol: str) -> Dict[str, Any]:
    return {
        "status": "ok",
        "symbol": symbol,
        "market": "KSA" if symbol.endswith(".SR") else "GLOBAL",
        "currency": "SAR" if symbol.endswith(".SR") else "",
        "data_source": "yahoo_chart",
        "data_quality": "OK",
        "error": "",
        # common fields
        "name": "",
        "current_price": None,
        "previous_close": None,
        "open": None,
        "day_high": None,
        "day_low": None,
        "high_52w": None,
        "low_52w": None,
        "position_52w_percent": None,
        "volume": None,
        "avg_volume_30d": None,
        "market_cap": None,
        "value_traded": None,
        "price_change": None,
        "percent_change": None,
        # scoring placeholders (engine can overwrite)
        "quality_score": 50,
        "value_score": 50,
        "momentum_score": 50,
        "risk_score": 35,
        "opportunity_score": 45,
        "provider_version": PROVIDER_VERSION,
    }


def _fill_derived(out: Dict[str, Any]) -> None:
    cur = _to_float(out.get("current_price"))
    prev = _to_float(out.get("previous_close"))
    vol = _to_float(out.get("volume"))

    if out.get("price_change") is None and cur is not None and prev is not None:
        out["price_change"] = cur - prev

    if out.get("percent_change") is None and cur is not None and prev is not None and prev != 0:
        out["percent_change"] = (cur - prev) / prev * 100.0

    if out.get("value_traded") is None and cur is not None and vol is not None:
        out["value_traded"] = cur * vol

    if out.get("position_52w_percent") is None:
        hi = _to_float(out.get("high_52w"))
        lo = _to_float(out.get("low_52w"))
        out["position_52w_percent"] = _position_52w(cur, lo, hi)


def _finalize_quality(out: Dict[str, Any]) -> None:
    # FULL if we have key fields typically needed in dashboard
    must = ["current_price", "previous_close", "day_high", "day_low", "volume"]
    ok = all(_to_float(out.get(k)) is not None for k in must)
    out["data_quality"] = "FULL" if ok else "OK"
    out["status"] = "success" if (out.get("error") in (None, "")) else "error"


async def _fetch_from_quote(symbol: str, debug: bool = False) -> Dict[str, Any]:
    out = _base_payload(symbol)
    params = {"symbols": symbol}
    js, err = await _http_get_json(QUOTE_URL, params=params)
    if js is None:
        out["error"] = f"yahoo_quote failed: {err}"
        return out

    try:
        res = (js.get("quoteResponse") or {}).get("result") or []
        if not res:
            out["error"] = "yahoo_quote empty result"
            return out
        q = res[0] or {}
    except Exception:
        out["error"] = "yahoo_quote parse error"
        return out

    # prefer longName/shortName
    out["name"] = (q.get("longName") or q.get("shortName") or q.get("displayName") or "") or ""

    out["currency"] = (q.get("currency") or out.get("currency") or "") or ""
    out["current_price"] = _to_float(q.get("regularMarketPrice"))
    out["previous_close"] = _to_float(q.get("regularMarketPreviousClose"))
    out["open"] = _to_float(q.get("regularMarketOpen"))
    out["day_high"] = _to_float(q.get("regularMarketDayHigh"))
    out["day_low"] = _to_float(q.get("regularMarketDayLow"))
    out["high_52w"] = _to_float(q.get("fiftyTwoWeekHigh"))
    out["low_52w"] = _to_float(q.get("fiftyTwoWeekLow"))
    out["volume"] = _to_float(q.get("regularMarketVolume"))
    out["avg_volume_30d"] = _to_float(q.get("averageDailyVolume3Month") or q.get("averageDailyVolume10Day"))
    out["market_cap"] = _to_float(q.get("marketCap"))

    if debug:
        out["_debug_quote_fields"] = {
            "exchange": q.get("exchange"),
            "fullExchangeName": q.get("fullExchangeName"),
            "quoteType": q.get("quoteType"),
        }

    _fill_derived(out)
    return out


async def _fetch_from_chart(symbol: str, debug: bool = False) -> Dict[str, Any]:
    out = _base_payload(symbol)
    url = CHART_URL.format(symbol=symbol)
    params = {
        "range": "5d",
        "interval": "1d",
        "includePrePost": "false",
        "events": "div|split|earn",
        "corsDomain": "finance.yahoo.com",
    }

    js, err = await _http_get_json(url, params=params)
    if js is None:
        out["error"] = f"yahoo_chart failed: {err}"
        return out

    try:
        res = ((js.get("chart") or {}).get("result") or [None])[0] or {}
        meta = res.get("meta") or {}
        ind = (res.get("indicators") or {}).get("quote") or []
        q0 = ind[0] if ind else {}
        opens = q0.get("open") or []
        highs = q0.get("high") or []
        lows = q0.get("low") or []
        closes = q0.get("close") or []
        vols = q0.get("volume") or []
    except Exception:
        out["error"] = "yahoo_chart parse error"
        return out

    out["currency"] = (meta.get("currency") or out.get("currency") or "") or ""
    out["current_price"] = _to_float(meta.get("regularMarketPrice")) or _to_float(closes[-1] if closes else None)
    out["previous_close"] = _to_float(meta.get("previousClose")) or _to_float(meta.get("chartPreviousClose"))
    out["open"] = _to_float(meta.get("regularMarketOpen")) or _to_float(opens[-1] if opens else None)
    out["day_high"] = _to_float(meta.get("regularMarketDayHigh")) or _to_float(highs[-1] if highs else None)
    out["day_low"] = _to_float(meta.get("regularMarketDayLow")) or _to_float(lows[-1] if lows else None)
    out["high_52w"] = _to_float(meta.get("fiftyTwoWeekHigh"))
    out["low_52w"] = _to_float(meta.get("fiftyTwoWeekLow"))
    out["volume"] = _to_float(meta.get("regularMarketVolume")) or _to_float(vols[-1] if vols else None)

    if debug:
        out["_debug_chart_meta_keys"] = sorted(list(meta.keys()))[:60]

    _fill_derived(out)
    return out


async def fetch_quote(symbol: str, debug: bool = False) -> Dict[str, Any]:
    sym = _normalize_symbol(symbol)

    # 1) Quote endpoint (rich + fast)
    out = await _fetch_from_quote(sym, debug=debug)

    # 2) If quote failed or too empty, fallback to chart
    if out.get("error") or _to_float(out.get("current_price")) is None:
        out2 = await _fetch_from_chart(sym, debug=debug)
        # keep the best pieces
        if not out2.get("error"):
            # preserve name from quote if available
            if out.get("name") and not out2.get("name"):
                out2["name"] = out.get("name")
            out = out2

    _finalize_quality(out)
    return out


# Convenience alias for engines that call get_quote()
async def get_quote(symbol: str, debug: bool = False) -> Dict[str, Any]:
    return await fetch_quote(symbol, debug=debug)


@dataclass
class YahooChartProvider:
    name: str = "yahoo_chart"

    async def fetch_quote(self, symbol: str, debug: bool = False) -> Dict[str, Any]:
        return await fetch_quote(symbol, debug=debug)
