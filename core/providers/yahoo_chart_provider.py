"""
core/providers/yahoo_chart_provider.py
------------------------------------------------------------
Yahoo Quote/Chart Provider (KSA-safe) — v1.4.0 (PATCH-ALIGNED)

Goals
- Reliable quote source for KSA (.SR) without yfinance.
- Prefer Yahoo v7 quote endpoint (fast, rich fields).
- Fallback to Yahoo v8 chart endpoint (robust when quote is missing/blocked).
- Optional "supplement" mode: if quote is partial, enrich missing fields via chart.
- Never do network at import time.
- Export PATCH-style functions expected by DataEngineV2.

Exports (engine-compatible)
- fetch_quote_patch(symbol, debug=False) -> (patch: dict, err: str|None)
- fetch_quote_and_enrichment_patch(symbol, debug=False) -> (patch, err)
- fetch_enriched_quote_patch(symbol, debug=False) -> (patch, err)
- fetch_quote(symbol, debug=False)  (compat; returns rich dict with status/error)
- get_quote(symbol, debug=False)    (alias)
- YahooChartProvider (class wrapper)
"""

from __future__ import annotations

import math
import os
from dataclasses import dataclass
from typing import Any, Dict, Optional, Tuple

import httpx

PROVIDER_VERSION = "1.4.0"

UA = os.getenv(
    "YAHOO_UA",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
)

# Robust timeout split
_TIMEOUT_S = float(os.getenv("YAHOO_TIMEOUT_S", "8.5") or "8.5")
TIMEOUT = httpx.Timeout(timeout=_TIMEOUT_S, connect=min(5.0, _TIMEOUT_S))

# If quote is partial, supplement missing fields from chart (default ON)
ENABLE_SUPPLEMENT = str(os.getenv("ENABLE_YAHOO_CHART_SUPPLEMENT", "true")).strip().lower() in {
    "1",
    "true",
    "yes",
    "y",
    "on",
    "t",
}

QUOTE_URL = "https://query1.finance.yahoo.com/v7/finance/quote"
CHART_URL = "https://query1.finance.yahoo.com/v8/finance/chart/{symbol}"


def _is_nan(x: Any) -> bool:
    try:
        return x is None or (isinstance(x, float) and math.isnan(x))
    except Exception:
        return False


def _to_float(x: Any) -> Optional[float]:
    try:
        if _is_nan(x) or x is None:
            return None
        return float(x)
    except Exception:
        return None


def _position_52w(cur: Optional[float], lo: Optional[float], hi: Optional[float]) -> Optional[float]:
    if cur is None or lo is None or hi is None:
        return None
    if hi == lo:
        return None
    return (cur - lo) / (hi - lo) * 100.0


def _normalize_symbol(symbol: str) -> str:
    return (symbol or "").strip().upper()


async def _http_get_json(url: str, params: Optional[Dict[str, Any]] = None) -> Tuple[Optional[dict], Optional[str]]:
    headers = {
        "User-Agent": UA,
        "Accept": "application/json,text/plain,*/*",
        "Accept-Language": os.getenv("YAHOO_ACCEPT_LANGUAGE", "en-US,en;q=0.9"),
        "Connection": "keep-alive",
    }
    try:
        async with httpx.AsyncClient(timeout=TIMEOUT, headers=headers, follow_redirects=True) as client:
            r = await client.get(url, params=params)
            if r.status_code != 200:
                return None, f"HTTP {r.status_code}"
            return r.json(), None
    except Exception as e:
        return None, f"{e.__class__.__name__}: {e}"


def _base_patch(symbol: str) -> Dict[str, Any]:
    """
    PATCH-style output aligned to DataEngineV2 keys.
    Only include fields that we can provide; engine will fill rest.
    """
    is_ksa = symbol.endswith(".SR")
    return {
        "symbol": symbol,
        "market": "KSA" if is_ksa else "GLOBAL",
        "currency": "SAR" if is_ksa else None,
        "data_source": "yahoo_chart",
        "provider_version": PROVIDER_VERSION,
        # identity
        "name": None,
        "exchange": None,
        # price / trading
        "current_price": None,
        "previous_close": None,
        "open": None,
        "day_high": None,
        "day_low": None,
        "week_52_high": None,
        "week_52_low": None,
        "position_52w_percent": None,
        "volume": None,
        "avg_volume_30d": None,
        "market_cap": None,
        "value_traded": None,
        "price_change": None,
        "percent_change": None,
    }


def _fill_derived(p: Dict[str, Any]) -> None:
    cur = _to_float(p.get("current_price"))
    prev = _to_float(p.get("previous_close"))
    vol = _to_float(p.get("volume"))

    if p.get("price_change") is None and cur is not None and prev is not None:
        p["price_change"] = cur - prev

    if p.get("percent_change") is None and cur is not None and prev not in (None, 0.0):
        try:
            p["percent_change"] = (cur - prev) / prev * 100.0
        except Exception:
            pass

    if p.get("value_traded") is None and cur is not None and vol is not None:
        p["value_traded"] = cur * vol

    if p.get("position_52w_percent") is None:
        hi = _to_float(p.get("week_52_high"))
        lo = _to_float(p.get("week_52_low"))
        p["position_52w_percent"] = _position_52w(cur, lo, hi)


def _is_patch_partial(p: Dict[str, Any]) -> bool:
    # If these are missing, chart supplement is useful
    need = ("day_high", "day_low", "volume", "previous_close")
    for k in need:
        if _to_float(p.get(k)) is None:
            return True
    return False


async def _fetch_from_quote(symbol: str, debug: bool = False) -> Tuple[Dict[str, Any], Optional[str]]:
    patch = _base_patch(symbol)
    params = {
        "symbols": symbol,
        "formatted": "false",
        "region": "SA" if symbol.endswith(".SR") else "US",
        "lang": "en-US",
    }

    js, err = await _http_get_json(QUOTE_URL, params=params)
    if js is None:
        return patch, f"yahoo_quote failed: {err}"

    try:
        res = (js.get("quoteResponse") or {}).get("result") or []
        if not res:
            return patch, "yahoo_quote empty result"
        q = res[0] or {}
    except Exception:
        return patch, "yahoo_quote parse error"

    patch["name"] = (q.get("longName") or q.get("shortName") or q.get("displayName") or None)
    patch["currency"] = (q.get("currency") or patch.get("currency") or None)
    patch["exchange"] = (q.get("fullExchangeName") or q.get("exchange") or None)

    patch["current_price"] = _to_float(q.get("regularMarketPrice"))
    patch["previous_close"] = _to_float(q.get("regularMarketPreviousClose"))
    patch["open"] = _to_float(q.get("regularMarketOpen"))
    patch["day_high"] = _to_float(q.get("regularMarketDayHigh"))
    patch["day_low"] = _to_float(q.get("regularMarketDayLow"))

    patch["week_52_high"] = _to_float(q.get("fiftyTwoWeekHigh"))
    patch["week_52_low"] = _to_float(q.get("fiftyTwoWeekLow"))

    patch["volume"] = _to_float(q.get("regularMarketVolume"))
    patch["avg_volume_30d"] = _to_float(q.get("averageDailyVolume3Month") or q.get("averageDailyVolume10Day"))
    patch["market_cap"] = _to_float(q.get("marketCap"))

    if debug:
        patch["_debug_quote_fields"] = {
            "quoteType": q.get("quoteType"),
            "exchange": q.get("exchange"),
            "fullExchangeName": q.get("fullExchangeName"),
        }

    _fill_derived(patch)
    return patch, None


async def _fetch_from_chart(symbol: str, debug: bool = False) -> Tuple[Dict[str, Any], Optional[str]]:
    patch = _base_patch(symbol)

    url = CHART_URL.format(symbol=symbol)
    params = {
        "range": os.getenv("YAHOO_CHART_RANGE", "5d"),
        "interval": os.getenv("YAHOO_CHART_INTERVAL", "1d"),
        "includePrePost": "false",
        "events": "div|split|earn",
        "corsDomain": "finance.yahoo.com",
    }

    js, err = await _http_get_json(url, params=params)
    if js is None:
        return patch, f"yahoo_chart failed: {err}"

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
        return patch, "yahoo_chart parse error"

    patch["currency"] = (meta.get("currency") or patch.get("currency") or None)

    # chart meta often has these reliably
    patch["current_price"] = _to_float(meta.get("regularMarketPrice")) or _to_float(closes[-1] if closes else None)
    patch["previous_close"] = _to_float(meta.get("previousClose")) or _to_float(meta.get("chartPreviousClose"))
    patch["open"] = _to_float(meta.get("regularMarketOpen")) or _to_float(opens[-1] if opens else None)
    patch["day_high"] = _to_float(meta.get("regularMarketDayHigh")) or _to_float(highs[-1] if highs else None)
    patch["day_low"] = _to_float(meta.get("regularMarketDayLow")) or _to_float(lows[-1] if lows else None)

    patch["week_52_high"] = _to_float(meta.get("fiftyTwoWeekHigh"))
    patch["week_52_low"] = _to_float(meta.get("fiftyTwoWeekLow"))

    patch["volume"] = _to_float(meta.get("regularMarketVolume")) or _to_float(vols[-1] if vols else None)

    # sometimes available
    patch["exchange"] = (meta.get("exchangeName") or meta.get("fullExchangeName") or patch.get("exchange") or None)

    if debug:
        patch["_debug_chart_meta_keys"] = sorted(list(meta.keys()))[:80]

    _fill_derived(patch)
    return patch, None


async def fetch_quote_patch(symbol: str, debug: bool = False) -> Tuple[Dict[str, Any], Optional[str]]:
    """
    Engine-facing PATCH function.
    """
    sym = _normalize_symbol(symbol)

    # 1) quote endpoint
    p1, e1 = await _fetch_from_quote(sym, debug=debug)

    # 2) fallback to chart if quote fails or missing price
    if e1 or _to_float(p1.get("current_price")) is None:
        p2, e2 = await _fetch_from_chart(sym, debug=debug)
        if not e2 and _to_float(p2.get("current_price")) is not None:
            return p2, None
        return p1, (e1 or e2 or "yahoo_chart fallback failed")

    # 3) optional supplement (quote partial -> fill missing from chart)
    if ENABLE_SUPPLEMENT and _is_patch_partial(p1):
        p2, e2 = await _fetch_from_chart(sym, debug=debug)
        if not e2:
            for k, v in (p2 or {}).items():
                if v is None:
                    continue
                if p1.get(k) is None or p1.get(k) == "":
                    p1[k] = v
            _fill_derived(p1)

    return p1, None


# Aliases expected by DataEngineV2 fn-name list
async def fetch_quote_and_enrichment_patch(symbol: str, debug: bool = False) -> Tuple[Dict[str, Any], Optional[str]]:
    return await fetch_quote_patch(symbol, debug=debug)


async def fetch_enriched_quote_patch(symbol: str, debug: bool = False) -> Tuple[Dict[str, Any], Optional[str]]:
    return await fetch_quote_patch(symbol, debug=debug)


# Back-compat helpers (return a full payload dict)
async def fetch_quote(symbol: str, debug: bool = False) -> Dict[str, Any]:
    patch, err = await fetch_quote_patch(symbol, debug=debug)
    out = dict(patch or {})
    out.setdefault("data_quality", "OK" if _to_float(out.get("current_price")) is not None else "BAD")
    out["status"] = "success" if not err else "error"
    out["error"] = "" if not err else str(err)
    return out


async def get_quote(symbol: str, debug: bool = False) -> Dict[str, Any]:
    return await fetch_quote(symbol, debug=debug)


@dataclass
class YahooChartProvider:
    name: str = "yahoo_chart"

    async def fetch_quote(self, symbol: str, debug: bool = False) -> Dict[str, Any]:
        return await fetch_quote(symbol, debug=debug)
