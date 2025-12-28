# core/providers/yahoo_chart_provider.py  (FULL REPLACEMENT)
"""
core/providers/yahoo_chart_provider.py
------------------------------------------------------------
Yahoo Quote/Chart Provider (KSA-safe) — v1.6.1 (PATCH-ALIGNED + RETRY + CLIENT REUSE + 52W GUARDS)

Goals
- Reliable quote source for KSA (.SR) without yfinance.
- Prefer Yahoo v7 quote endpoint (fast, rich fields).
- Fallback to Yahoo v8 chart endpoint (robust when quote is missing/blocked).
- Optional "supplement" mode: if quote is partial, enrich missing fields via chart.
- Never do network at import time.
- Export PATCH-style functions expected by DataEngineV2.
- Reuse a single AsyncClient (keep-alive) + lightweight retries for transient statuses.

Exports (engine-compatible)
- fetch_quote_patch(symbol, debug=False) -> (patch: dict, err: str|None)
- fetch_quote_and_enrichment_patch(symbol, debug=False) -> (patch, err)
- fetch_enriched_quote_patch(symbol, debug=False) -> (patch, err)
- fetch_quote(symbol, debug=False)  (compat; returns rich dict with status/error)
- get_quote(symbol, debug=False)    (alias)
- aclose_yahoo_client()            (best-effort close hook)
- YahooChartProvider (class wrapper)
"""

from __future__ import annotations

import asyncio
import math
import os
import time
from dataclasses import dataclass
from typing import Any, Dict, Optional, Tuple, List

import httpx

PROVIDER_VERSION = "1.6.1"

# ---------------------------
# HTTP config
# ---------------------------
UA = os.getenv(
    "YAHOO_UA",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
)

_ACCEPT_LANGUAGE = os.getenv("YAHOO_ACCEPT_LANGUAGE", "en-US,en;q=0.9")

_TIMEOUT_S = float(os.getenv("YAHOO_TIMEOUT_S", "8.5") or "8.5")
TIMEOUT = httpx.Timeout(timeout=_TIMEOUT_S, connect=min(5.0, _TIMEOUT_S))

# Retries for transient Yahoo responses
_RETRY_MAX = int(os.getenv("YAHOO_RETRY_MAX", "2") or "2")  # additional retries (0 => no retry)
_RETRY_BACKOFF_MS = float(os.getenv("YAHOO_RETRY_BACKOFF_MS", "250") or "250")  # base backoff
_RETRY_STATUSES = {429, 500, 502, 503, 504}

# If quote is partial, supplement missing fields from chart (default ON)
ENABLE_SUPPLEMENT = str(os.getenv("ENABLE_YAHOO_CHART_SUPPLEMENT", "true")).strip().lower() in {
    "1", "true", "yes", "y", "on", "t"
}

# Allow alternative hosts (comma-separated): query2.finance.yahoo.com, etc.
_DEFAULT_QUOTE_URL = "https://query1.finance.yahoo.com/v7/finance/quote"
_DEFAULT_CHART_URL = "https://query1.finance.yahoo.com/v8/finance/chart/{symbol}"

_ALT_HOSTS = [h.strip() for h in (os.getenv("YAHOO_ALT_HOSTS", "") or "").split(",") if h.strip()]

QUOTE_URLS: List[str] = [_DEFAULT_QUOTE_URL]
CHART_URLS: List[str] = [_DEFAULT_CHART_URL]

for host in _ALT_HOSTS:
    if "://" in host:
        base = host.rstrip("/")
        QUOTE_URLS.append(f"{base}/v7/finance/quote")
        CHART_URLS.append(f"{base}/v8/finance/chart/{{symbol}}")
    else:
        QUOTE_URLS.append(f"https://{host}/v7/finance/quote")
        CHART_URLS.append(f"https://{host}/v8/finance/chart/{{symbol}}")


# ---------------------------
# Client reuse (keep-alive)
# ---------------------------
_CLIENT: Optional[httpx.AsyncClient] = None
_CLIENT_LOCK = asyncio.Lock()


def _headers_for(symbol: str) -> Dict[str, str]:
    sym = (symbol or "").strip().upper()
    ref = f"https://finance.yahoo.com/quote/{sym}"
    return {
        "User-Agent": UA,
        "Accept": "application/json,text/plain,*/*",
        "Accept-Language": _ACCEPT_LANGUAGE,
        "Connection": "keep-alive",
        "Referer": ref,
        "Origin": "https://finance.yahoo.com",
    }


async def _get_client(symbol: str) -> httpx.AsyncClient:
    global _CLIENT
    if _CLIENT is not None:
        return _CLIENT
    async with _CLIENT_LOCK:
        if _CLIENT is None:
            _CLIENT = httpx.AsyncClient(timeout=TIMEOUT, follow_redirects=True)
    return _CLIENT


async def aclose_yahoo_client() -> None:
    global _CLIENT
    c = _CLIENT
    _CLIENT = None
    if c is not None:
        try:
            await c.aclose()
        except Exception:
            pass


# ---------------------------
# Helpers
# ---------------------------
def _is_nan(x: Any) -> bool:
    try:
        return x is None or (isinstance(x, float) and math.isnan(x))
    except Exception:
        return False


def _to_float(x: Any) -> Optional[float]:
    try:
        if _is_nan(x) or x is None:
            return None
        if isinstance(x, bool):
            return None
        return float(x)
    except Exception:
        return None


def _last_non_null(vals: Any) -> Optional[float]:
    try:
        if not isinstance(vals, list) or not vals:
            return None
        for v in reversed(vals):
            f = _to_float(v)
            if f is not None:
                return f
        return None
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
    # if user passes numeric Tadawul code, normalize to .SR
    if s.isdigit():
        return f"{s}.SR"
    return s


def _base_patch(symbol: str) -> Dict[str, Any]:
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
    need = ("day_high", "day_low", "volume", "previous_close")
    for k in need:
        if _to_float(p.get(k)) is None:
            return True
    return False


def _guard_52w(p: Dict[str, Any]) -> None:
    """
    Yahoo sometimes returns fiftyTwoWeekLow = 0.0 for KSA.
    If low==0 while price is normal, treat as missing (None).
    """
    cur = _to_float(p.get("current_price"))
    hi = _to_float(p.get("week_52_high"))
    lo = _to_float(p.get("week_52_low"))

    if lo is not None and lo == 0.0 and cur is not None and cur > 1.0:
        # almost certainly bogus for equities
        p["week_52_low"] = None

    # if hi exists but lo missing, don't compute position
    # (engine may compute too; keep ours safe)
    if _to_float(p.get("week_52_low")) is None:
        p["position_52w_percent"] = None


async def _http_get_json(
    url: str,
    symbol: str,
    params: Optional[Dict[str, Any]] = None,
) -> Tuple[Optional[dict], Optional[str]]:
    client = await _get_client(symbol)
    headers = _headers_for(symbol)

    last_err: Optional[str] = None
    attempts = 1 + max(0, _RETRY_MAX)

    for i in range(attempts):
        try:
            r = await client.get(url, params=params, headers=headers)

            if r.status_code == 200:
                try:
                    js = r.json()
                    if isinstance(js, dict):
                        return js, None
                    last_err = "unexpected JSON type"
                except Exception as je:
                    last_err = f"JSON decode failed: {je.__class__.__name__}"

            else:
                if r.status_code in _RETRY_STATUSES and i < attempts - 1:
                    last_err = f"HTTP {r.status_code}"
                else:
                    return None, f"HTTP {r.status_code}"

        except Exception as e:
            last_err = f"{e.__class__.__name__}: {e}"

        if i < attempts - 1:
            delay = (_RETRY_BACKOFF_MS / 1000.0) * (2 ** i)
            jitter = (time.time() % 0.2)
            await asyncio.sleep(min(2.0, delay + jitter))

    return None, last_err or "request failed"


async def _http_get_json_multi(
    urls: List[str],
    symbol: str,
    params: Optional[Dict[str, Any]] = None,
) -> Tuple[Optional[dict], Optional[str], Optional[str]]:
    last_err = None
    for u in urls:
        js, err = await _http_get_json(u, symbol, params=params)
        if js is not None:
            return js, None, u
        last_err = err
    return None, last_err or "all hosts failed", None


# ---------------------------
# Providers
# ---------------------------
async def _fetch_from_quote(symbol: str, debug: bool = False) -> Tuple[Dict[str, Any], Optional[str]]:
    patch = _base_patch(symbol)
    params = {
        "symbols": symbol,
        "formatted": "false",
        "region": "SA" if symbol.endswith(".SR") else "US",
        "lang": "en-US",
    }

    js, err, used = await _http_get_json_multi(QUOTE_URLS, symbol, params=params)
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
        patch["_debug_quote"] = {
            "used_url": used,
            "quoteType": q.get("quoteType"),
            "exchange": q.get("exchange"),
            "fullExchangeName": q.get("fullExchangeName"),
            "marketState": q.get("marketState"),
        }

    _guard_52w(patch)
    _fill_derived(patch)
    return patch, None


async def _fetch_from_chart(symbol: str, debug: bool = False) -> Tuple[Dict[str, Any], Optional[str]]:
    patch = _base_patch(symbol)

    params = {
        "range": os.getenv("YAHOO_CHART_RANGE", "5d"),
        "interval": os.getenv("YAHOO_CHART_INTERVAL", "1d"),
        "includePrePost": "false",
        "events": "div|split|earn",
        "corsDomain": "finance.yahoo.com",
    }

    js = None
    last_err = None
    used_url = None

    for templ in CHART_URLS:
        url = templ.format(symbol=symbol)
        js, err, used = await _http_get_json_multi([url], symbol, params=params)
        used_url = used or url
        if js is not None:
            last_err = None
            break
        last_err = err

    if js is None:
        return patch, f"yahoo_chart failed: {last_err}"

    # Yahoo chart can return chart.error
    try:
        chart = js.get("chart") or {}
        if chart.get("error"):
            e = chart.get("error") or {}
            msg = str(e.get("description") or e.get("message") or "chart error").strip()
            return patch, f"yahoo_chart error: {msg}"
    except Exception:
        pass

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

    patch["exchange"] = (meta.get("exchangeName") or meta.get("fullExchangeName") or patch.get("exchange") or None)
    patch["name"] = (meta.get("longName") or meta.get("shortName") or patch.get("name") or None)

    patch["current_price"] = _to_float(meta.get("regularMarketPrice")) or _last_non_null(closes)
    patch["previous_close"] = _to_float(meta.get("previousClose")) or _to_float(meta.get("chartPreviousClose"))
    patch["open"] = _to_float(meta.get("regularMarketOpen")) or _last_non_null(opens)
    patch["day_high"] = _to_float(meta.get("regularMarketDayHigh")) or _last_non_null(highs)
    patch["day_low"] = _to_float(meta.get("regularMarketDayLow")) or _last_non_null(lows)

    patch["week_52_high"] = _to_float(meta.get("fiftyTwoWeekHigh"))
    patch["week_52_low"] = _to_float(meta.get("fiftyTwoWeekLow"))

    patch["volume"] = _to_float(meta.get("regularMarketVolume")) or _last_non_null(vols)

    if debug:
        patch["_debug_chart"] = {
            "used_url": used_url,
            "meta_keys_sample": sorted(list(meta.keys()))[:80],
        }

    _guard_52w(patch)
    _fill_derived(patch)
    return patch, None


# ---------------------------
# Engine-facing PATCH API
# ---------------------------
async def fetch_quote_patch(symbol: str, debug: bool = False) -> Tuple[Dict[str, Any], Optional[str]]:
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
            _guard_52w(p1)
            _fill_derived(p1)

    return p1, None


async def fetch_quote_and_enrichment_patch(symbol: str, debug: bool = False) -> Tuple[Dict[str, Any], Optional[str]]:
    return await fetch_quote_patch(symbol, debug=debug)


async def fetch_enriched_quote_patch(symbol: str, debug: bool = False) -> Tuple[Dict[str, Any], Optional[str]]:
    return await fetch_quote_patch(symbol, debug=debug)


# Back-compat helpers (return a full payload dict)
async def fetch_quote(symbol: str, debug: bool = False) -> Dict[str, Any]:
    patch, err = await fetch_quote_patch(symbol, debug=debug)
    out = dict(patch or {})
    has_price = _to_float(out.get("current_price")) is not None
    out.setdefault("data_quality", "OK" if has_price else "BAD")
    out["status"] = "success" if has_price else "error"
    out["error"] = "" if (not err) else str(err)
    return out


async def get_quote(symbol: str, debug: bool = False) -> Dict[str, Any]:
    return await fetch_quote(symbol, debug=debug)


@dataclass
class YahooChartProvider:
    name: str = "yahoo_chart"

    async def fetch_quote(self, symbol: str, debug: bool = False) -> Dict[str, Any]:
        return await fetch_quote(symbol, debug=debug)


__all__ = [
    "PROVIDER_VERSION",
    "fetch_quote_patch",
    "fetch_quote_and_enrichment_patch",
    "fetch_enriched_quote_patch",
    "fetch_quote",
    "get_quote",
    "aclose_yahoo_client",
    "YahooChartProvider",
]
