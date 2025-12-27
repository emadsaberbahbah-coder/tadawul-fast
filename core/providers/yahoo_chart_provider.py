# core/providers/yahoo_chart_provider.py  (FULL REPLACEMENT)
"""
core/providers/yahoo_chart_provider.py
------------------------------------------------------------
Yahoo Quote/Chart Provider (KSA-safe, no yfinance) — v1.4.0 (PROD SAFE)

What this revision fixes / guarantees
- ✅ No misleading placeholder scores (provider only returns market data)
- ✅ Returns a consistent, engine-friendly dict shape (status/data_quality/error/symbol)
- ✅ Quote endpoint first (v7), chart endpoint fallback (v8)
- ✅ Optional "supplement" mode: chart fills missing fields when quote is partial
- ✅ Never does network at import-time
- ✅ Optional numeric KSA normalization: "1120" -> "1120.SR" (configurable)
- ✅ Retries (bounded) for 429/5xx and safe JSON parsing
- ✅ Lazy shared AsyncClient for performance

Supported env vars
- YAHOO_TIMEOUT_S                    (default 8.5)
- YAHOO_RETRY_ATTEMPTS               (default 2)
- YAHOO_UA                           (optional)
- YAHOO_TTL_SEC                      (default 10)  cache TTL
- ENABLE_YAHOO_CHART_SUPPLEMENT      (default true) chart fills blanks
- ENABLE_YAHOO_CHART_KSA             (default true) allow provider for .SR symbols
- ENABLE_YAHOO_CHART_GLOBAL          (default false) allow provider for non-.SR symbols
- YAHOO_ASSUME_KSA_NUMERIC           (default true) "1120" => "1120.SR"

Exports
- fetch_quote(symbol, debug=False)
- get_quote(symbol, debug=False)
- fetch_quote_patch(symbol, *args, **kwargs)                 (engine discovery)
- fetch_enriched_quote_patch(symbol, *args, **kwargs)        (engine discovery)
- fetch_quote_and_enrichment_patch(symbol, *args, **kwargs)  (engine discovery)
- YahooChartProvider (class wrapper)
"""

from __future__ import annotations

import asyncio
import math
import os
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, Optional, Tuple

import httpx

PROVIDER_NAME = "yahoo_chart"
PROVIDER_VERSION = "1.4.0"

QUOTE_URL = "https://query1.finance.yahoo.com/v7/finance/quote"
CHART_URL = "https://query1.finance.yahoo.com/v8/finance/chart/{symbol}"

UA = os.getenv(
    "YAHOO_UA",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
)

TIMEOUT_S = float(os.getenv("YAHOO_TIMEOUT_S", "8.5") or "8.5")
RETRY_ATTEMPTS = int(os.getenv("YAHOO_RETRY_ATTEMPTS", "2") or "2")

TTL_SEC = float(os.getenv("YAHOO_TTL_SEC", "10") or "10")

ENABLE_SUPPLEMENT = str(os.getenv("ENABLE_YAHOO_CHART_SUPPLEMENT", "true")).strip().lower() in {
    "1",
    "true",
    "yes",
    "y",
    "on",
}

ENABLE_KSA = str(os.getenv("ENABLE_YAHOO_CHART_KSA", "true")).strip().lower() in {
    "1",
    "true",
    "yes",
    "y",
    "on",
}

ENABLE_GLOBAL = str(os.getenv("ENABLE_YAHOO_CHART_GLOBAL", "false")).strip().lower() in {
    "1",
    "true",
    "yes",
    "y",
    "on",
}

ASSUME_KSA_NUMERIC = str(os.getenv("YAHOO_ASSUME_KSA_NUMERIC", "true")).strip().lower() in {
    "1",
    "true",
    "yes",
    "y",
    "on",
}

# ---------------------------------------------------------------------
# Lazy shared HTTP client + tiny cache (PROD SAFE)
# ---------------------------------------------------------------------
_CLIENT: Optional[httpx.AsyncClient] = None
_CLIENT_LOCK = asyncio.Lock()

_CACHE: Dict[str, Tuple[float, Dict[str, Any]]] = {}
_CACHE_LOCK = asyncio.Lock()


def _utc_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


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
    s = (symbol or "").strip().upper()
    if not s:
        return ""
    if ASSUME_KSA_NUMERIC and s.isdigit():
        return f"{s}.SR"
    return s


def _is_ksa_symbol(sym: str) -> bool:
    return sym.endswith(".SR") or sym.endswith(".SAU")  # tolerate any internal variant


def _base_payload(symbol: str) -> Dict[str, Any]:
    ksa = _is_ksa_symbol(symbol)
    return {
        "status": "success",
        "symbol": symbol,
        "market": "KSA" if ksa else "GLOBAL",
        "currency": "SAR" if ksa else "",
        "data_source": PROVIDER_NAME,
        "data_quality": "OK",
        "error": "",
        "provider_version": PROVIDER_VERSION,
        "last_updated_utc": _utc_iso(),
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
    }


async def _get_client() -> httpx.AsyncClient:
    global _CLIENT
    if _CLIENT is not None:
        return _CLIENT
    async with _CLIENT_LOCK:
        if _CLIENT is not None:
            return _CLIENT
        timeout = httpx.Timeout(TIMEOUT_S, connect=min(10.0, TIMEOUT_S))
        _CLIENT = httpx.AsyncClient(
            timeout=timeout,
            headers={"User-Agent": UA, "Accept": "application/json,text/plain,*/*"},
            follow_redirects=True,
            limits=httpx.Limits(max_keepalive_connections=20, max_connections=40),
        )
        return _CLIENT


async def aclose_yahoo_client() -> None:
    global _CLIENT
    if _CLIENT is None:
        return
    try:
        await _CLIENT.aclose()
    finally:
        _CLIENT = None


async def _http_get_json(url: str, params: Optional[Dict[str, Any]] = None) -> Tuple[Optional[dict], Optional[str]]:
    client = await _get_client()

    for attempt in range(max(1, RETRY_ATTEMPTS)):
        try:
            r = await client.get(url, params=params)

            if r.status_code == 429 or 500 <= r.status_code < 600:
                if attempt < (RETRY_ATTEMPTS - 1):
                    await asyncio.sleep(0.2 * (2**attempt))
                    continue
                return None, f"HTTP {r.status_code}"

            if r.status_code != 200:
                return None, f"HTTP {r.status_code}"

            try:
                return r.json(), None
            except Exception:
                return None, "invalid JSON"
        except Exception as e:
            if attempt < (RETRY_ATTEMPTS - 1):
                await asyncio.sleep(0.2 * (2**attempt))
                continue
            return None, f"{e.__class__.__name__}: {e}"
    return None, "request failed"


def _fill_derived(out: Dict[str, Any]) -> None:
    cur = _to_float(out.get("current_price"))
    prev = _to_float(out.get("previous_close"))
    vol = _to_float(out.get("volume"))

    if out.get("price_change") is None and cur is not None and prev is not None:
        out["price_change"] = cur - prev

    if out.get("percent_change") is None and cur is not None and prev not in (None, 0.0):
        out["percent_change"] = (cur - prev) / prev * 100.0

    if out.get("value_traded") is None and cur is not None and vol is not None:
        out["value_traded"] = cur * vol

    if out.get("position_52w_percent") is None:
        hi = _to_float(out.get("high_52w"))
        lo = _to_float(out.get("low_52w"))
        out["position_52w_percent"] = _position_52w(cur, lo, hi)


def _finalize_quality(out: Dict[str, Any]) -> None:
    # FULL if we have the typical dashboard minimums
    must = ["current_price", "previous_close", "day_high", "day_low", "volume"]
    has_all = all(_to_float(out.get(k)) is not None for k in must)
    has_price = _to_float(out.get("current_price")) is not None

    if out.get("error"):
        out["status"] = "error"
        out["data_quality"] = "BAD"
        return

    if has_all:
        out["data_quality"] = "FULL"
    elif has_price:
        out["data_quality"] = "OK"
    else:
        out["status"] = "error"
        out["data_quality"] = "BAD"
        out["error"] = "yahoo: missing current_price"


def _should_allow_symbol(sym: str) -> bool:
    if _is_ksa_symbol(sym):
        return bool(ENABLE_KSA)
    return bool(ENABLE_GLOBAL)


async def _cache_get(key: str) -> Optional[Dict[str, Any]]:
    if TTL_SEC <= 0:
        return None
    now = time.time()
    async with _CACHE_LOCK:
        hit = _CACHE.get(key)
        if not hit:
            return None
        exp, payload = hit
        if now >= exp:
            _CACHE.pop(key, None)
            return None
        return dict(payload)


async def _cache_set(key: str, payload: Dict[str, Any]) -> None:
    if TTL_SEC <= 0:
        return
    exp = time.time() + float(TTL_SEC)
    async with _CACHE_LOCK:
        _CACHE[key] = (exp, dict(payload))


async def _fetch_from_quote(symbol: str, debug: bool = False) -> Dict[str, Any]:
    out = _base_payload(symbol)

    js, err = await _http_get_json(QUOTE_URL, params={"symbols": symbol})
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

    # Some chart metas include marketCap; keep best-effort only
    out["market_cap"] = _to_float(meta.get("marketCap"))

    if debug:
        out["_debug_chart_meta_keys"] = sorted(list(meta.keys()))[:80]

    _fill_derived(out)
    return out


def _supplement_fill(dst: Dict[str, Any], src: Dict[str, Any]) -> None:
    """
    Fill dst missing fields from src (never overwrite an existing numeric/string value).
    """
    for k, v in src.items():
        if k.startswith("_"):
            continue
        if v is None:
            continue
        if isinstance(v, str) and not v.strip():
            continue
        cur = dst.get(k)
        if cur is None or (isinstance(cur, str) and not cur.strip()):
            dst[k] = v


async def fetch_quote(symbol: str, debug: bool = False) -> Dict[str, Any]:
    sym = _normalize_symbol(symbol)
    out = _base_payload(sym)

    if not sym:
        out["status"] = "error"
        out["data_quality"] = "BAD"
        out["error"] = "yahoo: empty symbol"
        return out

    if not _should_allow_symbol(sym):
        out["status"] = "error"
        out["data_quality"] = "BAD"
        out["error"] = "yahoo: disabled for this market (check ENABLE_YAHOO_CHART_KSA / ENABLE_YAHOO_CHART_GLOBAL)"
        return out

    ck = f"yahoo::{sym}::debug={1 if debug else 0}"
    cached = await _cache_get(ck)
    if cached:
        return cached

    # 1) Quote endpoint (fast, rich)
    q = await _fetch_from_quote(sym, debug=debug)

    # 2) If quote fails or missing price -> chart fallback
    if q.get("error") or _to_float(q.get("current_price")) is None:
        c = await _fetch_from_chart(sym, debug=debug)
        out = c if not c.get("error") else q
    else:
        out = q
        # 3) Supplement (optional): chart fills blanks if quote is partial
        if ENABLE_SUPPLEMENT:
            c = await _fetch_from_chart(sym, debug=debug)
            if not c.get("error"):
                # keep name from quote if exists
                if out.get("name") and not c.get("name"):
                    c["name"] = out.get("name")
                _supplement_fill(out, c)
                _fill_derived(out)

    _finalize_quality(out)

    # Ensure required metadata
    out["symbol"] = sym
    out["data_source"] = PROVIDER_NAME
    out["provider_version"] = PROVIDER_VERSION
    out["last_updated_utc"] = _utc_iso()

    await _cache_set(ck, out)
    return out


# Convenience alias
async def get_quote(symbol: str, debug: bool = False) -> Dict[str, Any]:
    return await fetch_quote(symbol, debug=debug)


# ---------------------------------------------------------------------
# Engine discovery callables (match other providers)
# ---------------------------------------------------------------------
async def fetch_quote_patch(symbol: str, *args: Any, **kwargs: Any) -> Dict[str, Any]:
    # quote-only (still uses quote endpoint; chart fallback for current_price)
    return await fetch_quote(symbol, debug=bool(kwargs.get("debug", False)))


async def fetch_enriched_quote_patch(symbol: str, *args: Any, **kwargs: Any) -> Dict[str, Any]:
    # same for Yahoo: “enriched” == quote + chart supplement
    # (supplement behavior controlled by ENABLE_YAHOO_CHART_SUPPLEMENT)
    return await fetch_quote(symbol, debug=bool(kwargs.get("debug", False)))


async def fetch_quote_and_enrichment_patch(symbol: str, *args: Any, **kwargs: Any) -> Dict[str, Any]:
    return await fetch_quote(symbol, debug=bool(kwargs.get("debug", False)))


@dataclass
class YahooChartProvider:
    name: str = PROVIDER_NAME

    async def fetch_quote(self, symbol: str, debug: bool = False) -> Dict[str, Any]:
        return await fetch_quote(symbol, debug=debug)


__all__ = [
    "fetch_quote",
    "get_quote",
    "fetch_quote_patch",
    "fetch_enriched_quote_patch",
    "fetch_quote_and_enrichment_patch",
    "YahooChartProvider",
    "aclose_yahoo_client",
    "PROVIDER_VERSION",
]
