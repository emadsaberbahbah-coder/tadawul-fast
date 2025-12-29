from __future__ import annotations

"""
Yahoo Fundamentals Provider (NO yfinance) — v1.1.0 (PROD SAFE)

Goal:
- Best-effort fundamentals for symbols (KSA .SR + GLOBAL)
- Fill:
    market_cap, pe_ttm, pb, dividend_yield, roe, roa, currency
- NEVER crash the app on import
- Works as supplement provider (after yahoo_chart price) inside DataEngineV2

Why v1.1.0:
- Yahoo quoteSummary is sometimes gated by cookie/crumb flows (common)
- Adds cookie+crumb best-effort + fallback fundamentals-timeseries endpoint

Refs:
- Crumb/cookie gating patterns are widely discussed in yfinance issues. (example) :contentReference[oaicite:3]{index=3}
- fundamentals-timeseries endpoint format used in yfinance scrapers. :contentReference[oaicite:4]{index=4}
"""

import asyncio
import json
import random
import time
from datetime import datetime, timezone
from typing import Any, Dict, Optional, Tuple

import httpx

PROVIDER_VERSION = "1.1.0"

YAHOO_QUOTE_SUMMARY_URL = "https://query1.finance.yahoo.com/v10/finance/quoteSummary/{symbol}"
YAHOO_QUOTE_URL = "https://query1.finance.yahoo.com/v7/finance/quote"

# Timeseries endpoint used by yfinance for Yahoo datastores
YAHOO_FUND_TIMESERIES_URL = (
    "https://query2.finance.yahoo.com/ws/fundamentals-timeseries/v1/finance/timeseries/{symbol}"
)

YAHOO_GETCRUMB_URL = "https://query1.finance.yahoo.com/v1/test/getcrumb"
YAHOO_QUOTE_PAGE_URL = "https://finance.yahoo.com/quote/{symbol}?p={symbol}"

USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/124.0.0.0 Safari/537.36"
)

# ---- crumb cache (best-effort) ----
_CRUMB_LOCK = asyncio.Lock()
_CRUMB_CACHE: Dict[str, Any] = {"crumb": None, "ts": 0.0}
_CRUMB_TTL_SEC = 60 * 30  # 30 minutes


def _now_utc_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _safe_float(x: Any) -> Optional[float]:
    try:
        if x is None or isinstance(x, bool):
            return None
        if isinstance(x, (int, float)):
            v = float(x)
            return None if v != v else v
        s = str(x).strip().replace(",", "")
        if not s or s in {"-", "—", "N/A", "NA", "null", "None"}:
            return None
        v = float(s)
        return None if v != v else v
    except Exception:
        return None


def _yahoo_symbol(symbol: str) -> str:
    s = (symbol or "").strip().upper()
    if not s:
        return ""
    # internal convention: AAPL.US => AAPL
    if s.endswith(".US"):
        return s[:-3]
    return s


def _get_raw(obj: Any) -> Optional[float]:
    if obj is None:
        return None
    if isinstance(obj, (int, float)):
        return _safe_float(obj)
    if isinstance(obj, dict):
        return _safe_float(obj.get("raw"))
    return _safe_float(obj)


async def _sleep_backoff(attempt: int) -> None:
    base = 0.25 * (2**attempt)
    await asyncio.sleep(min(2.8, base + random.random() * 0.35))


def _base(symbol: str) -> Dict[str, Any]:
    return {
        "symbol": symbol,
        "provider": "yahoo_fundamentals",
        "provider_version": PROVIDER_VERSION,
        "last_updated_utc": _now_utc_iso(),
        "market_cap": None,
        "pe_ttm": None,
        "pb": None,
        "dividend_yield": None,
        "roe": None,
        "roa": None,
        "currency": None,
        "error": None,
        "meta": {},
    }


def _has_any(out: Dict[str, Any]) -> bool:
    for k in ("market_cap", "pe_ttm", "pb", "dividend_yield", "roe", "roa"):
        if _safe_float(out.get(k)) is not None:
            return True
    return False


async def _ensure_crumb(client: httpx.AsyncClient, symbol: str, timeout: float) -> Optional[str]:
    """
    Best-effort cookie+crumb retrieval.
    Never raises (PROD SAFE).
    """
    now = time.time()
    cached = _CRUMB_CACHE.get("crumb")
    ts = float(_CRUMB_CACHE.get("ts") or 0.0)

    if cached and (now - ts) < _CRUMB_TTL_SEC:
        return str(cached)

    async with _CRUMB_LOCK:
        now2 = time.time()
        cached2 = _CRUMB_CACHE.get("crumb")
        ts2 = float(_CRUMB_CACHE.get("ts") or 0.0)
        if cached2 and (now2 - ts2) < _CRUMB_TTL_SEC:
            return str(cached2)

        try:
            # Step 1: load quote page to set cookies
            client.timeout = httpx.Timeout(timeout)
            await client.get(YAHOO_QUOTE_PAGE_URL.format(symbol=symbol))

            # Step 2: fetch crumb
            r = await client.get(YAHOO_GETCRUMB_URL)
            if r.status_code >= 400:
                _CRUMB_CACHE["crumb"] = None
                _CRUMB_CACHE["ts"] = time.time()
                return None

            crumb = (r.text or "").strip()
            if not crumb:
                _CRUMB_CACHE["crumb"] = None
                _CRUMB_CACHE["ts"] = time.time()
                return None

            _CRUMB_CACHE["crumb"] = crumb
            _CRUMB_CACHE["ts"] = time.time()
            return crumb
        except Exception:
            _CRUMB_CACHE["crumb"] = None
            _CRUMB_CACHE["ts"] = time.time()
            return None


async def yahoo_fundamentals(
    symbol: str,
    *,
    timeout: float = 12.0,
    retry_attempts: int = 3,
    client: Optional[httpx.AsyncClient] = None,
) -> Dict[str, Any]:
    sym = _yahoo_symbol(symbol)
    out = _base(sym)
    if not sym:
        out["error"] = "Empty symbol"
        return out

    close_client = False
    if client is None:
        client = httpx.AsyncClient(
            headers={
                "User-Agent": USER_AGENT,
                "Accept": "application/json,text/plain,*/*;q=0.8",
                "Accept-Language": "en-US,en;q=0.8,ar;q=0.6",
                "Referer": "https://finance.yahoo.com/",
                "Connection": "keep-alive",
            },
            follow_redirects=True,
            timeout=httpx.Timeout(timeout),
            limits=httpx.Limits(max_keepalive_connections=10, max_connections=20),
        )
        close_client = True

    async def _try_quote_summary() -> None:
        crumb = await _ensure_crumb(client, sym, timeout)
        url = YAHOO_QUOTE_SUMMARY_URL.format(symbol=sym)
        params = {"modules": "price,summaryDetail,defaultKeyStatistics,financialData"}
        if crumb:
            params["crumb"] = crumb

        r = await client.get(url, params=params)
        out["meta"]["quoteSummary_status"] = r.status_code

        if r.status_code >= 400:
            raise RuntimeError(f"quoteSummary HTTP {r.status_code}")

        # Yahoo can sometimes return HTML; protect JSON parse
        try:
            j = r.json() or {}
        except Exception:
            txt = (r.text or "")[:200].replace("\n", " ")
            raise RuntimeError(f"quoteSummary non-JSON ({txt})")

        qs = j.get("quoteSummary") or {}
        if qs.get("error"):
            raise RuntimeError(f"quoteSummary error: {qs.get('error')}")

        res = (qs.get("result") or [])
        if not res:
            raise RuntimeError("quoteSummary empty result")

        data = res[0] or {}

        price = data.get("price") or {}
        summ = data.get("summaryDetail") or {}
        stats = data.get("defaultKeyStatistics") or {}
        fin = data.get("financialData") or {}

        out["currency"] = (price.get("currency") or out["currency"])

        out["market_cap"] = _get_raw(price.get("marketCap")) or _get_raw(stats.get("marketCap"))
        out["pe_ttm"] = _get_raw(summ.get("trailingPE")) or _get_raw(stats.get("trailingPE"))
        out["pb"] = _get_raw(stats.get("priceToBook"))
        out["dividend_yield"] = _get_raw(summ.get("dividendYield"))  # fraction
        out["roe"] = _get_raw(fin.get("returnOnEquity"))
        out["roa"] = _get_raw(fin.get("returnOnAssets"))

    async def _try_quote_v7() -> None:
        r = await client.get(YAHOO_QUOTE_URL, params={"symbols": sym})
        out["meta"]["quoteV7_status"] = r.status_code

        if r.status_code >= 400:
            raise RuntimeError(f"quote v7 HTTP {r.status_code}")

        try:
            j = r.json() or {}
        except Exception:
            txt = (r.text or "")[:200].replace("\n", " ")
            raise RuntimeError(f"quote v7 non-JSON ({txt})")

        q = (((j.get("quoteResponse") or {}).get("result")) or [])
        if not q:
            raise RuntimeError("quote v7 empty result")

        q0 = q[0] or {}
        out["currency"] = q0.get("currency") or out["currency"]
        out["market_cap"] = _safe_float(q0.get("marketCap")) or out["market_cap"]
        out["pe_ttm"] = _safe_float(q0.get("trailingPE")) or out["pe_ttm"]
        out["pb"] = _safe_float(q0.get("priceToBook")) or out["pb"]
        out["dividend_yield"] = _safe_float(q0.get("trailingAnnualDividendYield")) or out["dividend_yield"]

    async def _try_timeseries() -> None:
        """
        Best-effort: pull latest values from fundamentals-timeseries endpoint.

        NOTE: Yahoo naming is not always consistent across markets; we request a bundle of
        common types and accept whichever are returned.
        """
        # common types seen in the wild
        types = [
            "trailingMarketCap",
            "trailingPeRatio",
            "trailingPbRatio",
            "trailingAnnualDividendYield",
            "returnOnEquity",
            "returnOnAssets",
        ]

        # period window (Yahoo caps history anyway)
        period1 = int(datetime(2016, 12, 31, tzinfo=timezone.utc).timestamp())
        period2 = int(datetime.now(timezone.utc).timestamp())

        url = YAHOO_FUND_TIMESERIES_URL.format(symbol=sym)
        params = {
            "symbol": sym,
            "type": ",".join(types),
            "period1": str(period1),
            "period2": str(period2),
        }

        r = await client.get(url, params=params)
        out["meta"]["timeseries_status"] = r.status_code

        if r.status_code >= 400:
            raise RuntimeError(f"timeseries HTTP {r.status_code}")

        try:
            j = r.json() or {}
        except Exception:
            txt = (r.text or "")[:200].replace("\n", " ")
            raise RuntimeError(f"timeseries non-JSON ({txt})")

        res = ((j.get("timeseries") or {}).get("result") or [])
        if not res:
            raise RuntimeError("timeseries empty result")

        # Usually first record carries metrics
        rec = res[0] or {}

        # Currency might exist in per-metric meta or top meta; keep best-effort only
        # Pull latest metric point:
        def _latest(metric_key: str) -> Optional[float]:
            arr = rec.get(metric_key)
            if not isinstance(arr, list) or not arr:
                return None
            # choose last element with reportedValue.raw
            for item in reversed(arr):
                try:
                    rv = ((item or {}).get("reportedValue") or {}).get("raw")
                    v = _safe_float(rv)
                    if v is not None:
                        return v
                except Exception:
                    continue
            return None

        # Map if available
        out["market_cap"] = _latest("trailingMarketCap") or out["market_cap"]
        out["pe_ttm"] = _latest("trailingPeRatio") or out["pe_ttm"]
        out["pb"] = _latest("trailingPbRatio") or out["pb"]
        out["dividend_yield"] = _latest("trailingAnnualDividendYield") or out["dividend_yield"]
        out["roe"] = _latest("returnOnEquity") or out["roe"]
        out["roa"] = _latest("returnOnAssets") or out["roa"]

    last_err: Optional[str] = None
    try:
        # 1) quoteSummary (with crumb best-effort)
        for attempt in range(max(1, int(retry_attempts))):
            try:
                await _try_quote_summary()
                last_err = None
                break
            except Exception as e:
                last_err = str(e)
                await _sleep_backoff(attempt)

        # 2) v7 quote fallback
        if not _has_any(out):
            for attempt in range(max(1, int(retry_attempts))):
                try:
                    await _try_quote_v7()
                    last_err = None
                    break
                except Exception as e:
                    last_err = str(e)
                    await _sleep_backoff(attempt)

        # 3) timeseries fallback
        if not _has_any(out):
            for attempt in range(max(1, int(retry_attempts))):
                try:
                    await _try_timeseries()
                    last_err = None
                    break
                except Exception as e:
                    last_err = str(e)
                    await _sleep_backoff(attempt)

        # If still nothing usable -> clear error for engine warning
        if not _has_any(out):
            out["error"] = "yahoo fundamentals returned no usable fields"
        elif last_err:
            # Non-fatal: got something, but record last warning
            out["error"] = last_err

        return out

    finally:
        if close_client:
            await client.aclose()


# --- Engine adapter (what DataEngineV2 calls) --------------------------------

async def fetch_fundamentals_patch(symbol: str) -> Dict[str, Any]:
    """
    Returns a patch aligned to UnifiedQuote keys.
    Only returns non-null fields.
    """
    d = await yahoo_fundamentals(symbol)
    patch = {
        "currency": d.get("currency"),
        "market_cap": d.get("market_cap"),
        "pe_ttm": d.get("pe_ttm"),
        "pb": d.get("pb"),
        "dividend_yield": d.get("dividend_yield"),
        "roe": d.get("roe"),
        "roa": d.get("roa"),
    }
    return {k: v for k, v in patch.items() if v is not None}
