from __future__ import annotations

"""
Yahoo Fundamentals Provider (NO yfinance) — v1.2.0 (PROD SAFE)

Purpose
- Best-effort fundamentals for symbols (works for KSA .SR + GLOBAL)
- Fills (when available):
  market_cap, pe_ttm, pb, dividend_yield, roe, roa, currency

Key upgrades vs v1.0.0
- Adds Yahoo fundamentals-timeseries fallback (often better for missing fields)
- Returns (patch, err) from fetch_fundamentals_patch to allow engine warnings
- Stronger key fallbacks + safer parsing
- Never crashes app on import / never raises outward
"""

import asyncio
import random
import time
from datetime import datetime, timezone
from typing import Any, Dict, Optional, Tuple

import httpx

PROVIDER_VERSION = "1.2.0"

# Primary endpoints
YAHOO_QUOTE_SUMMARY_URL = "https://query1.finance.yahoo.com/v10/finance/quoteSummary/{symbol}"
YAHOO_QUOTE_URL = "https://query1.finance.yahoo.com/v7/finance/quote"

# Timeseries fallback (often helps when quoteSummary is sparse)
# NOTE: Some regions work better on query2, so we try both.
YAHOO_TIMESERIES_URLS = [
    "https://query2.finance.yahoo.com/ws/fundamentals-timeseries/v1/finance/timeseries/{symbol}",
    "https://query1.finance.yahoo.com/ws/fundamentals-timeseries/v1/finance/timeseries/{symbol}",
]

USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/124.0.0.0 Safari/537.36"
)

_NULL_STRINGS = {"-", "—", "N/A", "NA", "null", "None", ""}


def _now_utc_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _safe_float(x: Any) -> Optional[float]:
    try:
        if x is None or isinstance(x, bool):
            return None
        if isinstance(x, (int, float)):
            v = float(x)
            return None if v != v else v  # NaN
        s = str(x).strip().replace(",", "")
        if s in _NULL_STRINGS:
            return None
        v = float(s)
        return None if v != v else v
    except Exception:
        return None


def _yahoo_symbol(symbol: str) -> str:
    s = (symbol or "").strip().upper()
    if not s:
        return ""
    # Your system sometimes uses .US, Yahoo doesn't need it
    if s.endswith(".US"):
        return s[:-3]
    return s


def _get_raw(obj: Any) -> Optional[float]:
    if obj is None:
        return None
    if isinstance(obj, (int, float)):
        return _safe_float(obj)
    if isinstance(obj, dict):
        # Yahoo style: {"raw": ..., "fmt": ...}
        if "raw" in obj:
            return _safe_float(obj.get("raw"))
        # sometimes: {"value": {"raw":...}}
        if "value" in obj and isinstance(obj.get("value"), dict):
            return _safe_float(obj["value"].get("raw"))
        return _safe_float(obj.get("fmt")) or _safe_float(obj.get("value"))
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
        "dividend_yield": None,  # NOTE: usually fraction (e.g., 0.0037 = 0.37%)
        "roe": None,
        "roa": None,
        "currency": None,
        "error": None,
    }


def _has_any(out: Dict[str, Any]) -> bool:
    return any(
        out.get(k) is not None
        for k in ("market_cap", "pe_ttm", "pb", "dividend_yield", "roe", "roa", "currency")
    )


def _ts_latest_value(result0: Dict[str, Any], key: str) -> Optional[float]:
    """
    Timeseries shape usually:
      result0[key] = [ { "asOfDate": "...", "reportedValue": {"raw": ...} }, ... ]
    We take the last element.
    """
    arr = result0.get(key)
    if not isinstance(arr, list) or not arr:
        return None
    last = arr[-1]
    if not isinstance(last, dict):
        return _safe_float(last)
    if "reportedValue" in last:
        return _get_raw(last.get("reportedValue"))
    # some variants might use "value"
    if "value" in last:
        return _get_raw(last.get("value"))
    return _get_raw(last)


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
                "Accept": "application/json,*/*;q=0.8",
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
        url = YAHOO_QUOTE_SUMMARY_URL.format(symbol=sym)
        params = {"modules": "price,summaryDetail,defaultKeyStatistics,financialData"}
        r = await client.get(url, params=params)
        if r.status_code >= 400:
            raise RuntimeError(f"quoteSummary HTTP {r.status_code}")
        j = r.json() or {}
        res = (((j.get("quoteSummary") or {}).get("result")) or [])
        if not res:
            raise RuntimeError("quoteSummary empty result")
        data = res[0] or {}

        price = data.get("price") or {}
        summ = data.get("summaryDetail") or {}
        stats = data.get("defaultKeyStatistics") or {}
        fin = data.get("financialData") or {}

        out["currency"] = price.get("currency") or out["currency"]

        out["market_cap"] = (
            _get_raw(price.get("marketCap"))
            or _get_raw(stats.get("marketCap"))
            or _get_raw(stats.get("enterpriseValue"))
        )
        out["pe_ttm"] = _get_raw(summ.get("trailingPE")) or _get_raw(stats.get("trailingPE"))
        out["pb"] = _get_raw(stats.get("priceToBook")) or out["pb"]

        # dividendYield is often a fraction
        out["dividend_yield"] = _get_raw(summ.get("dividendYield")) or out["dividend_yield"]

        out["roe"] = _get_raw(fin.get("returnOnEquity")) or out["roe"]
        out["roa"] = _get_raw(fin.get("returnOnAssets")) or out["roa"]

    async def _try_quote_v7() -> None:
        r = await client.get(YAHOO_QUOTE_URL, params={"symbols": sym})
        if r.status_code >= 400:
            raise RuntimeError(f"quote v7 HTTP {r.status_code}")
        j = r.json() or {}
        q = (((j.get("quoteResponse") or {}).get("result")) or [])
        if not q:
            raise RuntimeError("quote v7 empty result")
        q0 = q[0] or {}

        out["currency"] = q0.get("currency") or out["currency"]
        out["market_cap"] = _safe_float(q0.get("marketCap")) or out["market_cap"]
        out["pe_ttm"] = _safe_float(q0.get("trailingPE")) or out["pe_ttm"]
        out["pb"] = _safe_float(q0.get("priceToBook")) or out["pb"]

        # Some symbols expose dividend yield here:
        out["dividend_yield"] = (
            _safe_float(q0.get("trailingAnnualDividendYield"))
            or _safe_float(q0.get("dividendYield"))
            or out["dividend_yield"]
        )

    async def _try_timeseries() -> None:
        # We request multiple “type” candidates, then pick whichever exists.
        # Yahoo varies naming across endpoints/regions, so we include several.
        types = [
            "trailingMarketCap",
            "marketCap",
            "trailingPE",
            "trailingPeRatio",
            "trailingPB",
            "trailingPbRatio",
            "trailingAnnualDividendYield",
            "dividendYield",
            "returnOnEquity",
            "returnOnAssets",
        ]
        period2 = int(time.time())
        period1 = 0  # oldest

        last_err = None
        for base_url in YAHOO_TIMESERIES_URLS:
            try:
                url = base_url.format(symbol=sym)
                params = {
                    "symbol": sym,
                    "type": ",".join(types),
                    "period1": str(period1),
                    "period2": str(period2),
                    "merge": "false",
                }
                r = await client.get(url, params=params)
                if r.status_code >= 400:
                    raise RuntimeError(f"timeseries HTTP {r.status_code}")
                j = r.json() or {}
                ts = j.get("timeseries") or {}
                if ts.get("error"):
                    raise RuntimeError(f"timeseries error: {ts.get('error')}")
                res = ts.get("result") or []
                if not res:
                    raise RuntimeError("timeseries empty result")
                r0 = res[0] or {}

                # Market cap
                out["market_cap"] = out["market_cap"] or (
                    _ts_latest_value(r0, "trailingMarketCap") or _ts_latest_value(r0, "marketCap")
                )

                # PE
                out["pe_ttm"] = out["pe_ttm"] or (
                    _ts_latest_value(r0, "trailingPE") or _ts_latest_value(r0, "trailingPeRatio")
                )

                # PB
                out["pb"] = out["pb"] or (
                    _ts_latest_value(r0, "trailingPB") or _ts_latest_value(r0, "trailingPbRatio")
                )

                # Dividend yield (fraction)
                out["dividend_yield"] = out["dividend_yield"] or (
                    _ts_latest_value(r0, "trailingAnnualDividendYield") or _ts_latest_value(r0, "dividendYield")
                )

                # ROE/ROA (fractions)
                out["roe"] = out["roe"] or _ts_latest_value(r0, "returnOnEquity")
                out["roa"] = out["roa"] or _ts_latest_value(r0, "returnOnAssets")

                # Meta currency sometimes exists
                meta = r0.get("meta") or {}
                out["currency"] = out["currency"] or meta.get("currency")

                return  # success
            except Exception as e:
                last_err = str(e)

        if last_err:
            raise RuntimeError(last_err)

    last_err: Optional[str] = None
    try:
        # 1) quoteSummary
        for attempt in range(max(1, int(retry_attempts))):
            try:
                await _try_quote_summary()
                last_err = None
                break
            except Exception as e:
                last_err = str(e)
                await _sleep_backoff(attempt)

        # 2) quote v7 if still sparse
        if not _has_any(out):
            for attempt in range(max(1, int(retry_attempts))):
                try:
                    await _try_quote_v7()
                    last_err = None
                    break
                except Exception as e:
                    last_err = str(e)
                    await _sleep_backoff(attempt)

        # 3) timeseries fallback if still missing key fields
        # Run it if any of the core metrics are missing
        needs_ts = (
            out["market_cap"] is None
            or out["pe_ttm"] is None
            or out["pb"] is None
            or out["dividend_yield"] is None
            or out["roe"] is None
            or out["roa"] is None
        )
        if needs_ts:
            for attempt in range(max(1, int(retry_attempts))):
                try:
                    await _try_timeseries()
                    last_err = None
                    break
                except Exception as e:
                    last_err = str(e)
                    await _sleep_backoff(attempt)

        # Error policy:
        if not _has_any(out):
            out["error"] = last_err or "yahoo fundamentals returned no usable fields"
        else:
            out["error"] = None

        return out

    finally:
        if close_client:
            await client.aclose()


# --- Engine adapter (what DataEngineV2 calls) --------------------------------

async def fetch_fundamentals_patch(symbol: str) -> Tuple[Dict[str, Any], Optional[str]]:
    """
    Returns (patch, err) aligned to UnifiedQuote keys.
    Engine will merge only non-None fields; err is used as warning when empty.
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
    patch = {k: v for k, v in patch.items() if v is not None}

    err = (d.get("error") or None)
    if not patch and not err:
        err = "yahoo fundamentals returned no usable fields"
    return patch, err
