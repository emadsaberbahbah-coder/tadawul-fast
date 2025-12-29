# core/yahoo_fundamentals_provider.py
from __future__ import annotations

"""
Yahoo Fundamentals Provider (NO yfinance) — v1.0.0 (PROD SAFE)

Goal:
- Fill missing fundamentals for KSA (.SR) (and works for GLOBAL too)
- market_cap, pe_ttm, pb, dividend_yield, roe, roa
- Best-effort, retry on 429/5xx
"""

import asyncio
import random
from datetime import datetime, timezone
from typing import Any, Dict, Optional

import httpx

PROVIDER_VERSION = "1.0.0"

YAHOO_QUOTE_SUMMARY_URL = "https://query1.finance.yahoo.com/v10/finance/quoteSummary/{symbol}"
YAHOO_QUOTE_URL = "https://query1.finance.yahoo.com/v7/finance/quote"

USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/124.0.0.0 Safari/537.36"
)


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
    if s.endswith(".US"):
        return s[:-3]
    return s


def _get_raw(obj: Any) -> Optional[float]:
    """
    Yahoo often returns: {"raw": 123, "fmt": "123"} or a plain number.
    """
    if obj is None:
        return None
    if isinstance(obj, (int, float)):
        return _safe_float(obj)
    if isinstance(obj, dict):
        return _safe_float(obj.get("raw"))
    return _safe_float(obj)


async def _sleep_backoff(attempt: int) -> None:
    base = 0.25 * (2**attempt)
    await asyncio.sleep(min(2.5, base + random.random() * 0.35))


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
    }


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

        out["currency"] = (price.get("currency") or out["currency"])

        out["market_cap"] = _get_raw(price.get("marketCap")) or _get_raw(stats.get("marketCap"))
        out["pe_ttm"] = _get_raw(summ.get("trailingPE")) or _get_raw(stats.get("trailingPE"))
        out["pb"] = _get_raw(stats.get("priceToBook"))
        out["dividend_yield"] = _get_raw(summ.get("dividendYield"))  # usually fraction
        out["roe"] = _get_raw(fin.get("returnOnEquity"))
        out["roa"] = _get_raw(fin.get("returnOnAssets"))

    async def _try_quote_v7() -> None:
        # fallback if quoteSummary is blocked
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
        out["dividend_yield"] = _safe_float(q0.get("trailingAnnualDividendYield")) or out["dividend_yield"]

    last_err: Optional[str] = None
    try:
        for attempt in range(max(1, int(retry_attempts))):
            try:
                await _try_quote_summary()
                last_err = None
                break
            except Exception as e:
                last_err = str(e)
                # retry on rate limits / transient
                await _sleep_backoff(attempt)

        # fallback if still empty
        if (
            out["market_cap"] is None
            and out["pe_ttm"] is None
            and out["pb"] is None
            and out["dividend_yield"] is None
        ):
            for attempt in range(max(1, int(retry_attempts))):
                try:
                    await _try_quote_v7()
                    last_err = None
                    break
                except Exception as e:
                    last_err = str(e)
                    await _sleep_backoff(attempt)

        if last_err:
            out["error"] = last_err

        return out

    finally:
        if close_client:
            await client.aclose()
