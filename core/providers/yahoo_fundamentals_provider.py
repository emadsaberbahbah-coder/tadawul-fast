from __future__ import annotations

"""
Yahoo Fundamentals Provider (NO yfinance) — v1.1.0 (PROD SAFE)

Best-effort fundamentals for symbols (KSA .SR + GLOBAL):
- market_cap, shares_outstanding
- eps_ttm, pe_ttm (computed if missing)
- pb (or computed using bookValue if available)
- dividend_yield, dividend_rate
- roe, roa, beta
- currency

Design:
- Safe import (never crashes app on import)
- Uses Yahoo JSON endpoints only
- Adds lang/region params (helps non-US symbols sometimes)
"""

import asyncio
import random
from datetime import datetime, timezone
from typing import Any, Dict, Optional, Tuple

import httpx

PROVIDER_VERSION = "1.1.0"

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
    # allow legacy GLOBAL suffix
    if s.endswith(".US"):
        return s[:-3]
    return s


def _get_raw(obj: Any) -> Optional[float]:
    """
    Yahoo often returns { raw: ..., fmt: ... }.
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
        "currency": None,
        "current_price": None,
        "market_cap": None,
        "shares_outstanding": None,
        "eps_ttm": None,
        "pe_ttm": None,
        "pb": None,
        "dividend_yield": None,  # fraction (e.g. 0.035)
        "dividend_rate": None,
        "roe": None,
        "roa": None,
        "beta": None,
        "error": None,
    }


def _compute_if_possible(out: Dict[str, Any]) -> None:
    """
    Compute PE and Market Cap if Yahoo didn't provide them.
    """
    px = _safe_float(out.get("current_price"))
    sh = _safe_float(out.get("shares_outstanding"))
    eps = _safe_float(out.get("eps_ttm"))

    if out.get("market_cap") is None and px is not None and sh is not None:
        out["market_cap"] = px * sh

    if out.get("pe_ttm") is None and px is not None and eps not in (None, 0.0):
        try:
            out["pe_ttm"] = px / eps
        except Exception:
            pass


def _has_any_useful(out: Dict[str, Any]) -> bool:
    keys = ("market_cap", "pe_ttm", "pb", "dividend_yield", "roe", "roa", "shares_outstanding", "eps_ttm", "beta")
    return any(_safe_float(out.get(k)) is not None for k in keys)


def _safe_json(resp: httpx.Response) -> Dict[str, Any]:
    try:
        j = resp.json()
        return j if isinstance(j, dict) else {}
    except Exception:
        return {}


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
        params = {
            "modules": "price,summaryDetail,defaultKeyStatistics,financialData",
            "lang": "en-US",
            "region": "US",
        }
        r = await client.get(url, params=params)
        if r.status_code >= 400:
            raise RuntimeError(f"quoteSummary HTTP {r.status_code}")

        j = _safe_json(r)
        res = (((j.get("quoteSummary") or {}).get("result")) or [])
        if not res:
            raise RuntimeError("quoteSummary empty result")

        data = res[0] or {}
        price = data.get("price") or {}
        summ = data.get("summaryDetail") or {}
        stats = data.get("defaultKeyStatistics") or {}
        fin = data.get("financialData") or {}

        out["currency"] = price.get("currency") or out["currency"]
        out["current_price"] = _get_raw(price.get("regularMarketPrice")) or out["current_price"]

        out["market_cap"] = _get_raw(price.get("marketCap")) or _get_raw(stats.get("marketCap")) or out["market_cap"]
        out["shares_outstanding"] = _get_raw(stats.get("sharesOutstanding")) or out["shares_outstanding"]

        # EPS + PE
        out["eps_ttm"] = _get_raw(stats.get("trailingEps")) or out["eps_ttm"]
        out["pe_ttm"] = _get_raw(summ.get("trailingPE")) or _get_raw(stats.get("trailingPE")) or out["pe_ttm"]

        # PB
        out["pb"] = _get_raw(stats.get("priceToBook")) or out["pb"]

        # Dividends
        out["dividend_yield"] = _get_raw(summ.get("dividendYield")) or out["dividend_yield"]  # fraction
        out["dividend_rate"] = _get_raw(summ.get("dividendRate")) or out["dividend_rate"]

        # Returns
        out["roe"] = _get_raw(fin.get("returnOnEquity")) or out["roe"]
        out["roa"] = _get_raw(fin.get("returnOnAssets")) or out["roa"]

        # Risk
        out["beta"] = _get_raw(stats.get("beta")) or out["beta"]

        _compute_if_possible(out)

    async def _try_quote_v7() -> None:
        r = await client.get(
            YAHOO_QUOTE_URL,
            params={"symbols": sym, "lang": "en-US", "region": "US"},
        )
        if r.status_code >= 400:
            raise RuntimeError(f"quote v7 HTTP {r.status_code}")

        j = _safe_json(r)
        q = (((j.get("quoteResponse") or {}).get("result")) or [])
        if not q:
            raise RuntimeError("quote v7 empty result")

        q0 = q[0] or {}
        out["currency"] = q0.get("currency") or out["currency"]
        out["current_price"] = _safe_float(q0.get("regularMarketPrice")) or out["current_price"]

        out["market_cap"] = _safe_float(q0.get("marketCap")) or out["market_cap"]
        out["shares_outstanding"] = _safe_float(q0.get("sharesOutstanding")) or out["shares_outstanding"]

        out["eps_ttm"] = _safe_float(q0.get("epsTrailingTwelveMonths")) or out["eps_ttm"]
        out["pe_ttm"] = _safe_float(q0.get("trailingPE")) or out["pe_ttm"]
        out["pb"] = _safe_float(q0.get("priceToBook")) or out["pb"]

        out["dividend_yield"] = _safe_float(q0.get("trailingAnnualDividendYield")) or out["dividend_yield"]
        out["dividend_rate"] = _safe_float(q0.get("trailingAnnualDividendRate")) or out["dividend_rate"]

        _compute_if_possible(out)

    last_err: Optional[str] = None
    try:
        for attempt in range(max(1, int(retry_attempts))):
            try:
                await _try_quote_summary()
                if _has_any_useful(out):
                    last_err = None
                    break
                last_err = "quoteSummary returned no usable fields"
            except Exception as e:
                last_err = str(e)
            await _sleep_backoff(attempt)

        if not _has_any_useful(out):
            for attempt in range(max(1, int(retry_attempts))):
                try:
                    await _try_quote_v7()
                    if _has_any_useful(out):
                        last_err = None
                        break
                    last_err = "quote v7 returned no usable fields"
                except Exception as e:
                    last_err = str(e)
                await _sleep_backoff(attempt)

        if last_err and not _has_any_useful(out):
            out["error"] = last_err

        return out

    finally:
        if close_client and client is not None:
            await client.aclose()


# --- Engine adapter (what DataEngineV2 calls) --------------------------------

async def fetch_fundamentals_patch(symbol: str) -> Dict[str, Any]:
    """
    Returns a patch aligned to UnifiedQuote keys.
    """
    d = await yahoo_fundamentals(symbol)
    patch = {
        "currency": d.get("currency"),
        "current_price": d.get("current_price"),
        "market_cap": d.get("market_cap"),
        "shares_outstanding": d.get("shares_outstanding"),
        "eps_ttm": d.get("eps_ttm"),
        "pe_ttm": d.get("pe_ttm"),
        "pb": d.get("pb"),
        "dividend_yield": d.get("dividend_yield"),
        "dividend_rate": d.get("dividend_rate"),
        "roe": d.get("roe"),
        "roa": d.get("roa"),
        "beta": d.get("beta"),
    }
    return {k: v for k, v in patch.items() if v is not None}
