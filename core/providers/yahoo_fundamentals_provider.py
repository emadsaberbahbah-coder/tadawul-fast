# core/providers/yahoo_fundamentals_provider.py
"""
core/providers/yahoo_fundamentals_provider.py
------------------------------------------------------------
Yahoo Fundamentals Provider (NO yfinance) — v1.3.0
PROD SAFE + ENGINE-ALIGNED (DICT RETURNS) + CLIENT REUSE + RETRY + TTL CACHE

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
- Retry/backoff on 429 + transient 5xx
- Reuses one AsyncClient (keep-alive)
- TTL cache to reduce Yahoo rate limits (configurable)
- Engine adapter returns PATCH dict (UnifiedQuote-aligned keys)

Exports (engine-friendly)
- fetch_fundamentals_patch(symbol, debug=False) -> Dict[str, Any]
- fetch_quote_patch(symbol, debug=False) -> Dict[str, Any]                  (alias to fundamentals patch)
- fetch_enriched_quote_patch(symbol, debug=False) -> Dict[str, Any]         (alias)
- fetch_quote_and_enrichment_patch(symbol, debug=False) -> Dict[str, Any]   (alias)
- aclose_yahoo_fundamentals_client() -> None

Env vars (supported)
- YAHOO_UA (optional)
- YAHOO_ACCEPT_LANGUAGE (optional)
- YAHOO_TIMEOUT_S (default 12.0)
- YAHOO_FUND_RETRY_MAX (default 2)                  # additional retries
- YAHOO_FUND_RETRY_BACKOFF_MS (default 250)
- YAHOO_FUND_TTL_SEC (default 3600, min 30)
- YAHOO_ALT_HOSTS (optional, comma-separated; same as yahoo_chart_provider)
"""

from __future__ import annotations

import asyncio
import math
import os
import random
import time
from datetime import datetime, timezone
from typing import Any, Dict, Optional, Tuple, List

import httpx

PROVIDER_NAME = "yahoo_fundamentals"
PROVIDER_VERSION = "1.3.0"

# ---------------------------
# URLs (host overridable)
# ---------------------------
_DEFAULT_HOST = "https://query1.finance.yahoo.com"
_QUOTE_SUMMARY_PATH = "/v10/finance/quoteSummary/{symbol}"
_QUOTE_V7_PATH = "/v7/finance/quote"

_ALT_HOSTS = [h.strip() for h in (os.getenv("YAHOO_ALT_HOSTS", "") or "").split(",") if h.strip()]
_HOSTS: List[str] = [_DEFAULT_HOST]
for host in _ALT_HOSTS:
    if "://" in host:
        _HOSTS.append(host.rstrip("/"))
    else:
        _HOSTS.append(f"https://{host}".rstrip("/"))

USER_AGENT = os.getenv(
    "YAHOO_UA",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/124.0.0.0 Safari/537.36",
)
_ACCEPT_LANGUAGE = os.getenv("YAHOO_ACCEPT_LANGUAGE", "en-US,en;q=0.8,ar;q=0.6")


def _env_float(name: str, default: float) -> float:
    v = os.getenv(name)
    if v is None:
        return default
    try:
        f = float(str(v).strip())
        return f if f > 0 else default
    except Exception:
        return default


_TIMEOUT_S = _env_float("YAHOO_TIMEOUT_S", 12.0)
_TIMEOUT = httpx.Timeout(timeout=_TIMEOUT_S, connect=min(6.0, _TIMEOUT_S))

# Retries for transient Yahoo responses
_RETRY_MAX = int(os.getenv("YAHOO_FUND_RETRY_MAX", "2") or "2")  # additional retries (0 => no retry)
_RETRY_BACKOFF_MS = _env_float("YAHOO_FUND_RETRY_BACKOFF_MS", 250.0)
_RETRY_STATUSES = {429, 500, 502, 503, 504}

# TTL Cache
_TTL_SEC = _env_float("YAHOO_FUND_TTL_SEC", 3600.0)
_TTL_SEC = max(30.0, _TTL_SEC)


# ---------------------------
# Client reuse (keep-alive)
# ---------------------------
_CLIENT: Optional[httpx.AsyncClient] = None
_CLIENT_LOCK = asyncio.Lock()

# Minimal TTL cache (fallback if cachetools isn't installed)
try:
    from cachetools import TTLCache  # type: ignore

    _HAS_CACHETOOLS = True
except Exception:  # pragma: no cover
    _HAS_CACHETOOLS = False

    class TTLCache(dict):  # type: ignore
        def __init__(self, maxsize: int = 1024, ttl: float = 60.0) -> None:
            super().__init__()
            self._maxsize = max(1, int(maxsize))
            self._ttl = max(1.0, float(ttl))
            self._exp: Dict[str, float] = {}

        def get(self, key: str, default: Any = None) -> Any:  # type: ignore
            now = time.time()
            exp = self._exp.get(key)
            if exp is not None and exp < now:
                try:
                    super().pop(key, None)
                except Exception:
                    pass
                self._exp.pop(key, None)
                return default
            return super().get(key, default)

        def __setitem__(self, key: str, value: Any) -> None:  # type: ignore
            if len(self) >= self._maxsize:
                try:
                    oldest_key = next(iter(self.keys()))
                    super().pop(oldest_key, None)
                    self._exp.pop(oldest_key, None)
                except Exception:
                    pass
            super().__setitem__(key, value)
            self._exp[key] = time.time() + self._ttl


_CACHE: TTLCache = TTLCache(maxsize=8000, ttl=_TTL_SEC)


def _headers_for(symbol: str) -> Dict[str, str]:
    sym = (symbol or "").strip().upper()
    ref = f"https://finance.yahoo.com/quote/{sym}"
    return {
        "User-Agent": USER_AGENT,
        "Accept": "application/json,*/*;q=0.8",
        "Accept-Language": _ACCEPT_LANGUAGE,
        "Referer": ref,
        "Origin": "https://finance.yahoo.com",
        "Connection": "keep-alive",
    }


async def _get_client(symbol: str) -> httpx.AsyncClient:
    global _CLIENT
    if _CLIENT is not None:
        return _CLIENT
    async with _CLIENT_LOCK:
        if _CLIENT is None:
            _CLIENT = httpx.AsyncClient(
                headers=_headers_for(symbol),
                follow_redirects=True,
                timeout=_TIMEOUT,
                limits=httpx.Limits(max_keepalive_connections=20, max_connections=40),
            )
    return _CLIENT


async def aclose_yahoo_fundamentals_client() -> None:
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
def _now_utc_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _safe_float(x: Any) -> Optional[float]:
    try:
        if x is None or isinstance(x, bool):
            return None
        if isinstance(x, (int, float)):
            v = float(x)
            if math.isnan(v) or math.isinf(v):
                return None
            return v
        s = str(x).strip().replace(",", "")
        if not s or s in {"-", "—", "N/A", "NA", "null", "None"}:
            return None
        v = float(s)
        if math.isnan(v) or math.isinf(v):
            return None
        return v
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


def _safe_json(resp: httpx.Response) -> Dict[str, Any]:
    try:
        j = resp.json()
        return j if isinstance(j, dict) else {}
    except Exception:
        return {}


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


def _base(symbol: str) -> Dict[str, Any]:
    return {
        "symbol": symbol,
        "provider": PROVIDER_NAME,
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


async def _sleep_backoff(attempt: int) -> None:
    base = (_RETRY_BACKOFF_MS / 1000.0) * (2**attempt)
    await asyncio.sleep(min(2.5, base + random.random() * 0.35))


async def _http_get_json(
    symbol: str,
    url: str,
    params: Dict[str, Any],
) -> Tuple[Optional[Dict[str, Any]], Optional[str]]:
    client = await _get_client(symbol)
    headers = _headers_for(symbol)

    attempts = 1 + max(0, _RETRY_MAX)
    last_err: Optional[str] = None

    for i in range(attempts):
        try:
            r = await client.get(url, params=params, headers=headers)
            if r.status_code == 200:
                j = _safe_json(r)
                if j:
                    return j, None
                last_err = "invalid JSON"
            else:
                if r.status_code in _RETRY_STATUSES and i < attempts - 1:
                    last_err = f"HTTP {r.status_code}"
                else:
                    return None, f"HTTP {r.status_code}"
        except Exception as e:
            last_err = f"{e.__class__.__name__}: {e}"

        if i < attempts - 1:
            await _sleep_backoff(i)

    return None, last_err or "request failed"


# ---------------------------
# Core fetchers
# ---------------------------
async def yahoo_fundamentals(symbol: str, *, debug: bool = False) -> Dict[str, Any]:
    sym = _yahoo_symbol(symbol)
    out = _base(sym)
    if not sym:
        out["error"] = "Empty symbol"
        return out

    # Cache
    ck = f"yahoo_fund::{sym}"
    hit = _CACHE.get(ck)
    if isinstance(hit, dict) and hit:
        return dict(hit)

    # Region heuristic: KSA => SA, else US
    region = "SA" if sym.endswith(".SR") else "US"
    lang = "en-US"

    last_err: Optional[str] = None

    # 1) quoteSummary (richer)
    for host in _HOSTS:
        url = f"{host}{_QUOTE_SUMMARY_PATH}".format(symbol=sym)
        params = {
            "modules": "price,summaryDetail,defaultKeyStatistics,financialData",
            "lang": lang,
            "region": region,
        }
        js, err = await _http_get_json(sym, url, params)
        if js is None:
            last_err = err
            continue

        try:
            res = (((js.get("quoteSummary") or {}).get("result")) or [])
            if not res:
                last_err = "quoteSummary empty result"
                continue

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

            # PB (or compute using bookValue if provided)
            out["pb"] = _get_raw(stats.get("priceToBook")) or out["pb"]
            if out["pb"] is None:
                bv = _get_raw(fin.get("bookValue")) or _get_raw(stats.get("bookValue"))
                px = _safe_float(out.get("current_price"))
                if bv not in (None, 0.0) and px is not None:
                    try:
                        out["pb"] = px / bv
                    except Exception:
                        pass

            # Dividends
            out["dividend_yield"] = _get_raw(summ.get("dividendYield")) or out["dividend_yield"]  # fraction
            out["dividend_rate"] = _get_raw(summ.get("dividendRate")) or out["dividend_rate"]

            # Returns
            out["roe"] = _get_raw(fin.get("returnOnEquity")) or out["roe"]
            out["roa"] = _get_raw(fin.get("returnOnAssets")) or out["roa"]

            # Risk
            out["beta"] = _get_raw(stats.get("beta")) or out["beta"]

            if debug:
                out["_debug"] = {
                    "host": host,
                    "region": region,
                    "source": "quoteSummary",
                }

            _compute_if_possible(out)
            if _has_any_useful(out):
                out["error"] = None
                out["last_updated_utc"] = _now_utc_iso()
                _CACHE[ck] = dict(out)
                return out

            last_err = "quoteSummary returned no usable fields"
        except Exception as e:
            last_err = f"quoteSummary parse error: {e.__class__.__name__}"

    # 2) quote v7 fallback (lighter)
    for host in _HOSTS:
        url = f"{host}{_QUOTE_V7_PATH}"
        params = {"symbols": sym, "lang": lang, "region": region}
        js, err = await _http_get_json(sym, url, params)
        if js is None:
            last_err = err
            continue

        try:
            q = (((js.get("quoteResponse") or {}).get("result")) or [])
            if not q:
                last_err = "quote v7 empty result"
                continue

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

            if debug:
                out["_debug"] = {
                    "host": host,
                    "region": region,
                    "source": "quote_v7",
                }

            _compute_if_possible(out)
            if _has_any_useful(out):
                out["error"] = None
                out["last_updated_utc"] = _now_utc_iso()
                _CACHE[ck] = dict(out)
                return out

            last_err = "quote v7 returned no usable fields"
        except Exception as e:
            last_err = f"quote v7 parse error: {e.__class__.__name__}"

    # Failed
    out["error"] = last_err or "yahoo fundamentals failed"
    out["last_updated_utc"] = _now_utc_iso()
    _CACHE[ck] = dict(out)
    return out


# --- Engine adapter (what DataEngineV2 calls) --------------------------------
def _clean_patch(patch: Dict[str, Any]) -> Dict[str, Any]:
    out: Dict[str, Any] = {}
    for k, v in (patch or {}).items():
        if v is None:
            continue
        if isinstance(v, str) and not v.strip():
            continue
        out[k] = v
    return out


async def fetch_fundamentals_patch(symbol: str, debug: bool = False) -> Dict[str, Any]:
    """
    Returns a patch aligned to UnifiedQuote keys.
    """
    d = await yahoo_fundamentals(symbol, debug=debug)
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
    # surface warning only if fully failed (engine can still merge others)
    if d.get("error"):
        patch["error"] = f"warning: yahoo_fundamentals: {d.get('error')}"
    return _clean_patch(patch)


# Engine discovery callables (some engines call these names)
async def fetch_quote_patch(symbol: str, debug: bool = False, *args: Any, **kwargs: Any) -> Dict[str, Any]:
    return await fetch_fundamentals_patch(symbol, debug=debug)


async def fetch_enriched_quote_patch(symbol: str, debug: bool = False, *args: Any, **kwargs: Any) -> Dict[str, Any]:
    return await fetch_fundamentals_patch(symbol, debug=debug)


async def fetch_quote_and_enrichment_patch(symbol: str, debug: bool = False, *args: Any, **kwargs: Any) -> Dict[str, Any]:
    return await fetch_fundamentals_patch(symbol, debug=debug)


__all__ = [
    "PROVIDER_NAME",
    "PROVIDER_VERSION",
    "yahoo_fundamentals",
    "fetch_fundamentals_patch",
    "fetch_quote_patch",
    "fetch_enriched_quote_patch",
    "fetch_quote_and_enrichment_patch",
    "aclose_yahoo_fundamentals_client",
]
