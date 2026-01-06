# core/providers/yahoo_fundamentals_provider.py
"""
core/providers/yahoo_fundamentals_provider.py
============================================================
Yahoo Fundamentals Provider (NO yfinance) — v1.5.0
PROD SAFE + ENGINE-ALIGNED (PATCH DICT) + CUSTOM HEADERS + CLIENT REUSE
+ RETRY + TTL CACHE + COOKIE WARMUP + TARGET-BASED FORECAST ENRICHMENT

What’s new in v1.5.0 (your requested plan items)
- ✅ Customized headers (shared + provider-specific + endpoint-specific)
    • YAHOO_HEADERS_JSON (base for all Yahoo providers)
    • YAHOO_HEADERS_FUND_JSON (common for this provider)
    • YAHOO_HEADERS_FUND_SUMMARY_JSON (quoteSummary endpoint)
    • YAHOO_HEADERS_FUND_QUOTE_JSON   (v7 quote endpoint)
- ✅ Forecast enrichment from Yahoo analyst targets (if available)
    • fair_value            (maps to targetMeanPrice)
    • upside_percent        ((fair_value/current_price - 1)*100)
    • expected_price_12m    (same as fair_value)
    • expected_return_12m   (same as upside_percent)
    • confidence_score      (0..100, based on analyst count + target spread)
    • forecast_method       ("yahoo_analyst_targets_v1")
- ✅ Keeps sheet-ready normalization (percent fields -> % not fraction)
- ✅ Import-safe, no network at import-time, never throws to callers
- ✅ Still supports KSA (.SR) + GLOBAL, with feature flags

Best-effort fundamentals for symbols (KSA .SR + GLOBAL):
- market_cap, shares_outstanding
- eps_ttm, pe_ttm (computed if missing)
- pb (or computed using bookValue if available)
- dividend_yield (%), dividend_rate
- payout_ratio (%)
- roe (%), roa (%), beta
- forward_eps, forward_pe
- ps, ev_ebitda
- currency
- + analyst target forecast fields (best-effort)

Exports (engine-friendly)
- fetch_fundamentals_patch(symbol, debug=False) -> Dict[str, Any]
- fetch_quote_patch(symbol, debug=False)                  (alias)
- fetch_enriched_quote_patch(symbol, debug=False)         (alias)
- fetch_quote_and_enrichment_patch(symbol, debug=False)   (alias)
- aclose_yahoo_fundamentals_client() -> None

Env vars (supported)
Feature flags
- ENABLE_YAHOO_FUNDAMENTALS=true/false (default true)
- ENABLE_YAHOO_FUNDAMENTALS_KSA=true/false (default true)
- YAHOO_FUND_ENABLE_TARGETS=true/false (default true)

HTTP / headers
- YAHOO_UA (optional)
- YAHOO_ACCEPT_LANGUAGE (optional)
- YAHOO_TIMEOUT_S (default 12.0)
- YAHOO_FUND_RETRY_MAX (default 2)                  # additional retries
- YAHOO_FUND_RETRY_BACKOFF_MS (default 250)
- YAHOO_FUND_TTL_SEC (default 3600, min 30)

- YAHOO_HEADERS_JSON (optional JSON dict; base headers across Yahoo providers)
- YAHOO_HEADERS_FUND_JSON (optional JSON dict; added for this provider)
- YAHOO_HEADERS_FUND_SUMMARY_JSON (optional JSON dict; quoteSummary override)
- YAHOO_HEADERS_FUND_QUOTE_JSON   (optional JSON dict; v7 quote override)

Hosts / region / lang
- YAHOO_ALT_HOSTS (optional, comma-separated; appended after defaults)
  e.g. "https://query1.finance.yahoo.com,https://query2.finance.yahoo.com"
- YAHOO_REGION_KSA (default "SA")
- YAHOO_REGION_GLOBAL (default "US")
- YAHOO_LANG (default "en-US")
"""

from __future__ import annotations

import asyncio
import json
import math
import os
import random
import time
from datetime import datetime, timezone
from typing import Any, Dict, Optional, Tuple, List, Set

import httpx

PROVIDER_NAME = "yahoo_fundamentals"
PROVIDER_VERSION = "1.5.0"

# ---------------------------
# Safe env parsing
# ---------------------------
_TRUTHY = {"1", "true", "yes", "y", "on", "t"}
_FALSY = {"0", "false", "no", "n", "off", "f"}


def _env_bool(name: str, default: bool) -> bool:
    raw = (os.getenv(name) or "").strip().lower()
    if not raw:
        return default
    if raw in _TRUTHY:
        return True
    if raw in _FALSY:
        return False
    return default


def _env_float(name: str, default: float) -> float:
    v = os.getenv(name)
    if v is None:
        return default
    try:
        f = float(str(v).strip())
        return f if f > 0 else default
    except Exception:
        return default


def _env_int(name: str, default: int) -> int:
    v = os.getenv(name)
    if v is None:
        return default
    try:
        return int(str(v).strip())
    except Exception:
        return default


def _env_str(name: str, default: str = "") -> str:
    v = os.getenv(name)
    if v is None:
        return default
    s = str(v).strip()
    return s if s else default


# ---------------------------
# Feature flags
# ---------------------------
ENABLE_YAHOO_FUNDAMENTALS = _env_bool("ENABLE_YAHOO_FUNDAMENTALS", True)
ENABLE_YAHOO_FUNDAMENTALS_KSA = _env_bool("ENABLE_YAHOO_FUNDAMENTALS_KSA", True)
ENABLE_TARGETS = _env_bool("YAHOO_FUND_ENABLE_TARGETS", True)

# ---------------------------
# URLs (host overridable)
# ---------------------------
_DEFAULT_HOSTS = [
    "https://query1.finance.yahoo.com",
    "https://query2.finance.yahoo.com",
]
_QUOTE_SUMMARY_PATH = "/v10/finance/quoteSummary/{symbol}"
_QUOTE_V7_PATH = "/v7/finance/quote"
_WARMUP_URL = "https://finance.yahoo.com/quote/{symbol}"

_ALT_HOSTS = [h.strip() for h in (_env_str("YAHOO_ALT_HOSTS", "")).split(",") if h.strip()]
_HOSTS: List[str] = []
for h in _DEFAULT_HOSTS:
    if h:
        hh = h.rstrip("/")
        if hh not in _HOSTS:
            _HOSTS.append(hh)
for host in _ALT_HOSTS:
    if "://" in host:
        h2 = host.rstrip("/")
    else:
        h2 = f"https://{host}".rstrip("/")
    if h2 and h2 not in _HOSTS:
        _HOSTS.append(h2)

USER_AGENT = _env_str(
    "YAHOO_UA",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/124.0.0.0 Safari/537.36",
)
_ACCEPT_LANGUAGE = _env_str("YAHOO_ACCEPT_LANGUAGE", "en-US,en;q=0.8,ar;q=0.6")

_TIMEOUT_S = _env_float("YAHOO_TIMEOUT_S", 12.0)
_TIMEOUT = httpx.Timeout(timeout=_TIMEOUT_S, connect=min(6.0, _TIMEOUT_S))

# Retries for transient Yahoo responses
_RETRY_MAX = max(0, _env_int("YAHOO_FUND_RETRY_MAX", 2))  # additional retries (0 => no retry)
_RETRY_BACKOFF_MS = _env_float("YAHOO_FUND_RETRY_BACKOFF_MS", 250.0)
_RETRY_STATUSES = {403, 429, 500, 502, 503, 504}  # 403 often cookie/edge blocks

# TTL Cache
_TTL_SEC = max(30.0, _env_float("YAHOO_FUND_TTL_SEC", 3600.0))

# Region / language
_REGION_KSA = (_env_str("YAHOO_REGION_KSA", "SA") or "SA").strip()
_REGION_GLOBAL = (_env_str("YAHOO_REGION_GLOBAL", "US") or "US").strip()
_LANG = (_env_str("YAHOO_LANG", "en-US") or "en-US").strip()

# ---------------------------
# Client reuse (keep-alive)
# ---------------------------
_CLIENT: Optional[httpx.AsyncClient] = None
_CLIENT_LOCK = asyncio.Lock()

# Best-effort warmup set (avoid repeated warmups)
_WARMED: Set[str] = set()
_WARM_LOCK = asyncio.Lock()

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


def _json_headers(env_key: str) -> Dict[str, str]:
    raw = _env_str(env_key, "")
    if not raw:
        return {}
    try:
        obj = json.loads(raw)
        if isinstance(obj, dict):
            return {str(k): str(v) for k, v in obj.items()}
    except Exception:
        return {}
    return {}


# Base headers (shared across Yahoo providers)
_BASE_HEADERS: Dict[str, str] = {
    "User-Agent": USER_AGENT,
    "Accept": "application/json,*/*;q=0.8",
    "Accept-Language": _ACCEPT_LANGUAGE,
    "Connection": "keep-alive",
    "DNT": "1",
    "Pragma": "no-cache",
    "Cache-Control": "no-cache",
}
_BASE_HEADERS.update(_json_headers("YAHOO_HEADERS_JSON"))
# Provider-level overrides
_BASE_HEADERS.update(_json_headers("YAHOO_HEADERS_FUND_JSON"))


def _headers_for(kind: str, symbol: str) -> Dict[str, str]:
    """
    kind:
      - "summary" for quoteSummary
      - "quote"   for v7 quote
      - "warmup"  for finance.yahoo.com
    """
    sym = (symbol or "").strip().upper()
    ref = f"https://finance.yahoo.com/quote/{sym}"

    h = dict(_BASE_HEADERS)

    if kind == "summary":
        h.update(_json_headers("YAHOO_HEADERS_FUND_SUMMARY_JSON"))
    elif kind == "quote":
        h.update(_json_headers("YAHOO_HEADERS_FUND_QUOTE_JSON"))

    # Dynamic (helps Yahoo edges)
    h.setdefault("Referer", ref)
    h.setdefault("Origin", "https://finance.yahoo.com")
    return h


async def _get_client() -> httpx.AsyncClient:
    global _CLIENT
    if _CLIENT is not None:
        return _CLIENT
    async with _CLIENT_LOCK:
        if _CLIENT is None:
            _CLIENT = httpx.AsyncClient(
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
    if s.endswith(".US"):
        return s[:-3]
    return s


def _get_raw(obj: Any) -> Optional[float]:
    """Yahoo often returns { raw: ..., fmt: ... }."""
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


def _to_percent(v: Optional[float]) -> Optional[float]:
    """
    Normalize percent-like fields to percent.
    If Yahoo returns fraction (0.12), convert to 12.0
    If already percent-like (12.0), keep as-is.
    """
    if v is None:
        return None
    try:
        if abs(v) <= 1.5:
            return v * 100.0
        return v
    except Exception:
        return v


def _compute_if_possible(out: Dict[str, Any]) -> None:
    """Compute PE and Market Cap if Yahoo didn't provide them."""
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


def _clamp(x: float, lo: float, hi: float) -> float:
    return float(max(lo, min(hi, x)))


def _apply_targets_forecast(out: Dict[str, Any]) -> None:
    """
    Forecast enrichment using analyst targets (best-effort).
    We map:
      fair_value         := target_mean_price
      upside_percent     := (fair_value/current_price - 1)*100
      expected_price_12m := fair_value
      expected_return_12m:= upside_percent
    """
    if not ENABLE_TARGETS:
        return

    px = _safe_float(out.get("current_price"))
    t_mean = _safe_float(out.get("target_mean_price"))
    if px in (None, 0.0) or t_mean is None:
        return

    out["fair_value"] = t_mean
    out["expected_price_12m"] = t_mean
    out["upside_percent"] = ((t_mean / px) - 1.0) * 100.0
    out["expected_return_12m"] = out["upside_percent"]
    out["forecast_method"] = "yahoo_analyst_targets_v1"

    # confidence_score: analyst count helps; huge target spread hurts
    n = _safe_float(out.get("analyst_opinions"))
    n_i = int(n) if n is not None and n >= 0 else 0

    # base by analyst count (0 => low)
    base = 20.0 + (10.0 * math.log2(n_i + 1))  # 0->20, 3->40, 7->50, 15->60...
    base = _clamp(base, 15.0, 75.0)

    t_hi = _safe_float(out.get("target_high_price"))
    t_lo = _safe_float(out.get("target_low_price"))
    penalty = 0.0
    if t_hi is not None and t_lo is not None and t_mean not in (None, 0.0):
        spread = abs(t_hi - t_lo) / max(1e-9, abs(t_mean))
        penalty = _clamp(spread * 80.0, 0.0, 45.0)  # wide spread => reduce confidence

    conf = _clamp(base - penalty, 0.0, 100.0)
    out["confidence_score"] = conf


def _has_any_useful(out: Dict[str, Any]) -> bool:
    keys = (
        "market_cap",
        "shares_outstanding",
        "eps_ttm",
        "pe_ttm",
        "pb",
        "dividend_yield",
        "dividend_rate",
        "payout_ratio",
        "roe",
        "roa",
        "beta",
        "forward_eps",
        "forward_pe",
        "ps",
        "ev_ebitda",
        "fair_value",
        "upside_percent",
        "expected_return_12m",
    )
    return any(_safe_float(out.get(k)) is not None for k in keys)


def _base(symbol: str) -> Dict[str, Any]:
    # NOTE: "data_source"/"provider_version" aligned with your other providers.
    return {
        "symbol": symbol,
        "data_source": PROVIDER_NAME,
        "provider_version": PROVIDER_VERSION,
        "last_updated_utc": _now_utc_iso(),
        "currency": None,
        "current_price": None,
        "market_cap": None,
        "shares_outstanding": None,
        "eps_ttm": None,
        "pe_ttm": None,
        "forward_eps": None,
        "forward_pe": None,
        "pb": None,
        "ps": None,
        "ev_ebitda": None,
        "dividend_yield": None,  # percent
        "dividend_rate": None,
        "payout_ratio": None,  # percent
        "roe": None,  # percent
        "roa": None,  # percent
        "beta": None,
        # Target/forecast enrichment (optional)
        "target_mean_price": None,
        "target_high_price": None,
        "target_low_price": None,
        "analyst_opinions": None,
        "recommendation_mean": None,
        "recommendation_key": None,
        "fair_value": None,
        "upside_percent": None,
        "expected_price_12m": None,
        "expected_return_12m": None,
        "confidence_score": None,
        "forecast_method": None,
        "error": None,
    }


async def _sleep_backoff(attempt: int) -> None:
    base = (_RETRY_BACKOFF_MS / 1000.0) * (2**attempt)
    await asyncio.sleep(min(2.5, base + random.random() * 0.35))


def _region_for(sym: str) -> str:
    return _REGION_KSA if sym.endswith(".SR") else _REGION_GLOBAL


async def _warmup_cookie(symbol: str) -> None:
    """Best-effort: hit finance.yahoo.com to set cookies to reduce 401/403 blocks."""
    sym = (symbol or "").strip().upper()
    if not sym:
        return

    key = f"warm::{sym}"
    async with _WARM_LOCK:
        if key in _WARMED:
            return
        _WARMED.add(key)

    try:
        client = await _get_client()
        url = _WARMUP_URL.format(symbol=sym)
        await client.get(url, headers=_headers_for("warmup", sym))
    except Exception:
        pass


async def _http_get_json(
    symbol: str,
    url: str,
    params: Dict[str, Any],
    *,
    kind: str,
) -> Tuple[Optional[Dict[str, Any]], Optional[str]]:
    client = await _get_client()
    headers = _headers_for(kind, symbol)

    attempts = 1 + max(0, _RETRY_MAX)
    last_err: Optional[str] = None
    warmed = False

    for i in range(attempts):
        try:
            r = await client.get(url, params=params, headers=headers)

            if r.status_code == 200:
                j = _safe_json(r)
                if j:
                    return j, None
                last_err = "invalid JSON"
            else:
                if r.status_code in (401, 403) and not warmed:
                    warmed = True
                    await _warmup_cookie(symbol)
                    last_err = f"HTTP {r.status_code} (warmup)"
                elif r.status_code in _RETRY_STATUSES and i < attempts - 1:
                    last_err = f"HTTP {r.status_code}"
                else:
                    return None, f"HTTP {r.status_code}"
        except Exception as e:
            last_err = f"{e.__class__.__name__}: {e}"

        if i < attempts - 1:
            await _sleep_backoff(i)

    return None, last_err or "request failed"


# ---------------------------
# Core fetcher
# ---------------------------
async def yahoo_fundamentals(symbol: str, *, debug: bool = False) -> Dict[str, Any]:
    sym = _yahoo_symbol(symbol)

    # Feature flags
    if not ENABLE_YAHOO_FUNDAMENTALS:
        return {
            **_base(sym),
            "error": "Yahoo fundamentals disabled (ENABLE_YAHOO_FUNDAMENTALS=false)",
            "last_updated_utc": _now_utc_iso(),
        }

    if sym.endswith(".SR") and not ENABLE_YAHOO_FUNDAMENTALS_KSA:
        return {
            **_base(sym),
            "error": "Yahoo fundamentals disabled for KSA (ENABLE_YAHOO_FUNDAMENTALS_KSA=false)",
            "last_updated_utc": _now_utc_iso(),
        }

    out = _base(sym)
    if not sym:
        out["error"] = "Empty symbol"
        return out

    # Cache (bypass in debug)
    ck = f"yahoo_fund::{PROVIDER_VERSION}::{sym}"
    if not debug:
        hit = _CACHE.get(ck)
        if isinstance(hit, dict) and hit:
            return dict(hit)

    region = _region_for(sym)
    lang = _LANG
    last_err: Optional[str] = None

    # 1) quoteSummary (richer)
    for host in _HOSTS:
        url = f"{host}{_QUOTE_SUMMARY_PATH}".format(symbol=sym)
        params = {
            "modules": "price,summaryDetail,defaultKeyStatistics,financialData",
            "lang": lang,
            "region": region,
        }
        js, err = await _http_get_json(sym, url, params, kind="summary")
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

            out["market_cap"] = (
                _get_raw(price.get("marketCap"))
                or _get_raw(stats.get("marketCap"))
                or out["market_cap"]
            )
            out["shares_outstanding"] = _get_raw(stats.get("sharesOutstanding")) or out["shares_outstanding"]

            # EPS + PE (TTM)
            out["eps_ttm"] = _get_raw(stats.get("trailingEps")) or out["eps_ttm"]
            out["pe_ttm"] = _get_raw(summ.get("trailingPE")) or _get_raw(stats.get("trailingPE")) or out["pe_ttm"]

            # Forward EPS + Forward PE
            out["forward_eps"] = _get_raw(stats.get("forwardEps")) or out["forward_eps"]
            out["forward_pe"] = _get_raw(summ.get("forwardPE")) or _get_raw(stats.get("forwardPE")) or out["forward_pe"]

            # PB (or compute using bookValue)
            out["pb"] = _get_raw(stats.get("priceToBook")) or out["pb"]
            if out["pb"] is None:
                bv = _get_raw(fin.get("bookValue")) or _get_raw(stats.get("bookValue"))
                px = _safe_float(out.get("current_price"))
                if bv not in (None, 0.0) and px is not None:
                    try:
                        out["pb"] = px / bv
                    except Exception:
                        pass

            # P/S
            out["ps"] = _get_raw(summ.get("priceToSalesTrailing12Months")) or out["ps"]

            # EV/EBITDA
            out["ev_ebitda"] = (
                _get_raw(stats.get("enterpriseToEbitda"))
                or _get_raw(fin.get("enterpriseToEbitda"))
                or out["ev_ebitda"]
            )

            # Dividends (dividendYield usually fraction)
            dy = _get_raw(summ.get("dividendYield"))
            if dy is not None:
                out["dividend_yield"] = _to_percent(dy)
            out["dividend_rate"] = _get_raw(summ.get("dividendRate")) or out["dividend_rate"]

            # Payout ratio (usually fraction)
            pr = _get_raw(summ.get("payoutRatio"))
            if pr is not None:
                out["payout_ratio"] = _to_percent(pr)

            # ROE/ROA (often fraction)
            roe = _get_raw(fin.get("returnOnEquity"))
            roa = _get_raw(fin.get("returnOnAssets"))
            if roe is not None:
                out["roe"] = _to_percent(roe)
            if roa is not None:
                out["roa"] = _to_percent(roa)

            out["beta"] = _get_raw(stats.get("beta")) or out["beta"]

            # Analyst targets / recommendation (forecast enrichment)
            out["target_mean_price"] = _get_raw(fin.get("targetMeanPrice")) or out["target_mean_price"]
            out["target_high_price"] = _get_raw(fin.get("targetHighPrice")) or out["target_high_price"]
            out["target_low_price"] = _get_raw(fin.get("targetLowPrice")) or out["target_low_price"]
            out["analyst_opinions"] = _get_raw(fin.get("numberOfAnalystOpinions")) or out["analyst_opinions"]
            out["recommendation_mean"] = _get_raw(fin.get("recommendationMean")) or out["recommendation_mean"]
            # recommendationKey is a string, keep it raw
            try:
                rk = fin.get("recommendationKey")
                if isinstance(rk, str) and rk.strip():
                    out["recommendation_key"] = rk.strip().lower()
            except Exception:
                pass

            _compute_if_possible(out)
            _apply_targets_forecast(out)

            if debug:
                out["_debug"] = {"host": host, "region": region, "lang": lang, "source": "quoteSummary"}

            if _has_any_useful(out):
                out["error"] = None
                out["last_updated_utc"] = _now_utc_iso()
                if not debug:
                    _CACHE[ck] = dict(out)
                return out

            last_err = "quoteSummary returned no usable fields"
        except Exception as e:
            last_err = f"quoteSummary parse error: {e.__class__.__name__}"

    # 2) quote v7 fallback (lighter)
    for host in _HOSTS:
        url = f"{host}{_QUOTE_V7_PATH}"
        params = {"symbols": sym, "lang": lang, "region": region}
        js, err = await _http_get_json(sym, url, params, kind="quote")
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

            out["forward_eps"] = _safe_float(q0.get("epsForward")) or out["forward_eps"]
            out["forward_pe"] = _safe_float(q0.get("forwardPE")) or out["forward_pe"]

            out["pb"] = _safe_float(q0.get("priceToBook")) or out["pb"]
            out["ps"] = _safe_float(q0.get("priceToSalesTrailing12Months")) or out["ps"]
            out["ev_ebitda"] = _safe_float(q0.get("enterpriseToEbitda")) or out["ev_ebitda"]

            dy = _safe_float(q0.get("trailingAnnualDividendYield"))
            if dy is not None:
                out["dividend_yield"] = _to_percent(dy)
            out["dividend_rate"] = _safe_float(q0.get("trailingAnnualDividendRate")) or out["dividend_rate"]

            pr = _safe_float(q0.get("payoutRatio"))
            if pr is not None:
                out["payout_ratio"] = _to_percent(pr)

            # Some v7 responses can include these
            roe = _safe_float(q0.get("returnOnEquity"))
            roa = _safe_float(q0.get("returnOnAssets"))
            if roe is not None:
                out["roe"] = _to_percent(roe)
            if roa is not None:
                out["roa"] = _to_percent(roa)

            out["beta"] = _safe_float(q0.get("beta")) or out["beta"]

            _compute_if_possible(out)
            _apply_targets_forecast(out)  # v7 may not have targets, but safe

            if debug:
                out["_debug"] = {"host": host, "region": region, "lang": lang, "source": "quote_v7"}

            if _has_any_useful(out):
                out["error"] = None
                out["last_updated_utc"] = _now_utc_iso()
                if not debug:
                    _CACHE[ck] = dict(out)
                return out

            last_err = "quote v7 returned no usable fields"
        except Exception as e:
            last_err = f"quote v7 parse error: {e.__class__.__name__}"

    # Failed
    out["error"] = last_err or "yahoo fundamentals failed"
    out["last_updated_utc"] = _now_utc_iso()
    if not debug:
        _CACHE[ck] = dict(out)
    return out


# --- Engine adapter (DataEngineV2 patch functions) ---------------------------
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
    Returns a patch aligned to UnifiedQuote keys + safe extras.
    Percent fields are sheet-ready (% not fraction):
      - dividend_yield (%)
      - roe (%)
      - roa (%)
      - payout_ratio (%)
    Forecast fields are best-effort (if targets exist):
      - fair_value, upside_percent, expected_price_12m, expected_return_12m, confidence_score
    """
    d = await yahoo_fundamentals(symbol, debug=debug)

    patch: Dict[str, Any] = {
        "data_source": PROVIDER_NAME,
        "provider_version": PROVIDER_VERSION,
        "last_updated_utc": d.get("last_updated_utc"),

        "currency": d.get("currency"),
        "current_price": d.get("current_price"),

        "market_cap": d.get("market_cap"),
        "shares_outstanding": d.get("shares_outstanding"),

        "eps_ttm": d.get("eps_ttm"),
        "pe_ttm": d.get("pe_ttm"),

        "forward_eps": d.get("forward_eps"),
        "forward_pe": d.get("forward_pe"),

        "pb": d.get("pb"),
        "ps": d.get("ps"),
        "ev_ebitda": d.get("ev_ebitda"),

        "dividend_yield": d.get("dividend_yield"),
        "dividend_rate": d.get("dividend_rate"),
        "payout_ratio": d.get("payout_ratio"),

        "roe": d.get("roe"),
        "roa": d.get("roa"),
        "beta": d.get("beta"),

        # Forecast / targets (extras; engine can ignore if not used)
        "target_mean_price": d.get("target_mean_price"),
        "target_high_price": d.get("target_high_price"),
        "target_low_price": d.get("target_low_price"),
        "analyst_opinions": d.get("analyst_opinions"),
        "recommendation_mean": d.get("recommendation_mean"),
        "recommendation_key": d.get("recommendation_key"),

        "fair_value": d.get("fair_value"),
        "upside_percent": d.get("upside_percent"),
        "expected_price_12m": d.get("expected_price_12m"),
        "expected_return_12m": d.get("expected_return_12m"),
        "confidence_score": d.get("confidence_score"),
        "forecast_method": d.get("forecast_method"),
    }

    # Surface warning only if provider failed (engine can still merge other providers)
    if d.get("error"):
        patch["error"] = f"warning: {PROVIDER_NAME}: {d.get('error')}"
    return _clean_patch(patch)


# Engine discovery aliases
async def fetch_quote_patch(symbol: str, debug: bool = False, *args: Any, **kwargs: Any) -> Dict[str, Any]:
    return await fetch_fundamentals_patch(symbol, debug=debug)


async def fetch_enriched_quote_patch(symbol: str, debug: bool = False, *args: Any, **kwargs: Any) -> Dict[str, Any]:
    return await fetch_fundamentals_patch(symbol, debug=debug)


async def fetch_quote_and_enrichment_patch(
    symbol: str, debug: bool = False, *args: Any, **kwargs: Any
) -> Dict[str, Any]:
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
