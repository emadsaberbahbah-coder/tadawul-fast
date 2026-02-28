#!/usr/bin/env python3
# core/providers/eodhd_provider.py
"""
================================================================================
EODHD Provider — v4.2.0 (GLOBAL-STABLE / RENDER-SAFE / RATIO-SAFE / PRIMARY)
================================================================================

Main goals (aligned with your ratio-safe / sheet-stable pipeline):
- ✅ GLOBAL-first symbol normalization:
     - "AAPL" -> "AAPL.US" (configurable; uses core.symbols.normalize.to_eodhd_symbol if available)
- ✅ Render-safe: NO hard dependency on numpy/orjson/scipy/sklearn/statsmodels, etc.
- ✅ Production-grade HTTP behavior:
     - bounded concurrency + soft rate limiter (token bucket)
     - retries with Full-Jitter exponential backoff
     - correct handling for 429 (Retry-After)
- ✅ Ratio-safe outputs:
     - any "*_pct" and most percent-like fields are returned as **fractions** (0.12 == 12%)
       so downstream (router) can safely format them as percentages without explosions.
- ✅ Returns a consistent enriched patch:
     name, currency, current_price, previous_close, change, change_pct (fraction), etc.
- ✅ Optional history enrichment:
     52W high/low, 52W position (fraction), avg vol 30D, volatility 30D (fraction),
     MA20/MA50/MA200, RSI14, and simple returns (1W/1M/3M/6M/12M) as fractions.

Env vars:
- EODHD_API_KEY (or EODHD_API_TOKEN / EODHD_KEY)
- EODHD_BASE_URL (default: https://eodhistoricaldata.com/api)
- EODHD_DEFAULT_EXCHANGE (default: US)
- EODHD_APPEND_EXCHANGE_SUFFIX (default: true)
- EODHD_RATE_LIMIT_RPS (default: 4.0)
- EODHD_RATE_LIMIT_BURST (default: 8.0)
- EODHD_TIMEOUT_SEC (default: 15)
- EODHD_RETRY_ATTEMPTS (default: 4)
- EODHD_ENABLE_FUNDAMENTALS (default: true)
- EODHD_ENABLE_HISTORY (default: true)
- EODHD_HISTORY_DAYS (default: 420)
- EODHD_HISTORY_WINDOW_52W (default: 252)
- KSA_DISALLOW_EODHD (default: false)
- ALLOW_EODHD_KSA / EODHD_ALLOW_KSA (override)
"""

from __future__ import annotations

import asyncio
import logging
import math
import os
import random
import re
import time
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Callable, Dict, List, Optional, Tuple, Union

import httpx

logger = logging.getLogger("core.providers.eodhd_provider")

PROVIDER_NAME = "eodhd"
PROVIDER_VERSION = "4.2.0"

DEFAULT_BASE_URL = "https://eodhistoricaldata.com/api"
UA_DEFAULT = "TFB-EODHD/4.2.0 (Render)"

# -----------------------------
# Optional JSON perf (orjson)
# -----------------------------
try:
    import orjson  # type: ignore

    def json_loads(data: Union[str, bytes]) -> Any:
        if isinstance(data, str):
            data = data.encode("utf-8")
        return orjson.loads(data)

    _HAS_ORJSON = True
except Exception:
    import json

    def json_loads(data: Union[str, bytes]) -> Any:
        if isinstance(data, bytes):
            data = data.decode("utf-8", errors="replace")
        return json.loads(data)

    _HAS_ORJSON = False

_TRUTHY = {"1", "true", "yes", "y", "on", "t"}
_FALSY = {"0", "false", "no", "n", "off", "f"}

_US_LIKE_RE = re.compile(r"^[A-Z0-9][A-Z0-9\-\_]{0,11}$")  # allow BRK-B style
_KSA_RE = re.compile(r"^\d{3,6}\.SR$", re.IGNORECASE)


# =============================================================================
# Env helpers
# =============================================================================
def _env_str(name: str, default: str = "") -> str:
    return (os.getenv(name) or default).strip()


def _env_bool(name: str, default: bool = False) -> bool:
    raw = (os.getenv(name) or "").strip().lower()
    if not raw:
        return default
    if raw in _TRUTHY:
        return True
    if raw in _FALSY:
        return False
    return default


def _env_int(name: str, default: int, lo: Optional[int] = None, hi: Optional[int] = None) -> int:
    try:
        v = int(float((os.getenv(name) or str(default)).strip()))
    except Exception:
        v = default
    if lo is not None and v < lo:
        v = lo
    if hi is not None and v > hi:
        v = hi
    return v


def _env_float(name: str, default: float, lo: Optional[float] = None, hi: Optional[float] = None) -> float:
    try:
        v = float((os.getenv(name) or str(default)).strip())
    except Exception:
        v = default
    if lo is not None and v < lo:
        v = lo
    if hi is not None and v > hi:
        v = hi
    return v


def _token() -> str:
    return _env_str("EODHD_API_KEY") or _env_str("EODHD_API_TOKEN") or _env_str("EODHD_KEY")


def _base_url() -> str:
    return _env_str("EODHD_BASE_URL", DEFAULT_BASE_URL).rstrip("/")


def _default_exchange() -> str:
    ex = _env_str("EODHD_DEFAULT_EXCHANGE") or _env_str("EODHD_SYMBOL_SUFFIX_DEFAULT") or "US"
    return ex.strip().upper() or "US"


def _append_exchange_suffix() -> bool:
    return _env_bool("EODHD_APPEND_EXCHANGE_SUFFIX", True)


def _ksa_blocked_by_default() -> bool:
    return _env_bool("KSA_DISALLOW_EODHD", False)


def _allow_ksa_override() -> bool:
    return _env_bool("ALLOW_EODHD_KSA", False) or _env_bool("EODHD_ALLOW_KSA", False)


def _utc_iso(dt: Optional[datetime] = None) -> str:
    d = dt or datetime.now(timezone.utc)
    if d.tzinfo is None:
        d = d.replace(tzinfo=timezone.utc)
    return d.astimezone(timezone.utc).isoformat()


def _riyadh_iso(dt: Optional[datetime] = None) -> str:
    d = dt or datetime.now(timezone.utc)
    tz = timezone(timedelta(hours=3))
    if d.tzinfo is None:
        d = d.replace(tzinfo=timezone.utc)
    return d.astimezone(tz).isoformat()


# =============================================================================
# Coercion helpers
# =============================================================================
def safe_float(v: Any) -> Optional[float]:
    if v is None:
        return None
    try:
        if isinstance(v, (int, float)) and not isinstance(v, bool):
            x = float(v)
        else:
            s = str(v).strip().replace(",", "")
            if not s:
                return None
            if s.endswith("%"):
                s = s[:-1].strip()
                x = float(s) / 100.0
            else:
                x = float(s)
        if math.isnan(x) or math.isinf(x):
            return None
        return x
    except Exception:
        return None


def safe_str(v: Any) -> Optional[str]:
    if v is None:
        return None
    try:
        s = str(v).strip()
        return s if s else None
    except Exception:
        return None


def _clean_patch(p: Dict[str, Any]) -> Dict[str, Any]:
    return {k: v for k, v in (p or {}).items() if v is not None}


def _frac_from_percentish(v: Any) -> Optional[float]:
    """
    Normalize percent-like value to a fraction:
      - "12%" -> 0.12
      - 12    -> 0.12
      - 0.12  -> 0.12
    """
    f = safe_float(v)
    if f is None:
        return None
    if abs(f) > 1.5:  # assume percent-points
        return f / 100.0
    return f


# =============================================================================
# Symbol normalization (GLOBAL fix)
# =============================================================================
def normalize_eodhd_symbol(symbol: str) -> str:
    """
    Normalize a symbol for EODHD.

    Preferred:
      - Use core.symbols.normalize.to_eodhd_symbol if available
    Fallback:
      - keep already-qualified: AAPL.US, 7203.T, VOD.L, 2222.SR
      - if plain US-like ticker: AAPL -> AAPL.US (configurable)
      - do not modify indices/fx/futures notations (^GSPC, EURUSD=X, GC=F, etc.)
    """
    s = (symbol or "").strip()
    if not s:
        return ""
    s_up = s.upper()

    # optional shared normalizer (best)
    try:
        from core.symbols.normalize import to_eodhd_symbol as _to_eodhd_symbol  # type: ignore

        if callable(_to_eodhd_symbol):
            out = _to_eodhd_symbol(s_up)
            if isinstance(out, str) and out.strip():
                return out.strip().upper()
    except Exception:
        pass

    # already qualified
    if "." in s_up:
        return s_up

    # do not touch special notations
    if "=" in s_up or "^" in s_up or "/" in s_up:
        return s_up

    if not _append_exchange_suffix():
        return s_up

    if _US_LIKE_RE.match(s_up):
        return f"{s_up}.{_default_exchange()}"

    return s_up


# =============================================================================
# Async primitives: SingleFlight + TTL Cache + TokenBucket
# =============================================================================
class _SingleFlight:
    def __init__(self) -> None:
        self._lock = asyncio.Lock()
        self._calls: Dict[str, asyncio.Future] = {}

    async def do(self, key: str, coro_factory: Callable[[], Any]) -> Any:
        async with self._lock:
            fut = self._calls.get(key)
            if fut is None:
                fut = asyncio.get_running_loop().create_future()
                self._calls[key] = fut
                owner = True
            else:
                owner = False

        if not owner:
            return await fut  # type: ignore

        try:
            res = await coro_factory()
            if not fut.done():
                fut.set_result(res)
            return res
        except Exception as e:
            if not fut.done():
                fut.set_exception(e)
            raise
        finally:
            async with self._lock:
                self._calls.pop(key, None)


@dataclass
class _CacheItem:
    exp: float
    val: Any


class _TTLCache:
    def __init__(self, maxsize: int, ttl_sec: float):
        self.maxsize = max(128, int(maxsize))
        self.ttl_sec = max(1.0, float(ttl_sec))
        self._d: Dict[str, _CacheItem] = {}
        self._lock = asyncio.Lock()

    async def get(self, key: str) -> Any:
        now = time.monotonic()
        async with self._lock:
            it = self._d.get(key)
            if not it:
                return None
            if it.exp > now:
                return it.val
            self._d.pop(key, None)
            return None

    async def set(self, key: str, val: Any, ttl_sec: Optional[float] = None) -> None:
        now = time.monotonic()
        ttl = self.ttl_sec if ttl_sec is None else max(1.0, float(ttl_sec))
        async with self._lock:
            if len(self._d) >= self.maxsize and key not in self._d:
                self._d.pop(next(iter(self._d.keys())), None)
            self._d[key] = _CacheItem(exp=now + ttl, val=val)


class _TokenBucket:
    def __init__(self, rate_per_sec: float, burst: float):
        self.rate = max(0.0, float(rate_per_sec))
        self.capacity = max(1.0, float(burst))
        self.tokens = self.capacity
        self.last = time.monotonic()
        self._lock = asyncio.Lock()

    async def wait(self, amount: float = 1.0) -> None:
        if self.rate <= 0:
            return
        amount = max(0.0001, float(amount))
        while True:
            async with self._lock:
                now = time.monotonic()
                elapsed = max(0.0, now - self.last)
                self.last = now
                self.tokens = min(self.capacity, self.tokens + elapsed * self.rate)
                if self.tokens >= amount:
                    self.tokens -= amount
                    return
                deficit = amount - self.tokens
                sleep_s = deficit / self.rate if self.rate > 0 else 0.25
            await asyncio.sleep(min(1.0, max(0.05, sleep_s)))


# =============================================================================
# EODHD Client
# =============================================================================
class EODHDClient:
    def __init__(self) -> None:
        self.api_key = _token()
        self.base_url = _base_url()

        self.timeout_sec = _env_float("EODHD_TIMEOUT_SEC", 15.0, lo=3.0, hi=120.0)
        self.retry_attempts = _env_int("EODHD_RETRY_ATTEMPTS", 4, lo=0, hi=10)
        self.retry_base_delay = _env_float("EODHD_RETRY_BASE_DELAY", 0.6, lo=0.0, hi=10.0)

        self.max_concurrency = _env_int("EODHD_MAX_CONCURRENCY", 20, lo=1, hi=100)
        self._sem = asyncio.Semaphore(self.max_concurrency)

        rps = _env_float("EODHD_RATE_LIMIT_RPS", 4.0, lo=0.0, hi=50.0)
        burst = _env_float("EODHD_RATE_LIMIT_BURST", 8.0, lo=1.0, hi=200.0)
        self._bucket = _TokenBucket(rate_per_sec=rps, burst=burst)

        self._sf = _SingleFlight()

        self.quote_cache = _TTLCache(maxsize=6000, ttl_sec=_env_float("EODHD_QUOTE_TTL_SEC", 12.0, lo=1.0, hi=600.0))
        self.fund_cache = _TTLCache(maxsize=3000, ttl_sec=_env_float("EODHD_FUND_TTL_SEC", 21600.0, lo=60.0, hi=86400.0))
        self.hist_cache = _TTLCache(maxsize=2000, ttl_sec=_env_float("EODHD_HISTORY_TTL_SEC", 1800.0, lo=60.0, hi=86400.0))

        self._client = httpx.AsyncClient(
            timeout=httpx.Timeout(self.timeout_sec),
            headers={"User-Agent": _env_str("EODHD_USER_AGENT", UA_DEFAULT)},
            limits=httpx.Limits(max_keepalive_connections=30, max_connections=60),
            http2=True,
        )

    def _base_params(self) -> Dict[str, str]:
        return {"api_token": self.api_key, "fmt": "json"}

    async def _request_json(self, endpoint: str, params: Optional[Dict[str, Any]] = None) -> Tuple[Optional[Any], Optional[str]]:
        if not self.api_key:
            return None, "EODHD_API_KEY missing"

        url = f"{self.base_url}/{endpoint.lstrip('/')}"
        p = {**self._base_params(), **(params or {})}

        last_err: Optional[str] = None

        async with self._sem:
            for attempt in range(max(1, self.retry_attempts + 1)):
                await self._bucket.wait(1.0)

                try:
                    r = await self._client.get(url, params=p)
                    sc = int(r.status_code)

                    # 429 rate limit
                    if sc == 429:
                        ra = r.headers.get("Retry-After")
                        wait = float(ra) if ra and ra.isdigit() else min(15.0, 1.0 + attempt)
                        last_err = "HTTP 429"
                        await asyncio.sleep(wait)
                        continue

                    # transient server errors
                    if 500 <= sc < 600:
                        last_err = f"HTTP {sc}"
                        base = self.retry_base_delay * (2 ** (attempt - 1))
                        await asyncio.sleep(min(12.0, random.uniform(0, base + 0.25)))
                        continue

                    if sc == 404:
                        return None, "HTTP 404 not_found"

                    if sc >= 400:
                        return None, f"HTTP {sc}"

                    try:
                        data = json_loads(r.content)
                        return data, None
                    except Exception:
                        return None, "invalid_json_payload"

                except httpx.RequestError as e:
                    last_err = f"network_error:{e.__class__.__name__}"
                    base = self.retry_base_delay * (2 ** (attempt - 1))
                    await asyncio.sleep(min(12.0, random.uniform(0, base + 0.25)))
                except Exception as e:
                    last_err = f"unexpected_error:{e.__class__.__name__}"
                    break

        return None, last_err or "request_failed"

    # ---------------------------------------------------------------------
    # Quote
    # ---------------------------------------------------------------------
    async def fetch_quote(self, symbol: str) -> Tuple[Dict[str, Any], Optional[str]]:
        sym = normalize_eodhd_symbol(symbol)
        if not sym:
            return {}, "invalid_symbol"

        if _KSA_RE.match(sym) and _ksa_blocked_by_default() and not _allow_ksa_override():
            return {}, "ksa_blocked"

        ck = f"q:{sym}"
        cached = await self.quote_cache.get(ck)
        if cached:
            return cached, None

        async def _do():
            data, err = await self._request_json(f"real-time/{sym}", params={})
            if err or not isinstance(data, dict):
                return {}, err or "bad_payload"

            close = safe_float(data.get("close"))
            prev = safe_float(data.get("previousClose"))
            chg = safe_float(data.get("change"))

            # ratio-safe: compute change_pct as FRACTION from price/prev whenever possible
            change_pct = None
            if close is not None and prev not in (None, 0.0):
                change_pct = (close / prev) - 1.0
            else:
                # fallback: change_p often comes as percent-points (0.79 means 0.79%)
                # Convert percentish -> fraction safely
                change_pct = _frac_from_percentish(data.get("change_p"))

            patch = _clean_patch(
                {
                    "symbol": (symbol or "").strip().upper(),
                    "symbol_normalized": sym,
                    "provider": PROVIDER_NAME,
                    "data_source": PROVIDER_NAME,
                    "current_price": close,
                    "previous_close": prev,
                    "change": chg if chg is not None else (close - prev if close is not None and prev is not None else None),
                    "change_pct": change_pct,  # FRACTION (0.0079 == 0.79%)
                    "day_open": safe_float(data.get("open")),
                    "day_high": safe_float(data.get("high")),
                    "day_low": safe_float(data.get("low")),
                    "volume": safe_float(data.get("volume")),
                    "currency": safe_str(data.get("currency")) or None,
                    "last_updated_utc": _utc_iso(),
                    "last_updated_riyadh": _riyadh_iso(),
                }
            )
            await self.quote_cache.set(ck, patch)
            return patch, None

        return await self._sf.do(ck, _do)

    # ---------------------------------------------------------------------
    # Fundamentals
    # ---------------------------------------------------------------------
    async def fetch_fundamentals(self, symbol: str) -> Tuple[Dict[str, Any], Optional[str]]:
        sym = normalize_eodhd_symbol(symbol)
        if not sym:
            return {}, "invalid_symbol"

        if _KSA_RE.match(sym) and _ksa_blocked_by_default() and not _allow_ksa_override():
            return {}, "ksa_blocked"

        ck = f"f:{sym}"
        cached = await self.fund_cache.get(ck)
        if cached:
            return cached, None

        async def _do():
            data, err = await self._request_json(f"fundamentals/{sym}", params={})
            if err or not isinstance(data, dict):
                return {}, err or "bad_payload"

            general = data.get("General") or {}
            highlights = data.get("Highlights") or {}
            valuation = data.get("Valuation") or {}
            shares = data.get("SharesStats") or {}

            currency = safe_str(general.get("CurrencyCode")) or safe_str(general.get("Currency")) or None

            patch = _clean_patch(
                {
                    "symbol": (symbol or "").strip().upper(),
                    "symbol_normalized": sym,
                    "provider": PROVIDER_NAME,
                    "data_source": PROVIDER_NAME,
                    # identity
                    "name": safe_str(general.get("Name")),
                    "exchange": safe_str(general.get("Exchange")),
                    "country": safe_str(general.get("CountryName")) or safe_str(general.get("CountryISO")),
                    "currency": currency,
                    "sector": safe_str(general.get("Sector")),
                    "sub_sector": safe_str(general.get("Industry")),
                    # cap / shares
                    "market_cap": safe_float(highlights.get("MarketCapitalization")) or safe_float(general.get("MarketCapitalization")),
                    "shares_outstanding": safe_float(shares.get("SharesOutstanding")),
                    # earnings / valuation
                    "eps_ttm": safe_float(highlights.get("EarningsShare")) or safe_float(highlights.get("DilutedEpsTTM")),
                    "pe_ttm": safe_float(valuation.get("TrailingPE")) or safe_float(highlights.get("PERatio")),
                    "forward_pe": safe_float(valuation.get("ForwardPE")),
                    "pb": safe_float(valuation.get("PriceBookMRQ")) or safe_float(valuation.get("PriceBook")),
                    "ps": safe_float(valuation.get("PriceSalesTTM")) or safe_float(valuation.get("PriceSales")),
                    "ev_ebitda": safe_float(valuation.get("EnterpriseValueEbitda")) or safe_float(valuation.get("EnterpriseValueEBITDA")),
                    # percent-like fields as FRACTIONS
                    "dividend_yield": _frac_from_percentish(highlights.get("DividendYield")),
                    "payout_ratio": _frac_from_percentish(highlights.get("PayoutRatio")),
                    "roe": _frac_from_percentish(highlights.get("ROE")),
                    "roa": _frac_from_percentish(highlights.get("ROA")),
                    "net_margin": _frac_from_percentish(highlights.get("ProfitMargin")),
                    # timestamps
                    "last_updated_utc": _utc_iso(),
                    "last_updated_riyadh": _riyadh_iso(),
                }
            )
            await self.fund_cache.set(ck, patch)
            return patch, None

        return await self._sf.do(ck, _do)

    # ---------------------------------------------------------------------
    # History stats (52W, avg vol, vol, MA, RSI, simple returns)
    # ---------------------------------------------------------------------
    async def fetch_history_stats(self, symbol: str) -> Tuple[Dict[str, Any], Optional[str]]:
        sym = normalize_eodhd_symbol(symbol)
        if not sym:
            return {}, "invalid_symbol"

        if _KSA_RE.match(sym) and _ksa_blocked_by_default() and not _allow_ksa_override():
            return {}, "ksa_blocked"

        ck = f"h:{sym}"
        cached = await self.hist_cache.get(ck)
        if cached:
            return cached, None

        async def _do():
            days = _env_int("EODHD_HISTORY_DAYS", 420, lo=60, hi=5000)
            from_date = (datetime.now(timezone.utc) - timedelta(days=days)).strftime("%Y-%m-%d")

            data, err = await self._request_json(f"eod/{sym}", params={"from": from_date})
            if err or not isinstance(data, list):
                return {}, err or "bad_payload"

            closes: List[float] = []
            vols: List[float] = []
            last_hist_dt: Optional[str] = None

            for row in data:
                if not isinstance(row, dict):
                    continue
                c = safe_float(row.get("close"))
                if c is not None:
                    closes.append(c)
                    last_hist_dt = safe_str(row.get("date")) or last_hist_dt
                v = safe_float(row.get("volume"))
                if v is not None:
                    vols.append(v)

            n = len(closes)
            if n < 25:
                patch = {
                    "symbol": (symbol or "").strip().upper(),
                    "symbol_normalized": sym,
                    "provider": PROVIDER_NAME,
                    "data_source": PROVIDER_NAME,
                    "history_points": n,
                    "history_source": PROVIDER_NAME,
                    "history_last_utc": last_hist_dt,
                    "last_updated_utc": _utc_iso(),
                    "last_updated_riyadh": _riyadh_iso(),
                }
                await self.hist_cache.set(ck, patch)
                return patch, None

            last = closes[-1]

            # 52W window
            win_52 = _env_int("EODHD_HISTORY_WINDOW_52W", 252, lo=60, hi=800)
            win = min(win_52, n)
            low_52 = min(closes[-win:])
            high_52 = max(closes[-win:])
            pos_52_frac = None
            if high_52 != low_52:
                pos_52_frac = (last - low_52) / (high_52 - low_52)  # FRACTION 0..1

            # avg volume 30D
            avg_vol_30 = None
            if len(vols) >= 30:
                avg_vol_30 = sum(vols[-30:]) / 30.0

            # volatility 30D: stdev of daily returns (FRACTION)
            vol_30 = None
            if n >= 31:
                rets: List[float] = []
                start = n - 30
                for i in range(start, n):
                    if i <= 0:
                        continue
                    prev = closes[i - 1]
                    cur = closes[i]
                    if prev:
                        rets.append((cur / prev) - 1.0)
                if len(rets) >= 5:
                    m = sum(rets) / len(rets)
                    vol_30 = math.sqrt(sum((x - m) ** 2 for x in rets) / max(1, (len(rets) - 1)))

            # moving averages
            def _ma(k: int) -> Optional[float]:
                if n < k:
                    return None
                return sum(closes[-k:]) / k

            ma20 = _ma(20)
            ma50 = _ma(50)
            ma200 = _ma(200)

            # RSI 14
            def _rsi_14() -> Optional[float]:
                if n < 15:
                    return None
                gains = 0.0
                losses = 0.0
                for i in range(n - 14, n):
                    if i <= 0:
                        continue
                    d = closes[i] - closes[i - 1]
                    if d > 0:
                        gains += d
                    else:
                        losses += (-d)
                avg_gain = gains / 14.0
                avg_loss = losses / 14.0
                if avg_loss == 0:
                    return 100.0
                rs = avg_gain / avg_loss
                return 100.0 - (100.0 / (1.0 + rs))

            rsi14 = _rsi_14()

            # simple returns (FRACTIONS)
            def _ret(k: int) -> Optional[float]:
                if n <= k:
                    return None
                base = closes[-1 - k]
                if not base:
                    return None
                return (last / base) - 1.0

            returns_1w = _ret(5)
            returns_1m = _ret(21)
            returns_3m = _ret(63)
            returns_6m = _ret(126)
            returns_12m = _ret(252)

            patch = _clean_patch(
                {
                    "symbol": (symbol or "").strip().upper(),
                    "symbol_normalized": sym,
                    "provider": PROVIDER_NAME,
                    "data_source": PROVIDER_NAME,
                    # 52W
                    "low_52w": low_52,
                    "high_52w": high_52,
                    "position_52w_pct": pos_52_frac,      # FRACTION 0..1 (router will format safely)
                    "week_52_position_pct": pos_52_frac,  # alias
                    # liquidity / tech
                    "avg_vol_30d": avg_vol_30,
                    "avg_volume_30d": avg_vol_30,         # alias
                    "volatility_30d": vol_30,             # FRACTION
                    "rsi_14": rsi14,
                    "ma20": ma20,
                    "ma50": ma50,
                    "ma200": ma200,
                    # returns (fractions)
                    "returns_1w": returns_1w,
                    "returns_1m": returns_1m,
                    "returns_3m": returns_3m,
                    "returns_6m": returns_6m,
                    "returns_12m": returns_12m,
                    "return_1w": returns_1w,   # aliases for other schemas
                    "return_1m": returns_1m,
                    "return_3m": returns_3m,
                    "return_6m": returns_6m,
                    "return_1y": returns_12m,
                    # meta
                    "history_points": n,
                    "history_source": PROVIDER_NAME,
                    "history_last_utc": last_hist_dt,
                    "last_updated_utc": _utc_iso(),
                    "last_updated_riyadh": _riyadh_iso(),
                }
            )

            await self.hist_cache.set(ck, patch)
            return patch, None

        return await self._sf.do(ck, _do)

    # ---------------------------------------------------------------------
    # Public: enriched patch
    # ---------------------------------------------------------------------
    async def fetch_enriched_quote_patch(self, symbol: str) -> Dict[str, Any]:
        """
        Returns a dict patch to merge into the unified enriched quote model.
        Never throws.
        """
        now_utc = _utc_iso()
        now_riy = _riyadh_iso()

        sym_raw = (symbol or "").strip().upper()
        sym_norm = normalize_eodhd_symbol(sym_raw)

        if not sym_norm:
            return {
                "symbol": sym_raw,
                "symbol_normalized": "",
                "provider": PROVIDER_NAME,
                "data_source": PROVIDER_NAME,
                "data_quality": "MISSING",
                "error": "invalid_symbol",
                "last_updated_utc": now_utc,
                "last_updated_riyadh": now_riy,
            }

        if _KSA_RE.match(sym_norm) and _ksa_blocked_by_default() and not _allow_ksa_override():
            return {
                "symbol": sym_raw,
                "symbol_normalized": sym_norm,
                "provider": PROVIDER_NAME,
                "data_source": PROVIDER_NAME,
                "data_quality": "BLOCKED",
                "error": "ksa_blocked",
                "last_updated_utc": now_utc,
                "last_updated_riyadh": now_riy,
            }

        enable_fund = _env_bool("EODHD_ENABLE_FUNDAMENTALS", True)
        enable_hist = _env_bool("EODHD_ENABLE_HISTORY", True)

        tasks: List[asyncio.Task] = []
        tasks.append(asyncio.create_task(self.fetch_quote(sym_raw)))
        if enable_fund:
            tasks.append(asyncio.create_task(self.fetch_fundamentals(sym_raw)))
        if enable_hist:
            tasks.append(asyncio.create_task(self.fetch_history_stats(sym_raw)))

        results = await asyncio.gather(*tasks, return_exceptions=True)

        merged: Dict[str, Any] = {
            "symbol": sym_raw,
            "symbol_normalized": sym_norm,
            "provider": PROVIDER_NAME,
            "data_source": PROVIDER_NAME,
            "last_updated_utc": now_utc,
            "last_updated_riyadh": now_riy,
        }

        errors: List[str] = []

        for r in results:
            if isinstance(r, Exception):
                errors.append(f"exception:{r.__class__.__name__}")
                continue

            patch, err = r  # type: ignore
            if err:
                errors.append(str(err))
            if isinstance(patch, dict) and patch:
                for k, v in patch.items():
                    if v is None:
                        continue
                    if k not in merged or merged.get(k) in (None, "", [], {}):
                        merged[k] = v

        if merged.get("current_price") is None:
            merged["data_quality"] = "MISSING"
            merged["error"] = "fetch_failed"
            merged["error_detail"] = ",".join(sorted(set(errors))) if errors else "no_data"
        else:
            merged["data_quality"] = "OK"

        # ensure canonical fields
        merged["symbol"] = sym_raw
        merged["symbol_normalized"] = sym_norm

        return _clean_patch(merged)

    async def aclose(self) -> None:
        try:
            await self._client.aclose()
        except Exception:
            pass


# =============================================================================
# Singleton + public API
# =============================================================================
_INSTANCE: Optional[EODHDClient] = None
_INSTANCE_LOCK = asyncio.Lock()

async def get_client() -> EODHDClient:
    global _INSTANCE
    if _INSTANCE is None:
        async with _INSTANCE_LOCK:
            if _INSTANCE is None:
                _INSTANCE = EODHDClient()
    return _INSTANCE

async def fetch_enriched_quote_patch(symbol: str, *args, **kwargs) -> Dict[str, Any]:
    client = await get_client()
    return await client.fetch_enriched_quote_patch(symbol)

__all__ = [
    "fetch_enriched_quote_patch",
    "PROVIDER_NAME",
    "PROVIDER_VERSION",
    "EODHDClient",
    "get_client",
    "normalize_eodhd_symbol",
]
