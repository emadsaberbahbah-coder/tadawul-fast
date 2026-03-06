#!/usr/bin/env python3
# core/providers/eodhd_provider.py
"""
================================================================================
EODHD Provider — v4.4.0 (GLOBAL PRIMARY / HISTORY+FUNDAMENTALS / SCHEMA-ALIGNED)
================================================================================

Goal (Phase D + EODHD Global Primary)
- Global pages should use EODHD for: price + history + fundamentals (where available).
- Only fall back to Yahoo providers when EODHD doesn't have a field.
- International symbols should be supported across all pages (Global_Markets, Market_Leaders, My_Portfolio, etc.)

What this provider guarantees
- ✅ Render-safe (no numpy/pandas/scipy; httpx only)
- ✅ Async: bounded concurrency + token bucket + jitter retries + 429 Retry-After
- ✅ Provider-first normalization using core.symbols.normalize.to_eodhd_symbol when available
- ✅ NEVER returns {} on failure (returns {"error": "..."} patch)
- ✅ Schema-aligned patch keys (fractions for ratio fields where appropriate):
  Identity/Profile:
    name, exchange, currency, country, sector, industry, asset_class
  Prices/Liquidity:
    current_price, previous_close, day_high, day_low, volume, market_cap
    price_change, percent_change (FRACTION), change, change_pct (aliases)
  52W:
    week_52_high, week_52_low, week_52_position_pct (FRACTION)
  Fundamentals/Valuation (best effort):
    pe_ttm, pb_ratio, ps_ratio, peg_ratio, ev_ebitda, enterprise_value
    dividend_yield (FRACTION), payout_ratio (FRACTION), roe/roa/net_margin (FRACTION)
    revenue_growth, earnings_growth (FRACTION where possible)
  Risk/Stats (computed from EODHD history, if enabled):
    volatility_90d (FRACTION annualized), max_drawdown_1y (FRACTION),
    var_95_1d (FRACTION), sharpe_1y (unitless)
    plus: volatility_30d (FRACTION), volatility_365d (FRACTION)

Endpoints used (EODHD API)
- real-time/{SYMBOL}
- fundamentals/{SYMBOL}
- eod/{SYMBOL}?from=YYYY-MM-DD

Env vars
- EODHD_API_KEY (or EODHD_API_TOKEN / EODHD_KEY)
- EODHD_BASE_URL (default: https://eodhistoricaldata.com/api)
- EODHD_DEFAULT_EXCHANGE (default: US)
- EODHD_APPEND_EXCHANGE_SUFFIX (default: true)
- EODHD_RATE_LIMIT_RPS (default: 4.0)
- EODHD_RATE_LIMIT_BURST (default: 8.0)
- EODHD_TIMEOUT_SEC (default: 15)
- EODHD_RETRY_ATTEMPTS (default: 4)
- EODHD_RETRY_BASE_DELAY (default: 0.6)
- EODHD_MAX_CONCURRENCY (default: 20)

Feature flags
- EODHD_ENABLE_FUNDAMENTALS (default: true)
- EODHD_ENABLE_HISTORY (default: true)

History windows
- EODHD_HISTORY_DAYS (default: 420)
- EODHD_HISTORY_WINDOW_52W (default: 252)
- EODHD_STATS_WINDOW_VAR_DAYS (default: 252)
- EODHD_STATS_WINDOW_VOL_DAYS (default: 90)
- EODHD_RISK_FREE_RATE (default: 0.03)   # annual

KSA protection (keep your constraint)
- KSA_DISALLOW_EODHD (default: false)
- ALLOW_EODHD_KSA / EODHD_ALLOW_KSA overrides (default: false)

================================================================================
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
logger.addHandler(logging.NullHandler())

PROVIDER_NAME = "eodhd"
PROVIDER_VERSION = "4.4.0"

DEFAULT_BASE_URL = "https://eodhistoricaldata.com/api"
UA_DEFAULT = "TFB-EODHD/4.4.0 (Render)"

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
    import json  # type: ignore

    def json_loads(data: Union[str, bytes]) -> Any:
        if isinstance(data, bytes):
            data = data.decode("utf-8", errors="replace")
        return json.loads(data)

    _HAS_ORJSON = False

_TRUTHY = {"1", "true", "yes", "y", "on", "t", "enabled", "enable"}
_FALSY = {"0", "false", "no", "n", "off", "f", "disabled", "disable"}

# Accept plain US tickers and dash-share-class (BRK-B).
_US_LIKE_RE = re.compile(r"^[A-Z0-9][A-Z0-9\-_]{0,16}$")
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
    return {k: v for k, v in (p or {}).items() if v is not None and v != ""}


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


def _as_pct_points_from_frac(v: Any) -> Optional[float]:
    """
    For sources that might return fractions, convert to percent-points if small.
    (We generally prefer FRACTIONS in canonical fields; this is for compatibility only.)
    """
    f = safe_float(v)
    if f is None:
        return None
    return f * 100.0 if abs(f) <= 1.0 else f


# =============================================================================
# Symbol normalization (GLOBAL, incl. international)
# =============================================================================
def normalize_eodhd_symbol(symbol: str) -> str:
    """
    Normalize a symbol for EODHD.

    Preferred:
      - core.symbols.normalize.to_eodhd_symbol(symbol, default_exchange=...)
        (handles share-class: BRK.B -> BRK.B.US; international tickers if your normalizer supports them)
    Fallback:
      - keep KSA (.SR) as-is (blocked by default anyway)
      - keep already-qualified symbols with suffix >=2 (e.g., AAPL.US, BMW.XETRA)
      - treat dot-share-class (BRK.B) as unqualified -> append default exchange when enabled
      - for plain US-like tickers -> append .{default_exchange} when enabled
      - do not touch special notations (FX/Index/Futures): contains '=', '^', '/'
    """
    s = (symbol or "").strip()
    if not s:
        return ""
    s_up = s.upper()

    # Best: project normalizer (safe if exists)
    try:
        from core.symbols.normalize import to_eodhd_symbol as _to_eodhd_symbol  # type: ignore

        if callable(_to_eodhd_symbol):
            out = _to_eodhd_symbol(s_up, default_exchange=_default_exchange())  # type: ignore[arg-type]
            if isinstance(out, str) and out.strip():
                return out.strip().upper()
    except Exception:
        pass

    # Do not touch special notations
    if "=" in s_up or "^" in s_up or "/" in s_up:
        return s_up

    # KSA canonical
    if s_up.endswith(".SR") and _KSA_RE.match(s_up):
        return s_up

    # Already qualified? (has dot + suffix len>=2)
    if "." in s_up:
        base, suf = s_up.rsplit(".", 1)
        if len(suf) >= 2:
            return s_up
        # len == 1: share-class (e.g. BRK.B) -> append exchange suffix if enabled
        if not _append_exchange_suffix():
            return s_up
        return f"{s_up}.{_default_exchange()}"

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
# Stats calculations from close series (no numpy)
# =============================================================================
def _daily_returns(closes: List[float]) -> List[float]:
    rets: List[float] = []
    for i in range(1, len(closes)):
        p0 = closes[i - 1]
        p1 = closes[i]
        if p0 and p0 > 0 and p1 and p1 > 0:
            rets.append((p1 / p0) - 1.0)
    return rets


def _stdev(x: List[float]) -> Optional[float]:
    if len(x) < 2:
        return None
    m = sum(x) / len(x)
    var = sum((v - m) ** 2 for v in x) / max(1, len(x) - 1)
    return math.sqrt(max(0.0, var))


def _max_drawdown(closes: List[float]) -> Optional[float]:
    if len(closes) < 2:
        return None
    peak = closes[0]
    mdd = 0.0
    for p in closes:
        if p > peak:
            peak = p
        if peak > 0:
            dd = (p / peak) - 1.0
            if dd < mdd:
                mdd = dd
    return mdd  # negative fraction


def _var_95_1d(returns: List[float]) -> Optional[float]:
    """
    Historical VaR at 95% for 1-day returns (fraction, positive number for loss threshold).
    We return abs(5th percentile) if negative.
    """
    if len(returns) < 20:
        return None
    xs = sorted(returns)
    idx = int(round(0.05 * (len(xs) - 1)))
    q = xs[max(0, min(len(xs) - 1, idx))]
    # VaR is a loss metric; if q is -0.02 => var=0.02
    return abs(q) if q < 0 else 0.0


def _sharpe_1y(returns: List[float], rf_annual: float) -> Optional[float]:
    if len(returns) < 60:
        return None
    mu = sum(returns) / len(returns)
    sd = _stdev(returns)
    if sd is None or sd == 0:
        return None
    rf_daily = float(rf_annual) / 252.0
    ex = mu - rf_daily
    return (ex / sd) * math.sqrt(252.0)


def _annualized_vol(returns: List[float]) -> Optional[float]:
    sd = _stdev(returns)
    if sd is None:
        return None
    return sd * math.sqrt(252.0)


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

    async def _request_json(
        self,
        endpoint: str,
        params: Optional[Dict[str, Any]] = None,
    ) -> Tuple[Optional[Any], Optional[str]]:
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

                    if sc == 429:
                        ra = r.headers.get("Retry-After")
                        wait = float(ra) if ra and ra.replace(".", "", 1).isdigit() else min(15.0, 1.0 + attempt)
                        last_err = "HTTP 429"
                        await asyncio.sleep(wait)
                        continue

                    if sc in (401, 403):
                        body_hint = ""
                        try:
                            body_hint = (r.text or "")[:160]
                        except Exception:
                            body_hint = ""
                        return None, f"HTTP {sc} auth_error {body_hint}".strip()

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
        sym_raw = (symbol or "").strip().upper()
        sym = normalize_eodhd_symbol(sym_raw)
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

            # percent_change as FRACTION
            change_frac = None
            if close is not None and prev not in (None, 0.0):
                change_frac = (close / prev) - 1.0
            else:
                change_frac = _frac_from_percentish(data.get("change_p"))

            patch = _clean_patch(
                {
                    "symbol": sym_raw,
                    "symbol_normalized": sym,
                    "provider": PROVIDER_NAME,
                    "data_source": PROVIDER_NAME,

                    "current_price": close,
                    "previous_close": prev,

                    "price_change": chg if chg is not None else (close - prev if close is not None and prev is not None else None),
                    "percent_change": change_frac,  # FRACTION

                    # legacy aliases
                    "change": chg if chg is not None else (close - prev if close is not None and prev is not None else None),
                    "change_pct": change_frac,  # FRACTION

                    "day_open": safe_float(data.get("open")),
                    "day_high": safe_float(data.get("high")),
                    "day_low": safe_float(data.get("low")),
                    "volume": safe_float(data.get("volume")),

                    "currency": safe_str(data.get("currency")),

                    "last_updated_utc": _utc_iso(),
                    "last_updated_riyadh": _riyadh_iso(),
                }
            )
            await self.quote_cache.set(ck, patch)
            return patch, None

        return await self._sf.do(ck, _do)

    # ---------------------------------------------------------------------
    # Fundamentals (and classification)
    # ---------------------------------------------------------------------
    async def fetch_fundamentals(self, symbol: str) -> Tuple[Dict[str, Any], Optional[str]]:
        sym_raw = (symbol or "").strip().upper()
        sym = normalize_eodhd_symbol(sym_raw)
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
            tech = data.get("Technicals") or {}

            currency = safe_str(general.get("CurrencyCode")) or safe_str(general.get("Currency")) or safe_str(highlights.get("Currency")) or None

            # EODHD sometimes returns "CountryName" and "CountryISO"
            country = safe_str(general.get("CountryISO")) or safe_str(general.get("CountryName")) or None
            # Some symbols may not have an ISO; keep name if only that exists
            if country and len(country) > 3:
                # attempt to keep name as-is; engine can normalize if desired
                pass

            patch = _clean_patch(
                {
                    "symbol": sym_raw,
                    "symbol_normalized": sym,
                    "provider": PROVIDER_NAME,
                    "data_source": PROVIDER_NAME,

                    # identity/classification
                    "name": safe_str(general.get("Name")) or safe_str(general.get("ShortName")) or safe_str(general.get("LongName")),
                    "exchange": safe_str(general.get("Exchange")) or safe_str(general.get("PrimaryExchange")),
                    "currency": currency,
                    "country": country,
                    "sector": safe_str(general.get("Sector")),
                    "industry": safe_str(general.get("Industry")),

                    # profile & cap
                    "market_cap": safe_float(highlights.get("MarketCapitalization")) or safe_float(general.get("MarketCapitalization")),
                    "enterprise_value": safe_float(highlights.get("EnterpriseValue")) or safe_float(valuation.get("EnterpriseValue")),
                    "shares_outstanding": safe_float(shares.get("SharesOutstanding")),

                    # valuation/fundamentals (schema-aligned names)
                    "eps_ttm": safe_float(highlights.get("EarningsShare")) or safe_float(highlights.get("DilutedEpsTTM")),
                    "pe_ttm": safe_float(valuation.get("TrailingPE")) or safe_float(highlights.get("PERatio")),
                    "forward_pe": safe_float(valuation.get("ForwardPE")),
                    "pb_ratio": safe_float(valuation.get("PriceBookMRQ")) or safe_float(valuation.get("PriceBook")),
                    "ps_ratio": safe_float(valuation.get("PriceSalesTTM")) or safe_float(valuation.get("PriceSales")),
                    "peg_ratio": safe_float(valuation.get("PEGRatio")) or safe_float(valuation.get("PegRatio")),
                    "ev_ebitda": safe_float(valuation.get("EnterpriseValueEbitda")) or safe_float(valuation.get("EnterpriseValueEBITDA")),

                    # keep legacy aliases used by some scorers
                    "pb": safe_float(valuation.get("PriceBookMRQ")) or safe_float(valuation.get("PriceBook")),
                    "ps": safe_float(valuation.get("PriceSalesTTM")) or safe_float(valuation.get("PriceSales")),
                    "peg": safe_float(valuation.get("PEGRatio")) or safe_float(valuation.get("PegRatio")),

                    # ratio fields as FRACTIONS
                    "dividend_yield": _frac_from_percentish(highlights.get("DividendYield")),
                    "payout_ratio": _frac_from_percentish(highlights.get("PayoutRatio")),
                    "roe": _frac_from_percentish(highlights.get("ROE")),
                    "roa": _frac_from_percentish(highlights.get("ROA")),
                    "net_margin": _frac_from_percentish(highlights.get("ProfitMargin")),

                    # growth (often percent-points; normalize to fraction)
                    "revenue_growth": _frac_from_percentish(highlights.get("RevenueGrowth")),
                    "earnings_growth": _frac_from_percentish(highlights.get("EarningsGrowth")),

                    # beta (if available)
                    "beta": safe_float(tech.get("Beta")) or safe_float(highlights.get("Beta")),

                    "last_updated_utc": _utc_iso(),
                    "last_updated_riyadh": _riyadh_iso(),
                }
            )
            await self.fund_cache.set(ck, patch)
            return patch, None

        return await self._sf.do(ck, _do)

    # ---------------------------------------------------------------------
    # History stats + risk fields (computed)
    # ---------------------------------------------------------------------
    async def fetch_history_stats(self, symbol: str) -> Tuple[Dict[str, Any], Optional[str]]:
        sym_raw = (symbol or "").strip().upper()
        sym = normalize_eodhd_symbol(sym_raw)
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
                patch = _clean_patch(
                    {
                        "symbol": sym_raw,
                        "symbol_normalized": sym,
                        "provider": PROVIDER_NAME,
                        "data_source": PROVIDER_NAME,
                        "history_points": n,
                        "history_source": PROVIDER_NAME,
                        "history_last_utc": last_hist_dt,
                        "last_updated_utc": _utc_iso(),
                        "last_updated_riyadh": _riyadh_iso(),
                    }
                )
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

            # returns series for stats
            rets_all = _daily_returns(closes)
            rets_1y = rets_all[-min(len(rets_all), 252):] if rets_all else []
            rets_90 = rets_all[-min(len(rets_all), 90):] if rets_all else []
            rets_30 = rets_all[-min(len(rets_all), 30):] if rets_all else []

            vol_30 = _annualized_vol(rets_30)  # FRACTION annualized
            vol_90 = _annualized_vol(rets_90)  # FRACTION annualized
            vol_1y = _annualized_vol(rets_1y)   # FRACTION annualized

            mdd_1y = _max_drawdown(closes[-min(len(closes), 252):])  # negative fraction
            var95 = _var_95_1d(rets_1y)

            rf = _env_float("EODHD_RISK_FREE_RATE", 0.03, lo=0.0, hi=0.20)
            sharpe = _sharpe_1y(rets_1y, rf_annual=rf)

            # simple returns (fractions)
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
                    "symbol": sym_raw,
                    "symbol_normalized": sym,
                    "provider": PROVIDER_NAME,
                    "data_source": PROVIDER_NAME,

                    # 52W
                    "week_52_low": low_52,
                    "week_52_high": high_52,
                    "week_52_position_pct": pos_52_frac,  # FRACTION 0..1

                    # liquidity/tech
                    "avg_vol_30d": avg_vol_30,
                    "avg_volume_30d": avg_vol_30,  # alias

                    # volatility/risk (fractions)
                    "volatility_30d": vol_30,
                    "volatility_90d": vol_90,
                    "volatility_365d": vol_1y,

                    "max_drawdown_1y": mdd_1y,   # negative fraction
                    "var_95_1d": var95,          # positive fraction loss threshold
                    "sharpe_1y": sharpe,

                    # returns
                    "returns_1w": returns_1w,
                    "returns_1m": returns_1m,
                    "returns_3m": returns_3m,
                    "returns_6m": returns_6m,
                    "returns_12m": returns_12m,

                    "history_points": n,
                    "history_source": PROVIDER_NAME,
                    "history_last_utc": last_hist_dt,
                    "last_updated_utc": _utc_iso(),
                    "last_updated_riyadh": _riyadh_iso(),
                }
            )

            # legacy aliases (optional)
            if "week_52_high" in patch:
                patch["52w_high"] = patch.get("week_52_high")
            if "week_52_low" in patch:
                patch["52w_low"] = patch.get("week_52_low")
            if "week_52_position_pct" in patch:
                patch["position_52w_pct"] = patch.get("week_52_position_pct")

            await self.hist_cache.set(ck, patch)
            return patch, None

        return await self._sf.do(ck, _do)

    # ---------------------------------------------------------------------
    # Public: enriched patch
    # ---------------------------------------------------------------------
    async def fetch_enriched_quote_patch(self, symbol: str) -> Dict[str, Any]:
        """
        Returns a dict patch to merge into the unified enriched quote model.
        Never throws; never returns {}.
        """
        now_utc = _utc_iso()
        now_riy = _riyadh_iso()

        sym_raw = (symbol or "").strip().upper()
        sym_norm = normalize_eodhd_symbol(sym_raw)

        if not sym_norm:
            return _clean_patch(
                {
                    "symbol": sym_raw,
                    "symbol_normalized": "",
                    "provider": PROVIDER_NAME,
                    "data_source": PROVIDER_NAME,
                    "data_quality": "MISSING",
                    "error": "invalid_symbol",
                    "error_detail": "normalize_eodhd_symbol returned empty",
                    "last_updated_utc": now_utc,
                    "last_updated_riyadh": now_riy,
                }
            )

        if not self.api_key:
            return _clean_patch(
                {
                    "symbol": sym_raw,
                    "symbol_normalized": sym_norm,
                    "provider": PROVIDER_NAME,
                    "data_source": PROVIDER_NAME,
                    "data_quality": "MISSING",
                    "error": "missing_api_key",
                    "error_detail": "EODHD_API_KEY (or EODHD_API_TOKEN/EODHD_KEY) is not set",
                    "last_updated_utc": now_utc,
                    "last_updated_riyadh": now_riy,
                }
            )

        if _KSA_RE.match(sym_norm) and _ksa_blocked_by_default() and not _allow_ksa_override():
            return _clean_patch(
                {
                    "symbol": sym_raw,
                    "symbol_normalized": sym_norm,
                    "provider": PROVIDER_NAME,
                    "data_source": PROVIDER_NAME,
                    "data_quality": "BLOCKED",
                    "error": "ksa_blocked",
                    "error_detail": "KSA_DISALLOW_EODHD=true (override with ALLOW_EODHD_KSA=1)",
                    "last_updated_utc": now_utc,
                    "last_updated_riyadh": now_riy,
                }
            )

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
            "data_sources": [PROVIDER_NAME],
            "last_updated_utc": now_utc,
            "last_updated_riyadh": now_riy,
        }

        errors: List[str] = []

        for r in results:
            if isinstance(r, Exception):
                errors.append(f"exception:{r.__class__.__name__}")
                continue
            try:
                patch, err = r  # type: ignore[misc]
            except Exception:
                errors.append("bad_task_result")
                continue

            if err:
                errors.append(str(err))

            if isinstance(patch, dict) and patch:
                for k, v in patch.items():
                    if v is None:
                        continue
                    if k not in merged or merged.get(k) in (None, "", [], {}):
                        merged[k] = v

        # Ensure core coherency
        if merged.get("current_price") is None and merged.get("price") is not None:
            merged["current_price"] = merged.get("price")
        if merged.get("price") is None and merged.get("current_price") is not None:
            merged["price"] = merged.get("current_price")

        if merged.get("previous_close") is None and merged.get("prev_close") is not None:
            merged["previous_close"] = merged.get("prev_close")
        if merged.get("prev_close") is None and merged.get("previous_close") is not None:
            merged["prev_close"] = merged.get("previous_close")

        # Determine quality
        if merged.get("current_price") is None:
            merged["data_quality"] = "MISSING"
            merged["error"] = "fetch_failed"
            merged["error_detail"] = ",".join(sorted(set(errors))) if errors else "no_data"
        else:
            merged["data_quality"] = "OK"
            if errors:
                merged["warning"] = "partial_sources"
                merged["info"] = {"warnings": sorted(set(errors))[:6]}

        # Backward-compatible aliases
        merged["change"] = merged.get("price_change")
        merged["change_pct"] = merged.get("percent_change")
        merged["52w_high"] = merged.get("week_52_high")
        merged["52w_low"] = merged.get("week_52_low")

        return _clean_patch(merged)

    async def aclose(self) -> None:
        try:
            await self._client.aclose()
        except Exception:
            pass


# =============================================================================
# Singleton + public API + compatibility wrappers
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


async def fetch_enriched_quote_patch(symbol: str, *args: Any, **kwargs: Any) -> Dict[str, Any]:
    client = await get_client()
    return await client.fetch_enriched_quote_patch(symbol)


# Many engines discover provider capabilities by searching common method names.
async def quote(symbol: str, *args: Any, **kwargs: Any) -> Dict[str, Any]:
    client = await get_client()
    patch, err = await client.fetch_quote(symbol)
    if err:
        raise RuntimeError(err)
    return patch


async def get_quote(symbol: str, *args: Any, **kwargs: Any) -> Dict[str, Any]:
    return await quote(symbol, *args, **kwargs)


async def fetch_quote(symbol: str, *args: Any, **kwargs: Any) -> Dict[str, Any]:
    return await quote(symbol, *args, **kwargs)


async def enriched_quote(symbol: str, *args: Any, **kwargs: Any) -> Dict[str, Any]:
    return await fetch_enriched_quote_patch(symbol)


async def fetch_quotes(symbols: List[str], *args: Any, **kwargs: Any) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    for s in symbols or []:
        try:
            out.append(await quote(s))
        except Exception:
            continue
    return out


__all__ = [
    "fetch_enriched_quote_patch",
    "quote",
    "get_quote",
    "fetch_quote",
    "enriched_quote",
    "fetch_quotes",
    "PROVIDER_NAME",
    "PROVIDER_VERSION",
    "EODHDClient",
    "get_client",
    "normalize_eodhd_symbol",
]
