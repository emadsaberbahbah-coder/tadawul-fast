#!/usr/bin/env python3
# core/providers/finnhub_provider.py
"""
================================================================================
Finnhub Provider — v4.2.0 (GLOBAL-STABLE / RENDER-SAFE / RATIO-SAFE)
================================================================================

Why this revision:
- ✅ Render-safe: NO hard dependency on numpy/scipy/sklearn/xgboost/statsmodels (all removed).
- ✅ Ratio-safe outputs for Sheets:
     - percent-like fields are returned as FRACTIONS (0.0079 == 0.79%)
     - prevents “9877% / 5,266,038%” explosions when the Sheet column is formatted as PERCENT
- ✅ Strong network reliability:
     - bounded concurrency + token-bucket rate limiting
     - retries with Full-Jitter exponential backoff
     - proper 429 Retry-After handling
     - circuit breaker to protect the service
- ✅ Consistent enriched patch shape:
     current_price, previous_close, change, change_pct (fraction),
     day_open, day_high, day_low, volume,
     plus profile/metrics/history enrichments (optional via env)

Supported Finnhub endpoints used:
- /quote
- /stock/profile2
- /stock/metric?metric=all   (optional)
- /stock/candle              (optional)

Key env vars:
- FINNHUB_API_KEY (or FINNHUB_API_TOKEN / FINNHUB_TOKEN)
- FINNHUB_BASE_URL (default: https://finnhub.io/api/v1)
- FINNHUB_TIMEOUT_SEC (default: 15)
- FINNHUB_RETRY_ATTEMPTS (default: 4)
- FINNHUB_MAX_CONCURRENCY (default: 25)
- FINNHUB_RATE_LIMIT_RPS (default: 20)
- FINNHUB_RATE_LIMIT_BURST (default: 40)
- FINNHUB_CB_THRESHOLD (default: 6)
- FINNHUB_CB_COOLDOWN_SEC (default: 45)

Feature toggles:
- FINNHUB_ENABLE_PROFILE (default: true)
- FINNHUB_ENABLE_METRIC (default: true)     # valuation/fundamentals
- FINNHUB_ENABLE_HISTORY (default: true)    # 52W, MA, RSI, returns, vol

Cache TTLs:
- FINNHUB_QUOTE_TTL_SEC (default: 10)
- FINNHUB_PROFILE_TTL_SEC (default: 21600)
- FINNHUB_METRIC_TTL_SEC (default: 21600)
- FINNHUB_HISTORY_TTL_SEC (default: 1800)

Symbol rules:
- Blocks KSA (Tadawul) by default
- Blocks Yahoo special symbols (^GSPC, EURUSD=X, GC=F, etc.)
- Strips ".US" suffix automatically (engine may send MPLX.US)
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

logger = logging.getLogger("core.providers.finnhub_provider")

PROVIDER_NAME = "finnhub"
PROVIDER_VERSION = "4.2.0"
DEFAULT_BASE_URL = "https://finnhub.io/api/v1"

_TRUTHY = {"1", "true", "yes", "y", "on", "t"}
_FALSY = {"0", "false", "no", "n", "off", "f"}

# KSA + special symbol blocking
_KSA_CODE_RE = re.compile(r"^\d{3,6}(\.SR)?$", re.IGNORECASE)
_SPECIAL_SYMBOL_RE = re.compile(r"(\^|=|/|:)", re.IGNORECASE)

# --------------------------------------------------------------------------------------
# Optional fast JSON
# --------------------------------------------------------------------------------------
try:
    import orjson  # type: ignore

    def json_loads(data: Union[str, bytes]) -> Any:
        if isinstance(data, str):
            data = data.encode("utf-8")
        return orjson.loads(data)

except Exception:
    import json  # type: ignore

    def json_loads(data: Union[str, bytes]) -> Any:
        if isinstance(data, bytes):
            data = data.decode("utf-8", errors="replace")
        return json.loads(data)


# =============================================================================
# Env helpers
# =============================================================================
def _env_str(name: str, default: str = "") -> str:
    return (os.getenv(name) or default).strip()


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


def _env_bool(name: str, default: bool = False) -> bool:
    raw = (os.getenv(name) or "").strip().lower()
    if not raw:
        return default
    if raw in _TRUTHY:
        return True
    if raw in _FALSY:
        return False
    return default


def _token() -> Optional[str]:
    for k in ("FINNHUB_API_KEY", "FINNHUB_API_TOKEN", "FINNHUB_TOKEN"):
        v = (os.getenv(k) or "").strip()
        if v:
            return v
    return None


def _base_url() -> str:
    return _env_str("FINNHUB_BASE_URL", DEFAULT_BASE_URL).rstrip("/")


def _utc_iso(dt: Optional[datetime] = None) -> str:
    d = dt or datetime.now(timezone.utc)
    if d.tzinfo is None:
        d = d.replace(tzinfo=timezone.utc)
    return d.astimezone(timezone.utc).isoformat()


def _riyadh_iso(dt: Optional[datetime] = None) -> str:
    tz = timezone(timedelta(hours=3))
    d = dt or datetime.now(timezone.utc)
    if d.tzinfo is None:
        d = d.replace(tzinfo=timezone.utc)
    return d.astimezone(tz).isoformat()


# =============================================================================
# Safe coercion + ratio-safe percent normalization
# =============================================================================
def safe_float(x: Any) -> Optional[float]:
    if x is None:
        return None
    try:
        if isinstance(x, (int, float)) and not isinstance(x, bool):
            f = float(x)
        else:
            s = str(x).strip().replace(",", "")
            if not s:
                return None
            if s.endswith("%"):
                # treat percent string as percent-points -> fraction
                f = float(s[:-1].strip()) / 100.0
            else:
                f = float(s)
        if math.isnan(f) or math.isinf(f):
            return None
        return f
    except Exception:
        return None


def safe_int(x: Any) -> Optional[int]:
    f = safe_float(x)
    return int(round(f)) if f is not None else None


def safe_str(x: Any) -> Optional[str]:
    if x is None:
        return None
    try:
        s = str(x).strip()
        return s if s else None
    except Exception:
        return None


def _pct_points_to_fraction(x: Any) -> Optional[float]:
    """
    Finnhub quote 'dp' is percent-points (e.g., 0.79 means 0.79%).
    Convert to fraction for Sheets percent formatting.
    """
    f = safe_float(x)
    if f is None:
        return None
    # assume percent-points => fraction
    return f / 100.0


def _percentish_to_fraction(x: Any) -> Optional[float]:
    """
    Generic percent-ish normalization:
    - 0.12 => 0.12 (already fraction)
    - 12   => 0.12 (percent-points)
    - "12%" => 0.12
    """
    f = safe_float(x)
    if f is None:
        return None
    if abs(f) > 1.5:
        return f / 100.0
    return f


def _clean_patch(p: Dict[str, Any]) -> Dict[str, Any]:
    return {k: v for k, v in (p or {}).items() if v is not None and not (isinstance(v, str) and not v.strip())}


# =============================================================================
# Symbol handling
# =============================================================================
def looks_like_ksa(symbol: str) -> bool:
    s = (symbol or "").strip().upper()
    if not s:
        return False
    # prefer shared helper if present
    try:
        from core.symbols.normalize import looks_like_ksa as _lk  # type: ignore

        if callable(_lk):
            return bool(_lk(s))
    except Exception:
        pass

    if s.startswith("TADAWUL:"):
        return True
    if s.endswith(".SR"):
        return True
    if _KSA_CODE_RE.match(s):
        return True
    return False


def is_blocked_special(symbol: str) -> bool:
    s = (symbol or "").strip().upper()
    if not s:
        return True
    return bool(_SPECIAL_SYMBOL_RE.search(s))


def normalize_finnhub_symbol(symbol: str) -> str:
    """
    Finnhub usually expects:
      - US ticker: AAPL
      - Some globals: 7203.T, VOD.L, AIR.PA, etc.
    Your engine may send AAPL.US -> strip .US.
    """
    s = (symbol or "").strip().upper()
    if not s:
        return ""

    # prefer shared formatter if available
    try:
        from core.symbols.normalize import to_finnhub_symbol as _to  # type: ignore

        if callable(_to):
            out = _to(s)
            if isinstance(out, str) and out.strip():
                s = out.strip().upper()
    except Exception:
        pass

    if s.endswith(".US"):
        s = s[:-3]
    return s


def symbol_variants(symbol: str) -> List[str]:
    """
    Try a few safe variants; do NOT go wild (quota + latency).
    """
    s = normalize_finnhub_symbol(symbol)
    if not s:
        return []
    out = [s]

    # If something came as BRK-B or BRK.B, try both (some APIs differ)
    if "-" in s and "." not in s:
        out.append(s.replace("-", "."))
    if "." in s:
        base = s.split(".", 1)[0]
        if base and base not in out:
            out.append(base)

    # de-dupe
    dedup: List[str] = []
    seen = set()
    for x in out:
        if x and x not in seen:
            seen.add(x)
            dedup.append(x)
    return dedup


# =============================================================================
# Async primitives: token bucket + circuit breaker + TTL cache + singleflight
# =============================================================================
class TokenBucket:
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


@dataclass
class _CacheItem:
    exp: float
    val: Any


class TTLCache:
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


class SingleFlight:
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


class CircuitBreaker:
    def __init__(self, threshold: int, cooldown_sec: float):
        self.threshold = max(1, int(threshold))
        self.cooldown_sec = max(1.0, float(cooldown_sec))
        self.failures = 0
        self.open_until = 0.0
        self._lock = asyncio.Lock()

    async def allow(self) -> bool:
        async with self._lock:
            now = time.monotonic()
            if now < self.open_until:
                return False
            return True

    async def on_success(self) -> None:
        async with self._lock:
            self.failures = 0
            self.open_until = 0.0

    async def on_failure(self) -> None:
        async with self._lock:
            self.failures += 1
            if self.failures >= self.threshold:
                self.open_until = time.monotonic() + self.cooldown_sec


# =============================================================================
# Technicals (pure python)
# =============================================================================
def _sma(values: List[float], window: int) -> Optional[float]:
    if window <= 0 or len(values) < window:
        return None
    return sum(values[-window:]) / float(window)


def _returns(values: List[float], days: int) -> Optional[float]:
    # fraction return
    if len(values) <= days:
        return None
    base = values[-1 - days]
    if not base:
        return None
    return (values[-1] / base) - 1.0


def _volatility_30d_annualized_fraction(closes: List[float]) -> Optional[float]:
    if len(closes) < 31:
        return None
    rets: List[float] = []
    for i in range(len(closes) - 30, len(closes)):
        if i <= 0:
            continue
        prev = closes[i - 1]
        cur = closes[i]
        if prev:
            rets.append((cur / prev) - 1.0)
    if len(rets) < 5:
        return None
    m = sum(rets) / len(rets)
    var = sum((x - m) ** 2 for x in rets) / max(1, (len(rets) - 1))
    return math.sqrt(var) * math.sqrt(252.0)


def _rsi_14(closes: List[float]) -> Optional[float]:
    if len(closes) < 15:
        return None
    gains = 0.0
    losses = 0.0
    for i in range(len(closes) - 14, len(closes)):
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


# =============================================================================
# Finnhub client
# =============================================================================
class FinnhubClient:
    def __init__(self) -> None:
        self.api_key = _token()
        self.base_url = _base_url()

        self.timeout_sec = _env_float("FINNHUB_TIMEOUT_SEC", 15.0, lo=3.0, hi=120.0)
        self.retry_attempts = _env_int("FINNHUB_RETRY_ATTEMPTS", 4, lo=0, hi=10)
        self.retry_base_delay = _env_float("FINNHUB_RETRY_BASE_DELAY", 0.6, lo=0.0, hi=10.0)

        self.max_concurrency = _env_int("FINNHUB_MAX_CONCURRENCY", 25, lo=1, hi=100)
        self._sem = asyncio.Semaphore(self.max_concurrency)

        rps = _env_float("FINNHUB_RATE_LIMIT_RPS", 20.0, lo=0.0, hi=200.0)
        burst = _env_float("FINNHUB_RATE_LIMIT_BURST", 40.0, lo=1.0, hi=500.0)
        self._bucket = TokenBucket(rate_per_sec=rps, burst=burst)

        cb_th = _env_int("FINNHUB_CB_THRESHOLD", 6, lo=1, hi=100)
        cb_cd = _env_float("FINNHUB_CB_COOLDOWN_SEC", 45.0, lo=1.0, hi=600.0)
        self._cb = CircuitBreaker(threshold=cb_th, cooldown_sec=cb_cd)

        self._sf = SingleFlight()

        self.quote_cache = TTLCache(maxsize=8000, ttl_sec=_env_float("FINNHUB_QUOTE_TTL_SEC", 10.0, lo=1.0, hi=600.0))
        self.profile_cache = TTLCache(maxsize=4000, ttl_sec=_env_float("FINNHUB_PROFILE_TTL_SEC", 21600.0, lo=60.0, hi=86400.0))
        self.metric_cache = TTLCache(maxsize=4000, ttl_sec=_env_float("FINNHUB_METRIC_TTL_SEC", 21600.0, lo=60.0, hi=86400.0))
        self.history_cache = TTLCache(maxsize=2500, ttl_sec=_env_float("FINNHUB_HISTORY_TTL_SEC", 1800.0, lo=60.0, hi=86400.0))

        self._client = httpx.AsyncClient(
            timeout=httpx.Timeout(self.timeout_sec),
            headers={"User-Agent": _env_str("FINNHUB_USER_AGENT", "TFB-Finnhub/4.2.0")},
            limits=httpx.Limits(max_keepalive_connections=30, max_connections=60),
            follow_redirects=True,
            http2=True,
        )

    async def _request_json(self, endpoint: str, params: Optional[Dict[str, Any]] = None) -> Tuple[Optional[Any], Optional[str]]:
        if not self.api_key:
            return None, "FINNHUB_API_KEY missing"

        if not await self._cb.allow():
            return None, "circuit_open"

        url = f"{self.base_url}/{endpoint.lstrip('/')}"
        p = dict(params or {})
        p["token"] = self.api_key

        last_err: Optional[str] = None

        async with self._sem:
            for attempt in range(max(1, self.retry_attempts + 1)):
                await self._bucket.wait(1.0)

                try:
                    r = await self._client.get(url, params=p)
                    sc = int(r.status_code)

                    # rate limit
                    if sc == 429:
                        ra = r.headers.get("Retry-After")
                        wait = float(ra) if ra and ra.isdigit() else min(15.0, 1.0 + attempt)
                        last_err = "HTTP 429"
                        await self._cb.on_failure()
                        await asyncio.sleep(wait)
                        continue

                    if 500 <= sc < 600:
                        last_err = f"HTTP {sc}"
                        await self._cb.on_failure()
                        base = self.retry_base_delay * (2 ** (attempt - 1))
                        await asyncio.sleep(min(12.0, random.uniform(0, base + 0.25)))
                        continue

                    if sc == 404:
                        # not found is not retryable
                        await self._cb.on_failure()
                        return None, "HTTP 404 not_found"

                    if sc >= 400:
                        await self._cb.on_failure()
                        return None, f"HTTP {sc}"

                    try:
                        data = json_loads(r.content)
                    except Exception:
                        await self._cb.on_failure()
                        return None, "invalid_json"

                    await self._cb.on_success()
                    return data, None

                except httpx.RequestError as e:
                    last_err = f"network_error:{e.__class__.__name__}"
                    await self._cb.on_failure()
                    base = self.retry_base_delay * (2 ** (attempt - 1))
                    await asyncio.sleep(min(12.0, random.uniform(0, base + 0.25)))
                except Exception as e:
                    last_err = f"unexpected_error:{e.__class__.__name__}"
                    await self._cb.on_failure()
                    break

        return None, last_err or "request_failed"

    async def fetch_quote(self, symbol: str) -> Tuple[Dict[str, Any], Optional[str]]:
        sym = normalize_finnhub_symbol(symbol)
        if not sym:
            return {}, "invalid_symbol"

        ck = f"q:{sym}"
        cached = await self.quote_cache.get(ck)
        if cached:
            return cached, None

        async def _do():
            data, err = await self._request_json("quote", params={"symbol": sym})
            if err or not isinstance(data, dict):
                return {}, err or "bad_payload"

            # Finnhub keys: c=current, pc=prev close, d=change, dp=percent change (percent-points)
            cur = safe_float(data.get("c"))
            prev = safe_float(data.get("pc"))
            chg = safe_float(data.get("d"))
            chg_frac = _pct_points_to_fraction(data.get("dp"))

            patch = _clean_patch(
                {
                    "provider": PROVIDER_NAME,
                    "data_source": PROVIDER_NAME,
                    "current_price": cur,
                    "previous_close": prev,
                    "change": chg if chg is not None else (cur - prev if cur is not None and prev is not None else None),
                    "change_pct": chg_frac,  # FRACTION for Sheets percent format
                    "day_open": safe_float(data.get("o")),
                    "day_high": safe_float(data.get("h")),
                    "day_low": safe_float(data.get("l")),
                    "volume": safe_float(data.get("v")),
                    "last_updated_utc": _utc_iso(),
                    "last_updated_riyadh": _riyadh_iso(),
                }
            )
            await self.quote_cache.set(ck, patch)
            return patch, None

        return await self._sf.do(ck, _do)

    async def fetch_profile(self, symbol: str) -> Tuple[Dict[str, Any], Optional[str]]:
        sym = normalize_finnhub_symbol(symbol)
        if not sym:
            return {}, "invalid_symbol"

        ck = f"p:{sym}"
        cached = await self.profile_cache.get(ck)
        if cached:
            return cached, None

        async def _do():
            data, err = await self._request_json("stock/profile2", params={"symbol": sym})
            if err or not isinstance(data, dict):
                return {}, err or "bad_payload"
            if not data:
                return {}, "empty_profile"

            patch = _clean_patch(
                {
                    "provider": PROVIDER_NAME,
                    "data_source": PROVIDER_NAME,
                    "name": safe_str(data.get("name")),
                    "currency": safe_str(data.get("currency")),
                    "exchange": safe_str(data.get("exchange")),
                    "country": safe_str(data.get("country")),
                    "listing_date": safe_str(data.get("ipo")),
                    "market_cap": safe_float(data.get("marketCapitalization")),
                    "shares_outstanding": safe_float(data.get("shareOutstanding")),
                    "last_updated_utc": _utc_iso(),
                    "last_updated_riyadh": _riyadh_iso(),
                }
            )
            await self.profile_cache.set(ck, patch)
            return patch, None

        return await self._sf.do(ck, _do)

    async def fetch_metrics(self, symbol: str) -> Tuple[Dict[str, Any], Optional[str]]:
        sym = normalize_finnhub_symbol(symbol)
        if not sym:
            return {}, "invalid_symbol"

        ck = f"m:{sym}"
        cached = await self.metric_cache.get(ck)
        if cached:
            return cached, None

        async def _do():
            data, err = await self._request_json("stock/metric", params={"symbol": sym, "metric": "all"})
            if err or not isinstance(data, dict):
                return {}, err or "bad_payload"

            metric = data.get("metric") if isinstance(data.get("metric"), dict) else {}
            if not isinstance(metric, dict) or not metric:
                return {}, "empty_metric"

            patch = _clean_patch(
                {
                    "provider": PROVIDER_NAME,
                    "data_source": PROVIDER_NAME,
                    # valuation / fundamentals
                    "eps_ttm": safe_float(metric.get("epsTTM")),
                    "pe_ttm": safe_float(metric.get("peTTM")),
                    "pb": safe_float(metric.get("pbAnnual")) or safe_float(metric.get("pbQuarterly")),
                    "ps": safe_float(metric.get("psTTM")),
                    "beta": safe_float(metric.get("beta")),
                    # percent-ish values -> FRACTIONS
                    "dividend_yield": _percentish_to_fraction(metric.get("dividendYieldIndicatedAnnual")),
                    "payout_ratio": _percentish_to_fraction(metric.get("payoutRatioTTM")),
                    "roe": _percentish_to_fraction(metric.get("roeTTM")),
                    "roa": _percentish_to_fraction(metric.get("roaTTM")),
                    "net_margin": _percentish_to_fraction(metric.get("netMargin")),
                    # 52w sometimes exists here
                    "high_52w": safe_float(metric.get("52WeekHigh")),
                    "low_52w": safe_float(metric.get("52WeekLow")),
                    "last_updated_utc": _utc_iso(),
                    "last_updated_riyadh": _riyadh_iso(),
                }
            )
            await self.metric_cache.set(ck, patch)
            return patch, None

        return await self._sf.do(ck, _do)

    async def fetch_history_stats(self, symbol: str) -> Tuple[Dict[str, Any], Optional[str]]:
        sym = normalize_finnhub_symbol(symbol)
        if not sym:
            return {}, "invalid_symbol"

        days = _env_int("FINNHUB_HISTORY_DAYS", 500, lo=60, hi=3000)
        ck = f"h:{sym}:{days}"
        cached = await self.history_cache.get(ck)
        if cached:
            return cached, None

        async def _do():
            # Finnhub candle requires epoch seconds
            to_ts = int(time.time())
            from_ts = to_ts - int(days * 86400)

            data, err = await self._request_json(
                "stock/candle",
                params={"symbol": sym, "resolution": "D", "from": from_ts, "to": to_ts},
            )
            if err or not isinstance(data, dict):
                return {}, err or "bad_payload"

            if data.get("s") != "ok":
                return {}, f"candle_status:{safe_str(data.get('s')) or 'bad'}"

            closes_raw = data.get("c") or []
            vols_raw = data.get("v") or []
            times_raw = data.get("t") or []

            closes = [safe_float(x) for x in closes_raw]
            closes = [x for x in closes if x is not None]
            vols = [safe_float(x) for x in vols_raw]
            vols = [x for x in vols if x is not None]

            if len(closes) < 25:
                return {"history_points": len(closes)}, None

            # 52W window
            win_52 = _env_int("FINNHUB_WINDOW_52W", 252, lo=60, hi=800)
            win = min(win_52, len(closes))
            last = closes[-1]
            low_52 = min(closes[-win:])
            high_52 = max(closes[-win:])
            pos_52 = None
            if high_52 != low_52:
                pos_52 = (last - low_52) / (high_52 - low_52)  # FRACTION 0..1

            avg_vol_30 = None
            if len(vols) >= 30:
                avg_vol_30 = sum(vols[-30:]) / 30.0

            vol_30 = _volatility_30d_annualized_fraction(closes)  # FRACTION

            ma20 = _sma(closes, 20)
            ma50 = _sma(closes, 50)
            ma200 = _sma(closes, 200)

            rsi14 = _rsi_14(closes)

            r1w = _returns(closes, 5)
            r1m = _returns(closes, 21)
            r3m = _returns(closes, 63)
            r6m = _returns(closes, 126)
            r12m = _returns(closes, 252)

            last_dt = None
            if times_raw:
                try:
                    last_dt = datetime.fromtimestamp(float(times_raw[-1]), tz=timezone.utc)
                except Exception:
                    last_dt = None

            patch = _clean_patch(
                {
                    "provider": PROVIDER_NAME,
                    "data_source": PROVIDER_NAME,
                    # 52W
                    "high_52w": high_52,
                    "low_52w": low_52,
                    "position_52w_pct": pos_52,      # FRACTION (sheet percent)
                    "week_52_position_pct": pos_52,  # alias
                    # volume / tech
                    "avg_vol_30d": avg_vol_30,
                    "volatility_30d": vol_30,        # FRACTION (sheet percent)
                    "rsi_14": rsi14,
                    "ma20": ma20,
                    "ma50": ma50,
                    "ma200": ma200,
                    # returns (FRACTIONS)
                    "returns_1w": r1w,
                    "returns_1m": r1m,
                    "returns_3m": r3m,
                    "returns_6m": r6m,
                    "returns_12m": r12m,
                    # meta
                    "history_points": len(closes),
                    "history_last_utc": _utc_iso(last_dt) if last_dt else None,
                    "last_updated_utc": _utc_iso(),
                    "last_updated_riyadh": _riyadh_iso(),
                }
            )

            await self.history_cache.set(ck, patch)
            return patch, None

        return await self._sf.do(ck, _do)

    async def aclose(self) -> None:
        try:
            await self._client.aclose()
        except Exception:
            pass


# =============================================================================
# Provider-level orchestration
# =============================================================================
_INSTANCE: Optional[FinnhubClient] = None
_INSTANCE_LOCK = asyncio.Lock()

async def get_client() -> FinnhubClient:
    global _INSTANCE
    if _INSTANCE is None:
        async with _INSTANCE_LOCK:
            if _INSTANCE is None:
                _INSTANCE = FinnhubClient()
    return _INSTANCE

async def close_client() -> None:
    global _INSTANCE
    if _INSTANCE is not None:
        await _INSTANCE.aclose()
        _INSTANCE = None


def _enabled() -> bool:
    if not _env_bool("FINNHUB_ENABLED", True):
        return False
    return bool(_token())


async def fetch_enriched_quote_patch(symbol: str, *args: Any, **kwargs: Any) -> Dict[str, Any]:
    """
    Main provider entrypoint used by DataEngine.
    Returns a mergeable patch. Never throws.
    """
    now_utc = _utc_iso()
    now_riy = _riyadh_iso()

    raw = (symbol or "").strip()
    if not raw:
        return {
            "provider": PROVIDER_NAME,
            "data_source": PROVIDER_NAME,
            "data_quality": "MISSING",
            "error": "empty_symbol",
            "last_updated_utc": now_utc,
            "last_updated_riyadh": now_riy,
        }

    if not _enabled():
        return {
            "provider": PROVIDER_NAME,
            "data_source": PROVIDER_NAME,
            "data_quality": "DISABLED",
            "error": "provider_disabled_or_missing_key",
            "last_updated_utc": now_utc,
            "last_updated_riyadh": now_riy,
        }

    if looks_like_ksa(raw):
        return {
            "provider": PROVIDER_NAME,
            "data_source": PROVIDER_NAME,
            "data_quality": "BLOCKED",
            "error": "ksa_blocked",
            "last_updated_utc": now_utc,
            "last_updated_riyadh": now_riy,
        }

    if is_blocked_special(raw):
        return {
            "provider": PROVIDER_NAME,
            "data_source": PROVIDER_NAME,
            "data_quality": "BLOCKED",
            "error": "special_symbol_blocked",
            "last_updated_utc": now_utc,
            "last_updated_riyadh": now_riy,
        }

    client = await get_client()

    enable_profile = _env_bool("FINNHUB_ENABLE_PROFILE", True)
    enable_metric = _env_bool("FINNHUB_ENABLE_METRIC", True)
    enable_history = _env_bool("FINNHUB_ENABLE_HISTORY", True)

    variants = symbol_variants(raw)
    if not variants:
        variants = [normalize_finnhub_symbol(raw)]

    errors: List[str] = []
    merged: Dict[str, Any] = {
        "provider": PROVIDER_NAME,
        "data_source": PROVIDER_NAME,
        "last_updated_utc": now_utc,
        "last_updated_riyadh": now_riy,
    }

    # Quote: try variants until one succeeds
    used_variant = variants[0]
    quote_ok = False
    for v in variants[:3]:
        patch, err = await client.fetch_quote(v)
        if err:
            errors.append(f"quote:{v}:{err}")
            continue
        if patch and patch.get("current_price") is not None:
            merged.update(patch)
            used_variant = normalize_finnhub_symbol(v)
            quote_ok = True
            break

    # Profile / metric / history (best-effort; use the used_variant)
    tasks: List[asyncio.Task] = []
    if enable_profile:
        tasks.append(asyncio.create_task(client.fetch_profile(used_variant)))
    if enable_metric:
        tasks.append(asyncio.create_task(client.fetch_metrics(used_variant)))
    if enable_history:
        tasks.append(asyncio.create_task(client.fetch_history_stats(used_variant)))

    results = await asyncio.gather(*tasks, return_exceptions=True)
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

    if not quote_ok:
        merged["data_quality"] = "MISSING"
        merged["error"] = "fetch_failed"
        merged["error_detail"] = ",".join(sorted(set(errors))) if errors else "no_data"
    else:
        merged["data_quality"] = "OK"

    return _clean_patch(merged)


# Back-compat aliases expected by your DataEngine
async def fetch_quote_patch(symbol: str, *args: Any, **kwargs: Any) -> Dict[str, Any]:
    client = await get_client()
    v = normalize_finnhub_symbol(symbol)
    patch, err = await client.fetch_quote(v)
    if err:
        return {"provider": PROVIDER_NAME, "data_source": PROVIDER_NAME, "error": err, "data_quality": "MISSING"}
    patch.setdefault("data_quality", "OK")
    return patch

async def fetch_patch(symbol: str, *args: Any, **kwargs: Any) -> Dict[str, Any]:
    return await fetch_enriched_quote_patch(symbol, *args, **kwargs)


__all__ = [
    "PROVIDER_NAME",
    "PROVIDER_VERSION",
    "fetch_enriched_quote_patch",
    "fetch_quote_patch",
    "fetch_patch",
    "get_client",
    "close_client",
    "normalize_finnhub_symbol",
]
