#!/usr/bin/env python3
# core/providers/finnhub_provider.py
"""
core/providers/finnhub_provider.py
============================================================
Finnhub Provider (GLOBAL enrichment fallback) — v3.1.0
PROD SAFE + ASYNC + ENGINE PATCH-STYLE + ALIGNED ROI KEYS + RIYADH TIME

Upgrades in v3.1.0 (FULL REPLACEMENT)
- ✅ Strict routing guard:
    - Rejects KSA symbols (#### / ####.SR / TADAWUL:* etc.)
    - Rejects Yahoo special symbols (^INDEX, GC=F, BZ=F, BTC-USD, etc.)
- ✅ Shared normalizer integration when available (core.symbols.normalize)
- ✅ Import-safe + no network calls at import time
- ✅ Lazy singleton client with:
    - concurrency limit (FINNHUB_MAX_CONCURRENCY)
    - optional token-bucket rate limiter (FINNHUB_RATE_LIMIT_PER_SEC)
    - circuit breaker (FINNHUB_CIRCUIT_BREAKER, threshold + cooldown)
- ✅ Robust retries with backoff + jitter for 429/5xx
- ✅ Better quote mapping:
    - derives price_change + percent_change if missing
    - sets last_updated_utc + last_updated_riyadh
- ✅ Profile enrichment:
    - name, sector, currency, market_cap, shares_outstanding
    - and optional exchange/country/ipo/weburl/logo
- ✅ History enrichment:
    - returns (1W/1M/3M/6M/12M), MA20/50/200, RSI14, vol_30d, max_drawdown
    - forecast uses log-price regression (stable) with confidence score
    - outputs ROI keys: expected_roi_1m/3m/12m
    - outputs price aliases: forecast_price_1m/3m/12m
    - outputs forecast_updated_utc + forecast_updated_riyadh derived from last candle time
- ✅ Patch style:
    - only useful fields returned
    - always includes provider + data_source="finnhub"
    - silent {} if not configured (unless FINNHUB_VERBOSE_WARNINGS=true)

Environment variables
- FINNHUB_ENABLED=true/false
- FINNHUB_API_KEY (or FINNHUB_API_TOKEN / FINNHUB_TOKEN)
- FINNHUB_BASE_URL (default https://finnhub.io/api/v1)
- FINNHUB_TIMEOUT_SEC (default 10)
- FINNHUB_RETRY_ATTEMPTS (default 3)
- FINNHUB_RETRY_DELAY_SEC (default 0.25)
- FINNHUB_QUOTE_TTL_SEC (default 10)
- FINNHUB_PROFILE_TTL_SEC (default 21600)
- FINNHUB_HISTORY_TTL_SEC (default 1200)
- FINNHUB_HISTORY_DAYS (default 400)
- FINNHUB_HISTORY_POINTS_MAX (default 400)
- FINNHUB_ENABLE_PROFILE=true/false (default true)
- FINNHUB_ENABLE_HISTORY=true/false (default true)
- FINNHUB_ENABLE_FORECAST=true/false (default true)
- FINNHUB_VERBOSE_WARNINGS=true/false (default false)

Advanced controls
- FINNHUB_MAX_CONCURRENCY (default 25)
- FINNHUB_RATE_LIMIT_PER_SEC (0 disables; default 0)
- FINNHUB_CIRCUIT_BREAKER=true/false (default true)
- FINNHUB_CB_FAIL_THRESHOLD (default 6)
- FINNHUB_CB_COOLDOWN_SEC (default 30)
"""

from __future__ import annotations

import asyncio
import json
import logging
import math
import os
import random
import re
import time
from datetime import datetime, timezone, timedelta
from typing import Any, Dict, List, Optional, Sequence, Tuple, Union

import httpx

logger = logging.getLogger("core.providers.finnhub_provider")

PROVIDER_NAME = "finnhub"
PROVIDER_VERSION = "3.1.0"

DEFAULT_BASE_URL = "https://finnhub.io/api/v1"
DEFAULT_TIMEOUT_SEC = 10.0
DEFAULT_RETRY_ATTEMPTS = 3

_TRUTHY = {"1", "true", "yes", "y", "on", "t"}
_FALSY = {"0", "false", "no", "n", "off", "f"}

USER_AGENT_DEFAULT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/124.0.0.0 Safari/537.36"
)

# Reject Yahoo-ish specials: indices, futures, commodities, crypto tickers formatted like Yahoo
_SPECIAL_SYMBOL_RE = re.compile(r"(\^|=|/|:)", re.IGNORECASE)
_CRYPTO_YAHOO_RE = re.compile(r"^[A-Z0-9]{2,10}-[A-Z0-9]{2,10}$", re.IGNORECASE)

# KSA strict code detection
_KSA_CODE_RE = re.compile(r"^\d{3,6}(\.SR)?$", re.IGNORECASE)

# ---------------------------------------------------------------------------
# Shared Normalizer Import (optional)
# ---------------------------------------------------------------------------
def _try_import_shared_normalizer() -> Tuple[Optional[Any], Optional[Any]]:
    try:
        from core.symbols.normalize import normalize_symbol as _ns  # type: ignore
        from core.symbols.normalize import looks_like_ksa as _lk  # type: ignore
        return _ns, _lk
    except Exception:
        return None, None


_SHARED_NORMALIZE, _SHARED_LOOKS_KSA = _try_import_shared_normalizer()

# ---------------------------------------------------------------------------
# Env & helpers
# ---------------------------------------------------------------------------
def _env_str(name: str, default: str = "") -> str:
    v = os.getenv(name)
    if v is None:
        return default
    s = str(v).strip()
    return s if s else default


def _env_int(name: str, default: int) -> int:
    v = os.getenv(name)
    if v is None:
        return default
    try:
        return int(str(v).strip())
    except Exception:
        return default


def _env_float(name: str, default: float) -> float:
    v = os.getenv(name)
    if v is None:
        return default
    try:
        return float(str(v).strip())
    except Exception:
        return default


def _env_bool(name: str, default: bool) -> bool:
    raw = (os.getenv(name) or "").strip().lower()
    if not raw:
        return default
    if raw in _FALSY:
        return False
    if raw in _TRUTHY:
        return True
    return default


def _utc_iso(dt: Optional[datetime] = None) -> str:
    d = dt or datetime.now(timezone.utc)
    if d.tzinfo is None:
        d = d.replace(tzinfo=timezone.utc)
    return d.astimezone(timezone.utc).isoformat()


def _to_riyadh_iso(dt: Optional[datetime]) -> Optional[str]:
    if not dt:
        return None
    tz = timezone(timedelta(hours=3))
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(tz).isoformat()


def _riyadh_now_iso() -> str:
    tz = timezone(timedelta(hours=3))
    return datetime.now(tz).isoformat()


def _configured() -> bool:
    if not _env_bool("FINNHUB_ENABLED", True):
        return False
    return bool(_token())


def _emit_warnings() -> bool:
    return _env_bool("FINNHUB_VERBOSE_WARNINGS", False)


def _token() -> Optional[str]:
    for k in ("FINNHUB_API_KEY", "FINNHUB_API_TOKEN", "FINNHUB_TOKEN"):
        v = (os.getenv(k) or "").strip()
        if v:
            return v
    return None


def _base_url() -> str:
    u = _env_str("FINNHUB_BASE_URL", DEFAULT_BASE_URL).rstrip("/")
    return u or DEFAULT_BASE_URL


def _timeout_sec() -> float:
    t = _env_float("FINNHUB_TIMEOUT_SEC", DEFAULT_TIMEOUT_SEC)
    return max(3.0, float(t))


def _retry_attempts() -> int:
    r = _env_int("FINNHUB_RETRY_ATTEMPTS", DEFAULT_RETRY_ATTEMPTS)
    return max(1, int(r))


def _retry_delay_sec() -> float:
    d = _env_float("FINNHUB_RETRY_DELAY_SEC", 0.25)
    return max(0.05, float(d))


def _quote_ttl_sec() -> float:
    ttl = _env_float("FINNHUB_QUOTE_TTL_SEC", 10.0)
    return max(5.0, float(ttl))


def _profile_ttl_sec() -> float:
    ttl = _env_float("FINNHUB_PROFILE_TTL_SEC", 21600.0)
    return max(60.0, float(ttl))


def _history_ttl_sec() -> float:
    ttl = _env_float("FINNHUB_HISTORY_TTL_SEC", 1200.0)
    return max(60.0, float(ttl))


def _history_days() -> int:
    d = _env_int("FINNHUB_HISTORY_DAYS", 400)
    return max(60, int(d))


def _history_points_max() -> int:
    n = _env_int("FINNHUB_HISTORY_POINTS_MAX", 400)
    return max(100, int(n))


def _enable_profile() -> bool:
    return _env_bool("FINNHUB_ENABLE_PROFILE", True)


def _enable_history() -> bool:
    return _env_bool("FINNHUB_ENABLE_HISTORY", True)


def _enable_forecast() -> bool:
    return _env_bool("FINNHUB_ENABLE_FORECAST", True)


def _max_concurrency() -> int:
    return max(2, _env_int("FINNHUB_MAX_CONCURRENCY", 25))


def _rate_limit_per_sec() -> float:
    return max(0.0, _env_float("FINNHUB_RATE_LIMIT_PER_SEC", 0.0))


def _cb_enabled() -> bool:
    return _env_bool("FINNHUB_CIRCUIT_BREAKER", True)


def _cb_fail_threshold() -> int:
    return max(2, _env_int("FINNHUB_CB_FAIL_THRESHOLD", 6))


def _cb_cooldown_sec() -> float:
    return max(5.0, _env_float("FINNHUB_CB_COOLDOWN_SEC", 30.0))


def _safe_str(x: Any) -> Optional[str]:
    if x is None:
        return None
    s = str(x).strip()
    return s or None


def _to_float(x: Any) -> Optional[float]:
    try:
        if x is None:
            return None
        if isinstance(x, (int, float)):
            f = float(x)
            if math.isnan(f) or math.isinf(f):
                return None
            return f
        s = str(x).replace(",", "").strip()
        if not s:
            return None
        f = float(s)
        if math.isnan(f) or math.isinf(f):
            return None
        return f
    except Exception:
        return None


# ---------------------------------------------------------------------------
# Caching (TTLCache fallback)
# ---------------------------------------------------------------------------
try:
    from cachetools import TTLCache  # type: ignore

    _HAS_CACHETOOLS = True
except Exception:
    _HAS_CACHETOOLS = False

    class TTLCache(dict):  # type: ignore
        def __init__(self, maxsize: int, ttl: float):
            super().__init__()
            self._maxsize = max(1, int(maxsize))
            self._ttl = max(1.0, float(ttl))
            self._exp: Dict[str, float] = {}

        def get(self, key, default=None):  # type: ignore
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

        def __setitem__(self, key, value):  # type: ignore
            if len(self) >= self._maxsize:
                try:
                    oldest_key = next(iter(self.keys()))
                    super().pop(oldest_key, None)
                    self._exp.pop(oldest_key, None)
                except Exception:
                    pass
            super().__setitem__(key, value)
            self._exp[key] = time.time() + self._ttl


_Q_CACHE = TTLCache(maxsize=8000, ttl=_quote_ttl_sec())
_P_CACHE = TTLCache(maxsize=4000, ttl=_profile_ttl_sec())
_H_CACHE = TTLCache(maxsize=2500, ttl=_history_ttl_sec())

# ---------------------------------------------------------------------------
# Routing guards
# ---------------------------------------------------------------------------
def _looks_like_ksa(symbol: str) -> bool:
    s = (symbol or "").strip().upper()
    if not s:
        return False

    if callable(_SHARED_LOOKS_KSA):
        try:
            return bool(_SHARED_LOOKS_KSA(s))
        except Exception:
            pass

    # local fallback
    if s.startswith("TADAWUL:"):
        return True
    if s.endswith(".SR"):
        return True
    if _KSA_CODE_RE.match(s):
        return True
    return False


def _blocked_special(symbol: str) -> bool:
    s = (symbol or "").strip().upper()
    if not s:
        return True
    if _SPECIAL_SYMBOL_RE.search(s):
        return True
    if _CRYPTO_YAHOO_RE.match(s):
        return True
    return False


def _normalize_global_symbol(symbol: str) -> str:
    """
    Finnhub generally expects:
      - US equities like AAPL
      - non-US: depends on exchange mapping (user should pass correct Finnhub symbol)
    We keep minimal cleanup:
      - remove .US suffix
      - strip spaces
    """
    s = (symbol or "").strip().upper()
    if s.endswith(".US"):
        s = s[:-3]
    return s


# ---------------------------------------------------------------------------
# Rate limiter + circuit breaker
# ---------------------------------------------------------------------------
class _TokenBucket:
    def __init__(self, rate_per_sec: float) -> None:
        self.rate = max(0.0, float(rate_per_sec))
        self.capacity = max(1.0, self.rate) if self.rate > 0 else 0.0
        self.tokens = self.capacity
        self.last = time.monotonic()
        self._lock = asyncio.Lock()

    async def acquire(self) -> None:
        if self.rate <= 0:
            return

        async with self._lock:
            now = time.monotonic()
            elapsed = max(0.0, now - self.last)
            self.last = now
            self.tokens = min(self.capacity, self.tokens + elapsed * self.rate)

            if self.tokens >= 1.0:
                self.tokens -= 1.0
                return

            need = 1.0 - self.tokens
            wait = need / self.rate if self.rate > 0 else 0.0

        if wait > 0:
            await asyncio.sleep(min(2.0, wait + random.random() * 0.05))

        async with self._lock:
            self.tokens = max(0.0, self.tokens - 1.0)


class _CircuitBreaker:
    def __init__(self, enabled: bool, fail_threshold: int, cooldown_sec: float) -> None:
        self.enabled = bool(enabled)
        self.fail_threshold = max(2, int(fail_threshold))
        self.cooldown_sec = max(5.0, float(cooldown_sec))

        self._fail_count = 0
        self._open_until = 0.0
        self._lock = asyncio.Lock()

    async def allow(self) -> bool:
        if not self.enabled:
            return True
        async with self._lock:
            return not (self._open_until > time.monotonic())

    async def on_success(self) -> None:
        if not self.enabled:
            return
        async with self._lock:
            self._fail_count = 0
            self._open_until = 0.0

    async def on_failure(self) -> None:
        if not self.enabled:
            return
        async with self._lock:
            self._fail_count += 1
            if self._fail_count >= self.fail_threshold:
                self._open_until = time.monotonic() + self.cooldown_sec


# ---------------------------------------------------------------------------
# History analytics + forecast
# ---------------------------------------------------------------------------
def _stddev(xs: List[float]) -> Optional[float]:
    try:
        n = len(xs)
        if n < 2:
            return None
        mu = sum(xs) / n
        var = sum((x - mu) ** 2 for x in xs) / (n - 1)
        return math.sqrt(var)
    except Exception:
        return None


def _compute_rsi_14(closes: List[float]) -> Optional[float]:
    try:
        if len(closes) < 15:
            return None
        gains: List[float] = []
        losses: List[float] = []
        for i in range(-14, 0):
            d = closes[i] - closes[i - 1]
            if d >= 0:
                gains.append(d)
                losses.append(0.0)
            else:
                gains.append(0.0)
                losses.append(-d)
        avg_gain = sum(gains) / 14.0
        avg_loss = sum(losses) / 14.0
        if avg_loss == 0:
            return 100.0
        rs = avg_gain / avg_loss
        rsi = 100.0 - (100.0 / (1.0 + rs))
        if math.isnan(rsi) or math.isinf(rsi):
            return None
        return float(max(0.0, min(100.0, rsi)))
    except Exception:
        return None


def _max_drawdown_pct(closes: List[float]) -> Optional[float]:
    try:
        if len(closes) < 2:
            return None
        peak = closes[0]
        mdd = 0.0
        for p in closes[1:]:
            if p > peak:
                peak = p
            if peak > 0:
                dd = (p / peak) - 1.0
                if dd < mdd:
                    mdd = dd
        return float(mdd * 100.0)
    except Exception:
        return None


def _log_regression(prices: List[float]) -> Optional[Dict[str, float]]:
    """
    Fit y=a+b*x on y=log(price), x=0..n-1. Returns slope b and r2.
    """
    try:
        n = len(prices)
        if n < 12:
            return None
        ys = []
        for p in prices:
            if p is None or p <= 0:
                return None
            ys.append(math.log(float(p)))

        xs = list(range(n))
        x_mean = (n - 1) / 2.0
        y_mean = sum(ys) / n

        sxx = sum((x - x_mean) ** 2 for x in xs)
        if sxx == 0:
            return None
        sxy = sum((x - x_mean) * (y - y_mean) for x, y in zip(xs, ys))
        b = sxy / sxx
        a = y_mean - b * x_mean

        y_hat = [a + b * x for x in xs]
        ss_tot = sum((y - y_mean) ** 2 for y in ys)
        ss_res = sum((y - yh) ** 2 for y, yh in zip(ys, y_hat))
        r2 = 1.0 - (ss_res / ss_tot) if ss_tot > 0 else 0.0

        if any(map(lambda z: math.isnan(z) or math.isinf(z), [b, r2])):
            return None
        return {"slope": float(b), "r2": float(max(0.0, min(1.0, r2))), "n": float(n)}
    except Exception:
        return None


def _roi_from_reg(reg: Optional[Dict[str, float]], horizon_days: int) -> Optional[float]:
    if not reg:
        return None
    b = reg.get("slope")
    if b is None:
        return None
    try:
        r = math.exp(float(b) * float(horizon_days)) - 1.0
        return float(r * 100.0)
    except Exception:
        return None


def _compute_history_analytics(closes: List[float]) -> Dict[str, Any]:
    out: Dict[str, Any] = {}
    if not closes:
        return out

    last = float(closes[-1])

    # Returns
    idx = {"returns_1w": 5, "returns_1m": 21, "returns_3m": 63, "returns_6m": 126, "returns_12m": 252}
    for k, n in idx.items():
        if len(closes) > n:
            prior = float(closes[-(n + 1)])
            if prior != 0:
                out[k] = float(((last / prior) - 1.0) * 100.0)

    # MAs
    def ma(n: int) -> Optional[float]:
        if len(closes) < n:
            return None
        xs = closes[-n:]
        return float(sum(xs) / float(n))

    out["ma20"] = ma(20)
    out["ma50"] = ma(50)
    out["ma200"] = ma(200)

    # Volatility 30D (annualized)
    if len(closes) >= 31:
        rets: List[float] = []
        window = closes[-31:]
        for i in range(1, len(window)):
            p0 = float(window[i - 1])
            p1 = float(window[i])
            if p0 != 0:
                rets.append((p1 / p0) - 1.0)
        sd = _stddev(rets)
        if sd is not None:
            out["volatility_30d"] = float(sd * math.sqrt(252.0) * 100.0)

    out["rsi_14"] = _compute_rsi_14(closes)
    out["max_drawdown_pct"] = _max_drawdown_pct(closes)

    # Forecast (log regression)
    if _enable_forecast():
        # windows capped for stability
        reg_1m = _log_regression([float(x) for x in closes[-min(len(closes), 84):]])
        reg_3m = _log_regression([float(x) for x in closes[-min(len(closes), 252):]])
        reg_12m = _log_regression([float(x) for x in closes[-min(len(closes), 400):]])

        roi_1m = _roi_from_reg(reg_1m, 21)
        roi_3m = _roi_from_reg(reg_3m, 63)
        roi_12m = _roi_from_reg(reg_12m, 252)

        if roi_1m is not None:
            out["expected_roi_1m"] = float(roi_1m)
            out["forecast_price_1m"] = float(last * (1.0 + roi_1m / 100.0))
        if roi_3m is not None:
            out["expected_roi_3m"] = float(roi_3m)
            out["forecast_price_3m"] = float(last * (1.0 + roi_3m / 100.0))
        if roi_12m is not None:
            out["expected_roi_12m"] = float(roi_12m)
            out["forecast_price_12m"] = float(last * (1.0 + roi_12m / 100.0))

        # Confidence: points + r2 - vol penalty
        pts = len(closes)
        base = max(0.0, min(100.0, (pts / 252.0) * 100.0))

        r2 = None
        for reg in (reg_3m, reg_1m, reg_12m):
            if reg and isinstance(reg.get("r2"), (int, float)):
                r2 = float(reg["r2"])
                break
        if r2 is None:
            r2 = 0.25

        vol = _to_float(out.get("volatility_30d"))
        vol_pen = 0.0 if vol is None else min(55.0, max(0.0, (vol / 100.0) * 35.0))

        conf = (0.55 * base) + (0.45 * (r2 * 100.0)) - vol_pen
        conf = max(0.0, min(100.0, conf))

        out["forecast_confidence"] = float(conf)
        out["forecast_method"] = "finnhub_history_logreg_v2"
    else:
        out["forecast_method"] = "history_only"

    return out


# ---------------------------------------------------------------------------
# Internal client singleton
# ---------------------------------------------------------------------------
_CLIENT: Optional[httpx.AsyncClient] = None
_LOCK = asyncio.Lock()
_SEM: Optional[asyncio.Semaphore] = None
_BUCKET: Optional[_TokenBucket] = None
_CB: Optional[_CircuitBreaker] = None


async def _get_client() -> httpx.AsyncClient:
    global _CLIENT, _SEM, _BUCKET, _CB
    async with _LOCK:
        if _CLIENT is None:
            timeout = httpx.Timeout(_timeout_sec(), connect=min(10.0, _timeout_sec()))
            _CLIENT = httpx.AsyncClient(
                timeout=timeout,
                follow_redirects=True,
                headers={"User-Agent": _env_str("FINNHUB_USER_AGENT", USER_AGENT_DEFAULT)},
                limits=httpx.Limits(max_keepalive_connections=25, max_connections=50),
            )
            _SEM = asyncio.Semaphore(_max_concurrency())
            _BUCKET = _TokenBucket(_rate_limit_per_sec())
            _CB = _CircuitBreaker(_cb_enabled(), _cb_fail_threshold(), _cb_cooldown_sec())

            logger.info(
                "Finnhub client init v%s | base_url=%s | timeout=%.1fs | retries=%s | cachetools=%s | max_conc=%s | rate/s=%.2f | cb=%s",
                PROVIDER_VERSION,
                _base_url(),
                _timeout_sec(),
                _retry_attempts(),
                _HAS_CACHETOOLS,
                _max_concurrency(),
                _rate_limit_per_sec(),
                _cb_enabled(),
            )
    return _CLIENT


async def _http_get_json(path: str, params: Dict[str, Any]) -> Tuple[Optional[dict], Optional[str]]:
    """
    Returns (json_dict, error_str).
    Retries on 429/5xx.
    """
    client = await _get_client()
    assert _SEM is not None
    assert _BUCKET is not None
    assert _CB is not None

    if not await _CB.allow():
        return None, "circuit_open"

    retries = _retry_attempts()
    base_delay = _retry_delay_sec()
    url = f"{_base_url().rstrip('/')}/{path.lstrip('/')}"

    async with _SEM:
        await _BUCKET.acquire()

        last_err: Optional[str] = None
        for attempt in range(retries):
            try:
                r = await client.get(url, params=params)
                sc = int(r.status_code)

                if sc == 429 or 500 <= sc < 600:
                    last_err = f"HTTP {sc}"
                    if attempt < retries - 1:
                        await asyncio.sleep(base_delay * (2**attempt) + random.random() * 0.25)
                        continue
                    await _CB.on_failure()
                    return None, last_err

                if sc >= 400:
                    await _CB.on_failure()
                    return None, f"HTTP {sc}"

                try:
                    js = r.json()
                except Exception:
                    txt = (r.text or "").strip()
                    if txt.startswith("{"):
                        try:
                            js = json.loads(txt)
                        except Exception:
                            await _CB.on_failure()
                            return None, "invalid JSON"
                    else:
                        await _CB.on_failure()
                        return None, "non-JSON response"

                if not isinstance(js, dict):
                    await _CB.on_failure()
                    return None, "unexpected JSON type"

                await _CB.on_success()
                return js, None

            except Exception as e:
                last_err = f"{e.__class__.__name__}: {e}"
                if attempt < retries - 1:
                    await asyncio.sleep(base_delay * (2**attempt) + random.random() * 0.25)
                    continue
                await _CB.on_failure()
                return None, last_err

        await _CB.on_failure()
        return None, last_err or "request_failed"


# ---------------------------------------------------------------------------
# Core fetch
# ---------------------------------------------------------------------------
async def _fetch(symbol_raw: str, want_profile: bool, want_history: bool) -> Dict[str, Any]:
    if not _configured():
        return {}

    sym_in = (symbol_raw or "").strip()
    sym = sym_in.upper()

    # routing guards
    if not sym or _looks_like_ksa(sym):
        return {} if not _emit_warnings() else {"_warn": "finnhub blocked: KSA symbol"}
    if _blocked_special(sym):
        return {} if not _emit_warnings() else {"_warn": "finnhub blocked: index/commodity/unsupported symbol"}

    sym_norm = _normalize_global_symbol(sym)
    token = _token()
    if not token:
        return {} if not _emit_warnings() else {"_warn": "FINNHUB_API_KEY missing"}

    params = {"symbol": sym_norm, "token": token}

    # Cache key per normalized symbol (quote)
    qk = f"q::{sym_norm}"
    cached_q = _Q_CACHE.get(qk)
    if isinstance(cached_q, dict) and cached_q:
        out = dict(cached_q)
    else:
        js, err = await _http_get_json("/quote", params)
        if js is None:
            return {} if not _emit_warnings() else {"_warn": f"finnhub quote failed: {err}"}

        cp = _to_float(js.get("c"))
        pc = _to_float(js.get("pc"))
        dp = _to_float(js.get("dp"))
        hi = _to_float(js.get("h"))
        lo = _to_float(js.get("l"))
        op = _to_float(js.get("o"))

        if cp is None or cp == 0:
            return {} if not _emit_warnings() else {"_warn": "finnhub empty quote (c=0)"}

        price_change = _to_float(js.get("d"))
        if price_change is None and cp is not None and pc is not None:
            price_change = float(cp - pc)

        percent_change = dp
        if percent_change is None and cp is not None and pc is not None and pc != 0:
            percent_change = float(((cp / pc) - 1.0) * 100.0)

        out = {
            "requested_symbol": sym_in,
            "symbol": sym,  # keep user-facing
            "normalized_symbol": sym_norm,
            "current_price": cp,
            "previous_close": pc,
            "open": op,
            "day_high": hi,
            "day_low": lo,
            "price_change": price_change,
            "percent_change": percent_change,
            "provider": PROVIDER_NAME,
            "data_source": PROVIDER_NAME,
            "provider_version": PROVIDER_VERSION,
            "last_updated_utc": _utc_iso(),
            "last_updated_riyadh": _riyadh_now_iso(),
        }

        # Save quote cache
        _Q_CACHE[qk] = dict(out)

    # Profile enrichment
    if want_profile and _enable_profile():
        pk = f"p::{sym_norm}"
        cached_p = _P_CACHE.get(pk)
        if isinstance(cached_p, dict):
            out.update({k: v for k, v in cached_p.items() if v is not None})
        else:
            p_js, p_err = await _http_get_json("/stock/profile2", params)
            if p_js is not None and p_js:
                prof = {
                    "name": _safe_str(p_js.get("name")),
                    "sector": _safe_str(p_js.get("finnhubIndustry")),
                    "currency": _safe_str(p_js.get("currency")),
                    "market_cap": _to_float(p_js.get("marketCapitalization")),
                    "shares_outstanding": _to_float(p_js.get("shareOutstanding")),
                    "exchange": _safe_str(p_js.get("exchange")),
                    "country": _safe_str(p_js.get("country")),
                    "ipo": _safe_str(p_js.get("ipo")),
                    "weburl": _safe_str(p_js.get("weburl")),
                    "logo": _safe_str(p_js.get("logo")),
                }
                prof = {k: v for k, v in prof.items() if v not in (None, "", [])}
                _P_CACHE[pk] = dict(prof)
                # fill missing only (do not overwrite quote values)
                for k, v in prof.items():
                    if k not in out or out.get(k) in (None, ""):
                        out[k] = v

    # History enrichment
    if want_history and _enable_history():
        hk = f"h::{sym_norm}"
        cached_h = _H_CACHE.get(hk)
        if isinstance(cached_h, dict):
            for k, v in cached_h.items():
                if k not in out and v is not None:
                    out[k] = v
        else:
            end = int(time.time())
            start = end - (_history_days() * 86400)

            h_params = {**params, "resolution": "D", "from": start, "to": end}
            h_js, h_err = await _http_get_json("/stock/candle", h_params)
            if h_js and isinstance(h_js, dict) and h_js.get("s") == "ok":
                closes = h_js.get("c") or []
                times = h_js.get("t") or []

                # sanitize closes
                closes_f: List[float] = []
                for x in closes:
                    fx = _to_float(x)
                    if fx is not None:
                        closes_f.append(float(fx))
                closes_f = closes_f[-_history_points_max():]

                last_dt = None
                try:
                    if isinstance(times, list) and times:
                        t_last = times[-1]
                        if isinstance(t_last, (int, float)):
                            last_dt = datetime.fromtimestamp(float(t_last), tz=timezone.utc)
                except Exception:
                    last_dt = None

                analytics = _compute_history_analytics(closes_f)
                patch: Dict[str, Any] = {
                    "history_points": len(closes_f),
                    "history_last_utc": _utc_iso(last_dt) if last_dt else _utc_iso(),
                    "forecast_updated_utc": _utc_iso(last_dt) if last_dt else _utc_iso(),
                }
                riyadh = _to_riyadh_iso(last_dt)
                if riyadh:
                    patch["forecast_updated_riyadh"] = riyadh

                # Merge analytics (don’t overwrite quote/profile fields)
                patch.update(analytics)

                # Align ROI key aliases (keep expected_roi_* as primary)
                # Also ensure forecast_price_* exists when expected_roi_* exists
                cp = _to_float(out.get("current_price"))
                if cp is not None:
                    for horizon, days in (("1m", 21), ("3m", 63), ("12m", 252)):
                        roi_key = f"expected_roi_{horizon}"
                        fp_key = f"forecast_price_{horizon}"
                        roi = _to_float(patch.get(roi_key))
                        if roi is not None and patch.get(fp_key) is None:
                            patch[fp_key] = float(cp * (1.0 + roi / 100.0))

                patch["forecast_source"] = "finnhub_history"
                patch["provider"] = PROVIDER_NAME
                patch["data_source"] = PROVIDER_NAME

                patch = {k: v for k, v in patch.items() if v is not None and not (isinstance(v, str) and not v.strip())}
                _H_CACHE[hk] = dict(patch)

                for k, v in patch.items():
                    if k not in out and v is not None:
                        out[k] = v
            else:
                if _emit_warnings() and h_err:
                    out["_warn"] = (out.get("_warn", "") + " | " if out.get("_warn") else "") + f"history failed: {h_err}"

    # Final cleanup
    out = {k: v for k, v in out.items() if v is not None and not (isinstance(v, str) and not v.strip())}
    out.setdefault("provider", PROVIDER_NAME)
    out.setdefault("data_source", PROVIDER_NAME)
    out.setdefault("provider_version", PROVIDER_VERSION)
    return out


# ---------------------------------------------------------------------------
# Exported engine callables
# ---------------------------------------------------------------------------
async def fetch_enriched_quote_patch(symbol: str, *args: Any, **kwargs: Any) -> Dict[str, Any]:
    """
    Primary engine entry:
    Returns a patch with quote + profile + history analytics (if enabled).
    """
    return await _fetch(symbol, want_profile=True, want_history=True)


async def fetch_quote_patch(symbol: str, *args: Any, **kwargs: Any) -> Dict[str, Any]:
    """
    Quote-only patch (fast).
    """
    return await _fetch(symbol, want_profile=False, want_history=False)


async def fetch_quote_and_profile_patch(symbol: str, *args: Any, **kwargs: Any) -> Dict[str, Any]:
    """
    Quote + profile patch.
    """
    return await _fetch(symbol, want_profile=True, want_history=False)


async def fetch_quote_and_history_patch(symbol: str, *args: Any, **kwargs: Any) -> Dict[str, Any]:
    """
    Quote + history patch.
    """
    return await _fetch(symbol, want_profile=False, want_history=True)


async def aclose_finnhub_client() -> None:
    """
    Close underlying HTTP client (optional lifecycle hook).
    """
    global _CLIENT
    if _CLIENT is not None:
        try:
            await _CLIENT.aclose()
        except Exception:
            pass
    _CLIENT = None


__all__ = [
    "PROVIDER_NAME",
    "PROVIDER_VERSION",
    "fetch_enriched_quote_patch",
    "fetch_quote_patch",
    "fetch_quote_and_profile_patch",
    "fetch_quote_and_history_patch",
    "aclose_finnhub_client",
]
