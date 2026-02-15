#!/usr/bin/env python3
# core/providers/yahoo_chart_provider.py
"""
core/providers/yahoo_chart_provider.py
============================================================
Yahoo Quote/Chart Provider (via yfinance) — v3.1.0
(PROD SAFE + ASYNC + ENGINE PATCH-STYLE + ADVANCED HISTORY ANALYTICS)

FULL REPLACEMENT (v3.1.0) — What’s improved vs v2.3.0
- ✅ Import-safe (no network calls at import-time)
- ✅ Patch-style output (engine aligned): always Dict[str, Any], returns only useful fields
- ✅ Strict ROI key alignment:
    expected_roi_1m/3m/12m + forecast_price_1m/3m/12m
- ✅ Riyadh localization:
    last_updated_riyadh + forecast_updated_riyadh (UTC+3)
- ✅ Robust price fetch:
    - Uses fast_info when available
    - Falls back to recent history close/open/high/low
- ✅ Advanced history analytics (from daily closes):
    returns_1w/1m/3m/6m/12m, MA20/50/200, RSI14, vol_30d, max_drawdown_pct
- ✅ Forecast: log-price regression (stable) + confidence
    - confidence_score: 0..100
    - forecast_confidence: 0..1 (engine-friendly)
- ✅ Performance & safety:
    - Lazy singleton provider + concurrency limit (YAHOO_MAX_CONCURRENCY)
    - Optional token-bucket rate limit (YAHOO_RATE_LIMIT_PER_SEC)
    - Circuit breaker (YAHOO_CIRCUIT_BREAKER, threshold + cooldown)
    - Singleflight (anti-stampede): concurrent calls for same symbol share one fetch
    - TTL caches for quote/history + short error backoff cache

Environment variables
- YAHOO_ENABLED=true/false (default true)
- YAHOO_TIMEOUT_SEC (default 20)
- YAHOO_MAX_CONCURRENCY (default 16)
- YAHOO_RATE_LIMIT_PER_SEC (default 0; 0 disables)
- YAHOO_CIRCUIT_BREAKER=true/false (default true)
- YAHOO_CB_FAIL_THRESHOLD (default 6)
- YAHOO_CB_COOLDOWN_SEC (default 30)

History controls
- YAHOO_HISTORY_PERIOD (default "2y")  # must be yfinance-compatible: "1y","2y","5y","max", etc.
- YAHOO_HISTORY_INTERVAL (default "1d")
- YAHOO_HISTORY_TTL_SEC (default 1200)
- YAHOO_QUOTE_TTL_SEC (default 15)
- YAHOO_ERROR_TTL_SEC (default 6)

Toggles
- YAHOO_ENABLE_HISTORY=true/false (default true)
- YAHOO_ENABLE_FORECAST=true/false (default true)
- YAHOO_VERBOSE_WARNINGS=true/false (default false)
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
from datetime import datetime, timezone, timedelta
from typing import Any, Dict, List, Optional, Tuple

logger = logging.getLogger("core.providers.yahoo_chart_provider")

PROVIDER_NAME = "yahoo_chart"
PROVIDER_VERSION = "3.1.0"

_TRUTHY = {"1", "true", "yes", "y", "on", "t"}
_FALSY = {"0", "false", "no", "n", "off", "f"}

_KSA_CODE_RE = re.compile(r"^\d{3,6}$", re.IGNORECASE)
_ARABIC_DIGITS = str.maketrans("٠١٢٣٤٥٦٧٨٩", "0123456789")

# ---------------------------------------------------------------------------
# Optional import (keeps module import-safe)
# ---------------------------------------------------------------------------
try:
    import yfinance as yf  # type: ignore
    _HAS_YFINANCE = True
except Exception:
    yf = None  # type: ignore
    _HAS_YFINANCE = False


# ---------------------------------------------------------------------------
# Env helpers
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


def _configured() -> bool:
    if not _env_bool("YAHOO_ENABLED", True):
        return False
    return _HAS_YFINANCE


def _emit_warnings() -> bool:
    return _env_bool("YAHOO_VERBOSE_WARNINGS", False)


def _enable_history() -> bool:
    return _env_bool("YAHOO_ENABLE_HISTORY", True)


def _enable_forecast() -> bool:
    return _env_bool("YAHOO_ENABLE_FORECAST", True)


def _timeout_sec() -> float:
    return max(5.0, _env_float("YAHOO_TIMEOUT_SEC", 20.0))


def _quote_ttl_sec() -> float:
    return max(5.0, _env_float("YAHOO_QUOTE_TTL_SEC", 15.0))


def _hist_ttl_sec() -> float:
    return max(60.0, _env_float("YAHOO_HISTORY_TTL_SEC", 1200.0))


def _err_ttl_sec() -> float:
    return max(2.0, _env_float("YAHOO_ERROR_TTL_SEC", 6.0))


def _max_concurrency() -> int:
    return max(2, _env_int("YAHOO_MAX_CONCURRENCY", 16))


def _rate_limit_per_sec() -> float:
    return max(0.0, _env_float("YAHOO_RATE_LIMIT_PER_SEC", 0.0))


def _cb_enabled() -> bool:
    return _env_bool("YAHOO_CIRCUIT_BREAKER", True)


def _cb_fail_threshold() -> int:
    return max(2, _env_int("YAHOO_CB_FAIL_THRESHOLD", 6))


def _cb_cooldown_sec() -> float:
    return max(5.0, _env_float("YAHOO_CB_COOLDOWN_SEC", 30.0))


def _hist_period() -> str:
    return _env_str("YAHOO_HISTORY_PERIOD", "2y")


def _hist_interval() -> str:
    return _env_str("YAHOO_HISTORY_INTERVAL", "1d")


def _utc_iso(dt: Optional[datetime] = None) -> str:
    d = dt or datetime.now(timezone.utc)
    if d.tzinfo is None:
        d = d.replace(tzinfo=timezone.utc)
    return d.astimezone(timezone.utc).isoformat()


def _riyadh_iso(dt: Optional[datetime] = None) -> str:
    tz = timezone(timedelta(hours=3))
    d = dt or datetime.now(tz)
    if d.tzinfo is None:
        d = d.replace(tzinfo=timezone.utc)
    return d.astimezone(tz).isoformat()


def _to_riyadh_iso(dt: Optional[datetime]) -> Optional[str]:
    if not dt:
        return None
    tz = timezone(timedelta(hours=3))
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(tz).isoformat()


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


# ---------------------------------------------------------------------------
# Utils
# ---------------------------------------------------------------------------
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
        s = str(x).strip()
        if not s:
            return None
        s = s.translate(_ARABIC_DIGITS).replace(",", "").replace("%", "").replace("+", "").strip()
        if s.startswith("(") and s.endswith(")"):
            s = "-" + s[1:-1].strip()
        f = float(s)
        if math.isnan(f) or math.isinf(f):
            return None
        return f
    except Exception:
        return None


def _clean_patch(p: Dict[str, Any]) -> Dict[str, Any]:
    out: Dict[str, Any] = {}
    for k, v in (p or {}).items():
        if v is None:
            continue
        if isinstance(v, str) and not v.strip():
            continue
        out[k] = v
    return out


def _normalize_symbol(symbol: str) -> str:
    s = (symbol or "").strip()
    if not s:
        return ""
    s = s.translate(_ARABIC_DIGITS).strip().upper()

    if s.startswith("TADAWUL:"):
        s = s.split(":", 1)[1].strip()
    if s.endswith(".TADAWUL"):
        s = s.replace(".TADAWUL", "").strip()

    # KSA code -> .SR
    if _KSA_CODE_RE.match(s):
        return f"{s}.SR"

    # "1120.SR" stays
    if s.endswith(".SR") and _KSA_CODE_RE.match(s[:-3]):
        return s

    return s


# ---------------------------------------------------------------------------
# Rate limiter + circuit breaker + singleflight
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


class _SingleFlight:
    def __init__(self) -> None:
        self._lock = asyncio.Lock()
        self._futs: Dict[str, asyncio.Future] = {}

    async def run(self, key: str, coro_fn):
        async with self._lock:
            fut = self._futs.get(key)
            if fut is not None:
                return await fut
            fut = asyncio.get_event_loop().create_future()
            self._futs[key] = fut

        try:
            val = await coro_fn()
            if not fut.done():
                fut.set_result(val)
            return val
        except Exception as e:
            if not fut.done():
                fut.set_exception(e)
            raise
        finally:
            async with self._lock:
                self._futs.pop(key, None)


# ---------------------------------------------------------------------------
# History analytics + forecast (log regression)
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

    # Moving averages
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

        conf_pct = (0.55 * base) + (0.45 * (r2 * 100.0)) - vol_pen
        conf_pct = max(0.0, min(100.0, conf_pct))

        out["confidence_score"] = float(conf_pct)            # 0..100
        out["forecast_confidence"] = float(conf_pct / 100.0) # 0..1
        out["forecast_method"] = "yahoo_history_logreg_v3"
    else:
        out["forecast_method"] = "history_only"

    return _clean_patch(out)


# ---------------------------------------------------------------------------
# Provider implementation (singleton)
# ---------------------------------------------------------------------------
@dataclass
class YahooChartProvider:
    name: str = PROVIDER_NAME

    def __post_init__(self) -> None:
        self._timeout = _timeout_sec()
        self._sem = asyncio.Semaphore(_max_concurrency())
        self._bucket = _TokenBucket(_rate_limit_per_sec())
        self._cb = _CircuitBreaker(_cb_enabled(), _cb_fail_threshold(), _cb_cooldown_sec())
        self._sf = _SingleFlight()

        self._q_cache: TTLCache = TTLCache(maxsize=7000, ttl=_quote_ttl_sec())
        self._h_cache: TTLCache = TTLCache(maxsize=2500, ttl=_hist_ttl_sec())
        self._e_cache: TTLCache = TTLCache(maxsize=4000, ttl=_err_ttl_sec())

        logger.info(
            "YahooChartProvider init v%s | yfinance=%s | timeout=%.1fs | max_conc=%s | rate/s=%.2f | cb=%s",
            PROVIDER_VERSION,
            _HAS_YFINANCE,
            self._timeout,
            _max_concurrency(),
            _rate_limit_per_sec(),
            _cb_enabled(),
        )

    # ---------------------------
    # Low-level blocking fetch (runs in thread)
    # ---------------------------
    def _blocking_fetch(self, sym: str) -> Tuple[Dict[str, Any], Optional[datetime]]:
        """
        Returns: (payload_dict, last_candle_dt_utc)
        payload contains: price-ish fields + history closes (optional)
        """
        if not _HAS_YFINANCE or yf is None:
            return {"error": "yfinance not installed"}, None

        ticker = yf.Ticker(sym)

        out: Dict[str, Any] = {}
        last_dt: Optional[datetime] = None

        # 1) Try fast_info
        try:
            fi = getattr(ticker, "fast_info", None)
            if fi:
                # fast_info sometimes acts like dict, sometimes attributes
                last_price = None
                prev_close = None

                try:
                    last_price = fi.get("last_price") if hasattr(fi, "get") else getattr(fi, "last_price", None)
                except Exception:
                    last_price = getattr(fi, "last_price", None)

                try:
                    prev_close = fi.get("previous_close") if hasattr(fi, "get") else getattr(fi, "previous_close", None)
                except Exception:
                    prev_close = getattr(fi, "previous_close", None)

                out["current_price"] = _to_float(last_price)
                out["previous_close"] = _to_float(prev_close)

                try:
                    day_high = fi.get("day_high") if hasattr(fi, "get") else getattr(fi, "day_high", None)
                    day_low = fi.get("day_low") if hasattr(fi, "get") else getattr(fi, "day_low", None)
                    out["day_high"] = _to_float(day_high)
                    out["day_low"] = _to_float(day_low)
                except Exception:
                    pass

                try:
                    vol = fi.get("last_volume") if hasattr(fi, "get") else getattr(fi, "last_volume", None)
                    out["volume"] = _to_float(vol)
                except Exception:
                    pass
        except Exception:
            # ignore fast_info failures
            pass

        # 2) History (also used as fallback for price)
        hist = None
        try:
            hist = ticker.history(period=_hist_period(), interval=_hist_interval(), auto_adjust=False)
        except Exception as e:
            out.setdefault("history_error", str(e))
            hist = None

        closes: List[float] = []
        if hist is not None and getattr(hist, "empty", True) is False:
            # last candle time
            try:
                idx = hist.index
                if len(idx) > 0:
                    ts = idx[-1]
                    # pandas timestamp -> python datetime
                    try:
                        dt = ts.to_pydatetime()
                    except Exception:
                        dt = None
                    if isinstance(dt, datetime):
                        # yfinance can return tz-aware or naive; assume UTC if naive
                        last_dt = dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)
            except Exception:
                last_dt = None

            # closes
            try:
                if "Close" in hist:
                    raw = hist["Close"].tolist()
                    for c in raw:
                        fc = _to_float(c)
                        if fc is not None:
                            closes.append(float(fc))
            except Exception:
                closes = []

            # if missing current_price, use last close
            if out.get("current_price") is None and closes:
                out["current_price"] = float(closes[-1])

            # if missing previous_close, use prior close
            if out.get("previous_close") is None and len(closes) >= 2:
                out["previous_close"] = float(closes[-2])

            # fallback day high/low using last 5 closes (best-effort)
            if closes:
                if out.get("day_high") is None:
                    out["day_high"] = float(max(closes[-5:]))
                if out.get("day_low") is None:
                    out["day_low"] = float(min(closes[-5:]))

            # 52w high/low using history close series
            if closes:
                out["week_52_high"] = float(max(closes[-252:])) if len(closes) >= 252 else float(max(closes))
                out["week_52_low"] = float(min(closes[-252:])) if len(closes) >= 252 else float(min(closes))

            out["_closes"] = closes  # internal for analytics

        return out, last_dt

    # ---------------------------
    # Public fetch (async)
    # ---------------------------
    async def fetch_enriched_quote_patch(self, symbol: str, debug: bool = False) -> Dict[str, Any]:
        if not _configured():
            return {} if _HAS_YFINANCE else ({} if not _emit_warnings() else {"_warn": "yfinance not installed"})

        u_sym = _normalize_symbol(symbol)
        if not u_sym:
            return {}

        ck = f"yq::{u_sym}"
        hit = self._q_cache.get(ck)
        if isinstance(hit, dict) and hit:
            return dict(hit)

        # short error backoff
        if self._e_cache.get(ck):
            return {} if not _emit_warnings() else {"_warn": "yahoo temporarily backed off"}

        if not await self._cb.allow():
            return {} if not _emit_warnings() else {"_warn": "yahoo circuit open"}

        async def _do():
            async with self._sem:
                await self._bucket.acquire()

                try:
                    # yfinance is blocking -> run in thread
                    payload, last_dt = await asyncio.wait_for(
                        asyncio.to_thread(self._blocking_fetch, u_sym),
                        timeout=self._timeout,
                    )

                    if not isinstance(payload, dict):
                        self._e_cache[ck] = "bad_payload"
                        await self._cb.on_failure()
                        return {} if not _emit_warnings() else {"_warn": "yahoo returned bad payload"}

                    if payload.get("error"):
                        self._e_cache[ck] = payload.get("error")
                        await self._cb.on_failure()
                        return {"error": str(payload.get("error"))} if _emit_warnings() else {}

                    cp = _to_float(payload.get("current_price"))
                    if cp is None:
                        self._e_cache[ck] = "no_price"
                        await self._cb.on_failure()
                        return {} if not _emit_warnings() else {"_warn": "yahoo returned no price"}

                    out: Dict[str, Any] = {
                        "requested_symbol": symbol,
                        "symbol": u_sym,
                        "current_price": float(cp),
                        "previous_close": _to_float(payload.get("previous_close")),
                        "day_high": _to_float(payload.get("day_high")),
                        "day_low": _to_float(payload.get("day_low")),
                        "volume": _to_float(payload.get("volume")),
                        "week_52_high": _to_float(payload.get("week_52_high")),
                        "week_52_low": _to_float(payload.get("week_52_low")),
                        "provider": PROVIDER_NAME,
                        "data_source": PROVIDER_NAME,
                        "provider_version": PROVIDER_VERSION,
                        "last_updated_utc": _utc_iso(),
                        "last_updated_riyadh": _riyadh_iso(),
                    }

                    # changes
                    pc = _to_float(out.get("previous_close"))
                    if pc is not None:
                        diff = float(out["current_price"] - pc)
                        out["price_change"] = diff
                        if pc != 0:
                            out["percent_change"] = float((diff / pc) * 100.0)

                    # 52w position
                    w52h = _to_float(out.get("week_52_high"))
                    w52l = _to_float(out.get("week_52_low"))
                    if w52h is not None and w52l is not None and w52h != w52l:
                        out["week_52_position_pct"] = float(((out["current_price"] - w52l) / (w52h - w52l)) * 100.0)

                    # history analytics (from closes)
                    if _enable_history():
                        closes = payload.get("_closes") if isinstance(payload.get("_closes"), list) else []
                        closes = [float(x) for x in closes if _to_float(x) is not None]
                        if closes:
                            analytics = _compute_history_analytics(closes)

                            # forecast timestamps (best-effort from last candle time)
                            if last_dt:
                                out["history_last_utc"] = _utc_iso(last_dt)
                                out["forecast_updated_utc"] = _utc_iso(last_dt)
                                riy = _to_riyadh_iso(last_dt)
                                if riy:
                                    out["forecast_updated_riyadh"] = riy
                            else:
                                out["history_last_utc"] = _utc_iso()
                                out["forecast_updated_utc"] = _utc_iso()
                                out["forecast_updated_riyadh"] = _riyadh_iso()

                            out["history_points"] = len(closes)
                            # add analytics without overwriting quote fields
                            for k, v in analytics.items():
                                if k not in out and v is not None:
                                    out[k] = v

                    out = _clean_patch(out)
                    self._q_cache[ck] = dict(out)
                    await self._cb.on_success()

                    if debug:
                        logger.info("[YahooChart] %s price=%s", u_sym, out.get("current_price"))

                    return out

                except asyncio.TimeoutError:
                    self._e_cache[ck] = "timeout"
                    await self._cb.on_failure()
                    return {} if not _emit_warnings() else {"_warn": "yahoo timeout"}
                except Exception as e:
                    self._e_cache[ck] = f"{e.__class__.__name__}: {e}"
                    await self._cb.on_failure()
                    return {"error": str(e)} if _emit_warnings() else {}

        return await self._sf.run(ck, _do)


# ---------------------------------------------------------------------------
# Lazy singleton access (engine-friendly)
# ---------------------------------------------------------------------------
_PROVIDER_SINGLETON: Optional[YahooChartProvider] = None
_PROVIDER_LOCK = asyncio.Lock()


async def get_yahoo_provider() -> YahooChartProvider:
    global _PROVIDER_SINGLETON
    if _PROVIDER_SINGLETON is None:
        async with _PROVIDER_LOCK:
            if _PROVIDER_SINGLETON is None:
                _PROVIDER_SINGLETON = YahooChartProvider()
    return _PROVIDER_SINGLETON


# ---------------------------------------------------------------------------
# Compatibility aliases expected by your engine
# ---------------------------------------------------------------------------
async def fetch_enriched_quote_patch(symbol: str, debug: bool = False, *args: Any, **kwargs: Any) -> Dict[str, Any]:
    p = await get_yahoo_provider()
    return await p.fetch_enriched_quote_patch(symbol, debug=debug)


async def aclose_yahoo_client() -> None:
    # yfinance manages its own sessions; keep as no-op for compatibility.
    return None


__all__ = [
    "YahooChartProvider",
    "get_yahoo_provider",
    "fetch_enriched_quote_patch",
    "aclose_yahoo_client",
    "PROVIDER_NAME",
    "PROVIDER_VERSION",
]
