#!/usr/bin/env python3
# core/providers/yahoo_fundamentals_provider.py
"""
core/providers/yahoo_fundamentals_provider.py
============================================================
Yahoo Fundamentals Provider (via yfinance) — v3.0.0
(PROD SAFE + ASYNC + ENGINE PATCH-STYLE + ADVANCED FUNDAMENTALS + ANALYST TARGETS)

FULL REPLACEMENT (v3.0.0) — What’s improved vs v1.9.0
- ✅ Import-safe: no network calls at import-time, yfinance import guarded
- ✅ Patch-style output: only useful fields, always Dict[str, Any]
- ✅ Strict key alignment:
    - Identity: name, sector, industry, sub_sector, market, currency, exchange, country
    - Fundamentals: market_cap, shares_outstanding, pe_ttm, forward_pe, pb, ps, eps_ttm, forward_eps, beta
    - Profitability & growth: roe, roa, profit_margin, revenue_growth
    - Balance sheet / cash flow: total_debt, ebitda, free_cashflow
    - Analyst: recommendation, analyst_count, forecast_price_12m, expected_roi_12m, target_mean_price
- ✅ Riyadh localization:
    last_updated_riyadh + forecast_updated_riyadh (UTC+3)
- ✅ Resilience:
    - Uses yfinance Ticker.info first, then fast_info for last price fallback
    - Handles Yahoo’s common empty-info failure mode cleanly
    - Safe float coercion + % conversion (decimal->pct)
- ✅ Performance & safety:
    - Lazy singleton provider + concurrency limit (YF_MAX_CONCURRENCY)
    - Optional token-bucket rate limiter (YF_RATE_LIMIT_PER_SEC)
    - Circuit breaker (YF_CIRCUIT_BREAKER, threshold + cooldown)
    - Singleflight (anti-stampede): concurrent calls for same symbol share one fetch
    - TTL caches for fundamentals + short error backoff cache
- ✅ Engine-compat exports:
    fetch_fundamentals_patch(), fetch_enriched_quote_patch() aliases

Environment variables
- YF_ENABLED=true/false (default true)
- YF_TIMEOUT_SEC (default 20)
- YF_MAX_CONCURRENCY (default 10)
- YF_RATE_LIMIT_PER_SEC (default 0; 0 disables)
- YF_CIRCUIT_BREAKER=true/false (default true)
- YF_CB_FAIL_THRESHOLD (default 6)
- YF_CB_COOLDOWN_SEC (default 30)

Caches
- YF_FUND_TTL_SEC (default 21600)  # 6h
- YF_ERROR_TTL_SEC (default 10)

Toggles
- YF_VERBOSE_WARNINGS=true/false (default false)

Notes
- Yahoo recommendationKey is normalized to BUY/HOLD/REDUCE/SELL best-effort
- forecast_confidence is returned as 0..1 (float), confidence_score as 0..100 (float)
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
from typing import Any, Dict, Optional, Tuple

logger = logging.getLogger("core.providers.yahoo_fundamentals_provider")

PROVIDER_NAME = "yahoo_fundamentals"
PROVIDER_VERSION = "3.0.0"

_TRUTHY = {"1", "true", "yes", "y", "on", "t"}
_FALSY = {"0", "false", "no", "n", "off", "f"}

_KSA_CODE_RE = re.compile(r"^\d{3,6}$", re.IGNORECASE)
_ARABIC_DIGITS = str.maketrans("٠١٢٣٤٥٦٧٨٩", "0123456789")

# Optional import (keeps module import-safe)
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
    if not _env_bool("YF_ENABLED", True):
        return False
    return _HAS_YFINANCE


def _emit_warnings() -> bool:
    return _env_bool("YF_VERBOSE_WARNINGS", False)


def _timeout_sec() -> float:
    return max(5.0, _env_float("YF_TIMEOUT_SEC", 20.0))


def _fund_ttl_sec() -> float:
    return max(60.0, _env_float("YF_FUND_TTL_SEC", 21600.0))


def _err_ttl_sec() -> float:
    return max(2.0, _env_float("YF_ERROR_TTL_SEC", 10.0))


def _max_concurrency() -> int:
    return max(2, _env_int("YF_MAX_CONCURRENCY", 10))


def _rate_limit_per_sec() -> float:
    return max(0.0, _env_float("YF_RATE_LIMIT_PER_SEC", 0.0))


def _cb_enabled() -> bool:
    return _env_bool("YF_CIRCUIT_BREAKER", True)


def _cb_fail_threshold() -> int:
    return max(2, _env_int("YF_CB_FAIL_THRESHOLD", 6))


def _cb_cooldown_sec() -> float:
    return max(5.0, _env_float("YF_CB_COOLDOWN_SEC", 30.0))


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
    if x is None:
        return None
    try:
        if isinstance(x, (int, float)):
            f = float(x)
            if math.isnan(f) or math.isinf(f):
                return None
            return f
        s = str(x).strip()
        if not s:
            return None
        s = s.translate(_ARABIC_DIGITS)
        s = s.replace(",", "").replace("%", "").replace("$", "").strip()
        if s.lower() in ("n/a", "na", "null", "none", "-", "--"):
            return None
        if s.startswith("(") and s.endswith(")"):
            s = "-" + s[1:-1].strip()
        f = float(s)
        if math.isnan(f) or math.isinf(f):
            return None
        return f
    except Exception:
        return None


def _as_pct_decimal_to_pct(x: Any) -> Optional[float]:
    """
    Yahoo often returns decimals (0.05) representing 5%.
    We convert only when value looks like a fraction.
    """
    v = _to_float(x)
    if v is None:
        return None
    # convert to pct if abs <= 1.5 (covers 0.05, 0.2, 1.0)
    return float(v * 100.0) if abs(v) <= 1.5 else float(v)


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

    if _KSA_CODE_RE.match(s):
        return f"{s}.SR"
    if s.endswith(".SR") and _KSA_CODE_RE.match(s[:-3]):
        return s

    return s


def _map_recommendation(rec_key: Optional[str]) -> str:
    """
    Yahoo recommendationKey examples:
      "strong_buy","buy","hold","underperform","sell","strong_sell"
    We map to BUY/HOLD/REDUCE/SELL.
    """
    if not rec_key:
        return "HOLD"
    k = str(rec_key).strip().lower()
    if "strong_buy" in k or k == "buy":
        return "BUY"
    if "underperform" in k or "reduce" in k:
        return "REDUCE"
    if "sell" in k:
        return "SELL"
    return "HOLD"


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
# Provider (singleton)
# ---------------------------------------------------------------------------
@dataclass
class YahooFundamentalsProvider:
    name: str = PROVIDER_NAME

    def __post_init__(self) -> None:
        self._timeout = _timeout_sec()
        self._sem = asyncio.Semaphore(_max_concurrency())
        self._bucket = _TokenBucket(_rate_limit_per_sec())
        self._cb = _CircuitBreaker(_cb_enabled(), _cb_fail_threshold(), _cb_cooldown_sec())
        self._sf = _SingleFlight()

        self._f_cache: TTLCache = TTLCache(maxsize=5000, ttl=_fund_ttl_sec())
        self._e_cache: TTLCache = TTLCache(maxsize=5000, ttl=_err_ttl_sec())

        logger.info(
            "YahooFundamentalsProvider init v%s | yfinance=%s | timeout=%.1fs | max_conc=%s | rate/s=%.2f | cb=%s",
            PROVIDER_VERSION,
            _HAS_YFINANCE,
            self._timeout,
            _max_concurrency(),
            _rate_limit_per_sec(),
            _cb_enabled(),
        )

    # ---------------------------
    # Blocking fetch (runs in thread)
    # ---------------------------
    def _blocking_fetch_info(self, sym: str) -> Dict[str, Any]:
        if not _HAS_YFINANCE or yf is None:
            return {"error": "yfinance not installed"}

        t = yf.Ticker(sym)

        # Primary: info dict
        info: Dict[str, Any] = {}
        try:
            info = t.info or {}
        except Exception as e:
            info = {"_info_error": f"{e.__class__.__name__}: {e}"}

        # Fallback: fast_info for current price if needed
        fast_price = None
        try:
            fi = getattr(t, "fast_info", None)
            if fi:
                try:
                    fast_price = fi.get("last_price") if hasattr(fi, "get") else getattr(fi, "last_price", None)
                except Exception:
                    fast_price = getattr(fi, "last_price", None)
        except Exception:
            fast_price = None

        if isinstance(info, dict) and fast_price is not None:
            info.setdefault("_fast_last_price", fast_price)

        return info if isinstance(info, dict) else {}

    # ---------------------------
    # Public fetch (async)
    # ---------------------------
    async def fetch_fundamentals_patch(self, symbol: str, debug: bool = False) -> Dict[str, Any]:
        if not _configured():
            return {} if _HAS_YFINANCE else ({} if not _emit_warnings() else {"_warn": "yfinance not installed"})

        u_sym = _normalize_symbol(symbol)
        if not u_sym:
            return {}

        ck = f"yf::{u_sym}"
        hit = self._f_cache.get(ck)
        if isinstance(hit, dict) and hit:
            return dict(hit)

        # short error backoff
        if self._e_cache.get(ck):
            return {} if not _emit_warnings() else {"_warn": "yahoo fundamentals temporarily backed off"}

        if not await self._cb.allow():
            return {} if not _emit_warnings() else {"_warn": "yahoo fundamentals circuit open"}

        async def _do():
            async with self._sem:
                await self._bucket.acquire()

                try:
                    info = await asyncio.wait_for(
                        asyncio.to_thread(self._blocking_fetch_info, u_sym),
                        timeout=self._timeout,
                    )

                    if not isinstance(info, dict) or not info:
                        self._e_cache[ck] = "empty_info"
                        await self._cb.on_failure()
                        return {} if not _emit_warnings() else {"_warn": "empty fundamentals info"}

                    # Common failure mode: info is present but useless
                    if ("symbol" not in info and "shortName" not in info and "longName" not in info and
                        "regularMarketPrice" not in info and "currentPrice" not in info and "_fast_last_price" not in info):
                        self._e_cache[ck] = "no_useful_keys"
                        await self._cb.on_failure()
                        return {} if not _emit_warnings() else {"_warn": "no useful fundamentals keys returned"}

                    # Identity
                    name = info.get("longName") or info.get("shortName")
                    sector = info.get("sector")
                    industry = info.get("industry")
                    sub_sector = industry  # your controller expects sub_sector; keep same mapping

                    # Market/currency
                    currency = info.get("currency") or info.get("financialCurrency")
                    market = info.get("market")
                    exchange = info.get("exchange")
                    country = info.get("country")

                    # Financial metrics
                    market_cap = _to_float(info.get("marketCap"))
                    pe_ttm = _to_float(info.get("trailingPE"))
                    forward_pe = _to_float(info.get("forwardPE"))
                    pb = _to_float(info.get("priceToBook"))
                    ps = _to_float(info.get("priceToSalesTrailing12Months"))
                    eps_ttm = _to_float(info.get("trailingEps"))
                    forward_eps = _to_float(info.get("forwardEps"))
                    beta = _to_float(info.get("beta"))
                    shares_outstanding = _to_float(info.get("sharesOutstanding"))

                    ebitda = _to_float(info.get("ebitda"))
                    total_debt = _to_float(info.get("totalDebt"))
                    free_cashflow = _to_float(info.get("freeCashflow"))
                    total_revenue = _to_float(info.get("totalRevenue"))

                    # % fields
                    dividend_yield = _as_pct_decimal_to_pct(info.get("dividendYield"))
                    dividend_rate = _to_float(info.get("dividendRate"))
                    roe = _as_pct_decimal_to_pct(info.get("returnOnEquity"))
                    roa = _as_pct_decimal_to_pct(info.get("returnOnAssets"))
                    profit_margin = _as_pct_decimal_to_pct(info.get("profitMargins"))
                    revenue_growth = _as_pct_decimal_to_pct(info.get("revenueGrowth"))
                    payout_ratio = _as_pct_decimal_to_pct(info.get("payoutRatio"))

                    # Analyst targets
                    rec_key = info.get("recommendationKey")
                    recommendation = _map_recommendation(rec_key)
                    analyst_count = _to_float(info.get("numberOfAnalystOpinions"))

                    # price / target
                    current_price = (
                        _to_float(info.get("currentPrice"))
                        or _to_float(info.get("regularMarketPrice"))
                        or _to_float(info.get("_fast_last_price"))
                    )
                    target_mean = _to_float(info.get("targetMeanPrice"))

                    out: Dict[str, Any] = {
                        "requested_symbol": symbol,
                        "symbol": u_sym,
                        "name": _safe_str(name),
                        "sector": _safe_str(sector),
                        "industry": _safe_str(industry),
                        "sub_sector": _safe_str(sub_sector),
                        "market": _safe_str(market),
                        "currency": _safe_str(currency),
                        "exchange": _safe_str(exchange),
                        "country": _safe_str(country),

                        "market_cap": market_cap,
                        "shares_outstanding": shares_outstanding,
                        "pe_ttm": pe_ttm,
                        "forward_pe": forward_pe,
                        "pb": pb,
                        "ps": ps,
                        "eps_ttm": eps_ttm,
                        "forward_eps": forward_eps,
                        "beta": beta,

                        "ebitda": ebitda,
                        "total_debt": total_debt,
                        "free_cashflow": free_cashflow,
                        "total_revenue": total_revenue,

                        "dividend_yield": dividend_yield,
                        "dividend_rate": dividend_rate,
                        "roe": roe,
                        "roa": roa,
                        "profit_margin": profit_margin,
                        "net_margin": profit_margin,  # alias
                        "revenue_growth": revenue_growth,
                        "payout_ratio": payout_ratio,

                        "recommendation": recommendation,
                        "analyst_count": analyst_count,
                        "target_mean_price": target_mean,

                        "provider": PROVIDER_NAME,
                        "data_source": PROVIDER_NAME,
                        "provider_version": PROVIDER_VERSION,
                        "last_updated_utc": _utc_iso(),
                        "last_updated_riyadh": _riyadh_iso(),
                    }

                    # Forecast 12M based on analyst target (aligned keys)
                    if current_price is not None and target_mean is not None and current_price != 0:
                        roi = float(((target_mean / current_price) - 1.0) * 100.0)
                        out["forecast_price_12m"] = float(target_mean)
                        out["expected_roi_12m"] = float(roi)
                        out["upside_percent"] = float(roi)  # alias
                        out["forecast_method"] = "analyst_consensus"

                        # confidence: base + analysts effect (bounded)
                        conf = 0.60
                        if analyst_count and analyst_count > 0:
                            # log-based lift, capped
                            conf = min(0.95, 0.60 + (math.log(float(analyst_count) + 1.0) * 0.05))
                        out["forecast_confidence"] = float(conf)              # 0..1
                        out["confidence_score"] = float(conf * 100.0)         # 0..100
                    else:
                        out["forecast_method"] = "fundamentals_only"
                        out["forecast_confidence"] = 0.0
                        out["confidence_score"] = 0.0

                    # forecast timestamps always present to keep sheet stable
                    out["forecast_updated_utc"] = _utc_iso()
                    out["forecast_updated_riyadh"] = _riyadh_iso()

                    out = _clean_patch(out)
                    self._f_cache[ck] = dict(out)
                    await self._cb.on_success()

                    if debug:
                        logger.info("[YahooFund] %s name=%s sector=%s", u_sym, out.get("name"), out.get("sector"))

                    return out

                except asyncio.TimeoutError:
                    self._e_cache[ck] = "timeout"
                    await self._cb.on_failure()
                    return {} if not _emit_warnings() else {"_warn": "yahoo fundamentals timeout"}
                except Exception as e:
                    self._e_cache[ck] = f"{e.__class__.__name__}: {e}"
                    await self._cb.on_failure()
                    return {"error": str(e)} if _emit_warnings() else {}

        # Singleflight to avoid duplicate network calls
        return await self._sf.run(ck, _do)


# ---------------------------------------------------------------------------
# Lazy singleton access
# ---------------------------------------------------------------------------
_PROVIDER_SINGLETON: Optional[YahooFundamentalsProvider] = None
_PROVIDER_LOCK = asyncio.Lock()


async def get_yahoo_fundamentals_provider() -> YahooFundamentalsProvider:
    global _PROVIDER_SINGLETON
    if _PROVIDER_SINGLETON is None:
        async with _PROVIDER_LOCK:
            if _PROVIDER_SINGLETON is None:
                _PROVIDER_SINGLETON = YahooFundamentalsProvider()
    return _PROVIDER_SINGLETON


async def aclose_yahoo_fundamentals_client() -> None:
    # no-op (yfinance session managed internally)
    return None


# ---------------------------------------------------------------------------
# Engine-compatible exported callables
# ---------------------------------------------------------------------------
async def fetch_fundamentals_patch(symbol: str, debug: bool = False, *args: Any, **kwargs: Any) -> Dict[str, Any]:
    p = await get_yahoo_fundamentals_provider()
    return await p.fetch_fundamentals_patch(symbol, debug=debug)


async def fetch_enriched_quote_patch(symbol: str, debug: bool = False, *args: Any, **kwargs: Any) -> Dict[str, Any]:
    # compatibility alias (some engines call fundamentals via enriched)
    return await fetch_fundamentals_patch(symbol, debug=debug)


__all__ = [
    "YahooFundamentalsProvider",
    "get_yahoo_fundamentals_provider",
    "fetch_fundamentals_patch",
    "fetch_enriched_quote_patch",
    "aclose_yahoo_fundamentals_client",
    "PROVIDER_NAME",
    "PROVIDER_VERSION",
]
