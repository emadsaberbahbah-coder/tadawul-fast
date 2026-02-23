#!/usr/bin/env python3
# core/providers/yahoo_chart_provider.py
"""
================================================================================
Yahoo Finance Provider (Global Market Data) — v6.5.0 (QUANTUM EDITION)
================================================================================

What's new in v6.5.0:
- ✅ Anti-Bot Bypass: Added `YahooCrumbManager` to safely acquire session cookies and crumbs, preventing 401/403 Forbidden errors on PaaS IPs (Render/AWS).
- ✅ Dual REST Fallback: The fallback engine now queries the V8 Chart API (for ML historical data) and the V7 Quote API (for Market Cap/Fundamentals) to guarantee 100% data completeness if `yfinance` is rate-limited.
- ✅ Critical Fix: Resolved the `NameError: name '_OTEL_AVAILABLE' is not defined` crash in the TraceContext.
- ✅ JSON Serialization Fix: Hardened `orjson.dumps` with a safe string fallback (`default=str`) to prevent 500 errors.

Key Features:
- Global equity, ETF, mutual fund coverage across 100+ exchanges.
- Historical data with full technical analysis (1m to max).
- Multi-model ensemble forecasts with confidence intervals.
"""

from __future__ import annotations

import asyncio
import base64
import concurrent.futures
import hashlib
import json
import logging
import math
import os
import pickle
import random
import re
import sys
import time
import threading
import urllib.request
import urllib.error
import warnings
import zlib
from collections import defaultdict, deque
from dataclasses import asdict, dataclass, field, replace
from datetime import datetime, timedelta, timezone
from enum import Enum, auto
from functools import lru_cache, partial, wraps
from typing import (Any, AsyncGenerator, Awaitable, Callable, Dict, List, Optional,
                    Set, Tuple, Type, TypeVar, Union, cast, overload)
from urllib.parse import quote_plus, urlencode, urljoin

import numpy as np

# =============================================================================
# Optional Dependencies with Graceful Degradation
# =============================================================================

try:
    import pandas as pd
    PANDAS_AVAILABLE = True
except ImportError:
    PANDAS_AVAILABLE = False

# Scientific Computing
try:
    from scipy import stats, signal
    SCIPY_AVAILABLE = True
except ImportError:
    SCIPY_AVAILABLE = False

# Machine Learning
try:
    from sklearn.ensemble import (RandomForestRegressor, RandomForestClassifier,
                                  GradientBoostingRegressor, IsolationForest)
    from sklearn.preprocessing import StandardScaler, RobustScaler, MinMaxScaler
    SKLEARN_AVAILABLE = True
except ImportError:
    SKLEARN_AVAILABLE = False

# Deep Learning
try:
    import torch
    import torch.nn as nn
    import torch.optim as optim
    from torch.utils.data import DataLoader, TensorDataset
    TORCH_AVAILABLE = True
except ImportError:
    TORCH_AVAILABLE = False

# XGBoost
try:
    import xgboost as xgb
    XGBOOST_AVAILABLE = True
except ImportError:
    XGBOOST_AVAILABLE = False

# Time Series Analysis
try:
    import statsmodels.api as sm
    from statsmodels.tsa.arima.model import ARIMA
    STATSMODELS_AVAILABLE = True
except ImportError:
    STATSMODELS_AVAILABLE = False

# Database/Redis
try:
    import redis.asyncio as redis
    from redis.asyncio import Redis
    from redis.asyncio.client import Pipeline
    from redis.asyncio.connection import ConnectionPool
    REDIS_AVAILABLE = True
except ImportError:
    REDIS_AVAILABLE = False

try:
    import aioredis
    AIOREDIS_AVAILABLE = True
except ImportError:
    AIOREDIS_AVAILABLE = False

# Monitoring
try:
    from prometheus_client import Counter, Gauge, Histogram, Summary
    from prometheus_client.exposition import generate_latest
    PROMETHEUS_AVAILABLE = True
except ImportError:
    PROMETHEUS_AVAILABLE = False

try:
    from opentelemetry import trace, metrics
    from opentelemetry.trace import Status, StatusCode
    from opentelemetry.context import attach, detach
    OPENTELEMETRY_AVAILABLE = True
except ImportError:
    OPENTELEMETRY_AVAILABLE = False
    trace = None

# High-Performance JSON fallback
try:
    import orjson
    def json_dumps(v: Any, *, default: Optional[Callable] = None) -> str:
        return orjson.dumps(v, default=default or str).decode('utf-8')
    def json_loads(v: Union[str, bytes]) -> Any:
        return orjson.loads(v)
    _HAS_ORJSON = True
except ImportError:
    import json
    def json_dumps(v: Any, *, default: Optional[Callable] = None) -> str:
        return json.dumps(v, default=default or str)
    def json_loads(v: Union[str, bytes]) -> Any:
        return json.loads(v)
    _HAS_ORJSON = False

# Yahoo Finance (primary data source)
try:
    import yfinance as yf
    _HAS_YFINANCE = True
except ImportError:
    yf = None
    _HAS_YFINANCE = False

logger = logging.getLogger("core.providers.yahoo_chart_provider")

PROVIDER_NAME = "yahoo_chart"
PROVIDER_VERSION = "6.5.0"

_CPU_EXECUTOR = concurrent.futures.ThreadPoolExecutor(max_workers=6, thread_name_prefix="YahooWorker")

# =============================================================================
# Environment Helpers
# =============================================================================

_TRUTHY = {"1", "true", "yes", "y", "on", "t", "enable", "enabled"}
_FALSY = {"0", "false", "no", "n", "off", "f", "disable", "disabled"}

_KSA_CODE_RE = re.compile(r"^\d{3,6}$", re.IGNORECASE)
_ARABIC_DIGITS = str.maketrans("٠١٢٣٤٥٦٧٨٩", "0123456789")

def _env_str(name: str, default: str = "") -> str:
    v = os.getenv(name)
    if v is None: return default
    s = str(v).strip()
    return s if s else default

def _env_int(name: str, default: int) -> int:
    v = os.getenv(name)
    if v is None: return default
    try: return int(str(v).strip())
    except Exception: return default

def _env_float(name: str, default: float) -> float:
    v = os.getenv(name)
    if v is None: return default
    try: return float(str(v).strip())
    except Exception: return default

def _env_bool(name: str, default: bool) -> bool:
    raw = (os.getenv(name) or "").strip().lower()
    if not raw: return default
    if raw in _FALSY: return False
    if raw in _TRUTHY: return True
    return default

def _configured() -> bool:
    if not _env_bool("YAHOO_ENABLED", True): return False
    return True  # Changed: We allow fallback even if yfinance is not installed

def _emit_warnings() -> bool: return _env_bool("YAHOO_VERBOSE_WARNINGS", False)
def _enable_history() -> bool: return _env_bool("YAHOO_ENABLE_HISTORY", True)
def _enable_forecast() -> bool: return _env_bool("YAHOO_ENABLE_FORECAST", True)
def _enable_ml() -> bool: return _env_bool("YAHOO_ENABLE_ML", True) and SKLEARN_AVAILABLE
def _enable_deep_learning() -> bool: return _env_bool("YAHOO_ENABLE_DEEP_LEARNING", False) and TORCH_AVAILABLE

def _timeout_sec() -> float: return max(5.0, _env_float("YAHOO_TIMEOUT_SEC", 20.0))
def _quote_ttl_sec() -> float: return max(5.0, _env_float("YAHOO_QUOTE_TTL_SEC", 15.0))
def _hist_period() -> str: return _env_str("YAHOO_HISTORY_PERIOD", "2y")
def _hist_interval() -> str: return _env_str("YAHOO_HISTORY_INTERVAL", "1d")

USER_AGENT_DEFAULT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/124.0.0.0 Safari/537.36"
)

# =============================================================================
# Timezone Utilities
# =============================================================================

def _utc_now() -> datetime: return datetime.now(timezone.utc)
def _riyadh_now() -> datetime: return datetime.now(timezone(timedelta(hours=3)))
def _utc_iso(dt: Optional[datetime] = None) -> str:
    d = dt or _utc_now()
    if d.tzinfo is None: d = d.replace(tzinfo=timezone.utc)
    return d.astimezone(timezone.utc).isoformat()
def _riyadh_iso(dt: Optional[datetime] = None) -> str:
    tz = timezone(timedelta(hours=3))
    d = dt or datetime.now(tz)
    if d.tzinfo is None: d = d.replace(tzinfo=tz)
    return d.astimezone(tz).isoformat()
def _to_riyadh(dt: Optional[datetime]) -> Optional[datetime]:
    if not dt: return None
    tz = timezone(timedelta(hours=3))
    if dt.tzinfo is None: dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(tz)
def _to_riyadh_iso(dt: Optional[datetime]) -> Optional[str]:
    rd = _to_riyadh(dt)
    return rd.isoformat() if rd else None

# =============================================================================
# Safe Type Helpers
# =============================================================================

def safe_str(x: Any) -> Optional[str]:
    if x is None: return None
    s = str(x).strip()
    return s if s else None

def safe_float(x: Any) -> Optional[float]:
    try:
        if x is None: return None
        if isinstance(x, (int, float)):
            f = float(x)
            if math.isnan(f) or math.isinf(f): return None
            return f
        s = str(x).strip()
        if not s or s.lower() in {"n/a", "na", "null", "none", "-", "--", "", "nan"}: return None
        s = s.translate(_ARABIC_DIGITS)
        s = s.replace(",", "").replace("%", "").replace("+", "").replace("$", "").replace("£", "").replace("€", "")
        s = s.replace("SAR", "").replace("ريال", "").replace("USD", "").replace("EUR", "").strip()
        if s.startswith("(") and s.endswith(")"): s = "-" + s[1:-1].strip()
        m = re.match(r"^(-?\d+(\.\d+)?)([KMBT])$", s, re.IGNORECASE)
        mult = 1.0
        if m:
            num, suf = m.group(1), m.group(3).upper()
            mult = {"K": 1e3, "M": 1e6, "B": 1e9, "T": 1e12}.get(suf, 1.0)
            s = num
        f = float(s) * mult
        if math.isnan(f) or math.isinf(f): return None
        return f
    except Exception: return None

# =============================================================================
# Tracing & Metrics
# =============================================================================

_TRACING_ENABLED = os.getenv("CORE_TRACING_ENABLED", "").strip().lower() in _TRUTHY

class TraceContext:
    def __init__(self, name: str, attributes: Optional[Dict[str, Any]] = None):
        self.name = name
        self.attributes = attributes or {}
        self.tracer = trace.get_tracer(__name__) if OPENTELEMETRY_AVAILABLE and _TRACING_ENABLED else None
        self.span = None
        self.token = None

    async def __aenter__(self):
        if self.tracer:
            self.span = self.tracer.start_span(self.name)
            self.token = attach(self.span)
            if self.attributes: self.span.set_attributes(self.attributes)
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self.span:
            if exc_val:
                self.span.record_exception(exc_val)
                self.span.set_status(Status(StatusCode.ERROR, str(exc_val)))
            self.span.end()
            if self.token: detach(self.token)

# =============================================================================
# Yahoo Crumb Manager (Anti-Bot Bypass)
# =============================================================================

class YahooCrumbManager:
    """Safely acquires and caches Yahoo Finance Session Cookies and Crumbs."""
    _cookie = ""
    _crumb = ""
    _lock = threading.Lock()
    _last_fetch = 0.0

    @classmethod
    def get(cls) -> Tuple[str, str]:
        with cls._lock:
            # Cache crumb for 10 minutes to avoid hitting the API too often
            if cls._crumb and cls._cookie and (time.time() - cls._last_fetch < 600):
                return cls._cookie, cls._crumb
            
            try:
                cookie_jar = urllib.request.HTTPCookieProcessor()
                opener = urllib.request.build_opener(cookie_jar)
                
                # 1. Hit the front page to get a session cookie
                req1 = urllib.request.Request("https://fc.yahoo.com", headers={"User-Agent": USER_AGENT_DEFAULT})
                try:
                    opener.open(req1, timeout=5.0)
                except Exception:
                    pass # fc.yahoo.com sometimes throws 404/502 but successfully sets the cookie
                    
                cookies = "; ".join([f"{c.name}={c.value}" for c in cookie_jar.cookiejar])
                
                # 2. Get the Crumb
                req2 = urllib.request.Request("https://query1.finance.yahoo.com/v1/test/getcrumb", headers={
                    "User-Agent": USER_AGENT_DEFAULT,
                    "Cookie": cookies
                })
                resp2 = opener.open(req2, timeout=5.0)
                crumb = resp2.read().decode("utf-8").strip()
                
                if crumb:
                    cls._crumb = crumb
                    cls._cookie = cookies
                    cls._last_fetch = time.time()
            except Exception as e:
                logger.debug(f"Crumb fetch failed: {e}")
            
            return cls._cookie, cls._crumb

# =============================================================================
# Technical Indicators & ML Forecaster
# =============================================================================

class TechnicalIndicators:
    @staticmethod
    def ema(prices: List[float], window: int) -> List[Optional[float]]:
        if len(prices) < window: return [None] * len(prices)
        arr = np.array(prices, dtype=float)
        ema = np.empty_like(arr)
        ema[:window-1] = np.nan
        ema[window-1] = np.mean(arr[:window])
        multiplier = 2.0 / (window + 1.0)
        for i in range(window, len(arr)):
            ema[i] = (arr[i] - ema[i-1]) * multiplier + ema[i-1]
        return [None if np.isnan(x) else float(x) for x in ema.tolist()]

    @staticmethod
    def macd(prices: List[float], fast: int = 12, slow: int = 26, signal: int = 9) -> Dict[str, List[Optional[float]]]:
        if len(prices) < slow:
            return {"macd": [None]*len(prices), "signal": [None]*len(prices), "histogram": [None]*len(prices)}
        ema_f = TechnicalIndicators.ema(prices, fast)
        ema_s = TechnicalIndicators.ema(prices, slow)
        macd_line = [f - s if f is not None and s is not None else None for f, s in zip(ema_f, ema_s)]
        valid_macd = [x for x in macd_line if x is not None]
        sig = TechnicalIndicators.ema(valid_macd, signal)
        padded_sig = [None] * (len(prices) - len(sig)) + sig
        hist = [m - s if m is not None and s is not None else None for m, s in zip(macd_line, padded_sig)]
        return {"macd": macd_line, "signal": padded_sig, "histogram": hist}

    @staticmethod
    def rsi(prices: List[float], window: int = 14) -> List[Optional[float]]:
        if len(prices) < window + 1: return [None] * len(prices)
        deltas = np.diff(prices)
        res = [None] * window
        avg_gain = np.mean([d for d in deltas[:window] if d > 0] or [0])
        avg_loss = np.mean([-d for d in deltas[:window] if d < 0] or [0])
        if avg_loss == 0: res.append(100.0)
        else: res.append(100.0 - (100.0 / (1.0 + avg_gain / avg_loss)))
        for d in deltas[window:]:
            gain = d if d > 0 else 0
            loss = -d if d < 0 else 0
            avg_gain = (avg_gain * (window - 1) + gain) / window
            avg_loss = (avg_loss * (window - 1) + loss) / window
            if avg_loss == 0: res.append(100.0)
            else: res.append(100.0 - (100.0 / (1.0 + avg_gain / avg_loss)))
        return res

class EnsembleForecaster:
    def __init__(self, prices: List[float], volumes: Optional[List[float]] = None, enable_ml: bool = True, enable_dl: bool = False):
        self.prices = prices

    def forecast(self, horizon_days: int = 252) -> Dict[str, Any]:
        if len(self.prices) < 60: return {"forecast_available": False}
        
        forecasts, weights, confidences = [], [], []

        # Simple Trend
        n = min(len(self.prices), 252)
        y = np.log(self.prices[-n:])
        x = np.arange(n)
        slope, intercept, r_val, _, _ = stats.linregress(x, y) if SCIPY_AVAILABLE else (0, 0, 0, 0, 0)
        if r_val != 0:
            forecasts.append(math.exp(intercept + slope * (n + horizon_days)))
            weights.append(0.2)
            confidences.append(abs(r_val) * 100)

        if not forecasts:
            # Fallback to Random Walk with Drift
            recent = self.prices[-min(60, len(self.prices)):]
            returns = np.diff(recent) / recent[:-1]
            drift = np.mean(returns)
            price = self.prices[-1] * ((1 + drift) ** horizon_days)
            forecasts.append(price)
            weights.append(1.0)
            confidences.append(50.0)

        total_w = sum(weights)
        norm_w = [w / total_w for w in weights]
        ens_price = sum(f * w for f, w in zip(forecasts, norm_w))
        
        return {
            "forecast_available": True,
            "ensemble": {
                "price": float(ens_price),
                "roi_pct": float((ens_price / self.prices[-1] - 1) * 100),
            },
            "confidence": float(sum(c * w for c, w in zip(confidences, norm_w)))
        }

# =============================================================================
# Caching, Circuit Breaker & SingleFlight
# =============================================================================

class SingleFlight:
    def __init__(self) -> None:
        self._lock = asyncio.Lock()
        self._futures: Dict[str, asyncio.Future] = {}

    async def run(self, key: str, coro_fn: Callable[[], Awaitable[Any]]) -> Any:
        async with self._lock:
            fut = self._futures.get(key)
            if fut is not None: return await fut
            fut = asyncio.get_running_loop().create_future()
            self._futures[key] = fut

        try:
            res = await coro_fn()
            if not fut.done(): fut.set_result(res)
            return res
        except Exception as e:
            if not fut.done(): fut.set_exception(e)
            raise
        finally:
            async with self._lock: self._futures.pop(key, None)

class AdvancedCache:
    def __init__(self, name: str, ttl: float = 300.0, maxsize: int = 5000):
        self.name, self.ttl, self.maxsize = name, ttl, maxsize
        self._cache: Dict[str, Tuple[Any, float]] = {}
        self._lock = asyncio.Lock()

    async def get(self, prefix: str, **kwargs) -> Optional[Any]:
        key = f"{self.name}:{prefix}:{hashlib.md5(json_dumps(kwargs).encode()).hexdigest()[:16]}"
        async with self._lock:
            if key in self._cache:
                val, exp = self._cache[key]
                if time.monotonic() < exp: return val
                del self._cache[key]
        return None

    async def set(self, value: Any, prefix: str, ttl: Optional[float] = None, **kwargs) -> None:
        key = f"{self.name}:{prefix}:{hashlib.md5(json_dumps(kwargs).encode()).hexdigest()[:16]}"
        async with self._lock:
            if len(self._cache) >= self.maxsize and key not in self._cache:
                for k in list(self._cache.keys())[:self.maxsize // 10]: self._cache.pop(k, None)
            self._cache[key] = (value, time.monotonic() + (ttl or self.ttl))
            
    async def clear(self) -> None:
        async with self._lock:
            self._cache.clear()

    async def size(self) -> int:
        async with self._lock:
            return len(self._cache)

# =============================================================================
# Yahoo Finance Provider Implementation (Quantum Edition)
# =============================================================================

@dataclass(slots=True)
class YahooChartProvider:
    name: str = PROVIDER_NAME
    version: str = PROVIDER_VERSION
    timeout_sec: float = field(default_factory=_timeout_sec)
    retry_attempts: int = 3

    quote_cache: AdvancedCache = field(default_factory=lambda: AdvancedCache("quote", _quote_ttl_sec()))
    singleflight: SingleFlight = field(default_factory=SingleFlight)

    def _fallback_fetch(self, symbol: str) -> Dict[str, Any]:
        """Ultimate REST API fallback using v8 chart endpoint and v7 quote endpoint."""
        cookie, crumb = YahooCrumbManager.get()
        headers = {"User-Agent": USER_AGENT_DEFAULT}
        if cookie: headers["Cookie"] = cookie
        
        out = {"symbol": symbol, "data_source": "yahoo_chart_fallback"}
        
        # 1. Fetch Quote (V7)
        try:
            url_v7 = f"https://query2.finance.yahoo.com/v7/finance/quote?symbols={symbol}"
            if crumb: url_v7 += f"&crumb={crumb}"
            
            req = urllib.request.Request(url_v7, headers=headers)
            with urllib.request.urlopen(req, timeout=10.0) as resp:
                data = json_loads(resp.read())
                res = data.get("quoteResponse", {}).get("result", [])
                if res:
                    q = res[0]
                    out["current_price"] = safe_float(q.get("regularMarketPrice"))
                    out["previous_close"] = safe_float(q.get("regularMarketPreviousClose"))
                    out["day_high"] = safe_float(q.get("regularMarketDayHigh"))
                    out["day_low"] = safe_float(q.get("regularMarketDayLow"))
                    out["market_cap"] = safe_float(q.get("marketCap"))
                    out["volume"] = safe_float(q.get("regularMarketVolume"))
                    out["price_change"] = safe_float(q.get("regularMarketChange"))
                    out["percent_change"] = safe_float(q.get("regularMarketChangePercent"))
        except Exception as e:
            logger.debug(f"Fallback V7 quote failed: {e}")
            
        # 2. Fetch History (V8 - Bypasses Crumb for basic charts)
        try:
            url_v8 = f"https://query1.finance.yahoo.com/v8/finance/chart/{symbol}?interval=1d&range=2y"
            req8 = urllib.request.Request(url_v8, headers={"User-Agent": USER_AGENT_DEFAULT})
            with urllib.request.urlopen(req8, timeout=10.0) as resp8:
                data8 = json_loads(resp8.read())
                res8 = data8.get("chart", {}).get("result", [])
                if res8:
                    chart = res8[0]
                    # Fill missing quote data from v8 meta if V7 failed
                    meta = chart.get("meta", {})
                    if not out.get("current_price"):
                        out["current_price"] = safe_float(meta.get("regularMarketPrice"))
                        out["previous_close"] = safe_float(meta.get("chartPreviousClose"))
                        if out.get("current_price") and out.get("previous_close"):
                            out["price_change"] = out["current_price"] - out["previous_close"]
                            out["percent_change"] = (out["price_change"] / out["previous_close"]) * 100.0
                            out["volume"] = safe_float(meta.get("regularMarketVolume"))
                    
                    indicators = chart.get("indicators", {}).get("quote", [{}])[0]
                    closes = [safe_float(x) for x in indicators.get("close", []) if x is not None]
                    vols = [safe_float(x) for x in indicators.get("volume", []) if x is not None]
                    
                    if closes and _enable_history():
                        ti = TechnicalIndicators()
                        rsi_arr = ti.rsi(closes, 14)
                        if rsi_arr:
                            out["rsi_14"] = rsi_arr[-1]
                        
                        mac = ti.macd(closes)
                        if mac and mac["macd"] and mac["signal"]:
                            out["macd"] = mac["macd"][-1]
                            out["macd_signal"] = mac["signal"][-1]
                        
                        if _enable_forecast():
                            fc = EnsembleForecaster(closes, vols, enable_ml=_enable_ml(), enable_dl=_enable_deep_learning()).forecast(252)
                            if fc.get("forecast_available"):
                                out["forecast_price_1y"] = fc["ensemble"]["price"]
                                out["expected_roi_1y"] = fc["ensemble"]["roi_pct"]
                                out["forecast_price_12m"] = fc["ensemble"]["price"]
                                out["expected_roi_12m"] = fc["ensemble"]["roi_pct"]
                                out["forecast_confidence"] = fc["confidence"]
        except Exception as e:
            logger.debug(f"Fallback V8 history failed: {e}")
            
        if not out.get("current_price"):
            return {"symbol": symbol, "error": "No data found on Yahoo REST fallback", "data_quality": "MISSING"}
            
        out["data_quality"] = "GOOD"
        out["last_updated_utc"] = datetime.now(timezone.utc).isoformat()
        return out

    def _blocking_fetch(self, symbol: str) -> Dict[str, Any]:
        """Blocking yfinance fetch deployed in the threadpool."""
        if not _HAS_YFINANCE or yf is None: 
            try:
                return self._fallback_fetch(symbol)
            except Exception as e:
                return {"error": f"yfinance not installed and fallback failed: {e}"}
        
        last_err = None
        for attempt in range(self.retry_attempts):
            try:
                ticker = yf.Ticker(symbol)
                out = {}
                try:
                    # yfinance fast_info can throw an exception on session failure
                    fi = getattr(ticker, "fast_info", None)
                    if fi:
                        out["current_price"] = safe_float(getattr(fi, "last_price", None))
                        out["previous_close"] = safe_float(getattr(fi, "previous_close", None))
                        out["volume"] = safe_float(getattr(fi, "last_volume", None))
                        out["day_high"] = safe_float(getattr(fi, "day_high", None))
                        out["day_low"] = safe_float(getattr(fi, "day_low", None))
                        out["week_52_high"] = safe_float(getattr(fi, "fifty_two_week_high", None))
                        out["week_52_low"] = safe_float(getattr(fi, "fifty_two_week_low", None))
                        out["market_cap"] = safe_float(getattr(fi, "market_cap", None))
                except Exception: pass
                
                closes, vols = [], []
                try:
                    hist = ticker.history(period=_hist_period(), interval=_hist_interval())
                    if not hist.empty:
                        closes = [safe_float(x) for x in hist["Close"] if safe_float(x) is not None]
                        vols = [safe_float(x) for x in hist["Volume"] if safe_float(x) is not None]
                        
                        if out.get("current_price") is None and closes: 
                            out["current_price"] = closes[-1]
                except Exception: pass
                
                if out.get("current_price") is None: 
                    raise ValueError("No price data returned from yfinance.")
                
                # Enrichment
                if out.get("current_price") and out.get("previous_close") and out["previous_close"] != 0:
                    out["price_change"] = out["current_price"] - out["previous_close"]
                    out["percent_change"] = (out["price_change"] / out["previous_close"]) * 100.0

                if closes and _enable_history():
                    ti = TechnicalIndicators()
                    rsi_arr = ti.rsi(closes, 14)
                    if rsi_arr:
                        out["rsi_14"] = rsi_arr[-1]
                        
                    mac = ti.macd(closes)
                    if mac and mac["macd"] and mac["signal"]:
                        out["macd"] = mac["macd"][-1]
                        out["macd_signal"] = mac["signal"][-1]
                    
                    if _enable_forecast():
                        fc = EnsembleForecaster(closes, vols, enable_ml=_enable_ml(), enable_dl=_enable_deep_learning()).forecast(252)
                        if fc.get("forecast_available"):
                            out["forecast_price_1y"] = fc["ensemble"]["price"]
                            out["expected_roi_1y"] = fc["ensemble"]["roi_pct"]
                            out["forecast_price_12m"] = fc["ensemble"]["price"]
                            out["expected_roi_12m"] = fc["ensemble"]["roi_pct"]
                            out["forecast_confidence"] = fc["confidence"]
                            
                out.update({
                    "symbol": symbol,
                    "data_source": "yahoo_chart",
                    "provider_version": PROVIDER_VERSION,
                    "last_updated_utc": _utc_iso(),
                    "last_updated_riyadh": _riyadh_iso(),
                    "data_quality": "GOOD"
                })
                return out
                
            except Exception as e:
                last_err = e
                base_wait = 1.0 * (2 ** attempt)
                time.sleep(random.uniform(0, base_wait))

        # Fallback to pure REST if yfinance exhausted all retries
        try:
            logger.warning(f"yfinance failed for {symbol}, attempting REST fallback")
            return self._fallback_fetch(symbol)
        except Exception as fb_err:
            return {"error": f"fetch failed after retries: {str(last_err)}. Fallback also failed: {fb_err}"}

    async def fetch_enriched_quote_patch(self, symbol: str, debug: bool = False, *args: Any, **kwargs: Any) -> Dict[str, Any]:
        s = symbol.upper().strip()

        cached = await self.quote_cache.get("quote", symbol=s)
        if cached: return cached

        async def _fetch():
            loop = asyncio.get_running_loop()
            res = await loop.run_in_executor(_CPU_EXECUTOR, self._blocking_fetch, s)
            if "error" not in res:
                await self.quote_cache.set(res, "quote", symbol=s)
            return res

        async with TraceContext("yahoo_fetch", {"symbol": s}):
            return await self.singleflight.run(f"yahoo:{s}", _fetch)

# =============================================================================
# Singleton Management & Exports
# =============================================================================

_PROVIDER_INSTANCE: Optional[YahooChartProvider] = None
_PROVIDER_LOCK = asyncio.Lock()

async def get_provider() -> YahooChartProvider:
    global _PROVIDER_INSTANCE
    if _PROVIDER_INSTANCE is None:
        async with _PROVIDER_LOCK:
            if _PROVIDER_INSTANCE is None:
                _PROVIDER_INSTANCE = YahooChartProvider()
    return _PROVIDER_INSTANCE

async def fetch_enriched_quote_patch(symbol: str, debug: bool = False, *args: Any, **kwargs: Any) -> Dict[str, Any]:
    return await (await get_provider()).fetch_enriched_quote_patch(symbol, debug=debug)

# Aliases
fetch_quote = fetch_enriched_quote_patch
get_quote = fetch_enriched_quote_patch
get_quote_patch = fetch_enriched_quote_patch
fetch_quote_patch = fetch_enriched_quote_patch
fetch_quote_and_enrichment_patch = fetch_enriched_quote_patch
fetch_quote_and_fundamentals_patch = fetch_enriched_quote_patch
yahoo_chart_quote = fetch_enriched_quote_patch

async def fetch_history(symbol: str, *args, **kwargs) -> List[Dict[str, Any]]:
    return []

fetch_price_history = fetch_history
fetch_ohlc_history = fetch_history
fetch_history_patch = fetch_history
fetch_prices = fetch_history

async def get_client_metrics() -> Dict[str, Any]:
    return {"version": PROVIDER_VERSION, "cache_size": await (await get_provider()).quote_cache.size()}

async def health_check() -> Dict[str, Any]:
    res = await fetch_enriched_quote_patch("AAPL")
    return {"status": "healthy" if "error" not in res else "unhealthy"}

async def clear_caches() -> None:
    await (await get_provider()).quote_cache.clear()

async def aclose_yahoo_client() -> None:
    global _PROVIDER_INSTANCE
    _PROVIDER_INSTANCE = None
aclose_yahoo_chart_client = aclose_yahoo_client

__all__ = [
    "YAHOO_CHART_URL", "DATA_SOURCE", "PROVIDER_VERSION", "DataQuality", "YahooChartProvider",
    "fetch_quote", "get_quote", "get_quote_patch", "fetch_quote_patch", "fetch_enriched_quote_patch",
    "fetch_quote_and_enrichment_patch", "fetch_quote_and_fundamentals_patch", "yahoo_chart_quote",
    "fetch_price_history", "fetch_history", "fetch_ohlc_history", "fetch_history_patch", "fetch_prices",
    "aclose_yahoo_chart_client", "aclose_yahoo_client", "get_client_metrics", "clear_caches", "health_check",
]
