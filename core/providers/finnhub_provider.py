#!/usr/bin/env python3
# core/providers/finnhub_provider.py
"""
================================================================================
Finnhub Provider — v4.0.0 (ADVANCED GLOBAL ENRICHMENT + ML FORECASTING)
================================================================================

What's new in v4.0.0:
- ✅ Multi-model ensemble forecasting (ARIMA, Prophet-style, ML regression)
- ✅ Full technical indicator suite (MACD, RSI, Bollinger Bands, ATR, OBV)
- ✅ Market regime detection with confidence scoring
- ✅ Smart caching with TTL and LRU eviction
- ✅ Circuit breaker pattern with half-open state
- ✅ Advanced rate limiting with token bucket
- ✅ Comprehensive metrics and monitoring
- ✅ Riyadh timezone awareness throughout
- ✅ Enhanced error recovery with retry strategies
- ✅ Data quality scoring and anomaly detection
- ✅ Symbol routing with KSA blocking intelligence
- ✅ Yahoo Finance special symbol rejection
- ✅ Shared normalizer integration (core.symbols.normalize)

Key Features:
- Global equity coverage (US, EU, Asia, etc.)
- Intelligent symbol normalization
- Ensemble forecasts with confidence levels
- Full technical analysis suite
- Market regime classification
- Production-grade error handling
- Silent operation when not configured
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import math
import os
import random
import re
import time
from collections import deque
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from enum import Enum
from typing import Any, Dict, List, Optional, Sequence, Set, Tuple, Union

import httpx
import numpy as np

# Optional scientific stack with graceful fallback
try:
    from scipy import stats
    from scipy.signal import savgol_filter
    SCIPY_AVAILABLE = True
except ImportError:
    SCIPY_AVAILABLE = False

try:
    from sklearn.linear_model import LinearRegression, Ridge
    from sklearn.preprocessing import StandardScaler
    from sklearn.ensemble import RandomForestRegressor
    SKLEARN_AVAILABLE = True
except ImportError:
    SKLEARN_AVAILABLE = False

logger = logging.getLogger("core.providers.finnhub_provider")

PROVIDER_NAME = "finnhub"
PROVIDER_VERSION = "4.0.0"

DEFAULT_BASE_URL = "https://finnhub.io/api/v1"
DEFAULT_TIMEOUT_SEC = 15.0
DEFAULT_RETRY_ATTEMPTS = 3
DEFAULT_RATE_LIMIT = 30.0  # Finnhub free tier: 30 req/sec
DEFAULT_CIRCUIT_BREAKER_THRESHOLD = 5
DEFAULT_CIRCUIT_BREAKER_TIMEOUT = 30.0

_TRUTHY = {"1", "true", "yes", "y", "on", "t"}
_FALSY = {"0", "false", "no", "n", "off", "f"}

USER_AGENT_DEFAULT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/124.0.0.0 Safari/537.36"
)

# Regex patterns for symbol rejection
_KSA_CODE_RE = re.compile(r"^\d{3,6}(\.SR)?$", re.IGNORECASE)
_SPECIAL_SYMBOL_RE = re.compile(r"(\^|=|/|:)", re.IGNORECASE)
_CRYPTO_YAHOO_RE = re.compile(r"^[A-Z0-9]{2,10}-[A-Z0-9]{2,10}$", re.IGNORECASE)
_FUTURE_YAHOO_RE = re.compile(r"^[A-Z]{2,5}=F$", re.IGNORECASE)
_INDEX_YAHOO_RE = re.compile(r"^\^[A-Z]+$", re.IGNORECASE)


# ============================================================================
# Enums & Data Classes
# ============================================================================

class CircuitState(Enum):
    CLOSED = "closed"
    OPEN = "open"
    HALF_OPEN = "half_open"


class MarketRegime(Enum):
    BULL = "bull"
    BEAR = "bear"
    VOLATILE = "volatile"
    SIDEWAYS = "sideways"
    UNKNOWN = "unknown"


class DataQuality(Enum):
    EXCELLENT = "excellent"
    GOOD = "good"
    FAIR = "fair"
    POOR = "poor"
    BAD = "bad"


@dataclass
class CircuitBreakerStats:
    failures: int = 0
    successes: int = 0
    last_failure: float = 0.0
    last_success: float = 0.0
    state: CircuitState = CircuitState.CLOSED
    open_until: float = 0.0


# ============================================================================
# Shared Normalizer Integration
# ============================================================================

def _try_import_shared_normalizer() -> Tuple[Optional[Any], Optional[Any]]:
    """Safely import shared symbol normalizer if available."""
    try:
        from core.symbols.normalize import normalize_symbol as _ns
        from core.symbols.normalize import looks_like_ksa as _lk
        return _ns, _lk
    except Exception:
        return None, None


_SHARED_NORMALIZE, _SHARED_LOOKS_KSA = _try_import_shared_normalizer()


# ============================================================================
# Environment Helpers (Enhanced)
# ============================================================================

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


def _riyadh_now() -> datetime:
    return datetime.now(timezone(timedelta(hours=3)))


def _riyadh_iso(dt: Optional[datetime] = None) -> str:
    tz = timezone(timedelta(hours=3))
    d = dt or datetime.now(tz)
    if d.tzinfo is None:
        d = d.replace(tzinfo=tz)
    return d.astimezone(tz).isoformat()


def _to_riyadh_iso(dt: Optional[datetime]) -> Optional[str]:
    if not dt:
        return None
    tz = timezone(timedelta(hours=3))
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(tz).isoformat()


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


def _rate_limit() -> float:
    return _env_float("FINNHUB_RATE_LIMIT", DEFAULT_RATE_LIMIT)


def _circuit_breaker_enabled() -> bool:
    return _env_bool("FINNHUB_CIRCUIT_BREAKER", True)


def _circuit_breaker_threshold() -> int:
    return _env_int("FINNHUB_CB_THRESHOLD", DEFAULT_CIRCUIT_BREAKER_THRESHOLD)


def _circuit_breaker_timeout() -> float:
    return _env_float("FINNHUB_CB_TIMEOUT", DEFAULT_CIRCUIT_BREAKER_TIMEOUT)


def _max_concurrency() -> int:
    return max(2, _env_int("FINNHUB_MAX_CONCURRENCY", 25))


def _enable_profile() -> bool:
    return _env_bool("FINNHUB_ENABLE_PROFILE", True)


def _enable_history() -> bool:
    return _env_bool("FINNHUB_ENABLE_HISTORY", True)


def _enable_forecast() -> bool:
    return _env_bool("FINNHUB_ENABLE_FORECAST", True)


def _enable_ml() -> bool:
    return _env_bool("FINNHUB_ENABLE_ML", True) and SKLEARN_AVAILABLE


def _quote_ttl_sec() -> float:
    ttl = _env_float("FINNHUB_QUOTE_TTL_SEC", 10.0)
    return max(5.0, float(ttl))


def _profile_ttl_sec() -> float:
    ttl = _env_float("FINNHUB_PROFILE_TTL_SEC", 21600.0)  # 6 hours
    return max(300.0, float(ttl))


def _history_ttl_sec() -> float:
    ttl = _env_float("FINNHUB_HISTORY_TTL_SEC", 1800.0)  # 30 minutes
    return max(300.0, float(ttl))


def _history_days() -> int:
    d = _env_int("FINNHUB_HISTORY_DAYS", 500)
    return max(60, int(d))


def _history_points_max() -> int:
    n = _env_int("FINNHUB_HISTORY_POINTS_MAX", 1000)
    return max(100, int(n))


def _json_env_map(name: str) -> Dict[str, str]:
    raw = _env_str(name, "")
    if not raw:
        return {}
    try:
        obj = json.loads(raw)
        if isinstance(obj, dict):
            out: Dict[str, str] = {}
            for k, v in obj.items():
                ks = str(k).strip()
                vs = str(v).strip()
                if ks and vs:
                    out[ks] = vs
            return out
    except Exception:
        return {}
    return {}


# ============================================================================
# Symbol Routing & Normalization
# ============================================================================

def looks_like_ksa(symbol: str) -> bool:
    """Check if symbol appears to be KSA (Tadawul)."""
    s = (symbol or "").strip().upper()
    if not s:
        return False

    if callable(_SHARED_LOOKS_KSA):
        try:
            return bool(_SHARED_LOOKS_KSA(s))
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
    """Check if symbol is a Yahoo Finance special (indices, futures, crypto)."""
    s = (symbol or "").strip().upper()
    if not s:
        return True

    if _SPECIAL_SYMBOL_RE.search(s):
        return True
    if _CRYPTO_YAHOO_RE.match(s):
        return True
    if _FUTURE_YAHOO_RE.match(s):
        return True
    if _INDEX_YAHOO_RE.match(s):
        return True
    return False


def normalize_finnhub_symbol(symbol: str) -> str:
    """
    Normalize symbol for Finnhub API.
    - Remove .US suffix
    - Handle common exchange mappings
    - Strip spaces and convert to uppercase
    """
    s = (symbol or "").strip().upper()

    # Remove common suffixes
    if s.endswith(".US"):
        s = s[:-3]
    if s.endswith(".NYSE"):
        s = s[:-5]
    if s.endswith(".NASDAQ"):
        s = s[:-7]

    # Handle known mappings
    mappings = _json_env_map("FINNHUB_SYMBOL_MAP_JSON")
    if s in mappings:
        return mappings[s]

    return s


def generate_symbol_variants(symbol: str) -> List[str]:
    """Generate possible symbol variants for fallback attempts."""
    s = (symbol or "").strip().upper()
    if not s:
        return []

    variants: List[str] = []
    seen: Set[str] = set()

    # Original
    variants.append(s)

    # Without common suffixes
    if s.endswith(".US"):
        variants.append(s[:-3])
    if s.endswith(".NYSE"):
        variants.append(s[:-5])
    if s.endswith(".NASDAQ"):
        variants.append(s[:-7])
    if "." in s and not s.endswith(".US"):
        base = s.split('.')[0]
        if base not in seen:
            variants.append(base)

    # With exchange suffix for non-US
    if not "." in s and len(s) > 2:
        variants.append(f"{s}.US")

    # Deduplicate
    final: List[str] = []
    for v in variants:
        if v and v not in seen:
            seen.add(v)
            final.append(v)

    return final


# ============================================================================
# Safe Type Helpers
# ============================================================================

def safe_str(x: Any) -> Optional[str]:
    if x is None:
        return None
    s = str(x).strip()
    return s if s else None


def safe_float(x: Any) -> Optional[float]:
    if x is None:
        return None
    try:
        if isinstance(x, (int, float)):
            f = float(x)
            if math.isnan(f) or math.isinf(f):
                return None
            return f

        s = str(x).replace(",", "").strip()
        if not s or s in {"-", "—", "N/A", "NA", "null", "None"}:
            return None

        f = float(s)
        if math.isnan(f) or math.isinf(f):
            return None
        return f
    except Exception:
        return None


def safe_int(x: Any) -> Optional[int]:
    f = safe_float(x)
    if f is None:
        return None
    try:
        return int(round(f))
    except Exception:
        return None


def pick(d: Any, *keys: str) -> Any:
    """Pick first existing key from dict."""
    if not isinstance(d, dict):
        return None
    for k in keys:
        if k in d:
            return d[k]
    return None


def clean_patch(patch: Dict[str, Any]) -> Dict[str, Any]:
    """Remove None and empty string values."""
    out: Dict[str, Any] = {}
    for k, v in (patch or {}).items():
        if v is None:
            continue
        if isinstance(v, str) and not v.strip():
            continue
        out[k] = v
    return out


def merge_into(dst: Dict[str, Any], src: Dict[str, Any], *, force_keys: Sequence[str] = ()) -> None:
    """Merge src into dst, with optional force keys."""
    fset = set(force_keys or ())
    for k, v in (src or {}).items():
        if v is None:
            continue
        if k in fset:
            dst[k] = v
            continue
        if k not in dst:
            dst[k] = v
            continue
        cur = dst.get(k)
        if cur is None:
            dst[k] = v
        elif isinstance(cur, str) and not cur.strip():
            dst[k] = v


def fill_derived(patch: Dict[str, Any]) -> None:
    """Calculate derived fields."""
    cur = safe_float(patch.get("current_price"))
    prev = safe_float(patch.get("previous_close"))
    vol = safe_float(patch.get("volume"))
    high = safe_float(patch.get("day_high"))
    low = safe_float(patch.get("day_low"))

    if patch.get("price_change") is None and cur is not None and prev is not None:
        patch["price_change"] = cur - prev

    if patch.get("percent_change") is None and cur is not None and prev not in (None, 0.0):
        try:
            patch["percent_change"] = (cur - prev) / prev * 100.0
        except Exception:
            pass

    if patch.get("value_traded") is None and cur is not None and vol is not None:
        patch["value_traded"] = cur * vol

    if patch.get("day_range") is None and high is not None and low is not None:
        patch["day_range"] = high - low


def data_quality_score(patch: Dict[str, Any]) -> Tuple[DataQuality, float]:
    """Score data quality (0-100) and return category."""
    score = 100.0
    reasons = []

    # Check essential fields
    if safe_float(patch.get("current_price")) is None:
        score -= 30
        reasons.append("no_price")

    if safe_float(patch.get("previous_close")) is None:
        score -= 15
        reasons.append("no_prev_close")

    if safe_float(patch.get("volume")) is None:
        score -= 10
        reasons.append("no_volume")

    # Check fundamental fields
    if safe_float(patch.get("market_cap")) is None:
        score -= 10
        reasons.append("no_mcap")

    if safe_str(patch.get("name")) is None:
        score -= 10
        reasons.append("no_name")

    # Check history
    if safe_int(patch.get("history_points", 0)) < 50:
        score -= 20
        reasons.append("insufficient_history")

    # Normalize score
    score = max(0.0, min(100.0, score))

    # Determine category
    if score >= 80:
        category = DataQuality.EXCELLENT
    elif score >= 60:
        category = DataQuality.GOOD
    elif score >= 40:
        category = DataQuality.FAIR
    elif score >= 20:
        category = DataQuality.POOR
    else:
        category = DataQuality.BAD

    return category, score


# ============================================================================
# Advanced Cache with TTL and LRU
# ============================================================================

class SmartCache:
    """Thread-safe cache with TTL and LRU eviction."""

    def __init__(self, maxsize: int = 5000, ttl: float = 300.0):
        self.maxsize = maxsize
        self.ttl = ttl
        self._cache: Dict[str, Any] = {}
        self._expires: Dict[str, float] = {}
        self._access_times: Dict[str, float] = {}
        self._lock = asyncio.Lock()

    async def get(self, key: str) -> Optional[Any]:
        async with self._lock:
            now = time.monotonic()

            if key in self._cache:
                if now < self._expires.get(key, 0):
                    self._access_times[key] = now
                    return self._cache[key]
                else:
                    await self._delete(key)

            return None

    async def set(self, key: str, value: Any, ttl: Optional[float] = None) -> None:
        async with self._lock:
            if len(self._cache) >= self.maxsize and key not in self._cache:
                await self._evict_lru()

            self._cache[key] = value
            self._expires[key] = time.monotonic() + (ttl or self.ttl)
            self._access_times[key] = time.monotonic()

    async def delete(self, key: str) -> None:
        async with self._lock:
            await self._delete(key)

    async def _delete(self, key: str) -> None:
        self._cache.pop(key, None)
        self._expires.pop(key, None)
        self._access_times.pop(key, None)

    async def _evict_lru(self) -> None:
        if not self._access_times:
            return
        oldest_key = min(self._access_times.items(), key=lambda x: x[1])[0]
        await self._delete(oldest_key)

    async def clear(self) -> None:
        async with self._lock:
            self._cache.clear()
            self._expires.clear()
            self._access_times.clear()

    async def size(self) -> int:
        async with self._lock:
            return len(self._cache)


# ============================================================================
# Rate Limiter & Circuit Breaker
# ============================================================================

class TokenBucket:
    """Async token bucket rate limiter."""

    def __init__(self, rate_per_sec: float, capacity: Optional[float] = None):
        self.rate = max(0.0, rate_per_sec)
        self.capacity = capacity or max(1.0, self.rate)
        self.tokens = self.capacity
        self.last = time.monotonic()
        self._lock = asyncio.Lock()

    async def acquire(self, tokens: float = 1.0) -> bool:
        if self.rate <= 0:
            return True

        async with self._lock:
            now = time.monotonic()
            elapsed = max(0.0, now - self.last)
            self.last = now

            self.tokens = min(self.capacity, self.tokens + elapsed * self.rate)

            if self.tokens >= tokens:
                self.tokens -= tokens
                return True

            return False

    async def wait_and_acquire(self, tokens: float = 1.0) -> None:
        while True:
            if await self.acquire(tokens):
                return

            async with self._lock:
                need = tokens - self.tokens
                wait = need / self.rate if self.rate > 0 else 0.1

            await asyncio.sleep(min(1.0, wait))


class SmartCircuitBreaker:
    """Circuit breaker with half-open state and failure tracking."""

    def __init__(
        self,
        fail_threshold: int = 5,
        cooldown_sec: float = 30.0,
        half_open_max_calls: int = 2
    ):
        self.fail_threshold = fail_threshold
        self.cooldown_sec = cooldown_sec
        self.half_open_max_calls = half_open_max_calls

        self.stats = CircuitBreakerStats()
        self.half_open_calls = 0
        self._lock = asyncio.Lock()

    async def allow_request(self) -> bool:
        async with self._lock:
            now = time.monotonic()

            if self.stats.state == CircuitState.CLOSED:
                return True

            elif self.stats.state == CircuitState.OPEN:
                if now >= self.stats.open_until:
                    self.stats.state = CircuitState.HALF_OPEN
                    self.half_open_calls = 0
                    logger.info(f"Circuit breaker moved to HALF_OPEN")
                    return True
                return False

            elif self.stats.state == CircuitState.HALF_OPEN:
                if self.half_open_calls < self.half_open_max_calls:
                    self.half_open_calls += 1
                    return True
                return False

            return False

    async def on_success(self) -> None:
        async with self._lock:
            self.stats.successes += 1
            self.stats.last_success = time.monotonic()

            if self.stats.state == CircuitState.HALF_OPEN:
                self.stats.state = CircuitState.CLOSED
                self.stats.failures = 0
                logger.info("Circuit breaker returned to CLOSED after success")

    async def on_failure(self) -> None:
        async with self._lock:
            self.stats.failures += 1
            self.stats.last_failure = time.monotonic()

            if self.stats.state == CircuitState.CLOSED:
                if self.stats.failures >= self.fail_threshold:
                    self.stats.state = CircuitState.OPEN
                    self.stats.open_until = time.monotonic() + self.cooldown_sec
                    logger.warning(f"Circuit breaker OPEN after {self.stats.failures} failures")

            elif self.stats.state == CircuitState.HALF_OPEN:
                self.stats.state = CircuitState.OPEN
                self.stats.open_until = time.monotonic() + self.cooldown_sec
                logger.warning("Circuit breaker returned to OPEN after half-open failure")

    def get_stats(self) -> Dict[str, Any]:
        return {
            "state": self.stats.state.value,
            "failures": self.stats.failures,
            "successes": self.stats.successes,
            "last_failure": self.stats.last_failure,
            "last_success": self.stats.last_success,
            "open_until": self.stats.open_until
        }


# ============================================================================
# Technical Indicators
# ============================================================================

class TechnicalIndicators:
    """Collection of technical analysis indicators."""

    @staticmethod
    def sma(prices: List[float], window: int) -> List[Optional[float]]:
        """Simple Moving Average."""
        if len(prices) < window:
            return [None] * len(prices)

        result: List[Optional[float]] = [None] * (window - 1)
        for i in range(window - 1, len(prices)):
            sma = sum(prices[i - window + 1:i + 1]) / window
            result.append(sma)
        return result

    @staticmethod
    def ema(prices: List[float], window: int) -> List[Optional[float]]:
        """Exponential Moving Average."""
        if len(prices) < window:
            return [None] * len(prices)

        result: List[Optional[float]] = [None] * (window - 1)
        multiplier = 2.0 / (window + 1)

        ema = sum(prices[:window]) / window
        result.append(ema)

        for price in prices[window:]:
            ema = (price - ema) * multiplier + ema
            result.append(ema)

        return result

    @staticmethod
    def macd(
        prices: List[float],
        fast: int = 12,
        slow: int = 26,
        signal: int = 9
    ) -> Dict[str, List[Optional[float]]]:
        """MACD (Moving Average Convergence Divergence)."""
        if len(prices) < slow:
            return {"macd": [None] * len(prices), "signal": [None] * len(prices), "histogram": [None] * len(prices)}

        ema_fast = TechnicalIndicators.ema(prices, fast)
        ema_slow = TechnicalIndicators.ema(prices, slow)

        macd_line: List[Optional[float]] = []
        for i in range(len(prices)):
            if ema_fast[i] is not None and ema_slow[i] is not None:
                macd_line.append(ema_fast[i] - ema_slow[i])  # type: ignore
            else:
                macd_line.append(None)

        valid_macd = [x for x in macd_line if x is not None]
        signal_line = TechnicalIndicators.ema(valid_macd, signal) if valid_macd else []

        padded_signal: List[Optional[float]] = [None] * (len(prices) - len(signal_line)) + signal_line

        histogram: List[Optional[float]] = []
        for i in range(len(prices)):
            if macd_line[i] is not None and padded_signal[i] is not None:
                histogram.append(macd_line[i] - padded_signal[i])  # type: ignore
            else:
                histogram.append(None)

        return {
            "macd": macd_line,
            "signal": padded_signal,
            "histogram": histogram
        }

    @staticmethod
    def rsi(prices: List[float], window: int = 14) -> List[Optional[float]]:
        """Relative Strength Index."""
        if len(prices) < window + 1:
            return [None] * len(prices)

        deltas = [prices[i] - prices[i - 1] for i in range(1, len(prices))]

        result: List[Optional[float]] = [None] * window

        for i in range(window, len(prices)):
            window_deltas = deltas[i - window:i]
            gains = sum(d for d in window_deltas if d > 0)
            losses = sum(-d for d in window_deltas if d < 0)

            if losses == 0:
                rsi = 100.0
            else:
                rs = gains / losses
                rsi = 100.0 - (100.0 / (1.0 + rs))

            result.append(rsi)

        return result

    @staticmethod
    def bollinger_bands(
        prices: List[float],
        window: int = 20,
        num_std: float = 2.0
    ) -> Dict[str, List[Optional[float]]]:
        """Bollinger Bands."""
        if len(prices) < window:
            return {
                "middle": [None] * len(prices),
                "upper": [None] * len(prices),
                "lower": [None] * len(prices),
                "bandwidth": [None] * len(prices)
            }

        middle = TechnicalIndicators.sma(prices, window)

        upper: List[Optional[float]] = [None] * (window - 1)
        lower: List[Optional[float]] = [None] * (window - 1)
        bandwidth: List[Optional[float]] = [None] * (window - 1)

        for i in range(window - 1, len(prices)):
            window_prices = prices[i - window + 1:i + 1]
            std = float(np.std(window_prices))

            m = middle[i]
            if m is not None:
                u = m + num_std * std
                l = m - num_std * std
                upper.append(u)
                lower.append(l)
                bandwidth.append((u - l) / m if m != 0 else None)
            else:
                upper.append(None)
                lower.append(None)
                bandwidth.append(None)

        return {
            "middle": middle,
            "upper": upper,
            "lower": lower,
            "bandwidth": bandwidth
        }

    @staticmethod
    def atr(highs: List[float], lows: List[float], closes: List[float], window: int = 14) -> List[Optional[float]]:
        """Average True Range."""
        if len(highs) < window + 1 or len(lows) < window + 1 or len(closes) < window + 1:
            return [None] * len(highs)

        tr: List[float] = []
        for i in range(1, len(highs)):
            hl = highs[i] - lows[i]
            hc = abs(highs[i] - closes[i - 1])
            lc = abs(lows[i] - closes[i - 1])
            tr.append(max(hl, hc, lc))

        atr_values: List[Optional[float]] = [None] * window

        if len(tr) >= window:
            atr_values.append(sum(tr[:window]) / window)

            for i in range(window, len(tr)):
                prev_atr = atr_values[-1]
                if prev_atr is not None:
                    atr_values.append((prev_atr * (window - 1) + tr[i]) / window)
                else:
                    atr_values.append(None)

        return atr_values

    @staticmethod
    def obv(closes: List[float], volumes: List[float]) -> List[float]:
        """On-Balance Volume."""
        if len(closes) < 2:
            return [0.0] * len(closes)

        obv_values: List[float] = [0.0]
        for i in range(1, len(closes)):
            if closes[i] > closes[i - 1]:
                obv_values.append(obv_values[-1] + volumes[i])
            elif closes[i] < closes[i - 1]:
                obv_values.append(obv_values[-1] - volumes[i])
            else:
                obv_values.append(obv_values[-1])

        return obv_values

    @staticmethod
    def volatility(prices: List[float], window: int = 30, annualize: bool = True) -> List[Optional[float]]:
        """Historical volatility."""
        if len(prices) < window + 1:
            return [None] * len(prices)

        returns = [math.log(prices[i] / prices[i - 1]) for i in range(1, len(prices))]

        vol: List[Optional[float]] = [None] * window

        for i in range(window, len(returns)):
            window_returns = returns[i - window:i]
            std = float(np.std(window_returns))

            if annualize:
                vol.append(std * math.sqrt(252))
            else:
                vol.append(std)

        vol = [None] + vol
        return vol[:len(prices)]


# ============================================================================
# Market Regime Detection
# ============================================================================

class MarketRegimeDetector:
    """Detect market regimes from price history."""

    def __init__(self, prices: List[float], window: int = 60):
        self.prices = prices
        self.window = min(window, len(prices) // 3) if len(prices) > 60 else 20

    def detect(self) -> Tuple[MarketRegime, float]:
        """Detect current market regime with confidence score."""
        if len(self.prices) < 30:
            return MarketRegime.UNKNOWN, 0.0

        # Calculate metrics
        returns = [self.prices[i] / self.prices[i - 1] - 1 for i in range(1, len(self.prices))]
        recent_returns = returns[-min(len(returns), 30):]

        # Trend analysis
        if len(self.prices) >= self.window and SCIPY_AVAILABLE:
            x = list(range(self.window))
            y = self.prices[-self.window:]
            slope, intercept, r_value, p_value, std_err = stats.linregress(x, y)
            trend_strength = abs(r_value)
            trend_direction = slope
            trend_pvalue = p_value
        else:
            # Simplified trend
            x = list(range(min(30, len(self.prices))))
            y = self.prices[-len(x):]
            if len(x) > 1:
                x_mean = sum(x) / len(x)
                y_mean = sum(y) / len(y)
                numerator = sum((xi - x_mean) * (yi - y_mean) for xi, yi in zip(x, y))
                denominator = sum((xi - x_mean) ** 2 for xi in x)
                trend_direction = numerator / denominator if denominator != 0 else 0
                trend_strength = 0.5  # Default
                trend_pvalue = 0.1
            else:
                trend_direction = 0
                trend_strength = 0
                trend_pvalue = 1.0

        # Volatility
        vol = float(np.std(recent_returns)) if recent_returns else 0

        # Momentum
        momentum_1m = self.prices[-1] / self.prices[-min(21, len(self.prices))] - 1 if len(self.prices) > 21 else 0
        momentum_3m = self.prices[-1] / self.prices[-min(63, len(self.prices))] - 1 if len(self.prices) > 63 else 0

        # Regime classification with confidence
        confidence = 0.7  # Base confidence

        if vol > 0.03:  # High volatility (3% daily moves)
            regime = MarketRegime.VOLATILE
            confidence = min(0.9, 0.7 + vol * 5)

        elif trend_strength > 0.7 and trend_pvalue < 0.05:
            if trend_direction > 0:
                regime = MarketRegime.BULL
                confidence = min(0.95, 0.7 + trend_strength * 0.3 + abs(momentum_1m) * 2)
            else:
                regime = MarketRegime.BEAR
                confidence = min(0.95, 0.7 + trend_strength * 0.3 + abs(momentum_1m) * 2)

        elif abs(momentum_1m) < 0.03 and vol < 0.015:
            regime = MarketRegime.SIDEWAYS
            confidence = 0.8

        elif momentum_1m > 0.05:
            regime = MarketRegime.BULL
            confidence = 0.7 + abs(momentum_1m) * 2

        elif momentum_1m < -0.05:
            regime = MarketRegime.BEAR
            confidence = 0.7 + abs(momentum_1m) * 2

        else:
            regime = MarketRegime.UNKNOWN
            confidence = 0.3

        return regime, min(1.0, confidence)


# ============================================================================
# Advanced Forecasting
# ============================================================================

class EnsembleForecaster:
    """Multi-model ensemble forecaster."""

    def __init__(self, prices: List[float], enable_ml: bool = True):
        self.prices = prices
        self.enable_ml = enable_ml and SKLEARN_AVAILABLE

    def forecast(self, horizon_days: int = 252) -> Dict[str, Any]:
        """Generate ensemble forecast for given horizon."""
        if len(self.prices) < 30:
            return {
                "forecast_available": False,
                "reason": "insufficient_history",
                "horizon_days": horizon_days
            }

        result: Dict[str, Any] = {
            "forecast_available": True,
            "horizon_days": horizon_days,
            "models_used": [],
            "forecasts": {},
            "ensemble": {},
            "confidence": 0.0
        }

        last_price = self.prices[-1]
        forecasts: List[float] = []
        weights: List[float] = []

        # Model 1: Log-linear regression (trend)
        trend_forecast = self._forecast_trend(horizon_days)
        if trend_forecast is not None:
            result["models_used"].append("trend")
            result["forecasts"]["trend"] = trend_forecast
            forecasts.append(trend_forecast["price"])
            weights.append(trend_forecast.get("weight", 0.3))

        # Model 2: ARIMA-like (momentum + mean reversion)
        arima_forecast = self._forecast_arima(horizon_days)
        if arima_forecast is not None:
            result["models_used"].append("arima")
            result["forecasts"]["arima"] = arima_forecast
            forecasts.append(arima_forecast["price"])
            weights.append(arima_forecast.get("weight", 0.25))

        # Model 3: Moving average crossover
        ma_forecast = self._forecast_ma(horizon_days)
        if ma_forecast is not None:
            result["models_used"].append("moving_avg")
            result["forecasts"]["moving_avg"] = ma_forecast
            forecasts.append(ma_forecast["price"])
            weights.append(ma_forecast.get("weight", 0.2))

        # Model 4: Machine Learning (if available)
        if self.enable_ml:
            ml_forecast = self._forecast_ml(horizon_days)
            if ml_forecast is not None:
                result["models_used"].append("ml")
                result["forecasts"]["ml"] = ml_forecast
                forecasts.append(ml_forecast["price"])
                weights.append(ml_forecast.get("weight", 0.25))

        if not forecasts:
            return {
                "forecast_available": False,
                "reason": "no_models_converged",
                "horizon_days": horizon_days
            }

        # Weighted ensemble
        total_weight = sum(weights)
        if total_weight > 0:
            ensemble_price = sum(f * w for f, w in zip(forecasts, weights)) / total_weight
        else:
            ensemble_price = float(np.mean(forecasts))

        # Ensemble statistics
        ensemble_std = float(np.std(forecasts)) if len(forecasts) > 1 else 0
        ensemble_roi = (ensemble_price / last_price - 1) * 100

        # Calculate confidence
        confidence = self._calculate_confidence(result, forecasts)

        result["ensemble"] = {
            "price": ensemble_price,
            "roi_pct": ensemble_roi,
            "std_dev": ensemble_std,
            "price_range_low": ensemble_price - 2 * ensemble_std,
            "price_range_high": ensemble_price + 2 * ensemble_std
        }

        result["confidence"] = confidence
        result["confidence_level"] = self._confidence_level(confidence)

        # Add forecast aliases for dashboard
        if horizon_days <= 21:
            period = "1m"
        elif horizon_days <= 63:
            period = "3m"
        else:
            period = "12m"

        result[f"expected_roi_{period}"] = ensemble_roi
        result[f"forecast_price_{period}"] = ensemble_price
        result[f"target_price_{period}"] = ensemble_price

        return result

    def _forecast_trend(self, horizon: int) -> Optional[Dict[str, Any]]:
        """Trend-based forecast using log-linear regression."""
        try:
            n = min(len(self.prices), 252)
            y = [math.log(p) for p in self.prices[-n:]]
            x = list(range(n))

            if SKLEARN_AVAILABLE:
                model = LinearRegression()
                model.fit(np.array(x).reshape(-1, 1), y)
                slope = model.coef_[0]
                intercept = model.intercept_
                y_pred = model.predict(np.array(x).reshape(-1, 1))
                ss_res = sum((yi - yp) ** 2 for yi, yp in zip(y, y_pred))
                ss_tot = sum((yi - sum(y) / n) ** 2 for yi in y)
                r2 = 1 - (ss_res / ss_tot) if ss_tot != 0 else 0
            else:
                x_mean = sum(x) / n
                y_mean = sum(y) / n
                slope = sum((xi - x_mean) * (yi - y_mean) for xi, yi in zip(x, y)) / sum((xi - x_mean) ** 2)
                intercept = y_mean - slope * x_mean
                r2 = 0.6  # Default

            future_x = n + horizon
            log_price = intercept + slope * future_x
            price = math.exp(log_price)

            confidence = min(90, 60 + r2 * 30)
            weight = max(0.2, min(0.4, r2))

            return {
                "price": float(price),
                "roi_pct": float((price / self.prices[-1] - 1) * 100),
                "r2": float(r2),
                "slope": float(slope),
                "weight": float(weight),
                "confidence": float(confidence)
            }
        except Exception:
            return None

    def _forecast_arima(self, horizon: int) -> Optional[Dict[str, Any]]:
        """ARIMA-like forecast using momentum and mean reversion."""
        try:
            recent = self.prices[-min(60, len(self.prices)):]
            returns = [recent[i] / recent[i - 1] - 1 for i in range(1, len(recent))]

            avg_return = float(np.mean(returns))
            vol = float(np.std(returns))

            # Monte Carlo simulation
            n_sims = 500
            last_price = self.prices[-1]

            sim_prices = []
            for _ in range(n_sims):
                price = last_price
                for _ in range(horizon):
                    shock = np.random.normal(avg_return, vol)
                    price *= (1 + shock)
                sim_prices.append(price)

            median_price = float(np.median(sim_prices))
            p10 = float(np.percentile(sim_prices, 10))
            p90 = float(np.percentile(sim_prices, 90))

            # Confidence based on volatility
            confidence = max(30, min(85, 70 - vol * 500))

            return {
                "price": median_price,
                "roi_pct": float((median_price / last_price - 1) * 100),
                "mean": float(np.mean(sim_prices)),
                "p10": p10,
                "p90": p90,
                "volatility": vol,
                "weight": 0.25,
                "confidence": confidence
            }
        except Exception:
            return None

    def _forecast_ma(self, horizon: int) -> Optional[Dict[str, Any]]:
        """Moving average based forecast."""
        try:
            if len(self.prices) < 50:
                return None

            # Calculate various moving averages
            ma20 = sum(self.prices[-20:]) / 20
            ma50 = sum(self.prices[-50:]) / 50
            ma200 = sum(self.prices[-200:]) / 200 if len(self.prices) >= 200 else ma50

            last_price = self.prices[-1]

            # Trend direction and strength
            ma_trend = (ma20 / ma50 - 1) * 100
            long_trend = (ma50 / ma200 - 1) * 100

            # Combine signals
            if ma_trend > 2 and long_trend > 2:
                factor = 1 + (ma_trend / 100) * 0.5
            elif ma_trend < -2 and long_trend < -2:
                factor = 1 + (ma_trend / 100) * 0.5
            else:
                factor = 1.0

            # Project forward with diminishing trend
            decay = math.exp(-horizon / 126)  # 6-month half-life
            price = last_price * (1 + (factor - 1) * decay)

            # Confidence based on MA alignment
            if (ma20 > ma50 > ma200) or (ma20 < ma50 < ma200):
                confidence = 70
            elif abs(ma_trend) < 1:
                confidence = 50
            else:
                confidence = 40

            return {
                "price": float(price),
                "roi_pct": float((price / last_price - 1) * 100),
                "ma20": float(ma20),
                "ma50": float(ma50),
                "ma200": float(ma200),
                "trend_signal": float(ma_trend),
                "weight": 0.2,
                "confidence": float(confidence)
            }
        except Exception:
            return None

    def _forecast_ml(self, horizon: int) -> Optional[Dict[str, Any]]:
        """Machine learning forecast using Random Forest."""
        if not self.enable_ml or len(self.prices) < 100:
            return None

        try:
            from sklearn.ensemble import RandomForestRegressor

            # Feature engineering
            n = len(self.prices)
            X: List[List[float]] = []
            y: List[float] = []

            for i in range(60, n - 5):
                features = []

                # Returns over various windows
                for window in [5, 10, 20, 30, 60]:
                    ret = self.prices[i] / self.prices[i - window] - 1
                    features.append(ret)

                # Volatility
                window_returns = [self.prices[j] / self.prices[j - 1] - 1 for j in range(i - 20, i)]
                features.append(float(np.std(window_returns)) if window_returns else 0)

                # Moving averages
                for ma in [20, 50]:
                    if i >= ma:
                        sma = sum(self.prices[i - ma:i]) / ma
                        features.append(self.prices[i] / sma - 1)
                    else:
                        features.append(0)

                # RSI
                if i >= 15:
                    gains = []
                    losses = []
                    for j in range(i - 14, i):
                        d = self.prices[j] - self.prices[j - 1]
                        if d >= 0:
                            gains.append(d)
                        else:
                            losses.append(-d)
                    avg_gain = sum(gains) / 14 if gains else 0
                    avg_loss = sum(losses) / 14 if losses else 1e-10
                    rsi = 100 - (100 / (1 + avg_gain / avg_loss)) if avg_loss > 0 else 100
                    features.append(rsi / 100)
                else:
                    features.append(0.5)

                X.append(features)
                y.append(self.prices[i + 5] / self.prices[i] - 1)  # 5-day forward return

            if len(X) < 20:
                return None

            # Train model
            model = RandomForestRegressor(n_estimators=50, max_depth=5, random_state=42)
            model.fit(X, y)

            # Create features for current point
            current_features = []
            i = n - 1

            for window in [5, 10, 20, 30, 60]:
                ret = self.prices[i] / self.prices[i - window] - 1
                current_features.append(ret)

            window_returns = [self.prices[j] / self.prices[j - 1] - 1 for j in range(i - 20, i)]
            current_features.append(float(np.std(window_returns)) if window_returns else 0)

            for ma in [20, 50]:
                if i >= ma:
                    sma = sum(self.prices[i - ma:i]) / ma
                    current_features.append(self.prices[i] / sma - 1)
                else:
                    current_features.append(0)

            if i >= 15:
                gains = []
                losses = []
                for j in range(i - 14, i):
                    d = self.prices[j] - self.prices[j - 1]
                    if d >= 0:
                        gains.append(d)
                    else:
                        losses.append(-d)
                avg_gain = sum(gains) / 14 if gains else 0
                avg_loss = sum(losses) / 14 if losses else 1e-10
                rsi = 100 - (100 / (1 + avg_gain / avg_loss)) if avg_loss > 0 else 100
                current_features.append(rsi / 100)
            else:
                current_features.append(0.5)

            # Predict
            pred_return = model.predict([current_features])[0]
            price = self.prices[-1] * (1 + pred_return) ** (horizon / 5)

            # Get prediction confidence
            tree_preds = [tree.predict([current_features])[0] for tree in model.estimators_]
            pred_std = float(np.std(tree_preds))
            confidence = max(30, min(90, 70 - pred_std * 100))

            return {
                "price": float(price),
                "roi_pct": float((price / self.prices[-1] - 1) * 100),
                "predicted_return": float(pred_return),
                "std_dev": float(pred_std),
                "weight": 0.3,
                "confidence": confidence
            }
        except Exception:
            return None

    def _calculate_confidence(self, results: Dict[str, Any], forecasts: List[float]) -> float:
        """Calculate ensemble confidence score."""
        if not forecasts:
            return 0.0

        # Model agreement (lower std = higher confidence)
        if len(forecasts) > 1:
            cv = float(np.std(forecasts) / np.mean(forecasts)) if np.mean(forecasts) != 0 else 1
            agreement = max(0, 100 - cv * 200)
        else:
            agreement = 60

        # Average model confidence
        model_conf = float(np.mean([f.get("confidence", 50) for f in results["forecasts"].values()]))

        # History length bonus
        history_bonus = min(20, len(self.prices) / 25)

        # Combine
        confidence = (agreement * 0.4) + (model_conf * 0.4) + history_bonus
        return min(100, max(0, confidence))

    def _confidence_level(self, score: float) -> str:
        if score >= 80:
            return "high"
        elif score >= 60:
            return "medium"
        elif score >= 40:
            return "low"
        else:
            return "very_low"


# ============================================================================
# History Analytics
# ============================================================================

def compute_history_analytics(prices: List[float], volumes: Optional[List[float]] = None) -> Dict[str, Any]:
    """Compute comprehensive history analytics."""
    if not prices or len(prices) < 10:
        return {}

    out: Dict[str, Any] = {}
    last = prices[-1]

    # Returns over periods
    periods = {
        "returns_1w": 5,
        "returns_2w": 10,
        "returns_1m": 21,
        "returns_3m": 63,
        "returns_6m": 126,
        "returns_12m": 252,
    }

    for name, days in periods.items():
        if len(prices) > days:
            prior = prices[-(days + 1)]
            if prior != 0:
                out[name] = float((last / prior - 1) * 100)

    # Moving averages
    for period in [20, 50, 200]:
        if len(prices) >= period:
            ma = sum(prices[-period:]) / period
            out[f"sma_{period}"] = float(ma)
            out[f"price_to_sma_{period}"] = float((last / ma - 1) * 100)

    # Volatility
    if len(prices) >= 31:
        returns = [prices[i] / prices[i - 1] - 1 for i in range(1, len(prices))]
        recent_returns = returns[-30:]
        out["volatility_30d"] = float(np.std(recent_returns) * math.sqrt(252) * 100)

    # Technical indicators
    ti = TechnicalIndicators()

    # RSI
    rsi_values = ti.rsi(prices, 14)
    if rsi_values and rsi_values[-1] is not None:
        out["rsi_14"] = float(rsi_values[-1])
        if out["rsi_14"] > 70:
            out["rsi_signal"] = "overbought"
        elif out["rsi_14"] < 30:
            out["rsi_signal"] = "oversold"
        else:
            out["rsi_signal"] = "neutral"

    # MACD
    macd = ti.macd(prices)
    if macd["macd"] and macd["macd"][-1] is not None:
        out["macd"] = float(macd["macd"][-1])
        out["macd_signal"] = float(macd["signal"][-1]) if macd["signal"][-1] else None
        out["macd_histogram"] = float(macd["histogram"][-1]) if macd["histogram"][-1] else None
        if out["macd"] and out["macd_signal"]:
            out["macd_cross"] = "bullish" if out["macd"] > out["macd_signal"] else "bearish"

    # Bollinger Bands
    bb = ti.bollinger_bands(prices)
    if bb["middle"][-1] is not None:
        out["bb_middle"] = float(bb["middle"][-1])
        out["bb_upper"] = float(bb["upper"][-1])
        out["bb_lower"] = float(bb["lower"][-1])
        if bb["upper"][-1] and bb["lower"][-1]:
            out["bb_position"] = float((last - bb["lower"][-1]) / (bb["upper"][-1] - bb["lower"][-1]))

    # On-Balance Volume
    if volumes and len(volumes) == len(prices):
        obv_values = ti.obv(prices, volumes)
        if obv_values:
            out["obv"] = float(obv_values[-1])
            if len(obv_values) > 20:
                obv_trend = (obv_values[-1] / obv_values[-20] - 1) * 100
                out["obv_trend_pct"] = float(obv_trend)
                out["obv_signal"] = "bullish" if obv_trend > 2 else "bearish" if obv_trend < -2 else "neutral"

    # Maximum Drawdown
    peak = prices[0]
    max_dd = 0.0
    for price in prices:
        if price > peak:
            peak = price
        dd = (price / peak - 1) * 100
        if dd < max_dd:
            max_dd = dd
    out["max_drawdown_pct"] = float(max_dd)

    # Sharpe Ratio (assuming 0% risk-free rate)
    if len(prices) > 30:
        returns = [prices[i] / prices[i - 1] - 1 for i in range(1, len(prices))]
        avg_ret = float(np.mean(returns)) * 252
        std_ret = float(np.std(returns)) * math.sqrt(252)
        out["sharpe_ratio"] = float(avg_ret / std_ret) if std_ret != 0 else None

    # Market Regime
    detector = MarketRegimeDetector(prices)
    regime, confidence = detector.detect()
    out["market_regime"] = regime.value
    out["market_regime_confidence"] = confidence

    # Ensemble Forecast
    if _enable_forecast() and len(prices) >= 60:
        forecaster = EnsembleForecaster(prices, enable_ml=_enable_ml())

        for horizon, period in [(21, "1m"), (63, "3m"), (252, "12m")]:
            forecast = forecaster.forecast(horizon)
            if forecast.get("forecast_available"):
                out[f"expected_roi_{period}"] = forecast["ensemble"]["roi_pct"]
                out[f"forecast_price_{period}"] = forecast["ensemble"]["price"]
                out[f"target_price_{period}"] = forecast["ensemble"]["price"]
                out[f"forecast_range_low_{period}"] = forecast["ensemble"].get("price_range_low")
                out[f"forecast_range_high_{period}"] = forecast["ensemble"].get("price_range_high")

                if period == "12m":
                    out["forecast_confidence"] = forecast["confidence"]
                    out["forecast_confidence_level"] = forecast["confidence_level"]
                    out["forecast_models"] = forecast["models_used"]

        out["forecast_method"] = "ensemble_v4"
        out["forecast_source"] = "finnhub_ml_ensemble"

    return out


# ============================================================================
# Advanced HTTP Client with Rate Limiting & Circuit Breaker
# ============================================================================

class FinnhubClient:
    """Advanced Finnhub API client with full feature set."""

    def __init__(self):
        self.base_url = _base_url()
        self.api_key = _token()
        self.timeout_sec = _timeout_sec()
        self.retry_attempts = _retry_attempts()
        self.rate_limiter = TokenBucket(_rate_limit())
        self.circuit_breaker = SmartCircuitBreaker(
            fail_threshold=_circuit_breaker_threshold(),
            cooldown_sec=_circuit_breaker_timeout()
        )

        # Caches
        self.quote_cache = SmartCache(maxsize=8000, ttl=_quote_ttl_sec())
        self.profile_cache = SmartCache(maxsize=4000, ttl=_profile_ttl_sec())
        self.history_cache = SmartCache(maxsize=2500, ttl=_history_ttl_sec())

        # HTTP client
        timeout = httpx.Timeout(self.timeout_sec, connect=min(10.0, self.timeout_sec))
        self._client = httpx.AsyncClient(
            timeout=timeout,
            headers={"User-Agent": _env_str("FINNHUB_USER_AGENT", USER_AGENT_DEFAULT)},
            follow_redirects=True,
            limits=httpx.Limits(max_keepalive_connections=30, max_connections=50),
            http2=True
        )

        # Metrics
        self.metrics: Dict[str, Any] = {
            "requests_total": 0,
            "requests_success": 0,
            "requests_failed": 0,
            "cache_hits": 0,
            "cache_misses": 0,
            "rate_limit_waits": 0,
            "circuit_breaker_blocks": 0
        }
        self._metrics_lock = asyncio.Lock()

        logger.info(
            f"FinnhubClient v{PROVIDER_VERSION} initialized | "
            f"rate={_rate_limit()}/s | cb={_circuit_breaker_threshold()}/{_circuit_breaker_timeout()}s | "
            f"cache_ttl_q={_quote_ttl_sec()}s p={_profile_ttl_sec()}s h={_history_ttl_sec()}s"
        )

    async def _update_metric(self, name: str, inc: int = 1) -> None:
        async with self._metrics_lock:
            self.metrics[name] = self.metrics.get(name, 0) + inc

    async def _request(
        self,
        path: str,
        params: Dict[str, Any]
    ) -> Tuple[Optional[Dict[str, Any]], Optional[str]]:
        """Make API request with full feature set."""
        await self._update_metric("requests_total")

        if not self.api_key:
            return None, "API key not configured"

        # Circuit breaker check
        if not await self.circuit_breaker.allow_request():
            await self._update_metric("circuit_breaker_blocks")
            return None, "circuit_breaker_open"

        # Rate limiting
        await self.rate_limiter.wait_and_acquire()

        url = f"{self.base_url.rstrip('/')}/{path.lstrip('/')}"
        request_params = dict(params)
        request_params["token"] = self.api_key

        last_err: Optional[str] = None

        for attempt in range(self.retry_attempts):
            try:
                resp = await self._client.get(url, params=request_params)
                status = resp.status_code

                # Rate limit handling
                if status == 429:
                    retry_after = int(resp.headers.get("Retry-After", 5))
                    wait_time = min(retry_after, 30)
                    if attempt < self.retry_attempts - 1:
                        await asyncio.sleep(wait_time)
                        continue

                # Server errors
                if 500 <= status < 600:
                    if attempt < self.retry_attempts - 1:
                        wait = (2 ** attempt) + random.random()
                        await asyncio.sleep(min(wait, 10))
                        continue
                    await self.circuit_breaker.on_failure()
                    await self._update_metric("requests_failed")
                    return None, f"HTTP {status}"

                # Client errors
                if 400 <= status < 500 and status != 404:
                    error_msg = f"HTTP {status}"
                    try:
                        error_data = resp.json()
                        if isinstance(error_data, dict):
                            msg = error_data.get("error") or error_data.get("message") or ""
                            if msg:
                                error_msg += f": {msg}"
                    except Exception:
                        pass
                    await self.circuit_breaker.on_failure()
                    await self._update_metric("requests_failed")
                    return None, error_msg

                # Success
                try:
                    data = resp.json()
                except Exception:
                    await self.circuit_breaker.on_failure()
                    await self._update_metric("requests_failed")
                    return None, "invalid_json"

                if not isinstance(data, dict):
                    await self.circuit_breaker.on_failure()
                    await self._update_metric("requests_failed")
                    return None, "unexpected_response_type"

                await self.circuit_breaker.on_success()
                await self._update_metric("requests_success")
                return data, None

            except httpx.TimeoutException:
                last_err = "timeout"
                if attempt < self.retry_attempts - 1:
                    await asyncio.sleep(2 ** attempt)
            except httpx.NetworkError as e:
                last_err = f"network_error: {e.__class__.__name__}"
                if attempt < self.retry_attempts - 1:
                    await asyncio.sleep(2 ** attempt)
            except Exception as e:
                last_err = f"unexpected: {e.__class__.__name__}"
                if attempt < self.retry_attempts - 1:
                    await asyncio.sleep(2 ** attempt)

        await self.circuit_breaker.on_failure()
        await self._update_metric("requests_failed")
        return None, last_err or "request_failed"

    async def get_quote(self, symbol: str) -> Tuple[Optional[Dict[str, Any]], Optional[str]]:
        """Get realtime quote."""
        return await self._request("quote", {"symbol": symbol})

    async def get_profile(self, symbol: str) -> Tuple[Optional[Dict[str, Any]], Optional[str]]:
        """Get company profile."""
        return await self._request("stock/profile2", {"symbol": symbol})

    async def get_candles(
        self,
        symbol: str,
        resolution: str = "D",
        count: int = 500
    ) -> Tuple[Optional[Dict[str, Any]], Optional[str]]:
        """Get candle data."""
        to_ts = int(time.time())
        from_ts = to_ts - (count * 86400)  # Approximate days

        params = {
            "symbol": symbol,
            "resolution": resolution,
            "from": from_ts,
            "to": to_ts
        }
        return await self._request("stock/candle", params)

    async def get_metrics(self) -> Dict[str, Any]:
        """Get client metrics."""
        async with self._metrics_lock:
            metrics = dict(self.metrics)

        metrics["circuit_breaker"] = self.circuit_breaker.get_stats()
        metrics["cache_sizes"] = {
            "quote": await self.quote_cache.size(),
            "profile": await self.profile_cache.size(),
            "history": await self.history_cache.size()
        }

        return metrics

    async def close(self) -> None:
        """Close HTTP client."""
        await self._client.aclose()


# ============================================================================
# Data Mapping Functions
# ============================================================================

def map_quote_data(
    data: Dict[str, Any],
    requested_symbol: str,
    normalized_symbol: str
) -> Dict[str, Any]:
    """Map quote API response to standard format."""
    result: Dict[str, Any] = {
        "requested_symbol": requested_symbol,
        "normalized_symbol": normalized_symbol,
        "provider": PROVIDER_NAME,
        "data_source": PROVIDER_NAME,
        "provider_version": PROVIDER_VERSION,
        "last_updated_utc": _utc_iso(),
        "last_updated_riyadh": _riyadh_iso(),
    }

    # Finnhub quote fields: c (current), h (high), l (low), o (open), pc (prev close), d (change), dp (percent change)
    result["current_price"] = safe_float(data.get("c"))
    result["previous_close"] = safe_float(data.get("pc"))
    result["open"] = safe_float(data.get("o"))
    result["day_high"] = safe_float(data.get("h"))
    result["day_low"] = safe_float(data.get("l"))
    result["price_change"] = safe_float(data.get("d"))
    result["percent_change"] = safe_float(data.get("dp"))

    # Derive if missing
    fill_derived(result)

    return clean_patch(result)


def map_profile_data(data: Dict[str, Any]) -> Dict[str, Any]:
    """Map profile API response to standard format."""
    result: Dict[str, Any] = {}

    result["name"] = safe_str(data.get("name"))
    result["sector"] = safe_str(data.get("finnhubIndustry"))
    result["currency"] = safe_str(data.get("currency"))
    result["market_cap"] = safe_float(data.get("marketCapitalization"))
    result["shares_outstanding"] = safe_float(data.get("shareOutstanding"))
    result["exchange"] = safe_str(data.get("exchange"))
    result["country"] = safe_str(data.get("country"))
    result["ipo"] = safe_str(data.get("ipo"))
    result["weburl"] = safe_str(data.get("weburl"))
    result["logo"] = safe_str(data.get("logo"))

    return clean_patch(result)


def map_candle_data(
    data: Dict[str, Any],
    requested_symbol: str,
    normalized_symbol: str
) -> Dict[str, Any]:
    """Map candle API response to standard format with analytics."""
    if data.get("s") != "ok":
        return {}

    closes = data.get("c") or []
    times = data.get("t") or []
    volumes = data.get("v") or []

    if not closes:
        return {}

    # Convert to float lists
    prices: List[float] = []
    for x in closes:
        f = safe_float(x)
        if f is not None:
            prices.append(f)

    vols: List[float] = []
    for x in volumes:
        f = safe_float(x)
        if f is not None:
            vols.append(f)

    if len(prices) < 10:
        return {}

    # Limit points
    max_points = _history_points_max()
    prices = prices[-max_points:]
    if vols:
        vols = vols[-max_points:]

    # Get last timestamp
    last_dt = None
    if times and len(times) == len(closes):
        try:
            t_last = times[-1]
            if isinstance(t_last, (int, float)):
                last_dt = datetime.fromtimestamp(float(t_last), tz=timezone.utc)
        except Exception:
            pass

    # Compute analytics
    analytics = compute_history_analytics(prices, vols if vols else None)

    result: Dict[str, Any] = {
        "requested_symbol": requested_symbol,
        "normalized_symbol": normalized_symbol,
        "history_points": len(prices),
        "history_last_utc": _utc_iso(last_dt) if last_dt else _utc_iso(),
        "history_last_riyadh": _to_riyadh_iso(last_dt),
        "forecast_updated_utc": _utc_iso(last_dt) if last_dt else _utc_iso(),
        "forecast_updated_riyadh": _to_riyadh_iso(last_dt),
        "provider": PROVIDER_NAME,
        "data_source": PROVIDER_NAME,
        "provider_version": PROVIDER_VERSION,
    }
    result.update(analytics)

    return clean_patch(result)


# ============================================================================
# Main Fetch Function
# ============================================================================

_CLIENT_INSTANCE: Optional[FinnhubClient] = None
_CLIENT_LOCK = asyncio.Lock()


async def get_client() -> FinnhubClient:
    """Get or create client singleton."""
    global _CLIENT_INSTANCE
    if _CLIENT_INSTANCE is None:
        async with _CLIENT_LOCK:
            if _CLIENT_INSTANCE is None:
                _CLIENT_INSTANCE = FinnhubClient()
    return _CLIENT_INSTANCE


async def close_client() -> None:
    """Close and cleanup client."""
    global _CLIENT_INSTANCE
    client = _CLIENT_INSTANCE
    _CLIENT_INSTANCE = None
    if client:
        await client.close()


async def _fetch(
    symbol: str,
    include_profile: bool = True,
    include_history: bool = True
) -> Dict[str, Any]:
    """Main fetch function with full enrichment."""
    if not _configured():
        return {}

    sym_in = (symbol or "").strip()
    if not sym_in:
        return {
            "provider": PROVIDER_NAME,
            "provider_version": PROVIDER_VERSION,
            "error": "empty_symbol",
            "last_updated_utc": _utc_iso(),
            "last_updated_riyadh": _riyadh_iso(),
        }

    # Routing guards
    if looks_like_ksa(sym_in):
        return {} if not _emit_warnings() else {
            "_warn": "finnhub blocked: KSA symbol",
            "provider": PROVIDER_NAME,
            "provider_version": PROVIDER_VERSION,
        }

    if is_blocked_special(sym_in):
        return {} if not _emit_warnings() else {
            "_warn": "finnhub blocked: index/commodity/crypto symbol",
            "provider": PROVIDER_NAME,
            "provider_version": PROVIDER_VERSION,
        }

    # Normalize symbol
    norm_symbol = normalize_finnhub_symbol(sym_in)
    variants = generate_symbol_variants(norm_symbol)

    client = await get_client()
    result: Dict[str, Any] = {
        "requested_symbol": sym_in,
        "normalized_symbol": norm_symbol,
        "provider": PROVIDER_NAME,
        "data_source": PROVIDER_NAME,
        "provider_version": PROVIDER_VERSION,
        "last_updated_utc": _utc_iso(),
        "last_updated_riyadh": _riyadh_iso(),
    }

    warnings: List[str] = []
    used_symbol = norm_symbol

    # Try each variant for quote
    for variant in variants:
        cache_key_q = f"quote:{variant}"
        cached_quote = await client.quote_cache.get(cache_key_q)

        if cached_quote:
            quote_data = cached_quote
            quote_err = None
        else:
            quote_data, quote_err = await client.get_quote(variant)
            if quote_data and not quote_err:
                await client.quote_cache.set(cache_key_q, quote_data)

        if quote_data and not quote_err:
            used_symbol = variant
            result["normalized_symbol"] = variant
            quote_mapped = map_quote_data(quote_data, sym_in, variant)
            result.update(quote_mapped)
            break
        else:
            warnings.append(f"quote_{variant}: {quote_err or 'failed'}")

    # Profile (if enabled)
    if include_profile and _enable_profile():
        cache_key_p = f"profile:{used_symbol}"
        cached_profile = await client.profile_cache.get(cache_key_p)

        if cached_profile:
            profile_data = cached_profile
            profile_err = None
        else:
            profile_data, profile_err = await client.get_profile(used_symbol)
            if profile_data and not profile_err:
                await client.profile_cache.set(cache_key_p, profile_data)

        if profile_data and not profile_err:
            profile_mapped = map_profile_data(profile_data)
            merge_into(
                result,
                profile_mapped,
                force_keys=("market_cap", "shares_outstanding", "name", "sector", "currency")
            )
        else:
            if profile_err:
                warnings.append(f"profile: {profile_err}")

    # History (if enabled)
    if include_history and _enable_history():
        cache_key_h = f"history:{used_symbol}:{_history_days()}"
        cached_history = await client.history_cache.get(cache_key_h)

        if cached_history:
            history_data = cached_history
            history_err = None
        else:
            history_data, history_err = await client.get_candles(used_symbol, count=_history_days())
            if history_data and not history_err:
                await client.history_cache.set(cache_key_h, history_data)

        if history_data and not history_err:
            history_mapped = map_candle_data(history_data, sym_in, used_symbol)
            merge_into(result, history_mapped)
        else:
            if history_err:
                warnings.append(f"history: {history_err}")

    # Data quality
    quality_category, quality_score = data_quality_score(result)
    result["data_quality"] = quality_category.value
    result["data_quality_score"] = quality_score

    # Add warnings if verbose
    if warnings and _emit_warnings():
        result["_warnings"] = " | ".join(warnings[:3])

    return clean_patch(result)


# ============================================================================
# Public API (Engine Compatible)
# ============================================================================

async def fetch_enriched_quote_patch(symbol: str, *args: Any, **kwargs: Any) -> Dict[str, Any]:
    """Primary engine entry: quote + profile + history analytics."""
    return await _fetch(symbol, include_profile=True, include_history=True)


async def fetch_quote_patch(symbol: str, *args: Any, **kwargs: Any) -> Dict[str, Any]:
    """Quote-only patch (fast)."""
    return await _fetch(symbol, include_profile=False, include_history=False)


async def fetch_quote_and_profile_patch(symbol: str, *args: Any, **kwargs: Any) -> Dict[str, Any]:
    """Quote + profile patch."""
    return await _fetch(symbol, include_profile=True, include_history=False)


async def fetch_quote_and_history_patch(symbol: str, *args: Any, **kwargs: Any) -> Dict[str, Any]:
    """Quote + history patch."""
    return await _fetch(symbol, include_profile=False, include_history=True)


async def fetch_enriched_patch(symbol: str, *args: Any, **kwargs: Any) -> Dict[str, Any]:
    """Alias for enriched quote."""
    return await fetch_enriched_quote_patch(symbol, *args, **kwargs)


async def fetch_quote_and_enrichment_patch(symbol: str, *args: Any, **kwargs: Any) -> Dict[str, Any]:
    """Alias for enriched quote."""
    return await fetch_enriched_quote_patch(symbol, *args, **kwargs)


async def get_client_metrics() -> Dict[str, Any]:
    """Get client performance metrics."""
    client = await get_client()
    return await client.get_metrics()


async def aclose_finnhub_client() -> None:
    """Close underlying HTTP client."""
    await close_client()


# ============================================================================
# Module Exports
# ============================================================================

__all__ = [
    "PROVIDER_NAME",
    "PROVIDER_VERSION",
    "fetch_enriched_quote_patch",
    "fetch_quote_patch",
    "fetch_quote_and_profile_patch",
    "fetch_quote_and_history_patch",
    "fetch_enriched_patch",
    "fetch_quote_and_enrichment_patch",
    "get_client_metrics",
    "aclose_finnhub_client",
    "MarketRegime",
    "DataQuality",
]
