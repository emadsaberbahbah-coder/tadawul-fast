#!/usr/bin/env python3
# core/providers/tadawul_provider.py
"""
================================================================================
Tadawul Provider (KSA Market Data) — v3.0.0 (ADVANCED PRODUCTION)
================================================================================

What's new in v3.0.0:
- ✅ Multi-model ensemble forecasting (ARIMA, Prophet-style, ML regression)
- ✅ Full technical indicator suite (MACD, RSI, Bollinger Bands, ATR, OBV)
- ✅ Market regime detection with confidence scoring
- ✅ Smart caching with TTL and LRU eviction
- ✅ Circuit breaker pattern with half-open state
- ✅ Advanced rate limiting with token bucket
- ✅ Singleflight pattern to prevent cache stampede
- ✅ Comprehensive metrics and monitoring
- ✅ Riyadh timezone awareness throughout
- ✅ Error backoff cache for failed symbols
- ✅ Data quality scoring and anomaly detection
- ✅ Enhanced candle parsing with multiple formats
- ✅ Strict KSA symbol normalization with shared helpers

Key Features:
- KSA market data (Tadawul)
- Real-time quotes with derived fields
- Fundamentals (market cap, P/E, dividend yield, etc.)
- Historical data with full technical analysis
- Ensemble forecasts with confidence levels
- Market regime classification
- Production-grade error handling
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

logger = logging.getLogger("core.providers.tadawul_provider")

PROVIDER_NAME = "tadawul"
PROVIDER_VERSION = "3.0.0"

USER_AGENT_DEFAULT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/124.0.0.0 Safari/537.36"
)

DEFAULT_TIMEOUT_SEC = 25.0
DEFAULT_RETRY_ATTEMPTS = 3
DEFAULT_RATE_LIMIT = 10.0  # requests per second
DEFAULT_CIRCUIT_BREAKER_THRESHOLD = 5
DEFAULT_CIRCUIT_BREAKER_TIMEOUT = 30.0

_ARABIC_DIGITS = str.maketrans("٠١٢٣٤٥٦٧٨٩", "0123456789")
_TRUTHY = {"1", "true", "yes", "y", "on", "t"}
_FALSY = {"0", "false", "no", "n", "off", "f"}

_KSA_CODE_RE = re.compile(r"^\d{3,6}$", re.IGNORECASE)
_KSA_SYMBOL_RE = re.compile(r"^\d{3,6}(\.SR)?$", re.IGNORECASE)


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

def _try_import_shared_ksa_helpers() -> Tuple[Optional[Any], Optional[Any]]:
    """Safely import shared KSA normalizer if available."""
    try:
        from core.symbols.normalize import normalize_ksa_symbol as _nksa
        from core.symbols.normalize import looks_like_ksa as _lk
        return _nksa, _lk
    except Exception:
        return None, None


_SHARED_NORM_KSA, _SHARED_LOOKS_KSA = _try_import_shared_ksa_helpers()


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


def _safe_str(x: Any) -> Optional[str]:
    if x is None:
        return None
    s = str(x).strip()
    return s if s else None


def _configured() -> bool:
    if not _env_bool("TADAWUL_ENABLED", True):
        return False
    return bool(_safe_str(_env_str("TADAWUL_QUOTE_URL", "")))


def _emit_warnings() -> bool:
    return _env_bool("TADAWUL_VERBOSE_WARNINGS", False)


def _enable_fundamentals() -> bool:
    return _env_bool("TADAWUL_ENABLE_FUNDAMENTALS", True)


def _enable_history() -> bool:
    return _env_bool("TADAWUL_ENABLE_HISTORY", True)


def _enable_forecast() -> bool:
    return _env_bool("TADAWUL_ENABLE_FORECAST", True)


def _enable_ml() -> bool:
    return _env_bool("TADAWUL_ENABLE_ML", True) and SKLEARN_AVAILABLE


def _timeout_sec() -> float:
    t = _env_float("TADAWUL_TIMEOUT_SEC", DEFAULT_TIMEOUT_SEC)
    return max(5.0, float(t))


def _retry_attempts() -> int:
    r = _env_int("TADAWUL_RETRY_ATTEMPTS", DEFAULT_RETRY_ATTEMPTS)
    return max(1, int(r))


def _retry_delay_sec() -> float:
    d = _env_float("TADAWUL_RETRY_DELAY_SEC", 0.25)
    return max(0.05, float(d))


def _rate_limit() -> float:
    return _env_float("TADAWUL_RATE_LIMIT", DEFAULT_RATE_LIMIT)


def _circuit_breaker_enabled() -> bool:
    return _env_bool("TADAWUL_CIRCUIT_BREAKER", True)


def _circuit_breaker_threshold() -> int:
    return _env_int("TADAWUL_CB_THRESHOLD", DEFAULT_CIRCUIT_BREAKER_THRESHOLD)


def _circuit_breaker_timeout() -> float:
    return _env_float("TADAWUL_CB_TIMEOUT", DEFAULT_CIRCUIT_BREAKER_TIMEOUT)


def _max_concurrency() -> int:
    return max(2, _env_int("TADAWUL_MAX_CONCURRENCY", 25))


def _quote_ttl_sec() -> float:
    ttl = _env_float("TADAWUL_QUOTE_TTL_SEC", 15.0)
    return max(5.0, float(ttl))


def _fund_ttl_sec() -> float:
    ttl = _env_float("TADAWUL_FUNDAMENTALS_TTL_SEC", 21600.0)  # 6 hours
    return max(300.0, float(ttl))


def _hist_ttl_sec() -> float:
    ttl = _env_float("TADAWUL_HISTORY_TTL_SEC", 1800.0)  # 30 minutes
    return max(300.0, float(ttl))


def _err_ttl_sec() -> float:
    ttl = _env_float("TADAWUL_ERROR_TTL_SEC", 10.0)  # Backoff for failed symbols
    return max(5.0, float(ttl))


def _history_days() -> int:
    d = _env_int("TADAWUL_HISTORY_DAYS", 500)
    return max(60, int(d))


def _history_points_max() -> int:
    n = _env_int("TADAWUL_HISTORY_POINTS_MAX", 1000)
    return max(100, int(n))


def _extra_headers() -> Dict[str, str]:
    """Parse extra headers from JSON env var."""
    raw = _env_str("TADAWUL_HEADERS_JSON", "")
    if not raw:
        return {}
    try:
        js = json.loads(raw)
        if isinstance(js, dict):
            out: Dict[str, str] = {}
            for k, v in js.items():
                ks = _safe_str(k)
                vs = _safe_str(v)
                if ks and vs:
                    out[ks] = vs
            return out
    except Exception:
        pass
    return {}


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


# ============================================================================
# KSA Symbol Normalization (Strict)
# ============================================================================

def normalize_ksa_symbol(symbol: str) -> str:
    """
    Strict KSA symbol normalization.
    Examples:
    - "1234" -> "1234.SR"
    - "TADAWUL:1234" -> "1234.SR"
    - "0123.SR" -> "0123.SR"
    - "AAPL" -> "" (non-KSA)
    """
    raw = (symbol or "").strip()
    if not raw:
        return ""

    # Translate Arabic digits
    raw = raw.translate(_ARABIC_DIGITS).strip()

    # Use shared normalizer if available
    if callable(_SHARED_NORM_KSA):
        try:
            s = (_SHARED_NORM_KSA(raw) or "").strip().upper()
            if s.endswith(".SR"):
                code = s[:-3].strip()
                return f"{code}.SR" if _KSA_CODE_RE.match(code) else ""
            return ""
        except Exception:
            pass

    # Manual normalization
    s = raw.strip().upper()

    # Remove TADAWUL prefix
    if s.startswith("TADAWUL:"):
        s = s.split(":", 1)[1].strip()

    # Handle .SR suffix
    if s.endswith(".SR"):
        code = s[:-3].strip()
        return f"{code}.SR" if _KSA_CODE_RE.match(code) else ""

    # Just the code
    if _KSA_CODE_RE.match(s):
        return f"{s}.SR"

    return ""


def extract_symbol_code(symbol: str) -> str:
    """Extract the numeric code from a normalized symbol."""
    sym = normalize_ksa_symbol(symbol)
    if sym and sym.endswith(".SR"):
        return sym[:-3]
    return sym


def format_url(tpl: str, symbol: str, *, days: Optional[int] = None) -> str:
    """
    Format URL template with symbol and optional parameters.
    Supports {symbol}, {code} placeholders.
    """
    sym = normalize_ksa_symbol(symbol)
    if not sym:
        return tpl

    code = sym[:-3] if sym.endswith(".SR") else sym
    url = tpl.replace("{symbol}", sym).replace("{code}", code)

    if days is not None:
        url = url.replace("{days}", str(int(days)))

    return url


# ============================================================================
# Safe Type Helpers
# ============================================================================

def safe_float(val: Any) -> Optional[float]:
    """Safely convert value to float, handling Arabic digits and currency."""
    if val is None:
        return None
    try:
        if isinstance(val, (int, float)):
            f = float(val)
            if math.isnan(f) or math.isinf(f):
                return None
            return f

        s = str(val).strip()
        if not s or s in {"-", "—", "N/A", "NA", "null", "None", ""}:
            return None

        # Handle Arabic digits and currency symbols
        s = s.translate(_ARABIC_DIGITS)
        s = s.replace("٬", ",").replace("٫", ".")
        s = s.replace("−", "-")  # unicode minus
        s = s.replace("SAR", "").replace("ريال", "").strip()
        s = s.replace("%", "").replace(",", "").replace("+", "").strip()

        # Handle parentheses for negative numbers
        if s.startswith("(") and s.endswith(")"):
            s = "-" + s[1:-1].strip()

        # Handle K/M/B suffixes
        m = re.match(r"^(-?\d+(\.\d+)?)([KMB])$", s, re.IGNORECASE)
        mult = 1.0
        if m:
            num = m.group(1)
            suf = m.group(3).upper()
            mult = 1_000.0 if suf == "K" else 1_000_000.0 if suf == "M" else 1_000_000_000.0
            s = num

        f = float(s) * mult
        if math.isnan(f) or math.isinf(f):
            return None
        return f
    except Exception:
        return None


def safe_int(val: Any) -> Optional[int]:
    f = safe_float(val)
    if f is None:
        return None
    try:
        return int(round(f))
    except Exception:
        return None


def safe_str(val: Any) -> Optional[str]:
    if val is None:
        return None
    s = str(val).strip()
    return s if s else None


def pick(d: Any, *keys: str) -> Any:
    """Pick first existing key from dict."""
    if not isinstance(d, dict):
        return None
    for k in keys:
        if k in d:
            return d[k]
    return None


def unwrap_common_envelopes(js: Union[dict, list]) -> Union[dict, list]:
    """Unwrap common API wrappers like {"data": {...}}."""
    current = js
    for _ in range(3):
        if isinstance(current, dict):
            for key in ("data", "result", "payload", "quote", "profile", "response"):
                if key in current and isinstance(current[key], (dict, list)):
                    current = current[key]
                    break
            else:
                break
        else:
            break
    return current


def coerce_dict(data: Union[dict, list]) -> dict:
    """Convert to dict if possible, otherwise return empty dict."""
    if isinstance(data, dict):
        return data
    if isinstance(data, list) and data and isinstance(data[0], dict):
        return data[0]
    return {}


def find_first_value(
    obj: Any,
    keys: Sequence[str],
    max_depth: int = 7,
    max_nodes: int = 3000,
) -> Any:
    """DFS search for first occurrence of any key."""
    if obj is None:
        return None

    keys_lower = {str(k).strip().lower() for k in keys if k}
    if not keys_lower:
        return None

    queue: List[Tuple[Any, int]] = [(obj, 0)]
    seen: Set[int] = set()
    nodes = 0

    while queue:
        current, depth = queue.pop(0)
        if current is None or depth > max_depth:
            continue

        current_id = id(current)
        if current_id in seen:
            continue
        seen.add(current_id)

        nodes += 1
        if nodes > max_nodes:
            return None

        if isinstance(current, dict):
            for k, v in current.items():
                if str(k).strip().lower() in keys_lower:
                    return v
                queue.append((v, depth + 1))
        elif isinstance(current, list):
            for item in current:
                queue.append((item, depth + 1))

    return None


def pick_num(obj: Any, *keys: str) -> Optional[float]:
    return safe_float(find_first_value(obj, keys))


def pick_str(obj: Any, *keys: str) -> Optional[str]:
    return safe_str(find_first_value(obj, keys))


def pick_pct(obj: Any, *keys: str) -> Optional[float]:
    v = safe_float(find_first_value(obj, keys))
    if v is None:
        return None
    return v * 100.0 if abs(v) <= 1.0 else v


def clean_patch(p: Dict[str, Any]) -> Dict[str, Any]:
    """Remove None and empty string values."""
    out: Dict[str, Any] = {}
    for k, v in (p or {}).items():
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


def fill_derived_quote_fields(patch: Dict[str, Any]) -> None:
    """Calculate derived quote fields."""
    cur = safe_float(patch.get("current_price"))
    prev = safe_float(patch.get("previous_close"))
    vol = safe_float(patch.get("volume"))
    high = safe_float(patch.get("day_high"))
    low = safe_float(patch.get("day_low"))
    w52h = safe_float(patch.get("week_52_high"))
    w52l = safe_float(patch.get("week_52_low"))

    # Price change
    if patch.get("price_change") is None and cur is not None and prev is not None:
        patch["price_change"] = cur - prev

    # Percent change
    if patch.get("percent_change") is None and cur is not None and prev not in (None, 0.0):
        try:
            patch["percent_change"] = (cur - prev) / prev * 100.0
        except Exception:
            pass

    # Value traded
    if patch.get("value_traded") is None and cur is not None and vol is not None:
        patch["value_traded"] = cur * vol

    # Day range percent
    if cur is not None and high is not None and low is not None and cur != 0:
        patch["day_range_pct"] = ((high - low) / cur) * 100.0

    # 52-week position
    if cur is not None and w52h is not None and w52l is not None and w52h != w52l:
        patch["week_52_position_pct"] = ((cur - w52l) / (w52h - w52l)) * 100.0

    # Currency
    if patch.get("currency") is None:
        patch["currency"] = "SAR"


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
                    logger.info("Circuit breaker moved to HALF_OPEN")
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
# Singleflight Pattern (Prevent Cache Stampede)
# ============================================================================

class SingleFlight:
    """Deduplicate concurrent requests for the same key."""

    def __init__(self) -> None:
        self._lock = asyncio.Lock()
        self._futures: Dict[str, asyncio.Future] = {}

    async def run(self, key: str, coro_fn):
        async with self._lock:
            fut = self._futures.get(key)
            if fut is not None:
                return await fut

            fut = asyncio.get_event_loop().create_future()
            self._futures[key] = fut

        try:
            result = await coro_fn()
            if not fut.done():
                fut.set_result(result)
            return result
        except Exception as e:
            if not fut.done():
                fut.set_exception(e)
            raise
        finally:
            async with self._lock:
                self._futures.pop(key, None)


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

def compute_history_analytics(
    closes: List[float],
    highs: Optional[List[float]] = None,
    lows: Optional[List[float]] = None,
    volumes: Optional[List[float]] = None
) -> Dict[str, Any]:
    """Compute comprehensive history analytics."""
    if not closes or len(closes) < 10:
        return {}

    out: Dict[str, Any] = {}
    last = closes[-1]

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
        if len(closes) > days:
            prior = closes[-(days + 1)]
            if prior != 0:
                out[name] = float((last / prior - 1) * 100)

    # Moving averages
    for period in [20, 50, 200]:
        if len(closes) >= period:
            ma = sum(closes[-period:]) / period
            out[f"sma_{period}"] = float(ma)
            out[f"price_to_sma_{period}"] = float((last / ma - 1) * 100)

    # Volatility
    if len(closes) >= 31:
        returns = [closes[i] / closes[i - 1] - 1 for i in range(1, len(closes))]
        recent_returns = returns[-30:]
        out["volatility_30d"] = float(np.std(recent_returns) * math.sqrt(252) * 100)

    # Technical indicators
    ti = TechnicalIndicators()

    # RSI
    rsi_values = ti.rsi(closes, 14)
    if rsi_values and rsi_values[-1] is not None:
        out["rsi_14"] = float(rsi_values[-1])
        if out["rsi_14"] > 70:
            out["rsi_signal"] = "overbought"
        elif out["rsi_14"] < 30:
            out["rsi_signal"] = "oversold"
        else:
            out["rsi_signal"] = "neutral"

    # MACD
    macd = ti.macd(closes)
    if macd["macd"] and macd["macd"][-1] is not None:
        out["macd"] = float(macd["macd"][-1])
        out["macd_signal"] = float(macd["signal"][-1]) if macd["signal"][-1] else None
        out["macd_histogram"] = float(macd["histogram"][-1]) if macd["histogram"][-1] else None
        if out["macd"] and out["macd_signal"]:
            out["macd_cross"] = "bullish" if out["macd"] > out["macd_signal"] else "bearish"

    # Bollinger Bands
    bb = ti.bollinger_bands(closes)
    if bb["middle"][-1] is not None:
        out["bb_middle"] = float(bb["middle"][-1])
        out["bb_upper"] = float(bb["upper"][-1])
        out["bb_lower"] = float(bb["lower"][-1])
        if bb["upper"][-1] and bb["lower"][-1]:
            out["bb_position"] = float((last - bb["lower"][-1]) / (bb["upper"][-1] - bb["lower"][-1]))

    # ATR (if highs/lows available)
    if highs and lows and len(highs) == len(closes) and len(lows) == len(closes):
        atr_values = ti.atr(highs, lows, closes, 14)
        if atr_values and atr_values[-1] is not None:
            out["atr_14"] = float(atr_values[-1])

    # On-Balance Volume (if volume available)
    if volumes and len(volumes) == len(closes):
        obv_values = ti.obv(closes, volumes)
        if obv_values:
            out["obv"] = float(obv_values[-1])
            if len(obv_values) > 20:
                obv_trend = (obv_values[-1] / obv_values[-20] - 1) * 100
                out["obv_trend_pct"] = float(obv_trend)
                out["obv_signal"] = "bullish" if obv_trend > 2 else "bearish" if obv_trend < -2 else "neutral"

    # Maximum Drawdown
    peak = closes[0]
    max_dd = 0.0
    for price in closes:
        if price > peak:
            peak = price
        dd = (price / peak - 1) * 100
        if dd < max_dd:
            max_dd = dd
    out["max_drawdown_pct"] = float(max_dd)

    # Sharpe Ratio (assuming 0% risk-free rate)
    if len(closes) > 30:
        returns = [closes[i] / closes[i - 1] - 1 for i in range(1, len(closes))]
        avg_ret = float(np.mean(returns)) * 252
        std_ret = float(np.std(returns)) * math.sqrt(252)
        out["sharpe_ratio"] = float(avg_ret / std_ret) if std_ret != 0 else None

    # Market Regime
    detector = MarketRegimeDetector(closes)
    regime, confidence = detector.detect()
    out["market_regime"] = regime.value
    out["market_regime_confidence"] = confidence

    # Ensemble Forecast
    if _enable_forecast() and len(closes) >= 60:
        forecaster = EnsembleForecaster(closes, enable_ml=_enable_ml())

        for horizon, period in [(21, "1m"), (63, "3m"), (252, "12m")]:
            forecast = forecaster.forecast(horizon)
            if forecast.get("forecast_available"):
                out[f"expected_roi_{period}"] = forecast["ensemble"]["roi_pct"]
                out[f"forecast_price_{period}"] = forecast["ensemble"]["price"]
                out[f"target_price_{period}"] = forecast["ensemble"]["price"]
                out[f"forecast_range_low_{period}"] = forecast["ensemble"].get("price_range_low")
                out[f"forecast_range_high_{period}"] = forecast["ensemble"].get("price_range_high")

                if period == "12m":
                    out["forecast_confidence"] = forecast["confidence"] / 100.0  # 0..1 scale
                    out["forecast_confidence_pct"] = forecast["confidence"]      # 0..100 scale
                    out["forecast_confidence_level"] = forecast["confidence_level"]
                    out["forecast_models"] = forecast["models_used"]

        out["forecast_method"] = "ensemble_v3"
        out["forecast_source"] = "tadawul_ml_ensemble"

    return out


# ============================================================================
# Advanced Tadawul Client
# ============================================================================

class TadawulClient:
    """Advanced Tadawul API client with full feature set."""

    def __init__(self) -> None:
        self.timeout_sec = _timeout_sec()
        self.retry_attempts = _retry_attempts()
        self.base_delay = _retry_delay_sec()

        # URLs
        self.quote_url = _safe_str(_env_str("TADAWUL_QUOTE_URL", ""))
        self.fundamentals_url = _safe_str(_env_str("TADAWUL_FUNDAMENTALS_URL", ""))
        self.history_url = _safe_str(_env_str("TADAWUL_HISTORY_URL", "")) or _safe_str(_env_str("TADAWUL_CANDLES_URL", ""))

        # HTTP client
        timeout = httpx.Timeout(self.timeout_sec, connect=min(10.0, self.timeout_sec))
        headers = {
            "User-Agent": _env_str("TADAWUL_USER_AGENT", USER_AGENT_DEFAULT),
            "Accept": "application/json, text/plain, */*",
            "Accept-Language": "ar,en;q=0.9",
            "Accept-Encoding": "gzip, deflate, br",
        }
        headers.update(_extra_headers())

        self._client = httpx.AsyncClient(
            timeout=timeout,
            follow_redirects=True,
            headers=headers,
            limits=httpx.Limits(max_keepalive_connections=25, max_connections=50),
            http2=True
        )

        # Caches
        self.quote_cache = SmartCache(maxsize=7000, ttl=_quote_ttl_sec())
        self.fund_cache = SmartCache(maxsize=4000, ttl=_fund_ttl_sec())
        self.history_cache = SmartCache(maxsize=2500, ttl=_hist_ttl_sec())
        self.error_cache = SmartCache(maxsize=4000, ttl=_err_ttl_sec())

        # Rate limiting & circuit breaker
        self.semaphore = asyncio.Semaphore(_max_concurrency())
        self.rate_limiter = TokenBucket(_rate_limit())
        self.circuit_breaker = SmartCircuitBreaker(
            fail_threshold=_circuit_breaker_threshold(),
            cooldown_sec=_circuit_breaker_timeout()
        )

        # Singleflight
        self.singleflight = SingleFlight()

        # Metrics
        self.metrics: Dict[str, Any] = {
            "requests_total": 0,
            "requests_success": 0,
            "requests_failed": 0,
            "cache_hits": 0,
            "cache_misses": 0,
            "rate_limit_waits": 0,
            "circuit_breaker_blocks": 0,
            "singleflight_dedup": 0,
        }
        self._metrics_lock = asyncio.Lock()

        logger.info(
            f"TadawulClient v{PROVIDER_VERSION} initialized | "
            f"quote={bool(self.quote_url)} fund={bool(self.fundamentals_url)} hist={bool(self.history_url)} | "
            f"timeout={self.timeout_sec}s | retries={self.retry_attempts} | "
            f"rate={_rate_limit()}/s | cb={_circuit_breaker_threshold()}/{_circuit_breaker_timeout()}s"
        )

    async def _update_metric(self, name: str, inc: int = 1) -> None:
        async with self._metrics_lock:
            self.metrics[name] = self.metrics.get(name, 0) + inc

    async def _get_json(self, url: str) -> Tuple[Optional[Union[dict, list]], Optional[str]]:
        """Make HTTP request with full feature set."""
        await self._update_metric("requests_total")

        # Circuit breaker check
        if not await self.circuit_breaker.allow_request():
            await self._update_metric("circuit_breaker_blocks")
            return None, "circuit_breaker_open"

        # Rate limiting
        await self.rate_limiter.wait_and_acquire()

        last_err: Optional[str] = None

        async with self.semaphore:
            for attempt in range(self.retry_attempts):
                try:
                    resp = await self._client.get(url)
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
                        await self.circuit_breaker.on_failure()
                        await self._update_metric("requests_failed")
                        return None, f"HTTP {status}"

                    # Success
                    try:
                        data = resp.json()
                    except Exception:
                        await self.circuit_breaker.on_failure()
                        await self._update_metric("requests_failed")
                        return None, "invalid_json"

                    # Unwrap common envelopes
                    data = unwrap_common_envelopes(data)

                    if not isinstance(data, (dict, list)):
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

    def _map_quote(self, root: Any) -> Dict[str, Any]:
        """Map quote response to standard format."""
        patch: Dict[str, Any] = {}

        # Price fields
        patch["current_price"] = pick_num(root,
            "last", "last_price", "price", "close", "c", "tradingPrice", "regularMarketPrice")
        patch["previous_close"] = pick_num(root,
            "previous_close", "prev_close", "pc", "PreviousClose", "prevClose")
        patch["open"] = pick_num(root,
            "open", "o", "Open", "openPrice")
        patch["day_high"] = pick_num(root,
            "high", "day_high", "h", "High", "dayHigh", "sessionHigh")
        patch["day_low"] = pick_num(root,
            "low", "day_low", "l", "Low", "dayLow", "sessionLow")
        patch["volume"] = pick_num(root,
            "volume", "v", "Volume", "tradedVolume", "qty", "quantity")
        patch["value_traded"] = pick_num(root,
            "value_traded", "tradedValue", "turnover", "value", "tradeValue")

        # 52-week range
        patch["week_52_high"] = pick_num(root,
            "week_52_high", "fiftyTwoWeekHigh", "52w_high", "yearHigh")
        patch["week_52_low"] = pick_num(root,
            "week_52_low", "fiftyTwoWeekLow", "52w_low", "yearLow")

        # Change fields
        patch["price_change"] = pick_num(root,
            "change", "d", "price_change", "Change", "diff", "delta")
        patch["percent_change"] = pick_pct(root,
            "change_pct", "change_percent", "dp", "percent_change", "pctChange", "changePercent")

        # Fill derived fields
        fill_derived_quote_fields(patch)

        return clean_patch(patch)

    def _map_fundamentals(self, root: Any) -> Dict[str, Any]:
        """Map fundamentals response to standard format."""
        patch: Dict[str, Any] = {}

        # Identity
        patch["name"] = pick_str(root,
            "name", "company", "company_name", "CompanyName", "shortName", "longName")
        patch["sector"] = pick_str(root,
            "sector", "Sector", "sectorName")
        patch["industry"] = pick_str(root,
            "industry", "Industry", "industryName")
        patch["sub_sector"] = pick_str(root,
            "sub_sector", "subSector", "SubSector", "subSectorName", "subIndustry")

        # Valuation
        patch["market_cap"] = pick_num(root,
            "market_cap", "marketCap", "marketCapitalization", "MarketCap")
        patch["shares_outstanding"] = pick_num(root,
            "shares", "shares_outstanding", "shareOutstanding", "SharesOutstanding")
        patch["pe_ttm"] = pick_num(root,
            "pe", "pe_ttm", "trailingPE", "PE", "priceEarnings")
        patch["pb"] = pick_num(root,
            "pb", "priceToBook", "PBR", "price_book")
        patch["eps_ttm"] = pick_num(root,
            "eps", "eps_ttm", "trailingEps", "EPS")
        patch["dividend_yield"] = pick_pct(root,
            "dividend_yield", "divYield", "yield", "DividendYield")
        patch["beta"] = pick_num(root,
            "beta", "Beta")

        # Currency
        if patch.get("currency") is None:
            patch["currency"] = "SAR"

        return clean_patch(patch)

    def _parse_candles(self, js: Any) -> Tuple[List[float], List[float], List[float], List[float], Optional[datetime]]:
        """
        Parse candle data from various formats.
        Returns (closes, highs, lows, volumes, last_dt)
        """
        closes: List[float] = []
        highs: List[float] = []
        lows: List[float] = []
        volumes: List[float] = []
        last_dt: Optional[datetime] = None

        if isinstance(js, dict):
            # Array format: {"c": [...], "h": [...], "l": [...], "v": [...], "t": [...]}
            c_raw = js.get("c") or js.get("close") or js.get("closes")
            h_raw = js.get("h") or js.get("high") or js.get("highs")
            l_raw = js.get("l") or js.get("low") or js.get("lows")
            v_raw = js.get("v") or js.get("volume") or js.get("volumes")
            t_raw = js.get("t") or js.get("time") or js.get("timestamp") or js.get("timestamps")

            if isinstance(c_raw, list):
                for x in c_raw:
                    fx = safe_float(x)
                    if fx is not None:
                        closes.append(fx)
            if isinstance(h_raw, list):
                for x in h_raw:
                    fx = safe_float(x)
                    if fx is not None:
                        highs.append(fx)
            if isinstance(l_raw, list):
                for x in l_raw:
                    fx = safe_float(x)
                    if fx is not None:
                        lows.append(fx)
            if isinstance(v_raw, list):
                for x in v_raw:
                    fx = safe_float(x)
                    if fx is not None:
                        volumes.append(fx)

            if isinstance(t_raw, list) and t_raw:
                try:
                    t_last = t_raw[-1]
                    if isinstance(t_last, (int, float)):
                        last_dt = datetime.fromtimestamp(float(t_last), tz=timezone.utc)
                except Exception:
                    pass

            # List of candles format: [{"close":..., "high":..., "low":..., "volume":..., "date":...}, ...]
            if not closes:
                candles = js.get("candles") or js.get("data") or js.get("items") or js.get("prices") or js.get("history")
                if isinstance(candles, list) and candles and isinstance(candles[0], dict):
                    for item in candles:
                        c = safe_float(item.get("c") or item.get("close") or item.get("price"))
                        h = safe_float(item.get("h") or item.get("high"))
                        l = safe_float(item.get("l") or item.get("low"))
                        v = safe_float(item.get("v") or item.get("volume") or item.get("vol"))

                        if c is not None:
                            closes.append(c)
                        if h is not None:
                            highs.append(h)
                        if l is not None:
                            lows.append(l)
                        if v is not None:
                            volumes.append(v)

                        # Get last date
                        t = item.get("t") or item.get("time") or item.get("timestamp") or item.get("date")
                        if last_dt is None and t is not None:
                            try:
                                if isinstance(t, (int, float)):
                                    last_dt = datetime.fromtimestamp(float(t), tz=timezone.utc)
                                elif isinstance(t, str):
                                    last_dt = datetime.fromisoformat(t.replace('Z', '+00:00'))
                            except Exception:
                                pass

        # Limit to max points
        maxp = _history_points_max()
        closes = closes[-maxp:]
        highs = highs[-maxp:] if highs else []
        lows = lows[-maxp:] if lows else []
        volumes = volumes[-maxp:] if volumes else []

        return closes, highs, lows, volumes, last_dt

    async def fetch_quote_patch(self, symbol: str) -> Dict[str, Any]:
        """Fetch quote data only."""
        if not _configured():
            return {}

        sym = normalize_ksa_symbol(symbol)
        if not sym:
            return {} if not _emit_warnings() else {"_warn": "invalid_ksa_symbol"}

        if not self.quote_url:
            return {} if not _emit_warnings() else {"_warn": "quote_url_missing"}

        cache_key = f"quote:{sym}"

        # Check error cache
        if await self.error_cache.get(cache_key):
            return {} if not _emit_warnings() else {"_warn": "temporarily_backed_off"}

        # Check quote cache
        cached = await self.quote_cache.get(cache_key)
        if cached:
            await self._update_metric("cache_hits")
            return dict(cached)

        await self._update_metric("cache_misses")

        async def _fetch():
            url = format_url(self.quote_url, sym)
            js, err = await self._get_json(url)

            if js is None:
                if err:
                    await self.error_cache.set(cache_key, True)
                return {} if not _emit_warnings() else {"_warn": f"quote_failed: {err}"}

            root = coerce_dict(js)
            mapped = self._map_quote(root)

            if safe_float(mapped.get("current_price")) is None:
                await self.error_cache.set(cache_key, True)
                return {} if not _emit_warnings() else {"_warn": "quote_no_price"}

            result = {
                "requested_symbol": symbol,
                "symbol": sym,
                "provider": PROVIDER_NAME,
                "data_source": PROVIDER_NAME,
                "provider_version": PROVIDER_VERSION,
                "last_updated_utc": _utc_iso(),
                "last_updated_riyadh": _riyadh_iso(),
                **mapped
            }

            await self.quote_cache.set(cache_key, result)
            return result

        return await self.singleflight.run(cache_key, _fetch)

    async def fetch_fundamentals_patch(self, symbol: str) -> Dict[str, Any]:
        """Fetch fundamentals data only."""
        if not _configured() or not _enable_fundamentals():
            return {}

        sym = normalize_ksa_symbol(symbol)
        if not sym:
            return {}

        if not self.fundamentals_url:
            return {}

        cache_key = f"fund:{sym}"

        # Check error cache
        if await self.error_cache.get(cache_key):
            return {}

        # Check cache
        cached = await self.fund_cache.get(cache_key)
        if cached:
            await self._update_metric("cache_hits")
            return dict(cached)

        await self._update_metric("cache_misses")

        async def _fetch():
            url = format_url(self.fundamentals_url, sym)
            js, err = await self._get_json(url)

            if js is None:
                if err:
                    await self.error_cache.set(cache_key, True)
                return {} if not _emit_warnings() else {"_warn": f"fundamentals_failed: {err}"}

            root = coerce_dict(js)
            mapped = self._map_fundamentals(root)

            if not mapped:
                await self.error_cache.set(cache_key, True)
                return {} if not _emit_warnings() else {"_warn": "fundamentals_no_data"}

            result = {
                "provider": PROVIDER_NAME,
                "data_source": PROVIDER_NAME,
                "provider_version": PROVIDER_VERSION,
                **mapped
            }

            await self.fund_cache.set(cache_key, result)
            return result

        return await self.singleflight.run(cache_key, _fetch)

    async def fetch_history_patch(self, symbol: str) -> Dict[str, Any]:
        """Fetch historical data with full analytics."""
        if not _configured() or not _enable_history():
            return {}

        sym = normalize_ksa_symbol(symbol)
        if not sym:
            return {}

        if not self.history_url:
            return {}

        days = _history_days()
        cache_key = f"history:{sym}:{days}"

        # Check error cache
        if await self.error_cache.get(cache_key):
            return {}

        # Check cache
        cached = await self.history_cache.get(cache_key)
        if cached:
            await self._update_metric("cache_hits")
            return dict(cached)

        await self._update_metric("cache_misses")

        async def _fetch():
            url = format_url(self.history_url, sym, days=days)
            js, err = await self._get_json(url)

            if js is None:
                if err:
                    await self.error_cache.set(cache_key, True)
                return {} if not _emit_warnings() else {"_warn": f"history_failed: {err}"}

            closes, highs, lows, volumes, last_dt = self._parse_candles(js)

            if not closes or len(closes) < 10:
                await self.error_cache.set(cache_key, True)
                return {} if not _emit_warnings() else {"_warn": "history_insufficient_data"}

            analytics = compute_history_analytics(closes, highs, lows, volumes)

            result: Dict[str, Any] = {
                "requested_symbol": symbol,
                "normalized_symbol": sym,
                "history_points": len(closes),
                "history_last_utc": _utc_iso(last_dt) if last_dt else _utc_iso(),
                "history_last_riyadh": _to_riyadh_iso(last_dt),
                "provider": PROVIDER_NAME,
                "data_source": PROVIDER_NAME,
                "provider_version": PROVIDER_VERSION,
                **analytics
            }

            # Add forecast timestamps if forecast data present
            if any(k in analytics for k in ("expected_roi_1m", "expected_roi_3m", "expected_roi_12m", "forecast_method")):
                result["forecast_updated_utc"] = _utc_iso(last_dt) if last_dt else _utc_iso()
                result["forecast_updated_riyadh"] = _to_riyadh_iso(last_dt)
                result["forecast_source"] = "tadawul_history"

            await self.history_cache.set(cache_key, result)
            return result

        return await self.singleflight.run(cache_key, _fetch)

    async def fetch_enriched_quote_patch(self, symbol: str) -> Dict[str, Any]:
        """
        Fetch fully enriched data (quote + fundamentals + history).
        This is the main method for dashboard consumption.
        """
        if not _configured():
            return {}

        sym = normalize_ksa_symbol(symbol)
        if not sym:
            return {} if not _emit_warnings() else {"_warn": "invalid_ksa_symbol"}

        # Fetch all components concurrently
        quote_task = self.fetch_quote_patch(symbol)
        fund_task = self.fetch_fundamentals_patch(symbol)
        hist_task = self.fetch_history_patch(symbol)

        quote_data, fund_data, hist_data = await asyncio.gather(
            quote_task, fund_task, hist_task, return_exceptions=True
        )

        # Handle exceptions
        if isinstance(quote_data, Exception):
            quote_data = {}
        if isinstance(fund_data, Exception):
            fund_data = {}
        if isinstance(hist_data, Exception):
            hist_data = {}

        # Start with quote data
        result = dict(quote_data) if quote_data else {
            "requested_symbol": symbol,
            "symbol": sym,
            "provider": PROVIDER_NAME,
            "data_source": PROVIDER_NAME,
            "provider_version": PROVIDER_VERSION,
            "last_updated_utc": _utc_iso(),
            "last_updated_riyadh": _riyadh_iso(),
        }

        # Merge fundamentals (only if missing)
        if fund_data:
            merge_into(
                result,
                fund_data,
                force_keys=("market_cap", "pe_ttm", "eps_ttm", "shares_outstanding", "name", "sector", "industry")
            )

        # Merge history (only if missing)
        if hist_data:
            for k, v in hist_data.items():
                if k not in result and v is not None:
                    result[k] = v

        # Data quality
        quality_category, quality_score = data_quality_score(result)
        result["data_quality"] = quality_category.value
        result["data_quality_score"] = quality_score

        # Collect warnings if enabled
        if _emit_warnings():
            warnings = []
            for src in (quote_data, fund_data, hist_data):
                if isinstance(src, dict) and "_warn" in src:
                    warnings.append(src["_warn"])
            if warnings:
                result["_warnings"] = " | ".join(warnings[:3])

        return clean_patch(result)

    async def get_metrics(self) -> Dict[str, Any]:
        """Get client metrics."""
        async with self._metrics_lock:
            metrics = dict(self.metrics)

        metrics["circuit_breaker"] = self.circuit_breaker.get_stats()
        metrics["cache_sizes"] = {
            "quote": await self.quote_cache.size(),
            "fund": await self.fund_cache.size(),
            "history": await self.history_cache.size(),
            "error": await self.error_cache.size(),
        }

        return metrics

    async def close(self) -> None:
        """Close HTTP client."""
        await self._client.aclose()


# ============================================================================
# Singleton Management
# ============================================================================

_CLIENT_INSTANCE: Optional[TadawulClient] = None
_CLIENT_LOCK = asyncio.Lock()


async def get_client() -> TadawulClient:
    """Get or create client singleton."""
    global _CLIENT_INSTANCE
    if _CLIENT_INSTANCE is None:
        async with _CLIENT_LOCK:
            if _CLIENT_INSTANCE is None:
                _CLIENT_INSTANCE = TadawulClient()
    return _CLIENT_INSTANCE


async def close_client() -> None:
    """Close and cleanup client."""
    global _CLIENT_INSTANCE
    client = _CLIENT_INSTANCE
    _CLIENT_INSTANCE = None
    if client:
        await client.close()


# ============================================================================
# Public API (Engine Compatible)
# ============================================================================

async def fetch_enriched_quote_patch(symbol: str, *args: Any, **kwargs: Any) -> Dict[str, Any]:
    """Primary engine entry: fully enriched quote."""
    client = await get_client()
    return await client.fetch_enriched_quote_patch(symbol)


async def fetch_quote_patch(symbol: str, *args: Any, **kwargs: Any) -> Dict[str, Any]:
    """Quote-only patch."""
    client = await get_client()
    return await client.fetch_quote_patch(symbol)


async def fetch_fundamentals_patch(symbol: str, *args: Any, **kwargs: Any) -> Dict[str, Any]:
    """Fundamentals-only patch."""
    client = await get_client()
    return await client.fetch_fundamentals_patch(symbol)


async def fetch_history_patch(symbol: str, *args: Any, **kwargs: Any) -> Dict[str, Any]:
    """History-only patch."""
    client = await get_client()
    return await client.fetch_history_patch(symbol)


async def fetch_quote_and_fundamentals_patch(symbol: str, *args: Any, **kwargs: Any) -> Dict[str, Any]:
    """Quote + fundamentals (no history)."""
    client = await get_client()
    quote = await client.fetch_quote_patch(symbol)
    fund = await client.fetch_fundamentals_patch(symbol)

    result = dict(quote)
    merge_into(result, fund)
    return clean_patch(result)


async def fetch_quote_and_history_patch(symbol: str, *args: Any, **kwargs: Any) -> Dict[str, Any]:
    """Quote + history (no fundamentals)."""
    client = await get_client()
    quote = await client.fetch_quote_patch(symbol)
    hist = await client.fetch_history_patch(symbol)

    result = dict(quote)
    merge_into(result, hist)
    return clean_patch(result)


async def get_client_metrics() -> Dict[str, Any]:
    """Get client performance metrics."""
    client = await get_client()
    return await client.get_metrics()


async def aclose_tadawul_client() -> None:
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
    "fetch_fundamentals_patch",
    "fetch_history_patch",
    "fetch_quote_and_fundamentals_patch",
    "fetch_quote_and_history_patch",
    "get_client_metrics",
    "aclose_tadawul_client",
    "normalize_ksa_symbol",
    "MarketRegime",
    "DataQuality",
]
