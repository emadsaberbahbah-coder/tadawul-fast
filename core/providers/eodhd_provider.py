#!/usr/bin/env python3
"""
core/providers/eodhd_provider.py
================================================================================
EODHD Provider -- v6.0.0 (GLOBAL PRIMARY / HISTORY+FUNDAMENTALS / SCHEMA-ALIGNED)
================================================================================

Purpose
-------
Primary provider for global market data:
  - Real-time quotes for international symbols
  - Historical OHLCV data with technical indicators
  - Fundamental data (financials, ratios, company info)
  - Symbol normalization across exchanges

v6.0.0 Changes (from v5.0.0)
----------------------------
Bug fixes:
  - fetch_price_history / fetch_ohlc_history no longer forward
    `*args` after a keyword (`days=days, *args, **kwargs`). That pattern
    raises `TypeError: got multiple values for 'days'` if any caller passes
    a single extra positional argument, because the unpacked *args binds
    positionally BEFORE the keyword — colliding with `days`.
  - change_p percent-to-ratio bug: EODHD's /real-time `change_p` is
    documented in percent units. v5.0.0 passed it through `_to_ratio`, which
    leaves values with |v| <= 1.5 untouched — so a typical 1.25% daily move
    became a ratio of 1.25 (i.e., 125%). Fixed via explicit `_pct_to_ratio`
    for this known-percent field.
  - `merged["beta_5y"] = merged.get("beta_5y") or merged.get("beta")` would
    overwrite a legitimate `beta_5y=0.0` (market-neutral) with `beta`.
    Replaced with an explicit None check. Same pattern fixed for the other
    alias assignments.
  - All asyncio primitives (Lock, Semaphore, TokenBucket's Lock) are now
    lazily initialized inside async methods rather than in __init__, so
    module import stays event-loop-free.

Cleanup:
  - Removed unused imports: `cast`, `field`.
  - Removed unused exception classes (EODHDAuthError, EODHDNotFoundError,
    EODHDRateLimitError -- defined, never raised, not in __all__).
  - Batch method no longer creates a second semaphore on top of
    self._semaphore -- single-tier gating.
  - _TTLCache eviction uses OrderedDict.popitem(last=False) for explicit FIFO.

Preserved for backward compatibility:
  - Every name in __all__.
  - Every environment variable name and default.
  - Response patch shape (field names, aliases, data_quality values).
  - Symbol normalization rules and exchange suffix maps.
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
from collections import OrderedDict
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Callable, Dict, Iterable, List, Optional, Tuple, Union

import httpx

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------

logger = logging.getLogger(__name__)
logger.addHandler(logging.NullHandler())

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

PROVIDER_NAME = "eodhd"
PROVIDER_VERSION = "6.0.0"
VERSION = PROVIDER_VERSION
PROVIDER_BATCH_SUPPORTED = True

DEFAULT_BASE_URL = "https://eodhistoricaldata.com/api"
DEFAULT_USER_AGENT = "TFB-EODHD/6.0.0 (Render)"

_TRUTHY = {"1", "true", "yes", "y", "on", "t", "enabled", "enable"}
_FALSY = {"0", "false", "no", "n", "off", "f", "disabled", "disable"}

_US_LIKE_RE = re.compile(r"^[A-Z0-9][A-Z0-9\-_]{0,16}$")
_KSA_RE = re.compile(r"^\d{3,6}\.SR$", re.IGNORECASE)

# Exchange suffix remapping for EODHD
_EODHD_SUFFIX_REMAP: Dict[str, str] = {
    "XETRA": "F",     # Deutsche Borse XETRA -> Frankfurt
    "XLON": "LSE",    # London XLON notation -> LSE
}

# Exchange name by suffix (for identity fallback)
_SUFFIX_EXCHANGE_MAP: Dict[str, str] = {
    "US": "NYSE/NASDAQ", "MC": "Bolsa de Madrid", "LSE": "London Stock Exchange",
    "L": "London Stock Exchange", "PA": "Euronext Paris", "F": "Deutsche Borse XETRA",
    "XETRA": "Deutsche Borse XETRA", "ST": "Nasdaq Stockholm", "OL": "Oslo Bors",
    "HE": "Nasdaq Helsinki", "CO": "Nasdaq Copenhagen", "JK": "Jakarta Stock Exchange",
    "TW": "Taiwan Stock Exchange", "HK": "HKEX", "TO": "Toronto Stock Exchange",
    "AU": "ASX", "SI": "Singapore Exchange", "SR": "Tadawul (Saudi Exchange)",
    "MX": "Bolsa Mexicana de Valores", "BR": "B3 Brazil", "SZ": "Shenzhen Stock Exchange",
    "SS": "Shanghai Stock Exchange", "BO": "BSE India", "NS": "NSE India",
    "KS": "Korea Stock Exchange", "T": "Tokyo Stock Exchange", "IL": "Tel Aviv Stock Exchange",
    "WA": "Warsaw Stock Exchange", "VI": "Vienna Stock Exchange", "SW": "SIX Swiss Exchange",
    "AS": "Euronext Amsterdam", "LI": "Lisbon Exchange", "IR": "Euronext Dublin",
}

# Country by suffix
_SUFFIX_COUNTRY_MAP: Dict[str, str] = {
    "US": "USA", "MC": "Spain", "LSE": "United Kingdom", "L": "United Kingdom",
    "PA": "France", "F": "Germany", "XETRA": "Germany", "ST": "Sweden", "OL": "Norway",
    "HE": "Finland", "CO": "Denmark", "JK": "Indonesia", "TW": "Taiwan", "HK": "Hong Kong",
    "TO": "Canada", "AU": "Australia", "SI": "Singapore", "SR": "Saudi Arabia",
    "MX": "Mexico", "BR": "Brazil", "SZ": "China", "SS": "China", "BO": "India", "NS": "India",
    "KS": "South Korea", "T": "Japan", "IL": "Israel", "WA": "Poland", "VI": "Austria",
    "SW": "Switzerland", "AS": "Netherlands", "LI": "Portugal", "IR": "Ireland",
}

# ---------------------------------------------------------------------------
# JSON Parser (orjson preferred)
# ---------------------------------------------------------------------------

try:
    import orjson  # type: ignore

    def _json_loads(data: Union[str, bytes]) -> Any:
        if isinstance(data, str):
            data = data.encode("utf-8")
        return orjson.loads(data)
except ImportError:
    import json

    def _json_loads(data: Union[str, bytes]) -> Any:
        if isinstance(data, bytes):
            data = data.decode("utf-8", errors="replace")
        return json.loads(data)


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class EODHDConfig:
    """Immutable configuration for EODHD provider."""
    api_key: str
    base_url: str = DEFAULT_BASE_URL
    timeout_sec: float = 15.0
    retry_attempts: int = 4
    retry_base_delay: float = 0.6
    max_concurrency: int = 20
    rate_limit_rps: float = 4.0
    rate_limit_burst: float = 8.0
    quote_ttl_sec: float = 12.0
    fund_ttl_sec: float = 21600.0
    history_ttl_sec: float = 1800.0
    history_days: int = 420
    history_window_52w: int = 252
    risk_free_rate: float = 0.03
    default_exchange: str = "US"
    append_exchange_suffix: bool = True
    enable_fundamentals: bool = True
    enable_history: bool = True
    ksa_blocked: bool = False
    allow_ksa_override: bool = False
    user_agent: str = DEFAULT_USER_AGENT

    @classmethod
    def from_env(cls) -> "EODHDConfig":
        """Load configuration from environment variables."""

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

        api_key = _env_str("EODHD_API_KEY") or _env_str("EODHD_API_TOKEN") or _env_str("EODHD_KEY")
        ksa_blocked_by_default = _env_bool("KSA_DISALLOW_EODHD", False)
        allow_ksa_override = _env_bool("ALLOW_EODHD_KSA", False) or _env_bool("EODHD_ALLOW_KSA", False)

        return cls(
            api_key=api_key,
            base_url=_env_str("EODHD_BASE_URL", DEFAULT_BASE_URL).rstrip("/"),
            timeout_sec=_env_float("EODHD_TIMEOUT_SEC", 15.0, lo=3.0, hi=120.0),
            retry_attempts=_env_int("EODHD_RETRY_ATTEMPTS", 4, lo=0, hi=10),
            retry_base_delay=_env_float("EODHD_RETRY_BASE_DELAY", 0.6, lo=0.0, hi=10.0),
            max_concurrency=_env_int("EODHD_MAX_CONCURRENCY", 20, lo=1, hi=100),
            rate_limit_rps=_env_float("EODHD_RATE_LIMIT_RPS", 4.0, lo=0.0, hi=50.0),
            rate_limit_burst=_env_float("EODHD_RATE_LIMIT_BURST", 8.0, lo=1.0, hi=200.0),
            quote_ttl_sec=_env_float("EODHD_QUOTE_TTL_SEC", 12.0, lo=1.0, hi=600.0),
            fund_ttl_sec=_env_float("EODHD_FUND_TTL_SEC", 21600.0, lo=60.0, hi=86400.0),
            history_ttl_sec=_env_float("EODHD_HISTORY_TTL_SEC", 1800.0, lo=60.0, hi=86400.0),
            history_days=_env_int("EODHD_HISTORY_DAYS", 420, lo=60, hi=5000),
            history_window_52w=_env_int("EODHD_HISTORY_WINDOW_52W", 252, lo=60, hi=800),
            risk_free_rate=_env_float("EODHD_RISK_FREE_RATE", 0.03, lo=0.0, hi=0.20),
            default_exchange=_env_str("EODHD_DEFAULT_EXCHANGE", "US").upper(),
            append_exchange_suffix=_env_bool("EODHD_APPEND_EXCHANGE_SUFFIX", True),
            enable_fundamentals=_env_bool("FUNDAMENTALS_ENABLED", True) or _env_bool("EODHD_ENABLE_FUNDAMENTALS", True),
            enable_history=_env_bool("EODHD_ENABLE_HISTORY", True),
            ksa_blocked=ksa_blocked_by_default and not allow_ksa_override,
            allow_ksa_override=allow_ksa_override,
            user_agent=_env_str("EODHD_USER_AGENT", DEFAULT_USER_AGENT),
        )

    def should_allow_ksa(self, symbol: str) -> bool:
        """Return True unless this is a KSA symbol and KSA fetching is blocked."""
        if not _KSA_RE.match(symbol):
            return True
        return not self.ksa_blocked


# ---------------------------------------------------------------------------
# Custom Exceptions (base + the ones that are actually raised)
# ---------------------------------------------------------------------------

class EODHDProviderError(Exception):
    """Base exception for EODHD provider errors."""


class EODHDFetchError(EODHDProviderError):
    """Raised when a fetch fails (used by the strict `quote()` wrapper)."""


# Defensive back-compat: keep these names available even though they're
# not raised by the module itself. External code may `except` on them.
class EODHDAuthError(EODHDProviderError):
    """Raised when API key is missing or invalid."""


class EODHDNotFoundError(EODHDProviderError):
    """Raised when symbol is not found."""


class EODHDRateLimitError(EODHDProviderError):
    """Raised when rate limit is exceeded."""


# ---------------------------------------------------------------------------
# Pure Utility Functions
# ---------------------------------------------------------------------------

def _utc_iso(dt: Optional[datetime] = None) -> str:
    """Get UTC time in ISO format."""
    d = dt or datetime.now(timezone.utc)
    if d.tzinfo is None:
        d = d.replace(tzinfo=timezone.utc)
    return d.astimezone(timezone.utc).isoformat()


def _riyadh_iso(dt: Optional[datetime] = None) -> str:
    """Get Riyadh time (UTC+3) in ISO format."""
    d = dt or datetime.now(timezone.utc)
    tz = timezone(timedelta(hours=3))
    if d.tzinfo is None:
        d = d.replace(tzinfo=timezone.utc)
    return d.astimezone(tz).isoformat()


def _safe_float(value: Any) -> Optional[float]:
    """Safely convert value to float; handles %, commas, None, NaN, inf."""
    if value is None:
        return None
    try:
        if isinstance(value, (int, float)) and not isinstance(value, bool):
            f = float(value)
        else:
            s = str(value).strip().replace(",", "")
            if not s:
                return None
            if s.endswith("%"):
                s = s[:-1].strip()
                f = float(s) / 100.0
            else:
                f = float(s)
        return None if (math.isnan(f) or math.isinf(f)) else f
    except (ValueError, TypeError):
        return None


def _safe_int(value: Any) -> Optional[int]:
    """Safely convert value to int."""
    f = _safe_float(value)
    return int(round(f)) if f is not None else None


def _safe_str(value: Any) -> Optional[str]:
    """Safely convert to non-empty stripped string; None for empty."""
    if value is None:
        return None
    try:
        s = str(value).strip()
        return s if s else None
    except Exception:
        return None


def _to_ratio(value: Any) -> Optional[float]:
    """
    Convert percent-like value to ratio using the abs>1.5 heuristic.

    WARNING: ambiguous for values between 0 and 1.5 (treated as ratios even
    if they were meant as percents). Use `_pct_to_ratio` for fields you
    KNOW are in percent units.
    """
    f = _safe_float(value)
    if f is None:
        return None
    return f / 100.0 if abs(f) > 1.5 else f


def _pct_to_ratio(value: Any) -> Optional[float]:
    """
    Convert a known-percent value to a ratio by dividing by 100.

    Use for fields whose API docs confirm percent units (e.g., EODHD's
    /real-time change_p). Safer than `_to_ratio` when the source scale is
    unambiguous.
    """
    f = _safe_float(value)
    return f / 100.0 if f is not None else None


def _safe_div(a: Any, b: Any) -> Optional[float]:
    """Safely divide two values; returns None if denominator is 0 or parse fails."""
    x = _safe_float(a)
    y = _safe_float(b)
    if x is None or y is None or y == 0:
        return None
    return x / y


def _first_present(*values: Any) -> Any:
    """Return the first non-None, non-empty-string value."""
    for v in values:
        if v is None:
            continue
        if isinstance(v, str) and not v.strip():
            continue
        return v
    return None


def _first_non_none(primary: Any, fallback: Any) -> Any:
    """
    Return `primary` if it's not None; otherwise `fallback`.

    Fixes the `a or b` pattern which incorrectly prefers `b` when `a == 0`.
    """
    return primary if primary is not None else fallback


def _sum_present(values: Iterable[Any]) -> Optional[float]:
    """Sum numeric values, ignoring None; returns None if no values."""
    total = 0.0
    seen = False
    for v in values:
        f = _safe_float(v)
        if f is not None:
            total += f
            seen = True
    return total if seen else None


def _clean_patch(patch: Dict[str, Any]) -> Dict[str, Any]:
    """Remove None and empty-string values from a patch dict."""
    return {k: v for k, v in patch.items() if v is not None and v != ""}


def _exchange_from_suffix(symbol_norm: str) -> Optional[str]:
    """Derive exchange name from the symbol suffix."""
    if "." in symbol_norm:
        suffix = symbol_norm.rsplit(".", 1)[1].upper()
        return _SUFFIX_EXCHANGE_MAP.get(suffix)
    return None


def _country_from_suffix(symbol_norm: str) -> Optional[str]:
    """Derive country from the symbol suffix."""
    if "." in symbol_norm:
        suffix = symbol_norm.rsplit(".", 1)[1].upper()
        return _SUFFIX_COUNTRY_MAP.get(suffix)
    return None


def normalize_eodhd_symbol(symbol: str, config: EODHDConfig) -> str:
    """
    Normalize a symbol to EODHD format.

    Examples:
        "AAPL"          -> "AAPL.US"
        "2222"          -> "2222.SR"
        "VOD.L"         -> "VOD.L"
        "TADAWUL:2222"  -> "2222.SR"
        "TICK.XETRA"    -> "TICK.F" (remapped)
    """
    if not symbol:
        return ""

    s = symbol.strip().upper()
    if not s:
        return ""

    # Special characters (indices, forex, futures) -- pass through
    if "=" in s or "^" in s or "/" in s:
        return s

    # KSA symbols
    if s.endswith(".SR") and _KSA_RE.match(s):
        return s

    # Delegate to core symbol normalizer if available
    try:
        from core.symbols.normalize import to_eodhd_symbol  # type: ignore

        if callable(to_eodhd_symbol):
            result = to_eodhd_symbol(s, default_exchange=config.default_exchange)
            if isinstance(result, str) and result.strip():
                return result.strip().upper()
    except ImportError:
        pass

    if "." in s:
        base, suffix = s.rsplit(".", 1)
        if suffix in _EODHD_SUFFIX_REMAP:
            remapped = _EODHD_SUFFIX_REMAP[suffix]
            logger.debug("Symbol suffix remap: %s.%s -> %s.%s", base, suffix, base, remapped)
            return f"{base}.{remapped}"
        if len(suffix) >= 2:
            return s
        if config.append_exchange_suffix:
            return f"{s}.{config.default_exchange}"
        return s

    if config.append_exchange_suffix and _US_LIKE_RE.match(s):
        return f"{s}.{config.default_exchange}"

    return s


# ---------------------------------------------------------------------------
# Statistics Helpers (no numpy / pandas)
# ---------------------------------------------------------------------------

def _daily_returns(closes: List[float]) -> List[float]:
    """Calculate daily returns from close prices."""
    returns: List[float] = []
    for i in range(1, len(closes)):
        p0, p1 = closes[i - 1], closes[i]
        if p0 and p0 > 0 and p1 and p1 > 0:
            returns.append((p1 / p0) - 1.0)
    return returns


def _stdev(values: List[float]) -> Optional[float]:
    """Sample standard deviation (n-1 denominator)."""
    if len(values) < 2:
        return None
    mean = sum(values) / len(values)
    variance = sum((v - mean) ** 2 for v in values) / (len(values) - 1)
    return math.sqrt(max(0.0, variance))


def _max_drawdown(closes: List[float]) -> Optional[float]:
    """Maximum drawdown from running peak; negative number or 0."""
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
    return mdd


def _var_95_1d(returns: List[float]) -> Optional[float]:
    """95% daily VaR as a positive number (loss magnitude); 0 if no negatives."""
    if len(returns) < 20:
        return None
    sorted_returns = sorted(returns)
    idx = int(round(0.05 * (len(sorted_returns) - 1)))
    idx = max(0, min(len(sorted_returns) - 1, idx))
    q = sorted_returns[idx]
    return abs(q) if q < 0 else 0.0


def _sharpe_1y(returns: List[float], rf_annual: float) -> Optional[float]:
    """Annualized Sharpe ratio (assumes 252 trading days)."""
    if len(returns) < 60:
        return None
    mu = sum(returns) / len(returns)
    sd = _stdev(returns)
    if sd is None or sd == 0:
        return None
    rf_daily = rf_annual / 252.0
    return ((mu - rf_daily) / sd) * math.sqrt(252.0)


def _annualized_vol(returns: List[float]) -> Optional[float]:
    """Annualized volatility from daily returns."""
    sd = _stdev(returns)
    return sd * math.sqrt(252.0) if sd is not None else None


def _rsi_14(closes: List[float]) -> Optional[float]:
    """RSI(14) with Wilder's smoothing; 0-100 or None."""
    if len(closes) < 15:
        return None

    gains: List[float] = []
    losses: List[float] = []
    for i in range(1, len(closes)):
        delta = closes[i] - closes[i - 1]
        gains.append(max(delta, 0.0))
        losses.append(abs(min(delta, 0.0)))

    if len(gains) < 14:
        return None

    period = 14
    avg_gain = sum(gains[:period]) / period
    avg_loss = sum(losses[:period]) / period

    for i in range(period, len(gains)):
        avg_gain = ((avg_gain * (period - 1)) + gains[i]) / period
        avg_loss = ((avg_loss * (period - 1)) + losses[i]) / period

    if avg_loss == 0:
        return 100.0

    rs = avg_gain / avg_loss
    return 100.0 - (100.0 / (1.0 + rs))


# ---------------------------------------------------------------------------
# Async Primitives: SingleFlight + TTL Cache + TokenBucket
# ---------------------------------------------------------------------------

class _SingleFlight:
    """Deduplicate concurrent requests for the same key."""

    def __init__(self) -> None:
        self._lock: Optional[asyncio.Lock] = None
        self._calls: Dict[str, asyncio.Future] = {}

    def _get_lock(self) -> asyncio.Lock:
        if self._lock is None:
            self._lock = asyncio.Lock()
        return self._lock

    async def do(self, key: str, coro_factory: Callable[[], Any]) -> Any:
        """Execute coroutine; concurrent callers for the same key share the result."""
        lock = self._get_lock()
        async with lock:
            future = self._calls.get(key)
            if future is None:
                future = asyncio.get_running_loop().create_future()
                self._calls[key] = future
                owner = True
            else:
                owner = False

        if not owner:
            return await future

        try:
            result = await coro_factory()
            if not future.done():
                future.set_result(result)
            return result
        except Exception as exc:
            if not future.done():
                future.set_exception(exc)
            raise
        finally:
            async with lock:
                self._calls.pop(key, None)


@dataclass
class _CacheItem:
    """Cache item with expiration timestamp."""
    expires_at: float
    value: Any


class _TTLCache:
    """Async-safe TTL cache with FIFO eviction on overflow."""

    def __init__(self, max_size: int, ttl_sec: float):
        self.max_size = max(128, max_size)
        self.ttl_sec = max(1.0, ttl_sec)
        self._cache: "OrderedDict[str, _CacheItem]" = OrderedDict()
        self._lock: Optional[asyncio.Lock] = None

    def _get_lock(self) -> asyncio.Lock:
        if self._lock is None:
            self._lock = asyncio.Lock()
        return self._lock

    async def get(self, key: str) -> Optional[Any]:
        """Get value from cache if not expired."""
        now = time.monotonic()
        async with self._get_lock():
            item = self._cache.get(key)
            if not item:
                return None
            if item.expires_at > now:
                return item.value
            self._cache.pop(key, None)
            return None

    async def set(self, key: str, value: Any, ttl_sec: Optional[float] = None) -> None:
        """Set value in cache with TTL; FIFO-evicts on overflow."""
        now = time.monotonic()
        ttl = self.ttl_sec if ttl_sec is None else max(1.0, ttl_sec)
        async with self._get_lock():
            if key not in self._cache and len(self._cache) >= self.max_size:
                # Pop oldest insertion
                self._cache.popitem(last=False)
            self._cache[key] = _CacheItem(expires_at=now + ttl, value=value)


class _TokenBucket:
    """Token bucket rate limiter."""

    def __init__(self, rate_per_sec: float, burst: float):
        self.rate = max(0.0, rate_per_sec)
        self.capacity = max(1.0, burst)
        self.tokens = self.capacity
        self.last = time.monotonic()
        self._lock: Optional[asyncio.Lock] = None

    def _get_lock(self) -> asyncio.Lock:
        if self._lock is None:
            self._lock = asyncio.Lock()
        return self._lock

    async def wait(self, amount: float = 1.0) -> None:
        """Wait until `amount` tokens are available."""
        if self.rate <= 0:
            return
        amount = max(0.0001, amount)
        while True:
            async with self._get_lock():
                now = time.monotonic()
                elapsed = max(0.0, now - self.last)
                self.last = now
                self.tokens = min(self.capacity, self.tokens + elapsed * self.rate)
                if self.tokens >= amount:
                    self.tokens -= amount
                    return
                deficit = amount - self.tokens
                sleep_sec = deficit / self.rate if self.rate > 0 else 0.25
            await asyncio.sleep(min(1.0, max(0.05, sleep_sec)))


# ---------------------------------------------------------------------------
# EODHD Client
# ---------------------------------------------------------------------------

class EODHDClient:
    """Async client for EODHD API."""

    def __init__(self, config: Optional[EODHDConfig] = None):
        self.config = config or EODHDConfig.from_env()
        self._semaphore: Optional[asyncio.Semaphore] = None  # lazy
        self._bucket = _TokenBucket(
            rate_per_sec=self.config.rate_limit_rps,
            burst=self.config.rate_limit_burst,
        )
        self._single_flight = _SingleFlight()

        self._quote_cache = _TTLCache(max_size=6000, ttl_sec=self.config.quote_ttl_sec)
        self._fund_cache = _TTLCache(max_size=3000, ttl_sec=self.config.fund_ttl_sec)
        self._hist_cache = _TTLCache(max_size=2000, ttl_sec=self.config.history_ttl_sec)

        self._client = httpx.AsyncClient(
            timeout=httpx.Timeout(self.config.timeout_sec),
            headers={"User-Agent": self.config.user_agent},
            limits=httpx.Limits(max_keepalive_connections=30, max_connections=60),
            http2=True,
        )

    def _get_semaphore(self) -> asyncio.Semaphore:
        if self._semaphore is None:
            self._semaphore = asyncio.Semaphore(self.config.max_concurrency)
        return self._semaphore

    def _base_params(self) -> Dict[str, str]:
        """Base query parameters for every API request."""
        return {"api_token": self.config.api_key, "fmt": "json"}

    # -- HTTP layer -----------------------------------------------------

    async def _request_json(
        self,
        endpoint: str,
        params: Optional[Dict[str, Any]] = None,
    ) -> Tuple[Optional[Any], Optional[str]]:
        """Make JSON request to EODHD API. Returns (payload, error_or_None)."""
        if not self.config.api_key:
            return None, "EODHD_API_KEY missing"

        url = f"{self.config.base_url}/{endpoint.lstrip('/')}"
        query_params = {**self._base_params(), **(params or {})}
        last_error: Optional[str] = None

        async with self._get_semaphore():
            for attempt in range(max(1, self.config.retry_attempts + 1)):
                await self._bucket.wait(1.0)

                try:
                    response = await self._client.get(url, params=query_params)
                    status_code = response.status_code

                    if status_code == 429:
                        retry_after = response.headers.get("Retry-After")
                        wait = (
                            float(retry_after)
                            if retry_after and retry_after.replace(".", "", 1).isdigit()
                            else min(15.0, 1.0 + attempt)
                        )
                        last_error = "HTTP 429"
                        await asyncio.sleep(wait)
                        continue

                    if status_code in (401, 403):
                        body_hint = ""
                        try:
                            body_hint = (response.text or "")[:160]
                        except Exception:
                            pass
                        return None, f"HTTP {status_code} auth_error {body_hint}".strip()

                    if 500 <= status_code < 600:
                        last_error = f"HTTP {status_code}"
                        base_delay = self.config.retry_base_delay * (2 ** max(0, attempt - 1))
                        await asyncio.sleep(min(12.0, random.uniform(0, base_delay + 0.25)))
                        continue

                    if status_code == 404:
                        return None, "HTTP 404 not_found"

                    if status_code >= 400:
                        return None, f"HTTP {status_code}"

                    try:
                        data = _json_loads(response.content)
                        return data, None
                    except Exception:
                        return None, "invalid_json_payload"

                except httpx.RequestError as exc:
                    last_error = f"network_error:{exc.__class__.__name__}"
                    base_delay = self.config.retry_base_delay * (2 ** max(0, attempt - 1))
                    await asyncio.sleep(min(12.0, random.uniform(0, base_delay + 0.25)))
                except Exception as exc:
                    last_error = f"unexpected_error:{exc.__class__.__name__}"
                    break

        return None, last_error or "request_failed"

    # -- Quote ----------------------------------------------------------

    async def _fetch_quote(self, symbol: str) -> Tuple[Dict[str, Any], Optional[str]]:
        """Fetch real-time quote for a symbol."""
        sym_norm = normalize_eodhd_symbol(symbol, self.config)
        if not sym_norm:
            return {}, "invalid_symbol"

        if not self.config.should_allow_ksa(sym_norm):
            return {}, "ksa_blocked"

        cache_key = f"q:{sym_norm}"
        cached = await self._quote_cache.get(cache_key)
        if cached:
            return cached, None

        async def _fetch() -> Tuple[Dict[str, Any], Optional[str]]:
            data, err = await self._request_json(f"real-time/{sym_norm}", params={})
            if err or not isinstance(data, dict):
                return {}, err or "bad_payload"

            close = _safe_float(_first_present(
                data.get("close"), data.get("adjusted_close"), data.get("last"), data.get("price"),
            ))
            prev = _safe_float(_first_present(
                data.get("previousClose"), data.get("previous_close"), data.get("prev_close"),
            ))
            open_px = _safe_float(_first_present(data.get("open"), data.get("dayOpen"), data.get("day_open")))
            high_px = _safe_float(_first_present(data.get("high"), data.get("dayHigh"), data.get("day_high")))
            low_px = _safe_float(_first_present(data.get("low"), data.get("dayLow"), data.get("day_low")))
            volume = _safe_float(_first_present(
                data.get("volume"), data.get("shareVolume"), data.get("avgVolume"),
            ))
            chg = _safe_float(_first_present(data.get("change"), data.get("price_change")))

            if chg is None and close is not None and prev is not None and prev != 0:
                chg = close - prev

            # v6.0.0 bug fix: EODHD's `change_p` is documented as PERCENT
            # (e.g., 1.25 for 1.25%). v5.0.0 piped it through `_to_ratio`,
            # which treats values with |v| <= 1.5 as ratios -- so a 1.25%
            # daily move became a 125% ratio. Use explicit /100 here.
            change_frac: Optional[float] = _pct_to_ratio(data.get("change_p"))
            if change_frac is None:
                # Fall back to the ambiguous keys via the heuristic
                change_frac = _to_ratio(_first_present(
                    data.get("percent_change"), data.get("changePercent"),
                ))
            if change_frac is None and close is not None and prev is not None and prev != 0:
                change_frac = (close / prev) - 1.0

            exchange = _safe_str(_first_present(
                data.get("exchange"), data.get("fullExchangeName"), data.get("primaryExchange"),
            ))
            currency = _safe_str(_first_present(data.get("currency"), data.get("currency_code")))
            market_cap = _safe_float(_first_present(
                data.get("market_cap"), data.get("marketCapitalization"), data.get("marketCapitalisation"),
            ))

            patch = _clean_patch({
                "symbol": symbol,
                "symbol_normalized": sym_norm,
                "provider": PROVIDER_NAME,
                "data_source": PROVIDER_NAME,
                "name": _safe_str(_first_present(data.get("name"), data.get("shortName"), data.get("longName"))),
                "exchange": exchange,
                "currency": currency,
                "current_price": close,
                "price": close,
                "previous_close": prev,
                "prev_close": prev,
                "day_open": open_px,
                "open": open_px,
                "day_high": high_px,
                "day_low": low_px,
                "volume": volume,
                "market_cap": market_cap,
                "price_change": chg,
                "change": chg,
                "percent_change": change_frac,
                "change_pct": change_frac,
                "timestamp": _safe_str(_first_present(data.get("timestamp"), data.get("date"))),
                "last_updated_utc": _utc_iso(),
                "last_updated_riyadh": _riyadh_iso(),
            })
            await self._quote_cache.set(cache_key, patch)
            return patch, None

        return await self._single_flight.do(cache_key, _fetch)

    # -- Fundamentals ---------------------------------------------------

    async def _fetch_fundamentals(self, symbol: str) -> Tuple[Dict[str, Any], Optional[str]]:
        """Fetch fundamentals data for a symbol."""
        sym_norm = normalize_eodhd_symbol(symbol, self.config)
        if not sym_norm:
            return {}, "invalid_symbol"

        if not self.config.should_allow_ksa(sym_norm):
            return {}, "ksa_blocked"

        cache_key = f"f:{sym_norm}"
        cached = await self._fund_cache.get(cache_key)
        if cached:
            return cached, None

        async def _fetch() -> Tuple[Dict[str, Any], Optional[str]]:
            data, err = await self._request_json(f"fundamentals/{sym_norm}", params={})
            if err or not isinstance(data, dict):
                return {}, err or "bad_payload"

            general = data.get("General") or {}
            highlights = data.get("Highlights") or {}
            valuation = data.get("Valuation") or {}
            shares = data.get("SharesStats") or {}
            tech = data.get("Technicals") or {}
            splits = data.get("SplitsDividends") or {}
            etf_data = data.get("ETF_Data") or {}
            financials = data.get("Financials") or {}

            def _statement_rows(section: str, periodicity: str) -> List[Dict[str, Any]]:
                sec = financials.get(section) or {}
                if not isinstance(sec, dict):
                    return []
                rows = sec.get(periodicity)
                if rows is None and periodicity == "quarterly":
                    rows = sec.get("quarter")
                if rows is None and periodicity == "yearly":
                    rows = sec.get("annual") or sec.get("year")
                if isinstance(rows, list):
                    return [r for r in rows if isinstance(r, dict)]
                if isinstance(rows, dict):
                    result = []
                    for k, v in rows.items():
                        if isinstance(v, dict):
                            v["date"] = k
                            result.append(v)
                    result.sort(key=lambda r: _safe_str(r.get("date")) or "", reverse=True)
                    return result
                return []

            def _pick_numeric(row: Dict[str, Any], *keys: str) -> Optional[float]:
                for k in keys:
                    if k in row:
                        f = _safe_float(row.get(k))
                        if f is not None:
                            return f
                lower_map = {str(k).lower(): v for k, v in row.items()}
                for k in keys:
                    f = _safe_float(lower_map.get(str(k).lower()))
                    if f is not None:
                        return f
                return None

            def _latest_numeric(rows: List[Dict[str, Any]], *keys: str) -> Optional[float]:
                for row in rows:
                    v = _pick_numeric(row, *keys)
                    if v is not None:
                        return v
                return None

            def _sum_latest_n(rows: List[Dict[str, Any]], n: int, *keys: str) -> Optional[float]:
                if not rows:
                    return None
                values: List[float] = []
                for row in rows[:max(1, n)]:
                    v = _pick_numeric(row, *keys)
                    if v is not None:
                        values.append(v)
                return sum(values) if values else None

            income_q = _statement_rows("Income_Statement", "quarterly")
            income_y = _statement_rows("Income_Statement", "yearly")
            cash_q = _statement_rows("Cash_Flow", "quarterly")
            balance_q = _statement_rows("Balance_Sheet", "quarterly")
            balance_y = _statement_rows("Balance_Sheet", "yearly")

            revenue_ttm = _sum_latest_n(income_q, 4, "totalRevenue", "revenue", "Revenue")
            gross_profit_ttm = _sum_latest_n(income_q, 4, "grossProfit", "GrossProfit")
            operating_income_ttm = _sum_latest_n(income_q, 4, "operatingIncome", "OperatingIncome")
            net_income_ttm = _sum_latest_n(income_q, 4, "netIncome", "NetIncome")
            cfo_ttm = _sum_latest_n(
                cash_q, 4, "totalCashFromOperatingActivities", "operatingCashFlow", "OperatingCashFlow",
            )
            capex_ttm = _sum_latest_n(
                cash_q, 4, "capitalExpenditures", "CapitalExpenditure", "capitalExpenditure",
            )

            fcf_ttm: Optional[float] = None
            if cfo_ttm is not None and capex_ttm is not None:
                fcf_ttm = cfo_ttm - abs(capex_ttm)
            elif cfo_ttm is not None:
                fcf_ttm = cfo_ttm

            # Revenue growth (quarterly YoY first, then yearly)
            latest_rev_q = _latest_numeric(income_q, "totalRevenue", "revenue", "Revenue")
            prev_year_rev_q = (
                _pick_numeric(income_q[4], "totalRevenue", "revenue", "Revenue")
                if len(income_q) >= 5 else None
            )
            revenue_growth_yoy: Optional[float] = None
            if latest_rev_q is not None and prev_year_rev_q is not None and prev_year_rev_q != 0:
                revenue_growth_yoy = (latest_rev_q / prev_year_rev_q) - 1.0
            if revenue_growth_yoy is None:
                latest_rev_y = _latest_numeric(income_y, "totalRevenue", "revenue", "Revenue")
                prev_rev_y = (
                    _pick_numeric(income_y[1], "totalRevenue", "revenue", "Revenue")
                    if len(income_y) >= 2 else None
                )
                if latest_rev_y is not None and prev_rev_y is not None and prev_rev_y != 0:
                    revenue_growth_yoy = (latest_rev_y / prev_rev_y) - 1.0

            gross_margin = _safe_div(gross_profit_ttm, revenue_ttm)
            operating_margin = _safe_div(operating_income_ttm, revenue_ttm)
            profit_margin = _safe_div(net_income_ttm, revenue_ttm)

            latest_bs = balance_q[0] if balance_q else (balance_y[0] if balance_y else {})
            total_assets = _pick_numeric(latest_bs, "totalAssets", "TotalAssets")
            total_equity = _pick_numeric(
                latest_bs,
                "totalStockholderEquity", "totalShareholderEquity", "shareholdersEquity",
                "totalEquity", "TotalStockholderEquity",
            )
            total_debt = _first_present(
                _pick_numeric(latest_bs, "shortLongTermDebtTotal", "ShortLongTermDebtTotal"),
                _pick_numeric(latest_bs, "totalDebt", "TotalDebt"),
                _sum_present([
                    _pick_numeric(latest_bs, "shortTermDebt", "ShortTermDebt"),
                    _pick_numeric(latest_bs, "longTermDebt", "LongTermDebt"),
                ]),
            )
            debt_to_equity = _safe_div(total_debt, total_equity)

            roe = _to_ratio(_first_present(highlights.get("ROE"), _safe_div(net_income_ttm, total_equity)))
            roa = _to_ratio(_first_present(highlights.get("ROA"), _safe_div(net_income_ttm, total_assets)))

            shares_outstanding = _safe_float(_first_present(
                shares.get("SharesOutstanding"), highlights.get("SharesOutstanding"),
            ))
            float_shares = _safe_float(_first_present(
                shares.get("SharesFloat"), shares.get("FloatShares"), highlights.get("SharesFloat"),
            ))
            market_cap = _safe_float(_first_present(
                highlights.get("MarketCapitalization"), general.get("MarketCapitalization"),
            ))

            week_52_high = _safe_float(_first_present(
                tech.get("52WeekHigh"), tech.get("WeekHigh52"), highlights.get("52WeekHigh"),
            ))
            week_52_low = _safe_float(_first_present(
                tech.get("52WeekLow"), tech.get("WeekLow52"), highlights.get("52WeekLow"),
            ))

            # Asset class inference
            asset_type = _safe_str(_first_present(general.get("Type"), general.get("Category"))) or ""
            asset_class: Optional[str] = None
            if asset_type:
                type_lower = asset_type.lower()
                if "common stock" in type_lower or "stock" in type_lower:
                    asset_class = "Equity"
                elif "etf" in type_lower:
                    asset_class = "ETF"
                elif "mutual fund" in type_lower:
                    asset_class = "Mutual Fund"
                elif "adr" in type_lower:
                    asset_class = "ADR"
                elif "reit" in type_lower:
                    asset_class = "REIT"
                elif etf_data:
                    asset_class = "ETF"

            country = _safe_str(_first_present(
                general.get("CountryISO"), general.get("CountryName"), general.get("Country"),
            ))
            exchange = _safe_str(_first_present(general.get("Exchange"), general.get("PrimaryExchange")))
            currency = _safe_str(_first_present(
                general.get("CurrencyCode"), general.get("Currency"), highlights.get("Currency"),
            ))

            dividend_yield = _to_ratio(_first_present(
                splits.get("ForwardAnnualDividendYield"),
                highlights.get("DividendYield"),
                splits.get("TrailingAnnualDividendYield"),
            ))
            payout_ratio = _to_ratio(_first_present(
                splits.get("PayoutRatio"), highlights.get("PayoutRatio"),
            ))

            patch = _clean_patch({
                "symbol": symbol,
                "symbol_normalized": sym_norm,
                "provider": PROVIDER_NAME,
                "data_source": PROVIDER_NAME,
                "name": _safe_str(_first_present(
                    general.get("Name"), general.get("ShortName"), general.get("LongName"),
                )),
                "exchange": exchange,
                "currency": currency,
                "country": country,
                "sector": _safe_str(general.get("Sector")),
                "industry": _safe_str(general.get("Industry")),
                "asset_class": asset_class,
                "asset_type": asset_class,
                "market_cap": market_cap,
                "enterprise_value": _safe_float(_first_present(
                    highlights.get("EnterpriseValue"), valuation.get("EnterpriseValue"),
                )),
                "shares_outstanding": shares_outstanding,
                "float_shares": float_shares,
                "shares_float": float_shares,
                "eps_ttm": _safe_float(_first_present(
                    highlights.get("EarningsShare"), highlights.get("DilutedEpsTTM"),
                )),
                "pe_ttm": _safe_float(_first_present(valuation.get("TrailingPE"), highlights.get("PERatio"))),
                "pe_forward": _safe_float(_first_present(valuation.get("ForwardPE"), highlights.get("ForwardPE"))),
                "forward_pe": _safe_float(_first_present(valuation.get("ForwardPE"), highlights.get("ForwardPE"))),
                "pb_ratio": _safe_float(_first_present(valuation.get("PriceBookMRQ"), valuation.get("PriceBook"))),
                "ps_ratio": _safe_float(_first_present(valuation.get("PriceSalesTTM"), valuation.get("PriceSales"))),
                "peg_ratio": _safe_float(_first_present(valuation.get("PEGRatio"), valuation.get("PegRatio"))),
                "ev_ebitda": _safe_float(_first_present(
                    valuation.get("EnterpriseValueEbitda"), valuation.get("EnterpriseValueEBITDA"),
                )),
                "dividend_yield": dividend_yield,
                "payout_ratio": payout_ratio,
                "roe": roe,
                "roa": roa,
                "net_margin": _to_ratio(_first_present(highlights.get("ProfitMargin"), profit_margin)),
                "revenue_growth": _to_ratio(_first_present(
                    highlights.get("RevenueGrowth"), revenue_growth_yoy,
                )),
                "revenue_growth_yoy": _to_ratio(_first_present(
                    highlights.get("RevenueGrowth"), revenue_growth_yoy,
                )),
                "beta": _safe_float(_first_present(tech.get("Beta"), highlights.get("Beta"))),
                "beta_5y": _safe_float(_first_present(tech.get("Beta"), highlights.get("Beta"))),
                "week_52_high": week_52_high,
                "week_52_low": week_52_low,
                "revenue_ttm": revenue_ttm,
                "revenue": revenue_ttm,
                "gross_margin": _to_ratio(_first_present(highlights.get("GrossMargin"), gross_margin)),
                "operating_margin": _to_ratio(_first_present(highlights.get("OperatingMargin"), operating_margin)),
                "profit_margin": _to_ratio(_first_present(highlights.get("ProfitMargin"), profit_margin)),
                "free_cash_flow_ttm": fcf_ttm,
                "fcf_ttm": fcf_ttm,
                "debt_to_equity": debt_to_equity,
                "d_e_ratio": debt_to_equity,
                "last_updated_utc": _utc_iso(),
                "last_updated_riyadh": _riyadh_iso(),
            })
            await self._fund_cache.set(cache_key, patch)
            return patch, None

        return await self._single_flight.do(cache_key, _fetch)

    # -- History + derived stats ---------------------------------------

    async def _fetch_history_stats(self, symbol: str) -> Tuple[Dict[str, Any], Optional[str]]:
        """Fetch historical OHLCV data and derive technical/risk statistics."""
        sym_norm = normalize_eodhd_symbol(symbol, self.config)
        if not sym_norm:
            return {}, "invalid_symbol"

        if not self.config.should_allow_ksa(sym_norm):
            return {}, "ksa_blocked"

        cache_key = f"h:{sym_norm}"
        cached = await self._hist_cache.get(cache_key)
        if cached:
            return cached, None

        async def _fetch() -> Tuple[Dict[str, Any], Optional[str]]:
            from_date = (
                datetime.now(timezone.utc) - timedelta(days=self.config.history_days)
            ).strftime("%Y-%m-%d")
            data, err = await self._request_json(f"eod/{sym_norm}", params={"from": from_date})
            if err or not isinstance(data, list):
                return {}, err or "bad_payload"

            rows = [r for r in data if isinstance(r, dict) and _safe_float(r.get("close")) is not None]
            rows.sort(key=lambda r: _safe_str(r.get("date")) or "")

            if len(rows) < 2:
                patch = _clean_patch({
                    "symbol": symbol,
                    "symbol_normalized": sym_norm,
                    "provider": PROVIDER_NAME,
                    "data_source": PROVIDER_NAME,
                    "history_points": len(rows),
                    "history_source": PROVIDER_NAME,
                    "history_last_utc": _safe_str(rows[-1].get("date")) if rows else None,
                    "last_updated_utc": _utc_iso(),
                    "last_updated_riyadh": _riyadh_iso(),
                })
                await self._hist_cache.set(cache_key, patch)
                return patch, None

            closes: List[float] = []
            highs: List[float] = []
            lows: List[float] = []
            opens: List[float] = []
            volumes: List[float] = []
            last_hist_dt: Optional[str] = None

            for row in rows:
                c = _safe_float(row.get("close"))
                if c is not None:
                    closes.append(c)
                    last_hist_dt = _safe_str(row.get("date")) or last_hist_dt
                h = _safe_float(row.get("high"))
                if h is not None:
                    highs.append(h)
                l = _safe_float(row.get("low"))
                if l is not None:
                    lows.append(l)
                o = _safe_float(row.get("open"))
                if o is not None:
                    opens.append(o)
                v = _safe_float(row.get("volume"))
                if v is not None:
                    volumes.append(v)

            n = len(closes)
            latest_row = rows[-1]
            prev_row = rows[-2] if len(rows) >= 2 else {}

            last = closes[-1]
            prev = _safe_float(prev_row.get("close"))
            latest_open = _safe_float(latest_row.get("open"))
            latest_high = _safe_float(latest_row.get("high"))
            latest_low = _safe_float(latest_row.get("low"))
            latest_vol = _safe_float(latest_row.get("volume"))

            history_price_change = (last - prev) if (last is not None and prev is not None and prev != 0) else None
            history_percent_change = ((last / prev) - 1.0) if (last is not None and prev is not None and prev != 0) else None

            win_52 = min(self.config.history_window_52w, len(closes))
            close_win = closes[-win_52:]
            high_win = highs[-win_52:] if highs else []
            low_win = lows[-win_52:] if lows else []

            high_52 = max(high_win) if high_win else max(close_win)
            low_52 = min(low_win) if low_win else min(close_win)
            pos_52_frac = None
            if high_52 != low_52:
                pos_52_frac = (last - low_52) / (high_52 - low_52)

            avg_vol_10 = sum(volumes[-10:]) / 10.0 if len(volumes) >= 10 else (sum(volumes) / len(volumes) if volumes else None)
            avg_vol_30 = sum(volumes[-30:]) / 30.0 if len(volumes) >= 30 else (sum(volumes) / len(volumes) if volumes else None)

            returns = _daily_returns(closes)
            returns_1y = returns[-min(len(returns), 252):] if returns else []
            returns_90 = returns[-min(len(returns), 90):] if returns else []
            returns_30 = returns[-min(len(returns), 30):] if returns else []

            vol_30 = _annualized_vol(returns_30)
            vol_90 = _annualized_vol(returns_90)
            vol_1y = _annualized_vol(returns_1y)
            mdd_1y = _max_drawdown(closes[-min(len(closes), 252):])
            var95 = _var_95_1d(returns_1y)
            sharpe = _sharpe_1y(returns_1y, self.config.risk_free_rate)
            rsi14 = _rsi_14(closes[-100:])

            def _period_return(days: int) -> Optional[float]:
                if n <= days:
                    return None
                base = closes[-1 - days]
                if not base:
                    return None
                return (last / base) - 1.0

            patch = _clean_patch({
                "symbol": symbol,
                "symbol_normalized": sym_norm,
                "provider": PROVIDER_NAME,
                "data_source": PROVIDER_NAME,
                "current_price": last,
                "price": last,
                "previous_close": prev,
                "prev_close": prev,
                "day_open": latest_open,
                "open": latest_open,
                "day_high": latest_high,
                "day_low": latest_low,
                "volume": latest_vol,
                "price_change": history_price_change,
                "change": history_price_change,
                "percent_change": history_percent_change,
                "change_pct": history_percent_change,
                "week_52_low": low_52,
                "week_52_high": high_52,
                "week_52_position_pct": pos_52_frac,
                "avg_vol_10d": avg_vol_10,
                "avg_volume_10d": avg_vol_10,
                "avg_vol_30d": avg_vol_30,
                "avg_volume_30d": avg_vol_30,
                "rsi_14": rsi14,
                "volatility_30d": vol_30,
                "volatility_90d": vol_90,
                "volatility_365d": vol_1y,
                "max_drawdown_1y": mdd_1y,
                "var_95_1d": var95,
                "sharpe_1y": sharpe,
                "returns_1w": _period_return(5),
                "returns_1m": _period_return(21),
                "returns_3m": _period_return(63),
                "returns_6m": _period_return(126),
                "returns_12m": _period_return(252),
                "history_points": n,
                "history_source": PROVIDER_NAME,
                "history_last_utc": last_hist_dt,
                "last_updated_utc": _utc_iso(),
                "last_updated_riyadh": _riyadh_iso(),
            })
            patch["52w_high"] = patch.get("week_52_high")
            patch["52w_low"] = patch.get("week_52_low")
            patch["position_52w_pct"] = patch.get("week_52_position_pct")

            await self._hist_cache.set(cache_key, patch)
            return patch, None

        return await self._single_flight.do(cache_key, _fetch)

    # -- Enriched patch (merged quote + fundamentals + history) --------

    async def fetch_enriched_quote_patch(self, symbol: str) -> Dict[str, Any]:
        """Fetch the enriched quote patch combining all available sources."""
        now_utc = _utc_iso()
        now_riy = _riyadh_iso()
        sym_norm = normalize_eodhd_symbol(symbol, self.config)

        if not sym_norm:
            return _clean_patch({
                "symbol": symbol,
                "symbol_normalized": "",
                "provider": PROVIDER_NAME,
                "data_source": PROVIDER_NAME,
                "data_quality": "MISSING",
                "error": "invalid_symbol",
                "error_detail": "normalize_eodhd_symbol returned empty",
                "last_updated_utc": now_utc,
                "last_updated_riyadh": now_riy,
            })

        if not self.config.api_key:
            return _clean_patch({
                "symbol": symbol,
                "symbol_normalized": sym_norm,
                "provider": PROVIDER_NAME,
                "data_source": PROVIDER_NAME,
                "data_quality": "MISSING",
                "error": "missing_api_key",
                "error_detail": "EODHD_API_KEY (or EODHD_API_TOKEN/EODHD_KEY) is not set",
                "last_updated_utc": now_utc,
                "last_updated_riyadh": now_riy,
            })

        if not self.config.should_allow_ksa(sym_norm):
            return _clean_patch({
                "symbol": symbol,
                "symbol_normalized": sym_norm,
                "provider": PROVIDER_NAME,
                "data_source": PROVIDER_NAME,
                "data_quality": "BLOCKED",
                "error": "ksa_blocked",
                "error_detail": "KSA_DISALLOW_EODHD=true (override with ALLOW_EODHD_KSA=1)",
                "last_updated_utc": now_utc,
                "last_updated_riyadh": now_riy,
            })

        tasks: List[asyncio.Task] = [asyncio.create_task(self._fetch_quote(symbol))]
        if self.config.enable_fundamentals:
            tasks.append(asyncio.create_task(self._fetch_fundamentals(symbol)))
        if self.config.enable_history:
            tasks.append(asyncio.create_task(self._fetch_history_stats(symbol)))

        results = await asyncio.gather(*tasks, return_exceptions=True)

        merged: Dict[str, Any] = {
            "symbol": symbol,
            "symbol_normalized": sym_norm,
            "provider": PROVIDER_NAME,
            "data_source": PROVIDER_NAME,
            "data_sources": [PROVIDER_NAME],
            "provider_role": "primary_global",
            "last_updated_utc": now_utc,
            "last_updated_riyadh": now_riy,
        }
        errors: List[str] = []

        for result in results:
            if isinstance(result, Exception):
                errors.append(f"exception:{result.__class__.__name__}")
                continue
            try:
                patch, err = result
                if err:
                    errors.append(str(err))
                if isinstance(patch, dict) and patch:
                    for k, v in patch.items():
                        if v is None:
                            continue
                        if k not in merged or merged.get(k) in (None, "", [], {}):
                            merged[k] = v
            except (ValueError, TypeError):
                errors.append("bad_task_result")

        # Cross-fill aliases (None-aware, so zero values survive)
        if merged.get("current_price") is None and merged.get("price") is not None:
            merged["current_price"] = merged.get("price")
        if merged.get("price") is None and merged.get("current_price") is not None:
            merged["price"] = merged.get("current_price")
        if merged.get("previous_close") is None and merged.get("prev_close") is not None:
            merged["previous_close"] = merged.get("prev_close")
        if merged.get("prev_close") is None and merged.get("previous_close") is not None:
            merged["prev_close"] = merged.get("previous_close")
        if merged.get("open") is None and merged.get("day_open") is not None:
            merged["open"] = merged.get("day_open")
        if merged.get("day_open") is None and merged.get("open") is not None:
            merged["day_open"] = merged.get("open")

        if (merged.get("market_cap") is None
                and merged.get("shares_outstanding") is not None
                and merged.get("current_price") is not None):
            try:
                merged["market_cap"] = float(merged["shares_outstanding"]) * float(merged["current_price"])
            except Exception:
                pass

        if (merged.get("percent_change") is None
                and merged.get("current_price") is not None
                and merged.get("previous_close") not in (None, 0)):
            try:
                merged["percent_change"] = (float(merged["current_price"]) / float(merged["previous_close"])) - 1.0
            except Exception:
                pass

        if (merged.get("price_change") is None
                and merged.get("current_price") is not None
                and merged.get("previous_close") is not None):
            try:
                merged["price_change"] = float(merged["current_price"]) - float(merged["previous_close"])
            except Exception:
                pass

        if merged.get("week_52_position_pct") is None:
            pos = _safe_div(
                (_safe_float(merged.get("current_price")) or 0.0) - (_safe_float(merged.get("week_52_low")) or 0.0),
                (_safe_float(merged.get("week_52_high")) or 0.0) - (_safe_float(merged.get("week_52_low")) or 0.0),
            )
            if pos is not None:
                merged["week_52_position_pct"] = pos

        if not merged.get("exchange"):
            suffix_exchange = _exchange_from_suffix(sym_norm)
            if suffix_exchange:
                merged["exchange"] = suffix_exchange
        if not merged.get("country"):
            suffix_country = _country_from_suffix(sym_norm)
            if suffix_country:
                merged["country"] = suffix_country

        # v6.0.0: use explicit None-check rather than `a or b` so zero values
        # (e.g. a genuine beta of 0.0 for a market-neutral ETF) survive.
        merged["change"] = merged.get("price_change")
        merged["change_pct"] = merged.get("percent_change")
        merged["52w_high"] = merged.get("week_52_high")
        merged["52w_low"] = merged.get("week_52_low")
        merged["beta_5y"] = _first_non_none(merged.get("beta_5y"), merged.get("beta"))
        merged["avg_volume_10d"] = _first_non_none(merged.get("avg_volume_10d"), merged.get("avg_vol_10d"))
        merged["avg_volume_30d"] = _first_non_none(merged.get("avg_volume_30d"), merged.get("avg_vol_30d"))
        merged["fcf_ttm"] = _first_non_none(merged.get("fcf_ttm"), merged.get("free_cash_flow_ttm"))
        merged["d_e_ratio"] = _first_non_none(merged.get("d_e_ratio"), merged.get("debt_to_equity"))

        if merged.get("current_price") is None:
            merged["data_quality"] = "MISSING"
            merged["error"] = "fetch_failed"
            merged["error_detail"] = ",".join(sorted(set(errors))) if errors else "no_data"
        else:
            merged["data_quality"] = "OK"
            if errors:
                merged["warning"] = "partial_sources"
                merged["info"] = {"warnings": sorted(set(errors))[:6]}

        return _clean_patch(merged)

    async def get_enriched_quotes_batch(
        self,
        symbols: List[str],
        mode: str = "",
    ) -> Dict[str, Any]:
        """
        Batch fetch enriched quote patches for multiple symbols.

        v6.0.0: the per-call semaphore inside `_request_json` already gates
        concurrency to `config.max_concurrency`. v5.0.0 also created a batch-
        level semaphore on top of that -- redundant double-gating.
        """
        if not symbols:
            return {}

        async def _fetch_one(sym: str) -> Tuple[str, Dict[str, Any]]:
            try:
                patch = await self.fetch_enriched_quote_patch(sym)
                return sym, patch
            except Exception as exc:
                return sym, _clean_patch({
                    "symbol": sym,
                    "provider": PROVIDER_NAME,
                    "data_quality": "MISSING",
                    "error": f"batch_error:{exc.__class__.__name__}",
                    "last_updated_utc": _utc_iso(),
                })

        results = await asyncio.gather(
            *(_fetch_one(sym) for sym in symbols),
            return_exceptions=True,
        )

        output: Dict[str, Any] = {}
        for result in results:
            if isinstance(result, Exception):
                continue
            sym, patch = result
            key = patch.get("symbol_normalized") or patch.get("symbol") or sym
            if isinstance(patch, dict):
                output[key] = patch
        return output

    async def fetch_price_history(
        self,
        symbol: str,
        days: Optional[int] = None,
    ) -> List[Dict[str, Any]]:
        """Fetch raw price history rows for a symbol."""
        sym_norm = normalize_eodhd_symbol(symbol, self.config)
        if not sym_norm:
            return []

        if not self.config.should_allow_ksa(sym_norm):
            return []

        hist_days = days or self.config.history_days
        from_date = (datetime.now(timezone.utc) - timedelta(days=hist_days)).strftime("%Y-%m-%d")
        data, err = await self._request_json(f"eod/{sym_norm}", params={"from": from_date})
        if err or not isinstance(data, list):
            return []
        return [row for row in data if isinstance(row, dict)]

    async def close(self) -> None:
        """Close the underlying httpx client."""
        try:
            await self._client.aclose()
        except Exception as exc:
            logger.debug("eodhd client close failed: %s", exc)


# ---------------------------------------------------------------------------
# Singleton Client (lazy lock)
# ---------------------------------------------------------------------------

_CLIENT_INSTANCE: Optional[EODHDClient] = None
_CLIENT_LOCK: Optional[asyncio.Lock] = None


def _get_client_lock() -> asyncio.Lock:
    global _CLIENT_LOCK
    if _CLIENT_LOCK is None:
        _CLIENT_LOCK = asyncio.Lock()
    return _CLIENT_LOCK


async def get_client() -> EODHDClient:
    """Get the singleton EODHDClient instance."""
    global _CLIENT_INSTANCE
    if _CLIENT_INSTANCE is not None:
        return _CLIENT_INSTANCE
    async with _get_client_lock():
        if _CLIENT_INSTANCE is None:
            _CLIENT_INSTANCE = EODHDClient()
    return _CLIENT_INSTANCE


async def close_client() -> None:
    """Close and reset the singleton client."""
    global _CLIENT_INSTANCE
    if _CLIENT_INSTANCE is not None:
        await _CLIENT_INSTANCE.close()
        _CLIENT_INSTANCE = None


# ---------------------------------------------------------------------------
# Engine-Facing Functions
# ---------------------------------------------------------------------------

async def fetch_enriched_quote_patch(symbol: str, *args: Any, **kwargs: Any) -> Dict[str, Any]:
    """Fetch enriched quote patch for a symbol. Extra args/kwargs ignored."""
    if args or kwargs:
        logger.debug("fetch_enriched_quote_patch(%s): ignoring args=%r kwargs=%r", symbol, args, kwargs)
    client = await get_client()
    return await client.fetch_enriched_quote_patch(symbol)


async def fetch_enriched_quotes_batch(
    symbols: List[str],
    mode: str = "",
    **kwargs: Any,
) -> Dict[str, Any]:
    """Batch fetch enriched quote patches."""
    if kwargs:
        logger.debug("fetch_enriched_quotes_batch: ignoring kwargs=%r", kwargs)
    client = await get_client()
    return await client.get_enriched_quotes_batch(symbols, mode=mode)


async def fetch_enriched_quote(symbol: str, *args: Any, **kwargs: Any) -> Dict[str, Any]:
    """Alias for fetch_enriched_quote_patch."""
    return await fetch_enriched_quote_patch(symbol, *args, **kwargs)


async def enriched_quote(symbol: str, *args: Any, **kwargs: Any) -> Dict[str, Any]:
    """Alias for fetch_enriched_quote_patch."""
    return await fetch_enriched_quote_patch(symbol, *args, **kwargs)


async def quote(symbol: str, *args: Any, **kwargs: Any) -> Dict[str, Any]:
    """Fetch a quote; raises EODHDFetchError on failure."""
    if args or kwargs:
        logger.debug("quote(%s): ignoring args=%r kwargs=%r", symbol, args, kwargs)
    client = await get_client()
    patch, err = await client._fetch_quote(symbol)
    if err:
        raise EODHDFetchError(f"Failed to fetch quote for {symbol}: {err}")
    return patch


async def get_quote(symbol: str, *args: Any, **kwargs: Any) -> Dict[str, Any]:
    """Alias for quote."""
    return await quote(symbol, *args, **kwargs)


async def fetch_quote(symbol: str, *args: Any, **kwargs: Any) -> Dict[str, Any]:
    """Alias for quote."""
    return await quote(symbol, *args, **kwargs)


async def fetch_quote_patch(symbol: str, *args: Any, **kwargs: Any) -> Dict[str, Any]:
    """Fetch a quote patch; returns an error-marked patch on failure (never raises)."""
    if args or kwargs:
        logger.debug("fetch_quote_patch(%s): ignoring args=%r kwargs=%r", symbol, args, kwargs)
    client = await get_client()
    patch, err = await client._fetch_quote(symbol)
    if err or not patch:
        return _clean_patch({
            "symbol": symbol,
            "provider": PROVIDER_NAME,
            "error": err or "fetch_quote_failed",
            "data_quality": "MISSING",
            "last_updated_utc": _utc_iso(),
            "last_updated_riyadh": _riyadh_iso(),
        })
    return patch


async def fetch_history(
    symbol: str,
    days: Optional[int] = None,
    *args: Any,
    **kwargs: Any,
) -> List[Dict[str, Any]]:
    """Fetch history rows. Extra args/kwargs ignored."""
    if args or kwargs:
        logger.debug("fetch_history(%s): ignoring args=%r kwargs=%r", symbol, args, kwargs)
    client = await get_client()
    return await client.fetch_price_history(symbol, days=days)


async def fetch_price_history(
    symbol: str,
    days: Optional[int] = None,
    *args: Any,
    **kwargs: Any,
) -> List[Dict[str, Any]]:
    """
    Alias for fetch_history.

    v6.0.0 bug fix: drops `*args, **kwargs` from the forward call. v5.0.0 had
    `await fetch_history(symbol, days=days, *args, **kwargs)` which raises
    `TypeError: got multiple values for 'days'` whenever any extra positional
    arg is passed, because the unpacked *args binds positionally BEFORE the
    kwarg at the call site.
    """
    if args or kwargs:
        logger.debug("fetch_price_history(%s): ignoring args=%r kwargs=%r", symbol, args, kwargs)
    return await fetch_history(symbol, days=days)


async def fetch_ohlc_history(
    symbol: str,
    days: Optional[int] = None,
    *args: Any,
    **kwargs: Any,
) -> List[Dict[str, Any]]:
    """Alias for fetch_history. v6.0.0 bug fix: see fetch_price_history."""
    if args or kwargs:
        logger.debug("fetch_ohlc_history(%s): ignoring args=%r kwargs=%r", symbol, args, kwargs)
    return await fetch_history(symbol, days=days)


async def fetch_quotes(symbols: List[str], *args: Any, **kwargs: Any) -> List[Dict[str, Any]]:
    """Fetch quotes for multiple symbols; returns list of patches."""
    if args or kwargs:
        logger.debug("fetch_quotes: ignoring args=%r kwargs=%r", args, kwargs)
    client = await get_client()
    result = await client.get_enriched_quotes_batch(symbols)
    return list(result.values())


# ---------------------------------------------------------------------------
# Module Exports
# ---------------------------------------------------------------------------

__all__ = [
    "PROVIDER_NAME",
    "PROVIDER_VERSION",
    "VERSION",
    "PROVIDER_BATCH_SUPPORTED",
    "EODHDClient",
    "EODHDConfig",
    "get_client",
    "close_client",
    "normalize_eodhd_symbol",
    "fetch_enriched_quote_patch",
    "fetch_enriched_quotes_batch",
    "fetch_enriched_quote",
    "enriched_quote",
    "quote",
    "get_quote",
    "fetch_quote",
    "fetch_quote_patch",
    "fetch_history",
    "fetch_price_history",
    "fetch_ohlc_history",
    "fetch_quotes",
    # Exceptions
    "EODHDProviderError",
    "EODHDFetchError",
    "EODHDAuthError",
    "EODHDNotFoundError",
    "EODHDRateLimitError",
]
