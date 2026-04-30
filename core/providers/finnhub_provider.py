#!/usr/bin/env python3
"""
core/providers/finnhub_provider.py
================================================================================
Finnhub Provider -- v6.1.0 (SCHEMA-ALIGNED / BATCH-CAPABLE / FULLY TYPED)
================================================================================

Purpose
-------
Provides financial market data from Finnhub API:
  - Real-time quotes for global stocks
  - Company profiles and fundamentals
  - Historical OHLCV data with technical indicators
  - Financial metrics and ratios

v6.1.0 Changes (from v6.0.0)
----------------------------
KSA override flag (parity with EODHD / TADAWUL providers):
  - `should_block_ksa()` now honors the FINNHUB_ALLOW_KSA env override.
    v6.0.0 was a hard block: any .SR ticker returned `data_quality:
    BLOCKED` with no escape hatch. If/when Finnhub adds Tadawul
    coverage (or a regional reseller starts proxying through Finnhub),
    operators can flip `FINNHUB_ALLOW_KSA=true` without editing source.
  - Symmetry env var `FINNHUB_KSA_DISALLOW` (default True) lets
    operators express the same intent positively. Default behavior is
    unchanged: KSA tickers are still blocked unless an override is set.
  - The new flags are read at config-load time and stored on
    FinnhubConfig as `ksa_disallowed_by_default` and `ksa_override_allow`,
    so should_block_ksa() doesn't re-read os.environ on every call.
  - `is_available_for_ksa` property added for engine-side introspection
    (lets the provider router log a clear "finnhub_ksa_allowed" tag in
    debug output rather than guessing from the block return).

Bug-compat: the public API (functions, fetch methods, dataclass field
order, __all__, env-var names, return shapes, exception classes) is
unchanged. Two new optional fields appended to FinnhubConfig with safe
defaults that preserve v6.0.0 behavior exactly.

----------------------------
v6.0.0 Changes (from v5.0.0)
----------------------------
Bug fixes:
  - Fundamentals percent-conversion bug: `_percentish_to_fraction` only
    divides by 100 when |value| > 1.5, so a stock with a 0.8% dividend
    yield had `dividend_yield=0.8` (i.e., 80%). v5.0.0 applied this to
    five Finnhub fields that are documented as percent-points:
    dividendYieldIndicatedAnnual, roeTTM, roaTTM, payoutRatioTTM, netMargin.
    v6.0.0 uses the existing `_percent_points_to_fraction` (unconditional
    /100) for these known-percent fields. `_percentish_to_fraction` is
    retained for any ambiguous callers.
  - All asyncio primitives (Lock, Semaphore) are now lazily initialized
    inside async methods rather than in __init__ or at module top level.
    Affected: TokenBucket._lock, TTLCache._lock, SingleFlight._lock,
    CircuitBreaker._lock, FinnhubClient._semaphore, module _CLIENT_LOCK.
  - Retry-After header parsing now accepts decimal seconds (HTTP spec
    allows both integer and delta-seconds). v5.0.0 used `.isdigit()`
    which rejects "1.5".

Cleanup:
  - Removed unused imports: `cast`, `field`.
  - TTLCache eviction uses OrderedDict.popitem(last=False) for explicit FIFO.
  - Engine-facing functions log a debug line when extra args/kwargs are
    dropped (parity with other v6 providers).
  - Added VERSION alias export (parity with argaam/eodhd).

Preserved for backward compatibility:
  - Every name in __all__.
  - All environment variable names and defaults.
  - Patch shape (every field + alias).
  - Exception classes (FinnhubAuthError/NotFound/RateLimit/CircuitOpen/Fetch) --
    unused but kept because external code may `except` on them.
  - Two-tier semaphore layering in batch: max_concurrency gates HTTP
    requests, batch_concurrency gates symbols-in-flight. These are
    intentionally different limits serving different layers.
  - RSI/volatility implementations unchanged to preserve numeric output.
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
from typing import Any, Callable, Dict, List, Optional, Tuple, Union

import httpx

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------

logger = logging.getLogger(__name__)
logger.addHandler(logging.NullHandler())

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

PROVIDER_NAME = "finnhub"
PROVIDER_VERSION = "6.1.0"
VERSION = PROVIDER_VERSION
PROVIDER_BATCH_SUPPORTED = True

DEFAULT_BASE_URL = "https://finnhub.io/api/v1"
DEFAULT_USER_AGENT = "TFB-Finnhub/6.1.0"

_TRUTHY = {"1", "true", "yes", "y", "on", "t", "enabled", "enable"}
_FALSY = {"0", "false", "no", "n", "off", "f", "disabled", "disable"}

_KSA_CODE_RE = re.compile(r"^\d{3,6}(\.SR)?$", re.IGNORECASE)
_SPECIAL_SYMBOL_RE = re.compile(r"(\^|=|/|:)", re.IGNORECASE)

# Matches an integer or decimal Retry-After header (delta-seconds per HTTP spec)
_RETRY_AFTER_NUMERIC_RE = re.compile(r"^\d+(?:\.\d+)?$")


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
class FinnhubConfig:
    """Immutable configuration for Finnhub provider."""

    api_key: str
    base_url: str = DEFAULT_BASE_URL
    timeout_sec: float = 15.0
    retry_attempts: int = 4
    retry_base_delay: float = 0.6
    max_concurrency: int = 25
    rate_limit_rps: float = 20.0
    rate_limit_burst: float = 40.0
    circuit_breaker_threshold: int = 6
    circuit_breaker_cooldown_sec: float = 45.0
    quote_ttl_sec: float = 10.0
    profile_ttl_sec: float = 21600.0
    metric_ttl_sec: float = 21600.0
    history_ttl_sec: float = 1800.0
    history_days: int = 500
    history_window_52w: int = 252
    batch_concurrency: int = 8
    enabled: bool = True
    enable_profile: bool = True
    enable_metric: bool = True
    enable_history: bool = True
    fundamentals_enabled: bool = True
    user_agent: str = DEFAULT_USER_AGENT
    # v6.1.0: KSA override flags (parity with EODHD provider).
    # Defaults preserve v6.0.0 behavior exactly: KSA tickers blocked.
    # Set FINNHUB_ALLOW_KSA=true to override.
    ksa_disallowed_by_default: bool = True
    ksa_override_allow: bool = False

    @classmethod
    def from_env(cls) -> "FinnhubConfig":
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

        api_key = ""
        for key in ("FINNHUB_API_KEY", "FINNHUB_API_TOKEN", "FINNHUB_TOKEN"):
            val = _env_str(key)
            if val:
                api_key = val
                break

        fundamentals_enabled = _env_bool("FUNDAMENTALS_ENABLED", True)

        # v6.1.0: KSA override flags. Default behavior unchanged (block).
        # FINNHUB_KSA_DISALLOW=true (default) means: by default, KSA is blocked.
        # FINNHUB_ALLOW_KSA=true (default false) overrides the block when set.
        # Aliased ALLOW_FINNHUB_KSA also accepted for parity with EODHD's
        # ALLOW_EODHD_KSA / EODHD_ALLOW_KSA dual-name pattern.
        ksa_disallowed_by_default = _env_bool("FINNHUB_KSA_DISALLOW", True)
        ksa_override_allow = (
            _env_bool("FINNHUB_ALLOW_KSA", False)
            or _env_bool("ALLOW_FINNHUB_KSA", False)
        )

        return cls(
            api_key=api_key,
            base_url=_env_str("FINNHUB_BASE_URL", DEFAULT_BASE_URL).rstrip("/"),
            timeout_sec=_env_float("FINNHUB_TIMEOUT_SEC", 15.0, lo=3.0, hi=120.0),
            retry_attempts=_env_int("FINNHUB_RETRY_ATTEMPTS", 4, lo=0, hi=10),
            retry_base_delay=_env_float("FINNHUB_RETRY_BASE_DELAY", 0.6, lo=0.0, hi=10.0),
            max_concurrency=_env_int("FINNHUB_MAX_CONCURRENCY", 25, lo=1, hi=100),
            rate_limit_rps=_env_float("FINNHUB_RATE_LIMIT_RPS", 20.0, lo=0.0, hi=200.0),
            rate_limit_burst=_env_float("FINNHUB_RATE_LIMIT_BURST", 40.0, lo=1.0, hi=500.0),
            circuit_breaker_threshold=_env_int("FINNHUB_CB_THRESHOLD", 6, lo=1, hi=100),
            circuit_breaker_cooldown_sec=_env_float("FINNHUB_CB_COOLDOWN_SEC", 45.0, lo=1.0, hi=600.0),
            quote_ttl_sec=_env_float("FINNHUB_QUOTE_TTL_SEC", 10.0, lo=1.0, hi=600.0),
            profile_ttl_sec=_env_float("FINNHUB_PROFILE_TTL_SEC", 21600.0, lo=60.0, hi=86400.0),
            metric_ttl_sec=_env_float("FINNHUB_METRIC_TTL_SEC", 21600.0, lo=60.0, hi=86400.0),
            history_ttl_sec=_env_float("FINNHUB_HISTORY_TTL_SEC", 1800.0, lo=60.0, hi=86400.0),
            history_days=_env_int("FINNHUB_HISTORY_DAYS", 500, lo=60, hi=3000),
            history_window_52w=_env_int("FINNHUB_WINDOW_52W", 252, lo=60, hi=800),
            batch_concurrency=_env_int("FINNHUB_BATCH_CONCURRENCY", 8, lo=1, hi=32),
            enabled=_env_bool("FINNHUB_ENABLED", True),
            enable_profile=fundamentals_enabled and _env_bool("FINNHUB_ENABLE_PROFILE", True),
            enable_metric=fundamentals_enabled and _env_bool("FINNHUB_ENABLE_METRIC", True),
            enable_history=_env_bool("FINNHUB_ENABLE_HISTORY", True),
            fundamentals_enabled=fundamentals_enabled,
            user_agent=_env_str("FINNHUB_USER_AGENT", DEFAULT_USER_AGENT),
            ksa_disallowed_by_default=ksa_disallowed_by_default,
            ksa_override_allow=ksa_override_allow,
        )

    @property
    def is_available(self) -> bool:
        """Return True if the provider is enabled and has an API key."""
        return self.enabled and bool(self.api_key)

    @property
    def is_available_for_ksa(self) -> bool:
        """
        Return True if the provider is currently configured to attempt KSA
        symbols. Used by the engine-side provider router for diagnostic
        logging (so the chain shows finnhub_ksa_allowed=True/False rather
        than just 'finnhub returned BLOCKED' for every Saudi ticker).
        """
        if not self.is_available:
            return False
        # Override always wins
        if self.ksa_override_allow:
            return True
        # Default: blocked-by-default => not available for KSA
        return not self.ksa_disallowed_by_default

    def should_block_ksa(self, symbol: str) -> bool:
        """
        Return True if this is a KSA symbol AND no override is set.

        v6.1.0: previously this was a hard block (any .SR returned True).
        Now operators can flip FINNHUB_ALLOW_KSA=true (or
        ALLOW_FINNHUB_KSA=true, or set FINNHUB_KSA_DISALLOW=false) to
        let Finnhub attempt KSA tickers. Default behavior is unchanged
        because Finnhub does not currently cover Tadawul.
        """
        if not looks_like_ksa(symbol):
            return False
        # Symbol IS KSA. Block unless overridden.
        if self.ksa_override_allow:
            return False
        if not self.ksa_disallowed_by_default:
            return False
        return True

    def should_block_special(self, symbol: str) -> bool:
        """Return True if this is a special symbol (index/forex/crypto)."""
        return is_blocked_special(symbol)


# ---------------------------------------------------------------------------
# Custom Exceptions (kept for defensive back-compat even though unraised)
# ---------------------------------------------------------------------------

class FinnhubProviderError(Exception):
    """Base exception for Finnhub provider errors."""


class FinnhubAuthError(FinnhubProviderError):
    """Raised when API key is missing or invalid."""


class FinnhubNotFoundError(FinnhubProviderError):
    """Raised when symbol is not found."""


class FinnhubRateLimitError(FinnhubProviderError):
    """Raised when rate limit is exceeded."""


class FinnhubCircuitOpenError(FinnhubProviderError):
    """Raised when circuit breaker is open."""


class FinnhubFetchError(FinnhubProviderError):
    """Raised when a fetch fails."""


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
    tz = timezone(timedelta(hours=3))
    d = dt or datetime.now(timezone.utc)
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


def _percent_points_to_fraction(value: Any) -> Optional[float]:
    """
    Convert a KNOWN-PERCENT value to a fraction by dividing by 100.

    Use this for fields whose API docs confirm percent units -- Finnhub's
    quote `dp`, and metrics like roeTTM, roaTTM, netMargin, payoutRatioTTM,
    dividendYieldIndicatedAnnual, which are all documented percent-points.
    """
    f = _safe_float(value)
    return f / 100.0 if f is not None else None


def _percentish_to_fraction(value: Any) -> Optional[float]:
    """
    Convert a percent-ISH value using the abs>1.5 heuristic.

    LEGACY: divides by 100 only when |value| > 1.5. Kept for ambiguous
    external callers that may import it. The provider itself no longer
    uses this for fundamentals fields -- see `_percent_points_to_fraction`.
    """
    f = _safe_float(value)
    if f is None:
        return None
    return f / 100.0 if abs(f) > 1.5 else f


def _clean_patch(patch: Dict[str, Any]) -> Dict[str, Any]:
    """Remove None and whitespace-only-string values from a patch dict."""
    return {
        k: v for k, v in patch.items()
        if v is not None and not (isinstance(v, str) and not v.strip())
    }


def looks_like_ksa(symbol: str) -> bool:
    """Return True if the symbol looks like a KSA (Saudi) symbol."""
    s = (symbol or "").strip().upper()
    if not s:
        return False

    # Delegate to shared normalizer if available
    try:
        from core.symbols.normalize import looks_like_ksa as _lk  # type: ignore

        if callable(_lk):
            return bool(_lk(s))
    except ImportError:
        pass

    if s.startswith("TADAWUL:"):
        return True
    if s.endswith(".SR"):
        return True
    if _KSA_CODE_RE.match(s):
        return True
    return False


def is_blocked_special(symbol: str) -> bool:
    """Return True if the symbol is an index/forex/crypto (or empty)."""
    s = (symbol or "").strip().upper()
    if not s:
        return True
    return bool(_SPECIAL_SYMBOL_RE.search(s))


def normalize_finnhub_symbol(symbol: str) -> str:
    """Normalize symbol to Finnhub format (strips `.US` suffix)."""
    s = (symbol or "").strip().upper()
    if not s:
        return ""

    try:
        from core.symbols.normalize import to_finnhub_symbol as _to  # type: ignore

        if callable(_to):
            result = _to(s)
            if isinstance(result, str) and result.strip():
                s = result.strip().upper()
    except ImportError:
        pass

    if s.endswith(".US"):
        s = s[:-3]

    return s


def symbol_variants(symbol: str) -> List[str]:
    """
    Generate symbol variants for fallback lookups.

    Examples:
        "BRK-B" -> ["BRK-B", "BRK.B"]    (hyphen→dot)
        "VOD.L" -> ["VOD.L", "VOD"]      (strip suffix)
        "AAPL"  -> ["AAPL"]
    """
    s = normalize_finnhub_symbol(symbol)
    if not s:
        return []

    variants = [s]

    if "-" in s and "." not in s:
        variants.append(s.replace("-", "."))

    if "." in s:
        base = s.split(".", 1)[0]
        if base and base not in variants:
            variants.append(base)

    seen: set = set()
    result: List[str] = []
    for v in variants:
        if v and v not in seen:
            seen.add(v)
            result.append(v)
    return result


# ---------------------------------------------------------------------------
# Async Primitives: TokenBucket, TTLCache, SingleFlight, CircuitBreaker
# ---------------------------------------------------------------------------

class TokenBucket:
    """Token bucket rate limiter."""

    def __init__(self, rate_per_sec: float, burst: float):
        self.rate = max(0.0, rate_per_sec)
        self.capacity = max(1.0, burst)
        self.tokens = self.capacity
        self.last = time.monotonic()
        self._lock: Optional[asyncio.Lock] = None  # lazy

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


@dataclass
class _CacheItem:
    """Cache item with expiration timestamp."""
    expires_at: float
    value: Any


class TTLCache:
    """Async-safe TTL cache with FIFO eviction on overflow."""

    def __init__(self, max_size: int, ttl_sec: float):
        self.max_size = max(128, max_size)
        self.ttl_sec = max(1.0, ttl_sec)
        self._cache: "OrderedDict[str, _CacheItem]" = OrderedDict()
        self._lock: Optional[asyncio.Lock] = None  # lazy

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
                self._cache.popitem(last=False)
            self._cache[key] = _CacheItem(expires_at=now + ttl, value=value)


class SingleFlight:
    """Deduplicate concurrent requests for the same key."""

    def __init__(self) -> None:
        self._lock: Optional[asyncio.Lock] = None  # lazy
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


class CircuitBreaker:
    """Circuit breaker to short-circuit repeated failures."""

    def __init__(self, threshold: int, cooldown_sec: float):
        self.threshold = max(1, threshold)
        self.cooldown_sec = max(1.0, cooldown_sec)
        self.failures = 0
        self.open_until = 0.0
        self._lock: Optional[asyncio.Lock] = None  # lazy

    def _get_lock(self) -> asyncio.Lock:
        if self._lock is None:
            self._lock = asyncio.Lock()
        return self._lock

    async def allow(self) -> bool:
        """Return True if requests are allowed (breaker closed or cooled down)."""
        async with self._get_lock():
            return time.monotonic() >= self.open_until

    async def on_success(self) -> None:
        """Record a success; resets failure count."""
        async with self._get_lock():
            self.failures = 0
            self.open_until = 0.0

    async def on_failure(self) -> None:
        """Record a failure; may open the circuit."""
        async with self._get_lock():
            self.failures += 1
            if self.failures >= self.threshold:
                self.open_until = time.monotonic() + self.cooldown_sec


# ---------------------------------------------------------------------------
# Technical Indicators (Pure Python, no numpy)
# ---------------------------------------------------------------------------

def _sma(values: List[float], window: int) -> Optional[float]:
    """Simple moving average over the last `window` values; None if insufficient."""
    if window <= 0 or len(values) < window:
        return None
    return sum(values[-window:]) / float(window)


def _returns(values: List[float], days: int) -> Optional[float]:
    """Return over N days as a fraction; None if insufficient data."""
    if len(values) <= days:
        return None
    base = values[-1 - days]
    if not base:
        return None
    return (values[-1] / base) - 1.0


def _volatility_30d_annualized_fraction(closes: List[float]) -> Optional[float]:
    """Annualized volatility from the last 30 days of daily returns."""
    if len(closes) < 31:
        return None

    returns: List[float] = []
    for i in range(len(closes) - 30, len(closes)):
        if i <= 0:
            continue
        prev = closes[i - 1]
        curr = closes[i]
        if prev:
            returns.append((curr / prev) - 1.0)

    if len(returns) < 5:
        return None

    mean = sum(returns) / len(returns)
    variance = sum((r - mean) ** 2 for r in returns) / max(1, len(returns) - 1)
    return math.sqrt(variance) * math.sqrt(252.0)


def _rsi_14(closes: List[float]) -> Optional[float]:
    """RSI(14) using simple period averaging; 0-100 or None."""
    if len(closes) < 15:
        return None

    period = 14
    gains = 0.0
    losses = 0.0

    for i in range(len(closes) - period, len(closes)):
        if i <= 0:
            continue
        delta = closes[i] - closes[i - 1]
        if delta > 0:
            gains += delta
        else:
            losses += -delta

    avg_gain = gains / period
    avg_loss = losses / period

    if avg_loss == 0:
        return 100.0

    rs = avg_gain / avg_loss
    return 100.0 - (100.0 / (1.0 + rs))


# ---------------------------------------------------------------------------
# Finnhub HTTP Client
# ---------------------------------------------------------------------------

class FinnhubClient:
    """Async client for Finnhub API."""

    def __init__(self, config: Optional[FinnhubConfig] = None):
        self.config = config or FinnhubConfig.from_env()
        self._semaphore: Optional[asyncio.Semaphore] = None  # lazy
        self._bucket = TokenBucket(
            rate_per_sec=self.config.rate_limit_rps,
            burst=self.config.rate_limit_burst,
        )
        self._circuit_breaker = CircuitBreaker(
            threshold=self.config.circuit_breaker_threshold,
            cooldown_sec=self.config.circuit_breaker_cooldown_sec,
        )
        self._single_flight = SingleFlight()

        self._quote_cache = TTLCache(max_size=8000, ttl_sec=self.config.quote_ttl_sec)
        self._profile_cache = TTLCache(max_size=4000, ttl_sec=self.config.profile_ttl_sec)
        self._metric_cache = TTLCache(max_size=4000, ttl_sec=self.config.metric_ttl_sec)
        self._history_cache = TTLCache(max_size=2500, ttl_sec=self.config.history_ttl_sec)

        self._client = httpx.AsyncClient(
            timeout=httpx.Timeout(self.config.timeout_sec),
            headers={"User-Agent": self.config.user_agent},
            limits=httpx.Limits(max_keepalive_connections=30, max_connections=60),
            follow_redirects=True,
            http2=True,
        )

    def _get_semaphore(self) -> asyncio.Semaphore:
        if self._semaphore is None:
            self._semaphore = asyncio.Semaphore(self.config.max_concurrency)
        return self._semaphore

    @staticmethod
    def _parse_retry_after(value: Optional[str]) -> Optional[float]:
        """
        Parse a Retry-After header value.

        HTTP spec allows integer or decimal delta-seconds. v5.0.0 used
        `.isdigit()` which rejects "1.5".
        """
        if not value:
            return None
        cleaned = value.strip()
        if _RETRY_AFTER_NUMERIC_RE.match(cleaned):
            try:
                return float(cleaned)
            except (ValueError, TypeError):
                return None
        return None

    async def _request_json(
        self,
        endpoint: str,
        params: Optional[Dict[str, Any]] = None,
    ) -> Tuple[Optional[Any], Optional[str]]:
        """Make JSON request to Finnhub API."""
        if not self.config.api_key:
            return None, "FINNHUB_API_KEY missing"

        if not await self._circuit_breaker.allow():
            return None, "circuit_open"

        url = f"{self.config.base_url}/{endpoint.lstrip('/')}"
        query_params = dict(params or {})
        query_params["token"] = self.config.api_key
        last_error: Optional[str] = None

        async with self._get_semaphore():
            for attempt in range(max(1, self.config.retry_attempts + 1)):
                await self._bucket.wait(1.0)

                try:
                    response = await self._client.get(url, params=query_params)
                    status_code = response.status_code

                    if status_code == 429:
                        retry_after = self._parse_retry_after(response.headers.get("Retry-After"))
                        wait = retry_after if retry_after is not None else min(15.0, 1.0 + attempt)
                        last_error = "HTTP 429"
                        await self._circuit_breaker.on_failure()
                        await asyncio.sleep(wait)
                        continue

                    if 500 <= status_code < 600:
                        last_error = f"HTTP {status_code}"
                        await self._circuit_breaker.on_failure()
                        base_delay = self.config.retry_base_delay * (2 ** max(0, attempt - 1))
                        await asyncio.sleep(min(12.0, random.uniform(0, base_delay + 0.25)))
                        continue

                    if status_code == 404:
                        await self._circuit_breaker.on_failure()
                        return None, "HTTP 404 not_found"

                    if status_code >= 400:
                        await self._circuit_breaker.on_failure()
                        return None, f"HTTP {status_code}"

                    try:
                        data = _json_loads(response.content)
                    except Exception:
                        await self._circuit_breaker.on_failure()
                        return None, "invalid_json"

                    await self._circuit_breaker.on_success()
                    return data, None

                except httpx.RequestError as exc:
                    last_error = f"network_error:{exc.__class__.__name__}"
                    await self._circuit_breaker.on_failure()
                    base_delay = self.config.retry_base_delay * (2 ** max(0, attempt - 1))
                    await asyncio.sleep(min(12.0, random.uniform(0, base_delay + 0.25)))
                except Exception as exc:
                    last_error = f"unexpected_error:{exc.__class__.__name__}"
                    await self._circuit_breaker.on_failure()
                    break

        return None, last_error or "request_failed"

    # -- Quote ---------------------------------------------------------

    async def _fetch_quote(self, symbol: str) -> Tuple[Dict[str, Any], Optional[str]]:
        """Fetch real-time quote for a symbol."""
        sym = normalize_finnhub_symbol(symbol)
        if not sym:
            return {}, "invalid_symbol"

        cache_key = f"q:{sym}"
        cached = await self._quote_cache.get(cache_key)
        if cached:
            return cached, None

        async def _fetch() -> Tuple[Dict[str, Any], Optional[str]]:
            data, err = await self._request_json("quote", params={"symbol": sym})
            if err or not isinstance(data, dict):
                return {}, err or "bad_payload"

            current = _safe_float(data.get("c"))
            previous = _safe_float(data.get("pc"))
            price_change = _safe_float(data.get("d"))

            # Prefer computing percent_change from prices (always correct fraction);
            # fall back to Finnhub's dp which is percent-points.
            if current is not None and previous is not None and previous != 0:
                percent_change = (current / previous) - 1.0
            else:
                percent_change = _percent_points_to_fraction(data.get("dp"))

            if price_change is None and current is not None and previous is not None:
                price_change = current - previous

            patch = _clean_patch({
                "provider": PROVIDER_NAME,
                "data_source": PROVIDER_NAME,
                "current_price": current,
                "previous_close": previous,
                "price_change": price_change,
                "percent_change": percent_change,
                "open_price": _safe_float(data.get("o")),
                "day_high": _safe_float(data.get("h")),
                "day_low": _safe_float(data.get("l")),
                "volume": _safe_float(data.get("v")),
                # Backward-compat aliases
                "change": price_change,
                "change_pct": percent_change,
                "day_open": _safe_float(data.get("o")),
                "price": current,
                "last_updated_utc": _utc_iso(),
                "last_updated_riyadh": _riyadh_iso(),
            })
            await self._quote_cache.set(cache_key, patch)
            return patch, None

        return await self._single_flight.do(cache_key, _fetch)

    # -- Profile -------------------------------------------------------

    async def _fetch_profile(self, symbol: str) -> Tuple[Dict[str, Any], Optional[str]]:
        """Fetch company profile for a symbol."""
        sym = normalize_finnhub_symbol(symbol)
        if not sym:
            return {}, "invalid_symbol"

        cache_key = f"p:{sym}"
        cached = await self._profile_cache.get(cache_key)
        if cached:
            return cached, None

        async def _fetch() -> Tuple[Dict[str, Any], Optional[str]]:
            data, err = await self._request_json("stock/profile2", params={"symbol": sym})
            if err or not isinstance(data, dict):
                return {}, err or "bad_payload"
            if not data:
                return {}, "empty_profile"

            patch = _clean_patch({
                "provider": PROVIDER_NAME,
                "data_source": PROVIDER_NAME,
                "name": _safe_str(data.get("name")),
                "currency": _safe_str(data.get("currency")),
                "exchange": _safe_str(data.get("exchange")),
                "country": _safe_str(data.get("country")),
                "sector": _safe_str(data.get("finnhubIndustry")),
                "listing_date": _safe_str(data.get("ipo")),
                "market_cap": _safe_float(data.get("marketCapitalization")),
                "float_shares": _safe_float(data.get("shareOutstanding")),
                "shares_outstanding": _safe_float(data.get("shareOutstanding")),
                "last_updated_utc": _utc_iso(),
                "last_updated_riyadh": _riyadh_iso(),
            })
            await self._profile_cache.set(cache_key, patch)
            return patch, None

        return await self._single_flight.do(cache_key, _fetch)

    # -- Metrics -------------------------------------------------------

    async def _fetch_metrics(self, symbol: str) -> Tuple[Dict[str, Any], Optional[str]]:
        """Fetch financial metrics for a symbol."""
        sym = normalize_finnhub_symbol(symbol)
        if not sym:
            return {}, "invalid_symbol"

        cache_key = f"m:{sym}"
        cached = await self._metric_cache.get(cache_key)
        if cached:
            return cached, None

        async def _fetch() -> Tuple[Dict[str, Any], Optional[str]]:
            data, err = await self._request_json("stock/metric", params={"symbol": sym, "metric": "all"})
            if err or not isinstance(data, dict):
                return {}, err or "bad_payload"

            metric = data.get("metric") if isinstance(data.get("metric"), dict) else {}
            if not isinstance(metric, dict) or not metric:
                return {}, "empty_metric"

            # v6.0.0 bug fix: Finnhub /stock/metric returns these five fields as
            # percent-points (e.g., 2.47 for 2.47% dividend yield, 15.5 for
            # 15.5% ROE). v5.0.0 used `_percentish_to_fraction` whose abs>1.5
            # heuristic left small values (<1.5%) unconverted -- a 0.8%
            # dividend yield became a ratio of 0.8 (i.e., 80%). Unconditional
            # /100 is correct for these known-percent fields.
            dividend_yield = _percent_points_to_fraction(metric.get("dividendYieldIndicatedAnnual"))
            payout_ratio = _percent_points_to_fraction(metric.get("payoutRatioTTM"))
            roe = _percent_points_to_fraction(metric.get("roeTTM"))
            roa = _percent_points_to_fraction(metric.get("roaTTM"))
            profit_margin = _percent_points_to_fraction(metric.get("netMargin"))

            high_52 = _safe_float(metric.get("52WeekHigh"))
            low_52 = _safe_float(metric.get("52WeekLow"))
            pb_value = _safe_float(metric.get("pbAnnual")) or _safe_float(metric.get("pbQuarterly"))

            patch = _clean_patch({
                "provider": PROVIDER_NAME,
                "data_source": PROVIDER_NAME,
                # Schema-aligned keys
                "eps_ttm": _safe_float(metric.get("epsTTM")),
                "pe_ttm": _safe_float(metric.get("peTTM")),
                "pb_ratio": pb_value,
                "ps_ratio": _safe_float(metric.get("psTTM")),
                "beta_5y": _safe_float(metric.get("beta")),
                "week_52_high": high_52,
                "week_52_low": low_52,
                # Percent fields (fractions)
                "dividend_yield": dividend_yield,
                "payout_ratio": payout_ratio,
                "roe": roe,
                "roa": roa,
                "profit_margin": profit_margin,
                # Backward-compat aliases
                "pb": pb_value,
                "ps": _safe_float(metric.get("psTTM")),
                "beta": _safe_float(metric.get("beta")),
                "high_52w": high_52,
                "low_52w": low_52,
                "net_margin": profit_margin,
                "last_updated_utc": _utc_iso(),
                "last_updated_riyadh": _riyadh_iso(),
            })
            await self._metric_cache.set(cache_key, patch)
            return patch, None

        return await self._single_flight.do(cache_key, _fetch)

    # -- History + derived stats --------------------------------------

    async def _fetch_history_stats(self, symbol: str) -> Tuple[Dict[str, Any], Optional[str]]:
        """Fetch historical OHLCV data and derive technical statistics."""
        sym = normalize_finnhub_symbol(symbol)
        if not sym:
            return {}, "invalid_symbol"

        days = self.config.history_days
        cache_key = f"h:{sym}:{days}"
        cached = await self._history_cache.get(cache_key)
        if cached:
            return cached, None

        async def _fetch() -> Tuple[Dict[str, Any], Optional[str]]:
            to_ts = int(time.time())
            from_ts = to_ts - int(days * 86400)

            data, err = await self._request_json(
                "stock/candle",
                params={"symbol": sym, "resolution": "D", "from": from_ts, "to": to_ts},
            )
            if err or not isinstance(data, dict):
                return {}, err or "bad_payload"
            if data.get("s") != "ok":
                return {}, f"candle_status:{_safe_str(data.get('s')) or 'bad'}"

            closes_raw = data.get("c") or []
            volumes_raw = data.get("v") or []
            times_raw = data.get("t") or []

            closes = [x for x in (_safe_float(v) for v in closes_raw) if x is not None]
            volumes = [x for x in (_safe_float(v) for v in volumes_raw) if x is not None]

            if len(closes) < 25:
                return {"history_points": len(closes)}, None

            window = min(self.config.history_window_52w, len(closes))
            last = closes[-1]
            high_52 = max(closes[-window:])
            low_52 = min(closes[-window:])
            pos_52_frac = ((last - low_52) / (high_52 - low_52)) if high_52 != low_52 else None

            avg_vol_30 = (sum(volumes[-30:]) / 30.0) if len(volumes) >= 30 else None
            vol_30 = _volatility_30d_annualized_fraction(closes)

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
                    pass

            patch = _clean_patch({
                "provider": PROVIDER_NAME,
                "data_source": PROVIDER_NAME,
                "week_52_high": high_52,
                "week_52_low": low_52,
                "week_52_position_pct": pos_52_frac,
                "avg_volume_30d": avg_vol_30,
                "volatility_30d": vol_30,
                "rsi_14": rsi14,
                "ma20": ma20,
                "ma50": ma50,
                "ma200": ma200,
                "returns_1w": r1w,
                "returns_1m": r1m,
                "returns_3m": r3m,
                "returns_6m": r6m,
                "returns_12m": r12m,
                # Backward-compat aliases
                "high_52w": high_52,
                "low_52w": low_52,
                "position_52w_pct": pos_52_frac,
                "avg_vol_30d": avg_vol_30,
                "history_points": len(closes),
                "history_last_utc": _utc_iso(last_dt) if last_dt else None,
                "last_updated_utc": _utc_iso(),
                "last_updated_riyadh": _riyadh_iso(),
            })
            await self._history_cache.set(cache_key, patch)
            return patch, None

        return await self._single_flight.do(cache_key, _fetch)

    # -- Enriched patch (merged quote + profile + metrics + history) --

    async def fetch_enriched_quote_patch(self, symbol: str) -> Dict[str, Any]:
        """Fetch the enriched quote patch combining all available sources."""
        now_utc = _utc_iso()
        now_riy = _riyadh_iso()
        raw_symbol = (symbol or "").strip()

        def _error_patch(reason: str, quality: str = "MISSING") -> Dict[str, Any]:
            return _clean_patch({
                "provider": PROVIDER_NAME,
                "data_source": PROVIDER_NAME,
                "data_quality": quality,
                "error": reason,
                "last_updated_utc": now_utc,
                "last_updated_riyadh": now_riy,
            })

        if not raw_symbol:
            return _error_patch("empty_symbol")
        if not self.config.is_available:
            return _error_patch("provider_disabled_or_missing_key", "DISABLED")
        if self.config.should_block_ksa(raw_symbol):
            return _error_patch("ksa_blocked", "BLOCKED")
        if self.config.should_block_special(raw_symbol):
            return _error_patch("special_symbol_blocked", "BLOCKED")

        variants = symbol_variants(raw_symbol) or [normalize_finnhub_symbol(raw_symbol)]
        errors: List[str] = []

        merged: Dict[str, Any] = {
            "provider": PROVIDER_NAME,
            "data_source": PROVIDER_NAME,
            "last_updated_utc": now_utc,
            "last_updated_riyadh": now_riy,
        }

        # Fetch quote with variant fallback
        used_variant = variants[0]
        quote_ok = False

        for variant in variants[:3]:
            patch, err = await self._fetch_quote(variant)
            if err:
                errors.append(f"quote:{variant}:{err}")
                continue
            if patch and patch.get("current_price") is not None:
                merged.update(patch)
                used_variant = normalize_finnhub_symbol(variant)
                quote_ok = True
                break

        # Fetch profile / metrics / history in parallel
        tasks: List[asyncio.Task] = []
        if self.config.enable_profile:
            tasks.append(asyncio.create_task(self._fetch_profile(used_variant)))
        if self.config.enable_metric:
            tasks.append(asyncio.create_task(self._fetch_metrics(used_variant)))
        if self.config.enable_history:
            tasks.append(asyncio.create_task(self._fetch_history_stats(used_variant)))

        if tasks:
            results = await asyncio.gather(*tasks, return_exceptions=True)
            for result in results:
                if isinstance(result, Exception):
                    errors.append(f"exception:{result.__class__.__name__}")
                    continue
                patch, err = result
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
            if errors:
                merged["warning"] = "partial_sources"
                merged["info"] = {"warnings": errors[:5]}

        return _clean_patch(merged)

    async def get_enriched_quotes_batch(
        self,
        symbols: List[str],
        mode: str = "",
    ) -> Dict[str, Dict[str, Any]]:
        """
        Batch fetch enriched quote patches for multiple symbols.

        Note: batch_concurrency gates symbols-in-flight; max_concurrency
        (inside _request_json) gates HTTP requests. These are intentionally
        different layers -- NOT double-gating the same thing.
        """
        if not symbols:
            return {}

        semaphore = asyncio.Semaphore(max(1, self.config.batch_concurrency))

        async def _fetch_one(sym: str) -> Tuple[str, Dict[str, Any]]:
            async with semaphore:
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

        output: Dict[str, Dict[str, Any]] = {}
        for result in results:
            if isinstance(result, Exception):
                continue
            sym, patch = result
            key = patch.get("symbol") or sym
            output[key] = patch
        return output

    async def close(self) -> None:
        """Close the underlying httpx client."""
        try:
            await self._client.aclose()
        except Exception as exc:
            logger.debug("finnhub client close failed: %s", exc)


# ---------------------------------------------------------------------------
# Singleton Client (lazy lock)
# ---------------------------------------------------------------------------

_CLIENT_INSTANCE: Optional[FinnhubClient] = None
_CLIENT_LOCK: Optional[asyncio.Lock] = None


def _get_client_lock() -> asyncio.Lock:
    global _CLIENT_LOCK
    if _CLIENT_LOCK is None:
        _CLIENT_LOCK = asyncio.Lock()
    return _CLIENT_LOCK


async def get_client() -> FinnhubClient:
    """Get the singleton FinnhubClient instance."""
    global _CLIENT_INSTANCE
    if _CLIENT_INSTANCE is not None:
        return _CLIENT_INSTANCE
    async with _get_client_lock():
        if _CLIENT_INSTANCE is None:
            _CLIENT_INSTANCE = FinnhubClient()
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
        logger.debug(
            "fetch_enriched_quote_patch(%s): ignoring args=%r kwargs=%r",
            symbol, args, kwargs,
        )
    client = await get_client()
    return await client.fetch_enriched_quote_patch(symbol)


async def fetch_enriched_quotes_batch(
    symbols: List[str],
    mode: str = "",
    **kwargs: Any,
) -> Dict[str, Dict[str, Any]]:
    """Batch fetch enriched quote patches for multiple symbols."""
    if kwargs:
        logger.debug("fetch_enriched_quotes_batch: ignoring kwargs=%r", kwargs)
    client = await get_client()
    return await client.get_enriched_quotes_batch(symbols, mode=mode)


async def fetch_quote_patch(symbol: str, *args: Any, **kwargs: Any) -> Dict[str, Any]:
    """Fetch quote patch (quote only, no enrichment)."""
    if args or kwargs:
        logger.debug("fetch_quote_patch(%s): ignoring args=%r kwargs=%r", symbol, args, kwargs)
    client = await get_client()
    sym = normalize_finnhub_symbol(symbol)
    patch, err = await client._fetch_quote(sym)
    if err:
        return _clean_patch({
            "provider": PROVIDER_NAME,
            "data_source": PROVIDER_NAME,
            "error": err,
            "data_quality": "MISSING",
        })
    patch.setdefault("data_quality", "OK")
    return patch


async def fetch_patch(symbol: str, *args: Any, **kwargs: Any) -> Dict[str, Any]:
    """Alias for fetch_enriched_quote_patch."""
    return await fetch_enriched_quote_patch(symbol, *args, **kwargs)


# ---------------------------------------------------------------------------
# Module Exports
# ---------------------------------------------------------------------------

__all__ = [
    # Metadata
    "PROVIDER_NAME",
    "PROVIDER_VERSION",
    "VERSION",
    "PROVIDER_BATCH_SUPPORTED",
    # Classes
    "FinnhubConfig",
    "FinnhubClient",
    # Singletons
    "get_client",
    "close_client",
    # Engine-facing
    "fetch_enriched_quote_patch",
    "fetch_enriched_quotes_batch",
    "fetch_quote_patch",
    "fetch_patch",
    # Symbol helpers
    "normalize_finnhub_symbol",
    "symbol_variants",
    "looks_like_ksa",
    "is_blocked_special",
    # Exceptions (kept for back-compat even though unraised internally)
    "FinnhubProviderError",
    "FinnhubAuthError",
    "FinnhubNotFoundError",
    "FinnhubRateLimitError",
    "FinnhubCircuitOpenError",
    "FinnhubFetchError",
]
