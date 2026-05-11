#!/usr/bin/env python3
# core/providers/yahoo_chart_provider.py
"""
================================================================================
Yahoo Chart Provider (Global + KSA History) -- v8.2.0
================================================================================

Purpose
-------
Provides financial market data from Yahoo Finance:
  - Real-time quotes for global stocks, ETFs, indices, forex, commodities
  - Historical OHLCV data with technical indicators
  - Fundamental data (light) and financial metrics
  - KSA symbol support (.SR suffix)
  - Risk statistics (volatility, drawdown, VaR, Sharpe, RSI)
  - Price forecasts using log-linear regression

v8.2.0 Changes (from v8.1.0)
----------------------------
Resilience (HTTP 403 / 429 from Yahoo):
  - USER-AGENT ROTATION. Production audit showed ~100% of provider calls
    returning `HTTP 403` from Yahoo Finance's edge layer, cascading into
    blank Volatility 30D/90D, Max Drawdown 1Y, VaR 95% (1D), and Sharpe
    (1Y) columns across every row of Global_Markets and Market_Leaders
    (Class 2 from the cross-provider field audit). Yahoo aggressively
    blocks requests carrying the default python-urllib User-Agent. v8.2.0
    constructs a `requests.Session` carrying one of 8 modern browser UA
    strings (Chrome / Firefox / Safari on Win / Mac / Linux) and passes it
    to `yf.Ticker(symbol, session=session)`. The UA is re-rotated on
    every retry. Gracefully degrades to bare `yf.Ticker(symbol)` when
    `requests` is unavailable, when the running yfinance doesn't accept
    the `session` kwarg, or when `YAHOO_USER_AGENT_ROTATION=0`.
  - EXPONENTIAL BACKOFF WITH JITTER. v8.1.0 had no retry path at all --
    `_fetch_ticker_sync` caught the first exception and returned empty
    `({}, {}, [])`. v8.2.0 retries up to `YAHOO_RETRY_ATTEMPTS` (default:
    4) times with base = min(8.0, 0.5 * 2**attempt) and 25% jitter.
    Detects 403/429/rate-limit substrings in the error string and
    lengthens the cooldown to discourage tight-loop rate hammering.
  - Retry count is configurable via `YAHOO_RETRY_ATTEMPTS`.

Data accuracy (currency-mismatch guard):
  - 52W BOUNDS VALIDATION. Yahoo's `info` block occasionally returns
    `fiftyTwoWeekHigh` / `fiftyTwoWeekLow` in a foreign listing's
    currency while `current_price` is correctly in the primary
    listing's currency. Observed in the audit:
      BP.US   -- 52W high 609.40 (GBX from LSE) vs price 43.34 USD
      RIO.US  -- 52W high 7,834.00 (GBX) vs price 105.38 USD
      BRK-B.US -- 52W high 782,014.25 (BRK-A USD) vs price 475.94 USD
      CHT.US  -- 52W high 138.00 (TWD) vs price 43.81 USD
      ASR.US  -- 52W high 690.99 (MXN) vs price 308.99 USD
      ZTO.US  -- 52W high 205.60 (HKD) vs price 25.05 USD
      PRU.L   -- 52W high 119.76 (USD ADR) vs price 1,135.00 GBX
    Same problem can hit the history-derived fallback when yfinance
    serves the foreign listing's bar series. v8.2.0 validates both
    sources via the |candidate / current_price| ratio (default suspect
    thresholds: >= 8.0 or <= 0.125), drops the offender, and falls back
    to the in-range source when one is available. Risk metrics
    (volatility, Sharpe, VaR, drawdown, RSI) are unit-invariant because
    they're computed on log returns / ratios, so they remain correct
    even when the underlying series is in the wrong currency -- only
    the absolute-price fields (52W bounds, forecast price) suffer.
  - FORECAST MAGNITUDE SANITY. `forecast_price_12m` is now compared
    against `current_price`; if the ratio is outside [0.1, 10] the
    forecast is treated as mis-scaled and dropped (with the matching
    `forecast_magnitude_suspect` warning). `expected_roi_12m` is also
    dropped in that case because it would otherwise propagate the
    bad-price ratio downstream. Risk metrics computed off the same
    history series are NOT dropped because they're unit-invariant.

Observability:
  - STRUCTURED `warnings: List[str]` IN OUTPUT. Same channel as
    yahoo_fundamentals_provider v6.1.0. Examples emitted:
      `chart_history_empty`, `info_empty`, `currency_missing_from_provider`,
      `week_52_high_unit_mismatch_dropped`,
      `week_52_high_used_history_fallback`,
      `week_52_high_history_unit_mismatch_dropped`,
      `forecast_magnitude_suspect`,
      `current_price_outside_52w_range`.
    `enriched_quote.py` v4.3.0 already coerces these into the
    "; "-joined string the sheet's Warnings column expects, so no
    downstream changes are required.
  - `last_error_class` is recorded on full-fail returns alongside the
    error message, so the circuit breaker's status-code inference can
    be more accurate on follow-up calls.

Configuration:
  - YahooConfig extended with `user_agent_rotation`, `price_sanity_guard`,
    `retry_attempts`, `price_ratio_high`, `price_ratio_low`. All read
    from new env vars with v8.1.0-compatible defaults if unset.

Cleanup:
  - Re-imported `random` (needed for UA rotation and backoff jitter).
    The v8.0.0 cleanup pass had removed it because it was unused;
    v8.2.0 brings it back with explicit purpose.
  - `requests` added as optional dependency (transitive yfinance dep,
    so it's a free addition for any working install).

Deferred (still):
  - Direct Yahoo Chart API fallback for futures (=F symbols) -- now
    deferred to v8.3.0. yfinance fails on Render's egress IP for
    GC=F / SI=F / CL=F because Yahoo rate-limits those requests, and
    fixing it requires adding an httpx-based fallback to
    query1.finance.yahoo.com/v8/finance/chart/{SYMBOL} with proper
    headers + JSON parsing. v8.2.0 surfaces this case via the
    `chart_history_empty` warning so it can be tracked while the
    futures fallback is built out.

v8.1.0 Changes (PRESERVED verbatim)
-----------------------------------
  - `_BARE_FX_RE`, `_KNOWN_CURRENCIES`, `_is_likely_fx_pair` for bare
    6-letter FX pair detection (AUDCAD -> AUDCAD=X).
  - `normalize_symbol` appends `=X` for bare currency pairs.

v8.0.0 Changes (PRESERVED verbatim)
-----------------------------------
  - `_fetch_ticker_sync` accepts period/interval from config (not the
    v7.0.0 fossil that always returned "2y").
  - Real `_safe_history_metadata(ticker)` extraction.
  - `fetch_history` uses the shared executor (no thread leak).
  - `_PROVIDER_LOCK` lazy-initialized.
  - Dead-code removal (`_trace`, `_TraceContext`, `_json_dumps`).

Preserved for backward compatibility:
  - Every name in __all__.
  - All environment variable names and defaults from v8.0.0 / v8.1.0.
  - Patch shape (all price, fundamentals, risk, forecast fields).
  - Prometheus metrics integration.
  - Optional numpy/pandas/yfinance/orjson/prometheus/opentelemetry support.
  - Provider singleton + engine-facing functions.
  - KSA `.SR` symbol handling and Arabic-digit parsing.

New env variables (v8.2.0):
  - YAHOO_USER_AGENT_ROTATION (default: 1)   enable session-based UA rotation
  - YAHOO_PRICE_SANITY_GUARD  (default: 1)   enable 52W bounds validation
  - YAHOO_RETRY_ATTEMPTS      (default: 4)   per-fetch retry cap
  - YAHOO_PRICE_RATIO_HIGH    (default: 8.0) suspect-ratio upper threshold
  - YAHOO_PRICE_RATIO_LOW     (default: 0.125) suspect-ratio lower threshold
================================================================================
"""

from __future__ import annotations

import asyncio
import concurrent.futures
import json
import logging
import math
import os
import random
import re
import statistics
import time
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Any, Awaitable, Callable, Dict, List, Optional, Tuple

# =============================================================================
# Optional Dependencies (prod-safe)
# =============================================================================

try:
    import numpy as np  # type: ignore
    _HAS_NUMPY = True
except ImportError:
    np = None  # type: ignore[assignment]
    _HAS_NUMPY = False

try:
    import pandas as pd  # type: ignore
    _HAS_PANDAS = True
except ImportError:
    pd = None  # type: ignore[assignment]
    _HAS_PANDAS = False

try:
    import yfinance as yf  # type: ignore
    _HAS_YFINANCE = True
except ImportError:
    yf = None  # type: ignore[assignment]
    _HAS_YFINANCE = False

# v8.2.0: `requests` is required for UA-rotation sessions. It's pulled in
# transitively by yfinance, so this should always be available in any
# environment where the provider can function -- but we degrade gracefully
# if it's not.
try:
    import requests  # type: ignore
    _HAS_REQUESTS = True
except ImportError:
    requests = None  # type: ignore[assignment]
    _HAS_REQUESTS = False

# Prometheus metrics (optional)
try:
    from prometheus_client import Counter, Gauge, Histogram, REGISTRY  # type: ignore
    _HAS_PROM = True
except ImportError:
    Counter = None  # type: ignore[assignment]
    Gauge = None  # type: ignore[assignment]
    Histogram = None  # type: ignore[assignment]
    REGISTRY = None  # type: ignore[assignment]
    _HAS_PROM = False

# OpenTelemetry (optional)
try:
    from opentelemetry import trace  # type: ignore
    _HAS_OTEL = True
except ImportError:
    trace = None  # type: ignore[assignment]
    _HAS_OTEL = False

# =============================================================================
# Logging
# =============================================================================

logger = logging.getLogger(__name__)
logger.addHandler(logging.NullHandler())

# =============================================================================
# Constants
# =============================================================================

PROVIDER_NAME = "yahoo_chart"
PROVIDER_VERSION = "8.2.0"
VERSION = PROVIDER_VERSION
PROVIDER_BATCH_SUPPORTED = True

_RIYADH_TZ = timezone(timedelta(hours=3))

_TRUTHY = {"1", "true", "yes", "y", "on", "t", "enable", "enabled"}
_FALSY = {"0", "false", "no", "n", "off", "f", "disable", "disabled"}

_KSA_CODE_RE = re.compile(r"^\d{3,6}$", re.IGNORECASE)
_FX_PAIR_RE = re.compile(r"^([A-Z]{6})=X$")
_ARABIC_DIGITS = str.maketrans("٠١٢٣٤٥٦٧٨٩", "0123456789")
_K_M_B_T_RE = re.compile(r"^(-?\d+(?:\.\d+)?)([KMBT])$", re.IGNORECASE)
_K_M_B_T_MULT = {"K": 1_000.0, "M": 1_000_000.0, "B": 1_000_000_000.0, "T": 1_000_000_000_000.0}

# v8.1.0: bare-6-letter FX pair detection
_BARE_FX_RE = re.compile(r"^[A-Z]{6}$")

# v8.1.0: known ISO 4217 codes for FX-pair half membership test.
_KNOWN_CURRENCIES = frozenset({
    # Majors
    "USD", "EUR", "GBP", "JPY", "CHF", "CAD", "AUD", "NZD",
    # GCC + regional
    "SAR", "AED", "QAR", "KWD", "BHD", "OMR", "EGP", "TRY", "ILS",
    # Asia
    "CNY", "HKD", "SGD", "INR", "KRW", "THB", "MYR", "IDR", "PHP", "TWD",
    # Other commonly-traded
    "ZAR", "MXN", "BRL", "RUB", "PLN", "SEK", "NOK", "DKK", "CZK", "HUF",
})


def _is_likely_fx_pair(symbol: str) -> bool:
    """
    Detect a bare 6-letter currency pair token (e.g. ``AUDCAD``,
    ``USDSAR``, ``EURJPY``). Returns True iff:

      * `symbol` is exactly six A-Z characters, AND
      * the first three (base) AND last three (quote) are both in
        ``_KNOWN_CURRENCIES``.

    Both-halves membership is what makes this safe: a 6-letter US stock
    ticker would have to coincidentally split into two known ISO codes
    AND both halves would have to match -- which doesn't happen for any
    real ticker we've seen. KSA equities use the `.SR` suffix and never
    reach this check (they hit `_KSA_CODE_RE` first). Crypto symbols
    like ``BTCUSD`` are not matched because ``BTC`` is not an ISO 4217
    currency code in our table.
    """
    if not _BARE_FX_RE.match(symbol):
        return False
    base = symbol[:3]
    quote = symbol[3:]
    return base in _KNOWN_CURRENCIES and quote in _KNOWN_CURRENCIES


DEFAULT_TIMEOUT_SEC = 20.0
DEFAULT_QUOTE_TTL_SEC = 15.0
DEFAULT_HISTORY_TTL_SEC = 300.0
DEFAULT_MAX_CONCURRENCY = 24
DEFAULT_RATE_LIMIT_PER_SEC = 10.0
DEFAULT_RATE_LIMIT_BURST = 20
DEFAULT_CB_FAIL_THRESHOLD = 6
DEFAULT_CB_COOLDOWN_SEC = 30.0
DEFAULT_CB_SUCCESS_THRESHOLD = 3
DEFAULT_THREADPOOL_WORKERS = 6
DEFAULT_HISTORY_PERIOD = "2y"
DEFAULT_HISTORY_INTERVAL = "1d"

# v8.2.0: new defaults
DEFAULT_USER_AGENT_ROTATION = True
DEFAULT_PRICE_SANITY_GUARD = True
DEFAULT_RETRY_ATTEMPTS = 4
DEFAULT_PRICE_RATIO_HIGH = 8.0
DEFAULT_PRICE_RATIO_LOW = 0.125

# v8.2.0: rotated User-Agent pool (modern browsers, ~Q1 2026). Same set as
# yahoo_fundamentals_provider v6.1.0 -- consistency across providers
# makes server-side rate-limit identification predictable.
_USER_AGENTS: Tuple[str, ...] = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 "
    "(KHTML, like Gecko) Version/17.2 Safari/605.1.15",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:121.0) Gecko/20100101 "
    "Firefox/121.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36 Edg/120.0.2210.144",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_2_1) AppleWebKit/605.1.15 "
    "(KHTML, like Gecko) Version/17.2.1 Safari/605.1.15",
    "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:121.0) Gecko/20100101 "
    "Firefox/121.0",
)

# v8.2.0: substring markers used to detect rate-limit / auth errors in
# yfinance exception text (it doesn't expose status codes cleanly).
_RATE_LIMIT_MARKERS: Tuple[str, ...] = (
    "403", "429", "forbidden", "rate limit", "too many requests",
    "unauthorized", "401",
)


# =============================================================================
# Configuration
# =============================================================================

@dataclass(frozen=True)
class YahooConfig:
    """Immutable configuration for Yahoo provider."""

    enabled: bool = True
    timeout_sec: float = DEFAULT_TIMEOUT_SEC
    quote_ttl_sec: float = DEFAULT_QUOTE_TTL_SEC
    history_ttl_sec: float = DEFAULT_HISTORY_TTL_SEC
    max_concurrency: int = DEFAULT_MAX_CONCURRENCY
    rate_limit_per_sec: float = DEFAULT_RATE_LIMIT_PER_SEC
    rate_limit_burst: int = DEFAULT_RATE_LIMIT_BURST
    cb_fail_threshold: int = DEFAULT_CB_FAIL_THRESHOLD
    cb_cooldown_sec: float = DEFAULT_CB_COOLDOWN_SEC
    cb_success_threshold: int = DEFAULT_CB_SUCCESS_THRESHOLD
    threadpool_workers: int = DEFAULT_THREADPOOL_WORKERS
    history_period: str = DEFAULT_HISTORY_PERIOD
    history_interval: str = DEFAULT_HISTORY_INTERVAL
    tracing_enabled: bool = False
    metrics_enabled: bool = False
    # v8.2.0: resilience + accuracy settings
    user_agent_rotation: bool = DEFAULT_USER_AGENT_ROTATION
    price_sanity_guard: bool = DEFAULT_PRICE_SANITY_GUARD
    retry_attempts: int = DEFAULT_RETRY_ATTEMPTS
    price_ratio_high: float = DEFAULT_PRICE_RATIO_HIGH
    price_ratio_low: float = DEFAULT_PRICE_RATIO_LOW

    @classmethod
    def from_env(cls) -> "YahooConfig":
        """Load configuration from environment variables."""

        def _env_str(name: str, default: str = "") -> str:
            v = os.getenv(name)
            return str(v).strip() if v is not None and str(v).strip() else default

        def _env_bool(name: str, default: bool) -> bool:
            raw = (os.getenv(name) or "").strip().lower()
            if not raw:
                return default
            if raw in _FALSY:
                return False
            if raw in _TRUTHY:
                return True
            return default

        def _env_int(name: str, default: int) -> int:
            try:
                return int(float(str(os.getenv(name, str(default))).strip()))
            except Exception:
                return default

        def _env_float(name: str, default: float) -> float:
            try:
                return float(str(os.getenv(name, str(default))).strip())
            except Exception:
                return default

        enabled = _env_bool("YAHOO_ENABLED", True) and _HAS_YFINANCE

        # v8.2.0: clamp/validate new fields the same way the older ones are.
        ratio_high = _env_float("YAHOO_PRICE_RATIO_HIGH", DEFAULT_PRICE_RATIO_HIGH)
        if not (ratio_high > 1.0):
            ratio_high = DEFAULT_PRICE_RATIO_HIGH
        ratio_low = _env_float("YAHOO_PRICE_RATIO_LOW", DEFAULT_PRICE_RATIO_LOW)
        if not (0.0 < ratio_low < 1.0):
            ratio_low = DEFAULT_PRICE_RATIO_LOW

        return cls(
            enabled=enabled,
            timeout_sec=max(5.0, _env_float("YAHOO_TIMEOUT_SEC", DEFAULT_TIMEOUT_SEC)),
            quote_ttl_sec=max(5.0, _env_float("YAHOO_QUOTE_TTL_SEC", DEFAULT_QUOTE_TTL_SEC)),
            history_ttl_sec=max(30.0, _env_float("YAHOO_HISTORY_TTL_SEC", DEFAULT_HISTORY_TTL_SEC)),
            max_concurrency=max(2, _env_int("YAHOO_MAX_CONCURRENCY", DEFAULT_MAX_CONCURRENCY)),
            rate_limit_per_sec=max(0.0, _env_float("YAHOO_RATE_LIMIT_PER_SEC", DEFAULT_RATE_LIMIT_PER_SEC)),
            rate_limit_burst=max(1, _env_int("YAHOO_RATE_LIMIT_BURST", DEFAULT_RATE_LIMIT_BURST)),
            cb_fail_threshold=max(2, _env_int("YAHOO_CB_FAIL_THRESHOLD", DEFAULT_CB_FAIL_THRESHOLD)),
            cb_cooldown_sec=max(5.0, _env_float("YAHOO_CB_COOLDOWN_SEC", DEFAULT_CB_COOLDOWN_SEC)),
            cb_success_threshold=max(1, _env_int("YAHOO_CB_SUCCESS_THRESHOLD", DEFAULT_CB_SUCCESS_THRESHOLD)),
            threadpool_workers=max(2, _env_int("YAHOO_THREADPOOL_WORKERS", DEFAULT_THREADPOOL_WORKERS)),
            history_period=_env_str("YAHOO_HISTORY_PERIOD", DEFAULT_HISTORY_PERIOD),
            history_interval=_env_str("YAHOO_HISTORY_INTERVAL", DEFAULT_HISTORY_INTERVAL),
            tracing_enabled=_env_bool("CORE_TRACING_ENABLED", False) or _env_bool("TRACING_ENABLED", False),
            metrics_enabled=_env_bool("PROMETHEUS_ENABLED", False) or _env_bool("METRICS_ENABLED", False),
            # v8.2.0
            user_agent_rotation=_env_bool("YAHOO_USER_AGENT_ROTATION", DEFAULT_USER_AGENT_ROTATION) and _HAS_REQUESTS,
            price_sanity_guard=_env_bool("YAHOO_PRICE_SANITY_GUARD", DEFAULT_PRICE_SANITY_GUARD),
            retry_attempts=max(1, _env_int("YAHOO_RETRY_ATTEMPTS", DEFAULT_RETRY_ATTEMPTS)),
            price_ratio_high=ratio_high,
            price_ratio_low=ratio_low,
        )

    @property
    def is_available(self) -> bool:
        """Return True if the provider can serve requests."""
        return self.enabled and _HAS_YFINANCE


# =============================================================================
# Custom Exceptions
# =============================================================================

class YahooProviderError(Exception):
    """Base exception for Yahoo provider errors."""


class YahooFetchError(YahooProviderError):
    """Raised when a data fetch fails."""


# =============================================================================
# Metrics (Optional)
# =============================================================================

class _DummyMetric:
    """No-op metric used when Prometheus isn't available."""

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        pass

    def labels(self, *args: Any, **kwargs: Any) -> "_DummyMetric":
        return self

    def inc(self, *args: Any, **kwargs: Any) -> None:
        pass

    def observe(self, *args: Any, **kwargs: Any) -> None:
        pass

    def set(self, *args: Any, **kwargs: Any) -> None:
        pass


def _safe_create_metric(ctor: Any, name: str, doc: str, labelnames: List[str]) -> Any:
    """Create a Prometheus metric or return a DummyMetric if unavailable/duplicate."""
    if not _HAS_PROM or ctor is None:
        return _DummyMetric()
    try:
        return ctor(name, doc, labelnames)
    except Exception:
        return _DummyMetric()


@dataclass(slots=True)
class _Metrics:
    """Container for Prometheus metrics."""
    requests_total: Any
    request_duration: Any
    cache_hits_total: Any
    cache_misses_total: Any
    circuit_state: Any


_METRICS_INSTANCE: Optional[_Metrics] = None


def _get_metrics() -> _Metrics:
    """Get or create the module-level metrics instance."""
    global _METRICS_INSTANCE
    if _METRICS_INSTANCE is None:
        _METRICS_INSTANCE = _Metrics(
            requests_total=_safe_create_metric(
                Counter, "yahoo_requests_total", "Total Yahoo requests", ["symbol", "op", "status"],
            ),
            request_duration=_safe_create_metric(
                Histogram, "yahoo_request_duration_seconds", "Yahoo request duration", ["symbol", "op"],
            ),
            cache_hits_total=_safe_create_metric(
                Counter, "yahoo_cache_hits_total", "Yahoo cache hits", ["symbol"],
            ),
            cache_misses_total=_safe_create_metric(
                Counter, "yahoo_cache_misses_total", "Yahoo cache misses", ["symbol"],
            ),
            circuit_state=_safe_create_metric(
                Gauge, "yahoo_circuit_state", "Yahoo circuit breaker", ["state"],
            ),
        )
    return _METRICS_INSTANCE


# =============================================================================
# Pure Utility Functions
# =============================================================================

def _utc_now() -> datetime:
    """Get current UTC time."""
    return datetime.now(timezone.utc)


def _utc_iso(dt: Optional[datetime] = None) -> str:
    """Get UTC time in ISO format."""
    d = dt or _utc_now()
    if d.tzinfo is None:
        d = d.replace(tzinfo=timezone.utc)
    return d.astimezone(timezone.utc).isoformat()


def _riyadh_iso(dt: Optional[datetime] = None) -> str:
    """Get Riyadh time (UTC+3) in ISO format."""
    d = dt or datetime.now(_RIYADH_TZ)
    if d.tzinfo is None:
        d = d.replace(tzinfo=_RIYADH_TZ)
    return d.astimezone(_RIYADH_TZ).isoformat()


def _safe_str(value: Any) -> Optional[str]:
    """Safely convert to non-empty stripped string; None for empty."""
    if value is None:
        return None
    try:
        s = str(value).strip()
        return s if s else None
    except Exception:
        return None


def _safe_float(value: Any) -> Optional[float]:
    """Safely convert to float; handles Arabic digits, K/M/B/T, parens-negative."""
    if value is None:
        return None

    try:
        if isinstance(value, (int, float)) and not isinstance(value, bool):
            f = float(value)
            return None if (math.isnan(f) or math.isinf(f)) else f

        s = str(value).strip()
        if not s or s.lower() in {"-", "—", "n/a", "na", "null", "none", "nan"}:
            return None

        s = s.translate(_ARABIC_DIGITS)
        s = s.replace(",", "").replace("%", "").replace("+", "")
        s = s.replace("$", "").replace("£", "").replace("€", "")
        s = s.replace("SAR", "").replace("USD", "").replace("EUR", "").strip()

        if s.startswith("(") and s.endswith(")"):
            s = "-" + s[1:-1].strip()

        match = _K_M_B_T_RE.match(s)
        if match:
            f = float(match.group(1)) * _K_M_B_T_MULT[match.group(2).upper()]
        else:
            f = float(s)

        return None if (math.isnan(f) or math.isinf(f)) else f
    except (ValueError, TypeError):
        return None


def _as_fraction(value: Any) -> Optional[float]:
    """
    Convert percent-like value to ratio using the abs>1.5 heuristic.

    Used for Yahoo's percent-ish fields (dividendYield, margins, ROE).
    Yahoo sometimes returns ratios (0.025) and sometimes percents (2.5),
    so the heuristic is defensible here. Low-magnitude values <1.5 are
    left as-is (treated as ratios).
    """
    f = _safe_float(value)
    if f is None:
        return None
    return f / 100.0 if abs(f) > 1.5 else f


def _first_number(*values: Any) -> Optional[float]:
    """Return the first non-None numeric value."""
    for v in values:
        f = _safe_float(v)
        if f is not None:
            return f
    return None


def _first_fraction(*values: Any) -> Optional[float]:
    """Return the first non-None value coerced to a fraction."""
    for v in values:
        f = _as_fraction(v)
        if f is not None:
            return f
    return None


def _first_str(*values: Any) -> Optional[str]:
    """Return the first non-None string value."""
    for v in values:
        s = _safe_str(v)
        if s is not None:
            return s
    return None


def normalize_symbol(symbol: str) -> str:
    """
    Normalize symbol to Yahoo Finance format.

    Detection order (first match wins):
      1. Empty / whitespace -> ""
      2. KSA numeric code (3-6 digits) -> append ".SR"
      3. Bare 6-letter currency pair (v8.1.0) -> append "=X"
      4. Anything else -> upper-cased pass-through

    Examples:
        ""         -> ""
        "2222"     -> "2222.SR"
        "2222.SR"  -> "2222.SR"
        "AAPL"     -> "AAPL"
        "GC=F"     -> "GC=F"
        "^GSPC"    -> "^GSPC"
        "EURUSD=X" -> "EURUSD=X"        (already has =X, untouched)
        "AUDCAD"   -> "AUDCAD=X"        (v8.1.0)
        "USDSAR"   -> "USDSAR=X"        (v8.1.0)
        "BTCUSD"   -> "BTCUSD"          (BTC not a known currency, untouched)
    """
    s = (symbol or "").strip()
    if not s:
        return ""

    s = s.translate(_ARABIC_DIGITS).strip().upper()

    # 1. KSA numeric codes get the .SR suffix.
    if _KSA_CODE_RE.match(s):
        return f"{s}.SR"

    # 2. Bare 6-letter currency-pair tokens get =X. (v8.1.0)
    if _is_likely_fx_pair(s):
        return f"{s}=X"

    # 3. Otherwise keep the symbol as-is.
    return s


def clean_dict(d: Dict[str, Any]) -> Dict[str, Any]:
    """Remove None, empty strings, and recursively empty containers from a dict."""
    result: Dict[str, Any] = {}
    for k, v in d.items():
        if v is None:
            continue
        if isinstance(v, str) and not v.strip():
            continue
        if isinstance(v, dict):
            cleaned = clean_dict(v)
            if cleaned:
                result[k] = cleaned
        elif isinstance(v, (list, tuple)):
            cleaned_list = []
            for item in v:
                if isinstance(item, dict):
                    cleaned_item = clean_dict(item)
                    if cleaned_item:
                        cleaned_list.append(cleaned_item)
                elif item is not None:
                    cleaned_list.append(item)
            if cleaned_list:
                result[k] = cleaned_list
        else:
            result[k] = v
    return result


# =============================================================================
# v8.2.0: User-Agent Rotation + Session Factory
# =============================================================================

def _pick_random_ua() -> str:
    """Pick a User-Agent string uniformly at random from the pool."""
    return random.choice(_USER_AGENTS)


def _create_yf_session(use_rotation: bool) -> Optional[Any]:
    """
    Create a requests.Session pre-loaded with a rotated browser User-Agent.

    Returns None if:
      - requests is not installed, OR
      - `use_rotation` is False (e.g. YAHOO_USER_AGENT_ROTATION=0).

    The session is suitable to pass into yfinance:
        yf.Ticker(symbol, session=_create_yf_session(True))

    Newer yfinance versions accept the `session` kwarg and route their
    underlying HTTP calls through it; older versions ignore it (we wrap
    the constructor in try/except to handle that).
    """
    if not use_rotation or not _HAS_REQUESTS or requests is None:
        return None
    try:
        sess = requests.Session()
        sess.headers.update({
            "User-Agent": _pick_random_ua(),
            "Accept": (
                "text/html,application/xhtml+xml,application/xml;q=0.9,"
                "*/*;q=0.8"
            ),
            "Accept-Language": "en-US,en;q=0.9",
            "Accept-Encoding": "gzip, deflate, br",
            "DNT": "1",
            "Connection": "keep-alive",
            "Upgrade-Insecure-Requests": "1",
            "Sec-Fetch-Dest": "document",
            "Sec-Fetch-Mode": "navigate",
            "Sec-Fetch-Site": "none",
            "Sec-Fetch-User": "?1",
        })
        return sess
    except Exception as exc:
        logger.debug("yf session creation failed: %s", exc)
        return None


def _rotate_session_ua(session: Any) -> None:
    """Rotate the User-Agent header on an existing session (in-place)."""
    if session is None:
        return
    try:
        session.headers["User-Agent"] = _pick_random_ua()
    except Exception:
        pass


def _is_rate_limit_error(err: Optional[BaseException]) -> bool:
    """Heuristic: does the error string look like a 403 / 429 / rate-limit?"""
    if err is None:
        return False
    try:
        s = str(err).lower()
    except Exception:
        return False
    return any(m in s for m in _RATE_LIMIT_MARKERS)


def _construct_ticker(symbol: str, session: Any) -> Any:
    """
    Construct a yf.Ticker. Try with `session=` first (newer yfinance);
    fall back to positional-only construction if the kwarg is rejected.
    Returns None when yfinance itself is unavailable.
    """
    if yf is None:
        return None
    if session is None:
        return yf.Ticker(symbol)
    try:
        return yf.Ticker(symbol, session=session)
    except TypeError:
        # Older yfinance: doesn't accept session kwarg
        return yf.Ticker(symbol)
    except Exception as exc:
        logger.debug("yf.Ticker(%s, session=...) failed (%s); retrying bare",
                     symbol, exc)
        try:
            return yf.Ticker(symbol)
        except Exception:
            return None


# =============================================================================
# v8.2.0: 52W Bounds Validation (Currency-Mismatch Guard)
# =============================================================================

def _is_suspect_price_ratio(
    ref: Optional[float],
    candidate: Optional[float],
    *,
    ratio_high: float = DEFAULT_PRICE_RATIO_HIGH,
    ratio_low: float = DEFAULT_PRICE_RATIO_LOW,
) -> bool:
    """
    Return True if `candidate` is suspiciously far from `ref` (likely a
    currency/scale mismatch -- e.g. GBX value paired with a USD reference).

    Both must be positive finite floats. Caller supplies thresholds; the
    defaults [0.125, 8.0] match yahoo_fundamentals_provider v6.1.0.
    """
    if ref is None or candidate is None:
        return False
    if ref <= 0 or candidate <= 0:
        return False
    ratio = candidate / ref
    return ratio >= ratio_high or ratio <= ratio_low


def _validate_52w_bounds(
    current_price: Optional[float],
    info_52w_high: Optional[float],
    info_52w_low: Optional[float],
    hist_52w_high: Optional[float],
    hist_52w_low: Optional[float],
    *,
    enabled: bool = True,
    ratio_high: float = DEFAULT_PRICE_RATIO_HIGH,
    ratio_low: float = DEFAULT_PRICE_RATIO_LOW,
) -> Tuple[Optional[float], Optional[float], List[str]]:
    """
    Validate the 52W high/low against `current_price` for unit consistency.

    Two sources are checked: `info`-provided bounds (which Yahoo's
    fast_info / .info return) and history-derived bounds (max/min over
    closes[-252:]). Either can be in the wrong currency if yfinance
    served the foreign listing's data for an ADR / dual-listing.

    Behavior:
      1. If the info-provided bound is suspect, try the history-derived
         one as a fallback. Emit `week_52_<bound>_unit_mismatch_dropped`
         and `week_52_<bound>_used_history_fallback` accordingly.
      2. If the history-derived bound is also suspect (or the info one
         was the chosen source), drop it -> None and emit
         `week_52_<bound>_history_unit_mismatch_dropped`.
      3. If high < low after validation, swap them and emit
         `week_52_high_low_inverted`.
      4. If `current_price` is more than 5% outside the validated
         [low, high] band, emit `current_price_outside_52w_range`.

    Returns: (validated_high, validated_low, warnings)
    """
    warnings: List[str] = []

    if not enabled:
        # Pass through the existing v8.1.0 behavior: info first, then history
        # fallback, no validation.
        hi = info_52w_high if info_52w_high is not None else hist_52w_high
        lo = info_52w_low if info_52w_low is not None else hist_52w_low
        return hi, lo, warnings

    cp = _safe_float(current_price)
    info_hi = _safe_float(info_52w_high)
    info_lo = _safe_float(info_52w_low)
    hist_hi = _safe_float(hist_52w_high)
    hist_lo = _safe_float(hist_52w_low)

    # Without a usable reference price we can't validate ratios.
    if cp is None or cp <= 0:
        hi = info_hi if info_hi is not None else hist_hi
        lo = info_lo if info_lo is not None else hist_lo
        return hi, lo, warnings

    # --- High ---
    hi: Optional[float]
    if info_hi is not None and not _is_suspect_price_ratio(cp, info_hi,
                                                           ratio_high=ratio_high,
                                                           ratio_low=ratio_low):
        hi = info_hi
    elif info_hi is not None:
        warnings.append("week_52_high_unit_mismatch_dropped")
        if hist_hi is not None and not _is_suspect_price_ratio(cp, hist_hi,
                                                               ratio_high=ratio_high,
                                                               ratio_low=ratio_low):
            hi = hist_hi
            warnings.append("week_52_high_used_history_fallback")
        else:
            if hist_hi is not None:
                warnings.append("week_52_high_history_unit_mismatch_dropped")
            hi = None
    elif hist_hi is not None and not _is_suspect_price_ratio(cp, hist_hi,
                                                             ratio_high=ratio_high,
                                                             ratio_low=ratio_low):
        hi = hist_hi
    elif hist_hi is not None:
        warnings.append("week_52_high_history_unit_mismatch_dropped")
        hi = None
    else:
        hi = None

    # --- Low ---
    lo: Optional[float]
    if info_lo is not None and not _is_suspect_price_ratio(cp, info_lo,
                                                           ratio_high=ratio_high,
                                                           ratio_low=ratio_low):
        lo = info_lo
    elif info_lo is not None:
        warnings.append("week_52_low_unit_mismatch_dropped")
        if hist_lo is not None and not _is_suspect_price_ratio(cp, hist_lo,
                                                               ratio_high=ratio_high,
                                                               ratio_low=ratio_low):
            lo = hist_lo
            warnings.append("week_52_low_used_history_fallback")
        else:
            if hist_lo is not None:
                warnings.append("week_52_low_history_unit_mismatch_dropped")
            lo = None
    elif hist_lo is not None and not _is_suspect_price_ratio(cp, hist_lo,
                                                             ratio_high=ratio_high,
                                                             ratio_low=ratio_low):
        lo = hist_lo
    elif hist_lo is not None:
        warnings.append("week_52_low_history_unit_mismatch_dropped")
        lo = None
    else:
        lo = None

    # Sanity: high should be >= low
    if hi is not None and lo is not None and hi < lo:
        warnings.append("week_52_high_low_inverted")
        hi, lo = lo, hi

    # Informational: current price should sit in the validated band
    if hi is not None and cp > hi * 1.05:
        warnings.append("current_price_outside_52w_range")
    elif lo is not None and cp < lo * 0.95:
        warnings.append("current_price_outside_52w_range")

    return hi, lo, warnings


def _validate_forecast_magnitude(
    forecast_price: Optional[float],
    current_price: Optional[float],
    *,
    enabled: bool = True,
    ratio_high: float = DEFAULT_PRICE_RATIO_HIGH,
    ratio_low: float = DEFAULT_PRICE_RATIO_LOW,
) -> Tuple[Optional[float], List[str]]:
    """
    Drop the forecast price when its ratio to the current price is
    outside the suspect band -- a strong signal that the underlying
    series was in a different currency than `current_price`.

    Risk metrics (volatility, drawdown, etc.) are unit-invariant and are
    NOT dropped from the same suspicion; the forecast is the only field
    whose magnitude matters.

    Returns: (validated_forecast_price, warnings)
    """
    warnings: List[str] = []
    if not enabled:
        return forecast_price, warnings

    fp = _safe_float(forecast_price)
    cp = _safe_float(current_price)
    if fp is None or cp is None or cp <= 0:
        return fp, warnings

    if _is_suspect_price_ratio(cp, fp, ratio_high=ratio_high, ratio_low=ratio_low):
        warnings.append("forecast_magnitude_suspect")
        return None, warnings

    return fp, warnings


# =============================================================================
# Async Primitives: TokenBucket, CircuitBreaker, SingleFlight, AdvancedCache
# =============================================================================

@dataclass(slots=True)
class TokenBucket:
    """Token bucket rate limiter."""

    rate_per_sec: float
    burst: float
    tokens: float = 0.0
    last: float = 0.0
    lock: asyncio.Lock = field(default_factory=asyncio.Lock)

    async def acquire(self, n: float = 1.0) -> None:
        """Acquire `n` tokens (blocking until available)."""
        if self.rate_per_sec <= 0:
            return

        async with self.lock:
            now = time.monotonic()
            if self.last <= 0:
                self.last = now
                self.tokens = float(self.burst)

            elapsed = max(0.0, now - self.last)
            self.tokens = min(float(self.burst), self.tokens + elapsed * float(self.rate_per_sec))
            self.last = now

            if self.tokens >= n:
                self.tokens -= n
                return

            need = n - self.tokens
            wait = need / float(self.rate_per_sec)
            self.tokens = 0.0

        await asyncio.sleep(min(5.0, max(0.0, wait)))


@dataclass(slots=True)
class CircuitBreaker:
    """Circuit breaker for failure isolation."""

    fail_threshold: int
    cooldown_sec: float
    success_threshold: int
    failures: int = 0
    successes: int = 0
    opened_at: float = 0.0
    state: str = "closed"
    lock: asyncio.Lock = field(default_factory=asyncio.Lock)

    async def allow(self) -> bool:
        """Return True if a request is allowed through."""
        metrics = _get_metrics()
        async with self.lock:
            if self.state == "closed":
                metrics.circuit_state.labels(state="closed").set(0.0)
                return True

            if self.state == "open":
                if time.monotonic() - self.opened_at >= self.cooldown_sec:
                    self.state = "half_open"
                    self.successes = 0
                    metrics.circuit_state.labels(state="half_open").set(1.0)
                    return True
                metrics.circuit_state.labels(state="open").set(2.0)
                return False

            metrics.circuit_state.labels(state="half_open").set(1.0)
            return True

    async def record_success(self) -> None:
        """Record a successful request."""
        async with self.lock:
            if self.state == "half_open":
                self.successes += 1
                if self.successes >= self.success_threshold:
                    self.state = "closed"
                    self.failures = 0
                    self.successes = 0
            else:
                self.failures = max(0, self.failures - 1)

    async def record_failure(self, rate_limited: bool = False) -> None:
        """
        Record a failed request (may open the breaker).

        v8.2.0: optional `rate_limited` flag extends the cooldown for
        403/429 failures (matches v6.1.0 fundamentals semantics).
        """
        async with self.lock:
            self.failures += 1
            if self.state == "half_open":
                self.state = "open"
                self.opened_at = time.monotonic()
                return
            if self.failures >= self.fail_threshold:
                self.state = "open"
                self.opened_at = time.monotonic()
                if rate_limited:
                    # Bump cooldown by 50% (bounded at 300s) when we know
                    # we hit a rate limit -- avoids tight retry storms.
                    self.cooldown_sec = min(300.0, self.cooldown_sec * 1.5)


class SingleFlight:
    """Deduplicate concurrent requests for the same key."""

    def __init__(self) -> None:
        self._lock = asyncio.Lock()
        self._futures: Dict[str, asyncio.Future] = {}

    async def run(self, key: str, coro_fn: Callable[[], Awaitable[Any]]) -> Any:
        """Execute coroutine; concurrent callers for the same key share the result."""
        async with self._lock:
            future = self._futures.get(key)
            if future is not None:
                return await future

            loop = asyncio.get_running_loop()
            future = loop.create_future()
            self._futures[key] = future

        try:
            result = await coro_fn()
            if not future.cancelled() and not future.done():
                future.set_result(result)
            return result
        except Exception as exc:
            if not future.cancelled() and not future.done():
                future.set_exception(exc)
            raise
        finally:
            async with self._lock:
                self._futures.pop(key, None)


@dataclass(slots=True)
class AdvancedCache:
    """Async TTL cache with size limit (mass eviction on overflow)."""

    name: str
    ttl_sec: float
    max_size: int = 5000
    _data: Dict[str, Tuple[Any, float]] = field(default_factory=dict)
    _lock: asyncio.Lock = field(default_factory=asyncio.Lock)

    def _make_key(self, symbol: str, kind: str) -> str:
        return f"{self.name}:{kind}:{symbol}"

    async def get(self, symbol: str, kind: str = "quote") -> Optional[Any]:
        """Get value from cache if not expired."""
        key = self._make_key(symbol, kind)
        now = time.monotonic()

        async with self._lock:
            item = self._data.get(key)
            if not item:
                return None
            value, expires_at = item
            if now <= expires_at:
                return value
            self._data.pop(key, None)
            return None

    async def set(
        self,
        symbol: str,
        value: Any,
        kind: str = "quote",
        ttl_sec: Optional[float] = None,
    ) -> None:
        """Set value in cache with TTL; mass-evicts on overflow."""
        key = self._make_key(symbol, kind)
        ttl = ttl_sec if ttl_sec is not None else self.ttl_sec
        expires_at = time.monotonic() + max(0.5, ttl)

        async with self._lock:
            if len(self._data) >= self.max_size and key not in self._data:
                n = max(1, self.max_size // 10)
                for k in list(self._data.keys())[:n]:
                    self._data.pop(k, None)
            self._data[key] = (value, expires_at)

    async def clear(self) -> None:
        """Clear cache."""
        async with self._lock:
            self._data.clear()

    async def size(self) -> int:
        """Get current cache size."""
        async with self._lock:
            return len(self._data)


# =============================================================================
# Technical Indicators (Pure Python with NumPy acceleration)
# =============================================================================

def _simple_returns(closes: List[float]) -> List[float]:
    """Simple returns from close prices."""
    returns: List[float] = []
    for i in range(1, len(closes)):
        prev, curr = closes[i - 1], closes[i]
        if prev and prev > 0:
            returns.append((curr / prev) - 1.0)
    return returns


def _log_returns(closes: List[float]) -> List[float]:
    """Log returns from close prices."""
    returns: List[float] = []
    for i in range(1, len(closes)):
        prev, curr = closes[i - 1], closes[i]
        if prev and prev > 0 and curr and curr > 0:
            returns.append(math.log(curr / prev))
    return returns


def _annualized_vol_from_log_returns(log_returns: List[float]) -> Optional[float]:
    """Annualized volatility from log returns."""
    if len(log_returns) < 2:
        return None
    try:
        if _HAS_NUMPY and np is not None:
            vol = float(np.std(np.asarray(log_returns, dtype=float), ddof=1) * math.sqrt(252.0))
            return None if (math.isnan(vol) or math.isinf(vol)) else vol
        std = statistics.stdev(log_returns)
        return float(std * math.sqrt(252.0))
    except Exception:
        return None


def _volatility_nd(closes: List[float], days: int) -> Optional[float]:
    """N-day annualized volatility as a fraction."""
    if len(closes) < days + 1:
        return None
    window = closes[-(days + 1):]
    return _annualized_vol_from_log_returns(_log_returns(window))


def _max_drawdown_1y(closes: List[float]) -> Optional[float]:
    """Maximum drawdown over ~1 year as a positive fraction."""
    if len(closes) < 20:
        return None
    window = closes[-252:] if len(closes) >= 252 else closes[:]
    peak: Optional[float] = None
    max_dd = 0.0
    for p in window:
        if p is None or p <= 0:
            continue
        if peak is None or p > peak:
            peak = p
        if peak and peak > 0:
            dd = (p / peak) - 1.0
            if dd < max_dd:
                max_dd = dd
    return float(abs(max_dd)) if max_dd < 0 else 0.0


def _var_95_1d(closes: List[float]) -> Optional[float]:
    """95% daily VaR as a positive fraction."""
    returns = _simple_returns(closes)
    if len(returns) < 30:
        return None
    try:
        if _HAS_NUMPY and np is not None:
            q = float(np.quantile(np.asarray(returns, dtype=float), 0.05))
        else:
            sorted_returns = sorted(returns)
            idx = max(0, int(0.05 * (len(sorted_returns) - 1)))
            q = float(sorted_returns[idx])
        return float(abs(q)) if q < 0 else 0.0
    except Exception:
        return None


def _sharpe_1y(closes: List[float]) -> Optional[float]:
    """Annualized Sharpe ratio (dimensionless)."""
    returns = _simple_returns(closes)
    if len(returns) < 60:
        return None
    window = returns[-252:] if len(returns) >= 252 else returns[:]
    if len(window) < 30:
        return None
    try:
        if _HAS_NUMPY and np is not None:
            arr = np.asarray(window, dtype=float)
            mu = float(np.mean(arr))
            sd = float(np.std(arr, ddof=1))
        else:
            mu = float(sum(window) / len(window))
            sd = float(statistics.stdev(window))
        if sd <= 0 or math.isnan(sd) or math.isinf(sd):
            return None
        return float((mu * 252.0) / (sd * math.sqrt(252.0)))
    except Exception:
        return None


def _rsi_14(closes: List[float]) -> Optional[float]:
    """RSI(14) with Wilder's smoothing (NumPy-accelerated with pure-Python fallback)."""
    if len(closes) < 15:
        return None

    period = 14

    if _HAS_NUMPY and np is not None:
        try:
            arr = np.asarray(closes, dtype=float)
            diff = np.diff(arr)
            gains = np.where(diff > 0, diff, 0.0)
            losses = np.where(diff < 0, -diff, 0.0)

            avg_gain = float(gains[:period].mean())
            avg_loss = float(losses[:period].mean())

            for i in range(period, len(gains)):
                avg_gain = (avg_gain * (period - 1) + float(gains[i])) / period
                avg_loss = (avg_loss * (period - 1) + float(losses[i])) / period

            if avg_loss == 0:
                return 100.0
            rs = avg_gain / avg_loss
            return 100.0 - (100.0 / (1.0 + rs))
        except Exception:
            pass

    # Pure Python fallback
    diffs = [closes[i] - closes[i - 1] for i in range(1, len(closes))]
    gains = [d if d > 0 else 0.0 for d in diffs]
    losses = [-d if d < 0 else 0.0 for d in diffs]

    avg_gain = sum(gains[:period]) / period
    avg_loss = sum(losses[:period]) / period

    for i in range(period, len(gains)):
        avg_gain = (avg_gain * (period - 1) + gains[i]) / period
        avg_loss = (avg_loss * (period - 1) + losses[i]) / period

    if avg_loss == 0:
        return 100.0
    rs = avg_gain / avg_loss
    return 100.0 - (100.0 / (1.0 + rs))


def _simple_forecast(
    closes: List[float],
    horizon_days: int,
) -> Tuple[Optional[float], Optional[float], float]:
    """Simple price forecast via log-linear regression. Returns (price, roi_frac, confidence)."""
    if len(closes) < 60 or horizon_days <= 0:
        return None, None, 0.0

    try:
        n = min(len(closes), 252)
        y = closes[-n:]
        if any(p <= 0 for p in y):
            return None, None, 0.0

        last = float(closes[-1])

        if _HAS_NUMPY and np is not None:
            arr = np.asarray(y, dtype=float)
            x = np.arange(len(arr), dtype=float)
            log_y = np.log(arr)
            slope, intercept = np.polyfit(x, log_y, 1)
            pred = slope * x + intercept
            ss_res = float(np.sum((log_y - pred) ** 2))
            ss_tot = float(np.sum((log_y - float(np.mean(log_y))) ** 2))
            r2 = 1.0 - ss_res / ss_tot if ss_tot > 0 else 0.0
            future_x = float(len(arr) - 1 + horizon_days)
            forecast_price = float(math.exp(float(slope) * future_x + float(intercept)))
        else:
            # CAGR-based fallback
            start, end = y[0], y[-1]
            years = max(1e-6, len(y) / 252.0)
            cagr = (end / start) ** (1 / years) - 1
            forecast_price = end * ((1 + cagr) ** (horizon_days / 252.0))
            r2 = 0.25

        roi_fraction = (forecast_price / last) - 1.0
        vol30 = _volatility_nd(closes, 30) or 0.25
        confidence = max(0.05, min(0.95,
            (max(0.0, min(1.0, r2)) * 0.8 + 0.2)
            * (1.0 / (1.0 + max(0.0, vol30 - 0.25)))
        ))
        return forecast_price, roi_fraction, confidence
    except Exception:
        return None, None, 0.0


# =============================================================================
# Yahoo Finance Sync Fetcher (runs in ThreadPool)
# =============================================================================

def _safe_history_metadata(ticker: Any) -> Dict[str, Any]:
    """
    Safely extract history metadata from a yfinance Ticker.

    yfinance populates `ticker.history_metadata` after `.history()` is called.
    v7.0.0 had a dead placeholder that always returned {} -- v8.0.0 reads
    the real attribute, then falls back to the `get_history_metadata()`
    method if present.
    """
    if ticker is None:
        return {}

    try:
        meta = getattr(ticker, "history_metadata", None)
        if isinstance(meta, dict) and meta:
            return meta
    except Exception:
        pass

    try:
        fn = getattr(ticker, "get_history_metadata", None)
        if callable(fn):
            meta = fn()
            if isinstance(meta, dict) and meta:
                return meta
    except Exception:
        pass

    return {}


def _infer_asset_class(symbol: str, info: Any = None, meta: Optional[Dict[str, Any]] = None) -> Optional[str]:
    """Infer asset class from symbol + info + metadata."""
    quote_type = None
    if isinstance(info, dict):
        quote_type = _safe_str(info.get("quoteType") or info.get("quote_type") or info.get("type"))
    if quote_type is None and isinstance(meta, dict):
        quote_type = _safe_str(meta.get("instrumentType") or meta.get("quoteType"))

    if quote_type:
        qt_upper = quote_type.upper()
        if qt_upper in {"CURRENCY", "FOREX", "FX"}:
            return "FX"
        if qt_upper in {"ETF", "MUTUALFUND", "MUTUAL FUND"}:
            return qt_upper
        return qt_upper

    s = symbol.upper()
    if s.endswith(".SR"):
        return "EQUITY"
    if s.endswith("=F"):
        return "FUTURE"
    if s.endswith("=X"):
        return "FX"
    if s.startswith("^"):
        return "INDEX"
    return None


def _infer_exchange(symbol: str, info: Any = None, meta: Optional[Dict[str, Any]] = None) -> Optional[str]:
    """Infer exchange from symbol + info + metadata."""
    if isinstance(info, dict):
        exchange = _first_str(
            info.get("exchange"), info.get("fullExchangeName"), info.get("exchangeName"),
        )
        if exchange:
            return exchange
    if isinstance(meta, dict):
        exchange = _first_str(meta.get("exchangeName"), meta.get("exchangeTimezoneName"))
        if exchange:
            return exchange

    s = symbol.upper()
    if s.endswith(".SR"):
        return "Tadawul"
    if s.endswith("=F"):
        return "NYMEX"
    if s.endswith("=X"):
        return "CCY"
    if s.startswith("^"):
        return "INDEX"
    return None


def _fetch_ticker_sync(
    symbol: str,
    period: str = DEFAULT_HISTORY_PERIOD,
    interval: str = DEFAULT_HISTORY_INTERVAL,
    retry_attempts: int = DEFAULT_RETRY_ATTEMPTS,
    use_ua_rotation: bool = DEFAULT_USER_AGENT_ROTATION,
) -> Tuple[Dict[str, Any], Dict[str, Any], List[Dict[str, Any]], List[str], str]:
    """
    Blocking fetch of ticker data. Runs in ThreadPoolExecutor.

    v8.2.0 changes:
      - Adds `retry_attempts` and `use_ua_rotation` parameters.
      - Wraps the inner fetch in a retry loop with exponential backoff
        and 25% jitter (doubled cooldown on rate-limit signals).
      - Constructs a requests.Session with rotated User-Agent and
        re-rotates UA on every retry.
      - Returns an expanded tuple: (info, meta, history, warnings,
        last_error_class). The warnings list captures field-level data-
        quality flags; `last_error_class` is the type name of the final
        exception (empty string if the fetch eventually succeeded).

    v8.0.0 baseline (preserved):
      - `period` and `interval` are proper config-driven parameters
        (not the v7.0.0 fossil that always returned "2y").

    Returns:
        Tuple of (info_dict, history_metadata, history_list, warnings, last_error_class)
    """
    warnings: List[str] = []
    if not _HAS_YFINANCE or yf is None:
        warnings.append("yfinance_not_installed")
        return {}, {}, [], warnings, "ImportError"

    session = _create_yf_session(use_ua_rotation)
    max_attempts = max(1, int(retry_attempts))
    last_exc: Optional[BaseException] = None
    last_err_class: str = ""

    for attempt in range(max_attempts):
        try:
            ticker = _construct_ticker(symbol, session)
            if ticker is None:
                last_err_class = "RuntimeError"
                last_exc = RuntimeError("yf_ticker_construct_failed")
                break

            # Info: fast_info first, then full .info
            info_dict: Dict[str, Any] = {}
            try:
                fast_info = getattr(ticker, "fast_info", None)
                if fast_info:
                    info_dict = dict(fast_info)
            except Exception:
                pass

            try:
                info = getattr(ticker, "info", None)
                if isinstance(info, dict):
                    info_dict.update(info)
            except Exception:
                pass

            # History (respects config-provided period/interval)
            history_list: List[Dict[str, Any]] = []
            try:
                hist_df = ticker.history(period=period, interval=interval)
                if (_HAS_PANDAS and pd is not None
                        and isinstance(hist_df, pd.DataFrame) and not hist_df.empty):
                    hist_df = hist_df.reset_index()
                    for _, row in hist_df.iterrows():
                        row_dict = row.to_dict()
                        dt_val = row_dict.get("Date") or row_dict.get("Datetime")
                        if hasattr(dt_val, "to_pydatetime"):
                            dt_val = dt_val.to_pydatetime()

                        history_list.append({
                            "timestamp": _utc_iso(dt_val) if isinstance(dt_val, datetime) else str(dt_val),
                            "open": _safe_float(row_dict.get("Open")),
                            "high": _safe_float(row_dict.get("High")),
                            "low": _safe_float(row_dict.get("Low")),
                            "close": _safe_float(row_dict.get("Close")),
                            "volume": _safe_float(row_dict.get("Volume")),
                        })
            except Exception as hist_exc:
                # History failure is non-fatal -- we may still have info.
                # But record the class for diagnostics.
                if _is_rate_limit_error(hist_exc):
                    last_err_class = type(hist_exc).__name__
                    last_exc = hist_exc
                    # Treat this as a retryable failure.
                    raise

            # v8.0.0: capture history_metadata *after* calling .history()
            meta = _safe_history_metadata(ticker)

            # Field-level warnings
            if not history_list:
                warnings.append("chart_history_empty")
            if not info_dict:
                warnings.append("info_empty")

            # Industry / currency / sector enrichment can be sparse on
            # Yahoo for non-equity instruments. Flag for downstream.
            if isinstance(info_dict, dict):
                if not _safe_str(info_dict.get("industry")):
                    warnings.append("industry_missing_from_provider")
                if not _safe_str(info_dict.get("currency") or info_dict.get("financialCurrency")):
                    warnings.append("currency_missing_from_provider")

            return info_dict, meta, history_list, warnings, ""

        except Exception as exc:
            last_exc = exc
            last_err_class = type(exc).__name__

            # v8.2.0: exponential backoff with jitter; doubled cooldown
            # when the error string indicates rate-limiting.
            base = min(8.0, 0.5 * (2 ** attempt))
            if _is_rate_limit_error(exc):
                base = min(16.0, base * 2.0)
            sleep_for = base + random.uniform(0.0, base * 0.25)
            time.sleep(sleep_for)

            # Rotate UA on retry to evade per-UA throttling.
            _rotate_session_ua(session)

    # All attempts exhausted
    if last_exc is not None:
        logger.warning("Sync fetch failed for %s after %d attempts: %s",
                       symbol, max_attempts, last_exc)
        warnings.append(f"fetch_failed:{last_err_class}")
    return {}, {}, [], warnings, last_err_class or "Unknown"


def _enrich_data(
    symbol: str,
    info: Dict[str, Any],
    meta: Dict[str, Any],
    history: List[Dict[str, Any]],
    *,
    upstream_warnings: Optional[List[str]] = None,
    price_sanity_guard: bool = DEFAULT_PRICE_SANITY_GUARD,
    price_ratio_high: float = DEFAULT_PRICE_RATIO_HIGH,
    price_ratio_low: float = DEFAULT_PRICE_RATIO_LOW,
) -> Dict[str, Any]:
    """
    Enrich raw Yahoo data into the canonical patch shape.

    v8.2.0:
      - Accepts `upstream_warnings` so field-level flags raised in
        `_fetch_ticker_sync` (chart_history_empty, info_empty,
        industry_missing_from_provider, ...) flow through to the
        output patch's `warnings` list.
      - 52W high/low pass through `_validate_52w_bounds()` to catch
        currency-mismatch and history-derived unit problems.
      - `forecast_price_12m` passes through `_validate_forecast_
        magnitude()`; mis-scaled forecasts are dropped (and
        `expected_roi_12m` is dropped alongside them since it would
        propagate the bad ratio).
    """
    warnings: List[str] = list(upstream_warnings or [])

    # 1. Quote core
    price = _first_number(
        info.get("currentPrice"),
        info.get("regularMarketPrice"),
        info.get("lastPrice"),
    )
    prev_close = _first_number(
        info.get("previousClose"),
        info.get("regularMarketPreviousClose"),
    )

    # Fallback to history when live fields are missing
    if not price and history:
        price = history[-1].get("close")
        if not prev_close and len(history) > 1:
            prev_close = history[-2].get("close")

    percent_change = None
    if price and prev_close and prev_close > 0:
        percent_change = (price / prev_close) - 1.0

    change = None
    if price and prev_close:
        change = price - prev_close

    # 2. Volume + market cap
    volume = _first_number(info.get("volume"), info.get("regularMarketVolume"))
    if not volume and history:
        volume = history[-1].get("volume")

    market_cap = _first_number(info.get("marketCap"))

    # 3. 52-week range (with v8.2.0 currency-mismatch validation)
    info_52w_high = _first_number(info.get("fiftyTwoWeekHigh"))
    info_52w_low = _first_number(info.get("fiftyTwoWeekLow"))

    hist_52w_high: Optional[float] = None
    hist_52w_low: Optional[float] = None
    closes_252 = [h["close"] for h in history[-252:] if h.get("close")]
    if closes_252:
        hist_52w_high = max(closes_252)
        hist_52w_low = min(closes_252)

    week_52_high, week_52_low, bounds_warnings = _validate_52w_bounds(
        current_price=price,
        info_52w_high=info_52w_high,
        info_52w_low=info_52w_low,
        hist_52w_high=hist_52w_high,
        hist_52w_low=hist_52w_low,
        enabled=price_sanity_guard,
        ratio_high=price_ratio_high,
        ratio_low=price_ratio_low,
    )
    warnings.extend(bounds_warnings)

    week_52_position = None
    if price and week_52_high and week_52_low and week_52_high > week_52_low:
        # Clamp to [0, 1] -- a price slightly outside the validated band
        # is independently flagged by `current_price_outside_52w_range`.
        pos = (price - week_52_low) / (week_52_high - week_52_low)
        week_52_position = max(0.0, min(1.0, pos))

    # 4. History-derived risk metrics (unit-invariant -- safe regardless
    # of any currency drift in the price series itself)
    closes_list = [h["close"] for h in history if h.get("close")]
    volatility_30d = _volatility_nd(closes_list, 30)
    volatility_90d = _volatility_nd(closes_list, 90)
    rsi_14 = _rsi_14(closes_list)
    max_drawdown_1y = _max_drawdown_1y(closes_list)
    sharpe_1y = _sharpe_1y(closes_list)
    var_95_1d = _var_95_1d(closes_list)

    # 5. Forecast (with v8.2.0 magnitude sanity check)
    forecast_price_12m, expected_roi_12m, forecast_confidence = _simple_forecast(closes_list, 252)
    forecast_price_12m, forecast_warnings = _validate_forecast_magnitude(
        forecast_price=forecast_price_12m,
        current_price=price,
        enabled=price_sanity_guard,
        ratio_high=price_ratio_high,
        ratio_low=price_ratio_low,
    )
    warnings.extend(forecast_warnings)
    if forecast_price_12m is None:
        # Drop the dependent ROI too, since it would just propagate the
        # bad ratio. Confidence stays so downstream knows the regression
        # was attempted.
        expected_roi_12m = None

    # 6. Fundamentals (light)
    dividend_yield = _first_fraction(
        info.get("dividendYield"),
        info.get("trailingAnnualDividendYield"),
    )
    gross_margin = _first_fraction(info.get("grossMargins"))
    operating_margin = _first_fraction(info.get("operatingMargins"))
    profit_margin = _first_fraction(info.get("profitMargins"))
    roe = _first_fraction(info.get("returnOnEquity"))

    # 7. Valuation
    pe_ttm = _first_number(info.get("trailingPE"))
    pb_ratio = _first_number(info.get("priceToBook"))
    eps_ttm = _first_number(info.get("trailingEps"))

    # 8. Identity (v8.0.0: real `meta` from ticker.history_metadata)
    asset_class = _infer_asset_class(symbol, info, meta)
    exchange = _infer_exchange(symbol, info, meta)
    currency = _first_str(info.get("currency"), info.get("financialCurrency"))

    # v8.0.0 minor: compute open_price once instead of twice
    open_price = _first_number(info.get("open"), info.get("regularMarketOpen"))
    day_high = _first_number(info.get("dayHigh"), info.get("regularMarketDayHigh"))
    day_low = _first_number(info.get("dayLow"), info.get("regularMarketDayLow"))

    # Data quality
    if price and history:
        data_quality = "EXCELLENT"
    elif price:
        data_quality = "GOOD"
    else:
        data_quality = "STALE"

    # Dedupe warnings while preserving order
    seen_w: set = set()
    deduped_warnings: List[str] = []
    for w in warnings:
        if w and w not in seen_w:
            seen_w.add(w)
            deduped_warnings.append(w)

    result = {
        "symbol": symbol,
        "symbol_normalized": symbol,
        "provider": PROVIDER_NAME,
        "provider_version": PROVIDER_VERSION,
        "data_source": PROVIDER_NAME,
        "data_quality": data_quality,
        "last_updated_utc": _utc_iso(),
        "last_updated_riyadh": _riyadh_iso(),
        # Price fields
        "current_price": price,
        "price": price,
        "previous_close": prev_close,
        "prev_close": prev_close,
        "price_change": change,
        "change": change,
        "percent_change": percent_change,
        "change_pct": percent_change,
        "open_price": open_price,
        "open": open_price,
        "day_high": day_high,
        "day_low": day_low,
        "volume": volume,
        "market_cap": market_cap,
        # 52-week
        "week_52_high": week_52_high,
        "week_52_low": week_52_low,
        "week_52_position_pct": week_52_position,
        # Fundamentals
        "dividend_yield": dividend_yield,
        "pe_ttm": pe_ttm,
        "pb_ratio": pb_ratio,
        "roe": roe,
        "eps_ttm": eps_ttm,
        "gross_margin": gross_margin,
        "operating_margin": operating_margin,
        "profit_margin": profit_margin,
        # Risk metrics (unit-invariant, safe even if history currency drifted)
        "volatility_30d": volatility_30d,
        "volatility_90d": volatility_90d,
        "rsi_14": rsi_14,
        "max_drawdown_1y": max_drawdown_1y,
        "sharpe_1y": sharpe_1y,
        "var_95_1d": var_95_1d,
        # Forecast
        "forecast_price_12m": forecast_price_12m,
        "expected_roi_12m": expected_roi_12m,
        "forecast_confidence": forecast_confidence,
        # Identity
        "currency": currency,
        "asset_class": asset_class,
        "exchange": exchange,
        # v8.2.0: structured warnings
        "warnings": deduped_warnings,
    }

    # Don't strip `warnings` even if empty -- downstream
    # `enriched_quote._normalize_warnings_field` will collapse an
    # empty list to None gracefully.
    cleaned = clean_dict(result)
    if "warnings" not in cleaned and deduped_warnings:
        cleaned["warnings"] = deduped_warnings
    return cleaned


# =============================================================================
# Yahoo Chart Provider (async wrapper)
# =============================================================================

class YahooChartProvider:
    """Async wrapper for Yahoo Finance."""

    def __init__(self, config: Optional[YahooConfig] = None):
        self.config = config or YahooConfig.from_env()
        self._token_bucket = TokenBucket(
            rate_per_sec=self.config.rate_limit_per_sec,
            burst=self.config.rate_limit_burst,
        )
        self._circuit_breaker = CircuitBreaker(
            fail_threshold=self.config.cb_fail_threshold,
            cooldown_sec=self.config.cb_cooldown_sec,
            success_threshold=self.config.cb_success_threshold,
        )
        self._single_flight = SingleFlight()
        self._cache = AdvancedCache(
            name="yahoo",
            ttl_sec=self.config.quote_ttl_sec,
            max_size=5000,
        )
        self._executor = concurrent.futures.ThreadPoolExecutor(
            max_workers=self.config.threadpool_workers,
            thread_name_prefix="YahooWorker",
        )
        logger.info(
            "YahooChartProvider v%s initialized | yfinance=%s | requests=%s | "
            "UA_rotation=%s | sanity_guard=%s | retries=%s | "
            "concurrency=%s | rate=%s/s | cb=%s/%ss",
            PROVIDER_VERSION,
            _HAS_YFINANCE,
            _HAS_REQUESTS,
            self.config.user_agent_rotation,
            self.config.price_sanity_guard,
            self.config.retry_attempts,
            self.config.max_concurrency,
            self.config.rate_limit_per_sec,
            self.config.cb_fail_threshold,
            self.config.cb_cooldown_sec,
        )

    async def get_enriched_quote(self, symbol: str) -> Optional[Dict[str, Any]]:
        """Get an enriched quote for a single symbol."""
        if not self.config.is_available or not symbol:
            return None

        sym = normalize_symbol(symbol)
        # v8.0.0: early-return on empty normalized symbol
        if not sym:
            return None

        cached = await self._cache.get(sym, kind="enriched")
        metrics = _get_metrics()
        if cached:
            metrics.cache_hits_total.labels(symbol=sym).inc()
            return cached

        metrics.cache_misses_total.labels(symbol=sym).inc()

        if not await self._circuit_breaker.allow():
            return None

        await self._token_bucket.acquire()

        async def _do_fetch() -> Optional[Dict[str, Any]]:
            loop = asyncio.get_running_loop()
            start_time = time.monotonic()

            try:
                # v8.2.0: pass retry + UA-rotation config to the worker.
                info, meta, history, fetch_warnings, err_class = await loop.run_in_executor(
                    self._executor,
                    _fetch_ticker_sync,
                    sym,
                    self.config.history_period,
                    self.config.history_interval,
                    self.config.retry_attempts,
                    self.config.user_agent_rotation,
                )

                if not info and not history:
                    rate_limited = bool(err_class) and any(
                        m in (err_class or "").lower() for m in _RATE_LIMIT_MARKERS
                    )
                    await self._circuit_breaker.record_failure(rate_limited=rate_limited)
                    metrics.requests_total.labels(symbol=sym, op="enriched", status="error").inc()
                    return None

                result = _enrich_data(
                    symbol=sym,
                    info=info,
                    meta=meta,
                    history=history,
                    upstream_warnings=fetch_warnings,
                    price_sanity_guard=self.config.price_sanity_guard,
                    price_ratio_high=self.config.price_ratio_high,
                    price_ratio_low=self.config.price_ratio_low,
                )

                await self._circuit_breaker.record_success()
                metrics.requests_total.labels(symbol=sym, op="enriched", status="ok").inc()
                metrics.request_duration.labels(symbol=sym, op="enriched").observe(
                    time.monotonic() - start_time
                )

                if result.get("current_price"):
                    await self._cache.set(sym, result, kind="enriched")

                return result
            except Exception as exc:
                await self._circuit_breaker.record_failure(rate_limited=_is_rate_limit_error(exc))
                metrics.requests_total.labels(symbol=sym, op="enriched", status="error").inc()
                logger.error("Error fetching %s: %s", sym, exc)
                return None

        return await self._single_flight.run(f"enriched:{sym}", _do_fetch)

    async def get_enriched_quotes_batch(
        self,
        symbols: List[str],
        concurrency: Optional[int] = None,
    ) -> Dict[str, Dict[str, Any]]:
        """Batch fetch enriched quotes for multiple symbols."""
        if not symbols:
            return {}

        semaphore = asyncio.Semaphore(concurrency or self.config.max_concurrency)

        async def _fetch_one(sym: str) -> Tuple[str, Optional[Dict[str, Any]]]:
            async with semaphore:
                result = await self.get_enriched_quote(sym)
                return sym, result

        results = await asyncio.gather(
            *(_fetch_one(sym) for sym in symbols if sym),
            return_exceptions=True,
        )

        output: Dict[str, Dict[str, Any]] = {}
        for result in results:
            if isinstance(result, Exception):
                continue
            sym, data = result
            if data:
                output[sym] = data
        return output

    async def fetch_history_raw(
        self,
        symbol: str,
        period: str = "1mo",
        interval: str = "1d",
    ) -> List[Dict[str, Any]]:
        """
        Fetch raw history rows for a symbol.

        v8.2.0: also uses the UA-rotated session when configured.
        v8.0.0: runs on the provider's shared executor (no thread leak).
        """
        if not _HAS_YFINANCE or yf is None:
            return []

        sym = normalize_symbol(symbol)
        if not sym:
            return []

        use_rotation = self.config.user_agent_rotation

        def _sync_fetch() -> List[Dict[str, Any]]:
            session = _create_yf_session(use_rotation)
            try:
                ticker = _construct_ticker(sym, session)
                if ticker is None:
                    return []
                df = ticker.history(period=period, interval=interval)
                if not _HAS_PANDAS or pd is None or df is None or df.empty:
                    return []
                df = df.reset_index()
                rows: List[Dict[str, Any]] = []
                for _, row in df.iterrows():
                    row_dict = row.to_dict()
                    dt_val = row_dict.get("Date") or row_dict.get("Datetime")
                    ts = _utc_iso(dt_val) if hasattr(dt_val, "to_pydatetime") else str(dt_val)
                    rows.append({
                        "timestamp": ts,
                        "open": _safe_float(row_dict.get("Open")),
                        "high": _safe_float(row_dict.get("High")),
                        "low": _safe_float(row_dict.get("Low")),
                        "close": _safe_float(row_dict.get("Close")),
                        "volume": _safe_float(row_dict.get("Volume")),
                    })
                return rows
            except Exception as exc:
                logger.warning("History fetch failed for %s: %s", sym, exc)
                return []

        try:
            loop = asyncio.get_running_loop()
            return await loop.run_in_executor(self._executor, _sync_fetch)
        except Exception as exc:
            logger.warning("History executor failed for %s: %s", sym, exc)
            return []

    async def close(self) -> None:
        """Close the provider and shut down the thread pool."""
        try:
            self._executor.shutdown(wait=False)
        except Exception as exc:
            logger.debug("yahoo provider close failed: %s", exc)


# =============================================================================
# Singleton Instance (lazy lock)
# =============================================================================

_PROVIDER_INSTANCE: Optional[YahooChartProvider] = None
_PROVIDER_LOCK: Optional[asyncio.Lock] = None


def _get_provider_lock() -> asyncio.Lock:
    global _PROVIDER_LOCK
    if _PROVIDER_LOCK is None:
        _PROVIDER_LOCK = asyncio.Lock()
    return _PROVIDER_LOCK


async def get_provider() -> YahooChartProvider:
    """Get the singleton YahooChartProvider instance."""
    global _PROVIDER_INSTANCE
    if _PROVIDER_INSTANCE is not None:
        return _PROVIDER_INSTANCE
    async with _get_provider_lock():
        if _PROVIDER_INSTANCE is None:
            _PROVIDER_INSTANCE = YahooChartProvider()
    return _PROVIDER_INSTANCE


async def close_provider() -> None:
    """Close and reset the singleton provider."""
    global _PROVIDER_INSTANCE
    if _PROVIDER_INSTANCE is not None:
        await _PROVIDER_INSTANCE.close()
        _PROVIDER_INSTANCE = None


# =============================================================================
# Engine-Facing Functions
# =============================================================================

async def fetch_enriched_quote(symbol: str) -> Optional[Dict[str, Any]]:
    """Fetch enriched quote for a symbol."""
    provider = await get_provider()
    return await provider.get_enriched_quote(symbol)


async def fetch_enriched_quotes_batch(symbols: List[str]) -> Dict[str, Dict[str, Any]]:
    """Batch fetch enriched quotes for multiple symbols."""
    provider = await get_provider()
    return await provider.get_enriched_quotes_batch(symbols)


async def fetch_quote(symbol: str) -> Optional[Dict[str, Any]]:
    """Alias for fetch_enriched_quote."""
    return await fetch_enriched_quote(symbol)


async def fetch_history(
    symbol: str,
    period: str = "1mo",
    interval: str = "1d",
) -> List[Dict[str, Any]]:
    """
    Fetch raw history for a symbol.

    v8.0.0: delegates to the provider's shared ThreadPoolExecutor, so
    repeated calls no longer leak a fresh executor per invocation.
    v8.2.0: the underlying yfinance call routes through the provider's
    UA-rotated session when YAHOO_USER_AGENT_ROTATION is enabled.
    """
    provider = await get_provider()
    return await provider.fetch_history_raw(symbol, period=period, interval=interval)


# =============================================================================
# Module Exports
# =============================================================================

__all__ = [
    # Metadata
    "PROVIDER_NAME",
    "PROVIDER_VERSION",
    "VERSION",
    "PROVIDER_BATCH_SUPPORTED",
    # Classes
    "YahooChartProvider",
    "YahooConfig",
    # Singletons
    "get_provider",
    "close_provider",
    # Engine-facing
    "fetch_enriched_quote",
    "fetch_enriched_quotes_batch",
    "fetch_quote",
    "fetch_history",
    # Symbol helper
    "normalize_symbol",
    # Exceptions (kept for defensive back-compat even though unraised internally)
    "YahooProviderError",
    "YahooFetchError",
]
