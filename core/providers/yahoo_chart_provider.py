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
Provider-side identity alignment with `enriched_quote.py` v4.6.0
`_SUFFIX_TO_LOCALE`. Goal: when Yahoo's `info` payload doesn't return
`exchange` / `currency` / `country` (delayed feeds, ADRs, quirky listings),
the provider populates them from the symbol's suffix instead of leaving
them blank. The downstream normalization stage in `enriched_quote.py`
then has nothing left to repair.

Background: the May 2026 Global_Markets audit showed Kuwait/Qatar/UAE/
South Africa/Egypt/Israel listings (MABANEE.KW, OOREDOO.KW, ANG.JSE,
MTN.JSE, etc.) reaching the sheet tagged as NASDAQ / USD / USA because
Yahoo's `info.exchange`, `info.currency`, and `info.country` were
empty for those tickers. The engine's defensive correction in
`enriched_quote.py` v4.6.0 catches this downstream, but plugging the
hole at the source is cleaner and gives a single point of audit.

New:
  - `_infer_symbol_metadata_external`: optional binding to
    `normalize.infer_symbol_metadata()` (the v5.3.0+ SSOT used by
    `enriched_quote.py` v4.6.0). When the import succeeds, the provider
    delegates to it for identity inference.
  - `_SUFFIX_TO_LOCALE_DEFAULTS`: 64-entry mapping of Yahoo suffixes
    to `(exchange, currency, country)` — byte-identical structure to
    enriched_quote v4.6.0 `_SUFFIX_TO_LOCALE`. Used as fallback when
    normalize.py SSOT is unavailable. Covers GCC (.KW, .QA, .AE, .DFM,
    .ADX, .SR, .EG, .EGX), MENA (.TA, .TASE, .IS), Europe (.L, .LSE,
    .DE, .PA, .AS, .MI, .MC, .BR, .LS, .HE, .ST, .OL, .SW, .CO, .IR,
    .WA, .VI, .PR, .BD, .AT), Asia-Pacific (.HK, .NS, .NSE, .BO, .T,
    .TYO, .KS, .KQ, .SI, .KL, .BK, .JK, .SS, .SZ, .TW, .TWO, .AX, .NZ),
    Americas (.SA, .MX, .BA, .TO, .V, .CN, .NE, .SN, .LM), and Africa
    (.JO, .JSE).
  - `_identity_defaults_for_symbol(symbol)`: returns a dict with
    `exchange`, `currency`, `country`, `asset_class` keys derived from
    the symbol's suffix structure. Resolution order: special patterns
    (=F, =X, ^) first; then SSOT delegation; then longest-match suffix
    in `_SUFFIX_TO_LOCALE_DEFAULTS`; then plain-alpha-as-US-equity
    convention; finally all None for unknown formats.

Modified:
  - `_infer_asset_class`: when Yahoo's `quoteType` is blank, falls
    through to `_identity_defaults_for_symbol` so 60+ stock-exchange
    suffixes return `EQUITY` (previously only `.SR` did).
  - `_infer_exchange`: same fallthrough for exchange-display names.
    MABANEE.KW now returns "Boursa Kuwait" instead of None.
  - `_enrich_data`: identity block now adds a `country` field to the
    patch shape (NEW — was absent in v8.1.0) and fills blank
    `currency` / `exchange` / `country` from the suffix-derived defaults
    when Yahoo's `info` left them empty. Yahoo's own values always win
    when present.

Bumped:
  - `PROVIDER_VERSION = "8.2.0"`.

NOT changed (deliberate):
  - v8.1.0 FX-symbol fix (bare 6-letter pairs -> `=X`) preserved verbatim.
  - v8.0.0 fixes (period/interval config, shared executor, real metadata)
    preserved verbatim.
  - Direct Yahoo Chart API fallback for futures (deferred to v8.3.0).
  - Patch shape is additive only: existing fields keep their values,
    `country` is added. Downstream consumers see one more key, never
    a missing or renamed one.

v8.1.0 Changes (from v8.0.0)
----------------------------
Surgical FX-symbol fix. Tadawul Tracker user reported the entire
Commodities_FX sheet was filling with placeholder rows for symbols like
AUDCAD, EURUSD, USDSAR, NZDJPY, USDTRY -- the engine got `None` back
from `fetch_enriched_quote` and `data_engine_v2._compute_scores_fallback`
filled the gap with synthetic data (101/102/103-style forecasts and
9700% ROIs).

Root cause: `normalize_symbol("AUDCAD")` returned `"AUDCAD"` unchanged.
Yahoo Finance requires the `=X` suffix for currency pairs, so
`yf.Ticker("AUDCAD").history()` returned an empty DataFrame, which
collapsed all the way back to the engine's placeholder fallback.

Bug fix:
  - `normalize_symbol` now detects bare 6-letter currency-pair tokens
    (where both the base and quote are in a known-currency set) and
    appends `=X` so yfinance can actually resolve them. KSA codes,
    crypto pseudo-pairs (BTCUSD), already-suffixed FX (EURUSD=X),
    futures (GC=F), indices (^GSPC), and US stocks remain untouched.

New helpers:
  - `_BARE_FX_RE`: compiled regex matching exactly six A-Z chars.
  - `_KNOWN_CURRENCIES`: frozenset of ISO 4217 codes for currencies
    that appear on Tadawul-tracked sheets and major global pairs.
    Includes GCC currencies (SAR, AED, QAR, KWD, BHD, OMR), majors
    (USD, EUR, GBP, JPY, CHF, CAD, AUD, NZD), regional emerging
    (TRY, EGP, ILS, ZAR), and other commonly-traded codes.
  - `_is_likely_fx_pair(symbol)`: returns True when both halves of a
    6-letter token are recognized currencies. Two-half membership test
    (rather than a string-prefix scan) keeps the function O(1) on the
    32-currency table while staying safe against 6-letter US tickers
    that happen to start with `USD` or end with `EUR`.

Bumped:
  - `PROVIDER_VERSION = "8.1.0"`.
  - Module docstring now documents the v8.1.0 changes.

NOT changed (deliberate scope limit):
  - Direct Yahoo Chart API fallback for futures (=F symbols) is
    deferred to v8.2.0. yfinance fails on Render's egress IP for
    GC=F / SI=F / CL=F because Yahoo rate-limits those requests, and
    fixing it requires adding an httpx-based fallback to
    query1.finance.yahoo.com/v8/finance/chart/{SYMBOL} with proper
    headers + JSON parsing. That is a 200+ line change. Keeping v8.1.0
    surgical so the FX fix can ship and be verified independently.
  - Placeholder injection in `data_engine_v2._compute_scores_fallback`
    is upstream of this provider and lives in a different file.

v8.0.0 Changes (carried over)
-----------------------------
Bug fixes:
  - `_fetch_ticker_sync` now accepts period/interval from config. v7.0.0
    had the fossil expression
      period=_HAS_PANDAS and pd is not None and "YAHOO_HISTORY_PERIOD"
             in globals() or "2y"
    which always evaluated to the string literal "2y" -- regardless of
    YahooConfig.history_period, YAHOO_HISTORY_PERIOD env var, or anything
    else. `interval` was hardcoded "1d" too. Both are now config-driven.
  - `_safe_history_metadata(None)` placeholder removed. v7.0.0 called this
    with None (comment: "Placeholder"), got back {} every time, and fed
    an empty dict to `_infer_asset_class` / `_infer_exchange`. Metadata is
    now extracted from the live ticker inside `_fetch_ticker_sync` and
    flowed through the pipeline.
  - `fetch_history` no longer leaks a fresh `ThreadPoolExecutor` on every
    call. Uses the provider's shared executor instead, and respects the
    config's thread-pool sizing and shutdown semantics.
  - Module-level `_PROVIDER_LOCK` is now lazy-initialized (parity with
    other v6 providers).
  - The `_trace` decorator used `@functools.wraps` without importing
    `functools`. It was never applied, so this never fired -- but any
    attempt to use it would raise NameError. Removed (was dead code).

Cleanup:
  - Removed unused imports: `cast`, `Union`, `functools`.
  - Removed dead helpers: `_trace` decorator, `_TraceContext` class,
    `_json_dumps` (defined but never called anywhere).
  - `_safe_history_metadata` simplified: no longer loops over
    single-element tuples.
  - `_enrich_data` deduplicates the `_first_number` calls for
    `open_price`/`open` (computed once).
  - Early-return from `get_enriched_quote` when `normalize_symbol("")`
    yields an empty string (avoids a pointless cache lookup).

Preserved for backward compatibility:
  - Every name in __all__.
  - All environment variable names and defaults.
  - Patch shape (all price, fundamentals, risk, forecast fields).
  - Prometheus metrics integration (with DummyMetric fallback).
  - Optional numpy/pandas/yfinance/orjson/prometheus/opentelemetry support.
  - Provider singleton + engine-facing functions.
  - KSA `.SR` symbol handling and Arabic-digit parsing.
================================================================================
"""

from __future__ import annotations

import asyncio
import concurrent.futures
import json
import logging
import math
import os
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
# v8.2.0: Optional Identity SSOT (normalize.py v5.3.0+)
# =============================================================================
#
# The PRIMARY path for symbol -> (exchange, currency, country) inference
# delegates to core/symbols/normalize.py::infer_symbol_metadata() -- the
# same SSOT used by enriched_quote.py v4.6.0. When that import fails
# (running against an older normalize.py, or no core package on path),
# we fall through to the local _SUFFIX_TO_LOCALE_DEFAULTS table further
# down in this module.

_infer_symbol_metadata_external: Optional[Callable[[str], Dict[str, Any]]] = None
for _norm_path in (
    "core.symbols.normalize",
    "core.normalize",
    "symbols.normalize",
    "normalize",
):
    try:
        _norm_mod_v82 = __import__(_norm_path, fromlist=["infer_symbol_metadata"])
        _fn_v82 = getattr(_norm_mod_v82, "infer_symbol_metadata", None)
        if callable(_fn_v82):
            _infer_symbol_metadata_external = _fn_v82
            break
    except ImportError:
        continue
    except Exception:
        continue

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
_ARABIC_DIGITS = str.maketrans("\u0660\u0661\u0662\u0663\u0664\u0665\u0666\u0667\u0668\u0669", "0123456789")
_K_M_B_T_RE = re.compile(r"^(-?\d+(?:\.\d+)?)([KMBT])$", re.IGNORECASE)
_K_M_B_T_MULT = {"K": 1_000.0, "M": 1_000_000.0, "B": 1_000_000_000.0, "T": 1_000_000_000_000.0}

# v8.1.0: bare-6-letter FX pair detection
# ----------------------------------------
# Regex matches symbols that are exactly six A-Z characters with nothing
# else attached (no `=X`, no `=F`, no `.`, no digit).
_BARE_FX_RE = re.compile(r"^[A-Z]{6}$")

# ISO 4217 codes for currencies the Tadawul Tracker pages reference, plus
# the global majors. A 6-letter token only converts to `=X` when BOTH the
# 3-letter base and 3-letter quote are members of this set, which keeps
# us safe from collisions with 6-letter US/global stock tickers.
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
        if not s or s.lower() in {"-", "\u2014", "n/a", "na", "null", "none", "nan"}:
            return None

        s = s.translate(_ARABIC_DIGITS)
        s = s.replace(",", "").replace("%", "").replace("+", "")
        s = s.replace("$", "").replace("\u00a3", "").replace("\u20ac", "")
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
    #    Both halves must be in _KNOWN_CURRENCIES, so 6-letter US stock
    #    tickers and BTCUSD-style crypto are not affected.
    if _is_likely_fx_pair(s):
        return f"{s}=X"

    # 3. Otherwise keep the symbol as-is (ETFs, futures, indices,
    #    already-suffixed FX, US/global equities, KSA .SR, etc).
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

    async def record_failure(self) -> None:
        """Record a failed request (may open the breaker)."""
        async with self.lock:
            self.failures += 1
            if self.state == "half_open":
                self.state = "open"
                self.opened_at = time.monotonic()
                return
            if self.failures >= self.fail_threshold:
                self.state = "open"
                self.opened_at = time.monotonic()


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
# v8.2.0: Identity Defaults Table (suffix -> exchange/currency/country)
# =============================================================================
#
# Byte-aligned with enriched_quote.py v4.6.0 _SUFFIX_TO_LOCALE. Used as
# fallback when the normalize.py SSOT (`_infer_symbol_metadata_external`)
# is unavailable. Adding a new suffix here AND in enriched_quote.py keeps
# the two layers in sync; preferring the SSOT route avoids the need.

_SUFFIX_TO_LOCALE_DEFAULTS: Dict[str, Tuple[str, str, str]] = {
    # Hong Kong
    ".HK":    ("HKEX",                    "HKD", "Hong Kong"),
    # United Kingdom
    ".L":     ("LSE",                     "GBp", "United Kingdom"),
    ".LON":   ("LSE",                     "GBp", "United Kingdom"),
    ".LSE":   ("LSE",                     "GBp", "United Kingdom"),
    # Denmark
    ".CO":    ("Copenhagen",              "DKK", "Denmark"),
    # India
    ".NS":    ("NSE",                     "INR", "India"),
    ".NSE":   ("NSE",                     "INR", "India"),
    ".BO":    ("BSE",                     "INR", "India"),
    # Brazil
    ".SA":    ("B3",                      "BRL", "Brazil"),
    # Saudi Arabia
    ".SR":    ("Tadawul",                 "SAR", "Saudi Arabia"),
    # Canada
    ".TO":    ("TSX",                     "CAD", "Canada"),
    ".V":     ("TSX Venture",             "CAD", "Canada"),
    ".CN":    ("CSE",                     "CAD", "Canada"),
    ".NE":    ("NEO Exchange",            "CAD", "Canada"),
    # Germany
    ".XETRA": ("XETRA",                   "EUR", "Germany"),
    ".DE":    ("XETRA",                   "EUR", "Germany"),
    ".F":     ("Frankfurt",               "EUR", "Germany"),
    ".HM":    ("Hamburg",                 "EUR", "Germany"),
    ".MU":    ("Munich",                  "EUR", "Germany"),
    # France / Benelux / Iberia / Italy
    ".PA":    ("Euronext Paris",          "EUR", "France"),
    ".AS":    ("Euronext Amsterdam",      "EUR", "Netherlands"),
    ".MI":    ("Borsa Italiana",          "EUR", "Italy"),
    ".MC":    ("BME",                     "EUR", "Spain"),
    ".BR":    ("Euronext Brussels",       "EUR", "Belgium"),
    ".LS":    ("Euronext Lisbon",         "EUR", "Portugal"),
    ".HE":    ("Helsinki",                "EUR", "Finland"),
    ".IR":    ("Euronext Dublin",         "EUR", "Ireland"),
    # Scandinavia / Switzerland
    ".ST":    ("Stockholm",               "SEK", "Sweden"),
    ".OL":    ("Oslo",                    "NOK", "Norway"),
    ".SW":    ("SIX",                     "CHF", "Switzerland"),
    # Oceania
    ".AX":    ("ASX",                     "AUD", "Australia"),
    ".NZ":    ("NZX",                     "NZD", "New Zealand"),
    # Japan / Korea / SE Asia
    ".T":     ("TSE",                     "JPY", "Japan"),
    ".TYO":   ("TSE",                     "JPY", "Japan"),
    ".KS":    ("KRX",                     "KRW", "South Korea"),
    ".KQ":    ("KOSDAQ",                  "KRW", "South Korea"),
    ".SI":    ("SGX",                     "SGD", "Singapore"),
    ".KL":    ("Bursa Malaysia",          "MYR", "Malaysia"),
    ".BK":    ("SET",                     "THB", "Thailand"),
    ".JK":    ("IDX",                     "IDR", "Indonesia"),
    # Greater China / Taiwan
    ".SS":    ("Shanghai",                "CNY", "China"),
    ".SZ":    ("Shenzhen",                "CNY", "China"),
    ".TW":    ("TWSE",                    "TWD", "Taiwan"),
    ".TWO":   ("TPEx",                    "TWD", "Taiwan"),
    # Latin America / Africa
    ".MX":    ("BMV",                     "MXN", "Mexico"),
    ".BA":    ("BCBA",                    "ARS", "Argentina"),
    ".JO":    ("JSE",                     "ZAR", "South Africa"),
    ".JSE":   ("JSE",                     "ZAR", "South Africa"),
    # GCC (Kuwait / Qatar / UAE)
    ".KW":    ("Boursa Kuwait",           "KWD", "Kuwait"),
    ".KSE":   ("Boursa Kuwait",           "KWD", "Kuwait"),
    ".QA":    ("QSE",                     "QAR", "Qatar"),
    ".QE":    ("QSE",                     "QAR", "Qatar"),
    ".AE":    ("DFM/ADX",                 "AED", "United Arab Emirates"),
    ".DFM":   ("DFM",                     "AED", "United Arab Emirates"),
    ".ADX":   ("ADX",                     "AED", "United Arab Emirates"),
    # MENA other
    ".EG":    ("EGX",                     "EGP", "Egypt"),
    ".EGX":   ("EGX",                     "EGP", "Egypt"),
    ".TA":    ("TASE",                    "ILS", "Israel"),
    ".TASE":  ("TASE",                    "ILS", "Israel"),
    # Emerging Europe (v4.6.0 expansion)
    ".IS":    ("BIST",                    "TRY", "Turkey"),
    ".WA":    ("GPW",                     "PLN", "Poland"),
    ".VI":    ("Wien",                    "EUR", "Austria"),
    ".PR":    ("PSE",                     "CZK", "Czech Republic"),
    ".BD":    ("BUX",                     "HUF", "Hungary"),
    ".AT":    ("ATHEX",                   "EUR", "Greece"),
    # Latin America (v4.6.0 expansion)
    ".SN":    ("Santiago",                "CLP", "Chile"),
    ".LM":    ("Lima",                    "PEN", "Peru"),
    # US sentinel (some upstream feeds attach .US to ADRs)
    ".US":    ("NASDAQ/NYSE",             "USD", "USA"),
}


def _identity_defaults_for_symbol(symbol: str) -> Dict[str, Optional[str]]:
    """
    Return identity defaults (exchange / currency / country / asset_class)
    derived purely from the symbol's structure (v8.2.0).

    Resolution order (first match wins):
      1. Empty / None             -> all None
      2. ``...=F`` futures        -> NYMEX / USD / USA / FUTURE
      3. ``...=X`` FX pairs       -> CCY / None / None / FX
      4. ``^...`` indices         -> INDEX / None / None / INDEX
      5. SSOT (normalize.py)      -> whatever it returns (when available)
      6. Longest-match suffix in  -> (exchange, currency, country) / EQUITY
         ``_SUFFIX_TO_LOCALE_DEFAULTS``
      7. Plain alpha, no dot      -> NASDAQ/NYSE / USD / USA / EQUITY
      8. Unknown                  -> all None (don't guess)

    Used by ``_infer_asset_class`` / ``_infer_exchange`` / ``_enrich_data``
    when Yahoo's ``info`` payload doesn't supply the relevant field.
    """
    if not symbol:
        return {"exchange": None, "currency": None, "country": None, "asset_class": None}

    s = symbol.strip().upper()

    # Special patterns (these don't appear in _SUFFIX_TO_LOCALE_DEFAULTS)
    if s.endswith("=F"):
        return {"exchange": "NYMEX", "currency": "USD", "country": "USA", "asset_class": "FUTURE"}
    if s.endswith("=X"):
        return {"exchange": "CCY", "currency": None, "country": None, "asset_class": "FX"}
    if s.startswith("^"):
        return {"exchange": "INDEX", "currency": None, "country": None, "asset_class": "INDEX"}

    # SSOT path (normalize.py v5.3.0+) when available
    if _infer_symbol_metadata_external is not None:
        try:
            meta = _infer_symbol_metadata_external(s)
            if isinstance(meta, dict):
                inferred_from = meta.get("inferred_from")
                if inferred_from not in (None, "", "none"):
                    return {
                        "exchange": meta.get("exchange"),
                        "currency": meta.get("currency"),
                        "country": meta.get("country"),
                        "asset_class": meta.get("asset_class") or "EQUITY",
                    }
        except Exception:
            pass

    # Local table fallback: longest matching suffix wins
    best_suffix: Optional[str] = None
    for suf in _SUFFIX_TO_LOCALE_DEFAULTS:
        if s.endswith(suf):
            if best_suffix is None or len(suf) > len(best_suffix):
                best_suffix = suf
    if best_suffix is not None:
        exch, curr, country = _SUFFIX_TO_LOCALE_DEFAULTS[best_suffix]
        return {
            "exchange": exch,
            "currency": curr,
            "country": country,
            "asset_class": "EQUITY",
        }

    # Plain alpha with no dot -> US equity by Yahoo convention
    # (futures/FX/indices already handled above; any remaining no-dot
    # symbol is a US ticker like AAPL, BRK-A, GOOG, etc.)
    if "." not in s:
        return {
            "exchange": "NASDAQ/NYSE",
            "currency": "USD",
            "country": "USA",
            "asset_class": "EQUITY",
        }

    # Unknown suffix -> don't guess
    return {"exchange": None, "currency": None, "country": None, "asset_class": None}


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
    """
    Infer asset class from symbol + info + metadata.

    Priority order (v8.2.0):
      1. Yahoo `info.quoteType` (or `meta.instrumentType`) when present.
      2. v8.2.0: fall through to `_identity_defaults_for_symbol` which
         covers 60+ stock-exchange suffixes (all return "EQUITY") plus
         the futures / FX / index special patterns.
    """
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

    # v8.2.0: suffix-table fallthrough (replaces v8.1.0's hand-coded
    # .SR / =F / =X / ^ checks; same patterns + 60 more)
    defaults = _identity_defaults_for_symbol(symbol)
    return defaults.get("asset_class")


def _infer_exchange(symbol: str, info: Any = None, meta: Optional[Dict[str, Any]] = None) -> Optional[str]:
    """
    Infer exchange from symbol + info + metadata.

    Priority order (v8.2.0):
      1. Yahoo `info.exchange` / `info.fullExchangeName` / `info.exchangeName`.
      2. Yahoo `meta.exchangeName` / `meta.exchangeTimezoneName`.
      3. v8.2.0: fall through to `_identity_defaults_for_symbol` which
         resolves 60+ stock-exchange suffixes to display names
         (e.g. .KW -> "Boursa Kuwait", .NS -> "NSE", .HK -> "HKEX").
    """
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

    # v8.2.0: suffix-table fallthrough (replaces v8.1.0's hand-coded
    # .SR / =F / =X / ^ checks)
    defaults = _identity_defaults_for_symbol(symbol)
    return defaults.get("exchange")


def _fetch_ticker_sync(
    symbol: str,
    period: str = DEFAULT_HISTORY_PERIOD,
    interval: str = DEFAULT_HISTORY_INTERVAL,
) -> Tuple[Dict[str, Any], Dict[str, Any], List[Dict[str, Any]]]:
    """
    Blocking fetch of ticker data. Runs in ThreadPoolExecutor.

    v8.0.0 bug fix: `period` and `interval` are now proper parameters.
    v7.0.0 hardcoded interval="1d" and had the nonsense expression
      period=_HAS_PANDAS and pd is not None and "YAHOO_HISTORY_PERIOD"
             in globals() or "2y"
    which always evaluated to "2y" regardless of config / env var.

    Returns:
        Tuple of (info_dict, history_metadata, history_list)
    """
    if not _HAS_YFINANCE or yf is None:
        return {}, {}, []

    try:
        ticker = yf.Ticker(symbol)

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
        except Exception:
            pass

        # v8.0.0: capture history_metadata *after* calling .history() (it's
        # populated lazily by yfinance). v7.0.0 passed None to the helper
        # and got {} back every time.
        meta = _safe_history_metadata(ticker)

        return info_dict, meta, history_list
    except Exception as exc:
        logger.warning("Sync fetch failed for %s: %s", symbol, exc)
        return {}, {}, []


def _enrich_data(
    symbol: str,
    info: Dict[str, Any],
    meta: Dict[str, Any],
    history: List[Dict[str, Any]],
) -> Dict[str, Any]:
    """Enrich raw Yahoo data into the canonical patch shape."""
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

    # 3. 52-week range
    week_52_high = _first_number(info.get("fiftyTwoWeekHigh"))
    week_52_low = _first_number(info.get("fiftyTwoWeekLow"))

    if not week_52_high and history:
        closes_252 = [h["close"] for h in history[-252:] if h.get("close")]
        if closes_252:
            week_52_high = max(closes_252)
            week_52_low = min(closes_252)

    week_52_position = None
    if price and week_52_high and week_52_low and week_52_high > week_52_low:
        week_52_position = (price - week_52_low) / (week_52_high - week_52_low)

    # 4. History-derived stats
    closes_list = [h["close"] for h in history if h.get("close")]
    volatility_30d = _volatility_nd(closes_list, 30)
    rsi_14 = _rsi_14(closes_list)
    max_drawdown_1y = _max_drawdown_1y(closes_list)
    sharpe_1y = _sharpe_1y(closes_list)
    var_95_1d = _var_95_1d(closes_list)

    # 5. Forecast
    forecast_price_12m, expected_roi_12m, forecast_confidence = _simple_forecast(closes_list, 252)

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

    # 8. Identity (v8.2.0: when Yahoo doesn't return exchange / currency /
    #    country, fall through to suffix-derived defaults so the downstream
    #    `enriched_quote.py` v4.6.0 normalization stage has nothing left to
    #    repair. Yahoo's values always win when present.)
    #    v8.0.0: `meta` is now the REAL ticker metadata, not an empty
    #    placeholder -- so asset class / exchange inference actually
    #    benefits from yfinance's history_metadata field.
    asset_class = _infer_asset_class(symbol, info, meta)
    exchange = _infer_exchange(symbol, info, meta)

    # Currency: prefer Yahoo's explicit field, fall back to suffix table
    currency = _first_str(info.get("currency"), info.get("financialCurrency"))

    # Country: NEW in v8.2.0 (was absent from the v8.1.0 patch shape).
    # Prefer Yahoo's `country` field, fall back to suffix table.
    country = _first_str(info.get("country"))

    # Single lookup, used for any field Yahoo left blank
    _identity_defaults = _identity_defaults_for_symbol(symbol)
    if currency is None and _identity_defaults.get("currency"):
        currency = _identity_defaults["currency"]
    if country is None and _identity_defaults.get("country"):
        country = _identity_defaults["country"]

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
        # Risk metrics
        "volatility_30d": volatility_30d,
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
        "country": country,
        "asset_class": asset_class,
        "exchange": exchange,
    }

    return clean_dict(result)


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

    async def get_enriched_quote(self, symbol: str) -> Optional[Dict[str, Any]]:
        """Get an enriched quote for a single symbol."""
        if not self.config.is_available or not symbol:
            return None

        sym = normalize_symbol(symbol)
        # v8.0.0: early-return on empty normalized symbol, avoiding a pointless cache lookup
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
                # v8.0.0: pass config-provided period/interval to the worker.
                info, meta, history = await loop.run_in_executor(
                    self._executor,
                    _fetch_ticker_sync,
                    sym,
                    self.config.history_period,
                    self.config.history_interval,
                )

                if not info and not history:
                    await self._circuit_breaker.record_failure()
                    metrics.requests_total.labels(symbol=sym, op="enriched", status="error").inc()
                    return None

                result = _enrich_data(sym, info, meta, history)

                await self._circuit_breaker.record_success()
                metrics.requests_total.labels(symbol=sym, op="enriched", status="ok").inc()
                metrics.request_duration.labels(symbol=sym, op="enriched").observe(
                    time.monotonic() - start_time
                )

                if result.get("current_price"):
                    await self._cache.set(sym, result, kind="enriched")

                return result
            except Exception as exc:
                await self._circuit_breaker.record_failure()
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

        v8.0.0: this runs on the provider's SHARED executor instead of
        allocating a fresh ThreadPoolExecutor per call (v7.0.0 leaked threads).
        """
        if not _HAS_YFINANCE or yf is None:
            return []

        sym = normalize_symbol(symbol)
        if not sym:
            return []

        def _sync_fetch() -> List[Dict[str, Any]]:
            try:
                ticker = yf.Ticker(sym)
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
