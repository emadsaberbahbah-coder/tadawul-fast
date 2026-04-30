#!/usr/bin/env python3
# core/providers/tadawul_provider.py
"""
================================================================================
Tadawul Provider (KSA Market Data) -- v6.1.0
================================================================================

Purpose
-------
Provides KSA market data from Tadawul-compatible APIs:
  - Real-time quotes for Saudi symbols
  - Company profiles and classification
  - Fundamental data and financial metrics
  - Historical OHLCV data with technical indicators
  - KSA-specific symbol normalization and defaults

v6.1.0 Changes (from v6.0.0)
----------------------------
The "satisfaction problem" fix: previously the provider's URL fields all
defaulted to empty strings, which silently disabled the entire KSA
primary-data path. Operators had no diagnostic signal that the provider
was unreachable -- KSA tickers just got Yahoo-only data and scored as
HOLD with reason "Insufficient data".

Operator-facing improvements:
  - Added PROVIDER_PRESETS map for three known Saudi data APIs
    (sahmk, twelvedata, stockerapi). Set
    `TADAWUL_PROVIDER_PRESET=sahmk` (or another preset name) and the
    quote/history/profile URLs are auto-filled with the right URL
    template for that provider.
  - Added `TADAWUL_API_KEY` env var. When set, the provider auto-injects
    an Authorization-style header (default `X-API-Key`, override via
    `TADAWUL_API_KEY_HEADER`). Custom header schemes (e.g. `Bearer ...`)
    can still be done via `TADAWUL_HEADERS_JSON`.
  - Explicit URL env vars (TADAWUL_QUOTE_URL, etc.) ALWAYS win over the
    preset. Presets only fill in URLs that aren't explicitly set, so
    you can mix-and-match.
  - When the provider is enabled but no URLs are configured AND no
    preset is set, a single WARNING is logged at construction time
    (idempotent; not on every quote call). Operators see a clear
    "TADAWUL_QUOTE_URL not set; KSA data unavailable" message in the
    Render logs at startup.
  - `is_configured` property now also returns True when a preset is
    set (the preset will fill the URL on first use).
  - New `provider_preset` field on TadawulConfig (frozen dataclass).
  - `to_dict()` reports preset state in `_health` / health-check JSON.

Bug-compat: the public API (functions, fetch methods, dataclass field
order, __all__, env-var names, return shapes) is unchanged. Adding new
optional fields to the frozen dataclass is safe because TadawulConfig
constructors use keyword-only assignment.

Provider preset templates (configurable via env if a vendor changes URLs):
  - sahmk      :  Saudi-licensed market data
                  https://app.sahmk.sa/api/v1/quote/{code}/
                  https://app.sahmk.sa/api/v1/history/{code}/?days={days}
                  https://app.sahmk.sa/api/v1/profile/{code}/
                  Auth: X-API-Key header (set TADAWUL_API_KEY)
  - twelvedata :  Saudi exchange (XSAU) via Twelve Data REST
                  https://api.twelvedata.com/quote?symbol={code}&exchange=XSAU&apikey={key}
                  https://api.twelvedata.com/time_series?symbol={code}&exchange=XSAU&interval=1day&outputsize={days}&apikey={key}
                  https://api.twelvedata.com/profile?symbol={code}&exchange=XSAU&apikey={key}
                  Auth: in-URL apikey (still honors TADAWUL_API_KEY for header schemes)
  - stockerapi :  Third-party Tadawul provider
                  https://stockerapi.com/api/v1/saudi/quote/{code}
                  https://stockerapi.com/api/v1/saudi/history/{code}?days={days}
                  https://stockerapi.com/api/v1/saudi/profile/{code}
                  Auth: Authorization Bearer (header name overridable)

Setting up KSA primary data on Render (one-time, ~5 minutes):
  1. Sign up for one of the providers above and obtain an API key.
  2. In Render Dashboard -> Environment, add:
       TADAWUL_PROVIDER_PRESET = sahmk           (or your chosen preset)
       TADAWUL_API_KEY         = <your key>
  3. Trigger a deploy (or click "Manual Deploy" to apply env vars).
  4. Hit GET /v1/enriched/sheet-rows?page=Market_Leaders or run a
     market_scan to verify Saudi rows now have pe_ttm, dividend_yield,
     and the other KSA fundamentals populated.

You can still set explicit URLs (the v6.0.0 way) -- they take precedence
over the preset. Use this when your vendor's URL format differs from the
preset template.

----------------------------
v6.0.0 Changes (from v5.0.0)
----------------------------
Bug fixes:
  - 404 responses no longer fall through to JSON parsing. v5.0.0 had
    `if status_code >= 400 and status_code != 404: return ...` which
    means a 404 with a JSON error body was parsed and returned as
    *successful* data. Now handled explicitly, matching EODHD/Finnhub.
  - `percent_change` is computed from current/previous FIRST, falling
    back to the API field only when prices aren't available. v5.0.0
    used `_pick_ratio` on the API field, which applied the `|v| > 1.5`
    heuristic -- so a 0.75% daily move (raw value 0.75) was kept as a
    ratio of 0.75 (i.e., 75%). The fallback in `_fill_derived_quote_fields`
    only fires when the field is None, so the poisoned value never got
    corrected. Same class of fix as Finnhub v6.
  - All asyncio primitives (Lock, Semaphore) lazy-initialized in async
    methods rather than constructors. Affected: TokenBucket._lock,
    SmartCache._lock, SingleFlight._lock, TadawulClient._semaphore,
    module _CLIENT_LOCK.

Cleanup:
  - Removed unused imports: `cast`, `numpy` (+ `_HAS_NUMPY` flag).
    numpy was imported with a try/except but never actually called.
  - Removed dead `self._closing` flag (set but never read).
  - Engine-facing functions log a debug line when extra args/kwargs are
    dropped (parity with other v6 providers).

Preserved for backward compatibility:
  - Every name in __all__.
  - All environment variable names and defaults.
  - Patch shape (including the lowercase DataQuality values
    "excellent"/"good"/"fair"/"poor"/"bad" that are unique to this
    provider -- other providers use "OK"/"MISSING"/"BLOCKED").
  - Provider adapter + singleton (`provider`, `get_provider`,
    `build_provider`).
  - MarketRegime enum (exported but unused internally; external code
    may import it).
  - Optional OpenTelemetry / Prometheus integration.
  - _map_fundamentals still uses the `_to_ratio` heuristic because
    Tadawul's data source is configurable via env URLs -- could be
    any aggregator with any scale convention. Conservative default.
================================================================================
"""

from __future__ import annotations

import asyncio
import functools
import json
import logging
import math
import os
import random
import re
import time
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, Sequence, Set, Tuple, Union

import httpx

# ---------------------------------------------------------------------------
# Optional Dependencies (prod-safe)
# ---------------------------------------------------------------------------

# JSON parser (orjson preferred)
try:
    import orjson  # type: ignore

    def _json_loads(data: Union[str, bytes]) -> Any:
        if isinstance(data, str):
            data = data.encode("utf-8")
        return orjson.loads(data)
except ImportError:
    def _json_loads(data: Union[str, bytes]) -> Any:
        if isinstance(data, bytes):
            data = data.decode("utf-8", errors="replace")
        return json.loads(data)

# OpenTelemetry (optional)
try:
    from opentelemetry import trace  # type: ignore
    from opentelemetry.trace import Status, StatusCode  # type: ignore
    _OTEL_AVAILABLE = True
except ImportError:
    trace = None  # type: ignore[assignment]
    Status = None  # type: ignore[assignment]
    StatusCode = None  # type: ignore[assignment]
    _OTEL_AVAILABLE = False

# Prometheus (optional)
try:
    from prometheus_client import Counter, Gauge  # type: ignore
    _PROM_AVAILABLE = True
except ImportError:
    Counter = None  # type: ignore[assignment]
    Gauge = None  # type: ignore[assignment]
    _PROM_AVAILABLE = False

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------

logger = logging.getLogger(__name__)
logger.addHandler(logging.NullHandler())

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

PROVIDER_NAME = "tadawul"
PROVIDER_VERSION = "6.1.0"
VERSION = PROVIDER_VERSION
PROVIDER_BATCH_SUPPORTED = True

# KSA defaults
KSA_COUNTRY_CODE = "SAU"
KSA_COUNTRY_NAME = "Saudi Arabia"
KSA_EXCHANGE_NAME = "Tadawul"
KSA_CURRENCY = "SAR"
KSA_TIMEZONE = timezone(timedelta(hours=3))

# Default configuration values
DEFAULT_TIMEOUT_SEC = 25.0
DEFAULT_RETRY_ATTEMPTS = 4
DEFAULT_MAX_CONCURRENCY = 30
DEFAULT_RATE_LIMIT = 15.0
DEFAULT_USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/124.0.0.0 Safari/537.36"
)

_TRUTHY = {"1", "true", "yes", "y", "on", "t", "enabled", "enable"}
_FALSY = {"0", "false", "no", "n", "off", "f", "disabled", "disable"}
_ARABIC_DIGITS = str.maketrans("٠١٢٣٤٥٦٧٨٩", "0123456789")
_KSA_CODE_RE = re.compile(r"^\d{3,6}$", re.IGNORECASE)
_K_M_B_RE = re.compile(r"^(-?\d+(?:\.\d+)?)([KMB])$", re.IGNORECASE)
_K_M_B_MULT = {"K": 1_000.0, "M": 1_000_000.0, "B": 1_000_000_000.0}


# ---------------------------------------------------------------------------
# Provider presets (v6.1.0)
# ---------------------------------------------------------------------------
# Preset names map to URL templates for known Saudi data providers. Operators
# set TADAWUL_PROVIDER_PRESET=<name> + TADAWUL_API_KEY=<key> and the provider
# auto-fills URLs and headers. Explicit TADAWUL_*_URL env vars still win.
#
# These templates use:
#   {symbol} -> the full normalized form, e.g. "2222.SR"
#   {code}   -> just the numeric code, e.g. "2222"
#   {days}   -> history days, filled by format_url() at runtime
#
# Templates are intentionally NOT URL-encoded; format_url handles symbol
# substitution. If a vendor changes URLs, override via the explicit
# TADAWUL_QUOTE_URL etc. env vars without editing this file.
# ---------------------------------------------------------------------------

PROVIDER_PRESETS: Dict[str, Dict[str, str]] = {
    # SAHMK: Saudi-licensed Tadawul market data provider (sahmk.sa).
    # Auth: X-API-Key header.
    "sahmk": {
        "quote_url":        "https://app.sahmk.sa/api/v1/quote/{code}/",
        "history_url":      "https://app.sahmk.sa/api/v1/history/{code}/?days={days}",
        "profile_url":      "https://app.sahmk.sa/api/v1/profile/{code}/",
        "fundamentals_url": "https://app.sahmk.sa/api/v1/fundamentals/{code}/",
        "api_key_header":   "X-API-Key",
    },
    # Twelve Data: covers Saudi exchange (XSAU) via standard REST.
    # Auth: in-URL apikey (no header needed).
    "twelvedata": {
        "quote_url":        "https://api.twelvedata.com/quote?symbol={code}&exchange=XSAU&apikey={api_key}",
        "history_url":      "https://api.twelvedata.com/time_series?symbol={code}&exchange=XSAU&interval=1day&outputsize={days}&apikey={api_key}",
        "profile_url":      "https://api.twelvedata.com/profile?symbol={code}&exchange=XSAU&apikey={api_key}",
        "fundamentals_url": "https://api.twelvedata.com/statistics?symbol={code}&exchange=XSAU&apikey={api_key}",
        "api_key_header":   "",  # not needed; key is in URL
    },
    # StockerAPI: third-party Tadawul provider.
    # Auth: Authorization Bearer header.
    "stockerapi": {
        "quote_url":        "https://stockerapi.com/api/v1/saudi/quote/{code}",
        "history_url":      "https://stockerapi.com/api/v1/saudi/history/{code}?days={days}",
        "profile_url":      "https://stockerapi.com/api/v1/saudi/profile/{code}",
        "fundamentals_url": "https://stockerapi.com/api/v1/saudi/fundamentals/{code}",
        "api_key_header":   "Authorization",  # value will be "Bearer <key>"
    },
}


def _resolve_preset(preset_name: str) -> Dict[str, str]:
    """Return the URL template dict for a preset name, or {} if unknown."""
    if not preset_name:
        return {}
    return dict(PROVIDER_PRESETS.get(preset_name.strip().lower(), {}) or {})


# ---------------------------------------------------------------------------
# Enums
# ---------------------------------------------------------------------------

class MarketRegime(str, Enum):
    """Market regime classification (exported for external consumers)."""
    BULL = "bull"
    BEAR = "bear"
    VOLATILE = "volatile"
    SIDEWAYS = "sideways"
    UNKNOWN = "unknown"


class DataQuality(str, Enum):
    """Data quality levels (Tadawul-specific; other providers use different scales)."""
    EXCELLENT = "excellent"
    GOOD = "good"
    FAIR = "fair"
    POOR = "poor"
    BAD = "bad"


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class TadawulConfig:
    """Immutable configuration for Tadawul provider."""

    enabled: bool = True
    quote_url: str = ""
    fundamentals_url: str = ""
    history_url: str = ""
    profile_url: str = ""
    # v6.1.0: preset-driven URL filling
    provider_preset: str = ""
    api_key: str = ""
    api_key_header: str = ""
    timeout_sec: float = DEFAULT_TIMEOUT_SEC
    retry_attempts: int = DEFAULT_RETRY_ATTEMPTS
    max_concurrency: int = DEFAULT_MAX_CONCURRENCY
    rate_limit: float = DEFAULT_RATE_LIMIT
    user_agent: str = DEFAULT_USER_AGENT
    extra_headers: Dict[str, str] = field(default_factory=dict)
    verbose_warnings: bool = False
    enable_fundamentals: bool = True
    enable_history: bool = True
    enable_profile: bool = True
    enable_forecast: bool = True
    history_days: int = 500
    history_points_max: int = 1000
    quote_ttl_sec: float = 15.0
    fund_ttl_sec: float = 21600.0
    hist_ttl_sec: float = 1800.0
    profile_ttl_sec: float = 43200.0
    error_ttl_sec: float = 10.0
    tracing_enabled: bool = False
    metrics_enabled: bool = False

    @classmethod
    def from_env(cls) -> "TadawulConfig":
        """Load configuration from environment variables."""

        def _env_str(name: str, default: str = "") -> str:
            v = os.getenv(name)
            return str(v).strip() if v is not None and str(v).strip() else default

        def _env_int(name: str, default: int) -> int:
            v = os.getenv(name)
            try:
                return int(str(v).strip()) if v is not None else default
            except Exception:
                return default

        def _env_float(name: str, default: float) -> float:
            v = os.getenv(name)
            try:
                return float(str(v).strip()) if v is not None else default
            except Exception:
                return default

        def _env_bool(name: str, default: bool) -> bool:
            raw = (os.getenv(name) or "").strip().lower()
            if not raw:
                return default
            if raw in _TRUTHY:
                return True
            if raw in _FALSY:
                return False
            return default

        def _extra_headers() -> Dict[str, str]:
            raw = _env_str("TADAWUL_HEADERS_JSON", "")
            if not raw:
                return {}
            try:
                obj = json.loads(raw)
                if isinstance(obj, dict):
                    return {
                        str(k).strip(): str(v).strip()
                        for k, v in obj.items()
                        if str(k).strip() and str(v).strip()
                    }
            except Exception:
                pass
            return {}

        # v6.1.0: preset support
        preset_name = _env_str("TADAWUL_PROVIDER_PRESET", "")
        api_key = _env_str("TADAWUL_API_KEY", "")
        api_key_header_explicit = _env_str("TADAWUL_API_KEY_HEADER", "")
        preset = _resolve_preset(preset_name)

        # Explicit env URLs always win; preset only fills the gaps.
        # Templates get {api_key} substituted now (e.g., Twelve Data uses
        # in-URL keys); {symbol}/{code}/{days} are filled at request time.
        def _from_preset(field_name: str, env_name: str, alt_env: str = "") -> str:
            explicit = _env_str(env_name, "") or (_env_str(alt_env, "") if alt_env else "")
            if explicit:
                return explicit
            tmpl = preset.get(field_name, "")
            if tmpl and "{api_key}" in tmpl:
                tmpl = tmpl.replace("{api_key}", api_key)
            return tmpl

        # Default API key header. Resolution order:
        #   1. Explicit TADAWUL_API_KEY_HEADER env var (operator override)
        #   2. Preset's recommendation (including the explicit empty string,
        #      which means "no header needed; key goes in URL" -- e.g.,
        #      twelvedata)
        #   3. Fall back to "X-API-Key" only if no preset is set and an
        #      api_key was provided (operator forgot to set a preset but
        #      gave a key)
        if api_key_header_explicit:
            api_key_header = api_key_header_explicit
        elif "api_key_header" in preset:
            # Preset explicitly declared a header policy (may be "")
            api_key_header = preset["api_key_header"]
        elif api_key:
            api_key_header = "X-API-Key"
        else:
            api_key_header = ""

        # Construct headers (preset + extra_headers + api_key auto-injection
        # in TadawulClient.__init__; we don't merge here so users can still
        # override via TADAWUL_HEADERS_JSON).

        cfg = cls(
            enabled=_env_bool("TADAWUL_ENABLED", True),
            quote_url=_from_preset("quote_url", "TADAWUL_QUOTE_URL"),
            fundamentals_url=_from_preset("fundamentals_url", "TADAWUL_FUNDAMENTALS_URL"),
            history_url=_from_preset("history_url", "TADAWUL_HISTORY_URL", "TADAWUL_CANDLES_URL"),
            profile_url=_from_preset("profile_url", "TADAWUL_PROFILE_URL"),
            provider_preset=preset_name.strip().lower(),
            api_key=api_key,
            api_key_header=api_key_header,
            timeout_sec=max(5.0, _env_float("TADAWUL_TIMEOUT_SEC", DEFAULT_TIMEOUT_SEC)),
            retry_attempts=max(1, _env_int("TADAWUL_RETRY_ATTEMPTS", DEFAULT_RETRY_ATTEMPTS)),
            max_concurrency=max(2, _env_int("TADAWUL_MAX_CONCURRENCY", DEFAULT_MAX_CONCURRENCY)),
            rate_limit=max(0.0, _env_float("TADAWUL_RATE_LIMIT", DEFAULT_RATE_LIMIT)),
            user_agent=_env_str("TADAWUL_USER_AGENT", DEFAULT_USER_AGENT),
            extra_headers=_extra_headers(),
            verbose_warnings=_env_bool("TADAWUL_VERBOSE_WARNINGS", False),
            enable_fundamentals=_env_bool("TADAWUL_ENABLE_FUNDAMENTALS", True),
            enable_history=_env_bool("TADAWUL_ENABLE_HISTORY", True),
            enable_profile=_env_bool("TADAWUL_ENABLE_PROFILE", True),
            enable_forecast=_env_bool("TADAWUL_ENABLE_FORECAST", True),
            history_days=max(60, _env_int("TADAWUL_HISTORY_DAYS", 500)),
            history_points_max=max(100, _env_int("TADAWUL_HISTORY_POINTS_MAX", 1000)),
            quote_ttl_sec=max(5.0, _env_float("TADAWUL_QUOTE_TTL_SEC", 15.0)),
            fund_ttl_sec=max(300.0, _env_float("TADAWUL_FUNDAMENTALS_TTL_SEC", 21600.0)),
            hist_ttl_sec=max(300.0, _env_float("TADAWUL_HISTORY_TTL_SEC", 1800.0)),
            profile_ttl_sec=max(3600.0, _env_float("TADAWUL_PROFILE_TTL_SEC", 43200.0)),
            error_ttl_sec=max(5.0, _env_float("TADAWUL_ERROR_TTL_SEC", 10.0)),
            tracing_enabled=_env_bool("TADAWUL_TRACING_ENABLED", False),
            metrics_enabled=_env_bool("TADAWUL_METRICS_ENABLED", False),
        )

        # Startup-time configuration warning. Logged ONCE per process at
        # config load. This is the diagnostic the v6.0.0 silent-empty-URL
        # fix needed: operators see a clear message in Render logs telling
        # them KSA primary data is dead and how to fix it.
        if cfg.enabled and not cfg.quote_url:
            try:
                logger.warning(
                    "Tadawul provider ENABLED but NO quote URL configured. "
                    "All KSA primary-data calls will be skipped. "
                    "Fix: set TADAWUL_PROVIDER_PRESET (sahmk|twelvedata|stockerapi) "
                    "+ TADAWUL_API_KEY in Render env, OR set TADAWUL_QUOTE_URL "
                    "explicitly. Without this, .SR tickers fall back to Yahoo "
                    "only and score as HOLD/Insufficient data."
                )
            except Exception:
                pass

        return cfg

    @property
    def is_configured(self) -> bool:
        """Return True if the provider has at least the quote URL configured."""
        return self.enabled and bool(self.quote_url)


# ---------------------------------------------------------------------------
# Custom Exceptions
# ---------------------------------------------------------------------------

class TadawulProviderError(Exception):
    """Base exception for Tadawul provider errors."""


class TadawulConfigurationError(TadawulProviderError):
    """Raised when provider is not properly configured."""


class TadawulFetchError(TadawulProviderError):
    """Raised when a data fetch fails."""


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
    d = dt or datetime.now(KSA_TIMEZONE)
    if d.tzinfo is None:
        d = d.replace(tzinfo=KSA_TIMEZONE)
    return d.astimezone(KSA_TIMEZONE).isoformat()


def _to_riyadh_iso(dt: Optional[datetime]) -> Optional[str]:
    """Convert datetime to Riyadh ISO format; returns None for None input."""
    if not dt:
        return None
    d = dt
    if d.tzinfo is None:
        d = d.replace(tzinfo=timezone.utc)
    return d.astimezone(KSA_TIMEZONE).isoformat()


def _safe_float(value: Any) -> Optional[float]:
    """
    Safely convert value to float with Arabic digit support.

    Handles Arabic digits (٠-٩), Arabic separators (٬ ٫), currency labels
    (SAR, ريال), percentage signs, K/M/B suffixes, and parentheses for
    negatives.
    """
    if value is None:
        return None

    try:
        if isinstance(value, (int, float)) and not isinstance(value, bool):
            f = float(value)
            return None if (math.isnan(f) or math.isinf(f)) else f

        s = str(value).strip()
        if not s or s.lower() in {"-", "—", "n/a", "na", "null", "none"}:
            return None

        s = s.translate(_ARABIC_DIGITS)
        s = s.replace("٬", ",").replace("٫", ".").replace("−", "-")
        s = s.replace("SAR", "").replace("ريال", "")
        s = s.replace("%", "").replace(",", "").replace("+", "").strip()

        if s.startswith("(") and s.endswith(")"):
            s = "-" + s[1:-1].strip()

        match = _K_M_B_RE.match(s)
        if match:
            f = float(match.group(1)) * _K_M_B_MULT[match.group(2).upper()]
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
    if they were meant as percents). Kept for use on Tadawul fundamentals
    fields where the source scale is unknown (URLs are env-configurable).
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


def _ensure_ksa_identity(patch: Dict[str, Any], symbol: str) -> None:
    """Ensure KSA default identity fields are set."""
    patch.setdefault("country", KSA_COUNTRY_CODE)
    patch.setdefault("currency", KSA_CURRENCY)
    patch.setdefault("exchange", KSA_EXCHANGE_NAME)
    patch.setdefault("asset_class", "EQUITY")
    if not patch.get("name"):
        patch["name"] = symbol


def _fill_derived_quote_fields(patch: Dict[str, Any]) -> None:
    """Fill derived quote fields from existing data (idempotent)."""
    current = _safe_float(patch.get("current_price"))
    previous = _safe_float(patch.get("previous_close"))
    week_52_high = _safe_float(patch.get("week_52_high"))
    week_52_low = _safe_float(patch.get("week_52_low"))

    if patch.get("price_change") is None and current is not None and previous is not None:
        patch["price_change"] = current - previous

    if (patch.get("percent_change") is None
            and current is not None and previous is not None and previous != 0):
        patch["percent_change"] = (current - previous) / previous

    if (patch.get("week_52_position_pct") is None
            and current is not None and week_52_high is not None and week_52_low is not None
            and week_52_high != week_52_low):
        patch["week_52_position_pct"] = (current - week_52_low) / (week_52_high - week_52_low)

    # Aliases
    if patch.get("price") is None and patch.get("current_price") is not None:
        patch["price"] = patch["current_price"]
    if patch.get("prev_close") is None and patch.get("previous_close") is not None:
        patch["prev_close"] = patch["previous_close"]
    if patch.get("change") is None and patch.get("price_change") is not None:
        patch["change"] = patch["price_change"]
    if patch.get("change_pct") is None and patch.get("percent_change") is not None:
        patch["change_pct"] = patch["percent_change"]


def _data_quality_score(patch: Dict[str, Any]) -> Tuple[DataQuality, float]:
    """Calculate data quality score based on field completeness."""
    score = 100.0
    if _safe_float(patch.get("current_price")) is None:
        score -= 30
    if _safe_float(patch.get("previous_close")) is None:
        score -= 10
    if _safe_float(patch.get("volume")) is None:
        score -= 8
    if _safe_str(patch.get("name")) is None:
        score -= 10
    if _safe_str(patch.get("sector")) is None:
        score -= 8
    if _safe_str(patch.get("industry")) is None:
        score -= 8
    if _safe_str(patch.get("country")) is None:
        score -= 8

    score = max(0.0, min(100.0, score))
    if score >= 80:
        return DataQuality.EXCELLENT, score
    if score >= 60:
        return DataQuality.GOOD, score
    if score >= 40:
        return DataQuality.FAIR, score
    if score >= 20:
        return DataQuality.POOR, score
    return DataQuality.BAD, score


# ---------------------------------------------------------------------------
# Symbol Normalization (Strict KSA)
# ---------------------------------------------------------------------------

def normalize_ksa_symbol(symbol: str) -> str:
    """
    Normalize KSA symbol to canonical format (e.g., "2222.SR").

    Examples:
        "2222"         -> "2222.SR"
        "2222.SR"      -> "2222.SR"
        "TADAWUL:2222" -> "2222.SR"
        "٢٢٢٢"         -> "2222.SR"
    """
    raw = (symbol or "").strip()
    if not raw:
        return ""

    raw = raw.translate(_ARABIC_DIGITS).strip().upper()

    try:
        from core.symbols.normalize import normalize_ksa_symbol as _norm  # type: ignore

        if callable(_norm):
            result = _norm(raw)
            if result:
                return result
    except ImportError:
        pass

    if raw.startswith("TADAWUL:"):
        raw = raw.split(":", 1)[1].strip()

    if raw.endswith(".SR"):
        code = raw[:-3].strip()
        return f"{code}.SR" if _KSA_CODE_RE.match(code) else ""

    if _KSA_CODE_RE.match(raw):
        return f"{raw}.SR"

    return ""


def format_url(template: str, symbol: str, days: Optional[int] = None) -> str:
    """Format URL template with {symbol}, {code}, and optional {days} placeholders."""
    sym = normalize_ksa_symbol(symbol)
    if not sym:
        return template
    code = sym[:-3] if sym.endswith(".SR") else sym
    url = template.replace("{symbol}", sym).replace("{code}", code)
    if days is not None:
        url = url.replace("{days}", str(days))
    return url


# ---------------------------------------------------------------------------
# History Analytics (pure Python)
# ---------------------------------------------------------------------------

def _rsi_14(closes: List[float]) -> Optional[float]:
    """RSI(14) using Wilder's smoothing; 0-100 or None."""
    if len(closes) < 15:
        return None

    period = 14
    gains: List[float] = []
    losses: List[float] = []

    for i in range(1, len(closes)):
        delta = closes[i] - closes[i - 1]
        gains.append(delta if delta > 0 else 0.0)
        losses.append(-delta if delta < 0 else 0.0)

    avg_gain = sum(gains[:period]) / period
    avg_loss = sum(losses[:period]) / period

    for i in range(period, len(gains)):
        avg_gain = (avg_gain * (period - 1) + gains[i]) / period
        avg_loss = (avg_loss * (period - 1) + losses[i]) / period

    if avg_loss == 0:
        return 100.0

    rs = avg_gain / avg_loss
    return 100.0 - (100.0 / (1.0 + rs))


def _volatility_30d(closes: List[float]) -> Optional[float]:
    """Annualized volatility from 30-day log returns."""
    if len(closes) < 31:
        return None

    window = closes[-31:]
    returns: List[float] = []

    for i in range(1, len(window)):
        prev, curr = window[i - 1], window[i]
        if prev > 0 and curr > 0:
            returns.append(math.log(curr / prev))

    if len(returns) < 2:
        return None

    mean = sum(returns) / len(returns)
    variance = sum((r - mean) ** 2 for r in returns) / (len(returns) - 1)
    std = math.sqrt(max(0.0, variance))
    return std * math.sqrt(252.0)


def _avg_last(values: List[float], n: int) -> Optional[float]:
    """Average of the last n values; None if empty."""
    if not values:
        return None
    window = values[-n:] if len(values) >= n else values
    valid = [v for v in window if v is not None]
    return sum(valid) / len(valid) if valid else None


def _compute_history_analytics(rows: List[Dict[str, Any]]) -> Dict[str, Any]:
    """Compute analytics (prices, 52w range, volumes, RSI, volatility) from history rows."""
    if not rows or len(rows) < 2:
        return {}

    closes = [_safe_float(r.get("close")) for r in rows]
    highs = [_safe_float(r.get("high")) for r in rows]
    lows = [_safe_float(r.get("low")) for r in rows]
    volumes = [_safe_float(r.get("volume")) for r in rows]

    closes_f = [x for x in closes if x is not None]
    highs_f = [x for x in highs if x is not None]
    lows_f = [x for x in lows if x is not None]
    vols_f = [x for x in volumes if x is not None]

    if len(closes_f) < 2:
        return {}

    result: Dict[str, Any] = {}

    # Quote fallbacks from latest bars
    latest = rows[-1]
    prev = rows[-2] if len(rows) >= 2 else None

    if _safe_float(latest.get("close")) is not None:
        result["current_price"] = _safe_float(latest.get("close"))
    if prev and _safe_float(prev.get("close")) is not None:
        result["previous_close"] = _safe_float(prev.get("close"))
    if _safe_float(latest.get("open")) is not None:
        result["open_price"] = _safe_float(latest.get("open"))
    if _safe_float(latest.get("high")) is not None:
        result["day_high"] = _safe_float(latest.get("high"))
    if _safe_float(latest.get("low")) is not None:
        result["day_low"] = _safe_float(latest.get("low"))
    if _safe_float(latest.get("volume")) is not None:
        result["volume"] = _safe_float(latest.get("volume"))

    # 52-week range
    last_252_highs = highs_f[-252:] if highs_f else []
    last_252_lows = lows_f[-252:] if lows_f else []

    if last_252_highs:
        result["week_52_high"] = max(last_252_highs)
    elif closes_f:
        result["week_52_high"] = max(closes_f[-252:])

    if last_252_lows:
        result["week_52_low"] = min(last_252_lows)
    elif closes_f:
        result["week_52_low"] = min(closes_f[-252:])

    # Volume averages
    avg10 = _avg_last(vols_f, 10)
    avg30 = _avg_last(vols_f, 30)
    if avg10 is not None:
        result["avg_volume_10d"] = avg10
    if avg30 is not None:
        result["avg_volume_30d"] = avg30

    rsi = _rsi_14(closes_f)
    if rsi is not None:
        result["rsi_14"] = rsi

    vol30 = _volatility_30d(closes_f)
    if vol30 is not None:
        result["volatility_30d"] = vol30

    return _clean_patch(result)


# ---------------------------------------------------------------------------
# Async Primitives: TokenBucket, SmartCache, SingleFlight (lazy locks)
# ---------------------------------------------------------------------------

class TokenBucket:
    """Token bucket rate limiter."""

    def __init__(self, rate_per_sec: float, capacity: Optional[float] = None):
        self.rate = max(0.0, rate_per_sec)
        self.capacity = capacity if capacity is not None else max(1.0, self.rate)
        self.tokens = self.capacity
        self.last = time.monotonic()
        self._lock: Optional[asyncio.Lock] = None  # lazy

    def _get_lock(self) -> asyncio.Lock:
        if self._lock is None:
            self._lock = asyncio.Lock()
        return self._lock

    async def wait_and_acquire(self, tokens: float = 1.0) -> None:
        """Wait until `tokens` tokens are available, then deduct them."""
        if self.rate <= 0:
            return

        while True:
            async with self._get_lock():
                now = time.monotonic()
                elapsed = max(0.0, now - self.last)
                self.tokens = min(self.capacity, self.tokens + elapsed * self.rate)
                self.last = now

                if self.tokens >= tokens:
                    self.tokens -= tokens
                    return

                wait = (tokens - self.tokens) / self.rate if self.rate > 0 else 0.1
            await asyncio.sleep(min(1.0, max(0.05, wait)))


class SmartCache:
    """Async-safe TTL cache with LRU-on-access eviction."""

    def __init__(self, max_size: int = 5000, ttl_sec: float = 300.0):
        self.max_size = max_size
        self.ttl_sec = ttl_sec
        self._cache: Dict[str, Any] = {}
        self._expires: Dict[str, float] = {}
        self._access_times: Dict[str, float] = {}
        self._lock: Optional[asyncio.Lock] = None  # lazy

    def _get_lock(self) -> asyncio.Lock:
        if self._lock is None:
            self._lock = asyncio.Lock()
        return self._lock

    async def get(self, key: str) -> Optional[Any]:
        """Get value from cache if not expired; refreshes access time."""
        async with self._get_lock():
            now = time.monotonic()
            if key in self._cache:
                if now < self._expires.get(key, 0):
                    self._access_times[key] = now
                    return self._cache[key]
                self._cache.pop(key, None)
                self._expires.pop(key, None)
                self._access_times.pop(key, None)
            return None

    async def set(self, key: str, value: Any, ttl_sec: Optional[float] = None) -> None:
        """Set value in cache with TTL; LRU-evicts least-recently-accessed on overflow."""
        async with self._get_lock():
            if len(self._cache) >= self.max_size and key not in self._cache:
                if self._access_times:
                    oldest = min(self._access_times.items(), key=lambda x: x[1])[0]
                    self._cache.pop(oldest, None)
                    self._expires.pop(oldest, None)
                    self._access_times.pop(oldest, None)

            now = time.monotonic()
            self._cache[key] = value
            self._expires[key] = now + (ttl_sec or self.ttl_sec)
            self._access_times[key] = now

    async def size(self) -> int:
        """Get current cache size."""
        async with self._get_lock():
            return len(self._cache)


class SingleFlight:
    """Deduplicate concurrent requests for the same key."""

    def __init__(self) -> None:
        self._lock: Optional[asyncio.Lock] = None  # lazy
        self._futures: Dict[str, asyncio.Future] = {}

    def _get_lock(self) -> asyncio.Lock:
        if self._lock is None:
            self._lock = asyncio.Lock()
        return self._lock

    async def run(self, key: str, coro_fn: Callable[[], Any]) -> Any:
        """Execute coroutine; concurrent callers for the same key share the result."""
        lock = self._get_lock()
        async with lock:
            future = self._futures.get(key)
            if future is not None:
                return await future

            loop = asyncio.get_running_loop()
            future = loop.create_future()
            self._futures[key] = future

        try:
            result = await coro_fn()
            if not future.done():
                future.set_result(result)
            return result
        except Exception as exc:
            if not future.done():
                future.set_exception(exc)
            raise
        finally:
            async with lock:
                self._futures.pop(key, None)


# ---------------------------------------------------------------------------
# Tracing and Metrics Decorators
# ---------------------------------------------------------------------------

def _trace(name: Optional[str] = None):
    """OpenTelemetry tracing decorator (no-op when OTEL unavailable)."""
    def decorator(func):
        @functools.wraps(func)
        async def wrapper(*args, **kwargs):
            if not (_OTEL_AVAILABLE and trace):
                return await func(*args, **kwargs)

            tracer = trace.get_tracer(__name__)
            span_name = name or func.__name__
            with tracer.start_as_current_span(span_name) as span:
                try:
                    return await func(*args, **kwargs)
                except Exception as exc:
                    if span and Status and StatusCode:
                        span.set_status(Status(StatusCode.ERROR, str(exc)))
                    raise
        return wrapper
    return decorator


# Prometheus metrics
if _PROM_AVAILABLE and Counter and Gauge:
    _tadawul_requests_total = Counter(
        "tadawul_requests_total", "Tadawul provider requests", ["status"],
    )
    _tadawul_queue_size = Gauge("tadawul_queue_size", "Tadawul request queue size")
else:
    class _DummyMetric:
        def labels(self, *args, **kwargs):
            return self

        def inc(self, *args, **kwargs):
            pass

        def set(self, *args, **kwargs):
            pass

    _tadawul_requests_total = _DummyMetric()
    _tadawul_queue_size = _DummyMetric()


# ---------------------------------------------------------------------------
# Tadawul HTTP Client
# ---------------------------------------------------------------------------

class TadawulClient:
    """Async client for Tadawul API."""

    def __init__(self, config: Optional[TadawulConfig] = None):
        self.config = config or TadawulConfig.from_env()
        self._semaphore: Optional[asyncio.Semaphore] = None  # lazy
        self._rate_limiter = TokenBucket(self.config.rate_limit)
        self._single_flight = SingleFlight()

        self._quote_cache = SmartCache(max_size=7000, ttl_sec=self.config.quote_ttl_sec)
        self._fund_cache = SmartCache(max_size=4000, ttl_sec=self.config.fund_ttl_sec)
        self._history_cache = SmartCache(max_size=2500, ttl_sec=self.config.hist_ttl_sec)
        self._profile_cache = SmartCache(max_size=4000, ttl_sec=self.config.profile_ttl_sec)
        self._error_cache = SmartCache(max_size=4000, ttl_sec=self.config.error_ttl_sec)

        headers = {
            "User-Agent": self.config.user_agent,
            "Accept": "application/json",
            "Accept-Language": "ar,en;q=0.9",
            **self.config.extra_headers,
        }

        # v6.1.0: auto-inject API key header from TADAWUL_API_KEY env var.
        # Skipped when api_key_header is empty (e.g., Twelve Data preset
        # uses {api_key} in URL instead). Skipped when extra_headers
        # already declares the header (operator override wins).
        if self.config.api_key and self.config.api_key_header:
            hk = self.config.api_key_header
            if hk not in headers:
                if hk.lower() == "authorization" and not self.config.api_key.lower().startswith(("bearer ", "token ", "basic ")):
                    headers[hk] = f"Bearer {self.config.api_key}"
                else:
                    headers[hk] = self.config.api_key

        self._client = httpx.AsyncClient(
            timeout=httpx.Timeout(self.config.timeout_sec),
            follow_redirects=True,
            headers=headers,
            limits=httpx.Limits(max_keepalive_connections=20, max_connections=50),
            http2=True,
        )

        logger.info(
            "TadawulClient v%s initialized | configured=%s | preset=%s | has_api_key=%s | has_profile=%s | has_history=%s",
            PROVIDER_VERSION,
            self.config.is_configured,
            self.config.provider_preset or "(none)",
            bool(self.config.api_key),
            bool(self.config.profile_url),
            bool(self.config.history_url),
        )

    def _get_semaphore(self) -> asyncio.Semaphore:
        if self._semaphore is None:
            self._semaphore = asyncio.Semaphore(self.config.max_concurrency)
        return self._semaphore

    # -- HTTP layer -----------------------------------------------------

    @_trace("tadawul_request")
    async def _request(self, url: str) -> Tuple[Optional[Union[dict, list]], Optional[str]]:
        """Make HTTP request to Tadawul API."""
        await self._rate_limiter.wait_and_acquire()

        async with self._get_semaphore():
            last_error: Optional[str] = None

            for attempt in range(self.config.retry_attempts):
                try:
                    response = await self._client.get(url)
                    status_code = response.status_code

                    if status_code == 429:
                        retry_after = _safe_float(response.headers.get("Retry-After")) or 3.0
                        await asyncio.sleep(min(30.0, max(1.0, retry_after)))
                        last_error = "http_429"
                        continue

                    if 500 <= status_code < 600:
                        base = 2 ** attempt
                        await asyncio.sleep(min(8.0, base + random.uniform(0, base)))
                        last_error = f"http_{status_code}"
                        continue

                    # v6.0.0 bug fix: 404 is now returned as an error immediately
                    # rather than falling through to JSON parsing. v5.0.0 had
                    # `if status_code >= 400 and status_code != 404: return ...`
                    # which meant a 404 with a JSON error body was parsed and
                    # returned as *successful* data. Matches EODHD/Finnhub.
                    if status_code == 404:
                        _tadawul_requests_total.labels(status="error").inc()
                        return None, "HTTP 404 not_found"

                    if status_code >= 400:
                        _tadawul_requests_total.labels(status="error").inc()
                        return None, f"HTTP {status_code}"

                    try:
                        data = _json_loads(response.content)
                        data = self._unwrap_envelope(data)
                    except Exception:
                        _tadawul_requests_total.labels(status="error").inc()
                        return None, "invalid_json"

                    if not isinstance(data, (dict, list)):
                        _tadawul_requests_total.labels(status="error").inc()
                        return None, "unexpected_response_type"

                    _tadawul_requests_total.labels(status="success").inc()
                    return data, None

                except httpx.RequestError as exc:
                    last_error = f"network_error_{exc.__class__.__name__}"
                    base = 2 ** attempt
                    await asyncio.sleep(min(8.0, base + random.uniform(0, base)))

            _tadawul_requests_total.labels(status="error").inc()
            return None, last_error or "max_retries_exceeded"

    def _unwrap_envelope(self, data: Any) -> Any:
        """Unwrap common API envelope structures."""
        current = data
        for _ in range(5):
            if not isinstance(current, dict):
                break
            for key in ("data", "result", "payload", "quote", "profile", "response",
                        "content", "results", "items"):
                if key in current and isinstance(current[key], (dict, list)):
                    current = current[key]
                    break
            else:
                break
        return current

    def _coerce_dict(self, data: Any) -> Dict[str, Any]:
        """Coerce data to a dict; handles list-of-dicts by taking the first element."""
        if isinstance(data, dict):
            return data
        if isinstance(data, list) and data and isinstance(data[0], dict):
            return data[0]
        return {}

    def _find_value(self, obj: Any, keys: Sequence[str], max_depth: int = 8) -> Any:
        """BFS through nested structure for the first value matching any of `keys`."""
        if obj is None:
            return None

        keys_lower = {str(k).strip().lower() for k in keys if k}
        if not keys_lower:
            return None

        queue: List[Tuple[Any, int]] = [(obj, 0)]
        seen: Set[int] = set()

        while queue:
            current, depth = queue.pop(0)
            if current is None or depth > max_depth:
                continue

            current_id = id(current)
            if current_id in seen:
                continue
            seen.add(current_id)

            if isinstance(current, dict):
                for k, v in current.items():
                    if str(k).strip().lower() in keys_lower:
                        return v
                    queue.append((v, depth + 1))
            elif isinstance(current, list):
                for item in current:
                    queue.append((item, depth + 1))

        return None

    def _pick_num(self, obj: Any, *keys: str) -> Optional[float]:
        """Pick numeric value by keys."""
        return _safe_float(self._find_value(obj, keys))

    def _pick_str(self, obj: Any, *keys: str) -> Optional[str]:
        """Pick string value by keys."""
        return _safe_str(self._find_value(obj, keys))

    def _pick_ratio(self, obj: Any, *keys: str) -> Optional[float]:
        """Pick percent-ish value and convert to ratio via the abs>1.5 heuristic."""
        return _to_ratio(self._find_value(obj, keys))

    # -- Mapping methods ------------------------------------------------

    def _map_identity(self, root: Any) -> Dict[str, Any]:
        """Map identity fields (name/sector/industry/exchange/currency) from response."""
        patch: Dict[str, Any] = {}
        patch["name"] = self._pick_str(
            root,
            "name", "company_name", "companyName", "securityName", "securityNameEn",
            "securityNameAr", "instrumentName", "symbolName", "shortName", "longName",
        )
        patch["sector"] = self._pick_str(
            root,
            "sector", "sectorName", "sector_name", "sectorEn", "sectorAr",
            "gicsSector", "sector_description", "classificationSector", "sectorDesc",
        )
        patch["industry"] = self._pick_str(
            root,
            "industry", "industryName", "industry_name", "industryEn", "industryAr",
            "gicsIndustry", "industry_description", "subSectorName", "sub_sector",
            "subSector", "classificationIndustry", "industryDesc",
        )
        patch["exchange"] = self._pick_str(root, "exchange", "exchangeName", "market", "marketName", "mic")
        patch["currency"] = self._pick_str(root, "currency", "ccy", "tradingCurrency")
        return _clean_patch(patch)

    def _map_quote(self, root: Any) -> Dict[str, Any]:
        """Map quote fields from response."""
        patch = self._map_identity(root)

        patch["current_price"] = self._pick_num(
            root,
            "last", "last_price", "lastPrice", "price", "close", "c", "tradingPrice",
            "regularMarketPrice", "tradePrice", "ltp", "lastTradePrice", "value",
        )
        patch["previous_close"] = self._pick_num(
            root,
            "previous_close", "prev_close", "pc", "PreviousClose", "prevClose",
            "regularMarketPreviousClose", "yesterdayClose", "closePrev",
        )
        patch["open_price"] = self._pick_num(
            root,
            "open", "open_price", "regularMarketOpen", "o", "Open", "openPrice", "sessionOpen",
        )
        patch["day_high"] = self._pick_num(
            root, "high", "day_high", "h", "High", "dayHigh", "sessionHigh", "highPrice",
        )
        patch["day_low"] = self._pick_num(
            root, "low", "day_low", "l", "Low", "dayLow", "sessionLow", "lowPrice",
        )
        patch["volume"] = self._pick_num(
            root, "volume", "v", "Volume", "tradedVolume", "qty", "quantity",
            "totalVolume", "tradeVolume",
        )
        patch["market_cap"] = self._pick_num(
            root, "market_cap", "marketCap", "marketCapitalization", "MarketCap",
        )
        patch["week_52_high"] = self._pick_num(
            root, "week_52_high", "fiftyTwoWeekHigh", "52w_high", "yearHigh", "week52High",
        )
        patch["week_52_low"] = self._pick_num(
            root, "week_52_low", "fiftyTwoWeekLow", "52w_low", "yearLow", "week52Low",
        )
        patch["price_change"] = self._pick_num(
            root, "change", "d", "price_change", "Change", "diff", "delta",
        )

        # v6.0.0 bug fix: compute percent_change from current/previous FIRST,
        # falling back to the API field only when prices aren't available.
        # v5.0.0 always pulled the API field via `_pick_ratio`, whose abs>1.5
        # heuristic leaves small values (<1.5%) unconverted -- so a 0.75%
        # daily move (raw value 0.75) became a ratio of 0.75 (75% gain).
        # `_fill_derived_quote_fields`'s fallback only fires when the field
        # is None, so the poisoned value from the API was never corrected.
        current = patch.get("current_price")
        previous = patch.get("previous_close")
        if current is not None and previous is not None and previous != 0:
            patch["percent_change"] = (current - previous) / previous
        else:
            patch["percent_change"] = self._pick_ratio(
                root, "change_pct", "change_percent", "dp",
                "percent_change", "pctChange", "changePercent",
            )

        patch["beta_5y"] = self._pick_num(root, "beta", "beta5y", "beta_5y")
        patch["asset_class"] = self._pick_str(root, "asset_class", "assetClass", "type", "securityType")

        _fill_derived_quote_fields(patch)
        return _clean_patch(patch)

    def _map_fundamentals(self, root: Any) -> Dict[str, Any]:
        """
        Map fundamental fields from response.

        Note: uses `_pick_ratio` (heuristic) for percent fields. Tadawul's data
        source is configurable via env URLs -- could be any aggregator with
        any scale convention. Conservative default; switch to explicit /100
        if your aggregator is known to return percent-points.
        """
        patch = self._map_identity(root)

        patch["market_cap"] = self._pick_num(root, "market_cap", "marketCap", "marketCapitalization", "MarketCap")
        patch["float_shares"] = self._pick_num(root, "floatShares", "float_shares", "freeFloatShares", "free_float_shares")
        patch["shares_outstanding"] = self._pick_num(root, "shares", "shares_outstanding", "shareOutstanding", "SharesOutstanding")
        patch["pe_ttm"] = self._pick_num(root, "pe", "pe_ttm", "trailingPE", "PE", "priceEarnings")
        patch["pb_ratio"] = self._pick_num(root, "pb", "pb_ratio", "priceToBook", "PBR", "price_book")
        patch["ps_ratio"] = self._pick_num(root, "ps", "ps_ratio", "priceToSales")
        patch["eps_ttm"] = self._pick_num(root, "eps", "eps_ttm", "trailingEps", "EPS")
        patch["dividend_yield"] = self._pick_ratio(root, "dividend_yield", "divYield", "yield", "DividendYield")
        patch["payout_ratio"] = self._pick_ratio(root, "payout_ratio", "payoutRatio")
        patch["beta_5y"] = self._pick_num(root, "beta", "beta5y", "beta_5y")
        patch["revenue_ttm"] = self._pick_num(root, "revenue_ttm", "revenue", "sales", "totalRevenue")
        patch["revenue_growth_yoy"] = self._pick_ratio(root, "revenue_growth_yoy", "revenueGrowth", "salesGrowth")
        patch["gross_margin"] = self._pick_ratio(root, "gross_margin", "grossMargin")
        patch["operating_margin"] = self._pick_ratio(root, "operating_margin", "operatingMargin")
        patch["profit_margin"] = self._pick_ratio(root, "profit_margin", "netMargin", "profitMargin")

        return _clean_patch(patch)

    def _map_profile(self, root: Any) -> Dict[str, Any]:
        """Map profile fields from response."""
        patch = self._map_identity(root)

        if "sector" not in patch:
            patch["sector"] = self._pick_str(
                root, "sectorClassification", "classificationSector", "sectorDesc",
            )
        if "industry" not in patch:
            patch["industry"] = self._pick_str(
                root, "industryClassification", "classificationIndustry", "industryDesc",
            )
        if "name" not in patch:
            patch["name"] = self._pick_str(root, "issuerName", "issuer_name")

        return _clean_patch(patch)

    # -- Fetch methods --------------------------------------------------

    async def fetch_quote_patch(self, symbol: str) -> Dict[str, Any]:
        """Fetch quote patch for a symbol."""
        if not self.config.is_configured:
            return {} if not self.config.verbose_warnings else {"_warn": "provider_not_configured"}

        sym = normalize_ksa_symbol(symbol)
        if not sym or not self.config.quote_url:
            return {} if not self.config.verbose_warnings else {"_warn": "invalid_symbol_or_url"}

        cache_key = f"quote:{sym}"
        cached = await self._quote_cache.get(cache_key)
        if cached is not None:
            return dict(cached)

        async def _fetch() -> Dict[str, Any]:
            url = format_url(self.config.quote_url, sym)
            data, err = await self._request(url)
            if data is None:
                await self._error_cache.set(cache_key, True)
                return {} if not self.config.verbose_warnings else {"_warn": f"quote_failed: {err}"}

            mapped = self._map_quote(self._coerce_dict(data))
            if (_safe_float(mapped.get("current_price")) is None
                    and _safe_float(mapped.get("day_high")) is None):
                return {} if not self.config.verbose_warnings else {"_warn": "quote_payload_sparse"}

            _ensure_ksa_identity(mapped, symbol=sym)
            result = {
                "requested_symbol": symbol,
                "symbol": sym,
                "symbol_normalized": sym,
                "provider": PROVIDER_NAME,
                "data_source": PROVIDER_NAME,
                "provider_version": PROVIDER_VERSION,
                "last_updated_utc": _utc_iso(),
                "last_updated_riyadh": _riyadh_iso(),
                **mapped,
            }
            _fill_derived_quote_fields(result)
            await self._quote_cache.set(cache_key, result)
            return result

        return await self._single_flight.run(cache_key, _fetch)

    async def fetch_fundamentals_patch(self, symbol: str) -> Dict[str, Any]:
        """Fetch fundamentals patch for a symbol."""
        if not (self.config.is_configured and self.config.enable_fundamentals):
            return {}

        sym = normalize_ksa_symbol(symbol)
        if not sym or not self.config.fundamentals_url:
            return {}

        cache_key = f"fund:{sym}"
        cached = await self._fund_cache.get(cache_key)
        if cached is not None:
            return dict(cached)

        async def _fetch() -> Dict[str, Any]:
            url = format_url(self.config.fundamentals_url, sym)
            data, err = await self._request(url)
            if data is None:
                await self._error_cache.set(cache_key, True)
                return {}

            mapped = self._map_fundamentals(self._coerce_dict(data))
            if not mapped:
                return {}

            _ensure_ksa_identity(mapped, symbol=sym)
            result = {
                "provider": PROVIDER_NAME,
                "data_source": PROVIDER_NAME,
                "provider_version": PROVIDER_VERSION,
                **mapped,
            }
            await self._fund_cache.set(cache_key, result)
            return result

        return await self._single_flight.run(cache_key, _fetch)

    async def fetch_profile_patch(self, symbol: str) -> Dict[str, Any]:
        """Fetch profile patch for a symbol."""
        if not (self.config.is_configured and self.config.enable_profile):
            return {}

        sym = normalize_ksa_symbol(symbol)
        if not sym or not self.config.profile_url:
            return {}

        cache_key = f"profile:{sym}"
        cached = await self._profile_cache.get(cache_key)
        if cached is not None:
            return dict(cached)

        async def _fetch() -> Dict[str, Any]:
            url = format_url(self.config.profile_url, sym)
            data, err = await self._request(url)
            if data is None:
                return {}

            mapped = self._map_profile(self._coerce_dict(data))
            if not mapped:
                return {}

            _ensure_ksa_identity(mapped, symbol=sym)
            result = {
                "provider": PROVIDER_NAME,
                "data_source": PROVIDER_NAME,
                "provider_version": PROVIDER_VERSION,
                **mapped,
            }
            await self._profile_cache.set(cache_key, result)
            return result

        return await self._single_flight.run(cache_key, _fetch)

    async def fetch_history_patch(self, symbol: str) -> Dict[str, Any]:
        """Fetch history patch for a symbol."""
        if not (self.config.is_configured and self.config.enable_history):
            return {}

        sym = normalize_ksa_symbol(symbol)
        if not sym or not self.config.history_url:
            return {}

        days = self.config.history_days
        cache_key = f"history:{sym}:{days}"
        cached = await self._history_cache.get(cache_key)
        if cached is not None:
            return dict(cached)

        async def _fetch() -> Dict[str, Any]:
            url = format_url(self.config.history_url, sym, days=days)
            data, err = await self._request(url)
            if data is None:
                await self._error_cache.set(cache_key, True)
                return {}

            rows, last_dt = self._parse_history_payload(data)
            if len(rows) < 2:
                return {}

            analytics = _compute_history_analytics(rows)
            result = {
                "requested_symbol": symbol,
                "normalized_symbol": sym,
                "history_points": len(rows),
                "history_last_utc": _utc_iso(last_dt),
                "history_last_riyadh": _to_riyadh_iso(last_dt),
                "provider": PROVIDER_NAME,
                "data_source": PROVIDER_NAME,
                "provider_version": PROVIDER_VERSION,
                **analytics,
            }
            _ensure_ksa_identity(result, symbol=sym)
            _fill_derived_quote_fields(result)
            await self._history_cache.set(cache_key, result)
            return result

        return await self._single_flight.run(cache_key, _fetch)

    def _parse_history_payload(
        self,
        data: Any,
    ) -> Tuple[List[Dict[str, Any]], Optional[datetime]]:
        """Parse history payload into (rows, last_dt)."""
        rows: List[Dict[str, Any]] = []
        last_dt: Optional[datetime] = None

        # Case 1: dict with aligned arrays
        if isinstance(data, dict):
            t_raw = data.get("t") or data.get("time") or data.get("timestamp") or data.get("dates")
            o_raw = data.get("o") or data.get("open")
            h_raw = data.get("h") or data.get("high")
            l_raw = data.get("l") or data.get("low")
            c_raw = data.get("c") or data.get("close")
            v_raw = data.get("v") or data.get("volume")

            if isinstance(c_raw, list):
                n = len(c_raw)
                for i in range(n):
                    row: Dict[str, Any] = {
                        "open": _safe_float(o_raw[i]) if isinstance(o_raw, list) and i < len(o_raw) else None,
                        "high": _safe_float(h_raw[i]) if isinstance(h_raw, list) and i < len(h_raw) else None,
                        "low": _safe_float(l_raw[i]) if isinstance(l_raw, list) and i < len(l_raw) else None,
                        "close": _safe_float(c_raw[i]),
                        "volume": _safe_float(v_raw[i]) if isinstance(v_raw, list) and i < len(v_raw) else None,
                    }
                    t = t_raw[i] if isinstance(t_raw, list) and i < len(t_raw) else None
                    if t is not None:
                        try:
                            if isinstance(t, (int, float)):
                                dt = datetime.fromtimestamp(float(t), tz=timezone.utc)
                            else:
                                dt = datetime.fromisoformat(str(t).replace("Z", "+00:00"))
                            row["date"] = dt.isoformat()
                            last_dt = dt
                        except Exception:
                            pass
                    rows.append(row)

            if not rows:
                for key in ("candles", "data", "items", "prices", "history", "rows"):
                    if isinstance(data.get(key), list):
                        data = data[key]
                        break

        # Case 2: list of dict bars
        if isinstance(data, list):
            for item in data:
                if not isinstance(item, dict):
                    continue
                row = {
                    "open": _safe_float(item.get("o") if "o" in item else item.get("open")),
                    "high": _safe_float(item.get("h") if "h" in item else item.get("high")),
                    "low": _safe_float(item.get("l") if "l" in item else item.get("low")),
                    "close": _safe_float(item.get("c") if "c" in item else item.get("close")),
                    "volume": _safe_float(item.get("v") if "v" in item else item.get("volume")),
                }
                t = item.get("t") or item.get("time") or item.get("timestamp") or item.get("date")
                if t is not None:
                    try:
                        if isinstance(t, (int, float)):
                            dt = datetime.fromtimestamp(float(t), tz=timezone.utc)
                        else:
                            dt = datetime.fromisoformat(str(t).replace("Z", "+00:00"))
                        row["date"] = dt.isoformat()
                        last_dt = dt
                    except Exception:
                        pass
                rows.append(row)

        max_points = self.config.history_points_max
        rows = rows[-max_points:]
        return rows, last_dt

    async def fetch_enriched_quote_patch(self, symbol: str) -> Dict[str, Any]:
        """Fetch enriched quote patch merging all available sources."""
        if not self.config.is_configured:
            return {}

        sym = normalize_ksa_symbol(symbol)
        if not sym:
            return {} if not self.config.verbose_warnings else {"_warn": "invalid_ksa_symbol"}

        quote_data, fund_data, hist_data, prof_data = await asyncio.gather(
            self.fetch_quote_patch(symbol),
            self.fetch_fundamentals_patch(symbol),
            self.fetch_history_patch(symbol),
            self.fetch_profile_patch(symbol),
            return_exceptions=True,
        )

        quote_data = quote_data if isinstance(quote_data, dict) else {}
        fund_data = fund_data if isinstance(fund_data, dict) else {}
        hist_data = hist_data if isinstance(hist_data, dict) else {}
        prof_data = prof_data if isinstance(prof_data, dict) else {}

        if quote_data:
            result = dict(quote_data)
        else:
            result = {
                "requested_symbol": symbol,
                "symbol": sym,
                "symbol_normalized": sym,
                "provider": PROVIDER_NAME,
                "data_source": PROVIDER_NAME,
                "provider_version": PROVIDER_VERSION,
                "last_updated_utc": _utc_iso(),
                "last_updated_riyadh": _riyadh_iso(),
            }

        # Merge in precedence order: profile (classification), fund (financials), history (technicals)
        if prof_data:
            for k in ("name", "sector", "industry", "exchange", "currency", "country"):
                if k in prof_data and prof_data[k] is not None:
                    if k not in result or result.get(k) in (None, ""):
                        result[k] = prof_data[k]

        if fund_data:
            for k in ("market_cap", "pe_ttm", "eps_ttm", "pb_ratio", "ps_ratio",
                      "dividend_yield", "beta_5y"):
                if k in fund_data and fund_data[k] is not None:
                    if k not in result or result.get(k) in (None, ""):
                        result[k] = fund_data[k]

        if hist_data:
            for k in (
                "week_52_high", "week_52_low", "week_52_position_pct",
                "avg_volume_10d", "avg_volume_30d", "rsi_14", "volatility_30d",
            ):
                if k in hist_data and hist_data[k] is not None:
                    if k not in result or result.get(k) in (None, ""):
                        result[k] = hist_data[k]

        _ensure_ksa_identity(result, symbol=sym)
        _fill_derived_quote_fields(result)

        quality, score = _data_quality_score(result)
        result["data_quality"] = quality.value
        result["data_quality_score"] = score

        if self.config.verbose_warnings:
            warnings = []
            for src, name in [(quote_data, "quote"), (fund_data, "fund"),
                              (hist_data, "hist"), (prof_data, "prof")]:
                if isinstance(src, dict) and "_warn" in src:
                    warnings.append(f"{name}:{src['_warn']}")
            if warnings:
                result["_warnings"] = " | ".join(warnings[:3])

        return _clean_patch(result)

    async def get_enriched_quotes_batch(
        self,
        symbols: List[str],
        mode: str = "",
    ) -> Dict[str, Dict[str, Any]]:
        """Batch fetch enriched quotes for multiple symbols."""
        if not symbols:
            return {}

        results = await asyncio.gather(
            *(self.fetch_enriched_quote_patch(sym) for sym in symbols),
            return_exceptions=True,
        )

        output: Dict[str, Dict[str, Any]] = {}
        for sym, result in zip(symbols, results):
            if isinstance(result, Exception):
                output[sym] = {
                    "symbol": sym,
                    "provider": PROVIDER_NAME,
                    "data_quality": "MISSING",
                    "error": str(result),
                }
            elif isinstance(result, dict):
                key = result.get("symbol") or normalize_ksa_symbol(sym) or sym
                output[key] = result
            else:
                output[sym] = {
                    "symbol": sym,
                    "provider": PROVIDER_NAME,
                    "data_quality": "MISSING",
                }
        return output

    async def get_metrics(self) -> Dict[str, Any]:
        """Get client metrics snapshot."""
        return {
            "provider": PROVIDER_NAME,
            "version": PROVIDER_VERSION,
            "configured": self.config.is_configured,
            "preset": self.config.provider_preset or None,
            "auth": {
                "has_api_key": bool(self.config.api_key),
                "header": self.config.api_key_header or None,
            },
            "urls": {
                "quote": bool(self.config.quote_url),
                "fundamentals": bool(self.config.fundamentals_url),
                "history": bool(self.config.history_url),
                "profile": bool(self.config.profile_url),
            },
            "cache_sizes": {
                "quote": await self._quote_cache.size(),
                "fund": await self._fund_cache.size(),
                "history": await self._history_cache.size(),
                "profile": await self._profile_cache.size(),
                "error": await self._error_cache.size(),
            },
            "ksa_defaults": {
                "country": KSA_COUNTRY_CODE,
                "currency": KSA_CURRENCY,
                "exchange": KSA_EXCHANGE_NAME,
            },
        }

    async def close(self) -> None:
        """Close the underlying httpx client."""
        try:
            await self._client.aclose()
        except Exception as exc:
            logger.debug("tadawul client close failed: %s", exc)


# ---------------------------------------------------------------------------
# Singleton Client (lazy lock)
# ---------------------------------------------------------------------------

_CLIENT_INSTANCE: Optional[TadawulClient] = None
_CLIENT_LOCK: Optional[asyncio.Lock] = None


def _get_client_lock() -> asyncio.Lock:
    global _CLIENT_LOCK
    if _CLIENT_LOCK is None:
        _CLIENT_LOCK = asyncio.Lock()
    return _CLIENT_LOCK


async def get_client() -> TadawulClient:
    """Get the singleton TadawulClient instance."""
    global _CLIENT_INSTANCE
    if _CLIENT_INSTANCE is not None:
        return _CLIENT_INSTANCE
    async with _get_client_lock():
        if _CLIENT_INSTANCE is None:
            _CLIENT_INSTANCE = TadawulClient()
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
    """Batch fetch enriched quotes for multiple symbols."""
    if kwargs:
        logger.debug("fetch_enriched_quotes_batch: ignoring kwargs=%r", kwargs)
    client = await get_client()
    return await client.get_enriched_quotes_batch(symbols, mode=mode)


async def fetch_quote_patch(symbol: str, *args: Any, **kwargs: Any) -> Dict[str, Any]:
    """Fetch quote patch (quote only)."""
    if args or kwargs:
        logger.debug("fetch_quote_patch(%s): ignoring args=%r kwargs=%r", symbol, args, kwargs)
    client = await get_client()
    return await client.fetch_quote_patch(symbol)


async def fetch_fundamentals_patch(symbol: str, *args: Any, **kwargs: Any) -> Dict[str, Any]:
    """Fetch fundamentals patch."""
    if args or kwargs:
        logger.debug("fetch_fundamentals_patch(%s): ignoring args=%r kwargs=%r", symbol, args, kwargs)
    client = await get_client()
    return await client.fetch_fundamentals_patch(symbol)


async def fetch_history_patch(symbol: str, *args: Any, **kwargs: Any) -> Dict[str, Any]:
    """Fetch history patch."""
    if args or kwargs:
        logger.debug("fetch_history_patch(%s): ignoring args=%r kwargs=%r", symbol, args, kwargs)
    client = await get_client()
    return await client.fetch_history_patch(symbol)


async def fetch_profile_patch(symbol: str, *args: Any, **kwargs: Any) -> Dict[str, Any]:
    """Fetch profile patch."""
    if args or kwargs:
        logger.debug("fetch_profile_patch(%s): ignoring args=%r kwargs=%r", symbol, args, kwargs)
    client = await get_client()
    return await client.fetch_profile_patch(symbol)


async def fetch_quote(symbol: str, *args: Any, **kwargs: Any) -> Dict[str, Any]:
    """Alias for fetch_enriched_quote_patch."""
    return await fetch_enriched_quote_patch(symbol, *args, **kwargs)


async def get_quote(symbol: str, *args: Any, **kwargs: Any) -> Dict[str, Any]:
    """Alias for fetch_enriched_quote_patch."""
    return await fetch_enriched_quote_patch(symbol, *args, **kwargs)


async def fetch_history(symbol: str, *args: Any, **kwargs: Any) -> Dict[str, Any]:
    """Alias for fetch_history_patch."""
    return await fetch_history_patch(symbol, *args, **kwargs)


async def get_history(symbol: str, *args: Any, **kwargs: Any) -> Dict[str, Any]:
    """Alias for fetch_history_patch."""
    return await fetch_history_patch(symbol, *args, **kwargs)


async def get_client_metrics() -> Dict[str, Any]:
    """Get client metrics snapshot."""
    client = await get_client()
    return await client.get_metrics()


async def aclose_tadawul_client() -> None:
    """Close the Tadawul client."""
    await close_client()


# ---------------------------------------------------------------------------
# Provider Adapter (singleton)
# ---------------------------------------------------------------------------

class _TadawulProviderAdapter:
    """Provider adapter for engine compatibility."""

    @property
    def name(self) -> str:
        return PROVIDER_NAME

    @property
    def version(self) -> str:
        return PROVIDER_VERSION

    async def get_quote(self, symbol: str) -> Dict[str, Any]:
        return await fetch_enriched_quote_patch(symbol)

    async def quote(self, symbol: str) -> Dict[str, Any]:
        return await fetch_enriched_quote_patch(symbol)

    async def fetch_quote(self, symbol: str) -> Dict[str, Any]:
        return await fetch_enriched_quote_patch(symbol)

    async def fetch_enriched_quote(self, symbol: str) -> Dict[str, Any]:
        return await fetch_enriched_quote_patch(symbol)

    async def fetch_quote_patch(self, symbol: str) -> Dict[str, Any]:
        return await fetch_quote_patch(symbol)

    async def fetch_enriched_quote_patch(self, symbol: str) -> Dict[str, Any]:
        return await fetch_enriched_quote_patch(symbol)

    async def fetch_fundamentals_patch(self, symbol: str) -> Dict[str, Any]:
        return await fetch_fundamentals_patch(symbol)

    async def fetch_history_patch(self, symbol: str) -> Dict[str, Any]:
        return await fetch_history_patch(symbol)

    async def fetch_history(self, symbol: str) -> Dict[str, Any]:
        return await fetch_history_patch(symbol)

    async def get_history(self, symbol: str) -> Dict[str, Any]:
        return await fetch_history_patch(symbol)

    async def get_enriched_quotes_batch(
        self,
        symbols: List[str],
        mode: str = "",
    ) -> Dict[str, Dict[str, Any]]:
        return await fetch_enriched_quotes_batch(symbols, mode=mode)

    async def get_metrics(self) -> Dict[str, Any]:
        return await get_client_metrics()


# Singleton provider instance
provider = _TadawulProviderAdapter()


def get_provider() -> _TadawulProviderAdapter:
    """Get the singleton provider instance."""
    return provider


def build_provider() -> _TadawulProviderAdapter:
    """Alias for get_provider (back-compat)."""
    return provider


# ---------------------------------------------------------------------------
# Module Exports
# ---------------------------------------------------------------------------

__all__ = [
    # Metadata
    "PROVIDER_NAME",
    "PROVIDER_VERSION",
    "VERSION",
    "PROVIDER_BATCH_SUPPORTED",
    # v6.1.0: preset registry (operators can introspect supported presets)
    "PROVIDER_PRESETS",
    # Symbol normalization
    "normalize_ksa_symbol",
    # Engine-facing functions
    "fetch_enriched_quote_patch",
    "fetch_enriched_quotes_batch",
    "fetch_quote_patch",
    "fetch_fundamentals_patch",
    "fetch_history_patch",
    "fetch_profile_patch",
    "fetch_quote",
    "get_quote",
    "fetch_history",
    "get_history",
    "get_client_metrics",
    "aclose_tadawul_client",
    # Provider adapter
    "provider",
    "get_provider",
    "build_provider",
    # Enums
    "MarketRegime",
    "DataQuality",
    # Classes (newly exported for clarity)
    "TadawulConfig",
    "TadawulClient",
    # Exceptions
    "TadawulProviderError",
    "TadawulConfigurationError",
    "TadawulFetchError",
]
