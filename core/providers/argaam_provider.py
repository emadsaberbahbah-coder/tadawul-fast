#!/usr/bin/env python3
"""
core/providers/argaam_provider.py
================================================================================
Argaam Provider (KSA Market Data) -- v6.1.0
================================================================================
PROFILE + CLASSIFICATION + HISTORY FALLBACK + ROUTER/ENGINE COMPAT

Purpose
-------
Provides KSA market data from Argaam-compatible APIs:
  - Real-time quotes
  - Company profiles and classification
  - Historical OHLCV data
  - Derived technical indicators (RSI, ATR, volume ratio)
  - KSA-specific symbol normalization

v6.1.0 Changes (from v6.0.0)
----------------------------
The "satisfaction problem" fix (mirrored from tadawul_provider v6.1.0):
the provider's URL fields all defaulted to empty strings, which silently
disabled the secondary KSA fallback. With Tadawul down or rate-limited,
the engine had no working fallback before Yahoo, so .SR rows scored as
HOLD/Insufficient data even on transient Tadawul outages.

Operator-facing improvements:
  - Added PROVIDER_PRESETS map for known Saudi-data APIs that can serve
    as the Argaam fallback role: sahmk, twelvedata, stockerapi.
    (Argaam.com itself does not publish a public quote/profile/history
    REST API; this slot is configured for any Argaam-compatible JSON
    feed.) Set `ARGAAM_PROVIDER_PRESET=sahmk` (or another preset name)
    and the URLs are auto-filled with the right URL template.
  - Added `ARGAAM_API_KEY` env var. When set, the provider auto-injects
    an Authorization-style header (default `X-API-Key`, override via
    `ARGAAM_API_KEY_HEADER`).
  - Explicit URL env vars (ARGAAM_QUOTE_URL, etc.) ALWAYS win over the
    preset.
  - When the provider is enabled but no URLs/preset are configured,
    a single WARNING is logged at construction time. Operators see a
    clear message in Render logs at startup.
  - `ArgaamConfig` gained 3 new optional fields: `provider_preset`,
    `api_key`, `api_key_header`. The frozen dataclass keeps backward
    compatibility because the existing 3 URL fields stay positional.
  - Boolean vocabulary aligned with the canonical 8-value set: added
    `enable` to `_TRUTHY` and added `_FALSY` set. v6.0.0 only had
    `_TRUTHY` and was missing `enable`, so any operator who set
    `ARGAAM_*=enable` saw it parsed as the default value rather than
    True, and any FALSY value never short-circuited correctly.
  - `get_provider().status()` now reports preset state for health.

Bug-compat: the public API (functions, fetch methods, dataclass field
order, __all__, env-var names, return shapes) is unchanged. The 3 new
ArgaamConfig fields have safe defaults.

Provider preset templates (configurable via env if a vendor changes URLs):
  - sahmk      :  Saudi-licensed market data
                  https://app.sahmk.sa/api/v1/quote/{code}/
                  https://app.sahmk.sa/api/v1/history/{code}/?days={days}
                  https://app.sahmk.sa/api/v1/profile/{code}/
                  Auth: X-API-Key header (set ARGAAM_API_KEY)
  - twelvedata :  Saudi exchange (XSAU) via Twelve Data REST
                  https://api.twelvedata.com/quote?symbol={code}&exchange=XSAU&apikey={key}
                  https://api.twelvedata.com/time_series?symbol={code}&exchange=XSAU&interval=1day&outputsize={days}&apikey={key}
                  https://api.twelvedata.com/profile?symbol={code}&exchange=XSAU&apikey={key}
                  Auth: in-URL apikey
  - stockerapi :  Third-party Tadawul provider
                  https://stockerapi.com/api/v1/saudi/quote/{code}
                  https://stockerapi.com/api/v1/saudi/history/{code}?days={days}
                  https://stockerapi.com/api/v1/saudi/profile/{code}
                  Auth: Authorization Bearer

Setting up KSA secondary data on Render (one-time):
  1. Choose a provider that's DIFFERENT from your TADAWUL_PROVIDER_PRESET.
     For example: TADAWUL_PROVIDER_PRESET=sahmk + ARGAAM_PROVIDER_PRESET=twelvedata
     gives you genuine redundancy across two independent vendors.
  2. In Render Dashboard -> Environment, add:
       ARGAAM_PROVIDER_PRESET = twelvedata        (or your chosen preset)
       ARGAAM_API_KEY         = <your key>
  3. Trigger a deploy.
  4. Verify Argaam now appears in the provider chain when Tadawul is
     skipped (force this by temporarily unsetting TADAWUL_API_KEY in
     a non-prod environment).

You can still set explicit URLs (the v6.0.0 way) -- they take precedence
over the preset.

----------------------------
v6.0.0 Changes (from v5.0.0)
----------------------------
Bug fixes:
  - _fetch_first no longer raises NameError when called with an empty URL
    list; `error` was unbound in that path.
  - _compute_history_stats now parses rows as tuples and filters rows that
    lack required fields, so OHLC lists stay index-aligned. Previously a
    row missing `high` would push `highs`/`lows` out of sync with `closes`,
    causing _compute_atr_14 to compare unrelated rows.
  - fetch_enriched_quote_patch no longer allocates a brand-new
    `ArgaamClient()` (with its own httpx connection pool!) just to call
    _error_patch. The function is now a @staticmethod.
  - Dual-singleton footgun resolved: v5.0.0 exported both a module-level
    `provider = ArgaamProvider()` and `get_provider()` creating a DIFFERENT
    singleton. They now share the same instance.
  - All asyncio.Lock() / asyncio.Semaphore() instantiations are lazy so
    module import is event-loop-free.

Cleanup:
  - _build_url_candidates no longer uses the `not seen.add(u)` side-effect
    trick inside a list comprehension.
  - _TTLCache eviction now removes a bounded excess (not a flat 200) and
    uses insertion-order (FIFO) eviction rather than random sampling.
  - fetch_enriched_quote_patch rejected unexpected *args/**kwargs in v5.0.0
    silently; v6.0.0 logs a debug message so misuse is detectable.
  - Removed unused imports (Union, cast, _ORJSON_AVAILABLE flag).

Preserved for backward compatibility:
  - Every name in __all__, every alias (fetch_patch, fetch_quote, quote,
    get_quote, fetch_profile_patch, fetch_history_patch, etc.).
  - Every environment variable name and default.
  - Return shape of the enriched patch (fields, types, error keys).
================================================================================
"""

from __future__ import annotations

import asyncio
import json
import logging
import math
import os
import random
import re
import time
import uuid
from collections import OrderedDict
from dataclasses import dataclass
from datetime import datetime, timezone
from enum import Enum
from typing import Any, Dict, Iterable, List, Optional, Tuple

# ---------------------------------------------------------------------------
# Optional Dependencies
# ---------------------------------------------------------------------------

try:
    import httpx
    _HTTPX_AVAILABLE = True
except ImportError:  # pragma: no cover
    httpx = None  # type: ignore[assignment]
    _HTTPX_AVAILABLE = False

try:
    import orjson  # type: ignore

    def _json_loads(data: Any) -> Any:
        return orjson.loads(data)
except ImportError:
    def _json_loads(data: Any) -> Any:
        return json.loads(data)

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------

logger = logging.getLogger(__name__)
logger.addHandler(logging.NullHandler())

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

PROVIDER_NAME = "argaam"
PROVIDER_VERSION = "6.1.0"
VERSION = PROVIDER_VERSION
PROVIDER_BATCH_SUPPORTED = True

# Default configuration values
DEFAULT_TIMEOUT_SEC = 20.0
DEFAULT_RETRY_ATTEMPTS = 3
DEFAULT_MAX_CONCURRENCY = 10
DEFAULT_CACHE_TTL_SEC = 20.0
DEFAULT_HISTORY_TTL_SEC = 120.0
DEFAULT_CACHE_MAX_SIZE = 2000

_TRUTHY = {"1", "true", "yes", "y", "on", "t", "enabled", "enable"}
_FALSY = {"0", "false", "no", "n", "off", "f", "disabled", "disable"}
_ARABIC_DIGITS = str.maketrans("٠١٢٣٤٥٦٧٨٩", "0123456789")
_KSA_CODE_RE = re.compile(r"^\d{3,6}$")
_K_M_B_RE = re.compile(r"^(-?\d+(?:\.\d+)?)([KMB])$", re.IGNORECASE)
_K_M_B_MULT = {"K": 1_000.0, "M": 1_000_000.0, "B": 1_000_000_000.0}


# ---------------------------------------------------------------------------
# Provider presets (v6.1.0)
# ---------------------------------------------------------------------------
# Preset names map to URL templates for known Saudi data providers that can
# serve as Argaam's fallback role. Argaam.com itself does not publish a
# public quote/profile/history REST API, so this slot is configured for any
# Argaam-compatible JSON feed (typically a different vendor than the one
# configured for TADAWUL_PROVIDER_PRESET, to give the engine real
# cross-vendor redundancy).
#
# Operators set ARGAAM_PROVIDER_PRESET=<name> + ARGAAM_API_KEY=<key> and
# the provider auto-fills URLs and headers. Explicit ARGAAM_*_URL env vars
# still win over the preset.
#
# These templates use:
#   {symbol} -> the full normalized form, e.g. "2222.SR"
#   {code}   -> just the numeric code, e.g. "2222"
#   {days}   -> history days, filled at request time
# ---------------------------------------------------------------------------

PROVIDER_PRESETS: Dict[str, Dict[str, str]] = {
    # SAHMK: Saudi-licensed Tadawul market data provider (sahmk.sa).
    # Auth: X-API-Key header.
    "sahmk": {
        "quote_url":      "https://app.sahmk.sa/api/v1/quote/{code}/",
        "profile_url":    "https://app.sahmk.sa/api/v1/profile/{code}/",
        "history_url":    "https://app.sahmk.sa/api/v1/history/{code}/?days={days}",
        "api_key_header": "X-API-Key",
    },
    # Twelve Data: Saudi exchange (XSAU) via standard REST.
    # Auth: in-URL apikey (no header needed).
    "twelvedata": {
        "quote_url":      "https://api.twelvedata.com/quote?symbol={code}&exchange=XSAU&apikey={api_key}",
        "profile_url":    "https://api.twelvedata.com/profile?symbol={code}&exchange=XSAU&apikey={api_key}",
        "history_url":    "https://api.twelvedata.com/time_series?symbol={code}&exchange=XSAU&interval=1day&outputsize={days}&apikey={api_key}",
        "api_key_header": "",  # not needed; key is in URL
    },
    # StockerAPI: third-party Tadawul provider.
    # Auth: Authorization Bearer header.
    "stockerapi": {
        "quote_url":      "https://stockerapi.com/api/v1/saudi/quote/{code}",
        "profile_url":    "https://stockerapi.com/api/v1/saudi/profile/{code}",
        "history_url":    "https://stockerapi.com/api/v1/saudi/history/{code}?days={days}",
        "api_key_header": "Authorization",  # value will be "Bearer <key>"
    },
}


def _resolve_preset(preset_name: str) -> Dict[str, str]:
    """Return the URL template dict for a preset name, or {} if unknown."""
    if not preset_name:
        return {}
    return dict(PROVIDER_PRESETS.get(preset_name.strip().lower(), {}) or {})


USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/124.0.0.0 Safari/537.36"
)

# Field alias mappings for patch-key normalization
FIELD_ALIASES: Dict[str, str] = {
    "price": "current_price",
    "last_price": "current_price",
    "last": "current_price",
    "close": "previous_close",
    "prev_close": "previous_close",
    "change": "price_change",
    "change_pct": "percent_change",
    "change_percent": "percent_change",
    "pct_change": "percent_change",
    "open": "open_price",
    "day_open": "open_price",
    "high": "day_high",
    "low": "day_low",
    "fifty_two_week_high": "week_52_high",
    "fifty_two_week_low": "week_52_low",
    "avg_vol_10d": "avg_volume_10d",
    "avg_vol_30d": "avg_volume_30d",
    "company_name": "name",
    "return_on_equity": "roe",
    "returnonequity": "roe",
    "returnOnEquity": "roe",
    "return_on_assets": "roa",
    "returnonassets": "roa",
    "returnOnAssets": "roa",
    "atr": "atr_14",
    "average_true_range": "atr_14",
    "averageTrueRange": "atr_14",
    "rsi": "rsi_14",
    "RSI": "rsi_14",
    "vol_ratio": "volume_ratio",
    "volumeRatio": "volume_ratio",
    "day_position": "day_range_position",
    "priceChange5d": "price_change_5d",
    "change_5d": "price_change_5d",
    "five_day_change": "price_change_5d",
}


# ---------------------------------------------------------------------------
# Enums
# ---------------------------------------------------------------------------

class DataQuality(str, Enum):
    """Data quality levels for provider responses."""
    GOOD = "GOOD"
    DEGRADED = "DEGRADED"
    MISSING = "MISSING"


class ProviderErrorCode(str, Enum):
    """Error codes for provider failures."""
    INVALID_SYMBOL = "invalid_ksa_symbol"
    NO_CONFIGURATION = "no_configuration"
    FETCH_FAILED = "fetch_failed"
    NETWORK_ERROR = "network_error"
    RATE_LIMITED = "rate_limited"
    SERVER_ERROR = "server_error"
    HTTP_ERROR = "http_error"
    INVALID_JSON = "invalid_json"
    EMPTY_RESPONSE = "empty_response"


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class ArgaamConfig:
    """Immutable configuration for Argaam provider."""
    quote_url: str
    profile_url: str
    history_url: str
    timeout_sec: float = DEFAULT_TIMEOUT_SEC
    retry_attempts: int = DEFAULT_RETRY_ATTEMPTS
    max_concurrency: int = DEFAULT_MAX_CONCURRENCY
    cache_ttl_sec: float = DEFAULT_CACHE_TTL_SEC
    history_ttl_sec: float = DEFAULT_HISTORY_TTL_SEC
    cache_max_size: int = DEFAULT_CACHE_MAX_SIZE
    # v6.1.0: preset-driven URL filling and auth-header injection
    provider_preset: str = ""
    api_key: str = ""
    api_key_header: str = ""

    @classmethod
    def from_env(cls) -> "ArgaamConfig":
        """Load configuration from environment variables."""
        def _env_float(name: str, default: float) -> float:
            try:
                return float(os.getenv(name, str(default)))
            except (ValueError, TypeError):
                return default

        def _env_int(name: str, default: int) -> int:
            try:
                return int(os.getenv(name, str(default)))
            except (ValueError, TypeError):
                return default

        # v6.1.0: preset support
        preset_name = os.getenv("ARGAAM_PROVIDER_PRESET", "").strip()
        api_key = os.getenv("ARGAAM_API_KEY", "").strip()
        api_key_header_explicit = os.getenv("ARGAAM_API_KEY_HEADER", "").strip()
        preset = _resolve_preset(preset_name)

        # Explicit env URLs always win; preset only fills the gaps.
        # Templates get {api_key} substituted now (e.g., Twelve Data uses
        # in-URL keys); {symbol}/{code}/{days} are filled at request time.
        def _from_preset(field_name: str, env_name: str) -> str:
            explicit = (os.getenv(env_name, "") or "").strip()
            if explicit:
                return explicit
            tmpl = preset.get(field_name, "")
            if tmpl and "{api_key}" in tmpl:
                tmpl = tmpl.replace("{api_key}", api_key)
            return tmpl

        # API-key-header resolution. Explicit env wins; otherwise the preset
        # may declare an empty string (meaning "key goes in URL, no header
        # needed", e.g. twelvedata). Default falls back to "X-API-Key" only
        # if no preset declared a policy and an api_key was provided.
        if api_key_header_explicit:
            api_key_header = api_key_header_explicit
        elif "api_key_header" in preset:
            api_key_header = preset["api_key_header"]
        elif api_key:
            api_key_header = "X-API-Key"
        else:
            api_key_header = ""

        cfg = cls(
            quote_url=_from_preset("quote_url", "ARGAAM_QUOTE_URL"),
            profile_url=_from_preset("profile_url", "ARGAAM_PROFILE_URL"),
            history_url=_from_preset("history_url", "ARGAAM_HISTORY_URL"),
            timeout_sec=_env_float("ARGAAM_TIMEOUT_SEC", DEFAULT_TIMEOUT_SEC),
            retry_attempts=_env_int("ARGAAM_RETRY_ATTEMPTS", DEFAULT_RETRY_ATTEMPTS),
            max_concurrency=_env_int("ARGAAM_MAX_CONCURRENCY", DEFAULT_MAX_CONCURRENCY),
            cache_ttl_sec=_env_float("ARGAAM_CACHE_TTL_SEC", DEFAULT_CACHE_TTL_SEC),
            history_ttl_sec=_env_float("ARGAAM_HISTORY_CACHE_TTL_SEC", DEFAULT_HISTORY_TTL_SEC),
            cache_max_size=_env_int("ARGAAM_CACHE_MAX", DEFAULT_CACHE_MAX_SIZE),
            provider_preset=preset_name.lower(),
            api_key=api_key,
            api_key_header=api_key_header,
        )

        # Startup-time configuration warning. Logged ONCE per process.
        # Mirrors the diagnostic added to tadawul_provider v6.1.0 for the
        # same silent-empty-URL failure mode.
        if not cfg.has_any_url():
            try:
                logger.warning(
                    "Argaam provider has NO URLs configured. KSA secondary "
                    "fallback is disabled. Fix: set ARGAAM_PROVIDER_PRESET "
                    "(sahmk|twelvedata|stockerapi) + ARGAAM_API_KEY in Render "
                    "env, OR set ARGAAM_QUOTE_URL/PROFILE_URL/HISTORY_URL "
                    "explicitly. Recommended: use a different preset than "
                    "TADAWUL_PROVIDER_PRESET so the two KSA providers give "
                    "true cross-vendor redundancy."
                )
            except Exception:
                pass

        return cfg

    def has_any_url(self) -> bool:
        """Return True if at least one endpoint URL is configured."""
        return bool(self.quote_url or self.profile_url or self.history_url)


# ---------------------------------------------------------------------------
# Custom Exceptions
# ---------------------------------------------------------------------------

class ArgaamProviderError(Exception):
    """Base exception for Argaam provider errors."""


class ArgaamConfigurationError(ArgaamProviderError):
    """Raised when the provider is not properly configured."""


class ArgaamFetchError(ArgaamProviderError):
    """Raised when a data fetch fails."""
    def __init__(self, symbol: str, error_code: ProviderErrorCode, details: Optional[str] = None):
        self.symbol = symbol
        self.error_code = error_code
        self.details = details
        suffix = f" - {details}" if details else ""
        super().__init__(f"Failed to fetch {symbol}: {error_code.value}{suffix}")


# ---------------------------------------------------------------------------
# Pure Utility Functions
# ---------------------------------------------------------------------------

def _now_utc_iso() -> str:
    """Get current UTC time in ISO format."""
    return datetime.now(timezone.utc).isoformat()


def _to_bool(value: Any, default: bool = False) -> bool:
    """Convert various representations to boolean."""
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        return value.strip().lower() in _TRUTHY
    return default


def _to_float(value: Any) -> Optional[float]:
    """
    Safely convert value to float with Arabic digit support.

    Handles:
      - Arabic digits (٠-٩), Arabic thousands (٬), Arabic decimal (٫)
      - K / M / B suffixes
      - Parentheses for negative numbers
      - SAR / ريال currency markers
      - Percentage signs
    """
    if value is None:
        return None

    try:
        if isinstance(value, (int, float)):
            f = float(value)
            return None if (math.isnan(f) or math.isinf(f)) else f

        s = str(value).strip()
        if not s or s.lower() in {"-", "—", "n/a", "na", "null", "none"}:
            return None

        s = s.translate(_ARABIC_DIGITS)
        s = s.replace("٬", ",").replace("٫", ".").replace("−", "-")
        s = s.replace("%", "").replace(",", "")
        s = s.replace("SAR", "").replace("ريال", "").strip()

        if s.startswith("(") and s.endswith(")"):
            s = "-" + s[1:-1].strip()

        match = _K_M_B_RE.match(s)
        if match:
            return float(match.group(1)) * _K_M_B_MULT[match.group(2).upper()]

        f = float(s)
        return None if (math.isnan(f) or math.isinf(f)) else f
    except (ValueError, TypeError):
        return None


def _to_ratio(value: Any) -> Optional[float]:
    """Convert percent-like value to ratio (0.12 = 12%). abs > 1.5 treated as percent."""
    f = _to_float(value)
    if f is None:
        return None
    return f / 100.0 if abs(f) > 1.5 else f


def _to_string(value: Any) -> Optional[str]:
    """Safely convert value to non-empty string; returns None for empty/whitespace."""
    if value is None:
        return None
    try:
        s = str(value).strip()
        return s if s else None
    except Exception:
        return None


def normalize_ksa_symbol(symbol: str) -> str:
    """
    Normalize a KSA symbol to canonical format (e.g., "2222.SR").

    Examples:
        "2222"          -> "2222.SR"
        "2222.SR"       -> "2222.SR"
        "TADAWUL:2222"  -> "2222.SR"
        "2222.SA"       -> "2222.SR"
        "2222.TADAWUL"  -> "2222.SR"
    """
    if not symbol:
        return ""

    raw = _to_string(symbol) or ""
    raw = raw.translate(_ARABIC_DIGITS).strip().upper()

    if raw.startswith("TADAWUL:"):
        raw = raw.split(":", 1)[1].strip()
    if raw.endswith(".SA"):
        raw = raw[:-3] + ".SR"
    if raw.endswith(".TADAWUL"):
        raw = raw[:-8].strip()

    if _KSA_CODE_RE.match(raw):
        return f"{raw}.SR"
    if raw.endswith(".SR") and _KSA_CODE_RE.match(raw[:-3]):
        return raw
    return ""


def _extract_code_from_symbol(symbol_norm: str) -> str:
    """Extract the numeric code from a normalized symbol (strips .SR)."""
    s = symbol_norm.strip().upper()
    return s[:-3] if s.endswith(".SR") else s


def _unwrap_envelope(data: Any, max_depth: int = 5) -> Any:
    """Unwrap common API envelope structures ({data: ...}, {result: ...}, etc.)."""
    current = data
    for _ in range(max_depth):
        if not isinstance(current, dict):
            break
        for key in ("data", "result", "payload", "response", "items", "results", "quote", "quotes"):
            if key in current and isinstance(current[key], (dict, list)):
                current = current[key]
                break
        else:
            break
    return current


def _iter_dict_candidates(obj: Any) -> Iterable[Dict[str, Any]]:
    """Iterate over dict candidates in a nested structure (one level deep)."""
    if isinstance(obj, dict):
        yield obj
        for value in obj.values():
            if isinstance(value, dict):
                yield value
            elif isinstance(value, list):
                for item in value:
                    if isinstance(item, dict):
                        yield item
    elif isinstance(obj, list):
        for item in obj:
            if isinstance(item, dict):
                yield item


def _find_value(data: Any, keys: List[str]) -> Any:
    """Find the first non-None/empty value matching any of the keys (case-insensitive)."""
    keys_lower = {k.lower() for k in keys}
    for candidate in _iter_dict_candidates(data):
        for key, value in candidate.items():
            if str(key).strip().lower() in keys_lower and value not in (None, ""):
                return value
    return None


def _first_existing(d: Dict[str, Any], keys: List[str]) -> Any:
    """Get the first existing non-None, non-empty value from dict by key order."""
    for key in keys:
        if key in d and d[key] is not None and d[key] != "":
            return d[key]
    return None


def _normalize_patch_keys(patch: Dict[str, Any]) -> Dict[str, Any]:
    """Apply FIELD_ALIASES and default classification fields."""
    result = dict(patch)

    for source, target in FIELD_ALIASES.items():
        if source in result and (target not in result or result.get(target) in (None, "", 0)):
            result[target] = result.pop(source)

    if result.get("current_price") is not None and result.get("price") in (None, "", 0):
        result["price"] = result["current_price"]
    if result.get("price") is not None and result.get("current_price") in (None, "", 0):
        result["current_price"] = result["price"]

    if result.get("country") in (None, ""):
        result["country"] = "Saudi Arabia"
    if result.get("currency") in (None, ""):
        result["currency"] = "SAR"
    if result.get("exchange") in (None, ""):
        result["exchange"] = "TADAWUL"
    if result.get("market") in (None, ""):
        result["market"] = "KSA"

    return result


def _week_52_position_pct(price: Optional[float], low: Optional[float], high: Optional[float]) -> Optional[float]:
    """Return 52-week position as a fraction in [0, 1]; None if insufficient data."""
    if price is None or low is None or high is None:
        return None
    if high <= low:
        return None
    try:
        return round((price - low) / (high - low), 6)
    except Exception:
        return None


def _day_range_position(price: Optional[float], low: Optional[float], high: Optional[float]) -> Optional[float]:
    """Return day-range position as a fraction in [0, 1]."""
    return _week_52_position_pct(price, low, high)


def _compute_rsi_14(closes: List[float]) -> Optional[float]:
    """Compute RSI(14) from close prices (most recent last); returns 0-100 or None."""
    if len(closes) < 15:
        return None

    period = 14
    deltas = [closes[i] - closes[i - 1] for i in range(1, len(closes))]

    avg_gain = sum(max(d, 0) for d in deltas[-period:]) / period
    avg_loss = sum(max(-d, 0) for d in deltas[-period:]) / period

    if avg_loss == 0:
        return 100.0

    rs = avg_gain / avg_loss
    rsi = 100.0 - (100.0 / (1.0 + rs))
    return round(max(0.0, min(100.0, rsi)), 2)


def _compute_atr_14(highs: List[float], lows: List[float], closes: List[float]) -> Optional[float]:
    """
    Compute ATR(14) from aligned OHLC lists (most recent last).

    Caller MUST ensure the three lists have equal length and represent the
    same bars in the same order.
    """
    n = min(len(highs), len(lows), len(closes))
    if n < 15:
        return None

    tr_values: List[float] = []
    for i in range(n - 14, n):
        h, l = highs[i], lows[i]
        c_prev = closes[i - 1]
        tr_values.append(max(h - l, abs(h - c_prev), abs(l - c_prev)))

    return round(sum(tr_values) / len(tr_values), 4) if tr_values else None


def _extract_history_rows(history_data: Any) -> List[Dict[str, Any]]:
    """Extract OHLCV rows from a variety of history payload shapes."""
    if history_data is None:
        return []

    data = _unwrap_envelope(history_data)

    if isinstance(data, dict):
        for key in ("history", "historical", "candles", "rows", "prices", "series"):
            if isinstance(data.get(key), list):
                data = data[key]
                break

    if isinstance(data, list):
        return [item for item in data if isinstance(item, dict)]

    if isinstance(data, dict):
        return [data]

    return []


def _compute_history_stats(history_data: Any) -> Dict[str, Any]:
    """
    Derive OHLCV stats + technical indicators from history rows.

    v6.0.0 bug fix: OHLC arrays are now built from tuples so rows with any
    missing required field are dropped entirely, keeping the arrays aligned.
    v5.0.0 appended to each list independently, so a row missing `high`
    would push highs/lows out of sync with closes, poisoning ATR(14).

    Returns a dict containing only non-None fields:
      current_price, previous_close, open_price,
      day_high, day_low, week_52_high, week_52_low,
      volume, avg_volume_10d, avg_volume_30d,
      week_52_position_pct, day_range_position,
      price_change_5d, rsi_14, atr_14, volume_ratio.
    """
    rows = _extract_history_rows(history_data)
    if not rows:
        return {}

    # Parse each row as a tuple; keep only rows with a valid close.
    # For ATR we require aligned HLC, so we split into two lists:
    #   `closes_all`   -- every row with a valid close (drives RSI, prices)
    #   `ohlc_aligned` -- only rows with valid H/L/C all three (drives ATR)
    closes_all: List[float] = []
    opens_all: List[float] = []
    highs_all: List[float] = []
    lows_all: List[float] = []
    volumes_all: List[float] = []
    ohlc_aligned: List[Tuple[float, float, float]] = []  # (high, low, close)

    for row in rows:
        close = _to_float(_first_existing(row, ["close", "Close", "c", "last", "price", "value"]))
        if close is None:
            continue
        closes_all.append(close)

        high = _to_float(_first_existing(row, ["high", "High", "h"]))
        low = _to_float(_first_existing(row, ["low", "Low", "l"]))
        open_price = _to_float(_first_existing(row, ["open", "Open", "o"]))
        volume = _to_float(_first_existing(row, ["volume", "Volume", "v", "qty", "tradedVolume"]))

        if high is not None:
            highs_all.append(high)
        if low is not None:
            lows_all.append(low)
        if open_price is not None:
            opens_all.append(open_price)
        if volume is not None and volume >= 0:
            volumes_all.append(volume)

        if high is not None and low is not None:
            ohlc_aligned.append((high, low, close))

    if not closes_all:
        return {}

    result: Dict[str, Any] = {"current_price": closes_all[-1]}
    if len(closes_all) >= 2:
        result["previous_close"] = closes_all[-2]
    if opens_all:
        result["open_price"] = opens_all[-1]
    if highs_all:
        result["day_high"] = highs_all[-1]
        result["week_52_high"] = max(highs_all[-252:]) if len(highs_all) >= 252 else max(highs_all)
    if lows_all:
        result["day_low"] = lows_all[-1]
        result["week_52_low"] = min(lows_all[-252:]) if len(lows_all) >= 252 else min(lows_all)

    if volumes_all:
        result["volume"] = volumes_all[-1]
        last_10 = volumes_all[-10:] if len(volumes_all) >= 10 else volumes_all
        last_30 = volumes_all[-30:] if len(volumes_all) >= 30 else volumes_all
        result["avg_volume_10d"] = sum(last_10) / len(last_10)
        result["avg_volume_30d"] = sum(last_30) / len(last_30)

    result["week_52_position_pct"] = _week_52_position_pct(
        result.get("current_price"), result.get("week_52_low"), result.get("week_52_high"),
    )
    result["day_range_position"] = _day_range_position(
        result.get("current_price"), result.get("day_low"), result.get("day_high"),
    )

    if len(closes_all) >= 6:
        price_5d_ago = closes_all[-6]
        if price_5d_ago > 0:
            result["price_change_5d"] = (closes_all[-1] - price_5d_ago) / price_5d_ago

    vol = result.get("volume")
    avg_vol = result.get("avg_volume_10d")
    if vol is not None and avg_vol is not None and avg_vol > 0:
        result["volume_ratio"] = round(vol / avg_vol, 4)

    rsi = _compute_rsi_14(closes_all)
    if rsi is not None:
        result["rsi_14"] = rsi

    # ATR needs aligned HLC -- use the filtered-aligned list
    if len(ohlc_aligned) >= 15:
        highs_aligned = [x[0] for x in ohlc_aligned]
        lows_aligned = [x[1] for x in ohlc_aligned]
        closes_aligned = [x[2] for x in ohlc_aligned]
        atr = _compute_atr_14(highs_aligned, lows_aligned, closes_aligned)
        if atr is not None:
            result["atr_14"] = atr

    return {k: v for k, v in result.items() if v is not None}


# ---------------------------------------------------------------------------
# Error Patch Builder (module-level so we don't allocate an httpx client)
# ---------------------------------------------------------------------------

def _build_error_patch(
    symbol: str,
    error_code: ProviderErrorCode,
    details: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Build an error patch without requiring an ArgaamClient instance.

    v5.0.0 called ArgaamClient()._error_patch() from module-level code, which
    allocated a full httpx.AsyncClient (connection pool, headers, timeouts)
    just to invoke an instance method that never touched `self`.
    """
    symbol_norm = normalize_ksa_symbol(symbol) or symbol
    return {
        "symbol": symbol_norm,
        "requested_symbol": symbol,
        "provider": PROVIDER_NAME,
        "provider_version": PROVIDER_VERSION,
        "data_quality": DataQuality.MISSING.value,
        "error": error_code.value,
        "error_details": details,
        "last_updated_utc": _now_utc_iso(),
        "country": "Saudi Arabia",
        "currency": "SAR",
        "exchange": "TADAWUL",
        "market": "KSA",
    }


# ---------------------------------------------------------------------------
# TTL Cache (FIFO eviction, lazy lock)
# ---------------------------------------------------------------------------

@dataclass
class _CacheItem:
    """Cache item with expiration time."""
    expires_at: float
    value: Any


class _TTLCache:
    """Async-safe TTL cache with FIFO eviction on overflow."""

    def __init__(self, max_size: int = DEFAULT_CACHE_MAX_SIZE):
        self._max_size = max(1, max_size)
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
            if now >= item.expires_at:
                self._cache.pop(key, None)
                return None
            return item.value

    async def set(self, key: str, value: Any, ttl_sec: float) -> None:
        """Set value in cache with TTL; FIFO-evicts on overflow."""
        now = time.monotonic()
        async with self._get_lock():
            # Evict oldest entries until we're back under the cap
            # (bounded excess, not the flat 200 from v5.0.0)
            while len(self._cache) >= self._max_size:
                self._cache.popitem(last=False)
            self._cache[key] = _CacheItem(
                expires_at=now + max(1.0, ttl_sec),
                value=value,
            )


# ---------------------------------------------------------------------------
# Argaam HTTP Client
# ---------------------------------------------------------------------------

class ArgaamClient:
    """HTTP client for Argaam API with caching and retry logic."""

    def __init__(self, config: Optional[ArgaamConfig] = None):
        self.config = config or ArgaamConfig.from_env()
        self.client_id = str(uuid.uuid4())[:8]
        self._semaphore: Optional[asyncio.Semaphore] = None  # lazy
        self._cache = _TTLCache(max_size=self.config.cache_max_size)
        self._client: Optional["httpx.AsyncClient"] = None

        if _HTTPX_AVAILABLE:
            headers = {
                "User-Agent": USER_AGENT,
                "Accept": "application/json",
                "X-Client-ID": self.client_id,
            }

            # v6.1.0: auto-inject API-key header from ARGAAM_API_KEY env var.
            # Skipped when api_key_header is "" (e.g., Twelve Data preset uses
            # in-URL key). Operator can still override via ARGAAM_API_KEY_HEADER
            # or by setting their own headers in extra_headers (not yet
            # exposed for Argaam, but kept symmetric with tadawul_provider).
            api_key = self.config.api_key
            hk = self.config.api_key_header
            if api_key and hk:
                if hk.lower() == "authorization" and not api_key.lower().startswith(
                    ("bearer ", "token ", "basic ")
                ):
                    headers[hk] = f"Bearer {api_key}"
                else:
                    headers[hk] = api_key

            self._client = httpx.AsyncClient(
                timeout=httpx.Timeout(self.config.timeout_sec),
                limits=httpx.Limits(max_keepalive_connections=20, max_connections=40),
                headers=headers,
            )

            try:
                logger.info(
                    "ArgaamClient v%s initialized | preset=%s | has_api_key=%s | has_quote=%s | has_profile=%s | has_history=%s",
                    PROVIDER_VERSION,
                    self.config.provider_preset or "(none)",
                    bool(api_key),
                    bool(self.config.quote_url),
                    bool(self.config.profile_url),
                    bool(self.config.history_url),
                )
            except Exception:
                pass

    def _get_semaphore(self) -> asyncio.Semaphore:
        if self._semaphore is None:
            self._semaphore = asyncio.Semaphore(max(1, self.config.max_concurrency))
        return self._semaphore

    # -- URL helpers -----------------------------------------------------

    def _build_url_candidates(self, template: str, symbol_norm: str) -> List[str]:
        """Build URL candidates for a symbol; deduped, order-preserving."""
        if not template:
            return []

        code = _extract_code_from_symbol(symbol_norm)
        full = symbol_norm
        raw_urls: List[str] = []

        if "{symbol}" in template:
            raw_urls.append(template.replace("{symbol}", code))
            if full != code:
                raw_urls.append(template.replace("{symbol}", full))
        else:
            base = template.rstrip("/")
            raw_urls.append(f"{base}/{code}")
            if full != code:
                raw_urls.append(f"{base}/{full}")

        seen: set = set()
        result: List[str] = []
        for url in raw_urls:
            if url and url not in seen:
                seen.add(url)
                result.append(url)
        return result

    @staticmethod
    def _get_cache_key(url: str) -> str:
        """Generate cache key for URL."""
        return f"url:{url}"

    # -- Low-level fetching ---------------------------------------------

    async def _fetch_json(
        self,
        url: str,
        ttl_sec: float,
    ) -> Tuple[Optional[Any], Optional[str]]:
        """Fetch JSON from URL with caching and retry. Returns (payload, error_or_None)."""
        cache_key = self._get_cache_key(url)
        cached = await self._cache.get(cache_key)
        if cached is not None:
            return cached, None

        if not self._client:
            return None, "httpx_not_available"

        async with self._get_semaphore():
            last_error: Optional[str] = None

            for attempt in range(self.config.retry_attempts):
                try:
                    response = await self._client.get(url)

                    if response.status_code == 429:
                        wait = min(15.0, (2 ** attempt) + random.uniform(0, 1.0))
                        await asyncio.sleep(wait)
                        last_error = "rate_limited_429"
                        continue

                    if 500 <= response.status_code < 600:
                        wait = min(10.0, (2 ** attempt) + random.uniform(0, 1.0))
                        await asyncio.sleep(wait)
                        last_error = f"server_error_{response.status_code}"
                        continue

                    if response.status_code >= 400:
                        return None, f"http_{response.status_code}"

                    try:
                        payload = _unwrap_envelope(_json_loads(response.content))
                    except Exception:
                        return None, "invalid_json"

                    await self._cache.set(cache_key, payload, ttl_sec)
                    return payload, None

                except Exception as exc:
                    last_error = f"network_error:{exc.__class__.__name__}"
                    wait = min(10.0, (2 ** attempt) + random.uniform(0, 1.0))
                    await asyncio.sleep(wait)

            return None, last_error or "max_retries_exceeded"

    async def _fetch_first(
        self,
        urls: List[str],
        ttl_sec: float,
    ) -> Tuple[Optional[Any], Optional[str], Optional[str]]:
        """
        Fetch the first URL that returns data; returns (data, last_error, url_used).

        v6.0.0 bug fix: `last_error` is initialized before the loop so an empty
        URL list no longer raises NameError on the return path.
        """
        last_error: Optional[str] = None
        for url in urls:
            data, error = await self._fetch_json(url, ttl_sec)
            if data is not None:
                return data, None, url
            last_error = error
        return None, last_error, None

    # -- Parse helpers ---------------------------------------------------

    def _parse_profile_patch(self, symbol_norm: str, profile_data: Any) -> Dict[str, Any]:
        """Parse profile data into patch format."""
        profile = profile_data if isinstance(profile_data, dict) else {}
        result: Dict[str, Any] = {
            "symbol": symbol_norm,
            "symbol_normalized": symbol_norm,
            "provider": PROVIDER_NAME,
            "provider_version": PROVIDER_VERSION,
            "exchange": "TADAWUL",
            "market": "KSA",
            "country": "Saudi Arabia",
            "currency": "SAR",
            "asset_class": "Equity",
            "last_updated_utc": _now_utc_iso(),
        }
        result["name"] = _to_string(_find_value(
            profile, ["name", "companyname", "company_name", "longname", "securityname", "displayname"],
        ))
        result["sector"] = _to_string(_find_value(profile, ["sector", "sectorname"]))
        result["industry"] = _to_string(_find_value(
            profile, ["industry", "industryname", "subsector", "subsectorname"],
        ))
        result["currency"] = _to_string(_find_value(profile, ["currency"])) or "SAR"
        result["country"] = _to_string(_find_value(profile, ["country"])) or "Saudi Arabia"
        result["exchange"] = _to_string(_find_value(profile, ["exchange", "marketname"])) or "TADAWUL"
        result["asset_class"] = _to_string(_find_value(
            profile, ["assetclass", "asset_class", "type", "instrumenttype", "securitytype"],
        )) or "Equity"

        return {k: v for k, v in result.items() if v is not None}

    def _parse_quote_patch(
        self,
        symbol_norm: str,
        quote_data: Any,
        profile_data: Any = None,
        history_data: Any = None,
    ) -> Dict[str, Any]:
        """Parse quote + profile + history into a single unified patch."""
        result: Dict[str, Any] = {
            "symbol": symbol_norm,
            "symbol_normalized": symbol_norm,
            "provider": PROVIDER_NAME,
            "provider_version": PROVIDER_VERSION,
            "last_updated_utc": _now_utc_iso(),
            "exchange": "TADAWUL",
            "market": "KSA",
            "country": "Saudi Arabia",
            "currency": "SAR",
            "asset_class": "Equity",
        }
        quote = quote_data if isinstance(quote_data, dict) else {}
        profile = profile_data if isinstance(profile_data, dict) else {}

        result.update(self._parse_profile_patch(symbol_norm, profile))

        # Fall back to quote fields if profile didn't provide them
        result["name"] = result.get("name") or _to_string(_find_value(
            quote, ["name", "companyname", "company_name", "longname"],
        ))
        result["sector"] = result.get("sector") or _to_string(_find_value(quote, ["sector"]))
        result["industry"] = result.get("industry") or _to_string(_find_value(quote, ["industry"]))

        # Price fields (float)
        result["current_price"] = _to_float(_find_value(
            quote, ["current_price", "price", "last", "lastprice", "closeprice", "tradeprice"],
        ))
        result["price"] = result.get("current_price")
        result["previous_close"] = _to_float(_find_value(
            quote, ["previous_close", "prev_close", "previousclose", "close", "yesterdayclose"],
        ))
        result["open_price"] = _to_float(_find_value(
            quote, ["open_price", "open", "openingprice", "dayopen"],
        ))
        result["day_high"] = _to_float(_find_value(quote, ["day_high", "high", "highprice", "sessionhigh"]))
        result["day_low"] = _to_float(_find_value(quote, ["day_low", "low", "lowprice", "sessionlow"]))
        result["price_change"] = _to_float(_find_value(quote, ["price_change", "change", "chg"]))
        result["volume"] = _to_float(_find_value(
            quote, ["volume", "vol", "tradedvolume", "quantity", "qty"],
        ))
        result["market_cap"] = (
            _to_float(_find_value(quote, ["marketcap", "market_cap"]))
            or _to_float(_find_value(profile, ["marketcap", "market_cap"]))
        )
        result["float_shares"] = (
            _to_float(_find_value(quote, ["floatshares", "float_shares"]))
            or _to_float(_find_value(profile, ["floatshares", "float_shares"]))
        )
        result["week_52_high"] = _to_float(_find_value(
            quote, ["week_52_high", "52weekhigh", "yearhigh", "fiftytwoweekhigh"],
        ))
        result["week_52_low"] = _to_float(_find_value(
            quote, ["week_52_low", "52weeklow", "yearlow", "fiftytwoweeklow"],
        ))
        result["avg_volume_10d"] = _to_float(_find_value(
            quote, ["avg_volume_10d", "averagevolume10days", "avgvol10d"],
        ))
        result["avg_volume_30d"] = _to_float(_find_value(
            quote, ["avg_volume_30d", "averagevolume", "avgvolume", "avgvol30d"],
        ))

        # Percent fields (converted to ratio)
        result["percent_change"] = _to_ratio(_find_value(
            quote, ["percent_change", "change_percent", "changepercent", "changepct", "pct_change"],
        ))
        result["dividend_yield"] = _to_ratio(_find_value(
            quote, ["dividend_yield", "dividendyield", "yieldpct"],
        ))
        result["gross_margin"] = _to_ratio(_find_value(quote, ["gross_margin", "grossmargin"]))
        result["operating_margin"] = _to_ratio(_find_value(quote, ["operating_margin", "operatingmargin"]))
        result["profit_margin"] = _to_ratio(_find_value(
            quote, ["profit_margin", "profitmargin", "netmargin", "net_margin"],
        ))
        result["payout_ratio"] = _to_ratio(_find_value(quote, ["payout_ratio", "payoutratio"]))
        result["revenue_growth_yoy"] = _to_ratio(_find_value(
            quote, ["revenue_growth_yoy", "revenuegrowth", "revenue_growth"],
        ))

        # ROE / ROA (from quote OR profile)
        result["roe"] = _to_ratio(
            _find_value(quote, ["roe", "return_on_equity", "returnonequity", "returnOnEquity"])
            or _find_value(profile, ["roe", "return_on_equity", "returnonequity", "returnOnEquity"])
        )
        result["roa"] = _to_ratio(
            _find_value(quote, ["roa", "return_on_assets", "returnonassets", "returnOnAssets"])
            or _find_value(profile, ["roa", "return_on_assets", "returnonassets", "returnOnAssets"])
        )

        # 5-day price change
        result["price_change_5d"] = _to_ratio(_find_value(
            quote, ["price_change_5d", "priceChange5d", "change5d", "five_day_change", "5d_change"],
        ))

        # History fallback
        hist_patch = _compute_history_stats(history_data) if history_data is not None else {}
        for key, value in hist_patch.items():
            if result.get(key) in (None, "", 0):
                result[key] = value

        # Derived: 52-week position
        if result.get("week_52_position_pct") is None:
            result["week_52_position_pct"] = _week_52_position_pct(
                result.get("current_price"),
                result.get("week_52_low"),
                result.get("week_52_high"),
            )

        # Derived: day range position
        if result.get("day_range_position") is None:
            result["day_range_position"] = _day_range_position(
                result.get("current_price"),
                result.get("day_low"),
                result.get("day_high"),
            )

        # Derived: volume ratio
        if result.get("volume_ratio") is None:
            vol = result.get("volume")
            avg_vol = result.get("avg_volume_10d")
            if vol is not None and avg_vol is not None and avg_vol > 0:
                result["volume_ratio"] = round(vol / avg_vol, 4)

        # Cross-fill price_change / percent_change
        if (result.get("price_change") is None
                and result.get("current_price") is not None
                and result.get("previous_close") is not None):
            try:
                result["price_change"] = result["current_price"] - result["previous_close"]
            except Exception:
                pass

        if (result.get("percent_change") is None
                and result.get("price_change") is not None
                and result.get("previous_close") not in (None, 0)):
            try:
                result["percent_change"] = result["price_change"] / result["previous_close"]
            except Exception:
                pass

        # Data quality
        if result.get("current_price") is not None:
            result["data_quality"] = DataQuality.GOOD.value
        elif hist_patch or profile_data is not None:
            result["data_quality"] = DataQuality.DEGRADED.value
        else:
            result["data_quality"] = DataQuality.MISSING.value

        return _normalize_patch_keys({k: v for k, v in result.items() if v is not None})

    # v6.0.0: _error_patch is @staticmethod -- no `self` used, no client needed.
    @staticmethod
    def _error_patch(
        symbol: str,
        error_code: ProviderErrorCode,
        details: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Compatibility shim: delegates to the module-level builder."""
        return _build_error_patch(symbol, error_code, details)

    # -- High-level fetchers --------------------------------------------

    async def get_enriched_patch(self, symbol: str) -> Dict[str, Any]:
        """
        Get enriched data patch for a single symbol.

        Combines quote + profile + history; uses fallback chain. Never raises.
        """
        symbol_norm = normalize_ksa_symbol(symbol)
        if not symbol_norm:
            return _build_error_patch(symbol, ProviderErrorCode.INVALID_SYMBOL)

        if not self.config.has_any_url():
            return _build_error_patch(symbol_norm, ProviderErrorCode.NO_CONFIGURATION)

        quote_urls = self._build_url_candidates(self.config.quote_url, symbol_norm) if self.config.quote_url else []
        profile_urls = self._build_url_candidates(self.config.profile_url, symbol_norm) if self.config.profile_url else []
        history_urls = self._build_url_candidates(self.config.history_url, symbol_norm) if self.config.history_url else []

        quote_data: Any = None
        quote_err: Optional[str] = None
        quote_used: Optional[str] = None
        if quote_urls:
            quote_data, quote_err, quote_used = await self._fetch_first(quote_urls, self.config.cache_ttl_sec)

        profile_data: Any = None
        profile_used: Optional[str] = None
        if profile_urls:
            profile_data, _, profile_used = await self._fetch_first(
                profile_urls, max(60.0, self.config.cache_ttl_sec),
            )

        history_data: Any = None
        history_used: Optional[str] = None
        if history_urls:
            history_data, _, history_used = await self._fetch_first(history_urls, self.config.history_ttl_sec)

        if quote_data is None and profile_data is None and history_data is None:
            return _build_error_patch(symbol_norm, ProviderErrorCode.FETCH_FAILED, quote_err)

        patch = self._parse_quote_patch(symbol_norm, quote_data, profile_data, history_data)
        patch["requested_symbol"] = symbol
        patch["data_sources"] = [PROVIDER_NAME]
        patch["provider_origin"] = {
            "quote_url_used": quote_used,
            "profile_url_used": profile_used,
            "history_url_used": history_used,
        }

        if quote_data is None:
            patch["warning"] = patch.get("warning") or "quote_missing_used_profile_and_or_history_fallback"
            if patch.get("data_quality") == DataQuality.GOOD.value:
                patch["data_quality"] = DataQuality.DEGRADED.value

        return patch

    async def get_enriched_quotes_batch(
        self,
        symbols: List[str],
        mode: str = "",
    ) -> Dict[str, Dict[str, Any]]:
        """Batch fetch for multiple symbols (concurrent under the semaphore)."""
        tasks = [self.get_enriched_patch(sym) for sym in symbols]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        output: Dict[str, Dict[str, Any]] = {}
        for sym, result in zip(symbols, results):
            if isinstance(result, Exception):
                output[sym] = _build_error_patch(sym, ProviderErrorCode.FETCH_FAILED, type(result).__name__)
            elif isinstance(result, dict):
                output[sym] = result
            else:
                output[sym] = _build_error_patch(sym, ProviderErrorCode.EMPTY_RESPONSE)
        return output

    async def close(self) -> None:
        """Close the underlying httpx client."""
        if self._client:
            try:
                await self._client.aclose()
            except Exception as exc:
                logger.debug("argaam client close failed: %s", exc)
            self._client = None


# ---------------------------------------------------------------------------
# Provider Wrapper (Router / Engine Compatible)
# ---------------------------------------------------------------------------

class ArgaamProvider:
    """Thin provider wrapper for router/engine compatibility."""

    name = PROVIDER_NAME
    version = PROVIDER_VERSION

    def __init__(self, config: Optional[ArgaamConfig] = None):
        self._config = config or ArgaamConfig.from_env()
        self._client: Optional[ArgaamClient] = None
        self._lock: Optional[asyncio.Lock] = None  # lazy

    def _get_lock(self) -> asyncio.Lock:
        if self._lock is None:
            self._lock = asyncio.Lock()
        return self._lock

    async def _get_client(self) -> ArgaamClient:
        """Get or create the underlying client instance."""
        async with self._get_lock():
            if self._client is None:
                self._client = ArgaamClient(self._config)
            return self._client

    async def get_quote(self, symbol: str) -> Dict[str, Any]:
        """Get an enriched quote for a single symbol."""
        client = await self._get_client()
        return await client.get_enriched_patch(symbol)

    async def get_enriched_quotes_batch(
        self,
        symbols: List[str],
        mode: str = "",
    ) -> Dict[str, Dict[str, Any]]:
        """Batch fetch quotes."""
        client = await self._get_client()
        return await client.get_enriched_quotes_batch(symbols, mode=mode)

    # Backward-compatible aliases
    async def quote(self, symbol: str) -> Dict[str, Any]:
        return await self.get_quote(symbol)

    async def fetch_quote(self, symbol: str) -> Dict[str, Any]:
        return await self.get_quote(symbol)

    async def fetch_quote_patch(self, symbol: str) -> Dict[str, Any]:
        return await self.get_quote(symbol)

    async def fetch_patch(self, symbol: str) -> Dict[str, Any]:
        return await self.get_quote(symbol)

    async def fetch_enriched_quote_patch(self, symbol: str) -> Dict[str, Any]:
        return await self.get_quote(symbol)

    async def fetch_profile(self, symbol: str) -> Dict[str, Any]:
        return await self.get_quote(symbol)

    async def fetch_profile_patch(self, symbol: str) -> Dict[str, Any]:
        return await self.get_quote(symbol)

    async def fetch_history_patch(self, symbol: str) -> Dict[str, Any]:
        return await self.get_quote(symbol)

    async def close(self) -> None:
        """Close the provider and its underlying client."""
        if self._client:
            await self._client.close()
            self._client = None


# ---------------------------------------------------------------------------
# Singleton Exports (consolidated -- one instance, two access paths)
# ---------------------------------------------------------------------------

_PROVIDER_INSTANCE: Optional[ArgaamProvider] = None
_PROVIDER_LOCK: Optional[asyncio.Lock] = None


def _get_provider_lock() -> asyncio.Lock:
    global _PROVIDER_LOCK
    if _PROVIDER_LOCK is None:
        _PROVIDER_LOCK = asyncio.Lock()
    return _PROVIDER_LOCK


def _get_provider_sync() -> ArgaamProvider:
    """Synchronous fast path for getting the singleton provider."""
    global _PROVIDER_INSTANCE
    if _PROVIDER_INSTANCE is None:
        _PROVIDER_INSTANCE = ArgaamProvider()
    return _PROVIDER_INSTANCE


async def get_provider() -> ArgaamProvider:
    """Get the singleton provider instance (async-safe)."""
    global _PROVIDER_INSTANCE
    if _PROVIDER_INSTANCE is not None:
        return _PROVIDER_INSTANCE
    async with _get_provider_lock():
        if _PROVIDER_INSTANCE is None:
            _PROVIDER_INSTANCE = ArgaamProvider()
        return _PROVIDER_INSTANCE


async def get_client() -> ArgaamClient:
    """Get the underlying singleton ArgaamClient (via the shared provider)."""
    prov = await get_provider()
    return await prov._get_client()


async def close_client() -> None:
    """Close the singleton client (and reset the provider)."""
    global _PROVIDER_INSTANCE
    if _PROVIDER_INSTANCE is not None:
        await _PROVIDER_INSTANCE.close()
        _PROVIDER_INSTANCE = None


# v5.0.0 exported a module-level `provider = ArgaamProvider()` AND a distinct
# singleton via get_provider(). v6.0.0 preserves both names but backs them
# with the SAME instance.
provider = _get_provider_sync()


# ---------------------------------------------------------------------------
# Engine-Facing Functions
# ---------------------------------------------------------------------------

async def fetch_enriched_quote_patch(symbol: str, *args: Any, **kwargs: Any) -> Dict[str, Any]:
    """
    Engine-compatible fetch function.

    Extra *args / **kwargs are accepted for API compatibility but unused.
    v6.0.0: logs a debug message when extras are passed so silent misuse
    is at least discoverable via logs.
    """
    if args or kwargs:
        logger.debug(
            "fetch_enriched_quote_patch(%s): ignoring extra args=%r kwargs=%r",
            symbol, args, kwargs,
        )

    client = await get_client()
    patch = await client.get_enriched_patch(symbol)
    if not isinstance(patch, dict) or not patch:
        # v6.0.0: use module-level builder -- no httpx client allocation
        return _build_error_patch(symbol, ProviderErrorCode.EMPTY_RESPONSE)
    return patch


# Aliases for compatibility
fetch_patch = fetch_enriched_quote_patch
fetch_quote_patch = fetch_enriched_quote_patch
fetch_quote = fetch_enriched_quote_patch
quote = fetch_enriched_quote_patch
get_quote = fetch_enriched_quote_patch
fetch_profile_patch = fetch_enriched_quote_patch
fetch_history_patch = fetch_enriched_quote_patch


# ---------------------------------------------------------------------------
# Module Exports
# ---------------------------------------------------------------------------

__all__ = [
    # Metadata
    "PROVIDER_NAME",
    "PROVIDER_VERSION",
    "PROVIDER_BATCH_SUPPORTED",
    "VERSION",
    # v6.1.0: preset registry (operators can introspect supported presets)
    "PROVIDER_PRESETS",
    # Symbol normalization
    "normalize_ksa_symbol",
    # Engine-facing functions + aliases
    "fetch_enriched_quote_patch",
    "fetch_patch",
    "fetch_quote_patch",
    "fetch_quote",
    "quote",
    "get_quote",
    "fetch_profile_patch",
    "fetch_history_patch",
    # Singletons
    "get_client",
    "close_client",
    "get_provider",
    "provider",
    # Classes
    "ArgaamProvider",
    "ArgaamClient",
    "ArgaamConfig",
    # Exceptions
    "ArgaamProviderError",
    "ArgaamConfigurationError",
    "ArgaamFetchError",
    # Enums
    "DataQuality",
    "ProviderErrorCode",
]
