#!/usr/bin/env python3
"""
core/yahoo_chart_provider.py
================================================================================
YAHOO CHART COMPATIBILITY SHIM -- v5.1.0
================================================================================
SAFE * STARTUP-FRIENDLY * CANONICAL-DELEGATING * HISTORY-AWARE *
COMMODITY/FX-TOLERANT * ROUTE/ENGINE COMPATIBLE * PAYLOAD-RECOVERY ENHANCED

v5.1.0 changes (what moved from v5.0.0)
---------------------------------------
- FIX: missing ``_T = TypeVar("_T")`` definition. v5.0.0 imported
  ``TypeVar`` from ``typing`` and referenced ``_T`` in the type
  annotations of ``SingleFlight.execute`` and ``FullJitterBackoff.run``
  but never assigned it. Thanks to ``from __future__ import annotations``
  the module imported fine, but any downstream tool calling
  ``typing.get_type_hints()`` on those methods raised
  ``NameError: name '_T' is not defined`` -- breaking static
  type-checking, pydantic v2 analysis, and FastAPI reflection.

- FIX: ``SingleFlight.execute`` serialized ALL execute() calls behind
  any single in-flight request. v5.0.0 did ``return await future``
  INSIDE ``async with self._lock``, which meant a follower waiting on
  an existing key held the shared lock during its await, blocking new
  callers for unrelated keys until the leader's ``coro_fn`` finished.
  v5.1.0 releases the lock before awaiting the existing future.

- FIX: ``YahooChartProvider._dispatch`` mis-classified method results.
  v5.0.0 did ``kind = "history" if "history" in method_name else
  "quote"; if "patch" in method_name: kind = "patch"`` -- which meant
  ``fetch_history_patch`` was classified as "patch" even though its
  ``ShimFunction`` twin declares ``result_kind="history"`` and the
  canonical returns a list of rows. The mismatched kind caused
  ``ensure_shape`` to return a dict instead of a list, breaking
  callers that iterate the result. v5.1.0 decides kind by explicit
  precedence: history-indicating tokens first, then patch, then quote.

- CLEANUP: removed dead code that was imported/defined but never used:
  * ``orjson`` import block with ``_json_dumps``/``_json_loads`` shims
    (never called) and the ``_HAS_ORJSON`` flag (never read).
  * ``threading`` import (module uses ``asyncio.Lock`` throughout).
  * ``Iterable``, ``cast`` from typing (unused).
  * ``field`` from dataclasses (unused).
  Kept/reintroduced: ``sys`` (used by the self-discovery guard in
  ``ProviderCache._load``), ``lru_cache`` (used by ``_signature_params``),
  ``asdict`` and ``is_dataclass`` (used by ``_to_dict``), and all
  typing imports that are actually referenced.

- FIX: removed ``logging.basicConfig()`` side effect at import time.
  Library modules must not configure the root logger -- doing so
  overrides whatever logging setup the host application has configured.
  v5.1.0 only creates ``logger = logging.getLogger(__name__)``.

- ENHANCE: canonical import paths now include ``core.yahoo_chart_provider``
  (sibling path) and ``yahoo_chart_provider`` (bare path) in addition
  to the ``providers/`` subdir paths. This covers deployments that
  ship the real provider next to the shim rather than under a
  dedicated ``providers/`` package.

- SAFETY: ``ProviderCache._load`` now guards against self-discovery.
  If a canonical-path resolves to this very shim module (because the
  shim is deployed at a bare name that also appears in
  ``CANONICAL_IMPORT_PATHS``), the candidate is skipped. Without this
  guard the shim would find itself, treat its own ``ShimFunction``
  callables as canonical functions, and recurse on every call.

- FIX: ``ensure_shape`` now preserves ``error`` and ``status`` from
  the input payload. When a ``ShimFunction``'s ``fallback_factory``
  returned ``error_payload(...)`` for quote- or patch-kind calls,
  ``ensure_shape`` would pass it through ``normalize_quote_payload`` /
  ``normalize_patch_payload``, which stripped unrecognized top-level
  keys. The error surfaced to callers as ``status="ok"`` with no
  ``error`` field -- silencing the "canonical unavailable" signal.
  v5.1.0 reads the error/status BEFORE normalization and restores
  them afterward.

- Bump version: ``SHIM_VERSION = "5.1.0"``.

Preserved
---------
- All public symbols in ``__all__``.
- ShimFunction behavior (retry, circuit breaker, fallback factories).
- YahooChartProvider class surface with all dispatch methods.
- Payload normalization / history extraction / quote derivation logic.
- Environment variable names (``SHIM_YAHOO_*``).
================================================================================
"""

from __future__ import annotations

import asyncio
import inspect
import logging
import math
import os
import random
import re
import sys
import time
from dataclasses import asdict, dataclass, is_dataclass
from datetime import datetime, timedelta, timezone
from enum import Enum
from functools import lru_cache
from importlib import import_module
from typing import (
    Any,
    Awaitable,
    Callable,
    Dict,
    List,
    Optional,
    Sequence,
    Tuple,
    TypeVar,
    Union,
)

# TypeVar for SingleFlight / FullJitterBackoff generic return types.
_T = TypeVar("_T")

# =============================================================================
# Version
# =============================================================================

SHIM_VERSION = "5.1.0"
VERSION = SHIM_VERSION
PROVIDER_VERSION = SHIM_VERSION
DATA_SOURCE = "yahoo_chart"

# =============================================================================
# Canonical Import Paths
# =============================================================================

CANONICAL_IMPORT_PATHS = (
    "core.providers.yahoo_chart_provider",
    "providers.yahoo_chart_provider",
    "core.yahoo_chart_provider",
    "yahoo_chart_provider",
)

MIN_CANONICAL_VERSION = "0.4.0"

# =============================================================================
# Time Helpers
# =============================================================================

UTC = timezone.utc
RIYADH_TZ = timezone(timedelta(hours=3))


def _utc_iso(dt: Optional[datetime] = None) -> str:
    """Get UTC time in ISO format."""
    d = dt or datetime.now(UTC)
    if d.tzinfo is None:
        d = d.replace(tzinfo=UTC)
    return d.astimezone(UTC).isoformat()


def _riyadh_iso(dt: Optional[datetime] = None) -> str:
    """Get Riyadh time in ISO format."""
    d = dt or datetime.now(UTC)
    if d.tzinfo is None:
        d = d.replace(tzinfo=UTC)
    return d.astimezone(RIYADH_TZ).isoformat()


# =============================================================================
# Enums
# =============================================================================

class DataQuality(str, Enum):
    """Data quality levels."""
    EXCELLENT = "EXCELLENT"
    HIGH = "HIGH"
    MEDIUM = "MEDIUM"
    LOW = "LOW"
    STALE = "STALE"
    OK = "OK"
    PARTIAL = "PARTIAL"
    MISSING = "MISSING"
    ERROR = "ERROR"


class CircuitState(str, Enum):
    """Circuit breaker states."""
    CLOSED = "closed"
    HALF = "half_open"
    OPEN = "open"


# =============================================================================
# Constants
# =============================================================================

_TRUTHY = {"1", "true", "yes", "y", "on", "t"}
_FALSY = {"0", "false", "no", "n", "off", "f"}

# Field aliases
PRICE_FIELD_ALIASES = (
    "current_price", "price", "last_price", "last", "regularMarketPrice",
    "regular_market_price", "postMarketPrice", "preMarketPrice", "bid", "ask",
    "close", "last_close", "navPrice",
)

PREV_CLOSE_ALIASES = (
    "previous_close", "prev_close", "regularMarketPreviousClose",
    "chartPreviousClose", "previousClose",
)

OPEN_ALIASES = ("open", "regularMarketOpen")
HIGH_ALIASES = ("day_high", "high", "regularMarketDayHigh", "fiftyTwoWeekHigh", "dayHigh")
LOW_ALIASES = ("day_low", "low", "regularMarketDayLow", "fiftyTwoWeekLow", "dayLow")
VOLUME_ALIASES = ("volume", "regularMarketVolume", "averageDailyVolume3Month", "avgVolume", "averageVolume")
NAME_ALIASES = ("name", "shortName", "longName", "displayName", "instrument_name")
EXCHANGE_ALIASES = ("exchange", "fullExchangeName", "exchangeName", "market")
CURRENCY_ALIASES = ("currency", "financialCurrency")

HISTORY_ROW_KEYS = (
    "timestamp", "date", "open", "high", "low", "close", "adj_close", "adjclose", "volume",
)

# =============================================================================
# Logging Setup (library-safe: no basicConfig side effect)
# =============================================================================

logger = logging.getLogger(__name__)

# =============================================================================
# Environment Helpers
# =============================================================================

def _env_bool(name: str, default: bool = False) -> bool:
    """Get boolean from environment."""
    raw = (os.getenv(name) or "").strip().lower()
    if not raw:
        return default
    if raw in _TRUTHY:
        return True
    if raw in _FALSY:
        return False
    return default


def _env_int(name: str, default: int, lo: Optional[int] = None, hi: Optional[int] = None) -> int:
    """Get integer from environment."""
    try:
        value = int(float((os.getenv(name) or str(default)).strip()))
    except Exception:
        value = default
    if lo is not None:
        value = max(lo, value)
    if hi is not None:
        value = min(hi, value)
    return value


def _env_float(name: str, default: float, lo: Optional[float] = None, hi: Optional[float] = None) -> float:
    """Get float from environment."""
    try:
        value = float((os.getenv(name) or str(default)).strip())
    except Exception:
        value = default
    if lo is not None:
        value = max(lo, value)
    if hi is not None:
        value = min(hi, value)
    return value


# =============================================================================
# Custom Exceptions
# =============================================================================

class YahooChartError(Exception):
    """Base exception for Yahoo Chart provider."""
    pass


class CircuitBreakerOpenError(YahooChartError):
    """Raised when circuit breaker is open."""
    pass


class CanonicalProviderError(YahooChartError):
    """Raised when canonical provider is unavailable."""
    pass


# =============================================================================
# Pure Utility Functions
# =============================================================================

def _safe_symbol(value: Any) -> str:
    """Safely convert to symbol string."""
    try:
        return str(value or "").strip().upper()
    except Exception:
        return ""


def _is_nonempty(value: Any) -> bool:
    """Check if value is non-empty."""
    if value is None:
        return False
    if value == 0:
        return True
    if value is False:
        return True
    if isinstance(value, float) and math.isnan(value):
        return False
    if isinstance(value, str) and not value.strip():
        return False
    if isinstance(value, (list, tuple, set, dict)) and len(value) == 0:
        return False
    return True


def _nonempty_count(obj: Any) -> int:
    """Count non-empty values in dict."""
    if not isinstance(obj, dict):
        return 0
    return sum(1 for v in obj.values() if _is_nonempty(v))


def _coerce_float(value: Any) -> Optional[float]:
    """Safely convert to float."""
    if value is None:
        return None
    if isinstance(value, bool):
        return float(value)
    if isinstance(value, (int, float)):
        try:
            if isinstance(value, float) and math.isnan(value):
                return None
            return float(value)
        except Exception:
            return None
    try:
        text = str(value).strip().replace(",", "")
        if not text or text.lower() in {"none", "nan", "null", "n/a", "na"}:
            return None
        return float(text)
    except Exception:
        return None


def _coerce_int(value: Any) -> Optional[int]:
    """Safely convert to integer."""
    num = _coerce_float(value)
    if num is None:
        return None
    try:
        return int(num)
    except Exception:
        return None


def _safe_get(mapping: Any, *keys: str, default: Any = None) -> Any:
    """Get first non-None value from mapping."""
    if not isinstance(mapping, dict):
        return default
    for key in keys:
        if key in mapping and mapping[key] is not None:
            return mapping[key]
    return default


def _deep_get(mapping: Any, path: Sequence[Union[str, int]], default: Any = None) -> Any:
    """Get nested value from dict/list structure."""
    current = mapping
    try:
        for part in path:
            if isinstance(part, int):
                if not isinstance(current, (list, tuple)) or part >= len(current):
                    return default
                current = current[part]
            else:
                if not isinstance(current, dict) or part not in current:
                    return default
                current = current[part]
        return current
    except Exception:
        return default


def _to_dict(value: Any) -> Dict[str, Any]:
    """Convert value to dictionary."""
    if value is None:
        return {}
    if isinstance(value, dict):
        return dict(value)
    if is_dataclass(value):
        try:
            return asdict(value)
        except Exception:
            return {}
    if hasattr(value, "model_dump"):
        try:
            dumped = value.model_dump()
            return dict(dumped) if isinstance(dumped, dict) else {}
        except Exception:
            pass
    if hasattr(value, "dict"):
        try:
            dumped = value.dict()
            return dict(dumped) if isinstance(dumped, dict) else {}
        except Exception:
            pass
    if hasattr(value, "__dict__"):
        try:
            raw = vars(value)
            return dict(raw) if isinstance(raw, dict) else {}
        except Exception:
            return {}
    return {}


def _merge_nonempty(base: Optional[Dict[str, Any]], patch: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    """Merge dicts, preferring non-empty values from patch."""
    result = dict(base or {})
    for key, value in (patch or {}).items():
        if _is_nonempty(value) or key not in result:
            result[key] = value
    return result


def _merge_prefer_base(base: Optional[Dict[str, Any]], patch: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    """Merge dicts, preferring non-empty values from base."""
    result = dict(base or {})
    for key, value in (patch or {}).items():
        if not _is_nonempty(result.get(key)) and _is_nonempty(value):
            result[key] = value
        elif key not in result:
            result[key] = value
    return result


def _normalize_timestamp(value: Any) -> Optional[str]:
    """Normalize timestamp to ISO format."""
    if value is None:
        return None
    if isinstance(value, datetime):
        dt = value if value.tzinfo else value.replace(tzinfo=UTC)
        return dt.astimezone(UTC).isoformat()
    if isinstance(value, (int, float)):
        try:
            return datetime.fromtimestamp(float(value), tz=UTC).isoformat()
        except Exception:
            return None
    text = str(value).strip()
    if not text:
        return None
    try:
        dt = datetime.fromisoformat(text.replace("Z", "+00:00"))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=UTC)
        return dt.astimezone(UTC).isoformat()
    except Exception:
        return text


def _version_tuple(value: str) -> Tuple[int, int, int]:
    """Convert version string to tuple."""
    nums = re.findall(r"\d+", str(value))
    parts = [int(x) for x in nums[:3]]
    while len(parts) < 3:
        parts.append(0)
    return (parts[0], parts[1], parts[2])


# =============================================================================
# Symbol Classification
# =============================================================================

def _symbol_kind(symbol: str) -> str:
    """Classify symbol type."""
    sym = _safe_symbol(symbol)
    if not sym:
        return "unknown"
    if sym.endswith("=X"):
        return "fx"
    if "-USD" in sym or (sym.endswith("USD") and "=" not in sym and "." not in sym and len(sym) <= 10):
        return "crypto"
    if sym.endswith("=F"):
        return "future"
    if sym.endswith(".SR"):
        return "ksa_equity"
    if any(tag in sym for tag in ("ETF", "VOO", "VTI", "QQQ", "SPY")):
        return "fund"
    return "equity"


def _fx_display_name(sym: str) -> str:
    """Get display name for FX pair."""
    root = sym.replace("=X", "")
    if len(root) == 6:
        return f"{root[:3]}/{root[3:]}"
    if len(root) == 3:
        return f"{root}/USD"
    return sym


def _identity_defaults_for_symbol(symbol: str) -> Dict[str, Any]:
    """Get identity defaults for symbol type."""
    sym = _safe_symbol(symbol)
    kind = _symbol_kind(sym)
    defaults: Dict[str, Any] = {
        "symbol": sym,
        "symbol_normalized": sym,
        "requested_symbol": sym,
        "instrument_type": kind,
    }

    if kind == "fx":
        root = sym.replace("=X", "")
        defaults.update({
            "name": _fx_display_name(sym),
            "asset_class": "FX",
            "exchange": "FX",
            "currency": root[3:] if len(root) == 6 else "USD",
            "country": "Global",
            "sector": "Foreign Exchange",
            "industry": "Currency Pair",
        })
    elif kind == "future":
        name_map = {
            "GC=F": "Gold Futures", "SI=F": "Silver Futures", "HG=F": "Copper Futures",
            "CL=F": "Crude Oil Futures", "BZ=F": "Brent Crude Futures", "NG=F": "Natural Gas Futures",
        }
        defaults.update({
            "name": name_map.get(sym, sym),
            "asset_class": "Commodity",
            "exchange": "Futures",
            "currency": "USD",
            "country": "Global",
            "sector": "Commodities",
            "industry": "Futures Contract",
        })
    elif kind == "crypto":
        defaults.update({
            "asset_class": "Crypto",
            "exchange": "Crypto",
            "country": "Global",
            "sector": "Digital Assets",
            "industry": "Cryptocurrency",
        })
    elif kind == "ksa_equity":
        defaults.update({
            "asset_class": "Equity",
            "exchange": "Tadawul",
            "currency": "SAR",
            "country": "Saudi Arabia",
        })
    elif kind == "fund":
        defaults.update({
            "asset_class": "Fund",
            "exchange": "ETF",
            "country": "Global",
            "sector": "Funds",
            "industry": "Exchange Traded Fund",
        })

    return defaults


# =============================================================================
# History Extraction
# =============================================================================

def _ensure_history_row(row: Dict[str, Any]) -> Dict[str, Any]:
    """Ensure history row has proper types."""
    result = dict(row)
    timestamp = _normalize_timestamp(_safe_get(result, "timestamp", "date"))
    if timestamp:
        result["timestamp"] = timestamp
    for key in ("open", "high", "low", "close", "adj_close", "adjclose"):
        if key in result:
            result[key] = _coerce_float(result[key])
    if "volume" in result:
        result["volume"] = _coerce_int(result["volume"])
    return result


def _rows_from_parallel_arrays(payload: Dict[str, Any]) -> List[Dict[str, Any]]:
    """Extract rows from parallel arrays (chart format)."""
    timestamps = _deep_get(payload, ["timestamp"], default=None)
    if not isinstance(timestamps, list):
        timestamps = _deep_get(payload, ["timestamps"], default=None)

    quotes = _deep_get(payload, ["indicators", "quote", 0], default={})
    adjclose_block = _deep_get(payload, ["indicators", "adjclose", 0], default={})

    if not isinstance(timestamps, list) or not isinstance(quotes, dict):
        return []

    opens = quotes.get("open") if isinstance(quotes.get("open"), list) else []
    highs = quotes.get("high") if isinstance(quotes.get("high"), list) else []
    lows = quotes.get("low") if isinstance(quotes.get("low"), list) else []
    closes = quotes.get("close") if isinstance(quotes.get("close"), list) else []
    vols = quotes.get("volume") if isinstance(quotes.get("volume"), list) else []
    adjs = adjclose_block.get("adjclose") if isinstance(adjclose_block.get("adjclose"), list) else []

    rows: List[Dict[str, Any]] = []
    for idx, ts in enumerate(timestamps):
        row = {
            "timestamp": _normalize_timestamp(ts),
            "open": opens[idx] if idx < len(opens) else None,
            "high": highs[idx] if idx < len(highs) else None,
            "low": lows[idx] if idx < len(lows) else None,
            "close": closes[idx] if idx < len(closes) else None,
            "adj_close": adjs[idx] if idx < len(adjs) else None,
            "volume": vols[idx] if idx < len(vols) else None,
        }
        row = _ensure_history_row(row)
        if any(_is_nonempty(v) for v in row.values()):
            rows.append(row)
    return rows


def _rows_from_candle_arrays(payload: Dict[str, Any]) -> List[Dict[str, Any]]:
    """Extract rows from candle arrays."""
    ts = payload.get("timestamp") or payload.get("timestamps") or payload.get("t") or []
    opens = payload.get("open") or payload.get("o") or []
    highs = payload.get("high") or payload.get("h") or []
    lows = payload.get("low") or payload.get("l") or []
    closes = payload.get("close") or payload.get("c") or []
    vols = payload.get("volume") or payload.get("v") or []

    if not isinstance(ts, list):
        return []
    if not any(isinstance(x, list) for x in (opens, highs, lows, closes, vols)) and not isinstance(closes, list):
        return []

    rows: List[Dict[str, Any]] = []
    for idx, raw_ts in enumerate(ts):
        row = {
            "timestamp": _normalize_timestamp(raw_ts),
            "open": opens[idx] if isinstance(opens, list) and idx < len(opens) else None,
            "high": highs[idx] if isinstance(highs, list) and idx < len(highs) else None,
            "low": lows[idx] if isinstance(lows, list) and idx < len(lows) else None,
            "close": closes[idx] if isinstance(closes, list) and idx < len(closes) else None,
            "volume": vols[idx] if isinstance(vols, list) and idx < len(vols) else None,
        }
        row = _ensure_history_row(row)
        if any(_is_nonempty(v) for v in row.values()):
            rows.append(row)
    return rows


def _rows_from_dataframe_like(payload: Dict[str, Any]) -> List[Dict[str, Any]]:
    """Extract rows from pandas DataFrame-like dict."""
    keys = {k.lower(): k for k in payload.keys()}
    core_cols = [keys.get(k) for k in ("open", "high", "low", "close")]
    if not any(core_cols):
        return []

    # Check if it's a dict of dicts (pandas to_dict() format)
    if not any(isinstance(payload.get(col), dict) for col in core_cols if col):
        return []

    # Get index values
    index_values: List[Any] = []
    for col in core_cols:
        if col and isinstance(payload.get(col), dict):
            index_values = list(payload[col].keys())
            if index_values:
                break
    if not index_values:
        return []

    rows: List[Dict[str, Any]] = []
    for idx in index_values:
        row = {
            "timestamp": _normalize_timestamp(idx),
            "open": payload.get(keys.get("open", ""), {}).get(idx) if keys.get("open") else None,
            "high": payload.get(keys.get("high", ""), {}).get(idx) if keys.get("high") else None,
            "low": payload.get(keys.get("low", ""), {}).get(idx) if keys.get("low") else None,
            "close": payload.get(keys.get("close", ""), {}).get(idx) if keys.get("close") else None,
            "adj_close": payload.get(keys.get("adj_close") or keys.get("adjclose", ""), {}).get(idx) if keys.get("adj_close") or keys.get("adjclose") else None,
            "volume": payload.get(keys.get("volume", ""), {}).get(idx) if keys.get("volume") else None,
        }
        row = _ensure_history_row(row)
        if any(_is_nonempty(v) for v in row.values()):
            rows.append(row)
    return rows


def _extract_candidate_dicts(payload_dict: Dict[str, Any]) -> List[Dict[str, Any]]:
    """Extract candidate dicts from nested payload."""
    candidates: List[Dict[str, Any]] = [payload_dict]

    paths = [
        ("meta",),
        ("price",),
        ("summaryDetail",),
        ("defaultKeyStatistics",),
        ("quoteResponse", "result", 0),
        ("spark", "result", 0, "response", 0, "meta"),
        ("chart", "result", 0, "meta"),
        ("chart", "result", 0),
        ("result", 0),
        ("response", 0),
        ("data", 0),
        ("items", 0),
        ("records", 0),
    ]
    for path in paths:
        value = _deep_get(payload_dict, path, default=None)
        value_dict = _to_dict(value)
        if value_dict:
            candidates.append(value_dict)
    return candidates


def extract_history_rows(payload: Any) -> List[Dict[str, Any]]:
    """Extract history rows from various payload formats."""
    if payload is None:
        return []

    if isinstance(payload, list):
        rows: List[Dict[str, Any]] = []
        for item in payload:
            if isinstance(item, dict):
                if any(key in item for key in HISTORY_ROW_KEYS):
                    rows.append(_ensure_history_row(item))
                else:
                    nested = extract_history_rows(item)
                    if nested:
                        rows.extend(nested)
            else:
                as_dict = _to_dict(item)
                if as_dict:
                    rows.extend(extract_history_rows(as_dict))
        return rows

    payload_dict = _to_dict(payload)
    if not payload_dict:
        return []

    if any(key in payload_dict for key in HISTORY_ROW_KEYS):
        return [_ensure_history_row(payload_dict)]

    for key in ("rows", "history", "prices", "data", "items", "records", "candles", "ohlcv"):
        nested = payload_dict.get(key)
        rows = extract_history_rows(nested)
        if rows:
            return rows

    dataframe_rows = _rows_from_dataframe_like(payload_dict)
    if dataframe_rows:
        return dataframe_rows

    candle_rows = _rows_from_candle_arrays(payload_dict)
    if candle_rows:
        return candle_rows

    chart_root = _deep_get(payload_dict, ["chart", "result", 0], default=None)
    if isinstance(chart_root, dict):
        rows = _rows_from_parallel_arrays(chart_root)
        if rows:
            return rows

    spark_root = _deep_get(payload_dict, ["spark", "result", 0, "response", 0], default=None)
    if isinstance(spark_root, dict):
        rows = _rows_from_parallel_arrays(spark_root)
        if rows:
            return rows

    for key in ("result", "quoteResponse", "response"):
        nested = payload_dict.get(key)
        rows = extract_history_rows(nested)
        if rows:
            return rows

    return []


def derive_quote_from_rows(rows: Sequence[Dict[str, Any]]) -> Dict[str, Any]:
    """Derive quote fields from history rows."""
    cleaned = [r for r in rows if isinstance(r, dict)]
    if not cleaned:
        return {}

    latest = cleaned[-1]
    previous = cleaned[-2] if len(cleaned) >= 2 else {}

    closes = [_coerce_float(r.get("close")) for r in cleaned]
    highs = [_coerce_float(r.get("high")) for r in cleaned]
    lows = [_coerce_float(r.get("low")) for r in cleaned]
    opens = [_coerce_float(r.get("open")) for r in cleaned]
    volumes = [_coerce_int(r.get("volume")) for r in cleaned]

    closes_valid = [v for v in closes if v is not None]
    highs_valid = [v for v in highs if v is not None]
    lows_valid = [v for v in lows if v is not None]
    opens_valid = [v for v in opens if v is not None]
    vols_valid = [v for v in volumes if v is not None]

    current = _coerce_float(_safe_get(latest, "close", "adj_close", "adjclose", "high", "low", "open"))
    previous_close = _coerce_float(_safe_get(previous, "close", "adj_close", "adjclose"))
    if previous_close is None and len(closes_valid) >= 2:
        previous_close = closes_valid[-2]

    latest_open = _coerce_float(latest.get("open"))
    latest_high = _coerce_float(latest.get("high"))
    latest_low = _coerce_float(latest.get("low"))
    latest_volume = _coerce_int(latest.get("volume"))

    result: Dict[str, Any] = {
        "current_price": current,
        "price": current,
        "previous_close": previous_close,
        "open": latest_open if latest_open is not None else (opens_valid[-1] if opens_valid else None),
        "day_high": latest_high if latest_high is not None else current,
        "day_low": latest_low if latest_low is not None else current,
        "volume": latest_volume,
        "week_52_high": max(highs_valid) if highs_valid else None,
        "week_52_low": min(lows_valid) if lows_valid else None,
        "avg_volume_30d": int(sum(vols_valid[-30:]) / len(vols_valid[-30:])) if vols_valid else None,
        "avg_volume_10d": int(sum(vols_valid[-10:]) / len(vols_valid[-10:])) if vols_valid else None,
        "history_points": len(cleaned),
        "history_last_timestamp": _normalize_timestamp(_safe_get(latest, "timestamp", "date")),
    }

    # Price change
    if current is not None and previous_close not in (None, 0):
        price_change = current - previous_close
        result["price_change"] = price_change
        result["percent_change"] = (price_change / previous_close) * 100.0

    # 52-week position
    if current is not None and highs_valid and lows_valid and max(highs_valid) != min(lows_valid):
        lo = min(lows_valid)
        hi = max(highs_valid)
        result["week_52_position_pct"] = ((current - lo) / (hi - lo)) * 100.0

    # Returns and volatility
    if len(closes_valid) >= 2:
        returns: List[float] = []
        for i in range(1, len(closes_valid)):
            prev = closes_valid[i - 1]
            cur = closes_valid[i]
            if prev not in (None, 0) and cur is not None:
                returns.append((cur / prev) - 1.0)

        if returns:
            mean_ret = sum(returns) / len(returns)
            if len(returns) >= 2:
                variance = sum((r - mean_ret) ** 2 for r in returns) / max(1, len(returns) - 1)
                daily_vol = math.sqrt(max(0.0, variance))
                result["volatility_30d"] = daily_vol * math.sqrt(min(30, len(returns)))
                result["volatility_90d"] = daily_vol * math.sqrt(min(90, len(returns)))

            # Drawdown
            if current is not None and closes_valid:
                peak = closes_valid[0]
                max_drawdown = 0.0
                for price in closes_valid:
                    peak = max(peak, price)
                    if peak not in (None, 0):
                        dd = (price / peak) - 1.0
                        max_drawdown = min(max_drawdown, dd)
                result["max_drawdown_1y"] = max_drawdown

            # VaR
            if len(returns) >= 5:
                downside = sorted(returns)[max(0, int(len(returns) * 0.05) - 1)]
                result["var_95_1d"] = downside

            # Sharpe
            if len(returns) >= 5:
                denom = math.sqrt(sum((r - mean_ret) ** 2 for r in returns) / max(1, len(returns) - 1))
                if denom > 0:
                    result["sharpe_1y"] = (mean_ret / denom) * math.sqrt(min(252, len(returns)))

            # RSI
            if len(closes_valid) >= 15:
                gains: List[float] = []
                losses: List[float] = []
                tail = closes_valid[-15:]
                for i in range(1, len(tail)):
                    delta = tail[i] - tail[i - 1]
                    gains.append(max(delta, 0.0))
                    losses.append(abs(min(delta, 0.0)))
                avg_gain = sum(gains) / max(1, len(gains))
                avg_loss = sum(losses) / max(1, len(losses))
                if avg_loss == 0 and avg_gain > 0:
                    result["rsi_14"] = 100.0
                elif avg_loss > 0:
                    rs = avg_gain / avg_loss
                    result["rsi_14"] = 100.0 - (100.0 / (1.0 + rs))

    return {k: v for k, v in result.items() if _is_nonempty(v)}


def derive_quote_from_chart_payload(payload: Dict[str, Any]) -> Dict[str, Any]:
    """Derive quote from chart payload."""
    chart_root = _deep_get(payload, ["chart", "result", 0], default=None)
    if not isinstance(chart_root, dict):
        chart_root = payload

    meta = chart_root.get("meta") if isinstance(chart_root.get("meta"), dict) else {}
    rows = _rows_from_parallel_arrays(chart_root)
    if not rows:
        rows = _rows_from_candle_arrays(chart_root)
    derived = derive_quote_from_rows(rows)

    result: Dict[str, Any] = {
        "current_price": _coerce_float(_safe_get(meta, *PRICE_FIELD_ALIASES)),
        "price": _coerce_float(_safe_get(meta, *PRICE_FIELD_ALIASES)),
        "previous_close": _coerce_float(_safe_get(meta, *PREV_CLOSE_ALIASES)),
        "open": _coerce_float(_safe_get(meta, *OPEN_ALIASES)),
        "day_high": _coerce_float(_safe_get(meta, *HIGH_ALIASES)),
        "day_low": _coerce_float(_safe_get(meta, *LOW_ALIASES)),
        "volume": _coerce_int(_safe_get(meta, *VOLUME_ALIASES)),
        "exchange": _safe_get(meta, *EXCHANGE_ALIASES),
        "currency": _safe_get(meta, *CURRENCY_ALIASES),
        "name": _safe_get(meta, *NAME_ALIASES),
        "week_52_high": _coerce_float(_safe_get(meta, "fiftyTwoWeekHigh", "fifty_two_week_high", "52w_high")),
        "week_52_low": _coerce_float(_safe_get(meta, "fiftyTwoWeekLow", "fifty_two_week_low", "52w_low")),
        "market_state": _safe_get(meta, "marketState", "market_state"),
        "instrument_type": _safe_get(meta, "instrumentType", "quoteType", "typeDisp"),
        "exchange_timezone": _safe_get(meta, "exchangeTimezoneName", "timezone"),
    }

    result = _merge_nonempty(derived, result)

    current = _coerce_float(_safe_get(result, "current_price", "price"))
    prev = _coerce_float(result.get("previous_close"))
    if current is not None and prev not in (None, 0):
        price_change = current - prev
        result.setdefault("price_change", price_change)
        result.setdefault("percent_change", (price_change / prev) * 100.0)

    return {k: v for k, v in result.items() if _is_nonempty(v)}


def classify_quality(payload: Dict[str, Any]) -> str:
    """Classify data quality."""
    nonempty = _nonempty_count(payload)
    if nonempty >= 22:
        return DataQuality.EXCELLENT.value
    if nonempty >= 16:
        return DataQuality.HIGH.value
    if nonempty >= 10:
        return DataQuality.MEDIUM.value
    if nonempty >= 5:
        return DataQuality.PARTIAL.value
    if payload.get("error"):
        return DataQuality.ERROR.value
    return DataQuality.LOW.value


def normalize_quote_payload(payload: Any, symbol: str) -> Dict[str, Any]:
    """Normalize quote payload to standard format."""
    sym = _safe_symbol(symbol)
    defaults = _identity_defaults_for_symbol(sym)

    if payload is None:
        return defaults

    payload_dict = _to_dict(payload)
    if not payload_dict and isinstance(payload, list):
        rows = extract_history_rows(payload)
        return _merge_nonempty(defaults, derive_quote_from_rows(rows))
    if not payload_dict:
        return defaults

    result = dict(defaults)

    for candidate in _extract_candidate_dicts(payload_dict):
        for alias in NAME_ALIASES:
            if _is_nonempty(candidate.get(alias)):
                result["name"] = candidate.get(alias)
                break
        for alias in EXCHANGE_ALIASES:
            if _is_nonempty(candidate.get(alias)):
                result["exchange"] = candidate.get(alias)
                break
        for alias in CURRENCY_ALIASES:
            if _is_nonempty(candidate.get(alias)):
                result["currency"] = candidate.get(alias)
                break

        result = _merge_nonempty(result, {
            "current_price": _coerce_float(_safe_get(candidate, *PRICE_FIELD_ALIASES)),
            "price": _coerce_float(_safe_get(candidate, *PRICE_FIELD_ALIASES)),
            "previous_close": _coerce_float(_safe_get(candidate, *PREV_CLOSE_ALIASES)),
            "open": _coerce_float(_safe_get(candidate, *OPEN_ALIASES)),
            "day_high": _coerce_float(_safe_get(candidate, *HIGH_ALIASES)),
            "day_low": _coerce_float(_safe_get(candidate, *LOW_ALIASES)),
            "volume": _coerce_int(_safe_get(candidate, *VOLUME_ALIASES)),
            "week_52_high": _coerce_float(_safe_get(candidate, "52w_high", "fiftyTwoWeekHigh", "fifty_two_week_high")),
            "week_52_low": _coerce_float(_safe_get(candidate, "52w_low", "fiftyTwoWeekLow", "fifty_two_week_low")),
            "market_cap": _coerce_float(_safe_get(candidate, "market_cap", "marketCap")),
            "beta_5y": _coerce_float(_safe_get(candidate, "beta_5y", "beta", "beta5YMonthly")),
            "dividend_yield": _coerce_float(_safe_get(candidate, "dividend_yield", "dividendYield")),
            "long_name": _safe_get(candidate, "longName", "long_name"),
            "short_name": _safe_get(candidate, "shortName", "short_name"),
            "market_state": _safe_get(candidate, "market_state", "marketState"),
            "instrument_type": _safe_get(candidate, "instrumentType", "quoteType", "typeDisp"),
            "exchange_timezone": _safe_get(candidate, "exchangeTimezoneName", "timezone"),
        })

    chart_derived = derive_quote_from_chart_payload(payload_dict)
    history_rows = extract_history_rows(payload_dict)
    history_derived = derive_quote_from_rows(history_rows)

    result = _merge_nonempty(result, chart_derived)
    result = _merge_nonempty(result, history_derived)

    # Price change
    current = _coerce_float(_safe_get(result, "current_price", "price"))
    prev = _coerce_float(result.get("previous_close"))
    if current is not None and prev not in (None, 0):
        price_change = current - prev
        result["price_change"] = price_change
        result["percent_change"] = (price_change / prev) * 100.0

    # Warnings
    warnings: List[str] = []
    for key in ("warnings", "warning", "notes", "message"):
        value = payload_dict.get(key)
        if isinstance(value, list):
            warnings.extend([str(x) for x in value if _is_nonempty(x)])
        elif _is_nonempty(value):
            warnings.append(str(value))
    if warnings:
        result["warnings"] = warnings

    return {k: v for k, v in result.items() if _is_nonempty(v) or k in {"symbol", "symbol_normalized", "requested_symbol", "instrument_type"}}


def normalize_history_payload(payload: Any, symbol: str) -> List[Dict[str, Any]]:
    """Normalize history payload to list of rows."""
    rows = extract_history_rows(payload)
    if rows:
        return rows

    # Fallback: create synthetic row from quote
    sym = _safe_symbol(symbol)
    quote = normalize_quote_payload(payload, symbol=sym)
    current = _coerce_float(_safe_get(quote, "current_price", "price"))
    if current is None:
        return []

    synthetic = {
        "timestamp": _utc_iso(),
        "open": _coerce_float(quote.get("open")),
        "high": _coerce_float(quote.get("day_high")),
        "low": _coerce_float(quote.get("day_low")),
        "close": current,
        "adj_close": current,
        "volume": _coerce_int(quote.get("volume")),
    }
    return [_ensure_history_row(synthetic)]


def normalize_patch_payload(payload: Any, symbol: str, base: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    """Normalize patch payload."""
    patch = normalize_quote_payload(payload, symbol=symbol)
    merged = _merge_prefer_base(base or {}, patch)
    if not _is_nonempty(merged.get("current_price")) and not _is_nonempty(merged.get("price")):
        rows = normalize_history_payload(payload, symbol=symbol)
        merged = _merge_prefer_base(merged, derive_quote_from_rows(rows))
    return merged


def ensure_shape(
    payload: Any,
    symbol: str,
    provider_version: Optional[str],
    fn_name: str,
    result_kind: str,
) -> Any:
    """Ensure payload has correct shape."""
    sym = _safe_symbol(symbol)

    if result_kind == "history":
        return normalize_history_payload(payload, symbol=sym)

    # Preserve error context from an already-shaped error payload so it
    # survives the downstream normalize_quote_payload / normalize_patch_payload
    # calls (which otherwise strip unrecognized top-level keys).  v5.1.0 fix:
    # without this, a fallback_factory returning ``error_payload(...)`` was
    # losing the ``error`` / ``status`` fields, causing callers to see
    # ``status="ok"`` with no error info.
    incoming_error: Optional[str] = None
    incoming_status: Optional[str] = None
    if isinstance(payload, dict):
        raw_err = payload.get("error")
        if _is_nonempty(raw_err):
            incoming_error = str(raw_err)
        raw_status = payload.get("status")
        if isinstance(raw_status, str) and raw_status.strip():
            incoming_status = raw_status.strip()

    if result_kind == "patch":
        base = None
        if isinstance(payload, dict):
            base_candidate = payload.get("base")
            if isinstance(base_candidate, dict):
                base = base_candidate
        result = normalize_patch_payload(payload, symbol=sym, base=base)
    else:
        result = normalize_quote_payload(payload, symbol=sym)

    # Restore error context (if any) that was stripped by normalization.
    if incoming_error is not None:
        result.setdefault("error", incoming_error)
    if incoming_status is not None:
        result.setdefault("status", incoming_status)

    result.setdefault("symbol", sym)
    result.setdefault("symbol_normalized", sym)
    result.setdefault("requested_symbol", sym)
    result.setdefault("data_source", DATA_SOURCE)
    result.setdefault("provider", DATA_SOURCE)
    result.setdefault("shim_version", SHIM_VERSION)
    if provider_version:
        result.setdefault("provider_version", provider_version)
    result.setdefault("last_updated_utc", _utc_iso())
    result.setdefault("last_updated_riyadh", _riyadh_iso())

    if result.get("error"):
        result.setdefault("status", "error")
        result["data_quality"] = DataQuality.ERROR.value
    else:
        result.setdefault("status", "ok")
        result.setdefault("data_quality", classify_quality(result))

    result.setdefault("where", fn_name)
    return result


def error_payload(symbol: str, error: str, fn_name: str) -> Dict[str, Any]:
    """Create error payload."""
    sym = _safe_symbol(symbol)
    result = _identity_defaults_for_symbol(sym)
    result.update({
        "status": "error",
        "data_source": DATA_SOURCE,
        "provider": DATA_SOURCE,
        "data_quality": DataQuality.ERROR.value,
        "error": error,
        "where": fn_name,
        "shim_version": SHIM_VERSION,
        "last_updated_utc": _utc_iso(),
        "last_updated_riyadh": _riyadh_iso(),
    })
    return result


def _classify_result_kind(method_name: str) -> str:
    """
    Map a canonical provider method name to the shape kind expected by
    ``ensure_shape``.

    Precedence (v5.1.0):
      1. History-indicating tokens (``history``, ``prices``, ``ohlc``) win
         -- because methods like ``fetch_history_patch`` return a list of
         rows even though ``patch`` is in the name.
      2. ``patch`` -> "patch" (dict merged over a base row).
      3. Default -> "quote".
    """
    name = (method_name or "").lower()
    if ("history" in name) or ("price_history" in name) or ("ohlc" in name) or name in {"fetch_prices"}:
        return "history"
    if "patch" in name:
        return "patch"
    return "quote"


# =============================================================================
# Async Primitives
# =============================================================================

class SingleFlight:
    """
    Deduplicate concurrent requests for the same key.

    Semantics:
      - The FIRST caller for a given key runs ``coro_fn()`` and seeds the
        result into a shared future.
      - FOLLOWERS for the same key in flight observe the shared future and
        await its result without re-running ``coro_fn()``.
      - The internal lock is held ONLY to look up / install futures. Waits
        on the shared future happen OUTSIDE the lock so followers for
        one key do not block leaders for unrelated keys (v5.1.0 fix).
    """

    def __init__(self) -> None:
        self._lock = asyncio.Lock()
        self._futures: Dict[str, "asyncio.Future[Any]"] = {}

    async def execute(self, key: str, coro_fn: Callable[[], Awaitable[_T]]) -> _T:
        """Execute coroutine with deduplication."""
        existing_future: Optional["asyncio.Future[Any]"] = None
        new_future: Optional["asyncio.Future[Any]"] = None

        async with self._lock:
            found = self._futures.get(key)
            if found is not None:
                existing_future = found
            else:
                new_future = asyncio.get_running_loop().create_future()
                self._futures[key] = new_future

        # Await OUTSIDE the lock (v5.1.0 fix).
        if existing_future is not None:
            return await existing_future  # type: ignore[no-any-return]

        assert new_future is not None
        try:
            result = await coro_fn()
            if not new_future.done():
                new_future.set_result(result)
            return result
        except Exception as exc:
            if not new_future.done():
                new_future.set_exception(exc)
            raise
        finally:
            async with self._lock:
                self._futures.pop(key, None)


class FullJitterBackoff:
    """Full jitter backoff for retries."""

    def __init__(self, attempts: int = 2, base: float = 0.4, cap: float = 4.0) -> None:
        self.attempts = max(0, attempts)
        self.base = max(0.05, base)
        self.cap = max(self.base, cap)

    async def run(self, fn: Callable[[], Awaitable[_T]]) -> _T:
        """Run function with retries."""
        last_error: Optional[BaseException] = None
        total_tries = max(1, self.attempts + 1)

        for idx in range(total_tries):
            try:
                return await fn()
            except Exception as exc:
                last_error = exc
                if idx >= total_tries - 1:
                    break
                delay = min(self.cap, self.base * (2 ** idx))
                await asyncio.sleep(random.uniform(0.0, delay))

        if last_error is not None:
            raise last_error
        raise RuntimeError("retry_exhausted")


class CircuitBreaker:
    """Circuit breaker for provider calls."""

    def __init__(self, threshold: int, timeout_sec: float, half_open_calls: int) -> None:
        self.threshold = max(1, threshold)
        self.timeout_sec = max(1.0, timeout_sec)
        self.half_open_calls = max(1, half_open_calls)

        self._lock = asyncio.Lock()
        self.state: CircuitState = CircuitState.CLOSED
        self.failures = 0
        self.opened_mono: Optional[float] = None
        self.half_used = 0
        self.half_success = 0

    async def allow(self) -> None:
        """Check if request is allowed. Raises CircuitBreakerOpenError when blocked."""
        async with self._lock:
            if self.state == CircuitState.CLOSED:
                return

            if self.state == CircuitState.OPEN:
                if self.opened_mono is None:
                    self.opened_mono = time.monotonic()
                if (time.monotonic() - self.opened_mono) >= self.timeout_sec:
                    self.state = CircuitState.HALF
                    self.half_used = 0
                    self.half_success = 0
                    return
                raise CircuitBreakerOpenError("circuit_open")

            if self.state == CircuitState.HALF:
                if self.half_used >= self.half_open_calls:
                    raise CircuitBreakerOpenError("circuit_half_open_limit")
                self.half_used += 1

    async def on_success(self) -> None:
        """Record success."""
        async with self._lock:
            if self.state == CircuitState.HALF:
                self.half_success += 1
                if self.half_success >= 2:
                    self.state = CircuitState.CLOSED
                    self.failures = 0
                    self.opened_mono = None
                    self.half_used = 0
                    self.half_success = 0
            else:
                self.failures = 0

    async def on_failure(self) -> None:
        """Record failure."""
        async with self._lock:
            if self.state == CircuitState.HALF:
                self.state = CircuitState.OPEN
                self.opened_mono = time.monotonic()
                self.failures = self.threshold
                self.half_used = 0
                self.half_success = 0
                return

            self.failures += 1
            if self.failures >= self.threshold:
                self.state = CircuitState.OPEN
                self.opened_mono = time.monotonic()


# =============================================================================
# Provider Cache
# =============================================================================

@dataclass(slots=True)
class ProviderInfo:
    """Information about canonical provider."""
    module: Any
    version: str
    funcs: Dict[str, Callable[..., Any]]
    available: bool
    checked_mono: float
    origin_path: Optional[str] = None
    error: Optional[str] = None


class ProviderCache:
    """Cache for canonical provider."""

    def __init__(self) -> None:
        self.ttl = _env_float("SHIM_YAHOO_PROVIDER_TTL_SEC", 300.0, lo=10.0, hi=86400.0)
        self._lock = asyncio.Lock()
        self._info: Optional[ProviderInfo] = None
        self._single_flight = SingleFlight()
        self._circuit_breaker = CircuitBreaker(
            threshold=_env_int("SHIM_YAHOO_CB_THRESHOLD", 3, lo=1, hi=20),
            timeout_sec=_env_float("SHIM_YAHOO_CB_TIMEOUT_SEC", 60.0, lo=5.0, hi=600.0),
            half_open_calls=_env_int("SHIM_YAHOO_CB_HALF_CALLS", 2, lo=1, hi=10),
        )

    def _is_valid(self) -> bool:
        """Check if cached info is still valid."""
        return self._info is not None and (time.monotonic() - self._info.checked_mono) < self.ttl

    async def get(self) -> ProviderInfo:
        """Get provider info."""
        async with self._lock:
            if self._is_valid():
                return self._info  # type: ignore[return-value]

        info = await self._single_flight.execute("provider_load", self._load)
        async with self._lock:
            self._info = info
        return info

    async def _load(self) -> ProviderInfo:
        """Load provider info."""
        async def _do_import() -> ProviderInfo:
            try:
                await self._circuit_breaker.allow()
            except CircuitBreakerOpenError as cb_exc:
                return ProviderInfo(
                    module=None,
                    version="unknown",
                    funcs={},
                    available=False,
                    checked_mono=time.monotonic(),
                    origin_path=None,
                    error=str(cb_exc),
                )

            try:
                module = None
                origin_path: Optional[str] = None
                last_error: Optional[BaseException] = None
                self_module = sys.modules.get(__name__)

                for path in CANONICAL_IMPORT_PATHS:
                    try:
                        candidate = import_module(path)
                    except Exception as exc:
                        last_error = exc
                        continue
                    # Guard against self-discovery: if the canonical path
                    # resolves to this very shim (e.g. the shim is deployed
                    # at a bare name that is also in CANONICAL_IMPORT_PATHS),
                    # skip it to avoid infinite recursion in dispatch.
                    if candidate is self_module:
                        last_error = ImportError(f"self_import:{path}")
                        continue
                    module = candidate
                    origin_path = path
                    last_error = None
                    break

                if module is None:
                    raise ImportError(
                        f"canonical_missing:"
                        f"{last_error.__class__.__name__ if last_error else 'unknown'}"
                    )

                version = str(
                    getattr(module, "PROVIDER_VERSION", None)
                    or getattr(module, "VERSION", None)
                    or getattr(module, "SHIM_VERSION", None)
                    or "unknown"
                )
                if _version_tuple(version) < _version_tuple(MIN_CANONICAL_VERSION):
                    raise RuntimeError(f"canonical_version_too_old:{version}")

                funcs: Dict[str, Callable[..., Any]] = {}
                for name in dir(module):
                    if name.startswith("_"):
                        continue
                    try:
                        obj = getattr(module, name)
                    except Exception:
                        continue
                    if callable(obj):
                        funcs[name] = obj

                await self._circuit_breaker.on_success()
                return ProviderInfo(
                    module=module,
                    version=version,
                    funcs=funcs,
                    available=True,
                    checked_mono=time.monotonic(),
                    origin_path=origin_path,
                    error=None,
                )
            except Exception as exc:
                await self._circuit_breaker.on_failure()
                return ProviderInfo(
                    module=None,
                    version="unknown",
                    funcs={},
                    available=False,
                    checked_mono=time.monotonic(),
                    origin_path=None,
                    error=str(exc),
                )

        return await _do_import()


_PROVIDER_CACHE = ProviderCache()


# =============================================================================
# Signature Helpers
# =============================================================================

@lru_cache(maxsize=256)
def _signature_params(fn: Callable[..., Any]) -> Dict[str, inspect.Parameter]:
    """Get function signature parameters."""
    try:
        return dict(inspect.signature(fn).parameters)
    except Exception:
        return {}


def _adapt_kwargs(fn: Callable[..., Any], kwargs: Dict[str, Any]) -> Dict[str, Any]:
    """Adapt kwargs to function signature."""
    try:
        params = _signature_params(fn)
        if not params:
            return dict(kwargs)
        if any(p.kind == inspect.Parameter.VAR_KEYWORD for p in params.values()):
            return dict(kwargs)
        return {k: v for k, v in kwargs.items() if k in params}
    except Exception:
        return dict(kwargs)


def _coerce_args_for_call(
    fn: Callable[..., Any],
    args: Tuple[Any, ...],
    kwargs: Dict[str, Any],
) -> Tuple[Tuple[Any, ...], Dict[str, Any]]:
    """Coerce args for function call.

    Removes kwargs that would collide with positional args to prevent
    ``TypeError: got multiple values for argument`` from the canonical
    function.
    """
    params = _signature_params(fn)
    if not params:
        return args, dict(kwargs)

    arg_names = [
        name for name, p in params.items()
        if p.kind in (inspect.Parameter.POSITIONAL_ONLY, inspect.Parameter.POSITIONAL_OR_KEYWORD)
    ]
    result_kwargs = _adapt_kwargs(fn, kwargs)

    # Remove every kwarg that would collide with one of the passed
    # positional args (v5.1.0: generalized beyond the first two).
    for idx in range(min(len(args), len(arg_names))):
        result_kwargs.pop(arg_names[idx], None)

    return args, result_kwargs


async def _maybe_await(value: Any) -> Any:
    """Await if value is awaitable."""
    if inspect.isawaitable(value):
        return await value
    return value


# =============================================================================
# Shim Function
# =============================================================================

class ShimFunction:
    """Shim function that delegates to canonical provider with fallback."""

    def __init__(
        self,
        name: str,
        canonical_names: Optional[Sequence[str]] = None,
        fallback_factory: Optional[Callable[..., Any]] = None,
        result_kind: str = "quote",
    ) -> None:
        self.name = name
        self.canonical_names = tuple(canonical_names or (name,))
        self.fallback_factory = fallback_factory
        self.result_kind = result_kind
        self._backoff = FullJitterBackoff(
            attempts=_env_int("SHIM_YAHOO_RETRY_ATTEMPTS", 2, lo=0, hi=6),
            base=_env_float("SHIM_YAHOO_RETRY_BASE_SEC", 0.35, lo=0.05, hi=5.0),
            cap=_env_float("SHIM_YAHOO_RETRY_CAP_SEC", 3.0, lo=0.2, hi=20.0),
        )

    async def __call__(self, *args: Any, **kwargs: Any) -> Any:
        """Execute shim function."""
        sym = _safe_symbol(kwargs.get("symbol") or kwargs.get("ticker") or (args[0] if args else ""))

        async def _call_canonical() -> Any:
            info = await _PROVIDER_CACHE.get()
            if not info.available:
                raise RuntimeError(info.error or "canonical_unavailable")

            fn = None
            for candidate in self.canonical_names:
                fn = info.funcs.get(candidate)
                if fn is not None:
                    break
            if fn is None:
                raise AttributeError(f"canonical_missing_fn:{self.canonical_names[0]}")

            call_args, call_kwargs = _coerce_args_for_call(fn, tuple(args), dict(kwargs))
            result = fn(*call_args, **call_kwargs)
            result = await _maybe_await(result)
            return ensure_shape(
                result,
                symbol=sym,
                provider_version=info.version,
                fn_name=self.name,
                result_kind=self.result_kind,
            )

        async def _call_fallback() -> Any:
            if self.fallback_factory is None:
                raise RuntimeError("fallback_missing")
            result = self.fallback_factory(*args, **kwargs)
            result = await _maybe_await(result)
            return ensure_shape(
                result,
                symbol=sym,
                provider_version=None,
                fn_name=self.name,
                result_kind=self.result_kind,
            )

        try:
            result = await self._backoff.run(_call_canonical)
            return result
        except Exception as exc:
            if _env_bool("SHIM_YAHOO_FALLBACK_ON_ERROR", True) and self.fallback_factory is not None:
                try:
                    return await self._backoff.run(_call_fallback)
                except Exception as fallback_exc:
                    return error_payload(
                        sym,
                        f"{exc.__class__.__name__}:{exc}; fallback:"
                        f"{fallback_exc.__class__.__name__}:{fallback_exc}",
                        fn_name=self.name,
                    )
            return error_payload(sym, f"{exc.__class__.__name__}:{exc}", fn_name=self.name)


# =============================================================================
# Public Shim Callables
# =============================================================================

fetch_quote = ShimFunction(
    "fetch_quote",
    canonical_names=("fetch_quote", "get_quote", "yahoo_chart_quote"),
    fallback_factory=lambda symbol, *args, **kwargs: error_payload(symbol, "canonical_unavailable", "fetch_quote"),
    result_kind="quote",
)

get_quote = ShimFunction(
    "get_quote",
    canonical_names=("get_quote", "fetch_quote", "yahoo_chart_quote"),
    fallback_factory=lambda symbol, *args, **kwargs: error_payload(symbol, "canonical_unavailable", "get_quote"),
    result_kind="quote",
)

yahoo_chart_quote = ShimFunction(
    "yahoo_chart_quote",
    canonical_names=("yahoo_chart_quote", "fetch_quote", "get_quote"),
    fallback_factory=lambda symbol, *args, **kwargs: error_payload(symbol, "canonical_unavailable", "yahoo_chart_quote"),
    result_kind="quote",
)

get_quote_patch = ShimFunction(
    "get_quote_patch",
    canonical_names=("get_quote_patch", "fetch_quote_patch", "fetch_enriched_quote_patch", "fetch_quote"),
    fallback_factory=lambda symbol, base=None, *args, **kwargs: error_payload(symbol, "canonical_unavailable", "get_quote_patch"),
    result_kind="patch",
)

fetch_quote_patch = ShimFunction(
    "fetch_quote_patch",
    canonical_names=("fetch_quote_patch", "get_quote_patch", "fetch_enriched_quote_patch", "fetch_quote"),
    fallback_factory=lambda symbol, base=None, *args, **kwargs: error_payload(symbol, "canonical_unavailable", "fetch_quote_patch"),
    result_kind="patch",
)

fetch_enriched_quote_patch = ShimFunction(
    "fetch_enriched_quote_patch",
    canonical_names=("fetch_enriched_quote_patch", "fetch_quote_patch", "get_quote_patch", "fetch_quote"),
    fallback_factory=lambda symbol, base=None, *args, **kwargs: error_payload(symbol, "canonical_unavailable", "fetch_enriched_quote_patch"),
    result_kind="patch",
)

fetch_quote_and_enrichment_patch = ShimFunction(
    "fetch_quote_and_enrichment_patch",
    canonical_names=("fetch_quote_and_enrichment_patch", "fetch_enriched_quote_patch", "fetch_quote_patch", "fetch_quote"),
    fallback_factory=lambda symbol, base=None, *args, **kwargs: error_payload(symbol, "canonical_unavailable", "fetch_quote_and_enrichment_patch"),
    result_kind="patch",
)

fetch_quote_and_fundamentals_patch = ShimFunction(
    "fetch_quote_and_fundamentals_patch",
    canonical_names=("fetch_quote_and_fundamentals_patch", "fetch_enriched_quote_patch", "fetch_quote_patch", "fetch_quote"),
    fallback_factory=lambda symbol, base=None, *args, **kwargs: error_payload(symbol, "canonical_unavailable", "fetch_quote_and_fundamentals_patch"),
    result_kind="patch",
)

fetch_price_history = ShimFunction(
    "fetch_price_history",
    canonical_names=("fetch_price_history", "fetch_history", "fetch_ohlc_history", "fetch_prices"),
    fallback_factory=lambda *args, **kwargs: [],
    result_kind="history",
)

fetch_history = ShimFunction(
    "fetch_history",
    canonical_names=("fetch_history", "fetch_price_history", "fetch_ohlc_history", "fetch_prices"),
    fallback_factory=lambda *args, **kwargs: [],
    result_kind="history",
)

fetch_ohlc_history = ShimFunction(
    "fetch_ohlc_history",
    canonical_names=("fetch_ohlc_history", "fetch_history", "fetch_price_history", "fetch_prices"),
    fallback_factory=lambda *args, **kwargs: [],
    result_kind="history",
)

fetch_history_patch = ShimFunction(
    "fetch_history_patch",
    canonical_names=("fetch_history_patch", "fetch_history", "fetch_price_history", "fetch_ohlc_history"),
    fallback_factory=lambda *args, **kwargs: [],
    result_kind="history",
)

fetch_prices = ShimFunction(
    "fetch_prices",
    canonical_names=("fetch_prices", "fetch_history", "fetch_price_history", "fetch_ohlc_history"),
    fallback_factory=lambda *args, **kwargs: [],
    result_kind="history",
)


# =============================================================================
# Class Compatibility Wrapper
# =============================================================================

class YahooChartProvider:
    """Backward-compatible provider class."""

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        self._args = args
        self._kwargs = kwargs
        self._inner: Any = None
        self._lock = asyncio.Lock()

    async def _ensure_inner(self) -> None:
        """Ensure inner provider is initialized."""
        if self._inner is not None:
            return
        async with self._lock:
            if self._inner is not None:
                return
            info = await _PROVIDER_CACHE.get()
            if info.available and hasattr(info.module, "YahooChartProvider"):
                try:
                    self._inner = info.module.YahooChartProvider(*self._args, **self._kwargs)
                except Exception:
                    self._inner = None

    async def __aenter__(self) -> "YahooChartProvider":
        await self._ensure_inner()
        return self

    async def __aexit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        await self.aclose()

    async def aclose(self) -> None:
        """Close provider."""
        await self._ensure_inner()
        if self._inner is not None and hasattr(self._inner, "aclose"):
            try:
                value = self._inner.aclose()
                if inspect.isawaitable(value):
                    await value
            except Exception:
                pass
        self._inner = None

    async def _dispatch(
        self,
        method_names: Sequence[str],
        shim_fn: Callable[..., Awaitable[Any]],
        *args: Any,
        **kwargs: Any,
    ) -> Any:
        """Dispatch to inner provider or shim.

        v5.1.0 fix: result-kind classification now uses
        :func:`_classify_result_kind` so that history-indicating tokens
        take precedence over ``patch`` in the method name (e.g.
        ``fetch_history_patch`` is classified as "history", matching its
        ``ShimFunction`` declaration).
        """
        await self._ensure_inner()
        if self._inner is not None:
            for method_name in method_names:
                if hasattr(self._inner, method_name):
                    try:
                        method = getattr(self._inner, method_name)
                        call_args, call_kwargs = _coerce_args_for_call(
                            method, tuple(args), dict(kwargs)
                        )
                        value = method(*call_args, **call_kwargs)
                        value = await _maybe_await(value)
                        symbol = _safe_symbol(
                            kwargs.get("symbol")
                            or kwargs.get("ticker")
                            or (args[0] if args else "")
                        )
                        kind = _classify_result_kind(method_name)
                        return ensure_shape(
                            value,
                            symbol=symbol,
                            provider_version=None,
                            fn_name=method_name,
                            result_kind=kind,
                        )
                    except Exception:
                        continue
        return await shim_fn(*args, **kwargs)

    async def fetch_quote(self, symbol: str, *args: Any, **kwargs: Any) -> Dict[str, Any]:
        return await self._dispatch(("fetch_quote", "get_quote"), fetch_quote, symbol, *args, **kwargs)

    async def get_quote(self, symbol: str, *args: Any, **kwargs: Any) -> Dict[str, Any]:
        return await self._dispatch(("get_quote", "fetch_quote"), get_quote, symbol, *args, **kwargs)

    async def get_quote_patch(self, symbol: str, base: Optional[Dict[str, Any]] = None, *args: Any, **kwargs: Any) -> Dict[str, Any]:
        return await self._dispatch(("get_quote_patch", "fetch_quote_patch", "fetch_enriched_quote_patch"), get_quote_patch, symbol, base, *args, **kwargs)

    async def fetch_quote_patch(self, symbol: str, base: Optional[Dict[str, Any]] = None, *args: Any, **kwargs: Any) -> Dict[str, Any]:
        return await self._dispatch(("fetch_quote_patch", "get_quote_patch", "fetch_enriched_quote_patch"), fetch_quote_patch, symbol, base, *args, **kwargs)

    async def fetch_enriched_quote_patch(self, symbol: str, base: Optional[Dict[str, Any]] = None, *args: Any, **kwargs: Any) -> Dict[str, Any]:
        return await self._dispatch(("fetch_enriched_quote_patch", "fetch_quote_patch", "get_quote_patch"), fetch_enriched_quote_patch, symbol, base, *args, **kwargs)

    async def fetch_quote_and_enrichment_patch(self, symbol: str, base: Optional[Dict[str, Any]] = None, *args: Any, **kwargs: Any) -> Dict[str, Any]:
        return await self._dispatch(("fetch_quote_and_enrichment_patch", "fetch_enriched_quote_patch", "fetch_quote_patch"), fetch_quote_and_enrichment_patch, symbol, base, *args, **kwargs)

    async def fetch_quote_and_fundamentals_patch(self, symbol: str, base: Optional[Dict[str, Any]] = None, *args: Any, **kwargs: Any) -> Dict[str, Any]:
        return await self._dispatch(("fetch_quote_and_fundamentals_patch", "fetch_enriched_quote_patch", "fetch_quote_patch"), fetch_quote_and_fundamentals_patch, symbol, base, *args, **kwargs)

    async def fetch_price_history(self, *args: Any, **kwargs: Any) -> List[Dict[str, Any]]:
        return await self._dispatch(("fetch_price_history", "fetch_history", "fetch_ohlc_history", "fetch_prices"), fetch_price_history, *args, **kwargs)

    async def fetch_history(self, *args: Any, **kwargs: Any) -> List[Dict[str, Any]]:
        return await self._dispatch(("fetch_history", "fetch_price_history", "fetch_ohlc_history", "fetch_prices"), fetch_history, *args, **kwargs)

    async def fetch_ohlc_history(self, *args: Any, **kwargs: Any) -> List[Dict[str, Any]]:
        return await self._dispatch(("fetch_ohlc_history", "fetch_history", "fetch_price_history", "fetch_prices"), fetch_ohlc_history, *args, **kwargs)

    async def fetch_history_patch(self, *args: Any, **kwargs: Any) -> List[Dict[str, Any]]:
        return await self._dispatch(("fetch_history_patch", "fetch_history", "fetch_price_history", "fetch_ohlc_history"), fetch_history_patch, *args, **kwargs)

    async def fetch_prices(self, *args: Any, **kwargs: Any) -> List[Dict[str, Any]]:
        return await self._dispatch(("fetch_prices", "fetch_history", "fetch_price_history", "fetch_ohlc_history"), fetch_prices, *args, **kwargs)


# =============================================================================
# Utilities
# =============================================================================

async def get_provider_status() -> Dict[str, Any]:
    """Get provider status."""
    info = await _PROVIDER_CACHE.get()
    return {
        "shim_version": SHIM_VERSION,
        "provider_version": PROVIDER_VERSION,
        "data_source": DATA_SOURCE,
        "canonical_available": info.available,
        "canonical_version": info.version if info.available else None,
        "canonical_origin_path": info.origin_path if info.available else None,
        "canonical_error": info.error if not info.available else None,
        "ttl_sec": _PROVIDER_CACHE.ttl,
        "timestamp_utc": _utc_iso(),
        "timestamp_riyadh": _riyadh_iso(),
    }


async def clear_cache() -> None:
    """Clear provider cache.

    Kept ``async`` for API compatibility with callers written against
    v5.0.0 (even though the implementation is synchronous, since
    rebinding a module-level name is not an async operation).
    """
    global _PROVIDER_CACHE
    _PROVIDER_CACHE = ProviderCache()


def get_version() -> str:
    """Get shim version."""
    return SHIM_VERSION


# =============================================================================
# Module Exports
# =============================================================================

__all__ = [
    "SHIM_VERSION",
    "VERSION",
    "PROVIDER_VERSION",
    "DATA_SOURCE",
    "DataQuality",
    "YahooChartProvider",
    "fetch_quote",
    "get_quote",
    "get_quote_patch",
    "fetch_quote_patch",
    "fetch_enriched_quote_patch",
    "fetch_quote_and_enrichment_patch",
    "fetch_quote_and_fundamentals_patch",
    "yahoo_chart_quote",
    "fetch_price_history",
    "fetch_history",
    "fetch_ohlc_history",
    "fetch_history_patch",
    "fetch_prices",
    "get_provider_status",
    "clear_cache",
    "get_version",
]
