#!/usr/bin/env python3
"""
core/yahoo_chart_provider.py
================================================================================
YAHOO CHART COMPATIBILITY SHIM — v4.2.0
================================================================================
SAFE • STARTUP-FRIENDLY • CANONICAL-DELEGATING • HISTORY-AWARE •
COMMODITY/FX-TOLERANT • ROUTE/ENGINE COMPATIBLE • PAYLOAD-RECOVERY ENHANCED

Purpose
-------
Compatibility shim for legacy imports that still reference
`core.yahoo_chart_provider` instead of the canonical provider module.

What this revision improves
---------------------------
- FIX: stronger normalization from nested Yahoo payloads:
      - chart.result[0].meta / indicators
      - quoteResponse.result[0]
      - spark.result[0].response[0].meta
      - rows/history/prices/items/records
      - common candle / OHLC array shapes
      - DataFrame-like dict-of-dicts history payloads
- FIX: derives quote-like fields from history more aggressively when live quote
      payloads are sparse, especially for commodities / FX.
- FIX: preserves stronger base fields during patch merges instead of letting thin
      fallback payloads overwrite better upstream values.
- FIX: broader commodity/FX identity defaults and display-name normalization.
- FIX: better synthetic quote recovery for weak payloads, including last trade,
      52W range, volume averages, RSI, volatility, drawdown, VaR, Sharpe.
- FIX: safer compatibility wrapper for class-based callers.
- FIX: keeps import-time behavior network-safe and startup-safe.
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
import threading
import time
from dataclasses import asdict, dataclass, field, is_dataclass
from datetime import datetime, timedelta, timezone
from enum import Enum
from functools import lru_cache
from importlib import import_module
from typing import Any, Awaitable, Callable, Dict, Iterable, List, Optional, Sequence, Tuple, TypeVar, Union

# =============================================================================
# Versioning / constants
# =============================================================================

SHIM_VERSION = "4.2.0"
VERSION = SHIM_VERSION
PROVIDER_VERSION = SHIM_VERSION
DATA_SOURCE = "yahoo_chart"
CANONICAL_IMPORT_PATHS = (
    "core.providers.yahoo_chart_provider",
    "providers.yahoo_chart_provider",
)
MIN_CANONICAL_VERSION = "0.4.0"

UTC = timezone.utc
RIYADH = timezone(timedelta(hours=3))

_T = TypeVar("_T")

_TRUTHY = {"1", "true", "yes", "y", "on", "t"}
_FALSY = {"0", "false", "no", "n", "off", "f"}

PRICE_FIELD_ALIASES = (
    "current_price",
    "price",
    "last_price",
    "last",
    "regularMarketPrice",
    "regular_market_price",
    "postMarketPrice",
    "preMarketPrice",
    "bid",
    "ask",
    "close",
    "last_close",
    "navPrice",
)
PREV_CLOSE_ALIASES = (
    "previous_close",
    "prev_close",
    "regularMarketPreviousClose",
    "chartPreviousClose",
    "previousClose",
)
OPEN_ALIASES = ("open", "regularMarketOpen")
HIGH_ALIASES = ("day_high", "high", "regularMarketDayHigh", "fiftyTwoWeekHigh", "dayHigh")
LOW_ALIASES = ("day_low", "low", "regularMarketDayLow", "fiftyTwoWeekLow", "dayLow")
VOLUME_ALIASES = ("volume", "regularMarketVolume", "averageDailyVolume3Month", "avgVolume", "averageVolume")
NAME_ALIASES = (
    "name",
    "shortName",
    "longName",
    "displayName",
    "instrument_name",
)
EXCHANGE_ALIASES = (
    "exchange",
    "fullExchangeName",
    "exchangeName",
    "market",
)
CURRENCY_ALIASES = ("currency", "financialCurrency")
HISTORY_ROW_KEYS = (
    "timestamp",
    "date",
    "open",
    "high",
    "low",
    "close",
    "adj_close",
    "adjclose",
    "volume",
)

# =============================================================================
# Env helpers / time helpers
# =============================================================================


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
        value = int(float((os.getenv(name) or str(default)).strip()))
    except Exception:
        value = default
    if lo is not None:
        value = max(lo, value)
    if hi is not None:
        value = min(hi, value)
    return value


def _env_float(name: str, default: float, lo: Optional[float] = None, hi: Optional[float] = None) -> float:
    try:
        value = float((os.getenv(name) or str(default)).strip())
    except Exception:
        value = default
    if lo is not None:
        value = max(lo, value)
    if hi is not None:
        value = min(hi, value)
    return value


def _utc_iso(dt: Optional[datetime] = None) -> str:
    d = dt or datetime.now(UTC)
    if d.tzinfo is None:
        d = d.replace(tzinfo=UTC)
    return d.astimezone(UTC).isoformat()


def _riyadh_iso(dt: Optional[datetime] = None) -> str:
    d = dt or datetime.now(UTC)
    if d.tzinfo is None:
        d = d.replace(tzinfo=UTC)
    return d.astimezone(RIYADH).isoformat()


# =============================================================================
# Logging / optional JSON / metrics / tracing
# =============================================================================

_LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").strip().upper()
logging.basicConfig(
    level=getattr(logging, _LOG_LEVEL, logging.INFO),
    format="%(asctime)s | %(levelname)8s | %(name)s | %(message)s",
)
logger = logging.getLogger("core.yahoo_chart_shim")

try:
    import orjson  # type: ignore

    def json_dumps(v: Any, *, default: Any = str) -> str:
        return orjson.dumps(v, default=default).decode("utf-8")

    def json_loads(v: Union[str, bytes]) -> Any:
        if isinstance(v, str):
            v = v.encode("utf-8")
        return orjson.loads(v)

except Exception:
    import json

    def json_dumps(v: Any, *, default: Any = str) -> str:
        return json.dumps(v, default=default, ensure_ascii=False)

    def json_loads(v: Union[str, bytes]) -> Any:
        if isinstance(v, (bytes, bytearray)):
            v = v.decode("utf-8", errors="replace")
        return json.loads(v)

try:
    from prometheus_client import Counter, Gauge, Histogram  # type: ignore

    shim_requests_total = Counter(
        "tfb_yahoo_shim_requests_total",
        "Total requests handled by yahoo shim",
        ["fn", "status"],
    )
    shim_request_seconds = Histogram(
        "tfb_yahoo_shim_request_seconds",
        "Yahoo shim request duration (seconds)",
        ["fn"],
    )
    shim_provider_available = Gauge(
        "tfb_yahoo_shim_provider_available",
        "Canonical provider availability (1/0)",
    )
    shim_cb_state = Gauge(
        "tfb_yahoo_shim_circuit_state",
        "Circuit state (0=closed,1=half,2=open)",
    )
except Exception:
    class _DummyMetric:
        def labels(self, *args, **kwargs):
            return self

        def inc(self, *args, **kwargs):
            return None

        def observe(self, *args, **kwargs):
            return None

        def set(self, *args, **kwargs):
            return None

    shim_requests_total = _DummyMetric()
    shim_request_seconds = _DummyMetric()
    shim_provider_available = _DummyMetric()
    shim_cb_state = _DummyMetric()

try:
    from opentelemetry import trace  # type: ignore
    from opentelemetry.trace import Status, StatusCode  # type: ignore

    _TRACER = trace.get_tracer(__name__)
    _OTEL_AVAILABLE = True
except Exception:
    Status = None  # type: ignore
    StatusCode = None  # type: ignore
    _TRACER = None
    _OTEL_AVAILABLE = False

_TRACING_ENABLED = _env_bool("CORE_TRACING_ENABLED", False)


class TraceContext:
    def __init__(self, name: str, attributes: Optional[Dict[str, Any]] = None):
        self.name = name
        self.attributes = attributes or {}
        self._cm = None
        self._span = None

    def __enter__(self):
        if _OTEL_AVAILABLE and _TRACING_ENABLED and _TRACER is not None:
            try:
                self._cm = _TRACER.start_as_current_span(self.name)
                self._span = self._cm.__enter__()
                for key, value in self.attributes.items():
                    try:
                        self._span.set_attribute(str(key), value)
                    except Exception:
                        pass
            except Exception:
                self._cm = None
                self._span = None
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        try:
            if self._span is not None and exc_val is not None and Status is not None and StatusCode is not None:
                try:
                    self._span.record_exception(exc_val)
                except Exception:
                    pass
                try:
                    self._span.set_status(Status(StatusCode.ERROR, str(exc_val)))
                except Exception:
                    pass
        finally:
            if self._cm is not None:
                try:
                    return self._cm.__exit__(exc_type, exc_val, exc_tb)
                except Exception:
                    return False
        return False

    async def __aenter__(self):
        return self.__enter__()

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        return self.__exit__(exc_type, exc_val, exc_tb)


# =============================================================================
# Data quality / telemetry
# =============================================================================


class DataQuality(str, Enum):
    EXCELLENT = "EXCELLENT"
    HIGH = "HIGH"
    MEDIUM = "MEDIUM"
    LOW = "LOW"
    STALE = "STALE"
    OK = "OK"
    PARTIAL = "PARTIAL"
    MISSING = "MISSING"
    ERROR = "ERROR"


@dataclass(slots=True)
class CallMetrics:
    fn: str
    start_mono: float
    end_mono: float
    ok: bool
    error_type: Optional[str] = None
    duration_ms: float = field(init=False)

    def __post_init__(self):
        self.duration_ms = max(0.0, (self.end_mono - self.start_mono) * 1000.0)


class TelemetryCollector:
    def __init__(self, max_items: int = 1500):
        self._lock = threading.RLock()
        self._max = max(200, int(max_items))
        self._calls: List[CallMetrics] = []

    def record(self, metric: CallMetrics) -> None:
        if not _env_bool("SHIM_YAHOO_TELEMETRY", True):
            return
        with self._lock:
            self._calls.append(metric)
            if len(self._calls) > self._max:
                self._calls = self._calls[-self._max :]

    def snapshot(self) -> Dict[str, Any]:
        with self._lock:
            calls = list(self._calls)

        if not calls:
            return {}

        total = len(calls)
        ok = sum(1 for c in calls if c.ok)
        durations = sorted(c.duration_ms for c in calls)
        p50 = durations[total // 2]
        p95 = durations[min(total - 1, int(total * 0.95))]
        p99 = durations[min(total - 1, int(total * 0.99))]

        by_fn: Dict[str, Dict[str, Any]] = {}
        for c in calls:
            row = by_fn.setdefault(c.fn, {"calls": 0, "ok": 0, "fail": 0, "dur_ms_sum": 0.0})
            row["calls"] += 1
            row["ok"] += 1 if c.ok else 0
            row["fail"] += 0 if c.ok else 1
            row["dur_ms_sum"] += c.duration_ms

        for row in by_fn.values():
            row["avg_duration_ms"] = row["dur_ms_sum"] / max(1, row["calls"])
            row["success_rate"] = row["ok"] / max(1, row["calls"])
            row.pop("dur_ms_sum", None)

        return {
            "total_calls": total,
            "success_rate": ok / max(1, total),
            "avg_duration_ms": sum(durations) / max(1, total),
            "p50_duration_ms": p50,
            "p95_duration_ms": p95,
            "p99_duration_ms": p99,
            "by_fn": by_fn,
        }


_TELEMETRY = TelemetryCollector()


def _track(fn_name: str, start_mono: float, ok: bool, err: Optional[BaseException] = None) -> None:
    _TELEMETRY.record(
        CallMetrics(
            fn=fn_name,
            start_mono=start_mono,
            end_mono=time.monotonic(),
            ok=ok,
            error_type=(err.__class__.__name__ if err else None),
        )
    )


# =============================================================================
# General helpers
# =============================================================================


def _safe_symbol(value: Any) -> str:
    try:
        return str(value or "").strip().upper()
    except Exception:
        return ""


def _extract_version_parts(value: str) -> Tuple[int, int, int]:
    raw = str(value or "").strip()
    nums = re.findall(r"\d+", raw)
    parts = [int(x) for x in nums[:3]]
    while len(parts) < 3:
        parts.append(0)
    return (parts[0], parts[1], parts[2])


def _version_tuple(value: str) -> Tuple[int, int, int]:
    try:
        return _extract_version_parts(value)
    except Exception:
        return (0, 0, 0)


def _is_nonempty(value: Any) -> bool:
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
    if not isinstance(obj, dict):
        return 0
    total = 0
    for value in obj.values():
        if _is_nonempty(value):
            total += 1
    return total


def _coerce_float(value: Any) -> Optional[float]:
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
    num = _coerce_float(value)
    if num is None:
        return None
    try:
        return int(num)
    except Exception:
        return None


def _safe_get(mapping: Any, *keys: str, default: Any = None) -> Any:
    if not isinstance(mapping, dict):
        return default
    for key in keys:
        if key in mapping and mapping[key] is not None:
            return mapping[key]
    return default


def _deep_get(mapping: Any, path: Sequence[Union[str, int]], default: Any = None) -> Any:
    cur = mapping
    try:
        for part in path:
            if isinstance(part, int):
                if not isinstance(cur, (list, tuple)) or part >= len(cur):
                    return default
                cur = cur[part]
            else:
                if not isinstance(cur, dict) or part not in cur:
                    return default
                cur = cur[part]
        return cur
    except Exception:
        return default


def _to_dict(value: Any) -> Dict[str, Any]:
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
    out = dict(base or {})
    for key, value in (patch or {}).items():
        if _is_nonempty(value) or key not in out:
            out[key] = value
    return out


def _merge_prefer_base(base: Optional[Dict[str, Any]], patch: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    out = dict(base or {})
    for key, value in (patch or {}).items():
        if not _is_nonempty(out.get(key)) and _is_nonempty(value):
            out[key] = value
        elif key not in out:
            out[key] = value
    return out


def _normalize_timestamp(value: Any) -> Optional[str]:
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


# =============================================================================
# Symbol classification / defaults
# =============================================================================


def _symbol_kind(symbol: str) -> str:
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
    root = sym.replace("=X", "")
    if len(root) == 6:
        return f"{root[:3]}/{root[3:]}"
    if len(root) == 3:
        return f"{root}/USD"
    return sym


def _identity_defaults_for_symbol(symbol: str) -> Dict[str, Any]:
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
        defaults.update(
            {
                "name": _fx_display_name(sym),
                "asset_class": "FX",
                "exchange": "FX",
                "currency": root[3:] if len(root) == 6 else "USD",
                "country": "Global",
                "sector": "Foreign Exchange",
                "industry": "Currency Pair",
            }
        )
    elif kind == "future":
        name_map = {
            "GC=F": "Gold Futures",
            "SI=F": "Silver Futures",
            "HG=F": "Copper Futures",
            "CL=F": "Crude Oil Futures",
            "BZ=F": "Brent Crude Futures",
            "NG=F": "Natural Gas Futures",
        }
        defaults.update(
            {
                "name": name_map.get(sym, sym),
                "asset_class": "Commodity",
                "exchange": "Futures",
                "currency": "USD",
                "country": "Global",
                "sector": "Commodities",
                "industry": "Futures Contract",
            }
        )
    elif kind == "crypto":
        defaults.update(
            {
                "asset_class": "Crypto",
                "exchange": "Crypto",
                "country": "Global",
                "sector": "Digital Assets",
                "industry": "Cryptocurrency",
            }
        )
    elif kind == "ksa_equity":
        defaults.update(
            {
                "asset_class": "Equity",
                "exchange": "Tadawul",
                "currency": "SAR",
                "country": "Saudi Arabia",
            }
        )
    elif kind == "fund":
        defaults.update(
            {
                "asset_class": "Fund",
                "exchange": "ETF",
                "country": "Global",
                "sector": "Funds",
                "industry": "Exchange Traded Fund",
            }
        )
    return defaults


# =============================================================================
# History / quote normalization
# =============================================================================


def _ensure_history_row(row: Dict[str, Any]) -> Dict[str, Any]:
    out = dict(row)
    timestamp = _normalize_timestamp(_safe_get(out, "timestamp", "date"))
    if timestamp:
        out["timestamp"] = timestamp
    for key in ("open", "high", "low", "close", "adj_close", "adjclose"):
        if key in out:
            out[key] = _coerce_float(out[key])
    if "volume" in out:
        out["volume"] = _coerce_int(out["volume"])
    return out


def _rows_from_parallel_arrays(payload: Dict[str, Any]) -> List[Dict[str, Any]]:
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
    keys = {k.lower(): k for k in payload.keys()}
    core_cols = [keys.get(k) for k in ("open", "high", "low", "close")]
    if not any(core_cols):
        return []

    # pandas.to_dict() often yields {column: {index: value}}
    if not any(isinstance(payload.get(col), dict) for col in core_cols if col):
        return []

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
            "open": payload.get(keys.get("open"), {}).get(idx) if keys.get("open") else None,
            "high": payload.get(keys.get("high"), {}).get(idx) if keys.get("high") else None,
            "low": payload.get(keys.get("low"), {}).get(idx) if keys.get("low") else None,
            "close": payload.get(keys.get("close"), {}).get(idx) if keys.get("close") else None,
            "adj_close": payload.get(keys.get("adj_close") or keys.get("adjclose"), {}).get(idx) if keys.get("adj_close") or keys.get("adjclose") else None,
            "volume": payload.get(keys.get("volume"), {}).get(idx) if keys.get("volume") else None,
        }
        row = _ensure_history_row(row)
        if any(_is_nonempty(v) for v in row.values()):
            rows.append(row)
    return rows


def _extract_candidate_dicts(payload_dict: Dict[str, Any]) -> List[Dict[str, Any]]:
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


def _extract_history_rows(payload: Any) -> List[Dict[str, Any]]:
    if payload is None:
        return []

    if isinstance(payload, list):
        rows: List[Dict[str, Any]] = []
        for item in payload:
            if isinstance(item, dict):
                if any(key in item for key in HISTORY_ROW_KEYS):
                    rows.append(_ensure_history_row(item))
                else:
                    nested = _extract_history_rows(item)
                    if nested:
                        rows.extend(nested)
            else:
                as_dict = _to_dict(item)
                if as_dict:
                    rows.extend(_extract_history_rows(as_dict))
        return rows

    payload_dict = _to_dict(payload)
    if not payload_dict:
        return []

    if any(key in payload_dict for key in HISTORY_ROW_KEYS):
        return [_ensure_history_row(payload_dict)]

    for key in ("rows", "history", "prices", "data", "items", "records", "candles", "ohlcv"):
        nested = payload_dict.get(key)
        rows = _extract_history_rows(nested)
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
        rows = _extract_history_rows(nested)
        if rows:
            return rows

    return []


def _derive_quote_from_rows(rows: Sequence[Dict[str, Any]]) -> Dict[str, Any]:
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

    out: Dict[str, Any] = {
        "current_price": current,
        "price": current,
        "previous_close": previous_close,
        "open": latest_open if latest_open is not None else (opens_valid[-1] if opens_valid else None),
        "day_high": latest_high if latest_high is not None else current,
        "day_low": latest_low if latest_low is not None else current,
        "volume": latest_volume,
        "52w_high": max(highs_valid) if highs_valid else None,
        "52w_low": min(lows_valid) if lows_valid else None,
        "avg_volume_30d": int(sum(vols_valid[-30:]) / len(vols_valid[-30:])) if vols_valid else None,
        "avg_volume_10d": int(sum(vols_valid[-10:]) / len(vols_valid[-10:])) if vols_valid else None,
        "history_points": len(cleaned),
        "history_last_timestamp": _normalize_timestamp(_safe_get(latest, "timestamp", "date")),
    }

    if current is not None and previous_close not in (None, 0):
        price_change = current - previous_close
        out["price_change"] = price_change
        out["percent_change"] = (price_change / previous_close) * 100.0

    if current is not None and highs_valid and lows_valid and max(highs_valid) != min(lows_valid):
        lo = min(lows_valid)
        hi = max(highs_valid)
        out["52w_position_pct"] = ((current - lo) / (hi - lo)) * 100.0

    if len(closes_valid) >= 2:
        rets: List[float] = []
        for i in range(1, len(closes_valid)):
            prev = closes_valid[i - 1]
            cur = closes_valid[i]
            if prev not in (None, 0) and cur is not None:
                rets.append((cur / prev) - 1.0)
        if rets:
            mean_ret = sum(rets) / len(rets)
            if len(rets) >= 2:
                variance = sum((r - mean_ret) ** 2 for r in rets) / max(1, len(rets) - 1)
                daily_vol = math.sqrt(max(0.0, variance))
                out["volatility_30d"] = daily_vol * math.sqrt(min(30, len(rets)))
                out["volatility_90d"] = daily_vol * math.sqrt(min(90, len(rets)))
            if current is not None and closes_valid:
                peak = closes_valid[0]
                max_drawdown = 0.0
                for price in closes_valid:
                    peak = max(peak, price)
                    if peak not in (None, 0):
                        dd = (price / peak) - 1.0
                        max_drawdown = min(max_drawdown, dd)
                out["max_drawdown_1y"] = max_drawdown
            if len(rets) >= 5:
                downside = sorted(rets)[max(0, int(len(rets) * 0.05) - 1)]
                out["var_95_1d"] = downside
            if len(rets) >= 5:
                denom = math.sqrt(sum((r - mean_ret) ** 2 for r in rets) / max(1, len(rets) - 1))
                if denom > 0:
                    out["sharpe_1y"] = (mean_ret / denom) * math.sqrt(min(252, len(rets)))
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
                    out["rsi_14"] = 100.0
                elif avg_loss > 0:
                    rs = avg_gain / avg_loss
                    out["rsi_14"] = 100.0 - (100.0 / (1.0 + rs))

    return {k: v for k, v in out.items() if _is_nonempty(v)}


def _derive_quote_from_chart_payload(payload: Dict[str, Any]) -> Dict[str, Any]:
    chart_root = _deep_get(payload, ["chart", "result", 0], default=None)
    if not isinstance(chart_root, dict):
        chart_root = payload

    meta = chart_root.get("meta") if isinstance(chart_root.get("meta"), dict) else {}
    rows = _rows_from_parallel_arrays(chart_root)
    if not rows:
        rows = _rows_from_candle_arrays(chart_root)
    derived = _derive_quote_from_rows(rows)

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
        "52w_high": _coerce_float(_safe_get(meta, "fiftyTwoWeekHigh", "fifty_two_week_high", "52w_high")),
        "52w_low": _coerce_float(_safe_get(meta, "fiftyTwoWeekLow", "fifty_two_week_low", "52w_low")),
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


def _classify_quality(payload: Dict[str, Any]) -> str:
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


def _normalize_quote_payload(payload: Any, *, symbol: str) -> Dict[str, Any]:
    sym = _safe_symbol(symbol)
    defaults = _identity_defaults_for_symbol(sym)

    if payload is None:
        return defaults

    payload_dict = _to_dict(payload)
    if not payload_dict and isinstance(payload, list):
        rows = _extract_history_rows(payload)
        return _merge_nonempty(defaults, _derive_quote_from_rows(rows))
    if not payload_dict:
        return defaults

    out = dict(defaults)

    for candidate in _extract_candidate_dicts(payload_dict):
        for alias in NAME_ALIASES:
            if _is_nonempty(candidate.get(alias)):
                out["name"] = candidate.get(alias)
                break
        for alias in EXCHANGE_ALIASES:
            if _is_nonempty(candidate.get(alias)):
                out["exchange"] = candidate.get(alias)
                break
        for alias in CURRENCY_ALIASES:
            if _is_nonempty(candidate.get(alias)):
                out["currency"] = candidate.get(alias)
                break

        out = _merge_nonempty(
            out,
            {
                "current_price": _coerce_float(_safe_get(candidate, *PRICE_FIELD_ALIASES)),
                "price": _coerce_float(_safe_get(candidate, *PRICE_FIELD_ALIASES)),
                "previous_close": _coerce_float(_safe_get(candidate, *PREV_CLOSE_ALIASES)),
                "open": _coerce_float(_safe_get(candidate, *OPEN_ALIASES)),
                "day_high": _coerce_float(_safe_get(candidate, *HIGH_ALIASES)),
                "day_low": _coerce_float(_safe_get(candidate, *LOW_ALIASES)),
                "volume": _coerce_int(_safe_get(candidate, *VOLUME_ALIASES)),
                "52w_high": _coerce_float(_safe_get(candidate, "52w_high", "fiftyTwoWeekHigh", "fifty_two_week_high")),
                "52w_low": _coerce_float(_safe_get(candidate, "52w_low", "fiftyTwoWeekLow", "fifty_two_week_low")),
                "market_cap": _coerce_float(_safe_get(candidate, "market_cap", "marketCap")),
                "beta_5y": _coerce_float(_safe_get(candidate, "beta_5y", "beta", "beta5YMonthly")),
                "dividend_yield": _coerce_float(_safe_get(candidate, "dividend_yield", "dividendYield")),
                "long_name": _safe_get(candidate, "longName", "long_name"),
                "short_name": _safe_get(candidate, "shortName", "short_name"),
                "market_state": _safe_get(candidate, "market_state", "marketState"),
                "instrument_type": _safe_get(candidate, "instrumentType", "quoteType", "typeDisp"),
                "exchange_timezone": _safe_get(candidate, "exchangeTimezoneName", "timezone"),
            },
        )

    chart_derived = _derive_quote_from_chart_payload(payload_dict)
    history_rows = _extract_history_rows(payload_dict)
    history_derived = _derive_quote_from_rows(history_rows)

    out = _merge_nonempty(out, chart_derived)
    out = _merge_nonempty(out, history_derived)

    current = _coerce_float(_safe_get(out, "current_price", "price"))
    prev = _coerce_float(out.get("previous_close"))
    if current is not None and prev not in (None, 0):
        price_change = current - prev
        out["price_change"] = price_change
        out["percent_change"] = (price_change / prev) * 100.0

    warnings: List[str] = []
    for key in ("warnings", "warning", "notes", "message"):
        value = payload_dict.get(key)
        if isinstance(value, list):
            warnings.extend([str(x) for x in value if _is_nonempty(x)])
        elif _is_nonempty(value):
            warnings.append(str(value))
    if warnings:
        out["warnings"] = warnings

    return {k: v for k, v in out.items() if _is_nonempty(v) or k in {"symbol", "symbol_normalized", "requested_symbol", "instrument_type"}}


def _normalize_history_payload(payload: Any, *, symbol: str) -> List[Dict[str, Any]]:
    rows = _extract_history_rows(payload)
    if rows:
        return rows

    sym = _safe_symbol(symbol)
    quote = _normalize_quote_payload(payload, symbol=sym)
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


def _normalize_patch_payload(payload: Any, *, symbol: str, base: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    patch = _normalize_quote_payload(payload, symbol=symbol)
    merged = _merge_prefer_base(base or {}, patch)
    if not _is_nonempty(merged.get("current_price")) and not _is_nonempty(merged.get("price")):
        rows = _normalize_history_payload(payload, symbol=symbol)
        merged = _merge_prefer_base(merged, _derive_quote_from_rows(rows))
    return merged


def _ensure_shape(payload: Any, *, symbol: str, provider_version: Optional[str], fn_name: str, result_kind: str) -> Any:
    sym = _safe_symbol(symbol)

    if result_kind == "history":
        return _normalize_history_payload(payload, symbol=sym)

    if result_kind == "patch":
        base = None
        if isinstance(payload, dict):
            base_candidate = payload.get("base")
            if isinstance(base_candidate, dict):
                base = base_candidate
        out = _normalize_patch_payload(payload, symbol=sym, base=base)
    else:
        out = _normalize_quote_payload(payload, symbol=sym)

    out.setdefault("symbol", sym)
    out.setdefault("symbol_normalized", sym)
    out.setdefault("requested_symbol", sym)
    out.setdefault("data_source", DATA_SOURCE)
    out.setdefault("provider", DATA_SOURCE)
    out.setdefault("shim_version", SHIM_VERSION)
    if provider_version:
        out.setdefault("provider_version", provider_version)
    out.setdefault("last_updated_utc", _utc_iso())
    out.setdefault("last_updated_riyadh", _riyadh_iso())

    if out.get("error"):
        out.setdefault("status", "error")
        out.setdefault("data_quality", DataQuality.ERROR.value)
    else:
        out.setdefault("status", "ok")
        out.setdefault("data_quality", _classify_quality(out))

    out.setdefault("where", fn_name)
    return out


def _error_payload(symbol: str, err: str, *, fn_name: str) -> Dict[str, Any]:
    sym = _safe_symbol(symbol)
    out = _identity_defaults_for_symbol(sym)
    out.update(
        {
            "status": "error",
            "data_source": DATA_SOURCE,
            "provider": DATA_SOURCE,
            "data_quality": DataQuality.ERROR.value,
            "error": err,
            "where": fn_name,
            "shim_version": SHIM_VERSION,
            "last_updated_utc": _utc_iso(),
            "last_updated_riyadh": _riyadh_iso(),
        }
    )
    return out


# =============================================================================
# Async control helpers
# =============================================================================


class SingleFlight:
    def __init__(self):
        self._lock = asyncio.Lock()
        self._futs: Dict[str, asyncio.Future] = {}

    async def do(self, key: str, coro_fn: Callable[[], Awaitable[_T]]) -> _T:
        fut: Optional[asyncio.Future] = None
        leader = False

        async with self._lock:
            fut = self._futs.get(key)
            if fut is None:
                fut = asyncio.get_running_loop().create_future()
                self._futs[key] = fut
                leader = True

        if not leader:
            return await fut  # type: ignore[return-value]

        try:
            res = await coro_fn()
            if not fut.done():
                fut.set_result(res)
            return res
        except Exception as exc:
            if not fut.done():
                fut.set_exception(exc)
            raise
        finally:
            async with self._lock:
                self._futs.pop(key, None)


class FullJitterBackoff:
    def __init__(self, attempts: int = 2, base: float = 0.4, cap: float = 4.0):
        self.attempts = max(0, int(attempts))
        self.base = max(0.05, float(base))
        self.cap = max(self.base, float(cap))

    async def run(self, fn: Callable[[], Awaitable[_T]]) -> _T:
        last: Optional[BaseException] = None
        total_tries = max(1, self.attempts + 1)

        for idx in range(total_tries):
            try:
                return await fn()
            except Exception as exc:
                last = exc
                if idx >= total_tries - 1:
                    break
                temp = min(self.cap, self.base * (2 ** idx))
                await asyncio.sleep(random.uniform(0.0, temp))

        raise last if last else RuntimeError("retry_exhausted")


class CircuitState(str, Enum):
    CLOSED = "closed"
    HALF = "half_open"
    OPEN = "open"


class CircuitBreakerOpenError(RuntimeError):
    pass


class CircuitBreaker:
    def __init__(self, threshold: int, timeout_sec: float, half_open_calls: int):
        self.threshold = max(1, int(threshold))
        self.timeout_sec = max(1.0, float(timeout_sec))
        self.half_open_calls = max(1, int(half_open_calls))

        self._lock = asyncio.Lock()
        self.state = CircuitState.CLOSED
        self.failures = 0
        self.opened_mono: Optional[float] = None
        self.half_used = 0
        self.half_success = 0

        shim_cb_state.set(0)

    async def allow(self) -> None:
        async with self._lock:
            if self.state == CircuitState.CLOSED:
                shim_cb_state.set(0)
                return

            if self.state == CircuitState.OPEN:
                if self.opened_mono is None:
                    self.opened_mono = time.monotonic()
                if (time.monotonic() - self.opened_mono) >= self.timeout_sec:
                    self.state = CircuitState.HALF
                    self.half_used = 0
                    self.half_success = 0
                    shim_cb_state.set(1)
                    return
                shim_cb_state.set(2)
                raise CircuitBreakerOpenError("circuit_open")

            if self.state == CircuitState.HALF:
                shim_cb_state.set(1)
                if self.half_used >= self.half_open_calls:
                    raise CircuitBreakerOpenError("circuit_half_open_limit")
                self.half_used += 1

    async def on_ok(self) -> None:
        async with self._lock:
            if self.state == CircuitState.HALF:
                self.half_success += 1
                if self.half_success >= 2:
                    self.state = CircuitState.CLOSED
                    self.failures = 0
                    self.opened_mono = None
                    shim_cb_state.set(0)
            else:
                self.failures = 0

    async def on_fail(self) -> None:
        async with self._lock:
            if self.state == CircuitState.HALF:
                self.state = CircuitState.OPEN
                self.opened_mono = time.monotonic()
                self.failures = self.threshold
                shim_cb_state.set(2)
                return

            self.failures += 1
            if self.failures >= self.threshold:
                self.state = CircuitState.OPEN
                self.opened_mono = time.monotonic()
                shim_cb_state.set(2)


# =============================================================================
# Canonical provider cache
# =============================================================================


@dataclass(slots=True)
class ProviderInfo:
    module: Any
    version: str
    funcs: Dict[str, Callable[..., Any]]
    available: bool
    checked_mono: float
    origin_path: Optional[str] = None
    error: Optional[str] = None


class ProviderCache:
    def __init__(self):
        self.ttl = _env_float("SHIM_YAHOO_PROVIDER_TTL_SEC", 300.0, lo=10.0, hi=86400.0)
        self._lock = asyncio.Lock()
        self._info: Optional[ProviderInfo] = None
        self._sf = SingleFlight()
        self._cb = CircuitBreaker(
            threshold=_env_int("SHIM_YAHOO_CB_THRESHOLD", 3, lo=1, hi=20),
            timeout_sec=_env_float("SHIM_YAHOO_CB_TIMEOUT_SEC", 60.0, lo=5.0, hi=600.0),
            half_open_calls=_env_int("SHIM_YAHOO_CB_HALF_CALLS", 2, lo=1, hi=10),
        )

    def _valid(self) -> bool:
        return self._info is not None and (time.monotonic() - self._info.checked_mono) < self.ttl

    async def get(self) -> ProviderInfo:
        async with self._lock:
            if self._valid():
                shim_provider_available.set(1 if self._info and self._info.available else 0)
                return self._info  # type: ignore[return-value]

        info = await self._sf.do("provider_load", self._load)
        async with self._lock:
            self._info = info
        shim_provider_available.set(1 if info.available else 0)
        return info

    async def _load(self) -> ProviderInfo:
        async def _do_import() -> ProviderInfo:
            await self._cb.allow()
            try:
                mod = None
                origin_path = None
                last_err: Optional[BaseException] = None

                for path in CANONICAL_IMPORT_PATHS:
                    try:
                        mod = import_module(path)
                        origin_path = path
                        last_err = None
                        break
                    except Exception as exc:
                        last_err = exc
                        continue

                if mod is None:
                    raise ImportError(f"canonical_missing:{last_err.__class__.__name__ if last_err else 'unknown'}")

                ver = str(
                    getattr(mod, "PROVIDER_VERSION", getattr(mod, "VERSION", getattr(mod, "SHIM_VERSION", "unknown")))
                    or "unknown"
                )
                if _version_tuple(ver) < _version_tuple(MIN_CANONICAL_VERSION):
                    raise RuntimeError(f"canonical_version_too_old:{ver}")

                funcs: Dict[str, Callable[..., Any]] = {}
                for name in dir(mod):
                    if name.startswith("_"):
                        continue
                    try:
                        obj = getattr(mod, name)
                    except Exception:
                        continue
                    if callable(obj):
                        funcs[name] = obj

                await self._cb.on_ok()
                return ProviderInfo(
                    module=mod,
                    version=ver,
                    funcs=funcs,
                    available=True,
                    checked_mono=time.monotonic(),
                    origin_path=origin_path,
                    error=None,
                )
            except Exception as exc:
                await self._cb.on_fail()
                return ProviderInfo(
                    module=None,
                    version="unknown",
                    funcs={},
                    available=False,
                    checked_mono=time.monotonic(),
                    origin_path=None,
                    error=str(exc),
                )

        with TraceContext("yahoo_shim.provider_load"):
            return await _do_import()


_PROVIDER = ProviderCache()


# =============================================================================
# Signature helpers
# =============================================================================


@lru_cache(maxsize=256)
def _sig_params(fn: Callable[..., Any]) -> Dict[str, inspect.Parameter]:
    try:
        return dict(inspect.signature(fn).parameters)
    except Exception:
        return {}


def _adapt_kwargs(fn: Callable[..., Any], kwargs: Dict[str, Any]) -> Dict[str, Any]:
    try:
        params = _sig_params(fn)
        if not params:
            return dict(kwargs)
        if any(p.kind == inspect.Parameter.VAR_KEYWORD for p in params.values()):
            return dict(kwargs)
        return {k: v for k, v in kwargs.items() if k in params}
    except Exception:
        return dict(kwargs)


def _coerce_args_for_call(fn: Callable[..., Any], args: Tuple[Any, ...], kwargs: Dict[str, Any]) -> Tuple[Tuple[Any, ...], Dict[str, Any]]:
    params = _sig_params(fn)
    if not params:
        return args, dict(kwargs)

    arg_names = [name for name, p in params.items() if p.kind in (inspect.Parameter.POSITIONAL_ONLY, inspect.Parameter.POSITIONAL_OR_KEYWORD)]
    out_kwargs = _adapt_kwargs(fn, kwargs)

    if args:
        if len(arg_names) >= 1 and arg_names[0] in out_kwargs:
            out_kwargs.pop(arg_names[0], None)
        if len(args) >= 2 and len(arg_names) >= 2 and arg_names[1] in out_kwargs:
            out_kwargs.pop(arg_names[1], None)

    return args, out_kwargs


async def _maybe_await(value: Any) -> Any:
    if inspect.isawaitable(value):
        return await value
    return value


# =============================================================================
# Default handlers
# =============================================================================


async def _default_quote(symbol: str, *args, **kwargs) -> Dict[str, Any]:
    sym = _safe_symbol(symbol)
    out = _identity_defaults_for_symbol(sym)
    out.update(
        {
            "status": "error",
            "data_source": DATA_SOURCE,
            "provider": DATA_SOURCE,
            "data_quality": DataQuality.MISSING.value,
            "error": "canonical_unavailable",
            "warnings": ["No live provider data available"],
            "shim_version": SHIM_VERSION,
            "last_updated_utc": _utc_iso(),
            "last_updated_riyadh": _riyadh_iso(),
        }
    )
    return out


async def _default_patch(symbol: str, base: Optional[Dict[str, Any]] = None, *args, **kwargs) -> Dict[str, Any]:
    base_dict = dict(base or {})
    fallback = await _default_quote(symbol, *args, **kwargs)
    return _merge_prefer_base(base_dict, fallback)


async def _default_history(*args, **kwargs) -> List[Dict[str, Any]]:
    return []


# =============================================================================
# Shim callable
# =============================================================================


class ShimFunction:
    def __init__(
        self,
        name: str,
        *,
        canonical_names: Optional[Sequence[str]] = None,
        fallback_factory: Optional[Callable[..., Any]] = None,
        result_kind: str = "quote",
    ):
        self.name = name
        self.canonical_names = tuple(canonical_names or (name,))
        self.fallback_factory = fallback_factory
        self.result_kind = result_kind
        self._backoff = FullJitterBackoff(
            attempts=_env_int("SHIM_YAHOO_RETRY_ATTEMPTS", 2, lo=0, hi=6),
            base=_env_float("SHIM_YAHOO_RETRY_BASE_SEC", 0.35, lo=0.05, hi=5.0),
            cap=_env_float("SHIM_YAHOO_RETRY_CAP_SEC", 3.0, lo=0.2, hi=20.0),
        )

    async def __call__(self, *args, **kwargs) -> Any:
        start_mono = time.monotonic()
        sym = _safe_symbol(kwargs.get("symbol") or kwargs.get("ticker") or (args[0] if args else ""))

        async def _call_canonical() -> Any:
            info = await _PROVIDER.get()
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
            return _ensure_shape(
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
            return _ensure_shape(
                result,
                symbol=sym,
                provider_version=None,
                fn_name=self.name,
                result_kind=self.result_kind,
            )

        with TraceContext(f"yahoo_shim.{self.name}", {"symbol": sym, "fn": self.name}):
            try:
                t0 = time.monotonic()
                out = await self._backoff.run(_call_canonical)
                shim_requests_total.labels(fn=self.name, status="ok").inc()
                shim_request_seconds.labels(fn=self.name).observe(max(0.0, time.monotonic() - t0))
                _track(self.name, start_mono, True)
                return out
            except Exception as exc:
                shim_requests_total.labels(fn=self.name, status="err").inc()
                _track(self.name, start_mono, False, exc)

                if _env_bool("SHIM_YAHOO_FALLBACK_ON_ERROR", True) and self.fallback_factory is not None:
                    try:
                        t0 = time.monotonic()
                        out = await self._backoff.run(_call_fallback)
                        shim_requests_total.labels(fn=self.name, status="fallback").inc()
                        shim_request_seconds.labels(fn=self.name).observe(max(0.0, time.monotonic() - t0))
                        return out
                    except Exception as fallback_exc:
                        return _error_payload(
                            sym,
                            f"{exc.__class__.__name__}:{exc}; fallback:{fallback_exc.__class__.__name__}:{fallback_exc}",
                            fn_name=self.name,
                        )

                return _error_payload(sym, f"{exc.__class__.__name__}:{exc}", fn_name=self.name)


# =============================================================================
# Public shim callables
# =============================================================================

fetch_quote = ShimFunction(
    "fetch_quote",
    canonical_names=("fetch_quote", "get_quote", "yahoo_chart_quote"),
    fallback_factory=_default_quote,
    result_kind="quote",
)
get_quote = ShimFunction(
    "get_quote",
    canonical_names=("get_quote", "fetch_quote", "yahoo_chart_quote"),
    fallback_factory=_default_quote,
    result_kind="quote",
)
yahoo_chart_quote = ShimFunction(
    "yahoo_chart_quote",
    canonical_names=("yahoo_chart_quote", "fetch_quote", "get_quote"),
    fallback_factory=_default_quote,
    result_kind="quote",
)

get_quote_patch = ShimFunction(
    "get_quote_patch",
    canonical_names=("get_quote_patch", "fetch_quote_patch", "fetch_enriched_quote_patch", "fetch_quote"),
    fallback_factory=_default_patch,
    result_kind="patch",
)
fetch_quote_patch = ShimFunction(
    "fetch_quote_patch",
    canonical_names=("fetch_quote_patch", "get_quote_patch", "fetch_enriched_quote_patch", "fetch_quote"),
    fallback_factory=_default_patch,
    result_kind="patch",
)
fetch_enriched_quote_patch = ShimFunction(
    "fetch_enriched_quote_patch",
    canonical_names=("fetch_enriched_quote_patch", "fetch_quote_patch", "get_quote_patch", "fetch_quote"),
    fallback_factory=_default_patch,
    result_kind="patch",
)
fetch_quote_and_enrichment_patch = ShimFunction(
    "fetch_quote_and_enrichment_patch",
    canonical_names=("fetch_quote_and_enrichment_patch", "fetch_enriched_quote_patch", "fetch_quote_patch", "fetch_quote"),
    fallback_factory=_default_patch,
    result_kind="patch",
)
fetch_quote_and_fundamentals_patch = ShimFunction(
    "fetch_quote_and_fundamentals_patch",
    canonical_names=("fetch_quote_and_fundamentals_patch", "fetch_enriched_quote_patch", "fetch_quote_patch", "fetch_quote"),
    fallback_factory=_default_patch,
    result_kind="patch",
)

fetch_price_history = ShimFunction(
    "fetch_price_history",
    canonical_names=("fetch_price_history", "fetch_history", "fetch_ohlc_history", "fetch_prices"),
    fallback_factory=_default_history,
    result_kind="history",
)
fetch_history = ShimFunction(
    "fetch_history",
    canonical_names=("fetch_history", "fetch_price_history", "fetch_ohlc_history", "fetch_prices"),
    fallback_factory=_default_history,
    result_kind="history",
)
fetch_ohlc_history = ShimFunction(
    "fetch_ohlc_history",
    canonical_names=("fetch_ohlc_history", "fetch_history", "fetch_price_history", "fetch_prices"),
    fallback_factory=_default_history,
    result_kind="history",
)
fetch_history_patch = ShimFunction(
    "fetch_history_patch",
    canonical_names=("fetch_history_patch", "fetch_history", "fetch_price_history", "fetch_ohlc_history"),
    fallback_factory=_default_history,
    result_kind="history",
)
fetch_prices = ShimFunction(
    "fetch_prices",
    canonical_names=("fetch_prices", "fetch_history", "fetch_price_history", "fetch_ohlc_history"),
    fallback_factory=_default_history,
    result_kind="history",
)


# =============================================================================
# Class compatibility wrapper
# =============================================================================


class YahooChartProvider:
    """
    Backward-compatible provider class.
    Delegates to canonical class if available, otherwise to shim callables.
    """

    def __init__(self, *args, **kwargs):
        self._args = args
        self._kwargs = kwargs
        self._inner = None
        self._lock = asyncio.Lock()

    async def _ensure_inner(self) -> None:
        if self._inner is not None:
            return
        async with self._lock:
            if self._inner is not None:
                return
            info = await _PROVIDER.get()
            if info.available and hasattr(info.module, "YahooChartProvider"):
                try:
                    self._inner = info.module.YahooChartProvider(*self._args, **self._kwargs)
                except Exception:
                    self._inner = None

    async def __aenter__(self):
        await self._ensure_inner()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.aclose()

    async def aclose(self) -> None:
        await self._ensure_inner()
        if self._inner is not None and hasattr(self._inner, "aclose"):
            try:
                value = self._inner.aclose()
                if inspect.isawaitable(value):
                    await value
            except Exception:
                pass
        self._inner = None

    async def _dispatch(self, method_names: Sequence[str], shim_fn: Callable[..., Awaitable[Any]], *args, **kwargs) -> Any:
        await self._ensure_inner()
        if self._inner is not None:
            for method_name in method_names:
                if hasattr(self._inner, method_name):
                    try:
                        method = getattr(self._inner, method_name)
                        call_args, call_kwargs = _coerce_args_for_call(method, tuple(args), dict(kwargs))
                        value = method(*call_args, **call_kwargs)
                        value = await _maybe_await(value)
                        symbol = _safe_symbol(kwargs.get("symbol") or kwargs.get("ticker") or (args[0] if args else ""))
                        kind = "history" if ("history" in method_name or method_name in {"fetch_prices"}) else "quote"
                        if "patch" in method_name:
                            kind = "patch"
                        return _ensure_shape(value, symbol=symbol, provider_version=None, fn_name=method_name, result_kind=kind)
                    except Exception:
                        continue
        return await shim_fn(*args, **kwargs)

    async def fetch_quote(self, symbol: str, *args, **kwargs) -> Dict[str, Any]:
        return await self._dispatch(("fetch_quote", "get_quote"), fetch_quote, symbol, *args, **kwargs)

    async def get_quote(self, symbol: str, *args, **kwargs) -> Dict[str, Any]:
        return await self._dispatch(("get_quote", "fetch_quote"), get_quote, symbol, *args, **kwargs)

    async def get_quote_patch(self, symbol: str, base: Optional[Dict[str, Any]] = None, *args, **kwargs) -> Dict[str, Any]:
        return await self._dispatch(("get_quote_patch", "fetch_quote_patch", "fetch_enriched_quote_patch"), get_quote_patch, symbol, base, *args, **kwargs)

    async def fetch_quote_patch(self, symbol: str, base: Optional[Dict[str, Any]] = None, *args, **kwargs) -> Dict[str, Any]:
        return await self._dispatch(("fetch_quote_patch", "get_quote_patch", "fetch_enriched_quote_patch"), fetch_quote_patch, symbol, base, *args, **kwargs)

    async def fetch_enriched_quote_patch(self, symbol: str, base: Optional[Dict[str, Any]] = None, *args, **kwargs) -> Dict[str, Any]:
        return await self._dispatch(("fetch_enriched_quote_patch", "fetch_quote_patch", "get_quote_patch"), fetch_enriched_quote_patch, symbol, base, *args, **kwargs)

    async def fetch_quote_and_enrichment_patch(self, symbol: str, base: Optional[Dict[str, Any]] = None, *args, **kwargs) -> Dict[str, Any]:
        return await self._dispatch(("fetch_quote_and_enrichment_patch", "fetch_enriched_quote_patch", "fetch_quote_patch"), fetch_quote_and_enrichment_patch, symbol, base, *args, **kwargs)

    async def fetch_quote_and_fundamentals_patch(self, symbol: str, base: Optional[Dict[str, Any]] = None, *args, **kwargs) -> Dict[str, Any]:
        return await self._dispatch(("fetch_quote_and_fundamentals_patch", "fetch_enriched_quote_patch", "fetch_quote_patch"), fetch_quote_and_fundamentals_patch, symbol, base, *args, **kwargs)

    async def fetch_price_history(self, *args, **kwargs) -> List[Dict[str, Any]]:
        return await self._dispatch(("fetch_price_history", "fetch_history", "fetch_ohlc_history", "fetch_prices"), fetch_price_history, *args, **kwargs)

    async def fetch_history(self, *args, **kwargs) -> List[Dict[str, Any]]:
        return await self._dispatch(("fetch_history", "fetch_price_history", "fetch_ohlc_history", "fetch_prices"), fetch_history, *args, **kwargs)

    async def fetch_ohlc_history(self, *args, **kwargs) -> List[Dict[str, Any]]:
        return await self._dispatch(("fetch_ohlc_history", "fetch_history", "fetch_price_history", "fetch_prices"), fetch_ohlc_history, *args, **kwargs)

    async def fetch_history_patch(self, *args, **kwargs) -> List[Dict[str, Any]]:
        return await self._dispatch(("fetch_history_patch", "fetch_history", "fetch_price_history", "fetch_ohlc_history"), fetch_history_patch, *args, **kwargs)

    async def fetch_prices(self, *args, **kwargs) -> List[Dict[str, Any]]:
        return await self._dispatch(("fetch_prices", "fetch_history", "fetch_price_history", "fetch_ohlc_history"), fetch_prices, *args, **kwargs)


# =============================================================================
# Utilities
# =============================================================================


async def get_provider_status() -> Dict[str, Any]:
    info = await _PROVIDER.get()
    return {
        "shim_version": SHIM_VERSION,
        "provider_version": PROVIDER_VERSION,
        "data_source": DATA_SOURCE,
        "canonical_available": bool(info.available),
        "canonical_version": info.version if info.available else None,
        "canonical_origin_path": info.origin_path if info.available else None,
        "canonical_error": info.error if not info.available else None,
        "ttl_sec": _PROVIDER.ttl,
        "telemetry": _TELEMETRY.snapshot(),
        "timestamp_utc": _utc_iso(),
        "timestamp_riyadh": _riyadh_iso(),
    }


async def clear_cache() -> None:
    global _PROVIDER
    _PROVIDER = ProviderCache()
    shim_provider_available.set(0)


def get_version() -> str:
    return SHIM_VERSION


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


if __name__ == "__main__":
    async def _diag() -> None:
        sys.stdout.write("Yahoo shim diagnostics\n")
        sys.stdout.write("=" * 60 + "\n")
        status = await get_provider_status()
        sys.stdout.write(json_dumps(status, default=str) + "\n")
        sys.stdout.write("=" * 60 + "\n")
        res = await fetch_quote(symbol="AAPL")
        sys.stdout.write(json_dumps(res, default=str) + "\n")
        hist = await fetch_history(symbol="GC=F")
        sys.stdout.write(json_dumps({"history_rows": len(hist), "sample": hist[:2]}, default=str) + "\n")

    asyncio.run(_diag())
