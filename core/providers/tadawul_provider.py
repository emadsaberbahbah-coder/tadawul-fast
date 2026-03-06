#!/usr/bin/env python3
# core/providers/tadawul_provider.py
"""
================================================================================
Tadawul Provider (KSA Market Data) — v4.3.0 (KSA CLASSIFICATION FIX + SCHEMA KEYS)
================================================================================

Phase E — KSA completeness fix (your key constraint)
----------------------------------------------------
KSA Market_Leaders was missing: country / sector / industry + inconsistent identity fields.

This revision guarantees (even when Tadawul payloads are sparse):
- ✅ country is ALWAYS set for KSA: country="SAU"
- ✅ currency ALWAYS set: currency="SAR"
- ✅ exchange ALWAYS set: exchange="Tadawul"
- ✅ name best-effort from Tadawul payloads, else fallback to symbol
- ✅ sector/industry best-effort extraction from ALL Tadawul payload shapes (quote/fundamentals/profile)
- ✅ canonical schema keys emitted whenever possible:
    country, sector, industry, name, exchange, currency, asset_class,
    current_price, previous_close, open_price, day_high, day_low,
    week_52_high, week_52_low, volume, market_cap,
    price_change, percent_change, week_52_position_pct,
    rsi_14, volatility_30d,
    forecast_price_1m/3m/12m, expected_roi_1m/3m/12m, forecast_confidence,
    last_updated_utc, last_updated_riyadh

Notes
- This provider is KSA-only. EODHD is not used for KSA.
- Sector/industry may still be missing if Tadawul does not provide classification for a symbol;
  Argaam provider (next phase) should enrich missing classification/profile fields.

Exported (engine-compatible)
- fetch_enriched_quote_patch(symbol)
- fetch_quote_patch(symbol)
- fetch_fundamentals_patch(symbol)
- fetch_history_patch(symbol)

Adapter exports (router/health-friendly)
- provider, get_provider(), build_provider()

Startup safety
- No network I/O at import-time.
- Optional deps never crash startup.

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
import threading
import time
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from enum import Enum
from typing import Any, Dict, List, Optional, Sequence, Set, Tuple, Union

import httpx

# =============================================================================
# Optional numeric stack (NEVER crash if missing)
# =============================================================================
try:
    import numpy as _np  # type: ignore

    _HAS_NUMPY = True
except Exception:
    _np = None  # type: ignore
    _HAS_NUMPY = False

# ---------------------------------------------------------------------------
# High-Performance JSON fallback
# ---------------------------------------------------------------------------
try:
    import orjson  # type: ignore

    def json_loads(data: Union[str, bytes]) -> Any:
        return orjson.loads(data)

except Exception:

    def json_loads(data: Union[str, bytes]) -> Any:
        if isinstance(data, (bytes, bytearray)):
            data = data.decode("utf-8", errors="replace")
        return json.loads(data)

# ---------------------------------------------------------------------------
# Optional Scientific & ML Stack (safe)
# ---------------------------------------------------------------------------
try:
    from scipy import stats  # type: ignore

    SCIPY_AVAILABLE = True
except Exception:
    stats = None  # type: ignore
    SCIPY_AVAILABLE = False

try:
    from sklearn.ensemble import RandomForestRegressor  # type: ignore

    SKLEARN_AVAILABLE = True
except Exception:
    RandomForestRegressor = None  # type: ignore
    SKLEARN_AVAILABLE = False

try:
    import xgboost as xgb  # type: ignore

    XGB_AVAILABLE = True
except Exception:
    xgb = None  # type: ignore
    XGB_AVAILABLE = False

try:
    from statsmodels.tsa.arima.model import ARIMA as StatsARIMA  # type: ignore

    import warnings
    from statsmodels.tools.sm_exceptions import ConvergenceWarning  # type: ignore

    warnings.simplefilter("ignore", ConvergenceWarning)
    STATSMODELS_AVAILABLE = True
except Exception:
    StatsARIMA = None  # type: ignore
    STATSMODELS_AVAILABLE = False

# ---------------------------------------------------------------------------
# Tracing and Metrics Stack (safe)
# ---------------------------------------------------------------------------
try:
    from opentelemetry import trace  # type: ignore
    from opentelemetry.trace import Status, StatusCode  # type: ignore

    _OTEL_AVAILABLE = True
except Exception:
    trace = None  # type: ignore
    Status = None  # type: ignore
    StatusCode = None  # type: ignore
    _OTEL_AVAILABLE = False

try:
    from prometheus_client import Counter, Histogram, Gauge  # type: ignore

    _PROMETHEUS_AVAILABLE = True
except Exception:
    Counter = Histogram = Gauge = None  # type: ignore
    _PROMETHEUS_AVAILABLE = False

logger = logging.getLogger("core.providers.tadawul_provider")
logger.addHandler(logging.NullHandler())

PROVIDER_NAME = "tadawul"
PROVIDER_VERSION = "4.3.0"

USER_AGENT_DEFAULT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/124.0.0.0 Safari/537.36"
)

DEFAULT_TIMEOUT_SEC = 25.0
DEFAULT_RETRY_ATTEMPTS = 4
DEFAULT_MAX_CONCURRENCY = 40
DEFAULT_RATE_LIMIT = 15.0  # requests per second
DEFAULT_CIRCUIT_BREAKER_THRESHOLD = 5
DEFAULT_CIRCUIT_BREAKER_TIMEOUT = 30.0
DEFAULT_QUEUE_SIZE = 2000

_ARABIC_DIGITS = str.maketrans("٠١٢٣٤٥٦٧٨٩", "0123456789")
_TRUTHY = {"1", "true", "yes", "y", "on", "t", "enabled", "enable"}
_FALSY = {"0", "false", "no", "n", "off", "f", "disabled", "disable"}

_KSA_CODE_RE = re.compile(r"^\d{3,6}$", re.IGNORECASE)

_TRACING_ENABLED = os.getenv("TADAWUL_TRACING_ENABLED", "").strip().lower() in _TRUTHY
_METRICS_ENABLED = os.getenv("TADAWUL_METRICS_ENABLED", "").strip().lower() in _TRUTHY

# =============================================================================
# Phase E defaults (KSA identity)
# =============================================================================
KSA_COUNTRY_CODE = "SAU"
KSA_COUNTRY_NAME = "Saudi Arabia"
KSA_EXCHANGE_NAME = "Tadawul"
KSA_CURRENCY = "SAR"


# =============================================================================
# Enums & Data Classes
# =============================================================================
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


class RequestPriority(Enum):
    LOW = 0
    NORMAL = 1
    HIGH = 2
    CRITICAL = 3


@dataclass
class CircuitBreakerStats:
    failures: int = 0
    successes: int = 0
    state: CircuitState = CircuitState.CLOSED
    open_until: float = 0.0
    current_cooldown: float = DEFAULT_CIRCUIT_BREAKER_TIMEOUT
    total_failures: int = 0
    total_successes: int = 0


@dataclass
class RequestQueueItem:
    priority: RequestPriority
    url: str
    future: asyncio.Future
    created_at: float = field(default_factory=time.time)


# =============================================================================
# Shared Normalizer Integration (optional)
# =============================================================================
def _try_import_shared_ksa_helpers() -> Tuple[Optional[Any], Optional[Any]]:
    try:
        from core.symbols.normalize import normalize_ksa_symbol as _nksa  # type: ignore
        from core.symbols.normalize import looks_like_ksa as _lk  # type: ignore

        return _nksa, _lk
    except Exception:
        return None, None


_SHARED_NORM_KSA, _SHARED_LOOKS_KSA = _try_import_shared_ksa_helpers()


# =============================================================================
# Environment Helpers
# =============================================================================
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
    # At minimum quote URL must exist
    return bool(_safe_str(_env_str("TADAWUL_QUOTE_URL", "")))


def _emit_warnings() -> bool:
    return _env_bool("TADAWUL_VERBOSE_WARNINGS", False)


def _enable_fundamentals() -> bool:
    return _env_bool("TADAWUL_ENABLE_FUNDAMENTALS", True)


def _enable_history() -> bool:
    return _env_bool("TADAWUL_ENABLE_HISTORY", True)


def _enable_profile() -> bool:
    # New optional enrichment endpoint (safe): if not configured, no effect.
    return _env_bool("TADAWUL_ENABLE_PROFILE", True)


def _enable_forecast() -> bool:
    return _env_bool("TADAWUL_ENABLE_FORECAST", True)


def _enable_ml() -> bool:
    return _env_bool("TADAWUL_ENABLE_ML", True)


def _timeout_sec() -> float:
    return max(5.0, _env_float("TADAWUL_TIMEOUT_SEC", DEFAULT_TIMEOUT_SEC))


def _retry_attempts() -> int:
    return max(1, _env_int("TADAWUL_RETRY_ATTEMPTS", DEFAULT_RETRY_ATTEMPTS))


def _rate_limit() -> float:
    return _env_float("TADAWUL_RATE_LIMIT", DEFAULT_RATE_LIMIT)


def _circuit_breaker_threshold() -> int:
    return _env_int("TADAWUL_CB_THRESHOLD", DEFAULT_CIRCUIT_BREAKER_THRESHOLD)


def _circuit_breaker_timeout() -> float:
    return _env_float("TADAWUL_CB_TIMEOUT", DEFAULT_CIRCUIT_BREAKER_TIMEOUT)


def _max_concurrency() -> int:
    return max(2, _env_int("TADAWUL_MAX_CONCURRENCY", DEFAULT_MAX_CONCURRENCY))


def _queue_size() -> int:
    return _env_int("TADAWUL_QUEUE_SIZE", DEFAULT_QUEUE_SIZE)


def _quote_ttl_sec() -> float:
    return max(5.0, _env_float("TADAWUL_QUOTE_TTL_SEC", 15.0))


def _fund_ttl_sec() -> float:
    return max(300.0, _env_float("TADAWUL_FUNDAMENTALS_TTL_SEC", 21600.0))


def _hist_ttl_sec() -> float:
    return max(300.0, _env_float("TADAWUL_HISTORY_TTL_SEC", 1800.0))


def _profile_ttl_sec() -> float:
    return max(3600.0, _env_float("TADAWUL_PROFILE_TTL_SEC", 43200.0))  # 12h default


def _err_ttl_sec() -> float:
    return max(5.0, _env_float("TADAWUL_ERROR_TTL_SEC", 10.0))


def _history_days() -> int:
    return max(60, _env_int("TADAWUL_HISTORY_DAYS", 500))


def _history_points_max() -> int:
    return max(100, _env_int("TADAWUL_HISTORY_POINTS_MAX", 1000))


def _extra_headers() -> Dict[str, str]:
    raw = _env_str("TADAWUL_HEADERS_JSON", "")
    if not raw:
        return {}
    try:
        js = json.loads(raw)
        if isinstance(js, dict):
            return {
                str(k).strip(): str(v).strip()
                for k, v in js.items()
                if str(k).strip() and str(v).strip()
            }
    except Exception:
        pass
    return {}


# =============================================================================
# Time helpers (UTC + Riyadh)
# =============================================================================
_RIYADH_TZ = timezone(timedelta(hours=3))


def _utc_iso(dt: Optional[datetime] = None) -> str:
    d = dt or datetime.now(timezone.utc)
    if d.tzinfo is None:
        d = d.replace(tzinfo=timezone.utc)
    return d.astimezone(timezone.utc).isoformat()


def _riyadh_iso(dt: Optional[datetime] = None) -> str:
    d = dt or datetime.now(_RIYADH_TZ)
    if d.tzinfo is None:
        d = d.replace(tzinfo=_RIYADH_TZ)
    return d.astimezone(_RIYADH_TZ).isoformat()


def _to_riyadh_iso(dt: Optional[datetime]) -> Optional[str]:
    if not dt:
        return None
    d = dt
    if d.tzinfo is None:
        d = d.replace(tzinfo=timezone.utc)
    return d.astimezone(_RIYADH_TZ).isoformat()


# =============================================================================
# KSA Symbol Normalization (Strict)
# =============================================================================
def normalize_ksa_symbol(symbol: str) -> str:
    raw = (symbol or "").strip()
    if not raw:
        return ""
    raw = raw.translate(_ARABIC_DIGITS).strip()

    if callable(_SHARED_NORM_KSA):
        try:
            s = (_SHARED_NORM_KSA(raw) or "").strip().upper()
            if s.endswith(".SR"):
                code = s[:-3].strip()
                return f"{code}.SR" if _KSA_CODE_RE.match(code) else ""
            return ""
        except Exception:
            pass

    s = raw.strip().upper()
    if s.startswith("TADAWUL:"):
        s = s.split(":", 1)[1].strip()
    if s.endswith(".SR"):
        code = s[:-3].strip()
        return f"{code}.SR" if _KSA_CODE_RE.match(code) else ""
    if _KSA_CODE_RE.match(s):
        return f"{s}.SR"
    return ""


def format_url(tpl: str, symbol: str, *, days: Optional[int] = None) -> str:
    sym = normalize_ksa_symbol(symbol)
    if not sym:
        return tpl
    code = sym[:-3] if sym.endswith(".SR") else sym
    url = tpl.replace("{symbol}", sym).replace("{code}", code)
    if days is not None:
        url = url.replace("{days}", str(int(days)))
    return url


# =============================================================================
# Safe numeric helpers (no numpy required)
# =============================================================================
def _diff(arr: List[float]) -> List[float]:
    if len(arr) < 2:
        return []
    return [arr[i] - arr[i - 1] for i in range(1, len(arr))]


def _std(arr: List[float]) -> float:
    if len(arr) < 2:
        return 0.0
    mean = sum(arr) / len(arr)
    var = sum((x - mean) ** 2 for x in arr) / (len(arr) - 1)
    return math.sqrt(max(0.0, var))


def _mean(arr: List[float]) -> float:
    return sum(arr) / len(arr) if arr else 0.0


def _np_or_py_std(arr: List[float]) -> float:
    if _HAS_NUMPY and _np is not None and arr:
        try:
            return float(_np.std(_np.asarray(arr, dtype=float), ddof=1)) if len(arr) > 1 else 0.0
        except Exception:
            return _std(arr)
    return _std(arr)


def safe_float(val: Any) -> Optional[float]:
    if val is None:
        return None
    try:
        if isinstance(val, (int, float)) and not isinstance(val, bool):
            f = float(val)
            return f if not math.isnan(f) and not math.isinf(f) else None
        s = str(val).strip()
        if not s or s.lower() in {"-", "—", "n/a", "na", "null", "none", ""}:
            return None
        s = (
            s.translate(_ARABIC_DIGITS)
            .replace("٬", ",")
            .replace("٫", ".")
            .replace("−", "-")
        )
        s = s.replace("SAR", "").replace("ريال", "").strip()
        s = s.replace("%", "").replace(",", "").replace("+", "").strip()
        if s.startswith("(") and s.endswith(")"):
            s = "-" + s[1:-1].strip()
        m = re.match(r"^(-?\d+(\.\d+)?)([KMB])$", s, re.IGNORECASE)
        mult = 1.0
        if m:
            num = m.group(1)
            suf = m.group(3).upper()
            mult = 1_000.0 if suf == "K" else 1_000_000.0 if suf == "M" else 1_000_000_000.0
            s = num
        f = float(s) * mult
        return f if not math.isnan(f) and not math.isinf(f) else None
    except Exception:
        return None


def safe_int(val: Any) -> Optional[int]:
    f = safe_float(val)
    return int(round(f)) if f is not None else None


def safe_str(val: Any) -> Optional[str]:
    return str(val).strip() if val is not None and str(val).strip() else None


def unwrap_common_envelopes(js: Union[dict, list]) -> Union[dict, list]:
    current = js
    for _ in range(4):
        if isinstance(current, dict):
            for key in ("data", "result", "payload", "quote", "profile", "response", "content"):
                if key in current and isinstance(current[key], (dict, list)):
                    current = current[key]
                    break
            else:
                break
        else:
            break
    return current


def coerce_dict(data: Union[dict, list]) -> dict:
    if isinstance(data, dict):
        return data
    if isinstance(data, list) and data and isinstance(data[0], dict):
        return data[0]
    return {}


def find_first_value(
    obj: Any,
    keys: Sequence[str],
    max_depth: int = 8,
    max_nodes: int = 5000,
) -> Any:
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
    # If it's likely fraction (<= 1), convert to percent-points
    return v * 100.0 if abs(v) <= 1.0 else v


def clean_patch(p: Dict[str, Any]) -> Dict[str, Any]:
    return {
        k: v
        for k, v in (p or {}).items()
        if v is not None and not (isinstance(v, str) and not v.strip())
    }


def merge_into(dst: Dict[str, Any], src: Dict[str, Any], *, force_keys: Sequence[str] = ()) -> None:
    fset = set(force_keys or ())
    for k, v in (src or {}).items():
        if v is None:
            continue
        if (
            k in fset
            or k not in dst
            or dst.get(k) is None
            or (isinstance(dst.get(k), str) and not str(dst.get(k)).strip())
        ):
            dst[k] = v


def _ensure_ksa_identity(patch: Dict[str, Any], *, symbol: str) -> None:
    # Phase E guarantees
    patch["country"] = KSA_COUNTRY_CODE
    patch["currency"] = patch.get("currency") or KSA_CURRENCY
    patch["exchange"] = patch.get("exchange") or KSA_EXCHANGE_NAME
    patch["asset_class"] = patch.get("asset_class") or "EQUITY"
    if not patch.get("name"):
        # fallback (safe)
        patch["name"] = symbol


def fill_derived_quote_fields(patch: Dict[str, Any]) -> None:
    cur = safe_float(patch.get("current_price"))
    prev = safe_float(patch.get("previous_close"))
    vol = safe_float(patch.get("volume"))
    w52h = safe_float(patch.get("week_52_high"))
    w52l = safe_float(patch.get("week_52_low"))

    if patch.get("price_change") is None and cur is not None and prev is not None:
        patch["price_change"] = cur - prev

    if patch.get("percent_change") is None and cur is not None and prev not in (None, 0.0):
        try:
            patch["percent_change"] = (cur - prev) / prev * 100.0
        except Exception:
            pass

    if (
        patch.get("week_52_position_pct") is None
        and cur is not None
        and w52h is not None
        and w52l is not None
        and w52h != w52l
    ):
        patch["week_52_position_pct"] = ((cur - w52l) / (w52h - w52l)) * 100.0

    # legacy aliases (harmless; engine may ignore)
    if patch.get("price") is None and patch.get("current_price") is not None:
        patch["price"] = patch["current_price"]
    if patch.get("prev_close") is None and patch.get("previous_close") is not None:
        patch["prev_close"] = patch["previous_close"]
    if patch.get("change") is None and patch.get("price_change") is not None:
        patch["change"] = patch["price_change"]
    if patch.get("change_pct") is None and patch.get("percent_change") is not None:
        patch["change_pct"] = patch["percent_change"]


def data_quality_score(patch: Dict[str, Any]) -> Tuple[DataQuality, float]:
    score = 100.0
    if safe_float(patch.get("current_price")) is None:
        score -= 30
    if safe_float(patch.get("previous_close")) is None:
        score -= 12
    if safe_float(patch.get("volume")) is None:
        score -= 8
    if safe_str(patch.get("name")) is None:
        score -= 10
    if safe_str(patch.get("sector")) is None:
        score -= 8
    if safe_str(patch.get("industry")) is None:
        score -= 8
    if safe_str(patch.get("country")) is None:
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


# =============================================================================
# Tracing (safe)
# =============================================================================
class TraceContext:
    def __init__(self, name: str, attributes: Optional[Dict[str, Any]] = None):
        self.name = name
        self.attributes = attributes or {}
        self.tracer = trace.get_tracer(__name__) if _OTEL_AVAILABLE and _TRACING_ENABLED else None  # type: ignore
        self._span_cm = None
        self.span = None

    async def __aenter__(self):
        # Prefer proper CM start_as_current_span if available
        if self.tracer:
            try:
                start_as_current = getattr(self.tracer, "start_as_current_span", None)
                if callable(start_as_current):
                    self._span_cm = start_as_current(self.name)
                    self.span = self._span_cm.__enter__()
                else:
                    self.span = self.tracer.start_span(self.name)  # type: ignore
                if self.span and self.attributes:
                    for k, v in self.attributes.items():
                        try:
                            self.span.set_attribute(str(k), v)  # type: ignore
                        except Exception:
                            pass
            except Exception:
                self._span_cm = None
                self.span = None
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self.span and _OTEL_AVAILABLE:
            try:
                if exc_val and Status and StatusCode:
                    if hasattr(self.span, "record_exception"):
                        self.span.record_exception(exc_val)  # type: ignore
                    if hasattr(self.span, "set_status"):
                        self.span.set_status(Status(StatusCode.ERROR, str(exc_val)))  # type: ignore
            except Exception:
                pass
        if self._span_cm:
            try:
                self._span_cm.__exit__(exc_type, exc_val, exc_tb)
            except Exception:
                pass
        else:
            if self.span:
                try:
                    self.span.end()  # type: ignore
                except Exception:
                    pass


def _trace(name: Optional[str] = None):
    def decorator(func):
        @functools.wraps(func)
        async def wrapper(*args, **kwargs):
            trace_name = name or func.__name__
            async with TraceContext(trace_name, {"function": func.__name__}):
                return await func(*args, **kwargs)

        return wrapper

    return decorator


# =============================================================================
# Metrics (optional)
# =============================================================================
class MetricsRegistry:
    def __init__(self, namespace: str = "tadawul"):
        self.namespace = namespace
        self._metrics: Dict[str, Any] = {}
        self._lock = threading.RLock()
        if _PROMETHEUS_AVAILABLE and _METRICS_ENABLED and Counter and Histogram and Gauge:
            self._init_metrics()

    def _init_metrics(self):
        with self._lock:
            self._metrics["requests_total"] = Counter(
                f"{self.namespace}_requests_total", "Total requests", ["status"]
            )
            self._metrics["queue_size"] = Gauge(
                f"{self.namespace}_queue_size", "Queue size"
            )
            self._metrics["rate_limiter_tokens"] = Gauge(
                f"{self.namespace}_rate_limiter_tokens", "Tokens"
            )
            self._metrics["circuit_breaker_state"] = Gauge(
                f"{self.namespace}_circuit_breaker_state", "State (1=closed, 0=open, -1=half)"
            )

    def inc(self, name: str, value: float = 1.0, labels: Optional[Dict[str, str]] = None):
        if not (_PROMETHEUS_AVAILABLE and _METRICS_ENABLED):
            return
        with self._lock:
            metric = self._metrics.get(name)
            if not metric:
                return
            if labels:
                metric.labels(**labels).inc(value)
            else:
                metric.inc(value)

    def set(self, name: str, value: float, labels: Optional[Dict[str, str]] = None):
        if not (_PROMETHEUS_AVAILABLE and _METRICS_ENABLED):
            return
        with self._lock:
            metric = self._metrics.get(name)
            if not metric:
                return
            if labels:
                metric.labels(**labels).set(value)
            else:
                metric.set(value)


_METRICS = MetricsRegistry()


# =============================================================================
# Core tools (Cache, Limiter, Circuit Breaker, Queue, SingleFlight)
# =============================================================================
class SmartCache:
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
                self._cache.pop(key, None)
                self._expires.pop(key, None)
                self._access_times.pop(key, None)
            return None

    async def set(self, key: str, value: Any, ttl: Optional[float] = None) -> None:
        async with self._lock:
            if len(self._cache) >= self.maxsize and key not in self._cache:
                await self._evict_lru()
            self._cache[key] = value
            self._expires[key] = time.monotonic() + (ttl or self.ttl)
            self._access_times[key] = time.monotonic()

    async def _evict_lru(self) -> None:
        if not self._access_times:
            return
        oldest_key = min(self._access_times.items(), key=lambda x: x[1])[0]
        self._cache.pop(oldest_key, None)
        self._expires.pop(oldest_key, None)
        self._access_times.pop(oldest_key, None)

    async def size(self) -> int:
        async with self._lock:
            return len(self._cache)


class TokenBucket:
    def __init__(self, rate_per_sec: float, capacity: Optional[float] = None):
        self.rate = max(0.0, float(rate_per_sec))
        self.capacity = float(capacity) if capacity is not None else max(1.0, self.rate)
        self.tokens = self.capacity
        self.last = time.monotonic()
        self._lock = asyncio.Lock()

    async def acquire(self, tokens: float = 1.0) -> bool:
        if self.rate <= 0:
            return True
        async with self._lock:
            now = time.monotonic()
            self.tokens = min(self.capacity, self.tokens + max(0.0, now - self.last) * self.rate)
            self.last = now
            if self.tokens >= tokens:
                self.tokens -= tokens
                _METRICS.set("rate_limiter_tokens", self.tokens)
                return True
            return False

    async def wait_and_acquire(self, tokens: float = 1.0) -> None:
        while True:
            if await self.acquire(tokens):
                return
            async with self._lock:
                wait = (tokens - self.tokens) / self.rate if self.rate > 0 else 0.1
            await asyncio.sleep(min(1.0, max(0.05, wait)))


class DynamicCircuitBreaker:
    def __init__(self, fail_threshold: int = 5, base_cooldown: float = 30.0):
        self.fail_threshold = int(fail_threshold)
        self.base_cooldown = float(base_cooldown)
        self.stats = CircuitBreakerStats(current_cooldown=self.base_cooldown)
        self.half_open_calls = 0
        self._lock = asyncio.Lock()

    async def allow_request(self) -> bool:
        async with self._lock:
            now = time.monotonic()
            if self.stats.state == CircuitState.CLOSED:
                _METRICS.set("circuit_breaker_state", 1)
                return True
            if self.stats.state == CircuitState.OPEN:
                if now >= self.stats.open_until:
                    self.stats.state = CircuitState.HALF_OPEN
                    self.half_open_calls = 0
                    _METRICS.set("circuit_breaker_state", -1)
                    return True
                _METRICS.set("circuit_breaker_state", 0)
                return False
            # HALF_OPEN
            if self.half_open_calls < 2:
                self.half_open_calls += 1
                _METRICS.set("circuit_breaker_state", -1)
                return True
            return False

    async def on_success(self) -> None:
        async with self._lock:
            self.stats.successes += 1
            self.stats.total_successes += 1
            if self.stats.state == CircuitState.HALF_OPEN:
                self.stats.state = CircuitState.CLOSED
                self.stats.failures = 0
                self.stats.current_cooldown = self.base_cooldown
                _METRICS.set("circuit_breaker_state", 1)
            else:
                self.stats.failures = max(0, self.stats.failures - 1)

    async def on_failure(self, status_code: int = 500) -> None:
        async with self._lock:
            self.stats.failures += 1
            self.stats.total_failures += 1

            if status_code in (401, 403, 429):
                self.stats.current_cooldown = min(300.0, self.stats.current_cooldown * 1.5)

            if self.stats.state == CircuitState.CLOSED and self.stats.failures >= self.fail_threshold:
                self.stats.state = CircuitState.OPEN
                self.stats.open_until = time.monotonic() + self.stats.current_cooldown
                _METRICS.set("circuit_breaker_state", 0)
            elif self.stats.state == CircuitState.HALF_OPEN:
                self.stats.state = CircuitState.OPEN
                self.stats.current_cooldown = min(300.0, self.stats.current_cooldown * 2.0)
                self.stats.open_until = time.monotonic() + self.stats.current_cooldown
                _METRICS.set("circuit_breaker_state", 0)

    def get_stats(self) -> Dict[str, Any]:
        return {
            "state": self.stats.state.value,
            "failures": self.stats.failures,
            "successes": self.stats.successes,
            "open_until": self.stats.open_until,
            "current_cooldown": self.stats.current_cooldown,
            "total_failures": self.stats.total_failures,
            "total_successes": self.stats.total_successes,
        }


class RequestQueue:
    def __init__(self, max_size: int = 1000):
        self.max_size = int(max_size)
        self.queues: Dict[RequestPriority, asyncio.Queue] = {
            RequestPriority.LOW: asyncio.Queue(),
            RequestPriority.NORMAL: asyncio.Queue(),
            RequestPriority.HIGH: asyncio.Queue(),
            RequestPriority.CRITICAL: asyncio.Queue(),
        }
        self._lock = asyncio.Lock()

    async def put(self, item: RequestQueueItem) -> bool:
        async with self._lock:
            total_size = sum(q.qsize() for q in self.queues.values())
            if total_size >= self.max_size:
                return False
            await self.queues[item.priority].put(item)
            _METRICS.set("queue_size", total_size + 1)
            return True

    async def get(self) -> Optional[RequestQueueItem]:
        for priority in sorted(RequestPriority, key=lambda p: p.value, reverse=True):
            q = self.queues[priority]
            if not q.empty():
                item = await q.get()
                _METRICS.set("queue_size", sum(qq.qsize() for qq in self.queues.values()))
                return item
        return None


class SingleFlight:
    def __init__(self) -> None:
        self._lock = asyncio.Lock()
        self._futures: Dict[str, asyncio.Future] = {}

    async def run(self, key: str, coro_fn):
        async with self._lock:
            fut = self._futures.get(key)
            if fut is not None:
                return await fut
            loop = asyncio.get_running_loop()
            fut = loop.create_future()
            self._futures[key] = fut

        try:
            result = await coro_fn()
            if not fut.cancelled() and not fut.done():
                fut.set_result(result)
            return result
        except Exception as e:
            if not fut.cancelled() and not fut.done():
                fut.set_exception(e)
            raise
        finally:
            async with self._lock:
                self._futures.pop(key, None)


# =============================================================================
# Technical Indicators (minimal)
# =============================================================================
def _rsi_14(closes: List[float]) -> Optional[float]:
    if len(closes) < 15:
        return None
    try:
        diffs = [closes[i] - closes[i - 1] for i in range(1, len(closes))]
        gains = [d if d > 0 else 0.0 for d in diffs]
        losses = [-d if d < 0 else 0.0 for d in diffs]
        n = 14
        avg_gain = sum(gains[:n]) / n
        avg_loss = sum(losses[:n]) / n
        for i in range(n, len(gains)):
            avg_gain = (avg_gain * (n - 1) + gains[i]) / n
            avg_loss = (avg_loss * (n - 1) + losses[i]) / n
        if avg_loss == 0:
            return 100.0
        rs = avg_gain / avg_loss
        return float(100.0 - (100.0 / (1.0 + rs)))
    except Exception:
        return None


def _volatility_30d(closes: List[float]) -> Optional[float]:
    if len(closes) < 31:
        return None
    try:
        window = closes[-31:]
        rets: List[float] = []
        for i in range(1, len(window)):
            if window[i - 1] > 0 and window[i] > 0:
                rets.append(math.log(window[i] / window[i - 1]))
        if len(rets) < 2:
            return None
        return float(_std(rets) * math.sqrt(252) * 100.0)  # percent points
    except Exception:
        return None


# =============================================================================
# History Analytics (light + schema aligned)
# =============================================================================
def compute_history_analytics(closes: List[float]) -> Dict[str, Any]:
    if not closes or len(closes) < 20:
        return {}
    out: Dict[str, Any] = {}
    rsi = _rsi_14(closes)
    if rsi is not None:
        out["rsi_14"] = float(rsi)
    vol30 = _volatility_30d(closes)
    if vol30 is not None:
        out["volatility_30d"] = float(vol30)

    # simple forecasts (best-effort): do not overfit; keep stable
    if _enable_forecast() and len(closes) >= 90:
        last = float(closes[-1])
        def _forecast(h: int) -> Tuple[Optional[float], Optional[float]]:
            if len(closes) < 60 or last <= 0:
                return None, None
            # drift based on last 60 trading days
            w = closes[-61:]
            rets = []
            for i in range(1, len(w)):
                if w[i - 1] > 0:
                    rets.append((w[i] / w[i - 1]) - 1.0)
            mu = _mean(rets) if rets else 0.0
            price = last * ((1.0 + mu) ** h)
            roi = (price / last - 1.0) * 100.0
            return float(price), float(roi)

        p1, r1 = _forecast(21)
        p3, r3 = _forecast(63)
        p12, r12 = _forecast(252)

        if p1 is not None:
            out["forecast_price_1m"] = p1
            out["expected_roi_1m"] = r1
        if p3 is not None:
            out["forecast_price_3m"] = p3
            out["expected_roi_3m"] = r3
        if p12 is not None:
            out["forecast_price_12m"] = p12
            out["expected_roi_12m"] = r12

        # confidence 0..1 (simple function of vol)
        vol = vol30 / 100.0 if vol30 is not None else 0.35
        conf = max(0.20, min(0.85, 0.70 * (1.0 / (1.0 + max(0.0, vol - 0.30)))))
        out["forecast_confidence"] = float(conf)

    return out


# =============================================================================
# Tadawul Client
# =============================================================================
class TadawulClient:
    """Async Tadawul API client with queue + singleflight + cache + CB + rate limiter."""

    def __init__(self) -> None:
        self.timeout_sec = _timeout_sec()
        self.retry_attempts = _retry_attempts()

        self.quote_url = _safe_str(_env_str("TADAWUL_QUOTE_URL", ""))
        self.fundamentals_url = _safe_str(_env_str("TADAWUL_FUNDAMENTALS_URL", ""))
        self.history_url = _safe_str(_env_str("TADAWUL_HISTORY_URL", "")) or _safe_str(_env_str("TADAWUL_CANDLES_URL", ""))
        self.profile_url = _safe_str(_env_str("TADAWUL_PROFILE_URL", ""))

        headers = {
            "User-Agent": _env_str("TADAWUL_USER_AGENT", USER_AGENT_DEFAULT),
            "Accept": "application/json",
            "Accept-Language": "ar,en;q=0.9",
        }
        headers.update(_extra_headers())

        self._client = httpx.AsyncClient(
            timeout=httpx.Timeout(self.timeout_sec),
            follow_redirects=True,
            headers=headers,
            limits=httpx.Limits(max_keepalive_connections=25, max_connections=60),
            http2=True,
        )

        self.quote_cache = SmartCache(maxsize=7000, ttl=_quote_ttl_sec())
        self.fund_cache = SmartCache(maxsize=4000, ttl=_fund_ttl_sec())
        self.history_cache = SmartCache(maxsize=2500, ttl=_hist_ttl_sec())
        self.profile_cache = SmartCache(maxsize=4000, ttl=_profile_ttl_sec())
        self.error_cache = SmartCache(maxsize=4000, ttl=_err_ttl_sec())

        self.semaphore = asyncio.Semaphore(_max_concurrency())
        self.rate_limiter = TokenBucket(_rate_limit())
        self.circuit_breaker = DynamicCircuitBreaker(
            fail_threshold=_circuit_breaker_threshold(),
            base_cooldown=_circuit_breaker_timeout(),
        )

        self.request_queue = RequestQueue(max_size=_queue_size())
        self.singleflight = SingleFlight()

        self._queue_task: Optional[asyncio.Task] = None
        self._closing = False

        logger.info(
            "TadawulClient v%s initialized | configured=%s | rate=%s/s | cb=%s/%ss | has_profile=%s",
            PROVIDER_VERSION,
            _configured(),
            _rate_limit(),
            _circuit_breaker_threshold(),
            _circuit_breaker_timeout(),
            bool(self.profile_url),
        )

    async def _ensure_queue_task(self) -> None:
        if self._queue_task and not self._queue_task.done():
            return
        loop = asyncio.get_running_loop()
        self._queue_task = loop.create_task(self._process_queue())

    @_trace("tadawul_request")
    async def _request(
        self,
        url: str,
        priority: RequestPriority = RequestPriority.NORMAL,
    ) -> Tuple[Optional[Union[dict, list]], Optional[str]]:
        _METRICS.inc("requests_total", 1, {"status": "started"})

        if not await self.circuit_breaker.allow_request():
            return None, "circuit_breaker_open"

        await self._ensure_queue_task()

        loop = asyncio.get_running_loop()
        future = loop.create_future()
        item = RequestQueueItem(priority=priority, url=url, future=future)

        if not await self.request_queue.put(item):
            return None, "queue_full"

        try:
            return await future
        except asyncio.CancelledError:
            return None, "cancelled"

    async def _process_queue(self) -> None:
        try:
            while not self._closing:
                item = await self.request_queue.get()
                if item is None:
                    await asyncio.sleep(0.05)
                    continue
                asyncio.create_task(self._process_queue_item(item))
        except asyncio.CancelledError:
            return
        except Exception as e:
            logger.exception("Queue processor crashed: %s", e)

    async def _process_queue_item(self, item: RequestQueueItem) -> None:
        if item.future.cancelled():
            return
        try:
            result = await self._execute_request(item.url)
            if not item.future.cancelled() and not item.future.done():
                item.future.set_result(result)
        except Exception as e:
            if not item.future.cancelled() and not item.future.done():
                item.future.set_exception(e)

    async def _execute_request(self, url: str) -> Tuple[Optional[Union[dict, list]], Optional[str]]:
        await self.rate_limiter.wait_and_acquire()
        last_err = None

        async with self.semaphore:
            for attempt in range(self.retry_attempts):
                try:
                    resp = await self._client.get(url)
                    status = resp.status_code

                    if status == 429:
                        await self.circuit_breaker.on_failure(status)
                        await asyncio.sleep(min(30, int(resp.headers.get("Retry-After", 5))))
                        continue

                    if 500 <= status < 600:
                        await self.circuit_breaker.on_failure(status)
                        base_wait = 2 ** attempt
                        await asyncio.sleep(min(10.0, base_wait + random.uniform(0, base_wait)))
                        continue

                    if status >= 400 and status != 404:
                        await self.circuit_breaker.on_failure(status)
                        return None, f"HTTP {status}"

                    try:
                        data = json_loads(resp.content)
                        data = unwrap_common_envelopes(data)
                    except Exception:
                        await self.circuit_breaker.on_failure()
                        return None, "invalid_json"

                    if not isinstance(data, (dict, list)):
                        await self.circuit_breaker.on_failure()
                        return None, "unexpected_response_type"

                    await self.circuit_breaker.on_success()
                    _METRICS.inc("requests_total", 1, {"status": "success"})
                    return data, None

                except httpx.RequestError as e:
                    last_err = f"network_error_{e.__class__.__name__}"
                    base_wait = 2 ** attempt
                    await asyncio.sleep(min(10.0, base_wait + random.uniform(0, base_wait)))

        await self.circuit_breaker.on_failure()
        _METRICS.inc("requests_total", 1, {"status": "error"})
        return None, last_err or "max_retries_exceeded"

    # -------------------------------------------------------------------------
    # Mapping helpers (Phase E: classification + identity)
    # -------------------------------------------------------------------------
    def _map_identity_from_any(self, root: Any) -> Dict[str, Any]:
        """
        Extracts name/sector/industry/exchange/currency from any Tadawul payload shape.
        Best-effort: if missing, caller will fill defaults.
        """
        patch: Dict[str, Any] = {}
        patch["name"] = pick_str(
            root,
            "name",
            "company_name",
            "companyName",
            "securityName",
            "securityNameEn",
            "securityNameAr",
            "instrumentName",
            "symbolName",
            "shortName",
            "longName",
        )
        patch["sector"] = pick_str(
            root,
            "sector",
            "sectorName",
            "sector_name",
            "sectorEn",
            "sectorAr",
            "gicsSector",
            "sector_description",
        )
        patch["industry"] = pick_str(
            root,
            "industry",
            "industryName",
            "industry_name",
            "industryEn",
            "industryAr",
            "gicsIndustry",
            "industry_description",
            "subSectorName",
            "sub_sector",
            "subSector",
        )

        # Some APIs return exchange fields
        patch["exchange"] = pick_str(root, "exchange", "exchangeName", "market", "marketName", "mic")
        patch["currency"] = pick_str(root, "currency", "ccy", "tradingCurrency")

        return clean_patch(patch)

    def _map_quote(self, root: Any) -> Dict[str, Any]:
        patch: Dict[str, Any] = {}

        # Identity/classification (in case quote provides it)
        merge_into(patch, self._map_identity_from_any(root))

        # Canonical price fields
        patch["current_price"] = pick_num(root, "last", "last_price", "price", "close", "c", "tradingPrice", "regularMarketPrice")
        patch["previous_close"] = pick_num(root, "previous_close", "prev_close", "pc", "PreviousClose", "prevClose", "regularMarketPreviousClose")
        patch["open_price"] = pick_num(root, "open", "open_price", "regularMarketOpen", "o", "Open", "openPrice")
        patch["day_high"] = pick_num(root, "high", "day_high", "h", "High", "dayHigh", "sessionHigh")
        patch["day_low"] = pick_num(root, "low", "day_low", "l", "Low", "dayLow", "sessionLow")
        patch["volume"] = pick_num(root, "volume", "v", "Volume", "tradedVolume", "qty", "quantity")
        patch["market_cap"] = pick_num(root, "market_cap", "marketCap", "marketCapitalization", "MarketCap")
        patch["week_52_high"] = pick_num(root, "week_52_high", "fiftyTwoWeekHigh", "52w_high", "yearHigh", "week52High")
        patch["week_52_low"] = pick_num(root, "week_52_low", "fiftyTwoWeekLow", "52w_low", "yearLow", "week52Low")
        patch["price_change"] = pick_num(root, "change", "d", "price_change", "Change", "diff", "delta")
        patch["percent_change"] = pick_pct(root, "change_pct", "change_percent", "dp", "percent_change", "pctChange", "changePercent")

        fill_derived_quote_fields(patch)
        return clean_patch(patch)

    def _map_fundamentals(self, root: Any) -> Dict[str, Any]:
        patch: Dict[str, Any] = {}

        # Identity/classification (fundamentals often better for sector/industry)
        merge_into(patch, self._map_identity_from_any(root), force_keys=("sector", "industry", "name"))

        patch["market_cap"] = pick_num(root, "market_cap", "marketCap", "marketCapitalization", "MarketCap")
        patch["float_shares"] = pick_num(root, "floatShares", "float_shares", "freeFloatShares")
        patch["shares_outstanding"] = pick_num(root, "shares", "shares_outstanding", "shareOutstanding", "SharesOutstanding")

        patch["pe_ttm"] = pick_num(root, "pe", "pe_ttm", "trailingPE", "PE", "priceEarnings")
        patch["pb_ratio"] = pick_num(root, "pb", "pb_ratio", "priceToBook", "PBR", "price_book")
        patch["eps_ttm"] = pick_num(root, "eps", "eps_ttm", "trailingEps", "EPS")
        patch["dividend_yield"] = pick_pct(root, "dividend_yield", "divYield", "yield", "DividendYield")

        return clean_patch(patch)

    def _map_profile(self, root: Any) -> Dict[str, Any]:
        """
        Optional profile endpoint mapping (if configured).
        Intended mainly for sector/industry/name (Phase E).
        """
        patch = self._map_identity_from_any(root)
        # Some profile endpoints contain standardized fields
        if "sector" not in patch:
            patch["sector"] = pick_str(root, "sectorClassification", "classificationSector", "sectorDesc")
        if "industry" not in patch:
            patch["industry"] = pick_str(root, "industryClassification", "classificationIndustry", "industryDesc")
        return clean_patch(patch)

    def _parse_candles(self, js: Any) -> Tuple[List[float], Optional[datetime]]:
        closes: List[float] = []
        last_dt: Optional[datetime] = None

        if isinstance(js, dict):
            c_raw = js.get("c") or js.get("close")
            t_raw = js.get("t") or js.get("time") or js.get("timestamp")

            if isinstance(c_raw, list):
                closes = [f for f in (safe_float(x) for x in c_raw) if f is not None]

            if isinstance(t_raw, list) and t_raw:
                try:
                    last_dt = datetime.fromtimestamp(float(t_raw[-1]), tz=timezone.utc)
                except Exception:
                    last_dt = None

            if not closes:
                candles = js.get("candles") or js.get("data") or js.get("items") or js.get("prices") or js.get("history")
                if isinstance(candles, list) and candles and isinstance(candles[0], dict):
                    for item in candles:
                        c = safe_float(item.get("c") or item.get("close"))
                        if c is not None:
                            closes.append(c)
                        t = item.get("t") or item.get("time") or item.get("date")
                        if t is not None:
                            try:
                                if isinstance(t, (int, float)):
                                    last_dt = datetime.fromtimestamp(float(t), tz=timezone.utc)
                                elif isinstance(t, str):
                                    last_dt = datetime.fromisoformat(t.replace("Z", "+00:00"))
                            except Exception:
                                pass

        maxp = _history_points_max()
        return closes[-maxp:], last_dt

    # -------------------------------------------------------------------------
    # Public fetchers
    # -------------------------------------------------------------------------
    async def fetch_quote_patch(self, symbol: str) -> Dict[str, Any]:
        if not _configured():
            return {}
        sym = normalize_ksa_symbol(symbol)
        if not sym or not self.quote_url:
            return {} if not _emit_warnings() else {"_warn": "invalid_symbol_or_url"}

        cache_key = f"quote:{sym}"
        if await self.error_cache.get(cache_key):
            return {}

        async def _fetch():
            js, err = await self._request(format_url(self.quote_url or "", sym), priority=RequestPriority.HIGH)
            if js is None:
                await self.error_cache.set(cache_key, True)
                return {} if not _emit_warnings() else {"_warn": f"quote_failed: {err}"}

            mapped = self._map_quote(coerce_dict(js))
            if safe_float(mapped.get("current_price")) is None:
                await self.error_cache.set(cache_key, True)
                return {}

            # Phase E: always inject KSA identity defaults
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
            await self.quote_cache.set(cache_key, result)
            return result

        cached = await self.quote_cache.get(cache_key)
        return dict(cached) if cached else await self.singleflight.run(cache_key, _fetch)

    async def fetch_fundamentals_patch(self, symbol: str) -> Dict[str, Any]:
        if not (_configured() and _enable_fundamentals()):
            return {}
        sym = normalize_ksa_symbol(symbol)
        if not sym or not self.fundamentals_url:
            return {}

        cache_key = f"fund:{sym}"
        if await self.error_cache.get(cache_key):
            return {}

        async def _fetch():
            js, err = await self._request(format_url(self.fundamentals_url or "", sym), priority=RequestPriority.NORMAL)
            if js is None:
                await self.error_cache.set(cache_key, True)
                return {}
            mapped = self._map_fundamentals(coerce_dict(js))
            if not mapped:
                await self.error_cache.set(cache_key, True)
                return {}

            _ensure_ksa_identity(mapped, symbol=sym)

            result = {
                "provider": PROVIDER_NAME,
                "data_source": PROVIDER_NAME,
                "provider_version": PROVIDER_VERSION,
                **mapped,
            }
            await self.fund_cache.set(cache_key, result)
            return result

        cached = await self.fund_cache.get(cache_key)
        return dict(cached) if cached else await self.singleflight.run(cache_key, _fetch)

    async def fetch_profile_patch(self, symbol: str) -> Dict[str, Any]:
        """
        Optional: profile endpoint mainly to improve name/sector/industry (Phase E).
        If TADAWUL_PROFILE_URL is not set, this returns {}.
        """
        if not (_configured() and _enable_profile()):
            return {}
        sym = normalize_ksa_symbol(symbol)
        if not sym or not self.profile_url:
            return {}

        cache_key = f"profile:{sym}"
        cached = await self.profile_cache.get(cache_key)
        if cached is not None:
            return dict(cached) if isinstance(cached, dict) else {}

        async def _fetch():
            js, err = await self._request(format_url(self.profile_url or "", sym), priority=RequestPriority.NORMAL)
            if js is None:
                return {}
            mapped = self._map_profile(coerce_dict(js))
            if not mapped:
                return {}
            _ensure_ksa_identity(mapped, symbol=sym)
            out = {
                "provider": PROVIDER_NAME,
                "data_source": PROVIDER_NAME,
                "provider_version": PROVIDER_VERSION,
                **mapped,
            }
            await self.profile_cache.set(cache_key, out)
            return out

        return await self.singleflight.run(cache_key, _fetch)

    async def fetch_history_patch(self, symbol: str) -> Dict[str, Any]:
        if not (_configured() and _enable_history()):
            return {}
        sym = normalize_ksa_symbol(symbol)
        if not sym or not self.history_url:
            return {}

        days = _history_days()
        cache_key = f"history:{sym}:{days}"
        if await self.error_cache.get(cache_key):
            return {}

        async def _fetch():
            js, err = await self._request(format_url(self.history_url or "", sym, days=days), priority=RequestPriority.LOW)
            if js is None:
                await self.error_cache.set(cache_key, True)
                return {}
            closes, last_dt = self._parse_candles(js)
            if not closes or len(closes) < 20:
                await self.error_cache.set(cache_key, True)
                return {}

            analytics = compute_history_analytics(closes)
            out = {
                "requested_symbol": symbol,
                "normalized_symbol": sym,
                "history_points": len(closes),
                "history_last_utc": _utc_iso(last_dt),
                "history_last_riyadh": _to_riyadh_iso(last_dt),
                "provider": PROVIDER_NAME,
                "data_source": PROVIDER_NAME,
                "provider_version": PROVIDER_VERSION,
                **analytics,
            }
            await self.history_cache.set(cache_key, out)
            return out

        cached = await self.history_cache.get(cache_key)
        return dict(cached) if cached else await self.singleflight.run(cache_key, _fetch)

    async def fetch_enriched_quote_patch(self, symbol: str) -> Dict[str, Any]:
        """
        Enriched KSA quote: quote + fundamentals + history + optional profile
        and Phase E identity defaults (country/currency/exchange always).
        """
        if not _configured():
            return {}
        sym = normalize_ksa_symbol(symbol)
        if not sym:
            return {} if not _emit_warnings() else {"_warn": "invalid_ksa_symbol"}

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

        result: Dict[str, Any] = dict(quote_data) if quote_data else {
            "requested_symbol": symbol,
            "symbol": sym,
            "symbol_normalized": sym,
            "provider": PROVIDER_NAME,
            "data_source": PROVIDER_NAME,
            "provider_version": PROVIDER_VERSION,
            "last_updated_utc": _utc_iso(),
            "last_updated_riyadh": _riyadh_iso(),
        }

        # Merge order: profile (classification) -> fundamentals -> history -> quote already in base
        if prof_data:
            merge_into(result, prof_data, force_keys=("name", "sector", "industry", "exchange", "currency", "country"))
        if fund_data:
            merge_into(result, fund_data, force_keys=("market_cap", "pe_ttm", "eps_ttm", "name", "sector", "industry"))
        if hist_data:
            merge_into(
                result,
                {k: v for k, v in hist_data.items() if v is not None},
                force_keys=("rsi_14", "volatility_30d", "forecast_confidence", "forecast_price_1m", "forecast_price_3m", "forecast_price_12m"),
            )

        # Always enforce KSA identity defaults
        _ensure_ksa_identity(result, symbol=sym)

        # Derived + aliases
        fill_derived_quote_fields(result)

        cat, score = data_quality_score(result)
        result["data_quality"] = cat.value
        result["data_quality_score"] = float(score)

        if _emit_warnings():
            warns = []
            for src in (quote_data, fund_data, hist_data, prof_data):
                if isinstance(src, dict) and "_warn" in src:
                    warns.append(str(src["_warn"]))
            if warns:
                result["_warnings"] = " | ".join(warns[:3])

        return clean_patch(result)

    async def get_metrics(self) -> Dict[str, Any]:
        return {
            "provider": PROVIDER_NAME,
            "version": PROVIDER_VERSION,
            "configured": _configured(),
            "urls": {
                "quote": bool(self.quote_url),
                "fundamentals": bool(self.fundamentals_url),
                "history": bool(self.history_url),
                "profile": bool(self.profile_url),
            },
            "circuit_breaker": self.circuit_breaker.get_stats(),
            "cache_sizes": {
                "quote": await self.quote_cache.size(),
                "fund": await self.fund_cache.size(),
                "history": await self.history_cache.size(),
                "profile": await self.profile_cache.size(),
                "error": await self.error_cache.size(),
            },
            "runtime": {
                "has_numpy": _HAS_NUMPY,
                "has_scipy": SCIPY_AVAILABLE,
                "has_sklearn": SKLEARN_AVAILABLE,
                "has_xgboost": XGB_AVAILABLE,
                "has_statsmodels": STATSMODELS_AVAILABLE,
            },
            "ksa_defaults": {"country": KSA_COUNTRY_CODE, "currency": KSA_CURRENCY, "exchange": KSA_EXCHANGE_NAME},
        }

    async def close(self) -> None:
        self._closing = True
        if self._queue_task and not self._queue_task.done():
            self._queue_task.cancel()
            try:
                await self._queue_task
            except Exception:
                pass
        await self._client.aclose()


# =============================================================================
# Public API (singleton)
# =============================================================================
_CLIENT_INSTANCE: Optional[TadawulClient] = None
_CLIENT_LOCK = asyncio.Lock()


async def get_client() -> TadawulClient:
    global _CLIENT_INSTANCE
    if _CLIENT_INSTANCE is None:
        async with _CLIENT_LOCK:
            if _CLIENT_INSTANCE is None:
                _CLIENT_INSTANCE = TadawulClient()
    return _CLIENT_INSTANCE


async def close_client() -> None:
    global _CLIENT_INSTANCE
    if _CLIENT_INSTANCE:
        await _CLIENT_INSTANCE.close()
        _CLIENT_INSTANCE = None


async def fetch_enriched_quote_patch(symbol: str, *args: Any, **kwargs: Any) -> Dict[str, Any]:
    return await (await get_client()).fetch_enriched_quote_patch(symbol)


async def fetch_quote_patch(symbol: str, *args: Any, **kwargs: Any) -> Dict[str, Any]:
    return await (await get_client()).fetch_quote_patch(symbol)


async def fetch_fundamentals_patch(symbol: str, *args: Any, **kwargs: Any) -> Dict[str, Any]:
    return await (await get_client()).fetch_fundamentals_patch(symbol)


async def fetch_history_patch(symbol: str, *args: Any, **kwargs: Any) -> Dict[str, Any]:
    return await (await get_client()).fetch_history_patch(symbol)


async def fetch_profile_patch(symbol: str, *args: Any, **kwargs: Any) -> Dict[str, Any]:
    return await (await get_client()).fetch_profile_patch(symbol)


async def fetch_quote_and_fundamentals_patch(symbol: str, *args: Any, **kwargs: Any) -> Dict[str, Any]:
    client = await get_client()
    result = dict(await client.fetch_quote_patch(symbol))
    merge_into(result, await client.fetch_fundamentals_patch(symbol))
    _ensure_ksa_identity(result, symbol=normalize_ksa_symbol(symbol) or symbol)
    fill_derived_quote_fields(result)
    return clean_patch(result)


async def fetch_quote_and_history_patch(symbol: str, *args: Any, **kwargs: Any) -> Dict[str, Any]:
    client = await get_client()
    result = dict(await client.fetch_quote_patch(symbol))
    merge_into(result, await client.fetch_history_patch(symbol))
    _ensure_ksa_identity(result, symbol=normalize_ksa_symbol(symbol) or symbol)
    fill_derived_quote_fields(result)
    return clean_patch(result)


async def get_client_metrics() -> Dict[str, Any]:
    return await (await get_client()).get_metrics()


async def aclose_tadawul_client() -> None:
    await close_client()


# =============================================================================
# Provider adapter (router/health compatible)
# =============================================================================
class _TadawulProviderAdapter:
    async def get_quote(self, symbol: str) -> Dict[str, Any]:
        return await fetch_enriched_quote_patch(symbol)

    async def quote(self, symbol: str) -> Dict[str, Any]:
        return await fetch_enriched_quote_patch(symbol)

    async def fetch_quote(self, symbol: str) -> Dict[str, Any]:
        return await fetch_enriched_quote_patch(symbol)


provider = _TadawulProviderAdapter()


def get_provider():
    return provider


def build_provider():
    return provider


__all__ = [
    "PROVIDER_NAME",
    "PROVIDER_VERSION",
    "normalize_ksa_symbol",
    "fetch_enriched_quote_patch",
    "fetch_quote_patch",
    "fetch_fundamentals_patch",
    "fetch_history_patch",
    "fetch_profile_patch",
    "fetch_quote_and_fundamentals_patch",
    "fetch_quote_and_history_patch",
    "get_client_metrics",
    "aclose_tadawul_client",
    "MarketRegime",
    "DataQuality",
    # adapter exports
    "provider",
    "get_provider",
    "build_provider",
]
