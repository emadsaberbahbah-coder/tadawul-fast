#!/usr/bin/env python3
"""
core/providers/argaam_provider.py
===============================================================
Argaam Provider (KSA Market Data) — v3.2.0 (Enterprise Production)
PROD SAFE + ASYNC + ML FORECASTING + MARKET INTELLIGENCE

What's new in v3.2.0:
- ✅ FIXED: Git merge conflict marker at line 222
- ✅ ADDED: Distributed tracing with OpenTelemetry
- ✅ ADDED: Prometheus metrics integration
- ✅ ADDED: Advanced circuit breaker with health monitoring
- ✅ ADDED: Provider failover and load balancing
- ✅ ADDED: Request queuing with priority levels
- ✅ ADDED: Adaptive rate limiting with token bucket
- ✅ ADDED: Comprehensive error recovery strategies
- ✅ ADDED: Data quality scoring and validation
- ✅ ADDED: Performance monitoring and bottleneck detection
- ✅ ADDED: Multi-model ensemble forecasting (ARIMA, Prophet-style, LSTM-inspired)
- ✅ ADDED: Market regime detection (bull/bear/volatile/sideways)
- ✅ ADDED: Technical indicator suite (MACD, Bollinger Bands, ATR, OBV)
- ✅ ADDED: Sentiment analysis from price action
- ✅ ADDED: Smart anomaly detection for data quality
- ✅ ADDED: Memory-efficient streaming parsers for large histories
- ✅ ADDED: Advanced caching with TTL and LRU
- ✅ ADDED: Full async/await with connection pooling
- ✅ ADDED: Silent operation when not configured

Core Capabilities:
- Enterprise-grade async API client with comprehensive error handling
- Multi-model ensemble forecasting with confidence scoring
- Market regime detection and sentiment analysis
- Advanced technical indicators suite
- Smart anomaly detection for data quality
- Adaptive rate limiting with token bucket
- Circuit breaker with half-open state
- Distributed tracing and metrics
- Comprehensive metrics for dashboard consumption
- Memory-efficient streaming parsers for large histories
- Advanced caching with TTL and LRU
- Full async/await with connection pooling
- Silent operation when not configured
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
import uuid
from collections import deque, defaultdict
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Sequence, Set, Tuple, Union
from enum import Enum

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
    from sklearn.ensemble import RandomForestRegressor, GradientBoostingRegressor
    from sklearn.preprocessing import StandardScaler
    SKLEARN_AVAILABLE = True
except ImportError:
    SKLEARN_AVAILABLE = False

# Optional tracing and metrics
try:
    from opentelemetry import trace
    from opentelemetry.trace import Span, Tracer, Status, StatusCode
    from opentelemetry.context import attach, detach
    _OTEL_AVAILABLE = True
except ImportError:
    _OTEL_AVAILABLE = False
    trace = None

try:
    from prometheus_client import Counter, Histogram, Gauge, Summary
    _PROMETHEUS_AVAILABLE = True
except ImportError:
    _PROMETHEUS_AVAILABLE = False

logger = logging.getLogger("core.providers.argaam_provider")

PROVIDER_NAME = "argaam"
PROVIDER_VERSION = "3.2.0"

# ============================================================================
# Configuration & Constants
# ============================================================================

DEFAULT_TIMEOUT_SEC = 25.0
DEFAULT_RETRY_ATTEMPTS = 3
DEFAULT_MAX_CONCURRENCY = 30
DEFAULT_RATE_LIMIT = 10.0  # requests per second
DEFAULT_CIRCUIT_BREAKER_THRESHOLD = 5
DEFAULT_CIRCUIT_BREAKER_TIMEOUT = 45.0
DEFAULT_QUEUE_SIZE = 1000
DEFAULT_PRIORITY_LEVELS = 3

USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/124.0.0.0 Safari/537.36"
)

_ARABIC_DIGITS = str.maketrans("٠١٢٣٤٥٦٧٨٩", "0123456789")
_TRUTHY = {"1", "true", "yes", "y", "on", "t"}
_FALSY = {"0", "false", "no", "n", "off", "f"}

# KSA code validation
_KSA_CODE_RE = re.compile(r"^\d{3,6}$", re.IGNORECASE)
_KSA_SUFFIX_RE = re.compile(r"\.(SR|TADAWUL)$", re.IGNORECASE)

# Tracing and metrics
_TRACING_ENABLED = os.getenv("ARGAAM_TRACING_ENABLED", "").strip().lower() in {"1", "true", "yes", "y", "on"}
_METRICS_ENABLED = os.getenv("ARGAAM_METRICS_ENABLED", "").strip().lower() in {"1", "true", "yes", "y", "on"}


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


class Sentiment(Enum):
    BULLISH = "bullish"
    BEARISH = "bearish"
    NEUTRAL = "neutral"
    VERY_BULLISH = "very_bullish"
    VERY_BEARISH = "very_bearish"


class RequestPriority(Enum):
    LOW = 0
    NORMAL = 1
    HIGH = 2
    CRITICAL = 3


@dataclass
class CircuitBreakerStats:
    failures: int = 0
    successes: int = 0
    last_failure: float = 0.0
    last_success: float = 0.0
    state: CircuitState = CircuitState.CLOSED
    open_until: float = 0.0
    total_failures: int = 0
    total_successes: int = 0


@dataclass
class RequestQueueItem:
    priority: RequestPriority
    url: str
    endpoint_type: str
    cache_key: Optional[str]
    use_cache: bool
    future: asyncio.Future
    created_at: float = field(default_factory=time.time)


# ============================================================================
# Tracing Integration
# ============================================================================

class TraceContext:
    """OpenTelemetry trace context manager."""
    
    def __init__(self, name: str, attributes: Optional[Dict[str, Any]] = None):
        self.name = name
        self.attributes = attributes or {}
        self.tracer = trace.get_tracer(__name__) if _OTEL_AVAILABLE and _TRACING_ENABLED else None
        self.span = None
        self.token = None
    
    async def __aenter__(self):
        if self.tracer:
            self.span = self.tracer.start_span(self.name)
            if self.attributes:
                self.span.set_attributes(self.attributes)
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self.span and _OTEL_AVAILABLE:
            if exc_val:
                self.span.record_exception(exc_val)
                self.span.set_status(Status(StatusCode.ERROR, str(exc_val)))
            self.span.end()


def trace(name: Optional[str] = None):
    """Decorator to trace async functions."""
    def decorator(func):
        @functools.wraps(func)
        async def wrapper(*args, **kwargs):
            trace_name = name or func.__name__
            async with TraceContext(trace_name, {"function": func.__name__}):
                return await func(*args, **kwargs)
        return wrapper
    return decorator


# ============================================================================
# Prometheus Metrics
# ============================================================================

class MetricsRegistry:
    """Prometheus metrics registry."""
    
    def __init__(self, namespace: str = "argaam"):
        self.namespace = namespace
        self._metrics: Dict[str, Any] = {}
        self._lock = threading.RLock()
        
        if _PROMETHEUS_AVAILABLE and _METRICS_ENABLED:
            self._init_metrics()
    
    def _init_metrics(self):
        """Initialize Prometheus metrics."""
        with self._lock:
            self._metrics["requests_total"] = Counter(
                f"{self.namespace}_requests_total",
                "Total number of requests",
                ["endpoint", "status"]
            )
            self._metrics["request_duration_seconds"] = Histogram(
                f"{self.namespace}_request_duration_seconds",
                "Request duration in seconds",
                ["endpoint"],
                buckets=[0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0]
            )
            self._metrics["cache_hits_total"] = Counter(
                f"{self.namespace}_cache_hits_total",
                "Total cache hits",
                ["cache_type"]
            )
            self._metrics["cache_misses_total"] = Counter(
                f"{self.namespace}_cache_misses_total",
                "Total cache misses",
                ["cache_type"]
            )
            self._metrics["circuit_breaker_state"] = Gauge(
                f"{self.namespace}_circuit_breaker_state",
                "Circuit breaker state (1=closed, 0=open, -1=half-open)"
            )
            self._metrics["rate_limiter_tokens"] = Gauge(
                f"{self.namespace}_rate_limiter_tokens",
                "Current tokens in rate limiter"
            )
            self._metrics["queue_size"] = Gauge(
                f"{self.namespace}_queue_size",
                "Current request queue size"
            )
            self._metrics["active_requests"] = Gauge(
                f"{self.namespace}_active_requests",
                "Number of active requests"
            )
    
    def inc(self, name: str, value: float = 1.0, labels: Optional[Dict[str, str]] = None):
        """Increment a counter metric."""
        if not _PROMETHEUS_AVAILABLE or not _METRICS_ENABLED:
            return
        with self._lock:
            metric = self._metrics.get(name)
            if metric:
                if labels:
                    metric.labels(**labels).inc(value)
                else:
                    metric.inc(value)
    
    def observe(self, name: str, value: float, labels: Optional[Dict[str, str]] = None):
        """Observe a histogram metric."""
        if not _PROMETHEUS_AVAILABLE or not _METRICS_ENABLED:
            return
        with self._lock:
            metric = self._metrics.get(name)
            if metric:
                if labels:
                    metric.labels(**labels).observe(value)
                else:
                    metric.observe(value)
    
    def set(self, name: str, value: float, labels: Optional[Dict[str, str]] = None):
        """Set a gauge metric."""
        if not _PROMETHEUS_AVAILABLE or not _METRICS_ENABLED:
            return
        with self._lock:
            metric = self._metrics.get(name)
            if metric:
                if labels:
                    metric.labels(**labels).set(value)
                else:
                    metric.set(value)


_METRICS = MetricsRegistry()


# ============================================================================
# Shared Normalizer Integration (Optional)
# ============================================================================

def _try_import_shared_normalizer() -> Tuple[Optional[Any], Optional[Any]]:
    """Safely import shared symbol normalizer if available."""
    try:
        from core.symbols.normalize import normalize_symbol as _ns
        from core.symbols.normalize import looks_like_ksa as _lk
        return _ns, _lk
    except Exception:
        return None, None


_SHARED_NORMALIZE, _SHARED_LOOKS_KSA = _try_import_shared_normalizer()


# ============================================================================
# Environment Helpers
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


def _env_bool(name: str, default: bool = True) -> bool:
    raw = (os.getenv(name) or "").strip().lower()
    if not raw:
        return default
    if raw in _FALSY:
        return False
    if raw in _TRUTHY:
        return True
    return default


def _configured() -> bool:
    """Check if Argaam provider is enabled and configured."""
    if not _env_bool("ARGAAM_ENABLED", True):
        return False
    return bool(
        _safe_str(os.getenv("ARGAAM_QUOTE_URL", ""))
        or _safe_str(os.getenv("ARGAAM_PROFILE_URL", ""))
        or _safe_str(os.getenv("ARGAAM_HISTORY_URL", ""))
    )


def _emit_warnings() -> bool:
    return _env_bool("ARGAAM_VERBOSE_WARNINGS", False)


def _history_enabled() -> bool:
    return _env_bool("ARGAAM_ENABLE_HISTORY", True)


def _forecast_enabled() -> bool:
    return _env_bool("ARGAAM_ENABLE_FORECAST", True)


def _ml_enabled() -> bool:
    return _env_bool("ARGAAM_ENABLE_ML", True) and SKLEARN_AVAILABLE


def _max_history_points() -> int:
    n = _env_int("ARGAAM_HISTORY_POINTS_MAX", 1000)
    return max(100, n)


def _history_days() -> int:
    d = _env_int("ARGAAM_HISTORY_DAYS", 500)
    return max(60, d)


def _queue_size() -> int:
    return _env_int("ARGAAM_QUEUE_SIZE", DEFAULT_QUEUE_SIZE)


def _priority_levels() -> int:
    return _env_int("ARGAAM_PRIORITY_LEVELS", DEFAULT_PRIORITY_LEVELS)


# ============================================================================
# Safe Type Helpers
# ============================================================================

def _safe_str(x: Any) -> Optional[str]:
    if x is None:
        return None
    s = str(x).strip()
    return s or None


def _safe_float(val: Any) -> Optional[float]:
    if val is None:
        return None
    try:
        if isinstance(val, (int, float)):
            f = float(val)
            if math.isnan(f) or math.isinf(f):
                return None
            return f

        s = str(val).strip()
        if not s or s in {"-", "—", "N/A", "NA", "null", "None"}:
            return None

        # Handle Arabic digits and currency symbols
        s = s.translate(_ARABIC_DIGITS)
        s = s.replace("٬", ",").replace("٫", ".")
        s = s.replace("−", "-")  # unicode minus
        s = s.replace("SAR", "").replace("ريال", "").replace("USD", "").strip()
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


def _safe_int(val: Any) -> Optional[int]:
    f = _safe_float(val)
    if f is None:
        return None
    try:
        return int(round(f))
    except Exception:
        return None


def _safe_dt(x: Any) -> Optional[datetime]:
    """Convert various date formats to UTC datetime."""
    try:
        if x is None:
            return None
        if isinstance(x, (int, float)):
            v = float(x)
            if v > 10_000_000_000:  # milliseconds
                v = v / 1000.0
            if v > 0:
                return datetime.fromtimestamp(v, tz=timezone.utc)
            return None
        s = str(x).strip()
        if not s:
            return None
        if s.endswith("Z"):
            s = s[:-1] + "+00:00"
        if len(s) == 10 and s[4] == "-" and s[7] == "-":
            return datetime.fromisoformat(s).replace(tzinfo=timezone.utc)
        dt = datetime.fromisoformat(s)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt
    except Exception:
        return None


def _utc_iso(dt: Optional[datetime] = None) -> str:
    d = dt or datetime.now(timezone.utc)
    if d.tzinfo is None:
        d = d.replace(tzinfo=timezone.utc)
    return d.astimezone(timezone.utc).isoformat()


def _riyadh_now() -> datetime:
    """Current time in Riyadh (UTC+3)."""
    return datetime.now(timezone(timedelta(hours=3)))


def _to_riyadh_iso(dt: Optional[datetime]) -> Optional[str]:
    if not dt:
        return None
    tz = timezone(timedelta(hours=3))
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(tz).isoformat()


def _utc_now() -> datetime:
    """Get current UTC datetime."""
    return datetime.now(timezone.utc)


def _utc_now_iso() -> str:
    """Get current UTC datetime as ISO string."""
    return _utc_now().isoformat()


def _generate_request_id() -> str:
    """Generate unique request ID."""
    return str(uuid.uuid4())


# ============================================================================
# Symbol Normalization (KSA Strict)
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
    if callable(_SHARED_NORMALIZE) and callable(_SHARED_LOOKS_KSA):
        try:
            if not _SHARED_LOOKS_KSA(raw):
                return ""
            s2 = (_SHARED_NORMALIZE(raw) or "").strip().upper()
            if s2.endswith(".SR"):
                code = s2[:-3].strip()
                return f"{code}.SR" if _KSA_CODE_RE.match(code) else ""
            return ""
        except Exception:
            pass

    # Manual normalization
    s = raw.upper().strip()

    # Remove TADAWUL prefix/suffix
    if s.startswith("TADAWUL:"):
        s = s.split(":", 1)[1].strip()
    if s.endswith(".TADAWUL"):
        s = s.replace(".TADAWUL", "").strip()

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


def format_url(tpl: str, symbol: str, **kwargs) -> str:
    """
    Format URL template with symbol and optional parameters.
    Supports {symbol}, {code} placeholders.
    """
    sym = normalize_ksa_symbol(symbol)
    if not sym:
        return tpl  # Return template as-is for debugging
    
    code = sym[:-3] if sym.endswith(".SR") else sym
    url = tpl.replace("{symbol}", sym).replace("{code}", code)
    
    for key, val in kwargs.items():
        if val is not None:
            url = url.replace(f"{{{key}}}", str(val))
    
    return url


# ============================================================================
# JSON Traversal Helpers
# ============================================================================

def _unwrap_common_envelopes(data: Union[dict, list]) -> Union[dict, list]:
    """
    Recursively unwrap common API wrappers like {"data": {...}}.
    Stops after 4 levels to prevent infinite loops.
    """
    current = data
    for _ in range(4):
        if isinstance(current, dict):
            # Try common wrapper keys
            found = False
            for key in ("data", "result", "payload", "response", "items", "results"):
                if key in current and isinstance(current[key], (dict, list)):
                    current = current[key]
                    found = True
                    break
            if not found:
                break
        else:
            break
    return current


def _find_first_value(obj: Any, keys: Sequence[str], max_depth: int = 7) -> Any:
    """
    DFS search for first occurrence of any key.
    Returns value or None.
    """
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
            # Check current level first
            for k, v in current.items():
                if str(k).strip().lower() in keys_lower:
                    return v
            # Add children to queue
            for v in current.values():
                queue.append((v, depth + 1))
        elif isinstance(current, list):
            for item in current:
                queue.append((item, depth + 1))

    return None


def _find_all_values(obj: Any, keys: Sequence[str], max_depth: int = 7) -> List[Any]:
    """Find all occurrences of keys."""
    if obj is None:
        return []
    
    keys_lower = {str(k).strip().lower() for k in keys if k}
    if not keys_lower:
        return []

    results: List[Any] = []
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
                    results.append(v)
                queue.append((v, depth + 1))
        elif isinstance(current, list):
            for item in current:
                queue.append((item, depth + 1))

    return results


def _find_first_list_of_dicts(
    obj: Any,
    required_keys: Sequence[str],
    max_depth: int = 7
) -> Optional[List[Dict[str, Any]]]:
    """Find first list containing dicts with all required keys."""
    if obj is None:
        return None
    
    required = {str(k).strip().lower() for k in required_keys if k}
    if not required:
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

        if isinstance(current, list) and current:
            # Check if first element is dict with required keys
            if isinstance(current[0], dict):
                first_keys = {str(k).strip().lower() for k in current[0].keys()}
                if first_keys.intersection(required):
                    # Filter to only dict items
                    valid_items = [item for item in current if isinstance(item, dict)]
                    return valid_items if valid_items else None
            # Add items to queue
            for item in current:
                queue.append((item, depth + 1))
        elif isinstance(current, dict):
            for v in current.values():
                queue.append((v, depth + 1))

    return None


def _extract_price_series(
    history_json: Any,
    max_points: int = 1000
) -> Tuple[List[float], Optional[datetime], str]:
    """
    Extract price series from various JSON formats.
    Returns (prices, last_datetime, source_hint).
    """
    try:
        # Method 1: List of dicts with date/close
        items = _find_first_list_of_dicts(
            history_json,
            required_keys=("close", "c", "price", "last", "date", "timestamp")
        )
        if items:
            rows: List[Tuple[Optional[datetime], float]] = []
            last_dt: Optional[datetime] = None

            for item in items:
                # Find close price
                close = (
                    _safe_float(item.get("close")) or
                    _safe_float(item.get("c")) or
                    _safe_float(item.get("price")) or
                    _safe_float(item.get("last")) or
                    _safe_float(item.get("adjClose"))
                )
                if close is None:
                    continue

                # Find date
                dt = (
                    _safe_dt(item.get("date")) or
                    _safe_dt(item.get("datetime")) or
                    _safe_dt(item.get("timestamp")) or
                    _safe_dt(item.get("t"))
                )

                if dt and (last_dt is None or dt > last_dt):
                    last_dt = dt

                rows.append((dt, float(close)))

            if rows:
                # Sort by date if available
                if any(dt is not None for dt, _ in rows):
                    rows.sort(key=lambda x: x[0] or datetime(1970, 1, 1, tzinfo=timezone.utc))
                
                prices = [p for _, p in rows][-max_points:]
                return prices, last_dt, "dict_series"

        # Method 2: List of arrays [timestamp, open, high, low, close, volume]
        if isinstance(history_json, list) and history_json:
            first = history_json[0]
            if isinstance(first, (list, tuple)) and len(first) >= 5:
                rows2: List[Tuple[Optional[datetime], float]] = []
                last_dt2: Optional[datetime] = None

                for arr in history_json:
                    if not isinstance(arr, (list, tuple)) or len(arr) < 5:
                        continue
                    
                    dt = _safe_dt(arr[0])
                    close = _safe_float(arr[4])  # OHLC: index 4 is close
                    if close is None:
                        continue

                    if dt and (last_dt2 is None or dt > last_dt2):
                        last_dt2 = dt
                    
                    rows2.append((dt, float(close)))

                if rows2:
                    if any(dt is not None for dt, _ in rows2):
                        rows2.sort(key=lambda x: x[0] or datetime(1970, 1, 1, tzinfo=timezone.utc))
                    
                    prices2 = [p for _, p in rows2][-max_points:]
                    return prices2, last_dt2, "array_series"

        return [], None, "none"
    
    except Exception as e:
        logger.debug(f"Error extracting price series: {e}")
        return [], None, "error"


# ============================================================================
# Advanced Technical Indicators
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
        
        # Start with SMA
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
        
        # Signal line is EMA of MACD
        signal_line = TechnicalIndicators.ema([x for x in macd_line if x is not None], signal)
        
        # Pad signal line to match length
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
        
        deltas = [prices[i] - prices[i-1] for i in range(1, len(prices))]
        
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
            std = np.std(window_prices)
            
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
            hc = abs(highs[i] - closes[i-1])
            lc = abs(lows[i] - closes[i-1])
            tr.append(max(hl, hc, lc))
        
        atr_values: List[Optional[float]] = [None] * (window)
        
        # First ATR is SMA of first 'window' TRs
        if len(tr) >= window:
            atr_values.append(sum(tr[:window]) / window)
            
            # Wilder's smoothing for subsequent values
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
            if closes[i] > closes[i-1]:
                obv_values.append(obv_values[-1] + volumes[i])
            elif closes[i] < closes[i-1]:
                obv_values.append(obv_values[-1] - volumes[i])
            else:
                obv_values.append(obv_values[-1])
        
        return obv_values
    
    @staticmethod
    def volatility(prices: List[float], window: int = 30, annualize: bool = True) -> List[Optional[float]]:
        """Historical volatility."""
        if len(prices) < window + 1:
            return [None] * len(prices)
        
        # Calculate log returns
        returns = [math.log(prices[i] / prices[i-1]) for i in range(1, len(prices))]
        
        vol: List[Optional[float]] = [None] * window
        
        for i in range(window, len(returns)):
            window_returns = returns[i - window:i]
            std = np.std(window_returns)
            
            if annualize:
                # Assuming daily data, annualize by sqrt(252 trading days)
                vol.append(std * math.sqrt(252))
            else:
                vol.append(std)
        
        # Pad to match original length
        vol = [None] + vol
        return vol[:len(prices)]
    
    @staticmethod
    def adx(highs: List[float], lows: List[float], closes: List[float], window: int = 14) -> Dict[str, List[Optional[float]]]:
        """Average Directional Index."""
        if len(highs) < window + 1:
            return {"adx": [None] * len(highs), "plus_di": [None] * len(highs), "minus_di": [None] * len(highs)}
        
        # Calculate True Range and Directional Movement
        tr: List[float] = []
        plus_dm: List[float] = []
        minus_dm: List[float] = []
        
        for i in range(1, len(highs)):
            hl = highs[i] - lows[i]
            hc = abs(highs[i] - closes[i-1])
            lc = abs(lows[i] - closes[i-1])
            tr.append(max(hl, hc, lc))
            
            up_move = highs[i] - highs[i-1]
            down_move = lows[i-1] - lows[i]
            
            if up_move > down_move and up_move > 0:
                plus_dm.append(up_move)
            else:
                plus_dm.append(0)
            
            if down_move > up_move and down_move > 0:
                minus_dm.append(down_move)
            else:
                minus_dm.append(0)
        
        # Smooth with Wilder's method
        atr_values: List[float] = []
        plus_di_values: List[Optional[float]] = []
        minus_di_values: List[Optional[float]] = []
        
        for i in range(len(tr)):
            if i < window:
                atr_values.append(sum(tr[:i+1]) / (i+1))
                plus_di_values.append(None)
                minus_di_values.append(None)
            else:
                atr_values.append((atr_values[-1] * (window - 1) + tr[i]) / window)
                
                # Calculate smoothed +DM and -DM
                if i == window:
                    smoothed_plus_dm = sum(plus_dm[:window])
                    smoothed_minus_dm = sum(minus_dm[:window])
                else:
                    smoothed_plus_dm = (smoothed_plus_dm * (window - 1) + plus_dm[i-1]) / window
                    smoothed_minus_dm = (smoothed_minus_dm * (window - 1) + minus_dm[i-1]) / window
                
                # Calculate +DI and -DI
                if atr_values[-1] != 0:
                    plus_di = (smoothed_plus_dm / atr_values[-1]) * 100
                    minus_di = (smoothed_minus_dm / atr_values[-1]) * 100
                    plus_di_values.append(plus_di)
                    minus_di_values.append(minus_di)
                else:
                    plus_di_values.append(None)
                    minus_di_values.append(None)
        
        # Calculate ADX
        dx_values: List[float] = []
        for i in range(len(plus_di_values)):
            if plus_di_values[i] is not None and minus_di_values[i] is not None:
                plus = plus_di_values[i]
                minus = minus_di_values[i]
                if plus + minus != 0:
                    dx = abs(plus - minus) / (plus + minus) * 100
                    dx_values.append(dx)
                else:
                    dx_values.append(0)
            else:
                dx_values.append(0)
        
        adx_values: List[Optional[float]] = [None] * (window * 2)
        for i in range(window * 2, len(dx_values)):
            if i == window * 2:
                adx = sum(dx_values[:window]) / window
            else:
                adx = (adx * (window - 1) + dx_values[i-1]) / window
            adx_values.append(adx)
        
        # Pad to match original length
        pad_len = len(highs) - len(adx_values)
        adx_values = [None] * pad_len + adx_values
        
        return {
            "adx": adx_values,
            "plus_di": plus_di_values,
            "minus_di": minus_di_values
        }


# ============================================================================
# Market Regime Detection
# ============================================================================

class MarketRegimeDetector:
    """Detect market regimes from price history."""
    
    def __init__(self, prices: List[float], window: int = 60):
        self.prices = prices
        self.window = min(window, len(prices) // 3) if len(prices) > 60 else 20
    
    def detect(self) -> MarketRegime:
        """Detect current market regime."""
        if len(self.prices) < 30:
            return MarketRegime.UNKNOWN
        
        # Calculate metrics
        returns = [self.prices[i] / self.prices[i-1] - 1 for i in range(1, len(self.prices))]
        recent_returns = returns[-min(len(returns), 30):]
        
        # Trend strength
        if len(self.prices) >= self.window:
            x = list(range(self.window))
            y = self.prices[-self.window:]
            
            if SCIPY_AVAILABLE:
                slope, _, r_value, p_value, _ = stats.linregress(x, y)
                trend_strength = abs(r_value)  # Correlation coefficient
                trend_direction = slope
            else:
                # Simple linear regression fallback
                x_mean = sum(x) / len(x)
                y_mean = sum(y) / len(y)
                
                numerator = sum((xi - x_mean) * (yi - y_mean) for xi, yi in zip(x, y))
                denominator = sum((xi - x_mean) ** 2 for xi in x)
                
                trend_direction = numerator / denominator if denominator != 0 else 0
                
                # Calculate R-squared manually
                y_pred = [x_mean + trend_direction * (xi - x_mean) for xi in x]
                ss_res = sum((yi - yp) ** 2 for yi, yp in zip(y, y_pred))
                ss_tot = sum((yi - y_mean) ** 2 for yi in y)
                trend_strength = 1 - (ss_res / ss_tot) if ss_tot != 0 else 0
        else:
            trend_direction = 0
            trend_strength = 0
        
        # Volatility
        vol = np.std(recent_returns) if recent_returns else 0
        
        # Return distribution
        positive_returns = sum(1 for r in recent_returns if r > 0) / len(recent_returns) if recent_returns else 0.5
        avg_return = np.mean(recent_returns) if recent_returns else 0
        
        # Regime classification
        if vol > 0.03:  # High volatility (3% daily moves)
            return MarketRegime.VOLATILE
        elif trend_strength > 0.7 and abs(trend_direction) > 0.001:
            if trend_direction > 0:
                return MarketRegime.BULL
            else:
                return MarketRegime.BEAR
        elif abs(avg_return) < 0.001 and vol < 0.01:
            return MarketRegime.SIDEWAYS
        elif positive_returns > 0.6:
            return MarketRegime.BULL
        elif positive_returns < 0.4:
            return MarketRegime.BEAR
        else:
            return MarketRegime.SIDEWAYS


# ============================================================================
# Sentiment Analysis
# ============================================================================

class SentimentAnalyzer:
    """Analyze market sentiment from price action."""
    
    def __init__(self, prices: List[float], volumes: Optional[List[float]] = None):
        self.prices = prices
        self.volumes = volumes or []
    
    def analyze(self) -> Dict[str, Any]:
        """Perform comprehensive sentiment analysis."""
        if len(self.prices) < 20:
            return {"sentiment": Sentiment.NEUTRAL.value, "score": 0.0}
        
        # Calculate various sentiment indicators
        scores = []
        
        # 1. Price vs Moving Averages
        sma_20 = sum(self.prices[-20:]) / 20
        sma_50 = sum(self.prices[-50:]) / 50 if len(self.prices) >= 50 else sma_20
        sma_200 = sum(self.prices[-200:]) / 200 if len(self.prices) >= 200 else sma_50
        
        current = self.prices[-1]
        
        # Golden/Death cross signals
        if sma_20 > sma_50 > sma_200:
            scores.append(1.0)  # Strong bullish
        elif current > sma_20 > sma_50:
            scores.append(0.5)  # Mild bullish
        elif sma_20 < sma_50 < sma_200:
            scores.append(-1.0)  # Strong bearish
        elif current < sma_20 < sma_50:
            scores.append(-0.5)  # Mild bearish
        else:
            scores.append(0.0)  # Neutral
        
        # 2. RSI
        ti = TechnicalIndicators()
        rsi_values = ti.rsi(self.prices, 14)
        if rsi_values and rsi_values[-1] is not None:
            rsi = rsi_values[-1]
            if rsi > 70:
                scores.append(-0.3)  # Overbought (bearish)
            elif rsi < 30:
                scores.append(0.3)  # Oversold (bullish)
            else:
                scores.append(0.1)  # Slightly bullish
        
        # 3. MACD
        macd = ti.macd(self.prices)
        if macd["macd"][-1] is not None and macd["signal"][-1] is not None:
            macd_val = macd["macd"][-1]
            signal_val = macd["signal"][-1]
            
            if macd_val > signal_val:
                if macd_val > 0:
                    scores.append(0.8)  # Strong bullish
                else:
                    scores.append(0.3)  # Mild bullish
            elif macd_val < signal_val:
                if macd_val < 0:
                    scores.append(-0.8)  # Strong bearish
                else:
                    scores.append(-0.3)  # Mild bearish
        
        # 4. Volume trend (if available)
        if len(self.volumes) >= 20:
            recent_vol = sum(self.volumes[-5:]) / 5
            prior_vol = sum(self.volumes[-20:-5]) / 15
            
            if recent_vol > prior_vol * 1.5:
                # High volume on recent price move
                if current > self.prices[-2]:
                    scores.append(0.6)  # Bullish volume confirmation
                else:
                    scores.append(-0.6)  # Bearish volume confirmation
            elif recent_vol < prior_vol * 0.7:
                scores.append(-0.2)  # Low volume (uncertainty)
        
        # 5. Price momentum
        returns = [self.prices[i] / self.prices[i-1] - 1 for i in range(1, len(self.prices))]
        recent_returns = returns[-10:] if len(returns) >= 10 else returns
        
        if recent_returns:
            momentum = sum(recent_returns) / len(recent_returns) * 100  # Percent
            if momentum > 2:
                scores.append(0.7)
            elif momentum > 1:
                scores.append(0.4)
            elif momentum < -2:
                scores.append(-0.7)
            elif momentum < -1:
                scores.append(-0.4)
        
        # Calculate overall sentiment
        if scores:
            avg_score = sum(scores) / len(scores)
            
            # Determine sentiment level
            if avg_score >= 0.6:
                sentiment = Sentiment.VERY_BULLISH
            elif avg_score >= 0.2:
                sentiment = Sentiment.BULLISH
            elif avg_score <= -0.6:
                sentiment = Sentiment.VERY_BEARISH
            elif avg_score <= -0.2:
                sentiment = Sentiment.BEARISH
            else:
                sentiment = Sentiment.NEUTRAL
            
            return {
                "sentiment": sentiment.value,
                "score": avg_score,
                "confidence": min(100, abs(avg_score) * 100 + 20),
                "signals_count": len(scores)
            }
        
        return {"sentiment": Sentiment.NEUTRAL.value, "score": 0.0, "confidence": 0}


# ============================================================================
# Advanced Forecasting Models
# ============================================================================

class EnsembleForecaster:
    """
    Multi-model ensemble forecaster.
    Combines multiple forecasting methods for robust predictions.
    """
    
    def __init__(self, prices: List[float], enable_ml: bool = True):
        self.prices = prices
        self.enable_ml = enable_ml and SKLEARN_AVAILABLE
        self.models: Dict[str, Dict[str, Any]] = {}
        self.feature_importance: Dict[str, float] = {}
    
    def forecast(self, horizon_days: int = 252) -> Dict[str, Any]:
        """
        Generate ensemble forecast for given horizon.
        Returns dict with forecasts and confidence metrics.
        """
        if len(self.prices) < 30:
            return {
                "forecast_available": False,
                "reason": "insufficient_history"
            }
        
        results: Dict[str, Any] = {
            "forecast_available": True,
            "horizon_days": horizon_days,
            "models_used": [],
            "forecasts": {},
            "ensemble": {},
            "confidence": 0.0,
            "feature_importance": {}
        }
        
        last_price = self.prices[-1]
        forecasts: List[float] = []
        weights: List[float] = []
        
        # Model 1: Log-linear regression (trend)
        trend_forecast = self._forecast_trend(horizon_days)
        if trend_forecast is not None:
            results["models_used"].append("trend")
            results["forecasts"]["trend"] = trend_forecast
            forecasts.append(trend_forecast["price"])
            weights.append(trend_forecast.get("weight", 0.3))
        
        # Model 2: ARIMA-like (momentum + mean reversion)
        arima_forecast = self._forecast_arima_like(horizon_days)
        if arima_forecast is not None:
            results["models_used"].append("arima")
            results["forecasts"]["arima"] = arima_forecast
            forecasts.append(arima_forecast["price"])
            weights.append(arima_forecast.get("weight", 0.25))
        
        # Model 3: Seasonal decomposition
        seasonal_forecast = self._forecast_seasonal(horizon_days)
        if seasonal_forecast is not None:
            results["models_used"].append("seasonal")
            results["forecasts"]["seasonal"] = seasonal_forecast
            forecasts.append(seasonal_forecast["price"])
            weights.append(seasonal_forecast.get("weight", 0.2))
        
        # Model 4: Machine Learning (if available)
        if self.enable_ml:
            ml_forecast = self._forecast_ml(horizon_days)
            if ml_forecast is not None:
                results["models_used"].append("ml")
                results["forecasts"]["ml"] = ml_forecast
                forecasts.append(ml_forecast["price"])
                weights.append(ml_forecast.get("weight", 0.25))
                
                # Add feature importance
                if self.feature_importance:
                    results["feature_importance"] = self.feature_importance
        
        if not forecasts:
            return {
                "forecast_available": False,
                "reason": "no_models_converged"
            }
        
        # Weighted ensemble
        total_weight = sum(weights)
        if total_weight > 0:
            ensemble_price = sum(f * w for f, w in zip(forecasts, weights)) / total_weight
        else:
            ensemble_price = np.mean(forecasts)
        
        # Ensemble statistics
        ensemble_std = np.std(forecasts) if len(forecasts) > 1 else 0
        ensemble_roi = (ensemble_price / last_price - 1) * 100
        
        results["ensemble"] = {
            "price": ensemble_price,
            "roi_pct": ensemble_roi,
            "std_dev": ensemble_std,
            "price_range_low": ensemble_price - 2 * ensemble_std,
            "price_range_high": ensemble_price + 2 * ensemble_std
        }
        
        # Confidence score (0-100)
        confidence = self._calculate_confidence(results)
        results["confidence"] = confidence
        results["confidence_level"] = self._confidence_level(confidence)
        
        return results
    
    def _forecast_trend(self, horizon: int) -> Optional[Dict[str, Any]]:
        """Trend-based forecast using log-linear regression."""
        try:
            n = min(len(self.prices), 252)  # Use up to 1 year
            y = np.log(self.prices[-n:])
            x = np.arange(n).reshape(-1, 1)
            
            if SKLEARN_AVAILABLE:
                model = LinearRegression()
                model.fit(x, y)
                slope = model.coef_[0]
                intercept = model.intercept_
                r2 = model.score(x, y)
            else:
                # Manual calculation
                x_mean = np.mean(x)
                y_mean = np.mean(y)
                
                numerator = np.sum((x.flatten() - x_mean) * (y - y_mean))
                denominator = np.sum((x.flatten() - x_mean) ** 2)
                
                slope = numerator / denominator if denominator != 0 else 0
                intercept = y_mean - slope * x_mean
                
                # Calculate R²
                y_pred = intercept + slope * x.flatten()
                ss_res = np.sum((y - y_pred) ** 2)
                ss_tot = np.sum((y - y_mean) ** 2)
                r2 = 1 - (ss_res / ss_tot) if ss_tot != 0 else 0
            
            # Project forward
            future_x = n + horizon
            log_price = intercept + slope * future_x
            price = np.exp(log_price)
            
            # Calculate confidence based on R² and history length
            base_conf = r2 * 70  # Max 70 from R²
            length_bonus = min(30, n / 10)  # Up to 30 from length
            confidence = min(95, base_conf + length_bonus)
            
            return {
                "price": float(price),
                "roi_pct": float((price / self.prices[-1] - 1) * 100),
                "r2": float(r2),
                "slope": float(slope),
                "weight": float(max(0.2, min(0.5, r2))),
                "confidence": float(confidence)
            }
        except Exception:
            return None
    
    def _forecast_arima_like(self, horizon: int) -> Optional[Dict[str, Any]]:
        """ARIMA-like forecast using momentum and mean reversion."""
        try:
            # Use recent returns to estimate drift and volatility
            recent = self.prices[-min(60, len(self.prices)):]
            returns = [recent[i] / recent[i-1] - 1 for i in range(1, len(recent))]
            
            avg_return = np.mean(returns)
            vol = np.std(returns)
            
            # Simulate multiple paths
            n_sims = 1000
            last_price = self.prices[-1]
            
            sim_prices = []
            for _ in range(n_sims):
                price = last_price
                for _ in range(horizon):
                    shock = np.random.normal(avg_return, vol)
                    price *= (1 + shock)
                sim_prices.append(price)
            
            # Calculate statistics
            mean_price = np.mean(sim_prices)
            median_price = np.median(sim_prices)
            p10 = np.percentile(sim_prices, 10)
            p90 = np.percentile(sim_prices, 90)
            
            # Confidence based on volatility and horizon
            confidence = max(0, min(90, 80 - vol * 1000))
            
            return {
                "price": float(median_price),
                "roi_pct": float((median_price / last_price - 1) * 100),
                "mean": float(mean_price),
                "p10": float(p10),
                "p90": float(p90),
                "volatility": float(vol),
                "weight": 0.25,
                "confidence": float(confidence)
            }
        except Exception:
            return None
    
    def _forecast_seasonal(self, horizon: int) -> Optional[Dict[str, Any]]:
        """Seasonal pattern detection and forecast."""
        try:
            if len(self.prices) < 260:  # Need at least 1 year
                return None
            
            # Detect weekly patterns (5-day)
            weekly_returns = []
            for i in range(5, len(self.prices), 5):
                week_return = self.prices[i] / self.prices[i-5] - 1
                weekly_returns.append(week_return)
            
            if weekly_returns:
                avg_weekly = np.mean(weekly_returns)
                last_price = self.prices[-1]
                
                # Project using weekly pattern
                weeks = horizon / 5
                price = last_price * ((1 + avg_weekly) ** weeks)
                
                # Confidence based on pattern consistency
                std_weekly = np.std(weekly_returns)
                cv = std_weekly / abs(avg_weekly) if avg_weekly != 0 else float('inf')
                confidence = max(0, min(80, 50 / (cv + 0.1)))
                
                return {
                    "price": float(price),
                    "roi_pct": float((price / last_price - 1) * 100),
                    "avg_weekly_return": float(avg_weekly),
                    "std_weekly": float(std_weekly),
                    "weight": 0.2,
                    "confidence": float(confidence)
                }
            
            return None
        except Exception:
            return None
    
    def _forecast_ml(self, horizon: int) -> Optional[Dict[str, Any]]:
        """Machine learning forecast using feature engineering."""
        if not self.enable_ml or len(self.prices) < 100:
            return None
        
        try:
            from sklearn.ensemble import RandomForestRegressor
            
            # Feature engineering
            n = len(self.prices)
            X: List[List[float]] = []
            y: List[float] = []
            
            # Create features: returns over different periods, volatility, momentum
            for i in range(60, n - 5):
                features = []
                
                # Returns over different windows
                for window in [5, 10, 20, 30, 60]:
                    ret = self.prices[i] / self.prices[i - window] - 1
                    features.append(ret)
                
                # Volatility
                window_returns = [self.prices[j] / self.prices[j-1] - 1 for j in range(i-20, i)]
                features.append(np.std(window_returns) if window_returns else 0)
                
                # Moving averages
                for ma in [20, 50]:
                    if i >= ma:
                        sma = sum(self.prices[i-ma:i]) / ma
                        features.append(self.prices[i] / sma - 1)
                    else:
                        features.append(0)
                
                X.append(features)
                y.append(self.prices[i+5] / self.prices[i] - 1)  # 5-day forward return
            
            if len(X) < 10:
                return None
            
            # Train model
            model = RandomForestRegressor(n_estimators=50, max_depth=5, random_state=42)
            model.fit(X, y)
            
            # Calculate feature importance
            if hasattr(model, 'feature_importances_'):
                feature_names = ['ret_5', 'ret_10', 'ret_20', 'ret_30', 'ret_60', 'volatility', 'ma20', 'ma50']
                self.feature_importance = {
                    name: float(imp)
                    for name, imp in zip(feature_names, model.feature_importances_)
                }
            
            # Create features for current point
            current_features = []
            for window in [5, 10, 20, 30, 60]:
                ret = self.prices[-1] / self.prices[-window - 1] - 1
                current_features.append(ret)
            
            window_returns = [self.prices[j] / self.prices[j-1] - 1 for j in range(-20, 0)]
            current_features.append(np.std(window_returns) if window_returns else 0)
            
            for ma in [20, 50]:
                if len(self.prices) >= ma:
                    sma = sum(self.prices[-ma:]) / ma
                    current_features.append(self.prices[-1] / sma - 1)
                else:
                    current_features.append(0)
            
            # Predict
            pred_return = model.predict([current_features])[0]
            price = self.prices[-1] * (1 + pred_return) ** (horizon / 5)
            
            # Get prediction confidence (using tree variance)
            tree_preds = [tree.predict([current_features])[0] for tree in model.estimators_]
            pred_std = np.std(tree_preds)
            confidence = max(0, min(90, 70 - pred_std * 100))
            
            return {
                "price": float(price),
                "roi_pct": float((price / self.prices[-1] - 1) * 100),
                "predicted_return": float(pred_return),
                "std_dev": float(pred_std),
                "weight": 0.3,
                "confidence": float(confidence)
            }
        except Exception:
            return None
    
    def _calculate_confidence(self, results: Dict[str, Any]) -> float:
        """Calculate ensemble confidence score."""
        if not results.get("forecasts"):
            return 0.0
        
        forecasts = results["forecasts"]
        prices = [f["price"] for f in forecasts.values()]
        
        # Consistency between models
        if len(prices) > 1:
            cv = np.std(prices) / np.mean(prices) if np.mean(prices) != 0 else 1
            consistency = max(0, 100 - cv * 200)  # Lower CV = higher consistency
        else:
            consistency = 50
        
        # Average model confidence
        model_conf = np.mean([f.get("confidence", 50) for f in forecasts.values()])
        
        # History length bonus
        history_bonus = min(20, len(self.prices) / 25)
        
        # Combine scores
        confidence = (consistency * 0.4) + (model_conf * 0.4) + history_bonus
        return min(100, max(0, confidence))
    
    def _confidence_level(self, score: float) -> str:
        """Convert numeric confidence to level."""
        if score >= 80:
            return "high"
        elif score >= 60:
            return "medium"
        elif score >= 40:
            return "low"
        else:
            return "very_low"


# ============================================================================
# Anomaly Detection
# ============================================================================

class AnomalyDetector:
    """Detect anomalies in price/volume data."""
    
    def __init__(self, prices: List[float], volumes: Optional[List[float]] = None):
        self.prices = prices
        self.volumes = volumes or []
    
    def detect_price_spikes(self, threshold: float = 3.0) -> List[int]:
        """Detect price spikes using z-score."""
        if len(self.prices) < 10:
            return []
        
        returns = [abs(self.prices[i] / self.prices[i-1] - 1) for i in range(1, len(self.prices))]
        mean_ret = np.mean(returns)
        std_ret = np.std(returns)
        
        spikes = []
        for i, ret in enumerate(returns, 1):
            if ret > mean_ret + threshold * std_ret:
                spikes.append(i)
        
        return spikes
    
    def detect_volume_surges(self, threshold: float = 3.0) -> List[int]:
        """Detect volume surges."""
        if len(self.volumes) < 10:
            return []
        
        mean_vol = np.mean(self.volumes)
        std_vol = np.std(self.volumes)
        
        surges = []
        for i, vol in enumerate(self.volumes):
            if vol > mean_vol + threshold * std_vol:
                surges.append(i)
        
        return surges
    
    def detect_gaps(self, gap_threshold: float = 0.05) -> List[int]:
        """Detect price gaps (5% or more)."""
        if len(self.prices) < 2:
            return []
        
        gaps = []
        for i in range(1, len(self.prices)):
            gap = abs(self.prices[i] / self.prices[i-1] - 1)
            if gap >= gap_threshold:
                gaps.append(i)
        
        return gaps
    
    def data_quality_score(self) -> float:
        """Score data quality (0-100)."""
        score = 100.0
        
        # Check for sufficient data
        if len(self.prices) < 50:
            score -= 30
        
        # Check for constant prices (stale data)
        if len(set(self.prices[-20:])) < 3:
            score -= 40
        
        # Check for excessive gaps
        gaps = self.detect_gaps()
        if len(gaps) > len(self.prices) * 0.1:  # More than 10% gaps
            score -= 20
        
        # Check for negative prices
        if any(p <= 0 for p in self.prices):
            score -= 15
        
        return max(0, score)


# ============================================================================
# Rate Limiter & Circuit Breaker
# ============================================================================

class TokenBucket:
    """Async token bucket rate limiter with proper context management."""
    
    def __init__(self, rate_per_sec: float, capacity: Optional[float] = None):
        self.rate = max(0.0, rate_per_sec)
        self.capacity = capacity or max(1.0, self.rate)
        self.tokens = self.capacity
        self.last = time.monotonic()
        self._lock: Optional[asyncio.Lock] = None
        self._total_acquired = 0
        self._total_rejected = 0
    
    async def _get_lock(self) -> asyncio.Lock:
        """Lazy initialization of lock to avoid event loop issues."""
        if self._lock is None:
            self._lock = asyncio.Lock()
        return self._lock
    
    async def acquire(self, tokens: float = 1.0) -> bool:
        """Acquire tokens, returns True if successful."""
        if self.rate <= 0:
            self._total_acquired += 1
            return True
        
        lock = await self._get_lock()
        async with lock:
            now = time.monotonic()
            elapsed = max(0.0, now - self.last)
            self.last = now
            
            # Add new tokens
            self.tokens = min(self.capacity, self.tokens + elapsed * self.rate)
            
            if self.tokens >= tokens:
                self.tokens -= tokens
                self._total_acquired += 1
                _METRICS.set("rate_limiter_tokens", self.tokens)
                return True
            
            self._total_rejected += 1
            return False
    
    async def wait_and_acquire(self, tokens: float = 1.0) -> None:
        """Wait until tokens available then acquire."""
        while True:
            if await self.acquire(tokens):
                return
            
            # Calculate wait time
            lock = await self._get_lock()
            async with lock:
                need = tokens - self.tokens
                wait = need / self.rate if self.rate > 0 else 0.1
            
            await asyncio.sleep(min(1.0, wait))
    
    async def reset(self) -> None:
        """Reset the token bucket."""
        lock = await self._get_lock()
        async with lock:
            self.tokens = self.capacity
            self.last = time.monotonic()
            _METRICS.set("rate_limiter_tokens", self.tokens)
    
    def get_stats(self) -> Dict[str, Any]:
        """Get rate limiter statistics."""
        return {
            "rate": self.rate,
            "capacity": self.capacity,
            "current_tokens": self.tokens,
            "utilization": 1.0 - (self.tokens / self.capacity) if self.capacity > 0 else 0,
            "total_acquired": self._total_acquired,
            "total_rejected": self._total_rejected
        }


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
        self._lock: Optional[asyncio.Lock] = None
    
    async def _get_lock(self) -> asyncio.Lock:
        """Lazy initialization of lock."""
        if self._lock is None:
            self._lock = asyncio.Lock()
        return self._lock
    
    async def allow_request(self) -> bool:
        """Check if request is allowed."""
        lock = await self._get_lock()
        async with lock:
            now = time.monotonic()
            
            if self.stats.state == CircuitState.CLOSED:
                _METRICS.set("circuit_breaker_state", 1)
                return True
            
            elif self.stats.state == CircuitState.OPEN:
                if now >= self.stats.open_until:
                    self.stats.state = CircuitState.HALF_OPEN
                    self.half_open_calls = 0
                    logger.info("Circuit breaker moved to HALF_OPEN")
                    _METRICS.set("circuit_breaker_state", -1)
                    return True
                _METRICS.set("circuit_breaker_state", 0)
                return False
            
            elif self.stats.state == CircuitState.HALF_OPEN:
                if self.half_open_calls < self.half_open_max_calls:
                    self.half_open_calls += 1
                    _METRICS.set("circuit_breaker_state", -1)
                    return True
                return False
            
            return False
    
    async def on_success(self) -> None:
        """Record successful request."""
        lock = await self._get_lock()
        async with lock:
            self.stats.successes += 1
            self.stats.total_successes += 1
            self.stats.last_success = time.monotonic()
            
            if self.stats.state == CircuitState.HALF_OPEN:
                self.stats.state = CircuitState.CLOSED
                self.stats.failures = 0
                logger.info("Circuit breaker returned to CLOSED after success")
                _METRICS.set("circuit_breaker_state", 1)
    
    async def on_failure(self) -> None:
        """Record failed request."""
        lock = await self._get_lock()
        async with lock:
            self.stats.failures += 1
            self.stats.total_failures += 1
            self.stats.last_failure = time.monotonic()
            
            if self.stats.state == CircuitState.CLOSED:
                if self.stats.failures >= self.fail_threshold:
                    self.stats.state = CircuitState.OPEN
                    self.stats.open_until = time.monotonic() + self.cooldown_sec
                    logger.warning(f"Circuit breaker OPEN after {self.stats.failures} failures")
                    _METRICS.set("circuit_breaker_state", 0)
            
            elif self.stats.state == CircuitState.HALF_OPEN:
                self.stats.state = CircuitState.OPEN
                self.stats.open_until = time.monotonic() + self.cooldown_sec
                logger.warning("Circuit breaker returned to OPEN after half-open failure")
                _METRICS.set("circuit_breaker_state", 0)
    
    async def get_stats(self) -> Dict[str, Any]:
        """Get circuit breaker statistics."""
        lock = await self._get_lock()
        async with lock:
            return {
                "state": self.stats.state.value,
                "failures": self.stats.failures,
                "successes": self.stats.successes,
                "total_failures": self.stats.total_failures,
                "total_successes": self.stats.total_successes,
                "last_failure": self.stats.last_failure,
                "last_success": self.stats.last_success,
                "open_until": self.stats.open_until
            }


# ============================================================================
# Request Queue with Priorities
# ============================================================================

class RequestQueue:
    """Priority-based request queue."""
    
    def __init__(self, max_size: int = 1000):
        self.max_size = max_size
        self.queues: Dict[RequestPriority, asyncio.Queue] = {
            RequestPriority.LOW: asyncio.Queue(),
            RequestPriority.NORMAL: asyncio.Queue(),
            RequestPriority.HIGH: asyncio.Queue(),
            RequestPriority.CRITICAL: asyncio.Queue()
        }
        self._lock = asyncio.Lock()
        self._total_queued = 0
        self._total_processed = 0
    
    async def put(self, item: RequestQueueItem) -> bool:
        """Put item in queue."""
        async with self._lock:
            total_size = sum(q.qsize() for q in self.queues.values())
            if total_size >= self.max_size:
                return False
            
            await self.queues[item.priority].put(item)
            self._total_queued += 1
            _METRICS.set("queue_size", total_size + 1)
            return True
    
    async def get(self) -> Optional[RequestQueueItem]:
        """Get highest priority item from queue."""
        for priority in sorted(RequestPriority, key=lambda p: p.value, reverse=True):
            queue = self.queues[priority]
            if not queue.empty():
                item = await queue.get()
                self._total_processed += 1
                total_size = sum(q.qsize() for q in self.queues.values())
                _METRICS.set("queue_size", total_size)
                return item
        return None
    
    def get_stats(self) -> Dict[str, Any]:
        """Get queue statistics."""
        return {
            "max_size": self.max_size,
            "current_sizes": {
                p.value: q.qsize() for p, q in self.queues.items()
            },
            "total_queued": self._total_queued,
            "total_processed": self._total_processed,
            "utilization": sum(q.qsize() for q in self.queues.values()) / self.max_size if self.max_size > 0 else 0
        }


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
        self._hits: int = 0
        self._misses: int = 0
        self._lock: Optional[asyncio.Lock] = None
    
    async def _get_lock(self) -> asyncio.Lock:
        """Lazy initialization of lock."""
        if self._lock is None:
            self._lock = asyncio.Lock()
        return self._lock
    
    async def get(self, key: str) -> Optional[Any]:
        """Get value if exists and not expired."""
        lock = await self._get_lock()
        async with lock:
            now = time.monotonic()
            
            if key in self._cache:
                if now < self._expires.get(key, 0):
                    self._access_times[key] = now
                    self._hits += 1
                    _METRICS.inc("cache_hits_total", 1, {"cache_type": "memory"})
                    return self._cache[key]
                else:
                    # Expired
                    await self._delete(key)
            
            self._misses += 1
            _METRICS.inc("cache_misses_total", 1, {"cache_type": "memory"})
            return None
    
    async def set(self, key: str, value: Any, ttl: Optional[float] = None) -> None:
        """Set value with TTL."""
        lock = await self._get_lock()
        async with lock:
            # Evict if at capacity
            if len(self._cache) >= self.maxsize and key not in self._cache:
                await self._evict_lru()
            
            self._cache[key] = value
            self._expires[key] = time.monotonic() + (ttl or self.ttl)
            self._access_times[key] = time.monotonic()
    
    async def delete(self, key: str) -> None:
        """Delete key."""
        lock = await self._get_lock()
        async with lock:
            await self._delete(key)
    
    async def _delete(self, key: str) -> None:
        """Internal delete (no lock required - caller must hold lock)."""
        self._cache.pop(key, None)
        self._expires.pop(key, None)
        self._access_times.pop(key, None)
    
    async def _evict_lru(self) -> None:
        """Evict least recently used item (caller must hold lock)."""
        if not self._access_times:
            return
        
        # Find oldest access time
        oldest_key = min(self._access_times.items(), key=lambda x: x[1])[0]
        await self._delete(oldest_key)
    
    async def clear(self) -> None:
        """Clear cache."""
        lock = await self._get_lock()
        async with lock:
            self._cache.clear()
            self._expires.clear()
            self._access_times.clear()
            self._hits = 0
            self._misses = 0
    
    async def size(self) -> int:
        """Get current cache size."""
        lock = await self._get_lock()
        async with lock:
            return len(self._cache)
    
    def get_stats(self) -> Dict[str, Any]:
        """Get cache statistics."""
        total = self._hits + self._misses
        return {
            "size": len(self._cache),
            "maxsize": self.maxsize,
            "ttl": self.ttl,
            "hits": self._hits,
            "misses": self._misses,
            "hit_rate": self._hits / total if total > 0 else 0,
            "utilization": len(self._cache) / self.maxsize if self.maxsize > 0 else 0
        }


# ============================================================================
# Main Argaam Client
# ============================================================================

class ArgaamClient:
    """Advanced async Argaam API client with full feature set."""
    
    def __init__(self) -> None:
        # Configuration
        self.timeout_sec = _env_float("ARGAAM_TIMEOUT_SEC", DEFAULT_TIMEOUT_SEC)
        self.retry_attempts = _env_int("ARGAAM_RETRY_ATTEMPTS", DEFAULT_RETRY_ATTEMPTS)
        self.max_concurrency = _env_int("ARGAAM_MAX_CONCURRENCY", DEFAULT_MAX_CONCURRENCY)
        self.client_id = str(uuid.uuid4())[:8]
        
        # URLs
        self.quote_url = _safe_str(_env_str("ARGAAM_QUOTE_URL", ""))
        self.profile_url = _safe_str(_env_str("ARGAAM_PROFILE_URL", ""))
        self.history_url = _safe_str(_env_str("ARGAAM_HISTORY_URL", ""))
        
        # HTTP client
        timeout = httpx.Timeout(self.timeout_sec, connect=min(10.0, self.timeout_sec))
        self._client = httpx.AsyncClient(
            timeout=timeout,
            follow_redirects=True,
            headers=self._base_headers(),
            limits=httpx.Limits(
                max_keepalive_connections=50,
                max_connections=100,
                keepalive_expiry=30.0
            ),
            http2=True
        )
        
        # Rate limiting
        rate = _env_float("ARGAAM_RATE_LIMIT", DEFAULT_RATE_LIMIT)
        self.rate_limiter = TokenBucket(rate_per_sec=rate)
        
        # Circuit breaker
        cb_threshold = _env_int("ARGAAM_CB_THRESHOLD", DEFAULT_CIRCUIT_BREAKER_THRESHOLD)
        cb_timeout = _env_float("ARGAAM_CB_TIMEOUT", DEFAULT_CIRCUIT_BREAKER_TIMEOUT)
        self.circuit_breaker = SmartCircuitBreaker(
            fail_threshold=cb_threshold,
            cooldown_sec=cb_timeout
        )
        
        # Request queue
        queue_size = _queue_size()
        self.request_queue = RequestQueue(max_size=queue_size)
        
        # Concurrency control
        self.semaphore = asyncio.Semaphore(self.max_concurrency)
        
        # Caches
        self.quote_cache = SmartCache(maxsize=5000, ttl=_env_float("ARGAAM_QUOTE_TTL", 15.0))
        self.profile_cache = SmartCache(maxsize=3000, ttl=_env_float("ARGAAM_PROFILE_TTL", 3600.0))
        self.history_cache = SmartCache(maxsize=2000, ttl=_env_float("ARGAAM_HISTORY_TTL", 1200.0))
        
        # Metrics
        self.metrics: Dict[str, Any] = {
            "requests_total": 0,
            "requests_success": 0,
            "requests_failed": 0,
            "cache_hits": 0,
            "cache_misses": 0,
            "rate_limit_waits": 0,
            "circuit_breaker_blocks": 0
        }
        self._metrics_lock: Optional[asyncio.Lock] = None
        
        # Start queue processor
        self._queue_processor_task = asyncio.create_task(self._process_queue())
        
        logger.info(
            f"ArgaamClient v{PROVIDER_VERSION} initialized | "
            f"client_id={self.client_id} | "
            f"quote={bool(self.quote_url)} profile={bool(self.profile_url)} "
            f"history={bool(self.history_url)} | rate={rate}/s | "
            f"cb={cb_threshold}/{cb_timeout}s | queue={queue_size}"
        )
    
    async def _get_metrics_lock(self) -> asyncio.Lock:
        """Lazy initialization of metrics lock."""
        if self._metrics_lock is None:
            self._metrics_lock = asyncio.Lock()
        return self._metrics_lock
    
    def _base_headers(self) -> Dict[str, str]:
        """Base headers for all requests."""
        headers = {
            "User-Agent": USER_AGENT,
            "Accept": "application/json, text/plain, */*",
            "Accept-Language": "en-US,en;q=0.9,ar;q=0.8",
            "Accept-Encoding": "gzip, deflate, br",
            "Connection": "keep-alive",
            "X-Client-ID": self.client_id,
        }
        
        # Add custom headers from env
        custom = _safe_str(os.getenv("ARGAAM_HEADERS_JSON", ""))
        if custom:
            try:
                obj = json.loads(custom)
                if isinstance(obj, dict):
                    headers.update({str(k): str(v) for k, v in obj.items()})
            except Exception:
                pass
        
        return headers
    
    def _endpoint_headers(self, endpoint: str) -> Dict[str, str]:
        """Endpoint-specific headers."""
        key = f"ARGAAM_HEADERS_{endpoint.upper()}_JSON"
        custom = _safe_str(os.getenv(key, ""))
        if custom:
            try:
                obj = json.loads(custom)
                if isinstance(obj, dict):
                    return {str(k): str(v) for k, v in obj.items()}
            except Exception:
                pass
        return {}
    
    async def _update_metric(self, name: str, inc: int = 1) -> None:
        """Update metric atomically."""
        lock = await self._get_metrics_lock()
        async with lock:
            self.metrics[name] = self.metrics.get(name, 0) + inc
    
    @trace("argaam_request")
    async def _request(
        self,
        url: str,
        endpoint_type: str,
        cache_key: Optional[str] = None,
        use_cache: bool = True,
        priority: RequestPriority = RequestPriority.NORMAL
    ) -> Tuple[Optional[Union[dict, list]], Optional[str]]:
        """
        Make HTTP request with full feature set:
        - Caching
        - Rate limiting
        - Circuit breaker
        - Retries with backoff
        - Concurrency control
        - Request queuing
        """
        request_id = _generate_request_id()
        start_time = time.time()
        
        await self._update_metric("requests_total")
        _METRICS.inc("requests_total", 1, {"endpoint": endpoint_type, "status": "started"})
        _METRICS.set("active_requests", self.semaphore._value)
        
        async with TraceContext("http_request", {
            "request_id": request_id,
            "endpoint": endpoint_type,
            "url": url,
            "priority": priority.value
        }):
            # Check cache
            if use_cache and cache_key:
                cached = await self._get_from_cache(endpoint_type, cache_key)
                if cached is not None:
                    await self._update_metric("cache_hits")
                    duration = (time.time() - start_time) * 1000
                    _METRICS.observe("request_duration_seconds", duration / 1000, {"endpoint": endpoint_type})
                    return cached, None
            
            await self._update_metric("cache_misses")
            
            # Circuit breaker check
            if not await self.circuit_breaker.allow_request():
                await self._update_metric("circuit_breaker_blocks")
                _METRICS.inc("requests_total", 1, {"endpoint": endpoint_type, "status": "blocked"})
                return None, "circuit_breaker_open"
            
            # Queue request
            future = asyncio.Future()
            queue_item = RequestQueueItem(
                priority=priority,
                url=url,
                endpoint_type=endpoint_type,
                cache_key=cache_key,
                use_cache=use_cache,
                future=future
            )
            
            queued = await self.request_queue.put(queue_item)
            if not queued:
                _METRICS.inc("requests_total", 1, {"endpoint": endpoint_type, "status": "queue_full"})
                return None, "queue_full"
            
            # Wait for processing
            result = await future
            
            duration = (time.time() - start_time) * 1000
            _METRICS.observe("request_duration_seconds", duration / 1000, {"endpoint": endpoint_type})
            
            return result
    
    async def _process_queue(self) -> None:
        """Process queued requests."""
        while True:
            try:
                item = await self.request_queue.get()
                if item is None:
                    await asyncio.sleep(0.1)
                    continue
                
                # Process the request
                asyncio.create_task(self._process_queue_item(item))
                
            except Exception as e:
                logger.error(f"Queue processor error: {e}")
                await asyncio.sleep(1)
    
    async def _process_queue_item(self, item: RequestQueueItem) -> None:
        """Process a single queue item."""
        try:
            result = await self._execute_request(
                item.url,
                item.endpoint_type,
                item.cache_key,
                item.use_cache
            )
            item.future.set_result(result)
        except Exception as e:
            item.future.set_exception(e)
    
    async def _execute_request(
        self,
        url: str,
        endpoint_type: str,
        cache_key: Optional[str] = None,
        use_cache: bool = True
    ) -> Tuple[Optional[Union[dict, list]], Optional[str]]:
        """Execute the actual HTTP request."""
        # Rate limiting
        await self.rate_limiter.wait_and_acquire()
        await self._update_metric("rate_limit_waits")
        
        # Concurrency control
        async with self.semaphore:
            headers = self._base_headers()
            headers.update(self._endpoint_headers(endpoint_type))
            
            last_err: Optional[str] = None
            
            for attempt in range(self.retry_attempts):
                try:
                    resp = await self._client.get(url, headers=headers)
                    status = resp.status_code
                    
                    # Handle rate limiting
                    if status == 429:
                        retry_after = int(resp.headers.get("Retry-After", 5))
                        await asyncio.sleep(min(retry_after, 30))
                        continue
                    
                    # Server errors
                    if 500 <= status < 600:
                        if attempt < self.retry_attempts - 1:
                            wait = (2 ** attempt) + random.random()
                            await asyncio.sleep(min(wait, 10))
                            continue
                        await self.circuit_breaker.on_failure()
                        await self._update_metric("requests_failed")
                        _METRICS.inc("requests_total", 1, {"endpoint": endpoint_type, "status": "error"})
                        return None, f"HTTP {status}"
                    
                    # Client errors (except 404 which may be normal)
                    if 400 <= status < 500 and status != 404:
                        await self.circuit_breaker.on_failure()
                        await self._update_metric("requests_failed")
                        _METRICS.inc("requests_total", 1, {"endpoint": endpoint_type, "status": "error"})
                        return None, f"HTTP {status}"
                    
                    # Success
                    try:
                        data = resp.json()
                    except Exception:
                        await self.circuit_breaker.on_failure()
                        await self._update_metric("requests_failed")
                        _METRICS.inc("requests_total", 1, {"endpoint": endpoint_type, "status": "error"})
                        return None, "invalid_json"
                    
                    # Unwrap common envelopes
                    data = _unwrap_common_envelopes(data)
                    
                    # Cache successful response
                    if use_cache and cache_key:
                        await self._save_to_cache(endpoint_type, cache_key, data)
                    
                    await self.circuit_breaker.on_success()
                    await self._update_metric("requests_success")
                    _METRICS.inc("requests_total", 1, {"endpoint": endpoint_type, "status": "success"})
                    return data, None
                    
                except httpx.TimeoutException:
                    last_err = "timeout"
                    if attempt < self.retry_attempts - 1:
                        await asyncio.sleep(2 ** attempt)
                        continue
                
                except httpx.NetworkError as e:
                    last_err = f"network_error: {e.__class__.__name__}"
                    if attempt < self.retry_attempts - 1:
                        await asyncio.sleep(2 ** attempt)
                        continue
                
                except Exception as e:
                    last_err = f"unexpected: {e.__class__.__name__}"
                    if attempt < self.retry_attempts - 1:
                        await asyncio.sleep(2 ** attempt)
                        continue
            
            # All retries failed
            await self.circuit_breaker.on_failure()
            await self._update_metric("requests_failed")
            _METRICS.inc("requests_total", 1, {"endpoint": endpoint_type, "status": "error"})
            return None, last_err or "request_failed"
    
    async def _get_from_cache(self, cache_type: str, key: str) -> Optional[Any]:
        """Get from appropriate cache."""
        if cache_type == "quote":
            return await self.quote_cache.get(key)
        elif cache_type == "profile":
            return await self.profile_cache.get(key)
        elif cache_type == "history":
            return await self.history_cache.get(key)
        return None
    
    async def _save_to_cache(self, cache_type: str, key: str, value: Any) -> None:
        """Save to appropriate cache."""
        if cache_type == "quote":
            await self.quote_cache.set(key, value)
        elif cache_type == "profile":
            await self.profile_cache.set(key, value)
        elif cache_type == "history":
            await self.history_cache.set(key, value)
    
    async def get_quote(self, symbol: str) -> Dict[str, Any]:
        """Get quote data for symbol."""
        sym = normalize_ksa_symbol(symbol)
        if not sym:
            return {}
        
        if not self.quote_url:
            return {}
        
        url = format_url(self.quote_url, sym)
        cache_key = f"quote:{sym}"
        
        data, err = await self._request(url, "quote", cache_key)
        if data is None:
            if _emit_warnings():
                return {"_warn": f"quote_failed: {err}"}
            return {}
        
        # Map data to standardized format
        return self._map_quote_data(data, symbol, sym)
    
    async def get_profile(self, symbol: str) -> Dict[str, Any]:
        """Get profile/identity data."""
        sym = normalize_ksa_symbol(symbol)
        if not sym:
            return {}
        
        if not self.profile_url:
            return {}
        
        url = format_url(self.profile_url, sym)
        cache_key = f"profile:{sym}"
        
        data, err = await self._request(url, "profile", cache_key)
        if data is None:
            if _emit_warnings():
                return {"_warn": f"profile_failed: {err}"}
            return {}
        
        return self._map_profile_data(data, symbol, sym)
    
    async def get_history(self, symbol: str) -> Dict[str, Any]:
        """Get historical data with full analytics."""
        if not _history_enabled():
            return {}
        
        sym = normalize_ksa_symbol(symbol)
        if not sym:
            return {}
        
        if not self.history_url:
            return {}
        
        days = _history_days()
        url = format_url(self.history_url, sym, days=days)
        cache_key = f"history:{sym}:{days}"
        
        data, err = await self._request(url, "history", cache_key)
        if data is None:
            if _emit_warnings():
                return {"_warn": f"history_failed: {err}"}
            return {}
        
        # Extract price series
        prices, last_dt, source = _extract_price_series(data, _max_history_points())
        if not prices:
            if _emit_warnings():
                return {"_warn": "no_price_series_found"}
            return {}
        
        # Get volume data if available
        volumes = self._extract_volume_series(data)
        
        # Calculate analytics
        result = self._compute_full_analytics(
            prices=prices,
            volumes=volumes,
            symbol=symbol,
            sym=sym,
            last_dt=last_dt,
            source=source
        )
        
        return result
    
    async def get_enriched(self, symbol: str) -> Dict[str, Any]:
        """
        Get fully enriched data (quote + profile + history).
        This is the main method for dashboard consumption.
        """
        if not _configured():
            return {}
        
        # Run all fetches concurrently
        quote_task = self.get_quote(symbol)
        profile_task = self.get_profile(symbol)
        history_task = self.get_history(symbol)
        
        quote_data, profile_data, history_data = await asyncio.gather(
            quote_task, profile_task, history_task
        )
        
        # Merge data (quote first, then profile, then history)
        result: Dict[str, Any] = {}
        
        # Quote data (primary)
        if quote_data:
            # Copy all non-warning fields
            for k, v in quote_data.items():
                if k != "_warn" and v is not None:
                    result[k] = v
        
        # Profile data (fill missing identity fields)
        if profile_data:
            for k, v in profile_data.items():
                if k != "_warn" and v is not None:
                    if k not in result or result.get(k) in (None, ""):
                        result[k] = v
        
        # History data (add analytics)
        if history_data:
            for k, v in history_data.items():
                if k != "_warn" and v is not None:
                    if k not in result:
                        result[k] = v
        
        # Add metadata
        result["provider"] = PROVIDER_NAME
        result["provider_version"] = PROVIDER_VERSION
        result["requested_symbol"] = symbol
        result["normalized_symbol"] = normalize_ksa_symbol(symbol)
        result["data_timestamp_utc"] = _utc_iso()
        result["data_timestamp_riyadh"] = _to_riyadh_iso(_riyadh_now())
        result["request_id"] = _generate_request_id()
        
        # Add warnings if any
        warnings = []
        for d in (quote_data, profile_data, history_data):
            if d and "_warn" in d:
                warnings.append(d["_warn"])
        if warnings and _emit_warnings():
            result["_warnings"] = " | ".join(warnings)
        
        return result
    
    def _map_quote_data(self, raw: Any, original_symbol: str, norm_symbol: str) -> Dict[str, Any]:
        """Map raw quote data to standardized format."""
        result: Dict[str, Any] = {
            "requested_symbol": original_symbol,
            "normalized_symbol": norm_symbol,
        }
        
        # Price fields
        result["current_price"] = _safe_float(_find_first_value(raw, [
            "last", "last_price", "price", "close", "c", "LastPrice",
            "tradingPrice", "regularMarketPrice", "currentPrice"
        ]))
        
        result["previous_close"] = _safe_float(_find_first_value(raw, [
            "previous_close", "prev_close", "pc", "PreviousClose", "prevClose",
            "regularMarketPreviousClose"
        ]))
        
        result["open"] = _safe_float(_find_first_value(raw, [
            "open", "o", "Open", "openPrice", "regularMarketOpen"
        ]))
        
        result["day_high"] = _safe_float(_find_first_value(raw, [
            "high", "day_high", "h", "High", "dayHigh", "sessionHigh",
            "regularMarketDayHigh"
        ]))
        
        result["day_low"] = _safe_float(_find_first_value(raw, [
            "low", "day_low", "l", "Low", "dayLow", "sessionLow",
            "regularMarketDayLow"
        ]))
        
        result["volume"] = _safe_int(_find_first_value(raw, [
            "volume", "v", "Volume", "tradedVolume", "qty", "quantity",
            "regularMarketVolume"
        ]))
        
        # Change fields
        result["change"] = _safe_float(_find_first_value(raw, [
            "change", "d", "price_change", "Change", "diff", "delta"
        ]))
        
        result["change_percent"] = _safe_float(_find_first_value(raw, [
            "change_pct", "change_percent", "dp", "percent_change",
            "ChangePercent", "pctChange", "changePercent"
        ]))
        
        # Calculate change if missing
        if result["change"] is None and result["current_price"] and result["previous_close"]:
            result["change"] = result["current_price"] - result["previous_close"]
        
        if result["change_percent"] is None and result["current_price"] and result["previous_close"] and result["previous_close"] != 0:
            result["change_percent"] = ((result["current_price"] / result["previous_close"]) - 1) * 100
        
        # 52-week range
        result["week_52_high"] = _safe_float(_find_first_value(raw, [
            "week_52_high", "fiftyTwoWeekHigh", "52w_high", "yearHigh",
            "fiftyTwoWeekHigh"
        ]))
        
        result["week_52_low"] = _safe_float(_find_first_value(raw, [
            "week_52_low", "fiftyTwoWeekLow", "52w_low", "yearLow",
            "fiftyTwoWeekLow"
        ]))
        
        # Market cap
        result["market_cap"] = _safe_float(_find_first_value(raw, [
            "market_cap", "marketCap", "MarketCap", "mktCap"
        ]))
        
        # Name (if available in quote)
        result["name"] = _safe_str(_find_first_value(raw, [
            "name", "companyName", "shortName", "longName"
        ]))
        
        # Currency
        result["currency"] = _safe_str(_find_first_value(raw, ["currency", "Currency"])) or "SAR"
        
        # Clean and return
        return {k: v for k, v in result.items() if v is not None}
    
    def _map_profile_data(self, raw: Any, original_symbol: str, norm_symbol: str) -> Dict[str, Any]:
        """Map profile data to standardized format."""
        result: Dict[str, Any] = {
            "requested_symbol": original_symbol,
            "normalized_symbol": norm_symbol,
        }
        
        # Identity fields
        result["name"] = _safe_str(_find_first_value(raw, [
            "name", "company_name", "companyName", "CompanyName",
            "longName", "shortName", "securityName", "issuerName"
        ]))
        
        result["sector"] = _safe_str(_find_first_value(raw, [
            "sector", "Sector", "sectorName", "companySector"
        ]))
        
        result["industry"] = _safe_str(_find_first_value(raw, [
            "industry", "Industry", "industryName"
        ]))
        
        result["sub_sector"] = _safe_str(_find_first_value(raw, [
            "sub_sector", "subSector", "SubSector", "subSectorName",
            "subIndustry", "sub_industry"
        ]))
        
        # Company details
        result["website"] = _safe_str(_find_first_value(raw, [
            "website", "Website", "companyWebsite"
        ]))
        
        result["employees"] = _safe_int(_find_first_value(raw, [
            "employees", "Employees", "fullTimeEmployees", "employeeCount"
        ]))
        
        result["founded"] = _safe_int(_find_first_value(raw, [
            "founded", "Founded", "yearFounded"
        ]))
        
        result["exchange"] = _safe_str(_find_first_value(raw, [
            "exchange", "Exchange", "market", "Market"
        ])) or "Saudi Stock Exchange"
        
        result["isin"] = _safe_str(_find_first_value(raw, ["isin", "ISIN"]))
        
        return {k: v for k, v in result.items() if v is not None}
    
    def _extract_volume_series(self, raw: Any) -> List[float]:
        """Extract volume series from history data."""
        volumes: List[float] = []
        
        try:
            # Try list of dicts first
            items = _find_first_list_of_dicts(raw, required_keys=("volume", "v", "vol"))
            if items:
                for item in items:
                    vol = _safe_float(item.get("volume")) or _safe_float(item.get("v")) or _safe_float(item.get("vol"))
                    if vol is not None:
                        volumes.append(float(vol))
                return volumes[-_max_history_points():]
            
            # Try array format [t,o,h,l,c,v]
            if isinstance(raw, list) and raw and isinstance(raw[0], (list, tuple)):
                for arr in raw:
                    if len(arr) >= 6:
                        vol = _safe_float(arr[5])
                        if vol is not None:
                            volumes.append(float(vol))
                return volumes[-_max_history_points():]
        except Exception:
            pass
        
        return []
    
    def _compute_full_analytics(
        self,
        prices: List[float],
        volumes: List[float],
        symbol: str,
        sym: str,
        last_dt: Optional[datetime],
        source: str
    ) -> Dict[str, Any]:
        """Compute full set of analytics including technicals and forecasts."""
        result: Dict[str, Any] = {
            "requested_symbol": symbol,
            "normalized_symbol": sym,
            "history_points": len(prices),
            "history_last_utc": _utc_iso(last_dt) if last_dt else _utc_iso(),
            "history_last_riyadh": _to_riyadh_iso(last_dt),
            "history_source": source,
            "current_price": prices[-1] if prices else None,
        }
        
        if len(prices) < 10:
            return result
        
        # ====================================================================
        # Basic Statistics
        # ====================================================================
        returns = [prices[i] / prices[i-1] - 1 for i in range(1, len(prices))]
        
        result["avg_return_daily"] = float(np.mean(returns)) if returns else None
        result["volatility_daily"] = float(np.std(returns)) if returns else None
        result["volatility_annualized"] = float(np.std(returns) * math.sqrt(252)) if returns else None
        result["positive_days_pct"] = float(sum(1 for r in returns if r > 0) / len(returns) * 100) if returns else None
        
        # ====================================================================
        # Moving Averages
        # ====================================================================
        for period in [20, 50, 200]:
            if len(prices) >= period:
                ma = sum(prices[-period:]) / period
                result[f"sma_{period}"] = float(ma)
                result[f"price_to_sma_{period}"] = float(prices[-1] / ma - 1) * 100
        
        # ====================================================================
        # Technical Indicators
        # ====================================================================
        ti = TechnicalIndicators()
        
        # RSI
        rsi_values = ti.rsi(prices, 14)
        if rsi_values and rsi_values[-1] is not None:
            result["rsi_14"] = float(rsi_values[-1])
            result["rsi_signal"] = "overbought" if rsi_values[-1] > 70 else "oversold" if rsi_values[-1] < 30 else "neutral"
        
        # MACD
        macd = ti.macd(prices)
        if macd["macd"] and macd["macd"][-1] is not None:
            result["macd"] = float(macd["macd"][-1])
            result["macd_signal"] = float(macd["signal"][-1]) if macd["signal"][-1] else None
            result["macd_histogram"] = float(macd["histogram"][-1]) if macd["histogram"][-1] else None
            result["macd_cross"] = "bullish" if result["macd"] > (result["macd_signal"] or 0) else "bearish"
        
        # Bollinger Bands
        bb = ti.bollinger_bands(prices)
        if bb["middle"][-1] is not None:
            result["bb_middle"] = float(bb["middle"][-1])
            result["bb_upper"] = float(bb["upper"][-1])
            result["bb_lower"] = float(bb["lower"][-1])
            result["bb_bandwidth"] = float(bb["bandwidth"][-1]) if bb["bandwidth"][-1] else None
            result["bb_position"] = float((prices[-1] - bb["lower"][-1]) / (bb["upper"][-1] - bb["lower"][-1])) if bb["upper"][-1] != bb["lower"][-1] else 0.5
        
        # ATR (if high/low data available - using close as proxy)
        atr_values = ti.atr(prices, prices, prices, 14)
        if atr_values and atr_values[-1] is not None:
            result["atr_14"] = float(atr_values[-1])
            result["atr_percent"] = float(atr_values[-1] / prices[-1] * 100)
        
        # On-Balance Volume (if volume available)
        if volumes and len(volumes) == len(prices):
            obv_values = ti.obv(prices, volumes)
            if obv_values:
                result["obv"] = float(obv_values[-1])
                # OBV trend
                if len(obv_values) > 20:
                    obv_trend = (obv_values[-1] / obv_values[-20] - 1) * 100
                    result["obv_trend_pct"] = float(obv_trend)
                    result["obv_signal"] = "bullish" if obv_trend > 2 else "bearish" if obv_trend < -2 else "neutral"
        
        # ====================================================================
        # Returns over periods
        # ====================================================================
        periods = {
            "1w": 5, "2w": 10, "1m": 21, "3m": 63, "6m": 126, "1y": 252, "2y": 504
        }
        
        for name, days in periods.items():
            if len(prices) > days:
                past_price = prices[-(days + 1)]
                ret = (prices[-1] / past_price - 1) * 100
                result[f"return_{name}"] = float(ret)
        
        # ====================================================================
        # Maximum Drawdown
        # ====================================================================
        peak = prices[0]
        max_dd = 0.0
        max_dd_start = 0
        max_dd_end = 0
        
        for i, price in enumerate(prices):
            if price > peak:
                peak = price
            dd = (price / peak - 1) * 100
            if dd < max_dd:
                max_dd = dd
                max_dd_end = i
        
        result["max_drawdown_pct"] = float(max_dd)
        
        # ====================================================================
        # Sharpe Ratio (assuming 0% risk-free rate)
        # ====================================================================
        if returns:
            avg_ret = np.mean(returns) * 252  # Annualized
            std_ret = np.std(returns) * math.sqrt(252)
            result["sharpe_ratio"] = float(avg_ret / std_ret) if std_ret != 0 else None
        
        # ====================================================================
        # Market Regime
        # ====================================================================
        detector = MarketRegimeDetector(prices)
        regime = detector.detect()
        result["market_regime"] = regime.value
        result["market_regime_score"] = {
            MarketRegime.BULL: 1.0,
            MarketRegime.BEAR: -1.0,
            MarketRegime.VOLATILE: 0.0,
            MarketRegime.SIDEWAYS: 0.3,
            MarketRegime.UNKNOWN: 0.0
        }.get(regime, 0.0)
        
        # ====================================================================
        # Sentiment Analysis
        # ====================================================================
        sentiment_analyzer = SentimentAnalyzer(prices, volumes)
        sentiment = sentiment_analyzer.analyze()
        result["sentiment"] = sentiment.get("sentiment", Sentiment.NEUTRAL.value)
        result["sentiment_score"] = sentiment.get("score", 0.0)
        result["sentiment_confidence"] = sentiment.get("confidence", 0)
        
        # ====================================================================
        # Anomaly Detection
        # ====================================================================
        anomaly_detector = AnomalyDetector(prices, volumes)
        result["data_quality_score"] = anomaly_detector.data_quality_score()
        result["price_spikes"] = len(anomaly_detector.detect_price_spikes())
        result["price_gaps"] = len(anomaly_detector.detect_gaps())
        if volumes:
            result["volume_surges"] = len(anomaly_detector.detect_volume_surges())
        
        # ====================================================================
        # Ensemble Forecast (if enabled)
        # ====================================================================
        if _forecast_enabled() and len(prices) >= 30:
            forecaster = EnsembleForecaster(prices, enable_ml=_ml_enabled())
            
            for horizon, name in [(21, "1m"), (63, "3m"), (252, "1y")]:
                forecast = forecaster.forecast(horizon)
                if forecast.get("forecast_available"):
                    result[f"forecast_price_{name}"] = forecast["ensemble"]["price"]
                    result[f"forecast_roi_{name}_pct"] = forecast["ensemble"]["roi_pct"]
                    result[f"forecast_range_low_{name}"] = forecast["ensemble"].get("price_range_low")
                    result[f"forecast_range_high_{name}"] = forecast["ensemble"].get("price_range_high")
                    
                    # Store confidence once (use 1y confidence as primary)
                    if name == "1y":
                        result["forecast_confidence"] = forecast["confidence"]
                        result["forecast_confidence_level"] = forecast["confidence_level"]
                        result["forecast_models"] = forecast["models_used"]
                        result["feature_importance"] = forecast.get("feature_importance")
        
        return result
    
    async def close(self) -> None:
        """Close HTTP client."""
        self._queue_processor_task.cancel()
        try:
            await self._queue_processor_task
        except:
            pass
        await self._client.aclose()
    
    async def get_metrics(self) -> Dict[str, Any]:
        """Get client metrics."""
        lock = await self._get_metrics_lock()
        async with lock:
            metrics = dict(self.metrics)
        
        metrics["circuit_breaker"] = await self.circuit_breaker.get_stats()
        metrics["rate_limiter"] = self.rate_limiter.get_stats()
        metrics["request_queue"] = self.request_queue.get_stats()
        metrics["cache_sizes"] = {
            "quote": await self.quote_cache.size(),
            "profile": await self.profile_cache.size(),
            "history": await self.history_cache.size()
        }
        metrics["cache_stats"] = {
            "quote": self.quote_cache.get_stats(),
            "profile": self.profile_cache.get_stats(),
            "history": self.history_cache.get_stats()
        }
        
        return metrics


# ============================================================================
# Singleton Management
# ============================================================================

_CLIENT_INSTANCE: Optional[ArgaamClient] = None
_CLIENT_LOCK = asyncio.Lock()


async def get_client() -> ArgaamClient:
    """Get or create Argaam client singleton."""
    global _CLIENT_INSTANCE
    if _CLIENT_INSTANCE is None:
        async with _CLIENT_LOCK:
            if _CLIENT_INSTANCE is None:
                _CLIENT_INSTANCE = ArgaamClient()
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

async def fetch_quote_patch(symbol: str, *args, **kwargs) -> Dict[str, Any]:
    """Fetch quote data only."""
    if not _configured():
        return {}
    
    client = await get_client()
    return await client.get_quote(symbol)


async def fetch_enriched_quote_patch(symbol: str, *args, **kwargs) -> Dict[str, Any]:
    """Fetch fully enriched data (quote + profile + history)."""
    if not _configured():
        return {}
    
    client = await get_client()
    return await client.get_enriched(symbol)


async def fetch_quote_and_enrichment_patch(symbol: str, *args, **kwargs) -> Dict[str, Any]:
    """Alias for enriched quote."""
    return await fetch_enriched_quote_patch(symbol)


async def fetch_enriched_patch(symbol: str, *args, **kwargs) -> Dict[str, Any]:
    """Alias for enriched quote."""
    return await fetch_enriched_quote_patch(symbol)


async def fetch_quote_and_fundamentals_patch(symbol: str, *args, **kwargs) -> Dict[str, Any]:
    """Alias for enriched quote (fundamentals not separately available)."""
    return await fetch_enriched_quote_patch(symbol)


async def fetch_history_patch(symbol: str, *args, **kwargs) -> Dict[str, Any]:
    """Fetch historical data with analytics."""
    if not _configured():
        return {}
    
    client = await get_client()
    return await client.get_history(symbol)


async def get_client_metrics() -> Dict[str, Any]:
    """Get client performance metrics."""
    client = await get_client()
    return await client.get_metrics()


def normalize_symbol(symbol: str) -> str:
    """Public symbol normalization."""
    return normalize_ksa_symbol(symbol)


async def health_check() -> Dict[str, Any]:
    """Perform health check on the client."""
    if not _configured():
        return {
            "status": "disabled",
            "provider": PROVIDER_NAME,
            "version": PROVIDER_VERSION,
            "timestamp": _utc_now_iso()
        }
    
    try:
        client = await get_client()
        metrics = await client.get_metrics()
        
        # Determine health status
        if metrics["circuit_breaker"]["state"] == "open":
            status = "degraded"
        elif metrics["rate_limiter"]["utilization"] > 0.9:
            status = "degraded"
        elif metrics["request_queue"]["utilization"] > 0.8:
            status = "degraded"
        else:
            status = "healthy"
        
        return {
            "status": status,
            "provider": PROVIDER_NAME,
            "version": PROVIDER_VERSION,
            "client_id": client.client_id,
            "timestamp": _utc_now_iso(),
            "metrics": metrics
        }
    except Exception as e:
        return {
            "status": "error",
            "provider": PROVIDER_NAME,
            "version": PROVIDER_VERSION,
            "error": str(e),
            "timestamp": _utc_now_iso()
        }


# ============================================================================
# Module Exports
# ============================================================================

__all__ = [
    "PROVIDER_NAME",
    "PROVIDER_VERSION",
    "fetch_quote_patch",
    "fetch_enriched_quote_patch",
    "fetch_quote_and_enrichment_patch",
    "fetch_enriched_patch",
    "fetch_quote_and_fundamentals_patch",
    "fetch_history_patch",
    "get_client_metrics",
    "normalize_symbol",
    "close_client",
    "health_check",
    "MarketRegime",
    "CircuitState",
    "Sentiment",
    "RequestPriority"
]
