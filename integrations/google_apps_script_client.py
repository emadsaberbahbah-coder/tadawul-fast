#!/usr/bin/env python3
"""
integrations/google_apps_script_client.py
================================================================================
GOOGLE APPS SCRIPT CLIENT FOR TADAWUL FAST BRIDGE -- v6.1.0
(Emad Bahbah -- Enterprise Integration Architecture)

INSTITUTIONAL GRADE * ZERO DOWNTIME * COMPLETE ALIGNMENT

v6.1.0 changes (what moved from v6.0.0)
---------------------------------------
- CRITICAL FIX: ``concurrent.futures`` is now imported. v6.0.0 used
  ``concurrent.futures.ThreadPoolExecutor`` inside
  ``AsyncGoogleAppsScriptClient._get_executor()`` without importing the
  module. Because class-body annotations are lazy under
  ``from __future__ import annotations``, import-time was fine, but the
  first call to any async method raised
  ``NameError: name 'concurrent' is not defined``. v6.1.0 adds the
  import and also types the attribute correctly.

- FIX: ``AsyncGenerator`` is now imported from ``typing``. v6.0.0
  referenced it in the ``@asynccontextmanager`` return annotation
  without importing. Under ``from __future__ import annotations`` this
  was a lazy string so no immediate failure, but
  ``typing.get_type_hints()`` and every static type-checker reported it
  as an unresolved name.

- FIX: ``TraceContext`` no longer misuses
  ``tracer.start_as_current_span()`` as if it returned a Span. v6.0.0
  stored the returned *context manager* in ``self.span`` and then
  called Span methods (``.set_attributes``, ``.record_exception``,
  ``.set_status``) on it -- every call silently failed through the
  catch-all ``except Exception: pass``. v6.1.0 enters the CM to obtain
  the real span and stores both (``_cm`` + ``span``) so attributes and
  exception recording actually work when OTEL is enabled.

- FIX: schema drift -- removed the ``KSA_TADAWUL`` entry from
  ``PAGE_SPECS``. The authoritative ``schema_registry`` does not
  declare a ``KSA_TADAWUL`` sheet; canonical sheets are
  ``Market_Leaders``, ``Global_Markets``, ``Commodities_FX``,
  ``Mutual_Funds``, ``My_Portfolio``, ``Insights_Analysis``,
  ``Top_10_Investments``, and ``Data_Dictionary``. The
  ``KSA``/``TADAWUL``/``SAUDI`` aliases now redirect to the canonical
  ``MARKET_LEADERS`` (the sheet that actually carries Tadawul
  listings) rather than to a phantom page.

- CLEANUP: removed redundant ``import json`` in the orjson fallback
  branch (``json`` is already imported at the top of the module).
  Removed unused ``cast`` and ``Mapping`` imports.

- Bumped ``CLIENT_VERSION`` to ``"6.1.0"``.

Preserved
---------
- All public symbols in ``__all__``.
- ``GoogleAppsScriptClient`` behavior: circuit breaker, rate limiter,
  retry/backoff, payload shape, telemetry, Prometheus metrics.
- ``AppsScriptConfig.from_env`` env var names unchanged.
- ``AppsScriptResult`` dataclass shape.
- ``sync_page_quotes`` payload structure.
================================================================================
"""

from __future__ import annotations

import asyncio
import concurrent.futures
import hashlib
import json
import logging
import os
import random
import ssl
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from functools import lru_cache
from typing import (
    Any,
    AsyncGenerator,
    Callable,
    Dict,
    List,
    Optional,
    Sequence,
    Set,
    Tuple,
    Union,
)

# =============================================================================
# Optional Dependencies (Keep PROD Safe)
# =============================================================================

try:
    import orjson  # type: ignore

    def _json_dumps(value: Any, default: Any = None) -> str:
        return orjson.dumps(value, default=default).decode("utf-8")

    def _json_loads(data: Union[str, bytes]) -> Any:
        if isinstance(data, str):
            data = data.encode("utf-8")
        return orjson.loads(data)

    _HAS_ORJSON = True
except ImportError:
    def _json_dumps(value: Any, default: Any = None) -> str:
        return json.dumps(value, default=default, ensure_ascii=False)

    def _json_loads(data: Union[str, bytes]) -> Any:
        if isinstance(data, bytes):
            data = data.decode("utf-8", errors="replace")
        return json.loads(data)

    _HAS_ORJSON = False

# ---------------------------------------------------------------------------
# Prometheus Metrics (Optional)
# ---------------------------------------------------------------------------

try:
    from prometheus_client import Counter, Histogram  # type: ignore
    _PROMETHEUS_AVAILABLE = True
except ImportError:
    _PROMETHEUS_AVAILABLE = False

    class _DummyMetric:
        def labels(self, *args: Any, **kwargs: Any) -> "_DummyMetric":
            return self

        def inc(self, *args: Any, **kwargs: Any) -> None:
            pass

        def observe(self, *args: Any, **kwargs: Any) -> None:
            pass

    Counter = _DummyMetric  # type: ignore
    Histogram = _DummyMetric  # type: ignore

# ---------------------------------------------------------------------------
# OpenTelemetry Tracing (Optional)
# ---------------------------------------------------------------------------

try:
    from opentelemetry import trace  # type: ignore
    from opentelemetry.trace import Status, StatusCode  # type: ignore
    _OTEL_AVAILABLE = True
except ImportError:
    trace = None  # type: ignore
    Status = None  # type: ignore
    StatusCode = None  # type: ignore
    _OTEL_AVAILABLE = False

_TRACING_ENABLED = os.getenv("CORE_TRACING_ENABLED", "").strip().lower() in {
    "1", "true", "yes", "y", "on"
}

# =============================================================================
# Core Module Imports (Optional Fallbacks)
# =============================================================================

try:
    from core.schemas import (
        MarketType,
        AssetClass,
        normalize_sheet_name,
        get_headers_for_sheet,
        VNEXT_SCHEMAS,
    )
    from core.symbols.normalize import normalize_symbol as core_normalize_symbol
    _HAS_CORE = True
except ImportError:
    _HAS_CORE = False

    class MarketType(str, Enum):  # type: ignore[no-redef]
        KSA = "KSA"
        GLOBAL = "GLOBAL"
        MIXED = "MIXED"

    class AssetClass(str, Enum):  # type: ignore[no-redef]
        EQUITY = "EQUITY"
        ETF = "ETF"
        COMMODITY = "COMMODITY"
        UNKNOWN = "UNKNOWN"

    def normalize_sheet_name(name: str) -> str:  # type: ignore[no-redef]
        return name.strip().upper().replace(" ", "_") if name else ""

    def get_headers_for_sheet(sheet: str) -> List[str]:  # type: ignore[no-redef]
        return []

    VNEXT_SCHEMAS: Dict[str, Any] = {}  # type: ignore[no-redef]

# =============================================================================
# Logging Setup
# =============================================================================

logger = logging.getLogger(__name__)
logger.addHandler(logging.NullHandler())

# =============================================================================
# Version
# =============================================================================

CLIENT_VERSION = "6.1.0"

# =============================================================================
# Constants
# =============================================================================

_TRUTHY = {"1", "true", "yes", "y", "on", "t", "enabled"}

# =============================================================================
# Enums
# =============================================================================

class RequestMethod(str, Enum):
    """HTTP methods supported."""
    GET = "GET"
    POST = "POST"
    PUT = "PUT"
    DELETE = "DELETE"


class TokenTransport(str, Enum):
    """Token transport mechanisms."""
    QUERY = "query"
    HEADER = "header"
    BODY = "body"


class CircuitState(str, Enum):
    """Circuit breaker states."""
    CLOSED = "closed"
    OPEN = "open"
    HALF_OPEN = "half_open"


class FailureType(str, Enum):
    """Types of failures for circuit breaker."""
    NETWORK = "network"
    TIMEOUT = "timeout"
    AUTH = "auth"
    SERVER = "server"
    CLIENT = "client"
    PARSING = "parsing"
    UNKNOWN = "unknown"


class MarketCategory(str, Enum):
    """Market categories for routing."""
    KSA = "ksa"
    GLOBAL = "global"
    MIXED = "mixed"


# =============================================================================
# Custom Exceptions
# =============================================================================

class AppsScriptClientError(Exception):
    """Base exception for Apps Script client."""
    pass


class ConfigurationError(AppsScriptClientError):
    """Raised when configuration is invalid."""
    pass


class CircuitBreakerOpenError(AppsScriptClientError):
    """Raised when circuit breaker is open."""
    pass


class RateLimitError(AppsScriptClientError):
    """Raised when rate limit is exceeded."""
    pass


class RequestError(AppsScriptClientError):
    """Raised when request fails."""
    pass


# =============================================================================
# Configuration
# =============================================================================

@dataclass(slots=True)
class AppsScriptConfig:
    """Immutable configuration for Apps Script client."""

    primary_url: str = ""
    backup_url: str = ""
    token: str = ""
    backup_token: str = ""
    token_transport: Set[TokenTransport] = field(
        default_factory=lambda: {TokenTransport.QUERY, TokenTransport.BODY}
    )
    token_param_name: str = "token"
    timeout_seconds: float = 45.0
    max_retries: int = 2
    retry_backoff_base: float = 1.2
    retry_jitter: float = 0.85
    cb_failure_threshold: int = 4
    cb_open_seconds: int = 45
    auth_lock_seconds: int = 300
    max_payload_size_bytes: int = 1_000_000
    max_log_chars: int = 12000
    connection_pool_size: int = 10
    keep_alive_seconds: int = 30
    verify_ssl: bool = True
    ssl_cert_path: Optional[str] = None
    enable_compression: bool = False
    enable_telemetry: bool = True
    enable_tracing: bool = False
    environment: str = "prod"
    backend_base_url: Optional[str] = None
    http_method: RequestMethod = RequestMethod.POST

    @classmethod
    def from_env(cls) -> "AppsScriptConfig":
        """Load configuration from environment variables."""
        env = os.getenv("APP_ENV", "prod").lower()

        # Robust HTTP method parse: fall back to POST on invalid env.
        http_method_raw = os.getenv("APPS_SCRIPT_HTTP_METHOD", "POST").upper()
        try:
            http_method = RequestMethod(http_method_raw)
        except ValueError:
            logger.warning(
                "Invalid APPS_SCRIPT_HTTP_METHOD=%r; defaulting to POST",
                http_method_raw,
            )
            http_method = RequestMethod.POST

        return cls(
            primary_url=os.getenv("GOOGLE_APPS_SCRIPT_URL", "").rstrip("/"),
            backup_url=os.getenv("GOOGLE_APPS_SCRIPT_BACKUP_URL", "").rstrip("/"),
            token=os.getenv("APP_TOKEN", ""),
            backup_token=os.getenv("BACKUP_APP_TOKEN", ""),
            token_transport=cls._parse_token_transport(
                os.getenv("APPS_SCRIPT_TOKEN_TRANSPORT", "query,body")
            ),
            token_param_name=os.getenv("APPS_SCRIPT_TOKEN_PARAM_NAME", "token"),
            timeout_seconds=float(os.getenv("APPS_SCRIPT_TIMEOUT_SEC", "45")),
            max_retries=int(os.getenv("APPS_SCRIPT_MAX_RETRIES", "2")),
            cb_failure_threshold=int(os.getenv("APPS_SCRIPT_CB_FAIL_THRESHOLD", "4")),
            cb_open_seconds=int(os.getenv("APPS_SCRIPT_CB_OPEN_SEC", "45")),
            auth_lock_seconds=int(os.getenv("APPS_SCRIPT_AUTH_LOCK_SEC", "300")),
            max_log_chars=int(os.getenv("APPS_SCRIPT_MAX_RAW_TEXT_CHARS", "12000")),
            verify_ssl=cls._parse_bool(os.getenv("APPS_SCRIPT_VERIFY_SSL", "true")),
            enable_telemetry=cls._parse_bool(os.getenv("APPS_SCRIPT_ENABLE_TELEMETRY", "true")),
            enable_tracing=cls._parse_bool(os.getenv("APPS_SCRIPT_ENABLE_TRACING", "false")),
            environment=env,
            backend_base_url=os.getenv("BACKEND_BASE_URL"),
            http_method=http_method,
        )

    @staticmethod
    def _parse_token_transport(value: str) -> Set[TokenTransport]:
        """Parse token transport string."""
        transports: Set[TokenTransport] = set()
        parts = value.lower().split(",")
        for part in parts:
            part = part.strip()
            if part == "query":
                transports.add(TokenTransport.QUERY)
            elif part == "header":
                transports.add(TokenTransport.HEADER)
            elif part == "body":
                transports.add(TokenTransport.BODY)
        return transports

    @staticmethod
    def _parse_bool(value: str) -> bool:
        """Parse boolean from string."""
        return value.lower().strip() in _TRUTHY

    @property
    def is_configured(self) -> bool:
        """Check if client is configured."""
        return bool(self.primary_url) and bool(self.token)


# =============================================================================
# Telemetry
# =============================================================================

@dataclass(slots=True)
class RequestMetrics:
    """Metrics for a single request."""
    request_id: str
    start_time: float
    end_time: float
    success: bool
    status_code: int
    url: str
    mode: str
    used_backup: bool
    retry_count: int
    error_type: Optional[str] = None
    response_size: int = 0
    payload_size: int = 0

    @property
    def duration_ms(self) -> float:
        """Get duration in milliseconds."""
        return (self.end_time - self.start_time) * 1000


class TelemetryCollector:
    """Thread-safe telemetry collection with aggregations."""

    def __init__(self, max_items: int = 10000) -> None:
        self._max_items = max(1000, max_items)
        self._metrics: List[RequestMetrics] = []
        self._lock = threading.RLock()
        self._counters: Dict[str, int] = {}
        self._latencies: Dict[str, List[float]] = {}

    def record(self, metrics: RequestMetrics) -> None:
        """Record a request."""
        with self._lock:
            self._metrics.append(metrics)
            if len(self._metrics) > self._max_items:
                self._metrics = self._metrics[-self._max_items:]

            self._counters["total"] = self._counters.get("total", 0) + 1
            if metrics.success:
                self._counters["success"] = self._counters.get("success", 0) + 1
            else:
                self._counters["failure"] = self._counters.get("failure", 0) + 1

            if metrics.mode not in self._latencies:
                self._latencies[metrics.mode] = []
            self._latencies[metrics.mode].append(metrics.duration_ms)
            if len(self._latencies[metrics.mode]) > 100:
                self._latencies[metrics.mode] = self._latencies[metrics.mode][-100:]

    def get_stats(self) -> Dict[str, Any]:
        """Get aggregated statistics."""
        with self._lock:
            total = self._counters.get("total", 0)
            success = self._counters.get("success", 0)

            stats: Dict[str, Any] = {
                "total_requests": total,
                "success_rate": (success / total * 100) if total > 0 else 0,
                "failures": self._counters.get("failure", 0),
                "latency_by_mode": {},
            }

            for mode, latencies in self._latencies.items():
                if latencies:
                    sorted_lat = sorted(latencies)
                    n = len(sorted_lat)
                    # Clamp percentile indices to n-1 to avoid IndexError on
                    # small sample sizes.
                    p50_idx = min(n // 2, n - 1)
                    p95_idx = min(int(n * 0.95), n - 1)
                    p99_idx = min(int(n * 0.99), n - 1)
                    stats["latency_by_mode"][mode] = {
                        "avg": sum(latencies) / n,
                        "p50": sorted_lat[p50_idx],
                        "p95": sorted_lat[p95_idx],
                        "p99": sorted_lat[p99_idx],
                        "max": max(latencies),
                    }

            return stats

    def reset(self) -> None:
        """Reset all metrics."""
        with self._lock:
            self._metrics.clear()
            self._counters.clear()
            self._latencies.clear()


# =============================================================================
# Trace Context
# =============================================================================

class TraceContext:
    """
    OpenTelemetry trace context manager (sync + async compatible).

    v6.1.0 fix: enter the context manager returned by
    ``tracer.start_as_current_span`` to obtain the real Span before
    calling Span methods on it. v6.0.0 stored the CM in ``self.span``
    and then called ``set_attributes`` / ``record_exception`` /
    ``set_status`` on it -- every call failed silently through the
    catch-all ``except Exception: pass``.
    """

    def __init__(self, name: str, attributes: Optional[Dict[str, Any]] = None) -> None:
        self.name = name
        self.attributes = attributes or {}
        self.tracer = (
            trace.get_tracer(__name__)
            if (_OTEL_AVAILABLE and _TRACING_ENABLED and trace)
            else None
        )
        self._cm: Any = None
        self.span: Any = None

    def __enter__(self) -> "TraceContext":
        if self.tracer:
            try:
                self._cm = self.tracer.start_as_current_span(self.name)
                self.span = self._cm.__enter__()
                if self.attributes and self.span is not None:
                    try:
                        self.span.set_attributes(self.attributes)
                    except Exception:
                        pass
            except Exception:
                self._cm = None
                self.span = None
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        if self.span is not None and _OTEL_AVAILABLE and Status and StatusCode:
            if exc_val:
                try:
                    self.span.record_exception(exc_val)
                    self.span.set_status(Status(StatusCode.ERROR, str(exc_val)))
                except Exception:
                    pass
        if self._cm is not None:
            try:
                self._cm.__exit__(exc_type, exc_val, exc_tb)
            except Exception:
                pass
        self._cm = None
        self.span = None

    async def __aenter__(self) -> "TraceContext":
        return self.__enter__()

    async def __aexit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        self.__exit__(exc_type, exc_val, exc_tb)


# =============================================================================
# Circuit Breaker
# =============================================================================

class FailureClassifier:
    """Classifies failures for circuit breaker decisions."""

    @staticmethod
    def classify(exception: Exception, status_code: Optional[int] = None) -> FailureType:
        """Classify failure type."""
        if status_code:
            if status_code in (401, 403):
                return FailureType.AUTH
            if 400 <= status_code < 500:
                return FailureType.CLIENT
            if 500 <= status_code < 600:
                return FailureType.SERVER

        if isinstance(exception, (urllib.error.URLError, ConnectionError)):
            return FailureType.NETWORK
        if isinstance(exception, TimeoutError):
            return FailureType.TIMEOUT
        if isinstance(exception, (ValueError, TypeError, json.JSONDecodeError)):
            return FailureType.PARSING

        return FailureType.UNKNOWN


class CircuitBreaker:
    """Advanced circuit breaker with failure type awareness."""

    def __init__(
        self,
        failure_threshold: int = 4,
        open_seconds: int = 45,
        auth_lock_seconds: int = 300,
    ) -> None:
        self.failure_threshold = failure_threshold
        self.open_seconds = open_seconds
        self.auth_lock_seconds = auth_lock_seconds

        self.state = CircuitState.CLOSED
        self.failure_counts: Dict[FailureType, int] = {}
        self.total_failures = 0
        self.last_failure_time: Optional[float] = None
        self.state_until: float = 0
        self._lock = threading.RLock()

    def record_success(self) -> None:
        """Record a successful call."""
        with self._lock:
            if self.state == CircuitState.HALF_OPEN:
                self.state = CircuitState.CLOSED
                self.state_until = 0

            self.failure_counts.clear()
            self.total_failures = 0

    def record_failure(self, failure_type: FailureType) -> None:
        """Record a failure."""
        with self._lock:
            now = time.time()
            self.last_failure_time = now

            self.failure_counts[failure_type] = (
                self.failure_counts.get(failure_type, 0) + 1
            )
            self.total_failures += 1

            if self.state == CircuitState.CLOSED:
                if self.total_failures >= self.failure_threshold:
                    self.state = CircuitState.OPEN
                    self.state_until = now + self.open_seconds
                    logger.warning(
                        "Circuit breaker opened after %d failures",
                        self.total_failures,
                    )

            if failure_type == FailureType.AUTH:
                self.state_until = max(self.state_until, now + self.auth_lock_seconds)

    def can_execute(self) -> bool:
        """Check if execution is allowed."""
        with self._lock:
            if self.state == CircuitState.CLOSED:
                return True

            if self.state == CircuitState.OPEN:
                if time.time() >= self.state_until:
                    self.state = CircuitState.HALF_OPEN
                    logger.info("Circuit breaker half-open, testing recovery")
                    return True
                return False

            return True

    def get_state(self) -> Dict[str, Any]:
        """Get current circuit state."""
        with self._lock:
            return {
                "state": self.state.value,
                "total_failures": self.total_failures,
                "failure_counts": {k.value: v for k, v in self.failure_counts.items()},
                "state_until": self.state_until,
                "remaining_seconds": (
                    max(0, self.state_until - time.time()) if self.state_until else 0
                ),
            }


# =============================================================================
# Connection Pool
# =============================================================================

class ConnectionPool:
    """Simple connection pool with keep-alive."""

    def __init__(self, max_size: int = 10) -> None:
        self.max_size = max_size
        self._pool: List[Any] = []
        self._in_use: Set[int] = set()
        self._lock = threading.RLock()

    def acquire(self) -> Optional[Any]:
        """Acquire a connection from the pool."""
        with self._lock:
            if self._pool:
                return self._pool.pop()
            return None

    def release(self, conn: Any) -> None:
        """Release connection back to pool."""
        with self._lock:
            if len(self._pool) < self.max_size:
                self._pool.append(conn)

    def clear(self) -> None:
        """Clear all connections."""
        with self._lock:
            self._pool.clear()
            self._in_use.clear()


# =============================================================================
# Rate Limiter
# =============================================================================

class TokenBucket:
    """Token bucket rate limiter."""

    def __init__(self, rate: float, capacity: int) -> None:
        self.rate = rate
        self.capacity = capacity
        self.tokens: float = float(capacity)
        self.last_refill = time.time()
        self._lock = threading.RLock()

    def acquire(self, tokens: int = 1) -> bool:
        """Acquire tokens, returns True if successful."""
        with self._lock:
            self._refill()
            if self.tokens >= tokens:
                self.tokens -= tokens
                return True
            return False

    def _refill(self) -> None:
        """Refill tokens based on elapsed time."""
        now = time.time()
        elapsed = now - self.last_refill
        self.tokens = min(float(self.capacity), self.tokens + elapsed * self.rate)
        self.last_refill = now


# =============================================================================
# Symbol Normalization
# =============================================================================

class SymbolNormalizer:
    """Advanced symbol normalization with market detection."""

    @staticmethod
    @lru_cache(maxsize=4096)
    def normalize_ksa(symbol: str) -> str:
        """Normalize KSA symbol to canonical format (####.SR)."""
        s = str(symbol or "").strip().upper()
        if not s:
            return ""

        for prefix in ("TADAWUL:", "TDWL:", "SA:", "KSA:", "TASI:", "SR:", "SAR:"):
            if s.startswith(prefix):
                s = s[len(prefix):].strip()

        if s.endswith(".TADAWUL"):
            s = s[:-8].strip()
        if s.endswith(".SR"):
            s = s[:-3].strip()

        if s.isdigit() and 3 <= len(s) <= 6:
            return f"{s}.SR"

        return ""

    @staticmethod
    @lru_cache(maxsize=4096)
    def normalize_global(symbol: str) -> str:
        """Normalize global symbol."""
        s = str(symbol or "").strip().upper()
        if not s:
            return ""

        for prefix in ("NASDAQ:", "NYSE:", "US:", "GLOBAL:"):
            if s.startswith(prefix):
                s = s[len(prefix):].strip()

        return s

    @staticmethod
    @lru_cache(maxsize=1024)
    def detect_market(symbol: str) -> MarketCategory:
        """Detect market category from symbol."""
        s = str(symbol or "").strip().upper()

        if s.endswith(".SR"):
            core = s[:-3].strip()
            if core.isdigit() and 3 <= len(core) <= 6:
                return MarketCategory.KSA

        if s.isdigit() and 3 <= len(s) <= 6:
            return MarketCategory.KSA

        return MarketCategory.GLOBAL


def split_tickers_by_market(
    tickers: Sequence[str],
    deduplicate: bool = True,
    preserve_order: bool = True,
) -> Tuple[List[str], List[str]]:
    """
    Split tickers into KSA and Global lists.

    Args:
        tickers: List of tickers to split.
        deduplicate: Whether to deduplicate tickers.
        preserve_order: Whether to preserve original order.

    Returns:
        Tuple of (ksa_tickers, global_tickers).
    """
    if not tickers:
        return [], []

    seen_ksa: Set[str] = set()
    seen_global: Set[str] = set()
    ksa_result: List[str] = []
    global_result: List[str] = []

    normalizer = SymbolNormalizer()

    for ticker in tickers:
        if not ticker or not isinstance(ticker, str):
            continue

        ticker = ticker.strip()
        if not ticker:
            continue

        ksa_norm = normalizer.normalize_ksa(ticker)
        if ksa_norm:
            if not deduplicate or ksa_norm not in seen_ksa:
                seen_ksa.add(ksa_norm)
                ksa_result.append(ksa_norm)
            continue

        global_norm = normalizer.normalize_global(ticker)
        if global_norm:
            if not deduplicate or global_norm not in seen_global:
                seen_global.add(global_norm)
                global_result.append(global_norm)

    return ksa_result, global_result


# =============================================================================
# Page Specification
# =============================================================================

@dataclass(slots=True)
class PageSpec:
    """Page specification with market and mode."""
    market: MarketCategory
    mode: str
    sheet_headers: Optional[List[str]] = None
    description: str = ""


# v6.1.0: removed ``KSA_TADAWUL`` entry -- not a canonical sheet in the
# authoritative schema_registry. ``KSA``/``TADAWUL``/``SAUDI`` aliases
# below route to the canonical ``MARKET_LEADERS`` sheet instead.
PAGE_SPECS: Dict[str, PageSpec] = {
    "MARKET_LEADERS": PageSpec(
        market=MarketCategory.MIXED,
        mode="refresh_quotes",
        description="Top market leaders (includes Tadawul listings)",
    ),
    "GLOBAL_MARKETS": PageSpec(
        market=MarketCategory.GLOBAL,
        mode="refresh_quotes_global",
        description="Global markets",
    ),
    "MY_PORTFOLIO": PageSpec(
        market=MarketCategory.MIXED,
        mode="refresh_quotes",
        description="User portfolio",
    ),
    "MUTUAL_FUNDS": PageSpec(
        market=MarketCategory.GLOBAL,
        mode="refresh_quotes_global",
        description="Mutual funds",
    ),
    "COMMODITIES_FX": PageSpec(
        market=MarketCategory.GLOBAL,
        mode="refresh_quotes_global",
        description="Commodities & Forex",
    ),
    "INSIGHTS_ANALYSIS": PageSpec(
        market=MarketCategory.MIXED,
        mode="refresh_quotes",
        description="Investment insights",
    ),
    "TOP_10_INVESTMENTS": PageSpec(
        market=MarketCategory.MIXED,
        mode="refresh_top10",
        description="Top 10 investment recommendations",
    ),
    "DATA_DICTIONARY": PageSpec(
        market=MarketCategory.MIXED,
        mode="refresh_dictionary",
        description="Data dictionary / schema reference",
    ),
}


@lru_cache(maxsize=128)
def normalize_page_key(page_key: Optional[str]) -> str:
    """Normalize page key to canonical form."""
    if not page_key:
        return ""

    if _HAS_CORE:
        try:
            # core.schemas.normalize_sheet_name returns Pascal_Snake_Case
            # like ``Market_Leaders``; our PAGE_SPECS keys are SHOUTY_CASE
            # like ``MARKET_LEADERS``. Upper-case the result so the two
            # conventions meet.
            canonical = normalize_sheet_name(page_key)
            if canonical:
                return canonical.upper()
        except Exception:
            pass

    s = page_key.strip().upper()

    for prefix in ("SHEET_", "PAGE_", "TAB_"):
        if s.startswith(prefix):
            s = s[len(prefix):]

    for suffix in ("_SHEET", "_PAGE", "_TAB"):
        if s.endswith(suffix):
            s = s[:-len(suffix)]

    for ch in ("-", " ", ".", "/", "\\", "|", ":", ";", ",", "(", ")"):
        s = s.replace(ch, "_")

    while "__" in s:
        s = s.replace("__", "_")

    aliases = {
        # v6.1.0: KSA/TADAWUL/SAUDI no longer point at a phantom
        # KSA_TADAWUL page; they route to the canonical MARKET_LEADERS
        # sheet which carries Tadawul listings in the registry.
        "KSA": "MARKET_LEADERS",
        "TADAWUL": "MARKET_LEADERS",
        "SAUDI": "MARKET_LEADERS",
        "KSA_TADAWUL": "MARKET_LEADERS",
        "GLOBAL": "GLOBAL_MARKETS",
        "WORLD": "GLOBAL_MARKETS",
        "PORTFOLIO": "MY_PORTFOLIO",
        "FUNDS": "MUTUAL_FUNDS",
        "COMMODITIES": "COMMODITIES_FX",
        "FX": "COMMODITIES_FX",
        "FOREX": "COMMODITIES_FX",
        "INSIGHTS": "INSIGHTS_ANALYSIS",
        "ANALYSIS": "INSIGHTS_ANALYSIS",
        "TOP10": "TOP_10_INVESTMENTS",
        "TOP_10": "TOP_10_INVESTMENTS",
        "TOP10INVESTMENTS": "TOP_10_INVESTMENTS",
        "ADVISOR": "TOP_10_INVESTMENTS",
        "DICTIONARY": "DATA_DICTIONARY",
        "DICT": "DATA_DICTIONARY",
    }

    return aliases.get(s, s)


def resolve_page_spec(page_key: Optional[str]) -> PageSpec:
    """Resolve page specification from page key."""
    norm_key = normalize_page_key(page_key)
    return PAGE_SPECS.get(norm_key, PAGE_SPECS["GLOBAL_MARKETS"])


# =============================================================================
# Payload Utilities
# =============================================================================

def prune_none(obj: Any) -> Any:
    """Recursively remove None values from dictionaries and lists."""
    if obj is None:
        return None

    if isinstance(obj, dict):
        result_d: Dict[Any, Any] = {}
        for key, value in obj.items():
            pruned = prune_none(value)
            if pruned is not None:
                result_d[key] = pruned
        return result_d

    if isinstance(obj, (list, tuple)):
        result_l: List[Any] = []
        for item in obj:
            pruned = prune_none(item)
            if pruned is not None:
                result_l.append(pruned)
        return type(obj)(result_l) if isinstance(obj, tuple) else result_l

    return obj


def compute_payload_hash(payload: Dict[str, Any]) -> str:
    """Compute hash of payload for deduplication."""
    try:
        if _HAS_ORJSON:
            json_str = _json_dumps(payload, default=str)
        else:
            json_str = json.dumps(payload, sort_keys=True, default=str)
        return hashlib.sha256(json_str.encode()).hexdigest()[:16]
    except Exception:
        return ""


def now_utc_iso() -> str:
    """Get current UTC time as ISO string."""
    try:
        return datetime.now(timezone.utc).isoformat()
    except Exception:
        return ""


# =============================================================================
# Result Model
# =============================================================================

@dataclass(slots=True)
class AppsScriptResult:
    """Result from Apps Script call."""
    ok: bool
    status_code: int
    data: Any
    error: Optional[str] = None
    error_type: Optional[FailureType] = None
    raw_text: Optional[str] = None
    url: Optional[str] = None
    elapsed_ms: Optional[int] = None
    used_backup: bool = False
    retry_count: int = 0
    request_id: Optional[str] = None

    def as_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return {
            "ok": self.ok,
            "status_code": self.status_code,
            "error": self.error,
            "elapsed_ms": self.elapsed_ms,
            "used_backup": self.used_backup,
            "request_id": self.request_id,
        }


# =============================================================================
# Prometheus Metrics
# =============================================================================

_gas_requests_total = (
    Counter(
        "gas_requests_total",
        "Total requests to Google Apps Script",
        ["mode", "status"],
    )
    if _PROMETHEUS_AVAILABLE
    else _DummyMetric()
)

_gas_request_duration = (
    Histogram(
        "gas_request_duration_seconds",
        "Google Apps Script request duration",
        ["mode"],
    )
    if _PROMETHEUS_AVAILABLE
    else _DummyMetric()
)


# =============================================================================
# Main Client
# =============================================================================

class GoogleAppsScriptClient:
    """Advanced Google Apps Script client with enterprise features."""

    def __init__(self, config: Optional[AppsScriptConfig] = None) -> None:
        self.config = config or AppsScriptConfig.from_env()
        self._circuit_breaker = CircuitBreaker(
            failure_threshold=self.config.cb_failure_threshold,
            open_seconds=self.config.cb_open_seconds,
            auth_lock_seconds=self.config.auth_lock_seconds,
        )
        self._connection_pool = ConnectionPool(max_size=self.config.connection_pool_size)
        self._rate_limiter = TokenBucket(rate=10.0, capacity=20)
        self._ssl_context = self._create_ssl_context()
        self._closed = False
        self._lock = threading.RLock()
        self._telemetry = TelemetryCollector()

        logger.info(
            "GoogleAppsScriptClient v%s initialized (env=%s)",
            CLIENT_VERSION,
            self.config.environment,
        )

    def _create_ssl_context(self) -> ssl.SSLContext:
        """Create SSL context with configuration."""
        ctx = ssl.create_default_context()

        if not self.config.verify_ssl:
            ctx.check_hostname = False
            ctx.verify_mode = ssl.CERT_NONE

        if self.config.ssl_cert_path:
            try:
                ctx.load_verify_locations(cafile=self.config.ssl_cert_path)
            except Exception as e:
                logger.warning(
                    "Failed to load SSL cert from %s: %s",
                    self.config.ssl_cert_path,
                    e,
                )

        return ctx

    def _get_active_token(self, used_backup: bool) -> str:
        """Get appropriate token based on URL selection."""
        if used_backup and self.config.backup_token:
            return self.config.backup_token
        return self.config.token

    def _build_url(
        self,
        base_url: str,
        mode: str,
        query: Optional[Dict[str, str]] = None,
        used_backup: bool = False,
    ) -> str:
        """Build URL with query parameters."""
        if not base_url:
            return ""

        parsed = urllib.parse.urlparse(base_url)
        existing = dict(urllib.parse.parse_qsl(parsed.query, keep_blank_values=True))

        existing["mode"] = mode

        if TokenTransport.QUERY in self.config.token_transport:
            token = self._get_active_token(used_backup)
            if token:
                existing[self.config.token_param_name] = token

        if query:
            for key, value in query.items():
                if value is not None:
                    existing[str(key)] = str(value)

        new_query = urllib.parse.urlencode(existing)
        rebuilt = parsed._replace(query=new_query)
        return urllib.parse.urlunparse(rebuilt)

    def _build_headers(
        self,
        used_backup: bool = False,
        request_id: Optional[str] = None,
        content_length: Optional[int] = None,
    ) -> Dict[str, str]:
        """Build HTTP headers."""
        headers = {
            "Content-Type": "application/json",
            "User-Agent": f"TFB-AppsScriptClient/{CLIENT_VERSION}",
            "Accept": "application/json",
            "X-Client-Version": CLIENT_VERSION,
        }

        if TokenTransport.HEADER in self.config.token_transport:
            token = self._get_active_token(used_backup)
            if token:
                headers["X-APP-TOKEN"] = token

        if request_id:
            headers["X-Request-ID"] = request_id

        if content_length is not None:
            headers["Content-Length"] = str(content_length)

        headers["Connection"] = "keep-alive"
        headers["Keep-Alive"] = f"timeout={self.config.keep_alive_seconds}"

        return headers

    def _build_payload(
        self,
        base_payload: Optional[Dict[str, Any]],
        used_backup: bool = False,
    ) -> Dict[str, Any]:
        """Build request payload with auth if needed."""
        payload = dict(base_payload or {})

        if TokenTransport.BODY in self.config.token_transport:
            token = self._get_active_token(used_backup)
            if token:
                if "auth" not in payload or not isinstance(payload["auth"], dict):
                    payload["auth"] = {}
                payload["auth"][self.config.token_param_name] = token

        return prune_none(payload)

    def _get_urls_to_try(self) -> List[Tuple[str, bool]]:
        """Get ordered list of URLs to try."""
        urls: List[Tuple[str, bool]] = []
        if self.config.primary_url:
            urls.append((self.config.primary_url, False))
        if self.config.backup_url and self.config.backup_url != self.config.primary_url:
            urls.append((self.config.backup_url, True))
        return urls

    def _calculate_backoff(
        self,
        attempt: int,
        retry_after: Optional[float] = None,
    ) -> float:
        """Calculate backoff time with jitter."""
        if retry_after is not None and retry_after > 0:
            return min(30.0, retry_after)

        base = self.config.retry_backoff_base ** max(0, attempt)
        jitter = random.uniform(0, self.config.retry_jitter)
        return min(12.0, base + jitter)

    def _handle_response(
        self,
        response: Any,
        url: str,
        used_backup: bool,
        start_time: float,
        request_id: Optional[str],
        mode: str,
        retry_count: int,
    ) -> AppsScriptResult:
        """Handle HTTP response."""
        try:
            raw = response.read().decode("utf-8", errors="replace")
            code = int(getattr(response, "status", 200) or 200)

            try:
                data = _json_loads(raw) if raw else {}
                success = True
                error: Optional[str] = None
                error_type: Optional[FailureType] = None
            except Exception as e:
                data = None
                success = False
                error = f"Invalid JSON: {e}"
                error_type = FailureType.PARSING

            if success:
                self._circuit_breaker.record_success()
                _gas_requests_total.labels(mode=mode, status="success").inc()
                _gas_request_duration.labels(mode=mode).observe(time.time() - start_time)

            if self.config.enable_telemetry:
                metrics = RequestMetrics(
                    request_id=request_id or "",
                    start_time=start_time,
                    end_time=time.time(),
                    success=success,
                    status_code=code,
                    url=url,
                    mode=mode,
                    used_backup=used_backup,
                    retry_count=retry_count,
                    error_type=error_type.value if error_type else None,
                    response_size=len(raw),
                )
                self._telemetry.record(metrics)

            return AppsScriptResult(
                ok=success,
                status_code=code,
                data=data,
                error=error,
                error_type=error_type,
                raw_text=raw[:self.config.max_log_chars] if raw else None,
                url=url,
                elapsed_ms=int((time.time() - start_time) * 1000),
                used_backup=used_backup,
                retry_count=retry_count,
                request_id=request_id,
            )

        except Exception as e:
            failure_type = FailureClassifier.classify(e)
            self._circuit_breaker.record_failure(failure_type)
            _gas_requests_total.labels(mode=mode, status="failure").inc()

            return AppsScriptResult(
                ok=False,
                status_code=0,
                data=None,
                error=f"Response error: {e}",
                error_type=failure_type,
                url=url,
                elapsed_ms=int((time.time() - start_time) * 1000),
                used_backup=used_backup,
                retry_count=retry_count,
                request_id=request_id,
            )

    def _handle_http_error(
        self,
        e: urllib.error.HTTPError,
        url: str,
        used_backup: bool,
        start_time: float,
        request_id: Optional[str],
        mode: str,
        retry_count: int,
    ) -> AppsScriptResult:
        """Handle HTTP error."""
        code = int(getattr(e, "code", 0) or 0)

        try:
            raw = e.read()
            raw_text = raw.decode("utf-8", errors="replace") if raw else ""
        except Exception:
            raw_text = ""

        failure_type = FailureClassifier.classify(e, code)
        self._circuit_breaker.record_failure(failure_type)
        _gas_requests_total.labels(mode=mode, status="error").inc()

        error_msg = f"HTTP {code}"
        if hasattr(e, "reason") and e.reason:
            error_msg += f": {e.reason}"

        return AppsScriptResult(
            ok=False,
            status_code=code,
            data=None,
            error=error_msg,
            error_type=failure_type,
            raw_text=raw_text[:self.config.max_log_chars] if raw_text else None,
            url=url,
            elapsed_ms=int((time.time() - start_time) * 1000),
            used_backup=used_backup,
            retry_count=retry_count,
            request_id=request_id,
        )

    def call_script(
        self,
        mode: str,
        payload: Optional[Dict[str, Any]] = None,
        method: Optional[RequestMethod] = None,
        retries: Optional[int] = None,
        query: Optional[Dict[str, str]] = None,
        request_id: Optional[str] = None,
        timeout: Optional[float] = None,
    ) -> AppsScriptResult:
        """
        Make a call to Google Apps Script.

        Args:
            mode: Operation mode (e.g., "refresh_quotes", "health").
            payload: Request payload.
            method: HTTP method (default from config).
            retries: Number of retries (default from config).
            query: Query parameters.
            request_id: Optional request ID for tracing.
            timeout: Request timeout in seconds.

        Returns:
            AppsScriptResult with response data.
        """
        with TraceContext(f"gas_call_{mode}", {"mode": mode, "request_id": request_id}):
            if self._closed:
                return AppsScriptResult(
                    ok=False,
                    status_code=0,
                    data=None,
                    error="Client is closed",
                    error_type=FailureType.UNKNOWN,
                )

            if not self._circuit_breaker.can_execute():
                state = self._circuit_breaker.get_state()
                return AppsScriptResult(
                    ok=False,
                    status_code=0,
                    data=None,
                    error=f"Circuit breaker {state['state']}",
                    error_type=FailureType.UNKNOWN,
                )

            if not self._rate_limiter.acquire():
                return AppsScriptResult(
                    ok=False,
                    status_code=429,
                    data=None,
                    error="Rate limit exceeded",
                    error_type=FailureType.CLIENT,
                )

            if not request_id and self.config.enable_tracing:
                request_id = hashlib.md5(
                    f"{time.time()}{random.random()}".encode()
                ).hexdigest()[:16]

            use_method = method or self.config.http_method
            max_retries = retries if retries is not None else self.config.max_retries
            timeout_sec = timeout or self.config.timeout_seconds

            clean_payload = self._build_payload(payload, used_backup=False)

            body_bytes: Optional[bytes] = None
            if use_method != RequestMethod.GET:
                try:
                    body_bytes = _json_dumps(clean_payload).encode("utf-8")
                    if len(body_bytes) > self.config.max_payload_size_bytes:
                        return AppsScriptResult(
                            ok=False,
                            status_code=413,
                            data=None,
                            error=(
                                f"Payload too large: {len(body_bytes)} > "
                                f"{self.config.max_payload_size_bytes}"
                            ),
                            error_type=FailureType.CLIENT,
                        )
                except Exception as e:
                    return AppsScriptResult(
                        ok=False,
                        status_code=0,
                        data=None,
                        error=f"JSON encode error: {e}",
                        error_type=FailureType.PARSING,
                    )

            urls_to_try = self._get_urls_to_try()
            if not urls_to_try:
                return AppsScriptResult(
                    ok=False,
                    status_code=0,
                    data=None,
                    error="No URLs configured",
                    error_type=FailureType.CLIENT,
                )

            start_time = time.time()
            last_result: Optional[AppsScriptResult] = None

            for url_index, (base_url, used_backup) in enumerate(urls_to_try):
                url = self._build_url(base_url, mode, query, used_backup)
                if not url:
                    continue

                headers = self._build_headers(
                    used_backup=used_backup,
                    request_id=request_id,
                    content_length=len(body_bytes) if body_bytes else None,
                )

                for attempt in range(max_retries + 1):
                    try:
                        req = urllib.request.Request(
                            url,
                            data=body_bytes,
                            headers=headers,
                            method=use_method.value,
                        )

                        with urllib.request.urlopen(
                            req, timeout=timeout_sec, context=self._ssl_context
                        ) as resp:
                            result = self._handle_response(
                                resp, url, used_backup, start_time,
                                request_id, mode, attempt,
                            )
                            if result.ok:
                                return result
                            last_result = result

                            if attempt < max_retries:
                                retryable_codes = {408, 409, 425, 429, 500, 502, 503, 504}
                                if result.status_code in retryable_codes:
                                    retry_after: Optional[float] = None
                                    if hasattr(resp, "headers"):
                                        ra = resp.headers.get("Retry-After")
                                        try:
                                            retry_after = float(ra) if ra else None
                                        except (TypeError, ValueError):
                                            retry_after = None
                                    backoff = self._calculate_backoff(
                                        attempt, retry_after
                                    )
                                    logger.debug(
                                        "Retry %d/%d after %.2fs",
                                        attempt + 1, max_retries, backoff,
                                    )
                                    time.sleep(backoff)
                                    continue

                    except urllib.error.HTTPError as e:
                        result = self._handle_http_error(
                            e, url, used_backup, start_time,
                            request_id, mode, attempt,
                        )
                        last_result = result
                        if attempt < max_retries:
                            retryable_codes = {408, 409, 425, 429, 500, 502, 503, 504}
                            if result.status_code in retryable_codes:
                                retry_after = None
                                if hasattr(e, "headers") and e.headers:
                                    ra = e.headers.get("Retry-After")
                                    try:
                                        retry_after = float(ra) if ra else None
                                    except (TypeError, ValueError):
                                        retry_after = None
                                backoff = self._calculate_backoff(attempt, retry_after)
                                logger.debug(
                                    "HTTP %d retry %d/%d after %.2fs",
                                    result.status_code, attempt + 1,
                                    max_retries, backoff,
                                )
                                time.sleep(backoff)
                                continue
                        break

                    except Exception as e:
                        failure_type = FailureClassifier.classify(e)
                        self._circuit_breaker.record_failure(failure_type)
                        result = AppsScriptResult(
                            ok=False,
                            status_code=0,
                            data=None,
                            error=f"Request error: {e}",
                            error_type=failure_type,
                            url=url,
                            elapsed_ms=int((time.time() - start_time) * 1000),
                            used_backup=used_backup,
                            retry_count=attempt,
                            request_id=request_id,
                        )
                        last_result = result
                        if (
                            attempt < max_retries
                            and failure_type in (FailureType.NETWORK, FailureType.TIMEOUT)
                        ):
                            backoff = self._calculate_backoff(attempt)
                            logger.debug(
                                "Network error, retry %d/%d after %.2fs",
                                attempt + 1, max_retries, backoff,
                            )
                            time.sleep(backoff)
                            continue
                        break

                if url_index < len(urls_to_try) - 1:
                    logger.info(
                        "Failing over to backup URL after %d attempts",
                        max_retries + 1,
                    )
                    continue
                break

            if last_result:
                return last_result

            return AppsScriptResult(
                ok=False,
                status_code=0,
                data=None,
                error="No response from any URL",
                error_type=FailureType.NETWORK,
                elapsed_ms=int((time.time() - start_time) * 1000),
                request_id=request_id,
            )

    def sync_page_quotes(
        self,
        page_key: str,
        sheet_id: str,
        sheet_name: str,
        tickers: Sequence[str],
        request_id: Optional[str] = None,
        extra_meta: Optional[Dict[str, Any]] = None,
        method: Optional[RequestMethod] = None,
        retries: Optional[int] = None,
    ) -> AppsScriptResult:
        """
        Sync page quotes for a given page.

        Args:
            page_key: Page identifier (e.g., "MARKET_LEADERS").
            sheet_id: Google Sheet ID.
            sheet_name: Sheet name.
            tickers: List of tickers to sync.
            request_id: Optional request ID.
            extra_meta: Extra metadata to include.
            method: HTTP method.
            retries: Number of retries.

        Returns:
            AppsScriptResult with response data.
        """
        spec = resolve_page_spec(page_key)
        ksa_tickers, global_tickers = split_tickers_by_market(tickers)

        payload: Dict[str, Any] = {
            "sheet": {"id": sheet_id, "name": sheet_name},
            "tickers": {
                "all": list(tickers),
                "ksa": ksa_tickers,
                "global": global_tickers,
            },
            "market": spec.market.value,
            "meta": {
                "client": "tfb_backend",
                "version": CLIENT_VERSION,
                "timestamp_utc": now_utc_iso(),
                "page_key": normalize_page_key(page_key),
                "page_mode": spec.mode,
            },
        }

        if self.config.backend_base_url:
            payload["backend"] = {"base_url": self.config.backend_base_url}

        if extra_meta:
            payload["meta"].update(extra_meta)

        if _HAS_CORE:
            try:
                headers = get_headers_for_sheet(page_key)
                if headers:
                    payload["sheet"]["headers"] = headers
            except Exception:
                pass

        logger.info(
            "Syncing page %s: %d KSA, %d global",
            page_key,
            len(ksa_tickers),
            len(global_tickers),
        )

        return self.call_script(
            mode=spec.mode,
            payload=payload,
            method=method,
            retries=retries,
            request_id=request_id,
        )

    def ping(
        self,
        request_id: Optional[str] = None,
        method: Optional[RequestMethod] = None,
        retries: Optional[int] = None,
    ) -> AppsScriptResult:
        """
        Ping the Apps Script endpoint.

        Args:
            request_id: Optional request ID.
            method: HTTP method.
            retries: Number of retries.

        Returns:
            AppsScriptResult with health check response.
        """
        return self.call_script(
            mode="health",
            payload={
                "meta": {
                    "client": "tfb_backend",
                    "version": CLIENT_VERSION,
                    "timestamp_utc": now_utc_iso(),
                }
            },
            method=method or RequestMethod.GET,
            retries=retries or 1,
            request_id=request_id,
        )

    def get_status(self) -> Dict[str, Any]:
        """Get client status."""
        return {
            "client_version": CLIENT_VERSION,
            "config": {
                "environment": self.config.environment,
                "primary_url": bool(self.config.primary_url),
                "backup_url": bool(self.config.backup_url),
                "token_configured": bool(self.config.token),
                "timeout_seconds": self.config.timeout_seconds,
                "max_retries": self.config.max_retries,
            },
            "circuit_breaker": self._circuit_breaker.get_state(),
            "telemetry": (
                self._telemetry.get_stats() if self.config.enable_telemetry else {}
            ),
            "closed": self._closed,
        }

    def close(self) -> None:
        """Close the client."""
        with self._lock:
            if not self._closed:
                self._connection_pool.clear()
                self._closed = True
                logger.info("GoogleAppsScriptClient closed")

    def __enter__(self) -> "GoogleAppsScriptClient":
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        self.close()


# =============================================================================
# Async Client
# =============================================================================

class AsyncGoogleAppsScriptClient:
    """
    Async version of the client for asyncio applications.

    Uses a thread pool for blocking operations while maintaining OTEL
    contexts. v6.1.0 fix: ``concurrent.futures`` is now imported at the
    module level, so ``_get_executor()`` no longer raises ``NameError``
    on first call.
    """

    def __init__(self, config: Optional[AppsScriptConfig] = None) -> None:
        self._sync_client = GoogleAppsScriptClient(config)
        self._executor: Optional[concurrent.futures.ThreadPoolExecutor] = None
        self._loop: Optional[asyncio.AbstractEventLoop] = None

    def _get_executor(self) -> concurrent.futures.ThreadPoolExecutor:
        """Get or create thread pool executor."""
        if self._executor is None:
            self._executor = concurrent.futures.ThreadPoolExecutor(
                max_workers=10,
                thread_name_prefix="GasAsyncWorker",
            )
        return self._executor

    async def call_script(self, *args: Any, **kwargs: Any) -> AppsScriptResult:
        """Async wrapper for call_script."""
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(
            self._get_executor(),
            lambda: self._sync_client.call_script(*args, **kwargs),
        )

    async def sync_page_quotes(self, *args: Any, **kwargs: Any) -> AppsScriptResult:
        """Async wrapper for sync_page_quotes."""
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(
            self._get_executor(),
            lambda: self._sync_client.sync_page_quotes(*args, **kwargs),
        )

    async def ping(self, *args: Any, **kwargs: Any) -> AppsScriptResult:
        """Async wrapper for ping."""
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(
            self._get_executor(),
            lambda: self._sync_client.ping(*args, **kwargs),
        )

    async def get_status(self) -> Dict[str, Any]:
        """Async wrapper for get_status."""
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(
            self._get_executor(),
            self._sync_client.get_status,
        )

    async def close(self) -> None:
        """Close the client."""
        loop = asyncio.get_running_loop()
        await loop.run_in_executor(
            self._get_executor(),
            self._sync_client.close,
        )
        if self._executor:
            self._executor.shutdown(wait=False)
            self._executor = None

    @asynccontextmanager
    async def session(self) -> AsyncGenerator["AsyncGoogleAppsScriptClient", None]:
        """Async context manager for session."""
        try:
            yield self
        finally:
            await self.close()


# =============================================================================
# Singleton Instances
# =============================================================================

_apps_script_client = GoogleAppsScriptClient()
_async_apps_script_client = AsyncGoogleAppsScriptClient()


def get_apps_script_client() -> GoogleAppsScriptClient:
    """Get singleton sync client instance."""
    return _apps_script_client


def get_async_apps_script_client() -> AsyncGoogleAppsScriptClient:
    """Get singleton async client instance."""
    return _async_apps_script_client


# =============================================================================
# Module Exports
# =============================================================================

__all__ = [
    "CLIENT_VERSION",
    "RequestMethod",
    "TokenTransport",
    "CircuitState",
    "FailureType",
    "MarketCategory",
    "AppsScriptConfig",
    "AppsScriptResult",
    "PageSpec",
    "SymbolNormalizer",
    "split_tickers_by_market",
    "normalize_page_key",
    "resolve_page_spec",
    "prune_none",
    "now_utc_iso",
    "GoogleAppsScriptClient",
    "AsyncGoogleAppsScriptClient",
    "get_apps_script_client",
    "get_async_apps_script_client",
    "PAGE_SPECS",
]
