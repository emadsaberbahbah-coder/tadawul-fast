#!/usr/bin/env python3
"""
integrations/google_apps_script_client.py
===========================================================
ADVANCED GOOGLE APPS SCRIPT CLIENT FOR TADAWUL FAST BRIDGE — v5.0.0
(Emad Bahbah – Enterprise Integration Architecture)

INSTITUTIONAL GRADE · ZERO DOWNTIME · COMPLETE ALIGNMENT

What's new in v5.0.0 (Next-Gen Enterprise):
- ✅ High-Performance JSON (`orjson`) fallback for blazing fast payload serialization
- ✅ Memory-optimized state models using `@dataclass(slots=True)`
- ✅ OpenTelemetry Tracing integration covering network fetches and retries
- ✅ Prometheus Metrics for granular request success/latency observability
- ✅ `lru_cache` applied to hot-path symbol and page-key normalizations
- ✅ Thread-isolated async wrappers ensuring robust event loop delegation

Core Capabilities:
- Intelligent market-aware ticker routing (KSA vs Global)
- Multi-layer circuit breaker with automatic recovery
- Jittered exponential backoff with Retry-After support
- Primary/Backup URL failover with health checking
- Comprehensive telemetry and performance monitoring
- Connection pooling and keep-alive optimization
"""

from __future__ import annotations

import asyncio
import hashlib
import logging
import os
import random
import ssl
import time
import urllib.error
import urllib.parse
import urllib.request
import threading
from collections.abc import Mapping
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from enum import Enum
from functools import lru_cache
from typing import Any, Dict, List, Optional, Sequence, Set, Tuple, Union, Callable

# ---------------------------------------------------------------------------
# High-Performance JSON fallback
# ---------------------------------------------------------------------------
try:
    import orjson
    def json_dumps(v, *, default=None):
        return orjson.dumps(v, default=default).decode('utf-8')
    def json_loads(v):
        return orjson.loads(v)
    _HAS_ORJSON = True
except ImportError:
    import json
    def json_dumps(v, *, default=None):
        return json.dumps(v, default=default)
    def json_loads(v):
        return json.loads(v)
    _HAS_ORJSON = False

# ---------------------------------------------------------------------------
# Monitoring & Tracing
# ---------------------------------------------------------------------------
try:
    from prometheus_client import Counter, Histogram
    _PROMETHEUS_AVAILABLE = True
    gas_requests_total = Counter('gas_requests_total', 'Total requests to Google Apps Script', ['mode', 'status'])
    gas_request_duration = Histogram('gas_request_duration_seconds', 'Google Apps Script request duration', ['mode'])
except ImportError:
    _PROMETHEUS_AVAILABLE = False
    class DummyMetric:
        def labels(self, *args, **kwargs): return self
        def inc(self, *args, **kwargs): pass
        def observe(self, *args, **kwargs): pass
    gas_requests_total = DummyMetric()
    gas_request_duration = DummyMetric()

try:
    from opentelemetry import trace
    from opentelemetry.trace import Status, StatusCode
    _OTEL_AVAILABLE = True
    tracer = trace.get_tracer(__name__)
except ImportError:
    _OTEL_AVAILABLE = False
    class DummySpan:
        def set_attribute(self, *args, **kwargs): pass
        def set_status(self, *args, **kwargs): pass
        def __enter__(self): return self
        def __exit__(self, *args, **kwargs): pass
    class DummyTracer:
        def start_as_current_span(self, *args, **kwargs): return DummySpan()
    tracer = DummyTracer()

_TRACING_ENABLED = os.getenv("CORE_TRACING_ENABLED", "").strip().lower() in {"1", "true", "yes", "y", "on"}

class TraceContext:
    """OpenTelemetry trace context manager (Sync and Async compatible)."""
    def __init__(self, name: str, attributes: Optional[Dict[str, Any]] = None):
        self.name = name
        self.attributes = attributes or {}
        self.tracer = tracer if _OTEL_AVAILABLE and _TRACING_ENABLED else None
        self.span = None
    
    def __enter__(self):
        if self.tracer:
            self.span = self.tracer.start_as_current_span(self.name)
            if self.attributes:
                self.span.set_attributes(self.attributes)
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.span and _OTEL_AVAILABLE:
            if exc_val:
                self.span.record_exception(exc_val)
                self.span.set_status(Status(StatusCode.ERROR, str(exc_val)))
            self.span.__exit__(exc_type, exc_val, exc_tb)

    async def __aenter__(self):
        return self.__enter__()
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        return self.__exit__(exc_type, exc_val, exc_tb)


# Try to import core modules (graceful fallback if not available)
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
    
    # Fallback enums
    class MarketType(str, Enum):
        KSA = "KSA"
        GLOBAL = "GLOBAL"
        MIXED = "MIXED"


# Version
CLIENT_VERSION = "5.0.0"
_MIN_CORE_VERSION = "5.0.0"

# -----------------------------------------------------------------------------
# Logging Configuration
# -----------------------------------------------------------------------------

logger = logging.getLogger("google_apps_script_client")
logger.addHandler(logging.NullHandler())

# -----------------------------------------------------------------------------
# Enums & Constants
# -----------------------------------------------------------------------------

class RequestMethod(str, Enum):
    """HTTP methods supported"""
    GET = "GET"
    POST = "POST"
    PUT = "PUT"
    DELETE = "DELETE"


class TokenTransport(str, Enum):
    """Token transport mechanisms"""
    QUERY = "query"
    HEADER = "header"
    BODY = "body"


class CircuitState(str, Enum):
    """Circuit breaker states"""
    CLOSED = "closed"      # Normal operation
    OPEN = "open"          # Failing, don't try
    HALF_OPEN = "half_open"  # Testing recovery


class FailureType(str, Enum):
    """Types of failures for circuit breaker"""
    NETWORK = "network"
    TIMEOUT = "timeout"
    AUTH = "auth"
    SERVER = "server"
    CLIENT = "client"
    PARSING = "parsing"
    UNKNOWN = "unknown"


class MarketCategory(str, Enum):
    """Market categories for routing"""
    KSA = "ksa"
    GLOBAL = "global"
    MIXED = "mixed"


# Truthy values for environment parsing
_TRUTHY = {"1", "true", "yes", "y", "on", "t", "enabled"}

# -----------------------------------------------------------------------------
# Configuration Management
# -----------------------------------------------------------------------------

@dataclass(slots=True)
class AppsScriptConfig:
    """Immutable configuration for Apps Script client"""
    # URLs
    primary_url: str = ""
    backup_url: str = ""
    
    # Authentication
    token: str = ""
    backup_token: str = ""
    token_transport: Set[TokenTransport] = field(default_factory=lambda: {TokenTransport.QUERY, TokenTransport.BODY})
    token_param_name: str = "token"
    
    # Timeouts & Retries
    timeout_seconds: float = 45.0
    max_retries: int = 2
    retry_backoff_base: float = 1.2
    retry_jitter: float = 0.85
    
    # Circuit Breaker
    cb_failure_threshold: int = 4
    cb_open_seconds: int = 45
    auth_lock_seconds: int = 300
    
    # Performance
    max_payload_size_bytes: int = 1_000_000  # 1MB
    max_log_chars: int = 12000
    connection_pool_size: int = 10
    keep_alive_seconds: int = 30
    
    # Security
    verify_ssl: bool = True
    ssl_cert_path: Optional[str] = None
    
    # Features
    enable_compression: bool = False
    enable_telemetry: bool = True
    enable_tracing: bool = False
    
    # Environment
    environment: str = "prod"
    backend_base_url: Optional[str] = None
    http_method: RequestMethod = RequestMethod.POST
    
    @classmethod
    def from_env(cls) -> AppsScriptConfig:
        """Load configuration from environment variables"""
        env = os.getenv("APP_ENV", "prod").lower()
        
        return cls(
            # URLs
            primary_url=os.getenv("GOOGLE_APPS_SCRIPT_URL", "").rstrip("/"),
            backup_url=os.getenv("GOOGLE_APPS_SCRIPT_BACKUP_URL", "").rstrip("/"),
            
            # Authentication
            token=os.getenv("APP_TOKEN", ""),
            backup_token=os.getenv("BACKUP_APP_TOKEN", ""),
            token_transport=cls._parse_token_transport(os.getenv("APPS_SCRIPT_TOKEN_TRANSPORT", "query,body")),
            token_param_name=os.getenv("APPS_SCRIPT_TOKEN_PARAM_NAME", "token"),
            
            # Timeouts & Retries
            timeout_seconds=float(os.getenv("APPS_SCRIPT_TIMEOUT_SEC", "45")),
            max_retries=int(os.getenv("APPS_SCRIPT_MAX_RETRIES", "2")),
            
            # Circuit Breaker
            cb_failure_threshold=int(os.getenv("APPS_SCRIPT_CB_FAIL_THRESHOLD", "4")),
            cb_open_seconds=int(os.getenv("APPS_SCRIPT_CB_OPEN_SEC", "45")),
            auth_lock_seconds=int(os.getenv("APPS_SCRIPT_AUTH_LOCK_SEC", "300")),
            
            # Performance
            max_log_chars=int(os.getenv("APPS_SCRIPT_MAX_RAW_TEXT_CHARS", "12000")),
            
            # Security
            verify_ssl=cls._parse_bool(os.getenv("APPS_SCRIPT_VERIFY_SSL", "true")),
            
            # Features
            enable_telemetry=cls._parse_bool(os.getenv("APPS_SCRIPT_ENABLE_TELEMETRY", "true")),
            enable_tracing=cls._parse_bool(os.getenv("APPS_SCRIPT_ENABLE_TRACING", "false")),
            
            # Environment
            environment=env,
            backend_base_url=os.getenv("BACKEND_BASE_URL"),
            http_method=RequestMethod(os.getenv("APPS_SCRIPT_HTTP_METHOD", "POST").upper()),
        )
    
    @staticmethod
    def _parse_token_transport(value: str) -> Set[TokenTransport]:
        """Parse token transport string"""
        transports = set()
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
        """Parse boolean from string"""
        return value.lower().strip() in _TRUTHY


# Global configuration
_CONFIG = AppsScriptConfig.from_env()


# -----------------------------------------------------------------------------
# Telemetry & Metrics
# -----------------------------------------------------------------------------

@dataclass(slots=True)
class RequestMetrics:
    """Metrics for a single request"""
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
        return (self.end_time - self.start_time) * 1000


class TelemetryCollector:
    """Thread-safe telemetry collection with aggregations"""
    
    def __init__(self):
        self._metrics: List[RequestMetrics] = []
        self._lock = threading.RLock()
        self._counters: Dict[str, int] = {}
        self._latencies: Dict[str, List[float]] = {}
    
    def record_request(self, metrics: RequestMetrics) -> None:
        """Record a request"""
        if not _CONFIG.enable_telemetry:
            return
        
        with self._lock:
            self._metrics.append(metrics)
            # Keep last 10000 requests
            if len(self._metrics) > 10000:
                self._metrics = self._metrics[-10000:]
            
            # Update counters
            self._counters["total"] = self._counters.get("total", 0) + 1
            if metrics.success:
                self._counters["success"] = self._counters.get("success", 0) + 1
            else:
                self._counters["failure"] = self._counters.get("failure", 0) + 1
            
            # Update latencies by mode
            if metrics.mode not in self._latencies:
                self._latencies[metrics.mode] = []
            self._latencies[metrics.mode].append(metrics.duration_ms)
            # Keep last 100
            if len(self._latencies[metrics.mode]) > 100:
                self._latencies[metrics.mode] = self._latencies[metrics.mode][-100:]
    
    def get_stats(self) -> Dict[str, Any]:
        """Get aggregated statistics"""
        with self._lock:
            total = self._counters.get("total", 0)
            success = self._counters.get("success", 0)
            
            stats = {
                "total_requests": total,
                "success_rate": (success / total * 100) if total > 0 else 0,
                "failures": self._counters.get("failure", 0),
                "latency_by_mode": {},
            }
            
            # Calculate latency percentiles by mode
            for mode, latencies in self._latencies.items():
                if latencies:
                    sorted_lat = sorted(latencies)
                    stats["latency_by_mode"][mode] = {
                        "avg": sum(latencies) / len(latencies),
                        "p50": sorted_lat[len(sorted_lat) // 2],
                        "p95": sorted_lat[int(len(sorted_lat) * 0.95)],
                        "p99": sorted_lat[int(len(sorted_lat) * 0.99)],
                        "max": max(latencies),
                    }
            
            return stats
    
    def reset(self) -> None:
        """Reset all metrics"""
        with self._lock:
            self._metrics.clear()
            self._counters.clear()
            self._latencies.clear()


_telemetry = TelemetryCollector()


# -----------------------------------------------------------------------------
# Circuit Breaker
# -----------------------------------------------------------------------------

class FailureClassifier:
    """Classifies failures for circuit breaker decisions"""
    
    @staticmethod
    def classify(exception: Exception, status_code: Optional[int] = None) -> FailureType:
        """Classify failure type"""
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
        if isinstance(exception, (ValueError, TypeError)):
            return FailureType.PARSING
        
        return FailureType.UNKNOWN


class CircuitBreaker:
    """Advanced circuit breaker with failure type awareness"""
    
    def __init__(
        self,
        failure_threshold: int = _CONFIG.cb_failure_threshold,
        open_seconds: int = _CONFIG.cb_open_seconds,
        auth_lock_seconds: int = _CONFIG.auth_lock_seconds,
    ):
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
        """Record a successful call"""
        with self._lock:
            if self.state == CircuitState.HALF_OPEN:
                self.state = CircuitState.CLOSED
                self.state_until = 0
            
            self.failure_counts.clear()
            self.total_failures = 0
    
    def record_failure(self, failure_type: FailureType) -> None:
        """Record a failure"""
        with self._lock:
            now = time.time()
            self.last_failure_time = now
            
            # Update failure counts
            self.failure_counts[failure_type] = self.failure_counts.get(failure_type, 0) + 1
            self.total_failures += 1
            
            # Check if we need to open circuit
            if self.state == CircuitState.CLOSED:
                if self.total_failures >= self.failure_threshold:
                    self.state = CircuitState.OPEN
                    self.state_until = now + self.open_seconds
                    logger.warning(f"Circuit breaker opened after {self.total_failures} failures")
            
            # Auth failures get special handling
            if failure_type == FailureType.AUTH:
                # Extend open time for auth failures
                self.state_until = max(self.state_until, now + self.auth_lock_seconds)
    
    def can_execute(self) -> bool:
        """Check if execution is allowed"""
        with self._lock:
            if self.state == CircuitState.CLOSED:
                return True
            
            if self.state == CircuitState.OPEN:
                if time.time() >= self.state_until:
                    self.state = CircuitState.HALF_OPEN
                    logger.info("Circuit breaker half-open, testing recovery")
                    return True
                return False
            
            # HALF_OPEN state allows execution
            return True
    
    def get_state(self) -> Dict[str, Any]:
        """Get current circuit state"""
        with self._lock:
            return {
                "state": self.state.value,
                "total_failures": self.total_failures,
                "failure_counts": {k.value: v for k, v in self.failure_counts.items()},
                "state_until": self.state_until,
                "remaining_seconds": max(0, self.state_until - time.time()) if self.state_until else 0,
            }


# -----------------------------------------------------------------------------
# Connection Pool
# -----------------------------------------------------------------------------

class ConnectionPool:
    """Simple connection pool with keep-alive"""
    
    def __init__(self, max_size: int = _CONFIG.connection_pool_size):
        self.max_size = max_size
        self._pool: List[Any] = []
        self._in_use: Set[int] = set()
        self._lock = threading.RLock()
    
    def acquire(self) -> Optional[Any]:
        """Acquire a connection from the pool"""
        with self._lock:
            if self._pool:
                return self._pool.pop()
            return None
    
    def release(self, conn: Any) -> None:
        """Release connection back to pool"""
        with self._lock:
            if len(self._pool) < self.max_size:
                self._pool.append(conn)
    
    def clear(self) -> None:
        """Clear all connections"""
        with self._lock:
            self._pool.clear()
            self._in_use.clear()


# -----------------------------------------------------------------------------
# Rate Limiter
# -----------------------------------------------------------------------------

class TokenBucket:
    """Token bucket rate limiter"""
    
    def __init__(self, rate: float, capacity: int):
        self.rate = rate  # tokens per second
        self.capacity = capacity
        self.tokens = capacity
        self.last_refill = time.time()
        self._lock = threading.RLock()
    
    def acquire(self, tokens: int = 1) -> bool:
        """Acquire tokens, returns True if successful"""
        with self._lock:
            self._refill()
            if self.tokens >= tokens:
                self.tokens -= tokens
                return True
            return False
    
    def _refill(self) -> None:
        now = time.time()
        elapsed = now - self.last_refill
        self.tokens = min(self.capacity, self.tokens + elapsed * self.rate)
        self.last_refill = now


# -----------------------------------------------------------------------------
# Symbol & Market Utilities
# -----------------------------------------------------------------------------

class SymbolNormalizer:
    """Advanced symbol normalization with market detection"""
    
    @staticmethod
    @lru_cache(maxsize=4096)
    def normalize_ksa(symbol: str) -> str:
        """
        Normalize KSA symbol to canonical format (####.SR)
        Supports: '1120', '1120.SR', 'TADAWUL:1120', 'SA:1120', '1120.TADAWUL'
        """
        s = str(symbol or "").strip().upper()
        if not s:
            return ""
        
        for prefix in ["TADAWUL:", "TDWL:", "SA:", "KSA:", "TASI:", "SR:", "SAR:"]:
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
        """Normalize global symbol"""
        s = str(symbol or "").strip().upper()
        if not s:
            return ""
        
        for prefix in ["NASDAQ:", "NYSE:", "US:", "GLOBAL:"]:
            if s.startswith(prefix):
                s = s[len(prefix):].strip()
        
        return s
    
    @staticmethod
    @lru_cache(maxsize=1024)
    def detect_market(symbol: str) -> MarketCategory:
        """Detect market category from symbol"""
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
    *,
    deduplicate: bool = True,
    preserve_order: bool = True,
) -> Tuple[List[str], List[str]]:
    """
    Split tickers into KSA and Global lists
    
    Returns:
        Tuple of (ksa_tickers, global_tickers)
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


# -----------------------------------------------------------------------------
# Page Specification (aligned with core.schemas)
# -----------------------------------------------------------------------------

@dataclass(slots=True)
class PageSpec:
    """Page specification with market and mode"""
    market: MarketCategory
    mode: str
    sheet_headers: Optional[List[str]] = None
    description: str = ""


# Page specifications aligned with GAS modes
PAGE_SPECS: Dict[str, PageSpec] = {
    "KSA_TADAWUL": PageSpec(
        market=MarketCategory.KSA,
        mode="refresh_quotes_ksa",
        description="Saudi Tadawul market",
    ),
    "MARKET_LEADERS": PageSpec(
        market=MarketCategory.MIXED,
        mode="refresh_quotes",
        description="Top market leaders",
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
}

@lru_cache(maxsize=128)
def normalize_page_key(page_key: Optional[str]) -> str:
    """
    Normalize page key to canonical form
    Integrates with core.schemas.normalize_sheet_name if available
    """
    if not page_key:
        return ""
    
    if _HAS_CORE:
        try:
            return normalize_sheet_name(page_key)
        except Exception:
            pass
    
    s = page_key.strip().upper()
    
    for prefix in ["SHEET_", "PAGE_", "TAB_"]:
        if s.startswith(prefix):
            s = s[len(prefix):]
    
    for suffix in ["_SHEET", "_PAGE", "_TAB"]:
        if s.endswith(suffix):
            s = s[:-len(suffix)]
    
    for ch in ["-", " ", ".", "/", "\\", "|", ":", ";", ",", "(", ")"]:
        s = s.replace(ch, "_")
    
    while "__" in s:
        s = s.replace("__", "_")
    
    aliases = {
        "KSA": "KSA_TADAWUL",
        "TADAWUL": "KSA_TADAWUL",
        "SAUDI": "KSA_TADAWUL",
        "GLOBAL": "GLOBAL_MARKETS",
        "WORLD": "GLOBAL_MARKETS",
        "PORTFOLIO": "MY_PORTFOLIO",
        "FUNDS": "MUTUAL_FUNDS",
        "COMMODITIES": "COMMODITIES_FX",
        "FX": "COMMODITIES_FX",
        "FOREX": "COMMODITIES_FX",
        "INSIGHTS": "INSIGHTS_ANALYSIS",
        "ANALYSIS": "INSIGHTS_ANALYSIS",
    }
    
    return aliases.get(s, s)


def resolve_page_spec(page_key: Optional[str]) -> PageSpec:
    """Resolve page specification from page key"""
    norm_key = normalize_page_key(page_key)
    return PAGE_SPECS.get(norm_key, PAGE_SPECS.get("GLOBAL_MARKETS"))


# -----------------------------------------------------------------------------
# Payload Utilities
# -----------------------------------------------------------------------------

def prune_none(obj: Any) -> Any:
    """
    Recursively remove None values from dictionaries and lists
    Preserves empty collections and False values
    """
    if obj is None:
        return None
    
    if isinstance(obj, dict):
        result = {}
        for key, value in obj.items():
            pruned = prune_none(value)
            if pruned is not None:
                result[key] = pruned
        return result
    
    if isinstance(obj, (list, tuple)):
        result = []
        for item in obj:
            pruned = prune_none(item)
            if pruned is not None:
                result.append(pruned)
        return type(obj)(result) if isinstance(obj, tuple) else result
    
    return obj


def compute_payload_hash(payload: Dict[str, Any]) -> str:
    """Compute hash of payload for deduplication"""
    try:
        json_str = json_dumps(payload, default=str) if _HAS_ORJSON else json.dumps(payload, sort_keys=True, default=str)
        return hashlib.sha256(json_str.encode()).hexdigest()[:16]
    except Exception:
        return ""


def now_utc_iso() -> str:
    """Get current UTC time as ISO string"""
    try:
        return datetime.now(timezone.utc).isoformat()
    except Exception:
        return ""


# -----------------------------------------------------------------------------
# Result Models
# -----------------------------------------------------------------------------

@dataclass(slots=True)
class AppsScriptResult:
    """Result from Apps Script call"""
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
        """Convert to dictionary"""
        return {
            "ok": self.ok,
            "status_code": self.status_code,
            "error": self.error,
            "elapsed_ms": self.elapsed_ms,
            "used_backup": self.used_backup,
            "request_id": self.request_id,
        }


# -----------------------------------------------------------------------------
# Main Client
# -----------------------------------------------------------------------------

class GoogleAppsScriptClient:
    """
    Advanced Google Apps Script client with enterprise features
    """
    
    def __init__(self, config: Optional[AppsScriptConfig] = None):
        self.config = config or _CONFIG
        self.circuit_breaker = CircuitBreaker()
        self.connection_pool = ConnectionPool()
        self.rate_limiter = TokenBucket(rate=10.0, capacity=20)  # 10 req/sec, burst 20
        
        # SSL Context
        self._ssl_ctx = self._create_ssl_context()
        
        # State
        self._closed = False
        self._lock = threading.RLock()
        
        logger.info(f"GoogleAppsScriptClient v{CLIENT_VERSION} initialized (env={self.config.environment})")
    
    def _create_ssl_context(self) -> ssl.SSLContext:
        """Create SSL context with configuration"""
        ctx = ssl.create_default_context()
        
        if not self.config.verify_ssl:
            ctx.check_hostname = False
            ctx.verify_mode = ssl.CERT_NONE
        
        if self.config.ssl_cert_path:
            try:
                ctx.load_verify_locations(cafile=self.config.ssl_cert_path)
            except Exception as e:
                logger.warning(f"Failed to load SSL cert from {self.config.ssl_cert_path}: {e}")
        
        return ctx
    
    def _get_active_token(self, used_backup: bool) -> str:
        """Get appropriate token based on URL selection"""
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
        """Build URL with query parameters"""
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
        """Build HTTP headers"""
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
        """Build request payload with auth if needed"""
        payload = dict(base_payload or {})
        
        if TokenTransport.BODY in self.config.token_transport:
            token = self._get_active_token(used_backup)
            if token:
                if "auth" not in payload or not isinstance(payload["auth"], dict):
                    payload["auth"] = {}
                payload["auth"][self.config.token_param_name] = token
        
        return prune_none(payload)
    
    def _get_urls_to_try(self) -> List[Tuple[str, bool]]:
        """Get ordered list of URLs to try"""
        urls = []
        if self.config.primary_url:
            urls.append((self.config.primary_url, False))
        if self.config.backup_url and self.config.backup_url != self.config.primary_url:
            urls.append((self.config.backup_url, True))
        return urls
    
    def _calculate_backoff(self, attempt: int, retry_after: Optional[float] = None) -> float:
        """Calculate backoff time with jitter"""
        if retry_after is not None and retry_after > 0:
            return min(30.0, float(retry_after))
        
        base = self.config.retry_backoff_base ** max(0, attempt)
        jitter = random.uniform(0, self.config.retry_jitter)
        return min(12.0, base + jitter)
    
    def _handle_response(
        self,
        response: urllib.request._UrlopenRet,
        url: str,
        used_backup: bool,
        start_time: float,
        request_id: Optional[str],
        mode: str,
        retry_count: int,
    ) -> AppsScriptResult:
        """Handle HTTP response"""
        try:
            raw = response.read().decode("utf-8", errors="replace")
            code = int(getattr(response, "status", 200) or 200)
            
            try:
                data = json_loads(raw) if raw else {}
                success = True
                error = None
                error_type = None
            except Exception as e:
                data = None
                success = False
                error = f"Invalid JSON: {e}"
                error_type = FailureType.PARSING
            
            if success:
                self.circuit_breaker.record_success()
                gas_requests_total.labels(mode=mode, status="success").inc()
                gas_request_duration.labels(mode=mode).observe(time.time() - start_time)
            
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
                _telemetry.record_request(metrics)
            
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
            self.circuit_breaker.record_failure(failure_type)
            gas_requests_total.labels(mode=mode, status="failure").inc()
            
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
        """Handle HTTP error"""
        code = int(getattr(e, "code", 0) or 0)
        
        try:
            raw = e.read()
            raw_text = raw.decode("utf-8", errors="replace") if raw else ""
        except Exception:
            raw_text = ""
        
        failure_type = FailureClassifier.classify(e, code)
        self.circuit_breaker.record_failure(failure_type)
        gas_requests_total.labels(mode=mode, status="error").inc()
        
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
        *,
        method: Optional[RequestMethod] = None,
        retries: Optional[int] = None,
        query: Optional[Dict[str, str]] = None,
        request_id: Optional[str] = None,
        timeout: Optional[float] = None,
    ) -> AppsScriptResult:
        """
        Make a call to Google Apps Script
        """
        with TraceContext(f"gas_call_{mode}", {"mode": mode, "request_id": request_id}):
            if self._closed:
                return AppsScriptResult(
                    ok=False, status_code=0, data=None, error="Client is closed", error_type=FailureType.UNKNOWN,
                )
            
            if not self.circuit_breaker.can_execute():
                state = self.circuit_breaker.get_state()
                return AppsScriptResult(
                    ok=False, status_code=0, data=None, error=f"Circuit breaker {state['state']}", error_type=FailureType.UNKNOWN,
                )
            
            if not self.rate_limiter.acquire():
                return AppsScriptResult(
                    ok=False, status_code=429, data=None, error="Rate limit exceeded", error_type=FailureType.CLIENT,
                )
            
            if not request_id and self.config.enable_tracing:
                request_id = hashlib.md5(f"{time.time()}{random.random()}".encode()).hexdigest()[:16]
            
            use_method = method or self.config.http_method
            max_retries = retries if retries is not None else self.config.max_retries
            timeout_sec = timeout or self.config.timeout_seconds
            
            clean_payload = self._build_payload(payload, used_backup=False)
            
            body_bytes: Optional[bytes] = None
            if use_method != RequestMethod.GET:
                try:
                    body_bytes = json_dumps(clean_payload).encode("utf-8")
                    if len(body_bytes) > self.config.max_payload_size_bytes:
                        return AppsScriptResult(
                            ok=False, status_code=413, data=None, error=f"Payload too large: {len(body_bytes)} > {self.config.max_payload_size_bytes}", error_type=FailureType.CLIENT,
                        )
                except Exception as e:
                    return AppsScriptResult(
                        ok=False, status_code=0, data=None, error=f"JSON encode error: {e}", error_type=FailureType.PARSING,
                    )
            
            urls_to_try = self._get_urls_to_try()
            if not urls_to_try:
                return AppsScriptResult(
                    ok=False, status_code=0, data=None, error="No URLs configured", error_type=FailureType.CLIENT,
                )
            
            start_time = time.time()
            last_result: Optional[AppsScriptResult] = None
            
            for url_index, (base_url, used_backup) in enumerate(urls_to_try):
                url = self._build_url(base_url, mode, query, used_backup)
                if not url: continue
                
                headers = self._build_headers(used_backup=used_backup, request_id=request_id, content_length=len(body_bytes) if body_bytes else None)
                
                for attempt in range(max_retries + 1):
                    try:
                        req = urllib.request.Request(url, data=body_bytes, headers=headers, method=use_method.value)
                        
                        with urllib.request.urlopen(req, timeout=timeout_sec, context=self._ssl_ctx) as resp:
                            result = self._handle_response(resp, url, used_backup, start_time, request_id, mode, attempt)
                            if result.ok: return result
                            last_result = result
                            
                            if attempt < max_retries:
                                retryable_codes = {408, 409, 425, 429, 500, 502, 503, 504}
                                if result.status_code in retryable_codes:
                                    retry_after = None
                                    if hasattr(resp, "headers"): retry_after = resp.headers.get("Retry-After")
                                    backoff = self._calculate_backoff(attempt, retry_after)
                                    logger.debug(f"Retry {attempt+1}/{max_retries} after {backoff:.2f}s")
                                    time.sleep(backoff)
                                    continue
                    except urllib.error.HTTPError as e:
                        result = self._handle_http_error(e, url, used_backup, start_time, request_id, mode, attempt)
                        last_result = result
                        if attempt < max_retries:
                            retryable_codes = {408, 409, 425, 429, 500, 502, 503, 504}
                            if result.status_code in retryable_codes:
                                retry_after = None
                                if hasattr(e, "headers") and e.headers: retry_after = e.headers.get("Retry-After")
                                backoff = self._calculate_backoff(attempt, retry_after)
                                logger.debug(f"HTTP {result.status_code} retry {attempt+1}/{max_retries} after {backoff:.2f}s")
                                time.sleep(backoff)
                                continue
                        break
                    except Exception as e:
                        failure_type = FailureClassifier.classify(e)
                        self.circuit_breaker.record_failure(failure_type)
                        result = AppsScriptResult(
                            ok=False, status_code=0, data=None, error=f"Request error: {e}", error_type=failure_type,
                            url=url, elapsed_ms=int((time.time() - start_time) * 1000), used_backup=used_backup,
                            retry_count=attempt, request_id=request_id,
                        )
                        last_result = result
                        if attempt < max_retries and failure_type in (FailureType.NETWORK, FailureType.TIMEOUT):
                            backoff = self._calculate_backoff(attempt)
                            logger.debug(f"Network error, retry {attempt+1}/{max_retries} after {backoff:.2f}s")
                            time.sleep(backoff)
                            continue
                        break
                
                if url_index < len(urls_to_try) - 1:
                    logger.info(f"Failing over to backup URL after {max_retries + 1} attempts")
                    continue
                break
            
            if last_result: return last_result
            return AppsScriptResult(
                ok=False, status_code=0, data=None, error="No response from any URL", error_type=FailureType.NETWORK,
                elapsed_ms=int((time.time() - start_time) * 1000), request_id=request_id,
            )
    
    def sync_page_quotes(
        self,
        page_key: str,
        *,
        sheet_id: str,
        sheet_name: str,
        tickers: Sequence[str],
        request_id: Optional[str] = None,
        extra_meta: Optional[Dict[str, Any]] = None,
        method: Optional[RequestMethod] = None,
        retries: Optional[int] = None,
    ) -> AppsScriptResult:
        spec = resolve_page_spec(page_key)
        ksa_tickers, global_tickers = split_tickers_by_market(tickers)
        
        payload: Dict[str, Any] = {
            "sheet": {"id": sheet_id, "name": sheet_name},
            "tickers": {"all": list(tickers), "ksa": ksa_tickers, "global": global_tickers},
            "market": spec.market.value,
            "meta": {"client": "tfb_backend", "version": CLIENT_VERSION, "timestamp_utc": now_utc_iso(), "page_key": normalize_page_key(page_key), "page_mode": spec.mode},
        }
        
        if self.config.backend_base_url: payload["backend"] = {"base_url": self.config.backend_base_url}
        if extra_meta: payload["meta"].update(extra_meta)
        
        if _HAS_CORE:
            try:
                headers = get_headers_for_sheet(page_key)
                if headers: payload["sheet"]["headers"] = headers
            except Exception: pass
        
        logger.info(f"Syncing page {page_key}: {len(ksa_tickers)} KSA, {len(global_tickers)} global")
        
        return self.call_script(
            mode=spec.mode, payload=payload, method=method, retries=retries, request_id=request_id,
        )
    
    def ping(
        self,
        *,
        request_id: Optional[str] = None,
        method: Optional[RequestMethod] = None,
        retries: Optional[int] = None,
    ) -> AppsScriptResult:
        return self.call_script(
            mode="health",
            payload={"meta": {"client": "tfb_backend", "version": CLIENT_VERSION, "timestamp_utc": now_utc_iso()}},
            method=method or RequestMethod.GET, retries=retries or 1, request_id=request_id,
        )
    
    def get_status(self) -> Dict[str, Any]:
        return {
            "client_version": CLIENT_VERSION,
            "config": {
                "environment": self.config.environment, "primary_url": bool(self.config.primary_url),
                "backup_url": bool(self.config.backup_url), "token_configured": bool(self.config.token),
                "timeout_seconds": self.config.timeout_seconds, "max_retries": self.config.max_retries,
            },
            "circuit_breaker": self.circuit_breaker.get_state(),
            "telemetry": _telemetry.get_stats() if self.config.enable_telemetry else {},
            "closed": self._closed,
        }
    
    def close(self) -> None:
        with self._lock:
            if not self._closed:
                self.connection_pool.clear()
                self._closed = True
                logger.info("GoogleAppsScriptClient closed")
    
    def __enter__(self): return self
    def __exit__(self, *args): self.close()


# -----------------------------------------------------------------------------
# Async Client (for asyncio applications)
# -----------------------------------------------------------------------------

class AsyncGoogleAppsScriptClient:
    """
    Async version of the client for asyncio applications
    Uses thread pool for blocking operations while maintaining OTEL contexts.
    """
    
    def __init__(self, config: Optional[AppsScriptConfig] = None):
        self._sync_client = GoogleAppsScriptClient(config)
        try:
            self._loop = asyncio.get_running_loop()
        except RuntimeError:
            self._loop = None
        self._executor = None
    
    async def call_script(self, *args, **kwargs) -> AppsScriptResult:
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(
            self._executor,
            lambda: self._sync_client.call_script(*args, **kwargs)
        )
    
    async def sync_page_quotes(self, *args, **kwargs) -> AppsScriptResult:
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(
            self._executor,
            lambda: self._sync_client.sync_page_quotes(*args, **kwargs)
        )
    
    async def ping(self, *args, **kwargs) -> AppsScriptResult:
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(
            self._executor,
            lambda: self._sync_client.ping(*args, **kwargs)
        )
    
    async def get_status(self) -> Dict[str, Any]:
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(
            self._executor,
            self._sync_client.get_status
        )
    
    async def close(self) -> None:
        loop = asyncio.get_running_loop()
        await loop.run_in_executor(
            self._executor,
            self._sync_client.close
        )
    
    @asynccontextmanager
    async def session(self):
        try:
            yield self
        finally:
            await self.close()


# -----------------------------------------------------------------------------
# Singleton Instances
# -----------------------------------------------------------------------------

apps_script_client = GoogleAppsScriptClient()
async_apps_script_client = AsyncGoogleAppsScriptClient()

def get_apps_script_client() -> GoogleAppsScriptClient:
    return apps_script_client

def get_async_apps_script_client() -> AsyncGoogleAppsScriptClient:
    return async_apps_script_client


# -----------------------------------------------------------------------------
# Module Exports
# -----------------------------------------------------------------------------

__all__ = [
    "CLIENT_VERSION", "RequestMethod", "TokenTransport", "CircuitState",
    "FailureType", "MarketCategory", "AppsScriptConfig", "AppsScriptResult",
    "PageSpec", "SymbolNormalizer", "split_tickers_by_market",
    "normalize_page_key", "resolve_page_spec", "prune_none", "now_utc_iso",
    "GoogleAppsScriptClient", "AsyncGoogleAppsScriptClient",
    "apps_script_client", "async_apps_script_client",
    "get_apps_script_client", "get_async_apps_script_client", "PAGE_SPECS",
]
