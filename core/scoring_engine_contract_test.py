#!/usr/bin/env python3
"""
core/yahoo_chart_provider.py
===========================================================
ADVANCED COMPATIBILITY SHIM + REPO HYGIENE — v6.0.0
(Emad Bahbah – Production Architecture)

INSTITUTIONAL GRADE · ZERO DOWNTIME · COMPLETE BACKWARD COMPATIBILITY

What's new in v6.0.0 (Quantum Edition):
- ✅ **Singleflight Coalescing**: Prevents import storms by deduplicating concurrent provider resolution.
- ✅ **Full Jitter Exponential Backoff**: Protects legacy fallback mechanisms during high-concurrency spikes.
- ✅ **Advanced Circuit Breaker**: Progressive timeout scaling and strict half-open request limits.
- ✅ **Structural Logging**: Native `structlog` integration for Datadog/ELK ingestion when `JSON_LOGS=true`.
- ✅ **Hygiene Compliant**: Strictly zero `print()` statements; completely relies on `sys.stdout.write` and `rich`.
- ✅ **Memory Optimization**: Applied `@dataclass(slots=True)` to telemetry and provider models.
- ✅ **High-Performance JSON**: Standard `orjson` fallback integration.

Purpose
- Production-safe compatibility layer between legacy imports and canonical provider.
- Zero-downtime migration path for provider restructuring.
- Complete backward compatibility with all historical function signatures.

Why This Exists
The canonical Yahoo Chart provider now lives at:
    core/providers/yahoo_chart_provider.py

This top-level module MUST remain valid Python forever because:
    - Hundreds of legacy imports may still do `import core.yahoo_chart_provider`
    - Cron jobs, notebooks, and deployed services may have hard dependencies
"""

from __future__ import annotations

import asyncio
import functools
import inspect
import logging
import os
import random
import sys
import time
import warnings
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from functools import lru_cache
from typing import Any, Callable, Dict, List, Optional, Tuple, Union, Awaitable, TypeVar, cast

# ---------------------------------------------------------------------------
# High-Performance JSON fallback
# ---------------------------------------------------------------------------
try:
    import orjson
    def json_dumps(v: Any, *, default=None): return orjson.dumps(v, default=default).decode('utf-8')
    def json_loads(v: Union[str, bytes]): return orjson.loads(v)
    _HAS_ORJSON = True
except ImportError:
    import json
    def json_dumps(v: Any, *, default=None): return json.dumps(v, default=default)
    def json_loads(v: Union[str, bytes]): return json.loads(v)
    _HAS_ORJSON = False

# ---------------------------------------------------------------------------
# Rich UI
# ---------------------------------------------------------------------------
try:
    from rich.console import Console
    from rich.table import Table
    from rich.panel import Panel
    _RICH_AVAILABLE = True
    console = Console()
except ImportError:
    _RICH_AVAILABLE = False
    console = None

# ---------------------------------------------------------------------------
# Logging Configuration
# ---------------------------------------------------------------------------
if os.getenv("JSON_LOGS", "false").lower() == "true":
    try:
        import structlog
        structlog.configure(
            processors=[
                structlog.processors.TimeStamper(fmt="iso"),
                structlog.processors.add_log_level,
                structlog.processors.JSONRenderer(),
            ],
            logger_factory=structlog.PrintLoggerFactory(),
        )
        logger = structlog.get_logger("shim.yahoo")
    except ImportError:
        logging.basicConfig(level=logging.INFO)
        logger = logging.getLogger("shim.yahoo")
else:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)8s | %(name)s | %(message)s")
    logger = logging.getLogger("shim.yahoo")

# ---------------------------------------------------------------------------
# Monitoring & Tracing
# ---------------------------------------------------------------------------
try:
    from prometheus_client import Counter, Histogram, Gauge
    _PROMETHEUS_AVAILABLE = True
    shim_requests_total = Counter('shim_yahoo_requests_total', 'Total requests to Yahoo shim', ['function', 'status'])
    shim_request_duration = Histogram('shim_yahoo_duration_seconds', 'Yahoo shim request duration', ['function'])
    shim_circuit_breaker = Gauge('shim_yahoo_circuit_breaker_state', 'Shim CB State (0=Closed, 1=Half-Open, 2=Open)')
except ImportError:
    _PROMETHEUS_AVAILABLE = False
    class DummyMetric:
        def labels(self, *args, **kwargs): return self
        def inc(self, *args, **kwargs): pass
        def observe(self, *args, **kwargs): pass
        def set(self, *args, **kwargs): pass
    shim_requests_total = DummyMetric()
    shim_request_duration = DummyMetric()
    shim_circuit_breaker = DummyMetric()

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
        def record_exception(self, *args, **kwargs): pass
        def end(self): pass
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
            self.span.end()

    async def __aenter__(self):
        return self.__enter__()
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        return self.__exit__(exc_type, exc_val, exc_tb)

# Version
SHIM_VERSION = "6.0.0"
MIN_CANONICAL_VERSION = "0.4.0"

# -----------------------------------------------------------------------------
# Constants & Configuration
# -----------------------------------------------------------------------------

YAHOO_CHART_URL = "https://query1.finance.yahoo.com/v7/finance/quote"
DATA_SOURCE = "yahoo_chart"

class DataQuality(str, Enum):
    EXCELLENT = "EXCELLENT"
    HIGH = "HIGH"
    MEDIUM = "MEDIUM"
    LOW = "LOW"
    STALE = "STALE"
    ERROR = "ERROR"
    MISSING = "MISSING"
    PARTIAL = "PARTIAL"
    OK = "OK"

class ShimConfig:
    CACHE_TTL_SECONDS = 300
    CIRCUIT_BREAKER_THRESHOLD = 3
    CIRCUIT_BREAKER_TIMEOUT = 60
    CIRCUIT_BREAKER_HALF_OPEN_LIMIT = 2
    ENABLE_TELEMETRY = True
    LOG_LEVEL = logging.INFO
    FALLBACK_ON_ERROR = True

# -----------------------------------------------------------------------------
# Resiliency Patterns: SingleFlight & Full Jitter
# -----------------------------------------------------------------------------

class SingleFlight:
    """Deduplicate concurrent requests for the same key to prevent cache/import stampedes."""
    def __init__(self):
        self._futures: Dict[str, asyncio.Future] = {}
        self._lock = asyncio.Lock()

    async def execute(self, key: str, coro_fn: Callable[[], Awaitable[Any]]) -> Any:
        async with self._lock:
            if key in self._futures:
                return await self._futures[key]
            fut = asyncio.get_running_loop().create_future()
            self._futures[key] = fut

        try:
            result = await coro_fn()
            if not fut.done():
                fut.set_result(result)
            return result
        except Exception as e:
            if not fut.done():
                fut.set_exception(e)
            raise
        finally:
            async with self._lock:
                self._futures.pop(key, None)

class FullJitterBackoff:
    """Safe retry mechanism implementing AWS Full Jitter Backoff."""
    def __init__(self, max_retries: int = 3, base_delay: float = 1.0, max_delay: float = 30.0):
        self.max_retries = max_retries
        self.base_delay = base_delay
        self.max_delay = max_delay

    async def execute_async(self, func: Callable, *args, **kwargs) -> Any:
        for attempt in range(self.max_retries):
            try:
                return await func(*args, **kwargs)
            except Exception as e:
                if attempt == self.max_retries - 1:
                    raise
                temp = min(self.max_delay, self.base_delay * (2 ** attempt))
                await asyncio.sleep(random.uniform(0, temp))

# -----------------------------------------------------------------------------
# Telemetry & Metrics
# -----------------------------------------------------------------------------

@dataclass(slots=True)
class CallMetrics:
    """Metrics for a single function call"""
    function_name: str
    start_time: float
    end_time: float
    success: bool
    error_type: Optional[str] = None
    duration_ms: float = field(init=False)
    
    def __post_init__(self):
        self.duration_ms = (self.end_time - self.start_time) * 1000

class TelemetryCollector:
    """Thread-safe telemetry collection"""
    def __init__(self):
        self._calls: List[CallMetrics] = []
        self._lock = asyncio.Lock()
    
    async def record_call(self, metrics: CallMetrics) -> None:
        if not ShimConfig.ENABLE_TELEMETRY: return
        async with self._lock:
            self._calls.append(metrics)
            if len(self._calls) > 1000:
                self._calls = self._calls[-1000:]
    
    def get_stats(self) -> Dict[str, Any]:
        if not self._calls: return {}
        total = len(self._calls)
        successes = sum(1 for c in self._calls if c.success)
        failures = total - successes
        durations = [c.duration_ms for c in self._calls]
        sorted_durations = sorted(durations)
        
        return {
            "total_calls": total,
            "success_rate": successes / total if total > 0 else 0,
            "failures": failures,
            "avg_duration_ms": sum(durations) / total if total > 0 else 0,
            "p50_duration_ms": sorted_durations[len(sorted_durations) // 2] if total > 0 else 0,
            "p95_duration_ms": sorted_durations[int(total * 0.95)] if total > 5 else 0,
            "p99_duration_ms": sorted_durations[int(total * 0.99)] if total > 100 else sorted_durations[-1] if total > 0 else 0,
            "by_function": self._group_by_function(),
        }
    
    def _group_by_function(self) -> Dict[str, Dict[str, Any]]:
        result = {}
        for call in self._calls:
            if call.function_name not in result:
                result[call.function_name] = {"calls": 0, "successes": 0, "failures": 0, "total_duration": 0}
            stats = result[call.function_name]
            stats["calls"] += 1
            if call.success: stats["successes"] += 1
            else: stats["failures"] += 1
            stats["total_duration"] += call.duration_ms
        
        for stats in result.values():
            stats["avg_duration_ms"] = stats["total_duration"] / stats["calls"]
            stats["success_rate"] = stats["successes"] / stats["calls"]
            del stats["total_duration"]
        return result

_telemetry = TelemetryCollector()

def track_telemetry(func: Callable) -> Callable:
    @functools.wraps(func)
    async def wrapper(*args, **kwargs):
        start = time.time()
        function_name = func.__name__
        success, error_type = False, None
        try:
            result = await func(*args, **kwargs)
            success = True
            return result
        except Exception as e:
            error_type = e.__class__.__name__
            raise
        finally:
            metrics = CallMetrics(function_name=function_name, start_time=start, end_time=time.time(), success=success, error_type=error_type)
            await _telemetry.record_call(metrics)
    return wrapper

# -----------------------------------------------------------------------------
# Advanced Circuit Breaker
# -----------------------------------------------------------------------------

class CircuitState(str, Enum):
    CLOSED = "closed"
    OPEN = "open"
    HALF_OPEN = "half_open"

class AdvancedCircuitBreaker:
    def __init__(self, threshold: int = ShimConfig.CIRCUIT_BREAKER_THRESHOLD, timeout: int = ShimConfig.CIRCUIT_BREAKER_TIMEOUT, half_open_limit: int = ShimConfig.CIRCUIT_BREAKER_HALF_OPEN_LIMIT):
        self.threshold = threshold
        self.timeout = timeout
        self.half_open_limit = half_open_limit
        self.state = CircuitState.CLOSED
        self.failure_count = 0
        self.last_failure_time: Optional[float] = None
        self.half_open_calls = 0
        self.success_count = 0
        self._lock = asyncio.Lock()
        shim_circuit_breaker.set(0)
    
    async def call(self, func: Callable, *args, **kwargs) -> Any:
        async with self._lock:
            if self.state == CircuitState.OPEN:
                if time.time() - self.last_failure_time > self.timeout:
                    self.state = CircuitState.HALF_OPEN
                    self.half_open_calls = 0
                    self.success_count = 0
                    shim_circuit_breaker.set(1)
                else:
                    raise CircuitBreakerOpenError(f"Circuit breaker is OPEN. {self.failure_count} failures recorded.")
            
            if self.state == CircuitState.HALF_OPEN:
                if self.half_open_calls >= self.half_open_limit:
                    raise CircuitBreakerOpenError("Circuit breaker HALF_OPEN capacity reached.")
                self.half_open_calls += 1
        
        try:
            result = await func(*args, **kwargs) if asyncio.iscoroutinefunction(func) else func(*args, **kwargs)
            await self._record_success()
            return result
        except Exception as e:
            await self._record_failure()
            raise
    
    async def _record_success(self):
        async with self._lock:
            if self.state == CircuitState.HALF_OPEN:
                self.success_count += 1
                if self.success_count >= 2:
                    self.state = CircuitState.CLOSED
                    self.failure_count = 0
                    shim_circuit_breaker.set(0)
            else:
                self.failure_count = 0
    
    async def _record_failure(self):
        async with self._lock:
            self.failure_count += 1
            self.last_failure_time = time.time()
            if self.state == CircuitState.CLOSED and self.failure_count >= self.threshold:
                self.state = CircuitState.OPEN
                shim_circuit_breaker.set(2)
            elif self.state == CircuitState.HALF_OPEN:
                self.state = CircuitState.OPEN
                self.timeout = min(300.0, self.timeout * 1.5) # Progressive scaling
                shim_circuit_breaker.set(2)

class CircuitBreakerOpenError(Exception): pass


# -----------------------------------------------------------------------------
# Provider Cache with SingleFlight Protection
# -----------------------------------------------------------------------------

@dataclass(slots=True)
class ProviderInfo:
    module: Any
    version: str
    functions: Dict[str, Callable]
    available: bool
    last_check: float
    error: Optional[str] = None


class ProviderCache:
    def __init__(self, ttl: int = ShimConfig.CACHE_TTL_SECONDS):
        self.ttl = ttl
        self._provider: Optional[ProviderInfo] = None
        self._lock = asyncio.Lock()
        self._circuit_breaker = AdvancedCircuitBreaker()
        self._singleflight = SingleFlight()
    
    async def get_provider(self) -> Optional[ProviderInfo]:
        async with self._lock:
            if self._is_cache_valid():
                return self._provider
        
        # Cache expired, refresh safely with singleflight
        self._provider = await self._singleflight.execute("load_provider", self._load_provider)
        return self._provider
    
    def _is_cache_valid(self) -> bool:
        if not self._provider: return False
        return (time.time() - self._provider.last_check) < self.ttl
    
    async def _load_provider(self) -> Optional[ProviderInfo]:
        with TraceContext("shim_load_provider"):
            try:
                return await self._circuit_breaker.call(self._import_provider)
            except Exception as e:
                return ProviderInfo(module=None, version="unknown", functions={}, available=False, last_check=time.time(), error=str(e))
    
    async def _import_provider(self) -> ProviderInfo:
        try:
            import core.providers.yahoo_chart_provider as canonical
            version = getattr(canonical, "PROVIDER_VERSION", "unknown")
            functions = {name: getattr(canonical, name) for name in dir(canonical) if not name.startswith("_") and callable(getattr(canonical, name))}
            return ProviderInfo(module=canonical, version=version, functions=functions, available=True, last_check=time.time())
        except ImportError as e:
            raise ImportError(f"Canonical provider not available: {e}")
        except Exception as e:
            raise RuntimeError(f"Error loading canonical provider: {e}")

_provider_cache = ProviderCache()

# -----------------------------------------------------------------------------
# Signature Caching (Performance)
# -----------------------------------------------------------------------------

@lru_cache(maxsize=128)
def _get_cached_signature_parameters(func: Callable) -> Dict[str, inspect.Parameter]:
    try: return inspect.signature(func).parameters
    except Exception: return {}

# -----------------------------------------------------------------------------
# Shim Function Factory
# -----------------------------------------------------------------------------

T = TypeVar('T')

class ShimFunction:
    """Wrapper for shim functions with intelligent delegation and Jitter Backoff"""
    
    def __init__(self, name: str, default_factory: Callable[..., Any], canonical_name: Optional[str] = None, fallback_factory: Optional[Callable[..., Any]] = None):
        self.name = name
        self.default_factory = default_factory
        self.canonical_name = canonical_name or name
        self.fallback_factory = fallback_factory
        self._backoff = FullJitterBackoff(max_retries=2, base_delay=0.5, max_delay=5.0)
    
    async def __call__(self, *args, **kwargs) -> Any:
        start_time = time.time()
        with TraceContext(f"shim_{self.name}"):
            try:
                provider = await _provider_cache.get_provider()
                if provider and provider.available:
                    canonical_func = provider.functions.get(self.canonical_name)
                    if canonical_func:
                        adapted_args, adapted_kwargs = self._adapt_arguments(canonical_func, args, kwargs)
                        result = await self._backoff.execute_async(self._execute_canonical, canonical_func, adapted_args, adapted_kwargs)
                        result = self._post_process(result, kwargs.get("symbol", ""))
                        await self._record_metrics(start_time, True)
                        shim_requests_total.labels(function=self.name, status="success").inc()
                        shim_request_duration.labels(function=self.name).observe(time.time() - start_time)
                        return result
                
                if self.fallback_factory:
                    logger.warning(f"Using fallback for {self.name}")
                    result = await self._backoff.execute_async(self._execute_fallback, *args, **kwargs)
                    await self._record_metrics(start_time, True)
                    shim_requests_total.labels(function=self.name, status="fallback").inc()
                    return result
                
                result = self.default_factory(*args, **kwargs)
                if inspect.isawaitable(result): result = await result
                await self._record_metrics(start_time, True)
                shim_requests_total.labels(function=self.name, status="default").inc()
                return result
                
            except Exception as e:
                await self._record_metrics(start_time, False, e)
                shim_requests_total.labels(function=self.name, status="error").inc()
                if ShimConfig.FALLBACK_ON_ERROR and self.fallback_factory:
                    logger.error(f"Error in {self.name}: {e}, using fallback")
                    return await self._execute_fallback(*args, **kwargs)
                return self._create_error_payload(kwargs.get("symbol", ""), str(e), where=self.name)
    
    def _adapt_arguments(self, func: Callable, args: Tuple, kwargs: Dict) -> Tuple[Tuple, Dict]:
        try:
            parameters = _get_cached_signature_parameters(func)
            if not parameters: return args, kwargs
            filtered_kwargs = {name: value for name, value in kwargs.items() if name in parameters or any(p.kind == inspect.Parameter.VAR_KEYWORD for p in parameters.values())}
            return args, filtered_kwargs
        except Exception:
            return args, kwargs
    
    async def _execute_canonical(self, func: Callable, args: Tuple, kwargs: Dict) -> Any:
        result = func(*args, **kwargs)
        if inspect.isawaitable(result): return await result
        return result
    
    async def _execute_fallback(self, *args, **kwargs) -> Any:
        if not self.fallback_factory: raise NotImplementedError(f"No fallback for {self.name}")
        result = self.fallback_factory(*args, **kwargs)
        if inspect.isawaitable(result): return await result
        return result
    
    def _post_process(self, result: Any, symbol: str) -> Any:
        if isinstance(result, dict): return self._ensure_quote_shape(result, symbol)
        return result
    
    def _ensure_quote_shape(self, data: Dict[str, Any], symbol: str) -> Dict[str, Any]:
        result = dict(data)
        result.setdefault("symbol", symbol.upper() if symbol else "")
        result.setdefault("status", "ok" if not result.get("error") else "error")
        result.setdefault("data_source", DATA_SOURCE)
        result.setdefault("shim_version", SHIM_VERSION)
        
        if result.get("error"): result.setdefault("data_quality", DataQuality.ERROR.value)
        elif not result.get("data_quality"): result.setdefault("data_quality", DataQuality.OK.value)
        if not result.get("last_updated_utc"): result["last_updated_utc"] = self._now_utc_iso()
        return result
    
    def _create_error_payload(self, symbol: str, error: str, where: str = "", base: Optional[Dict] = None) -> Dict[str, Any]:
        payload = dict(base or {})
        payload.update({
            "status": "error", "symbol": symbol.upper() if symbol else "", "data_source": DATA_SOURCE,
            "data_quality": DataQuality.ERROR.value, "error": error, "where": where or self.name,
            "shim_version": SHIM_VERSION, "last_updated_utc": self._now_utc_iso(),
        })
        return payload
    
    def _now_utc_iso(self) -> str:
        try: return datetime.now(timezone.utc).isoformat()
        except Exception: return ""
    
    async def _record_metrics(self, start_time: float, success: bool, error: Optional[Exception] = None):
        if not ShimConfig.ENABLE_TELEMETRY: return
        metrics = CallMetrics(function_name=self.name, start_time=start_time, end_time=time.time(), success=success, error_type=error.__class__.__name__ if error else None)
        await _telemetry.record_call(metrics)

# -----------------------------------------------------------------------------
# Shim Function Implementations
# -----------------------------------------------------------------------------

def _default_get_quote(symbol: str, *args, **kwargs) -> Dict[str, Any]:
    return {
        "symbol": symbol.upper() if symbol else "", "status": "error", "error": "Canonical provider not available",
        "data_source": DATA_SOURCE, "data_quality": DataQuality.MISSING.value, "shim_version": SHIM_VERSION, "last_updated_utc": datetime.now(timezone.utc).isoformat(),
    }

async def _default_fetch_quote(symbol: str, *args, **kwargs) -> Dict[str, Any]:
    return _default_get_quote(symbol, *args, **kwargs)

async def _default_get_quote_patch(symbol: str, base: Optional[Dict[str, Any]] = None, *args, **kwargs) -> Dict[str, Any]:
    result = dict(base or {})
    result.update(await _default_fetch_quote(symbol, *args, **kwargs))
    return result

async def _default_fetch_history(symbol: str, *args, **kwargs) -> List[Dict[str, Any]]:
    return []

fetch_quote = ShimFunction(name="fetch_quote", default_factory=_default_fetch_quote, canonical_name="fetch_quote", fallback_factory=_default_fetch_quote)
get_quote = ShimFunction(name="get_quote", default_factory=_default_get_quote, canonical_name="get_quote", fallback_factory=_default_fetch_quote)
get_quote_patch = ShimFunction(name="get_quote_patch", default_factory=_default_get_quote_patch, canonical_name="get_quote_patch", fallback_factory=_default_get_quote_patch)
fetch_quote_patch = ShimFunction(name="fetch_quote_patch", default_factory=_default_get_quote_patch, canonical_name="fetch_quote_patch", fallback_factory=_default_get_quote_patch)
fetch_enriched_quote_patch = ShimFunction(name="fetch_enriched_quote_patch", default_factory=_default_get_quote_patch, canonical_name="fetch_enriched_quote_patch", fallback_factory=_default_get_quote_patch)
fetch_quote_and_enrichment_patch = ShimFunction(name="fetch_quote_and_enrichment_patch", default_factory=_default_get_quote_patch, canonical_name="fetch_quote_and_enrichment_patch", fallback_factory=_default_get_quote_patch)
fetch_quote_and_fundamentals_patch = ShimFunction(name="fetch_quote_and_fundamentals_patch", default_factory=_default_get_quote_patch, canonical_name="fetch_quote_and_fundamentals_patch", fallback_factory=_default_get_quote_patch)
yahoo_chart_quote = ShimFunction(name="yahoo_chart_quote", default_factory=_default_get_quote, canonical_name="yahoo_chart_quote", fallback_factory=_default_fetch_quote)
fetch_price_history = ShimFunction(name="fetch_price_history", default_factory=_default_fetch_history, canonical_name="fetch_price_history", fallback_factory=_default_fetch_history)
fetch_history = ShimFunction(name="fetch_history", default_factory=_default_fetch_history, canonical_name="fetch_history", fallback_factory=_default_fetch_history)
fetch_ohlc_history = ShimFunction(name="fetch_ohlc_history", default_factory=_default_fetch_history, canonical_name="fetch_ohlc_history", fallback_factory=_default_fetch_history)
fetch_history_patch = ShimFunction(name="fetch_history_patch", default_factory=_default_fetch_history, canonical_name="fetch_history_patch", fallback_factory=_default_fetch_history)
fetch_prices = ShimFunction(name="fetch_prices", default_factory=_default_fetch_history, canonical_name="fetch_prices", fallback_factory=_default_fetch_history)

# -----------------------------------------------------------------------------
# Client Lifecycle Management
# -----------------------------------------------------------------------------

class ClientManager:
    """Manages client lifecycle with connection pooling and async safety"""
    def __init__(self):
        self._client = None
        self._lock = asyncio.Lock()
        self._closed = False
    
    async def get_client(self):
        if self._closed: raise RuntimeError("Client manager is closed")
        async with self._lock:
            if self._client is None: self._client = await self._create_client()
            return self._client
    
    async def _create_client(self):
        provider = await _provider_cache.get_provider()
        if provider and provider.available and hasattr(provider.module, "YahooChartProvider"):
            try: return provider.module.YahooChartProvider()
            except Exception as e: logger.debug(f"Failed to instantiate YahooChartProvider: {e}")
        return None
    
    async def close(self):
        async with self._lock:
            if self._client and hasattr(self._client, "aclose"):
                try: await self._client.aclose()
                except Exception: pass
            self._client, self._closed = None, True

_client_manager = ClientManager()
async def aclose_yahoo_chart_client() -> None: await _client_manager.close()
async def aclose_yahoo_client() -> None: await aclose_yahoo_chart_client()

# -----------------------------------------------------------------------------
# Provider Class Shim
# -----------------------------------------------------------------------------

class YahooChartProvider:
    """Provides backward compatibility for class-based usage"""
    def __init__(self, *args, **kwargs):
        self._args, self._kwargs, self._client = args, kwargs, None
    
    async def __aenter__(self): return self
    async def __aexit__(self, exc_type, exc_val, exc_tb): await self.aclose()
    
    async def get_quote_patch(self, symbol: str, base: Optional[Dict[str, Any]] = None, *args, **kwargs) -> Dict[str, Any]:
        if self._client is None: self._client = await _client_manager.get_client()
        if self._client and hasattr(self._client, "get_quote_patch"):
            try: return await self._client.get_quote_patch(symbol, base, *args, **kwargs)
            except Exception: pass
        return await get_quote_patch(symbol, base, *args, **kwargs)
    
    async def fetch_quote(self, symbol: str, debug: bool = False, *args, **kwargs) -> Dict[str, Any]:
        if self._client is None: self._client = await _client_manager.get_client()
        if self._client and hasattr(self._client, "fetch_quote"):
            try: return await self._client.fetch_quote(symbol, debug, *args, **kwargs)
            except Exception: pass
        return await fetch_quote(symbol, debug=debug, *args, **kwargs)
    
    async def aclose(self) -> None:
        if self._client and hasattr(self._client, "aclose"):
            try: await self._client.aclose()
            except Exception: pass
        self._client = None

# -----------------------------------------------------------------------------
# Utility Functions
# -----------------------------------------------------------------------------

async def get_provider_status() -> Dict[str, Any]:
    provider = await _provider_cache.get_provider()
    return {
        "shim_version": SHIM_VERSION, "provider_available": provider.available if provider else False,
        "provider_version": provider.version if provider else None, "provider_error": provider.error if provider else None,
        "cache_ttl": ShimConfig.CACHE_TTL_SECONDS, "circuit_breaker": {"threshold": ShimConfig.CIRCUIT_BREAKER_THRESHOLD, "timeout": ShimConfig.CIRCUIT_BREAKER_TIMEOUT},
        "telemetry": _telemetry.get_stats(), "timestamp": datetime.now(timezone.utc).isoformat(),
    }

async def clear_cache() -> None:
    global _provider_cache
    _provider_cache = ProviderCache()

def get_version() -> str: return SHIM_VERSION

# -----------------------------------------------------------------------------
# Module Initialization
# -----------------------------------------------------------------------------

try:
    loop = asyncio.get_event_loop()
    if loop.is_running(): asyncio.create_task(_provider_cache.get_provider())
    else: loop.run_until_complete(_provider_cache.get_provider())
except Exception: pass

# -----------------------------------------------------------------------------
# Module Exports
# -----------------------------------------------------------------------------

__all__ = [
    "YAHOO_CHART_URL", "DATA_SOURCE", "SHIM_VERSION", "DataQuality", "YahooChartProvider",
    "fetch_quote", "get_quote", "get_quote_patch", "fetch_quote_patch", "fetch_enriched_quote_patch",
    "fetch_quote_and_enrichment_patch", "fetch_quote_and_fundamentals_patch", "yahoo_chart_quote",
    "fetch_price_history", "fetch_history", "fetch_ohlc_history", "fetch_history_patch", "fetch_prices",
    "aclose_yahoo_chart_client", "aclose_yahoo_client", "get_provider_status", "clear_cache", "get_version",
]

# -----------------------------------------------------------------------------
# Self-Test
# -----------------------------------------------------------------------------
if __name__ == "__main__":
    async def test_shim():
        if _RICH_AVAILABLE and console:
            console.print(f"\n[bold blue]🔧 Testing Yahoo Chart Provider Shim v{SHIM_VERSION}[/bold blue]")
            console.print("=" * 60)
            status = await get_provider_status()
            console.print(f"\n[bold yellow]📊 Provider Status:[/bold yellow]")
            console.print(f"  Available: [green]{status['provider_available']}[/green]")
            console.print(f"  Version: {status['provider_version']}")
            console.print(f"  Error: {status['provider_error']}")
            console.print(f"\n[bold yellow]📈 Testing fetch_quote:[/bold yellow]")
            result = await fetch_quote("AAPL")
            console.print(f"  Symbol: {result.get('symbol')}")
            console.print(f"  Status: {result.get('status')}")
            console.print(f"  Data Quality: {result.get('data_quality')}")
            if result.get('error'): console.print(f"  [red]Error: {result.get('error')}[/red]")
            stats = _telemetry.get_stats()
            console.print(f"\n[bold yellow]📉 Telemetry:[/bold yellow]")
            console.print(f"  Total calls: {stats.get('total_calls', 0)}")
            console.print(f"  Success rate: {stats.get('success_rate', 0):.1%}")
            console.print("\n[bold green]✅ Shim test complete[/bold green]")
            console.print("=" * 60)
        else:
            sys.stdout.write(f"\n🔧 Testing Yahoo Chart Provider Shim v{SHIM_VERSION}\n")
            sys.stdout.write("=" * 60 + "\n")
            status = await get_provider_status()
            sys.stdout.write(f"\n📊 Provider Status:\n")
            sys.stdout.write(f"  Available: {status['provider_available']}\n")
            sys.stdout.write(f"  Version: {status['provider_version']}\n")
            sys.stdout.write(f"  Error: {status['provider_error']}\n")
            sys.stdout.write(f"\n📈 Testing fetch_quote:\n")
            result = await fetch_quote("AAPL")
            sys.stdout.write(f"  Symbol: {result.get('symbol')}\n")
            sys.stdout.write(f"  Status: {result.get('status')}\n")
            sys.stdout.write(f"  Data Quality: {result.get('data_quality')}\n")
            if result.get('error'): sys.stdout.write(f"  Error: {result.get('error')}\n")
            stats = _telemetry.get_stats()
            sys.stdout.write(f"\n📉 Telemetry:\n")
            sys.stdout.write(f"  Total calls: {stats.get('total_calls', 0)}\n")
            sys.stdout.write(f"  Success rate: {stats.get('success_rate', 0):.1%}\n")
            sys.stdout.write("\n✅ Shim test complete\n")
            sys.stdout.write("=" * 60 + "\n")
    
    asyncio.run(test_shim())
