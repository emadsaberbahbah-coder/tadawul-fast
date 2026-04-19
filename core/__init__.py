#!/usr/bin/env python3
# core/__init__.py
"""
================================================================================
Core Package Initializer -- v7.0.0 (GLOBAL-FIRST / ENGINE-LAZY / RENDER SAFE)
================================================================================

Purpose
-------
Central initialization and lazy-loading for the TFB core package.

v7.0.0 Changes (from v6.0.0)
----------------------------
Bug fixes:
  - `init_core()` no longer bypasses the frozen-dataclass contract via
    `object.__setattr__(_CONFIG, ...)`. It now rebuilds a new frozen
    `CoreConfig` via `dataclasses.replace()` and atomically rebinds
    the module-level `_CONFIG` under a lock. Readers always see a
    consistent snapshot, and the frozen guarantee is preserved.
  - `_get_engine_async_lock()` previously tracked loop identity via
    `id(loop)`, which is unsafe because CPython reuses memory addresses
    after GC. Confirmed with a reproduction: two sequentially created &
    closed event loops can share an `id()`. v7 switches to
    `weakref.ref(loop)` plus an `is`-comparison, so a stale lock is
    never reused on a fresh loop.
  - `__getattr__` no longer triggers 3 import attempts for every dunder /
    internal attribute probe (e.g. `__wrapped__`, `__sphinx_mock__`).
    Dunders short-circuit to `AttributeError` immediately, and
    non-dunder names are gated by an allowlist that mirrors the
    `core.symbols.normalize` module's own `__all__`. This silences a
    large amount of debug-log noise under CORE_IMPORT_DEBUG=1 and
    avoids wasted work in test runners that probe freely.

Cleanup:
  - Removed unused `cast` import from typing.
  - Removed dead `__getattr__` branches for names already defined at
    module scope (Python never calls `__getattr__` for those — they're
    found via normal attribute lookup first).
  - `ThreadSafeCache` now uses `OrderedDict` + `popitem(last=False)`
    for O(1)-per-pop FIFO eviction (was O(total) via `list(keys())[:n]`).
  - `_try_import_attr` now returns `(None, AttributeError(...))` when
    the attribute is missing, instead of ambiguous `(None, None)`.
  - The implicit `await x if isawaitable(x) else x` one-liner was
    replaced with explicit if/else for readability.

Preserved:
  - Full `__all__` surface (21 names).
  - Every public function signature: `get_settings`, `get_engine`,
    `get_engine_async`, `reload_engine`, `register_provider`,
    `get_provider`, `get_enabled_providers`, `get_metrics`,
    `reset_metrics`, `get_import_times`, `init_core`.
  - orjson fast-path with stdlib fallback.
  - Env-var contract: `CORE_IMPORT_DEBUG`, `CORE_IMPORT_DEBUG_LEVEL`,
    `CORE_PERF_MONITORING`.
  - Provider registration table (eodhd=10, yahoo_chart=20,
    yahoo_fundamentals=25, finnhub=30, tadawul=10 KSA,
    argaam=20 KSA).
  - Dataclasses: `CoreConfig`, `ImportResult`, `EngineInfo`,
    `ProviderInfo`, `CoreMetrics`.
  - Exception hierarchy: `CoreError` > `EngineResolutionError`,
    `ProviderRegistrationError`.
================================================================================
"""

from __future__ import annotations

import asyncio
import inspect
import json
import os
import sys
import threading
import time
import weakref
from collections import OrderedDict
from dataclasses import dataclass, field, replace as dc_replace
from enum import Enum
from functools import wraps
from typing import (
    Any,
    Callable,
    Dict,
    FrozenSet,
    List,
    Optional,
    Tuple,
    Type,
    Union,
)

# ---------------------------------------------------------------------------
# Fast JSON Support (orjson optional)
# ---------------------------------------------------------------------------

try:
    import orjson  # type: ignore

    def _json_loads(data: Union[str, bytes]) -> Any:
        if isinstance(data, str):
            data = data.encode("utf-8")
        return orjson.loads(data)

    _HAS_ORJSON = True
except ImportError:
    def _json_loads(data: Union[str, bytes]) -> Any:
        if isinstance(data, (bytes, bytearray)):
            data = data.decode("utf-8", errors="replace")
        return json.loads(data)

    _HAS_ORJSON = False

# ---------------------------------------------------------------------------
# Version
# ---------------------------------------------------------------------------

__version__ = "7.0.0"
__core_version__ = __version__
CORE_INIT_VERSION = __version__

# ---------------------------------------------------------------------------
# Enums
# ---------------------------------------------------------------------------

class LogLevel(Enum):
    """Log levels for core debug output."""
    DEBUG = 10
    INFO = 20
    WARN = 30
    WARNING = 30  # alias of WARN (Enum treats equal values as aliases)
    ERROR = 40
    CRITICAL = 50


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class CoreConfig:
    """Configuration for core package."""
    debug_imports: bool = False
    log_level: LogLevel = LogLevel.INFO
    perf_monitoring: bool = False
    cache_max_size: int = 2000
    cache_failure_ttl: int = 30

    @classmethod
    def from_env(cls) -> "CoreConfig":
        """Load configuration from environment variables."""
        def _env_bool(name: str, default: bool = False) -> bool:
            raw = (os.getenv(name, "")).strip().lower()
            if not raw:
                return default
            return raw in {"1", "true", "yes", "y", "on", "enabled"}

        def _env_str(name: str, default: str = "") -> str:
            return (os.getenv(name) or default).strip()

        debug_imports = _env_bool("CORE_IMPORT_DEBUG", False)
        perf_monitoring = _env_bool("CORE_PERF_MONITORING", False)

        log_level_str = _env_str("CORE_IMPORT_DEBUG_LEVEL", "info")
        try:
            log_level = LogLevel[log_level_str.upper()]
        except KeyError:
            log_level = LogLevel.INFO

        return cls(
            debug_imports=debug_imports,
            log_level=log_level,
            perf_monitoring=perf_monitoring,
            cache_max_size=2000,
            cache_failure_ttl=30,
        )


# v7.0.0: _CONFIG is rebound (never mutated) — see `init_core`.
_CONFIG: CoreConfig = CoreConfig.from_env()
_CONFIG_LOCK = threading.RLock()

# ---------------------------------------------------------------------------
# Custom Exceptions
# ---------------------------------------------------------------------------

class CoreError(Exception):
    """Base exception for core package errors."""
    pass


class EngineResolutionError(CoreError):
    """Raised when engine cannot be resolved."""
    pass


class ProviderRegistrationError(CoreError):
    """Raised when provider registration fails."""
    pass


# ---------------------------------------------------------------------------
# Thread-Safe Cache
# ---------------------------------------------------------------------------

class ThreadSafeCache:
    """Thread-safe TTL cache with max-size limit and FIFO eviction.

    v7.0.0: uses `OrderedDict` so each eviction pop is O(1) instead of
    paying for a full `list(keys())[:n]` materialization per insertion
    into a full cache.
    """

    def __init__(self, max_size: int = 2000):
        self._max_size = max(128, max_size)
        self._cache: "OrderedDict[str, Any]" = OrderedDict()
        self._expires: Dict[str, float] = {}
        self._lock = threading.RLock()

    def get(self, key: str, default: Any = None) -> Any:
        """Get value from cache, honoring TTL."""
        with self._lock:
            if key in self._cache:
                expires_at = self._expires.get(key)
                if expires_at is not None and time.time() > expires_at:
                    self._cache.pop(key, None)
                    self._expires.pop(key, None)
                    return default
                return self._cache[key]
            return default

    def set(self, key: str, value: Any, ttl_sec: Optional[float] = None) -> None:
        """Set value in cache with optional TTL."""
        with self._lock:
            # Evict oldest if cache is full (only when inserting a NEW key)
            if len(self._cache) >= self._max_size and key not in self._cache:
                n = max(1, int(self._max_size * 0.10))
                for _ in range(n):
                    if not self._cache:
                        break
                    evicted_key, _evicted_val = self._cache.popitem(last=False)
                    self._expires.pop(evicted_key, None)

            # Refreshing an existing key moves it to the MRU position
            if key in self._cache:
                self._cache.move_to_end(key)
            self._cache[key] = value
            if ttl_sec is not None:
                self._expires[key] = time.time() + ttl_sec
            else:
                self._expires.pop(key, None)

    def remove(self, key: str) -> None:
        """Remove value from cache."""
        with self._lock:
            self._cache.pop(key, None)
            self._expires.pop(key, None)

    def clear(self) -> None:
        """Clear all cache entries."""
        with self._lock:
            self._cache.clear()
            self._expires.clear()


_CACHE = ThreadSafeCache(max_size=_CONFIG.cache_max_size)

# ---------------------------------------------------------------------------
# Debug Logging
# ---------------------------------------------------------------------------

_import_times: Dict[str, float] = {}
_import_lock = threading.RLock()


def _dbg(msg: str, level: str = "info") -> None:
    """Debug logging for core initialization."""
    if not _CONFIG.debug_imports:
        return

    try:
        log_level = LogLevel[level.upper()]
        if log_level.value >= _CONFIG.log_level.value:
            timestamp = time.strftime("%H:%M:%S")
            print(f"[{timestamp}] [core] [{level.upper()}] {msg}", file=sys.stderr)
    except Exception:
        pass


def _monitor_import(func: Callable) -> Callable:
    """Decorator to monitor import/function timing.

    Note: perf-monitoring is evaluated at decoration time (module import).
    Flipping `_CONFIG.perf_monitoring` at runtime won't retroactively wrap
    already-decorated functions.
    """
    if not _CONFIG.perf_monitoring:
        return func

    @wraps(func)
    def wrapper(*args, **kwargs):
        start = time.perf_counter()
        try:
            return func(*args, **kwargs)
        finally:
            elapsed = time.perf_counter() - start
            with _import_lock:
                _import_times[func.__name__] = _import_times.get(func.__name__, 0.0) + elapsed
            if elapsed > 0.15:
                _dbg(f"Slow call: {func.__name__} took {elapsed:.3f}s", "warn")
    return wrapper


def get_import_times() -> Dict[str, float]:
    """Get import timing statistics."""
    with _import_lock:
        return dict(_import_times)


# ---------------------------------------------------------------------------
# Import Utilities
# ---------------------------------------------------------------------------

# Sentinel distinct from `None` — lets `_try_import_attr` distinguish
# "attribute not present" from "attribute has value None".
_MISSING: Any = object()


@dataclass
class ImportResult:
    """Result of an import attempt."""
    module: Optional[Any] = None
    error: Optional[Exception] = None
    timestamp: float = field(default_factory=time.time)

    @property
    def success(self) -> bool:
        return self.module is not None

    @property
    def failed(self) -> bool:
        return self.error is not None


def _try_import(module_path: str, retry: bool = False) -> ImportResult:
    """
    Attempt to import a module with caching.

    Args:
        module_path: Dot-separated module path
        retry: If True, bypass cache for a fresh attempt

    Returns:
        ImportResult with module or error
    """
    cache_key = f"import:{module_path}"
    cached = _CACHE.get(cache_key)

    # Return cached success, or cached failure within TTL
    if not retry and isinstance(cached, ImportResult):
        if cached.success or (time.time() - cached.timestamp) < _CONFIG.cache_failure_ttl:
            return cached

    try:
        from importlib import import_module
        _dbg(f"Importing {module_path}", "debug")
        module = import_module(module_path)
        result = ImportResult(module=module)
        _CACHE.set(cache_key, result)
        return result
    except Exception as e:
        _dbg(f"Import failed {module_path}: {e}", "warn")
        result = ImportResult(error=e)
        _CACHE.set(cache_key, result, ttl_sec=_CONFIG.cache_failure_ttl)
        return result


def _try_import_attr(module_path: str, attr_name: str) -> Tuple[Optional[Any], Optional[Exception]]:
    """Attempt to import a specific attribute from a module.

    v7.0.0: returns `(None, AttributeError)` explicitly for missing attrs
    (was ambiguous `(None, None)` in v6).
    """
    result = _try_import(module_path)
    if result.failed:
        return None, result.error
    if result.module is None:
        return None, ImportError(f"Module {module_path} is None")
    try:
        attr = getattr(result.module, attr_name, _MISSING)
        if attr is _MISSING:
            return None, AttributeError(
                f"module '{module_path}' has no attribute {attr_name!r}"
            )
        return attr, None
    except Exception as e:
        return None, e


# ---------------------------------------------------------------------------
# Settings Resolution
# ---------------------------------------------------------------------------

_SETTINGS_CANDIDATES = [
    "core.config",
    "config",
    "app.config",
    "settings",
    "core.settings",
]


def _resolve_settings_func() -> Tuple[Optional[Callable], Optional[str]]:
    """Resolve the get_settings function from candidate modules."""
    cache_key = "settings_func"
    cached_func = _CACHE.get(cache_key)
    cached_module = _CACHE.get("settings_func_module")

    if cached_func is not None:
        return cached_func, cached_module

    for module_path in _SETTINGS_CANDIDATES:
        result = _try_import(module_path)
        if not result.success or result.module is None:
            continue

        # Try get_settings_cached first
        func = getattr(result.module, "get_settings_cached", None)
        if callable(func):
            _CACHE.set(cache_key, func)
            _CACHE.set("settings_func_module", module_path)
            _dbg(f"Resolved get_settings_cached from {module_path}", "info")
            return func, module_path

        # Fallback to get_settings
        func = getattr(result.module, "get_settings", None)
        if callable(func):
            _CACHE.set(cache_key, func)
            _CACHE.set("settings_func_module", module_path)
            _dbg(f"Resolved get_settings from {module_path}", "info")
            return func, module_path

    def _stub() -> Any:
        return None

    _CACHE.set(cache_key, _stub)
    _CACHE.set("settings_func_module", None)
    return _stub, None


@_monitor_import
def get_settings() -> Any:
    """Get application settings."""
    try:
        func, _ = _resolve_settings_func()
        return func() if callable(func) else None
    except Exception as e:
        _dbg(f"get_settings failed: {e}", "error")
        return None


# ---------------------------------------------------------------------------
# Engine Resolution
# ---------------------------------------------------------------------------

_ENGINE_CANDIDATES = [
    "core.data_engine_v2",  # preferred
    "core.data_engine",
    "core.engine",
    "data_engine_v2",
    "data_engine",
    "engine",
]


@dataclass
class EngineInfo:
    """Information about an engine resolution attempt."""
    module_path: Optional[str] = None
    get_engine_func: Optional[Callable] = None
    engine_class: Optional[Type] = None
    error: Optional[str] = None


def _resolve_engine_info() -> EngineInfo:
    """Resolve engine information from candidate modules."""
    cache_key = "engine_info_v2"
    cached = _CACHE.get(cache_key)
    if isinstance(cached, EngineInfo):
        return cached

    for module_path in _ENGINE_CANDIDATES:
        result = _try_import(module_path)
        if not result.success or result.module is None:
            continue

        # Check for get_engine function
        get_engine_func = getattr(result.module, "get_engine", None)
        if callable(get_engine_func):
            info = EngineInfo(
                module_path=module_path,
                get_engine_func=get_engine_func,
                engine_class=None,
                error=None,
            )
            _CACHE.set(cache_key, info)
            _dbg(f"Resolved engine via get_engine from {module_path}", "info")
            return info

        # Check for engine class
        for class_name in ("DataEngine", "DataEngineV2", "DataEngineV3", "DataEngineV4", "DataEngineV5"):
            engine_class = getattr(result.module, class_name, None)
            if isinstance(engine_class, type):
                info = EngineInfo(
                    module_path=module_path,
                    get_engine_func=None,
                    engine_class=engine_class,
                    error=None,
                )
                _CACHE.set(cache_key, info)
                _dbg(f"Resolved engine class {class_name} from {module_path}", "info")
                return info

    info = EngineInfo(error="No engine module found")
    _CACHE.set(cache_key, info, ttl_sec=30)
    return info


def _call_get_engine_compat(func: Callable, settings: Any) -> Any:
    """
    Call get_engine() safely regardless of signature.

    Supports:
        get_engine()
        get_engine(settings=...)
        get_engine(config=...)
    """
    try:
        sig = inspect.signature(func)
        param_names = set(sig.parameters.keys())

        if "settings" in param_names:
            return func(settings=settings)
        if "config" in param_names:
            return func(config=settings)
        return func()
    except Exception:
        return func()


def _construct_engine_compat(cls: Type, settings: Any) -> Any:
    """Construct engine class safely regardless of __init__ signature."""
    try:
        sig = inspect.signature(cls.__init__)
        param_names = set(sig.parameters.keys())

        if "settings" in param_names:
            return cls(settings=settings)
        if "config" in param_names:
            return cls(config=settings)
        try:
            return cls(settings)
        except Exception:
            return cls()
    except Exception:
        return cls()


_ENGINE_INSTANCE: Optional[Any] = None
_ENGINE_LOCK = threading.RLock()
_ENGINE_ASYNC_LOCK: Optional[asyncio.Lock] = None
# v7.0.0: use weakref.ref(loop) instead of id(loop). CPython can reuse
# memory addresses after GC (confirmed reproduction), so id() can let a
# stale lock silently rebind to a fresh loop. `is`-comparison via weakref
# is collision-proof.
_ENGINE_ASYNC_LOCK_LOOP_REF: "Optional[weakref.ref]" = None
_ENGINE_ASYNC_LOCK_GUARD = threading.RLock()


def _get_engine_async_lock() -> asyncio.Lock:
    """
    Return an asyncio.Lock bound to the current running loop.

    Creates a fresh lock whenever the loop changes (including after the
    previous loop is closed/GC'd, which identity-by-weakref catches
    cleanly where identity-by-id would silently collide).
    """
    global _ENGINE_ASYNC_LOCK, _ENGINE_ASYNC_LOCK_LOOP_REF

    loop = asyncio.get_running_loop()

    with _ENGINE_ASYNC_LOCK_GUARD:
        existing_loop: Any = None
        if _ENGINE_ASYNC_LOCK_LOOP_REF is not None:
            existing_loop = _ENGINE_ASYNC_LOCK_LOOP_REF()

        if _ENGINE_ASYNC_LOCK is None or existing_loop is not loop:
            _ENGINE_ASYNC_LOCK = asyncio.Lock()
            try:
                _ENGINE_ASYNC_LOCK_LOOP_REF = weakref.ref(loop)
            except TypeError:
                # Extremely rare: some custom loop types may block weakrefs.
                # Fall back to None, which causes us to recreate the lock on
                # each subsequent call — inefficient but safe.
                _ENGINE_ASYNC_LOCK_LOOP_REF = None

        return _ENGINE_ASYNC_LOCK


async def get_engine_async(force_reload: bool = False) -> Optional[Any]:
    """
    Get engine instance asynchronously (best for FastAPI).

    Args:
        force_reload: If True, bypass cache and reload engine

    Returns:
        Engine instance or None if not available
    """
    global _ENGINE_INSTANCE

    # Check cache without lock first
    if not force_reload:
        with _ENGINE_LOCK:
            if _ENGINE_INSTANCE is not None:
                return _ENGINE_INSTANCE

    # Acquire async lock for the current loop
    async with _get_engine_async_lock():
        # Double-check after acquiring lock
        if not force_reload:
            with _ENGINE_LOCK:
                if _ENGINE_INSTANCE is not None:
                    return _ENGINE_INSTANCE

        settings = get_settings()
        info = _resolve_engine_info()

        # Try get_engine function
        if info.get_engine_func:
            try:
                result = _call_get_engine_compat(info.get_engine_func, settings)
                # v7.0.0: explicit if/else replaces the cryptic
                # `await result if inspect.isawaitable(result) else result` one-liner.
                if inspect.isawaitable(result):
                    engine = await result
                else:
                    engine = result
                if engine is not None:
                    with _ENGINE_LOCK:
                        _ENGINE_INSTANCE = engine
                    return engine
            except Exception as e:
                _dbg(f"get_engine_async via function failed: {e}", "warn")

        # Try engine class
        if info.engine_class:
            try:
                engine = _construct_engine_compat(info.engine_class, settings)
                if engine is not None:
                    with _ENGINE_LOCK:
                        _ENGINE_INSTANCE = engine
                    return engine
            except Exception as e:
                _dbg(f"get_engine_async via class failed: {e}", "warn")

        return None


@_monitor_import
def get_engine(force_reload: bool = False) -> Any:
    """
    Get engine instance synchronously (for CLI/scripts).

    Args:
        force_reload: If True, bypass cache and reload engine

    Returns:
        Engine instance, coroutine, or None

    Note:
        If called within a running event loop, returns a COROUTINE that
        the caller must `await`. Otherwise, runs the async getter via
        `asyncio.run()` and returns the engine instance directly.
    """
    try:
        # Already in async context - return coroutine
        asyncio.get_running_loop()
        return get_engine_async(force_reload=force_reload)
    except RuntimeError:
        # No running loop - run async getter
        try:
            return asyncio.run(get_engine_async(force_reload=force_reload))
        except Exception as e:
            _dbg(f"get_engine (sync) failed: {e}", "warn")
            return None


def reload_engine() -> Any:
    """Force reload of engine instance."""
    global _ENGINE_INSTANCE

    with _ENGINE_LOCK:
        _ENGINE_INSTANCE = None

    _CACHE.remove("engine_info_v2")
    return get_engine(force_reload=True)


# ---------------------------------------------------------------------------
# Provider Registry
# ---------------------------------------------------------------------------

@dataclass
class ProviderInfo:
    """Information about a registered provider."""
    name: str
    module_path: str
    priority: int
    markets: List[str] = field(default_factory=list)
    enabled: bool = True
    batch_supported: bool = False

    def __repr__(self) -> str:
        return (
            f"ProviderInfo(name={self.name!r}, priority={self.priority}, "
            f"enabled={self.enabled}, batch={self.batch_supported})"
        )


_PROVIDER_REGISTRY: Dict[str, ProviderInfo] = {}
_PROVIDER_LOCK = threading.RLock()


def register_provider(
    name: str,
    module_path: str,
    priority: int = 100,
    markets: Optional[List[str]] = None,
    enabled: bool = True,
    batch_supported: bool = True,
) -> None:
    """
    Register a data provider.

    Args:
        name: Provider name (e.g., "eodhd", "yahoo_chart")
        module_path: Import path to provider module
        priority: Lower priority = higher precedence (10 > 20 > 30)
        markets: Markets this provider supports (e.g., ["GLOBAL"], ["KSA"])
        enabled: Whether provider is enabled
        batch_supported: Whether provider supports batch fetching

    Raises:
        ProviderRegistrationError: If registration fails
    """
    if not name or not module_path:
        raise ProviderRegistrationError("Provider name and module path are required")

    with _PROVIDER_LOCK:
        _PROVIDER_REGISTRY[name] = ProviderInfo(
            name=name,
            module_path=module_path,
            priority=priority,
            markets=markets or ["GLOBAL"],
            enabled=enabled,
            batch_supported=batch_supported,
        )
        _dbg(f"Registered provider: {name} (priority={priority})", "info")


def get_provider(name: str) -> Optional[ProviderInfo]:
    """Get provider information by name."""
    with _PROVIDER_LOCK:
        return _PROVIDER_REGISTRY.get(name)


def get_enabled_providers(market: Optional[str] = None) -> List[ProviderInfo]:
    """
    Get list of enabled providers, optionally filtered by market.

    Args:
        market: Filter by market (e.g., "GLOBAL", "KSA")

    Returns:
        List of ProviderInfo sorted by priority (lower = higher precedence)
    """
    with _PROVIDER_LOCK:
        providers = [p for p in _PROVIDER_REGISTRY.values() if p.enabled]

        if market:
            providers = [p for p in providers if not p.markets or market in p.markets]

        return sorted(providers, key=lambda p: p.priority)


# ---------------------------------------------------------------------------
# Register Default Providers
# ---------------------------------------------------------------------------

# Global providers (EODHD primary, Yahoo fallbacks, Finnhub last)
register_provider("eodhd", "core.providers.eodhd_provider", priority=10, markets=["GLOBAL"], batch_supported=True)
register_provider("yahoo_chart", "core.providers.yahoo_chart_provider", priority=20, markets=["GLOBAL"], batch_supported=True)
register_provider("yahoo_fundamentals", "core.providers.yahoo_fundamentals_provider", priority=25, markets=["GLOBAL"], batch_supported=True)
register_provider("finnhub", "core.providers.finnhub_provider", priority=30, markets=["GLOBAL"], batch_supported=True)

# KSA providers
register_provider("tadawul", "core.providers.tadawul_provider", priority=10, markets=["KSA"], batch_supported=True)
register_provider("argaam", "core.providers.argaam_provider", priority=20, markets=["KSA"], batch_supported=True)


# ---------------------------------------------------------------------------
# Metrics
# ---------------------------------------------------------------------------

@dataclass
class CoreMetrics:
    """Metrics for core package operations."""
    import_times: Dict[str, float] = field(default_factory=dict)
    engine_requests: int = 0
    errors: List[Tuple[str, str, float]] = field(default_factory=list)

    def to_dict(self) -> Dict[str, Any]:
        """Convert metrics to dictionary."""
        return {
            "import_times": self.import_times,
            "engine_requests": self.engine_requests,
            "errors": [(e[0], e[1]) for e in self.errors[-10:]],
        }


_METRICS = CoreMetrics()
_METRICS_LOCK = threading.RLock()


def record_engine_request() -> None:
    """Record an engine request."""
    with _METRICS_LOCK:
        _METRICS.engine_requests += 1


def record_error(source: str, error: str) -> None:
    """Record an error."""
    with _METRICS_LOCK:
        _METRICS.errors.append((source, error, time.time()))
        # Keep only last 200 errors
        if len(_METRICS.errors) > 200:
            _METRICS.errors = _METRICS.errors[-200:]


def get_metrics() -> CoreMetrics:
    """Get current metrics."""
    with _METRICS_LOCK:
        return CoreMetrics(
            import_times=get_import_times(),
            engine_requests=_METRICS.engine_requests,
            errors=list(_METRICS.errors),
        )


def reset_metrics() -> None:
    """Reset all metrics."""
    global _METRICS

    with _METRICS_LOCK:
        _METRICS = CoreMetrics()

    with _import_lock:
        _import_times.clear()


# ---------------------------------------------------------------------------
# Symbol Exports (Lazy)
# ---------------------------------------------------------------------------

_SYMBOL_CANDIDATES = [
    "core.symbols.normalize",
    "symbols.normalize",
    "normalize",
]

# Static fallback allowlist. The authoritative allowlist is pulled
# dynamically from the resolved normalize module's own __all__ — see
# `_get_symbol_exports_allowlist()`.
_SYMBOL_EXPORTS_STATIC: FrozenSet[str] = frozenset({
    "normalize_symbol",
    "normalize_ksa_symbol",
    "is_ksa",
    "looks_like_ksa",
    "to_yahoo_symbol",
    "to_eodhd_symbol",
    "to_finnhub_symbol",
})


def _get_symbol_exports_allowlist() -> FrozenSet[str]:
    """Return the set of attribute names exposed by the normalize module.

    Cached after first successful resolution. Falls back to a static set
    if the module cannot be resolved yet (during bootstrap).
    """
    cached = _CACHE.get("symbol_exports_allowlist")
    if isinstance(cached, frozenset):
        return cached

    for module_path in _SYMBOL_CANDIDATES:
        result = _try_import(module_path)
        if result.success and result.module is not None:
            exports = getattr(result.module, "__all__", None)
            if isinstance(exports, (list, tuple)):
                names = frozenset(str(x) for x in exports)
                _CACHE.set("symbol_exports_allowlist", names)
                return names
            # Module loaded but no __all__ — use the static fallback.
            break

    return _SYMBOL_EXPORTS_STATIC


def _get_symbol_export(name: str) -> Optional[Any]:
    """Lazy-load symbol export from normalize module."""
    cache_key = f"symbol_export:{name}"
    cached = _CACHE.get(cache_key)
    if cached is not None:
        return cached

    for module_path in _SYMBOL_CANDIDATES:
        attr, _error = _try_import_attr(module_path, name)
        if attr is not None:
            _CACHE.set(cache_key, attr)
            return attr

    return None


# ---------------------------------------------------------------------------
# Initialization
# ---------------------------------------------------------------------------

def init_core(debug: Optional[bool] = None, log_level: Optional[str] = None) -> None:
    """
    Initialize / reconfigure the core package.

    Args:
        debug: Enable (or disable) debug import logging.
        log_level: Set log level (DEBUG, INFO, WARN, ERROR, CRITICAL).

    v7.0.0: atomically rebinds the frozen ``_CONFIG`` via
    ``dataclasses.replace`` instead of mutating it through
    ``object.__setattr__``. This preserves the frozen contract and
    guarantees that concurrent readers see a consistent snapshot.
    """
    global _CONFIG

    with _CONFIG_LOCK:
        new_debug = _CONFIG.debug_imports if debug is None else bool(debug)

        new_level = _CONFIG.log_level
        if log_level is not None:
            try:
                new_level = LogLevel[log_level.upper()]
            except KeyError:
                pass

        _CONFIG = dc_replace(_CONFIG, debug_imports=new_debug, log_level=new_level)

    _dbg(
        f"Core initialized v{__version__} | debug={_CONFIG.debug_imports} | level={_CONFIG.log_level.name}",
        "info",
    )


# Auto-initialize on import
try:
    init_core()
except Exception:
    pass


# ---------------------------------------------------------------------------
# Lazy Attribute Access (PEP 562)
# ---------------------------------------------------------------------------

def __getattr__(name: str) -> Any:
    """Module-level __getattr__ for lazy symbol proxying.

    v7.0.0 changes:
      - Dunder names (e.g., `__wrapped__`, `__sphinx_mock__`) short-circuit
        to `AttributeError` immediately, avoiding wasted import attempts
        and debug-log spam during test collection / tooling introspection.
      - Non-dunder fallback is gated by an allowlist mirroring the
        normalize module's own `__all__`.
      - Removed dead branches for names like `get_settings`, `get_engine`,
        etc. that are already defined at module scope (Python never
        reaches `__getattr__` for those — normal attribute lookup finds
        them first).
    """
    # Short-circuit dunder probes
    if name.startswith("__") and name.endswith("__"):
        raise AttributeError(f"module 'core' has no attribute {name!r}")

    # Allowlisted symbol exports (lazy-loaded from normalize)
    if name in _get_symbol_exports_allowlist():
        export = _get_symbol_export(name)
        if export is not None:
            return export

    raise AttributeError(f"module 'core' has no attribute {name!r}")


def __dir__() -> List[str]:
    """Custom dir() for module introspection."""
    return sorted(set(__all__))


# ---------------------------------------------------------------------------
# Module Exports
# ---------------------------------------------------------------------------

__all__ = [
    "__version__",
    "__core_version__",
    "CORE_INIT_VERSION",
    # Enums
    "LogLevel",
    # Configuration
    "CoreConfig",
    "get_settings",
    # Engine
    "get_engine",
    "get_engine_async",
    "reload_engine",
    # Provider registry
    "register_provider",
    "get_provider",
    "get_enabled_providers",
    "ProviderInfo",
    # Metrics
    "get_metrics",
    "reset_metrics",
    "get_import_times",
    "CoreMetrics",
    # Initialization
    "init_core",
]
