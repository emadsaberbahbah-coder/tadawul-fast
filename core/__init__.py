#!/usr/bin/env python3
# core/__init__.py
"""
================================================================================
Core Package Initializer -- v5.2.0 (GLOBAL-FIRST / ENGINE-LAZY / RENDER SAFE)
================================================================================

v5.2.0 changes vs v5.1.1
--------------------------
FIX: _SYMBOL_CANDIDATES reordered.
  v5.1.1 tried "symbols.normalize" before "core.symbols.normalize".
  In all TFB deployments the file lives at core/symbols/normalize.py,
  so the more-specific path must be tried first to avoid misresolution.
  "normalize" (repo-root) added as a final fallback for single-directory
  deployments where schema_registry and normalize.py are at the same level.

  v5.1.1:  ["symbols.normalize", "core.symbols.normalize"]
  v5.2.0:  ["core.symbols.normalize", "symbols.normalize", "normalize"]

ENH: __all__ extended with LogLevel, ProviderInfo, CoreMetrics.
  These classes are returned by public functions (init_core, get_provider,
  get_metrics). Consumers that do `from core import ...` could not import
  them previously.

Preserved from v5.1.1 (no functional changes):
- asyncio.Lock NOT created at import-time (prevents cross-loop issues on Render/FastAPI)
- _try_import(retry=True) truly retries (bypasses cache)
- Provider priorities: EODHD(10) > Yahoo-Chart(20) > Yahoo-Fund(25) > Finnhub(30)
- KSA priorities:      Tadawul(10) > Argaam(20)
- Engine resolution: data_engine_v2 preferred
- Thread-safe cache with TTL eviction

Usage
-----
In an async context (FastAPI): await core.get_engine_async()
In sync context (scripts/CLI): core.get_engine()
================================================================================
"""

from __future__ import annotations

import asyncio
import inspect
import os
import sys
import time
import threading
from dataclasses import dataclass, field
from enum import Enum
from functools import wraps
from typing import Any, Callable, Dict, List, Optional, Tuple, Type, Union

# Define early to avoid NameError in rare cases (introspection during import)
__all__: List[str] = []

# ---------------------------------------------------------------------------
# Fast JSON fallback (optional orjson)
# ---------------------------------------------------------------------------
try:
    import orjson  # type: ignore

    def json_loads(data: Union[str, bytes]) -> Any:
        if isinstance(data, str):
            data = data.encode("utf-8")
        return orjson.loads(data)

    _HAS_ORJSON = True
except Exception:
    import json  # type: ignore

    def json_loads(data: Union[str, bytes]) -> Any:
        if isinstance(data, (bytes, bytearray)):
            data = data.decode("utf-8", errors="replace")
        return json.loads(data)

    _HAS_ORJSON = False


__version__       = "5.2.0"
__core_version__  = __version__
CORE_INIT_VERSION = __version__

# =============================================================================
# Debug / Logging
# =============================================================================

class LogLevel(Enum):
    DEBUG    = 10
    INFO     = 20
    WARN     = 30
    WARNING  = 30
    ERROR    = 40
    CRITICAL = 50


_DEBUG_IMPORTS   = (os.getenv("CORE_IMPORT_DEBUG", "") or "").strip().lower() in {"1", "true", "yes", "y", "on"}
_DEBUG_LEVEL     = (os.getenv("CORE_IMPORT_DEBUG_LEVEL", "info") or "info").strip().lower()

_LOG_LEVEL = LogLevel.INFO
try:
    _LOG_LEVEL = LogLevel[_DEBUG_LEVEL.upper()]
except Exception:
    _LOG_LEVEL = LogLevel.INFO

_PERF_MONITORING = (os.getenv("CORE_PERF_MONITORING", "") or "").strip().lower() in {"1", "true", "yes", "y", "on"}
_import_times: Dict[str, float] = {}
_import_lock = threading.RLock()


def _dbg(msg: str, level: str = "info") -> None:
    if not _DEBUG_IMPORTS:
        return
    try:
        lvl = LogLevel[level.upper()]
        if lvl.value >= _LOG_LEVEL.value:
            ts = time.strftime("%H:%M:%S")
            print(f"[{ts}] [core] [{level.upper()}] {msg}", file=sys.stderr)  # noqa: T201
    except Exception:
        pass


def _monitor_import(func: Callable) -> Callable:
    if not _PERF_MONITORING:
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
    with _import_lock:
        return dict(_import_times)


# =============================================================================
# Thread-safe small cache
# =============================================================================

class ThreadSafeCache:
    def __init__(self, max_size: int = 2000):
        self._cache:   Dict[str, Any]   = {}
        self._expires: Dict[str, float] = {}
        self._lock     = threading.RLock()
        self._max_size = max(128, int(max_size))

    def get(self, key: str, default: Any = None) -> Any:
        with self._lock:
            if key in self._cache:
                exp = self._expires.get(key)
                if exp is not None and time.time() > exp:
                    self._cache.pop(key, None)
                    self._expires.pop(key, None)
                    return default
                return self._cache[key]
            return default

    def set(self, key: str, value: Any, ttl: Optional[float] = None) -> Any:
        with self._lock:
            if len(self._cache) >= self._max_size and key not in self._cache:
                n = max(1, int(self._max_size * 0.10))
                for k in list(self._cache.keys())[:n]:
                    self._cache.pop(k, None)
                    self._expires.pop(k, None)
            self._cache[key] = value
            if ttl is not None:
                self._expires[key] = time.time() + float(ttl)
            else:
                self._expires.pop(key, None)
            return value

    def remove(self, key: str) -> None:
        with self._lock:
            self._cache.pop(key, None)
            self._expires.pop(key, None)

    def clear(self) -> None:
        with self._lock:
            self._cache.clear()
            self._expires.clear()


_CACHE = ThreadSafeCache()


# =============================================================================
# Safe import utilities
# =============================================================================

class ImportResult:
    def __init__(self, module: Any = None, error: Optional[Exception] = None):
        self.module    = module
        self.error     = error
        self.timestamp = time.time()

    @property
    def success(self) -> bool:
        return self.module is not None

    @property
    def failed(self) -> bool:
        return self.error is not None


def _try_import(module_path: str, retry: bool = False) -> ImportResult:
    """
    retry=True MUST bypass cache (real retry).
    """
    cache_key = f"import:{module_path}"
    cached = _CACHE.get(cache_key)

    if not retry and isinstance(cached, ImportResult):
        # cache successes always, cache failures briefly (60s window)
        if cached.success or (time.time() - cached.timestamp) < 60:
            return cached

    try:
        from importlib import import_module
        _dbg(f"Importing {module_path}", "debug")
        mod = import_module(module_path)
        res = ImportResult(module=mod, error=None)
        _CACHE.set(cache_key, res)
        return res
    except Exception as e:
        _dbg(f"Import failed {module_path}: {e}", "warn")
        res = ImportResult(module=None, error=e)
        _CACHE.set(cache_key, res, ttl=30)
        return res


def _try_import_attr(module_path: str, attr_name: str) -> Tuple[Optional[Any], Optional[Exception]]:
    res = _try_import(module_path)
    if res.failed:
        return None, res.error
    if not res.module:
        return None, ImportError(f"Module {module_path} is None")
    try:
        return getattr(res.module, attr_name, None), None
    except Exception as e:
        return None, e


# =============================================================================
# Settings resolution
# =============================================================================

_SETTINGS_CANDIDATES = [
    "core.config",
    "config",
    "app.config",
    "settings",
    "core.settings",
]


def _resolve_get_settings_func() -> Tuple[Optional[Callable], Optional[str]]:
    """Prefer: get_settings_cached() then get_settings()."""
    cache_key = "settings_func"
    fn      = _CACHE.get(cache_key)
    mod_used = _CACHE.get("settings_func_module")
    if fn is not None:
        return fn, mod_used

    for mod in _SETTINGS_CANDIDATES:
        r = _try_import(mod)
        if not r.success or not r.module:
            continue
        f1 = getattr(r.module, "get_settings_cached", None)
        if callable(f1):
            _CACHE.set(cache_key, f1)
            _CACHE.set("settings_func_module", mod)
            _dbg(f"Resolved get_settings_cached from {mod}", "info")
            return f1, mod
        f2 = getattr(r.module, "get_settings", None)
        if callable(f2):
            _CACHE.set(cache_key, f2)
            _CACHE.set("settings_func_module", mod)
            _dbg(f"Resolved get_settings from {mod}", "info")
            return f2, mod

    def _stub() -> Any:
        return None

    _CACHE.set(cache_key, _stub)
    _CACHE.set("settings_func_module", None)
    return _stub, None


@_monitor_import
def get_settings() -> Any:
    try:
        fn, _ = _resolve_get_settings_func()
        return fn() if callable(fn) else None
    except Exception as e:
        _dbg(f"get_settings failed: {e}", "error")
        return None


# =============================================================================
# Engine resolution (GLOBAL-FIRST / async-safe)
# =============================================================================

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
    module_path:     Optional[str]      = None
    get_engine_func: Optional[Callable] = None
    engine_class:    Optional[Type]     = None
    error:           Optional[str]      = None


def _resolve_engine_info() -> EngineInfo:
    cache_key = "engine_info_v2"
    cached = _CACHE.get(cache_key)
    if isinstance(cached, EngineInfo):
        return cached

    for mod in _ENGINE_CANDIDATES:
        r = _try_import(mod)
        if not r.success or not r.module:
            continue

        ge = getattr(r.module, "get_engine", None)
        if callable(ge):
            info = EngineInfo(module_path=mod, get_engine_func=ge, engine_class=None, error=None)
            _CACHE.set(cache_key, info)
            _dbg(f"Resolved engine via get_engine from {mod}", "info")
            return info

        for cls_name in ("DataEngine", "DataEngineV2", "DataEngineV3", "DataEngineV4", "DataEngineV5"):
            cls = getattr(r.module, cls_name, None)
            if isinstance(cls, type):
                info = EngineInfo(module_path=mod, get_engine_func=None, engine_class=cls, error=None)
                _CACHE.set(cache_key, info)
                _dbg(f"Resolved engine class {cls_name} from {mod}", "info")
                return info

    info = EngineInfo(error="No engine module found")
    _CACHE.set(cache_key, info, ttl=30)
    return info


def _call_get_engine_compat(fn: Callable, settings: Any) -> Any:
    """
    Calls get_engine() safely regardless of its signature.
    Supports: get_engine() / get_engine(settings=...) / get_engine(config=...)
    """
    try:
        sig   = inspect.signature(fn)
        names = set(sig.parameters.keys())
        if "settings" in names:
            return fn(settings=settings)
        if "config" in names:
            return fn(config=settings)
        return fn()
    except Exception:
        return fn()


def _construct_engine_compat(cls: Type, settings: Any) -> Any:
    """Construct engine class safely regardless of __init__ signature."""
    try:
        sig   = inspect.signature(cls.__init__)
        names = set(sig.parameters.keys())
        if "settings" in names:
            return cls(settings=settings)
        if "config" in names:
            return cls(config=settings)
        try:
            return cls(settings)
        except Exception:
            return cls()
    except Exception:
        return cls()


_ENGINE_INSTANCE: Optional[Any] = None
_ENGINE_LOCK = threading.RLock()

# IMPORTANT: Do NOT create asyncio.Lock at import-time (can bind to wrong loop)
_ENGINE_ASYNC_LOCK:         Optional[asyncio.Lock] = None
_ENGINE_ASYNC_LOCK_LOOP_ID: Optional[int]          = None
_ENGINE_ASYNC_LOCK_GUARD    = threading.RLock()


def _get_engine_async_lock() -> asyncio.Lock:
    """Create/refresh an asyncio lock per running loop to avoid cross-loop issues."""
    global _ENGINE_ASYNC_LOCK, _ENGINE_ASYNC_LOCK_LOOP_ID
    loop = asyncio.get_running_loop()
    lid  = id(loop)
    with _ENGINE_ASYNC_LOCK_GUARD:
        if _ENGINE_ASYNC_LOCK is None or _ENGINE_ASYNC_LOCK_LOOP_ID != lid:
            _ENGINE_ASYNC_LOCK         = asyncio.Lock()
            _ENGINE_ASYNC_LOCK_LOOP_ID = lid
        return _ENGINE_ASYNC_LOCK


async def get_engine_async(force_reload: bool = False) -> Optional[Any]:
    """
    Async engine getter (best for FastAPI).
    Uses core.data_engine_v2.get_engine (async) when available.
    Caches instance globally.
    """
    global _ENGINE_INSTANCE

    if not force_reload:
        with _ENGINE_LOCK:
            if _ENGINE_INSTANCE is not None:
                return _ENGINE_INSTANCE

    async with _get_engine_async_lock():
        if not force_reload:
            with _ENGINE_LOCK:
                if _ENGINE_INSTANCE is not None:
                    return _ENGINE_INSTANCE

        settings = get_settings()
        info     = _resolve_engine_info()

        if info.get_engine_func:
            try:
                out = _call_get_engine_compat(info.get_engine_func, settings)
                eng = await out if inspect.isawaitable(out) else out
                if eng is not None:
                    with _ENGINE_LOCK:
                        _ENGINE_INSTANCE = eng
                    return eng
            except Exception as e:
                _dbg(f"get_engine_async via function failed: {e}", "warn")

        if info.engine_class:
            try:
                eng = _construct_engine_compat(info.engine_class, settings)
                if eng is not None:
                    with _ENGINE_LOCK:
                        _ENGINE_INSTANCE = eng
                    return eng
            except Exception as e:
                _dbg(f"get_engine_async via class failed: {e}", "warn")

        return None


@_monitor_import
def get_engine(force_reload: bool = False) -> Any:
    """
    Sync engine getter (for CLI/scripts).
    - Inside a running event loop: returns a coroutine (caller must await).
    - No running loop: runs async getter via asyncio.run and returns instance.
    """
    try:
        asyncio.get_running_loop()
        return get_engine_async(force_reload=force_reload)
    except RuntimeError:
        try:
            return asyncio.run(get_engine_async(force_reload=force_reload))
        except Exception as e:
            _dbg(f"get_engine (sync) failed: {e}", "warn")
            return None


def reload_engine() -> Any:
    global _ENGINE_INSTANCE
    with _ENGINE_LOCK:
        _ENGINE_INSTANCE = None
    _CACHE.remove("engine_info_v2")
    return get_engine(force_reload=True)


# =============================================================================
# Symbol exports (best-effort, no hard dependency)
# FIX v5.2.0: core.symbols.normalize tried first (correct package path).
#             "normalize" added as repo-root fallback.
# =============================================================================

_SYMBOL_CANDIDATES = [
    "core.symbols.normalize",   # FIX v5.2.0: was second, now first (correct path)
    "symbols.normalize",        # alternative package layout
    "normalize",                # repo-root fallback (single-directory deployments)
]

_SYMBOL_EXPORTS = [
    "normalize_symbol",
    "normalize_ksa_symbol",
    "is_ksa",
    "looks_like_ksa",
    "to_yahoo_symbol",
    "to_eodhd_symbol",
    "to_finnhub_symbol",
]


def _get_symbol_export(name: str) -> Any:
    cache_key = f"symbol_export:{name}"
    cached = _CACHE.get(cache_key)
    if cached is not None:
        return cached

    for mod in _SYMBOL_CANDIDATES:
        attr, _ = _try_import_attr(mod, name)
        if attr is not None:
            _CACHE.set(cache_key, attr)
            return attr

    return None


# =============================================================================
# Provider registry (lightweight, aligned priorities)
# =============================================================================

class ProviderInfo:
    def __init__(
        self,
        name: str,
        module_path: str,
        priority: int,
        markets: Optional[List[str]] = None,
        enabled: bool = True,
        batch_supported: bool = False,
    ):
        self.name            = name
        self.module_path     = module_path
        self.priority        = int(priority)
        self.markets         = markets or ["GLOBAL"]
        self.enabled         = bool(enabled)
        self.batch_supported = bool(batch_supported)

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
    *,
    priority: int = 100,
    markets: Optional[List[str]] = None,
    enabled: bool = True,
    batch_supported: bool = True,   # all revised providers now export PROVIDER_BATCH_SUPPORTED=True
) -> None:
    with _PROVIDER_LOCK:
        _PROVIDER_REGISTRY[name] = ProviderInfo(
            name, module_path, priority,
            markets=markets, enabled=enabled,
            batch_supported=batch_supported,
        )


def get_provider(name: str) -> Optional[ProviderInfo]:
    with _PROVIDER_LOCK:
        return _PROVIDER_REGISTRY.get(name)


def get_enabled_providers(market: Optional[str] = None) -> List[ProviderInfo]:
    with _PROVIDER_LOCK:
        ps = [p for p in _PROVIDER_REGISTRY.values() if p.enabled]
        if market:
            ps = [p for p in ps if not p.markets or market in p.markets]
        return sorted(ps, key=lambda p: p.priority)


# ---------------------------------------------------------------------------
# GLOBAL-first priorities (EODHD primary, Yahoo fallbacks, Finnhub last)
# All revised providers v4.x/v5.x/v6.x now have PROVIDER_BATCH_SUPPORTED=True
# ---------------------------------------------------------------------------
register_provider("eodhd",              "core.providers.eodhd_provider",              priority=10,  markets=["GLOBAL"], batch_supported=True)
register_provider("yahoo_chart",        "core.providers.yahoo_chart_provider",         priority=20,  markets=["GLOBAL"], batch_supported=True)
register_provider("yahoo_fundamentals", "core.providers.yahoo_fundamentals_provider",  priority=25,  markets=["GLOBAL"], batch_supported=True)
register_provider("finnhub",            "core.providers.finnhub_provider",             priority=30,  markets=["GLOBAL"], batch_supported=True)

# KSA sources
register_provider("tadawul",            "core.providers.tadawul_provider",             priority=10,  markets=["KSA"],    batch_supported=True)
register_provider("argaam",             "core.providers.argaam_provider",              priority=20,  markets=["KSA"],    batch_supported=True)


# =============================================================================
# Metrics (minimal)
# =============================================================================

@dataclass
class CoreMetrics:
    import_times:     Dict[str, float]          = field(default_factory=dict)
    engine_requests:  int                        = 0
    errors:           List[Tuple[str, str, float]] = field(default_factory=list)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "import_times":    self.import_times,
            "engine_requests": self.engine_requests,
            "errors":          [(e[0], e[1]) for e in self.errors[-10:]],
        }


_METRICS      = CoreMetrics()
_METRICS_LOCK = threading.RLock()


def record_engine_request() -> None:
    with _METRICS_LOCK:
        _METRICS.engine_requests += 1


def record_error(source: str, error: str) -> None:
    with _METRICS_LOCK:
        _METRICS.errors.append((source, error, time.time()))
        if len(_METRICS.errors) > 200:
            _METRICS.errors = _METRICS.errors[-200:]


def get_metrics() -> CoreMetrics:
    with _METRICS_LOCK:
        return CoreMetrics(
            import_times    = get_import_times(),
            engine_requests = _METRICS.engine_requests,
            errors          = list(_METRICS.errors),
        )


def reset_metrics() -> None:
    global _METRICS
    with _METRICS_LOCK:
        _METRICS = CoreMetrics()
    with _import_lock:
        _import_times.clear()


# =============================================================================
# PEP 562: Lazy attribute access
# =============================================================================

def __getattr__(name: str) -> Any:
    # core API
    if name == "get_settings":       return get_settings
    if name == "get_engine":         return get_engine
    if name == "get_engine_async":   return get_engine_async
    if name == "reload_engine":      return reload_engine

    # provider registry
    if name == "register_provider":   return register_provider
    if name == "get_provider":        return get_provider
    if name == "get_enabled_providers": return get_enabled_providers

    # metrics
    if name == "get_metrics":         return get_metrics
    if name == "reset_metrics":       return reset_metrics
    if name == "get_import_times":    return get_import_times

    # symbol exports (best-effort)
    sym = _get_symbol_export(name)
    if sym is not None:
        return sym

    # versions
    if name == "CORE_INIT_VERSION":   return CORE_INIT_VERSION

    raise AttributeError(f"module 'core' has no attribute '{name}'")


def __dir__() -> List[str]:
    return sorted(set(__all__))


# =============================================================================
# Init
# =============================================================================

def init_core(debug: Optional[bool] = None, log_level: Optional[str] = None) -> None:
    global _DEBUG_IMPORTS, _LOG_LEVEL
    with _import_lock:
        if debug is not None:
            _DEBUG_IMPORTS = bool(debug)
        if log_level is not None:
            try:
                _LOG_LEVEL = LogLevel[log_level.upper()]
            except Exception:
                _LOG_LEVEL = LogLevel.INFO
    _dbg(f"Core initialized v{__version__} | debug={_DEBUG_IMPORTS} | level={_LOG_LEVEL.name}", "info")


# Auto-init (safe)
try:
    init_core()
except Exception:
    pass


# =============================================================================
# Exports
# =============================================================================

__all__ = [
    "__version__",
    "__core_version__",
    "CORE_INIT_VERSION",
    # settings
    "get_settings",
    # engine
    "get_engine",
    "get_engine_async",
    "reload_engine",
    # providers registry
    "register_provider",
    "get_provider",
    "get_enabled_providers",
    # metrics
    "get_metrics",
    "reset_metrics",
    "get_import_times",
    # init
    "init_core",
    # types (returned by public functions -- consumers need them)
    "LogLevel",
    "ProviderInfo",
    "CoreMetrics",
]
