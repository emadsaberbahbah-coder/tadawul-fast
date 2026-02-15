"""
core/__init__.py
------------------------------------------------------------
Core package initialization (PROD SAFE) — v2.3.0
(LAZY + IMPORT-SAFE + SYMBOL/ENGINE/SCHEMA RESILIENT)

FULL REPLACEMENT (v2.3.0) — What’s improved vs v1.8.0
- ✅ Stronger lazy-loading with safer caching + explicit resolver maps
- ✅ Normalization aligned with core.symbols.normalize v3.x:
    exports normalize_symbol, normalize_ksa_symbol, is_ksa, looks_like_ksa,
    is_special_symbol/is_index_or_fx, and provider helpers (to_eodhd_symbol, ...)
- ✅ Engine resolution hardened:
    - supports multiple module candidates
    - supports get_engine(), get_engine(settings), get_engine(config) signatures
    - supports DataEngine / DataEngineV2 constructors with (settings/config) or none
- ✅ Schema wrappers more defensive + stable return types
- ✅ Optional debug logging with richer context:
    CORE_IMPORT_DEBUG=true and CORE_IMPORT_DEBUG_LEVEL=info|warn|error
- ✅ Never crashes app startup:
    ALL exports are best-effort; wrappers never raise

Exports (safe)
- get_settings (safe)
- get_engine (best-effort singleton)
- DataEngine, DataEngineV2, UnifiedQuote (lazy)
- normalize_symbol, normalize_ksa_symbol, is_ksa (lazy)
- looks_like_ksa, is_special_symbol, is_index_or_fx (lazy)
- to_yahoo_symbol, to_finnhub_symbol, to_eodhd_symbol (lazy)
- get_headers_for_sheet / resolve_sheet_key / get_supported_sheets (safe wrappers)
- BatchProcessRequest (lazy)

Environment variables
- CORE_IMPORT_DEBUG=true/false (default false)
- CORE_IMPORT_DEBUG_LEVEL=info|warn|error (default info)
"""

from __future__ import annotations

import inspect
import os
from typing import Any, Callable, Dict, List, Optional

CORE_INIT_VERSION = "2.3.0"

# ---------------------------------------------------------------------------
# Debug logging (opt-in)
# ---------------------------------------------------------------------------
_DEBUG_IMPORTS = str(os.getenv("CORE_IMPORT_DEBUG", "false")).strip().lower() in {
    "1",
    "true",
    "yes",
    "y",
    "on",
}

_DEBUG_LEVEL = str(os.getenv("CORE_IMPORT_DEBUG_LEVEL", "info")).strip().lower() or "info"


def _dbg(msg: str, level: str = "info") -> None:
    if not _DEBUG_IMPORTS:
        return
    try:
        # simple level gate
        levels = {"info": 10, "warn": 20, "warning": 20, "error": 30}
        cur = levels.get(_DEBUG_LEVEL, 10)
        lvl = levels.get(level, 10)
        if lvl >= cur:
            print(f"[core.__init__:{level}] {msg}")  # noqa: T201
    except Exception:
        pass


def _try_import(module_path: str) -> Optional[Any]:
    try:
        from importlib import import_module

        return import_module(module_path)
    except Exception as e:
        _dbg(f"Import failed: {module_path} -> {e.__class__.__name__}: {e}", level="warn")
        return None


# ---------------------------------------------------------------------------
# Tiny cache to avoid repeated imports / lookups
# ---------------------------------------------------------------------------
_CACHE: Dict[str, Any] = {}


def _cached(key: str) -> Any:
    return _CACHE.get(key, None)


def _set_cache(key: str, value: Any) -> Any:
    _CACHE[key] = value
    return value


# ---------------------------------------------------------------------------
# Settings loader
# ---------------------------------------------------------------------------
def _resolve_get_settings_func() -> Callable[[], Any]:
    gs = _cached("get_settings_func")
    if callable(gs):
        return gs

    # Prefer repo-root config.py (canonical)
    m0 = _try_import("config")
    if m0 is not None and callable(getattr(m0, "get_settings", None)):
        _dbg("Resolved get_settings from config.get_settings", "info")
        return _set_cache("get_settings_func", getattr(m0, "get_settings"))

    # Fallback: core.config (compat shim)
    m1 = _try_import("core.config")
    if m1 is not None and callable(getattr(m1, "get_settings", None)):
        _dbg("Resolved get_settings from core.config.get_settings", "info")
        return _set_cache("get_settings_func", getattr(m1, "get_settings"))

    def _stub_get_settings() -> Any:
        return None

    _dbg("Resolved get_settings to stub (no config module found)", "warn")
    return _set_cache("get_settings_func", _stub_get_settings)


def get_settings() -> Any:
    """Safe: never raises."""
    try:
        fn = _resolve_get_settings_func()
        return fn() if callable(fn) else None
    except Exception as e:
        _dbg(f"get_settings() failed -> {e.__class__.__name__}: {e}", "error")
        return None


# ---------------------------------------------------------------------------
# Symbols lazy resolvers (core.symbols.normalize)
# ---------------------------------------------------------------------------
def _resolve_symbol_exports() -> Dict[str, Any]:
    cached = _cached("symbol_exports")
    if isinstance(cached, dict):
        return cached

    exports: Dict[str, Any] = {
        # canonical
        "normalize_symbol": None,
        "normalize_ksa_symbol": None,
        "is_ksa": None,
        "looks_like_ksa": None,
        "is_special_symbol": None,
        "is_index_or_fx": None,
        # provider helpers
        "to_yahoo_symbol": None,
        "to_finnhub_symbol": None,
        "to_eodhd_symbol": None,
        # variants (optional)
        "symbol_variants": None,
        "yahoo_symbol_variants": None,
        "finnhub_symbol_variants": None,
        "eodhd_symbol_variants": None,
    }

    m = _try_import("core.symbols.normalize")
    if m:
        for k in list(exports.keys()):
            exports[k] = getattr(m, k, None)
        _dbg("Resolved symbol exports from core.symbols.normalize", "info")
    else:
        _dbg("core.symbols.normalize not available; symbol exports unresolved", "warn")

    return _set_cache("symbol_exports", exports)


# ---------------------------------------------------------------------------
# Engine + schema lazy resolvers
# ---------------------------------------------------------------------------
_ENGINE_MODULE_CANDIDATES = (
    "core.data_engine_v2",
    "core.data_engine",
    "core.engine",
)


def _resolve_engine_exports() -> Dict[str, Any]:
    cached = _cached("engine_exports")
    if isinstance(cached, dict):
        return cached

    exports: Dict[str, Any] = {
        "DataEngine": None,
        "DataEngineV2": None,
        "UnifiedQuote": None,
        "get_engine_func": None,
        "engine_module": None,
    }

    for mod_name in _ENGINE_MODULE_CANDIDATES:
        m = _try_import(mod_name)
        if not m:
            continue

        exports["engine_module"] = mod_name

        for key in ("DataEngine", "DataEngineV2", "UnifiedQuote"):
            if exports[key] is None and hasattr(m, key):
                exports[key] = getattr(m, key)

        ge = getattr(m, "get_engine", None)
        if callable(ge):
            exports["get_engine_func"] = ge

        if exports["DataEngineV2"] or exports["DataEngine"]:
            _dbg(f"Resolved engine exports from {mod_name}", "info")
            break

    if not exports["engine_module"]:
        _dbg("No engine module resolved from candidates", "warn")

    return _set_cache("engine_exports", exports)


def _resolve_schema_exports() -> Dict[str, Any]:
    cached = _cached("schema_exports")
    if isinstance(cached, dict):
        return cached

    exports: Dict[str, Any] = {
        "BatchProcessRequest": None,
        "get_headers_for_sheet": None,
        "resolve_sheet_key": None,
        "get_supported_sheets": None,
    }

    ms = _try_import("core.schemas")
    if ms:
        for key in list(exports.keys()):
            exports[key] = getattr(ms, key, None)
        _dbg("Resolved schema exports from core.schemas", "info")
    else:
        _dbg("core.schemas not available; schema exports unresolved", "warn")

    return _set_cache("schema_exports", exports)


# ---------------------------------------------------------------------------
# Safe schema wrappers
# ---------------------------------------------------------------------------
def get_headers_for_sheet(sheet_key: str, *args: Any, **kwargs: Any) -> List[str]:
    """Safe wrapper: always returns a list[str]. Never raises."""
    try:
        fn = _resolve_schema_exports().get("get_headers_for_sheet")
        if callable(fn):
            out = fn(sheet_key, *args, **kwargs)
            if out is None:
                return []
            return [str(x) for x in list(out)]
    except Exception as e:
        _dbg(f"get_headers_for_sheet({sheet_key}) failed -> {e.__class__.__name__}: {e}", "warn")
    return []


def resolve_sheet_key(sheet_key: str, *args: Any, **kwargs: Any) -> str:
    """Safe wrapper: always returns a non-null string."""
    try:
        fn = _resolve_schema_exports().get("resolve_sheet_key")
        if callable(fn):
            out = fn(sheet_key, *args, **kwargs)
            return str(out).strip() if out else str(sheet_key or "").strip()
    except Exception as e:
        _dbg(f"resolve_sheet_key({sheet_key}) failed -> {e.__class__.__name__}: {e}", "warn")
    return str(sheet_key or "").strip()


def get_supported_sheets(*args: Any, **kwargs: Any) -> List[str]:
    """Safe wrapper: always returns a list[str]."""
    try:
        fn = _resolve_schema_exports().get("get_supported_sheets")
        if callable(fn):
            out = fn(*args, **kwargs)
            if out is None:
                return []
            return [str(x) for x in list(out)]
    except Exception as e:
        _dbg(f"get_supported_sheets() failed -> {e.__class__.__name__}: {e}", "warn")
    return []


# ---------------------------------------------------------------------------
# Best-effort engine singleton
# ---------------------------------------------------------------------------
def _call_get_engine(ge: Callable[..., Any], settings: Any) -> Optional[Any]:
    """Try calling get_engine with compatible signatures."""
    try:
        sig = inspect.signature(ge)
        params = [p for p in sig.parameters.values() if p.name != "self"]
        if not params:
            return ge()

        names = {p.name for p in params}
        if "settings" in names:
            return ge(settings=settings)
        if "config" in names:
            return ge(config=settings)

        # If single positional param
        if len(params) == 1:
            try:
                return ge(settings)
            except Exception:
                return ge()

        # fallback: no-arg
        return ge()
    except Exception:
        try:
            return ge()
        except Exception:
            return None


def _try_construct_engine(cls: Any, settings: Any) -> Optional[Any]:
    """Try instantiating DataEngine/DataEngineV2 safely."""
    if cls is None:
        return None
    try:
        sig = inspect.signature(cls)
        params = [p for p in sig.parameters.values() if p.name != "self"]
        if not params:
            return cls()

        names = {p.name for p in params}
        if "settings" in names:
            return cls(settings=settings)
        if "config" in names:
            return cls(config=settings)

        # One positional parameter
        if len(params) == 1:
            try:
                return cls(settings)
            except Exception:
                return cls()

        return cls()
    except Exception:
        try:
            return cls()
        except Exception:
            return None


def get_engine() -> Any:
    """Best-effort singleton accessor. Safe: never raises."""
    cached_engine = _cached("engine_instance")
    if cached_engine is not None:
        return cached_engine

    try:
        ex = _resolve_engine_exports()
        settings = get_settings()

        ge = ex.get("get_engine_func")
        if callable(ge):
            inst = _call_get_engine(ge, settings)
            if inst is not None:
                _dbg(f"Engine instance resolved via get_engine() from {ex.get('engine_module')}", "info")
                return _set_cache("engine_instance", inst)

        for key in ("DataEngineV2", "DataEngine"):
            inst = _try_construct_engine(ex.get(key), settings)
            if inst is not None:
                _dbg(f"Engine instance constructed via {key} from {ex.get('engine_module')}", "info")
                return _set_cache("engine_instance", inst)

        _dbg("Engine instance not resolved (returning None)", "warn")
        return _set_cache("engine_instance", None)
    except Exception as e:
        _dbg(f"get_engine() failed -> {e.__class__.__name__}: {e}", "error")
        return _set_cache("engine_instance", None)


# ---------------------------------------------------------------------------
# Lazy attribute access (PEP 562)
# ---------------------------------------------------------------------------
def __getattr__(name: str) -> Any:
    # Direct functions (always safe)
    if name == "get_settings":
        return get_settings
    if name == "get_engine":
        return get_engine
    if name == "get_headers_for_sheet":
        return get_headers_for_sheet
    if name == "resolve_sheet_key":
        return resolve_sheet_key
    if name == "get_supported_sheets":
        return get_supported_sheets

    # Symbol exports (lazy)
    if name in (
        "normalize_symbol",
        "normalize_ksa_symbol",
        "is_ksa",
        "looks_like_ksa",
        "is_special_symbol",
        "is_index_or_fx",
        "to_yahoo_symbol",
        "to_finnhub_symbol",
        "to_eodhd_symbol",
        "symbol_variants",
        "yahoo_symbol_variants",
        "finnhub_symbol_variants",
        "eodhd_symbol_variants",
    ):
        return _resolve_symbol_exports().get(name)

    # Engine exports (lazy)
    if name in ("DataEngine", "DataEngineV2", "UnifiedQuote"):
        return _resolve_engine_exports().get(name)

    # Schema exports (lazy)
    if name == "BatchProcessRequest":
        return _resolve_schema_exports().get("BatchProcessRequest")

    raise AttributeError(name)


def __dir__() -> List[str]:
    return sorted(__all__)


__all__ = [
    "CORE_INIT_VERSION",
    # settings + engine
    "get_settings",
    "get_engine",
    "DataEngine",
    "DataEngineV2",
    "UnifiedQuote",
    # symbols (canonical)
    "normalize_symbol",
    "normalize_ksa_symbol",
    "is_ksa",
    "looks_like_ksa",
    "is_special_symbol",
    "is_index_or_fx",
    # provider helpers
    "to_yahoo_symbol",
    "to_finnhub_symbol",
    "to_eodhd_symbol",
    # schema wrappers
    "BatchProcessRequest",
    "get_headers_for_sheet",
    "resolve_sheet_key",
    "get_supported_sheets",
    # version
    "CORE_INIT_VERSION",
]
