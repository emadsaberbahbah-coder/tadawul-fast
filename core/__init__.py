"""
core/__init__.py
------------------------------------------------------------
Core package initialization (PROD SAFE) — v1.7.0 (LAZY + SYMBOL ALIGNED)

Goals:
- Keep imports LIGHT to avoid circular-import + cold-start issues on Render.
- Export only stable entrypoints via LAZY resolution (PEP 562).
- Prioritize core.symbols.normalize v1.1.0 for all symbol logic.
- Never crash app startup because of optional modules.

Key improvement in v1.7.0
- ✅ Explicit Normalization: Directly exports normalize_symbol, normalize_ksa_symbol, and is_ksa.
- ✅ Engine Resilience: Better signature matching for DataEngineV2 vs DataEngine.
- ✅ Schema Wrappers: Hardened get_headers_for_sheet to align with vNext Registry.

Exports (safe):
- get_settings
- DataEngine, DataEngineV2, UnifiedQuote (lazy)
- normalize_symbol, normalize_ksa_symbol, is_ksa (lazy)
- get_engine (best-effort singleton accessor)
- BatchProcessRequest (lazy)
- get_headers_for_sheet / resolve_sheet_key / get_supported_sheets (safe wrappers)
"""

from __future__ import annotations

import inspect
import os
from typing import Any, Callable, Dict, List, Optional


CORE_INIT_VERSION = "1.7.0"

# -----------------------------------------------------------------------------
# Optional debug logging for import issues (set CORE_IMPORT_DEBUG=true)
# -----------------------------------------------------------------------------
_DEBUG_IMPORTS = str(os.getenv("CORE_IMPORT_DEBUG", "false")).strip().lower() in {
    "1",
    "true",
    "yes",
    "y",
    "on",
}


def _dbg(msg: str) -> None:
    if _DEBUG_IMPORTS:
        try:
            print(f"[core.__init__] {msg}")  # noqa: T201
        except Exception:
            pass


def _try_import(module_path: str) -> Optional[Any]:
    try:
        from importlib import import_module
        return import_module(module_path)
    except Exception as e:
        _dbg(f"Import failed: {module_path} -> {e.__class__.__name__}: {e}")
        return None


# -----------------------------------------------------------------------------
# Tiny cache to avoid repeated imports / lookups
# -----------------------------------------------------------------------------
_CACHE: Dict[str, Any] = {}


def _cached(key: str) -> Any:
    return _CACHE.get(key, None)


def _set_cache(key: str, value: Any) -> Any:
    _CACHE[key] = value
    return value


# -----------------------------------------------------------------------------
# Settings loader
# -----------------------------------------------------------------------------
def _resolve_get_settings_func() -> Callable[[], Any]:
    gs = _cached("get_settings_func")
    if callable(gs):
        return gs

    # Prefer repo-root config.py (canonical)
    m0 = _try_import("config")
    if m0 is not None and callable(getattr(m0, "get_settings", None)):
        return _set_cache("get_settings_func", getattr(m0, "get_settings"))

    # Fallback: core.config (compat shim)
    m1 = _try_import("core.config")
    if m1 is not None and callable(getattr(m1, "get_settings", None)):
        return _set_cache("get_settings_func", getattr(m1, "get_settings"))

    def _stub_get_settings() -> Any:
        return None

    return _set_cache("get_settings_func", _stub_get_settings)


def get_settings() -> Any:
    """Safe: never raises."""
    try:
        fn = _resolve_get_settings_func()
        return fn() if callable(fn) else None
    except Exception as e:
        _dbg(f"get_settings() failed -> {e.__class__.__name__}: {e}")
        return None


# -----------------------------------------------------------------------------
# Normalization lazy resolvers (NEW in v1.7.0)
# -----------------------------------------------------------------------------
def _resolve_symbol_exports() -> Dict[str, Any]:
    cached = _cached("symbol_exports")
    if isinstance(cached, dict):
        return cached

    exports: Dict[str, Any] = {
        "normalize_symbol": None,
        "normalize_ksa_symbol": None,
        "is_ksa": None,
    }

    m = _try_import("core.symbols.normalize")
    if m:
        exports["normalize_symbol"] = getattr(m, "normalize_symbol", None)
        exports["normalize_ksa_symbol"] = getattr(m, "normalize_ksa_symbol", None)
        exports["is_ksa"] = getattr(m, "is_ksa", None)

    return _set_cache("symbol_exports", exports)


# -----------------------------------------------------------------------------
# Engine + schema lazy resolvers
# -----------------------------------------------------------------------------
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

    for mod_name in ("core.data_engine_v2", "core.data_engine"):
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
            break

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
        for key in exports.keys():
            exports[key] = getattr(ms, key, None)

    return _set_cache("schema_exports", exports)


# -----------------------------------------------------------------------------
# Safe schema wrappers
# -----------------------------------------------------------------------------
def get_headers_for_sheet(sheet_key: str, *args: Any, **kwargs: Any) -> List[str]:
    try:
        fn = _resolve_schema_exports().get("get_headers_for_sheet")
        if callable(fn):
            out = fn(sheet_key, *args, **kwargs)
            return list(out) if out is not None else []
    except Exception: pass
    return []


def resolve_sheet_key(sheet_key: str, *args: Any, **kwargs: Any) -> str:
    try:
        fn = _resolve_schema_exports().get("resolve_sheet_key")
        if callable(fn):
            out = fn(sheet_key, *args, **kwargs)
            return str(out).strip() if out else str(sheet_key or "").strip()
    except Exception: pass
    return str(sheet_key or "").strip()


def get_supported_sheets(*args: Any, **kwargs: Any) -> List[str]:
    try:
        fn = _resolve_schema_exports().get("get_supported_sheets")
        if callable(fn):
            out = fn(*args, **kwargs)
            return list(out) if out is not None else []
    except Exception: pass
    return []


# -----------------------------------------------------------------------------
# Best-effort engine singleton
# -----------------------------------------------------------------------------
def _try_construct_engine(cls: Any, settings: Any) -> Optional[Any]:
    if cls is None: return None
    try:
        sig = inspect.signature(cls)
        params = list(sig.parameters.values())
        if not params: return cls()
        
        kw = {}
        names = {p.name for p in params if p.name != "self"}
        if "settings" in names: kw["settings"] = settings
        elif "config" in names: kw["config"] = settings
        
        return cls(**kw) if kw else cls()
    except Exception:
        try: return cls(settings)
        except: return None


def get_engine() -> Any:
    cached_engine = _cached("engine_instance")
    if cached_engine is not None: return cached_engine

    try:
        ex = _resolve_engine_exports()
        settings = get_settings()

        ge = ex.get("get_engine_func")
        if callable(ge):
            try:
                inst = ge()
                if inst: return _set_cache("engine_instance", inst)
            except Exception: pass

        for key in ("DataEngineV2", "DataEngine"):
            inst = _try_construct_engine(ex.get(key), settings)
            if inst: return _set_cache("engine_instance", inst)

        return _set_cache("engine_instance", None)
    except Exception:
        return _set_cache("engine_instance", None)


# -----------------------------------------------------------------------------
# Lazy attribute access
# -----------------------------------------------------------------------------
def __getattr__(name: str) -> Any:
    # Direct access functions
    if name == "get_settings": return get_settings
    if name == "get_engine": return get_engine
    if name == "get_headers_for_sheet": return get_headers_for_sheet
    if name == "resolve_sheet_key": return resolve_sheet_key
    if name == "get_supported_sheets": return get_supported_sheets

    # Symbol Helpers (New v1.7.0)
    if name in ("normalize_symbol", "normalize_ksa_symbol", "is_ksa"):
        return _resolve_symbol_exports().get(name)

    # Engine exports
    if name in ("DataEngine", "DataEngineV2", "UnifiedQuote"):
        return _resolve_engine_exports().get(name)

    # Schema exports
    if name == "BatchProcessRequest":
        return _resolve_schema_exports().get("BatchProcessRequest")

    raise AttributeError(name)


def __dir__() -> List[str]:
    return sorted(__all__)


__all__ = [
    "CORE_INIT_VERSION",
    "get_settings",
    "DataEngine",
    "DataEngineV2",
    "UnifiedQuote",
    "normalize_symbol",
    "normalize_ksa_symbol",
    "is_ksa",
    "get_engine",
    "BatchProcessRequest",
    "get_headers_for_sheet",
    "resolve_sheet_key",
    "get_supported_sheets",
]
