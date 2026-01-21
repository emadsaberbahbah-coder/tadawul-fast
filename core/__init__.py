# core/__init__.py
"""
core/__init__.py
------------------------------------------------------------
Core package initialization (PROD SAFE) — v1.6.0 (LAZY + STABLE + ENGINE SAFE)

Goals:
- Keep imports LIGHT to avoid circular-import + cold-start issues on Render.
- Export only stable entrypoints via LAZY resolution (PEP 562).
- Prefer v2 engine, fallback to legacy, then schemas (last resort).
- Never crash app startup because of optional modules.
- Provide optional debug traces when CORE_IMPORT_DEBUG=true.
- Provide a BEST-EFFORT get_engine() even if engine modules do not expose one.

Key improvement in v1.6.0
- ✅ Export schema helpers as REAL wrappers (not None), so:
    from core import get_headers_for_sheet
  never imports None and then crashes later. Wrappers remain lazy + safe.

Exports (safe):
- get_settings
- DataEngine, DataEngineV2, UnifiedQuote, normalize_symbol (lazy attr or wrapper)
- get_engine (best-effort singleton accessor)
- BatchProcessRequest (lazy)
- get_headers_for_sheet / resolve_sheet_key / get_supported_sheets (safe wrappers)

Notes:
- Do NOT import heavy modules at import-time.
- All imports are wrapped; failures only affect that attribute, not app boot.
"""

from __future__ import annotations

import inspect
import os
from typing import Any, Callable, Dict, List, Optional


CORE_INIT_VERSION = "1.6.0"

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
# Settings loader (prefer root config.py; fallback to core.config shim; then stub)
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

    # Last resort stub (must never raise)
    def _stub_get_settings() -> Any:
        return None

    return _set_cache("get_settings_func", _stub_get_settings)


def get_settings() -> Any:
    """
    Returns settings object if available, else None.
    Safe: never raises.
    """
    try:
        fn = _resolve_get_settings_func()
        return fn() if callable(fn) else None
    except Exception as e:
        _dbg(f"get_settings() failed -> {e.__class__.__name__}: {e}")
        return None


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
        "normalize_symbol": None,
        "get_engine_func": None,  # internal: engine module accessor if present
        "engine_module": None,    # internal: which module provided exports
    }

    # Prefer: v2 -> legacy
    for mod_name in ("core.data_engine_v2", "core.data_engine"):
        m = _try_import(mod_name)
        if not m:
            continue

        exports["engine_module"] = mod_name

        try:
            if exports["DataEngine"] is None and hasattr(m, "DataEngine"):
                exports["DataEngine"] = getattr(m, "DataEngine")
        except Exception:
            pass

        try:
            if exports["DataEngineV2"] is None and hasattr(m, "DataEngineV2"):
                exports["DataEngineV2"] = getattr(m, "DataEngineV2")
        except Exception:
            pass

        try:
            if exports["UnifiedQuote"] is None:
                uq = getattr(m, "UnifiedQuote", None)
                if uq is not None:
                    exports["UnifiedQuote"] = uq
        except Exception:
            pass

        try:
            if exports["normalize_symbol"] is None:
                ns = getattr(m, "normalize_symbol", None)
                if callable(ns):
                    exports["normalize_symbol"] = ns
        except Exception:
            pass

        try:
            if exports["get_engine_func"] is None:
                ge = getattr(m, "get_engine", None)
                if callable(ge):
                    exports["get_engine_func"] = ge
        except Exception:
            pass

        # Stop early if we got the important ones
        if exports["normalize_symbol"] and (exports["DataEngineV2"] or exports["DataEngine"]):
            break

    # Last resort: UnifiedQuote from schemas (if you ever expose it there)
    if exports["UnifiedQuote"] is None:
        ms = _try_import("core.schemas")
        if ms is not None:
            try:
                uq2 = getattr(ms, "UnifiedQuote", None)
                if uq2 is not None:
                    exports["UnifiedQuote"] = uq2
            except Exception:
                pass

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
        exports["BatchProcessRequest"] = getattr(ms, "BatchProcessRequest", None)
        exports["get_headers_for_sheet"] = getattr(ms, "get_headers_for_sheet", None)
        exports["resolve_sheet_key"] = getattr(ms, "resolve_sheet_key", None)
        exports["get_supported_sheets"] = getattr(ms, "get_supported_sheets", None)

    return _set_cache("schema_exports", exports)


# -----------------------------------------------------------------------------
# Safe schema wrappers (real callables; still lazy)
# -----------------------------------------------------------------------------
def get_headers_for_sheet(sheet_key: str, *args: Any, **kwargs: Any) -> List[str]:
    """
    Safe wrapper. Never raises. Returns [] if schema function is unavailable.
    """
    try:
        fn = _resolve_schema_exports().get("get_headers_for_sheet")
        if callable(fn):
            out = fn(sheet_key, *args, **kwargs)
            return list(out) if out is not None else []
        return []
    except Exception as e:
        _dbg(f"get_headers_for_sheet() failed -> {e.__class__.__name__}: {e}")
        return []


def resolve_sheet_key(sheet_key: str, *args: Any, **kwargs: Any) -> str:
    """
    Safe wrapper. Never raises. Returns original key if resolver is unavailable.
    """
    try:
        fn = _resolve_schema_exports().get("resolve_sheet_key")
        if callable(fn):
            out = fn(sheet_key, *args, **kwargs)
            s = str(out).strip() if out is not None else ""
            return s or str(sheet_key or "").strip()
        return str(sheet_key or "").strip()
    except Exception as e:
        _dbg(f"resolve_sheet_key() failed -> {e.__class__.__name__}: {e}")
        return str(sheet_key or "").strip()


def get_supported_sheets(*args: Any, **kwargs: Any) -> List[str]:
    """
    Safe wrapper. Never raises. Returns [] if schema function is unavailable.
    """
    try:
        fn = _resolve_schema_exports().get("get_supported_sheets")
        if callable(fn):
            out = fn(*args, **kwargs)
            return list(out) if out is not None else []
        return []
    except Exception as e:
        _dbg(f"get_supported_sheets() failed -> {e.__class__.__name__}: {e}")
        return []


# -----------------------------------------------------------------------------
# Best-effort engine singleton (PROD SAFE)
# -----------------------------------------------------------------------------
def _try_construct_engine(cls: Any, settings: Any) -> Optional[Any]:
    """
    Construct engine with best-effort signature matching.
    Never raises; returns instance or None.
    """
    if cls is None:
        return None
    try:
        sig = None
        try:
            sig = inspect.signature(cls)  # class constructor signature
        except Exception:
            sig = None

        if sig is None:
            try:
                return cls()
            except Exception:
                try:
                    return cls(settings)
                except Exception:
                    return None

        params = list(sig.parameters.values())

        if not params:
            try:
                return cls()
            except Exception:
                return None

        kw: Dict[str, Any] = {}
        names = {p.name for p in params if p.name not in ("self",)}

        if "settings" in names:
            kw["settings"] = settings
        elif "config" in names:
            kw["config"] = settings
        elif "cfg" in names:
            kw["cfg"] = settings

        if kw:
            try:
                return cls(**kw)
            except Exception:
                pass

        try:
            return cls()
        except Exception:
            try:
                return cls(settings)
            except Exception:
                return None
    except Exception:
        return None


def get_engine() -> Any:
    """
    Best-effort engine accessor (singleton-like).
    Safe: never raises. Returns engine instance or None.

    Resolution order:
    1) If engine module exposes get_engine(), call it (v2 preferred)
    2) Else attempt to instantiate DataEngineV2, then DataEngine
    """
    cached_engine = _cached("engine_instance")
    if cached_engine is not None:
        return cached_engine

    try:
        ex = _resolve_engine_exports()
        settings = get_settings()

        ge = ex.get("get_engine_func")
        if callable(ge):
            try:
                inst = ge()
                if inst is not None:
                    return _set_cache("engine_instance", inst)
            except Exception as e:
                _dbg(f"engine get_engine() failed -> {e.__class__.__name__}: {e}")

        cls_v2 = ex.get("DataEngineV2")
        inst = _try_construct_engine(cls_v2, settings)
        if inst is not None:
            return _set_cache("engine_instance", inst)

        cls_legacy = ex.get("DataEngine")
        inst = _try_construct_engine(cls_legacy, settings)
        if inst is not None:
            return _set_cache("engine_instance", inst)

        return _set_cache("engine_instance", None)
    except Exception as e:
        _dbg(f"get_engine() failed -> {e.__class__.__name__}: {e}")
        return _set_cache("engine_instance", None)


# -----------------------------------------------------------------------------
# Lazy attribute access (PEP 562)
# -----------------------------------------------------------------------------
def __getattr__(name: str) -> Any:  # pragma: no cover
    # Direct functions
    if name == "get_settings":
        return get_settings
    if name == "get_engine":
        return get_engine

    # Engine exports (lazy)
    if name in ("DataEngine", "DataEngineV2", "UnifiedQuote", "normalize_symbol"):
        ex = _resolve_engine_exports()
        if name == "DataEngine":
            return ex.get("DataEngine")
        if name == "DataEngineV2":
            return ex.get("DataEngineV2")
        if name == "UnifiedQuote":
            return ex.get("UnifiedQuote")
        if name == "normalize_symbol":
            return ex.get("normalize_symbol")

    # Schema exports (BatchProcessRequest stays lazy)
    if name == "BatchProcessRequest":
        return _resolve_schema_exports().get("BatchProcessRequest")

    raise AttributeError(name)


def __dir__() -> List[str]:  # pragma: no cover
    return sorted(__all__)


__all__ = [
    "CORE_INIT_VERSION",
    "get_settings",
    "DataEngine",
    "DataEngineV2",
    "UnifiedQuote",
    "normalize_symbol",
    "get_engine",
    "BatchProcessRequest",
    "get_headers_for_sheet",
    "resolve_sheet_key",
    "get_supported_sheets",
]
