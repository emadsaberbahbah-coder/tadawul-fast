# core/__init__.py  (FULL REPLACEMENT)
"""
core/__init__.py
------------------------------------------------------------
Core package initialization (PROD SAFE) — v1.3.0 (LAZY + STABLE)

Goals:
- Keep imports LIGHT to avoid circular-import + cold-start issues on Render.
- Export only stable entrypoints via LAZY resolution (PEP 562).
- Prefer v2 engine, fallback to legacy, then schemas (last resort).
- Never crash app startup because of optional modules.
- Provide optional debug traces when CORE_IMPORT_DEBUG=true.

Exports (lazy):
- get_settings
- DataEngine, UnifiedQuote, normalize_symbol
- get_engine (best-effort)
- TickerRequest, MarketData, BatchProcessRequest, get_headers_for_sheet

Notes:
- Do NOT import heavy modules at import-time.
- All imports are wrapped; failures only affect that attribute, not app boot.
"""

from __future__ import annotations

import os
from typing import Any, Optional, Dict, Callable

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
        # Keep minimal: avoid importing logging here.
        print(f"[core.__init__] {msg}")  # noqa: T201


def _try_import(module_path: str) -> Optional[Any]:
    try:
        from importlib import import_module

        return import_module(module_path)
    except Exception as e:
        _dbg(f"Import failed: {module_path} -> {e}")
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
# Settings loader (prefer core.config shim; fallback to root config.py; then stub)
# -----------------------------------------------------------------------------
def _resolve_get_settings_func() -> Callable[[], Any]:
    gs = _cached("get_settings_func")
    if callable(gs):
        return gs

    # Prefer core.config (shim)
    m = _try_import("core.config")
    if m is not None and callable(getattr(m, "get_settings", None)):
        return _set_cache("get_settings_func", getattr(m, "get_settings"))

    # Fallback to repo-root config.py
    m2 = _try_import("config")
    if m2 is not None and callable(getattr(m2, "get_settings", None)):
        return _set_cache("get_settings_func", getattr(m2, "get_settings"))

    # Last resort stub
    def _stub_get_settings() -> Any:
        return None

    return _set_cache("get_settings_func", _stub_get_settings)


def get_settings() -> Any:
    """
    Returns settings object if available, else None.
    Safe: never raises.
    """
    try:
        return _resolve_get_settings_func()()
    except Exception as e:
        _dbg(f"get_settings() failed -> {e}")
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
        "UnifiedQuote": None,
        "normalize_symbol": None,
        "get_engine": None,
    }

    # Prefer: v2 -> legacy
    for mod_name in ("core.data_engine_v2", "core.data_engine"):
        m = _try_import(mod_name)
        if not m:
            continue

        if exports["DataEngine"] is None and hasattr(m, "DataEngine"):
            exports["DataEngine"] = getattr(m, "DataEngine")

        # UnifiedQuote may be on module, or via __getattr__ in legacy adapter
        if exports["UnifiedQuote"] is None:
            try:
                uq = getattr(m, "UnifiedQuote", None)
                if uq is not None:
                    exports["UnifiedQuote"] = uq
            except Exception:
                pass

        if exports["normalize_symbol"] is None and hasattr(m, "normalize_symbol"):
            exports["normalize_symbol"] = getattr(m, "normalize_symbol")

        # Optional async engine accessor (exists in legacy adapter, may exist in v2)
        ge = getattr(m, "get_engine", None)
        if exports["get_engine"] is None and callable(ge):
            exports["get_engine"] = ge

        # Stop early if we got the important ones
        if exports["DataEngine"] and exports["UnifiedQuote"] and exports["normalize_symbol"]:
            break

    # Last resort: UnifiedQuote from schemas
    if exports["UnifiedQuote"] is None:
        ms = _try_import("core.schemas")
        if ms is not None and hasattr(ms, "UnifiedQuote"):
            exports["UnifiedQuote"] = getattr(ms, "UnifiedQuote")

    return _set_cache("engine_exports", exports)


def _resolve_schema_exports() -> Dict[str, Any]:
    cached = _cached("schema_exports")
    if isinstance(cached, dict):
        return cached

    exports: Dict[str, Any] = {
        "TickerRequest": None,
        "MarketData": None,
        "BatchProcessRequest": None,
        "get_headers_for_sheet": None,
    }

    ms = _try_import("core.schemas")
    if ms:
        exports["TickerRequest"] = getattr(ms, "TickerRequest", None)
        exports["MarketData"] = getattr(ms, "MarketData", None)
        exports["BatchProcessRequest"] = getattr(ms, "BatchProcessRequest", None)
        exports["get_headers_for_sheet"] = getattr(ms, "get_headers_for_sheet", None)

    return _set_cache("schema_exports", exports)


# -----------------------------------------------------------------------------
# Lazy attribute access (PEP 562)
# -----------------------------------------------------------------------------
def __getattr__(name: str) -> Any:  # pragma: no cover
    if name == "get_settings":
        return get_settings

    if name in ("DataEngine", "UnifiedQuote", "normalize_symbol", "get_engine"):
        return _resolve_engine_exports().get(name)

    if name in ("TickerRequest", "MarketData", "BatchProcessRequest", "get_headers_for_sheet"):
        return _resolve_schema_exports().get(name)

    raise AttributeError(name)


__all__ = [
    "get_settings",
    "DataEngine",
    "UnifiedQuote",
    "normalize_symbol",
    "get_engine",
    "TickerRequest",
    "MarketData",
    "BatchProcessRequest",
    "get_headers_for_sheet",
]
