# core/__init__.py  (FULL REPLACEMENT)
"""
core/__init__.py
------------------------------------------------------------
Core package initialization (PROD SAFE) – v1.2.0 (LAZY)

Goals:
- Keep imports light to avoid circular-import + cold-start issues on Render.
- Export only stable entrypoints.
- Allow optional exports (DataEngine/UnifiedQuote/etc.) without breaking app boot.
- Prefer v2 engine, fallback to legacy, then schemas (last resort).
- IMPORTANT: avoid importing heavy modules at import-time (lazy resolution).
"""

from __future__ import annotations

import os
from typing import Any, Optional

# -----------------------------------------------------------------------------
# Optional debug logging for import issues (set CORE_IMPORT_DEBUG=true)
# -----------------------------------------------------------------------------
_DEBUG_IMPORTS = str(os.getenv("CORE_IMPORT_DEBUG", "false")).strip().lower() in ("1", "true", "yes", "y", "on")


def _dbg(msg: str) -> None:
    if _DEBUG_IMPORTS:
        # Avoid importing logging here to keep __init__ minimal
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
_CACHE: dict[str, Any] = {}


def _cached(key: str) -> Any:
    return _CACHE.get(key, None)


def _set_cache(key: str, value: Any) -> Any:
    _CACHE[key] = value
    return value


# -----------------------------------------------------------------------------
# Settings loader (prefer core.config shim; fallback to root config.py; then stub)
# -----------------------------------------------------------------------------
def _resolve_get_settings() -> Any:
    gs = _cached("get_settings")
    if gs is not None:
        return gs

    m = _try_import("core.config")
    if m is not None and hasattr(m, "get_settings"):
        return _set_cache("get_settings", getattr(m, "get_settings"))

    m2 = _try_import("config")
    if m2 is not None and hasattr(m2, "get_settings"):
        return _set_cache("get_settings", getattr(m2, "get_settings"))

    def _stub_get_settings() -> Any:  # type: ignore
        """Last-resort stub. Keeps imports safe during refactors."""
        return None

    return _set_cache("get_settings", _stub_get_settings)


def get_settings() -> Any:  # type: ignore
    return _resolve_get_settings()()


# -----------------------------------------------------------------------------
# Engine + schema lazy resolvers
# -----------------------------------------------------------------------------
def _resolve_engine_exports() -> dict[str, Any]:
    cached = _cached("engine_exports")
    if isinstance(cached, dict):
        return cached

    exports: dict[str, Any] = {
        "DataEngine": None,
        "UnifiedQuote": None,
        "normalize_symbol": None,
    }

    # Prefer: v2 -> legacy
    for mod_name in ("core.data_engine_v2", "core.data_engine"):
        m = _try_import(mod_name)
        if not m:
            continue

        if exports["DataEngine"] is None and hasattr(m, "DataEngine"):
            exports["DataEngine"] = getattr(m, "DataEngine")

        if exports["UnifiedQuote"] is None and hasattr(m, "UnifiedQuote"):
            exports["UnifiedQuote"] = getattr(m, "UnifiedQuote")

        if exports["normalize_symbol"] is None and hasattr(m, "normalize_symbol"):
            exports["normalize_symbol"] = getattr(m, "normalize_symbol")

        if exports["DataEngine"] is not None and exports["UnifiedQuote"] is not None and exports["normalize_symbol"] is not None:
            break

    # Last resort: UnifiedQuote from schemas
    if exports["UnifiedQuote"] is None:
        ms = _try_import("core.schemas")
        if ms and hasattr(ms, "UnifiedQuote"):
            exports["UnifiedQuote"] = getattr(ms, "UnifiedQuote")

    return _set_cache("engine_exports", exports)


def _resolve_schema_exports() -> dict[str, Any]:
    cached = _cached("schema_exports")
    if isinstance(cached, dict):
        return cached

    exports: dict[str, Any] = {
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
        return _resolve_get_settings()

    if name in ("DataEngine", "UnifiedQuote", "normalize_symbol"):
        return _resolve_engine_exports().get(name)

    if name in ("TickerRequest", "MarketData", "BatchProcessRequest", "get_headers_for_sheet"):
        return _resolve_schema_exports().get(name)

    raise AttributeError(name)


__all__ = [
    "get_settings",
    "DataEngine",
    "UnifiedQuote",
    "normalize_symbol",
    "TickerRequest",
    "MarketData",
    "BatchProcessRequest",
    "get_headers_for_sheet",
]
