"""
core/__init__.py
------------------------------------------------------------
Core package initialization (PROD SAFE) – v1.1.0

Goals:
- Keep imports light to avoid circular-import + cold-start issues on Render.
- Export only stable entrypoints.
- Allow optional exports (DataEngine/UnifiedQuote) without breaking app boot.
- Prefer v2 engine, fallback to legacy, then schemas (last resort).
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
# Settings loader (prefer core.config shim; fallback to root config.py; then stub)
# -----------------------------------------------------------------------------
get_settings = None  # type: ignore

_m = _try_import("core.config")
if _m and hasattr(_m, "get_settings"):
    get_settings = getattr(_m, "get_settings")

if get_settings is None:
    _m2 = _try_import("config")
    if _m2 and hasattr(_m2, "get_settings"):
        get_settings = getattr(_m2, "get_settings")

if get_settings is None:

    def get_settings() -> Any:  # type: ignore
        """Last-resort stub. Keeps imports safe during refactors."""
        return None


# -----------------------------------------------------------------------------
# Optional exports (engine + unified quote)
# Prefer: data_engine_v2 -> data_engine -> schemas
# -----------------------------------------------------------------------------
DataEngine = None  # type: ignore
UnifiedQuote = None  # type: ignore

for mod_name in ("core.data_engine_v2", "core.data_engine"):
    _m = _try_import(mod_name)
    if not _m:
        continue

    if DataEngine is None and hasattr(_m, "DataEngine"):
        DataEngine = getattr(_m, "DataEngine")

    # UnifiedQuote might live in engine module
    if UnifiedQuote is None and hasattr(_m, "UnifiedQuote"):
        UnifiedQuote = getattr(_m, "UnifiedQuote")

    # If both are resolved, stop
    if DataEngine is not None and UnifiedQuote is not None:
        break

# Last resort: UnifiedQuote from schemas (if engine didn't provide it)
if UnifiedQuote is None:
    _ms = _try_import("core.schemas")
    if _ms and hasattr(_ms, "UnifiedQuote"):
        UnifiedQuote = getattr(_ms, "UnifiedQuote")


# -----------------------------------------------------------------------------
# Schemas & helpers (should be lightweight; keep failure non-fatal)
# -----------------------------------------------------------------------------
TickerRequest = None  # type: ignore
MarketData = None  # type: ignore
BatchProcessRequest = None  # type: ignore
get_headers_for_sheet = None  # type: ignore

_ms = _try_import("core.schemas")
if _ms:
    TickerRequest = getattr(_ms, "TickerRequest", None)
    MarketData = getattr(_ms, "MarketData", None)
    BatchProcessRequest = getattr(_ms, "BatchProcessRequest", None)
    get_headers_for_sheet = getattr(_ms, "get_headers_for_sheet", None)


__all__ = [
    "get_settings",
    "DataEngine",
    "UnifiedQuote",
    "TickerRequest",
    "MarketData",
    "BatchProcessRequest",
    "get_headers_for_sheet",
]
