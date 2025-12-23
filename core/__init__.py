
"""
core/__init__.py
------------------------------------------------------------
Core package initialization.

✅ Goals (enhanced):
- Keep imports light to avoid circular-import + cold-start issues on Render.
- Export only stable entrypoints.
- Allow optional exports (DataEngine/UnifiedQuote) without breaking app boot.
"""

from __future__ import annotations

from .config import get_settings  # always safe (shim)

# Optional / heavy imports (avoid breaking boot if one module fails)
DataEngine = None  # type: ignore
UnifiedQuote = None  # type: ignore

try:
    from .data_engine_v2 import DataEngine as _DataEngine  # type: ignore
    from .schemas import UnifiedQuote as _UnifiedQuote  # type: ignore

    DataEngine = _DataEngine
    UnifiedQuote = _UnifiedQuote
except Exception:
    # Keep package importable even if optional dependencies/routes are still evolving
    pass


# Schemas & helpers (lightweight only)
try:
    from .schemas import (
        TickerRequest,
        MarketData,
        BatchProcessRequest,
        get_headers_for_sheet,
    )
except Exception:
    # If schemas is mid-refactor, do not break package import
    TickerRequest = None  # type: ignore
    MarketData = None  # type: ignore
    BatchProcessRequest = None  # type: ignore
    get_headers_for_sheet = None  # type: ignore


__all__ = [
    "get_settings",
    "DataEngine",
    "UnifiedQuote",
    "TickerRequest",
    "MarketData",
    "BatchProcessRequest",
    "get_headers_for_sheet",
]
