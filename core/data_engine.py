"""
core/data_engine.py
===============================================================
LEGACY WRAPPER AROUND DATAENGINE v2 – v3.0

PURPOSE
- Provide a *backwards-compatible* module-level API for the original
  "v1 core data engine" while delegating all real work to the new
  class-based engine in `core.data_engine_v2`.
- Keep old imports working, such as:
      from core import data_engine
      from core.data_engine import get_enriched_quotes, UnifiedQuote, DataEngine
- Ensure KSA safety and provider logic are defined ONLY once in
  `core.data_engine_v2` (single source of truth).

MAIN BEHAVIOR
- Tries to import:
      from core.data_engine_v2 import DataEngine, UnifiedQuote
- If successful:
      • DataEngine (here) is an alias of the v2 DataEngine.
      • get_enriched_quote(s) simply forward to the v2 engine instance.
      • UnifiedQuote is re-exported from v2.
- If v2 cannot be imported:
      • Falls back to a very small stub engine that always returns
        MISSING UnifiedQuote objects (no crashes, but clearly marked).
      • This keeps the app importable even in emergency / partial setups.

NOTES
- EODHD is NEVER called for KSA (.SR) tickers – that logic lives in
  `core.data_engine_v2` and its provider set.
"""

from __future__ import annotations

import logging
import asyncio
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

# Try to pull config from env.py, else use os
try:
    import env
except ImportError:
    env = None

logger = logging.getLogger("core.data_engine")

# ---------------------------------------------------------------------------
# Try to import the "real" v2 engine + models
# ---------------------------------------------------------------------------

_ENGINE_MODE: str = "stub"
_ENGINE_IS_STUB: bool = True

# We define placeholders first so they exist in module scope if import fails
class StubUnifiedQuote:
    pass

try:
    # Primary, modern engine
    from core.data_engine_v2 import (  # type: ignore
        DataEngine as _V2DataEngine,
        UnifiedQuote,
    )

    _ENGINE_MODE = "v2"
    _ENGINE_IS_STUB = False
    logger.info("core.data_engine: Delegating to core.data_engine_v2.DataEngine.")

except Exception as exc:  # pragma: no cover - defensive fallback
    logger.exception(
        "core.data_engine: Failed to import core.data_engine_v2. "
        "Falling back to stub engine. Error: %s",
        exc,
    )
    _ENGINE_MODE = "stub"
    _ENGINE_IS_STUB = True

    # -----------------------------------------------------------------------
    # Minimal local models for stub mode
    # -----------------------------------------------------------------------
    try:
        from pydantic import BaseModel, Field
    except ImportError:
        class BaseModel: # type: ignore
            def __init__(self, **kwargs):
                self.__dict__.update(kwargs)
            def dict(self, **kwargs): return self.__dict__
        def Field(default=None, **kwargs): return default

    class UnifiedQuote(BaseModel):  # type: ignore
        """
        Minimal UnifiedQuote placeholder used in stub mode.
        """
        symbol: str
        price: Optional[float] = None
        data_quality: str = "MISSING"
        last_updated_utc: Optional[datetime] = None
        market: Optional[str] = None
        market_region: Optional[str] = None
        currency: Optional[str] = None
        error: Optional[str] = None
        # Minimal fields to prevent attribute errors in legacy code
        change: Optional[float] = None
        change_pct: Optional[float] = None
        volume: Optional[float] = None

    class _V2DataEngine:  # type: ignore[no-redef]
        """
        Stub engine implementation used ONLY when core.data_engine_v2
        is not available. It never raises, but always returns MISSING data.
        """

        def __init__(self, *args: Any, **kwargs: Any) -> None:
            logger.error(
                "core.data_engine: Using stub DataEngine – core.data_engine_v2 "
                "is not available. All quotes will be MISSING."
            )

        async def get_enriched_quote(self, symbol: str) -> UnifiedQuote:
            sym = (symbol or "").strip().upper()
            now = datetime.now(timezone.utc)
            return UnifiedQuote(
                symbol=sym or "",
                price=None,
                data_quality="MISSING",
                last_updated_utc=now,
                market=None,
                market_region=None,
                currency=None,
                error="Unified engine v2 unavailable (stub mode).",
            )

        async def get_enriched_quotes(self, symbols: List[str]) -> List[UnifiedQuote]:
            if not symbols:
                return []
            return [await self.get_enriched_quote(s) for s in symbols]


# ---------------------------------------------------------------------------
# Engine instance & compatibility alias
# ---------------------------------------------------------------------------

# Shared engine instance used by module-level helpers below
# We initialize it once. In a real app, this might be handled by dependency injection,
# but for the legacy module wrapper, a singleton is standard.
try:
    _engine = _V2DataEngine()
except Exception as e:
    logger.error(f"Failed to initialize DataEngine: {e}")
    # Fallback to a dumb stub if even init fails
    class CrashStub:
        async def get_enriched_quote(self, s): return UnifiedQuote(symbol=s, error="Engine Crash")
        async def get_enriched_quotes(self, s): return [await self.get_enriched_quote(x) for x in s]
    _engine = CrashStub()

# For backwards compatibility, expose DataEngine as the engine class type.
DataEngine = _V2DataEngine  # type: ignore[assignment]

# ---------------------------------------------------------------------------
# Module-level convenience functions (v1-style API)
# ---------------------------------------------------------------------------

async def get_enriched_quote(symbol: str) -> UnifiedQuote:
    """
    Backwards-compatible convenience wrapper:
        from core.data_engine import get_enriched_quote
    Delegates to the shared DataEngine instance (_engine).
    """
    try:
        return await _engine.get_enriched_quote(symbol)  # type: ignore[attr-defined]
    except Exception as exc:  # pragma: no cover - extremely defensive
        logger.exception("core.data_engine: Error in get_enriched_quote(%s)", symbol)
        now = datetime.now(timezone.utc)
        return UnifiedQuote(
            symbol=(symbol or "").strip().upper(),
            price=None,
            data_quality="ERROR",
            last_updated_utc=now,
            error=f"Unhandled error in get_enriched_quote: {exc}",
        )


async def get_enriched_quotes(symbols: List[str]) -> List[UnifiedQuote]:
    """
    Backwards-compatible batch wrapper:
        from core.data_engine import get_enriched_quotes
    Delegates to the shared DataEngine instance (_engine).
    """
    clean: List[str] = [s.strip() for s in (symbols or []) if s and s.strip()]
    if not clean:
        return []

    try:
        results = await _engine.get_enriched_quotes(clean)  # type: ignore[attr-defined]
    except Exception as exc:  # pragma: no cover - extremely defensive
        logger.exception(
            "core.data_engine: Error in get_enriched_quotes(%s)", clean
        )
        now = datetime.now(timezone.utc)
        # Return one error quote per requested symbol
        return [
            UnifiedQuote(
                symbol=sym.upper(),
                price=None,
                data_quality="ERROR",
                last_updated_utc=now,
                error=f"Unhandled error in get_enriched_quotes: {exc}",
            )
            for sym in clean
        ]

    # Be defensive: enforce type + symbol mapping
    out: List[UnifiedQuote] = []
    now = datetime.now(timezone.utc)
    
    # If engine returns fewer results than requested (it shouldn't), handle gracefully
    # We attempt to match order if possible, but v2 usually returns mapped list.
    
    for q in results:
        if isinstance(q, UnifiedQuote):
            out.append(q)
        else:
            # Unexpected type from underlying engine; wrap as error quote
            # Likely a dict if using an old v1 StubEngine
            sym = "UNKNOWN"
            if isinstance(q, dict):
                sym = q.get("symbol", "UNKNOWN")
            
            out.append(
                UnifiedQuote(
                    symbol=str(sym),
                    price=None,
                    data_quality="ERROR",
                    last_updated_utc=now,
                    error=f"Engine returned non-UnifiedQuote instance: {type(q)!r}",
                )
            )
    return out


# ---------------------------------------------------------------------------
# Introspection helpers (optional, used by diagnostics)
# ---------------------------------------------------------------------------

def get_engine_meta() -> Dict[str, Any]:
    """
    Light introspection about the underlying engine.
    Useful for /v1/status or debugging endpoints.
    """
    enabled_providers: Optional[List[str]] = None

    try:
        enabled_providers = list(getattr(_engine, "enabled_providers", []))  # type: ignore[attr-defined]
    except Exception:
        enabled_providers = None

    return {
        "engine_mode": _ENGINE_MODE,
        "engine_is_stub": _ENGINE_IS_STUB,
        "enabled_providers": enabled_providers,
    }


__all__ = [
    "UnifiedQuote",
    "DataEngine",
    "get_enriched_quote",
    "get_enriched_quotes",
    "get_engine_meta",
]
