"""
core/data_engine.py
===============================================================
LEGACY WRAPPER AROUND DATAENGINE v2 – v3.2.0

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
      • QuoteSource is polyfilled if missing in v2 to satisfy legacy imports.
- If v2 cannot be imported:
      • Falls back to a very small stub engine that always returns
        MISSING UnifiedQuote objects (no crashes, but clearly marked).
      • This keeps the app importable even in emergency / partial setups.

NOTES
- EODHD is NEVER called for KSA (.SR) tickers – that logic lives in
  `core.data_engine_v2`.
"""

from __future__ import annotations

import logging
import asyncio
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

logger = logging.getLogger("core.data_engine")
if not logger.handlers:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )

# ---------------------------------------------------------------------------
# Polyfill for QuoteSource (Legacy Compatibility)
# ---------------------------------------------------------------------------
try:
    from pydantic import BaseModel, Field
except ImportError:
    class BaseModel: # type: ignore
        def __init__(self, **kwargs): self.__dict__.update(kwargs)
    def Field(default=None, **kwargs): return default

class QuoteSource(BaseModel):
    """
    Legacy provider metadata model.
    Re-defined here to ensure 'from core.data_engine import QuoteSource' works
    even if v2 dropped it.
    """
    provider: str
    latency_ms: Optional[float] = None
    timestamp_utc: Optional[datetime] = None
    raw: Optional[Dict[str, Any]] = None

# ---------------------------------------------------------------------------
# Try to import the "real" v2 engine + models
# ---------------------------------------------------------------------------

_ENGINE_MODE: str = "stub"
_ENGINE_IS_STUB: bool = True

try:
    # Primary, modern engine
    from core.data_engine_v2 import (  # type: ignore
        DataEngine as _V2DataEngine,
        UnifiedQuote,
    )

    # Check if UnifiedQuote has 'sources' field compatibility
    # V2 UnifiedQuote might not have 'sources' defined as List[QuoteSource]
    # We accept this; legacy code usually checks dicts or attributes loosely.

    _ENGINE_MODE = "v2"
    _ENGINE_IS_STUB = False
    logger.info("core.data_engine: Delegating to core.data_engine_v2.DataEngine.")

except Exception as exc:  # pragma: no cover - defensive fallback
    logger.warning(
        "core.data_engine: Failed to import core.data_engine_v2. "
        "Falling back to stub engine. Error: %s",
        exc,
    )
    _ENGINE_MODE = "stub"
    _ENGINE_IS_STUB = True

    # -----------------------------------------------------------------------
    # Minimal local models for stub mode
    # -----------------------------------------------------------------------
    class UnifiedQuote(BaseModel):  # type: ignore[no-redef]
        """
        Minimal UnifiedQuote placeholder used in stub mode.
        """
        symbol: str
        price: Optional[float] = None
        current_price: Optional[float] = None # V2 alias
        data_quality: str = "MISSING"
        last_updated_utc: Optional[datetime] = None
        market: Optional[str] = None
        market_region: Optional[str] = None
        currency: Optional[str] = None
        error: Optional[str] = None
        sources: List[QuoteSource] = Field(default_factory=list)
        
        # Compatibility fields
        change: Optional[float] = None
        percent_change: Optional[float] = None
        volume: Optional[float] = None

        def finalize(self): return self

    class _V2DataEngine:  # type: ignore[no-redef]
        """
        Stub engine implementation used ONLY when core.data_engine_v2
        is not available.
        """
        def __init__(self, *args: Any, **kwargs: Any) -> None:
            logger.error("core.data_engine: Using stub DataEngine.")

        async def get_enriched_quote(self, symbol: str) -> UnifiedQuote:
            sym = (symbol or "").strip().upper()
            now = datetime.now(timezone.utc)
            return UnifiedQuote(
                symbol=sym or "",
                current_price=None,
                data_quality="MISSING",
                last_updated_utc=now,
                error="Unified engine v2 unavailable (stub mode).",
                sources=[QuoteSource(provider="stub", timestamp_utc=now)]
            )

        async def get_enriched_quotes(self, symbols: List[str]) -> List[UnifiedQuote]:
            if not symbols: return []
            return [await self.get_enriched_quote(s) for s in symbols]


# ---------------------------------------------------------------------------
# Engine instance & compatibility alias
# ---------------------------------------------------------------------------

# Expose DataEngine as the class type
DataEngine = _V2DataEngine  # type: ignore[assignment]

# Shared singleton instance
_engine_instance = None

def _get_engine():
    global _engine_instance
    if _engine_instance is None:
        try:
            _engine_instance = _V2DataEngine()
        except Exception as e:
            logger.error(f"Failed to initialize DataEngine: {e}")
            # Crash-proof stub
            class CrashStub:
                async def get_enriched_quote(self, s): 
                    return UnifiedQuote(symbol=s, error=f"Engine Init Failed: {e}")
                async def get_enriched_quotes(self, s): 
                    return [await self.get_enriched_quote(x) for x in s]
            _engine_instance = CrashStub()
    return _engine_instance

# ---------------------------------------------------------------------------
# Module-level convenience functions (v1-style API)
# ---------------------------------------------------------------------------

async def get_enriched_quote(symbol: str) -> UnifiedQuote:
    """
    Backwards-compatible convenience wrapper:
        from core.data_engine import get_enriched_quote
    """
    eng = _get_engine()
    try:
        return await eng.get_enriched_quote(symbol)  # type: ignore[attr-defined]
    except Exception as exc:
        logger.exception("core.data_engine: Error in get_enriched_quote(%s)", symbol)
        return UnifiedQuote(
            symbol=(symbol or "").strip().upper(),
            data_quality="ERROR",
            error=f"Unhandled error: {exc}",
        )


async def get_enriched_quotes(symbols: List[str]) -> List[UnifiedQuote]:
    """
    Backwards-compatible batch wrapper:
        from core.data_engine import get_enriched_quotes
    """
    clean: List[str] = [s.strip() for s in (symbols or []) if s and s.strip()]
    if not clean:
        return []

    eng = _get_engine()
    try:
        results = await eng.get_enriched_quotes(clean)  # type: ignore[attr-defined]
    except Exception as exc:
        logger.exception("core.data_engine: Error in get_enriched_quotes batch")
        return [
            UnifiedQuote(
                symbol=sym.upper(),
                data_quality="ERROR",
                error=f"Unhandled error: {exc}",
            )
            for sym in clean
        ]

    # Ensure list
    if not isinstance(results, list):
        return [
            UnifiedQuote(symbol=s, data_quality="ERROR", error="Engine return type mismatch") 
            for s in clean
        ]

    return results


# ---------------------------------------------------------------------------
# Introspection helpers
# ---------------------------------------------------------------------------

def get_engine_meta() -> Dict[str, Any]:
    eng = _get_engine()
    enabled_providers: Optional[List[str]] = None
    try:
        enabled_providers = list(getattr(eng, "enabled_providers", []))  # type: ignore
    except Exception:
        enabled_providers = None

    return {
        "engine_mode": _ENGINE_MODE,
        "engine_is_stub": _ENGINE_IS_STUB,
        "enabled_providers": enabled_providers,
    }


__all__ = [
    "UnifiedQuote",
    "QuoteSource",
    "DataEngine",
    "get_enriched_quote",
    "get_enriched_quotes",
    "get_engine_meta",
]
