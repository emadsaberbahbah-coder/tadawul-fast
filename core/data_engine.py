"""
core/data_engine.py
===============================================================
LEGACY COMPATIBILITY ADAPTER (v3.2.0)

PURPOSE:
- Acts as a bridge between older code requesting `core.data_engine` 
  and the modern `core.data_engine_v2` implementation.
- Ensures backward compatibility for imports and function calls.
- Provides a "Stub" mode if V2 fails to load, preventing app crashes.

MAPPING:
- get_enriched_quote(s) -> delegates to v2.DataEngine.get_quote(s)
- UnifiedQuote -> re-exports v2.UnifiedQuote
"""

from __future__ import annotations

import logging
import asyncio
from typing import Any, Dict, List, Optional
from datetime import datetime

# Pydantic V2 Imports
try:
    from pydantic import BaseModel, Field, ConfigDict
except ImportError:
    # Fallback for extreme environments
    class BaseModel: pass
    def Field(*args, **kwargs): return None
    def ConfigDict(*args, **kwargs): return None

logger = logging.getLogger("core.data_engine")

# ---------------------------------------------------------------------------
# 1. QuoteSource Polyfill (For Legacy Metadata Support)
# ---------------------------------------------------------------------------
class QuoteSource(BaseModel):
    """
    Legacy provider metadata model. 
    Kept ensures older type-checks don't fail.
    """
    model_config = ConfigDict(extra='ignore')
    provider: str
    latency_ms: Optional[float] = None
    timestamp_utc: Optional[datetime] = None
    raw: Optional[Dict[str, Any]] = None

# ---------------------------------------------------------------------------
# 2. Dynamic Import of V2 Engine
# ---------------------------------------------------------------------------
_ENGINE_MODE = "stub"

try:
    # Attempt to import the modern engine
    from core.data_engine_v2 import DataEngine as _V2DataEngine, UnifiedQuote
    _ENGINE_MODE = "v2"
    logger.info("Legacy Adapter: Linked successfully to DataEngine V2.")

except ImportError as e:
    logger.warning(f"Legacy Adapter: V2 Import Failed ({e}). Activating Stub Mode.")
    _ENGINE_MODE = "stub"

    # --- STUB DEFINITIONS (Safe Mode) ---
    
    class UnifiedQuote(BaseModel): # type: ignore
        """Safe placeholder if V2 is missing."""
        model_config = ConfigDict(populate_by_name=True, extra='ignore')
        symbol: str
        current_price: Optional[float] = None
        data_quality: str = "MISSING"
        error: Optional[str] = "Engine Unavailable"
        market: str = "UNKNOWN"
        
        # Legacy fields
        price: Optional[float] = None 
        change: Optional[float] = None
        
        def calculate_simple_scores(self): return self
        def dict(self, *args, **kwargs): return self.model_dump(*args, **kwargs)

    class _V2DataEngine: # type: ignore
        """Stub engine that returns empty/error states."""
        def __init__(self, *args, **kwargs):
            logger.error("Legacy Adapter: Initialized Stub Engine.")
        
        async def get_quote(self, ticker: str) -> UnifiedQuote:
            return UnifiedQuote(symbol=ticker, error="Engine V2 Missing")

        async def get_quotes(self, tickers: List[str]) -> List[UnifiedQuote]:
            return [UnifiedQuote(symbol=t, error="Engine V2 Missing") for t in tickers]
        
        async def aclose(self): pass

# ---------------------------------------------------------------------------
# 3. Singleton Instance Management
# ---------------------------------------------------------------------------

# Expose the class type for type hinting
DataEngine = _V2DataEngine 

_engine_instance = None

def _get_engine():
    """Singleton accessor for the DataEngine."""
    global _engine_instance
    if _engine_instance is None:
        try:
            _engine_instance = _V2DataEngine()
        except Exception as e:
            logger.critical(f"Legacy Adapter: Engine Init Failed: {e}")
            # Emergency crash-proof stub
            class CrashStub:
                async def get_quote(self, s): return UnifiedQuote(symbol=s, error=str(e))
                async def get_quotes(self, s): return [UnifiedQuote(symbol=x, error=str(e)) for x in s]
            _engine_instance = CrashStub()
    return _engine_instance

# ---------------------------------------------------------------------------
# 4. Public API Functions (The Compatibility Layer)
# ---------------------------------------------------------------------------

async def get_enriched_quote(symbol: str) -> UnifiedQuote:
    """
    Legacy Wrapper: Fetches a single quote using the V2 engine.
    Usage: quote = await get_enriched_quote("1120.SR")
    """
    engine = _get_engine()
    try:
        # V2 method name is 'get_quote'
        if hasattr(engine, "get_quote"):
            return await engine.get_quote(symbol)
        
        # Fallback if V2 interface changes
        return UnifiedQuote(symbol=symbol, error="Method Mismatch in Engine")
    except Exception as exc:
        logger.error(f"Legacy Adapter Error (Single): {exc}")
        return UnifiedQuote(symbol=symbol, error=str(exc))


async def get_enriched_quotes(symbols: List[str]) -> List[UnifiedQuote]:
    """
    Legacy Wrapper: Fetches multiple quotes using the V2 engine.
    Usage: quotes = await get_enriched_quotes(["1120.SR", "AAPL"])
    """
    clean_symbols = [s.strip() for s in (symbols or []) if s and isinstance(s, str) and s.strip()]
    
    if not clean_symbols:
        return []

    engine = _get_engine()
    try:
        # V2 method name is 'get_quotes'
        if hasattr(engine, "get_quotes"):
            return await engine.get_quotes(clean_symbols)
        
        # Fallback: Sequential loop if batch method missing
        return [await get_enriched_quote(s) for s in clean_symbols]
        
    except Exception as exc:
        logger.error(f"Legacy Adapter Error (Batch): {exc}")
        return [UnifiedQuote(symbol=s, error=str(exc)) for s in clean_symbols]

# ---------------------------------------------------------------------------
# 5. Meta Info
# ---------------------------------------------------------------------------

def get_engine_meta() -> Dict[str, Any]:
    """Returns diagnostics about the active engine."""
    return {
        "mode": _ENGINE_MODE,
        "is_stub": _ENGINE_MODE == "stub",
        "adapter_version": "3.2.0"
    }

__all__ = [
    "UnifiedQuote",
    "QuoteSource",
    "DataEngine",
    "get_enriched_quote",
    "get_enriched_quotes",
    "get_engine_meta",
]
