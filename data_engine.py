# core/data_engine.py
"""
core/data_engine.py
===============================================================
LEGACY COMPATIBILITY ADAPTER (v3.4.0)

PURPOSE
- Keeps older imports working: `from core.data_engine import ...`
- Delegates all real work to `core.data_engine_v2.DataEngine`
- Provides a safe STUB mode if V2 cannot be imported (never crashes the app)

MAPPING
- get_enriched_quote(s)  -> v2.DataEngine.get_quote(s)
- get_enriched_quotes(ss)-> v2.DataEngine.get_quotes(ss)
- UnifiedQuote           -> re-export v2.UnifiedQuote (or stub)
- DataEngine             -> re-export v2.DataEngine (or stub)

NOTES
- Import of V2 is LAZY to reduce startup failures and avoid hard import cycles.
"""

from __future__ import annotations

import logging
import threading
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple, Type

# Pydantic (best-effort)
try:
    from pydantic import BaseModel, ConfigDict, Field
except Exception:  # pragma: no cover
    class BaseModel:  # type: ignore
        def __init__(self, **kwargs): self.__dict__.update(kwargs)
        def model_dump(self, *a, **k): return dict(self.__dict__)
    def Field(default=None, **kwargs): return default  # type: ignore
    def ConfigDict(**kwargs): return dict(kwargs)  # type: ignore

logger = logging.getLogger("core.data_engine")

# ---------------------------------------------------------------------------
# 1) QuoteSource Polyfill (Legacy Metadata Support)
# ---------------------------------------------------------------------------
class QuoteSource(BaseModel):
    """
    Legacy provider metadata model kept so older type-checks don't fail.
    """
    model_config = ConfigDict(extra="ignore")
    provider: str
    latency_ms: Optional[float] = None
    timestamp_utc: Optional[datetime] = None
    raw: Optional[Dict[str, Any]] = None


# ---------------------------------------------------------------------------
# 2) Lazy V2 Loader (never raises to caller)
# ---------------------------------------------------------------------------
_ENGINE_MODE: str = "stub"
_V2: Dict[str, Any] = {}
_V2_LOAD_ERR: Optional[str] = None
_V2_LOAD_LOCK = threading.Lock()


def _load_v2() -> Tuple[bool, Optional[str]]:
    """
    Loads core.data_engine_v2 lazily and caches references.
    Returns: (ok, error_message)
    """
    global _ENGINE_MODE, _V2, _V2_LOAD_ERR
    if _V2:
        return True, None
    if _ENGINE_MODE == "stub" and _V2_LOAD_ERR:
        return False, _V2_LOAD_ERR

    with _V2_LOAD_LOCK:
        if _V2:
            return True, None
        try:
            from core.data_engine_v2 import DataEngine as V2DataEngine, UnifiedQuote as V2UnifiedQuote  # type: ignore

            _V2["DataEngine"] = V2DataEngine
            _V2["UnifiedQuote"] = V2UnifiedQuote
            _ENGINE_MODE = "v2"
            _V2_LOAD_ERR = None
            logger.info("Legacy Adapter: linked to DataEngine V2 successfully.")
            return True, None
        except Exception as exc:  # ImportError + runtime import issues
            _ENGINE_MODE = "stub"
            _V2_LOAD_ERR = str(exc)
            logger.warning("Legacy Adapter: V2 import failed (%s). Using STUB mode.", exc)
            return False, _V2_LOAD_ERR


# ---------------------------------------------------------------------------
# 3) STUB DEFINITIONS (Safe Mode)
# ---------------------------------------------------------------------------
class _StubUnifiedQuote(BaseModel):
    model_config = ConfigDict(populate_by_name=True, extra="ignore")

    symbol: str
    market: str = "UNKNOWN"
    currency: Optional[str] = None

    # Common fields used by Sheets/routes
    current_price: Optional[float] = None
    previous_close: Optional[float] = None
    price_change: Optional[float] = None
    percent_change: Optional[float] = None
    volume: Optional[float] = None

    data_source: str = "none"
    data_quality: str = "MISSING"
    last_updated_utc: str = Field(default_factory=lambda: datetime.now(timezone.utc).isoformat())
    error: Optional[str] = "Engine Unavailable"

    # Very old legacy aliases sometimes used elsewhere
    price: Optional[float] = None
    change: Optional[float] = None

    def finalize(self) -> "_StubUnifiedQuote":
        # keep interface compatible with v2
        if self.current_price is None and self.price is not None:
            self.current_price = self.price
        if self.price_change is None and self.change is not None:
            self.price_change = self.change
        return self

    def dict(self, *args, **kwargs):  # legacy convenience
        if hasattr(self, "model_dump"):
            return self.model_dump(*args, **kwargs)  # type: ignore
        return dict(self.__dict__)


class _StubEngine:
    def __init__(self, *args, **kwargs) -> None:
        logger.error("Legacy Adapter: initialized STUB engine (V2 missing/unavailable).")

    async def get_quote(self, symbol: str) -> _StubUnifiedQuote:
        return _StubUnifiedQuote(symbol=str(symbol or ""), error="Engine V2 Missing").finalize()

    async def get_quotes(self, symbols: List[str]) -> List[_StubUnifiedQuote]:
        out: List[_StubUnifiedQuote] = []
        for s in (symbols or []):
            if s and str(s).strip():
                out.append(_StubUnifiedQuote(symbol=str(s).strip(), error="Engine V2 Missing").finalize())
        return out

    async def aclose(self) -> None:
        return None


# ---------------------------------------------------------------------------
# 4) Public re-exports: UnifiedQuote + DataEngine (either V2 or STUB)
# ---------------------------------------------------------------------------
def _get_types() -> Tuple[Type[Any], Type[Any]]:
    ok, _ = _load_v2()
    if ok:
        return _V2["UnifiedQuote"], _V2["DataEngine"]
    return _StubUnifiedQuote, _StubEngine


UnifiedQuote, DataEngine = _get_types()  # type: ignore


# ---------------------------------------------------------------------------
# 5) Singleton Instance Management (shared across module calls)
# ---------------------------------------------------------------------------
_engine_instance: Optional[Any] = None
_engine_init_lock = threading.Lock()


def _get_engine() -> Any:
    """
    Singleton accessor for the underlying engine.
    Never raises; returns a stub engine on failure.
    """
    global _engine_instance, UnifiedQuote, DataEngine

    if _engine_instance is not None:
        return _engine_instance

    with _engine_init_lock:
        if _engine_instance is not None:
            return _engine_instance

        ok, err = _load_v2()
        UnifiedQuote, DataEngine = _get_types()  # refresh public bindings

        try:
            _engine_instance = DataEngine()  # v2 or stub
            return _engine_instance
        except Exception as exc:
            logger.critical("Legacy Adapter: engine init failed: %s", exc)

            # Emergency crash-proof stub
            class _CrashStub:
                async def get_quote(self, s):
                    return UnifiedQuote(symbol=str(s or ""), error=str(exc)).finalize()

                async def get_quotes(self, ss):
                    clean = [str(x).strip() for x in (ss or []) if x and str(x).strip()]
                    return [UnifiedQuote(symbol=x, error=str(exc)).finalize() for x in clean]

                async def aclose(self):
                    return None

            _engine_instance = _CrashStub()
            return _engine_instance


async def close_engine() -> None:
    """
    Optional: call on shutdown to close underlying http clients cleanly.
    """
    global _engine_instance
    eng = _engine_instance
    _engine_instance = None
    try:
        if eng is not None and hasattr(eng, "aclose"):
            await eng.aclose()
    except Exception:
        pass


# ---------------------------------------------------------------------------
# 6) Public API Functions (Compatibility Layer)
# ---------------------------------------------------------------------------
async def get_enriched_quote(symbol: str) -> Any:
    """
    Legacy Wrapper: Fetches a single quote using the V2 engine.

    Usage:
      quote = await get_enriched_quote("1120.SR")
    """
    eng = _get_engine()
    sym = str(symbol or "").strip()
    if not sym:
        return UnifiedQuote(symbol="", error="Empty symbol").finalize()

    try:
        if hasattr(eng, "get_quote"):
            return await eng.get_quote(sym)
        return UnifiedQuote(symbol=sym, error="Engine method mismatch: get_quote missing").finalize()
    except Exception as exc:
        logger.error("Legacy Adapter Error (Single): %s", exc)
        return UnifiedQuote(symbol=sym, error=str(exc)).finalize()


async def get_enriched_quotes(symbols: List[str]) -> List[Any]:
    """
    Legacy Wrapper: Fetches multiple quotes using the V2 engine.

    Usage:
      quotes = await get_enriched_quotes(["1120.SR", "AAPL"])
    """
    clean = [str(s).strip() for s in (symbols or []) if s and str(s).strip()]
    if not clean:
        return []

    eng = _get_engine()
    try:
        if hasattr(eng, "get_quotes"):
            return await eng.get_quotes(clean)
        # fallback: sequential
        out: List[Any] = []
        for s in clean:
            out.append(await get_enriched_quote(s))
        return out
    except Exception as exc:
        logger.error("Legacy Adapter Error (Batch): %s", exc)
        return [UnifiedQuote(symbol=s, error=str(exc)).finalize() for s in clean]


# Backward-compatible aliases some modules may use
async def get_quote(symbol: str) -> Any:
    return await get_enriched_quote(symbol)


async def get_quotes(symbols: List[str]) -> List[Any]:
    return await get_enriched_quotes(symbols)


# ---------------------------------------------------------------------------
# 7) Meta Info
# ---------------------------------------------------------------------------
def get_engine_meta() -> Dict[str, Any]:
    ok, err = _load_v2()
    prov = []
    try:
        # try to read providers from core.config if available (best-effort)
        from core.config import get_settings as _gs  # type: ignore
        s = _gs()
        prov_list = getattr(s, "providers_list", None)
        if isinstance(prov_list, list):
            prov = prov_list
        else:
            prov_csv = getattr(s, "providers", "") or ""
            prov = [p.strip().lower() for p in str(prov_csv).split(",") if p.strip()]
    except Exception:
        pass

    return {
        "mode": _ENGINE_MODE,
        "is_stub": _ENGINE_MODE == "stub",
        "adapter_version": "3.4.0",
        "v2_loaded": bool(ok),
        "v2_error": err or _V2_LOAD_ERR,
        "providers": prov,
    }


__all__ = [
    "UnifiedQuote",
    "QuoteSource",
    "DataEngine",
    "get_quote",
    "get_quotes",
    "get_enriched_quote",
    "get_enriched_quotes",
    "close_engine",
    "get_engine_meta",
]
