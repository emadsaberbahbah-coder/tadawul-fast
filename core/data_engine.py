# core/data_engine.py
"""
core/data_engine.py
===============================================================
LEGACY COMPATIBILITY ADAPTER (v3.6.0) — PROD SAFE

PURPOSE
- Keeps older imports working:
    from core.data_engine import DataEngine, UnifiedQuote, get_enriched_quote(s)
- Delegates all real work to: core.data_engine_v2.DataEngine
- Provides a safe STUB mode if V2 cannot be imported (never crashes the app)

MAPPING (best-effort)
- get_enriched_quote(symbol)    -> v2_engine.get_quote(symbol) OR v2_engine.get_enriched_quote(symbol)
- get_enriched_quotes(symbols)  -> v2_engine.get_quotes(symbols) OR v2_engine.get_enriched_quotes(symbols)
- get_quote(s) / get_quotes(ss) -> aliases for enriched variants

DESIGN
- Lazy V2 import to avoid cold-start failures and circular-import issues.
- Stable module-level singleton engine (shared across calls).
- Export DataEngine/UnifiedQuote symbols (V2 if available, else STUB).
"""

from __future__ import annotations

import logging
import threading
from datetime import datetime, timezone
from importlib import import_module
from typing import Any, Dict, List, Optional, Sequence, Tuple, Type

# Pydantic (best-effort)
try:
    from pydantic import BaseModel, ConfigDict, Field
except Exception:  # pragma: no cover
    class BaseModel:  # type: ignore
        def __init__(self, **kwargs):
            self.__dict__.update(kwargs)

        def model_dump(self, *a, **k):
            return dict(self.__dict__)

    def Field(default=None, **kwargs):  # type: ignore
        return default

    def ConfigDict(**kwargs):  # type: ignore
        return dict(kwargs)


logger = logging.getLogger("core.data_engine")

# ---------------------------------------------------------------------------
# 1) QuoteSource Polyfill (Legacy Metadata Support)
# ---------------------------------------------------------------------------
class QuoteSource(BaseModel):
    """Legacy provider metadata model kept so older type-checks don't fail."""
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

    # If previously failed, keep stub mode (Render won't hot-fix at runtime)
    if _ENGINE_MODE == "stub" and _V2_LOAD_ERR:
        return False, _V2_LOAD_ERR

    with _V2_LOAD_LOCK:
        if _V2:
            return True, None

        try:
            mod = import_module("core.data_engine_v2")
            v2_engine = getattr(mod, "DataEngine", None)
            v2_uq = getattr(mod, "UnifiedQuote", None)

            # If UnifiedQuote is not in data_engine_v2, try schemas (optional)
            if v2_uq is None:
                try:
                    schemas = import_module("core.schemas")
                    v2_uq = getattr(schemas, "UnifiedQuote", None)
                except Exception:
                    v2_uq = None

            if v2_engine is None or v2_uq is None:
                raise ImportError("core.data_engine_v2 missing DataEngine and/or UnifiedQuote")

            _V2["DataEngine"] = v2_engine
            _V2["UnifiedQuote"] = v2_uq
            _ENGINE_MODE = "v2"
            _V2_LOAD_ERR = None

            logger.info("Legacy Adapter: linked to DataEngine V2 successfully.")
            return True, None

        except Exception as exc:
            _ENGINE_MODE = "stub"
            _V2_LOAD_ERR = str(exc)
            logger.warning("Legacy Adapter: V2 import failed (%s). Using STUB mode.", exc)
            return False, _V2_LOAD_ERR


# ---------------------------------------------------------------------------
# 3) STUB DEFINITIONS (Safe Mode)
# ---------------------------------------------------------------------------
class _StubUnifiedQuote(BaseModel):
    """
    A wide-but-light stub to keep downstream code (routes/enriched_quote, EnrichedQuote)
    from crashing when V2 is unavailable.
    """
    model_config = ConfigDict(populate_by_name=True, extra="ignore")

    symbol: str

    # identity
    name: Optional[str] = None
    market: str = "UNKNOWN"
    exchange: Optional[str] = None
    currency: Optional[str] = None

    # price / trading
    current_price: Optional[float] = None
    previous_close: Optional[float] = None
    open: Optional[float] = None
    day_high: Optional[float] = None
    day_low: Optional[float] = None
    week_52_high: Optional[float] = None
    week_52_low: Optional[float] = None
    volume: Optional[float] = None
    avg_volume_30d: Optional[float] = None
    value_traded: Optional[float] = None

    price_change: Optional[float] = None
    percent_change: Optional[float] = None

    # fundamentals (optional)
    market_cap: Optional[float] = None
    shares_outstanding: Optional[float] = None
    free_float_pct: Optional[float] = None
    pe_ttm: Optional[float] = None
    pe_forward: Optional[float] = None
    pb: Optional[float] = None
    ps: Optional[float] = None
    eps_ttm: Optional[float] = None
    eps_forward: Optional[float] = None
    dividend_yield: Optional[float] = None

    # scoring / analytics (optional)
    quality_score: Optional[float] = None
    value_score: Optional[float] = None
    momentum_score: Optional[float] = None
    risk_score: Optional[float] = None
    opportunity_score: Optional[float] = None

    # provenance
    data_source: str = "none"
    data_quality: str = "MISSING"
    last_updated_utc: str = Field(default_factory=lambda: datetime.now(timezone.utc).isoformat())
    error: Optional[str] = "Engine Unavailable"

    # legacy aliases sometimes used elsewhere
    price: Optional[float] = None
    change: Optional[float] = None

    def finalize(self) -> "_StubUnifiedQuote":
        # keep interface compatible with V2 models that provide finalize()
        if self.current_price is None and self.price is not None:
            self.current_price = self.price
        if self.price_change is None and self.change is not None:
            self.price_change = self.change
        if self.percent_change is None and self.price_change is not None and self.previous_close:
            try:
                self.percent_change = (self.price_change / self.previous_close) * 100.0
            except Exception:
                pass
        return self

    def dict(self, *args, **kwargs):  # legacy convenience
        if hasattr(self, "model_dump"):
            return self.model_dump(*args, **kwargs)  # type: ignore
        return dict(self.__dict__)


class _StubEngine:
    """
    Stub engine that matches the V2 async interface closely enough for routes.
    """
    def __init__(self, *args, **kwargs) -> None:
        logger.error("Legacy Adapter: initialized STUB engine (V2 missing/unavailable).")

    async def get_quote(self, symbol: str) -> _StubUnifiedQuote:
        sym = str(symbol or "").strip()
        return _StubUnifiedQuote(symbol=sym, error="Engine V2 Missing").finalize()

    async def get_quotes(self, symbols: List[str]) -> List[_StubUnifiedQuote]:
        out: List[_StubUnifiedQuote] = []
        for s in (symbols or []):
            sym = str(s or "").strip()
            if sym:
                out.append(_StubUnifiedQuote(symbol=sym, error="Engine V2 Missing").finalize())
        return out

    # legacy naming (some callers might use these)
    async def get_enriched_quote(self, symbol: str) -> _StubUnifiedQuote:
        return await self.get_quote(symbol)

    async def get_enriched_quotes(self, symbols: List[str]) -> List[_StubUnifiedQuote]:
        return await self.get_quotes(symbols)

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


def _instantiate_engine(EngineCls: Type[Any]) -> Any:
    """
    Instantiate DataEngine in a tolerant way (signature may evolve).
    """
    # 1) no-arg
    try:
        return EngineCls()
    except TypeError:
        pass

    # 2) try passing settings (best-effort)
    try:
        from core.config import get_settings as _gs  # type: ignore
        return EngineCls(_gs())
    except Exception:
        pass

    # 3) last resort: generic
    return EngineCls()


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

        _load_v2()
        UnifiedQuote, DataEngine = _get_types()  # refresh public bindings

        try:
            _engine_instance = _instantiate_engine(DataEngine)  # v2 or stub
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
def _clean_symbols(symbols: Sequence[Any]) -> List[str]:
    out: List[str] = []
    for s in (symbols or []):
        if s is None:
            continue
        x = str(s).strip()
        if x:
            out.append(x)
    return out


async def get_enriched_quote(symbol: str) -> Any:
    """
    Legacy Wrapper: Fetches a single quote using the V2 engine.
    """
    eng = _get_engine()
    sym = str(symbol or "").strip()
    if not sym:
        return UnifiedQuote(symbol="", error="Empty symbol").finalize()

    try:
        if hasattr(eng, "get_quote"):
            return await eng.get_quote(sym)
        if hasattr(eng, "get_enriched_quote"):
            return await eng.get_enriched_quote(sym)
        return UnifiedQuote(symbol=sym, error="Engine method mismatch: get_quote missing").finalize()
    except Exception as exc:
        logger.error("Legacy Adapter Error (Single): %s", exc)
        return UnifiedQuote(symbol=sym, error=str(exc)).finalize()


async def get_enriched_quotes(symbols: List[str]) -> List[Any]:
    """
    Legacy Wrapper: Fetches multiple quotes using the V2 engine.
    """
    clean = _clean_symbols(symbols)
    if not clean:
        return []

    eng = _get_engine()
    try:
        if hasattr(eng, "get_quotes"):
            return await eng.get_quotes(clean)
        if hasattr(eng, "get_enriched_quotes"):
            return await eng.get_enriched_quotes(clean)

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
    providers: List[str] = []
    ksa_providers: List[str] = []

    try:
        from core.config import get_settings as _gs  # type: ignore

        s = _gs()

        # prefer computed lists if present
        providers = list(getattr(s, "enabled_providers", []) or []) or list(getattr(s, "PROVIDERS", []) or [])
        ksa_providers = list(getattr(s, "enabled_ksa_providers", []) or []) or list(getattr(s, "KSA_PROVIDERS", []) or [])
    except Exception:
        pass

    return {
        "mode": _ENGINE_MODE,
        "is_stub": _ENGINE_MODE == "stub",
        "adapter_version": "3.6.0",
        "v2_loaded": bool(ok),
        "v2_error": err or _V2_LOAD_ERR,
        "providers": providers,
        "ksa_providers": ksa_providers,
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
