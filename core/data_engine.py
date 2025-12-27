# core/data_engine.py  (FULL REPLACEMENT)
"""
core/data_engine.py
===============================================================
LEGACY COMPATIBILITY ADAPTER (v3.6.2) — PROD SAFE (TRUE LAZY)

Same as v3.6.1, but:
- ✅ Slightly safer v2 import resolution (UnifiedQuote now exists in v2)
- ✅ Keeps strict "never crash startup" policy
"""

from __future__ import annotations

import inspect
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


class QuoteSource(BaseModel):
    """Legacy provider metadata model kept so older type-checks don't fail."""
    model_config = ConfigDict(extra="ignore")
    provider: str
    latency_ms: Optional[float] = None
    timestamp_utc: Optional[datetime] = None
    raw: Optional[Dict[str, Any]] = None


async def _maybe_await(v: Any) -> Any:
    if inspect.isawaitable(v):
        return await v
    return v


_ENGINE_MODE: str = "unknown"  # unknown | v2 | stub
_V2: Dict[str, Any] = {}
_V2_LOAD_ERR: Optional[str] = None
_V2_LOAD_LOCK = threading.Lock()


def _load_v2() -> Tuple[bool, Optional[str]]:
    global _ENGINE_MODE, _V2, _V2_LOAD_ERR

    if _V2:
        return True, None

    if _ENGINE_MODE == "stub" and _V2_LOAD_ERR:
        return False, _V2_LOAD_ERR

    with _V2_LOAD_LOCK:
        if _V2:
            return True, None

        try:
            mod = import_module("core.data_engine_v2")

            v2_engine = getattr(mod, "DataEngine", None)
            v2_uq = getattr(mod, "UnifiedQuote", None)
            v2_norm = getattr(mod, "normalize_symbol", None)

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
            if callable(v2_norm):
                _V2["normalize_symbol"] = v2_norm

            _ENGINE_MODE = "v2"
            _V2_LOAD_ERR = None
            logger.info("Legacy Adapter: linked to DataEngine V2 successfully.")
            return True, None

        except Exception as exc:
            _ENGINE_MODE = "stub"
            _V2_LOAD_ERR = str(exc)
            logger.warning("Legacy Adapter: V2 import failed (%s). Using STUB mode.", exc)
            return False, _V2_LOAD_ERR


def _get_uq_cls() -> Type[Any]:
    ok, _ = _load_v2()
    if ok and _V2.get("UnifiedQuote"):
        return _V2["UnifiedQuote"]
    return _StubUnifiedQuote  # type: ignore


def _get_engine_cls() -> Type[Any]:
    ok, _ = _load_v2()
    if ok and _V2.get("DataEngine"):
        return _V2["DataEngine"]
    return _StubEngine  # type: ignore


class _StubUnifiedQuote(BaseModel):
    model_config = ConfigDict(populate_by_name=True, extra="ignore")

    symbol: str

    name: Optional[str] = None
    market: str = "UNKNOWN"
    exchange: Optional[str] = None
    currency: Optional[str] = None

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

    quality_score: Optional[float] = None
    value_score: Optional[float] = None
    momentum_score: Optional[float] = None
    risk_score: Optional[float] = None
    opportunity_score: Optional[float] = None

    data_source: str = "none"
    data_quality: str = "MISSING"
    last_updated_utc: str = Field(default_factory=lambda: datetime.now(timezone.utc).isoformat())
    error: Optional[str] = "Engine Unavailable"

    price: Optional[float] = None
    change: Optional[float] = None

    def finalize(self) -> "_StubUnifiedQuote":
        if self.current_price is None and self.price is not None:
            self.current_price = self.price
        if self.price_change is None and self.change is not None:
            self.price_change = self.change
        if self.percent_change is None and self.price_change is not None and self.previous_close not in (None, 0):
            try:
                self.percent_change = (self.price_change / self.previous_close) * 100.0
            except Exception:
                pass
        return self

    def dict(self, *args, **kwargs):
        if hasattr(self, "model_dump"):
            return self.model_dump(*args, **kwargs)  # type: ignore
        return dict(self.__dict__)


_STUB_LOGGED = False


class _StubEngine:
    def __init__(self, *args, **kwargs) -> None:
        global _STUB_LOGGED
        if not _STUB_LOGGED:
            _STUB_LOGGED = True
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

    async def get_enriched_quote(self, symbol: str) -> _StubUnifiedQuote:
        return await self.get_quote(symbol)

    async def get_enriched_quotes(self, symbols: List[str]) -> List[_StubUnifiedQuote]:
        return await self.get_quotes(symbols)

    async def aclose(self) -> None:
        return None


_engine_instance: Optional[Any] = None
_engine_init_lock = threading.Lock()


def _instantiate_engine(EngineCls: Type[Any]) -> Any:
    try:
        return EngineCls()
    except TypeError:
        pass

    try:
        from core.config import get_settings as _gs  # type: ignore
        return EngineCls(settings=_gs())
    except Exception:
        pass

    try:
        from core.config import get_settings as _gs  # type: ignore
        return EngineCls(_gs())
    except Exception:
        pass

    return EngineCls()


def _get_engine() -> Any:
    global _engine_instance

    if _engine_instance is not None:
        return _engine_instance

    with _engine_init_lock:
        if _engine_instance is not None:
            return _engine_instance

        _load_v2()
        EngineCls = _get_engine_cls()
        UQ = _get_uq_cls()

        try:
            _engine_instance = _instantiate_engine(EngineCls)
            return _engine_instance
        except Exception as exc:
            logger.critical("Legacy Adapter: engine init failed: %s", exc)

            class _CrashStub:
                async def get_quote(self, s):
                    try:
                        return UQ(symbol=str(s or ""), error=str(exc)).finalize()
                    except Exception:
                        return _StubUnifiedQuote(symbol=str(s or ""), error=str(exc)).finalize()

                async def get_quotes(self, ss):
                    clean = [str(x).strip() for x in (ss or []) if x and str(x).strip()]
                    out = []
                    for x in clean:
                        try:
                            out.append(UQ(symbol=x, error=str(exc)).finalize())
                        except Exception:
                            out.append(_StubUnifiedQuote(symbol=x, error=str(exc)).finalize())
                    return out

                async def aclose(self):
                    return None

            _engine_instance = _CrashStub()
            return _engine_instance


async def close_engine() -> None:
    global _engine_instance
    eng = _engine_instance
    _engine_instance = None
    try:
        if eng is not None and hasattr(eng, "aclose"):
            await _maybe_await(eng.aclose())
    except Exception:
        pass


def normalize_symbol(symbol: str) -> str:
    s = (symbol or "").strip()
    if not s:
        return ""

    ok, _ = _load_v2()
    fn = _V2.get("normalize_symbol")
    if ok and callable(fn):
        try:
            out = fn(s)
            return (out or "").strip().upper()
        except Exception:
            pass

    su = s.strip().upper()
    if su.startswith("TADAWUL:"):
        su = su.split(":", 1)[1].strip()
    if su.endswith(".TADAWUL"):
        su = su.replace(".TADAWUL", "")
    if any(ch in su for ch in ("=", "^")):
        return su
    if "." in su:
        return su
    if su.isdigit():
        return f"{su}.SR"
    if su.isalpha():
        return f"{su}.US"
    return su


def _clean_symbols(symbols: Sequence[Any]) -> List[str]:
    seen = set()
    out: List[str] = []
    for s in (symbols or []):
        if s is None:
            continue
        x = str(s).strip()
        if not x:
            continue
        xu = x.upper()
        if xu in seen:
            continue
        seen.add(xu)
        out.append(x)
    return out


async def get_enriched_quote(symbol: str) -> Any:
    sym_in = str(symbol or "").strip()
    UQ = _get_uq_cls()

    if not sym_in:
        try:
            return UQ(symbol="", error="Empty symbol").finalize()
        except Exception:
            return _StubUnifiedQuote(symbol="", error="Empty symbol").finalize()

    sym = normalize_symbol(sym_in) or sym_in

    eng = _get_engine()
    try:
        if hasattr(eng, "get_enriched_quote"):
            return await _maybe_await(eng.get_enriched_quote(sym))
        if hasattr(eng, "get_quote"):
            return await _maybe_await(eng.get_quote(sym))
        try:
            return UQ(symbol=sym, error="Engine method mismatch: no get_enriched_quote/get_quote").finalize()
        except Exception:
            return _StubUnifiedQuote(symbol=sym, error="Engine method mismatch").finalize()
    except Exception as exc:
        logger.error("Legacy Adapter Error (Single): %s", exc)
        try:
            return UQ(symbol=sym, error=str(exc)).finalize()
        except Exception:
            return _StubUnifiedQuote(symbol=sym, error=str(exc)).finalize()


async def get_enriched_quotes(symbols: List[str]) -> List[Any]:
    clean = _clean_symbols(symbols)
    if not clean:
        return []

    eng = _get_engine()
    UQ = _get_uq_cls()

    try:
        if hasattr(eng, "get_enriched_quotes"):
            return await _maybe_await(eng.get_enriched_quotes([normalize_symbol(x) or x for x in clean]))
        if hasattr(eng, "get_quotes"):
            return await _maybe_await(eng.get_quotes([normalize_symbol(x) or x for x in clean]))

        out: List[Any] = []
        for s in clean:
            out.append(await get_enriched_quote(s))
        return out
    except Exception as exc:
        logger.error("Legacy Adapter Error (Batch): %s", exc)
        out: List[Any] = []
        for s in clean:
            sym = normalize_symbol(s) or s
            try:
                out.append(UQ(symbol=sym, error=str(exc)).finalize())
            except Exception:
                out.append(_StubUnifiedQuote(symbol=sym, error=str(exc)).finalize())
        return out


async def get_quote(symbol: str) -> Any:
    return await get_enriched_quote(symbol)


async def get_quotes(symbols: List[str]) -> List[Any]:
    return await get_enriched_quotes(symbols)


def get_engine_meta() -> Dict[str, Any]:
    ok, err = _load_v2()
    providers: List[str] = []
    ksa_providers: List[str] = []

    try:
        from core.config import get_settings as _gs  # type: ignore
        s = _gs()
        providers = list(getattr(s, "enabled_providers", []) or []) or list(getattr(s, "PROVIDERS", []) or [])
        ksa_providers = list(getattr(s, "enabled_ksa_providers", []) or []) or list(getattr(s, "KSA_PROVIDERS", []) or [])
    except Exception:
        pass

    return {
        "mode": _ENGINE_MODE,
        "is_stub": _ENGINE_MODE == "stub",
        "adapter_version": "3.6.2",
        "v2_loaded": bool(ok),
        "v2_error": err or _V2_LOAD_ERR,
        "providers": providers,
        "ksa_providers": ksa_providers,
    }


def __getattr__(name: str) -> Any:  # pragma: no cover
    if name == "UnifiedQuote":
        return _get_uq_cls()
    if name == "DataEngine":
        return _get_engine_cls()
    if name == "ENGINE_MODE":
        return _ENGINE_MODE
    raise AttributeError(name)


__all__ = [
    "QuoteSource",
    "normalize_symbol",
    "UnifiedQuote",
    "DataEngine",
    "get_quote",
    "get_quotes",
    "get_enriched_quote",
    "get_enriched_quotes",
    "close_engine",
    "get_engine_meta",
]
