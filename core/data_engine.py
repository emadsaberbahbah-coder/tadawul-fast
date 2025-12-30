# core/data_engine.py  (FULL REPLACEMENT)
"""
core/data_engine.py
===============================================================
LEGACY COMPATIBILITY ADAPTER (v3.8.2) — PROD SAFE (TRUE LAZY)

What this module guarantees
- ✅ Never crashes app startup (no hard dependency on data_engine_v2 at import-time).
- ✅ Best-effort delegation to core.data_engine_v2 when available.
- ✅ Provides stable legacy API surface:
    - normalize_symbol()
    - get_quote / get_quotes
    - get_enriched_quote / get_enriched_quotes
    - close_engine()
    - get_engine_meta()
    - DataEngine wrapper class (lazy, safe)
- ✅ Works whether V2 methods are async OR sync (auto await).
- ✅ Handles V2 returning list OR dict OR (payload, err) tuples (defensive).
- ✅ If V2 missing/broken -> returns stub quotes with data_quality="MISSING" and error.
- ✅ Never leaks secrets in meta/health.

v3.8.2 improvements
- ✅ Fixes Pydantic v1 fallback: avoids model_config usage when ConfigDict is unavailable.
- ✅ More robust V2 linking: accepts UnifiedQuote from v2 OR core.schemas OR core.models.schemas fallback.
- ✅ Better tuple-unwrapping + list normalization for both single and batch.
- ✅ Engine singleton init: safer signature probing (settings kwarg, settings positional, none).
- ✅ get_engine_meta reads providers from env/settings safely and can show v2 module version if present.
"""

from __future__ import annotations

import inspect
import logging
import os
import threading
from datetime import datetime, timezone
from importlib import import_module
from typing import Any, Dict, List, Optional, Sequence, Tuple, Type

# -----------------------------------------------------------------------------
# Pydantic (best-effort) with robust fallback
# -----------------------------------------------------------------------------
try:
    from pydantic import BaseModel, Field  # type: ignore

    try:
        from pydantic import ConfigDict  # type: ignore

        _PYDANTIC_HAS_CONFIGDICT = True
    except Exception:  # pragma: no cover
        ConfigDict = None  # type: ignore
        _PYDANTIC_HAS_CONFIGDICT = False

except Exception:  # pragma: no cover
    _PYDANTIC_HAS_CONFIGDICT = False

    class BaseModel:  # type: ignore
        def __init__(self, **kwargs):
            self.__dict__.update(kwargs)

        def model_dump(self, *a, **k):
            return dict(self.__dict__)

        def dict(self, *a, **k):  # legacy
            return dict(self.__dict__)

    def Field(default=None, **kwargs):  # type: ignore
        return default

    ConfigDict = None  # type: ignore


ADAPTER_VERSION = "3.8.2"
logger = logging.getLogger("core.data_engine")

# =============================================================================
# Legacy type kept so older type-checks don't fail
# =============================================================================
class QuoteSource(BaseModel):
    """Legacy provider metadata model kept so older type-checks don't fail."""

    if _PYDANTIC_HAS_CONFIGDICT and ConfigDict is not None:
        model_config = ConfigDict(extra="ignore")  # type: ignore
    else:  # pydantic v1 or fallback
        class Config:  # pragma: no cover
            extra = "ignore"

    provider: str
    latency_ms: Optional[float] = None
    timestamp_utc: Optional[datetime] = None
    raw: Optional[Dict[str, Any]] = None


# =============================================================================
# Small helpers
# =============================================================================
async def _maybe_await(v: Any) -> Any:
    if inspect.isawaitable(v):
        return await v
    return v


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _finalize_quote(obj: Any) -> Any:
    """If the quote model provides finalize(), call it. Otherwise return as-is."""
    try:
        fn = getattr(obj, "finalize", None)
        if callable(fn):
            return fn()
    except Exception:
        pass
    return obj


def _unwrap_payload(x: Any) -> Any:
    """
    Some engines/providers might return (payload, err).
    We always keep payload if present.
    """
    try:
        if isinstance(x, tuple) and len(x) == 2:
            return x[0]
    except Exception:
        pass
    return x


def _as_list(x: Any) -> List[Any]:
    if x is None:
        return []
    if isinstance(x, list):
        return x
    if isinstance(x, dict):
        # dict[symbol -> quote] style
        return list(x.values())
    return [x]


# =============================================================================
# V2 linkage (TRUE LAZY)
# =============================================================================
_ENGINE_MODE: str = "unknown"  # unknown | v2 | stub
_V2: Dict[str, Any] = {}
_V2_LOAD_ERR: Optional[str] = None
_V2_LOAD_LOCK = threading.Lock()


def _load_v2() -> Tuple[bool, Optional[str]]:
    """
    Best-effort import of core.data_engine_v2.
    Never raises out of here.
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
            mod = import_module("core.data_engine_v2")

            v2_engine = getattr(mod, "DataEngine", None)
            v2_uq = getattr(mod, "UnifiedQuote", None)
            v2_norm = getattr(mod, "normalize_symbol", None)
            v2_get_engine = getattr(mod, "get_engine", None)

            # UnifiedQuote may live elsewhere depending on repo evolution
            if v2_uq is None:
                for path in ("core.schemas", "core.models.schemas", "schemas"):
                    try:
                        schemas = import_module(path)
                        v2_uq = getattr(schemas, "UnifiedQuote", None)
                        if v2_uq is not None:
                            break
                    except Exception:
                        continue

            if v2_engine is None or v2_uq is None:
                raise ImportError("core.data_engine_v2 missing DataEngine and/or UnifiedQuote")

            _V2["module"] = mod
            _V2["DataEngine"] = v2_engine
            _V2["UnifiedQuote"] = v2_uq
            if callable(v2_norm):
                _V2["normalize_symbol"] = v2_norm
            if callable(v2_get_engine):
                _V2["get_engine"] = v2_get_engine

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


def _get_v2_engine_cls() -> Type[Any]:
    ok, _ = _load_v2()
    if ok and _V2.get("DataEngine"):
        return _V2["DataEngine"]
    return _StubEngine  # type: ignore


# =============================================================================
# Stub models (when V2 missing)
# =============================================================================
class _StubUnifiedQuote(BaseModel):
    if _PYDANTIC_HAS_CONFIGDICT and ConfigDict is not None:
        model_config = ConfigDict(populate_by_name=True, extra="ignore")  # type: ignore
    else:  # pragma: no cover
        class Config:
            extra = "ignore"
            allow_population_by_field_name = True

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
    last_updated_utc: str = Field(default_factory=_utc_now_iso)
    error: Optional[str] = "Engine Unavailable"

    # legacy aliases
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


# =============================================================================
# Engine singleton (lazy)
# =============================================================================
_engine_instance: Optional[Any] = None
_engine_init_lock = threading.Lock()


def _instantiate_engine(EngineCls: Type[Any]) -> Any:
    """
    Instantiate V2 engine class with best-effort signatures.
    Never raises outward; caller handles exceptions.
    """
    # 1) no-arg
    try:
        return EngineCls()
    except TypeError:
        pass

    # 2) settings kwarg
    try:
        from core.config import get_settings as _gs  # type: ignore

        return EngineCls(settings=_gs())
    except Exception:
        pass

    # 3) settings positional
    try:
        from core.config import get_settings as _gs  # type: ignore

        return EngineCls(_gs())
    except Exception:
        pass

    # 4) last resort
    return EngineCls()


def _get_engine_sync() -> Any:
    """
    Internal sync getter for engine singleton.
    """
    global _engine_instance

    if _engine_instance is not None:
        return _engine_instance

    with _engine_init_lock:
        if _engine_instance is not None:
            return _engine_instance

        _load_v2()
        EngineCls = _get_v2_engine_cls()
        UQ = _get_uq_cls()

        try:
            _engine_instance = _instantiate_engine(EngineCls)
            return _engine_instance
        except Exception as exc:
            logger.critical("Legacy Adapter: engine init failed: %s", exc)

            class _CrashStub:
                async def get_quote(self, s):
                    sym = str(s or "").strip()
                    try:
                        q = UQ(symbol=sym, data_quality="MISSING", error=str(exc))
                        return _finalize_quote(q)
                    except Exception:
                        return _StubUnifiedQuote(symbol=sym, error=str(exc)).finalize()

                async def get_quotes(self, ss):
                    clean = [str(x).strip() for x in (ss or []) if x and str(x).strip()]
                    out: List[Any] = []
                    for x in clean:
                        try:
                            q = UQ(symbol=x, data_quality="MISSING", error=str(exc))
                            out.append(_finalize_quote(q))
                        except Exception:
                            out.append(_StubUnifiedQuote(symbol=x, error=str(exc)).finalize())
                    return out

                async def get_enriched_quote(self, s):
                    return await self.get_quote(s)

                async def get_enriched_quotes(self, ss):
                    return await self.get_quotes(ss)

                async def aclose(self):
                    return None

            _engine_instance = _CrashStub()
            return _engine_instance


async def get_engine() -> Any:
    """
    Async engine accessor that prefers v2.get_engine() if available.
    Safe: falls back to this module singleton if v2 getter unavailable.
    """
    ok, _ = _load_v2()
    if ok:
        fn = _V2.get("get_engine")
        if callable(fn):
            try:
                eng = await _maybe_await(fn())
                global _engine_instance
                _engine_instance = eng
                return eng
            except Exception:
                pass
    return _get_engine_sync()


async def close_engine() -> None:
    global _engine_instance
    eng = _engine_instance
    _engine_instance = None
    try:
        if eng is not None and hasattr(eng, "aclose"):
            await _maybe_await(eng.aclose())
    except Exception:
        pass


# =============================================================================
# Symbol normalization
# =============================================================================
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
    """
    Cleans + dedups while trying to normalize to stable keys.
    """
    seen = set()
    out: List[str] = []
    for s in (symbols or []):
        if s is None:
            continue
        raw = str(s).strip()
        if not raw:
            continue
        key = normalize_symbol(raw) or raw.strip().upper()
        if key in seen:
            continue
        seen.add(key)
        out.append(raw)
    return out


# =============================================================================
# Public API (legacy)
# =============================================================================
async def get_enriched_quote(symbol: str) -> Any:
    sym_in = str(symbol or "").strip()
    UQ = _get_uq_cls()

    if not sym_in:
        try:
            q = UQ(symbol="", data_quality="MISSING", error="Empty symbol")
            return _finalize_quote(q)
        except Exception:
            return _StubUnifiedQuote(symbol="", error="Empty symbol").finalize()

    sym = normalize_symbol(sym_in) or sym_in

    eng = await get_engine()
    try:
        if hasattr(eng, "get_enriched_quote"):
            res = await _maybe_await(eng.get_enriched_quote(sym))
            return _finalize_quote(_unwrap_payload(res))
        if hasattr(eng, "get_quote"):
            res = await _maybe_await(eng.get_quote(sym))
            return _finalize_quote(_unwrap_payload(res))

        try:
            q = UQ(symbol=sym, data_quality="MISSING", error="Engine method mismatch: no get_enriched_quote/get_quote")
            return _finalize_quote(q)
        except Exception:
            return _StubUnifiedQuote(symbol=sym, error="Engine method mismatch").finalize()

    except Exception as exc:
        logger.error("Legacy Adapter Error (Single): %s", exc)
        try:
            q = UQ(symbol=sym, data_quality="MISSING", error=str(exc))
            return _finalize_quote(q)
        except Exception:
            return _StubUnifiedQuote(symbol=sym, error=str(exc)).finalize()


async def get_enriched_quotes(symbols: List[str]) -> List[Any]:
    clean = _clean_symbols(symbols)
    if not clean:
        return []

    eng = await get_engine()
    UQ = _get_uq_cls()

    try:
        normed = [normalize_symbol(x) or x for x in clean]

        if hasattr(eng, "get_enriched_quotes"):
            res = await _maybe_await(eng.get_enriched_quotes(normed))
            return [_finalize_quote(_unwrap_payload(x)) for x in _as_list(res)]
        if hasattr(eng, "get_quotes"):
            res = await _maybe_await(eng.get_quotes(normed))
            return [_finalize_quote(_unwrap_payload(x)) for x in _as_list(res)]

        # fallback: per-symbol
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
                q = UQ(symbol=sym, data_quality="MISSING", error=str(exc))
                out.append(_finalize_quote(q))
            except Exception:
                out.append(_StubUnifiedQuote(symbol=sym, error=str(exc)).finalize())
        return out


async def get_quote(symbol: str) -> Any:
    return await get_enriched_quote(symbol)


async def get_quotes(symbols: List[str]) -> List[Any]:
    return await get_enriched_quotes(symbols)


# =============================================================================
# Lazy DataEngine wrapper (TRUE LAZY for importers)
# =============================================================================
class DataEngine:
    """
    Lightweight wrapper to preserve "from core.data_engine import DataEngine"
    without forcing V2 import at import-time.

    It delegates to the shared engine instance returned by get_engine().
    """

    def __init__(self, *args, **kwargs) -> None:
        self._engine: Optional[Any] = None

    async def _ensure(self) -> Any:
        if self._engine is None:
            self._engine = await get_engine()
        return self._engine

    async def get_quote(self, symbol: str) -> Any:
        eng = await self._ensure()
        if hasattr(eng, "get_quote"):
            return _finalize_quote(_unwrap_payload(await _maybe_await(eng.get_quote(symbol))))
        return await get_quote(symbol)

    async def get_quotes(self, symbols: List[str]) -> List[Any]:
        eng = await self._ensure()
        if hasattr(eng, "get_quotes"):
            res = await _maybe_await(eng.get_quotes(symbols))
            return [_finalize_quote(_unwrap_payload(x)) for x in _as_list(res)]
        return await get_quotes(symbols)

    async def get_enriched_quote(self, symbol: str) -> Any:
        eng = await self._ensure()
        if hasattr(eng, "get_enriched_quote"):
            return _finalize_quote(_unwrap_payload(await _maybe_await(eng.get_enriched_quote(symbol))))
        return await get_enriched_quote(symbol)

    async def get_enriched_quotes(self, symbols: List[str]) -> List[Any]:
        eng = await self._ensure()
        if hasattr(eng, "get_enriched_quotes"):
            res = await _maybe_await(eng.get_enriched_quotes(symbols))
            return [_finalize_quote(_unwrap_payload(x)) for x in _as_list(res)]
        return await get_enriched_quotes(symbols)

    async def aclose(self) -> None:
        # closes shared engine instance
        await close_engine()


# =============================================================================
# Diagnostics
# =============================================================================
def _safe_read_settings() -> Optional[object]:
    try:
        from config import get_settings as _gs  # type: ignore

        return _gs()
    except Exception:
        pass
    try:
        from core.config import get_settings as _gs  # type: ignore

        return _gs()
    except Exception:
        return None


def _safe_parse_env_list(key: str) -> List[str]:
    raw = (os.getenv(key) or "").strip()
    if not raw:
        return []
    return [p.strip().lower() for p in raw.split(",") if p.strip()]


def get_engine_meta() -> Dict[str, Any]:
    ok, err = _load_v2()

    providers: List[str] = []
    ksa_providers: List[str] = []

    s = _safe_read_settings()
    try:
        if s is not None:
            p = getattr(s, "enabled_providers", None) or getattr(s, "providers", None)
            k = getattr(s, "ksa_providers", None) or getattr(s, "providers_ksa", None)
            if isinstance(p, list):
                providers = [str(x).strip().lower() for x in p if str(x).strip()]
            if isinstance(k, list):
                ksa_providers = [str(x).strip().lower() for x in k if str(x).strip()]
    except Exception:
        pass

    if not providers:
        providers = _safe_parse_env_list("ENABLED_PROVIDERS") or _safe_parse_env_list("PROVIDERS")
    if not ksa_providers:
        ksa_providers = _safe_parse_env_list("KSA_PROVIDERS")

    v2_mod = _V2.get("module")
    v2_version = None
    try:
        if v2_mod is not None:
            v2_version = getattr(v2_mod, "ENGINE_VERSION", None) or getattr(v2_mod, "VERSION", None)
    except Exception:
        v2_version = None

    return {
        "mode": _ENGINE_MODE,
        "is_stub": _ENGINE_MODE == "stub",
        "adapter_version": ADAPTER_VERSION,
        "v2_loaded": bool(ok),
        "v2_error": err or _V2_LOAD_ERR,
        "v2_version": v2_version,
        "providers": providers,
        "ksa_providers": ksa_providers,
    }


def __getattr__(name: str) -> Any:  # pragma: no cover
    """
    Provide lazy access to UnifiedQuote only.
    DataEngine is defined above (true-lazy for importers).
    """
    if name == "UnifiedQuote":
        return _get_uq_cls()
    if name == "ENGINE_MODE":
        return _ENGINE_MODE
    raise AttributeError(name)


__all__ = [
    "QuoteSource",
    "normalize_symbol",
    "UnifiedQuote",
    "DataEngine",
    "get_engine",
    "get_quote",
    "get_quotes",
    "get_enriched_quote",
    "get_enriched_quotes",
    "close_engine",
    "get_engine_meta",
]
