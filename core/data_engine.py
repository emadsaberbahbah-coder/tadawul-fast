# core/data_engine.py  (FULL REPLACEMENT)
"""
core/data_engine.py
===============================================================
LEGACY COMPATIBILITY ADAPTER (v3.7.0) — PROD SAFE (TRUE LAZY)

PURPOSE
- Keeps older imports working:
    from core.data_engine import DataEngine, UnifiedQuote, get_enriched_quote(s)
- Delegates all real work to: core.data_engine_v2.DataEngine (when available)
- Provides a safe STUB mode if V2 cannot be imported (never crashes the app)

TRUE LAZY RULE (important)
- Importing this module MUST NOT import core.data_engine_v2.
- V2 import only happens when you actually call:
    get_enriched_quote(s) / get_quote(s) / DataEngine()

MAPPING (best-effort)
- get_enriched_quote(symbol)    -> v2_engine.get_enriched_quote(symbol) OR v2_engine.get_quote(symbol)
- get_enriched_quotes(symbols)  -> v2_engine.get_enriched_quotes(symbols) OR v2_engine.get_quotes(symbols)
- get_quote(s) / get_quotes(ss) -> aliases for enriched variants

DESIGN
- Stable module-level singleton engine (shared across calls).
- Safe normalization fallback if V2 normalizer is not available.
- “UnifiedQuote” exported as a STUB model to prevent FastAPI schema/import crashes.
  Real V2 quote objects are still returned from get_enriched_quote(s) when V2 works.
"""

from __future__ import annotations

import inspect
import logging
import threading
from datetime import datetime, timezone
from importlib import import_module
from typing import Any, Dict, List, Optional, Sequence, Tuple, Type

# ---------------------------------------------------------------------------
# Pydantic (best-effort, PROD SAFE)
# ---------------------------------------------------------------------------
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

ADAPTER_VERSION = "3.7.0"

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
# 2) Async helper (defensive)
# ---------------------------------------------------------------------------
async def _maybe_await(v: Any) -> Any:
    if inspect.isawaitable(v):
        return await v
    return v


def _utc_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _safe_finalize(obj: Any) -> Any:
    """
    Some quote models in your codebase may have finalize().
    Never assume it exists.
    """
    try:
        fn = getattr(obj, "finalize", None)
        if callable(fn):
            return fn()
    except Exception:
        pass
    return obj


def _safe_model_dump(obj: Any) -> Dict[str, Any]:
    try:
        if hasattr(obj, "model_dump"):
            return obj.model_dump()  # type: ignore
    except Exception:
        pass
    try:
        if isinstance(obj, dict):
            return dict(obj)
    except Exception:
        pass
    try:
        return dict(getattr(obj, "__dict__", {}) or {})
    except Exception:
        return {}


# ---------------------------------------------------------------------------
# 3) V2 Lazy Loader (never raises to caller)
# ---------------------------------------------------------------------------
_ENGINE_MODE: str = "unknown"  # unknown | v2 | stub
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
            mod = import_module("core.data_engine_v2")

            v2_engine = getattr(mod, "DataEngine", None)
            v2_norm = getattr(mod, "normalize_symbol", None)

            # UnifiedQuote can live in v2 or schemas
            v2_uq = getattr(mod, "UnifiedQuote", None)
            if v2_uq is None:
                try:
                    schemas = import_module("core.schemas")
                    v2_uq = getattr(schemas, "UnifiedQuote", None)
                except Exception:
                    v2_uq = None

            if v2_engine is None:
                raise ImportError("core.data_engine_v2 missing DataEngine")
            if v2_uq is None:
                # Not fatal for runtime if engine returns dicts, but we record it.
                logger.warning("Legacy Adapter: V2 UnifiedQuote not found; will use STUB model for coercion.")

            _V2["DataEngine"] = v2_engine
            if v2_uq is not None:
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


def _get_v2_engine_cls() -> Optional[Type[Any]]:
    ok, _ = _load_v2()
    if ok:
        cls = _V2.get("DataEngine")
        if cls is not None:
            return cls
    return None


def _get_v2_uq_cls() -> Optional[Type[Any]]:
    ok, _ = _load_v2()
    if ok:
        cls = _V2.get("UnifiedQuote")
        if cls is not None:
            return cls
    return None


# ---------------------------------------------------------------------------
# 4) STUB UnifiedQuote + STUB Engine (Safe Mode)
# ---------------------------------------------------------------------------
class UnifiedQuote(BaseModel):
    """
    STUB UnifiedQuote (exported symbol).

    Why stub?
    - Prevents app boot / FastAPI schema from failing if V2 is missing.
    - Runtime functions still return real V2 quote objects when V2 is available.

    Field names are aligned to your provider patches (high_52w/low_52w/free_float/etc).
    """
    model_config = ConfigDict(populate_by_name=True, extra="ignore")

    symbol: str = ""

    # identity
    name: Optional[str] = None
    market: str = "UNKNOWN"
    currency: Optional[str] = None
    exchange: Optional[str] = None

    # price / trading
    current_price: Optional[float] = None
    previous_close: Optional[float] = None
    open: Optional[float] = None
    day_high: Optional[float] = None
    day_low: Optional[float] = None
    high_52w: Optional[float] = None
    low_52w: Optional[float] = None
    position_52w_percent: Optional[float] = None
    volume: Optional[float] = None
    avg_volume_30d: Optional[float] = None
    value_traded: Optional[float] = None

    price_change: Optional[float] = None
    percent_change: Optional[float] = None

    # fundamentals (common)
    market_cap: Optional[float] = None
    shares_outstanding: Optional[float] = None
    free_float: Optional[float] = None
    free_float_market_cap: Optional[float] = None

    eps_ttm: Optional[float] = None
    pe_ttm: Optional[float] = None
    pb: Optional[float] = None
    ps: Optional[float] = None
    dividend_yield: Optional[float] = None

    # analytics / scoring (optional; if absent, leave None)
    quality_score: Optional[float] = None
    value_score: Optional[float] = None
    momentum_score: Optional[float] = None
    risk_score: Optional[float] = None
    opportunity_score: Optional[float] = None

    # provenance
    data_source: str = "none"
    data_quality: str = "MISSING"
    last_updated_utc: str = Field(default_factory=_utc_iso)
    error: str = ""

    # legacy aliases some older code may still pass
    price: Optional[float] = None
    change: Optional[float] = None
    week_52_high: Optional[float] = None
    week_52_low: Optional[float] = None
    free_float_pct: Optional[float] = None

    def finalize(self) -> "UnifiedQuote":
        # Normalize legacy aliases into canonical names (never overwrites good data)
        if self.current_price is None and self.price is not None:
            self.current_price = self.price

        if self.price_change is None and self.change is not None:
            self.price_change = self.change

        if self.high_52w is None and self.week_52_high is not None:
            self.high_52w = self.week_52_high

        if self.low_52w is None and self.week_52_low is not None:
            self.low_52w = self.week_52_low

        if self.free_float is None and self.free_float_pct is not None:
            self.free_float = self.free_float_pct

        if self.percent_change is None and self.price_change is not None and self.previous_close not in (None, 0):
            try:
                self.percent_change = (self.price_change / float(self.previous_close)) * 100.0
            except Exception:
                pass

        return self


_STUB_LOGGED = False


class _StubEngine:
    """
    Stub engine that matches the V2 async interface closely enough for routes.
    """
    def __init__(self, *args, **kwargs) -> None:
        global _STUB_LOGGED
        if not _STUB_LOGGED:
            _STUB_LOGGED = True
            logger.error("Legacy Adapter: initialized STUB engine (V2 missing/unavailable).")

    async def get_quote(self, symbol: str) -> UnifiedQuote:
        sym = str(symbol or "").strip()
        return UnifiedQuote(symbol=sym, error="Engine V2 Missing").finalize()

    async def get_quotes(self, symbols: List[str]) -> List[UnifiedQuote]:
        out: List[UnifiedQuote] = []
        for s in (symbols or []):
            sym = str(s or "").strip()
            if sym:
                out.append(UnifiedQuote(symbol=sym, error="Engine V2 Missing").finalize())
        return out

    async def get_enriched_quote(self, symbol: str) -> UnifiedQuote:
        return await self.get_quote(symbol)

    async def get_enriched_quotes(self, symbols: List[str]) -> List[UnifiedQuote]:
        return await self.get_quotes(symbols)

    async def aclose(self) -> None:
        return None


# ---------------------------------------------------------------------------
# 5) Singleton Engine Instance (shared across module calls)
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
    except Exception:
        raise

    # 2) try keyword settings=
    try:
        from core.config import get_settings as _gs  # type: ignore
        return EngineCls(settings=_gs())
    except TypeError:
        pass
    except Exception:
        pass

    # 3) try positional settings
    try:
        from core.config import get_settings as _gs  # type: ignore
        return EngineCls(_gs())
    except TypeError:
        pass
    except Exception:
        pass

    # 4) last resort
    return EngineCls()


def _get_engine() -> Any:
    """
    Singleton accessor for the underlying engine.
    Never raises; returns a stub engine on failure.
    """
    global _engine_instance

    if _engine_instance is not None:
        return _engine_instance

    with _engine_init_lock:
        if _engine_instance is not None:
            return _engine_instance

        v2_cls = _get_v2_engine_cls()
        EngineCls = v2_cls or _StubEngine  # type: ignore

        try:
            _engine_instance = _instantiate_engine(EngineCls) if EngineCls is not _StubEngine else _StubEngine()
            return _engine_instance
        except Exception as exc:
            logger.critical("Legacy Adapter: engine init failed: %s", exc)
            _engine_instance = _StubEngine()
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
            await _maybe_await(eng.aclose())
    except Exception:
        pass


# ---------------------------------------------------------------------------
# 6) Symbol normalization (exported)
# ---------------------------------------------------------------------------
def normalize_symbol(symbol: str) -> str:
    """
    Exported normalizer:
    - Prefer core.data_engine_v2.normalize_symbol if available (lazy at call-time)
    - Else safe fallback:
        * trims + uppercases
        * keeps Yahoo special (^, =)
        * numeric => 1120.SR
        * alpha => AAPL.US
        * if '.' exists => keep
    """
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


# ---------------------------------------------------------------------------
# 7) Coercion helper (handles dict/list/objects from V2)
# ---------------------------------------------------------------------------
def _coerce_to_uq(obj: Any, symbol_fallback: str = "", error_fallback: str = "") -> Any:
    """
    If V2 returns dicts, convert to V2 UnifiedQuote (if available),
    else to STUB UnifiedQuote.
    If obj already looks like a model, return it as-is.
    """
    if obj is None:
        return UnifiedQuote(symbol=symbol_fallback, error=error_fallback).finalize()

    # If already a model-like object
    if hasattr(obj, "model_dump") or hasattr(obj, "__dict__"):
        return _safe_finalize(obj)

    # Dict case
    if isinstance(obj, dict):
        v2_uq = _get_v2_uq_cls()
        if v2_uq is not None:
            try:
                inst = v2_uq(**obj)  # type: ignore
                return _safe_finalize(inst)
            except Exception:
                pass
        try:
            inst2 = UnifiedQuote(**obj)  # type: ignore
            return inst2.finalize()
        except Exception:
            return UnifiedQuote(symbol=symbol_fallback, error=error_fallback).finalize()

    return UnifiedQuote(symbol=symbol_fallback, error=error_fallback).finalize()


# ---------------------------------------------------------------------------
# 8) Public API Functions (Compatibility Layer)
# ---------------------------------------------------------------------------
async def get_enriched_quote(symbol: str) -> Any:
    """
    Legacy Wrapper: Fetches a single quote using the V2 engine.
    """
    sym_in = str(symbol or "").strip()
    if not sym_in:
        return UnifiedQuote(symbol="", error="Empty symbol").finalize()

    sym = normalize_symbol(sym_in) or sym_in
    eng = _get_engine()

    try:
        if hasattr(eng, "get_enriched_quote"):
            res = await _maybe_await(eng.get_enriched_quote(sym))
            return _coerce_to_uq(res, symbol_fallback=sym)
        if hasattr(eng, "get_quote"):
            res = await _maybe_await(eng.get_quote(sym))
            return _coerce_to_uq(res, symbol_fallback=sym)

        return UnifiedQuote(symbol=sym, error="Engine method mismatch: no get_enriched_quote/get_quote").finalize()
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

    sym_list = [normalize_symbol(x) or x for x in clean]
    eng = _get_engine()

    try:
        if hasattr(eng, "get_enriched_quotes"):
            res = await _maybe_await(eng.get_enriched_quotes(sym_list))
            if isinstance(res, list):
                return [_coerce_to_uq(x, symbol_fallback=str(sym_list[i]) if i < len(sym_list) else "") for i, x in enumerate(res)]
            return [_coerce_to_uq(res, symbol_fallback=sym_list[0])]
        if hasattr(eng, "get_quotes"):
            res = await _maybe_await(eng.get_quotes(sym_list))
            if isinstance(res, list):
                return [_coerce_to_uq(x, symbol_fallback=str(sym_list[i]) if i < len(sym_list) else "") for i, x in enumerate(res)]
            return [_coerce_to_uq(res, symbol_fallback=sym_list[0])]

        # fallback: sequential
        out: List[Any] = []
        for s in clean:
            out.append(await get_enriched_quote(s))
        return out

    except Exception as exc:
        logger.error("Legacy Adapter Error (Batch): %s", exc)
        return [UnifiedQuote(symbol=(normalize_symbol(s) or s), error=str(exc)).finalize() for s in clean]


# Backward-compatible aliases
async def get_quote(symbol: str) -> Any:
    return await get_enriched_quote(symbol)


async def get_quotes(symbols: List[str]) -> List[Any]:
    return await get_enriched_quotes(symbols)


# ---------------------------------------------------------------------------
# 9) Exported DataEngine “Factory” (safe for old imports)
# ---------------------------------------------------------------------------
class DataEngine:  # pylint: disable=too-few-public-methods
    """
    Legacy-exported DataEngine symbol.

    Calling DataEngine() will instantiate V2 engine if available, otherwise STUB engine.
    Importing DataEngine does NOT import V2 (true-lazy).
    """

    def __new__(cls, *args: Any, **kwargs: Any) -> Any:
        v2_cls = _get_v2_engine_cls()
        EngineCls = v2_cls or _StubEngine  # type: ignore

        # If caller explicitly passes args/kwargs, try to honor them.
        if EngineCls is _StubEngine:
            return _StubEngine()

        try:
            return EngineCls(*args, **kwargs)  # type: ignore
        except TypeError:
            # fall back to our tolerant instantiation
            return _instantiate_engine(EngineCls)  # type: ignore
        except Exception:
            # never crash importers
            return _StubEngine()


# ---------------------------------------------------------------------------
# 10) Meta Info
# ---------------------------------------------------------------------------
def get_engine_meta() -> Dict[str, Any]:
    ok, err = _load_v2()

    providers: List[str] = []
    ksa_providers: List[str] = []
    primary_provider: Optional[str] = None

    try:
        from core.config import get_settings as _gs  # type: ignore
        s = _gs()

        providers = list(getattr(s, "enabled_providers", []) or []) or list(getattr(s, "PROVIDERS", []) or [])
        ksa_providers = list(getattr(s, "enabled_ksa_providers", []) or []) or list(getattr(s, "KSA_PROVIDERS", []) or [])
        primary_provider = (getattr(s, "primary_provider", None) or getattr(s, "PRIMARY_PROVIDER", None) or None)
        if isinstance(primary_provider, str):
            primary_provider = primary_provider.strip().lower() or None
    except Exception:
        pass

    return {
        "adapter_version": ADAPTER_VERSION,
        "mode": _ENGINE_MODE,
        "is_stub": _ENGINE_MODE == "stub",
        "v2_loaded": bool(ok),
        "v2_error": err or _V2_LOAD_ERR,
        "primary_provider": primary_provider,
        "providers": providers,
        "ksa_providers": ksa_providers,
    }


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
