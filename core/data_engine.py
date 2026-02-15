#!/usr/bin/env python3
"""
core/data_engine.py
===============================================================
LEGACY COMPATIBILITY ADAPTER (v5.3.0) — PROD SAFE (TRUE LAZY)

FULL REPLACEMENT (v5.3.0) — What’s improved vs v4.1.0
- ✅ Even safer startup: NO hard dependency on data_engine_v2 at import-time (kept)
- ✅ Stronger V2 discovery:
    - supports v2 engines exposing DataEngine / DataEngineV2 / Engine
    - supports v2 functions: get_engine, get_quote(s), get_enriched_quote(s), fetch_quote(s), fetch_enriched_quote_patch
- ✅ Robust auto-await everywhere + unified call pipeline (single + batch)
- ✅ Order preservation upgraded:
    - aligns outputs to input order even if V2 returns dict, list, tuple envelopes, or mixed payloads
    - stable mapping by normalized symbol + “requested_symbol” + “symbol” keys
- ✅ Better stub parity:
    - stub quote includes wide schema coverage + stable forecast keys (1m/3m/12m)
    - ensures last_updated_utc always present
- ✅ Safer symbol normalization:
    - prefers core.symbols.normalize.normalize_symbol if available (NO ".US" auto-suffix)
    - falls back to conservative rules
- ✅ Optional “strict mode” to surface errors for internal debugging (DATA_ENGINE_STRICT=true)
- ✅ Cleaner meta reporting (never leaks secrets)
- ✅ Thread-safe singleton + close_engine() is idempotent

Environment toggles
- DATA_ENGINE_V2_DISABLED=true/false (default false) -> forces stub mode
- DATA_ENGINE_STRICT=true/false     (default false) -> raise internal errors (use only for dev)

Public legacy API surface (stable)
- normalize_symbol()
- get_quote / get_quotes
- get_enriched_quote / get_enriched_quotes
- close_engine()
- get_engine_meta()
- DataEngine wrapper class (lazy, safe)
"""

from __future__ import annotations

import inspect
import logging
import os
import threading
from datetime import datetime, timezone
from importlib import import_module
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple, Type, Union

logger = logging.getLogger("core.data_engine")

ADAPTER_VERSION = "5.3.0"

# ---------------------------------------------------------------------------
# Pydantic (best-effort) with robust fallback
# ---------------------------------------------------------------------------
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
        def __init__(self, **kwargs: Any):
            self.__dict__.update(kwargs)

        def model_dump(self, *a: Any, **k: Any) -> Dict[str, Any]:
            return dict(self.__dict__)

        def dict(self, *a: Any, **k: Any) -> Dict[str, Any]:  # legacy
            return dict(self.__dict__)

    def Field(default: Any = None, **kwargs: Any) -> Any:  # type: ignore
        return default

    ConfigDict = None  # type: ignore


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
def _truthy_env(name: str, default: bool = False) -> bool:
    raw = str(os.getenv(name, "")).strip().lower()
    if not raw:
        return default
    return raw in {"1", "true", "yes", "y", "on"}


def _strict_mode() -> bool:
    return _truthy_env("DATA_ENGINE_STRICT", False)


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
    Some engines/providers might return:
      - payload
      - (payload, err)
      - (payload, err, meta...)
    We always keep payload if present.
    """
    try:
        if isinstance(x, tuple) and len(x) >= 1:
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
        return list(x.values())
    return [x]


def _safe_str(x: Any) -> str:
    try:
        return str(x).strip()
    except Exception:
        return ""


def _safe_upper(x: Any) -> str:
    return _safe_str(x).upper()


def _coerce_symbol_from_payload(p: Any) -> Optional[str]:
    """
    Best-effort extract a symbol key from payload objects/dicts:
    - symbol
    - requested_symbol
    """
    try:
        if p is None:
            return None
        if isinstance(p, dict):
            s = p.get("symbol") or p.get("requested_symbol") or p.get("ticker")
            return _safe_upper(s) if s else None
        # pydantic / object
        for attr in ("symbol", "requested_symbol", "ticker"):
            if hasattr(p, attr):
                s = getattr(p, attr)
                s2 = _safe_upper(s)
                if s2:
                    return s2
    except Exception:
        return None
    return None


# =============================================================================
# V2 linkage (TRUE LAZY)
# =============================================================================
_ENGINE_MODE: str = "unknown"  # unknown | v2 | stub
_V2: Dict[str, Any] = {}
_V2_LOAD_ERR: Optional[str] = None
_V2_LOAD_LOCK = threading.Lock()


def _find_unified_quote_fallback() -> Optional[Any]:
    for path in ("core.schemas", "core.models.schemas", "schemas"):
        try:
            m = import_module(path)
            uq = getattr(m, "UnifiedQuote", None)
            if uq is not None:
                return uq
        except Exception:
            continue
    return None


def _load_v2() -> Tuple[bool, Optional[str]]:
    """
    Best-effort import of core.data_engine_v2.
    Never raises out of here.
    """
    global _ENGINE_MODE, _V2, _V2_LOAD_ERR

    if _V2:
        return True, None

    if _truthy_env("DATA_ENGINE_V2_DISABLED", False):
        _ENGINE_MODE = "stub"
        _V2_LOAD_ERR = "V2 disabled by env: DATA_ENGINE_V2_DISABLED=true"
        return False, _V2_LOAD_ERR

    if _ENGINE_MODE == "stub" and _V2_LOAD_ERR:
        return False, _V2_LOAD_ERR

    with _V2_LOAD_LOCK:
        if _V2:
            return True, None

        try:
            mod = import_module("core.data_engine_v2")

            # Engine classes might be named differently over time
            v2_engine = getattr(mod, "DataEngine", None) or getattr(mod, "Engine", None)
            v2_engine_v2 = getattr(mod, "DataEngineV2", None)
            v2_uq = getattr(mod, "UnifiedQuote", None)

            v2_norm = getattr(mod, "normalize_symbol", None)
            v2_get_engine = getattr(mod, "get_engine", None)

            # some repos expose module-level funcs
            v2_get_quote = getattr(mod, "get_quote", None)
            v2_get_quotes = getattr(mod, "get_quotes", None)
            v2_get_enriched_quote = getattr(mod, "get_enriched_quote", None)
            v2_get_enriched_quotes = getattr(mod, "get_enriched_quotes", None)

            # UnifiedQuote may live elsewhere depending on repo evolution
            if v2_uq is None:
                v2_uq = _find_unified_quote_fallback()

            # If neither engine class nor module-level quote funcs exist, treat as missing
            has_any_entry = any(
                callable(x)
                for x in (v2_get_engine, v2_get_quote, v2_get_quotes, v2_get_enriched_quote, v2_get_enriched_quotes)
            ) or (v2_engine is not None) or (v2_engine_v2 is not None)
            if not has_any_entry:
                raise ImportError("core.data_engine_v2 missing usable engine exports")

            if v2_uq is None:
                raise ImportError("UnifiedQuote not found (v2 or schemas)")

            _V2["module"] = mod
            _V2["DataEngine"] = v2_engine or v2_engine_v2
            _V2["DataEngineV2"] = v2_engine_v2
            _V2["UnifiedQuote"] = v2_uq

            if callable(v2_norm):
                _V2["normalize_symbol"] = v2_norm
            if callable(v2_get_engine):
                _V2["get_engine"] = v2_get_engine

            # module-level funcs (optional)
            for k, fn in (
                ("get_quote", v2_get_quote),
                ("get_quotes", v2_get_quotes),
                ("get_enriched_quote", v2_get_enriched_quote),
                ("get_enriched_quotes", v2_get_enriched_quotes),
            ):
                if callable(fn):
                    _V2[k] = fn

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


def _get_v2_engine_cls() -> Optional[Type[Any]]:
    ok, _ = _load_v2()
    if ok and _V2.get("DataEngine"):
        return _V2["DataEngine"]
    return None


def _get_v2_engine_v2_cls() -> Optional[Type[Any]]:
    ok, _ = _load_v2()
    if ok and _V2.get("DataEngineV2"):
        return _V2["DataEngineV2"]
    return None


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

    # Scores
    quality_score: Optional[float] = None
    value_score: Optional[float] = None
    momentum_score: Optional[float] = None
    risk_score: Optional[float] = None
    opportunity_score: Optional[float] = None
    overall_score: Optional[float] = None

    # Forecasts (Aligned with Sheet Controller)
    forecast_price_1m: Optional[float] = None
    expected_roi_1m: Optional[float] = None
    forecast_price_3m: Optional[float] = None
    expected_roi_3m: Optional[float] = None
    forecast_price_12m: Optional[float] = None
    expected_roi_12m: Optional[float] = None
    forecast_confidence: Optional[float] = None
    forecast_method: Optional[str] = None
    forecast_updated_utc: Optional[str] = None
    forecast_updated_riyadh: Optional[str] = None

    data_source: str = "none"
    provider: Optional[str] = None
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
        if (
            self.percent_change is None
            and self.price_change is not None
            and self.previous_close not in (None, 0)
        ):
            try:
                self.percent_change = (self.price_change / self.previous_close) * 100.0
            except Exception:
                pass
        if self.forecast_updated_utc is None:
            self.forecast_updated_utc = self.last_updated_utc
        return self

    def dict(self, *args: Any, **kwargs: Any) -> Dict[str, Any]:
        if hasattr(self, "model_dump"):
            return self.model_dump(*args, **kwargs)  # type: ignore
        return dict(self.__dict__)


class _StubEngine:
    def __init__(self, *args: Any, **kwargs: Any) -> None:
        logger.error("Legacy Adapter: initialized STUB engine (V2 missing/unavailable).")

    async def get_quote(self, symbol: str) -> _StubUnifiedQuote:
        sym = _safe_str(symbol)
        return _StubUnifiedQuote(symbol=sym, error="Engine V2 Missing").finalize()

    async def get_quotes(self, symbols: List[str]) -> List[_StubUnifiedQuote]:
        out: List[_StubUnifiedQuote] = []
        for s in symbols or []:
            sym = _safe_str(s)
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
# Symbol normalization (prefer shared core.symbols.normalize)
# =============================================================================
_NORM_FN: Optional[Any] = None
_NORM_LOCK = threading.Lock()


def _resolve_norm_fn() -> Optional[Any]:
    global _NORM_FN
    if callable(_NORM_FN):
        return _NORM_FN
    with _NORM_LOCK:
        if callable(_NORM_FN):
            return _NORM_FN
        # Prefer the canonical normalizer if present
        try:
            m = import_module("core.symbols.normalize")
            fn = getattr(m, "normalize_symbol", None)
            if callable(fn):
                _NORM_FN = fn
                return _NORM_FN
        except Exception:
            pass
        # If v2 exposes normalize_symbol, use it
        ok, _ = _load_v2()
        if ok and callable(_V2.get("normalize_symbol")):
            _NORM_FN = _V2["normalize_symbol"]
            return _NORM_FN
        _NORM_FN = None
        return None


def normalize_symbol(symbol: str) -> str:
    """
    Normalization rules (legacy-safe, dashboard-friendly):
    - Prefer core.symbols.normalize.normalize_symbol when available.
    - Otherwise conservative fallback:
        - Tadawul numeric code -> "####.SR"
        - keep indices/FX (= or ^)
        - keep already suffixed symbols (contains ".")
        - never auto-append ".US"
    """
    raw = _safe_str(symbol)
    if not raw:
        return ""
    fn = _resolve_norm_fn()
    if callable(fn):
        try:
            out = fn(raw)
            return _safe_upper(out) if out else ""
        except Exception:
            pass

    su = _safe_upper(raw)

    # common prefixes used by some sources
    if su.startswith("TADAWUL:"):
        su = su.split(":", 1)[1].strip()
    if su.endswith(".TADAWUL"):
        su = su.replace(".TADAWUL", "").strip()

    # Yahoo/FX/commodities/index markers: keep
    if any(ch in su for ch in ("=", "^")):
        return su

    # already has suffix -> keep
    if "." in su:
        return su

    # Tadawul numeric => .SR
    if su.isdigit():
        return f"{su}.SR"

    return su


def _clean_symbols(symbols: Sequence[Any]) -> List[str]:
    """
    Cleans + dedups while preserving input order.
    Dedup key uses normalized symbol.
    """
    seen = set()
    out: List[str] = []
    for s in symbols or []:
        raw = _safe_str(s)
        if not raw:
            continue
        key = normalize_symbol(raw) or _safe_upper(raw)
        if key in seen:
            continue
        seen.add(key)
        out.append(raw)
    return out


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

    # 2) settings kwarg (prefer repo-root config)
    try:
        from config import get_settings as _gs  # type: ignore

        return EngineCls(settings=_gs())
    except Exception:
        pass

    # 3) core.config shim settings kwarg
    try:
        from core.config import get_settings as _gs  # type: ignore

        return EngineCls(settings=_gs())
    except Exception:
        pass

    # 4) settings positional
    try:
        from config import get_settings as _gs  # type: ignore

        return EngineCls(_gs())
    except Exception:
        pass

    # 5) last resort
    return EngineCls()


def _get_engine_sync() -> Any:
    """Internal sync getter for engine singleton."""
    global _engine_instance

    if _engine_instance is not None:
        return _engine_instance

    with _engine_init_lock:
        if _engine_instance is not None:
            return _engine_instance

        ok, _ = _load_v2()

        # Prefer v2.get_engine() (even from sync context we just store callable; async wrapper will call it)
        EngineCls = _get_v2_engine_cls() if ok else None

        if EngineCls is None:
            _engine_instance = _StubEngine()
            return _engine_instance

        try:
            _engine_instance = _instantiate_engine(EngineCls)
            return _engine_instance
        except Exception as exc:
            logger.critical("Legacy Adapter: engine init failed: %s", exc)
            _engine_instance = _StubEngine()
            return _engine_instance


async def get_engine() -> Any:
    """
    Async engine accessor that prefers v2.get_engine() if available.
    Safe: falls back to this module singleton if v2 getter unavailable.
    """
    ok, _ = _load_v2()
    if ok and callable(_V2.get("get_engine")):
        try:
            eng = await _maybe_await(_V2["get_engine"]())
            global _engine_instance
            _engine_instance = eng
            return eng
        except Exception:
            # fall through to local singleton
            pass
    return _get_engine_sync()


async def close_engine() -> None:
    """Idempotent engine shutdown."""
    global _engine_instance
    eng = _engine_instance
    _engine_instance = None
    try:
        if eng is not None and hasattr(eng, "aclose"):
            await _maybe_await(eng.aclose())
    except Exception:
        pass


# =============================================================================
# Engine call helpers (tolerant to naming + module-level funcs)
# =============================================================================
def _candidate_method_names(enriched: bool, batch: bool) -> List[str]:
    if enriched and batch:
        return ["get_enriched_quotes", "get_enriched_quote_batch", "fetch_enriched_quotes", "fetch_enriched_quote_batch"]
    if enriched and not batch:
        return ["get_enriched_quote", "fetch_enriched_quote", "fetch_enriched_quote_patch", "enriched_quote"]
    if not enriched and batch:
        return ["get_quotes", "fetch_quotes", "quotes", "fetch_many"]
    return ["get_quote", "fetch_quote", "quote", "fetch"]


async def _call_engine_single(eng: Any, symbol: str, *, enriched: bool) -> Any:
    """
    Try V2 module-level funcs first if engine mismatch, then methods on engine.
    """
    sym = symbol

    # module-level function fast path (when v2 exported those)
    if enriched and callable(_V2.get("get_enriched_quote")):
        return await _maybe_await(_V2["get_enriched_quote"](sym))
    if (not enriched) and callable(_V2.get("get_quote")):
        return await _maybe_await(_V2["get_quote"](sym))

    for name in _candidate_method_names(enriched=enriched, batch=False):
        fn = getattr(eng, name, None)
        if callable(fn):
            return await _maybe_await(fn(sym))
    raise AttributeError("Engine method mismatch: no single-quote method found")


async def _call_engine_batch(eng: Any, symbols: List[str], *, enriched: bool) -> Any:
    # module-level batch function fast path
    if enriched and callable(_V2.get("get_enriched_quotes")):
        return await _maybe_await(_V2["get_enriched_quotes"](symbols))
    if (not enriched) and callable(_V2.get("get_quotes")):
        return await _maybe_await(_V2["get_quotes"](symbols))

    for name in _candidate_method_names(enriched=enriched, batch=True):
        fn = getattr(eng, name, None)
        if callable(fn):
            return await _maybe_await(fn(symbols))

    # fallback: per-symbol
    out: List[Any] = []
    for s in symbols:
        out.append(await _call_engine_single(eng, s, enriched=enriched))
    return out


def _normalize_key(s: str) -> str:
    return normalize_symbol(s) or _safe_upper(s)


def _coerce_batch_in_order(res: Any, requested_symbols: List[str]) -> List[Any]:
    """
    Ensure output is a list aligned with requested_symbols order.

    Handles:
    - dict[symbol->payload]
    - list[payload]
    - tuple envelopes
    """
    payload = _unwrap_payload(res)

    # Dict response -> map by normalized key
    if isinstance(payload, dict):
        lookup: Dict[str, Any] = {}
        for k, v in payload.items():
            nk = _normalize_key(_safe_str(k))
            if nk:
                lookup[nk] = v

            # also index by payload's own symbol if present
            ps = _coerce_symbol_from_payload(v)
            if ps:
                lookup[_normalize_key(ps)] = v

        out: List[Any] = []
        for s in requested_symbols:
            out.append(lookup.get(_normalize_key(s)))
        return out

    # List response -> if items have symbol field, map them; else return as list
    arr = _as_list(payload)
    if not arr:
        return []

    # Try building a symbol lookup if possible (safer than trusting list order)
    lookup2: Dict[str, Any] = {}
    for item in arr:
        ps = _coerce_symbol_from_payload(item)
        if ps:
            lookup2[_normalize_key(ps)] = item

    if lookup2:
        out2: List[Any] = []
        for s in requested_symbols:
            out2.append(lookup2.get(_normalize_key(s)))
        return out2

    # Otherwise assume same order (best-effort)
    return arr


# =============================================================================
# Public API (legacy)
# =============================================================================
async def get_enriched_quote(symbol: str) -> Any:
    UQ = _get_uq_cls()
    sym_in = _safe_str(symbol)
    if not sym_in:
        try:
            return _finalize_quote(UQ(symbol="", data_quality="MISSING", error="Empty symbol"))
        except Exception:
            return _StubUnifiedQuote(symbol="", error="Empty symbol").finalize()

    sym = normalize_symbol(sym_in) or sym_in

    try:
        eng = await get_engine()
        res = await _call_engine_single(eng, sym, enriched=True)
        return _finalize_quote(_unwrap_payload(res))
    except Exception as exc:
        logger.error("Legacy Adapter Error (Single enriched): %s", exc)
        if _strict_mode():
            raise
        try:
            return _finalize_quote(UQ(symbol=sym, data_quality="MISSING", error=str(exc)))
        except Exception:
            return _StubUnifiedQuote(symbol=sym, error=str(exc)).finalize()


async def get_enriched_quotes(symbols: List[str]) -> List[Any]:
    UQ = _get_uq_cls()
    clean = _clean_symbols(symbols)
    if not clean:
        return []

    # Normalize (requested for routing keys)
    normed = [normalize_symbol(x) or x for x in clean]

    try:
        eng = await get_engine()
        res = await _call_engine_batch(eng, normed, enriched=True)
        aligned = _coerce_batch_in_order(res, normed)

        out: List[Any] = []
        for i, sym in enumerate(normed):
            item = aligned[i] if i < len(aligned) else None
            item = _unwrap_payload(item)

            if item is None:
                try:
                    out.append(_finalize_quote(UQ(symbol=sym, data_quality="MISSING", error="Missing quote in batch result")))
                except Exception:
                    out.append(_StubUnifiedQuote(symbol=sym, error="Missing quote in batch result").finalize())
            else:
                out.append(_finalize_quote(item))
        return out

    except Exception as exc:
        logger.error("Legacy Adapter Error (Batch enriched): %s", exc)
        if _strict_mode():
            raise
        out2: List[Any] = []
        for sym in normed:
            try:
                out2.append(_finalize_quote(UQ(symbol=sym, data_quality="MISSING", error=str(exc))))
            except Exception:
                out2.append(_StubUnifiedQuote(symbol=sym, error=str(exc)).finalize())
        return out2


async def get_quote(symbol: str) -> Any:
    # legacy: quote == enriched quote
    return await get_enriched_quote(symbol)


async def get_quotes(symbols: List[str]) -> List[Any]:
    # legacy: quotes == enriched quotes
    return await get_enriched_quotes(symbols)


# =============================================================================
# Lazy DataEngine wrapper (TRUE LAZY for importers)
# =============================================================================
class DataEngine:
    """
    Lightweight wrapper to preserve "from core.data_engine import DataEngine"
    without forcing V2 import at import-time.
    Delegates to the shared engine instance returned by get_engine().
    """

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        self._engine: Optional[Any] = None

    async def _ensure(self) -> Any:
        if self._engine is None:
            self._engine = await get_engine()
        return self._engine

    async def get_quote(self, symbol: str) -> Any:
        eng = await self._ensure()
        try:
            res = await _call_engine_single(eng, symbol, enriched=False)
            return _finalize_quote(_unwrap_payload(res))
        except Exception:
            return await get_quote(symbol)

    async def get_quotes(self, symbols: List[str]) -> List[Any]:
        eng = await self._ensure()
        try:
            res = await _call_engine_batch(eng, symbols, enriched=False)
            arr = _coerce_batch_in_order(res, symbols)
            out: List[Any] = []
            for x, s in zip(arr, symbols):
                if x is None:
                    out.append(_StubUnifiedQuote(symbol=_safe_str(s), error="Missing quote in batch result").finalize())
                else:
                    out.append(_finalize_quote(_unwrap_payload(x)))
            return out
        except Exception:
            return await get_quotes(symbols)

    async def get_enriched_quote(self, symbol: str) -> Any:
        eng = await self._ensure()
        try:
            res = await _call_engine_single(eng, symbol, enriched=True)
            return _finalize_quote(_unwrap_payload(res))
        except Exception:
            return await get_enriched_quote(symbol)

    async def get_enriched_quotes(self, symbols: List[str]) -> List[Any]:
        eng = await self._ensure()
        try:
            res = await _call_engine_batch(eng, symbols, enriched=True)
            arr = _coerce_batch_in_order(res, symbols)
            out: List[Any] = []
            for x, s in zip(arr, symbols):
                if x is None:
                    out.append(_StubUnifiedQuote(symbol=_safe_str(s), error="Missing quote in batch result").finalize())
                else:
                    out.append(_finalize_quote(_unwrap_payload(x)))
            return out
        except Exception:
            return await get_enriched_quotes(symbols)

    async def aclose(self) -> None:
        await close_engine()


# =============================================================================
# Diagnostics (never leak secrets)
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
        "strict": _strict_mode(),
        "v2_disabled": _truthy_env("DATA_ENGINE_V2_DISABLED", False),
        "v2_loaded": bool(ok),
        "v2_error": err or _V2_LOAD_ERR,
        "v2_version": v2_version,
        "providers": providers,
        "ksa_providers": ksa_providers,
    }


def __getattr__(name: str) -> Any:  # pragma: no cover
    """
    Provide lazy access to V2 types without importing V2 at module import-time.
    """
    if name == "UnifiedQuote":
        return _get_uq_cls()
    if name == "DataEngineV2":
        return _get_v2_engine_v2_cls()
    if name == "ENGINE_MODE":
        return _ENGINE_MODE
    raise AttributeError(name)


__all__ = [
    "QuoteSource",
    "normalize_symbol",
    "UnifiedQuote",
    "DataEngine",
    "DataEngineV2",
    "get_engine",
    "get_quote",
    "get_quotes",
    "get_enriched_quote",
    "get_enriched_quotes",
    "close_engine",
    "get_engine_meta",
]
