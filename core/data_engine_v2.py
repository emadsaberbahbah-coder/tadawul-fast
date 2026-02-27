#!/usr/bin/env python3
"""
core/data_engine_v2.py
================================================================================
Data Engine V2 — THE MASTER ORCHESTRATOR — v5.1.1 (RELIABILITY PATCH)
================================================================================

Fixes in v5.1.1 (your current production blockers):
- ✅ Provider imports fixed: supports BOTH `providers.*` (your repo) and legacy `core.providers.*`
- ✅ Symbol normalization fixed: supports BOTH `symbols.normalize` and legacy `core.symbols.normalize`
- ✅ Provider call compatibility: works with async OR sync provider functions
- ✅ Patch key normalization: maps `price` -> `current_price` (prevents MISSING data_quality)
- ✅ Removes undefined `_PYDANTIC_V2` crash risk (safe serialization for cache/ws)

Result:
- AAPL / 2222.SR will stop returning all-null payloads once providers load successfully.
"""

from __future__ import annotations

import asyncio
import hashlib
import logging
import math
import os
import pickle
import random
import time
import uuid
import zlib
from collections import defaultdict, deque
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from enum import Enum
from typing import Any, AsyncGenerator, Callable, Dict, List, Optional, Set, Tuple, Union
from importlib import import_module

# ---------------------------------------------------------------------------
# High-Performance JSON fallback
# ---------------------------------------------------------------------------
try:
    import orjson
    def json_loads(data: Union[str, bytes]) -> Any: return orjson.loads(data)
    def json_dumps(obj: Any) -> str: return orjson.dumps(obj, default=str).decode("utf-8")
except ImportError:
    import json
    def json_loads(data: Union[str, bytes]) -> Any: return json.loads(data)
    def json_dumps(obj: Any) -> str: return json.dumps(obj, default=str)

logger = logging.getLogger("core.data_engine_v2")

# ---------------------------------------------------------------------------
# Pydantic safe detection
# ---------------------------------------------------------------------------
def _model_to_dict(obj: Any) -> Dict[str, Any]:
    if obj is None:
        return {}
    if isinstance(obj, dict):
        return obj
    # pydantic v2
    if hasattr(obj, "model_dump") and callable(getattr(obj, "model_dump")):
        return obj.model_dump()
    # pydantic v1
    if hasattr(obj, "dict") and callable(getattr(obj, "dict")):
        return obj.dict()
    # dataclass-like
    if hasattr(obj, "__dict__"):
        return dict(obj.__dict__)
    return {"result": obj}

# ---------------------------------------------------------------------------
# UnifiedQuote import (keep fallback safe)
# ---------------------------------------------------------------------------
try:
    from core.schemas import UnifiedQuote  # type: ignore
    SCHEMAS_AVAILABLE = True
except Exception:
    SCHEMAS_AVAILABLE = False
    try:
        from pydantic import BaseModel
    except Exception:
        BaseModel = object  # type: ignore

    class UnifiedQuote(BaseModel):  # type: ignore
        symbol: str
        current_price: Optional[float] = None
        data_quality: str = "MISSING"
        latency_ms: Optional[float] = None
        data_sources: Optional[List[str]] = None
        provider_latency: Optional[Dict[str, float]] = None
        class Config:
            extra = "ignore"

# ---------------------------------------------------------------------------
# Local Enums used by DataEngineV4
# ---------------------------------------------------------------------------
class QuoteQuality(str, Enum):
    EXCELLENT = "EXCELLENT"
    GOOD = "GOOD"
    FAIR = "FAIR"
    POOR = "POOR"
    MISSING = "MISSING"

class MarketRegime(str, Enum):
    BULL = "bull"
    BEAR = "bear"
    VOLATILE = "volatile"
    SIDEWAYS = "sideways"
    UNKNOWN = "unknown"

class DataSource(str, Enum):
    CACHE = "cache"
    PRIMARY = "primary"
    FALLBACK = "fallback"
    ENRICHMENT = "enrichment"

# ---------------------------------------------------------------------------
# Symbol normalization (IMPORTANT FIX)
# Supports BOTH layouts:
# - symbols/normalize.py  (your repo list)
# - core/symbols/normalize.py (legacy)
# ---------------------------------------------------------------------------
SYMBOL_NORMALIZATION_AVAILABLE = False

def _fallback_normalize_symbol(s: str) -> str:
    return (s or "").strip().upper()

def _fallback_is_ksa(s: str) -> bool:
    u = (s or "").strip().upper()
    if u.endswith(".SR"):
        return True
    if u.isdigit() and 3 <= len(u) <= 6:
        return True
    return False

def _fallback_to_yahoo_symbol(s: str) -> str:
    u = _fallback_normalize_symbol(s)
    if _fallback_is_ksa(u):
        return u if u.endswith(".SR") else f"{u}.SR"
    return u

try:
    # Preferred (per your repo listing)
    from symbols.normalize import normalize_symbol as normalize_symbol  # type: ignore
    from symbols.normalize import is_ksa as is_ksa  # type: ignore
    try:
        from symbols.normalize import to_yahoo_symbol as to_yahoo_symbol  # type: ignore
    except Exception:
        to_yahoo_symbol = _fallback_to_yahoo_symbol  # type: ignore
    SYMBOL_NORMALIZATION_AVAILABLE = True
except Exception:
    try:
        # Legacy
        from core.symbols.normalize import normalize_symbol as normalize_symbol  # type: ignore
        from core.symbols.normalize import is_ksa as is_ksa  # type: ignore
        try:
            from core.symbols.normalize import to_yahoo_symbol as to_yahoo_symbol  # type: ignore
        except Exception:
            to_yahoo_symbol = _fallback_to_yahoo_symbol  # type: ignore
        SYMBOL_NORMALIZATION_AVAILABLE = True
    except Exception:
        normalize_symbol = _fallback_normalize_symbol  # type: ignore
        is_ksa = _fallback_is_ksa  # type: ignore
        to_yahoo_symbol = _fallback_to_yahoo_symbol  # type: ignore

def get_symbol_info(symbol: str) -> Dict[str, Any]:
    """
    Unified symbol info used for provider selection.
    """
    norm = normalize_symbol(symbol) if callable(normalize_symbol) else _fallback_normalize_symbol(symbol)
    ksa = bool(is_ksa(norm)) if callable(is_ksa) else _fallback_is_ksa(norm)
    return {
        "raw": symbol,
        "normalized": norm,
        "market": "KSA" if ksa else "GLOBAL",
        "is_ksa": ksa,
    }

# ---------------------------------------------------------------------------
# Env helpers
# ---------------------------------------------------------------------------
def _get_env_list(key: str, default: str) -> List[str]:
    raw = os.getenv(key, default)
    return [s.strip().lower() for s in raw.split(",") if s.strip()]

def _get_env_bool(key: str, default: bool = False) -> bool:
    raw = os.getenv(key, "").strip().lower()
    if not raw:
        return default
    return raw in {"1", "true", "yes", "y", "on", "enabled", "enable"}

def _get_env_int(key: str, default: int) -> int:
    try:
        return int(os.getenv(key, str(default)))
    except Exception:
        return default

def _get_env_float(key: str, default: float) -> float:
    try:
        return float(os.getenv(key, str(default)))
    except Exception:
        return default

# ---------------------------------------------------------------------------
# Time helpers
# ---------------------------------------------------------------------------
def _now_utc() -> datetime:
    return datetime.now(timezone.utc)

def _now_utc_iso() -> str:
    return _now_utc().isoformat()

def _now_riyadh() -> datetime:
    return datetime.now(timezone(timedelta(hours=3)))

def _now_riyadh_iso() -> str:
    return _now_riyadh().isoformat()

# ---------------------------------------------------------------------------
# Safe type helpers
# ---------------------------------------------------------------------------
def _safe_float(x: Any) -> Optional[float]:
    if x is None:
        return None
    try:
        f = float(x)
        if math.isnan(f) or math.isinf(f):
            return None
        return f
    except Exception:
        return None

def _clean_patch(patch: Dict[str, Any]) -> Dict[str, Any]:
    return {k: v for k, v in patch.items() if v is not None and v != ""}

# ---------------------------------------------------------------------------
# Patch key normalization (IMPORTANT FIX)
# Providers often return `price` not `current_price`.
# Without this, data_quality stays MISSING.
# ---------------------------------------------------------------------------
_PATCH_ALIASES: Dict[str, str] = {
    "price": "current_price",
    "last_price": "current_price",
    "close": "previous_close",
    "prev_close": "previous_close",
    "change": "price_change",
    "change_pct": "percent_change",
    "change_percent": "percent_change",
    "pct_change": "percent_change",
    "open": "day_open",
    "high": "day_high",
    "low": "day_low",
}

def _normalize_patch_keys(patch: Dict[str, Any]) -> Dict[str, Any]:
    if not patch:
        return {}
    out = dict(patch)
    for src, dst in _PATCH_ALIASES.items():
        if src in out and (dst not in out or out.get(dst) in (None, "", 0)):
            out[dst] = out.get(src)
    # also keep `price` field if consumer expects it
    if "current_price" in out and ("price" not in out or out.get("price") in (None, "", 0)):
        out["price"] = out.get("current_price")
    return out

# ============================================================================
# Provider configuration (IMPORTANT FIX)
# Your repo uses `providers.*`, older drafts used `core.providers.*`.
# We support both.
# ============================================================================

DEFAULT_PROVIDERS = "tadawul,argaam,yahoo_chart,yahoo_fundamentals,finnhub,eodhd"
DEFAULT_KSA_PROVIDERS = "tadawul,argaam,yahoo_chart"
DEFAULT_GLOBAL_PROVIDERS = "yahoo_chart,yahoo_fundamentals,finnhub,eodhd"

PROVIDER_PRIORITIES = {
    "tadawul": 10,
    "argaam": 20,
    "yahoo_chart": 30,
    "yahoo_fundamentals": 35,
    "finnhub": 50,
    "eodhd": 60,
}

# Candidate module paths for each provider (try in order)
PROVIDER_MODULE_CANDIDATES: Dict[str, List[str]] = {
    "tadawul": ["providers.tadawul_provider", "core.providers.tadawul_provider"],
    "argaam": ["providers.argaam_provider", "core.providers.argaam_provider"],
    "yahoo_chart": ["providers.yahoo_chart_provider", "core.providers.yahoo_chart_provider"],
    "yahoo_fundamentals": ["providers.yahoo_fundamentals_provider", "core.providers.yahoo_fundamentals_provider"],
    "finnhub": ["providers.finnhub_provider", "core.providers.finnhub_provider"],
    "eodhd": ["providers.eodhd_provider", "core.providers.eodhd_provider"],
}

# Preferred function names (we will also try fallbacks)
PROVIDER_FUNCTIONS: Dict[str, List[str]] = {
    "tadawul": ["fetch_enriched_quote_patch", "fetch_quote_patch", "fetch_patch"],
    "argaam": ["fetch_enriched_quote_patch", "fetch_quote_patch", "fetch_patch"],
    "yahoo_chart": ["fetch_enriched_quote_patch", "fetch_quote_patch", "fetch_patch"],
    "yahoo_fundamentals": ["fetch_fundamentals_patch", "fetch_enriched_quote_patch", "fetch_patch"],
    "finnhub": ["fetch_enriched_quote_patch", "fetch_quote_patch", "fetch_patch"],
    "eodhd": ["fetch_enriched_quote_patch", "fetch_quote_patch", "fetch_patch"],
}

def _import_provider(provider_name: str) -> Tuple[Optional[Any], Optional[str]]:
    candidates = PROVIDER_MODULE_CANDIDATES.get(provider_name, [])
    last_err = None
    for module_path in candidates:
        try:
            return import_module(module_path), None
        except Exception as e:
            last_err = f"{module_path}: {e!r}"
            continue
    return None, last_err or "no candidates"

def _pick_provider_callable(module: Any, provider_name: str) -> Optional[Callable]:
    for fn_name in PROVIDER_FUNCTIONS.get(provider_name, ["fetch_enriched_quote_patch"]):
        fn = getattr(module, fn_name, None)
        if callable(fn):
            return fn
    return None

async def _call_maybe_async(fn: Callable, *args, **kwargs) -> Any:
    out = fn(*args, **kwargs)
    if asyncio.iscoroutine(out) or asyncio.isfuture(out):
        return await out
    return out

# ============================================================================
# Provider Stats / Registry
# ============================================================================

@dataclass(slots=True)
class ProviderStats:
    name: str
    success_count: int = 0
    failure_count: int = 0
    total_latency_ms: float = 0.0
    consecutive_failures: int = 0
    circuit_open_until: Optional[datetime] = None
    last_error: Optional[str] = None
    last_import_error: Optional[str] = None

    @property
    def avg_latency_ms(self) -> float:
        return self.total_latency_ms / self.success_count if self.success_count > 0 else 0.0

    @property
    def success_rate(self) -> float:
        t = self.success_count + self.failure_count
        return self.success_count / t if t > 0 else 1.0

    @property
    def is_circuit_open(self) -> bool:
        return _now_utc() < self.circuit_open_until if self.circuit_open_until else False

    def record_success(self, latency_ms: float) -> None:
        self.success_count += 1
        self.total_latency_ms += latency_ms
        self.consecutive_failures = 0
        self.circuit_open_until = None
        self.last_error = None

    def record_failure(self, err: str) -> None:
        self.failure_count += 1
        self.consecutive_failures += 1
        self.last_error = err

        threshold = _get_env_int("PROVIDER_CIRCUIT_BREAKER_THRESHOLD", 5)
        cooldown = _get_env_int("PROVIDER_CIRCUIT_BREAKER_COOLDOWN", 60)
        if self.consecutive_failures >= threshold:
            self.circuit_open_until = _now_utc() + timedelta(seconds=cooldown)

class ProviderRegistry:
    def __init__(self) -> None:
        self._lock = asyncio.Lock()
        self._providers: Dict[str, Tuple[Optional[Any], ProviderStats]] = {}

    async def get_provider(self, name: str) -> Tuple[Optional[Any], ProviderStats]:
        async with self._lock:
            if name not in self._providers:
                module, import_err = _import_provider(name)
                stats = ProviderStats(name=name, last_import_error=import_err)
                self._providers[name] = (module, stats)
            return self._providers[name]

    async def record_success(self, name: str, latency_ms: float) -> None:
        async with self._lock:
            if name in self._providers:
                self._providers[name][1].record_success(latency_ms)

    async def record_failure(self, name: str, err: str) -> None:
        async with self._lock:
            if name in self._providers:
                self._providers[name][1].record_failure(err)

    async def get_stats(self) -> Dict[str, Any]:
        async with self._lock:
            out = {}
            for name, (_, s) in self._providers.items():
                out[name] = {
                    "success": s.success_count,
                    "failure": s.failure_count,
                    "avg_latency_ms": round(s.avg_latency_ms, 2),
                    "success_rate": round(s.success_rate, 3),
                    "circuit_open": s.is_circuit_open,
                    "last_error": s.last_error,
                    "last_import_error": s.last_import_error,
                }
            return out

# ============================================================================
# Cache (L1 memory + optional disk)
# ============================================================================
class MultiLevelCache:
    def __init__(self, name: str, l1_ttl: int = 60, l3_ttl: int = 3600, max_l1_size: int = 5000):
        self.name = name
        self.l1_ttl = l1_ttl
        self.l3_ttl = l3_ttl
        self.max_l1_size = max_l1_size
        self._l1: Dict[str, Tuple[Any, float]] = {}
        self._l1_access: Dict[str, float] = {}
        self._lock = asyncio.Lock()
        self._dir = os.path.join("/tmp", f"cache_{name}")
        os.makedirs(self._dir, exist_ok=True)

    def _key(self, **kwargs) -> str:
        h = hashlib.sha256(json_dumps(kwargs).encode()).hexdigest()[:16]
        return f"{self.name}:{h}"

    def _compress(self, data: Any) -> bytes:
        try:
            return zlib.compress(pickle.dumps(data), level=6)
        except Exception:
            return pickle.dumps(data)

    def _decompress(self, data: bytes) -> Any:
        try:
            return pickle.loads(zlib.decompress(data))
        except Exception:
            try:
                return pickle.loads(data)
            except Exception:
                return None

    async def get(self, **kwargs) -> Optional[Any]:
        key = self._key(**kwargs)
        now = time.time()
        async with self._lock:
            if key in self._l1:
                val, exp = self._l1[key]
                if now < exp:
                    self._l1_access[key] = now
                    return val
                self._l1.pop(key, None)
                self._l1_access.pop(key, None)

        disk_path = os.path.join(self._dir, key)
        if os.path.exists(disk_path):
            try:
                if (time.time() - os.path.getmtime(disk_path)) <= self.l3_ttl:
                    with open(disk_path, "rb") as f:
                        raw = f.read()
                    return self._decompress(raw)
            except Exception:
                pass
        return None

    async def set(self, value: Any, **kwargs) -> None:
        key = self._key(**kwargs)
        now = time.time()
        async with self._lock:
            if len(self._l1) >= self.max_l1_size:
                oldest = min(self._l1_access.items(), key=lambda x: x[1])[0]
                self._l1.pop(oldest, None)
                self._l1_access.pop(oldest, None)
            self._l1[key] = (value, now + self.l1_ttl)
            self._l1_access[key] = now

        # disk write async-ish
        try:
            with open(os.path.join(self._dir, key), "wb") as f:
                f.write(self._compress(value))
        except Exception:
            pass

# ============================================================================
# SingleFlight
# ============================================================================
class SingleFlight:
    def __init__(self) -> None:
        self._lock = asyncio.Lock()
        self._calls: Dict[str, asyncio.Future] = {}

    async def execute(self, key: str, coro_func: Callable[[], Any]) -> Any:
        async with self._lock:
            if key in self._calls:
                return await self._calls[key]
            fut = asyncio.get_running_loop().create_future()
            self._calls[key] = fut
        try:
            res = await coro_func()
            if not fut.done():
                fut.set_result(res)
            return res
        except Exception as e:
            if not fut.done():
                fut.set_exception(e)
            raise
        finally:
            async with self._lock:
                self._calls.pop(key, None)

# ============================================================================
# DataEngineV4
# ============================================================================
class DataEngineV4:
    def __init__(self, settings: Any = None):
        self.settings = settings
        self.version = "5.1.1"

        self.enabled_providers = _get_env_list("ENABLED_PROVIDERS", DEFAULT_PROVIDERS)
        self.ksa_providers = _get_env_list("KSA_PROVIDERS", DEFAULT_KSA_PROVIDERS)
        self.global_providers = _get_env_list("GLOBAL_PROVIDERS", DEFAULT_GLOBAL_PROVIDERS)

        self.max_concurrency = _get_env_int("DATA_ENGINE_MAX_CONCURRENCY", 25)
        self.request_timeout = _get_env_float("DATA_ENGINE_TIMEOUT_SECONDS", 15.0)

        self._sem = asyncio.Semaphore(self.max_concurrency)
        self._singleflight = SingleFlight()
        self._registry = ProviderRegistry()
        self._cache = MultiLevelCache(
            name="data_engine",
            l1_ttl=_get_env_int("CACHE_L1_TTL", 60),
            l3_ttl=_get_env_int("CACHE_L3_TTL", 3600),
            max_l1_size=_get_env_int("CACHE_L1_MAX", 5000),
        )

        logger.info(f"DataEngineV4 v{self.version} initialized with {len(self.enabled_providers)} providers")

    async def aclose(self) -> None:
        return

    def _providers_for(self, symbol: str) -> List[str]:
        info = get_symbol_info(symbol)
        base = self.ksa_providers if info.get("market") == "KSA" else self.global_providers
        providers = [p for p in base if p in self.enabled_providers]
        providers.sort(key=lambda p: PROVIDER_PRIORITIES.get(p, 999))
        return providers

    def _provider_symbol(self, provider: str, symbol: str) -> str:
        # Yahoo providers need Yahoo-style symbols; others accept normalized.
        if provider.startswith("yahoo"):
            try:
                return to_yahoo_symbol(symbol)  # type: ignore
            except Exception:
                return symbol
        return symbol

    async def _fetch_patch(self, provider: str, symbol: str) -> Tuple[str, Optional[Dict[str, Any]], float, Optional[str]]:
        start = time.time()
        module, stats = await self._registry.get_provider(provider)

        if stats.is_circuit_open:
            return provider, None, 0.0, "circuit_open"

        if module is None:
            err = stats.last_import_error or "provider module missing"
            await self._registry.record_failure(provider, err)
            return provider, None, (time.time() - start) * 1000, err

        fn = _pick_provider_callable(module, provider)
        if fn is None:
            err = f"no callable fetch function for provider '{provider}'"
            await self._registry.record_failure(provider, err)
            return provider, None, (time.time() - start) * 1000, err

        provider_symbol = self._provider_symbol(provider, symbol)

        try:
            # timeout wrapper compatible with py3.11+ and safe fallback
            try:
                async with asyncio.timeout(self.request_timeout):
                    res = await _call_maybe_async(fn, provider_symbol)
            except AttributeError:
                res = await asyncio.wait_for(_call_maybe_async(fn, provider_symbol), timeout=self.request_timeout)

            latency = (time.time() - start) * 1000
            if isinstance(res, dict) and res and "error" not in res:
                res2 = _normalize_patch_keys(_clean_patch(res))
                await self._registry.record_success(provider, latency)
                return provider, res2, latency, None

            # dict but error or empty
            err = None
            if isinstance(res, dict):
                err = res.get("error") or "empty_result"
            else:
                err = "non_dict_result"
            await self._registry.record_failure(provider, str(err))
            return provider, None, latency, str(err)

        except Exception as e:
            latency = (time.time() - start) * 1000
            await self._registry.record_failure(provider, repr(e))
            return provider, None, latency, repr(e)

    def _merge(self, symbol: str, norm: str, patches: List[Tuple[str, Dict[str, Any], float]]) -> Dict[str, Any]:
        merged: Dict[str, Any] = {
            "symbol": norm,
            "symbol_normalized": norm,
            "requested_symbol": symbol,
            "last_updated_utc": _now_utc_iso(),
            "last_updated_riyadh": _now_riyadh_iso(),
            "data_sources": [],
            "provider_latency": {},
        }

        for prov, patch, latency in patches:
            merged["data_sources"].append(prov)
            merged["provider_latency"][prov] = round(latency, 2)

            # prefer fill-missing strategy
            for k, v in patch.items():
                if v is None:
                    continue
                if k not in merged or merged.get(k) in (None, "", 0):
                    merged[k] = v

        # ensure current_price + price coherence
        if merged.get("current_price") is None and merged.get("price") is not None:
            merged["current_price"] = merged.get("price")
        if merged.get("price") is None and merged.get("current_price") is not None:
            merged["price"] = merged.get("current_price")

        return merged

    def _data_quality(self, quote: UnifiedQuote) -> str:
        # minimal: requires current_price
        cp = getattr(quote, "current_price", None)
        return QuoteQuality.MISSING.value if not cp else QuoteQuality.GOOD.value

    async def get_enriched_quote(self, symbol: str, use_cache: bool = True) -> UnifiedQuote:
        return await self._singleflight.execute(f"quote:{symbol}", lambda: self._get_enriched_quote_impl(symbol, use_cache))

    async def _get_enriched_quote_impl(self, symbol: str, use_cache: bool = True) -> UnifiedQuote:
        norm = normalize_symbol(symbol) if callable(normalize_symbol) else _fallback_normalize_symbol(symbol)
        if not norm:
            return UnifiedQuote(symbol=symbol, data_quality=QuoteQuality.MISSING.value, error="Invalid symbol")  # type: ignore

        if use_cache:
            cached = await self._cache.get(symbol=norm)
            if cached:
                try:
                    if isinstance(cached, dict):
                        return UnifiedQuote(**cached)
                    if isinstance(cached, UnifiedQuote):
                        return cached
                except Exception:
                    pass

        providers = self._providers_for(norm)
        if not providers:
            return UnifiedQuote(symbol=norm, data_quality=QuoteQuality.MISSING.value, error="No providers available")  # type: ignore

        # take top 3 by priority
        top = providers[:3]

        results: List[Tuple[str, Optional[Dict[str, Any]], float, Optional[str]]] = []
        async with self._sem:
            gathered = await asyncio.gather(*[self._fetch_patch(p, norm) for p in top], return_exceptions=True)
        for r in gathered:
            if isinstance(r, tuple) and len(r) == 4:
                results.append(r)

        patches_ok: List[Tuple[str, Dict[str, Any], float]] = [(p, patch, lat) for (p, patch, lat, err) in results if patch]

        if not patches_ok:
            # build helpful error message
            stats = await self._registry.get_stats()
            err_detail = {
                "symbol": symbol,
                "normalized": norm,
                "attempted_providers": top,
                "provider_stats": {k: stats.get(k) for k in top},
                "errors": [{ "provider": p, "error": err, "latency_ms": round(lat, 2) } for (p, _, lat, err) in results],
            }
            return UnifiedQuote(symbol=norm, data_quality=QuoteQuality.MISSING.value, error="No data available", debug=err_detail)  # type: ignore

        merged = self._merge(symbol, norm, patches_ok)
        quote = UnifiedQuote(**merged)

        # derive data_quality
        try:
            quote.data_quality = self._data_quality(quote)  # type: ignore
        except Exception:
            pass

        # cache
        if use_cache:
            await self._cache.set(_model_to_dict(quote), symbol=norm)

        return quote

    async def get_enriched_quotes(self, symbols: List[str]) -> List[UnifiedQuote]:
        if not symbols:
            return []
        batch = _get_env_int("BATCH_SIZE", 15)
        out: List[UnifiedQuote] = []
        for i in range(0, len(symbols), batch):
            part = symbols[i:i + batch]
            out.extend(await asyncio.gather(*[self.get_enriched_quote(s) for s in part]))
        return out

    # Backwards-compatible aliases
    get_quote = get_enriched_quote
    get_quotes = get_enriched_quotes
    fetch_quote = get_enriched_quote
    fetch_quotes = get_enriched_quotes

    async def get_stats(self) -> Dict[str, Any]:
        return {
            "version": self.version,
            "enabled_providers": self.enabled_providers,
            "ksa_providers": self.ksa_providers,
            "global_providers": self.global_providers,
            "provider_stats": await self._registry.get_stats(),
        }

# ============================================================================
# Singleton exports
# ============================================================================
_ENGINE_INSTANCE: Optional[DataEngineV4] = None
_ENGINE_LOCK = asyncio.Lock()

async def get_engine() -> DataEngineV4:
    global _ENGINE_INSTANCE
    if _ENGINE_INSTANCE is None:
        async with _ENGINE_LOCK:
            if _ENGINE_INSTANCE is None:
                _ENGINE_INSTANCE = DataEngineV4()
    return _ENGINE_INSTANCE

async def close_engine() -> None:
    global _ENGINE_INSTANCE
    if _ENGINE_INSTANCE:
        await _ENGINE_INSTANCE.aclose()
        _ENGINE_INSTANCE = None

def get_cache() -> Any:
    global _ENGINE_INSTANCE
    return getattr(_ENGINE_INSTANCE, "_cache", None)

DataEngine = DataEngineV4
DataEngineV2 = DataEngineV4
DataEngineV3 = DataEngineV4

__all__ = [
    "DataEngineV4", "DataEngineV3", "DataEngineV2", "DataEngine",
    "get_engine", "close_engine", "get_cache",
    "QuoteQuality", "MarketRegime", "DataSource",
]
