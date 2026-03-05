#!/usr/bin/env python3
# core/data_engine_v2.py
"""
================================================================================
Data Engine V2 — GLOBAL-FIRST ORCHESTRATOR — v5.7.0 (PHASE 5 SCHEMA-ALIGNED+)
================================================================================

WHY v5.7.0 (alignment + hardening)
- ✅ FIX (CRITICAL): Engine sheet-rows now has an explicit SPECIAL DISPATCH map so
  Insights_Analysis / Top_10_Investments / Data_Dictionary NEVER fall back to the
  DEFAULT 80-col instrument schema.
- ✅ Schema-first contract is enforced:
    - headers == schema headers (correct count + correct order)
    - keys    == schema keys    (correct count + correct order)
    - rows    are normalized + projected to schema keys (missing=None)
- ✅ Special sheets:
    - Data_Dictionary:
        - prefers core.sheets.data_dictionary.build_data_dictionary_rows(include_meta_sheet=True)
        - otherwise falls back to internal schema-derived generator (deterministic)
    - Insights_Analysis:
        - prefers core.analysis.insights_builder.* builder functions (best-effort signatures)
        - normalizes output (keys OR headers) to schema keys
    - Top_10_Investments:
        - prefers core.analysis.top10_builder (and common variants)
        - normalizes output (keys OR headers) to schema keys
        - if missing, falls back to delegated builder module; else schema-only empty rows
- ✅ Startup-safe: no network IO at import-time; all heavy imports are lazy inside calls.
- ✅ Keeps your existing provider orchestration, caching, scoring, forecasting logic.

STRICTNESS (prevents silent fallback)
- Env: SCHEMA_STRICT_SHEET_ROWS (default true)
  If true and a requested sheet is not in schema_registry, we return a structured error
  (instead of silently using the union/DEFAULT schema).

================================================================================
"""

from __future__ import annotations

import asyncio
import hashlib
import logging
import math
import os
import pickle
import sys
import time
import zlib
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from enum import Enum
from importlib import import_module
from pathlib import Path
from typing import Any, Callable, Dict, Iterable, List, Optional, Sequence, Tuple, Union

# ---------------------------------------------------------------------------
# Ensure repo root is importable (fixes "No module named 'providers'")
# core/data_engine_v2.py -> core/ -> ROOT
# ---------------------------------------------------------------------------
ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

__version__ = "5.7.0"

logger = logging.getLogger("core.data_engine_v2")
logger.addHandler(logging.NullHandler())

# ---------------------------------------------------------------------------
# Fast JSON (orjson optional)
# ---------------------------------------------------------------------------
try:
    import orjson  # type: ignore

    def json_loads(data: Union[str, bytes]) -> Any:
        if isinstance(data, str):
            data = data.encode("utf-8")
        return orjson.loads(data)

    def json_dumps(obj: Any) -> str:
        return orjson.dumps(obj, default=str).decode("utf-8")

except Exception:
    import json  # type: ignore

    def json_loads(data: Union[str, bytes]) -> Any:
        if isinstance(data, (bytes, bytearray)):
            data = data.decode("utf-8", errors="replace")
        return json.loads(data)

    def json_dumps(obj: Any) -> str:
        return json.dumps(obj, default=str, ensure_ascii=False)


# ---------------------------------------------------------------------------
# Schema Registry (Phase 1)
# ---------------------------------------------------------------------------
try:
    from core.sheets.schema_registry import SCHEMA_REGISTRY, get_sheet_spec  # type: ignore

    _SCHEMA_AVAILABLE = True
except Exception:
    SCHEMA_REGISTRY = {}  # type: ignore
    _SCHEMA_AVAILABLE = False

    def get_sheet_spec(_: str) -> Any:  # type: ignore
        raise KeyError("schema_registry not available")


# ---------------------------------------------------------------------------
# Canonical engine keys (fallback union, even if schema isn't importable yet)
# ---------------------------------------------------------------------------
DEFAULT_ENGINE_KEYS: List[str] = [
    # identity
    "symbol",
    "symbol_normalized",
    "requested_symbol",
    "name",
    "exchange",
    "currency",
    "asset_class",
    # prices / liquidity
    "current_price",
    "price",  # alias
    "previous_close",
    "open_price",
    "day_high",
    "day_low",
    "week_52_high",
    "week_52_low",
    "volume",
    "market_cap",
    "price_change",
    "percent_change",
    "change",  # alias
    "change_pct",  # alias
    # fundamentals / technicals
    "pe_ttm",
    "dividend_yield",
    "rsi_14",
    "volatility_30d",
    # forecast / roi
    "forecast_price_1m",
    "forecast_price_3m",
    "forecast_price_12m",
    "expected_roi_1m",
    "expected_roi_3m",
    "expected_roi_12m",
    "forecast_confidence",
    # scores
    "risk_score",
    "overall_score",
    "valuation_score",
    "momentum_score",
    "confidence_score",
    "recommendation",
    "recommendation_reason",
    # meta / provenance
    "data_quality",
    "error",
    "warning",
    "info",
    "data_sources",
    "provider_latency",
    "last_updated_utc",
    "last_updated_riyadh",
]


def _build_union_schema_keys() -> List[str]:
    """
    Union of keys across all sheets in SCHEMA_REGISTRY, preserving stable order.
    Also appends DEFAULT_ENGINE_KEYS as a safety net.

    NOTE: This is ONLY a fallback for non-schema scenarios.
    Sheet-rows MUST NOT silently collapse to this when schema exists.
    """
    keys: List[str] = []
    seen = set()

    if isinstance(SCHEMA_REGISTRY, dict) and SCHEMA_REGISTRY:
        for _, spec in SCHEMA_REGISTRY.items():
            cols = getattr(spec, "columns", None) or []
            for c in cols:
                k = getattr(c, "key", None)
                if not k:
                    continue
                k = str(k)
                if k and k not in seen:
                    seen.add(k)
                    keys.append(k)

    for k in DEFAULT_ENGINE_KEYS:
        if k not in seen:
            seen.add(k)
            keys.append(k)

    return keys


_SCHEMA_UNION_KEYS: List[str] = _build_union_schema_keys()


def normalize_row_to_schema(schema: Any, rowdict: Dict[str, Any], *, keep_extras: bool = True) -> Dict[str, Any]:
    """
    Normalize a row dict to a schema, guaranteeing all keys exist.

    `schema` can be:
      - sheet name (str) -> uses schema_registry
      - sheet spec object (has .columns with .key)
      - list/tuple of keys
      - None -> uses union schema across all sheets (+ DEFAULT_ENGINE_KEYS)

    Returns: dict with ALL schema keys, missing => None.
    """
    keys: List[str] = []

    if schema is None:
        keys = list(_SCHEMA_UNION_KEYS)

    elif isinstance(schema, str):
        try:
            spec = get_sheet_spec(schema)
            keys = [str(getattr(c, "key", "")) for c in (getattr(spec, "columns", None) or [])]
            keys = [k for k in keys if k]
        except Exception:
            keys = list(_SCHEMA_UNION_KEYS)

    elif isinstance(schema, (list, tuple)):
        keys = [str(k) for k in schema if str(k).strip()]

    else:
        cols = getattr(schema, "columns", None)
        if cols:
            keys = [getattr(c, "key", None) for c in cols]
            keys = [str(k) for k in keys if k]
        else:
            keys = list(_SCHEMA_UNION_KEYS)

    raw = rowdict or {}
    out: Dict[str, Any] = {k: raw.get(k, None) for k in keys}

    if keep_extras:
        for k, v in raw.items():
            if k not in out:
                out[k] = v

    return out


# ---------------------------------------------------------------------------
# Pydantic / dataclass safe detection
# ---------------------------------------------------------------------------
def _model_to_dict(obj: Any) -> Dict[str, Any]:
    if obj is None:
        return {}
    if isinstance(obj, dict):
        return obj
    if hasattr(obj, "model_dump") and callable(getattr(obj, "model_dump")):
        try:
            return obj.model_dump(mode="python")
        except Exception:
            try:
                return obj.model_dump()
            except Exception:
                return {}
    if hasattr(obj, "dict") and callable(getattr(obj, "dict")):
        try:
            return obj.dict()
        except Exception:
            return {}
    if hasattr(obj, "__dict__"):
        try:
            return dict(obj.__dict__)
        except Exception:
            pass
    return {"result": obj}


# ---------------------------------------------------------------------------
# UnifiedQuote import (fallback is expanded for Phase 4 keys)
# ---------------------------------------------------------------------------
try:
    from core.schemas import UnifiedQuote  # type: ignore

    SCHEMAS_AVAILABLE = True
except Exception:
    SCHEMAS_AVAILABLE = False
    try:
        from pydantic import BaseModel, Field  # type: ignore
    except Exception:  # pragma: no cover
        BaseModel = object  # type: ignore

        def Field(default=None, **kwargs):  # type: ignore
            return default

    class UnifiedQuote(BaseModel):  # type: ignore
        # identity
        symbol: str = Field(default="")
        symbol_normalized: Optional[str] = None
        requested_symbol: Optional[str] = None
        name: Optional[str] = None
        exchange: Optional[str] = None
        currency: Optional[str] = None
        asset_class: Optional[str] = None

        # price/liquidity
        current_price: Optional[float] = None
        price: Optional[float] = None
        previous_close: Optional[float] = None
        open_price: Optional[float] = None
        day_high: Optional[float] = None
        day_low: Optional[float] = None
        week_52_high: Optional[float] = None
        week_52_low: Optional[float] = None
        volume: Optional[float] = None
        market_cap: Optional[float] = None
        price_change: Optional[float] = None
        percent_change: Optional[float] = None
        change: Optional[float] = None
        change_pct: Optional[float] = None

        # fundamentals/technicals
        pe_ttm: Optional[float] = None
        dividend_yield: Optional[float] = None
        rsi_14: Optional[float] = None
        volatility_30d: Optional[float] = None

        # forecast/roi
        forecast_price_1m: Optional[float] = None
        forecast_price_3m: Optional[float] = None
        forecast_price_12m: Optional[float] = None
        expected_roi_1m: Optional[float] = None
        expected_roi_3m: Optional[float] = None
        expected_roi_12m: Optional[float] = None
        forecast_confidence: Optional[float] = None

        # scores
        risk_score: Optional[float] = None
        overall_score: Optional[float] = None
        valuation_score: Optional[float] = None
        momentum_score: Optional[float] = None
        confidence_score: Optional[float] = None
        recommendation: Optional[str] = None
        recommendation_reason: Optional[str] = None

        # meta
        data_quality: str = "MISSING"
        error: Optional[str] = None
        warning: Optional[str] = None
        info: Optional[Any] = None
        latency_ms: Optional[float] = None
        data_sources: Optional[List[str]] = None
        provider_latency: Optional[Dict[str, float]] = None
        last_updated_utc: Optional[str] = None
        last_updated_riyadh: Optional[str] = None

        class Config:
            extra = "allow"


# ---------------------------------------------------------------------------
# Enums
# ---------------------------------------------------------------------------
class QuoteQuality(str, Enum):
    EXCELLENT = "EXCELLENT"
    GOOD = "GOOD"
    FAIR = "FAIR"
    POOR = "POOR"
    MISSING = "MISSING"


class DataSource(str, Enum):
    CACHE = "cache"
    PRIMARY = "primary"
    FALLBACK = "fallback"
    ENRICHMENT = "enrichment"


# ---------------------------------------------------------------------------
# Symbol normalization (supports BOTH layouts)
# - symbols/normalize.py
# - core/symbols/normalize.py
# ---------------------------------------------------------------------------
def _fallback_is_ksa(s: str) -> bool:
    u = (s or "").strip().upper()
    if u.startswith("TADAWUL:"):
        u = u.split(":", 1)[1].strip()
    if u.endswith(".SR"):
        code = u[:-3].strip()
        return code.isdigit() and 3 <= len(code) <= 6
    return u.isdigit() and 3 <= len(u) <= 6


def _fallback_normalize_symbol(s: str) -> str:
    u = (s or "").strip().upper()
    if not u:
        return ""
    if u.startswith("TADAWUL:"):
        u = u.split(":", 1)[1].strip()
    if _fallback_is_ksa(u):
        if u.endswith(".SR"):
            code = u[:-3].strip()
            return f"{code}.SR"
        if u.isdigit():
            return f"{u}.SR"
    return u


def _fallback_to_yahoo_symbol(s: str) -> str:
    u = _fallback_normalize_symbol(s)
    if _fallback_is_ksa(u):
        return u if u.endswith(".SR") else f"{u}.SR"
    return u


try:
    from symbols.normalize import normalize_symbol as normalize_symbol  # type: ignore
    from symbols.normalize import is_ksa as is_ksa  # type: ignore

    try:
        from symbols.normalize import to_yahoo_symbol as to_yahoo_symbol  # type: ignore
    except Exception:
        to_yahoo_symbol = _fallback_to_yahoo_symbol  # type: ignore

except Exception:
    try:
        from core.symbols.normalize import normalize_symbol as normalize_symbol  # type: ignore
        from core.symbols.normalize import is_ksa as is_ksa  # type: ignore

        try:
            from core.symbols.normalize import to_yahoo_symbol as to_yahoo_symbol  # type: ignore
        except Exception:
            to_yahoo_symbol = _fallback_to_yahoo_symbol  # type: ignore

    except Exception:
        normalize_symbol = _fallback_normalize_symbol  # type: ignore
        is_ksa = _fallback_is_ksa  # type: ignore
        to_yahoo_symbol = _fallback_to_yahoo_symbol  # type: ignore


def get_symbol_info(symbol: str) -> Dict[str, Any]:
    norm = normalize_symbol(symbol) if callable(normalize_symbol) else _fallback_normalize_symbol(symbol)
    ksa = bool(is_ksa(norm)) if callable(is_ksa) else _fallback_is_ksa(norm)
    return {"raw": symbol, "normalized": norm, "market": "KSA" if ksa else "GLOBAL", "is_ksa": ksa}


# ---------------------------------------------------------------------------
# Settings loader (optional)
# ---------------------------------------------------------------------------
def _try_get_settings() -> Any:
    try:
        from core.config import get_settings_cached  # type: ignore

        return get_settings_cached()
    except Exception:
        return None


# ---------------------------------------------------------------------------
# Env helpers
# ---------------------------------------------------------------------------
def _get_env_list(key: str, default: str) -> List[str]:
    raw = os.getenv(key, default) or default
    return [s.strip().lower() for s in raw.split(",") if s.strip()]


def _get_env_int(key: str, default: int) -> int:
    try:
        return int(float(os.getenv(key, str(default))))
    except Exception:
        return default


def _get_env_float(key: str, default: float) -> float:
    try:
        return float(os.getenv(key, str(default)))
    except Exception:
        return default


def _get_env_str(key: str, default: str = "") -> str:
    return (os.getenv(key, default) or default).strip()


def _get_env_bool(key: str, default: bool = False) -> bool:
    raw = (os.getenv(key) or "").strip().lower()
    if not raw:
        return default
    return raw in {"1", "true", "yes", "y", "on", "t"}


def _feature_flags(settings: Any) -> Dict[str, bool]:
    def _get(name: str, env: str, default: bool) -> bool:
        if settings is not None and hasattr(settings, name):
            try:
                return bool(getattr(settings, name))
            except Exception:
                return default
        return _get_env_bool(env, default)

    return {
        "computations_enabled": _get("computations_enabled", "COMPUTATIONS_ENABLED", True),
        "fundamentals_enabled": _get("fundamentals_enabled", "FUNDAMENTALS_ENABLED", True),
        "technicals_enabled": _get("technicals_enabled", "TECHNICALS_ENABLED", True),
        "forecasting_enabled": _get("forecasting_enabled", "FORECASTING_ENABLED", True),
        "scoring_enabled": _get("scoring_enabled", "SCORING_ENABLED", True),
    }


# ---------------------------------------------------------------------------
# Time helpers
# ---------------------------------------------------------------------------
def _now_utc_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _now_riyadh_iso() -> str:
    return datetime.now(timezone(timedelta(hours=3))).isoformat()


# ---------------------------------------------------------------------------
# Patch helpers
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
    return {k: v for k, v in (patch or {}).items() if v is not None and v != ""}


_PATCH_ALIASES: Dict[str, str] = {
    "last_price": "current_price",
    "price": "current_price",
    "prev_close": "previous_close",
    "close": "previous_close",
    "open": "open_price",
    "high": "day_high",
    "low": "day_low",
    "52w_high": "week_52_high",
    "high_52w": "week_52_high",
    "52w_low": "week_52_low",
    "low_52w": "week_52_low",
    "change": "price_change",
    "price_diff": "price_change",
    "pct_change": "percent_change",
    "percent_change": "percent_change",
    "change_percent": "percent_change",
    "change_pct": "percent_change",
}


def _normalize_patch_keys(patch: Dict[str, Any]) -> Dict[str, Any]:
    if not patch:
        return {}
    out = dict(patch)

    for src, dst in _PATCH_ALIASES.items():
        if src in out and (dst not in out or out.get(dst) in (None, "")):
            out[dst] = out.get(src)

    if "current_price" in out and ("price" not in out or out.get("price") in (None, "")):
        out["price"] = out.get("current_price")
    if "price" in out and ("current_price" not in out or out.get("current_price") in (None, "")):
        out["current_price"] = out.get("price")

    if "price_change" in out and ("change" not in out or out.get("change") in (None, "")):
        out["change"] = out.get("price_change")
    if "percent_change" in out and ("change_pct" not in out or out.get("change_pct") in (None, "")):
        out["change_pct"] = out.get("percent_change")

    return out


def _is_useful_patch(p: Dict[str, Any]) -> bool:
    if not isinstance(p, dict) or not p:
        return False
    if _safe_float(p.get("current_price")) is not None:
        return True
    if _safe_float(p.get("price")) is not None:
        return True
    if (str(p.get("name") or "")).strip():
        return True
    if (str(p.get("currency") or "")).strip():
        return True
    return False


# ============================================================================
# Provider configuration
# ============================================================================
DEFAULT_PROVIDERS = "tadawul,argaam,yahoo_chart,yahoo_fundamentals,finnhub,eodhd"
DEFAULT_KSA_PROVIDERS = "tadawul,argaam,yahoo_chart"
DEFAULT_GLOBAL_PROVIDERS = "eodhd,yahoo_chart,yahoo_fundamentals,finnhub"  # GLOBAL-FIRST

PROVIDER_PRIORITIES = {
    "tadawul": 10,
    "argaam": 20,
    "yahoo_chart": 30,
    "yahoo_fundamentals": 35,
    "finnhub": 50,
    "eodhd": 60,
}

PROVIDER_MODULE_CANDIDATES: Dict[str, List[str]] = {
    "tadawul": ["providers.tadawul_provider", "core.providers.tadawul_provider"],
    "argaam": ["providers.argaam_provider", "core.providers.argaam_provider"],
    "yahoo_chart": ["providers.yahoo_chart_provider", "core.providers.yahoo_chart_provider"],
    "yahoo_fundamentals": ["providers.yahoo_fundamentals_provider", "core.providers.yahoo_fundamentals_provider"],
    "finnhub": ["providers.finnhub_provider", "core.providers.finnhub_provider"],
    "eodhd": ["providers.eodhd_provider", "core.providers.eodhd_provider"],
}

PROVIDER_FUNCTIONS: Dict[str, List[str]] = {
    "tadawul": ["fetch_enriched_quote_patch", "fetch_quote_patch", "fetch_patch"],
    "argaam": ["fetch_enriched_quote_patch", "fetch_quote_patch", "fetch_patch"],
    "yahoo_chart": ["fetch_enriched_quote_patch", "fetch_quote_patch", "fetch_patch"],
    "yahoo_fundamentals": ["fetch_fundamentals_patch", "fetch_enriched_quote_patch", "fetch_patch"],
    "finnhub": ["fetch_enriched_quote_patch", "fetch_quote_patch", "fetch_patch"],
    "eodhd": ["fetch_enriched_quote_patch", "fetch_quote_patch", "fetch_patch"],
}


def _import_provider(provider_name: str) -> Tuple[Optional[Any], Optional[str]]:
    last_err: Optional[str] = None
    for module_path in PROVIDER_MODULE_CANDIDATES.get(provider_name, []):
        try:
            return import_module(module_path), None
        except Exception as e:
            last_err = f"{module_path}: {e!r}"
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
    last_import_attempt_utc: float = 0.0

    @property
    def avg_latency_ms(self) -> float:
        return self.total_latency_ms / self.success_count if self.success_count > 0 else 0.0

    @property
    def success_rate(self) -> float:
        t = self.success_count + self.failure_count
        return self.success_count / t if t > 0 else 1.0

    @property
    def is_circuit_open(self) -> bool:
        if not self.circuit_open_until:
            return False
        return datetime.now(timezone.utc) < self.circuit_open_until

    def record_success(self, latency_ms: float) -> None:
        self.success_count += 1
        self.total_latency_ms += float(latency_ms or 0.0)
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
            self.circuit_open_until = datetime.now(timezone.utc) + timedelta(seconds=cooldown)


class ProviderRegistry:
    def __init__(self) -> None:
        self._lock = asyncio.Lock()
        self._providers: Dict[str, Tuple[Optional[Any], ProviderStats]] = {}

    async def get_provider(self, name: str) -> Tuple[Optional[Any], ProviderStats]:
        retry_sec = _get_env_int("PROVIDER_IMPORT_RETRY_SEC", 60)
        now = time.time()

        async with self._lock:
            if name not in self._providers:
                module, import_err = _import_provider(name)
                stats = ProviderStats(name=name, last_import_error=import_err, last_import_attempt_utc=now)
                self._providers[name] = (module, stats)
                return self._providers[name]

            module, stats = self._providers[name]

            if module is None and stats.last_import_error and (now - stats.last_import_attempt_utc) >= retry_sec:
                module2, import_err2 = _import_provider(name)
                stats.last_import_error = import_err2
                stats.last_import_attempt_utc = now
                self._providers[name] = (module2, stats)
                module = module2

            return module, stats

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
            out: Dict[str, Any] = {}
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
# Cache (L1 memory + disk)
# ============================================================================
class MultiLevelCache:
    def __init__(self, name: str, l1_ttl: int = 60, l3_ttl: int = 3600, max_l1_size: int = 5000):
        self.name = name
        self.l1_ttl = max(1, int(l1_ttl))
        self.l3_ttl = max(1, int(l3_ttl))
        self.max_l1_size = max(128, int(max_l1_size))
        self._l1: Dict[str, Tuple[Any, float]] = {}
        self._l1_access: Dict[str, float] = {}
        self._lock = asyncio.Lock()
        self._dir = os.path.join("/tmp", f"cache_{name}")
        os.makedirs(self._dir, exist_ok=True)

    def _key(self, **kwargs) -> str:
        payload = json_dumps(kwargs)
        h = hashlib.sha256(payload.encode("utf-8")).hexdigest()[:16]
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
            item = self._l1.get(key)
            if item:
                val, exp = item
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
                    val = self._decompress(raw)
                    if val is not None:
                        async with self._lock:
                            if len(self._l1) >= self.max_l1_size and self._l1_access:
                                oldest = min(self._l1_access.items(), key=lambda x: x[1])[0]
                                self._l1.pop(oldest, None)
                                self._l1_access.pop(oldest, None)
                            self._l1[key] = (val, now + self.l1_ttl)
                            self._l1_access[key] = now
                    return val
            except Exception:
                pass
        return None

    async def set(self, value: Any, **kwargs) -> None:
        key = self._key(**kwargs)
        now = time.time()

        async with self._lock:
            if len(self._l1) >= self.max_l1_size and self._l1_access:
                oldest = min(self._l1_access.items(), key=lambda x: x[1])[0]
                self._l1.pop(oldest, None)
                self._l1_access.pop(oldest, None)
            self._l1[key] = (value, now + self.l1_ttl)
            self._l1_access[key] = now

        try:
            with open(os.path.join(self._dir, key), "wb") as f:
                f.write(self._compress(value))
        except Exception:
            pass


# ============================================================================
# SingleFlight (no await inside lock)
# ============================================================================
class SingleFlight:
    def __init__(self) -> None:
        self._lock = asyncio.Lock()
        self._calls: Dict[str, asyncio.Future] = {}

    async def execute(self, key: str, coro_func: Callable[[], Any]) -> Any:
        async with self._lock:
            fut = self._calls.get(key)
            if fut is None:
                fut = asyncio.get_running_loop().create_future()
                self._calls[key] = fut
                owner = True
            else:
                owner = False

        if not owner:
            return await fut  # type: ignore

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
# Optional Scoring / Forecasting adapters (LAZY import; best-effort)
# ============================================================================
_SCORING_MOD: Optional[Any] = None
_FORECAST_MOD: Optional[Any] = None
_ANALYTICS_IMPORT_LOCK = asyncio.Lock()


def _try_import_any(paths: Sequence[str]) -> Optional[Any]:
    for p in paths:
        try:
            return import_module(p)
        except Exception:
            continue
    return None


async def _get_scoring_module() -> Optional[Any]:
    global _SCORING_MOD
    if _SCORING_MOD is not None:
        return _SCORING_MOD
    async with _ANALYTICS_IMPORT_LOCK:
        if _SCORING_MOD is None:
            _SCORING_MOD = _try_import_any(["core.scoring", "core.analysis.scoring"])
    return _SCORING_MOD


async def _get_forecast_module() -> Optional[Any]:
    global _FORECAST_MOD
    if _FORECAST_MOD is not None:
        return _FORECAST_MOD
    async with _ANALYTICS_IMPORT_LOCK:
        if _FORECAST_MOD is None:
            _FORECAST_MOD = _try_import_any(["core.forecasting", "core.analysis.forecasting"])
    return _FORECAST_MOD


def _fn_accepts_settings(fn: Callable) -> bool:
    try:
        co = getattr(fn, "__code__", None)
        varnames = getattr(co, "co_varnames", ()) if co is not None else ()
        return "settings" in set(varnames or ())
    except Exception:
        return False


async def _maybe_apply_scoring(row: Dict[str, Any], settings: Any) -> Dict[str, Any]:
    mod = await _get_scoring_module()
    if mod is None:
        return row

    for fn_name in ("compute_scores", "score_row", "score_quote"):
        fn = getattr(mod, fn_name, None)
        if callable(fn):
            try:
                r = fn(row, settings=settings) if _fn_accepts_settings(fn) else fn(row)
                if asyncio.iscoroutine(r) or asyncio.isfuture(r):
                    r = await r
                if isinstance(r, dict):
                    for k, v in r.items():
                        if v is not None:
                            row[k] = v
            except Exception:
                pass
            break
    return row


async def _maybe_apply_forecast(row: Dict[str, Any], settings: Any) -> Dict[str, Any]:
    mod = await _get_forecast_module()
    if mod is None:
        return row

    for fn_name in ("compute_forecast", "forecast_row", "forecast_quote"):
        fn = getattr(mod, fn_name, None)
        if callable(fn):
            try:
                r = fn(row, settings=settings) if _fn_accepts_settings(fn) else fn(row)
                if asyncio.iscoroutine(r) or asyncio.isfuture(r):
                    r = await r
                if isinstance(r, dict):
                    for k, v in r.items():
                        if v is not None:
                            row[k] = v
            except Exception:
                pass
            break
    return row


# ============================================================================
# Sheet schema helpers (CRITICAL for correctness)
# ============================================================================
_ROWS_BUILDER_MOD: Optional[Any] = None
_ROWS_BUILDER_LOCK = asyncio.Lock()


def _canonicalize_sheet_name(sheet: str) -> str:
    s = (sheet or "").strip()
    if not s:
        return s

    # Prefer page_catalog resolve/canonicalize if available
    try:
        from core.sheets.page_catalog import resolve_page  # type: ignore

        out = resolve_page(s)
        if isinstance(out, str) and out.strip():
            return out.strip()
    except Exception:
        pass

    try:
        from core.sheets.page_catalog import canonicalize_page  # type: ignore

        out = canonicalize_page(s)
        if isinstance(out, str) and out.strip():
            return out.strip()
    except Exception:
        pass

    return s.replace(" ", "_")


def _schema_for_sheet(sheet: str) -> Tuple[Optional[Any], List[str], List[str], str]:
    """
    Returns (spec, headers, keys, source)
    """
    if not sheet:
        return None, [], [], "none"
    try:
        spec = get_sheet_spec(sheet)
        cols = getattr(spec, "columns", None) or []
        headers = [str(getattr(c, "header", "")) for c in cols]
        keys = [str(getattr(c, "key", "")) for c in cols]
        headers = [h for h in headers if h]
        keys = [k for k in keys if k]
        if headers and keys and len(headers) == len(keys):
            return spec, headers, keys, "schema_registry.get_sheet_spec"
        return spec, headers, keys, "schema_registry.partial"
    except Exception:
        return None, [], [], "none"


def _strict_project_row(keys: List[str], row: Dict[str, Any]) -> Dict[str, Any]:
    return {k: row.get(k, None) for k in keys}


def _rows_matrix(rows: List[Dict[str, Any]], keys: List[str]) -> List[List[Any]]:
    return [[r.get(k) for k in keys] for r in rows]


def _normalize_to_schema_keys(schema_keys: Sequence[str], schema_headers: Sequence[str], raw: Dict[str, Any]) -> Dict[str, Any]:
    """
    Normalizes a raw row dict to schema KEYS.
    Supports raw using either schema keys OR schema headers.
    Missing -> None
    Extras are ignored by callers via _strict_project_row.
    """
    raw = raw or {}
    raw_ci = {str(k).strip().lower(): v for k, v in raw.items()}

    header_by_key: Dict[str, str] = {}
    for k, h in zip(schema_keys, schema_headers):
        header_by_key[str(k)] = str(h)

    out: Dict[str, Any] = {}
    for k in schema_keys:
        ks = str(k)
        v = None

        if ks in raw:
            v = raw.get(ks)
        else:
            v = raw_ci.get(ks.lower())

        if v is None:
            h = header_by_key.get(ks, "")
            if h:
                if h in raw:
                    v = raw.get(h)
                else:
                    v = raw_ci.get(h.strip().lower())

        out[ks] = v

    return out


async def _try_load_rows_builder() -> Optional[Any]:
    """
    Best-effort import of an existing repo rows builder module (if your repo has one).
    We use it to preserve your current “how to build rows” logic, but we ALWAYS enforce schema on output.
    """
    global _ROWS_BUILDER_MOD
    if _ROWS_BUILDER_MOD is not None:
        return _ROWS_BUILDER_MOD

    async with _ROWS_BUILDER_LOCK:
        if _ROWS_BUILDER_MOD is not None:
            return _ROWS_BUILDER_MOD

        candidates = [
            "core.sheets.sheet_rows",
            "core.sheets.rows_builder",
            "core.sheets.service",
            "core.sheets.sheet_rows_builder",
            "core.sheet_rows",
            "core.rows_builder",
        ]
        for modpath in candidates:
            try:
                _ROWS_BUILDER_MOD = import_module(modpath)
                return _ROWS_BUILDER_MOD
            except Exception:
                continue

        _ROWS_BUILDER_MOD = None
        return None


async def _delegate_build_rows(
    sheet: str,
    *,
    limit: int,
    offset: int,
    mode: str,
    body: Dict[str, Any],
) -> Tuple[List[Dict[str, Any]], Optional[str]]:
    """
    Try to call an existing build_sheet_rows / sheet_rows function if present in repo.
    Returns (rows, note). Never raises.
    """
    mod = await _try_load_rows_builder()
    if mod is None:
        return [], "no_builder_module"

    fn_candidates = [
        "build_sheet_rows",
        "sheet_rows",
        "get_sheet_rows",
        "build_rows",
        "get_rows",
    ]

    for fn_name in fn_candidates:
        fn = getattr(mod, fn_name, None)
        if not callable(fn):
            continue
        try:
            out = fn(sheet=sheet, limit=limit, offset=offset, mode=mode, body=body)
            if hasattr(out, "__await__"):
                out = await out
            if isinstance(out, dict):
                rows = out.get("rows") or out.get("data") or out.get("items") or []
                if isinstance(rows, list):
                    rows = [r for r in rows if isinstance(r, dict)]
                    return rows, f"delegated:{fn_name}"
                return [], f"delegated:{fn_name}:no_rows"
            if isinstance(out, list):
                rows = [r for r in out if isinstance(r, dict)]
                return rows, f"delegated:{fn_name}"
        except Exception as e:
            return [], f"delegated:{fn_name}:error:{e!s}"

    return [], "builder_no_callable"


def _list_sheet_names_best_effort() -> List[str]:
    if isinstance(SCHEMA_REGISTRY, dict) and SCHEMA_REGISTRY:
        try:
            return [str(k) for k in SCHEMA_REGISTRY.keys()]
        except Exception:
            pass
    try:
        from core.sheets.page_catalog import CANONICAL_PAGES  # type: ignore

        return [str(x) for x in CANONICAL_PAGES]
    except Exception:
        return []


def _data_dictionary_rows(
    headers: List[str],
    keys: List[str],
    *,
    limit: int,
    offset: int,
) -> List[Dict[str, Any]]:
    sheet_names = _list_sheet_names_best_effort() or [
        "Market_Leaders",
        "Global_Markets",
        "Commodities_FX",
        "Mutual_Funds",
        "My_Portfolio",
        "Insights_Analysis",
        "Top_10_Investments",
        "Data_Dictionary",
    ]

    rows: List[Dict[str, Any]] = []
    for sn in sheet_names:
        spec, _, _, _src = _schema_for_sheet(sn)
        cols = getattr(spec, "columns", None) if spec else None
        if not cols:
            continue

        for c in cols:
            group = getattr(c, "group", None) or getattr(spec, "group", None) or ""
            source = getattr(c, "source", None) or getattr(spec, "source", None) or ""
            notes = getattr(c, "notes", None) or getattr(spec, "notes", None) or ""
            dtype = getattr(c, "dtype", None) or getattr(c, "type", None) or ""
            fmt = getattr(c, "format", None) or getattr(c, "fmt", None) or ""
            req = getattr(c, "required", None)
            required = bool(req) if req is not None else False

            base = {
                "Sheet": sn,
                "Group": str(group) if group is not None else "",
                "Header": str(getattr(c, "header", "")),
                "Key": str(getattr(c, "key", "")),
                "DType": str(dtype) if dtype is not None else "",
                "Format": str(fmt) if fmt is not None else "",
                "Required": required,
                "Source": str(source) if source is not None else "",
                "Notes": str(notes) if notes is not None else "",
            }

            row: Dict[str, Any] = {}
            for k in keys:
                lk = k.strip().lower().replace(" ", "_")
                if k in base:
                    row[k] = base[k]
                elif lk in ("sheet", "page"):
                    row[k] = base["Sheet"]
                elif lk == "group":
                    row[k] = base["Group"]
                elif lk in ("header", "column"):
                    row[k] = base["Header"]
                elif lk == "key":
                    row[k] = base["Key"]
                elif lk in ("dtype", "type"):
                    row[k] = base["DType"]
                elif lk in ("format", "fmt"):
                    row[k] = base["Format"]
                elif lk in ("required", "is_required", "req"):
                    row[k] = base["Required"]
                elif lk == "source":
                    row[k] = base["Source"]
                elif lk in ("notes", "note", "description"):
                    row[k] = base["Notes"]
                else:
                    row[k] = None

            rows.append(row)

    return rows[offset : offset + limit]


async def _call_builder_best_effort(
    module_names: Sequence[str],
    function_names: Sequence[str],
    *,
    sheet: str,
    limit: int,
    offset: int,
    mode: str,
    body: Dict[str, Any],
    settings: Any,
) -> Tuple[Optional[Any], Optional[str]]:
    """
    Import first available module, pick first callable function, call with best-effort signatures.
    Returns (result, note) where result is builder output (any), and note indicates which function used.
    Never raises (returns None, error-note).
    """
    last_err: Optional[str] = None
    mod = None
    for mn in module_names:
        try:
            mod = import_module(mn)
            break
        except Exception as e:
            last_err = f"{mn}: {e!s}"
            continue
    if mod is None:
        return None, f"import_failed:{last_err or 'no_modules'}"

    fn = None
    used_name = None
    for name in function_names:
        cand = getattr(mod, name, None)
        if callable(cand):
            fn = cand
            used_name = name
            break
    if fn is None:
        return None, "no_callable"

    candidates: List[Tuple[Tuple[Any, ...], Dict[str, Any]]] = [
        ((), {"request": None, "settings": settings, "mode": mode, "body": body, "sheet": sheet, "limit": limit, "offset": offset}),
        ((), {"settings": settings, "mode": mode, "body": body, "sheet": sheet, "limit": limit, "offset": offset}),
        ((), {"settings": settings, "mode": mode, "body": body}),
        ((), {"settings": settings, "mode": mode}),
        ((), {"mode": mode, "body": body}),
        ((), {"mode": mode}),
        ((), {"body": body}),
        ((sheet,), {"limit": limit, "offset": offset, "mode": mode, "body": body, "settings": settings}),
        ((sheet,), {"limit": limit, "offset": offset, "mode": mode, "body": body}),
        ((sheet,), {"limit": limit, "offset": offset, "mode": mode}),
        ((sheet,), {"limit": limit, "offset": offset}),
        ((sheet,), {}),
        ((), {}),
    ]

    for args, kwargs in candidates:
        try:
            out = fn(*args, **kwargs)
            if asyncio.iscoroutine(out) or asyncio.isfuture(out) or hasattr(out, "__await__"):
                out = await out
            return out, f"builder:{used_name}"
        except TypeError:
            continue
        except Exception as e:
            return None, f"builder_error:{used_name}:{e!s}"

    return None, f"builder_signature_mismatch:{used_name or 'unknown'}"


def _coerce_rows_list(out: Any) -> List[Dict[str, Any]]:
    """
    Coerce builder output to list[dict].
    Accepts:
      - list[dict]
      - dict envelope: rows/data/items/records
      - single dict row
    """
    if out is None:
        return []
    if isinstance(out, list):
        return [r for r in out if isinstance(r, dict)]
    if isinstance(out, dict):
        r2 = out.get("rows") or out.get("data") or out.get("items") or out.get("records")
        if isinstance(r2, list):
            return [r for r in r2 if isinstance(r, dict)]
        return [out]
    return []


# ============================================================================
# DataEngine V5 (v5.7.0)
# ============================================================================
class DataEngineV5:
    def __init__(self, settings: Any = None):
        self.settings = settings if settings is not None else _try_get_settings()
        self.flags = _feature_flags(self.settings)
        self.version = __version__

        if self.settings is not None:
            try:
                enabled = [str(x).lower() for x in (getattr(self.settings, "enabled_providers", None) or [])]
            except Exception:
                enabled = []
            try:
                ksa_list = [str(x).lower() for x in (getattr(self.settings, "ksa_providers", None) or [])]
            except Exception:
                ksa_list = []
            try:
                primary = str(getattr(self.settings, "primary_provider", "eodhd") or "eodhd").lower()
            except Exception:
                primary = "eodhd"
        else:
            enabled = _get_env_list("ENABLED_PROVIDERS", DEFAULT_PROVIDERS)
            ksa_list = _get_env_list("KSA_PROVIDERS", DEFAULT_KSA_PROVIDERS)
            primary = _get_env_str("PRIMARY_PROVIDER", "eodhd").lower()

        self.primary_provider = primary or "eodhd"
        self.enabled_providers = enabled or _get_env_list("ENABLED_PROVIDERS", DEFAULT_PROVIDERS)
        self.ksa_providers = ksa_list or _get_env_list("KSA_PROVIDERS", DEFAULT_KSA_PROVIDERS)
        self.global_providers = _get_env_list("GLOBAL_PROVIDERS", DEFAULT_GLOBAL_PROVIDERS)

        self.max_concurrency = _get_env_int("DATA_ENGINE_MAX_CONCURRENCY", 25)
        self.request_timeout = _get_env_float("DATA_ENGINE_TIMEOUT_SECONDS", 20.0)

        self.ksa_disallow_eodhd = _get_env_bool("KSA_DISALLOW_EODHD", False)

        # IMPORTANT: strict sheet-rows prevents silent “default schema” collapses
        self.schema_strict_sheet_rows = _get_env_bool("SCHEMA_STRICT_SHEET_ROWS", True)

        self._sem = asyncio.Semaphore(max(1, self.max_concurrency))
        self._singleflight = SingleFlight()
        self._registry = ProviderRegistry()
        self._cache = MultiLevelCache(
            name="data_engine",
            l1_ttl=_get_env_int("CACHE_L1_TTL", 60),
            l3_ttl=_get_env_int("CACHE_L3_TTL", 3600),
            max_l1_size=_get_env_int("CACHE_L1_MAX", 5000),
        )

        logger.info(
            "DataEngineV5 v%s initialized | primary=%s | enabled=%s | ksa=%s | global=%s | flags=%s | schema=%s | strict_sheet_rows=%s",
            self.version,
            self.primary_provider,
            len(self.enabled_providers),
            len(self.ksa_providers),
            len(self.global_providers),
            self.flags,
            "available" if _SCHEMA_AVAILABLE else "missing",
            self.schema_strict_sheet_rows,
        )

    async def aclose(self) -> None:
        return

    # -----------------------------
    # Provider ordering (GLOBAL-first)
    # -----------------------------
    def _providers_for(self, symbol: str) -> List[str]:
        info = get_symbol_info(symbol)
        is_ksa_sym = bool(info.get("is_ksa"))

        base = self.ksa_providers if is_ksa_sym else self.global_providers
        providers = [p for p in base if p in self.enabled_providers]

        if is_ksa_sym and self.ksa_disallow_eodhd:
            providers = [p for p in providers if p != "eodhd"]

        if self.primary_provider and (self.primary_provider in self.enabled_providers):
            if self.primary_provider in providers:
                providers = [p for p in providers if p != self.primary_provider]
                providers.insert(0, self.primary_provider)
            else:
                if (not is_ksa_sym) or (self.primary_provider != "eodhd") or (not self.ksa_disallow_eodhd):
                    providers.insert(0, self.primary_provider)

        seen: set = set()
        providers = [p for p in providers if not (p in seen or seen.add(p))]

        def pr(p: str) -> int:
            return PROVIDER_PRIORITIES.get(p, 999)

        if providers:
            head = providers[0]
            tail = sorted(providers[1:], key=pr)
            return [head] + tail

        return providers

    def _provider_symbol(self, provider: str, symbol: str) -> str:
        if provider.startswith("yahoo"):
            try:
                return to_yahoo_symbol(symbol)  # type: ignore
            except Exception:
                return symbol
        return symbol

    async def _fetch_patch(self, provider: str, symbol: str) -> Tuple[str, Optional[Dict[str, Any]], float, Optional[str]]:
        start = time.time()

        async with self._sem:
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
                async with asyncio.timeout(self.request_timeout):
                    res = await _call_maybe_async(fn, provider_symbol)

                latency = (time.time() - start) * 1000

                if isinstance(res, dict) and res:
                    patch = _normalize_patch_keys(_clean_patch(res))
                    if _is_useful_patch(patch):
                        await self._registry.record_success(provider, latency)
                        return provider, patch, latency, None

                    err = str(res.get("error") or "empty_result")
                    await self._registry.record_failure(provider, err)
                    return provider, None, latency, err

                err = "non_dict_or_empty"
                await self._registry.record_failure(provider, err)
                return provider, None, latency, err

            except TimeoutError:
                latency = (time.time() - start) * 1000
                err = "timeout"
                await self._registry.record_failure(provider, err)
                return provider, None, latency, err

            except Exception as e:
                latency = (time.time() - start) * 1000
                await self._registry.record_failure(provider, repr(e))
                return provider, None, latency, repr(e)

    def _merge(self, requested_symbol: str, norm: str, patches: List[Tuple[str, Dict[str, Any], float]]) -> Dict[str, Any]:
        merged: Dict[str, Any] = {
            "symbol": norm,
            "symbol_normalized": norm,
            "requested_symbol": requested_symbol,
            "last_updated_utc": _now_utc_iso(),
            "last_updated_riyadh": _now_riyadh_iso(),
            "data_sources": [],
            "provider_latency": {},
        }

        protected = {"symbol", "symbol_normalized", "requested_symbol"}

        for prov, patch, latency in patches:
            merged["data_sources"].append(prov)
            merged["provider_latency"][prov] = round(float(latency or 0.0), 2)

            for k, v in patch.items():
                if k in protected or v is None:
                    continue
                if k not in merged or merged.get(k) in (None, "", []):
                    merged[k] = v

        if merged.get("current_price") is None and merged.get("price") is not None:
            merged["current_price"] = merged.get("price")
        if merged.get("price") is None and merged.get("current_price") is not None:
            merged["price"] = merged.get("current_price")

        if merged.get("price_change") is None and merged.get("change") is not None:
            merged["price_change"] = merged.get("change")
        if merged.get("change") is None and merged.get("price_change") is not None:
            merged["change"] = merged.get("price_change")

        if merged.get("percent_change") is None and merged.get("change_pct") is not None:
            merged["percent_change"] = merged.get("change_pct")
        if merged.get("change_pct") is None and merged.get("percent_change") is not None:
            merged["change_pct"] = merged.get("percent_change")

        return merged

    def _data_quality(self, row: Dict[str, Any]) -> str:
        cp = row.get("current_price")
        if _safe_float(cp) is None:
            return QuoteQuality.MISSING.value
        return QuoteQuality.GOOD.value

    # -----------------------------
    # Public Quote API
    # -----------------------------
    async def get_enriched_quote(self, symbol: str, use_cache: bool = True, *, schema: Any = None) -> UnifiedQuote:
        return await self._singleflight.execute(
            f"quote:{symbol}",
            lambda: self._get_enriched_quote_impl(symbol, use_cache, schema=schema),
        )

    async def get_enriched_quote_dict(self, symbol: str, use_cache: bool = True, *, schema: Any = None) -> Dict[str, Any]:
        q = await self.get_enriched_quote(symbol, use_cache=use_cache, schema=schema)
        return _model_to_dict(q)

    async def _get_enriched_quote_impl(self, symbol: str, use_cache: bool = True, *, schema: Any = None) -> UnifiedQuote:
        norm = normalize_symbol(symbol) if callable(normalize_symbol) else _fallback_normalize_symbol(symbol)
        if not norm:
            row = {
                "symbol": symbol,
                "symbol_normalized": None,
                "requested_symbol": symbol,
                "data_quality": QuoteQuality.MISSING.value,
                "error": "Invalid symbol",
                "last_updated_utc": _now_utc_iso(),
                "last_updated_riyadh": _now_riyadh_iso(),
            }
            row = normalize_row_to_schema(schema, row) if schema is not None else normalize_row_to_schema(None, row)
            return UnifiedQuote(**row)  # type: ignore

        if use_cache:
            cached = await self._cache.get(symbol=norm)
            if cached:
                try:
                    if isinstance(cached, dict):
                        return UnifiedQuote(**cached)  # type: ignore
                    if isinstance(cached, UnifiedQuote):
                        return cached  # type: ignore
                except Exception:
                    pass

        providers = self._providers_for(norm)
        if not providers:
            row = {
                "symbol": norm,
                "symbol_normalized": norm,
                "requested_symbol": symbol,
                "data_quality": QuoteQuality.MISSING.value,
                "error": "No providers available",
                "data_sources": [],
                "provider_latency": {},
                "last_updated_utc": _now_utc_iso(),
                "last_updated_riyadh": _now_riyadh_iso(),
            }
            row = normalize_row_to_schema(schema, row) if schema is not None else normalize_row_to_schema(None, row)
            return UnifiedQuote(**row)  # type: ignore

        top_n = _get_env_int("PROVIDER_TOP_N", 3)
        top = providers[: max(1, int(top_n))]

        gathered = await asyncio.gather(*[self._fetch_patch(p, norm) for p in top], return_exceptions=True)

        results: List[Tuple[str, Optional[Dict[str, Any]], float, Optional[str]]] = []
        for r in gathered:
            if isinstance(r, tuple) and len(r) == 4:
                results.append(r)

        patches_ok: List[Tuple[str, Dict[str, Any], float]] = [(p, patch, lat) for (p, patch, lat, _) in results if patch]

        if not patches_ok:
            stats = await self._registry.get_stats()
            err_detail = {
                "requested": symbol,
                "normalized": norm,
                "attempted_providers": top,
                "provider_stats": {k: stats.get(k) for k in top},
                "errors": [{"provider": p, "error": err, "latency_ms": round(lat, 2)} for (p, _, lat, err) in results],
            }
            row = {
                "symbol": norm,
                "symbol_normalized": norm,
                "requested_symbol": symbol,
                "data_quality": QuoteQuality.MISSING.value,
                "error": "No data available",
                "info": err_detail,
                "data_sources": [],
                "provider_latency": {},
                "last_updated_utc": _now_utc_iso(),
                "last_updated_riyadh": _now_riyadh_iso(),
            }
            row = normalize_row_to_schema(schema, row) if schema is not None else normalize_row_to_schema(None, row)
            q = UnifiedQuote(**row)  # type: ignore
            if use_cache:
                await self._cache.set(_model_to_dict(q), symbol=norm)
            return q

        row = self._merge(symbol, norm, patches_ok)

        if self.flags.get("computations_enabled", True):
            if self.flags.get("forecasting_enabled", True):
                row = await _maybe_apply_forecast(row, self.settings)
            if self.flags.get("scoring_enabled", True):
                row = await _maybe_apply_scoring(row, self.settings)

        row["data_quality"] = self._data_quality(row)
        row = normalize_row_to_schema(schema, row) if schema is not None else normalize_row_to_schema(None, row)

        q = UnifiedQuote(**row)  # type: ignore
        if use_cache:
            await self._cache.set(_model_to_dict(q), symbol=norm)
        return q

    async def get_enriched_quotes(self, symbols: List[str], *, schema: Any = None) -> List[UnifiedQuote]:
        if not symbols:
            return []
        batch = _get_env_int("QUOTE_BATCH_SIZE", 25)
        try:
            if self.settings is not None and getattr(self.settings, "quote_batch_size", None):
                batch = int(getattr(self.settings, "quote_batch_size"))
        except Exception:
            pass
        batch = max(1, min(500, int(batch)))
        out: List[UnifiedQuote] = []
        for i in range(0, len(symbols), batch):
            part = symbols[i : i + batch]
            out.extend(await asyncio.gather(*[self.get_enriched_quote(s, schema=schema) for s in part]))
        return out

    async def get_enriched_quotes_batch(self, symbols: List[str], mode: str = "", *, schema: Any = None) -> Dict[str, Dict[str, Any]]:
        out: Dict[str, Dict[str, Any]] = {}
        if not symbols:
            return out
        quotes = await asyncio.gather(*[self.get_enriched_quote_dict(s, schema=schema) for s in symbols])
        for s, qd in zip(symbols, quotes):
            out[s] = qd
        return out

    # Backwards-compatible aliases
    get_quote = get_enriched_quote
    get_quotes = get_enriched_quotes
    fetch_quote = get_enriched_quote
    fetch_quotes = get_enriched_quotes
    get_quotes_batch = get_enriched_quotes_batch

    # -----------------------------
    # ✅ PHASE 5: Schema-correct Sheet Rows API (SPECIAL DISPATCH + STRICT)
    # -----------------------------
    async def get_sheet_rows(
        self,
        *,
        sheet: str,
        limit: int = 2000,
        offset: int = 0,
        mode: str = "",
        body: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        """
        Engine-native sheet rows builder that NEVER silently falls back to the DEFAULT 80-column schema
        when schema_registry knows the requested sheet.

        Returns (schema-correct):
            { status, sheet/page, headers, keys, rows, rows_matrix?, meta, version }
        """
        body = body or {}
        limit = max(1, min(5000, int(limit or 2000)))
        offset = max(0, int(offset or 0))

        include_matrix = True
        try:
            v = body.get("include_matrix")
            if isinstance(v, bool):
                include_matrix = v
            elif isinstance(v, (int, float)):
                include_matrix = bool(int(v))
            elif isinstance(v, str):
                include_matrix = v.strip().lower() in {"1", "true", "yes", "y", "on"}
        except Exception:
            include_matrix = True

        raw_sheet = (sheet or "").strip()
        sheet2 = _canonicalize_sheet_name(raw_sheet)

        spec, headers, keys, schema_src = _schema_for_sheet(sheet2)

        # STRICTNESS: if schema exists in your project, unknown sheets should NOT collapse to union/default.
        if (not headers or not keys) and self.schema_strict_sheet_rows:
            if _SCHEMA_AVAILABLE:
                return {
                    "status": "error",
                    "sheet": sheet2,
                    "page": sheet2,
                    "headers": [],
                    "keys": [],
                    "rows": [],
                    "rows_matrix": [] if include_matrix else None,
                    "error": f"Unknown sheet or schema missing for '{sheet2}'",
                    "meta": {
                        "schema_source": schema_src,
                        "strict": True,
                        "known_sheets": _list_sheet_names_best_effort(),
                    },
                    "version": self.version,
                }

        # -----------------------------
        # ✅ SPECIAL DISPATCH (explicit)
        # -----------------------------
        if sheet2 == "Data_Dictionary":
            # Prefer canonical generator if present (aligned with routers)
            dd_note = None
            rows: List[Dict[str, Any]] = []
            try:
                from core.sheets.data_dictionary import build_data_dictionary_rows as _dd  # type: ignore

                raw_rows = _dd(include_meta_sheet=True)
                rows = [_normalize_to_schema_keys(keys, headers, r) for r in _coerce_rows_list(raw_rows)]
                dd_note = "core.sheets.data_dictionary.build_data_dictionary_rows"
            except Exception:
                # Fallback to internal generator (deterministic)
                if not headers or not keys:
                    headers = ["Sheet", "Group", "Header", "Key", "DType", "Format", "Required", "Source", "Notes"]
                    keys = headers[:]
                    schema_src = "fallback:standard_data_dictionary"
                rows = _data_dictionary_rows(headers, keys, limit=limit, offset=offset)
                dd_note = "fallback:_data_dictionary_rows"

            # paginate if canonical generator returned all rows
            if dd_note and dd_note.startswith("core."):
                rows = rows[offset : offset + limit]

            payload = {
                "status": "success",
                "sheet": "Data_Dictionary",
                "page": "Data_Dictionary",
                "headers": headers,
                "keys": keys,
                "rows": [_strict_project_row(keys, r) for r in rows],
                "rows_matrix": _rows_matrix([_strict_project_row(keys, r) for r in rows], keys) if include_matrix else None,
                "meta": {
                    "schema_source": schema_src,
                    "builder": dd_note,
                    "rows": len(rows),
                    "limit": limit,
                    "offset": offset,
                },
                "version": self.version,
            }
            return payload

        if sheet2 == "Insights_Analysis":
            out, note = await _call_builder_best_effort(
                module_names=("core.analysis.insights_builder",),
                function_names=(
                    "build_insights_analysis_rows",
                    "build_insights_rows",
                    "build_insights_analysis",
                    "get_insights_rows",
                    "build_rows",
                ),
                sheet=sheet2,
                limit=limit,
                offset=offset,
                mode=mode,
                body=body,
                settings=self.settings,
            )
            rows0 = _coerce_rows_list(out)
            rows_norm = [_strict_project_row(keys, _normalize_to_schema_keys(keys, headers, r)) for r in rows0]
            rows_norm = rows_norm[offset : offset + limit]

            return {
                "status": "success",
                "sheet": sheet2,
                "page": sheet2,
                "headers": headers,
                "keys": keys,
                "rows": rows_norm,
                "rows_matrix": _rows_matrix(rows_norm, keys) if include_matrix else None,
                "meta": {
                    "schema_source": schema_src,
                    "builder": note or "insights_builder",
                    "rows": len(rows_norm),
                    "limit": limit,
                    "offset": offset,
                    "mode": mode,
                },
                "version": self.version,
            }

        if sheet2 == "Top_10_Investments":
            out, note = await _call_builder_best_effort(
                module_names=(
                    "core.analysis.top10_builder",
                    "core.analysis.top_10_builder",
                    "core.analysis.top_10_investments_builder",
                    "core.analysis.top10_investments_builder",
                ),
                function_names=(
                    "build_top_10_investments_rows",
                    "build_top10_investments_rows",
                    "build_top_10_rows",
                    "build_top10_rows",
                    "get_top10_rows",
                    "build_rows",
                ),
                sheet=sheet2,
                limit=limit,
                offset=offset,
                mode=mode,
                body=body,
                settings=self.settings,
            )

            rows0 = _coerce_rows_list(out)
            if rows0:
                rows_norm = [_strict_project_row(keys, _normalize_to_schema_keys(keys, headers, r)) for r in rows0]
                rows_norm = rows_norm[offset : offset + limit]
                return {
                    "status": "success",
                    "sheet": sheet2,
                    "page": sheet2,
                    "headers": headers,
                    "keys": keys,
                    "rows": rows_norm,
                    "rows_matrix": _rows_matrix(rows_norm, keys) if include_matrix else None,
                    "meta": {
                        "schema_source": schema_src,
                        "builder": note or "top10_builder",
                        "rows": len(rows_norm),
                        "limit": limit,
                        "offset": offset,
                        "mode": mode,
                    },
                    "version": self.version,
                }
            # If builder missing, we fall through to delegated builder below (then schema-only)

        # -----------------------------
        # Delegate to existing repo builder (if any), then FORCE schema projection
        # -----------------------------
        delegated_rows: List[Dict[str, Any]] = []
        delegate_note: Optional[str] = None
        try:
            delegated_rows, delegate_note = await _delegate_build_rows(sheet2, limit=limit, offset=offset, mode=mode, body=body)
        except Exception as e:
            delegated_rows, delegate_note = [], f"delegate_error:{e!s}"

        rows: List[Dict[str, Any]] = []
        if delegated_rows and keys:
            for r in delegated_rows:
                rr = _normalize_to_schema_keys(keys, headers, dict(r))
                rows.append(_strict_project_row(keys, rr))
        elif delegated_rows:
            rows = [dict(r) for r in delegated_rows]
        else:
            rows = []

        out_headers = headers[:] if headers else (keys[:] if keys else [])
        out_keys = keys[:] if keys else (headers[:] if headers else [])

        if not out_keys and rows:
            out_keys = list(rows[0].keys())
        if not out_headers and out_keys:
            out_headers = out_keys[:]

        rows = rows[offset : offset + limit] if rows else rows

        return {
            "status": "success" if out_headers else "partial",
            "sheet": sheet2,
            "page": sheet2,
            "headers": out_headers,
            "keys": out_keys,
            "rows": rows,
            "rows_matrix": _rows_matrix(rows, out_keys) if (include_matrix and out_keys) else None,
            "meta": {
                "schema_source": schema_src,
                "strict": self.schema_strict_sheet_rows,
                "delegate": delegate_note or "none",
                "rows": len(rows),
                "limit": limit,
                "offset": offset,
                "mode": mode,
            },
            "version": self.version,
        }

    # Common aliases used by routers in your repo
    async def sheet_rows(self, *args, **kwargs) -> Dict[str, Any]:
        return await self.get_sheet_rows(*args, **kwargs)

    async def build_sheet_rows(self, *args, **kwargs) -> Dict[str, Any]:
        return await self.get_sheet_rows(*args, **kwargs)

    # Health helpers used by routers
    async def health(self) -> Dict[str, Any]:
        return {"status": "ok", "version": self.version, "schema_available": bool(_SCHEMA_AVAILABLE)}

    async def get_health(self) -> Dict[str, Any]:
        return await self.health()

    async def health_check(self) -> Dict[str, Any]:
        return await self.health()

    async def get_stats(self) -> Dict[str, Any]:
        return {
            "version": self.version,
            "primary_provider": self.primary_provider,
            "enabled_providers": self.enabled_providers,
            "ksa_providers": self.ksa_providers,
            "global_providers": self.global_providers,
            "ksa_disallow_eodhd": self.ksa_disallow_eodhd,
            "flags": dict(self.flags),
            "provider_stats": await self._registry.get_stats(),
            "schema_available": bool(_SCHEMA_AVAILABLE),
            "schema_strict_sheet_rows": bool(self.schema_strict_sheet_rows),
        }


# ============================================================================
# Singleton exports
# ============================================================================
_ENGINE_INSTANCE: Optional[DataEngineV5] = None
_ENGINE_LOCK = asyncio.Lock()


async def get_engine() -> DataEngineV5:
    global _ENGINE_INSTANCE
    if _ENGINE_INSTANCE is None:
        async with _ENGINE_LOCK:
            if _ENGINE_INSTANCE is None:
                _ENGINE_INSTANCE = DataEngineV5()
    return _ENGINE_INSTANCE


async def close_engine() -> None:
    global _ENGINE_INSTANCE
    if _ENGINE_INSTANCE:
        await _ENGINE_INSTANCE.aclose()
        _ENGINE_INSTANCE = None


def get_cache() -> Any:
    global _ENGINE_INSTANCE
    return getattr(_ENGINE_INSTANCE, "_cache", None)


# Backwards names
DataEngineV4 = DataEngineV5
DataEngineV3 = DataEngineV5
DataEngineV2 = DataEngineV5
DataEngine = DataEngineV5

__all__ = [
    "DataEngineV5",
    "DataEngineV4",
    "DataEngineV3",
    "DataEngineV2",
    "DataEngine",
    "get_engine",
    "close_engine",
    "get_cache",
    "QuoteQuality",
    "DataSource",
    "__version__",
    # Phase-required export
    "normalize_row_to_schema",
]
