#!/usr/bin/env python3
"""
routes/ai_analysis.py
------------------------------------------------------------
TADAWUL ENTERPRISE AI ANALYSIS ENGINE — v8.6.0 (PHASE 3 SCHEMA-ROWS)
SAMA Compliant | Resilient Routes | Safe Tracing | No Raw 500s

PHASE 3 REQUIREMENTS (DONE)
- ✅ Accept page names + aliases (page_catalog.normalize_page_name)
- ✅ Return FULL schema headers from schema_registry (not custom headers)
- ✅ For each row: emit dict with all schema keys, missing => null
- ✅ Stable ordering:
     - headers/keys follow schema order
     - rows follow requested symbols order
- ✅ Supports computed/analysis fields when enabled (engine enriched output; schema fill guarantees)
- ✅ Never fails on naming / alias mismatch (clean 400)

Extra:
- ✅ page="Data_Dictionary" returns schema dictionary rows (JSON) for debugging / Apps Script
- ✅ Legacy compatibility: optional rows_matrix (list-of-lists) via include_matrix=true
"""

from __future__ import annotations

import asyncio
import hashlib
import logging
import math
import os
import random
import time
import traceback
import uuid
import zlib
import pickle
from dataclasses import dataclass, is_dataclass
from datetime import datetime, timedelta, timezone
from enum import Enum
from typing import Any, Dict, List, Optional, Set, Tuple, Union, Sequence

from fastapi import APIRouter, Body, Header, HTTPException, Query, Request, Response, status

# ---------------------------------------------------------------------------
# Phase 1/3: Schema + Page Catalog
# ---------------------------------------------------------------------------
from core.sheets.schema_registry import get_sheet_spec
from core.sheets.page_catalog import normalize_page_name, CANONICAL_PAGES
from core.sheets.data_dictionary import build_data_dictionary_rows

# ---------------------------------------------------------------------------
# Utility: NaN/Inf safe serialization
# ---------------------------------------------------------------------------
def clean_nans(obj: Any) -> Any:
    if isinstance(obj, dict):
        return {k: clean_nans(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [clean_nans(v) for v in obj]
    if isinstance(obj, float) and (math.isnan(obj) or math.isinf(obj)):
        return None
    return obj

# ---------------------------------------------------------------------------
# JSON response (orjson if available)
# ---------------------------------------------------------------------------
try:
    import orjson
    from fastapi.responses import Response as StarletteResponse

    class BestJSONResponse(StarletteResponse):
        media_type = "application/json"

        def render(self, content: Any) -> bytes:
            return orjson.dumps(clean_nans(content), default=str)

    _HAS_ORJSON = True
except Exception:
    import json
    from fastapi.responses import JSONResponse as _JSONResponse

    class BestJSONResponse(_JSONResponse):
        def render(self, content: Any) -> bytes:
            return json.dumps(clean_nans(content), default=str, ensure_ascii=False).encode("utf-8")

    _HAS_ORJSON = False

logger = logging.getLogger("routes.ai_analysis")

AI_ANALYSIS_VERSION = "8.6.0"
router = APIRouter(prefix="/v1/analysis", tags=["AI & Analysis"])

# ---------------------------------------------------------------------------
# Optional HTTP client for internal fallback
# ---------------------------------------------------------------------------
try:
    import httpx  # type: ignore
    _HTTPX_AVAILABLE = True
except Exception:
    httpx = None
    _HTTPX_AVAILABLE = False

# ---------------------------------------------------------------------------
# Optional OpenTelemetry (safe)
# ---------------------------------------------------------------------------
try:
    from opentelemetry import trace  # type: ignore
    from opentelemetry.trace import Status, StatusCode  # type: ignore
    _OTEL_AVAILABLE = True
    _TRACER = trace.get_tracer(__name__)
except Exception:
    trace = None  # type: ignore
    Status = None  # type: ignore
    StatusCode = None  # type: ignore
    _OTEL_AVAILABLE = False
    _TRACER = None

# ---------------------------------------------------------------------------
# Optional Prometheus
# ---------------------------------------------------------------------------
try:
    from prometheus_client import Counter, generate_latest, CONTENT_TYPE_LATEST  # type: ignore
    _PROMETHEUS_AVAILABLE = True
except Exception:
    _PROMETHEUS_AVAILABLE = False

# ---------------------------------------------------------------------------
# Optional Redis (use redis.asyncio if available)
# ---------------------------------------------------------------------------
try:
    import redis.asyncio as redis_async  # type: ignore
    _REDIS_AVAILABLE = True
except Exception:
    redis_async = None  # type: ignore
    _REDIS_AVAILABLE = False

# ---------------------------------------------------------------------------
# Pydantic (v2 preferred)
# ---------------------------------------------------------------------------
try:
    from pydantic import BaseModel, ConfigDict, Field, model_validator  # type: ignore
    _PYDANTIC_V2 = True
except Exception:
    from pydantic import BaseModel, Field  # type: ignore
    _PYDANTIC_V2 = False

# ---------------------------------------------------------------------------
# core.config (preferred auth + feature flags) — optional
# ---------------------------------------------------------------------------
try:
    from core.config import auth_ok, get_settings_cached  # type: ignore
except Exception:
    auth_ok = None  # type: ignore

    def get_settings_cached(*args, **kwargs):  # type: ignore
        return None


# =============================================================================
# Enums (kept; not all used directly, but safe)
# =============================================================================
class FlexibleEnum(str, Enum):
    @classmethod
    def _missing_(cls, value: object) -> Any:
        val = str(value).upper().replace(" ", "_")
        for member in cls:
            if member.name == val or str(member.value).upper() == val:
                return member
        return None

class DataQuality(FlexibleEnum):
    EXCELLENT = "EXCELLENT"
    GOOD = "GOOD"
    FAIR = "FAIR"
    POOR = "POOR"
    MISSING = "MISSING"
    STALE = "STALE"
    ERROR = "ERROR"

class Recommendation(FlexibleEnum):
    STRONG_BUY = "STRONG_BUY"
    BUY = "BUY"
    HOLD = "HOLD"
    REDUCE = "REDUCE"
    SELL = "SELL"
    STRONG_SELL = "STRONG_SELL"
    UNDER_REVIEW = "UNDER_REVIEW"


# =============================================================================
# Time (Riyadh)
# =============================================================================
class SaudiTime:
    def __init__(self):
        self._tz = timezone(timedelta(hours=3))
        self._weekend_days = [4, 5]  # Fri=4, Sat=5

    def now(self) -> datetime:
        return datetime.now(self._tz)

    def now_iso(self) -> str:
        return self.now().isoformat(timespec="milliseconds")

    def now_utc_iso(self) -> str:
        return datetime.now(timezone.utc).isoformat(timespec="milliseconds")

    def is_trading_day(self, dt: Optional[datetime] = None) -> bool:
        dt = dt or self.now()
        return dt.weekday() not in self._weekend_days

_saudi_time = SaudiTime()


# =============================================================================
# Config
# =============================================================================
@dataclass(slots=True)
class AIConfig:
    batch_concurrency: int = 6
    max_tickers: int = 1200
    cache_ttl_seconds: int = 300
    cache_max_size: int = 10000
    enable_redis_cache: bool = True
    redis_url: str = "redis://localhost:6379"

    adaptive_concurrency: bool = True

    allow_query_token: bool = False
    require_api_key: bool = True
    enable_tracing: bool = True
    trace_sample_rate: float = 1.0

    enable_websocket: bool = False

_CONFIG = AIConfig()

def _load_config_from_env() -> None:
    def env_int(name: str, default: int) -> int:
        try:
            return int(os.getenv(name, str(default)).strip())
        except Exception:
            return default

    def env_float(name: str, default: float) -> float:
        try:
            return float(os.getenv(name, str(default)).strip())
        except Exception:
            return default

    def env_bool(name: str, default: bool) -> bool:
        v = os.getenv(name, str(default)).strip().lower()
        return v in ("true", "1", "yes", "on", "y")

    _CONFIG.batch_concurrency = env_int("AI_BATCH_CONCURRENCY", _CONFIG.batch_concurrency)
    _CONFIG.max_tickers = env_int("AI_MAX_TICKERS", _CONFIG.max_tickers)
    _CONFIG.cache_ttl_seconds = env_int("AI_CACHE_TTL", _CONFIG.cache_ttl_seconds)
    _CONFIG.cache_max_size = env_int("AI_CACHE_MAX_SIZE", _CONFIG.cache_max_size)
    _CONFIG.enable_redis_cache = env_bool("AI_ENABLE_REDIS", _CONFIG.enable_redis_cache)
    _CONFIG.redis_url = os.getenv("REDIS_URL", _CONFIG.redis_url) or _CONFIG.redis_url

    _CONFIG.allow_query_token = env_bool("ALLOW_QUERY_TOKEN", _CONFIG.allow_query_token)
    _CONFIG.require_api_key = env_bool("REQUIRE_API_KEY", _CONFIG.require_api_key)

    _CONFIG.enable_tracing = env_bool("CORE_TRACING_ENABLED", _CONFIG.enable_tracing)
    _CONFIG.trace_sample_rate = env_float("TRACE_SAMPLE_RATE", _CONFIG.trace_sample_rate)

    _CONFIG.adaptive_concurrency = env_bool("ADAPTIVE_CONCURRENCY", _CONFIG.adaptive_concurrency)

_load_config_from_env()


# =============================================================================
# ✅ TraceContext (safe)
# =============================================================================
class TraceContext:
    def __init__(self, name: str, attributes: Optional[Dict[str, Any]] = None):
        self.name = name
        self.attributes = attributes or {}
        self.enabled = bool(_OTEL_AVAILABLE and _CONFIG.enable_tracing and _TRACER is not None)
        self._cm = None
        self.span = None

    async def __aenter__(self):
        if not self.enabled:
            return self
        if random.random() > float(_CONFIG.trace_sample_rate or 1.0):
            return self
        try:
            self._cm = _TRACER.start_as_current_span(self.name)  # type: ignore
            self.span = self._cm.__enter__()
            for k, v in self.attributes.items():
                try:
                    if self.span:
                        self.span.set_attribute(str(k), v)
                except Exception:
                    pass
        except Exception:
            self._cm = None
            self.span = None
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if not self._cm:
            return False
        try:
            if exc_val and self.span and Status is not None and StatusCode is not None:
                try:
                    self.span.record_exception(exc_val)
                except Exception:
                    pass
                try:
                    self.span.set_status(Status(StatusCode.ERROR, str(exc_val)))
                except Exception:
                    pass
        finally:
            try:
                self._cm.__exit__(exc_type, exc_val, exc_tb)
            except Exception:
                pass
        return False


# =============================================================================
# Auth & Rate limiter
# =============================================================================
class TokenManager:
    """
    Fallback token manager if core.config.auth_ok isn't available.
    Behavior:
      - if REQUIRE_API_KEY=true and no tokens configured => DENY (secure by default)
      - else validate against configured tokens
    """
    def __init__(self):
        self._tokens: Set[str] = set()
        for key in ("APP_TOKEN", "BACKUP_APP_TOKEN", "TFB_APP_TOKEN", "BACKEND_TOKEN"):
            t = (os.getenv(key, "") or "").strip()
            if t:
                self._tokens.add(t)

    def validate_token(self, token: str) -> bool:
        token = (token or "").strip()
        if not _CONFIG.require_api_key:
            return True
        if not self._tokens:
            return False
        return token in self._tokens

_token_manager = TokenManager()

class RateLimiter:
    def __init__(self):
        self._buckets: Dict[str, Tuple[int, float]] = {}
        self._lock = asyncio.Lock()
        self.requests = int(os.getenv("RATE_LIMIT_REQUESTS", "120"))
        self.window = int(os.getenv("RATE_LIMIT_WINDOW", "60"))

    async def check(self, key: str) -> bool:
        if self.requests <= 0:
            return True
        async with self._lock:
            now = time.time()
            count, reset = self._buckets.get(key, (0, now + self.window))
            if now > reset:
                count, reset = 0, now + self.window
            if count < self.requests:
                self._buckets[key] = (count + 1, reset)
                return True
            return False

_rate_limiter = RateLimiter()

def _extract_auth_token(token_q: Optional[str], x_app_token: Optional[str], authorization: Optional[str]) -> str:
    if authorization:
        a = authorization.strip()
        if a.lower().startswith("bearer "):
            return a.split(" ", 1)[1].strip()
        return a
    if token_q and token_q.strip():
        return token_q.strip()
    if x_app_token and x_app_token.strip():
        return x_app_token.strip()
    return ""


# =============================================================================
# Cache Manager
# =============================================================================
class CacheManager:
    def __init__(self):
        self._local: Dict[str, Tuple[Any, float]] = {}
        self._lock = asyncio.Lock()
        self._redis = None
        self._redis_inited = False
        self._stats = {"hits": 0, "misses": 0, "redis_hits": 0, "redis_misses": 0}

    def _key(self, key: str, namespace: str = "ai") -> str:
        return f"{namespace}:{hashlib.md5(key.encode()).hexdigest()[:32]}"

    def _compress(self, data: Any) -> bytes:
        return zlib.compress(pickle.dumps(data), level=6)

    def _decompress(self, data: bytes) -> Any:
        try:
            return pickle.loads(zlib.decompress(data))
        except Exception:
            return pickle.loads(data)

    async def _ensure_redis(self) -> None:
        if self._redis_inited:
            return
        self._redis_inited = True
        if not (_CONFIG.enable_redis_cache and _REDIS_AVAILABLE and redis_async):
            return
        try:
            self._redis = redis_async.from_url(_CONFIG.redis_url, encoding=None, decode_responses=False)  # type: ignore
            logger.info("Redis cache enabled")
        except Exception as e:
            logger.warning("Redis init failed: %s", e)
            self._redis = None

    async def get(self, key: str, namespace: str = "ai") -> Optional[Any]:
        ck = self._key(key, namespace)
        now = time.time()

        async with self._lock:
            if ck in self._local:
                v, exp = self._local[ck]
                if exp > now:
                    self._stats["hits"] += 1
                    return v
                del self._local[ck]

        await self._ensure_redis()
        if self._redis:
            try:
                raw = await self._redis.get(ck)
                if raw is not None:
                    self._stats["redis_hits"] += 1
                    self._stats["hits"] += 1
                    v = self._decompress(raw)
                    async with self._lock:
                        self._local[ck] = (v, now + (_CONFIG.cache_ttl_seconds // 2))
                    return v
                self._stats["redis_misses"] += 1
            except Exception:
                self._stats["redis_misses"] += 1

        self._stats["misses"] += 1
        return None

    async def set(self, key: str, value: Any, ttl: Optional[int] = None, namespace: str = "ai") -> None:
        ck = self._key(key, namespace)
        ttl = int(ttl or _CONFIG.cache_ttl_seconds)
        exp = time.time() + ttl

        async with self._lock:
            if len(self._local) >= _CONFIG.cache_max_size:
                items = sorted(self._local.items(), key=lambda x: x[1][1])
                for k, _ in items[: max(1, _CONFIG.cache_max_size // 5)]:
                    self._local.pop(k, None)
            self._local[ck] = (value, exp)

        await self._ensure_redis()
        if self._redis:
            try:
                await self._redis.setex(ck, ttl, self._compress(value))
            except Exception:
                pass

    def stats(self) -> Dict[str, Any]:
        total = self._stats["hits"] + self._stats["misses"]
        return {
            **self._stats,
            "hit_rate": (self._stats["hits"] / total) if total else 0.0,
            "local_cache_size": len(self._local),
            "redis_enabled": bool(self._redis),
        }

_cache = CacheManager()


# =============================================================================
# Request model (Phase 3 schema rows)
# =============================================================================
class AnalysisSheetRowsRequest(BaseModel):
    page: str = Field(default="Market_Leaders", description="Sheet/page name or alias (e.g., 'Market Leaders').")
    symbols: List[str] = Field(default_factory=list)
    tickers: List[str] = Field(default_factory=list)  # backward compat
    top_n: int = Field(default=50, ge=1, le=2000)
    include_matrix: bool = Field(default=True, description="Include rows_matrix for legacy clients.")

    if _PYDANTIC_V2:
        model_config = ConfigDict(extra="ignore")

        @model_validator(mode="after")
        def _combine(self) -> "AnalysisSheetRowsRequest":
            all_syms = list(self.symbols or []) + list(self.tickers or [])
            out: List[str] = []
            seen = set()
            for s in all_syms:
                n = str(s).strip()
                if not n:
                    continue
                n = n.upper()
                if n.startswith("TADAWUL:"):
                    n = n.split(":", 1)[1]
                if n.endswith(".TADAWUL"):
                    n = n.replace(".TADAWUL", "")
                if n and n not in seen:
                    seen.add(n)
                    out.append(n)
            self.symbols = out
            self.tickers = []
            return self


# =============================================================================
# Engine Access + Enriched Fallback
# =============================================================================
def _to_dict(obj: Any) -> Dict[str, Any]:
    if obj is None:
        return {}
    if isinstance(obj, dict):
        return obj
    try:
        if hasattr(obj, "model_dump"):
            return obj.model_dump(mode="python")
        if hasattr(obj, "dict"):
            return obj.dict()
        if is_dataclass(obj):
            return {k: v for k, v in obj.__dict__.items() if not str(k).startswith("_")}
    except Exception:
        pass
    return {"raw": str(obj)}

def _internal_base_url() -> str:
    port = os.getenv("PORT", "10000").strip() or "10000"
    return f"http://127.0.0.1:{port}"

def _needs_provider_fallback(d: Dict[str, Any]) -> bool:
    err = str(d.get("error") or "").lower()
    if not err:
        return False
    return ("no providers available" in err) or ("providers available" in err)

async def _fetch_enriched_via_local_http(symbol: str, auth_token: str) -> Optional[Dict[str, Any]]:
    if not _HTTPX_AVAILABLE or httpx is None:
        return None
    base = _internal_base_url()
    url = f"{base}/v1/enriched/quote"
    headers: Dict[str, str] = {}
    if auth_token:
        headers["Authorization"] = f"Bearer {auth_token}"
        headers["X-APP-TOKEN"] = auth_token

    try:
        async with httpx.AsyncClient(timeout=10.0) as client:
            r = await client.get(
                url,
                params={"symbol": symbol, "debug": False, "include_headers": False, "include_rows": False},
                headers=headers,
            )
            if r.status_code != 200:
                return None
            data = r.json()
            return data if isinstance(data, dict) else None
    except Exception:
        return None

class AnalysisEngine:
    def __init__(self):
        self._engine_lock = asyncio.Lock()

    async def get_engine(self, request: Request) -> Optional[Any]:
        try:
            st = getattr(request.app, "state", None)
            if st and getattr(st, "engine", None):
                return st.engine
        except Exception:
            pass

        async with self._engine_lock:
            # prefer v2 engine
            for modpath in ("core.data_engine_v2", "core.data_engine"):
                try:
                    mod = __import__(modpath, fromlist=["get_engine"])
                    get_engine = getattr(mod, "get_engine", None)
                    if callable(get_engine):
                        eng = get_engine()
                        if hasattr(eng, "__await__"):
                            eng = await eng
                        return eng
                except Exception:
                    continue
        return None

    async def fetch_enriched(self, request: Request, engine: Any, symbols: List[str], auth_token: str, mode: str = "") -> Dict[str, Dict[str, Any]]:
        """
        Fetch enriched quotes (best-effort). If engine returns "No providers available",
        fallback to local /v1/enriched/quote and try .SR if needed.
        """
        out: Dict[str, Dict[str, Any]] = {}

        # Batch methods first
        for method in ("get_enriched_quotes_batch", "get_enriched_quotes", "enriched_quotes", "get_quotes_batch"):
            fn = getattr(engine, method, None)
            if callable(fn):
                try:
                    res = fn(symbols, mode=mode) if mode else fn(symbols)
                    if hasattr(res, "__await__"):
                        res = await res
                    if isinstance(res, dict):
                        for s in symbols:
                            out[s] = _to_dict(res.get(s))
                        break
                except Exception:
                    pass

        # Fallback: per symbol via engine
        if not out:
            per_methods = ("get_enriched_quote", "enriched_quote", "get_quote", "quote")
            sem = asyncio.Semaphore(max(1, int(_CONFIG.batch_concurrency)))

            async def one(s: str):
                async with sem:
                    last_err = None
                    for m in per_methods:
                        fn = getattr(engine, m, None)
                        if callable(fn):
                            try:
                                r = fn(s, mode=mode) if mode and "mode" in getattr(fn, "__code__", {}).co_varnames else fn(s)  # best effort
                                if hasattr(r, "__await__"):
                                    r = await r
                                return s, _to_dict(r)
                            except Exception as e:
                                last_err = str(e)
                                continue
                    return s, {"symbol": s, "error": last_err or "No engine method for enriched quote", "data_quality": "MISSING"}

            results = await asyncio.gather(*(one(s) for s in symbols), return_exceptions=True)
            for r in results:
                if isinstance(r, tuple) and len(r) == 2:
                    out[r[0]] = r[1]

        # Provider fallback to /v1/enriched/quote
        for s in list(symbols):
            d = out.get(s) or {"symbol": s, "error": "Empty result", "data_quality": "MISSING"}
            if _needs_provider_fallback(d):
                candidates = [s]
                if s.isdigit() and len(s) in (3, 4, 5):
                    candidates.append(f"{s}.SR")
                if s.upper().endswith(".SR"):
                    candidates.append(s.split(".", 1)[0])

                fixed = None
                for c in candidates:
                    fixed = await _fetch_enriched_via_local_http(c, auth_token=auth_token)
                    if isinstance(fixed, dict) and not _needs_provider_fallback(fixed) and not fixed.get("error"):
                        break
                    fixed = None

                if fixed:
                    out[s] = fixed

        # normalize timestamps
        for s in symbols:
            d = out.get(s) or {}
            d.setdefault("symbol", s)
            d.setdefault("last_updated_utc", _saudi_time.now_utc_iso())
            d.setdefault("last_updated_riyadh", _saudi_time.now_iso())
            out[s] = d

        return out

_analysis_engine = AnalysisEngine()


# =============================================================================
# Phase 3: Schema normalization
# =============================================================================
_CANONICAL_ALIASES: Dict[str, Tuple[str, ...]] = {
    # identity
    "symbol": ("symbol", "ticker"),
    "name": ("name", "company_name"),
    "exchange": ("exchange",),
    "currency": ("currency",),
    "asset_class": ("asset_class", "type", "instrument_type"),

    # price
    "current_price": ("current_price", "price", "last_price"),
    "previous_close": ("previous_close", "prev_close"),
    "open_price": ("open_price", "open"),
    "day_high": ("day_high", "high"),
    "day_low": ("day_low", "low"),
    "week_52_high": ("week_52_high", "52w_high", "high_52w"),
    "week_52_low": ("week_52_low", "52w_low", "low_52w"),
    "price_change": ("price_change", "change"),
    "percent_change": ("percent_change", "change_pct", "change_percent"),

    # liquidity
    "volume": ("volume",),
    "market_cap": ("market_cap",),

    # fundamentals
    "pe_ttm": ("pe_ttm", "pe_ratio", "pe"),
    "dividend_yield": ("dividend_yield",),

    # technical/risk
    "rsi_14": ("rsi_14", "rsi14", "rsi_14d", "rsi"),
    "volatility_30d": ("volatility_30d", "vol_30d"),
    "risk_score": ("risk_score",),

    # forecast/roi
    "forecast_price_1m": ("forecast_price_1m",),
    "forecast_price_3m": ("forecast_price_3m",),
    "forecast_price_12m": ("forecast_price_12m",),
    "expected_roi_1m": ("expected_roi_1m",),
    "expected_roi_3m": ("expected_roi_3m",),
    "expected_roi_12m": ("expected_roi_12m",),
    "forecast_confidence": ("forecast_confidence",),

    # scores
    "overall_score": ("overall_score",),
    "confidence_score": ("confidence_score",),
    "recommendation": ("recommendation",),
    "recommendation_reason": ("recommendation_reason",),

    # provenance
    "last_updated_utc": ("last_updated_utc",),
    "last_updated_riyadh": ("last_updated_riyadh",),
    "warnings": ("warnings",),
    "data_provider": ("data_provider",),
}

def _normalize_row_to_schema(schema_keys: Sequence[str], raw: Dict[str, Any], *, symbol_fallback: str) -> Dict[str, Any]:
    raw = raw or {}
    raw_lc = {str(k).lower(): v for k, v in raw.items()}
    out: Dict[str, Any] = {}

    for k in schema_keys:
        v = None
        if k in raw:
            v = raw.get(k)
        elif k.lower() in raw_lc:
            v = raw_lc.get(k.lower())
        else:
            aliases = _CANONICAL_ALIASES.get(k, ())
            for a in aliases:
                if a in raw:
                    v = raw.get(a)
                    break
                if a.lower() in raw_lc:
                    v = raw_lc.get(a.lower())
                    break
        out[k] = v

    if "symbol" in out and not out.get("symbol"):
        out["symbol"] = symbol_fallback

    return out


# =============================================================================
# Routes
# =============================================================================
@router.get("/health")
async def health(request: Request) -> Dict[str, Any]:
    engine = await _analysis_engine.get_engine(request)
    settings = None
    try:
        settings = get_settings_cached()
    except Exception:
        settings = None

    return {
        "status": "ok" if engine else "degraded",
        "version": AI_ANALYSIS_VERSION,
        "timestamp_riyadh": _saudi_time.now_iso(),
        "engine": {"available": bool(engine), "type": type(engine).__name__ if engine else "none"},
        "cache": _cache.stats(),
        "schema_pages": list(CANONICAL_PAGES),
        "schema_headers_always": bool(getattr(settings, "schema_headers_always", True)) if settings else True,
        "computations_enabled": bool(getattr(settings, "computations_enabled", True)) if settings else True,
        "request_id": getattr(request.state, "request_id", None),
    }

@router.get("/metrics")
async def metrics() -> Response:
    if not _PROMETHEUS_AVAILABLE:
        return BestJSONResponse(status_code=503, content={"error": "Metrics not available"})
    return Response(content=generate_latest(), media_type=CONTENT_TYPE_LATEST)  # type: ignore

@router.post("/sheet-rows")
async def sheet_rows(
    request: Request,
    body: Dict[str, Any] = Body(...),
    mode: str = Query(default="", description="Mode hint for engine/provider"),
    token: Optional[str] = Query(default=None, description="Auth token (query only if ALLOW_QUERY_TOKEN=true)"),
    x_app_token: Optional[str] = Header(default=None, alias="X-APP-TOKEN"),
    authorization: Optional[str] = Header(default=None, alias="Authorization"),
    x_request_id: Optional[str] = Header(default=None, alias="X-Request-ID"),
) -> BestJSONResponse:
    """
    Phase 3 schema-driven sheet rows:
    - page aliases accepted
    - full schema headers/keys returned
    - rows are list of dicts with all schema keys (missing => null)
    """
    start = time.time()
    request_id = x_request_id or getattr(request.state, "request_id", None) or str(uuid.uuid4())[:18]

    try:
        client_ip = request.client.host if request.client else "unknown"
        if not await _rate_limiter.check(client_ip):
            raise HTTPException(status_code=status.HTTP_429_TOO_MANY_REQUESTS, detail="Rate limit exceeded")

        # Auth token selection
        auth_token = _extract_auth_token(token if _CONFIG.allow_query_token else None, x_app_token, authorization)

        # Prefer core.config auth if available
        if auth_ok is not None:
            if not auth_ok(token=auth_token, authorization=authorization, headers={"X-APP-TOKEN": x_app_token, "Authorization": authorization}):
                raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid token")
        else:
            if not _token_manager.validate_token(auth_token):
                raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid token")

        # Parse request
        async with TraceContext("analysis.sheet_rows.parse", {"request_id": request_id}):
            try:
                req = AnalysisSheetRowsRequest.model_validate(body) if _PYDANTIC_V2 else AnalysisSheetRowsRequest.parse_obj(body)  # type: ignore
            except Exception as e:
                return BestJSONResponse(
                    status_code=400,
                    content={
                        "status": "error",
                        "page": None,
                        "headers": [],
                        "keys": [],
                        "rows": [],
                        "error": f"Invalid request: {e}",
                        "version": AI_ANALYSIS_VERSION,
                        "request_id": request_id,
                        "meta": {"duration_ms": (time.time() - start) * 1000},
                    },
                )

        # Normalize page alias -> canonical
        try:
            page = normalize_page_name(req.page, allow_output_pages=True)
        except Exception as e:
            return BestJSONResponse(
                status_code=400,
                content={
                    "status": "error",
                    "page": req.page,
                    "headers": [],
                    "keys": [],
                    "rows": [],
                    "error": f"Invalid page: {str(e)}",
                    "allowed_pages": list(CANONICAL_PAGES),
                    "version": AI_ANALYSIS_VERSION,
                    "request_id": request_id,
                    "meta": {"duration_ms": (time.time() - start) * 1000},
                },
            )

        # Data_Dictionary path (no engine required)
        if page == "Data_Dictionary":
            spec = get_sheet_spec("Data_Dictionary")
            headers = [c.header for c in spec.columns]
            keys = [c.key for c in spec.columns]

            rows_dict = build_data_dictionary_rows(include_meta_sheet=True)
            normalized_rows = [_normalize_row_to_schema(keys, r, symbol_fallback="") for r in rows_dict]

            payload = {
                "status": "success",
                "page": page,
                "headers": headers,
                "keys": keys,
                "rows": normalized_rows,
                "rows_matrix": [[row.get(k) for k in keys] for row in normalized_rows] if req.include_matrix else None,
                "version": AI_ANALYSIS_VERSION,
                "request_id": request_id,
                "meta": {
                    "duration_ms": (time.time() - start) * 1000,
                    "count": len(normalized_rows),
                    "schema_mode": "data_dictionary",
                },
            }
            return BestJSONResponse(content=clean_nans(payload))

        # Resolve schema for requested page
        try:
            sheet_spec = get_sheet_spec(page)
        except Exception as e:
            return BestJSONResponse(
                status_code=400,
                content={
                    "status": "error",
                    "page": page,
                    "headers": [],
                    "keys": [],
                    "rows": [],
                    "error": f"Schema not found for page '{page}': {str(e)}",
                    "allowed_pages": list(CANONICAL_PAGES),
                    "version": AI_ANALYSIS_VERSION,
                    "request_id": request_id,
                    "meta": {"duration_ms": (time.time() - start) * 1000},
                },
            )

        headers = [c.header for c in sheet_spec.columns]
        keys = [c.key for c in sheet_spec.columns]

        symbols = (req.symbols or [])[: int(req.top_n or 50)]
        if not symbols:
            payload = {
                "status": "skipped",
                "page": page,
                "headers": headers,
                "keys": keys,
                "rows": [],
                "rows_matrix": [] if req.include_matrix else None,
                "error": "No symbols provided",
                "version": AI_ANALYSIS_VERSION,
                "request_id": request_id,
                "meta": {"duration_ms": (time.time() - start) * 1000},
            }
            return BestJSONResponse(content=clean_nans(payload))

        # Engine
        engine = await _analysis_engine.get_engine(request)
        if not engine:
            return BestJSONResponse(
                status_code=503,
                content={
                    "status": "error",
                    "page": page,
                    "headers": headers,
                    "keys": keys,
                    "rows": [],
                    "error": "Data engine unavailable",
                    "version": AI_ANALYSIS_VERSION,
                    "request_id": request_id,
                    "meta": {"duration_ms": (time.time() - start) * 1000},
                },
            )

        # Feature flags snapshot (do not affect headers)
        settings = None
        try:
            settings = get_settings_cached()
        except Exception:
            settings = None

        computations_enabled = bool(getattr(settings, "computations_enabled", True)) if settings else True

        # Fetch enriched data (best effort)
        async with TraceContext("analysis.sheet_rows.fetch", {"count": len(symbols), "request_id": request_id, "mode": mode or ""}):
            data_map = await _analysis_engine.fetch_enriched(request, engine, symbols, auth_token=auth_token, mode=mode or "")

        # Normalize rows to schema (fill missing keys => None)
        normalized_rows: List[Dict[str, Any]] = []
        errors = 0
        for s in symbols:
            d = data_map.get(s) or {"symbol": s, "error": "Not found", "data_quality": "MISSING", "last_updated_riyadh": _saudi_time.now_iso()}
            if d.get("error"):
                errors += 1
            row = _normalize_row_to_schema(keys, _to_dict(d), symbol_fallback=s)
            normalized_rows.append(row)

        status_out = "success" if errors == 0 else ("partial" if errors < len(symbols) else "error")

        payload = {
            "status": status_out,
            "page": page,
            "headers": headers,
            "keys": keys,
            "rows": normalized_rows,
            "rows_matrix": [[row.get(k) for k in keys] for row in normalized_rows] if req.include_matrix else None,
            "error": f"{errors} errors" if errors else None,
            "version": AI_ANALYSIS_VERSION,
            "request_id": request_id,
            "meta": {
                "duration_ms": (time.time() - start) * 1000,
                "requested": len(symbols),
                "errors": errors,
                "cache_stats": _cache.stats(),
                "riyadh_time": _saudi_time.now_iso(),
                "business_day": _saudi_time.is_trading_day(),
                "mode": mode,
                "computations_enabled": computations_enabled,
            },
        }
        return BestJSONResponse(content=clean_nans(payload))

    except HTTPException:
        raise
    except Exception as e:
        logger.error("sheet_rows failed: %s\n%s", e, traceback.format_exc())
        payload = {
            "status": "error",
            "page": None,
            "headers": [],
            "keys": [],
            "rows": [],
            "error": f"Internal Server Error: {e}",
            "version": AI_ANALYSIS_VERSION,
            "request_id": request_id,
            "meta": {"duration_ms": (time.time() - start) * 1000},
        }
        return BestJSONResponse(status_code=500, content=clean_nans(payload))

__all__ = ["router"]
