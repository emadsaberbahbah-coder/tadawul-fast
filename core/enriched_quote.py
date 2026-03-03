#!/usr/bin/env python3
# routes/enriched_quote.py
"""
================================================================================
Enriched Quote Router — v5.4.0 (PHASE 4 SCHEMA-ALIGNED / ENGINE-LAZY)
================================================================================
Goal:
- /v1/enriched/quote must emit KEYS aligned to schema_registry mapping.
- This becomes the standard row object consumed by sheet-row endpoints and fallbacks.

Design:
- Engine is lazy: request.app.state.engine OR core.data_engine_v2.get_engine()
- Output is a schema-aligned row dict with guaranteed keys via normalize_row_to_schema()
- Backward compatible response wrapper:
    { "status": "success", "data": <row>, ... }
  PLUS we also copy row keys to the top-level for easy consumers.

Endpoints:
- GET  /v1/enriched/health
- GET  /v1/enriched/headers
- GET  /v1/enriched/quote
- GET  /v1/enriched/quotes
- POST /v1/enriched/quotes

Notes:
- Percent-like fields are returned as FRACTIONS (0.12 => 12%) when applicable.
- No startup network I/O.
================================================================================
"""

from __future__ import annotations

import asyncio
import inspect
import logging
import math
import os
import re
import time
import traceback
import uuid
from datetime import datetime, timedelta, timezone
from typing import Any, Callable, Dict, List, Optional, Sequence, Set, Tuple, Union

from fastapi import APIRouter, Body, Query, Request
from fastapi.encoders import jsonable_encoder

# ---------------------------------------------------------------------------
# High-Performance JSON response
# ---------------------------------------------------------------------------
try:
    from fastapi.responses import ORJSONResponse as BestJSONResponse
except Exception:
    from starlette.responses import JSONResponse as BestJSONResponse  # type: ignore

# ---------------------------------------------------------------------------
# Optional monitoring
# ---------------------------------------------------------------------------
try:
    from prometheus_client import Counter, Histogram  # type: ignore

    _PROMETHEUS_AVAILABLE = True
    router_requests_total = Counter(
        "router_enriched_requests_total",
        "Total requests to enriched router",
        ["endpoint", "status"],
    )
    router_request_duration = Histogram(
        "router_enriched_duration_seconds",
        "Router request duration",
        ["endpoint"],
    )
except Exception:
    _PROMETHEUS_AVAILABLE = False

    class _DummyMetric:
        def labels(self, *args, **kwargs):  # noqa
            return self

        def inc(self, *args, **kwargs):  # noqa
            return None

        def observe(self, *args, **kwargs):  # noqa
            return None

    router_requests_total = _DummyMetric()
    router_request_duration = _DummyMetric()

# ---------------------------------------------------------------------------
# Optional tracing
# ---------------------------------------------------------------------------
try:
    from opentelemetry import trace  # type: ignore

    tracer = trace.get_tracer(__name__)
except Exception:

    class _DummySpan:
        def set_attribute(self, *args, **kwargs):  # noqa
            return None

        def __enter__(self):  # noqa
            return self

        def __exit__(self, *args, **kwargs):  # noqa
            return None

    class _DummyTracer:
        def start_as_current_span(self, *args, **kwargs):  # noqa
            return _DummySpan()

    tracer = _DummyTracer()

# ---------------------------------------------------------------------------
# Symbol normalization (supports both repo layouts)
# ---------------------------------------------------------------------------
def _fallback_normalize_symbol(s: str) -> str:
    return (s or "").strip().upper()


try:
    from symbols.normalize import normalize_symbol as _normalize_symbol  # type: ignore
except Exception:
    try:
        from core.symbols.normalize import normalize_symbol as _normalize_symbol  # type: ignore
    except Exception:
        _normalize_symbol = _fallback_normalize_symbol  # type: ignore

# ---------------------------------------------------------------------------
# Phase 4 normalize helper (preferred)
# ---------------------------------------------------------------------------
try:
    from core.data_engine_v2 import normalize_row_to_schema as _normalize_row_to_schema  # type: ignore

    _HAS_ENGINE_SCHEMA_NORMALIZER = True
except Exception:
    _HAS_ENGINE_SCHEMA_NORMALIZER = False

    def _normalize_row_to_schema(schema: Any, rowdict: Dict[str, Any]) -> Dict[str, Any]:  # type: ignore
        return dict(rowdict or {})

# ---------------------------------------------------------------------------
# Schema registry (optional)
# ---------------------------------------------------------------------------
try:
    from core.sheets.schema_registry import SCHEMA_REGISTRY  # type: ignore

    _SCHEMA_AVAILABLE = True
except Exception:
    SCHEMA_REGISTRY = {}  # type: ignore
    _SCHEMA_AVAILABLE = False

__version__ = "5.4.0"
ROUTER_VERSION = __version__

logger = logging.getLogger("routes.enriched_quote")

router = APIRouter(prefix="/v1/enriched", tags=["enriched"])

_TRUTHY = {"1", "true", "yes", "y", "on", "t", "enabled", "active"}
_RECOMMENDATIONS = ("STRONG_BUY", "BUY", "HOLD", "REDUCE", "SELL", "STRONG_SELL", "UNDER_REVIEW")
RIYADH_TZ = timezone(timedelta(hours=3))


# =============================================================================
# Safe helpers
# =============================================================================
def _safe_str(x: Any, max_len: int = 0) -> str:
    if x is None:
        return ""
    try:
        s = str(x).strip()
        if max_len > 0 and len(s) > max_len:
            return s[:max_len] + "..."
        return s
    except Exception:
        return ""


def _safe_int(x: Any, default: Optional[int] = None) -> Optional[int]:
    if x is None:
        return default
    try:
        s = str(x).strip().replace(",", "")
        return int(float(s))
    except Exception:
        return default


def _safe_float(x: Any, default: Optional[float] = None) -> Optional[float]:
    if x is None:
        return default
    try:
        if isinstance(x, (int, float)) and not isinstance(x, bool):
            f = float(x)
        else:
            s = str(x).strip().replace(",", "")
            if not s or s.lower() in {"na", "n/a", "null", "none"}:
                return default
            if s.endswith("%"):
                f = float(s[:-1].strip()) / 100.0
            else:
                f = float(s)
        if math.isnan(f) or math.isinf(f):
            return default
        return f
    except Exception:
        return default


def _is_truthy(v: Any) -> bool:
    return str(v or "").strip().lower() in _TRUTHY


def _now_utc_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _now_riyadh_iso() -> str:
    return datetime.now(RIYADH_TZ).isoformat()


def _to_riyadh_iso(dt: Any) -> Optional[str]:
    if dt is None:
        return None
    try:
        d = dt if isinstance(dt, datetime) else datetime.fromisoformat(str(dt).replace("Z", "+00:00"))
        if d.tzinfo is None:
            d = d.replace(tzinfo=timezone.utc)
        return d.astimezone(RIYADH_TZ).isoformat()
    except Exception:
        return _safe_str(dt)


def _split_symbols(raw: str) -> List[str]:
    s = (raw or "").replace("\n", " ").replace("\t", " ").strip().replace(",", " ")
    return [p.strip() for p in s.split(" ") if p.strip()]


def _normalize_recommendation(rec: Any) -> str:
    if rec is None:
        return "HOLD"
    s = _safe_str(rec).upper()
    if not s:
        return "HOLD"
    if s in _RECOMMENDATIONS:
        return s

    s2 = re.sub(r"[\s\-_/]+", " ", s).strip()
    if "STRONG BUY" in s2:
        return "STRONG_BUY"
    if any(x in s2 for x in ["BUY", "ACCUMULATE", "ADD", "OUTPERFORM", "OVERWEIGHT", "LONG"]):
        return "BUY"
    if any(x in s2 for x in ["HOLD", "NEUTRAL", "MAINTAIN", "MARKET PERFORM", "EQUAL WEIGHT"]):
        return "HOLD"
    if any(x in s2 for x in ["REDUCE", "TRIM", "LIGHTEN", "UNDERWEIGHT", "PARTIAL SELL"]):
        return "REDUCE"
    if any(x in s2 for x in ["SELL", "STRONG SELL", "EXIT", "AVOID", "UNDERPERFORM"]):
        return "SELL"
    return "HOLD"


def _as_payload(obj: Any) -> Dict[str, Any]:
    """
    Convert engine return object -> plain dict (safe).
    """
    if obj is None:
        return {}
    if isinstance(obj, dict):
        return jsonable_encoder(obj)
    if hasattr(obj, "model_dump") and callable(obj.model_dump):  # pydantic v2
        try:
            return jsonable_encoder(obj.model_dump(mode="python"))
        except Exception:
            try:
                return jsonable_encoder(obj.model_dump())
            except Exception:
                pass
    if hasattr(obj, "dict") and callable(obj.dict):  # pydantic v1
        try:
            return jsonable_encoder(obj.dict())
        except Exception:
            pass
    if hasattr(obj, "__dict__"):
        try:
            return jsonable_encoder(dict(obj.__dict__))
        except Exception:
            pass
    return jsonable_encoder({"value": _safe_str(obj)})


def _unwrap_container(x: Any) -> Any:
    if not isinstance(x, dict):
        return x
    for key in ("items", "data", "quotes", "results", "payload"):
        if key in x and isinstance(x[key], (list, dict)):
            return x[key]
    return x


def _unwrap_tuple(x: Any) -> Any:
    return x[0] if isinstance(x, tuple) and len(x) >= 1 else x


def _is_ksa_symbol(s: str) -> bool:
    u = _safe_str(s).upper()
    if not u:
        return False
    if u.startswith("TADAWUL:"):
        u = u.split(":", 1)[1].strip()
    if u.endswith(".SR"):
        code = u[:-3].strip()
        return code.isdigit() and 3 <= len(code) <= 6
    return u.isdigit() and 3 <= len(u) <= 6


def _canonical_output_symbol(raw_symbol: str, payload: Dict[str, Any]) -> str:
    """
    Canonical output:
    - KSA codes => NNNN.SR
    - GLOBAL plain tickers => keep as requested
    - If user supplies explicit suffix (e.g., 7203.T) => keep normalized form
    """
    raw = _safe_str(raw_symbol).upper()
    if not raw:
        return _safe_str(payload.get("symbol_normalized") or payload.get("symbol") or "").upper()

    if _is_ksa_symbol(raw):
        if raw.endswith(".SR"):
            return raw
        if raw.isdigit():
            return f"{raw}.SR"
        if raw.startswith("TADAWUL:"):
            code = raw.split(":", 1)[1].strip()
            if code.isdigit():
                return f"{code}.SR"

    if "." not in raw:
        return raw

    try:
        return _normalize_symbol(raw)
    except Exception:
        return raw


def _origin_from_symbol(symbol: str) -> str:
    s = _safe_str(symbol).upper()
    if s.endswith(".SR"):
        return "KSA_TADAWUL"
    return "GLOBAL_MARKETS"


# =============================================================================
# Percent normalization (Sheet-safe fractions)
# =============================================================================
_PERCENT_KEYS = {
    "percent_change",
    "change_pct",
    "dividend_yield",
    "payout_ratio",
    "roe",
    "roa",
    "net_margin",
    "ebitda_margin",
    "revenue_growth",
    "net_income_growth",
    "free_float",
    "turnover_percent",
    "turnover_pct",
    "week_52_position_pct",
}


def _maybe_fraction(x: Any) -> Optional[float]:
    """
    Convert percent-like numbers to fractions:
      - "12%" -> 0.12
      - 12 -> 0.12 (percent points)
      - 0.12 -> 0.12 (already fraction)
    """
    if x is None:
        return None
    if isinstance(x, str):
        s = x.strip()
        if not s:
            return None
        if s.endswith("%"):
            f = _safe_float(s[:-1])
            return None if f is None else f / 100.0
        f = _safe_float(s)
    else:
        f = _safe_float(x)

    if f is None:
        return None
    if abs(f) > 1.5:
        return f / 100.0
    return f


def _compute_change(price: Any, prev_close: Any) -> Tuple[Optional[float], Optional[float]]:
    p = _safe_float(price)
    pc = _safe_float(prev_close)
    if p is None or pc is None or pc == 0:
        return None, None
    change = p - pc
    pct_fraction = (p / pc) - 1.0
    return round(change, 8), round(pct_fraction, 10)


def _compute_52w_position_fraction(price: Any, low_52w: Any, high_52w: Any) -> Optional[float]:
    p = _safe_float(price)
    lo = _safe_float(low_52w)
    hi = _safe_float(high_52w)
    if p is None or lo is None or hi is None or hi == lo:
        return None
    return (p - lo) / (hi - lo)


def _finalize_row(raw_symbol: str, row: Dict[str, Any], *, engine_source: str = "") -> Dict[str, Any]:
    """
    Phase 4: Ensure canonical keys exist + derived values are filled if possible.
    """
    row = dict(row or {})
    row["requested_symbol"] = raw_symbol

    # canonicalize symbol
    out_symbol = _canonical_output_symbol(raw_symbol, row)
    if out_symbol:
        row["symbol_normalized"] = out_symbol
        row["symbol"] = out_symbol
    else:
        row.setdefault("symbol", _safe_str(raw_symbol).upper())
        row.setdefault("symbol_normalized", row.get("symbol"))

    row.setdefault("origin", _origin_from_symbol(row.get("symbol") or ""))

    # timestamps
    row.setdefault("last_updated_utc", _now_utc_iso())
    row.setdefault("last_updated_riyadh", _to_riyadh_iso(row.get("last_updated_utc")) or _now_riyadh_iso())

    # provenance
    if engine_source:
        row.setdefault("engine_source", engine_source)
        row.setdefault("data_source", engine_source)

    # recommendation normalize
    if "recommendation" in row:
        row["recommendation"] = _normalize_recommendation(row.get("recommendation"))

    # canonical price sync
    if row.get("current_price") is None and row.get("price") is not None:
        row["current_price"] = row.get("price")
    if row.get("price") is None and row.get("current_price") is not None:
        row["price"] = row.get("current_price")

    # change/percent_change (canonical) + aliases
    if row.get("price_change") is None or row.get("percent_change") is None:
        ch, pct = _compute_change(row.get("current_price"), row.get("previous_close"))
        if row.get("price_change") is None and ch is not None:
            row["price_change"] = ch
        if row.get("percent_change") is None and pct is not None:
            row["percent_change"] = pct

    # aliases
    if row.get("change") is None and row.get("price_change") is not None:
        row["change"] = row.get("price_change")
    if row.get("change_pct") is None and row.get("percent_change") is not None:
        row["change_pct"] = row.get("percent_change")
    if row.get("price_change") is None and row.get("change") is not None:
        row["price_change"] = row.get("change")
    if row.get("percent_change") is None and row.get("change_pct") is not None:
        row["percent_change"] = row.get("change_pct")

    # 52w position pct (fraction)
    if row.get("week_52_position_pct") is None:
        pos = _compute_52w_position_fraction(row.get("current_price"), row.get("week_52_low"), row.get("week_52_high"))
        if pos is not None:
            row["week_52_position_pct"] = pos

    # percent fields -> fraction normalization
    for k in list(_PERCENT_KEYS):
        if k in row and row[k] is not None:
            v = _maybe_fraction(row[k])
            if v is not None:
                row[k] = v

    # basic data quality
    if not row.get("data_quality"):
        row["data_quality"] = "GOOD" if _safe_float(row.get("current_price")) is not None else "MISSING"

    # containers
    if row.get("data_sources") is None:
        row["data_sources"] = []
    if row.get("provider_latency") is None:
        row["provider_latency"] = {}

    # guarantee schema keys exist (Phase 4 requirement)
    try:
        row = _normalize_row_to_schema(None, row)
    except Exception:
        pass

    return row


# =============================================================================
# Engine calling (lazy + signature-safe kwargs)
# =============================================================================
class EngineCall:
    __slots__ = ("result", "source", "error", "latency_ms")

    def __init__(self, result: Optional[Any] = None, source: str = "", error: Optional[str] = None, latency_ms: float = 0.0):
        self.result = result
        self.source = source
        self.error = error
        self.latency_ms = latency_ms


async def _maybe_await(v: Any) -> Any:
    if inspect.isawaitable(v):
        return await v
    return v


def _filter_kwargs_for_callable(fn: Callable, kwargs: Dict[str, Any]) -> Dict[str, Any]:
    if not kwargs:
        return {}
    try:
        sig = inspect.signature(fn)
        params = sig.parameters
        if any(p.kind == inspect.Parameter.VAR_KEYWORD for p in params.values()):
            return kwargs
        allowed = set(params.keys())
        return {k: v for k, v in kwargs.items() if k in allowed}
    except Exception:
        return {}


_ENGINE_INIT_LOCK = asyncio.Lock()
_ENGINE_LAST_FAIL_AT = 0.0
_ENGINE_FAIL_TTL_SEC = 10.0


async def _get_or_init_engine(request: Request) -> Tuple[Optional[Any], str, Optional[str]]:
    """
    Returns (engine, source, error).
    Caches engine in request.app.state.engine.
    """
    try:
        engine = getattr(request.app.state, "engine", None)
        if engine is not None:
            return engine, "app.state.engine", None
    except Exception:
        pass

    global _ENGINE_LAST_FAIL_AT
    if time.time() - _ENGINE_LAST_FAIL_AT < _ENGINE_FAIL_TTL_SEC:
        return None, "engine_init_throttled", "recent_engine_init_failure"

    async with _ENGINE_INIT_LOCK:
        try:
            engine = getattr(request.app.state, "engine", None)
            if engine is not None:
                return engine, "app.state.engine", None
        except Exception:
            pass

        last_err = None

        try:
            from core.data_engine_v2 import get_engine  # type: ignore

            eng = get_engine()
            engine = await eng if inspect.isawaitable(eng) else eng
            try:
                request.app.state.engine = engine
            except Exception:
                pass
            return engine, "core.data_engine_v2.get_engine", None
        except Exception as e:
            last_err = repr(e)

        try:
            from core.data_engine import get_engine as get_engine_legacy  # type: ignore

            eng2 = get_engine_legacy()
            engine2 = await eng2 if inspect.isawaitable(eng2) else eng2
            try:
                request.app.state.engine = engine2
            except Exception:
                pass
            return engine2, "core.data_engine.get_engine", None
        except Exception as e:
            last_err = f"{last_err} | legacy:{repr(e)}"

        _ENGINE_LAST_FAIL_AT = time.time()
        return None, "engine_init_failed", last_err


# Prefer dict-returning methods if present
_ENGINE_SINGLE_METHODS = (
    "get_enriched_quote_dict",
    "get_enriched_quote",
    "get_quote",
    "quote",
    "fetch_quote",
)

_ENGINE_BATCH_METHODS = (
    "get_enriched_quotes_batch",
    "get_enriched_quotes",
    "get_quotes",
    "quotes",
    "fetch_quotes",
)


async def _call_engine(request: Request, symbol: str, *, schema: Any = None) -> EngineCall:
    start = time.time()
    engine, src, err = await _get_or_init_engine(request)
    if engine is None:
        return EngineCall(error="No engine available", source=src, latency_ms=(time.time() - start) * 1000)

    # Phase 4: encourage schema normalization inside engine if accepted
    call_kwargs = {"schema": schema}

    last_err = None
    for method in _ENGINE_SINGLE_METHODS:
        fn = getattr(engine, method, None)
        if callable(fn):
            try:
                kw = _filter_kwargs_for_callable(fn, call_kwargs)
                res = await _maybe_await(fn(symbol, **kw))
                res = _unwrap_container(_unwrap_tuple(res))
                return EngineCall(result=res, source=f"{src}.{method}", latency_ms=(time.time() - start) * 1000)
            except Exception as e:
                last_err = repr(e)

    return EngineCall(error=last_err or err or "engine_call_failed", source=src, latency_ms=(time.time() - start) * 1000)


async def _call_engine_batch(request: Request, symbols: List[str], *, schema: Any = None) -> EngineCall:
    start = time.time()
    engine, src, err = await _get_or_init_engine(request)
    if engine is None:
        return EngineCall(error="No engine available", source=src, latency_ms=(time.time() - start) * 1000)

    call_kwargs = {"schema": schema}

    last_err = None
    for method in _ENGINE_BATCH_METHODS:
        fn = getattr(engine, method, None)
        if callable(fn):
            try:
                kw = _filter_kwargs_for_callable(fn, call_kwargs)
                res = await _maybe_await(fn(symbols, **kw))
                res = _unwrap_container(_unwrap_tuple(res))
                if isinstance(res, (list, dict)):
                    return EngineCall(result=res, source=f"{src}.{method}", latency_ms=(time.time() - start) * 1000)
            except Exception as e:
                last_err = repr(e)

    return EngineCall(error=last_err or err or "engine_batch_call_failed", source=src, latency_ms=(time.time() - start) * 1000)


# =============================================================================
# Auth (align with core.config when available)
# =============================================================================
def _is_authorized(request: Request) -> bool:
    try:
        from core.config import is_open_mode  # type: ignore

        if callable(is_open_mode) and is_open_mode():
            return True
    except Exception:
        pass

    token = (
        request.headers.get("X-APP-TOKEN")
        or request.headers.get("X-App-Token")
        or request.headers.get("X-API-Key")
        or request.headers.get("X-Api-Key")
        or request.query_params.get("token")
    )
    authz = request.headers.get("Authorization")

    # If auth_ok exists, use it
    try:
        from core.config import auth_ok  # type: ignore

        if callable(auth_ok):
            return bool(auth_ok(token=token, authorization=authz, headers=dict(request.headers)))
    except Exception:
        pass

    # Fallback behavior:
    # - If REQUIRE_AUTH is truthy -> deny when no auth_ok exists
    require_auth = str(os.getenv("REQUIRE_AUTH", "")).strip().lower() in _TRUTHY
    if require_auth:
        return False
    # open by default if not explicitly required
    return True


# =============================================================================
# Headers endpoints (schema introspection)
# =============================================================================
def _schema_union_keys() -> List[str]:
    keys: List[str] = []
    seen: Set[str] = set()
    if isinstance(SCHEMA_REGISTRY, dict):
        for _, spec in SCHEMA_REGISTRY.items():
            cols = getattr(spec, "columns", None) or []
            for c in cols:
                k = getattr(c, "key", None)
                if not k:
                    continue
                if k not in seen:
                    seen.add(k)
                    keys.append(k)
    # minimal must-haves
    for k in ("symbol", "current_price", "previous_close", "price_change", "percent_change", "data_quality", "last_updated_utc", "last_updated_riyadh"):
        if k not in seen:
            keys.append(k)
    return keys


@router.get("/headers", include_in_schema=False)
async def get_headers():
    return BestJSONResponse(
        status_code=200,
        content={
            "status": "success",
            "schema_union_keys": _schema_union_keys(),
            "schema_available": bool(_SCHEMA_AVAILABLE),
            "version": ROUTER_VERSION,
            "timestamp_utc": _now_utc_iso(),
        },
    )


@router.get("/health", include_in_schema=False)
async def health_check(request: Request):
    info: Dict[str, Any] = {
        "status": "ok",
        "module": "enriched_quote",
        "version": ROUTER_VERSION,
        "timestamp_utc": _now_utc_iso(),
        "engine_cached": bool(getattr(getattr(request, "app", None), "state", None) and getattr(request.app.state, "engine", None) is not None),
        "schema_available": bool(_SCHEMA_AVAILABLE),
        "engine_schema_normalizer_available": bool(_HAS_ENGINE_SCHEMA_NORMALIZER),
    }
    try:
        from core.config import config_health_check  # type: ignore

        if callable(config_health_check):
            info["config"] = config_health_check()
    except Exception:
        pass
    return info


# =============================================================================
# Quote endpoints
# =============================================================================
def _build_error_response(
    request_id: str,
    error: str,
    *,
    symbol: str = "",
    status_code: int = 200,
    debug: bool = False,
    trace: Optional[str] = None,
    engine_source: Optional[str] = None,
    engine_error: Optional[str] = None,
) -> BestJSONResponse:
    content: Dict[str, Any] = {
        "status": "error",
        "error": error,
        "request_id": request_id,
        "timestamp_utc": _now_utc_iso(),
        "timestamp_riyadh": _now_riyadh_iso(),
    }
    if symbol:
        content["symbol"] = symbol
    if engine_source:
        content["engine_source"] = engine_source
    if engine_error:
        content["engine_error"] = engine_error
    if debug and trace:
        content["traceback"] = _safe_str(trace, 12000)
    return BestJSONResponse(status_code=status_code, content=content)


@router.get("/quote")
async def get_enriched_quote(
    request: Request,
    symbol: str = Query("", description="Ticker symbol (AAPL, 1120.SR, ^GSPC, GC=F)"),
    debug: bool = Query(False, description="Include debug information"),
    wrap: bool = Query(True, description="Keep wrapper fields (status/data/request_id)."),
):
    """
    Returns schema-aligned row dict for one symbol.
    - By default returns wrapper + row at top level:
        { status, request_id, data: <row>, <row keys...> }
    """
    request_id = request.headers.get("X-Request-ID", str(uuid.uuid4())[:12])
    dbg = _is_truthy(os.getenv("DEBUG_ERRORS", "0")) or debug

    with tracer.start_as_current_span("router_enriched_quote") as span:
        span.set_attribute("symbol", symbol)

        if not _is_authorized(request):
            router_requests_total.labels(endpoint="quote", status="unauthorized").inc()
            return _build_error_response(request_id=request_id, error="unauthorized", status_code=401)

        raw = (symbol or "").strip()
        if not raw:
            router_requests_total.labels(endpoint="quote", status="empty_symbol").inc()
            return _build_error_response(request_id=request_id, error="empty_symbol", symbol=raw)

        start_time = time.time()
        try:
            call = await _call_engine(request, raw, schema=None)
            if call.error or call.result is None:
                router_requests_total.labels(endpoint="quote", status="no_data").inc()
                return _build_error_response(
                    request_id=request_id,
                    error=call.error or "no_data",
                    symbol=raw,
                    debug=dbg,
                    trace=traceback.format_exc() if dbg else None,
                    engine_source=call.source,
                    engine_error=call.error,
                )

            payload = _as_payload(call.result)
            row = _finalize_row(raw, payload, engine_source=call.source)

            router_requests_total.labels(endpoint="quote", status="success").inc()
            router_request_duration.labels(endpoint="quote").observe(time.time() - start_time)

            # Backward compatible wrapper + flat row keys
            if wrap:
                out: Dict[str, Any] = {
                    "status": "success",
                    "request_id": request_id,
                    "timestamp_utc": _now_utc_iso(),
                    "engine_source": call.source,
                    "data": row,
                }
                out.update(row)
                return BestJSONResponse(status_code=200, content=out)

            # Flat only
            return BestJSONResponse(status_code=200, content=row)

        except Exception as e:
            router_requests_total.labels(endpoint="quote", status="error").inc()
            logger.exception("Error processing /v1/enriched/quote")
            return _build_error_response(
                request_id=request_id,
                error=str(e),
                symbol=raw,
                debug=dbg,
                trace=traceback.format_exc() if dbg else None,
            )


@router.get("/quotes")
async def get_enriched_quotes(
    request: Request,
    symbols: str = Query("", description="Comma/space separated symbols"),
    tickers: str = Query("", description="Alias for symbols"),
    format: str = Query("items", description="items | dict"),
    debug: bool = Query(False, description="Include debug information"),
    wrap: bool = Query(True, description="Keep wrapper fields (status/items/request_id)."),
):
    return await _quotes_impl(
        request=request,
        symbols=_dedup_symbols(_split_symbols(symbols) + _split_symbols(tickers)),
        fmt=(format or "").strip().lower(),
        debug=debug,
        wrap=wrap,
    )


@router.post("/quotes")
async def post_enriched_quotes(
    request: Request,
    body: Dict[str, Any] = Body(default_factory=dict),
):
    """
    JSON body supported:
      {
        "symbols": ["AAPL","MSFT"],
        "format": "items" | "dict",
        "debug": true|false,
        "wrap": true|false
      }
    """
    symbols_in = body.get("symbols") or body.get("tickers") or body.get("symbol") or []
    fmt = str(body.get("format") or "items").strip().lower()
    debug = bool(body.get("debug") or False)
    wrap = bool(body.get("wrap") if body.get("wrap") is not None else True)

    raw_list: List[str] = []
    if isinstance(symbols_in, str):
        raw_list = _split_symbols(symbols_in)
    elif isinstance(symbols_in, list):
        for x in symbols_in:
            raw_list.extend(_split_symbols(str(x)))

    return await _quotes_impl(
        request=request,
        symbols=_dedup_symbols(raw_list),
        fmt=fmt,
        debug=debug,
        wrap=wrap,
    )


def _dedup_symbols(symbols: List[str]) -> List[str]:
    seen: Set[str] = set()
    out: List[str] = []
    for s in symbols:
        s = (s or "").strip()
        if not s:
            continue
        if s in seen:
            continue
        seen.add(s)
        out.append(s)
    return out


async def _quotes_impl(
    *,
    request: Request,
    symbols: List[str],
    fmt: str,
    debug: bool,
    wrap: bool,
):
    request_id = request.headers.get("X-Request-ID", str(uuid.uuid4())[:12])
    dbg = _is_truthy(os.getenv("DEBUG_ERRORS", "0")) or debug

    with tracer.start_as_current_span("router_enriched_quotes") as span:
        if not _is_authorized(request):
            router_requests_total.labels(endpoint="quotes", status="unauthorized").inc()
            return _build_error_response(request_id=request_id, error="unauthorized", status_code=401)

        if not symbols:
            router_requests_total.labels(endpoint="quotes", status="empty_symbols_list").inc()
            return _build_error_response(request_id=request_id, error="empty_symbols_list")

        span.set_attribute("symbol_count", len(symbols))
        start_time = time.time()

        try:
            call = await _call_engine_batch(request, symbols, schema=None)

            # Normalize batch result into a dict mapping requested_symbol -> row
            rows_by_requested: Dict[str, Dict[str, Any]] = {}

            if call.result is not None and not call.error:
                if isinstance(call.result, dict):
                    # engine may return dict keyed by input symbol or normalized symbol
                    for k, v in call.result.items():
                        payload = _as_payload(v)
                        # prefer requested_symbol, else key
                        raw = _safe_str(payload.get("requested_symbol") or k)
                        rows_by_requested[raw] = _finalize_row(raw, payload, engine_source=call.source)

                elif isinstance(call.result, list):
                    # best-effort align by order
                    for raw, v in zip(symbols, call.result):
                        payload = _as_payload(v)
                        rows_by_requested[raw] = _finalize_row(raw, payload, engine_source=call.source)

            # Fill missing with per-symbol calls (robust)
            missing = [s for s in symbols if s not in rows_by_requested]
            if missing:
                cfg_conc = max(1, min(50, _safe_int(os.getenv("ENRICHED_CONCURRENCY", 10), 10) or 10))
                cfg_timeout = max(3.0, min(90.0, _safe_float(os.getenv("ENRICHED_TIMEOUT_SEC", 30.0), 30.0) or 30.0))
                sem = asyncio.Semaphore(cfg_conc)

                async def _fetch_one(raw: str) -> Dict[str, Any]:
                    async with sem:
                        try:
                            single = await asyncio.wait_for(_call_engine(request, raw, schema=None), timeout=cfg_timeout)
                            if single.result is not None and not single.error:
                                payload = _as_payload(single.result)
                                return _finalize_row(raw, payload, engine_source=single.source)
                            # error row
                            return _finalize_row(raw, {"error": single.error or "no_data", "data_quality": "MISSING"}, engine_source=single.source)
                        except asyncio.TimeoutError:
                            return _finalize_row(raw, {"error": "timeout", "data_quality": "MISSING"}, engine_source="timeout")
                        except Exception as e:
                            return _finalize_row(raw, {"error": f"exception:{e.__class__.__name__}", "data_quality": "MISSING"}, engine_source="exception")

                fetched = await asyncio.gather(*[_fetch_one(s) for s in missing])
                for raw, row in zip(missing, fetched):
                    rows_by_requested[raw] = row

            # Output shape
            items: List[Dict[str, Any]] = [rows_by_requested[s] for s in symbols if s in rows_by_requested]
            out_obj: Union[List[Dict[str, Any]], Dict[str, Dict[str, Any]]]
            if fmt == "dict":
                out_obj = {s: rows_by_requested.get(s) for s in symbols}
            else:
                out_obj = items

            router_requests_total.labels(endpoint="quotes", status="success").inc()
            router_request_duration.labels(endpoint="quotes").observe(time.time() - start_time)

            if wrap:
                return BestJSONResponse(
                    status_code=200,
                    content={
                        "status": "success",
                        "request_id": request_id,
                        "timestamp_utc": _now_utc_iso(),
                        "engine_source": call.source,
                        "count": len(items),
                        "items": out_obj if fmt != "dict" else None,
                        "dict": out_obj if fmt == "dict" else None,
                    },
                )

            # flat only (no wrapper)
            return BestJSONResponse(status_code=200, content=out_obj)

        except Exception as e:
            router_requests_total.labels(endpoint="quotes", status="error").inc()
            logger.exception("Error processing /v1/enriched/quotes")
            return _build_error_response(
                request_id=request_id,
                error=str(e),
                debug=dbg,
                trace=traceback.format_exc() if dbg else None,
            )


# =============================================================================
# Mount helpers (optional)
# =============================================================================
def get_router() -> APIRouter:
    return router


def mount(app: Any) -> None:
    try:
        if app is not None and hasattr(app, "include_router"):
            app.include_router(router)
    except Exception:
        pass


__all__ = [
    "router",
    "get_router",
    "mount",
    "__version__",
]
