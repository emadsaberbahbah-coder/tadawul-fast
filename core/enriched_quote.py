#!/usr/bin/env python3
# routes/enriched_quote.py
"""
================================================================================
Enriched Quote Router — v5.6.1 (SCHEMA-ALIGNED + /quotes COMPAT + RISK FALLBACK)
================================================================================

What this revision fixes (based on your PowerShell logs)
1) ✅ Adds a first-class PUBLIC /quotes endpoint (compat with your dashboard runner)
   - Accepts page/sheet/sheet_name/tab/name + symbols/tickers
   - Returns {headers, keys, rows, rows_matrix, meta} schema-projected

2) ✅ Guarantees schema keys exist on every row (prevents missing-property drops)
   - If a key is in the schema, it will exist in the row dict (None if unknown)

3) ✅ Optional Yahoo fallback to fill missing PHASE-D risk fields
   - volatility_90d, max_drawdown_1y, var_95_1d, sharpe_1y
   - Only runs when those fields are missing/None
   - Controlled by env ENRICHED_YAHOO_FALLBACK_ENABLED (default: true)

Design
- Engine is lazy: request.app.state.engine OR core.data_engine_v2.get_engine()
- Output is schema-aligned via normalize_row_to_schema(schema, row)
- No startup network I/O
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
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Awaitable, Callable, Dict, List, Optional, Sequence, Set, Tuple, Union

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
        # Fallback: no schema padding here. We'll pad manually later when schema is known.
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

# Optional page catalog normalization
try:
    from core.sheets.page_catalog import normalize_page_name as _normalize_page_name  # type: ignore

    _HAS_PAGE_CATALOG = True
except Exception:
    _HAS_PAGE_CATALOG = False

    def _normalize_page_name(x: str) -> str:  # type: ignore
        return (x or "").strip()

# ---------------------------------------------------------------------------
# Optional Yahoo fallback for missing risk fields (PHASE D)
# ---------------------------------------------------------------------------
try:
    # This is your updated provider file.
    from core.providers.yahoo_chart_provider import fetch_enriched_quote_patch as _yahoo_patch  # type: ignore

    _HAS_YAHOO_FALLBACK = True
except Exception:
    _HAS_YAHOO_FALLBACK = False
    _yahoo_patch = None  # type: ignore

__version__ = "5.6.1"
ROUTER_VERSION = __version__

logger = logging.getLogger("routes.enriched_quote")

# v1 router
router = APIRouter(prefix="/v1/enriched", tags=["enriched"])
# public compatibility router (only mounted if paths are free)
public_router = APIRouter(tags=["enriched"])

_TRUTHY = {"1", "true", "yes", "y", "on", "t", "enabled", "active"}
_RECOMMENDATIONS = ("STRONG_BUY", "BUY", "HOLD", "REDUCE", "SELL", "STRONG_SELL", "UNDER_REVIEW")
RIYADH_TZ = timezone(timedelta(hours=3))

# Missing-risk fields to fill if absent
_RISK_KEYS_PHASE_D = ("volatility_90d", "max_drawdown_1y", "var_95_1d", "sharpe_1y")


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
    """Convert engine return object -> plain dict (safe)."""
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


# =============================================================================
# Schema helpers
# =============================================================================
def _norm_sheet_name(name: str) -> str:
    s = (name or "").strip()
    if not s:
        return ""
    try:
        if _HAS_PAGE_CATALOG:
            return _normalize_page_name(s)
    except Exception:
        pass
    return s


def _get_sheet_spec(sheet: str) -> Optional[Any]:
    if not sheet:
        return None
    s = _norm_sheet_name(sheet)
    reg = SCHEMA_REGISTRY
    if not isinstance(reg, dict) or not reg:
        return None
    if s in reg:
        return reg.get(s)
    # case-insensitive / alias attempts
    s2 = s.replace(" ", "_").strip()
    if s2 in reg:
        return reg.get(s2)
    for k, v in reg.items():
        if str(k).strip().lower() == s.strip().lower():
            return v
        if str(k).strip().lower() == s2.strip().lower():
            return v
    return None


def _extract_headers_keys(spec: Any) -> Tuple[List[str], List[str]]:
    """
    Supports:
    - spec.columns: list[Column] with .header/.key or dicts with ["header","key"]
    - dict {"columns":[...]}
    - dict {"headers":[...], "keys":[...]}
    """
    headers: List[str] = []
    keys: List[str] = []

    if spec is None:
        return headers, keys

    # dict with columns
    if isinstance(spec, dict):
        cols = spec.get("columns")
        if isinstance(cols, list):
            for c in cols:
                if isinstance(c, dict):
                    h = _safe_str(c.get("header"))
                    k = _safe_str(c.get("key"))
                else:
                    h = _safe_str(getattr(c, "header", None))
                    k = _safe_str(getattr(c, "key", None))
                if k:
                    keys.append(k)
                    headers.append(h or k)
            return headers, keys

        hs = spec.get("headers")
        ks = spec.get("keys")
        if isinstance(hs, list) and isinstance(ks, list) and len(hs) == len(ks) and len(ks) > 0:
            return [str(x) for x in hs], [str(x) for x in ks]

    # object with columns
    cols = getattr(spec, "columns", None)
    if isinstance(cols, list):
        for c in cols:
            if isinstance(c, dict):
                h = _safe_str(c.get("header"))
                k = _safe_str(c.get("key"))
            else:
                h = _safe_str(getattr(c, "header", None))
                k = _safe_str(getattr(c, "key", None))
            if k:
                keys.append(k)
                headers.append(h or k)

    return headers, keys


def _ensure_keys_exist(row: Dict[str, Any], keys: Sequence[str]) -> Dict[str, Any]:
    """
    Ensure every key exists in row dict (even if None).
    This prevents missing-property drops in clients (PowerShell, Apps Script, etc.).
    """
    out = dict(row or {})
    for k in keys:
        if not k:
            continue
        if k not in out:
            out[k] = None
    return out


def _project_row(row: Dict[str, Any], keys: Sequence[str]) -> Dict[str, Any]:
    out: Dict[str, Any] = {}
    for k in keys:
        if not k:
            continue
        out[k] = row.get(k)
    return out


def _rows_to_matrix(rows: List[Dict[str, Any]], keys: Sequence[str]) -> List[List[Any]]:
    mat: List[List[Any]] = []
    for r in rows:
        mat.append([r.get(k) for k in keys])
    return mat


# =============================================================================
# Optional Yahoo fallback for missing PHASE-D risk fields
# =============================================================================
def _yahoo_fallback_enabled() -> bool:
    raw = (os.getenv("ENRICHED_YAHOO_FALLBACK_ENABLED", "1") or "").strip().lower()
    if raw in {"0", "false", "no", "n", "off", "disabled"}:
        return False
    return True


def _yahoo_fallback_timeout_sec() -> float:
    return max(3.0, min(45.0, float(_safe_float(os.getenv("ENRICHED_YAHOO_FALLBACK_TIMEOUT_SEC", "15"), 15.0) or 15.0)))


def _yahoo_fallback_concurrency() -> int:
    return max(1, min(20, int(_safe_int(os.getenv("ENRICHED_YAHOO_FALLBACK_CONCURRENCY", "4"), 4) or 4)))


async def _maybe_fill_risk_fields(row: Dict[str, Any]) -> Dict[str, Any]:
    """
    If PHASE-D risk keys are missing/None, try Yahoo provider patch and merge.
    This is best-effort and non-fatal.
    """
    if not _HAS_YAHOO_FALLBACK or _yahoo_patch is None:
        return row
    if not _yahoo_fallback_enabled():
        return row

    # If all are present and not None, skip
    missing = [k for k in _RISK_KEYS_PHASE_D if row.get(k) is None]
    if not missing:
        return row

    sym = _safe_str(row.get("symbol") or row.get("symbol_normalized") or row.get("requested_symbol")).strip()
    if not sym:
        return row

    try:
        patch = await asyncio.wait_for(_yahoo_patch(sym, debug=False), timeout=_yahoo_fallback_timeout_sec())  # type: ignore[misc]
        if isinstance(patch, dict) and not patch.get("error"):
            merged = dict(row)
            # bring over only missing risk keys (plus helpful related keys if absent)
            for k in _RISK_KEYS_PHASE_D:
                if merged.get(k) is None and patch.get(k) is not None:
                    merged[k] = patch.get(k)
            # also align volatility_30d if missing
            if merged.get("volatility_30d") is None and patch.get("volatility_30d") is not None:
                merged["volatility_30d"] = patch.get("volatility_30d")
            # helpful provenance
            if patch.get("provider"):
                merged.setdefault("data_provider", patch.get("provider"))
            return merged
    except Exception:
        return row

    return row


# =============================================================================
# Finalize row (schema + derived fields)
# =============================================================================
def _finalize_row(raw_symbol: str, row: Dict[str, Any], *, engine_source: str = "", schema: Any = None) -> Dict[str, Any]:
    """
    Ensure canonical keys exist + derived values are filled if possible.
    - If schema is provided, pad keys to schema.
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

    # Normalize/pad to schema if available
    try:
        row = _normalize_row_to_schema(schema, row)
    except Exception:
        pass

    # If schema is provided, ensure all schema keys exist
    if schema is not None:
        _, keys = _extract_headers_keys(schema)
        if keys:
            row = _ensure_keys_exist(row, keys)

    return row


# =============================================================================
# Engine calling (lazy + signature-safe kwargs)
# =============================================================================
@dataclass
class EngineCall:
    result: Optional[Any] = None
    source: str = ""
    error: Optional[str] = None
    latency_ms: float = 0.0


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
    """Returns (engine, source, error). Caches engine in request.app.state.engine."""
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

    try:
        from core.config import auth_ok  # type: ignore

        if callable(auth_ok):
            return bool(auth_ok(token=token, authorization=authz, headers=dict(request.headers)))
    except Exception:
        pass

    require_auth = str(os.getenv("REQUIRE_AUTH", "")).strip().lower() in _TRUTHY
    if require_auth:
        return False
    return True


# =============================================================================
# Error response
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


# =============================================================================
# v1: headers + health
# =============================================================================
def _schema_union_keys() -> List[str]:
    keys: List[str] = []
    seen: Set[str] = set()
    if isinstance(SCHEMA_REGISTRY, dict):
        for _, spec in SCHEMA_REGISTRY.items():
            hdrs, ks = _extract_headers_keys(spec)
            for k in ks:
                if k and k not in seen:
                    seen.add(k)
                    keys.append(k)

    # minimal must-haves
    must = (
        "symbol",
        "current_price",
        "previous_close",
        "price_change",
        "percent_change",
        "data_quality",
        "last_updated_utc",
        "last_updated_riyadh",
        *_RISK_KEYS_PHASE_D,
    )
    for k in must:
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
        "yahoo_fallback_available": bool(_HAS_YAHOO_FALLBACK),
        "yahoo_fallback_enabled": bool(_yahoo_fallback_enabled()),
    }
    try:
        from core.config import config_health_check  # type: ignore

        if callable(config_health_check):
            info["config"] = config_health_check()
    except Exception:
        pass
    return info


# =============================================================================
# v1: /quote (single)
# =============================================================================
@router.get("/quote")
async def get_enriched_quote(
    request: Request,
    symbol: str = Query("", description="Ticker symbol (AAPL, 1120.SR, ^GSPC, GC=F)"),
    sheet: str = Query("", description="Optional sheet/page name to apply schema padding"),
    debug: bool = Query(False, description="Include debug information"),
    wrap: bool = Query(True, description="Keep wrapper fields (status/data/request_id)."),
):
    request_id = request.headers.get("X-Request-ID", str(uuid.uuid4())[:12])
    dbg = _is_truthy(os.getenv("DEBUG_ERRORS", "0")) or debug

    with tracer.start_as_current_span("router_enriched_quote") as span:
        span.set_attribute("symbol", symbol)

        if not _is_authorized(request):
            router_requests_total.labels(endpoint="v1_quote", status="unauthorized").inc()
            return _build_error_response(request_id=request_id, error="unauthorized", status_code=401)

        raw = (symbol or "").strip()
        if not raw:
            router_requests_total.labels(endpoint="v1_quote", status="empty_symbol").inc()
            return _build_error_response(request_id=request_id, error="empty_symbol", symbol=raw)

        schema = _get_sheet_spec(sheet) if sheet else None

        start_time = time.time()
        try:
            call = await _call_engine(request, raw, schema=schema)
            if call.error or call.result is None:
                router_requests_total.labels(endpoint="v1_quote", status="no_data").inc()
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
            row = _finalize_row(raw, payload, engine_source=call.source, schema=schema)
            row = await _maybe_fill_risk_fields(row)

            router_requests_total.labels(endpoint="v1_quote", status="success").inc()
            router_request_duration.labels(endpoint="v1_quote").observe(time.time() - start_time)

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

            return BestJSONResponse(status_code=200, content=row)

        except Exception as e:
            router_requests_total.labels(endpoint="v1_quote", status="error").inc()
            logger.exception("Error processing /v1/enriched/quote")
            return _build_error_response(
                request_id=request_id,
                error=str(e),
                symbol=raw,
                debug=dbg,
                trace=traceback.format_exc() if dbg else None,
            )


# =============================================================================
# v1: /quotes (batch)
# =============================================================================
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


@router.get("/quotes")
async def get_enriched_quotes(
    request: Request,
    symbols: str = Query("", description="Comma/space separated symbols"),
    tickers: str = Query("", description="Alias for symbols"),
    sheet: str = Query("", description="Optional sheet/page name to apply schema padding"),
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
        schema=_get_sheet_spec(sheet) if sheet else None,
    )


@router.post("/quotes")
async def post_enriched_quotes(
    request: Request,
    body: Dict[str, Any] = Body(default_factory=dict),
):
    symbols_in = body.get("symbols") or body.get("tickers") or body.get("symbol") or []
    fmt = str(body.get("format") or "items").strip().lower()
    debug = bool(body.get("debug") or False)
    wrap = bool(body.get("wrap") if body.get("wrap") is not None else True)

    sheet = _safe_str(body.get("sheet") or body.get("page") or body.get("sheet_name") or body.get("tab") or body.get("name"))
    schema = _get_sheet_spec(sheet) if sheet else None

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
        schema=schema,
    )


async def _quotes_impl(
    *,
    request: Request,
    symbols: List[str],
    fmt: str,
    debug: bool,
    wrap: bool,
    schema: Any = None,
):
    request_id = request.headers.get("X-Request-ID", str(uuid.uuid4())[:12])
    dbg = _is_truthy(os.getenv("DEBUG_ERRORS", "0")) or debug

    with tracer.start_as_current_span("router_enriched_quotes") as span:
        if not _is_authorized(request):
            router_requests_total.labels(endpoint="v1_quotes", status="unauthorized").inc()
            return _build_error_response(request_id=request_id, error="unauthorized", status_code=401)

        if not symbols:
            router_requests_total.labels(endpoint="v1_quotes", status="empty_symbols_list").inc()
            return _build_error_response(request_id=request_id, error="empty_symbols_list")

        span.set_attribute("symbol_count", len(symbols))
        start_time = time.time()

        try:
            call = await _call_engine_batch(request, symbols, schema=schema)

            rows_by_requested: Dict[str, Dict[str, Any]] = {}

            if call.result is not None and not call.error:
                if isinstance(call.result, dict):
                    for k, v in call.result.items():
                        payload = _as_payload(v)
                        raw = _safe_str(payload.get("requested_symbol") or k)
                        rows_by_requested[raw] = _finalize_row(raw, payload, engine_source=call.source, schema=schema)
                elif isinstance(call.result, list):
                    for raw, v in zip(symbols, call.result):
                        payload = _as_payload(v)
                        rows_by_requested[raw] = _finalize_row(raw, payload, engine_source=call.source, schema=schema)

            # Fill missing with per-symbol calls
            missing = [s for s in symbols if s not in rows_by_requested]
            if missing:
                cfg_conc = max(1, min(50, _safe_int(os.getenv("ENRICHED_CONCURRENCY", 10), 10) or 10))
                cfg_timeout = max(3.0, min(90.0, _safe_float(os.getenv("ENRICHED_TIMEOUT_SEC", 30.0), 30.0) or 30.0))
                sem = asyncio.Semaphore(cfg_conc)

                async def _fetch_one(raw: str) -> Dict[str, Any]:
                    async with sem:
                        try:
                            single = await asyncio.wait_for(_call_engine(request, raw, schema=schema), timeout=cfg_timeout)
                            if single.result is not None and not single.error:
                                payload = _as_payload(single.result)
                                return _finalize_row(raw, payload, engine_source=single.source, schema=schema)
                            return _finalize_row(raw, {"error": single.error or "no_data", "data_quality": "MISSING"}, engine_source=single.source, schema=schema)
                        except asyncio.TimeoutError:
                            return _finalize_row(raw, {"error": "timeout", "data_quality": "MISSING"}, engine_source="timeout", schema=schema)
                        except Exception as e:
                            return _finalize_row(raw, {"error": f"exception:{e.__class__.__name__}", "data_quality": "MISSING"}, engine_source="exception", schema=schema)

                fetched = await asyncio.gather(*[_fetch_one(s) for s in missing])
                for raw, row in zip(missing, fetched):
                    rows_by_requested[raw] = row

            # Optional Yahoo fallback for risk fields (batch-safe)
            if _HAS_YAHOO_FALLBACK and _yahoo_fallback_enabled():
                sem2 = asyncio.Semaphore(_yahoo_fallback_concurrency())

                async def _fill_one(raw: str) -> None:
                    async with sem2:
                        rows_by_requested[raw] = await _maybe_fill_risk_fields(rows_by_requested[raw])

                await asyncio.gather(*[_fill_one(s) for s in symbols if s in rows_by_requested])

            # Output shape
            items: List[Dict[str, Any]] = [rows_by_requested[s] for s in symbols if s in rows_by_requested]
            out_obj: Union[List[Dict[str, Any]], Dict[str, Dict[str, Any]]]
            if fmt == "dict":
                out_obj = {s: rows_by_requested.get(s) for s in symbols}
            else:
                out_obj = items

            router_requests_total.labels(endpoint="v1_quotes", status="success").inc()
            router_request_duration.labels(endpoint="v1_quotes").observe(time.time() - start_time)

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

            return BestJSONResponse(status_code=200, content=out_obj)

        except Exception as e:
            router_requests_total.labels(endpoint="v1_quotes", status="error").inc()
            logger.exception("Error processing /v1/enriched/quotes")
            return _build_error_response(
                request_id=request_id,
                error=str(e),
                debug=dbg,
                trace=traceback.format_exc() if dbg else None,
            )


# =============================================================================
# PUBLIC COMPAT: /quotes and /quote (schema-projected)
# =============================================================================
def _pick_sheet_from_body(body: Dict[str, Any]) -> str:
    return _safe_str(body.get("page") or body.get("sheet") or body.get("sheet_name") or body.get("tab") or body.get("name") or "")


@public_router.post("/quotes")
async def public_quotes(request: Request, body: Dict[str, Any] = Body(default_factory=dict)):
    """
    Compatibility endpoint for your dashboard runner.

    Accepts:
      {
        "page": "Top_10_Investments",
        "sheet_name": "...",
        "symbols": [...],
        "tickers": [...],
        "include_raw": true|false,
        "include_matrix": true|false
      }

    Returns:
      {
        status, page, headers, keys, rows, rows_matrix, request_id, version, meta
      }
    """
    request_id = request.headers.get("X-Request-ID", str(uuid.uuid4())[:12])
    t0 = time.time()

    if not _is_authorized(request):
        return BestJSONResponse(status_code=401, content={"status": "error", "error": "unauthorized", "request_id": request_id})

    sheet = _pick_sheet_from_body(body) or "Market_Leaders"
    sheet = _norm_sheet_name(sheet)

    symbols_in = body.get("symbols") or body.get("tickers") or body.get("symbol") or []
    include_raw = bool(body.get("include_raw") or False)
    include_matrix = bool(body.get("include_matrix") if body.get("include_matrix") is not None else True)

    raw_list: List[str] = []
    if isinstance(symbols_in, str):
        raw_list = _split_symbols(symbols_in)
    elif isinstance(symbols_in, list):
        for x in symbols_in:
            raw_list.extend(_split_symbols(str(x)))

    symbols = _dedup_symbols(raw_list)

    # schema for page
    schema = _get_sheet_spec(sheet)
    headers, keys = _extract_headers_keys(schema)

    # if schema missing, fall back to union keys (but keep response stable)
    if not keys:
        keys = _schema_union_keys()
        headers = [k for k in keys]

    # Fetch enriched rows via v1 engine path (dict rows)
    call = await _call_engine_batch(request, symbols, schema=schema)
    rows_by_requested: Dict[str, Dict[str, Any]] = {}

    if call.result is not None and not call.error:
        if isinstance(call.result, dict):
            for k, v in call.result.items():
                payload = _as_payload(v)
                raw = _safe_str(payload.get("requested_symbol") or k)
                rows_by_requested[raw] = _finalize_row(raw, payload, engine_source=call.source, schema=schema)
        elif isinstance(call.result, list):
            for raw, v in zip(symbols, call.result):
                payload = _as_payload(v)
                rows_by_requested[raw] = _finalize_row(raw, payload, engine_source=call.source, schema=schema)

    # Fill missing via single calls
    missing = [s for s in symbols if s not in rows_by_requested]
    if missing:
        sem = asyncio.Semaphore(max(1, min(20, _safe_int(os.getenv("ENRICHED_PUBLIC_CONCURRENCY", 6), 6) or 6)))

        async def _one(s: str) -> None:
            async with sem:
                single = await _call_engine(request, s, schema=schema)
                if single.result is not None and not single.error:
                    payload = _as_payload(single.result)
                    rows_by_requested[s] = _finalize_row(s, payload, engine_source=single.source, schema=schema)
                else:
                    rows_by_requested[s] = _finalize_row(s, {"error": single.error or "no_data"}, engine_source=single.source, schema=schema)

        await asyncio.gather(*[_one(s) for s in missing])

    # Optional Yahoo fallback for risk fields
    if _HAS_YAHOO_FALLBACK and _yahoo_fallback_enabled():
        sem2 = asyncio.Semaphore(_yahoo_fallback_concurrency())

        async def _fill(s: str) -> None:
            async with sem2:
                rows_by_requested[s] = await _maybe_fill_risk_fields(rows_by_requested[s])

        await asyncio.gather(*[_fill(s) for s in symbols if s in rows_by_requested])

    # Ensure schema keys exist + project rows to schema keys
    raw_rows: List[Dict[str, Any]] = []
    proj_rows: List[Dict[str, Any]] = []

    for s in symbols:
        r = rows_by_requested.get(s) or _finalize_row(s, {"error": "no_data"}, engine_source="missing", schema=schema)
        r = _ensure_keys_exist(r, keys)
        raw_rows.append(r)
        proj_rows.append(_project_row(r, keys))

    resp: Dict[str, Any] = {
        "status": "success",
        "page": sheet,
        "headers": headers,
        "keys": keys,
        "rows": proj_rows,
        "error": None,
        "version": "5.6.1",
        "request_id": request_id,
        "meta": {
            "duration_ms": (time.time() - t0) * 1000.0,
            "requested": len(symbols),
            "errors": sum(1 for r in proj_rows if r.get("error")),
            "mode": _safe_str(body.get("mode") or ""),
        },
    }

    # compatibility aliases
    resp["quotes"] = resp["rows"]
    resp["data"] = resp["rows"]

    # rows_matrix
    if include_matrix:
        resp["rows_matrix"] = _rows_to_matrix(proj_rows, keys)

    # raw rows (optional)
    if include_raw:
        resp["raw_rows"] = raw_rows

    return BestJSONResponse(status_code=200, content=resp)


@public_router.get("/quote")
async def public_quote(
    request: Request,
    symbol: str = Query("", description="Ticker symbol"),
    page: str = Query("", description="Optional page/sheet name for schema"),
):
    # Simple convenience wrapper using v1 engine path + schema padding
    if not symbol.strip():
        return BestJSONResponse(status_code=200, content={"status": "error", "error": "empty_symbol", "request_id": str(uuid.uuid4())[:12]})

    body = {"page": page or "Market_Leaders", "symbols": [symbol], "include_matrix": True, "include_raw": False}
    return await public_quotes(request, body)


# =============================================================================
# Mount helpers
# =============================================================================
def get_router() -> APIRouter:
    # return the v1 router by default
    return router


def _app_has_route(app: Any, path: str, method: str) -> bool:
    try:
        m = (method or "").upper()
        for r in getattr(app, "router", getattr(app, "routes", None)).routes:  # type: ignore
            try:
                if getattr(r, "path", None) == path and m in (getattr(r, "methods", None) or set()):
                    return True
            except Exception:
                continue
    except Exception:
        return False
    return False


def mount(app: Any) -> None:
    """
    Mount v1 router always.
    Mount public router only if /quotes and /quote are not already defined.
    """
    try:
        if app is None or not hasattr(app, "include_router"):
            return

        app.include_router(router)

        # avoid duplicate conflicts
        has_post_quotes = _app_has_route(app, "/quotes", "POST")
        has_get_quote = _app_has_route(app, "/quote", "GET")
        if not has_post_quotes or not has_get_quote:
            app.include_router(public_router)
    except Exception:
        # never crash startup due to mounting
        pass


__all__ = [
    "router",
    "public_router",
    "get_router",
    "mount",
    "__version__",
]
