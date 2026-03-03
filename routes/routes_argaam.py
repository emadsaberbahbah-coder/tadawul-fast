#!/usr/bin/env python3
# routes/routes_argaam.py
"""
KSA Routes (Argaam + Engine Fallback) — v2.0.0 (PHASE 3/4 ALIGNED / AUTH-SAFE / SCHEMA-FIRST)
---------------------------------------------------------------------------------------------

What this revision fixes / improves:
- ✅ AUTH aligned with core.config (single source of truth). Secure fallback if core.config missing.
- ✅ Consistent output contract:
    - Always returns: status, request_id, timestamp_utc, timestamp_riyadh, symbol(s)
    - Returns: data (normalized object) and optional raw (provider/engine original)
- ✅ Schema alignment:
    - Uses schema_registry.get_sheet_spec("Market_Leaders") as KSA quote schema default (stable keys)
    - Normalizes payload to schema keys so Apps Script sees consistent columns
- ✅ Provider and engine are both optional:
    - Provider preferred, engine fallback
    - Better "provider available but unusable" diagnostics
- ✅ Supports BOTH layouts:
    - providers.argaam_provider and core.providers.argaam_provider
    - symbols.normalize and core.symbols.normalize
- ✅ Batch endpoint concurrency + stable ordering (preserves input symbols order)
- ✅ Startup-safe: no network I/O at import time

Mounting:
- This router has prefix="/v1/ksa" by default.
- If you already mount under /v1/ksa in main.py, set ROUTER_PREFIX="" or edit the APIRouter prefix below.

Endpoints:
- GET  /v1/ksa/ping
- GET  /v1/ksa/health
- GET  /v1/ksa/quote/{symbol}
- GET  /v1/ksa/quote?symbol=...
- POST /v1/ksa/quotes

"""

from __future__ import annotations

import asyncio
import importlib
import inspect
import logging
import math
import os
import time
import uuid
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Sequence, Set, Tuple, Union

from fastapi import APIRouter, Body, Header, HTTPException, Query, Request, status

# -----------------------------------------------------------------------------
# Logging
# -----------------------------------------------------------------------------
log = logging.getLogger("routes.routes_argaam")

ROUTE_VERSION = "2.0.0"
ROUTER_PREFIX = "/v1/ksa"  # change to "" ONLY if you mount under /v1/ksa externally

router = APIRouter(prefix=ROUTER_PREFIX, tags=["KSA"])
api_router = router
__all__ = ["router", "api_router"]

RIYADH_TZ = timezone(timedelta(hours=3))
_TRUTHY = {"1", "true", "yes", "y", "on", "t", "enabled", "active"}


# -----------------------------------------------------------------------------
# JSON safety
# -----------------------------------------------------------------------------
def clean_nans(obj: Any) -> Any:
    """Recursively replaces NaN/Inf with None to prevent JSON serialization crashes."""
    try:
        if isinstance(obj, dict):
            return {k: clean_nans(v) for k, v in obj.items()}
        if isinstance(obj, list):
            return [clean_nans(v) for v in obj]
        if isinstance(obj, float) and (math.isnan(obj) or math.isinf(obj)):
            return None
    except Exception:
        pass
    return obj


def _now_utc_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _now_riyadh_iso() -> str:
    return datetime.now(RIYADH_TZ).isoformat()


def _request_id(request: Request) -> str:
    try:
        rid = getattr(request.state, "request_id", None)
        if rid:
            return str(rid)
    except Exception:
        pass
    return request.headers.get("X-Request-ID") or str(uuid.uuid4())[:12]


def _is_truthy(v: Any) -> bool:
    return str(v or "").strip().lower() in _TRUTHY


# -----------------------------------------------------------------------------
# Auth (core.config first)
# -----------------------------------------------------------------------------
def _extract_auth_token(token_q: Optional[str], x_app_token: Optional[str], authorization: Optional[str]) -> str:
    if authorization:
        a = authorization.strip()
        if a.lower().startswith("bearer "):
            return a.split(" ", 1)[1].strip()
        return a
    if x_app_token and x_app_token.strip():
        return x_app_token.strip()
    if token_q and token_q.strip():
        return token_q.strip()
    return ""


def _auth_ok(
    request: Request,
    *,
    token_q: Optional[str],
    x_app_token: Optional[str],
    authorization: Optional[str],
) -> bool:
    """
    Single source of truth: core.config.auth_ok + core.config.is_open_mode.
    Secure fallback if core.config isn't available.
    """
    try:
        from core.config import is_open_mode, auth_ok  # type: ignore

        if callable(is_open_mode) and is_open_mode():
            return True

        tok = _extract_auth_token(token_q, x_app_token, authorization)
        if callable(auth_ok):
            return bool(auth_ok(token=tok, authorization=authorization, headers=dict(request.headers)))
        return False
    except Exception:
        # secure fallback: require auth unless REQUIRE_AUTH is false
        require = (os.getenv("REQUIRE_AUTH", "true") or "true").strip().lower() in _TRUTHY
        if not require:
            return True
        tok = _extract_auth_token(token_q, x_app_token, authorization)
        return bool(tok)


# -----------------------------------------------------------------------------
# Models (minimal, no pydantic dependency required)
# -----------------------------------------------------------------------------
def _split_symbols(raw: Any) -> List[str]:
    if raw is None:
        return []
    if isinstance(raw, list):
        out: List[str] = []
        for x in raw:
            out.extend(_split_symbols(x))
        return out
    s = str(raw).replace("\n", " ").replace("\t", " ").replace(",", " ").strip()
    parts = [p.strip() for p in s.split(" ") if p.strip()]
    # dedup preserve order
    seen: Set[str] = set()
    out2: List[str] = []
    for p in parts:
        u = p.upper()
        if u and u not in seen:
            seen.add(u)
            out2.append(u)
    return out2


# -----------------------------------------------------------------------------
# Helpers: symbol normalize + awaitable wrapper
# -----------------------------------------------------------------------------
def _normalize_symbol(symbol: str) -> str:
    s = (symbol or "").strip()
    if not s:
        return ""

    # Try preferred path first
    for mod_path in ("symbols.normalize", "core.symbols.normalize"):
        try:
            mod = importlib.import_module(mod_path)
            fn = getattr(mod, "normalize_symbol", None)
            if callable(fn):
                out = fn(s)
                return str(out).strip()
        except Exception:
            continue

    return s


async def _call_maybe_async(fn, *args, **kwargs):
    out = fn(*args, **kwargs)
    if inspect.isawaitable(out):
        return await out
    return out


def _as_payload(obj: Any) -> Dict[str, Any]:
    """Normalize provider/engine outputs into a JSON-friendly dict."""
    if obj is None:
        return {}
    if isinstance(obj, dict):
        return obj

    md = getattr(obj, "model_dump", None)  # pydantic v2
    if callable(md):
        try:
            return md(mode="python")
        except Exception:
            try:
                return md()
            except Exception:
                pass

    d = getattr(obj, "dict", None)  # pydantic v1
    if callable(d):
        try:
            return d()
        except Exception:
            pass

    try:
        return dict(getattr(obj, "__dict__", {})) or {"result": str(obj)}
    except Exception:
        return {"result": str(obj)}


# -----------------------------------------------------------------------------
# Schema alignment (KSA quote => Market_Leaders schema by default)
# -----------------------------------------------------------------------------
def _get_default_schema_keys() -> List[str]:
    """
    For KSA quotes, we map to the Market_Leaders schema (it contains the common instrument fields).
    If schema_registry not present, return a safe minimal key set.
    """
    try:
        from core.sheets.schema_registry import get_sheet_spec  # type: ignore

        spec = get_sheet_spec("Market_Leaders")
        return [c.key for c in spec.columns]
    except Exception:
        return [
            "symbol",
            "name",
            "exchange",
            "currency",
            "asset_class",
            "current_price",
            "previous_close",
            "day_high",
            "day_low",
            "week_52_high",
            "week_52_low",
            "volume",
            "market_cap",
            "rsi_14",
            "volatility_30d",
            "forecast_price_1m",
            "forecast_price_3m",
            "forecast_price_12m",
            "expected_roi_1m",
            "expected_roi_3m",
            "expected_roi_12m",
            "risk_score",
            "overall_score",
            "confidence_score",
            "recommendation",
            "data_provider",
            "data_quality",
            "last_updated_utc",
            "last_updated_riyadh",
            "warnings",
            "error",
        ]


_CANONICAL_ALIASES: Dict[str, Tuple[str, ...]] = {
    "symbol": ("symbol", "ticker", "code"),
    "name": ("name", "company_name", "long_name"),
    "exchange": ("exchange", "market", "primary_exchange"),
    "currency": ("currency", "ccy"),
    "asset_class": ("asset_class", "type", "instrument_type"),
    "current_price": ("current_price", "price", "last_price", "close"),
    "previous_close": ("previous_close", "prev_close", "prior_close"),
    "day_high": ("day_high", "high"),
    "day_low": ("day_low", "low"),
    "week_52_high": ("week_52_high", "high_52w", "52w_high"),
    "week_52_low": ("week_52_low", "low_52w", "52w_low"),
    "volume": ("volume",),
    "market_cap": ("market_cap",),
    "rsi_14": ("rsi_14", "rsi14", "rsi"),
    "volatility_30d": ("volatility_30d", "vol_30d"),
    "forecast_price_1m": ("forecast_price_1m",),
    "forecast_price_3m": ("forecast_price_3m",),
    "forecast_price_12m": ("forecast_price_12m",),
    "expected_roi_1m": ("expected_roi_1m",),
    "expected_roi_3m": ("expected_roi_3m",),
    "expected_roi_12m": ("expected_roi_12m",),
    "risk_score": ("risk_score",),
    "overall_score": ("overall_score",),
    "confidence_score": ("confidence_score",),
    "recommendation": ("recommendation",),
    "data_provider": ("data_provider", "provider", "source"),
    "data_quality": ("data_quality",),
    "last_updated_utc": ("last_updated_utc", "timestamp_utc"),
    "last_updated_riyadh": ("last_updated_riyadh",),
    "warnings": ("warnings", "warning"),
    "error": ("error", "error_message"),
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
            for a in _CANONICAL_ALIASES.get(k, ()):
                if a in raw:
                    v = raw.get(a)
                    break
                if a.lower() in raw_lc:
                    v = raw_lc.get(a.lower())
                    break
        out[k] = v

    if "symbol" in out and not out.get("symbol"):
        out["symbol"] = symbol_fallback

    # timestamps defaults
    out.setdefault("last_updated_utc", _now_utc_iso())
    out.setdefault("last_updated_riyadh", _now_riyadh_iso())

    # provider tag default
    if "data_provider" in out and not out.get("data_provider"):
        out["data_provider"] = "argaam"

    return out


# -----------------------------------------------------------------------------
# Provider loading (supports providers.* and legacy core.providers.*)
# -----------------------------------------------------------------------------
def _import_first_available(module_candidates: List[str]) -> Tuple[Optional[Any], Optional[str], Optional[str]]:
    last_err = None
    for path in module_candidates:
        try:
            mod = importlib.import_module(path)
            return mod, None, path
        except Exception as e:
            last_err = f"{path}: {e!r}"
    return None, last_err or "no candidates", None


def _load_argaam_provider() -> Tuple[Optional[Any], Optional[str], Optional[str]]:
    """
    Returns: (provider_obj, error_str, module_path_used)
    """
    mod, err, used = _import_first_available(["providers.argaam_provider", "core.providers.argaam_provider"])
    if mod is None:
        return None, f"Unable to import Argaam provider: {err}", used

    # Factories/instances first
    for attr in ("get_provider", "build_provider", "provider", "PROVIDER"):
        try:
            obj = getattr(mod, attr, None)
            if callable(obj):
                prov = obj()
                if prov is not None:
                    return prov, None, getattr(mod, "__name__", used)
            elif obj is not None:
                return obj, None, getattr(mod, "__name__", used)
        except Exception:
            pass

    # Classes next
    last_err = None
    for cls_name in ("ArgaamProvider", "Provider", "KsaProvider"):
        cls = getattr(mod, cls_name, None)
        if cls is None:
            continue
        try:
            return cls(), None, getattr(mod, "__name__", used)
        except Exception as e:
            last_err = f"Found {cls_name} but could not instantiate: {e!r}"

    return None, (last_err or "Provider module found but no usable provider factory/class detected."), getattr(mod, "__name__", used)


async def _call_provider_best_effort(provider: Any, symbol: str) -> Dict[str, Any]:
    """
    Try multiple method names / signatures to maximize compatibility.
    """
    candidates = [
        ("get_quote", True),
        ("quote", True),
        ("fetch_quote", True),
        ("fetch", True),
        ("get", True),
        ("get_price", True),
        ("price", True),
        # positional fallbacks
        ("get_quote", False),
        ("quote", False),
        ("fetch_quote", False),
        ("fetch", False),
        ("get", False),
        ("get_price", False),
        ("price", False),
    ]

    last_err: Optional[Exception] = None
    for method_name, kw in candidates:
        fn = getattr(provider, method_name, None)
        if not callable(fn):
            continue
        try:
            out = await _call_maybe_async(fn, symbol=symbol) if kw else await _call_maybe_async(fn, symbol)
            payload = _as_payload(out)
            return payload if payload else {"result": out}
        except Exception as e:
            last_err = e

    raise RuntimeError(f"Provider loaded but no compatible quote method succeeded. last_err={last_err!r}")


# -----------------------------------------------------------------------------
# Engine fallback
# -----------------------------------------------------------------------------
def _get_engine_from_request(request: Request) -> Tuple[Optional[Any], bool]:
    """
    Returns (engine, engine_ready_flag).
    """
    try:
        engine = getattr(request.app.state, "engine", None)
        ready = bool(getattr(request.app.state, "engine_ready", False))
        return engine, ready
    except Exception:
        return None, False


async def _call_engine_best_effort(engine: Any, symbol: str) -> Dict[str, Any]:
    candidates = [
        ("get_enriched_quote", True),
        ("get_quote", True),
        ("quote", True),
        ("fetch_quote", True),
        ("fetch", True),
        ("get", True),
        # positional
        ("get_enriched_quote", False),
        ("get_quote", False),
        ("quote", False),
        ("fetch_quote", False),
        ("fetch", False),
        ("get", False),
    ]

    last_err: Optional[Exception] = None
    for method_name, kw in candidates:
        fn = getattr(engine, method_name, None)
        if not callable(fn):
            continue
        try:
            out = await _call_maybe_async(fn, symbol=symbol) if kw else await _call_maybe_async(fn, symbol)
            payload = _as_payload(out)
            return payload if payload else {"result": out}
        except Exception as e:
            last_err = e

    raise RuntimeError(f"Engine available but no compatible quote method succeeded. last_err={last_err!r}")


# -----------------------------------------------------------------------------
# Response builders
# -----------------------------------------------------------------------------
def _resp_ok(
    *,
    request: Request,
    data: Any,
    source: str,
    request_id: str,
    symbol: Optional[str] = None,
    symbols: Optional[List[str]] = None,
    raw: Optional[Any] = None,
    include_raw: bool = False,
    meta: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    payload: Dict[str, Any] = {
        "status": "success",
        "version": ROUTE_VERSION,
        "request_id": request_id,
        "timestamp_utc": _now_utc_iso(),
        "timestamp_riyadh": _now_riyadh_iso(),
        "source": source,
    }
    if symbol is not None:
        payload["symbol"] = symbol
    if symbols is not None:
        payload["symbols"] = symbols

    payload["data"] = data
    if include_raw:
        payload["raw"] = raw

    if meta:
        payload["meta"] = meta

    return clean_nans(payload)


def _resp_err(
    *,
    request_id: str,
    message: str,
    status_code: int = 200,
    symbol: Optional[str] = None,
    meta: Optional[Dict[str, Any]] = None,
) -> HTTPException:
    detail: Dict[str, Any] = {
        "status": "error",
        "error": message,
        "version": ROUTE_VERSION,
        "request_id": request_id,
        "timestamp_utc": _now_utc_iso(),
        "timestamp_riyadh": _now_riyadh_iso(),
    }
    if symbol is not None:
        detail["symbol"] = symbol
    if meta:
        detail["meta"] = meta
    raise HTTPException(status_code=status_code, detail=clean_nans(detail))


# -----------------------------------------------------------------------------
# Routes
# -----------------------------------------------------------------------------
@router.get("/ping")
async def ping() -> Dict[str, Any]:
    return {
        "status": "success",
        "router": "KSA",
        "module": "routes.routes_argaam",
        "version": ROUTE_VERSION,
        "timestamp_utc": _now_utc_iso(),
        "timestamp_riyadh": _now_riyadh_iso(),
    }


@router.get("/health")
async def health(
    request: Request,
    token: Optional[str] = Query(default=None, description="Auth token (query only if allowed)"),
    x_app_token: Optional[str] = Header(default=None, alias="X-APP-TOKEN"),
    authorization: Optional[str] = Header(default=None, alias="Authorization"),
) -> Dict[str, Any]:
    rid = _request_id(request)

    auth = _auth_ok(request, token_q=token, x_app_token=x_app_token, authorization=authorization)

    provider, perr, mod_used = _load_argaam_provider()
    engine, engine_ready = _get_engine_from_request(request)

    schema_keys = _get_default_schema_keys()

    return {
        "status": "success",
        "version": ROUTE_VERSION,
        "request_id": rid,
        "timestamp_utc": _now_utc_iso(),
        "timestamp_riyadh": _now_riyadh_iso(),
        "auth_ok": bool(auth),
        "provider": {
            "loaded": provider is not None,
            "module": mod_used,
            "error": perr,
            "type": type(provider).__name__ if provider else None,
        },
        "engine": {
            "present": engine is not None,
            "ready_flag": bool(engine_ready),
            "type": type(engine).__name__ if engine else None,
        },
        "schema": {
            "default_page": "Market_Leaders",
            "key_count": len(schema_keys),
        },
        "fallback_mode": (provider is None and engine is not None),
    }


@router.get("/quote/{symbol}")
async def quote_path(
    request: Request,
    symbol: str,
    include_raw: bool = Query(False),
    token: Optional[str] = Query(default=None, description="Auth token (query only if allowed)"),
    x_app_token: Optional[str] = Header(default=None, alias="X-APP-TOKEN"),
    authorization: Optional[str] = Header(default=None, alias="Authorization"),
) -> Dict[str, Any]:
    rid = _request_id(request)

    if not _auth_ok(request, token_q=token, x_app_token=x_app_token, authorization=authorization):
        _resp_err(request_id=rid, message="unauthorized", status_code=status.HTTP_401_UNAUTHORIZED, symbol=symbol)

    sym = _normalize_symbol(symbol)
    if not sym:
        _resp_err(request_id=rid, message="empty_symbol", status_code=400, symbol=symbol)

    provider, perr, pmod = _load_argaam_provider()
    engine, _engine_ready = _get_engine_from_request(request)

    raw_payload: Optional[Dict[str, Any]] = None
    source: Optional[str] = None
    last_error: Optional[str] = None

    # 1) Prefer provider if available
    if provider is not None:
        try:
            raw_payload = await _call_provider_best_effort(provider, sym)
            source = f"argaam_provider:{pmod or 'unknown'}"
        except Exception as e:
            last_error = f"provider_error:{type(e).__name__}:{e}"
            raw_payload = None

    # 2) Fallback to engine
    if raw_payload is None and engine is not None:
        try:
            raw_payload = await _call_engine_best_effort(engine, sym)
            source = "data_engine"
        except Exception as e:
            last_error = f"engine_error:{type(e).__name__}:{e}"
            raw_payload = None

    if raw_payload is None:
        _resp_err(
            request_id=rid,
            message="KSA quote unavailable",
            status_code=503,
            symbol=sym,
            meta={"provider_error": perr, "last_error": last_error, "provider_module": pmod},
        )

    # Schema normalize (stable keys)
    schema_keys = _get_default_schema_keys()
    normalized = _normalize_row_to_schema(schema_keys, raw_payload or {}, symbol_fallback=sym)
    # set provider tag
    normalized.setdefault("data_provider", "argaam")

    return _resp_ok(
        request=request,
        request_id=rid,
        symbol=sym,
        source=source or "unknown",
        data=normalized,
        raw=raw_payload,
        include_raw=bool(include_raw),
        meta={"schema_page": "Market_Leaders", "schema_keys": len(schema_keys)},
    )


@router.get("/quote")
async def quote_query(
    request: Request,
    symbol: str = Query(...),
    include_raw: bool = Query(False),
    token: Optional[str] = Query(default=None),
    x_app_token: Optional[str] = Header(default=None, alias="X-APP-TOKEN"),
    authorization: Optional[str] = Header(default=None, alias="Authorization"),
) -> Dict[str, Any]:
    return await quote_path(
        request=request,
        symbol=symbol,
        include_raw=include_raw,
        token=token,
        x_app_token=x_app_token,
        authorization=authorization,
    )


@router.post("/quotes")
async def quotes_batch(
    request: Request,
    body: Dict[str, Any] = Body(default_factory=dict),
    include_raw: Optional[bool] = Query(default=None, description="Overrides body.include_raw if provided"),
    token: Optional[str] = Query(default=None),
    x_app_token: Optional[str] = Header(default=None, alias="X-APP-TOKEN"),
    authorization: Optional[str] = Header(default=None, alias="Authorization"),
) -> Dict[str, Any]:
    """
    Body supported:
      { "symbols": ["1120.SR","2222.SR"], "include_raw": false }
      { "symbols": "1120.SR,2222.SR", "include_raw": true }

    Output preserves input order in results list.
    """
    rid = _request_id(request)

    if not _auth_ok(request, token_q=token, x_app_token=x_app_token, authorization=authorization):
        _resp_err(request_id=rid, message="unauthorized", status_code=status.HTTP_401_UNAUTHORIZED)

    symbols_in = body.get("symbols") or body.get("tickers") or body.get("symbol") or []
    sym_list = _split_symbols(symbols_in)

    if not sym_list:
        _resp_err(request_id=rid, message="empty_symbols_list", status_code=400)

    # include_raw precedence: query param > body > default False
    include_raw_effective = bool(include_raw) if include_raw is not None else bool(body.get("include_raw", False))

    provider, perr, pmod = _load_argaam_provider()
    engine, _engine_ready = _get_engine_from_request(request)

    if provider is None and engine is None:
        _resp_err(
            request_id=rid,
            message="No provider/engine available",
            status_code=503,
            meta={"provider_error": perr, "provider_module": pmod},
        )

    # concurrency (safe)
    max_conc = int(os.getenv("KSA_QUOTES_CONCURRENCY", "12") or "12")
    max_conc = max(1, min(50, max_conc))
    sem = asyncio.Semaphore(max_conc)

    schema_keys = _get_default_schema_keys()

    async def _one(sym_raw: str) -> Tuple[str, Optional[Dict[str, Any]], Optional[Dict[str, Any]], str, Optional[str]]:
        """
        Returns: (symbol, normalized_data_or_none, raw_or_none, source, error_or_none)
        """
        sym = _normalize_symbol(sym_raw)
        if not sym:
            return sym_raw, None, None, "none", "empty_symbol"

        async with sem:
            raw_payload: Optional[Dict[str, Any]] = None
            source: str = "unknown"
            last_error: Optional[str] = None

            if provider is not None:
                try:
                    raw_payload = await _call_provider_best_effort(provider, sym)
                    source = f"argaam_provider:{pmod or 'unknown'}"
                except Exception as e:
                    last_error = f"provider_error:{type(e).__name__}:{e}"
                    raw_payload = None

            if raw_payload is None and engine is not None:
                try:
                    raw_payload = await _call_engine_best_effort(engine, sym)
                    source = "data_engine"
                except Exception as e:
                    last_error = f"engine_error:{type(e).__name__}:{e}"
                    raw_payload = None

            if raw_payload is None:
                return sym, None, None, source, last_error or perr or "no_data"

            normalized = _normalize_row_to_schema(schema_keys, raw_payload, symbol_fallback=sym)
            normalized.setdefault("data_provider", "argaam")
            return sym, normalized, raw_payload, source, None

    results = await asyncio.gather(*[_one(s) for s in sym_list], return_exceptions=False)

    out_results: List[Dict[str, Any]] = []
    errors: List[Dict[str, Any]] = []

    ok_count = 0
    for sym, normalized, raw_payload, source, err in results:
        if err or normalized is None:
            errors.append({"symbol": sym, "error": err or "no_data", "source": source})
            continue

        row: Dict[str, Any] = {"symbol": sym, "source": source, "data": normalized}
        if include_raw_effective:
            row["raw"] = raw_payload
        out_results.append(row)
        ok_count += 1

    status_out = "success" if ok_count == len(sym_list) else ("partial" if ok_count > 0 else "error")

    return _resp_ok(
        request=request,
        request_id=rid,
        symbols=[_normalize_symbol(s) for s in sym_list],
        source="argaam+engine_fallback",
        data={
            "status": status_out,
            "count": len(out_results),
            "results": out_results,
            "errors": errors,
        },
        raw=None,
        include_raw=False,  # raw per-item only
        meta={
            "provider_module": pmod,
            "provider_error": perr,
            "schema_page": "Market_Leaders",
            "schema_keys": len(schema_keys),
            "concurrency": max_conc,
        },
    )
