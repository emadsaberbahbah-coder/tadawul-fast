# routes/routes_argaam.py
"""
KSA Routes (Argaam + Engine Fallback) — v1.3.0
------------------------------------------------------------
Fixes:
- KSA router mounted but provider missing -> 503
Solution:
- Try Argaam provider if available
- Otherwise fall back to app.state.engine (DataEngineV4) so KSA + Global still work

Endpoints (if mounted under /v1/ksa):
- GET  /v1/ksa/ping
- GET  /v1/ksa/health
- GET  /v1/ksa/quote/{symbol}
- GET  /v1/ksa/quote?symbol=...
- POST /v1/ksa/quotes
"""

from __future__ import annotations

import importlib
import logging
import inspect
from typing import Any, Dict, List, Optional, Tuple

from fastapi import APIRouter, Body, HTTPException, Query, Request
from pydantic import BaseModel, Field

log = logging.getLogger(__name__)

router = APIRouter(tags=["KSA"])
api_router = router
__all__ = ["router", "api_router"]


class QuotesRequest(BaseModel):
    symbols: List[str] = Field(..., min_items=1)
    include_raw: bool = Field(False)


def _normalize_symbol(symbol: str) -> str:
    s = (symbol or "").strip()
    try:
        mod = importlib.import_module("symbols.normalize")
        fn = getattr(mod, "normalize_symbol", None)
        if callable(fn):
            return str(fn(s))
    except Exception:
        pass
    return s


async def _call_maybe_async(fn, *args, **kwargs):
    out = fn(*args, **kwargs)
    if inspect.isawaitable(out):
        return await out
    return out


def _load_argaam_provider() -> Tuple[Optional[Any], Optional[str]]:
    try:
        mod = importlib.import_module("providers.argaam_provider")
    except Exception as e:
        return None, f"Unable to import providers.argaam_provider: {e!r}"

    for attr in ("get_provider", "build_provider", "provider"):
        try:
            obj = getattr(mod, attr, None)
            if callable(obj):
                prov = obj()
                if prov is not None:
                    return prov, None
            elif obj is not None:
                return obj, None
        except Exception:
            pass

    for cls_name in ("ArgaamProvider", "Provider", "KsaProvider"):
        cls = getattr(mod, cls_name, None)
        if cls is None:
            continue
        try:
            return cls(), None
        except Exception as e:
            last_err = f"Found {cls_name} but could not instantiate: {e!r}"
            continue

    return None, "Argaam provider module found but no usable provider factory/class was detected."


async def _call_provider_best_effort(provider: Any, symbol: str) -> Dict[str, Any]:
    candidates = [
        ("get_quote", {"symbol": symbol}),
        ("quote", {"symbol": symbol}),
        ("fetch_quote", {"symbol": symbol}),
        ("get_price", {"symbol": symbol}),
        ("price", {"symbol": symbol}),
        ("get", {"symbol": symbol}),
        ("fetch", {"symbol": symbol}),
    ]

    last_err = None
    for method_name, kwargs in candidates:
        fn = getattr(provider, method_name, None)
        if callable(fn):
            try:
                out = await _call_maybe_async(fn, **kwargs)
                if isinstance(out, dict):
                    return out
                return {"result": out}
            except Exception as e:
                last_err = e
                continue

    raise RuntimeError(f"Provider loaded but no compatible quote method succeeded. last_err={last_err!r}")


def _get_engine_from_request(request: Request) -> Optional[Any]:
    try:
        engine = getattr(request.app.state, "engine", None)
        ready = bool(getattr(request.app.state, "engine_ready", False))
        return engine if engine and ready else engine
    except Exception:
        return None


async def _call_engine_best_effort(engine: Any, symbol: str) -> Dict[str, Any]:
    candidates = [
        ("get_enriched_quote", {"symbol": symbol}),
        ("enriched_quote", {"symbol": symbol}),
        ("get_quote", {"symbol": symbol}),
        ("quote", {"symbol": symbol}),
        ("fetch_quote", {"symbol": symbol}),
        ("get", {"symbol": symbol}),
        ("fetch", {"symbol": symbol}),
    ]

    last_err = None
    for method_name, kwargs in candidates:
        fn = getattr(engine, method_name, None)
        if callable(fn):
            try:
                out = await _call_maybe_async(fn, **kwargs)
                if isinstance(out, dict):
                    return out
                return {"result": out}
            except Exception as e:
                last_err = e
                continue

    raise RuntimeError(f"Engine available but no compatible quote method succeeded. last_err={last_err!r}")


@router.get("/ping")
async def ping() -> Dict[str, Any]:
    return {"ok": True, "router": "KSA", "module": "routes.routes_argaam"}


@router.get("/health")
async def health(request: Request) -> Dict[str, Any]:
    provider, err = _load_argaam_provider()
    engine = _get_engine_from_request(request)
    return {
        "ok": True,
        "router": "KSA",
        "provider_loaded": provider is not None,
        "provider_error": err,
        "engine_present": engine is not None,
        "engine_ready": bool(getattr(request.app.state, "engine_ready", False)),
        "engine_type": type(engine).__name__ if engine else None,
        "fallback_mode": (provider is None and engine is not None),
    }


@router.get("/quote/{symbol}")
async def quote_path(request: Request, symbol: str, include_raw: bool = Query(False)) -> Dict[str, Any]:
    sym = _normalize_symbol(symbol)

    provider, perr = _load_argaam_provider()
    engine = _get_engine_from_request(request)

    payload = None
    source = None
    last_error = None

    # 1) Prefer provider if available
    if provider is not None:
        try:
            payload = await _call_provider_best_effort(provider, sym)
            source = "argaam_provider"
        except Exception as e:
            last_error = e

    # 2) Fallback to engine
    if payload is None and engine is not None:
        try:
            payload = await _call_engine_best_effort(engine, sym)
            source = "data_engine"
        except Exception as e:
            last_error = e

    if payload is None:
        raise HTTPException(
            status_code=503,
            detail=f"KSA quote unavailable. provider_err={perr!r} last_error={last_error!r}"
        )

    resp: Dict[str, Any] = {"ok": True, "symbol": sym, "source": source}
    if include_raw:
        resp["raw"] = payload
    else:
        resp["data"] = payload
    return resp


@router.get("/quote")
async def quote_query(request: Request, symbol: str = Query(...), include_raw: bool = Query(False)) -> Dict[str, Any]:
    return await quote_path(request=request, symbol=symbol, include_raw=include_raw)


@router.post("/quotes")
async def quotes_batch(request: Request, req: QuotesRequest = Body(...)) -> Dict[str, Any]:
    provider, perr = _load_argaam_provider()
    engine = _get_engine_from_request(request)

    results: List[Dict[str, Any]] = []
    errors: List[Dict[str, Any]] = []

    for s in req.symbols:
        sym = _normalize_symbol(s)
        try:
            payload = None
            source = None

            if provider is not None:
                try:
                    payload = await _call_provider_best_effort(provider, sym)
                    source = "argaam_provider"
                except Exception:
                    payload = None

            if payload is None and engine is not None:
                payload = await _call_engine_best_effort(engine, sym)
                source = "data_engine"

            if payload is None:
                raise RuntimeError(f"No provider/engine could serve {sym}")

            row = {"symbol": sym, "source": source}
            if req.include_raw:
                row["raw"] = payload
            else:
                row["data"] = payload
            results.append(row)

        except Exception as e:
            errors.append({"symbol": sym, "error": repr(e)})

    return {"ok": True, "count": len(results), "results": results, "errors": errors}
