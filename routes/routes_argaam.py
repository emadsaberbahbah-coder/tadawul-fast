#!/usr/bin/env python3
# routes/routes_argaam.py
"""
KSA Routes (Argaam + Engine Fallback) — v1.5.0
------------------------------------------------------------
Goal:
- If Argaam provider is missing/broken, KSA endpoints must still work via app.state.engine (DataEngineV4).
- Works with BOTH provider paths:
    - providers.argaam_provider   (your repo)
    - core.providers.argaam_provider (legacy)
- Works with BOTH symbol normalizers:
    - symbols.normalize
    - core.symbols.normalize
- Fixes: safer engine readiness logic, better payload unwrapping, stable response schema.

Endpoints (if mounted under /v1/ksa):
- GET  /v1/ksa/ping
- GET  /v1/ksa/health
- GET  /v1/ksa/quote/{symbol}
- GET  /v1/ksa/quote?symbol=...
- POST /v1/ksa/quotes
"""

from __future__ import annotations

import importlib
import inspect
import logging
from typing import Any, Dict, List, Optional, Tuple

from fastapi import APIRouter, Body, Query, Request
from fastapi import HTTPException
from pydantic import BaseModel, Field

log = logging.getLogger("routes.routes_argaam")

router = APIRouter(tags=["KSA"])
api_router = router
__all__ = ["router", "api_router"]

# --------------------------------------------------------------------------------------
# Models
# --------------------------------------------------------------------------------------


class QuotesRequest(BaseModel):
    symbols: List[str] = Field(..., min_items=1)
    include_raw: bool = Field(False)


# --------------------------------------------------------------------------------------
# Helpers: symbol normalize + awaitable wrapper
# --------------------------------------------------------------------------------------


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
    """
    Normalize provider/engine outputs into a JSON-friendly dict.

    Handles:
    - dict
    - Pydantic v1/v2 models
    - dataclasses / objects
    """
    if obj is None:
        return {}

    if isinstance(obj, dict):
        return obj

    # pydantic v2
    md = getattr(obj, "model_dump", None)
    if callable(md):
        try:
            return md()
        except Exception:
            pass

    # pydantic v1
    d = getattr(obj, "dict", None)
    if callable(d):
        try:
            return d()
        except Exception:
            pass

    # dataclass-ish / object
    try:
        return dict(getattr(obj, "__dict__", {})) or {"result": str(obj)}
    except Exception:
        return {"result": str(obj)}


# --------------------------------------------------------------------------------------
# Provider loading (supports providers.* and legacy core.providers.*)
# --------------------------------------------------------------------------------------


def _import_first_available(module_candidates: List[str]) -> Tuple[Optional[Any], Optional[str]]:
    last_err = None
    for path in module_candidates:
        try:
            return importlib.import_module(path), None
        except Exception as e:
            last_err = f"{path}: {e!r}"
    return None, last_err or "no candidates"


def _load_argaam_provider() -> Tuple[Optional[Any], Optional[str], Optional[str]]:
    """
    Returns: (provider_obj, error_str, module_path_used)
    """
    mod, err = _import_first_available(["providers.argaam_provider", "core.providers.argaam_provider"])
    if mod is None:
        return None, f"Unable to import Argaam provider: {err}", None

    # Factories/instances first
    for attr in ("get_provider", "build_provider", "provider", "PROVIDER"):
        try:
            obj = getattr(mod, attr, None)
            if callable(obj):
                prov = obj()
                if prov is not None:
                    return prov, None, mod.__name__
            elif obj is not None:
                return obj, None, mod.__name__
        except Exception:
            pass

    # Classes next
    last_err = None
    for cls_name in ("ArgaamProvider", "Provider", "KsaProvider"):
        cls = getattr(mod, cls_name, None)
        if cls is None:
            continue
        try:
            return cls(), None, mod.__name__
        except Exception as e:
            last_err = f"Found {cls_name} but could not instantiate: {e!r}"

    return None, (last_err or "Provider module found but no usable provider factory/class detected."), mod.__name__


async def _call_provider_best_effort(provider: Any, symbol: str) -> Dict[str, Any]:
    """
    Try multiple method names / signatures (symbol or positional) to maximize compatibility.
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

    last_err = None
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


# --------------------------------------------------------------------------------------
# Engine fallback
# --------------------------------------------------------------------------------------


def _get_engine_from_request(request: Request) -> Tuple[Optional[Any], bool]:
    """
    Returns (engine, engine_ready_flag).
    We still return engine even if not marked ready, but we surface readiness in /health.
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

    last_err = None
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


# --------------------------------------------------------------------------------------
# Routes
# --------------------------------------------------------------------------------------


@router.get("/ping")
async def ping() -> Dict[str, Any]:
    return {"ok": True, "router": "KSA", "module": "routes.routes_argaam", "version": "1.5.0"}


@router.get("/health")
async def health(request: Request) -> Dict[str, Any]:
    provider, err, mod_used = _load_argaam_provider()
    engine, engine_ready = _get_engine_from_request(request)
    return {
        "ok": True,
        "router": "KSA",
        "provider_loaded": provider is not None,
        "provider_module": mod_used,
        "provider_error": err,
        "engine_present": engine is not None,
        "engine_ready": engine_ready,
        "engine_type": type(engine).__name__ if engine else None,
        "fallback_mode": (provider is None and engine is not None),
    }


@router.get("/quote/{symbol}")
async def quote_path(
    request: Request,
    symbol: str,
    include_raw: bool = Query(False),
) -> Dict[str, Any]:
    sym = _normalize_symbol(symbol)
    if not sym:
        raise HTTPException(status_code=400, detail="empty_symbol")

    provider, perr, _ = _load_argaam_provider()
    engine, _engine_ready = _get_engine_from_request(request)

    payload: Optional[Dict[str, Any]] = None
    source: Optional[str] = None
    last_error: Optional[Exception] = None

    # 1) Prefer provider if available
    if provider is not None:
        try:
            payload = await _call_provider_best_effort(provider, sym)
            source = "argaam_provider"
        except Exception as e:
            last_error = e
            payload = None

    # 2) Fallback to engine
    if payload is None and engine is not None:
        try:
            payload = await _call_engine_best_effort(engine, sym)
            source = "data_engine"
        except Exception as e:
            last_error = e
            payload = None

    if payload is None:
        raise HTTPException(
            status_code=503,
            detail=f"KSA quote unavailable. provider_err={perr!r} last_error={last_error!r}",
        )

    # Stable response shape:
    # - include_raw=True => returns provider/engine payload in "raw"
    # - include_raw=False => returns payload in "data"
    resp: Dict[str, Any] = {"ok": True, "symbol": sym, "source": source}
    if include_raw:
        resp["raw"] = payload
    else:
        resp["data"] = payload
    return resp


@router.get("/quote")
async def quote_query(
    request: Request,
    symbol: str = Query(...),
    include_raw: bool = Query(False),
) -> Dict[str, Any]:
    return await quote_path(request=request, symbol=symbol, include_raw=include_raw)


@router.post("/quotes")
async def quotes_batch(request: Request, req: QuotesRequest = Body(...)) -> Dict[str, Any]:
    provider, perr, _ = _load_argaam_provider()
    engine, _engine_ready = _get_engine_from_request(request)

    if provider is None and engine is None:
        raise HTTPException(status_code=503, detail=f"No provider/engine available. provider_err={perr!r}")

    results: List[Dict[str, Any]] = []
    errors: List[Dict[str, Any]] = []

    for s in req.symbols:
        sym = _normalize_symbol(s)
        if not sym:
            errors.append({"symbol": s, "error": "empty_symbol"})
            continue

        payload: Optional[Dict[str, Any]] = None
        source: Optional[str] = None
        last_error: Optional[Exception] = None

        if provider is not None:
            try:
                payload = await _call_provider_best_effort(provider, sym)
                source = "argaam_provider"
            except Exception as e:
                last_error = e
                payload = None

        if payload is None and engine is not None:
            try:
                payload = await _call_engine_best_effort(engine, sym)
                source = "data_engine"
            except Exception as e:
                last_error = e
                payload = None

        if payload is None:
            errors.append({"symbol": sym, "error": repr(last_error) if last_error else f"provider_err={perr!r}"})
            continue

        row: Dict[str, Any] = {"symbol": sym, "source": source}
        if req.include_raw:
            row["raw"] = payload
        else:
            row["data"] = payload
        results.append(row)

    return {"ok": True, "count": len(results), "results": results, "errors": errors}
