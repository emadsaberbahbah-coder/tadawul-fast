# routes/routes_argaam.py
"""
KSA / Argaam Routes — v1.2.0
------------------------------------------------------------
Fixes the Render boot warning:
  "Router 'KSA' not mounted: Module found, but no APIRouter instance detected"

Key design goals:
- Always exposes a FastAPI APIRouter instance at module scope (`router`)
- Never breaks startup (all provider imports are lazy + guarded)
- No hidden network calls at import time (only when endpoints are hit)
- Works even if Argaam provider interface changes (best-effort method probing)

Mounted by main.py under a prefix (example): /v1/ksa  (your mounter decides).
So endpoints below are RELATIVE, e.g. GET /v1/ksa/ping
"""

from __future__ import annotations

import importlib
import logging
from typing import Any, Dict, List, Optional, Tuple

from fastapi import APIRouter, Body, HTTPException, Query
from pydantic import BaseModel, Field

log = logging.getLogger(__name__)

# ✅ IMPORTANT: module-scope APIRouter so main.mount_routers_once() can detect it
router = APIRouter(tags=["KSA"])

# Optional alias in case your loader expects a different common name
api_router = router

__all__ = ["router", "api_router"]


# ----------------------------
# Models
# ----------------------------
class QuotesRequest(BaseModel):
    symbols: List[str] = Field(..., min_items=1, description="List of symbols (e.g., 2222.SR, 1120.SR)")
    include_raw: bool = Field(False, description="If true, returns provider raw payload when available")


# ----------------------------
# Helpers
# ----------------------------
def _normalize_symbol(symbol: str) -> str:
    s = (symbol or "").strip()
    # Keep it conservative: don’t change user symbols aggressively.
    # If you have a shared normalize util in symbols/normalize.py, we’ll try it.
    try:
        mod = importlib.import_module("symbols.normalize")
        fn = getattr(mod, "normalize_symbol", None)
        if callable(fn):
            return str(fn(s))
    except Exception:
        pass
    return s


def _load_argaam_provider() -> Tuple[Optional[Any], Optional[str]]:
    """
    Lazy-load Argaam provider.
    Returns (provider_instance, error_message).
    Never raises to caller.
    """
    try:
        mod = importlib.import_module("providers.argaam_provider")
    except Exception as e:
        return None, f"Unable to import providers.argaam_provider: {e!r}"

    # Common patterns (try in order)
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
            # try next pattern
            pass

    # Class-based pattern
    for cls_name in ("ArgaamProvider", "Provider", "KsaProvider"):
        cls = getattr(mod, cls_name, None)
        if cls is None:
            continue
        try:
            return cls(), None
        except Exception as e:
            # Constructor may require args; we'll keep trying other patterns.
            last_err = f"Found {cls_name} but could not instantiate: {e!r}"
            continue

    return None, "Argaam provider module found but no usable provider factory/class was detected."


def _call_provider_best_effort(provider: Any, symbol: str) -> Dict[str, Any]:
    """
    Calls the provider using best-effort method probing.
    This avoids tightly coupling routes to a single provider interface.
    """
    candidates = [
        ("get_quote", {"symbol": symbol}),
        ("quote", {"symbol": symbol}),
        ("fetch_quote", {"symbol": symbol}),
        ("get_price", {"symbol": symbol}),
        ("price", {"symbol": symbol}),
        ("get", {"symbol": symbol}),
        ("fetch", {"symbol": symbol}),
    ]

    for method_name, kwargs in candidates:
        fn = getattr(provider, method_name, None)
        if callable(fn):
            try:
                out = fn(**kwargs)
                # Normalize output to dict
                if isinstance(out, dict):
                    return out
                return {"result": out}
            except Exception as e:
                # try next method
                last_err = e
                continue

    raise RuntimeError("Provider loaded but no compatible quote method succeeded.")


# ----------------------------
# Routes
# ----------------------------
@router.get("/ping")
def ping() -> Dict[str, Any]:
    return {
        "ok": True,
        "router": "KSA",
        "module": "routes.routes_argaam",
        "hint": "If mounted under /v1/ksa then this is /v1/ksa/ping",
    }


@router.get("/health")
def health() -> Dict[str, Any]:
    provider, err = _load_argaam_provider()
    return {
        "ok": True,
        "router": "KSA",
        "provider_loaded": provider is not None,
        "provider_error": err,
    }


@router.get("/quote/{symbol}")
def quote_path(
    symbol: str,
    include_raw: bool = Query(False, description="If true, return raw provider output when available"),
) -> Dict[str, Any]:
    sym = _normalize_symbol(symbol)
    provider, err = _load_argaam_provider()
    if provider is None:
        raise HTTPException(status_code=503, detail=f"Argaam/KSA provider not available: {err}")

    try:
        payload = _call_provider_best_effort(provider, sym)
    except Exception as e:
        log.exception("KSA quote failed for %s", sym)
        raise HTTPException(status_code=502, detail=f"KSA provider call failed: {e!r}")

    # Keep response stable for Sheets clients
    resp: Dict[str, Any] = {"ok": True, "symbol": sym}
    if include_raw:
        resp["raw"] = payload
    else:
        # If provider returns a nested structure, keep it under "data"
        resp["data"] = payload
    return resp


@router.get("/quote")
def quote_query(
    symbol: str = Query(..., description="Symbol, e.g. 2222.SR"),
    include_raw: bool = Query(False),
) -> Dict[str, Any]:
    # Convenience wrapper: /quote?symbol=2222.SR
    return quote_path(symbol=symbol, include_raw=include_raw)


@router.post("/quotes")
def quotes_batch(req: QuotesRequest = Body(...)) -> Dict[str, Any]:
    provider, err = _load_argaam_provider()
    if provider is None:
        raise HTTPException(status_code=503, detail=f"Argaam/KSA provider not available: {err}")

    out: List[Dict[str, Any]] = []
    errors: List[Dict[str, Any]] = []

    for s in req.symbols:
        sym = _normalize_symbol(s)
        try:
            payload = _call_provider_best_effort(provider, sym)
            if req.include_raw:
                out.append({"symbol": sym, "raw": payload})
            else:
                out.append({"symbol": sym, "data": payload})
        except Exception as e:
            errors.append({"symbol": sym, "error": repr(e)})

    return {"ok": True, "count": len(out), "results": out, "errors": errors}
