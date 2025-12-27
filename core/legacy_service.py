# core/legacy_service.py  (FULL REPLACEMENT)
from __future__ import annotations

"""
core/legacy_service.py
------------------------------------------------------------
Compatibility shim (quiet + useful) — v1.5.0 (ENV + ENGINE SAFE)

Why this exists
- main.py mounts core.legacy_service.router
- Older versions sometimes had a router in:
    - legacy_service.py (repo root)
    - routes/legacy_service.py

This shim:
✅ QUIET by default (no warning spam)
✅ Uses app.state.engine (shared DataEngine) if present
✅ Defensive fallback: tries core.data_engine_v2.get_engine() singleton, then temp DataEngine
✅ Provides stable legacy endpoints:
   - GET  /v1/legacy/health
   - GET  /v1/legacy/quote?symbol=...
   - POST /v1/legacy/quotes   {"symbols":[...]}
✅ Optional: can try importing external legacy routers ONLY if enabled via env

Design goal
- Never crash app startup.
- Never throw 500 from these legacy endpoints (best-effort returns).
"""

import importlib
import inspect
import os
import traceback
from typing import Any, Dict, List, Optional, Tuple

from fastapi import APIRouter, Query, Request
from fastapi.encoders import jsonable_encoder
from pydantic import BaseModel
from starlette.responses import JSONResponse

_TRUTHY = {"1", "true", "yes", "y", "on", "t"}


def _env_bool(name: str, default: bool = False) -> bool:
    v = os.getenv(name)
    if v is None:
        return default
    return str(v).strip().lower() in _TRUTHY


async def _maybe_await(v: Any) -> Any:
    if inspect.isawaitable(v):
        return await v
    return v


def _normalize_symbol_safe(raw: str) -> str:
    """
    Prefer: core.data_engine_v2.normalize_symbol
    Fallback:
      - trims + uppercases
      - keeps Yahoo special (^, =)
      - numeric => 1120.SR
      - alpha => AAPL.US
      - has '.' => keep
    """
    s = (raw or "").strip()
    if not s:
        return ""

    try:
        from core.data_engine_v2 import normalize_symbol as _norm  # type: ignore

        ns = _norm(s)
        return (ns or "").strip().upper()
    except Exception:
        pass

    su = s.upper()

    if su.startswith("TADAWUL:"):
        su = su.split(":", 1)[1].strip()
    if su.endswith(".TADAWUL"):
        su = su.replace(".TADAWUL", "")

    if any(ch in su for ch in ("=", "^")):
        return su
    if "." in su:
        return su
    if su.isdigit():
        return f"{su}.SR"
    if su.isalpha():
        return f"{su}.US"
    return su


# ---------------------------------------------------------------------------
# Optional external-router import (OFF by default)
# ---------------------------------------------------------------------------
ENABLE_EXTERNAL_LEGACY_ROUTER = _env_bool("ENABLE_EXTERNAL_LEGACY_ROUTER", False)
LOG_EXTERNAL_IMPORT_FAILURE = _env_bool("LOG_EXTERNAL_LEGACY_IMPORT_FAILURE", False)

_external_loaded_from: Optional[str] = None


def _safe_mod_file(mod: Any) -> str:
    try:
        return str(getattr(mod, "__file__", "") or "")
    except Exception:
        return ""


def _try_import_external_router() -> Optional[APIRouter]:
    """
    Only executed if ENABLE_EXTERNAL_LEGACY_ROUTER=true.
    Returns external router if found, else None (no exceptions raised).

    Safety:
    - Avoids circular self-import by checking imported module file path.
    """
    global _external_loaded_from

    # 1) Try repo-root legacy_service.py first
    try:
        mod = importlib.import_module("legacy_service")
        # Avoid circular self-import (core/legacy_service.py)
        if _safe_mod_file(mod).replace("\\", "/").endswith("/core/legacy_service.py"):
            raise RuntimeError("circular import: legacy_service points to core.legacy_service")

        r = getattr(mod, "router", None)
        if isinstance(r, APIRouter):
            _external_loaded_from = "legacy_service"
            return r

        raise RuntimeError("legacy_service.router is missing/not an APIRouter")
    except Exception as exc1:
        # 2) Try routes/legacy_service.py
        try:
            mod2 = importlib.import_module("routes.legacy_service")
            r2 = getattr(mod2, "router", None)
            if isinstance(r2, APIRouter):
                _external_loaded_from = "routes.legacy_service"
                return r2
            raise RuntimeError("routes.legacy_service.router is missing/not an APIRouter")
        except Exception as exc2:
            if LOG_EXTERNAL_IMPORT_FAILURE:
                try:
                    print(
                        "External legacy router not importable. Using internal router. "
                        f"errors=[{exc1.__class__.__name__}] / [{exc2.__class__.__name__}]"
                    )
                except Exception:
                    pass
            return None


# ---------------------------------------------------------------------------
# Internal router (default)
# ---------------------------------------------------------------------------
router: APIRouter = APIRouter(prefix="/v1/legacy", tags=["legacy_compat"])


class SymbolsIn(BaseModel):
    symbols: List[str] = []


async def _get_engine_best_effort(request: Request) -> Tuple[Optional[Any], str]:
    """
    Reuse the shared engine created in main.py startup and stored at app.state.engine.
    Defensive fallback order:
      1) request.app.state.engine
      2) core.data_engine_v2.get_engine() (singleton)
      3) core.data_engine_v2.DataEngine() (temp instance)
      4) legacy core.data_engine.DataEngine() (temp)

    Returns: (engine_or_none, source_label)
    """
    eng = getattr(request.app.state, "engine", None)
    if eng is not None:
        return eng, "app.state.engine"

    # 2) v2 singleton (preferred)
    try:
        from core.data_engine_v2 import get_engine as v2_get_engine  # type: ignore

        eng2 = await _maybe_await(v2_get_engine())
        request.app.state.engine = eng2
        return eng2, "core.data_engine_v2.get_engine(singleton)"
    except Exception:
        pass

    # 3) v2 temp
    try:
        from core.data_engine_v2 import DataEngine as V2Engine  # type: ignore

        eng3 = V2Engine()
        request.app.state.engine = eng3
        return eng3, "core.data_engine_v2.DataEngine(temp)"
    except Exception:
        pass

    # 4) legacy temp (adapter)
    try:
        from core.data_engine import DataEngine as V1Engine  # type: ignore

        eng4 = V1Engine()
        request.app.state.engine = eng4
        return eng4, "core.data_engine.DataEngine(temp-legacy)"
    except Exception:
        return None, "none"


def _safe_err(e: BaseException) -> str:
    msg = str(e).strip()
    return msg or e.__class__.__name__


def _as_dict(obj: Any) -> Dict[str, Any]:
    if obj is None:
        return {}
    if isinstance(obj, dict):
        return jsonable_encoder(obj)
    md = getattr(obj, "model_dump", None)
    if callable(md):
        try:
            return jsonable_encoder(md())
        except Exception:
            pass
    d = getattr(obj, "dict", None)
    if callable(d):
        try:
            return jsonable_encoder(d())
        except Exception:
            pass
    od = getattr(obj, "__dict__", None)
    if isinstance(od, dict) and od:
        try:
            return jsonable_encoder(dict(od))
        except Exception:
            pass
    return {"value": str(obj)}


@router.get("/health", summary="Legacy compatibility health")
async def legacy_health(request: Request):
    eng = getattr(request.app.state, "engine", None)

    info: Dict[str, Any] = {
        "ok": True,
        "router": "core.legacy_service",
        "version": "1.5.0",
        "mode": "internal",
        "engine_present": eng is not None,
        "external_router_enabled": ENABLE_EXTERNAL_LEGACY_ROUTER,
    }

    # engine metadata (best-effort)
    try:
        from core.data_engine_v2 import ENGINE_VERSION  # type: ignore

        info["engine_version"] = ENGINE_VERSION
    except Exception:
        info["engine_version"] = "unknown"

    # richer meta if legacy adapter exposes it
    try:
        from core.data_engine import get_engine_meta  # type: ignore

        info["legacy_adapter_meta"] = get_engine_meta()
    except Exception:
        pass

    if _external_loaded_from:
        info["external_loaded_from"] = _external_loaded_from

    if eng is not None:
        try:
            info["engine_class"] = eng.__class__.__name__
            info["engine_module"] = eng.__class__.__module__
        except Exception:
            pass

    return info


@router.get("/quote", summary="Legacy quote endpoint (UnifiedQuote-like)")
async def legacy_quote(
    request: Request,
    symbol: str = Query(..., min_length=1),
    debug: int = Query(0, description="Set 1 to include traceback (or enable DEBUG_ERRORS=1)"),
):
    dbg = _env_bool("DEBUG_ERRORS", False) or bool(debug)

    raw = (symbol or "").strip()
    norm = _normalize_symbol_safe(raw)

    if not raw:
        out = {
            "status": "error",
            "symbol": "",
            "symbol_input": "",
            "symbol_normalized": "",
            "data_quality": "MISSING",
            "data_source": "none",
            "error": "Empty symbol",
        }
        return JSONResponse(status_code=200, content=jsonable_encoder(out))

    try:
        eng, src = await _get_engine_best_effort(request)
        if eng is None:
            out = {
                "status": "error",
                "symbol": norm or raw,
                "symbol_input": raw,
                "symbol_normalized": norm or raw,
                "data_quality": "MISSING",
                "data_source": "none",
                "error": "Legacy engine not available (no working provider).",
            }
            return JSONResponse(status_code=200, content=jsonable_encoder(out))

        # Prefer V2 naming first; tolerate older engines
        fn = getattr(eng, "get_enriched_quote", None) or getattr(eng, "get_quote", None)
        if not callable(fn):
            out = {
                "status": "error",
                "symbol": norm or raw,
                "symbol_input": raw,
                "symbol_normalized": norm or raw,
                "data_quality": "MISSING",
                "data_source": "none",
                "error": f"Engine missing get_enriched_quote/get_quote (source={src}).",
            }
            return JSONResponse(status_code=200, content=jsonable_encoder(out))

        q = await _maybe_await(fn(norm or raw))
        payload = _as_dict(q)

        # Ensure minimum fields for old clients
        payload.setdefault("status", "success")
        payload.setdefault("symbol", norm or raw)
        payload.setdefault("symbol_input", raw)
        payload.setdefault("symbol_normalized", norm or raw)
        payload.setdefault("error", "")
        payload.setdefault("data_source", payload.get("data_source") or src)
        payload.setdefault("data_quality", payload.get("data_quality") or ("OK" if payload.get("current_price") is not None else "PARTIAL"))

        return JSONResponse(status_code=200, content=jsonable_encoder(payload))

    except Exception as e:
        out: Dict[str, Any] = {
            "status": "error",
            "symbol": norm or raw,
            "symbol_input": raw,
            "symbol_normalized": norm or raw,
            "data_quality": "MISSING",
            "data_source": "none",
            "error": _safe_err(e),
        }
        if dbg:
            out["traceback"] = traceback.format_exc()[:8000]
        return JSONResponse(status_code=200, content=jsonable_encoder(out))


@router.post("/quotes", summary="Legacy batch quotes endpoint (list[UnifiedQuote-like])")
async def legacy_quotes(
    request: Request,
    payload: SymbolsIn,
    debug: int = Query(0, description="Set 1 to include traceback (or enable DEBUG_ERRORS=1)"),
):
    dbg = _env_bool("DEBUG_ERRORS", False) or bool(debug)

    raw_syms = payload.symbols or []
    norms = [(_normalize_symbol_safe(s) or (s or "").strip()) for s in raw_syms]
    norms = [n for n in norms if n]

    if not norms:
        # Keep legacy behavior: return list directly
        return JSONResponse(status_code=200, content=jsonable_encoder([]))

    try:
        eng, src = await _get_engine_best_effort(request)
        if eng is None:
            # Keep legacy behavior: list directly (no wrapper)
            items = [
                {
                    "status": "error",
                    "symbol": n,
                    "data_quality": "MISSING",
                    "data_source": "none",
                    "error": "Legacy engine not available (no working provider).",
                }
                for n in norms
            ]
            return JSONResponse(status_code=200, content=jsonable_encoder(items))

        # Prefer V2 naming first; tolerate older engines
        fn = getattr(eng, "get_enriched_quotes", None) or getattr(eng, "get_quotes", None)
        if not callable(fn):
            items = [
                {
                    "status": "error",
                    "symbol": n,
                    "data_quality": "MISSING",
                    "data_source": "none",
                    "error": f"Engine missing get_enriched_quotes/get_quotes (source={src}).",
                }
                for n in norms
            ]
            return JSONResponse(status_code=200, content=jsonable_encoder(items))

        res = await _maybe_await(fn(norms))
        if not isinstance(res, list):
            # Fallback: sequential calls
            single_fn = getattr(eng, "get_enriched_quote", None) or getattr(eng, "get_quote", None)
            if not callable(single_fn):
                items = [
                    {
                        "status": "error",
                        "symbol": n,
                        "data_quality": "MISSING",
                        "data_source": "none",
                        "error": f"Engine missing quote methods (source={src}).",
                    }
                    for n in norms
                ]
                return JSONResponse(status_code=200, content=jsonable_encoder(items))

            res = []
            for n in norms:
                try:
                    res.append(await _maybe_await(single_fn(n)))
                except Exception as e:
                    res.append({"status": "error", "symbol": n, "data_quality": "MISSING", "data_source": "none", "error": _safe_err(e)})

        # Ensure JSON-safe + minimal fields
        items_out: List[Dict[str, Any]] = []
        for i, n in enumerate(norms):
            obj = res[i] if i < len(res) else None
            d = _as_dict(obj)
            d.setdefault("status", "success")
            d.setdefault("symbol", n)
            d.setdefault("error", "")
            d.setdefault("data_source", d.get("data_source") or src)
            d.setdefault("data_quality", d.get("data_quality") or ("OK" if d.get("current_price") is not None else "PARTIAL"))
            items_out.append(d)

        # Keep legacy shape: return list directly
        return JSONResponse(status_code=200, content=jsonable_encoder(items_out))

    except Exception as e:
        # Keep legacy shape: list directly (best-effort errors per item)
        base_err = _safe_err(e)
        items = [
            {
                "status": "error",
                "symbol": n,
                "data_quality": "MISSING",
                "data_source": "none",
                "error": base_err,
                **({"traceback": traceback.format_exc()[:8000]} if dbg else {}),
            }
            for n in norms
        ]
        return JSONResponse(status_code=200, content=jsonable_encoder(items))


# ---------------------------------------------------------------------------
# If external router is enabled and import succeeds, replace internal router
# ---------------------------------------------------------------------------
if ENABLE_EXTERNAL_LEGACY_ROUTER:
    ext = _try_import_external_router()
    if ext is not None:
        router = ext  # type: ignore


def get_router() -> APIRouter:
    return router


__all__ = ["router", "get_router"]
