# core/legacy_service.py  (FULL REPLACEMENT)
from __future__ import annotations

"""
core/legacy_service.py
------------------------------------------------------------
Compatibility shim (quiet + useful) — v1.4.0

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
                # Keep very small (avoid log spam)
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
    symbols: List[str]


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

    # 2) v2 singleton
    try:
        from core.data_engine_v2 import get_engine as v2_get_engine  # type: ignore

        eng2 = await v2_get_engine()
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

    # 4) legacy temp
    try:
        from core.data_engine import DataEngine as V1Engine  # type: ignore

        eng4 = V1Engine()
        request.app.state.engine = eng4
        return eng4, "core.data_engine.DataEngine(temp)"
    except Exception:
        return None, "none"


def _safe_err(e: BaseException) -> str:
    msg = str(e).strip()
    return msg or e.__class__.__name__


@router.get("/health", summary="Legacy compatibility health")
async def legacy_health(request: Request):
    eng = getattr(request.app.state, "engine", None)

    info: Dict[str, Any] = {
        "ok": True,
        "router": "core.legacy_service",
        "version": "1.4.0",
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

    if _external_loaded_from:
        info["external_loaded_from"] = _external_loaded_from

    if eng is not None:
        try:
            info["engine_class"] = eng.__class__.__name__
            info["engine_module"] = eng.__class__.__module__
        except Exception:
            pass

    return info


@router.get("/quote", summary="Legacy quote endpoint (UnifiedQuote)")
async def legacy_quote(
    request: Request,
    symbol: str = Query(..., min_length=1),
    debug: int = Query(0, description="Set 1 to include traceback (or enable DEBUG_ERRORS=1)"),
):
    dbg = _env_bool("DEBUG_ERRORS", False) or bool(debug)

    try:
        eng, src = await _get_engine_best_effort(request)
        if eng is None:
            out = {
                "status": "error",
                "symbol": (symbol or "").strip(),
                "data_quality": "MISSING",
                "data_source": "none",
                "error": "Legacy engine not available (no working provider).",
            }
            return JSONResponse(status_code=200, content=jsonable_encoder(out))

        # Prefer v2 method names
        fn = getattr(eng, "get_quote", None) or getattr(eng, "get_enriched_quote", None)
        if not callable(fn):
            out = {
                "status": "error",
                "symbol": (symbol or "").strip(),
                "data_quality": "MISSING",
                "data_source": "none",
                "error": f"Engine missing get_quote/get_enriched_quote (source={src}).",
            }
            return JSONResponse(status_code=200, content=jsonable_encoder(out))

        q = await fn(symbol)
        # Ensure JSON-safe
        return JSONResponse(status_code=200, content=jsonable_encoder(q))

    except Exception as e:
        out: Dict[str, Any] = {
            "status": "error",
            "symbol": (symbol or "").strip(),
            "data_quality": "MISSING",
            "data_source": "none",
            "error": _safe_err(e),
        }
        if dbg:
            out["traceback"] = traceback.format_exc()[:8000]
        return JSONResponse(status_code=200, content=jsonable_encoder(out))


@router.post("/quotes", summary="Legacy batch quotes endpoint (list[UnifiedQuote])")
async def legacy_quotes(
    request: Request,
    payload: SymbolsIn,
    debug: int = Query(0, description="Set 1 to include traceback (or enable DEBUG_ERRORS=1)"),
):
    dbg = _env_bool("DEBUG_ERRORS", False) or bool(debug)

    try:
        eng, src = await _get_engine_best_effort(request)
        if eng is None:
            out = {
                "status": "error",
                "count": 0,
                "items": [],
                "error": "Legacy engine not available (no working provider).",
            }
            return JSONResponse(status_code=200, content=jsonable_encoder(out))

        # Prefer batch method names when available
        fn = getattr(eng, "get_quotes", None) or getattr(eng, "get_enriched_quotes", None)
        if not callable(fn):
            out = {
                "status": "error",
                "count": 0,
                "items": [],
                "error": f"Engine missing get_quotes/get_enriched_quotes (source={src}).",
            }
            return JSONResponse(status_code=200, content=jsonable_encoder(out))

        items = await fn(payload.symbols)
        # Keep legacy shape: return list directly (older clients may expect [])
        return JSONResponse(status_code=200, content=jsonable_encoder(items))

    except Exception as e:
        out: Dict[str, Any] = {
            "status": "error",
            "count": 0,
            "items": [],
            "error": _safe_err(e),
        }
        if dbg:
            out["traceback"] = traceback.format_exc()[:8000]
        return JSONResponse(status_code=200, content=jsonable_encoder(out))


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
