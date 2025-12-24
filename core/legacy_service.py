from __future__ import annotations

"""
core/legacy_service.py
------------------------------------------------------------
Compatibility shim (quiet + useful) — v1.3.0

Why this exists
- main.py mounts core.legacy_service.router
- Older versions sometimes had a router in:
    - legacy_service.py (repo root)
    - routes/legacy_service.py

This shim:
✅ DOES NOT spam logs by default
✅ Uses app.state.engine (shared DataEngine) if present
✅ Provides stable legacy endpoints:
   - GET  /v1/legacy/health
   - GET  /v1/legacy/quote?symbol=...
   - POST /v1/legacy/quotes   {"symbols":[...]}
✅ Optional: can try importing external legacy routers ONLY if enabled via env
"""

import os
import logging
from typing import List, Optional

from fastapi import APIRouter, Query, Request
from pydantic import BaseModel

logger = logging.getLogger("core.legacy_service")

_TRUTHY = {"1", "true", "yes", "y", "on", "t"}


def _env_bool(name: str, default: bool = False) -> bool:
    v = os.getenv(name)
    if v is None:
        return default
    return str(v).strip().lower() in _TRUTHY


# ---------------------------------------------------------------------------
# Optional external-router import (OFF by default to avoid startup warning spam)
# ---------------------------------------------------------------------------
ENABLE_EXTERNAL_LEGACY_ROUTER = _env_bool("ENABLE_EXTERNAL_LEGACY_ROUTER", False)
LOG_EXTERNAL_IMPORT_FAILURE = _env_bool("LOG_EXTERNAL_LEGACY_IMPORT_FAILURE", False)

_external_loaded_from: Optional[str] = None


def _try_import_external_router() -> Optional[APIRouter]:
    """
    Only executed if ENABLE_EXTERNAL_LEGACY_ROUTER=true.
    Returns external router if found, else None (no exceptions raised).
    """
    global _external_loaded_from

    # Try repo-root legacy_service.py first
    try:
        from legacy_service import router as r  # type: ignore

        _external_loaded_from = "legacy_service"
        return r
    except Exception as exc1:
        # Try routes/legacy_service.py
        try:
            from routes.legacy_service import router as r  # type: ignore

            _external_loaded_from = "routes.legacy_service"
            return r
        except Exception as exc2:
            if LOG_EXTERNAL_IMPORT_FAILURE:
                logger.warning(
                    "External legacy router not importable. Using internal router. Errors: [%s] / [%s]",
                    exc1,
                    exc2,
                )
            return None


# ---------------------------------------------------------------------------
# Internal router (default)
# ---------------------------------------------------------------------------
router: APIRouter = APIRouter(prefix="/v1/legacy", tags=["legacy_compat"])


def _get_engine(request: Request):
    """
    Reuse the shared engine created in main.py startup and stored at app.state.engine.
    Extremely defensive fallback: if missing, create once and store.
    """
    eng = getattr(request.app.state, "engine", None)
    if eng is None:
        # Defensive fallback (should not happen if init_engine_on_boot=true)
        from core.data_engine_v2 import DataEngine  # local import to avoid any import graph surprises

        eng = DataEngine()
        request.app.state.engine = eng
    return eng


@router.get("/health", summary="Legacy compatibility health")
async def legacy_health(request: Request):
    eng = getattr(request.app.state, "engine", None)

    info = {
        "ok": True,
        "router": "core.legacy_service",
        "mode": "internal",
        "engine_present": eng is not None,
    }

    # best-effort engine metadata (do not crash if not available)
    try:
        from core.data_engine_v2 import ENGINE_VERSION  # type: ignore

        info["engine_version"] = ENGINE_VERSION
    except Exception:
        info["engine_version"] = "unknown"

    # tell if external router import is enabled and/or used
    info["external_router_enabled"] = ENABLE_EXTERNAL_LEGACY_ROUTER
    if _external_loaded_from:
        info["external_loaded_from"] = _external_loaded_from

    return info


@router.get("/quote", summary="Legacy quote endpoint (UnifiedQuote)")
async def legacy_quote(request: Request, symbol: str = Query(..., min_length=1)):
    eng = _get_engine(request)
    # DataEngine v2 returns UnifiedQuote already
    return await eng.get_quote(symbol)


class SymbolsIn(BaseModel):
    symbols: List[str]


@router.post("/quotes", summary="Legacy batch quotes endpoint (list[UnifiedQuote])")
async def legacy_quotes(request: Request, payload: SymbolsIn):
    eng = _get_engine(request)
    return await eng.get_quotes(payload.symbols)


# ---------------------------------------------------------------------------
# If external router is enabled and import succeeds, replace internal router
# ---------------------------------------------------------------------------
if ENABLE_EXTERNAL_LEGACY_ROUTER:
    ext = _try_import_external_router()
    if ext is not None:
        router = ext  # type: ignore
        try:
            logger.info("Legacy router loaded from %s", _external_loaded_from or "external")
        except Exception:
            pass


__all__ = ["router"]
