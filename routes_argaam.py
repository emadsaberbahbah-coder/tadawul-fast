# routes_argaam.py  (REPO ROOT SHIM — FULL REPLACEMENT)
"""
routes_argaam.py
------------------------------------------------------------
Compatibility Shim / Stable Import Surface (ROOT)

Why this file exists
- Some entrypoints/import graphs expect:  `from routes_argaam import router`
- The real implementation lives at:      `routes/routes_argaam.py`
- This shim re-exports the real router **without breaking boot**
  if optional deps or submodules fail to import (Render-safe).

Behavior
- Preferred: import and re-export `router` from `routes.routes_argaam`
- Fallback: expose a minimal router at /v1/argaam that returns an
  informative error payload (never raises at import-time).
"""

from __future__ import annotations

import logging
from importlib import import_module
from typing import Any, Optional

logger = logging.getLogger("routes_argaam_shim")

SHIM_VERSION = "1.0.0"
ROUTE_VERSION = "unknown"  # will be overwritten if real router loads

# -----------------------------------------------------------------------------
# Try to load the real router
# -----------------------------------------------------------------------------
def _safe_import(path: str) -> Optional[Any]:
    try:
        return import_module(path)
    except Exception as exc:
        logger.warning("[routes_argaam shim] import failed: %s (%s)", path, exc)
        return None


def _load_real_router() -> Optional[Any]:
    global ROUTE_VERSION

    mod = _safe_import("routes.routes_argaam")
    if not mod:
        return None

    r = getattr(mod, "router", None)
    if r is None:
        logger.warning("[routes_argaam shim] routes.routes_argaam has no `router` attribute.")
        return None

    # best-effort propagate route version if present
    rv = getattr(mod, "ROUTE_VERSION", None) or getattr(mod, "route_version", None)
    if isinstance(rv, str) and rv.strip():
        ROUTE_VERSION = rv.strip()

    return r


_real = _load_real_router()

# -----------------------------------------------------------------------------
# Fallback router (never crash app boot)
# -----------------------------------------------------------------------------
if _real is None:
    try:
        from fastapi import APIRouter, Query, Request
    except Exception as exc:  # ultra-defensive: if fastapi missing (shouldn't happen)
        logger.error("[routes_argaam shim] fastapi import failed: %s", exc)

        class _DummyRouter:  # type: ignore
            def __init__(self) -> None:
                self.prefix = "/v1/argaam"

        router = _DummyRouter()  # type: ignore

    else:
        router = APIRouter(prefix="/v1/argaam", tags=["KSA / Argaam (shim-fallback)"])

        @router.get("/health")
        async def health(_: Request) -> dict:
            return {
                "status": "error",
                "module": "routes_argaam_shim",
                "shim_version": SHIM_VERSION,
                "route_version": ROUTE_VERSION,
                "engine_available": False,
                "error": "Real router failed to import: routes.routes_argaam",
            }

        @router.get("/quote")
        async def quote(symbol: str = Query(..., description="KSA symbol e.g. 1120.SR")) -> dict:
            return {
                "symbol": (symbol or "").strip().upper(),
                "market": "KSA",
                "currency": "SAR",
                "data_quality": "MISSING",
                "data_source": "none",
                "error": "Real router failed to import: routes.routes_argaam",
            }

        @router.post("/sheet-rows")
        async def sheet_rows() -> dict:
            return {
                "status": "error",
                "headers": [],
                "rows": [],
                "meta": {},
                "error": "Real router failed to import: routes.routes_argaam",
            }

else:
    router = _real  # type: ignore


__all__ = ["router", "ROUTE_VERSION", "SHIM_VERSION"]
