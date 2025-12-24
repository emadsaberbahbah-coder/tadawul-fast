# routes_argaam.py  (REPO ROOT SHIM)  - FULL REPLACEMENT
"""
routes_argaam.py (repo root shim)
------------------------------------------------------------
Argaam Router Shim – v1.2.0 (PROD SAFE)

Purpose
- Keep backward compatibility for deployments that import `routes_argaam.router`
  from the repo root.
- Delegate to the real package router: `routes.routes_argaam.router`
  (your v3.2.x implementation).
- If delegation fails for any reason, expose a minimal fallback router that
  returns 200 + error payload (never crashes app startup).

Design rules
- Very small surface area.
- No heavy side effects at import time (no engine init, no network).
- Safe logging only.
"""

from __future__ import annotations

import logging
from typing import Any, Optional

logger = logging.getLogger("routes.argaam_shim")

SHIM_VERSION = "1.2.0"
DELEGATE_MODULE = "routes.routes_argaam"


def _try_import_delegate() -> Optional[Any]:
    try:
        import importlib

        m = importlib.import_module(DELEGATE_MODULE)
        r = getattr(m, "router", None)
        if r is None:
            raise AttributeError(f"{DELEGATE_MODULE}.router not found")
        return r
    except Exception as exc:
        logger.exception("[argaam_shim] Failed to import delegate router from %s: %s", DELEGATE_MODULE, exc)
        return None


_delegate_router = _try_import_delegate()

if _delegate_router is not None:
    # ✅ Happy path: expose the real router
    router = _delegate_router  # type: ignore
else:
    # -------------------------------------------------------------------------
    # Fallback router: do not block startup; return informative error payloads.
    # -------------------------------------------------------------------------
    from fastapi import APIRouter, Header, Query

    router = APIRouter(prefix="/v1/argaam", tags=["KSA / Argaam (shim fallback)"])

    @router.get("/health")
    async def _shim_health() -> dict:
        return {
            "status": "error",
            "module": "routes_argaam (repo root shim)",
            "shim_version": SHIM_VERSION,
            "delegate_module": DELEGATE_MODULE,
            "error": "Delegate router import failed. Ensure routes/routes_argaam.py exists and exports `router`.",
        }

    @router.get("/quote")
    async def _shim_quote(
        symbol: str = Query(...),
        x_app_token: Optional[str] = Header(default=None, alias="X-APP-TOKEN"),
        debug: int = Query(default=0),
    ) -> dict:
        # Keep the response shape friendly for callers
        return {
            "symbol": (symbol or "").strip().upper(),
            "market": "KSA",
            "currency": "SAR",
            "data_quality": "MISSING",
            "data_source": "shim_fallback",
            "error": "Delegate router import failed (routes.routes_argaam). Check deployment package structure.",
            "shim_version": SHIM_VERSION,
            "debug": {"token_provided": bool((x_app_token or "").strip()), "debug": debug} if debug else None,
        }

    __all__ = ["router"]

__all__ = ["router", "SHIM_VERSION", "DELEGATE_MODULE"]
