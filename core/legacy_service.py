# core/legacy_service.py
"""
core/legacy_service.py
------------------------------------------------------------
Compatibility shim (hardened) — v1.2.0

Some code mounts:
  _mount_router(app, "core.legacy_service")

But the real router may live in:
  - legacy_service.py (repo root)
  - routes/legacy_service.py

Goals
✅ Never break app boot if missing
✅ Avoid circular imports (keep imports local)
✅ Provide a minimal /health endpoint when fallback router is used
✅ Log a clear warning (best-effort) instead of failing silently
"""

from __future__ import annotations

import logging
from typing import Optional

from fastapi import APIRouter

logger = logging.getLogger("core.legacy_service")

router: APIRouter = APIRouter(tags=["legacy_compat"])
_loaded_from: Optional[str] = None


def _fallback_router(reason: str) -> APIRouter:
    r = APIRouter(tags=["legacy_compat"])

    @r.get("/v1/legacy/health", summary="Legacy compatibility health (fallback)")
    async def legacy_health_fallback():
        return {
            "ok": True,
            "router": "fallback",
            "module": "core.legacy_service",
            "reason": reason,
            "hint": "legacy_service router not importable; using fallback router",
        }

    return r


# Prefer root module first (if project still has legacy_service.py at repo root),
# else fallback to routes.legacy_service
try:
    from legacy_service import router as _router  # type: ignore

    router = _router
    _loaded_from = "legacy_service"

except Exception as exc1:
    try:
        from routes.legacy_service import router as _router  # type: ignore

        router = _router
        _loaded_from = "routes.legacy_service"

    except Exception as exc2:
        router = _fallback_router(reason=f"{exc1} | {exc2}")
        _loaded_from = None

        # best-effort log once
        try:
            logger.warning(
                "core.legacy_service: cannot import legacy router from legacy_service or routes.legacy_service. "
                "Using fallback router. Errors: [%s] / [%s]",
                exc1,
                exc2,
            )
        except Exception:
            pass


__all__ = ["router"]
