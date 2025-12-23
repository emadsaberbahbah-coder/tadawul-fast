# core/enriched_quote.py
"""
core/enriched_quote.py
------------------------------------------------------------
Compatibility shim (hardened) — v1.1.0

Why this exists
- Older code may import: `from core.enriched_quote import router`
- Real router lives in: `routes.enriched_quote`

Design goals
✅ Never break app boot if routes are missing / mid-refactor
✅ Keep import graph light (avoid circular imports)
✅ Provide a minimal health endpoint when fallback router is used
✅ Log a clear warning once (optional) instead of failing silently
"""

from __future__ import annotations

import logging
from typing import Optional

from fastapi import APIRouter

logger = logging.getLogger("core.enriched_quote")

router: APIRouter = APIRouter(tags=["enriched_quote_compat"])

_loaded_from: Optional[str] = None

try:
    # Prefer the real router
    from routes.enriched_quote import router as _real_router  # type: ignore

    router = _real_router
    _loaded_from = "routes.enriched_quote"

except Exception as exc:
    _loaded_from = None

    # Minimal fallback so the app still starts (and monitoring sees it)
    @router.get("/v1/enriched/health", summary="Compatibility health (fallback)")
    async def enriched_quote_health_fallback():
        return {
            "ok": True,
            "router": "fallback",
            "module": "core.enriched_quote",
            "hint": "routes.enriched_quote not importable; using fallback router",
        }

    # Log once (avoid noisy logs in serverless / multi-worker)
    try:
        logger.warning("core.enriched_quote: failed to import routes.enriched_quote (%s). Using fallback router.", exc)
    except Exception:
        pass


__all__ = ["router"]
