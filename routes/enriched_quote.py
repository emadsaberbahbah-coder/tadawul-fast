#!/usr/bin/env python3
"""
routes/enriched_quote.py
================================================================================
TFB Enriched Quote Routes Wrapper — v5.2.1 (RENDER-SAFE / PROMETHEUS-SAFE)
================================================================================

Why this wrapper exists:
- ✅ FIX (your deploy error): avoid Prometheus duplicate-timeseries crashes by NOT creating
  any metrics in this module and by NOT importing legacy routes.config at import-time.
- ✅ Keeps startup safe: no heavy imports at module import-time.
- ✅ Ensures the dynamic router loader can mount this module via mount(app).

This module mounts the real implementation from:
  - core.enriched_quote (preferred)

Contract:
- Exposes mount(app) for the dynamic loader
- Exposes get_router() for direct use
"""

from __future__ import annotations

import importlib
import logging
from typing import Any, Optional

from fastapi import APIRouter

logger = logging.getLogger("routes.enriched_quote")

ROUTER_VERSION = "5.2.1"

# IMPORTANT:
# Do NOT define `router = APIRouter(...)` at import-time, because your dynamic loader
# prefers `router` over `mount()`. We want lazy import of core.enriched_quote.
router: Optional[APIRouter] = None


def _fallback_router(reason: str) -> APIRouter:
    """
    Minimal router used only if core.enriched_quote cannot be imported.
    Keeps service alive + makes the failure visible.
    """
    r = APIRouter(prefix="/v1/enriched", tags=["enriched"])

    @r.get("/health", include_in_schema=False)
    async def health():
        return {
            "status": "degraded",
            "module": "routes.enriched_quote",
            "router_version": ROUTER_VERSION,
            "reason": reason,
        }

    @r.get("/headers", include_in_schema=False)
    async def headers():
        return {
            "status": "degraded",
            "module": "routes.enriched_quote",
            "router_version": ROUTER_VERSION,
            "headers": [],
            "reason": reason,
        }

    return r


def get_router() -> APIRouter:
    """
    Lazily imports and returns the real enriched router.
    """
    global router
    if router is not None:
        return router

    try:
        mod = importlib.import_module("core.enriched_quote")
    except Exception as e:
        logger.warning("Failed to import core.enriched_quote: %s", e)
        router = _fallback_router(f"import_failed: {type(e).__name__}: {e}")
        return router

    # Preferred: core.enriched_quote exports `router`
    r = getattr(mod, "router", None)

    # Fallback: core.enriched_quote exports get_router()
    if r is None:
        fn = getattr(mod, "get_router", None)
        if callable(fn):
            try:
                r = fn()
            except Exception as e:
                logger.warning("core.enriched_quote.get_router() failed: %s", e)
                r = None

    if not isinstance(r, APIRouter):
        router = _fallback_router("core.enriched_quote has no APIRouter export")
        return router

    router = r
    return router


def mount(app: Any) -> None:
    """
    Dynamic loader hook.
    """
    r = get_router()
    try:
        app.include_router(r)
        logger.info("Mounted enriched router: %s", getattr(r, "prefix", "/v1/enriched"))
    except Exception as e:
        # Last resort: mount fallback router so service stays up
        logger.warning("Mount failed; using fallback router. err=%s", e)
        app.include_router(_fallback_router(f"mount_failed: {type(e).__name__}: {e}"))


__all__ = ["ROUTER_VERSION", "get_router", "mount", "router"]
