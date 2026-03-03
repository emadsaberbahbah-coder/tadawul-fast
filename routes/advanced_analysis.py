#!/usr/bin/env python3
"""
routes/advanced_analysis.py
------------------------------------------------------------
TADAWUL ADVANCED ANALYSIS ENGINE — v5.3.2 (PHASE 3 WIRED / COMPLETE)
PROD HARDENED + SCHEMA-DRIVEN SHEET-ROWS

Phase 3 design (IMPORTANT):
- ✅ /v1/advanced/sheet-rows is implemented ONLY in routes/advanced_sheet_rows.py
- ✅ This module mounts that router and adds:
    - GET /v1/advanced/health
    - GET /v1/advanced/metrics (optional Prometheus)
- ✅ Prevents duplicate path registration (FastAPI breaks if /sheet-rows exists twice)

Notes:
- Startup-safe: no network I/O at import time.
- Engine access is lazy via request.app.state.engine or core.data_engine_v2.get_engine().
"""

from __future__ import annotations

import logging
import os
from typing import Any, Dict, Optional

from fastapi import Request, Response

# -----------------------------------------------------------------------------
# ✅ Single source of truth for Phase 3 /sheet-rows
# -----------------------------------------------------------------------------
# This router already has:
#   prefix="/v1/advanced"
#   POST /sheet-rows
from routes.advanced_sheet_rows import router  # noqa: F401

logger = logging.getLogger("routes.advanced_analysis")

ADVANCED_ANALYSIS_VERSION = "5.3.2"


# -----------------------------------------------------------------------------
# Optional Prometheus (safe)
# -----------------------------------------------------------------------------
try:
    from prometheus_client import generate_latest, CONTENT_TYPE_LATEST  # type: ignore

    _PROMETHEUS_AVAILABLE = True
except Exception:
    generate_latest = None  # type: ignore
    CONTENT_TYPE_LATEST = "text/plain"
    _PROMETHEUS_AVAILABLE = False


# -----------------------------------------------------------------------------
# Engine accessor (lazy + safe)
# -----------------------------------------------------------------------------
async def _get_engine(request: Request) -> Optional[Any]:
    # Prefer app.state.engine (set by main.py lifespan)
    try:
        st = getattr(request.app, "state", None)
        if st and getattr(st, "engine", None):
            return st.engine
    except Exception:
        pass

    # Fallback to core.data_engine_v2.get_engine()
    try:
        from core.data_engine_v2 import get_engine  # type: ignore

        eng = get_engine()
        if hasattr(eng, "__await__"):
            eng = await eng
        return eng
    except Exception:
        return None


def _safe_engine_type(engine: Any) -> str:
    try:
        return type(engine).__name__
    except Exception:
        return "unknown"


def _safe_env_port() -> Optional[str]:
    p = (os.getenv("PORT") or "").strip()
    return p or None


# -----------------------------------------------------------------------------
# Added endpoints
# -----------------------------------------------------------------------------
@router.get("/health")
async def advanced_health(request: Request) -> Dict[str, Any]:
    """
    Lightweight health endpoint.
    Must not do heavy computations.
    """
    engine = await _get_engine(request)

    # Best-effort optional engine self-health
    engine_health: Optional[Dict[str, Any]] = None
    if engine is not None:
        for attr in ("health", "health_check", "get_health"):
            fn = getattr(engine, attr, None)
            if callable(fn):
                try:
                    r = fn()
                    if hasattr(r, "__await__"):
                        r = await r
                    if isinstance(r, dict):
                        engine_health = r
                except Exception:
                    engine_health = None
                break

    return {
        "status": "ok" if engine else "degraded",
        "version": ADVANCED_ANALYSIS_VERSION,
        "engine_available": bool(engine),
        "engine_type": _safe_engine_type(engine) if engine else "none",
        "engine_health": engine_health,
        "port": _safe_env_port(),
    }


@router.get("/metrics")
async def advanced_metrics() -> Response:
    """
    Prometheus metrics if available, otherwise 503.
    """
    if not _PROMETHEUS_AVAILABLE or generate_latest is None:
        return Response(content="Metrics not available", media_type="text/plain", status_code=503)
    return Response(content=generate_latest(), media_type=CONTENT_TYPE_LATEST)


__all__ = ["router"]
