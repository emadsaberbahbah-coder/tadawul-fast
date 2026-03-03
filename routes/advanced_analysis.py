#!/usr/bin/env python3
"""
routes/advanced_analysis.py
------------------------------------------------------------
TADAWUL ADVANCED ANALYSIS ROUTER — v5.3.3 (PHASE 3 WIRED / STARTUP-SAFE)
PROD HARDENED + SCHEMA-DRIVEN SHEET-ROWS

Phase 3 design (IMPORTANT):
- ✅ /v1/advanced/sheet-rows is implemented ONLY in routes/advanced_sheet_rows.py
- ✅ This module imports that router (same prefix="/v1/advanced") and adds:
    - GET /v1/advanced/health
    - GET /v1/advanced/metrics (optional Prometheus)
- ✅ Prevents duplicate path registration (FastAPI breaks if /sheet-rows exists twice)

Hardening upgrades in v5.3.3:
- ✅ No heavy work at import time (startup-safe)
- ✅ Engine access is lazy (app.state.engine first; then core.data_engine_v2.get_engine)
- ✅ Best-effort engine stats/health discovery without raising
- ✅ Health includes:
    - schema_pages (from page_catalog) if available
    - provider/engine stats (if engine exposes get_stats/health*)
    - request_id passthrough (if middleware set it)
- ✅ Metrics endpoint returns 503 safely when Prometheus is missing
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

ADVANCED_ANALYSIS_VERSION = "5.3.3"


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


async def _maybe_call(obj: Any, name: str) -> Optional[Any]:
    """
    Best-effort call obj.<name>(), supporting sync/async.
    Never raises.
    """
    try:
        fn = getattr(obj, name, None)
        if not callable(fn):
            return None
        out = fn()
        if hasattr(out, "__await__"):
            out = await out
        return out
    except Exception:
        return None


def _safe_bool_env(name: str, default: bool = False) -> bool:
    try:
        v = (os.getenv(name, str(default)) or "").strip().lower()
        return v in ("1", "true", "yes", "y", "on", "t")
    except Exception:
        return default


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

    # schema pages (optional)
    schema_pages = None
    try:
        from core.sheets.page_catalog import CANONICAL_PAGES  # type: ignore

        schema_pages = list(CANONICAL_PAGES)
    except Exception:
        schema_pages = None

    # best-effort engine health/stats
    engine_health: Optional[Dict[str, Any]] = None
    engine_stats: Optional[Dict[str, Any]] = None

    if engine is not None:
        # health-like
        for attr in ("health", "health_check", "get_health"):
            r = await _maybe_call(engine, attr)
            if isinstance(r, dict):
                engine_health = r
                break

        # stats-like
        for attr in ("get_stats", "stats", "metrics"):
            r = await _maybe_call(engine, attr)
            if isinstance(r, dict):
                engine_stats = r
                break

    # request_id passthrough if middleware sets it
    request_id = None
    try:
        request_id = getattr(request.state, "request_id", None)
    except Exception:
        request_id = None

    return {
        "status": "ok" if engine else "degraded",
        "version": ADVANCED_ANALYSIS_VERSION,
        "engine_available": bool(engine),
        "engine_type": _safe_engine_type(engine) if engine else "none",
        "engine_health": engine_health,
        "engine_stats": engine_stats,
        "schema_pages": schema_pages,
        "port": _safe_env_port(),
        "require_auth": _safe_bool_env("REQUIRE_AUTH", True),
        "request_id": request_id,
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
