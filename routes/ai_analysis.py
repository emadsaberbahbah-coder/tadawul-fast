#!/usr/bin/env python3
"""
routes/ai_analysis.py
------------------------------------------------------------
TADAWUL ENTERPRISE AI ANALYSIS ENGINE — v8.7.0 (PHASE 5 HARDENED)
SAMA Compliant | Resilient Routes | Safe Tracing | No Raw 500s

Wiring policy (IMPORTANT):
- ✅ /v1/analysis/sheet-rows is implemented ONLY in routes/analysis_sheet_rows.py
- ✅ This module mounts that router and adds:
    - GET /v1/analysis/health
    - GET /v1/analysis/metrics (optional Prometheus)
- ✅ Prevents duplicate path registration (FastAPI breaks if /sheet-rows exists twice)

Phase 5 alignment upgrades:
- ✅ Consistent auth handling (best-effort) via core.config.auth_ok when available
- ✅ Health now includes:
    - canonical_pages + forbidden_pages (from page_catalog) when available
    - schema_version + schema_available (from core.config / schema_registry) when available
    - safe flags snapshot (schema_headers_always, computations_enabled, forecasting_enabled, scoring_enabled)
- ✅ Startup-safe: no network I/O at import time.
- ✅ Engine access is lazy via request.app.state.engine or core.data_engine_v2/core.data_engine.get_engine().
"""

from __future__ import annotations

import logging
import os
from typing import Any, Dict, Optional

from fastapi import Header, HTTPException, Query, Request, Response, status

# -----------------------------------------------------------------------------
# ✅ Single source of truth for Phase 3 /sheet-rows
# -----------------------------------------------------------------------------
# This router already has:
#   prefix="/v1/analysis"
#   POST /sheet-rows
from routes.analysis_sheet_rows import router  # noqa: F401

logger = logging.getLogger("routes.ai_analysis")

AI_ANALYSIS_VERSION = "8.7.0"


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
# Optional core.config (safe)
# -----------------------------------------------------------------------------
try:
    from core.config import auth_ok, get_settings_cached  # type: ignore
except Exception:
    auth_ok = None  # type: ignore

    def get_settings_cached(*args, **kwargs):  # type: ignore
        return None


def _safe_env_port() -> Optional[str]:
    p = (os.getenv("PORT") or "").strip()
    return p or None


def _safe_engine_type(engine: Any) -> str:
    try:
        return type(engine).__name__
    except Exception:
        return "unknown"


# -----------------------------------------------------------------------------
# Auth helper (best-effort; no startup dependency)
# -----------------------------------------------------------------------------
def _extract_auth_token(
    *,
    token_query: Optional[str],
    x_app_token: Optional[str],
    authorization: Optional[str],
) -> str:
    auth_token = (x_app_token or "").strip()
    if authorization and authorization.strip().lower().startswith("bearer "):
        auth_token = authorization.strip().split(" ", 1)[1].strip()

    if token_query and not auth_token:
        allow_query = False
        try:
            settings = get_settings_cached()
            allow_query = bool(getattr(settings, "allow_query_token", False))
        except Exception:
            allow_query = False
        if allow_query:
            auth_token = token_query.strip()

    return auth_token


def _require_auth_or_401(
    *,
    token_query: Optional[str],
    x_app_token: Optional[str],
    authorization: Optional[str],
) -> None:
    if auth_ok is None:
        return
    auth_token = _extract_auth_token(token_query=token_query, x_app_token=x_app_token, authorization=authorization)
    if not auth_ok(
        token=auth_token,
        authorization=authorization,
        headers={"X-APP-TOKEN": x_app_token, "Authorization": authorization},
    ):
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid token")


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

    # Fallback to core.data_engine_v2.get_engine(), then core.data_engine.get_engine()
    for modpath in ("core.data_engine_v2", "core.data_engine"):
        try:
            mod = __import__(modpath, fromlist=["get_engine"])
            get_engine = getattr(mod, "get_engine", None)
            if callable(get_engine):
                eng = get_engine()
                if hasattr(eng, "__await__"):
                    eng = await eng
                return eng
        except Exception:
            continue

    return None


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


# -----------------------------------------------------------------------------
# Added endpoints
# -----------------------------------------------------------------------------
@router.get("/health")
async def analysis_health(
    request: Request,
    token: Optional[str] = Query(default=None, description="Auth token (query only if allowed)"),
    x_app_token: Optional[str] = Header(default=None, alias="X-APP-TOKEN"),
    authorization: Optional[str] = Header(default=None, alias="Authorization"),
) -> Dict[str, Any]:
    """
    Lightweight health endpoint.
    Must not do heavy computations.
    """
    _require_auth_or_401(token_query=token, x_app_token=x_app_token, authorization=authorization)

    engine = await _get_engine(request)

    # Best-effort optional engine self-health
    engine_health: Optional[Dict[str, Any]] = None
    engine_stats: Optional[Dict[str, Any]] = None

    if engine is not None:
        for attr in ("health", "health_check", "get_health"):
            r = await _maybe_call(engine, attr)
            if isinstance(r, dict):
                engine_health = r
                break

        for attr in ("get_stats", "stats", "metrics"):
            r = await _maybe_call(engine, attr)
            if isinstance(r, dict):
                engine_stats = r
                break

    # Best-effort read flags (do not fail if settings not available)
    settings = None
    try:
        settings = get_settings_cached()
    except Exception:
        settings = None

    schema_available = False
    schema_version = None
    canonical_pages = None
    forbidden_pages = None

    try:
        from core.sheets.page_catalog import CANONICAL_PAGES, FORBIDDEN_PAGES  # type: ignore

        canonical_pages = list(CANONICAL_PAGES)
        forbidden_pages = list(FORBIDDEN_PAGES)
    except Exception:
        canonical_pages = None
        forbidden_pages = None

    try:
        schema_version = getattr(settings, "schema_version", None) if settings else None
        schema_available = bool(getattr(settings, "schema_enabled", True) and getattr(settings, "schema_headers_always", True)) if settings else False
    except Exception:
        schema_available = False
        schema_version = None

    return {
        "status": "ok" if engine else "degraded",
        "version": AI_ANALYSIS_VERSION,
        "engine_available": bool(engine),
        "engine_type": _safe_engine_type(engine) if engine else "none",
        "engine_health": engine_health,
        "engine_stats": engine_stats,
        "schema_available": bool(schema_available),
        "schema_version": schema_version or "unknown",
        "schema_headers_always": bool(getattr(settings, "schema_headers_always", True)) if settings else True,
        "computations_enabled": bool(getattr(settings, "computations_enabled", True)) if settings else True,
        "forecasting_enabled": bool(getattr(settings, "forecasting_enabled", True)) if settings else True,
        "scoring_enabled": bool(getattr(settings, "scoring_enabled", True)) if settings else True,
        "canonical_pages": canonical_pages,
        "forbidden_pages": forbidden_pages,
        "port": _safe_env_port(),
        "request_id": getattr(request.state, "request_id", None),
    }


@router.get("/metrics")
async def analysis_metrics() -> Response:
    """
    Prometheus metrics if available; otherwise 503.
    """
    if not _PROMETHEUS_AVAILABLE or generate_latest is None:
        return Response(content="Metrics not available", media_type="text/plain", status_code=503)
    return Response(content=generate_latest(), media_type=CONTENT_TYPE_LATEST)


__all__ = ["router"]
