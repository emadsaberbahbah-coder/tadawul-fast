#!/usr/bin/env python3
"""
routes/advanced_analysis.py
------------------------------------------------------------
TADAWUL ADVANCED ANALYSIS ROUTER — v5.4.0 (PHASE 5 / STARTUP-SAFE)
PROD HARDENED + SCHEMA-DRIVEN SHEET-ROWS + INSIGHTS CRITERIA EMBED

Phase 3 (kept, IMPORTANT):
- ✅ /v1/advanced/sheet-rows is implemented ONLY in routes/advanced_sheet_rows.py
- ✅ This module imports that router (prefix="/v1/advanced") and adds:
    - GET  /v1/advanced/health
    - GET  /v1/advanced/metrics (optional Prometheus)
- ✅ Prevents duplicate path registration (FastAPI breaks if /sheet-rows exists twice)

Phase 5 (NEW, Insights_Analysis criteria embedded):
- ✅ Adds:
    - GET  /v1/advanced/insights-criteria   (criteria rules + weights snapshot)
    - POST /v1/advanced/insights-analysis   (schema-driven rows + optional criteria embed)
- ✅ Startup-safe: NO heavy imports / NO network at import-time
- ✅ Auth is best-effort and consistent with core.config.auth_ok if available
- ✅ Schema-driven: uses schema_registry for headers/keys when available
- ✅ Does NOT redefine /sheet-rows (avoids duplicate route)

Notes:
- The /insights-analysis endpoint is intentionally "best-effort":
  it fetches rows from the engine (like /sheet-rows) but can also embed criteria
  fields IF those keys exist in the Insights_Analysis schema.
"""

from __future__ import annotations

import logging
import os
from typing import Any, Dict, List, Optional

from fastapi import Body, Header, HTTPException, Query, Request, Response, status

# -----------------------------------------------------------------------------
# ✅ Single source of truth for Phase 3 /sheet-rows
# -----------------------------------------------------------------------------
# This router already has:
#   prefix="/v1/advanced"
#   POST /sheet-rows
from routes.advanced_sheet_rows import router  # noqa: F401


logger = logging.getLogger("routes.advanced_analysis")

ADVANCED_ANALYSIS_VERSION = "5.4.0"

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
# core.config is preferred for auth + flags, but router must be safe if unavailable
# -----------------------------------------------------------------------------
try:
    from core.config import auth_ok, get_settings_cached  # type: ignore
except Exception:
    auth_ok = None  # type: ignore

    def get_settings_cached(*args, **kwargs):  # type: ignore
        return None


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
# Auth helper (best-effort; consistent with advanced_sheet_rows)
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
# Phase 5 — criteria snapshot builder (safe, no schema assumptions)
# -----------------------------------------------------------------------------
def _build_insights_criteria_snapshot() -> Dict[str, Any]:
    """
    Returns a stable criteria snapshot that matches core/scoring.py logic.
    Best-effort: reads DEFAULT_WEIGHTS when present, else uses safe defaults.
    """
    weights = {
        "w_valuation": 0.30,
        "w_momentum": 0.30,
        "w_quality": 0.20,
        "w_opportunity": 0.20,
        "risk_penalty_strength": 0.55,
        "confidence_penalty_strength": 0.45,
        "source": "defaults",
    }

    try:
        from core import scoring as scoring_mod  # type: ignore

        dw = getattr(scoring_mod, "DEFAULT_WEIGHTS", None)
        if dw is not None:
            weights = {
                "w_valuation": float(getattr(dw, "w_valuation", weights["w_valuation"])),
                "w_momentum": float(getattr(dw, "w_momentum", weights["w_momentum"])),
                "w_quality": float(getattr(dw, "w_quality", weights["w_quality"])),
                "w_opportunity": float(getattr(dw, "w_opportunity", weights["w_opportunity"])),
                "risk_penalty_strength": float(getattr(dw, "risk_penalty_strength", weights["risk_penalty_strength"])),
                "confidence_penalty_strength": float(getattr(dw, "confidence_penalty_strength", weights["confidence_penalty_strength"])),
                "source": "core.scoring.DEFAULT_WEIGHTS",
            }
    except Exception:
        pass

    rules = [
        {
            "rule": "Low confidence gate",
            "when": "confidence_score < 45",
            "then": "HOLD",
        },
        {
            "rule": "High risk + moderate score",
            "when": "risk_score >= 75 AND overall_score < 75",
            "then": "REDUCE",
        },
        {
            "rule": "Strong buy (ROI + confidence + low risk + high score)",
            "when": "expected_roi_3m >= 25% AND confidence_score >= 70 AND risk_score <= 45 AND overall_score >= 78",
            "then": "STRONG_BUY",
        },
        {
            "rule": "Buy (ROI + confidence + acceptable risk + score)",
            "when": "expected_roi_3m >= 12% AND confidence_score >= 60 AND risk_score <= 55 AND overall_score >= 70",
            "then": "BUY",
        },
        {
            "rule": "Buy (score-based fallback)",
            "when": "overall_score >= 82 AND risk_score <= 55",
            "then": "BUY",
        },
        {
            "rule": "Hold (moderate)",
            "when": "overall_score >= 65",
            "then": "HOLD",
        },
        {
            "rule": "Reduce (weak)",
            "when": "overall_score >= 50",
            "then": "REDUCE",
        },
        {
            "rule": "Sell (very weak)",
            "when": "overall_score < 50",
            "then": "SELL",
        },
    ]

    return {"weights": weights, "rules": rules}


def _get_list(body: Dict[str, Any], *keys: str) -> List[str]:
    for k in keys:
        v = body.get(k)
        if isinstance(v, list):
            return [str(x).strip() for x in v if str(x).strip()]
    return []


def _get_bool(body: Dict[str, Any], key: str, default: bool) -> bool:
    v = body.get(key)
    if isinstance(v, bool):
        return v
    if isinstance(v, str):
        return v.strip().lower() in {"1", "true", "yes", "y", "on"}
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
        "phase5_insights_criteria": True,
    }


@router.get("/metrics")
async def advanced_metrics() -> Response:
    """
    Prometheus metrics if available, otherwise 503.
    """
    if not _PROMETHEUS_AVAILABLE or generate_latest is None:
        return Response(content="Metrics not available", media_type="text/plain", status_code=503)
    return Response(content=generate_latest(), media_type=CONTENT_TYPE_LATEST)


@router.get("/insights-criteria")
async def insights_criteria(
    token: Optional[str] = Query(default=None, description="Auth token (query only if allowed)"),
    x_app_token: Optional[str] = Header(default=None, alias="X-APP-TOKEN"),
    authorization: Optional[str] = Header(default=None, alias="Authorization"),
) -> Dict[str, Any]:
    """
    Returns a criteria snapshot that is designed to be embedded into Insights_Analysis.
    This endpoint is schema-agnostic (safe for all clients).
    """
    _require_auth_or_401(token_query=token, x_app_token=x_app_token, authorization=authorization)

    snap = _build_insights_criteria_snapshot()
    return {
        "status": "success",
        "version": ADVANCED_ANALYSIS_VERSION,
        "criteria": snap,
        "embedded_target": "Insights_Analysis",
    }


@router.post("/insights-analysis")
async def insights_analysis(
    request: Request,
    body: Dict[str, Any] = Body(...),
    mode: str = Query(default="", description="Optional mode hint for engine/provider"),
    include_matrix: Optional[bool] = Query(default=None, description="Return rows_matrix for legacy clients"),
    embed_criteria: bool = Query(default=True, description="Attempt to embed criteria into schema keys (if present)"),
    token: Optional[str] = Query(default=None, description="Auth token (query only if allowed)"),
    x_app_token: Optional[str] = Header(default=None, alias="X-APP-TOKEN"),
    authorization: Optional[str] = Header(default=None, alias="Authorization"),
    x_request_id: Optional[str] = Header(default=None, alias="X-Request-ID"),
) -> Dict[str, Any]:
    """
    Phase 5 endpoint: best-effort Insights_Analysis computation.
    - Fetches rows via engine for requested symbols (like /sheet-rows)
    - Returns full schema headers/keys for Insights_Analysis when schema_registry is available
    - Optionally embeds criteria JSON into the FIRST row if schema supports relevant keys
    """
    _require_auth_or_401(token_query=token, x_app_token=x_app_token, authorization=authorization)
    request_id = x_request_id or (getattr(request.state, "request_id", None) or "")

    symbols = _get_list(body, "symbols", "tickers")
    top_n = int(body.get("top_n") or 2000)
    top_n = max(1, min(5000, top_n))
    symbols = symbols[:top_n]

    include_matrix_final = include_matrix if isinstance(include_matrix, bool) else _get_bool(body, "include_matrix", True)

    # Load schema (best-effort)
    headers: List[str] = []
    keys: List[str] = []
    spec = None
    try:
        from core.sheets.schema_registry import get_sheet_spec  # type: ignore

        spec = get_sheet_spec("Insights_Analysis")
        headers = [c.header for c in spec.columns]
        keys = [c.key for c in spec.columns]
    except Exception:
        headers = []
        keys = []

    # Engine required
    engine = await _get_engine(request)
    if not engine:
        raise HTTPException(status_code=status.HTTP_503_SERVICE_UNAVAILABLE, detail="Data engine unavailable")

    # Prefer engine batch method
    quotes: Dict[str, Any] = {}
    try:
        fn = getattr(engine, "get_enriched_quotes_batch", None)
        if callable(fn):
            res = await fn(symbols, mode=mode or "", schema="Insights_Analysis")
            if isinstance(res, dict):
                quotes = res
    except Exception:
        quotes = {}

    # Fallback per-symbol if needed
    if not quotes and symbols:
        out: Dict[str, Any] = {}
        for s in symbols:
            try:
                fn2 = getattr(engine, "get_enriched_quote_dict", None)
                if callable(fn2):
                    out[s] = await fn2(s, schema="Insights_Analysis")
                else:
                    fn3 = getattr(engine, "get_enriched_quote", None)
                    if callable(fn3):
                        q = await fn3(s, schema="Insights_Analysis")
                        if hasattr(q, "model_dump"):
                            out[s] = q.model_dump(mode="python")
                        elif hasattr(q, "dict"):
                            out[s] = q.dict()
                        else:
                            out[s] = {"symbol": s, "error": "engine_quote_unserializable"}
                    else:
                        out[s] = {"symbol": s, "error": "engine_missing_quote_methods"}
            except Exception as e:
                out[s] = {"symbol": s, "error": str(e)}
        quotes = out

    # Normalize to schema keys if available
    normalized_rows: List[Dict[str, Any]] = []
    errors = 0

    normalize_fn = None
    try:
        from core.data_engine_v2 import normalize_row_to_schema  # type: ignore

        normalize_fn = normalize_row_to_schema
    except Exception:
        normalize_fn = None

    for sym in symbols:
        raw = quotes.get(sym)
        if raw is None:
            raw = {"symbol": sym, "error": "no_data"}
        if isinstance(raw, dict) and raw.get("error"):
            errors += 1

        row = dict(raw) if isinstance(raw, dict) else {"symbol": sym, "result": raw}

        if normalize_fn is not None:
            try:
                row = normalize_fn("Insights_Analysis", row)
            except Exception:
                pass
        normalized_rows.append(row)

    # Embed criteria into first row (only if schema has matching keys)
    if embed_criteria and normalized_rows:
        snap = _build_insights_criteria_snapshot()
        criteria_json = None
        try:
            criteria_json = json.dumps(snap, ensure_ascii=False, default=str)
        except Exception:
            criteria_json = str(snap)

        candidate_keys = [
            "advisor_criteria_json",
            "advisor_criteria",
            "criteria_json",
            "insights_criteria",
            "scoring_criteria",
            "recommendation_rules",
            "criteria",
        ]
        if keys:
            first = normalized_rows[0]
            for ck in candidate_keys:
                if ck in keys:
                    first[ck] = criteria_json
            # also try weights if present
            for wk in ("scoring_weights", "weights_json", "weights"):
                if wk in keys:
                    first[wk] = criteria_json

    status_out = "success" if errors == 0 else ("partial" if errors < len(symbols) else "error")

    # If no schema available, still provide a basic key set from the first row
    if not keys and normalized_rows:
        keys = list(normalized_rows[0].keys())
        headers = keys[:]  # best-effort

    return {
        "status": status_out,
        "page": "Insights_Analysis",
        "headers": headers,
        "keys": keys,
        "rows": normalized_rows,
        "rows_matrix": [[row.get(k) for k in keys] for row in normalized_rows] if include_matrix_final else None,
        "error": f"{errors} errors" if errors else None,
        "version": ADVANCED_ANALYSIS_VERSION,
        "request_id": request_id or None,
        "meta": {
            "requested": len(symbols),
            "errors": errors,
            "mode": mode,
            "criteria_embedded": bool(embed_criteria),
            "schema_available": bool(headers and keys),
        },
    }


__all__ = ["router"]
