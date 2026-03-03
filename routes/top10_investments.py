#!/usr/bin/env python3
# routes/top10_investments.py
"""
================================================================================
Top 10 Investments Router — v1.1.0 (PHASE 6)
================================================================================
Endpoints:
  - GET  /v1/analysis/top10
  - POST /v1/analysis/top10   (criteria override)

Returns schema-driven rows for Top_10_Investments sheet:
- Uses Criteria from Insights_Analysis top block (via criteria_model when available),
  with optional override from POST body.
- Uses core/analysis/top10_selector.py to filter + rank candidates.
- Normalizes output rows to Top_10_Investments schema (all keys exist, missing => null).
- Never fails on page alias mismatch (clean 400 where needed).

Startup-safe:
- No network calls at import time
- Engine is lazy (request.app.state.engine or core.data_engine_v2.get_engine)

Notes:
- This router is under /v1/analysis (same family as ai_analysis.py).
"""

from __future__ import annotations

import logging
import time
import uuid
from typing import Any, Dict, List, Optional, Sequence, Tuple, Union

from fastapi import APIRouter, Body, Header, HTTPException, Query, Request, status

# Schema / pages
from core.sheets.schema_registry import get_sheet_spec
from core.sheets.page_catalog import CANONICAL_PAGES, normalize_page_name

# Selector
from core.analysis.top10_selector import (
    Criteria,
    build_top10_output_rows,
    load_criteria_best_effort,
    merge_criteria_overrides,
    select_top10,
)

# Best JSON response (orjson if available)
try:
    import orjson  # type: ignore
    from fastapi.responses import ORJSONResponse as BestJSONResponse  # type: ignore

    def _json_content(v: Any) -> Any:
        return v

    _HAS_ORJSON = True
except Exception:
    from fastapi.responses import JSONResponse as BestJSONResponse  # type: ignore

    def _json_content(v: Any) -> Any:
        return v

    _HAS_ORJSON = False


logger = logging.getLogger("routes.top10_investments")

ROUTE_VERSION = "1.1.0"
router = APIRouter(prefix="/v1/analysis", tags=["AI & Analysis"])


# -----------------------------------------------------------------------------
# Helpers
# -----------------------------------------------------------------------------
def _safe_uuid(x: Optional[str]) -> str:
    return (x or "").strip() or str(uuid.uuid4())[:18]


async def _maybe_await(v: Any) -> Any:
    if hasattr(v, "__await__"):
        return await v
    return v


async def _get_engine(request: Request) -> Optional[Any]:
    # Prefer app.state.engine
    try:
        st = getattr(request.app, "state", None)
        if st and getattr(st, "engine", None):
            return st.engine
    except Exception:
        pass

    # Fallback to core.data_engine_v2.get_engine
    try:
        from core.data_engine_v2 import get_engine  # type: ignore

        eng = get_engine()
        eng = await _maybe_await(eng)
        try:
            request.app.state.engine = eng
        except Exception:
            pass
        return eng
    except Exception:
        return None


def _clean_nans(obj: Any) -> Any:
    # lightweight NaN/Inf cleaning without importing math at module top
    try:
        import math

        if isinstance(obj, dict):
            return {k: _clean_nans(v) for k, v in obj.items()}
        if isinstance(obj, list):
            return [_clean_nans(v) for v in obj]
        if isinstance(obj, float) and (math.isnan(obj) or math.isinf(obj)):
            return None
    except Exception:
        pass
    return obj


def _normalize_to_schema(schema_keys: Sequence[str], raw: Dict[str, Any]) -> Dict[str, Any]:
    raw = raw or {}
    raw_lc = {str(k).lower(): v for k, v in raw.items()}
    out: Dict[str, Any] = {}
    for k in schema_keys:
        if k in raw:
            out[k] = raw.get(k)
        elif k.lower() in raw_lc:
            out[k] = raw_lc.get(k.lower())
        else:
            out[k] = None
    return out


def _schema_for_top10() -> Tuple[List[str], List[str]]:
    """
    Uses schema_registry for Top_10_Investments (must exist in Phase 3 registry).
    Falls back to a safe minimal schema if missing (still returns consistent shape).
    """
    try:
        spec = get_sheet_spec("Top_10_Investments")
        headers = [c.header for c in spec.columns]
        keys = [c.key for c in spec.columns]
        return headers, keys
    except Exception:
        # Minimal fallback (won't crash clients)
        headers = [
            "Rank",
            "Source Page",
            "Symbol",
            "Name",
            "Currency",
            "Exchange",
            "Current Price",
            "Expected ROI (Horizon)",
            "Forecast Price (Horizon)",
            "Forecast Confidence",
            "Overall Score",
            "Risk Score",
            "Recommendation",
            "Last Updated (UTC)",
            "Last Updated (Riyadh)",
        ]
        keys = [
            "rank",
            "source_page",
            "symbol",
            "name",
            "currency",
            "exchange",
            "current_price",
            "expected_roi",
            "forecast_price",
            "forecast_confidence",
            "overall_score",
            "risk_score",
            "recommendation",
            "last_updated_utc",
            "last_updated_riyadh",
        ]
        return headers, keys


# -----------------------------------------------------------------------------
# Request model (lightweight dict body; Pydantic optional)
# -----------------------------------------------------------------------------
def _parse_override_body(body: Any) -> Dict[str, Any]:
    """
    Accepts dict overrides; ignores extra keys.
    """
    if isinstance(body, dict):
        return body
    return {}


# -----------------------------------------------------------------------------
# Endpoints
# -----------------------------------------------------------------------------
@router.get("/top10")
async def get_top10(
    request: Request,
    include_matrix: bool = Query(True, description="Include rows_matrix (legacy compatibility)."),
    enrich_final: Optional[bool] = Query(None, description="Override whether to enrich final Top-N via engine."),
    top_n: Optional[int] = Query(None, ge=1, le=50, description="Override Top-N size."),
    x_request_id: Optional[str] = Header(default=None, alias="X-Request-ID"),
) -> BestJSONResponse:
    """
    Uses criteria from Insights_Analysis (embedded criteria block) and returns Top 10 investments.
    """
    t0 = time.time()
    request_id = _safe_uuid(x_request_id)

    engine = await _get_engine(request)
    if not engine:
        return BestJSONResponse(
            status_code=503,
            content=_clean_nans(
                {
                    "status": "error",
                    "error": "Data engine unavailable",
                    "version": ROUTE_VERSION,
                    "request_id": request_id,
                    "meta": {"duration_ms": (time.time() - t0) * 1000.0},
                }
            ),
        )

    # Load criteria
    criteria = load_criteria_best_effort(engine)

    # Apply query overrides
    overrides: Dict[str, Any] = {}
    if enrich_final is not None:
        overrides["enrich_final"] = bool(enrich_final)
    if top_n is not None:
        overrides["top_n"] = int(top_n)

    if overrides:
        criteria = merge_criteria_overrides(criteria, overrides)

    # Select
    candidates, sel_meta = await select_top10(engine=engine, criteria=criteria)
    horizon = criteria.horizon()
    output_rows = build_top10_output_rows(candidates, horizon=horizon)

    # Schema normalize to Top_10_Investments
    headers, keys = _schema_for_top10()

    # If schema doesn't include horizon-specific ROI keys, we keep your schema registry as truth.
    # We still return the horizon in meta, and rows will include schema keys only.
    normalized_rows: List[Dict[str, Any]] = []
    for r in output_rows:
        # If sheet schema expects expected_roi_1m/3m/12m etc., r already contains those keys.
        normalized_rows.append(_normalize_to_schema(keys, r))

    payload: Dict[str, Any] = {
        "status": "success",
        "page": "Top_10_Investments",
        "headers": headers,
        "keys": keys,
        "rows": normalized_rows,
        "rows_matrix": [[row.get(k) for k in keys] for row in normalized_rows] if include_matrix else None,
        "version": ROUTE_VERSION,
        "request_id": request_id,
        "meta": {
            "duration_ms": (time.time() - t0) * 1000.0,
            "criteria": {
                "pages_selected": list(criteria.pages_selected),
                "invest_period_days": int(criteria.invest_period_days),
                "horizon": horizon,
                "min_expected_roi": criteria.min_expected_roi,
                "max_risk_score": float(criteria.max_risk_score),
                "min_confidence": float(criteria.min_confidence),
                "enforce_risk_confidence": bool(criteria.enforce_risk_confidence),
                "top_n": int(criteria.top_n),
                "enrich_final": bool(criteria.enrich_final),
            },
            "selection": sel_meta,
        },
    }

    return BestJSONResponse(content=_clean_nans(payload))


@router.post("/top10")
async def post_top10(
    request: Request,
    body: Dict[str, Any] = Body(default_factory=dict),
    include_matrix: bool = Query(True, description="Include rows_matrix (legacy compatibility)."),
    x_request_id: Optional[str] = Header(default=None, alias="X-Request-ID"),
) -> BestJSONResponse:
    """
    POST override form:
      {
        "invest_period_days": 90,
        "pages_selected": ["Market_Leaders","Global_Markets"],
        "min_expected_roi": 0.05,
        "max_risk_score": 60,
        "min_confidence": 0.75,
        "top_n": 10,
        "enrich_final": true,
        "enforce_risk_confidence": true
      }
    """
    t0 = time.time()
    request_id = _safe_uuid(x_request_id)

    engine = await _get_engine(request)
    if not engine:
        return BestJSONResponse(
            status_code=503,
            content=_clean_nans(
                {
                    "status": "error",
                    "error": "Data engine unavailable",
                    "version": ROUTE_VERSION,
                    "request_id": request_id,
                    "meta": {"duration_ms": (time.time() - t0) * 1000.0},
                }
            ),
        )

    base_criteria = load_criteria_best_effort(engine)
    overrides = _parse_override_body(body or {})
    criteria = merge_criteria_overrides(base_criteria, overrides)

    candidates, sel_meta = await select_top10(engine=engine, criteria=criteria)
    horizon = criteria.horizon()
    output_rows = build_top10_output_rows(candidates, horizon=horizon)

    headers, keys = _schema_for_top10()

    normalized_rows: List[Dict[str, Any]] = []
    for r in output_rows:
        normalized_rows.append(_normalize_to_schema(keys, r))

    payload: Dict[str, Any] = {
        "status": "success",
        "page": "Top_10_Investments",
        "headers": headers,
        "keys": keys,
        "rows": normalized_rows,
        "rows_matrix": [[row.get(k) for k in keys] for row in normalized_rows] if include_matrix else None,
        "version": ROUTE_VERSION,
        "request_id": request_id,
        "meta": {
            "duration_ms": (time.time() - t0) * 1000.0,
            "criteria": {
                "pages_selected": list(criteria.pages_selected),
                "invest_period_days": int(criteria.invest_period_days),
                "horizon": horizon,
                "min_expected_roi": criteria.min_expected_roi,
                "max_risk_score": float(criteria.max_risk_score),
                "min_confidence": float(criteria.min_confidence),
                "enforce_risk_confidence": bool(criteria.enforce_risk_confidence),
                "top_n": int(criteria.top_n),
                "enrich_final": bool(criteria.enrich_final),
            },
            "selection": sel_meta,
        },
    }

    return BestJSONResponse(content=_clean_nans(payload))


# -----------------------------------------------------------------------------
# Dynamic loader hook (optional)
# -----------------------------------------------------------------------------
def get_router() -> APIRouter:
    return router


def mount(app: Any) -> None:
    try:
        app.include_router(router)
    except Exception:
        pass


__all__ = ["router", "mount", "get_router", "ROUTE_VERSION"]
