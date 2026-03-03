#!/usr/bin/env python3
# routes/top10_investments.py
"""
================================================================================
Top 10 Investments Router — v1.0.0 (PHASE 6)
================================================================================
Endpoint:
  GET  /v1/analysis/top10
  POST /v1/analysis/top10   (criteria overrides)

Returns rows to populate Top_10_Investments (schema-driven):
- headers (from schema_registry)
- keys (schema keys)
- rows (list[dict] with all schema keys, missing => null)
- rows_matrix (optional legacy list-of-lists)

Auth:
- Uses core.config.auth_ok / is_open_mode when available
- Secure fallback if missing

Engine:
- Lazy access via request.app.state.engine or core.data_engine_v2.get_engine()

"""

from __future__ import annotations

import logging
import math
import os
import time
import uuid
from typing import Any, Dict, List, Optional, Sequence, Tuple, Union

from fastapi import APIRouter, Body, Header, HTTPException, Query, Request, status

# -----------------------------------------------------------------------------
# JSON response (orjson if available)
# -----------------------------------------------------------------------------
def clean_nans(obj: Any) -> Any:
    try:
        import math as _m

        if isinstance(obj, dict):
            return {k: clean_nans(v) for k, v in obj.items()}
        if isinstance(obj, list):
            return [clean_nans(v) for v in obj]
        if isinstance(obj, float) and (_m.isnan(obj) or _m.isinf(obj)):
            return None
    except Exception:
        pass
    return obj


try:
    import orjson
    from fastapi.responses import Response as StarletteResponse

    class BestJSONResponse(StarletteResponse):
        media_type = "application/json"

        def render(self, content: Any) -> bytes:
            return orjson.dumps(clean_nans(content), default=str)

    _HAS_ORJSON = True
except Exception:
    import json
    from fastapi.responses import JSONResponse as _JSONResponse

    class BestJSONResponse(_JSONResponse):
        def render(self, content: Any) -> bytes:
            return json.dumps(clean_nans(content), default=str, ensure_ascii=False).encode("utf-8")

    _HAS_ORJSON = False


logger = logging.getLogger("routes.top10_investments")

ROUTER_VERSION = "1.0.0"
router = APIRouter(prefix="/v1/analysis", tags=["AI & Analysis"])


# -----------------------------------------------------------------------------
# Auth (align to core.config)
# -----------------------------------------------------------------------------
_TRUTHY = {"1", "true", "yes", "y", "on", "t"}


def _request_id(request: Request, x_request_id: Optional[str]) -> str:
    try:
        rid = getattr(request.state, "request_id", None)
        if rid:
            return str(rid)
    except Exception:
        pass
    return x_request_id or str(uuid.uuid4())[:18]


def _extract_auth_token(token_q: Optional[str], x_app_token: Optional[str], authorization: Optional[str]) -> str:
    if authorization:
        a = authorization.strip()
        if a.lower().startswith("bearer "):
            return a.split(" ", 1)[1].strip()
        return a
    if x_app_token and x_app_token.strip():
        return x_app_token.strip()
    if token_q and token_q.strip():
        return token_q.strip()
    return ""


def _auth_ok(request: Request, *, token_q: Optional[str], x_app_token: Optional[str], authorization: Optional[str]) -> bool:
    try:
        from core.config import is_open_mode, auth_ok  # type: ignore

        if callable(is_open_mode) and is_open_mode():
            return True

        tok = _extract_auth_token(token_q, x_app_token, authorization)
        if callable(auth_ok):
            return bool(auth_ok(token=tok, authorization=authorization, headers={"X-APP-TOKEN": x_app_token, "Authorization": authorization}))
        return False
    except Exception:
        require = (os.getenv("REQUIRE_AUTH", "true") or "true").strip().lower() in _TRUTHY
        if not require:
            return True
        tok = _extract_auth_token(token_q, x_app_token, authorization)
        return bool(tok)


# -----------------------------------------------------------------------------
# Engine (lazy)
# -----------------------------------------------------------------------------
async def _maybe_await(v: Any) -> Any:
    if hasattr(v, "__await__"):
        return await v
    return v


_ENGINE_LOCK = None


async def _get_engine(request: Request) -> Tuple[Optional[Any], str, Optional[str]]:
    # 1) app.state.engine
    try:
        st = getattr(request.app, "state", None)
        if st and getattr(st, "engine", None):
            return st.engine, "app.state.engine", None
    except Exception:
        pass

    # 2) core.data_engine_v2.get_engine
    try:
        from core.data_engine_v2 import get_engine  # type: ignore

        eng = await _maybe_await(get_engine())
        try:
            request.app.state.engine = eng
        except Exception:
            pass
        return eng, "core.data_engine_v2.get_engine", None
    except Exception as e:
        return None, "engine_init_failed", f"{type(e).__name__}: {e}"


# -----------------------------------------------------------------------------
# Schema normalization (Top_10_Investments)
# -----------------------------------------------------------------------------
def _get_sheet_spec(page: str):
    from core.sheets.schema_registry import get_sheet_spec  # type: ignore

    return get_sheet_spec(page)


def _normalize_row_to_schema(schema_keys: Sequence[str], raw: Dict[str, Any], *, symbol_fallback: str = "") -> Dict[str, Any]:
    raw = raw or {}
    raw_lc = {str(k).lower(): v for k, v in raw.items()}
    out: Dict[str, Any] = {}

    # simple aliasing for common keys
    aliases: Dict[str, Tuple[str, ...]] = {
        "symbol": ("symbol", "ticker"),
        "rank": ("rank",),
        "source_page": ("source_page", "origin", "page"),
        "name": ("name", "company_name"),
        "current_price": ("current_price", "price"),
        "forecast_confidence": ("forecast_confidence", "confidence_score"),
        "overall_score": ("overall_score",),
        "risk_score": ("risk_score",),
        "expected_roi_1m": ("expected_roi_1m",),
        "expected_roi_3m": ("expected_roi_3m",),
        "expected_roi_12m": ("expected_roi_12m",),
        "forecast_price_1m": ("forecast_price_1m",),
        "forecast_price_3m": ("forecast_price_3m",),
        "forecast_price_12m": ("forecast_price_12m",),
        "recommendation": ("recommendation",),
        "last_updated_utc": ("last_updated_utc",),
        "last_updated_riyadh": ("last_updated_riyadh",),
    }

    for k in schema_keys:
        v = None
        if k in raw:
            v = raw.get(k)
        elif k.lower() in raw_lc:
            v = raw_lc.get(k.lower())
        else:
            for a in aliases.get(k, ()):
                if a in raw:
                    v = raw.get(a)
                    break
                if a.lower() in raw_lc:
                    v = raw_lc.get(a.lower())
                    break
        out[k] = v

    if "symbol" in out and not out.get("symbol"):
        out["symbol"] = symbol_fallback

    return out


# -----------------------------------------------------------------------------
# Core selector
# -----------------------------------------------------------------------------
from core.analysis.top10_selector import (
    Criteria,
    load_criteria_best_effort,
    merge_criteria_overrides,
    select_top10,
    build_top10_output_rows,
)


@router.get("/top10")
async def get_top10(
    request: Request,
    token: Optional[str] = Query(default=None, description="Auth token (query only if ALLOW_QUERY_TOKEN=true in server)"),
    include_matrix: bool = Query(default=True, description="Include rows_matrix for legacy clients"),
    x_app_token: Optional[str] = Header(default=None, alias="X-APP-TOKEN"),
    authorization: Optional[str] = Header(default=None, alias="Authorization"),
    x_request_id: Optional[str] = Header(default=None, alias="X-Request-ID"),
) -> BestJSONResponse:
    t0 = time.time()
    rid = _request_id(request, x_request_id)

    if not _auth_ok(request, token_q=token if (os.getenv("ALLOW_QUERY_TOKEN", "").strip().lower() in _TRUTHY) else None, x_app_token=x_app_token, authorization=authorization):
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="unauthorized")

    engine, src, err = await _get_engine(request)
    if not engine:
        return BestJSONResponse(
            status_code=503,
            content={
                "status": "error",
                "error": "Data engine unavailable",
                "engine_source": src,
                "engine_error": err,
                "version": ROUTER_VERSION,
                "request_id": rid,
                "meta": {"duration_ms": (time.time() - t0) * 1000.0},
            },
        )

    criteria = load_criteria_best_effort(engine)
    top, meta = await select_top10(engine=engine, criteria=criteria)
    rows_raw = build_top10_output_rows(top, horizon=criteria.horizon())

    # Normalize to Top_10_Investments schema
    try:
        spec = _get_sheet_spec("Top_10_Investments")
        headers = [c.header for c in spec.columns]
        keys = [c.key for c in spec.columns]
    except Exception as e:
        # fallback: keys from raw rows
        headers = []
        keys = sorted({k for r in rows_raw for k in r.keys()})

    normalized_rows = [_normalize_row_to_schema(keys, r, symbol_fallback=str(r.get("symbol") or "")) for r in rows_raw]

    payload = {
        "status": "success",
        "page": "Top_10_Investments",
        "headers": headers,
        "keys": keys,
        "rows": normalized_rows,
        "rows_matrix": [[row.get(k) for k in keys] for row in normalized_rows] if include_matrix else None,
        "criteria": {
            "pages_selected": criteria.pages_selected,
            "invest_period_days": criteria.invest_period_days,
            "horizon": criteria.horizon(),
            "min_expected_roi": criteria.min_expected_roi,
            "max_risk_score": criteria.max_risk_score,
            "min_confidence": criteria.min_confidence,
            "enforce_risk_confidence": criteria.enforce_risk_confidence,
            "top_n": criteria.top_n,
            "enrich_final": criteria.enrich_final,
        },
        "version": ROUTER_VERSION,
        "request_id": rid,
        "meta": {
            **(meta or {}),
            "duration_ms": (time.time() - t0) * 1000.0,
            "engine_source": src,
        },
    }
    return BestJSONResponse(content=clean_nans(payload))


@router.post("/top10")
async def post_top10(
    request: Request,
    body: Dict[str, Any] = Body(default_factory=dict),
    token: Optional[str] = Query(default=None, description="Auth token (query only if ALLOW_QUERY_TOKEN=true in server)"),
    include_matrix: bool = Query(default=True, description="Include rows_matrix for legacy clients"),
    x_app_token: Optional[str] = Header(default=None, alias="X-APP-TOKEN"),
    authorization: Optional[str] = Header(default=None, alias="Authorization"),
    x_request_id: Optional[str] = Header(default=None, alias="X-Request-ID"),
) -> BestJSONResponse:
    """
    POST lets you override criteria.
    Example body:
      {
        "invest_period_days": 90,
        "pages_selected": ["Market_Leaders","Global_Markets"],
        "min_expected_roi": 0.05,
        "max_risk_score": 60,
        "min_confidence": 0.75,
        "top_n": 10,
        "enrich_final": true
      }
    """
    t0 = time.time()
    rid = _request_id(request, x_request_id)

    if not _auth_ok(request, token_q=token if (os.getenv("ALLOW_QUERY_TOKEN", "").strip().lower() in _TRUTHY) else None, x_app_token=x_app_token, authorization=authorization):
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="unauthorized")

    engine, src, err = await _get_engine(request)
    if not engine:
        return BestJSONResponse(
            status_code=503,
            content={
                "status": "error",
                "error": "Data engine unavailable",
                "engine_source": src,
                "engine_error": err,
                "version": ROUTER_VERSION,
                "request_id": rid,
                "meta": {"duration_ms": (time.time() - t0) * 1000.0},
            },
        )

    base = load_criteria_best_effort(engine)
    criteria = merge_criteria_overrides(base, body or {})

    top, meta = await select_top10(engine=engine, criteria=criteria)
    rows_raw = build_top10_output_rows(top, horizon=criteria.horizon())

    try:
        spec = _get_sheet_spec("Top_10_Investments")
        headers = [c.header for c in spec.columns]
        keys = [c.key for c in spec.columns]
    except Exception:
        headers = []
        keys = sorted({k for r in rows_raw for k in r.keys()})

    normalized_rows = [_normalize_row_to_schema(keys, r, symbol_fallback=str(r.get("symbol") or "")) for r in rows_raw]

    payload = {
        "status": "success",
        "page": "Top_10_Investments",
        "headers": headers,
        "keys": keys,
        "rows": normalized_rows,
        "rows_matrix": [[row.get(k) for k in keys] for row in normalized_rows] if include_matrix else None,
        "criteria": {
            "pages_selected": criteria.pages_selected,
            "invest_period_days": criteria.invest_period_days,
            "horizon": criteria.horizon(),
            "min_expected_roi": criteria.min_expected_roi,
            "max_risk_score": criteria.max_risk_score,
            "min_confidence": criteria.min_confidence,
            "enforce_risk_confidence": criteria.enforce_risk_confidence,
            "top_n": criteria.top_n,
            "enrich_final": criteria.enrich_final,
        },
        "version": ROUTER_VERSION,
        "request_id": rid,
        "meta": {
            **(meta or {}),
            "duration_ms": (time.time() - t0) * 1000.0,
            "engine_source": src,
        },
    }
    return BestJSONResponse(content=clean_nans(payload))


__all__ = ["router"]
