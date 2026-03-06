#!/usr/bin/env python3
"""
routes/top10_investments.py
------------------------------------------------------------
TOP 10 INVESTMENTS ROUTER — v1.1.0
(ONE SOURCE OF TRUTH / LIVE / SCHEMA-FIRST / TOP10-FIELD-HARDENED)

What this revision fixes
- ✅ FIX: Top10-only fields are now normalized and backfilled when possible:
      - top10_rank
      - selection_reason
      - criteria_snapshot
- ✅ FIX: Rows are projected to schema keys while preserving special fields
- ✅ FIX: Dedicated fallback enrichment for missing Top10 metadata
- ✅ FIX: Criteria snapshot is carried into each returned row when absent
- ✅ FIX: Top10 rank is auto-assigned from returned order when missing
- ✅ SAFE: no network calls at import time
- ✅ SAFE: lazy engine loading
- ✅ SAFE: schema-aware normalization

Endpoints
- POST /v1/top10/investments
- POST /v1/top10/top-10-investments
- POST /v1/top10/top10
- GET  /v1/top10/health
"""

from __future__ import annotations

import copy
import json
import logging
import time
import uuid
from typing import Any, Dict, List, Mapping, Optional

from fastapi import APIRouter, Body, Header, HTTPException, Query, Request, status
from fastapi.encoders import jsonable_encoder

logger = logging.getLogger("routes.top10_investments")
logger.addHandler(logging.NullHandler())

TOP10_ROUTE_VERSION = "1.1.0"

router = APIRouter(prefix="/v1/top10", tags=["top10"])


# -----------------------------------------------------------------------------
# Auth (best-effort, consistent with other routers)
# -----------------------------------------------------------------------------
try:
    from core.config import auth_ok, get_settings_cached, is_open_mode  # type: ignore
except Exception:
    auth_ok = None  # type: ignore
    is_open_mode = None  # type: ignore

    def get_settings_cached(*args: Any, **kwargs: Any) -> Any:  # type: ignore
        return None


def _extract_auth_token(*, token_query: Optional[str], x_app_token: Optional[str], authorization: Optional[str]) -> str:
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


def _require_auth_or_401(*, token_query: Optional[str], x_app_token: Optional[str], authorization: Optional[str]) -> None:
    try:
        if callable(is_open_mode) and bool(is_open_mode()):
            return
    except Exception:
        pass

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
    try:
        st = getattr(request.app, "state", None)
        if st and getattr(st, "engine", None):
            return st.engine
    except Exception:
        pass

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


# -----------------------------------------------------------------------------
# Generic helpers
# -----------------------------------------------------------------------------
def _safe_int(v: Any, default: int) -> int:
    try:
        return int(float(v))
    except Exception:
        return default


def _coerce_bool(v: Any, default: bool = False) -> bool:
    if isinstance(v, bool):
        return v
    if isinstance(v, str):
        return v.strip().lower() in {"1", "true", "yes", "y", "on"}
    if isinstance(v, (int, float)):
        try:
            return bool(int(v))
        except Exception:
            return default
    return default


def _s(v: Any) -> str:
    try:
        if v is None:
            return ""
        return str(v).strip()
    except Exception:
        return ""


def _is_blank(v: Any) -> bool:
    return v is None or (isinstance(v, str) and not v.strip())


def _as_dict(v: Any) -> Dict[str, Any]:
    if isinstance(v, dict):
        return dict(v)
    if isinstance(v, Mapping):
        return dict(v)
    return {}


def _jsonable_snapshot(value: Any) -> Any:
    try:
        return jsonable_encoder(value)
    except Exception:
        try:
            return json.loads(json.dumps(value, default=str))
        except Exception:
            return str(value)


def _rows_to_matrix(rows: List[Dict[str, Any]], keys: List[str]) -> List[List[Any]]:
    return [[row.get(k) for k in keys] for row in rows]


def _flatten_criteria(body: Dict[str, Any]) -> Dict[str, Any]:
    """
    Accept criteria from:
      - body["criteria"] dict (preferred)
      - top-level keys (override)
    """
    crit: Dict[str, Any] = {}
    if isinstance(body.get("criteria"), dict):
        crit.update(body["criteria"])

    for k in (
        "pages_selected",
        "pages",
        "invest_period_days",
        "min_expected_roi",
        "max_risk_score",
        "min_confidence",
        "min_ai_confidence",
        "min_volume",
        "use_liquidity_tiebreak",
        "enforce_risk_confidence",
        "top_n",
        "enrich_final",
        "risk_level",
        "confidence_bucket",
        "horizon_days",
        "invest_period_label",
        "include_positions",
    ):
        if k in body and body.get(k) is not None:
            crit[k] = body.get(k)

    return crit


def _canonical_selection_reason(row: Dict[str, Any]) -> Optional[str]:
    """
    Builds a simple explanation when selector did not provide one.
    Keeps it concise and deterministic.
    """
    recommendation = _s(row.get("recommendation"))
    confidence_bucket = _s(row.get("confidence_bucket"))
    risk_bucket = _s(row.get("risk_bucket"))

    score_parts: List[str] = []
    for label, key in (
        ("overall", "overall_score"),
        ("opportunity", "opportunity_score"),
        ("value", "value_score"),
        ("quality", "quality_score"),
        ("momentum", "momentum_score"),
        ("growth", "growth_score"),
    ):
        val = row.get(key)
        if isinstance(val, (int, float)):
            score_parts.append(f"{label}={round(float(val), 2)}")

    roi_parts: List[str] = []
    for label, key in (
        ("1M", "expected_roi_1m"),
        ("3M", "expected_roi_3m"),
        ("12M", "expected_roi_12m"),
    ):
        val = row.get(key)
        if isinstance(val, (int, float)):
            roi_parts.append(f"{label} ROI={round(float(val) * 100, 2)}%")

    reason_parts: List[str] = []
    if recommendation:
        reason_parts.append(f"Recommendation={recommendation}")
    if confidence_bucket:
        reason_parts.append(f"Confidence={confidence_bucket}")
    if risk_bucket:
        reason_parts.append(f"Risk={risk_bucket}")
    if score_parts:
        reason_parts.append(", ".join(score_parts[:3]))
    if roi_parts:
        reason_parts.append(", ".join(roi_parts[:2]))

    if not reason_parts:
        return None
    return " | ".join(reason_parts)


def _rank_rows_in_order(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    If rank_overall/top10_rank are missing, assign deterministic ranks
    using current returned order.
    """
    out: List[Dict[str, Any]] = []
    for idx, row in enumerate(rows, start=1):
        r = dict(row)

        if _is_blank(r.get("top10_rank")):
            r["top10_rank"] = idx

        if _is_blank(r.get("rank_overall")):
            r["rank_overall"] = idx

        out.append(r)
    return out


def _apply_top10_field_backfill(
    rows: List[Dict[str, Any]],
    *,
    keys: List[str],
    criteria: Dict[str, Any],
) -> List[Dict[str, Any]]:
    """
    Backfill the three Top10-specific fields when selector returns them empty.
    """
    criteria_snapshot = _jsonable_snapshot(criteria) if criteria else None
    out: List[Dict[str, Any]] = []

    for idx, row in enumerate(rows, start=1):
        r = dict(row)

        if "top10_rank" in keys and _is_blank(r.get("top10_rank")):
            r["top10_rank"] = idx

        if "selection_reason" in keys and _is_blank(r.get("selection_reason")):
            r["selection_reason"] = _canonical_selection_reason(r)

        if "criteria_snapshot" in keys and _is_blank(r.get("criteria_snapshot")) and criteria_snapshot is not None:
            r["criteria_snapshot"] = criteria_snapshot

        out.append(r)

    return out


def _ensure_schema_projection(rows: List[Dict[str, Any]], keys: List[str]) -> List[Dict[str, Any]]:
    """
    Project rows into exact schema key order, while ensuring every key exists.
    """
    if not keys:
        return [dict(r) for r in rows if isinstance(r, dict)]

    normalized: List[Dict[str, Any]] = []
    for row in rows:
        if not isinstance(row, dict):
            continue
        normalized.append({k: row.get(k, None) for k in keys})
    return normalized


def _ensure_top10_keys_present(keys: List[str], headers: List[str]) -> tuple[List[str], List[str]]:
    """
    Guarantees the 3 Top10 extra keys exist if route is serving Top_10_Investments.
    This avoids accidental loss if selector payload is incomplete.
    """
    extras = [
        ("top10_rank", "Top10 Rank"),
        ("selection_reason", "Selection Reason"),
        ("criteria_snapshot", "Criteria Snapshot"),
    ]

    out_keys = list(keys or [])
    out_headers = list(headers or [])

    for key, header in extras:
        if key not in out_keys:
            out_keys.append(key)
            out_headers.append(header)

    return out_keys, out_headers


def _load_schema_defaults() -> tuple[List[str], List[str]]:
    """
    Best-effort fallback from schema_registry if selector does not return headers/keys.
    """
    try:
        from core.sheets.schema_registry import get_sheet_spec  # type: ignore

        spec = get_sheet_spec("Top_10_Investments")
        cols = getattr(spec, "columns", None) or []
        keys = [getattr(c, "key", "") for c in cols]
        headers = [getattr(c, "header", "") for c in cols]
        keys = [k for k in keys if isinstance(k, str) and k]
        headers = [h for h in headers if isinstance(h, str) and h]
        return headers, keys
    except Exception:
        return [], []


# -----------------------------------------------------------------------------
# Health
# -----------------------------------------------------------------------------
@router.get("/health")
async def top10_health(request: Request) -> Dict[str, Any]:
    engine = await _get_engine(request)
    return jsonable_encoder(
        {
            "status": "ok" if engine else "degraded",
            "version": TOP10_ROUTE_VERSION,
            "engine_available": bool(engine),
            "engine_type": type(engine).__name__ if engine else "none",
        }
    )


# -----------------------------------------------------------------------------
# Main endpoint
# -----------------------------------------------------------------------------
@router.post("/investments")
@router.post("/top-10-investments")
@router.post("/top10")
async def top10_investments(
    request: Request,
    body: Dict[str, Any] = Body(default_factory=dict),
    mode: str = Query(default="", description="Optional mode hint for engine/provider"),
    include_matrix: Optional[bool] = Query(default=None, description="Return rows_matrix for legacy clients"),
    limit: Optional[int] = Query(default=None, ge=1, le=50, description="How many items to return (1..50)"),
    token: Optional[str] = Query(default=None, description="Auth token (query only if allowed)"),
    x_app_token: Optional[str] = Header(default=None, alias="X-APP-TOKEN"),
    authorization: Optional[str] = Header(default=None, alias="Authorization"),
    x_request_id: Optional[str] = Header(default=None, alias="X-Request-ID"),
) -> Dict[str, Any]:
    """
    Dedicated Top_10_Investments page generator (schema-aligned + Top10 metadata aware).
    """
    t0 = time.perf_counter()
    _require_auth_or_401(token_query=token, x_app_token=x_app_token, authorization=authorization)
    request_id = x_request_id or getattr(request.state, "request_id", None) or str(uuid.uuid4())

    engine = await _get_engine(request)
    if engine is None:
        raise HTTPException(status_code=503, detail="Data engine unavailable")

    eff_limit = limit if isinstance(limit, int) else _safe_int(body.get("limit") or body.get("top_n") or 10, 10)
    eff_limit = max(1, min(50, int(eff_limit)))

    include_matrix_final = include_matrix if isinstance(include_matrix, bool) else _coerce_bool(body.get("include_matrix"), True)

    criteria = _flatten_criteria(body or {})
    criteria["top_n"] = eff_limit

    try:
        from core.analysis.top10_selector import build_top10_rows  # type: ignore
    except Exception as e:
        raise HTTPException(
            status_code=503,
            detail={"error": "Top10 selector unavailable", "detail": f"{type(e).__name__}: {e}"},
        )

    try:
        payload = await build_top10_rows(
            engine=engine,
            criteria=criteria,
            limit=eff_limit,
            mode=mode or "",
        )
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail={"error": "Top10 generation failed", "detail": f"{type(e).__name__}: {e}"},
        )

    if not isinstance(payload, dict):
        raise HTTPException(status_code=500, detail="Top10 generation returned invalid payload")

    payload = copy.deepcopy(payload)

    headers = payload.get("headers") or []
    keys = payload.get("keys") or []
    rows = payload.get("rows") or []

    if not isinstance(headers, list):
        headers = []
    if not isinstance(keys, list):
        keys = []
    if not isinstance(rows, list):
        rows = []

    # Fallback to canonical schema if selector omitted headers/keys
    if not headers or not keys:
        schema_headers, schema_keys = _load_schema_defaults()
        if not headers:
            headers = schema_headers
        if not keys:
            keys = schema_keys

    # Ensure Top10 extras are always included
    keys, headers = _ensure_top10_keys_present(keys, headers)

    dict_rows = [dict(r) for r in rows if isinstance(r, dict)]

    # Backfill special Top10 fields
    dict_rows = _apply_top10_field_backfill(
        dict_rows,
        keys=keys,
        criteria=criteria,
    )

    # Ensure general ranking fields are not blank when current order is final order
    dict_rows = _rank_rows_in_order(dict_rows)

    # Project to exact schema
    norm_rows = _ensure_schema_projection(dict_rows, keys)

    # hard cap
    norm_rows = norm_rows[:eff_limit]

    meta = payload.get("meta") if isinstance(payload.get("meta"), dict) else {}
    meta = dict(meta)
    meta.update(
        {
            "route_version": TOP10_ROUTE_VERSION,
            "request_id": request_id,
            "limit": eff_limit,
            "mode": mode or "",
            "duration_ms": round((time.perf_counter() - t0) * 1000.0, 3),
            "schema_aligned": bool(keys),
            "top10_fields_backfilled": True,
            "criteria_used": _jsonable_snapshot(criteria),
        }
    )

    return jsonable_encoder(
        {
            "status": payload.get("status") or "success",
            "page": "Top_10_Investments",
            "headers": headers,
            "keys": keys,
            "rows": norm_rows,
            "rows_matrix": _rows_to_matrix(norm_rows, keys) if (include_matrix_final and keys) else None,
            "version": TOP10_ROUTE_VERSION,
            "request_id": request_id,
            "meta": meta,
        }
    )


__all__ = ["router", "TOP10_ROUTE_VERSION"]
