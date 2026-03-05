#!/usr/bin/env python3
# routes/top10_investments.py
"""
================================================================================
Top 10 Investments Router — v1.1.0 (PHASE 6 / SCHEMA-DRIVEN / TOP10 EXTRAS GUARANTEED)
================================================================================

Endpoints:
  GET  /v1/analysis/top10
  POST /v1/analysis/top10   (criteria overrides)

What this revision fixes (prevents regression):
- ✅ Always returns Top_10_Investments schema from schema_registry (83 columns).
- ✅ Always populates the 3 Top10 extra fields when present in schema keys:
    - top10_rank
    - selection_reason
    - criteria_snapshot
  even if the upstream selector/builder forgets them.

Design:
- Startup-safe: no heavy imports at module import-time (top10 selector imported lazily).
- Schema-first: headers/keys order is authoritative from schema_registry.
- Best-effort engine: request.app.state.engine -> core.data_engine_v2.get_engine -> core.data_engine.get_engine
- Auth: core.config.is_open_mode/auth_ok when available; query token only if allowed.

================================================================================
"""

from __future__ import annotations

import json
import logging
import os
import time
import uuid
from dataclasses import asdict, is_dataclass
from typing import Any, Dict, List, Optional, Sequence, Tuple

from fastapi import APIRouter, Body, Header, HTTPException, Query, Request, status

# -----------------------------------------------------------------------------
# JSON response (orjson if available)
# -----------------------------------------------------------------------------
def _clean_nans(obj: Any) -> Any:
    try:
        import math as _m

        if isinstance(obj, dict):
            return {k: _clean_nans(v) for k, v in obj.items()}
        if isinstance(obj, list):
            return [_clean_nans(v) for v in obj]
        if isinstance(obj, float) and (_m.isnan(obj) or _m.isinf(obj)):
            return None
    except Exception:
        pass
    return obj


try:
    from fastapi.responses import ORJSONResponse as BestJSONResponse  # type: ignore

    def _json_response(content: Any, status_code: int = 200) -> BestJSONResponse:
        return BestJSONResponse(status_code=status_code, content=_clean_nans(content))

except Exception:
    from fastapi.responses import JSONResponse as BestJSONResponse  # type: ignore

    def _json_response(content: Any, status_code: int = 200) -> BestJSONResponse:
        return BestJSONResponse(status_code=status_code, content=_clean_nans(content))


logger = logging.getLogger("routes.top10_investments")

ROUTER_VERSION = "1.1.0"
router = APIRouter(prefix="/v1/analysis", tags=["AI & Analysis"])

_TRUTHY = {"1", "true", "yes", "y", "on", "t"}


# -----------------------------------------------------------------------------
# Auth (align to core.config)
# -----------------------------------------------------------------------------
def _request_id(request: Request, x_request_id: Optional[str]) -> str:
    try:
        rid = getattr(request.state, "request_id", None)
        if rid:
            return str(rid)
    except Exception:
        pass
    return x_request_id or str(uuid.uuid4())[:18]


def _allow_query_token(settings: Any, request: Request) -> bool:
    # Settings-first
    try:
        if settings is not None:
            return bool(getattr(settings, "allow_query_token", False))
    except Exception:
        pass

    # Env fallback
    try:
        v = (os.getenv("ALLOW_QUERY_TOKEN", "") or "").strip().lower()
        if v in _TRUTHY:
            return True
    except Exception:
        pass

    # Optional debug header override (kept minimal)
    try:
        hv = (request.headers.get("X-Allow-Query-Token") or "").strip().lower()
        if hv in _TRUTHY:
            return True
    except Exception:
        pass

    return False


def _extract_auth_token(
    *,
    token_q: Optional[str],
    x_app_token: Optional[str],
    authorization: Optional[str],
    settings: Any,
    request: Request,
) -> str:
    # Bearer preferred
    if authorization:
        a = authorization.strip()
        if a.lower().startswith("bearer "):
            return a.split(" ", 1)[1].strip()
        # If someone sends raw token in Authorization (rare), accept it
        if a:
            return a

    # Header token
    if x_app_token and x_app_token.strip():
        return x_app_token.strip()

    # Query token only if allowed
    if token_q and token_q.strip() and _allow_query_token(settings, request):
        return token_q.strip()

    return ""


def _auth_ok(
    request: Request,
    *,
    token_q: Optional[str],
    x_app_token: Optional[str],
    authorization: Optional[str],
) -> bool:
    try:
        from core.config import is_open_mode, auth_ok, get_settings_cached  # type: ignore

        if callable(is_open_mode) and is_open_mode():
            return True

        settings = get_settings_cached() if callable(get_settings_cached) else None
        tok = _extract_auth_token(token_q=token_q, x_app_token=x_app_token, authorization=authorization, settings=settings, request=request)

        if callable(auth_ok):
            return bool(auth_ok(token=tok, authorization=authorization, headers={"X-APP-TOKEN": x_app_token, "Authorization": authorization}))
        return False
    except Exception:
        # safest fallback: if REQUIRE_AUTH is off -> allow, else require *some* token
        require = (os.getenv("REQUIRE_AUTH", "true") or "true").strip().lower() in _TRUTHY
        if not require:
            return True
        tok = (x_app_token or "").strip()
        if authorization and authorization.strip().lower().startswith("bearer "):
            tok = authorization.strip().split(" ", 1)[1].strip()
        return bool(tok)


# -----------------------------------------------------------------------------
# Engine (lazy)
# -----------------------------------------------------------------------------
async def _maybe_await(v: Any) -> Any:
    if hasattr(v, "__await__"):
        return await v
    return v


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
        err_v2 = f"{type(e).__name__}: {e}"

    # 3) core.data_engine.get_engine
    try:
        from core.data_engine import get_engine as get_engine_legacy  # type: ignore

        eng = await _maybe_await(get_engine_legacy())
        try:
            request.app.state.engine = eng
        except Exception:
            pass
        return eng, "core.data_engine.get_engine", None
    except Exception as e2:
        return None, "engine_init_failed", f"{err_v2} | legacy: {type(e2).__name__}: {e2}"


# -----------------------------------------------------------------------------
# Schema (Top_10_Investments)
# -----------------------------------------------------------------------------
def _get_top10_schema() -> Tuple[List[str], List[str], Any]:
    try:
        from core.sheets.schema_registry import get_sheet_spec  # type: ignore

        spec = get_sheet_spec("Top_10_Investments")
        cols = getattr(spec, "columns", None) or []
        headers = [str(getattr(c, "header", "")) for c in cols if getattr(c, "header", None)]
        keys = [str(getattr(c, "key", "")) for c in cols if getattr(c, "key", None)]
        if not headers or not keys or len(headers) != len(keys):
            raise ValueError(f"Invalid Top_10_Investments spec (headers={len(headers)}, keys={len(keys)})")
        return headers, keys, spec
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail={
                "error": "schema_registry unavailable or invalid for Top_10_Investments",
                "detail": str(e),
                "page": "Top_10_Investments",
            },
        )


# -----------------------------------------------------------------------------
# Top10 selector (lazy import)
# -----------------------------------------------------------------------------
def _load_top10_module() -> Any:
    """
    Lazily import to avoid startup blockers if the selector has optional deps.
    """
    try:
        import importlib

        return importlib.import_module("core.analysis.top10_selector")
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail={"error": "Top10 selector unavailable", "module": "core.analysis.top10_selector", "detail": f"{type(e).__name__}: {e}"},
        )


def _criteria_snapshot(criteria: Any) -> Dict[str, Any]:
    """
    Convert criteria object into JSON-safe dict for criteria_snapshot.
    """
    if criteria is None:
        return {}
    try:
        if is_dataclass(criteria):
            return asdict(criteria)
    except Exception:
        pass
    if isinstance(criteria, dict):
        return dict(criteria)
    out: Dict[str, Any] = {}
    for k in (
        "pages_selected",
        "invest_period_days",
        "risk_level",
        "confidence_level",
        "min_expected_roi",
        "max_risk_score",
        "min_confidence",
        "enforce_risk_confidence",
        "top_n",
        "enrich_final",
    ):
        try:
            if hasattr(criteria, k):
                out[k] = getattr(criteria, k)
        except Exception:
            continue
    # derive horizon if method exists
    try:
        h = getattr(criteria, "horizon", None)
        if callable(h):
            out["horizon"] = h()
    except Exception:
        pass
    return out


def _json_str(obj: Any) -> str:
    try:
        return json.dumps(obj, ensure_ascii=False, default=str)
    except Exception:
        return str(obj)


def _ensure_top10_extras(
    *,
    rows: List[Dict[str, Any]],
    keys: Sequence[str],
    criteria: Any,
) -> None:
    """
    Guarantees Top10 Rank / Selection Reason / Criteria Snapshot fields exist when in schema keys.
    """
    need_rank = "top10_rank" in keys
    need_reason = "selection_reason" in keys
    need_snap = "criteria_snapshot" in keys

    if not (need_rank or need_reason or need_snap):
        return

    snap_json = _json_str(_criteria_snapshot(criteria)) if need_snap else None

    for i, r in enumerate(rows):
        if need_rank and (r.get("top10_rank") is None or r.get("top10_rank") == ""):
            r["top10_rank"] = i + 1

        if need_reason and (r.get("selection_reason") is None or r.get("selection_reason") == ""):
            # Try common sources, then fallback
            reason = (
                r.get("selection_reason")
                or r.get("recommendation_reason")
                or r.get("reason")
                or r.get("notes")
                or ""
            )
            if not reason:
                # derive a minimal reason from ROI/score if present
                roi = r.get("expected_roi_3m")
                score = r.get("overall_score")
                risk = r.get("risk_score")
                bits = []
                if roi is not None:
                    bits.append(f"ROI3M={roi}")
                if score is not None:
                    bits.append(f"Score={score}")
                if risk is not None:
                    bits.append(f"Risk={risk}")
                reason = "Selected by Top10 model" + (f" ({', '.join(bits)})" if bits else "")
            r["selection_reason"] = reason

        if need_snap and (r.get("criteria_snapshot") is None or r.get("criteria_snapshot") == ""):
            r["criteria_snapshot"] = snap_json


def _normalize_rows_to_schema(
    *,
    page: str,
    keys: Sequence[str],
    raw_rows: List[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    """
    Prefer core.data_engine_v2.normalize_row_to_schema if available; otherwise do projection.
    """
    normalize_fn = None
    try:
        from core.data_engine_v2 import normalize_row_to_schema as _n  # type: ignore

        normalize_fn = _n
    except Exception:
        normalize_fn = None

    out: List[Dict[str, Any]] = []
    for rr in raw_rows:
        r = dict(rr or {})
        if callable(normalize_fn):
            try:
                r = normalize_fn(page, r, keep_extras=True)
            except Exception:
                pass
        # enforce keys existence + order
        fixed = {k: r.get(k, None) for k in keys}
        # keep extras (harmless, but do not affect matrix)
        for k, v in r.items():
            if k not in fixed:
                fixed[k] = v
        out.append(fixed)
    return out


# -----------------------------------------------------------------------------
# Routes
# -----------------------------------------------------------------------------
@router.get("/top10")
async def get_top10(
    request: Request,
    token: Optional[str] = Query(default=None, description="Auth token (query only if allowed)"),
    include_matrix: bool = Query(default=True, description="Include rows_matrix for legacy clients"),
    x_app_token: Optional[str] = Header(default=None, alias="X-APP-TOKEN"),
    authorization: Optional[str] = Header(default=None, alias="Authorization"),
    x_request_id: Optional[str] = Header(default=None, alias="X-Request-ID"),
) -> BestJSONResponse:
    t0 = time.time()
    rid = _request_id(request, x_request_id)

    if not _auth_ok(request, token_q=token, x_app_token=x_app_token, authorization=authorization):
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="unauthorized")

    engine, engine_src, engine_err = await _get_engine(request)
    if not engine:
        return _json_response(
            {
                "status": "error",
                "error": "Data engine unavailable",
                "engine_source": engine_src,
                "engine_error": engine_err,
                "version": ROUTER_VERSION,
                "request_id": rid,
                "meta": {"duration_ms": (time.time() - t0) * 1000.0},
            },
            status_code=503,
        )

    headers, keys, _spec = _get_top10_schema()

    mod = _load_top10_module()

    # criteria + selection
    load_criteria_best_effort = getattr(mod, "load_criteria_best_effort", None)
    select_top10 = getattr(mod, "select_top10", None)
    build_top10_output_rows = getattr(mod, "build_top10_output_rows", None)

    if not callable(load_criteria_best_effort) or not callable(select_top10) or not callable(build_top10_output_rows):
        return _json_response(
            {
                "status": "error",
                "error": "Top10 selector contract missing required functions",
                "required": ["load_criteria_best_effort", "select_top10", "build_top10_output_rows"],
                "module": "core.analysis.top10_selector",
                "version": ROUTER_VERSION,
                "request_id": rid,
                "meta": {"duration_ms": (time.time() - t0) * 1000.0},
            },
            status_code=503,
        )

    criteria = load_criteria_best_effort(engine)
    top, meta = await _maybe_await(select_top10(engine=engine, criteria=criteria))
    raw_rows = build_top10_output_rows(top, horizon=getattr(criteria, "horizon", lambda: None)())

    if not isinstance(raw_rows, list):
        raw_rows = []

    # Normalize to schema + ensure extras
    rows = _normalize_rows_to_schema(page="Top_10_Investments", keys=keys, raw_rows=[r for r in raw_rows if isinstance(r, dict)])
    _ensure_top10_extras(rows=rows, keys=keys, criteria=criteria)

    payload = {
        "status": "success",
        "page": "Top_10_Investments",
        "headers": headers,
        "keys": list(keys),
        "rows": rows,
        "rows_matrix": [[row.get(k) for k in keys] for row in rows] if include_matrix else None,
        "criteria": _criteria_snapshot(criteria),
        "version": ROUTER_VERSION,
        "request_id": rid,
        "meta": {
            **(meta or {}),
            "duration_ms": (time.time() - t0) * 1000.0,
            "engine_source": engine_src,
            "schema_cols": len(keys),
            "top10_extras_enforced": True,
        },
    }
    return _json_response(payload, status_code=200)


@router.post("/top10")
async def post_top10(
    request: Request,
    body: Dict[str, Any] = Body(default_factory=dict),
    token: Optional[str] = Query(default=None, description="Auth token (query only if allowed)"),
    include_matrix: bool = Query(default=True, description="Include rows_matrix for legacy clients"),
    x_app_token: Optional[str] = Header(default=None, alias="X-APP-TOKEN"),
    authorization: Optional[str] = Header(default=None, alias="Authorization"),
    x_request_id: Optional[str] = Header(default=None, alias="X-Request-ID"),
) -> BestJSONResponse:
    t0 = time.time()
    rid = _request_id(request, x_request_id)

    if not _auth_ok(request, token_q=token, x_app_token=x_app_token, authorization=authorization):
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="unauthorized")

    engine, engine_src, engine_err = await _get_engine(request)
    if not engine:
        return _json_response(
            {
                "status": "error",
                "error": "Data engine unavailable",
                "engine_source": engine_src,
                "engine_error": engine_err,
                "version": ROUTER_VERSION,
                "request_id": rid,
                "meta": {"duration_ms": (time.time() - t0) * 1000.0},
            },
            status_code=503,
        )

    headers, keys, _spec = _get_top10_schema()

    mod = _load_top10_module()

    load_criteria_best_effort = getattr(mod, "load_criteria_best_effort", None)
    merge_criteria_overrides = getattr(mod, "merge_criteria_overrides", None)
    select_top10 = getattr(mod, "select_top10", None)
    build_top10_output_rows = getattr(mod, "build_top10_output_rows", None)

    if not callable(load_criteria_best_effort) or not callable(select_top10) or not callable(build_top10_output_rows):
        return _json_response(
            {
                "status": "error",
                "error": "Top10 selector contract missing required functions",
                "required": ["load_criteria_best_effort", "select_top10", "build_top10_output_rows"],
                "module": "core.analysis.top10_selector",
                "version": ROUTER_VERSION,
                "request_id": rid,
                "meta": {"duration_ms": (time.time() - t0) * 1000.0},
            },
            status_code=503,
        )

    base = load_criteria_best_effort(engine)
    if callable(merge_criteria_overrides):
        criteria = merge_criteria_overrides(base, body or {})
    else:
        # Best-effort override if merge fn not present
        criteria = base
        if isinstance(body, dict):
            for k, v in body.items():
                try:
                    if hasattr(criteria, k):
                        setattr(criteria, k, v)
                except Exception:
                    continue

    top, meta = await _maybe_await(select_top10(engine=engine, criteria=criteria))
    raw_rows = build_top10_output_rows(top, horizon=getattr(criteria, "horizon", lambda: None)())

    if not isinstance(raw_rows, list):
        raw_rows = []

    rows = _normalize_rows_to_schema(page="Top_10_Investments", keys=keys, raw_rows=[r for r in raw_rows if isinstance(r, dict)])
    _ensure_top10_extras(rows=rows, keys=keys, criteria=criteria)

    payload = {
        "status": "success",
        "page": "Top_10_Investments",
        "headers": headers,
        "keys": list(keys),
        "rows": rows,
        "rows_matrix": [[row.get(k) for k in keys] for row in rows] if include_matrix else None,
        "criteria": _criteria_snapshot(criteria),
        "version": ROUTER_VERSION,
        "request_id": rid,
        "meta": {
            **(meta or {}),
            "duration_ms": (time.time() - t0) * 1000.0,
            "engine_source": engine_src,
            "schema_cols": len(keys),
            "top10_extras_enforced": True,
        },
    }
    return _json_response(payload, status_code=200)


__all__ = ["router"]
