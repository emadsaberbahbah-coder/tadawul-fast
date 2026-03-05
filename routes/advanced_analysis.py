#!/usr/bin/env python3
"""
routes/advanced_analysis.py
------------------------------------------------------------
TADAWUL ADVANCED ANALYSIS ROUTER — v5.8.0 (INSIGHTS-FIX / STARTUP-SAFE / SCHEMA-FIRST / JSON-SAFE)

Key Fix (Phase B / Script #3):
- ✅ /v1/advanced/insights-analysis now calls the REAL builder:
      core/analysis/insights_builder.py  (build_insights_analysis_rows)
  and ALWAYS returns meaningful rows (even when symbols=[]), using auto-universe.

Also improved:
- ✅ /v1/advanced/sheet-rows when sheet=Insights_Analysis delegates to the builder (no more shell rows)
- ✅ Schema-first for headers/keys; special sheets never fall back to 80-col
- ✅ JSON-safe responses everywhere (health/stats/payloads coerced)
- ✅ Import-time safe: no network calls; heavy imports inside handlers
"""

from __future__ import annotations

import json
import logging
import os
import time
import uuid
from dataclasses import asdict, is_dataclass
from datetime import date, datetime, timezone
from enum import Enum
from typing import Any, Dict, List, Optional, Sequence, Tuple

from fastapi import Body, Header, HTTPException, Query, Request, Response, status
from fastapi.encoders import jsonable_encoder
from fastapi.routing import APIRouter

logger = logging.getLogger("routes.advanced_analysis")
ADVANCED_ANALYSIS_VERSION = "5.8.0"

# -----------------------------------------------------------------------------
# Router: Prefer base router if available, otherwise create a local one.
# This avoids import-time crashes (missing module / circular import).
# -----------------------------------------------------------------------------
_base_router_import_error: Optional[str] = None
try:
    from routes.advanced_sheet_rows import router as router  # type: ignore  # noqa: F401
except Exception as e:
    _base_router_import_error = f"{type(e).__name__}: {e}"
    router = APIRouter(prefix="/v1/advanced", tags=["advanced"])

# -----------------------------------------------------------------------------
# Optional Prometheus (safe)
# -----------------------------------------------------------------------------
try:
    from prometheus_client import CONTENT_TYPE_LATEST, generate_latest  # type: ignore

    _PROMETHEUS_AVAILABLE = True
except Exception:
    generate_latest = None  # type: ignore
    CONTENT_TYPE_LATEST = "text/plain"
    _PROMETHEUS_AVAILABLE = False

# -----------------------------------------------------------------------------
# core.config preferred for auth + flags, but must be safe if unavailable
# -----------------------------------------------------------------------------
try:
    from core.config import auth_ok, get_settings_cached  # type: ignore
except Exception:
    auth_ok = None  # type: ignore

    def get_settings_cached(*args: Any, **kwargs: Any) -> Any:  # type: ignore
        return None


# -----------------------------------------------------------------------------
# JSON-safety helpers (prevents serialization failures)
# -----------------------------------------------------------------------------
def _to_jsonable(obj: Any) -> Any:
    """Best-effort conversion to JSON-safe primitives."""
    try:
        return jsonable_encoder(obj)
    except Exception:
        pass

    if obj is None or isinstance(obj, (str, int, float, bool)):
        return obj
    if isinstance(obj, Enum):
        return obj.value
    if isinstance(obj, (datetime, date)):
        try:
            return obj.isoformat()
        except Exception:
            return str(obj)
    if is_dataclass(obj):
        try:
            return {k: _to_jsonable(v) for k, v in asdict(obj).items()}
        except Exception:
            return str(obj)
    if isinstance(obj, dict):
        return {str(k): _to_jsonable(v) for k, v in obj.items()}
    if isinstance(obj, (list, tuple, set)):
        return [_to_jsonable(v) for v in obj]
    try:
        # pydantic
        if hasattr(obj, "model_dump"):
            return _to_jsonable(obj.model_dump(mode="python"))  # type: ignore
        if hasattr(obj, "dict"):
            return _to_jsonable(obj.dict())  # type: ignore
    except Exception:
        pass
    return str(obj)


def _json_dumps_safe(obj: Any) -> str:
    try:
        return json.dumps(obj, ensure_ascii=False, default=str)
    except Exception:
        return str(obj)


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


def _safe_engine_type(engine: Any) -> str:
    try:
        return type(engine).__name__
    except Exception:
        return "unknown"


def _safe_env_port() -> Optional[str]:
    p = (os.getenv("PORT") or "").strip()
    return p or None


async def _maybe_call(obj: Any, name: str, *args: Any, **kwargs: Any) -> Optional[Any]:
    try:
        fn = getattr(obj, name, None)
        if not callable(fn):
            return None
        out = fn(*args, **kwargs)
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
# Auth helper (best-effort)
# -----------------------------------------------------------------------------
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
# Criteria snapshot builder (safe)
# -----------------------------------------------------------------------------
def _build_insights_criteria_snapshot() -> Dict[str, Any]:
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
                "confidence_penalty_strength": float(
                    getattr(dw, "confidence_penalty_strength", weights["confidence_penalty_strength"])
                ),
                "source": "core.scoring.DEFAULT_WEIGHTS",
            }
    except Exception:
        pass

    rules = [
        {"rule": "Low confidence gate", "when": "confidence_score < 45", "then": "HOLD"},
        {"rule": "High risk + moderate score", "when": "risk_score >= 75 AND overall_score < 75", "then": "REDUCE"},
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
        {"rule": "Buy (score-based fallback)", "when": "overall_score >= 82 AND risk_score <= 55", "then": "BUY"},
        {"rule": "Hold (moderate)", "when": "overall_score >= 65", "then": "HOLD"},
        {"rule": "Reduce (weak)", "when": "overall_score >= 50", "then": "REDUCE"},
        {"rule": "Sell (very weak)", "when": "overall_score < 50", "then": "SELL"},
    ]

    return {
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "weights": weights,
        "rules": rules,
    }


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
# Schema helpers (lazy)
# -----------------------------------------------------------------------------
def _canonicalize_sheet_name(sheet: str) -> str:
    s = (sheet or "").strip()
    if not s:
        return s

    for fn_name in ("normalize_page_name", "resolve_page", "canonicalize_page"):
        try:
            mod = __import__("core.sheets.page_catalog", fromlist=[fn_name])
            fn = getattr(mod, fn_name, None)
            if callable(fn):
                try:
                    out = fn(s, allow_output_pages=True)
                except TypeError:
                    out = fn(s)
                if isinstance(out, str) and out.strip():
                    return out.strip()
        except Exception:
            continue

    return s.replace(" ", "_")


def _get_sheet_spec(sheet: str) -> Optional[Any]:
    try:
        from core.sheets.schema_registry import get_sheet_spec  # type: ignore

        return get_sheet_spec(sheet)
    except Exception:
        return None


def _schema_headers_keys(sheet: str) -> Tuple[List[str], List[str], str]:
    """
    IMPORTANT: headers and keys MUST stay aligned.
    We only include a column if BOTH header and key are present.
    """
    spec = _get_sheet_spec(sheet)
    if spec and getattr(spec, "columns", None):
        headers: List[str] = []
        keys: List[str] = []
        for c in spec.columns:
            h = str(getattr(c, "header", "") or "").strip()
            k = str(getattr(c, "key", "") or "").strip()
            if not h or not k:
                continue
            headers.append(h)
            keys.append(k)
        if headers and keys and len(headers) == len(keys):
            return headers, keys, "schema_registry.get_sheet_spec"
    return [], [], "none"


def _normalize_row_to_schema(sheet: str, row: Dict[str, Any]) -> Dict[str, Any]:
    spec = _get_sheet_spec(sheet)
    if not spec or not getattr(spec, "columns", None):
        return row

    try:
        from core.data_engine_v2 import normalize_row_to_schema  # type: ignore

        out = normalize_row_to_schema(sheet, row)
        return out if isinstance(out, dict) else row
    except Exception:
        out2: Dict[str, Any] = {}
        for c in spec.columns:
            k = str(getattr(c, "key", "") or "").strip()
            h = str(getattr(c, "header", "") or "").strip()
            if not k:
                continue
            if k in row:
                out2[k] = row.get(k)
            elif h and h in row:
                out2[k] = row.get(h)
            else:
                out2[k] = None
        return out2


def _project_row(keys: Sequence[str], row: Dict[str, Any]) -> Dict[str, Any]:
    kk = [str(k) for k in keys]
    return {k: row.get(k, None) for k in kk}


def _rows_to_matrix(rows: List[Dict[str, Any]], keys: Sequence[str]) -> List[List[Any]]:
    kk = [str(k) for k in keys]
    return [[r.get(k) for k in kk] for r in rows]


def _matrix_to_rows(rows_matrix: List[List[Any]], keys: Sequence[str]) -> List[Dict[str, Any]]:
    kk = [str(k) for k in keys]
    out: List[Dict[str, Any]] = []
    for r in rows_matrix or []:
        if not isinstance(r, list):
            continue
        out.append({kk[i]: (r[i] if i < len(r) else None) for i in range(len(kk))})
    return out


def _coerce_payload_to_rows(payload: Any) -> Tuple[List[Dict[str, Any]], List[str], List[str]]:
    if payload is None:
        return [], [], []

    if isinstance(payload, tuple) and len(payload) == 2:
        hdrs, rows = payload
        hdrs_list = [str(x) for x in (hdrs or [])] if isinstance(hdrs, (list, tuple)) else []
        if isinstance(rows, list):
            if rows and isinstance(rows[0], dict):
                return [dict(r) for r in rows if isinstance(r, dict)], hdrs_list, hdrs_list[:]
            if rows and isinstance(rows[0], list):
                keys = hdrs_list[:]
                return _matrix_to_rows(rows, keys), hdrs_list, keys
        return [], hdrs_list, hdrs_list[:]

    if isinstance(payload, list):
        rows = [dict(r) for r in payload if isinstance(r, dict)]
        keys = list(rows[0].keys()) if rows else []
        headers = keys[:]
        return rows, headers, keys

    if isinstance(payload, dict):
        if isinstance(payload.get("data"), dict):
            return _coerce_payload_to_rows(payload.get("data"))

        headers = payload.get("headers") or payload.get("columns") or payload.get("fields") or []
        keys = payload.get("keys") or payload.get("schema") or payload.get("fields") or []
        rows = payload.get("rows") or payload.get("items") or payload.get("records") or payload.get("data") or []

        headers_list = [str(x) for x in headers] if isinstance(headers, list) else []
        keys_list = [str(x) for x in keys] if isinstance(keys, list) else []

        if isinstance(rows, list) and rows:
            if isinstance(rows[0], dict):
                if not keys_list:
                    keys_list = list(rows[0].keys())
                if not headers_list:
                    headers_list = keys_list[:]
                return [dict(r) for r in rows if isinstance(r, dict)], headers_list, keys_list
            if isinstance(rows[0], list):
                use_keys = keys_list or headers_list
                return _matrix_to_rows(rows, use_keys), (headers_list or use_keys[:]), use_keys[:]

        if not keys_list and headers_list:
            keys_list = headers_list[:]
        return [], headers_list, keys_list

    try:
        if hasattr(payload, "model_dump"):
            return _coerce_payload_to_rows(payload.model_dump(mode="python"))  # type: ignore
        if hasattr(payload, "dict"):
            return _coerce_payload_to_rows(payload.dict())  # type: ignore
    except Exception:
        pass

    return [], [], []


async def _engine_fetch_any(engine: Any, *, sheet: str, limit: int, offset: int, mode: str, body: Dict[str, Any]) -> Any:
    snap = await _maybe_call(engine, "get_cached_sheet_snapshot", sheet)
    if isinstance(snap, dict) and ("headers" in snap or "rows" in snap):
        if isinstance(snap.get("rows"), list):
            rows = snap.get("rows") or []
            snap["rows"] = rows[offset : offset + limit]
        return snap

    candidates = [
        ("get_sheet_rows", dict(sheet=sheet, limit=limit, offset=offset, mode=mode, body=body)),
        ("get_sheet_rows", dict(sheet=sheet, limit=limit, offset=offset, mode=mode)),
        ("get_sheet_rows", dict(sheet_name=sheet, limit=limit, offset=offset, mode=mode, body=body)),
        ("sheet_rows", dict(sheet=sheet, limit=limit, offset=offset, mode=mode, body=body)),
        ("sheet_rows", dict(sheet=sheet, limit=limit, offset=offset, mode=mode)),
        ("build_sheet_rows", dict(sheet=sheet, limit=limit, offset=offset, mode=mode, body=body)),
        ("build_sheet_rows", dict(sheet=sheet, limit=limit, offset=offset, mode=mode)),
        ("get_cached_sheet_rows", dict(sheet_name=sheet)),
        ("get_sheet_rows", dict(sheet_name=sheet)),
        ("get_sheet", dict(sheet_name=sheet)),
    ]

    last_err: Optional[Exception] = None
    for name, kwargs in candidates:
        fn = getattr(engine, name, None)
        if not callable(fn):
            continue
        try:
            out = fn(**kwargs)
            if hasattr(out, "__await__"):
                out = await out
            return out
        except TypeError as e:
            last_err = e
            continue
        except Exception as e:
            last_err = e
            continue

    if last_err is not None:
        raise last_err
    raise RuntimeError("Engine has no supported sheet-rows method")


# -----------------------------------------------------------------------------
# Insights helpers (route must call the real builder)
# -----------------------------------------------------------------------------
def _extract_criteria_from_body(body: Dict[str, Any]) -> Dict[str, Any]:
    """
    Collect criteria from:
      - body["criteria"] dict (preferred), AND/OR
      - top-level keys matching schema_registry criteria_fields
    """
    crit: Dict[str, Any] = {}
    if isinstance(body.get("criteria"), dict):
        crit.update(body["criteria"])

    # pull schema-defined criteria keys (best-effort)
    try:
        from core.sheets.schema_registry import get_sheet_spec  # type: ignore

        spec = get_sheet_spec("Insights_Analysis")
        cfs = getattr(spec, "criteria_fields", None) or ()
        for cf in cfs:
            k = str(getattr(cf, "key", "") or "").strip()
            if not k:
                continue
            if k in body and body.get(k) is not None:
                crit[k] = body.get(k)
    except Exception:
        # fallback minimal list
        for k in (
            "risk_level",
            "confidence_level",
            "invest_period_days",
            "required_return_pct",
            "amount",
            "pages_selected",
            "min_expected_roi_pct",
            "max_risk_score",
            "min_ai_confidence",
        ):
            if k in body and body.get(k) is not None:
                crit[k] = body.get(k)

    return crit


def _extract_universes_from_body(body: Dict[str, Any]) -> Dict[str, List[str]]:
    """
    Universes may be provided as:
      - body["universes"] = { "Section Name": [symbols...] }
    """
    out: Dict[str, List[str]] = {}
    v = body.get("universes")
    if isinstance(v, dict):
        for name, seq in v.items():
            if not name:
                continue
            if isinstance(seq, list):
                syms = [str(x).strip() for x in seq if str(x).strip()]
                if syms:
                    out[str(name).strip()] = syms
    return out


async def _build_insights_payload(
    *,
    request: Request,
    body: Dict[str, Any],
    mode: str,
    include_matrix: bool,
    limit: int,
    offset: int,
) -> Dict[str, Any]:
    """
    Calls core.analysis.insights_builder.build_insights_analysis_rows and returns schema-projected rows.
    ALWAYS meaningful:
      - if symbols empty, builder auto-universe is used (when engine exists)
      - if engine missing, builder still returns criteria/system/summary rows
    """
    t0 = time.perf_counter()
    headers, keys, schema_source = _schema_headers_keys("Insights_Analysis")

    # Always have *some* schema keys; fallback if registry is missing
    if not headers or not keys:
        # minimal 7-col fallback (matches builder fallback)
        keys = ["section", "item", "symbol", "metric", "value", "notes", "last_updated_riyadh"]
        headers = ["Section", "Item", "Symbol", "Metric", "Value", "Notes", "Last Updated (Riyadh)"]
        schema_source = "fallback"

    engine = await _get_engine(request)

    crit = _extract_criteria_from_body(body)
    universes = _extract_universes_from_body(body)
    symbols = _get_list(body, "symbols", "tickers", "tickers_list")

    # Call the REAL builder
    try:
        from core.analysis.insights_builder import build_insights_analysis_rows  # type: ignore

        res = build_insights_analysis_rows(
            engine=engine,
            criteria=crit or None,
            universes=universes or None,
            symbols=symbols or None,
            mode=mode or "",
            include_criteria_rows=True,
            include_system_rows=True,
            auto_universe_when_empty=True,
            include_top10_section=True,
            include_portfolio_kpis=True,
        )
        if hasattr(res, "__await__"):
            res = await res  # type: ignore[misc]
    except Exception as e:
        # even on builder failure, return meaningful diagnostic rows (schema-correct)
        diag_rows = [
            _project_row(
                keys,
                _normalize_row_to_schema(
                    "Insights_Analysis",
                    {
                        "section": "System",
                        "item": "Builder Error",
                        "symbol": "",
                        "metric": "insights_builder_error",
                        "value": f"{type(e).__name__}",
                        "notes": str(e),
                        "last_updated_riyadh": datetime.now().isoformat(),
                    },
                ),
            )
        ]
        diag_rows = diag_rows[offset : offset + limit]
        return _to_jsonable(
            {
                "status": "degraded",
                "page": "Insights_Analysis",
                "sheet": "Insights_Analysis",
                "headers": headers,
                "keys": keys,
                "rows": diag_rows,
                "rows_matrix": _rows_to_matrix(diag_rows, keys) if include_matrix else None,
                "version": ADVANCED_ANALYSIS_VERSION,
                "meta": {
                    "schema_source": schema_source,
                    "engine_available": bool(engine),
                    "duration_ms": (time.perf_counter() - t0) * 1000.0,
                },
            }
        )

    # Normalize builder output
    payload = res if isinstance(res, dict) else {"rows": res}
    rows_raw = payload.get("rows") if isinstance(payload, dict) else None
    if not isinstance(rows_raw, list):
        rows_raw = []

    rows_norm: List[Dict[str, Any]] = []
    for r in rows_raw:
        if not isinstance(r, dict):
            continue
        rr = _normalize_row_to_schema("Insights_Analysis", dict(r))
        rows_norm.append(_project_row(keys, rr))

    rows_norm = rows_norm[offset : offset + limit]

    return _to_jsonable(
        {
            "status": "success",
            "page": "Insights_Analysis",
            "sheet": "Insights_Analysis",
            "headers": headers,
            "keys": keys,
            "rows": rows_norm,
            "rows_matrix": _rows_to_matrix(rows_norm, keys) if include_matrix else None,
            "version": ADVANCED_ANALYSIS_VERSION,
            "meta": {
                "schema_source": schema_source,
                "engine_available": bool(engine),
                "engine_type": _safe_engine_type(engine) if engine else "none",
                "builder_meta": payload.get("meta") if isinstance(payload, dict) else None,
                "duration_ms": (time.perf_counter() - t0) * 1000.0,
            },
        }
    )


# -----------------------------------------------------------------------------
# Replace base POST /sheet-rows safely (no duplicates)
# -----------------------------------------------------------------------------
def _remove_router_route_inplace(rtr: APIRouter, path: str, method: str) -> int:
    removed = 0
    try:
        routes = list(getattr(rtr, "routes", []) or [])
        new_routes = []
        for r in routes:
            try:
                rp = getattr(r, "path", None)
                rm = getattr(r, "methods", None) or set()
                if rp == path and method.upper() in set(rm):
                    removed += 1
                    continue
            except Exception:
                pass
            new_routes.append(r)
        try:
            rtr.routes[:] = new_routes  # type: ignore[attr-defined]
        except Exception:
            setattr(rtr, "routes", new_routes)
    except Exception:
        return removed
    return removed


_removed = _remove_router_route_inplace(router, "/sheet-rows", "POST")
if _removed:
    logger.info("AdvancedAnalysis: removed %s existing POST /sheet-rows route(s) to replace schema-driven handler.", _removed)


@router.post("/sheet-rows")
async def advanced_sheet_rows_schema_driven(
    request: Request,
    body: Dict[str, Any] = Body(...),
    mode: str = Query(default="", description="Optional mode hint for engine/provider"),
    include_matrix: Optional[bool] = Query(default=None, description="Return rows_matrix for legacy clients"),
    token: Optional[str] = Query(default=None, description="Auth token (query only if allowed)"),
    x_app_token: Optional[str] = Header(default=None, alias="X-APP-TOKEN"),
    authorization: Optional[str] = Header(default=None, alias="Authorization"),
    x_request_id: Optional[str] = Header(default=None, alias="X-Request-ID"),
) -> Dict[str, Any]:
    t0 = time.perf_counter()
    _require_auth_or_401(token_query=token, x_app_token=x_app_token, authorization=authorization)

    request_id = x_request_id or getattr(request.state, "request_id", None) or str(uuid.uuid4())

    raw_sheet = str(
        body.get("sheet")
        or body.get("sheet_name")
        or body.get("page")
        or body.get("name")
        or body.get("tab")
        or ""
    ).strip()
    if not raw_sheet:
        raise HTTPException(status_code=422, detail="Missing required field: sheet")

    sheet = _canonicalize_sheet_name(raw_sheet)

    limit = int(body.get("limit") or 2000)
    offset = int(body.get("offset") or 0)
    limit = max(1, min(5000, limit))
    offset = max(0, offset)

    include_matrix_final = include_matrix if isinstance(include_matrix, bool) else _get_bool(body, "include_matrix", True)

    schema_headers, schema_keys, schema_source = _schema_headers_keys(sheet)

    # -------------------------------------------------------------------------
    # Data_Dictionary: generated from schema_registry (no engine)
    # -------------------------------------------------------------------------
    if sheet == "Data_Dictionary":
        try:
            mod = __import__("core.sheets.data_dictionary", fromlist=["*"])
            build_rows = getattr(mod, "build_data_dictionary_rows", None)
            headers_fn = getattr(mod, "data_dictionary_headers", None)
            keys_fn = getattr(mod, "data_dictionary_keys", None)

            out_headers = list(headers_fn()) if callable(headers_fn) else (schema_headers or [])
            out_keys = list(keys_fn()) if callable(keys_fn) else (schema_keys or [])

            dd_rows: Any = []
            if callable(build_rows):
                try:
                    dd_rows = build_rows(include_meta_sheet=True)
                except TypeError:
                    dd_rows = build_rows()
            else:
                dd_rows = []

        except Exception as e:
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail={"error": "Data_Dictionary generator failed", "detail": str(e)},
            )

        rows: List[Dict[str, Any]] = []
        for r in dd_rows or []:
            if not isinstance(r, dict):
                continue
            rows.append({k: r.get(k, None) for k in out_keys})

        rows = rows[offset : offset + limit]

        return _to_jsonable(
            {
                "status": "success",
                "page": "Data_Dictionary",
                "sheet": "Data_Dictionary",
                "headers": out_headers,
                "keys": out_keys,
                "rows": rows,
                "rows_matrix": _rows_to_matrix(rows, out_keys) if include_matrix_final else None,
                "version": ADVANCED_ANALYSIS_VERSION,
                "request_id": request_id,
                "meta": {
                    "schema_source": schema_source,
                    "generated": True,
                    "rows": len(rows),
                    "limit": limit,
                    "offset": offset,
                    "duration_ms": (time.perf_counter() - t0) * 1000.0,
                },
            }
        )

    # -------------------------------------------------------------------------
    # Insights_Analysis: MUST call real builder (always meaningful)
    # -------------------------------------------------------------------------
    if sheet == "Insights_Analysis":
        payload = await _build_insights_payload(
            request=request,
            body=body,
            mode=mode or "",
            include_matrix=bool(include_matrix_final),
            limit=limit,
            offset=offset,
        )
        # add standard envelope fields
        payload = dict(payload) if isinstance(payload, dict) else {"status": "success", "rows": []}
        payload["request_id"] = request_id
        payload.setdefault("meta", {})
        if isinstance(payload["meta"], dict):
            payload["meta"].update(
                {
                    "path": "insights_builder",
                    "schema_source": schema_source or payload["meta"].get("schema_source"),
                    "limit": limit,
                    "offset": offset,
                    "duration_ms": (time.perf_counter() - t0) * 1000.0,
                }
            )
        payload["version"] = ADVANCED_ANALYSIS_VERSION
        return _to_jsonable(payload)

    # -------------------------------------------------------------------------
    # All other sheets: engine best-effort; schema-first headers/keys always
    # -------------------------------------------------------------------------
    engine = await _get_engine(request)

    if engine is None:
        return _to_jsonable(
            {
                "status": "degraded",
                "page": sheet,
                "sheet": sheet,
                "headers": schema_headers,
                "keys": schema_keys,
                "rows": [],
                "rows_matrix": [] if include_matrix_final else None,
                "version": ADVANCED_ANALYSIS_VERSION,
                "request_id": request_id,
                "meta": {
                    "schema_source": schema_source,
                    "engine_available": False,
                    "path": "schema_only_no_engine",
                    "rows": 0,
                    "limit": limit,
                    "offset": offset,
                    "duration_ms": (time.perf_counter() - t0) * 1000.0,
                },
            }
        )

    try:
        payload = await _engine_fetch_any(engine, sheet=sheet, limit=limit, offset=offset, mode=mode or "", body=body)
    except Exception as e:
        return _to_jsonable(
            {
                "status": "degraded",
                "page": sheet,
                "sheet": sheet,
                "headers": schema_headers,
                "keys": schema_keys,
                "rows": [],
                "rows_matrix": [] if include_matrix_final else None,
                "version": ADVANCED_ANALYSIS_VERSION,
                "request_id": request_id,
                "meta": {
                    "schema_source": schema_source,
                    "engine_available": True,
                    "engine_error": str(e),
                    "path": "schema_only_engine_error",
                    "rows": 0,
                    "limit": limit,
                    "offset": offset,
                    "duration_ms": (time.perf_counter() - t0) * 1000.0,
                },
            }
        )

    rows, eng_headers, eng_keys = _coerce_payload_to_rows(payload)

    out_headers = schema_headers[:] if schema_headers else (eng_headers[:] if eng_headers else [])
    out_keys = schema_keys[:] if schema_keys else (eng_keys[:] if eng_keys else [])

    if not out_keys and rows:
        out_keys = list(rows[0].keys())
    if not out_headers and out_keys:
        out_headers = out_keys[:]

    norm_rows: List[Dict[str, Any]] = []
    if schema_keys:
        for r in rows:
            rr = _normalize_row_to_schema(sheet, dict(r))
            norm_rows.append(_project_row(schema_keys, rr))
    else:
        norm_rows = [dict(r) for r in rows]

    return _to_jsonable(
        {
            "status": "success" if out_headers else "partial",
            "page": sheet,
            "sheet": sheet,
            "headers": out_headers,
            "keys": out_keys,
            "rows": norm_rows,
            "rows_matrix": _rows_to_matrix(norm_rows, out_keys) if (include_matrix_final and out_keys) else None,
            "version": ADVANCED_ANALYSIS_VERSION,
            "request_id": request_id,
            "meta": {
                "schema_source": schema_source,
                "engine_type": _safe_engine_type(engine),
                "schema_cols": len(schema_headers) if schema_headers else 0,
                "engine_cols": len(eng_headers) if eng_headers else 0,
                "returned_cols": len(out_headers) if out_headers else 0,
                "rows": len(norm_rows),
                "limit": limit,
                "offset": offset,
                "mode": mode,
                "sheet_rows_schema_driven": True,
                "replaced_base_sheet_rows_routes": _removed,
                "duration_ms": (time.perf_counter() - t0) * 1000.0,
            },
        }
    )


@router.get("/health")
async def advanced_health(request: Request) -> Dict[str, Any]:
    """Lightweight health endpoint. Must not do heavy computations."""
    engine = await _get_engine(request)

    schema_pages = None
    try:
        from core.sheets.page_catalog import CANONICAL_PAGES  # type: ignore

        schema_pages = list(CANONICAL_PAGES)
    except Exception:
        schema_pages = None

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

    request_id = getattr(request.state, "request_id", None)

    return _to_jsonable(
        {
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
            "sheet_rows_schema_driven": True,
            "replaced_base_sheet_rows_routes": _removed,
            "base_router_import_error": _base_router_import_error,
            "insights_builder_wired": True,
        }
    )


@router.get("/metrics")
async def advanced_metrics() -> Response:
    """Prometheus metrics if available, otherwise 503."""
    if not _PROMETHEUS_AVAILABLE or generate_latest is None:
        return Response(content="Metrics not available", media_type="text/plain", status_code=503)
    return Response(content=generate_latest(), media_type=CONTENT_TYPE_LATEST)


@router.get("/insights-criteria")
async def insights_criteria(
    token: Optional[str] = Query(default=None, description="Auth token (query only if allowed)"),
    x_app_token: Optional[str] = Header(default=None, alias="X-APP-TOKEN"),
    authorization: Optional[str] = Header(default=None, alias="Authorization"),
) -> Dict[str, Any]:
    _require_auth_or_401(token_query=token, x_app_token=x_app_token, authorization=authorization)

    snap = _build_insights_criteria_snapshot()
    return _to_jsonable(
        {
            "status": "success",
            "version": ADVANCED_ANALYSIS_VERSION,
            "criteria": snap,
            "embedded_target": "Insights_Analysis",
        }
    )


@router.post("/insights-analysis")
async def insights_analysis(
    request: Request,
    body: Dict[str, Any] = Body(default_factory=dict),
    mode: str = Query(default="", description="Optional mode hint for engine/provider"),
    include_matrix: Optional[bool] = Query(default=None, description="Return rows_matrix for legacy clients"),
    limit: int = Query(default=2000, ge=1, le=5000, description="Max rows to return"),
    offset: int = Query(default=0, ge=0, description="Row offset"),
    token: Optional[str] = Query(default=None, description="Auth token (query only if allowed)"),
    x_app_token: Optional[str] = Header(default=None, alias="X-APP-TOKEN"),
    authorization: Optional[str] = Header(default=None, alias="Authorization"),
    x_request_id: Optional[str] = Header(default=None, alias="X-Request-ID"),
) -> Dict[str, Any]:
    """
    MUST be meaningful even when symbols=[]:
    - Delegates to core.analysis.insights_builder.build_insights_analysis_rows
    - Uses auto-universe when caller provides no symbols/universes
    """
    _require_auth_or_401(token_query=token, x_app_token=x_app_token, authorization=authorization)
    request_id = x_request_id or getattr(request.state, "request_id", None) or str(uuid.uuid4())

    include_matrix_final = include_matrix if isinstance(include_matrix, bool) else _get_bool(body, "include_matrix", True)

    payload = await _build_insights_payload(
        request=request,
        body=body or {},
        mode=mode or "",
        include_matrix=bool(include_matrix_final),
        limit=max(1, min(5000, int(limit))),
        offset=max(0, int(offset)),
    )

    payload = dict(payload) if isinstance(payload, dict) else {"status": "success", "rows": []}
    payload["request_id"] = request_id
    payload["version"] = ADVANCED_ANALYSIS_VERSION
    payload.setdefault("meta", {})
    if isinstance(payload["meta"], dict):
        payload["meta"]["endpoint"] = "/v1/advanced/insights-analysis"
    return _to_jsonable(payload)


__all__ = ["router"]
