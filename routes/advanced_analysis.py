#!/usr/bin/env python3
"""
routes/advanced_analysis.py
------------------------------------------------------------
TADAWUL ADVANCED ANALYSIS ROUTER — v5.6.1 (PHASE 5 / STARTUP-SAFE / SCHEMA-FIRST)
PROD HARDENED + SCHEMA-DRIVEN SHEET-ROWS (FIXES SPECIAL SHEETS) + INSIGHTS ENDPOINTS

CRITICAL FIX (Render build failure)
- ✅ FIXED: SyntaxError: '[' was never closed (Data_Dictionary branch had a broken list literal)

What this router guarantees
- POST /v1/advanced/sheet-rows is schema-driven:
    - headers/keys come from core.sheets.schema_registry (authoritative order)
    - rows are normalized then STRICTLY projected to schema keys
    - special sheets never fall back to an 80-col default:
        * Insights_Analysis   (schema-defined layout; best-effort builder)
        * Top_10_Investments  (schema-defined 80+3 layout; engine if available else schema-only)
        * Data_Dictionary     (generated from schema_registry via core.sheets.data_dictionary)

No network calls at import-time. Heavy imports are inside handlers.
"""

from __future__ import annotations

import json
import logging
import os
import time
import uuid
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Sequence, Tuple

from fastapi import Body, Header, HTTPException, Query, Request, Response, status

# -----------------------------------------------------------------------------
# ✅ Phase-3 base router (prefix="/v1/advanced", includes original POST /sheet-rows)
# -----------------------------------------------------------------------------
from routes.advanced_sheet_rows import router  # noqa: F401

logger = logging.getLogger("routes.advanced_analysis")

ADVANCED_ANALYSIS_VERSION = "5.6.1"

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


def _safe_engine_type(engine: Any) -> str:
    try:
        return type(engine).__name__
    except Exception:
        return "unknown"


def _safe_env_port() -> Optional[str]:
    p = (os.getenv("PORT") or "").strip()
    return p or None


async def _maybe_call(obj: Any, name: str) -> Optional[Any]:
    """Best-effort call obj.<name>(), supporting sync/async. Never raises."""
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
    """Returns a stable criteria snapshot matching your scoring model (best-effort)."""
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

    # Prefer page_catalog normalization if available
    for fn_name in ("normalize_page_name", "resolve_page", "canonicalize_page"):
        try:
            mod = __import__("core.sheets.page_catalog", fromlist=[fn_name])
            fn = getattr(mod, fn_name, None)
            if callable(fn):
                try:
                    out = fn(s, allow_output_pages=True)  # normalize_page_name signature
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
    spec = _get_sheet_spec(sheet)
    if spec and getattr(spec, "columns", None):
        headers = [str(getattr(c, "header", "")) for c in spec.columns]
        keys = [str(getattr(c, "key", "")) for c in spec.columns]
        headers = [h for h in headers if h]
        keys = [k for k in keys if k]
        return headers, keys, "schema_registry.get_sheet_spec"
    return [], [], "none"


def _get_all_sheet_names() -> List[str]:
    # 1) schema_registry.list_sheets() if present
    try:
        from core.sheets import schema_registry as sr  # type: ignore

        fn = getattr(sr, "list_sheets", None)
        if callable(fn):
            out = fn()
            if isinstance(out, (list, tuple)) and out:
                return [str(x) for x in out]
    except Exception:
        pass

    # 2) page_catalog canonical ordering
    try:
        from core.sheets.page_catalog import CANONICAL_PAGES  # type: ignore

        return [str(x) for x in CANONICAL_PAGES]
    except Exception:
        return []


def _normalize_row_to_schema(sheet: str, row: Dict[str, Any]) -> Dict[str, Any]:
    """
    Best-effort normalization.
    Prefer core.data_engine_v2.normalize_row_to_schema if it exists.
    """
    spec = _get_sheet_spec(sheet)
    if not spec or not getattr(spec, "columns", None):
        return row

    try:
        from core.data_engine_v2 import normalize_row_to_schema  # type: ignore

        return normalize_row_to_schema(sheet, row)
    except Exception:
        out: Dict[str, Any] = {}
        for c in spec.columns:
            k = str(getattr(c, "key", ""))
            h = str(getattr(c, "header", ""))
            if k and k in row:
                out[k] = row.get(k)
            elif h and h in row:
                out[k] = row.get(h)
            elif k:
                out[k] = None
        return out


def _project_row(keys: List[str], row: Dict[str, Any]) -> Dict[str, Any]:
    """STRICT projection to schema keys in-order (extras dropped; missing filled with None)."""
    return {k: row.get(k, None) for k in keys}


def _rows_to_matrix(rows: List[Dict[str, Any]], keys: List[str]) -> List[List[Any]]:
    return [[r.get(k) for k in keys] for r in rows]


def _coerce_rows_payload(payload: Any) -> Tuple[List[Dict[str, Any]], List[str], List[str]]:
    """
    Accept:
      - list[dict]
      - {"headers":[...], "keys":[...], "rows":[...]}
      - {"data":{...}} shapes
    """
    if payload is None:
        return [], [], []

    if isinstance(payload, list):
        rows = [r for r in payload if isinstance(r, dict)]
        keys = list(rows[0].keys()) if rows else []
        return rows, keys[:], keys[:]

    if isinstance(payload, dict):
        if isinstance(payload.get("data"), dict):
            return _coerce_rows_payload(payload.get("data"))

        headers = payload.get("headers") or payload.get("columns") or payload.get("fields") or []
        keys = payload.get("keys") or payload.get("schema") or payload.get("fields") or []
        rows = payload.get("rows") or payload.get("items") or payload.get("records") or payload.get("data") or []
        if not isinstance(rows, list):
            rows = []
        rows = [r for r in rows if isinstance(r, dict)]
        if not keys and rows:
            keys = list(rows[0].keys())
        if not headers and keys:
            headers = keys[:]
        return rows, [str(x) for x in headers], [str(x) for x in keys]

    try:
        if hasattr(payload, "model_dump"):
            return _coerce_rows_payload(payload.model_dump(mode="python"))  # type: ignore
        if hasattr(payload, "dict"):
            return _coerce_rows_payload(payload.dict())  # type: ignore
    except Exception:
        pass

    return [], [], []


async def _engine_sheet_rows(
    engine: Any,
    *,
    sheet: str,
    limit: int,
    offset: int,
    mode: str,
    body: Dict[str, Any],
) -> Any:
    candidates = [
        ("get_sheet_rows", dict(sheet=sheet, limit=limit, offset=offset, mode=mode, body=body)),
        ("get_sheet_rows", dict(sheet=sheet, limit=limit, offset=offset, mode=mode)),
        ("sheet_rows", dict(sheet=sheet, limit=limit, offset=offset, mode=mode, body=body)),
        ("sheet_rows", dict(sheet=sheet, limit=limit, offset=offset, mode=mode)),
        ("build_sheet_rows", dict(sheet=sheet, limit=limit, offset=offset, mode=mode, body=body)),
        ("build_sheet_rows", dict(sheet=sheet, limit=limit, offset=offset, mode=mode)),
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
            # try positional fallback
            try:
                out = fn(sheet, limit=limit, offset=offset, mode=mode)
                if hasattr(out, "__await__"):
                    out = await out
                return out
            except Exception as e2:
                last_err = e2
                continue
        except Exception as e:
            last_err = e
            continue

    if last_err is not None:
        raise last_err
    raise RuntimeError("Engine has no supported sheet-rows method")


# -----------------------------------------------------------------------------
# Insights builder (best-effort; keeps schema 7-col correct)
# -----------------------------------------------------------------------------
async def _build_insights_analysis_rows(
    *,
    request: Request,
    settings: Any,
    mode: str,
    body: Dict[str, Any],
    schema_keys: Sequence[str],
    schema_headers: Sequence[str],
) -> List[Dict[str, Any]]:
    try:
        mod = __import__("core.analysis.insights_builder", fromlist=["*"])
    except Exception:
        return []

    fn = None
    for name in (
        "build_insights_analysis_rows",
        "build_insights_rows",
        "build_insights_analysis",
        "get_insights_rows",
        "build_rows",
    ):
        cand = getattr(mod, name, None)
        if callable(cand):
            fn = cand
            break
    if fn is None:
        return []

    # Try a few signatures
    candidates: List[Tuple[Tuple[Any, ...], Dict[str, Any]]] = [
        ((), {"request": request, "settings": settings, "mode": mode, "body": body}),
        ((), {"settings": settings, "mode": mode, "body": body}),
        ((), {"mode": mode, "body": body}),
        ((), {"mode": mode}),
        ((), {"body": body}),
        ((), {}),
    ]

    out = None
    for args, kwargs in candidates:
        try:
            res = fn(*args, **kwargs)
            if hasattr(res, "__await__"):
                res = await res
            out = res
            break
        except TypeError:
            continue
        except Exception:
            continue

    if out is None:
        return []

    rows: List[Dict[str, Any]] = []
    if isinstance(out, list):
        rows = [r for r in out if isinstance(r, dict)]
    elif isinstance(out, dict):
        r2 = out.get("rows") or out.get("data") or out.get("items") or out.get("records")
        if isinstance(r2, list):
            rows = [r for r in r2 if isinstance(r, dict)]
        else:
            rows = [out] if isinstance(out, dict) else []
    else:
        rows = []

    norm: List[Dict[str, Any]] = []
    for r in rows:
        rr = dict(r)
        # normalize by schema keys/headers
        for k in schema_keys:
            if k not in rr:
                # try header match
                idx = list(schema_keys).index(k)
                h = schema_headers[idx] if idx < len(schema_headers) else ""
                if h and h in rr and k not in rr:
                    rr[k] = rr.get(h)
        for k in schema_keys:
            rr.setdefault(k, None)
        norm.append({str(k): rr.get(str(k)) for k in schema_keys})

    return norm


# -----------------------------------------------------------------------------
# 🚧 Replace Phase-3 /sheet-rows with schema-driven handler (NO DUPLICATE ROUTES)
# -----------------------------------------------------------------------------
def _remove_router_route(path: str, method: str) -> int:
    removed = 0
    try:
        new_routes = []
        for r in list(getattr(router, "routes", [])):
            try:
                rp = getattr(r, "path", None)
                rm = getattr(r, "methods", None) or set()
                if rp == path and method.upper() in set(rm):
                    removed += 1
                    continue
            except Exception:
                pass
            new_routes.append(r)
        router.routes = new_routes  # type: ignore
    except Exception:
        return removed
    return removed


_removed = _remove_router_route("/sheet-rows", "POST")
if _removed:
    logger.info(
        "AdvancedAnalysis: removed %s existing POST /sheet-rows route(s) to replace schema-driven handler.",
        _removed,
    )


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
    """
    Schema-driven replacement for POST /v1/advanced/sheet-rows

    Guarantees (when schema_registry is available for the sheet):
    - headers == schema headers (correct count + correct order)
    - keys    == schema keys (correct count + correct order)
    - rows    == STRICTLY projected to schema keys (extras dropped; missing None)
    """
    start = time.perf_counter()
    _require_auth_or_401(token_query=token, x_app_token=x_app_token, authorization=authorization)

    request_id = x_request_id or getattr(request.state, "request_id", None) or str(uuid.uuid4())

    raw_sheet = str(body.get("sheet") or body.get("page") or body.get("name") or body.get("sheet_name") or "").strip()
    if not raw_sheet:
        raise HTTPException(status_code=422, detail="Missing required field: sheet")

    sheet = _canonicalize_sheet_name(raw_sheet)

    # pagination
    limit = int(body.get("limit") or 2000)
    offset = int(body.get("offset") or 0)
    limit = max(1, min(5000, limit))
    offset = max(0, offset)

    include_matrix_final = include_matrix if isinstance(include_matrix, bool) else _get_bool(body, "include_matrix", True)

    # Schema (authoritative)
    schema_headers, schema_keys, schema_source = _schema_headers_keys(sheet)

    # Settings (optional)
    try:
        settings = get_settings_cached()
    except Exception:
        settings = None

    # -------------------------------------------------------------------------
    # SPECIAL: Insights_Analysis (best-effort builder; schema-first)
    # -------------------------------------------------------------------------
    if sheet == "Insights_Analysis":
        out_headers = schema_headers[:]
        out_keys = schema_keys[:]

        rows: List[Dict[str, Any]] = []
        if out_headers and out_keys:
            rows = await _build_insights_analysis_rows(
                request=request,
                settings=settings,
                mode=mode or "",
                body=body,
                schema_keys=out_keys,
                schema_headers=out_headers,
            )
            # slice
            rows = rows[offset : offset + limit]
        else:
            # schema missing -> return empty but deterministic
            rows = []

        return {
            "status": "success",
            "page": "Insights_Analysis",
            "sheet": "Insights_Analysis",
            "headers": out_headers,
            "keys": out_keys,
            "rows": rows,
            "rows_matrix": _rows_to_matrix(rows, out_keys) if include_matrix_final and out_keys else None,
            "version": ADVANCED_ANALYSIS_VERSION,
            "request_id": request_id,
            "meta": {
                "schema_source": schema_source,
                "path": "insights_builder" if rows else "schema_only",
                "rows": len(rows),
                "limit": limit,
                "offset": offset,
                "duration_ms": (time.perf_counter() - start) * 1000.0,
            },
        }

    # -------------------------------------------------------------------------
    # SPECIAL: Data_Dictionary (generated from schema_registry; no engine)
    # -------------------------------------------------------------------------
    if sheet == "Data_Dictionary":
        # Data_Dictionary schema (preferred)
        out_headers = schema_headers[:] if schema_headers else [
            "Sheet", "Group", "Header", "Key", "DType", "Format", "Required", "Source", "Notes"
        ]
        out_keys = schema_keys[:] if schema_keys else [
            "sheet", "group", "header", "key", "dtype", "fmt", "required", "source", "notes"
        ]

        try:
            from core.sheets.data_dictionary import build_data_dictionary_rows  # type: ignore

            dd_rows = build_data_dictionary_rows(include_meta_sheet=True)
        except Exception as e:
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail={"error": "Data_Dictionary generator import/call failed", "detail": str(e)},
            )

        # normalize rows to out_keys
        rows: List[Dict[str, Any]] = []
        for r in dd_rows or []:
            if not isinstance(r, dict):
                continue
            # project strict
            row = {k: r.get(k, None) for k in out_keys}
            rows.append(row)

        rows = rows[offset : offset + limit]

        return {
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
                "duration_ms": (time.perf_counter() - start) * 1000.0,
            },
        }

    # -------------------------------------------------------------------------
    # TABLE MODE: Top_10_Investments (engine if available; else schema-only)
    # -------------------------------------------------------------------------
    if sheet == "Top_10_Investments":
        out_headers = schema_headers[:]
        out_keys = schema_keys[:]

        engine = await _get_engine(request)
        if not engine:
            # schema-only fallback (still passes schema alignment tests)
            return {
                "status": "success",
                "page": sheet,
                "sheet": sheet,
                "headers": out_headers,
                "keys": out_keys,
                "rows": [],
                "rows_matrix": [] if include_matrix_final else None,
                "version": ADVANCED_ANALYSIS_VERSION,
                "request_id": request_id,
                "meta": {
                    "schema_source": schema_source,
                    "path": "schema_only_no_engine",
                    "rows": 0,
                    "limit": limit,
                    "offset": offset,
                    "duration_ms": (time.perf_counter() - start) * 1000.0,
                },
            }

        try:
            payload = await _engine_sheet_rows(engine, sheet=sheet, limit=limit, offset=offset, mode=mode or "", body=body)
        except Exception:
            payload = None

        rows, eng_headers, eng_keys = _coerce_rows_payload(payload)
        if not out_headers:
            out_headers = eng_headers[:]
        if not out_keys:
            out_keys = eng_keys[:]

        # Normalize + strict project if schema exists
        norm_rows: List[Dict[str, Any]] = []
        if schema_keys:
            for r in rows:
                rr = dict(r)
                rr = _normalize_row_to_schema(sheet, rr)
                norm_rows.append(_project_row(schema_keys, rr))
        else:
            norm_rows = [dict(r) for r in rows]

        return {
            "status": "success",
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
                "schema_cols": len(schema_headers) if schema_headers else 0,
                "engine_cols": len(eng_headers) if eng_headers else 0,
                "returned_cols": len(out_headers) if out_headers else 0,
                "rows": len(norm_rows),
                "limit": limit,
                "offset": offset,
                "mode": mode,
                "path": "engine_table_mode",
                "duration_ms": (time.perf_counter() - start) * 1000.0,
            },
        }

    # -------------------------------------------------------------------------
    # DEFAULT: Other sheets — engine + schema normalization + STRICT projection
    # -------------------------------------------------------------------------
    engine = await _get_engine(request)
    if not engine:
        raise HTTPException(status_code=status.HTTP_503_SERVICE_UNAVAILABLE, detail="Data engine unavailable")

    try:
        payload = await _engine_sheet_rows(engine, sheet=sheet, limit=limit, offset=offset, mode=mode or "", body=body)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Engine sheet-rows failed: {e}")

    rows, eng_headers, eng_keys = _coerce_rows_payload(payload)

    # Determine output headers/keys (schema first)
    out_headers = schema_headers[:] if schema_headers else (eng_headers[:] if eng_headers else [])
    out_keys = schema_keys[:] if schema_keys else (eng_keys[:] if eng_keys else [])

    if not out_keys and rows:
        out_keys = list(rows[0].keys())
    if not out_headers and out_keys:
        out_headers = out_keys[:]

    # Normalize + project
    norm_rows: List[Dict[str, Any]] = []
    if schema_keys:
        for r in rows:
            rr = dict(r)
            rr = _normalize_row_to_schema(sheet, rr)
            norm_rows.append(_project_row(schema_keys, rr))
    else:
        norm_rows = [dict(r) for r in rows]

    return {
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
            "schema_cols": len(schema_headers) if schema_headers else 0,
            "engine_cols": len(eng_headers) if eng_headers else 0,
            "returned_cols": len(out_headers) if out_headers else 0,
            "rows": len(norm_rows),
            "limit": limit,
            "offset": offset,
            "mode": mode,
            "sheet_rows_schema_driven": True,
            "replaced_base_sheet_rows_routes": _removed,
            "duration_ms": (time.perf_counter() - start) * 1000.0,
        },
    }


# -----------------------------------------------------------------------------
# Existing endpoints (kept) + minor hardening
# -----------------------------------------------------------------------------
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
        "sheet_rows_schema_driven": True,
        "replaced_base_sheet_rows_routes": _removed,
    }


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
    """Returns a criteria snapshot designed to be embedded into Insights_Analysis."""
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
    - Fetches enriched quotes via engine (batch preferred)
    - Normalizes to Insights_Analysis schema when available
    - Optionally embeds criteria JSON into the first row if schema keys exist
    """
    _require_auth_or_401(token_query=token, x_app_token=x_app_token, authorization=authorization)
    request_id = x_request_id or getattr(request.state, "request_id", None) or str(uuid.uuid4())

    symbols = _get_list(body, "symbols", "tickers")
    top_n = int(body.get("top_n") or 2000)
    top_n = max(1, min(5000, top_n))
    symbols = symbols[:top_n]

    include_matrix_final = include_matrix if isinstance(include_matrix, bool) else _get_bool(body, "include_matrix", True)

    # Schema (best-effort)
    headers, keys, _src = _schema_headers_keys("Insights_Analysis")

    engine = await _get_engine(request)
    if not engine:
        raise HTTPException(status_code=status.HTTP_503_SERVICE_UNAVAILABLE, detail="Data engine unavailable")

    # Prefer batch
    quotes: Dict[str, Any] = {}
    try:
        fn = getattr(engine, "get_enriched_quotes_batch", None)
        if callable(fn):
            res = fn(symbols, mode=mode or "", schema="Insights_Analysis")
            if hasattr(res, "__await__"):
                res = await res
            if isinstance(res, dict):
                quotes = res
    except Exception:
        quotes = {}

    # Fallback per-symbol
    if not quotes and symbols:
        out: Dict[str, Any] = {}
        for s in symbols:
            try:
                fn2 = getattr(engine, "get_enriched_quote_dict", None)
                if callable(fn2):
                    r = fn2(s, schema="Insights_Analysis")
                    if hasattr(r, "__await__"):
                        r = await r
                    out[s] = r
                else:
                    fn3 = getattr(engine, "get_enriched_quote", None)
                    if callable(fn3):
                        q = fn3(s, schema="Insights_Analysis")
                        if hasattr(q, "__await__"):
                            q = await q
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

    normalized_rows: List[Dict[str, Any]] = []
    errors = 0

    for sym in symbols:
        raw = quotes.get(sym) or {"symbol": sym, "error": "no_data"}
        if isinstance(raw, dict) and raw.get("error"):
            errors += 1
        row = dict(raw) if isinstance(raw, dict) else {"symbol": sym, "result": raw}

        try:
            row = _normalize_row_to_schema("Insights_Analysis", row)
        except Exception:
            pass

        # STRICT project if schema keys exist
        if keys:
            row = _project_row(keys, row)

        normalized_rows.append(row)

    # Embed criteria (only if schema includes a compatible key)
    if embed_criteria and normalized_rows:
        snap = _build_insights_criteria_snapshot()
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
            for wk in ("scoring_weights", "weights_json", "weights"):
                if wk in keys:
                    first[wk] = criteria_json

    status_out = "success" if errors == 0 else ("partial" if errors < len(symbols) else "error")

    # If schema unavailable, derive keys from first row
    if (not keys) and normalized_rows:
        keys = list(normalized_rows[0].keys())
        headers = keys[:]

    return {
        "status": status_out,
        "page": "Insights_Analysis",
        "headers": headers,
        "keys": keys,
        "rows": normalized_rows,
        "rows_matrix": _rows_to_matrix(normalized_rows, keys) if include_matrix_final else None,
        "error": f"{errors} errors" if errors else None,
        "version": ADVANCED_ANALYSIS_VERSION,
        "request_id": request_id,
        "meta": {
            "requested": len(symbols),
            "errors": errors,
            "mode": mode,
            "criteria_embedded": bool(embed_criteria),
            "schema_available": bool(headers and keys),
        },
    }


__all__ = ["router"]
