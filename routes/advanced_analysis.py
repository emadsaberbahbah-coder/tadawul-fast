#!/usr/bin/env python3
"""
routes/advanced_analysis.py
--------------------------------------------------------------------------------
TADAWUL ADVANCED ANALYSIS ROUTER — v5.9.0
(SCHEMA-ENDPOINT FIX / ROOT+V1 COMPAT / INSIGHTS+TOP10 WIRED / STARTUP-SAFE)

What this revision improves
- ✅ FIX: adds real schema endpoints:
      /v1/schema/sheet-spec
      /v1/schema/spec
      /schema/sheet-spec
      /schema/spec
  and they return 200 without requiring sheet/query params.
- ✅ Keeps root /sheet-rows compatibility while also exposing /v1/advanced/sheet-rows.
- ✅ Insights_Analysis stays builder-driven and schema-projected.
- ✅ NEW: Top_10_Investments gets a builder-first path before engine fallback.
- ✅ Data_Dictionary remains schema-generated and projected to canonical keys.
- ✅ Schema-first everywhere: headers/keys stay aligned; special sheets never fall back to 80 cols.
- ✅ Import-time safe: no network calls; heavy imports remain inside handlers.
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
ADVANCED_ANALYSIS_VERSION = "5.9.0"

# -----------------------------------------------------------------------------
# Routers
# -----------------------------------------------------------------------------
_base_router_import_error: Optional[str] = None
try:
    from routes.advanced_sheet_rows import router as _root_sheet_rows_router  # type: ignore
except Exception as e:
    _base_router_import_error = f"{type(e).__name__}: {e}"
    _root_sheet_rows_router = APIRouter(tags=["advanced-root"])

advanced_router = APIRouter(prefix="/v1/advanced", tags=["advanced"])
schema_router_v1 = APIRouter(prefix="/v1/schema", tags=["schema"])
schema_router_compat = APIRouter(prefix="/schema", tags=["schema"])
router = APIRouter(tags=["advanced-analysis"])

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
# JSON-safety helpers
# -----------------------------------------------------------------------------
def _to_jsonable(obj: Any) -> Any:
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
    auth_token = _extract_auth_token(
        token_query=token_query,
        x_app_token=x_app_token,
        authorization=authorization,
    )
    if not auth_ok(
        token=auth_token,
        authorization=authorization,
        headers={"X-APP-TOKEN": x_app_token, "Authorization": authorization},
    ):
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid token")


# -----------------------------------------------------------------------------
# Small helpers
# -----------------------------------------------------------------------------
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


# -----------------------------------------------------------------------------
# Schema helpers (lazy)
# -----------------------------------------------------------------------------
def _get_sheet_spec(sheet: str) -> Optional[Any]:
    try:
        from core.sheets.schema_registry import get_sheet_spec  # type: ignore

        return get_sheet_spec(sheet)
    except Exception:
        return None


def _column_attr(obj: Any, name: str, default: Any = None) -> Any:
    if obj is None:
        return default
    if isinstance(obj, dict):
        return obj.get(name, default)
    return getattr(obj, name, default)


def _schema_headers_keys(sheet: str) -> Tuple[List[str], List[str], str]:
    spec = _get_sheet_spec(sheet)
    if spec and getattr(spec, "columns", None):
        headers: List[str] = []
        keys: List[str] = []
        for c in spec.columns:
            h = str(_column_attr(c, "header", "") or "").strip()
            k = str(_column_attr(c, "key", "") or "").strip()
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
            k = str(_column_attr(c, "key", "") or "").strip()
            h = str(_column_attr(c, "header", "") or "").strip()
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


def _collect_schema_registry() -> Tuple[Dict[str, Any], str]:
    try:
        mod = __import__("core.sheets.schema_registry", fromlist=["*"])
    except Exception:
        return {}, "import_failed"

    candidates = [
        ("get_all_sheet_specs", True),
        ("list_sheet_specs", True),
        ("get_schema_registry", False),
        ("schema_registry", False),
        ("SCHEMA_REGISTRY", False),
        ("SHEET_REGISTRY", False),
        ("REGISTRY", False),
        ("SHEETS", False),
    ]

    for name, should_call in candidates:
        obj = getattr(mod, name, None)
        if obj is None:
            continue
        try:
            out = obj() if should_call and callable(obj) else obj
        except Exception:
            continue

        if isinstance(out, dict):
            normalized: Dict[str, Any] = {}
            for k, v in out.items():
                kk = _canonicalize_sheet_name(str(k))
                normalized[kk] = v
            if normalized:
                return normalized, f"schema_registry.{name}"

        if isinstance(out, (list, tuple)):
            normalized = {}
            for spec in out:
                sheet_name = str(_column_attr(spec, "sheet", "") or _column_attr(spec, "name", "") or "").strip()
                if not sheet_name:
                    continue
                normalized[_canonicalize_sheet_name(sheet_name)] = spec
            if normalized:
                return normalized, f"schema_registry.{name}"

    normalized = {}
    try:
        from core.sheets.page_catalog import CANONICAL_PAGES  # type: ignore

        for page in list(CANONICAL_PAGES):
            spec = _get_sheet_spec(str(page))
            if spec is not None:
                normalized[_canonicalize_sheet_name(str(page))] = spec
    except Exception:
        pass

    return normalized, "schema_registry.get_sheet_spec_fallback"


def _sheet_spec_to_payload(sheet: str, spec: Any) -> Dict[str, Any]:
    headers, keys, _ = _schema_headers_keys(sheet)
    columns_payload: List[Dict[str, Any]] = []

    cols = getattr(spec, "columns", None) or _column_attr(spec, "columns", []) or []
    for c in cols:
        columns_payload.append(
            {
                "group": _column_attr(c, "group"),
                "header": _column_attr(c, "header"),
                "key": _column_attr(c, "key"),
                "dtype": _column_attr(c, "dtype"),
                "fmt": _column_attr(c, "fmt"),
                "required": _column_attr(c, "required"),
                "source": _column_attr(c, "source"),
                "notes": _column_attr(c, "notes"),
            }
        )

    criteria_fields_payload: List[Dict[str, Any]] = []
    cfs = getattr(spec, "criteria_fields", None) or _column_attr(spec, "criteria_fields", []) or []
    for cf in cfs:
        criteria_fields_payload.append(
            {
                "key": _column_attr(cf, "key"),
                "header": _column_attr(cf, "header"),
                "dtype": _column_attr(cf, "dtype"),
                "required": _column_attr(cf, "required"),
                "default": _column_attr(cf, "default"),
                "notes": _column_attr(cf, "notes"),
            }
        )

    return {
        "sheet": sheet,
        "headers": headers,
        "keys": keys,
        "columns": columns_payload,
        "criteria_fields": criteria_fields_payload,
        "column_count": len(headers),
        "criteria_field_count": len(criteria_fields_payload),
    }


def _schema_spec_payload(sheet_filter: Optional[str] = None) -> Dict[str, Any]:
    registry, source = _collect_schema_registry()

    if sheet_filter:
        wanted = _canonicalize_sheet_name(sheet_filter)
        spec = registry.get(wanted) or _get_sheet_spec(wanted)
        if spec is None:
            raise HTTPException(status_code=404, detail=f"Unknown sheet: {wanted}")
        payload = _sheet_spec_to_payload(wanted, spec)
        return _to_jsonable(
            {
                "status": "success",
                "version": ADVANCED_ANALYSIS_VERSION,
                "source": source,
                "sheet": wanted,
                "spec": payload,
            }
        )

    sheets_payload: Dict[str, Any] = {}
    for sheet_name in sorted(registry.keys()):
        try:
            sheets_payload[sheet_name] = _sheet_spec_to_payload(sheet_name, registry[sheet_name])
        except Exception as e:
            sheets_payload[sheet_name] = {
                "sheet": sheet_name,
                "error": f"{type(e).__name__}: {e}",
            }

    return _to_jsonable(
        {
            "status": "success",
            "version": ADVANCED_ANALYSIS_VERSION,
            "source": source,
            "sheet_count": len(sheets_payload),
            "pages": list(sheets_payload.keys()),
            "sheets": sheets_payload,
        }
    )


# -----------------------------------------------------------------------------
# Engine fallback fetcher
# -----------------------------------------------------------------------------
async def _engine_fetch_any(
    engine: Any,
    *,
    sheet: str,
    limit: int,
    offset: int,
    mode: str,
    body: Dict[str, Any],
) -> Any:
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
# Insights helpers
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


def _extract_criteria_from_body(body: Dict[str, Any]) -> Dict[str, Any]:
    crit: Dict[str, Any] = {}
    if isinstance(body.get("criteria"), dict):
        crit.update(body["criteria"])

    try:
        from core.sheets.schema_registry import get_sheet_spec  # type: ignore

        spec = get_sheet_spec("Insights_Analysis")
        cfs = getattr(spec, "criteria_fields", None) or ()
        for cf in cfs:
            k = str(_column_attr(cf, "key", "") or "").strip()
            if not k:
                continue
            if k in body and body.get(k) is not None:
                crit[k] = body.get(k)
    except Exception:
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
    t0 = time.perf_counter()
    headers, keys, schema_source = _schema_headers_keys("Insights_Analysis")

    if not headers or not keys:
        keys = ["section", "item", "symbol", "metric", "value", "notes", "last_updated_riyadh"]
        headers = ["Section", "Item", "Symbol", "Metric", "Value", "Notes", "Last Updated (Riyadh)"]
        schema_source = "fallback"

    engine = await _get_engine(request)
    crit = _extract_criteria_from_body(body)
    universes = _extract_universes_from_body(body)
    symbols = _get_list(body, "symbols", "tickers", "tickers_list")

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
# Top10 helpers (builder-first)
# -----------------------------------------------------------------------------
def _extract_top10_criteria_from_body(body: Dict[str, Any]) -> Dict[str, Any]:
    crit: Dict[str, Any] = {}
    if isinstance(body.get("criteria"), dict):
        crit.update(body["criteria"])

    for k in (
        "pages_selected",
        "invest_period_days",
        "horizon",
        "top_n",
        "min_expected_roi",
        "max_risk_score",
        "min_confidence",
        "min_volume",
        "use_liquidity_tiebreak",
        "enforce_risk_confidence",
        "risk_level",
        "confidence_level",
        "required_return_pct",
    ):
        if k in body and body.get(k) is not None and k not in crit:
            crit[k] = body.get(k)
    return crit


async def _build_top10_payload(
    *,
    request: Request,
    body: Dict[str, Any],
    mode: str,
    include_matrix: bool,
    limit: int,
    offset: int,
) -> Dict[str, Any]:
    t0 = time.perf_counter()
    headers, keys, schema_source = _schema_headers_keys("Top_10_Investments")
    engine = await _get_engine(request)
    crit = _extract_top10_criteria_from_body(body)
    symbols = _get_list(body, "symbols", "tickers", "tickers_list", "direct_symbols")

    if not headers or not keys:
        payload = {
            "status": "degraded",
            "page": "Top_10_Investments",
            "sheet": "Top_10_Investments",
            "headers": [],
            "keys": [],
            "rows": [],
            "rows_matrix": [] if include_matrix else None,
            "version": ADVANCED_ANALYSIS_VERSION,
            "meta": {
                "schema_source": "missing",
                "engine_available": bool(engine),
                "reason": "Top_10_Investments schema unavailable",
                "duration_ms": (time.perf_counter() - t0) * 1000.0,
            },
        }
        return _to_jsonable(payload)

    try:
        mod = __import__("core.analysis.top10_selector", fromlist=["*"])
    except Exception as e:
        return _to_jsonable(
            {
                "status": "degraded",
                "page": "Top_10_Investments",
                "sheet": "Top_10_Investments",
                "headers": headers,
                "keys": keys,
                "rows": [],
                "rows_matrix": [] if include_matrix else None,
                "version": ADVANCED_ANALYSIS_VERSION,
                "meta": {
                    "schema_source": schema_source,
                    "engine_available": bool(engine),
                    "top10_import_error": f"{type(e).__name__}: {e}",
                    "duration_ms": (time.perf_counter() - t0) * 1000.0,
                },
            }
        )

    fn_names = (
        "build_top10_output_rows",
        "build_top10_rows",
        "select_top10",
        "select_top10_symbols",
    )

    call_variants = [
        lambda fn: fn(
            engine=engine,
            request=body.get("request") or body,
            settings=body.get("settings") or crit or {},
            body=body,
            mode=mode or "",
            limit=limit,
        ),
        lambda fn: fn(
            engine=engine,
            request=body.get("request") or body,
            settings=body.get("settings") or crit or {},
            mode=mode or "",
            limit=limit,
        ),
        lambda fn: fn(
            engine=engine,
            criteria=crit or None,
            symbols=symbols or None,
            body=body,
            mode=mode or "",
            limit=limit,
        ),
        lambda fn: fn(
            engine=engine,
            criteria=crit or None,
            symbols=symbols or None,
            mode=mode or "",
            limit=limit,
        ),
        lambda fn: fn(engine=engine, body=body, mode=mode or "", limit=limit),
        lambda fn: fn(engine=engine, mode=mode or "", limit=limit),
    ]

    result: Any = None
    dispatch = "none"
    last_err: Optional[Exception] = None

    for fn_name in fn_names:
        fn = getattr(mod, fn_name, None)
        if not callable(fn):
            continue
        for call_variant in call_variants:
            try:
                out = call_variant(fn)
                if hasattr(out, "__await__"):
                    out = await out
                result = out
                dispatch = fn_name
                raise StopIteration
            except StopIteration:
                break
            except TypeError as e:
                last_err = e
                continue
            except Exception as e:
                last_err = e
                continue
        if dispatch != "none":
            break

    if dispatch == "none":
        return _to_jsonable(
            {
                "status": "degraded",
                "page": "Top_10_Investments",
                "sheet": "Top_10_Investments",
                "headers": headers,
                "keys": keys,
                "rows": [],
                "rows_matrix": [] if include_matrix else None,
                "version": ADVANCED_ANALYSIS_VERSION,
                "meta": {
                    "schema_source": schema_source,
                    "engine_available": bool(engine),
                    "dispatch": dispatch,
                    "top10_error": str(last_err) if last_err else "No compatible top10 function found",
                    "duration_ms": (time.perf_counter() - t0) * 1000.0,
                },
            }
        )

    if isinstance(result, list) and result and all(isinstance(x, str) for x in result):
        rows = []
        for i, sym in enumerate(result[offset : offset + limit], start=offset + 1):
            rows.append(
                _project_row(
                    keys,
                    _normalize_row_to_schema(
                        "Top_10_Investments",
                        {
                            "symbol": sym,
                            "top10_rank": i,
                            "selection_reason": "Selected by top10_selector symbols-only result.",
                            "criteria_snapshot": _json_dumps_safe(crit),
                            "last_updated_utc": datetime.now(timezone.utc).isoformat(),
                        },
                    ),
                )
            )
        return _to_jsonable(
            {
                "status": "partial",
                "page": "Top_10_Investments",
                "sheet": "Top_10_Investments",
                "headers": headers,
                "keys": keys,
                "rows": rows,
                "rows_matrix": _rows_to_matrix(rows, keys) if include_matrix else None,
                "version": ADVANCED_ANALYSIS_VERSION,
                "meta": {
                    "schema_source": schema_source,
                    "engine_available": bool(engine),
                    "dispatch": dispatch,
                    "symbols_only_result": True,
                    "duration_ms": (time.perf_counter() - t0) * 1000.0,
                },
            }
        )

    rows_raw, _, _ = _coerce_payload_to_rows(result)

    norm_rows: List[Dict[str, Any]] = []
    for r in rows_raw:
        if not isinstance(r, dict):
            continue
        rr = _normalize_row_to_schema("Top_10_Investments", dict(r))
        norm_rows.append(_project_row(keys, rr))

    norm_rows = norm_rows[offset : offset + limit]

    return _to_jsonable(
        {
            "status": "success",
            "page": "Top_10_Investments",
            "sheet": "Top_10_Investments",
            "headers": headers,
            "keys": keys,
            "rows": norm_rows,
            "rows_matrix": _rows_to_matrix(norm_rows, keys) if include_matrix else None,
            "version": ADVANCED_ANALYSIS_VERSION,
            "meta": {
                "schema_source": schema_source,
                "engine_available": bool(engine),
                "engine_type": _safe_engine_type(engine) if engine else "none",
                "dispatch": dispatch,
                "criteria": crit,
                "direct_symbols": symbols,
                "rows": len(norm_rows),
                "duration_ms": (time.perf_counter() - t0) * 1000.0,
            },
        }
    )


# -----------------------------------------------------------------------------
# Route replacement helpers
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


_removed_root_sheet_rows = _remove_router_route_inplace(_root_sheet_rows_router, "/sheet-rows", "POST")
if _removed_root_sheet_rows:
    logger.info(
        "AdvancedAnalysis: removed %s existing POST /sheet-rows route(s) for schema-driven replacement.",
        _removed_root_sheet_rows,
    )


# -----------------------------------------------------------------------------
# Shared endpoints
# -----------------------------------------------------------------------------
@schema_router_v1.get("/sheet-spec")
@schema_router_v1.get("/spec")
@schema_router_compat.get("/sheet-spec")
@schema_router_compat.get("/spec")
async def schema_sheet_spec(
    sheet: Optional[str] = Query(default=None, description="Optional sheet/page name"),
    token: Optional[str] = Query(default=None, description="Auth token (query only if allowed)"),
    x_app_token: Optional[str] = Header(default=None, alias="X-APP-TOKEN"),
    authorization: Optional[str] = Header(default=None, alias="Authorization"),
) -> Dict[str, Any]:
    _require_auth_or_401(token_query=token, x_app_token=x_app_token, authorization=authorization)
    return _schema_spec_payload(sheet_filter=sheet)


@_root_sheet_rows_router.post("/sheet-rows")
@advanced_router.post("/sheet-rows")
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

    if sheet == "Insights_Analysis":
        payload = await _build_insights_payload(
            request=request,
            body=body,
            mode=mode or "",
            include_matrix=bool(include_matrix_final),
            limit=limit,
            offset=offset,
        )
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

    if sheet == "Top_10_Investments":
        payload = await _build_top10_payload(
            request=request,
            body=body,
            mode=mode or "",
            include_matrix=bool(include_matrix_final),
            limit=limit,
            offset=offset,
        )
        payload = dict(payload) if isinstance(payload, dict) else {"status": "success", "rows": []}
        payload["request_id"] = request_id
        payload.setdefault("meta", {})
        if isinstance(payload["meta"], dict):
            payload["meta"].update(
                {
                    "path": "top10_selector",
                    "schema_source": schema_source or payload["meta"].get("schema_source"),
                    "limit": limit,
                    "offset": offset,
                    "duration_ms": (time.perf_counter() - t0) * 1000.0,
                }
            )
        payload["version"] = ADVANCED_ANALYSIS_VERSION
        return _to_jsonable(payload)

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
                "replaced_root_sheet_rows_routes": _removed_root_sheet_rows,
                "duration_ms": (time.perf_counter() - t0) * 1000.0,
            },
        }
    )


@advanced_router.get("/health")
async def advanced_health(request: Request) -> Dict[str, Any]:
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
            "replaced_root_sheet_rows_routes": _removed_root_sheet_rows,
            "base_router_import_error": _base_router_import_error,
            "insights_builder_wired": True,
            "top10_builder_wired": True,
        }
    )


@advanced_router.get("/metrics")
async def advanced_metrics() -> Response:
    if not _PROMETHEUS_AVAILABLE or generate_latest is None:
        return Response(content="Metrics not available", media_type="text/plain", status_code=503)
    return Response(content=generate_latest(), media_type=CONTENT_TYPE_LATEST)


@advanced_router.get("/insights-criteria")
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


@advanced_router.post("/insights-analysis")
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


@advanced_router.post("/top10-investments")
async def top10_investments(
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
    _require_auth_or_401(token_query=token, x_app_token=x_app_token, authorization=authorization)
    request_id = x_request_id or getattr(request.state, "request_id", None) or str(uuid.uuid4())

    include_matrix_final = include_matrix if isinstance(include_matrix, bool) else _get_bool(body, "include_matrix", True)

    payload = await _build_top10_payload(
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
        payload["meta"]["endpoint"] = "/v1/advanced/top10-investments"
    return _to_jsonable(payload)


# -----------------------------------------------------------------------------
# Final router composition
# -----------------------------------------------------------------------------
router.include_router(_root_sheet_rows_router)
router.include_router(advanced_router)
router.include_router(schema_router_v1)
router.include_router(schema_router_compat)

__all__ = ["router"]
