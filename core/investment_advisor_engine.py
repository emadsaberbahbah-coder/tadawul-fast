#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
routes/investment_advisor.py
--------------------------------------------------------------------------------
ADVANCED TOP10 / INVESTMENT ADVISOR ROUTER — v2.4.0
--------------------------------------------------------------------------------
ALIGNMENT-FIRST • SHARED-RESOLVER READY • BUILDER-RESOLUTION SAFE • SCHEMA-FIRST
TIMEOUT-GUARDED • UNCONSTRAINED-SAFE • FALLBACK-SAFE • APP-STATE / MODULE /
ENGINE TOLERANT • GET+POST ALIAS SAFE • TOP10-FIELD GUARANTEED • JSON-SAFE
CONTRACT-HARDENED

Why this revision
-----------------
- ✅ FIX: adds shared resolver support for advisor-like runners, including:
      - run_investment_advisor_engine
      - run_investment_advisor
      - run_advisor
      - execute_investment_advisor
      - execute_advisor
- ✅ FIX: exports `_resolve_advisor_runner(...)` so other route families can
      reuse the same runtime discovery logic safely.
- ✅ FIX: Top10 builder discovery is stricter first, then safely falls back to
      advisor-runner discovery when appropriate.
- ✅ FIX: payload normalization is broader and safer:
      - dict payloads
      - list payloads
      - rows_matrix payloads
      - recommendations/items/data/rows/result shapes
      - single-row dict payloads
- ✅ FIX: schema extraction is more tolerant across multiple schema modules.
- ✅ FIX: engine fallback first tries true Top10/advisor methods, then generic
      page-row methods for Top_10_Investments only if needed.
- ✅ FIX: Top10 special fields always backfilled:
      - top10_rank
      - selection_reason
      - criteria_snapshot
- ✅ FIX: response envelope is aligned and stable:
      - headers + keys + display_headers + rows + rows_matrix + data + items + meta
- ✅ SAFE: no network calls at import time.

Primary endpoints
-----------------
- POST /v1/advanced/top10-investments
- POST /v1/advanced/top10
- POST /v1/advanced/investment-advisor
- POST /v1/advanced/advisor
- POST /v1/advanced/run
- GET  /v1/advanced/recommendations
- GET  /v1/advanced/sheet-rows
- GET  /v1/advanced/health
- GET  /v1/advanced/metrics
"""

from __future__ import annotations

import asyncio
import copy
import importlib
import inspect
import json
import logging
import os
import time
import uuid
from typing import Any, Callable, Dict, Iterable, List, Mapping, Optional, Sequence, Tuple

from fastapi import APIRouter, Body, Header, HTTPException, Query, Request, Response, status
from fastapi.encoders import jsonable_encoder

logger = logging.getLogger("routes.investment_advisor")
logger.addHandler(logging.NullHandler())

INVESTMENT_ADVISOR_VERSION = "2.4.0"
TOP10_PAGE_NAME = "Top_10_Investments"

_BASE_SOURCE_PAGES = [
    "Market_Leaders",
    "Global_Markets",
    "Mutual_Funds",
    "Commodities_FX",
    "My_Portfolio",
]

_DERIVED_OR_NON_SOURCE_PAGES = {
    "KSA_TADAWUL",
    "Advisor_Criteria",
    "AI_Opportunity_Report",
    "Insights_Analysis",
    "Top_10_Investments",
    "Data_Dictionary",
}

# Primary modules for Top10/advisor discovery
_BUILDER_MODULE_CANDIDATES = (
    "core.analysis.top10_selector",
    "core.analysis.top10_builder",
    "core.investment_advisor",
    "core.investment_advisor_engine",
    "core.analysis.investment_advisor",
    "core.analysis.investment_advisor_engine",
    "core.advisor",
    "core.advisor_engine",
)

# Strict Top10 builder names
_TOP10_BUILDER_NAME_CANDIDATES = (
    "build_top10_rows",
    "build_top10_output_rows",
    "build_top10",
    "build_top10_payload",
    "build_top10_result",
    "build_top10_investments_rows",
    "build_top_10_investments_rows",
    "select_top10",
    "select_top10_symbols",
)

# Advisor-like runner names
_ADVISOR_RUNNER_NAME_CANDIDATES = (
    "run_investment_advisor_engine",
    "run_investment_advisor",
    "run_advisor",
    "execute_investment_advisor",
    "execute_advisor",
    "recommend",
    "recommend_investments",
    "get_recommendations",
    "build_recommendations",
    "run",
    "run_async",
    "execute",
)

# Combined builder-like names
_BUILDER_NAME_CANDIDATES = _TOP10_BUILDER_NAME_CANDIDATES + _ADVISOR_RUNNER_NAME_CANDIDATES

# Holder/object names that may expose a builder-like callable
_CONTAINER_NAME_CANDIDATES = (
    "top10_selector",
    "top10_builder",
    "advisor_service",
    "investment_advisor_service",
    "advisor_engine",
    "investment_advisor_engine",
    "advisor_runner",
    "investment_advisor_runner",
    "advisor",
    "investment_advisor",
    "service",
    "engine",
    "runner",
    "AdvisorService",
    "InvestmentAdvisorService",
    "AdvisorEngine",
    "InvestmentAdvisorEngine",
    "Top10Selector",
    "Top10Builder",
    "Advisor",
    "InvestmentAdvisor",
    "create_top10_selector",
    "create_top10_builder",
    "get_top10_selector",
    "get_top10_builder",
    "create_investment_advisor",
    "get_investment_advisor",
    "build_investment_advisor",
    "create_advisor",
    "get_advisor",
    "build_advisor",
    "create_engine_adapter",
)

# app.state candidates: direct callable attrs or object attrs
_STATE_ATTR_CANDIDATES = (
    "top10_builder",
    "top10_selector",
    "investment_advisor_runner",
    "advisor_runner",
    "build_top10_rows",
    "select_top10",
    "select_top10_symbols",
    "run_investment_advisor_engine",
    "run_investment_advisor",
    "run_advisor",
    "investment_advisor_service",
    "advisor_service",
    "investment_advisor_engine",
    "advisor_engine",
    "advisor",
    "investment_advisor",
)

# Engine fallback methods only
_ENGINE_TOP10_METHOD_CANDIDATES = (
    "build_top10_rows",
    "build_top10_output_rows",
    "select_top10",
    "select_top10_symbols",
    "run_investment_advisor_engine",
    "run_investment_advisor",
    "run_advisor",
    "recommend",
    "get_recommendations",
)

_ENGINE_SHEET_METHOD_CANDIDATES = (
    "get_sheet_rows",
    "get_page_rows",
    "sheet_rows",
    "build_sheet_rows",
)

_SCHEMA_MODULE_CANDIDATES = (
    "core.sheets.schema_registry",
    "core.schema_registry",
    "core.page_catalog",
    "core.schemas",
    "core.schema",
)

_SCHEMA_FN_CANDIDATES = (
    "get_sheet_spec",
    "get_page_spec",
    "get_schema_for_page",
    "sheet_spec",
    "build_sheet_spec",
)

router = APIRouter(prefix="/v1/advanced", tags=["advanced"])


# =============================================================================
# Optional Prometheus
# =============================================================================
try:
    from prometheus_client import CONTENT_TYPE_LATEST, generate_latest  # type: ignore

    _PROMETHEUS_AVAILABLE = True
except Exception:
    CONTENT_TYPE_LATEST = "text/plain"
    generate_latest = None  # type: ignore
    _PROMETHEUS_AVAILABLE = False


# =============================================================================
# Optional auth/config
# =============================================================================
try:
    from core.config import auth_ok, get_settings_cached, is_open_mode  # type: ignore
except Exception:
    auth_ok = None  # type: ignore
    is_open_mode = None  # type: ignore

    def get_settings_cached(*args: Any, **kwargs: Any) -> Any:  # type: ignore
        return None


# =============================================================================
# Generic helpers
# =============================================================================
def _s(v: Any) -> str:
    try:
        if v is None:
            return ""
        s = str(v).strip()
        return "" if s.lower() in {"none", "null", "nil"} else s
    except Exception:
        return ""


def _is_blank(v: Any) -> bool:
    return v is None or (isinstance(v, str) and not v.strip())


def _safe_int(v: Any, default: int) -> int:
    try:
        if isinstance(v, bool):
            return default
        return int(float(v))
    except Exception:
        return default


def _safe_float(v: Any, default: float) -> float:
    try:
        if isinstance(v, bool):
            return default
        return float(v)
    except Exception:
        return default


def _coerce_bool(v: Any, default: bool = False) -> bool:
    if isinstance(v, bool):
        return v
    if isinstance(v, str):
        s = v.strip().lower()
        if s in {"1", "true", "yes", "y", "on"}:
            return True
        if s in {"0", "false", "no", "n", "off"}:
            return False
    if isinstance(v, (int, float)):
        try:
            return bool(int(v))
        except Exception:
            return default
    return default


def _jsonable_snapshot(value: Any) -> Any:
    try:
        return jsonable_encoder(value)
    except Exception:
        try:
            return json.loads(json.dumps(value, default=str))
        except Exception:
            return str(value)


def _json_compact(value: Any) -> str:
    try:
        return json.dumps(_jsonable_snapshot(value), ensure_ascii=False, separators=(",", ":"))
    except Exception:
        return str(value)


def _rows_to_matrix(rows: List[Dict[str, Any]], keys: List[str]) -> List[List[Any]]:
    return [[row.get(k) for k in keys] for row in rows]


def _env_int(name: str, default: int, *, lo: Optional[int] = None, hi: Optional[int] = None) -> int:
    try:
        raw = os.getenv(name, "").strip()
        val = int(raw) if raw else int(default)
    except Exception:
        val = int(default)

    if lo is not None:
        val = max(lo, val)
    if hi is not None:
        val = min(hi, val)
    return val


def _env_float(name: str, default: float, *, lo: Optional[float] = None, hi: Optional[float] = None) -> float:
    try:
        raw = os.getenv(name, "").strip()
        val = float(raw) if raw else float(default)
    except Exception:
        val = float(default)

    if lo is not None:
        val = max(lo, val)
    if hi is not None:
        val = min(hi, val)
    return val


def _env_csv(name: str, default: Sequence[str]) -> List[str]:
    raw = os.getenv(name, "").strip()
    if not raw:
        return [str(x).strip() for x in default if str(x).strip()]

    out: List[str] = []
    seen = set()
    for part in raw.replace(";", ",").split(","):
        item = part.strip()
        if item and item not in seen:
            seen.add(item)
            out.append(item)
    return out


def _normalize_list(value: Any) -> List[str]:
    if value is None:
        return []

    if isinstance(value, str):
        seq = [x.strip() for x in value.replace(";", ",").replace("\n", ",").split(",") if x.strip()]
    elif isinstance(value, (list, tuple, set)):
        seq = list(value)
    else:
        seq = [value]

    out: List[str] = []
    seen = set()
    for item in seq:
        s = _s(item)
        if s and s not in seen:
            seen.add(s)
            out.append(s)
    return out


def _dedupe_keep_order(values: Iterable[Any]) -> List[str]:
    out: List[str] = []
    seen = set()
    for value in values:
        s = _s(value)
        if not s or s in seen:
            continue
        seen.add(s)
        out.append(s)
    return out


def _safe_source_pages(values: Sequence[str]) -> List[str]:
    out: List[str] = []
    seen = set()
    for item in values:
        s = _s(item)
        if not s or s in _DERIVED_OR_NON_SOURCE_PAGES:
            continue
        if s not in seen:
            seen.add(s)
            out.append(s)
    return out


def _get_request_id(request: Optional[Request], header_request_id: Optional[str] = None) -> str:
    if _s(header_request_id):
        return _s(header_request_id)

    try:
        if request is not None:
            rid = request.headers.get("X-Request-ID")
            if rid:
                return str(rid).strip()
            state_rid = getattr(getattr(request, "state", None), "request_id", None)
            if state_rid:
                return str(state_rid).strip()
    except Exception:
        pass

    return str(uuid.uuid4())


def _model_to_dict(obj: Any) -> Dict[str, Any]:
    if obj is None:
        return {}
    if isinstance(obj, dict):
        return dict(obj)
    if isinstance(obj, Mapping):
        try:
            return dict(obj)
        except Exception:
            return {}

    try:
        if hasattr(obj, "model_dump") and callable(getattr(obj, "model_dump")):
            dumped = obj.model_dump(mode="python")
            if isinstance(dumped, dict):
                return dumped
    except Exception:
        pass

    try:
        if hasattr(obj, "dict") and callable(getattr(obj, "dict")):
            dumped = obj.dict()
            if isinstance(dumped, dict):
                return dumped
    except Exception:
        pass

    try:
        if hasattr(obj, "__dict__"):
            d = vars(obj)
            if isinstance(d, dict):
                return dict(d)
    except Exception:
        pass

    return {"result": obj}


def _extract_rows_candidate(payload: Any) -> List[Dict[str, Any]]:
    if isinstance(payload, list):
        if payload and isinstance(payload[0], dict):
            return [dict(x) for x in payload if isinstance(x, dict)]
        return []

    if not isinstance(payload, dict):
        return []

    for key in ("rows", "recommendations", "data", "items", "results", "records", "quotes"):
        value = payload.get(key)
        if isinstance(value, list) and value and isinstance(value[0], dict):
            return [dict(x) for x in value if isinstance(x, dict)]

    result = payload.get("result")
    if isinstance(result, list) and result and isinstance(result[0], dict):
        return [dict(x) for x in result if isinstance(x, dict)]

    if isinstance(payload.get("row"), dict):
        return [dict(payload["row"])]

    return []


def _extract_matrix_candidate(payload: Any) -> Optional[List[List[Any]]]:
    if not isinstance(payload, dict):
        return None

    rows_matrix = payload.get("rows_matrix")
    if isinstance(rows_matrix, list):
        out: List[List[Any]] = []
        for row in rows_matrix:
            if isinstance(row, (list, tuple)):
                out.append(list(row))
            else:
                out.append([row])
        return out

    rows = payload.get("rows")
    if isinstance(rows, list) and rows and isinstance(rows[0], (list, tuple)):
        return [list(r) if isinstance(r, (list, tuple)) else [r] for r in rows]

    data = payload.get("data")
    if isinstance(data, list) and data and isinstance(data[0], (list, tuple)):
        return [list(r) if isinstance(r, (list, tuple)) else [r] for r in data]

    return None


def _extract_keys_headers(payload: Any) -> Tuple[List[str], List[str]]:
    if not isinstance(payload, dict):
        return [], []

    headers = (
        payload.get("display_headers")
        or payload.get("sheet_headers")
        or payload.get("column_headers")
        or payload.get("headers")
        or []
    )
    keys = payload.get("keys") or payload.get("fields") or []

    if not isinstance(headers, list):
        headers = []
    if not isinstance(keys, list):
        keys = []

    clean_headers = [_s(x) for x in headers if _s(x)]
    clean_keys = [_s(x) for x in keys if _s(x)]
    return clean_headers, clean_keys


def _derive_keys_from_rows(rows: List[Dict[str, Any]]) -> List[str]:
    out: List[str] = []
    seen = set()
    for row in rows:
        if not isinstance(row, dict):
            continue
        for key in row.keys():
            k = _s(key)
            if k and k not in seen:
                seen.add(k)
                out.append(k)
    return out


async def _call_maybe_async(fn: Callable[..., Any], *args: Any, **kwargs: Any) -> Any:
    if inspect.iscoroutinefunction(fn):
        return await fn(*args, **kwargs)

    out = await asyncio.to_thread(fn, *args, **kwargs)
    if inspect.isawaitable(out):
        return await out
    return out


# =============================================================================
# Auth helpers
# =============================================================================
def _extract_auth_token(
    *,
    token_query: Optional[str],
    x_app_token: Optional[str],
    authorization: Optional[str],
) -> str:
    auth_token = _s(x_app_token)

    authz = _s(authorization)
    if authz.lower().startswith("bearer "):
        auth_token = authz.split(" ", 1)[1].strip()

    if token_query and not auth_token:
        allow_query = False
        try:
            settings = get_settings_cached()
            allow_query = bool(getattr(settings, "allow_query_token", False))
        except Exception:
            allow_query = False
        if allow_query:
            auth_token = _s(token_query)

    return auth_token


def _is_public_path(path: str) -> bool:
    p = _s(path)
    if not p:
        return False

    if p in {
        "/v1/advanced/health",
        "/v1/advanced/metrics",
    }:
        return True

    env_paths = os.getenv("PUBLIC_PATHS", "") or os.getenv("AUTH_PUBLIC_PATHS", "")
    for raw in env_paths.split(","):
        candidate = raw.strip()
        if not candidate:
            continue
        if candidate.endswith("*") and p.startswith(candidate[:-1]):
            return True
        if p == candidate:
            return True

    return False


def _auth_passed(
    *,
    request: Request,
    token_query: Optional[str],
    x_app_token: Optional[str],
    authorization: Optional[str],
) -> bool:
    try:
        if callable(is_open_mode) and bool(is_open_mode()):
            return True
    except Exception:
        pass

    try:
        path = str(getattr(getattr(request, "url", None), "path", "") or "")
    except Exception:
        path = ""

    if _is_public_path(path):
        return True

    if auth_ok is None:
        return True

    auth_token = _extract_auth_token(
        token_query=token_query,
        x_app_token=x_app_token,
        authorization=authorization,
    )

    headers_dict = dict(request.headers)
    settings = None
    try:
        settings = get_settings_cached()
    except Exception:
        settings = None

    call_attempts = [
        {
            "token": auth_token or None,
            "authorization": authorization,
            "headers": headers_dict,
            "path": path,
            "request": request,
            "settings": settings,
        },
        {
            "token": auth_token or None,
            "authorization": authorization,
            "headers": headers_dict,
            "path": path,
            "request": request,
        },
        {
            "token": auth_token or None,
            "authorization": authorization,
            "headers": headers_dict,
            "path": path,
        },
        {
            "token": auth_token or None,
            "authorization": authorization,
            "headers": headers_dict,
        },
        {
            "token": auth_token or None,
            "authorization": authorization,
        },
        {
            "token": auth_token or None,
        },
        {},
    ]

    for kwargs in call_attempts:
        try:
            return bool(auth_ok(**kwargs))
        except TypeError:
            continue
        except Exception:
            return False

    return False


def _require_auth_or_401(
    *,
    request: Request,
    token_query: Optional[str],
    x_app_token: Optional[str],
    authorization: Optional[str],
) -> None:
    if not _auth_passed(
        request=request,
        token_query=token_query,
        x_app_token=x_app_token,
        authorization=authorization,
    ):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid token",
        )


# =============================================================================
# Engine accessor
# =============================================================================
async def _get_engine(request: Request) -> Optional[Any]:
    try:
        st = getattr(request.app, "state", None)
        if st and getattr(st, "engine", None):
            return st.engine
    except Exception:
        pass

    for modpath in ("core.data_engine_v2", "core.data_engine"):
        try:
            mod = importlib.import_module(modpath)
            get_engine = getattr(mod, "get_engine", None)
            if callable(get_engine):
                eng = get_engine()
                if inspect.isawaitable(eng):
                    eng = await eng
                return eng
        except Exception:
            continue

    return None


# =============================================================================
# Schema helpers
# =============================================================================
def _ensure_top10_keys_present(keys: List[str], headers: List[str]) -> Tuple[List[str], List[str]]:
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


def _extract_columns_from_spec(spec: Any) -> List[Tuple[str, str]]:
    columns: List[Tuple[str, str]] = []

    # object-style spec.columns
    raw_columns = None
    if isinstance(spec, dict):
        raw_columns = spec.get("columns") or spec.get("fields")
    else:
        raw_columns = getattr(spec, "columns", None) or getattr(spec, "fields", None)

    if isinstance(raw_columns, list):
        for col in raw_columns:
            if isinstance(col, dict):
                key = _s(col.get("key") or col.get("field") or col.get("name") or col.get("id"))
                header = _s(col.get("header") or col.get("title") or col.get("label") or key)
            else:
                key = _s(getattr(col, "key", None) or getattr(col, "field", None) or getattr(col, "name", None))
                header = _s(getattr(col, "header", None) or getattr(col, "title", None) or getattr(col, "label", None) or key)

            if key:
                columns.append((key, header or key))

    # dict-style already separated
    if not columns and isinstance(spec, dict):
        keys = spec.get("keys") or []
        headers = spec.get("headers") or spec.get("display_headers") or []
        if isinstance(keys, list):
            for idx, key in enumerate(keys):
                k = _s(key)
                h = _s(headers[idx]) if isinstance(headers, list) and idx < len(headers) else k
                if k:
                    columns.append((k, h or k))

    return columns


def _load_schema_defaults(page_name: str = TOP10_PAGE_NAME) -> Tuple[List[str], List[str]]:
    for mod_name in _SCHEMA_MODULE_CANDIDATES:
        try:
            mod = importlib.import_module(mod_name)
        except Exception:
            continue

        for fn_name in _SCHEMA_FN_CANDIDATES:
            fn = getattr(mod, fn_name, None)
            if not callable(fn):
                continue

            attempts = [
                {"sheet": page_name},
                {"page": page_name},
                {"sheet_name": page_name},
                {"name": page_name},
            ]

            for kwargs in attempts:
                try:
                    spec = fn(**kwargs)
                    cols = _extract_columns_from_spec(spec)
                    if cols:
                        keys = [k for k, _ in cols]
                        headers = [h for _, h in cols]
                        keys, headers = _ensure_top10_keys_present(keys, headers)
                        return headers, keys
                except TypeError:
                    continue
                except Exception:
                    continue

    keys = [
        "symbol",
        "name",
        "current_price",
        "expected_roi_3m",
        "forecast_confidence",
        "risk_score",
        "overall_score",
        "recommendation",
        "last_updated_riyadh",
        "top10_rank",
        "selection_reason",
        "criteria_snapshot",
    ]
    headers = [
        "Symbol",
        "Name",
        "Current Price",
        "Expected ROI 3M",
        "Forecast Confidence",
        "Risk Score",
        "Overall Score",
        "Recommendation",
        "Last Updated (Riyadh)",
        "Top10 Rank",
        "Selection Reason",
        "Criteria Snapshot",
    ]
    return headers, keys


def _schema_payload(
    *,
    request_id: str,
    headers: List[str],
    keys: List[str],
    include_matrix: bool,
    meta: Dict[str, Any],
    status_text: str = "success",
    error_text: Optional[str] = None,
    detail_text: Optional[str] = None,
) -> Dict[str, Any]:
    return {
        "status": status_text,
        "page": TOP10_PAGE_NAME,
        "sheet": TOP10_PAGE_NAME,
        "route_family": "top10",
        "headers": headers,
        "display_headers": headers,
        "sheet_headers": headers,
        "column_headers": headers,
        "keys": keys,
        "rows": [],
        "data": [],
        "items": [],
        "rows_matrix": [] if (include_matrix and keys) else None,
        "error": error_text,
        "detail": detail_text,
        "version": INVESTMENT_ADVISOR_VERSION,
        "request_id": request_id,
        "meta": meta,
    }


# =============================================================================
# Criteria normalization
# =============================================================================
def _flatten_criteria(body: Dict[str, Any]) -> Dict[str, Any]:
    crit: Dict[str, Any] = {}

    if isinstance(body.get("criteria"), dict):
        crit.update(body["criteria"])

    if isinstance(body.get("filters"), dict):
        crit.update(body["filters"])

    settings = body.get("settings")
    if isinstance(settings, dict) and isinstance(settings.get("criteria"), dict):
        crit.update(settings["criteria"])

    for k in (
        "pages_selected",
        "pages",
        "selected_pages",
        "sources",
        "page",
        "sheet",
        "sheet_name",
        "direct_symbols",
        "symbols",
        "tickers",
        "limit",
        "top_n",
        "invest_period_days",
        "investment_period_days",
        "horizon_days",
        "invest_period_label",
        "min_expected_roi",
        "min_roi",
        "max_risk_score",
        "risk_level",
        "risk_profile",
        "min_confidence",
        "min_ai_confidence",
        "confidence_level",
        "confidence_bucket",
        "min_volume",
        "use_liquidity_tiebreak",
        "enforce_risk_confidence",
        "include_positions",
        "enrich_final",
        "preview",
        "schema_only",
        "mode",
    ):
        if k in body and body.get(k) is not None:
            crit[k] = body.get(k)

    return crit


def _effective_limit(body: Dict[str, Any], limit_q: Optional[int]) -> int:
    max_limit = _env_int("ADV_TOP10_MAX_LIMIT", 50, lo=1, hi=200)
    default_limit = _env_int("ADV_TOP10_DEFAULT_LIMIT", 10, lo=1, hi=max_limit)

    if isinstance(limit_q, int):
        eff = limit_q
    else:
        eff = _safe_int(
            body.get("limit")
            or body.get("top_n")
            or body.get("criteria", {}).get("top_n")
            or default_limit,
            default_limit,
        )

    return max(1, min(max_limit, int(eff)))


def _prepare_effective_criteria(body: Dict[str, Any], eff_limit: int) -> Tuple[Dict[str, Any], Dict[str, Any]]:
    crit = _flatten_criteria(body or {})

    pages = (
        _normalize_list(crit.get("pages_selected"))
        or _normalize_list(crit.get("pages"))
        or _normalize_list(crit.get("selected_pages"))
        or _normalize_list(crit.get("sources"))
        or _normalize_list(crit.get("page"))
        or _normalize_list(crit.get("sheet"))
        or _normalize_list(crit.get("sheet_name"))
    )
    pages = _safe_source_pages(pages)

    direct_symbols = _normalize_list(crit.get("direct_symbols") or crit.get("symbols") or crit.get("tickers"))

    request_unconstrained = (not pages) and (not direct_symbols)
    pages_explicit = bool(pages)

    if request_unconstrained:
        pages = _safe_source_pages(_env_csv("ADV_TOP10_DEFAULT_PAGES", ["Market_Leaders", "Global_Markets"]))
        crit["pages_selected"] = pages

    max_pages = _env_int("ADV_TOP10_MAX_PAGES", 5, lo=1, hi=20)
    pages_trimmed = False
    if pages and len(pages) > max_pages:
        pages = pages[:max_pages]
        crit["pages_selected"] = pages
        pages_trimmed = True

    if direct_symbols:
        crit["direct_symbols"] = direct_symbols

    risk_level = _s(crit.get("risk_level")) or _s(crit.get("risk_profile")) or "moderate"
    crit["risk_level"] = risk_level
    crit["risk_profile"] = risk_level

    if _is_blank(crit.get("invest_period_days")):
        if not _is_blank(crit.get("investment_period_days")):
            crit["invest_period_days"] = crit.get("investment_period_days")
        elif not _is_blank(crit.get("horizon_days")):
            crit["invest_period_days"] = crit.get("horizon_days")
        else:
            crit["invest_period_days"] = 90

    if _is_blank(crit.get("horizon_days")) and not _is_blank(crit.get("invest_period_days")):
        crit["horizon_days"] = crit.get("invest_period_days")

    if _is_blank(crit.get("min_roi")) and not _is_blank(crit.get("min_expected_roi")):
        crit["min_roi"] = crit.get("min_expected_roi")
    if _is_blank(crit.get("min_expected_roi")) and not _is_blank(crit.get("min_roi")):
        crit["min_expected_roi"] = crit.get("min_roi")

    confidence_bucket = _s(crit.get("confidence_bucket")) or _s(crit.get("confidence_level"))
    if confidence_bucket:
        crit["confidence_bucket"] = confidence_bucket
        crit["confidence_level"] = confidence_bucket

    crit["top_n"] = eff_limit
    crit["limit"] = eff_limit

    if "enrich_final" not in crit:
        crit["enrich_final"] = False if request_unconstrained else (len(pages) <= 2 or bool(direct_symbols))

    prep_meta = {
        "request_unconstrained": request_unconstrained,
        "pages_explicit": pages_explicit,
        "pages_effective": list(pages),
        "direct_symbols_count": len(direct_symbols),
        "pages_trimmed": pages_trimmed,
        "allow_row_fallback": True,
    }
    return crit, prep_meta


def _narrow_criteria_for_fallback(criteria: Dict[str, Any], eff_limit: int) -> Dict[str, Any]:
    narrowed = copy.deepcopy(criteria)

    fallback_pages_cap = _env_int("ADV_TOP10_FALLBACK_MAX_PAGES", 2, lo=1, hi=10)
    fallback_top_n = _env_int("ADV_TOP10_FALLBACK_TOP_N", min(3, eff_limit), lo=1, hi=eff_limit)

    direct_symbols = _normalize_list(
        narrowed.get("direct_symbols") or narrowed.get("symbols") or narrowed.get("tickers")
    )
    pages = _normalize_list(
        narrowed.get("pages_selected") or narrowed.get("pages") or narrowed.get("selected_pages")
    )
    pages = _safe_source_pages(pages)

    if direct_symbols:
        narrowed["direct_symbols"] = direct_symbols[: max(1, min(len(direct_symbols), eff_limit))]
        narrowed["top_n"] = min(eff_limit, len(narrowed["direct_symbols"]))
        narrowed["limit"] = narrowed["top_n"]
    else:
        if not pages:
            pages = _safe_source_pages(_env_csv("ADV_TOP10_DEFAULT_PAGES", ["Market_Leaders"]))
        pages = pages[:fallback_pages_cap]
        narrowed["pages_selected"] = pages
        narrowed["top_n"] = fallback_top_n
        narrowed["limit"] = fallback_top_n

    narrowed["enrich_final"] = False
    return narrowed


# =============================================================================
# Top10 field helpers
# =============================================================================
def _canonical_selection_reason(row: Dict[str, Any]) -> Optional[str]:
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
    criteria_snapshot = _json_compact(criteria) if criteria else None
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
    if not keys:
        return [dict(r) for r in rows if isinstance(r, dict)]

    normalized: List[Dict[str, Any]] = []
    for row in rows:
        if not isinstance(row, dict):
            continue
        normalized.append({k: row.get(k, None) for k in keys})
    return normalized


# =============================================================================
# Builder discovery
# =============================================================================
def _callable_by_names(container: Any, names: Iterable[str]) -> Optional[Tuple[Callable[..., Any], str]]:
    for name in names:
        try:
            fn = getattr(container, name, None)
        except Exception:
            fn = None
        if callable(fn):
            return fn, name
    return None


async def _materialize_holder(holder: Any) -> Any:
    if holder is None:
        return None

    try:
        if inspect.isclass(holder):
            return holder()
    except Exception:
        return None

    if callable(holder):
        try:
            return await _call_maybe_async(holder)
        except TypeError:
            return None
        except Exception:
            return None

    return holder


async def _resolve_from_container(
    container: Any,
    label: str,
    direct_names: Sequence[str],
    holder_names: Sequence[str],
) -> Optional[Tuple[Callable[..., Any], str, str]]:
    direct = _callable_by_names(container, direct_names)
    if direct:
        fn, fn_name = direct
        return fn, label, fn_name

    for holder_name in holder_names:
        try:
            holder = getattr(container, holder_name, None)
        except Exception:
            holder = None

        if holder is None:
            continue

        obj = await _materialize_holder(holder)
        if obj is None:
            continue

        # If the holder itself resolves to a callable function/service, accept it.
        if callable(obj) and not inspect.isclass(obj):
            return obj, label, holder_name

        direct_obj = _callable_by_names(obj, direct_names)
        if direct_obj:
            fn, fn_name = direct_obj
            return fn, label, f"{holder_name}.{fn_name}"

    return None


async def _resolve_advisor_runner(
    request: Request,
    engine: Any = None,
) -> Tuple[Optional[Callable[..., Any]], str, str, List[str]]:
    searched: List[str] = []
    import_errors: List[str] = []

    try:
        st = getattr(request.app, "state", None)
    except Exception:
        st = None

    if st is not None:
        searched.append("app.state")

        resolved_state = await _resolve_from_container(
            st,
            "app.state",
            tuple(list(_ADVISOR_RUNNER_NAME_CANDIDATES) + list(_STATE_ATTR_CANDIDATES)),
            tuple(list(_STATE_ATTR_CANDIDATES) + list(_CONTAINER_NAME_CANDIDATES)),
        )
        if resolved_state:
            fn, src, name = resolved_state
            return fn, src, name, searched

    for mod_name in _BUILDER_MODULE_CANDIDATES:
        searched.append(mod_name)
        try:
            mod = importlib.import_module(mod_name)
        except Exception as e:
            import_errors.append(f"{mod_name}: {type(e).__name__}: {e}")
            continue

        resolved = await _resolve_from_container(
            mod,
            mod_name,
            _ADVISOR_RUNNER_NAME_CANDIDATES,
            _CONTAINER_NAME_CANDIDATES,
        )
        if resolved:
            fn, src, name = resolved
            return fn, src, name, searched

    if engine is not None:
        searched.append("engine")
        resolved_engine = await _resolve_from_container(
            engine,
            "engine",
            _ADVISOR_RUNNER_NAME_CANDIDATES,
            _CONTAINER_NAME_CANDIDATES,
        )
        if resolved_engine:
            fn, src, name = resolved_engine
            return fn, src, name, searched

    if import_errors:
        logger.warning("Advisor runner import errors: %s", import_errors)

    return None, "", "", searched


async def _resolve_top10_builder(
    request: Request,
    engine: Any,
) -> Tuple[Optional[Callable[..., Any]], str, str, List[str]]:
    searched: List[str] = []
    import_errors: List[str] = []

    try:
        st = getattr(request.app, "state", None)
    except Exception:
        st = None

    if st is not None:
        searched.append("app.state")

        resolved_state = await _resolve_from_container(
            st,
            "app.state",
            tuple(list(_TOP10_BUILDER_NAME_CANDIDATES) + list(_STATE_ATTR_CANDIDATES)),
            tuple(list(_STATE_ATTR_CANDIDATES) + list(_CONTAINER_NAME_CANDIDATES)),
        )
        if resolved_state:
            fn, src, name = resolved_state
            return fn, src, name, searched

    for mod_name in _BUILDER_MODULE_CANDIDATES:
        searched.append(mod_name)
        try:
            mod = importlib.import_module(mod_name)
        except Exception as e:
            import_errors.append(f"{mod_name}: {type(e).__name__}: {e}")
            continue

        resolved = await _resolve_from_container(
            mod,
            mod_name,
            _TOP10_BUILDER_NAME_CANDIDATES,
            _CONTAINER_NAME_CANDIDATES,
        )
        if resolved:
            fn, src, name = resolved
            return fn, src, name, searched

    if engine is not None:
        searched.append("engine")
        resolved_engine = await _resolve_from_container(
            engine,
            "engine",
            _ENGINE_TOP10_METHOD_CANDIDATES,
            _CONTAINER_NAME_CANDIDATES,
        )
        if resolved_engine:
            fn, src, name = resolved_engine
            return fn, src, name, searched

    # Final safe fallback: advisor-runner discovery
    runner, runner_source, runner_name, runner_search = await _resolve_advisor_runner(request, engine)
    if runner is not None:
        searched.extend([x for x in runner_search if x not in searched])
        return runner, runner_source, runner_name, searched

    if import_errors:
        logger.warning("Top10 builder import errors: %s", import_errors)

    return None, "", "", searched


# =============================================================================
# Builder / engine invocation
# =============================================================================
async def _call_builder_with_tolerance(
    builder: Callable[..., Any],
    *,
    request: Request,
    engine: Any,
    criteria: Dict[str, Any],
    eff_limit: int,
    mode: str,
    settings: Any,
) -> Any:
    body_payload = {
        "page": TOP10_PAGE_NAME,
        "sheet": TOP10_PAGE_NAME,
        "sheet_name": TOP10_PAGE_NAME,
        "criteria": criteria,
        "top_n": eff_limit,
        "limit": eff_limit,
        "mode": mode or "",
        "advisor_data_mode": mode or "",
    }

    attempts = [
        {
            "engine": engine,
            "criteria": criteria,
            "limit": eff_limit,
            "top_n": eff_limit,
            "mode": mode or "",
            "advisor_data_mode": mode or "",
            "request": request,
            "settings": settings,
            "page": TOP10_PAGE_NAME,
            "sheet": TOP10_PAGE_NAME,
        },
        {
            "engine": engine,
            "criteria": criteria,
            "limit": eff_limit,
            "mode": mode or "",
            "settings": settings,
        },
        {
            "engine": engine,
            "criteria": criteria,
            "limit": eff_limit,
            "mode": mode or "",
        },
        {
            "engine": engine,
            "body": body_payload,
            "limit": eff_limit,
            "top_n": eff_limit,
            "mode": mode or "",
            "request": request,
            "settings": settings,
        },
        {
            "engine": engine,
            "payload": body_payload,
            "limit": eff_limit,
            "mode": mode or "",
        },
        {
            "payload": body_payload,
            "engine": engine,
            "settings": settings,
        },
        {
            "body": body_payload,
            "engine": engine,
            "settings": settings,
        },
        {
            "request": request,
            "settings": settings,
            "body": body_payload,
            "engine": engine,
        },
        {
            "criteria": criteria,
            "engine": engine,
        },
        {
            "payload": body_payload,
        },
        {
            "body": body_payload,
        },
        {
            "criteria": criteria,
        },
    ]

    for kwargs in attempts:
        try:
            return await _call_maybe_async(builder, **kwargs)
        except TypeError:
            continue

    positional_attempts = [
        (engine, criteria, eff_limit),
        (criteria, engine, eff_limit),
        (criteria, engine),
        (engine, criteria),
        (criteria,),
        (body_payload,),
    ]

    last_error: Optional[Exception] = None
    for args in positional_attempts:
        try:
            return await _call_maybe_async(builder, *args)
        except TypeError as e:
            last_error = e
            continue

    if last_error is not None:
        raise last_error

    raise RuntimeError("No compatible builder signature matched")


async def _run_selector_with_timeout(
    *,
    builder: Callable[..., Any],
    request: Request,
    engine: Any,
    criteria: Dict[str, Any],
    eff_limit: int,
    mode: str,
    timeout_sec: float,
    settings: Any,
) -> Any:
    coro = _call_builder_with_tolerance(
        builder,
        request=request,
        engine=engine,
        criteria=criteria,
        eff_limit=eff_limit,
        mode=mode,
        settings=settings,
    )
    return await asyncio.wait_for(coro, timeout=timeout_sec)


async def _try_engine_top10_fallback(
    *,
    engine: Any,
    request: Request,
    criteria: Dict[str, Any],
    eff_limit: int,
    mode: str,
    settings: Any,
) -> Optional[Dict[str, Any]]:
    if engine is None:
        return None

    # First: true engine Top10/advisor methods
    for method_name in _ENGINE_TOP10_METHOD_CANDIDATES:
        fn = getattr(engine, method_name, None)
        if not callable(fn):
            continue

        try:
            out = await _call_builder_with_tolerance(
                fn,
                request=request,
                engine=engine,
                criteria=criteria,
                eff_limit=eff_limit,
                mode=mode,
                settings=settings,
            )
        except Exception:
            continue

        if isinstance(out, dict):
            payload = dict(out)
            meta = payload.get("meta") if isinstance(payload.get("meta"), dict) else {}
            meta["dispatch"] = f"engine.{method_name}"
            payload["meta"] = meta
            return payload

        rows = _extract_rows_candidate(out)
        if rows:
            return {
                "status": "success",
                "page": TOP10_PAGE_NAME,
                "sheet": TOP10_PAGE_NAME,
                "rows": rows,
                "meta": {"dispatch": f"engine.{method_name}"},
            }

    # Second: generic page-row methods only as fallback
    for method_name in _ENGINE_SHEET_METHOD_CANDIDATES:
        fn = getattr(engine, method_name, None)
        if not callable(fn):
            continue

        attempts = [
            {
                "page": TOP10_PAGE_NAME,
                "sheet": TOP10_PAGE_NAME,
                "sheet_name": TOP10_PAGE_NAME,
                "limit": eff_limit,
                "mode": mode or "",
                "body": {
                    "page": TOP10_PAGE_NAME,
                    "sheet": TOP10_PAGE_NAME,
                    "criteria": criteria,
                    "top_n": eff_limit,
                    "limit": eff_limit,
                    "mode": mode or "",
                },
            },
            {
                "page": TOP10_PAGE_NAME,
                "sheet": TOP10_PAGE_NAME,
                "limit": eff_limit,
                "mode": mode or "",
            },
            {
                "page": TOP10_PAGE_NAME,
                "sheet": TOP10_PAGE_NAME,
                "limit": eff_limit,
            },
            {
                "page": TOP10_PAGE_NAME,
            },
        ]

        out = None
        for kwargs in attempts:
            try:
                out = await _call_maybe_async(fn, **kwargs)
                break
            except TypeError:
                continue
            except Exception:
                out = None
                break

        if out is None:
            continue

        if isinstance(out, dict):
            payload = dict(out)
            meta = payload.get("meta") if isinstance(payload.get("meta"), dict) else {}
            meta["dispatch"] = f"engine.{method_name}"
            payload["meta"] = meta
            return payload

        rows = _extract_rows_candidate(out)
        if rows:
            return {
                "status": "success",
                "page": TOP10_PAGE_NAME,
                "sheet": TOP10_PAGE_NAME,
                "rows": rows,
                "meta": {"dispatch": f"engine.{method_name}"},
            }

    return None


# =============================================================================
# Payload normalization
# =============================================================================
def _normalize_selector_payload(
    payload: Any,
    *,
    criteria_used: Dict[str, Any],
    eff_limit: int,
    forced_dispatch: str = "",
) -> Tuple[List[str], List[str], List[Dict[str, Any]], Dict[str, Any], str, str]:
    if isinstance(payload, dict):
        payload_dict = dict(payload)
    elif isinstance(payload, list):
        payload_dict = {"rows": payload}
    else:
        payload_dict = _model_to_dict(payload)

    headers, keys = _extract_keys_headers(payload_dict)

    rows = _extract_rows_candidate(payload_dict)
    matrix = _extract_matrix_candidate(payload_dict)

    if not rows and matrix and keys:
        rows = [{k: (row[idx] if idx < len(row) else None) for idx, k in enumerate(keys)} for row in matrix]

    if not headers or not keys:
        schema_headers, schema_keys = _load_schema_defaults()
        if not keys and rows:
            keys = _derive_keys_from_rows(rows)
        if not keys:
            keys = schema_keys
        if not headers:
            headers = schema_headers

    keys, headers = _ensure_top10_keys_present(list(keys), list(headers))

    status_out = _s(payload_dict.get("status")) or ("success" if rows else "partial")
    detail_out = _s(payload_dict.get("detail") or payload_dict.get("error") or payload_dict.get("message"))

    dict_rows = [dict(r) for r in rows if isinstance(r, dict)]
    dict_rows = _apply_top10_field_backfill(dict_rows, keys=keys, criteria=criteria_used)
    dict_rows = _rank_rows_in_order(dict_rows)
    norm_rows = _ensure_schema_projection(dict_rows, keys)
    norm_rows = norm_rows[:eff_limit]

    meta = payload_dict.get("meta") if isinstance(payload_dict.get("meta"), dict) else {}
    if forced_dispatch and not _s(meta.get("dispatch")):
        meta["dispatch"] = forced_dispatch

    return headers, keys, norm_rows, dict(meta), status_out, detail_out


# =============================================================================
# Core executor
# =============================================================================
async def _execute_advanced_top10(
    *,
    request: Request,
    body: Dict[str, Any],
    mode: str,
    include_matrix: Optional[bool],
    limit: Optional[int],
    schema_only: Optional[bool],
    x_request_id: Optional[str],
) -> Dict[str, Any]:
    t0 = time.perf_counter()
    stages: Dict[str, float] = {}
    request_id = _get_request_id(request, x_request_id)

    include_matrix_final = include_matrix if isinstance(include_matrix, bool) else _coerce_bool(body.get("include_matrix"), True)
    schema_only_final = schema_only if isinstance(schema_only, bool) else _coerce_bool(body.get("schema_only"), False)
    eff_limit = _effective_limit(body or {}, limit)

    schema_headers, schema_keys = _load_schema_defaults()
    schema_keys, schema_headers = _ensure_top10_keys_present(schema_keys, schema_headers)

    if schema_only_final:
        meta = {
            "route_version": INVESTMENT_ADVISOR_VERSION,
            "request_id": request_id,
            "limit": eff_limit,
            "mode": mode or "",
            "schema_aligned": bool(schema_keys),
            "build_status": "SCHEMA_ONLY",
            "dispatch": "advanced_top10",
            "stage_durations_ms": stages,
            "duration_ms": round((time.perf_counter() - t0) * 1000.0, 3),
        }
        return jsonable_encoder(
            _schema_payload(
                request_id=request_id,
                headers=schema_headers,
                keys=schema_keys,
                include_matrix=include_matrix_final,
                meta=meta,
                status_text="success",
            )
        )

    s1 = time.perf_counter()
    engine = await _get_engine(request)
    stages["engine_ms"] = round((time.perf_counter() - s1) * 1000.0, 3)

    if engine is None:
        meta = {
            "route_version": INVESTMENT_ADVISOR_VERSION,
            "request_id": request_id,
            "limit": eff_limit,
            "mode": mode or "",
            "schema_aligned": bool(schema_keys),
            "build_status": "DEGRADED",
            "dispatch": "advanced_top10",
            "warning": "engine_unavailable",
            "stage_durations_ms": stages,
            "duration_ms": round((time.perf_counter() - t0) * 1000.0, 3),
        }
        return jsonable_encoder(
            _schema_payload(
                request_id=request_id,
                headers=schema_headers,
                keys=schema_keys,
                include_matrix=include_matrix_final,
                meta=meta,
                status_text="partial",
                error_text="Data engine unavailable",
                detail_text="Data engine unavailable",
            )
        )

    s2 = time.perf_counter()
    settings = None
    try:
        settings = get_settings_cached()
    except Exception:
        settings = None
    stages["settings_ms"] = round((time.perf_counter() - s2) * 1000.0, 3)

    s3 = time.perf_counter()
    builder, builder_source, builder_name, builder_search_path = await _resolve_top10_builder(request, engine)
    stages["builder_resolve_ms"] = round((time.perf_counter() - s3) * 1000.0, 3)

    s4 = time.perf_counter()
    effective_criteria, prep_meta = _prepare_effective_criteria(body or {}, eff_limit)
    stages["criteria_prepare_ms"] = round((time.perf_counter() - s4) * 1000.0, 3)

    primary_timeout_sec = _env_float("ADV_TOP10_TIMEOUT_SEC", 45.0, lo=3.0, hi=300.0)
    fallback_timeout_sec = _env_float("ADV_TOP10_FALLBACK_TIMEOUT_SEC", 15.0, lo=2.0, hi=120.0)

    selected_payload: Optional[Any] = None
    selected_criteria = copy.deepcopy(effective_criteria)
    fallback_used = False
    fallback_reason = ""
    warnings: List[str] = []
    normalize_detail = ""

    # Primary builder path
    if builder is not None:
        s5 = time.perf_counter()
        try:
            selected_payload = await _run_selector_with_timeout(
                builder=builder,
                request=request,
                engine=engine,
                criteria=effective_criteria,
                eff_limit=eff_limit,
                mode=mode or "",
                timeout_sec=primary_timeout_sec,
                settings=settings,
            )
        except asyncio.TimeoutError:
            fallback_used = True
            fallback_reason = f"primary_timeout_{primary_timeout_sec}s"
            warnings.append("primary_selector_timeout")
        except Exception as e:
            fallback_used = True
            fallback_reason = f"primary_error:{type(e).__name__}"
            warnings.append(f"primary_selector_error:{type(e).__name__}:{e}")
        stages["primary_selector_ms"] = round((time.perf_counter() - s5) * 1000.0, 3)
    else:
        fallback_used = True
        fallback_reason = "builder_unavailable"
        warnings.append("top10_builder_unavailable")

    # If unconstrained and empty, force fallback path
    try:
        normalized_rows_probe = _extract_rows_candidate(selected_payload)
        if prep_meta.get("request_unconstrained") and len(normalized_rows_probe) == 0:
            fallback_used = True
            fallback_reason = fallback_reason or "primary_empty_unconstrained"
            warnings.append("primary_empty_for_unconstrained_request")
            selected_payload = None
    except Exception:
        pass

    # Fallback builder path with narrowed criteria
    if selected_payload is None:
        s6 = time.perf_counter()
        narrowed_criteria = _narrow_criteria_for_fallback(effective_criteria, eff_limit)

        if builder is not None:
            try:
                selected_payload = await _run_selector_with_timeout(
                    builder=builder,
                    request=request,
                    engine=engine,
                    criteria=narrowed_criteria,
                    eff_limit=min(eff_limit, _safe_int(narrowed_criteria.get("top_n"), eff_limit)),
                    mode=mode or "",
                    timeout_sec=fallback_timeout_sec,
                    settings=settings,
                )
                selected_criteria = narrowed_criteria
            except asyncio.TimeoutError:
                warnings.append("fallback_selector_timeout")
                fallback_reason = (fallback_reason + "; " if fallback_reason else "") + f"fallback_timeout_{fallback_timeout_sec}s"
            except Exception as e:
                warnings.append(f"fallback_selector_error:{type(e).__name__}:{e}")
                fallback_reason = (fallback_reason + "; " if fallback_reason else "") + f"fallback_error:{type(e).__name__}"
        stages["fallback_selector_ms"] = round((time.perf_counter() - s6) * 1000.0, 3)

        # Engine fallback
        if selected_payload is None:
            s7 = time.perf_counter()
            engine_payload = await _try_engine_top10_fallback(
                engine=engine,
                request=request,
                criteria=narrowed_criteria,
                eff_limit=min(eff_limit, _safe_int(narrowed_criteria.get("top_n"), eff_limit)),
                mode=mode or "",
                settings=settings,
            )
            stages["engine_fallback_ms"] = round((time.perf_counter() - s7) * 1000.0, 3)

            if engine_payload is not None:
                selected_payload = engine_payload
                selected_criteria = narrowed_criteria
                warnings.append("engine_top10_fallback_used")
                fallback_used = True
                if not fallback_reason:
                    fallback_reason = "engine_top10_fallback"

    # Final degraded schema-safe payload
    if selected_payload is None:
        meta = {
            "route_version": INVESTMENT_ADVISOR_VERSION,
            "request_id": request_id,
            "limit": eff_limit,
            "mode": mode or "",
            "schema_aligned": bool(schema_keys),
            "build_status": "DEGRADED",
            "dispatch": "advanced_top10",
            "request_unconstrained": prep_meta.get("request_unconstrained", False),
            "pages_explicit": prep_meta.get("pages_explicit", False),
            "pages_effective": prep_meta.get("pages_effective", []),
            "direct_symbols_count": prep_meta.get("direct_symbols_count", 0),
            "pages_trimmed": prep_meta.get("pages_trimmed", False),
            "allow_row_fallback": prep_meta.get("allow_row_fallback", True),
            "fallback_used": fallback_used,
            "fallback_reason": fallback_reason,
            "criteria_used": _jsonable_snapshot(selected_criteria),
            "warnings": warnings,
            "engine_present": True,
            "engine_type": type(engine).__name__,
            "builder_available": bool(builder),
            "builder_source": builder_source,
            "builder_name": builder_name,
            "builder_search_path": builder_search_path,
            "stage_durations_ms": stages,
            "duration_ms": round((time.perf_counter() - t0) * 1000.0, 3),
        }
        return jsonable_encoder(
            _schema_payload(
                request_id=request_id,
                headers=schema_headers,
                keys=schema_keys,
                include_matrix=include_matrix_final,
                meta=meta,
                status_text="partial",
                error_text="No advanced Top10 payload could be produced",
                detail_text="No advanced Top10 payload could be produced",
            )
        )

    # Normalize final payload
    s8 = time.perf_counter()
    headers, keys, norm_rows, meta_in, status_out, normalize_detail = _normalize_selector_payload(
        selected_payload,
        criteria_used=selected_criteria,
        eff_limit=eff_limit,
        forced_dispatch="advanced_top10",
    )
    stages["normalize_ms"] = round((time.perf_counter() - s8) * 1000.0, 3)

    build_status = _s(meta_in.get("build_status"))
    if not build_status:
        build_status = "OK" if norm_rows else "WARN"

    meta_warnings = meta_in.get("warnings")
    merged_warnings: List[str] = []
    if isinstance(meta_warnings, list):
        merged_warnings.extend([_s(x) for x in meta_warnings if _s(x)])
    merged_warnings.extend([_s(x) for x in warnings if _s(x)])

    dedup_warnings: List[str] = []
    seen_warn = set()
    for w in merged_warnings:
        if w and w not in seen_warn:
            seen_warn.add(w)
            dedup_warnings.append(w)

    meta = dict(meta_in)
    meta.update(
        {
            "route_version": INVESTMENT_ADVISOR_VERSION,
            "request_id": request_id,
            "limit": eff_limit,
            "mode": mode or "",
            "duration_ms": round((time.perf_counter() - t0) * 1000.0, 3),
            "schema_aligned": bool(keys),
            "top10_fields_backfilled": True,
            "criteria_used": _jsonable_snapshot(selected_criteria),
            "request_unconstrained": prep_meta.get("request_unconstrained", False),
            "pages_explicit": prep_meta.get("pages_explicit", False),
            "pages_effective": prep_meta.get("pages_effective", []),
            "direct_symbols_count": prep_meta.get("direct_symbols_count", 0),
            "pages_trimmed": prep_meta.get("pages_trimmed", False),
            "allow_row_fallback": prep_meta.get("allow_row_fallback", True),
            "fallback_used": fallback_used,
            "fallback_reason": fallback_reason,
            "engine_present": True,
            "engine_type": type(engine).__name__,
            "builder_available": bool(builder),
            "builder_source": builder_source,
            "builder_name": builder_name,
            "builder_search_path": builder_search_path,
            "warnings": dedup_warnings,
            "build_status": build_status,
            "dispatch": _s(meta_in.get("dispatch")) or "advanced_top10",
            "stage_durations_ms": stages,
        }
    )

    response = {
        "status": status_out or ("success" if norm_rows else "partial"),
        "page": TOP10_PAGE_NAME,
        "sheet": TOP10_PAGE_NAME,
        "route_family": "top10",
        "headers": headers,
        "display_headers": headers,
        "sheet_headers": headers,
        "column_headers": headers,
        "keys": keys,
        "rows": norm_rows,
        "data": norm_rows,
        "items": norm_rows,
        "rows_matrix": _rows_to_matrix(norm_rows, keys) if (include_matrix_final and keys) else None,
        "error": None if norm_rows else "No Top10 rows returned",
        "detail": normalize_detail or (None if norm_rows else "No Top10 rows returned"),
        "version": INVESTMENT_ADVISOR_VERSION,
        "request_id": request_id,
        "meta": meta,
    }

    return jsonable_encoder(response)


# =============================================================================
# Health / Metrics
# =============================================================================
@router.get("/health")
async def advanced_health(request: Request) -> Dict[str, Any]:
    engine = await _get_engine(request)
    builder, builder_source, builder_name, builder_search_path = await _resolve_top10_builder(request, engine)
    runner, runner_source, runner_name, runner_search_path = await _resolve_advisor_runner(request, engine)

    return jsonable_encoder(
        {
            "status": "ok" if engine else "degraded",
            "service": "advanced_top10",
            "version": INVESTMENT_ADVISOR_VERSION,
            "engine_available": bool(engine),
            "engine_type": type(engine).__name__ if engine else "none",
            "builder_available": bool(builder),
            "builder_source": builder_source,
            "builder_name": builder_name,
            "builder_search_path": builder_search_path,
            "advisor_runner_available": bool(runner),
            "advisor_runner_source": runner_source,
            "advisor_runner_name": runner_name,
            "advisor_runner_search_path": runner_search_path,
        }
    )


@router.get("/metrics")
async def advanced_metrics() -> Response:
    if not _PROMETHEUS_AVAILABLE or generate_latest is None:
        return Response(content="Metrics not available", media_type="text/plain", status_code=503)
    return Response(content=generate_latest(), media_type=CONTENT_TYPE_LATEST)


# =============================================================================
# POST routes
# =============================================================================
@router.post("/top10-investments")
@router.post("/top10")
@router.post("/investment-advisor")
@router.post("/advisor")
@router.post("/run")
async def advanced_top10_investments(
    request: Request,
    body: Dict[str, Any] = Body(default_factory=dict),
    mode: str = Query(default="", description="Optional mode hint for selector"),
    include_matrix: Optional[bool] = Query(default=None, description="Return rows_matrix"),
    limit: Optional[int] = Query(default=None, ge=1, le=50, description="How many items to return"),
    schema_only: Optional[bool] = Query(default=None, description="Return schema only"),
    token: Optional[str] = Query(default=None, description="Auth token (query only if allowed)"),
    x_app_token: Optional[str] = Header(default=None, alias="X-APP-TOKEN"),
    authorization: Optional[str] = Header(default=None, alias="Authorization"),
    x_request_id: Optional[str] = Header(default=None, alias="X-Request-ID"),
) -> Dict[str, Any]:
    _require_auth_or_401(
        request=request,
        token_query=token,
        x_app_token=x_app_token,
        authorization=authorization,
    )

    return await _execute_advanced_top10(
        request=request,
        body=dict(body or {}),
        mode=mode,
        include_matrix=include_matrix,
        limit=limit,
        schema_only=schema_only,
        x_request_id=x_request_id,
    )


# =============================================================================
# GET aliases
# =============================================================================
@router.get("/recommendations")
@router.get("/sheet-rows")
async def advanced_top10_get(
    request: Request,
    symbols: Optional[str] = Query(default=None, description="Comma/space separated symbols"),
    tickers: Optional[str] = Query(default=None, description="Comma/space separated tickers"),
    direct_symbols: Optional[str] = Query(default=None, description="Comma/space separated direct symbols"),
    pages: Optional[str] = Query(default=None, description="Comma/space separated source pages"),
    sources: Optional[str] = Query(default=None, description="Comma/space separated source pages"),
    page: Optional[str] = Query(default=None),
    sheet: Optional[str] = Query(default=None),
    sheet_name: Optional[str] = Query(default=None),
    risk_level: Optional[str] = Query(default=None),
    risk_profile: Optional[str] = Query(default=None),
    confidence_level: Optional[str] = Query(default=None),
    confidence_bucket: Optional[str] = Query(default=None),
    investment_period_days: Optional[int] = Query(default=None, ge=1, le=3650),
    invest_period_days: Optional[int] = Query(default=None, ge=1, le=3650),
    horizon_days: Optional[int] = Query(default=None, ge=1, le=3650),
    min_expected_roi: Optional[float] = Query(default=None),
    min_roi: Optional[float] = Query(default=None),
    min_confidence: Optional[float] = Query(default=None),
    top_n: Optional[int] = Query(default=None, ge=1, le=50),
    limit: Optional[int] = Query(default=None, ge=1, le=50),
    mode: str = Query(default="", description="Optional mode hint for selector"),
    include_matrix: Optional[bool] = Query(default=None),
    schema_only: Optional[bool] = Query(default=None),
    token: Optional[str] = Query(default=None, description="Auth token (query only if allowed)"),
    x_app_token: Optional[str] = Header(default=None, alias="X-APP-TOKEN"),
    authorization: Optional[str] = Header(default=None, alias="Authorization"),
    x_request_id: Optional[str] = Header(default=None, alias="X-Request-ID"),
) -> Dict[str, Any]:
    _require_auth_or_401(
        request=request,
        token_query=token,
        x_app_token=x_app_token,
        authorization=authorization,
    )

    selected_direct_symbols = (
        _normalize_list(direct_symbols)
        or _normalize_list(symbols)
        or _normalize_list(tickers)
    )

    selected_pages = (
        _normalize_list(pages)
        or _normalize_list(sources)
        or _normalize_list(page)
        or _normalize_list(sheet)
        or _normalize_list(sheet_name)
    )

    effective_limit = limit if limit is not None else top_n
    effective_period = (
        investment_period_days
        if investment_period_days is not None
        else (invest_period_days if invest_period_days is not None else horizon_days)
    )

    body: Dict[str, Any] = {
        "direct_symbols": selected_direct_symbols,
        "pages_selected": selected_pages,
        "risk_level": _s(risk_level) or _s(risk_profile),
        "risk_profile": _s(risk_profile) or _s(risk_level),
        "confidence_level": _s(confidence_level),
        "confidence_bucket": _s(confidence_bucket),
        "investment_period_days": effective_period,
        "invest_period_days": effective_period,
        "horizon_days": horizon_days if horizon_days is not None else effective_period,
        "min_expected_roi": min_expected_roi,
        "min_roi": min_roi if min_roi is not None else min_expected_roi,
        "min_confidence": min_confidence,
        "top_n": top_n if top_n is not None else limit,
        "limit": effective_limit,
        "include_matrix": include_matrix,
        "schema_only": schema_only,
    }

    return await _execute_advanced_top10(
        request=request,
        body=body,
        mode=mode,
        include_matrix=include_matrix,
        limit=effective_limit,
        schema_only=schema_only,
        x_request_id=x_request_id,
    )


__all__ = [
    "router",
    "INVESTMENT_ADVISOR_VERSION",
    "_resolve_advisor_runner",
    "_resolve_top10_builder",
]
