#!/usr/bin/env python3
"""
routes/investment_advisor.py
================================================================================
ADVANCED INVESTMENT ADVISOR ROUTER — v2.9.0
================================================================================
OWNER-ALIGNED • SPECIAL-PAGE SAFE • ADVANCED-PREFIX OWNER • QUERY-PARSE SAFE •
CONTRACT-CORRECT • TOP10-HARDENED • BRIDGE-TOLERANT • JSON-SAFE • STARTUP-SAFE •
AUTH-TOLERANT

Why this revision
-----------------
- FIX: preserves special-page routing for `Insights_Analysis` and `Data_Dictionary`
       by preferring engine sheet-row methods instead of Top10-oriented runners.
- FIX: makes resolver order page-aware so `/v1/advanced/sheet-rows?page=Insights_Analysis`
       no longer falls into Top10/advanced-advisor builders.
- FIX: keeps known canonical pages on their exact schema contract rather than
       widening keys from stray payload fields.
- FIX: strengthens row projection so canonical keys can be populated from exact,
       case-insensitive, normalized, and header-style aliases.
- FIX: keeps `/v1/advanced` family ownership stable with GET + POST at the family
       root and all canonical aliases used by live tests.
- SAFE: no import-time network activity.
"""

from __future__ import annotations

import asyncio
import inspect
import json
import logging
import math
import os
import re
import time
import uuid
from dataclasses import asdict, is_dataclass
from datetime import date, datetime, time as dt_time, timezone
from decimal import Decimal
from enum import Enum
from importlib import import_module
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence, Tuple

from fastapi import APIRouter, Body, Header, HTTPException, Query, Request, Response, status
from fastapi.encoders import jsonable_encoder

logger = logging.getLogger("routes.investment_advisor")
logger.addHandler(logging.NullHandler())

INVESTMENT_ADVISOR_VERSION = "2.9.0"
ROUTE_FAMILY_NAME = "advanced"
ROUTE_OWNER_NAME = "investment_advisor"

TOP10_PAGE_NAME = "Top_10_Investments"
INSIGHTS_PAGE_NAME = "Insights_Analysis"
DATA_DICTIONARY_PAGE_NAME = "Data_Dictionary"

BASE_SOURCE_PAGES: Tuple[str, ...] = (
    "Market_Leaders",
    "Global_Markets",
    "Commodities_FX",
    "Mutual_Funds",
    "My_Portfolio",
)
DERIVED_PAGES = {TOP10_PAGE_NAME, INSIGHTS_PAGE_NAME}
SPECIAL_PAGES = {INSIGHTS_PAGE_NAME, DATA_DICTIONARY_PAGE_NAME}
_SOURCE_PAGES_SET = set(BASE_SOURCE_PAGES)

KNOWN_CANONICAL_HEADER_COUNTS: Dict[str, int] = {
    "Market_Leaders": 80,
    "Global_Markets": 80,
    "Commodities_FX": 80,
    "Mutual_Funds": 80,
    "My_Portfolio": 80,
    "Insights_Analysis": 7,
    "Top_10_Investments": 83,
    "Data_Dictionary": 9,
}

TOP10_SPECIAL_FIELDS: Tuple[str, ...] = (
    "top10_rank",
    "selection_reason",
    "criteria_snapshot",
)

PAGE_ALIAS_MAP: Dict[str, str] = {
    "top10": TOP10_PAGE_NAME,
    "top_10": TOP10_PAGE_NAME,
    "top-10": TOP10_PAGE_NAME,
    "top10investments": TOP10_PAGE_NAME,
    "top_10_investments": TOP10_PAGE_NAME,
    "top-10-investments": TOP10_PAGE_NAME,
    "top 10 investments": TOP10_PAGE_NAME,
    "investment_advisor": TOP10_PAGE_NAME,
    "investment-advisor": TOP10_PAGE_NAME,
    "investment advisor": TOP10_PAGE_NAME,
    "advisor": TOP10_PAGE_NAME,
    "insights": INSIGHTS_PAGE_NAME,
    "insight": INSIGHTS_PAGE_NAME,
    "insights_analysis": INSIGHTS_PAGE_NAME,
    "insights-analysis": INSIGHTS_PAGE_NAME,
    "insights analysis": INSIGHTS_PAGE_NAME,
    "data_dictionary": DATA_DICTIONARY_PAGE_NAME,
    "data-dictionary": DATA_DICTIONARY_PAGE_NAME,
    "data dictionary": DATA_DICTIONARY_PAGE_NAME,
    "dictionary": DATA_DICTIONARY_PAGE_NAME,
    "market_leaders": "Market_Leaders",
    "market-leaders": "Market_Leaders",
    "market leaders": "Market_Leaders",
    "global_markets": "Global_Markets",
    "global-markets": "Global_Markets",
    "global markets": "Global_Markets",
    "commodities_fx": "Commodities_FX",
    "commodities-fx": "Commodities_FX",
    "commodities fx": "Commodities_FX",
    "mutual_funds": "Mutual_Funds",
    "mutual-funds": "Mutual_Funds",
    "mutual funds": "Mutual_Funds",
    "my_portfolio": "My_Portfolio",
    "my-portfolio": "My_Portfolio",
    "my portfolio": "My_Portfolio",
}

TOP10_MODULE_CANDIDATES: Tuple[str, ...] = (
    "core.analysis.top10_selector",
    "core.analysis.top10_builder",
    "core.investment_advisor",
    "core.investment_advisor_engine",
    "core.analysis.investment_advisor",
    "core.analysis.investment_advisor_engine",
)
RUNNER_MODULE_CANDIDATES: Tuple[str, ...] = (
    "core.investment_advisor",
    "core.investment_advisor_engine",
    "core.analysis.investment_advisor",
    "core.analysis.investment_advisor_engine",
    "core.data_engine_v2",
    "core.data_engine",
)
BRIDGE_MODULE_CANDIDATES: Tuple[str, ...] = (
    "routes.analysis_sheet_rows",
    "routes.advanced_analysis",
    "routes.advanced_sheet_rows",
)
SCHEMA_MODULE_CANDIDATES: Tuple[str, ...] = (
    "core.sheets.schema_registry",
    "core.schema_registry",
    "core.sheets.page_catalog",
)

TOP10_FUNCTION_CANDIDATES: Tuple[str, ...] = (
    "build_top10_output_rows",
    "build_top10_rows",
    "build_top10_investments_rows",
    "build_top10_investments_output_rows",
    "build_top10_investments",
    "build_top10",
    "get_top10_rows",
    "select_top10",
    "select_top10_symbols",
)
GENERIC_RUNNER_FUNCTION_CANDIDATES: Tuple[str, ...] = (
    "get_sheet_rows",
    "get_page_rows",
    "sheet_rows",
    "build_sheet_rows",
    "fetch_sheet_rows",
    "read_sheet_rows",
    "run_sheet_rows",
    "execute_sheet_rows",
    "run_investment_advisor_engine",
    "run_investment_advisor",
    "run_advisor",
    "execute_investment_advisor",
    "execute_advisor",
)
TOP10_RUNNER_FUNCTION_CANDIDATES: Tuple[str, ...] = TOP10_FUNCTION_CANDIDATES + GENERIC_RUNNER_FUNCTION_CANDIDATES
BRIDGE_FUNCTION_CANDIDATES: Tuple[str, ...] = (
    "_analysis_sheet_rows_impl",
    "_run_analysis_sheet_rows_impl",
    "run_analysis_sheet_rows_impl",
    "_run_advanced_sheet_rows_impl",
    "run_advanced_sheet_rows_impl",
    "_run_sheet_rows_impl",
    "run_sheet_rows_impl",
    "_sheet_rows_impl",
)
GENERIC_ENGINE_METHOD_CANDIDATES: Tuple[str, ...] = (
    "get_sheet_rows",
    "get_page_rows",
    "sheet_rows",
    "fetch_sheet_rows",
    "build_sheet_rows",
    "read_sheet_rows",
    "run_sheet_rows",
    "execute_sheet_rows",
    "get_rows_for_sheet",
    "get_rows_for_page",
    "execute_sheet_rows",
    "run_analysis_sheet_rows",
    "build_analysis_sheet_rows",
)
TOP10_ENGINE_METHOD_CANDIDATES: Tuple[str, ...] = TOP10_FUNCTION_CANDIDATES + GENERIC_ENGINE_METHOD_CANDIDATES

SCHEMA_MAP_CANDIDATES: Tuple[str, ...] = (
    "SCHEMA_REGISTRY",
    "SHEET_SPEC",
    "SHEET_SPECS",
    "PAGE_SPECS",
    "SPECS",
    "SHEET_CONTRACTS",
    "STATIC_CANONICAL_SHEET_CONTRACTS",
)
SCHEMA_FUNCTION_CANDIDATES: Tuple[str, ...] = (
    "get_sheet_spec",
    "get_schema_for_sheet",
    "get_schema",
    "resolve_sheet_spec",
    "resolve_sheet_schema",
    "get_contract_for_sheet",
)

router = APIRouter(prefix="/v1/advanced", tags=["advanced"])

try:
    from prometheus_client import CONTENT_TYPE_LATEST, generate_latest  # type: ignore
    PROMETHEUS_AVAILABLE = True
except Exception:  # pragma: no cover
    CONTENT_TYPE_LATEST = "text/plain"
    generate_latest = None  # type: ignore
    PROMETHEUS_AVAILABLE = False

try:
    from core.config import auth_ok, get_settings_cached, is_open_mode  # type: ignore
except Exception:  # pragma: no cover
    auth_ok = None  # type: ignore
    is_open_mode = None  # type: ignore

    def get_settings_cached(*args: Any, **kwargs: Any) -> Any:  # type: ignore
        return None


_CANONICAL_TOP10_SCHEMA_FALLBACK: List[Tuple[str, str]] = [
    ("symbol", "Symbol"),
    ("name", "Name"),
    ("asset_class", "Asset Class"),
    ("exchange", "Exchange"),
    ("currency", "Currency"),
    ("country", "Country"),
    ("sector", "Sector"),
    ("industry", "Industry"),
    ("current_price", "Current Price"),
    ("price_change", "Price Change"),
    ("percent_change", "Percent Change"),
    ("risk_score", "Risk Score"),
    ("valuation_score", "Valuation Score"),
    ("overall_score", "Overall Score"),
    ("opportunity_score", "Opportunity Score"),
    ("risk_bucket", "Risk Bucket"),
    ("confidence_bucket", "Confidence Bucket"),
    ("recommendation", "Recommendation"),
    ("expected_roi_1m", "Expected ROI 1M"),
    ("expected_roi_3m", "Expected ROI 3M"),
    ("expected_roi_12m", "Expected ROI 12M"),
    ("forecast_confidence", "Forecast Confidence"),
    ("source_page", "Source Page"),
    ("row_status", "Row Status"),
    ("degraded", "Degraded"),
    ("top10_rank", "Top10 Rank"),
    ("selection_reason", "Selection Reason"),
    ("criteria_snapshot", "Criteria Snapshot"),
]
_CANONICAL_INSTRUMENT_SCHEMA_FALLBACK: List[Tuple[str, str]] = _CANONICAL_TOP10_SCHEMA_FALLBACK[:-3]
_CANONICAL_INSIGHTS_SCHEMA_FALLBACK: List[Tuple[str, str]] = [
    ("section", "Section"),
    ("item", "Item"),
    ("metric", "Metric"),
    ("value", "Value"),
    ("confidence", "Confidence"),
    ("notes", "Notes"),
    ("as_of_utc", "As Of UTC"),
]
_CANONICAL_DATA_DICTIONARY_SCHEMA_FALLBACK: List[Tuple[str, str]] = [
    ("sheet_name", "Sheet Name"),
    ("column_key", "Column Key"),
    ("header", "Header"),
    ("section", "Section"),
    ("data_type", "Data Type"),
    ("required", "Required"),
    ("description", "Description"),
    ("example", "Example"),
    ("order_index", "Order Index"),
]

_SCHEMA_CACHE: Dict[str, Tuple[List[str], List[str]]] = {}


def _s(v: Any) -> str:
    if v is None:
        return ""
    try:
        out = str(v).strip()
    except Exception:
        return ""
    return "" if out.lower() in {"none", "null", "nil", "undefined"} else out


def _safe_dict(v: Any) -> Dict[str, Any]:
    return dict(v) if isinstance(v, Mapping) else {}


def _safe_int(v: Any, default: int) -> int:
    try:
        if v is None or isinstance(v, bool):
            return default
        return int(float(v))
    except Exception:
        return default


def _safe_float(v: Any, default: float) -> float:
    try:
        if v is None or isinstance(v, bool):
            return default
        out = float(v)
        return default if math.isnan(out) or math.isinf(out) else out
    except Exception:
        return default


def _boolish(v: Any, default: bool = False) -> bool:
    if isinstance(v, bool):
        return v
    if v is None:
        return default
    s = _s(v).lower()
    if s in {"1", "true", "yes", "y", "on"}:
        return True
    if s in {"0", "false", "no", "n", "off"}:
        return False
    return default


def _dedupe_keep_order(values: Iterable[str]) -> List[str]:
    out: List[str] = []
    seen = set()
    for item in values:
        s = _s(item)
        if s and s not in seen:
            seen.add(s)
            out.append(s)
    return out


def _split_csv(text: str) -> List[str]:
    raw = (text or "").replace(";", ",").replace("\n", ",").replace("\t", ",")
    return _dedupe_keep_order(part for part in raw.split(","))


def _split_symbol_text(text: str) -> List[str]:
    raw = _s(text)
    if not raw:
        return []
    return _dedupe_keep_order(x for x in re.split(r"[\s,;]+", raw) if _s(x))


def _normalize_list(value: Any) -> List[str]:
    if value is None:
        return []
    if isinstance(value, str):
        return _split_csv(value)
    if isinstance(value, (list, tuple, set)):
        return _dedupe_keep_order(_s(item) for item in value)
    s = _s(value)
    return [s] if s else []


def _normalize_symbol_list(value: Any) -> List[str]:
    if value is None:
        return []
    if isinstance(value, str):
        return _split_symbol_text(value)
    if isinstance(value, (list, tuple, set)):
        out: List[str] = []
        seen = set()
        for item in value:
            parts = _split_symbol_text(item) if isinstance(item, str) else [_s(item)]
            for part in parts:
                if part and part not in seen:
                    seen.add(part)
                    out.append(part)
        return out
    s = _s(value)
    return [s] if s else []


def _slice_rows(rows: List[Dict[str, Any]], *, offset: int, limit: int) -> List[Dict[str, Any]]:
    start = max(0, int(offset or 0))
    if limit <= 0:
        return rows[start:]
    return rows[start : start + limit]


def _now_utc() -> str:
    return datetime.now(timezone.utc).isoformat()


def _request_id(request: Optional[Request], x_request_id: Optional[str]) -> str:
    rid = _s(x_request_id)
    if rid:
        return rid
    try:
        if request is not None:
            state_rid = _s(getattr(request.state, "request_id", ""))
            if state_rid:
                return state_rid
            hdr = _s(request.headers.get("X-Request-ID"))
            if hdr:
                return hdr
    except Exception:
        pass
    return uuid.uuid4().hex[:12]


def _json_safe(value: Any) -> Any:
    def _clean(obj: Any) -> Any:
        if obj is None:
            return None
        if isinstance(obj, (str, int, bool)):
            return obj
        if isinstance(obj, float):
            return None if math.isnan(obj) or math.isinf(obj) else obj
        if isinstance(obj, Decimal):
            try:
                f = float(obj)
                return None if math.isnan(f) or math.isinf(f) else f
            except Exception:
                return _s(obj)
        if isinstance(obj, (datetime, date, dt_time)):
            try:
                return obj.isoformat()
            except Exception:
                return _s(obj)
        if isinstance(obj, Enum):
            return _clean(obj.value)
        if is_dataclass(obj):
            try:
                return _clean(asdict(obj))
            except Exception:
                return _s(obj)
        if isinstance(obj, Mapping):
            return {str(k): _clean(v) for k, v in obj.items()}
        if isinstance(obj, (list, tuple, set)):
            return [_clean(x) for x in obj]
        if hasattr(obj, "model_dump") and callable(getattr(obj, "model_dump")):
            try:
                return _clean(obj.model_dump(mode="python"))
            except Exception:
                pass
        if hasattr(obj, "dict") and callable(getattr(obj, "dict")):
            try:
                return _clean(obj.dict())
            except Exception:
                pass
        try:
            return jsonable_encoder(obj)
        except Exception:
            return _s(obj)

    return _clean(value)


def _json_compact(value: Any) -> str:
    try:
        return json.dumps(_json_safe(value), ensure_ascii=False, separators=(",", ":"))
    except Exception:
        return _s(value)


def _canonicalize_name(raw: str) -> str:
    text = _s(raw).lower()
    text = re.sub(r"[\s\-]+", "_", text)
    text = re.sub(r"_+", "_", text)
    return text.strip("_")


def _normalize_page_name(value: Any) -> str:
    raw = _s(value)
    if not raw:
        return TOP10_PAGE_NAME

    try:
        from core.sheets.page_catalog import normalize_page_name as normalize_from_catalog  # type: ignore
        normalized = _s(normalize_from_catalog(raw))
        if normalized:
            return normalized
    except Exception:
        pass

    try:
        from core.sheets.page_catalog import resolve_page_name as resolve_from_catalog  # type: ignore
        normalized = _s(resolve_from_catalog(raw))
        if normalized:
            return normalized
    except Exception:
        pass

    compact = _canonicalize_name(raw)
    return PAGE_ALIAS_MAP.get(compact, PAGE_ALIAS_MAP.get(raw.strip().lower(), raw))


def _source_pages_only(values: Sequence[str]) -> List[str]:
    out: List[str] = []
    seen = set()
    for item in values:
        page = _normalize_page_name(item)
        if page in _SOURCE_PAGES_SET and page not in seen:
            seen.add(page)
            out.append(page)
    return out


def _page_family(page: str) -> str:
    normalized = _normalize_page_name(page)
    if normalized == TOP10_PAGE_NAME:
        return "top10"
    if normalized == INSIGHTS_PAGE_NAME:
        return "insights"
    if normalized == DATA_DICTIONARY_PAGE_NAME:
        return "data_dictionary"
    if normalized in _SOURCE_PAGES_SET:
        return "source"
    return ROUTE_FAMILY_NAME


def _canonical_page_has_fixed_contract(page: str) -> bool:
    return _normalize_page_name(page) in KNOWN_CANONICAL_HEADER_COUNTS


def _nonempty(value: Any) -> bool:
    if value is None:
        return False
    if isinstance(value, str):
        return bool(value.strip())
    if isinstance(value, (list, tuple, set, dict)):
        return bool(value)
    return True


def _timeout_seconds(env_name: str, default: float) -> float:
    return max(0.1, _safe_float(os.getenv(env_name), default))


def _resolver_timeout(stage: str, *, page: str) -> float:
    page = _normalize_page_name(page)
    defaults = {
        "builder": 14.0 if page == TOP10_PAGE_NAME else 10.0,
        "runner": 12.0 if page == TOP10_PAGE_NAME else 16.0,
        "bridge": 10.0 if page in _SOURCE_PAGES_SET or page == DATA_DICTIONARY_PAGE_NAME else 14.0,
        "engine": 12.0 if page in DERIVED_PAGES else 16.0,
    }
    env_map = {
        "builder": "TFB_ADV_BUILDER_TIMEOUT_SEC",
        "runner": "TFB_ADV_RUNNER_TIMEOUT_SEC",
        "bridge": "TFB_ADV_BRIDGE_TIMEOUT_SEC",
        "engine": "TFB_ADV_ENGINE_TIMEOUT_SEC",
    }
    return _timeout_seconds(env_map[stage], defaults[stage])


def _is_public_path(path: str) -> bool:
    p = _s(path)
    if not p:
        return False
    if p in {"/v1/advanced", "/v1/advanced/health", "/v1/advanced/meta", "/v1/advanced/metrics"}:
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


def _extract_auth_token(*, token_query: Optional[str], x_app_token: Optional[str], authorization: Optional[str]) -> str:
    token = _s(x_app_token)
    authz = _s(authorization)
    if authz.lower().startswith("bearer "):
        token = authz.split(" ", 1)[1].strip()
    if token_query and not token:
        allow_query = False
        try:
            settings = get_settings_cached()
            allow_query = bool(getattr(settings, "allow_query_token", False))
        except Exception:
            allow_query = False
        if allow_query:
            token = _s(token_query)
    return token


def _is_open_mode_enabled() -> bool:
    try:
        if callable(is_open_mode):
            result = is_open_mode()
            if inspect.isawaitable(result):
                return False
            return bool(result)
    except Exception:
        pass
    for name in ("OPEN_MODE", "TFB_OPEN_MODE", "AUTH_DISABLED"):
        env_v = _s(os.getenv(name))
        if env_v:
            return _boolish(env_v, False)
    return False


def _auth_passed(*, request: Request, token_query: Optional[str], x_app_token: Optional[str], authorization: Optional[str]) -> bool:
    if _is_open_mode_enabled():
        return True
    try:
        path = str(getattr(getattr(request, "url", None), "path", "") or "")
    except Exception:
        path = ""
    if _is_public_path(path):
        return True
    if auth_ok is None:
        return True

    auth_token = _extract_auth_token(token_query=token_query, x_app_token=x_app_token, authorization=authorization)
    headers_dict = dict(request.headers)

    settings = None
    try:
        settings = get_settings_cached()
    except Exception:
        settings = None

    attempts = [
        {
            "token": auth_token or None,
            "authorization": authorization,
            "headers": headers_dict,
            "path": path,
            "request": request,
            "settings": settings,
        },
        {"token": auth_token or None, "authorization": authorization, "headers": headers_dict, "path": path, "request": request},
        {"token": auth_token or None, "authorization": authorization, "headers": headers_dict, "path": path},
        {"token": auth_token or None, "authorization": authorization, "headers": headers_dict},
        {"token": auth_token or None, "authorization": authorization},
        {"token": auth_token or None},
        {},
    ]
    for kwargs in attempts:
        try:
            return bool(auth_ok(**kwargs))
        except TypeError:
            continue
        except Exception:
            return False
    return False


def _require_auth_or_401(*, request: Request, token_query: Optional[str], x_app_token: Optional[str], authorization: Optional[str]) -> None:
    if not _auth_passed(request=request, token_query=token_query, x_app_token=x_app_token, authorization=authorization):
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid token")


async def _get_engine(request: Request) -> Optional[Any]:
    try:
        st = getattr(request.app, "state", None)
        if st and getattr(st, "engine", None):
            return st.engine
        if st and getattr(st, "data_engine", None):
            return st.data_engine
    except Exception:
        pass

    for modpath in ("core.data_engine_v2", "core.data_engine", "core.investment_advisor_engine"):
        try:
            mod = import_module(modpath)
        except Exception:
            continue
        for attr in ("get_engine", "get_data_engine", "engine", "data_engine", "ENGINE", "DATA_ENGINE"):
            candidate = getattr(mod, attr, None)
            if candidate is None:
                continue
            try:
                if callable(candidate):
                    eng = candidate()
                    if inspect.isawaitable(eng):
                        eng = await eng
                    if eng is not None:
                        return eng
                elif candidate is not None:
                    return candidate
            except Exception:
                continue
    return None


def _schema_fallback_for_page(page: str) -> Tuple[List[str], List[str]]:
    family = _page_family(page)
    if family == "top10":
        schema = _CANONICAL_TOP10_SCHEMA_FALLBACK
    elif family == "insights":
        schema = _CANONICAL_INSIGHTS_SCHEMA_FALLBACK
    elif family == "data_dictionary":
        schema = _CANONICAL_DATA_DICTIONARY_SCHEMA_FALLBACK
    else:
        schema = _CANONICAL_INSTRUMENT_SCHEMA_FALLBACK
    return [h for _, h in schema], [k for k, _ in schema]


def _append_missing_headers(headers: List[str], fields: Sequence[str]) -> List[str]:
    out = list(headers)
    existing = set(out)
    for field in fields:
        if field not in existing:
            out.append(field)
            existing.add(field)
    return out


def _append_missing_keys(keys: List[str], fields: Sequence[str]) -> List[str]:
    out = list(keys)
    existing = set(out)
    for field in fields:
        if field not in existing:
            out.append(field)
            existing.add(field)
    return out


def _extract_schema_headers_keys_from_spec(spec: Any) -> Tuple[List[str], List[str]]:
    if spec is None:
        return [], []
    if not isinstance(spec, Mapping):
        columns = getattr(spec, "columns", None) or []
        if columns:
            headers: List[str] = []
            keys: List[str] = []
            for idx, col in enumerate(columns):
                key = _s(getattr(col, "key", ""))
                header = _s(getattr(col, "header", ""))
                if not key and not header:
                    continue
                if not key and header:
                    key = f"column_{idx + 1}"
                if key and not header:
                    header = key.replace("_", " ").title()
                keys.append(key)
                headers.append(header)
            return headers, keys
        spec = _safe_dict(spec)

    direct_headers = spec.get("headers") or spec.get("display_headers") or []
    direct_keys = spec.get("keys") or spec.get("fields") or spec.get("columns") or []

    headers = [_s(x) for x in direct_headers] if isinstance(direct_headers, list) else []
    keys = [_s(x) for x in direct_keys] if isinstance(direct_keys, list) else []
    headers = [x for x in headers if x]
    keys = [x for x in keys if x]
    if headers and keys:
        return headers, keys

    columns = spec.get("columns") if isinstance(spec.get("columns"), list) else []
    if columns:
        headers_out: List[str] = []
        keys_out: List[str] = []
        for idx, col in enumerate(columns):
            if isinstance(col, Mapping):
                key = _s(col.get("key") or col.get("field") or col.get("name"))
                header = _s(col.get("header") or col.get("label") or col.get("title"))
            else:
                key = _s(getattr(col, "key", "") or getattr(col, "field", "") or getattr(col, "name", ""))
                header = _s(getattr(col, "header", "") or getattr(col, "label", "") or getattr(col, "title", ""))
            if not key and not header:
                continue
            if not key and header:
                key = f"column_{idx + 1}"
            if key and not header:
                header = key.replace("_", " ").title()
            keys_out.append(key)
            headers_out.append(header)
        if headers_out and keys_out:
            return headers_out, keys_out

    for nested_key in ("sheet", "spec", "schema", "contract", "definition"):
        nested = spec.get(nested_key)
        headers2, keys2 = _extract_schema_headers_keys_from_spec(nested)
        if headers2 and keys2:
            return headers2, keys2
    return headers, keys


def _import_module_safely(name: str) -> Optional[Any]:
    try:
        return import_module(name)
    except Exception:
        return None


def _load_schema_defaults(page: str) -> Tuple[List[str], List[str]]:
    page = _normalize_page_name(page)
    if page in _SCHEMA_CACHE:
        headers, keys = _SCHEMA_CACHE[page]
        headers = list(headers)
        keys = list(keys)
        if page == TOP10_PAGE_NAME:
            headers = _append_missing_headers(headers, [x.replace("_", " ").title() for x in TOP10_SPECIAL_FIELDS])
            keys = _append_missing_keys(keys, TOP10_SPECIAL_FIELDS)
        return headers, keys

    try:
        from core.sheets.schema_registry import get_sheet_spec  # type: ignore
        spec = get_sheet_spec(page)
        headers, keys = _extract_schema_headers_keys_from_spec(spec)
        if headers and keys:
            if page == TOP10_PAGE_NAME:
                headers = _append_missing_headers(headers, [x.replace("_", " ").title() for x in TOP10_SPECIAL_FIELDS])
                keys = _append_missing_keys(keys, TOP10_SPECIAL_FIELDS)
            _SCHEMA_CACHE[page] = (list(headers), list(keys))
            return headers, keys
    except Exception:
        pass

    for module_name in SCHEMA_MODULE_CANDIDATES:
        module = _import_module_safely(module_name)
        if module is None:
            continue
        for map_name in SCHEMA_MAP_CANDIDATES:
            spec_map = getattr(module, map_name, None)
            if not isinstance(spec_map, Mapping):
                continue
            try:
                hit = spec_map.get(page) or spec_map.get(page.lower()) or spec_map.get(page.upper())
            except Exception:
                hit = None
            headers, keys = _extract_schema_headers_keys_from_spec(hit)
            if headers and keys:
                if page == TOP10_PAGE_NAME:
                    headers = _append_missing_headers(headers, [x.replace("_", " ").title() for x in TOP10_SPECIAL_FIELDS])
                    keys = _append_missing_keys(keys, TOP10_SPECIAL_FIELDS)
                _SCHEMA_CACHE[page] = (list(headers), list(keys))
                return headers, keys
        for fn_name in SCHEMA_FUNCTION_CANDIDATES:
            fn = getattr(module, fn_name, None)
            if not callable(fn):
                continue
            for kwargs in ({"sheet": page}, {"page": page}, {"sheet_name": page}, {"name": page}, {}):
                try:
                    out = fn(**kwargs) if kwargs else fn(page)
                except TypeError:
                    continue
                except Exception:
                    out = None
                headers, keys = _extract_schema_headers_keys_from_spec(out)
                if headers and keys:
                    if page == TOP10_PAGE_NAME:
                        headers = _append_missing_headers(headers, [x.replace("_", " ").title() for x in TOP10_SPECIAL_FIELDS])
                        keys = _append_missing_keys(keys, TOP10_SPECIAL_FIELDS)
                    _SCHEMA_CACHE[page] = (list(headers), list(keys))
                    return headers, keys

    headers, keys = _schema_fallback_for_page(page)
    if page == TOP10_PAGE_NAME:
        headers = _append_missing_headers(headers, [x.replace("_", " ").title() for x in TOP10_SPECIAL_FIELDS])
        keys = _append_missing_keys(keys, TOP10_SPECIAL_FIELDS)
    _SCHEMA_CACHE[page] = (list(headers), list(keys))
    return headers, keys


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
    if is_dataclass(obj):
        try:
            return asdict(obj)
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
    return {}


def _extract_rows_from_payload(payload: Any) -> List[Dict[str, Any]]:
    if payload is None:
        return []
    if isinstance(payload, list):
        out: List[Dict[str, Any]] = []
        for item in payload:
            d = _model_to_dict(item)
            if d:
                out.append(d)
            elif isinstance(item, Mapping):
                out.append(dict(item))
        return out

    data = _safe_dict(payload)
    for key in ("row_objects", "records", "items", "data", "results", "quotes", "rows"):
        raw = data.get(key)
        if isinstance(raw, list):
            rows = _extract_rows_from_payload(raw)
            if rows:
                return rows

    matrix = data.get("rows_matrix")
    keys = data.get("keys") or []
    if isinstance(matrix, list) and isinstance(keys, list) and keys:
        rows: List[Dict[str, Any]] = []
        for row in matrix:
            if isinstance(row, list):
                rows.append({str(keys[i]): row[i] if i < len(row) else None for i in range(len(keys))})
        if rows:
            return rows
    return []


def _canonical_key_name(value: str) -> str:
    text = _s(value).strip().lower()
    if not text:
        return ""
    text = text.replace("-", "_").replace("/", "_").replace("&", "_")
    text = re.sub(r"\s+", "_", text)
    text = re.sub(r"_+", "_", text)
    return text.strip("_")


def _canonical_key_loose(value: str) -> str:
    return re.sub(r"[^a-z0-9]+", "", _s(value).lower())


def _row_value_for_aliases(row: Mapping[str, Any], aliases: Sequence[str]) -> Any:
    if not isinstance(row, Mapping):
        return None
    exact = {str(k): v for k, v in row.items()}
    lower = {str(k).lower(): v for k, v in row.items()}
    canon = {_canonical_key_name(str(k)): v for k, v in row.items()}
    loose = {_canonical_key_loose(str(k)): v for k, v in row.items()}

    for alias in aliases:
        a = _s(alias)
        if not a:
            continue
        if a in exact and exact[a] is not None:
            return exact[a]
        if a.lower() in lower and lower[a.lower()] is not None:
            return lower[a.lower()]
        ck = _canonical_key_name(a)
        if ck in canon and canon[ck] is not None:
            return canon[ck]
        lk = _canonical_key_loose(a)
        if lk in loose and loose[lk] is not None:
            return loose[lk]
    return None


def _extract_keys_from_payload(payload: Any, page: str, rows: List[Dict[str, Any]]) -> List[str]:
    page = _normalize_page_name(page)
    schema_headers, schema_keys = _load_schema_defaults(page)
    data = _safe_dict(payload)
    payload_keys = data.get("keys") or data.get("fields") or []
    if isinstance(payload_keys, list):
        payload_keys = [_s(x) for x in payload_keys if _s(x)]
    else:
        payload_keys = []

    if _canonical_page_has_fixed_contract(page):
        keys = list(schema_keys)
        if page == TOP10_PAGE_NAME:
            keys = _append_missing_keys(keys, TOP10_SPECIAL_FIELDS)
        return keys

    keys = list(payload_keys) if payload_keys else list(schema_keys)
    seen = set(keys)
    for row in rows:
        for key in row.keys():
            sk = _s(key)
            if sk and sk not in seen:
                seen.add(sk)
                keys.append(sk)
    return keys


def _extract_headers_from_payload(payload: Any, page: str, keys: List[str]) -> List[str]:
    page = _normalize_page_name(page)
    schema_headers, schema_keys = _load_schema_defaults(page)
    data = _safe_dict(payload)
    payload_headers = data.get("headers") or data.get("display_headers") or []
    if isinstance(payload_headers, list):
        payload_headers = [_s(x) for x in payload_headers if _s(x)]
    else:
        payload_headers = []

    if _canonical_page_has_fixed_contract(page):
        headers = list(schema_headers)
        if page == TOP10_PAGE_NAME:
            headers = _append_missing_headers(headers, [x.replace("_", " ").title() for x in TOP10_SPECIAL_FIELDS])
        return headers[: len(keys)]

    headers = list(payload_headers) if payload_headers else list(schema_headers)
    if len(headers) < len(keys):
        extra = [k.replace("_", " ").title() for k in keys[len(headers):]]
        headers.extend(extra)
    return headers[: len(keys)] if len(headers) >= len(keys) else headers


def _rows_to_matrix(rows: List[Dict[str, Any]], keys: List[str]) -> List[List[Any]]:
    return [[row.get(key) for key in keys] for row in rows]


def _normalize_row_to_keys(row: Mapping[str, Any], keys: List[str], headers: Optional[List[str]] = None) -> Dict[str, Any]:
    header_lookup = headers or []
    out: Dict[str, Any] = {}
    for idx, key in enumerate(keys):
        aliases = [key, key.replace("_", " "), key.replace("_", "-")]
        if idx < len(header_lookup):
            aliases.extend([header_lookup[idx], header_lookup[idx].replace(" ", "_"), header_lookup[idx].replace(" ", "-")])
        val = _row_value_for_aliases(row, aliases)
        out[key] = _json_safe(val)
    return out


def _make_schema_only_response(page: str, *, include_matrix: bool, request_id: str, meta: Dict[str, Any]) -> Dict[str, Any]:
    headers, keys = _load_schema_defaults(page)
    rows: List[Dict[str, Any]] = []
    matrix: Optional[List[List[Any]]] = [] if include_matrix else None
    return jsonable_encoder(
        {
            "status": "success",
            "page": page,
            "sheet": page,
            "sheet_name": page,
            "route_family": _page_family(page),
            "headers": headers,
            "display_headers": headers,
            "keys": keys,
            "rows": rows,
            "row_objects": rows,
            "records": rows,
            "results": rows,
            "data": rows,
            "items": rows,
            "quotes": rows,
            "rows_matrix": matrix,
            "version": INVESTMENT_ADVISOR_VERSION,
            "request_id": request_id,
            "meta": meta,
        }
    )


async def _call_candidate(fn: Any, *, body: Dict[str, Any], request: Request, page: str, limit: int, offset: int, schema_only: bool) -> Any:
    if not callable(fn):
        return None
    kwargs_variants = [
        {"request": request, "body": body, "page": page, "limit": limit, "offset": offset, "schema_only": schema_only},
        {"request": request, "payload": body, "page": page, "limit": limit, "offset": offset, "schema_only": schema_only},
        {"request": request, "page": page, "limit": limit, "offset": offset, "schema_only": schema_only, **body},
        {"page": page, "limit": limit, "offset": offset, "schema_only": schema_only, **body},
        {"request": request, "body": body},
        {"payload": body},
        body,
        {},
    ]
    for kwargs in kwargs_variants:
        try:
            result = fn(**kwargs)
        except TypeError:
            continue
        except Exception:
            continue
        if inspect.isawaitable(result):
            result = await result
        return result
    return None


async def _resolve_function(module_candidates: Sequence[str], function_candidates: Sequence[str]) -> Tuple[Optional[Any], str, str, List[str]]:
    search_path: List[str] = []
    for module_name in module_candidates:
        search_path.append(module_name)
        module = _import_module_safely(module_name)
        if module is None:
            continue
        for fn_name in function_candidates:
            fn = getattr(module, fn_name, None)
            if callable(fn):
                return fn, module_name, fn_name, search_path
    return None, "", "", search_path


def _engine_method_candidates_for_page(page: str) -> Tuple[str, ...]:
    family = _page_family(page)
    if family == "top10":
        return TOP10_ENGINE_METHOD_CANDIDATES
    return GENERIC_ENGINE_METHOD_CANDIDATES


def _runner_function_candidates_for_page(page: str) -> Tuple[str, ...]:
    family = _page_family(page)
    if family == "top10":
        return TOP10_RUNNER_FUNCTION_CANDIDATES
    return GENERIC_RUNNER_FUNCTION_CANDIDATES


async def _resolve_top10_builder(request: Request, engine: Optional[Any]) -> Tuple[Optional[Any], str, str, List[str]]:
    try:
        state = getattr(request.app, "state", None)
        if state is not None:
            for attr in ("top10_builder", "build_top10_rows", "select_top10", "investment_advisor_runner"):
                candidate = getattr(state, attr, None)
                if callable(candidate):
                    return candidate, "app.state", attr, ["app.state"]
    except Exception:
        pass
    if engine is not None:
        for attr in TOP10_FUNCTION_CANDIDATES:
            candidate = getattr(engine, attr, None)
            if callable(candidate):
                return candidate, type(engine).__name__, attr, [type(engine).__name__]
    return await _resolve_function(TOP10_MODULE_CANDIDATES, TOP10_FUNCTION_CANDIDATES)


async def _resolve_runner(request: Request, engine: Optional[Any], page: str) -> Tuple[Optional[Any], str, str, List[str]]:
    family = _page_family(page)

    if family != "top10" and engine is not None:
        for attr in _engine_method_candidates_for_page(page):
            candidate = getattr(engine, attr, None)
            if callable(candidate):
                return candidate, type(engine).__name__, attr, [type(engine).__name__]

    try:
        state = getattr(request.app, "state", None)
        if state is not None:
            attrs = ("investment_advisor_runner", "advisor_runner", "run_investment_advisor", "run_advisor")
            for attr in attrs:
                candidate = getattr(state, attr, None)
                if callable(candidate):
                    return candidate, "app.state", attr, ["app.state"]
    except Exception:
        pass

    if family == "top10" and engine is not None:
        for attr in _engine_method_candidates_for_page(page):
            candidate = getattr(engine, attr, None)
            if callable(candidate):
                return candidate, type(engine).__name__, attr, [type(engine).__name__]

    return await _resolve_function(RUNNER_MODULE_CANDIDATES, _runner_function_candidates_for_page(page))


async def _resolve_bridge_impl(request: Request) -> Tuple[Optional[Any], str, str]:
    try:
        state = getattr(request.app, "state", None)
        if state is not None:
            for attr in BRIDGE_FUNCTION_CANDIDATES:
                candidate = getattr(state, attr, None)
                if callable(candidate):
                    return candidate, "app.state", attr
    except Exception:
        pass
    fn, module_name, fn_name, _ = await _resolve_function(BRIDGE_MODULE_CANDIDATES, BRIDGE_FUNCTION_CANDIDATES)
    return fn, module_name, fn_name


def _make_meta(*, request_id: str, page: str, status_out: str, engine: Optional[Any], top10_source: str, top10_name: str, runner_source: str, runner_name: str, bridge_source: str, bridge_name: str, stages: Dict[str, int], warnings: List[str]) -> Dict[str, Any]:
    return jsonable_encoder(
        {
            "request_id": request_id,
            "timestamp_utc": _now_utc(),
            "version": INVESTMENT_ADVISOR_VERSION,
            "route_owner": ROUTE_OWNER_NAME,
            "route_family": ROUTE_FAMILY_NAME,
            "page_family": _page_family(page),
            "engine_present": bool(engine),
            "engine_type": type(engine).__name__ if engine else "none",
            "top10_builder_source": top10_source,
            "top10_builder_name": top10_name,
            "runner_source": runner_source,
            "runner_name": runner_name,
            "bridge_source": bridge_source,
            "bridge_name": bridge_name,
            "contract_header_count": KNOWN_CANONICAL_HEADER_COUNTS.get(page),
            "warnings": warnings,
            "status": status_out,
            "stage_durations_ms": stages,
        }
    )


def _advanced_get_body(*, page: Optional[str], sheet: Optional[str], sheet_name: Optional[str], name: Optional[str], tab: Optional[str], symbols: Optional[str], tickers: Optional[str], pages: Optional[str], sources: Optional[str], risk_level: Optional[str], risk_profile: Optional[str], confidence_level: Optional[str], confidence_bucket: Optional[str], investment_period_days: Optional[int], horizon_days: Optional[int], min_expected_roi: Optional[float], min_roi: Optional[float], min_confidence: Optional[float], top_n: Optional[int], limit: Optional[int], offset: Optional[int], include_matrix: Optional[bool], schema_only: Optional[bool]) -> Dict[str, Any]:
    target_page = _normalize_page_name(page or sheet or sheet_name or name or tab or TOP10_PAGE_NAME)
    direct_symbols = _normalize_symbol_list(symbols) or _normalize_symbol_list(tickers)
    selected_pages = _normalize_list(pages) or _normalize_list(sources)
    return {
        "page": target_page,
        "sheet": target_page,
        "sheet_name": target_page,
        "direct_symbols": direct_symbols,
        "symbols": direct_symbols,
        "tickers": direct_symbols,
        "pages_selected": selected_pages,
        "source_pages": _source_pages_only(selected_pages),
        "risk_level": _s(risk_level) or _s(risk_profile),
        "risk_profile": _s(risk_profile) or _s(risk_level),
        "confidence_level": _s(confidence_level),
        "confidence_bucket": _s(confidence_bucket),
        "investment_period_days": investment_period_days,
        "horizon_days": horizon_days,
        "min_expected_roi": min_expected_roi,
        "min_roi": min_roi if min_roi is not None else min_expected_roi,
        "min_confidence": min_confidence,
        "top_n": top_n if top_n is not None else limit,
        "limit": limit if limit is not None else top_n,
        "offset": offset,
        "include_matrix": include_matrix,
        "schema_only": schema_only,
    }


def _advanced_root_has_request_filters(*, page: Optional[str], sheet: Optional[str], sheet_name: Optional[str], name: Optional[str], tab: Optional[str], symbols: Optional[str], tickers: Optional[str], pages: Optional[str], sources: Optional[str], risk_level: Optional[str], risk_profile: Optional[str], confidence_level: Optional[str], confidence_bucket: Optional[str], investment_period_days: Optional[int], horizon_days: Optional[int], min_expected_roi: Optional[float], min_roi: Optional[float], min_confidence: Optional[float], top_n: Optional[int], limit: Optional[int], offset: Optional[int], mode: str, include_matrix: Optional[bool], schema_only: Optional[bool]) -> bool:
    values = [
        page, sheet, sheet_name, name, tab, symbols, tickers, pages, sources,
        risk_level, risk_profile, confidence_level, confidence_bucket,
        investment_period_days, horizon_days, min_expected_roi, min_roi,
        min_confidence, top_n, limit, offset, mode, include_matrix, schema_only,
    ]
    for value in values:
        if value is None:
            continue
        if isinstance(value, str) and not value.strip():
            continue
        return True
    return False


async def _advanced_root_summary(request: Request) -> Dict[str, Any]:
    engine = await _get_engine(request)
    top10_builder, top10_builder_source, top10_builder_name, top10_builder_search_path = await _resolve_top10_builder(request, engine)
    runner, runner_source, runner_name, runner_search_path = await _resolve_runner(request, engine, TOP10_PAGE_NAME)
    bridge_impl, bridge_source, bridge_name = await _resolve_bridge_impl(request)
    return jsonable_encoder(
        {
            "status": "success" if engine else "degraded",
            "service": "advanced_investment_advisor",
            "version": INVESTMENT_ADVISOR_VERSION,
            "route_owner": ROUTE_OWNER_NAME,
            "route_family": ROUTE_FAMILY_NAME,
            "root_path": "/v1/advanced",
            "engine_present": bool(engine),
            "engine_type": type(engine).__name__ if engine else "none",
            "top10_builder_available": bool(top10_builder),
            "top10_builder_source": top10_builder_source,
            "top10_builder_name": top10_builder_name,
            "top10_builder_search_path": top10_builder_search_path,
            "runner_available": bool(runner),
            "runner_source": runner_source,
            "runner_name": runner_name,
            "runner_search_path": runner_search_path,
            "bridge_available": bool(bridge_impl),
            "bridge_source": bridge_source,
            "bridge_name": bridge_name,
            "contract_header_counts": dict(KNOWN_CANONICAL_HEADER_COUNTS),
            "supported_aliases": [
                "/v1/advanced",
                "/v1/advanced/sheet-rows",
                "/v1/advanced/recommendations",
                "/v1/advanced/top10",
                "/v1/advanced/top10-investments",
                "/v1/advanced/investment-advisor",
                "/v1/advanced/advisor",
                "/v1/advanced/run",
                "/v1/advanced/health",
                "/v1/advanced/meta",
                "/v1/advanced/metrics",
            ],
            "timestamp_utc": _now_utc(),
        }
    )


async def _execute_advanced_request(*, request: Request, body: Dict[str, Any], mode: str, include_matrix: Optional[bool], limit: Optional[int], offset: Optional[int], schema_only: Optional[bool], x_request_id: Optional[str], token: Optional[str], x_app_token: Optional[str], authorization: Optional[str]) -> Dict[str, Any]:
    del token, x_app_token, authorization  # already checked before entering
    started = time.perf_counter()
    request_id = _request_id(request, x_request_id)
    target_page = _normalize_page_name(body.get("page") or body.get("sheet") or body.get("sheet_name") or TOP10_PAGE_NAME)
    body["page"] = target_page
    body["sheet"] = target_page
    body["sheet_name"] = target_page

    include_matrix_final = _boolish(include_matrix if include_matrix is not None else body.get("include_matrix"), False)
    schema_only_final = _boolish(schema_only if schema_only is not None else body.get("schema_only"), False)
    limit_final = _safe_int(limit if limit is not None else body.get("limit") or body.get("top_n"), 20)
    offset_final = _safe_int(offset if offset is not None else body.get("offset"), 0)
    body["limit"] = limit_final
    body["top_n"] = limit_final
    body["offset"] = offset_final
    body["mode"] = _s(mode) or _s(body.get("mode"))

    warnings: List[str] = []
    stages: Dict[str, int] = {}
    engine = await _get_engine(request)

    if schema_only_final:
        meta = _make_meta(
            request_id=request_id,
            page=target_page,
            status_out="success",
            engine=engine,
            top10_source="",
            top10_name="",
            runner_source="",
            runner_name="",
            bridge_source="",
            bridge_name="",
            stages={"total": int((time.perf_counter() - started) * 1000)},
            warnings=warnings,
        )
        return _make_schema_only_response(target_page, include_matrix=include_matrix_final, request_id=request_id, meta=meta)

    page_family = _page_family(target_page)
    payload: Any = None

    top10_source = ""
    top10_name = ""
    runner_source = ""
    runner_name = ""
    bridge_source = ""
    bridge_name = ""

    async def _run_with_timeout(label: str, coro: Any, timeout_sec: float) -> Any:
        try:
            return await asyncio.wait_for(coro, timeout=timeout_sec)
        except Exception as exc:
            warnings.append(f"{label}: {exc.__class__.__name__}")
            return None

    if page_family == "top10":
        stage_start = time.perf_counter()
        top10_builder, top10_source, top10_name, _ = await _resolve_top10_builder(request, engine)
        stages["resolve_builder"] = int((time.perf_counter() - stage_start) * 1000)
        if top10_builder is not None:
            stage_start = time.perf_counter()
            payload = await _run_with_timeout(
                "builder",
                _call_candidate(top10_builder, body=body, request=request, page=target_page, limit=limit_final, offset=offset_final, schema_only=schema_only_final),
                _resolver_timeout("builder", page=target_page),
            )
            stages["builder"] = int((time.perf_counter() - stage_start) * 1000)

    stage_start = time.perf_counter()
    runner, runner_source, runner_name, _ = await _resolve_runner(request, engine, target_page)
    stages["resolve_runner"] = int((time.perf_counter() - stage_start) * 1000)

    stage_start = time.perf_counter()
    bridge_impl, bridge_source, bridge_name = await _resolve_bridge_impl(request)
    stages["resolve_bridge"] = int((time.perf_counter() - stage_start) * 1000)

    if payload is None and runner is not None:
        stage_start = time.perf_counter()
        payload = await _run_with_timeout(
            "runner",
            _call_candidate(runner, body=body, request=request, page=target_page, limit=limit_final, offset=offset_final, schema_only=schema_only_final),
            _resolver_timeout("runner", page=target_page),
        )
        stages["runner"] = int((time.perf_counter() - stage_start) * 1000)

    if payload is None and bridge_impl is not None:
        stage_start = time.perf_counter()
        payload = await _run_with_timeout(
            "bridge",
            _call_candidate(bridge_impl, body=body, request=request, page=target_page, limit=limit_final, offset=offset_final, schema_only=schema_only_final),
            _resolver_timeout("bridge", page=target_page),
        )
        stages["bridge"] = int((time.perf_counter() - stage_start) * 1000)

    if payload is None and engine is not None:
        stage_start = time.perf_counter()
        for method_name in _engine_method_candidates_for_page(target_page):
            method = getattr(engine, method_name, None)
            if not callable(method):
                continue
            payload = await _run_with_timeout(
                f"engine.{method_name}",
                _call_candidate(method, body=body, request=request, page=target_page, limit=limit_final, offset=offset_final, schema_only=schema_only_final),
                _resolver_timeout("engine", page=target_page),
            )
            if payload is not None:
                runner_source = runner_source or type(engine).__name__
                runner_name = runner_name or method_name
                break
        stages["engine"] = int((time.perf_counter() - stage_start) * 1000)

    raw_rows = _extract_rows_from_payload(payload)
    keys = _extract_keys_from_payload(payload, target_page, raw_rows)
    headers = _extract_headers_from_payload(payload, target_page, keys)

    norm_rows: List[Dict[str, Any]] = []
    for row in raw_rows:
        norm_rows.append(_normalize_row_to_keys(row, keys, headers))
    if norm_rows:
        norm_rows = _slice_rows(norm_rows, offset=offset_final, limit=limit_final)

    if page_family == "top10":
        for idx, row in enumerate(norm_rows, start=1 + offset_final):
            row.setdefault("top10_rank", idx)
            row.setdefault("selection_reason", row.get("recommendation") or "top10_selection")
            row.setdefault(
                "criteria_snapshot",
                _json_compact(
                    {
                        "risk_level": body.get("risk_level") or body.get("risk_profile"),
                        "confidence_level": body.get("confidence_level") or body.get("confidence_bucket"),
                        "top_n": body.get("top_n") or body.get("limit"),
                        "source_pages": body.get("source_pages") or body.get("pages_selected"),
                    }
                ),
            )
        keys = _append_missing_keys(keys, TOP10_SPECIAL_FIELDS)
        headers = _append_missing_headers(headers, [x.replace("_", " ").title() for x in TOP10_SPECIAL_FIELDS])
        norm_rows = [_normalize_row_to_keys(row, keys, headers) for row in norm_rows]

    status_out = "success" if payload is not None or norm_rows or schema_only_final else "degraded"
    if not raw_rows and payload is None:
        warnings.append("No builder/runner/bridge/engine payload resolved; returned canonical empty contract.")

    meta = _make_meta(
        request_id=request_id,
        page=target_page,
        status_out=status_out,
        engine=engine,
        top10_source=top10_source,
        top10_name=top10_name,
        runner_source=runner_source,
        runner_name=runner_name,
        bridge_source=bridge_source,
        bridge_name=bridge_name,
        stages={**stages, "total": int((time.perf_counter() - started) * 1000)},
        warnings=warnings,
    )

    rows_matrix = _rows_to_matrix(norm_rows, keys) if include_matrix_final and keys else None
    response = {
        "status": status_out,
        "page": target_page,
        "sheet": target_page,
        "sheet_name": target_page,
        "route_family": page_family,
        "headers": headers,
        "display_headers": headers,
        "keys": keys,
        "rows": norm_rows,
        "row_objects": norm_rows,
        "records": norm_rows,
        "results": norm_rows,
        "data": norm_rows,
        "items": norm_rows,
        "quotes": norm_rows,
        "rows_matrix": rows_matrix,
        "version": INVESTMENT_ADVISOR_VERSION,
        "request_id": request_id,
        "meta": meta,
    }
    return jsonable_encoder(response)


@router.get("")
async def advanced_root_get(
    request: Request,
    response: Response,
    page: Optional[str] = Query(default=None),
    sheet: Optional[str] = Query(default=None),
    sheet_name: Optional[str] = Query(default=None),
    name: Optional[str] = Query(default=None),
    tab: Optional[str] = Query(default=None),
    symbols: Optional[str] = Query(default=None),
    tickers: Optional[str] = Query(default=None),
    pages: Optional[str] = Query(default=None),
    sources: Optional[str] = Query(default=None),
    risk_level: Optional[str] = Query(default=None),
    risk_profile: Optional[str] = Query(default=None),
    confidence_level: Optional[str] = Query(default=None),
    confidence_bucket: Optional[str] = Query(default=None),
    investment_period_days: Optional[int] = Query(default=None, ge=1, le=3650),
    horizon_days: Optional[int] = Query(default=None, ge=1, le=3650),
    min_expected_roi: Optional[float] = Query(default=None),
    min_roi: Optional[float] = Query(default=None),
    min_confidence: Optional[float] = Query(default=None),
    top_n: Optional[int] = Query(default=None, ge=1, le=200),
    limit: Optional[int] = Query(default=None, ge=1, le=200),
    offset: Optional[int] = Query(default=None, ge=0, le=50000),
    mode: str = Query(default=""),
    include_matrix: Optional[bool] = Query(default=None),
    schema_only: Optional[bool] = Query(default=None),
    token: Optional[str] = Query(default=None),
    x_app_token: Optional[str] = Header(default=None, alias="X-APP-TOKEN"),
    authorization: Optional[str] = Header(default=None, alias="Authorization"),
    x_request_id: Optional[str] = Header(default=None, alias="X-Request-ID"),
) -> Dict[str, Any]:
    _require_auth_or_401(request=request, token_query=token, x_app_token=x_app_token, authorization=authorization)

    has_filters = _advanced_root_has_request_filters(
        page=page,
        sheet=sheet,
        sheet_name=sheet_name,
        name=name,
        tab=tab,
        symbols=symbols,
        tickers=tickers,
        pages=pages,
        sources=sources,
        risk_level=risk_level,
        risk_profile=risk_profile,
        confidence_level=confidence_level,
        confidence_bucket=confidence_bucket,
        investment_period_days=investment_period_days,
        horizon_days=horizon_days,
        min_expected_roi=min_expected_roi,
        min_roi=min_roi,
        min_confidence=min_confidence,
        top_n=top_n,
        limit=limit,
        offset=offset,
        mode=mode,
        include_matrix=include_matrix,
        schema_only=schema_only,
    )
    if not has_filters:
        payload = await _advanced_root_summary(request)
        response.headers["X-Request-ID"] = _request_id(request, x_request_id)
        return payload

    body = _advanced_get_body(
        page=page,
        sheet=sheet,
        sheet_name=sheet_name,
        name=name,
        tab=tab,
        symbols=symbols,
        tickers=tickers,
        pages=pages,
        sources=sources,
        risk_level=risk_level,
        risk_profile=risk_profile,
        confidence_level=confidence_level,
        confidence_bucket=confidence_bucket,
        investment_period_days=investment_period_days,
        horizon_days=horizon_days,
        min_expected_roi=min_expected_roi,
        min_roi=min_roi,
        min_confidence=min_confidence,
        top_n=top_n,
        limit=limit,
        offset=offset,
        include_matrix=include_matrix,
        schema_only=schema_only,
    )
    payload = await _execute_advanced_request(
        request=request,
        body=body,
        mode=mode,
        include_matrix=include_matrix,
        limit=limit if limit is not None else top_n,
        offset=offset,
        schema_only=schema_only,
        x_request_id=x_request_id,
        token=token,
        x_app_token=x_app_token,
        authorization=authorization,
    )
    response.headers["X-Request-ID"] = payload.get("request_id") or _request_id(request, x_request_id)
    return payload


@router.post("")
async def advanced_root_post(
    request: Request,
    response: Response,
    body: Dict[str, Any] = Body(default_factory=dict),
    mode: str = Query(default="", description="Optional mode hint"),
    include_matrix: Optional[bool] = Query(default=None, description="Return rows_matrix"),
    limit: Optional[int] = Query(default=None, ge=1, le=200, description="How many items to return"),
    offset: Optional[int] = Query(default=None, ge=0, le=50000, description="How many items to skip"),
    schema_only: Optional[bool] = Query(default=None, description="Return schema only"),
    token: Optional[str] = Query(default=None, description="Auth token (query only if allowed)"),
    x_app_token: Optional[str] = Header(default=None, alias="X-APP-TOKEN"),
    authorization: Optional[str] = Header(default=None, alias="Authorization"),
    x_request_id: Optional[str] = Header(default=None, alias="X-Request-ID"),
) -> Dict[str, Any]:
    _require_auth_or_401(request=request, token_query=token, x_app_token=x_app_token, authorization=authorization)
    payload = await _execute_advanced_request(
        request=request,
        body=dict(body or {}),
        mode=mode,
        include_matrix=include_matrix,
        limit=limit,
        offset=offset,
        schema_only=schema_only,
        x_request_id=x_request_id,
        token=token,
        x_app_token=x_app_token,
        authorization=authorization,
    )
    response.headers["X-Request-ID"] = payload.get("request_id") or _request_id(request, x_request_id)
    return payload


@router.get("/health")
async def advanced_health(request: Request) -> Dict[str, Any]:
    engine = await _get_engine(request)
    top10_builder, top10_builder_source, top10_builder_name, top10_builder_search_path = await _resolve_top10_builder(request, engine)
    runner, runner_source, runner_name, runner_search_path = await _resolve_runner(request, engine, TOP10_PAGE_NAME)
    bridge_impl, bridge_source, bridge_name = await _resolve_bridge_impl(request)
    return jsonable_encoder(
        {
            "status": "ok" if engine else "degraded",
            "service": "advanced_investment_advisor",
            "version": INVESTMENT_ADVISOR_VERSION,
            "route_owner": ROUTE_OWNER_NAME,
            "route_family": ROUTE_FAMILY_NAME,
            "engine_available": bool(engine),
            "engine_type": type(engine).__name__ if engine else "none",
            "top10_builder_available": bool(top10_builder),
            "top10_builder_source": top10_builder_source,
            "top10_builder_name": top10_builder_name,
            "top10_builder_search_path": top10_builder_search_path,
            "runner_available": bool(runner),
            "runner_source": runner_source,
            "runner_name": runner_name,
            "runner_search_path": runner_search_path,
            "bridge_available": bool(bridge_impl),
            "bridge_source": bridge_source,
            "bridge_name": bridge_name,
            "contract_header_counts": dict(KNOWN_CANONICAL_HEADER_COUNTS),
            "timestamp_utc": _now_utc(),
        }
    )


@router.get("/meta")
async def advanced_meta(request: Request) -> Dict[str, Any]:
    engine = await _get_engine(request)
    top10_builder, top10_builder_source, top10_builder_name, top10_builder_search_path = await _resolve_top10_builder(request, engine)
    runner, runner_source, runner_name, runner_search_path = await _resolve_runner(request, engine, TOP10_PAGE_NAME)
    bridge_impl, bridge_source, bridge_name = await _resolve_bridge_impl(request)
    return jsonable_encoder(
        {
            "status": "success",
            "version": INVESTMENT_ADVISOR_VERSION,
            "route_owner": ROUTE_OWNER_NAME,
            "route_family": ROUTE_FAMILY_NAME,
            "engine_present": bool(engine),
            "engine_type": type(engine).__name__ if engine else "none",
            "top10_builder_available": bool(top10_builder),
            "top10_builder_source": top10_builder_source,
            "top10_builder_name": top10_builder_name,
            "top10_builder_search_path": top10_builder_search_path,
            "runner_available": bool(runner),
            "runner_source": runner_source,
            "runner_name": runner_name,
            "runner_search_path": runner_search_path,
            "bridge_available": bool(bridge_impl),
            "bridge_source": bridge_source,
            "bridge_name": bridge_name,
            "contract_header_counts": dict(KNOWN_CANONICAL_HEADER_COUNTS),
            "timestamp_utc": _now_utc(),
        }
    )


@router.get("/metrics")
async def advanced_metrics() -> Response:
    if not PROMETHEUS_AVAILABLE or generate_latest is None:
        return Response(content="Metrics not available", media_type="text/plain", status_code=503)
    return Response(content=generate_latest(), media_type=CONTENT_TYPE_LATEST)


@router.post("/top10-investments")
@router.post("/top10")
@router.post("/investment-advisor")
@router.post("/advisor")
@router.post("/run")
@router.post("/sheet-rows")
async def advanced_request_post(
    request: Request,
    response: Response,
    body: Dict[str, Any] = Body(default_factory=dict),
    mode: str = Query(default="", description="Optional mode hint"),
    include_matrix: Optional[bool] = Query(default=None, description="Return rows_matrix"),
    limit: Optional[int] = Query(default=None, ge=1, le=200, description="How many items to return"),
    offset: Optional[int] = Query(default=None, ge=0, le=50000, description="How many items to skip"),
    schema_only: Optional[bool] = Query(default=None, description="Return schema only"),
    token: Optional[str] = Query(default=None, description="Auth token (query only if allowed)"),
    x_app_token: Optional[str] = Header(default=None, alias="X-APP-TOKEN"),
    authorization: Optional[str] = Header(default=None, alias="Authorization"),
    x_request_id: Optional[str] = Header(default=None, alias="X-Request-ID"),
) -> Dict[str, Any]:
    _require_auth_or_401(request=request, token_query=token, x_app_token=x_app_token, authorization=authorization)
    payload = await _execute_advanced_request(
        request=request,
        body=dict(body or {}),
        mode=mode,
        include_matrix=include_matrix,
        limit=limit,
        offset=offset,
        schema_only=schema_only,
        x_request_id=x_request_id,
        token=token,
        x_app_token=x_app_token,
        authorization=authorization,
    )
    response.headers["X-Request-ID"] = payload.get("request_id") or _request_id(request, x_request_id)
    return payload


@router.get("/recommendations")
@router.get("/sheet-rows")
async def advanced_request_get(
    request: Request,
    response: Response,
    page: Optional[str] = Query(default=None),
    sheet: Optional[str] = Query(default=None),
    sheet_name: Optional[str] = Query(default=None),
    name: Optional[str] = Query(default=None),
    tab: Optional[str] = Query(default=None),
    symbols: Optional[str] = Query(default=None, description="Comma/space separated symbols"),
    tickers: Optional[str] = Query(default=None, description="Comma/space separated tickers"),
    pages: Optional[str] = Query(default=None, description="Comma separated source pages"),
    sources: Optional[str] = Query(default=None, description="Comma separated source pages"),
    risk_level: Optional[str] = Query(default=None),
    risk_profile: Optional[str] = Query(default=None),
    confidence_level: Optional[str] = Query(default=None),
    confidence_bucket: Optional[str] = Query(default=None),
    investment_period_days: Optional[int] = Query(default=None, ge=1, le=3650),
    horizon_days: Optional[int] = Query(default=None, ge=1, le=3650),
    min_expected_roi: Optional[float] = Query(default=None),
    min_roi: Optional[float] = Query(default=None),
    min_confidence: Optional[float] = Query(default=None),
    top_n: Optional[int] = Query(default=None, ge=1, le=200),
    limit: Optional[int] = Query(default=None, ge=1, le=200),
    offset: Optional[int] = Query(default=None, ge=0, le=50000),
    mode: str = Query(default="", description="Optional mode hint"),
    include_matrix: Optional[bool] = Query(default=None),
    schema_only: Optional[bool] = Query(default=None),
    token: Optional[str] = Query(default=None, description="Auth token (query only if allowed)"),
    x_app_token: Optional[str] = Header(default=None, alias="X-APP-TOKEN"),
    authorization: Optional[str] = Header(default=None, alias="Authorization"),
    x_request_id: Optional[str] = Header(default=None, alias="X-Request-ID"),
) -> Dict[str, Any]:
    _require_auth_or_401(request=request, token_query=token, x_app_token=x_app_token, authorization=authorization)
    body = _advanced_get_body(
        page=page,
        sheet=sheet,
        sheet_name=sheet_name,
        name=name,
        tab=tab,
        symbols=symbols,
        tickers=tickers,
        pages=pages,
        sources=sources,
        risk_level=risk_level,
        risk_profile=risk_profile,
        confidence_level=confidence_level,
        confidence_bucket=confidence_bucket,
        investment_period_days=investment_period_days,
        horizon_days=horizon_days,
        min_expected_roi=min_expected_roi,
        min_roi=min_roi,
        min_confidence=min_confidence,
        top_n=top_n,
        limit=limit,
        offset=offset,
        include_matrix=include_matrix,
        schema_only=schema_only,
    )
    payload = await _execute_advanced_request(
        request=request,
        body=body,
        mode=mode,
        include_matrix=include_matrix,
        limit=limit if limit is not None else top_n,
        offset=offset,
        schema_only=schema_only,
        x_request_id=x_request_id,
        token=token,
        x_app_token=x_app_token,
        authorization=authorization,
    )
    response.headers["X-Request-ID"] = payload.get("request_id") or _request_id(request, x_request_id)
    return payload


__all__ = ["router", "INVESTMENT_ADVISOR_VERSION"]
