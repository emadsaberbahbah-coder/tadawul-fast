#!/usr/bin/env python3
"""
routes/advisor.py
================================================================================
ADVISOR ROUTER — v6.6.0
================================================================================
SPECIAL-PAGE PROXY-FIRST • ROOT/ANALYSIS ALIGNED • SHORT-ADVISOR HARDENED •
JSON-SAFE • GET+POST SAFE • FAIL-SOFT • CONTRACT-PROJECTED • ENGINE-TOLERANT

What this revision improves
--------------------------
- FIX: special pages now prefer the canonical sheet-rows owners first:
      analysis wrapper -> advanced root owner -> engine fallback.
- FIX: advisor short family no longer depends on buggy downstream derived-page
      runners before trying the already-working root/analysis routes.
- FIX: Insights_Analysis, Top_10_Investments, and Data_Dictionary are handled
      with bridge-first logic so advisor routes align with proven live paths.
- FIX: tabular payloads are projected to canonical headers when available.
- FIX: responses remain fail-soft and JSON-safe, exposing resolver metadata.
"""

from __future__ import annotations

import asyncio
import inspect
import logging
import math
import os
import time
import uuid
from dataclasses import asdict, is_dataclass
from datetime import date, datetime, time as dt_time, timezone
from decimal import Decimal
from enum import Enum
from importlib import import_module
from typing import Any, Callable, Dict, Iterable, List, Mapping, Optional, Sequence, Tuple

from fastapi import Body, Header, HTTPException, Query, Request, Response
from fastapi.encoders import jsonable_encoder
from fastapi.routing import APIRouter

logger = logging.getLogger("routes.advisor")
logger.addHandler(logging.NullHandler())

ADVISOR_VERSION = "6.6.0"
router = APIRouter(prefix="/v1/advisor", tags=["advisor"])

DEFAULT_ADVISOR_PAGE = "Top_10_Investments"

BASE_PAGES = {
    "Market_Leaders",
    "Global_Markets",
    "Commodities_FX",
    "Mutual_Funds",
    "My_Portfolio",
}
DERIVED_PAGES = {"Top_10_Investments", "Insights_Analysis", "Data_Dictionary"}
KNOWN_CANONICAL_HEADER_COUNTS: Dict[str, int] = {
    "Market_Leaders": 85,
    "Global_Markets": 85,
    "Commodities_FX": 85,
    "Mutual_Funds": 85,
    "My_Portfolio": 85,
    "Insights_Analysis": 7,
    "Top_10_Investments": 88,
    "Data_Dictionary": 9,
}
TOP10_SPECIAL_FIELDS: Tuple[str, ...] = ("top10_rank", "selection_reason", "criteria_snapshot")
TOP10_BUSINESS_SIGNAL_FIELDS: Tuple[str, ...] = (
    "symbol", "name", "asset_class", "exchange", "currency", "country", "sector", "industry",
    "current_price", "price_change", "percent_change", "risk_score", "valuation_score",
    "overall_score", "opportunity_score", "risk_bucket", "confidence_bucket", "recommendation",
)

PROMETHEUS_AVAILABLE = False
try:
    from prometheus_client import CONTENT_TYPE_LATEST, generate_latest  # type: ignore
    PROMETHEUS_AVAILABLE = True
except Exception:
    CONTENT_TYPE_LATEST = "text/plain"
    generate_latest = None  # type: ignore

try:
    from core.config import auth_ok, get_settings_cached, is_open_mode  # type: ignore
except Exception:
    auth_ok = None  # type: ignore
    is_open_mode = None  # type: ignore
    def get_settings_cached(*args: Any, **kwargs: Any) -> Any:  # type: ignore
        return None

_SCHEMA_HEADERS_CACHE: Dict[str, List[str]] = {}
AUTH_ENV_TOKEN_NAMES: Tuple[str, ...] = (
    "TFB_TOKEN", "APP_TOKEN", "BACKEND_TOKEN", "BACKUP_APP_TOKEN",
    "X_APP_TOKEN", "AUTH_TOKEN", "TOKEN", "TFB_APP_TOKEN",
)


def _strip(v: Any) -> str:
    if v is None:
        return ""
    try:
        s = str(v).strip()
    except Exception:
        return ""
    return "" if s.lower() in {"none", "null", "nil", "undefined"} else s


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
        if math.isnan(out) or math.isinf(out):
            return default
        return out
    except Exception:
        return default


def _boolish(v: Any, default: bool = False) -> bool:
    if isinstance(v, bool):
        return v
    if v is None:
        return default
    s = _strip(v).lower()
    if s in {"1", "true", "yes", "y", "on"}:
        return True
    if s in {"0", "false", "no", "n", "off"}:
        return False
    return default


def _split_csv(text: str) -> List[str]:
    raw = (text or "").replace(";", ",").replace("\n", ",").replace("\t", ",")
    out: List[str] = []
    seen = set()
    for part in raw.split(","):
        s = _strip(part)
        if s and s not in seen:
            seen.add(s)
            out.append(s)
    return out


def _get_list(data: Mapping[str, Any], *keys: str) -> List[str]:
    for key in keys:
        value = data.get(key)
        if isinstance(value, list):
            out: List[str] = []
            seen = set()
            for item in value:
                s = _strip(item)
                if s and s not in seen:
                    seen.add(s)
                    out.append(s)
            if out:
                return out
        if isinstance(value, str) and value.strip():
            out = _split_csv(value)
            if out:
                return out
    return []


def _now_utc() -> str:
    return datetime.now(timezone.utc).isoformat()


def _request_id(request: Request, x_request_id: Optional[str]) -> str:
    rid = _strip(x_request_id)
    if rid:
        return rid
    try:
        state_rid = _strip(getattr(request.state, "request_id", ""))
        if state_rid:
            return state_rid
    except Exception:
        pass
    return uuid.uuid4().hex[:12]


def _safe_engine_type(engine: Any) -> str:
    if engine is None:
        return "none"
    try:
        return f"{engine.__class__.__module__}.{engine.__class__.__name__}"
    except Exception:
        return type(engine).__name__


def _timeout_seconds(env_name: str, default: float) -> float:
    return max(0.1, _safe_float(os.getenv(env_name), default))


def _resolver_timeout(stage: str, *, page: str) -> float:
    page = _strip(page)
    defaults = {
        "bridge": 12.0 if page in DERIVED_PAGES else 18.0,
        "engine": 18.0 if page in DERIVED_PAGES else 25.0,
        "runner": 12.0 if page in DERIVED_PAGES else 22.0,
        "top10": 14.0,
    }
    env_map = {
        "bridge": "TFB_ADVISOR_BRIDGE_TIMEOUT_SEC",
        "engine": "TFB_ADVISOR_ENGINE_TIMEOUT_SEC",
        "runner": "TFB_ADVISOR_RUNNER_TIMEOUT_SEC",
        "top10": "TFB_TOP10_BUILDER_TIMEOUT_SEC",
    }
    return _timeout_seconds(env_map[stage], defaults[stage])


def _row_count(value: Any) -> int:
    if value is None:
        return 0
    if isinstance(value, (list, tuple, set)):
        return len(value)
    if isinstance(value, Mapping):
        return len(value)
    return 1


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
                return _strip(obj)
        if isinstance(obj, (datetime, date, dt_time)):
            try:
                return obj.isoformat()
            except Exception:
                return _strip(obj)
        if isinstance(obj, Enum):
            return _clean(obj.value)
        if is_dataclass(obj):
            try:
                return _clean(asdict(obj))
            except Exception:
                return _strip(obj)
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
            return _strip(obj)
    return _clean(value)


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
        env_v = _strip(os.getenv(name))
        if env_v:
            return _boolish(env_v, False)
    return False


def _auth_ok_via_hook(token_query: Optional[str], x_app_token: Optional[str], authorization: Optional[str]) -> bool:
    if auth_ok is None:
        return False
    token = _strip(token_query)
    x_token = _strip(x_app_token)
    authz = _strip(authorization)
    candidate_kwargs: Tuple[Dict[str, Any], ...] = (
        {"token": token or None, "x_app_token": x_token or None, "authorization": authz or None},
        {"token_query": token or None, "x_app_token": x_token or None, "authorization": authz or None},
        {"token": token or None, "authorization": authz or None},
        {"authorization": authz or None},
        {"x_app_token": x_token or None},
        {"token": token or None},
        {},
    )
    for kwargs in candidate_kwargs:
        try:
            result = auth_ok(**kwargs)  # type: ignore[misc]
            if inspect.isawaitable(result):
                return False
            return bool(result)
        except TypeError:
            continue
        except Exception:
            continue
    return False


def _candidate_tokens_from_env() -> List[str]:
    out: List[str] = []
    seen = set()
    for name in AUTH_ENV_TOKEN_NAMES:
        token = _strip(os.getenv(name))
        if token and token not in seen:
            seen.add(token)
            out.append(token)
    return out


def _auth_ok_via_env_match(token_query: Optional[str], x_app_token: Optional[str], authorization: Optional[str]) -> bool:
    env_tokens = _candidate_tokens_from_env()
    if not env_tokens:
        return False
    presented: List[str] = []
    for value in (token_query, x_app_token):
        token = _strip(value)
        if token:
            presented.append(token)
    authz = _strip(authorization)
    if authz:
        lowered = authz.lower()
        if lowered.startswith("bearer "):
            bearer = _strip(authz[7:])
            if bearer:
                presented.append(bearer)
        else:
            presented.append(authz)
    return any(token in env_tokens for token in presented)


def _require_auth_or_401(*, token_query: Optional[str], x_app_token: Optional[str], authorization: Optional[str]) -> None:
    if _is_open_mode_enabled():
        return
    if _auth_ok_via_hook(token_query=token_query, x_app_token=x_app_token, authorization=authorization):
        return
    if _auth_ok_via_env_match(token_query=token_query, x_app_token=x_app_token, authorization=authorization):
        return
    raise HTTPException(status_code=401, detail="Unauthorized")


def _import_module_safely(name: str) -> Optional[Any]:
    try:
        return import_module(name)
    except Exception:
        return None


def _get_engine_from_state_obj(state_obj: Any) -> Any:
    if state_obj is None:
        return None
    for attr in ("engine", "data_engine", "engine_v2", "data_engine_v2", "advisor_engine", "investment_advisor_engine"):
        try:
            obj = getattr(state_obj, attr, None)
            if obj is not None:
                return obj
        except Exception:
            continue
    return None


async def _get_engine(request: Request) -> Any:
    try:
        engine = _get_engine_from_state_obj(request.app.state)
        if engine is not None:
            return engine
    except Exception:
        pass
    for module_name in ("core.data_engine_v2", "core.data_engine", "core.investment_advisor_engine"):
        module = _import_module_safely(module_name)
        if module is None:
            continue
        for attr in ("engine", "data_engine", "ENGINE", "DATA_ENGINE", "advisor_engine", "investment_advisor_engine", "get_engine", "get_data_engine", "build_engine", "create_engine"):
            candidate = getattr(module, attr, None)
            if candidate is None:
                continue
            try:
                if callable(candidate):
                    out = candidate()
                    if inspect.isawaitable(out):
                        out = await out
                    if out is not None:
                        return out
                elif candidate is not None:
                    return candidate
            except Exception:
                continue
    return None


def _canonicalize_page_name(value: Any) -> str:
    raw = _strip(value)
    if not raw:
        return ""
    try:
        page_catalog = import_module("core.sheets.page_catalog")
    except Exception:
        page_catalog = None
    if page_catalog is not None:
        for fn_name in ("canonicalize_page_name", "canonical_page_name", "normalize_page_name", "resolve_page_name", "resolve_canonical_page_name"):
            fn = getattr(page_catalog, fn_name, None)
            if callable(fn):
                try:
                    out = fn(raw)
                    out_s = _strip(out)
                    if out_s:
                        return out_s
                except Exception:
                    continue
        for attr_name in ("PAGE_ALIASES", "ALIASES", "PAGE_ALIAS_MAP"):
            aliases = getattr(page_catalog, attr_name, None)
            if isinstance(aliases, Mapping):
                try:
                    hit = aliases.get(raw) or aliases.get(raw.lower()) or aliases.get(raw.upper())
                    hit_s = _strip(hit)
                    if hit_s:
                        return hit_s
                except Exception:
                    pass
        canonical_pages = getattr(page_catalog, "CANONICAL_PAGES", None)
        if isinstance(canonical_pages, (list, tuple, set)):
            lower_map = {_strip(x).lower(): _strip(x) for x in canonical_pages if _strip(x)}
            if raw.lower() in lower_map:
                return lower_map[raw.lower()]
    fallback_map = {
        "market leaders": "Market_Leaders", "market_leaders": "Market_Leaders",
        "global markets": "Global_Markets", "global_markets": "Global_Markets",
        "commodities fx": "Commodities_FX", "commodities_fx": "Commodities_FX", "commodities-fx": "Commodities_FX",
        "mutual funds": "Mutual_Funds", "mutual_funds": "Mutual_Funds",
        "my portfolio": "My_Portfolio", "my_portfolio": "My_Portfolio",
        "insights analysis": "Insights_Analysis", "insights_analysis": "Insights_Analysis",
        "top 10 investments": "Top_10_Investments", "top_10_investments": "Top_10_Investments", "top10_investments": "Top_10_Investments",
        "data dictionary": "Data_Dictionary", "data_dictionary": "Data_Dictionary",
    }
    return fallback_map.get(raw.lower(), raw)


def _extract_page(data: Mapping[str, Any], default_page: str = DEFAULT_ADVISOR_PAGE) -> str:
    raw = data.get("page") or data.get("sheet") or data.get("sheet_name") or data.get("name") or data.get("tab") or default_page
    page = _canonicalize_page_name(raw)
    return page or default_page


def _normalize_mode(value: Any) -> str:
    raw = _strip(value).lower()
    if raw in {"", "live", "default"}:
        return "live"
    if raw in {"live_quotes", "quotes", "quote", "market"}:
        return "live_quotes"
    if raw in {"snapshot", "cached", "cache"}:
        return "snapshot"
    return _strip(value) or "live"


def _default_payload(data: Mapping[str, Any], default_page: str = DEFAULT_ADVISOR_PAGE) -> Dict[str, Any]:
    payload = _safe_dict(data)
    page = _extract_page(payload, default_page=default_page)
    mode = _normalize_mode(payload.get("mode") or payload.get("advisor_mode") or payload.get("data_mode"))
    payload.update({
        "page": page, "sheet": page, "sheet_name": page, "name": page, "page_name": page, "tab": page, "target_sheet": page,
        "mode": mode, "advisor_mode": _strip(payload.get("advisor_mode")) or mode, "data_mode": _strip(payload.get("data_mode")) or mode,
        "advisor_data_mode": _strip(payload.get("advisor_data_mode")) or mode, "format": _strip(payload.get("format")) or "rows",
    })
    symbols = _get_list(payload, "symbols", "tickers", "symbol", "ticker")
    if symbols:
        payload["symbols"] = symbols
        payload["tickers"] = list(symbols)
    payload["top_n"] = max(1, min(100, _safe_int(payload.get("top_n"), 10)))
    payload["limit"] = max(1, min(5000, _safe_int(payload.get("limit"), 200)))
    payload["offset"] = max(0, _safe_int(payload.get("offset"), 0))
    payload["include_matrix"] = _boolish(payload.get("include_matrix"), True)
    payload["include_headers"] = _boolish(payload.get("include_headers"), True)
    payload["schema_only"] = _boolish(payload.get("schema_only"), False)
    payload["headers_only"] = _boolish(payload.get("headers_only"), False)
    return payload


def _prepare_payload_for_downstream(payload: Mapping[str, Any], *, request_id: str, operation: str) -> Dict[str, Any]:
    out = _default_payload(payload)
    page = _extract_page(out)
    out.update({
        "page": page, "sheet": page, "sheet_name": page, "name": page, "page_name": page, "tab": page,
        "target_sheet": page, "request_id": request_id, "operation": operation, "router": "advisor_short",
        "advisor_version": ADVISOR_VERSION, "format": _strip(out.get("format")) or "rows",
        "include_headers": _boolish(out.get("include_headers"), True),
        "include_matrix": _boolish(out.get("include_matrix"), True),
        "schema_only": _boolish(out.get("schema_only"), False),
        "headers_only": _boolish(out.get("headers_only"), False),
    })
    return out


def _payload_from_get(*, page: Optional[str], sheet: Optional[str], sheet_name: Optional[str], name: Optional[str], tab: Optional[str], symbol: Optional[str], ticker: Optional[str], symbols: Optional[str], tickers: Optional[str], mode: Optional[str], include_matrix: Optional[bool], schema_only: Optional[bool], headers_only: Optional[bool], top_n: Optional[int], limit: Optional[int], offset: Optional[int]) -> Dict[str, Any]:
    payload: Dict[str, Any] = {}
    for k, v in {
        "page": page, "sheet": sheet, "sheet_name": sheet_name, "name": name, "tab": tab,
        "mode": mode, "include_matrix": include_matrix, "schema_only": schema_only,
        "headers_only": headers_only, "top_n": top_n, "limit": limit, "offset": offset,
    }.items():
        if v not in (None, ""):
            payload[k] = v
    symbol_list: List[str] = []
    for raw in (symbols, tickers, symbol, ticker):
        if isinstance(raw, str) and raw.strip():
            for item in _split_csv(raw):
                if item not in symbol_list:
                    symbol_list.append(item)
    if symbol_list:
        payload["symbols"] = symbol_list
        payload["tickers"] = list(symbol_list)
    return payload


async def _invoke_callable(fn: Callable[..., Any], *args: Any, timeout_seconds: float, **kwargs: Any) -> Any:
    if inspect.iscoroutinefunction(fn):
        return await asyncio.wait_for(fn(*args, **kwargs), timeout=timeout_seconds)
    async def _runner() -> Any:
        return await asyncio.to_thread(fn, *args, **kwargs)
    return await asyncio.wait_for(_runner(), timeout=timeout_seconds)


def _looks_like_signature_type_error(exc: TypeError) -> bool:
    message = _strip(exc).lower()
    signature_markers = (
        "unexpected keyword", "unexpected positional", "positional argument", "required positional argument",
        "takes ", "got an unexpected", "multiple values for argument", "missing 1 required", "missing required", "keyword-only argument",
    )
    return any(marker in message for marker in signature_markers)


async def _call_with_tolerant_signatures(fn: Callable[..., Any], *, timeout_seconds: float, args: Sequence[Any] = (), kwargs: Optional[Dict[str, Any]] = None) -> Any:
    payload_kwargs = kwargs or {}
    attempts = [
        (tuple(args), payload_kwargs),
        ((), payload_kwargs),
        ((), {"request": payload_kwargs.get("request"), "body": payload_kwargs.get("body"), "payload": payload_kwargs.get("payload")}),
        ((), {"payload": payload_kwargs.get("payload"), "mode": payload_kwargs.get("mode")}),
        ((), {"body": payload_kwargs.get("body"), "mode": payload_kwargs.get("mode")}),
        ((), {"payload": payload_kwargs.get("payload")}),
        ((), {"body": payload_kwargs.get("body")}),
        ((), {"request": payload_kwargs.get("request")}),
        ((), {
            "page": payload_kwargs.get("page"), "sheet": payload_kwargs.get("sheet"), "sheet_name": payload_kwargs.get("sheet_name"),
            "symbols": payload_kwargs.get("symbols"), "tickers": payload_kwargs.get("tickers"), "top_n": payload_kwargs.get("top_n"),
            "limit": payload_kwargs.get("limit"), "offset": payload_kwargs.get("offset"), "mode": payload_kwargs.get("mode"),
        }),
        ((), {}),
    ]
    last_error: Optional[Exception] = None
    for call_args, call_kwargs in attempts:
        call_kwargs = {k: v for k, v in call_kwargs.items() if v is not None}
        try:
            return await _invoke_callable(fn, *call_args, timeout_seconds=timeout_seconds, **call_kwargs)
        except TypeError as exc:
            last_error = exc
            if _looks_like_signature_type_error(exc):
                continue
            raise
        except asyncio.TimeoutError as exc:
            last_error = exc
            raise
        except Exception as exc:
            last_error = exc
            raise
    if last_error is not None:
        raise last_error
    return None


def _extract_headers_from_spec(spec: Any) -> List[str]:
    if spec is None:
        return []
    if isinstance(spec, Mapping):
        for key in ("headers", "display_headers", "keys", "columns"):
            value = spec.get(key)
            if isinstance(value, list) and value:
                return [_strip(x) for x in value if _strip(x)]
        for key in ("sheet", "spec", "schema", "contract", "definition"):
            nested = spec.get(key)
            headers = _extract_headers_from_spec(nested)
            if headers:
                return headers
    return []


def _resolve_contract_headers(page: str) -> List[str]:
    page = _canonicalize_page_name(page)
    if not page:
        return []
    if page in _SCHEMA_HEADERS_CACHE:
        return list(_SCHEMA_HEADERS_CACHE[page])
    for module_name in ("core.sheets.schema_registry", "core.schema_registry", "core.sheets.page_catalog"):
        module = _import_module_safely(module_name)
        if module is None:
            continue
        for map_name in ("SCHEMA_REGISTRY", "SHEET_SPEC", "SHEET_SPECS", "PAGE_SPECS", "SPECS", "SHEET_CONTRACTS", "STATIC_CANONICAL_SHEET_CONTRACTS"):
            spec_map = getattr(module, map_name, None)
            if isinstance(spec_map, Mapping):
                try:
                    hit = spec_map.get(page) or spec_map.get(page.lower()) or spec_map.get(page.upper())
                except Exception:
                    hit = None
                headers = _extract_headers_from_spec(hit)
                if headers:
                    _SCHEMA_HEADERS_CACHE[page] = headers
                    return list(headers)
        for fn_name in ("get_sheet_spec", "get_schema_for_sheet", "get_schema", "resolve_sheet_spec", "resolve_sheet_schema", "get_contract_for_sheet"):
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
                headers = _extract_headers_from_spec(out)
                if headers:
                    _SCHEMA_HEADERS_CACHE[page] = headers
                    return list(headers)
    return []


def _contract_header_count(page: str) -> int:
    headers = _resolve_contract_headers(page)
    if headers:
        return len(headers)
    return KNOWN_CANONICAL_HEADER_COUNTS.get(page, 0)


def _append_missing_headers(headers: List[str], fields: Sequence[str]) -> List[str]:
    out = list(headers)
    for field in fields:
        if field not in out:
            out.append(field)
    return out


def _mapping_list_to_rows(rows: Iterable[Mapping[str, Any]], headers: Sequence[str]) -> List[List[Any]]:
    matrix: List[List[Any]] = []
    for row in rows:
        matrix.append([row.get(h) for h in headers])
    return matrix


def _rows_matrix_to_objects(matrix: Sequence[Sequence[Any]], headers: Sequence[str]) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    for row in matrix:
        row_list = list(row)
        item: Dict[str, Any] = {}
        for idx, h in enumerate(headers):
            item[str(h)] = row_list[idx] if idx < len(row_list) else None
        out.append(item)
    return out


def _pad_or_trim_matrix(matrix: Sequence[Sequence[Any]], width: int) -> List[List[Any]]:
    out: List[List[Any]] = []
    for row in matrix:
        row_list = list(row)
        if len(row_list) < width:
            row_list = row_list + [None] * (width - len(row_list))
        elif len(row_list) > width:
            row_list = row_list[:width]
        out.append(row_list)
    return out


def _extract_meta_mapping(result: Mapping[str, Any]) -> Dict[str, Any]:
    meta = result.get("meta")
    return dict(meta) if isinstance(meta, Mapping) else {}


def _effective_headers_from_result(result: Mapping[str, Any]) -> List[str]:
    for key in ("headers", "display_headers", "keys", "columns"):
        candidate = result.get(key)
        if isinstance(candidate, list):
            headers = [_strip(x) for x in candidate if _strip(x)]
            if headers:
                return headers
    return []


def _extract_object_rows_any(result: Mapping[str, Any]) -> List[Dict[str, Any]]:
    for key in ("row_objects", "rowObjects", "rows", "items", "data", "quotes", "results", "records"):
        candidate = result.get(key)
        if isinstance(candidate, list) and candidate and all(isinstance(x, Mapping) for x in candidate):
            return [dict(x) for x in candidate]
    return []


def _extract_matrix_rows_any(result: Mapping[str, Any]) -> List[List[Any]]:
    for key in ("rows_matrix", "matrix", "rows"):
        candidate = result.get(key)
        if isinstance(candidate, list) and candidate and all(not isinstance(x, Mapping) for x in candidate):
            return [list(x) if isinstance(x, (list, tuple)) else [x] for x in candidate]
    return []


def _project_to_contract_headers(*, page: str, headers: List[str], row_objects: List[Dict[str, Any]], rows_matrix: List[List[Any]]) -> Tuple[List[str], List[Dict[str, Any]], List[List[Any]]]:
    contract_headers = _resolve_contract_headers(page)
    effective_headers = list(contract_headers or headers or [])
    if page == "Top_10_Investments":
        effective_headers = _append_missing_headers(effective_headers, TOP10_SPECIAL_FIELDS)
    if not effective_headers and row_objects:
        seen: List[str] = []
        for row in row_objects:
            for key in row.keys():
                key_s = _strip(key)
                if key_s and key_s not in seen:
                    seen.append(key_s)
        effective_headers = seen
    if page == "Top_10_Investments":
        for idx, row in enumerate(row_objects, start=1):
            row.setdefault("top10_rank", idx)
            row.setdefault("selection_reason", row.get("selection_reason") or None)
            row.setdefault("criteria_snapshot", row.get("criteria_snapshot") or None)
    if row_objects and effective_headers:
        rows_matrix = _mapping_list_to_rows(row_objects, effective_headers)
    elif rows_matrix and effective_headers:
        rows_matrix = _pad_or_trim_matrix(rows_matrix, len(effective_headers))
        row_objects = _rows_matrix_to_objects(rows_matrix, effective_headers)
    return effective_headers, row_objects, rows_matrix


def _ensure_tabular_shape(result: Dict[str, Any], *, page: str) -> Dict[str, Any]:
    effective_headers: List[str] = _effective_headers_from_result(result)
    object_rows: List[Dict[str, Any]] = _extract_object_rows_any(result)
    matrix_rows: List[List[Any]] = _extract_matrix_rows_any(result)
    effective_headers, object_rows, matrix_rows = _project_to_contract_headers(page=page, headers=effective_headers, row_objects=object_rows, rows_matrix=matrix_rows)
    result["headers"] = effective_headers
    result["keys"] = list(effective_headers)
    result["display_headers"] = list(effective_headers)
    if object_rows:
        result["row_objects"] = object_rows
        result["items"] = object_rows
        result["data"] = object_rows if page != "Data_Dictionary" else result.get("data", object_rows)
    if matrix_rows:
        result["rows_matrix"] = matrix_rows
        result["rows"] = matrix_rows if not object_rows else object_rows
    elif object_rows:
        result["rows_matrix"] = _mapping_list_to_rows(object_rows, effective_headers)
        result["rows"] = object_rows
    meta = _extract_meta_mapping(result)
    if effective_headers:
        meta.setdefault("contract_header_count", _contract_header_count(page) or len(effective_headers))
        meta.setdefault("actual_header_count", len(effective_headers))
    result["meta"] = meta
    return result


def _looks_like_fallback_only_top10(result: Mapping[str, Any]) -> bool:
    page = _strip(result.get("page"))
    if page != "Top_10_Investments":
        return False
    meta = _extract_meta_mapping(result)
    fallback_flag = _boolish(meta.get("fallback"), False)
    reason = _strip(meta.get("reason")).lower()
    rows: List[Mapping[str, Any]] = _extract_object_rows_any(result)
    if not rows:
        headers = _effective_headers_from_result(result)
        matrix_rows = _extract_matrix_rows_any(result)
        if headers and matrix_rows:
            rows = _rows_matrix_to_objects(matrix_rows, headers)
    if not rows:
        return False
    fallback_selection_hits = 0
    low_signal_rows = 0
    for row in rows:
        selection_reason = _strip(row.get("selection_reason")).lower()
        if "fallback candidate" in selection_reason:
            fallback_selection_hits += 1
        informative = 0
        for field in TOP10_BUSINESS_SIGNAL_FIELDS:
            if field in {"top10_rank", "selection_reason", "criteria_snapshot"}:
                continue
            if row.get(field) not in (None, "", [], {}, ()):
                informative += 1
        if informative <= 2:
            low_signal_rows += 1
    mostly_fallback_selection = fallback_selection_hits >= max(1, math.ceil(len(rows) * 0.6))
    mostly_low_signal = low_signal_rows >= max(1, math.ceil(len(rows) * 0.6))
    if fallback_flag and (mostly_fallback_selection or mostly_low_signal):
        return True
    if "engine_unavailable_or_empty" in reason and mostly_low_signal:
        return True
    return False


def _has_usable_payload(result: Mapping[str, Any], *, page: str) -> bool:
    if _boolish(result.get("schema_only"), False) or _boolish(result.get("headers_only"), False):
        return _row_count(result.get("headers")) > 0
    if page == "Data_Dictionary":
        if _row_count(result.get("rows_matrix")) > 0 or _row_count(result.get("row_objects")) > 0:
            return True
        return _row_count(result.get("headers")) > 0
    has_rows = any(_row_count(result.get(k)) > 0 for k in ("rows_matrix", "row_objects", "rows", "items", "data", "quotes"))
    if not has_rows:
        return False
    if page == "Top_10_Investments" and _looks_like_fallback_only_top10(result):
        return False
    return True


def _normalize_result_payload(out: Any, *, page: str) -> Dict[str, Any]:
    safe = _json_safe(out)
    if isinstance(safe, Mapping):
        result = dict(safe)
    elif isinstance(safe, list):
        result = {"status": "success", "page": page, "data": safe, "items": safe}
        if safe and all(isinstance(x, Mapping) for x in safe):
            result["row_objects"] = safe
    else:
        result = {"status": "success", "page": page, "data": safe}
    result.setdefault("status", "success")
    result.setdefault("page", page)
    result.setdefault("sheet", result.get("page") or page)
    result.setdefault("sheet_name", result.get("page") or page)
    result.setdefault("meta", {})
    result = _ensure_tabular_shape(result, page=page)
    return result


def _merge_meta_resolver(result: Dict[str, Any], resolver_meta: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    meta = _extract_meta_mapping(result)
    if resolver_meta:
        meta["resolver"] = dict(resolver_meta)
    result["meta"] = meta
    return result


def _envelope_from_payload_result(*, result: Dict[str, Any], page: str, request_id: str, started_at: float, resolver_meta: Optional[Dict[str, Any]] = None, operation: str = "sheet_rows") -> Dict[str, Any]:
    out = _normalize_result_payload(result, page=page)
    out = _merge_meta_resolver(out, resolver_meta)
    meta = _extract_meta_mapping(out)
    meta.update({
        "request_id": request_id,
        "elapsed_ms": round((time.perf_counter() - started_at) * 1000.0, 2),
        "router": "advisor_short",
        "advisor_version": ADVISOR_VERSION,
        "page": page,
        "operation": operation,
        "contract_header_count": _contract_header_count(page),
        "actual_header_count": len(out.get("headers", []) or []),
        "degraded_top10_rejected": False,
    })
    out["status"] = _strip(out.get("status")) or "success"
    out["page"] = _strip(out.get("page")) or page
    out["sheet"] = _strip(out.get("sheet")) or out["page"]
    out["sheet_name"] = _strip(out.get("sheet_name")) or out["page"]
    out["meta"] = meta
    return _json_safe(out)


async def _resolve_analysis_bridge_impl() -> Tuple[Optional[Callable[..., Any]], Dict[str, Any]]:
    module = _import_module_safely("routes.analysis_sheet_rows")
    if module is None:
        return None, {}
    for name in ("_analysis_sheet_rows_impl", "analysis_sheet_rows_impl", "_analysis_sheet_rows_impl_core"):
        impl = getattr(module, name, None)
        if callable(impl):
            return impl, {"module": "routes.analysis_sheet_rows", "callable": name, "kind": "bridge"}
    return None, {}


async def _resolve_advanced_bridge_impl() -> Tuple[Optional[Callable[..., Any]], Dict[str, Any]]:
    module = _import_module_safely("routes.advanced_analysis")
    if module is None:
        return None, {}
    for name in ("_run_advanced_sheet_rows_impl", "run_advanced_sheet_rows_impl", "_advanced_sheet_rows_impl"):
        impl = getattr(module, name, None)
        if callable(impl):
            return impl, {"module": "routes.advanced_analysis", "callable": name, "kind": "bridge"}
    return None, {}


async def _resolve_advisor_runner(request: Request, *, page: str) -> Tuple[Optional[Callable[..., Any]], Dict[str, Any]]:
    try:
        state = request.app.state
    except Exception:
        state = None
    if state is not None:
        for attr_name in ("advisor_service", "investment_advisor_service", "advisor_runner", "advisor_engine", "investment_advisor_engine", "engine", "service"):
            obj = getattr(state, attr_name, None)
            for method_name in ("run_investment_advisor_engine", "run_investment_advisor", "run_advisor", "advisor_run", "get_recommendations", "recommendations", "run", "execute"):
                fn = getattr(obj, method_name, None)
                if callable(fn):
                    return fn, {"module": "app.state", "object": attr_name, "callable": method_name, "kind": "state_method"}
    for module_name in ("routes.investment_advisor", "core.investment_advisor", "core.investment_advisor_engine"):
        module = _import_module_safely(module_name)
        if module is None:
            continue
        for fn_name in ("_run_investment_advisor_impl", "_run_advisor_impl", "run_investment_advisor_engine", "run_investment_advisor", "run_advisor", "advisor_run", "advisor_recommendations", "get_recommendations", "recommendations", "build_recommendations", "execute", "run"):
            fn = getattr(module, fn_name, None)
            if callable(fn):
                return fn, {"module": module_name, "callable": fn_name, "kind": "function"}
    return None, {}


async def _resolve_top10_builder(request: Request) -> Tuple[Optional[Callable[..., Any]], Dict[str, Any]]:
    try:
        state = request.app.state
    except Exception:
        state = None
    if state is not None:
        for attr_name in ("build_top10_output_rows", "build_top10_rows", "build_top10_investments_rows", "build_top10_investments", "build_top10", "get_top10_rows"):
            candidate = getattr(state, attr_name, None)
            if callable(candidate):
                return candidate, {"module": "app.state", "callable": attr_name, "kind": "state_function"}
    for module_name in ("core.analysis.top10_selector", "routes.investment_advisor", "core.investment_advisor", "core.investment_advisor_engine"):
        module = _import_module_safely(module_name)
        if module is None:
            continue
        for fn_name in ("build_top10_output_rows", "build_top10_rows", "build_top10_investments_rows", "build_top10_investments", "build_top10", "get_top10_rows"):
            fn = getattr(module, fn_name, None)
            if callable(fn):
                return fn, {"module": module_name, "callable": fn_name, "kind": "function"}
    return None, {}


async def _delegate_to_analysis_bridge(*, request: Request, payload: Dict[str, Any], token: Optional[str], x_app_token: Optional[str], authorization: Optional[str], x_request_id: Optional[str]) -> Optional[Dict[str, Any]]:
    impl, meta = await _resolve_analysis_bridge_impl()
    if impl is None:
        return None
    page = _extract_page(payload)
    try:
        out = await _call_with_tolerant_signatures(
            impl,
            timeout_seconds=_resolver_timeout("bridge", page=page),
            kwargs={
                "request": request,
                "body": payload,
                "mode": _strip(payload.get("mode")) or "live",
                "include_matrix_q": _boolish(payload.get("include_matrix"), True),
                "token": token,
                "x_app_token": x_app_token,
                "authorization": authorization,
                "x_request_id": x_request_id,
            },
        )
        result = _normalize_result_payload(out, page=page)
        if not _has_usable_payload(result, page=page):
            return None
        return _merge_meta_resolver(result, meta)
    except Exception as exc:
        logger.warning("Analysis bridge failed. page=%s error=%s", page, exc, exc_info=True)
        return None


async def _delegate_to_advanced_bridge(*, request: Request, payload: Dict[str, Any], token: Optional[str], x_app_token: Optional[str], authorization: Optional[str], x_request_id: Optional[str]) -> Optional[Dict[str, Any]]:
    impl, meta = await _resolve_advanced_bridge_impl()
    if impl is None:
        return None
    page = _extract_page(payload)
    try:
        out = await _call_with_tolerant_signatures(
            impl,
            timeout_seconds=_resolver_timeout("bridge", page=page),
            kwargs={
                "request": request,
                "body": payload,
                "mode": _strip(payload.get("mode")) or "live",
                "include_matrix_q": _boolish(payload.get("include_matrix"), True),
                "token": token,
                "x_app_token": x_app_token,
                "authorization": authorization,
                "x_request_id": x_request_id,
                "page": page,
                "sheet": page,
                "sheet_name": page,
            },
        )
        result = _normalize_result_payload(out, page=page)
        if not _has_usable_payload(result, page=page):
            return None
        return _merge_meta_resolver(result, meta)
    except Exception as exc:
        logger.warning("Advanced bridge failed. page=%s error=%s", page, exc, exc_info=True)
        return None


async def _run_engine_sheet_rows_fallback(*, request: Request, payload: Dict[str, Any], request_id: str) -> Optional[Dict[str, Any]]:
    engine = await _get_engine(request)
    if engine is None:
        return None
    page = _extract_page(payload)
    include_matrix = _boolish(payload.get("include_matrix"), True)
    mode = _strip(payload.get("mode")) or "live"
    last_error: Optional[Exception] = None
    for method_name in ("get_sheet_rows", "sheet_rows", "fetch_sheet_rows", "build_sheet_rows", "read_sheet_rows", "run_sheet_rows"):
        method = getattr(engine, method_name, None)
        if not callable(method):
            continue
        attempts = [
            {"request": request, "body": payload, "payload": payload, "mode": mode, "include_matrix": include_matrix,
             "page": page, "sheet": page, "sheet_name": page, "target_sheet": page,
             "limit": payload.get("limit"), "offset": payload.get("offset"), "symbols": payload.get("symbols"),
             "tickers": payload.get("tickers"), "top_n": payload.get("top_n")},
            {"body": payload, "payload": payload, "mode": mode, "include_matrix": include_matrix,
             "page": page, "sheet": page, "sheet_name": page, "target_sheet": page,
             "limit": payload.get("limit"), "offset": payload.get("offset"), "symbols": payload.get("symbols"),
             "tickers": payload.get("tickers"), "top_n": payload.get("top_n")},
            {"body": payload, "page": page, "sheet": page, "sheet_name": page, "target_sheet": page},
            {"page": page, "sheet": page, "sheet_name": page, "target_sheet": page},
            {"sheet": page}, {},
        ]
        for kwargs in attempts:
            kwargs = {k: v for k, v in kwargs.items() if v is not None}
            try:
                out = await _call_with_tolerant_signatures(method, timeout_seconds=_resolver_timeout("engine", page=page), kwargs=kwargs)
                result = _normalize_result_payload(out, page=page)
                if not _has_usable_payload(result, page=page):
                    continue
                result = _merge_meta_resolver(result, {"source": _safe_engine_type(engine), "callable": method_name, "kind": "engine_fallback"})
                meta = _extract_meta_mapping(result)
                meta.setdefault("request_id", request_id)
                result["meta"] = meta
                return result
            except TypeError as exc:
                last_error = exc
                continue
            except Exception as exc:
                last_error = exc
                continue
    if last_error is not None:
        logger.warning("Engine sheet-rows fallback failed. error=%s", last_error, exc_info=True)
    return None


async def _run_top10_builder(*, request: Request, payload: Dict[str, Any], page: str) -> Tuple[Optional[Dict[str, Any]], Dict[str, Any]]:
    builder, resolver_meta = await _resolve_top10_builder(request)
    if builder is None:
        return None, {}
    try:
        out = await _call_with_tolerant_signatures(
            builder,
            timeout_seconds=_resolver_timeout("top10", page=page),
            kwargs={
                "request": request, "body": payload, "payload": payload, "page": page, "sheet": page, "sheet_name": page,
                "mode": _strip(payload.get("mode")) or "live", "symbols": payload.get("symbols"), "tickers": payload.get("tickers"),
                "top_n": payload.get("top_n"), "limit": payload.get("limit"), "offset": payload.get("offset"),
            },
        )
        result = _normalize_result_payload(out, page=page)
        if _has_usable_payload(result, page=page):
            return result, resolver_meta
        return None, resolver_meta
    except Exception as exc:
        logger.warning("Top10 builder failed; page=%s error=%s", page, exc, exc_info=True)
        return None, resolver_meta or {}


async def _run_advisor_runner(*, request: Request, payload: Dict[str, Any], page: str) -> Tuple[Optional[Dict[str, Any]], Dict[str, Any]]:
    runner, resolver_meta = await _resolve_advisor_runner(request, page=page)
    if runner is None:
        return None, {}
    try:
        out = await _call_with_tolerant_signatures(
            runner,
            timeout_seconds=_resolver_timeout("runner", page=page),
            kwargs={
                "request": request, "body": payload, "payload": payload, "page": page, "sheet": page, "sheet_name": page,
                "mode": _strip(payload.get("mode")) or "live", "symbols": payload.get("symbols"), "tickers": payload.get("tickers"),
                "top_n": payload.get("top_n"), "limit": payload.get("limit"), "offset": payload.get("offset"),
            },
        )
        result = _normalize_result_payload(out, page=page)
        if _has_usable_payload(result, page=page):
            return result, resolver_meta
        return None, resolver_meta
    except Exception as exc:
        logger.warning("Advisor runner failed; page=%s error=%s", page, exc, exc_info=True)
        return None, resolver_meta or {}


def _prefer_direct_bridge(page: str, *, operation: str) -> bool:
    if page in DERIVED_PAGES:
        return True
    if operation == "sheet_rows" and page in BASE_PAGES:
        return True
    return False


def _prefer_top10_builder(page: str, *, operation: str) -> bool:
    return page == "Top_10_Investments" and operation in {"sheet_rows", "recommendations", "run"}


def _prefer_runner(page: str, *, operation: str) -> bool:
    if page in DERIVED_PAGES:
        return False
    return operation in {"recommendations", "run"}


async def _run_advisor_logic(*, request: Request, payload: Dict[str, Any], token: Optional[str], x_app_token: Optional[str], authorization: Optional[str], x_request_id: Optional[str], operation: str = "sheet_rows") -> Dict[str, Any]:
    started_at = time.perf_counter()
    _require_auth_or_401(token_query=token, x_app_token=x_app_token, authorization=authorization)
    request_id = _request_id(request, x_request_id)
    payload = _prepare_payload_for_downstream(payload, request_id=request_id, operation=operation)
    page = _extract_page(payload, default_page=DEFAULT_ADVISOR_PAGE)
    payload.update({"page": page, "sheet": page, "sheet_name": page})

    if _prefer_top10_builder(page, operation=operation):
        top10_result, top10_meta = await _run_top10_builder(request=request, payload=payload, page=page)
        if top10_result is not None:
            return _envelope_from_payload_result(result=top10_result, page=page, request_id=request_id, started_at=started_at, resolver_meta=top10_meta or {"source": "top10_builder", "kind": "direct"}, operation=operation)

    if _prefer_direct_bridge(page, operation=operation):
        analysis_result = await _delegate_to_analysis_bridge(request=request, payload=payload, token=token, x_app_token=x_app_token, authorization=authorization, x_request_id=request_id)
        if analysis_result is not None:
            return _envelope_from_payload_result(result=analysis_result, page=page, request_id=request_id, started_at=started_at, resolver_meta=_extract_meta_mapping(analysis_result).get("resolver"), operation=operation)
        advanced_result = await _delegate_to_advanced_bridge(request=request, payload=payload, token=token, x_app_token=x_app_token, authorization=authorization, x_request_id=request_id)
        if advanced_result is not None:
            return _envelope_from_payload_result(result=advanced_result, page=page, request_id=request_id, started_at=started_at, resolver_meta=_extract_meta_mapping(advanced_result).get("resolver"), operation=operation)
        engine_result = await _run_engine_sheet_rows_fallback(request=request, payload=payload, request_id=request_id)
        if engine_result is not None:
            return _envelope_from_payload_result(result=engine_result, page=page, request_id=request_id, started_at=started_at, resolver_meta=_extract_meta_mapping(engine_result).get("resolver"), operation=operation)

    if _prefer_runner(page, operation=operation):
        runner_result, runner_meta = await _run_advisor_runner(request=request, payload=payload, page=page)
        if runner_result is not None:
            return _envelope_from_payload_result(result=runner_result, page=page, request_id=request_id, started_at=started_at, resolver_meta=runner_meta, operation=operation)

    analysis_result = await _delegate_to_analysis_bridge(request=request, payload=payload, token=token, x_app_token=x_app_token, authorization=authorization, x_request_id=request_id)
    if analysis_result is not None:
        return _envelope_from_payload_result(result=analysis_result, page=page, request_id=request_id, started_at=started_at, resolver_meta=_extract_meta_mapping(analysis_result).get("resolver"), operation=operation)
    advanced_result = await _delegate_to_advanced_bridge(request=request, payload=payload, token=token, x_app_token=x_app_token, authorization=authorization, x_request_id=request_id)
    if advanced_result is not None:
        return _envelope_from_payload_result(result=advanced_result, page=page, request_id=request_id, started_at=started_at, resolver_meta=_extract_meta_mapping(advanced_result).get("resolver"), operation=operation)
    engine_result = await _run_engine_sheet_rows_fallback(request=request, payload=payload, request_id=request_id)
    if engine_result is not None:
        return _envelope_from_payload_result(result=engine_result, page=page, request_id=request_id, started_at=started_at, resolver_meta=_extract_meta_mapping(engine_result).get("resolver"), operation=operation)

    raise HTTPException(status_code=503, detail="Advisor router could not resolve a usable bridge, builder, or engine provider")


@router.get("/health")
@router.get("/healthz")
@router.get("/ping")
async def advisor_health(request: Request) -> Dict[str, Any]:
    engine = await _get_engine(request)
    analysis_impl, analysis_meta = await _resolve_analysis_bridge_impl()
    advanced_impl, advanced_meta = await _resolve_advanced_bridge_impl()
    runner, runner_meta = await _resolve_advisor_runner(request, page=DEFAULT_ADVISOR_PAGE)
    top10_builder, top10_meta = await _resolve_top10_builder(request)
    settings_summary: Dict[str, Any] = {}
    try:
        settings = get_settings_cached()
        if isinstance(settings, Mapping):
            for key in ("APP_VERSION", "ENV", "OPEN_MODE", "REQUIRE_AUTH"):
                value = settings.get(key)
                if value not in (None, ""):
                    settings_summary[key] = value
    except Exception:
        settings_summary = {}
    contract_counts = {k: _contract_header_count(k) for k in KNOWN_CANONICAL_HEADER_COUNTS.keys()}
    return _json_safe({
        "status": "ok" if (engine is not None or analysis_impl is not None or advanced_impl is not None) else "degraded",
        "version": ADVISOR_VERSION,
        "short_family": True,
        "engine_available": bool(engine),
        "engine_type": _safe_engine_type(engine),
        "analysis_bridge_available": bool(analysis_impl),
        "analysis_bridge": analysis_meta or None,
        "advanced_bridge_available": bool(advanced_impl),
        "advanced_bridge": advanced_meta or None,
        "advisor_runner_available": bool(runner),
        "advisor_runner": runner_meta or None,
        "top10_builder_available": bool(top10_builder),
        "top10_builder": top10_meta or None,
        "default_page": DEFAULT_ADVISOR_PAGE,
        "base_pages": sorted(BASE_PAGES),
        "derived_pages": sorted(DERIVED_PAGES),
        "contract_header_counts": contract_counts,
        "open_mode": _is_open_mode_enabled(),
        "settings": settings_summary or None,
        "timestamp_utc": _now_utc(),
        "request_id": _strip(getattr(request.state, "request_id", "")) or None,
    })


@router.get("/meta")
async def advisor_meta(request: Request) -> Dict[str, Any]:
    engine = await _get_engine(request)
    analysis_impl, analysis_meta = await _resolve_analysis_bridge_impl()
    advanced_impl, advanced_meta = await _resolve_advanced_bridge_impl()
    runner, runner_meta = await _resolve_advisor_runner(request, page=DEFAULT_ADVISOR_PAGE)
    top10_builder, top10_meta = await _resolve_top10_builder(request)
    return _json_safe({
        "status": "success",
        "version": ADVISOR_VERSION,
        "route_family": "advisor_short",
        "default_page": DEFAULT_ADVISOR_PAGE,
        "engine_present": bool(engine),
        "engine_type": _safe_engine_type(engine),
        "analysis_bridge_available": bool(analysis_impl),
        "analysis_bridge": analysis_meta or None,
        "advanced_bridge_available": bool(advanced_impl),
        "advanced_bridge": advanced_meta or None,
        "advisor_runner_available": bool(runner),
        "advisor_runner": runner_meta or None,
        "top10_builder_available": bool(top10_builder),
        "top10_builder": top10_meta or None,
        "contract_header_counts": {k: _contract_header_count(k) for k in KNOWN_CANONICAL_HEADER_COUNTS.keys()},
        "timestamp_utc": _now_utc(),
        "request_id": _strip(getattr(request.state, "request_id", "")) or None,
    })


@router.get("/metrics")
async def advisor_metrics() -> Response:
    if not PROMETHEUS_AVAILABLE or generate_latest is None:
        return Response(content="Metrics not available", media_type="text/plain", status_code=503)
    return Response(content=generate_latest(), media_type=CONTENT_TYPE_LATEST)


@router.get("/sheet-rows")
@router.get("/sheet_rows")
async def advisor_sheet_rows_get(request: Request, page: Optional[str] = Query(default=None), sheet: Optional[str] = Query(default=None), sheet_name: Optional[str] = Query(default=None), name: Optional[str] = Query(default=None), tab: Optional[str] = Query(default=None), symbol: Optional[str] = Query(default=None), ticker: Optional[str] = Query(default=None), symbols: Optional[str] = Query(default=None), tickers: Optional[str] = Query(default=None), mode: str = Query(default="live"), include_matrix: Optional[bool] = Query(default=None), schema_only: Optional[bool] = Query(default=None), headers_only: Optional[bool] = Query(default=None), top_n: Optional[int] = Query(default=None, ge=1, le=500), limit: int = Query(default=200, ge=1, le=5000), offset: int = Query(default=0, ge=0), token: Optional[str] = Query(default=None), x_app_token: Optional[str] = Header(default=None, alias="X-APP-TOKEN"), authorization: Optional[str] = Header(default=None, alias="Authorization"), x_request_id: Optional[str] = Header(default=None, alias="X-Request-ID")) -> Dict[str, Any]:
    payload = _payload_from_get(page=page, sheet=sheet, sheet_name=sheet_name, name=name, tab=tab, symbol=symbol, ticker=ticker, symbols=symbols, tickers=tickers, mode=mode, include_matrix=include_matrix, schema_only=schema_only, headers_only=headers_only, top_n=top_n, limit=limit, offset=offset)
    return await _run_advisor_logic(request=request, payload=payload, token=token, x_app_token=x_app_token, authorization=authorization, x_request_id=x_request_id, operation="sheet_rows")


@router.post("/sheet-rows")
@router.post("/sheet_rows")
async def advisor_sheet_rows_post(request: Request, body: Dict[str, Any] = Body(default_factory=dict), token: Optional[str] = Query(default=None), x_app_token: Optional[str] = Header(default=None, alias="X-APP-TOKEN"), authorization: Optional[str] = Header(default=None, alias="Authorization"), x_request_id: Optional[str] = Header(default=None, alias="X-Request-ID")) -> Dict[str, Any]:
    return await _run_advisor_logic(request=request, payload=_safe_dict(body), token=token, x_app_token=x_app_token, authorization=authorization, x_request_id=x_request_id, operation="sheet_rows")


@router.get("/recommendations")
async def advisor_recommendations_get(request: Request, symbol: Optional[str] = Query(default=None), ticker: Optional[str] = Query(default=None), symbols: Optional[str] = Query(default=None), tickers: Optional[str] = Query(default=None), page: Optional[str] = Query(default=DEFAULT_ADVISOR_PAGE), mode: str = Query(default="live"), top_n: int = Query(default=10, ge=1, le=100), limit: int = Query(default=200, ge=1, le=5000), offset: int = Query(default=0, ge=0), include_matrix: Optional[bool] = Query(default=None), token: Optional[str] = Query(default=None), x_app_token: Optional[str] = Header(default=None, alias="X-APP-TOKEN"), authorization: Optional[str] = Header(default=None, alias="Authorization"), x_request_id: Optional[str] = Header(default=None, alias="X-Request-ID")) -> Dict[str, Any]:
    payload = _payload_from_get(page=page, sheet=None, sheet_name=None, name=None, tab=None, symbol=symbol, ticker=ticker, symbols=symbols, tickers=tickers, mode=mode, include_matrix=include_matrix, schema_only=None, headers_only=None, top_n=top_n, limit=limit, offset=offset)
    return await _run_advisor_logic(request=request, payload=payload, token=token, x_app_token=x_app_token, authorization=authorization, x_request_id=x_request_id, operation="recommendations")


@router.post("/recommendations")
async def advisor_recommendations_post(request: Request, body: Dict[str, Any] = Body(default_factory=dict), token: Optional[str] = Query(default=None), x_app_token: Optional[str] = Header(default=None, alias="X-APP-TOKEN"), authorization: Optional[str] = Header(default=None, alias="Authorization"), x_request_id: Optional[str] = Header(default=None, alias="X-Request-ID")) -> Dict[str, Any]:
    payload = _safe_dict(body)
    payload.setdefault("page", payload.get("sheet") or payload.get("sheet_name") or DEFAULT_ADVISOR_PAGE)
    return await _run_advisor_logic(request=request, payload=payload, token=token, x_app_token=x_app_token, authorization=authorization, x_request_id=x_request_id, operation="recommendations")


@router.get("/run")
async def advisor_run_get(request: Request, page: Optional[str] = Query(default=DEFAULT_ADVISOR_PAGE), symbols: Optional[str] = Query(default=None), tickers: Optional[str] = Query(default=None), mode: str = Query(default="live"), top_n: int = Query(default=10, ge=1, le=100), limit: int = Query(default=200, ge=1, le=5000), offset: int = Query(default=0, ge=0), token: Optional[str] = Query(default=None), x_app_token: Optional[str] = Header(default=None, alias="X-APP-TOKEN"), authorization: Optional[str] = Header(default=None, alias="Authorization"), x_request_id: Optional[str] = Header(default=None, alias="X-Request-ID")) -> Dict[str, Any]:
    payload = _payload_from_get(page=page, sheet=None, sheet_name=None, name=None, tab=None, symbol=None, ticker=None, symbols=symbols, tickers=tickers, mode=mode, include_matrix=None, schema_only=None, headers_only=None, top_n=top_n, limit=limit, offset=offset)
    return await _run_advisor_logic(request=request, payload=payload, token=token, x_app_token=x_app_token, authorization=authorization, x_request_id=x_request_id, operation="run")


@router.post("/run")
async def advisor_run_post(request: Request, body: Dict[str, Any] = Body(default_factory=dict), token: Optional[str] = Query(default=None), x_app_token: Optional[str] = Header(default=None, alias="X-APP-TOKEN"), authorization: Optional[str] = Header(default=None, alias="Authorization"), x_request_id: Optional[str] = Header(default=None, alias="X-Request-ID")) -> Dict[str, Any]:
    return await _run_advisor_logic(request=request, payload=_safe_dict(body), token=token, x_app_token=x_app_token, authorization=authorization, x_request_id=x_request_id, operation="run")


__all__ = ["router", "ADVISOR_VERSION"]
