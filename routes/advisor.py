#!/usr/bin/env python3
"""
routes/advisor.py
================================================================================
ADVISOR ROUTER — v6.0.0
================================================================================
SHORT-ADVISOR FAMILY RESTORED • LIVE-BY-DEFAULT • UNIFIED-SHEET-ROWS
RESOLVER-HARDENED • QUERY+BODY COMPATIBLE • JSON-SAFE • ENGINE-TOLERANT
ADVANCED-ROUTER BRIDGE • TOP10 DEFAULT • ASYNC-SAFE

Why this revision
-----------------
- Restores the short advisor family under `/v1/advisor/*`.
- Adds direct `/v1/advisor/sheet-rows` and `/v1/advisor/sheet_rows`.
- Bridges short advisor sheet-rows to `routes.advanced_analysis` so schema,
  root/v1 behavior, Top10, Insights, and dictionary handling stay aligned.
- Prefers LIVE mode and defaults to `Top_10_Investments` when advisor-specific
  endpoints are called without an explicit page.
- Tolerantly resolves advisor runners from `routes.investment_advisor`,
  `core.investment_advisor`, and `core.investment_advisor_engine`.
- Falls back safely to schema-aligned sheet-rows output when a dedicated
  advisor runner is unavailable.
"""

from __future__ import annotations

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
from typing import Any, Callable, Dict, List, Mapping, Optional, Tuple

from fastapi import Body, Header, HTTPException, Query, Request, Response
from fastapi.encoders import jsonable_encoder
from fastapi.routing import APIRouter

logger = logging.getLogger("routes.advisor")
logger.addHandler(logging.NullHandler())

ADVISOR_VERSION = "6.0.0"

router = APIRouter(prefix="/v1/advisor", tags=["advisor"])

DEFAULT_ADVISOR_PAGE = "Top_10_Investments"
BASE_PAGES = {
    "Market_Leaders",
    "Global_Markets",
    "Commodities_FX",
    "Mutual_Funds",
    "My_Portfolio",
}
SPECIAL_PAGES = {
    "Top_10_Investments",
    "Insights_Analysis",
    "Data_Dictionary",
}

RUNNER_MODULE_CANDIDATES: Tuple[str, ...] = (
    "routes.investment_advisor",
    "core.investment_advisor",
    "core.investment_advisor_engine",
)

RUNNER_FUNCTION_CANDIDATES: Tuple[str, ...] = (
    "run_investment_advisor_engine",
    "run_investment_advisor",
    "run_advisor",
    "advisor_run",
    "advisor_recommendations",
    "get_recommendations",
    "recommendations",
    "build_recommendations",
    "execute",
    "run",
)

RUNNER_METHOD_CANDIDATES: Tuple[str, ...] = (
    "run_investment_advisor_engine",
    "run_investment_advisor",
    "run_advisor",
    "advisor_run",
    "get_recommendations",
    "recommendations",
    "run",
    "execute",
)

SERVICE_OBJECT_CANDIDATES: Tuple[str, ...] = (
    "advisor_service",
    "investment_advisor_service",
    "advisor_runner",
    "advisor_engine",
    "engine",
    "service",
)

# -----------------------------------------------------------------------------
# Optional Prometheus
# -----------------------------------------------------------------------------
try:
    from prometheus_client import CONTENT_TYPE_LATEST, generate_latest  # type: ignore

    PROMETHEUS_AVAILABLE = True
except Exception:
    CONTENT_TYPE_LATEST = "text/plain"
    generate_latest = None  # type: ignore
    PROMETHEUS_AVAILABLE = False

# -----------------------------------------------------------------------------
# Optional config/auth hooks
# -----------------------------------------------------------------------------
try:
    from core.config import auth_ok, get_settings_cached, is_open_mode  # type: ignore
except Exception:
    auth_ok = None  # type: ignore
    is_open_mode = None  # type: ignore

    def get_settings_cached(*args: Any, **kwargs: Any) -> Any:  # type: ignore
        return None


# -----------------------------------------------------------------------------
# Generic helpers
# -----------------------------------------------------------------------------
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


def _safe_float(v: Any) -> Optional[float]:
    try:
        if v is None or isinstance(v, bool):
            return None
        f = float(v)
        if math.isnan(f) or math.isinf(f):
            return None
        return f
    except Exception:
        return None


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


def _require_auth_or_401(
    *,
    token_query: Optional[str],
    x_app_token: Optional[str],
    authorization: Optional[str],
) -> None:
    if _is_open_mode_enabled():
        return
    if _auth_ok_via_hook(token_query=token_query, x_app_token=x_app_token, authorization=authorization):
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
    for attr in (
        "engine",
        "data_engine",
        "engine_v2",
        "data_engine_v2",
        "advisor_engine",
        "investment_advisor_engine",
    ):
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

    for module_name in (
        "core.data_engine_v2",
        "core.data_engine",
        "core.investment_advisor_engine",
    ):
        module = _import_module_safely(module_name)
        if module is None:
            continue

        for attr in (
            "engine",
            "data_engine",
            "ENGINE",
            "DATA_ENGINE",
            "advisor_engine",
            "investment_advisor_engine",
            "get_engine",
            "get_data_engine",
            "build_engine",
            "create_engine",
        ):
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
        for fn_name in (
            "canonicalize_page_name",
            "canonical_page_name",
            "normalize_page_name",
            "resolve_page_name",
            "resolve_canonical_page_name",
        ):
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
        "market leaders": "Market_Leaders",
        "market_leaders": "Market_Leaders",
        "global markets": "Global_Markets",
        "global_markets": "Global_Markets",
        "commodities fx": "Commodities_FX",
        "commodities_fx": "Commodities_FX",
        "commodities-fx": "Commodities_FX",
        "mutual funds": "Mutual_Funds",
        "mutual_funds": "Mutual_Funds",
        "my portfolio": "My_Portfolio",
        "my_portfolio": "My_Portfolio",
        "insights analysis": "Insights_Analysis",
        "insights_analysis": "Insights_Analysis",
        "top 10 investments": "Top_10_Investments",
        "top_10_investments": "Top_10_Investments",
        "top10_investments": "Top_10_Investments",
        "data dictionary": "Data_Dictionary",
        "data_dictionary": "Data_Dictionary",
    }
    return fallback_map.get(raw.lower(), raw)


def _extract_page(data: Mapping[str, Any], default_page: str = DEFAULT_ADVISOR_PAGE) -> str:
    raw = (
        data.get("page")
        or data.get("sheet")
        or data.get("sheet_name")
        or data.get("name")
        or data.get("tab")
        or default_page
    )
    page = _canonicalize_page_name(raw)
    return page or default_page


def _default_payload(data: Mapping[str, Any], default_page: str = DEFAULT_ADVISOR_PAGE) -> Dict[str, Any]:
    payload = _safe_dict(data)
    page = _extract_page(payload, default_page=default_page)
    payload["page"] = page
    payload["sheet"] = page
    payload["sheet_name"] = page
    payload["name"] = page
    payload.setdefault("mode", payload.get("advisor_mode") or payload.get("data_mode") or "live")
    payload.setdefault("advisor_mode", payload.get("mode") or "live")
    payload.setdefault("data_mode", payload.get("mode") or "live")
    payload["mode"] = _strip(payload.get("mode")) or "live"

    symbols = _get_list(payload, "symbols", "tickers", "symbol", "ticker")
    if symbols:
        payload["symbols"] = symbols
        payload["tickers"] = list(symbols)

    payload["limit"] = max(1, min(5000, _safe_int(payload.get("limit"), 200)))
    payload["offset"] = max(0, _safe_int(payload.get("offset"), 0))
    return payload


def _payload_from_get(
    *,
    page: Optional[str],
    sheet: Optional[str],
    sheet_name: Optional[str],
    name: Optional[str],
    tab: Optional[str],
    symbol: Optional[str],
    ticker: Optional[str],
    symbols: Optional[str],
    tickers: Optional[str],
    mode: Optional[str],
    include_matrix: Optional[bool],
    schema_only: Optional[bool],
    headers_only: Optional[bool],
    top_n: Optional[int],
    limit: Optional[int],
    offset: Optional[int],
) -> Dict[str, Any]:
    payload: Dict[str, Any] = {}
    for k, v in {
        "page": page,
        "sheet": sheet,
        "sheet_name": sheet_name,
        "name": name,
        "tab": tab,
        "mode": mode,
        "include_matrix": include_matrix,
        "schema_only": schema_only,
        "headers_only": headers_only,
        "top_n": top_n,
        "limit": limit,
        "offset": offset,
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


async def _call_with_tolerant_signatures(fn: Callable[..., Any], *args: Any, **kwargs: Any) -> Any:
    attempts = [
        (args, kwargs),
        ((), kwargs),
        ((), {"request": kwargs.get("request"), "body": kwargs.get("body"), "payload": kwargs.get("payload")}),
        ((), {"payload": kwargs.get("payload"), "mode": kwargs.get("mode")}),
        ((), {"body": kwargs.get("body"), "mode": kwargs.get("mode")}),
        ((), {"payload": kwargs.get("payload")}),
        ((), {"body": kwargs.get("body")}),
        ((), {"request": kwargs.get("request")}),
        ((), {}),
    ]

    last_error: Optional[Exception] = None
    for call_args, call_kwargs in attempts:
        call_kwargs = {k: v for k, v in call_kwargs.items() if v is not None}
        try:
            out = fn(*call_args, **call_kwargs)
            if inspect.isawaitable(out):
                out = await out
            return out
        except TypeError as exc:
            last_error = exc
            continue
        except Exception as exc:
            last_error = exc
            continue

    if last_error is not None:
        raise last_error
    return None


def _resolve_runner_from_module(module: Any):
    if module is None:
        return None, {}

    for fn_name in RUNNER_FUNCTION_CANDIDATES:
        fn = getattr(module, fn_name, None)
        if callable(fn):
            return fn, {"module": getattr(module, "__name__", "unknown"), "callable": fn_name, "kind": "function"}

    for obj_name in SERVICE_OBJECT_CANDIDATES:
        obj = getattr(module, obj_name, None)
        if obj is None:
            continue
        for method_name in RUNNER_METHOD_CANDIDATES:
            fn = getattr(obj, method_name, None)
            if callable(fn):
                return fn, {
                    "module": getattr(module, "__name__", "unknown"),
                    "object": obj_name,
                    "callable": method_name,
                    "kind": "object_method",
                }

    for factory_name in ("get_service", "build_service", "create_service", "get_advisor_service"):
        factory = getattr(module, factory_name, None)
        if not callable(factory):
            continue
        try:
            obj = factory()
            if inspect.isawaitable(obj):
                continue
            if obj is None:
                continue
            for method_name in RUNNER_METHOD_CANDIDATES:
                fn = getattr(obj, method_name, None)
                if callable(fn):
                    return fn, {
                        "module": getattr(module, "__name__", "unknown"),
                        "object": factory_name,
                        "callable": method_name,
                        "kind": "factory_method",
                    }
        except Exception:
            continue

    return None, {}


def _resolve_advisor_runner():
    for module_name in RUNNER_MODULE_CANDIDATES:
        module = _import_module_safely(module_name)
        fn, meta = _resolve_runner_from_module(module)
        if fn is not None:
            return fn, meta
    return None, {}


def _is_payload_like(value: Any) -> bool:
    return isinstance(value, Mapping)


async def _delegate_to_advanced_sheet_rows(
    *,
    request: Request,
    payload: Dict[str, Any],
    include_matrix_q: Optional[bool],
    token: Optional[str],
    x_app_token: Optional[str],
    authorization: Optional[str],
    x_request_id: Optional[str],
) -> Dict[str, Any]:
    module = _import_module_safely("routes.advanced_analysis")
    if module is None:
        raise HTTPException(status_code=503, detail="Advanced analysis router not available")

    impl = getattr(module, "_run_advanced_sheet_rows_impl", None)
    if not callable(impl):
        raise HTTPException(status_code=503, detail="Advanced sheet-rows implementation not available")

    out = impl(
        request=request,
        body=payload,
        mode=_strip(payload.get("mode")) or "live",
        include_matrix_q=include_matrix_q,
        token=token,
        x_app_token=x_app_token,
        authorization=authorization,
        x_request_id=x_request_id,
    )
    if inspect.isawaitable(out):
        out = await out
    if not isinstance(out, dict):
        return _json_safe({"status": "error", "detail": "Unexpected advanced-analysis response"})
    return _json_safe(out)


def _envelope_from_payload_result(
    *,
    result: Dict[str, Any],
    page: str,
    request_id: str,
    started_at: float,
    resolver_meta: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    meta = _safe_dict(result.get("meta"))
    meta.update({
        "request_id": request_id,
        "elapsed_ms": round((time.perf_counter() - started_at) * 1000.0, 2),
        "router": "advisor_short",
        "advisor_version": ADVISOR_VERSION,
        "page": page,
    })
    if resolver_meta:
        meta["resolver"] = resolver_meta

    out = dict(result)
    out["status"] = _strip(out.get("status")) or "success"
    out["page"] = _strip(out.get("page")) or page
    out["sheet"] = _strip(out.get("sheet")) or out["page"]
    out["sheet_name"] = _strip(out.get("sheet_name")) or out["page"]
    out["meta"] = meta
    return _json_safe(out)


async def _run_advisor_logic(
    *,
    request: Request,
    payload: Dict[str, Any],
    token: Optional[str],
    x_app_token: Optional[str],
    authorization: Optional[str],
    x_request_id: Optional[str],
) -> Dict[str, Any]:
    started_at = time.perf_counter()
    _require_auth_or_401(token_query=token, x_app_token=x_app_token, authorization=authorization)
    request_id = _request_id(request, x_request_id)

    payload = _default_payload(payload, default_page=DEFAULT_ADVISOR_PAGE)
    page = _extract_page(payload, default_page=DEFAULT_ADVISOR_PAGE)
    payload["page"] = page
    payload["sheet"] = page
    payload["sheet_name"] = page

    if page in BASE_PAGES or page in SPECIAL_PAGES:
        advanced_result = await _delegate_to_advanced_sheet_rows(
            request=request,
            payload=payload,
            include_matrix_q=_boolish(payload.get("include_matrix"), True),
            token=token,
            x_app_token=x_app_token,
            authorization=authorization,
            x_request_id=request_id,
        )
        return _envelope_from_payload_result(
            result=advanced_result,
            page=page,
            request_id=request_id,
            started_at=started_at,
            resolver_meta={"source": "routes.advanced_analysis", "kind": "bridge"},
        )

    runner, resolver_meta = _resolve_advisor_runner()
    if runner is not None:
        try:
            out = await _call_with_tolerant_signatures(
                runner,
                request=request,
                body=payload,
                payload=payload,
                mode=_strip(payload.get("mode")) or "live",
            )
            if isinstance(out, dict):
                return _envelope_from_payload_result(
                    result=out,
                    page=page,
                    request_id=request_id,
                    started_at=started_at,
                    resolver_meta=resolver_meta,
                )
            if _is_payload_like(out):
                return _envelope_from_payload_result(
                    result=dict(out),
                    page=page,
                    request_id=request_id,
                    started_at=started_at,
                    resolver_meta=resolver_meta,
                )
        except Exception as exc:
            logger.warning("Advisor runner failed; falling back to advanced sheet-rows. error=%s", exc, exc_info=True)

    advanced_result = await _delegate_to_advanced_sheet_rows(
        request=request,
        payload=payload,
        include_matrix_q=_boolish(payload.get("include_matrix"), True),
        token=token,
        x_app_token=x_app_token,
        authorization=authorization,
        x_request_id=request_id,
    )
    return _envelope_from_payload_result(
        result=advanced_result,
        page=page,
        request_id=request_id,
        started_at=started_at,
        resolver_meta={"source": "routes.advanced_analysis", "kind": "fallback_bridge"},
    )


@router.get("/health")
async def advisor_health(request: Request) -> Dict[str, Any]:
    engine = await _get_engine(request)
    runner, resolver_meta = _resolve_advisor_runner()

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

    return _json_safe({
        "status": "ok" if engine is not None else "degraded",
        "version": ADVISOR_VERSION,
        "short_family": True,
        "engine_available": bool(engine),
        "engine_type": _safe_engine_type(engine),
        "advisor_runner_available": bool(runner),
        "advisor_runner": resolver_meta or None,
        "default_page": DEFAULT_ADVISOR_PAGE,
        "base_pages": sorted(BASE_PAGES),
        "special_pages": sorted(SPECIAL_PAGES),
        "open_mode": _is_open_mode_enabled(),
        "settings": settings_summary or None,
        "timestamp_utc": _now_utc(),
        "request_id": _strip(getattr(request.state, "request_id", "")) or None,
    })


@router.get("/meta")
async def advisor_meta(request: Request) -> Dict[str, Any]:
    engine = await _get_engine(request)
    runner, resolver_meta = _resolve_advisor_runner()
    return _json_safe({
        "status": "success",
        "version": ADVISOR_VERSION,
        "route_family": "advisor_short",
        "default_page": DEFAULT_ADVISOR_PAGE,
        "engine_present": bool(engine),
        "engine_type": _safe_engine_type(engine),
        "advisor_runner_available": bool(runner),
        "advisor_runner": resolver_meta or None,
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
async def advisor_sheet_rows_get(
    request: Request,
    page: Optional[str] = Query(default=None),
    sheet: Optional[str] = Query(default=None),
    sheet_name: Optional[str] = Query(default=None),
    name: Optional[str] = Query(default=None),
    tab: Optional[str] = Query(default=None),
    symbol: Optional[str] = Query(default=None),
    ticker: Optional[str] = Query(default=None),
    symbols: Optional[str] = Query(default=None),
    tickers: Optional[str] = Query(default=None),
    mode: str = Query(default="live"),
    include_matrix: Optional[bool] = Query(default=None),
    schema_only: Optional[bool] = Query(default=None),
    headers_only: Optional[bool] = Query(default=None),
    top_n: Optional[int] = Query(default=None, ge=1, le=500),
    limit: int = Query(default=200, ge=1, le=5000),
    offset: int = Query(default=0, ge=0),
    token: Optional[str] = Query(default=None),
    x_app_token: Optional[str] = Header(default=None, alias="X-APP-TOKEN"),
    authorization: Optional[str] = Header(default=None, alias="Authorization"),
    x_request_id: Optional[str] = Header(default=None, alias="X-Request-ID"),
) -> Dict[str, Any]:
    payload = _payload_from_get(
        page=page,
        sheet=sheet,
        sheet_name=sheet_name,
        name=name,
        tab=tab,
        symbol=symbol,
        ticker=ticker,
        symbols=symbols,
        tickers=tickers,
        mode=mode,
        include_matrix=include_matrix,
        schema_only=schema_only,
        headers_only=headers_only,
        top_n=top_n,
        limit=limit,
        offset=offset,
    )
    return await _run_advisor_logic(
        request=request,
        payload=payload,
        token=token,
        x_app_token=x_app_token,
        authorization=authorization,
        x_request_id=x_request_id,
    )


@router.post("/sheet-rows")
@router.post("/sheet_rows")
async def advisor_sheet_rows_post(
    request: Request,
    body: Dict[str, Any] = Body(default_factory=dict),
    token: Optional[str] = Query(default=None),
    x_app_token: Optional[str] = Header(default=None, alias="X-APP-TOKEN"),
    authorization: Optional[str] = Header(default=None, alias="Authorization"),
    x_request_id: Optional[str] = Header(default=None, alias="X-Request-ID"),
) -> Dict[str, Any]:
    return await _run_advisor_logic(
        request=request,
        payload=_safe_dict(body),
        token=token,
        x_app_token=x_app_token,
        authorization=authorization,
        x_request_id=x_request_id,
    )


@router.get("/recommendations")
async def advisor_recommendations_get(
    request: Request,
    symbol: Optional[str] = Query(default=None),
    ticker: Optional[str] = Query(default=None),
    symbols: Optional[str] = Query(default=None),
    tickers: Optional[str] = Query(default=None),
    page: Optional[str] = Query(default=DEFAULT_ADVISOR_PAGE),
    mode: str = Query(default="live"),
    top_n: int = Query(default=10, ge=1, le=100),
    limit: int = Query(default=200, ge=1, le=5000),
    offset: int = Query(default=0, ge=0),
    include_matrix: Optional[bool] = Query(default=None),
    token: Optional[str] = Query(default=None),
    x_app_token: Optional[str] = Header(default=None, alias="X-APP-TOKEN"),
    authorization: Optional[str] = Header(default=None, alias="Authorization"),
    x_request_id: Optional[str] = Header(default=None, alias="X-Request-ID"),
) -> Dict[str, Any]:
    payload = _payload_from_get(
        page=page,
        sheet=None,
        sheet_name=None,
        name=None,
        tab=None,
        symbol=symbol,
        ticker=ticker,
        symbols=symbols,
        tickers=tickers,
        mode=mode,
        include_matrix=include_matrix,
        schema_only=None,
        headers_only=None,
        top_n=top_n,
        limit=limit,
        offset=offset,
    )
    return await _run_advisor_logic(
        request=request,
        payload=payload,
        token=token,
        x_app_token=x_app_token,
        authorization=authorization,
        x_request_id=x_request_id,
    )


@router.post("/recommendations")
async def advisor_recommendations_post(
    request: Request,
    body: Dict[str, Any] = Body(default_factory=dict),
    token: Optional[str] = Query(default=None),
    x_app_token: Optional[str] = Header(default=None, alias="X-APP-TOKEN"),
    authorization: Optional[str] = Header(default=None, alias="Authorization"),
    x_request_id: Optional[str] = Header(default=None, alias="X-Request-ID"),
) -> Dict[str, Any]:
    payload = _safe_dict(body)
    payload.setdefault("page", payload.get("sheet") or payload.get("sheet_name") or DEFAULT_ADVISOR_PAGE)
    return await _run_advisor_logic(
        request=request,
        payload=payload,
        token=token,
        x_app_token=x_app_token,
        authorization=authorization,
        x_request_id=x_request_id,
    )


@router.get("/run")
async def advisor_run_get(
    request: Request,
    page: Optional[str] = Query(default=DEFAULT_ADVISOR_PAGE),
    symbols: Optional[str] = Query(default=None),
    tickers: Optional[str] = Query(default=None),
    mode: str = Query(default="live"),
    top_n: int = Query(default=10, ge=1, le=100),
    limit: int = Query(default=200, ge=1, le=5000),
    offset: int = Query(default=0, ge=0),
    token: Optional[str] = Query(default=None),
    x_app_token: Optional[str] = Header(default=None, alias="X-APP-TOKEN"),
    authorization: Optional[str] = Header(default=None, alias="Authorization"),
    x_request_id: Optional[str] = Header(default=None, alias="X-Request-ID"),
) -> Dict[str, Any]:
    payload = _payload_from_get(
        page=page,
        sheet=None,
        sheet_name=None,
        name=None,
        tab=None,
        symbol=None,
        ticker=None,
        symbols=symbols,
        tickers=tickers,
        mode=mode,
        include_matrix=None,
        schema_only=None,
        headers_only=None,
        top_n=top_n,
        limit=limit,
        offset=offset,
    )
    return await _run_advisor_logic(
        request=request,
        payload=payload,
        token=token,
        x_app_token=x_app_token,
        authorization=authorization,
        x_request_id=x_request_id,
    )


@router.post("/run")
async def advisor_run_post(
    request: Request,
    body: Dict[str, Any] = Body(default_factory=dict),
    token: Optional[str] = Query(default=None),
    x_app_token: Optional[str] = Header(default=None, alias="X-APP-TOKEN"),
    authorization: Optional[str] = Header(default=None, alias="Authorization"),
    x_request_id: Optional[str] = Header(default=None, alias="X-Request-ID"),
) -> Dict[str, Any]:
    return await _run_advisor_logic(
        request=request,
        payload=_safe_dict(body),
        token=token,
        x_app_token=x_app_token,
        authorization=authorization,
        x_request_id=x_request_id,
    )
