#!/usr/bin/env python3
"""
routes/investment_advisor.py
================================================================================
INVESTMENT ADVISOR COMPAT ROUTER — v8.0.0
================================================================================
LONG-FAMILY ONLY • NO SHORT/ADVANCED OVERLAP • SHARED-PIPELINE DELEGATION
TOP10 DEFAULT • INSIGHTS/TOP10 ALIASES • QUERY+BODY COMPATIBLE • JSON-SAFE
AUTH-BRIDGED • ASYNC-SAFE • CANONICAL PAGE NORMALIZATION

Why this revision
-----------------
- Removes ownership of `/v1/advisor/*` from this module.
- Removes ownership of `/v1/advanced/*` from this module.
- Keeps only the long compatibility families:
    - `/v1/investment_advisor/*`
    - `/v1/investment-advisor/*`
- Delegates sheet-row execution to `routes.advanced_analysis` so the long
  families stay aligned with the canonical shared pipeline.
- Keeps legacy compatibility endpoints for:
    - `/sheet-rows`
    - `/sheet_rows`
    - `/run`
    - `/recommendations`
    - `/top10-investments`
    - `/insights-analysis`
- Defaults advisor-style calls to `Top_10_Investments`.
"""

from __future__ import annotations

import inspect
import logging
import math
import uuid
from dataclasses import asdict, is_dataclass
from datetime import date, datetime, time as dt_time, timezone
from decimal import Decimal
from importlib import import_module
from typing import Any, Dict, List, Mapping, Optional

from fastapi import APIRouter, Body, Header, HTTPException, Query, Request
from fastapi.encoders import jsonable_encoder

logger = logging.getLogger("routes.investment_advisor")
logger.addHandler(logging.NullHandler())

ROUTER_VERSION = "8.0.0"
MODULE_NAME = "routes.investment_advisor"

DEFAULT_PAGE = "Top_10_Investments"
INSIGHTS_PAGE = "Insights_Analysis"

router = APIRouter(tags=["investment-advisor-compat"])
_router_long_us = APIRouter(prefix="/v1/investment_advisor", tags=["investment_advisor"])
_router_long_dash = APIRouter(prefix="/v1/investment-advisor", tags=["investment-advisor"])


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


def _json_safe(value: Any) -> Any:
    try:
        return jsonable_encoder(value)
    except Exception:
        pass

    if value is None or isinstance(value, (str, int, bool)):
        return value

    if isinstance(value, float):
        return None if math.isnan(value) or math.isinf(value) else value

    if isinstance(value, Decimal):
        try:
            f = float(value)
            return None if math.isnan(f) or math.isinf(f) else f
        except Exception:
            return str(value)

    if isinstance(value, (datetime, date, dt_time)):
        try:
            return value.isoformat()
        except Exception:
            return str(value)

    if is_dataclass(value):
        try:
            return {k: _json_safe(v) for k, v in asdict(value).items()}
        except Exception:
            return str(value)

    if isinstance(value, Mapping):
        return {str(k): _json_safe(v) for k, v in value.items()}

    if isinstance(value, (list, tuple, set)):
        return [_json_safe(v) for v in value]

    try:
        if hasattr(value, "model_dump") and callable(getattr(value, "model_dump")):
            return _json_safe(value.model_dump(mode="python"))
    except Exception:
        pass

    try:
        if hasattr(value, "dict") and callable(getattr(value, "dict")):
            return _json_safe(value.dict())
    except Exception:
        pass

    try:
        return _json_safe(vars(value))
    except Exception:
        return str(value)


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
                except TypeError:
                    continue
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

    fallback_map = {
        "top_10_investments": "Top_10_Investments",
        "top10_investments": "Top_10_Investments",
        "top10": "Top_10_Investments",
        "advisor": "Top_10_Investments",
        "advanced": "Top_10_Investments",
        "investment_advisor": "Top_10_Investments",
        "insights_analysis": "Insights_Analysis",
        "insights-analysis": "Insights_Analysis",
        "insights": "Insights_Analysis",
        "data_dictionary": "Data_Dictionary",
        "data-dictionary": "Data_Dictionary",
        "market_leaders": "Market_Leaders",
        "global_markets": "Global_Markets",
        "commodities_fx": "Commodities_FX",
        "commodities-fx": "Commodities_FX",
        "mutual_funds": "Mutual_Funds",
        "my_portfolio": "My_Portfolio",
    }
    return fallback_map.get(raw.lower(), raw)


def _extract_page(data: Mapping[str, Any], default_page: str = DEFAULT_PAGE) -> str:
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


def _normalize_payload(data: Mapping[str, Any], *, default_page: str = DEFAULT_PAGE) -> Dict[str, Any]:
    payload = _safe_dict(data)

    page = _extract_page(payload, default_page=default_page)
    payload["page"] = page
    payload["sheet"] = page
    payload["sheet_name"] = page
    payload["name"] = page
    payload["tab"] = page

    payload["limit"] = max(1, min(5000, _safe_int(payload.get("limit"), 200)))
    payload["offset"] = max(0, _safe_int(payload.get("offset"), 0))
    payload["top_n"] = max(1, min(5000, _safe_int(payload.get("top_n"), payload["limit"])))

    mode = _strip(payload.get("mode") or payload.get("advisor_mode") or payload.get("data_mode"))
    payload["mode"] = mode or "live"
    payload["advisor_mode"] = payload["mode"]
    payload["data_mode"] = payload["mode"]

    symbols_out: List[str] = []
    seen = set()
    for key in ("symbols", "tickers", "symbol", "ticker"):
        value = payload.get(key)
        items: List[str] = []
        if isinstance(value, list):
            items = [_strip(x) for x in value if _strip(x)]
        elif isinstance(value, str) and value.strip():
            items = _split_csv(value)
        for item in items:
            u = item.upper()
            if u and u not in seen:
                seen.add(u)
                symbols_out.append(u)

    if symbols_out:
        payload["symbols"] = list(symbols_out)
        payload["tickers"] = list(symbols_out)

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
    limit: Optional[int],
    offset: Optional[int],
    top_n: Optional[int],
    prefer_engine_sheet: Optional[bool],
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
        "limit": limit,
        "offset": offset,
        "top_n": top_n,
        "prefer_engine_sheet": prefer_engine_sheet,
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


async def _get_engine(request: Request) -> Any:
    try:
        state = getattr(request.app, "state", None)
        if state is not None:
            for attr in ("engine", "data_engine", "quote_engine", "cache_engine"):
                value = getattr(state, attr, None)
                if value is not None:
                    return value
    except Exception:
        pass

    for modpath in ("core.data_engine_v2", "core.data_engine"):
        try:
            mod = import_module(modpath)
            fn = getattr(mod, "get_engine", None)
            if callable(fn):
                out = fn()
                if inspect.isawaitable(out):
                    out = await out
                if out is not None:
                    return out
        except Exception:
            continue
    return None


def _safe_engine_type(engine: Any) -> str:
    if engine is None:
        return "none"
    try:
        return f"{engine.__class__.__module__}.{engine.__class__.__name__}"
    except Exception:
        return type(engine).__name__


# -----------------------------------------------------------------------------
# Shared delegation
# -----------------------------------------------------------------------------
async def _delegate_to_advanced_impl(
    *,
    request: Request,
    payload: Dict[str, Any],
    token: Optional[str],
    x_app_token: Optional[str],
    authorization: Optional[str],
    x_request_id: Optional[str],
) -> Dict[str, Any]:
    try:
        mod = import_module("routes.advanced_analysis")
    except Exception as exc:
        raise HTTPException(status_code=503, detail=f"advanced_analysis import failed: {type(exc).__name__}: {exc}") from exc

    impl = getattr(mod, "_run_advanced_sheet_rows_impl", None)
    if not callable(impl):
        raise HTTPException(status_code=503, detail="advanced_analysis shared implementation not available")

    out = impl(
        request=request,
        body=payload,
        mode=_strip(payload.get("mode")) or "",
        include_matrix_q=payload.get("include_matrix"),
        token=token,
        x_app_token=x_app_token,
        authorization=authorization,
        x_request_id=x_request_id,
    )
    if inspect.isawaitable(out):
        out = await out

    if not isinstance(out, Mapping):
        raise HTTPException(status_code=503, detail="advanced_analysis returned unexpected response type")

    result = dict(out)
    meta = _safe_dict(result.get("meta"))
    meta["compat_router"] = MODULE_NAME
    meta["compat_router_version"] = ROUTER_VERSION
    result["meta"] = meta
    return _json_safe(result)


async def _handle_sheet_rows(
    *,
    request: Request,
    raw_payload: Dict[str, Any],
    default_page: str,
    token: Optional[str],
    x_app_token: Optional[str],
    authorization: Optional[str],
    x_request_id: Optional[str],
) -> Dict[str, Any]:
    request_id = _request_id(request, x_request_id)
    payload = _normalize_payload(raw_payload, default_page=default_page)
    result = await _delegate_to_advanced_impl(
        request=request,
        payload=payload,
        token=token,
        x_app_token=x_app_token,
        authorization=authorization,
        x_request_id=request_id,
    )
    result["request_id"] = request_id
    return _json_safe(result)


# -----------------------------------------------------------------------------
# Public endpoints
# -----------------------------------------------------------------------------
async def compat_health(
    request: Request,
    x_request_id: Optional[str] = Header(default=None, alias="X-Request-ID"),
) -> Dict[str, Any]:
    request_id = _request_id(request, x_request_id)
    engine = await _get_engine(request)

    advanced_available = False
    try:
        mod = import_module("routes.advanced_analysis")
        advanced_available = callable(getattr(mod, "_run_advanced_sheet_rows_impl", None))
    except Exception:
        advanced_available = False

    return _json_safe(
        {
            "status": "ok" if advanced_available else "degraded",
            "service": MODULE_NAME,
            "router_version": ROUTER_VERSION,
            "request_id": request_id,
            "timestamp_utc": _now_utc(),
            "compat_only": True,
            "owns_short_family": False,
            "owns_advanced_family": False,
            "long_families": ["/v1/investment_advisor", "/v1/investment-advisor"],
            "shared_impl_source": "routes.advanced_analysis._run_advanced_sheet_rows_impl",
            "shared_impl_available": advanced_available,
            "engine_present": bool(engine),
            "engine_type": _safe_engine_type(engine),
        }
    )


async def compat_meta(
    request: Request,
    x_request_id: Optional[str] = Header(default=None, alias="X-Request-ID"),
) -> Dict[str, Any]:
    request_id = _request_id(request, x_request_id)
    return _json_safe(
        {
            "status": "success",
            "service": MODULE_NAME,
            "router_version": ROUTER_VERSION,
            "request_id": request_id,
            "timestamp_utc": _now_utc(),
            "route_family": "advisor_long_compat",
            "default_page": DEFAULT_PAGE,
            "compat_only": True,
            "owned_prefixes": ["/v1/investment_advisor", "/v1/investment-advisor"],
        }
    )


async def compat_sheet_rows_get(
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
    mode: Optional[str] = Query(default=None),
    include_matrix: Optional[bool] = Query(default=None),
    schema_only: Optional[bool] = Query(default=None),
    headers_only: Optional[bool] = Query(default=None),
    limit: Optional[int] = Query(default=None),
    offset: Optional[int] = Query(default=None),
    top_n: Optional[int] = Query(default=None),
    prefer_engine_sheet: Optional[bool] = Query(default=None),
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
        limit=limit,
        offset=offset,
        top_n=top_n,
        prefer_engine_sheet=prefer_engine_sheet,
    )
    return await _handle_sheet_rows(
        request=request,
        raw_payload=payload,
        default_page=DEFAULT_PAGE,
        token=token,
        x_app_token=x_app_token,
        authorization=authorization,
        x_request_id=x_request_id,
    )


async def compat_sheet_rows_post(
    request: Request,
    body: Dict[str, Any] = Body(default_factory=dict),
    token: Optional[str] = Query(default=None),
    x_app_token: Optional[str] = Header(default=None, alias="X-APP-TOKEN"),
    authorization: Optional[str] = Header(default=None, alias="Authorization"),
    x_request_id: Optional[str] = Header(default=None, alias="X-Request-ID"),
) -> Dict[str, Any]:
    return await _handle_sheet_rows(
        request=request,
        raw_payload=_safe_dict(body),
        default_page=DEFAULT_PAGE,
        token=token,
        x_app_token=x_app_token,
        authorization=authorization,
        x_request_id=x_request_id,
    )


async def compat_recommendations_get(
    request: Request,
    symbol: Optional[str] = Query(default=None),
    ticker: Optional[str] = Query(default=None),
    symbols: Optional[str] = Query(default=None),
    tickers: Optional[str] = Query(default=None),
    page: Optional[str] = Query(default=DEFAULT_PAGE),
    mode: Optional[str] = Query(default="live"),
    include_matrix: Optional[bool] = Query(default=None),
    limit: Optional[int] = Query(default=200),
    offset: Optional[int] = Query(default=0),
    top_n: Optional[int] = Query(default=10),
    prefer_engine_sheet: Optional[bool] = Query(default=None),
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
        limit=limit,
        offset=offset,
        top_n=top_n,
        prefer_engine_sheet=prefer_engine_sheet,
    )
    return await _handle_sheet_rows(
        request=request,
        raw_payload=payload,
        default_page=DEFAULT_PAGE,
        token=token,
        x_app_token=x_app_token,
        authorization=authorization,
        x_request_id=x_request_id,
    )


async def compat_recommendations_post(
    request: Request,
    body: Dict[str, Any] = Body(default_factory=dict),
    token: Optional[str] = Query(default=None),
    x_app_token: Optional[str] = Header(default=None, alias="X-APP-TOKEN"),
    authorization: Optional[str] = Header(default=None, alias="Authorization"),
    x_request_id: Optional[str] = Header(default=None, alias="X-Request-ID"),
) -> Dict[str, Any]:
    payload = _safe_dict(body)
    payload.setdefault("page", DEFAULT_PAGE)
    return await _handle_sheet_rows(
        request=request,
        raw_payload=payload,
        default_page=DEFAULT_PAGE,
        token=token,
        x_app_token=x_app_token,
        authorization=authorization,
        x_request_id=x_request_id,
    )


async def compat_run_get(
    request: Request,
    page: Optional[str] = Query(default=DEFAULT_PAGE),
    symbol: Optional[str] = Query(default=None),
    ticker: Optional[str] = Query(default=None),
    symbols: Optional[str] = Query(default=None),
    tickers: Optional[str] = Query(default=None),
    mode: Optional[str] = Query(default="live"),
    include_matrix: Optional[bool] = Query(default=None),
    limit: Optional[int] = Query(default=200),
    offset: Optional[int] = Query(default=0),
    top_n: Optional[int] = Query(default=10),
    prefer_engine_sheet: Optional[bool] = Query(default=None),
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
        limit=limit,
        offset=offset,
        top_n=top_n,
        prefer_engine_sheet=prefer_engine_sheet,
    )
    return await _handle_sheet_rows(
        request=request,
        raw_payload=payload,
        default_page=DEFAULT_PAGE,
        token=token,
        x_app_token=x_app_token,
        authorization=authorization,
        x_request_id=x_request_id,
    )


async def compat_run_post(
    request: Request,
    body: Dict[str, Any] = Body(default_factory=dict),
    token: Optional[str] = Query(default=None),
    x_app_token: Optional[str] = Header(default=None, alias="X-APP-TOKEN"),
    authorization: Optional[str] = Header(default=None, alias="Authorization"),
    x_request_id: Optional[str] = Header(default=None, alias="X-Request-ID"),
) -> Dict[str, Any]:
    payload = _safe_dict(body)
    payload.setdefault("page", DEFAULT_PAGE)
    return await _handle_sheet_rows(
        request=request,
        raw_payload=payload,
        default_page=DEFAULT_PAGE,
        token=token,
        x_app_token=x_app_token,
        authorization=authorization,
        x_request_id=x_request_id,
    )


async def compat_top10_post(
    request: Request,
    body: Dict[str, Any] = Body(default_factory=dict),
    mode: Optional[str] = Query(default=None),
    include_matrix: Optional[bool] = Query(default=None),
    limit: Optional[int] = Query(default=2000),
    offset: Optional[int] = Query(default=0),
    top_n: Optional[int] = Query(default=10),
    prefer_engine_sheet: Optional[bool] = Query(default=None),
    token: Optional[str] = Query(default=None),
    x_app_token: Optional[str] = Header(default=None, alias="X-APP-TOKEN"),
    authorization: Optional[str] = Header(default=None, alias="Authorization"),
    x_request_id: Optional[str] = Header(default=None, alias="X-Request-ID"),
) -> Dict[str, Any]:
    payload = _safe_dict(body)
    payload["page"] = DEFAULT_PAGE
    payload["mode"] = mode or payload.get("mode") or "live"
    payload["include_matrix"] = include_matrix if include_matrix is not None else payload.get("include_matrix")
    payload["limit"] = limit if limit is not None else payload.get("limit")
    payload["offset"] = offset if offset is not None else payload.get("offset")
    payload["top_n"] = top_n if top_n is not None else payload.get("top_n")
    if prefer_engine_sheet is not None:
        payload["prefer_engine_sheet"] = prefer_engine_sheet
    return await _handle_sheet_rows(
        request=request,
        raw_payload=payload,
        default_page=DEFAULT_PAGE,
        token=token,
        x_app_token=x_app_token,
        authorization=authorization,
        x_request_id=x_request_id,
    )


async def compat_insights_post(
    request: Request,
    body: Dict[str, Any] = Body(default_factory=dict),
    mode: Optional[str] = Query(default=None),
    include_matrix: Optional[bool] = Query(default=None),
    limit: Optional[int] = Query(default=2000),
    offset: Optional[int] = Query(default=0),
    top_n: Optional[int] = Query(default=2000),
    prefer_engine_sheet: Optional[bool] = Query(default=None),
    token: Optional[str] = Query(default=None),
    x_app_token: Optional[str] = Header(default=None, alias="X-APP-TOKEN"),
    authorization: Optional[str] = Header(default=None, alias="Authorization"),
    x_request_id: Optional[str] = Header(default=None, alias="X-Request-ID"),
) -> Dict[str, Any]:
    payload = _safe_dict(body)
    payload["page"] = INSIGHTS_PAGE
    payload["mode"] = mode or payload.get("mode") or "live"
    payload["include_matrix"] = include_matrix if include_matrix is not None else payload.get("include_matrix")
    payload["limit"] = limit if limit is not None else payload.get("limit")
    payload["offset"] = offset if offset is not None else payload.get("offset")
    payload["top_n"] = top_n if top_n is not None else payload.get("top_n")
    if prefer_engine_sheet is not None:
        payload["prefer_engine_sheet"] = prefer_engine_sheet
    return await _handle_sheet_rows(
        request=request,
        raw_payload=payload,
        default_page=INSIGHTS_PAGE,
        token=token,
        x_app_token=x_app_token,
        authorization=authorization,
        x_request_id=x_request_id,
    )


# -----------------------------------------------------------------------------
# Router wiring
# -----------------------------------------------------------------------------
def _wire_router(r: APIRouter) -> None:
    r.add_api_route("/health", compat_health, methods=["GET"])
    r.add_api_route("/meta", compat_meta, methods=["GET"])

    r.add_api_route("/sheet-rows", compat_sheet_rows_get, methods=["GET"])
    r.add_api_route("/sheet_rows", compat_sheet_rows_get, methods=["GET"])
    r.add_api_route("/sheet-rows", compat_sheet_rows_post, methods=["POST"])
    r.add_api_route("/sheet_rows", compat_sheet_rows_post, methods=["POST"])

    r.add_api_route("/recommendations", compat_recommendations_get, methods=["GET"])
    r.add_api_route("/recommendations", compat_recommendations_post, methods=["POST"])

    r.add_api_route("/run", compat_run_get, methods=["GET"])
    r.add_api_route("/run", compat_run_post, methods=["POST"])

    r.add_api_route("/top10-investments", compat_top10_post, methods=["POST"])
    r.add_api_route("/insights-analysis", compat_insights_post, methods=["POST"])


for _r in (_router_long_us, _router_long_dash):
    _wire_router(_r)
    router.include_router(_r)

__all__ = ["router", "ROUTER_VERSION"]
