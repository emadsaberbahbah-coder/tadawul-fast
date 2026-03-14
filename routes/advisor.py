#!/usr/bin/env python3
# routes/advisor.py
"""
================================================================================
ADVISOR ROUTER — v5.6.0
(LIVE-BY-DEFAULT / UNIFIED-SHEET-ROWS / RESOLVER-HARDENED / QUERY-COMPATIBLE)
================================================================================

What this revision fixes
------------------------
- ✅ FIX: short /v1/advisor/* family now resolves advisor runner from:
         - app.state
         - multiple core module candidates
         - direct functions
         - factory-returned service objects
         - engine/service class instances
         - optional shared resolver from routes.investment_advisor
- ✅ FIX: adds support for runner names that were missing before, including:
         - run_investment_advisor_engine
- ✅ FIX: adds /v1/advisor/sheet-rows directly in this router so the short
         advisor family no longer depends on a separate fragile route path.
- ✅ FIX: base source pages (Market_Leaders / Global_Markets / etc.) use
         engine sheet-row execution; derived pages (Top_10_Investments /
         Insights_Analysis / etc.) use advisor execution and projection.
- ✅ FIX: recommendations endpoint accepts modern advisor criteria:
         risk_level, confidence_level, investment_period_days, min_expected_roi,
         limit, horizon_days, min_confidence.
- ✅ FIX: source normalization safely falls back to base live pages when
         derived pages are passed as sources.
- ✅ FIX: advisor runner invocation is more signature-tolerant.
- ✅ FIX: health/meta expose runner discovery details.
- ✅ SAFE: no network calls at import time.
================================================================================
"""

from __future__ import annotations

import asyncio
import importlib
import inspect
import logging
import os
from datetime import datetime, timezone
from typing import Any, Callable, Dict, Iterable, List, Mapping, Optional, Tuple

from fastapi import APIRouter, Body, Header, HTTPException, Query, Request, Response, status

router = APIRouter(prefix="/v1/advisor", tags=["advisor"])

logger = logging.getLogger("routes.advisor")

ADVISOR_ROUTE_VERSION = "5.6.0"

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

_RUNNER_NAME_CANDIDATES = (
    "run_investment_advisor_engine",
    "run_investment_advisor",
    "run_advisor",
    "execute_investment_advisor",
    "execute_advisor",
    "build_investment_advisor",
    "build_advisor",
    "run",
    "execute",
    "recommend",
    "recommend_investments",
    "build_recommendations",
    "get_recommendations",
)

_FACTORY_OR_OBJECT_CANDIDATES = (
    "advisor",
    "investment_advisor",
    "advisor_service",
    "investment_advisor_service",
    "advisor_engine",
    "investment_advisor_engine",
    "runner",
    "advisor_runner",
    "investment_advisor_runner",
    "service",
    "engine",
    "adapter",
    "Advisor",
    "InvestmentAdvisor",
    "AdvisorService",
    "InvestmentAdvisorService",
    "AdvisorEngine",
    "InvestmentAdvisorEngine",
    "AdvisorRunner",
    "InvestmentAdvisorRunner",
    "create_advisor",
    "create_investment_advisor",
    "build_advisor",
    "build_investment_advisor",
    "get_advisor",
    "get_investment_advisor",
    "get_advisor_service",
    "get_investment_advisor_service",
    "create_engine_adapter",
)

_MODULE_CANDIDATES = (
    "core.investment_advisor",
    "core.investment_advisor_engine",
    "core.analysis.investment_advisor",
    "core.analysis.investment_advisor_engine",
    "core.advisor",
    "core.advisor_engine",
)

_SHARED_ROUTE_MODULES = (
    "routes.investment_advisor",
)

_STATE_RUNNER_ATTRS = (
    "investment_advisor_runner",
    "advisor_runner",
    "run_investment_advisor_engine",
    "run_investment_advisor",
    "run_advisor",
    "execute_investment_advisor",
    "execute_advisor",
    "investment_advisor_service",
    "advisor_service",
    "investment_advisor_engine",
    "advisor_engine",
)

_ENGINE_SHEET_ROWS_METHODS = (
    "get_sheet_rows",
    "sheet_rows",
    "get_rows_for_page",
    "build_sheet_rows",
    "get_rows",
)

_SCHEMA_MODULE_CANDIDATES = (
    "core.schema_registry",
    "core.page_catalog",
    "core.schemas",
    "core.schema",
)

_SCHEMA_FN_CANDIDATES = (
    "get_sheet_spec",
    "get_page_spec",
    "get_schema_for_page",
    "build_sheet_spec",
    "sheet_spec",
)

# ---------------------------------------------------------------------------
# Optional Prometheus (safe)
# ---------------------------------------------------------------------------
try:
    from prometheus_client import CONTENT_TYPE_LATEST, generate_latest  # type: ignore

    _PROMETHEUS_AVAILABLE = True
except Exception:
    CONTENT_TYPE_LATEST = "text/plain"
    generate_latest = None  # type: ignore
    _PROMETHEUS_AVAILABLE = False


# ---------------------------------------------------------------------------
# core.config preferred (safe import)
# ---------------------------------------------------------------------------
try:
    from core.config import auth_ok, get_settings_cached, is_open_mode  # type: ignore
except Exception:
    auth_ok = None  # type: ignore
    is_open_mode = None  # type: ignore

    def get_settings_cached(*args: Any, **kwargs: Any) -> Any:  # type: ignore
        return None


# ---------------------------------------------------------------------------
# Small helpers
# ---------------------------------------------------------------------------
def _now_utc_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def _clean_str(v: Any) -> str:
    if v is None:
        return ""
    try:
        s = str(v).strip()
    except Exception:
        return ""
    if not s:
        return ""
    if s.lower() in {"none", "null", "nil"}:
        return ""
    return s


def _safe_bool_env(name: str, default: bool = False) -> bool:
    raw = _clean_str(os.getenv(name, str(default)))
    if not raw:
        return default
    return raw.lower() in {"1", "true", "yes", "y", "on", "t"}


def _safe_engine_type(engine: Any) -> str:
    try:
        return type(engine).__name__
    except Exception:
        return "unknown"


def _get_request_id(request: Optional[Request]) -> str:
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
    return "advisor"


def _list_from_any(v: Any) -> List[str]:
    if v is None:
        return []

    if isinstance(v, list):
        out: List[str] = []
        for x in v:
            sx = _clean_str(x)
            if sx:
                out.append(sx)
        return out

    if isinstance(v, tuple):
        return _list_from_any(list(v))

    if isinstance(v, set):
        return _list_from_any(list(v))

    if isinstance(v, str):
        s = v.replace(",", " ")
        return [x.strip() for x in s.split() if _clean_str(x)]

    sx = _clean_str(v)
    return [sx] if sx else []


def _dedupe_keep_order(values: Iterable[Any]) -> List[str]:
    out: List[str] = []
    seen = set()
    for value in values:
        s = _clean_str(value)
        if not s or s in seen:
            continue
        seen.add(s)
        out.append(s)
    return out


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


def _json_safe(value: Any) -> Any:
    if value is None:
        return None

    if isinstance(value, (bool, int, str)):
        return value

    if isinstance(value, float):
        if value != value or value in (float("inf"), float("-inf")):
            return None
        return value

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
        if hasattr(value, "__dict__"):
            return _json_safe(vars(value))
    except Exception:
        pass

    try:
        return str(value)
    except Exception:
        return None


async def _call_maybe_async(fn: Callable[..., Any], *args: Any, **kwargs: Any) -> Any:
    if inspect.iscoroutinefunction(fn):
        return await fn(*args, **kwargs)

    out = await asyncio.to_thread(fn, *args, **kwargs)
    if inspect.isawaitable(out):
        return await out
    return out


def _route_family_for_page(page: str) -> str:
    p = _clean_str(page)
    if p == "Top_10_Investments":
        return "top10"
    return "advisor"


def _normalize_target_page(
    *,
    page: Any = None,
    sheet: Any = None,
    sheet_name: Any = None,
    default: str = "Market_Leaders",
) -> str:
    return _clean_str(page) or _clean_str(sheet) or _clean_str(sheet_name) or default


# ---------------------------------------------------------------------------
# Auth helpers (signature-safe)
# ---------------------------------------------------------------------------
def _extract_auth_token(
    *,
    token_query: Optional[str],
    x_app_token: Optional[str],
    authorization: Optional[str],
) -> str:
    auth_token = _clean_str(x_app_token)
    authz = _clean_str(authorization)

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
            auth_token = _clean_str(token_query)

    return auth_token


def _is_public_path(path: str) -> bool:
    p = _clean_str(path)
    if not p:
        return False

    if p in {
        "/v1/advisor/health",
        "/v1/advisor/metrics",
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
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid token")


# ---------------------------------------------------------------------------
# Engine accessor (lazy + safe)
# ---------------------------------------------------------------------------
async def _get_engine(request: Request) -> Optional[Any]:
    try:
        st = getattr(request.app, "state", None)
        if st and getattr(st, "engine", None):
            return st.engine
    except Exception:
        pass

    try:
        from core.data_engine_v2 import get_engine  # type: ignore

        eng = get_engine()
        if inspect.isawaitable(eng):
            eng = await eng
        return eng
    except Exception:
        return None


# ---------------------------------------------------------------------------
# Request helpers (key consistency)
# ---------------------------------------------------------------------------
def _normalize_sources(
    *,
    sources_in: Any,
    page: Any = None,
    sheet: Any = None,
    sheet_name: Any = None,
) -> List[str]:
    page_s = _clean_str(page) or _clean_str(sheet) or _clean_str(sheet_name)
    sources = _list_from_any(sources_in)

    if page_s and not sources:
        sources = [page_s]

    if not sources:
        sources = ["ALL"]

    out: List[str] = []
    for s in sources:
        ss = _clean_str(s)
        if not ss:
            continue
        if ss.upper() == "ALL":
            out.extend(_BASE_SOURCE_PAGES)
        else:
            out.append(ss)

    out = _dedupe_keep_order(out)
    out = [s for s in out if s and s not in _DERIVED_OR_NON_SOURCE_PAGES]

    if not out:
        out = list(_BASE_SOURCE_PAGES)

    return out


def _normalize_payload_keys(payload: Dict[str, Any]) -> Dict[str, Any]:
    p = dict(payload or {})

    if "tickers" not in p or not _list_from_any(p.get("tickers")):
        sym_in = p.get("symbols")
        if sym_in is None:
            sym_in = p.get("symbol")
        if sym_in is not None:
            p["tickers"] = _list_from_any(sym_in)

    p["sources"] = _normalize_sources(
        sources_in=p.get("sources"),
        page=p.get("page"),
        sheet=p.get("sheet"),
        sheet_name=p.get("sheet_name"),
    )

    for k in ("page", "sheet", "sheet_name"):
        if k in p and not _clean_str(p.get(k)):
            p.pop(k, None)

    for k in ("mode", "data_mode", "advisor_data_mode"):
        if k in p and not _clean_str(p.get(k)):
            p.pop(k, None)

    if "tickers" in p:
        p["tickers"] = _dedupe_keep_order(_list_from_any(p.get("tickers")))

    risk_level = _clean_str(p.get("risk_level"))
    risk_profile = _clean_str(p.get("risk_profile"))
    if risk_level and not risk_profile:
        p["risk_profile"] = risk_level
    if risk_profile and not risk_level:
        p["risk_level"] = risk_profile

    horizon_days = p.get("horizon_days")
    investment_period_days = p.get("investment_period_days")
    if investment_period_days is not None and horizon_days in (None, "", "None"):
        p["horizon_days"] = investment_period_days
    if horizon_days is not None and investment_period_days in (None, "", "None"):
        p["investment_period_days"] = horizon_days

    min_expected_roi = p.get("min_expected_roi")
    if min_expected_roi is not None and p.get("min_roi") in (None, "", "None"):
        p["min_roi"] = min_expected_roi
    if p.get("min_roi") not in (None, "", "None") and min_expected_roi in (None, "", "None"):
        p["min_expected_roi"] = p.get("min_roi")

    limit = p.get("limit")
    top_n = p.get("top_n")
    if limit is not None and top_n in (None, "", "None"):
        p["top_n"] = limit
    if top_n is not None and limit in (None, "", "None"):
        p["limit"] = top_n

    cleaned: Dict[str, Any] = {}
    for k, v in p.items():
        if v is None:
            continue
        if isinstance(v, str) and not _clean_str(v):
            continue
        cleaned[k] = v

    return cleaned


def _normalize_mode(mode: str) -> str:
    m = _clean_str(mode).lower()
    if not m or m == "auto":
        return ""
    if m in {"snapshot", "snapshots"}:
        return "snapshot"
    if m in {"live", "live_quotes", "quotes"}:
        return "live_quotes"
    if m in {"live_sheet", "sheet"}:
        return "live_sheet"
    return m


def _force_default_live_mode(payload: Dict[str, Any], *, mode_override: str = "") -> Dict[str, Any]:
    p = dict(payload or {})
    m = _normalize_mode(mode_override)

    if m:
        p["advisor_data_mode"] = m
        return p

    existing = _clean_str(p.get("advisor_data_mode") or p.get("data_mode") or p.get("mode"))
    if existing:
        p["advisor_data_mode"] = _normalize_mode(existing) or existing.strip().lower()
        return p

    p["advisor_data_mode"] = (_clean_str(os.getenv("ADVISOR_DATA_MODE")) or "live_quotes").lower()
    return p


# ---------------------------------------------------------------------------
# Advisor runner discovery
# ---------------------------------------------------------------------------
def _module_import_ok(module_name: str) -> Tuple[Optional[Any], Optional[str]]:
    try:
        return importlib.import_module(module_name), None
    except Exception as e:
        return None, f"{module_name}: {type(e).__name__}: {e}"


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
            out = await _call_maybe_async(holder)
            return out
        except TypeError:
            return None
        except Exception:
            return None

    return holder


async def _resolve_runner_from_container(container: Any, label: str) -> Optional[Tuple[Callable[..., Any], str, str]]:
    direct = _callable_by_names(container, _RUNNER_NAME_CANDIDATES)
    if direct:
        fn, fn_name = direct
        return fn, label, fn_name

    for holder_name in _FACTORY_OR_OBJECT_CANDIDATES:
        try:
            holder = getattr(container, holder_name, None)
        except Exception:
            holder = None

        if holder is None:
            continue

        obj = await _materialize_holder(holder)
        if obj is None:
            continue

        if callable(obj) and not inspect.isclass(obj):
            return obj, label, holder_name

        direct_obj = _callable_by_names(obj, _RUNNER_NAME_CANDIDATES)
        if direct_obj:
            fn, fn_name = direct_obj
            return fn, label, f"{holder_name}.{fn_name}"

    return None


async def _resolve_runner_via_shared_route(request: Request) -> Optional[Tuple[Callable[..., Any], str, str]]:
    for mod_name in _SHARED_ROUTE_MODULES:
        mod, _ = _module_import_ok(mod_name)
        if mod is None:
            continue

        resolver = getattr(mod, "_resolve_advisor_runner", None)
        if callable(resolver):
            try:
                out = await _call_maybe_async(resolver, request)
                if isinstance(out, tuple) and len(out) >= 3 and callable(out[0]):
                    return out[0], str(out[1]), str(out[2])
            except Exception:
                continue
    return None


async def _resolve_advisor_runner(request: Request) -> Tuple[Callable[..., Any], str, str]:
    searched_labels: List[str] = []
    import_errors: List[str] = []

    # 1) app.state first
    try:
        st = getattr(request.app, "state", None)
    except Exception:
        st = None

    if st is not None:
        searched_labels.append("app.state")
        direct_state = _callable_by_names(st, _STATE_RUNNER_ATTRS)
        if direct_state:
            fn, fn_name = direct_state
            return fn, "app.state", fn_name

        state_obj_resolved = await _resolve_runner_from_container(st, "app.state")
        if state_obj_resolved:
            return state_obj_resolved

    # 2) module candidates
    for mod_name in _MODULE_CANDIDATES:
        mod, err = _module_import_ok(mod_name)
        searched_labels.append(mod_name)
        if mod is None:
            if err:
                import_errors.append(err)
            continue

        resolved = await _resolve_runner_from_container(mod, mod_name)
        if resolved:
            return resolved

    # 3) shared route fallback if present
    shared = await _resolve_runner_via_shared_route(request)
    if shared:
        return shared

    logger.error(
        "Advisor runner resolution failed. searched=%s import_errors=%s",
        searched_labels,
        import_errors,
    )
    raise HTTPException(
        status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
        detail="Advisor engine runner not found",
    )


# ---------------------------------------------------------------------------
# Advisor invocation
# ---------------------------------------------------------------------------
async def _warm_adapter_if_possible(adapter: Any, sources: List[str]) -> Any:
    if adapter is None:
        return None

    for fn_name in ("warm_cache", "warm_snapshots", "preload_snapshots", "build_snapshot_cache"):
        fn = getattr(adapter, fn_name, None)
        if not callable(fn):
            continue
        try:
            return await _call_maybe_async(fn, list(sources or []))
        except TypeError:
            try:
                return await _call_maybe_async(fn)
            except Exception:
                continue
        except Exception:
            continue

    return None


async def _call_advisor_runner(
    runner: Callable[..., Any],
    *,
    payload: Dict[str, Any],
    engine: Any,
    settings: Any,
    cache_strategy: str,
    cache_ttl: int,
    debug: bool,
    request: Request,
) -> Dict[str, Any]:
    call_attempts = [
        {
            "payload": payload,
            "engine": engine,
            "settings": settings,
            "cache_strategy": cache_strategy,
            "cache_ttl": int(cache_ttl),
            "debug": bool(debug),
            "request": request,
        },
        {
            "payload": payload,
            "engine": engine,
            "settings": settings,
            "debug": bool(debug),
            "request": request,
        },
        {
            "payload": payload,
            "engine": engine,
            "cache_strategy": cache_strategy,
            "cache_ttl": int(cache_ttl),
            "debug": bool(debug),
        },
        {
            "body": payload,
            "engine": engine,
            "settings": settings,
            "cache_strategy": cache_strategy,
            "cache_ttl": int(cache_ttl),
            "debug": bool(debug),
            "request": request,
        },
        {
            "body": payload,
            "engine": engine,
            "settings": settings,
            "debug": bool(debug),
        },
        {
            "request_data": payload,
            "engine": engine,
            "settings": settings,
            "cache_strategy": cache_strategy,
            "cache_ttl": int(cache_ttl),
            "debug": bool(debug),
        },
        {
            "request_data": payload,
            "engine": engine,
            "settings": settings,
        },
        {
            "criteria": payload,
            "engine": engine,
            "settings": settings,
            "debug": bool(debug),
        },
        {
            "params": payload,
            "engine": engine,
            "settings": settings,
            "debug": bool(debug),
        },
        {
            "payload": payload,
        },
        {
            "body": payload,
        },
        {
            "request_data": payload,
        },
        {
            "criteria": payload,
        },
        {
            "params": payload,
        },
    ]

    out: Any = None
    for kwargs in call_attempts:
        try:
            out = await _call_maybe_async(runner, **kwargs)
            break
        except TypeError:
            continue
    else:
        positional_attempts = [
            (payload,),
            (payload, engine),
            (payload, engine, settings),
            (payload, settings, engine),
        ]
        last_error: Optional[Exception] = None
        for args in positional_attempts:
            try:
                out = await _call_maybe_async(runner, *args)
                last_error = None
                break
            except TypeError as e:
                last_error = e
                continue
        if out is None and last_error is not None:
            raise HTTPException(
                status_code=500,
                detail=f"Advisor runner signature mismatch: {type(last_error).__name__}: {last_error}",
            )

    if isinstance(out, dict):
        return dict(out)

    if isinstance(out, list):
        return {
            "status": "success",
            "recommendations": out,
            "count": len(out),
        }

    model_dict = _model_to_dict(out)
    if model_dict:
        return model_dict

    raise HTTPException(status_code=500, detail="Advisor engine returned invalid response")


# ---------------------------------------------------------------------------
# Schema + projection helpers
# ---------------------------------------------------------------------------
def _extract_headers_from_spec(spec: Any) -> List[str]:
    if spec is None:
        return []

    if isinstance(spec, dict):
        for key in ("headers", "display_headers", "keys", "columns"):
            value = spec.get(key)
            if isinstance(value, list):
                if value and isinstance(value[0], dict):
                    out: List[str] = []
                    for col in value:
                        if not isinstance(col, dict):
                            continue
                        name = _clean_str(
                            col.get("name")
                            or col.get("field")
                            or col.get("key")
                            or col.get("id")
                            or col.get("header")
                            or col.get("title")
                        )
                        if name:
                            out.append(name)
                    if out:
                        return out
                else:
                    out = [_clean_str(x) for x in value if _clean_str(x)]
                    if out:
                        return out

    return []


async def _get_schema_headers(page: str) -> List[str]:
    for mod_name in _SCHEMA_MODULE_CANDIDATES:
        mod, _ = _module_import_ok(mod_name)
        if mod is None:
            continue

        for fn_name in _SCHEMA_FN_CANDIDATES:
            fn = getattr(mod, fn_name, None)
            if not callable(fn):
                continue

            attempts = [
                {"sheet": page},
                {"page": page},
                {"sheet_name": page},
                {"name": page},
            ]
            for kwargs in attempts:
                try:
                    spec = await _call_maybe_async(fn, **kwargs)
                    headers = _extract_headers_from_spec(spec)
                    if headers:
                        return headers
                except TypeError:
                    continue
                except Exception:
                    continue

    return []


def _rows_from_any(obj: Any) -> List[Any]:
    if obj is None:
        return []

    if isinstance(obj, dict):
        for key in ("rows", "recommendations", "data", "items", "results", "records", "quotes"):
            value = obj.get(key)
            if value is None:
                continue
            if isinstance(value, list):
                return value
            return [value]

    if isinstance(obj, list):
        return obj

    return [obj]


def _headers_from_any(obj: Any) -> List[str]:
    if isinstance(obj, dict):
        for key in ("headers", "display_headers", "keys", "columns"):
            value = obj.get(key)
            if isinstance(value, list):
                return [_clean_str(x) for x in value if _clean_str(x)]
    return []


def _project_rows_to_headers(items: List[Any], headers: List[str]) -> List[List[Any]]:
    projected: List[List[Any]] = []
    if not headers:
        return projected

    for item in items:
        if isinstance(item, dict):
            projected.append([_json_safe(item.get(h)) for h in headers])
            continue

        if isinstance(item, (list, tuple)):
            row = list(item)
            if len(row) < len(headers):
                row.extend([None] * (len(headers) - len(row)))
            projected.append([_json_safe(x) for x in row[: len(headers)]])
            continue

        projected.append([_json_safe(item)] + [None] * (len(headers) - 1))

    return projected


def _derive_headers_from_items(items: List[Any]) -> List[str]:
    seen: List[str] = []
    seen_set = set()

    for item in items:
        if not isinstance(item, dict):
            continue
        for key in item.keys():
            k = _clean_str(key)
            if not k or k in seen_set:
                continue
            seen_set.add(k)
            seen.append(k)

    return seen


async def _build_sheet_rows_from_advisor_result(
    *,
    page: str,
    advisor_result: Dict[str, Any],
    route_family: str,
) -> Dict[str, Any]:
    headers = _headers_from_any(advisor_result)
    items = _rows_from_any(advisor_result)

    if not headers:
        headers = await _get_schema_headers(page)

    if not headers and items:
        headers = _derive_headers_from_items(items)

    rows = _project_rows_to_headers(items, headers) if headers else []

    meta = advisor_result.get("meta") if isinstance(advisor_result.get("meta"), dict) else {}
    meta = dict(meta)
    meta.setdefault("route_version", ADVISOR_ROUTE_VERSION)
    meta.setdefault("projection", "advisor_result_to_sheet_rows")

    status_value = _clean_str(advisor_result.get("status")) or ("success" if rows else "partial")
    detail_value = _clean_str(
        advisor_result.get("detail") or advisor_result.get("error") or advisor_result.get("message")
    )

    return _json_safe(
        {
            "status": status_value,
            "page": page,
            "sheet": page,
            "route_family": route_family,
            "headers": headers,
            "display_headers": headers,
            "keys": headers,
            "rows": rows,
            "count": len(rows),
            "detail": detail_value,
            "meta": meta,
        }
    )


# ---------------------------------------------------------------------------
# Engine sheet-rows execution
# ---------------------------------------------------------------------------
async def _call_engine_sheet_rows(
    *,
    engine: Any,
    page: str,
    limit: int,
    mode: str,
    body: Dict[str, Any],
) -> Dict[str, Any]:
    if engine is None:
        raise HTTPException(status_code=503, detail="Data engine unavailable")

    fn = None
    fn_name = ""
    for name in _ENGINE_SHEET_ROWS_METHODS:
        candidate = getattr(engine, name, None)
        if callable(candidate):
            fn = candidate
            fn_name = name
            break

    if fn is None:
        raise HTTPException(status_code=503, detail="Engine sheet-rows handler not found")

    attempts = [
        {
            "page": page,
            "limit": limit,
            "format": "rows",
            "mode": mode,
            "body": body,
        },
        {
            "sheet": page,
            "limit": limit,
            "format": "rows",
            "mode": mode,
            "body": body,
        },
        {
            "sheet_name": page,
            "limit": limit,
            "format": "rows",
            "mode": mode,
            "body": body,
        },
        {
            "page": page,
            "limit": limit,
            "mode": mode,
        },
        {
            "sheet": page,
            "limit": limit,
            "mode": mode,
        },
        {
            "sheet_name": page,
            "limit": limit,
            "mode": mode,
        },
        {
            "page": page,
            "limit": limit,
            "format": "rows",
        },
        {
            "sheet": page,
            "limit": limit,
            "format": "rows",
        },
        {
            "sheet_name": page,
            "limit": limit,
            "format": "rows",
        },
        {
            "page": page,
            "limit": limit,
        },
        {
            "sheet": page,
            "limit": limit,
        },
        {
            "sheet_name": page,
            "limit": limit,
        },
    ]

    out: Any = None
    for kwargs in attempts:
        try:
            out = await _call_maybe_async(fn, **kwargs)
            break
        except TypeError:
            continue

    if out is None:
        try:
            out = await _call_maybe_async(fn, page, limit)
        except TypeError:
            try:
                out = await _call_maybe_async(fn, page)
            except Exception as e:
                raise HTTPException(
                    status_code=500,
                    detail=f"Engine sheet-rows call failed: {type(e).__name__}: {e}",
                )

    if isinstance(out, dict):
        result = dict(out)
    elif isinstance(out, list):
        headers = await _get_schema_headers(page)
        rows = _project_rows_to_headers(out, headers) if headers else out
        result = {
            "status": "success" if out else "partial",
            "page": page,
            "sheet": page,
            "headers": headers,
            "display_headers": headers,
            "keys": headers,
            "rows": rows,
            "count": len(rows) if isinstance(rows, list) else 0,
        }
    else:
        result = _model_to_dict(out)

    meta = result.get("meta") if isinstance(result.get("meta"), dict) else {}
    meta = dict(meta)
    meta.setdefault("route_version", ADVISOR_ROUTE_VERSION)
    meta.setdefault("engine_method", fn_name)
    result["meta"] = meta

    result.setdefault("page", page)
    result.setdefault("sheet", page)
    result.setdefault("route_family", "advisor")

    return _json_safe(result)


# ---------------------------------------------------------------------------
# Core runner
# ---------------------------------------------------------------------------
async def _run_advisor(
    *,
    request: Request,
    payload: Dict[str, Any],
    mode: str,
    warm_snapshots: bool,
    cache_strategy: str,
    cache_ttl: int,
    debug: bool,
) -> Dict[str, Any]:
    request_id = _get_request_id(request)

    engine = await _get_engine(request)
    if engine is None:
        raise HTTPException(status_code=status.HTTP_503_SERVICE_UNAVAILABLE, detail="Data engine unavailable")

    payload0 = _normalize_payload_keys(payload or {})
    payload2 = _force_default_live_mode(payload0, mode_override=mode)

    advisor_mode = _clean_str(payload2.get("advisor_data_mode")).lower()
    wants_snapshot = advisor_mode == "snapshot"
    use_warm = bool(warm_snapshots or wants_snapshot)

    settings = None
    try:
        settings = get_settings_cached()
    except Exception:
        settings = None

    runner, runner_source, runner_name = await _resolve_advisor_runner(request)

    eng_for_advisor: Any = engine
    warmed: Any = None
    adapter_used = False

    if use_warm:
        adapter_factory = None

        for mod_name in _MODULE_CANDIDATES:
            mod, _ = _module_import_ok(mod_name)
            if mod is None:
                continue
            candidate = getattr(mod, "create_engine_adapter", None)
            if callable(candidate):
                adapter_factory = candidate
                break

        if callable(adapter_factory):
            try:
                adapter = await _call_maybe_async(
                    adapter_factory,
                    engine,
                    cache_strategy=_clean_str(cache_strategy).lower() or "memory",
                    cache_ttl=int(cache_ttl),
                )
                if adapter is not None:
                    eng_for_advisor = adapter
                    adapter_used = True
                    warmed = await _warm_adapter_if_possible(adapter, list(payload2.get("sources") or []))
            except TypeError:
                try:
                    adapter = await _call_maybe_async(adapter_factory, engine)
                    if adapter is not None:
                        eng_for_advisor = adapter
                        adapter_used = True
                        warmed = await _warm_adapter_if_possible(adapter, list(payload2.get("sources") or []))
                except Exception:
                    adapter_used = False
                    eng_for_advisor = engine
                    warmed = None
            except Exception:
                adapter_used = False
                eng_for_advisor = engine
                warmed = None

        if wants_snapshot and not adapter_used:
            payload2["advisor_data_mode"] = "live_quotes"

    result = await _call_advisor_runner(
        runner,
        payload=payload2,
        engine=eng_for_advisor,
        settings=settings,
        cache_strategy=_clean_str(cache_strategy).lower() or "memory",
        cache_ttl=int(cache_ttl),
        debug=bool(debug),
        request=request,
    )

    meta = result.get("meta") if isinstance(result.get("meta"), dict) else {}
    meta.update(
        {
            "route_version": ADVISOR_ROUTE_VERSION,
            "request_id": request_id,
            "generated_at_utc": _now_utc_iso(),
            "engine_type": _safe_engine_type(engine),
            "advisor_data_mode_effective": _clean_str(payload2.get("advisor_data_mode")),
            "warm_snapshots": bool(use_warm),
            "adapter_used": bool(adapter_used),
            "warm_results": warmed,
            "cache_strategy": _clean_str(cache_strategy).lower() or "memory",
            "cache_ttl": int(cache_ttl),
            "runner_source": runner_source,
            "runner_name": runner_name,
        }
    )

    if "status" not in result:
        result["status"] = "success"

    result["meta"] = meta
    return _json_safe(result)


# ---------------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------------
@router.get("/health")
async def advisor_health(request: Request) -> Dict[str, Any]:
    engine = await _get_engine(request)

    engine_health: Optional[Dict[str, Any]] = None
    if engine is not None:
        for attr in ("health", "health_check", "get_health"):
            try:
                fn = getattr(engine, attr, None)
                if callable(fn):
                    out = fn()
                    if inspect.isawaitable(out):
                        out = await out
                    if isinstance(out, dict):
                        engine_health = out
                        break
            except Exception:
                continue

    runner_available = False
    runner_source = ""
    runner_name = ""
    try:
        _, runner_source, runner_name = await _resolve_advisor_runner(request)
        runner_available = True
    except Exception:
        runner_available = False

    return {
        "status": "ok" if engine else "degraded",
        "version": ADVISOR_ROUTE_VERSION,
        "engine_available": bool(engine),
        "engine_type": _safe_engine_type(engine) if engine else "none",
        "engine_health": engine_health,
        "runner_available": runner_available,
        "runner_source": runner_source,
        "runner_name": runner_name,
        "default_mode": (_clean_str(os.getenv("ADVISOR_DATA_MODE")) or "live_quotes").lower(),
        "require_auth": _safe_bool_env("REQUIRE_AUTH", True),
        "prometheus_available": bool(_PROMETHEUS_AVAILABLE),
        "sheet_rows_supported": True,
    }


@router.get("/metrics")
async def advisor_metrics() -> Response:
    if not _PROMETHEUS_AVAILABLE or generate_latest is None:
        return Response(content="Metrics not available", media_type="text/plain", status_code=503)
    return Response(content=generate_latest(), media_type=CONTENT_TYPE_LATEST)


@router.post("/run")
async def advisor_run(
    request: Request,
    body: Dict[str, Any] = Body(default_factory=dict),
    page: Optional[str] = Query(default=None, description="Single page alias (same as sheet_name)"),
    sheet_name: Optional[str] = Query(default=None, description="Single page alias"),
    mode: str = Query(default="", description="snapshot | live_sheet | live_quotes | auto"),
    warm_snapshots: bool = Query(default=False, description="Warm snapshots before running"),
    cache_strategy: str = Query(default="memory", description="memory | none"),
    cache_ttl: int = Query(default=600, ge=30, le=86400, description="Snapshot cache TTL"),
    debug: bool = Query(default=False, description="Include debug metadata where available"),
    token: Optional[str] = Query(default=None, description="Auth token (query only if allowed)"),
    x_app_token: Optional[str] = Header(default=None, alias="X-APP-TOKEN"),
    authorization: Optional[str] = Header(default=None, alias="Authorization"),
) -> Dict[str, Any]:
    _require_auth_or_401(
        request=request,
        token_query=token,
        x_app_token=x_app_token,
        authorization=authorization,
    )

    payload = dict(body or {})

    if _clean_str(page) and not _clean_str(payload.get("page")) and not _clean_str(payload.get("sources")):
        payload["page"] = _clean_str(page)

    if _clean_str(sheet_name) and not _clean_str(payload.get("sheet_name")) and not _clean_str(payload.get("sources")):
        payload["sheet_name"] = _clean_str(sheet_name)

    if "tickers" not in payload and "symbols" in payload:
        payload["tickers"] = payload.get("symbols")

    return await _run_advisor(
        request=request,
        payload=payload,
        mode=mode,
        warm_snapshots=bool(warm_snapshots),
        cache_strategy=_clean_str(cache_strategy).lower() or "memory",
        cache_ttl=int(cache_ttl),
        debug=bool(debug),
    )


@router.get("/recommendations")
async def advisor_recommendations(
    request: Request,
    symbols: Optional[str] = Query(default=None, description="Comma/space separated symbols"),
    sources: Optional[str] = Query(default="ALL", description="ALL or comma-separated pages"),
    page: Optional[str] = Query(default=None, description="Single page alias"),
    sheet_name: Optional[str] = Query(default=None, description="Single page alias"),
    top_n: int = Query(default=20, ge=1, le=200),
    limit: Optional[int] = Query(default=None, ge=1, le=200),
    invest_amount: float = Query(default=0.0, ge=0.0),
    allocation_strategy: str = Query(default="maximum_sharpe"),
    risk_profile: str = Query(default="moderate"),
    risk_level: Optional[str] = Query(default=None),
    confidence_level: Optional[str] = Query(default=None),
    investment_period_days: Optional[int] = Query(default=None, ge=1, le=3650),
    horizon_days: Optional[int] = Query(default=None, ge=1, le=3650),
    min_expected_roi: Optional[float] = Query(default=None),
    min_confidence: Optional[float] = Query(default=None),
    mode: str = Query(default="", description="snapshot | live_sheet | live_quotes | auto"),
    warm_snapshots: bool = Query(default=False),
    cache_strategy: str = Query(default="memory"),
    cache_ttl: int = Query(default=600, ge=30, le=86400),
    debug: bool = Query(default=False),
    token: Optional[str] = Query(default=None, description="Auth token (query only if allowed)"),
    x_app_token: Optional[str] = Header(default=None, alias="X-APP-TOKEN"),
    authorization: Optional[str] = Header(default=None, alias="Authorization"),
) -> Dict[str, Any]:
    _require_auth_or_401(
        request=request,
        token_query=token,
        x_app_token=x_app_token,
        authorization=authorization,
    )

    effective_limit = int(limit) if limit is not None else int(top_n)
    effective_risk_level = _clean_str(risk_level) or _clean_str(risk_profile) or "moderate"

    payload: Dict[str, Any] = {
        "sources": _list_from_any(sources),
        "page": _clean_str(page),
        "sheet_name": _clean_str(sheet_name),
        "tickers": _list_from_any(symbols),
        "top_n": effective_limit,
        "limit": effective_limit,
        "invest_amount": float(invest_amount),
        "allocation_strategy": _clean_str(allocation_strategy).lower() or "maximum_sharpe",
        "risk_profile": effective_risk_level,
        "risk_level": effective_risk_level,
        "confidence_level": _clean_str(confidence_level),
        "investment_period_days": int(investment_period_days) if investment_period_days is not None else None,
        "horizon_days": int(horizon_days) if horizon_days is not None else (
            int(investment_period_days) if investment_period_days is not None else None
        ),
        "min_expected_roi": min_expected_roi,
        "min_roi": min_expected_roi,
        "min_confidence": min_confidence,
        "debug": bool(debug),
    }

    return await _run_advisor(
        request=request,
        payload=payload,
        mode=mode,
        warm_snapshots=bool(warm_snapshots),
        cache_strategy=_clean_str(cache_strategy).lower() or "memory",
        cache_ttl=int(cache_ttl),
        debug=bool(debug),
    )


@router.get("/sheet-rows")
async def advisor_sheet_rows(
    request: Request,
    page: Optional[str] = Query(default=None, description="Sheet/page name"),
    sheet: Optional[str] = Query(default=None, description="Sheet/page name alias"),
    sheet_name: Optional[str] = Query(default=None, description="Sheet/page name alias"),
    symbols: Optional[str] = Query(default=None, description="Comma/space separated symbols"),
    sources: Optional[str] = Query(default="ALL", description="ALL or comma-separated pages"),
    limit: int = Query(default=20, ge=1, le=500),
    top_n: Optional[int] = Query(default=None, ge=1, le=500),
    invest_amount: float = Query(default=0.0, ge=0.0),
    allocation_strategy: str = Query(default="maximum_sharpe"),
    risk_profile: str = Query(default="moderate"),
    risk_level: Optional[str] = Query(default=None),
    confidence_level: Optional[str] = Query(default=None),
    investment_period_days: Optional[int] = Query(default=None, ge=1, le=3650),
    horizon_days: Optional[int] = Query(default=None, ge=1, le=3650),
    min_expected_roi: Optional[float] = Query(default=None),
    min_confidence: Optional[float] = Query(default=None),
    mode: str = Query(default="", description="snapshot | live_sheet | live_quotes | auto"),
    warm_snapshots: bool = Query(default=False),
    cache_strategy: str = Query(default="memory"),
    cache_ttl: int = Query(default=600, ge=30, le=86400),
    debug: bool = Query(default=False),
    token: Optional[str] = Query(default=None, description="Auth token (query only if allowed)"),
    x_app_token: Optional[str] = Header(default=None, alias="X-APP-TOKEN"),
    authorization: Optional[str] = Header(default=None, alias="Authorization"),
) -> Dict[str, Any]:
    _require_auth_or_401(
        request=request,
        token_query=token,
        x_app_token=x_app_token,
        authorization=authorization,
    )

    target_page = _normalize_target_page(page=page, sheet=sheet, sheet_name=sheet_name)
    effective_limit = int(top_n) if top_n is not None else int(limit)

    payload: Dict[str, Any] = {
        "page": target_page,
        "sheet": target_page,
        "sheet_name": target_page,
        "sources": _list_from_any(sources),
        "tickers": _list_from_any(symbols),
        "limit": effective_limit,
        "top_n": effective_limit,
        "invest_amount": float(invest_amount),
        "allocation_strategy": _clean_str(allocation_strategy).lower() or "maximum_sharpe",
        "risk_profile": _clean_str(risk_level) or _clean_str(risk_profile) or "moderate",
        "risk_level": _clean_str(risk_level) or _clean_str(risk_profile) or "moderate",
        "confidence_level": _clean_str(confidence_level),
        "investment_period_days": int(investment_period_days) if investment_period_days is not None else None,
        "horizon_days": int(horizon_days) if horizon_days is not None else (
            int(investment_period_days) if investment_period_days is not None else None
        ),
        "min_expected_roi": min_expected_roi,
        "min_roi": min_expected_roi,
        "min_confidence": min_confidence,
        "debug": bool(debug),
    }

    # Base source pages -> use engine sheet-rows directly.
    if target_page in _BASE_SOURCE_PAGES:
        engine = await _get_engine(request)
        result = await _call_engine_sheet_rows(
            engine=engine,
            page=target_page,
            limit=effective_limit,
            mode=_normalize_mode(mode) or "live_quotes",
            body=payload,
        )
        result["route_family"] = "advisor"
        return result

    # Derived pages -> use advisor execution then project to sheet rows.
    advisor_result = await _run_advisor(
        request=request,
        payload=payload,
        mode=mode,
        warm_snapshots=bool(warm_snapshots),
        cache_strategy=_clean_str(cache_strategy).lower() or "memory",
        cache_ttl=int(cache_ttl),
        debug=bool(debug),
    )

    return await _build_sheet_rows_from_advisor_result(
        page=target_page,
        advisor_result=advisor_result,
        route_family=_route_family_for_page(target_page),
    )


__all__ = ["router"]
