#!/usr/bin/env python3
# routes/advisor.py
"""
================================================================================
ADVISOR ROUTER — v6.2.0
(ENGINE-NATIVE-SHEET-ROWS / UNIFIED-RUNNER / STATUS-PRESERVING /
 SHARED-PIPELINE / AUTH-TOLERANT / SCHEMA-KEY-CORRECT /
 ROW-OBJECTS + MATRIX / HEADER-ALIAS WIRING / SPECIAL-PAGE SAFE)
================================================================================

What this revision fixes
------------------------
- ✅ FIX: /v1/advisor/sheet-rows now returns BOTH:
         - rows / rows_matrix (matrix aligned to headers/keys)
         - row_objects / items / records (dict rows aligned to keys)
- ✅ FIX: dict-row projection now maps values by BOTH canonical key and display
         header aliases, preventing all-null rows when upstream uses display
         headers instead of schema keys.
- ✅ FIX: native sheet payload normalization preserves compatible contract while
         supplementing row_objects for easier testing/debugging.
- ✅ FIX: advisor/shared/native fallbacks all converge to one stable sheet-rows
         contract with correct key/header alignment.
- ✅ FIX: Top_10_Investments context fields remain guaranteed in matrix and dict
         forms.
- ✅ SAFE: backward compatibility preserved for existing matrix consumers.
================================================================================
"""

from __future__ import annotations

import asyncio
import importlib
import inspect
import logging
import math
import os
from datetime import date, datetime, time as dt_time, timezone
from decimal import Decimal
from typing import Any, Callable, Dict, Iterable, List, Mapping, Optional, Sequence, Tuple

from fastapi import APIRouter, Body, Header, HTTPException, Query, Request, Response, status

router = APIRouter(prefix="/v1/advisor", tags=["advisor"])

logger = logging.getLogger("routes.advisor")
logger.addHandler(logging.NullHandler())

ADVISOR_ROUTE_VERSION = "6.2.0"
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

_TOP10_REQUIRED_FIELDS = ("top10_rank", "selection_reason", "criteria_snapshot")

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
    if s.lower() in {"none", "null", "nil", "undefined"}:
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
        s = v.replace(";", ",").replace("\n", ",")
        out: List[str] = []
        for part in s.split(","):
            for token in part.split():
                st = _clean_str(token)
                if st:
                    out.append(st)
        return out

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


def _safe_int(v: Any, default: int) -> int:
    try:
        if v is None or isinstance(v, bool):
            return default
        return int(float(v))
    except Exception:
        return default


def _safe_float(v: Any, default: Optional[float] = None) -> Optional[float]:
    try:
        if v is None or isinstance(v, bool):
            return default
        f = float(v)
        if math.isnan(f) or math.isinf(f):
            return default
        return f
    except Exception:
        return default


def _as_ratio(v: Any) -> Optional[float]:
    f = _safe_float(v, None)
    if f is None:
        return None
    if abs(f) > 1.5:
        return f / 100.0
    return f


def _snake_like(header: str) -> str:
    s = str(header or "").strip().replace("%", " pct").replace("/", " ")
    out = []
    prev_us = False
    for ch in s:
        if ch.isalnum():
            out.append(ch.lower())
            prev_us = False
        else:
            if not prev_us:
                out.append("_")
                prev_us = True
    res = "".join(out).strip("_")
    while "__" in res:
        res = res.replace("__", "_")
    return res


def _route_family_for_page(page: str) -> str:
    p = _clean_str(page)
    if p == TOP10_PAGE_NAME:
        return "top10"
    if p == "Insights_Analysis":
        return "insights"
    if p == "Data_Dictionary":
        return "schema"
    if p in _BASE_SOURCE_PAGES:
        return "sheet"
    return "advisor"


def _normalize_symbol(sym: Any) -> str:
    s = _clean_str(sym).upper().replace(" ", "")
    if not s:
        return ""
    if s.startswith("TADAWUL:"):
        s = s.split(":", 1)[1]
    if s.endswith(".SA"):
        s = s[:-3] + ".SR"
    if s.isdigit():
        return f"{s}.SR"
    return s


def _normalize_page_name(raw: Any) -> str:
    s = _clean_str(raw)
    if not s:
        return s

    try:
        from core.sheets.page_catalog import normalize_page_name  # type: ignore

        out = normalize_page_name(s, allow_output_pages=True)
        if out:
            return str(out)
    except Exception:
        pass

    compact = s.replace("-", "_").replace(" ", "_").lower()
    mapping = {
        "top_10_investments": "Top_10_Investments",
        "top10_investments": "Top_10_Investments",
        "top10": "Top_10_Investments",
        "insights_analysis": "Insights_Analysis",
        "insights": "Insights_Analysis",
        "data_dictionary": "Data_Dictionary",
        "market_leaders": "Market_Leaders",
        "global_markets": "Global_Markets",
        "mutual_funds": "Mutual_Funds",
        "commodities_fx": "Commodities_FX",
        "my_portfolio": "My_Portfolio",
    }
    return mapping.get(compact, s)


def _normalize_target_page(
    *,
    page: Any = None,
    sheet: Any = None,
    sheet_name: Any = None,
    default: str = "Market_Leaders",
) -> str:
    target = _clean_str(page) or _clean_str(sheet) or _clean_str(sheet_name) or default
    return _normalize_page_name(target)


def _complete_headers_keys(headers: Sequence[Any], keys: Sequence[Any]) -> Tuple[List[str], List[str]]:
    hdrs = [_clean_str(h) for h in (headers or []) if _clean_str(h)]
    ks = [_clean_str(k) for k in (keys or []) if _clean_str(k)]

    if not ks and hdrs:
        ks = [_snake_like(h) for h in hdrs if _snake_like(h)]
    if not hdrs and ks:
        hdrs = [k.replace("_", " ").title() for k in ks]

    n = max(len(hdrs), len(ks))
    out_h: List[str] = []
    out_k: List[str] = []

    for i in range(n):
        h = hdrs[i] if i < len(hdrs) else ""
        k = ks[i] if i < len(ks) else ""
        if h and not k:
            k = _snake_like(h)
        elif k and not h:
            h = k.replace("_", " ").title()
        elif not h and not k:
            h = f"Column {i + 1}"
            k = f"column_{i + 1}"
        out_h.append(h)
        out_k.append(k)

    return out_h, out_k


def _ensure_top10_fields(headers: Sequence[Any], keys: Sequence[Any]) -> Tuple[List[str], List[str]]:
    hdrs, ks = _complete_headers_keys(headers, keys)
    extras = {
        "top10_rank": "Top10 Rank",
        "selection_reason": "Selection Reason",
        "criteria_snapshot": "Criteria Snapshot",
    }
    for k, h in extras.items():
        if k not in ks:
            ks.append(k)
            hdrs.append(h)
    return hdrs, ks


def _json_safe(value: Any) -> Any:
    if value is None:
        return None

    if isinstance(value, (bool, int, str)):
        return value

    if isinstance(value, float):
        if value != value or value in (float("inf"), float("-inf")):
            return None
        return value

    if isinstance(value, Decimal):
        try:
            f = float(value)
            if f != f or f in (float("inf"), float("-inf")):
                return None
            return f
        except Exception:
            return str(value)

    if isinstance(value, (datetime, date, dt_time)):
        try:
            return value.isoformat()
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
        if hasattr(value, "__dict__"):
            return _json_safe(vars(value))
    except Exception:
        pass

    try:
        return str(value)
    except Exception:
        return None


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


async def _call_maybe_async(fn: Callable[..., Any], *args: Any, **kwargs: Any) -> Any:
    if inspect.iscoroutinefunction(fn):
        return await fn(*args, **kwargs)

    out = await asyncio.to_thread(fn, *args, **kwargs)
    if inspect.isawaitable(out):
        return await out
    return out


# ---------------------------------------------------------------------------
# Auth helpers
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


def _call_auth_ok_tolerant(request: Request, auth_token: str, authorization: Optional[str]) -> bool:
    headers_dict = dict(request.headers)
    path = str(getattr(getattr(request, "url", None), "path", "") or "")

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
            "request": request,
        },
        {
            "token": auth_token or None,
        },
        {
            "authorization": authorization,
        },
        {
            "request": request,
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
    return _call_auth_ok_tolerant(request, auth_token, authorization)


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
# Engine accessor
# ---------------------------------------------------------------------------
async def _get_engine(request: Request) -> Optional[Any]:
    try:
        st = getattr(request.app, "state", None)
        if st and getattr(st, "engine", None):
            return st.engine
    except Exception:
        pass

    for mod_name in ("core.data_engine_v2", "core.data_engine"):
        try:
            mod = importlib.import_module(mod_name)
            get_engine = getattr(mod, "get_engine", None)
            if callable(get_engine):
                eng = get_engine()
                if inspect.isawaitable(eng):
                    eng = await eng
                return eng
        except Exception:
            continue

    return None


# ---------------------------------------------------------------------------
# Request normalization
# ---------------------------------------------------------------------------
def _normalize_sources(
    *,
    sources_in: Any,
    page: Any = None,
    sheet: Any = None,
    sheet_name: Any = None,
) -> List[str]:
    page_s = _normalize_page_name(_clean_str(page) or _clean_str(sheet) or _clean_str(sheet_name))
    sources = [_normalize_page_name(s) for s in _list_from_any(sources_in)]

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

    if not _clean_str(p.get("page")) and (_clean_str(p.get("sheet")) or _clean_str(p.get("sheet_name"))):
        p["page"] = _normalize_target_page(
            page=p.get("page"),
            sheet=p.get("sheet"),
            sheet_name=p.get("sheet_name"),
        )

    p["sources"] = _normalize_sources(
        sources_in=p.get("sources"),
        page=p.get("page"),
        sheet=p.get("sheet"),
        sheet_name=p.get("sheet_name"),
    )

    for k in ("page", "sheet", "sheet_name"):
        if k in p and _clean_str(p.get(k)):
            p[k] = _normalize_page_name(p.get(k))
        elif k in p:
            p.pop(k, None)

    for k in ("mode", "data_mode", "advisor_data_mode"):
        if k in p and not _clean_str(p.get(k)):
            p.pop(k, None)

    if "tickers" in p:
        p["tickers"] = [_normalize_symbol(x) for x in _dedupe_keep_order(_list_from_any(p.get("tickers"))) if _normalize_symbol(x)]

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
# Runner discovery
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


async def _resolve_runner_via_shared_route(
    request: Request,
    engine: Any = None,
) -> Optional[Tuple[Callable[..., Any], str, str]]:
    for mod_name in _SHARED_ROUTE_MODULES:
        mod, _ = _module_import_ok(mod_name)
        if mod is None:
            continue

        for helper_name in ("_resolve_advisor_runner", "_resolve_runner"):
            resolver = getattr(mod, helper_name, None)
            if callable(resolver):
                attempts = [
                    {"request": request, "engine": engine},
                    {"request": request},
                ]
                for kwargs in attempts:
                    try:
                        out = await _call_maybe_async(resolver, **kwargs)
                        if isinstance(out, tuple) and len(out) >= 2 and callable(out[0]):
                            source = str(out[1]) if len(out) > 1 else mod_name
                            name = str(out[2]) if len(out) > 2 else getattr(out[0], "__name__", "runner")
                            return out[0], source, name
                    except TypeError:
                        continue
                    except Exception:
                        continue
    return None


async def _resolve_advisor_runner(request: Request, engine: Any = None) -> Tuple[Callable[..., Any], str, str]:
    searched_labels: List[str] = []
    import_errors: List[str] = []

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

    if engine is not None:
        searched_labels.append("engine")
        resolved_engine = await _resolve_runner_from_container(engine, "engine")
        if resolved_engine:
            return resolved_engine

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

    shared = await _resolve_runner_via_shared_route(request, engine=engine)
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
# Shared page pipeline helpers
# ---------------------------------------------------------------------------
def _result_count(result: Mapping[str, Any]) -> int:
    if not isinstance(result, Mapping):
        return 0
    for key in ("items", "records", "row_objects", "rows", "data", "quotes", "rows_matrix"):
        value = result.get(key)
        if isinstance(value, list):
            return len(value)
    return 0


async def _run_shared_page_pipeline(
    *,
    request: Request,
    payload: Dict[str, Any],
    page: str,
    engine: Any,
    timeout_sec: float,
) -> Optional[Dict[str, Any]]:
    for mod_name in _SHARED_ROUTE_MODULES:
        mod, _ = _module_import_ok(mod_name)
        if mod is None:
            continue

        fn = getattr(mod, "_run_page_pipeline", None)
        if not callable(fn):
            continue

        attempts = [
            {"page": page, "payload": payload, "request": request, "engine": engine, "timeout_sec": timeout_sec},
            {"page": page, "payload": payload, "request": request, "engine": engine},
        ]
        for kwargs in attempts:
            try:
                out = await _call_maybe_async(fn, **kwargs)
                if isinstance(out, dict):
                    return dict(out)
                d = _model_to_dict(out)
                if d:
                    return d
            except TypeError:
                continue
            except Exception:
                continue
    return None


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
    target_page = _normalize_target_page(
        page=payload2.get("page"),
        sheet=payload2.get("sheet"),
        sheet_name=payload2.get("sheet_name"),
        default="Market_Leaders",
    )
    payload2["page"] = target_page
    payload2["sheet"] = target_page
    payload2["sheet_name"] = target_page

    timeout_sec = max(5.0, min(180.0, float(os.getenv("ADVISOR_ROUTE_TIMEOUT_SEC", "75") or "75")))

    shared = await _run_shared_page_pipeline(
        request=request,
        payload=payload2,
        page=target_page,
        engine=engine,
        timeout_sec=timeout_sec,
    )
    if isinstance(shared, dict):
        result = dict(shared)
        meta = result.get("meta") if isinstance(result.get("meta"), dict) else {}
        meta.update(
            {
                "route_version": ADVISOR_ROUTE_VERSION,
                "request_id": request_id,
                "generated_at_utc": _now_utc_iso(),
                "engine_type": _safe_engine_type(engine),
                "advisor_data_mode_effective": _clean_str(payload2.get("advisor_data_mode")),
                "warm_snapshots": bool(warm_snapshots),
                "cache_strategy": _clean_str(cache_strategy).lower() or "memory",
                "cache_ttl": int(cache_ttl),
                "shared_pipeline_used": True,
            }
        )
        result.setdefault("status", "success")
        result["meta"] = meta
        return _json_safe(result)

    wants_snapshot = _clean_str(payload2.get("advisor_data_mode")).lower() == "snapshot"
    use_warm = bool(warm_snapshots or wants_snapshot)

    settings = None
    try:
        settings = get_settings_cached()
    except Exception:
        settings = None

    runner, runner_source, runner_name = await _resolve_advisor_runner(request, engine=engine)

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
            "shared_pipeline_used": False,
        }
    )

    if "status" not in result:
        result["status"] = "success"

    result["meta"] = meta
    return _json_safe(result)


# ---------------------------------------------------------------------------
# Schema + projection helpers
# ---------------------------------------------------------------------------
async def _get_schema_layout(page: str) -> Tuple[List[str], List[str]]:
    page = _normalize_page_name(page)

    try:
        from core.sheets.schema_registry import get_sheet_spec  # type: ignore

        spec = get_sheet_spec(page)
        headers, keys = _extract_headers_keys_from_spec(spec)
        headers, keys = _complete_headers_keys(headers, keys)
        if page == TOP10_PAGE_NAME:
            headers, keys = _ensure_top10_fields(headers, keys)
        if headers or keys:
            return headers, keys
    except Exception:
        pass

    if page == TOP10_PAGE_NAME:
        return _ensure_top10_fields(
            [
                "Symbol",
                "Recommendation",
                "Recommendation Reason",
                "Current Price",
                "Forecast Price 1M",
                "Forecast Price 3M",
                "Forecast Price 12M",
                "Expected ROI 1M",
                "Expected ROI 3M",
                "Expected ROI 12M",
                "Forecast Confidence",
                "Overall Score",
                "Risk Bucket",
                "Horizon Days",
                "Invest Period Label",
            ],
            [
                "symbol",
                "recommendation",
                "recommendation_reason",
                "current_price",
                "forecast_price_1m",
                "forecast_price_3m",
                "forecast_price_12m",
                "expected_roi_1m",
                "expected_roi_3m",
                "expected_roi_12m",
                "forecast_confidence",
                "overall_score",
                "risk_bucket",
                "horizon_days",
                "invest_period_label",
            ],
        )

    if page == "Insights_Analysis":
        return _complete_headers_keys(
            [
                "Section",
                "Item",
                "Symbol",
                "Metric",
                "Value",
                "Notes",
                "Last Updated Riyadh",
            ],
            [
                "section",
                "item",
                "symbol",
                "metric",
                "value",
                "notes",
                "last_updated_riyadh",
            ],
        )

    if page == "Data_Dictionary":
        return _complete_headers_keys(
            [
                "Sheet",
                "Group",
                "Header",
                "Key",
                "DType",
                "Format",
                "Required",
                "Source",
                "Notes",
            ],
            [
                "sheet",
                "group",
                "header",
                "key",
                "dtype",
                "fmt",
                "required",
                "source",
                "notes",
            ],
        )

    return _complete_headers_keys(
        [
            "Symbol",
            "Name",
            "Current Price",
            "Recommendation",
            "Recommendation Reason",
            "Overall Score",
            "Risk Bucket",
            "Forecast Confidence",
            "Horizon Days",
            "Invest Period Label",
            "Top10 Rank",
            "Selection Reason",
            "Criteria Snapshot",
        ],
        [
            "symbol",
            "name",
            "current_price",
            "recommendation",
            "recommendation_reason",
            "overall_score",
            "risk_bucket",
            "forecast_confidence",
            "horizon_days",
            "invest_period_label",
            "top10_rank",
            "selection_reason",
            "criteria_snapshot",
        ],
    )


def _extract_headers_keys_from_spec(spec: Any) -> Tuple[List[str], List[str]]:
    headers: List[str] = []
    keys: List[str] = []

    raw_columns = None
    if isinstance(spec, dict):
        raw_columns = spec.get("columns") or spec.get("fields")
        if not raw_columns:
            maybe_keys = spec.get("keys") or []
            maybe_headers = spec.get("headers") or spec.get("display_headers") or []
            if isinstance(maybe_keys, list):
                keys = [_clean_str(x) for x in maybe_keys if _clean_str(x)]
                if isinstance(maybe_headers, list):
                    headers = [_clean_str(x) for x in maybe_headers if _clean_str(x)]
                if keys and not headers:
                    headers = [k.replace("_", " ").title() for k in keys]
                return headers, keys
    else:
        raw_columns = getattr(spec, "columns", None) or getattr(spec, "fields", None)

    if isinstance(raw_columns, list):
        for col in raw_columns:
            if isinstance(col, dict):
                key = _clean_str(col.get("key") or col.get("field") or col.get("name") or col.get("id"))
                header = _clean_str(col.get("header") or col.get("title") or col.get("label") or key)
            else:
                key = _clean_str(
                    getattr(col, "key", None)
                    or getattr(col, "field", None)
                    or getattr(col, "name", None)
                    or getattr(col, "id", None)
                )
                header = _clean_str(
                    getattr(col, "header", None)
                    or getattr(col, "title", None)
                    or getattr(col, "label", None)
                    or key
                )

            if key:
                keys.append(key)
                headers.append(header or key)

    return headers, keys


def _extract_headers_keys_from_payload(obj: Any) -> Tuple[List[str], List[str]]:
    if not isinstance(obj, dict):
        return [], []

    headers = (
        obj.get("display_headers")
        or obj.get("sheet_headers")
        or obj.get("column_headers")
        or obj.get("headers")
        or []
    )
    keys = obj.get("keys") or obj.get("fields") or []

    if not isinstance(headers, list):
        headers = []
    if not isinstance(keys, list):
        keys = []

    clean_headers = [_clean_str(x) for x in headers if _clean_str(x)]
    clean_keys = [_clean_str(x) for x in keys if _clean_str(x)]
    return _complete_headers_keys(clean_headers, clean_keys)


def _rows_from_any(obj: Any) -> List[Any]:
    if obj is None:
        return []

    if isinstance(obj, dict):
        for key in ("row_objects", "records", "items", "rows", "data", "results", "quotes", "rows_matrix"):
            value = obj.get(key)
            if value is None:
                continue
            if isinstance(value, list):
                return value
            return [value]

    if isinstance(obj, list):
        return obj

    return [obj]


def _rows_are_matrix(rows: List[Any]) -> bool:
    return bool(rows) and isinstance(rows[0], (list, tuple))


def _derive_keys_from_items(items: List[Any]) -> List[str]:
    out: List[str] = []
    seen = set()

    for item in items:
        if not isinstance(item, dict):
            continue
        for key in item.keys():
            k = _clean_str(key)
            if not k or k in seen:
                continue
            seen.add(k)
            out.append(k)

    return out


def _project_dict_items_to_matrix(items: List[Any], keys: List[str]) -> List[List[Any]]:
    projected: List[List[Any]] = []
    for item in items:
        if isinstance(item, dict):
            projected.append([_json_safe(item.get(k)) for k in keys])
        elif isinstance(item, (list, tuple)):
            row = list(item)
            if len(row) < len(keys):
                row.extend([None] * (len(keys) - len(row)))
            projected.append([_json_safe(x) for x in row[: len(keys)]])
        else:
            projected.append([_json_safe(item)] + [None] * (max(0, len(keys) - 1)))
    return projected


def _extract_value_ci(raw: Mapping[str, Any], key: str) -> Any:
    if key in raw:
        return raw.get(key)
    lowmap = {str(k).strip().lower(): v for k, v in raw.items()}
    if key.lower() in lowmap:
        return lowmap[key.lower()]
    compact = _snake_like(key).replace("_", "")
    for rk, rv in raw.items():
        if _snake_like(str(rk)).replace("_", "") == compact:
            return rv
    return None


def _lookup_candidates_for_key_header(key: str, header: str) -> List[str]:
    candidates: List[str] = []

    def _push(v: Any) -> None:
        s = _clean_str(v)
        if s:
            candidates.append(s)

    _push(key)
    _push(header)

    if key:
        _push(key.lower())
        _push(key.upper())
        _push(key.replace("_", " "))
        _push(key.replace("_", ""))
        _push(key.replace("_", "-"))
        _push(key.replace("_", "."))
        _push(key.title())
        _push(key.upper().replace("_", " "))
        _push(key.lower().replace("_", " "))

    if header:
        _push(header.lower())
        _push(header.upper())
        _push(header.title())
        _push(header.replace(" ", "_"))
        _push(header.replace(" ", ""))
        _push(header.replace(" ", "-"))
        _push(_snake_like(header))

    return _dedupe_keep_order(candidates)


def _extract_value_by_key_header(raw: Mapping[str, Any], key: str, header: str) -> Any:
    for candidate in _lookup_candidates_for_key_header(key, header):
        value = _extract_value_ci(raw, candidate)
        if value is not None:
            return value
    return None


def _matrix_row_to_row_object(
    row: Any,
    *,
    headers: Sequence[str],
    keys: Sequence[str],
    page: str,
    payload: Optional[Mapping[str, Any]] = None,
    rank: Optional[int] = None,
) -> Dict[str, Any]:
    vals = list(row) if isinstance(row, (list, tuple)) else [row]
    raw: Dict[str, Any] = {}

    for i, value in enumerate(vals):
        if i < len(keys):
            raw[str(keys[i])] = value
        if i < len(headers):
            hdr = _clean_str(headers[i])
            if hdr and hdr not in raw:
                raw[hdr] = value

    return _project_item_to_row_object(
        raw,
        headers=headers,
        keys=keys,
        page=page,
        payload=payload,
        rank=rank,
    )


def _project_item_to_row_object(
    item: Any,
    *,
    headers: Sequence[str],
    keys: Sequence[str],
    page: str,
    payload: Optional[Mapping[str, Any]] = None,
    rank: Optional[int] = None,
) -> Dict[str, Any]:
    raw = item if isinstance(item, dict) else _model_to_dict(item)
    if not isinstance(raw, dict):
        raw = {}

    out: Dict[str, Any] = {}
    for idx, key in enumerate(keys):
        header = headers[idx] if idx < len(headers) else key
        out[key] = _json_safe(_extract_value_by_key_header(raw, key, header))

    if page == TOP10_PAGE_NAME:
        out = _ensure_top10_context(out, payload or {}, rank)

    return out


def _infer_horizon_days(payload: Mapping[str, Any]) -> Optional[int]:
    for key in ("horizon_days", "invest_period_days", "investment_period_days", "period_days", "days"):
        v = _safe_int(payload.get(key), 0)
        if v > 0:
            return v

    label = _clean_str(
        payload.get("invest_period_label")
        or payload.get("investment_period_label")
        or payload.get("period_label")
        or payload.get("horizon_label")
    ).upper()

    label_map = {
        "1M": 30,
        "30D": 30,
        "3M": 90,
        "90D": 90,
        "6M": 180,
        "180D": 180,
        "12M": 365,
        "1Y": 365,
        "365D": 365,
    }
    return label_map.get(label)


def _infer_invest_period_label(payload: Mapping[str, Any], days: Optional[int]) -> str:
    label = _clean_str(
        payload.get("invest_period_label")
        or payload.get("investment_period_label")
        or payload.get("period_label")
        or payload.get("horizon_label")
    ).upper()
    if label:
        return label
    if days is None:
        return ""
    if days <= 45:
        return "1M"
    if days <= 135:
        return "3M"
    if days <= 240:
        return "6M"
    return "12M"


def _criteria_snapshot(payload: Mapping[str, Any]) -> Dict[str, Any]:
    compact = {
        "page": _normalize_page_name(
            payload.get("page")
            or payload.get("sheet_name")
            or payload.get("sheet")
            or payload.get("name")
            or payload.get("tab")
            or TOP10_PAGE_NAME
        ),
        "risk_profile": payload.get("risk_profile"),
        "allocation_strategy": payload.get("allocation_strategy"),
        "invest_amount": payload.get("invest_amount"),
        "horizon_days": _infer_horizon_days(payload),
        "invest_period_label": _infer_invest_period_label(payload, _infer_horizon_days(payload)),
        "top_n": payload.get("top_n") or payload.get("limit"),
        "symbols": payload.get("symbols") or payload.get("tickers"),
    }
    return {k: _json_safe(v) for k, v in compact.items() if v not in (None, "", [], {})}


def _build_recommendation_reason(row: Dict[str, Any], payload: Mapping[str, Any]) -> str:
    rec = _clean_str(row.get("recommendation")).upper()
    if not rec:
        return "Selected by advisor scoring."

    horizon_days = _safe_int(row.get("horizon_days"), 0) or (_infer_horizon_days(payload) or 0)
    invest_period_label = _clean_str(row.get("invest_period_label")) or _infer_invest_period_label(payload, horizon_days)
    current_price = _safe_float(row.get("current_price") or row.get("price"))
    fp1 = _safe_float(row.get("forecast_price_1m"))
    fp3 = _safe_float(row.get("forecast_price_3m"))
    fp12 = _safe_float(row.get("forecast_price_12m"))
    roi1 = _as_ratio(row.get("expected_roi_1m"))
    roi3 = _as_ratio(row.get("expected_roi_3m"))
    roi12 = _as_ratio(row.get("expected_roi_12m"))
    overall = _safe_float(row.get("overall_score"))
    confidence = _as_ratio(row.get("forecast_confidence") or row.get("confidence_score"))
    risk_bucket = _clean_str(row.get("risk_bucket"))
    risk_score = _safe_float(row.get("risk_score"))
    selected_roi = roi3
    selected_fp = fp3

    if horizon_days:
        if horizon_days <= 45:
            selected_roi = roi1 if roi1 is not None else roi3
            selected_fp = fp1 if fp1 is not None else fp3
        elif horizon_days > 135:
            selected_roi = roi12 if roi12 is not None else roi3
            selected_fp = fp12 if fp12 is not None else fp3

    parts: List[str] = []
    if selected_roi is not None:
        parts.append(f"expected {invest_period_label or 'target-horizon'} return is {round(selected_roi * 100.0, 2)}%")
    if selected_fp is not None and current_price is not None:
        cmp = "above" if selected_fp > current_price else "below" if selected_fp < current_price else "near"
        parts.append(f"forecast price {round(selected_fp, 2)} is {cmp} current price {round(current_price, 2)}")
    if confidence is not None:
        parts.append(f"confidence is {round(confidence * 100.0, 2)}%")
    if overall is not None:
        parts.append(f"overall score is {round(overall, 2)}")
    if risk_bucket:
        parts.append(f"risk bucket is {risk_bucket}")
    elif risk_score is not None:
        parts.append(f"risk score is {round(risk_score, 2)}")

    if not parts:
        return f"{rec} based on the current risk-return profile."
    return f"{rec} because " + ", ".join(parts[:4]) + "."


def _ensure_top10_context(row: Dict[str, Any], payload: Mapping[str, Any], rank: Optional[int]) -> Dict[str, Any]:
    out = dict(row or {})
    inferred_horizon_days = _infer_horizon_days(payload)
    inferred_label = _infer_invest_period_label(payload, inferred_horizon_days)

    if out.get("horizon_days") is None and inferred_horizon_days is not None:
        out["horizon_days"] = inferred_horizon_days

    if not out.get("invest_period_label") and inferred_label:
        out["invest_period_label"] = inferred_label

    if out.get("recommendation") and not _clean_str(out.get("recommendation_reason")):
        out["recommendation_reason"] = _build_recommendation_reason(out, payload)

    if rank is not None and out.get("top10_rank") is None:
        out["top10_rank"] = rank

    if not out.get("selection_reason"):
        out["selection_reason"] = out.get("recommendation_reason") or "Selected by advisor scoring."

    if not out.get("criteria_snapshot"):
        out["criteria_snapshot"] = _criteria_snapshot(payload)

    return out


def _row_objects_from_matrix(
    rows_matrix: Sequence[Any],
    *,
    headers: Sequence[str],
    keys: Sequence[str],
    page: str,
    payload: Optional[Mapping[str, Any]] = None,
) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    for idx, row in enumerate(rows_matrix, start=1):
        out.append(
            _matrix_row_to_row_object(
                row,
                headers=headers,
                keys=keys,
                page=page,
                payload=payload,
                rank=idx,
            )
        )
    return out


def _row_objects_from_items(
    items: Sequence[Any],
    *,
    headers: Sequence[str],
    keys: Sequence[str],
    page: str,
    payload: Optional[Mapping[str, Any]] = None,
) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    for idx, item in enumerate(items, start=1):
        out.append(
            _project_item_to_row_object(
                item,
                headers=headers,
                keys=keys,
                page=page,
                payload=payload,
                rank=idx,
            )
        )
    return out


async def _coerce_to_sheet_rows_response(
    *,
    page: str,
    advisor_result: Dict[str, Any],
    payload: Mapping[str, Any],
) -> Dict[str, Any]:
    raw = dict(advisor_result or {})
    items = _rows_from_any(raw)

    payload_headers, payload_keys = _extract_headers_keys_from_payload(raw)
    schema_headers, schema_keys = await _get_schema_layout(page)

    headers = payload_headers or schema_headers
    keys = payload_keys or schema_keys

    if not keys and items and not _rows_are_matrix(items):
        derived = _derive_keys_from_items([x for x in items if isinstance(x, dict)])
        if derived:
            keys = derived

    headers, keys = _complete_headers_keys(headers, keys)
    if page == TOP10_PAGE_NAME:
        headers, keys = _ensure_top10_fields(headers, keys)

    row_objects: List[Dict[str, Any]] = []

    if items and _rows_are_matrix(items):
        source_headers = payload_headers or headers
        source_keys = payload_keys or keys
        source_headers, source_keys = _complete_headers_keys(source_headers, source_keys)

        raw_like_items: List[Dict[str, Any]] = []
        for row in items:
            vals = list(row) if isinstance(row, (list, tuple)) else [row]
            temp: Dict[str, Any] = {}
            for i, value in enumerate(vals):
                if i < len(source_keys):
                    temp[str(source_keys[i])] = value
                if i < len(source_headers):
                    hdr = _clean_str(source_headers[i])
                    if hdr and hdr not in temp:
                        temp[hdr] = value
            raw_like_items.append(temp)

        row_objects = _row_objects_from_items(
            raw_like_items,
            headers=headers,
            keys=keys,
            page=page,
            payload=payload,
        )
    else:
        row_objects = _row_objects_from_items(
            items,
            headers=headers,
            keys=keys,
            page=page,
            payload=payload,
        )

    rows_matrix = _project_dict_items_to_matrix(row_objects, keys)

    meta = raw.get("meta") if isinstance(raw.get("meta"), dict) else {}
    meta = dict(meta)
    meta.setdefault("route_version", ADVISOR_ROUTE_VERSION)
    meta.setdefault("projection", "advisor_result_to_sheet_rows")
    meta["count"] = len(row_objects)
    meta["row_object_count"] = len(row_objects)

    status_value = _clean_str(raw.get("status"))
    if not status_value:
        status_value = "success" if rows_matrix else "partial"

    detail_value = _clean_str(raw.get("detail") or raw.get("error") or raw.get("message"))

    return _json_safe(
        {
            "status": status_value,
            "page": page,
            "sheet": page,
            "sheet_name": page,
            "route_family": _route_family_for_page(page),
            "headers": headers,
            "display_headers": headers,
            "sheet_headers": headers,
            "column_headers": headers,
            "keys": keys,
            "rows": rows_matrix,
            "rows_matrix": rows_matrix,
            "row_objects": row_objects,
            "items": row_objects,
            "records": row_objects,
            "count": len(row_objects),
            "detail": detail_value,
            "meta": meta,
            "version": raw.get("version") or ADVISOR_ROUTE_VERSION,
            "request_id": raw.get("request_id") or meta.get("request_id"),
        }
    )


def _looks_like_sheet_payload(obj: Any) -> bool:
    if not isinstance(obj, dict):
        return False

    if isinstance(obj.get("headers"), list) or isinstance(obj.get("keys"), list):
        if (
            isinstance(obj.get("rows"), list)
            or isinstance(obj.get("rows_matrix"), list)
            or isinstance(obj.get("row_objects"), list)
            or isinstance(obj.get("items"), list)
            or isinstance(obj.get("records"), list)
        ):
            return True

    if any(k in obj for k in ("sheet", "page", "headers", "keys", "rows", "rows_matrix", "row_objects", "items", "records")):
        score = 0
        if isinstance(obj.get("headers"), list):
            score += 1
        if isinstance(obj.get("keys"), list):
            score += 1
        if isinstance(obj.get("rows"), list):
            score += 1
        if isinstance(obj.get("rows_matrix"), list):
            score += 1
        if isinstance(obj.get("row_objects"), list):
            score += 1
        if isinstance(obj.get("items"), list):
            score += 1
        if isinstance(obj.get("records"), list):
            score += 1
        if obj.get("sheet") or obj.get("page"):
            score += 1
        return score >= 2

    return False


def _unwrap_sheet_payload(obj: Any) -> Dict[str, Any]:
    if not isinstance(obj, dict):
        return {}

    if _looks_like_sheet_payload(obj):
        return dict(obj)

    for key in ("data", "payload", "result"):
        inner = obj.get(key)
        if isinstance(inner, dict) and _looks_like_sheet_payload(inner):
            return dict(inner)

    return dict(obj)


def _normalize_sheet_payload(
    *,
    raw_result: Dict[str, Any],
    target_page: str,
    request_id: str,
    source: str,
    schema_only: bool = False,
    headers_only: bool = False,
    include_matrix: bool = True,
) -> Dict[str, Any]:
    raw = _unwrap_sheet_payload(raw_result)
    page = _normalize_page_name(raw.get("page") or raw.get("sheet") or target_page)

    headers = raw.get("headers") or raw.get("display_headers") or []
    keys = raw.get("keys") or raw.get("fields") or []
    headers, keys = _complete_headers_keys(headers, keys)

    row_objects_input = raw.get("row_objects")
    if not isinstance(row_objects_input, list):
        row_objects_input = raw.get("records")
    if not isinstance(row_objects_input, list):
        row_objects_input = raw.get("items")

    rows_obj = raw.get("rows")
    matrix_obj = raw.get("rows_matrix")

    if page == TOP10_PAGE_NAME:
        headers, keys = _ensure_top10_fields(headers, keys)

    row_objects: List[Dict[str, Any]] = []

    if schema_only or headers_only:
        row_objects = []
    elif isinstance(row_objects_input, list) and row_objects_input:
        if not keys:
            derived = _derive_keys_from_items([x for x in row_objects_input if isinstance(x, dict)])
            if derived:
                keys = derived
                headers, keys = _complete_headers_keys(headers, keys)
                if page == TOP10_PAGE_NAME:
                    headers, keys = _ensure_top10_fields(headers, keys)
        row_objects = _row_objects_from_items(
            row_objects_input,
            headers=headers,
            keys=keys,
            page=page,
        )
    elif isinstance(rows_obj, list) and rows_obj:
        if all(isinstance(r, dict) for r in rows_obj):
            if not keys:
                keys = _derive_keys_from_items(rows_obj)
                headers, keys = _complete_headers_keys(headers, keys)
                if page == TOP10_PAGE_NAME:
                    headers, keys = _ensure_top10_fields(headers, keys)
            row_objects = _row_objects_from_items(
                rows_obj,
                headers=headers,
                keys=keys,
                page=page,
            )
        elif all(isinstance(r, (list, tuple)) for r in rows_obj):
            row_objects = _row_objects_from_matrix(
                rows_obj,
                headers=headers,
                keys=keys,
                page=page,
            )
    elif isinstance(matrix_obj, list):
        row_objects = _row_objects_from_matrix(
            matrix_obj,
            headers=headers,
            keys=keys,
            page=page,
        )

    rows_matrix = _project_dict_items_to_matrix(row_objects, keys) if include_matrix else []

    if not keys and rows_matrix and headers:
        keys = [_snake_like(h) for h in headers]
        headers, keys = _complete_headers_keys(headers, keys)

    if not headers and keys:
        headers, keys = _complete_headers_keys(headers, keys)

    if not row_objects and rows_matrix:
        row_objects = _row_objects_from_matrix(
            rows_matrix,
            headers=headers,
            keys=keys,
            page=page,
        )

    meta = raw.get("meta") if isinstance(raw.get("meta"), dict) else {}
    meta = dict(meta)
    meta["route_version"] = ADVISOR_ROUTE_VERSION
    meta["native_source"] = source
    meta["count"] = len(row_objects)
    meta["row_object_count"] = len(row_objects)
    meta["schema_only"] = bool(schema_only)
    meta["headers_only"] = bool(headers_only)

    status_value = _clean_str(raw.get("status")) or ("success" if row_objects or schema_only or headers_only else "warn")

    return _json_safe(
        {
            "status": status_value,
            "page": page,
            "sheet": page,
            "sheet_name": page,
            "route_family": _route_family_for_page(page),
            "headers": headers,
            "display_headers": headers,
            "sheet_headers": headers,
            "column_headers": headers,
            "keys": keys,
            "rows": rows_matrix,
            "rows_matrix": rows_matrix,
            "row_objects": row_objects,
            "items": row_objects,
            "records": row_objects,
            "count": len(row_objects),
            "detail": _clean_str(raw.get("detail") or raw.get("error") or raw.get("message")),
            "meta": meta,
            "version": raw.get("version") or ADVISOR_ROUTE_VERSION,
            "request_id": request_id,
        }
    )


async def _run_native_sheet_rows(
    payload: Dict[str, Any],
    *,
    engine: Any,
    timeout_sec: float,
) -> Tuple[Optional[Dict[str, Any]], str, Optional[str]]:
    target_page = _normalize_target_page(
        page=payload.get("page"),
        sheet=payload.get("sheet"),
        sheet_name=payload.get("sheet_name"),
        default="Market_Leaders",
    )

    methods = (
        "get_sheet_rows",
        "sheet_rows",
        "build_sheet_rows",
        "get_page_rows",
        "get_sheet",
        "get_cached_sheet_rows",
        "get_sheet_snapshot",
        "get_cached_sheet_snapshot",
    )
    errors: List[str] = []

    limit = int(payload.get("limit") or payload.get("top_n") or 20)
    offset = int(payload.get("offset") or 0)
    mode = _clean_str(payload.get("mode") or payload.get("advisor_data_mode"))

    for method_name in methods:
        fn = getattr(engine, method_name, None)
        if not callable(fn):
            continue

        attempts = [
            {"sheet": target_page, "limit": limit, "offset": offset, "mode": mode, "body": payload},
            {"sheet_name": target_page, "limit": limit, "offset": offset, "mode": mode, "body": payload},
            {"page": target_page, "limit": limit, "offset": offset, "mode": mode, "body": payload},
            {"sheet": target_page, "limit": limit, "offset": offset, "mode": mode},
            {"sheet_name": target_page, "limit": limit, "offset": offset, "mode": mode},
            {"page": target_page, "limit": limit, "offset": offset, "mode": mode},
        ]

        out = None
        for kwargs in attempts:
            try:
                out = await asyncio.wait_for(
                    _call_maybe_async(fn, **{k: v for k, v in kwargs.items() if v not in (None, "")}),
                    timeout=timeout_sec,
                )
                break
            except TypeError:
                continue
            except Exception as e:
                errors.append(f"{method_name}: {type(e).__name__}: {e}")
                out = None
                break

        if out is None:
            for args in [(target_page,), (target_page, limit), (target_page, limit, offset)]:
                try:
                    out = await asyncio.wait_for(_call_maybe_async(fn, *args), timeout=timeout_sec)
                    break
                except TypeError:
                    continue
                except Exception as e:
                    errors.append(f"{method_name}: {type(e).__name__}: {e}")
                    out = None
                    break

        if isinstance(out, dict):
            return dict(out), f"engine.{method_name}", None

        d = _model_to_dict(out)
        if d:
            return d, f"engine.{method_name}", None

    return None, "native_sheet_rows_unresolved", (
        " | ".join(errors)[:3000] if errors else "no native sheet rows method available"
    )


def _build_schema_only_sheet_payload(
    *,
    page: str,
    request_id: str,
    schema_only: bool,
    headers_only: bool,
    include_matrix: bool,
    headers: Sequence[str],
    keys: Sequence[str],
) -> Dict[str, Any]:
    headers2, keys2 = _complete_headers_keys(headers, keys)
    if page == TOP10_PAGE_NAME:
        headers2, keys2 = _ensure_top10_fields(headers2, keys2)

    return _json_safe(
        {
            "status": "success",
            "page": page,
            "sheet": page,
            "sheet_name": page,
            "route_family": _route_family_for_page(page),
            "headers": headers2,
            "display_headers": headers2,
            "sheet_headers": headers2,
            "column_headers": headers2,
            "keys": keys2,
            "rows": [],
            "rows_matrix": [] if include_matrix else [],
            "row_objects": [],
            "items": [],
            "records": [],
            "count": 0,
            "detail": "",
            "meta": {
                "route_version": ADVISOR_ROUTE_VERSION,
                "request_id": request_id,
                "schema_only": bool(schema_only),
                "headers_only": bool(headers_only),
                "projection": "schema_layout_only",
                "row_object_count": 0,
            },
            "version": ADVISOR_ROUTE_VERSION,
            "request_id": request_id,
        }
    )


def _build_data_dictionary_sheet_payload(
    *,
    request_id: str,
    include_matrix: bool,
    schema_only: bool,
    headers_only: bool,
) -> Dict[str, Any]:
    headers, keys = _complete_headers_keys(
        [
            "Sheet",
            "Group",
            "Header",
            "Key",
            "DType",
            "Format",
            "Required",
            "Source",
            "Notes",
        ],
        [
            "sheet",
            "group",
            "header",
            "key",
            "dtype",
            "fmt",
            "required",
            "source",
            "notes",
        ],
    )

    if schema_only or headers_only:
        return _build_schema_only_sheet_payload(
            page="Data_Dictionary",
            request_id=request_id,
            schema_only=schema_only,
            headers_only=headers_only,
            include_matrix=include_matrix,
            headers=headers,
            keys=keys,
        )

    row_objects: List[Dict[str, Any]] = []
    status_value = "success"
    detail_value = ""

    try:
        mod = importlib.import_module("core.sheets.data_dictionary")
        build_rows = getattr(mod, "build_data_dictionary_rows", None)
        raw_rows = []
        if callable(build_rows):
            try:
                raw_rows = build_rows(include_meta_sheet=True)
            except TypeError:
                raw_rows = build_rows()

        for item in raw_rows or []:
            d = item if isinstance(item, dict) else _model_to_dict(item)
            row_objects.append({k: _json_safe(d.get(k)) for k in keys})

    except Exception as e:
        status_value = "partial"
        detail_value = f"{type(e).__name__}: {e}"

    rows = _project_dict_items_to_matrix(row_objects, keys) if include_matrix else []

    return _json_safe(
        {
            "status": status_value,
            "page": "Data_Dictionary",
            "sheet": "Data_Dictionary",
            "sheet_name": "Data_Dictionary",
            "route_family": "schema",
            "headers": headers,
            "display_headers": headers,
            "sheet_headers": headers,
            "column_headers": headers,
            "keys": keys,
            "rows": rows,
            "rows_matrix": rows,
            "row_objects": row_objects,
            "items": row_objects,
            "records": row_objects,
            "count": len(row_objects),
            "detail": detail_value,
            "meta": {
                "route_version": ADVISOR_ROUTE_VERSION,
                "request_id": request_id,
                "projection": "data_dictionary",
                "row_object_count": len(row_objects),
            },
            "version": ADVISOR_ROUTE_VERSION,
            "request_id": request_id,
        }
    )


# ---------------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------------
@router.get("/health")
async def advisor_health(request: Request) -> Dict[str, Any]:
    engine = await _get_engine(request)

    engine_health: Optional[Dict[str, Any]] = None
    native_methods: List[str] = []
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

        native_methods = [
            name
            for name in (
                "get_sheet_rows",
                "sheet_rows",
                "build_sheet_rows",
                "get_page_rows",
                "get_sheet",
                "get_cached_sheet_rows",
                "get_sheet_snapshot",
                "get_cached_sheet_snapshot",
            )
            if callable(getattr(engine, name, None))
        ]

    runner_available = False
    runner_source = ""
    runner_name = ""
    try:
        _, runner_source, runner_name = await _resolve_advisor_runner(request, engine=engine)
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
        "native_sheet_rows_methods": native_methods,
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
        payload["page"] = _normalize_page_name(page)

    if _clean_str(sheet_name) and not _clean_str(payload.get("sheet_name")) and not _clean_str(payload.get("sources")):
        payload["sheet_name"] = _normalize_page_name(sheet_name)

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
    effective_page = _normalize_page_name(_clean_str(page) or _clean_str(sheet_name))
    normalized_sources = _list_from_any(sources)

    if effective_page and (not normalized_sources or normalized_sources == ["ALL"]):
        normalized_sources = [effective_page]

    payload: Dict[str, Any] = {
        "sources": normalized_sources,
        "page": effective_page,
        "sheet_name": effective_page if effective_page else _clean_str(sheet_name),
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
    offset: int = Query(default=0, ge=0, le=5000),
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
    schema_only: bool = Query(default=False),
    headers_only: bool = Query(default=False),
    include_matrix: bool = Query(default=True),
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

    target_page = _normalize_target_page(page=page, sheet=sheet, sheet_name=sheet_name, default="Market_Leaders")
    effective_limit = int(top_n) if top_n is not None else int(limit)
    normalized_sources = _list_from_any(sources)
    if target_page and (not normalized_sources or normalized_sources == ["ALL"]):
        normalized_sources = [target_page]

    schema_headers, schema_keys = await _get_schema_layout(target_page)
    request_id = _get_request_id(request)

    if target_page == "Data_Dictionary":
        return _build_data_dictionary_sheet_payload(
            request_id=request_id,
            include_matrix=bool(include_matrix),
            schema_only=bool(schema_only),
            headers_only=bool(headers_only),
        )

    if schema_only or headers_only:
        return _build_schema_only_sheet_payload(
            page=target_page,
            request_id=request_id,
            schema_only=bool(schema_only),
            headers_only=bool(headers_only),
            include_matrix=bool(include_matrix),
            headers=schema_headers,
            keys=schema_keys,
        )

    payload: Dict[str, Any] = {
        "page": target_page,
        "sheet": target_page,
        "sheet_name": target_page,
        "sources": normalized_sources,
        "tickers": _list_from_any(symbols),
        "limit": effective_limit,
        "offset": int(offset),
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
        "format": "rows",
        "schema_only": bool(schema_only),
        "headers_only": bool(headers_only),
        "include_matrix": bool(include_matrix),
        "mode": _normalize_mode(mode),
    }

    engine = await _get_engine(request)
    if engine is None:
        raise HTTPException(status_code=status.HTTP_503_SERVICE_UNAVAILABLE, detail="Data engine unavailable")

    timeout_sec = max(5.0, min(180.0, float(os.getenv("ADVISOR_ROUTE_TIMEOUT_SEC", "75") or "75")))
    native_result, native_source, native_error = await _run_native_sheet_rows(
        payload,
        engine=engine,
        timeout_sec=timeout_sec,
    )

    if isinstance(native_result, dict) and _looks_like_sheet_payload(native_result):
        return _normalize_sheet_payload(
            raw_result=native_result,
            target_page=target_page,
            request_id=request_id,
            source=native_source,
            schema_only=bool(schema_only),
            headers_only=bool(headers_only),
            include_matrix=bool(include_matrix),
        )

    shared_pipeline = await _run_shared_page_pipeline(
        request=request,
        payload=payload,
        page=target_page,
        engine=engine,
        timeout_sec=timeout_sec,
    )
    if isinstance(shared_pipeline, dict):
        sheet_rows = await _coerce_to_sheet_rows_response(
            page=target_page,
            advisor_result=shared_pipeline,
            payload=payload,
        )
        meta = sheet_rows.get("meta") if isinstance(sheet_rows.get("meta"), dict) else {}
        meta["native_sheet_rows_attempted"] = True
        meta["native_source"] = native_source
        meta["native_error"] = native_error
        meta["shared_pipeline_used"] = True
        sheet_rows["meta"] = meta
        return sheet_rows

    advisor_result = await _run_advisor(
        request=request,
        payload=payload,
        mode=mode,
        warm_snapshots=bool(warm_snapshots),
        cache_strategy=_clean_str(cache_strategy).lower() or "memory",
        cache_ttl=int(cache_ttl),
        debug=bool(debug),
    )

    sheet_rows = await _coerce_to_sheet_rows_response(
        page=target_page,
        advisor_result=advisor_result,
        payload=payload,
    )
    meta = sheet_rows.get("meta") if isinstance(sheet_rows.get("meta"), dict) else {}
    meta["native_sheet_rows_attempted"] = True
    meta["native_source"] = native_source
    meta["native_error"] = native_error
    meta["shared_pipeline_used"] = False
    sheet_rows["meta"] = meta
    return sheet_rows


@router.post("/sheet-rows")
async def advisor_sheet_rows_post(
    request: Request,
    body: Dict[str, Any] = Body(default_factory=dict),
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

    payload = dict(body or {})
    target_page = _normalize_target_page(
        page=payload.get("page"),
        sheet=payload.get("sheet"),
        sheet_name=payload.get("sheet_name"),
        default="Market_Leaders",
    )
    payload["page"] = target_page
    payload["sheet"] = target_page
    payload["sheet_name"] = target_page
    payload.setdefault("format", "rows")
    payload.setdefault("offset", 0)

    include_matrix = bool(payload.get("include_matrix", True))
    schema_only = bool(payload.get("schema_only", False))
    headers_only = bool(payload.get("headers_only", False))
    schema_headers, schema_keys = await _get_schema_layout(target_page)
    request_id = _get_request_id(request)

    if target_page == "Data_Dictionary":
        return _build_data_dictionary_sheet_payload(
            request_id=request_id,
            include_matrix=include_matrix,
            schema_only=schema_only,
            headers_only=headers_only,
        )

    if schema_only or headers_only:
        return _build_schema_only_sheet_payload(
            page=target_page,
            request_id=request_id,
            schema_only=schema_only,
            headers_only=headers_only,
            include_matrix=include_matrix,
            headers=schema_headers,
            keys=schema_keys,
        )

    engine = await _get_engine(request)
    if engine is None:
        raise HTTPException(status_code=status.HTTP_503_SERVICE_UNAVAILABLE, detail="Data engine unavailable")

    timeout_sec = max(5.0, min(180.0, float(os.getenv("ADVISOR_ROUTE_TIMEOUT_SEC", "75") or "75")))
    native_result, native_source, native_error = await _run_native_sheet_rows(
        payload,
        engine=engine,
        timeout_sec=timeout_sec,
    )

    if isinstance(native_result, dict) and _looks_like_sheet_payload(native_result):
        return _normalize_sheet_payload(
            raw_result=native_result,
            target_page=target_page,
            request_id=request_id,
            source=native_source,
            schema_only=schema_only,
            headers_only=headers_only,
            include_matrix=include_matrix,
        )

    shared_pipeline = await _run_shared_page_pipeline(
        request=request,
        payload=payload,
        page=target_page,
        engine=engine,
        timeout_sec=timeout_sec,
    )
    if isinstance(shared_pipeline, dict):
        sheet_rows = await _coerce_to_sheet_rows_response(
            page=target_page,
            advisor_result=shared_pipeline,
            payload=payload,
        )
        meta = sheet_rows.get("meta") if isinstance(sheet_rows.get("meta"), dict) else {}
        meta["native_sheet_rows_attempted"] = True
        meta["native_source"] = native_source
        meta["native_error"] = native_error
        meta["shared_pipeline_used"] = True
        sheet_rows["meta"] = meta
        return sheet_rows

    advisor_result = await _run_advisor(
        request=request,
        payload=payload,
        mode=mode,
        warm_snapshots=bool(warm_snapshots),
        cache_strategy=_clean_str(cache_strategy).lower() or "memory",
        cache_ttl=int(cache_ttl),
        debug=bool(debug),
    )

    sheet_rows = await _coerce_to_sheet_rows_response(
        page=target_page,
        advisor_result=advisor_result,
        payload=payload,
    )
    meta = sheet_rows.get("meta") if isinstance(sheet_rows.get("meta"), dict) else {}
    meta["native_sheet_rows_attempted"] = True
    meta["native_source"] = native_source
    meta["native_error"] = native_error
    meta["shared_pipeline_used"] = False
    sheet_rows["meta"] = meta
    return sheet_rows


__all__ = ["router", "ADVISOR_ROUTE_VERSION"]
