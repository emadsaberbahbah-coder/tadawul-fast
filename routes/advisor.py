#!/usr/bin/env python3
# routes/advisor.py
"""
================================================================================
ADVISOR ROUTER — v5.4.0
(LIVE-BY-DEFAULT / AUTH-SIGNATURE-SAFE / ENGINE-SIGNATURE-TOLERANT)
================================================================================

What this revision fixes
------------------------
- ✅ FIX: auth_ok(...) is now called safely across multiple possible signatures,
         preventing GET /v1/advisor/recommendations from failing with 500
         just because core.config.auth_ok changed shape.
- ✅ FIX: advisor engine execution is now tolerant to sync/async implementations.
- ✅ FIX: advisor engine invocation now tries multiple compatible call signatures:
         payload/body + engine/settings/cache/debug variants.
- ✅ FIX: snapshot warming is optional and safe; snapshot-request failures degrade
         to live_quotes instead of crashing the route.
- ✅ FIX: request payload normalization is stricter:
         - symbols/symbol -> tickers
         - page/sheet/sheet_name -> sources
         - drops empty / None / "None"
- ✅ FIX: result normalization accepts dict / model / list payloads and always
         returns a JSON-safe dict response.
- ✅ SAFE: no network calls at import time.
- ✅ SAFE: health/metrics remain lightweight and startup-safe.

Endpoints
---------
GET  /v1/advisor/health
GET  /v1/advisor/metrics
POST /v1/advisor/run
GET  /v1/advisor/recommendations
================================================================================
"""

from __future__ import annotations

import asyncio
import inspect
import logging
import os
from datetime import datetime, timezone
from typing import Any, Callable, Dict, Iterable, List, Mapping, Optional

from fastapi import APIRouter, Body, Header, HTTPException, Query, Request, Response, status

router = APIRouter(prefix="/v1/advisor", tags=["advisor"])

logger = logging.getLogger("routes.advisor")

ADVISOR_ROUTE_VERSION = "5.4.0"


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


def _truthy(v: Any) -> bool:
    if isinstance(v, bool):
        return v
    s = _clean_str(v).lower()
    return s in {"1", "true", "yes", "y", "on", "t"}


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
            out.extend(
                [
                    "Market_Leaders",
                    "Global_Markets",
                    "Mutual_Funds",
                    "Commodities_FX",
                    "My_Portfolio",
                ]
            )
        else:
            out.append(ss)

    out = _dedupe_keep_order(out)

    out = [
        s
        for s in out
        if s
        and s
        not in {
            "KSA_TADAWUL",
            "Advisor_Criteria",
            "AI_Opportunity_Report",
            "Insights_Analysis",
            "Top_10_Investments",
            "Data_Dictionary",
        }
    ]
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
# Advisor core loader / caller
# ---------------------------------------------------------------------------
def _load_advisor_module() -> Any:
    try:
        import core.investment_advisor_engine as advisor_mod  # type: ignore

        return advisor_mod
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail=f"Advisor engine module unavailable: {type(e).__name__}: {e}",
        )


def _resolve_advisor_runner(module: Any) -> Callable[..., Any]:
    for fn_name in (
        "run_investment_advisor",
        "run_advisor",
        "execute_investment_advisor",
        "execute_advisor",
    ):
        fn = getattr(module, fn_name, None)
        if callable(fn):
            return fn

    raise HTTPException(
        status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
        detail="Advisor engine runner not found",
    )


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
) -> Dict[str, Any]:
    call_attempts = [
        {
            "payload": payload,
            "engine": engine,
            "settings": settings,
            "cache_strategy": cache_strategy,
            "cache_ttl": int(cache_ttl),
            "debug": bool(debug),
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
        },
        {
            "body": payload,
            "engine": engine,
            "cache_strategy": cache_strategy,
            "cache_ttl": int(cache_ttl),
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
            "cache_strategy": cache_strategy,
            "cache_ttl": int(cache_ttl),
            "debug": bool(debug),
        },
    ]

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
        ]
        out = None
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

    advisor_mod = _load_advisor_module()
    runner = _resolve_advisor_runner(advisor_mod)

    eng_for_advisor: Any = engine
    warmed: Any = None
    adapter_used = False

    if use_warm:
        create_engine_adapter = getattr(advisor_mod, "create_engine_adapter", None)
        if callable(create_engine_adapter):
            try:
                adapter = await _call_maybe_async(
                    create_engine_adapter,
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
                    adapter = await _call_maybe_async(create_engine_adapter, engine)
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

    return {
        "status": "ok" if engine else "degraded",
        "version": ADVISOR_ROUTE_VERSION,
        "engine_available": bool(engine),
        "engine_type": _safe_engine_type(engine) if engine else "none",
        "engine_health": engine_health,
        "default_mode": (_clean_str(os.getenv("ADVISOR_DATA_MODE")) or "live_quotes").lower(),
        "require_auth": _safe_bool_env("REQUIRE_AUTH", True),
        "prometheus_available": bool(_PROMETHEUS_AVAILABLE),
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
    invest_amount: float = Query(default=0.0, ge=0.0),
    allocation_strategy: str = Query(default="maximum_sharpe"),
    risk_profile: str = Query(default="moderate"),
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

    payload: Dict[str, Any] = {
        "sources": _list_from_any(sources),
        "page": _clean_str(page),
        "sheet_name": _clean_str(sheet_name),
        "tickers": _list_from_any(symbols),
        "top_n": int(top_n),
        "invest_amount": float(invest_amount),
        "allocation_strategy": _clean_str(allocation_strategy).lower() or "maximum_sharpe",
        "risk_profile": _clean_str(risk_profile).lower() or "moderate",
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


__all__ = ["router"]
