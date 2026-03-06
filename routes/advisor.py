#!/usr/bin/env python3
# routes/advisor.py
"""
================================================================================
ADVISOR ROUTER — v5.3.0 (LIVE-BY-DEFAULT / SNAPSHOT-WARM OPTIONAL / KEY-CONSISTENT)
================================================================================

Fixes (your points)
1) ✅ Advisor empty because snapshots cache is empty
   - Default behavior is now LIVE (no snapshots required):
       advisor_data_mode = "live_quotes"  (default)
   - Snapshot mode is still supported, and can warm snapshots on demand.

2) ✅ Route wiring consistency (prevents “Unknown page 'None'” / body mismatch)
   - Normalizes and sanitizes "page"/"sheet"/"sheet_name"/"sources"
   - Drops empty / None / "None" values before passing to advisor core
   - Normalizes "symbols" -> "tickers" (advisor expects tickers)

Endpoints
- GET  /v1/advisor/health
- GET  /v1/advisor/metrics        (optional Prometheus)
- POST /v1/advisor/run            (primary)
- GET  /v1/advisor/recommendations (convenience wrapper)

Notes
- Startup-safe: no network calls at import time.
- Auth best-effort: uses core.config.auth_ok if available.
================================================================================
"""

from __future__ import annotations

import logging
import os
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, Body, Header, HTTPException, Query, Request, Response, status

router = APIRouter(prefix="/v1/advisor", tags=["advisor"])

logger = logging.getLogger("routes.advisor")

ADVISOR_ROUTE_VERSION = "5.3.0"

# ---------------------------------------------------------------------------
# Optional Prometheus (safe)
# ---------------------------------------------------------------------------
try:
    from prometheus_client import generate_latest, CONTENT_TYPE_LATEST  # type: ignore

    _PROMETHEUS_AVAILABLE = True
except Exception:
    generate_latest = None  # type: ignore
    CONTENT_TYPE_LATEST = "text/plain"
    _PROMETHEUS_AVAILABLE = False

# ---------------------------------------------------------------------------
# core.config preferred (auth + settings); router remains safe if unavailable
# ---------------------------------------------------------------------------
try:
    from core.config import auth_ok, get_settings_cached  # type: ignore
except Exception:
    auth_ok = None  # type: ignore

    def get_settings_cached(*args: Any, **kwargs: Any) -> Any:  # type: ignore
        return None


# ---------------------------------------------------------------------------
# Engine accessor (lazy + safe)
# ---------------------------------------------------------------------------
async def _get_engine(request: Request) -> Optional[Any]:
    # Prefer app.state.engine (set by main.py lifespan)
    try:
        st = getattr(request.app, "state", None)
        if st and getattr(st, "engine", None):
            return st.engine
    except Exception:
        pass

    # Fallback to core.data_engine_v2.get_engine()
    try:
        from core.data_engine_v2 import get_engine  # type: ignore

        eng = get_engine()
        if hasattr(eng, "__await__"):
            eng = await eng
        return eng
    except Exception:
        return None


def _safe_engine_type(engine: Any) -> str:
    try:
        return type(engine).__name__
    except Exception:
        return "unknown"


def _safe_bool_env(name: str, default: bool = False) -> bool:
    try:
        v = (os.getenv(name, str(default)) or "").strip().lower()
        return v in ("1", "true", "yes", "y", "on", "t")
    except Exception:
        return default


# ---------------------------------------------------------------------------
# Auth helper (best-effort)
# ---------------------------------------------------------------------------
def _clean_str(v: Any) -> str:
    if v is None:
        return ""
    try:
        s = str(v).strip()
    except Exception:
        return ""
    if not s:
        return ""
    if s.strip().lower() in {"none", "null", "nil"}:
        return ""
    return s


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


def _require_auth_or_401(
    *,
    token_query: Optional[str],
    x_app_token: Optional[str],
    authorization: Optional[str],
) -> None:
    # If auth_ok is not available, do not block.
    if auth_ok is None:
        return

    auth_token = _extract_auth_token(token_query=token_query, x_app_token=x_app_token, authorization=authorization)
    if not auth_ok(
        token=auth_token,
        authorization=authorization,
        headers={"X-APP-TOKEN": x_app_token, "Authorization": authorization},
    ):
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid token")


# ---------------------------------------------------------------------------
# Request helpers (key consistency)
# ---------------------------------------------------------------------------
def _truthy(v: Any) -> bool:
    if isinstance(v, bool):
        return v
    s = _clean_str(v).lower()
    return s in {"1", "true", "yes", "y", "on", "t"}


def _list_from_any(v: Any) -> List[str]:
    if v is None:
        return []
    if isinstance(v, list):
        out = []
        for x in v:
            sx = _clean_str(x)
            if sx:
                out.append(sx)
        return out
    if isinstance(v, str):
        s = v.replace(",", " ")
        return [x.strip() for x in s.split() if _clean_str(x)]
    sx = _clean_str(v)
    return [sx] if sx else []


def _normalize_sources(
    *,
    sources_in: Any,
    page: Any = None,
    sheet: Any = None,
    sheet_name: Any = None,
) -> List[str]:
    """
    Normalizes *and sanitizes* sources/page/sheet/sheet_name, preventing "None".
    - If page/sheet/sheet_name provided, it is treated as a single requested page.
    - If "ALL" is used, it expands to default pages.
    """
    page_s = _clean_str(page) or _clean_str(sheet) or _clean_str(sheet_name)
    sources = _list_from_any(sources_in)

    # If caller provided explicit page/sheet_name and didn't provide sources, use it.
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
            out.extend(["Market_Leaders", "Global_Markets", "Mutual_Funds", "Commodities_FX", "My_Portfolio"])
        else:
            out.append(ss)

    # Unique preserve order
    seen = set()
    uniq: List[str] = []
    for s in out:
        if s and s not in seen:
            seen.add(s)
            uniq.append(s)

    # Hard excludes (project constraints)
    uniq = [
        s
        for s in uniq
        if s
        and s not in {
            "KSA_TADAWUL",
            "Advisor_Criteria",
            "AI_Opportunity_Report",
            "Insights_Analysis",
            "Top_10_Investments",
            "Data_Dictionary",
        }
    ]
    return uniq


def _normalize_payload_keys(payload: Dict[str, Any]) -> Dict[str, Any]:
    """
    Prevents body key mismatches:
    - symbols/symbol -> tickers
    - page/sheet/sheet_name -> sources (if sources missing)
    - strips None/"None"
    """
    p = dict(payload or {})

    # normalize tickers
    if "tickers" not in p or not _list_from_any(p.get("tickers")):
        sym_in = p.get("symbols")
        if sym_in is None:
            sym_in = p.get("symbol")
        if sym_in is not None:
            p["tickers"] = _list_from_any(sym_in)

    # normalize sources
    sources = _normalize_sources(
        sources_in=p.get("sources"),
        page=p.get("page"),
        sheet=p.get("sheet"),
        sheet_name=p.get("sheet_name"),
    )
    p["sources"] = sources

    # remove the confusing keys (optional)
    for k in ("page", "sheet", "sheet_name"):
        if k in p and not _clean_str(p.get(k)):
            p.pop(k, None)

    # Clean mode keys
    if "mode" in p and not _clean_str(p.get("mode")):
        p.pop("mode", None)
    if "data_mode" in p and not _clean_str(p.get("data_mode")):
        p.pop("data_mode", None)
    if "advisor_data_mode" in p and not _clean_str(p.get("advisor_data_mode")):
        p.pop("advisor_data_mode", None)

    return p


def _normalize_mode(mode: str) -> str:
    m = _clean_str(mode).lower()
    if not m or m == "auto":
        return ""
    # accepted: snapshot, live_sheet, live_quotes
    if m in {"snapshot", "snapshots"}:
        return "snapshot"
    if m in {"live", "live_quotes", "quotes"}:
        return "live_quotes"
    if m in {"live_sheet", "sheet"}:
        return "live_sheet"
    return m


def _force_default_live_mode(payload: Dict[str, Any], *, mode_override: str = "") -> Dict[str, Any]:
    """
    Force LIVE by default:
      advisor_data_mode := mode_override OR payload.advisor_data_mode/data_mode/mode OR env default OR "live_quotes"
    """
    p = dict(payload or {})
    m = _normalize_mode(mode_override)

    if m:
        p["advisor_data_mode"] = m
        return p

    # If caller already set it, keep it (but normalize)
    existing = _clean_str(p.get("advisor_data_mode") or p.get("data_mode") or p.get("mode"))
    if existing:
        p["advisor_data_mode"] = _normalize_mode(existing) or existing.strip().lower()
        return p

    p["advisor_data_mode"] = (_clean_str(os.getenv("ADVISOR_DATA_MODE")) or "live_quotes").lower()
    return p


# ---------------------------------------------------------------------------
# Core runner (live by default + optional warm snapshots)
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
    engine = await _get_engine(request)
    if engine is None:
        raise HTTPException(status_code=status.HTTP_503_SERVICE_UNAVAILABLE, detail="Data engine unavailable")

    # sanitize + key-normalize first (prevents page None)
    payload0 = _normalize_payload_keys(payload or {})

    # enforce default live mode unless explicitly overridden
    payload2 = _force_default_live_mode(payload0, mode_override=mode)

    advisor_mode = _clean_str(payload2.get("advisor_data_mode")).lower()
    wants_snapshot = advisor_mode == "snapshot"
    use_warm = bool(warm_snapshots or wants_snapshot)

    eng_for_advisor: Any = engine
    warmed: Optional[Dict[str, Any]] = None
    adapter_used = False

    if use_warm:
        try:
            from core.investment_advisor_engine import create_engine_adapter  # type: ignore

            adapter = create_engine_adapter(engine, cache_strategy=cache_strategy, cache_ttl=int(cache_ttl))
            adapter_used = True
            try:
                warmed = adapter.warm_cache(list(payload2.get("sources") or []))
            except Exception:
                warmed = None
            eng_for_advisor = adapter

            # If snapshot was requested, keep it. Otherwise keep live mode.
            if wants_snapshot:
                payload2["advisor_data_mode"] = "snapshot"
        except Exception:
            # adapter failed => never break; if snapshot requested, fall back to live
            adapter_used = False
            eng_for_advisor = engine
            warmed = None
            if wants_snapshot:
                payload2["advisor_data_mode"] = "live_quotes"

    # run advisor engine wrapper (sync function)
    try:
        from core.investment_advisor_engine import run_investment_advisor as run_engine  # type: ignore
    except Exception:
        from core import investment_advisor_engine as _iae  # type: ignore

        run_engine = _iae.run_investment_advisor  # type: ignore

    result = run_engine(
        payload2,
        engine=eng_for_advisor,
        cache_strategy=cache_strategy,
        cache_ttl=int(cache_ttl),
        debug=bool(debug),
    )

    if not isinstance(result, dict):
        raise HTTPException(status_code=500, detail="Advisor engine returned invalid response")

    meta = result.get("meta") if isinstance(result.get("meta"), dict) else {}
    meta.update(
        {
            "route_version": ADVISOR_ROUTE_VERSION,
            "engine_type": _safe_engine_type(engine),
            "advisor_data_mode_effective": _clean_str(payload2.get("advisor_data_mode")),
            "warm_snapshots": bool(use_warm),
            "adapter_used": bool(adapter_used),
            "warm_results": warmed,
        }
    )
    result["meta"] = meta
    return result


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
                    if hasattr(out, "__await__"):
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
    # Consistency aliases to prevent page None mismatch
    page: Optional[str] = Query(default=None, description="Single page alias (same as sheet_name)"),
    sheet_name: Optional[str] = Query(default=None, description="Single page alias"),
    # Mode wiring
    mode: str = Query(default="", description="snapshot | live_sheet | live_quotes | auto (default: live_quotes)"),
    warm_snapshots: bool = Query(default=False, description="Warm snapshots before running (helps snapshot mode)"),
    cache_strategy: str = Query(default="memory", description="memory | none"),
    cache_ttl: int = Query(default=600, ge=30, le=86400, description="Snapshot cache TTL for adapter (seconds)"),
    debug: bool = Query(default=False, description="Include debug metadata where available"),
    # Auth
    token: Optional[str] = Query(default=None, description="Auth token (query only if allowed)"),
    x_app_token: Optional[str] = Header(default=None, alias="X-APP-TOKEN"),
    authorization: Optional[str] = Header(default=None, alias="Authorization"),
) -> Dict[str, Any]:
    _require_auth_or_401(token_query=token, x_app_token=x_app_token, authorization=authorization)

    payload = dict(body or {})

    # Apply query aliases only if body doesn't already define them (and avoid None/"None")
    if _clean_str(page) and not _clean_str(payload.get("page")) and not _clean_str(payload.get("sources")):
        payload["page"] = _clean_str(page)
    if _clean_str(sheet_name) and not _clean_str(payload.get("sheet_name")) and not _clean_str(payload.get("sources")):
        payload["sheet_name"] = _clean_str(sheet_name)

    # normalize common symbol keys to what advisor expects
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
    # Convenience query parameters
    symbols: Optional[str] = Query(default=None, description="Comma/space separated symbols"),
    sources: Optional[str] = Query(default="ALL", description="ALL or comma-separated pages"),
    # Consistency aliases (some clients send page/sheet_name)
    page: Optional[str] = Query(default=None, description="Single page alias"),
    sheet_name: Optional[str] = Query(default=None, description="Single page alias"),
    top_n: int = Query(default=20, ge=1, le=200),
    invest_amount: float = Query(default=0.0, ge=0.0),
    allocation_strategy: str = Query(default="maximum_sharpe"),
    risk_profile: str = Query(default="moderate"),
    # Mode wiring
    mode: str = Query(default="", description="snapshot | live_sheet | live_quotes | auto (default: live_quotes)"),
    warm_snapshots: bool = Query(default=False),
    cache_strategy: str = Query(default="memory"),
    cache_ttl: int = Query(default=600, ge=30, le=86400),
    debug: bool = Query(default=False),
    # Auth
    token: Optional[str] = Query(default=None, description="Auth token (query only if allowed)"),
    x_app_token: Optional[str] = Header(default=None, alias="X-APP-TOKEN"),
    authorization: Optional[str] = Header(default=None, alias="Authorization"),
) -> Dict[str, Any]:
    _require_auth_or_401(token_query=token, x_app_token=x_app_token, authorization=authorization)

    payload: Dict[str, Any] = {
        "sources": _list_from_any(sources),
        "page": _clean_str(page),           # will be sanitized + merged properly
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
