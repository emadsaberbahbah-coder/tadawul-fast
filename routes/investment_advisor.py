#!/usr/bin/env python3
"""
routes/investment_advisor.py
================================================================================
TFB Investment Advisor Routes — v5.6.0 (PROMETHEUS-SAFE / ENGINE-LAZY / CORE-CONFIG-BRIDGE)
================================================================================

Why this revision:
- ✅ Still **NO prometheus_client metric creation** in this module (prevents duplicate-timeseries crash)
- ✅ Uses core.config as auth/open-mode source of truth (best-effort fallback if missing)
- ✅ Engine-lazy: no heavy imports / no network calls at import-time
- ✅ Uses core/investment_advisor_engine.py as the canonical executor (stable interface)
- ✅ Signature-safe and thread-safe: runs core advisor in a thread to avoid blocking the event loop
- ✅ Backward compatible endpoints:
    /v1/advisor/health
    /v1/advisor/metrics
    /v1/advisor/recommendations   (GET)
    /v1/advisor/run               (POST)

Notes:
- This router delegates the actual strategy/scoring/allocation to:
    core.investment_advisor_engine.run_investment_advisor(payload, engine=engine)
- It only handles: auth, request parsing, engine discovery, response envelope, and lightweight metrics.
"""

from __future__ import annotations

import asyncio
import inspect
import logging
import os
import time
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple, Union

from fastapi import APIRouter, Body, Query, Request

# -----------------------------------------------------------------------------
# JSON response (orjson if available) + NaN safe
# -----------------------------------------------------------------------------
def _clean_nans(obj: Any) -> Any:
    try:
        import math

        if isinstance(obj, dict):
            return {k: _clean_nans(v) for k, v in obj.items()}
        if isinstance(obj, list):
            return [_clean_nans(v) for v in obj]
        if isinstance(obj, float) and (math.isnan(obj) or math.isinf(obj)):
            return None
    except Exception:
        pass
    return obj


try:
    import orjson  # type: ignore
    from fastapi.responses import Response as StarletteResponse

    class BestJSONResponse(StarletteResponse):
        media_type = "application/json"

        def render(self, content: Any) -> bytes:
            return orjson.dumps(_clean_nans(content), default=str)

    def _json_dumps(v: Any) -> str:
        return orjson.dumps(_clean_nans(v), default=str).decode("utf-8")

    _HAS_ORJSON = True
except Exception:
    import json
    from fastapi.responses import JSONResponse as BestJSONResponse  # type: ignore

    def _json_dumps(v: Any) -> str:
        return json.dumps(_clean_nans(v), default=str, ensure_ascii=False)

    _HAS_ORJSON = False


logger = logging.getLogger("routes.investment_advisor")

ROUTER_VERSION = "5.6.0"
router = APIRouter(prefix="/v1/advisor", tags=["advisor"])

_TRUTHY = {"1", "true", "yes", "y", "on", "t", "enabled", "active"}


# -----------------------------------------------------------------------------
# Small in-module metrics (NO prometheus) to keep /metrics endpoint alive
# -----------------------------------------------------------------------------
@dataclass(slots=True)
class _AdvisorMetrics:
    started_at: float
    requests_total: int = 0
    success_total: int = 0
    unauthorized_total: int = 0
    errors_total: int = 0
    last_latency_ms: float = 0.0
    last_error: str = ""

    def to_dict(self) -> Dict[str, Any]:
        up = max(0.0, time.time() - self.started_at)
        return {
            "uptime_sec": round(up, 3),
            "requests_total": self.requests_total,
            "success_total": self.success_total,
            "unauthorized_total": self.unauthorized_total,
            "errors_total": self.errors_total,
            "last_latency_ms": round(float(self.last_latency_ms), 3),
            "last_error": (self.last_error or "")[:600],
        }


_METRICS = _AdvisorMetrics(started_at=time.time())
_METRICS_LOCK = asyncio.Lock()


def _now_utc() -> str:
    return datetime.now(timezone.utc).isoformat()


def _request_id(request: Request) -> str:
    # prefer middleware request_id if present
    try:
        rid = getattr(request.state, "request_id", None)
        if rid:
            return str(rid)
    except Exception:
        pass
    return request.headers.get("X-Request-ID") or str(uuid.uuid4())[:12]


def _split_symbols(raw: str) -> List[str]:
    s = (raw or "").replace("\n", " ").replace("\t", " ").replace(",", " ").strip()
    parts = [p.strip() for p in s.split(" ") if p.strip()]
    out: List[str] = []
    seen = set()
    for p in parts:
        u = p.upper()
        if u and u not in seen:
            seen.add(u)
            out.append(u)
    return out


def _env_bool(name: str, default: bool = False) -> bool:
    try:
        v = (os.getenv(name, str(default)) or "").strip().lower()
        return v in _TRUTHY
    except Exception:
        return default


def _extract_token(request: Request) -> Tuple[Optional[str], Optional[str]]:
    """
    Returns (token, authorization_header).
    token is preferred from X-APP-TOKEN/X-API-KEY or query token when allowed.
    """
    authz = request.headers.get("Authorization")
    token = (
        request.headers.get("X-APP-TOKEN")
        or request.headers.get("X-App-Token")
        or request.headers.get("X-API-KEY")
        or request.headers.get("X-Api-Key")
    )
    # query token is allowed only if core.config/settings says so (best effort)
    allow_q = False
    try:
        from core.config import get_settings_cached  # type: ignore

        st = get_settings_cached()
        allow_q = bool(getattr(st, "allow_query_token", False)) if st else False
    except Exception:
        allow_q = _env_bool("ALLOW_QUERY_TOKEN", False)

    if allow_q and not token:
        qt = request.query_params.get("token")
        if qt:
            token = qt

    # if Authorization: Bearer exists, we still pass authz to core.config
    return (token.strip() if isinstance(token, str) and token.strip() else None, authz)


def _auth_ok(request: Request) -> bool:
    """
    Uses core.config auth (single source of truth).
    Accepts:
      - X-APP-TOKEN / X-API-KEY
      - Authorization: Bearer <token>
      - ?token= (only when ALLOW_QUERY_TOKEN / settings.allow_query_token enabled)
    """
    try:
        from core.config import is_open_mode, auth_ok  # type: ignore

        if callable(is_open_mode) and is_open_mode():
            return True

        token, authz = _extract_token(request)

        if callable(auth_ok):
            return bool(auth_ok(token=token, authorization=authz, headers=dict(request.headers)))
        return False
    except Exception:
        # secure fallback: require auth unless explicitly disabled
        if _env_bool("REQUIRE_AUTH", True):
            return False
        # if auth not required, allow
        return True


async def _maybe_await(v: Any) -> Any:
    return await v if inspect.isawaitable(v) else v


_ENGINE_INIT_LOCK = asyncio.Lock()


async def _get_engine(request: Request) -> Tuple[Optional[Any], str, Optional[str]]:
    """
    Returns (engine, source, error).
    Caches engine in request.app.state.engine when possible.
    """
    # 1) app.state.engine
    try:
        eng = getattr(request.app.state, "engine", None)
        if eng is not None:
            return eng, "app.state.engine", None
    except Exception:
        pass

    async with _ENGINE_INIT_LOCK:
        try:
            eng = getattr(request.app.state, "engine", None)
            if eng is not None:
                return eng, "app.state.engine", None
        except Exception:
            pass

        last_err = None

        # 2) core.data_engine_v2.get_engine
        try:
            from core.data_engine_v2 import get_engine  # type: ignore

            e = get_engine()
            eng = await _maybe_await(e)
            try:
                request.app.state.engine = eng
            except Exception:
                pass
            return eng, "core.data_engine_v2.get_engine", None
        except Exception as e:
            last_err = f"core.data_engine_v2.get_engine: {type(e).__name__}: {e}"

        # 3) fallback core.data_engine.get_engine
        try:
            from core.data_engine import get_engine as get_engine_legacy  # type: ignore

            e = get_engine_legacy()
            eng = await _maybe_await(e)
            try:
                request.app.state.engine = eng
            except Exception:
                pass
            return eng, "core.data_engine.get_engine", None
        except Exception as e:
            last_err = f"{last_err} | core.data_engine.get_engine: {type(e).__name__}: {e}"

        return None, "engine_init_failed", last_err


async def _record_metrics(ok: bool, unauthorized: bool, latency_ms: float, err: str = "") -> None:
    async with _METRICS_LOCK:
        _METRICS.requests_total += 1
        _METRICS.last_latency_ms = float(latency_ms)
        if unauthorized:
            _METRICS.unauthorized_total += 1
        elif ok:
            _METRICS.success_total += 1
        else:
            _METRICS.errors_total += 1
            _METRICS.last_error = (err or "")[:600]


def _error(status_code: int, request_id: str, message: str, *, extra: Optional[Dict[str, Any]] = None) -> BestJSONResponse:
    payload: Dict[str, Any] = {
        "status": "error",
        "error": message,
        "request_id": request_id,
        "timestamp_utc": _now_utc(),
        "version": ROUTER_VERSION,
    }
    if extra:
        payload["meta"] = extra
    return BestJSONResponse(status_code=status_code, content=payload)


async def _run_core_advisor(payload: Dict[str, Any], *, engine: Any, timeout_sec: float) -> Dict[str, Any]:
    """
    Runs core advisor in a worker thread (keeps event loop responsive).
    """
    def _call() -> Dict[str, Any]:
        from core.investment_advisor_engine import run_investment_advisor as run_engine  # type: ignore
        out = run_engine(payload or {}, engine=engine)
        return out if isinstance(out, dict) else {"meta": {"ok": False, "error": "core_return_non_dict"}, "rows": [], "items": [], "headers": []}

    try:
        return await asyncio.wait_for(asyncio.to_thread(_call), timeout=timeout_sec)
    except asyncio.TimeoutError:
        return {
            "headers": [],
            "rows": [],
            "items": [],
            "meta": {"ok": False, "error": "timeout", "timeout_sec": timeout_sec},
        }


def _payload_from_query(
    symbols: str,
    tickers: str,
    *,
    top_n: Optional[int],
    invest_amount: Optional[float],
    allocation_strategy: Optional[str],
    risk_profile: Optional[str],
    debug: bool,
) -> Dict[str, Any]:
    sym_list: List[str] = []
    if symbols:
        sym_list.extend(_split_symbols(symbols))
    if tickers:
        sym_list.extend(_split_symbols(tickers))

    # dedup preserve order
    seen = set()
    sym_list = [s for s in sym_list if not (s in seen or seen.add(s))]

    payload: Dict[str, Any] = {
        "symbols": sym_list,
        "tickers": sym_list,  # keep both for compatibility
        "top_n": top_n,
        "invest_amount": invest_amount,
        "allocation_strategy": allocation_strategy,
        "risk_profile": risk_profile,
        "debug": bool(debug),
    }
    # remove None keys (keeps payload clean)
    return {k: v for k, v in payload.items() if v is not None and v != ""}


# -----------------------------------------------------------------------------
# Endpoints
# -----------------------------------------------------------------------------
@router.get("/health", include_in_schema=False)
async def advisor_health(request: Request) -> Any:
    rid = _request_id(request)
    engine, src, err = await _get_engine(request)
    core_version = None
    try:
        from core.investment_advisor_engine import ENGINE_VERSION  # type: ignore
        core_version = ENGINE_VERSION
    except Exception:
        core_version = None

    return BestJSONResponse(
        status_code=200,
        content=_clean_nans({
            "status": "ok",
            "module": "routes.investment_advisor",
            "version": ROUTER_VERSION,
            "timestamp_utc": _now_utc(),
            "engine": {
                "available": bool(engine),
                "type": (type(engine).__name__ if engine is not None else "none"),
                "source": src,
                "error": err,
            },
            "core_engine_version": core_version,
            "auth": {
                "open_mode": bool(getattr(__import__("core.config", fromlist=["is_open_mode"]), "is_open_mode", lambda: False)()) if "core.config" in globals().get("__builtins__", {}) else None,  # best-effort
                "require_auth_env": _env_bool("REQUIRE_AUTH", True),
                "allow_query_token_env": _env_bool("ALLOW_QUERY_TOKEN", False),
            },
            "request_id": rid,
        }),
    )


@router.get("/metrics", include_in_schema=False)
async def advisor_metrics() -> Any:
    async with _METRICS_LOCK:
        m = _METRICS.to_dict()
    return BestJSONResponse(
        status_code=200,
        content={
            "status": "ok",
            "module": "routes.investment_advisor",
            "version": ROUTER_VERSION,
            "timestamp_utc": _now_utc(),
            "metrics": m,
        },
    )


@router.get("/recommendations")
async def get_recommendations(
    request: Request,
    symbols: str = Query("", description="Comma/space separated symbols"),
    tickers: str = Query("", description="Alias for symbols"),
    top_n: Optional[int] = Query(None, description="Top N recommendations"),
    invest_amount: Optional[float] = Query(None, description="Investment amount (optional)"),
    allocation_strategy: Optional[str] = Query(None, description="Allocation strategy (optional)"),
    risk_profile: Optional[str] = Query(None, description="Risk profile (optional)"),
    debug: bool = Query(False, description="Include debug information on failures"),
) -> BestJSONResponse:
    rid = _request_id(request)
    t0 = time.perf_counter()

    if not _auth_ok(request):
        await _record_metrics(ok=False, unauthorized=True, latency_ms=(time.perf_counter() - t0) * 1000.0, err="unauthorized")
        return _error(401, rid, "unauthorized")

    payload = _payload_from_query(
        symbols,
        tickers,
        top_n=top_n,
        invest_amount=invest_amount,
        allocation_strategy=allocation_strategy,
        risk_profile=risk_profile,
        debug=debug,
    )

    sym_list = payload.get("symbols") or []
    if not sym_list:
        await _record_metrics(ok=False, unauthorized=False, latency_ms=(time.perf_counter() - t0) * 1000.0, err="empty_symbols_list")
        return _error(200, rid, "empty_symbols_list")

    # engine
    engine, src, err = await _get_engine(request)
    if engine is None:
        await _record_metrics(ok=False, unauthorized=False, latency_ms=(time.perf_counter() - t0) * 1000.0, err=err or "no_engine")
        return _error(503, rid, err or "No engine available")

    # timeout (env-configurable)
    timeout_sec = float(os.getenv("ADVISOR_ROUTE_TIMEOUT_SEC", "75") or "75")
    timeout_sec = max(5.0, min(180.0, timeout_sec))

    try:
        result = await _run_core_advisor(payload, engine=engine, timeout_sec=timeout_sec)

        ok = bool((result.get("meta") or {}).get("ok", True)) if isinstance(result, dict) else True
        await _record_metrics(ok=ok, unauthorized=False, latency_ms=(time.perf_counter() - t0) * 1000.0, err=str((result.get("meta") or {}).get("error") or ""))

        # Standard envelope (keeps Apps Script predictable)
        return BestJSONResponse(
            status_code=200,
            content=_clean_nans({
                "status": "ok" if ok else "error",
                "source": src,
                "request_id": rid,
                "timestamp_utc": _now_utc(),
                "version": ROUTER_VERSION,
                "result": result,
            }),
        )

    except Exception as e:
        msg = f"{type(e).__name__}: {e}"
        await _record_metrics(ok=False, unauthorized=False, latency_ms=(time.perf_counter() - t0) * 1000.0, err=msg)
        return _error(200, rid, msg, extra={"source": src})


@router.post("/run")
async def run_advisor(
    request: Request,
    payload: Dict[str, Any] = Body(default_factory=dict),
    debug: bool = Query(False, description="Include debug info on failures"),
) -> BestJSONResponse:
    """
    Canonical run endpoint (preferred by Apps Script).

    Body examples:
      {"symbols":["AAPL","MSFT"], "top_n":20, "invest_amount":50000, "allocation_strategy":"maximum_sharpe"}
      {"symbols":"AAPL,MSFT", "risk_profile":"moderate"}
    """
    rid = _request_id(request)
    t0 = time.perf_counter()

    if not _auth_ok(request):
        await _record_metrics(ok=False, unauthorized=True, latency_ms=(time.perf_counter() - t0) * 1000.0, err="unauthorized")
        return _error(401, rid, "unauthorized")

    p = dict(payload or {})
    # normalize symbols field to list (core supports both, but we keep consistent)
    raw_syms = p.get("symbols") or p.get("tickers") or p.get("symbol") or ""
    sym_list: List[str] = []
    if isinstance(raw_syms, list):
        sym_list = [str(x).strip().upper() for x in raw_syms if str(x).strip()]
    elif isinstance(raw_syms, str):
        sym_list = _split_symbols(raw_syms)

    # dedup preserve order
    seen = set()
    sym_list = [s for s in sym_list if not (s in seen or seen.add(s))]

    if not sym_list:
        await _record_metrics(ok=False, unauthorized=False, latency_ms=(time.perf_counter() - t0) * 1000.0, err="empty_symbols_list")
        return _error(200, rid, "empty_symbols_list")

    p["symbols"] = sym_list
    p["tickers"] = sym_list  # compatibility
    p["debug"] = bool(p.get("debug", False) or debug)

    engine, src, err = await _get_engine(request)
    if engine is None:
        await _record_metrics(ok=False, unauthorized=False, latency_ms=(time.perf_counter() - t0) * 1000.0, err=err or "no_engine")
        return _error(503, rid, err or "No engine available")

    timeout_sec = float(os.getenv("ADVISOR_ROUTE_TIMEOUT_SEC", "75") or "75")
    timeout_sec = max(5.0, min(180.0, timeout_sec))

    try:
        result = await _run_core_advisor(p, engine=engine, timeout_sec=timeout_sec)

        ok = bool((result.get("meta") or {}).get("ok", True)) if isinstance(result, dict) else True
        await _record_metrics(ok=ok, unauthorized=False, latency_ms=(time.perf_counter() - t0) * 1000.0, err=str((result.get("meta") or {}).get("error") or ""))

        return BestJSONResponse(
            status_code=200,
            content=_clean_nans({
                "status": "ok" if ok else "error",
                "source": src,
                "request_id": rid,
                "timestamp_utc": _now_utc(),
                "version": ROUTER_VERSION,
                "result": result,
            }),
        )

    except Exception as e:
        msg = f"{type(e).__name__}: {e}"
        await _record_metrics(ok=False, unauthorized=False, latency_ms=(time.perf_counter() - t0) * 1000.0, err=msg)
        return _error(200, rid, msg, extra={"source": src})


# -----------------------------------------------------------------------------
# Dynamic loader hooks
# -----------------------------------------------------------------------------
def mount(app: Any) -> None:
    try:
        if app is not None and hasattr(app, "include_router"):
            app.include_router(router)
    except Exception:
        pass


def get_router() -> APIRouter:
    return router


__all__ = ["router", "mount", "get_router", "ROUTER_VERSION"]
