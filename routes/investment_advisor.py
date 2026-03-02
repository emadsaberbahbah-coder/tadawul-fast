#!/usr/bin/env python3
"""
routes/investment_advisor.py
================================================================================
TFB Investment Advisor Routes — v5.5.2 (PROMETHEUS-SAFE / ENGINE-LAZY / CORE-CONFIG-BRIDGE)
================================================================================

Fixes your deployment blocker:
- ✅ NO Prometheus metric creation in this module (prevents duplicate-timeseries crash)
  (Your crash came from importing multiple modules that each declared the same metric names.)

Alignment goals:
- ✅ Uses core.config for auth/open-mode (single source of truth)
- ✅ Engine-lazy: no heavy imports / no network calls at import-time
- ✅ Provides router + mount(app) so dynamic loader shows "green"
- ✅ Backward compatible endpoints:
    /v1/advisor/health
    /v1/advisor/metrics
    /v1/advisor/recommendations
    /v1/advisor/run
"""

from __future__ import annotations

import asyncio
import inspect
import logging
import time
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

from fastapi import APIRouter, Body, Query, Request

try:
    from fastapi.responses import ORJSONResponse as BestJSONResponse  # type: ignore
except Exception:
    from starlette.responses import JSONResponse as BestJSONResponse  # type: ignore

logger = logging.getLogger("routes.investment_advisor")

ROUTER_VERSION = "5.5.2"
router = APIRouter(prefix="/v1/advisor", tags=["advisor"])

_TRUTHY = {"1", "true", "yes", "y", "on", "t", "enabled", "active"}


# -----------------------------------------------------------------------------
# Small in-module metrics (NO prometheus) to keep /metrics endpoint alive
# -----------------------------------------------------------------------------
@dataclass
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
            "last_latency_ms": round(self.last_latency_ms, 3),
            "last_error": self.last_error,
        }


_METRICS = _AdvisorMetrics(started_at=time.time())
_METRICS_LOCK = asyncio.Lock()


def _now_utc() -> str:
    return datetime.now(timezone.utc).isoformat()


def _request_id(request: Request) -> str:
    return request.headers.get("X-Request-ID") or str(uuid.uuid4())[:8]


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


def _auth_ok(request: Request) -> bool:
    """
    Uses core.config auth (single source of truth).
    Accepts:
      - X-APP-TOKEN
      - X-API-KEY
      - Authorization: Bearer <token>
    """
    try:
        from core.config import is_open_mode, auth_ok  # type: ignore

        if callable(is_open_mode) and is_open_mode():
            return True

        token = (
            request.headers.get("X-APP-TOKEN")
            or request.headers.get("X-App-Token")
            or request.headers.get("X-API-KEY")
            or request.headers.get("X-Api-Key")
        )
        authz = request.headers.get("Authorization")

        if callable(auth_ok):
            return bool(auth_ok(token=token, authorization=authz, headers=dict(request.headers)))
    except Exception:
        return False

    return False


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


async def _call_best_method(obj: Any, methods: List[str], *args, **kwargs) -> Any:
    """
    Try methods in order; return first successful result.
    """
    last_exc: Optional[Exception] = None
    for name in methods:
        fn = getattr(obj, name, None)
        if not callable(fn):
            continue
        try:
            res = fn(*args, **kwargs)
            return await _maybe_await(res)
        except Exception as e:
            last_exc = e
            continue
    if last_exc:
        raise last_exc
    raise RuntimeError("No compatible advisor method found on engine.")


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


def _error(status_code: int, request_id: str, message: str) -> BestJSONResponse:
    return BestJSONResponse(
        status_code=status_code,
        content={
            "status": "error",
            "error": message,
            "request_id": request_id,
            "timestamp_utc": _now_utc(),
            "version": ROUTER_VERSION,
        },
    )


# -----------------------------------------------------------------------------
# Endpoints
# -----------------------------------------------------------------------------
@router.get("/health", include_in_schema=False)
async def advisor_health() -> Any:
    return {
        "status": "ok",
        "module": "routes.investment_advisor",
        "version": ROUTER_VERSION,
        "timestamp_utc": _now_utc(),
    }


@router.get("/metrics", include_in_schema=False)
async def advisor_metrics() -> Any:
    async with _METRICS_LOCK:
        m = _METRICS.to_dict()
    return {
        "status": "ok",
        "module": "routes.investment_advisor",
        "version": ROUTER_VERSION,
        "timestamp_utc": _now_utc(),
        "metrics": m,
    }


@router.get("/recommendations")
async def get_recommendations(
    request: Request,
    symbols: str = Query("", description="Comma/space separated symbols"),
    tickers: str = Query("", description="Alias for symbols"),
    include_raw: bool = Query(False, description="Include raw provider payload when available"),
    debug: bool = Query(False, description="Include debug information on failures"),
) -> BestJSONResponse:
    rid = _request_id(request)
    t0 = time.perf_counter()

    if not _auth_ok(request):
        await _record_metrics(ok=False, unauthorized=True, latency_ms=(time.perf_counter() - t0) * 1000.0, err="unauthorized")
        return _error(401, rid, "unauthorized")

    sym_list: List[str] = []
    if symbols:
        sym_list.extend(_split_symbols(symbols))
    if tickers:
        sym_list.extend(_split_symbols(tickers))
    # dedup preserve order
    seen = set()
    sym_list = [s for s in sym_list if not (s in seen or seen.add(s))]

    if not sym_list:
        await _record_metrics(ok=False, unauthorized=False, latency_ms=(time.perf_counter() - t0) * 1000.0, err="empty_symbols_list")
        return _error(200, rid, "empty_symbols_list")

    try:
        engine, src, err = await _get_engine(request)
        if engine is None:
            await _record_metrics(ok=False, unauthorized=False, latency_ms=(time.perf_counter() - t0) * 1000.0, err=err or "no_engine")
            return _error(200, rid, err or "No engine available")

        # Try best-known advisor method names
        result = await _call_best_method(
            engine,
            methods=[
                "get_advisor_recommendations",
                "advisor_recommendations",
                "get_recommendations",
                "recommendations",
                "run_advisor",               # some engines compute and return recommendations
                "advisor_run",
            ],
            symbols=sym_list,
            include_raw=bool(include_raw),
        )

        # Normalize output shape
        items = result
        if isinstance(result, dict):
            # common shapes: {"items":[...]} or {"results":[...]}
            for k in ("items", "results", "data", "recommendations"):
                if k in result and isinstance(result[k], list):
                    items = result[k]
                    break

        if not isinstance(items, list):
            items = [result]

        await _record_metrics(ok=True, unauthorized=False, latency_ms=(time.perf_counter() - t0) * 1000.0)

        return BestJSONResponse(
            status_code=200,
            content={
                "status": "ok",
                "count": len(items),
                "items": items,
                "source": src,
                "request_id": rid,
                "timestamp_utc": _now_utc(),
                "version": ROUTER_VERSION,
            },
        )

    except Exception as e:
        msg = f"{type(e).__name__}: {e}"
        await _record_metrics(ok=False, unauthorized=False, latency_ms=(time.perf_counter() - t0) * 1000.0, err=msg)
        payload: Dict[str, Any] = {
            "status": "error",
            "error": msg,
            "request_id": rid,
            "timestamp_utc": _now_utc(),
            "version": ROUTER_VERSION,
        }
        if debug or (os.getenv("DEBUG_ERRORS", "").strip().lower() in _TRUTHY):
            payload["debug"] = {"note": "Enable stack traces in server logs for full traceback."}
        return BestJSONResponse(status_code=200, content=payload)


@router.post("/run")
async def run_advisor(
    request: Request,
    payload: Dict[str, Any] = Body(default_factory=dict),
    debug: bool = Query(False, description="Include debug info on failures"),
) -> BestJSONResponse:
    """
    Flexible advisor run endpoint.
    Expected payload examples:
      {"symbols":["AAPL","MSFT"], "mode":"portfolio", "include_raw":false}
      {"symbols":"AAPL,MSFT", "include_raw":true}
    """
    rid = _request_id(request)
    t0 = time.perf_counter()

    if not _auth_ok(request):
        await _record_metrics(ok=False, unauthorized=True, latency_ms=(time.perf_counter() - t0) * 1000.0, err="unauthorized")
        return _error(401, rid, "unauthorized")

    p = payload or {}
    raw_syms = p.get("symbols") or p.get("tickers") or p.get("symbol") or ""
    include_raw = bool(p.get("include_raw", False))

    sym_list: List[str] = []
    if isinstance(raw_syms, list):
        sym_list = [str(x).strip().upper() for x in raw_syms if str(x).strip()]
    elif isinstance(raw_syms, str):
        sym_list = _split_symbols(raw_syms)

    # optional query params
    if not sym_list:
        await _record_metrics(ok=False, unauthorized=False, latency_ms=(time.perf_counter() - t0) * 1000.0, err="empty_symbols_list")
        return _error(200, rid, "empty_symbols_list")

    try:
        engine, src, err = await _get_engine(request)
        if engine is None:
            await _record_metrics(ok=False, unauthorized=False, latency_ms=(time.perf_counter() - t0) * 1000.0, err=err or "no_engine")
            return _error(200, rid, err or "No engine available")

        # Try advisor runner method names, pass payload when possible
        result = await _call_best_method(
            engine,
            methods=[
                "run_advisor",
                "advisor_run",
                "run_investment_advisor",
                "investment_advisor_run",
                # fallback to recommendations logic
                "get_advisor_recommendations",
                "advisor_recommendations",
            ],
            payload=p,
            symbols=sym_list,
            include_raw=include_raw,
        )

        await _record_metrics(ok=True, unauthorized=False, latency_ms=(time.perf_counter() - t0) * 1000.0)

        return BestJSONResponse(
            status_code=200,
            content={
                "status": "ok",
                "result": result,
                "source": src,
                "request_id": rid,
                "timestamp_utc": _now_utc(),
                "version": ROUTER_VERSION,
            },
        )

    except Exception as e:
        msg = f"{type(e).__name__}: {e}"
        await _record_metrics(ok=False, unauthorized=False, latency_ms=(time.perf_counter() - t0) * 1000.0, err=msg)
        payload_out: Dict[str, Any] = {
            "status": "error",
            "error": msg,
            "request_id": rid,
            "timestamp_utc": _now_utc(),
            "version": ROUTER_VERSION,
        }
        if debug or (os.getenv("DEBUG_ERRORS", "").strip().lower() in _TRUTHY):
            payload_out["debug"] = {"note": "Enable stack traces in server logs for full traceback."}
        return BestJSONResponse(status_code=200, content=payload_out)


# -----------------------------------------------------------------------------
# Dynamic loader hook
# -----------------------------------------------------------------------------
def mount(app: Any) -> None:
    app.include_router(router)


def get_router() -> APIRouter:
    return router


__all__ = ["router", "mount", "get_router", "ROUTER_VERSION"]
