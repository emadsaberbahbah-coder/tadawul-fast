#!/usr/bin/env python3
# routes/routes_argaam.py
"""
================================================================================
Argaam Routes — v2.0.0 (PROMETHEUS-SAFE / STARTUP-SAFE / CORE-CONFIG-ALIGNED)
================================================================================

Why this router exists:
- ✅ Exposes lightweight Argaam debug endpoints (single-symbol + batch) without
     interfering with Phase 3 schema-driven sheet-rows endpoints.
- ✅ Startup-safe: NO network calls at import-time; provider imports are lazy.
- ✅ Prometheus-safe: NO prometheus_client metric creation here (prevents duplicate timeseries).
- ✅ Auth aligned with core.config (best-effort; secure fallback if missing).
- ✅ Works with both provider module layouts:
    - core.providers.argaam_provider
    - providers.argaam_provider

Endpoints:
  GET  /v1/argaam/health        (safe)
  GET  /v1/argaam/metrics       (internal lightweight counters)
  POST /v1/argaam/quote         (single symbol)
  POST /v1/argaam/batch         (batch symbols)

Body examples:
  POST /v1/argaam/quote
    {"symbol":"1120.SR","debug":false}

  POST /v1/argaam/batch
    {"symbols":["1120.SR","2222.SR"],"debug":false,"max_concurrency":10}

Notes:
- This router is NOT used by the engine provider pipeline; the engine calls the provider directly.
- These routes are for diagnostics, QA, and provider contract verification.
================================================================================
"""

from __future__ import annotations

import asyncio
import importlib
import inspect
import logging
import os
import time
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Callable, Dict, List, Optional, Tuple

from fastapi import APIRouter, Body, Header, HTTPException, Query, Request, status
from fastapi.responses import JSONResponse

logger = logging.getLogger("routes.routes_argaam")
logger.addHandler(logging.NullHandler())

ROUTER_VERSION = "2.0.0"
router = APIRouter(prefix="/v1/argaam", tags=["argaam"])

_TRUTHY = {"1", "true", "yes", "y", "on", "t", "enabled", "enable", "active"}


# -----------------------------------------------------------------------------
# Utilities
# -----------------------------------------------------------------------------
def _now_utc() -> str:
    return datetime.now(timezone.utc).isoformat()


def _strip(v: Any) -> str:
    try:
        return str(v).strip()
    except Exception:
        return ""


def _env_bool(name: str, default: bool = False) -> bool:
    try:
        v = (os.getenv(name, str(default)) or "").strip().lower()
        return v in _TRUTHY
    except Exception:
        return default


def _request_id(request: Request, x_request_id: Optional[str]) -> str:
    try:
        rid = getattr(request.state, "request_id", None)
        if rid:
            return str(rid)
    except Exception:
        pass
    return x_request_id or str(uuid.uuid4())[:12]


async def _maybe_await(v: Any) -> Any:
    return await v if inspect.isawaitable(v) else v


def _normalize_symbol_best_effort(symbol: str) -> str:
    s = _strip(symbol)
    if not s:
        return ""
    # Prefer project normalizer if present
    for modpath in ("core.symbols.normalize", "symbols.normalize"):
        try:
            mod = importlib.import_module(modpath)
            fn = getattr(mod, "normalize_symbol", None)
            if callable(fn):
                return _strip(fn(s))
        except Exception:
            continue
    # fallback
    return s.upper()


def _split_symbols(raw: Any) -> List[str]:
    if raw is None:
        return []
    if isinstance(raw, list):
        parts = [_strip(x) for x in raw]
    else:
        s = _strip(raw).replace("\n", " ").replace("\t", " ").replace(",", " ")
        parts = [p.strip() for p in s.split(" ") if p.strip()]
    out: List[str] = []
    seen = set()
    for p in parts:
        u = _normalize_symbol_best_effort(p)
        if u and u not in seen:
            seen.add(u)
            out.append(u)
    return out


# -----------------------------------------------------------------------------
# Auth (core.config aligned; secure fallback)
# -----------------------------------------------------------------------------
def _get_settings_cached() -> Any:
    try:
        from core.config import get_settings_cached  # type: ignore

        return get_settings_cached()
    except Exception:
        return None


def _allow_query_token(settings: Any) -> bool:
    try:
        if settings is not None and hasattr(settings, "allow_query_token"):
            return bool(getattr(settings, "allow_query_token", False))
    except Exception:
        pass
    return _env_bool("ALLOW_QUERY_TOKEN", False)


def _extract_auth_token(
    request: Request,
    *,
    token_q: Optional[str],
    x_app_token: Optional[str],
    authorization: Optional[str],
) -> Tuple[Optional[str], Optional[str]]:
    authz = authorization or request.headers.get("Authorization")
    token = x_app_token or request.headers.get("X-APP-TOKEN") or request.headers.get("X-API-KEY")

    # Bearer token wins
    if authz and authz.strip().lower().startswith("bearer "):
        token = authz.strip().split(" ", 1)[1].strip()

    settings = _get_settings_cached()
    if (not token) and token_q and _allow_query_token(settings):
        token = token_q.strip()

    tok = token.strip() if isinstance(token, str) and token.strip() else None
    return tok, authz


def _auth_ok(
    request: Request,
    *,
    token_q: Optional[str],
    x_app_token: Optional[str],
    authorization: Optional[str],
) -> bool:
    try:
        from core.config import is_open_mode, auth_ok  # type: ignore

        if callable(is_open_mode) and is_open_mode():
            return True

        tok, authz = _extract_auth_token(request, token_q=token_q, x_app_token=x_app_token, authorization=authorization)
        if callable(auth_ok):
            return bool(auth_ok(token=tok, authorization=authz, headers=dict(request.headers)))
        return False
    except Exception:
        # secure fallback
        if _env_bool("REQUIRE_AUTH", True):
            tok, _ = _extract_auth_token(request, token_q=token_q, x_app_token=x_app_token, authorization=authorization)
            return bool(tok)
        return True


# -----------------------------------------------------------------------------
# Internal lightweight metrics (NO prometheus)
# -----------------------------------------------------------------------------
@dataclass(slots=True)
class _RouteMetrics:
    started_at: float
    requests_total: int = 0
    success_total: int = 0
    unauthorized_total: int = 0
    errors_total: int = 0
    last_latency_ms: float = 0.0
    last_error: str = ""
    last_provider_module: str = ""

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
            "last_provider_module": self.last_provider_module or "",
        }


_METRICS = _RouteMetrics(started_at=time.time())
_METRICS_LOCK = asyncio.Lock()


async def _record(ok: bool, unauthorized: bool, latency_ms: float, err: str = "", provider_mod: str = "") -> None:
    async with _METRICS_LOCK:
        _METRICS.requests_total += 1
        _METRICS.last_latency_ms = float(latency_ms)
        if provider_mod:
            _METRICS.last_provider_module = provider_mod
        if unauthorized:
            _METRICS.unauthorized_total += 1
        elif ok:
            _METRICS.success_total += 1
        else:
            _METRICS.errors_total += 1
            _METRICS.last_error = (err or "")[:600]


# -----------------------------------------------------------------------------
# Provider loader (lazy)
# -----------------------------------------------------------------------------
_PROVIDER_CANDIDATES = (
    "core.providers.argaam_provider",
    "providers.argaam_provider",
)


def _import_provider() -> Tuple[Optional[Any], Optional[str], Optional[str]]:
    last_err: Optional[str] = None
    for modpath in _PROVIDER_CANDIDATES:
        try:
            mod = importlib.import_module(modpath)
            return mod, modpath, None
        except Exception as e:
            last_err = f"{modpath}: {type(e).__name__}: {e}"
    return None, None, last_err or "provider_import_failed"


def _pick_callable(mod: Any) -> Optional[Callable]:
    """
    Prefer module-level async functions; fall back to provider instance factories.
    """
    for fn_name in ("fetch_enriched_quote_patch", "fetch_quote_patch", "fetch_patch"):
        fn = getattr(mod, fn_name, None)
        if callable(fn):
            return fn
    return None


async def _call_provider_callable(fn: Callable, symbol: str, debug: bool) -> Dict[str, Any]:
    """
    Calls provider function, supports sync/async.
    """
    try:
        out = fn(symbol, debug=debug)  # most of your providers support debug kw
    except TypeError:
        out = fn(symbol)  # fallback
    out = await _maybe_await(out)
    return out if isinstance(out, dict) else {"symbol": symbol, "error": "provider_return_non_dict"}


async def _call_provider_instance(mod: Any, symbol: str, debug: bool) -> Dict[str, Any]:
    """
    Attempts provider factories:
      get_provider/build_provider/create_provider/provider_factory -> instance
    then calls instance.fetch_enriched_quote_patch / fetch_quote_patch / fetch_patch.
    """
    factories = ("get_provider", "build_provider", "create_provider", "provider_factory")
    inst = None
    for f in factories:
        fn = getattr(mod, f, None)
        if callable(fn):
            try:
                inst = fn()
                break
            except Exception:
                continue

    if inst is None:
        # maybe module exports `provider` instance
        inst = getattr(mod, "provider", None)

    if inst is None:
        return {"symbol": symbol, "error": "no_provider_callable_or_instance"}

    for m in ("fetch_enriched_quote_patch", "fetch_quote_patch", "fetch_patch", "quote", "get_quote"):
        meth = getattr(inst, m, None)
        if callable(meth):
            try:
                out = meth(symbol, debug=debug)
            except TypeError:
                out = meth(symbol)
            out = await _maybe_await(out)
            return out if isinstance(out, dict) else {"symbol": symbol, "error": "provider_instance_return_non_dict"}

    return {"symbol": symbol, "error": "provider_instance_missing_fetch_method"}


async def _fetch_argaam_patch(symbol: str, *, debug: bool = False) -> Tuple[Dict[str, Any], str, Optional[str]]:
    mod, modpath, import_err = _import_provider()
    if mod is None:
        return {"symbol": symbol, "error": import_err or "provider_import_failed", "provider": "argaam"}, "", import_err

    fn = _pick_callable(mod)
    if fn is not None:
        out = await _call_provider_callable(fn, symbol, debug)
        out.setdefault("provider", "argaam")
        return out, modpath or "", None

    out = await _call_provider_instance(mod, symbol, debug)
    out.setdefault("provider", "argaam")
    return out, modpath or "", None


# -----------------------------------------------------------------------------
# Routes
# -----------------------------------------------------------------------------
@router.get("/health", include_in_schema=False)
async def argaam_health(request: Request) -> JSONResponse:
    rid = _request_id(request, request.headers.get("X-Request-ID"))
    mod, modpath, import_err = _import_provider()
    return JSONResponse(
        status_code=200,
        content={
            "status": "ok" if mod is not None else "degraded",
            "module": "routes.routes_argaam",
            "version": ROUTER_VERSION,
            "timestamp_utc": _now_utc(),
            "provider": {
                "available": bool(mod is not None),
                "module": modpath,
                "error": import_err,
                "candidates": list(_PROVIDER_CANDIDATES),
            },
            "auth": {
                "require_auth_env": _env_bool("REQUIRE_AUTH", True),
                "allow_query_token_env": _env_bool("ALLOW_QUERY_TOKEN", False),
            },
            "request_id": rid,
        },
    )


@router.get("/metrics", include_in_schema=False)
async def argaam_metrics() -> JSONResponse:
    async with _METRICS_LOCK:
        m = _METRICS.to_dict()
    return JSONResponse(
        status_code=200,
        content={
            "status": "ok",
            "module": "routes.routes_argaam",
            "version": ROUTER_VERSION,
            "timestamp_utc": _now_utc(),
            "metrics": m,
        },
    )


@router.post("/quote")
async def argaam_quote(
    request: Request,
    body: Dict[str, Any] = Body(default_factory=dict),
    token: Optional[str] = Query(default=None, description="Auth token (query only if allowed)"),
    debug: bool = Query(default=False, description="Include debug information (best-effort)"),
    x_app_token: Optional[str] = Header(default=None, alias="X-APP-TOKEN"),
    authorization: Optional[str] = Header(default=None, alias="Authorization"),
    x_request_id: Optional[str] = Header(default=None, alias="X-Request-ID"),
) -> JSONResponse:
    rid = _request_id(request, x_request_id)
    t0 = time.perf_counter()

    if not _auth_ok(request, token_q=token, x_app_token=x_app_token, authorization=authorization):
        await _record(ok=False, unauthorized=True, latency_ms=(time.perf_counter() - t0) * 1000.0, err="unauthorized")
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="unauthorized")

    sym_raw = body.get("symbol") or body.get("ticker") or body.get("requested_symbol") or ""
    sym = _normalize_symbol_best_effort(sym_raw)
    if not sym:
        await _record(ok=False, unauthorized=False, latency_ms=(time.perf_counter() - t0) * 1000.0, err="invalid_symbol")
        return JSONResponse(
            status_code=400,
            content={
                "status": "error",
                "error": "invalid_symbol",
                "request_id": rid,
                "timestamp_utc": _now_utc(),
                "version": ROUTER_VERSION,
            },
        )

    # body debug can override query debug
    dbg = bool(body.get("debug", False) or debug)

    out, modpath, import_err = await _fetch_argaam_patch(sym, debug=dbg)
    ok = isinstance(out, dict) and not out.get("error")

    await _record(
        ok=ok,
        unauthorized=False,
        latency_ms=(time.perf_counter() - t0) * 1000.0,
        err=str(out.get("error") or import_err or ""),
        provider_mod=modpath or "",
    )

    return JSONResponse(
        status_code=200,
        content={
            "status": "success" if ok else "error",
            "symbol": sym,
            "provider_module": modpath,
            "request_id": rid,
            "timestamp_utc": _now_utc(),
            "version": ROUTER_VERSION,
            "data": out,
        },
    )


@router.post("/batch")
async def argaam_batch(
    request: Request,
    body: Dict[str, Any] = Body(default_factory=dict),
    token: Optional[str] = Query(default=None, description="Auth token (query only if allowed)"),
    debug: bool = Query(default=False, description="Include debug information (best-effort)"),
    max_concurrency: int = Query(default=10, ge=1, le=50, description="Max concurrent provider calls"),
    x_app_token: Optional[str] = Header(default=None, alias="X-APP-TOKEN"),
    authorization: Optional[str] = Header(default=None, alias="Authorization"),
    x_request_id: Optional[str] = Header(default=None, alias="X-Request-ID"),
) -> JSONResponse:
    rid = _request_id(request, x_request_id)
    t0 = time.perf_counter()

    if not _auth_ok(request, token_q=token, x_app_token=x_app_token, authorization=authorization):
        await _record(ok=False, unauthorized=True, latency_ms=(time.perf_counter() - t0) * 1000.0, err="unauthorized")
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="unauthorized")

    # Accept symbols/tickers in multiple shapes
    syms = _split_symbols(body.get("symbols") or body.get("tickers") or body.get("symbol_list") or "")
    top_n = int(body.get("top_n") or len(syms) or 50)
    top_n = max(1, min(2000, top_n))
    syms = syms[:top_n]

    if not syms:
        await _record(ok=False, unauthorized=False, latency_ms=(time.perf_counter() - t0) * 1000.0, err="empty_symbols")
        return JSONResponse(
            status_code=200,
            content={
                "status": "skipped",
                "error": "empty_symbols",
                "request_id": rid,
                "timestamp_utc": _now_utc(),
                "version": ROUTER_VERSION,
                "rows": [],
                "items": {},
            },
        )

    dbg = bool(body.get("debug", False) or debug)
    mc = int(body.get("max_concurrency") or max_concurrency or 10)
    mc = max(1, min(50, mc))
    sem = asyncio.Semaphore(mc)

    async def _one(s: str) -> Tuple[str, Dict[str, Any], str, Optional[str]]:
        async with sem:
            out, modpath, err = await _fetch_argaam_patch(s, debug=dbg)
            return s, out, modpath, err

    results = await asyncio.gather(*[_one(s) for s in syms], return_exceptions=True)

    items: Dict[str, Any] = {}
    errors = 0
    provider_mod = ""
    for r in results:
        if isinstance(r, Exception):
            errors += 1
            continue
        sym, out, modpath, _err = r
        provider_mod = provider_mod or (modpath or "")
        items[sym] = out
        if isinstance(out, dict) and out.get("error"):
            errors += 1

    ok = errors == 0
    await _record(
        ok=ok,
        unauthorized=False,
        latency_ms=(time.perf_counter() - t0) * 1000.0,
        err=f"{errors} errors" if errors else "",
        provider_mod=provider_mod,
    )

    return JSONResponse(
        status_code=200,
        content={
            "status": "success" if ok else ("partial" if errors < len(syms) else "error"),
            "count": len(syms),
            "errors": errors,
            "provider_module": provider_mod,
            "request_id": rid,
            "timestamp_utc": _now_utc(),
            "version": ROUTER_VERSION,
            "items": items,  # {symbol: provider_patch}
        },
    )


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
