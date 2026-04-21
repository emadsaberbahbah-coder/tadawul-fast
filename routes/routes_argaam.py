#!/usr/bin/env python3
# routes/routes_argaam.py
"""
================================================================================
Argaam Routes — v2.1.0 (FLEXIBLE-AUTH / ASYNC-PROVIDER-SAFE / CORE-CONFIG-ALIGNED)
================================================================================

Canonical-plan note (read first)
--------------------------------
This router is an OPTIONAL diagnostic route module in the project:
- Per `main._OPTIONAL_ROUTE_MODULES`, `routes.routes_argaam` is in the
  optional set — it mounts if importable, is skipped silently otherwise.
- It is NOT in `main._CONTROLLED_ROUTE_PLAN` (which owns config, schema,
  analysis, advanced, advisor, enriched families).
- It is NOT in `main._allowed_prefixes_for_key` (which means
  `_clone_filtered_router` does NOT restrict its prefixes — the `/v1/argaam/*`
  family passes through unfiltered).
- None of its paths appear in `main._CONTROLLED_CANONICAL_OWNER_MAP`, so
  there is no owner arbitration and no protected-prefix collision risk.

What this router does
---------------------
- Exposes lightweight Argaam debug endpoints (single-symbol + batch) without
  interfering with Phase 3 schema-driven sheet-rows endpoints.
- Startup-safe: NO network calls at import-time; provider imports are lazy.
- Prometheus-safe: NO prometheus_client metric creation here (prevents
  duplicate timeseries).
- Auth aligned with core.config via the project-wide flexible dispatch.
- Works with both provider module layouts:
    - core.providers.argaam_provider
    - providers.argaam_provider

Endpoints
---------
- GET  /v1/argaam/health    (diagnostic)
- GET  /v1/argaam/metrics   (internal lightweight counters)
- POST /v1/argaam/quote     (single symbol)
- POST /v1/argaam/batch     (batch symbols)

Why this revision (v2.1.0 vs v2.0.0)
------------------------------------
- FIX: `_auth_ok` now uses the project-wide 6-level flexible-dispatch pattern.
       Modern `core.config.auth_ok` accepts
       (token, authorization, headers, api_key, path, request, settings, **_);
       v2.0.0 only tried a 3-kwarg signature, so on TypeError the broad
       `except Exception` returned False — breaking auth entirely on modern
       `core.config`. Now matches `routes.investment_advisor v5.2.0`,
       `routes.enriched_quote v8.5.0`, `routes.analysis_sheet_rows v4.1.0`,
       `routes.data_dictionary v2.7.0`, `routes.config v5.9.0`.
- FIX: every POST endpoint now accepts `x_api_key: Optional[str] = Header(...,
       alias="X-API-Key")` and threads it as `api_key=` on the richest
       `auth_ok` attempt. v2.0.0 only read X-API-Key from raw headers and
       never passed it as an explicit kwarg to auth.
- FIX: `_call_provider_instance` now awaits `get_provider()` when it returns
       a coroutine. The canonical `core.providers.argaam_provider v4.4.0`
       exports `async def get_provider() -> ArgaamProvider` — v2.0.0 called
       it synchronously, captured the coroutine object, then failed on
       `.fetch_enriched_quote_patch` lookup and returned
       "provider_instance_missing_fetch_method". Functional bug, now fixed.
- FIX: `_TRUTHY` and `_FALSY` realigned to exact project-wide vocabulary from
       `main._TRUTHY` / `main._FALSY`. v2.0.0 had `"active"` as an outlier
       and no `_FALSY` set.
- FIX: `core.config.auth_ok` / `is_open_mode` / `get_settings_cached` are now
       captured at module load instead of re-imported inside every call.
       Matches the pattern used by every other revised router.
- FIX: `/health` and `/metrics` now expose `route_owner`, `route_family`,
       `capabilities` dict (auth_ok_callable / is_open_mode_callable /
       get_settings_cached_callable / provider_importable), `owned_paths`
       and `request_id`.
- FIX: provider importability is now cached after first success, with retry
       on failure. Previous v2.0.0 re-probed every call.
- KEEP: all 4 endpoints (`/health`, `/metrics`, `/quote`, `/batch`) with
       identical paths and behavior contracts.
- KEEP: bounded batch concurrency via `asyncio.Semaphore`.
- KEEP: safe fallback for sync-vs-async provider calls with TypeError retry
       for the `debug=` kwarg.
- KEEP: `mount(app)` and `get_router()` helper exports for dynamic loaders.
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

ROUTER_VERSION = "2.1.0"
ROUTE_OWNER_NAME = "routes_argaam"
ROUTE_FAMILY_NAME = "argaam"

# Project-wide truthy/falsy vocabulary — matches main._TRUTHY / main._FALSY.
_TRUTHY = {"1", "true", "yes", "y", "on", "t", "enabled", "enable"}
_FALSY = {"0", "false", "no", "n", "off", "f", "disabled", "disable"}

router = APIRouter(prefix="/v1/argaam", tags=["argaam"])


# =============================================================================
# Module-level core.config capture (lazy once, not per-request)
# =============================================================================
try:
    from core.config import auth_ok as _core_auth_ok  # type: ignore
except Exception:
    _core_auth_ok = None  # type: ignore

try:
    from core.config import is_open_mode as _core_is_open_mode  # type: ignore
except Exception:
    _core_is_open_mode = None  # type: ignore

try:
    from core.config import get_settings_cached as _core_get_settings_cached  # type: ignore
except Exception:
    def _core_get_settings_cached(*args: Any, **kwargs: Any) -> Any:  # type: ignore
        return None


# =============================================================================
# Utilities
# =============================================================================
def _now_utc() -> str:
    return datetime.now(timezone.utc).isoformat()


def _strip(v: Any) -> str:
    try:
        return str(v).strip()
    except Exception:
        return ""


def _env_bool(name: str, default: bool = False) -> bool:
    try:
        v = (os.getenv(name, "") or "").strip().lower()
        if not v:
            return bool(default)
        if v in _TRUTHY:
            return True
        if v in _FALSY:
            return False
        return bool(default)
    except Exception:
        return bool(default)


def _env_int(name: str, default: int) -> int:
    try:
        raw = (os.getenv(name, "") or "").strip()
        return int(float(raw)) if raw else int(default)
    except Exception:
        return int(default)


def _request_id(request: Request, x_request_id: Optional[str]) -> str:
    # Prefer RequestIDMiddleware-set value, then explicit header, then a UUID.
    try:
        rid = getattr(request.state, "request_id", None)
        if rid:
            return str(rid)
    except Exception:
        pass
    if x_request_id:
        return str(x_request_id)
    try:
        hdr = request.headers.get("X-Request-ID")
        if hdr:
            return str(hdr)
    except Exception:
        pass
    return uuid.uuid4().hex[:12]


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
                out = _strip(fn(s))
                if out:
                    return out
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
        s = _strip(raw).replace("\n", " ").replace("\t", " ").replace(",", " ").replace(";", " ").replace("|", " ")
        parts = [p.strip() for p in s.split(" ") if p.strip()]
    out: List[str] = []
    seen = set()
    for p in parts:
        u = _normalize_symbol_best_effort(p)
        if u and u not in seen:
            seen.add(u)
            out.append(u)
    return out


# =============================================================================
# Auth (project-wide flexible 6-level dispatch)
# =============================================================================
def _allow_query_token(settings: Any) -> bool:
    try:
        if settings is not None:
            for attr in ("allow_query_token", "ALLOW_QUERY_TOKEN"):
                if hasattr(settings, attr):
                    return bool(getattr(settings, attr, False))
    except Exception:
        pass
    return _env_bool("ALLOW_QUERY_TOKEN", False)


def _extract_auth_token(
    request: Request,
    *,
    token_q: Optional[str],
    x_app_token: Optional[str],
    x_api_key: Optional[str],
    authorization: Optional[str],
) -> Tuple[Optional[str], Optional[str]]:
    try:
        authz = authorization or request.headers.get("Authorization")
    except Exception:
        authz = authorization

    header_app_token = None
    header_api_key = None
    try:
        header_app_token = request.headers.get("X-APP-TOKEN")
        # Check both capitalizations — HTTP headers are case-insensitive but
        # raw dict-style headers inside test harnesses often aren't.
        header_api_key = request.headers.get("X-API-Key") or request.headers.get("X-API-KEY")
    except Exception:
        pass

    token = (x_app_token or header_app_token or x_api_key or header_api_key or "") or ""
    token = token.strip() if isinstance(token, str) else ""

    # Bearer token in Authorization wins
    if authz and isinstance(authz, str) and authz.strip().lower().startswith("bearer "):
        token = authz.strip().split(" ", 1)[1].strip()

    try:
        settings = _core_get_settings_cached()
    except Exception:
        settings = None

    if (not token) and token_q and _allow_query_token(settings):
        token = token_q.strip()

    return (token or None), authz


def _auth_ok_flexible(
    request: Request,
    *,
    token_q: Optional[str],
    x_app_token: Optional[str],
    x_api_key: Optional[str],
    authorization: Optional[str],
) -> bool:
    """
    Flexible multi-signature dispatch for `core.config.auth_ok` — matches
    main._call_auth_ok_flexible / routes.investment_advisor v5.2.0 /
    routes.enriched_quote v8.5.0. Starts with the richest signature
    (path + request + settings + api_key) and degrades to {token} only.
    """
    # Open-mode short-circuit
    try:
        if callable(_core_is_open_mode) and bool(_core_is_open_mode()):
            return True
    except Exception:
        pass

    # If core.config not importable, fall back to env-driven behavior
    if _core_auth_ok is None or not callable(_core_auth_ok):
        if not _env_bool("REQUIRE_AUTH", True):
            return True
        tok, _ = _extract_auth_token(
            request,
            token_q=token_q,
            x_app_token=x_app_token,
            x_api_key=x_api_key,
            authorization=authorization,
        )
        return bool(tok)

    tok, authz = _extract_auth_token(
        request,
        token_q=token_q,
        x_app_token=x_app_token,
        x_api_key=x_api_key,
        authorization=authorization,
    )

    try:
        headers_dict = dict(request.headers)
    except Exception:
        headers_dict = {}
    if x_app_token:
        headers_dict.setdefault("X-APP-TOKEN", x_app_token)
    if x_api_key:
        headers_dict.setdefault("X-API-Key", x_api_key)
    if authorization:
        headers_dict.setdefault("Authorization", authorization)

    try:
        path = str(getattr(getattr(request, "url", None), "path", "") or "")
    except Exception:
        path = ""

    try:
        settings = _core_get_settings_cached()
    except Exception:
        settings = None

    attempts: Tuple[Dict[str, Any], ...] = (
        {
            "token": tok,
            "authorization": authz,
            "headers": headers_dict,
            "api_key": x_api_key,
            "path": path,
            "request": request,
            "settings": settings,
        },
        {
            "token": tok,
            "authorization": authz,
            "headers": headers_dict,
            "path": path,
            "request": request,
        },
        {
            "token": tok,
            "authorization": authz,
            "headers": headers_dict,
            "path": path,
        },
        {
            "token": tok,
            "authorization": authz,
            "headers": headers_dict,
        },
        {
            "token": tok,
            "authorization": authz,
        },
        {
            "token": tok,
        },
    )

    for kwargs in attempts:
        try:
            return bool(_core_auth_ok(**kwargs))
        except TypeError:
            # Signature mismatch — try a narrower attempt.
            continue
        except Exception:
            # Non-signature auth failure — treat as unauthorized.
            return False
    return False


# =============================================================================
# Internal lightweight metrics (NO prometheus)
# =============================================================================
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


# =============================================================================
# Provider loader (lazy + cached + async-factory-safe)
# =============================================================================
_PROVIDER_CANDIDATES = (
    "core.providers.argaam_provider",
    "providers.argaam_provider",
)

_PROVIDER_CACHE: Dict[str, Any] = {
    "module": None,
    "modpath": "",
    "import_error": "",
    "attempted": False,
}


def _import_provider() -> Tuple[Optional[Any], Optional[str], Optional[str]]:
    """
    Import the Argaam provider module. Caches the first success; retries
    on failure so a later code push with a fixed import succeeds without
    restart.
    """
    cached_mod = _PROVIDER_CACHE.get("module")
    if cached_mod is not None:
        return cached_mod, str(_PROVIDER_CACHE.get("modpath") or ""), None

    last_err: Optional[str] = None
    for modpath in _PROVIDER_CANDIDATES:
        try:
            mod = importlib.import_module(modpath)
        except Exception as e:
            last_err = f"{modpath}: {type(e).__name__}: {e}"
            continue
        _PROVIDER_CACHE["module"] = mod
        _PROVIDER_CACHE["modpath"] = modpath
        _PROVIDER_CACHE["import_error"] = ""
        _PROVIDER_CACHE["attempted"] = True
        return mod, modpath, None

    _PROVIDER_CACHE["import_error"] = last_err or "provider_import_failed"
    _PROVIDER_CACHE["attempted"] = True
    return None, None, _PROVIDER_CACHE["import_error"]


def _pick_callable(mod: Any) -> Optional[Callable]:
    """
    Prefer module-level async/sync functions. Order is deliberate — first
    match wins, and `fetch_enriched_quote_patch` is the project's canonical
    name (core.providers.argaam_provider v4.4.0).
    """
    for fn_name in (
        "fetch_enriched_quote_patch",
        "fetch_quote_patch",
        "fetch_patch",
        "fetch_quote",
        "get_quote",
        "quote",
    ):
        fn = getattr(mod, fn_name, None)
        if callable(fn):
            return fn
    return None


async def _call_provider_callable(fn: Callable, symbol: str, debug: bool) -> Dict[str, Any]:
    """
    Call a provider function. Supports sync/async. Tries debug=kw first,
    then the 1-arg signature.
    """
    try:
        out = fn(symbol, debug=debug)
    except TypeError:
        try:
            out = fn(symbol)
        except Exception as e:
            return {"symbol": symbol, "error": f"provider_call_failed:{type(e).__name__}:{e}"}
    try:
        out = await _maybe_await(out)
    except Exception as e:
        return {"symbol": symbol, "error": f"provider_await_failed:{type(e).__name__}:{e}"}
    return out if isinstance(out, dict) else {"symbol": symbol, "error": "provider_return_non_dict"}


async def _resolve_provider_instance(mod: Any) -> Any:
    """
    Resolve a provider instance by trying async and sync factories, then
    the module-level `provider` attribute. Awaits coroutine factories —
    critical because `core.providers.argaam_provider v4.4.0` exports
    `async def get_provider()`.
    """
    for factory_name in ("get_provider", "build_provider", "create_provider", "provider_factory"):
        fn = getattr(mod, factory_name, None)
        if not callable(fn):
            continue
        try:
            inst = fn()
            # v2.0.0 bug: failed to await async factories. Now fixed.
            if inspect.isawaitable(inst):
                inst = await inst
            if inst is not None:
                return inst
        except Exception:
            continue

    # Fallback: module-level `provider` singleton
    return getattr(mod, "provider", None)


async def _call_provider_instance(mod: Any, symbol: str, debug: bool) -> Dict[str, Any]:
    """
    Resolve a provider instance and call its fetch method. The instance
    might be produced by an async factory — this is now awaited correctly.
    """
    inst = await _resolve_provider_instance(mod)
    if inst is None:
        return {"symbol": symbol, "error": "no_provider_callable_or_instance"}

    for method_name in (
        "fetch_enriched_quote_patch",
        "fetch_quote_patch",
        "fetch_patch",
        "fetch_quote",
        "get_quote",
        "quote",
    ):
        meth = getattr(inst, method_name, None)
        if not callable(meth):
            continue
        try:
            try:
                out = meth(symbol, debug=debug)
            except TypeError:
                out = meth(symbol)
            out = await _maybe_await(out)
        except Exception as e:
            return {"symbol": symbol, "error": f"provider_instance_call_failed:{type(e).__name__}:{e}"}
        return out if isinstance(out, dict) else {"symbol": symbol, "error": "provider_instance_return_non_dict"}

    return {"symbol": symbol, "error": "provider_instance_missing_fetch_method"}


async def _fetch_argaam_patch(symbol: str, *, debug: bool = False) -> Tuple[Dict[str, Any], str, Optional[str]]:
    mod, modpath, import_err = _import_provider()
    if mod is None:
        return (
            {"symbol": symbol, "error": import_err or "provider_import_failed", "provider": "argaam"},
            "",
            import_err,
        )

    fn = _pick_callable(mod)
    if fn is not None:
        out = await _call_provider_callable(fn, symbol, debug)
        out.setdefault("provider", "argaam")
        return out, modpath or "", None

    out = await _call_provider_instance(mod, symbol, debug)
    out.setdefault("provider", "argaam")
    return out, modpath or "", None


def _provider_importable() -> bool:
    """Cheap probe without forcing a re-import."""
    if _PROVIDER_CACHE.get("module") is not None:
        return True
    if _PROVIDER_CACHE.get("attempted"):
        return False
    mod, _, _ = _import_provider()
    return mod is not None


# =============================================================================
# Routes
# =============================================================================
_OWNED_PATHS: Tuple[str, ...] = (
    "/v1/argaam/health",
    "/v1/argaam/metrics",
    "/v1/argaam/quote",
    "/v1/argaam/batch",
)


@router.get("/health", include_in_schema=False)
async def argaam_health(request: Request) -> JSONResponse:
    rid = _request_id(request, None)
    mod, modpath, import_err = _import_provider()
    capabilities = {
        "auth_ok_callable": callable(_core_auth_ok),
        "is_open_mode_callable": callable(_core_is_open_mode),
        "get_settings_cached_callable": callable(_core_get_settings_cached),
        "provider_importable": bool(mod is not None),
    }
    return JSONResponse(
        status_code=200,
        content={
            "status": "ok" if mod is not None else "degraded",
            "module": "routes.routes_argaam",
            "version": ROUTER_VERSION,
            "route_owner": ROUTE_OWNER_NAME,
            "route_family": ROUTE_FAMILY_NAME,
            "timestamp_utc": _now_utc(),
            "provider": {
                "available": bool(mod is not None),
                "module": modpath,
                "error": import_err,
                "candidates": list(_PROVIDER_CANDIDATES),
            },
            "capabilities": capabilities,
            "auth": {
                "require_auth_env": _env_bool("REQUIRE_AUTH", True),
                "allow_query_token_env": _env_bool("ALLOW_QUERY_TOKEN", False),
            },
            "owned_paths": list(_OWNED_PATHS),
            "request_id": rid,
        },
    )


@router.get("/metrics", include_in_schema=False)
async def argaam_metrics(request: Request) -> JSONResponse:
    rid = _request_id(request, None)
    async with _METRICS_LOCK:
        m = _METRICS.to_dict()
    return JSONResponse(
        status_code=200,
        content={
            "status": "ok",
            "module": "routes.routes_argaam",
            "version": ROUTER_VERSION,
            "route_owner": ROUTE_OWNER_NAME,
            "route_family": ROUTE_FAMILY_NAME,
            "timestamp_utc": _now_utc(),
            "request_id": rid,
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
    x_api_key: Optional[str] = Header(default=None, alias="X-API-Key"),
    authorization: Optional[str] = Header(default=None, alias="Authorization"),
    x_request_id: Optional[str] = Header(default=None, alias="X-Request-ID"),
) -> JSONResponse:
    rid = _request_id(request, x_request_id)
    t0 = time.perf_counter()

    if not _auth_ok_flexible(
        request,
        token_q=token,
        x_app_token=x_app_token,
        x_api_key=x_api_key,
        authorization=authorization,
    ):
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
                "route_owner": ROUTE_OWNER_NAME,
                "route_family": ROUTE_FAMILY_NAME,
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
        err=str((out.get("error") if isinstance(out, dict) else "") or import_err or ""),
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
            "route_owner": ROUTE_OWNER_NAME,
            "route_family": ROUTE_FAMILY_NAME,
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
    x_api_key: Optional[str] = Header(default=None, alias="X-API-Key"),
    authorization: Optional[str] = Header(default=None, alias="Authorization"),
    x_request_id: Optional[str] = Header(default=None, alias="X-Request-ID"),
) -> JSONResponse:
    rid = _request_id(request, x_request_id)
    t0 = time.perf_counter()

    if not _auth_ok_flexible(
        request,
        token_q=token,
        x_app_token=x_app_token,
        x_api_key=x_api_key,
        authorization=authorization,
    ):
        await _record(ok=False, unauthorized=True, latency_ms=(time.perf_counter() - t0) * 1000.0, err="unauthorized")
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="unauthorized")

    # Accept symbols/tickers in multiple shapes
    syms = _split_symbols(
        body.get("symbols")
        or body.get("tickers")
        or body.get("symbol_list")
        or body.get("direct_symbols")
        or ""
    )
    try:
        top_n = int(body.get("top_n") or len(syms) or 50)
    except Exception:
        top_n = len(syms) or 50
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
                "route_owner": ROUTE_OWNER_NAME,
                "route_family": ROUTE_FAMILY_NAME,
                "rows": [],
                "items": {},
            },
        )

    dbg = bool(body.get("debug", False) or debug)

    # Env-overridable max concurrency ceiling
    env_ceiling = max(1, min(200, _env_int("ARGAAM_MAX_CONCURRENCY", 50)))
    try:
        mc = int(body.get("max_concurrency") or max_concurrency or 10)
    except Exception:
        mc = 10
    mc = max(1, min(env_ceiling, mc))
    sem = asyncio.Semaphore(mc)

    async def _one(s: str) -> Tuple[str, Dict[str, Any], str, Optional[str]]:
        async with sem:
            out, modpath, err = await _fetch_argaam_patch(s, debug=dbg)
            return s, out, modpath, err

    results = await asyncio.gather(*[_one(s) for s in syms], return_exceptions=True)

    items: Dict[str, Any] = {}
    errors = 0
    provider_mod = ""
    gather_exceptions = 0
    for r in results:
        if isinstance(r, Exception):
            errors += 1
            gather_exceptions += 1
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
            "gather_exceptions": gather_exceptions,
            "max_concurrency_effective": mc,
            "provider_module": provider_mod,
            "request_id": rid,
            "timestamp_utc": _now_utc(),
            "version": ROUTER_VERSION,
            "route_owner": ROUTE_OWNER_NAME,
            "route_family": ROUTE_FAMILY_NAME,
            "items": items,  # {symbol: provider_patch}
        },
    )


# =============================================================================
# Dynamic loader hooks
# =============================================================================
def mount(app: Any) -> None:
    try:
        if app is not None and hasattr(app, "include_router"):
            app.include_router(router)
    except Exception:
        pass


def get_router() -> APIRouter:
    return router


__all__ = [
    "router",
    "mount",
    "get_router",
    "ROUTER_VERSION",
    "ROUTE_OWNER_NAME",
    "ROUTE_FAMILY_NAME",
]
