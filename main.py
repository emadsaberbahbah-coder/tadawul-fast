#!/usr/bin/env python3
"""
main.py
================================================================================
TADAWUL FAST BRIDGE — RENDER-SAFE FASTAPI ENTRYPOINT (v7.2.0)
================================================================================
ALIGNED • DEPLOYMENT-SAFE • LIFESPAN-BASED • ROUTER-SNAPSHOT-AWARE
ENGINE-STATE-AWARE • OPENAPI-SAFE • RENDER-HEALTH-PROBE-SAFE

Why this revision (v7.2.0)
--------------------------
- ✅ FIX: Normalizes the rich snapshot returned by routes.mount_all_routers(...)
- ✅ FIX: Preserves app.state.routes_snapshot if routes package already wrote it
- ✅ FIX: Engine warm-up now stores engine on app.state.engine for request-time access
- ✅ FIX: Fallback route mount plan aligned to your real repo modules
- ✅ FIX: Runtime meta now reports mounted / duplicate / failed counts accurately
- ✅ FIX: Adds request-id middleware and X-Request-ID response header
- ✅ HARDEN: Startup remains safe even if routes or engine imports partially fail
- ✅ HARDEN: /meta remains schema-visible so OpenAPI is never empty
- ✅ SAFE: HEAD probes for /, /livez, /readyz, /health remain stable for Render
"""

from __future__ import annotations

import importlib
import json
import logging
import os
import sys
import uuid
from contextlib import asynccontextmanager
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Sequence, Set, Tuple

from fastapi import FastAPI, Request, Response
from fastapi.responses import JSONResponse
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.middleware.cors import CORSMiddleware
from starlette.middleware.gzip import GZipMiddleware

# --------------------------------------------------------------------------------------
# Version
# --------------------------------------------------------------------------------------
APP_ENTRY_VERSION = "7.2.0"

# --------------------------------------------------------------------------------------
# Safe env helpers
# --------------------------------------------------------------------------------------
_TRUTHY = {"1", "true", "yes", "y", "on", "t", "enabled", "enable"}
_FALSY = {"0", "false", "no", "n", "off", "f", "disabled", "disable"}


def _env_str(name: str, default: str = "") -> str:
    v = os.getenv(name)
    return default if v is None else str(v).strip()


def _env_bool(name: str, default: bool = False) -> bool:
    raw = _env_str(name, "")
    if raw == "":
        return bool(default)
    s = raw.strip().lower()
    if s in _TRUTHY:
        return True
    if s in _FALSY:
        return False
    return bool(default)


def _env_int(name: str, default: int) -> int:
    raw = _env_str(name, "")
    try:
        return int(float(raw))
    except Exception:
        return int(default)


def _parse_csv(value: str) -> List[str]:
    s = (value or "").strip()
    if not s:
        return []
    return [x.strip() for x in s.split(",") if x.strip()]


def _err_to_str(e: BaseException, limit: int = 1600) -> str:
    try:
        s = f"{type(e).__name__}: {e}"
    except Exception:
        s = "UnknownError"
    return s if len(s) <= limit else (s[:limit] + "...(truncated)")


# --------------------------------------------------------------------------------------
# Logging
# --------------------------------------------------------------------------------------
class _JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        payload = {
            "ts": datetime.now(timezone.utc).isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "msg": record.getMessage(),
        }
        if record.exc_info:
            payload["exc"] = self.formatException(record.exc_info)
        for k in ("request_id", "path", "status_code"):
            if hasattr(record, k):
                payload[k] = getattr(record, k)
        try:
            return json.dumps(payload, ensure_ascii=False)
        except Exception:
            return f"{payload}"


def _setup_logging() -> logging.Logger:
    level = _env_str("LOG_LEVEL", "INFO").upper()
    log_json = _env_bool("LOG_JSON", False) or (_env_str("LOG_FORMAT", "").lower() == "json")

    root = logging.getLogger()
    root.handlers.clear()
    root.setLevel(getattr(logging, level, logging.INFO))

    h = logging.StreamHandler(sys.stdout)
    if log_json:
        h.setFormatter(_JsonFormatter())
    else:
        h.setFormatter(logging.Formatter("%(asctime)s | %(levelname)-8s | %(name)s | %(message)s"))
    root.addHandler(h)

    for name in ("uvicorn", "uvicorn.error", "uvicorn.access"):
        logging.getLogger(name).setLevel(getattr(logging, level, logging.INFO))

    logging.getLogger("httpx").setLevel(logging.WARNING)
    return logging.getLogger("main")


logger = _setup_logging()

# --------------------------------------------------------------------------------------
# Settings bridge
# --------------------------------------------------------------------------------------
@dataclass(frozen=True)
class _SettingsView:
    APP_NAME: str = "Tadawul Fast Bridge"
    APP_VERSION: str = "dev"
    APP_ENV: str = "production"
    TIMEZONE_DEFAULT: str = "Asia/Riyadh"

    DEBUG: bool = False
    ENABLE_SWAGGER: bool = True
    ENABLE_REDOC: bool = True
    INIT_ENGINE_ON_BOOT: bool = True
    INIT_ENGINE_STRICT: bool = False

    REQUIRE_AUTH: bool = True
    OPEN_MODE: bool = False
    AUTH_HEADER_NAME: str = "X-APP-TOKEN"

    ENABLE_CORS_ALL_ORIGINS: bool = False
    CORS_ORIGINS: str = ""

    BACKEND_BASE_URL: str = ""
    ENGINE_CACHE_TTL_SEC: int = 20
    MAX_REQUESTS_PER_MINUTE: int = 240


def _load_settings() -> _SettingsView:
    # 1) env.py
    try:
        env_mod = importlib.import_module("env")
        if hasattr(env_mod, "get_settings"):
            s = env_mod.get_settings()  # type: ignore[attr-defined]
            return _SettingsView(
                APP_NAME=getattr(s, "APP_NAME", _env_str("APP_NAME", "Tadawul Fast Bridge")),
                APP_VERSION=getattr(s, "APP_VERSION", _env_str("APP_VERSION", "dev")),
                APP_ENV=getattr(s, "APP_ENV", _env_str("APP_ENV", "production")),
                TIMEZONE_DEFAULT=getattr(s, "TIMEZONE_DEFAULT", _env_str("TZ", "Asia/Riyadh")),
                DEBUG=bool(getattr(s, "DEBUG", _env_bool("DEBUG", False))),
                ENABLE_SWAGGER=bool(getattr(s, "ENABLE_SWAGGER", _env_bool("ENABLE_SWAGGER", True))),
                ENABLE_REDOC=bool(getattr(s, "ENABLE_REDOC", _env_bool("ENABLE_REDOC", True))),
                INIT_ENGINE_ON_BOOT=bool(getattr(s, "INIT_ENGINE_ON_BOOT", _env_bool("INIT_ENGINE_ON_BOOT", True))),
                INIT_ENGINE_STRICT=bool(getattr(s, "INIT_ENGINE_STRICT", _env_bool("INIT_ENGINE_STRICT", False))),
                REQUIRE_AUTH=bool(getattr(s, "REQUIRE_AUTH", _env_bool("REQUIRE_AUTH", True))),
                OPEN_MODE=bool(getattr(s, "OPEN_MODE", _env_bool("OPEN_MODE", False))),
                AUTH_HEADER_NAME=str(getattr(s, "AUTH_HEADER_NAME", _env_str("AUTH_HEADER_NAME", "X-APP-TOKEN"))),
                ENABLE_CORS_ALL_ORIGINS=bool(getattr(s, "ENABLE_CORS_ALL_ORIGINS", _env_bool("ENABLE_CORS_ALL_ORIGINS", False))),
                CORS_ORIGINS=str(getattr(s, "CORS_ORIGINS", _env_str("CORS_ORIGINS", ""))),
                BACKEND_BASE_URL=str(getattr(s, "BACKEND_BASE_URL", _env_str("BACKEND_BASE_URL", ""))),
                ENGINE_CACHE_TTL_SEC=int(getattr(s, "ENGINE_CACHE_TTL_SEC", _env_int("ENGINE_CACHE_TTL_SEC", 20))),
                MAX_REQUESTS_PER_MINUTE=int(getattr(s, "MAX_REQUESTS_PER_MINUTE", _env_int("MAX_REQUESTS_PER_MINUTE", 240))),
            )
    except Exception:
        pass

    # 2) core.config
    try:
        cfg = importlib.import_module("core.config")
        getter = getattr(cfg, "get_settings_cached", None) or getattr(cfg, "get_settings", None)
        if callable(getter):
            s = getter()  # type: ignore[misc]
            return _SettingsView(
                APP_NAME=getattr(s, "service_name", _env_str("APP_NAME", "Tadawul Fast Bridge")),
                APP_VERSION=getattr(s, "app_version", _env_str("APP_VERSION", "dev")),
                APP_ENV=str(getattr(s, "environment", _env_str("APP_ENV", "production"))),
                TIMEZONE_DEFAULT=getattr(s, "timezone", _env_str("TZ", "Asia/Riyadh")),
                DEBUG=_env_bool("DEBUG", False),
                ENABLE_SWAGGER=_env_bool("ENABLE_SWAGGER", True),
                ENABLE_REDOC=_env_bool("ENABLE_REDOC", True),
                INIT_ENGINE_ON_BOOT=_env_bool("INIT_ENGINE_ON_BOOT", True),
                INIT_ENGINE_STRICT=_env_bool("INIT_ENGINE_STRICT", False),
                REQUIRE_AUTH=bool(getattr(s, "require_auth", _env_bool("REQUIRE_AUTH", True))),
                OPEN_MODE=bool(getattr(s, "open_mode", _env_bool("OPEN_MODE", False))),
                AUTH_HEADER_NAME=getattr(s, "auth_header_name", _env_str("AUTH_HEADER_NAME", "X-APP-TOKEN")),
                ENABLE_CORS_ALL_ORIGINS=_env_bool("ENABLE_CORS_ALL_ORIGINS", False),
                CORS_ORIGINS=_env_str("CORS_ORIGINS", ""),
                BACKEND_BASE_URL=getattr(s, "backend_base_url", _env_str("BACKEND_BASE_URL", "")),
                ENGINE_CACHE_TTL_SEC=int(getattr(s, "engine_cache_ttl_sec", _env_int("ENGINE_CACHE_TTL_SEC", 20))),
                MAX_REQUESTS_PER_MINUTE=_env_int("MAX_REQUESTS_PER_MINUTE", 240),
            )
    except Exception:
        pass

    # 3) env vars
    return _SettingsView(
        APP_NAME=_env_str("APP_NAME", "Tadawul Fast Bridge"),
        APP_VERSION=_env_str("APP_VERSION", "dev"),
        APP_ENV=_env_str("APP_ENV", "production"),
        TIMEZONE_DEFAULT=_env_str("TZ", "Asia/Riyadh"),
        DEBUG=_env_bool("DEBUG", False),
        ENABLE_SWAGGER=_env_bool("ENABLE_SWAGGER", True),
        ENABLE_REDOC=_env_bool("ENABLE_REDOC", True),
        INIT_ENGINE_ON_BOOT=_env_bool("INIT_ENGINE_ON_BOOT", True),
        INIT_ENGINE_STRICT=_env_bool("INIT_ENGINE_STRICT", False),
        REQUIRE_AUTH=_env_bool("REQUIRE_AUTH", True),
        OPEN_MODE=_env_bool("OPEN_MODE", False),
        AUTH_HEADER_NAME=_env_str("AUTH_HEADER_NAME", "X-APP-TOKEN"),
        ENABLE_CORS_ALL_ORIGINS=_env_bool("ENABLE_CORS_ALL_ORIGINS", False),
        CORS_ORIGINS=_env_str("CORS_ORIGINS", ""),
        BACKEND_BASE_URL=_env_str("BACKEND_BASE_URL", ""),
        ENGINE_CACHE_TTL_SEC=_env_int("ENGINE_CACHE_TTL_SEC", 20),
        MAX_REQUESTS_PER_MINUTE=_env_int("MAX_REQUESTS_PER_MINUTE", 240),
    )


_SETTINGS = _load_settings()

# --------------------------------------------------------------------------------------
# Auth helper for debug endpoints
# --------------------------------------------------------------------------------------
def _auth_ok(request: Request) -> bool:
    try:
        if bool(getattr(_SETTINGS, "OPEN_MODE", False)):
            return True
    except Exception:
        pass

    try:
        cfg = importlib.import_module("core.config")
        fn = getattr(cfg, "auth_ok", None)
        if callable(fn):
            hdr = request.headers.get(_SETTINGS.AUTH_HEADER_NAME, "") or request.headers.get("X-APP-TOKEN", "")
            auth = request.headers.get("Authorization", "")
            token_q = request.query_params.get("token") if request.query_params else None
            return bool(fn(token=hdr or token_q, authorization=auth, headers=dict(request.headers)))  # type: ignore[misc]
    except Exception:
        pass

    allowed = {
        x for x in (
            _env_str("APP_TOKEN", ""),
            _env_str("BACKEND_TOKEN", ""),
            _env_str("BACKUP_APP_TOKEN", ""),
            _env_str("X_APP_TOKEN", ""),
        ) if x
    }
    if not allowed:
        return False

    hdr = request.headers.get(_SETTINGS.AUTH_HEADER_NAME, "") or request.headers.get("X-APP-TOKEN", "")
    auth = request.headers.get("Authorization", "")
    token_q = request.query_params.get("token") if request.query_params else ""

    if hdr and hdr in allowed:
        return True
    if token_q and token_q in allowed:
        return True
    if auth.lower().startswith("bearer "):
        t = auth.split(" ", 1)[1].strip()
        return bool(t and t in allowed)
    return False


# --------------------------------------------------------------------------------------
# Request ID middleware
# --------------------------------------------------------------------------------------
class RequestIDMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next):
        request_id = request.headers.get("X-Request-ID", "").strip() or uuid.uuid4().hex[:12]
        request.state.request_id = request_id
        response = await call_next(request)
        try:
            response.headers["X-Request-ID"] = request_id
        except Exception:
            pass
        return response


# --------------------------------------------------------------------------------------
# Router mounting helpers
# --------------------------------------------------------------------------------------
def _app_route_signature_set(app: Any) -> Set[Tuple[str, str]]:
    sigs: Set[Tuple[str, str]] = set()
    for r in getattr(app, "routes", []) or []:
        path = str(getattr(r, "path", "") or "")
        methods = getattr(r, "methods", None) or set()
        if not isinstance(methods, (set, list, tuple)):
            methods = set()
        meth = ",".join(sorted(str(m) for m in methods))
        sigs.add((path, meth))
    return sigs


def _router_route_signature_set(router: Any) -> Set[Tuple[str, str]]:
    sigs: Set[Tuple[str, str]] = set()
    for r in getattr(router, "routes", []) or []:
        path = str(getattr(r, "path", "") or "")
        methods = getattr(r, "methods", None) or set()
        if not isinstance(methods, (set, list, tuple)):
            methods = set()
        meth = ",".join(sorted(str(m) for m in methods))
        sigs.add((path, meth))
    return sigs


def _router_would_duplicate_existing(app: Any, router: Any) -> bool:
    router_sigs = _router_route_signature_set(router)
    if not router_sigs:
        return False
    return router_sigs.issubset(_app_route_signature_set(app))


def _normalize_routes_snapshot(
    ret: Any,
    *,
    used_strategy: str,
    app: Optional[FastAPI] = None,
) -> Dict[str, Any]:
    """
    Accept:
    - dict returned by routes.mount_all_routers(...)
    - dict already stored on app.state.routes_snapshot
    - anything else -> minimal safe snapshot
    """
    base_from_state: Dict[str, Any] = {}
    if app is not None:
        try:
            state_snap = getattr(app.state, "routes_snapshot", None)
            if isinstance(state_snap, dict):
                base_from_state = dict(state_snap)
        except Exception:
            base_from_state = {}

    src: Dict[str, Any] = {}
    if isinstance(base_from_state, dict):
        src.update(base_from_state)
    if isinstance(ret, dict):
        src.update(ret)

    mounted = list(src.get("mounted", []) or [])
    duplicate_skips = list(src.get("duplicate_skips", []) or [])
    missing = list(src.get("missing", []) or [])
    import_errors = dict(src.get("import_errors", {}) or {})
    mount_errors = dict(src.get("mount_errors", {}) or {})
    no_router = dict(src.get("no_router", {}) or {})
    strict = bool(src.get("strict", _env_bool("ROUTES_STRICT_IMPORT", False)))
    strategy = str(src.get("strategy", used_strategy) or used_strategy)

    missing_required_keys = list(src.get("missing_required_keys", []) or [])
    resolved_map = dict(src.get("resolved_map", {}) or {})
    module_to_key = dict(src.get("module_to_key", {}) or {})
    mount_modes = dict(src.get("mount_modes", {}) or {})
    expected_router_modules = list(src.get("expected_router_modules", []) or [])
    plan = list(src.get("plan", []) or [])
    resolved_entries = list(src.get("resolved_entries", []) or [])

    failed_count = len(import_errors) + len(mount_errors) + len(no_router)
    openapi_route_count_after_mount = int(src.get("openapi_route_count_after_mount", 0) or 0)

    return {
        "mounted": mounted,
        "mounted_count": len(mounted),
        "duplicate_skips": duplicate_skips,
        "duplicate_skips_count": len(duplicate_skips),
        "missing": missing,
        "missing_count": len(missing),
        "import_errors": import_errors,
        "mount_errors": mount_errors,
        "no_router": no_router,
        "failed_count": failed_count,
        "strict": strict,
        "strategy": strategy,
        "missing_required_keys": missing_required_keys,
        "resolved_map": resolved_map,
        "module_to_key": module_to_key,
        "mount_modes": mount_modes,
        "expected_router_modules": expected_router_modules,
        "plan": plan,
        "resolved_entries": resolved_entries,
        "openapi_route_count_after_mount": openapi_route_count_after_mount,
    }


def _import_router_module(module_name: str) -> Tuple[Optional[Any], Optional[BaseException]]:
    try:
        mod = importlib.import_module(module_name)
        return mod, None
    except Exception as e:
        return None, e


def _get_router_from_module(mod: Any) -> Tuple[Optional[Any], str]:
    router = getattr(mod, "router", None)
    if router is not None:
        return router, "router_attr"

    for fn_name in ("get_router", "build_router", "create_router"):
        fn = getattr(mod, fn_name, None)
        if callable(fn):
            try:
                out = fn()
                if out is not None:
                    return out, fn_name
            except Exception:
                continue

    return None, "none"


def _mount_routes_fallback(app: FastAPI) -> Dict[str, Any]:
    strict = _env_bool("ROUTES_STRICT_IMPORT", False)

    plan = [
        "routes.config",
        "routes.enriched_quote",
        "routes.advanced_analysis",
        "routes.ai_analysis",
        "routes.advisor",
        "routes.investment_advisor",
        "routes.analysis_sheet_rows",
        "routes.advanced_sheet_rows",
        "routes.data_dictionary",
        "routes.top10_investments",
        "routes.routes_argaam",
    ]

    mounted: List[str] = []
    duplicate_skips: List[str] = []
    missing: List[str] = []
    import_errors: Dict[str, str] = {}
    mount_errors: Dict[str, str] = {}
    no_router: Dict[str, str] = {}
    mount_modes: Dict[str, str] = {}

    for mod_name in plan:
        mod, exc = _import_router_module(mod_name)
        if mod is None:
            if isinstance(exc, ModuleNotFoundError) and getattr(exc, "name", None) == mod_name:
                missing.append(mod_name)
            else:
                import_errors[mod_name] = _err_to_str(exc or Exception("unknown import error"))
            continue

        router_obj, source = _get_router_from_module(mod)
        mount_fn = getattr(mod, "mount", None)

        try:
            if router_obj is not None:
                if _router_would_duplicate_existing(app, router_obj):
                    duplicate_skips.append(mod_name)
                    mount_modes[mod_name] = "duplicate_skip"
                    continue
                app.include_router(router_obj)
                mounted.append(mod_name)
                mount_modes[mod_name] = source
                continue

            if callable(mount_fn):
                mount_fn(app)
                mounted.append(mod_name)
                mount_modes[mod_name] = "mount_fn"
                continue

            no_router[mod_name] = "No router export and no mount(app) function found"
        except Exception as e:
            mount_errors[mod_name] = _err_to_str(e)
            if strict:
                raise

    return {
        "mounted": mounted,
        "duplicate_skips": duplicate_skips,
        "missing": missing,
        "import_errors": import_errors,
        "mount_errors": mount_errors,
        "no_router": no_router,
        "strict": strict,
        "strategy": "main.fallback_plan",
        "plan": plan,
        "mount_modes": mount_modes,
        "openapi_route_count_after_mount": len(getattr(app, "routes", []) or []),
    }


def _mount_routes(app: FastAPI) -> Dict[str, Any]:
    """
    Discover and mount routers from `routes` package (best-effort).
    Does not fail startup unless ROUTES_STRICT_IMPORT=1.
    """
    strict = _env_bool("ROUTES_STRICT_IMPORT", False)

    try:
        routes_pkg = importlib.import_module("routes")
        for fn_name in ("mount_all_routers", "mount_all", "mount_routers", "mount_routes", "mount_all_routes"):
            fn = getattr(routes_pkg, fn_name, None)
            if callable(fn):
                used_strategy = f"routes.{fn_name}"
                try:
                    ret = fn(app)  # type: ignore[misc]
                    snap = _normalize_routes_snapshot(ret, used_strategy=used_strategy, app=app)
                    logger.info(
                        "Routes mount summary: mounted=%s duplicate_skips=%s missing=%s failed=%s strategy=%s",
                        snap["mounted_count"],
                        snap["duplicate_skips_count"],
                        snap["missing_count"],
                        snap["failed_count"],
                        snap["strategy"],
                    )
                    return snap
                except Exception as e:
                    logger.warning("%s(app) failed; falling back to internal plan. err=%s", used_strategy, _err_to_str(e))
                    if strict:
                        raise
                break
    except Exception:
        pass

    snap = _mount_routes_fallback(app)
    snap = _normalize_routes_snapshot(snap, used_strategy="main.fallback_plan", app=app)
    logger.info(
        "Routes mount summary: mounted=%s duplicate_skips=%s missing=%s failed=%s strategy=%s",
        snap["mounted_count"],
        snap["duplicate_skips_count"],
        snap["missing_count"],
        snap["failed_count"],
        snap["strategy"],
    )
    return snap


# --------------------------------------------------------------------------------------
# Built-in endpoints
# --------------------------------------------------------------------------------------
def _runtime_meta(app: Optional[FastAPI] = None) -> Dict[str, Any]:
    snap: Dict[str, Any] = {}
    engine_obj: Any = None
    engine_source = ""
    routes_mounted = False

    try:
        if app is not None and hasattr(app, "state"):
            state_snap = getattr(app.state, "routes_snapshot", None)
            if isinstance(state_snap, dict):
                snap = state_snap
            engine_obj = getattr(app.state, "engine", None)
            engine_source = str(getattr(app.state, "engine_source", "") or "")
            routes_mounted = bool(getattr(app.state, "routes_mounted", False))
    except Exception:
        pass

    mounted_count = int(snap.get("mounted_count", len(snap.get("mounted", []) or [])) or 0)
    duplicate_skips_count = int(snap.get("duplicate_skips_count", len(snap.get("duplicate_skips", []) or [])) or 0)
    failed_count = int(
        snap.get(
            "failed_count",
            len(snap.get("import_errors", {}) or {}) + len(snap.get("mount_errors", {}) or {}) + len(snap.get("no_router", {}) or {}),
        ) or 0
    )
    strategy = str(snap.get("strategy", "") or "")

    return {
        "service": _SETTINGS.APP_NAME,
        "app_version": _SETTINGS.APP_VERSION,
        "entry_version": APP_ENTRY_VERSION,
        "env": _SETTINGS.APP_ENV,
        "timestamp_utc": datetime.now(timezone.utc).isoformat(),
        "python": sys.version.split()[0],
        "routes_mounted": routes_mounted,
        "routes_mounted_count": mounted_count,
        "routes_duplicate_skips_count": duplicate_skips_count,
        "routes_failed_count": failed_count,
        "routes_strategy": strategy,
        "engine_present": engine_obj is not None,
        "engine_source": engine_source,
    }


def _install_builtin_routes(app: FastAPI) -> None:
    @app.get("/meta", tags=["meta"])
    async def meta():
        return {"status": "ok", **_runtime_meta(app)}

    @app.api_route("/", methods=["GET", "HEAD"], include_in_schema=False)
    async def root(request: Request):
        if request.method == "HEAD":
            return Response(status_code=200)
        return {"status": "ok", **_runtime_meta(app)}

    @app.api_route("/readyz", methods=["GET", "HEAD"], include_in_schema=False)
    async def readyz(request: Request):
        if request.method == "HEAD":
            return Response(status_code=200)
        return {"status": "ready", **_runtime_meta(app)}

    @app.api_route("/livez", methods=["GET", "HEAD"], include_in_schema=False)
    async def livez(request: Request):
        if request.method == "HEAD":
            return Response(status_code=200)
        return {"status": "live", **_runtime_meta(app)}

    @app.api_route("/health", methods=["GET", "HEAD"], include_in_schema=False)
    async def health(request: Request):
        if request.method == "HEAD":
            return Response(status_code=200)
        return {"status": "healthy", **_runtime_meta(app)}

    @app.get("/ping", include_in_schema=False)
    async def ping():
        return {"pong": True, **_runtime_meta(app)}


# --------------------------------------------------------------------------------------
# Optional engine warm-up
# --------------------------------------------------------------------------------------
async def _maybe_init_engine(app: FastAPI) -> Optional[str]:
    if not bool(_SETTINGS.INIT_ENGINE_ON_BOOT):
        return None

    # Try V2 first
    try:
        de2 = importlib.import_module("core.data_engine_v2")
        init_fn = getattr(de2, "get_engine", None) or getattr(de2, "init_engine", None)
        if callable(init_fn):
            maybe = init_fn()
            engine = await maybe if hasattr(maybe, "__await__") else maybe
            if engine is not None:
                app.state.engine = engine
                app.state.engine_source = "core.data_engine_v2"
                return None
    except Exception as e:
        err_v2 = _err_to_str(e)
    else:
        err_v2 = ""

    # Fallback V1
    try:
        de1 = importlib.import_module("core.data_engine")
        init_fn = getattr(de1, "get_engine", None) or getattr(de1, "init_engine", None)
        if callable(init_fn):
            maybe = init_fn()
            engine = await maybe if hasattr(maybe, "__await__") else maybe
            if engine is not None:
                app.state.engine = engine
                app.state.engine_source = "core.data_engine"
                return None
    except Exception as e:
        err_v1 = _err_to_str(e)
    else:
        err_v1 = ""

    return f"data_engine_v2 failed: {err_v2} | data_engine failed: {err_v1}"


async def _maybe_close_engine() -> None:
    for mod_name in ("core.data_engine_v2", "core.data_engine"):
        try:
            mod = importlib.import_module(mod_name)
            fn = getattr(mod, "close_engine", None) or getattr(mod, "shutdown_engine", None)
            if callable(fn):
                maybe = fn()
                if hasattr(maybe, "__await__"):
                    await maybe
        except Exception:
            continue


# --------------------------------------------------------------------------------------
# App factory
# --------------------------------------------------------------------------------------
def create_app() -> FastAPI:
    docs_url = "/docs" if bool(_SETTINGS.ENABLE_SWAGGER) else None
    redoc_url = "/redoc" if bool(_SETTINGS.ENABLE_REDOC) else None
    openapi_url = "/openapi.json" if (docs_url or redoc_url) else None

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        app.state.routes_snapshot = {
            "mounted": [],
            "mounted_count": 0,
            "duplicate_skips": [],
            "duplicate_skips_count": 0,
            "missing": [],
            "missing_count": 0,
            "import_errors": {},
            "mount_errors": {},
            "no_router": {},
            "failed_count": 0,
            "strict": False,
            "strategy": "",
            "plan": [],
            "mount_modes": {},
        }
        app.state.routes_mounted = False
        app.state.engine = None
        app.state.engine_source = ""

        try:
            snap = _mount_routes(app)
            app.state.routes_snapshot = snap
            app.state.routes_mounted = True
        except Exception as e:
            logger.error("Route mounting crashed: %s", e, exc_info=True)
            if _env_bool("ROUTES_STRICT_IMPORT", False):
                raise

        err = await _maybe_init_engine(app)
        if err:
            strict = bool(_SETTINGS.INIT_ENGINE_STRICT)
            if strict:
                logger.error("Engine init failed (strict): %s", err)
                raise RuntimeError(f"Engine init failed: {err}")
            logger.warning("Engine init failed (non-fatal): %s", err)

        logger.info("Startup complete: %s", _runtime_meta(app))
        try:
            yield
        finally:
            try:
                m = importlib.import_module("integrations.google_sheets_service")
                fn = getattr(m, "close", None)
                if callable(fn):
                    maybe = fn()
                    if hasattr(maybe, "__await__"):
                        await maybe
            except Exception:
                pass

            await _maybe_close_engine()
            logger.info("Shutdown complete.")

    app = FastAPI(
        title=_SETTINGS.APP_NAME,
        version=_SETTINGS.APP_VERSION,
        docs_url=docs_url,
        redoc_url=redoc_url,
        openapi_url=openapi_url,
        lifespan=lifespan,
    )

    app.add_middleware(RequestIDMiddleware)
    app.add_middleware(GZipMiddleware, minimum_size=1024)

    cors_all = bool(_SETTINGS.ENABLE_CORS_ALL_ORIGINS)
    cors_origins = _parse_csv(_SETTINGS.CORS_ORIGINS) if isinstance(_SETTINGS.CORS_ORIGINS, str) else []
    if cors_all or cors_origins:
        allow_origins = ["*"] if cors_all else cors_origins
        app.add_middleware(
            CORSMiddleware,
            allow_origins=allow_origins,
            allow_credentials=False if cors_all else True,
            allow_methods=["*"],
            allow_headers=["*"],
            max_age=600,
        )

    @app.exception_handler(Exception)
    async def unhandled_exception_handler(request: Request, exc: Exception):
        logger.error("Unhandled exception: %s", exc, exc_info=True)
        return JSONResponse(
            status_code=500,
            content={
                "status": "error",
                "error": f"{type(exc).__name__}: {str(exc)}",
                "path": str(request.url.path),
                "request_id": getattr(request.state, "request_id", ""),
                "ts_utc": datetime.now(timezone.utc).isoformat(),
            },
        )

    _install_builtin_routes(app)

    @app.get("/_debug/routes", include_in_schema=False)
    async def debug_routes(request: Request):
        if _SETTINGS.APP_ENV == "production" and not _auth_ok(request):
            return JSONResponse(status_code=401, content={"status": "error", "error": "unauthorized"})
        return {
            "status": "ok",
            "routes_snapshot": getattr(request.app.state, "routes_snapshot", {}),
            "app_route_count": len(getattr(request.app, "routes", []) or []),
            "route_paths": sorted({str(getattr(r, "path", "") or "") for r in (getattr(request.app, "routes", []) or [])}),
            **_runtime_meta(app),
        }

    return app


# --------------------------------------------------------------------------------------
# ASGI export
# --------------------------------------------------------------------------------------
app = create_app()
