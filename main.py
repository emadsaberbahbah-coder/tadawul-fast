#!/usr/bin/env python3
"""
main.py
================================================================================
TADAWUL FAST BRIDGE — RENDER-SAFE FASTAPI ENTRYPOINT (v7.0.0)
================================================================================
Aligned • Deployment-safe • Minimal import chain • Dynamic router mounting

Design goals
- ✅ NEVER crash at import-time (keep imports light; defer heavy imports to startup)
- ✅ Render-safe defaults (docs can be toggled; proxy headers; binds handled by launcher)
- ✅ Works with your env.py + core/config.py patterns (best-effort detection)
- ✅ Dynamic router discovery via routes/__init__.py (mount plan + graceful degrade)
- ✅ Safe diagnostics endpoints (guarded in production)

Expected usage
- Uvicorn/Gunicorn should import: main:app
- Render Start Command (recommended): python scripts/start_web.py --app main:app
"""

from __future__ import annotations

import importlib
import json
import logging
import os
import sys
import time
import traceback
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
from starlette.middleware.cors import CORSMiddleware
from starlette.middleware.gzip import GZipMiddleware

# --------------------------------------------------------------------------------------
# Version
# --------------------------------------------------------------------------------------
APP_ENTRY_VERSION = "7.0.0"


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
        return int(raw)
    except Exception:
        return int(default)


def _parse_csv(value: str) -> List[str]:
    s = (value or "").strip()
    if not s:
        return []
    return [x.strip() for x in s.split(",") if x.strip()]


# --------------------------------------------------------------------------------------
# Logging (safe, no external deps)
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
        # include a couple common structured fields if present
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

    # keep noisy libs reasonable
    logging.getLogger("uvicorn").setLevel(getattr(logging, level, logging.INFO))
    logging.getLogger("uvicorn.error").setLevel(getattr(logging, level, logging.INFO))
    logging.getLogger("uvicorn.access").setLevel(getattr(logging, level, logging.INFO))
    logging.getLogger("httpx").setLevel(logging.WARNING)

    return logging.getLogger("main")


logger = _setup_logging()


# --------------------------------------------------------------------------------------
# Settings bridge (env.py preferred; core.config fallback)
# --------------------------------------------------------------------------------------
@dataclass(frozen=True)
class _SettingsView:
    # core app
    APP_NAME: str = "Tadawul Fast Bridge"
    APP_VERSION: str = "dev"
    APP_ENV: str = "production"
    TIMEZONE_DEFAULT: str = "Asia/Riyadh"

    # runtime flags
    DEBUG: bool = False
    ENABLE_SWAGGER: bool = True
    ENABLE_REDOC: bool = True
    DEFER_ROUTER_MOUNT: bool = False
    INIT_ENGINE_ON_BOOT: bool = True

    # auth
    REQUIRE_AUTH: bool = True
    OPEN_MODE: bool = False
    AUTH_HEADER_NAME: str = "X-APP-TOKEN"

    # cors
    ENABLE_CORS_ALL_ORIGINS: bool = False
    CORS_ORIGINS: str = ""

    # misc
    BACKEND_BASE_URL: str = ""
    ENGINE_CACHE_TTL_SEC: int = 20
    MAX_REQUESTS_PER_MINUTE: int = 240


def _load_settings() -> _SettingsView:
    """
    Best-effort: prefer env.py:get_settings(); else core.config:get_settings_cached(); else env vars.
    Never throws.
    """
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
                DEFER_ROUTER_MOUNT=bool(getattr(s, "DEFER_ROUTER_MOUNT", _env_bool("DEFER_ROUTER_MOUNT", False))),
                INIT_ENGINE_ON_BOOT=bool(getattr(s, "INIT_ENGINE_ON_BOOT", _env_bool("INIT_ENGINE_ON_BOOT", True))),
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
                DEFER_ROUTER_MOUNT=_env_bool("DEFER_ROUTER_MOUNT", False),
                INIT_ENGINE_ON_BOOT=_env_bool("INIT_ENGINE_ON_BOOT", True),
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
        DEFER_ROUTER_MOUNT=_env_bool("DEFER_ROUTER_MOUNT", False),
        INIT_ENGINE_ON_BOOT=_env_bool("INIT_ENGINE_ON_BOOT", True),
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
# Auth helper for sensitive debug endpoints (best-effort)
# --------------------------------------------------------------------------------------
def _auth_ok(request: Request) -> bool:
    """
    Best-effort auth guard.
    - Prefer core.config.auth_ok if available.
    - Else: allow when OPEN_MODE=true, or token matches APP_TOKEN/BACKEND_TOKEN/BACKUP_APP_TOKEN.
    Never throws.
    """
    try:
        if bool(getattr(_SETTINGS, "OPEN_MODE", False)):
            return True
    except Exception:
        pass

    # Try core.config.auth_ok
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

    # Simple fallback
    allowed = set(
        x
        for x in (
            _env_str("APP_TOKEN", ""),
            _env_str("BACKEND_TOKEN", ""),
            _env_str("BACKUP_APP_TOKEN", ""),
        )
        if x
    )
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
# Router mounting (dynamic + safe)
# --------------------------------------------------------------------------------------
def _import_router_module(module_name: str) -> Tuple[Optional[Any], Optional[str]]:
    """
    Returns (module, error_message). Never throws.
    """
    try:
        mod = importlib.import_module(module_name)
        return mod, None
    except Exception as e:
        return None, f"{type(e).__name__}: {e}"


def _mount_routes(app: FastAPI) -> Dict[str, Any]:
    """
    Discover and mount routers from `routes` package (best-effort).
    Does not fail startup unless ROUTES_STRICT_IMPORT=1.
    """
    strict = _env_bool("ROUTES_STRICT_IMPORT", False)
    mounted: List[str] = []
    missing: List[str] = []
    failed: Dict[str, str] = {}

    expected: List[str] = []
    plan: List[str] = []

    # Try to use your advanced routes/__init__.py API
    try:
        routes_pkg = importlib.import_module("routes")
        expected_fn = getattr(routes_pkg, "get_expected_router_modules", None)
        plan_fn = getattr(routes_pkg, "get_mount_plan", None)
        if callable(expected_fn):
            expected = list(expected_fn())
        if callable(plan_fn):
            plan = list(plan_fn())
    except Exception:
        routes_pkg = None  # noqa: F841

    if not expected:
        # safe fallback list (matches your routes/__init__.py)
        expected = [
            "routes.health",
            "routes.auth",
            "routes.legacy_service",
            "routes.data_engine",
            "routes.investment_advisor",
            "routes.portfolio",
            "routes.websockets",
            "routes.analytics",
            "routes.market_data",
            "routes.argaam",
            "routes.tadawul",
        ]

    if not plan:
        plan = expected[:]  # best-effort order

    # Mount
    for mod_name in plan:
        mod, err = _import_router_module(mod_name)
        if mod is None:
            missing.append(mod_name)
            if err:
                failed[mod_name] = err
            continue

        # router variable
        router = getattr(mod, "router", None)

        # optional: module exposes mount(app)
        mount_fn = getattr(mod, "mount", None)

        try:
            if router is not None:
                app.include_router(router)
                mounted.append(mod_name)
                continue
            if callable(mount_fn):
                mount_fn(app)
                mounted.append(mod_name)
                continue

            # no router
            missing.append(mod_name)
            failed[mod_name] = "No `router` object or `mount(app)` found"
        except Exception as e:
            failed[mod_name] = f"Mount failed: {type(e).__name__}: {e}"
            if strict:
                raise

    snapshot = {
        "mounted": mounted,
        "missing": missing,
        "failed": failed,
        "strict": strict,
    }

    logger.info("Routes mount summary: mounted=%s missing=%s failed=%s", len(mounted), len(missing), len(failed))
    if failed and strict:
        logger.error("Strict router import enabled; failing startup due to route errors.")
    return snapshot


# --------------------------------------------------------------------------------------
# Minimal safe built-in endpoints (always present)
# --------------------------------------------------------------------------------------
def _runtime_meta() -> Dict[str, Any]:
    return {
        "service": _SETTINGS.APP_NAME,
        "app_version": _SETTINGS.APP_VERSION,
        "entry_version": APP_ENTRY_VERSION,
        "env": _SETTINGS.APP_ENV,
        "timestamp_utc": datetime.now(timezone.utc).isoformat(),
        "python": sys.version.split()[0],
    }


def _install_builtin_routes(app: FastAPI) -> None:
    @app.get("/", include_in_schema=False)
    async def root():
        return {"status": "ok", **_runtime_meta()}

    @app.get("/health", include_in_schema=False)
    async def health():
        # keep very light; no heavy imports
        return {"status": "healthy", **_runtime_meta()}

    @app.get("/ready", include_in_schema=False)
    async def ready():
        # readiness is "healthy" unless we explicitly know otherwise
        return {"status": "ready", **_runtime_meta()}


# --------------------------------------------------------------------------------------
# Optional startup init (safe)
# --------------------------------------------------------------------------------------
async def _maybe_init_engine() -> Optional[str]:
    """
    Optional: initializes core engine if present.
    Never raises; returns error string on failure.
    """
    if not bool(_SETTINGS.INIT_ENGINE_ON_BOOT):
        return None
    try:
        de = importlib.import_module("core.data_engine")
        init_fn = getattr(de, "init_engine", None) or getattr(de, "get_engine", None)
        if callable(init_fn):
            # If get_engine exists, calling it should initialize lazily.
            maybe = init_fn()
            # allow async too
            if hasattr(maybe, "__await__"):
                await maybe
        return None
    except Exception as e:
        return f"{type(e).__name__}: {e}"


# --------------------------------------------------------------------------------------
# App factory
# --------------------------------------------------------------------------------------
def create_app() -> FastAPI:
    """
    Factory for Gunicorn/uvicorn.
    Keep it import-safe.
    """
    # Docs toggles
    docs_url = "/docs" if bool(_SETTINGS.ENABLE_SWAGGER) else None
    redoc_url = "/redoc" if bool(_SETTINGS.ENABLE_REDOC) else None
    openapi_url = "/openapi.json" if (docs_url or redoc_url) else None

    app = FastAPI(
        title=_SETTINGS.APP_NAME,
        version=_SETTINGS.APP_VERSION,
        docs_url=docs_url,
        redoc_url=redoc_url,
        openapi_url=openapi_url,
    )

    # Middlewares
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

    # Exception handler (ensures we see full errors)
    @app.exception_handler(Exception)
    async def unhandled_exception_handler(request: Request, exc: Exception):
        logger.error("Unhandled exception: %s", exc, exc_info=True)
        return JSONResponse(
            status_code=500,
            content={
                "status": "error",
                "error": f"{type(exc).__name__}: {str(exc)}",
                "path": str(request.url.path),
                "ts_utc": datetime.now(timezone.utc).isoformat(),
            },
        )

    _install_builtin_routes(app)

    # Mount routers now unless explicitly deferred
    routes_snapshot: Dict[str, Any] = {"mounted": [], "missing": [], "failed": {}, "strict": False}
    if not bool(_SETTINGS.DEFER_ROUTER_MOUNT):
        try:
            routes_snapshot = _mount_routes(app)
        except Exception as e:
            logger.error("Route mounting crashed: %s", e, exc_info=True)
            # In strict mode _mount_routes raises; here we preserve the failure semantics
            if _env_bool("ROUTES_STRICT_IMPORT", False):
                raise

    # Debug endpoints (guarded)
    @app.get("/_debug/routes", include_in_schema=bool(_SETTINGS.DEBUG))
    async def debug_routes(request: Request):
        if _SETTINGS.APP_ENV == "production" and not _auth_ok(request):
            return JSONResponse(status_code=401, content={"status": "error", "error": "unauthorized"})
        return {"status": "ok", "routes": routes_snapshot, **_runtime_meta()}

    @app.get("/_debug/settings", include_in_schema=bool(_SETTINGS.DEBUG))
    async def debug_settings(request: Request):
        if _SETTINGS.APP_ENV == "production" and not _auth_ok(request):
            return JSONResponse(status_code=401, content={"status": "error", "error": "unauthorized"})
        # never dump secrets; return only safe view
        return {
            "status": "ok",
            "settings": {
                "APP_NAME": _SETTINGS.APP_NAME,
                "APP_VERSION": _SETTINGS.APP_VERSION,
                "APP_ENV": _SETTINGS.APP_ENV,
                "DEBUG": _SETTINGS.DEBUG,
                "ENABLE_SWAGGER": _SETTINGS.ENABLE_SWAGGER,
                "ENABLE_REDOC": _SETTINGS.ENABLE_REDOC,
                "DEFER_ROUTER_MOUNT": _SETTINGS.DEFER_ROUTER_MOUNT,
                "INIT_ENGINE_ON_BOOT": _SETTINGS.INIT_ENGINE_ON_BOOT,
                "REQUIRE_AUTH": _SETTINGS.REQUIRE_AUTH,
                "OPEN_MODE": _SETTINGS.OPEN_MODE,
                "AUTH_HEADER_NAME": _SETTINGS.AUTH_HEADER_NAME,
                "ENABLE_CORS_ALL_ORIGINS": _SETTINGS.ENABLE_CORS_ALL_ORIGINS,
                "CORS_ORIGINS": _SETTINGS.CORS_ORIGINS,
                "BACKEND_BASE_URL": _SETTINGS.BACKEND_BASE_URL,
                "ENGINE_CACHE_TTL_SEC": _SETTINGS.ENGINE_CACHE_TTL_SEC,
                "MAX_REQUESTS_PER_MINUTE": _SETTINGS.MAX_REQUESTS_PER_MINUTE,
            },
            **_runtime_meta(),
        }

    # Lifespan hooks
    @app.on_event("startup")
    async def on_startup():
        # If routers were deferred, mount now (still safe)
        if bool(_SETTINGS.DEFER_ROUTER_MOUNT):
            try:
                _mount_routes(app)
            except Exception as e:
                logger.error("Deferred route mounting crashed: %s", e, exc_info=True)
                if _env_bool("ROUTES_STRICT_IMPORT", False):
                    raise

        # Optional engine init (safe)
        err = await _maybe_init_engine()
        if err:
            # Do NOT fail startup by default (Render must bind port)
            # If you want strict behavior: set INIT_ENGINE_STRICT=1
            strict = _env_bool("INIT_ENGINE_STRICT", False)
            if strict:
                logger.error("Engine init failed (strict): %s", err)
                raise RuntimeError(f"Engine init failed: {err}")
            logger.warning("Engine init failed (non-fatal): %s", err)

        logger.info("Startup complete: %s", _runtime_meta())

    @app.on_event("shutdown")
    async def on_shutdown():
        # Close optional clients if available
        for mod_name, fn_name in (
            ("integrations.google_sheets_service", "close"),
            ("integrations.google_sheets_service", "close"),
        ):
            try:
                m = importlib.import_module(mod_name)
                fn = getattr(m, fn_name, None)
                if callable(fn):
                    fn()
            except Exception:
                pass
        logger.info("Shutdown complete.")

    return app


# --------------------------------------------------------------------------------------
# ASGI app export
# --------------------------------------------------------------------------------------
app = create_app()
