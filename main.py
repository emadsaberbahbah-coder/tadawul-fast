#!/usr/bin/env python3
"""
main.py
================================================================================
TADAWUL FAST BRIDGE — RENDER-SAFE FASTAPI ENTRYPOINT (v7.1.1)
================================================================================
Aligned • Deployment-safe • Minimal import chain • Dynamic router mounting

v7.1.1 fix (critical)
- ✅ FIX: If routes.mount_all_routers(app) returns a dict, main.py now reads mounted/missing/errors
  from that dict directly (previous v7.1.0 incorrectly wrapped it and always showed mounted=0).

Keeps v7.1.0 improvements
- ✅ Lifespan-based router mounting (startup-safe, no import-time heavy work)
- ✅ /meta is schema-visible so OpenAPI is never empty
- ✅ Render HEAD probes stable (HEAD /, /livez, /readyz)
"""

from __future__ import annotations

import importlib
import json
import logging
import os
import sys
from contextlib import asynccontextmanager
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

from fastapi import FastAPI, Request, Response
from fastapi.responses import JSONResponse
from starlette.middleware.cors import CORSMiddleware
from starlette.middleware.gzip import GZipMiddleware

# --------------------------------------------------------------------------------------
# Version
# --------------------------------------------------------------------------------------
APP_ENTRY_VERSION = "7.1.1"

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
    APP_NAME: str = "Tadawul Fast Bridge"
    APP_VERSION: str = "dev"
    APP_ENV: str = "production"
    TIMEZONE_DEFAULT: str = "Asia/Riyadh"

    DEBUG: bool = False
    ENABLE_SWAGGER: bool = True
    ENABLE_REDOC: bool = True
    INIT_ENGINE_ON_BOOT: bool = True

    REQUIRE_AUTH: bool = True
    OPEN_MODE: bool = False
    AUTH_HEADER_NAME: str = "X-APP-TOKEN"

    ENABLE_CORS_ALL_ORIGINS: bool = False
    CORS_ORIGINS: str = ""

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
# Auth helper for debug endpoints (best-effort)
# --------------------------------------------------------------------------------------
def _auth_ok(request: Request) -> bool:
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

    allowed = {x for x in (_env_str("APP_TOKEN", ""), _env_str("BACKEND_TOKEN", ""), _env_str("BACKUP_APP_TOKEN", "")) if x}
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
@dataclass
class _RoutesSnapshot:
    mounted: List[str]
    missing: List[str]
    import_errors: Dict[str, str]
    mount_errors: Dict[str, str]
    no_router: Dict[str, str]
    strict: bool
    used_strategy: str


def _import_router_module(module_name: str) -> Tuple[Optional[Any], Optional[BaseException]]:
    try:
        mod = importlib.import_module(module_name)
        return mod, None
    except Exception as e:
        return None, e


def _err_to_str(e: BaseException, limit: int = 700) -> str:
    try:
        s = f"{type(e).__name__}: {e}"
    except Exception:
        s = "UnknownError"
    return s if len(s) <= limit else (s[:limit] + "...(truncated)")


def _mount_routes(app: FastAPI) -> Dict[str, Any]:
    """
    Discover and mount routers from `routes` package (best-effort).
    Does not fail startup unless ROUTES_STRICT_IMPORT=1.
    """
    strict = _env_bool("ROUTES_STRICT_IMPORT", False)

    # 1) Prefer routes package mounter if available
    try:
        routes_pkg = importlib.import_module("routes")

        for fn_name in ("mount_all_routers", "mount_all", "mount_routers", "mount_routes", "mount_all_routes"):
            fn = getattr(routes_pkg, fn_name, None)
            if callable(fn):
                used_strategy = f"routes.{fn_name}"
                try:
                    ret = fn(app)  # type: ignore[misc]

                    # ✅ v7.1.1 FIX: read fields from returned dict directly
                    if isinstance(ret, dict):
                        snap = _RoutesSnapshot(
                            mounted=list(ret.get("mounted", []) or []),
                            missing=list(ret.get("missing", []) or []),
                            import_errors=dict(ret.get("import_errors", {}) or {}),
                            mount_errors=dict(ret.get("mount_errors", {}) or {}),
                            no_router=dict(ret.get("no_router", {}) or {}),
                            strict=bool(ret.get("strict", strict)),
                            used_strategy=str(ret.get("strategy", used_strategy) or used_strategy),
                        )
                        failed_count = len(snap.import_errors) + len(snap.mount_errors) + len(snap.no_router)
                        logger.info("Mounted routers using %s", used_strategy)
                        logger.info(
                            "Routes mount summary: mounted=%s missing=%s failed=%s strategy=%s",
                            len(snap.mounted),
                            len(snap.missing),
                            failed_count,
                            snap.used_strategy,
                        )
                        return {
                            "mounted": snap.mounted,
                            "missing": snap.missing,
                            "import_errors": snap.import_errors,
                            "mount_errors": snap.mount_errors,
                            "no_router": snap.no_router,
                            "strict": snap.strict,
                            "strategy": snap.used_strategy,
                        }

                    # If no dict details returned, still log and return minimal snapshot
                    logger.info("Mounted routers using %s (no snapshot returned)", used_strategy)
                    return {
                        "mounted": [],
                        "missing": [],
                        "import_errors": {},
                        "mount_errors": {},
                        "no_router": {},
                        "strict": strict,
                        "strategy": used_strategy + ":no-details",
                    }

                except Exception as e:
                    logger.warning("%s(app) failed; falling back to plan mount. err=%s", used_strategy, _err_to_str(e))
                    if strict:
                        raise
                break
    except Exception:
        pass

    # 2) Fallback: plan-based mounting (very conservative)
    expected: List[str] = []
    plan: List[str] = []
    try:
        routes_pkg = importlib.import_module("routes")
        expected_fn = getattr(routes_pkg, "get_expected_router_modules", None)
        plan_fn = getattr(routes_pkg, "get_mount_plan", None)
        if callable(expected_fn):
            expected = list(expected_fn())
        if callable(plan_fn):
            plan = list(plan_fn())
    except Exception:
        pass

    if not expected:
        expected = [
            "routes.config",
            "routes.enriched_quote",
            "routes.advanced_analysis",
            "routes.ai_analysis",
            "routes.advisor",
            "routes.investment_advisor",
            "routes.sheets",
        ]
    if not plan:
        plan = expected[:]

    mounted: List[str] = []
    missing: List[str] = []
    import_errors: Dict[str, str] = {}
    mount_errors: Dict[str, str] = {}
    no_router: Dict[str, str] = {}

    for mod_name in plan:
        mod, exc = _import_router_module(mod_name)
        if mod is None:
            if isinstance(exc, ModuleNotFoundError) and getattr(exc, "name", None) == mod_name:
                missing.append(mod_name)
            else:
                import_errors[mod_name] = _err_to_str(exc or Exception("unknown import error"))
            continue

        router_obj = getattr(mod, "router", None)
        mount_fn = getattr(mod, "mount", None)
        try:
            if router_obj is not None:
                app.include_router(router_obj)
                mounted.append(mod_name)
                continue
            if callable(mount_fn):
                mount_fn(app)
                mounted.append(mod_name)
                continue
            no_router[mod_name] = "No `router` object or `mount(app)` found"
        except Exception as e:
            mount_errors[mod_name] = _err_to_str(e)
            if strict:
                raise

    failed_count = len(import_errors) + len(mount_errors) + len(no_router)
    logger.info(
        "Routes mount summary: mounted=%s missing=%s failed=%s strategy=%s",
        len(mounted),
        len(missing),
        failed_count,
        "plan",
    )

    return {
        "mounted": mounted,
        "missing": missing,
        "import_errors": import_errors,
        "mount_errors": mount_errors,
        "no_router": no_router,
        "strict": strict,
        "strategy": "plan",
    }


# --------------------------------------------------------------------------------------
# Built-in endpoints
# --------------------------------------------------------------------------------------
def _runtime_meta(app: Optional[FastAPI] = None) -> Dict[str, Any]:
    snap = None
    try:
        if app is not None:
            snap = getattr(app.state, "routes_snapshot", None)
    except Exception:
        snap = None

    mounted_count = 0
    failed_count = 0
    strategy = ""
    try:
        if isinstance(snap, dict):
            mounted_count = len(snap.get("mounted", []) or [])
            failed_count = len(snap.get("import_errors", {}) or {}) + len(snap.get("mount_errors", {}) or {}) + len(snap.get("no_router", {}) or {})
            strategy = str(snap.get("strategy", "") or "")
    except Exception:
        pass

    return {
        "service": _SETTINGS.APP_NAME,
        "app_version": _SETTINGS.APP_VERSION,
        "entry_version": APP_ENTRY_VERSION,
        "env": _SETTINGS.APP_ENV,
        "timestamp_utc": datetime.now(timezone.utc).isoformat(),
        "python": sys.version.split()[0],
        "routes_mounted_count": mounted_count,
        "routes_failed_count": failed_count,
        "routes_strategy": strategy,
    }


def _install_builtin_routes(app: FastAPI) -> None:
    # Schema-visible so OpenAPI is never empty
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
# Optional engine warm-up (safe)
# --------------------------------------------------------------------------------------
async def _maybe_init_engine() -> Optional[str]:
    if not bool(_SETTINGS.INIT_ENGINE_ON_BOOT):
        return None

    err_v2 = ""
    try:
        de2 = importlib.import_module("core.data_engine_v2")
        init_fn = getattr(de2, "init_engine", None) or getattr(de2, "get_engine", None)
        if callable(init_fn):
            maybe = init_fn()
            if hasattr(maybe, "__await__"):
                await maybe
        return None
    except Exception as e:
        err_v2 = _err_to_str(e)

    err_v1 = ""
    try:
        de1 = importlib.import_module("core.data_engine")
        init_fn = getattr(de1, "init_engine", None) or getattr(de1, "get_engine", None)
        if callable(init_fn):
            maybe = init_fn()
            if hasattr(maybe, "__await__"):
                await maybe
        return None
    except Exception as e:
        err_v1 = _err_to_str(e)

    return f"data_engine_v2 failed: {err_v2} | data_engine failed: {err_v1}"


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
            "missing": [],
            "import_errors": {},
            "mount_errors": {},
            "no_router": {},
            "strict": False,
            "strategy": "",
        }
        app.state.routes_mounted = False

        try:
            snap = _mount_routes(app)
            app.state.routes_snapshot = snap
            app.state.routes_mounted = True
        except Exception as e:
            logger.error("Route mounting crashed: %s", e, exc_info=True)
            if _env_bool("ROUTES_STRICT_IMPORT", False):
                raise

        err = await _maybe_init_engine()
        if err:
            strict = _env_bool("INIT_ENGINE_STRICT", False)
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
            logger.info("Shutdown complete.")

    app = FastAPI(
        title=_SETTINGS.APP_NAME,
        version=_SETTINGS.APP_VERSION,
        docs_url=docs_url,
        redoc_url=redoc_url,
        openapi_url=openapi_url,
        lifespan=lifespan,
    )

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
                "ts_utc": datetime.now(timezone.utc).isoformat(),
            },
        )

    _install_builtin_routes(app)

    @app.get("/_debug/routes", include_in_schema=False)
    async def debug_routes(request: Request):
        if _SETTINGS.APP_ENV == "production" and not _auth_ok(request):
            return JSONResponse(status_code=401, content={"status": "error", "error": "unauthorized"})
        return {"status": "ok", "routes": getattr(request.app.state, "routes_snapshot", {}), **_runtime_meta(app)}

    return app


# --------------------------------------------------------------------------------------
# ASGI export
# --------------------------------------------------------------------------------------
app = create_app()
