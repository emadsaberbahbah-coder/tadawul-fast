# main.py  (FULL REPLACEMENT)
"""
main.py
------------------------------------------------------------
Tadawul Fast Bridge – FastAPI Entry Point (PROD SAFE + FAST BOOT)

✅ Goals
- Always start FAST on Render (avoid 15-min deploy timeouts)
- Provide a "dumb" health endpoint for Render health checks: /healthz
- Defer heavy work (router imports + engine init) to background after startup
- Keep robust multi-candidate router mounting + safe diagnostics

IMPORTANT (Render):
- Set Health Check Path to: /healthz
- If your Start Command uses --log-level from env, it MUST be lowercase.
  Use:
    --log-level ${UVICORN_LOG_LEVEL:-info}
  And set UVICORN_LOG_LEVEL=info
  (Keep LOG_LEVEL for Python app logging, can be INFO/DEBUG etc.)

Render typical start command:
  uvicorn main:app --host 0.0.0.0 --port $PORT --proxy-headers --forwarded-allow-ips="*"
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import sys
import traceback
from datetime import datetime, timezone
from importlib import import_module
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from starlette.responses import JSONResponse

# -----------------------------------------------------------------------------
# Ensure repo root is in sys.path
# -----------------------------------------------------------------------------
BASE_DIR = Path(__file__).resolve().parent
if str(BASE_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DIR))

# -----------------------------------------------------------------------------
# Logging (bootstrap)
# -----------------------------------------------------------------------------
LOG_FORMAT = "%(asctime)s | %(levelname)s | %(name)s | %(message)s"
logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)
logger = logging.getLogger("main")


# =============================================================================
# Helpers
# =============================================================================
def _truthy(v: Any) -> bool:
    return str(v or "").strip().lower() in ("1", "true", "yes", "y", "on")


def _parse_providers(v: Any) -> List[str]:
    if v is None:
        return []
    if isinstance(v, list):
        return [str(x).strip().lower() for x in v if str(x).strip()]
    s = str(v).strip()
    if not s:
        return []
    if s.startswith("[") and s.endswith("]"):
        try:
            arr = json.loads(s)
            if isinstance(arr, list):
                return [str(x).strip().lower() for x in arr if str(x).strip()]
        except Exception:
            pass
    return [p.strip().lower() for p in s.split(",") if p.strip()]


def _import_first(candidates: List[str]) -> Tuple[Optional[object], Optional[str], Optional[str]]:
    last_tb = None
    for mod_path in candidates:
        try:
            mod = import_module(mod_path)
            return mod, mod_path, None
        except Exception:
            last_tb = traceback.format_exc()
            continue
    return None, None, last_tb


def _mount_router(
    app_: FastAPI,
    name: str,
    candidates: List[str],
    attr_candidates: Tuple[str, ...] = ("router",),
) -> Dict[str, Any]:
    report: Dict[str, Any] = {
        "name": name,
        "candidates": candidates,
        "mounted": False,
        "loaded_from": None,
        "router_attr": None,
        "error": None,
    }

    mod, loaded_from, err_tb = _import_first(candidates)
    if mod is None:
        report["error"] = f"All imports failed. Last traceback:\n{err_tb or '(none)'}"
        logger.warning("Router not mounted (%s): import failed for %s", name, candidates)
        return report

    router_obj = None
    router_attr = None
    for attr in attr_candidates:
        if hasattr(mod, attr):
            router_obj = getattr(mod, attr)
            router_attr = attr
            break

    if router_obj is None and hasattr(mod, "get_router"):
        try:
            router_obj = getattr(mod, "get_router")()
            router_attr = "get_router()"
        except Exception:
            report["error"] = f"get_router() failed:\n{traceback.format_exc()}"
            logger.warning("Router not mounted (%s): get_router() failed", name)
            return report

    if router_obj is None:
        report["error"] = f"Module '{loaded_from}' imported but no router attr found. attrs tried={list(attr_candidates)}"
        logger.warning("Router not mounted (%s): no router found in %s", name, loaded_from)
        return report

    try:
        app_.include_router(router_obj)
        report["mounted"] = True
        report["loaded_from"] = loaded_from
        report["router_attr"] = router_attr
        logger.info("Mounted router: %s (%s.%s)", name, loaded_from, router_attr)
        return report
    except Exception:
        report["error"] = f"include_router failed:\n{traceback.format_exc()}"
        logger.warning("Router not mounted (%s): include_router failed", name)
        return report


def _safe_set_root_log_level(level: str) -> None:
    try:
        logging.getLogger().setLevel(level.upper())
    except Exception:
        pass


def _try_load_settings() -> Tuple[Optional[object], Optional[str]]:
    # 1) root config.py (preferred)
    try:
        from config import get_settings  # type: ignore

        s = get_settings()
        return s, "config.get_settings"
    except Exception:
        pass

    # 2) core.config (compat)
    try:
        from core.config import get_settings  # type: ignore

        s = get_settings()
        return s, "core.config.get_settings"
    except Exception:
        pass

    return None, None


def _load_env_module() -> Optional[object]:
    try:
        import env as env_mod  # type: ignore

        return env_mod
    except Exception:
        return None


def _get(settings: Optional[object], env_mod: Optional[object], name: str, default: Any = None) -> Any:
    """
    Lookup order:
      1) settings.<name>
      2) env.<name>
      3) os.getenv(name)  (exact)
      4) os.getenv(upper name)
    """
    if settings is not None and hasattr(settings, name):
        return getattr(settings, name)
    if env_mod is not None and hasattr(env_mod, name):
        return getattr(env_mod, name)
    v = os.getenv(name, None)
    if v is not None:
        return v
    return os.getenv(name.upper(), default)


def _cors_allow_origins(settings: Optional[object], env_mod: Optional[object]) -> List[str]:
    # Prefer computed list if present
    try:
        if settings is not None and hasattr(settings, "cors_origins_list"):
            lst = getattr(settings, "cors_origins_list")
            if isinstance(lst, list) and lst:
                return lst
    except Exception:
        pass

    cors_all = _truthy(_get(settings, env_mod, "ENABLE_CORS_ALL_ORIGINS", _get(settings, env_mod, "CORS_ALL_ORIGINS", "true")))
    if cors_all:
        return ["*"]

    raw = str(_get(settings, env_mod, "CORS_ORIGINS", "")).strip()
    return [o.strip() for o in raw.split(",") if o.strip()] or []


def _rate_limit_default(settings: Optional[object]) -> str:
    try:
        rpm = int(getattr(settings, "rate_limit_per_minute"))
        if rpm > 0:
            return f"{rpm}/minute"
    except Exception:
        pass
    return "240/minute"


# =============================================================================
# Background tasks: router mount + engine init (so startup stays fast)
# =============================================================================
ROUTERS_TO_MOUNT: List[Tuple[str, List[str]]] = [
    ("enriched_quote", ["routes.enriched_quote", "enriched_quote", "core.enriched_quote"]),
    ("ai_analysis", ["routes.ai_analysis", "ai_analysis", "core.ai_analysis"]),
    ("advanced_analysis", ["routes.advanced_analysis", "advanced_analysis", "core.advanced_analysis"]),
    # FIX: routes_argaam module is ROOT file "routes_argaam.py" not "routes.routes_argaam"
    ("routes_argaam", ["routes_argaam", "routes.routes_argaam", "core.routes_argaam"]),
    ("legacy_service", ["routes.legacy_service", "legacy_service", "core.legacy_service"]),
]


def _init_engine_best_effort(app_: FastAPI) -> None:
    """
    Create shared engine instance and store it in app.state.engine.
    Never raises.
    """
    # Prefer v2
    try:
        mod = import_module("core.data_engine_v2")
        Engine = getattr(mod, "DataEngine", None)
        if Engine is not None:
            app_.state.engine = Engine()
            app_.state.engine_ready = True
            logger.info("Engine initialized and stored in app.state.engine (DataEngine v2).")
            return
    except Exception as e:
        app_.state.engine_ready = False
        app_.state.engine_error = str(e)[:2000]

    # Fallback legacy
    try:
        mod = import_module("core.data_engine")
        Engine = getattr(mod, "DataEngine", None)
        if Engine is not None:
            app_.state.engine = Engine()
            app_.state.engine_ready = True
            logger.info("Engine initialized and stored in app.state.engine (legacy DataEngine).")
            return
    except Exception as e:
        app_.state.engine_ready = False
        app_.state.engine_error = str(e)[:2000]


def _mount_all_routers(app_: FastAPI) -> None:
    results: List[Dict[str, Any]] = []
    for name, candidates in ROUTERS_TO_MOUNT:
        rep = _mount_router(app_, name=name, candidates=candidates)
        results.append(rep)

    app_.state.mount_report = results
    app_.state.routers_ready = all(r.get("mounted") for r in results if r["name"] != "legacy_service") or any(
        r.get("mounted") for r in results
    )
    # legacy_service can fail safely — don’t block readiness on it
    logger.info(
        "Router mount finished: mounted=%s failed=%s",
        [r["name"] for r in results if r.get("mounted")],
        [r["name"] for r in results if not r.get("mounted")],
    )


async def _background_boot(app_: FastAPI) -> None:
    """
    Runs AFTER startup so Render health checks pass quickly.
    """
    try:
        # 1) Mount routers (can be heavy)
        await asyncio.to_thread(_mount_all_routers, app_)

        # 2) Init engine (can be heavy)
        init_engine = _truthy(getattr(app_.state, "init_engine_on_boot", "true"))
        if init_engine:
            await asyncio.to_thread(_init_engine_best_effort, app_)

    except Exception as e:
        logger.warning("Background boot failed: %s", e)


# =============================================================================
# App factory
# =============================================================================
def create_app() -> FastAPI:
    settings, settings_source = _try_load_settings()
    env_mod = _load_env_module()

    # Logging level (app-side). This does NOT control uvicorn --log-level.
    log_level = str(_get(settings, env_mod, "log_level", os.getenv("LOG_LEVEL", "INFO"))).upper()
    _safe_set_root_log_level(log_level)

    title = _get(settings, env_mod, "app_name", _get(settings, env_mod, "APP_NAME", "Tadawul Fast Bridge"))
    version = _get(settings, env_mod, "version", _get(settings, env_mod, "APP_VERSION", "4.7.1"))
    app_env = _get(settings, env_mod, "env", _get(settings, env_mod, "APP_ENV", "production"))

    app_ = FastAPI(title=str(title), version=str(version))

    # state
    app_.state.settings = settings
    app_.state.settings_source = settings_source
    app_.state.app_env = str(app_env)
    app_.state.env_mod_loaded = env_mod is not None

    app_.state.mount_report = []          # will be populated after background boot
    app_.state.routers_ready = False
    app_.state.engine_ready = False
    app_.state.engine_error = None

    # Controls
    app_.state.defer_router_mount = _truthy(_get(settings, env_mod, "DEFER_ROUTER_MOUNT", "true"))
    app_.state.init_engine_on_boot = _get(settings, env_mod, "INIT_ENGINE_ON_BOOT", "true")

    # ------------------------------------------------------------
    # CORS
    # ------------------------------------------------------------
    allow_origins = _cors_allow_origins(settings, env_mod)

    # If "*" is used, allow_credentials MUST be False for browser compliance
    allow_credentials = False if allow_origins == ["*"] else True

    app_.add_middleware(
        CORSMiddleware,
        allow_origins=allow_origins if allow_origins else [],
        allow_credentials=allow_credentials,
        allow_methods=["*"],
        allow_headers=["*"],
    )

    # ------------------------------------------------------------
    # SlowAPI rate limiting (optional)
    # ------------------------------------------------------------
    try:
        from slowapi import Limiter
        from slowapi.errors import RateLimitExceeded
        from slowapi.middleware import SlowAPIMiddleware
        from slowapi.util import get_remote_address

        limiter = Limiter(key_func=get_remote_address, default_limits=[_rate_limit_default(settings)])
        app_.state.limiter = limiter
        app_.add_middleware(SlowAPIMiddleware)

        @app_.exception_handler(RateLimitExceeded)
        async def _rate_limit_handler(request, exc):  # noqa: ANN001
            return JSONResponse(status_code=429, content={"detail": "Rate limit exceeded"})

        logger.info("SlowAPI limiter enabled (default %s).", _rate_limit_default(settings))
    except Exception as e:
        logger.warning("SlowAPI not enabled: %s", e)

    # ------------------------------------------------------------
    # Global exception guard (never crash without JSON)
    # ------------------------------------------------------------
    @app_.exception_handler(Exception)
    async def _unhandled_exc_handler(request, exc):  # noqa: ANN001
        logger.exception("Unhandled exception: %s", exc)
        return JSONResponse(
            status_code=500,
            content={"status": "error", "error": "Internal Server Error", "detail": str(exc)[:2000]},
        )

    # ------------------------------------------------------------
    # System endpoints (VERY FAST)
    # ------------------------------------------------------------
    @app_.api_route("/", methods=["GET", "HEAD"], include_in_schema=False)
    async def root():
        # Keep root extremely fast (Render may probe this)
        return {"status": "ok", "app": app_.title, "version": app_.version, "env": app_.state.app_env}

    @app_.api_route("/healthz", methods=["GET", "HEAD"], include_in_schema=False)
    async def healthz():
        # Dumb health check endpoint for Render
        return {"status": "ok"}

    @app_.get("/health", tags=["system"])
    async def health():
        try:
            enabled = list(getattr(settings, "enabled_providers", [])) if settings is not None else []
        except Exception:
            enabled = _parse_providers(_get(settings, env_mod, "ENABLED_PROVIDERS", _get(settings, env_mod, "PROVIDERS", "")))

        try:
            ksa = list(getattr(settings, "enabled_ksa_providers", [])) if settings is not None else []
        except Exception:
            ksa = _parse_providers(_get(settings, env_mod, "KSA_PROVIDERS", ""))

        mounted = [r for r in app_.state.mount_report if r.get("mounted")]
        failed = [r for r in app_.state.mount_report if not r.get("mounted")]

        return {
            "status": "ok",
            "app": app_.title,
            "version": app_.version,
            "env": app_.state.app_env,
            "providers": enabled,
            "ksa_providers": ksa,
            "settings_source": app_.state.settings_source,
            "routers_ready": bool(app_.state.routers_ready),
            "engine_ready": bool(app_.state.engine_ready),
            "engine_error": app_.state.engine_error,
            "routers_mounted": [m["name"] for m in mounted],
            "routers_failed": [
                {"name": f["name"], "loaded_from": f.get("loaded_from"), "error": (f.get("error") or "")[:4000]}
                for f in failed
            ],
            "time_utc": datetime.now(timezone.utc).isoformat(),
        }

    @app_.get("/readyz", include_in_schema=False)
    async def readyz():
        # Optional “readiness” endpoint (returns 200 only when core components are ready)
        if app_.state.routers_ready and (app_.state.engine_ready or not _truthy(app_.state.init_engine_on_boot)):
            return {"status": "ready"}
        return JSONResponse(
            status_code=503,
            content={"status": "not_ready", "routers_ready": app_.state.routers_ready, "engine_ready": app_.state.engine_ready},
        )

    @app_.get("/system/routes", tags=["system"])
    async def system_routes():
        return {"mount_report": app_.state.mount_report}

    @app_.get("/system/info", tags=["system"])
    async def system_info():
        return {
            "cwd": os.getcwd(),
            "base_dir": str(BASE_DIR),
            "sys_path_head": sys.path[:10],
            "python": sys.version,
        }

    @app_.get("/system/settings", tags=["system"])
    async def system_settings():
        try:
            if env_mod is not None and hasattr(env_mod, "safe_env_summary"):
                return {"settings_source": app_.state.settings_source, "env": env_mod.safe_env_summary()}
        except Exception:
            pass
        return {
            "settings_source": app_.state.settings_source,
            "env": {"app": app_.title, "version": app_.version, "env": app_.state.app_env},
        }

    # ------------------------------------------------------------
    # Startup: FAST BOOT (no heavy imports unless you explicitly disable deferral)
    # ------------------------------------------------------------
    @app_.on_event("startup")
    async def _startup():  # noqa: ANN001
        logger.info("Settings loaded from %s", app_.state.settings_source or "(none)")
        logger.info("Fast boot: defer_router_mount=%s init_engine_on_boot=%s", app_.state.defer_router_mount, app_.state.init_engine_on_boot)

        if not app_.state.defer_router_mount:
            # Mount routers synchronously (not recommended for Render unless you're sure it's fast)
            await asyncio.to_thread(_mount_all_routers, app_)
            if _truthy(app_.state.init_engine_on_boot):
                await asyncio.to_thread(_init_engine_best_effort, app_)
        else:
            # Defer everything heavy
            asyncio.create_task(_background_boot(app_))

        # Startup banner (safe)
        try:
            enabled = list(getattr(settings, "enabled_providers", [])) if settings is not None else []
        except Exception:
            enabled = _parse_providers(_get(settings, env_mod, "ENABLED_PROVIDERS", _get(settings, env_mod, "PROVIDERS", "")))

        try:
            ksa = list(getattr(settings, "enabled_ksa_providers", [])) if settings is not None else []
        except Exception:
            ksa = _parse_providers(_get(settings, env_mod, "KSA_PROVIDERS", ""))

        logger.info("==============================================")
        logger.info("🚀 Tadawul Fast Bridge starting")
        logger.info("   Env: %s | Version: %s", app_.state.app_env, app_.version)
        logger.info("   Providers: %s", ",".join(enabled) if enabled else "(not set)")
        logger.info("   KSA Providers: %s", ",".join(ksa) if ksa else "(not set)")
        logger.info("   CORS allow origins: %s", "ALL (*)" if allow_origins == ["*"] else str(allow_origins))
        logger.info("==============================================")

    return app_


# REQUIRED BY RENDER
app = create_app()
