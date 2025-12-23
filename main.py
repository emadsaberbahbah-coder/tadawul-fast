# main.py  (FULL REPLACEMENT)
"""
main.py
------------------------------------------------------------
Tadawul Fast Bridge – FastAPI Entry Point (PROD SAFE)

Render runs:
  uvicorn main:app --host 0.0.0.0 --port $PORT --proxy-headers --forwarded-allow-ips="*"

This module MUST expose:
  app = FastAPI(...)
"""

from __future__ import annotations

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
logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO").upper(), format=LOG_FORMAT)
logger = logging.getLogger("main")


# -----------------------------------------------------------------------------
# Settings loader (try config.py, then core.config)
# -----------------------------------------------------------------------------
settings = None
settings_source = None
env_mod = None


def _try_load_settings() -> None:
    global settings, settings_source

    # 1) root config.py (preferred)
    try:
        from config import get_settings  # type: ignore
        settings = get_settings()
        settings_source = "config.get_settings"
        return
    except Exception:
        pass

    # 2) core.config (compat)
    try:
        from core.config import get_settings  # type: ignore
        settings = get_settings()
        settings_source = "core.config.get_settings"
        return
    except Exception:
        pass

    settings = None
    settings_source = None


_try_load_settings()

if settings_source:
    try:
        logging.getLogger().setLevel(str(getattr(settings, "log_level", "INFO")).upper())
    except Exception:
        pass
    logger.info("Settings loaded from %s", settings_source)
else:
    logger.warning("Settings not loaded (config/core.config). Falling back to env.py / OS env only.")

try:
    import env as env_mod  # type: ignore
except Exception:
    env_mod = None


def _get(name: str, default: Any = None) -> Any:
    """
    Lookup order:
      1) settings.<name>
      2) env.<NAME> export
      3) os.getenv(NAME)
    """
    if settings is not None and hasattr(settings, name):
        return getattr(settings, name)
    if env_mod is not None and hasattr(env_mod, name):
        return getattr(env_mod, name)
    return os.getenv(name, default)


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


def _cors_allow_origins() -> List[str]:
    # Prefer config computed list if present
    try:
        if settings is not None and hasattr(settings, "cors_origins_list"):
            lst = getattr(settings, "cors_origins_list")
            if isinstance(lst, list) and lst:
                return lst
    except Exception:
        pass

    cors_all = str(_get("ENABLE_CORS_ALL_ORIGINS", _get("CORS_ALL_ORIGINS", "true"))).strip().lower() in (
        "1", "true", "yes", "y", "on"
    )
    if cors_all:
        return ["*"]

    raw = str(_get("CORS_ORIGINS", "")).strip()
    return [o.strip() for o in raw.split(",") if o.strip()] or []


def _rate_limit_default() -> str:
    try:
        rpm = int(getattr(settings, "rate_limit_per_minute"))
        if rpm > 0:
            return f"{rpm}/minute"
    except Exception:
        pass
    return "240/minute"


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
    attr_candidates: List[str] = ("router",),
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


def _init_engine_best_effort(app_: FastAPI) -> None:
    """
    Optional: create a shared engine instance and store it in app.state.engine
    so all routers reuse it (performance + avoids duplicate init).
    Never raises.
    """
    try:
        # Prefer v2
        mod = import_module("core.data_engine_v2")
        Engine = getattr(mod, "DataEngine", None)
        if Engine is None:
            return
        app_.state.engine = Engine()
        logger.info("Engine initialized and stored in app.state.engine (DataEngine v2).")
        return
    except Exception:
        pass

    try:
        mod = import_module("core.data_engine")
        Engine = getattr(mod, "DataEngine", None)
        if Engine is None:
            return
        app_.state.engine = Engine()
        logger.info("Engine initialized and stored in app.state.engine (legacy DataEngine).")
    except Exception:
        pass


def create_app() -> FastAPI:
    title = _get("app_name", _get("APP_NAME", "Tadawul Fast Bridge"))
    version = _get("version", _get("APP_VERSION", "4.7.1"))
    app_env = _get("env", _get("APP_ENV", "production"))

    app_ = FastAPI(title=str(title), version=str(version))
    app_.state.settings = settings
    app_.state.settings_source = settings_source
    app_.state.app_env = str(app_env)
    app_.state.mount_report = []

    # ------------------------------------------------------------
    # CORS
    # ------------------------------------------------------------
    allow_origins = _cors_allow_origins()
    app_.add_middleware(
        CORSMiddleware,
        allow_origins=allow_origins if allow_origins else [],
        allow_credentials=True,
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

        limiter = Limiter(key_func=get_remote_address, default_limits=[_rate_limit_default()])
        app_.state.limiter = limiter
        app_.add_middleware(SlowAPIMiddleware)

        @app_.exception_handler(RateLimitExceeded)
        async def _rate_limit_handler(request, exc):  # noqa: ANN001
            return JSONResponse(status_code=429, content={"detail": "Rate limit exceeded"})

        logger.info("SlowAPI limiter enabled (default %s).", _rate_limit_default())
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
    # Routes (robust mounting with multiple candidates)
    # FIX: routes_argaam module is ROOT file "routes_argaam.py" not "routes.routes_argaam"
    # ------------------------------------------------------------
    routers_to_mount = [
        ("enriched_quote", ["routes.enriched_quote", "enriched_quote", "core.enriched_quote"]),
        ("ai_analysis", ["routes.ai_analysis", "ai_analysis", "core.ai_analysis"]),
        ("advanced_analysis", ["routes.advanced_analysis", "advanced_analysis", "core.advanced_analysis"]),
        ("routes_argaam", ["routes_argaam", "routes.routes_argaam", "core.routes_argaam"]),
        ("legacy_service", ["routes.legacy_service", "legacy_service", "core.legacy_service"]),
    ]

    for name, candidates in routers_to_mount:
        rep = _mount_router(app_, name=name, candidates=candidates)
        app_.state.mount_report.append(rep)

    # ------------------------------------------------------------
    # System endpoints
    # ------------------------------------------------------------
    @app_.api_route("/", methods=["GET", "HEAD"], tags=["system"])
    async def root():
        return {"status": "ok", "app": app_.title, "version": app_.version, "env": app_.state.app_env}

    @app_.get("/health", tags=["system"])
    async def health():
        try:
            enabled = list(getattr(settings, "enabled_providers", [])) if settings is not None else []
        except Exception:
            enabled = _parse_providers(_get("ENABLED_PROVIDERS", _get("PROVIDERS", "")))

        try:
            ksa = list(getattr(settings, "enabled_ksa_providers", [])) if settings is not None else []
        except Exception:
            ksa = _parse_providers(_get("KSA_PROVIDERS", ""))

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
            "routers_mounted": [m["name"] for m in mounted],
            "routers_failed": [
                {"name": f["name"], "loaded_from": f.get("loaded_from"), "error": (f.get("error") or "")[:4000]}
                for f in failed
            ],
            "time_utc": datetime.now(timezone.utc).isoformat(),
        }

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
        # Safe view of settings / env summary
        try:
            if env_mod and hasattr(env_mod, "safe_env_summary"):
                return {"settings_source": app_.state.settings_source, "env": env_mod.safe_env_summary()}
        except Exception:
            pass
        return {"settings_source": app_.state.settings_source, "env": {"app": app_.title, "version": app_.version, "env": app_.state.app_env}}

    # ------------------------------------------------------------
    # Startup: best-effort shared engine init
    # ------------------------------------------------------------
    @app_.on_event("startup")
    async def _startup():  # noqa: ANN001
        _init_engine_best_effort(app_)

    # Startup banner
    try:
        enabled = list(getattr(settings, "enabled_providers", [])) if settings is not None else []
    except Exception:
        enabled = _parse_providers(_get("ENABLED_PROVIDERS", _get("PROVIDERS", "")))

    try:
        ksa = list(getattr(settings, "enabled_ksa_providers", [])) if settings is not None else []
    except Exception:
        ksa = _parse_providers(_get("KSA_PROVIDERS", ""))

    logger.info("==============================================")
    logger.info("🚀 Tadawul Fast Bridge starting")
    logger.info("   Env: %s | Version: %s", app_.state.app_env, app_.version)
    logger.info("   Settings source: %s", app_.state.settings_source or "(none)")
    logger.info("   Providers: %s", ",".join(enabled) if enabled else "(not set)")
    logger.info("   KSA Providers: %s", ",".join(ksa) if ksa else "(not set)")
    logger.info("   CORS allow origins: %s", "ALL (*)" if allow_origins == ["*"] else str(allow_origins))
    logger.info("==============================================")

    return app_


# REQUIRED BY RENDER
app = create_app()
