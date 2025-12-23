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
from datetime import datetime, timezone
from importlib import import_module
from typing import Any, Dict, List, Optional, Sequence, Tuple

from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse

# ------------------------------------------------------------
# Logging (bootstrap)
# ------------------------------------------------------------
LOG_FORMAT = "%(asctime)s | %(levelname)s | %(name)s | %(message)s"
logging.basicConfig(level=os.getenv("LOG_LEVEL", "info").upper(), format=LOG_FORMAT)
logger = logging.getLogger("main")


# ------------------------------------------------------------
# Settings loader (prefer core.config -> config.py; env.py stays backward compatible)
# ------------------------------------------------------------
settings = None
env_mod = None


def _load_settings() -> Any:
    """
    Preferred order:
      1) core.config.get_settings()
      2) config.get_settings()
      3) None (env.py + os.environ will still work)
    """
    # 1) Preferred: core.config (compat shim)
    try:
        from core.config import get_settings as _gs  # type: ignore

        s = _gs()
        logger.info("Settings loaded from core.config")
        return s
    except Exception as exc:
        logger.warning("Settings not available from core.config (ok if shim missing): %s", exc)

    # 2) Secondary: root config.py
    try:
        from config import get_settings as _gs  # type: ignore

        s = _gs()
        logger.info("Settings loaded from config.py")
        return s
    except Exception as exc:
        logger.warning("Settings loader not available (config.py). Falling back to env.py / OS env. Error: %s", exc)

    return None


settings = _load_settings()

try:
    # backward-compatible exports (constants + banner)
    import env as env_mod  # type: ignore
except Exception:
    env_mod = None


def _get_any(names: Sequence[str], default: Any = None) -> Any:
    """
    Lookup order:
      1) settings.<name> (pydantic settings object)
      2) env.<NAME> export (legacy constants)
      3) os.getenv(NAME)
    """
    for name in names:
        if settings is not None and hasattr(settings, name):
            return getattr(settings, name)
        if env_mod is not None and hasattr(env_mod, name):
            return getattr(env_mod, name)
        v = os.getenv(name)
        if v is not None:
            return v
    return default


def _parse_list(v: Any) -> List[str]:
    """Accept list, CSV, or JSON list (string). Returns lower-case stripped list."""
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
    # Prefer config.py computed list if available
    try:
        if settings is not None and hasattr(settings, "cors_origins_list"):
            lst = getattr(settings, "cors_origins_list")
            if isinstance(lst, list) and lst:
                return lst
    except Exception:
        pass

    cors_all = str(_get_any(["ENABLE_CORS_ALL_ORIGINS", "CORS_ALL_ORIGINS"], "true")).strip().lower() in (
        "1",
        "true",
        "yes",
        "y",
        "on",
    )
    if cors_all:
        return ["*"]

    raw = str(_get_any(["CORS_ORIGINS"], "")).strip()
    return [o.strip() for o in raw.split(",") if o.strip()] or []


def _rate_limit_default() -> str:
    # Prefer settings.rate_limit_per_minute if present
    try:
        rpm = int(getattr(settings, "rate_limit_per_minute"))
        if rpm > 0:
            return f"{rpm}/minute"
    except Exception:
        pass
    return str(_get_any(["RATE_LIMIT_DEFAULT"], "240/minute"))


def _mount_router_multi(app_: FastAPI, module_paths: Sequence[str], attr: str = "router") -> Tuple[bool, str]:
    """
    Try mounting a router from multiple possible module paths (robust to refactors).
    Returns (mounted, used_module_path_or_error).
    """
    last_err: Optional[Exception] = None
    for module_path in module_paths:
        try:
            mod = import_module(module_path)
            router = getattr(mod, attr)
            app_.include_router(router)
            logger.info("Mounted router: %s.%s", module_path, attr)
            return True, module_path
        except Exception as e:
            last_err = e
            continue
    err = str(last_err) if last_err else "unknown error"
    logger.warning("Router not mounted (%s): %s", ",".join(module_paths), err)
    return False, err


def _safe_env_summary() -> Dict[str, Any]:
    # If env.py provides a safe summary, use it.
    try:
        if env_mod is not None and hasattr(env_mod, "safe_env_summary"):
            fn = getattr(env_mod, "safe_env_summary")
            if callable(fn):
                return fn()
    except Exception:
        pass

    # Minimal safe summary
    return {
        "app": str(_get_any(["app_name", "APP_NAME"], "Tadawul Fast Bridge")),
        "env": str(_get_any(["env", "APP_ENV"], "production")),
        "version": str(_get_any(["version", "APP_VERSION"], "4.6.0")),
        "providers": _parse_list(_get_any(["enabled_providers", "ENABLED_PROVIDERS", "PROVIDERS"], "")),
        "ksa_providers": _parse_list(_get_any(["enabled_ksa_providers", "KSA_PROVIDERS"], "")),
        "cors_all": _cors_allow_origins() == ["*"],
    }


def create_app() -> FastAPI:
    title = _get_any(["app_name", "APP_NAME"], "Tadawul Fast Bridge")
    version = _get_any(["version", "APP_VERSION"], "4.6.0")
    app_env = _get_any(["env", "APP_ENV"], "production")

    app_ = FastAPI(title=str(title), version=str(version))
    app_.state.settings = settings
    app_.state.app_env = str(app_env)

    # ------------------------------------------------------------
    # CORS
    # Note: If allow_origins == ["*"], we set allow_credentials=False
    # to avoid invalid CORS combination in some clients/browsers.
    # ------------------------------------------------------------
    allow_origins = _cors_allow_origins()
    allow_credentials = False if allow_origins == ["*"] else True

    app_.add_middleware(
        CORSMiddleware,
        allow_origins=allow_origins if allow_origins else [],
        allow_credentials=allow_credentials,
        allow_methods=["*"],
        allow_headers=["*"],
    )

    # ------------------------------------------------------------
    # SlowAPI rate limiting (optional, defensive)
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
        async def _rate_limit_handler(request: Request, exc: Exception):  # noqa: ARG001
            return JSONResponse(status_code=429, content={"detail": "Rate limit exceeded"})

        logger.info("SlowAPI limiter enabled (default %s).", _rate_limit_default())
    except Exception as e:
        logger.warning("SlowAPI not enabled: %s", e)

    # ------------------------------------------------------------
    # Routes (defensive mounting + multi-path fallback)
    # ------------------------------------------------------------
    mounted: Dict[str, Any] = {}

    ok, used = _mount_router_multi(app_, ["routes.enriched_quote", "enriched_quote"], attr="router")
    mounted["enriched_quote"] = {"mounted": ok, "source": used}

    ok, used = _mount_router_multi(app_, ["routes.ai_analysis", "ai_analysis"], attr="router")
    mounted["ai_analysis"] = {"mounted": ok, "source": used}

    ok, used = _mount_router_multi(app_, ["routes.advanced_analysis", "advanced_analysis"], attr="router")
    mounted["advanced_analysis"] = {"mounted": ok, "source": used}

    ok, used = _mount_router_multi(app_, ["routes_argaam", "routes.routes_argaam"], attr="router")
    mounted["routes_argaam"] = {"mounted": ok, "source": used}

    ok, used = _mount_router_multi(app_, ["legacy_service", "routes.legacy_service", "core.legacy_service"], attr="router")
    mounted["legacy_service"] = {"mounted": ok, "source": used}

    app_.state.mounted_routers = mounted

    # ------------------------------------------------------------
    # System endpoints
    # ------------------------------------------------------------
    @app_.api_route("/", methods=["GET", "HEAD"], tags=["system"])
    async def root():
        return {"status": "ok", "app": app_.title, "version": app_.version, "env": app_.state.app_env}

    @app_.get("/health", tags=["system"])
    async def health():
        enabled = []
        ksa = []

        # Prefer config.py computed lists
        try:
            enabled = list(getattr(settings, "enabled_providers", [])) if settings is not None else []
        except Exception:
            enabled = _parse_list(_get_any(["ENABLED_PROVIDERS", "PROVIDERS"], ""))

        try:
            ksa = list(getattr(settings, "enabled_ksa_providers", [])) if settings is not None else []
        except Exception:
            ksa = _parse_list(_get_any(["KSA_PROVIDERS"], ""))

        engine = _get_any(["engine", "ENGINE"], "DataEngineV2")

        return {
            "status": "ok",
            "app": app_.title,
            "version": app_.version,
            "env": app_.state.app_env,
            "engine": engine,
            "providers": enabled,
            "ksa_providers": ksa,
            "routers": app_.state.mounted_routers,
            "time_utc": datetime.now(timezone.utc).isoformat(),
        }

    @app_.get("/system/info", tags=["system"])
    async def system_info():
        return _safe_env_summary()

    @app_.get("/system/routes", tags=["system"])
    async def system_routes():
        # Simple visibility: which routes exist (helps debugging quickly)
        return {
            "count": len(list(app_.routes)),
            "paths": sorted({getattr(r, "path", "") for r in app_.routes if getattr(r, "path", None)}),
            "mounted_routers": app_.state.mounted_routers,
        }

    # ------------------------------------------------------------
    # Startup banner (single place)
    # ------------------------------------------------------------
    summary = _safe_env_summary()
    logger.info("==============================================")
    logger.info("🚀 Tadawul Fast Bridge starting")
    logger.info("   Env: %s | Version: %s", summary.get("env"), summary.get("version"))
    logger.info("   Providers: %s", ",".join(summary.get("providers", []) or []) or "(not set)")
    logger.info("   KSA Providers: %s", ",".join(summary.get("ksa_providers", []) or []) or "(not set)")
    logger.info("   CORS allow origins: %s", "ALL (*)" if allow_origins == ["*"] else str(allow_origins))
    logger.info("==============================================")

    return app_


# REQUIRED BY RENDER: uvicorn main:app
app = create_app()
