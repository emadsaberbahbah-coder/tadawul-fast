"""
main.py
------------------------------------------------------------
Tadawul Fast Bridge – FastAPI Entry Point (PROD SAFE) – v4.9.0

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
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from importlib import import_module
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from starlette.responses import JSONResponse


# -----------------------------------------------------------------------------
# Ensure repo root is in sys.path (important on some PaaS deploy layouts)
# -----------------------------------------------------------------------------
BASE_DIR = Path(__file__).resolve().parent
if str(BASE_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DIR))


# -----------------------------------------------------------------------------
# Logging (bootstrap; later refined after settings load)
# -----------------------------------------------------------------------------
LOG_FORMAT = "%(asctime)s | %(levelname)s | %(name)s | %(message)s"
logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO").upper(), format=LOG_FORMAT)
logger = logging.getLogger("main")


# -----------------------------------------------------------------------------
# Settings loader (try root config.py, then core.config) + env fallback
# -----------------------------------------------------------------------------
settings: Any = None
settings_source: Optional[str] = None
env_mod: Any = None


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

# optional legacy exports
try:
    import env as env_mod  # type: ignore
except Exception:
    env_mod = None


def _get(name: str, default: Any = None) -> Any:
    """
    Lookup order:
      1) settings.<name>   (pydantic settings object)
      2) env.<NAME>        (legacy constants from env.py)
      3) os.getenv(NAME)
      4) default
    """
    if settings is not None and hasattr(settings, name):
        return getattr(settings, name)
    if env_mod is not None and hasattr(env_mod, name):
        return getattr(env_mod, name)
    return os.getenv(name, default)


def _safe_bool(v: Any, default: bool = False) -> bool:
    if isinstance(v, bool):
        return v
    if v is None:
        return default
    s = str(v).strip().lower()
    if s in {"1", "true", "yes", "y", "on", "t"}:
        return True
    if s in {"0", "false", "no", "n", "off", "f"}:
        return False
    return default


def _parse_providers(v: Any) -> List[str]:
    """Accept list, CSV, or JSON list (string). Returns lower-case list."""
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


def _configure_logging_after_settings() -> None:
    """
    Refine log level once settings are available.
    Avoid leaking secrets; keep formatting stable.
    """
    try:
        lvl = str(_get("log_level", _get("LOG_LEVEL", "INFO"))).upper()
        logging.getLogger().setLevel(lvl)
    except Exception:
        pass


_configure_logging_after_settings()

if settings_source:
    logger.info("Settings loaded from %s", settings_source)
else:
    logger.warning("Settings not loaded (config/core.config). Falling back to env.py / OS env only.")


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
        "1",
        "true",
        "yes",
        "y",
        "on",
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
    """
    Try importing modules in order. Return: (module, loaded_from, error_trace_if_all_fail)
    """
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
    """
    Attempts to import one module from candidates and mount a router attribute.
    Returns a mount report dict (stored in app.state.mount_report).
    """
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

    router = None
    router_attr = None
    for attr in attr_candidates:
        if hasattr(mod, attr):
            router = getattr(mod, attr)
            router_attr = attr
            break

    if router is None and hasattr(mod, "get_router"):
        try:
            router = getattr(mod, "get_router")()
            router_attr = "get_router()"
        except Exception:
            report["error"] = f"get_router() failed:\n{traceback.format_exc()}"
            logger.warning("Router not mounted (%s): get_router() failed", name)
            return report

    if router is None:
        report["error"] = f"Module '{loaded_from}' imported but no router attr found. attrs tried={list(attr_candidates)}"
        logger.warning("Router not mounted (%s): no router found in %s", name, loaded_from)
        return report

    try:
        app_.include_router(router)
        report["mounted"] = True
        report["loaded_from"] = loaded_from
        report["router_attr"] = router_attr
        logger.info("Mounted router: %s (%s.%s)", name, loaded_from, router_attr)
        return report
    except Exception:
        report["error"] = f"include_router failed:\n{traceback.format_exc()}"
        logger.warning("Router not mounted (%s): include_router failed", name)
        return report


def _debug_mode() -> bool:
    # Prefer settings.debug if available; else env DEBUG
    try:
        if settings is not None and hasattr(settings, "debug"):
            return bool(getattr(settings, "debug"))
    except Exception:
        pass
    return _safe_bool(os.getenv("DEBUG"), False)


async def _init_engine(app_: FastAPI) -> Dict[str, Any]:
    """
    Initialize DataEngine v2 and attach to app.state.engine (preferred).
    Never raises (production-safe).
    """
    report: Dict[str, Any] = {"ok": False, "engine": None, "error": None, "providers": []}

    try:
        from core.data_engine_v2 import DataEngine  # type: ignore

        eng = DataEngine()  # sync init (engine does its own lazy IO)
        app_.state.engine = eng
        app_.state.data_engine = eng
        app_.state.data_engine_v2 = eng

        prov = []
        try:
            prov = list(getattr(eng, "enabled_providers", []) or [])
        except Exception:
            prov = []

        report.update({"ok": True, "engine": "DataEngineV2", "providers": prov})
        return report
    except Exception as exc:
        report["error"] = f"{type(exc).__name__}: {exc}"
        logger.warning("Engine init failed: %s", report["error"])
        return report


async def _shutdown_engine(app_: FastAPI) -> None:
    """
    Best-effort cleanup if engine exposes aclose/close.
    Never raises.
    """
    eng = getattr(app_.state, "engine", None)
    if eng is None:
        return

    # async close
    try:
        if hasattr(eng, "aclose") and callable(getattr(eng, "aclose")):
            await eng.aclose()  # type: ignore
            return
    except Exception:
        pass

    # sync close
    try:
        if hasattr(eng, "close") and callable(getattr(eng, "close")):
            eng.close()  # type: ignore
            return
    except Exception:
        pass


@asynccontextmanager
async def _lifespan(app_: FastAPI):
    # Engine init
    app.state.engine_report = await _init_engine(app)

    # Banner
    try:
        title = app.title
        version = app.version
        app_env = getattr(app.state, "app_env", "production")

        try:
            enabled = list(getattr(settings, "enabled_providers", [])) if settings is not None else []
        except Exception:
            enabled = _parse_providers(_get("ENABLED_PROVIDERS", _get("PROVIDERS", "")))

        try:
            ksa = list(getattr(settings, "enabled_ksa_providers", [])) if settings is not None else []
        except Exception:
            ksa = _parse_providers(_get("KSA_PROVIDERS", ""))

        allow_origins = getattr(app.state, "cors_allow_origins", [])
        engine_rep = getattr(app.state, "engine_report", {}) or {}

        logger.info("==============================================")
        logger.info("🚀 Tadawul Fast Bridge starting")
        logger.info("   App: %s | Env: %s | Version: %s", title, app_env, version)
        logger.info("   Settings source: %s", app.state.settings_source or "(none)")
        logger.info("   Engine: %s | Engine providers: %s",
                    engine_rep.get("engine") or "(none)",
                    ",".join(engine_rep.get("providers") or []) if (engine_rep.get("providers") or []) else "(n/a)")
        logger.info("   Providers: %s", ",".join(enabled) if enabled else "(not set)")
        logger.info("   KSA Providers: %s", ",".join(ksa) if ksa else "(not set)")
        logger.info("   CORS allow origins: %s", "ALL (*)" if allow_origins == ["*"] else str(allow_origins))
        logger.info("==============================================")
    except Exception:
        pass

    yield

    # Shutdown cleanup
    await _shutdown_engine(app)


def create_app() -> FastAPI:
    title = _get("app_name", _get("APP_NAME", "Tadawul Fast Bridge"))
    version = _get("version", _get("APP_VERSION", "4.9.0"))
    app_env = _get("env", _get("APP_ENV", "production"))

    app_ = FastAPI(title=str(title), version=str(version), lifespan=_lifespan)
    app_.state.settings = settings
    app_.state.settings_source = settings_source
    app_.state.app_env = str(app_env)
    app_.state.mount_report = []
    app_.state.engine = None  # set in lifespan
    app_.state.data_engine = None
    app_.state.data_engine_v2 = None

    # ------------------------------------------------------------
    # CORS
    # ------------------------------------------------------------
    allow_origins = _cors_allow_origins()
    app_.state.cors_allow_origins = allow_origins

    # If wildcard origin, do NOT allow credentials (browser rejects it).
    allow_credentials = False if allow_origins == ["*"] else True

    app_.add_middleware(
        CORSMiddleware,
        allow_origins=allow_origins if allow_origins else [],
        allow_credentials=allow_credentials,
        allow_methods=["*"],
        allow_headers=["*"],
    )

    # ------------------------------------------------------------
    # Global exception safety (avoid leaking stack traces in prod)
    # ------------------------------------------------------------
    @app_.exception_handler(Exception)
    async def _unhandled_exception_handler(request, exc):  # noqa: ANN001
        dbg = _debug_mode()
        payload: Dict[str, Any] = {
            "detail": "Internal server error",
            "error_type": type(exc).__name__,
        }
        if dbg:
            payload["error"] = str(exc)
            payload["traceback"] = traceback.format_exc()[:12000]
        logger.exception("Unhandled exception: %s", exc)
        return JSONResponse(status_code=500, content=payload)

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
        async def _rate_limit_handler(request, exc):  # noqa: ANN001
            return JSONResponse(status_code=429, content={"detail": "Rate limit exceeded"})

        logger.info("SlowAPI limiter enabled (default %s).", _rate_limit_default())
    except Exception as e:
        logger.warning("SlowAPI not enabled: %s", e)

    # ------------------------------------------------------------
    # Routes (robust mounting with multiple candidates)
    # ------------------------------------------------------------
    routers_to_mount = [
        ("enriched_quote", ["routes.enriched_quote", "enriched_quote", "core.enriched_quote"]),
        ("ai_analysis", ["routes.ai_analysis", "ai_analysis", "core.ai_analysis"]),
        ("advanced_analysis", ["routes.advanced_analysis", "advanced_analysis", "core.advanced_analysis"]),
        # KSA / Argaam gateway (your file is routes_argaam.py at repo root)
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

        engine_rep = getattr(app_.state, "engine_report", {}) or {}
        mounted = [r for r in app_.state.mount_report if r.get("mounted")]
        failed = [r for r in app_.state.mount_report if not r.get("mounted")]

        return {
            "status": "ok",
            "app": app_.title,
            "version": app_.version,
            "env": app_.state.app_env,
            "engine": engine_rep.get("engine") or _get("ENGINE", "DataEngineV2"),
            "engine_ok": bool(engine_rep.get("ok")),
            "engine_providers": engine_rep.get("providers") or [],
            "providers": enabled,
            "ksa_providers": ksa,
            "settings_source": app_.state.settings_source,
            "routers_mounted": [m["name"] for m in mounted],
            "routers_failed": [
                {
                    "name": f["name"],
                    "loaded_from": f.get("loaded_from"),
                    "error": (f.get("error") or "")[:4000],
                }
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
            "debug": _debug_mode(),
        }

    @app_.get("/system/engine", tags=["system"])
    async def system_engine():
        eng = getattr(app_.state, "engine", None)
        rep = getattr(app_.state, "engine_report", {}) or {}
        return {
            "engine_present": bool(eng),
            "engine_report": rep,
            "engine_type": str(type(eng).__name__) if eng is not None else None,
        }

    return app_


# REQUIRED BY RENDER
app = create_app()
