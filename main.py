# main.py  (FULL REPLACEMENT)
"""
main.py
------------------------------------------------------------
Tadawul Fast Bridge – FastAPI Entry Point (PROD SAFE + FAST BOOT)

Goals
- Always start FAST on Render (avoid deploy timeouts)
- Provide a dumb health endpoint for Render checks: /healthz
- Defer heavy work (router imports + engine init) after startup
- Robust multi-candidate router mounting + clear diagnostics
- Clean shutdown (lifespan) + best-effort engine close

Render
- Set Health Check Path to: /healthz
- Uvicorn log-level env must be lowercase:
    --log-level ${UVICORN_LOG_LEVEL:-info}
"""

from __future__ import annotations

import asyncio
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
from fastapi.exceptions import RequestValidationError
from fastapi.middleware.cors import CORSMiddleware
from starlette.exceptions import HTTPException as StarletteHTTPException
from starlette.responses import JSONResponse


# ---------------------------------------------------------------------
# Ensure repo root is in sys.path
# ---------------------------------------------------------------------
BASE_DIR = Path(__file__).resolve().parent
if str(BASE_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DIR))


# ---------------------------------------------------------------------
# Logging (bootstrap)
# ---------------------------------------------------------------------
LOG_FORMAT = "%(asctime)s | %(levelname)s | %(name)s | %(message)s"
logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)
logger = logging.getLogger("main")


# =============================================================================
# Helpers
# =============================================================================
_TRUTHY = {"1", "true", "yes", "y", "on", "t"}


def _truthy(v: Any) -> bool:
    return str(v or "").strip().lower() in _TRUTHY


def _parse_list_like(v: Any) -> List[str]:
    """
    Accepts:
      - list
      - "a,b,c"
      - '["a","b"]'
    Returns lowercase trimmed tokens.
    """
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
        logging.getLogger().setLevel(str(level).upper())
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
      3) os.getenv(name)
      4) os.getenv(name.upper())
    """
    if settings is not None and hasattr(settings, name):
        return getattr(settings, name)
    if env_mod is not None and hasattr(env_mod, name):
        return getattr(env_mod, name)
    v = os.getenv(name, None)
    if v is not None:
        return v
    return os.getenv(name.upper(), default)


def _normalize_version(v: Any) -> str:
    s = str(v or "").strip()
    if not s:
        return ""
    if s.lower() in ("unknown", "none", "null"):
        return ""
    return s


def _resolve_version(settings: Optional[object], env_mod: Optional[object]) -> str:
    # Prefer settings.version then APP_VERSION
    v = _normalize_version(_get(settings, env_mod, "version", None))
    if not v:
        v = _normalize_version(_get(settings, env_mod, "APP_VERSION", None))

    # If still empty, use commit short if available
    if not v:
        commit = (os.getenv("RENDER_GIT_COMMIT") or os.getenv("GIT_COMMIT") or "").strip()
        if commit:
            v = commit[:7]

    return v or "dev"


def _cors_allow_origins(settings: Optional[object], env_mod: Optional[object]) -> List[str]:
    # Prefer computed list if present
    try:
        if settings is not None and hasattr(settings, "cors_origins_list"):
            lst = getattr(settings, "cors_origins_list")
            if isinstance(lst, list) and lst:
                return [str(x).strip() for x in lst if str(x).strip()]
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


def _providers_from_settings(settings: Optional[object], env_mod: Optional[object]) -> Tuple[List[str], List[str]]:
    """
    Best-effort providers lists for /health output.
    """
    enabled: List[str] = []
    ksa: List[str] = []

    # Try settings first (if structured)
    try:
        if settings is not None and hasattr(settings, "enabled_providers"):
            enabled = [str(x).strip().lower() for x in (getattr(settings, "enabled_providers") or []) if str(x).strip()]
    except Exception:
        pass

    try:
        if settings is not None and hasattr(settings, "enabled_ksa_providers"):
            ksa = [str(x).strip().lower() for x in (getattr(settings, "enabled_ksa_providers") or []) if str(x).strip()]
    except Exception:
        pass

    # Fallback to env strings
    if not enabled:
        enabled = _parse_list_like(_get(settings, env_mod, "ENABLED_PROVIDERS", _get(settings, env_mod, "PROVIDERS", "")))
    if not ksa:
        ksa = _parse_list_like(_get(settings, env_mod, "KSA_PROVIDERS", ""))

    return enabled, ksa


# =============================================================================
# Router + Engine boot
# =============================================================================
ROUTERS_TO_MOUNT: List[Tuple[str, List[str]]] = [
    ("enriched_quote", ["routes.enriched_quote", "enriched_quote", "core.enriched_quote"]),
    ("ai_analysis", ["routes.ai_analysis", "ai_analysis", "core.ai_analysis"]),
    ("advanced_analysis", ["routes.advanced_analysis", "advanced_analysis", "core.advanced_analysis"]),
    # supports both layouts:
    ("routes_argaam", ["routes_argaam", "routes.routes_argaam", "core.routes_argaam"]),
    # keep compat shim always available:
    ("legacy_service", ["core.legacy_service", "routes.legacy_service", "legacy_service"]),
]

REQUIRED_ROUTERS_DEFAULT = ["enriched_quote", "ai_analysis", "advanced_analysis"]


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
            app_.state.engine_error = None
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
            app_.state.engine_error = None
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

    mounted_names = {r["name"] for r in results if r.get("mounted")}
    required = getattr(app_.state, "required_routers", REQUIRED_ROUTERS_DEFAULT)

    # readiness is based on REQUIRED routers only (legacy_service never blocks readiness)
    app_.state.routers_ready = all(r in mounted_names for r in required)

    logger.info(
        "Router mount finished: mounted=%s failed=%s required_ok=%s",
        [r["name"] for r in results if r.get("mounted")],
        [r["name"] for r in results if not r.get("mounted")],
        app_.state.routers_ready,
    )


async def _background_boot(app_: FastAPI) -> None:
    """
    Runs AFTER startup so Render health checks pass quickly.
    """
    try:
        await asyncio.to_thread(_mount_all_routers, app_)

        init_engine = _truthy(getattr(app_.state, "init_engine_on_boot", "true"))
        if init_engine:
            await asyncio.to_thread(_init_engine_best_effort, app_)

    except Exception as e:
        logger.warning("Background boot failed: %s", e)


async def _maybe_close_engine(app_: FastAPI) -> None:
    eng = getattr(app_.state, "engine", None)
    if eng is None:
        return
    try:
        aclose = getattr(eng, "aclose", None)
        if callable(aclose):
            await aclose()
    except Exception:
        pass


# =============================================================================
# App factory (lifespan)
# =============================================================================
def create_app() -> FastAPI:
    settings, settings_source = _try_load_settings()
    env_mod = _load_env_module()

    log_level = str(_get(settings, env_mod, "log_level", os.getenv("LOG_LEVEL", "INFO"))).upper()
    _safe_set_root_log_level(log_level)

    title = _get(settings, env_mod, "app_name", _get(settings, env_mod, "APP_NAME", "Tadawul Fast Bridge"))
    version = _resolve_version(settings, env_mod)
    app_env = _get(settings, env_mod, "env", _get(settings, env_mod, "APP_ENV", "production"))

    allow_origins = _cors_allow_origins(settings, env_mod)
    allow_credentials = False if allow_origins == ["*"] else True

    @asynccontextmanager
    async def lifespan(app_: FastAPI):
        # state defaults
        app_.state.settings = settings
        app_.state.settings_source = settings_source
        app_.state.app_env = str(app_env)
        app_.state.env_mod_loaded = env_mod is not None

        app_.state.mount_report = []
        app_.state.routers_ready = False
        app_.state.engine_ready = False
        app_.state.engine_error = None

        # controls
        app_.state.defer_router_mount = _truthy(_get(settings, env_mod, "DEFER_ROUTER_MOUNT", "true"))
        app_.state.init_engine_on_boot = _get(settings, env_mod, "INIT_ENGINE_ON_BOOT", "true")

        # required routers can be overridden by env:
        # REQUIRED_ROUTERS="enriched_quote,ai_analysis,advanced_analysis"
        rr = _parse_list_like(_get(settings, env_mod, "REQUIRED_ROUTERS", ""))
        app_.state.required_routers = rr or REQUIRED_ROUTERS_DEFAULT

        logger.info("Settings loaded from %s", app_.state.settings_source or "(none)")
        logger.info("Fast boot: defer_router_mount=%s init_engine_on_boot=%s", app_.state.defer_router_mount, app_.state.init_engine_on_boot)

        # boot
        if app_.state.defer_router_mount:
            app_.state.boot_task = asyncio.create_task(_background_boot(app_))
        else:
            await asyncio.to_thread(_mount_all_routers, app_)
            if _truthy(app_.state.init_engine_on_boot):
                await asyncio.to_thread(_init_engine_best_effort, app_)

        logger.info("==============================================")
        logger.info("🚀 Tadawul Fast Bridge starting")
        logger.info("   Env: %s | Version: %s", app_.state.app_env, version)
        logger.info("   Required routers: %s", ",".join(app_.state.required_routers))
        logger.info("   CORS allow origins: %s", "ALL (*)" if allow_origins == ["*"] else str(allow_origins))
        logger.info("==============================================")

        yield

        # shutdown
        try:
            task = getattr(app_.state, "boot_task", None)
            if task and not task.done():
                task.cancel()
        except Exception:
            pass
        await _maybe_close_engine(app_)

    app_ = FastAPI(title=str(title), version=str(version), lifespan=lifespan)

    # CORS
    app_.add_middleware(
        CORSMiddleware,
        allow_origins=allow_origins if allow_origins else [],
        allow_credentials=allow_credentials,
        allow_methods=["*"],
        allow_headers=["*"],
    )

    # SlowAPI (optional)
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

    # -----------------------------------------------------------------------------
    # Exception handling (keep HTTPException semantics + always JSON)
    # -----------------------------------------------------------------------------
    @app_.exception_handler(StarletteHTTPException)
    async def _http_exc_handler(request, exc: StarletteHTTPException):  # noqa: ANN001
        return JSONResponse(status_code=exc.status_code, content={"status": "error", "detail": exc.detail})

    @app_.exception_handler(RequestValidationError)
    async def _validation_exc_handler(request, exc: RequestValidationError):  # noqa: ANN001
        return JSONResponse(status_code=422, content={"status": "error", "detail": "Validation error", "errors": exc.errors()})

    @app_.exception_handler(Exception)
    async def _unhandled_exc_handler(request, exc: Exception):  # noqa: ANN001
        logger.exception("Unhandled exception: %s", exc)
        return JSONResponse(
            status_code=500,
            content={"status": "error", "error": "Internal Server Error", "detail": str(exc)[:2000]},
        )

    # -----------------------------------------------------------------------------
    # Very fast endpoints
    # -----------------------------------------------------------------------------
    @app_.api_route("/", methods=["GET", "HEAD"], include_in_schema=False)
    async def root():
        return {"status": "ok", "app": app_.title, "version": app_.version, "env": getattr(app_.state, "app_env", "unknown")}

    @app_.api_route("/healthz", methods=["GET", "HEAD"], include_in_schema=False)
    async def healthz():
        return {"status": "ok"}

    @app_.get("/readyz", include_in_schema=False)
    async def readyz():
        init_engine = _truthy(getattr(app_.state, "init_engine_on_boot", "true"))
        if getattr(app_.state, "routers_ready", False) and (getattr(app_.state, "engine_ready", False) or not init_engine):
            return {"status": "ready"}
        return JSONResponse(
            status_code=503,
            content={
                "status": "not_ready",
                "routers_ready": bool(getattr(app_.state, "routers_ready", False)),
                "engine_ready": bool(getattr(app_.state, "engine_ready", False)),
                "engine_error": getattr(app_.state, "engine_error", None),
                "required_routers": getattr(app_.state, "required_routers", []),
            },
        )

    @app_.get("/health", tags=["system"])
    async def health():
        enabled, ksa = _providers_from_settings(settings, env_mod)

        mounted = [r for r in getattr(app_.state, "mount_report", []) if r.get("mounted")]
        failed = [r for r in getattr(app_.state, "mount_report", []) if not r.get("mounted")]

        return {
            "status": "ok",
            "app": app_.title,
            "version": app_.version,
            "env": getattr(app_.state, "app_env", "unknown"),
            "providers": enabled,
            "ksa_providers": ksa,
            "settings_source": getattr(app_.state, "settings_source", None),
            "routers_ready": bool(getattr(app_.state, "routers_ready", False)),
            "engine_ready": bool(getattr(app_.state, "engine_ready", False)),
            "engine_error": getattr(app_.state, "engine_error", None),
            "routers_mounted": [m["name"] for m in mounted],
            "routers_failed": [
                {"name": f["name"], "loaded_from": f.get("loaded_from"), "error": (f.get("error") or "")[:2000]}
                for f in failed
            ],
            "time_utc": datetime.now(timezone.utc).isoformat(),
        }

    @app_.get("/system/routes", tags=["system"])
    async def system_routes():
        return {"mount_report": getattr(app_.state, "mount_report", [])}

    @app_.get("/system/bootstrap", tags=["system"])
    async def system_bootstrap():
        return {
            "defer_router_mount": bool(getattr(app_.state, "defer_router_mount", True)),
            "init_engine_on_boot": getattr(app_.state, "init_engine_on_boot", "true"),
            "routers_ready": bool(getattr(app_.state, "routers_ready", False)),
            "engine_ready": bool(getattr(app_.state, "engine_ready", False)),
            "engine_error": getattr(app_.state, "engine_error", None),
            "required_routers": getattr(app_.state, "required_routers", []),
        }

    @app_.get("/system/info", tags=["system"])
    async def system_info():
        return {
            "cwd": os.getcwd(),
            "base_dir": str(BASE_DIR),
            "sys_path_head": sys.path[:10],
            "python": sys.version,
            "env_mod_loaded": bool(getattr(app_.state, "env_mod_loaded", False)),
            "render_git_commit": (os.getenv("RENDER_GIT_COMMIT") or "")[:12],
        }

    @app_.get("/system/settings", tags=["system"])
    async def system_settings():
        try:
            if env_mod is not None and hasattr(env_mod, "safe_env_summary"):
                return {"settings_source": getattr(app_.state, "settings_source", None), "env": env_mod.safe_env_summary()}
        except Exception:
            pass
        return {"settings_source": getattr(app_.state, "settings_source", None), "env": {"app": app_.title, "version": app_.version}}

    return app_


# REQUIRED BY RENDER
app = create_app()
