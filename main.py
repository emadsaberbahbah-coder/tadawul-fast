# main.py — FULL REPLACEMENT — v5.2.1
"""
main.py
------------------------------------------------------------
Tadawul Fast Bridge – FastAPI Entry Point (PROD SAFE + FAST BOOT) — v5.2.1

What changed vs your v5.2.0 draft (fixes real-world boot issues):
- ✅ Fix: get_engine() may be sync in some builds → supports sync/async safely.
- ✅ Fix: background boot task uses asyncio.to_thread incorrectly for async init → corrected.
- ✅ Fix: router mount uses include_router safely with robust import candidates.
- ✅ /readyz always returns without touching routers/engine internals.
- ✅ Stronger settings/env resolution (ENV -> env.py -> config.py/core.config).
- ✅ Keeps “never crash startup” guarantee.

Entrypoint for Render/uvicorn:  main:app
"""

from __future__ import annotations

import asyncio
import inspect
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
from starlette.responses import JSONResponse, PlainTextResponse, Response

# ---------------------------------------------------------------------
# Path safety (Render/uvicorn import)
# ---------------------------------------------------------------------
BASE_DIR = Path(__file__).resolve().parent
if str(BASE_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DIR))

_TRUTHY = {"1", "true", "yes", "y", "on", "t"}
_FALSY = {"0", "false", "no", "n", "off", "f"}

APP_ENTRY_VERSION = "5.2.1"


def _truthy(v: Any) -> bool:
    return str(v or "").strip().lower() in _TRUTHY


def _clamp_str(s: Any, max_len: int = 2000) -> str:
    txt = (str(s) if s is not None else "").strip()
    if not txt:
        return ""
    if len(txt) <= max_len:
        return txt
    cut = max(0, max_len - 12)
    return txt[:cut] + " ...TRUNC..."


def _export_env_if_missing(key: str, value: Any) -> None:
    if value is None:
        return
    v = str(value).strip()
    if not v:
        return
    cur = os.getenv(key)
    if cur is None or str(cur).strip() == "":
        os.environ[key] = v


def _apply_runtime_env_aliases_last_resort() -> None:
    """
    Extra safety: ensure provider modules AND auth checks that read env directly still work.
    Never overwrites explicit values.
    """
    # --- Auth aliases ---
    tfb_token = (os.getenv("TFB_APP_TOKEN") or "").strip()
    app_token = (os.getenv("APP_TOKEN") or "").strip()
    backup_token = (os.getenv("BACKUP_APP_TOKEN") or "").strip()

    if tfb_token and not app_token:
        _export_env_if_missing("APP_TOKEN", tfb_token)
    if app_token and not tfb_token:
        _export_env_if_missing("TFB_APP_TOKEN", app_token)
    if backup_token:
        _export_env_if_missing("BACKUP_APP_TOKEN", backup_token)

    # --- Provider token aliases ---
    finnhub_key = (os.getenv("FINNHUB_API_KEY") or "").strip()
    eodhd_key = (os.getenv("EODHD_API_KEY") or "").strip()

    if finnhub_key:
        _export_env_if_missing("FINNHUB_API_TOKEN", finnhub_key)
        _export_env_if_missing("FINNHUB_TOKEN", finnhub_key)

    if eodhd_key:
        _export_env_if_missing("EODHD_API_TOKEN", eodhd_key)
        _export_env_if_missing("EODHD_TOKEN", eodhd_key)

    # --- Numeric aliases ---
    if os.getenv("HTTP_TIMEOUT") and not os.getenv("HTTP_TIMEOUT_SEC"):
        _export_env_if_missing("HTTP_TIMEOUT_SEC", os.getenv("HTTP_TIMEOUT"))
    if os.getenv("RETRY_DELAY") and not os.getenv("RETRY_DELAY_SEC"):
        _export_env_if_missing("RETRY_DELAY_SEC", os.getenv("RETRY_DELAY"))

    # --- Engine/provider flags defaults (legacy env-readers consistency) ---
    os.environ.setdefault("ENABLE_YAHOO_CHART_KSA", "true")
    os.environ.setdefault("ENABLE_YAHOO_CHART_SUPPLEMENT", "true")
    os.environ.setdefault("ENABLE_YFINANCE_KSA", "false")
    os.environ.setdefault("ENABLE_YAHOO_FUNDAMENTALS_KSA", "true")
    os.environ.setdefault("ENABLE_YAHOO_FUNDAMENTALS_GLOBAL", "false")
    os.environ.setdefault("ENABLE_HISTORY_ANALYTICS", "true")


def _resolve_log_format() -> str:
    raw = str(os.getenv("LOG_FORMAT", "") or "").strip()
    if not raw:
        return "%(asctime)s | %(levelname)s | %(name)s | %(message)s"
    low = raw.lower()
    if "%(" in raw:
        return raw
    if low in {"detailed", "detail", "full", "verbose"}:
        return "%(asctime)s | %(levelname)s | %(name)s | %(message)s"
    if low in {"simple", "compact"}:
        return "%(levelname)s | %(name)s | %(message)s"
    if low in {"json"}:
        return "%(asctime)s %(levelname)s %(name)s %(message)s"
    return "%(asctime)s | %(levelname)s | %(name)s | %(message)s"


LOG_FORMAT = _resolve_log_format()

# Configure logging only if not already configured
try:
    root_logger = logging.getLogger()
    if not root_logger.handlers:
        logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)
except Exception:
    try:
        logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(name)s | %(message)s")
    except Exception:
        pass

logger = logging.getLogger("main")


def _parse_list_like(v: Any) -> List[str]:
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
        report["error"] = _clamp_str(f"All imports failed. Last traceback:\n{err_tb or '(none)'}", 8000)
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
            report["error"] = _clamp_str(f"get_router() failed:\n{traceback.format_exc()}", 8000)
            logger.warning("Router not mounted (%s): get_router() failed", name)
            return report

    if router_obj is None:
        report["error"] = _clamp_str(
            f"Module '{loaded_from}' imported but no router attr found. attrs tried={list(attr_candidates)}",
            3000,
        )
        logger.warning("Router not mounted (%s): no router found in %s", name, loaded_from)
        return report

    try:
        app_.include_router(router_obj)  # type: ignore[arg-type]
        report["mounted"] = True
        report["loaded_from"] = loaded_from
        report["router_attr"] = router_attr
        logger.info("Mounted router: %s (%s.%s)", name, loaded_from, router_attr)
        return report
    except Exception:
        report["error"] = _clamp_str(f"include_router failed:\n{traceback.format_exc()}", 8000)
        logger.warning("Router not mounted (%s): include_router failed", name)
        return report


def _safe_set_root_log_level(level: str) -> None:
    try:
        logging.getLogger().setLevel(str(level).upper())
    except Exception:
        pass


def _try_load_settings() -> Tuple[Optional[object], Optional[str]]:
    """
    Prefer repo-root config.py. Fall back to core.config.
    """
    try:
        from config import get_settings  # type: ignore

        s = get_settings()
        return s, "config.get_settings"
    except Exception:
        pass

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


def _key_to_attr(name: str) -> str:
    return str(name or "").strip().lower()


def _get(settings: Optional[object], env_mod: Optional[object], name: str, default: Any = None) -> Any:
    """
    Resolution order:
      1) ENV (name / name.upper)
      2) env.py (exact attr, then lowercase)
      3) settings (exact attr, then lowercase)
      4) default
    """
    v = os.getenv(name, None)
    if v is None:
        v = os.getenv(name.upper(), None)
    if v is not None:
        return v

    if env_mod is not None:
        if hasattr(env_mod, name):
            return getattr(env_mod, name)
        low = _key_to_attr(name)
        if hasattr(env_mod, low):
            return getattr(env_mod, low)

    if settings is not None:
        if hasattr(settings, name):
            return getattr(settings, name)
        low = _key_to_attr(name)
        if hasattr(settings, low):
            return getattr(settings, low)

    return default


def _get_env_only(name: str) -> Optional[str]:
    v = os.getenv(name) or os.getenv(name.upper())
    if v is None:
        return None
    s = str(v).strip()
    return s or None


def _normalize_version(v: Any) -> str:
    s = str(v or "").strip()
    if not s:
        return ""
    if s.lower() in ("unknown", "none", "null", "0.0.0"):
        return ""
    return s


def _resolve_version(settings: Optional[object], env_mod: Optional[object]) -> str:
    for k in ("SERVICE_VERSION", "APP_VERSION", "VERSION", "RELEASE"):
        vv = _normalize_version(_get_env_only(k))
        if vv:
            return vv

    if settings is not None:
        for k in ("service_version", "version", "app_version", "APP_VERSION"):
            vv = _normalize_version(getattr(settings, k, None))
            if vv:
                return vv

    commit = (os.getenv("RENDER_GIT_COMMIT") or os.getenv("GIT_COMMIT") or "").strip()
    if commit:
        return commit[:7]

    for k in ("version", "app_version", "APP_VERSION"):
        vv = _normalize_version(_get(settings, env_mod, k, None))
        if vv:
            return vv

    return "dev"


def _resolve_title(settings: Optional[object], env_mod: Optional[object]) -> str:
    if settings is not None:
        for k in ("service_name", "app_name", "APP_NAME", "SERVICE_NAME"):
            vv = str(getattr(settings, k, "") or "").strip()
            if vv:
                return vv

    for k in ("APP_NAME", "SERVICE_NAME", "APP_TITLE", "TITLE"):
        vv = str(_get(settings, env_mod, k, "") or "").strip()
        if vv:
            return vv

    for k in ("app_name", "name"):
        vv = str(_get(settings, env_mod, k, "") or "").strip()
        if vv:
            return vv

    return "Tadawul Fast Bridge"


def _resolve_env_name(settings: Optional[object], env_mod: Optional[object]) -> str:
    if settings is not None:
        for k in ("environment", "env", "APP_ENV", "ENVIRONMENT"):
            vv = str(getattr(settings, k, "") or "").strip()
            if vv:
                return vv
    return str(_get(settings, env_mod, "ENVIRONMENT", _get(settings, env_mod, "APP_ENV", "production"))).strip() or "production"


def _cors_allow_origins(settings: Optional[object], env_mod: Optional[object]) -> List[str]:
    cors_all = _truthy(_get(settings, env_mod, "ENABLE_CORS_ALL_ORIGINS", _get(settings, env_mod, "CORS_ALL_ORIGINS", "true")))
    if cors_all:
        return ["*"]
    raw = str(_get(settings, env_mod, "CORS_ORIGINS", "")).strip()
    return [o.strip() for o in raw.split(",") if o.strip()] or []


def _providers_from_settings(settings: Optional[object], env_mod: Optional[object]) -> Tuple[List[str], List[str]]:
    if settings is not None:
        try:
            enabled = getattr(settings, "enabled_providers", None)
            ksa = getattr(settings, "ksa_providers", None)
            if isinstance(enabled, list) and isinstance(ksa, list):
                return (
                    [str(x).strip().lower() for x in enabled if str(x).strip()],
                    [str(x).strip().lower() for x in ksa if str(x).strip()],
                )
        except Exception:
            pass

    enabled = _parse_list_like(_get(settings, env_mod, "ENABLED_PROVIDERS", _get(settings, env_mod, "PROVIDERS", "")))
    ksa = _parse_list_like(_get(settings, env_mod, "KSA_PROVIDERS", ""))
    return enabled, ksa


def _feature_enabled(settings: Optional[object], env_mod: Optional[object], key: str, default: bool = True) -> bool:
    v = _get(settings, env_mod, key, None)
    if v is None:
        return default
    return _truthy(v)


def _safe_env_snapshot(settings: Optional[object], env_mod: Optional[object]) -> Dict[str, Any]:
    enabled, ksa = _providers_from_settings(settings, env_mod)
    token_mode = "open" if not (os.getenv("APP_TOKEN") or os.getenv("BACKUP_APP_TOKEN") or os.getenv("TFB_APP_TOKEN")) else "token"
    return {
        "APP_ENV": _resolve_env_name(settings, env_mod),
        "LOG_LEVEL": str(_get(settings, env_mod, "LOG_LEVEL", getattr(settings, "log_level", "INFO") if settings else "INFO")),
        "LOG_FORMAT": LOG_FORMAT,
        "ENTRY_VERSION": APP_ENTRY_VERSION,
        "APP_VERSION_RESOLVED": _resolve_version(settings, env_mod),
        "ENABLED_PROVIDERS": enabled,
        "KSA_PROVIDERS": ksa,
        "DEFER_ROUTER_MOUNT": str(_get(settings, env_mod, "DEFER_ROUTER_MOUNT", getattr(settings, "defer_router_mount", True) if settings else True)),
        "INIT_ENGINE_ON_BOOT": str(_get(settings, env_mod, "INIT_ENGINE_ON_BOOT", getattr(settings, "init_engine_on_boot", True) if settings else True)),
        "ENGINE_CACHE_TTL_SEC": str(_get(settings, env_mod, "ENGINE_CACHE_TTL_SEC", "")),
        "ENABLE_HISTORY_ANALYTICS": str(_get(settings, env_mod, "ENABLE_HISTORY_ANALYTICS", os.getenv("ENABLE_HISTORY_ANALYTICS", ""))),
        "ENABLE_YAHOO_CHART_KSA": str(_get(settings, env_mod, "ENABLE_YAHOO_CHART_KSA", os.getenv("ENABLE_YAHOO_CHART_KSA", ""))),
        "ENABLE_YAHOO_CHART_SUPPLEMENT": str(_get(settings, env_mod, "ENABLE_YAHOO_CHART_SUPPLEMENT", os.getenv("ENABLE_YAHOO_CHART_SUPPLEMENT", ""))),
        "ENABLE_YFINANCE_KSA": str(_get(settings, env_mod, "ENABLE_YFINANCE_KSA", os.getenv("ENABLE_YFINANCE_KSA", ""))),
        "ENABLE_YAHOO_FUNDAMENTALS_KSA": str(_get(settings, env_mod, "ENABLE_YAHOO_FUNDAMENTALS_KSA", os.getenv("ENABLE_YAHOO_FUNDAMENTALS_KSA", ""))),
        "ENABLE_YAHOO_FUNDAMENTALS_GLOBAL": str(_get(settings, env_mod, "ENABLE_YAHOO_FUNDAMENTALS_GLOBAL", os.getenv("ENABLE_YAHOO_FUNDAMENTALS_GLOBAL", ""))),
        "AUTH_MODE": token_mode,
        "RENDER_GIT_COMMIT": (os.getenv("RENDER_GIT_COMMIT") or "")[:12],
    }


def _router_plan(settings: Optional[object], env_mod: Optional[object]) -> Tuple[List[Tuple[str, List[str]]], List[str]]:
    ai_enabled = _feature_enabled(settings, env_mod, "AI_ANALYSIS_ENABLED", True)
    adv_enabled = _feature_enabled(settings, env_mod, "ADVANCED_ANALYSIS_ENABLED", True)

    routers: List[Tuple[str, List[str]]] = [
        ("enriched_quote", ["routes.enriched_quote", "enriched_quote", "core.enriched_quote"]),
    ]
    if ai_enabled:
        routers.append(("ai_analysis", ["routes.ai_analysis", "ai_analysis", "core.ai_analysis"]))
    if adv_enabled:
        routers.append(("advanced_analysis", ["routes.advanced_analysis", "advanced_analysis", "core.advanced_analysis"]))

    routers.append(("routes_argaam", ["routes_argaam", "routes.routes_argaam", "core.routes_argaam"]))
    routers.append(("legacy_service", ["core.legacy_service", "routes.legacy_service", "legacy_service"]))

    required = ["enriched_quote"]
    if ai_enabled:
        required.append("ai_analysis")
    if adv_enabled:
        required.append("advanced_analysis")
    return routers, required


# ---------------------------------------------------------------------
# Provider Defaults (applied EARLY)
# ---------------------------------------------------------------------
def _apply_provider_defaults_if_missing() -> None:
    """
    Enforce intended priority WITHOUT overriding explicit env values.
    """
    if not (os.getenv("ENABLED_PROVIDERS") or os.getenv("PROVIDERS")):
        os.environ["ENABLED_PROVIDERS"] = "eodhd,finnhub,fmp"
        os.environ.setdefault("PROVIDERS", os.environ["ENABLED_PROVIDERS"])

    if not os.getenv("KSA_PROVIDERS"):
        os.environ["KSA_PROVIDERS"] = "yahoo_chart,tadawul,argaam"

    os.environ.setdefault("ENABLE_YAHOO_CHART_KSA", "true")
    os.environ.setdefault("ENABLE_YAHOO_CHART_SUPPLEMENT", "true")
    os.environ.setdefault("ENABLE_YFINANCE_KSA", "false")
    os.environ.setdefault("ENABLE_YAHOO_FUNDAMENTALS_KSA", "true")
    os.environ.setdefault("ENABLE_YAHOO_FUNDAMENTALS_GLOBAL", "false")
    os.environ.setdefault("ENABLE_HISTORY_ANALYTICS", "true")


def _engine_info(eng: Any) -> Dict[str, Any]:
    if eng is None:
        return {"engine": "none", "engine_version": None, "providers": [], "ksa_providers": []}

    def _get_list_attr(obj: Any, name: str) -> List[str]:
        try:
            v = getattr(obj, name, None)
            if isinstance(v, list):
                return [str(x).strip().lower() for x in v if str(x).strip()]
        except Exception:
            pass
        return []

    providers: List[str] = []
    ksa: List[str] = []

    for attr in ("providers_global", "enabled_providers", "providers"):
        providers.extend(_get_list_attr(eng, attr))
    for attr in ("providers_ksa", "ksa_providers"):
        ksa.extend(_get_list_attr(eng, attr))

    def _dedup(xs: List[str]) -> List[str]:
        out: List[str] = []
        seen = set()
        for x in xs:
            if x and x not in seen:
                seen.add(x)
                out.append(x)
        return out

    version = getattr(eng, "ENGINE_VERSION", None) or getattr(eng, "engine_version", None) or getattr(eng, "version", None)

    return {
        "engine": type(eng).__name__,
        "engine_version": version,
        "providers": _dedup(providers),
        "ksa_providers": _dedup(ksa),
    }


async def _await_maybe(v: Any) -> Any:
    if inspect.isawaitable(v):
        return await v
    return v


async def _init_engine_best_effort_async(app_: FastAPI) -> None:
    """
    Best-effort engine init. Never throws.
    Sets:
      - app.state.engine
      - app.state.engine_ready
      - app.state.engine_error
    Priority:
      1) existing app.state.engine
      2) core.data_engine_v2.get_engine() (sync OR async)
      3) core.data_engine_v2.DataEngineV2/DataEngine (instantiate)
      4) core.data_engine.DataEngine (legacy)
    """
    try:
        existing = getattr(app_.state, "engine", None)
        if existing is not None:
            app_.state.engine_ready = True
            app_.state.engine_error = None
            logger.info("Engine already present on app.state.engine (%s).", type(existing).__name__)
            return
    except Exception:
        pass

    # 1) Preferred singleton
    try:
        from core.data_engine_v2 import get_engine  # type: ignore

        eng = await _await_maybe(get_engine())
        app_.state.engine = eng
        app_.state.engine_ready = True
        app_.state.engine_error = None
        logger.info("Engine initialized via core.data_engine_v2.get_engine() (singleton).")
        return
    except Exception:
        app_.state.engine_ready = False
        app_.state.engine_error = _clamp_str(traceback.format_exc(), 8000)

    # 2) Fallback instantiate v2
    try:
        mod = import_module("core.data_engine_v2")
        Engine = getattr(mod, "DataEngineV2", None) or getattr(mod, "DataEngine", None)
        if Engine is not None:
            app_.state.engine = Engine()
            app_.state.engine_ready = True
            app_.state.engine_error = None
            logger.info("Engine initialized (core.data_engine_v2.* instantiate).")
            return
    except Exception:
        app_.state.engine_ready = False
        app_.state.engine_error = _clamp_str(traceback.format_exc(), 8000)

    # 3) Legacy
    try:
        mod = import_module("core.data_engine")
        Engine = getattr(mod, "DataEngine", None)
        if Engine is not None:
            app_.state.engine = Engine()
            app_.state.engine_ready = True
            app_.state.engine_error = None
            logger.info("Engine initialized (legacy core.data_engine.DataEngine).")
            return
    except Exception:
        app_.state.engine_ready = False
        app_.state.engine_error = _clamp_str(traceback.format_exc(), 8000)


def _mount_all_routers(app_: FastAPI) -> None:
    routers = getattr(app_.state, "routers_to_mount", [])
    results: List[Dict[str, Any]] = []
    for name, candidates in routers:
        results.append(_mount_router(app_, name=name, candidates=candidates))

    app_.state.mount_report = results
    mounted_names = {r["name"] for r in results if r.get("mounted")}
    required = getattr(app_.state, "required_routers", [])
    app_.state.routers_ready = all(r in mounted_names for r in required)

    logger.info(
        "Router mount finished: mounted=%s failed=%s required_ok=%s",
        [r["name"] for r in results if r.get("mounted")],
        [r["name"] for r in results if not r.get("mounted")],
        app_.state.routers_ready,
    )


async def _background_boot(app_: FastAPI) -> None:
    """
    Background boot must be pure-async. Avoid asyncio.to_thread misuse for async funcs.
    """
    try:
        await asyncio.to_thread(_mount_all_routers, app_)
        init_engine = _truthy(getattr(app_.state, "init_engine_on_boot", "true"))
        if init_engine:
            await _init_engine_best_effort_async(app_)
        app_.state.boot_error = None
    except Exception:
        app_.state.boot_error = _clamp_str(traceback.format_exc(), 8000)
        logger.warning("Background boot failed: %s", app_.state.boot_error)
    finally:
        app_.state.boot_completed = True


async def _maybe_close_engine(app_: FastAPI) -> None:
    eng = getattr(app_.state, "engine", None)
    if eng is None:
        return
    try:
        aclose = getattr(eng, "aclose", None)
        if callable(aclose):
            await aclose()
            return
    except Exception:
        pass
    try:
        close = getattr(eng, "close", None)
        if callable(close):
            close()
    except Exception:
        pass


def create_app() -> FastAPI:
    _apply_provider_defaults_if_missing()
    _apply_runtime_env_aliases_last_resort()

    settings, settings_source = _try_load_settings()
    env_mod = _load_env_module()

    log_level = str(_get(settings, env_mod, "LOG_LEVEL", getattr(settings, "log_level", "INFO") if settings else "INFO")).upper()
    _safe_set_root_log_level(log_level)

    title = _resolve_title(settings, env_mod)
    version = _resolve_version(settings, env_mod)
    app_env = _resolve_env_name(settings, env_mod)

    allow_origins = _cors_allow_origins(settings, env_mod)
    allow_credentials = False if allow_origins == ["*"] else True

    routers_to_mount, required_default = _router_plan(settings, env_mod)

    @asynccontextmanager
    async def lifespan(app_: FastAPI):
        app_.state.settings = settings
        app_.state.settings_source = settings_source
        app_.state.app_env = app_env
        app_.state.env_mod_loaded = env_mod is not None
        app_.state.start_time_utc = datetime.now(timezone.utc).isoformat()

        app_.state.routers_to_mount = routers_to_mount
        app_.state.mount_report = []
        app_.state.routers_ready = False

        app_.state.engine_ready = False
        app_.state.engine_error = None

        app_.state.boot_error = None
        app_.state.boot_completed = False

        # Honor settings attrs even if env vars are not set
        defer_val = _get(settings, env_mod, "DEFER_ROUTER_MOUNT", getattr(settings, "defer_router_mount", True) if settings else True)
        init_val = _get(settings, env_mod, "INIT_ENGINE_ON_BOOT", getattr(settings, "init_engine_on_boot", True) if settings else True)

        app_.state.defer_router_mount = _truthy(defer_val) if isinstance(defer_val, str) else bool(defer_val)
        app_.state.init_engine_on_boot = init_val

        rr = _parse_list_like(_get(settings, env_mod, "REQUIRED_ROUTERS", ""))
        app_.state.required_routers = rr or required_default

        logger.info("Settings loaded from %s", app_.state.settings_source or "(none)")
        logger.info("Fast boot: defer_router_mount=%s init_engine_on_boot=%s", app_.state.defer_router_mount, app_.state.init_engine_on_boot)

        if app_.state.defer_router_mount:
            app_.state.boot_task = asyncio.create_task(_background_boot(app_))
        else:
            await asyncio.to_thread(_mount_all_routers, app_)
            if _truthy(app_.state.init_engine_on_boot):
                await _init_engine_best_effort_async(app_)
            app_.state.boot_completed = True

        enabled, ksa = _providers_from_settings(settings, env_mod)
        logger.info("==============================================")
        logger.info("🚀 Tadawul Fast Bridge starting")
        logger.info("   Env: %s | Version: %s | Entry: %s", app_env, version, APP_ENTRY_VERSION)
        logger.info("   Providers (GLOBAL): %s", ",".join(enabled) if enabled else "(not set)")
        logger.info("   KSA Providers: %s", ",".join(ksa) if ksa else "(not set)")
        logger.info("   Required routers: %s", ",".join(app_.state.required_routers))
        logger.info("   CORS allow origins: %s", "ALL (*)" if allow_origins == ["*"] else str(allow_origins))
        logger.info("   Render commit: %s", (os.getenv("RENDER_GIT_COMMIT") or "")[:12])
        logger.info("==============================================")

        yield

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

    # Optional rate limiting (won’t crash if slowapi missing)
    enable_rl = _feature_enabled(settings, env_mod, "ENABLE_RATE_LIMITING", True)
    if enable_rl:
        try:
            from slowapi import Limiter
            from slowapi.errors import RateLimitExceeded
            from slowapi.middleware import SlowAPIMiddleware
            from slowapi.util import get_remote_address

            rpm = _get(settings, env_mod, "MAX_REQUESTS_PER_MINUTE", 240)
            try:
                rpm_i = int(str(rpm).strip())
            except Exception:
                rpm_i = 240
            default_limit = f"{rpm_i}/minute"

            limiter = Limiter(key_func=get_remote_address, default_limits=[default_limit])
            app_.state.limiter = limiter
            app_.add_middleware(SlowAPIMiddleware)

            @app_.exception_handler(RateLimitExceeded)
            async def _rate_limit_handler(request, exc):  # noqa: ANN001
                return JSONResponse(status_code=429, content={"status": "error", "detail": "Rate limit exceeded"})

            logger.info("SlowAPI limiter enabled (default %s).", default_limit)
        except Exception as e:
            logger.warning("SlowAPI not enabled: %s", e)

    # Exception handlers
    @app_.exception_handler(StarletteHTTPException)
    async def _http_exc_handler(request, exc: StarletteHTTPException):  # noqa: ANN001
        return JSONResponse(status_code=exc.status_code, content={"status": "error", "detail": exc.detail})

    @app_.exception_handler(RequestValidationError)
    async def _validation_exc_handler(request, exc: RequestValidationError):  # noqa: ANN001
        return JSONResponse(
            status_code=422,
            content={"status": "error", "detail": "Validation error", "errors": exc.errors()},
        )

    @app_.exception_handler(Exception)
    async def _unhandled_exc_handler(request, exc: Exception):  # noqa: ANN001
        logger.exception("Unhandled exception: %s", exc)
        return JSONResponse(
            status_code=500,
            content={"status": "error", "error": "Internal Server Error", "detail": _clamp_str(exc)},
        )

    # -----------------------------------------------------------------
    # System routes (always available)
    # -----------------------------------------------------------------
    @app_.api_route("/", methods=["GET", "HEAD"], include_in_schema=False)
    async def root():
        return {
            "status": "ok",
            "app": app_.title,
            "version": app_.version,
            "env": getattr(app_.state, "app_env", "unknown"),
            "entry_version": APP_ENTRY_VERSION,
        }

    @app_.api_route("/favicon.ico", methods=["GET", "HEAD"], include_in_schema=False)
    async def favicon():
        return Response(status_code=204)

    @app_.api_route("/robots.txt", methods=["GET", "HEAD"], include_in_schema=False)
    async def robots():
        return PlainTextResponse("User-agent: *\nDisallow:\n", status_code=200)

    @app_.api_route("/healthz", methods=["GET", "HEAD"], include_in_schema=False)
    async def healthz():
        return {"status": "ok"}

    @app_.api_route("/livez", methods=["GET", "HEAD"], include_in_schema=False)
    async def livez():
        return {"status": "ok"}

    @app_.api_route("/readyz", methods=["GET", "HEAD"], include_in_schema=False)
    async def readyz():
        boot_task = getattr(app_.state, "boot_task", None)
        return {
            "status": "ok",
            "boot_completed": bool(getattr(app_.state, "boot_completed", False)),
            "boot_task_running": bool(boot_task is not None and not boot_task.done()),
            "routers_ready": bool(getattr(app_.state, "routers_ready", False)),
            "engine_ready": bool(getattr(app_.state, "engine_ready", False)),
            "boot_error": getattr(app_.state, "boot_error", None),
            "engine_error": getattr(app_.state, "engine_error", None),
        }

    @app_.get("/health", tags=["system"])
    async def health():
        enabled, ksa = _providers_from_settings(settings, env_mod)
        mounted = [r for r in getattr(app_.state, "mount_report", []) if r.get("mounted")]
        failed = [r for r in getattr(app_.state, "mount_report", []) if not r.get("mounted")]
        boot_task = getattr(app_.state, "boot_task", None)

        eng = getattr(app_.state, "engine", None)
        ei = _engine_info(eng)

        providers = ei["providers"] or enabled
        ksa_providers = ei["ksa_providers"] or ksa

        return {
            "status": "ok",
            "app": app_.title,
            "version": app_.version,
            "env": getattr(app_.state, "app_env", "unknown"),
            "entry_version": APP_ENTRY_VERSION,
            "engine": ei["engine"],
            "engine_version": ei["engine_version"],
            "providers": providers,
            "ksa_providers": ksa_providers,
            "settings_source": getattr(app_.state, "settings_source", None),
            "boot_completed": bool(getattr(app_.state, "boot_completed", False)),
            "boot_task_running": bool(boot_task is not None and not boot_task.done()),
            "routers_ready": bool(getattr(app_.state, "routers_ready", False)),
            "engine_ready": bool(getattr(app_.state, "engine_ready", False)),
            "engine_error": getattr(app_.state, "engine_error", None),
            "boot_error": getattr(app_.state, "boot_error", None),
            "routers_mounted": [m["name"] for m in mounted],
            "routers_failed": [
                {"name": f["name"], "loaded_from": f.get("loaded_from"), "error": _clamp_str(f.get("error") or "", 2000)}
                for f in failed
            ],
            "time_utc": datetime.now(timezone.utc).isoformat(),
        }

    @app_.get("/system/settings", tags=["system"])
    async def system_settings():
        safe = None
        try:
            if settings is not None and hasattr(settings, "as_safe_dict"):
                safe = settings.as_safe_dict()  # type: ignore
        except Exception:
            safe = None

        return {
            "settings_source": getattr(app_.state, "settings_source", None),
            "env_snapshot": _safe_env_snapshot(settings, env_mod),
            "settings_safe": safe,
        }

    @app_.get("/system/info", tags=["system"])
    async def system_info():
        return {
            "service": app_.title,
            "version": app_.version,
            "entry_version": APP_ENTRY_VERSION,
            "environment": getattr(app_.state, "app_env", "unknown"),
            "start_time_utc": getattr(app_.state, "start_time_utc", None),
            "render_git_commit": (os.getenv("RENDER_GIT_COMMIT") or "")[:40],
            "python": sys.version.split(" ")[0],
        }

    return app_


# Uvicorn entrypoint: "main:app"
app = create_app()
