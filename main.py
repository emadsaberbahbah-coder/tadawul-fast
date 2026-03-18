#!/usr/bin/env python3
"""
main.py
================================================================================
TADAWUL FAST BRIDGE — RENDER-SAFE FASTAPI ENTRYPOINT (v7.7.0)
================================================================================
ALIGNED • DEPLOYMENT-SAFE • PRE-MOUNT + STARTUP-VERIFY • ROUTER-SNAPSHOT-AWARE
ENGINE-STATE-AWARE • OPENAPI-SAFE • RENDER-HEALTH-PROBE-SAFE • PRIORITY-MOUNTED
ROOT-CONFIG-FIRST • CORE-CONFIG-COMPATIBLE • REQUEST-ID SAFE • STARTUP-HARDENED
ORJSON-READY • PREEXISTING-ENGINE SAFE • ROUTE-VERIFY SAFE • DEBUG-HARDENED
V1-HEALTH-ALIAS SAFE • V1-META-ALIAS SAFE • HEAD-PROBE SAFE

Why this revision (v7.7.0)
--------------------------
- ✅ FIX: adds `/v1/health` so health coverage can reach 100%
- ✅ FIX: adds `/v1/healthz`, `/v1/livez`, `/v1/readyz`, `/v1/meta`, `/v1/ping`
- ✅ FIX: keeps HEAD probes stable for both root and `/v1/*` health/meta aliases
- ✅ FIX: built-in meta path registry now includes `/v1/*` aliases for route-signature consistency
- ✅ SAFE: preserves existing router-mount, engine-init, OpenAPI, auth, and shutdown behavior
- ✅ SAFE: no route-family behavior changes for enriched / analysis / advisor / advanced
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
from typing import Any, Dict, List, Optional, Set, Tuple

from fastapi import FastAPI, Request, Response
from fastapi.responses import JSONResponse
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.middleware.cors import CORSMiddleware
from starlette.middleware.gzip import GZipMiddleware

try:
    from fastapi.responses import ORJSONResponse as _FastAPI_ORJSONResponse  # type: ignore
except Exception:
    _FastAPI_ORJSONResponse = JSONResponse  # type: ignore


# --------------------------------------------------------------------------------------
# Version
# --------------------------------------------------------------------------------------
APP_ENTRY_VERSION = "7.7.0"


# --------------------------------------------------------------------------------------
# Safe env helpers
# --------------------------------------------------------------------------------------
_TRUTHY = {"1", "true", "yes", "y", "on", "t", "enabled", "enable"}
_FALSY = {"0", "false", "no", "n", "off", "f", "disabled", "disable"}

_BUILTIN_META_PATHS = {
    "/",
    "/meta",
    "/ping",
    "/livez",
    "/readyz",
    "/health",
    "/healthz",
    "/v1/meta",
    "/v1/ping",
    "/v1/livez",
    "/v1/readyz",
    "/v1/health",
    "/v1/healthz",
    "/_debug/routes",
    "/docs",
    "/redoc",
    "/openapi.json",
}


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


def _pick_attr(obj: Any, *names: str, default: Any = None) -> Any:
    for name in names:
        try:
            value = getattr(obj, name)
            if value is not None:
                return value
        except Exception:
            continue
    return default


def _to_bool(value: Any, default: bool = False) -> bool:
    if isinstance(value, bool):
        return value
    if value is None:
        return default
    s = str(value).strip().lower()
    if s in _TRUTHY:
        return True
    if s in _FALSY:
        return False
    return default


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

    handler = logging.StreamHandler(sys.stdout)
    if log_json:
        handler.setFormatter(_JsonFormatter())
    else:
        handler.setFormatter(logging.Formatter("%(asctime)s | %(levelname)-8s | %(name)s | %(message)s"))
    root.addHandler(handler)

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

    CONFIG_SOURCE: str = "env"


def _settings_from_generic_object(s: Any, source: str) -> _SettingsView:
    return _SettingsView(
        APP_NAME=str(
            _pick_attr(s, "APP_NAME", "app_name", "service_name", default=_env_str("APP_NAME", "Tadawul Fast Bridge"))
        ),
        APP_VERSION=str(
            _pick_attr(s, "APP_VERSION", "app_version", default=_env_str("APP_VERSION", "dev"))
        ),
        APP_ENV=str(
            _pick_attr(s, "APP_ENV", "app_env", "environment", "env", default=_env_str("APP_ENV", "production"))
        ),
        TIMEZONE_DEFAULT=str(
            _pick_attr(s, "TIMEZONE_DEFAULT", "timezone_default", "timezone", default=_env_str("TZ", "Asia/Riyadh"))
        ),
        DEBUG=_to_bool(_pick_attr(s, "DEBUG", "debug", default=_env_bool("DEBUG", False)), _env_bool("DEBUG", False)),
        ENABLE_SWAGGER=_to_bool(
            _pick_attr(s, "ENABLE_SWAGGER", "enable_swagger", default=_env_bool("ENABLE_SWAGGER", True)),
            _env_bool("ENABLE_SWAGGER", True),
        ),
        ENABLE_REDOC=_to_bool(
            _pick_attr(s, "ENABLE_REDOC", "enable_redoc", default=_env_bool("ENABLE_REDOC", True)),
            _env_bool("ENABLE_REDOC", True),
        ),
        INIT_ENGINE_ON_BOOT=_to_bool(
            _pick_attr(s, "INIT_ENGINE_ON_BOOT", "init_engine_on_boot", default=_env_bool("INIT_ENGINE_ON_BOOT", True)),
            _env_bool("INIT_ENGINE_ON_BOOT", True),
        ),
        INIT_ENGINE_STRICT=_to_bool(
            _pick_attr(s, "INIT_ENGINE_STRICT", "init_engine_strict", default=_env_bool("INIT_ENGINE_STRICT", False)),
            _env_bool("INIT_ENGINE_STRICT", False),
        ),
        REQUIRE_AUTH=_to_bool(
            _pick_attr(s, "REQUIRE_AUTH", "require_auth", default=_env_bool("REQUIRE_AUTH", True)),
            _env_bool("REQUIRE_AUTH", True),
        ),
        OPEN_MODE=_to_bool(
            _pick_attr(s, "OPEN_MODE", "open_mode", default=_env_bool("OPEN_MODE", False)),
            _env_bool("OPEN_MODE", False),
        ),
        AUTH_HEADER_NAME=str(
            _pick_attr(
                s,
                "AUTH_HEADER_NAME",
                "auth_header_name",
                default=_env_str("AUTH_HEADER_NAME", "X-APP-TOKEN"),
            )
        ),
        ENABLE_CORS_ALL_ORIGINS=_to_bool(
            _pick_attr(
                s,
                "ENABLE_CORS_ALL_ORIGINS",
                "enable_cors_all_origins",
                default=_env_bool("ENABLE_CORS_ALL_ORIGINS", False),
            ),
            _env_bool("ENABLE_CORS_ALL_ORIGINS", False),
        ),
        CORS_ORIGINS=str(
            _pick_attr(s, "CORS_ORIGINS", "cors_origins", default=_env_str("CORS_ORIGINS", ""))
        ),
        BACKEND_BASE_URL=str(
            _pick_attr(s, "BACKEND_BASE_URL", "backend_base_url", default=_env_str("BACKEND_BASE_URL", ""))
        ),
        ENGINE_CACHE_TTL_SEC=int(
            _pick_attr(s, "ENGINE_CACHE_TTL_SEC", "engine_cache_ttl_sec", default=_env_int("ENGINE_CACHE_TTL_SEC", 20))
        ),
        MAX_REQUESTS_PER_MINUTE=int(
            _pick_attr(s, "MAX_REQUESTS_PER_MINUTE", "max_requests_per_minute", default=_env_int("MAX_REQUESTS_PER_MINUTE", 240))
        ),
        CONFIG_SOURCE=source,
    )


def _load_settings() -> _SettingsView:
    # 1) Root config.py (primary)
    try:
        cfg = importlib.import_module("config")
        getter = getattr(cfg, "get_settings_cached", None) or getattr(cfg, "get_settings", None)
        if callable(getter):
            return _settings_from_generic_object(getter(), source="config")
    except Exception:
        pass

    # 2) env.py
    try:
        env_mod = importlib.import_module("env")
        getter = getattr(env_mod, "get_settings", None)
        if callable(getter):
            return _settings_from_generic_object(getter(), source="env")
    except Exception:
        pass

    # 3) core.config (fallback compatibility)
    try:
        cfg = importlib.import_module("core.config")
        getter = getattr(cfg, "get_settings_cached", None) or getattr(cfg, "get_settings", None)
        if callable(getter):
            return _settings_from_generic_object(getter(), source="core.config")
    except Exception:
        pass

    # 4) raw env vars
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
        CONFIG_SOURCE="raw_env",
    )


_SETTINGS = _load_settings()


# --------------------------------------------------------------------------------------
# Default state helpers
# --------------------------------------------------------------------------------------
def _default_routes_snapshot() -> Dict[str, Any]:
    return {
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
        "missing_required_keys": [],
        "resolved_map": {},
        "module_to_key": {},
        "mount_modes": {},
        "expected_router_modules": [],
        "plan": [],
        "resolved_entries": [],
        "openapi_route_count_after_mount": 0,
        "route_signature_count_after_mount": 0,
    }


def _ensure_app_state_defaults(app: FastAPI) -> None:
    if not hasattr(app.state, "routes_snapshot") or not isinstance(getattr(app.state, "routes_snapshot", None), dict):
        app.state.routes_snapshot = _default_routes_snapshot()
    if not hasattr(app.state, "routes_mounted"):
        app.state.routes_mounted = False
    if not hasattr(app.state, "routes_mount_phase"):
        app.state.routes_mount_phase = ""
    if not hasattr(app.state, "engine"):
        app.state.engine = None
    if not hasattr(app.state, "engine_source"):
        app.state.engine_source = ""
    if not hasattr(app.state, "engine_init_error"):
        app.state.engine_init_error = ""
    if not hasattr(app.state, "config_source"):
        app.state.config_source = _SETTINGS.CONFIG_SOURCE
    if not hasattr(app.state, "settings"):
        app.state.settings = _SETTINGS
    if not hasattr(app.state, "startup_warnings"):
        app.state.startup_warnings = []


# --------------------------------------------------------------------------------------
# Auth helper for debug endpoints
# --------------------------------------------------------------------------------------
def _call_auth_ok_flexible(fn: Any, request: Request, token_value: str, authorization: str) -> bool:
    path = str(getattr(getattr(request, "url", None), "path", "") or "")
    headers_dict = dict(request.headers)

    settings = None
    for mod_name in ("config", "core.config"):
        try:
            cfg = importlib.import_module(mod_name)
            getter = getattr(cfg, "get_settings_cached", None) or getattr(cfg, "get_settings", None)
            if callable(getter):
                settings = getter()
                break
        except Exception:
            continue

    attempts = [
        {
            "token": token_value or None,
            "authorization": authorization or None,
            "headers": headers_dict,
            "path": path,
            "request": request,
            "settings": settings,
        },
        {
            "token": token_value or None,
            "authorization": authorization or None,
            "headers": headers_dict,
            "path": path,
            "request": request,
        },
        {
            "token": token_value or None,
            "authorization": authorization or None,
            "headers": headers_dict,
            "path": path,
        },
        {
            "token": token_value or None,
            "authorization": authorization or None,
            "headers": headers_dict,
        },
        {
            "token": token_value or None,
            "authorization": authorization or None,
        },
        {
            "token": token_value or None,
        },
    ]

    for kwargs in attempts:
        try:
            return bool(fn(**kwargs))
        except TypeError:
            continue
        except Exception:
            return False
    return False


def _auth_ok(request: Request) -> bool:
    try:
        if bool(getattr(_SETTINGS, "OPEN_MODE", False)):
            return True
        if not bool(getattr(_SETTINGS, "REQUIRE_AUTH", True)):
            return True
    except Exception:
        pass

    hdr = request.headers.get(_SETTINGS.AUTH_HEADER_NAME, "") or request.headers.get("X-APP-TOKEN", "")
    auth = request.headers.get("Authorization", "")
    token_q = request.query_params.get("token") if request.query_params else None
    token_value = str(hdr or token_q or "").strip()

    for mod_name in ("config", "core.config"):
        try:
            cfg = importlib.import_module(mod_name)
            fn = getattr(cfg, "auth_ok", None)
            if callable(fn):
                return _call_auth_ok_flexible(fn, request, token_value, auth)
        except Exception:
            continue

    allowed = {
        x
        for x in (
            _env_str("APP_TOKEN", ""),
            _env_str("BACKEND_TOKEN", ""),
            _env_str("BACKUP_APP_TOKEN", ""),
            _env_str("X_APP_TOKEN", ""),
            _env_str("AUTH_TOKEN", ""),
            _env_str("TOKEN", ""),
            _env_str("TFB_APP_TOKEN", ""),
        )
        if x
    }
    if not allowed:
        return False

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
def _app_route_signature_set(app: Any, *, include_builtin: bool = True) -> Set[Tuple[str, str]]:
    sigs: Set[Tuple[str, str]] = set()
    for r in getattr(app, "routes", []) or []:
        path = str(getattr(r, "path", "") or "")
        if not include_builtin and path in _BUILTIN_META_PATHS:
            continue
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


def _route_signature_count(app: Any, *, include_builtin: bool = True) -> int:
    return len(_app_route_signature_set(app, include_builtin=include_builtin))


def _invalidate_openapi_cache(app: FastAPI) -> None:
    try:
        app.openapi_schema = None  # type: ignore[attr-defined]
    except Exception:
        pass


def _normalize_routes_snapshot(
    ret: Any,
    *,
    used_strategy: str,
    app: Optional[FastAPI] = None,
) -> Dict[str, Any]:
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
    openapi_route_count_after_mount = int(
        src.get("openapi_route_count_after_mount", len(getattr(app, "routes", []) or []) if app is not None else 0) or 0
    )
    route_signature_count_after_mount = int(
        src.get(
            "route_signature_count_after_mount",
            _route_signature_count(app, include_builtin=True) if app is not None else 0,
        ) or 0
    )

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
        "route_signature_count_after_mount": route_signature_count_after_mount,
    }


def _merge_snapshots(old: Dict[str, Any], new: Dict[str, Any]) -> Dict[str, Any]:
    def _merge_list(a: Any, b: Any) -> List[Any]:
        out: List[Any] = []
        seen = set()
        for item in list(a or []) + list(b or []):
            key = str(item)
            if key in seen:
                continue
            seen.add(key)
            out.append(item)
        return out

    def _merge_dict(a: Any, b: Any) -> Dict[str, Any]:
        out = dict(a or {})
        out.update(dict(b or {}))
        return out

    merged = {
        "mounted": _merge_list(old.get("mounted"), new.get("mounted")),
        "duplicate_skips": _merge_list(old.get("duplicate_skips"), new.get("duplicate_skips")),
        "missing": _merge_list(old.get("missing"), new.get("missing")),
        "import_errors": _merge_dict(old.get("import_errors"), new.get("import_errors")),
        "mount_errors": _merge_dict(old.get("mount_errors"), new.get("mount_errors")),
        "no_router": _merge_dict(old.get("no_router"), new.get("no_router")),
        "strict": bool(new.get("strict", old.get("strict", False))),
        "strategy": str(new.get("strategy") or old.get("strategy") or ""),
        "missing_required_keys": _merge_list(old.get("missing_required_keys"), new.get("missing_required_keys")),
        "resolved_map": _merge_dict(old.get("resolved_map"), new.get("resolved_map")),
        "module_to_key": _merge_dict(old.get("module_to_key"), new.get("module_to_key")),
        "mount_modes": _merge_dict(old.get("mount_modes"), new.get("mount_modes")),
        "expected_router_modules": _merge_list(old.get("expected_router_modules"), new.get("expected_router_modules")),
        "plan": _merge_list(old.get("plan"), new.get("plan")),
        "resolved_entries": _merge_list(old.get("resolved_entries"), new.get("resolved_entries")),
    }

    merged["mounted_count"] = len(merged["mounted"])
    merged["duplicate_skips_count"] = len(merged["duplicate_skips"])
    merged["missing_count"] = len(merged["missing"])
    merged["failed_count"] = len(merged["import_errors"]) + len(merged["mount_errors"]) + len(merged["no_router"])
    merged["openapi_route_count_after_mount"] = int(
        new.get("openapi_route_count_after_mount", old.get("openapi_route_count_after_mount", 0)) or 0
    )
    merged["route_signature_count_after_mount"] = int(
        new.get("route_signature_count_after_mount", old.get("route_signature_count_after_mount", 0)) or 0
    )
    return merged


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
        "routes.data_dictionary",
        "routes.analysis_sheet_rows",
        "routes.advanced_sheet_rows",
        "routes.advanced_analysis",
        "routes.ai_analysis",
        "routes.advisor",
        "routes.investment_advisor",
        "routes.top10_investments",
        "routes.enriched_quote",
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

    snap = {
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
        "route_signature_count_after_mount": _route_signature_count(app, include_builtin=True),
    }
    _invalidate_openapi_cache(app)
    return snap


def _mount_routes(app: FastAPI) -> Dict[str, Any]:
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
                    snap["openapi_route_count_after_mount"] = len(getattr(app, "routes", []) or [])
                    snap["route_signature_count_after_mount"] = _route_signature_count(app, include_builtin=True)
                    _invalidate_openapi_cache(app)
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


def _should_reverify_prestart_snapshot(app: FastAPI, snap: Dict[str, Any]) -> bool:
    if not isinstance(snap, dict) or not snap:
        return True

    current_sig_count = _route_signature_count(app, include_builtin=True)
    snap_sig_count = int(snap.get("route_signature_count_after_mount", 0) or 0)
    snap_route_count = int(snap.get("openapi_route_count_after_mount", 0) or 0)
    mounted_count = int(snap.get("mounted_count", len(snap.get("mounted", []) or [])) or 0)

    if current_sig_count <= 0:
        return True

    if snap_sig_count > 0 and current_sig_count + 1 < snap_sig_count:
        return True

    if snap_route_count > 0 and len(getattr(app, "routes", []) or []) + 1 < snap_route_count:
        return True

    if mounted_count <= 0 and current_sig_count <= len(_BUILTIN_META_PATHS) + 2:
        return True

    return False


def _mount_routes_once(app: FastAPI, phase: str) -> Dict[str, Any]:
    _ensure_app_state_defaults(app)

    current = dict(getattr(app.state, "routes_snapshot", {}) or {})
    last_phase = str(getattr(app.state, "routes_mount_phase", "") or "")

    if phase == "startup" and last_phase == "startup":
        snap = _normalize_routes_snapshot(current, used_strategy=str(current.get("strategy", "startup")), app=app)
        snap["openapi_route_count_after_mount"] = len(getattr(app, "routes", []) or [])
        snap["route_signature_count_after_mount"] = _route_signature_count(app, include_builtin=True)
        app.state.routes_snapshot = snap
        app.state.routes_mounted = True
        _invalidate_openapi_cache(app)
        return snap

    if phase == "startup" and last_phase == "prestart":
        promote_only = not _should_reverify_prestart_snapshot(app, current)
        if promote_only:
            snap = _normalize_routes_snapshot(current, used_strategy=str(current.get("strategy", "prestart")), app=app)
            snap["openapi_route_count_after_mount"] = len(getattr(app, "routes", []) or [])
            snap["route_signature_count_after_mount"] = _route_signature_count(app, include_builtin=True)
            app.state.routes_snapshot = snap
            app.state.routes_mounted = True
            app.state.routes_mount_phase = "startup"
            _invalidate_openapi_cache(app)
            return snap

    fresh = _mount_routes(app)
    snap = _merge_snapshots(current, fresh) if current else fresh
    snap = _normalize_routes_snapshot(snap, used_strategy=str(snap.get("strategy", "mount")), app=app)
    snap["openapi_route_count_after_mount"] = len(getattr(app, "routes", []) or [])
    snap["route_signature_count_after_mount"] = _route_signature_count(app, include_builtin=True)

    app.state.routes_snapshot = snap
    app.state.routes_mounted = True
    app.state.routes_mount_phase = phase
    _invalidate_openapi_cache(app)
    return snap


# --------------------------------------------------------------------------------------
# Built-in endpoints
# --------------------------------------------------------------------------------------
def _runtime_meta(app: Optional[FastAPI] = None) -> Dict[str, Any]:
    snap: Dict[str, Any] = {}
    engine_obj: Any = None
    engine_source = ""
    engine_init_error = ""
    routes_mounted = False
    routes_mount_phase = ""
    config_source = _SETTINGS.CONFIG_SOURCE
    startup_warnings: List[str] = []

    try:
        if app is not None and hasattr(app, "state"):
            state_snap = getattr(app.state, "routes_snapshot", None)
            if isinstance(state_snap, dict):
                snap = state_snap
            engine_obj = getattr(app.state, "engine", None)
            engine_source = str(getattr(app.state, "engine_source", "") or "")
            engine_init_error = str(getattr(app.state, "engine_init_error", "") or "")
            routes_mounted = bool(getattr(app.state, "routes_mounted", False))
            routes_mount_phase = str(getattr(app.state, "routes_mount_phase", "") or "")
            config_source = str(getattr(app.state, "config_source", _SETTINGS.CONFIG_SOURCE) or _SETTINGS.CONFIG_SOURCE)
            startup_warnings = list(getattr(app.state, "startup_warnings", []) or [])
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
    route_signature_count = int(
        snap.get("route_signature_count_after_mount", _route_signature_count(app, include_builtin=True) if app is not None else 0) or 0
    )

    return {
        "service": _SETTINGS.APP_NAME,
        "app_version": _SETTINGS.APP_VERSION,
        "entry_version": APP_ENTRY_VERSION,
        "env": _SETTINGS.APP_ENV,
        "timestamp_utc": datetime.now(timezone.utc).isoformat(),
        "python": sys.version.split()[0],
        "config_source": config_source,
        "routes_mounted": routes_mounted,
        "routes_mount_phase": routes_mount_phase,
        "routes_mounted_count": mounted_count,
        "routes_duplicate_skips_count": duplicate_skips_count,
        "routes_failed_count": failed_count,
        "routes_strategy": strategy,
        "route_signature_count": route_signature_count,
        "engine_present": engine_obj is not None,
        "engine_ready": engine_obj is not None and not engine_init_error,
        "engine_source": engine_source,
        "engine_init_error": engine_init_error,
        "startup_warnings": startup_warnings,
    }


def _install_builtin_routes(app: FastAPI) -> None:
    def _status_payload(label: str) -> Dict[str, Any]:
        return {"status": label, **_runtime_meta(app)}

    def _add_status_route(path: str, label: str, *, include_in_schema: bool = False) -> None:
        @app.api_route(path, methods=["GET", "HEAD"], include_in_schema=include_in_schema)
        async def _status_route(request: Request, _label: str = label):
            if request.method == "HEAD":
                return Response(status_code=200)
            return _status_payload(_label)

    @app.get("/meta", tags=["meta"])
    async def meta():
        return _status_payload("ok")

    @app.get("/v1/meta", include_in_schema=False)
    async def meta_v1():
        return _status_payload("ok")

    @app.get("/ping", include_in_schema=False)
    async def ping():
        return {"pong": True, **_runtime_meta(app)}

    @app.get("/v1/ping", include_in_schema=False)
    async def ping_v1():
        return {"pong": True, **_runtime_meta(app)}

    @app.api_route("/", methods=["GET", "HEAD"], include_in_schema=False)
    async def root(request: Request):
        if request.method == "HEAD":
            return Response(status_code=200)
        return _status_payload("ok")

    _add_status_route("/readyz", "ready")
    _add_status_route("/livez", "live")
    _add_status_route("/health", "healthy")
    _add_status_route("/healthz", "healthy")

    _add_status_route("/v1/readyz", "ready")
    _add_status_route("/v1/livez", "live")
    _add_status_route("/v1/health", "healthy")
    _add_status_route("/v1/healthz", "healthy")


# --------------------------------------------------------------------------------------
# Optional engine warm-up
# --------------------------------------------------------------------------------------
async def _maybe_init_engine(app: FastAPI) -> Optional[str]:
    if getattr(app.state, "engine", None) is not None:
        if not str(getattr(app.state, "engine_source", "") or "").strip():
            app.state.engine_source = "preexisting"
        app.state.engine_init_error = ""
        return None

    if not bool(_SETTINGS.INIT_ENGINE_ON_BOOT):
        app.state.engine_init_error = ""
        return None

    err_v2 = ""
    err_v1 = ""

    try:
        de2 = importlib.import_module("core.data_engine_v2")
        init_fn = getattr(de2, "get_engine", None) or getattr(de2, "init_engine", None)
        if callable(init_fn):
            maybe = init_fn()
            engine = await maybe if hasattr(maybe, "__await__") else maybe
            if engine is not None:
                app.state.engine = engine
                app.state.engine_source = "core.data_engine_v2"
                app.state.engine_init_error = ""
                return None
    except Exception as e:
        err_v2 = _err_to_str(e)

    try:
        de1 = importlib.import_module("core.data_engine")
        init_fn = getattr(de1, "get_engine", None) or getattr(de1, "init_engine", None)
        if callable(init_fn):
            maybe = init_fn()
            engine = await maybe if hasattr(maybe, "__await__") else maybe
            if engine is not None:
                app.state.engine = engine
                app.state.engine_source = "core.data_engine"
                app.state.engine_init_error = ""
                return None
    except Exception as e:
        err_v1 = _err_to_str(e)

    err = f"data_engine_v2 failed: {err_v2 or 'not available'} | data_engine failed: {err_v1 or 'not available'}"
    app.state.engine_init_error = err
    return err


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


async def _maybe_close_google_sheets_service() -> None:
    candidates = (
        "integrations.google_sheets_service",
        "core.integrations.google_sheets_service",
        "google_sheets_service",
        "core.google_sheets_service",
    )
    for mod_name in candidates:
        try:
            mod = importlib.import_module(mod_name)
            fn = getattr(mod, "close", None) or getattr(mod, "shutdown", None)
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
        _ensure_app_state_defaults(app)

        try:
            snap = _mount_routes_once(app, phase="startup")
            logger.info(
                "Routes verified at startup: mounted=%s duplicate_skips=%s missing=%s failed=%s strategy=%s",
                snap.get("mounted_count", 0),
                snap.get("duplicate_skips_count", 0),
                snap.get("missing_count", 0),
                snap.get("failed_count", 0),
                snap.get("strategy", ""),
            )
        except Exception as e:
            msg = f"Route mounting crashed during startup: {_err_to_str(e)}"
            logger.error(msg, exc_info=True)
            try:
                app.state.startup_warnings.append(msg)
            except Exception:
                pass
            if _env_bool("ROUTES_STRICT_IMPORT", False):
                raise

        err = await _maybe_init_engine(app)
        if err:
            strict = bool(_SETTINGS.INIT_ENGINE_STRICT)
            if strict:
                logger.error("Engine init failed (strict): %s", err)
                raise RuntimeError(f"Engine init failed: {err}")
            logger.warning("Engine init failed (non-fatal): %s", err)
            try:
                app.state.startup_warnings.append(f"engine_init_warning: {err}")
            except Exception:
                pass

        logger.info("Startup complete: %s", _runtime_meta(app))
        try:
            yield
        finally:
            await _maybe_close_google_sheets_service()
            await _maybe_close_engine()
            logger.info("Shutdown complete.")

    app = FastAPI(
        title=_SETTINGS.APP_NAME,
        version=_SETTINGS.APP_VERSION,
        docs_url=docs_url,
        redoc_url=redoc_url,
        openapi_url=openapi_url,
        lifespan=lifespan,
        default_response_class=_FastAPI_ORJSONResponse,  # type: ignore[arg-type]
    )

    _ensure_app_state_defaults(app)
    app.state.settings = _SETTINGS
    app.state.config_source = _SETTINGS.CONFIG_SOURCE

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

    try:
        snap = _mount_routes_once(app, phase="prestart")
        logger.info(
            "Routes mounted at app creation: mounted=%s duplicate_skips=%s missing=%s failed=%s strategy=%s",
            snap.get("mounted_count", 0),
            snap.get("duplicate_skips_count", 0),
            snap.get("missing_count", 0),
            snap.get("failed_count", 0),
            snap.get("strategy", ""),
        )
    except Exception as e:
        logger.error("Prestart route mounting failed: %s", e, exc_info=True)
        try:
            app.state.startup_warnings.append(f"prestart_route_mount_failed: {_err_to_str(e)}")
        except Exception:
            pass
        if _env_bool("ROUTES_STRICT_IMPORT", False):
            raise

    @app.get("/_debug/routes", include_in_schema=False)
    async def debug_routes(request: Request):
        if _SETTINGS.APP_ENV == "production" and bool(_SETTINGS.REQUIRE_AUTH) and not _auth_ok(request):
            return JSONResponse(status_code=401, content={"status": "error", "error": "unauthorized"})

        route_paths = sorted({str(getattr(r, "path", "") or "") for r in (getattr(request.app, "routes", []) or [])})
        route_sigs = sorted([f"{path} [{methods}]" for path, methods in _app_route_signature_set(request.app, include_builtin=True)])

        return {
            "status": "ok",
            "routes_snapshot": getattr(request.app.state, "routes_snapshot", {}),
            "app_route_count": len(getattr(request.app, "routes", []) or []),
            "app_route_signature_count": _route_signature_count(request.app, include_builtin=True),
            "route_paths": route_paths,
            "route_signatures": route_sigs,
            **_runtime_meta(app),
        }

    return app


# --------------------------------------------------------------------------------------
# ASGI export
# --------------------------------------------------------------------------------------
app = create_app()
