#!/usr/bin/env python3
"""
main.py
================================================================================
TADAWUL FAST BRIDGE — RENDER-SAFE FASTAPI ENTRYPOINT (v8.6.0)
================================================================================
ALIGNED • DEPLOYMENT-SAFE • STARTUP-ONLY ROUTE MOUNT BY DEFAULT • ROUTER-SNAPSHOT-AWARE
ENGINE-STATE-AWARE • OPENAPI-SAFE • RENDER-HEALTH-PROBE-SAFE • PRIORITY-MOUNTED
ROOT-CONFIG-FIRST • CORE-CONFIG-COMPATIBLE • REQUEST-ID SAFE • STARTUP-HARDENED
STRICT-JSON-READY • PREEXISTING-ENGINE SAFE • ROUTE-VERIFY SAFE • DEBUG-HARDENED
V1-HEALTH-ALIAS SAFE • V1-META-ALIAS SAFE • HEAD-PROBE SAFE
CONTROLLED-ROUTE-OWNERSHIP • PARTIAL-DUPLICATE-BLOCK SAFE • FILTERED-MOUNT SAFE
ADVANCED-SHEET-ROWS OWNER FIXED • STRICT-JSON SAFE • ADVISOR-FAMILY NORMALIZED
HEALTH-COUNT DE-NOISED • NO-RESPONSE GUARD SAFE • ROOT-SHEET-ROWS DIAGNOSTICS
MOUNT-FN FILTER SAFE • CANONICAL-OWNER COVERAGE EXPANDED • TFB-TOKEN SAFE
ENGINE-INIT TIMEOUT SAFE • BOOT-LIGHTER PRESTART FLOW
OWNER-DIAGNOSTICS EXACT-MATCH SAFE • CONFIG-FAMILY FILTER SAFE

Why this revision (v8.6.0)
--------------------------
- FIX: disables heavy prestart route mounting by default; controlled routes mount once
       at startup unless PRESTART_MOUNT_ROUTES=1 is explicitly enabled.
- FIX: startup engine initialization is now executed through a timeout-aware helper,
       so a slow engine bootstrap is less likely to stall Render deployment.
- FIX: sync engine init/close hooks are offloaded safely to a worker thread, reducing
       event-loop blocking during startup and shutdown.
- FIX: startup promotes an already-mounted prestart snapshot instead of remounting,
       keeping controlled ownership diagnostics without duplicate mount work.
- FIX: canonical owner diagnostics now prefer exact path matches and then the
       nearest descendant route, preventing family-root mislabeling when a child
       path (for example /v1/advanced/sheet-rows) mounts earlier than its root.
- FIX: controlled module filtering now constrains routes.config to /v1/config so
       schema ownership cannot drift back through an overly broad config router.
- SAFE: preserves strict JSON rendering, route ownership policy, docs, health/meta,
       auth helpers, middleware, and shutdown behavior.
"""

from __future__ import annotations

import asyncio
import importlib
import inspect
import json
import logging
import math
import os
import sys
import uuid
from contextlib import asynccontextmanager
from dataclasses import asdict, dataclass, is_dataclass
from datetime import date, datetime, time as dt_time, timezone
from decimal import Decimal
from enum import Enum
from typing import Any, Dict, List, Mapping, Optional, Sequence, Set, Tuple

from fastapi import FastAPI, Request, Response
from fastapi.encoders import jsonable_encoder
from fastapi.responses import JSONResponse
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.middleware.cors import CORSMiddleware
from starlette.middleware.gzip import GZipMiddleware


# =============================================================================
# JSON safety
# =============================================================================
def _scrub_text(value: Any) -> str:
    try:
        s = value if isinstance(value, str) else str(value)
    except Exception:
        s = repr(value)
    s = s.replace("\x00", "")
    try:
        s = s.encode("utf-8", "replace").decode("utf-8", "replace")
    except Exception:
        pass
    return s


def _json_safe(value: Any, _seen: Optional[Set[int]] = None) -> Any:
    if value is None or isinstance(value, (bool, int)):
        return value

    if isinstance(value, float):
        return value if math.isfinite(value) else None

    if isinstance(value, str):
        return _scrub_text(value)

    if isinstance(value, Decimal):
        try:
            f = float(value)
            return f if math.isfinite(f) else None
        except Exception:
            return _scrub_text(value)

    if isinstance(value, (datetime, date, dt_time)):
        try:
            return value.isoformat()
        except Exception:
            return _scrub_text(value)

    if isinstance(value, timezone):
        return _scrub_text(value)

    if isinstance(value, uuid.UUID):
        return str(value)

    if isinstance(value, Enum):
        return _json_safe(value.value, _seen)

    if isinstance(value, (bytes, bytearray, memoryview)):
        try:
            return bytes(value).decode("utf-8", "replace")
        except Exception:
            return _scrub_text(value)

    if _seen is None:
        _seen = set()

    obj_id = id(value)
    if obj_id in _seen:
        return None

    _seen.add(obj_id)
    try:
        if is_dataclass(value):
            try:
                return _json_safe(asdict(value), _seen)
            except Exception:
                return _scrub_text(value)

        dump_fn = getattr(value, "model_dump", None)
        if callable(dump_fn):
            try:
                return _json_safe(dump_fn(), _seen)
            except Exception:
                pass

        dict_fn = getattr(value, "dict", None)
        if callable(dict_fn):
            try:
                return _json_safe(dict_fn(), _seen)
            except Exception:
                pass

        if isinstance(value, Mapping):
            out: Dict[str, Any] = {}
            for k, v in value.items():
                out[_scrub_text(k)] = _json_safe(v, _seen)
            return out

        if isinstance(value, (list, tuple, set, frozenset)):
            return [_json_safe(v, _seen) for v in value]

        try:
            encoded = jsonable_encoder(
                value,
                custom_encoder={
                    Decimal: lambda v: (float(v) if math.isfinite(float(v)) else None),
                    datetime: lambda v: v.isoformat(),
                    date: lambda v: v.isoformat(),
                    dt_time: lambda v: v.isoformat(),
                    uuid.UUID: lambda v: str(v),
                    bytes: lambda v: bytes(v).decode("utf-8", "replace"),
                    bytearray: lambda v: bytes(v).decode("utf-8", "replace"),
                    memoryview: lambda v: bytes(v).decode("utf-8", "replace"),
                    Enum: lambda v: v.value,
                },
            )
            if encoded is not value:
                return _json_safe(encoded, _seen)
        except Exception:
            pass

        return _scrub_text(value)
    finally:
        _seen.discard(obj_id)


class _StrictJSONResponse(JSONResponse):
    media_type = "application/json"

    def render(self, content: Any) -> bytes:
        safe_content = _json_safe(content)
        return json.dumps(
            safe_content,
            ensure_ascii=False,
            allow_nan=False,
            separators=(",", ":"),
        ).encode("utf-8")


APP_ENTRY_VERSION = "8.6.0"

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

_CONTROLLED_PROTECTED_PREFIXES: Tuple[str, ...] = (
    "/v1/advisor",
    "/v1/investment_advisor",
    "/v1/investment-advisor",
    "/v1/advanced",
    "/v1/analysis",
    "/v1/schema",
    "/schema",
    "/v1/enriched",
    "/v1/enriched_quote",
    "/v1/enriched-quote",
    "/quote",
    "/quotes",
    "/sheet-rows",
)

_CONTROLLED_CANONICAL_OWNER_MAP: Dict[str, str] = {
    "/v1/advisor": "advisor",
    "/v1/investment_advisor": "investment_advisor",
    "/v1/investment-advisor": "investment_advisor",
    "/v1/advanced/sheet-rows": "advanced_analysis",
    "/v1/advanced": "investment_advisor",
    "/v1/analysis": "analysis_sheet_rows",
    "/v1/schema": "advanced_analysis",
    "/schema": "advanced_analysis",
    "/v1/enriched": "enriched_quote",
    "/v1/enriched_quote": "enriched_quote",
    "/v1/enriched-quote": "enriched_quote",
    "/quote": "enriched_quote",
    "/quotes": "enriched_quote",
    "/sheet-rows": "advanced_analysis",
}

_OPTIONAL_ROUTE_MODULES: Set[str] = {
    "routes.config",
    "routes.data_dictionary",
    "routes.ai_analysis",
    "routes.routes_argaam",
    "routes.advisor",
}


# =============================================================================
# Env / generic helpers
# =============================================================================
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


def _env_float(name: str, default: float) -> float:
    raw = _env_str(name, "")
    try:
        value = float(raw)
        return value if math.isfinite(value) else float(default)
    except Exception:
        return float(default)


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


def _paths_start_with_any(paths: Set[str], *prefixes: str) -> bool:
    return any(any(path.startswith(prefix) for prefix in prefixes) for path in paths)


def _request_id_from_request(request: Request) -> str:
    try:
        rid = str(getattr(request.state, "request_id", "") or "").strip()
        if rid:
            return rid
    except Exception:
        pass
    try:
        hdr = str(request.headers.get("X-Request-ID", "") or "").strip()
        if hdr:
            return hdr
    except Exception:
        pass
    return uuid.uuid4().hex[:12]


def _coerce_positive_timeout(value: float, fallback: float) -> float:
    try:
        out = float(value)
        if math.isfinite(out) and out > 0:
            return out
    except Exception:
        pass
    return float(fallback)


# =============================================================================
# Logging
# =============================================================================
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


# =============================================================================
# Settings
# =============================================================================
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
    ENGINE_INIT_TIMEOUT_SEC: float = 12.0
    PRESTART_MOUNT_ROUTES: bool = False

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
        ENGINE_INIT_TIMEOUT_SEC=float(
            _pick_attr(
                s,
                "ENGINE_INIT_TIMEOUT_SEC",
                "engine_init_timeout_sec",
                default=_env_float("ENGINE_INIT_TIMEOUT_SEC", 12.0),
            )
        ),
        PRESTART_MOUNT_ROUTES=_to_bool(
            _pick_attr(s, "PRESTART_MOUNT_ROUTES", "prestart_mount_routes", default=_env_bool("PRESTART_MOUNT_ROUTES", False)),
            _env_bool("PRESTART_MOUNT_ROUTES", False),
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
            _pick_attr(
                s,
                "MAX_REQUESTS_PER_MINUTE",
                "max_requests_per_minute",
                default=_env_int("MAX_REQUESTS_PER_MINUTE", 240),
            )
        ),
        CONFIG_SOURCE=source,
    )


def _load_settings() -> _SettingsView:
    try:
        cfg = importlib.import_module("config")
        getter = getattr(cfg, "get_settings_cached", None) or getattr(cfg, "get_settings", None)
        if callable(getter):
            return _settings_from_generic_object(getter(), source="config")
    except Exception:
        pass

    try:
        env_mod = importlib.import_module("env")
        getter = getattr(env_mod, "get_settings", None)
        if callable(getter):
            return _settings_from_generic_object(getter(), source="env")
    except Exception:
        pass

    try:
        cfg = importlib.import_module("core.config")
        getter = getattr(cfg, "get_settings_cached", None) or getattr(cfg, "get_settings", None)
        if callable(getter):
            return _settings_from_generic_object(getter(), source="core.config")
    except Exception:
        pass

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
        ENGINE_INIT_TIMEOUT_SEC=_env_float("ENGINE_INIT_TIMEOUT_SEC", 12.0),
        PRESTART_MOUNT_ROUTES=_env_bool("PRESTART_MOUNT_ROUTES", False),
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


# =============================================================================
# App state defaults
# =============================================================================
def _default_routes_snapshot() -> Dict[str, Any]:
    return {
        "mounted": [],
        "mounted_count": 0,
        "duplicate_skips": [],
        "duplicate_skips_count": 0,
        "partial_duplicate_skips": [],
        "partial_duplicate_skips_count": 0,
        "filtered_out_routes": {},
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
        "protected_prefixes": list(_CONTROLLED_PROTECTED_PREFIXES),
        "canonical_owner_map": dict(_CONTROLLED_CANONICAL_OWNER_MAP),
        "openapi_route_count_after_mount": 0,
        "route_signature_count_after_mount": 0,
        "route_family_presence": {},
        "effective_failed_modules": [],
        "optional_route_modules": sorted(list(_OPTIONAL_ROUTE_MODULES)),
        "canonical_path_owners": {},
        "canonical_path_owner_mismatches": {},
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


# =============================================================================
# Auth
# =============================================================================
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
            _env_str("TFB_TOKEN", ""),
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


# =============================================================================
# Middleware
# =============================================================================
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


class NoResponseGuardMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next):
        request_id = _request_id_from_request(request)
        try:
            response = await call_next(request)
            if response is None:
                logger.error(
                    "Downstream returned None response",
                    extra={"request_id": request_id, "path": str(request.url.path), "status_code": 500},
                )
                return _StrictJSONResponse(
                    status_code=500,
                    content={
                        "status": "error",
                        "error": "RuntimeError: No response returned.",
                        "path": str(request.url.path),
                        "request_id": request_id,
                        "ts_utc": datetime.now(timezone.utc).isoformat(),
                    },
                )
            return response
        except RuntimeError as exc:
            if "No response returned" in str(exc):
                logger.error(
                    "Caught downstream no-response runtime error",
                    extra={"request_id": request_id, "path": str(request.url.path), "status_code": 500},
                )
                return _StrictJSONResponse(
                    status_code=500,
                    content={
                        "status": "error",
                        "error": f"{type(exc).__name__}: {str(exc)}",
                        "path": str(request.url.path),
                        "request_id": request_id,
                        "ts_utc": datetime.now(timezone.utc).isoformat(),
                    },
                )
            raise


# =============================================================================
# Route diagnostics helpers
# =============================================================================
def _route_signature_pairs_from_route(route: Any) -> Set[Tuple[str, str]]:
    path = str(getattr(route, "path", "") or "")
    methods = getattr(route, "methods", None)

    if isinstance(methods, (set, list, tuple)) and methods:
        return {(path, str(m)) for m in methods}

    route_kind = type(route).__name__.upper() or "ROUTE"
    return {(path, route_kind)}


def _app_route_signature_set(app: Any, *, include_builtin: bool = True) -> Set[Tuple[str, str]]:
    sigs: Set[Tuple[str, str]] = set()
    for r in getattr(app, "routes", []) or []:
        path = str(getattr(r, "path", "") or "")
        if not include_builtin and path in _BUILTIN_META_PATHS:
            continue
        sigs.update(_route_signature_pairs_from_route(r))
    return sigs


def _route_signature_count(app: Any, *, include_builtin: bool = True) -> int:
    return len(_app_route_signature_set(app, include_builtin=include_builtin))


def _invalidate_openapi_cache(app: FastAPI) -> None:
    try:
        app.openapi_schema = None  # type: ignore[attr-defined]
    except Exception:
        pass


def _route_family_presence_from_paths(paths: Set[str]) -> Dict[str, bool]:
    advisor_short = _paths_start_with_any(paths, "/v1/advisor")
    advisor_long_underscore = _paths_start_with_any(paths, "/v1/investment_advisor")
    advisor_long_hyphen = _paths_start_with_any(paths, "/v1/investment-advisor")

    return {
        "advisor_short": advisor_short,
        "advisor_long_underscore": advisor_long_underscore,
        "advisor_long_hyphen": advisor_long_hyphen,
        "advisor_any": advisor_short or advisor_long_underscore or advisor_long_hyphen,
        "advanced_sheet_rows": "/v1/advanced/sheet-rows" in paths,
        "advanced_any": _paths_start_with_any(paths, "/v1/advanced"),
        "analysis": _paths_start_with_any(paths, "/v1/analysis"),
        "schema": _paths_start_with_any(paths, "/v1/schema", "/schema"),
        "enriched": _paths_start_with_any(paths, "/v1/enriched", "/v1/enriched_quote", "/v1/enriched-quote"),
        "root_sheet_rows": "/sheet-rows" in paths,
        "root_health_alias": "/health" in paths and "/v1/health" in paths,
        "root_meta_alias": "/meta" in paths and "/v1/meta" in paths,
    }


def _endpoint_module_name(route: Any) -> str:
    endpoint = getattr(route, "endpoint", None)
    module_name = str(getattr(endpoint, "__module__", "") or "")
    if module_name:
        return module_name
    try:
        app_ref = getattr(route, "app", None)
        return str(getattr(app_ref, "__module__", "") or "")
    except Exception:
        return ""


_CANONICAL_DIAGNOSTIC_PATHS: Tuple[str, ...] = tuple(
    sorted(
        set(_CONTROLLED_CANONICAL_OWNER_MAP.keys())
        | {
            "/v1/advisor/sheet-rows",
            "/v1/analysis/sheet-rows",
            "/v1/schema/sheet-spec",
            "/schema/sheet-spec",
        }
    )
)


def _route_matches_canonical_target(route_path: str, canonical_path: str) -> bool:
    rp = str(route_path or "").strip()
    cp = str(canonical_path or "").strip()
    if not rp or not cp:
        return False
    if rp == cp:
        return True
    return rp.startswith(cp.rstrip("/") + "/")


def _canonical_route_match_rank(route_path: str, canonical_path: str) -> Tuple[int, int, int]:
    rp = str(route_path or "").strip()
    cp = str(canonical_path or "").strip()

    exact_flag = 0 if rp == cp else 1
    segment_count = rp.count("/")
    extra_depth = max(segment_count - cp.count("/"), 0)
    length_delta = max(len(rp) - len(cp), 0)
    return (exact_flag, extra_depth, length_delta)


def _canonical_path_owners_from_routes(app: FastAPI) -> Dict[str, str]:
    routes = list(getattr(app, "routes", []) or [])
    owners: Dict[str, str] = {}

    for canonical_path in _CANONICAL_DIAGNOSTIC_PATHS:
        candidates: List[Tuple[Tuple[int, int, int], int, Any]] = []
        for idx, route in enumerate(routes):
            path = str(getattr(route, "path", "") or "")
            if _route_matches_canonical_target(path, canonical_path):
                candidates.append((_canonical_route_match_rank(path, canonical_path), idx, route))

        if not candidates:
            continue

        _, _, best_route = sorted(candidates, key=lambda item: (item[0], item[1]))[0]
        owners[canonical_path] = _endpoint_module_name(best_route)
    return owners


def _canonical_path_owner_mismatches(path_owners: Mapping[str, str]) -> Dict[str, Dict[str, str]]:
    mismatches: Dict[str, Dict[str, str]] = {}
    for path, expected_owner in _CONTROLLED_CANONICAL_OWNER_MAP.items():
        actual_module = str(path_owners.get(path, "") or "")
        if not actual_module:
            continue
        actual_owner = actual_module.rsplit(".", 1)[-1] if actual_module else ""
        if actual_owner and actual_owner != expected_owner:
            mismatches[path] = {
                "expected_owner": expected_owner,
                "actual_module": actual_module,
                "actual_owner": actual_owner,
            }
    return mismatches


def _effective_failed_modules(src: Dict[str, Any]) -> List[str]:
    import_errors = dict(src.get("import_errors", {}) or {})
    mount_errors = dict(src.get("mount_errors", {}) or {})
    no_router = dict(src.get("no_router", {}) or {})

    failed: Set[str] = set()
    for bucket in (import_errors, mount_errors, no_router):
        for module_name in bucket.keys():
            if module_name in _OPTIONAL_ROUTE_MODULES:
                continue
            failed.add(str(module_name))
    return sorted(failed)


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
    partial_duplicate_skips = list(src.get("partial_duplicate_skips", []) or [])
    missing = list(src.get("missing", []) or [])
    import_errors = dict(src.get("import_errors", {}) or {})
    mount_errors = dict(src.get("mount_errors", {}) or {})
    no_router = dict(src.get("no_router", {}) or {})
    filtered_out_routes = dict(src.get("filtered_out_routes", {}) or {})
    strict = bool(src.get("strict", _env_bool("ROUTES_STRICT_IMPORT", False)))
    strategy = str(src.get("strategy", used_strategy) or used_strategy)

    missing_required_keys = list(src.get("missing_required_keys", []) or [])
    resolved_map = dict(src.get("resolved_map", {}) or {})
    module_to_key = dict(src.get("module_to_key", {}) or {})
    mount_modes = dict(src.get("mount_modes", {}) or {})
    expected_router_modules = list(src.get("expected_router_modules", []) or [])
    plan = list(src.get("plan", []) or [])
    resolved_entries = list(src.get("resolved_entries", []) or [])
    protected_prefixes = list(
        src.get("protected_prefixes", list(_CONTROLLED_PROTECTED_PREFIXES))
        or list(_CONTROLLED_PROTECTED_PREFIXES)
    )
    canonical_owner_map = dict(
        src.get("canonical_owner_map", dict(_CONTROLLED_CANONICAL_OWNER_MAP))
        or dict(_CONTROLLED_CANONICAL_OWNER_MAP)
    )

    openapi_route_count_after_mount = int(
        src.get("openapi_route_count_after_mount", len(getattr(app, "routes", []) or []) if app is not None else 0) or 0
    )
    route_signature_count_after_mount = int(
        src.get(
            "route_signature_count_after_mount",
            _route_signature_count(app, include_builtin=True) if app is not None else 0,
        )
        or 0
    )

    all_paths: Set[str] = set()
    path_owners: Dict[str, str] = {}
    path_owner_mismatches: Dict[str, Dict[str, str]] = {}
    if app is not None:
        all_paths = {str(getattr(r, "path", "") or "") for r in (getattr(app, "routes", []) or [])}
        path_owners = _canonical_path_owners_from_routes(app)
        path_owner_mismatches = _canonical_path_owner_mismatches(path_owners)

    route_family_presence = dict(src.get("route_family_presence", {}) or {})
    if all_paths:
        route_family_presence = _route_family_presence_from_paths(all_paths)

    effective_failed_modules = list(src.get("effective_failed_modules", []) or [])
    if import_errors or mount_errors or no_router:
        effective_failed_modules = _effective_failed_modules(
            {
                "import_errors": import_errors,
                "mount_errors": mount_errors,
                "no_router": no_router,
            }
        )

    failed_count = len(effective_failed_modules)

    return {
        "mounted": mounted,
        "mounted_count": len(mounted),
        "duplicate_skips": duplicate_skips,
        "duplicate_skips_count": len(duplicate_skips),
        "partial_duplicate_skips": partial_duplicate_skips,
        "partial_duplicate_skips_count": len(partial_duplicate_skips),
        "filtered_out_routes": filtered_out_routes,
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
        "protected_prefixes": protected_prefixes,
        "canonical_owner_map": canonical_owner_map,
        "openapi_route_count_after_mount": openapi_route_count_after_mount,
        "route_signature_count_after_mount": route_signature_count_after_mount,
        "route_family_presence": route_family_presence,
        "effective_failed_modules": effective_failed_modules,
        "optional_route_modules": sorted(list(_OPTIONAL_ROUTE_MODULES)),
        "canonical_path_owners": path_owners,
        "canonical_path_owner_mismatches": path_owner_mismatches,
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
        "partial_duplicate_skips": _merge_list(old.get("partial_duplicate_skips"), new.get("partial_duplicate_skips")),
        "filtered_out_routes": _merge_dict(old.get("filtered_out_routes"), new.get("filtered_out_routes")),
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
        "protected_prefixes": _merge_list(old.get("protected_prefixes"), new.get("protected_prefixes")),
        "canonical_owner_map": _merge_dict(old.get("canonical_owner_map"), new.get("canonical_owner_map")),
        "route_family_presence": _merge_dict(old.get("route_family_presence"), new.get("route_family_presence")),
        "effective_failed_modules": _merge_list(old.get("effective_failed_modules"), new.get("effective_failed_modules")),
        "optional_route_modules": _merge_list(old.get("optional_route_modules"), new.get("optional_route_modules")),
        "canonical_path_owners": _merge_dict(old.get("canonical_path_owners"), new.get("canonical_path_owners")),
        "canonical_path_owner_mismatches": _merge_dict(
            old.get("canonical_path_owner_mismatches"), new.get("canonical_path_owner_mismatches")
        ),
    }

    merged["mounted_count"] = len(merged["mounted"])
    merged["duplicate_skips_count"] = len(merged["duplicate_skips"])
    merged["partial_duplicate_skips_count"] = len(merged["partial_duplicate_skips"])
    merged["missing_count"] = len(merged["missing"])
    merged["failed_count"] = len(_effective_failed_modules(merged))
    merged["openapi_route_count_after_mount"] = int(
        new.get("openapi_route_count_after_mount", old.get("openapi_route_count_after_mount", 0)) or 0
    )
    merged["route_signature_count_after_mount"] = int(
        new.get("route_signature_count_after_mount", old.get("route_signature_count_after_mount", 0)) or 0
    )
    return merged


# =============================================================================
# Route importing / ownership / mounting
# =============================================================================
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


def _logical_key_from_module(module_name: str) -> str:
    return str(module_name or "").rsplit(".", 1)[-1].strip()


def _package_mounter_compatibility(routes_pkg: Any) -> Tuple[bool, Dict[str, Any]]:
    owner_map: Dict[str, Any] = {}
    protected_prefixes: List[str] = []

    try:
        getter = getattr(routes_pkg, "_canonical_owner_map", None)
        if callable(getter):
            raw = getter()
            if isinstance(raw, dict):
                owner_map = {str(k): str(v) for k, v in raw.items()}
    except Exception:
        owner_map = {}

    try:
        getter = getattr(routes_pkg, "_protected_prefixes", None)
        if callable(getter):
            raw = getter()
            if isinstance(raw, (list, tuple, set)):
                protected_prefixes = [str(x) for x in raw]
    except Exception:
        protected_prefixes = []

    critical_prefixes = tuple(_CONTROLLED_CANONICAL_OWNER_MAP.keys())
    expected_protected = set(_CONTROLLED_PROTECTED_PREFIXES)

    mismatches: Dict[str, Dict[str, str]] = {}
    for prefix in critical_prefixes:
        expected_owner = _CONTROLLED_CANONICAL_OWNER_MAP.get(prefix, "")
        actual_owner = str(owner_map.get(prefix, "") or "")
        if actual_owner != expected_owner:
            mismatches[prefix] = {
                "expected_owner": expected_owner,
                "package_owner": actual_owner or "(missing)",
            }

    missing_protected = sorted(prefix for prefix in expected_protected if prefix not in set(protected_prefixes))

    details = {
        "critical_prefixes": list(critical_prefixes),
        "mismatches": mismatches,
        "missing_protected_prefixes": missing_protected,
    }
    return (not mismatches and not missing_protected), details


_MODULE_ROUTE_POLICIES: Dict[str, Dict[str, Sequence[str]]] = {
    "routes.config": {
        "allow_prefixes": ("/v1/config",),
        "block_prefixes": ("/v1/schema", "/schema"),
        "block_exact": ("/sheet-rows", "/v1/advanced/sheet-rows"),
    },
    "routes.data_dictionary": {
        "block_prefixes": ("/v1/schema", "/schema"),
        "block_exact": ("/sheet-rows", "/v1/advanced/sheet-rows"),
    },
    "routes.analysis_sheet_rows": {
        "allow_prefixes": ("/v1/analysis",),
    },
    "routes.advisor": {
        "allow_prefixes": ("/v1/advisor",),
    },
    "routes.investment_advisor": {
        "allow_prefixes": ("/v1/advanced", "/v1/investment_advisor", "/v1/investment-advisor"),
        "block_prefixes": ("/v1/advisor",),
        "block_exact": ("/v1/advanced/sheet-rows",),
    },
    "routes.advanced_analysis": {
        "allow_prefixes": ("/v1/schema", "/schema"),
        "allow_exact": (
            "/sheet-rows",
            "/v1/advanced/sheet-rows",
            "/v1/advanced/insights-analysis",
            "/v1/advanced/top10-investments",
            "/v1/advanced/insights-criteria",
        ),
    },
    "routes.enriched_quote": {
        "allow_prefixes": ("/v1/enriched", "/v1/enriched_quote", "/v1/enriched-quote"),
        "allow_exact": ("/quote", "/quotes"),
        "block_exact": ("/sheet-rows", "/v1/advanced/sheet-rows"),
    },
}


def _path_allowed_for_module(module_name: str, path: str) -> bool:
    if path == "/v1/advanced/sheet-rows":
        return module_name == "routes.advanced_analysis"

    policy = _MODULE_ROUTE_POLICIES.get(module_name)
    if not policy:
        return True

    allow_prefixes = tuple(policy.get("allow_prefixes", ()) or ())
    allow_exact = tuple(policy.get("allow_exact", ()) or ())
    block_prefixes = tuple(policy.get("block_prefixes", ()) or ())
    block_exact = tuple(policy.get("block_exact", ()) or ())

    if path in block_exact:
        return False
    if any(path.startswith(prefix) for prefix in block_prefixes):
        return False

    if allow_exact or allow_prefixes:
        if path in allow_exact:
            return True
        if any(path.startswith(prefix) for prefix in allow_prefixes):
            return True
        return False

    return True


def _append_routes_from_iterable_filtered(app: FastAPI, routes_iter: Sequence[Any], module_name: str) -> Dict[str, Any]:
    existing = _app_route_signature_set(app, include_builtin=True)

    added = 0
    duplicate_skips = 0
    partial_duplicate_skips = 0
    filtered_out = 0

    for route in list(routes_iter or []):
        path = str(getattr(route, "path", "") or "")
        if not _path_allowed_for_module(module_name, path):
            filtered_out += 1
            continue

        sigs = _route_signature_pairs_from_route(route)
        if not sigs:
            continue

        overlap = sigs & existing
        if overlap == sigs:
            duplicate_skips += 1
            continue

        if overlap:
            partial_duplicate_skips += 1
            logger.warning(
                "Skipped partial-duplicate route while mounting %s: path=%s overlap=%s",
                module_name,
                path,
                sorted(list(overlap)),
            )
            continue

        app.router.routes.append(route)
        existing.update(sigs)
        added += 1

    _invalidate_openapi_cache(app)
    return {
        "added": added,
        "duplicate_skips": duplicate_skips,
        "partial_duplicate_skips": partial_duplicate_skips,
        "filtered_out": filtered_out,
    }



def _append_router_routes_filtered(app: FastAPI, router_obj: Any, module_name: str) -> Dict[str, Any]:
    return _append_routes_from_iterable_filtered(
        app,
        list(getattr(router_obj, "routes", []) or []),
        module_name,
    )



def _append_mount_fn_routes_filtered(app: FastAPI, mount_fn: Any, module_name: str) -> Dict[str, Any]:
    temp_app = FastAPI(docs_url=None, redoc_url=None, openapi_url=None, default_response_class=_StrictJSONResponse)
    _ensure_app_state_defaults(temp_app)
    mount_fn(temp_app)
    return _append_routes_from_iterable_filtered(
        app,
        list(getattr(temp_app, "routes", []) or []),
        module_name,
    )


def _build_controlled_mount_plan() -> List[str]:
    plan = [
        "routes.config",
        "routes.data_dictionary",
        "routes.analysis_sheet_rows",
        "routes.advanced_analysis",
        "routes.ai_analysis",
        "routes.advisor",
        "routes.investment_advisor",
        "routes.enriched_quote",
        "routes.routes_argaam",
    ]

    extra_modules = _parse_csv(_env_str("EXTRA_ROUTE_MODULES", ""))
    for mod_name in extra_modules:
        if mod_name not in plan:
            plan.append(mod_name)

    return plan


def _verify_required_route_families(app: FastAPI) -> List[str]:
    all_paths = {str(getattr(r, "path", "") or "") for r in (getattr(app, "routes", []) or [])}
    presence = _route_family_presence_from_paths(all_paths)

    required_checks = {
        "advisor": lambda p: bool(p.get("advisor_any", False)),
        "advanced": lambda p: bool(p.get("advanced_sheet_rows", False)),
        "analysis": lambda p: bool(p.get("analysis", False)),
        "schema": lambda p: bool(p.get("schema", False)),
        "enriched": lambda p: bool(p.get("enriched", False)),
        "root_sheet_rows": lambda p: bool(p.get("root_sheet_rows", False)),
        "root_health_alias": lambda p: bool(p.get("root_health_alias", False)),
        "root_meta_alias": lambda p: bool(p.get("root_meta_alias", False)),
    }

    missing: List[str] = []
    for key, predicate in required_checks.items():
        try:
            if not bool(predicate(presence)):
                missing.append(key)
        except Exception:
            missing.append(key)
    return missing


def _mount_routes_controlled(app: FastAPI) -> Dict[str, Any]:
    strict = _env_bool("ROUTES_STRICT_IMPORT", False)
    plan = _build_controlled_mount_plan()
    resolved_map: Dict[str, str] = {}
    module_to_key: Dict[str, str] = {}

    mounted: List[str] = []
    duplicate_skips: List[str] = []
    partial_duplicate_skips: List[str] = []
    missing: List[str] = []
    import_errors: Dict[str, str] = {}
    mount_errors: Dict[str, str] = {}
    no_router: Dict[str, str] = {}
    filtered_out_routes: Dict[str, int] = {}
    mount_modes: Dict[str, str] = {}
    resolved_entries: List[Dict[str, Any]] = []

    for mod_name in plan:
        key = _logical_key_from_module(mod_name)
        resolved_map[key] = mod_name
        module_to_key[mod_name] = key

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
                stats = _append_router_routes_filtered(app, router_obj, mod_name)
                filtered_out_routes[mod_name] = int(stats.get("filtered_out", 0))

                if int(stats.get("added", 0)) > 0:
                    mounted.append(mod_name)
                    mount_modes[mod_name] = f"filtered_router:{source}"
                else:
                    if int(stats.get("duplicate_skips", 0)) > 0:
                        duplicate_skips.append(mod_name)
                    if int(stats.get("partial_duplicate_skips", 0)) > 0:
                        partial_duplicate_skips.append(mod_name)
                    if (
                        int(stats.get("filtered_out", 0)) > 0
                        and int(stats.get("duplicate_skips", 0)) == 0
                        and int(stats.get("partial_duplicate_skips", 0)) == 0
                    ):
                        no_router[mod_name] = "All routes filtered by controlled ownership policy"
                    elif int(stats.get("duplicate_skips", 0)) > 0 or int(stats.get("partial_duplicate_skips", 0)) > 0:
                        mount_modes[mod_name] = "filtered_duplicate_skip"
                    else:
                        no_router[mod_name] = "Router present but no eligible routes were mounted"

                resolved_entries.append(
                    {
                        "module": mod_name,
                        "router_source": source,
                        "added_routes": int(stats.get("added", 0)),
                        "filtered_out_routes": int(stats.get("filtered_out", 0)),
                        "duplicate_skips": int(stats.get("duplicate_skips", 0)),
                        "partial_duplicate_skips": int(stats.get("partial_duplicate_skips", 0)),
                    }
                )
                continue

            if callable(mount_fn):
                stats = _append_mount_fn_routes_filtered(app, mount_fn, mod_name)
                filtered_out_routes[mod_name] = int(stats.get("filtered_out", 0))

                if int(stats.get("added", 0)) > 0:
                    mounted.append(mod_name)
                    mount_modes[mod_name] = "filtered_mount_fn"
                else:
                    if int(stats.get("duplicate_skips", 0)) > 0:
                        duplicate_skips.append(mod_name)
                    if int(stats.get("partial_duplicate_skips", 0)) > 0:
                        partial_duplicate_skips.append(mod_name)
                    if (
                        int(stats.get("filtered_out", 0)) > 0
                        and int(stats.get("duplicate_skips", 0)) == 0
                        and int(stats.get("partial_duplicate_skips", 0)) == 0
                    ):
                        no_router[mod_name] = "mount(app) produced routes but all were filtered by controlled ownership policy"
                    elif int(stats.get("duplicate_skips", 0)) > 0 or int(stats.get("partial_duplicate_skips", 0)) > 0:
                        mount_modes[mod_name] = "filtered_mount_fn_duplicate_skip"
                    else:
                        no_router[mod_name] = "mount(app) completed but produced no eligible routes"

                resolved_entries.append(
                    {
                        "module": mod_name,
                        "router_source": "mount_fn",
                        "added_routes": int(stats.get("added", 0)),
                        "filtered_out_routes": int(stats.get("filtered_out", 0)),
                        "duplicate_skips": int(stats.get("duplicate_skips", 0)),
                        "partial_duplicate_skips": int(stats.get("partial_duplicate_skips", 0)),
                    }
                )
                continue

            no_router[mod_name] = "No router export and no mount(app) function found"
        except Exception as e:
            mount_errors[mod_name] = _err_to_str(e)
            if strict:
                raise

    missing_required_keys = _verify_required_route_families(app)
    route_family_presence = _route_family_presence_from_paths(
        {str(getattr(r, "path", "") or "") for r in (getattr(app, "routes", []) or [])}
    )
    effective_failed_modules = _effective_failed_modules(
        {
            "import_errors": import_errors,
            "mount_errors": mount_errors,
            "no_router": no_router,
        }
    )
    canonical_path_owners = _canonical_path_owners_from_routes(app)
    canonical_path_owner_mismatches = _canonical_path_owner_mismatches(canonical_path_owners)

    snap = {
        "mounted": mounted,
        "duplicate_skips": duplicate_skips,
        "partial_duplicate_skips": partial_duplicate_skips,
        "filtered_out_routes": filtered_out_routes,
        "missing": missing,
        "import_errors": import_errors,
        "mount_errors": mount_errors,
        "no_router": no_router,
        "strict": strict,
        "strategy": "main.controlled_priority_plan",
        "resolved_map": resolved_map,
        "module_to_key": module_to_key,
        "plan": plan,
        "mount_modes": mount_modes,
        "resolved_entries": resolved_entries,
        "protected_prefixes": list(_CONTROLLED_PROTECTED_PREFIXES),
        "canonical_owner_map": dict(_CONTROLLED_CANONICAL_OWNER_MAP),
        "missing_required_keys": missing_required_keys,
        "openapi_route_count_after_mount": len(getattr(app, "routes", []) or []),
        "route_signature_count_after_mount": _route_signature_count(app, include_builtin=True),
        "route_family_presence": route_family_presence,
        "effective_failed_modules": effective_failed_modules,
        "optional_route_modules": sorted(list(_OPTIONAL_ROUTE_MODULES)),
        "canonical_path_owners": canonical_path_owners,
        "canonical_path_owner_mismatches": canonical_path_owner_mismatches,
    }
    _invalidate_openapi_cache(app)
    return snap


def _mount_routes(app: FastAPI) -> Dict[str, Any]:
    use_package_mounter = _env_bool("USE_PACKAGE_ROUTE_MOUNTER", False)
    strict = _env_bool("ROUTES_STRICT_IMPORT", False)

    if use_package_mounter:
        try:
            routes_pkg = importlib.import_module("routes")
            compatible, compatibility_details = _package_mounter_compatibility(routes_pkg)
            if not compatible:
                logger.warning(
                    "Package route mounter ownership mismatch; falling back to controlled plan. details=%s",
                    json.dumps(compatibility_details, ensure_ascii=False),
                )
            else:
                for fn_name in ("mount_all_routers", "mount_all", "mount_routers", "mount_routes", "mount_all_routes"):
                    fn = getattr(routes_pkg, fn_name, None)
                    if callable(fn):
                        used_strategy = f"routes.{fn_name}"
                        try:
                            ret = fn(app)  # type: ignore[misc]
                            snap = _normalize_routes_snapshot(ret, used_strategy=used_strategy, app=app)
                            snap["missing_required_keys"] = _verify_required_route_families(app)
                            snap["protected_prefixes"] = list(_CONTROLLED_PROTECTED_PREFIXES)
                            snap["canonical_owner_map"] = dict(_CONTROLLED_CANONICAL_OWNER_MAP)
                            snap["route_family_presence"] = _route_family_presence_from_paths(
                                {str(getattr(r, "path", "") or "") for r in (getattr(app, "routes", []) or [])}
                            )
                            snap["effective_failed_modules"] = _effective_failed_modules(snap)
                            snap["failed_count"] = len(snap["effective_failed_modules"])
                            snap["optional_route_modules"] = sorted(list(_OPTIONAL_ROUTE_MODULES))
                            snap["openapi_route_count_after_mount"] = len(getattr(app, "routes", []) or [])
                            snap["route_signature_count_after_mount"] = _route_signature_count(app, include_builtin=True)
                            snap["canonical_path_owners"] = _canonical_path_owners_from_routes(app)
                            snap["canonical_path_owner_mismatches"] = _canonical_path_owner_mismatches(snap["canonical_path_owners"])
                            _invalidate_openapi_cache(app)
                            logger.info(
                                "Routes mount summary: mounted=%s duplicate_skips=%s partial_duplicate_skips=%s missing=%s failed=%s strategy=%s",
                                snap["mounted_count"],
                                snap["duplicate_skips_count"],
                                snap["partial_duplicate_skips_count"],
                                snap["missing_count"],
                                snap["failed_count"],
                                snap["strategy"],
                            )
                            return snap
                        except Exception as e:
                            logger.warning("%s(app) failed; falling back to controlled plan. err=%s", used_strategy, _err_to_str(e))
                            if strict:
                                raise
                        break
        except Exception as e:
            logger.warning("Package route mounter import/compatibility check failed; falling back to controlled plan. err=%s", _err_to_str(e))
            if strict:
                raise

    snap = _mount_routes_controlled(app)
    snap = _normalize_routes_snapshot(snap, used_strategy="main.controlled_priority_plan", app=app)
    logger.info(
        "Routes mount summary: mounted=%s duplicate_skips=%s partial_duplicate_skips=%s missing=%s failed=%s strategy=%s",
        snap["mounted_count"],
        snap["duplicate_skips_count"],
        snap["partial_duplicate_skips_count"],
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

    if getattr(app.state, "routes_mounted", False) and phase == "startup":
        if last_phase == "prestart" and _should_reverify_prestart_snapshot(app, current):
            fresh = _mount_routes(app)
            snap = _merge_snapshots(current, fresh) if current else fresh
        else:
            snap = _normalize_routes_snapshot(current, used_strategy=str(current.get("strategy", "startup")), app=app)
        snap["openapi_route_count_after_mount"] = len(getattr(app, "routes", []) or [])
        snap["route_signature_count_after_mount"] = _route_signature_count(app, include_builtin=True)
        snap["canonical_path_owners"] = _canonical_path_owners_from_routes(app)
        snap["canonical_path_owner_mismatches"] = _canonical_path_owner_mismatches(snap["canonical_path_owners"])
        app.state.routes_snapshot = snap
        app.state.routes_mount_phase = "startup"
        app.state.routes_mounted = True
        _invalidate_openapi_cache(app)
        return snap

    if getattr(app.state, "routes_mounted", False) and phase == "prestart":
        snap = _normalize_routes_snapshot(current, used_strategy=str(current.get("strategy", "prestart")), app=app)
        snap["openapi_route_count_after_mount"] = len(getattr(app, "routes", []) or [])
        snap["route_signature_count_after_mount"] = _route_signature_count(app, include_builtin=True)
        snap["canonical_path_owners"] = _canonical_path_owners_from_routes(app)
        snap["canonical_path_owner_mismatches"] = _canonical_path_owner_mismatches(snap["canonical_path_owners"])
        app.state.routes_snapshot = snap
        app.state.routes_mounted = True
        _invalidate_openapi_cache(app)
        return snap

    fresh = _mount_routes(app)
    snap = _merge_snapshots(current, fresh) if current else fresh
    snap = _normalize_routes_snapshot(snap, used_strategy=str(snap.get("strategy", "mount")), app=app)
    snap["openapi_route_count_after_mount"] = len(getattr(app, "routes", []) or [])
    snap["route_signature_count_after_mount"] = _route_signature_count(app, include_builtin=True)
    snap["canonical_path_owners"] = _canonical_path_owners_from_routes(app)
    snap["canonical_path_owner_mismatches"] = _canonical_path_owner_mismatches(snap["canonical_path_owners"])

    app.state.routes_snapshot = snap
    app.state.routes_mounted = True
    app.state.routes_mount_phase = phase
    _invalidate_openapi_cache(app)
    return snap


# =============================================================================
# Runtime meta
# =============================================================================
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
    partial_duplicate_skips_count = int(snap.get("partial_duplicate_skips_count", len(snap.get("partial_duplicate_skips", []) or [])) or 0)
    failed_count = int(snap.get("failed_count", len(snap.get("effective_failed_modules", []) or [])) or 0)
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
        "routes_partial_duplicate_skips_count": partial_duplicate_skips_count,
        "routes_failed_count": failed_count,
        "routes_strategy": strategy,
        "route_signature_count": route_signature_count,
        "missing_required_keys": list(snap.get("missing_required_keys", []) or []),
        "route_family_presence": dict(snap.get("route_family_presence", {}) or {}),
        "effective_failed_modules": list(snap.get("effective_failed_modules", []) or []),
        "canonical_path_owners": dict(snap.get("canonical_path_owners", {}) or {}),
        "canonical_path_owner_mismatches": dict(snap.get("canonical_path_owner_mismatches", {}) or {}),
        "engine_present": engine_obj is not None,
        "engine_ready": engine_obj is not None and not engine_init_error,
        "engine_source": engine_source,
        "engine_init_error": engine_init_error,
        "startup_warnings": startup_warnings,
    }


# =============================================================================
# Built-in routes
# =============================================================================
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


# =============================================================================
# Engine init / shutdown
# =============================================================================
async def _call_zeroarg_maybe_async(fn: Any) -> Any:
    if inspect.iscoroutinefunction(fn):
        return await fn()
    result = await asyncio.to_thread(fn)
    if inspect.isawaitable(result):
        return await result
    return result


async def _try_init_engine_module(app: FastAPI, module_name: str) -> Optional[str]:
    mod = importlib.import_module(module_name)
    init_fn = getattr(mod, "get_engine", None) or getattr(mod, "init_engine", None)
    if not callable(init_fn):
        return None

    timeout_sec = _coerce_positive_timeout(float(_SETTINGS.ENGINE_INIT_TIMEOUT_SEC), 12.0)
    try:
        engine = await asyncio.wait_for(_call_zeroarg_maybe_async(init_fn), timeout=timeout_sec)
    except asyncio.TimeoutError as e:
        raise TimeoutError(f"{module_name} init exceeded {timeout_sec:.1f}s") from e

    if engine is None:
        return None

    app.state.engine = engine
    app.state.engine_source = module_name
    app.state.engine_init_error = ""
    return module_name


async def _maybe_init_engine(app: FastAPI) -> Optional[str]:
    if getattr(app.state, "engine", None) is not None:
        if not str(getattr(app.state, "engine_source", "") or "").strip():
            app.state.engine_source = "preexisting"
        app.state.engine_init_error = ""
        return None

    if not bool(_SETTINGS.INIT_ENGINE_ON_BOOT):
        app.state.engine_init_error = ""
        return None

    attempts: List[Tuple[str, str]] = []
    for module_name in ("core.data_engine_v2", "core.data_engine"):
        try:
            mounted_from = await _try_init_engine_module(app, module_name)
            if mounted_from:
                return None
            attempts.append((module_name, "no init function or returned None"))
        except Exception as e:
            attempts.append((module_name, _err_to_str(e)))

    err = " | ".join([f"{name} failed: {msg}" for name, msg in attempts]) or "engine init failed"
    app.state.engine_init_error = err
    return err


async def _maybe_close_engine() -> None:
    for mod_name in ("core.data_engine_v2", "core.data_engine"):
        try:
            mod = importlib.import_module(mod_name)
            fn = getattr(mod, "close_engine", None) or getattr(mod, "shutdown_engine", None)
            if callable(fn):
                await _call_zeroarg_maybe_async(fn)
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
                await _call_zeroarg_maybe_async(fn)
        except Exception:
            continue


# =============================================================================
# App factory
# =============================================================================
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
                "Routes verified at startup: mounted=%s duplicate_skips=%s partial_duplicate_skips=%s missing=%s failed=%s strategy=%s",
                snap.get("mounted_count", 0),
                snap.get("duplicate_skips_count", 0),
                snap.get("partial_duplicate_skips_count", 0),
                snap.get("missing_count", 0),
                snap.get("failed_count", 0),
                snap.get("strategy", ""),
            )
            missing_required = list(snap.get("missing_required_keys", []) or [])
            if missing_required:
                warning = f"missing_required_route_families: {', '.join(missing_required)}"
                logger.warning(warning)
                try:
                    app.state.startup_warnings.append(warning)
                except Exception:
                    pass

            owner_mismatches = dict(snap.get("canonical_path_owner_mismatches", {}) or {})
            if owner_mismatches:
                warning = f"canonical_path_owner_mismatches: {json.dumps(owner_mismatches, ensure_ascii=False)}"
                logger.warning(warning)
                try:
                    app.state.startup_warnings.append(warning)
                except Exception:
                    pass
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
        default_response_class=_StrictJSONResponse,
    )

    _ensure_app_state_defaults(app)
    app.state.settings = _SETTINGS
    app.state.config_source = _SETTINGS.CONFIG_SOURCE

    app.add_middleware(RequestIDMiddleware)
    app.add_middleware(NoResponseGuardMiddleware)
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
        request_id = _request_id_from_request(request)
        logger.error(
            "Unhandled exception: %s",
            exc,
            exc_info=True,
            extra={"request_id": request_id, "path": str(request.url.path), "status_code": 500},
        )
        return _StrictJSONResponse(
            status_code=500,
            content={
                "status": "error",
                "error": f"{type(exc).__name__}: {str(exc)}",
                "path": str(request.url.path),
                "request_id": request_id,
                "ts_utc": datetime.now(timezone.utc).isoformat(),
            },
        )

    _install_builtin_routes(app)

    if bool(_SETTINGS.PRESTART_MOUNT_ROUTES):
        try:
            snap = _mount_routes_once(app, phase="prestart")
            logger.info(
                "Routes mounted at app creation: mounted=%s duplicate_skips=%s partial_duplicate_skips=%s missing=%s failed=%s strategy=%s",
                snap.get("mounted_count", 0),
                snap.get("duplicate_skips_count", 0),
                snap.get("partial_duplicate_skips_count", 0),
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
    else:
        logger.info("Prestart route mounting skipped by configuration.")

    @app.get("/_debug/routes", include_in_schema=False)
    async def debug_routes(request: Request):
        if _SETTINGS.APP_ENV == "production" and bool(_SETTINGS.REQUIRE_AUTH) and not _auth_ok(request):
            return JSONResponse(status_code=401, content={"status": "error", "error": "unauthorized"})

        route_paths = sorted({str(getattr(r, "path", "") or "") for r in (getattr(request.app, "routes", []) or [])})
        route_sigs = sorted([f"{path} [{method}]" for path, method in _app_route_signature_set(request.app, include_builtin=True)])
        path_owners = _canonical_path_owners_from_routes(request.app)
        owner_mismatches = _canonical_path_owner_mismatches(path_owners)

        return {
            "status": "ok",
            "routes_snapshot": getattr(request.app.state, "routes_snapshot", {}),
            "app_route_count": len(getattr(request.app, "routes", []) or []),
            "app_route_signature_count": _route_signature_count(request.app, include_builtin=True),
            "route_paths": route_paths,
            "route_signatures": route_sigs,
            "canonical_path_owners": path_owners,
            "canonical_path_owner_mismatches": owner_mismatches,
            **_runtime_meta(app),
        }

    return app


app = create_app()
