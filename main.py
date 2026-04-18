#!/usr/bin/env python3
"""
main.py
================================================================================
TADAWUL FAST BRIDGE — RENDER-SAFE FASTAPI ENTRYPOINT (v8.12.1)
================================================================================
FASTAPI-NATIVE ROUTER INCLUDE • PRESTART-FIRST ROUTE MOUNT • OPENAPI CACHE SAFE
REQUEST-ID SAFE • ENGINE-STATE AWARE • CONTROLLED-ROUTE-OWNERSHIP SAFE
STRICT-JSON SAFE • HEALTH / META ALIAS SAFE • DEBUG ROUTE SAFE
INVESTMENT-ADVISOR CANONICAL OWNER PROTECTION • ADVANCED ROUTE PRIORITY SAFE
CUSTOM OPENAPI / DOCS SAFE • JSON CONTRACT HARDENING

Why this revision (v8.12.1)
---------------------------
- KEEP: custom OpenAPI/docs routes so /openapi.json always returns parseable JSON.
- KEEP: controlled route mounting with canonical owner protection.
- IMPROVE: OpenAPI cache now also carries a generated timestamp and optional servers
           entry when BACKEND_BASE_URL is configured.
- IMPROVE: /openapi.json explicitly returns no-store cache headers to avoid stale
           proxies during Render deployments and route changes.
- IMPROVE: routes snapshot normalization is recalculated more defensively from live
           app state so meta/debug diagnostics stay aligned with the real router set.
- IMPROVE: startup warnings are deduplicated and protected from noisy repeats.
- SAFE: preserves current public routing, auth, health/meta aliases, and render-safe startup.
"""
from __future__ import annotations

import asyncio
import importlib
import inspect
import hmac
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
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence, Set, Tuple

from fastapi import APIRouter, FastAPI, Request, Response
from fastapi.encoders import jsonable_encoder
from fastapi.openapi.docs import get_redoc_html, get_swagger_ui_html
from fastapi.openapi.utils import get_openapi
from fastapi.responses import JSONResponse
from fastapi.routing import APIRoute
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
            return {_scrub_text(k): _json_safe(v, _seen) for k, v in value.items()}
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
        return json.dumps(
            _json_safe(content),
            ensure_ascii=False,
            allow_nan=False,
            separators=(",", ":"),
        ).encode("utf-8")


APP_ENTRY_VERSION = "8.12.1"

_TRUTHY = {"1", "true", "yes", "y", "on", "t", "enabled", "enable"}
_FALSY = {"0", "false", "no", "n", "off", "f", "disabled", "disable"}

_BUILTIN_META_PATHS = {
    "/", "/meta", "/ping", "/livez", "/readyz", "/health", "/healthz",
    "/v1/meta", "/v1/ping", "/v1/livez", "/v1/readyz", "/v1/health", "/v1/healthz",
    "/_debug/routes", "/docs", "/redoc", "/openapi.json",
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
    "/sheet_rows",
)

_CONTROLLED_CANONICAL_OWNER_MAP: Dict[str, str] = {
    "/v1/advisor": "advisor",
    "/v1/advisor/sheet-rows": "advisor",
    "/v1/advisor/sheet_rows": "advisor",

    "/v1/investment_advisor": "investment_advisor",
    "/v1/investment_advisor/sheet-rows": "investment_advisor",
    "/v1/investment_advisor/sheet_rows": "investment_advisor",
    "/v1/investment-advisor": "investment_advisor",
    "/v1/investment-advisor/sheet-rows": "investment_advisor",
    "/v1/investment-advisor/sheet_rows": "investment_advisor",

    "/v1/advanced": "investment_advisor",
    "/v1/advanced/sheet-rows": "investment_advisor",
    "/v1/advanced/sheet_rows": "investment_advisor",

    "/v1/analysis": "analysis_sheet_rows",
    "/v1/analysis/sheet-rows": "analysis_sheet_rows",
    "/v1/analysis/sheet_rows": "analysis_sheet_rows",

    "/v1/schema": "advanced_analysis",
    "/v1/schema/sheet-spec": "advanced_analysis",
    "/v1/schema/sheet_spec": "advanced_analysis",
    "/v1/schema/pages": "advanced_analysis",
    "/v1/schema/data-dictionary": "advanced_analysis",
    "/v1/schema/data_dictionary": "advanced_analysis",
    "/schema": "advanced_analysis",
    "/schema/sheet-spec": "advanced_analysis",
    "/schema/sheet_spec": "advanced_analysis",
    "/sheet-rows": "advanced_analysis",
    "/sheet_rows": "advanced_analysis",

    "/v1/enriched": "enriched_quote",
    "/v1/enriched/sheet-rows": "enriched_quote",
    "/v1/enriched/sheet_rows": "enriched_quote",
    "/v1/enriched_quote": "enriched_quote",
    "/v1/enriched_quote/sheet-rows": "enriched_quote",
    "/v1/enriched_quote/sheet_rows": "enriched_quote",
    "/v1/enriched-quote": "enriched_quote",
    "/v1/enriched-quote/sheet-rows": "enriched_quote",
    "/v1/enriched-quote/sheet_rows": "enriched_quote",
    "/quote": "enriched_quote",
    "/quotes": "enriched_quote",
}

_OPTIONAL_ROUTE_MODULES: Set[str] = {
    "routes.config",
    "routes.data_dictionary",
    "routes.ai_analysis",
    "routes.routes_argaam",
}

_CONTROLLED_ROUTE_PLAN: Tuple[Tuple[str, str], ...] = (
    ("config", "routes.config"),
    ("advanced_analysis", "routes.advanced_analysis"),
    ("analysis_sheet_rows", "routes.analysis_sheet_rows"),
    ("investment_advisor", "routes.investment_advisor"),
    ("advanced_sheet_rows", "routes.advanced_sheet_rows"),
    ("advisor", "routes.advisor"),
    ("enriched_quote", "routes.enriched_quote"),
)


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


def _path_present_any(paths: Set[str], *candidates: str) -> bool:
    return any(c in paths for c in candidates)


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


def _secure_equals(left: str, right: str) -> bool:
    try:
        return hmac.compare_digest(str(left), str(right))
    except Exception:
        return str(left) == str(right)


def _append_startup_warning(app: FastAPI, message: str) -> None:
    try:
        warnings = list(getattr(app.state, "startup_warnings", []) or [])
        if message not in warnings:
            warnings.append(message)
        app.state.startup_warnings = warnings
    except Exception:
        pass


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
        for key in ("request_id", "path", "status_code"):
            if hasattr(record, key):
                payload[key] = getattr(record, key)
        try:
            return json.dumps(payload, ensure_ascii=False)
        except Exception:
            return str(payload)


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

    for name in ("uvicorn", "uvicorn.error", "uvicorn.access", "gunicorn", "gunicorn.error"):
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
    PRESTART_MOUNT_ROUTES: bool = True

    REQUIRE_AUTH: bool = True
    OPEN_MODE: bool = False
    AUTH_HEADER_NAME: str = "X-APP-TOKEN"
    ALLOW_QUERY_TOKEN: bool = False

    ENABLE_CORS_ALL_ORIGINS: bool = False
    CORS_ORIGINS: str = ""

    BACKEND_BASE_URL: str = ""
    ENGINE_CACHE_TTL_SEC: int = 20
    MAX_REQUESTS_PER_MINUTE: int = 240

    CONFIG_SOURCE: str = "env"


def _settings_from_generic_object(s: Any, source: str) -> _SettingsView:
    return _SettingsView(
        APP_NAME=str(_pick_attr(s, "APP_NAME", "app_name", "service_name", default=_env_str("APP_NAME", "Tadawul Fast Bridge"))),
        APP_VERSION=str(_pick_attr(s, "APP_VERSION", "app_version", default=_env_str("APP_VERSION", "dev"))),
        APP_ENV=str(_pick_attr(s, "APP_ENV", "app_env", "environment", "env", default=_env_str("APP_ENV", "production"))),
        TIMEZONE_DEFAULT=str(_pick_attr(s, "TIMEZONE_DEFAULT", "timezone_default", "timezone", default=_env_str("TZ", "Asia/Riyadh"))),
        DEBUG=_to_bool(_pick_attr(s, "DEBUG", "debug", default=_env_bool("DEBUG", False)), _env_bool("DEBUG", False)),
        ENABLE_SWAGGER=_to_bool(_pick_attr(s, "ENABLE_SWAGGER", "enable_swagger", default=_env_bool("ENABLE_SWAGGER", True)), _env_bool("ENABLE_SWAGGER", True)),
        ENABLE_REDOC=_to_bool(_pick_attr(s, "ENABLE_REDOC", "enable_redoc", default=_env_bool("ENABLE_REDOC", True)), _env_bool("ENABLE_REDOC", True)),
        INIT_ENGINE_ON_BOOT=_to_bool(_pick_attr(s, "INIT_ENGINE_ON_BOOT", "init_engine_on_boot", default=_env_bool("INIT_ENGINE_ON_BOOT", True)), _env_bool("INIT_ENGINE_ON_BOOT", True)),
        INIT_ENGINE_STRICT=_to_bool(_pick_attr(s, "INIT_ENGINE_STRICT", "init_engine_strict", default=_env_bool("INIT_ENGINE_STRICT", False)), _env_bool("INIT_ENGINE_STRICT", False)),
        ENGINE_INIT_TIMEOUT_SEC=float(_pick_attr(s, "ENGINE_INIT_TIMEOUT_SEC", "engine_init_timeout_sec", default=_env_float("ENGINE_INIT_TIMEOUT_SEC", 12.0))),
        PRESTART_MOUNT_ROUTES=_to_bool(_pick_attr(s, "PRESTART_MOUNT_ROUTES", "prestart_mount_routes", default=_env_bool("PRESTART_MOUNT_ROUTES", True)), _env_bool("PRESTART_MOUNT_ROUTES", True)),
        REQUIRE_AUTH=_to_bool(_pick_attr(s, "REQUIRE_AUTH", "require_auth", default=_env_bool("REQUIRE_AUTH", True)), _env_bool("REQUIRE_AUTH", True)),
        OPEN_MODE=_to_bool(_pick_attr(s, "OPEN_MODE", "open_mode", default=_env_bool("OPEN_MODE", False)), _env_bool("OPEN_MODE", False)),
        AUTH_HEADER_NAME=str(_pick_attr(s, "AUTH_HEADER_NAME", "auth_header_name", default=_env_str("AUTH_HEADER_NAME", "X-APP-TOKEN"))),
        ALLOW_QUERY_TOKEN=_to_bool(_pick_attr(s, "ALLOW_QUERY_TOKEN", "allow_query_token", default=_env_bool("ALLOW_QUERY_TOKEN", False)), _env_bool("ALLOW_QUERY_TOKEN", False)),
        ENABLE_CORS_ALL_ORIGINS=_to_bool(_pick_attr(s, "ENABLE_CORS_ALL_ORIGINS", "enable_cors_all_origins", default=_env_bool("ENABLE_CORS_ALL_ORIGINS", False)), _env_bool("ENABLE_CORS_ALL_ORIGINS", False)),
        CORS_ORIGINS=str(_pick_attr(s, "CORS_ORIGINS", "cors_origins", default=_env_str("CORS_ORIGINS", ""))),
        BACKEND_BASE_URL=str(_pick_attr(s, "BACKEND_BASE_URL", "backend_base_url", default=_env_str("BACKEND_BASE_URL", ""))),
        ENGINE_CACHE_TTL_SEC=int(_pick_attr(s, "ENGINE_CACHE_TTL_SEC", "engine_cache_ttl_sec", default=_env_int("ENGINE_CACHE_TTL_SEC", 20))),
        MAX_REQUESTS_PER_MINUTE=int(_pick_attr(s, "MAX_REQUESTS_PER_MINUTE", "max_requests_per_minute", default=_env_int("MAX_REQUESTS_PER_MINUTE", 240))),
        CONFIG_SOURCE=source,
    )


def _load_settings() -> _SettingsView:
    for mod_name, getter_names in (
        ("config", ("get_settings_cached", "get_settings")),
        ("env", ("get_settings",)),
        ("core.config", ("get_settings_cached", "get_settings")),
    ):
        try:
            mod = importlib.import_module(mod_name)
            for getter_name in getter_names:
                getter = getattr(mod, getter_name, None)
                if callable(getter):
                    return _settings_from_generic_object(getter(), source=mod_name)
        except Exception:
            continue

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
        PRESTART_MOUNT_ROUTES=_env_bool("PRESTART_MOUNT_ROUTES", True),
        REQUIRE_AUTH=_env_bool("REQUIRE_AUTH", True),
        OPEN_MODE=_env_bool("OPEN_MODE", False),
        AUTH_HEADER_NAME=_env_str("AUTH_HEADER_NAME", "X-APP-TOKEN"),
        ALLOW_QUERY_TOKEN=_env_bool("ALLOW_QUERY_TOKEN", False),
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
        "strategy": "main.controlled_priority_plan",
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
    if not hasattr(app.state, "_openapi_route_signature_count"):
        app.state._openapi_route_signature_count = -1


# =============================================================================
# Auth
# =============================================================================
def _call_auth_ok_flexible(fn: Any, request: Request, token_value: str, authorization: str, api_key_value: str = "") -> bool:
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

    query_token = ""
    if bool(getattr(_SETTINGS, "ALLOW_QUERY_TOKEN", False)):
        try:
            query_token = str(request.query_params.get("token") or "").strip()
        except Exception:
            query_token = ""

    attempts = [
        {
            "token": token_value or None,
            "authorization": authorization or None,
            "headers": headers_dict,
            "path": path,
            "request": request,
            "settings": settings,
            "x_app_token": token_value or None,
            "api_token": api_key_value or None,
            "query_token": query_token or None,
        },
        {
            "token": token_value or api_key_value or None,
            "authorization": authorization or None,
            "headers": headers_dict,
            "path": path,
            "request": request,
        },
        {
            "token": token_value or api_key_value or None,
            "authorization": authorization or None,
            "headers": headers_dict,
        },
        {
            "token": token_value or api_key_value or None,
            "authorization": authorization or None,
        },
        {"token": token_value or api_key_value or None},
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

    x_app_token = request.headers.get(_SETTINGS.AUTH_HEADER_NAME, "") or request.headers.get("X-APP-TOKEN", "")
    api_key_value = request.headers.get("X-API-Key", "") or request.headers.get("Api-Key", "")
    auth = request.headers.get("Authorization", "")

    token_value = str(x_app_token or api_key_value or "").strip()
    query_token = ""
    if bool(getattr(_SETTINGS, "ALLOW_QUERY_TOKEN", False)):
        try:
            query_token = str(request.query_params.get("token") or "").strip()
        except Exception:
            query_token = ""
        if not token_value:
            token_value = query_token

    for mod_name in ("config", "core.config"):
        try:
            cfg = importlib.import_module(mod_name)
            fn = getattr(cfg, "auth_ok", None)
            if callable(fn):
                return _call_auth_ok_flexible(fn, request, token_value, auth, api_key_value)
        except Exception:
            continue

    allowed = [
        x for x in (
            _env_str("APP_TOKEN", ""),
            _env_str("BACKEND_TOKEN", ""),
            _env_str("BACKUP_APP_TOKEN", ""),
            _env_str("X_APP_TOKEN", ""),
            _env_str("AUTH_TOKEN", ""),
            _env_str("TOKEN", ""),
            _env_str("TFB_APP_TOKEN", ""),
            _env_str("TFB_TOKEN", ""),
            _env_str("API_TOKEN", ""),
        ) if x
    ]
    if not allowed:
        return False

    if token_value and any(_secure_equals(token_value, t) for t in allowed):
        return True
    if query_token and bool(getattr(_SETTINGS, "ALLOW_QUERY_TOKEN", False)):
        if any(_secure_equals(query_token, t) for t in allowed):
            return True
    if auth.lower().startswith("bearer "):
        t = auth.split(" ", 1)[1].strip()
        return bool(t and any(_secure_equals(t, a) for a in allowed))
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
    for route in getattr(app, "routes", []) or []:
        path = str(getattr(route, "path", "") or "")
        if not include_builtin and path in _BUILTIN_META_PATHS:
            continue
        sigs.update(_route_signature_pairs_from_route(route))
    return sigs


def _route_signature_count(app: Any, *, include_builtin: bool = True) -> int:
    return len(_app_route_signature_set(app, include_builtin=include_builtin))


def _invalidate_openapi_cache(app: FastAPI) -> None:
    try:
        app.openapi_schema = None  # type: ignore[attr-defined]
    except Exception:
        pass
    try:
        app.state._openapi_route_signature_count = -1
    except Exception:
        pass


def _route_family_presence_from_paths(paths: Set[str]) -> Dict[str, bool]:
    advisor_short = _paths_start_with_any(paths, "/v1/advisor")
    advisor_short_rows = _path_present_any(paths, "/v1/advisor/sheet-rows", "/v1/advisor/sheet_rows")

    advisor_long_underscore = _paths_start_with_any(paths, "/v1/investment_advisor")
    advisor_long_hyphen = _paths_start_with_any(paths, "/v1/investment-advisor")

    advanced_rows = _path_present_any(paths, "/v1/advanced/sheet-rows", "/v1/advanced/sheet_rows")
    analysis_rows = _path_present_any(paths, "/v1/analysis/sheet-rows", "/v1/analysis/sheet_rows")

    schema_rows = _path_present_any(paths, "/v1/schema/sheet-spec", "/v1/schema/sheet_spec", "/schema/sheet-spec", "/schema/sheet_spec")
    schema_dict = _path_present_any(paths, "/v1/schema/data-dictionary", "/v1/schema/data_dictionary")

    enriched_primary = _path_present_any(paths, "/v1/enriched/sheet-rows", "/v1/enriched/sheet_rows")
    enriched_underscore = _path_present_any(paths, "/v1/enriched_quote/sheet-rows", "/v1/enriched_quote/sheet_rows")
    enriched_hyphen = _path_present_any(paths, "/v1/enriched-quote/sheet-rows", "/v1/enriched-quote/sheet_rows")

    root_sheet_rows = _path_present_any(paths, "/sheet-rows", "/sheet_rows")

    return {
        "advisor_short": advisor_short,
        "advisor_short_sheet_rows": advisor_short_rows,
        "advisor_long_underscore": advisor_long_underscore,
        "advisor_long_hyphen": advisor_long_hyphen,
        "advisor_any": advisor_short or advisor_long_underscore or advisor_long_hyphen,

        "advanced_sheet_rows": advanced_rows,
        "advanced_any": _paths_start_with_any(paths, "/v1/advanced"),

        "analysis": _paths_start_with_any(paths, "/v1/analysis"),
        "analysis_sheet_rows": analysis_rows,

        "schema": _paths_start_with_any(paths, "/v1/schema", "/schema"),
        "schema_sheet_spec": schema_rows,
        "schema_data_dictionary": schema_dict,

        "enriched": _paths_start_with_any(paths, "/v1/enriched", "/v1/enriched_quote", "/v1/enriched-quote"),
        "enriched_sheet_rows_primary": enriched_primary,
        "enriched_sheet_rows_underscore": enriched_underscore,
        "enriched_sheet_rows_hyphen": enriched_hyphen,
        "enriched_sheet_rows_any": enriched_primary or enriched_underscore or enriched_hyphen,

        "root_sheet_rows": root_sheet_rows,
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


_CANONICAL_DIAGNOSTIC_PATHS: Tuple[str, ...] = tuple(sorted(set(_CONTROLLED_CANONICAL_OWNER_MAP.keys())))


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
    extra_depth = max(rp.count("/") - cp.count("/"), 0)
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


def _verify_required_route_families(app: FastAPI) -> List[str]:
    paths = {str(getattr(r, "path", "") or "") for r in (getattr(app, "routes", []) or [])}
    presence = _route_family_presence_from_paths(paths)
    required = {
        "advisor_any": presence.get("advisor_any", False),
        "advanced_sheet_rows": presence.get("advanced_sheet_rows", False),
        "analysis": presence.get("analysis", False),
        "analysis_sheet_rows": presence.get("analysis_sheet_rows", False),
        "schema": presence.get("schema", False),
        "schema_sheet_spec": presence.get("schema_sheet_spec", False),
        "enriched": presence.get("enriched", False),
        "enriched_sheet_rows_any": presence.get("enriched_sheet_rows_any", False),
        "root_sheet_rows": presence.get("root_sheet_rows", False),
        "root_health_alias": presence.get("root_health_alias", False),
        "root_meta_alias": presence.get("root_meta_alias", False),
    }
    return sorted([key for key, ok in required.items() if not bool(ok)])


def _live_route_metrics(app: Optional[FastAPI]) -> Tuple[int, int]:
    if app is None:
        return 0, 0
    try:
        live_route_count = len(getattr(app, "routes", []) or [])
    except Exception:
        live_route_count = 0
    try:
        live_signature_count = _route_signature_count(app, include_builtin=True)
    except Exception:
        live_signature_count = 0
    return int(live_route_count), int(live_signature_count)


def _prefer_live_metric(raw_value: Any, live_value: int) -> int:
    try:
        parsed = int(raw_value)
    except Exception:
        parsed = 0
    if live_value > 0:
        return int(live_value)
    return max(0, parsed)


def _normalize_routes_snapshot(ret: Any, *, used_strategy: str, app: Optional[FastAPI] = None) -> Dict[str, Any]:
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
    protected_prefixes = list(src.get("protected_prefixes", list(_CONTROLLED_PROTECTED_PREFIXES)) or list(_CONTROLLED_PROTECTED_PREFIXES))
    canonical_owner_map = dict(src.get("canonical_owner_map", dict(_CONTROLLED_CANONICAL_OWNER_MAP)) or dict(_CONTROLLED_CANONICAL_OWNER_MAP))

    live_route_count, live_signature_count = _live_route_metrics(app)
    openapi_route_count_after_mount = _prefer_live_metric(src.get("openapi_route_count_after_mount", 0), live_route_count)
    route_signature_count_after_mount = _prefer_live_metric(src.get("route_signature_count_after_mount", 0), live_signature_count)

    path_owners: Dict[str, str] = {}
    path_owner_mismatches: Dict[str, Dict[str, str]] = {}
    route_family_presence = dict(src.get("route_family_presence", {}) or {})
    if app is not None:
        try:
            all_paths = {str(getattr(r, "path", "") or "") for r in (getattr(app, "routes", []) or [])}
            route_family_presence = _route_family_presence_from_paths(all_paths)
            path_owners = _canonical_path_owners_from_routes(app)
            path_owner_mismatches = _canonical_path_owner_mismatches(path_owners)
            missing_required_keys = _verify_required_route_families(app)
        except Exception:
            pass

    effective_failed_modules = _effective_failed_modules({
        "import_errors": import_errors,
        "mount_errors": mount_errors,
        "no_router": no_router,
    })

    out = {
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
        "failed_count": len(effective_failed_modules),
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
    if app is not None:
        app.state.routes_snapshot = out
    return out


# =============================================================================
# Controlled route mounting
# =============================================================================
def _allowed_prefixes_for_key(key: str) -> Tuple[str, ...]:
    mapping = {
        "config": ("/v1/config",),
        "advanced_analysis": ("/v1/schema", "/schema", "/sheet-rows", "/sheet_rows"),
        "advanced_sheet_rows": ("/v1/advanced",),
        "analysis_sheet_rows": ("/v1/analysis",),
        "investment_advisor": ("/v1/advanced", "/v1/investment_advisor", "/v1/investment-advisor"),
        "advisor": ("/v1/advisor",),
        "enriched_quote": ("/v1/enriched", "/v1/enriched_quote", "/v1/enriched-quote", "/quote", "/quotes"),
    }
    return mapping.get(key, tuple())


def _canonical_owner_for_exact_path(path: str) -> str:
    return str(_CONTROLLED_CANONICAL_OWNER_MAP.get(str(path or "").strip(), "") or "")


def _router_from_module(mod: Any) -> Optional[APIRouter]:
    for name in ("router", "api_router", "routes", "ROUTER"):
        obj = getattr(mod, name, None)
        if isinstance(obj, APIRouter):
            return obj
    return None


def _iter_router_api_routes(router: APIRouter) -> Iterable[APIRoute]:
    for route in list(getattr(router, "routes", []) or []):
        if isinstance(route, APIRoute):
            yield route


def _clone_filtered_router(router: APIRouter, *, key: str) -> Tuple[APIRouter, List[str], List[str]]:
    allowed = _allowed_prefixes_for_key(key)
    out = APIRouter()
    filtered_out: List[str] = []
    added_paths: List[str] = []

    for route in _iter_router_api_routes(router):
        path = str(getattr(route, "path", "") or "")
        if allowed and not any(path.startswith(prefix) for prefix in allowed):
            filtered_out.append(path)
            continue

        canonical_owner = _canonical_owner_for_exact_path(path)
        if canonical_owner and canonical_owner != key:
            filtered_out.append(path)
            continue

        kwargs = {
            "path": path,
            "endpoint": route.endpoint,
            "methods": list(route.methods or []),
            "name": route.name,
            "include_in_schema": route.include_in_schema,
            "response_class": getattr(route, "response_class", None),
            "status_code": getattr(route, "status_code", None),
            "tags": list(getattr(route, "tags", []) or []),
            "summary": getattr(route, "summary", None),
            "description": getattr(route, "description", None),
            "response_description": getattr(route, "response_description", "Successful Response"),
            "deprecated": getattr(route, "deprecated", None),
            "operation_id": getattr(route, "operation_id", None),
            "responses": getattr(route, "responses", None),
            "dependencies": list(getattr(route, "dependencies", []) or []),
        }
        try:
            out.add_api_route(**kwargs)
            added_paths.append(path)
        except Exception:
            try:
                out.add_api_route(
                    path=path,
                    endpoint=route.endpoint,
                    methods=list(route.methods or []),
                    name=route.name,
                    include_in_schema=route.include_in_schema,
                )
                added_paths.append(path)
            except Exception:
                filtered_out.append(path)

    return out, filtered_out, added_paths


def _router_signature_set(router: APIRouter) -> Set[Tuple[str, str]]:
    sigs: Set[Tuple[str, str]] = set()
    for route in _iter_router_api_routes(router):
        sigs.update(_route_signature_pairs_from_route(route))
    return sigs


def _mount_routes_controlled(app: FastAPI) -> Dict[str, Any]:
    strict = _env_bool("ROUTES_STRICT_IMPORT", False)
    snap = _default_routes_snapshot()
    snap["strict"] = strict
    snap["strategy"] = "main.controlled_priority_plan"
    snap["plan"] = [key for key, _ in _CONTROLLED_ROUTE_PLAN]
    snap["expected_router_modules"] = [module_name for _, module_name in _CONTROLLED_ROUTE_PLAN]

    for key, module_name in _CONTROLLED_ROUTE_PLAN:
        snap["module_to_key"][module_name] = key
        snap["resolved_map"][key] = module_name
        snap["mount_modes"][module_name] = "include_router_filtered"

        try:
            mod = importlib.import_module(module_name)
        except Exception as e:
            snap["import_errors"][module_name] = _err_to_str(e)
            if strict and module_name not in _OPTIONAL_ROUTE_MODULES:
                raise
            continue

        router = _router_from_module(mod)
        if router is None:
            snap["no_router"][module_name] = "router not found"
            if strict and module_name not in _OPTIONAL_ROUTE_MODULES:
                raise RuntimeError(f"{module_name} has no APIRouter")
            continue

        filtered_router, filtered_out, added_paths = _clone_filtered_router(router, key=key)
        if filtered_out:
            snap["filtered_out_routes"][module_name] = filtered_out

        routes_to_add = list(_iter_router_api_routes(filtered_router))
        if not routes_to_add:
            snap["no_router"][module_name] = "router filtered to 0 allowed routes"
            continue

        existing_sigs = _app_route_signature_set(app, include_builtin=True)
        router_sigs = _router_signature_set(filtered_router)
        overlap = router_sigs & existing_sigs

        if router_sigs and overlap == router_sigs:
            snap["duplicate_skips"].append({
                "module": module_name,
                "paths": sorted(set(added_paths)),
                "signature_count": len(router_sigs),
            })
            continue

        if overlap:
            snap["partial_duplicate_skips"].append({
                "module": module_name,
                "paths": sorted(set(added_paths)),
                "overlap_count": len(overlap),
                "signature_count": len(router_sigs),
            })

        try:
            app.include_router(filtered_router)
            snap["mounted"].append(module_name)
        except Exception as e:
            snap["mount_errors"][module_name] = _err_to_str(e)
            if strict and module_name not in _OPTIONAL_ROUTE_MODULES:
                raise

    snap = _normalize_routes_snapshot(snap, used_strategy="main.controlled_priority_plan", app=app)
    _invalidate_openapi_cache(app)
    return snap


def _mount_routes_once(app: FastAPI, *, phase: str) -> Dict[str, Any]:
    _ensure_app_state_defaults(app)
    if bool(getattr(app.state, "routes_mounted", False)) and isinstance(getattr(app.state, "routes_snapshot", None), dict):
        snap = _normalize_routes_snapshot(
            getattr(app.state, "routes_snapshot", {}),
            used_strategy="main.controlled_priority_plan",
            app=app,
        )
        app.state.routes_mount_phase = str(getattr(app.state, "routes_mount_phase", phase) or phase)
        return snap

    snap = _mount_routes_controlled(app)
    app.state.routes_snapshot = snap
    app.state.routes_mounted = True
    app.state.routes_mount_phase = phase
    app.state.config_source = _SETTINGS.CONFIG_SOURCE
    return snap


# =============================================================================
# Runtime metadata
# =============================================================================
def _runtime_meta(app: Optional[FastAPI] = None) -> Dict[str, Any]:
    snap: Dict[str, Any] = {}
    routes_mounted = False
    routes_mount_phase = ""
    config_source = _SETTINGS.CONFIG_SOURCE
    engine_obj = None
    engine_source = ""
    engine_init_error = ""
    startup_warnings: List[str] = []

    try:
        if app is not None:
            snap = dict(getattr(app.state, "routes_snapshot", {}) or {})
            engine_obj = getattr(app.state, "engine", None)
            engine_source = str(getattr(app.state, "engine_source", "") or "")
            engine_init_error = str(getattr(app.state, "engine_init_error", "") or "")
            routes_mounted = bool(getattr(app.state, "routes_mounted", False))
            routes_mount_phase = str(getattr(app.state, "routes_mount_phase", "") or "")
            config_source = str(getattr(app.state, "config_source", _SETTINGS.CONFIG_SOURCE) or _SETTINGS.CONFIG_SOURCE)
            startup_warnings = list(getattr(app.state, "startup_warnings", []) or [])
    except Exception:
        pass

    live_route_count, live_signature_count = _live_route_metrics(app)
    path_owners = dict(snap.get("canonical_path_owners", {}) or {})
    path_owner_mismatches = dict(snap.get("canonical_path_owner_mismatches", {}) or {})
    route_family_presence = dict(snap.get("route_family_presence", {}) or {})
    missing_required_keys = list(snap.get("missing_required_keys", []) or [])

    if app is not None:
        try:
            path_owners = _canonical_path_owners_from_routes(app)
            path_owner_mismatches = _canonical_path_owner_mismatches(path_owners)
            all_paths = {str(getattr(r, "path", "") or "") for r in (getattr(app, "routes", []) or [])}
            route_family_presence = _route_family_presence_from_paths(all_paths)
            missing_required_keys = _verify_required_route_families(app)
        except Exception:
            pass

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
        "routes_mounted_count": int(snap.get("mounted_count", len(snap.get("mounted", []) or [])) or 0),
        "routes_duplicate_skips_count": int(snap.get("duplicate_skips_count", len(snap.get("duplicate_skips", []) or [])) or 0),
        "routes_partial_duplicate_skips_count": int(snap.get("partial_duplicate_skips_count", len(snap.get("partial_duplicate_skips", []) or [])) or 0),
        "routes_failed_count": int(snap.get("failed_count", len(snap.get("effective_failed_modules", []) or [])) or 0),
        "routes_strategy": str(snap.get("strategy", "") or ""),
        "route_signature_count": live_signature_count if live_signature_count > 0 else int(snap.get("route_signature_count_after_mount", 0) or 0),
        "route_count_live": live_route_count,
        "missing_required_keys": missing_required_keys,
        "route_family_presence": route_family_presence,
        "effective_failed_modules": list(snap.get("effective_failed_modules", []) or []),
        "canonical_path_owners": path_owners,
        "canonical_path_owner_mismatches": path_owner_mismatches,
        "engine_present": engine_obj is not None,
        "engine_ready": engine_obj is not None and not engine_init_error,
        "engine_source": engine_source,
        "engine_init_error": engine_init_error,
        "startup_warnings": startup_warnings,
        "openapi_cached_route_signature_count": int(getattr(getattr(app, "state", None), "_openapi_route_signature_count", -1) or -1) if app is not None else -1,
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

    @app.get("/meta", include_in_schema=False, tags=["meta"])
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
# OpenAPI / Docs
# =============================================================================
def _minimal_openapi_paths(app: FastAPI) -> Dict[str, Any]:
    paths: Dict[str, Any] = {}
    for route in getattr(app, "routes", []) or []:
        if not isinstance(route, APIRoute):
            continue
        if not bool(getattr(route, "include_in_schema", False)):
            continue

        path = str(getattr(route, "path", "") or "")
        methods = sorted(
            str(m).upper()
            for m in (getattr(route, "methods", None) or set())
            if str(m).upper() not in {"HEAD", "OPTIONS"}
        )
        if not path or not methods:
            continue

        path_item = paths.setdefault(path, {})
        for method in methods:
            operation_id = str(getattr(route, "operation_id", "") or f"{route.name}_{method.lower()}")
            path_item[method.lower()] = {
                "summary": _scrub_text(getattr(route, "summary", None) or getattr(route, "name", None) or f"{method} {path}"),
                "operationId": _scrub_text(operation_id),
                "responses": {
                    "200": {"description": "Successful Response"}
                },
            }
    return paths


def _install_custom_openapi(app: FastAPI) -> None:
    def build_openapi_schema() -> Dict[str, Any]:
        current_signature_count = _route_signature_count(app, include_builtin=True)
        cached_signature_count = int(getattr(app.state, "_openapi_route_signature_count", -1) or -1)
        cached_schema = getattr(app, "openapi_schema", None)
        if isinstance(cached_schema, dict) and cached_signature_count == current_signature_count:
            return cached_schema

        schema: Dict[str, Any]
        try:
            generated = get_openapi(
                title=_SETTINGS.APP_NAME,
                version=str(_SETTINGS.APP_VERSION),
                routes=app.routes,
                description="Tadawul Fast Bridge API",
            )
            schema = generated if isinstance(generated, dict) else {}
        except Exception as e:
            logger.warning("OpenAPI generation failed; using fallback schema: %s", _err_to_str(e))
            schema = {
                "openapi": "3.1.0",
                "info": {
                    "title": _SETTINGS.APP_NAME,
                    "version": str(_SETTINGS.APP_VERSION),
                    "description": "Fallback schema generated by main.py",
                },
                "paths": _minimal_openapi_paths(app),
                "x-openapi-fallback": True,
                "x-openapi-error": _err_to_str(e),
            }
        else:
            schema.setdefault("openapi", "3.1.0")
            schema.setdefault("info", {
                "title": _SETTINGS.APP_NAME,
                "version": str(_SETTINGS.APP_VERSION),
            })
            if not isinstance(schema.get("paths"), dict) or not schema.get("paths"):
                schema["paths"] = _minimal_openapi_paths(app)
                schema["x-openapi-rebuilt-paths"] = True

        backend_base = str(_SETTINGS.BACKEND_BASE_URL or "").strip()
        if backend_base:
            schema["servers"] = [{"url": backend_base}]
        schema["x-generated-at-utc"] = datetime.now(timezone.utc).isoformat()
        schema["x-route-signature-count"] = current_signature_count

        app.openapi_schema = schema
        app.state._openapi_route_signature_count = current_signature_count
        return schema

    def custom_openapi() -> Dict[str, Any]:
        return build_openapi_schema()

    app.openapi = custom_openapi  # type: ignore[assignment]

    @app.api_route("/openapi.json", methods=["GET", "HEAD"], include_in_schema=False)
    async def openapi_json(request: Request):
        if request.method == "HEAD":
            return Response(status_code=200, media_type="application/json", headers={"Cache-Control": "no-store"})
        return _StrictJSONResponse(content=build_openapi_schema(), headers={"Cache-Control": "no-store"})

    if bool(_SETTINGS.ENABLE_SWAGGER):
        @app.get("/docs", include_in_schema=False)
        async def custom_docs():
            return get_swagger_ui_html(
                openapi_url="/openapi.json",
                title=f"{_SETTINGS.APP_NAME} - Swagger UI",
            )

    if bool(_SETTINGS.ENABLE_REDOC):
        @app.get("/redoc", include_in_schema=False)
        async def custom_redoc():
            return get_redoc_html(
                openapi_url="/openapi.json",
                title=f"{_SETTINGS.APP_NAME} - ReDoc",
            )


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
    @asynccontextmanager
    async def lifespan(app: FastAPI):
        _ensure_app_state_defaults(app)

        try:
            snap = _mount_routes_once(app, phase="startup")
            logger.info(
                "Routes verified at startup: mounted=%s duplicate_skips=%s partial_duplicate_skips=%s missing=%s failed=%s strategy=%s route_signatures=%s",
                snap.get("mounted_count", 0),
                snap.get("duplicate_skips_count", 0),
                snap.get("partial_duplicate_skips_count", 0),
                snap.get("missing_count", 0),
                snap.get("failed_count", 0),
                snap.get("strategy", ""),
                snap.get("route_signature_count_after_mount", 0),
            )

            missing_required = list(snap.get("missing_required_keys", []) or [])
            if missing_required:
                warning = f"missing_required_route_families: {', '.join(missing_required)}"
                logger.warning(warning)
                _append_startup_warning(app, warning)

            owner_mismatches = dict(snap.get("canonical_path_owner_mismatches", {}) or {})
            if owner_mismatches:
                warning = f"canonical_path_owner_mismatches: {json.dumps(owner_mismatches, ensure_ascii=False)}"
                logger.warning(warning)
                _append_startup_warning(app, warning)
        except Exception as e:
            msg = f"Route mounting crashed during startup: {_err_to_str(e)}"
            logger.error(msg, exc_info=True)
            _append_startup_warning(app, msg)
            if _env_bool("ROUTES_STRICT_IMPORT", False):
                raise

        err = await _maybe_init_engine(app)
        if err:
            strict = bool(_SETTINGS.INIT_ENGINE_STRICT)
            if strict:
                logger.error("Engine init failed (strict): %s", err)
                raise RuntimeError(f"Engine init failed: {err}")
            logger.warning("Engine init failed (non-fatal): %s", err)
            _append_startup_warning(app, f"engine_init_warning: {err}")

        logger.info("Startup complete: %s", json.dumps(_runtime_meta(app), ensure_ascii=False))
        try:
            yield
        finally:
            try:
                await _maybe_close_google_sheets_service()
            except Exception:
                logger.warning("Google Sheets service shutdown failed", exc_info=True)
            try:
                await _maybe_close_engine()
            except Exception:
                logger.warning("Engine shutdown failed", exc_info=True)

    app = FastAPI(
        title=_SETTINGS.APP_NAME,
        version=str(_SETTINGS.APP_VERSION),
        debug=bool(_SETTINGS.DEBUG),
        docs_url=None,
        redoc_url=None,
        openapi_url=None,
        default_response_class=_StrictJSONResponse,
        lifespan=lifespan,
    )
    _ensure_app_state_defaults(app)

    app.add_middleware(RequestIDMiddleware)
    app.add_middleware(NoResponseGuardMiddleware)
    app.add_middleware(GZipMiddleware, minimum_size=1024)

    if bool(_SETTINGS.ENABLE_CORS_ALL_ORIGINS):
        app.add_middleware(
            CORSMiddleware,
            allow_origins=["*"],
            allow_credentials=True,
            allow_methods=["*"],
            allow_headers=["*"],
        )
    else:
        origins = _parse_csv(_SETTINGS.CORS_ORIGINS)
        if origins:
            app.add_middleware(
                CORSMiddleware,
                allow_origins=origins,
                allow_credentials=True,
                allow_methods=["*"],
                allow_headers=["*"],
            )

    @app.exception_handler(Exception)
    async def unhandled_exception_handler(request: Request, exc: Exception):
        request_id = _request_id_from_request(request)
        logger.error(
            "Unhandled exception",
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
                "Routes mounted at app creation: mounted=%s duplicate_skips=%s partial_duplicate_skips=%s missing=%s failed=%s strategy=%s route_signatures=%s",
                snap.get("mounted_count", 0),
                snap.get("duplicate_skips_count", 0),
                snap.get("partial_duplicate_skips_count", 0),
                snap.get("missing_count", 0),
                snap.get("failed_count", 0),
                snap.get("strategy", ""),
                snap.get("route_signature_count_after_mount", 0),
            )
        except Exception as e:
            logger.error("Prestart route mounting failed: %s", e, exc_info=True)
            _append_startup_warning(app, f"prestart_route_mount_failed: {_err_to_str(e)}")
            if _env_bool("ROUTES_STRICT_IMPORT", False):
                raise
    else:
        _append_startup_warning(app, "prestart_route_mount_disabled")
        logger.info("Prestart route mounting disabled by PRESTART_MOUNT_ROUTES")

    _install_custom_openapi(app)

    @app.get("/_debug/routes", include_in_schema=False)
    async def debug_routes(request: Request):
        if _SETTINGS.APP_ENV == "production" and bool(_SETTINGS.REQUIRE_AUTH) and not _auth_ok(request):
            return JSONResponse(status_code=401, content={"status": "error", "error": "unauthorized"})

        route_paths = sorted({str(getattr(r, "path", "") or "") for r in (getattr(request.app, "routes", []) or [])})
        route_sigs = sorted([f"{path} [{method}]" for path, method in _app_route_signature_set(request.app, include_builtin=True)])
        path_owners = _canonical_path_owners_from_routes(request.app)
        owner_mismatches = _canonical_path_owner_mismatches(path_owners)
        openapi_schema = request.app.openapi()

        enriched_alias_presence = {
            "/v1/enriched/sheet-rows": "/v1/enriched/sheet-rows" in route_paths,
            "/v1/enriched/sheet_rows": "/v1/enriched/sheet_rows" in route_paths,
            "/v1/enriched_quote/sheet-rows": "/v1/enriched_quote/sheet-rows" in route_paths,
            "/v1/enriched_quote/sheet_rows": "/v1/enriched_quote/sheet_rows" in route_paths,
            "/v1/enriched-quote/sheet-rows": "/v1/enriched-quote/sheet-rows" in route_paths,
            "/v1/enriched-quote/sheet_rows": "/v1/enriched-quote/sheet_rows" in route_paths,
        }

        return {
            "status": "ok",
            "routes_snapshot": _normalize_routes_snapshot(getattr(request.app.state, "routes_snapshot", {}), used_strategy="main.controlled_priority_plan", app=request.app),
            "app_route_count": len(getattr(request.app, "routes", []) or []),
            "app_route_signature_count": _route_signature_count(request.app, include_builtin=True),
            "route_paths": route_paths,
            "route_signatures": route_sigs,
            "canonical_path_owners": path_owners,
            "canonical_path_owner_mismatches": owner_mismatches,
            "enriched_alias_presence": enriched_alias_presence,
            "openapi_paths_count": len((openapi_schema.get("paths", {}) if isinstance(openapi_schema, dict) else {}) or {}),
            "openapi_has_paths": bool((openapi_schema.get("paths", {}) if isinstance(openapi_schema, dict) else {}) or {}),
            **_runtime_meta(app),
        }

    return app


app = create_app()
application = app


if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=int(os.getenv("PORT", "10000")), reload=False)
