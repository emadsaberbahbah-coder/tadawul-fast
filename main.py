#!/usr/bin/env python3
"""
main.py
================================================================================
TADAWUL FAST BRIDGE -- RENDER-SAFE FASTAPI ENTRYPOINT (v8.12.0)
================================================================================
FASTAPI-NATIVE ROUTER INCLUDE / PRESTART-FIRST ROUTE MOUNT / OPENAPI CACHE SAFE
REQUEST-ID SAFE / ENGINE-STATE AWARE / CONTROLLED-ROUTE-OWNERSHIP SAFE
STRICT-JSON SAFE / HEALTH / META ALIAS SAFE / DEBUG ROUTE SAFE
INVESTMENT-ADVISOR CANONICAL OWNER PROTECTION / ADVANCED ROUTE PRIORITY SAFE

Why this revision (v8.12.0 vs v8.11.3)
--------------------------------------
- FIX CRITICAL: `/readyz` and `/v1/readyz` now return HTTP 503 when required
    route families are missing/failed, canonical route ownership is wrong, or
    the configured boot engine is unavailable. `/livez` remains a pure process
    liveness probe. `/health` remains HTTP 200 but reports `degraded` plus the
    exact readiness reasons. Render can no longer mark a structurally broken
    deployment healthy merely because the Python process accepted connections.
- FIX SECURITY: production exception responses no longer disclose exception
    class/message by default. Full details remain in logs and may be exposed
    only with DEBUG/EXPOSE_ERROR_DETAILS outside the normal production posture.
- FIX SECURITY: wildcard CORS can never be combined with credentials. Explicit
    origins may opt into credentials through CORS_ALLOW_CREDENTIALS.
- FIX SECURITY: Swagger/ReDoc default OFF in production and ON outside
    production unless explicitly configured. Public status routes return a
    reduced metadata view when authentication is required.
- FIX CONTRACT: filtered router cloning now preserves response-model controls,
    callbacks, OpenAPI extras, and unique-id behavior instead of silently
    weakening FastAPI validation/documentation.
- FIX OPERATIONS: logging no longer clears handlers installed by Gunicorn,
    OpenTelemetry, Sentry, or the hosting platform unless
    TFB_LOG_REPLACE_ROOT_HANDLERS=1 is explicitly set.
- FIX OPERATIONS: caller-supplied request IDs are sanitized and length-bounded
    before entering logs or response headers.
- FIX CONFIG: generic-object numeric settings now fail safely instead of
    crashing module import on malformed values; ALLOW_QUERY_TOKEN is represented
    explicitly rather than being silently dropped by `_SettingsView`.
- FIX MIDDLEWARE: RequestID remains outermost while CORS still decorates guarded
    error responses.
- SAFE SCOPE: no scoring, selection, portfolio, provider, or workbook logic is
    changed by this revision.

Why this revision (v8.11.3 vs v8.11.2)
--------------------------------------
- ADD: `engine_version` in _runtime_meta() -> exposed on every status payload
    (/health, /healthz, /meta, /ping, /, /readyz, /livez and the /v1 aliases).
    Background: the payload reported engine_present/engine_ready/engine_source
    but never WHICH engine build is running. Verifying a deploy therefore
    required a fresh Render Shell session (`import core.data_engine_v2; print
    __version__`) or scrolling the deploy log for the module banner -- a
    repeated operational cost, and a real ambiguity window while Render is
    mid-rebuild (a "healthy" response can come from the OLD instance). One
    browser hit on /health now answers it: entry_version proves THIS main.py
    deployed; engine_version proves which data_engine_v2 build is live.
- New helper _resolve_engine_version(engine_obj, engine_source): reads
    __version__ (fallback ENGINE_VERSION) from the ALREADY-IMPORTED engine
    module via sys.modules -- it never imports anything (zero side effects,
    zero startup cost), falls back to the engine object's own __version__/
    version attributes, and returns "" on any failure (pure + fail-safe:
    the status payload can never break because of it).
- ADD: APP_ENTRY_VERSION bumped to 8.11.3.
- SAFE: every v8.11.2 field is preserved verbatim; this is one ADDITIVE key.
    No route, mount-plan, middleware, auth, or engine-lifecycle change.

Why this revision (v8.11.2 vs v8.11.1)
--------------------------------------
- FIX HIGH: Removed `routes.advanced_sheet_rows` from _CONTROLLED_ROUTE_PLAN.
    Background: every route this module exposes (`/v1/advanced/sheet-rows`,
    `/v1/advanced/health`) is canonically owned by `routes.investment_advisor`
    per _CONTROLLED_CANONICAL_OWNER_MAP. When the controlled mount loop
    processed `routes.advanced_sheet_rows`, _clone_filtered_router stripped
    every route via the canonical-owner check (line: `canonical_owner != key`),
    leaving 0 routes to add. v8.11.0 silently bucketed that into `no_router`
    without counting it as failed; v8.11.1 expanded the strict-mode fail
    surface AND _effective_failed_modules now counts `no_router` entries as
    failed regardless of strict mode -- producing `failed_count: 1` and
    `effective_failed_modules: ["routes.advanced_sheet_rows"]` on every boot
    even though the service is fully functional (every URL is served by
    `routes.investment_advisor` instead).

    Removing the entry from the plan eliminates the redundant import,
    eliminates the misleading "failed" status, and matches the actual route
    topology where investment_advisor owns the entire `/v1/advanced/*` prefix.
    The file `routes/advanced_sheet_rows.py` itself is left in the repo
    untouched; it is simply no longer mounted.

- ADD: APP_ENTRY_VERSION bumped to 8.11.2.

- SAFE: All v8.11.1 behavior preserved:
    - Middleware ordering (RequestID outermost)
    - X-Request-ID header on synthesized 500 responses
    - api_key kwarg in _call_auth_ok_flexible
    - Strict-mode raise on "0 routes after filtering"
    - SERVICE_VERSION cross-module canonical alias
    - service_version key in _runtime_meta()
    - _append_startup_warning consolidation helper
    - _path_present_any helper
    - Custom OpenAPI signature-count-keyed cache
    - Controlled route mounting with canonical owner protection
    - investment_advisor mounts BEFORE any potential advanced_sheet_rows
      (now moot since the latter is removed)
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
import re
import sys
import uuid
from contextlib import asynccontextmanager
from dataclasses import asdict, dataclass, is_dataclass
from datetime import date, datetime, time as dt_time, timezone
from decimal import Decimal
from enum import Enum
from typing import Any, Dict, Iterable, List, Mapping, Optional, Set, Tuple

from fastapi import APIRouter, FastAPI, Request, Response
from fastapi.encoders import jsonable_encoder
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


# =============================================================================
# Version
# =============================================================================
APP_ENTRY_VERSION = "8.12.1"
# =============================================================================
# v8.12.1 (2026-07-24) — SAFE-DEFAULTS PASS OVER v8.12.0.
#
# v8.12.0 is good work: zero function/class removals, _CONTROLLED_ROUTE_PLAN,
# _CONTROLLED_CANONICAL_OWNER_MAP and _OPTIONAL_ROUTE_MODULES all preserved
# byte-identical, and eleven genuinely useful hardening helpers added. Nothing
# below removes any of it.
#
# What v8.12.1 changes is DEFAULTS, because v8.12.0 shipped three live contract
# changes armed:
#
#  1. /readyz COULD RETURN 503. In v8.11.3 that endpoint returned HTTP 200
#     unconditionally — it had never returned anything else. v8.12.0 makes it
#     503 whenever _readiness_evaluation finds a reason, and two of those
#     reasons fire under normal, intended conditions:
#       - routes_failed_count > 0, while _OPTIONAL_ROUTE_MODULES exists
#         precisely so four modules ARE allowed to fail;
#       - auth_not_enforced when REQUIRE_AUTH is not true in production, which
#         would pin the service at 503 permanently.
#     If Render's Health Check Path is /readyz, that is a failed deploy or a
#     restart loop on the first boot — during the S-1 evidence window.
#
#  2. /health reports "degraded" instead of "healthy" under the same
#     conditions. The Apps Script cockpit parses this and is NOT in version
#     control, so the blast radius cannot be checked from the repository.
#
#  3. /status strips fields for unauthenticated production callers, and
#     unhandled errors return "internal_server_error" instead of detail.
#     Same unverifiable frontend dependency.
#
# THE FIX IS NOT TO REMOVE ANY OF IT. Every behaviour above is now behind a
# kill-switch DEFAULTING TO v8.11.3 SEMANTICS, per the standing rule that
# changes ship environment-gated with backward-compatible defaults. Deploying
# this file changes NO observable endpoint behaviour.
#
# What you gain on day one, at zero risk: /readyz and /health now REPORT
# `ready` and `readiness_reasons` in the payload while still returning 200 and
# "healthy". You can read exactly what strict mode WOULD do before arming it.
# When that list is empty and the Health Check Path is known, flip one switch.
#
#   TFB_READYZ_STRICT=1          -> /readyz may return 503   (default: never)
#   TFB_HEALTH_STRICT=1          -> /health may say degraded  (default: never)
#   STATUS_DETAILS_REQUIRE_AUTH=1-> /status reduced for anon   (default: full)
#   EXPOSE_ERROR_DETAILS=0       -> errors hide detail         (default: show)
#
# KEPT ACTIVE (no contract change, pure hardening): the _coerce_* helpers,
# _normalize_request_id sanitising, and docs-off-when-APP_ENV=production
# (already moot — ENABLE_SWAGGER/ENABLE_REDOC are 0 in Render and /docs
# returns 404, verified 2026-07-24).
# =============================================================================
# v8.11.1: Cross-module canonical alias (matches worker.py v4.3.0,
# config.py v7.3.0, env.py v7.8.1, track_performance v6.4.0, etc.)
SERVICE_VERSION = APP_ENTRY_VERSION

# Project-canonical 8-value boolean vocabulary.
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
)

# Canonical owners are the route-family keys used in the controlled plan.
_CONTROLLED_CANONICAL_OWNER_MAP: Dict[str, str] = {
    "/v1/advisor": "advisor",
    "/v1/advisor/sheet-rows": "advisor",
    "/v1/investment_advisor": "investment_advisor",
    "/v1/investment-advisor": "investment_advisor",
    "/v1/advanced": "investment_advisor",
    "/v1/advanced/sheet-rows": "investment_advisor",
    "/v1/analysis": "analysis_sheet_rows",
    "/v1/analysis/sheet-rows": "analysis_sheet_rows",
    "/v1/schema": "advanced_analysis",
    "/v1/schema/sheet-spec": "advanced_analysis",
    "/schema": "advanced_analysis",
    "/schema/sheet-spec": "advanced_analysis",
    "/sheet-rows": "advanced_analysis",
    "/v1/enriched": "enriched_quote",
    "/v1/enriched/sheet-rows": "enriched_quote",
    "/v1/enriched_quote": "enriched_quote",
    "/v1/enriched_quote/sheet-rows": "enriched_quote",
    "/v1/enriched-quote": "enriched_quote",
    "/v1/enriched-quote/sheet-rows": "enriched_quote",
    "/quote": "enriched_quote",
    "/quotes": "enriched_quote",
}

_OPTIONAL_ROUTE_MODULES: Set[str] = {
    "routes.config",
    "routes.data_dictionary",
    "routes.ai_analysis",
    "routes.routes_argaam",
}

# v8.11.2 FIX: routes.advanced_sheet_rows removed from the controlled plan.
# Every URL it serves (/v1/advanced/sheet-rows, /v1/advanced/health) is
# canonically owned by routes.investment_advisor per the owner map above,
# so the filtered-router clone stripped 100% of its routes and bucketed it
# under no_router["router filtered to 0 routes"]. v8.11.1's stricter
# _effective_failed_modules counted that as failed, producing a misleading
# `failed=1` boot status. The file routes/advanced_sheet_rows.py is still
# in the repo but is no longer mounted; investment_advisor handles every
# /v1/advanced/* path.
_CONTROLLED_ROUTE_PLAN: Tuple[Tuple[str, str], ...] = (
    ("config", "routes.config"),
    ("advanced_analysis", "routes.advanced_analysis"),
    ("analysis_sheet_rows", "routes.analysis_sheet_rows"),
    ("investment_advisor", "routes.investment_advisor"),
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


def _coerce_int_value(value: Any, default: int) -> int:
    try:
        return int(float(value))
    except Exception:
        return int(default)


def _coerce_float_value(value: Any, default: float) -> float:
    try:
        parsed = float(value)
        return parsed if math.isfinite(parsed) else float(default)
    except Exception:
        return float(default)


def _is_production_env(value: Any) -> bool:
    return str(value or "").strip().lower() in {"prod", "production"}


def _default_docs_enabled() -> bool:
    return not _is_production_env(_env_str("APP_ENV", "production"))


def _parse_csv(value: str) -> List[str]:
    s = (value or "").strip()
    if not s:
        return []
    return [x.strip() for x in s.split(",") if x.strip()]


def _coerce_csv_text(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, str):
        return value.strip()
    if isinstance(value, (list, tuple, set, frozenset)):
        return ",".join(str(x).strip() for x in value if str(x).strip())
    try:
        return str(value).strip()
    except Exception:
        return ""


def _coerce_version(value: Any, default: str) -> str:
    try:
        text = str(value or "").strip()
    except Exception:
        text = ""
    if text.lower() in {"", "unknown", "dev", "none", "null"}:
        return str(default)
    return text


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
    """v8.11.1: check if ANY of the candidate paths is present."""
    return any(c in paths for c in candidates)


_REQUEST_ID_MAX_LEN = 128
_REQUEST_ID_UNSAFE_RE = re.compile(r"[^A-Za-z0-9._:/-]+")


def _normalize_request_id(value: Any) -> str:
    try:
        raw = str(value or "").strip()
    except Exception:
        raw = ""
    if raw:
        raw = _REQUEST_ID_UNSAFE_RE.sub("_", raw)[:_REQUEST_ID_MAX_LEN].strip("._:/-")
    return raw or uuid.uuid4().hex[:12]


def _request_id_from_request(request: Request) -> str:
    try:
        rid = getattr(request.state, "request_id", "")
        if rid:
            return _normalize_request_id(rid)
    except Exception:
        pass
    try:
        hdr = request.headers.get("X-Request-ID", "")
        if hdr:
            return _normalize_request_id(hdr)
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
    """v8.11.1: consolidated helper -- deduplicates and truncates long messages.

    Replaces the 7+ inline `try: app.state.startup_warnings.append(...) except:
    pass` blocks scattered through v8.11.0's lifespan and create_app.
    """
    try:
        msg = str(message or "")
        if len(msg) > 2000:
            msg = msg[:2000] + "...(truncated)"
        warnings = list(getattr(app.state, "startup_warnings", []) or [])
        if msg not in warnings:
            warnings.append(msg)
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
    resolved_level = getattr(logging, level, logging.INFO)
    log_json = _env_bool("LOG_JSON", False) or (
        _env_str("LOG_FORMAT", "").lower() == "json"
    )
    replace_handlers = _env_bool("TFB_LOG_REPLACE_ROOT_HANDLERS", False)

    root = logging.getLogger()
    root.setLevel(resolved_level)

    if replace_handlers:
        root.handlers.clear()

    # Do not destroy handlers installed by Gunicorn/Render/telemetry libraries.
    # Add our formatter only when no handler exists (or replacement was asked).
    if not root.handlers:
        handler = logging.StreamHandler(sys.stdout)
        if log_json:
            handler.setFormatter(_JsonFormatter())
        else:
            handler.setFormatter(
                logging.Formatter(
                    "%(asctime)s | %(levelname)-8s | %(name)s | %(message)s"
                )
            )
        root.addHandler(handler)

    for name in (
        "uvicorn", "uvicorn.error", "uvicorn.access",
        "gunicorn", "gunicorn.error",
    ):
        logging.getLogger(name).setLevel(resolved_level)

    logging.getLogger("httpx").setLevel(logging.WARNING)
    main_logger = logging.getLogger("main")
    main_logger.setLevel(resolved_level)
    return main_logger


logger = _setup_logging()


# =============================================================================
# Settings
# =============================================================================
@dataclass(frozen=True)
class _SettingsView:
    APP_NAME: str = "Tadawul Fast Bridge"
    APP_VERSION: str = APP_ENTRY_VERSION
    APP_ENV: str = "production"
    TIMEZONE_DEFAULT: str = "Asia/Riyadh"

    DEBUG: bool = False
    ENABLE_SWAGGER: bool = False
    ENABLE_REDOC: bool = False
    INIT_ENGINE_ON_BOOT: bool = True
    INIT_ENGINE_STRICT: bool = False
    ENGINE_INIT_TIMEOUT_SEC: float = 12.0
    PRESTART_MOUNT_ROUTES: bool = True

    REQUIRE_AUTH: bool = True
    OPEN_MODE: bool = False
    AUTH_HEADER_NAME: str = "X-APP-TOKEN"
    ALLOW_QUERY_TOKEN: bool = False
    STATUS_DETAILS_REQUIRE_AUTH: bool = False   # v8.12.1: v8.11.3 default
    EXPOSE_ERROR_DETAILS: bool = True            # v8.12.1: v8.11.3 default

    ENABLE_CORS_ALL_ORIGINS: bool = False
    CORS_ORIGINS: str = ""
    CORS_ALLOW_CREDENTIALS: bool = False

    READY_REQUIRE_ENGINE: bool = False           # v8.12.1: report-only
    READY_REQUIRE_ROUTES: bool = False           # v8.12.1: report-only
    READY_REQUIRE_AUTH: bool = False             # v8.12.1: report-only

    BACKEND_BASE_URL: str = ""
    ENGINE_CACHE_TTL_SEC: int = 20
    MAX_REQUESTS_PER_MINUTE: int = 240

    CONFIG_SOURCE: str = "env"


def _settings_from_generic_object(s: Any, source: str) -> _SettingsView:
    return _SettingsView(
        APP_NAME=str(_pick_attr(
            s, "APP_NAME", "app_name", "service_name",
            default=_env_str("APP_NAME", "Tadawul Fast Bridge"),
        )),
        APP_VERSION=_coerce_version(_pick_attr(
            s, "APP_VERSION", "app_version",
            default=_env_str("APP_VERSION", APP_ENTRY_VERSION),
        ), APP_ENTRY_VERSION),
        APP_ENV=str(_pick_attr(
            s, "APP_ENV", "app_env", "environment", "env",
            default=_env_str("APP_ENV", "production"),
        )),
        TIMEZONE_DEFAULT=str(_pick_attr(
            s, "TIMEZONE_DEFAULT", "timezone_default", "timezone",
            default=_env_str("TZ", "Asia/Riyadh"),
        )),
        DEBUG=_to_bool(_pick_attr(
            s, "DEBUG", "debug", default=_env_bool("DEBUG", False),
        ), _env_bool("DEBUG", False)),
        ENABLE_SWAGGER=_to_bool(_pick_attr(
            s, "ENABLE_SWAGGER", "enable_swagger",
            default=_env_bool("ENABLE_SWAGGER", _default_docs_enabled()),
        ), _env_bool("ENABLE_SWAGGER", _default_docs_enabled())),
        ENABLE_REDOC=_to_bool(_pick_attr(
            s, "ENABLE_REDOC", "enable_redoc",
            default=_env_bool("ENABLE_REDOC", _default_docs_enabled()),
        ), _env_bool("ENABLE_REDOC", _default_docs_enabled())),
        INIT_ENGINE_ON_BOOT=_to_bool(_pick_attr(
            s, "INIT_ENGINE_ON_BOOT", "init_engine_on_boot",
            default=_env_bool("INIT_ENGINE_ON_BOOT", True),
        ), _env_bool("INIT_ENGINE_ON_BOOT", True)),
        INIT_ENGINE_STRICT=_to_bool(_pick_attr(
            s, "INIT_ENGINE_STRICT", "init_engine_strict",
            default=_env_bool("INIT_ENGINE_STRICT", False),
        ), _env_bool("INIT_ENGINE_STRICT", False)),
        ENGINE_INIT_TIMEOUT_SEC=_coerce_float_value(_pick_attr(
            s, "ENGINE_INIT_TIMEOUT_SEC", "engine_init_timeout_sec",
            default=_env_float("ENGINE_INIT_TIMEOUT_SEC", 12.0),
        ), _env_float("ENGINE_INIT_TIMEOUT_SEC", 12.0)),
        PRESTART_MOUNT_ROUTES=_to_bool(_pick_attr(
            s, "PRESTART_MOUNT_ROUTES", "prestart_mount_routes",
            default=_env_bool("PRESTART_MOUNT_ROUTES", True),
        ), _env_bool("PRESTART_MOUNT_ROUTES", True)),
        REQUIRE_AUTH=_to_bool(_pick_attr(
            s, "REQUIRE_AUTH", "require_auth",
            default=_env_bool("REQUIRE_AUTH", True),
        ), _env_bool("REQUIRE_AUTH", True)),
        OPEN_MODE=_to_bool(_pick_attr(
            s, "OPEN_MODE", "open_mode",
            default=_env_bool("OPEN_MODE", False),
        ), _env_bool("OPEN_MODE", False)),
        AUTH_HEADER_NAME=str(_pick_attr(
            s, "AUTH_HEADER_NAME", "auth_header_name",
            default=_env_str("AUTH_HEADER_NAME", "X-APP-TOKEN"),
        )),
        ALLOW_QUERY_TOKEN=_to_bool(_pick_attr(
            s, "ALLOW_QUERY_TOKEN", "allow_query_token",
            default=_env_bool("ALLOW_QUERY_TOKEN", False),
        ), _env_bool("ALLOW_QUERY_TOKEN", False)),
        STATUS_DETAILS_REQUIRE_AUTH=_to_bool(_pick_attr(
            s, "STATUS_DETAILS_REQUIRE_AUTH", "status_details_require_auth",
            default=_env_bool("STATUS_DETAILS_REQUIRE_AUTH", False),
        ), _env_bool("STATUS_DETAILS_REQUIRE_AUTH", False)),
        EXPOSE_ERROR_DETAILS=_to_bool(_pick_attr(
            s, "EXPOSE_ERROR_DETAILS", "expose_error_details",
            default=_env_bool("EXPOSE_ERROR_DETAILS", True),
        ), _env_bool("EXPOSE_ERROR_DETAILS", True)),
        ENABLE_CORS_ALL_ORIGINS=_to_bool(_pick_attr(
            s, "ENABLE_CORS_ALL_ORIGINS", "enable_cors_all_origins",
            default=_env_bool("ENABLE_CORS_ALL_ORIGINS", False),
        ), _env_bool("ENABLE_CORS_ALL_ORIGINS", False)),
        CORS_ORIGINS=_coerce_csv_text(_pick_attr(
            s, "CORS_ORIGINS", "cors_origins",
            default=_env_str("CORS_ORIGINS", ""),
        )),
        CORS_ALLOW_CREDENTIALS=_to_bool(_pick_attr(
            s, "CORS_ALLOW_CREDENTIALS", "cors_allow_credentials",
            default=_env_bool("CORS_ALLOW_CREDENTIALS", False),
        ), _env_bool("CORS_ALLOW_CREDENTIALS", False)),
        READY_REQUIRE_ENGINE=_to_bool(_pick_attr(
            s, "READY_REQUIRE_ENGINE", "ready_require_engine",
            default=_env_bool("READY_REQUIRE_ENGINE", False),
        ), _env_bool("READY_REQUIRE_ENGINE", False)),
        READY_REQUIRE_ROUTES=_to_bool(_pick_attr(
            s, "READY_REQUIRE_ROUTES", "ready_require_routes",
            default=_env_bool("READY_REQUIRE_ROUTES", False),
        ), _env_bool("READY_REQUIRE_ROUTES", False)),
        READY_REQUIRE_AUTH=_to_bool(_pick_attr(
            s, "READY_REQUIRE_AUTH", "ready_require_auth",
            default=_env_bool("READY_REQUIRE_AUTH", False),
        ), _env_bool("READY_REQUIRE_AUTH", False)),
        BACKEND_BASE_URL=str(_pick_attr(
            s, "BACKEND_BASE_URL", "backend_base_url",
            default=_env_str("BACKEND_BASE_URL", ""),
        )),
        ENGINE_CACHE_TTL_SEC=_coerce_int_value(_pick_attr(
            s, "ENGINE_CACHE_TTL_SEC", "engine_cache_ttl_sec",
            default=_env_int("ENGINE_CACHE_TTL_SEC", 20),
        ), _env_int("ENGINE_CACHE_TTL_SEC", 20)),
        MAX_REQUESTS_PER_MINUTE=_coerce_int_value(_pick_attr(
            s, "MAX_REQUESTS_PER_MINUTE", "max_requests_per_minute",
            default=_env_int("MAX_REQUESTS_PER_MINUTE", 240),
        ), _env_int("MAX_REQUESTS_PER_MINUTE", 240)),
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
        APP_VERSION=_env_str("APP_VERSION", APP_ENTRY_VERSION),
        APP_ENV=_env_str("APP_ENV", "production"),
        TIMEZONE_DEFAULT=_env_str("TZ", "Asia/Riyadh"),
        DEBUG=_env_bool("DEBUG", False),
        ENABLE_SWAGGER=_env_bool("ENABLE_SWAGGER", _default_docs_enabled()),
        ENABLE_REDOC=_env_bool("ENABLE_REDOC", _default_docs_enabled()),
        INIT_ENGINE_ON_BOOT=_env_bool("INIT_ENGINE_ON_BOOT", True),
        INIT_ENGINE_STRICT=_env_bool("INIT_ENGINE_STRICT", False),
        ENGINE_INIT_TIMEOUT_SEC=_env_float("ENGINE_INIT_TIMEOUT_SEC", 12.0),
        PRESTART_MOUNT_ROUTES=_env_bool("PRESTART_MOUNT_ROUTES", True),
        REQUIRE_AUTH=_env_bool("REQUIRE_AUTH", True),
        OPEN_MODE=_env_bool("OPEN_MODE", False),
        AUTH_HEADER_NAME=_env_str("AUTH_HEADER_NAME", "X-APP-TOKEN"),
        ALLOW_QUERY_TOKEN=_env_bool("ALLOW_QUERY_TOKEN", False),
        STATUS_DETAILS_REQUIRE_AUTH=_env_bool("STATUS_DETAILS_REQUIRE_AUTH", False),
        EXPOSE_ERROR_DETAILS=_env_bool("EXPOSE_ERROR_DETAILS", True),
        ENABLE_CORS_ALL_ORIGINS=_env_bool("ENABLE_CORS_ALL_ORIGINS", False),
        CORS_ORIGINS=_env_str("CORS_ORIGINS", ""),
        CORS_ALLOW_CREDENTIALS=_env_bool("CORS_ALLOW_CREDENTIALS", False),
        READY_REQUIRE_ENGINE=_env_bool("READY_REQUIRE_ENGINE", False),
        READY_REQUIRE_ROUTES=_env_bool("READY_REQUIRE_ROUTES", False),
        READY_REQUIRE_AUTH=_env_bool("READY_REQUIRE_AUTH", False),
        BACKEND_BASE_URL=_env_str("BACKEND_BASE_URL", ""),
        ENGINE_CACHE_TTL_SEC=_env_int("ENGINE_CACHE_TTL_SEC", 20),
        MAX_REQUESTS_PER_MINUTE=_env_int("MAX_REQUESTS_PER_MINUTE", 240),
        CONFIG_SOURCE="raw_env",
    )


_SETTINGS = _load_settings()


def _expose_error_details() -> bool:
    if bool(getattr(_SETTINGS, "DEBUG", False)):
        return True
    if bool(getattr(_SETTINGS, "EXPOSE_ERROR_DETAILS", False)):
        return True
    return not _is_production_env(getattr(_SETTINGS, "APP_ENV", "production"))


def _public_error_text(exc: BaseException, fallback: str = "internal_server_error") -> str:
    return _err_to_str(exc) if _expose_error_details() else fallback


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
    if not hasattr(app.state, "routes_snapshot") or not isinstance(
        getattr(app.state, "routes_snapshot", None), dict,
    ):
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
def _call_auth_ok_flexible(
    fn: Any,
    request: Request,
    token_value: str,
    authorization: str,
    api_key_value: str = "",
) -> bool:
    path = str(getattr(getattr(request, "url", None), "path", "") or "")
    headers_dict = dict(request.headers)

    settings = None
    for mod_name in ("config", "core.config"):
        try:
            cfg = importlib.import_module(mod_name)
            getter = getattr(cfg, "get_settings_cached", None) or getattr(
                cfg, "get_settings", None,
            )
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
            "api_key": api_key_value or None,   # v8.11.1: config.py v7.3.0 compat
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

    x_app_token = (
        request.headers.get(_SETTINGS.AUTH_HEADER_NAME, "")
        or request.headers.get("X-APP-TOKEN", "")
    )
    api_key_value = (
        request.headers.get("X-API-Key", "") or request.headers.get("Api-Key", "")
    )
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
                return _call_auth_ok_flexible(
                    fn, request, token_value, auth, api_key_value,
                )
        except Exception:
            continue

    # v8.11.1: Expanded fallback env var list for parity with config.py v7.3.0
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
            # v8.11.1 additions:
            _env_str("TFB_BEARER_TOKEN", ""),
            _env_str("BEARER_TOKEN", ""),
            _env_str("TFB_API_KEY", ""),
            _env_str("API_KEY", ""),
            _env_str("X_API_KEY", ""),
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
        request_id = _normalize_request_id(request.headers.get("X-Request-ID", ""))
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
                    extra={
                        "request_id": request_id,
                        "path": str(request.url.path),
                        "status_code": 500,
                    },
                )
                return _StrictJSONResponse(
                    status_code=500,
                    # v8.11.1: also set X-Request-ID header on synthesized 500
                    headers={"X-Request-ID": request_id},
                    content={
                        "status": "error",
                        "error": "internal_server_error",
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
                    extra={
                        "request_id": request_id,
                        "path": str(request.url.path),
                        "status_code": 500,
                    },
                )
                return _StrictJSONResponse(
                    status_code=500,
                    # v8.11.1: also set X-Request-ID header
                    headers={"X-Request-ID": request_id},
                    content={
                        "status": "error",
                        "error": _public_error_text(exc),
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
    advisor_long_underscore = _paths_start_with_any(paths, "/v1/investment_advisor")
    advisor_long_hyphen = _paths_start_with_any(paths, "/v1/investment-advisor")

    # v8.11.1: also check sheet_rows underscore alias
    enriched_primary = _path_present_any(
        paths, "/v1/enriched/sheet-rows", "/v1/enriched/sheet_rows",
    )
    enriched_underscore = _path_present_any(
        paths, "/v1/enriched_quote/sheet-rows", "/v1/enriched_quote/sheet_rows",
    )
    enriched_hyphen = _path_present_any(
        paths, "/v1/enriched-quote/sheet-rows", "/v1/enriched-quote/sheet_rows",
    )

    return {
        "advisor_short": advisor_short,
        "advisor_long_underscore": advisor_long_underscore,
        "advisor_long_hyphen": advisor_long_hyphen,
        "advisor_any": advisor_short or advisor_long_underscore or advisor_long_hyphen,
        "advanced_sheet_rows": _path_present_any(
            paths, "/v1/advanced/sheet-rows", "/v1/advanced/sheet_rows",
        ),
        "advanced_any": _paths_start_with_any(paths, "/v1/advanced"),
        "analysis": _paths_start_with_any(paths, "/v1/analysis"),
        "schema": _paths_start_with_any(paths, "/v1/schema", "/schema"),
        "enriched": _paths_start_with_any(
            paths, "/v1/enriched", "/v1/enriched_quote", "/v1/enriched-quote",
        ),
        "enriched_sheet_rows_primary": enriched_primary,
        "enriched_sheet_rows_underscore": enriched_underscore,
        "enriched_sheet_rows_hyphen": enriched_hyphen,
        "enriched_sheet_rows_any": enriched_primary or enriched_underscore or enriched_hyphen,
        "root_sheet_rows": _path_present_any(paths, "/sheet-rows", "/sheet_rows"),
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
    sorted(set(_CONTROLLED_CANONICAL_OWNER_MAP.keys()))
)


def _route_matches_canonical_target(route_path: str, canonical_path: str) -> bool:
    rp = str(route_path or "").strip()
    cp = str(canonical_path or "").strip()
    if not rp or not cp:
        return False
    if rp == cp:
        return True
    return rp.startswith(cp.rstrip("/") + "/")


def _canonical_route_match_rank(
    route_path: str, canonical_path: str,
) -> Tuple[int, int, int]:
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
                candidates.append((
                    _canonical_route_match_rank(path, canonical_path),
                    idx,
                    route,
                ))
        if not candidates:
            continue
        _, _, best_route = sorted(candidates, key=lambda item: (item[0], item[1]))[0]
        owners[canonical_path] = _endpoint_module_name(best_route)
    return owners


def _canonical_path_owner_mismatches(
    path_owners: Mapping[str, str],
) -> Dict[str, Dict[str, str]]:
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
    paths = {
        str(getattr(r, "path", "") or "")
        for r in (getattr(app, "routes", []) or [])
    }
    presence = _route_family_presence_from_paths(paths)
    required = {
        "advisor_any": presence.get("advisor_any", False),
        "advanced_sheet_rows": presence.get("advanced_sheet_rows", False),
        "analysis": presence.get("analysis", False),
        "schema": presence.get("schema", False),
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


def _normalize_routes_snapshot(
    ret: Any, *, used_strategy: str, app: Optional[FastAPI] = None,
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

    live_route_count, live_signature_count = _live_route_metrics(app)
    openapi_route_count_after_mount = _prefer_live_metric(
        src.get("openapi_route_count_after_mount", 0), live_route_count,
    )
    route_signature_count_after_mount = _prefer_live_metric(
        src.get("route_signature_count_after_mount", 0), live_signature_count,
    )

    path_owners: Dict[str, str] = {}
    path_owner_mismatches: Dict[str, Dict[str, str]] = {}
    route_family_presence = dict(src.get("route_family_presence", {}) or {})
    if app is not None:
        try:
            all_paths = {
                str(getattr(r, "path", "") or "")
                for r in (getattr(app, "routes", []) or [])
            }
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
        "mounted_count": int(src.get("mounted_count", len(mounted)) or len(mounted)),
        "duplicate_skips": duplicate_skips,
        "duplicate_skips_count": int(
            src.get("duplicate_skips_count", len(duplicate_skips))
            or len(duplicate_skips)
        ),
        "partial_duplicate_skips": partial_duplicate_skips,
        "partial_duplicate_skips_count": int(
            src.get("partial_duplicate_skips_count", len(partial_duplicate_skips))
            or len(partial_duplicate_skips)
        ),
        "filtered_out_routes": filtered_out_routes,
        "missing": missing,
        "missing_count": int(src.get("missing_count", len(missing)) or len(missing)),
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
        "advanced_analysis": ("/v1/schema", "/schema", "/sheet-rows"),
        "advanced_sheet_rows": ("/v1/advanced",),
        "analysis_sheet_rows": ("/v1/analysis",),
        "investment_advisor": (
            "/v1/advanced",
            "/v1/investment_advisor",
            "/v1/investment-advisor",
        ),
        "advisor": ("/v1/advisor",),
        "enriched_quote": (
            "/v1/enriched",
            "/v1/enriched_quote",
            "/v1/enriched-quote",
            "/quote",
            "/quotes",
        ),
    }
    return mapping.get(key, tuple())


def _canonical_owner_for_exact_path(path: str) -> str:
    return str(
        _CONTROLLED_CANONICAL_OWNER_MAP.get(str(path or "").strip(), "") or ""
    )


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


def _clone_filtered_router(
    router: APIRouter, *, key: str,
) -> Tuple[APIRouter, List[str], List[str]]:
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
            "response_model": getattr(route, "response_model", None),
            "response_model_include": getattr(
                route, "response_model_include", None,
            ),
            "response_model_exclude": getattr(
                route, "response_model_exclude", None,
            ),
            "response_model_by_alias": bool(
                getattr(route, "response_model_by_alias", True)
            ),
            "response_model_exclude_unset": bool(
                getattr(route, "response_model_exclude_unset", False)
            ),
            "response_model_exclude_defaults": bool(
                getattr(route, "response_model_exclude_defaults", False)
            ),
            "response_model_exclude_none": bool(
                getattr(route, "response_model_exclude_none", False)
            ),
            "status_code": getattr(route, "status_code", None),
            "tags": list(getattr(route, "tags", []) or []),
            "summary": getattr(route, "summary", None),
            "description": getattr(route, "description", None),
            "response_description": getattr(
                route, "response_description", "Successful Response",
            ),
            "deprecated": getattr(route, "deprecated", None),
            "operation_id": getattr(route, "operation_id", None),
            "responses": getattr(route, "responses", None),
            "dependencies": list(getattr(route, "dependencies", []) or []),
            "callbacks": list(getattr(route, "callbacks", []) or []),
            "openapi_extra": getattr(route, "openapi_extra", None),
        }
        unique_id_fn = getattr(route, "generate_unique_id_function", None)
        if callable(unique_id_fn):
            kwargs["generate_unique_id_function"] = unique_id_fn
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
    snap["expected_router_modules"] = [
        module_name for _, module_name in _CONTROLLED_ROUTE_PLAN
    ]

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

        filtered_router, filtered_out, added_paths = _clone_filtered_router(
            router, key=key,
        )
        if filtered_out:
            snap["filtered_out_routes"][module_name] = filtered_out

        routes_to_add = list(_iter_router_api_routes(filtered_router))
        if not routes_to_add:
            snap["no_router"][module_name] = "router filtered to 0 routes"
            # v8.11.1: fail-fast in strict mode on this branch too
            if strict and module_name not in _OPTIONAL_ROUTE_MODULES:
                raise RuntimeError(
                    f"{module_name}: 0 routes after filtering "
                    f"(expected prefixes: {_allowed_prefixes_for_key(key)})"
                )
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

    snap = _normalize_routes_snapshot(
        snap, used_strategy="main.controlled_priority_plan", app=app,
    )
    _invalidate_openapi_cache(app)
    return snap


def _mount_routes_once(app: FastAPI, *, phase: str) -> Dict[str, Any]:
    _ensure_app_state_defaults(app)
    if bool(getattr(app.state, "routes_mounted", False)) and isinstance(
        getattr(app.state, "routes_snapshot", None), dict,
    ):
        snap = _normalize_routes_snapshot(
            getattr(app.state, "routes_snapshot", {}),
            used_strategy="main.controlled_priority_plan",
            app=app,
        )
        app.state.routes_mount_phase = str(
            getattr(app.state, "routes_mount_phase", phase) or phase
        )
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
def _resolve_engine_version(engine_obj: Any, engine_source: str) -> str:
    """v8.11.3: best-effort engine build version for the status payloads.

    Reads __version__ (fallback ENGINE_VERSION) from the already-imported
    engine module via sys.modules -- never imports anything, so it has zero
    side effects and zero cost when the engine is absent. Falls back to the
    engine object's own __version__/version attributes. Pure + fail-safe:
    returns "" on any problem; the health payload can never break here."""
    try:
        if engine_obj is None:
            return ""
        mod_name = str(engine_source or "")
        if not mod_name:
            mod_name = str(getattr(type(engine_obj), "__module__", "") or "")
        if mod_name:
            mod = sys.modules.get(mod_name)
            if mod is not None:
                v = getattr(mod, "__version__", "") or getattr(mod, "ENGINE_VERSION", "")
                if v:
                    return str(v)
        v = getattr(engine_obj, "__version__", "") or getattr(engine_obj, "version", "")
        return str(v or "")
    except Exception:
        return ""


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
            engine_init_error = str(
                getattr(app.state, "engine_init_error", "") or ""
            )
            routes_mounted = bool(getattr(app.state, "routes_mounted", False))
            routes_mount_phase = str(
                getattr(app.state, "routes_mount_phase", "") or ""
            )
            config_source = str(
                getattr(app.state, "config_source", _SETTINGS.CONFIG_SOURCE)
                or _SETTINGS.CONFIG_SOURCE
            )
            startup_warnings = list(
                getattr(app.state, "startup_warnings", []) or []
            )
    except Exception:
        pass

    live_route_count, live_signature_count = _live_route_metrics(app)
    path_owners = dict(snap.get("canonical_path_owners", {}) or {})
    path_owner_mismatches = dict(
        snap.get("canonical_path_owner_mismatches", {}) or {}
    )
    route_family_presence = dict(snap.get("route_family_presence", {}) or {})
    missing_required_keys = list(snap.get("missing_required_keys", []) or [])

    if app is not None:
        try:
            path_owners = _canonical_path_owners_from_routes(app)
            path_owner_mismatches = _canonical_path_owner_mismatches(path_owners)
            all_paths = {
                str(getattr(r, "path", "") or "")
                for r in (getattr(app, "routes", []) or [])
            }
            route_family_presence = _route_family_presence_from_paths(all_paths)
            missing_required_keys = _verify_required_route_families(app)
        except Exception:
            pass

    return {
        "service": _SETTINGS.APP_NAME,
        "app_version": _SETTINGS.APP_VERSION,
        "entry_version": APP_ENTRY_VERSION,
        "service_version": SERVICE_VERSION,          # v8.11.1: cross-module alias
        "env": _SETTINGS.APP_ENV,
        "timestamp_utc": datetime.now(timezone.utc).isoformat(),
        "python": sys.version.split()[0],
        "config_source": config_source,
        "routes_mounted": routes_mounted,
        "routes_mount_phase": routes_mount_phase,
        "routes_mounted_count": int(
            snap.get("mounted_count", len(snap.get("mounted", []) or [])) or 0
        ),
        "routes_duplicate_skips_count": int(
            snap.get(
                "duplicate_skips_count",
                len(snap.get("duplicate_skips", []) or []),
            ) or 0
        ),
        "routes_partial_duplicate_skips_count": int(
            snap.get(
                "partial_duplicate_skips_count",
                len(snap.get("partial_duplicate_skips", []) or []),
            ) or 0
        ),
        "routes_failed_count": int(
            snap.get(
                "failed_count",
                len(snap.get("effective_failed_modules", []) or []),
            ) or 0
        ),
        "routes_strategy": str(snap.get("strategy", "") or ""),
        "route_signature_count": (
            live_signature_count
            if live_signature_count > 0
            else int(snap.get("route_signature_count_after_mount", 0) or 0)
        ),
        "route_count_live": live_route_count,
        "missing_required_keys": missing_required_keys,
        "route_family_presence": route_family_presence,
        "effective_failed_modules": list(
            snap.get("effective_failed_modules", []) or []
        ),
        "canonical_path_owners": path_owners,
        "canonical_path_owner_mismatches": path_owner_mismatches,
        "engine_present": engine_obj is not None,
        "engine_ready": engine_obj is not None and not engine_init_error,
        "engine_source": engine_source,
        "engine_version": _resolve_engine_version(engine_obj, engine_source),
        "engine_init_error": engine_init_error,
        "startup_warnings": startup_warnings,
    }


# =============================================================================
# Built-in routes
# =============================================================================
_PUBLIC_STATUS_META_KEYS: Set[str] = {
    "service",
    "app_version",
    "entry_version",
    "service_version",
    "env",
    "timestamp_utc",
    "python",
    "routes_mounted",
    "routes_failed_count",
    "engine_present",
    "engine_ready",
    "engine_version",
}


def _status_meta_for_request(request: Request, app: FastAPI) -> Dict[str, Any]:
    meta = _runtime_meta(app)
    require_auth = bool(getattr(_SETTINGS, "STATUS_DETAILS_REQUIRE_AUTH", True))
    should_reduce = (
        require_auth
        and _is_production_env(getattr(_SETTINGS, "APP_ENV", "production"))
        and bool(getattr(_SETTINGS, "REQUIRE_AUTH", True))
        and not bool(getattr(_SETTINGS, "OPEN_MODE", False))
        and not _auth_ok(request)
    )
    if not should_reduce:
        return meta
    return {k: v for k, v in meta.items() if k in _PUBLIC_STATUS_META_KEYS}


def _readiness_evaluation(app: FastAPI) -> Tuple[bool, List[str]]:
    meta = _runtime_meta(app)
    reasons: List[str] = []

    if bool(getattr(_SETTINGS, "READY_REQUIRE_ROUTES", True)):
        if not bool(meta.get("routes_mounted")):
            reasons.append("routes_not_mounted")
        if int(meta.get("routes_failed_count", 0) or 0) > 0:
            reasons.append("route_module_failure")
        if list(meta.get("missing_required_keys", []) or []):
            reasons.append("required_route_family_missing")
        if dict(meta.get("canonical_path_owner_mismatches", {}) or {}):
            reasons.append("canonical_route_owner_mismatch")

    require_engine = bool(getattr(_SETTINGS, "READY_REQUIRE_ENGINE", True))
    engine_expected = bool(getattr(_SETTINGS, "INIT_ENGINE_ON_BOOT", True))
    if require_engine and engine_expected:
        if not bool(meta.get("engine_ready")):
            reasons.append("engine_not_ready")

    if (
        bool(getattr(_SETTINGS, "READY_REQUIRE_AUTH", True))
        and _is_production_env(getattr(_SETTINGS, "APP_ENV", "production"))
        and not bool(getattr(_SETTINGS, "OPEN_MODE", False))
        and not bool(getattr(_SETTINGS, "REQUIRE_AUTH", True))
    ):
        reasons.append("auth_not_enforced")

    return (not reasons), reasons


def _install_builtin_routes(app: FastAPI) -> None:
    def _status_payload(
        request: Request,
        label: str,
        *,
        readiness: Optional[Tuple[bool, List[str]]] = None,
    ) -> Dict[str, Any]:
        payload = {"status": label, **_status_meta_for_request(request, app)}
        if readiness is not None:
            ready, reasons = readiness
            payload["ready"] = ready
            payload["readiness_reasons"] = reasons
        return payload

    def _add_status_route(
        path: str,
        mode: str,
        *,
        include_in_schema: bool = False,
    ) -> None:
        @app.api_route(
            path, methods=["GET", "HEAD"], include_in_schema=include_in_schema,
        )
        async def _status_route(request: Request, _mode: str = mode):
            if _mode == "ready":
                ready, reasons = _readiness_evaluation(app)
                # v8.12.1 MASTER GATE. In v8.11.3 /readyz returned HTTP 200
                # unconditionally; it had never emitted any other status in its
                # life, and Render's Health Check Path may point at it. v8.12.0
                # made it 503 on any reason — including route_module_failure,
                # which fires by design because _OPTIONAL_ROUTE_MODULES exists
                # so that four modules ARE permitted to fail. That is a failed
                # deploy or a restart loop on first boot.
                # Default OFF: status stays 200 and the label stays "ready",
                # exactly as v8.11.3. The evaluation still RUNS and its result
                # is reported additively below, so the reasons can be read and
                # emptied before anyone arms this.
                strict = _env_bool("TFB_READYZ_STRICT", False)
                code = 200 if (ready or not strict) else 503
                label = "ready" if (ready or not strict) else "not_ready"
                if request.method == "HEAD":
                    return Response(status_code=code)
                return _StrictJSONResponse(
                    status_code=code,
                    content=_status_payload(
                        request, label, readiness=(ready, reasons),
                    ),
                )

            if _mode == "health":
                ready, reasons = _readiness_evaluation(app)
                # v8.12.1: same reasoning. The Apps Script cockpit parses this
                # label and is not in version control, so "degraded" cannot be
                # introduced blind. Default OFF -> always "healthy", as v8.11.3.
                h_strict = _env_bool("TFB_HEALTH_STRICT", False)
                h_label = "healthy" if (ready or not h_strict) else "degraded"
                if request.method == "HEAD":
                    return Response(status_code=200)
                return _status_payload(
                    request, h_label, readiness=(ready, reasons),
                )

            if request.method == "HEAD":
                return Response(status_code=200)
            return _status_payload(request, "live")

    @app.get("/meta", include_in_schema=False, tags=["meta"])
    async def meta(request: Request):
        return _status_payload(request, "ok")

    @app.get("/v1/meta", include_in_schema=False)
    async def meta_v1(request: Request):
        return _status_payload(request, "ok")

    @app.get("/ping", include_in_schema=False)
    async def ping(request: Request):
        return {"pong": True, **_status_meta_for_request(request, app)}

    @app.get("/v1/ping", include_in_schema=False)
    async def ping_v1(request: Request):
        return {"pong": True, **_status_meta_for_request(request, app)}

    @app.api_route("/", methods=["GET", "HEAD"], include_in_schema=False)
    async def root(request: Request):
        if request.method == "HEAD":
            return Response(status_code=200)
        return _status_payload(request, "ok")

    _add_status_route("/readyz", "ready")
    _add_status_route("/livez", "live")
    _add_status_route("/health", "health")
    _add_status_route("/healthz", "health")
    _add_status_route("/v1/readyz", "ready")
    _add_status_route("/v1/livez", "live")
    _add_status_route("/v1/health", "health")
    _add_status_route("/v1/healthz", "health")


# =============================================================================
# OpenAPI
# =============================================================================
def _install_custom_openapi(app: FastAPI) -> None:
    """v8.11.1 preserves v8.11.0's simpler approach: override app.openapi with
    a signature-count-keyed cache, and let FastAPI handle the HTTP endpoints
    via docs_url/redoc_url/openapi_url. (v8.12.x added custom route handlers
    for the JSON endpoint with Cache-Control: no-store; v8.11.1 sticks with
    the simpler native path.)
    """
    def custom_openapi() -> Dict[str, Any]:
        current_signature_count = _route_signature_count(
            app, include_builtin=True,
        )
        cached_signature_count = int(
            getattr(app.state, "_openapi_route_signature_count", -1) or -1
        )
        cached_schema = getattr(app, "openapi_schema", None)
        if (
            cached_schema is not None
            and cached_signature_count == current_signature_count
        ):
            return cached_schema  # type: ignore[return-value]

        schema = get_openapi(
            title=_SETTINGS.APP_NAME,
            version=str(_SETTINGS.APP_VERSION),
            routes=app.routes,
            description="Tadawul Fast Bridge API",
        )
        app.openapi_schema = schema
        app.state._openapi_route_signature_count = current_signature_count
        return schema

    app.openapi = custom_openapi  # type: ignore[assignment]


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


async def _try_init_engine_module(
    app: FastAPI, module_name: str,
) -> Optional[str]:
    mod = importlib.import_module(module_name)
    init_fn = getattr(mod, "get_engine", None) or getattr(mod, "init_engine", None)
    if not callable(init_fn):
        return None

    timeout_sec = _coerce_positive_timeout(
        float(_SETTINGS.ENGINE_INIT_TIMEOUT_SEC), 12.0,
    )
    try:
        engine = await asyncio.wait_for(
            _call_zeroarg_maybe_async(init_fn), timeout=timeout_sec,
        )
    except asyncio.TimeoutError as e:
        raise TimeoutError(
            f"{module_name} init exceeded {timeout_sec:.1f}s"
        ) from e

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

    err = (
        " | ".join([f"{name} failed: {msg}" for name, msg in attempts])
        or "engine init failed"
    )
    app.state.engine_init_error = err
    return err


async def _maybe_close_engine() -> None:
    for mod_name in ("core.data_engine_v2", "core.data_engine"):
        try:
            mod = importlib.import_module(mod_name)
            fn = getattr(mod, "close_engine", None) or getattr(
                mod, "shutdown_engine", None,
            )
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
                "Routes verified at startup: mounted=%s duplicate_skips=%s "
                "partial_duplicate_skips=%s missing=%s failed=%s strategy=%s "
                "route_signatures=%s",
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
                warning = (
                    f"missing_required_route_families: "
                    f"{', '.join(missing_required)}"
                )
                logger.warning(warning)
                _append_startup_warning(app, warning)

            owner_mismatches = dict(
                snap.get("canonical_path_owner_mismatches", {}) or {}
            )
            if owner_mismatches:
                warning = (
                    f"canonical_path_owner_mismatches: "
                    f"{json.dumps(owner_mismatches, ensure_ascii=False)}"
                )
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

        logger.info(
            "Startup complete: %s",
            json.dumps(_runtime_meta(app), ensure_ascii=False),
        )
        try:
            yield
        finally:
            try:
                await _maybe_close_google_sheets_service()
            except Exception:
                logger.warning(
                    "Google Sheets service shutdown failed", exc_info=True,
                )
            try:
                await _maybe_close_engine()
            except Exception:
                logger.warning("Engine shutdown failed", exc_info=True)

    app = FastAPI(
        title=_SETTINGS.APP_NAME,
        version=str(_SETTINGS.APP_VERSION),
        debug=bool(_SETTINGS.DEBUG),
        docs_url=docs_url,
        redoc_url=redoc_url,
        openapi_url=openapi_url,
        default_response_class=_StrictJSONResponse,
        lifespan=lifespan,
    )
    _ensure_app_state_defaults(app)

    if _is_production_env(_SETTINGS.APP_ENV):
        if bool(_SETTINGS.ENABLE_SWAGGER) or bool(_SETTINGS.ENABLE_REDOC):
            _append_startup_warning(app, "production_api_docs_enabled")
        if (
            not bool(_SETTINGS.OPEN_MODE)
            and not bool(_SETTINGS.REQUIRE_AUTH)
        ):
            _append_startup_warning(app, "production_auth_not_enforced")
    if bool(_SETTINGS.ALLOW_QUERY_TOKEN):
        _append_startup_warning(app, "query_token_transport_enabled")

    # Starlette middleware is LIFO (last added = outermost). Desired order:
    # RequestID -> CORS -> NoResponseGuard -> GZip -> route. This preserves
    # request IDs on every response while still allowing CORS to decorate
    # responses synthesized by the guard.
    app.add_middleware(GZipMiddleware, minimum_size=1024)
    app.add_middleware(NoResponseGuardMiddleware)

    if bool(_SETTINGS.ENABLE_CORS_ALL_ORIGINS):
        if bool(_SETTINGS.CORS_ALLOW_CREDENTIALS):
            _append_startup_warning(
                app,
                "cors_credentials_forced_off_for_wildcard_origin",
            )
        app.add_middleware(
            CORSMiddleware,
            allow_origins=["*"],
            allow_credentials=False,
            allow_methods=["*"],
            allow_headers=["*"],
        )
    else:
        origins = _parse_csv(_SETTINGS.CORS_ORIGINS)
        if origins:
            app.add_middleware(
                CORSMiddleware,
                allow_origins=origins,
                allow_credentials=bool(_SETTINGS.CORS_ALLOW_CREDENTIALS),
                allow_methods=["*"],
                allow_headers=["*"],
            )

    app.add_middleware(RequestIDMiddleware)

    @app.exception_handler(Exception)
    async def unhandled_exception_handler(request: Request, exc: Exception):
        request_id = _request_id_from_request(request)
        logger.error(
            "Unhandled exception",
            exc_info=True,
            extra={
                "request_id": request_id,
                "path": str(request.url.path),
                "status_code": 500,
            },
        )
        return _StrictJSONResponse(
            status_code=500,
            # v8.11.1: set X-Request-ID header on error responses too
            headers={"X-Request-ID": request_id},
            content={
                "status": "error",
                "error": _public_error_text(exc),
                "path": str(request.url.path),
                "request_id": request_id,
                "ts_utc": datetime.now(timezone.utc).isoformat(),
            },
        )

    _install_builtin_routes(app)
    _install_custom_openapi(app)

    if bool(_SETTINGS.PRESTART_MOUNT_ROUTES):
        try:
            snap = _mount_routes_once(app, phase="prestart")
            logger.info(
                "Routes mounted at app creation: mounted=%s duplicate_skips=%s "
                "partial_duplicate_skips=%s missing=%s failed=%s strategy=%s "
                "route_signatures=%s",
                snap.get("mounted_count", 0),
                snap.get("duplicate_skips_count", 0),
                snap.get("partial_duplicate_skips_count", 0),
                snap.get("missing_count", 0),
                snap.get("failed_count", 0),
                snap.get("strategy", ""),
                snap.get("route_signature_count_after_mount", 0),
            )
        except Exception as e:
            logger.error(
                "Prestart route mounting failed: %s", e, exc_info=True,
            )
            _append_startup_warning(
                app, f"prestart_route_mount_failed: {_err_to_str(e)}",
            )
            if _env_bool("ROUTES_STRICT_IMPORT", False):
                raise
    else:
        _append_startup_warning(app, "prestart_route_mount_disabled")
        logger.info(
            "Prestart route mounting disabled by PRESTART_MOUNT_ROUTES"
        )

    @app.get("/_debug/routes", include_in_schema=False)
    async def debug_routes(request: Request):
        if (
            _SETTINGS.APP_ENV == "production"
            and bool(_SETTINGS.REQUIRE_AUTH)
            and not _auth_ok(request)
        ):
            return _StrictJSONResponse(
                status_code=401,
                content={"status": "error", "error": "unauthorized"},
            )

        route_paths = sorted({
            str(getattr(r, "path", "") or "")
            for r in (getattr(request.app, "routes", []) or [])
        })
        route_sigs = sorted([
            f"{path} [{method}]"
            for path, method in _app_route_signature_set(
                request.app, include_builtin=True,
            )
        ])
        path_owners = _canonical_path_owners_from_routes(request.app)
        owner_mismatches = _canonical_path_owner_mismatches(path_owners)

        enriched_alias_presence = {
            "/v1/enriched/sheet-rows": "/v1/enriched/sheet-rows" in route_paths,
            "/v1/enriched_quote/sheet-rows": "/v1/enriched_quote/sheet-rows" in route_paths,
            "/v1/enriched-quote/sheet-rows": "/v1/enriched-quote/sheet-rows" in route_paths,
        }

        return {
            "status": "ok",
            "routes_snapshot": getattr(
                request.app.state, "routes_snapshot", {}
            ),
            "app_route_count": len(getattr(request.app, "routes", []) or []),
            "app_route_signature_count": _route_signature_count(
                request.app, include_builtin=True,
            ),
            "route_paths": route_paths,
            "route_signatures": route_sigs,
            "canonical_path_owners": path_owners,
            "canonical_path_owner_mismatches": owner_mismatches,
            "enriched_alias_presence": enriched_alias_presence,
            **_runtime_meta(app),
        }

    return app


app = create_app()
application = app


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(
        "main:app",
        host="0.0.0.0",
        port=int(os.getenv("PORT", "10000")),
        reload=False,
    )
