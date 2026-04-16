#!/usr/bin/env python3
# config.py
"""
===============================================================================
TFB Main Config — v7.2.0
===============================================================================
IMPORT-SAFE • CACHE-SAFE • AUTH-FLEXIBLE • OPEN-MODE SAFE • ENV-FIRST
RENDER-SAFE • ROUTER-FRIENDLY • BACKWARD-COMPATIBLE • ZERO NETWORK I/O

Purpose
-------
Main application configuration layer for Tadawul Fast Bridge (TFB).

This file lives at the project root as:
    config.py

It provides:
- cached settings
- flexible auth checks
- open-mode handling
- runtime metadata helpers
- backward-compatible helpers commonly expected across routers/services

Public helpers
--------------
- get_settings()
- get_settings_cached()
- reload_settings()
- auth_ok(...)
- is_open_mode()
- is_auth_required()
- settings_public_dict()
- build_runtime_meta()

v7.2.0 Changes
--------------
- FIX: auth_ok() now respects allow_query_token. Query/body-style token aliases
       are accepted only when that flag is enabled, instead of always being
       considered as valid candidates.
- FIX: token comparison now uses constant-time matching via hmac.compare_digest
       instead of plain membership checks.
- FIX: auth_ok() returns False (not True) when require_auth=True but no tokens
       are configured — previously this was an unsafe open fallback.
- ENH: supports additional common auth transports:
       X-API-Key / Api-Key headers and request.query_params when allowed.
- ENH: settings public/runtime metadata now expose settings_cache_ttl_sec.
- SAFE: preserves env naming, open-mode behavior, TTL cache behavior, and
       zero-network import safety.
"""

from __future__ import annotations

import hmac
import os
import socket
import threading
import time
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from typing import Any, Dict, Iterable, List, Mapping, Optional, Set

CONFIG_VERSION = "7.2.0"
__version__ = CONFIG_VERSION
DEFAULT_TIMEZONE = "Asia/Riyadh"
_DEFAULT_SETTINGS_CACHE_TTL = 30


# =============================================================================
# Low-level env helpers
# =============================================================================
def _strip(v: Any) -> str:
    if v is None:
        return ""
    try:
        s = str(v).strip()
        return s if s and s.lower() != "none" else ""
    except Exception:
        return ""


def _env_first(*names: str, default: str = "") -> str:
    for name in names:
        raw = os.getenv(name)
        if raw is not None:
            text = _strip(raw)
            if text != "":
                return text
    return default


def _env_bool(*names: str, default: bool = False) -> bool:
    raw = _env_first(*names, default="")
    if not raw:
        return bool(default)
    s = raw.lower()
    if s in {"1", "true", "yes", "y", "on"}:
        return True
    if s in {"0", "false", "no", "n", "off"}:
        return False
    return bool(default)


def _env_int(*names: str, default: int = 0) -> int:
    raw = _env_first(*names, default="")
    if not raw:
        return int(default)
    try:
        return int(float(raw))
    except Exception:
        return int(default)


def _env_float(*names: str, default: float = 0.0) -> float:
    raw = _env_first(*names, default="")
    if not raw:
        return float(default)
    try:
        return float(raw)
    except Exception:
        return float(default)


def _split_csv(raw: Any) -> List[str]:
    text = _strip(raw)
    if not text:
        return []
    normalized = (
        text.replace("\n", ",")
        .replace("\r", ",")
        .replace(";", ",")
        .replace("|", ",")
    )
    out: List[str] = []
    seen: Set[str] = set()
    for part in normalized.split(","):
        item = _strip(part)
        if item and item not in seen:
            seen.add(item)
            out.append(item)
    return out


def _env_csv(*names: str) -> List[str]:
    items: List[str] = []
    seen: Set[str] = set()
    for name in names:
        for item in _split_csv(os.getenv(name, "")):
            if item not in seen:
                seen.add(item)
                items.append(item)
    return items


def _mask_secret(value: str, keep: int = 4) -> str:
    text = _strip(value)
    if not text:
        return ""
    if len(text) <= keep:
        return "*" * len(text)
    return f"{'*' * max(4, len(text) - keep)}{text[-keep:]}"


def _safe_compare_token(candidate: str, allowed: str) -> bool:
    """Constant-time token comparison using hmac.compare_digest."""
    c = _strip(candidate)
    a = _strip(allowed)
    if not c or not a:
        return False
    try:
        return hmac.compare_digest(c, a)
    except Exception:
        return c == a


def _allow_query_transport(settings: "TFBSettings") -> bool:
    return bool(settings.allow_query_token)


def _coerce_headers(headers: Any) -> Dict[str, str]:
    if headers is None:
        return {}
    if isinstance(headers, Mapping):
        return {str(k): _strip(v) for k, v in headers.items()}
    out: Dict[str, str] = {}
    try:
        items = getattr(headers, "items", None)
        if callable(items):
            for k, v in items():
                out[str(k)] = _strip(v)
            return out
    except Exception:
        pass
    return out


def _coerce_mapping(obj: Any) -> Dict[str, Any]:
    if obj is None:
        return {}
    if isinstance(obj, Mapping):
        try:
            return {str(k): v for k, v in obj.items()}
        except Exception:
            return {}
    try:
        items = getattr(obj, "items", None)
        if callable(items):
            return {str(k): v for k, v in items()}
    except Exception:
        pass
    return {}


def _extract_bearer_token(authorization: Any) -> str:
    auth = _strip(authorization)
    if not auth:
        return ""
    if auth.lower().startswith("bearer "):
        return _strip(auth.split(" ", 1)[1])
    return auth


def _unique_keep_order(values: Iterable[str]) -> List[str]:
    out: List[str] = []
    seen: Set[str] = set()
    for value in values:
        item = _strip(value)
        if item and item not in seen:
            seen.add(item)
            out.append(item)
    return out


# =============================================================================
# Settings model
# =============================================================================
@dataclass
class TFBSettings:
    app_name: str = "Tadawul Fast Bridge"
    app_version: str = "unknown"
    entry_version: str = ""
    env: str = "production"
    debug: bool = False
    log_level: str = "INFO"

    timezone: str = DEFAULT_TIMEZONE
    host: str = "0.0.0.0"
    port: int = 10000

    open_mode: bool = False
    require_auth: bool = False
    allow_query_token: bool = False

    app_token: str = ""
    app_tokens: List[str] = field(default_factory=list)

    bearer_token: str = ""
    bearer_tokens: List[str] = field(default_factory=list)

    auth_token: str = ""
    auth_tokens: List[str] = field(default_factory=list)

    cors_origins: List[str] = field(default_factory=list)

    default_page: str = "Market_Leaders"
    max_limit: int = 5000
    route_rehydrate_concurrency: int = 6

    request_timeout_sec: float = 45.0
    quote_timeout_sec: float = 45.0
    engine_call_timeout_sec: float = 45.0
    special_builder_timeout_sec: float = 45.0
    core_builder_timeout_sec: float = 45.0

    eodhd_api_key: str = ""
    finnhub_api_key: str = ""
    openai_api_key: str = ""
    gemini_api_key: str = ""

    render_service_name: str = ""
    render_external_url: str = ""

    @classmethod
    def from_env(cls) -> "TFBSettings":
        app_tokens = _unique_keep_order(
            _env_csv(
                "TFB_APP_TOKENS", "APP_TOKENS", "AUTH_TOKENS",
                "API_TOKENS", "TOKENS", "X_APP_TOKENS", "BEARER_TOKENS",
            )
            + [
                _env_first("TFB_APP_TOKEN", "APP_TOKEN", "X_APP_TOKEN", "X_API_KEY", "API_KEY", default=""),
                _env_first("TFB_AUTH_TOKEN", "AUTH_TOKEN", "API_TOKEN", "TOKEN", default=""),
                _env_first("TFB_BEARER_TOKEN", "BEARER_TOKEN", default=""),
            ]
        )

        bearer_tokens = _unique_keep_order(
            _env_csv("TFB_BEARER_TOKENS", "BEARER_TOKENS")
            + [_env_first("TFB_BEARER_TOKEN", "BEARER_TOKEN", default="")]
        )

        auth_tokens = _unique_keep_order(
            _env_csv("TFB_AUTH_TOKENS", "AUTH_TOKENS", "API_TOKENS", "TOKENS")
            + [
                _env_first("TFB_AUTH_TOKEN", "AUTH_TOKEN", "API_TOKEN", "TOKEN", default=""),
                _env_first("TFB_API_KEY", "API_KEY", "X_API_KEY", default=""),
            ]
        )

        open_mode = _env_bool("TFB_OPEN_MODE", "OPEN_MODE", "APP_OPEN_MODE", default=False)

        explicit_require_auth = _env_first(
            "TFB_REQUIRE_AUTH", "REQUIRE_AUTH", "APP_REQUIRE_AUTH", default="",
        )
        if explicit_require_auth:
            require_auth = _env_bool(
                "TFB_REQUIRE_AUTH", "REQUIRE_AUTH", "APP_REQUIRE_AUTH", default=False,
            )
        else:
            require_auth = (not open_mode) and bool(app_tokens or bearer_tokens or auth_tokens)

        return cls(
            app_name=_env_first("TFB_APP_NAME", "APP_NAME", default="Tadawul Fast Bridge"),
            app_version=_env_first("TFB_APP_VERSION", "APP_VERSION", default="unknown"),
            entry_version=_env_first("TFB_ENTRY_VERSION", "ENTRY_VERSION", default=""),
            env=_env_first("TFB_ENV", "ENV", "APP_ENV", default="production").lower(),
            debug=_env_bool("TFB_DEBUG", "DEBUG", default=False),
            log_level=_env_first("TFB_LOG_LEVEL", "LOG_LEVEL", default="INFO").upper(),
            timezone=_env_first("TFB_TIMEZONE", "TIMEZONE", "TZ", default=DEFAULT_TIMEZONE),
            host=_env_first("TFB_HOST", "HOST", default="0.0.0.0"),
            port=_env_int("PORT", "TFB_PORT", default=10000),
            open_mode=open_mode,
            require_auth=require_auth,
            allow_query_token=_env_bool("TFB_ALLOW_QUERY_TOKEN", "ALLOW_QUERY_TOKEN", default=False),
            app_token=_env_first("TFB_APP_TOKEN", "APP_TOKEN", "X_APP_TOKEN", "X_API_KEY", "API_KEY", default=""),
            app_tokens=app_tokens,
            bearer_token=_env_first("TFB_BEARER_TOKEN", "BEARER_TOKEN", default=""),
            bearer_tokens=bearer_tokens,
            auth_token=_env_first("TFB_AUTH_TOKEN", "AUTH_TOKEN", "API_TOKEN", "TOKEN", "TFB_API_KEY", "API_KEY", default=""),
            auth_tokens=auth_tokens,
            cors_origins=_env_csv("TFB_CORS_ORIGINS", "CORS_ORIGINS"),
            default_page=_env_first("TFB_DEFAULT_PAGE", "DEFAULT_PAGE", default="Market_Leaders"),
            max_limit=max(1, _env_int("TFB_MAX_LIMIT", "MAX_LIMIT", default=5000)),
            route_rehydrate_concurrency=max(
                1, _env_int("TFB_ROUTE_REHYDRATE_CONCURRENCY", default=6),
            ),
            request_timeout_sec=max(0.0, _env_float("TFB_REQUEST_TIMEOUT_SEC", default=45.0)),
            quote_timeout_sec=max(0.0, _env_float("TFB_QUOTE_CALL_TIMEOUT_SEC", default=45.0)),
            engine_call_timeout_sec=max(0.0, _env_float("TFB_ENGINE_CALL_TIMEOUT_SEC", default=45.0)),
            special_builder_timeout_sec=max(0.0, _env_float("TFB_SPECIAL_BUILDER_TIMEOUT_SEC", default=45.0)),
            core_builder_timeout_sec=max(0.0, _env_float("TFB_CORE_BUILDER_TIMEOUT_SEC", default=45.0)),
            eodhd_api_key=_env_first("EODHD_API_KEY", "TFB_EODHD_API_KEY", default=""),
            finnhub_api_key=_env_first("FINNHUB_API_KEY", "TFB_FINNHUB_API_KEY", default=""),
            openai_api_key=_env_first("OPENAI_API_KEY", "TFB_OPENAI_API_KEY", default=""),
            gemini_api_key=_env_first("GEMINI_API_KEY", "GOOGLE_API_KEY", "TFB_GEMINI_API_KEY", default=""),
            render_service_name=_env_first("RENDER_SERVICE_NAME", default=""),
            render_external_url=_env_first("RENDER_EXTERNAL_URL", default=""),
        )

    def __post_init__(self) -> None:
        self.timezone = _strip(self.timezone) or DEFAULT_TIMEZONE
        self.env = (_strip(self.env) or "production").lower()
        self.log_level = (_strip(self.log_level) or "INFO").upper()
        self.host = _strip(self.host) or "0.0.0.0"
        self.default_page = _strip(self.default_page) or "Market_Leaders"
        self.cors_origins = _unique_keep_order(self.cors_origins)

        self.app_tokens = _unique_keep_order(self.app_tokens + [self.app_token])
        self.bearer_tokens = _unique_keep_order(self.bearer_tokens + [self.bearer_token])
        self.auth_tokens = _unique_keep_order(self.auth_tokens + [self.auth_token])

        if self.open_mode:
            self.require_auth = False

    @property
    def allowed_tokens(self) -> List[str]:
        return _unique_keep_order(self.app_tokens + self.bearer_tokens + self.auth_tokens)

    @property
    def auth_enabled(self) -> bool:
        return bool(self.require_auth and not self.open_mode)

    @property
    def has_any_token(self) -> bool:
        return bool(self.allowed_tokens)

    @property
    def settings_cache_ttl_sec(self) -> int:
        return _settings_cache_ttl()

    def public_dict(self) -> Dict[str, Any]:
        return {
            "app_name": self.app_name,
            "app_version": self.app_version,
            "entry_version": self.entry_version,
            "env": self.env,
            "debug": self.debug,
            "log_level": self.log_level,
            "timezone": self.timezone,
            "host": self.host,
            "port": self.port,
            "open_mode": self.open_mode,
            "require_auth": self.require_auth,
            "allow_query_token": self.allow_query_token,
            "auth_enabled": self.auth_enabled,
            "has_any_token": self.has_any_token,
            "settings_cache_ttl_sec": self.settings_cache_ttl_sec,
            "masked_tokens": [_mask_secret(t) for t in self.allowed_tokens],
            "cors_origins": list(self.cors_origins),
            "default_page": self.default_page,
            "max_limit": self.max_limit,
            "route_rehydrate_concurrency": self.route_rehydrate_concurrency,
            "request_timeout_sec": self.request_timeout_sec,
            "quote_timeout_sec": self.quote_timeout_sec,
            "engine_call_timeout_sec": self.engine_call_timeout_sec,
            "special_builder_timeout_sec": self.special_builder_timeout_sec,
            "core_builder_timeout_sec": self.core_builder_timeout_sec,
            "render_service_name": self.render_service_name,
            "render_external_url": self.render_external_url,
            "provider_flags": {
                "eodhd": bool(self.eodhd_api_key),
                "finnhub": bool(self.finnhub_api_key),
                "openai": bool(self.openai_api_key),
                "gemini": bool(self.gemini_api_key),
            },
        }

    def dict(self) -> Dict[str, Any]:
        return asdict(self)

    def model_dump(self) -> Dict[str, Any]:
        return asdict(self)


# =============================================================================
# Thread-safe TTL settings cache
# =============================================================================
_settings_cache_lock = threading.RLock()
_settings_cached: Optional[TFBSettings] = None
_settings_cache_expiry: float = 0.0


def _settings_cache_ttl() -> int:
    try:
        raw = os.getenv("SETTINGS_CACHE_TTL_SEC", "")
        if raw.strip():
            return max(1, int(float(raw.strip())))
    except Exception:
        pass
    return _DEFAULT_SETTINGS_CACHE_TTL


def get_settings() -> TFBSettings:
    return TFBSettings.from_env()


def get_settings_cached() -> TFBSettings:
    global _settings_cached, _settings_cache_expiry
    with _settings_cache_lock:
        now = time.monotonic()
        if _settings_cached is not None and now < _settings_cache_expiry:
            return _settings_cached
        fresh = get_settings()
        _settings_cached = fresh
        _settings_cache_expiry = now + _settings_cache_ttl()
        return fresh


def reload_settings() -> TFBSettings:
    global _settings_cached, _settings_cache_expiry
    with _settings_cache_lock:
        _settings_cached = None
        _settings_cache_expiry = 0.0
    return get_settings_cached()


# =============================================================================
# Auth helpers
# =============================================================================
def _query_token_candidates_from_request(request: Any, allow_query_token: bool) -> List[str]:
    """Extract token candidates from request.query_params — only when allowed."""
    if request is None or not allow_query_token:
        return []
    out: List[str] = []
    try:
        query_params = getattr(request, "query_params", None)
        query_map = _coerce_mapping(query_params)
        for key in ("token", "app_token", "auth_token", "api_token", "query_token", "x_app_token", "api_key"):
            if key in query_map:
                out.append(_strip(query_map.get(key)))
    except Exception:
        pass
    return out


def _token_candidates(
    *,
    settings: Optional[TFBSettings] = None,
    token: Any = None,
    authorization: Any = None,
    headers: Any = None,
    request: Any = None,
    x_app_token: Any = None,
    bearer_token: Any = None,
    **kwargs: Any,
) -> List[str]:
    s = settings or get_settings_cached()
    allow_query_token = _allow_query_transport(s)
    candidates: List[str] = []

    if token is not None:
        candidates.append(_strip(token))
    if x_app_token is not None:
        candidates.append(_strip(x_app_token))
    if bearer_token is not None:
        candidates.append(_strip(bearer_token))
    if authorization is not None:
        candidates.append(_extract_bearer_token(authorization))

    hdrs = _coerce_headers(headers)
    if hdrs:
        candidates.extend([
            _strip(hdrs.get("X-APP-TOKEN") or hdrs.get("x-app-token") or hdrs.get("X-App-Token") or ""),
            _extract_bearer_token(hdrs.get("Authorization") or hdrs.get("authorization") or ""),
            _strip(hdrs.get("X-API-Key") or hdrs.get("x-api-key") or hdrs.get("Api-Key") or hdrs.get("api-key") or ""),
        ])

    if request is not None:
        try:
            request_headers = _coerce_headers(getattr(request, "headers", None))
            candidates.extend([
                _strip(request_headers.get("X-APP-TOKEN") or request_headers.get("x-app-token") or request_headers.get("X-App-Token") or ""),
                _extract_bearer_token(request_headers.get("Authorization") or request_headers.get("authorization") or ""),
                _strip(request_headers.get("X-API-Key") or request_headers.get("x-api-key") or request_headers.get("Api-Key") or request_headers.get("api-key") or ""),
            ])
        except Exception:
            pass
        candidates.extend(_query_token_candidates_from_request(request, allow_query_token))

    # kwargs-based query token transport (only when explicitly allowed)
    if allow_query_token:
        for key in ("app_token", "auth_token", "api_token", "query_token", "api_key"):
            if key in kwargs:
                candidates.append(_strip(kwargs.get(key) or ""))

    return _unique_keep_order(candidates)


def auth_ok(
    token: Any = None,
    authorization: Any = None,
    headers: Any = None,
    request: Any = None,
    x_app_token: Any = None,
    bearer_token: Any = None,
    **kwargs: Any,
) -> bool:
    s = get_settings_cached()

    if s.open_mode:
        return True

    if not s.require_auth:
        return True

    allowed = [t for t in s.allowed_tokens if _strip(t)]
    if not allowed:
        # FIX v7.2.0: require_auth=True but no tokens configured → deny
        # (v7.0.0 returned True here, which was an unsafe open fallback)
        return False

    candidates = _token_candidates(
        settings=s,
        token=token,
        authorization=authorization,
        headers=headers,
        request=request,
        x_app_token=x_app_token,
        bearer_token=bearer_token,
        **kwargs,
    )

    for candidate in candidates:
        if not candidate:
            continue
        for allowed_token in allowed:
            # FIX v7.2.0: constant-time comparison (was plain set membership)
            if _safe_compare_token(candidate, allowed_token):
                return True
    return False


def is_open_mode() -> bool:
    return bool(get_settings_cached().open_mode)


def is_auth_required() -> bool:
    s = get_settings_cached()
    return bool(s.require_auth and not s.open_mode)


# =============================================================================
# Runtime / metadata helpers
# =============================================================================
def now_utc_iso() -> str:
    try:
        return datetime.now(timezone.utc).isoformat()
    except Exception:
        return ""


def now_riyadh_iso() -> str:
    try:
        from zoneinfo import ZoneInfo
        try:
            tz_name = get_settings_cached().timezone or DEFAULT_TIMEZONE
        except Exception:
            tz_name = DEFAULT_TIMEZONE
        try:
            tz = ZoneInfo(tz_name)
        except Exception:
            tz = ZoneInfo(DEFAULT_TIMEZONE)
        return datetime.now(tz).isoformat()
    except Exception:
        return now_utc_iso()


def local_hostname() -> str:
    try:
        return socket.gethostname()
    except Exception:
        return "unknown-host"


def settings_public_dict() -> Dict[str, Any]:
    try:
        return get_settings_cached().public_dict()
    except Exception:
        return {}


def build_runtime_meta() -> Dict[str, Any]:
    try:
        s = get_settings_cached()
        return {
            "status": "ok",
            "service": s.app_name,
            "app_version": s.app_version,
            "entry_version": s.entry_version,
            "env": s.env,
            "timestamp_utc": now_utc_iso(),
            "timestamp_riyadh": now_riyadh_iso(),
            "timezone": s.timezone,
            "host": s.host,
            "port": s.port,
            "hostname": local_hostname(),
            "open_mode": s.open_mode,
            "require_auth": s.require_auth,
            "allow_query_token": s.allow_query_token,
            "auth_enabled": s.auth_enabled,
            "default_page": s.default_page,
            "max_limit": s.max_limit,
            "settings_cache_ttl_sec": s.settings_cache_ttl_sec,
            "config_version": CONFIG_VERSION,
        }
    except Exception as e:
        return {
            "status": "degraded",
            "error": f"{type(e).__name__}: {e}",
            "timestamp_utc": now_utc_iso(),
            "config_version": CONFIG_VERSION,
        }


__all__ = [
    "__version__",
    "CONFIG_VERSION",
    "DEFAULT_TIMEZONE",
    "TFBSettings",
    "get_settings",
    "get_settings_cached",
    "reload_settings",
    "auth_ok",
    "is_open_mode",
    "is_auth_required",
    "settings_public_dict",
    "build_runtime_meta",
    "now_utc_iso",
    "now_riyadh_iso",
    "local_hostname",
]
