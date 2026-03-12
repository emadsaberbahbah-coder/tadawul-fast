
#!/usr/bin/env python3
# config.py
"""
===============================================================================
TFB Main Config — v7.0.0
===============================================================================
IMPORT-SAFE • CACHE-SAFE • AUTH-FLEXIBLE • OPEN-MODE SAFE • ENV-FIRST
RENDER-SAFE • ROUTER-FRIENDLY • BACKWARD-COMPATIBLE • ZERO NETWORK I/O

Purpose
-------
Main application configuration layer for Tadawul Fast Bridge (TFB).

This file is intended to live at the project root as:
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

Environment philosophy
----------------------
This module accepts multiple env naming styles to stay compatible with old and
new deployments.

Examples
--------
- TFB_OPEN_MODE=true
- TFB_REQUIRE_AUTH=true
- TFB_APP_TOKEN=secret
- TFB_APP_TOKENS=token1,token2
- APP_TOKEN=secret
- APP_TOKENS=token1,token2
- X_APP_TOKEN=secret
- AUTH_TOKEN=secret
- API_TOKEN=secret
- BEARER_TOKEN=secret
- TOKEN=secret
"""

from __future__ import annotations

import os
import socket
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from functools import lru_cache
from typing import Any, Dict, Iterable, List, Mapping, Set
from zoneinfo import ZoneInfo

CONFIG_VERSION = "7.0.0"
DEFAULT_TIMEZONE = "Asia/Riyadh"


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
    seen = set()
    for part in normalized.split(","):
        item = _strip(part)
        if item and item not in seen:
            seen.add(item)
            out.append(item)
    return out


def _env_csv(*names: str) -> List[str]:
    items: List[str] = []
    seen = set()
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


def _extract_bearer_token(authorization: Any) -> str:
    auth = _strip(authorization)
    if not auth:
        return ""
    if auth.lower().startswith("bearer "):
        return _strip(auth.split(" ", 1)[1])
    return auth


def _unique_keep_order(values: Iterable[str]) -> List[str]:
    out: List[str] = []
    seen = set()
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
                "TFB_APP_TOKENS",
                "APP_TOKENS",
                "AUTH_TOKENS",
                "API_TOKENS",
                "TOKENS",
                "X_APP_TOKENS",
                "BEARER_TOKENS",
            )
            + [
                _env_first("TFB_APP_TOKEN", "APP_TOKEN", "X_APP_TOKEN", default=""),
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
            + [_env_first("TFB_AUTH_TOKEN", "AUTH_TOKEN", "API_TOKEN", "TOKEN", default="")]
        )

        open_mode = _env_bool("TFB_OPEN_MODE", "OPEN_MODE", "APP_OPEN_MODE", default=False)

        explicit_require_auth = _env_first(
            "TFB_REQUIRE_AUTH",
            "REQUIRE_AUTH",
            "APP_REQUIRE_AUTH",
            default="",
        )
        if explicit_require_auth:
            require_auth = _env_bool(
                "TFB_REQUIRE_AUTH",
                "REQUIRE_AUTH",
                "APP_REQUIRE_AUTH",
                default=False,
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
            timezone=_env_first("TFB_TIMEZONE", "TIMEZONE", default=DEFAULT_TIMEZONE),
            host=_env_first("TFB_HOST", "HOST", default="0.0.0.0"),
            port=_env_int("PORT", "TFB_PORT", default=10000),
            open_mode=open_mode,
            require_auth=require_auth,
            allow_query_token=_env_bool("TFB_ALLOW_QUERY_TOKEN", "ALLOW_QUERY_TOKEN", default=False),
            app_token=_env_first("TFB_APP_TOKEN", "APP_TOKEN", "X_APP_TOKEN", default=""),
            app_tokens=app_tokens,
            bearer_token=_env_first("TFB_BEARER_TOKEN", "BEARER_TOKEN", default=""),
            bearer_tokens=bearer_tokens,
            auth_token=_env_first("TFB_AUTH_TOKEN", "AUTH_TOKEN", "API_TOKEN", "TOKEN", default=""),
            auth_tokens=auth_tokens,
            cors_origins=_env_csv("TFB_CORS_ORIGINS", "CORS_ORIGINS"),
            default_page=_env_first("TFB_DEFAULT_PAGE", "DEFAULT_PAGE", default="Market_Leaders"),
            max_limit=max(1, _env_int("TFB_MAX_LIMIT", "MAX_LIMIT", default=5000)),
            route_rehydrate_concurrency=max(
                1,
                _env_int("TFB_ROUTE_REHYDRATE_CONCURRENCY", default=6),
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
# Cached settings access
# =============================================================================
def get_settings() -> TFBSettings:
    return TFBSettings.from_env()


@lru_cache(maxsize=1)
def get_settings_cached() -> TFBSettings:
    return get_settings()


def reload_settings() -> TFBSettings:
    get_settings_cached.cache_clear()
    return get_settings_cached()


# =============================================================================
# Auth helpers
# =============================================================================
def _token_candidates(
    *,
    token: Any = None,
    authorization: Any = None,
    headers: Any = None,
    request: Any = None,
    x_app_token: Any = None,
    bearer_token: Any = None,
    **kwargs: Any,
) -> List[str]:
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
        candidates.extend(
            [
                _strip(hdrs.get("X-APP-TOKEN") or hdrs.get("x-app-token")),
                _extract_bearer_token(hdrs.get("Authorization") or hdrs.get("authorization")),
                _strip(hdrs.get("X-App-Token") or hdrs.get("x-app-token")),
            ]
        )

    if request is not None:
        try:
            request_headers = _coerce_headers(getattr(request, "headers", None))
            candidates.extend(
                [
                    _strip(request_headers.get("X-APP-TOKEN") or request_headers.get("x-app-token")),
                    _extract_bearer_token(request_headers.get("Authorization") or request_headers.get("authorization")),
                ]
            )
        except Exception:
            pass

    for key in ("app_token", "auth_token", "api_token", "query_token"):
        if key in kwargs:
            candidates.append(_strip(kwargs.get(key)))

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
    """
    Flexible auth checker used by routers and services.

    Behavior
    --------
    - If open_mode is True -> returns True
    - If auth is not required -> returns True
    - Otherwise checks provided tokens against configured allowed tokens
    """
    s = get_settings_cached()

    if s.open_mode:
        return True

    if not s.require_auth:
        return True

    allowed: Set[str] = {t for t in s.allowed_tokens if _strip(t)}
    if not allowed:
        return True

    candidates = _token_candidates(
        token=token,
        authorization=authorization,
        headers=headers,
        request=request,
        x_app_token=x_app_token,
        bearer_token=bearer_token,
        **kwargs,
    )

    return any(candidate in allowed for candidate in candidates if candidate)


def is_open_mode() -> bool:
    return bool(get_settings_cached().open_mode)


def is_auth_required() -> bool:
    s = get_settings_cached()
    return bool(s.require_auth and not s.open_mode)


# =============================================================================
# Runtime / metadata helpers
# =============================================================================
def now_utc_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def now_riyadh_iso() -> str:
    tz_name = get_settings_cached().timezone or DEFAULT_TIMEZONE
    try:
        tz = ZoneInfo(tz_name)
    except Exception:
        tz = ZoneInfo(DEFAULT_TIMEZONE)
    return datetime.now(tz).isoformat()


def local_hostname() -> str:
    try:
        return socket.gethostname()
    except Exception:
        return "unknown-host"


def settings_public_dict() -> Dict[str, Any]:
    return get_settings_cached().public_dict()


def build_runtime_meta() -> Dict[str, Any]:
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
        "config_version": CONFIG_VERSION,
    }


__all__ = [
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
]
