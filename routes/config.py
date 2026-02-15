# routes/config.py
"""
ROUTES CONFIG SHIM – PROD SAFE (v2.1.0)
------------------------------------------------------------

Purpose
- Backward compatibility for older imports like:
    from routes.config import get_settings, auth_ok, allowed_tokens, Settings, AUTH_HEADER_NAME
- Single source of truth ideally lives in repo-root `config.py`
- NEVER crashes app startup (defensive import + safe fallbacks)
- Supports BOTH:
    - X-APP-TOKEN header
    - Authorization: Bearer <token>
    - optional ?token=... when ALLOW_QUERY_TOKEN=1

Key upgrades in v2.1.0
- ✅ Exposes AUTH_HEADER_NAME and resolves dynamically from root config/settings/env
- ✅ Adds auth_ok_request() (FastAPI-friendly) while keeping auth_ok(x_app_token) compatibility
- ✅ Stable token resolution: root config helpers -> settings fields -> env vars
- ✅ Open-mode logic consistent across routes
- ✅ mask_settings_dict() returns safe snapshot suitable for /health logs
- ✅ No heavy imports, no side effects, no file IO, no network

Environment
- APP_TOKEN / BACKUP_APP_TOKEN / TFB_APP_TOKEN
- AUTH_HEADER_NAME (optional, default X-APP-TOKEN)
- ALLOW_QUERY_TOKEN=1 (optional)

Notes
- If root config exists but is missing some helpers, this shim provides safe defaults.
"""

from __future__ import annotations

import importlib
import os
from typing import Any, Dict, List, Optional, Sequence

SHIM_VERSION = "2.1.0"
CONFIG_VERSION = f"{SHIM_VERSION}-shim"

_TRUTHY = {"1", "true", "yes", "y", "on", "t"}

# -----------------------------------------------------------------------------
# Fallback Settings (used if root config can't be imported OR missing fields)
# -----------------------------------------------------------------------------
class _FallbackSettings:
    # Auth
    auth_header_name: str = "X-APP-TOKEN"
    allow_query_token: bool = False

    # AI batch defaults (commonly used by analysis routes)
    ai_batch_size: int = 25
    ai_batch_timeout_sec: float = 60.0
    ai_batch_concurrency: int = 6
    ai_max_tickers: int = 1200

    # Optional meta
    app_env: str = (os.getenv("APP_ENV") or "production").lower()


Settings = _FallbackSettings  # alias for callers expecting Settings


# -----------------------------------------------------------------------------
# Helpers
# -----------------------------------------------------------------------------
def _env_bool(name: str, default: bool = False) -> bool:
    v = (os.getenv(name) or "").strip().lower()
    if not v:
        return default
    return v in _TRUTHY


def _safe_getattr(obj: Any, name: str, default: Any = None) -> Any:
    try:
        return getattr(obj, name, default)
    except Exception:
        return default


def _strip(x: Any) -> str:
    try:
        return str(x or "").strip()
    except Exception:
        return ""


def _dedupe_keep_order(items: Sequence[str]) -> List[str]:
    out: List[str] = []
    seen = set()
    for s in items:
        if s and s not in seen:
            seen.add(s)
            out.append(s)
    return out


def _load_root_config() -> Optional[Any]:
    try:
        return importlib.import_module("config")
    except Exception:
        return None


# -----------------------------------------------------------------------------
# Defaults (will be overridden if root config provides them)
# -----------------------------------------------------------------------------
def get_settings() -> object:
    return _FallbackSettings()


def allowed_tokens() -> List[str]:
    toks: List[str] = []
    for k in ("APP_TOKEN", "BACKUP_APP_TOKEN", "TFB_APP_TOKEN"):
        v = (os.getenv(k) or "").strip()
        if v:
            toks.append(v)
    return _dedupe_keep_order(toks)


def is_open_mode() -> bool:
    return len(allowed_tokens()) == 0


def _extract_bearer(authorization: Optional[str]) -> str:
    a = _strip(authorization)
    if not a:
        return ""
    parts = a.split()
    if len(parts) == 2 and parts[0].lower() == "bearer":
        return parts[1].strip()
    return ""


def auth_ok(x_app_token: Optional[str]) -> bool:
    """
    Backward compatible:
      - If tokens NOT set => open mode => True
      - Else checks the configured auth header token value
    """
    toks = allowed_tokens()
    if not toks:
        return True
    return bool(x_app_token and _strip(x_app_token) in toks)


def auth_ok_request(
    *,
    x_app_token: Optional[str] = None,
    authorization: Optional[str] = None,
    query_token: Optional[str] = None,
) -> bool:
    """
    Modern helper:
    - Accepts X-APP-TOKEN (or configured header), Authorization Bearer, optional ?token when allowed
    """
    toks = allowed_tokens()
    if not toks:
        return True

    tok = _strip(x_app_token)
    if tok and tok in toks:
        return True

    b = _extract_bearer(authorization)
    if b and b in toks:
        return True

    allow_q = _env_bool("ALLOW_QUERY_TOKEN", False)
    if allow_q:
        qt = _strip(query_token)
        if qt and qt in toks:
            return True

    return False


def _resolve_auth_header_name_from_settings(s: Any) -> str:
    # Try multiple common names
    for attr in ("auth_header_name", "AUTH_HEADER_NAME", "authHeaderName"):
        v = _strip(_safe_getattr(s, attr))
        if v:
            return v
    return ""


def _resolve_auth_header_name() -> str:
    # env override wins
    env_v = _strip(os.getenv("AUTH_HEADER_NAME"))
    if env_v:
        return env_v

    # try settings object
    try:
        s = get_settings()
        v = _resolve_auth_header_name_from_settings(s)
        if v:
            return v
    except Exception:
        pass

    return "X-APP-TOKEN"


AUTH_HEADER_NAME = _resolve_auth_header_name()


def mask_settings_dict() -> Dict[str, Any]:
    """
    Safe snapshot for logs/health.
    NEVER returns secrets.
    """
    try:
        s = get_settings()
        exposed: Dict[str, Any] = {}

        for k in ("ai_batch_size", "ai_batch_timeout_sec", "ai_batch_concurrency", "ai_max_tickers", "app_env"):
            v = _safe_getattr(s, k, None)
            if v is not None:
                exposed[k] = v

        exposed["auth_header_name"] = _resolve_auth_header_name_from_settings(s) or AUTH_HEADER_NAME
        exposed["allow_query_token"] = _env_bool("ALLOW_QUERY_TOKEN", False)

        return {
            "status": "ok",
            "open_mode": is_open_mode(),
            "config_version": CONFIG_VERSION,
            "auth_header": AUTH_HEADER_NAME,
            "tokens_set": len(allowed_tokens()),
            "exposed": exposed,
        }
    except Exception:
        return {
            "status": "fallback",
            "open_mode": True,
            "config_version": CONFIG_VERSION,
            "auth_header": AUTH_HEADER_NAME,
            "tokens_set": len(allowed_tokens()),
        }


# -----------------------------------------------------------------------------
# Dynamic Proxy Logic (Prefer root config, fallback to stubs)
# -----------------------------------------------------------------------------
_root = _load_root_config()

if _root is not None:
    # Prefer root CONFIG_VERSION if present
    _v = _safe_getattr(_root, "CONFIG_VERSION")
    if _v is not None:
        try:
            CONFIG_VERSION = str(_v)
        except Exception:
            pass

    # Prefer root Settings if present
    _Settings = _safe_getattr(_root, "Settings")
    if _Settings is not None:
        Settings = _Settings  # type: ignore[assignment]

    # Prefer root get_settings if present
    _get_settings = _safe_getattr(_root, "get_settings")
    if callable(_get_settings):
        get_settings = _get_settings  # type: ignore[assignment]

    # Prefer root allowed_tokens if present
    _allowed_tokens = _safe_getattr(_root, "allowed_tokens")
    if callable(_allowed_tokens):
        allowed_tokens = _allowed_tokens  # type: ignore[assignment]
    else:
        # If root doesn't provide allowed_tokens(), derive from settings + env.
        def allowed_tokens() -> List[str]:  # type: ignore[override]
            toks: List[str] = []

            # Try settings object for token fields
            try:
                s = get_settings()
                for attr in (
                    "app_token",
                    "backup_app_token",
                    "tfb_app_token",
                    "APP_TOKEN",
                    "BACKUP_APP_TOKEN",
                    "TFB_APP_TOKEN",
                ):
                    v = _strip(_safe_getattr(s, attr))
                    if v:
                        toks.append(v)
            except Exception:
                pass

            # Env safety net
            for k in ("APP_TOKEN", "BACKUP_APP_TOKEN", "TFB_APP_TOKEN"):
                v = (os.getenv(k) or "").strip()
                if v:
                    toks.append(v)

            return _dedupe_keep_order(toks)

    # Prefer root is_open_mode if present
    _is_open_mode = _safe_getattr(_root, "is_open_mode")
    if callable(_is_open_mode):
        is_open_mode = _is_open_mode  # type: ignore[assignment]
    else:
        def is_open_mode() -> bool:  # type: ignore[override]
            try:
                return len(allowed_tokens()) == 0
            except Exception:
                return True

    # Prefer root auth_ok if present, else keep shim logic
    _auth_ok = _safe_getattr(_root, "auth_ok")
    if callable(_auth_ok):
        auth_ok = _auth_ok  # type: ignore[assignment]

    # Prefer root mask_settings_dict if present
    _mask_settings_dict = _safe_getattr(_root, "mask_settings_dict")
    if callable(_mask_settings_dict):
        mask_settings_dict = _mask_settings_dict  # type: ignore[assignment]

    # AUTH_HEADER_NAME: prefer root constant, else settings, else env/default
    _ahn = _safe_getattr(_root, "AUTH_HEADER_NAME")
    if _strip(_ahn):
        AUTH_HEADER_NAME = str(_ahn).strip()
    else:
        AUTH_HEADER_NAME = _resolve_auth_header_name()


__all__ = [
    "CONFIG_VERSION",
    "Settings",
    "AUTH_HEADER_NAME",
    "get_settings",
    "allowed_tokens",
    "is_open_mode",
    "auth_ok",
    "auth_ok_request",
    "mask_settings_dict",
]
