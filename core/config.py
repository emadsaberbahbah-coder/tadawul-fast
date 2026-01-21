# core/config.py
"""
TADAWUL FAST BRIDGE – CORE CONFIG SHIM (v3.0.0-shim) – PROD SAFE

Purpose
- Backward compatibility for imports like:
    from core.config import get_settings, auth_ok, Settings
- Ensure ONLY ONE canonical settings implementation exists: repo-root config.py
- Never crashes startup (defensive import + safe fallbacks)
- Avoid circular imports: do NOT import anything else from core at import-time.

Improvements in v3.0.0-shim
- ✅ Robust canonical import: supports missing optional exports (mask_settings_dict, etc.)
- ✅ OPEN fallback still works even if root config.py changes shape
- ✅ Minimal debug tracing via CORE_CONFIG_DEBUG=true (no logging import)
- ✅ Safer Settings export: always a usable type (real Settings class if present, else object)

Exports (stable)
- CONFIG_VERSION
- Settings
- get_settings
- allowed_tokens
- is_open_mode
- auth_ok
- mask_settings_dict
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional
import os

CONFIG_VERSION = "3.0.0-shim"

_DEBUG = str(os.getenv("CORE_CONFIG_DEBUG", "false")).strip().lower() in {"1", "true", "yes", "y", "on"}


def _dbg(msg: str) -> None:
    if _DEBUG:
        try:
            print(f"[core.config] {msg}")  # noqa: T201
        except Exception:
            pass


def _fallback_settings_obj() -> object:
    # Ultra-light placeholder. Do not assume pydantic.
    return object()


def _safe_list(x: Any) -> List[str]:
    try:
        if isinstance(x, (list, tuple)):
            return [str(i) for i in x if str(i).strip()]
        return []
    except Exception:
        return []


def _safe_bool(x: Any, default: bool = True) -> bool:
    try:
        return bool(x)
    except Exception:
        return default


def _safe_dict(x: Any, default: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    if default is None:
        default = {}
    try:
        return x if isinstance(x, dict) else dict(default)
    except Exception:
        return dict(default)


# ----------------------------
# Canonical import (repo-root)
# ----------------------------
try:
    # Import module first (more resilient than importing many names at once)
    from importlib import import_module

    _m = import_module("config")

    # Optional: root may expose CONFIG_VERSION or version
    _root_ver = getattr(_m, "CONFIG_VERSION", None) or getattr(_m, "version", None) or getattr(_m, "__version__", None)
    if _root_ver is not None:
        CONFIG_VERSION = str(_root_ver)

    # Settings class/type (optional)
    _Settings = getattr(_m, "Settings", None)
    Settings = _Settings if _Settings is not None else object  # type: ignore

    _get_settings = getattr(_m, "get_settings", None)
    _allowed_tokens = getattr(_m, "allowed_tokens", None)
    _is_open_mode = getattr(_m, "is_open_mode", None)
    _auth_ok = getattr(_m, "auth_ok", None)
    _mask_settings_dict = getattr(_m, "mask_settings_dict", None)

    def get_settings() -> Any:
        try:
            if callable(_get_settings):
                return _get_settings()
            # If root doesn't expose get_settings, still stay safe.
            return _fallback_settings_obj()
        except Exception as e:
            _dbg(f"get_settings() failed -> {e.__class__.__name__}: {e}")
            return _fallback_settings_obj()

    def allowed_tokens() -> List[str]:
        try:
            if callable(_allowed_tokens):
                return _safe_list(_allowed_tokens())
            # If root doesn't expose allowed_tokens, assume open mode
            return []
        except Exception as e:
            _dbg(f"allowed_tokens() failed -> {e.__class__.__name__}: {e}")
            return []

    def is_open_mode() -> bool:
        try:
            if callable(_is_open_mode):
                return _safe_bool(_is_open_mode(), True)
            # No function => treat as open mode
            return True
        except Exception as e:
            _dbg(f"is_open_mode() failed -> {e.__class__.__name__}: {e}")
            return True

    def auth_ok(x_app_token: Optional[str]) -> bool:
        try:
            if callable(_auth_ok):
                return _safe_bool(_auth_ok(x_app_token), True)
            # No function => open
            return True
        except Exception as e:
            _dbg(f"auth_ok() failed -> {e.__class__.__name__}: {e}")
            return True

    def mask_settings_dict() -> Dict[str, Any]:
        try:
            if callable(_mask_settings_dict):
                d = _mask_settings_dict()
                return _safe_dict(d, {"status": "ok", "config_version": CONFIG_VERSION})
            # If root doesn't expose it, provide a safe minimal shape
            return {"status": "ok", "open_mode": is_open_mode(), "config_version": CONFIG_VERSION}
        except Exception as e:
            _dbg(f"mask_settings_dict() failed -> {e.__class__.__name__}: {e}")
            return {"status": "fallback", "open_mode": True, "config_version": CONFIG_VERSION}

except Exception as e:
    _dbg(f"Canonical import failed -> {e.__class__.__name__}: {e}")

    # ----------------------------
    # Ultra-light fallback
    # ----------------------------
    Settings = object  # type: ignore

    def get_settings() -> object:  # type: ignore
        return _fallback_settings_obj()

    def allowed_tokens() -> List[str]:
        return []

    def is_open_mode() -> bool:
        return True

    def auth_ok(x_app_token: Optional[str]) -> bool:  # noqa: ARG001
        # OPEN mode (no token required)
        return True

    def mask_settings_dict() -> Dict[str, Any]:
        return {"status": "fallback", "open_mode": True, "config_version": CONFIG_VERSION}


__all__ = [
    "CONFIG_VERSION",
    "Settings",
    "get_settings",
    "allowed_tokens",
    "is_open_mode",
    "auth_ok",
    "mask_settings_dict",
]
