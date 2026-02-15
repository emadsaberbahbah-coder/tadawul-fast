"""
TADAWUL FAST BRIDGE – CORE CONFIG SHIM (v3.1.1-shim) – PROD SAFE

Purpose
- Backward compatibility for imports like:
    from core.config import get_settings, auth_ok, Settings
- Ensure ONLY ONE canonical settings implementation exists: repo-root config.py
- Never crashes startup (defensive import + safe fallbacks)
- Avoid circular imports: do NOT import anything else from core at import-time.

Enhancements in v3.1.1-shim
- ✅ Fixes signature drift safely: auth_ok() accepts flexible args/kwargs and adapts to root config shape.
- ✅ Removes “brittle” assumptions about root exports (all optional).
- ✅ Better bool parsing + safer fallbacks.
- ✅ Optional debug tracing via CORE_CONFIG_DEBUG=true.

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

import os
from importlib import import_module
from typing import Any, Dict, List, Optional, Set

CONFIG_VERSION = "3.1.1-shim"

_DEBUG = str(os.getenv("CORE_CONFIG_DEBUG", "false")).strip().lower() in {
    "1", "true", "yes", "y", "on"
}


def _dbg(msg: str) -> None:
    if not _DEBUG:
        return
    try:
        print(f"[core.config] {msg}")  # noqa: T201
    except Exception:
        pass


def _fallback_settings_obj() -> object:
    # Ultra-light placeholder. Do not assume pydantic.
    return object()


def _safe_list(x: Any) -> List[str]:
    try:
        if isinstance(x, (list, tuple, set)):
            out: List[str] = []
            for i in x:
                s = str(i).strip()
                if s:
                    out.append(s)
            return out
        return []
    except Exception:
        return []


def _parse_bool(x: Any, default: bool) -> bool:
    if x is None:
        return default
    if isinstance(x, bool):
        return x
    if isinstance(x, (int, float)):
        return bool(x)
    if isinstance(x, str):
        v = x.strip().lower()
        if v in {"1", "true", "yes", "y", "on"}:
            return True
        if v in {"0", "false", "no", "n", "off"}:
            return False
        return default
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
    _m = import_module("config")

    # Optional: root may expose CONFIG_VERSION or version
    _root_ver = (
        getattr(_m, "CONFIG_VERSION", None)
        or getattr(_m, "version", None)
        or getattr(_m, "__version__", None)
    )
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
            return _fallback_settings_obj()
        except Exception as e:
            _dbg(f"get_settings() failed -> {e.__class__.__name__}: {e}")
            return _fallback_settings_obj()

    def allowed_tokens() -> List[str]:
        try:
            if callable(_allowed_tokens):
                return _safe_list(_allowed_tokens())
            return []
        except Exception as e:
            _dbg(f"allowed_tokens() failed -> {e.__class__.__name__}: {e}")
            return []

    def is_open_mode() -> bool:
        try:
            if callable(_is_open_mode):
                return _parse_bool(_is_open_mode(), True)
            return True
        except Exception as e:
            _dbg(f"is_open_mode() failed -> {e.__class__.__name__}: {e}")
            return True

    def auth_ok(
        x_app_token: Optional[str] = None,
        authorization: Optional[str] = None,
        headers: Optional[Dict[str, str]] = None,
        *args: Any,
        **kwargs: Any,
    ) -> bool:
        """
        Backward/forward compatible auth shim.

        Supports older calls:
          auth_ok(x_app_token)

        And newer/root shapes like:
          auth_ok(x_app_token=..., authorization=..., headers=...)
          auth_ok(headers)
          auth_ok(request_headers_dict)
        """
        try:
            if not callable(_auth_ok):
                return True  # open mode fallback

            # If headers dict not provided, synthesize it
            hdrs: Dict[str, str] = {}
            if isinstance(headers, dict):
                hdrs.update({str(k): str(v) for k, v in headers.items()})

            if x_app_token:
                hdrs.setdefault("x-app-token", str(x_app_token))
            if authorization:
                hdrs.setdefault("authorization", str(authorization))

            # Try the most expressive call first
            try:
                return _parse_bool(
                    _auth_ok(x_app_token=x_app_token, authorization=authorization, headers=hdrs, *args, **kwargs),
                    True,
                )
            except TypeError:
                pass

            # Try passing headers only (common pattern)
            try:
                return _parse_bool(_auth_ok(hdrs, *args, **kwargs), True)
            except TypeError:
                pass

            # Try legacy single-arg token
            try:
                return _parse_bool(_auth_ok(x_app_token), True)
            except TypeError:
                pass

            # As a last resort, call without args
            return _parse_bool(_auth_ok(), True)

        except Exception as e:
            _dbg(f"auth_ok() failed -> {e.__class__.__name__}: {e}")
            return True

    def mask_settings_dict() -> Dict[str, Any]:
        try:
            if callable(_mask_settings_dict):
                d = _mask_settings_dict()
                return _safe_dict(d, {"status": "ok", "config_version": CONFIG_VERSION})
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

    def auth_ok(
        x_app_token: Optional[str] = None,  # noqa: ARG001
        authorization: Optional[str] = None,  # noqa: ARG001
        headers: Optional[Dict[str, str]] = None,  # noqa: ARG001
        *args: Any,  # noqa: ARG001
        **kwargs: Any,  # noqa: ARG001
    ) -> bool:
        return True  # OPEN mode fallback

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
