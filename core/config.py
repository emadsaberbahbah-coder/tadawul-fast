"""
core/config.py
------------------------------------------------------------
TADAWUL FAST BRIDGE – CORE CONFIG SHIM (v4.2.0-shim) – PROD SAFE

FULL REPLACEMENT (v4.2.0-shim) — What’s improved vs v3.1.1-shim
- ✅ Stronger canonical import strategy (repo-root config.py) with ZERO core imports (avoids circulars)
- ✅ Better signature adaptation for auth_ok():
    - supports token-only, headers-only, request-like objects, and kwargs styles
    - normalizes headers keys (case-insensitive) and extracts Bearer token safely
- ✅ Consistent “open mode” behavior:
    - prefers root is_open_mode() if present
    - otherwise infers from allowed_tokens() / env hints
- ✅ Safer Settings export:
    - if root Settings missing, provides minimal shim Settings class with dict() support
- ✅ Adds helpers widely used across codebase:
    - get_setting(name, default=None)  (safe attribute/dict getter)
    - as_dict(settings)               (safe serialization)
- ✅ Optional debug tracing:
    CORE_CONFIG_DEBUG=true
    CORE_CONFIG_DEBUG_LEVEL=info|warn|error
- ✅ Never crashes startup: all functions are defensive and return safe defaults

Stable Exports
- CONFIG_VERSION
- Settings
- get_settings
- get_setting
- as_dict
- allowed_tokens
- is_open_mode
- auth_ok
- mask_settings_dict
"""

from __future__ import annotations

import os
from importlib import import_module
from typing import Any, Dict, List, Optional, Sequence, Tuple

CONFIG_VERSION = "4.2.0-shim"

# ---------------------------------------------------------------------------
# Debug logging (opt-in)
# ---------------------------------------------------------------------------
_DEBUG = str(os.getenv("CORE_CONFIG_DEBUG", "false")).strip().lower() in {
    "1",
    "true",
    "yes",
    "y",
    "on",
}
_DEBUG_LEVEL = str(os.getenv("CORE_CONFIG_DEBUG_LEVEL", "info")).strip().lower() or "info"


def _dbg(msg: str, level: str = "info") -> None:
    if not _DEBUG:
        return
    try:
        levels = {"info": 10, "warn": 20, "warning": 20, "error": 30}
        cur = levels.get(_DEBUG_LEVEL, 10)
        lvl = levels.get(level, 10)
        if lvl >= cur:
            print(f"[core.config:{level}] {msg}")  # noqa: T201
    except Exception:
        pass


# ---------------------------------------------------------------------------
# Small safe helpers
# ---------------------------------------------------------------------------
_TRUTHY = {"1", "true", "yes", "y", "on", "t"}
_FALSY = {"0", "false", "no", "n", "off", "f"}


def _safe_str(x: Any) -> Optional[str]:
    if x is None:
        return None
    try:
        s = str(x).strip()
        return s or None
    except Exception:
        return None


def _safe_list(x: Any) -> List[str]:
    try:
        if isinstance(x, (list, tuple, set)):
            out: List[str] = []
            for i in x:
                s = _safe_str(i)
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
        if v in _TRUTHY:
            return True
        if v in _FALSY:
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


def _headers_lower(headers: Any) -> Dict[str, str]:
    """
    Normalize headers to a lowercase dict[str, str].
    Accepts dict-like, list of tuples, or request-like objects with .headers.
    """
    out: Dict[str, str] = {}
    try:
        if headers is None:
            return out

        # request-like object
        if not isinstance(headers, (dict, list, tuple)) and hasattr(headers, "headers"):
            headers = getattr(headers, "headers", None)

        if isinstance(headers, dict):
            for k, v in headers.items():
                ks = _safe_str(k)
                if not ks:
                    continue
                vs = _safe_str(v) or ""
                out[ks.lower()] = vs
            return out

        if isinstance(headers, (list, tuple)):
            for it in headers:
                if isinstance(it, (list, tuple)) and len(it) == 2:
                    ks = _safe_str(it[0])
                    if not ks:
                        continue
                    out[ks.lower()] = _safe_str(it[1]) or ""
            return out

        return out
    except Exception:
        return out


def _extract_bearer(authorization: Optional[str]) -> Optional[str]:
    a = _safe_str(authorization)
    if not a:
        return None
    if a.lower().startswith("bearer "):
        return _safe_str(a.split(" ", 1)[1])
    return None


def _fallback_settings_obj() -> object:
    # Ultra-light placeholder. Do not assume pydantic.
    return object()


def get_setting(settings: Any, name: str, default: Any = None) -> Any:
    """
    Safe getter for settings objects or dicts:
    - dict: settings.get(name)
    - pydantic: getattr / model_dump / dict()
    - generic object: getattr
    """
    if not settings or not name:
        return default
    try:
        if isinstance(settings, dict):
            return settings.get(name, default)
        if hasattr(settings, name):
            v = getattr(settings, name, default)
            return v if v is not None else default
        # pydantic-ish fallback
        if hasattr(settings, "dict") and callable(getattr(settings, "dict")):
            d = settings.dict()  # type: ignore
            if isinstance(d, dict):
                return d.get(name, default)
        if hasattr(settings, "model_dump") and callable(getattr(settings, "model_dump")):
            d = settings.model_dump()  # type: ignore
            if isinstance(d, dict):
                return d.get(name, default)
    except Exception:
        pass
    return default


def as_dict(settings: Any) -> Dict[str, Any]:
    """Safe settings serialization (best-effort)."""
    try:
        if settings is None:
            return {}
        if isinstance(settings, dict):
            return dict(settings)
        if hasattr(settings, "model_dump") and callable(getattr(settings, "model_dump")):
            d = settings.model_dump()  # type: ignore
            return d if isinstance(d, dict) else {}
        if hasattr(settings, "dict") and callable(getattr(settings, "dict")):
            d = settings.dict()  # type: ignore
            return d if isinstance(d, dict) else {}
        # generic object -> attrs
        out: Dict[str, Any] = {}
        for k in dir(settings):
            if k.startswith("_"):
                continue
            try:
                v = getattr(settings, k)
            except Exception:
                continue
            if callable(v):
                continue
            out[k] = v
        return out
    except Exception:
        return {}


# ---------------------------------------------------------------------------
# Minimal Settings shim (only used if root Settings is missing)
# ---------------------------------------------------------------------------
class _SettingsShim:
    def __init__(self, **kwargs: Any) -> None:
        self.__dict__.update(kwargs)

    def dict(self) -> Dict[str, Any]:
        return dict(self.__dict__)


# ---------------------------------------------------------------------------
# Canonical import (repo-root config.py)
# ---------------------------------------------------------------------------
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
    Settings = _Settings if _Settings is not None else _SettingsShim  # type: ignore

    # Root callables (all optional)
    _get_settings = getattr(_m, "get_settings", None)
    _allowed_tokens = getattr(_m, "allowed_tokens", None)
    _is_open_mode = getattr(_m, "is_open_mode", None)
    _auth_ok = getattr(_m, "auth_ok", None)
    _mask_settings_dict = getattr(_m, "mask_settings_dict", None)

    _dbg(f"Canonical root config imported; CONFIG_VERSION={CONFIG_VERSION}", "info")

    def get_settings() -> Any:
        """Safe: never raises."""
        try:
            if callable(_get_settings):
                return _get_settings()
            return _fallback_settings_obj()
        except Exception as e:
            _dbg(f"get_settings() failed -> {e.__class__.__name__}: {e}", "warn")
            return _fallback_settings_obj()

    def allowed_tokens() -> List[str]:
        """Safe: never raises."""
        try:
            if callable(_allowed_tokens):
                return _safe_list(_allowed_tokens())
            # fallback: env
            env = _safe_str(os.getenv("APP_TOKENS")) or _safe_str(os.getenv("ALLOWED_TOKENS"))
            if env:
                return [t.strip() for t in env.split(",") if t.strip()]
            return []
        except Exception as e:
            _dbg(f"allowed_tokens() failed -> {e.__class__.__name__}: {e}", "warn")
            return []

    def is_open_mode() -> bool:
        """
        Safe open mode decision:
        - prefer root is_open_mode() if present
        - else infer from allowed_tokens + env hint OPEN_MODE
        """
        try:
            if callable(_is_open_mode):
                return _parse_bool(_is_open_mode(), True)

            env = _safe_str(os.getenv("OPEN_MODE"))
            if env is not None:
                return _parse_bool(env, True)

            toks = allowed_tokens()
            return True if not toks else False
        except Exception as e:
            _dbg(f"is_open_mode() failed -> {e.__class__.__name__}: {e}", "warn")
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

        Supports:
          auth_ok(x_app_token)
          auth_ok(headers_dict)
          auth_ok(x_app_token=..., authorization=..., headers=...)
          auth_ok(request_like_with_headers)
        """
        try:
            if not callable(_auth_ok):
                # If root doesn’t expose auth_ok: fall back to open-mode inference
                return bool(is_open_mode())

            hdrs = _headers_lower(headers)

            # Accept request-like passed positionally
            if not hdrs and args:
                hdrs = _headers_lower(args[0])

            # Normalize token sources
            x_token = _safe_str(x_app_token) or _safe_str(hdrs.get("x-app-token")) or _safe_str(hdrs.get("x_app_token"))
            auth = _safe_str(authorization) or _safe_str(hdrs.get("authorization"))
            bearer = _extract_bearer(auth)

            # Keep original casing for root implementations that expect it
            hdrs_cased: Dict[str, str] = {}
            if isinstance(headers, dict):
                for k, v in headers.items():
                    ks = _safe_str(k)
                    if ks:
                        hdrs_cased[ks] = _safe_str(v) or ""
            else:
                # reconstruct with common canonical keys
                if x_token:
                    hdrs_cased["X-APP-TOKEN"] = x_token
                if auth:
                    hdrs_cased["Authorization"] = auth

            # Try the most expressive call first
            try:
                return _parse_bool(
                    _auth_ok(x_app_token=x_token, authorization=auth, headers=hdrs_cased, bearer=bearer, *args, **kwargs),
                    True,
                )
            except TypeError:
                pass

            # Try headers-only style
            try:
                return _parse_bool(_auth_ok(hdrs_cased, *args, **kwargs), True)
            except TypeError:
                pass

            # Try token-only legacy
            try:
                return _parse_bool(_auth_ok(x_token), True)
            except TypeError:
                pass

            # Last resort: no-arg
            return _parse_bool(_auth_ok(), True)

        except Exception as e:
            _dbg(f"auth_ok() failed -> {e.__class__.__name__}: {e}", "warn")
            return bool(is_open_mode())

    def mask_settings_dict() -> Dict[str, Any]:
        """Safe: never raises. Tries root mask_settings_dict, otherwise builds a safe minimal payload."""
        try:
            if callable(_mask_settings_dict):
                d = _mask_settings_dict()
                out = _safe_dict(d, {})
            else:
                out = {}

            # Always attach shim metadata (non-sensitive)
            out.setdefault("status", "ok")
            out.setdefault("config_version", CONFIG_VERSION)
            out.setdefault("open_mode", bool(is_open_mode()))
            out.setdefault("token_count", len(allowed_tokens()))
            return out

        except Exception as e:
            _dbg(f"mask_settings_dict() failed -> {e.__class__.__name__}: {e}", "warn")
            return {"status": "fallback", "open_mode": bool(is_open_mode()), "config_version": CONFIG_VERSION}

except Exception as e:
    _dbg(f"Canonical import failed -> {e.__class__.__name__}: {e}", "error")

    # -----------------------------------------------------------------------
    # Ultra-light fallback (OPEN mode)
    # -----------------------------------------------------------------------
    Settings = _SettingsShim  # type: ignore

    def get_settings() -> object:  # type: ignore
        return _fallback_settings_obj()

    def allowed_tokens() -> List[str]:
        env = _safe_str(os.getenv("APP_TOKENS")) or _safe_str(os.getenv("ALLOWED_TOKENS"))
        if env:
            return [t.strip() for t in env.split(",") if t.strip()]
        return []

    def is_open_mode() -> bool:
        env = _safe_str(os.getenv("OPEN_MODE"))
        if env is not None:
            return _parse_bool(env, True)
        toks = allowed_tokens()
        return True if not toks else False

    def auth_ok(
        x_app_token: Optional[str] = None,  # noqa: ARG001
        authorization: Optional[str] = None,  # noqa: ARG001
        headers: Optional[Dict[str, str]] = None,  # noqa: ARG001
        *args: Any,  # noqa: ARG001
        **kwargs: Any,  # noqa: ARG001
    ) -> bool:
        # OPEN mode fallback
        return bool(is_open_mode())

    def mask_settings_dict() -> Dict[str, Any]:
        return {
            "status": "fallback",
            "open_mode": bool(is_open_mode()),
            "config_version": CONFIG_VERSION,
            "token_count": len(allowed_tokens()),
        }


__all__ = [
    "CONFIG_VERSION",
    "Settings",
    "get_settings",
    "get_setting",
    "as_dict",
    "allowed_tokens",
    "is_open_mode",
    "auth_ok",
    "mask_settings_dict",
]
