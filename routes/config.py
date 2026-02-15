"""
ROUTES CONFIG SHIM – PROD SAFE (v1.2.2)

Purpose
- Backward compatibility for older imports like:
    from routes.config import get_settings, auth_ok, allowed_tokens, Settings
- Single source of truth lives in repo-root `config.py`
- Never crashes app startup (defensive import + safe fallbacks)
- Exposes `AUTH_HEADER_NAME` for dynamic auth configuration

Notes
- If the root `config.py` exists but is missing some helpers, this shim provides
  safe defaults while still exposing the expected names.
"""

from __future__ import annotations

import importlib
import os
from typing import Any, Dict, List, Optional

CONFIG_VERSION = "1.2.2-shim"

# Fallback stubs (used if root config can't be imported OR is missing fields)
class _FallbackSettings:
    auth_header_name: str = "X-APP-TOKEN"
    ai_batch_size: int = 20
    ai_batch_timeout_sec: float = 45.0
    ai_batch_concurrency: int = 5
    ai_max_tickers: int = 500


Settings = _FallbackSettings  # type: ignore[assignment]


def get_settings() -> object:  # type: ignore[override]
    return _FallbackSettings()


def allowed_tokens() -> List[str]:
    # Try direct ENV read if config is broken
    toks = []
    for k in ("APP_TOKEN", "BACKUP_APP_TOKEN", "TFB_APP_TOKEN"):
        v = (os.getenv(k) or "").strip()
        if v: toks.append(v)
    return list(set(toks))


def is_open_mode() -> bool:
    return len(allowed_tokens()) == 0


def auth_ok(x_app_token: Optional[str]) -> bool:  # noqa: ARG001
    toks = allowed_tokens()
    if not toks: return True
    return bool(x_app_token and x_app_token.strip() in toks)


def mask_settings_dict() -> Dict[str, Any]:
    return {
        "status": "fallback", 
        "open_mode": is_open_mode(), 
        "config_version": CONFIG_VERSION,
        "auth_header": "X-APP-TOKEN"
    }


def _safe_getattr(mod: Any, name: str) -> Any:
    try:
        return getattr(mod, name, None)
    except Exception:
        return None


def _load_root_config() -> Optional[Any]:
    try:
        return importlib.import_module("config")
    except Exception:
        return None


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

    # Settings class (if present)
    _Settings = _safe_getattr(_root, "Settings")
    if _Settings is not None:
        Settings = _Settings  # type: ignore[assignment]

    # get_settings() (if present)
    _get_settings = _safe_getattr(_root, "get_settings")
    if callable(_get_settings):
        get_settings = _get_settings  # type: ignore[assignment]

    # allowed_tokens() (if present) — else derive best-effort from env/root settings
    _allowed_tokens = _safe_getattr(_root, "allowed_tokens")
    if callable(_allowed_tokens):
        allowed_tokens = _allowed_tokens  # type: ignore[assignment]
    else:
        # Redefine using the *actual* get_settings we just resolved
        def allowed_tokens() -> List[str]:  # type: ignore[override]
            try:
                # Try common patterns on settings object
                s = get_settings()
                toks: List[str] = []
                for attr in ("app_token", "backup_app_token", "APP_TOKEN", "BACKUP_APP_TOKEN"):
                    try:
                        v = getattr(s, attr, None)
                        if isinstance(v, str) and v.strip():
                            toks.append(v.strip())
                    except Exception:
                        pass
                
                # Also check ENV vars directly as a safety net
                for k in ("APP_TOKEN", "BACKUP_APP_TOKEN", "TFB_APP_TOKEN"):
                    v = (os.getenv(k) or "").strip()
                    if v: toks.append(v)

                return list(set(toks))
            except Exception:
                return []

    # is_open_mode() (if present) — else open mode iff no tokens
    _is_open_mode = _safe_getattr(_root, "is_open_mode")
    if callable(_is_open_mode):
        is_open_mode = _is_open_mode  # type: ignore[assignment]
    else:
        def is_open_mode() -> bool:  # type: ignore[override]
            try:
                return len(allowed_tokens()) == 0
            except Exception:
                return True

    # auth_ok() (if present) — else check X-APP-TOKEN against allowed_tokens()
    _auth_ok = _safe_getattr(_root, "auth_ok")
    if callable(_auth_ok):
        auth_ok = _auth_ok  # type: ignore[assignment]
    else:
        def auth_ok(x_app_token: Optional[str]) -> bool:  # type: ignore[override]
            try:
                toks = allowed_tokens()
                if not toks:
                    return True
                return bool(x_app_token and x_app_token.strip() in toks)
            except Exception:
                return True

    # mask_settings_dict() (if present) — else minimal safe masking
    _mask_settings_dict = _safe_getattr(_root, "mask_settings_dict")
    if callable(_mask_settings_dict):
        mask_settings_dict = _mask_settings_dict  # type: ignore[assignment]
    else:
        def mask_settings_dict() -> Dict[str, Any]:  # type: ignore[override]
            try:
                s = get_settings()
                d: Dict[str, Any] = {}
                for k in ("ai_batch_size", "ai_batch_timeout_sec", "ai_batch_concurrency", "ai_max_tickers", "auth_header_name"):
                    try:
                        v = getattr(s, k, None)
                        if v is not None:
                            d[k] = v
                    except Exception:
                        pass
                return {
                    "status": "ok",
                    "open_mode": is_open_mode(),
                    "config_version": CONFIG_VERSION,
                    "exposed": d,
                }
            except Exception:
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
