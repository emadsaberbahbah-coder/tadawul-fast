"""
ROUTES CONFIG SHIM – PROD SAFE (v1.1.1)

Purpose:
- Backward compatibility for imports like:
    from routes.config import get_settings, auth_ok, Settings
- Single source of truth lives in repo-root config.py
- Never crashes startup (defensive import)
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional

CONFIG_VERSION = "1.1.1-shim"

try:
    from config import (  # type: ignore
        CONFIG_VERSION as _MAIN_CONFIG_VERSION,
        Settings,
        allowed_tokens,
        auth_ok,
        get_settings,
        is_open_mode,
        mask_settings_dict,
    )

    CONFIG_VERSION = str(_MAIN_CONFIG_VERSION)

except Exception:
    Settings = object  # type: ignore

    def get_settings() -> object:  # type: ignore
        return object()

    def allowed_tokens() -> List[str]:
        return []

    def is_open_mode() -> bool:
        return True

    def auth_ok(x_app_token: Optional[str]) -> bool:  # noqa: ARG001
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
