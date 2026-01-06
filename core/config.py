"""
TADAWUL FAST BRIDGE – CORE CONFIG SHIM (v2.8.1-shim) – PROD SAFE

Purpose
- Backward compatibility for imports like:
    from core.config import get_settings, auth_ok, Settings
- Ensure ONLY ONE canonical settings implementation exists: repo-root config.py
- Never crashes startup (defensive import)

If repo-root config.py is missing for any reason, this file provides a tiny fallback
that preserves OPEN mode behavior.
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional

CONFIG_VERSION = "2.8.1-shim"

try:
    # Canonical source (repo-root config.py)
    from config import (  # type: ignore
        CONFIG_VERSION as _MAIN_CONFIG_VERSION,
        Settings,
        allowed_tokens,
        auth_ok,
        get_settings,
        is_open_mode,
        mask_settings_dict,
    )

    # Mirror canonical version (helps debugging)
    CONFIG_VERSION = str(_MAIN_CONFIG_VERSION)

except Exception:
    # Ultra-light fallback (should almost never happen)
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
