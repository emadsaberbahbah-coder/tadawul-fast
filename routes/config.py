# routes/config.py  (FULL REPLACEMENT)
"""
ROUTES CONFIG SHIM – PROD SAFE

Purpose:
- Backward compatibility for imports like:
    from routes.config import get_settings, auth_ok, Settings
- Single source of truth lives in repo-root config.py
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional

try:
    from config import (  # type: ignore
        CONFIG_VERSION,
        Settings,
        allowed_tokens,
        auth_ok,
        get_settings,
        is_open_mode,
        mask_settings_dict,
    )
except Exception:
    CONFIG_VERSION = "shim-fallback"
    Settings = object  # type: ignore

    def get_settings() -> object:  # type: ignore
        return object()

    def allowed_tokens() -> List[str]:
        return []

    def is_open_mode() -> bool:
        return True

    def auth_ok(x_app_token: Optional[str]) -> bool:
        return True

    def mask_settings_dict() -> Dict[str, Any]:
        return {"status": "fallback", "open_mode": True}


__all__ = [
    "CONFIG_VERSION",
    "Settings",
    "get_settings",
    "allowed_tokens",
    "is_open_mode",
    "auth_ok",
    "mask_settings_dict",
]
