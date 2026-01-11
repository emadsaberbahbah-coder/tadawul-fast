# core/config.py
"""
TADAWUL FAST BRIDGE – CORE CONFIG SHIM (v2.9.0-shim) – PROD SAFE

Purpose
- Backward compatibility for imports like:
    from core.config import get_settings, auth_ok, Settings
- Ensure ONLY ONE canonical settings implementation exists: repo-root config.py
- Never crashes startup (defensive import + safe fallbacks)
- Avoid circular imports: do NOT import anything else from core at import-time.

Behavior
- Prefer repo-root `config.py` as the canonical settings source.
- If missing/unavailable, expose a minimal OPEN-MODE fallback that keeps the app usable.

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

CONFIG_VERSION = "2.9.0-shim"


def _fallback_settings_obj() -> object:
    # Ultra-light placeholder. Do not assume pydantic.
    return object()


# ----------------------------
# Canonical import (repo-root)
# ----------------------------
try:
    # Canonical source (repo-root config.py)
    from config import (  # type: ignore
        CONFIG_VERSION as _MAIN_CONFIG_VERSION,
        Settings as _Settings,
        allowed_tokens as _allowed_tokens,
        auth_ok as _auth_ok,
        get_settings as _get_settings,
        is_open_mode as _is_open_mode,
        mask_settings_dict as _mask_settings_dict,
    )

    # Re-export canonical symbols
    CONFIG_VERSION = str(_MAIN_CONFIG_VERSION)

    Settings = _Settings  # type: ignore

    def get_settings() -> Any:
        try:
            return _get_settings()
        except Exception:
            # Keep startup/runtime safe if canonical get_settings ever fails.
            return _fallback_settings_obj()

    def allowed_tokens() -> List[str]:
        try:
            v = _allowed_tokens()
            return list(v) if isinstance(v, (list, tuple)) else []
        except Exception:
            return []

    def is_open_mode() -> bool:
        try:
            return bool(_is_open_mode())
        except Exception:
            return True

    def auth_ok(x_app_token: Optional[str]) -> bool:
        try:
            # canonical logic (supports open mode / allow list)
            return bool(_auth_ok(x_app_token))
        except Exception:
            return True

    def mask_settings_dict() -> Dict[str, Any]:
        try:
            d = _mask_settings_dict()
            return d if isinstance(d, dict) else {"status": "ok", "config_version": CONFIG_VERSION}
        except Exception:
            return {"status": "fallback", "open_mode": True, "config_version": CONFIG_VERSION}

except Exception:
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
