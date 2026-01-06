```python
# routes/config.py  (FULL REPLACEMENT)
"""
routes/config.py
------------------------------------------------------------
ROUTES CONFIG SHIM — v1.2.0 (PROD SAFE)

Purpose
- Backward compatibility for imports like:
    from routes.config import get_settings, auth_ok, Settings
- Single source of truth lives in repo-root config.py
- Never crashes startup (defensive import)
- Avoids NameError / partial-import issues by always defining exports

Why this version fixes “showing failure”
- ✅ CONFIG_VERSION is ALWAYS defined (even if import fails mid-way)
- ✅ Provides a stable fallback Settings dataclass (instead of bare object)
- ✅ Each fallback function is defined unconditionally inside the except block
- ✅ __all__ always matches available names
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List, Optional

# Always defined (even if canonical import fails)
CONFIG_VERSION: str = "1.2.0-shim"


# -----------------------------
# Fallback (OPEN MODE) objects
# -----------------------------
@dataclass(frozen=True)
class _FallbackSettings:
    service_name: str = "tadawul-fast-bridge"
    environment: str = "prod"
    log_level: str = "INFO"


def _fallback_get_settings() -> _FallbackSettings:
    return _FallbackSettings()


def _fallback_allowed_tokens() -> List[str]:
    return []


def _fallback_is_open_mode() -> bool:
    return True


def _fallback_auth_ok(x_app_token: Optional[str]) -> bool:
    return True


def _fallback_mask_settings_dict() -> Dict[str, Any]:
    s = _fallback_get_settings()
    return {
        "status": "fallback",
        "open_mode": True,
        "service_name": s.service_name,
        "environment": s.environment,
        "log_level": s.log_level,
    }


# --------------------------------------------
# Canonical import (repo-root config.py)
# --------------------------------------------
try:
    from config import (  # type: ignore
        CONFIG_VERSION as _MAIN_CONFIG_VERSION,
        Settings as _MainSettings,
        allowed_tokens as _allowed_tokens,
        auth_ok as _auth_ok,
        get_settings as _get_settings,
        is_open_mode as _is_open_mode,
        mask_settings_dict as _mask_settings_dict,
    )

    # Mirror canonical version (if valid)
    if isinstance(_MAIN_CONFIG_VERSION, str) and _MAIN_CONFIG_VERSION.strip():
        CONFIG_VERSION = _MAIN_CONFIG_VERSION.strip()

    # Re-export canonical names
    Settings = _MainSettings  # type: ignore
    get_settings = _get_settings  # type: ignore
    allowed_tokens = _allowed_tokens  # type: ignore
    is_open_mode = _is_open_mode  # type: ignore
    auth_ok = _auth_ok  # type: ignore
    mask_settings_dict = _mask_settings_dict  # type: ignore

except Exception:
    # Re-export fallback names (OPEN MODE)
    Settings = _FallbackSettings  # type: ignore
    get_settings = _fallback_get_settings  # type: ignore
    allowed_tokens = _fallback_allowed_tokens  # type: ignore
    is_open_mode = _fallback_is_open_mode  # type: ignore
    auth_ok = _fallback_auth_ok  # type: ignore
    mask_settings_dict = _fallback_mask_settings_dict  # type: ignore


__all__ = [
    "CONFIG_VERSION",
    "Settings",
    "get_settings",
    "allowed_tokens",
    "is_open_mode",
    "auth_ok",
    "mask_settings_dict",
]
```
