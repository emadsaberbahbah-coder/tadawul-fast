```python
# core/config.py  (FULL REPLACEMENT)
"""
TADAWUL FAST BRIDGE – CORE CONFIG SHIM (v2.8.0-shim) – PROD SAFE

Purpose
- Keep backward compatibility for imports like:
    from core.config import get_settings, auth_ok, Settings
- Ensure ONLY ONE canonical settings implementation exists: repo-root config.py
- Never crashes startup (defensive import)

If repo-root config.py is missing for any reason, this file provides a tiny fallback
that preserves OPEN mode behavior (no auth required).
"""

from __future__ import annotations

from dataclasses import dataclass, asdict
from functools import lru_cache
from typing import Any, Dict, List, Optional

CONFIG_VERSION = "2.8.0-shim"

try:
    # Canonical source (repo root): config.py
    from config import (  # type: ignore
        CONFIG_VERSION as _MAIN_CONFIG_VERSION,
        Settings,
        allowed_tokens,
        auth_ok,
        get_settings,
        is_open_mode,
        mask_settings_dict,
    )

    # Mirror canonical version so /version endpoints stay consistent
    CONFIG_VERSION = _MAIN_CONFIG_VERSION

except Exception:
    # -------------------------------------------------------------------------
    # Ultra-light fallback (should almost never happen)
    # - Open mode only (no auth)
    # - Minimal Settings object for code expecting attributes
    # -------------------------------------------------------------------------

    @dataclass(frozen=True)
    class Settings:  # type: ignore
        service_name: str = "tadawul-fast-bridge"
        environment: str = "prod"
        log_level: str = "INFO"
        app_token: Optional[str] = None
        backup_app_token: Optional[str] = None

    @lru_cache(maxsize=1)
    def get_settings() -> Settings:  # type: ignore
        return Settings()

    def allowed_tokens() -> List[str]:
        return []

    def is_open_mode() -> bool:
        return True

    def auth_ok(x_app_token: Optional[str]) -> bool:
        return True

    def mask_settings_dict() -> Dict[str, Any]:
        s = get_settings()
        d = asdict(s)
        d.update({"status": "fallback", "open_mode": True})
        # never expose secrets even in fallback
        d["app_token"] = None
        d["backup_app_token"] = None
        return d


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
