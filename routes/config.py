# routes/config.py
"""
routes/config.py
------------------------------------------------------------
Compatibility shim for older route modules that do:
  from routes.config import Settings, get_settings

Goals
- Prefer core.config (canonical) to keep settings centralized.
- Fall back to root config.py to support older layouts.
- Never crash import-time: if both imports fail, provide a minimal safe fallback
  so the app can still boot and routers can mount.

Notes
- This module MUST stay lightweight (no heavy imports).
"""

from __future__ import annotations

from typing import Optional

# Pydantic v2 preferred, v1 fallback
try:
    from pydantic import BaseModel, Field, ConfigDict  # type: ignore
    _PYDANTIC_V2 = True
except Exception:  # pragma: no cover
    from pydantic import BaseModel, Field  # type: ignore
    ConfigDict = None  # type: ignore
    _PYDANTIC_V2 = False


# =============================================================================
# Preferred: core.config (single source of truth)
# =============================================================================
try:
    from core.config import Settings as Settings  # type: ignore
    from core.config import get_settings as get_settings  # type: ignore

except Exception:  # pragma: no cover
    # =============================================================================
    # Fallback: repo-root config.py
    # =============================================================================
    try:
        from config import Settings as Settings  # type: ignore
        from config import get_settings as get_settings  # type: ignore

    except Exception:  # pragma: no cover
        # =============================================================================
        # Last-resort fallback: minimal Settings + get_settings
        # (keeps app bootable even if config modules are broken)
        # =============================================================================
        import os

        def _env_str(name: str, default: Optional[str] = None) -> Optional[str]:
            v = os.getenv(name)
            if v is None:
                return default
            v = str(v).strip()
            return v if v else default

        def _env_bool(name: str, default: bool = False) -> bool:
            v = _env_str(name, None)
            if v is None:
                return default
            return v.strip().lower() in ("1", "true", "yes", "y", "on")

        class Settings(BaseModel):  # type: ignore
            """
            Minimal settings model used only if imports fail.
            Keep fields limited to what routers commonly need.
            """
            if _PYDANTIC_V2:
                model_config = ConfigDict(extra="ignore")  # type: ignore
            else:  # pragma: no cover
                class Config:
                    extra = "ignore"

            # Auth
            app_token: Optional[str] = Field(default=None)
            backup_app_token: Optional[str] = Field(default=None)

            # App meta
            environment: str = Field(default="production")
            debug: bool = Field(default=False)

        _CACHED: Optional[Settings] = None

        def get_settings() -> Settings:  # type: ignore
            global _CACHED
            if _CACHED is not None:
                return _CACHED

            _CACHED = Settings(
                app_token=_env_str("APP_TOKEN") or _env_str("app_token"),
                backup_app_token=_env_str("BACKUP_APP_TOKEN") or _env_str("backup_app_token"),
                environment=_env_str("ENVIRONMENT", "production") or "production",
                debug=_env_bool("DEBUG", False),
            )
            return _CACHED


__all__ = ["Settings", "get_settings"]
