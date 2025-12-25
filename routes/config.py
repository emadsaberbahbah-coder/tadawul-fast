# routes/config.py  (FULL REPLACEMENT)
"""
routes/config.py
------------------------------------------------------------
Compatibility shim for older route modules that do:
  from routes.config import Settings, get_settings

Goals
- Prefer core.config (canonical).
- Fall back to root config.py.
- Never crash import-time. Provide minimal safe fallback.

Notes
- Keep this module lightweight: no heavy imports.
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
        # =============================================================================
        import os

        _TRUTHY = {"1", "true", "yes", "y", "on", "t"}

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
            return v.strip().lower() in _TRUTHY

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
            require_auth: bool = Field(default=False)

            # App meta
            app_name: str = Field(default="Tadawul Fast Bridge")
            version: str = Field(default="0.0.0")
            env: str = Field(default="production")
            debug: bool = Field(default=False)
            log_level: str = Field(default="info")

            # Integrations (commonly referenced)
            base_url: str = Field(default="")
            backend_base_url: str = Field(default="")
            spreadsheet_id: Optional[str] = Field(default=None)
            google_apps_script_backup_url: str = Field(default="")

        _CACHED: Optional[Settings] = None

        def get_settings() -> Settings:  # type: ignore
            """
            Minimal env-backed settings.
            This is only used if both core.config and root config.py are unavailable.
            """
            global _CACHED
            if _CACHED is not None:
                return _CACHED

            _CACHED = Settings(
                app_token=_env_str("APP_TOKEN") or _env_str("TFB_APP_TOKEN") or _env_str("app_token"),
                backup_app_token=_env_str("BACKUP_APP_TOKEN") or _env_str("backup_app_token"),
                require_auth=_env_bool("REQUIRE_AUTH", False),

                app_name=_env_str("APP_NAME", "Tadawul Fast Bridge") or "Tadawul Fast Bridge",
                version=_env_str("SERVICE_VERSION") or _env_str("APP_VERSION", "0.0.0") or "0.0.0",
                env=_env_str("APP_ENV") or _env_str("ENVIRONMENT", "production") or "production",
                debug=_env_bool("DEBUG", False),
                log_level=(_env_str("LOG_LEVEL", "info") or "info").lower(),

                base_url=(_env_str("BASE_URL", "") or "").strip(),
                backend_base_url=(_env_str("BACKEND_BASE_URL", "") or "").strip(),
                spreadsheet_id=_env_str("SPREADSHEET_ID") or _env_str("DEFAULT_SPREADSHEET_ID") or _env_str("GOOGLE_SHEETS_ID"),
                google_apps_script_backup_url=(_env_str("GOOGLE_APPS_SCRIPT_BACKUP_URL", "") or "").strip(),
            )
            return _CACHED


__all__ = ["Settings", "get_settings"]
