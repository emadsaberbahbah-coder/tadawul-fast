```python
# routes/config.py  (FULL REPLACEMENT)
"""
routes/config.py
------------------------------------------------------------
Compatibility shim for older route modules that do:
  from routes.config import Settings, get_settings

Goals
- Prefer core.config (canonical).
- Fall back to repo-root config.py.
- Never crash import-time. Provide minimal safe fallback.
- Keep this module lightweight: no heavy imports, no side effects beyond env reads.

Enhancements (v1.2.0)
- ✅ Works with Pydantic v2 or v1 (graceful)
- ✅ Adds a few commonly-used tuning fields (batch sizes/timeouts) in fallback
- ✅ Normalizes env parsing (truthy, ints, floats)
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
        _FALSY = {"0", "false", "no", "n", "off", "f"}

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
            s = v.strip().lower()
            if s in _TRUTHY:
                return True
            if s in _FALSY:
                return False
            return default

        def _env_int(name: str, default: int) -> int:
            v = _env_str(name, None)
            if v is None:
                return default
            try:
                n = int(str(v).strip())
                return n
            except Exception:
                return default

        def _env_float(name: str, default: float) -> float:
            v = _env_str(name, None)
            if v is None:
                return default
            try:
                n = float(str(v).strip())
                return n
            except Exception:
                return default

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

            # -----------------------------------------------------------------
            # Auth
            # -----------------------------------------------------------------
            app_token: Optional[str] = Field(default=None)
            backup_app_token: Optional[str] = Field(default=None)
            require_auth: bool = Field(default=False)

            # -----------------------------------------------------------------
            # App meta
            # -----------------------------------------------------------------
            app_name: str = Field(default="Tadawul Fast Bridge")
            version: str = Field(default="0.0.0")
            env: str = Field(default="production")
            debug: bool = Field(default=False)
            log_level: str = Field(default="info")

            # -----------------------------------------------------------------
            # URLs / Integrations
            # -----------------------------------------------------------------
            base_url: str = Field(default="")
            backend_base_url: str = Field(default="")
            spreadsheet_id: Optional[str] = Field(default=None)
            default_spreadsheet_id: Optional[str] = Field(default=None)
            google_apps_script_backup_url: str = Field(default="")

            # -----------------------------------------------------------------
            # Common route tuning knobs (used by ai_analysis / advanced_analysis)
            # -----------------------------------------------------------------
            ai_batch_size: int = Field(default=20)
            ai_batch_timeout_sec: float = Field(default=45.0)
            ai_batch_concurrency: int = Field(default=5)
            ai_max_tickers: int = Field(default=500)

            adv_batch_size: int = Field(default=25)
            adv_batch_timeout_sec: float = Field(default=45.0)
            adv_batch_concurrency: int = Field(default=6)
            adv_max_tickers: int = Field(default=500)

        _CACHED: Optional[Settings] = None

        def get_settings() -> Settings:  # type: ignore
            """
            Minimal env-backed settings.
            Used ONLY if both core.config and root config.py are unavailable.
            """
            global _CACHED
            if _CACHED is not None:
                return _CACHED

            # Auth envs (support legacy aliases)
            app_token = (
                _env_str("APP_TOKEN")
                or _env_str("TFB_APP_TOKEN")
                or _env_str("APPKEY")
                or _env_str("app_token")
            )
            backup_token = _env_str("BACKUP_APP_TOKEN") or _env_str("backup_app_token")

            # Spreadsheet id envs
            sid = (
                _env_str("SPREADSHEET_ID")
                or _env_str("DEFAULT_SPREADSHEET_ID")
                or _env_str("GOOGLE_SHEETS_ID")
            )

            _CACHED = Settings(
                app_token=app_token,
                backup_app_token=backup_token,
                require_auth=_env_bool("REQUIRE_AUTH", False),

                app_name=_env_str("APP_NAME", "Tadawul Fast Bridge") or "Tadawul Fast Bridge",
                version=_env_str("SERVICE_VERSION") or _env_str("APP_VERSION", "0.0.0") or "0.0.0",
                env=_env_str("APP_ENV") or _env_str("ENVIRONMENT", "production") or "production",
                debug=_env_bool("DEBUG", False),
                log_level=(_env_str("LOG_LEVEL", "info") or "info").lower(),

                base_url=(_env_str("BASE_URL", "") or "").strip(),
                backend_base_url=(_env_str("BACKEND_BASE_URL", "") or "").strip(),
                spreadsheet_id=sid,
                default_spreadsheet_id=sid,
                google_apps_script_backup_url=(_env_str("GOOGLE_APPS_SCRIPT_BACKUP_URL", "") or "").strip(),

                ai_batch_size=_env_int("AI_BATCH_SIZE", 20),
                ai_batch_timeout_sec=_env_float("AI_BATCH_TIMEOUT_SEC", 45.0),
                ai_batch_concurrency=_env_int("AI_BATCH_CONCURRENCY", 5),
                ai_max_tickers=_env_int("AI_MAX_TICKERS", 500),

                adv_batch_size=_env_int("ADV_BATCH_SIZE", 25),
                adv_batch_timeout_sec=_env_float("ADV_BATCH_TIMEOUT_SEC", 45.0),
                adv_batch_concurrency=_env_int("ADV_BATCH_CONCURRENCY", 6),
                adv_max_tickers=_env_int("ADV_MAX_TICKERS", 500),
            )
            return _CACHED


__all__ = ["Settings", "get_settings"]
```
