# routes/config.py
"""
routes/config.py
------------------------------------------------------------
Compatibility shim for older route modules that do:
  from routes.config import Settings, get_settings

Goals
- Prefer core.config (canonical) to keep settings centralized.
- Fall back to repo-root config.py to support older layouts.
- Never crash at import-time: if both imports fail, provide a minimal safe fallback
  so the app can still boot and routers can mount.

Hardening / Enhancements (v1.3.0)
- Works with Pydantic v2 (preferred) and v1 (fallback) without breaking import-time.
- Safe env parsing helpers (str/bool/int/float).
- Cache get_settings() for speed and stability (Render cold start friendly).
- Keeps the fallback Settings minimal (routers need auth + a few limits).
- Supports both uppercase and lowercase env keys (APP_TOKEN / app_token).
- Does NOT import any heavy modules or routers.
"""

from __future__ import annotations

from typing import Optional

# =============================================================================
# Pydantic compatibility (v2 preferred, v1 fallback)
# =============================================================================
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

        # -------------------------
        # Env parsing helpers
        # -------------------------
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
            return str(v).strip().lower() in ("1", "true", "yes", "y", "on")

        def _env_int(name: str, default: int) -> int:
            v = _env_str(name, None)
            if v is None:
                return default
            try:
                x = int(str(v).strip())
                return x
            except Exception:
                return default

        def _env_float(name: str, default: float) -> float:
            v = _env_str(name, None)
            if v is None:
                return default
            try:
                x = float(str(v).strip())
                return x
            except Exception:
                return default

        # -------------------------
        # Minimal Settings used only if imports fail
        # -------------------------
        class Settings(BaseModel):  # type: ignore
            """
            Minimal settings model used ONLY if core/root config imports fail.
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

            # Common router limits (optional; safe defaults)
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
            Cached settings loader for last-resort mode.
            Supports both uppercase and lowercase env keys.
            """
            global _CACHED
            if _CACHED is not None:
                return _CACHED

            _CACHED = Settings(
                # Auth (both styles)
                app_token=_env_str("APP_TOKEN") or _env_str("app_token"),
                backup_app_token=_env_str("BACKUP_APP_TOKEN") or _env_str("backup_app_token"),
                # Meta
                environment=_env_str("ENVIRONMENT") or _env_str("environment") or "production",
                debug=_env_bool("DEBUG", False) or _env_bool("debug", False),
                # Limits (accept both styles)
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
