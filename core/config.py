# core/config.py
"""
core/config.py
------------------------------------------------------------
Compatibility shim (stable)

Many modules import settings via:
  from core.config import Settings, get_settings

Single source of truth is the repo-root `config.py`.
This module re-exports Settings/get_settings so old imports keep working.
"""

from __future__ import annotations

import os
from typing import Any, Optional, List


# =============================================================================
# Preferred: repo root config.py
# =============================================================================
try:
    from config import Settings as Settings  # type: ignore
    from config import get_settings as get_settings  # type: ignore

except Exception:  # pragma: no cover
    # Defensive fallback: minimal Settings + env parsing

    def _env_str(name: str, default: Optional[str] = None) -> Optional[str]:
        v = os.getenv(name)
        if v is None:
            return default
        v = v.strip()
        return v if v else default

    def _env_bool(name: str, default: bool = False) -> bool:
        v = _env_str(name)
        if v is None:
            return default
        return v.lower() in ("1", "true", "yes", "y", "on")

    def _env_csv(name: str, default: str = "") -> List[str]:
        v = _env_str(name, default) or ""
        return [x.strip() for x in v.split(",") if x.strip()]

    class Settings:  # type: ignore
        APP_NAME: str = _env_str("APP_NAME", "Tadawul Fast Bridge") or "Tadawul Fast Bridge"
        ENV: str = _env_str("ENV", _env_str("ENVIRONMENT", "production") or "production") or "production"
        VERSION: str = _env_str("VERSION", "unknown") or "unknown"

        LOG_LEVEL: str = _env_str("LOG_LEVEL", "INFO") or "INFO"
        CORS_ALLOW_ORIGINS: List[str] = _env_csv("CORS_ALLOW_ORIGINS", "*")

        PROVIDERS: List[str] = _env_csv("PROVIDERS", "eodhd,finnhub")
        KSA_PROVIDERS: List[str] = _env_csv("KSA_PROVIDERS", "tadawul,argaam")

        EODHD_API_KEY: Optional[str] = _env_str("EODHD_API_KEY")
        FINNHUB_API_KEY: Optional[str] = _env_str("FINNHUB_API_KEY")
        FMP_API_KEY: Optional[str] = _env_str("FMP_API_KEY")

        SLOWAPI_ENABLED: bool = _env_bool("SLOWAPI_ENABLED", True)
        SLOWAPI_DEFAULT_LIMIT: str = _env_str("SLOWAPI_DEFAULT_LIMIT", "240/minute") or "240/minute"

        # Google / Sheets (optional)
        GOOGLE_SA_JSON: Optional[str] = _env_str("GOOGLE_SA_JSON")
        GOOGLE_SHEETS_ID: Optional[str] = _env_str("GOOGLE_SHEETS_ID")

    _settings_singleton: Optional[Settings] = None

    def get_settings() -> Settings:  # type: ignore
        global _settings_singleton
        if _settings_singleton is None:
            _settings_singleton = Settings()
        return _settings_singleton
