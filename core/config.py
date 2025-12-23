# core/config.py
"""
core/config.py
------------------------------------------------------------
Compatibility shim (PROD SAFE) – v1.2.0

Many modules import settings via:
  from core.config import Settings, get_settings

Single source of truth is the repo-root `config.py`.
This module re-exports Settings/get_settings so old imports keep working.

Design goals:
- Never break app boot if root config.py is missing or mid-refactor.
- Provide a minimal, attribute-friendly Settings fallback.
- Support BOTH naming styles:
    - lower_case: app_name, env, version, log_level, ...
    - UPPER_CASE: APP_NAME, ENV, VERSION, LOG_LEVEL, ...
"""

from __future__ import annotations

import json
import os
from typing import Any, List, Optional

# -----------------------------------------------------------------------------
# Try to use repo-root config.py (preferred)
# -----------------------------------------------------------------------------
Settings = None  # type: ignore
get_settings = None  # type: ignore

try:
    import config as _root_config  # type: ignore

    if hasattr(_root_config, "Settings"):
        Settings = getattr(_root_config, "Settings")

    if hasattr(_root_config, "get_settings"):
        get_settings = getattr(_root_config, "get_settings")

except Exception:
    Settings = None  # type: ignore
    get_settings = None  # type: ignore


# -----------------------------------------------------------------------------
# Fallback implementation (only used if root exports are unavailable)
# -----------------------------------------------------------------------------
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


def _env_int(name: str, default: int) -> int:
    v = _env_str(name)
    if v is None:
        return default
    try:
        n = int(v)
        return n if n > 0 else default
    except Exception:
        return default


def _parse_list(value: Any) -> List[str]:
    """
    Accept:
      - python list
      - CSV string
      - JSON list string: '["eodhd","finnhub"]'
    Return: cleaned lower-case list (deduped, order preserved)
    """
    items: List[str] = []

    if value is None:
        return []

    if isinstance(value, list):
        items = [str(x).strip() for x in value if str(x).strip()]
    else:
        s = str(value).strip()
        if not s:
            return []
        if s.startswith("[") and s.endswith("]"):
            try:
                arr = json.loads(s)
                if isinstance(arr, list):
                    items = [str(x).strip() for x in arr if str(x).strip()]
                else:
                    items = [p.strip() for p in s.split(",") if p.strip()]
            except Exception:
                items = [p.strip() for p in s.split(",") if p.strip()]
        else:
            items = [p.strip() for p in s.split(",") if p.strip()]

    out: List[str] = []
    seen = set()
    for x in items:
        y = x.strip()
        if not y:
            continue
        yl = y.lower()
        if yl in seen:
            continue
        seen.add(yl)
        out.append(yl)
    return out


def _env_list(name: str, default: str = "") -> List[str]:
    return _parse_list(_env_str(name, default))


if Settings is None or get_settings is None:

    class Settings:  # type: ignore
        """
        Minimal Settings fallback.

        Exposes BOTH naming styles for compatibility:
          - app_name + APP_NAME
          - env + ENV
          - version + VERSION
          - log_level + LOG_LEVEL
          - enabled_providers + PROVIDERS
          - enabled_ksa_providers + KSA_PROVIDERS
          - cors_origins_list + CORS_ALLOW_ORIGINS
        """

        # ---- identity
        app_name: str = _env_str("APP_NAME", "Tadawul Fast Bridge") or "Tadawul Fast Bridge"
        env: str = (
            _env_str("APP_ENV")
            or _env_str("ENV")
            or _env_str("ENVIRONMENT")
            or "production"
        )
        version: str = _env_str("APP_VERSION", _env_str("VERSION", "unknown") or "unknown") or "unknown"

        # ---- logging
        log_level: str = (_env_str("LOG_LEVEL", "info") or "info").lower()

        # ---- auth
        app_token: Optional[str] = _env_str("APP_TOKEN")
        backup_app_token: Optional[str] = _env_str("BACKUP_APP_TOKEN")

        # ---- providers
        # support either PROVIDERS/ENABLED_PROVIDERS naming
        enabled_providers: List[str] = _parse_list(_env_str("ENABLED_PROVIDERS") or _env_str("PROVIDERS", "eodhd,finnhub"))
        enabled_ksa_providers: List[str] = _parse_list(_env_str("KSA_PROVIDERS", "tadawul,argaam"))

        # ---- CORS
        enable_cors_all_origins: bool = _env_bool("ENABLE_CORS_ALL_ORIGINS", _env_bool("CORS_ALL_ORIGINS", True))
        cors_origins_list: List[str] = (
            ["*"]
            if enable_cors_all_origins
            else _parse_list(_env_str("CORS_ORIGINS", _env_str("CORS_ALLOW_ORIGINS", "")) or "")
        )

        # ---- rate limiting
        # if you prefer per-minute integer, use RATE_LIMIT_PER_MINUTE
        rate_limit_per_minute: int = _env_int("RATE_LIMIT_PER_MINUTE", 240)

        # keep old slowapi env names too
        slowapi_enabled: bool = _env_bool("SLOWAPI_ENABLED", True)
        slowapi_default_limit: str = _env_str("SLOWAPI_DEFAULT_LIMIT", f"{rate_limit_per_minute}/minute") or f"{rate_limit_per_minute}/minute"

        # ---- API keys
        eodhd_api_key: Optional[str] = _env_str("EODHD_API_KEY")
        finnhub_api_key: Optional[str] = _env_str("FINNHUB_API_KEY")
        fmp_api_key: Optional[str] = _env_str("FMP_API_KEY")

        # ---- Google/Sheets (optional)
        google_sa_json: Optional[str] = _env_str("GOOGLE_SA_JSON")
        google_sheets_id: Optional[str] = _env_str("GOOGLE_SHEETS_ID")

        # ---------------------------------------------------------------------
        # UPPERCASE aliases (legacy compatibility)
        # ---------------------------------------------------------------------
        @property
        def APP_NAME(self) -> str:  # noqa: N802
            return self.app_name

        @property
        def ENV(self) -> str:  # noqa: N802
            return self.env

        @property
        def VERSION(self) -> str:  # noqa: N802
            return self.version

        @property
        def LOG_LEVEL(self) -> str:  # noqa: N802
            return self.log_level

        @property
        def PROVIDERS(self) -> List[str]:  # noqa: N802
            return list(self.enabled_providers)

        @property
        def KSA_PROVIDERS(self) -> List[str]:  # noqa: N802
            return list(self.enabled_ksa_providers)

        @property
        def CORS_ALLOW_ORIGINS(self) -> List[str]:  # noqa: N802
            return list(self.cors_origins_list)

        @property
        def EODHD_API_KEY(self) -> Optional[str]:  # noqa: N802
            return self.eodhd_api_key

        @property
        def FINNHUB_API_KEY(self) -> Optional[str]:  # noqa: N802
            return self.finnhub_api_key

        @property
        def FMP_API_KEY(self) -> Optional[str]:  # noqa: N802
            return self.fmp_api_key

        @property
        def SLOWAPI_ENABLED(self) -> bool:  # noqa: N802
            return self.slowapi_enabled

        @property
        def SLOWAPI_DEFAULT_LIMIT(self) -> str:  # noqa: N802
            return self.slowapi_default_limit

    _settings_singleton: Optional[Settings] = None

    def get_settings() -> Settings:  # type: ignore
        global _settings_singleton
        if _settings_singleton is None:
            _settings_singleton = Settings()
        return _settings_singleton


__all__ = ["Settings", "get_settings"]
