# core/config.py  (FULL REPLACEMENT)
"""
core/config.py
------------------------------------------------------------
Compatibility shim (PROD SAFE) – v1.3.0

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

v1.3.0 updates
- Aligns with your Render env-group keys (APP_ENV/ENVIRONMENT, REQUIRE_AUTH, ENABLE_RATE_LIMITING, etc.)
- Adds Google Sheets key alignment:
    GOOGLE_SHEETS_CREDENTIALS (preferred), fallback GOOGLE_CREDENTIALS / GOOGLE_SA_JSON
    SPREADSHEET_ID (preferred), fallback DEFAULT_SPREADSHEET_ID / GOOGLE_SHEETS_ID
- Adds common engine/runtime config knobs used across the repo:
    ENABLED_PROVIDERS / KSA_PROVIDERS, PRIMARY_PROVIDER
    HTTP_TIMEOUT_SEC, MAX_RETRIES, RETRY_DELAY_SEC
    TTL keys: CACHE_TTL_SEC, QUOTE_TTL_SEC, FUNDAMENTALS_TTL_SEC, ARGAAM_SNAPSHOT_TTL_SEC
    Feature flags: AI_ANALYSIS_ENABLED, ADVANCED_ANALYSIS_ENABLED
    KSA flags: ENABLE_YFINANCE_KSA, ENABLE_YAHOO_CHART_KSA
"""

from __future__ import annotations

import json
import os
from typing import Any, Dict, List, Optional

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
_TRUTHY = {"1", "true", "yes", "y", "on", "t"}


def _env_str(name: str, default: Optional[str] = None) -> Optional[str]:
    v = os.getenv(name)
    if v is None:
        return default
    v = v.strip()
    return v if v else default


def _env_str_any(names: List[str], default: Optional[str] = None) -> Optional[str]:
    for n in names:
        v = _env_str(n, None)
        if v is not None:
            return v
    return default


def _env_bool(name: str, default: bool = False) -> bool:
    v = _env_str(name, None)
    if v is None:
        return default
    return v.strip().lower() in _TRUTHY


def _env_bool_any(names: List[str], default: bool = False) -> bool:
    for n in names:
        v = _env_str(n, None)
        if v is None:
            continue
        return v.strip().lower() in _TRUTHY
    return default


def _env_int(name: str, default: int) -> int:
    v = _env_str(name, None)
    if v is None:
        return default
    try:
        n = int(v)
        return n if n > 0 else default
    except Exception:
        return default


def _env_int_any(names: List[str], default: int) -> int:
    for n in names:
        v = _env_str(n, None)
        if v is None:
            continue
        try:
            x = int(v)
            if x > 0:
                return x
        except Exception:
            continue
    return default


def _parse_list(value: Any) -> List[str]:
    """
    Accept:
      - python list
      - CSV string
      - JSON list string: '["eodhd","finnhub"]'
    Return: cleaned lower-case list (deduped, order preserved)
    """
    if value is None:
        return []

    items: List[str] = []

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


def _mask_tail(s: Optional[str], keep: int = 4) -> str:
    s2 = (s or "").strip()
    if not s2:
        return ""
    if len(s2) <= keep:
        return "•" * len(s2)
    return ("•" * (len(s2) - keep)) + s2[-keep:]


if Settings is None or get_settings is None:

    class Settings:  # type: ignore
        """
        Minimal Settings fallback.

        Exposes BOTH naming styles for compatibility:
          - app_name + APP_NAME
          - env + ENV
          - version + VERSION
          - log_level + LOG_LEVEL

        And common runtime knobs used by this repo.
        """

        # ---------------------------------------------------------------------
        # Identity
        # ---------------------------------------------------------------------
        app_name: str = _env_str_any(["APP_NAME", "SERVICE_NAME"], "Tadawul Fast Bridge") or "Tadawul Fast Bridge"

        env: str = (
            _env_str_any(["APP_ENV", "ENV", "ENVIRONMENT"], "production")
            or "production"
        )

        version: str = (
            _env_str_any(["SERVICE_VERSION", "APP_VERSION", "VERSION"], "dev")
            or "dev"
        )

        # ---------------------------------------------------------------------
        # Logging
        # ---------------------------------------------------------------------
        log_level: str = (_env_str_any(["LOG_LEVEL", "UVICORN_LOG_LEVEL"], "info") or "info").lower()
        log_format: str = _env_str("LOG_FORMAT", "%(asctime)s | %(levelname)s | %(name)s | %(message)s") or "%(asctime)s | %(levelname)s | %(name)s | %(message)s"

        # ---------------------------------------------------------------------
        # Auth / Security
        # ---------------------------------------------------------------------
        app_token: Optional[str] = _env_str_any(["APP_TOKEN", "TFB_APP_TOKEN"], None)
        backup_app_token: Optional[str] = _env_str_any(["BACKUP_APP_TOKEN"], None)

        # REQUIRE_AUTH is explicit; default false to preserve "open mode" if not configured
        require_auth: bool = _env_bool_any(["REQUIRE_AUTH"], False)

        # ---------------------------------------------------------------------
        # Providers
        # ---------------------------------------------------------------------
        enabled_providers: List[str] = _parse_list(
            _env_str_any(["ENABLED_PROVIDERS", "PROVIDERS"], "eodhd,finnhub,fmp,yahoo_chart")
        )
        enabled_ksa_providers: List[str] = _parse_list(_env_str("KSA_PROVIDERS", "tadawul,argaam,yahoo_chart"))

        primary_provider: str = (_env_str_any(["PRIMARY_PROVIDER"], "") or "").strip().lower()

        # KSA policy flags
        enable_yfinance_ksa: bool = _env_bool("ENABLE_YFINANCE_KSA", False)
        enable_yahoo_chart_ksa: bool = _env_bool("ENABLE_YAHOO_CHART_KSA", True)

        # ---------------------------------------------------------------------
        # HTTP / retry controls
        # ---------------------------------------------------------------------
        http_timeout_sec: int = _env_int_any(["HTTP_TIMEOUT_SEC", "HTTP_TIMEOUT"], 25)
        max_retries: int = _env_int_any(["MAX_RETRIES"], 2)
        retry_delay_sec: int = _env_int_any(["RETRY_DELAY_SEC", "RETRY_DELAY"], 1)

        # ---------------------------------------------------------------------
        # Cache / TTL controls (seconds)
        # ---------------------------------------------------------------------
        cache_ttl_sec: int = _env_int_any(["CACHE_TTL_SEC"], 20)
        quote_ttl_sec: int = _env_int_any(["QUOTE_TTL_SEC"], 30)
        fundamentals_ttl_sec: int = _env_int_any(["FUNDAMENTALS_TTL_SEC"], 21600)
        argaam_snapshot_ttl_sec: int = _env_int_any(["ARGAAM_SNAPSHOT_TTL_SEC"], 30)

        # ---------------------------------------------------------------------
        # Feature flags
        # ---------------------------------------------------------------------
        ai_analysis_enabled: bool = _env_bool("AI_ANALYSIS_ENABLED", True)
        advanced_analysis_enabled: bool = _env_bool("ADVANCED_ANALYSIS_ENABLED", True)

        # ---------------------------------------------------------------------
        # CORS
        # ---------------------------------------------------------------------
        enable_cors_all_origins: bool = _env_bool_any(["ENABLE_CORS_ALL_ORIGINS", "CORS_ALL_ORIGINS"], True)
        cors_origins_list: List[str] = (
            ["*"]
            if enable_cors_all_origins
            else _parse_list(_env_str_any(["CORS_ORIGINS", "CORS_ALLOW_ORIGINS"], "") or "")
        )

        # ---------------------------------------------------------------------
        # Rate limiting (SlowAPI)
        # ---------------------------------------------------------------------
        enable_rate_limiting: bool = _env_bool_any(["ENABLE_RATE_LIMITING"], True)
        rate_limit_per_minute: int = _env_int_any(["RATE_LIMIT_PER_MINUTE"], 240)

        # Keep old names too (legacy compatibility)
        slowapi_enabled: bool = _env_bool_any(["SLOWAPI_ENABLED"], True)
        slowapi_default_limit: str = _env_str("SLOWAPI_DEFAULT_LIMIT", f"{rate_limit_per_minute}/minute") or f"{rate_limit_per_minute}/minute"

        # ---------------------------------------------------------------------
        # API keys
        # ---------------------------------------------------------------------
        eodhd_api_key: Optional[str] = _env_str("EODHD_API_KEY", None)
        finnhub_api_key: Optional[str] = _env_str("FINNHUB_API_KEY", None)
        fmp_api_key: Optional[str] = _env_str("FMP_API_KEY", None)
        alpha_vantage_api_key: Optional[str] = _env_str("ALPHA_VANTAGE_API_KEY", None)
        twelvedata_api_key: Optional[str] = _env_str("TWELVEDATA_API_KEY", None)
        marketstack_api_key: Optional[str] = _env_str("MARKETSTACK_API_KEY", None)

        # ---------------------------------------------------------------------
        # URLs / Integration
        # ---------------------------------------------------------------------
        base_url: str = (_env_str_any(["BASE_URL", "BACKEND_BASE_URL"], "") or "").strip()
        backend_base_url: str = (_env_str_any(["BACKEND_BASE_URL", "BASE_URL"], "") or "").strip()

        google_apps_script_url: str = (_env_str("GOOGLE_APPS_SCRIPT_URL", "") or "").strip()
        google_apps_script_backup_url: str = (_env_str("GOOGLE_APPS_SCRIPT_BACKUP_URL", "") or "").strip()

        # ---------------------------------------------------------------------
        # Google / Sheets (critical alignment)
        # ---------------------------------------------------------------------
        # Preferred key in this repo:
        google_sheets_credentials: Optional[str] = _env_str("GOOGLE_SHEETS_CREDENTIALS", None)

        # Backward-compatible fallbacks (some earlier scripts used these):
        google_credentials: Optional[str] = _env_str("GOOGLE_CREDENTIALS", None)
        google_sa_json: Optional[str] = _env_str("GOOGLE_SA_JSON", None)

        spreadsheet_id: Optional[str] = _env_str_any(["SPREADSHEET_ID", "DEFAULT_SPREADSHEET_ID"], None)
        google_sheets_id: Optional[str] = _env_str("GOOGLE_SHEETS_ID", None)

        # ---------------------------------------------------------------------
        # Helpful “safe” summary for diagnostics (no secret leaks)
        # ---------------------------------------------------------------------
        def safe_summary(self) -> Dict[str, Any]:
            return {
                "APP_ENV": self.env,
                "APP_NAME": self.app_name,
                "VERSION": self.version,
                "LOG_LEVEL": self.log_level,
                "REQUIRE_AUTH": bool(self.require_auth),
                "APP_TOKEN_SET": bool(self.app_token),
                "BACKUP_APP_TOKEN_SET": bool(self.backup_app_token),
                "APP_TOKEN_MASK": _mask_tail(self.app_token, keep=4),
                "ENABLED_PROVIDERS": list(self.enabled_providers),
                "KSA_PROVIDERS": list(self.enabled_ksa_providers),
                "PRIMARY_PROVIDER": self.primary_provider,
                "AI_ANALYSIS_ENABLED": bool(self.ai_analysis_enabled),
                "ADVANCED_ANALYSIS_ENABLED": bool(self.advanced_analysis_enabled),
                "ENABLE_YFINANCE_KSA": bool(self.enable_yfinance_ksa),
                "ENABLE_YAHOO_CHART_KSA": bool(self.enable_yahoo_chart_ksa),
                "HTTP_TIMEOUT_SEC": int(self.http_timeout_sec),
                "MAX_RETRIES": int(self.max_retries),
                "RETRY_DELAY_SEC": int(self.retry_delay_sec),
                "CACHE_TTL_SEC": int(self.cache_ttl_sec),
                "QUOTE_TTL_SEC": int(self.quote_ttl_sec),
                "FUNDAMENTALS_TTL_SEC": int(self.fundamentals_ttl_sec),
                "ARGAAM_SNAPSHOT_TTL_SEC": int(self.argaam_snapshot_ttl_sec),
                "ENABLE_RATE_LIMITING": bool(self.enable_rate_limiting),
                "RATE_LIMIT_PER_MINUTE": int(self.rate_limit_per_minute),
                "CORS_ALL": bool(self.enable_cors_all_origins),
                "CORS_ORIGINS": ["*"] if self.enable_cors_all_origins else list(self.cors_origins_list),
                "SPREADSHEET_ID_SET": bool(self.spreadsheet_id or self.google_sheets_id),
                "GOOGLE_SHEETS_CREDENTIALS_SET": bool(self.google_sheets_credentials),
                "GOOGLE_CREDENTIALS_SET": bool(self.google_credentials or self.google_sa_json),
                "GOOGLE_APPS_SCRIPT_BACKUP_URL_SET": bool(self.google_apps_script_backup_url),
            }

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

        @property
        def GOOGLE_SHEETS_CREDENTIALS(self) -> Optional[str]:  # noqa: N802
            return self.google_sheets_credentials

        @property
        def SPREADSHEET_ID(self) -> Optional[str]:  # noqa: N802
            return self.spreadsheet_id or self.google_sheets_id

    _settings_singleton: Optional[Settings] = None

    def get_settings() -> Settings:  # type: ignore
        global _settings_singleton
        if _settings_singleton is None:
            _settings_singleton = Settings()
        return _settings_singleton


__all__ = ["Settings", "get_settings"]
