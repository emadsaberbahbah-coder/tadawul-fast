```python
# config.py  (REPO ROOT)  — FULL REPLACEMENT
"""
config.py
============================================================
Canonical Settings for Tadawul Fast Bridge (ROOT)

Why this file matters
- This is the **single source of truth** for environment variables.
- core.config and routes.config should import Settings/get_settings from here.
- Fixes the common mismatch you have right now:
    ✅ FINNHUB_API_KEY (Render)  -> finnhub_api_token (code expects token)
    ✅ EODHD_API_KEY   (Render)  -> eodhd_api_token   (code expects token)

Design goals
- Never crash on import-time due to missing optional deps.
- Prefer Pydantic v2 + pydantic-settings (your current stack).
- Provide a safe fallback if pydantic-settings is unavailable.

Version: v5.2.0 (KSA/GLOBAL env-alias hardening)
"""

from __future__ import annotations

from functools import lru_cache
from typing import List, Optional, Any, Dict
import os


# -----------------------------------------------------------------------------
# Helpers (no heavy imports, safe parsing)
# -----------------------------------------------------------------------------
_TRUTHY = {"1", "true", "yes", "y", "on", "t"}
_FALSY = {"0", "false", "no", "n", "off", "f"}


def _to_bool(v: Any, default: bool = False) -> bool:
    if v is None:
        return default
    if isinstance(v, bool):
        return v
    s = str(v).strip().lower()
    if s in _TRUTHY:
        return True
    if s in _FALSY:
        return False
    return default


def _to_int(v: Any, default: int) -> int:
    try:
        if v is None or str(v).strip() == "":
            return default
        return int(str(v).strip())
    except Exception:
        return default


def _to_float(v: Any, default: float) -> float:
    try:
        if v is None or str(v).strip() == "":
            return default
        return float(str(v).strip())
    except Exception:
        return default


def _csv(v: Any, *, lower: bool = True) -> List[str]:
    if v is None:
        return []
    s = str(v).strip()
    if not s:
        return []
    parts = [p.strip() for p in s.split(",")]
    parts = [p for p in parts if p]
    if lower:
        parts = [p.lower() for p in parts]
    return parts


def _first_env(*names: str) -> Optional[str]:
    for n in names:
        val = os.getenv(n)
        if val is None:
            continue
        s = str(val).strip()
        if s:
            return s
    return None


# =============================================================================
# Preferred: Pydantic v2 + pydantic-settings
# =============================================================================
try:
    from pydantic import Field  # type: ignore
    from pydantic_settings import BaseSettings, SettingsConfigDict  # type: ignore

    try:
        # Pydantic v2 helper (nice-to-have). If missing, we’ll fall back to first alias.
        from pydantic import AliasChoices  # type: ignore

        def _alias(*names: str) -> Any:
            return AliasChoices(*names)

    except Exception:  # pragma: no cover
        AliasChoices = None  # type: ignore

        def _alias(*names: str) -> Any:
            return names[0]

    class Settings(BaseSettings):  # type: ignore
        """
        Canonical env-backed settings model.
        IMPORTANT: We keep field names stable and map Render env variable names via aliases.
        """

        model_config = SettingsConfigDict(
            extra="ignore",
            case_sensitive=False,
        )

        # ---------------------------------------------------------------------
        # App / Meta
        # ---------------------------------------------------------------------
        service_name: str = Field(default="Tadawul Stock Analysis API", validation_alias=_alias("SERVICE_NAME", "APP_NAME"))
        service_version: str = Field(default="0.0.0", validation_alias=_alias("SERVICE_VERSION", "APP_VERSION", "VERSION"))
        environment: str = Field(default="production", validation_alias=_alias("ENVIRONMENT", "APP_ENV", "ENV"))
        tz: str = Field(default="Asia/Riyadh", validation_alias=_alias("TZ", "TIMEZONE"))
        debug: bool = Field(default=False, validation_alias=_alias("DEBUG"))
        log_level: str = Field(default="info", validation_alias=_alias("LOG_LEVEL"))

        # ---------------------------------------------------------------------
        # Auth
        # ---------------------------------------------------------------------
        require_auth: bool = Field(default=False, validation_alias=_alias("REQUIRE_AUTH"))
        app_token: Optional[str] = Field(default=None, validation_alias=_alias("APP_TOKEN", "TFB_APP_TOKEN", "X_APP_TOKEN"))
        backup_app_token: Optional[str] = Field(default=None, validation_alias=_alias("BACKUP_APP_TOKEN"))

        # ---------------------------------------------------------------------
        # Providers (GLOBAL + KSA)
        # ---------------------------------------------------------------------
        enabled_providers_raw: str = Field(
            default="eodhd,finnhub",
            validation_alias=_alias("ENABLED_PROVIDERS", "PROVIDERS"),
        )
        primary_provider: str = Field(
            default="eodhd",
            validation_alias=_alias("PRIMARY_PROVIDER"),
        )
        ksa_providers_raw: str = Field(
            default="yahoo_chart,tadawul,argaam",
            validation_alias=_alias("KSA_PROVIDERS", "KSA_ENABLED_PROVIDERS"),
        )

        # ---------------------------------------------------------------------
        # Provider tokens / keys  (✅ map Render names to code-expected names)
        # ---------------------------------------------------------------------
        eodhd_api_token: Optional[str] = Field(
            default=None,
            validation_alias=_alias("EODHD_API_KEY", "EODHD_API_TOKEN", "EODHD_TOKEN"),
        )
        finnhub_api_token: Optional[str] = Field(
            default=None,
            validation_alias=_alias("FINNHUB_API_KEY", "FINNHUB_API_TOKEN", "FINNHUB_TOKEN"),
        )

        # Optional other providers (kept for future expansion)
        fmp_api_key: Optional[str] = Field(default=None, validation_alias=_alias("FMP_API_KEY"))
        twelvedata_api_key: Optional[str] = Field(default=None, validation_alias=_alias("TWELVEDATA_API_KEY"))
        alpha_vantage_api_key: Optional[str] = Field(default=None, validation_alias=_alias("ALPHA_VANTAGE_API_KEY"))
        marketstack_api_key: Optional[str] = Field(default=None, validation_alias=_alias("MARKETSTACK_API_KEY"))

        # ---------------------------------------------------------------------
        # Base URLs / Integration
        # ---------------------------------------------------------------------
        backend_base_url: Optional[str] = Field(default=None, validation_alias=_alias("BACKEND_BASE_URL"))
        base_url: Optional[str] = Field(default=None, validation_alias=_alias("BASE_URL"))

        eodhd_base_url: str = Field(
            default="https://eodhistoricaldata.com/api",
            validation_alias=_alias("EODHD_BASE_URL"),
        )
        finnhub_base_url: str = Field(
            default="https://finnhub.io/api/v1",
            validation_alias=_alias("FINNHUB_BASE_URL"),
        )
        fmp_base_url: Optional[str] = Field(default=None, validation_alias=_alias("FMP_BASE_URL"))

        http_timeout: float = Field(default=30.0, validation_alias=_alias("HTTP_TIMEOUT"))
        max_retries: int = Field(default=2, validation_alias=_alias("MAX_RETRIES"))
        retry_delay: float = Field(default=0.5, validation_alias=_alias("RETRY_DELAY"))

        # ---------------------------------------------------------------------
        # CORS / Rate limiting
        # ---------------------------------------------------------------------
        cors_origins: str = Field(default="*", validation_alias=_alias("CORS_ORIGINS"))
        enable_rate_limiting: bool = Field(default=True, validation_alias=_alias("ENABLE_RATE_LIMITING"))
        max_requests_per_minute: int = Field(default=240, validation_alias=_alias("MAX_REQUESTS_PER_MINUTE"))

        # ---------------------------------------------------------------------
        # Cache tuning
        # ---------------------------------------------------------------------
        cache_default_ttl: int = Field(default=10, validation_alias=_alias("CACHE_DEFAULT_TTL"))
        cache_max_size: int = Field(default=5000, validation_alias=_alias("CACHE_MAX_SIZE"))
        cache_backup_enabled: bool = Field(default=False, validation_alias=_alias("CACHE_BACKUP_ENABLED"))
        cache_save_interval: int = Field(default=300, validation_alias=_alias("CACHE_SAVE_INTERVAL"))

        # ---------------------------------------------------------------------
        # Google Sheets / Apps Script
        # ---------------------------------------------------------------------
        default_spreadsheet_id: Optional[str] = Field(default=None, validation_alias=_alias("DEFAULT_SPREADSHEET_ID", "SPREADSHEET_ID"))
        google_sheets_credentials: Optional[str] = Field(
            default=None,
            validation_alias=_alias("GOOGLE_SHEETS_CREDENTIALS", "GOOGLE_CREDENTIALS"),
        )
        google_apps_script_url: Optional[str] = Field(default=None, validation_alias=_alias("GOOGLE_APPS_SCRIPT_URL"))
        google_apps_script_backup_url: Optional[str] = Field(default=None, validation_alias=_alias("GOOGLE_APPS_SCRIPT_BACKUP_URL"))

        # ---------------------------------------------------------------------
        # Feature flags
        # ---------------------------------------------------------------------
        advanced_analysis_enabled: bool = Field(default=True, validation_alias=_alias("ADVANCED_ANALYSIS_ENABLED"))
        tadawul_market_enabled: bool = Field(default=True, validation_alias=_alias("TADAWUL_MARKET_ENABLED"))

        # ---------------------------------------------------------------------
        # KSA routing config (optional URLs)
        # ---------------------------------------------------------------------
        tadawul_quote_url: Optional[str] = Field(default=None, validation_alias=_alias("TADAWUL_QUOTE_URL"))
        tadawul_fundamentals_url: Optional[str] = Field(default=None, validation_alias=_alias("TADAWUL_FUNDAMENTALS_URL"))

        argaam_quote_url: Optional[str] = Field(default=None, validation_alias=_alias("ARGAAM_QUOTE_URL"))
        argaam_profile_url: Optional[str] = Field(default=None, validation_alias=_alias("ARGAAM_PROFILE_URL"))

        # ---------------------------------------------------------------------
        # Derived helpers (no validation needed)
        # ---------------------------------------------------------------------
        @property
        def enabled_providers(self) -> List[str]:
            # Use raw env string and normalize
            providers = _csv(self.enabled_providers_raw, lower=True)
            return providers or ["eodhd", "finnhub"]

        @property
        def ksa_providers(self) -> List[str]:
            providers = _csv(self.ksa_providers_raw, lower=True)
            return providers or ["yahoo_chart", "tadawul", "argaam"]

        @property
        def cors_origins_list(self) -> List[str]:
            s = (self.cors_origins or "").strip()
            if not s or s == "*":
                return ["*"]
            return [x.strip() for x in s.split(",") if x.strip()]

        def as_safe_dict(self) -> Dict[str, Any]:
            """
            A safe-to-log snapshot (no secrets).
            """
            return {
                "service_name": self.service_name,
                "service_version": self.service_version,
                "environment": self.environment,
                "tz": self.tz,
                "debug": bool(self.debug),
                "log_level": (self.log_level or "info").lower(),
                "require_auth": bool(self.require_auth),
                "enabled_providers": self.enabled_providers,
                "primary_provider": (self.primary_provider or "").lower(),
                "ksa_providers": self.ksa_providers,
                "cache_default_ttl": int(self.cache_default_ttl),
                "http_timeout": float(self.http_timeout),
                "default_spreadsheet_id_set": bool(self.default_spreadsheet_id),
                "google_creds_set": bool(self.google_sheets_credentials),
                "eodhd_token_set": bool(self.eodhd_api_token),
                "finnhub_token_set": bool(self.finnhub_api_token),
                "tadawul_urls_set": bool(self.tadawul_quote_url or self.tadawul_fundamentals_url),
                "argaam_urls_set": bool(self.argaam_quote_url or self.argaam_profile_url),
            }

    @lru_cache(maxsize=1)
    def get_settings() -> Settings:
        # BaseSettings will read env values according to aliases above.
        s = Settings()

        # Normalize log level
        if s.log_level:
            s.log_level = str(s.log_level).strip().lower()

        # Be forgiving with weird bool strings from Render
        s.debug = _to_bool(s.debug, False)
        s.require_auth = _to_bool(s.require_auth, False)
        s.enable_rate_limiting = _to_bool(s.enable_rate_limiting, True)
        s.cache_backup_enabled = _to_bool(s.cache_backup_enabled, False)
        s.advanced_analysis_enabled = _to_bool(s.advanced_analysis_enabled, True)
        s.tadawul_market_enabled = _to_bool(s.tadawul_market_enabled, True)

        # Normalize numeric fields safely
        s.cache_default_ttl = _to_int(s.cache_default_ttl, 10)
        s.cache_max_size = _to_int(s.cache_max_size, 5000)
        s.cache_save_interval = _to_int(s.cache_save_interval, 300)
        s.max_requests_per_minute = _to_int(s.max_requests_per_minute, 240)
        s.max_retries = _to_int(s.max_retries, 2)
        s.http_timeout = _to_float(s.http_timeout, 30.0)
        s.retry_delay = _to_float(s.retry_delay, 0.5)

        # Normalize providers
        s.primary_provider = (s.primary_provider or "eodhd").strip().lower()

        return s

except Exception:  # pragma: no cover
    # =============================================================================
    # LAST-RESORT FALLBACK (no pydantic-settings available)
    # =============================================================================
    from pydantic import BaseModel  # type: ignore

    class Settings(BaseModel):  # type: ignore
        # App / Meta
        service_name: str = "Tadawul Stock Analysis API"
        service_version: str = "0.0.0"
        environment: str = "production"
        tz: str = "Asia/Riyadh"
        debug: bool = False
        log_level: str = "info"

        # Auth
        require_auth: bool = False
        app_token: Optional[str] = None
        backup_app_token: Optional[str] = None

        # Providers
        enabled_providers_raw: str = "eodhd,finnhub"
        primary_provider: str = "eodhd"
        ksa_providers_raw: str = "yahoo_chart,tadawul,argaam"

        # Tokens (aliases supported manually)
        eodhd_api_token: Optional[str] = None
        finnhub_api_token: Optional[str] = None

        # URLs
        eodhd_base_url: str = "https://eodhistoricaldata.com/api"
        finnhub_base_url: str = "https://finnhub.io/api/v1"
        backend_base_url: Optional[str] = None

        http_timeout: float = 30.0
        max_retries: int = 2
        retry_delay: float = 0.5

        # Sheets
        default_spreadsheet_id: Optional[str] = None
        google_sheets_credentials: Optional[str] = None

        # KSA URLs
        tadawul_quote_url: Optional[str] = None
        tadawul_fundamentals_url: Optional[str] = None
        argaam_quote_url: Optional[str] = None
        argaam_profile_url: Optional[str] = None

        # Derived
        @property
        def enabled_providers(self) -> List[str]:
            return _csv(self.enabled_providers_raw, lower=True) or ["eodhd", "finnhub"]

        @property
        def ksa_providers(self) -> List[str]:
            return _csv(self.ksa_providers_raw, lower=True) or ["yahoo_chart", "tadawul", "argaam"]

    _CACHED: Optional[Settings] = None

    def get_settings() -> Settings:  # type: ignore
        global _CACHED
        if _CACHED is not None:
            return _CACHED

        _CACHED = Settings(
            service_name=_first_env("SERVICE_NAME", "APP_NAME") or "Tadawul Stock Analysis API",
            service_version=_first_env("SERVICE_VERSION", "APP_VERSION", "VERSION") or "0.0.0",
            environment=_first_env("ENVIRONMENT", "APP_ENV", "ENV") or "production",
            tz=_first_env("TZ", "TIMEZONE") or "Asia/Riyadh",
            debug=_to_bool(_first_env("DEBUG"), False),
            log_level=(_first_env("LOG_LEVEL") or "info").lower(),

            require_auth=_to_bool(_first_env("REQUIRE_AUTH"), False),
            app_token=_first_env("APP_TOKEN", "TFB_APP_TOKEN", "X_APP_TOKEN"),
            backup_app_token=_first_env("BACKUP_APP_TOKEN"),

            enabled_providers_raw=_first_env("ENABLED_PROVIDERS", "PROVIDERS") or "eodhd,finnhub",
            primary_provider=(_first_env("PRIMARY_PROVIDER") or "eodhd").lower(),
            ksa_providers_raw=_first_env("KSA_PROVIDERS", "KSA_ENABLED_PROVIDERS") or "yahoo_chart,tadawul,argaam",

            # ✅ IMPORTANT: map Render keys to token fields
            eodhd_api_token=_first_env("EODHD_API_KEY", "EODHD_API_TOKEN", "EODHD_TOKEN"),
            finnhub_api_token=_first_env("FINNHUB_API_KEY", "FINNHUB_API_TOKEN", "FINNHUB_TOKEN"),

            eodhd_base_url=_first_env("EODHD_BASE_URL") or "https://eodhistoricaldata.com/api",
            finnhub_base_url=_first_env("FINNHUB_BASE_URL") or "https://finnhub.io/api/v1",
            backend_base_url=_first_env("BACKEND_BASE_URL"),

            http_timeout=_to_float(_first_env("HTTP_TIMEOUT"), 30.0),
            max_retries=_to_int(_first_env("MAX_RETRIES"), 2),
            retry_delay=_to_float(_first_env("RETRY_DELAY"), 0.5),

            default_spreadsheet_id=_first_env("DEFAULT_SPREADSHEET_ID", "SPREADSHEET_ID"),
            google_sheets_credentials=_first_env("GOOGLE_SHEETS_CREDENTIALS", "GOOGLE_CREDENTIALS"),

            tadawul_quote_url=_first_env("TADAWUL_QUOTE_URL"),
            tadawul_fundamentals_url=_first_env("TADAWUL_FUNDAMENTALS_URL"),
            argaam_quote_url=_first_env("ARGAAM_QUOTE_URL"),
            argaam_profile_url=_first_env("ARGAAM_PROFILE_URL"),
        )
        return _CACHED


__all__ = ["Settings", "get_settings"]
```
