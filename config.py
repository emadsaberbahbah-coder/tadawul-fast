# config.py  (REPO ROOT) — FULL REPLACEMENT
"""
config.py
============================================================
Canonical Settings for Tadawul Fast Bridge (ROOT)

✅ Single source of truth for env vars (Render names).
✅ HARD alignment with your Render env variable names (no renaming needed).
✅ Runtime alias export (ONLY inside get_settings()) to keep legacy/provider code working:
   - FINNHUB_API_KEY -> FINNHUB_API_TOKEN + FINNHUB_TOKEN
   - EODHD_API_KEY   -> EODHD_API_TOKEN   + EODHD_TOKEN
   - Also exports common numeric/env aliases used across the repo.

Compatibility notes (important for your repo)
- env.py and some legacy modules expect:
    settings.enabled_providers
    settings.enabled_ksa_providers   (or ksa_providers)
    settings.app_name / settings.env / settings.version (optional)
- This file provides BOTH:
    enabled_ksa_providers  + ksa_providers (alias)
    app_name/env/version properties

No network at import-time. No heavy side effects at import-time.
Side effects (env alias export) happen ONLY inside get_settings().

Version: v5.4.0 (compat + alias export hardened)
"""

from __future__ import annotations

import os
from functools import lru_cache
from typing import Any, Dict, List, Optional

# -----------------------------------------------------------------------------
# Safe parsing helpers
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
    parts = [p.strip() for p in s.split(",") if p.strip()]
    return [p.lower() for p in parts] if lower else parts


def _mask_tail(s: Optional[str], keep: int = 4) -> str:
    x = (s or "").strip()
    if not x:
        return ""
    if len(x) <= keep:
        return "•" * len(x)
    return ("•" * (len(x) - keep)) + x[-keep:]


def _export_env_if_missing(key: str, value: Any) -> None:
    """
    Export env alias only if:
    - value is non-empty
    - target env var is missing/blank
    """
    if value is None:
        return
    v = str(value).strip()
    if not v:
        return
    cur = os.getenv(key)
    if cur is None or str(cur).strip() == "":
        os.environ[key] = v


def _export_list_env_if_missing(key: str, values: List[str]) -> None:
    if not values:
        return
    _export_env_if_missing(key, ",".join(values))


# =============================================================================
# Preferred: Pydantic v2 + pydantic-settings
# =============================================================================
try:
    from pydantic import Field  # type: ignore
    from pydantic_settings import BaseSettings, SettingsConfigDict  # type: ignore

    try:
        from pydantic import AliasChoices  # type: ignore

        def _alias(*names: str) -> Any:
            return AliasChoices(*names)

    except Exception:  # pragma: no cover
        def _alias(*names: str) -> Any:
            return names[0]

    class Settings(BaseSettings):  # type: ignore
        """
        Canonical env-backed settings model.
        Field aliases are aligned to YOUR Render env variable names.
        """

        model_config = SettingsConfigDict(extra="ignore", case_sensitive=False)

        # ---------------------------------------------------------------------
        # App / Meta
        # ---------------------------------------------------------------------
        service_name: str = Field(
            default="Tadawul Stock Analysis API",
            validation_alias=_alias("SERVICE_NAME", "APP_NAME"),
        )
        service_version: str = Field(
            default="0.0.0",
            validation_alias=_alias("SERVICE_VERSION", "APP_VERSION", "VERSION"),
        )
        environment: str = Field(
            default="production",
            validation_alias=_alias("ENVIRONMENT", "APP_ENV", "ENV"),
        )
        tz: str = Field(default="Asia/Riyadh", validation_alias=_alias("TZ", "TIMEZONE"))
        debug: bool = Field(default=False, validation_alias=_alias("DEBUG"))
        log_level: str = Field(default="info", validation_alias=_alias("LOG_LEVEL"))
        log_format: str = Field(
            default="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
            validation_alias=_alias("LOG_FORMAT"),
        )

        # ---------------------------------------------------------------------
        # Auth
        # ---------------------------------------------------------------------
        require_auth: bool = Field(default=False, validation_alias=_alias("REQUIRE_AUTH"))
        app_token: Optional[str] = Field(default=None, validation_alias=_alias("APP_TOKEN"))
        backup_app_token: Optional[str] = Field(default=None, validation_alias=_alias("BACKUP_APP_TOKEN"))

        # ---------------------------------------------------------------------
        # Providers (GLOBAL + KSA)
        # ---------------------------------------------------------------------
        enabled_providers_raw: str = Field(
            default="eodhd,finnhub",
            validation_alias=_alias("ENABLED_PROVIDERS", "PROVIDERS"),
        )
        primary_provider: str = Field(default="eodhd", validation_alias=_alias("PRIMARY_PROVIDER"))

        # KSA providers (support multiple env names for compatibility)
        enabled_ksa_providers_raw: str = Field(
            default="yahoo_chart,tadawul,argaam",
            validation_alias=_alias("KSA_PROVIDERS", "ENABLED_KSA_PROVIDERS", "ENABLED_KSA_PROVIDERS_RAW"),
        )

        # ---------------------------------------------------------------------
        # Provider keys/tokens (Render names locked)
        # ---------------------------------------------------------------------
        eodhd_api_key: Optional[str] = Field(
            default=None,
            validation_alias=_alias("EODHD_API_KEY", "EODHD_API_TOKEN", "EODHD_TOKEN"),
        )
        finnhub_api_key: Optional[str] = Field(
            default=None,
            validation_alias=_alias("FINNHUB_API_KEY", "FINNHUB_API_TOKEN", "FINNHUB_TOKEN"),
        )

        fmp_api_key: Optional[str] = Field(default=None, validation_alias=_alias("FMP_API_KEY"))
        alpha_vantage_api_key: Optional[str] = Field(default=None, validation_alias=_alias("ALPHA_VANTAGE_API_KEY"))
        twelvedata_api_key: Optional[str] = Field(default=None, validation_alias=_alias("TWELVEDATA_API_KEY"))
        marketstack_api_key: Optional[str] = Field(default=None, validation_alias=_alias("MARKETSTACK_API_KEY"))

        # ---------------------------------------------------------------------
        # Base URLs / Networking
        # ---------------------------------------------------------------------
        backend_base_url: Optional[str] = Field(
            default=None,
            validation_alias=_alias("BACKEND_BASE_URL", "TFB_BASE_URL", "BASE_URL"),
        )
        eodhd_base_url: str = Field(
            default="https://eodhistoricaldata.com/api",
            validation_alias=_alias("EODHD_BASE_URL"),
        )
        finnhub_base_url: str = Field(
            default="https://finnhub.io/api/v1",
            validation_alias=_alias("FINNHUB_BASE_URL"),
        )
        fmp_base_url: Optional[str] = Field(default=None, validation_alias=_alias("FMP_BASE_URL"))

        http_timeout: float = Field(default=30.0, validation_alias=_alias("HTTP_TIMEOUT", "HTTP_TIMEOUT_SEC"))
        max_retries: int = Field(default=2, validation_alias=_alias("MAX_RETRIES"))
        retry_delay: float = Field(default=0.5, validation_alias=_alias("RETRY_DELAY", "RETRY_DELAY_SEC"))

        # ---------------------------------------------------------------------
        # CORS / Rate limiting
        # ---------------------------------------------------------------------
        cors_origins: str = Field(default="*", validation_alias=_alias("CORS_ORIGINS"))
        enable_rate_limiting: bool = Field(default=True, validation_alias=_alias("ENABLE_RATE_LIMITING"))
        max_requests_per_minute: int = Field(default=240, validation_alias=_alias("MAX_REQUESTS_PER_MINUTE"))

        # ---------------------------------------------------------------------
        # Cache tuning
        # ---------------------------------------------------------------------
        cache_default_ttl: int = Field(default=10, validation_alias=_alias("CACHE_DEFAULT_TTL", "ENGINE_CACHE_TTL_SEC"))
        cache_max_size: int = Field(default=5000, validation_alias=_alias("CACHE_MAX_SIZE"))
        cache_backup_enabled: bool = Field(default=False, validation_alias=_alias("CACHE_BACKUP_ENABLED"))
        cache_save_interval: int = Field(default=300, validation_alias=_alias("CACHE_SAVE_INTERVAL"))

        # ---------------------------------------------------------------------
        # Google / Sheets
        # ---------------------------------------------------------------------
        default_spreadsheet_id: Optional[str] = Field(
            default=None,
            validation_alias=_alias("DEFAULT_SPREADSHEET_ID", "SPREADSHEET_ID", "GOOGLE_SHEETS_ID"),
        )
        google_sheets_credentials: Optional[str] = Field(
            default=None,
            validation_alias=_alias("GOOGLE_SHEETS_CREDENTIALS", "GOOGLE_CREDENTIALS", "GOOGLE_SA_JSON"),
        )
        google_apps_script_url: Optional[str] = Field(default=None, validation_alias=_alias("GOOGLE_APPS_SCRIPT_URL"))
        google_apps_script_backup_url: Optional[str] = Field(
            default=None, validation_alias=_alias("GOOGLE_APPS_SCRIPT_BACKUP_URL")
        )

        # ---------------------------------------------------------------------
        # Feature flags
        # ---------------------------------------------------------------------
        advanced_analysis_enabled: bool = Field(default=True, validation_alias=_alias("ADVANCED_ANALYSIS_ENABLED"))
        tadawul_market_enabled: bool = Field(default=True, validation_alias=_alias("TADAWUL_MARKET_ENABLED"))
        enable_swagger: bool = Field(default=True, validation_alias=_alias("ENABLE_SWAGGER"))
        enable_redoc: bool = Field(default=True, validation_alias=_alias("ENABLE_REDOC"))

        # ---------------------------------------------------------------------
        # Optional KSA routing URLs (if ever added later)
        # ---------------------------------------------------------------------
        tadawul_quote_url: Optional[str] = Field(default=None, validation_alias=_alias("TADAWUL_QUOTE_URL"))
        tadawul_fundamentals_url: Optional[str] = Field(default=None, validation_alias=_alias("TADAWUL_FUNDAMENTALS_URL"))
        argaam_quote_url: Optional[str] = Field(default=None, validation_alias=_alias("ARGAAM_QUOTE_URL"))
        argaam_profile_url: Optional[str] = Field(default=None, validation_alias=_alias("ARGAAM_PROFILE_URL"))

        # ---------------------------------------------------------------------
        # Derived helpers / Compatibility fields expected by other modules
        # ---------------------------------------------------------------------
        @property
        def enabled_providers(self) -> List[str]:
            xs = _csv(self.enabled_providers_raw, lower=True)
            return xs or ["eodhd", "finnhub"]

        @property
        def enabled_ksa_providers(self) -> List[str]:
            xs = _csv(self.enabled_ksa_providers_raw, lower=True)
            return xs or ["yahoo_chart", "tadawul", "argaam"]

        # Back-compat alias used in older code
        @property
        def ksa_providers(self) -> List[str]:
            return self.enabled_ksa_providers

        @property
        def cors_origins_list(self) -> List[str]:
            s = (self.cors_origins or "").strip()
            if not s or s == "*":
                return ["*"]
            return [x.strip() for x in s.split(",") if x.strip()]

        # Many modules historically expect *_api_token
        @property
        def eodhd_api_token(self) -> Optional[str]:
            return (self.eodhd_api_key or "").strip() or None

        @property
        def finnhub_api_token(self) -> Optional[str]:
            return (self.finnhub_api_key or "").strip() or None

        # env.py expects these sometimes
        @property
        def app_name(self) -> str:
            return self.service_name

        @property
        def env(self) -> str:
            return self.environment

        @property
        def version(self) -> str:
            return self.service_version

        def as_safe_dict(self) -> Dict[str, Any]:
            return {
                "service_name": self.service_name,
                "service_version": self.service_version,
                "environment": self.environment,
                "tz": self.tz,
                "debug": bool(self.debug),
                "log_level": (self.log_level or "info").strip().lower(),
                "require_auth": bool(self.require_auth),
                "enabled_providers": self.enabled_providers,
                "primary_provider": (self.primary_provider or "").strip().lower(),
                "enabled_ksa_providers": self.enabled_ksa_providers,
                "http_timeout": float(self.http_timeout),
                "max_retries": int(self.max_retries),
                "retry_delay": float(self.retry_delay),
                "cache_default_ttl": int(self.cache_default_ttl),
                "cache_max_size": int(self.cache_max_size),
                "rate_limiting": {
                    "enabled": bool(self.enable_rate_limiting),
                    "max_requests_per_minute": int(self.max_requests_per_minute),
                },
                "app_token_set": bool((self.app_token or "").strip()),
                "backup_app_token_set": bool((self.backup_app_token or "").strip()),
                "app_token_mask": _mask_tail(self.app_token, keep=4),
                "eodhd_key_set": bool((self.eodhd_api_key or "").strip()),
                "finnhub_key_set": bool((self.finnhub_api_key or "").strip()),
                "tadawul_urls_set": bool(
                    (self.tadawul_quote_url or "").strip() or (self.tadawul_fundamentals_url or "").strip()
                ),
                "argaam_urls_set": bool((self.argaam_quote_url or "").strip() or (self.argaam_profile_url or "").strip()),
                "default_spreadsheet_id_set": bool((self.default_spreadsheet_id or "").strip()),
                "google_creds_set": bool((self.google_sheets_credentials or "").strip()),
            }

        # Optional: uppercase attribute compatibility (best-effort)
        def __getattr__(self, name: str) -> Any:
            if name.isupper():
                low = name.lower()
                if hasattr(self, low):
                    return getattr(self, low)
                mapping = {
                    "EODHD_API_TOKEN": "eodhd_api_token",
                    "FINNHUB_API_TOKEN": "finnhub_api_token",
                }
                if name in mapping and hasattr(self, mapping[name]):
                    return getattr(self, mapping[name])
            raise AttributeError(name)

    def _apply_runtime_env_aliases(s: Settings) -> None:
        """
        Critical: keep provider modules working with your Render env names WITHOUT changing Render.

        Providers/legacy modules may read:
          - FINNHUB_API_TOKEN, FINNHUB_TOKEN
          - EODHD_API_TOKEN,   EODHD_TOKEN
        But Render commonly stores:
          - FINNHUB_API_KEY
          - EODHD_API_KEY

        Export aliases at runtime (no overwrite if already set).
        """
        # Tokens
        _export_env_if_missing("FINNHUB_API_TOKEN", s.finnhub_api_token)
        _export_env_if_missing("FINNHUB_TOKEN", s.finnhub_api_token)

        _export_env_if_missing("EODHD_API_TOKEN", s.eodhd_api_token)
        _export_env_if_missing("EODHD_TOKEN", s.eodhd_api_token)

        # Provider lists (some modules read PROVIDERS / KSA_PROVIDERS at runtime)
        _export_list_env_if_missing("ENABLED_PROVIDERS", s.enabled_providers)
        _export_list_env_if_missing("PROVIDERS", s.enabled_providers)
        _export_list_env_if_missing("KSA_PROVIDERS", s.enabled_ksa_providers)

        # Primary provider
        _export_env_if_missing("PRIMARY_PROVIDER", (s.primary_provider or "").strip().lower())

        # Timeout / retry naming styles used elsewhere
        _export_env_if_missing("HTTP_TIMEOUT_SEC", str(float(s.http_timeout)))
        _export_env_if_missing("HTTP_TIMEOUT", str(float(s.http_timeout)))
        _export_env_if_missing("MAX_RETRIES", str(int(s.max_retries)))
        _export_env_if_missing("RETRY_DELAY", str(float(s.retry_delay)))
        _export_env_if_missing("RETRY_DELAY_SEC", str(float(s.retry_delay)))

        # Cache tuning (engine may read ENGINE_CACHE_TTL_SEC)
        _export_env_if_missing("CACHE_DEFAULT_TTL", str(int(s.cache_default_ttl)))
        _export_env_if_missing("CACHE_MAX_SIZE", str(int(s.cache_max_size)))
        _export_env_if_missing("ENGINE_CACHE_TTL_SEC", str(int(s.cache_default_ttl)))

        # CORS / rate limit
        _export_env_if_missing("CORS_ORIGINS", (s.cors_origins or "*").strip())
        _export_env_if_missing("ENABLE_RATE_LIMITING", "true" if bool(s.enable_rate_limiting) else "false")
        _export_env_if_missing("MAX_REQUESTS_PER_MINUTE", str(int(s.max_requests_per_minute)))

        # TZ
        _export_env_if_missing("TZ", (s.tz or "Asia/Riyadh").strip())

        # Ensure base URLs exist for modules reading env at call-time
        _export_env_if_missing("EODHD_BASE_URL", s.eodhd_base_url)
        _export_env_if_missing("FINNHUB_BASE_URL", s.finnhub_base_url)

        # Google / sheets common aliases
        if (s.default_spreadsheet_id or "").strip():
            _export_env_if_missing("DEFAULT_SPREADSHEET_ID", s.default_spreadsheet_id)
            _export_env_if_missing("SPREADSHEET_ID", s.default_spreadsheet_id)
            _export_env_if_missing("GOOGLE_SHEETS_ID", s.default_spreadsheet_id)

        if (s.google_sheets_credentials or "").strip():
            _export_env_if_missing("GOOGLE_SHEETS_CREDENTIALS", s.google_sheets_credentials)
            _export_env_if_missing("GOOGLE_CREDENTIALS", s.google_sheets_credentials)

        if (s.google_apps_script_url or "").strip():
            _export_env_if_missing("GOOGLE_APPS_SCRIPT_URL", s.google_apps_script_url)

        if (s.google_apps_script_backup_url or "").strip():
            _export_env_if_missing("GOOGLE_APPS_SCRIPT_BACKUP_URL", s.google_apps_script_backup_url)

    @lru_cache(maxsize=1)
    def get_settings() -> Settings:
        s = Settings()

        # Normalize
        s.log_level = (s.log_level or "info").strip().lower()
        s.primary_provider = (s.primary_provider or "eodhd").strip().lower()

        s.debug = _to_bool(s.debug, False)
        s.require_auth = _to_bool(s.require_auth, False)
        s.enable_rate_limiting = _to_bool(s.enable_rate_limiting, True)
        s.cache_backup_enabled = _to_bool(s.cache_backup_enabled, False)
        s.advanced_analysis_enabled = _to_bool(s.advanced_analysis_enabled, True)
        s.tadawul_market_enabled = _to_bool(s.tadawul_market_enabled, True)
        s.enable_swagger = _to_bool(s.enable_swagger, True)
        s.enable_redoc = _to_bool(s.enable_redoc, True)

        s.cache_default_ttl = max(3, _to_int(s.cache_default_ttl, 10))
        s.cache_max_size = max(100, _to_int(s.cache_max_size, 5000))
        s.cache_save_interval = max(30, _to_int(s.cache_save_interval, 300))
        s.max_requests_per_minute = max(10, _to_int(s.max_requests_per_minute, 240))
        s.max_retries = max(0, _to_int(s.max_retries, 2))
        s.http_timeout = max(5.0, _to_float(s.http_timeout, 30.0))
        s.retry_delay = max(0.0, _to_float(s.retry_delay, 0.5))

        # 🔥 IMPORTANT FIX (runtime env alias export)
        _apply_runtime_env_aliases(s)

        return s

except Exception:  # pragma: no cover
    # =============================================================================
    # LAST-RESORT FALLBACK (no pydantic-settings available)
    # =============================================================================
    try:
        from pydantic import BaseModel  # type: ignore
    except Exception:  # ultra fallback
        class BaseModel:  # type: ignore
            def __init__(self, **kwargs):
                self.__dict__.update(kwargs)

    class Settings(BaseModel):  # type: ignore
        service_name: str = "Tadawul Stock Analysis API"
        service_version: str = "0.0.0"
        environment: str = "production"
        tz: str = "Asia/Riyadh"
        debug: bool = False
        log_level: str = "info"
        log_format: str = "%(asctime)s | %(levelname)s | %(name)s | %(message)s"

        require_auth: bool = False
        app_token: Optional[str] = None
        backup_app_token: Optional[str] = None

        enabled_providers_raw: str = "eodhd,finnhub"
        primary_provider: str = "eodhd"
        enabled_ksa_providers_raw: str = "yahoo_chart,tadawul,argaam"

        eodhd_api_key: Optional[str] = None
        finnhub_api_key: Optional[str] = None

        fmp_api_key: Optional[str] = None
        alpha_vantage_api_key: Optional[str] = None

        eodhd_base_url: str = "https://eodhistoricaldata.com/api"
        finnhub_base_url: str = "https://finnhub.io/api/v1"
        backend_base_url: Optional[str] = None

        http_timeout: float = 30.0
        max_retries: int = 2
        retry_delay: float = 0.5

        cors_origins: str = "*"
        enable_rate_limiting: bool = True
        max_requests_per_minute: int = 240

        cache_default_ttl: int = 10
        cache_max_size: int = 5000
        cache_backup_enabled: bool = False
        cache_save_interval: int = 300

        default_spreadsheet_id: Optional[str] = None
        google_sheets_credentials: Optional[str] = None
        google_apps_script_url: Optional[str] = None
        google_apps_script_backup_url: Optional[str] = None

        advanced_analysis_enabled: bool = True
        tadawul_market_enabled: bool = True
        enable_swagger: bool = True
        enable_redoc: bool = True

        tadawul_quote_url: Optional[str] = None
        tadawul_fundamentals_url: Optional[str] = None
        argaam_quote_url: Optional[str] = None
        argaam_profile_url: Optional[str] = None

        @property
        def enabled_providers(self) -> List[str]:
            return _csv(self.enabled_providers_raw, lower=True) or ["eodhd", "finnhub"]

        @property
        def enabled_ksa_providers(self) -> List[str]:
            return _csv(self.enabled_ksa_providers_raw, lower=True) or ["yahoo_chart", "tadawul", "argaam"]

        @property
        def ksa_providers(self) -> List[str]:
            return self.enabled_ksa_providers

        @property
        def eodhd_api_token(self) -> Optional[str]:
            return (self.eodhd_api_key or "").strip() or None

        @property
        def finnhub_api_token(self) -> Optional[str]:
            return (self.finnhub_api_key or "").strip() or None

        @property
        def app_name(self) -> str:
            return self.service_name

        @property
        def env(self) -> str:
            return self.environment

        @property
        def version(self) -> str:
            return self.service_version

        def as_safe_dict(self) -> Dict[str, Any]:
            return {
                "service_name": self.service_name,
                "service_version": self.service_version,
                "environment": self.environment,
                "tz": self.tz,
                "debug": bool(self.debug),
                "log_level": (self.log_level or "info").strip().lower(),
                "require_auth": bool(self.require_auth),
                "enabled_providers": self.enabled_providers,
                "primary_provider": (self.primary_provider or "").strip().lower(),
                "enabled_ksa_providers": self.enabled_ksa_providers,
                "eodhd_key_set": bool((self.eodhd_api_key or "").strip()),
                "finnhub_key_set": bool((self.finnhub_api_key or "").strip()),
            }

    _CACHED: Optional[Settings] = None

    def get_settings() -> Settings:  # type: ignore
        global _CACHED
        if _CACHED is not None:
            return _CACHED

        _CACHED = Settings(
            service_name=(os.getenv("SERVICE_NAME") or os.getenv("APP_NAME") or "Tadawul Stock Analysis API").strip(),
            service_version=(os.getenv("SERVICE_VERSION") or os.getenv("APP_VERSION") or os.getenv("VERSION") or "0.0.0").strip(),
            environment=(os.getenv("ENVIRONMENT") or os.getenv("APP_ENV") or os.getenv("ENV") or "production").strip(),
            tz=(os.getenv("TZ") or os.getenv("TIMEZONE") or "Asia/Riyadh").strip(),
            debug=_to_bool(os.getenv("DEBUG"), False),
            log_level=(os.getenv("LOG_LEVEL") or "info").strip().lower(),
            require_auth=_to_bool(os.getenv("REQUIRE_AUTH"), False),
            app_token=(os.getenv("APP_TOKEN") or "").strip() or None,
            backup_app_token=(os.getenv("BACKUP_APP_TOKEN") or "").strip() or None,
            enabled_providers_raw=(os.getenv("ENABLED_PROVIDERS") or os.getenv("PROVIDERS") or "eodhd,finnhub").strip(),
            primary_provider=(os.getenv("PRIMARY_PROVIDER") or "eodhd").strip().lower(),
            enabled_ksa_providers_raw=(os.getenv("KSA_PROVIDERS") or "yahoo_chart,tadawul,argaam").strip(),
            eodhd_api_key=(os.getenv("EODHD_API_KEY") or os.getenv("EODHD_API_TOKEN") or os.getenv("EODHD_TOKEN") or "").strip() or None,
            finnhub_api_key=(os.getenv("FINNHUB_API_KEY") or os.getenv("FINNHUB_API_TOKEN") or os.getenv("FINNHUB_TOKEN") or "").strip() or None,
            eodhd_base_url=(os.getenv("EODHD_BASE_URL") or "https://eodhistoricaldata.com/api").strip(),
            finnhub_base_url=(os.getenv("FINNHUB_BASE_URL") or "https://finnhub.io/api/v1").strip(),
            backend_base_url=(os.getenv("BACKEND_BASE_URL") or os.getenv("TFB_BASE_URL") or os.getenv("BASE_URL") or "").strip() or None,
            http_timeout=_to_float(os.getenv("HTTP_TIMEOUT") or os.getenv("HTTP_TIMEOUT_SEC"), 30.0),
            max_retries=_to_int(os.getenv("MAX_RETRIES"), 2),
            retry_delay=_to_float(os.getenv("RETRY_DELAY") or os.getenv("RETRY_DELAY_SEC"), 0.5),
            cache_default_ttl=_to_int(os.getenv("CACHE_DEFAULT_TTL") or os.getenv("ENGINE_CACHE_TTL_SEC"), 10),
            cache_max_size=_to_int(os.getenv("CACHE_MAX_SIZE"), 5000),
            cache_backup_enabled=_to_bool(os.getenv("CACHE_BACKUP_ENABLED"), False),
            cache_save_interval=_to_int(os.getenv("CACHE_SAVE_INTERVAL"), 300),
            cors_origins=(os.getenv("CORS_ORIGINS") or "*").strip(),
            enable_rate_limiting=_to_bool(os.getenv("ENABLE_RATE_LIMITING"), True),
            max_requests_per_minute=_to_int(os.getenv("MAX_REQUESTS_PER_MINUTE"), 240),
            default_spreadsheet_id=(os.getenv("DEFAULT_SPREADSHEET_ID") or os.getenv("SPREADSHEET_ID") or os.getenv("GOOGLE_SHEETS_ID") or "").strip() or None,
            google_sheets_credentials=(os.getenv("GOOGLE_SHEETS_CREDENTIALS") or os.getenv("GOOGLE_CREDENTIALS") or os.getenv("GOOGLE_SA_JSON") or "").strip() or None,
            google_apps_script_url=(os.getenv("GOOGLE_APPS_SCRIPT_URL") or "").strip() or None,
            google_apps_script_backup_url=(os.getenv("GOOGLE_APPS_SCRIPT_BACKUP_URL") or "").strip() or None,
        )

        # Runtime alias exports (same intent as pydantic path)
        _export_env_if_missing("FINNHUB_API_TOKEN", _CACHED.finnhub_api_token)
        _export_env_if_missing("FINNHUB_TOKEN", _CACHED.finnhub_api_token)
        _export_env_if_missing("EODHD_API_TOKEN", _CACHED.eodhd_api_token)
        _export_env_if_missing("EODHD_TOKEN", _CACHED.eodhd_api_token)

        _export_list_env_if_missing("ENABLED_PROVIDERS", _CACHED.enabled_providers)
        _export_list_env_if_missing("PROVIDERS", _CACHED.enabled_providers)
        _export_list_env_if_missing("KSA_PROVIDERS", _CACHED.enabled_ksa_providers)

        _export_env_if_missing("HTTP_TIMEOUT_SEC", str(float(_CACHED.http_timeout)))
        _export_env_if_missing("HTTP_TIMEOUT", str(float(_CACHED.http_timeout)))
        _export_env_if_missing("ENGINE_CACHE_TTL_SEC", str(int(_CACHED.cache_default_ttl)))

        if (_CACHED.default_spreadsheet_id or "").strip():
            _export_env_if_missing("DEFAULT_SPREADSHEET_ID", _CACHED.default_spreadsheet_id)
            _export_env_if_missing("SPREADSHEET_ID", _CACHED.default_spreadsheet_id)
            _export_env_if_missing("GOOGLE_SHEETS_ID", _CACHED.default_spreadsheet_id)

        if (_CACHED.google_sheets_credentials or "").strip():
            _export_env_if_missing("GOOGLE_SHEETS_CREDENTIALS", _CACHED.google_sheets_credentials)
            _export_env_if_missing("GOOGLE_CREDENTIALS", _CACHED.google_sheets_credentials)

        return _CACHED


__all__ = ["Settings", "get_settings"]
