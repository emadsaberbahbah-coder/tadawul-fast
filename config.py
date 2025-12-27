# config.py  (REPO ROOT) — FULL REPLACEMENT
"""
config.py
============================================================
Canonical Settings for Tadawul Fast Bridge (ROOT)

✅ Single source of truth for env vars (Render names).
✅ Runtime alias export (so legacy/provider modules keep working).
✅ No network at import-time. Minimal side effects at import-time.

Version: v5.3.1 (aligned: APP_VERSION / SERVICE_VERSION / VERSION + *_SEC TTL + HTTP_TIMEOUT_SEC)
"""

from __future__ import annotations

import os
from functools import lru_cache
from typing import Any, Dict, List, Optional


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


def _export_env_if_missing(key: str, value: Optional[str]) -> None:
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


def _resolve_version_from_env_or_commit(service_version: str) -> str:
    """
    Version priority:
      1) SERVICE_VERSION / APP_VERSION / VERSION (already loaded by pydantic if set)
      2) Render commit short hash (RENDER_GIT_COMMIT / GIT_COMMIT)
      3) "dev"
    """
    sv = (service_version or "").strip()
    if sv and sv not in {"0.0.0", "unknown", "none", "null"}:
        return sv

    commit = (os.getenv("RENDER_GIT_COMMIT") or os.getenv("GIT_COMMIT") or "").strip()
    if commit:
        return commit[:7]

    return "dev"


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

        # IMPORTANT: these are the keys you asked about:
        # APP_VERSION / SERVICE_VERSION / VERSION
        service_version: str = Field(
            default="dev",
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
        # Boot / engine flags (safe defaults)
        # ---------------------------------------------------------------------
        defer_router_mount: bool = Field(default=True, validation_alias=_alias("DEFER_ROUTER_MOUNT"))
        init_engine_on_boot: bool = Field(default=True, validation_alias=_alias("INIT_ENGINE_ON_BOOT"))

        # ---------------------------------------------------------------------
        # Auth
        # ---------------------------------------------------------------------
        require_auth: bool = Field(default=False, validation_alias=_alias("REQUIRE_AUTH"))
        app_token: Optional[str] = Field(default=None, validation_alias=_alias("APP_TOKEN", "TFB_APP_TOKEN"))
        backup_app_token: Optional[str] = Field(default=None, validation_alias=_alias("BACKUP_APP_TOKEN"))

        # ---------------------------------------------------------------------
        # Providers (GLOBAL + KSA)
        # ---------------------------------------------------------------------
        enabled_providers_raw: str = Field(
            default="eodhd,finnhub",
            validation_alias=_alias("ENABLED_PROVIDERS", "PROVIDERS"),
        )
        primary_provider: str = Field(default="eodhd", validation_alias=_alias("PRIMARY_PROVIDER"))
        ksa_providers_raw: str = Field(default="yahoo_chart,tadawul,argaam", validation_alias=_alias("KSA_PROVIDERS"))

        # ---------------------------------------------------------------------
        # Provider keys/tokens (Render names locked)
        # ---------------------------------------------------------------------
        eodhd_api_key: Optional[str] = Field(default=None, validation_alias=_alias("EODHD_API_KEY", "EODHD_API_TOKEN", "EODHD_TOKEN"))
        finnhub_api_key: Optional[str] = Field(default=None, validation_alias=_alias("FINNHUB_API_KEY", "FINNHUB_API_TOKEN", "FINNHUB_TOKEN"))

        fmp_api_key: Optional[str] = Field(default=None, validation_alias=_alias("FMP_API_KEY"))
        alpha_vantage_api_key: Optional[str] = Field(default=None, validation_alias=_alias("ALPHA_VANTAGE_API_KEY"))
        twelvedata_api_key: Optional[str] = Field(default=None, validation_alias=_alias("TWELVEDATA_API_KEY"))
        marketstack_api_key: Optional[str] = Field(default=None, validation_alias=_alias("MARKETSTACK_API_KEY"))
        argaam_api_key: Optional[str] = Field(default=None, validation_alias=_alias("ARGAAM_API_KEY"))

        # ---------------------------------------------------------------------
        # Base URLs / Networking
        # ---------------------------------------------------------------------
        backend_base_url: Optional[str] = Field(default=None, validation_alias=_alias("BACKEND_BASE_URL", "TFB_BASE_URL", "BASE_URL"))

        eodhd_base_url: str = Field(default="https://eodhistoricaldata.com/api", validation_alias=_alias("EODHD_BASE_URL"))
        finnhub_base_url: str = Field(default="https://finnhub.io/api/v1", validation_alias=_alias("FINNHUB_BASE_URL"))
        fmp_base_url: Optional[str] = Field(default=None, validation_alias=_alias("FMP_BASE_URL"))

        # Render yaml uses HTTP_TIMEOUT_SEC; keep HTTP_TIMEOUT too
        http_timeout: float = Field(default=30.0, validation_alias=_alias("HTTP_TIMEOUT_SEC", "HTTP_TIMEOUT"))
        max_retries: int = Field(default=2, validation_alias=_alias("MAX_RETRIES"))
        retry_delay: float = Field(default=0.5, validation_alias=_alias("RETRY_DELAY", "RETRY_DELAY_SEC"))

        # ---------------------------------------------------------------------
        # CORS / Rate limiting
        # ---------------------------------------------------------------------
        enable_cors_all_origins: bool = Field(default=True, validation_alias=_alias("ENABLE_CORS_ALL_ORIGINS", "CORS_ALL_ORIGINS"))
        cors_origins: str = Field(default="*", validation_alias=_alias("CORS_ORIGINS"))
        enable_rate_limiting: bool = Field(default=True, validation_alias=_alias("ENABLE_RATE_LIMITING"))
        max_requests_per_minute: int = Field(default=240, validation_alias=_alias("MAX_REQUESTS_PER_MINUTE"))

        # ---------------------------------------------------------------------
        # Cache tuning (Render yaml uses *_TTL_SEC)
        # ---------------------------------------------------------------------
        cache_ttl_sec: float = Field(default=20.0, validation_alias=_alias("CACHE_TTL_SEC", "CACHE_DEFAULT_TTL"))
        quote_ttl_sec: float = Field(default=30.0, validation_alias=_alias("QUOTE_TTL_SEC"))
        fundamentals_ttl_sec: float = Field(default=21600.0, validation_alias=_alias("FUNDAMENTALS_TTL_SEC"))
        argaam_snapshot_ttl_sec: float = Field(default=30.0, validation_alias=_alias("ARGAAM_SNAPSHOT_TTL_SEC"))

        # NEW: engine ttl + batch concurrency
        engine_cache_ttl_sec: float = Field(
            default=20.0,
            validation_alias=_alias("ENGINE_CACHE_TTL_SEC", "ENGINE_TTL_SEC", "CACHE_TTL_SEC"),
        )
        enriched_batch_concurrency: int = Field(
            default=8,
            validation_alias=_alias("ENRICHED_BATCH_CONCURRENCY", "ENRICHED_CONCURRENCY"),
        )

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
        google_apps_script_backup_url: Optional[str] = Field(default=None, validation_alias=_alias("GOOGLE_APPS_SCRIPT_BACKUP_URL"))

        # ---------------------------------------------------------------------
        # Feature flags
        # ---------------------------------------------------------------------
        advanced_analysis_enabled: bool = Field(default=True, validation_alias=_alias("ADVANCED_ANALYSIS_ENABLED"))
        tadawul_market_enabled: bool = Field(default=True, validation_alias=_alias("TADAWUL_MARKET_ENABLED"))
        enable_swagger: bool = Field(default=True, validation_alias=_alias("ENABLE_SWAGGER"))
        enable_redoc: bool = Field(default=True, validation_alias=_alias("ENABLE_REDOC"))

        # Optional KSA routing URLs
        tadawul_quote_url: Optional[str] = Field(default=None, validation_alias=_alias("TADAWUL_QUOTE_URL"))
        tadawul_fundamentals_url: Optional[str] = Field(default=None, validation_alias=_alias("TADAWUL_FUNDAMENTALS_URL"))
        argaam_quote_url: Optional[str] = Field(default=None, validation_alias=_alias("ARGAAM_QUOTE_URL"))
        argaam_profile_url: Optional[str] = Field(default=None, validation_alias=_alias("ARGAAM_PROFILE_URL"))

        # ---------------------------------------------------------------------
        # Derived helpers / compatibility
        # ---------------------------------------------------------------------
        @property
        def enabled_providers(self) -> List[str]:
            xs = _csv(self.enabled_providers_raw, lower=True)
            return xs or ["eodhd", "finnhub"]

        @property
        def ksa_providers(self) -> List[str]:
            xs = _csv(self.ksa_providers_raw, lower=True)
            return xs or ["yahoo_chart", "tadawul", "argaam"]

        @property
        def enabled_ksa_providers(self) -> List[str]:
            # compatibility alias used by some modules
            return self.ksa_providers

        @property
        def cors_origins_list(self) -> List[str]:
            if bool(self.enable_cors_all_origins):
                return ["*"]
            s = (self.cors_origins or "").strip()
            if not s or s == "*":
                return ["*"]
            return [x.strip() for x in s.split(",") if x.strip()]

        # Compatibility: many modules historically expect *_api_token
        @property
        def eodhd_api_token(self) -> Optional[str]:
            return (self.eodhd_api_key or "").strip() or None

        @property
        def finnhub_api_token(self) -> Optional[str]:
            return (self.finnhub_api_key or "").strip() or None

        @property
        def http_timeout_sec(self) -> float:
            return float(self.http_timeout or 30.0)

        # Extra compatibility: env.py / legacy modules often expect these names
        @property
        def version(self) -> str:
            return self.service_version

        @property
        def app_version(self) -> str:
            return self.service_version

        @property
        def app_name(self) -> str:
            return self.service_name

        @property
        def env(self) -> str:
            return self.environment

        @property
        def base_url(self) -> str:
            return (self.backend_base_url or "").rstrip("/")

        @property
        def google_credentials(self) -> Optional[str]:
            # keep name used by some older code
            return self.google_sheets_credentials

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
                "ksa_providers": self.ksa_providers,
                "http_timeout_sec": float(self.http_timeout_sec),
                "max_retries": int(self.max_retries),
                "retry_delay": float(self.retry_delay),
                "cache_ttl_sec": float(self.cache_ttl_sec),
                "engine_cache_ttl_sec": float(self.engine_cache_ttl_sec),
                "enriched_batch_concurrency": int(self.enriched_batch_concurrency),
                "quote_ttl_sec": float(self.quote_ttl_sec),
                "fundamentals_ttl_sec": float(self.fundamentals_ttl_sec),
                "argaam_snapshot_ttl_sec": float(self.argaam_snapshot_ttl_sec),
                "cache_max_size": int(self.cache_max_size),
                "rate_limiting": {
                    "enabled": bool(self.enable_rate_limiting),
                    "max_requests_per_minute": int(self.max_requests_per_minute),
                },
                "cors": {
                    "all_origins": bool(self.enable_cors_all_origins),
                    "origins_list": self.cors_origins_list,
                },
                "app_token_set": bool((self.app_token or "").strip()),
                "backup_app_token_set": bool((self.backup_app_token or "").strip()),
                "app_token_mask": _mask_tail(self.app_token, keep=4),
                "eodhd_key_set": bool((self.eodhd_api_key or "").strip()),
                "finnhub_key_set": bool((self.finnhub_api_key or "").strip()),
                "default_spreadsheet_id_set": bool((self.default_spreadsheet_id or "").strip()),
                "google_creds_set": bool((self.google_sheets_credentials or "").strip()),
            }

        def __getattr__(self, name: str) -> Any:
            # Optional: uppercase attribute compatibility (best-effort)
            if name.isupper():
                low = name.lower()
                if hasattr(self, low):
                    return getattr(self, low)
                mapping = {
                    "EODHD_API_TOKEN": "eodhd_api_token",
                    "FINNHUB_API_TOKEN": "finnhub_api_token",
                    "HTTP_TIMEOUT_SEC": "http_timeout_sec",
                    "APP_NAME": "app_name",
                    "APP_ENV": "env",
                    "APP_VERSION": "app_version",
                }
                if name in mapping and hasattr(self, mapping[name]):
                    return getattr(self, mapping[name])
            raise AttributeError(name)

    def _apply_runtime_env_aliases(s: Settings) -> None:
        """
        Make providers + legacy modules work with your Render env names WITHOUT changing Render.
        """
        # --- Version keys (the ones you asked for) ---
        ver = _resolve_version_from_env_or_commit(s.service_version)
        s.service_version = ver

        _export_env_if_missing("SERVICE_VERSION", ver)
        _export_env_if_missing("APP_VERSION", ver)
        _export_env_if_missing("VERSION", ver)

        # Provider token aliases
        _export_env_if_missing("FINNHUB_API_TOKEN", s.finnhub_api_token)
        _export_env_if_missing("FINNHUB_TOKEN", s.finnhub_api_token)

        _export_env_if_missing("EODHD_API_TOKEN", s.eodhd_api_token)
        _export_env_if_missing("EODHD_TOKEN", s.eodhd_api_token)

        # Timeout aliases
        _export_env_if_missing("HTTP_TIMEOUT_SEC", str(float(s.http_timeout_sec)))
        _export_env_if_missing("HTTP_TIMEOUT", str(float(s.http_timeout_sec)))

        # TTL aliases
        _export_env_if_missing("CACHE_TTL_SEC", str(float(s.cache_ttl_sec)))
        _export_env_if_missing("CACHE_DEFAULT_TTL", str(float(s.cache_ttl_sec)))

        _export_env_if_missing("ENGINE_CACHE_TTL_SEC", str(float(s.engine_cache_ttl_sec)))
        _export_env_if_missing("ENRICHED_BATCH_CONCURRENCY", str(int(s.enriched_batch_concurrency)))
        _export_env_if_missing("ENRICHED_CONCURRENCY", str(int(s.enriched_batch_concurrency)))

        _export_env_if_missing("QUOTE_TTL_SEC", str(float(s.quote_ttl_sec)))
        _export_env_if_missing("FUNDAMENTALS_TTL_SEC", str(float(s.fundamentals_ttl_sec)))
        _export_env_if_missing("ARGAAM_SNAPSHOT_TTL_SEC", str(float(s.argaam_snapshot_ttl_sec)))

        # Base URLs for provider modules that read env at call-time
        _export_env_if_missing("EODHD_BASE_URL", s.eodhd_base_url)
        _export_env_if_missing("FINNHUB_BASE_URL", s.finnhub_base_url)

        # Backend base URL alias
        if s.backend_base_url:
            _export_env_if_missing("BACKEND_BASE_URL", (s.backend_base_url or "").rstrip("/"))
            _export_env_if_missing("TFB_BASE_URL", (s.backend_base_url or "").rstrip("/"))

        # Spreadsheet ID alias
        if s.default_spreadsheet_id:
            _export_env_if_missing("DEFAULT_SPREADSHEET_ID", s.default_spreadsheet_id)
            _export_env_if_missing("SPREADSHEET_ID", s.default_spreadsheet_id)
            _export_env_if_missing("GOOGLE_SHEETS_ID", s.default_spreadsheet_id)

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
        s.enable_cors_all_origins = _to_bool(s.enable_cors_all_origins, True)
        s.defer_router_mount = _to_bool(s.defer_router_mount, True)
        s.init_engine_on_boot = _to_bool(s.init_engine_on_boot, True)

        s.max_requests_per_minute = _to_int(s.max_requests_per_minute, 240)
        s.max_retries = _to_int(s.max_retries, 2)

        s.http_timeout = _to_float(s.http_timeout, 30.0)
        s.retry_delay = _to_float(s.retry_delay, 0.5)

        s.cache_ttl_sec = _to_float(s.cache_ttl_sec, 20.0)
        s.quote_ttl_sec = _to_float(s.quote_ttl_sec, 30.0)
        s.fundamentals_ttl_sec = _to_float(s.fundamentals_ttl_sec, 21600.0)
        s.argaam_snapshot_ttl_sec = _to_float(s.argaam_snapshot_ttl_sec, 30.0)

        # NEW
        s.engine_cache_ttl_sec = _to_float(s.engine_cache_ttl_sec, float(s.cache_ttl_sec or 20.0))
        s.enriched_batch_concurrency = _to_int(s.enriched_batch_concurrency, 8)

        s.cache_max_size = _to_int(s.cache_max_size, 5000)
        s.cache_save_interval = _to_int(s.cache_save_interval, 300)

        _apply_runtime_env_aliases(s)
        return s

except Exception:  # pragma: no cover
    # =============================================================================
    # LAST-RESORT FALLBACK (no pydantic-settings available)
    # =============================================================================
    from pydantic import BaseModel  # type: ignore

    class Settings(BaseModel):  # type: ignore
        service_name: str = "Tadawul Stock Analysis API"
        service_version: str = "dev"
        environment: str = "production"
        tz: str = "Asia/Riyadh"
        debug: bool = False
        log_level: str = "info"
        log_format: str = "%(asctime)s | %(levelname)s | %(name)s | %(message)s"

        defer_router_mount: bool = True
        init_engine_on_boot: bool = True

        require_auth: bool = False
        app_token: Optional[str] = None
        backup_app_token: Optional[str] = None

        enabled_providers_raw: str = "eodhd,finnhub"
        primary_provider: str = "eodhd"
        ksa_providers_raw: str = "yahoo_chart,tadawul,argaam"

        eodhd_api_key: Optional[str] = None
        finnhub_api_key: Optional[str] = None
        fmp_api_key: Optional[str] = None
        alpha_vantage_api_key: Optional[str] = None
        argaam_api_key: Optional[str] = None

        eodhd_base_url: str = "https://eodhistoricaldata.com/api"
        finnhub_base_url: str = "https://finnhub.io/api/v1"
        backend_base_url: Optional[str] = None

        http_timeout: float = 30.0
        max_retries: int = 2
        retry_delay: float = 0.5

        enable_cors_all_origins: bool = True
        cors_origins: str = "*"
        enable_rate_limiting: bool = True
        max_requests_per_minute: int = 240

        cache_ttl_sec: float = 20.0
        quote_ttl_sec: float = 30.0
        fundamentals_ttl_sec: float = 21600.0
        argaam_snapshot_ttl_sec: float = 30.0

        engine_cache_ttl_sec: float = 20.0
        enriched_batch_concurrency: int = 8

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

        @property
        def enabled_providers(self) -> List[str]:
            return _csv(self.enabled_providers_raw, lower=True) or ["eodhd", "finnhub"]

        @property
        def ksa_providers(self) -> List[str]:
            return _csv(self.ksa_providers_raw, lower=True) or ["yahoo_chart", "tadawul", "argaam"]

        @property
        def enabled_ksa_providers(self) -> List[str]:
            return self.ksa_providers

        @property
        def eodhd_api_token(self) -> Optional[str]:
            return (self.eodhd_api_key or "").strip() or None

        @property
        def finnhub_api_token(self) -> Optional[str]:
            return (self.finnhub_api_key or "").strip() or None

        @property
        def http_timeout_sec(self) -> float:
            return float(self.http_timeout or 30.0)

        @property
        def version(self) -> str:
            return self.service_version

        @property
        def app_version(self) -> str:
            return self.service_version

        @property
        def app_name(self) -> str:
            return self.service_name

        @property
        def env(self) -> str:
            return self.environment

        @property
        def base_url(self) -> str:
            return (self.backend_base_url or "").rstrip("/")

    _CACHED: Optional[Settings] = None

    def get_settings() -> Settings:  # type: ignore
        global _CACHED
        if _CACHED is not None:
            return _CACHED

        sv = (os.getenv("SERVICE_VERSION") or os.getenv("APP_VERSION") or os.getenv("VERSION") or "dev").strip()
        sv = _resolve_version_from_env_or_commit(sv)

        _CACHED = Settings(
            service_name=(os.getenv("SERVICE_NAME") or os.getenv("APP_NAME") or "Tadawul Stock Analysis API").strip(),
            service_version=sv,
            environment=(os.getenv("ENVIRONMENT") or os.getenv("APP_ENV") or os.getenv("ENV") or "production").strip(),
            tz=(os.getenv("TZ") or os.getenv("TIMEZONE") or "Asia/Riyadh").strip(),
            debug=_to_bool(os.getenv("DEBUG"), False),
            log_level=(os.getenv("LOG_LEVEL") or "info").strip().lower(),
            defer_router_mount=_to_bool(os.getenv("DEFER_ROUTER_MOUNT"), True),
            init_engine_on_boot=_to_bool(os.getenv("INIT_ENGINE_ON_BOOT"), True),

            require_auth=_to_bool(os.getenv("REQUIRE_AUTH"), False),
            app_token=(os.getenv("APP_TOKEN") or os.getenv("TFB_APP_TOKEN") or "").strip() or None,
            backup_app_token=(os.getenv("BACKUP_APP_TOKEN") or "").strip() or None,

            enabled_providers_raw=(os.getenv("ENABLED_PROVIDERS") or os.getenv("PROVIDERS") or "eodhd,finnhub").strip(),
            primary_provider=(os.getenv("PRIMARY_PROVIDER") or "eodhd").strip().lower(),
            ksa_providers_raw=(os.getenv("KSA_PROVIDERS") or "yahoo_chart,tadawul,argaam").strip(),

            eodhd_api_key=(os.getenv("EODHD_API_KEY") or os.getenv("EODHD_API_TOKEN") or os.getenv("EODHD_TOKEN") or "").strip() or None,
            finnhub_api_key=(os.getenv("FINNHUB_API_KEY") or os.getenv("FINNHUB_API_TOKEN") or os.getenv("FINNHUB_TOKEN") or "").strip() or None,

            backend_base_url=(os.getenv("BACKEND_BASE_URL") or os.getenv("TFB_BASE_URL") or os.getenv("BASE_URL") or "").strip() or None,

            http_timeout=_to_float(os.getenv("HTTP_TIMEOUT_SEC") or os.getenv("HTTP_TIMEOUT"), 30.0),
            max_retries=_to_int(os.getenv("MAX_RETRIES"), 2),
            retry_delay=_to_float(os.getenv("RETRY_DELAY") or os.getenv("RETRY_DELAY_SEC"), 0.5),

            cache_ttl_sec=_to_float(os.getenv("CACHE_TTL_SEC") or os.getenv("CACHE_DEFAULT_TTL"), 20.0),
            quote_ttl_sec=_to_float(os.getenv("QUOTE_TTL_SEC"), 30.0),
            fundamentals_ttl_sec=_to_float(os.getenv("FUNDAMENTALS_TTL_SEC"), 21600.0),
            argaam_snapshot_ttl_sec=_to_float(os.getenv("ARGAAM_SNAPSHOT_TTL_SEC"), 30.0),

            engine_cache_ttl_sec=_to_float(os.getenv("ENGINE_CACHE_TTL_SEC") or os.getenv("ENGINE_TTL_SEC") or os.getenv("CACHE_TTL_SEC"), 20.0),
            enriched_batch_concurrency=_to_int(os.getenv("ENRICHED_BATCH_CONCURRENCY") or os.getenv("ENRICHED_CONCURRENCY"), 8),
        )

        # Export runtime aliases
        _export_env_if_missing("SERVICE_VERSION", _CACHED.service_version)
        _export_env_if_missing("APP_VERSION", _CACHED.service_version)
        _export_env_if_missing("VERSION", _CACHED.service_version)

        _export_env_if_missing("FINNHUB_API_TOKEN", _CACHED.finnhub_api_token)
        _export_env_if_missing("EODHD_API_TOKEN", _CACHED.eodhd_api_token)

        _export_env_if_missing("HTTP_TIMEOUT_SEC", str(float(_CACHED.http_timeout_sec)))
        _export_env_if_missing("CACHE_TTL_SEC", str(float(_CACHED.cache_ttl_sec)))
        _export_env_if_missing("CACHE_DEFAULT_TTL", str(float(_CACHED.cache_ttl_sec)))

        _export_env_if_missing("ENGINE_CACHE_TTL_SEC", str(float(_CACHED.engine_cache_ttl_sec)))
        _export_env_if_missing("ENRICHED_BATCH_CONCURRENCY", str(int(_CACHED.enriched_batch_concurrency)))
        _export_env_if_missing("ENRICHED_CONCURRENCY", str(int(_CACHED.enriched_batch_concurrency)))

        _export_env_if_missing("QUOTE_TTL_SEC", str(float(_CACHED.quote_ttl_sec)))
        _export_env_if_missing("FUNDAMENTALS_TTL_SEC", str(float(_CACHED.fundamentals_ttl_sec)))
        _export_env_if_missing("ARGAAM_SNAPSHOT_TTL_SEC", str(float(_CACHED.argaam_snapshot_ttl_sec)))

        return _CACHED


__all__ = ["Settings", "get_settings"]
