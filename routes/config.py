# config.py  (REPO ROOT) — FULL REPLACEMENT — v5.4.0
"""
config.py
============================================================
Canonical Settings for Tadawul Fast Bridge (ROOT)

✅ Single source of truth for env vars (Render names).
✅ Runtime alias export (so legacy/provider modules keep working).
✅ No network at import-time. Minimal side effects at import-time.
✅ Defensive: never crashes import-time even if pydantic-settings missing.

Version: v5.4.0
Upgrades vs v5.3.2
- ✅ Adds KSA routing feature flags (ENABLE_YFINANCE_KSA / ENABLE_YAHOO_CHART_KSA).
- ✅ Adds optional Yahoo Chart base URL.
- ✅ Ensures auth is effectively “on” when APP_TOKEN is set (require_auth derived).
- ✅ Adds safe module alias for core.config imports (best-effort).
"""

from __future__ import annotations

import os
import sys
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
        Field aliases are aligned to your Render env variable names.
        """

        model_config = SettingsConfigDict(extra="ignore", case_sensitive=False)

        # ---------------------------------------------------------------------
        # App / Meta
        # ---------------------------------------------------------------------
        service_name: str = Field(default="Tadawul Stock Analysis API", validation_alias=_alias("SERVICE_NAME", "APP_NAME"))
        service_version: str = Field(default="0.0.0", validation_alias=_alias("SERVICE_VERSION", "APP_VERSION", "VERSION"))
        environment: str = Field(default="production", validation_alias=_alias("ENVIRONMENT", "APP_ENV", "ENV"))
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
        enabled_providers_raw: str = Field(default="eodhd,finnhub", validation_alias=_alias("ENABLED_PROVIDERS", "PROVIDERS"))
        primary_provider: str = Field(default="eodhd", validation_alias=_alias("PRIMARY_PROVIDER"))
        ksa_providers_raw: str = Field(default="yahoo_chart,tadawul,argaam", validation_alias=_alias("KSA_PROVIDERS"))

        # KSA routing feature flags (used by v2 engine)
        enable_yfinance_ksa: bool = Field(default=False, validation_alias=_alias("ENABLE_YFINANCE_KSA"))
        enable_yahoo_chart_ksa: bool = Field(default=True, validation_alias=_alias("ENABLE_YAHOO_CHART_KSA"))

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

        # Optional Yahoo chart base url (if you ever override)
        yahoo_chart_base_url: str = Field(default="https://query1.finance.yahoo.com", validation_alias=_alias("YAHOO_CHART_BASE_URL"))

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

        cache_max_size: int = Field(default=5000, validation_alias=_alias("CACHE_MAX_SIZE"))
        cache_backup_enabled: bool = Field(default=False, validation_alias=_alias("CACHE_BACKUP_ENABLED"))
        cache_save_interval: int = Field(default=300, validation_alias=_alias("CACHE_SAVE_INTERVAL"))

        # ---------------------------------------------------------------------
        # Advanced Analysis batching (routes/advanced_analysis.py)
        # ---------------------------------------------------------------------
        adv_batch_size: int = Field(default=25, validation_alias=_alias("ADV_BATCH_SIZE"))
        adv_batch_timeout_sec: float = Field(default=45.0, validation_alias=_alias("ADV_BATCH_TIMEOUT_SEC"))
        adv_max_tickers: int = Field(default=500, validation_alias=_alias("ADV_MAX_TICKERS"))
        adv_batch_concurrency: int = Field(default=6, validation_alias=_alias("ADV_BATCH_CONCURRENCY"))

        # ---------------------------------------------------------------------
        # Google / Sheets
        # ---------------------------------------------------------------------
        default_spreadsheet_id: Optional[str] = Field(default=None, validation_alias=_alias("DEFAULT_SPREADSHEET_ID", "SPREADSHEET_ID"))
        google_sheets_credentials: Optional[str] = Field(default=None, validation_alias=_alias("GOOGLE_SHEETS_CREDENTIALS", "GOOGLE_CREDENTIALS"))
        google_credentials: Optional[str] = Field(default=None, validation_alias=_alias("GOOGLE_CREDENTIALS"))
        google_apps_script_url: Optional[str] = Field(default=None, validation_alias=_alias("GOOGLE_APPS_SCRIPT_URL"))
        google_apps_script_backup_url: Optional[str] = Field(default=None, validation_alias=_alias("GOOGLE_APPS_SCRIPT_BACKUP_URL"))

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
                "ksa_flags": {
                    "enable_yfinance_ksa": bool(self.enable_yfinance_ksa),
                    "enable_yahoo_chart_ksa": bool(self.enable_yahoo_chart_ksa),
                },
                "http_timeout_sec": float(self.http_timeout_sec),
                "max_retries": int(self.max_retries),
                "retry_delay": float(self.retry_delay),
                "cache_ttl_sec": float(self.cache_ttl_sec),
                "quote_ttl_sec": float(self.quote_ttl_sec),
                "fundamentals_ttl_sec": float(self.fundamentals_ttl_sec),
                "argaam_snapshot_ttl_sec": float(self.argaam_snapshot_ttl_sec),
                "cache_max_size": int(self.cache_max_size),
                "advanced_batching": {
                    "adv_batch_size": int(self.adv_batch_size),
                    "adv_batch_timeout_sec": float(self.adv_batch_timeout_sec),
                    "adv_max_tickers": int(self.adv_max_tickers),
                    "adv_batch_concurrency": int(self.adv_batch_concurrency),
                },
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
                "google_creds_set": bool((self.google_sheets_credentials or "").strip() or (self.google_credentials or "").strip()),
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
                    "ADV_BATCH_TIMEOUT_SEC": "adv_batch_timeout_sec",
                    "ADV_BATCH_SIZE": "adv_batch_size",
                    "ADV_MAX_TICKERS": "adv_max_tickers",
                    "ADV_BATCH_CONCURRENCY": "adv_batch_concurrency",
                }
                if name in mapping and hasattr(self, mapping[name]):
                    return getattr(self, mapping[name])
            raise AttributeError(name)

    def _apply_runtime_env_aliases(s: Settings) -> None:
        """
        Make providers + legacy modules work with your Render env names WITHOUT changing Render.
        """
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

        _export_env_if_missing("QUOTE_TTL_SEC", str(float(s.quote_ttl_sec)))
        _export_env_if_missing("FUNDAMENTALS_TTL_SEC", str(float(s.fundamentals_ttl_sec)))
        _export_env_if_missing("ARGAAM_SNAPSHOT_TTL_SEC", str(float(s.argaam_snapshot_ttl_sec)))

        # Advanced Analysis env (so routes can read from os.getenv too)
        _export_env_if_missing("ADV_BATCH_SIZE", str(int(s.adv_batch_size)))
        _export_env_if_missing("ADV_BATCH_TIMEOUT_SEC", str(float(s.adv_batch_timeout_sec)))
        _export_env_if_missing("ADV_MAX_TICKERS", str(int(s.adv_max_tickers)))
        _export_env_if_missing("ADV_BATCH_CONCURRENCY", str(int(s.adv_batch_concurrency)))

        # Base URLs for provider modules that read env at call-time
        _export_env_if_missing("EODHD_BASE_URL", s.eodhd_base_url)
        _export_env_if_missing("FINNHUB_BASE_URL", s.finnhub_base_url)
        _export_env_if_missing("YAHOO_CHART_BASE_URL", s.yahoo_chart_base_url)

        # Providers lists (some older modules read these at runtime)
        _export_env_if_missing("ENABLED_PROVIDERS", ",".join(s.enabled_providers))
        _export_env_if_missing("KSA_PROVIDERS", ",".join(s.ksa_providers))
        _export_env_if_missing("PRIMARY_PROVIDER", (s.primary_provider or "eodhd").strip().lower())

        # Optional base URL alias
        if s.backend_base_url:
            _export_env_if_missing("BACKEND_BASE_URL", (s.backend_base_url or "").rstrip("/"))
            _export_env_if_missing("BASE_URL", (s.backend_base_url or "").rstrip("/"))

    @lru_cache(maxsize=1)
    def get_settings() -> Settings:
        s = Settings()

        # Normalize / coerce
        s.log_level = (s.log_level or "info").strip().lower()
        s.primary_provider = (s.primary_provider or "eodhd").strip().lower()

        s.debug = _to_bool(s.debug, False)

        s.enable_rate_limiting = _to_bool(s.enable_rate_limiting, True)
        s.cache_backup_enabled = _to_bool(s.cache_backup_enabled, False)
        s.advanced_analysis_enabled = _to_bool(s.advanced_analysis_enabled, True)
        s.tadawul_market_enabled = _to_bool(s.tadawul_market_enabled, True)
        s.enable_cors_all_origins = _to_bool(s.enable_cors_all_origins, True)

        s.enable_yfinance_ksa = _to_bool(s.enable_yfinance_ksa, False)
        s.enable_yahoo_chart_ksa = _to_bool(s.enable_yahoo_chart_ksa, True)

        s.max_requests_per_minute = _to_int(s.max_requests_per_minute, 240)
        s.max_retries = _to_int(s.max_retries, 2)

        s.http_timeout = _to_float(s.http_timeout, 30.0)
        s.retry_delay = _to_float(s.retry_delay, 0.5)

        s.cache_ttl_sec = _to_float(s.cache_ttl_sec, 20.0)
        s.quote_ttl_sec = _to_float(s.quote_ttl_sec, 30.0)
        s.fundamentals_ttl_sec = _to_float(s.fundamentals_ttl_sec, 21600.0)
        s.argaam_snapshot_ttl_sec = _to_float(s.argaam_snapshot_ttl_sec, 30.0)

        s.cache_max_size = _to_int(s.cache_max_size, 5000)
        s.cache_save_interval = _to_int(s.cache_save_interval, 300)

        # Advanced Analysis clamps (same intent as routes)
        s.adv_batch_size = max(5, min(200, _to_int(s.adv_batch_size, 25)))
        s.adv_batch_timeout_sec = max(5.0, min(180.0, _to_float(s.adv_batch_timeout_sec, 45.0)))
        s.adv_max_tickers = max(10, min(2000, _to_int(s.adv_max_tickers, 500)))
        s.adv_batch_concurrency = max(1, min(25, _to_int(s.adv_batch_concurrency, 6)))

        # Auth: if token is set, effectively require auth
        tok_set = bool((s.app_token or "").strip() or (s.backup_app_token or "").strip())
        s.require_auth = _to_bool(s.require_auth, False) or tok_set

        _apply_runtime_env_aliases(s)
        return s

except Exception:  # pragma: no cover
    # =============================================================================
    # LAST-RESORT FALLBACK (no pydantic-settings available)
    # =============================================================================
    try:
        from pydantic import BaseModel  # type: ignore
    except Exception:  # pragma: no cover

        class BaseModel:  # type: ignore
            def __init__(self, **kwargs: Any):
                for k, v in kwargs.items():
                    setattr(self, k, v)

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
        ksa_providers_raw: str = "yahoo_chart,tadawul,argaam"

        enable_yfinance_ksa: bool = False
        enable_yahoo_chart_ksa: bool = True

        eodhd_api_key: Optional[str] = None
        finnhub_api_key: Optional[str] = None
        fmp_api_key: Optional[str] = None
        alpha_vantage_api_key: Optional[str] = None
        argaam_api_key: Optional[str] = None

        eodhd_base_url: str = "https://eodhistoricaldata.com/api"
        finnhub_base_url: str = "https://finnhub.io/api/v1"
        yahoo_chart_base_url: str = "https://query1.finance.yahoo.com"
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
        cache_max_size: int = 5000
        cache_backup_enabled: bool = False
        cache_save_interval: int = 300

        # Advanced analysis
        adv_batch_size: int = 25
        adv_batch_timeout_sec: float = 45.0
        adv_max_tickers: int = 500
        adv_batch_concurrency: int = 6

        default_spreadsheet_id: Optional[str] = None
        google_sheets_credentials: Optional[str] = None
        google_credentials: Optional[str] = None
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
        def ksa_providers(self) -> List[str]:
            return _csv(self.ksa_providers_raw, lower=True) or ["yahoo_chart", "tadawul", "argaam"]

        @property
        def eodhd_api_token(self) -> Optional[str]:
            return (self.eodhd_api_key or "").strip() or None

        @property
        def finnhub_api_token(self) -> Optional[str]:
            return (self.finnhub_api_key or "").strip() or None

        @property
        def http_timeout_sec(self) -> float:
            return float(getattr(self, "http_timeout", 30.0) or 30.0)

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
                "ksa_flags": {
                    "enable_yfinance_ksa": bool(self.enable_yfinance_ksa),
                    "enable_yahoo_chart_ksa": bool(self.enable_yahoo_chart_ksa),
                },
                "http_timeout_sec": float(self.http_timeout_sec),
                "max_retries": int(self.max_retries),
                "retry_delay": float(self.retry_delay),
                "cache_ttl_sec": float(self.cache_ttl_sec),
                "quote_ttl_sec": float(self.quote_ttl_sec),
                "fundamentals_ttl_sec": float(self.fundamentals_ttl_sec),
                "argaam_snapshot_ttl_sec": float(self.argaam_snapshot_ttl_sec),
                "cache_max_size": int(self.cache_max_size),
                "advanced_batching": {
                    "adv_batch_size": int(self.adv_batch_size),
                    "adv_batch_timeout_sec": float(self.adv_batch_timeout_sec),
                    "adv_max_tickers": int(self.adv_max_tickers),
                    "adv_batch_concurrency": int(self.adv_batch_concurrency),
                },
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
                "google_creds_set": bool((self.google_sheets_credentials or "").strip() or (self.google_credentials or "").strip()),
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
                    "ADV_BATCH_TIMEOUT_SEC": "adv_batch_timeout_sec",
                    "ADV_BATCH_SIZE": "adv_batch_size",
                    "ADV_MAX_TICKERS": "adv_max_tickers",
                    "ADV_BATCH_CONCURRENCY": "adv_batch_concurrency",
                }
                if name in mapping and hasattr(self, mapping[name]):
                    return getattr(self, mapping[name])
            raise AttributeError(name)

    def _apply_runtime_env_aliases(s: Settings) -> None:
        """
        Make providers + legacy modules work with your Render env names WITHOUT changing Render.
        """
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

        _export_env_if_missing("QUOTE_TTL_SEC", str(float(s.quote_ttl_sec)))
        _export_env_if_missing("FUNDAMENTALS_TTL_SEC", str(float(s.fundamentals_ttl_sec)))
        _export_env_if_missing("ARGAAM_SNAPSHOT_TTL_SEC", str(float(s.argaam_snapshot_ttl_sec)))

        # Advanced Analysis env (so routes can read from os.getenv too)
        _export_env_if_missing("ADV_BATCH_SIZE", str(int(s.adv_batch_size)))
        _export_env_if_missing("ADV_BATCH_TIMEOUT_SEC", str(float(s.adv_batch_timeout_sec)))
        _export_env_if_missing("ADV_MAX_TICKERS", str(int(s.adv_max_tickers)))
        _export_env_if_missing("ADV_BATCH_CONCURRENCY", str(int(s.adv_batch_concurrency)))

        # Base URLs for provider modules that read env at call-time
        _export_env_if_missing("EODHD_BASE_URL", s.eodhd_base_url)
        _export_env_if_missing("FINNHUB_BASE_URL", s.finnhub_base_url)
        _export_env_if_missing("YAHOO_CHART_BASE_URL", s.yahoo_chart_base_url)

        # Providers lists (some older modules read these at runtime)
        _export_env_if_missing("ENABLED_PROVIDERS", ",".join(s.enabled_providers))
        _export_env_if_missing("KSA_PROVIDERS", ",".join(s.ksa_providers))
        _export_env_if_missing("PRIMARY_PROVIDER", (s.primary_provider or "eodhd").strip().lower())

        # Optional base URL alias
        if s.backend_base_url:
            _export_env_if_missing("BACKEND_BASE_URL", (s.backend_base_url or "").rstrip("/"))
            _export_env_if_missing("BASE_URL", (s.backend_base_url or "").rstrip("/"))

    @lru_cache(maxsize=1)
    def get_settings() -> Settings:
        s = Settings()

        # Normalize / coerce
        s.log_level = (s.log_level or "info").strip().lower()
        s.primary_provider = (s.primary_provider or "eodhd").strip().lower()

        s.debug = _to_bool(s.debug, False)

        s.enable_rate_limiting = _to_bool(s.enable_rate_limiting, True)
        s.cache_backup_enabled = _to_bool(s.cache_backup_enabled, False)
        s.advanced_analysis_enabled = _to_bool(s.advanced_analysis_enabled, True)
        s.tadawul_market_enabled = _to_bool(s.tadawul_market_enabled, True)
        s.enable_cors_all_origins = _to_bool(s.enable_cors_all_origins, True)

        s.enable_yfinance_ksa = _to_bool(s.enable_yfinance_ksa, False)
        s.enable_yahoo_chart_ksa = _to_bool(s.enable_yahoo_chart_ksa, True)

        s.max_requests_per_minute = _to_int(s.max_requests_per_minute, 240)
        s.max_retries = _to_int(s.max_retries, 2)

        s.http_timeout = _to_float(s.http_timeout, 30.0)
        s.retry_delay = _to_float(s.retry_delay, 0.5)

        s.cache_ttl_sec = _to_float(s.cache_ttl_sec, 20.0)
        s.quote_ttl_sec = _to_float(s.quote_ttl_sec, 30.0)
        s.fundamentals_ttl_sec = _to_float(s.fundamentals_ttl_sec, 21600.0)
        s.argaam_snapshot_ttl_sec = _to_float(s.argaam_snapshot_ttl_sec, 30.0)

        s.cache_max_size = _to_int(s.cache_max_size, 5000)
        s.cache_save_interval = _to_int(s.cache_save_interval, 300)

        # Advanced Analysis clamps (same intent as routes)
        s.adv_batch_size = max(5, min(200, _to_int(s.adv_batch_size, 25)))
        s.adv_batch_timeout_sec = max(5.0, min(180.0, _to_float(s.adv_batch_timeout_sec, 45.0)))
        s.adv_max_tickers = max(10, min(2000, _to_int(s.adv_max_tickers, 500)))
        s.adv_batch_concurrency = max(1, min(25, _to_int(s.adv_batch_concurrency, 6)))

        # Auth: if token is set, effectively require auth
        tok_set = bool((s.app_token or "").strip() or (s.backup_app_token or "").strip())
        s.require_auth = _to_bool(s.require_auth, False) or tok_set

        _apply_runtime_env_aliases(s)
        return s


# -----------------------------------------------------------------------------
# Best-effort module alias so `from core.config import get_settings` can work
# when this file is used as the canonical implementation.
# -----------------------------------------------------------------------------
try:
    sys.modules.setdefault("core.config", sys.modules[__name__])
except Exception:
    pass


__all__ = ["Settings", "get_settings"]
