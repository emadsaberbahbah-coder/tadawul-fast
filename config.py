# config.py  (REPO ROOT) — FULL REPLACEMENT — v5.3.7
"""
config.py
============================================================
Canonical Settings for Tadawul Fast Bridge (ROOT)

✅ Single source of truth for env vars (Render names).
✅ Runtime alias export (so legacy/provider modules keep working).
✅ No network at import-time. Minimal side effects at import-time.
✅ FULL fallback included (boots even if pydantic-settings is unavailable).

Version: v5.3.7 (ENHANCED + ALIGNED)

Key alignment guarantees (DO NOT rename Render env vars):
- FINNHUB_API_KEY / EODHD_API_KEY
- ENABLED_PROVIDERS / PRIMARY_PROVIDER / KSA_PROVIDERS
- HTTP_TIMEOUT / HTTP_TIMEOUT_SEC / MAX_RETRIES / RETRY_DELAY / RETRY_DELAY_SEC
- CACHE_DEFAULT_TTL / CACHE_TTL_SEC / ENGINE_CACHE_TTL_SEC / CACHE_MAX_SIZE / CACHE_BACKUP_ENABLED / CACHE_SAVE_INTERVAL
- CORS_ORIGINS / ENABLE_CORS_ALL_ORIGINS (CORS_ALL_ORIGINS) / ENABLE_RATE_LIMITING / MAX_REQUESTS_PER_MINUTE
- DEFAULT_SPREADSHEET_ID / GOOGLE_SHEETS_CREDENTIALS / GOOGLE_CREDENTIALS / GOOGLE_APPS_SCRIPT_URL / GOOGLE_APPS_SCRIPT_BACKUP_URL
- SERVICE_NAME / SERVICE_VERSION / ENVIRONMENT / TZ / DEBUG / LOG_LEVEL / LOG_FORMAT
- ADVANCED_ANALYSIS_ENABLED / AI_ANALYSIS_ENABLED / TADAWUL_MARKET_ENABLED / ENABLE_SWAGGER / ENABLE_REDOC
- DEFER_ROUTER_MOUNT / INIT_ENGINE_ON_BOOT
- YAHOO_CHART_BASE_URL (optional)

Engine/provider flags (exported as env aliases for legacy env-readers):
- ENABLE_YAHOO_CHART_KSA
- ENABLE_YAHOO_CHART_SUPPLEMENT
- ENABLE_YFINANCE_KSA
- ENABLE_YAHOO_FUNDAMENTALS_KSA
- ENABLE_YAHOO_FUNDAMENTALS_GLOBAL

v5.3.7 upgrades
- ✅ Enforces require_auth=True automatically when APP_TOKEN/BACKUP_APP_TOKEN is set.
- ✅ Adds optional YAHOO_CHART_BASE_URL setting + alias export.
- ✅ Best-effort module alias: `core.config` -> this module (helps legacy imports).
"""

from __future__ import annotations

import json
import os
import sys
from functools import lru_cache
from typing import Any, Dict, List, Optional

_TRUTHY = {"1", "true", "yes", "y", "on", "t"}
_FALSY = {"0", "false", "no", "n", "off", "f"}

_UNSET_STRINGS = {"", "0.0.0", "unknown", "none", "null", "unset", "n/a", "na"}


# =============================================================================
# Helpers (no heavy deps, no network)
# =============================================================================
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


def _positive_int(v: Any, default: int) -> int:
    x = _to_int(v, default)
    return x if x > 0 else default


def _positive_float(v: Any, default: float) -> float:
    x = _to_float(v, default)
    return x if x > 0 else default


def _is_unset_string(s: str) -> bool:
    return str(s or "").strip().lower() in _UNSET_STRINGS


def _resolve_version_from_env_or_commit(service_version: str) -> str:
    """
    Version priority:
      1) SERVICE_VERSION / APP_VERSION / VERSION / RELEASE
      2) Provided service_version (if meaningful)
      3) Render commit short hash
      4) "dev"
    """
    for k in ("SERVICE_VERSION", "APP_VERSION", "VERSION", "RELEASE"):
        v = (os.getenv(k) or "").strip()
        if v and not _is_unset_string(v):
            return v

    sv = (service_version or "").strip()
    if sv and not _is_unset_string(sv):
        return sv

    commit = (os.getenv("RENDER_GIT_COMMIT") or os.getenv("GIT_COMMIT") or "").strip()
    if commit:
        return commit[:7]

    return "dev"


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


def _parse_listlike(v: Any, *, lower: bool = True) -> List[str]:
    """
    Accept:
      - list
      - CSV string
      - JSON list string: '["eodhd","finnhub"]'
    Return: deduped list (order preserved)
    """
    if v is None:
        return []
    items: List[str] = []

    if isinstance(v, list):
        items = [str(x).strip() for x in v if str(x).strip()]
    else:
        s = str(v).strip()
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
        y = x.lower() if lower else x
        if not y or y in seen:
            continue
        seen.add(y)
        out.append(y)
    return out


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
        service_name: str = Field(default="Tadawul Stock Analysis API", validation_alias=_alias("SERVICE_NAME", "APP_NAME"))
        service_version: str = Field(default="dev", validation_alias=_alias("SERVICE_VERSION", "APP_VERSION", "VERSION", "RELEASE"))
        environment: str = Field(default="production", validation_alias=_alias("ENVIRONMENT", "APP_ENV", "ENV"))
        tz: str = Field(default="Asia/Riyadh", validation_alias=_alias("TZ", "TIMEZONE"))
        debug: bool = Field(default=False, validation_alias=_alias("DEBUG"))
        log_level: str = Field(default="info", validation_alias=_alias("LOG_LEVEL"))
        log_format: str = Field(
            default="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
            validation_alias=_alias("LOG_FORMAT"),
        )

        # ---------------------------------------------------------------------
        # Boot / engine flags
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
        enabled_providers_raw: str = Field(default="eodhd,finnhub,fmp", validation_alias=_alias("ENABLED_PROVIDERS", "PROVIDERS"))
        primary_provider: str = Field(default="eodhd", validation_alias=_alias("PRIMARY_PROVIDER"))
        ksa_providers_raw: str = Field(default="yahoo_chart,tadawul,argaam", validation_alias=_alias("KSA_PROVIDERS"))

        # ---------------------------------------------------------------------
        # Provider keys/tokens
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

        # Optional (only used if you override; safe default)
        yahoo_chart_base_url: str = Field(default="https://query1.finance.yahoo.com", validation_alias=_alias("YAHOO_CHART_BASE_URL"))

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
        # Cache tuning
        # ---------------------------------------------------------------------
        cache_ttl_sec: float = Field(default=20.0, validation_alias=_alias("CACHE_TTL_SEC", "CACHE_DEFAULT_TTL"))
        quote_ttl_sec: float = Field(default=30.0, validation_alias=_alias("QUOTE_TTL_SEC"))
        fundamentals_ttl_sec: float = Field(default=21600.0, validation_alias=_alias("FUNDAMENTALS_TTL_SEC"))
        argaam_snapshot_ttl_sec: float = Field(default=30.0, validation_alias=_alias("ARGAAM_SNAPSHOT_TTL_SEC"))

        engine_cache_ttl_sec: float = Field(default=20.0, validation_alias=_alias("ENGINE_CACHE_TTL_SEC", "ENGINE_TTL_SEC", "CACHE_TTL_SEC"))
        enriched_batch_concurrency: int = Field(default=8, validation_alias=_alias("ENRICHED_BATCH_CONCURRENCY", "ENRICHED_CONCURRENCY"))

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
        # Feature flags (general)
        # ---------------------------------------------------------------------
        ai_analysis_enabled: bool = Field(default=True, validation_alias=_alias("AI_ANALYSIS_ENABLED"))
        advanced_analysis_enabled: bool = Field(default=True, validation_alias=_alias("ADVANCED_ANALYSIS_ENABLED"))
        tadawul_market_enabled: bool = Field(default=True, validation_alias=_alias("TADAWUL_MARKET_ENABLED"))
        enable_swagger: bool = Field(default=True, validation_alias=_alias("ENABLE_SWAGGER"))
        enable_redoc: bool = Field(default=True, validation_alias=_alias("ENABLE_REDOC"))

        # ---------------------------------------------------------------------
        # Feature flags (engine/provider routing)
        # ---------------------------------------------------------------------
        enable_yahoo_chart_ksa: bool = Field(default=True, validation_alias=_alias("ENABLE_YAHOO_CHART_KSA"))
        enable_yahoo_chart_supplement: bool = Field(default=True, validation_alias=_alias("ENABLE_YAHOO_CHART_SUPPLEMENT"))
        enable_yfinance_ksa: bool = Field(default=False, validation_alias=_alias("ENABLE_YFINANCE_KSA"))
        enable_yahoo_fundamentals_ksa: bool = Field(default=True, validation_alias=_alias("ENABLE_YAHOO_FUNDAMENTALS_KSA"))
        enable_yahoo_fundamentals_global: bool = Field(default=False, validation_alias=_alias("ENABLE_YAHOO_FUNDAMENTALS_GLOBAL"))

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
            xs = _parse_listlike(self.enabled_providers_raw, lower=True)
            return xs or ["eodhd", "finnhub", "fmp"]

        @property
        def ksa_providers(self) -> List[str]:
            xs = _parse_listlike(self.ksa_providers_raw, lower=True)
            return xs or ["yahoo_chart", "tadawul", "argaam"]

        @property
        def cors_origins_list(self) -> List[str]:
            if bool(self.enable_cors_all_origins):
                return ["*"]
            s = (self.cors_origins or "").strip()
            if not s or s == "*":
                return ["*"]
            return [x.strip() for x in s.split(",") if x.strip()]

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

        @property
        def google_credentials(self) -> Optional[str]:
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
                "app_token_set": bool((self.app_token or "").strip()),
                "backup_app_token_set": bool((self.backup_app_token or "").strip()),
                "app_token_mask": _mask_tail(self.app_token, keep=4),
                "enabled_providers": self.enabled_providers,
                "primary_provider": (self.primary_provider or "eodhd").strip().lower(),
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
                "feature_flags": {
                    "AI_ANALYSIS_ENABLED": bool(self.ai_analysis_enabled),
                    "ADVANCED_ANALYSIS_ENABLED": bool(self.advanced_analysis_enabled),
                    "TADAWUL_MARKET_ENABLED": bool(self.tadawul_market_enabled),
                    "ENABLE_SWAGGER": bool(self.enable_swagger),
                    "ENABLE_REDOC": bool(self.enable_redoc),
                },
                "engine_flags": {
                    "ENABLE_YAHOO_CHART_KSA": bool(self.enable_yahoo_chart_ksa),
                    "ENABLE_YAHOO_CHART_SUPPLEMENT": bool(self.enable_yahoo_chart_supplement),
                    "ENABLE_YFINANCE_KSA": bool(self.enable_yfinance_ksa),
                    "ENABLE_YAHOO_FUNDAMENTALS_KSA": bool(self.enable_yahoo_fundamentals_ksa),
                    "ENABLE_YAHOO_FUNDAMENTALS_GLOBAL": bool(self.enable_yahoo_fundamentals_global),
                },
                "base_urls": {
                    "EODHD_BASE_URL": self.eodhd_base_url,
                    "FINNHUB_BASE_URL": self.finnhub_base_url,
                    "YAHOO_CHART_BASE_URL": self.yahoo_chart_base_url,
                    "BACKEND_BASE_URL": (self.backend_base_url or "").rstrip("/"),
                },
                "keys_set": {
                    "eodhd": bool((self.eodhd_api_key or "").strip()),
                    "finnhub": bool((self.finnhub_api_key or "").strip()),
                },
                "google": {
                    "default_spreadsheet_id_set": bool((self.default_spreadsheet_id or "").strip()),
                    "google_creds_set": bool((self.google_sheets_credentials or "").strip()),
                    "apps_script_url_set": bool((self.google_apps_script_url or "").strip()),
                },
            }

        def __getattr__(self, name: str) -> Any:
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
        Executed ONLY inside get_settings().
        """
        # Version keys
        ver = _resolve_version_from_env_or_commit(s.service_version)
        s.service_version = ver
        _export_env_if_missing("SERVICE_VERSION", ver)
        _export_env_if_missing("APP_VERSION", ver)
        _export_env_if_missing("VERSION", ver)
        _export_env_if_missing("RELEASE", ver)

        # App/name/env aliases
        _export_env_if_missing("APP_NAME", s.service_name)
        _export_env_if_missing("SERVICE_NAME", s.service_name)
        _export_env_if_missing("APP_ENV", s.environment)
        _export_env_if_missing("ENVIRONMENT", s.environment)
        _export_env_if_missing("ENV", s.environment)
        _export_env_if_missing("TZ", s.tz)
        _export_env_if_missing("TIMEZONE", s.tz)

        # Logging
        _export_env_if_missing("LOG_LEVEL", (s.log_level or "info").strip().lower())
        _export_env_if_missing("LOG_FORMAT", (s.log_format or "").strip())

        # Auth aliases
        if (s.app_token or "").strip():
            _export_env_if_missing("APP_TOKEN", (s.app_token or "").strip())
            _export_env_if_missing("TFB_APP_TOKEN", (s.app_token or "").strip())
        if (s.backup_app_token or "").strip():
            _export_env_if_missing("BACKUP_APP_TOKEN", (s.backup_app_token or "").strip())

        # Provider token aliases
        _export_env_if_missing("FINNHUB_API_TOKEN", s.finnhub_api_token)
        _export_env_if_missing("FINNHUB_TOKEN", s.finnhub_api_token)
        _export_env_if_missing("EODHD_API_TOKEN", s.eodhd_api_token)
        _export_env_if_missing("EODHD_TOKEN", s.eodhd_api_token)

        # Providers lists for legacy env-readers
        _export_env_if_missing("ENABLED_PROVIDERS", (s.enabled_providers_raw or "").strip())
        _export_env_if_missing("PROVIDERS", (s.enabled_providers_raw or "").strip())
        _export_env_if_missing("KSA_PROVIDERS", (s.ksa_providers_raw or "").strip())
        _export_env_if_missing("PRIMARY_PROVIDER", (s.primary_provider or "eodhd").strip().lower())

        # Timeout aliases
        _export_env_if_missing("HTTP_TIMEOUT_SEC", str(float(s.http_timeout_sec)))
        _export_env_if_missing("HTTP_TIMEOUT", str(float(s.http_timeout_sec)))
        _export_env_if_missing("MAX_RETRIES", str(int(s.max_retries)))
        _export_env_if_missing("RETRY_DELAY", str(float(s.retry_delay)))
        _export_env_if_missing("RETRY_DELAY_SEC", str(float(s.retry_delay)))

        # TTL aliases
        _export_env_if_missing("CACHE_TTL_SEC", str(float(s.cache_ttl_sec)))
        _export_env_if_missing("CACHE_DEFAULT_TTL", str(float(s.cache_ttl_sec)))
        _export_env_if_missing("ENGINE_CACHE_TTL_SEC", str(float(s.engine_cache_ttl_sec)))
        _export_env_if_missing("ENRICHED_BATCH_CONCURRENCY", str(int(s.enriched_batch_concurrency)))
        _export_env_if_missing("ENRICHED_CONCURRENCY", str(int(s.enriched_batch_concurrency)))
        _export_env_if_missing("QUOTE_TTL_SEC", str(float(s.quote_ttl_sec)))
        _export_env_if_missing("FUNDAMENTALS_TTL_SEC", str(float(s.fundamentals_ttl_sec)))
        _export_env_if_missing("ARGAAM_SNAPSHOT_TTL_SEC", str(float(s.argaam_snapshot_ttl_sec)))

        # Cache sizing / backup
        _export_env_if_missing("CACHE_MAX_SIZE", str(int(s.cache_max_size)))
        _export_env_if_missing("CACHE_BACKUP_ENABLED", str(bool(s.cache_backup_enabled)).lower())
        _export_env_if_missing("CACHE_SAVE_INTERVAL", str(int(s.cache_save_interval)))

        # CORS / RL
        _export_env_if_missing("ENABLE_CORS_ALL_ORIGINS", str(bool(s.enable_cors_all_origins)).lower())
        _export_env_if_missing("CORS_ALL_ORIGINS", str(bool(s.enable_cors_all_origins)).lower())
        _export_env_if_missing("CORS_ORIGINS", (s.cors_origins or "*").strip())
        _export_env_if_missing("ENABLE_RATE_LIMITING", str(bool(s.enable_rate_limiting)).lower())
        _export_env_if_missing("MAX_REQUESTS_PER_MINUTE", str(int(s.max_requests_per_minute)))

        # Feature flags
        _export_env_if_missing("AI_ANALYSIS_ENABLED", str(bool(s.ai_analysis_enabled)).lower())
        _export_env_if_missing("ADVANCED_ANALYSIS_ENABLED", str(bool(s.advanced_analysis_enabled)).lower())
        _export_env_if_missing("TADAWUL_MARKET_ENABLED", str(bool(s.tadawul_market_enabled)).lower())
        _export_env_if_missing("ENABLE_SWAGGER", str(bool(s.enable_swagger)).lower())
        _export_env_if_missing("ENABLE_REDOC", str(bool(s.enable_redoc)).lower())

        # Engine/provider flags
        _export_env_if_missing("ENABLE_YAHOO_CHART_KSA", str(bool(s.enable_yahoo_chart_ksa)).lower())
        _export_env_if_missing("ENABLE_YAHOO_CHART_SUPPLEMENT", str(bool(s.enable_yahoo_chart_supplement)).lower())
        _export_env_if_missing("ENABLE_YFINANCE_KSA", str(bool(s.enable_yfinance_ksa)).lower())
        _export_env_if_missing("ENABLE_YAHOO_FUNDAMENTALS_KSA", str(bool(s.enable_yahoo_fundamentals_ksa)).lower())
        _export_env_if_missing("ENABLE_YAHOO_FUNDAMENTALS_GLOBAL", str(bool(s.enable_yahoo_fundamentals_global)).lower())

        # Base URLs
        _export_env_if_missing("EODHD_BASE_URL", s.eodhd_base_url)
        _export_env_if_missing("FINNHUB_BASE_URL", s.finnhub_base_url)
        _export_env_if_missing("YAHOO_CHART_BASE_URL", s.yahoo_chart_base_url)

        if s.backend_base_url:
            base = (s.backend_base_url or "").rstrip("/")
            _export_env_if_missing("BACKEND_BASE_URL", base)
            _export_env_if_missing("TFB_BASE_URL", base)
            _export_env_if_missing("BASE_URL", base)

        # Spreadsheet ID alias
        if s.default_spreadsheet_id:
            _export_env_if_missing("DEFAULT_SPREADSHEET_ID", s.default_spreadsheet_id)
            _export_env_if_missing("SPREADSHEET_ID", s.default_spreadsheet_id)
            _export_env_if_missing("GOOGLE_SHEETS_ID", s.default_spreadsheet_id)

        # Google creds alias
        if s.google_sheets_credentials:
            _export_env_if_missing("GOOGLE_SHEETS_CREDENTIALS", s.google_sheets_credentials)
            _export_env_if_missing("GOOGLE_CREDENTIALS", s.google_sheets_credentials)
            _export_env_if_missing("GOOGLE_SA_JSON", s.google_sheets_credentials)

        if s.google_apps_script_url:
            _export_env_if_missing("GOOGLE_APPS_SCRIPT_URL", s.google_apps_script_url)
        if s.google_apps_script_backup_url:
            _export_env_if_missing("GOOGLE_APPS_SCRIPT_BACKUP_URL", s.google_apps_script_backup_url)

        # Optional KSA endpoint aliases
        if s.tadawul_quote_url:
            _export_env_if_missing("TADAWUL_QUOTE_URL", s.tadawul_quote_url)
        if s.tadawul_fundamentals_url:
            _export_env_if_missing("TADAWUL_FUNDAMENTALS_URL", s.tadawul_fundamentals_url)
        if s.argaam_quote_url:
            _export_env_if_missing("ARGAAM_QUOTE_URL", s.argaam_quote_url)
        if s.argaam_profile_url:
            _export_env_if_missing("ARGAAM_PROFILE_URL", s.argaam_profile_url)

    @lru_cache(maxsize=1)
    def get_settings() -> Settings:
        s = Settings()

        # Normalize strings
        s.log_level = (s.log_level or "info").strip().lower()
        s.primary_provider = (s.primary_provider or "eodhd").strip().lower()
        s.environment = (s.environment or "production").strip()
        s.tz = (s.tz or "Asia/Riyadh").strip()
        s.backend_base_url = (s.backend_base_url or "").strip() or None

        # Normalize bools
        s.debug = _to_bool(s.debug, False)
        s.enable_rate_limiting = _to_bool(s.enable_rate_limiting, True)
        s.cache_backup_enabled = _to_bool(s.cache_backup_enabled, False)
        s.advanced_analysis_enabled = _to_bool(s.advanced_analysis_enabled, True)
        s.ai_analysis_enabled = _to_bool(s.ai_analysis_enabled, True)
        s.tadawul_market_enabled = _to_bool(s.tadawul_market_enabled, True)
        s.enable_cors_all_origins = _to_bool(s.enable_cors_all_origins, True)
        s.defer_router_mount = _to_bool(s.defer_router_mount, True)
        s.init_engine_on_boot = _to_bool(s.init_engine_on_boot, True)

        # Engine/provider routing flags
        s.enable_yahoo_chart_ksa = _to_bool(s.enable_yahoo_chart_ksa, True)
        s.enable_yahoo_chart_supplement = _to_bool(s.enable_yahoo_chart_supplement, True)
        s.enable_yfinance_ksa = _to_bool(s.enable_yfinance_ksa, False)
        s.enable_yahoo_fundamentals_ksa = _to_bool(s.enable_yahoo_fundamentals_ksa, True)
        s.enable_yahoo_fundamentals_global = _to_bool(s.enable_yahoo_fundamentals_global, False)

        # Normalize ints
        s.max_requests_per_minute = _positive_int(s.max_requests_per_minute, 240)
        s.max_retries = _positive_int(s.max_retries, 2)
        s.cache_max_size = _positive_int(s.cache_max_size, 5000)
        s.cache_save_interval = _positive_int(s.cache_save_interval, 300)
        s.enriched_batch_concurrency = _positive_int(s.enriched_batch_concurrency, 8)

        # Normalize floats
        s.http_timeout = _positive_float(s.http_timeout, 30.0)
        s.retry_delay = _positive_float(s.retry_delay, 0.5)
        s.cache_ttl_sec = _positive_float(s.cache_ttl_sec, 20.0)
        s.quote_ttl_sec = _positive_float(s.quote_ttl_sec, 30.0)
        s.fundamentals_ttl_sec = _positive_float(s.fundamentals_ttl_sec, 21600.0)
        s.argaam_snapshot_ttl_sec = _positive_float(s.argaam_snapshot_ttl_sec, 30.0)
        s.engine_cache_ttl_sec = _positive_float(s.engine_cache_ttl_sec, float(s.cache_ttl_sec or 20.0))

        # Normalize version (env/commit can win)
        s.service_version = _resolve_version_from_env_or_commit(s.service_version)

        # ✅ Enforce require_auth when token exists
        tok_set = bool((s.app_token or "").strip() or (s.backup_app_token or "").strip())
        s.require_auth = _to_bool(s.require_auth, False) or tok_set

        _apply_runtime_env_aliases(s)
        return s

except Exception:  # pragma: no cover
    # =============================================================================
    # LAST-RESORT FALLBACK (NO pydantic-settings required) — FULL
    # =============================================================================

    class Settings:  # type: ignore
        """
        Minimal env-backed settings with the SAME attribute names expected across the codebase.
        This must never throw during import.
        """

        def __init__(self) -> None:
            # App / Meta
            self.service_name = (os.getenv("SERVICE_NAME") or os.getenv("APP_NAME") or "Tadawul Stock Analysis API").strip()
            self.service_version = (
                os.getenv("SERVICE_VERSION")
                or os.getenv("APP_VERSION")
                or os.getenv("VERSION")
                or os.getenv("RELEASE")
                or "dev"
            ).strip()
            self.environment = (os.getenv("ENVIRONMENT") or os.getenv("APP_ENV") or os.getenv("ENV") or "production").strip()
            self.tz = (os.getenv("TZ") or os.getenv("TIMEZONE") or "Asia/Riyadh").strip()
            self.debug = _to_bool(os.getenv("DEBUG"), False)
            self.log_level = (os.getenv("LOG_LEVEL") or "info").strip().lower()
            self.log_format = (os.getenv("LOG_FORMAT") or "%(asctime)s | %(levelname)s | %(name)s | %(message)s").strip()

            # Boot / engine flags
            self.defer_router_mount = _to_bool(os.getenv("DEFER_ROUTER_MOUNT"), True)
            self.init_engine_on_boot = _to_bool(os.getenv("INIT_ENGINE_ON_BOOT"), True)

            # Auth
            self.app_token = (os.getenv("APP_TOKEN") or os.getenv("TFB_APP_TOKEN") or "").strip() or None
            self.backup_app_token = (os.getenv("BACKUP_APP_TOKEN") or "").strip() or None
            self.require_auth = _to_bool(os.getenv("REQUIRE_AUTH"), False) or bool(self.app_token or self.backup_app_token)

            # Providers (GLOBAL + KSA)
            self.enabled_providers_raw = (os.getenv("ENABLED_PROVIDERS") or os.getenv("PROVIDERS") or "eodhd,finnhub,fmp").strip()
            self.primary_provider = (os.getenv("PRIMARY_PROVIDER") or "eodhd").strip().lower()
            self.ksa_providers_raw = (os.getenv("KSA_PROVIDERS") or "yahoo_chart,tadawul,argaam").strip()

            # Provider keys/tokens
            self.eodhd_api_key = (os.getenv("EODHD_API_KEY") or os.getenv("EODHD_API_TOKEN") or os.getenv("EODHD_TOKEN") or "").strip() or None
            self.finnhub_api_key = (os.getenv("FINNHUB_API_KEY") or os.getenv("FINNHUB_API_TOKEN") or os.getenv("FINNHUB_TOKEN") or "").strip() or None

            self.fmp_api_key = (os.getenv("FMP_API_KEY") or "").strip() or None
            self.alpha_vantage_api_key = (os.getenv("ALPHA_VANTAGE_API_KEY") or "").strip() or None
            self.twelvedata_api_key = (os.getenv("TWELVEDATA_API_KEY") or "").strip() or None
            self.marketstack_api_key = (os.getenv("MARKETSTACK_API_KEY") or "").strip() or None
            self.argaam_api_key = (os.getenv("ARGAAM_API_KEY") or "").strip() or None

            # Base URLs / Networking
            self.backend_base_url = (os.getenv("BACKEND_BASE_URL") or os.getenv("TFB_BASE_URL") or os.getenv("BASE_URL") or "").strip() or None
            self.eodhd_base_url = (os.getenv("EODHD_BASE_URL") or "https://eodhistoricaldata.com/api").strip()
            self.finnhub_base_url = (os.getenv("FINNHUB_BASE_URL") or "https://finnhub.io/api/v1").strip()
            self.fmp_base_url = (os.getenv("FMP_BASE_URL") or "").strip() or None
            self.yahoo_chart_base_url = (os.getenv("YAHOO_CHART_BASE_URL") or "https://query1.finance.yahoo.com").strip()

            self.http_timeout = _positive_float(os.getenv("HTTP_TIMEOUT_SEC") or os.getenv("HTTP_TIMEOUT"), 30.0)
            self.max_retries = _positive_int(os.getenv("MAX_RETRIES"), 2)
            self.retry_delay = _positive_float(os.getenv("RETRY_DELAY") or os.getenv("RETRY_DELAY_SEC"), 0.5)

            # CORS / Rate limiting
            self.enable_cors_all_origins = _to_bool(os.getenv("ENABLE_CORS_ALL_ORIGINS") or os.getenv("CORS_ALL_ORIGINS"), True)
            self.cors_origins = (os.getenv("CORS_ORIGINS") or "*").strip()
            self.enable_rate_limiting = _to_bool(os.getenv("ENABLE_RATE_LIMITING"), True)
            self.max_requests_per_minute = _positive_int(os.getenv("MAX_REQUESTS_PER_MINUTE"), 240)

            # Cache tuning
            self.cache_ttl_sec = _positive_float(os.getenv("CACHE_TTL_SEC") or os.getenv("CACHE_DEFAULT_TTL"), 20.0)
            self.quote_ttl_sec = _positive_float(os.getenv("QUOTE_TTL_SEC"), 30.0)
            self.fundamentals_ttl_sec = _positive_float(os.getenv("FUNDAMENTALS_TTL_SEC"), 21600.0)
            self.argaam_snapshot_ttl_sec = _positive_float(os.getenv("ARGAAM_SNAPSHOT_TTL_SEC"), 30.0)

            self.engine_cache_ttl_sec = _positive_float(
                os.getenv("ENGINE_CACHE_TTL_SEC") or os.getenv("ENGINE_TTL_SEC"),
                float(self.cache_ttl_sec or 20.0),
            )
            self.enriched_batch_concurrency = _positive_int(
                os.getenv("ENRICHED_BATCH_CONCURRENCY") or os.getenv("ENRICHED_CONCURRENCY"),
                8,
            )

            self.cache_max_size = _positive_int(os.getenv("CACHE_MAX_SIZE"), 5000)
            self.cache_backup_enabled = _to_bool(os.getenv("CACHE_BACKUP_ENABLED"), False)
            self.cache_save_interval = _positive_int(os.getenv("CACHE_SAVE_INTERVAL"), 300)

            # Google / Sheets
            self.default_spreadsheet_id = (
                os.getenv("DEFAULT_SPREADSHEET_ID")
                or os.getenv("SPREADSHEET_ID")
                or os.getenv("GOOGLE_SHEETS_ID")
                or ""
            ).strip() or None
            self.google_sheets_credentials = (
                os.getenv("GOOGLE_SHEETS_CREDENTIALS")
                or os.getenv("GOOGLE_CREDENTIALS")
                or os.getenv("GOOGLE_SA_JSON")
                or ""
            ).strip() or None
            self.google_apps_script_url = (os.getenv("GOOGLE_APPS_SCRIPT_URL") or "").strip() or None
            self.google_apps_script_backup_url = (os.getenv("GOOGLE_APPS_SCRIPT_BACKUP_URL") or "").strip() or None

            # Feature flags (general)
            self.ai_analysis_enabled = _to_bool(os.getenv("AI_ANALYSIS_ENABLED"), True)
            self.advanced_analysis_enabled = _to_bool(os.getenv("ADVANCED_ANALYSIS_ENABLED"), True)
            self.tadawul_market_enabled = _to_bool(os.getenv("TADAWUL_MARKET_ENABLED"), True)
            self.enable_swagger = _to_bool(os.getenv("ENABLE_SWAGGER"), True)
            self.enable_redoc = _to_bool(os.getenv("ENABLE_REDOC"), True)

            # Feature flags (engine/provider routing)
            self.enable_yahoo_chart_ksa = _to_bool(os.getenv("ENABLE_YAHOO_CHART_KSA"), True)
            self.enable_yahoo_chart_supplement = _to_bool(os.getenv("ENABLE_YAHOO_CHART_SUPPLEMENT"), True)
            self.enable_yfinance_ksa = _to_bool(os.getenv("ENABLE_YFINANCE_KSA"), False)
            self.enable_yahoo_fundamentals_ksa = _to_bool(os.getenv("ENABLE_YAHOO_FUNDAMENTALS_KSA"), True)
            self.enable_yahoo_fundamentals_global = _to_bool(os.getenv("ENABLE_YAHOO_FUNDAMENTALS_GLOBAL"), False)

            # Optional KSA routing URLs
            self.tadawul_quote_url = (os.getenv("TADAWUL_QUOTE_URL") or "").strip() or None
            self.tadawul_fundamentals_url = (os.getenv("TADAWUL_FUNDAMENTALS_URL") or "").strip() or None
            self.argaam_quote_url = (os.getenv("ARGAAM_QUOTE_URL") or "").strip() or None
            self.argaam_profile_url = (os.getenv("ARGAAM_PROFILE_URL") or "").strip() or None

            # Final normalize version + export runtime aliases
            self.service_version = _resolve_version_from_env_or_commit(self.service_version)
            _apply_runtime_env_aliases_fallback(self)

        # ---- Derived helpers ----
        @property
        def enabled_providers(self) -> List[str]:
            xs = _parse_listlike(self.enabled_providers_raw, lower=True)
            return xs or ["eodhd", "finnhub", "fmp"]

        @property
        def ksa_providers(self) -> List[str]:
            xs = _parse_listlike(self.ksa_providers_raw, lower=True)
            return xs or ["yahoo_chart", "tadawul", "argaam"]

        @property
        def cors_origins_list(self) -> List[str]:
            if bool(self.enable_cors_all_origins):
                return ["*"]
            s = (self.cors_origins or "").strip()
            if not s or s == "*":
                return ["*"]
            return [x.strip() for x in s.split(",") if x.strip()]

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

        @property
        def google_credentials(self) -> Optional[str]:
            return self.google_sheets_credentials

        def __getattr__(self, name: str) -> Any:
            if name.isupper():
                mapping = {
                    "EODHD_API_TOKEN": "eodhd_api_token",
                    "FINNHUB_API_TOKEN": "finnhub_api_token",
                    "HTTP_TIMEOUT_SEC": "http_timeout_sec",
                    "APP_NAME": "app_name",
                    "APP_ENV": "env",
                    "APP_VERSION": "app_version",
                }
                if name in mapping:
                    return getattr(self, mapping[name])
                low = name.lower()
                if hasattr(self, low):
                    return getattr(self, low)
            raise AttributeError(name)

    def _apply_runtime_env_aliases_fallback(s: Settings) -> None:
        ver = _resolve_version_from_env_or_commit(s.service_version)
        s.service_version = ver

        _export_env_if_missing("SERVICE_VERSION", ver)
        _export_env_if_missing("APP_VERSION", ver)
        _export_env_if_missing("VERSION", ver)
        _export_env_if_missing("RELEASE", ver)

        _export_env_if_missing("APP_NAME", s.service_name)
        _export_env_if_missing("SERVICE_NAME", s.service_name)
        _export_env_if_missing("APP_ENV", s.environment)
        _export_env_if_missing("ENVIRONMENT", s.environment)
        _export_env_if_missing("ENV", s.environment)
        _export_env_if_missing("TZ", s.tz)
        _export_env_if_missing("TIMEZONE", s.tz)
        _export_env_if_missing("LOG_LEVEL", (s.log_level or "info").strip().lower())
        _export_env_if_missing("LOG_FORMAT", (s.log_format or "").strip())

        if (s.app_token or "").strip():
            _export_env_if_missing("APP_TOKEN", (s.app_token or "").strip())
            _export_env_if_missing("TFB_APP_TOKEN", (s.app_token or "").strip())
        if (s.backup_app_token or "").strip():
            _export_env_if_missing("BACKUP_APP_TOKEN", (s.backup_app_token or "").strip())

        _export_env_if_missing("FINNHUB_API_TOKEN", s.finnhub_api_token)
        _export_env_if_missing("FINNHUB_TOKEN", s.finnhub_api_token)
        _export_env_if_missing("EODHD_API_TOKEN", s.eodhd_api_token)
        _export_env_if_missing("EODHD_TOKEN", s.eodhd_api_token)

        _export_env_if_missing("ENABLED_PROVIDERS", (s.enabled_providers_raw or "").strip())
        _export_env_if_missing("PROVIDERS", (s.enabled_providers_raw or "").strip())
        _export_env_if_missing("KSA_PROVIDERS", (s.ksa_providers_raw or "").strip())
        _export_env_if_missing("PRIMARY_PROVIDER", (s.primary_provider or "eodhd").strip().lower())

        _export_env_if_missing("HTTP_TIMEOUT_SEC", str(float(s.http_timeout_sec)))
        _export_env_if_missing("HTTP_TIMEOUT", str(float(s.http_timeout_sec)))
        _export_env_if_missing("MAX_RETRIES", str(int(s.max_retries)))
        _export_env_if_missing("RETRY_DELAY", str(float(s.retry_delay)))
        _export_env_if_missing("RETRY_DELAY_SEC", str(float(s.retry_delay)))

        _export_env_if_missing("CACHE_TTL_SEC", str(float(s.cache_ttl_sec)))
        _export_env_if_missing("CACHE_DEFAULT_TTL", str(float(s.cache_ttl_sec)))
        _export_env_if_missing("ENGINE_CACHE_TTL_SEC", str(float(s.engine_cache_ttl_sec)))
        _export_env_if_missing("ENRICHED_BATCH_CONCURRENCY", str(int(s.enriched_batch_concurrency)))
        _export_env_if_missing("ENRICHED_CONCURRENCY", str(int(s.enriched_batch_concurrency)))

        _export_env_if_missing("ENABLE_YAHOO_CHART_KSA", str(bool(s.enable_yahoo_chart_ksa)).lower())
        _export_env_if_missing("ENABLE_YAHOO_CHART_SUPPLEMENT", str(bool(s.enable_yahoo_chart_supplement)).lower())
        _export_env_if_missing("ENABLE_YFINANCE_KSA", str(bool(s.enable_yfinance_ksa)).lower())
        _export_env_if_missing("ENABLE_YAHOO_FUNDAMENTALS_KSA", str(bool(s.enable_yahoo_fundamentals_ksa)).lower())
        _export_env_if_missing("ENABLE_YAHOO_FUNDAMENTALS_GLOBAL", str(bool(s.enable_yahoo_fundamentals_global)).lower())

        _export_env_if_missing("EODHD_BASE_URL", s.eodhd_base_url)
        _export_env_if_missing("FINNHUB_BASE_URL", s.finnhub_base_url)
        _export_env_if_missing("YAHOO_CHART_BASE_URL", s.yahoo_chart_base_url)

        if s.backend_base_url:
            base = (s.backend_base_url or "").rstrip("/")
            _export_env_if_missing("BACKEND_BASE_URL", base)
            _export_env_if_missing("TFB_BASE_URL", base)
            _export_env_if_missing("BASE_URL", base)

        if s.default_spreadsheet_id:
            _export_env_if_missing("DEFAULT_SPREADSHEET_ID", s.default_spreadsheet_id)
            _export_env_if_missing("SPREADSHEET_ID", s.default_spreadsheet_id)
            _export_env_if_missing("GOOGLE_SHEETS_ID", s.default_spreadsheet_id)

        if s.google_sheets_credentials:
            _export_env_if_missing("GOOGLE_SHEETS_CREDENTIALS", s.google_sheets_credentials)
            _export_env_if_missing("GOOGLE_CREDENTIALS", s.google_sheets_credentials)
            _export_env_if_missing("GOOGLE_SA_JSON", s.google_sheets_credentials)

        if s.google_apps_script_url:
            _export_env_if_missing("GOOGLE_APPS_SCRIPT_URL", s.google_apps_script_url)
        if s.google_apps_script_backup_url:
            _export_env_if_missing("GOOGLE_APPS_SCRIPT_BACKUP_URL", s.google_apps_script_backup_url)

        if s.tadawul_quote_url:
            _export_env_if_missing("TADAWUL_QUOTE_URL", s.tadawul_quote_url)
        if s.tadawul_fundamentals_url:
            _export_env_if_missing("TADAWUL_FUNDAMENTALS_URL", s.tadawul_fundamentals_url)
        if s.argaam_quote_url:
            _export_env_if_missing("ARGAAM_QUOTE_URL", s.argaam_quote_url)
        if s.argaam_profile_url:
            _export_env_if_missing("ARGAAM_PROFILE_URL", s.argaam_profile_url)

    @lru_cache(maxsize=1)
    def get_settings() -> Settings:  # type: ignore
        return Settings()


# -----------------------------------------------------------------------------
# Best-effort module alias so `from core.config import get_settings` works.
# -----------------------------------------------------------------------------
try:
    sys.modules.setdefault("core.config", sys.modules[__name__])
except Exception:
    pass


__all__ = ["Settings", "get_settings"]
