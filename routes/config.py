# config.py  (REPO ROOT) — FULL REPLACEMENT — v5.4.1
"""
config.py
============================================================
Canonical Settings for Tadawul Fast Bridge (ROOT)

✅ Single source of truth for env vars (Render names).
✅ Runtime alias export (so legacy/provider modules keep working).
✅ No network at import-time. Minimal side effects at import-time.
✅ Defensive: never crashes import-time even if pydantic-settings missing.

Version: v5.4.1
Upgrades vs v5.4.0
- ✅ Adds vNext “Sheet Schemas + Forecasting plan” env keys (non-breaking).
- ✅ Fixes fallback bug: cors_origins_list referenced but missing.
- ✅ Keeps auth effectively “on” when APP_TOKEN/BACKUP_APP_TOKEN set (require_auth derived).
- ✅ Keeps safe module alias for core.config imports (best-effort).
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
        return int(float(str(v).strip()))
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


def _parse_int_list(v: Any, default: List[int]) -> List[int]:
    """
    Accept CSV: "30,90,365" or JSON-ish "[30,90,365]" (best-effort).
    Returns deduped positive ints preserving order; falls back to default if empty.
    """
    if v is None:
        return list(default)

    s = str(v).strip()
    if not s:
        return list(default)

    # best-effort strip [ ]
    if s.startswith("[") and s.endswith("]"):
        s = s[1:-1].strip()

    parts = [p.strip() for p in s.split(",") if p.strip()]
    out: List[int] = []
    seen = set()
    for p in parts:
        n = _to_int(p, -1)
        if n <= 0 or n in seen:
            continue
        seen.add(n)
        out.append(n)
    return out or list(default)


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
        # ✅ vNext: Sheet Schemas + Forecast + Recommendation controls
        # ---------------------------------------------------------------------
        sheet_schemas_enabled: bool = Field(default=True, validation_alias=_alias("SHEET_SCHEMAS_ENABLED"))
        sheet_schema_version: str = Field(default="vNext", validation_alias=_alias("SHEET_SCHEMA_VERSION"))

        forecast_enabled: bool = Field(default=True, validation_alias=_alias("FORECAST_ENABLED"))
        forecast_horizons_days_raw: str = Field(default="30,90,365", validation_alias=_alias("FORECAST_HORIZONS_DAYS"))
        forecast_lookback_days: int = Field(default=365, validation_alias=_alias("FORECAST_LOOKBACK_DAYS"))
        forecast_min_points: int = Field(default=90, validation_alias=_alias("FORECAST_MIN_POINTS"))
        forecast_method: str = Field(default="ewma", validation_alias=_alias("FORECAST_METHOD"))
        forecast_cache_ttl: int = Field(default=180, validation_alias=_alias("FORECAST_CACHE_TTL"))

        recommendation_mode: str = Field(default="hybrid", validation_alias=_alias("RECOMMENDATION_MODE"))
        recommendation_primary_horizon_days: int = Field(default=90, validation_alias=_alias("RECOMMENDATION_PRIMARY_HORIZON_DAYS"))
        percent_change_format: str = Field(default="ratio", validation_alias=_alias("PERCENT_CHANGE_FORMAT"))

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

        @property
        def forecast_horizons_days(self) -> List[int]:
            return _parse_int_list(self.forecast_horizons_days_raw, default=[30, 90, 365])

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
                # vNext controls
                "sheet_schema": {
                    "enabled": bool(self.sheet_schemas_enabled),
                    "version": (self.sheet_schema_version or "vNext").strip(),
                },
                "forecast": {
                    "enabled": bool(self.forecast_enabled),
                    "horizons_days": self.forecast_horizons_days,
                    "lookback_days": int(self.forecast_lookback_days),
                    "min_points": int(self.forecast_min_points),
                    "method": (self.forecast_method or "ewma").strip().lower(),
                    "cache_ttl": int(self.forecast_cache_ttl),
                },
                "recommendation": {
                    "mode": (self.recommendation_mode or "hybrid").strip().lower(),
                    "primary_horizon_days": int(self.recommendation_primary_horizon_days),
                    "percent_change_format": (self.percent_change_format or "ratio").strip().lower(),
                },
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
                    "FORECAST_HORIZONS_DAYS": "forecast_horizons_days_raw",
                    "RECOMMENDATION_PRIMARY_HORIZON_DAYS": "recommendation_primary_horizon_days",
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
        try:
            s.log_level = (s.log_level or "info").strip().lower()
        except Exception:
            pass
        try:
            s.primary_provider = (s.primary_provider or "eodhd").strip().lower()
        except Exception:
            pass

        s.debug = _to_bool(getattr(s, "debug", False), False)

        s.enable_rate_limiting = _to_bool(getattr(s, "enable_rate_limiting", True), True)
        s.cache_backup_enabled = _to_bool(getattr(s, "cache_backup_enabled", False), False)
        s.advanced_analysis_enabled = _to_bool(getattr(s, "advanced_analysis_enabled", True), True)
        s.tadawul_market_enabled = _to_bool(getattr(s, "tadawul_market_enabled", True), True)
        s.enable_cors_all_origins = _to_bool(getattr(s, "enable_cors_all_origins", True), True)

        s.enable_yfinance_ksa = _to_bool(getattr(s, "enable_yfinance_ksa", False), False)
        s.enable_yahoo_chart_ksa = _to_bool(getattr(s, "enable_yahoo_chart_ksa", True), True)

        # vNext flags
        s.sheet_schemas_enabled = _to_bool(getattr(s, "sheet_schemas_enabled", True), True)
        s.forecast_enabled = _to_bool(getattr(s, "forecast_enabled", True), True)

        s.max_requests_per_minute = _to_int(getattr(s, "max_requests_per_minute", 240), 240)
        s.max_retries = _to_int(getattr(s, "max_retries", 2), 2)

        s.http_timeout = _to_float(getattr(s, "http_timeout", 30.0), 30.0)
        s.retry_delay = _to_float(getattr(s, "retry_delay", 0.5), 0.5)

        s.cache_ttl_sec = _to_float(getattr(s, "cache_ttl_sec", 20.0), 20.0)
        s.quote_ttl_sec = _to_float(getattr(s, "quote_ttl_sec", 30.0), 30.0)
        s.fundamentals_ttl_sec = _to_float(getattr(s, "fundamentals_ttl_sec", 21600.0), 21600.0)
        s.argaam_snapshot_ttl_sec = _to_float(getattr(s, "argaam_snapshot_ttl_sec", 30.0), 30.0)

        s.cache_max_size = _to_int(getattr(s, "cache_max_size", 5000), 5000)
        s.cache_save_interval = _to_int(getattr(s, "cache_save_interval", 300), 300)

        # Advanced Analysis clamps (same intent as routes)
        s.adv_batch_size = max(5, min(200, _to_int(getattr(s, "adv_batch_size", 25), 25)))
        s.adv_batch_timeout_sec = max(5.0, min(180.0, _to_float(getattr(s, "adv_batch_timeout_sec", 45.0), 45.0)))
        s.adv_max_tickers = max(10, min(2000, _to_int(getattr(s, "adv_max_tickers", 500), 500)))
        s.adv_batch_concurrency = max(1, min(25, _to_int(getattr(s, "adv_batch_concurrency", 6), 6)))

        # vNext clamps
        s.forecast_lookback_days = max(30, min(3650, _to_int(getattr(s, "forecast_lookback_days", 365), 365)))
        s.forecast_min_points = max(10, min(2000, _to_int(getattr(s, "forecast_min_points", 90), 90)))
        s.forecast_cache_ttl = max(10, min(86400, _to_int(getattr(s, "forecast_cache_ttl", 180), 180)))
        s.recommendation_primary_horizon_days = max(7, min(3650, _to_int(getattr(s, "recommendation_primary_horizon_days", 90), 90)))

        # Auth: if token is set, effectively require auth
        tok_set = bool((getattr(s, "app_token", "") or "").strip() or (getattr(s, "backup_app_token", "") or "").strip())
        s.require_auth = _to_bool(getattr(s, "require_auth", False), False) or tok_set

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
        twelvedata_api_key: Optional[str] = None
        marketstack_api_key: Optional[str] = None
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

        # ✅ vNext controls
        sheet_schemas_enabled: bool = True
        sheet_schema_version: str = "vNext"

        forecast_enabled: bool = True
        forecast_horizons_days_raw: str = "30,90,365"
        forecast_lookback_days: int = 365
        forecast_min_points: int = 90
        forecast_method: str = "ewma"
        forecast_cache_ttl: int = 180

        recommendation_mode: str = "hybrid"
        recommendation_primary_horizon_days: int = 90
        percent_change_format: str = "ratio"

        @property
        def enabled_providers(self) -> List[str]:
            return _csv(self.enabled_providers_raw, lower=True) or ["eodhd", "finnhub"]

        @property
        def ksa_providers(self) -> List[str]:
            return _csv(self.ksa_providers_raw, lower=True) or ["yahoo_chart", "tadawul", "argaam"]

        # ✅ FIX: was referenced but missing in your v5.4.0 fallback
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
            return float(getattr(self, "http_timeout", 30.0) or 30.0)

        @property
        def forecast_horizons_days(self) -> List[int]:
            return _parse_int_list(self.forecast_horizons_days_raw, default=[30, 90, 365])

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
                "sheet_schema": {"enabled": bool(self.sheet_schemas_enabled), "version": (self.sheet_schema_version or "vNext").strip()},
                "forecast": {
                    "enabled": bool(self.forecast_enabled),
                    "horizons_days": self.forecast_horizons_days,
                    "lookback_days": int(self.forecast_lookback_days),
                    "min_points": int(self.forecast_min_points),
                    "method": (self.forecast_method or "ewma").strip().lower(),
                    "cache_ttl": int(self.forecast_cache_ttl),
                },
                "recommendation": {
                    "mode": (self.recommendation_mode or "hybrid").strip().lower(),
                    "primary_horizon_days": int(self.recommendation_primary_horizon_days),
                    "percent_change_format": (self.percent_change_format or "ratio").strip().lower(),
                },
            }

        def __getattr__(self, name: str) -> Any:
            if name.isupper():
                mapping = {
                    "EODHD_API_TOKEN": "eodhd_api_token",
                    "FINNHUB_API_TOKEN": "finnhub_api_token",
                    "HTTP_TIMEOUT_SEC": "http_timeout_sec",
                    "ADV_BATCH_TIMEOUT_SEC": "adv_batch_timeout_sec",
                    "ADV_BATCH_SIZE": "adv_batch_size",
                    "ADV_MAX_TICKERS": "adv_max_tickers",
                    "ADV_BATCH_CONCURRENCY": "adv_batch_concurrency",
                    "FORECAST_HORIZONS_DAYS": "forecast_horizons_days_raw",
                    "RECOMMENDATION_PRIMARY_HORIZON_DAYS": "recommendation_primary_horizon_days",
                }
                if name in mapping and hasattr(self, mapping[name]):
                    return getattr(self, mapping[name])
            raise AttributeError(name)

    def _apply_runtime_env_aliases(s: Settings) -> None:
        _export_env_if_missing("FINNHUB_API_TOKEN", s.finnhub_api_token)
        _export_env_if_missing("FINNHUB_TOKEN", s.finnhub_api_token)
        _export_env_if_missing("EODHD_API_TOKEN", s.eodhd_api_token)
        _export_env_if_missing("EODHD_TOKEN", s.eodhd_api_token)
        _export_env_if_missing("HTTP_TIMEOUT_SEC", str(float(s.http_timeout_sec)))
        _export_env_if_missing("HTTP_TIMEOUT", str(float(s.http_timeout_sec)))
        _export_env_if_missing("CACHE_TTL_SEC", str(float(s.cache_ttl_sec)))
        _export_env_if_missing("CACHE_DEFAULT_TTL", str(float(s.cache_ttl_sec)))
        _export_env_if_missing("QUOTE_TTL_SEC", str(float(s.quote_ttl_sec)))
        _export_env_if_missing("FUNDAMENTALS_TTL_SEC", str(float(s.fundamentals_ttl_sec)))
        _export_env_if_missing("ARGAAM_SNAPSHOT_TTL_SEC", str(float(s.argaam_snapshot_ttl_sec)))
        _export_env_if_missing("ADV_BATCH_SIZE", str(int(s.adv_batch_size)))
        _export_env_if_missing("ADV_BATCH_TIMEOUT_SEC", str(float(s.adv_batch_timeout_sec)))
        _export_env_if_missing("ADV_MAX_TICKERS", str(int(s.adv_max_tickers)))
        _export_env_if_missing("ADV_BATCH_CONCURRENCY", str(int(s.adv_batch_concurrency)))
        _export_env_if_missing("EODHD_BASE_URL", s.eodhd_base_url)
        _export_env_if_missing("FINNHUB_BASE_URL", s.finnhub_base_url)
        _export_env_if_missing("YAHOO_CHART_BASE_URL", s.yahoo_chart_base_url)
        _export_env_if_missing("ENABLED_PROVIDERS", ",".join(s.enabled_providers))
        _export_env_if_missing("KSA_PROVIDERS", ",".join(s.ksa_providers))
        _export_env_if_missing("PRIMARY_PROVIDER", (s.primary_provider or "eodhd").strip().lower())
        if s.backend_base_url:
            _export_env_if_missing("BACKEND_BASE_URL", (s.backend_base_url or "").rstrip("/"))
            _export_env_if_missing("BASE_URL", (s.backend_base_url or "").rstrip("/"))

    @lru_cache(maxsize=1)
    def get_settings() -> Settings:
        # Build from env manually (since no pydantic-settings)
        s = Settings()

        # Load key envs best-effort
        s.service_name = os.getenv("SERVICE_NAME") or os.getenv("APP_NAME") or s.service_name
        s.service_version = os.getenv("SERVICE_VERSION") or os.getenv("APP_VERSION") or os.getenv("VERSION") or s.service_version
        s.environment = os.getenv("ENVIRONMENT") or os.getenv("APP_ENV") or os.getenv("ENV") or s.environment
        s.tz = os.getenv("TZ") or os.getenv("TIMEZONE") or s.tz

        s.debug = _to_bool(os.getenv("DEBUG", s.debug), False)
        s.log_level = (os.getenv("LOG_LEVEL") or s.log_level).strip().lower()

        s.app_token = os.getenv("APP_TOKEN") or s.app_token
        s.backup_app_token = os.getenv("BACKUP_APP_TOKEN") or s.backup_app_token
        tok_set = bool((s.app_token or "").strip() or (s.backup_app_token or "").strip())
        s.require_auth = _to_bool(os.getenv("REQUIRE_AUTH", s.require_auth), False) or tok_set

        s.enabled_providers_raw = os.getenv("ENABLED_PROVIDERS") or os.getenv("PROVIDERS") or s.enabled_providers_raw
        s.primary_provider = (os.getenv("PRIMARY_PROVIDER") or s.primary_provider).strip().lower()
        s.ksa_providers_raw = os.getenv("KSA_PROVIDERS") or s.ksa_providers_raw

        s.enable_yfinance_ksa = _to_bool(os.getenv("ENABLE_YFINANCE_KSA", s.enable_yfinance_ksa), False)
        s.enable_yahoo_chart_ksa = _to_bool(os.getenv("ENABLE_YAHOO_CHART_KSA", s.enable_yahoo_chart_ksa), True)

        s.eodhd_api_key = os.getenv("EODHD_API_KEY") or os.getenv("EODHD_API_TOKEN") or os.getenv("EODHD_TOKEN") or s.eodhd_api_key
        s.finnhub_api_key = os.getenv("FINNHUB_API_KEY") or os.getenv("FINNHUB_API_TOKEN") or os.getenv("FINNHUB_TOKEN") or s.finnhub_api_key

        s.http_timeout = _to_float(os.getenv("HTTP_TIMEOUT_SEC") or os.getenv("HTTP_TIMEOUT") or s.http_timeout, 30.0)
        s.max_retries = _to_int(os.getenv("MAX_RETRIES") or s.max_retries, 2)
        s.retry_delay = _to_float(os.getenv("RETRY_DELAY") or os.getenv("RETRY_DELAY_SEC") or s.retry_delay, 0.5)

        s.cache_ttl_sec = _to_float(os.getenv("CACHE_TTL_SEC") or os.getenv("CACHE_DEFAULT_TTL") or s.cache_ttl_sec, 20.0)
        s.quote_ttl_sec = _to_float(os.getenv("QUOTE_TTL_SEC") or s.quote_ttl_sec, 30.0)
        s.fundamentals_ttl_sec = _to_float(os.getenv("FUNDAMENTALS_TTL_SEC") or s.fundamentals_ttl_sec, 21600.0)
        s.argaam_snapshot_ttl_sec = _to_float(os.getenv("ARGAAM_SNAPSHOT_TTL_SEC") or s.argaam_snapshot_ttl_sec, 30.0)

        s.cache_max_size = _to_int(os.getenv("CACHE_MAX_SIZE") or s.cache_max_size, 5000)
        s.cache_backup_enabled = _to_bool(os.getenv("CACHE_BACKUP_ENABLED", s.cache_backup_enabled), False)
        s.cache_save_interval = _to_int(os.getenv("CACHE_SAVE_INTERVAL") or s.cache_save_interval, 300)

        s.enable_cors_all_origins = _to_bool(os.getenv("ENABLE_CORS_ALL_ORIGINS") or os.getenv("CORS_ALL_ORIGINS") or s.enable_cors_all_origins, True)
        s.cors_origins = os.getenv("CORS_ORIGINS") or s.cors_origins
        s.enable_rate_limiting = _to_bool(os.getenv("ENABLE_RATE_LIMITING", s.enable_rate_limiting), True)
        s.max_requests_per_minute = _to_int(os.getenv("MAX_REQUESTS_PER_MINUTE") or s.max_requests_per_minute, 240)

        # vNext
        s.sheet_schemas_enabled = _to_bool(os.getenv("SHEET_SCHEMAS_ENABLED", s.sheet_schemas_enabled), True)
        s.sheet_schema_version = (os.getenv("SHEET_SCHEMA_VERSION") or s.sheet_schema_version).strip() or "vNext"

        s.forecast_enabled = _to_bool(os.getenv("FORECAST_ENABLED", s.forecast_enabled), True)
        s.forecast_horizons_days_raw = os.getenv("FORECAST_HORIZONS_DAYS") or s.forecast_horizons_days_raw
        s.forecast_lookback_days = max(30, min(3650, _to_int(os.getenv("FORECAST_LOOKBACK_DAYS") or s.forecast_lookback_days, 365)))
        s.forecast_min_points = max(10, min(2000, _to_int(os.getenv("FORECAST_MIN_POINTS") or s.forecast_min_points, 90)))
        s.forecast_method = (os.getenv("FORECAST_METHOD") or s.forecast_method).strip().lower() or "ewma"
        s.forecast_cache_ttl = max(10, min(86400, _to_int(os.getenv("FORECAST_CACHE_TTL") or s.forecast_cache_ttl, 180)))

        s.recommendation_mode = (os.getenv("RECOMMENDATION_MODE") or s.recommendation_mode).strip().lower() or "hybrid"
        s.recommendation_primary_horizon_days = max(7, min(3650, _to_int(os.getenv("RECOMMENDATION_PRIMARY_HORIZON_DAYS") or s.recommendation_primary_horizon_days, 90)))
        s.percent_change_format = (os.getenv("PERCENT_CHANGE_FORMAT") or s.percent_change_format).strip().lower() or "ratio"

        _apply_runtime_env_aliases(s)
        return s


# -------------------------------------------------------------------------
# Best-effort module alias so `from core.config import get_settings` can work
# when this file is used as the canonical implementation.
# -------------------------------------------------------------------------
try:
    sys.modules.setdefault("core.config", sys.modules[__name__])
except Exception:
    pass


__all__ = ["Settings", "get_settings"]
