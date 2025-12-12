```python
"""
env.py
------------------------------------------------------------
Centralized environment configuration for Tadawul Fast Bridge.

Goals
- Zero-crash config loading (Render/.env/local).
- Strong defaults + safe parsing for bool/int/list/JSON.
- Keeps backwards-compatible exports used across the codebase:
    • settings: Settings
    • Convenience constants: APP_ENV, APP_TOKEN, ENABLED_PROVIDERS, etc.

Notes
- EODHD is **GLOBAL ONLY** (never for KSA .SR) — enforced in the engine/routes.
- GOOGLE_SHEETS_CREDENTIALS is expected as full JSON string (usually one-line).
"""

from __future__ import annotations

import json
import logging
import os
from typing import Any, Dict, List, Optional

from pydantic import BaseModel, Field

# ----------------------------------------------------------------------
# Optional: load from .env during local development
# ----------------------------------------------------------------------
try:  # pragma: no cover
    from dotenv import load_dotenv  # type: ignore

    load_dotenv()
except Exception:
    pass

logger = logging.getLogger(__name__)


# ----------------------------------------------------------------------
# Helpers (safe parsing)
# ----------------------------------------------------------------------
_TRUTHY = {"1", "true", "yes", "on", "y"}
_FALSY = {"0", "false", "no", "off", "n"}


def _get_str(key: str, default: str = "") -> str:
    v = os.getenv(key)
    if v is None:
        return default
    return str(v).strip()


def _get_int(key: str, default: int) -> int:
    raw = os.getenv(key)
    if raw is None:
        return default
    try:
        return int(str(raw).strip())
    except Exception:
        logger.warning("[env] Invalid int for %s=%r, using default=%s", key, raw, default)
        return default


def _get_bool(key: str, default: bool) -> bool:
    raw = os.getenv(key)
    if raw is None:
        return default
    s = str(raw).strip().lower()
    if s in _TRUTHY:
        return True
    if s in _FALSY:
        return False
    logger.warning("[env] Invalid bool for %s=%r, using default=%s", key, raw, default)
    return default


def _split_list(value: Optional[str]) -> List[str]:
    if not value:
        return []
    return [v.strip() for v in str(value).split(",") if v.strip()]


def _split_list_lower(value: Optional[str]) -> List[str]:
    return [v.lower() for v in _split_list(value)]


def _masked(value: Optional[str]) -> str:
    if not value:
        return "<empty>"
    s = str(value)
    n = len(s)
    if n <= 4:
        return "*" * n
    return f"{s[:2]}***{s[-2:]} (len={n})"


def _load_json(value: Optional[str]) -> Optional[Dict[str, Any]]:
    """
    Parse JSON from an env string; returns None if missing/invalid.
    Handles common cases:
      - raw JSON object string
      - JSON wrapped in quotes
      - accidental whitespace
    """
    if not value:
        return None
    raw = str(value).strip()

    # If it looks like it's wrapped in quotes, unwrap once
    if (raw.startswith('"') and raw.endswith('"')) or (raw.startswith("'") and raw.endswith("'")):
        raw = raw[1:-1].strip()

    # Basic sanity: must look like JSON object
    if not raw.startswith("{"):
        return None

    try:
        return json.loads(raw)
    except Exception as exc:
        logger.warning("[env] Failed to parse JSON env value (len=%s): %s", len(raw), exc)
        return None


# ----------------------------------------------------------------------
# Settings Model (single source of truth)
# ----------------------------------------------------------------------
class Settings(BaseModel):
    # --------------------------------------------------------------
    # Application meta
    # --------------------------------------------------------------
    app_name: str = Field(default_factory=lambda: _get_str("APP_NAME", "Tadawul Fast Bridge"))
    app_env: str = Field(default_factory=lambda: _get_str("APP_ENV", "production"))
    app_version: str = Field(default_factory=lambda: _get_str("APP_VERSION", "4.5.1"))

    # --------------------------------------------------------------
    # Base URLs & tokens
    # --------------------------------------------------------------
    backend_base_url: str = Field(
        default_factory=lambda: _get_str("BACKEND_BASE_URL", "https://tadawul-fast-bridge.onrender.com")
    )

    app_token: Optional[str] = Field(default_factory=lambda: os.getenv("APP_TOKEN"))
    backup_app_token: Optional[str] = Field(default_factory=lambda: os.getenv("BACKUP_APP_TOKEN"))

    # If set, overrides auto-derivation in main.py
    has_secure_token_env: Optional[bool] = Field(
        default_factory=lambda: None if os.getenv("HAS_SECURE_TOKEN") is None else _get_bool("HAS_SECURE_TOKEN", True)
    )

    # --------------------------------------------------------------
    # Providers configuration (for unified data engine)
    # --------------------------------------------------------------
    enabled_providers: List[str] = Field(
        default_factory=lambda: _split_list_lower(_get_str("ENABLED_PROVIDERS", "eodhd,fmp,yfinance")),
        description="Enabled providers list: eodhd,fmp,yfinance,finnhub,...",
    )

    primary_provider: str = Field(default_factory=lambda: _get_str("PRIMARY_PROVIDER", "eodhd").lower())
    http_timeout: int = Field(default_factory=lambda: _get_int("HTTP_TIMEOUT", 25))
    max_retries: int = Field(default_factory=lambda: _get_int("MAX_RETRIES", 2))

    # Engine knobs (align with render.yaml)
    engine_cache_ttl_seconds: int = Field(default_factory=lambda: _get_int("ENGINE_CACHE_TTL_SECONDS", 60))
    engine_provider_timeout_seconds: int = Field(default_factory=lambda: _get_int("ENGINE_PROVIDER_TIMEOUT_SECONDS", 20))
    engine_enable_advanced_analysis: bool = Field(default_factory=lambda: _get_bool("ENGINE_ENABLE_ADVANCED_ANALYSIS", True))

    # KSA engine cache knob (align with render.yaml)
    ksa_engine_cache_ttl_seconds: int = Field(default_factory=lambda: _get_int("KSA_ENGINE_CACHE_TTL_SECONDS", 60))

    # --------------------------------------------------------------
    # EODHD provider (GLOBAL ONLY – NOT USED FOR KSA .SR)
    # --------------------------------------------------------------
    eodhd_api_key: Optional[str] = Field(default_factory=lambda: os.getenv("EODHD_API_KEY"))
    eodhd_base_url: str = Field(default_factory=lambda: _get_str("EODHD_BASE_URL", "https://eodhd.com/api"))

    # --------------------------------------------------------------
    # FinancialModelingPrep (FMP)
    # --------------------------------------------------------------
    fmp_api_key: Optional[str] = Field(default_factory=lambda: os.getenv("FMP_API_KEY"))
    fmp_base_url: str = Field(default_factory=lambda: _get_str("FMP_BASE_URL", "https://financialmodelingprep.com/api/v3"))

    # --------------------------------------------------------------
    # Optional providers
    # --------------------------------------------------------------
    finnhub_api_key: Optional[str] = Field(default_factory=lambda: os.getenv("FINNHUB_API_KEY"))
    alpha_vantage_api_key: Optional[str] = Field(default_factory=lambda: os.getenv("ALPHA_VANTAGE_API_KEY"))

    # --------------------------------------------------------------
    # KSA / Argaam provider (used by routes_argaam + legacy delegate)
    # --------------------------------------------------------------
    argaam_gateway_url: Optional[str] = Field(default_factory=lambda: os.getenv("ARGAAM_GATEWAY_URL"))
    argaam_api_key: Optional[str] = Field(default_factory=lambda: os.getenv("ARGAAM_API_KEY"))

    # --------------------------------------------------------------
    # Google Sheets / Apps Script integration
    # --------------------------------------------------------------
    google_sheets_credentials_raw: Optional[str] = Field(default_factory=lambda: os.getenv("GOOGLE_SHEETS_CREDENTIALS"))
    google_apps_script_backup_url: Optional[str] = Field(default_factory=lambda: os.getenv("GOOGLE_APPS_SCRIPT_BACKUP_URL"))
    google_project_id: Optional[str] = Field(default_factory=lambda: os.getenv("GOOGLE_PROJECT_ID"))
    default_spreadsheet_id: Optional[str] = Field(default_factory=lambda: os.getenv("DEFAULT_SPREADSHEET_ID"))

    # --------------------------------------------------------------
    # Dashboard – Sheet Name configuration
    # --------------------------------------------------------------
    ksa_sheet_name: str = Field(default_factory=lambda: _get_str("SHEET_KSA_TADAWUL", "KSA_Tadawul"))
    global_sheet_name: str = Field(default_factory=lambda: _get_str("SHEET_GLOBAL_MARKETS", "Global_Markets"))
    mutual_funds_sheet_name: str = Field(default_factory=lambda: _get_str("SHEET_MUTUAL_FUNDS", "Mutual_Funds"))
    commodities_fx_sheet_name: str = Field(default_factory=lambda: _get_str("SHEET_COMMODITIES_FX", "Commodities_FX"))
    market_leaders_sheet_name: str = Field(default_factory=lambda: _get_str("SHEET_MARKET_LEADERS", "Market_Leaders"))
    portfolio_sheet_name: str = Field(default_factory=lambda: _get_str("SHEET_MY_PORTFOLIO", "My_Portfolio"))
    insights_sheet_name: str = Field(default_factory=lambda: _get_str("SHEET_INSIGHTS_ANALYSIS", "Insights_Analysis"))
    advisor_sheet_name: str = Field(default_factory=lambda: _get_str("SHEET_INVESTMENT_ADVISOR", "Investment_Advisor"))
    economic_calendar_sheet_name: str = Field(default_factory=lambda: _get_str("SHEET_ECONOMIC_CALENDAR", "Economic_Calendar"))
    income_statement_sheet_name: str = Field(default_factory=lambda: _get_str("SHEET_INVESTMENT_INCOME", "Investment_Income_Statement"))

    # --------------------------------------------------------------
    # Misc flags
    # --------------------------------------------------------------
    enable_cors_all_origins: bool = Field(default_factory=lambda: _get_bool("ENABLE_CORS_ALL_ORIGINS", True))
    log_level: str = Field(default_factory=lambda: _get_str("LOG_LEVEL", "INFO").upper())

    # --------------------------------------------------------------
    # Derived / helper properties
    # --------------------------------------------------------------
    @property
    def google_sheets_credentials(self) -> Optional[Dict[str, Any]]:
        return _load_json(self.google_sheets_credentials_raw)

    @property
    def has_secure_token(self) -> bool:
        # If explicitly set by env, respect it; otherwise derive from token presence
        if self.has_secure_token_env is not None:
            return bool(self.has_secure_token_env)
        return bool(self.app_token or self.backup_app_token)

    @property
    def is_production(self) -> bool:
        return self.app_env.strip().lower() in {"prod", "production"}

    @property
    def primary_or_default_provider(self) -> str:
        enabled = [p for p in self.enabled_providers if p]
        primary = (self.primary_provider or "").lower()
        if primary and primary in enabled:
            return primary
        return (enabled or ["eodhd"])[0]

    @property
    def dashboard_sheet_names(self) -> Dict[str, str]:
        return {
            "ksa": self.ksa_sheet_name,
            "global": self.global_sheet_name,
            "mutual_funds": self.mutual_funds_sheet_name,
            "commodities_fx": self.commodities_fx_sheet_name,
            "market_leaders": self.market_leaders_sheet_name,
            "portfolio": self.portfolio_sheet_name,
            "insights": self.insights_sheet_name,
            "advisor": self.advisor_sheet_name,
            "economic_calendar": self.economic_calendar_sheet_name,
            "income_statement": self.income_statement_sheet_name,
        }

    @property
    def has_default_spreadsheet(self) -> bool:
        return bool(self.default_spreadsheet_id)

    # --------------------------------------------------------------
    # Startup logging (safe, no crashes)
    # --------------------------------------------------------------
    def post_init_warnings(self) -> None:
        # Minimal, safe logs (do not print secrets)
        logger.info("[env] App=%s | Env=%s | Version=%s", self.app_name, self.app_env, self.app_version)
        logger.info("[env] Providers=%s | primary=%s", ",".join(self.enabled_providers) or "<none>", self.primary_or_default_provider)
        logger.info("[env] APP_TOKEN=%s | BACKUP_APP_TOKEN=%s", _masked(self.app_token), _masked(self.backup_app_token))

        logger.info("[env] GOOGLE_SHEETS_CREDENTIALS=%s", "<set>" if self.google_sheets_credentials_raw else "<missing>")
        logger.info("[env] DEFAULT_SPREADSHEET_ID=%s", self.default_spreadsheet_id or "<missing>")
        logger.info("[env] Dashboard sheets=%s", self.dashboard_sheet_names)

        # Provider warnings
        if "eodhd" in self.enabled_providers and not self.eodhd_api_key:
            logger.warning("[env] EODHD_API_KEY missing but eodhd enabled (GLOBAL quotes may fail).")
        if "fmp" in self.enabled_providers and not self.fmp_api_key:
            logger.warning("[env] FMP_API_KEY missing but fmp enabled (fundamentals may be limited).")

        # KSA warning
        if not self.argaam_gateway_url:
            logger.warning("[env] ARGAAM_GATEWAY_URL missing (KSA .SR gateway may fail).")

        # Sheets warning
        if not self.google_sheets_credentials_raw:
            logger.warning("[env] GOOGLE_SHEETS_CREDENTIALS missing (backend Sheets API disabled).")
        if not self.default_spreadsheet_id:
            logger.warning("[env] DEFAULT_SPREADSHEET_ID missing (some dashboard helpers may need explicit spreadsheet_id).")


# ----------------------------------------------------------------------
# Global settings instance
# ----------------------------------------------------------------------
def _init_settings() -> Settings:
    try:
        s = Settings()
        s.post_init_warnings()
        return s
    except Exception as exc:
        # Last-resort: never crash import
        logger.error("[env] Failed to initialize Settings, using emergency defaults: %s", exc)
        return Settings.model_validate({})  # type: ignore


settings: Settings = _init_settings()


# ----------------------------------------------------------------------
# Convenience constants for legacy modules / routes (backwards compatible)
# ----------------------------------------------------------------------

# Meta
APP_ENV: str = settings.app_env
APP_NAME: str = settings.app_name
APP_VERSION: str = settings.app_version

# Base URLs & tokens
BACKEND_BASE_URL: str = settings.backend_base_url
APP_TOKEN: Optional[str] = settings.app_token
BACKUP_APP_TOKEN: Optional[str] = settings.backup_app_token

# Providers & timeouts
ENABLED_PROVIDERS: List[str] = settings.enabled_providers
PRIMARY_PROVIDER: str = settings.primary_or_default_provider
HTTP_TIMEOUT: int = settings.http_timeout
MAX_RETRIES: int = settings.max_retries

ENGINE_CACHE_TTL_SECONDS: int = settings.engine_cache_ttl_seconds
ENGINE_PROVIDER_TIMEOUT_SECONDS: int = settings.engine_provider_timeout_seconds
ENGINE_ENABLE_ADVANCED_ANALYSIS: bool = settings.engine_enable_advanced_analysis

KSA_ENGINE_CACHE_TTL_SECONDS: int = settings.ksa_engine_cache_ttl_seconds

# Provider-specific
EODHD_API_KEY: Optional[str] = settings.eodhd_api_key
EODHD_BASE_URL: str = settings.eodhd_base_url
FMP_API_KEY: Optional[str] = settings.fmp_api_key
FMP_BASE_URL: str = settings.fmp_base_url
FINNHUB_API_KEY: Optional[str] = settings.finnhub_api_key
ALPHA_VANTAGE_API_KEY: Optional[str] = settings.alpha_vantage_api_key

# KSA / Argaam
ARGAAM_GATEWAY_URL: Optional[str] = settings.argaam_gateway_url
ARGAAM_API_KEY: Optional[str] = settings.argaam_api_key

# Google integration
GOOGLE_SHEETS_CREDENTIALS_RAW: Optional[str] = settings.google_sheets_credentials_raw
GOOGLE_SHEETS_CREDENTIALS: Optional[Dict[str, Any]] = settings.google_sheets_credentials
GOOGLE_APPS_SCRIPT_BACKUP_URL: Optional[str] = settings.google_apps_script_backup_url
GOOGLE_PROJECT_ID: Optional[str] = settings.google_project_id
DEFAULT_SPREADSHEET_ID: Optional[str] = settings.default_spreadsheet_id

# Dashboard sheet names
SHEET_KSA_TADAWUL: str = settings.ksa_sheet_name
SHEET_GLOBAL_MARKETS: str = settings.global_sheet_name
SHEET_MUTUAL_FUNDS: str = settings.mutual_funds_sheet_name
SHEET_COMMODITIES_FX: str = settings.commodities_fx_sheet_name
SHEET_MARKET_LEADERS: str = settings.market_leaders_sheet_name
SHEET_MY_PORTFOLIO: str = settings.portfolio_sheet_name
SHEET_INSIGHTS_ANALYSIS: str = settings.insights_sheet_name
SHEET_INVESTMENT_ADVISOR: str = settings.advisor_sheet_name
SHEET_ECONOMIC_CALENDAR: str = settings.economic_calendar_sheet_name
SHEET_INVESTMENT_INCOME: str = settings.income_statement_sheet_name

# Misc
ENABLE_CORS_ALL_ORIGINS: bool = settings.enable_cors_all_origins
LOG_LEVEL: str = settings.log_level
HAS_SECURE_TOKEN: bool = settings.has_secure_token
IS_PRODUCTION: bool = settings.is_production


__all__ = [
    "Settings",
    "settings",
    "APP_ENV",
    "APP_NAME",
    "APP_VERSION",
    "BACKEND_BASE_URL",
    "APP_TOKEN",
    "BACKUP_APP_TOKEN",
    "ENABLED_PROVIDERS",
    "PRIMARY_PROVIDER",
    "HTTP_TIMEOUT",
    "MAX_RETRIES",
    "ENGINE_CACHE_TTL_SECONDS",
    "ENGINE_PROVIDER_TIMEOUT_SECONDS",
    "ENGINE_ENABLE_ADVANCED_ANALYSIS",
    "KSA_ENGINE_CACHE_TTL_SECONDS",
    "EODHD_API_KEY",
    "EODHD_BASE_URL",
    "FMP_API_KEY",
    "FMP_BASE_URL",
    "FINNHUB_API_KEY",
    "ALPHA_VANTAGE_API_KEY",
    "ARGAAM_GATEWAY_URL",
    "ARGAAM_API_KEY",
    "GOOGLE_SHEETS_CREDENTIALS_RAW",
    "GOOGLE_SHEETS_CREDENTIALS",
    "GOOGLE_APPS_SCRIPT_BACKUP_URL",
    "GOOGLE_PROJECT_ID",
    "DEFAULT_SPREADSHEET_ID",
    "SHEET_KSA_TADAWUL",
    "SHEET_GLOBAL_MARKETS",
    "SHEET_MUTUAL_FUNDS",
    "SHEET_COMMODITIES_FX",
    "SHEET_MARKET_LEADERS",
    "SHEET_MY_PORTFOLIO",
    "SHEET_INSIGHTS_ANALYSIS",
    "SHEET_INVESTMENT_ADVISOR",
    "SHEET_ECONOMIC_CALENDAR",
    "SHEET_INVESTMENT_INCOME",
    "ENABLE_CORS_ALL_ORIGINS",
    "LOG_LEVEL",
    "HAS_SECURE_TOKEN",
    "IS_PRODUCTION",
]
```
