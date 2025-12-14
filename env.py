"""
env.py
------------------------------------------------------------
Centralized environment configuration for Tadawul Fast Bridge (Hardened)

Goals
- Zero-crash config loading (Render/.env/local).
- Strong defaults + safe parsing for bool/int/float/list/JSON.
- Backwards compatible attribute names used across the codebase:
    settings.backend_base_url
    settings.enriched_batch_size / timeout / concurrency
    settings.google_sheets_credentials_raw / google_sheets_credentials
    settings.sheet_* names
"""

from __future__ import annotations

import json
import logging
import os
from functools import lru_cache
from typing import Any, Dict, List, Optional, Sequence, Union

from pydantic import Field, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

# Optional: load from .env during local dev (safe if python-dotenv not installed)
try:
    from dotenv import load_dotenv  # type: ignore
    load_dotenv()
except Exception:
    pass

logger = logging.getLogger(__name__)

_TRUTHY = {"1", "true", "yes", "on", "y"}
_FALSY = {"0", "false", "no", "off", "n"}


def _as_str(v: Any, default: str = "") -> str:
    if v is None:
        return default
    s = str(v).strip()
    return s if s else default


def _as_opt_str(v: Any) -> Optional[str]:
    s = _as_str(v, "")
    return s if s else None


def _as_int(v: Any, default: int) -> int:
    try:
        if v is None:
            return default
        return int(str(v).strip())
    except Exception:
        return default


def _as_float(v: Any, default: float) -> float:
    try:
        if v is None:
            return default
        return float(str(v).strip())
    except Exception:
        return default


def _as_bool(v: Any, default: bool) -> bool:
    if v is None:
        return default
    s = str(v).strip().lower()
    if s in _TRUTHY:
        return True
    if s in _FALSY:
        return False
    return default


def _dedupe_preserve_lower(items: Sequence[str]) -> List[str]:
    seen = set()
    out: List[str] = []
    for x in items:
        x = (x or "").strip().lower()
        if not x:
            continue
        if x in seen:
            continue
        seen.add(x)
        out.append(x)
    return out


def _parse_list(v: Union[str, List[str], None]) -> List[str]:
    """
    Accepts:
      - None
      - ["eodhd","fmp"]
      - '["eodhd","fmp"]'
      - 'eodhd,fmp,yfinance'
      - 'eodhd fmp yfinance'
    Returns: lowercased, deduped list.
    """
    if v is None:
        return []

    if isinstance(v, list):
        return _dedupe_preserve_lower([str(x) for x in v])

    s = str(v).strip()
    if not s:
        return []

    # JSON list
    if s.startswith("[") and s.endswith("]"):
        try:
            raw = json.loads(s)
            if isinstance(raw, list):
                return _dedupe_preserve_lower([str(x) for x in raw])
        except Exception:
            pass

    # Comma-separated
    if "," in s:
        return _dedupe_preserve_lower([p.strip() for p in s.split(",")])

    # Space-separated fallback
    return _dedupe_preserve_lower([p.strip() for p in s.split()])


def _load_json_object(raw: Optional[str]) -> Optional[Dict[str, Any]]:
    if not raw:
        return None
    s = str(raw).strip()
    if not s:
        return None

    # Unwrap one layer of quotes if env value is wrapped
    if (s.startswith('"') and s.endswith('"')) or (s.startswith("'") and s.endswith("'")):
        s = s[1:-1].strip()

    # Must look like JSON object
    if not (s.startswith("{") and s.endswith("}")):
        return None

    try:
        d = json.loads(s)
        if not isinstance(d, dict):
            return None
        # Fix Render newline escaping for private_key
        pk = d.get("private_key")
        if isinstance(pk, str):
            d["private_key"] = pk.replace("\\n", "\n")
        return d
    except Exception:
        return None


def _masked(value: Optional[str]) -> str:
    if not value:
        return "<empty>"
    s = str(value)
    if len(s) <= 6:
        return "*" * len(s)
    return f"{s[:2]}***{s[-2:]} (len={len(s)})"


class Settings(BaseSettings):
    # --------------------------------------------------------------
    # Application Meta
    # --------------------------------------------------------------
    app_name: str = Field(default="Tadawul Fast Bridge", alias="APP_NAME")
    app_env: str = Field(default="production", alias="APP_ENV")
    app_version: str = Field(default="4.0.0", alias="APP_VERSION")
    log_level: str = Field(default="INFO", alias="LOG_LEVEL")

    # --------------------------------------------------------------
    # Connectivity
    # --------------------------------------------------------------
    backend_base_url: str = Field(
        default="https://tadawul-fast-bridge.onrender.com",
        alias="BACKEND_BASE_URL",
    )
    http_timeout_sec: float = Field(default=25.0, alias="HTTP_TIMEOUT_SEC")

    # --------------------------------------------------------------
    # Auth (Optional / Legacy)
    # --------------------------------------------------------------
    app_token: Optional[str] = Field(default=None, alias="APP_TOKEN")
    backup_app_token: Optional[str] = Field(default=None, alias="BACKUP_APP_TOKEN")

    # --------------------------------------------------------------
    # Providers Configuration
    # --------------------------------------------------------------
    enabled_providers: List[str] = Field(default_factory=lambda: ["eodhd", "fmp", "yfinance"], alias="ENABLED_PROVIDERS")
    primary_provider: str = Field(default="eodhd", alias="PRIMARY_PROVIDER")

    # EODHD (Global)
    eodhd_api_key: Optional[str] = Field(default=None, alias="EODHD_API_KEY")
    eodhd_base_url: str = Field(default="https://eodhd.com/api", alias="EODHD_BASE_URL")
    eodhd_fetch_fundamentals: bool = Field(default=True, alias="EODHD_FETCH_FUNDAMENTALS")

    # FMP (Global/Fundamentals)
    fmp_api_key: Optional[str] = Field(default=None, alias="FMP_API_KEY")
    fmp_base_url: str = Field(default="https://financialmodelingprep.com/api/v3", alias="FMP_BASE_URL")

    # YFinance (Fallback)
    enable_yfinance: bool = Field(default=True, alias="ENABLE_YFINANCE")

    # Optional legacy/bridge
    argaam_gateway_url: Optional[str] = Field(default=None, alias="ARGAAM_GATEWAY_URL")

    # --------------------------------------------------------------
    # Caching (used by some modules)
    # --------------------------------------------------------------
    cache_ttl_sec: float = Field(default=20.0, alias="CACHE_TTL_SEC")
    argaam_snapshot_ttl_sec: float = Field(default=30.0, alias="ARGAAM_SNAPSHOT_TTL_SEC")
    fundamentals_ttl_sec: float = Field(default=21600.0, alias="FUNDAMENTALS_TTL_SEC")
    quote_ttl_sec: float = Field(default=30.0, alias="QUOTE_TTL_SEC")

    # --------------------------------------------------------------
    # Batch Processing Limits (Critical for Google Sheets stability)
    # --------------------------------------------------------------
    adv_batch_size: int = Field(default=20, alias="ADV_BATCH_SIZE")
    adv_batch_timeout_sec: float = Field(default=45.0, alias="ADV_BATCH_TIMEOUT_SEC")
    adv_max_tickers: int = Field(default=500, alias="ADV_MAX_TICKERS")
    adv_batch_concurrency: int = Field(default=5, alias="ADV_BATCH_CONCURRENCY")

    ai_batch_size: int = Field(default=20, alias="AI_BATCH_SIZE")
    ai_batch_timeout_sec: float = Field(default=45.0, alias="AI_BATCH_TIMEOUT_SEC")
    ai_max_tickers: int = Field(default=500, alias="AI_MAX_TICKERS")
    ai_batch_concurrency: int = Field(default=5, alias="AI_BATCH_CONCURRENCY")

    enriched_batch_size: int = Field(default=40, alias="ENRICHED_BATCH_SIZE")
    enriched_batch_timeout_sec: float = Field(default=45.0, alias="ENRICHED_BATCH_TIMEOUT_SEC")
    enriched_max_tickers: int = Field(default=250, alias="ENRICHED_MAX_TICKERS")
    enriched_batch_concurrency: int = Field(default=5, alias="ENRICHED_BATCH_CONCURRENCY")

    # --------------------------------------------------------------
    # Sheet Names (Reference)
    # --------------------------------------------------------------
    sheet_ksa_tadawul: str = Field(default="KSA_Tadawul_Market", alias="SHEET_KSA_TADAWUL")
    sheet_global_markets: str = Field(default="Global_Markets", alias="SHEET_GLOBAL_MARKETS")
    sheet_mutual_funds: str = Field(default="Mutual_Funds", alias="SHEET_MUTUAL_FUNDS")
    sheet_commodities_fx: str = Field(default="Commodities_FX", alias="SHEET_COMMODITIES_FX")
    sheet_market_leaders: str = Field(default="Market_Leaders", alias="SHEET_MARKET_LEADERS")
    sheet_my_portfolio: str = Field(default="My_Portfolio", alias="SHEET_MY_PORTFOLIO")
    sheet_insights_analysis: str = Field(default="Insights_Analysis", alias="SHEET_INSIGHTS_ANALYSIS")
    sheet_investment_advisor: str = Field(default="Investment_Advisor", alias="SHEET_INVESTMENT_ADVISOR")
    sheet_economic_calendar: str = Field(default="Economic_Calendar", alias="SHEET_ECONOMIC_CALENDAR")
    sheet_investment_income: str = Field(default="Investment_Income_Statement", alias="SHEET_INVESTMENT_INCOME")

    # --------------------------------------------------------------
    # Google Integration
    # --------------------------------------------------------------
    google_sheets_credentials_raw: Optional[str] = Field(default=None, alias="GOOGLE_SHEETS_CREDENTIALS")
    google_apps_script_backup_url: Optional[str] = Field(default=None, alias="GOOGLE_APPS_SCRIPT_BACKUP_URL")
    default_spreadsheet_id: Optional[str] = Field(default=None, alias="DEFAULT_SPREADSHEET_ID")

    # Misc
    enable_cors_all_origins: bool = Field(default=True, alias="ENABLE_CORS_ALL_ORIGINS")

    # -------------------------
    # Validators / coercion
    # -------------------------
    @field_validator("log_level", mode="before")
    @classmethod
    def _v_log_level(cls, v: Any) -> str:
        s = _as_str(v, "INFO").upper()
        return "WARNING" if s == "WARN" else s

    @field_validator("app_env", mode="before")
    @classmethod
    def _v_app_env(cls, v: Any) -> str:
        s = _as_str(v, "production").lower()
        if s == "prod":
            return "production"
        if s == "dev":
            return "development"
        return s or "production"

    @field_validator("http_timeout_sec", mode="before")
    @classmethod
    def _v_timeout(cls, v: Any) -> float:
        x = _as_float(v, 25.0)
        return 25.0 if x <= 0 else x

    @field_validator("enabled_providers", mode="before")
    @classmethod
    def _v_providers(cls, v: Any) -> List[str]:
        lst = _parse_list(v)
        return lst or ["eodhd", "fmp", "yfinance"]

    @field_validator("primary_provider", mode="before")
    @classmethod
    def _v_primary(cls, v: Any) -> str:
        return _as_str(v, "eodhd").lower()

    @field_validator(
        "adv_batch_size",
        "ai_batch_size",
        "enriched_batch_size",
        "adv_batch_concurrency",
        "ai_batch_concurrency",
        "enriched_batch_concurrency",
        "adv_max_tickers",
        "ai_max_tickers",
        "enriched_max_tickers",
        mode="before",
    )
    @classmethod
    def _v_ints_positive(cls, v: Any) -> int:
        x = _as_int(v, 1)
        return max(1, x)

    @field_validator(
        "adv_batch_timeout_sec",
        "ai_batch_timeout_sec",
        "enriched_batch_timeout_sec",
        "cache_ttl_sec",
        "argaam_snapshot_ttl_sec",
        "fundamentals_ttl_sec",
        "quote_ttl_sec",
        mode="before",
    )
    @classmethod
    def _v_floats_positive(cls, v: Any) -> float:
        x = _as_float(v, 0.0)
        return max(1.0, x)

    @field_validator(
        "eodhd_fetch_fundamentals",
        "enable_yfinance",
        "enable_cors_all_origins",
        mode="before",
    )
    @classmethod
    def _v_bools(cls, v: Any) -> bool:
        return _as_bool(v, True)

    @field_validator("backend_base_url", mode="before")
    @classmethod
    def _v_backend_url(cls, v: Any) -> str:
        s = _as_str(v, "https://tadawul-fast-bridge.onrender.com")
        return s.rstrip("/")

    # -------------------------
    # Convenience properties
    # -------------------------
    @property
    def google_sheets_credentials(self) -> Optional[Dict[str, Any]]:
        return _load_json_object(self.google_sheets_credentials_raw)

    @property
    def is_production(self) -> bool:
        return self.app_env.strip().lower() in {"prod", "production"}

    @property
    def primary_or_default_provider(self) -> str:
        enabled = [p for p in (self.enabled_providers or []) if p]
        primary = (self.primary_provider or "").strip().lower()
        if primary and primary in enabled:
            return primary
        return (enabled or ["eodhd"])[0]

    def post_init_warnings(self) -> None:
        logger.info("[env] App=%s | Env=%s | Version=%s", self.app_name, self.app_env, self.app_version)
        logger.info("[env] Providers=%s | Primary=%s", ",".join(self.enabled_providers), self.primary_or_default_provider)

        if "eodhd" in self.enabled_providers and not self.eodhd_api_key:
            logger.warning("[env] EODHD enabled but EODHD_API_KEY missing.")
        if "fmp" in self.enabled_providers and not self.fmp_api_key:
            logger.warning("[env] FMP enabled but FMP_API_KEY missing.")

        if self.app_token:
            logger.info("[env] APP_TOKEN=%s", _masked(self.app_token))
        if self.default_spreadsheet_id:
            logger.info("[env] DEFAULT_SPREADSHEET_ID=%s", _masked(self.default_spreadsheet_id))


    # Pydantic settings config
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=True,
        extra="ignore",
        populate_by_name=True,
    )


@lru_cache(maxsize=1)
def get_env_settings() -> Settings:
    """
    Cached settings instance; never crashes the app.
    """
    try:
        s = Settings()
        try:
            s.post_init_warnings()
        except Exception:
            pass
        return s
    except Exception as exc:
        logger.error("[env] Failed to initialize Settings: %s", exc)
        # Last-resort: return defaults
        return Settings.model_validate({})  # type: ignore[arg-type]


# Global singleton (backwards compatible)
settings: Settings = get_env_settings()

# ----------------------------------------------------------------------
# Convenience Exports (Backwards Compatibility)
# ----------------------------------------------------------------------
APP_ENV = settings.app_env
APP_NAME = settings.app_name
BACKEND_BASE_URL = settings.backend_base_url
APP_TOKEN = settings.app_token
BACKUP_APP_TOKEN = settings.backup_app_token
ENABLED_PROVIDERS = settings.enabled_providers
PRIMARY_PROVIDER = settings.primary_provider
HTTP_TIMEOUT = int(settings.http_timeout_sec)

EODHD_API_KEY = settings.eodhd_api_key
EODHD_BASE_URL = settings.eodhd_base_url
FMP_API_KEY = settings.fmp_api_key
FMP_BASE_URL = settings.fmp_base_url
ARGAAM_GATEWAY_URL = settings.argaam_gateway_url

SHEET_KSA_TADAWUL = settings.sheet_ksa_tadawul
SHEET_GLOBAL_MARKETS = settings.sheet_global_markets
SHEET_MUTUAL_FUNDS = settings.sheet_mutual_funds
SHEET_COMMODITIES_FX = settings.sheet_commodities_fx
SHEET_MARKET_LEADERS = settings.sheet_market_leaders
SHEET_MY_PORTFOLIO = settings.sheet_my_portfolio
SHEET_INSIGHTS_ANALYSIS = settings.sheet_insights_analysis
SHEET_INVESTMENT_ADVISOR = settings.sheet_investment_advisor
SHEET_ECONOMIC_CALENDAR = settings.sheet_economic_calendar
SHEET_INVESTMENT_INCOME = settings.sheet_investment_income

ENABLE_CORS_ALL_ORIGINS = settings.enable_cors_all_origins
LOG_LEVEL = settings.log_level
IS_PRODUCTION = settings.is_production

__all__ = [
    "Settings",
    "settings",
    "get_env_settings",
    "APP_ENV",
    "APP_NAME",
    "BACKEND_BASE_URL",
    "APP_TOKEN",
    "BACKUP_APP_TOKEN",
    "ENABLED_PROVIDERS",
    "PRIMARY_PROVIDER",
    "HTTP_TIMEOUT",
    "EODHD_API_KEY",
    "FMP_API_KEY",
]
