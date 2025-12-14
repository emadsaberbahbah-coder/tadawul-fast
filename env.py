"""
env.py
------------------------------------------------------------
Centralized environment configuration for Tadawul Fast Bridge (v4.6.0)

Enhancements
- Accept HTTP_TIMEOUT or HTTP_TIMEOUT_SEC
- Parse ENABLED_PROVIDERS from JSON or comma
- Adds FINNHUB / ALPHA_VANTAGE / ARGAAM API keys safely
- Keeps backward compatible exports
"""

from __future__ import annotations

import json
import logging
import os
from typing import Any, Dict, List, Optional

try:
    from pydantic import BaseModel, Field
except Exception:
    class BaseModel:  # type: ignore
        def __init__(self, **kwargs): self.__dict__.update(kwargs)
    def Field(default=None, **kwargs): return default  # type: ignore

try:
    from dotenv import load_dotenv  # type: ignore
    load_dotenv()
except Exception:
    pass

logger = logging.getLogger(__name__)

_TRUTHY = {"1", "true", "yes", "on", "y"}
_FALSY = {"0", "false", "no", "off", "n"}

def _get_str(key: str, default: str = "") -> str:
    v = os.getenv(key)
    return default if v is None else str(v).strip()

def _get_int(key: str, default: int) -> int:
    raw = os.getenv(key)
    if raw is None: return default
    try: return int(str(raw).strip())
    except Exception:
        logger.warning("[env] Invalid int %s=%r, default=%s", key, raw, default)
        return default

def _get_float(key: str, default: float) -> float:
    raw = os.getenv(key)
    if raw is None: return default
    try: return float(str(raw).strip())
    except Exception: return default

def _get_bool(key: str, default: bool) -> bool:
    raw = os.getenv(key)
    if raw is None: return default
    s = str(raw).strip().lower()
    if s in _TRUTHY: return True
    if s in _FALSY: return False
    return default

def _split_list(value: Optional[str]) -> List[str]:
    if not value: return []
    return [v.strip() for v in str(value).split(",") if v.strip()]

def _parse_providers(value: str) -> List[str]:
    v = (value or "").strip()
    if not v:
        return []
    # JSON list
    if v.startswith("[") and v.endswith("]"):
        try:
            arr = json.loads(v)
            if isinstance(arr, list):
                return [str(x).strip().lower() for x in arr if str(x).strip()]
        except Exception:
            pass
    # comma list
    return [x.lower() for x in _split_list(v)]

def _load_json(value: Optional[str]) -> Optional[Dict[str, Any]]:
    if not value: return None
    raw = str(value).strip()
    if (raw.startswith('"') and raw.endswith('"')) or (raw.startswith("'") and raw.endswith("'")):
        raw = raw[1:-1].strip()
    if not raw.startswith("{"): return None
    try: return json.loads(raw)
    except Exception as exc:
        logger.warning("[env] Failed to parse JSON env value: %s", exc)
        return None

class Settings(BaseModel):
    app_name: str = Field(default_factory=lambda: _get_str("APP_NAME", "Tadawul Fast Bridge"))
    app_env: str = Field(default_factory=lambda: _get_str("APP_ENV", "production"))
    app_version: str = Field(default_factory=lambda: _get_str("APP_VERSION", "4.6.0"))
    log_level: str = Field(default_factory=lambda: _get_str("LOG_LEVEL", "INFO").upper())

    backend_base_url: str = Field(default_factory=lambda: _get_str("BACKEND_BASE_URL", "https://tadawul-fast-bridge.onrender.com").rstrip("/"))

    # Accept BOTH names
    http_timeout_sec: float = Field(default_factory=lambda: _get_float("HTTP_TIMEOUT_SEC", _get_float("HTTP_TIMEOUT", 25.0)))

    app_token: Optional[str] = Field(default_factory=lambda: os.getenv("APP_TOKEN"))
    backup_app_token: Optional[str] = Field(default_factory=lambda: os.getenv("BACKUP_APP_TOKEN"))

    enabled_providers: List[str] = Field(default_factory=lambda: _parse_providers(_get_str("ENABLED_PROVIDERS", "eodhd,fmp,yfinance")))
    primary_provider: str = Field(default_factory=lambda: _get_str("PRIMARY_PROVIDER", "eodhd").lower())

    # Provider keys
    eodhd_api_key: Optional[str] = Field(default_factory=lambda: os.getenv("EODHD_API_KEY"))
    eodhd_base_url: str = Field(default_factory=lambda: _get_str("EODHD_BASE_URL", "https://eodhd.com/api"))
    fmp_api_key: Optional[str] = Field(default_factory=lambda: os.getenv("FMP_API_KEY"))
    fmp_base_url: str = Field(default_factory=lambda: _get_str("FMP_BASE_URL", "https://financialmodelingprep.com/api/v3"))

    finnhub_api_key: Optional[str] = Field(default_factory=lambda: os.getenv("FINNHUB_API_KEY"))
    alpha_vantage_api_key: Optional[str] = Field(default_factory=lambda: os.getenv("ALPHA_VANTAGE_API_KEY"))

    argaam_gateway_url: Optional[str] = Field(default_factory=lambda: os.getenv("ARGAAM_GATEWAY_URL"))
    argaam_api_key: Optional[str] = Field(default_factory=lambda: os.getenv("ARGAAM_API_KEY"))

    enable_yfinance: bool = Field(default_factory=lambda: _get_bool("ENABLE_YFINANCE", True))

    # Cache/TTL (align with your render.yaml naming too)
    cache_ttl_sec: float = Field(default_factory=lambda: _get_float("CACHE_TTL_SEC", _get_float("ENGINE_CACHE_TTL_SECONDS", 20.0)))
    quote_ttl_sec: float = Field(default_factory=lambda: _get_float("QUOTE_TTL_SEC", 30.0))
    fundamentals_ttl_sec: float = Field(default_factory=lambda: _get_float("FUNDAMENTALS_TTL_SEC", 21600.0))
    argaam_snapshot_ttl_sec: float = Field(default_factory=lambda: _get_float("ARGAAM_SNAPSHOT_TTL_SEC", 30.0))

    # Batch limits
    adv_batch_size: int = Field(default_factory=lambda: _get_int("ADV_BATCH_SIZE", 20))
    adv_batch_timeout_sec: float = Field(default_factory=lambda: _get_float("ADV_BATCH_TIMEOUT_SEC", 45.0))
    adv_max_tickers: int = Field(default_factory=lambda: _get_int("ADV_MAX_TICKERS", 500))
    adv_batch_concurrency: int = Field(default_factory=lambda: _get_int("ADV_BATCH_CONCURRENCY", 5))

    ai_batch_size: int = Field(default_factory=lambda: _get_int("AI_BATCH_SIZE", 20))
    ai_batch_timeout_sec: float = Field(default_factory=lambda: _get_float("AI_BATCH_TIMEOUT_SEC", 45.0))
    ai_max_tickers: int = Field(default_factory=lambda: _get_int("AI_MAX_TICKERS", 500))
    ai_batch_concurrency: int = Field(default_factory=lambda: _get_int("AI_BATCH_CONCURRENCY", 5))

    enriched_batch_size: int = Field(default_factory=lambda: _get_int("ENRICHED_BATCH_SIZE", 40))
    enriched_batch_timeout_sec: float = Field(default_factory=lambda: _get_float("ENRICHED_BATCH_TIMEOUT_SEC", 45.0))
    enriched_max_tickers: int = Field(default_factory=lambda: _get_int("ENRICHED_MAX_TICKERS", 250))
    enriched_batch_concurrency: int = Field(default_factory=lambda: _get_int("ENRICHED_BATCH_CONCURRENCY", 5))

    # Sheet names
    sheet_ksa_tadawul: str = Field(default_factory=lambda: _get_str("SHEET_KSA_TADAWUL", "KSA_Tadawul_Market"))
    sheet_global_markets: str = Field(default_factory=lambda: _get_str("SHEET_GLOBAL_MARKETS", "Global_Markets"))
    sheet_mutual_funds: str = Field(default_factory=lambda: _get_str("SHEET_MUTUAL_FUNDS", "Mutual_Funds"))
    sheet_commodities_fx: str = Field(default_factory=lambda: _get_str("SHEET_COMMODITIES_FX", "Commodities_FX"))
    sheet_market_leaders: str = Field(default_factory=lambda: _get_str("SHEET_MARKET_LEADERS", "Market_Leaders"))
    sheet_my_portfolio: str = Field(default_factory=lambda: _get_str("SHEET_MY_PORTFOLIO", "My_Portfolio"))
    sheet_insights_analysis: str = Field(default_factory=lambda: _get_str("SHEET_INSIGHTS_ANALYSIS", "Insights_Analysis"))
    sheet_investment_advisor: str = Field(default_factory=lambda: _get_str("SHEET_INVESTMENT_ADVISOR", "Investment_Advisor"))
    sheet_economic_calendar: str = Field(default_factory=lambda: _get_str("SHEET_ECONOMIC_CALENDAR", "Economic_Calendar"))
    sheet_investment_income: str = Field(default_factory=lambda: _get_str("SHEET_INVESTMENT_INCOME", "Investment_Income_Statement"))

    # Google integration
    google_sheets_credentials_raw: Optional[str] = Field(default_factory=lambda: os.getenv("GOOGLE_SHEETS_CREDENTIALS"))
    google_apps_script_backup_url: Optional[str] = Field(default_factory=lambda: os.getenv("GOOGLE_APPS_SCRIPT_BACKUP_URL"))
    default_spreadsheet_id: Optional[str] = Field(default_factory=lambda: os.getenv("DEFAULT_SPREADSHEET_ID"))

    enable_cors_all_origins: bool = Field(default_factory=lambda: _get_bool("ENABLE_CORS_ALL_ORIGINS", True))

    @property
    def google_sheets_credentials(self) -> Optional[Dict[str, Any]]:
        return _load_json(self.google_sheets_credentials_raw)

    @property
    def is_production(self) -> bool:
        return self.app_env.strip().lower() in {"prod", "production"}

    def post_init_warnings(self) -> None:
        logger.info("[env] App=%s | Env=%s | Version=%s", self.app_name, self.app_env, self.app_version)
        logger.info("[env] Providers=%s", ",".join(self.enabled_providers or []))
        if "eodhd" in (self.enabled_providers or []) and not self.eodhd_api_key:
            logger.warning("[env] EODHD enabled but EODHD_API_KEY missing.")
        if "fmp" in (self.enabled_providers or []) and not self.fmp_api_key:
            logger.warning("[env] FMP enabled but FMP_API_KEY missing.")

def _init_settings() -> Settings:
    try:
        s = Settings()
        s.post_init_warnings()
        return s
    except Exception as exc:
        logger.error("[env] Failed to initialize Settings: %s", exc)
        return Settings()  # type: ignore

settings: Settings = _init_settings()

# Backward compatible exports
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

__all__ = ["Settings", "settings"]
