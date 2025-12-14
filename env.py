"""
env.py
------------------------------------------------------------
Centralized environment configuration for Tadawul Fast Bridge (v4.5.0).

Goals
- Zero-crash config loading (Render/.env/local).
- Strong defaults + safe parsing for bool/int/float/list/JSON.
- Backward compatible aliases (e.g., HTTP_TIMEOUT vs HTTP_TIMEOUT_SEC).

Usage
- from env import settings
"""

from __future__ import annotations

import json
import logging
import os
from typing import Any, Dict, List, Optional

from pydantic import BaseModel, Field

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
    if raw is None:
        return default
    try:
        return int(str(raw).strip())
    except Exception:
        logger.warning("[env] Invalid int for %s=%r, default=%s", key, raw, default)
        return default

def _get_float(key: str, default: float) -> float:
    raw = os.getenv(key)
    if raw is None:
        return default
    try:
        return float(str(raw).strip())
    except Exception:
        logger.warning("[env] Invalid float for %s=%r, default=%s", key, raw, default)
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
    return default

def _split_list(value: Optional[str]) -> List[str]:
    if not value:
        return []
    return [v.strip() for v in str(value).split(",") if v.strip()]

def _split_list_lower(value: Optional[str]) -> List[str]:
    return [v.lower() for v in _split_list(value)]

def _load_json(value: Optional[str]) -> Optional[Dict[str, Any]]:
    if not value:
        return None
    raw = str(value).strip()
    if (raw.startswith('"') and raw.endswith('"')) or (raw.startswith("'") and raw.endswith("'")):
        raw = raw[1:-1].strip()
    try:
        obj = json.loads(raw)
        return obj if isinstance(obj, dict) else None
    except Exception:
        return None

def _get_float_alias(keys: List[str], default: float) -> float:
    for k in keys:
        if os.getenv(k) is not None:
            return _get_float(k, default)
    return default

def _get_int_alias(keys: List[str], default: int) -> int:
    for k in keys:
        if os.getenv(k) is not None:
            return _get_int(k, default)
    return default

class Settings(BaseModel):
    # App
    app_name: str = Field(default_factory=lambda: _get_str("APP_NAME", "Tadawul Fast Bridge"))
    app_env: str = Field(default_factory=lambda: _get_str("APP_ENV", "production"))
    app_version: str = Field(default_factory=lambda: _get_str("APP_VERSION", "4.5.0"))
    log_level: str = Field(default_factory=lambda: _get_str("LOG_LEVEL", "INFO").upper())

    # Connectivity
    backend_base_url: str = Field(default_factory=lambda: _get_str("BACKEND_BASE_URL", "https://tadawul-fast-bridge.onrender.com"))
    http_timeout_sec: float = Field(default_factory=lambda: _get_float_alias(["HTTP_TIMEOUT_SEC", "HTTP_TIMEOUT"], 25.0))

    # Auth
    app_token: Optional[str] = Field(default_factory=lambda: os.getenv("APP_TOKEN"))
    backup_app_token: Optional[str] = Field(default_factory=lambda: os.getenv("BACKUP_APP_TOKEN"))

    # Providers
    enabled_providers: List[str] = Field(default_factory=lambda: _split_list_lower(_get_str("ENABLED_PROVIDERS", "eodhd,fmp,yfinance")))
    primary_provider: str = Field(default_factory=lambda: _get_str("PRIMARY_PROVIDER", "eodhd").lower())

    eodhd_api_key: Optional[str] = Field(default_factory=lambda: os.getenv("EODHD_API_KEY"))
    eodhd_base_url: str = Field(default_factory=lambda: _get_str("EODHD_BASE_URL", "https://eodhd.com/api"))
    eodhd_fetch_fundamentals: bool = Field(default_factory=lambda: _get_bool("EODHD_FETCH_FUNDAMENTALS", True))

    fmp_api_key: Optional[str] = Field(default_factory=lambda: os.getenv("FMP_API_KEY"))
    fmp_base_url: str = Field(default_factory=lambda: _get_str("FMP_BASE_URL", "https://financialmodelingprep.com/api/v3"))

    enable_yfinance: bool = Field(default_factory=lambda: _get_bool("ENABLE_YFINANCE", True))

    argaam_gateway_url: Optional[str] = Field(default_factory=lambda: os.getenv("ARGAAM_GATEWAY_URL"))

    # Cache TTLs
    cache_ttl_sec: float = Field(default_factory=lambda: _get_float("CACHE_TTL_SEC", 20.0))
    argaam_snapshot_ttl_sec: float = Field(default_factory=lambda: _get_float("ARGAAM_SNAPSHOT_TTL_SEC", 30.0))
    fundamentals_ttl_sec: float = Field(default_factory=lambda: _get_float("FUNDAMENTALS_TTL_SEC", 21600.0))
    quote_ttl_sec: float = Field(default_factory=lambda: _get_float("QUOTE_TTL_SEC", 30.0))

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

    # Google
    google_sheets_credentials_raw: Optional[str] = Field(default_factory=lambda: os.getenv("GOOGLE_SHEETS_CREDENTIALS"))
    default_spreadsheet_id: Optional[str] = Field(default_factory=lambda: os.getenv("DEFAULT_SPREADSHEET_ID"))
    google_apps_script_backup_url: Optional[str] = Field(default_factory=lambda: os.getenv("GOOGLE_APPS_SCRIPT_BACKUP_URL"))

    enable_cors_all_origins: bool = Field(default_factory=lambda: _get_bool("ENABLE_CORS_ALL_ORIGINS", True))

    @property
    def google_sheets_credentials(self) -> Optional[Dict[str, Any]]:
        return _load_json(self.google_sheets_credentials_raw)

    @property
    def is_production(self) -> bool:
        return self.app_env.strip().lower() in {"prod", "production"}

    def post_init_warnings(self) -> None:
        logger.info("[env] App=%s | Env=%s | Version=%s", self.app_name, self.app_env, self.app_version)
        logger.info("[env] Providers=%s", ",".join(self.enabled_providers))
        if "eodhd" in self.enabled_providers and not self.eodhd_api_key:
            logger.warning("[env] EODHD enabled but EODHD_API_KEY missing.")
        if "fmp" in self.enabled_providers and not self.fmp_api_key:
            logger.warning("[env] FMP enabled but FMP_API_KEY missing.")

def _init_settings() -> Settings:
    try:
        s = Settings()
        s.post_init_warnings()
        return s
    except Exception as exc:
        logger.error("[env] Failed to initialize Settings: %s", exc)
        return Settings.model_validate({})

settings: Settings = _init_settings()

__all__ = ["Settings", "settings"]
