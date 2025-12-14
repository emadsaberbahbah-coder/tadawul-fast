"""
config.py
============================================================
Optional Pydantic Settings (v2) – SAFE + ALIGNED (v4.6.0)

✅ Purpose
- Keep this file as a *safe* Settings layer for modules that still import config.py
- Never crash on Render when env vars arrive as strings
- Fully aligned with your render.yaml + env.py philosophy

✅ Key Enhancements
- Accept BOTH HTTP_TIMEOUT / HTTP_TIMEOUT_SEC
- Parse ENABLED_PROVIDERS from CSV or JSON list
- Adds missing provider vars: ARGAAM / FINNHUB / ALPHA_VANTAGE + base URLs
- Safe JSON parsing for GOOGLE_SHEETS_CREDENTIALS (with private_key \\n fix)
- Adds engine/cache vars you use in render.yaml (ENGINE_CACHE_TTL_SECONDS, etc.)
- Adds sheet names (SHEET_*), so Sheets routing stays consistent
"""

from __future__ import annotations

import json
from functools import lru_cache
from typing import Any, Dict, List, Optional, Union

from pydantic import Field, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


def _to_bool(v: Any, default: bool = False) -> bool:
    if v is None:
        return default
    if isinstance(v, bool):
        return v
    s = str(v).strip().lower()
    if s in {"1", "true", "yes", "y", "on"}:
        return True
    if s in {"0", "false", "no", "n", "off"}:
        return False
    return default


def _to_float(v: Any, default: float) -> float:
    if v is None:
        return default
    try:
        return float(str(v).strip())
    except Exception:
        return default


def _to_int(v: Any, default: int) -> int:
    if v is None:
        return default
    try:
        return int(str(v).strip())
    except Exception:
        return default


def _parse_json_obj(v: Any) -> Optional[Dict[str, Any]]:
    """
    Parses JSON dict from env var safely.
    Returns None if not JSON object.
    """
    if v is None:
        return None
    if isinstance(v, dict):
        return v
    s = str(v).strip()
    if not s:
        return None
    # remove wrapping quotes if present
    if (s.startswith('"') and s.endswith('"')) or (s.startswith("'") and s.endswith("'")):
        s = s[1:-1].strip()
    if not (s.startswith("{") and s.endswith("}")):
        return None
    try:
        obj = json.loads(s)
        if isinstance(obj, dict):
            # common service-account issue: escaped newlines
            if "private_key" in obj and isinstance(obj["private_key"], str):
                obj["private_key"] = obj["private_key"].replace("\\n", "\n")
            return obj
    except Exception:
        return None
    return None


class Settings(BaseSettings):
    # ------------------------------------------------------------------
    # Application Info
    # ------------------------------------------------------------------
    APP_NAME: str = "Tadawul Fast Bridge"
    APP_VERSION: str = "4.6.0"
    APP_ENV: str = "production"
    LOG_LEVEL: str = "INFO"
    PYTHON_VERSION: str = "3.11.9"

    # ------------------------------------------------------------------
    # Connectivity / Auth
    # ------------------------------------------------------------------
    BACKEND_BASE_URL: str = "https://tadawul-fast-bridge.onrender.com"
    APP_TOKEN: Optional[str] = None
    BACKUP_APP_TOKEN: Optional[str] = None
    ENABLE_CORS_ALL_ORIGINS: bool = True

    # Accept BOTH names (Render yaml sometimes uses HTTP_TIMEOUT)
    HTTP_TIMEOUT_SEC: float = 25.0
    HTTP_TIMEOUT: Optional[float] = None  # if present, it can override

    HAS_SECURE_TOKEN: Optional[bool] = None  # optional flag if you use it

    # ------------------------------------------------------------------
    # Providers (GLOBAL + KSA-SAFE)
    # ------------------------------------------------------------------
    ENABLED_PROVIDERS: Union[str, List[str]] = "eodhd,fmp,yfinance"
    PRIMARY_PROVIDER: str = "eodhd"

    # EODHD (GLOBAL ONLY)
    EODHD_API_KEY: Optional[str] = None
    EODHD_BASE_URL: str = "https://eodhd.com/api"
    EODHD_FETCH_FUNDAMENTALS: bool = True

    # FMP (GLOBAL)
    FMP_API_KEY: Optional[str] = None
    FMP_BASE_URL: str = "https://financialmodelingprep.com/api/v3"

    # Yahoo (fallback)
    ENABLE_YFINANCE: bool = True

    # Optional future providers
    FINNHUB_API_KEY: Optional[str] = None
    ALPHA_VANTAGE_API_KEY: Optional[str] = None

    # KSA Gateway
    ARGAAM_GATEWAY_URL: Optional[str] = None
    ARGAAM_API_KEY: Optional[str] = None

    # ------------------------------------------------------------------
    # Engine / Cache / Retries (aligned with render.yaml)
    # ------------------------------------------------------------------
    ENGINE_CACHE_TTL_SECONDS: int = 60
    ENGINE_PROVIDER_TIMEOUT_SECONDS: int = 20
    ENGINE_ENABLE_ADVANCED_ANALYSIS: bool = True
    KSA_ENGINE_CACHE_TTL_SECONDS: int = 60
    MAX_RETRIES: int = 2

    # ------------------------------------------------------------------
    # Google
    # ------------------------------------------------------------------
    GOOGLE_SHEETS_CREDENTIALS: Optional[str] = None
    DEFAULT_SPREADSHEET_ID: Optional[str] = None
    GOOGLE_APPS_SCRIPT_BACKUP_URL: Optional[str] = None
    GOOGLE_PROJECT_ID: Optional[str] = None

    # ------------------------------------------------------------------
    # Sheet Names (so all modules stay consistent)
    # ------------------------------------------------------------------
    SHEET_KSA_TADAWUL: str = "KSA_Tadawul_Market"
    SHEET_GLOBAL_MARKETS: str = "Global_Markets"
    SHEET_MUTUAL_FUNDS: str = "Mutual_Funds"
    SHEET_COMMODITIES_FX: str = "Commodities_FX"
    SHEET_MARKET_LEADERS: str = "Market_Leaders"
    SHEET_MY_PORTFOLIO: str = "My_Portfolio"
    SHEET_INSIGHTS_ANALYSIS: str = "Insights_Analysis"
    SHEET_INVESTMENT_ADVISOR: str = "Investment_Advisor"
    SHEET_ECONOMIC_CALENDAR: str = "Economic_Calendar"
    SHEET_INVESTMENT_INCOME: str = "Investment_Income_Statement"

    # ------------------------------------------------------------------
    # Batch / Performance (used by routes)
    # ------------------------------------------------------------------
    ADV_BATCH_SIZE: int = 20
    ADV_BATCH_TIMEOUT_SEC: float = 45.0
    ADV_MAX_TICKERS: int = 500
    ADV_BATCH_CONCURRENCY: int = 5

    AI_BATCH_SIZE: int = 20
    AI_BATCH_TIMEOUT_SEC: float = 45.0
    AI_MAX_TICKERS: int = 500
    AI_BATCH_CONCURRENCY: int = 5

    ENRICHED_BATCH_SIZE: int = 40
    ENRICHED_BATCH_TIMEOUT_SEC: float = 45.0
    ENRICHED_MAX_TICKERS: int = 250
    ENRICHED_BATCH_CONCURRENCY: int = 5

    # ------------------------------------------------------------------
    # Validators
    # ------------------------------------------------------------------
    @field_validator("ENABLED_PROVIDERS", mode="before")
    @classmethod
    def parse_providers_list(cls, v: Union[str, List[str]]) -> List[str]:
        if isinstance(v, list):
            return [str(x).strip().lower() for x in v if str(x).strip()]
        if not isinstance(v, str):
            return []
        s = v.strip()
        if not s:
            return []
        # JSON list
        if s.startswith("[") and s.endswith("]"):
            try:
                arr = json.loads(s)
                if isinstance(arr, list):
                    return [str(x).strip().lower() for x in arr if str(x).strip()]
            except Exception:
                pass
        # CSV list
        return [p.strip().lower() for p in s.split(",") if p.strip()]

    @field_validator("ENABLE_CORS_ALL_ORIGINS", mode="before")
    @classmethod
    def parse_bool_cors(cls, v: Any) -> bool:
        return _to_bool(v, default=True)

    @field_validator("ENABLE_YFINANCE", mode="before")
    @classmethod
    def parse_bool_yf(cls, v: Any) -> bool:
        return _to_bool(v, default=True)

    @field_validator("EODHD_FETCH_FUNDAMENTALS", mode="before")
    @classmethod
    def parse_bool_eodhd_fund(cls, v: Any) -> bool:
        return _to_bool(v, default=True)

    @field_validator("ENGINE_ENABLE_ADVANCED_ANALYSIS", mode="before")
    @classmethod
    def parse_bool_adv(cls, v: Any) -> bool:
        return _to_bool(v, default=True)

    @field_validator("HTTP_TIMEOUT_SEC", mode="before")
    @classmethod
    def parse_timeout(cls, v: Any) -> float:
        return _to_float(v, default=25.0)

    @field_validator("HTTP_TIMEOUT", mode="before")
    @classmethod
    def parse_timeout_alt(cls, v: Any) -> Optional[float]:
        if v is None or str(v).strip() == "":
            return None
        return _to_float(v, default=25.0)

    @field_validator(
        "ENGINE_CACHE_TTL_SECONDS",
        "ENGINE_PROVIDER_TIMEOUT_SECONDS",
        "KSA_ENGINE_CACHE_TTL_SECONDS",
        "MAX_RETRIES",
        "ADV_BATCH_SIZE",
        "ADV_MAX_TICKERS",
        "ADV_BATCH_CONCURRENCY",
        "AI_BATCH_SIZE",
        "AI_MAX_TICKERS",
        "AI_BATCH_CONCURRENCY",
        "ENRICHED_BATCH_SIZE",
        "ENRICHED_MAX_TICKERS",
        "ENRICHED_BATCH_CONCURRENCY",
        mode="before",
    )
    @classmethod
    def parse_int_fields(cls, v: Any) -> int:
        # default will be applied by pydantic if conversion fails,
        # but we make it more robust here
        return _to_int(v, default=0)

    @field_validator(
        "ADV_BATCH_TIMEOUT_SEC",
        "AI_BATCH_TIMEOUT_SEC",
        "ENRICHED_BATCH_TIMEOUT_SEC",
        mode="before",
    )
    @classmethod
    def parse_float_fields(cls, v: Any) -> float:
        return _to_float(v, default=45.0)

    # ------------------------------------------------------------------
    # Computed helpers (safe, optional)
    # ------------------------------------------------------------------
    @property
    def enabled_providers_list(self) -> List[str]:
        # After validation, ENABLED_PROVIDERS becomes List[str]
        if isinstance(self.ENABLED_PROVIDERS, list):
            return self.ENABLED_PROVIDERS
        return self.parse_providers_list(self.ENABLED_PROVIDERS)  # fallback

    @property
    def effective_http_timeout_sec(self) -> float:
        # If HTTP_TIMEOUT is set, prefer it; else HTTP_TIMEOUT_SEC
        return float(self.HTTP_TIMEOUT) if self.HTTP_TIMEOUT is not None else float(self.HTTP_TIMEOUT_SEC)

    @property
    def is_production(self) -> bool:
        return str(self.APP_ENV).strip().lower() in {"prod", "production"}

    @property
    def google_sheets_credentials_dict(self) -> Optional[Dict[str, Any]]:
        return _parse_json_obj(self.GOOGLE_SHEETS_CREDENTIALS)

    @property
    def backend_base_url_stripped(self) -> str:
        return (self.BACKEND_BASE_URL or "").rstrip("/")

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,  # safer locally + on Render
        extra="ignore",
    )


@lru_cache()
def get_settings() -> Settings:
    return Settings()
