"""
config.py
--------------------------------------------
Optional Pydantic Settings (v2) – SAFE PARSING

If you already use env.py everywhere, you can keep this file,
but it must NOT crash on Render when env vars come as strings.

This Settings class mirrors key env vars and provides robust parsing.
"""

import json
from functools import lru_cache
from typing import List, Optional, Union

from pydantic import field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

class Settings(BaseSettings):
    # --- Application Info ---
    APP_NAME: str = "Tadawul Fast Bridge"
    APP_VERSION: str = "4.4.0"
    APP_ENV: str = "production"
    LOG_LEVEL: str = "INFO"
    PYTHON_VERSION: str = "3.11.9"

    # --- Connectivity / Auth ---
    BACKEND_BASE_URL: str = "https://tadawul-fast-bridge.onrender.com"
    APP_TOKEN: Optional[str] = None
    BACKUP_APP_TOKEN: Optional[str] = None
    ENABLE_CORS_ALL_ORIGINS: bool = True
    HTTP_TIMEOUT_SEC: float = 25.0

    # --- Providers ---
    ENABLED_PROVIDERS: Union[str, List[str]] = "eodhd,fmp,yfinance"
    PRIMARY_PROVIDER: str = "eodhd"
    EODHD_API_KEY: Optional[str] = None
    FMP_API_KEY: Optional[str] = None
    ENABLE_YFINANCE: bool = True
    EODHD_FETCH_FUNDAMENTALS: bool = True

    # --- Google ---
    GOOGLE_SHEETS_CREDENTIALS: Optional[str] = None
    DEFAULT_SPREADSHEET_ID: Optional[str] = None

    # --- Batch / Performance ---
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
        if s.startswith("[") and s.endswith("]"):
            try:
                arr = json.loads(s)
                if isinstance(arr, list):
                    return [str(x).strip().lower() for x in arr if str(x).strip()]
            except Exception:
                pass
        return [p.strip().lower() for p in s.split(",") if p.strip()]

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=True,
        extra="ignore",
    )

@lru_cache()
def get_settings() -> Settings:
    return Settings()
