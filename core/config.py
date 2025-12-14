# config.py
from __future__ import annotations

from functools import lru_cache
from typing import Optional

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    # --- App ---
    app_name: str = Field(default="Tadawul Fast Bridge", alias="APP_NAME")
    env: str = Field(default="production", alias="APP_ENV")
    version: str = Field(default="4.6.0", alias="APP_VERSION")
    providers: str = Field(default="eodhd,finnhub", alias="PROVIDERS")
    cors_all_origins: bool = Field(default=True, alias="CORS_ALL_ORIGINS")

    # --- Security ---
    app_token: Optional[str] = Field(default=None, alias="APP_TOKEN")

    # --- Providers ---
    eodhd_api_key: Optional[str] = Field(default=None, alias="EODHD_API_KEY")
    finnhub_api_key: Optional[str] = Field(default=None, alias="FINNHUB_API_KEY")
    fmp_api_key: Optional[str] = Field(default=None, alias="FMP_API_KEY")

    # --- Google Sheets ---
    google_sheets_credentials: Optional[str] = Field(default=None, alias="GOOGLE_SHEETS_CREDENTIALS")
    google_sheet_id: Optional[str] = Field(default=None, alias="GOOGLE_SHEET_ID")

    # Load env vars (and optional .env locally)
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    return Settings()
