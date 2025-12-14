from __future__ import annotations

from functools import lru_cache
from typing import Optional

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    # Load env vars (and optional local files). Render will use OS env.
    model_config = SettingsConfigDict(
        env_file=(".env", ".env.local", "env.local"),
        extra="ignore",
        case_sensitive=False,
    )

    # --- App ---
    app_name: str = Field(default="Tadawul Fast Bridge", validation_alias="APP_NAME")
    env: str = Field(default="production", validation_alias="APP_ENV")
    version: str = Field(default="4.6.0", validation_alias="APP_VERSION")
    providers: str = Field(default="eodhd,finnhub", validation_alias="PROVIDERS")
    cors_all_origins: bool = Field(default=True, validation_alias="CORS_ALL_ORIGINS")

    # --- Security ---
    app_token: Optional[str] = Field(default=None, validation_alias="APP_TOKEN")

    # --- Providers ---
    eodhd_api_key: Optional[str] = Field(default=None, validation_alias="EODHD_API_KEY")
    finnhub_api_key: Optional[str] = Field(default=None, validation_alias="FINNHUB_API_KEY")
    fmp_api_key: Optional[str] = Field(default=None, validation_alias="FMP_API_KEY")

    # --- Google Sheets ---
    google_sheets_credentials: Optional[str] = Field(default=None, validation_alias="GOOGLE_SHEETS_CREDENTIALS")
    google_sheet_id: Optional[str] = Field(default=None, validation_alias="GOOGLE_SHEET_ID")

    @property
    def providers_list(self) -> list[str]:
        return [p.strip().lower() for p in (self.providers or "").split(",") if p.strip()]


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    return Settings()
