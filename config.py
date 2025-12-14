# config.py
from __future__ import annotations

from functools import lru_cache
from typing import Optional, List

from pydantic import Field, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


def _split_csv(value: str) -> List[str]:
    return [x.strip().lower() for x in value.split(",") if x.strip()]


class Settings(BaseSettings):
    # --- App ---
    app_name: str = Field(default="Tadawul Fast Bridge", alias="APP_NAME")
    env: str = Field(default="production", alias="APP_ENV")
    version: str = Field(default="4.6.0", alias="APP_VERSION")

    # Keep as string for backward compatibility with your existing code
    providers: str = Field(default="eodhd,finnhub", alias="PROVIDERS")

    cors_all_origins: bool = Field(default=True, alias="CORS_ALL_ORIGINS")
    cors_origins: Optional[str] = Field(default=None, alias="CORS_ORIGINS")  # optional CSV

    # --- Security ---
    app_token: Optional[str] = Field(default=None, alias="APP_TOKEN")

    # --- Providers ---
    eodhd_api_key: Optional[str] = Field(default=None, alias="EODHD_API_KEY")
    finnhub_api_key: Optional[str] = Field(default=None, alias="FINNHUB_API_KEY")
    fmp_api_key: Optional[str] = Field(default=None, alias="FMP_API_KEY")

    # --- Google Sheets ---
    google_sheets_credentials: Optional[str] = Field(default=None, alias="GOOGLE_SHEETS_CREDENTIALS")
    google_sheet_id: Optional[str] = Field(default=None, alias="GOOGLE_SHEET_ID")

    # --- Operational defaults (safe to add; won’t break existing code) ---
    log_level: str = Field(default="INFO", alias="LOG_LEVEL")
    rate_limit_per_minute: int = Field(default=240, alias="RATE_LIMIT_PER_MINUTE")

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
        case_sensitive=False,
        populate_by_name=True,
    )

    @field_validator("providers", mode="before")
    @classmethod
    def normalize_providers(cls, v):
        # Accept list-like values but store as CSV string for compatibility
        if v is None:
            return "eodhd,finnhub"
        if isinstance(v, (list, tuple)):
            return ",".join(str(x).strip() for x in v if str(x).strip())
        return str(v).strip()

    @property
    def providers_list(self) -> List[str]:
        return _split_csv(self.providers or "")

    @property
    def cors_origins_list(self) -> List[str]:
        if self.cors_all_origins:
            return ["*"]
        if not self.cors_origins:
            return []
        return [x.strip() for x in self.cors_origins.split(",") if x.strip()]


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    return Settings()
