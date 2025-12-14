# config.py
from __future__ import annotations

from functools import lru_cache
from typing import List, Optional

from pydantic import Field, field_validator, computed_field
from pydantic_settings import BaseSettings, SettingsConfigDict


def _none_if_empty(v):
    """Convert empty / null-ish env values to None."""
    if v is None:
        return None
    if isinstance(v, str):
        s = v.strip()
        if s == "" or s.lower() in {"none", "null", "nil"}:
            return None
        return s
    return v


class Settings(BaseSettings):
    # ---------------------------
    # App
    # ---------------------------
    app_name: str = Field(default="Tadawul Fast Bridge", alias="APP_NAME")
    env: str = Field(default="production", alias="APP_ENV")
    version: str = Field(default="4.6.0", alias="APP_VERSION")

    # Comma-separated list in env: "eodhd,finnhub"
    providers: str = Field(default="eodhd,finnhub", alias="PROVIDERS")

    cors_all_origins: bool = Field(default=True, alias="CORS_ALL_ORIGINS")

    # ---------------------------
    # Performance / Safety
    # ---------------------------
    rate_limit_per_minute: int = Field(default=240, alias="RATE_LIMIT_PER_MINUTE")
    http_timeout_seconds: float = Field(default=15.0, alias="HTTP_TIMEOUT_SECONDS")
    cache_ttl_seconds: int = Field(default=60, alias="CACHE_TTL_SECONDS")

    # ---------------------------
    # Security
    # ---------------------------
    app_token: Optional[str] = Field(default=None, alias="APP_TOKEN")

    # ---------------------------
    # Providers
    # ---------------------------
    eodhd_api_key: Optional[str] = Field(default=None, alias="EODHD_API_KEY")
    finnhub_api_key: Optional[str] = Field(default=None, alias="FINNHUB_API_KEY")
    fmp_api_key: Optional[str] = Field(default=None, alias="FMP_API_KEY")

    # ---------------------------
    # Google Sheets
    # ---------------------------
    google_sheets_credentials: Optional[str] = Field(default=None, alias="GOOGLE_SHEETS_CREDENTIALS")
    google_sheet_id: Optional[str] = Field(default=None, alias="GOOGLE_SHEET_ID")

    # Load env vars (and optional .env for local dev only)
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
        populate_by_name=True,
    )

    # ---------------------------
    # Validators (defensive)
    # ---------------------------
    @field_validator(
        "app_token",
        "eodhd_api_key",
        "finnhub_api_key",
        "fmp_api_key",
        "google_sheets_credentials",
        "google_sheet_id",
        mode="before",
    )
    @classmethod
    def _empty_to_none(cls, v):
        return _none_if_empty(v)

    @field_validator("providers", mode="before")
    @classmethod
    def _providers_clean(cls, v):
        v = _none_if_empty(v)
        return v or "eodhd,finnhub"

    @computed_field
    @property
    def providers_list(self) -> List[str]:
        # "eodhd,finnhub" -> ["eodhd", "finnhub"]
        items = [p.strip().lower() for p in (self.providers or "").split(",")]
        return [p for p in items if p]

    @computed_field
    @property
    def is_production(self) -> bool:
        return (self.env or "").strip().lower() in {"prod", "production"}


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    # Cached singleton settings instance
    return Settings()
