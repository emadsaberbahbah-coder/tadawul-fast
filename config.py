from __future__ import annotations

from functools import lru_cache
from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """
    Single source of truth for environment variables.

    Render env vars (Phase-1):
      - APP_TOKEN
      - DATABASE_URL
      - DYNAMIC_PAGES_DIR

    Optional:
      - ENV
      - APP_VERSION
      - SNAPSHOT_MAX_ROWS
    """

    # App
    app_name: str = "Tadawul Fast Bridge"
    env: str = Field(default="production", validation_alias="ENV")
    version: str = Field(default="5.0.0-phase1", validation_alias="APP_VERSION")

    # Security
    app_token: str = Field(default="", validation_alias="APP_TOKEN")

    # Storage
    database_url: str = Field(default="", validation_alias="DATABASE_URL")

    # Dynamic Pages
    dynamic_pages_dir: str = Field(
        default="config/dynamic_pages",
        validation_alias="DYNAMIC_PAGES_DIR",
    )

    # Safety: avoid oversized snapshot payloads
    snapshot_max_rows: int = Field(default=1000, validation_alias="SNAPSHOT_MAX_ROWS")

    # Ignore unexpected env vars (safe on Render)
    model_config = SettingsConfigDict(extra="ignore")


@lru_cache
def get_settings() -> Settings:
    # Cached to avoid re-parsing env vars on every request
    return Settings()
