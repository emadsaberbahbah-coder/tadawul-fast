from pydantic_settings import BaseSettings, SettingsConfigDict
from pydantic import field_validator
from functools import lru_cache
from typing import List, Optional, Union
import json

class Settings(BaseSettings):
    """
    Central configuration management for Tadawul Fast Bridge.
    Loads variables from Environment (Render) or .env file (Local).
    """

    # --- Application Info ---
    APP_NAME: str = "Tadawul Fast Bridge"
    APP_VERSION: str = "4.0.0"
    APP_ENV: str = "production"
    LOG_LEVEL: str = "INFO"
    PYTHON_VERSION: str = "3.11.9"

    # --- Security & Connectivity ---
    APP_TOKEN: Optional[str] = None  # Secret token for internal auth
    ENABLE_CORS_ALL_ORIGINS: bool = True
    HTTP_TIMEOUT_SEC: int = 25

    # --- Market Data Providers ---
    # Validates comma-separated strings (e.g., "eodhd,fmp") from Render
    ENABLED_PROVIDERS: List[str] = ["eodhd", "fmp", "yfinance"]
    PRIMARY_PROVIDER: str = "eodhd"
    
    # API Keys (Secrets)
    EODHD_API_KEY: Optional[str] = None
    FMP_API_KEY: Optional[str] = None
    
    # --- Feature Flags ---
    EODHD_FETCH_FUNDAMENTALS: bool = True
    ENABLE_YFINANCE: bool = True
    
    # --- Google Integration (Secrets) ---
    GOOGLE_SHEETS_CREDENTIALS: Optional[str] = None
    DEFAULT_SPREADSHEET_ID: Optional[str] = None
    GOOGLE_APPS_SCRIPT_BACKUP_URL: Optional[str] = None

    # --- Batch Processing (Performance Tuning) ---
    ADV_BATCH_SIZE: int = 20
    ADV_BATCH_TIMEOUT_SEC: int = 60
    ENRICHED_BATCH_SIZE: int = 40
    ENRICHED_BATCH_TIMEOUT_SEC: int = 45
    AI_BATCH_SIZE: int = 10
    AI_BATCH_TIMEOUT_SEC: int = 60
    ADV_BATCH_CONCURRENCY: int = 5

    # --- Validators ---
    
    @field_validator("ENABLED_PROVIDERS", mode="before")
    @classmethod
    def parse_providers_list(cls, v: Union[str, List[str]]) -> List[str]:
        """
        Fixes the Render env var issue. 
        Converts 'eodhd,fmp,yfinance' string into a real Python list.
        """
        if isinstance(v, str):
            # Check if empty
            if not v.strip():
                return []
            # Check if it's already JSON (starts with [)
            if v.strip().startswith("["):
                try:
                    return json.loads(v)
                except json.JSONDecodeError:
                    pass # Fallback to split if JSON fails
            
            # Default: Split by comma
            return [item.strip() for item in v.split(",") if item.strip()]
            
        return v

    # --- Pydantic V2 Configuration ---
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=True,
        extra="ignore"  # Ignore variables in .env that aren't listed here
    )

@lru_cache()
def get_settings() -> Settings:
    """
    Returns a cached instance of the Settings class.
    """
    return Settings()
