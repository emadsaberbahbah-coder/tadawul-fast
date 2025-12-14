import json
from typing import List, Optional, Union
from functools import lru_cache

from pydantic import field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

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
    APP_TOKEN: Optional[str] = None
    ENABLE_CORS_ALL_ORIGINS: bool = True
    HTTP_TIMEOUT_SEC: int = 25

    # --- Market Data Providers ---
    # CRITICAL FIX: We use Union[str, List[str]] to prevent Pydantic 
    # from crashing when it sees a comma-separated string in Render.
    ENABLED_PROVIDERS: Union[str, List[str]] = ["eodhd", "fmp", "yfinance"]
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
        Parses the environment variable into a list.
        Handles:
        1. JSON strings: '["a", "b"]'
        2. Comma-separated strings: 'a,b,c'
        3. Existing lists
        """
        if isinstance(v, str):
            v = v.strip()
            if not v:
                return []
            
            # Try JSON first (e.g. if user entered ["eodhd", "fmp"])
            if v.startswith("[") and v.endswith("]"):
                try:
                    return json.loads(v)
                except json.JSONDecodeError:
                    pass
            
            # Fallback: Split by comma (e.g. eodhd,fmp)
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
