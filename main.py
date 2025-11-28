from pydantic import Field, validator
from pydantic_settings import BaseSettings
from typing import List, Optional, Dict, Any
import json
import logging

logger = logging.getLogger(__name__)

class Settings(BaseSettings):
    """Centralized configuration management"""
    
    # Service Configuration
    service_name: str = Field("Tadawul Stock Analysis API", env="SERVICE_NAME")
    service_version: str = Field("3.0.0", env="SERVICE_VERSION")
    app_host: str = Field("0.0.0.0", env="APP_HOST")
    app_port: int = Field(8000, env="APP_PORT")
    environment: str = Field("production", env="ENVIRONMENT")
    debug: bool = Field(False, env="DEBUG")

    # Security
    require_auth: bool = Field(True, env="REQUIRE_AUTH")
    app_token: Optional[str] = Field(None, env="APP_TOKEN")
    backup_app_token: Optional[str] = Field(None, env="BACKUP_APP_TOKEN")
    
    # Rate Limiting
    enable_rate_limiting: bool = Field(True, env="ENABLE_RATE_LIMITING")
    max_requests_per_minute: int = Field(60, env="RATE_LIMIT_REQUESTS_PER_MINUTE")

    # CORS
    cors_origins: List[str] = Field(default_factory=lambda: ["*"], env="CORS_ORIGINS")

    # Documentation
    enable_swagger: bool = Field(True, env="ENABLE_SWAGGER")
    enable_redoc: bool = Field(True, env="ENABLE_REDOC")

    # Google Services
    spreadsheet_id: str = Field(..., env="SPREADSHEET_ID")
    google_sheets_credentials: Optional[str] = Field(None, env="GOOGLE_SHEETS_CREDENTIALS")
    google_apps_script_url: str = Field(..., env="GOOGLE_APPS_SCRIPT_URL")
    google_apps_script_backup_url: Optional[str] = Field(None, env="GOOGLE_APPS_SCRIPT_BACKUP_URL")

    # Financial APIs
    alpha_vantage_api_key: Optional[str] = Field(None, env="ALPHA_VANTAGE_API_KEY")
    finnhub_api_key: Optional[str] = Field(None, env="FINNHUB_API_KEY")
    eodhd_api_key: Optional[str] = Field(None, env="EODHD_API_KEY")
    twelvedata_api_key: Optional[str] = Field(None, env="TWELVEDATA_API_KEY")
    marketstack_api_key: Optional[str] = Field(None, env="MARKETSTACK_API_KEY")
    fmp_api_key: Optional[str] = Field(None, env="FMP_API_KEY")

    # Cache
    cache_default_ttl: int = Field(1800, env="CACHE_DEFAULT_TTL")
    cache_max_size: int = Field(10000, env="CACHE_MAX_SIZE")
    cache_backup_enabled: bool = Field(True, env="CACHE_BACKUP_ENABLED")

    # Performance
    http_timeout: int = Field(30, env="HTTP_TIMEOUT")
    max_retries: int = Field(3, env="MAX_RETRIES")
    retry_delay: float = Field(1.0, env="RETRY_DELAY")

    # Symbols
    symbols_cache_ttl: int = Field(1800, env="SYMBOLS_CACHE_TTL")
    symbols_header_row: int = Field(2, env="SYMBOLS_HEADER_ROW")
    symbols_tab_name: str = Field("Market_Leaders", env="SYMBOLS_TAB_NAME")

    class Config:
        env_file = ".env"
        case_sensitive = False
        validate_assignment = True
        extra = "ignore"

    @property
    def google_service_account_json(self) -> Optional[str]:
        """Get Google service account JSON with backward compatibility"""
        if self.google_sheets_credentials:
            return self.google_sheets_credentials
        
        legacy_json = os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON")
        if legacy_json:
            logger.warning("Using deprecated GOOGLE_SERVICE_ACCOUNT_JSON")
            return legacy_json
        
        return None

    @validator("spreadsheet_id")
    def validate_spreadsheet_id(cls, v: str) -> str:
        if not v or len(v) < 10:
            raise ValueError("Invalid Spreadsheet ID")
        return v

    @validator("environment")
    def validate_environment(cls, v: str) -> str:
        allowed_environments = {"development", "staging", "production"}
        if v not in allowed_environments:
            raise ValueError(f"Environment must be one of: {allowed_environments}")
        return v

    @validator("google_sheets_credentials")
    def validate_google_credentials(cls, v: Optional[str]) -> Optional[str]:
        if v is None:
            return v
        
        try:
            json.loads(v)
            logger.info("Google Sheets credentials JSON format is valid")
        except json.JSONDecodeError as e:
            logger.error(f"Invalid JSON in GOOGLE_SHEETS_CREDENTIALS: {e}")
            raise ValueError(f"Invalid JSON format in Google Sheets credentials: {e}")
        
        return v

    def validate_urls(self) -> bool:
        """Validate critical URLs at startup"""
        urls_to_validate = [
            ("GOOGLE_APPS_SCRIPT_URL", self.google_apps_script_url),
        ]
        
        for name, url in urls_to_validate:
            if not url or url.strip() in ["", "undefined"]:
                logger.error(f"❌ Invalid URL provided for {name}: {url}")
                return False
                
            if not url.startswith(('http://', 'https://')):
                logger.error(f"❌ Invalid URL format for {name}: {url}")
                return False
                
        logger.info("✅ All URLs validated successfully")
        return True

    def log_config_summary(self):
        """Log configuration summary without sensitive data"""
        logger.info(f"🔧 {self.service_name} v{self.service_version}")
        logger.info(f"🌍 Environment: {self.environment}")
        logger.info(f"🔐 Auth: {'Enabled' if self.require_auth else 'Disabled'}")
        logger.info(f"⚡ Rate Limiting: {'Enabled' if self.enable_rate_limiting else 'Disabled'}")
        logger.info(f"📊 Google Sheets: {'Direct' if self.google_service_account_json else 'Apps Script'}")
        logger.info(f"💾 Cache TTL: {self.cache_default_ttl}s")
        
        # Count configured APIs
        configured_apis = sum([
            bool(self.alpha_vantage_api_key),
            bool(self.finnhub_api_key),
            bool(self.eodhd_api_key),
            bool(self.twelvedata_api_key),
            bool(self.marketstack_api_key),
            bool(self.fmp_api_key),
        ])
        logger.info(f"📈 Financial APIs: {configured_apis} configured")

# Global settings instance
settings = Settings()
