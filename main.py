#!/usr/bin/env python3
"""
Tadawul Stock Analysis API - Enhanced Version
Comprehensive financial data API with Google Sheets integration, caching, and multiple data sources.

Features:
- Real-time stock quotes from multiple financial APIs
- Google Sheets integration for portfolio tracking
- Advanced caching with TTL and persistence
- Comprehensive error handling and monitoring
- Rate limiting and security features
- Async/await for improved performance

Version: 2.4.0
"""

import asyncio
import datetime
import json
import logging
import os
import time
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import quote as url_quote

import aiohttp
import gspread
import uvicorn
from dotenv import load_dotenv
from fastapi import (
    BackgroundTasks,
    Depends,
    FastAPI,
    HTTPException,
    Query,
    Request,
    status,
)
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from google.oauth2.service_account import Credentials
from pydantic import BaseModel, Field, validator
from pydantic_settings import BaseSettings
from slowapi import Limiter, _rate_limit_exceeded_handler
from slowapi.errors import RateLimitExceeded
from slowapi.util import get_remote_address

# =============================================================================
# Configuration and Constants
# =============================================================================

# Load environment variables
BASE_DIR = Path(__file__).resolve().parent
load_dotenv(dotenv_path=BASE_DIR / ".env")

# Enhanced logging configuration
LOG_ENABLE_FILE = os.getenv("LOG_ENABLE_FILE", "false").lower() == "true"
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
LOG_FORMAT = os.getenv("LOG_FORMAT", "detailed")

# Configure logging based on format
if LOG_FORMAT == "detailed":
    log_format = "%(asctime)s - %(name)s - %(levelname)s - [%(filename)s:%(lineno)d] - %(message)s"
else:
    log_format = "%(asctime)s - %(levelname)s - %(message)s"

handlers: List[logging.Handler] = [logging.StreamHandler()]
if LOG_ENABLE_FILE:
    log_file = BASE_DIR / "app.log"
    handlers.append(logging.FileHandler(str(log_file), encoding='utf-8'))

logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.INFO),
    format=log_format,
    handlers=handlers,
)

logger = logging.getLogger(__name__)

# Constants
SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]

EXPECTED_SHEETS = [
    "KSA Tadawul Market",
    "Global Market Stock", 
    "Mutual Fund",
    "My Portfolio Investment",
    "Commodities & FX",
    "Advanced Analysis & Advice",
    "Economic Calendar",
    "Investment Income Statement YTD",
    "Investment Advisor Assumptions",
]

# =============================================================================
# Enhanced Configuration with Validation
# =============================================================================

class AppConfig(BaseSettings):
    """Enhanced application configuration with comprehensive validation"""
    
    # Service Configuration
    service_name: str = Field("Tadawul Stock Analysis API", env="SERVICE_NAME")
    service_version: str = Field("2.4.0", env="SERVICE_VERSION")
    app_host: str = Field("0.0.0.0", env="APP_HOST")
    app_port: int = Field(8000, env="APP_PORT")
    environment: str = Field("production", env="ENVIRONMENT")
    debug: bool = Field(False, env="DEBUG")

    # Security Configuration
    require_auth: bool = Field(True, env="REQUIRE_AUTH")
    app_token: Optional[str] = Field(None, env="APP_TOKEN")
    backup_app_token: Optional[str] = Field(None, env="BACKUP_APP_TOKEN")
    
    # Rate Limiting
    enable_rate_limiting: bool = Field(True, env="ENABLE_RATE_LIMITING")
    max_requests_per_minute: int = Field(60, env="RATE_LIMIT_REQUESTS_PER_MINUTE")

    # CORS Configuration
    cors_origins: List[str] = Field(default_factory=lambda: ["*"], env="CORS_ORIGINS")

    # API Documentation
    enable_swagger: bool = Field(True, env="ENABLE_SWAGGER")
    enable_redoc: bool = Field(True, env="ENABLE_REDOC")

    # Google Sheets Configuration
    spreadsheet_id: str = Field(..., env="SPREADSHEET_ID")
    google_sheets_credentials: Optional[str] = Field(None, env="GOOGLE_SHEETS_CREDENTIALS")
    google_apps_script_url: str = Field(..., env="GOOGLE_APPS_SCRIPT_URL")
    google_apps_script_backup_url: Optional[str] = Field(None, env="GOOGLE_APPS_SCRIPT_BACKUP_URL")

    # Financial Data APIs
    alpha_vantage_api_key: Optional[str] = Field(None, env="ALPHA_VANTAGE_API_KEY")
    finnhub_api_key: Optional[str] = Field(None, env="FINNHUB_API_KEY")
    eodhd_api_key: Optional[str] = Field(None, env="EODHD_API_KEY")
    twelvedata_api_key: Optional[str] = Field(None, env="TWELVEDATA_API_KEY")
    marketstack_api_key: Optional[str] = Field(None, env="MARKETSTACK_API_KEY")
    fmp_api_key: Optional[str] = Field(None, env="FMP_API_KEY")

    # Cache Configuration
    cache_default_ttl: int = Field(1800, env="CACHE_DEFAULT_TTL")  # 30 minutes
    cache_max_size: int = Field(10000, env="CACHE_MAX_SIZE")
    cache_backup_enabled: bool = Field(True, env="CACHE_BACKUP_ENABLED")

    # Performance Configuration
    http_timeout: int = Field(30, env="HTTP_TIMEOUT")
    max_retries: int = Field(3, env="MAX_RETRIES")
    retry_delay: float = Field(1.0, env="RETRY_DELAY")

    class Config:
        env_file = ".env"
        case_sensitive = False
        validate_assignment = True
        extra = "ignore"  # Ignore extra environment variables

    @property
    def google_service_account_json(self) -> Optional[str]:
        """Get Google service account JSON with backward compatibility"""
        if self.google_sheets_credentials:
            return self.google_sheets_credentials
        
        # Legacy support
        legacy_json = os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON")
        if legacy_json:
            logger.warning("Using deprecated GOOGLE_SERVICE_ACCOUNT_JSON - migrate to GOOGLE_SHEETS_CREDENTIALS")
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

    @validator("cache_default_ttl")
    def validate_cache_ttl(cls, v: int) -> int:
        if v < 60:
            raise ValueError("Cache TTL must be at least 60 seconds")
        return v

# Initialize configuration with enhanced error handling
try:
    config = AppConfig()
    logger.info(f"✅ Configuration loaded for {config.service_name} v{config.service_version}")
except Exception as e:
    logger.error(f"❌ Configuration validation failed: {e}")
    raise

# =============================================================================
# Enhanced Exception Classes
# =============================================================================

class FinancialAPIError(Exception):
    """Custom exception for financial API errors"""
    def __init__(self, message: str, api_name: str = "unknown", status_code: int = 500):
        self.message = message
        self.api_name = api_name
        self.status_code = status_code
        super().__init__(self.message)

class GoogleSheetsError(Exception):
    """Custom exception for Google Sheets errors"""
    def __init__(self, message: str, operation: str = "unknown", sheet_name: str = None):
        self.message = message
        self.operation = operation
        self.sheet_name = sheet_name
        super().__init__(self.message)

class CacheError(Exception):
    """Custom exception for cache errors"""
    def __init__(self, message: str, operation: str = "unknown"):
        self.message = message
        self.operation = operation
        super().__init__(self.message)

class ConfigurationError(Exception):
    """Custom exception for configuration errors"""
    pass

class SecurityError(Exception):
    """Custom exception for security errors"""
    pass

class RateLimitError(Exception):
    """Custom exception for rate limiting errors"""
    pass

# =============================================================================
# Enhanced Security Setup
# =============================================================================

security = HTTPBearer(auto_error=False)

# Enhanced rate limiter
limiter = Limiter(
    key_func=get_remote_address,
    default_limits=[f"{config.max_requests_per_minute}/minute"] if config.enable_rate_limiting else []
)

def rate_limit(rule: str):
    """Conditional rate limiting based on configuration"""
    if not config.enable_rate_limiting:
        def _decorator(fn):
            return fn
        return _decorator
    return limiter.limit(rule)

# =============================================================================
# Enhanced Authentication
# =============================================================================

def verify_auth(credentials: Optional[HTTPAuthorizationCredentials] = Depends(security)):
    """
    Enhanced authentication dependency with comprehensive security features
    """
    # If auth is disabled or no tokens configured, allow access
    if not config.require_auth:
        logger.debug("Authentication disabled via configuration")
        return True

    # Collect valid tokens
    valid_tokens = [token for token in [config.app_token, config.backup_app_token] if token]
    
    if not valid_tokens:
        logger.warning("REQUIRE_AUTH is True but no valid tokens configured. Allowing access.")
        return True

    if not credentials:
        logger.warning("Authentication required but no credentials provided")
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Missing authentication token",
            headers={"WWW-Authenticate": "Bearer"},
        )

    # Enhanced token validation
    token = credentials.credentials.strip()
    if not token:
        logger.warning("Empty token provided")
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid authentication token",
        )

    # Token length validation
    if len(token) < 10:
        logger.warning(f"Suspicious token length: {len(token)}")
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid authentication token",
        )

    if token not in valid_tokens:
        logger.warning(f"Invalid token attempt: {token[:8]}...")
        # Log additional security information
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid authentication token",
        )

    logger.info(f"Successful authentication with token: {token[:8]}...")
    return True

def public_endpoint():
    """Explicit public endpoint marker - no authentication required"""
    return True

# =============================================================================
# Enhanced Financial APIs Configuration
# =============================================================================

FINANCIAL_APIS = {
    "alpha_vantage": {
        "api_key": config.alpha_vantage_api_key,
        "base_url": "https://www.alphavantage.co/query",
        "timeout": config.http_timeout,
        "rate_limit": 5,  # requests per minute
        "enabled": bool(config.alpha_vantage_api_key),
    },
    "finnhub": {
        "api_key": config.finnhub_api_key,
        "base_url": "https://finnhub.io/api/v1",
        "timeout": config.http_timeout,
        "rate_limit": 60,  # requests per minute
        "enabled": bool(config.finnhub_api_key),
    },
    "eodhd": {
        "api_key": config.eodhd_api_key,
        "base_url": "https://eodhistoricaldata.com/api",
        "timeout": config.http_timeout,
        "rate_limit": 1000,  # requests per day
        "enabled": bool(config.eodhd_api_key),
    },
    "twelvedata": {
        "api_key": config.twelvedata_api_key,
        "base_url": "https://api.twelvedata.com",
        "timeout": config.http_timeout,
        "rate_limit": 800,  # requests per day
        "enabled": bool(config.twelvedata_api_key),
    },
    "marketstack": {
        "api_key": config.marketstack_api_key,
        "base_url": "http://api.marketstack.com/v1",
        "timeout": config.http_timeout,
        "rate_limit": 1000,  # requests per month
        "enabled": bool(config.marketstack_api_key),
    },
    "fmp": {
        "api_key": config.fmp_api_key,
        "base_url": "https://financialmodelingprep.com/api/v3",
        "timeout": config.http_timeout,
        "rate_limit": 250,  # requests per day
        "enabled": bool(config.fmp_api_key),
    },
}

GOOGLE_SERVICES = {
    "spreadsheet_id": config.spreadsheet_id,
    "apps_script_url": config.google_apps_script_url,
    "apps_script_backup_url": config.google_apps_script_backup_url,
}

# =============================================================================
# Enhanced Pydantic Models
# =============================================================================

class Quote(BaseModel):
    """Enhanced quote model with comprehensive validation"""
    ticker: str = Field(..., description="Stock ticker symbol", example="7201.SR")
    company: Optional[str] = Field(None, description="Company name")
    currency: Optional[str] = Field(None, description="Currency code", example="SAR")
    price: Optional[float] = Field(None, ge=0, description="Current price")
    previous_close: Optional[float] = Field(None, ge=0, description="Previous close price")
    day_change: Optional[float] = Field(None, description="Daily change amount")
    day_change_pct: Optional[float] = Field(None, description="Daily change percentage")
    market_cap: Optional[float] = Field(None, ge=0, description="Market capitalization")
    volume: Optional[float] = Field(None, ge=0, description="Trading volume")
    high_52_week: Optional[float] = Field(None, ge=0, description="52-week high")
    low_52_week: Optional[float] = Field(None, ge=0, description="52-week low")
    open_price: Optional[float] = Field(None, ge=0, description="Open price")
    timestamp_utc: Optional[str] = Field(None, description="UTC timestamp")
    data_source: Optional[str] = Field(None, description="Data source identifier")
    exchange: Optional[str] = Field(None, description="Stock exchange")
    sector: Optional[str] = Field(None, description="Company sector")
    country: Optional[str] = Field(None, description="Country code")

    @validator("ticker")
    def validate_ticker(cls, v: str) -> str:
        v = v.strip().upper()
        if not v:
            raise ValueError("Ticker cannot be empty")
        if len(v) > 20:
            raise ValueError("Ticker too long")
        return v

    @validator("day_change_pct")
    def validate_change_pct(cls, v: Optional[float]) -> Optional[float]:
        if v is not None and abs(v) > 1000:  # Sanity check for change percentage
            raise ValueError("Change percentage out of reasonable range")
        return v

    @validator("price", "previous_close", "market_cap", "volume", "high_52_week", "low_52_week", "open_price")
    def validate_positive_or_none(cls, v: Optional[float]) -> Optional[float]:
        if v is not None and v < 0:
            raise ValueError("Value must be positive or None")
        return v

class QuoteUpdatePayload(BaseModel):
    """Payload for updating quotes with enhanced validation"""
    data: List[Quote] = Field(..., description="List of quotes to update")
    source: Optional[str] = Field("api", description="Data source")
    force_refresh: bool = Field(False, description="Force cache refresh")

    @validator("data")
    def validate_data(cls, v: List[Quote]) -> List[Quote]:
        if not v:
            raise ValueError("Empty data list")
        if len(v) > 1000:
            raise ValueError("Too many quotes in single update (max 1000)")
        return v

class QuoteResponse(BaseModel):
    """Standardized quote response with metadata"""
    data: List[Quote] = Field(..., description="List of quotes")
    count: int = Field(..., description="Number of quotes returned")
    timestamp_utc: str = Field(..., description="Response timestamp")
    cache_hits: int = Field(0, description="Number of cache hits")
    cache_misses: int = Field(0, description="Number of cache misses")
    sources: List[str] = Field(default_factory=list, description="Data sources used")

class SheetVerificationResult(BaseModel):
    """Enhanced sheet verification result"""
    sheet_name: str = Field(..., description="Sheet name")
    status: str = Field(..., description="Verification status")
    row_count: int = Field(..., description="Number of rows")
    column_count: int = Field(..., description="Number of columns")
    headers: List[str] = Field(..., description="Column headers")
    sample_data: List[Dict[str, Any]] = Field(..., description="Sample data rows")
    error: Optional[str] = Field(None, description="Error message if any")
    data_quality: Optional[str] = Field(None, description="Data quality assessment")
    last_updated: Optional[str] = Field(None, description="Last update timestamp")

class VerificationResponse(BaseModel):
    """Enhanced sheet verification response"""
    spreadsheet_id: str = Field(..., description="Spreadsheet ID")
    overall_status: str = Field(..., description="Overall verification status")
    sheets_verified: int = Field(..., description="Number of sheets verified")
    sheets_ok: int = Field(..., description="Number of sheets with OK status")
    details: List[SheetVerificationResult] = Field(..., description="Detailed results")
    verification_timestamp: str = Field(..., description="Verification timestamp")

class SheetDataResponse(BaseModel):
    """Enhanced sheet data response"""
    sheet_name: str = Field(..., description="Sheet name")
    total_records: int = Field(..., description="Total records in sheet")
    data: List[Dict[str, Any]] = Field(..., description="Sheet data records")
    headers: List[str] = Field(..., description="Column headers")
    last_updated: Optional[str] = Field(None, description="Last update timestamp")

class HealthResponse(BaseModel):
    """Enhanced health check response"""
    status: str = Field(..., description="Overall health status")
    time_utc: str = Field(..., description="Current UTC time")
    version: str = Field(..., description="API version")
    uptime_seconds: float = Field(..., description="Application uptime in seconds")
    google_sheets_connected: bool = Field(..., description="Google Sheets connection status")
    cache_status: Dict[str, Any] = Field(..., description="Cache status information")
    features: Dict[str, bool] = Field(..., description="Available features")
    api_status: Dict[str, Any] = Field(..., description="Financial API status")
    performance: Dict[str, float] = Field(..., description="Performance metrics")

class ErrorResponse(BaseModel):
    """Enhanced error response"""
    error: str = Field(..., description="Error type")
    detail: Optional[str] = Field(None, description="Detailed error message")
    timestamp: str = Field(..., description="Error timestamp")
    request_id: Optional[str] = Field(None, description="Request identifier for debugging")
    code: Optional[str] = Field(None, description="Error code")
    suggestion: Optional[str] = Field(None, description="Suggested action")

class PerformanceMetrics(BaseModel):
    """Performance metrics model"""
    request_count: int = Field(0, description="Total requests processed")
    average_response_time: float = Field(0.0, description="Average response time in seconds")
    error_rate: float = Field(0.0, description="Error rate percentage")
    cache_hit_rate: float = Field(0.0, description="Cache hit rate percentage")
    active_connections: int = Field(0, description="Active HTTP connections")

# =============================================================================
# Enhanced TTL Cache with Performance Monitoring
# =============================================================================

class TTLCache:
    """
    Enhanced TTL cache with performance monitoring, backup, and advanced features
    """
    
    def __init__(self, ttl_minutes: int = None, max_size: int = None):
        self.cache_path = BASE_DIR / "quote_cache.json"
        self.backup_dir = BASE_DIR / "cache_backups"
        self.backup_dir.mkdir(parents=True, exist_ok=True)
        self.data: Dict[str, Dict[str, Any]] = {}
        self.ttl = (ttl_minutes or (config.cache_default_ttl // 60)) * 60
        self.max_size = max_size or config.cache_max_size
        self.logger = logging.getLogger(__name__)
        self.metrics = {
            "hits": 0,
            "misses": 0,
            "updates": 0,
            "expired_removals": 0,
            "errors": 0,
        }
        self._load_cache()

    def _is_valid(self, item: Dict[str, Any], now: Optional[float] = None) -> bool:
        """Check if cache item is still valid"""
        if now is None:
            now = time.time()
        timestamp = item.get("_cache_timestamp", 0)
        return (now - timestamp) < self.ttl

    def _load_cache(self) -> bool:
        """Load cache from disk with enhanced error handling and recovery"""
        try:
            if not self.cache_path.exists():
                self.logger.info("No cache file found, starting with empty cache")
                return True

            file_size = self.cache_path.stat().st_size
            if file_size == 0:
                self.logger.warning("Cache file is empty")
                return True

            with open(self.cache_path, "r", encoding="utf-8") as f:
                raw_data = json.load(f)

            now = time.time()
            valid_items: Dict[str, Dict[str, Any]] = {}
            loaded_count = 0

            # Handle different cache file formats
            if isinstance(raw_data, dict) and "metadata" in raw_data and "data" in raw_data:
                # New format with metadata
                for item in raw_data["data"]:
                    if isinstance(item, dict) and "ticker" in item and self._is_valid(item, now):
                        valid_items[item["ticker"]] = item
                        loaded_count += 1
            elif isinstance(raw_data, dict):
                # Old format or mixed
                for key, item in raw_data.items():
                    if isinstance(item, dict) and self._is_valid(item, now):
                        valid_items[key] = item
                        loaded_count += 1

            self.data = valid_items
            self.logger.info(f"✅ Cache loaded with {loaded_count} valid items from {len(raw_data) if isinstance(raw_data, dict) else 'unknown'} total")
            return True
            
        except json.JSONDecodeError as e:
            self.logger.error(f"❌ Cache file corrupted: {e}")
            self.metrics["errors"] += 1
            self._backup_corrupted_cache()
            return False
        except Exception as e:
            self.logger.error(f"❌ Failed to load cache: {e}")
            self.metrics["errors"] += 1
            return False

    def _backup_corrupted_cache(self):
        """Backup corrupted cache file for debugging"""
        try:
            if self.cache_path.exists():
                backup_name = f"corrupted_cache_{datetime.datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
                backup_path = self.backup_dir / backup_name
                self.cache_path.rename(backup_path)
                self.logger.warning(f"📦 Corrupted cache backed up to: {backup_path}")
        except Exception as e:
            self.logger.error(f"Failed to backup corrupted cache: {e}")

    def save_cache(self) -> bool:
        """Save cache to disk with atomic write and compression"""
        try:
            # Enforce size limit
            if len(self.data) > self.max_size:
                self._enforce_size_limit()
                
            cache_data = {
                "metadata": {
                    "version": "1.2",
                    "saved_at": datetime.datetime.utcnow().isoformat() + "Z",
                    "item_count": len(self.data),
                    "ttl_minutes": self.ttl // 60,
                    "total_hits": self.metrics["hits"],
                    "total_updates": self.metrics["updates"],
                },
                "data": [
                    {**item, "_cache_timestamp": item.get("_cache_timestamp", time.time())}
                    for item in self.data.values()
                ]
            }
            
            # Atomic write with temp file
            temp_path = self.cache_path.with_suffix(".tmp")
            with open(temp_path, "w", encoding="utf-8") as f:
                json.dump(cache_data, f, indent=2, ensure_ascii=False, default=str)
            
            # Replace original file
            temp_path.replace(self.cache_path)
            self.logger.debug(f"💾 Cache saved with {len(self.data)} items")
            return True
            
        except Exception as e:
            self.logger.error(f"❌ Failed to save cache: {e}")
            self.metrics["errors"] += 1
            return False

    def _enforce_size_limit(self):
        """Enforce cache size limit by removing oldest items"""
        if len(self.data) <= self.max_size:
            return
            
        items_to_remove = len(self.data) - self.max_size
        self.logger.warning(f"Cache size {len(self.data)} exceeds limit {self.max_size}, removing {items_to_remove} items")
        
        # Remove oldest items based on timestamp
        sorted_items = sorted(
            self.data.items(), 
            key=lambda x: x[1].get("_cache_timestamp", 0)
        )
        
        for ticker, _ in sorted_items[:items_to_remove]:
            del self.data[ticker]
            self.metrics["expired_removals"] += 1

    def create_backup(self) -> Optional[str]:
        """Create cache backup with timestamp and compression"""
        try:
            if not self.data:
                self.logger.warning("No data to backup")
                return None
                
            timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
            backup_path = self.backup_dir / f"quote_cache_backup_{timestamp}.json"
            
            backup_data = {
                "metadata": {
                    "backup_created": datetime.datetime.utcnow().isoformat() + "Z",
                    "original_item_count": len(self.data),
                    "api_version": config.service_version,
                },
                "data": list(self.data.values())
            }
            
            with open(backup_path, "w", encoding="utf-8") as f:
                json.dump(backup_data, f, indent=2, ensure_ascii=False, default=str)
                
            self.logger.info(f"📦 Cache backup created: {backup_path}")
            return str(backup_path)
            
        except Exception as e:
            self.logger.error(f"❌ Backup creation failed: {e}")
            self.metrics["errors"] += 1
            return None

    def get_quote(self, ticker: str) -> Optional[Quote]:
        """Get quote from cache with validation and metrics"""
        ticker = ticker.upper().strip()
        
        if ticker in self.data and self._is_valid(self.data[ticker]):
            self.metrics["hits"] += 1
            # Remove cache metadata before returning
            clean_data = {k: v for k, v in self.data[ticker].items() 
                         if not k.startswith("_")}
            try:
                return Quote(**clean_data)
            except Exception as e:
                self.logger.error(f"Invalid cache data for {ticker}: {e}")
                self.metrics["errors"] += 1
                # Remove invalid entry
                del self.data[ticker]
                self.save_cache()
                return None
        
        # Remove expired entry
        if ticker in self.data:
            del self.data[ticker]
            self.save_cache()
            
        self.metrics["misses"] += 1
        return None

    def update_quotes(self, quotes: List[Quote]) -> Tuple[int, List[str]]:
        """Update multiple quotes in cache with enhanced error handling"""
        updated_count = 0
        errors: List[str] = []
        now = time.time()

        for quote in quotes:
            try:
                quote_data = quote.dict()
                quote_data["_cache_timestamp"] = now
                quote_data["_last_updated"] = datetime.datetime.utcnow().isoformat() + "Z"
                
                self.data[quote.ticker] = quote_data
                updated_count += 1
                self.metrics["updates"] += 1
                
            except Exception as e:
                error_msg = f"Failed to update {quote.ticker}: {e}"
                errors.append(error_msg)
                self.metrics["errors"] += 1

        if updated_count > 0:
            self.save_cache()
            self.logger.info(f"Updated {updated_count} quotes in cache")
            
        return updated_count, errors

    def cleanup_expired(self) -> int:
        """Clean up expired cache entries with metrics"""
        now = time.time()
        expired_tickers = [
            ticker for ticker, item in self.data.items() 
            if not self._is_valid(item, now)
        ]
        
        for ticker in expired_tickers:
            del self.data[ticker]
            self.metrics["expired_removals"] += 1
            
        if expired_tickers:
            self.save_cache()
            self.logger.info(f"🧹 Removed {len(expired_tickers)} expired cache entries")
            
        return len(expired_tickers)

    def get_stats(self) -> Dict[str, Any]:
        """Get comprehensive cache statistics"""
        now = time.time()
        valid_count = sum(1 for item in self.data.values() if self._is_valid(item, now))
        expired_count = len(self.data) - valid_count
        
        total_requests = self.metrics["hits"] + self.metrics["misses"]
        hit_rate = (self.metrics["hits"] / total_requests * 100) if total_requests > 0 else 0
        
        return {
            "total_items": len(self.data),
            "valid_items": valid_count,
            "expired_items": expired_count,
            "max_size": self.max_size,
            "ttl_minutes": self.ttl // 60,
            "cache_file": str(self.cache_path),
            "file_exists": self.cache_path.exists(),
            "file_size": self.cache_path.stat().st_size if self.cache_path.exists() else 0,
            "performance": {
                "hits": self.metrics["hits"],
                "misses": self.metrics["misses"],
                "hit_rate": round(hit_rate, 2),
                "updates": self.metrics["updates"],
                "expired_removals": self.metrics["expired_removals"],
                "errors": self.metrics["errors"],
            }
        }

    def get_hit_rate(self) -> float:
        """Calculate cache hit rate"""
        total = self.metrics["hits"] + self.metrics["misses"]
        return (self.metrics["hits"] / total * 100) if total > 0 else 0.0

# Initialize enhanced cache
cache = TTLCache()

# =============================================================================
# Enhanced Google Sheets Manager with Connection Pooling
# =============================================================================

class GoogleSheetsManager:
    """
    Enhanced Google Sheets manager with connection pooling, retry logic, and monitoring
    """
    _client: Optional[gspread.Client] = None
    _spreadsheet: Optional[gspread.Spreadsheet] = None
    _last_connection_test: Optional[datetime.datetime] = None
    _connection_status: Dict[str, Any] = {}
    _metrics: Dict[str, Any] = {
        "requests": 0,
        "errors": 0,
        "last_error": None,
        "average_response_time": 0.0,
    }

    @classmethod
    def get_client(cls) -> gspread.Client:
        """Get or create Google Sheets client with enhanced error handling and retry"""
        if cls._client:
            return cls._client

        max_retries = 3
        for attempt in range(max_retries):
            try:
                creds_json = config.google_service_account_json
                
                if not creds_json:
                    logger.warning("Google Sheets credentials not configured - running in Apps Script mode")
                    raise GoogleSheetsError(
                        "Google Sheets credentials not configured", 
                        "initialization"
                    )

                # Validate JSON credentials
                try:
                    creds_dict = json.loads(creds_json)
                except json.JSONDecodeError as e:
                    logger.error(f"Invalid JSON in Google Sheets credentials: {e}")
                    raise GoogleSheetsError(f"Invalid credentials JSON: {e}", "initialization")

                # Enhanced credential validation
                required_fields = ["type", "project_id", "private_key_id", "private_key", "client_email"]
                missing_fields = [field for field in required_fields if field not in creds_dict]
                if missing_fields:
                    logger.error(f"Missing required credential fields: {missing_fields}")
                    raise GoogleSheetsError(f"Incomplete credentials: missing {missing_fields}", "initialization")

                # Create credentials and client
                creds = Credentials.from_service_account_info(creds_dict, scopes=SCOPES)
                cls._client = gspread.authorize(creds)
                
                logger.info("✅ Google Sheets client initialized successfully")
                return cls._client

            except GoogleSheetsError:
                raise
            except Exception as e:
                logger.warning(f"Attempt {attempt + 1} failed to initialize Google Sheets client: {e}")
                if attempt < max_retries - 1:
                    wait_time = config.retry_delay * (2 ** attempt)  # Exponential backoff
                    logger.debug(f"Retrying in {wait_time:.2f}s...")
                    time.sleep(wait_time)
                else:
                    logger.error(f"All retries failed to initialize Google Sheets client: {e}")
                    raise GoogleSheetsError(f"Connection failed after {max_retries} attempts: {e}", "initialization")

    @classmethod
    def get_spreadsheet(cls) -> gspread.Spreadsheet:
        """Get spreadsheet with enhanced error handling and caching"""
        if cls._spreadsheet:
            return cls._spreadsheet

        start_time = time.time()
        try:
            client = cls.get_client()
            spreadsheet_id = config.spreadsheet_id
            cls._spreadsheet = client.open_by_key(spreadsheet_id)
            
            # Verify access and log details
            title = cls._spreadsheet.title
            worksheets = cls._spreadsheet.worksheets()
            
            cls._metrics["requests"] += 1
            response_time = time.time() - start_time
            cls._metrics["average_response_time"] = (
                cls._metrics["average_response_time"] * (cls._metrics["requests"] - 1) + response_time
            ) / cls._metrics["requests"]
            
            logger.info(f"✅ Spreadsheet accessed: '{title}' with {len(worksheets)} worksheets in {response_time:.3f}s")
            return cls._spreadsheet
            
        except gspread.SpreadsheetNotFound:
            cls._metrics["errors"] += 1
            cls._metrics["last_error"] = "Spreadsheet not found"
            logger.error(f"Spreadsheet not found: {config.spreadsheet_id}")
            raise GoogleSheetsError("Spreadsheet not found. Check SPREADSHEET_ID.", "access")
        except gspread.APIError as e:
            cls._metrics["errors"] += 1
            cls._metrics["last_error"] = str(e)
            logger.error(f"Google API error: {e}")
            raise GoogleSheetsError(f"Google API error: {e}", "api")
        except Exception as e:
            cls._metrics["errors"] += 1
            cls._metrics["last_error"] = str(e)
            logger.error(f"Unexpected error accessing spreadsheet: {e}")
            raise GoogleSheetsError(f"Spreadsheet access failed: {e}", "access")

    @classmethod
    def test_connection(cls) -> Dict[str, Any]:
        """
        Enhanced connection test with detailed diagnostics and performance metrics
        """
        start_time = time.time()
        try:
            if not config.google_service_account_json:
                result = {
                    "status": "DISABLED", 
                    "message": "Google Sheets credentials not configured",
                    "timestamp": datetime.datetime.utcnow().isoformat() + "Z",
                    "response_time_seconds": 0.0,
                }
                return result

            spreadsheet = cls.get_spreadsheet()
            worksheets = spreadsheet.worksheets()
            elapsed = time.time() - start_time

            # Enhanced sheet analysis
            sheet_info = []
            expected_sheets_found = []
            expected_sheets_missing = []
            
            for ws in worksheets:
                try:
                    records = ws.get_all_records()
                    row_count = ws.row_count
                    col_count = ws.col_count
                    
                    # Check data quality
                    data_quality = "GOOD"
                    if row_count == 0:
                        data_quality = "EMPTY"
                    elif len(records) < 2:
                        data_quality = "LOW_DATA"
                    elif any(len(record) == 0 for record in records[:5]):
                        data_quality = "INCOMPLETE"
                    
                    # Check if expected sheet
                    if ws.title in EXPECTED_SHEETS:
                        expected_sheets_found.append(ws.title)
                    
                    sheet_info.append({
                        "name": ws.title,
                        "status": "OK",
                        "row_count": row_count,
                        "col_count": col_count,
                        "record_count": len(records),
                        "first_row": ws.row_values(1) if row_count > 0 else [],
                        "data_quality": data_quality,
                    })
                except Exception as e:
                    sheet_info.append({
                        "name": ws.title, 
                        "status": "ERROR",
                        "error": str(e),
                        "data_quality": "ERROR",
                    })

            # Identify missing expected sheets
            expected_sheets_missing = [sheet for sheet in EXPECTED_SHEETS if sheet not in expected_sheets_found]
            
            connection_status = {
                "status": "SUCCESS",
                "response_time_seconds": round(elapsed, 3),
                "spreadsheet_title": spreadsheet.title,
                "spreadsheet_id": config.spreadsheet_id,
                "total_sheets": len(worksheets),
                "expected_sheets_found": expected_sheets_found,
                "expected_sheets_missing": expected_sheets_missing,
                "expected_sheets_count": len(EXPECTED_SHEETS),
                "sheets": sheet_info,
                "timestamp": datetime.datetime.utcnow().isoformat() + "Z",
                "performance": {
                    "average_response_time": round(cls._metrics["average_response_time"], 3),
                    "total_requests": cls._metrics["requests"],
                    "error_count": cls._metrics["errors"],
                }
            }
            
            cls._connection_status = connection_status
            cls._last_connection_test = datetime.datetime.now()
            
            logger.info(f"✅ Google Sheets connection test successful ({elapsed:.3f}s)")
            return connection_status
            
        except GoogleSheetsError as e:
            error_status = {
                "status": "ERROR", 
                "message": str(e),
                "spreadsheet_id": config.spreadsheet_id,
                "response_time_seconds": round(time.time() - start_time, 3),
                "timestamp": datetime.datetime.utcnow().isoformat() + "Z",
                "last_error": cls._metrics["last_error"],
            }
            logger.error(f"❌ Google Sheets connection test failed: {e}")
            return error_status

    @classmethod
    def reset_connection(cls):
        """Reset connection for re-authentication"""
        cls._client = None
        cls._spreadsheet = None
        cls._last_connection_test = None
        logger.info("🔄 Google Sheets connection reset")

    @classmethod
    def get_metrics(cls) -> Dict[str, Any]:
        """Get Google Sheets manager metrics"""
        return cls._metrics.copy()

# =============================================================================
# Enhanced Google Sheets Service Implementation
# =============================================================================

class GoogleSheetsService:
    """
    Enhanced service for reading data from Google Sheets with comprehensive error handling
    """
    
    def __init__(self, sheets_manager):
        self.sheets_manager = sheets_manager
        self.logger = logging.getLogger(__name__)
        self.metrics = {
            "reads": 0,
            "errors": 0,
            "last_read": None,
        }
        
    def _transform_ksa_tadawul_market(self, record: Dict[str, Any]) -> Dict[str, Any]:
        """Transform KSA Tadawul Market record to standardized format"""
        return {
            'ticker': record.get('Ticker', ''),
            'company_name': record.get('Company Name', ''),
            'sector': record.get('Sector', ''),
            'trading_market': record.get('Trading Market', ''),
            'current_price': self._safe_float(record.get('Current Price')),
            'daily_change': self._safe_float(record.get('Daily Change %')),
            'volume': self._safe_int(record.get('Volume')),
            'market_cap': self._safe_float(record.get('Market Cap')),
            'last_updated': record.get('Last Updated', ''),
            'currency': 'SAR',
            'country': 'SA',
        }

    def _transform_global_market_stock(self, record: Dict[str, Any]) -> Dict[str, Any]:
        """Transform Global Market Stock record to standardized format"""
        return {
            'symbol': record.get('Symbol', ''),
            'name': record.get('Name', ''),
            'exchange': record.get('Exchange', ''),
            'currency': record.get('Currency', ''),
            'price': self._safe_float(record.get('Price')),
            'change_percent': self._safe_float(record.get('Change %')),
            'market_cap': self._safe_float(record.get('Market Cap')),
            'volume': self._safe_int(record.get('Volume', 0)),
            'country': self._infer_country(record.get('Exchange', '')),
        }

    def _safe_float(self, value: Any) -> Optional[float]:
        """Safely convert value to float"""
        if value is None:
            return None
        try:
            return float(value)
        except (ValueError, TypeError):
            return None

    def _safe_int(self, value: Any) -> Optional[int]:
        """Safely convert value to int"""
        if value is None:
            return None
        try:
            return int(float(value))
        except (ValueError, TypeError):
            return None

    def _infer_country(self, exchange: str) -> str:
        """Infer country from exchange name"""
        exchange_upper = exchange.upper()
        if 'NASDAQ' in exchange_upper or 'NYSE' in exchange_upper:
            return 'US'
        elif 'LSE' in exchange_upper:
            return 'UK'
        elif 'TSE' in exchange_upper:
            return 'JP'
        elif 'HKEX' in exchange_upper:
            return 'HK'
        else:
            return ''

    def read_ksa_tadawul_market(self, limit: int = 100) -> List[Dict[str, Any]]:
        """Read KSA Tadawul Market data with enhanced error handling"""
        try:
            self.metrics["reads"] += 1
            self.metrics["last_read"] = datetime.datetime.utcnow().isoformat() + "Z"
            
            spreadsheet = self.sheets_manager.get_spreadsheet()
            worksheet = spreadsheet.worksheet("KSA Tadawul Market")
            records = worksheet.get_all_records()
            
            # Transform to standardized format
            transformed = []
            for record in records[:limit]:
                transformed.append(self._transform_ksa_tadawul_market(record))
            
            self.logger.info(f"Read {len(transformed)} records from KSA Tadawul Market")
            return transformed
            
        except Exception as e:
            self.metrics["errors"] += 1
            self.logger.error(f"Error reading KSA Tadawul Market: {e}")
            return []

    def read_global_market_stock(self, limit: int = 50) -> List[Dict[str, Any]]:
        """Read Global Market Stock data with enhanced error handling"""
        try:
            self.metrics["reads"] += 1
            self.metrics["last_read"] = datetime.datetime.utcnow().isoformat() + "Z"
            
            spreadsheet = self.sheets_manager.get_spreadsheet()
            worksheet = spreadsheet.worksheet("Global Market Stock")
            records = worksheet.get_all_records()
            
            transformed = []
            for record in records[:limit]:
                transformed.append(self._transform_global_market_stock(record))
            
            self.logger.info(f"Read {len(transformed)} records from Global Market Stock")
            return transformed
            
        except Exception as e:
            self.metrics["errors"] += 1
            self.logger.error(f"Error reading Global Market Stock: {e}")
            return []

    def read_mutual_fund(self, limit: int = 30) -> List[Dict[str, Any]]:
        """Read Mutual Fund data with enhanced error handling"""
        try:
            self.metrics["reads"] += 1
            spreadsheet = self.sheets_manager.get_spreadsheet()
            worksheet = spreadsheet.worksheet("Mutual Fund")
            records = worksheet.get_all_records()
            
            transformed = []
            for record in records[:limit]:
                transformed.append({
                    'fund_name': record.get('Fund Name', ''),
                    'nav': self._safe_float(record.get('NAV')),
                    'daily_change': self._safe_float(record.get('Daily Change')),
                    'ytd_return': self._safe_float(record.get('YTD Return')),
                    'assets': self._safe_float(record.get('Assets')),
                    'expense_ratio': self._safe_float(record.get('Expense Ratio')),
                    'category': record.get('Category', ''),
                    'risk_level': record.get('Risk Level', ''),
                })
            
            self.logger.info(f"Read {len(transformed)} records from Mutual Fund")
            return transformed
            
        except Exception as e:
            self.metrics["errors"] += 1
            self.logger.error(f"Error reading Mutual Fund: {e}")
            return []

    def read_my_portfolio(self, limit: int = 50) -> List[Dict[str, Any]]:
        """Read My Portfolio Investment data with enhanced error handling"""
        try:
            self.metrics["reads"] += 1
            spreadsheet = self.sheets_manager.get_spreadsheet()
            worksheet = spreadsheet.worksheet("My Portfolio Investment")
            records = worksheet.get_all_records()
            
            transformed = []
            for record in records[:limit]:
                transformed.append({
                    'symbol': record.get('Symbol', ''),
                    'quantity': self._safe_int(record.get('Quantity')),
                    'avg_cost': self._safe_float(record.get('Average Cost')),
                    'current_price': self._safe_float(record.get('Current Price')),
                    'market_value': self._safe_float(record.get('Market Value')),
                    'gain_loss': self._safe_float(record.get('Gain/Loss')),
                    'gain_loss_pct': self._safe_float(record.get('Gain/Loss %')),
                    'sector': record.get('Sector', ''),
                    'allocation_pct': self._safe_float(record.get('Allocation %')),
                })
            
            self.logger.info(f"Read {len(transformed)} records from My Portfolio")
            return transformed
            
        except Exception as e:
            self.metrics["errors"] += 1
            self.logger.error(f"Error reading My Portfolio: {e}")
            return []

    def read_commodities_fx(self, limit: int = 20) -> List[Dict[str, Any]]:
        """Read Commodities & FX data with enhanced error handling"""
        try:
            self.metrics["reads"] += 1
            spreadsheet = self.sheets_manager.get_spreadsheet()
            worksheet = spreadsheet.worksheet("Commodities & FX")
            records = worksheet.get_all_records()
            
            transformed = []
            for record in records[:limit]:
                transformed.append({
                    'commodity': record.get('Commodity', ''),
                    'price': self._safe_float(record.get('Price')),
                    'change': self._safe_float(record.get('Change')),
                    'change_percent': self._safe_float(record.get('Change %')),
                    'unit': record.get('Unit', ''),
                    'category': record.get('Category', ''),
                })
            
            self.logger.info(f"Read {len(transformed)} records from Commodities & FX")
            return transformed
            
        except Exception as e:
            self.metrics["errors"] += 1
            self.logger.error(f"Error reading Commodities & FX: {e}")
            return []

    def read_economic_calendar(self, limit: int = 10) -> List[Dict[str, Any]]:
        """Read Economic Calendar data with enhanced error handling"""
        try:
            self.metrics["reads"] += 1
            spreadsheet = self.sheets_manager.get_spreadsheet()
            worksheet = spreadsheet.worksheet("Economic Calendar")
            records = worksheet.get_all_records()
            
            transformed = []
            for record in records[:limit]:
                transformed.append({
                    'date': record.get('Date', ''),
                    'time': record.get('Time', ''),
                    'currency': record.get('Currency', ''),
                    'event': record.get('Event', ''),
                    'importance': record.get('Importance', ''),
                    'actual': record.get('Actual', ''),
                    'forecast': record.get('Forecast', ''),
                    'previous': record.get('Previous', ''),
                    'impact': record.get('Impact', ''),
                })
            
            self.logger.info(f"Read {len(transformed)} records from Economic Calendar")
            return transformed
            
        except Exception as e:
            self.metrics["errors"] += 1
            self.logger.error(f"Error reading Economic Calendar: {e}")
            return []

    def get_metrics(self) -> Dict[str, Any]:
        """Get service metrics"""
        return self.metrics.copy()

# Initialize Google Sheets Service
google_sheets_service = GoogleSheetsService(GoogleSheetsManager)

# =============================================================================
# Enhanced Financial Data Client with Retry Logic
# =============================================================================

class FinancialDataClient:
    """
    Enhanced financial data client with comprehensive error handling, retry logic, and monitoring
    """
    
    def __init__(self):
        self.apis = FINANCIAL_APIS
        self.session: Optional[aiohttp.ClientSession] = None
        self.logger = logging.getLogger(__name__)
        self.metrics = {
            "requests": 0,
            "errors": 0,
            "successful_requests": 0,
            "total_response_time": 0.0,
            "api_usage": {api: 0 for api in FINANCIAL_APIS},
        }

    async def get_session(self) -> aiohttp.ClientSession:
        """Get or create aiohttp session with enhanced connection pooling"""
        if self.session is None:
            timeout = aiohttp.ClientTimeout(total=config.http_timeout)
            connector = aiohttp.TCPConnector(
                limit=100, 
                limit_per_host=20, 
                verify_ssl=True,
                keepalive_timeout=30,
            )
            self.session = aiohttp.ClientSession(
                timeout=timeout, 
                connector=connector,
                headers={
                    'User-Agent': f'{config.service_name}/{config.service_version}',
                    'Accept': 'application/json',
                }
            )
        return self.session

    async def close(self):
        """Close session gracefully"""
        if self.session:
            await self.session.close()
            self.session = None

    def get_api_status(self) -> Dict[str, Any]:
        """Get enhanced status of all configured APIs"""
        status = {}
        for name, cfg in self.apis.items():
            status[name] = {
                "configured": bool(cfg.get("api_key")),
                "enabled": cfg.get("enabled", False),
                "requests": self.metrics["api_usage"].get(name, 0),
                "rate_limit": cfg.get("rate_limit", 0),
            }
        return status

    async def get_stock_quote_with_retry(
        self, 
        symbol: str, 
        api_name: str = "alpha_vantage", 
        max_retries: int = None,
        retry_delay: float = None,
    ) -> Optional[Dict[str, Any]]:
        """
        Enhanced retry logic with exponential backoff and circuit breaker pattern
        """
        max_retries = max_retries or config.max_retries
        retry_delay = retry_delay or config.retry_delay
        
        for attempt in range(max_retries):
            try:
                start_time = time.time()
                self.logger.debug(f"Fetching {symbol} from {api_name} (attempt {attempt + 1})")
                
                result = await self.get_stock_quote(symbol, api_name)
                response_time = time.time() - start_time
                
                self.metrics["total_response_time"] += response_time
                self.metrics["requests"] += 1
                
                if result:
                    self.metrics["successful_requests"] += 1
                    self.metrics["api_usage"][api_name] = self.metrics["api_usage"].get(api_name, 0) + 1
                    self.logger.info(f"✅ Successfully fetched {symbol} from {api_name} in {response_time:.3f}s")
                    return result
                
                self.logger.warning(f"⚠️ No data returned for {symbol} from {api_name}")
                
            except Exception as e:
                self.metrics["errors"] += 1
                self.logger.warning(f"⚠️ Attempt {attempt + 1} failed for {api_name}: {e}")
                
                if attempt < max_retries - 1:
                    wait_time = retry_delay * (2 ** attempt)  # Exponential backoff
                    self.logger.debug(f"Retrying in {wait_time:.2f}s...")
                    await asyncio.sleep(wait_time)
                else:
                    self.logger.error(f"❌ All retries failed for {symbol} from {api_name}")
        
        return None

    async def get_stock_quote(self, symbol: str, api_name: str = "alpha_vantage") -> Optional[Dict[str, Any]]:
        """Get stock quote from specified API with comprehensive error handling"""
        if api_name not in self.apis:
            self.logger.error(f"API {api_name} not configured")
            raise FinancialAPIError(f"API {api_name} not configured", api_name)

        api_cfg = self.apis[api_name]
        if not api_cfg.get("api_key"):
            self.logger.warning(f"API key not configured for {api_name}")
            return None

        if not api_cfg.get("enabled", True):
            self.logger.debug(f"API {api_name} is disabled")
            return None

        try:
            session = await self.get_session()
            timeout = aiohttp.ClientTimeout(total=api_cfg["timeout"])

            if api_name == "alpha_vantage":
                params = {
                    "function": "GLOBAL_QUOTE",
                    "symbol": symbol,
                    "apikey": api_cfg["api_key"],
                }
                async with session.get(api_cfg["base_url"], params=params, timeout=timeout) as response:
                    return await self._handle_alpha_vantage_response(response, symbol)

            elif api_name == "finnhub":
                params = {"symbol": symbol, "token": api_cfg["api_key"]}
                async with session.get(f"{api_cfg['base_url']}/quote", params=params, timeout=timeout) as response:
                    return await self._handle_finnhub_response(response, symbol)

            elif api_name == "twelvedata":
                params = {
                    "symbol": symbol, 
                    "apikey": api_cfg["api_key"],
                    "source": "docs"
                }
                async with session.get(f"{api_cfg['base_url']}/quote", params=params, timeout=timeout) as response:
                    return await self._handle_twelvedata_response(response, symbol)

            elif api_name == "marketstack":
                params = {
                    "access_key": api_cfg["api_key"],
                    "symbols": symbol,
                    "limit": 1
                }
                async with session.get(f"{api_cfg['base_url']}/eod/latest", params=params, timeout=timeout) as response:
                    return await self._handle_marketstack_response(response, symbol)

            elif api_name == "fmp":
                params = {
                    "apikey": api_cfg["api_key"],
                    "limit": 1
                }
                async with session.get(f"{api_cfg['base_url']}/quote/{symbol}", params=params, timeout=timeout) as response:
                    return await self._handle_fmp_response(response, symbol)

        except asyncio.TimeoutError:
            self.logger.error(f"Timeout fetching data from {api_name} for {symbol}")
            raise FinancialAPIError(f"Timeout from {api_name}", api_name, 408)
        except aiohttp.ClientError as e:
            self.logger.error(f"Network error fetching from {api_name}: {e}")
            raise FinancialAPIError(f"Network error: {e}", api_name, 503)
        except Exception as e:
            self.logger.error(f"Unexpected error from {api_name}: {e}")
            raise FinancialAPIError(f"Unexpected error: {e}", api_name, 500)

        return None

    async def _handle_alpha_vantage_response(self, response: aiohttp.ClientResponse, symbol: str) -> Optional[Dict[str, Any]]:
        """Handle Alpha Vantage API response"""
        if response.status == 200:
            data = await response.json()
            parsed_data = self._parse_alpha_vantage_quote(data)
            if parsed_data:
                parsed_data["symbol"] = symbol
            return parsed_data
        else:
            error_text = await response.text()
            self.logger.error(f"Alpha Vantage HTTP {response.status}: {error_text}")
            raise FinancialAPIError(f"HTTP {response.status}: {error_text}", "alpha_vantage", response.status)

    async def _handle_finnhub_response(self, response: aiohttp.ClientResponse, symbol: str) -> Optional[Dict[str, Any]]:
        """Handle Finnhub API response"""
        if response.status == 200:
            data = await response.json()
            parsed_data = self._parse_finnhub_quote(data)
            if parsed_data:
                parsed_data["symbol"] = symbol
            return parsed_data
        else:
            error_text = await response.text()
            self.logger.error(f"Finnhub HTTP {response.status}: {error_text}")
            raise FinancialAPIError(f"HTTP {response.status}: {error_text}", "finnhub", response.status)

    async def _handle_twelvedata_response(self, response: aiohttp.ClientResponse, symbol: str) -> Optional[Dict[str, Any]]:
        """Handle TwelveData API response"""
        if response.status == 200:
            data = await response.json()
            parsed_data = self._parse_twelvedata_quote(data)
            if parsed_data:
                parsed_data["symbol"] = symbol
            return parsed_data
        else:
            error_text = await response.text()
            self.logger.error(f"TwelveData HTTP {response.status}: {error_text}")
            raise FinancialAPIError(f"HTTP {response.status}: {error_text}", "twelvedata", response.status)

    async def _handle_marketstack_response(self, response: aiohttp.ClientResponse, symbol: str) -> Optional[Dict[str, Any]]:
        """Handle Marketstack API response"""
        if response.status == 200:
            data = await response.json()
            return self._parse_marketstack_quote(data, symbol)
        else:
            error_text = await response.text()
            self.logger.error(f"Marketstack HTTP {response.status}: {error_text}")
            raise FinancialAPIError(f"HTTP {response.status}: {error_text}", "marketstack", response.status)

    async def _handle_fmp_response(self, response: aiohttp.ClientResponse, symbol: str) -> Optional[Dict[str, Any]]:
        """Handle FMP API response"""
        if response.status == 200:
            data = await response.json()
            return self._parse_fmp_quote(data, symbol)
        else:
            error_text = await response.text()
            self.logger.error(f"FMP HTTP {response.status}: {error_text}")
            raise FinancialAPIError(f"HTTP {response.status}: {error_text}", "fmp", response.status)

    def _parse_alpha_vantage_quote(self, data: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Parse Alpha Vantage response with enhanced validation"""
        try:
            quote_data = data.get("Global Quote", {})
            if not quote_data:
                # Check for error message
                if "Error Message" in data:
                    self.logger.warning(f"Alpha Vantage error: {data['Error Message']}")
                elif "Note" in data:
                    self.logger.warning(f"Alpha Vantage rate limit: {data['Note']}")
                self.logger.debug("Alpha Vantage returned empty Global Quote")
                return None
                
            return {
                "price": self._safe_float(quote_data.get("05. price")),
                "change": self._safe_float(quote_data.get("09. change")),
                "change_percent": self._safe_float(str(quote_data.get("10. change percent", "0")).rstrip("%")),
                "volume": self._safe_int(quote_data.get("06. volume")),
                "timestamp": quote_data.get("07. latest trading day"),
                "previous_close": self._safe_float(quote_data.get("08. previous close")),
                "open": self._safe_float(quote_data.get("02. open")),
                "high": self._safe_float(quote_data.get("03. high")),
                "low": self._safe_float(quote_data.get("04. low")),
                "data_source": "alpha_vantage",
            }
        except (ValueError, TypeError) as e:
            self.logger.error(f"Error parsing Alpha Vantage data: {e}")
            return None

    def _parse_finnhub_quote(self, data: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Parse Finnhub response with enhanced validation"""
        try:
            if not data or "c" not in data:
                self.logger.debug("Finnhub returned incomplete data")
                return None
                
            timestamp = None
            if data.get("t"):
                timestamp = datetime.datetime.fromtimestamp(data["t"]).isoformat()
                
            return {
                "price": data.get("c", 0),
                "change": data.get("d", 0),
                "change_percent": data.get("dp", 0),
                "high": data.get("h", 0),
                "low": data.get("l", 0),
                "open": data.get("o", 0),
                "previous_close": data.get("pc", 0),
                "timestamp": timestamp,
                "data_source": "finnhub",
            }
        except (ValueError, TypeError) as e:
            self.logger.error(f"Error parsing Finnhub data: {e}")
            return None

    def _parse_twelvedata_quote(self, data: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Parse TwelveData response with enhanced validation"""
        try:
            if data.get("status") != "ok":
                error_message = data.get('message', 'Unknown error')
                self.logger.debug(f"TwelveData returned error status: {data.get('status')} - {error_message}")
                return None
                
            return {
                "price": self._safe_float(data.get("close")),
                "change": self._safe_float(data.get("change")),
                "change_percent": self._safe_float(data.get("percent_change")),
                "high": self._safe_float(data.get("high")),
                "low": self._safe_float(data.get("low")),
                "open": self._safe_float(data.get("open")),
                "volume": self._safe_int(data.get("volume")),
                "timestamp": data.get("datetime"),
                "data_source": "twelvedata",
            }
        except (ValueError, TypeError) as e:
            self.logger.error(f"Error parsing TwelveData: {e}")
            return None

    def _parse_marketstack_quote(self, data: Dict[str, Any], symbol: str) -> Optional[Dict[str, Any]]:
        """Parse Marketstack response"""
        try:
            if "data" not in data or not data["data"]:
                self.logger.debug("Marketstack returned no data")
                return None
                
            quote_data = data["data"][0]
            return {
                "symbol": symbol,
                "price": self._safe_float(quote_data.get("close")),
                "change": self._safe_float(quote_data.get("change")),
                "change_percent": self._safe_float(quote_data.get("change_percent")),
                "volume": self._safe_int(quote_data.get("volume")),
                "timestamp": quote_data.get("date"),
                "data_source": "marketstack",
            }
        except (ValueError, TypeError, KeyError) as e:
            self.logger.error(f"Error parsing Marketstack data: {e}")
            return None

    def _parse_fmp_quote(self, data: Dict[str, Any], symbol: str) -> Optional[Dict[str, Any]]:
        """Parse FMP response"""
        try:
            if not data or not isinstance(data, list) or len(data) == 0:
                self.logger.debug("FMP returned no data")
                return None
                
            quote_data = data[0]
            return {
                "symbol": symbol,
                "price": self._safe_float(quote_data.get("price")),
                "change": self._safe_float(quote_data.get("change")),
                "change_percent": self._safe_float(quote_data.get("changesPercentage")),
                "volume": self._safe_int(quote_data.get("volume")),
                "timestamp": datetime.datetime.utcnow().isoformat() + "Z",
                "data_source": "fmp",
            }
        except (ValueError, TypeError, KeyError) as e:
            self.logger.error(f"Error parsing FMP data: {e}")
            return None

    def _safe_float(self, value: Any) -> Optional[float]:
        """Safely convert value to float"""
        if value is None:
            return None
        try:
            return float(value)
        except (ValueError, TypeError):
            return None

    def _safe_int(self, value: Any) -> Optional[int]:
        """Safely convert value to int"""
        if value is None:
            return None
        try:
            return int(float(value))
        except (ValueError, TypeError):
            return None

    def get_metrics(self) -> Dict[str, Any]:
        """Get client metrics"""
        avg_response_time = 0.0
        if self.metrics["requests"] > 0:
            avg_response_time = self.metrics["total_response_time"] / self.metrics["requests"]
            
        success_rate = 0.0
        if self.metrics["requests"] > 0:
            success_rate = (self.metrics["successful_requests"] / self.metrics["requests"]) * 100

        return {
            "total_requests": self.metrics["requests"],
            "successful_requests": self.metrics["successful_requests"],
            "errors": self.metrics["errors"],
            "success_rate": round(success_rate, 2),
            "average_response_time": round(avg_response_time, 3),
            "api_usage": self.metrics["api_usage"],
        }

# Initialize enhanced financial client
financial_client = FinancialDataClient()

# =============================================================================
# Enhanced Application Startup and Lifespan
# =============================================================================

# Global application state
app_start_time = time.time()
app_metrics = {
    "startup_time": app_start_time,
    "total_requests": 0,
    "successful_requests": 0,
    "failed_requests": 0,
    "active_connections": 0,
}

async def validate_configuration():
    """
    Enhanced configuration validation with comprehensive diagnostics
    """
    errors: List[str] = []
    warnings: List[str] = []

    # Required environment variables
    required_vars = ["SPREADSHEET_ID"]
    for var in required_vars:
        if not os.getenv(var):
            errors.append(f"Missing required environment variable: {var}")

    # Google Sheets configuration check
    has_secure_creds = bool(config.google_sheets_credentials)
    has_legacy_creds = bool(os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON"))
    
    if has_secure_creds:
        logger.info("🔐 Using secure GOOGLE_SHEETS_CREDENTIALS")
    elif has_legacy_creds:
        warnings.append("Using legacy GOOGLE_SERVICE_ACCOUNT_JSON - migrate to GOOGLE_SHEETS_CREDENTIALS")
        logger.warning("⚠️ Using legacy Google Sheets credentials")
    else:
        logger.warning("No Google Sheets credentials - running in Apps Script mode")

    # Test Google Sheets connection if credentials available
    if has_secure_creds or has_legacy_creds:
        try:
            sheets_status = GoogleSheetsManager.test_connection()
            status_value = sheets_status.get("status")
            
            if status_value == "SUCCESS":
                logger.info("✅ Google Sheets direct connection verified")
                
                # Log detailed sheet information
                found_count = len(sheets_status.get("expected_sheets_found", []))
                total_expected = len(EXPECTED_SHEETS)
                missing_sheets = sheets_status.get("expected_sheets_missing", [])
                
                logger.info(f"📊 Sheets Status: {found_count}/{total_expected} expected sheets found")
                
                if missing_sheets:
                    warnings.append(f"Missing expected sheets: {missing_sheets}")
                    
            elif status_value == "DISABLED":
                logger.info("ℹ️ Google Sheets direct access disabled")
            else:
                errors.append(f"Google Sheets connection failed: {sheets_status.get('message')}")
                
        except Exception as e:
            errors.append(f"Google Sheets validation error: {e}")

    # Financial APIs status (optional)
    api_status = financial_client.get_api_status()
    configured_apis = [name for name, status in api_status.items() if status["configured"]]
    
    if configured_apis:
        logger.info(f"✅ Financial APIs configured: {', '.join(configured_apis)}")
    else:
        warnings.append("No financial APIs configured - limited functionality")

    # Authentication configuration
    if config.require_auth:
        valid_tokens = [t for t in [config.app_token, config.backup_app_token] if t]
        if not valid_tokens:
            warnings.append("Authentication required but no tokens configured")
        else:
            logger.info("🔐 Authentication enabled with token validation")

    # Cache validation
    try:
        cache_stats = cache.get_stats()
        logger.info(f"💾 Cache initialized: {cache_stats['valid_items']} valid items")
    except Exception as e:
        errors.append(f"Cache initialization failed: {e}")

    # Report results
    if warnings:
        logger.warning("Configuration warnings:")
        for warning in warnings:
            logger.warning(f"  ⚠️ {warning}")

    if errors:
        logger.error("Configuration validation failed:")
        for error in errors:
            logger.error(f"  ❌ {error}")
            
        if config.environment == "production":
            raise ConfigurationError("Production configuration invalid")
        else:
            logger.warning("⚠️ Continuing with configuration errors in non-production environment")
    else:
        logger.info("✅ All configuration validations passed")

@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    Enhanced application lifespan with comprehensive resource management
    """
    startup_time = datetime.datetime.utcnow()
    logger.info(f"🚀 Starting {config.service_name} v{config.service_version}")
    
    try:
        # Validate configuration
        await validate_configuration()
        
        # Initialize cache
        expired_count = cache.cleanup_expired()
        if expired_count:
            logger.info(f"🧹 Cleaned {expired_count} expired cache entries on startup")
            
        # Create cache backup if enabled
        if config.cache_backup_enabled:
            backup_path = cache.create_backup()
            if backup_path:
                logger.info(f"📦 Initial cache backup created: {backup_path}")
            
        logger.info("✅ Application startup completed successfully")
        
    except Exception as e:
        logger.error(f"❌ Application startup failed: {e}")
        if config.environment == "production":
            raise
        else:
            logger.warning("⚠️ Continuing despite startup errors in development")

    yield

    # Shutdown cleanup
    shutdown_time = datetime.datetime.utcnow()
    uptime = shutdown_time - startup_time
    
    try:
        await financial_client.close()
        logger.info("✅ Financial client closed successfully")
    except Exception as e:
        logger.warning(f"⚠️ Error closing financial client: {e}")

    # Final cache backup
    if config.cache_backup_enabled:
        backup_path = cache.create_backup()
        if backup_path:
            logger.info(f"📦 Final cache backup created: {backup_path}")

    logger.info(f"🛑 Application shutdown after {uptime}")

# =============================================================================
# FastAPI Application Setup with Enhanced Features
# =============================================================================

# Create FastAPI application
app = FastAPI(
    title=config.service_name,
    version=config.service_version,
    description="""
    Enhanced Tadawul Stock Analysis API with comprehensive financial data integration.
    
    ## Features
    
    * 📈 Real-time stock quotes from multiple financial APIs
    * 📊 Google Sheets integration for portfolio management
    * 💾 Advanced caching with TTL and persistence
    * 🔐 Comprehensive security and rate limiting
    * 📱 Async/await for high performance
    * 🏥 Health monitoring and metrics
    
    ## Authentication
    
    Most endpoints require authentication via Bearer token.
    """,
    docs_url="/docs" if config.enable_swagger else None,
    redoc_url="/redoc" if config.enable_redoc else None,
    lifespan=lifespan,
    contact={
        "name": "API Support",
        "email": "support@example.com",
    },
    license_info={
        "name": "MIT",
        "url": "https://opensource.org/licenses/MIT",
    },
)

# Configure rate limiting
app.state.limiter = limiter
app.add_exception_handler(RateLimitExceeded, _rate_limit_exceeded_handler)

# Configure CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=config.cors_origins,
    allow_credentials=True,
    allow_methods=["GET", "POST", "PUT", "DELETE", "OPTIONS"],
    allow_headers=["*"],
)

# =============================================================================
# Enhanced Exception Handlers
# =============================================================================

@app.exception_handler(HTTPException)
async def http_exception_handler(request: Request, exc: HTTPException):
    """Enhanced HTTP exception handler with comprehensive logging"""
    request_id = request.headers.get("X-Request-ID", "unknown")
    client_ip = get_remote_address(request)
    
    app_metrics["failed_requests"] += 1
    
    logger.warning(
        f"HTTP {exc.status_code} for {request.method} {request.url.path} "
        f"from {client_ip} [{request_id}]: {exc.detail}"
    )
    
    return JSONResponse(
        status_code=exc.status_code,
        content=ErrorResponse(
            error=exc.detail,
            detail=exc.detail,
            timestamp=datetime.datetime.utcnow().isoformat() + "Z",
            request_id=request_id,
            code=f"HTTP_{exc.status_code}",
        ).dict(),
    )

@app.exception_handler(Exception)
async def global_exception_handler(request: Request, exc: Exception):
    """Enhanced global exception handler with security considerations"""
    request_id = request.headers.get("X-Request-ID", "unknown")
    client_ip = get_remote_address(request)
    
    app_metrics["failed_requests"] += 1
    
    # Don't expose internal errors in production
    error_detail = str(exc) if config.environment != "production" else "Internal server error"
    
    logger.error(
        f"Unhandled exception for {request.method} {request.url.path} "
        f"from {client_ip} [{request_id}]: {exc}",
        exc_info=config.debug
    )
    
    return JSONResponse(
        status_code=500,
        content=ErrorResponse(
            error="Internal server error",
            detail=error_detail,
            timestamp=datetime.datetime.utcnow().isoformat() + "Z",
            request_id=request_id,
            code="INTERNAL_ERROR",
            suggestion="Please contact support if this error persists",
        ).dict(),
    )

@app.exception_handler(RateLimitExceeded)
async def rate_limit_exception_handler(request: Request, exc: RateLimitExceeded):
    """Enhanced rate limit exception handler"""
    request_id = request.headers.get("X-Request-ID", "unknown")
    client_ip = get_remote_address(request)
    
    logger.warning(
        f"Rate limit exceeded for {client_ip} [{request_id}]: {request.method} {request.url.path}"
    )
    
    return JSONResponse(
        status_code=429,
        content=ErrorResponse(
            error="Rate limit exceeded",
            detail="Too many requests. Please try again later.",
            timestamp=datetime.datetime.utcnow().isoformat() + "Z",
            request_id=request_id,
            code="RATE_LIMIT_EXCEEDED",
            suggestion=f"Limit is {config.max_requests_per_minute} requests per minute",
        ).dict(),
    )

# =============================================================================
# Enhanced Core API Endpoints
# =============================================================================

@app.get("/", response_model=Dict[str, Any])
@rate_limit(f"{config.max_requests_per_minute}/minute")
async def root(request: Request):
    """
    Enhanced root endpoint with comprehensive system status and documentation
    """
    app_metrics["total_requests"] += 1
    app_metrics["successful_requests"] += 1
    
    # Check Google Sheets connection status
    has_sheets_creds = bool(config.google_service_account_json)
    sheets_connected = False
    spreadsheet_title = None
    total_sheets = 0
    
    if has_sheets_creds:
        try:
            sheets_status = GoogleSheetsManager.test_connection()
            sheets_connected = sheets_status.get("status") == "SUCCESS"
            spreadsheet_title = sheets_status.get("spreadsheet_title")
            total_sheets = sheets_status.get("total_sheets", 0)
        except Exception as e:
            logger.warning(f"Google Sheets status check failed: {e}")

    # Get API status
    api_status = financial_client.get_api_status()
    
    # Get cache statistics
    cache_stats = cache.get_stats()

    # Calculate uptime
    uptime_seconds = time.time() - app_metrics["startup_time"]

    return {
        "service": config.service_name,
        "version": config.service_version,
        "status": "operational",
        "timestamp": datetime.datetime.utcnow().isoformat() + "Z",
        "environment": config.environment,
        "uptime_seconds": round(uptime_seconds, 2),
        "authentication_required": config.require_auth,
        "rate_limiting": config.enable_rate_limiting,
        "performance": {
            "total_requests": app_metrics["total_requests"],
            "successful_requests": app_metrics["successful_requests"],
            "failed_requests": app_metrics["failed_requests"],
            "success_rate": round((app_metrics["successful_requests"] / app_metrics["total_requests"]) * 100, 2) if app_metrics["total_requests"] > 0 else 100.0,
        },
        "google_sheets": {
            "connected": sheets_connected,
            "mode": "direct_gspread" if has_sheets_creds else "apps_script_only",
            "spreadsheet": spreadsheet_title,
            "total_sheets": total_sheets,
        },
        "financial_apis": {
            name: {
                "status": "configured" if status["configured"] else "not_configured",
                "enabled": status["enabled"],
                "requests": status["requests"],
            }
            for name, status in api_status.items()
        },
        "cache": {
            "total_items": cache_stats["total_items"],
            "valid_items": cache_stats["valid_items"],
            "expired_items": cache_stats["expired_items"],
            "file_exists": cache_stats["file_exists"],
            "ttl_minutes": cache_stats["ttl_minutes"],
            "hit_rate": cache_stats["performance"]["hit_rate"],
        },
        "endpoints": {
            "health": "/health",
            "docs": "/docs" if config.enable_swagger else None,
            "quotes": "/v1/quote",
            "sheets": "/sheet-names",
            "metrics": "/metrics",
            "cache_info": "/v1/cache",
        },
        "documentation": "Visit /docs for full API documentation",
    }

@app.get("/health", response_model=HealthResponse)
@rate_limit("30/minute")
async def health_check(request: Request):
    """
    Enhanced health check endpoint with comprehensive system diagnostics
    """
    app_metrics["total_requests"] += 1
    
    # Check Google Sheets connection
    has_sheets_creds = bool(config.google_service_account_json)
    google_connected = False
    sheets_details = {}
    
    if has_sheets_creds:
        try:
            sheets_status = GoogleSheetsManager.test_connection()
            google_connected = sheets_status.get("status") == "SUCCESS"
            sheets_details = {
                "response_time": sheets_status.get("response_time_seconds", 0),
                "total_sheets": sheets_status.get("total_sheets", 0),
                "expected_sheets_found": len(sheets_status.get("expected_sheets_found", [])),
            }
        except Exception as e:
            logger.warning(f"Google Sheets health check failed: {e}")
            health_status = "degraded"
    else:
        health_status = "healthy"  # Apps Script mode is considered healthy

    # Determine overall health status
    if google_connected or not has_sheets_creds:
        health_status = "healthy"
    else:
        health_status = "degraded"

    # Get API status
    api_status = financial_client.get_api_status()
    
    # Get cache statistics
    cache_stats = cache.get_stats()

    # Calculate performance metrics
    uptime_seconds = time.time() - app_metrics["startup_time"]
    success_rate = 0.0
    if app_metrics["total_requests"] > 0:
        success_rate = (app_metrics["successful_requests"] / app_metrics["total_requests"]) * 100

    # Check feature availability
    features = {
        "dashboard": False,  # Placeholder for dashboard feature
        "argaam_routes": False,  # Placeholder for Argaam routes
        "google_apps_script": bool(config.google_apps_script_url),
        "direct_sheets_access": has_sheets_creds,
        "caching": True,
        "authentication": config.require_auth,
    }

    # Get financial client metrics
    financial_metrics = financial_client.get_metrics()

    app_metrics["successful_requests"] += 1

    return HealthResponse(
        status=health_status,
        time_utc=datetime.datetime.utcnow().isoformat() + "Z",
        version=config.service_version,
        uptime_seconds=round(uptime_seconds, 2),
        google_sheets_connected=google_connected,
        cache_status={
            "total_items": cache_stats["total_items"],
            "valid_items": cache_stats["valid_items"],
            "file_exists": cache_stats["file_exists"],
            "file_size_bytes": cache_stats["file_size"],
            "ttl_minutes": cache_stats["ttl_minutes"],
            "hit_rate": cache_stats["performance"]["hit_rate"],
            "performance": cache_stats["performance"],
        },
        features=features,
        api_status=api_status,
        performance={
            "total_requests": app_metrics["total_requests"],
            "success_rate": round(success_rate, 2),
            "average_response_time": financial_metrics["average_response_time"],
            "cache_hit_rate": cache_stats["performance"]["hit_rate"],
            "active_connections": app_metrics["active_connections"],
        },
    )

@app.get("/metrics", response_model=Dict[str, Any])
@rate_limit("10/minute")
async def get_metrics(request: Request, auth: bool = Depends(verify_auth)):
    """
    Get comprehensive application metrics
    """
    # Get all metrics
    cache_stats = cache.get_stats()
    financial_metrics = financial_client.get_metrics()
    sheets_metrics = GoogleSheetsManager.get_metrics()
    sheets_service_metrics = google_sheets_service.get_metrics()

    # Calculate rates
    success_rate = 0.0
    if app_metrics["total_requests"] > 0:
        success_rate = (app_metrics["successful_requests"] / app_metrics["total_requests"]) * 100

    uptime_seconds = time.time() - app_metrics["startup_time"]

    return {
        "application": {
            "uptime_seconds": round(uptime_seconds, 2),
            "total_requests": app_metrics["total_requests"],
            "successful_requests": app_metrics["successful_requests"],
            "failed_requests": app_metrics["failed_requests"],
            "success_rate": round(success_rate, 2),
            "active_connections": app_metrics["active_connections"],
        },
        "cache": cache_stats,
        "financial_apis": financial_metrics,
        "google_sheets": {
            "manager": sheets_metrics,
            "service": sheets_service_metrics,
        },
        "timestamp": datetime.datetime.utcnow().isoformat() + "Z",
    }

@app.get("/v1/ping", response_model=Dict[str, Any])
@rate_limit("120/minute")
async def ping(request: Request):
    """
    Simple ping endpoint for connectivity testing and latency measurement
    """
    start_time = time.time()
    app_metrics["total_requests"] += 1
    
    response = {
        "status": "ok",
        "service": config.service_name,
        "version": config.service_version,
        "timestamp": datetime.datetime.utcnow().isoformat() + "Z",
        "environment": config.environment,
        "response_time_ms": 0,
    }
    
    response_time = (time.time() - start_time) * 1000  # Convert to milliseconds
    response["response_time_ms"] = round(response_time, 2)
    
    app_metrics["successful_requests"] += 1
    
    return response

# =============================================================================
# Enhanced Protected API Endpoints
# =============================================================================

@app.get("/verify-sheets", response_model=VerificationResponse)
@rate_limit("10/minute")
async def verify_all_sheets(request: Request, auth: bool = Depends(verify_auth)):
    """
    Enhanced sheet verification with comprehensive diagnostics
    """
    if not config.google_service_account_json:
        raise HTTPException(
            status_code=400,
            detail="Google Sheets credentials not configured",
        )

    try:
        spreadsheet = GoogleSheetsManager.get_spreadsheet()
        results: List[SheetVerificationResult] = []
        
        for sheet_name in EXPECTED_SHEETS:
            try:
                worksheet = spreadsheet.worksheet(sheet_name)
                records = worksheet.get_all_records()
                headers = worksheet.row_values(1) if worksheet.row_count >= 1 else []
                
                # Assess data quality
                data_quality = "GOOD"
                if worksheet.row_count == 0:
                    data_quality = "EMPTY"
                elif len(records) < 2:
                    data_quality = "LOW_DATA"
                elif any(len(record) == 0 for record in records[:5]):
                    data_quality = "INCOMPLETE"
                elif any(None in record.values() for record in records[:5]):
                    data_quality = "MISSING_VALUES"
                
                # Get update time if available
                last_updated = None
                for record in records[:1]:
                    if 'Last Updated' in record:
                        last_updated = record['Last Updated']
                        break
                
                results.append(SheetVerificationResult(
                    sheet_name=sheet_name,
                    status="OK",
                    row_count=worksheet.row_count,
                    column_count=worksheet.col_count,
                    headers=headers,
                    sample_data=records[:3],
                    data_quality=data_quality,
                    last_updated=last_updated,
                ))
                
            except Exception as e:
                results.append(SheetVerificationResult(
                    sheet_name=sheet_name,
                    status="ERROR",
                    row_count=0,
                    column_count=0,
                    headers=[],
                    sample_data=[],
                    error=str(e),
                    data_quality="ERROR",
                ))
        
        # Calculate overall status
        sheets_ok = sum(1 for r in results if r.status == "OK")
        if sheets_ok == len(EXPECTED_SHEETS):
            overall_status = "SUCCESS"
        elif sheets_ok >= len(EXPECTED_SHEETS) * 0.7:  # 70% threshold
            overall_status = "DEGRADED"
        else:
            overall_status = "POOR"
        
        return VerificationResponse(
            spreadsheet_id=config.spreadsheet_id,
            overall_status=overall_status,
            sheets_verified=len(EXPECTED_SHEETS),
            sheets_ok=sheets_ok,
            details=results,
            verification_timestamp=datetime.datetime.utcnow().isoformat() + "Z",
        )
        
    except Exception as e:
        logger.error(f"Sheet verification failed: {e}")
        raise HTTPException(status_code=500, detail=f"Error verifying sheets: {e}")

@app.get("/sheet/{sheet_name}", response_model=SheetDataResponse)
@rate_limit("30/minute")
async def read_sheet_data(
    request: Request,
    sheet_name: str,
    limit: int = Query(10, ge=1, le=1000),
    offset: int = Query(0, ge=0),
    auth: bool = Depends(verify_auth),
):
    """
    Enhanced sheet data reading with pagination support
    """
    if not config.google_service_account_json:
        raise HTTPException(
            status_code=400,
            detail="Google Sheets credentials not configured",
        )

    try:
        spreadsheet = GoogleSheetsManager.get_spreadsheet()
        worksheet = spreadsheet.worksheet(sheet_name)
        all_records = worksheet.get_all_records()
        headers = worksheet.row_values(1) if worksheet.row_count >= 1 else []
        
        # Apply pagination
        start_idx = offset
        end_idx = offset + limit
        paginated_records = all_records[start_idx:end_idx]
        
        # Get last updated timestamp from first record if available
        last_updated = None
        if paginated_records and 'Last Updated' in paginated_records[0]:
            last_updated = paginated_records[0]['Last Updated']
        
        return SheetDataResponse(
            sheet_name=sheet_name, 
            total_records=len(all_records), 
            data=paginated_records,
            headers=headers,
            last_updated=last_updated,
        )
        
    except gspread.WorksheetNotFound:
        raise HTTPException(status_code=404, detail=f"Sheet '{sheet_name}' not found")
    except Exception as e:
        logger.error(f"Error reading sheet {sheet_name}: {e}")
        raise HTTPException(status_code=500, detail=f"Error reading sheet: {e}")

@app.get("/sheet-names", response_model=Dict[str, Any])
@rate_limit("20/minute")
async def get_sheet_names(request: Request, auth: bool = Depends(verify_auth)):
    """
    Enhanced sheet names endpoint with detailed analysis
    """
    if not config.google_service_account_json:
        raise HTTPException(
            status_code=400,
            detail="Google Sheets credentials not configured",
        )

    try:
        spreadsheet = GoogleSheetsManager.get_spreadsheet()
        worksheets = spreadsheet.worksheets()
        available_sheets = [ws.title for ws in worksheets]
        
        # Analyze sheets
        missing_sheets = list(set(EXPECTED_SHEETS) - set(available_sheets))
        extra_sheets = list(set(available_sheets) - set(EXPECTED_SHEETS))
        
        # Get sheet sizes
        sheet_sizes = []
        for ws in worksheets:
            try:
                sheet_sizes.append({
                    "name": ws.title,
                    "rows": ws.row_count,
                    "columns": ws.col_count,
                    "is_expected": ws.title in EXPECTED_SHEETS,
                })
            except Exception as e:
                sheet_sizes.append({
                    "name": ws.title,
                    "error": str(e),
                    "is_expected": ws.title in EXPECTED_SHEETS,
                })
        
        return {
            "spreadsheet_title": spreadsheet.title,
            "available_sheets": available_sheets,
            "expected_sheets": EXPECTED_SHEETS,
            "missing_sheets": missing_sheets,
            "extra_sheets": extra_sheets,
            "total_sheets": len(available_sheets),
            "coverage_percentage": round((1 - len(missing_sheets) / len(EXPECTED_SHEETS)) * 100, 2) if EXPECTED_SHEETS else 0,
            "sheet_sizes": sheet_sizes,
        }
        
    except Exception as e:
        logger.error(f"Error getting sheet names: {e}")
        raise HTTPException(status_code=500, detail=f"Error getting sheet names: {e}")

@app.get("/test-connection", response_model=Dict[str, Any])
@rate_limit("10/minute")
async def test_connection(request: Request, auth: bool = Depends(verify_auth)):
    """
    Enhanced Google Sheets connection test
    """
    if not config.google_service_account_json:
        return {
            "status": "DISABLED",
            "message": "Google Sheets credentials not configured",
            "timestamp": datetime.datetime.utcnow().isoformat() + "Z",
        }
        
    return GoogleSheetsManager.test_connection()

# =============================================================================
# Enhanced Quote Management Endpoints
# =============================================================================

@app.get("/v1/quote", response_model=QuoteResponse)
@rate_limit("60/minute")
async def get_quotes(
    request: Request,
    tickers: str = Query(..., description="Comma-separated ticker symbols"),
    include_metadata: bool = Query(False, description="Include cache metadata"),
    auth: bool = Depends(verify_auth),
):
    """
    Enhanced quotes endpoint with cache statistics and multiple data sources
    """
    symbols = [t.strip().upper() for t in tickers.split(",") if t.strip()]
    
    if not symbols:
        raise HTTPException(status_code=400, detail="No tickers provided")
        
    if len(symbols) > 100:
        raise HTTPException(status_code=400, detail="Too many tickers (max 100)")

    quotes: List[Quote] = []
    cache_hits = 0
    sources_used = set()
    
    for symbol in symbols:
        quote = cache.get_quote(symbol)
        if quote:
            quotes.append(quote)
            cache_hits += 1
            if quote.data_source:
                sources_used.add(quote.data_source)
        else:
            # Return basic quote structure for cache misses
            quotes.append(Quote(
                ticker=symbol,
                timestamp_utc=datetime.datetime.utcnow().isoformat() + "Z",
                data_source="cache_miss"
            ))

    logger.info(f"Quote request: {len(symbols)} symbols, {cache_hits} cache hits")
    
    return QuoteResponse(
        data=quotes,
        count=len(quotes),
        timestamp_utc=datetime.datetime.utcnow().isoformat() + "Z",
        cache_hits=cache_hits,
        cache_misses=len(symbols) - cache_hits,
        sources=list(sources_used),
    )

@app.post("/v1/quote/update", response_model=Dict[str, Any])
@rate_limit("30/minute")
async def update_quotes(
    request: Request,
    payload: QuoteUpdatePayload,
    background_tasks: BackgroundTasks,
    auth: bool = Depends(verify_auth),
):
    """
    Enhanced quote update endpoint with background processing support
    """
    updated_count, errors = cache.update_quotes(payload.data)
    
    logger.info(
        f"Quote update: {updated_count} successful, "
        f"{len(errors)} errors out of {len(payload.data)} quotes"
    )

    # Background task for cache backup if significant update
    if updated_count > 10 and config.cache_backup_enabled:
        background_tasks.add_task(cache.create_backup)

    response = {
        "status": "completed",
        "updated_count": updated_count,
        "error_count": len(errors),
        "total_cached": len(cache.data),
        "source": payload.source,
        "timestamp": datetime.datetime.utcnow().isoformat() + "Z",
    }
    
    if errors:
        response["errors"] = errors

    return response

# =============================================================================
# Enhanced Cache Management Endpoints
# =============================================================================

@app.get("/v1/cache", response_model=Dict[str, Any])
@rate_limit("20/minute")
async def get_cache_info(
    request: Request,
    limit: int = Query(50, ge=1, le=1000),
    include_expired: bool = Query(False, description="Include expired entries in sample"),
    auth: bool = Depends(verify_auth),
):
    """
    Enhanced cache information endpoint with filtering options
    """
    cache_stats = cache.get_stats()
    
    # Get sample data with optional expired entries
    sample_data = list(cache.data.values())
    if not include_expired:
        now = time.time()
        sample_data = [item for item in sample_data if cache._is_valid(item, now)]
    
    sample_data = sample_data[:limit]
    
    # Clean sample data (remove cache metadata)
    clean_sample = []
    for item in sample_data:
        clean_item = {k: v for k, v in item.items() if not k.startswith("_")}
        clean_sample.append(clean_item)

    return {
        "total_items": cache_stats["total_items"],
        "valid_items": cache_stats["valid_items"],
        "expired_items": cache_stats["expired_items"],
        "sample_size": len(clean_sample),
        "sample_data": clean_sample,
        "cache_file": cache_stats["cache_file"],
        "file_exists": cache_stats["file_exists"],
        "file_size_bytes": cache_stats["file_size"],
        "ttl_minutes": cache_stats["ttl_minutes"],
        "max_size": cache_stats["max_size"],
        "performance": cache_stats["performance"],
    }

@app.post("/v1/cache/backup", response_model=Dict[str, Any])
@rate_limit("5/minute")
async def create_cache_backup(request: Request, auth: bool = Depends(verify_auth)):
    """
    Enhanced cache backup endpoint with validation
    """
    if not config.cache_backup_enabled:
        raise HTTPException(
            status_code=400, 
            detail="Cache backup is disabled in configuration"
        )
        
    backup_path = cache.create_backup()
    if not backup_path:
        raise HTTPException(status_code=500, detail="Failed to create backup")
        
    return {
        "status": "success", 
        "backup_path": backup_path, 
        "item_count": len(cache.data),
        "backup_size": cache.get_stats()["file_size"],
        "timestamp": datetime.datetime.utcnow().isoformat() + "Z",
    }

@app.post("/v1/cache/cleanup", response_model=Dict[str, Any])
@rate_limit("5/minute")
async def cleanup_cache(request: Request, auth: bool = Depends(verify_auth)):
    """
    Enhanced cache cleanup with detailed reporting
    """
    expired_count = cache.cleanup_expired()
    cache_stats = cache.get_stats()
    
    return {
        "status": "success",
        "expired_removed": expired_count,
        "remaining_items": len(cache.data),
        "cache_stats": cache_stats["performance"],
        "timestamp": datetime.datetime.utcnow().isoformat() + "Z",
    }

@app.delete("/v1/cache/clear", response_model=Dict[str, Any])
@rate_limit("2/minute")
async def clear_cache(request: Request, auth: bool = Depends(verify_auth)):
    """
    Enhanced cache clear with confirmation and backup
    """
    item_count = len(cache.data)
    
    # Create backup before clearing if enabled
    backup_path = None
    if config.cache_backup_enabled and item_count > 0:
        backup_path = cache.create_backup()
    
    cache.data.clear()
    
    if cache.save_cache():
        logger.warning(f"Cache cleared by user: {item_count} items removed")
        response = {
            "status": "success",
            "items_removed": item_count,
            "timestamp": datetime.datetime.utcnow().isoformat() + "Z",
        }
        
        if backup_path:
            response["backup_created"] = backup_path
            response["backup_note"] = "Automatic backup created before clearing"
            
        return response
    else:
        raise HTTPException(status_code=500, detail="Failed to clear cache")

# =============================================================================
# Enhanced v4.1 Compatibility Endpoints
# =============================================================================

@app.get("/v41/quotes")
@rate_limit("60/minute")
async def v41_get_quotes(
    request: Request,
    symbols: str = Query(..., description="Comma-separated tickers"),
    cache_ttl: int = Query(60, ge=0),
    include_metadata: bool = Query(False),
    auth: bool = Depends(verify_auth),
):
    """
    Enhanced v4.1 compatible quotes endpoint
    """
    tickers = [t.strip().upper() for t in symbols.split(",") if t.strip()]
    
    if not tickers:
        raise HTTPException(status_code=400, detail="No symbols provided")

    data: List[Dict[str, Any]] = []
    now_ts = datetime.datetime.utcnow().isoformat() + "Z"
    cache_hits = 0
    sources_used = set()

    for symbol in tickers:
        quote = cache.get_quote(symbol)
        if quote:
            cache_hits += 1
            quote_data = {
                "ticker": quote.ticker,
                "price": quote.price,
                "previous_close": quote.previous_close,
                "day_change": quote.day_change,
                "day_change_pct": quote.day_change_pct,
                "currency": quote.currency,
                "timestamp_utc": quote.timestamp_utc,
                "data_source": quote.data_source,
            }
            
            if include_metadata:
                quote_data["cache_info"] = {
                    "hit": True,
                    "source": quote.data_source,
                }
                
            if quote.data_source:
                sources_used.add(quote.data_source)
                
            data.append(quote_data)
        else:
            data.append({
                "ticker": symbol,
                "price": None,
                "previous_close": None,
                "day_change": None,
                "day_change_pct": None,
                "currency": None,
                "timestamp_utc": now_ts,
                "data_source": "cache_miss",
            })

    return {
        "ok": True,
        "symbols": tickers,
        "count": len(tickers),
        "cache_hits": cache_hits,
        "cache_misses": len(tickers) - cache_hits,
        "sources": list(sources_used),
        "timestamp_utc": now_ts,
        "data": data,
        "quotes": data,  # Duplicate for compatibility
    }

@app.post("/v41/quotes")
@rate_limit("30/minute")
async def v41_post_quotes(
    request: Request,
    payload: QuoteUpdatePayload,
    auth: bool = Depends(verify_auth),
):
    """
    Enhanced v4.1 compatible quote update endpoint
    """
    updated_count, errors = cache.update_quotes(payload.data)
    now_ts = datetime.datetime.utcnow().isoformat() + "Z"
    
    response = {
        "ok": True,
        "updated": updated_count,
        "total_cached": len(cache.data),
        "timestamp_utc": now_ts,
        "source": payload.source,
    }
    
    if errors:
        response["errors"] = errors

    return response

# =============================================================================
# Enhanced Market Data Endpoints
# =============================================================================

@app.get("/api/saudi/symbols", response_model=Dict[str, Any])
@rate_limit("30/minute")
async def get_saudi_symbols(
    request: Request,
    limit: int = Query(20, ge=1, le=500),
    include_prices: bool = Query(True, description="Include current prices from cache"),
    auth: bool = Depends(verify_auth),
):
    """
    Enhanced Saudi market symbols endpoint with price integration
    """
    try:
        # Try to read from KSA Tadawul Market sheet
        ksa_data = google_sheets_service.read_ksa_tadawul_market(limit)
        
        symbols = []
        cache_hits = 0
        
        for item in ksa_data:
            symbol_data = {
                "symbol": item.get("ticker", ""),
                "company_name": item.get("company_name", ""),
                "sector": item.get("sector", ""),
                "trading_market": item.get("trading_market", ""),
                "current_price": item.get("current_price"),
                "daily_change": item.get("daily_change"),
                "currency": item.get("currency", "SAR"),
                "country": item.get("country", "SA"),
            }
            
            # Enhance with cache data if available and requested
            if include_prices and symbol_data["symbol"]:
                quote = cache.get_quote(symbol_data["symbol"])
                if quote:
                    cache_hits += 1
                    symbol_data.update({
                        "cached_price": quote.price,
                        "cached_change_percent": quote.day_change_pct,
                        "cached_volume": quote.volume,
                        "cached_market_cap": quote.market_cap,
                        "cached_timestamp": quote.timestamp_utc,
                        "data_source": quote.data_source,
                    })
            
            symbols.append(symbol_data)

        return {
            "data": symbols,
            "count": len(symbols),
            "source": "google_sheets_ksa_tadawul",
            "cache_hits": cache_hits if include_prices else 0,
            "timestamp": datetime.datetime.utcnow().isoformat() + "Z",
        }
        
    except Exception as e:
        logger.error(f"Failed to fetch KSA symbols: {e}")
        return {
            "data": [], 
            "count": 0, 
            "error": str(e), 
            "source": "error",
            "timestamp": datetime.datetime.utcnow().isoformat() + "Z",
        }

@app.get("/api/saudi/market", response_model=Dict[str, Any])
@rate_limit("30/minute")
async def get_saudi_market(
    request: Request,
    limit: int = Query(20, ge=1, le=500),
    refresh_prices: bool = Query(False, description="Force refresh prices from APIs"),
    auth: bool = Depends(verify_auth),
):
    """
    Enhanced Saudi market data with real-time price integration
    """
    try:
        # Get symbols first
        symbols_data = await get_saudi_symbols(
            request, limit=limit, include_prices=not refresh_prices, auth=auth
        )
        symbols = symbols_data.get("data", [])
        
        # Refresh prices if requested
        if refresh_prices:
            # This would be enhanced to actually fetch from APIs
            # For now, we'll just use cache data
            logger.info("Price refresh requested but not implemented in this version")
        
        # Enhance with cache data
        market_data = []
        cache_hits = 0
        
        for symbol_info in symbols:
            symbol = symbol_info.get("symbol")
            if symbol:
                quote = cache.get_quote(symbol)
                if quote:
                    cache_hits += 1
                    symbol_info.update({
                        "current_price": quote.price,
                        "daily_change": quote.day_change_pct,
                        "volume": quote.volume,
                        "market_cap": quote.market_cap,
                        "last_updated": quote.timestamp_utc,
                        "data_source": quote.data_source,
                        "price_source": "cache",
                    })
                else:
                    # Use data from sheet if available
                    symbol_info.update({
                        "current_price": symbol_info.get("current_price"),
                        "daily_change": symbol_info.get("daily_change"),
                        "last_updated": datetime.datetime.utcnow().isoformat() + "Z",
                        "data_source": "google_sheets",
                        "price_source": "sheet",
                    })
                    
            market_data.append(symbol_info)

        return {
            "count": len(market_data),
            "data": market_data,
            "source": symbols_data.get("source", "unknown"),
            "cache_hits": cache_hits,
            "cache_misses": len(market_data) - cache_hits,
            "price_refresh": refresh_prices,
            "timestamp": datetime.datetime.utcnow().isoformat() + "Z",
        }
        
    except Exception as e:
        logger.error(f"Failed to fetch Saudi market data: {e}")
        raise HTTPException(status_code=500, detail=f"Market data fetch failed: {e}")

# =============================================================================
# Enhanced Financial APIs Status & Data Endpoints
# =============================================================================

@app.get("/v1/financial-apis/status", response_model=Dict[str, Any])
@rate_limit("15/minute")
async def get_financial_apis_status(request: Request, auth: bool = Depends(verify_auth)):
    """
    Enhanced financial APIs status with comprehensive testing
    """
    api_status = financial_client.get_api_status()
    test_results: List[Dict[str, Any]] = []
    test_symbols = ["AAPL", "7201.SR", "EURUSD"]  # Diverse test symbols
    
    # Test each configured API
    for api_name, status_info in api_status.items():
        if not status_info["configured"] or not status_info["enabled"]:
            continue
            
        for test_symbol in test_symbols:
            try:
                start_time = time.time()
                
                data = await financial_client.get_stock_quote_with_retry(
                    test_symbol, api_name, max_retries=1
                )
                
                elapsed = (time.time() - start_time) * 1000  # Convert to milliseconds
                
                if data:
                    test_results.append({
                        "api_name": api_name,
                        "status": "SUCCESS",
                        "response_time_ms": round(elapsed, 2),
                        "test_symbol": test_symbol,
                        "price": data.get("price"),
                        "data_points": len(data),
                    })
                    break  # Test one symbol per API for speed
                else:
                    test_results.append({
                        "api_name": api_name,
                        "status": "ERROR",
                        "error": "No data returned",
                        "test_symbol": test_symbol,
                    })
                    
            except Exception as e:
                test_results.append({
                    "api_name": api_name,
                    "status": "ERROR",
                    "error": str(e),
                    "test_symbol": test_symbol,
                })

    # Calculate overall API health
    successful_tests = [r for r in test_results if r["status"] == "SUCCESS"]
    health_score = len(successful_tests) / len(test_results) if test_results else 0

    return {
        "api_configuration": api_status,
        "api_tests": test_results,
        "health_score": round(health_score * 100, 2),
        "timestamp": datetime.datetime.utcnow().isoformat() + "Z",
        "recommendations": self._generate_api_recommendations(api_status, test_results),
    }

def _generate_api_recommendations(self, api_status: Dict[str, Any], test_results: List[Dict[str, Any]]) -> List[str]:
    """Generate API recommendations based on status and test results"""
    recommendations = []
    
    # Check for unconfigured APIs
    unconfigured = [name for name, status in api_status.items() if not status["configured"]]
    if unconfigured:
        recommendations.append(f"Consider configuring: {', '.join(unconfigured)}")
    
    # Check for APIs with high error rates
    api_errors = {}
    for test in test_results:
        if test["status"] == "ERROR":
            api_errors[test["api_name"]] = api_errors.get(test["api_name"], 0) + 1
    
    for api_name, error_count in api_errors.items():
        if error_count >= 2:  # If most tests failed
            recommendations.append(f"API {api_name} has reliability issues, consider alternatives")
    
    return recommendations

@app.get("/v1/financial-data/{symbol}")
@rate_limit("30/minute")
async def get_financial_data(
    request: Request,
    symbol: str,
    api: str = Query("alpha_vantage"),
    max_retries: int = Query(None, ge=0, le=5),
    auth: bool = Depends(verify_auth),
):
    """
    Enhanced financial data endpoint with retry configuration
    """
    if api not in FINANCIAL_APIS:
        available_apis = [name for name, cfg in FINANCIAL_APIS.items() if cfg.get("enabled", False)]
        raise HTTPException(
            status_code=400, 
            detail=f"API {api} not supported or disabled. Available: {available_apis}"
        )
        
    data = await financial_client.get_stock_quote_with_retry(
        symbol, api, max_retries=max_retries or config.max_retries
    )
    
    if not data:
        raise HTTPException(
            status_code=404, 
            detail=f"No data found for {symbol} from {api}"
        )
        
    return {
        "symbol": symbol.upper(),
        "api": api,
        "data": data,
        "timestamp": datetime.datetime.utcnow().isoformat() + "Z",
        "retries_used": max_retries or config.max_retries,
    }

# =============================================================================
# Application Entry Point with Enhanced Features
# =============================================================================

def main():
    """Enhanced main function with comprehensive startup options"""
    import argparse
    
    parser = argparse.ArgumentParser(description=f"{config.service_name} v{config.service_version}")
    parser.add_argument("--host", default=config.app_host, help="Host to bind to")
    parser.add_argument("--port", type=int, default=config.app_port, help="Port to bind to")
    parser.add_argument("--reload", action="store_true", help="Enable auto-reload")
    parser.add_argument("--workers", type=int, default=1, help="Number of worker processes")
    parser.add_argument("--log-level", choices=["DEBUG", "INFO", "WARNING", "ERROR"], 
                       default=LOG_LEVEL, help="Log level")
    
    args = parser.parse_args()
    
    # Update log level if specified
    if args.log_level != LOG_LEVEL:
        logging.getLogger().setLevel(getattr(logging, args.log_level))
        logger.info(f"Log level changed to {args.log_level}")
    
    # Startup banner
    logger.info("=" * 70)
    logger.info(f"🚀 {config.service_name} v{config.service_version}")
    logger.info(f"🌍 Environment: {config.environment}")
    logger.info(f"🔐 Authentication: {'Enabled' if config.require_auth else 'Disabled'}")
    logger.info(f"⚡ Rate Limiting: {'Enabled' if config.enable_rate_limiting else 'Disabled'}")
    logger.info(f"📊 Google Sheets: {'Direct' if config.google_service_account_json else 'Apps Script'}")
    logger.info(f"💾 Cache: {len(cache.data)} items loaded")
    logger.info(f"🔧 Debug Mode: {config.debug}")
    logger.info(f"🌐 Starting server on {args.host}:{args.port}")
    logger.info("=" * 70)
    
    # Server configuration
    server_config = {
        "app": "main:app",
        "host": args.host,
        "port": args.port,
        "log_level": args.log_level.lower(),
        "access_log": True,
        "reload": args.reload or config.environment == "development",
        "workers": args.workers if args.workers > 1 else None,
    }
    
    # Remove None values
    server_config = {k: v for k, v in server_config.items() if v is not None}
    
    # Start server
    uvicorn.run(**server_config)

if __name__ == "__main__":
    main()
