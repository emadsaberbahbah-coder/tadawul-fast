#!/usr/bin/env python3
"""
Tadawul Stock Analysis API - Enhanced Version 3.2.0
Production-ready with all architectural improvements and optimizations
"""

import asyncio
import datetime
import hashlib
import json
import logging
import time
import uuid
from contextlib import asynccontextmanager
from enum import Enum
from pathlib import Path
from typing import Any, Dict, List, Optional, Protocol, Tuple

import aiohttp
import uvicorn
from dotenv import load_dotenv
from fastapi import (
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
from pydantic import BaseModel, Field, validator
from pydantic_settings import BaseSettings
from slowapi import Limiter, _rate_limit_exceeded_handler
from slowapi.errors import RateLimitExceeded
from slowapi.util import get_remote_address

# --- Advanced AI Trading Analysis integration ---
from advanced_analysis import (
    analyzer,                    # Global AdvancedTradingAnalyzer instance
    generate_ai_recommendation,  # async convenience function
    get_multi_source_analysis,   # async convenience function
)

# =============================================================================
# Configuration
# =============================================================================

load_dotenv()

class Settings(BaseSettings):
    """Enhanced configuration management with validation"""
    
    # Service Configuration
    service_name: str = Field("Tadawul Stock Analysis API", env="SERVICE_NAME")
    service_version: str = Field("3.2.0", env="SERVICE_VERSION")
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

    # CORS - Support both JSON and comma-separated values
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
    cache_save_interval: int = Field(10, env="CACHE_SAVE_INTERVAL")  # seconds

    # Performance (more conservative defaults)
    http_timeout: int = Field(10, env="HTTP_TIMEOUT")      # was 30
    max_retries: int = Field(2, env="MAX_RETRIES")         # was 3
    retry_delay: float = Field(0.7, env="RETRY_DELAY")     # was 1.0

    # Logging
    log_level: str = Field("INFO", env="LOG_LEVEL")
    log_format: str = Field("detailed", env="LOG_FORMAT")
    log_enable_file: bool = Field(False, env="LOG_ENABLE_FILE")

    class Config:
        env_file = ".env"
        case_sensitive = False
        extra = "ignore"

    @validator("cors_origins", pre=True)
    def parse_cors_origins(cls, v):
        """Parse CORS origins from both JSON and comma-separated formats"""
        if isinstance(v, str):
            # Try to parse as JSON first
            if v.startswith('[') and v.endswith(']'):
                try:
                    return json.loads(v)
                except json.JSONDecodeError:
                    pass
            # Fall back to comma-separated
            return [origin.strip() for origin in v.split(',') if origin.strip()]
        return v

    @property
    def is_production(self) -> bool:
        return self.environment == "production"

    @property
    def is_development(self) -> bool:
        return self.environment == "development"

    @property
    def has_google_sheets_access(self) -> bool:
        """Check if we have either direct Sheets access or Apps Script"""
        return bool(self.google_sheets_credentials or self.google_apps_script_url)

    def validate_configuration(self) -> List[str]:
        """Validate configuration and return list of errors"""
        errors = []
        warnings = []

        # Auth validation
        if self.require_auth and not any([self.app_token, self.backup_app_token]):
            if self.is_production:
                errors.append("Authentication required but no tokens configured in production")
            else:
                warnings.append("Authentication required but no tokens configured")

        # Google services validation
        if not self.has_google_sheets_access:
            warnings.append("No Google Sheets or Apps Script configuration - limited functionality")

        # Apps Script URL validation (only if provided)
        if self.google_apps_script_url and self.google_apps_script_url.strip() in ["", "undefined"]:
            errors.append("Invalid GOOGLE_APPS_SCRIPT_URL provided")
        elif self.google_apps_script_url and not self.google_apps_script_url.startswith(('http://', 'https://')):
            errors.append("Invalid GOOGLE_APPS_SCRIPT_URL format")

        # Log warnings
        for warning in warnings:
            logging.warning(f"⚠️ {warning}")

        return errors

    def log_config_summary(self):
        """Log configuration summary without sensitive data"""
        logging.info(f"🔧 {self.service_name} v{self.service_version}")
        logging.info(f"🌍 Environment: {self.environment} (Production: {self.is_production})")
        logging.info(f"🔐 Auth: {'Enabled' if self.require_auth else 'Disabled'}")
        logging.info(f"⚡ Rate Limiting: {'Enabled' if self.enable_rate_limiting else 'Disabled'}")
        logging.info(f"📊 Google Services: {'Available' if self.has_google_sheets_access else 'Not Available'}")
        logging.info(f"💾 Cache: TTL={self.cache_default_ttl}s, Save Interval={self.cache_save_interval}s")
        
        # Count configured APIs
        configured_apis = sum([
            bool(self.alpha_vantage_api_key),
            bool(self.finnhub_api_key),
            bool(self.eodhd_api_key),
            bool(self.twelvedata_api_key),
            bool(self.marketstack_api_key),
            bool(self.fmp_api_key),
        ])
        logging.info(f"📈 Financial APIs: {configured_apis} configured")

# Initialize settings
settings = Settings()

# =============================================================================
# Logging Setup
# =============================================================================

def setup_logging():
    """Configure structured logging with request context"""
    log_level = getattr(logging, settings.log_level.upper(), logging.INFO)
    
    # Configure format
    if settings.log_format == "detailed":
        log_format = "%(asctime)s - %(name)s - %(levelname)s - [%(filename)s:%(lineno)d] - %(message)s"
    else:
        log_format = "%(asctime)s - %(levelname)s - %(message)s"

    handlers = [logging.StreamHandler()]
    if settings.log_enable_file:
        log_file = Path(__file__).parent / "app.log"
        handlers.append(logging.FileHandler(str(log_file), encoding='utf-8'))

    logging.basicConfig(
        level=log_level,
        format=log_format,
        handlers=handlers,
    )

    # Suppress noisy loggers
    logging.getLogger("uvicorn.access").disabled = True

setup_logging()
logger = logging.getLogger(__name__)

# =============================================================================
# Data Models
# =============================================================================

class QuoteStatus(str, Enum):
    OK = "OK"
    NO_DATA = "NO_DATA"
    ERROR = "ERROR"

class Quote(BaseModel):
    """Unified quote model for all endpoints"""
    ticker: str = Field(..., description="Stock ticker symbol")
    status: QuoteStatus = Field(QuoteStatus.OK, description="Quote status")
    price: Optional[float] = Field(None, ge=0, description="Current price")
    previous_close: Optional[float] = Field(None, ge=0, description="Previous close price")
    change_value: Optional[float] = Field(None, description="Change amount")
    change_percent: Optional[float] = Field(None, description="Change percentage")
    volume: Optional[float] = Field(None, ge=0, description="Trading volume")
    market_cap: Optional[float] = Field(None, ge=0, description="Market capitalization")
    open_price: Optional[float] = Field(None, ge=0, description="Open price")
    high_price: Optional[float] = Field(None, ge=0, description="Daily high")
    low_price: Optional[float] = Field(None, ge=0, description="Daily low")
    
    # Fundamental data fields (for future extension)
    pe_ratio: Optional[float] = Field(None, description="Price-to-Earnings ratio")
    pb_ratio: Optional[float] = Field(None, description="Price-to-Book ratio")
    roe: Optional[float] = Field(None, description="Return on Equity")
    eps: Optional[float] = Field(None, description="Earnings Per Share")
    
    currency: Optional[str] = Field(None, description="Currency code")
    exchange: Optional[str] = Field(None, description="Stock exchange")
    sector: Optional[str] = Field(None, description="Company sector")
    country: Optional[str] = Field(None, description="Country code")
    provider: Optional[str] = Field(None, description="Data provider")
    as_of: Optional[datetime.datetime] = Field(None, description="Quote timestamp")
    message: Optional[str] = Field(None, description="Status message")

    @validator("ticker")
    def validate_ticker(cls, v: str) -> str:
        v = v.strip().upper()
        if not v:
            raise ValueError("Ticker cannot be empty")
        return v

class QuotesResponse(BaseModel):
    """Standardized quotes response"""
    timestamp: datetime.datetime = Field(..., description="Response timestamp")
    symbols: List[Quote] = Field(..., description="List of quotes")
    meta: Optional[Dict[str, Any]] = Field(None, description="Response metadata")

class ErrorResponse(BaseModel):
    """Standardized error response"""
    error: str = Field(..., description="Error type")
    message: str = Field(..., description="Error message")
    detail: Optional[Dict[str, Any]] = Field(None, description="Additional details")
    timestamp: datetime.datetime = Field(..., description="Error timestamp")
    request_id: Optional[str] = Field(None, description="Request ID for debugging")

class HealthResponse(BaseModel):
    """Enhanced health check response"""
    status: str = Field(..., description="Service status")
    version: str = Field(..., description="API version")
    timestamp: datetime.datetime = Field(..., description="Current timestamp")
    uptime_seconds: float = Field(..., description="Service uptime")
    dependencies: Dict[str, Any] = Field(..., description="Dependency status")

class QuoteRequest(BaseModel):
    """Quote request model"""
    symbols: List[str] = Field(..., description="List of symbols to fetch")
    cache_ttl: Optional[int] = Field(None, description="Cache TTL in seconds (future use)")
    providers: Optional[List[str]] = Field(None, description="Preferred providers")

# =============================================================================
# Enhanced Cache Implementation
# =============================================================================

import threading

class TTLCache:
    """Enhanced TTL cache with batched writes and thread safety"""
    
    def __init__(self, ttl_seconds: int = None, max_size: int = None, save_interval: int = None):
        self.cache_path = Path(__file__).parent / "quote_cache.json"
        self.backup_dir = Path(__file__).parent / "cache_backups"
        self.backup_dir.mkdir(parents=True, exist_ok=True)
        
        self.data: Dict[str, Dict[str, Any]] = {}
        self.ttl = ttl_seconds or settings.cache_default_ttl
        self.max_size = max_size or settings.cache_max_size
        self.save_interval = save_interval or settings.cache_save_interval
        
        # Thread safety and batching
        self._lock = threading.Lock()
        self._dirty = False
        self._last_save = time.time()
        
        self.metrics = {
            "hits": 0,
            "misses": 0,
            "updates": 0,
            "expired_removals": 0,
            "errors": 0,
            "immediate_saves": 0,
        }
        
        self._load_cache()

    def _is_valid(self, item: Dict[str, Any], now: Optional[float] = None) -> bool:
        """Check if cache item is still valid"""
        if now is None:
            now = time.time()
        timestamp = item.get("_cache_timestamp", 0)
        return (now - timestamp) < self.ttl

    def _load_cache(self) -> bool:
        """Load cache from disk"""
        try:
            if not self.cache_path.exists():
                logger.info("No cache file found, starting with empty cache")
                return True

            with open(self.cache_path, "r", encoding="utf-8") as f:
                raw_data = json.load(f)

            now = time.time()
            valid_items: Dict[str, Dict[str, Any]] = {}
            
            if isinstance(raw_data, dict) and "data" in raw_data:
                for item in raw_data["data"]:
                    if isinstance(item, dict) and "ticker" in item and self._is_valid(item, now):
                        valid_items[item["ticker"]] = item
            elif isinstance(raw_data, dict):
                for key, item in raw_data.items():
                    if isinstance(item, dict) and self._is_valid(item, now):
                        valid_items[key] = item

            self.data = valid_items
            logger.info(f"✅ Cache loaded with {len(valid_items)} valid items")
            return True
            
        except Exception as e:
            logger.error(f"❌ Failed to load cache: {e}")
            self.metrics["errors"] += 1
            return False

    def _save_cache_immediate(self) -> bool:
        """Immediate cache save (used for critical operations)"""
        with self._lock:
            try:
                if len(self.data) > self.max_size:
                    self._enforce_size_limit()
                    
                cache_data = {
                    "metadata": {
                        "version": "1.3",
                        "saved_at": datetime.datetime.utcnow().isoformat() + "Z",
                        "item_count": len(self.data),
                        "ttl_seconds": self.ttl,
                    },
                    "data": list(self.data.values())
                }
                
                temp_path = self.cache_path.with_suffix(".tmp")
                with open(temp_path, "w", encoding="utf-8") as f:
                    json.dump(cache_data, f, indent=2, ensure_ascii=False, default=str)
                
                temp_path.replace(self.cache_path)
                self._dirty = False
                self._last_save = time.time()
                self.metrics["immediate_saves"] += 1
                return True
                
            except Exception as e:
                logger.error(f"❌ Failed to save cache: {e}")
                self.metrics["errors"] += 1
                return False

    def _schedule_save(self):
        """Schedule a batched save if enough time has passed"""
        now = time.time()
        if self._dirty and (now - self._last_save) >= self.save_interval:
            self._save_cache_immediate()

    def _enforce_size_limit(self):
        """Enforce cache size limit"""
        if len(self.data) <= self.max_size:
            return
            
        items_to_remove = len(self.data) - self.max_size
        sorted_items = sorted(
            self.data.items(), 
            key=lambda x: x[1].get("_cache_timestamp", 0)
        )
        
        for ticker, _ in sorted_items[:items_to_remove]:
            del self.data[ticker]
            self.metrics["expired_removals"] += 1

    def get(self, symbol: str) -> Optional[Dict[str, Any]]:
        """Get quote from cache with thread safety"""
        symbol = symbol.upper().strip()
        
        with self._lock:
            if symbol in self.data and self._is_valid(self.data[symbol]):
                self.metrics["hits"] += 1
                # Return copy without internal metadata
                clean_data = {k: v for k, v in self.data[symbol].items() 
                             if not k.startswith("_")}
                return clean_data
            
            # Remove expired entry
            if symbol in self.data:
                del self.data[symbol]
                self._dirty = True
                self._schedule_save()
                
            self.metrics["misses"] += 1
            return None

    def set(self, symbol: str, quote: Dict[str, Any]) -> None:
        """Set quote in cache with batched saves"""
        symbol = symbol.upper().strip()
        quote_data = quote.copy()
        quote_data["_cache_timestamp"] = time.time()
        quote_data["_last_updated"] = datetime.datetime.utcnow().isoformat() + "Z"
        
        with self._lock:
            self.data[symbol] = quote_data
            self.metrics["updates"] += 1
            self._dirty = True
            self._schedule_save()

    def cleanup_expired(self) -> int:
        """Clean up expired entries"""
        now = time.time()
        expired_tickers = []
        
        with self._lock:
            expired_tickers = [
                ticker for ticker, item in self.data.items() 
                if not self._is_valid(item, now)
            ]
            
            for ticker in expired_tickers:
                del self.data[ticker]
                self.metrics["expired_removals"] += 1
                
            if expired_tickers:
                self._dirty = True
                self._schedule_save()
                logger.info(f"🧹 Removed {len(expired_tickers)} expired cache entries")
                
        return len(expired_tickers)

    def flush(self) -> bool:
        """Force immediate cache save"""
        if self._dirty:
            return self._save_cache_immediate()
        return True

    def get_item_count(self) -> int:
        """Get current item count with thread safety"""
        with self._lock:
            return len(self.data)

    def get_stats(self) -> Dict[str, Any]:
        """Get cache statistics"""
        now = time.time()
        with self._lock:
            valid_count = sum(1 for item in self.data.values() if self._is_valid(item, now))
            expired_count = len(self.data) - valid_count
            
            total_requests = self.metrics["hits"] + self.metrics["misses"]
            hit_rate = (self.metrics["hits"] / total_requests * 100) if total_requests > 0 else 0
            
            return {
                "total_items": len(self.data),
                "valid_items": valid_count,
                "expired_items": expired_count,
                "max_size": self.max_size,
                "ttl_seconds": self.ttl,
                "dirty": self._dirty,
                "last_save_seconds_ago": time.time() - self._last_save,
                "performance": {
                    "hits": self.metrics["hits"],
                    "misses": self.metrics["misses"],
                    "hit_rate": round(hit_rate, 2),
                    "updates": self.metrics["updates"],
                    "expired_removals": self.metrics["expired_removals"],
                    "immediate_saves": self.metrics["immediate_saves"],
                    "errors": self.metrics["errors"],
                }
            }

# Global cache instance
cache = TTLCache()

# =============================================================================
# Enhanced HTTP Client with Retry Logic
# =============================================================================

class HTTPClient:
    """Shared HTTP client with retry logic and connection pooling"""
    
    def __init__(self):
        self.session: Optional[aiohttp.ClientSession] = None
        self.timeout = aiohttp.ClientTimeout(total=settings.http_timeout)
        
    async def get_session(self) -> aiohttp.ClientSession:
        """Get or create shared session"""
        if self.session is None:
            connector = aiohttp.TCPConnector(
                limit=100,
                limit_per_host=20,
                verify_ssl=True,
            )
            self.session = aiohttp.ClientSession(
                timeout=self.timeout,
                connector=connector,
                headers={
                    'User-Agent': f'{settings.service_name}/{settings.service_version}',
                    'Accept': 'application/json',
                }
            )
        return self.session

    async def request_with_retry(
        self,
        method: str,
        url: str,
        params: Optional[Dict[str, Any]] = None,
        max_retries: Optional[int] = None
    ) -> Optional[Dict[str, Any]]:
        """Make HTTP request with retry logic"""
        max_retries = max_retries or settings.max_retries
        last_exception = None
        
        for attempt in range(max_retries):
            try:
                session = await self.get_session()
                async with session.request(method, url, params=params) as response:
                    if response.status == 200:
                        return await response.json()
                    else:
                        error_text = await response.text()
                        last_exception = Exception(f"HTTP {response.status}: {error_text}")
            except asyncio.TimeoutError:
                last_exception = Exception(f"Timeout after {settings.http_timeout}s")
            except Exception as e:
                last_exception = e
                
            if attempt < max_retries - 1:
                wait_time = settings.retry_delay * (2 ** attempt)  # Exponential backoff
                logger.debug(f"Retry {attempt + 1}/{max_retries} in {wait_time:.1f}s for {url}")
                await asyncio.sleep(wait_time)
        
        logger.error(f"All retries failed for {url}: {last_exception}")
        return None

    async def close(self):
        """Close the session"""
        if self.session:
            await self.session.close()
            self.session = None

# Global HTTP client
http_client = HTTPClient()

# =============================================================================
# Enhanced Provider Abstraction
# =============================================================================

class ProviderClient(Protocol):
    """Provider interface"""
    name: str
    rate_limit: int
    
    async def get_quote(self, symbol: str) -> Optional[Dict[str, Any]]: ...

class AlphaVantageProvider:
    """Alpha Vantage provider implementation"""
    
    def __init__(self):
        self.name = "alpha_vantage"
        self.rate_limit = 5
        self.base_url = "https://www.alphavantage.co/query"
        self.api_key = settings.alpha_vantage_api_key
        self.enabled = bool(self.api_key)

    async def get_quote(self, symbol: str) -> Optional[Dict[str, Any]]:
        if not self.enabled:
            return None

        params = {
            "function": "GLOBAL_QUOTE",
            "symbol": symbol,
            "apikey": self.api_key,
        }
        
        data = await http_client.request_with_retry("GET", self.base_url, params)
        if data:
            return self._parse_response(data, symbol)
        return None

    def _parse_response(self, data: Dict[str, Any], symbol: str) -> Optional[Dict[str, Any]]:
        """Parse Alpha Vantage response"""
        quote_data = data.get("Global Quote", {})
        if not quote_data:
            return None

        return {
            "ticker": symbol,
            "price": self._safe_float(quote_data.get("05. price")),
            "previous_close": self._safe_float(quote_data.get("08. previous close")),
            "change_value": self._safe_float(quote_data.get("09. change")),
            "change_percent": self._safe_float(str(quote_data.get("10. change percent", "0")).rstrip("%")),
            "volume": self._safe_int(quote_data.get("06. volume")),
            "open_price": self._safe_float(quote_data.get("02. open")),
            "high_price": self._safe_float(quote_data.get("03. high")),
            "low_price": self._safe_float(quote_data.get("04. low")),
            "provider": self.name,
        }

    def _safe_float(self, value: Any) -> Optional[float]:
        try:
            return float(value) if value else None
        except (ValueError, TypeError):
            return None

    def _safe_int(self, value: Any) -> Optional[int]:
        try:
            return int(float(value)) if value else None
        except (ValueError, TypeError):
            return None

class FinnhubProvider:
    """Finnhub provider implementation"""
    
    def __init__(self):
        self.name = "finnhub"
        self.rate_limit = 60
        self.base_url = "https://finnhub.io/api/v1"
        self.api_key = settings.finnhub_api_key
        self.enabled = bool(self.api_key)

    async def get_quote(self, symbol: str) -> Optional[Dict[str, Any]]:
        if not self.enabled:
            return None

        params = {"symbol": symbol, "token": self.api_key}
        data = await http_client.request_with_retry("GET", f"{self.base_url}/quote", params)
        if data:
            return self._parse_response(data, symbol)
        return None

    def _parse_response(self, data: Dict[str, Any], symbol: str) -> Optional[Dict[str, Any]]:
        if not data or "c" not in data:
            return None

        timestamp = None
        if data.get("t"):
            timestamp = datetime.datetime.fromtimestamp(data["t"]).isoformat()

        return {
            "ticker": symbol,
            "price": data.get("c"),
            "change_value": data.get("d"),
            "change_percent": data.get("dp"),
            "high_price": data.get("h"),
            "low_price": data.get("l"),
            "open_price": data.get("o"),
            "previous_close": data.get("pc"),
            "provider": self.name,
            "as_of": timestamp,
        }

class ProviderManager:
    """Enhanced manager for provider selection and fallback"""
    
    def __init__(self):
        self.providers: List[ProviderClient] = [
            AlphaVantageProvider(),
            FinnhubProvider(),
        ]
        
        self.enabled_providers = [p for p in self.providers if p.enabled]
        logger.info(f"✅ Enabled providers: {[p.name for p in self.enabled_providers]}")

    async def get_quote(self, symbol: str) -> Optional[Dict[str, Any]]:
        """Get quote from providers with enhanced error handling"""
        errors = []
        
        for provider in self.enabled_providers:
            try:
                quote = await provider.get_quote(symbol)
                if quote and quote.get("price") is not None:
                    logger.info(f"✅ Got quote for {symbol} from {provider.name}")
                    return quote
                else:
                    errors.append(f"{provider.name}: No data")
            except Exception as e:
                errors.append(f"{provider.name}: {str(e)}")
                logger.warning(f"Provider {provider.name} failed for {symbol}: {e}")
                continue
                
        logger.warning(f"❌ All providers failed for {symbol}: {', '.join(errors)}")
        return None

    async def get_quotes(self, symbols: List[str]) -> List[Dict[str, Any]]:
        """Get multiple quotes in parallel with enhanced error tracking"""
        tasks = [self.get_quote(symbol) for symbol in symbols]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        quotes = []
        for i, result in enumerate(results):
            symbol = symbols[i]
            if isinstance(result, dict):
                quotes.append(result)
            elif isinstance(result, Exception):
                logger.error(f"Quote fetch error for {symbol}: {result}")
            else:
                logger.debug(f"No data available for {symbol} from any provider")
                
        return quotes

# Global provider manager
provider_manager = ProviderManager()

# =============================================================================
# Tadawul Helper
# =============================================================================

def is_tadawul_symbol(symbol: str) -> bool:
    """
    Basic Tadawul symbol detector.

    Tadawul tickers in this project are expected to end with '.SR',
    e.g. '2222.SR', '1120.SR', etc.
    """
    if not symbol:
        return False
    return symbol.strip().upper().endswith(".SR")

# =============================================================================
# Enhanced Quote Service Layer
# =============================================================================

class QuoteService:
    """Enhanced service layer for quote management"""
    
    def __init__(self, cache, provider_manager):
        self.cache = cache
        self.provider_manager = provider_manager

    async def get_quote(self, symbol: str) -> Quote:
        """Get single quote with enhanced error handling"""
        # Normalize symbol
        symbol_norm = symbol.strip().upper()

        # Handle Tadawul upfront – providers on current plan don't support .SR
        if is_tadawul_symbol(symbol_norm):
            logger.info(f"Tadawul symbol detected with no enabled providers: {symbol_norm}")
            return Quote(
                ticker=symbol_norm,
                status=QuoteStatus.NO_DATA,
                currency="SAR",
                exchange="TADAWUL",
                message=(
                    "Tadawul (.SR) market data is not enabled on the current data provider plan. "
                    "Global / US symbols (e.g. AAPL, MSFT) are supported."
                ),
            )

        # Try cache first for non-Tadawul
        cached_quote = self.cache.get(symbol_norm)
        if cached_quote:
            logger.debug(f"Cache hit for {symbol_norm}")
            return self._create_quote_model(cached_quote, QuoteStatus.OK)

        # Fetch from provider
        try:
            provider_data = await self.provider_manager.get_quote(symbol_norm)
            if provider_data:
                # Cache the result
                self.cache.set(symbol_norm, provider_data)
                return self._create_quote_model(provider_data, QuoteStatus.OK)
            else:
                return Quote(
                    ticker=symbol_norm,
                    status=QuoteStatus.NO_DATA,
                    message="No data available from any provider"
                )
        except Exception as e:
            logger.error(f"Error fetching quote for {symbol_norm}: {e}")
            return Quote(
                ticker=symbol_norm,
                status=QuoteStatus.ERROR,
                message=f"Error fetching data: {str(e)}"
            )

    async def get_quotes(self, symbols: List[str]) -> QuotesResponse:
        """Get multiple quotes with efficient caching and error isolation"""
        cache_hits: List[Tuple[str, Dict[str, Any]]] = []
        cache_misses: List[str] = []
        sources_used = set()

        # Separate Tadawul and non-Tadawul symbols
        original_symbols = symbols
        tadawul_quotes: List[Quote] = []
        normal_symbols: List[str] = []

        for raw_symbol in original_symbols:
            sym = raw_symbol.strip().upper()
            if not sym:
                continue
            if is_tadawul_symbol(sym):
                logger.info(f"Tadawul symbol detected with no enabled providers: {sym}")
                tadawul_quotes.append(
                    Quote(
                        ticker=sym,
                        status=QuoteStatus.NO_DATA,
                        currency="SAR",
                        exchange="TADAWUL",
                        message=(
                            "Tadawul (.SR) market data is not enabled on the current data provider "
                            "plan. Global / US symbols (e.g. AAPL, MSFT) are supported."
                        ),
                    )
                )
            else:
                normal_symbols.append(sym)

        # Phase 1: Check cache for non-Tadawul symbols
        for symbol in normal_symbols:
            cached_quote = self.cache.get(symbol)
            if cached_quote:
                cache_hits.append((symbol, cached_quote))
                if cached_quote.get("provider"):
                    sources_used.add(cached_quote["provider"])
            else:
                cache_misses.append(symbol)

        # Phase 2: Fetch missing symbols from providers
        provider_quotes: List[Dict[str, Any]] = []
        if cache_misses:
            provider_quotes = await self.provider_manager.get_quotes(cache_misses)
            
            # Cache successful provider results
            for provider_quote in provider_quotes:
                if provider_quote and provider_quote.get("price") is not None:
                    symbol = provider_quote["ticker"]
                    self.cache.set(symbol, provider_quote)
                    if provider_quote.get("provider"):
                        sources_used.add(provider_quote["provider"])

        # Phase 3: Build final quotes list for non-Tadawul symbols
        quotes: List[Quote] = []
        
        # Add cache hits
        for symbol, cached_data in cache_hits:
            quotes.append(self._create_quote_model(cached_data, QuoteStatus.OK))
        
        # Add provider results (both successes and failures)
        provider_results_map = {q["ticker"]: q for q in provider_quotes if q and "ticker" in q}
        
        for symbol in cache_misses:
            if symbol in provider_results_map:
                # Success from provider
                quotes.append(self._create_quote_model(provider_results_map[symbol], QuoteStatus.OK))
            else:
                # No data from any provider
                quotes.append(Quote(
                    ticker=symbol,
                    status=QuoteStatus.NO_DATA,
                    message="No data available from providers"
                ))

        # Combine Tadawul + non-Tadawul quotes
        all_quotes = quotes + tadawul_quotes

        # Prepare metadata
        meta = {
            "cache_hits": len(cache_hits),
            "cache_misses": len(cache_misses),
            "provider_successes": len(provider_results_map),
            "sources": list(sources_used),
            "total_symbols": len(original_symbols),
            "successful_quotes": len([q for q in all_quotes if q.status == QuoteStatus.OK])
        }

        return QuotesResponse(
            timestamp=datetime.datetime.utcnow(),
            symbols=all_quotes,
            meta=meta
        )

    def _create_quote_model(self, data: Dict[str, Any], status: QuoteStatus) -> Quote:
        """Create Quote model from provider/cache data with proper timestamp handling"""
        # Use stored as_of if available, otherwise current time
        as_of = data.get("as_of")
        if as_of and isinstance(as_of, str):
            try:
                # Try to parse ISO format timestamp
                as_of = datetime.datetime.fromisoformat(as_of.replace('Z', '+00:00'))
            except (ValueError, TypeError):
                as_of = datetime.datetime.utcnow()
        else:
            as_of = datetime.datetime.utcnow()

        return Quote(
            ticker=data.get("ticker", ""),
            status=status,
            price=data.get("price"),
            previous_close=data.get("previous_close"),
            change_value=data.get("change_value"),
            change_percent=data.get("change_percent"),
            volume=data.get("volume"),
            market_cap=data.get("market_cap"),
            open_price=data.get("open_price"),
            high_price=data.get("high_price"),
            low_price=data.get("low_price"),
            pe_ratio=data.get("pe_ratio"),
            pb_ratio=data.get("pb_ratio"),
            roe=data.get("roe"),
            eps=data.get("eps"),
            currency=data.get("currency"),
            exchange=data.get("exchange"),
            sector=data.get("sector"),
            country=data.get("country"),
            provider=data.get("provider"),
            as_of=as_of,
        )

    def update_cache(self, quotes: List[Quote]) -> Dict[str, Any]:
        """Update cache with new quotes"""
        updated_count = 0
        errors = []

        for quote in quotes:
            if quote.status == QuoteStatus.OK and quote.price is not None:
                try:
                    quote_data = quote.dict()
                    self.cache.set(quote.ticker, quote_data)
                    updated_count += 1
                except Exception as e:
                    errors.append(f"Failed to update {quote.ticker}: {e}")

        return {
            "updated_count": updated_count,
            "errors": errors,
            "total_cached": cache.get_item_count()
        }

# Global quote service instance
quote_service = QuoteService(cache, provider_manager)

# =============================================================================
# Enhanced Security & Auth
# =============================================================================

security = HTTPBearer(auto_error=False)
limiter = Limiter(key_func=get_remote_address)

def get_token_hash(token: str) -> str:
    """Hash token for secure logging"""
    return hashlib.sha256(token.encode()).hexdigest()[:16]

def verify_auth(
    request: Request,
    credentials: Optional[HTTPAuthorizationCredentials] = Depends(security)
) -> bool:
    """
    Enhanced authentication supporting both Bearer header and token query parameter
    """
    if not settings.require_auth:
        return True

    valid_tokens = [token for token in [settings.app_token, settings.backup_app_token] if token]
    
    if not valid_tokens:
        if settings.is_production:
            raise HTTPException(
                status_code=500,
                detail="Authentication misconfigured - no tokens available"
            )
        else:
            logger.warning("REQUIRE_AUTH is True but no valid tokens configured")
            return True

    # Check Authorization header first
    token = None
    if credentials:
        token = credentials.credentials.strip()
    
    # Fall back to query parameter
    if not token:
        token = request.query_params.get("token", "").strip()
    
    if not token:
        logger.warning("Authentication required but no token provided")
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Missing authentication token",
        )

    # Token validation
    if token not in valid_tokens:
        token_hash = get_token_hash(token)
        logger.warning(f"Invalid token attempt: {token_hash}")
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid authentication token",
        )

    token_hash = get_token_hash(token)
    logger.info(f"Successful authentication for token: {token_hash}")
    return True

def rate_limit(rule: str):
    """Conditional rate limiting decorator"""
    if not settings.enable_rate_limiting:
        def noop_decorator(fn):
            return fn
        return noop_decorator
    
    return limiter.limit(rule)

# =============================================================================
# Custom Rate Limit Handler
# =============================================================================

async def custom_rate_limit_handler(request: Request, exc: RateLimitExceeded):
    """Custom rate limit handler with structured error response"""
    request_id = getattr(request.state, 'request_id', 'unknown')
    
    return JSONResponse(
        status_code=429,
        content=ErrorResponse(
            error="Rate limit exceeded",
            message="Too many requests. Please try again later.",
            detail={
                "limit": str(exc.limit),
                "retry_after": getattr(exc, 'retry_after', None),
            },
            timestamp=datetime.datetime.utcnow(),
            request_id=request_id,
        ).dict(),
        headers={"Retry-After": str(getattr(exc, 'retry_after', 60))}
    )

# =============================================================================
# FastAPI Application Setup
# =============================================================================

# Application start time for uptime calculation
APP_START_TIME = time.time()

@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    Enhanced application lifespan management
    """
    startup_time = time.time()
    
    try:
        # Validate configuration
        config_errors = settings.validate_configuration()
        if config_errors:
            error_msg = "Configuration errors:\n" + "\n".join(f"  - {e}" for e in config_errors)
            if settings.is_production:
                logger.error(f"❌ {error_msg}")
                raise RuntimeError("Production configuration invalid")
            else:
                logger.warning(f"⚠️ Configuration warnings in development:\n{error_msg}")
            
        settings.log_config_summary()
        
        # Initialize cache
        expired_count = cache.cleanup_expired()
        if expired_count:
            logger.info(f"🧹 Cleaned {expired_count} expired cache entries")
            
        logger.info("✅ Application startup completed successfully")
        
    except Exception as e:
        logger.error(f"❌ Application startup failed: {e}")
        if settings.is_production:
            raise

    yield

    # Shutdown cleanup
    try:
        # Flush cache to disk
        cache.flush()
        logger.info("✅ Cache flushed to disk")
        
        # Close HTTP client
        await http_client.close()
        logger.info("✅ HTTP client closed")

        # Close analysis engine sessions
        if analyzer is not None:
            try:
                await analyzer.close()
                logger.info("✅ Advanced analysis engine closed")
            except Exception as e:
                logger.warning(f"⚠️ Error closing analysis engine: {e}")
        
    except Exception as e:
        logger.warning(f"⚠️ Error during shutdown: {e}")

    uptime = time.time() - startup_time
    logger.info(f"🛑 Application shutdown after {uptime:.2f} seconds")

# Create FastAPI application
app = FastAPI(
    title=settings.service_name,
    version=settings.service_version,
    description="""
    Enhanced Tadawul Stock Analysis API with comprehensive financial data integration
    and AI-powered trading analysis.

    ## Features
    
    * 📈 Real-time stock quotes from multiple financial APIs
    * 🤖 Advanced multi-source AI analysis & recommendations
    * 💾 Advanced caching with TTL and batched persistence
    * 🔐 Comprehensive security with token authentication
    * ⚡ Async/await for high performance
    * 🏥 Health monitoring and metrics
    * 📊 Support for fundamental data (P/E, P/B, ROE, EPS)
    
    ## Authentication
    
    Most endpoints require authentication via:
    - Authorization: Bearer <token>
    - OR token query parameter: ?token=<token>
    """,
    docs_url="/docs" if settings.enable_swagger else None,
    redoc_url="/redoc" if settings.enable_redoc else None,
    lifespan=lifespan,
)

# Configure CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.cors_origins,
    allow_credentials=True,
    allow_methods=["GET", "POST", "PUT", "DELETE", "OPTIONS"],
    allow_headers=["*"],
)

# Configure rate limiting
app.state.limiter = limiter
app.add_exception_handler(RateLimitExceeded, custom_rate_limit_handler)

# =============================================================================
# Enhanced Middleware
# =============================================================================

@app.middleware("http")
async def add_request_id(request: Request, call_next):
    """Add request ID to all requests for correlation"""
    request_id = str(uuid.uuid4())
    request.state.request_id = request_id
    
    start_time = time.time()
    response = await call_next(request)
    
    # Calculate processing time
    process_time = (time.time() - start_time) * 1000
    
    # Structured access logging
    logger.info(
        f"Request completed: {request.method} {request.url.path} "
        f"-> {response.status_code} [{process_time:.1f}ms] "
        f"[{request_id}]"
    )
    
    response.headers["X-Request-ID"] = request_id
    response.headers["X-Process-Time"] = f"{process_time:.1f}ms"
    
    return response

# =============================================================================
# Enhanced Exception Handlers
# =============================================================================

@app.exception_handler(HTTPException)
async def http_exception_handler(request: Request, exc: HTTPException):
    """Enhanced HTTP exception handler"""
    request_id = getattr(request.state, 'request_id', 'unknown')
    
    logger.warning(
        f"HTTP {exc.status_code} for {request.method} {request.url.path} "
        f"[{request_id}]: {exc.detail}"
    )
    
    return JSONResponse(
        status_code=exc.status_code,
        content=ErrorResponse(
            error=str(exc.detail),  # Convert to string for safety
            message=str(exc.detail),
            timestamp=datetime.datetime.utcnow(),
            request_id=request_id,
        ).dict(),
    )

@app.exception_handler(Exception)
async def global_exception_handler(request: Request, exc: Exception):
    """Global exception handler"""
    request_id = getattr(request.state, 'request_id', 'unknown')
    
    logger.error(
        f"Unhandled exception for {request.method} {request.url.path} "
        f"[{request_id}]: {exc}",
        exc_info=settings.debug
    )
    
    return JSONResponse(
        status_code=500,
        content=ErrorResponse(
            error="Internal server error",
            message=str(exc) if settings.debug else "An internal error occurred",
            timestamp=datetime.datetime.utcnow(),
            request_id=request_id,
        ).dict(),
    )

# =============================================================================
# Enhanced API Endpoints
# =============================================================================

@app.get("/", response_model=Dict[str, Any])
@rate_limit(f"{settings.max_requests_per_minute}/minute")
async def root(request: Request):
    """
    Root endpoint with comprehensive service information
    """
    cache_stats = cache.get_stats()
    
    return {
        "service": settings.service_name,
        "version": settings.service_version,
        "status": "operational",
        "environment": settings.environment,
        "timestamp": datetime.datetime.utcnow().isoformat() + "Z",
        "authentication_required": settings.require_auth,
        "rate_limiting": settings.enable_rate_limiting,
        "cache": {
            "total_items": cache_stats["total_items"],
            "valid_items": cache_stats["valid_items"],
            "hit_rate": cache_stats["performance"]["hit_rate"],
        },
        "documentation": "/docs" if settings.enable_swagger else None,
        "endpoints": {
            "health": "/health",
            "ping": "/ping",
            "quotes_v1": "/v1/quote",
            "quotes_v41": "/v41/quotes",
            "cache_info": "/v1/cache/info",
            "analysis_multi_source": "/v1/analysis/multi-source",
            "analysis_recommendation": "/v1/analysis/recommendation",
        }
    }

@app.get("/health", response_model=HealthResponse)
@rate_limit("30/minute")
async def health_check(request: Request):
    """
    Enhanced health check with dependency status
    """
    # Cache status
    cache_stats = cache.get_stats()

    # Analysis engine cache info (best-effort)
    analysis_cache_info: Optional[Dict[str, Any]] = None
    analysis_status = False

    if analyzer is not None:
        try:
            analysis_cache_info = await analyzer.get_cache_info()
            analysis_status = True
        except Exception as e:
            logger.warning(f"⚠️ Analysis engine cache info failed: {e}")
            analysis_cache_info = {"error": str(e)}
            analysis_status = False

    dependencies = {
        "cache": {
            "status": True,
            "mode": "memory_persistent",
            "items": cache_stats["total_items"],
            "hit_rate": cache_stats["performance"]["hit_rate"],
        },
        "google_services": {
            "status": settings.has_google_sheets_access,
            "modes": {
                "direct_sheets": bool(settings.google_sheets_credentials),
                "apps_script": bool(settings.google_apps_script_url),
            }
        },
        "financial_apis": {
            "status": len(provider_manager.enabled_providers) > 0,
            "providers": [p.name for p in provider_manager.enabled_providers],
            "count": len(provider_manager.enabled_providers),
        },
        "analysis_engine": {
            "status": analysis_status,
            "available": analyzer is not None,
            "cache": analysis_cache_info,
        }
    }

    # Determine overall status
    critical_services = [
        dependencies["financial_apis"]["status"],
        dependencies["cache"]["status"],
    ]
    status = "healthy" if all(critical_services) else "degraded"

    return HealthResponse(
        status=status,
        version=settings.service_version,
        timestamp=datetime.datetime.utcnow(),
        uptime_seconds=time.time() - APP_START_TIME,
        dependencies=dependencies
    )

@app.get("/ping")
@rate_limit("120/minute")
async def ping(request: Request):
    """
    Simple ping endpoint for connectivity testing
    """
    return {
        "status": "ok",
        "service": settings.service_name,
        "version": settings.service_version,
        "timestamp": datetime.datetime.utcnow().isoformat(),
        "request_id": getattr(request.state, 'request_id', 'unknown'),
    }

@app.get("/v1/quote", response_model=QuotesResponse)
@rate_limit("60/minute")
async def get_quotes_v1(
    request: Request,
    tickers: str = Query(..., description="Comma-separated ticker symbols"),
    auth: bool = Depends(verify_auth)
):
    """
    Get quotes for multiple symbols with cache support
    
    - Supports up to 100 symbols per request
    - Returns standardized quote format
    - Uses cache with fallback to providers
    - Includes comprehensive metadata
    - Enforces an overall timeout for upstream providers
    """
    try:
        symbols = [t.strip().upper() for t in tickers.split(",") if t.strip()]
        
        if not symbols:
            raise HTTPException(status_code=400, detail="No tickers provided")
            
        if len(symbols) > 100:
            raise HTTPException(status_code=400, detail="Too many tickers (max 100)")

        OVERALL_TIMEOUT_SECONDS = 12.0

        try:
            response = await asyncio.wait_for(
                quote_service.get_quotes(symbols),
                timeout=OVERALL_TIMEOUT_SECONDS,
            )
            return response

        except asyncio.TimeoutError:
            # Build a graceful timeout response instead of hanging
            now = datetime.datetime.utcnow()
            quotes = [
                Quote(
                    ticker=sym,
                    status=QuoteStatus.ERROR,
                    message=(
                        "Timeout while contacting data providers. "
                        "Please try again; if this persists, reduce the number of symbols."
                    ),
                )
                for sym in symbols
            ]

            return QuotesResponse(
                timestamp=now,
                symbols=quotes,
                meta={
                    "timeout": True,
                    "timeout_seconds": OVERALL_TIMEOUT_SECONDS,
                    "total_symbols": len(symbols),
                    "successful_quotes": 0,
                },
            )
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error fetching quotes: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")

@app.get("/v41/quotes", response_model=QuotesResponse)
@rate_limit("60/minute")
async def get_quotes_v41(
    request: Request,
    symbols: str = Query(..., description="Comma-separated ticker symbols"),
    auth: bool = Depends(verify_auth)
):
    """
    v4.1 compatible quotes endpoint
    
    - Compatible with existing v4.1 clients
    - Uses the same underlying service as v1
    - Returns standardized quote format
    - Includes v4.1 compatibility metadata
    - Enforces an overall timeout for upstream providers
    """
    try:
        tickers = [t.strip().upper() for t in symbols.split(",") if t.strip()]
        
        if not tickers:
            raise HTTPException(status_code=400, detail="No symbols provided")

        OVERALL_TIMEOUT_SECONDS = 12.0

        try:
            response = await asyncio.wait_for(
                quote_service.get_quotes(tickers),
                timeout=OVERALL_TIMEOUT_SECONDS,
            )
        except asyncio.TimeoutError:
            now = datetime.datetime.utcnow()
            quotes = [
                Quote(
                    ticker=sym,
                    status=QuoteStatus.ERROR,
                    message=(
                        "Timeout while contacting data providers. "
                        "Please try again; if this persists, reduce the number of symbols."
                    ),
                )
                for sym in tickers
            ]

            response = QuotesResponse(
                timestamp=now,
                symbols=quotes,
                meta={
                    "timeout": True,
                    "timeout_seconds": OVERALL_TIMEOUT_SECONDS,
                    "total_symbols": len(tickers),
                    "successful_quotes": 0,
                },
            )
        
        # Add v4.1 specific metadata
        if response.meta is None:
            response.meta = {}
        response.meta["compatibility"] = "v4.1"
        response.meta["api_version"] = "4.1"
            
        return response
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error in v41 quotes: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")

@app.post("/v1/quote/update")
@rate_limit("30/minute")
async def update_quotes(
    request: QuoteRequest,
    auth: bool = Depends(verify_auth)
):
    """
    Update quotes in cache with fresh data from providers
    
    - Fetches fresh data for specified symbols
    - Updates cache with new data
    - Returns update statistics
    """
    try:
        # Fetch fresh quotes for the symbols
        response = await quote_service.get_quotes(request.symbols)
        
        # Return refresh statistics
        result = {
            "status": "success",
            "refreshed_symbols": len(request.symbols),
            "successful_quotes": len([q for q in response.symbols if q.status == QuoteStatus.OK]),
            "cache_size": cache.get_item_count(),
            "timestamp": response.timestamp.isoformat()
        }
        
        logger.info(f"Refreshed {len(request.symbols)} quotes in cache")
        
        return result
        
    except Exception as e:
        logger.error(f"Error updating quotes: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")

@app.get("/v1/cache/info")
@rate_limit("20/minute")
async def get_cache_info(
    request: Request,
    auth: bool = Depends(verify_auth)
):
    """
    Get comprehensive cache information and statistics
    
    - Current cache size and composition
    - Performance metrics (hit rate, updates, etc.)
    - Storage information
    """
    return cache.get_stats()

@app.post("/v1/cache/cleanup")
@rate_limit("5/minute")
async def cleanup_cache(
    request: Request,
    auth: bool = Depends(verify_auth)
):
    """
    Clean up expired cache entries
    
    - Removes expired entries
    - Returns cleanup statistics
    - Forces immediate cache save
    """
    expired_count = cache.cleanup_expired()
    cache.flush()  # Force save after cleanup
    
    return {
        "status": "success",
        "expired_removed": expired_count,
        "remaining_items": cache.get_item_count(),
        "timestamp": datetime.datetime.utcnow().isoformat() + "Z",
    }

@app.post("/v1/cache/flush")
@rate_limit("5/minute")
async def flush_cache(
    request: Request,
    auth: bool = Depends(verify_auth)
):
    """
    Force immediate cache save to disk
    
    - Useful for ensuring persistence after important updates
    - Returns save status
    """
    success = cache.flush()
    
    return {
        "status": "success" if success else "error",
        "message": "Cache flushed to disk" if success else "Failed to flush cache",
        "timestamp": datetime.datetime.utcnow().isoformat() + "Z",
    }

# =============================================================================
# New: Advanced Analysis Endpoints
# =============================================================================

@app.get("/v1/analysis/multi-source", response_model=Dict[str, Any])
@rate_limit("30/minute")
async def analysis_multi_source(
    request: Request,
    symbol: str = Query(..., description="Single ticker symbol for multi-source analysis"),
    auth: bool = Depends(verify_auth),
):
    """
    Multi-source analysis endpoint.

    - Aggregates real-time prices from all configured APIs
    - Includes consolidation metrics (avg, std, dispersion, confidence)
    - Optionally pulls extra data from Google Apps Script if configured
    """
    if analyzer is None:
        raise HTTPException(
            status_code=503,
            detail="Analysis engine is not available on this instance"
        )

    try:
        result = await get_multi_source_analysis(symbol)
        return result
    except Exception as e:
        logger.error(f"Error in multi-source analysis for {symbol}: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")

@app.get("/v1/analysis/recommendation", response_model=Dict[str, Any])
@rate_limit("20/minute")
async def analysis_recommendation(
    request: Request,
    symbol: str = Query(..., description="Single ticker symbol for AI trading recommendation"),
    auth: bool = Depends(verify_auth),
):
    """
    AI trading recommendation endpoint.

    - Combines technical indicators, fundamentals, and sentiment
    - Returns overall score, sub-scores and risk assessment
    - Provides suggested entry, stop-loss, and price targets
    """
    if analyzer is None:
        raise HTTPException(
            status_code=503,
            detail="Analysis engine is not available on this instance"
        )

    try:
        rec = await generate_ai_recommendation(symbol)
        # rec is an AIRecommendation dataclass; convert to dict for JSON response
        return rec.to_dict()
    except Exception as e:
        logger.error(f"Error generating AI recommendation for {symbol}: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")

@app.get("/v1/analysis/cache/info", response_model=Dict[str, Any])
@rate_limit("10/minute")
async def analysis_cache_info(
    request: Request,
    auth: bool = Depends(verify_auth),
):
    """
    Get cache information for the analysis engine.

    - File cache + optional Redis info
    - TTL and size configuration
    """
    if analyzer is None:
        raise HTTPException(
            status_code=503,
            detail="Analysis engine is not available on this instance"
        )

    try:
        info = await analyzer.get_cache_info()
        return info
    except Exception as e:
        logger.error(f"Error getting analysis cache info: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")

@app.post("/v1/analysis/cache/clear", response_model=Dict[str, Any])
@rate_limit("5/minute")
async def analysis_cache_clear(
    request: Request,
    symbol: Optional[str] = Query(
        None,
        description="Optional single symbol to clear; if omitted, clears all analysis cache entries",
    ),
    auth: bool = Depends(verify_auth),
):
    """
    Clear analysis engine cache.

    - If `symbol` is provided, clears only entries related to that symbol
    - If not provided, clears all analyzer cache entries (Redis + file)
    """
    if analyzer is None:
        raise HTTPException(
            status_code=503,
            detail="Analysis engine is not available on this instance"
        )

    try:
        result = await analyzer.clear_cache(symbol)
        return result
    except Exception as e:
        logger.error(f"Error clearing analysis cache: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")

# =============================================================================
# Application Entry Point
# =============================================================================

def main():
    """Application entry point with enhanced startup banner"""
    logger.info("=" * 70)
    logger.info(f"🚀 {settings.service_name} v{settings.service_version}")
    logger.info(f"🌍 Environment: {settings.environment} (Production: {settings.is_production})")
    logger.info(f"🔐 Authentication: {'Enabled' if settings.require_auth else 'Disabled'}")
    logger.info(f"⚡ Rate Limiting: {'Enabled' if settings.enable_rate_limiting else 'Disabled'}")
    logger.info(f"📊 Google Services: {'Available' if settings.has_google_sheets_access else 'Not Available'}")
    logger.info(f"💾 Cache: {cache.get_item_count()} items, Save Interval: {settings.cache_save_interval}s")
    logger.info(f"📈 Financial APIs: {len(provider_manager.enabled_providers)} enabled")
    logger.info(f"🤖 Analysis Engine: {'Available' if analyzer is not None else 'Not Available'}")
    logger.info(f"🔧 Debug Mode: {settings.debug}")
    logger.info(f"🌐 Starting server on {settings.app_host}:{settings.app_port}")
    logger.info("=" * 70)
    
    uvicorn.run(
        "main:app",
        host=settings.app_host,
        port=settings.app_port,
        reload=settings.environment == "development",
        log_level="info"
    )

if __name__ == "__main__":
    main()
