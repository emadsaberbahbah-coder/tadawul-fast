#!/usr/bin/env python3
"""
Tadawul Stock Analysis API - Refactored Version 3.0.0
Single-file implementation with modular architecture and service layer
"""

import asyncio
import datetime
import json
import logging
import os
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
from pydantic import BaseModel, Field, validator
from pydantic_settings import BaseSettings
from slowapi import Limiter, _rate_limit_exceeded_handler
from slowapi.errors import RateLimitExceeded
from slowapi.util import get_remote_address

# =============================================================================
# Configuration
# =============================================================================

load_dotenv()

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

    # Logging
    log_level: str = Field("INFO", env="LOG_LEVEL")
    log_format: str = Field("detailed", env="LOG_FORMAT")
    log_enable_file: bool = Field(False, env="LOG_ENABLE_FILE")

    class Config:
        env_file = ".env"
        case_sensitive = False
        extra = "ignore"

    def validate_urls(self) -> bool:
        """Validate critical URLs at startup"""
        urls_to_validate = [
            ("GOOGLE_APPS_SCRIPT_URL", self.google_apps_script_url),
        ]
        
        for name, url in urls_to_validate:
            if not url or url.strip() in ["", "undefined"]:
                logging.error(f"❌ Invalid URL provided for {name}: {url}")
                return False
                
            if not url.startswith(('http://', 'https://')):
                logging.error(f"❌ Invalid URL format for {name}: {url}")
                return False
                
        logging.info("✅ All URLs validated successfully")
        return True

    def log_config_summary(self):
        """Log configuration summary without sensitive data"""
        logging.info(f"🔧 {self.service_name} v{self.service_version}")
        logging.info(f"🌍 Environment: {self.environment}")
        logging.info(f"🔐 Auth: {'Enabled' if self.require_auth else 'Disabled'}")
        logging.info(f"⚡ Rate Limiting: {'Enabled' if self.enable_rate_limiting else 'Disabled'}")
        logging.info(f"📊 Google Sheets: {'Direct' if self.google_sheets_credentials else 'Apps Script'}")
        logging.info(f"💾 Cache TTL: {self.cache_default_ttl}s")
        
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
    """Configure structured logging"""
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
    """Health check response"""
    status: str = Field(..., description="Service status")
    version: str = Field(..., description="API version")
    timestamp: datetime.datetime = Field(..., description="Current timestamp")
    uptime_seconds: float = Field(..., description="Service uptime")
    dependencies: Dict[str, bool] = Field(..., description="Dependency status")

class QuoteRequest(BaseModel):
    """Quote request model"""
    symbols: List[str] = Field(..., description="List of symbols to fetch")
    cache_ttl: Optional[int] = Field(300, description="Cache TTL in seconds")
    providers: Optional[List[str]] = Field(None, description="Preferred providers")

# =============================================================================
# Cache Implementation
# =============================================================================

class QuoteCache(Protocol):
    """Cache interface for quote storage"""
    def get(self, symbol: str) -> Optional[Dict[str, Any]]: ...
    def set(self, symbol: str, quote: Dict[str, Any], ttl: int) -> None: ...
    def cleanup_expired(self) -> int: ...
    def get_stats(self) -> Dict[str, Any]: ...

class TTLCache:
    """Enhanced TTL cache implementation"""
    
    def __init__(self, ttl_seconds: int = None, max_size: int = None):
        self.cache_path = Path(__file__).parent / "quote_cache.json"
        self.backup_dir = Path(__file__).parent / "cache_backups"
        self.backup_dir.mkdir(parents=True, exist_ok=True)
        
        self.data: Dict[str, Dict[str, Any]] = {}
        self.ttl = ttl_seconds or settings.cache_default_ttl
        self.max_size = max_size or settings.cache_max_size
        
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
        """Load cache from disk"""
        try:
            if not self.cache_path.exists():
                logger.info("No cache file found, starting with empty cache")
                return True

            with open(self.cache_path, "r", encoding="utf-8") as f:
                raw_data = json.load(f)

            now = time.time()
            valid_items: Dict[str, Dict[str, Any]] = {}
            
            # Handle different cache formats
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

    def save_cache(self) -> bool:
        """Save cache to disk atomically"""
        try:
            if len(self.data) > self.max_size:
                self._enforce_size_limit()
                
            cache_data = {
                "metadata": {
                    "version": "1.2",
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
            return True
            
        except Exception as e:
            logger.error(f"❌ Failed to save cache: {e}")
            self.metrics["errors"] += 1
            return False

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
        """Get quote from cache"""
        symbol = symbol.upper().strip()
        
        if symbol in self.data and self._is_valid(self.data[symbol]):
            self.metrics["hits"] += 1
            # Remove cache metadata before returning
            clean_data = {k: v for k, v in self.data[symbol].items() 
                         if not k.startswith("_")}
            return clean_data
        
        # Remove expired entry
        if symbol in self.data:
            del self.data[symbol]
            self.save_cache()
            
        self.metrics["misses"] += 1
        return None

    def set(self, symbol: str, quote: Dict[str, Any], ttl: int = None) -> None:
        """Set quote in cache"""
        symbol = symbol.upper().strip()
        quote_data = quote.copy()
        quote_data["_cache_timestamp"] = time.time()
        quote_data["_last_updated"] = datetime.datetime.utcnow().isoformat() + "Z"
        
        self.data[symbol] = quote_data
        self.metrics["updates"] += 1
        self.save_cache()

    def cleanup_expired(self) -> int:
        """Clean up expired entries"""
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
            logger.info(f"🧹 Removed {len(expired_tickers)} expired cache entries")
            
        return len(expired_tickers)

    def get_stats(self) -> Dict[str, Any]:
        """Get cache statistics"""
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
            "ttl_seconds": self.ttl,
            "performance": {
                "hits": self.metrics["hits"],
                "misses": self.metrics["misses"],
                "hit_rate": round(hit_rate, 2),
                "updates": self.metrics["updates"],
                "expired_removals": self.metrics["expired_removals"],
                "errors": self.metrics["errors"],
            }
        }

# Global cache instance
cache = TTLCache()

# =============================================================================
# Provider Abstraction
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
        self.rate_limit = 5  # requests per minute
        self.base_url = "https://www.alphavantage.co/query"
        self.api_key = settings.alpha_vantage_api_key
        self.enabled = bool(self.api_key)

    async def get_quote(self, symbol: str) -> Optional[Dict[str, Any]]:
        if not self.enabled:
            return None

        try:
            async with aiohttp.ClientSession() as session:
                params = {
                    "function": "GLOBAL_QUOTE",
                    "symbol": symbol,
                    "apikey": self.api_key,
                }
                async with session.get(self.base_url, params=params) as response:
                    if response.status == 200:
                        data = await response.json()
                        return self._parse_response(data, symbol)
        except Exception as e:
            logger.error(f"Alpha Vantage error for {symbol}: {e}")
            
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

        try:
            async with aiohttp.ClientSession() as session:
                params = {"symbol": symbol, "token": self.api_key}
                async with session.get(f"{self.base_url}/quote", params=params) as response:
                    if response.status == 200:
                        data = await response.json()
                        return self._parse_response(data, symbol)
        except Exception as e:
            logger.error(f"Finnhub error for {symbol}: {e}")
            
        return None

    def _parse_response(self, data: Dict[str, Any], symbol: str) -> Optional[Dict[str, Any]]:
        if not data or "c" not in data:
            return None

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
        }

class ProviderManager:
    """Manager for provider selection and fallback"""
    
    def __init__(self):
        self.providers: List[ProviderClient] = [
            AlphaVantageProvider(),
            FinnhubProvider(),
        ]
        
        self.enabled_providers = [p for p in self.providers if p.enabled]
        logger.info(f"✅ Enabled providers: {[p.name for p in self.enabled_providers]}")

    async def get_quote(self, symbol: str) -> Optional[Dict[str, Any]]:
        """Get quote from providers with fallback"""
        for provider in self.enabled_providers:
            try:
                quote = await provider.get_quote(symbol)
                if quote and quote.get("price"):
                    logger.info(f"✅ Got quote for {symbol} from {provider.name}")
                    return quote
            except Exception as e:
                logger.warning(f"Provider {provider.name} failed for {symbol}: {e}")
                continue
                
        logger.warning(f"❌ All providers failed for {symbol}")
        return None

    async def get_quotes(self, symbols: List[str]) -> List[Dict[str, Any]]:
        """Get multiple quotes in parallel"""
        tasks = [self.get_quote(symbol) for symbol in symbols]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        quotes = []
        for result in results:
            if isinstance(result, dict):
                quotes.append(result)
            elif isinstance(result, Exception):
                logger.error(f"Quote fetch error: {result}")
                
        return quotes

# Global provider manager
provider_manager = ProviderManager()

# =============================================================================
# Quote Service Layer
# =============================================================================

class QuoteService:
    """Service layer for quote management"""
    
    def __init__(self, cache, provider_manager):
        self.cache = cache
        self.provider_manager = provider_manager

    async def get_quote(self, symbol: str) -> Quote:
        """Get single quote with cache fallback"""
        # Try cache first
        cached_quote = self.cache.get(symbol)
        if cached_quote:
            logger.debug(f"Cache hit for {symbol}")
            return self._create_quote_model(cached_quote, QuoteStatus.OK)

        # Fetch from provider
        provider_data = await self.provider_manager.get_quote(symbol)
        if provider_data:
            # Cache the result
            self.cache.set(symbol, provider_data)
            return self._create_quote_model(provider_data, QuoteStatus.OK)

        # No data available
        return Quote(
            ticker=symbol,
            status=QuoteStatus.NO_DATA,
            message="No data available from providers"
        )

    async def get_quotes(self, symbols: List[str]) -> QuotesResponse:
        """Get multiple quotes with efficient caching"""
        quotes = []
        cache_hits = 0
        cache_misses = 0
        sources_used = set()

        # Check cache first
        for symbol in symbols:
            cached_quote = self.cache.get(symbol)
            if cached_quote:
                cache_hits += 1
                quotes.append(self._create_quote_model(cached_quote, QuoteStatus.OK))
                if cached_quote.get("provider"):
                    sources_used.add(cached_quote["provider"])
            else:
                cache_misses += 1
                quotes.append(Quote(
                    ticker=symbol,
                    status=QuoteStatus.NO_DATA,
                    message="Not in cache"
                ))

        # Fetch missing quotes from providers
        missing_symbols = [symbol for symbol in symbols 
                          if symbol not in [q.ticker for q in quotes if q.status == QuoteStatus.OK]]
        
        if missing_symbols:
            provider_quotes = await self.provider_manager.get_quotes(missing_symbols)
            
            for provider_quote in provider_quotes:
                if provider_quote and provider_quote.get("price"):
                    symbol = provider_quote["ticker"]
                    # Update cache
                    self.cache.set(symbol, provider_quote)
                    # Update quote in response
                    for i, quote in enumerate(quotes):
                        if quote.ticker == symbol:
                            quotes[i] = self._create_quote_model(provider_quote, QuoteStatus.OK)
                            sources_used.add(provider_quote.get("provider", "unknown"))
                            break

        # Prepare metadata
        meta = {
            "cache_hits": cache_hits,
            "cache_misses": cache_misses,
            "sources": list(sources_used),
            "total_symbols": len(symbols),
            "successful_quotes": len([q for q in quotes if q.status == QuoteStatus.OK])
        }

        return QuotesResponse(
            timestamp=datetime.datetime.utcnow(),
            symbols=quotes,
            meta=meta
        )

    def _create_quote_model(self, data: Dict[str, Any], status: QuoteStatus) -> Quote:
        """Create Quote model from provider data"""
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
            currency=data.get("currency"),
            exchange=data.get("exchange"),
            sector=data.get("sector"),
            country=data.get("country"),
            provider=data.get("provider"),
            as_of=datetime.datetime.utcnow(),
        )

    def update_cache(self, quotes: List[Quote]) -> Dict[str, Any]:
        """Update cache with new quotes"""
        updated_count = 0
        errors = []

        for quote in quotes:
            if quote.status == QuoteStatus.OK and quote.price:
                try:
                    quote_data = quote.dict()
                    self.cache.set(quote.ticker, quote_data)
                    updated_count += 1
                except Exception as e:
                    errors.append(f"Failed to update {quote.ticker}: {e}")

        return {
            "updated_count": updated_count,
            "errors": errors,
            "total_cached": len(self.cache.data)
        }

# Global quote service instance
quote_service = QuoteService(cache, provider_manager)

# =============================================================================
# Security & Rate Limiting
# =============================================================================

security = HTTPBearer(auto_error=False)
limiter = Limiter(key_func=get_remote_address)

def verify_auth(credentials: Optional[HTTPAuthorizationCredentials] = Depends(security)):
    """
    Authentication dependency
    """
    if not settings.require_auth:
        return True

    valid_tokens = [token for token in [settings.app_token, settings.backup_app_token] if token]
    
    if not valid_tokens:
        logger.warning("REQUIRE_AUTH is True but no valid tokens configured")
        return True

    if not credentials:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Missing authentication token",
        )

    token = credentials.credentials.strip()
    if not token or token not in valid_tokens:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid authentication token",
        )

    logger.info(f"Successful authentication for token: {token[:8]}...")
    return True

def rate_limit(rule: str):
    """Conditional rate limiting decorator"""
    if not settings.enable_rate_limiting:
        def noop_decorator(fn):
            return fn
        return noop_decorator
    
    return limiter.limit(rule)

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
        if not settings.validate_urls():
            logger.error("❌ URL validation failed")
            raise RuntimeError("Invalid configuration URLs")
            
        settings.log_config_summary()
        
        # Initialize cache
        expired_count = cache.cleanup_expired()
        if expired_count:
            logger.info(f"🧹 Cleaned {expired_count} expired cache entries")
            
        logger.info("✅ Application startup completed successfully")
        
    except Exception as e:
        logger.error(f"❌ Application startup failed: {e}")
        if settings.environment == "production":
            raise

    yield

    # Shutdown cleanup
    logger.info("🛑 Application shutting down")

# Create FastAPI application
app = FastAPI(
    title=settings.service_name,
    version=settings.service_version,
    description="Enhanced Tadawul Stock Analysis API with modular architecture",
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
app.add_exception_handler(RateLimitExceeded, _rate_limit_exceeded_handler)

# =============================================================================
# Request ID Middleware
# =============================================================================

@app.middleware("http")
async def add_request_id(request: Request, call_next):
    """Add request ID to all requests for correlation"""
    request_id = str(uuid.uuid4())
    request.state.request_id = request_id
    
    response = await call_next(request)
    response.headers["X-Request-ID"] = request_id
    
    return response

# =============================================================================
# Exception Handlers
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
            error=exc.detail,
            message=exc.detail,
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
# API Endpoints
# =============================================================================

@app.get("/", response_model=Dict[str, Any])
@rate_limit(f"{settings.max_requests_per_minute}/minute")
async def root(request: Request):
    """
    Root endpoint with service information
    """
    return {
        "service": settings.service_name,
        "version": settings.service_version,
        "status": "operational",
        "environment": settings.environment,
        "timestamp": datetime.datetime.utcnow().isoformat() + "Z",
        "documentation": "/docs" if settings.enable_swagger else None,
        "endpoints": {
            "health": "/health",
            "quotes": "/v1/quote",
            "v41_quotes": "/v41/quotes",
        }
    }

@app.get("/health", response_model=HealthResponse)
@rate_limit("30/minute")
async def health_check(request: Request):
    """
    Health check endpoint with dependency status
    """
    # Check dependencies
    dependencies = {
        "cache": True,
        "google_sheets": bool(settings.google_sheets_credentials),
        "financial_apis": len(provider_manager.enabled_providers) > 0,
    }

    # Determine overall status
    critical_services = [dependencies["financial_apis"]]
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
        "timestamp": datetime.datetime.utcnow().isoformat()
    }

@app.get("/v1/quote", response_model=QuotesResponse)
@rate_limit("60/minute")
async def get_quotes(
    request: Request,
    tickers: str = Query(..., description="Comma-separated ticker symbols"),
    auth: bool = Depends(verify_auth)
):
    """
    Get quotes for multiple symbols with cache support
    """
    try:
        symbols = [t.strip().upper() for t in tickers.split(",") if t.strip()]
        
        if not symbols:
            raise HTTPException(status_code=400, detail="No tickers provided")
            
        if len(symbols) > 100:
            raise HTTPException(status_code=400, detail="Too many tickers (max 100)")

        return await quote_service.get_quotes(symbols)
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error fetching quotes: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")

@app.get("/v41/quotes", response_model=QuotesResponse)
@rate_limit("60/minute")
async def v41_get_quotes(
    request: Request,
    symbols: str = Query(..., description="Comma-separated tickers"),
    cache_ttl: int = Query(300, ge=0, le=3600),
    auth: bool = Depends(verify_auth)
):
    """
    Enhanced v4.1 compatible quotes endpoint
    """
    try:
        tickers = [t.strip().upper() for t in symbols.split(",") if t.strip()]
        
        if not tickers:
            raise HTTPException(status_code=400, detail="No symbols provided")

        response = await quote_service.get_quotes(tickers)
        
        # Add v4.1 specific metadata
        if response.meta:
            response.meta["cache_ttl"] = cache_ttl
            response.meta["compatibility"] = "v4.1"
            
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
    background_tasks: BackgroundTasks,
    auth: bool = Depends(verify_auth)
):
    """
    Update quotes in cache
    """
    try:
        # Fetch fresh quotes for the symbols
        response = await quote_service.get_quotes(request.symbols)
        
        # Update cache with fresh data
        result = quote_service.update_cache(response.symbols)
        
        logger.info(f"Updated {result['updated_count']} quotes in cache")
        
        return {
            "status": "success",
            "updated_count": result["updated_count"],
            "errors": result["errors"],
            "timestamp": response.timestamp.isoformat()
        }
        
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
    Get cache information and statistics
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
    """
    expired_count = cache.cleanup_expired()
    return {
        "status": "success",
        "expired_removed": expired_count,
        "remaining_items": len(cache.data),
        "timestamp": datetime.datetime.utcnow().isoformat() + "Z",
    }

# =============================================================================
# Application Entry Point
# =============================================================================

def main():
    """Application entry point"""
    logger.info("=" * 70)
    logger.info(f"🚀 {settings.service_name} v{settings.service_version}")
    logger.info(f"🌍 Environment: {settings.environment}")
    logger.info(f"🔐 Authentication: {'Enabled' if settings.require_auth else 'Disabled'}")
    logger.info(f"⚡ Rate Limiting: {'Enabled' if settings.enable_rate_limiting else 'Disabled'}")
    logger.info(f"📊 Google Sheets: {'Direct' if settings.google_sheets_credentials else 'Apps Script'}")
    logger.info(f"💾 Cache: {len(cache.data)} items loaded")
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
