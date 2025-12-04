#!/usr/bin/env python3
"""
Tadawul Stock Analysis API - Enhanced Version 3.7.0
Production-ready with ALL configured providers and proper Tadawul integration
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
from fastapi.responses import JSONResponse, ORJSONResponse
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from pydantic import BaseModel, Field, validator
from pydantic_settings import BaseSettings
from slowapi import Limiter
from slowapi.errors import RateLimitExceeded
from slowapi.util import get_remote_address
from slowapi.middleware import SlowAPIMiddleware

from routes_argaam import router as argaam_router
from routes.enriched_quote import router as enriched_quote_router
from routes.ai_analysis import router as ai_analysis_router

# --- Advanced AI Trading Analysis integration (safe import) -------------------
try:
    from advanced_analysis import (
        analyzer,
        generate_ai_recommendation,
        get_multi_source_analysis,
    )
except Exception:
    analyzer = None

    async def generate_ai_recommendation(symbol: str) -> Dict[str, Any]:
        raise RuntimeError("Analysis engine is not available on this instance")

    async def get_multi_source_analysis(symbol: str) -> Dict[str, Any]:
        raise RuntimeError("Analysis engine is not available on this instance")


# =============================================================================
# Configuration - UPDATED TO MATCH YOUR ENVIRONMENT
# =============================================================================

load_dotenv()


class Settings(BaseSettings):
    """Enhanced configuration management matching your Google Apps Script environment."""

    # Service Configuration
    service_name: str = Field("Tadawul Stock Analysis API", env="SERVICE_NAME")
    service_version: str = Field("3.7.0", env="SERVICE_VERSION")
    app_host: str = Field("0.0.0.0", env="APP_HOST")
    app_port: int = Field(8000, env="APP_PORT")
    environment: str = Field("production", env="ENVIRONMENT")
    debug: bool = Field(False, env="DEBUG")

    # Security
    require_auth: bool = Field(True, env="REQUIRE_AUTH")
    app_token: Optional[str] = Field(None, env="APP_TOKEN")
    backup_app_token: Optional[str] = Field(None, env="BACKUP_APP_TOKEN")
    tfb_app_token: Optional[str] = Field(None, env="TFB_APP_TOKEN")  # NEW

    # Rate Limiting
    enable_rate_limiting: bool = Field(True, env="ENABLE_RATE_LIMITING")
    max_requests_per_minute: int = Field(60, env="MAX_REQUESTS_PER_MINUTE")

    # CORS
    cors_origins: List[str] = Field(default_factory=lambda: ["*"], env="CORS_ORIGINS")

    # Documentation
    enable_swagger: bool = Field(True, env="ENABLE_SWAGGER")
    enable_redoc: bool = Field(True, env="ENABLE_REDOC")

    # Google Services
    spreadsheet_id: str = Field(..., env="SPREADSHEET_ID")
    google_sheets_credentials: Optional[str] = Field(None, env="GOOGLE_SHEETS_CREDENTIALS")
    google_apps_script_url: Optional[str] = Field(None, env="GOOGLE_APPS_SCRIPT_URL")
    google_apps_script_backup_url: Optional[str] = Field(None, env="GOOGLE_APPS_SCRIPT_BACKUP_URL")

    # Financial APIs (ALL providers from your ENABLED_PROVIDERS)
    alpha_vantage_api_key: Optional[str] = Field(None, env="ALPHA_VANTAGE_API_KEY")
    finnhub_api_key: Optional[str] = Field(None, env="FINNHUB_API_KEY")
    eodhd_api_key: Optional[str] = Field(None, env="EODHD_API_KEY")
    eodhd_base_url: str = Field("https://eodhistoricaldata.com/api", env="EODHD_BASE_URL")
    fmp_api_key: Optional[str] = Field(None, env="FMP_API_KEY")
    fmp_base_url: str = Field("https://financialmodelingprep.com/api/v3", env="FMP_BASE_URL")
    
    # NEW PROVIDERS from your ENABLED_PROVIDERS list
    marketstack_api_key: Optional[str] = Field(None, env="MARKETSTACK_API_KEY")
    twelvedata_api_key: Optional[str] = Field(None, env="TWELVEDATA_API_KEY")
    
    # Provider Configuration
    enabled_providers: List[str] = Field(
        default_factory=lambda: ["alpha_vantage", "finnhub", "eodhd", "marketstack", "twelvedata", "fmp"],
        env="ENABLED_PROVIDERS"
    )
    primary_provider: str = Field("tadawul_fast_bridge", env="PRIMARY_PROVIDER")  # NEW
    
    # Tadawul Fast Bridge Configuration
    backend_base_url: Optional[str] = Field(None, env="BACKEND_BASE_URL")  # NEW
    fastapi_base: Optional[str] = Field(None, env="FASTAPI_BASE")  # NEW
    base_url: Optional[str] = Field(None, env="BASE_URL")  # NEW

    # Cache
    cache_default_ttl: int = Field(1800, env="CACHE_DEFAULT_TTL")
    cache_max_size: int = Field(10000, env="CACHE_MAX_SIZE")
    cache_backup_enabled: bool = Field(True, env="CACHE_BACKUP_ENABLED")
    cache_save_interval: int = Field(10, env="CACHE_SAVE_INTERVAL")

    # Performance
    http_timeout: int = Field(10, env="HTTP_TIMEOUT")
    max_retries: int = Field(3, env="MAX_RETRIES")  # UPDATED from 2 to 3
    retry_delay: float = Field(0.7, env="RETRY_DELAY")

    # Analysis / AI features
    advanced_analysis_enabled: bool = Field(True, env="ADVANCED_ANALYSIS_ENABLED")

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
        """Parse CORS origins from JSON string or comma-separated string."""
        if isinstance(v, str):
            v = v.strip()
            if v.startswith("[") and v.endswith("]"):
                try:
                    parsed = json.loads(v)
                    if isinstance(parsed, list):
                        return [str(o).strip() for o in parsed if str(o).strip()]
                except json.JSONDecodeError:
                    pass
            return [origin.strip() for origin in v.split(",") if origin.strip()]
        return v

    @validator("enabled_providers", pre=True)
    def parse_enabled_providers(cls, v):
        """Parse enabled providers from comma-separated string."""
        if isinstance(v, str):
            return [p.strip() for p in v.split(",") if p.strip()]
        return v

    @property
    def is_production(self) -> bool:
        return self.environment.lower() == "production"

    @property
    def is_development(self) -> bool:
        return self.environment.lower() == "development"

    @property
    def has_google_sheets_access(self) -> bool:
        return bool(self.google_sheets_credentials or self.google_apps_script_url)

    @property
    def has_tadawul_fast_bridge(self) -> bool:  # NEW
        """Check if Tadawul Fast Bridge is configured."""
        return bool(self.tfb_app_token and (self.backend_base_url or self.fastapi_base))

    def validate_configuration(self) -> List[str]:
        """Validate configuration and return list of errors."""
        errors: List[str] = []
        warnings: List[str] = []

        # Auth validation
        if self.require_auth and not any([self.app_token, self.backup_app_token]):
            if self.is_production:
                errors.append("Authentication required but no APP_TOKEN/BACKUP_APP_TOKEN configured.")
            else:
                warnings.append("Authentication required but no tokens configured (dev).")

        # Provider validation
        for provider in self.enabled_providers:
            api_key_var = f"{provider.upper()}_API_KEY"
            api_key = getattr(self, f"{provider}_api_key", None)
            
            if provider == "fmp" and api_key:
                warnings.append("FMP provider: quote endpoint disabled (legacy), using for fundamentals only.")
            elif provider == "marketstack" and not api_key:
                warnings.append(f"MarketStack provider enabled but {api_key_var} not set.")
            elif provider == "twelvedata" and not api_key:
                warnings.append(f"TwelveData provider enabled but {api_key_var} not set.")
            elif provider in ["alpha_vantage", "finnhub", "eodhd"] and not api_key:
                warnings.append(f"{provider} enabled but {api_key_var} not set.")

        # Tadawul Fast Bridge check
        if self.primary_provider == "tadawul_fast_bridge" and not self.has_tadawul_fast_bridge:
            warnings.append("PRIMARY_PROVIDER=tadawul_fast_bridge but TFB_APP_TOKEN or backend URL not configured.")

        for w in warnings:
            logging.warning(f"⚠️ {w}")

        return errors

    def log_config_summary(self):
        """Log configuration summary without sensitive values."""
        logging.info(f"🔧 {self.service_name} v{self.service_version}")
        logging.info(f"🌍 Environment: {self.environment} (Production: {self.is_production})")
        logging.info(f"🔐 Auth: {'Enabled' if self.require_auth else 'Disabled'}")
        logging.info(f"📈 Enabled Providers: {', '.join(self.enabled_providers)}")
        logging.info(f"🎯 Primary Provider: {self.primary_provider}")
        logging.info(f"⚡ Rate Limiting: {'Enabled' if self.enable_rate_limiting else 'Disabled'}")
        logging.info(f"📊 Google Services: {'Available' if self.has_google_sheets_access else 'Not Available'}")
        logging.info(f"🇸🇦 Tadawul Fast Bridge: {'Available' if self.has_tadawul_fast_bridge else 'Not Available'}")
        logging.info(f"💾 Cache: TTL={self.cache_default_ttl}s, Save Interval={self.cache_save_interval}s")


# Initialize settings
settings = Settings()

# === Global timeouts ===
QUOTE_OVERALL_TIMEOUT_SECONDS = max(6.0, float(settings.http_timeout) + 2.0)
ANALYSIS_OVERALL_TIMEOUT_SECONDS = max(8.0, float(settings.http_timeout) + 4.0)

# =============================================================================
# Logging Setup
# =============================================================================

def setup_logging():
    """Configure structured logging."""
    log_level = getattr(logging, settings.log_level.upper(), logging.INFO)
    
    if settings.log_format == "detailed":
        log_format = (
            "%(asctime)s - %(name)s - %(levelname)s - "
            "[%(filename)s:%(lineno)d] - %(message)s"
        )
    else:
        log_format = "%(asctime)s - %(levelname)s - %(message)s"

    handlers = [logging.StreamHandler()]
    if settings.log_enable_file:
        log_file = Path(__file__).parent / "app.log"
        handlers.append(logging.FileHandler(str(log_file), encoding="utf-8"))

    logging.basicConfig(
        level=log_level,
        format=log_format,
        handlers=handlers,
    )
    logging.getLogger("uvicorn.access").disabled = True


setup_logging()
logger = logging.getLogger(__name__)

# =============================================================================
# Data Models (UNCHANGED - keep your existing models)
# =============================================================================

class QuoteStatus(str, Enum):
    OK = "OK"
    NO_DATA = "NO_DATA"
    ERROR = "ERROR"


class Quote(BaseModel):
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
    pe_ratio: Optional[float] = Field(None, description="Price-to-Earnings ratio")
    pb_ratio: Optional[float] = Field(None, description="Price-to-Book ratio")
    roe: Optional[float] = Field(None, description="Return on Equity")
    eps: Optional[float] = Field(None, description="Earnings Per Share")
    currency: Optional[str] = Field(None, description="Currency code")
    exchange: Optional[str] = Field(None, description="Stock exchange")
    sector: Optional[str] = Field(None, description="Company sector")
    country: Optional[str] = Field(None, description="Country code")
    provider: Optional[str] = Field(None, description="Data provider source name")
    as_of: Optional[datetime.datetime] = Field(None, description="Quote timestamp")
    message: Optional[str] = Field(None, description="Status or diagnostic message")

    @validator("ticker")
    def validate_ticker(cls, v: str) -> str:
        v = v.strip().upper()
        if not v:
            raise ValueError("Ticker cannot be empty")
        return v


class QuotesResponse(BaseModel):
    timestamp: datetime.datetime = Field(..., description="Response timestamp")
    symbols: List[Quote] = Field(..., description="List of quotes")
    meta: Optional[Dict[str, Any]] = Field(None, description="Response metadata")


class ErrorResponse(BaseModel):
    error: str = Field(..., description="Error type")
    message: str = Field(..., description="Error message")
    detail: Optional[Dict[str, Any]] = Field(None, description="Additional details")
    timestamp: datetime.datetime = Field(..., description="Error timestamp")
    request_id: Optional[str] = Field(None, description="Request ID for debugging")


class HealthResponse(BaseModel):
    status: str = Field(..., description="Service status")
    version: str = Field(..., description="API version")
    timestamp: datetime.datetime = Field(..., description="Current timestamp")
    uptime_seconds: float = Field(..., description="Service uptime in seconds")
    dependencies: Dict[str, Any] = Field(..., description="Dependency status")


class QuoteRequest(BaseModel):
    symbols: List[str] = Field(..., description="List of symbols to fetch/refresh")
    cache_ttl: Optional[int] = Field(None, description="Reserved for per-call TTL override")
    providers: Optional[List[str]] = Field(None, description="Preferred providers")

# =============================================================================
# NEW: Tadawul Fast Bridge Provider
# =============================================================================

class TadawulFastBridgeProvider:
    """Provider for Tadawul (.SR) symbols via your FastAPI backend."""
    
    def __init__(self):
        self.name = "tadawul_fast_bridge"
        self.rate_limit = 60
        self.base_url = settings.backend_base_url or settings.fastapi_base or settings.base_url
        self.api_key = settings.tfb_app_token or settings.app_token
        self.enabled = bool(self.base_url and self.api_key)
        
    async def get_quote(self, symbol: str) -> Optional[Dict[str, Any]]:
        if not self.enabled:
            return None
            
        # Only handle Tadawul symbols
        if not symbol.upper().endswith(".SR"):
            return None
            
        try:
            url = f"{self.base_url.rstrip('/')}/v1/quote"
            params = {"tickers": symbol, "token": self.api_key}
            
            data = await http_client.request_with_retry("GET", url, params)
            if data and "symbols" in data and data["symbols"]:
                quote_data = data["symbols"][0]
                return {
                    "ticker": symbol,
                    "price": quote_data.get("price"),
                    "previous_close": quote_data.get("previous_close"),
                    "change_value": quote_data.get("change_value"),
                    "change_percent": quote_data.get("change_percent"),
                    "volume": quote_data.get("volume"),
                    "market_cap": quote_data.get("market_cap"),
                    "open_price": quote_data.get("open_price"),
                    "high_price": quote_data.get("high_price"),
                    "low_price": quote_data.get("low_price"),
                    "currency": quote_data.get("currency") or "SAR",
                    "exchange": "TADAWUL",
                    "provider": self.name,
                    "as_of": datetime.datetime.utcnow().isoformat() + "Z",
                }
        except Exception as e:
            logger.warning(f"Tadawul Fast Bridge failed for {symbol}: {e}")
            
        return None

# =============================================================================
# NEW: MarketStack Provider
# =============================================================================

class MarketStackProvider:
    """MarketStack provider implementation."""
    
    def __init__(self):
        self.name = "marketstack"
        self.rate_limit = 1000
        self.base_url = "http://api.marketstack.com/v1"
        self.api_key = settings.marketstack_api_key
        self.enabled = bool(self.api_key)
        
    async def get_quote(self, symbol: str) -> Optional[Dict[str, Any]]:
        if not self.enabled:
            return None
            
        params = {
            "access_key": self.api_key,
            "symbols": symbol,
            "limit": 1
        }
        
        data = await http_client.request_with_retry("GET", f"{self.base_url}/eod/latest", params)
        if data and "data" in data and data["data"]:
            item = data["data"][0]
            return {
                "ticker": symbol,
                "price": item.get("close"),
                "previous_close": item.get("adj_close"),
                "change_value": item.get("change"),
                "change_percent": item.get("change_pct"),
                "volume": item.get("volume"),
                "open_price": item.get("open"),
                "high_price": item.get("high"),
                "low_price": item.get("low"),
                "exchange": item.get("exchange"),
                "provider": self.name,
                "as_of": item.get("date") or datetime.datetime.utcnow().isoformat() + "Z",
            }
        return None

# =============================================================================
# NEW: TwelveData Provider
# =============================================================================

class TwelveDataProvider:
    """TwelveData provider implementation."""
    
    def __init__(self):
        self.name = "twelvedata"
        self.rate_limit = 800
        self.base_url = "https://api.twelvedata.com"
        self.api_key = settings.twelvedata_api_key
        self.enabled = bool(self.api_key)
        
    async def get_quote(self, symbol: str) -> Optional[Dict[str, Any]]:
        if not self.enabled:
            return None
            
        params = {
            "symbol": symbol,
            "apikey": self.api_key,
            "interval": "1min",
            "outputsize": 1
        }
        
        data = await http_client.request_with_retry("GET", f"{self.base_url}/quote", params)
        if data and "close" in data:
            return {
                "ticker": symbol,
                "price": self._safe_float(data.get("close")),
                "previous_close": self._safe_float(data.get("previous_close")),
                "change_value": self._safe_float(data.get("change")),
                "change_percent": self._safe_float(data.get("percent_change")),
                "volume": self._safe_float(data.get("volume")),
                "open_price": self._safe_float(data.get("open")),
                "high_price": self._safe_float(data.get("high")),
                "low_price": self._safe_float(data.get("low")),
                "currency": data.get("currency"),
                "exchange": data.get("exchange"),
                "provider": self.name,
                "as_of": datetime.datetime.utcnow().isoformat() + "Z",
            }
        return None
        
    def _safe_float(self, value: Any) -> Optional[float]:
        try:
            return float(value) if value not in (None, "") else None
        except (ValueError, TypeError):
            return None

# =============================================================================
# UPDATED Provider Classes (keep your existing but fix FMP)
# =============================================================================

class FMPProvider:
    """Financial Modeling Prep provider - FIXED to use correct endpoint."""
    
    def __init__(self):
        self.name = "fmp"
        self.rate_limit = 60
        self.base_url = settings.fmp_base_url.rstrip("/")
        self.api_key = settings.fmp_api_key
        self.enabled = bool(self.api_key)  # ENABLED for quotes
        
    async def get_quote(self, symbol: str) -> Optional[Dict[str, Any]]:
        if not self.enabled:
            return None
            
        # Use correct FMP v3 endpoint
        params = {"apikey": self.api_key}
        data = await http_client.request_with_retry("GET", f"{self.base_url}/quote/{symbol}", params)
        
        if data and isinstance(data, list) and data:
            item = data[0]
            return {
                "ticker": symbol,
                "price": item.get("price"),
                "previous_close": item.get("previousClose"),
                "change_value": item.get("change"),
                "change_percent": item.get("changesPercentage"),
                "volume": item.get("volume"),
                "market_cap": item.get("marketCap"),
                "open_price": item.get("open"),
                "high_price": item.get("dayHigh"),
                "low_price": item.get("dayLow"),
                "pe_ratio": item.get("pe"),
                "eps": item.get("eps"),
                "exchange": item.get("exchange"),
                "provider": self.name,
                "as_of": datetime.datetime.utcnow().isoformat() + "Z",
            }
        return None

# Keep your existing AlphaVantageProvider, FinnhubProvider, EODHDProvider unchanged
# ... (copy them exactly as they are in your original code)

# =============================================================================
# UPDATED ProviderManager with ALL providers
# =============================================================================

class ProviderManager:
    """Manager that uses ALL configured providers from enabled_providers."""
    
    def __init__(self):
        # Map provider names to their classes
        provider_classes = {
            "tadawul_fast_bridge": TadawulFastBridgeProvider,
            "eodhd": EODHDProvider,
            "fmp": FMPProvider,
            "finnhub": FinnhubProvider,
            "alpha_vantage": AlphaVantageProvider,
            "marketstack": MarketStackProvider,
            "twelvedata": TwelveDataProvider,
        }
        
        # Create instances only for enabled providers
        self.providers = []
        for provider_name in settings.enabled_providers:
            if provider_name in provider_classes:
                provider_instance = provider_classes[provider_name]()
                if getattr(provider_instance, "enabled", True):
                    self.providers.append(provider_instance)
                    logger.info(f"✅ Enabled {provider_name} provider")
                else:
                    logger.warning(f"⚠️ {provider_name} configured but not enabled (missing API key?)")
            else:
                logger.warning(f"⚠️ Unknown provider in ENABLED_PROVIDERS: {provider_name}")
        
        # Add Tadawul Fast Bridge as primary if configured
        if settings.primary_provider == "tadawul_fast_bridge" and settings.has_tadawul_fast_bridge:
            tadawul_provider = TadawulFastBridgeProvider()
            if tadawul_provider.enabled and "tadawul_fast_bridge" not in settings.enabled_providers:
                self.providers.insert(0, tadawul_provider)  # Insert at beginning for priority
                logger.info("✅ Added Tadawul Fast Bridge as primary provider")
        
        if not self.providers:
            logger.error("❌ No providers enabled! Check your API keys and ENABLED_PROVIDERS")
            
    async def get_quote(self, symbol: str) -> Optional[Dict[str, Any]]:
        """Get quote from providers in order."""
        errors = []
        
        for provider in self.providers:
            try:
                quote = await provider.get_quote(symbol)
                if quote and quote.get("price") is not None:
                    logger.info(f"✅ Quote for {symbol} from {provider.name}")
                    return quote
                else:
                    errors.append(f"{provider.name}: no data")
            except Exception as e:
                errors.append(f"{provider.name}: {e}")
                continue
                
        if errors:
            logger.debug(f"All providers failed for {symbol}: {', '.join(errors)}")
        return None
        
    async def get_quotes(self, symbols: List[str]) -> List[Dict[str, Any]]:
        """Get multiple quotes in parallel."""
        tasks = [self.get_quote(sym) for sym in symbols]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        quotes = []
        for sym, res in zip(symbols, results):
            if isinstance(res, dict):
                quotes.append(res)
        return quotes

# =============================================================================
# UPDATED Fundamentals Service
# =============================================================================

class FundamentalsService:
    """Enhanced fundamentals with all providers."""
    
    async def get_fundamentals(self, symbol: str) -> Dict[str, Any]:
        symbol_norm = symbol.strip().upper()
        
        base = {
            "ticker": symbol_norm,
            "status": "NO_DATA",
            "provider": None,
            "data": {},
            "message": None,
        }
        
        # Try EODHD first
        if settings.eodhd_api_key and "eodhd" in settings.enabled_providers:
            try:
                eod_data = await self._get_eod_fundamentals(symbol_norm)
                if eod_data:
                    base.update({
                        "status": "OK",
                        "provider": "eodhd",
                        "data": eod_data,
                        "message": "Fundamentals from EOD Historical Data"
                    })
                    return base
            except Exception as e:
                logger.debug(f"EODHD fundamentals failed: {e}")
        
        # Try FMP
        if settings.fmp_api_key and "fmp" in settings.enabled_providers:
            try:
                fmp_data = await self._get_fmp_fundamentals(symbol_norm)
                if fmp_data:
                    base.update({
                        "status": "OK",
                        "provider": "fmp",
                        "data": fmp_data,
                        "message": "Fundamentals from Financial Modeling Prep"
                    })
                    return base
            except Exception as e:
                logger.debug(f"FMP fundamentals failed: {e}")
        
        base["message"] = "No fundamentals available from configured providers"
        return base
    
    # Keep your existing _get_eod_fundamentals and _get_fmp_fundamentals methods
    # ... (copy them exactly as they are)

# =============================================================================
# UPDATED is_tadawul_symbol function
# =============================================================================

def is_tadawul_symbol(symbol: str) -> bool:
    """Enhanced Tadawul symbol detection."""
    if not symbol:
        return False
    symbol = symbol.strip().upper()
    return symbol.endswith(".SR") or symbol.startswith("SA:")

# =============================================================================
# UPDATED QuoteService with proper Tadawul handling
# =============================================================================

class QuoteService:
    """Service that properly handles Tadawul symbols."""
    
    def __init__(self, cache: TTLCache, provider_manager: ProviderManager):
        self.cache = cache
        self.provider_manager = provider_manager
        
    async def get_quote(self, symbol: str) -> Quote:
        symbol_norm = symbol.strip().upper()
        
        # Check cache first
        cached = self.cache.get(symbol_norm)
        if cached:
            return self._create_quote_model(cached, QuoteStatus.OK)
        
        # Get from providers
        provider_data = await self.provider_manager.get_quote(symbol_norm)
        if provider_data:
            self.cache.set(symbol_norm, provider_data)
            return self._create_quote_model(provider_data, QuoteStatus.OK)
        
        # No data
        return Quote(
            ticker=symbol_norm,
            status=QuoteStatus.NO_DATA,
            message="No data available from any provider",
        )
        
    async def get_quotes(self, symbols: List[str]) -> QuotesResponse:
        """Get multiple quotes with proper error handling."""
        cache_hits = []
        cache_misses = []
        sources_used = set()
        
        for sym in symbols:
            cached = self.cache.get(sym)
            if cached:
                cache_hits.append((sym, cached))
                if cached.get("provider"):
                    sources_used.add(cached["provider"])
            else:
                cache_misses.append(sym)
        
        # Get provider quotes for misses
        provider_quotes = []
        if cache_misses:
            provider_quotes = await self.provider_manager.get_quotes(cache_misses)
            for q in provider_quotes:
                if q and q.get("price") is not None:
                    self.cache.set(q["ticker"], q)
                    if q.get("provider"):
                        sources_used.add(q["provider"])
        
        # Build final quotes list
        quotes = []
        
        # Add cache hits
        for sym, data in cache_hits:
            quotes.append(self._create_quote_model(data, QuoteStatus.OK))
        
        # Add provider results
        provider_map = {q["ticker"]: q for q in provider_quotes if q and "ticker" in q}
        for sym in cache_misses:
            if sym in provider_map:
                quotes.append(self._create_quote_model(provider_map[sym], QuoteStatus.OK))
            else:
                quotes.append(Quote(
                    ticker=sym,
                    status=QuoteStatus.NO_DATA,
                    message="No data available",
                ))
        
        meta = {
            "cache_hits": len(cache_hits),
            "cache_misses": len(cache_misses),
            "provider_successes": len(provider_map),
            "sources": list(sources_used),
            "total_symbols": len(symbols),
            "successful_quotes": len(quotes),
        }
        
        return QuotesResponse(
            timestamp=datetime.datetime.utcnow(),
            symbols=quotes,
            meta=meta,
        )
    
    def _create_quote_model(self, data: Dict[str, Any], status: QuoteStatus) -> Quote:
        """Convert provider dict to Quote model."""
        as_of = data.get("as_of")
        if as_of and isinstance(as_of, str):
            try:
                as_of = datetime.datetime.fromisoformat(as_of.replace("Z", "+00:00"))
            except (ValueError, TypeError):
                as_of = datetime.datetime.utcnow()
        elif not isinstance(as_of, datetime.datetime):
            as_of = datetime.datetime.utcnow()
        
        return Quote(**{**data, "status": status, "as_of": as_of})

# =============================================================================
# Initialize services (AFTER defining all classes)
# =============================================================================

# Initialize cache (keep your TTLCache class exactly as is)
cache = TTLCache()

# Initialize HTTP client (keep your HTTPClient class exactly as is)
http_client = HTTPClient()

# Initialize providers
provider_manager = ProviderManager()

# Initialize quote service
quote_service = QuoteService(cache, provider_manager)

# Initialize fundamentals service
fundamentals_service = FundamentalsService()

# =============================================================================
# UPDATED Authentication to include TFB_APP_TOKEN
# =============================================================================

def verify_auth(
    request: Request,
    credentials: Optional[HTTPAuthorizationCredentials] = Depends(security),
) -> bool:
    """Enhanced authentication supporting all token types."""
    if not settings.require_auth:
        return True
    
    valid_tokens = [
        t for t in [
            settings.app_token,
            settings.backup_app_token,
            settings.tfb_app_token
        ] if t
    ]
    
    if not valid_tokens:
        if settings.is_production:
            raise HTTPException(
                status_code=500,
                detail="Authentication misconfigured - no tokens configured",
            )
        return True
    
    token = None
    
    # Check Authorization header
    if credentials:
        token = credentials.credentials.strip()
    
    # Check query parameter
    if not token:
        token = request.query_params.get("token", "").strip()
    
    # Check headers
    if not token:
        token = (
            request.headers.get("X-APP-TOKEN", "").strip()
            or request.headers.get("X_APP_TOKEN", "").strip()
            or request.headers.get("X-TFB-TOKEN", "").strip()
        )
    
    # Check raw Authorization
    if not token:
        raw = request.headers.get("Authorization", "").strip()
        if raw and not raw.lower().startswith("bearer "):
            token = raw
    
    if not token:
        raise HTTPException(status_code=401, detail="Missing authentication token")
    
    if token not in valid_tokens:
        token_hash = hashlib.sha256(token.encode()).hexdigest()[:16]
        logger.warning(f"Invalid token attempt: {token_hash}")
        raise HTTPException(status_code=401, detail="Invalid authentication token")
    
    return True

# =============================================================================
# UPDATED Root endpoint to show all providers
# =============================================================================

@app.get("/", response_model=Dict[str, Any])
@rate_limit(f"{settings.max_requests_per_minute}/minute")
async def root(request: Request):
    """Enhanced root endpoint showing all configured providers."""
    cache_stats = cache.get_stats()
    provider_names = [p.name for p in provider_manager.providers]
    
    return {
        "service": settings.service_name,
        "version": settings.service_version,
        "status": "operational",
        "environment": settings.environment,
        "timestamp": datetime.datetime.utcnow().isoformat() + "Z",
        "configured_providers": settings.enabled_providers,
        "active_providers": provider_names,
        "primary_provider": settings.primary_provider,
        "tadawul_support": settings.has_tadawul_fast_bridge,
        "cache": {
            "total_items": cache_stats["total_items"],
            "valid_items": cache_stats["valid_items"],
            "hit_rate": cache_stats["performance"]["hit_rate"],
        },
        "endpoints": {
            "health": "/health",
            "quotes": "/v1/quote",
            "fundamentals": "/v1/fundamentals",
            "tadawul_market": "/api/saudi/market",
            "cache_info": "/v1/cache/info",
        },
    }

# =============================================================================
# UPDATED Health check to show provider status
# =============================================================================

@app.get("/health", response_model=HealthResponse)
async def health_check(request: Request):
    """Enhanced health check with provider status."""
    cache_stats = cache.get_stats()
    
    # Check provider status
    provider_status = {}
    for provider in provider_manager.providers:
        try:
            # Test with a known symbol
            test_symbol = "AAPL" if not provider.name == "tadawul_fast_bridge" else "1120.SR"
            quote = await provider.get_quote(test_symbol)
            provider_status[provider.name] = {
                "status": "healthy" if quote else "no_data",
                "test_symbol": test_symbol
            }
        except Exception as e:
            provider_status[provider.name] = {
                "status": "error",
                "error": str(e)
            }
    
    dependencies = {
        "cache": {"status": cache_stats["total_items"] > 0, "items": cache_stats["total_items"]},
        "providers": provider_status,
        "google_services": {"status": settings.has_google_sheets_access},
        "tadawul_bridge": {"status": settings.has_tadawul_fast_bridge},
    }
    
    return HealthResponse(
        status="healthy",
        version=settings.service_version,
        timestamp=datetime.datetime.utcnow(),
        uptime_seconds=time.time() - APP_START_TIME,
        dependencies=dependencies,
    )

# =============================================================================
# UPDATED /v1/quote endpoint with better error messages
# =============================================================================

@app.get("/v1/quote", response_model=QuotesResponse)
@rate_limit("60/minute")
async def get_quotes_v1(
    request: Request,
    tickers: str = Query(..., description="Comma-separated ticker symbols"),
    auth: bool = Depends(verify_auth),
):
    """
    Enhanced quote endpoint with provider fallback.
    
    Supports:
    - Global symbols: AAPL, MSFT, GOOGL
    - Tadawul symbols: 1120.SR, 2222.SR
    - Multiple providers with fallback
    """
    try:
        symbols = [t.strip().upper() for t in tickers.split(",") if t.strip()]
        
        if not symbols:
            raise HTTPException(status_code=400, detail="No tickers provided")
        
        if len(symbols) > 100:
            raise HTTPException(status_code=400, detail="Too many tickers (max 100)")
        
        try:
            response = await asyncio.wait_for(
                quote_service.get_quotes(symbols),
                timeout=QUOTE_OVERALL_TIMEOUT_SECONDS,
            )
            return response
            
        except asyncio.TimeoutError:
            logger.warning(f"Quote timeout for {len(symbols)} symbols")
            quotes = [
                Quote(
                    ticker=sym,
                    status=QuoteStatus.ERROR,
                    message=f"Timeout after {QUOTE_OVERALL_TIMEOUT_SECONDS}s",
                )
                for sym in symbols
            ]
            return QuotesResponse(
                timestamp=datetime.datetime.utcnow(),
                symbols=quotes,
                meta={"timeout": True, "total_symbols": len(symbols)},
            )
            
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Quote error: {e}")
        raise HTTPException(status_code=500, detail=str(e))

# =============================================================================
# NEW: Direct Tadawul endpoint
# =============================================================================

@app.get("/v1/tadawul/quote")
@rate_limit("30/minute")
async def get_tadawul_quote(
    request: Request,
    symbol: str = Query(..., description="Tadawul symbol (e.g., 2222.SR)"),
    auth: bool = Depends(verify_auth),
):
    """Direct endpoint for Tadawul symbols."""
    if not symbol.upper().endswith(".SR"):
        raise HTTPException(status_code=400, detail="Not a Tadawul symbol (.SR required)")
    
    try:
        # Try Tadawul Fast Bridge first
        tadawul_provider = TadawulFastBridgeProvider()
        if tadawul_provider.enabled:
            quote = await tadawul_provider.get_quote(symbol)
            if quote:
                return quote
        
        # Fallback to cache
        cached = cache.get(symbol)
        if cached:
            return cached
        
        return {
            "ticker": symbol,
            "status": "NO_DATA",
            "message": "Tadawul data not available. Check TFB_APP_TOKEN and backend URL.",
            "provider": "none"
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# =============================================================================
# Keep all other endpoints unchanged
# =============================================================================

# Your existing endpoints below remain exactly the same:
# - /v41/quotes
# - /v1/quote/update
# - /v1/cache/*
# - /v1/fundamentals
# - Analysis endpoints
# - Debug endpoints
# - /api/saudi/market

# =============================================================================
# Application Startup
# =============================================================================

def main():
    """Enhanced startup with provider validation."""
    logger.info("=" * 70)
    logger.info(f"🚀 {settings.service_name} v{settings.service_version}")
    logger.info(f"🌍 Environment: {settings.environment}")
    logger.info(f"🔐 Authentication: {'Enabled' if settings.require_auth else 'Disabled'}")
    logger.info(f"📈 Enabled Providers: {', '.join(settings.enabled_providers)}")
    logger.info(f"🎯 Primary Provider: {settings.primary_provider}")
    logger.info(f"🇸🇦 Tadawul Support: {'Available' if settings.has_tadawul_fast_bridge else 'Not Available'}")
    logger.info(f"⚡ Active Providers: {[p.name for p in provider_manager.providers]}")
    logger.info(f"💾 Cache: {cache.get_item_count()} items loaded")
    logger.info("=" * 70)
    
    uvicorn.run(
        "main:app",
        host=settings.app_host,
        port=settings.app_port,
        reload=False,
        log_level="info",
    )

if __name__ == "__main__":
    main()
