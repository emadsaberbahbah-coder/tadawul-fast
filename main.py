#!/usr/bin/env python3
"""
Tadawul Fast Bridge API v4.0.1 - Enhanced Production Server
Production-ready with comprehensive monitoring, async architecture, and enhanced providers
Aligned with google_apps_script_client.py and google_sheets_service.py patterns
"""

import asyncio
import datetime
import hashlib
import json
import logging
import os
import time
import uuid
import traceback
from contextlib import asynccontextmanager
from enum import Enum
from pathlib import Path
from typing import Any, Dict, List, Optional, Union, Tuple, Callable
from collections import defaultdict

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
    Response,
    status,
)
from fastapi.middleware.cors import CORSMiddleware
from fastapi.middleware.gzip import GZipMiddleware
from fastapi.middleware.trustedhost import TrustedHostMiddleware
from fastapi.responses import JSONResponse, ORJSONResponse, RedirectResponse
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from pydantic import BaseModel, Field, validator, ConfigDict
from pydantic_settings import BaseSettings, SettingsConfigDict
from slowapi import Limiter, _rate_limit_exceeded_handler
from slowapi.errors import RateLimitExceeded
from slowapi.middleware import SlowAPIMiddleware
from slowapi.util import get_remote_address

# Import enhanced components
from google_apps_script_client import (
    GoogleAppsScriptClient,
    GoogleAppsScriptResponse,
    get_google_apps_script_client,
    close_google_apps_script_client,
)
from google_sheets_service import (
    AsyncGoogleSheetsService,
    SheetsConfig,
    get_google_sheets_service,
    close_google_sheets_service,
)

# -----------------------------------------------------------------------------
# Load .env for local development (Render uses env vars directly)
# -----------------------------------------------------------------------------
load_dotenv()

# Configure enhanced logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - [%(filename)s:%(lineno)d] - %(message)s",
)
logger = logging.getLogger(__name__)

# =============================================================================
# Enhanced Configuration
# =============================================================================

class Environment(str, Enum):
    """Environment types."""
    DEVELOPMENT = "development"
    STAGING = "staging"
    PRODUCTION = "production"
    TESTING = "testing"


class LogLevel(str, Enum):
    """Log level enumeration."""
    DEBUG = "debug"
    INFO = "info"
    WARNING = "warning"
    ERROR = "error"
    CRITICAL = "critical"


class CacheStrategy(str, Enum):
    """Cache strategy enumeration."""
    NONE = "none"
    MEMORY = "memory"
    REDIS = "redis"
    HYBRID = "hybrid"


class AppSettings(BaseSettings):
    """Enhanced application configuration with comprehensive validation."""

    # Application Identity
    app_name: str = Field("Tadawul Fast Bridge", description="Application name")
    app_version: str = Field("4.0.1", description="Application version")
    app_description: str = Field(
        "Real-time Saudi stock market data API with comprehensive monitoring",
        description="Application description",
    )
    app_host: str = Field("0.0.0.0", description="Host to bind to")
    app_port: int = Field(8000, ge=1, le=65535, description="Port to bind to")
    app_workers: int = Field(4, ge=1, description="Number of worker processes")

    # Environment
    environment: Environment = Field(Environment.DEVELOPMENT, description="Runtime environment")
    log_level: LogLevel = Field(LogLevel.INFO, description="Logging level")
    debug: bool = Field(False, description="Debug mode")

    # Security
    require_auth: bool = Field(True, description="Require authentication")
    app_token: Optional[str] = Field(None, description="Primary application token")
    backup_app_token: Optional[str] = Field(None, description="Backup application token")
    tfb_app_token: Optional[str] = Field(None, description="Tadawul Fast Bridge token")
    jwt_secret: Optional[str] = Field(None, description="JWT secret key")
    jwt_algorithm: str = Field("HS256", description="JWT algorithm")
    api_key_header: str = Field("X-API-Key", description="API key header name")

    # Rate Limiting
    rate_limit_enabled: bool = Field(True, description="Enable rate limiting")
    rate_limit_default: str = Field("100/minute", description="Default rate limit")
    rate_limit_strict: str = Field("30/minute", description="Strict rate limit")
    rate_limit_burst: str = Field("10/second", description="Burst rate limit")

    # CORS
    cors_enabled: bool = Field(True, description="Enable CORS")
    cors_origins: List[str] = Field(
        default_factory=lambda: ["*"],
        description="Allowed CORS origins",
    )
    cors_methods: List[str] = Field(
        default_factory=lambda: ["GET", "POST", "PUT", "DELETE", "OPTIONS"],
        description="Allowed CORS methods",
    )
    cors_headers: List[str] = Field(
        default_factory=lambda: ["*"],
        description="Allowed CORS headers",
    )

    # Trusted Hosts
    trusted_hosts: List[str] = Field(
        default_factory=lambda: ["*"],
        description="Trusted hosts",
    )

    # Cache Configuration
    cache_strategy: CacheStrategy = Field(CacheStrategy.MEMORY, description="Cache strategy")
    cache_ttl_default: int = Field(300, ge=0, description="Default cache TTL (seconds)")
    cache_ttl_short: int = Field(60, ge=0, description="Short cache TTL (seconds)")
    cache_ttl_long: int = Field(3600, ge=0, description="Long cache TTL (seconds)")
    cache_max_size: int = Field(10000, ge=100, description="Maximum cache size")
    # Align with CACHE_SAVE_INTERVAL env (Render)
    cache_backup_interval: int = Field(
        60,
        ge=10,
        description="Cache backup interval (seconds)",
        validation_alias="CACHE_SAVE_INTERVAL",
    )

    # Redis Cache (if using Redis)
    redis_host: Optional[str] = Field(None, description="Redis host")
    redis_port: int = Field(6379, description="Redis port")
    redis_db: int = Field(0, description="Redis database")
    redis_password: Optional[str] = Field(None, description="Redis password")

    # Google Services
    # Align GOOGLE_SHEETS_CREDENTIALS env with settings
    google_sheets_credentials_json: Optional[str] = Field(
        None,
        description="Google Sheets credentials JSON",
        validation_alias="GOOGLE_SHEETS_CREDENTIALS",
    )
    google_sheets_credentials_path: Optional[str] = Field(
        None,
        description="Google Sheets credentials path",
    )
    # Align SPREADSHEET_ID env with settings
    google_sheets_spreadsheet_id: str = Field(
        ...,
        description="Google Sheets spreadsheet ID",
        validation_alias="SPREADSHEET_ID",
    )
    google_apps_script_url: str = Field(..., description="Google Apps Script URL")
    google_apps_script_backup_url: Optional[str] = Field(
        None,
        description="Google Apps Script backup URL",
    )

    # Tadawul Market Configuration
    # IMPORTANT: align with actual Google Sheet tab name: "KSA_Tadawul"
    tadawul_market_sheet: str = Field("KSA_Tadawul", description="Tadawul market sheet name")
    tadawul_all_sheet: Optional[str] = Field(None, description="Tadawul all data sheet")
    market_data_sheet: Optional[str] = Field(None, description="Market data sheet")
    portfolio_sheet: Optional[str] = Field(None, description="Portfolio sheet")

    # Financial Data Providers
    providers_enabled: List[str] = Field(
        default_factory=lambda: ["google_sheets", "google_apps_script"],
        description="Enabled data providers",
    )
    provider_priority: List[str] = Field(
        default_factory=lambda: ["google_sheets", "google_apps_script"],
        description="Provider priority order",
    )

    # Performance
    http_timeout: int = Field(30, ge=5, le=120, description="HTTP timeout (seconds)")
    http_max_retries: int = Field(3, ge=0, le=10, description="HTTP max retries")
    http_retry_delay: float = Field(1.0, ge=0.1, le=10.0, description="HTTP retry delay (seconds)")
    request_timeout: int = Field(10, ge=1, le=30, description="Request timeout (seconds)")
    max_concurrent_requests: int = Field(100, ge=10, description="Max concurrent requests")

    # Monitoring
    metrics_enabled: bool = Field(True, description="Enable metrics collection")
    health_check_interval: int = Field(30, ge=5, description="Health check interval (seconds)")
    request_logging: bool = Field(True, description="Enable request logging")
    performance_monitoring: bool = Field(True, description="Enable performance monitoring")

    # API Features
    enable_swagger: bool = Field(True, description="Enable Swagger UI")
    enable_redoc: bool = Field(True, description="Enable ReDoc")
    enable_prometheus: bool = Field(False, description="Enable Prometheus metrics")
    enable_sentry: bool = Field(False, description="Enable Sentry error tracking")
    sentry_dsn: Optional[str] = Field(None, description="Sentry DSN")

    # Background Tasks
    background_tasks_enabled: bool = Field(True, description="Enable background tasks")
    cache_cleanup_interval: int = Field(300, ge=60, description="Cache cleanup interval (seconds)")
    metrics_aggregation_interval: int = Field(60, ge=10, description="Metrics aggregation interval (seconds)")

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
    )

    @validator("cors_origins", "trusted_hosts", pre=True)
    def parse_comma_separated_list(cls, v):
        """Parse CORS / hosts from env (supports '*', comma list, JSON-style list)."""
        if isinstance(v, str):
            s = v.strip()
            if s == "*":
                return ["*"]
            # Handle JSON-style lists, e.g. ["*"] or ["https://a","https://b"]
            if s.startswith("[") and s.endswith("]"):
                try:
                    parsed = json.loads(s)
                    if isinstance(parsed, list):
                        return [str(item).strip() for item in parsed]
                except Exception:
                    pass
            return [item.strip() for item in s.split(",") if item.strip()]
        return v

    @validator("providers_enabled", "provider_priority", pre=True)
    def parse_providers(cls, v):
        """Parse providers from comma-separated string."""
        if isinstance(v, str):
            return [p.strip() for p in v.split(",") if p.strip()]
        return v

    @validator("environment", pre=True)
    def validate_environment(cls, v):
        """Validate environment value."""
        if isinstance(v, str):
            v = v.lower()
            if v not in [e.value for e in Environment]:
                raise ValueError(f"Invalid environment: {v}")
        return v

    @validator("log_level", pre=True)
    def validate_log_level(cls, v):
        """Validate log level."""
        if isinstance(v, str):
            v = v.lower()
            if v not in [l.value for l in LogLevel]:
                raise ValueError(f"Invalid log level: {v}")
        return v

    @property
    def is_production(self) -> bool:
        """Check if running in production."""
        return self.environment == Environment.PRODUCTION

    @property
    def is_development(self) -> bool:
        """Check if running in development."""
        return self.environment == Environment.DEVELOPMENT

    @property
    def is_staging(self) -> bool:
        """Check if running in staging."""
        return self.environment == Environment.STAGING

    @property
    def is_testing(self) -> bool:
        """Check if running in testing."""
        return self.environment == Environment.TESTING

    @property
    def has_google_sheets(self) -> bool:
        """Check if Google Sheets is configured."""
        return bool(self.google_sheets_credentials_json or self.google_sheets_credentials_path)

    @property
    def has_google_apps_script(self) -> bool:
        """Check if Google Apps Script is configured."""
        return bool(self.google_apps_script_url)

    @property
    def has_redis(self) -> bool:
        """Check if Redis is configured."""
        return bool(self.redis_host)

    def validate_configuration(self) -> List[str]:
        """Validate configuration and return list of issues."""
        issues: List[str] = []

        # Required configurations
        if not self.google_apps_script_url:
            issues.append("GOOGLE_APPS_SCRIPT_URL is required")

        if not self.google_sheets_spreadsheet_id:
            issues.append("SPREADSHEET_ID (Google Sheets spreadsheet ID) is required")

        if not self.has_google_sheets:
            issues.append("Google Sheets credentials not configured (GOOGLE_SHEETS_CREDENTIALS)")

        # Security warnings
        if self.is_production and not self.app_token:
            issues.append("APP_TOKEN is recommended for production")

        if self.cors_origins == ["*"] and self.is_production:
            issues.append("CORS origins set to '*' in production is not recommended")

        if self.trusted_hosts == ["*"] and self.is_production:
            issues.append("Trusted hosts set to '*' in production is not recommended")

        # Performance warnings
        if self.cache_max_size < 1000:
            issues.append("Cache max size is very small, consider increasing")

        if self.http_timeout > 60:
            issues.append("HTTP timeout is very high, may cause resource issues")

        return issues

    def get_log_level_numeric(self) -> int:
        """Convert log level string to numeric value."""
        levels = {
            LogLevel.DEBUG: logging.DEBUG,
            LogLevel.INFO: logging.INFO,
            LogLevel.WARNING: logging.WARNING,
            LogLevel.ERROR: logging.ERROR,
            LogLevel.CRITICAL: logging.CRITICAL,
        }
        return levels.get(self.log_level, logging.INFO)


# Load settings
settings = AppSettings()

# Reconfigure logging with proper level
logging.getLogger().setLevel(settings.get_log_level_numeric())

# =============================================================================
# Enhanced Data Models
# =============================================================================

class RequestContext(BaseModel):
    """Request context for tracing and monitoring."""
    request_id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    start_time: float = Field(default_factory=time.time)
    client_ip: Optional[str] = None
    user_agent: Optional[str] = None
    endpoint: Optional[str] = None
    method: Optional[str] = None
    metadata: Dict[str, Any] = Field(default_factory=dict)

    @property
    def duration(self) -> float:
        """Get request duration in seconds."""
        return time.time() - self.start_time


class QuoteStatus(str, Enum):
    """Quote status enumeration."""
    OK = "ok"
    NO_DATA = "no_data"
    ERROR = "error"
    CACHED = "cached"
    PENDING = "pending"
    PARTIAL = "partial"


class DataQuality(str, Enum):
    """Data quality enumeration."""
    EXCELLENT = "excellent"
    HIGH = "high"
    MEDIUM = "medium"
    LOW = "low"
    UNKNOWN = "unknown"


class Quote(BaseModel):
    """Enhanced quote model with comprehensive data."""

    # Identification
    ticker: str = Field(..., description="Stock ticker symbol")
    symbol: str = Field(..., description="Full symbol with exchange")
    exchange: Optional[str] = Field(None, description="Stock exchange")
    currency: str = Field("SAR", description="Currency code")
    name: Optional[str] = Field(None, description="Company name")
    sector: Optional[str] = Field(None, description="Sector/Industry")

    # Pricing Data
    price: Optional[float] = Field(None, ge=0, description="Current price")
    previous_close: Optional[float] = Field(None, ge=0, description="Previous close")
    open: Optional[float] = Field(None, ge=0, description="Open price")
    high: Optional[float] = Field(None, ge=0, description="Daily high")
    low: Optional[float] = Field(None, ge=0, description="Daily low")
    volume: Optional[float] = Field(None, ge=0, description="Trading volume")
    avg_volume: Optional[float] = Field(None, ge=0, description="Average volume")

    # Changes
    change: Optional[float] = Field(None, description="Price change")
    change_percent: Optional[float] = Field(None, description="Percentage change")

    # Market Data
    market_cap: Optional[float] = Field(None, ge=0, description="Market capitalization")
    shares_outstanding: Optional[float] = Field(None, ge=0, description="Shares outstanding")
    free_float: Optional[float] = Field(None, ge=0, le=100, description="Free float percentage")

    # Valuation
    pe_ratio: Optional[float] = Field(None, description="Price-to-earnings ratio")
    pb_ratio: Optional[float] = Field(None, description="Price-to-book ratio")
    ps_ratio: Optional[float] = Field(None, description="Price-to-sales ratio")
    ev_ebitda: Optional[float] = Field(None, description="EV/EBITDA ratio")
    dividend_yield: Optional[float] = Field(None, ge=0, description="Dividend yield")

    # Financials
    eps: Optional[float] = Field(None, description="Earnings per share")
    roe: Optional[float] = Field(None, description="Return on equity")
    roa: Optional[float] = Field(None, description="Return on assets")
    debt_equity: Optional[float] = Field(None, ge=0, description="Debt-to-equity ratio")

    # Technical Indicators
    rsi: Optional[float] = Field(None, ge=0, le=100, description="RSI")
    macd: Optional[float] = Field(None, description="MACD")
    moving_avg_20: Optional[float] = Field(None, description="20-day moving average")
    moving_avg_50: Optional[float] = Field(None, description="50-day moving average")
    volatility: Optional[float] = Field(None, ge=0, description="Volatility")

    # Metadata
    status: QuoteStatus = Field(QuoteStatus.OK, description="Quote status")
    quality: DataQuality = Field(DataQuality.UNKNOWN, description="Data quality")
    provider: Optional[str] = Field(None, description="Data provider")
    source: Optional[str] = Field(None, description="Data source")
    cached: bool = Field(False, description="Whether data is cached")
    cache_ttl: Optional[int] = Field(None, description="Cache TTL remaining")
    # New: to support error messages on failed quotes
    message: Optional[str] = Field(None, description="Additional status or error message")

    # Timestamps
    last_updated: datetime.datetime = Field(
        default_factory=lambda: datetime.datetime.now(datetime.timezone.utc),
        description="Last update timestamp",
    )
    timestamp: datetime.datetime = Field(
        default_factory=lambda: datetime.datetime.now(datetime.timezone.utc),
        description="Response timestamp",
    )

    # Raw Data
    raw_data: Optional[Dict[str, Any]] = Field(None, description="Raw provider data")

    model_config = ConfigDict(
        json_encoders={
            datetime.datetime: lambda dt: dt.isoformat(),
        }
    )

    @property
    def is_valid(self) -> bool:
        """Check if quote has minimum valid data."""
        return self.price is not None and self.price > 0

    @property
    def is_tadawul(self) -> bool:
        """Check if this is a Tadawul symbol."""
        return self.symbol.upper().endswith(".SR") or self.exchange == "TADAWUL"

    def to_simple_dict(self) -> Dict[str, Any]:
        """Convert to simplified dictionary."""
        return {
            "ticker": self.ticker,
            "price": self.price,
            "change": self.change,
            "change_percent": self.change_percent,
            "volume": self.volume,
            "market_cap": self.market_cap,
            "status": self.status,
            "timestamp": self.timestamp.isoformat(),
        }


class QuoteResponse(BaseModel):
    """Enhanced quote response with metadata."""

    # Response Data
    quotes: List[Quote] = Field(..., description="List of quotes")

    # Metadata
    timestamp: datetime.datetime = Field(
        default_factory=lambda: datetime.datetime.now(datetime.timezone.utc),
        description="Response timestamp",
    )
    request_id: Optional[str] = Field(None, description="Request ID")
    success_count: int = Field(0, ge=0, description="Number of successful quotes")
    error_count: int = Field(0, ge=0, description="Number of failed quotes")
    cache_hits: int = Field(0, ge=0, description="Number of cache hits")
    cache_misses: int = Field(0, ge=0, description="Number of cache misses")

    # Performance
    execution_time: Optional[float] = Field(None, ge=0, description="Execution time in seconds")
    providers_used: List[str] = Field(default_factory=list, description="Providers used")

    # Status
    status: str = Field("success", description="Overall response status")
    message: Optional[str] = Field(None, description="Response message")

    @property
    def total_quotes(self) -> int:
        """Get total number of quotes."""
        return len(self.quotes)

    @property
    def success_rate(self) -> float:
        """Get success rate."""
        total = self.total_quotes
        return self.success_count / total if total > 0 else 0.0

    @property
    def cache_hit_rate(self) -> float:
        """Get cache hit rate."""
        total_requests = self.cache_hits + self.cache_misses
        return self.cache_hits / total_requests if total_requests > 0 else 0.0


class HealthStatus(str, Enum):
    """Health status enumeration."""
    HEALTHY = "healthy"
    DEGRADED = "degraded"
    UNHEALTHY = "unhealthy"
    OFFLINE = "offline"


class ServiceHealth(BaseModel):
    """Service health information."""

    service: str = Field(..., description="Service name")
    status: HealthStatus = Field(..., description="Health status")
    version: str = Field(..., description="Service version")
    timestamp: datetime.datetime = Field(..., description="Check timestamp")

    # Performance Metrics
    response_time: Optional[float] = Field(None, description="Response time in seconds")
    uptime: Optional[float] = Field(None, description="Uptime in seconds")
    error_rate: Optional[float] = Field(None, description="Error rate")

    # Dependencies (simplified typing to avoid forward-ref issues)
    dependencies: Dict[str, Any] = Field(default_factory=dict, description="Dependency health")

    # Additional Info
    details: Dict[str, Any] = Field(default_factory=dict, description="Additional details")
    message: Optional[str] = Field(None, description="Status message")


class HealthResponse(BaseModel):
    """Comprehensive health response."""

    status: HealthStatus = Field(..., description="Overall status")
    timestamp: datetime.datetime = Field(
        default_factory=lambda: datetime.datetime.now(datetime.timezone.utc),
        description="Check timestamp",
    )
    version: str = Field(..., description="API version")
    environment: str = Field(..., description="Environment")
    uptime: float = Field(..., description="Uptime in seconds")

    # Services
    services: Dict[str, ServiceHealth] = Field(..., description="Service health")

    # System Metrics
    system: Dict[str, Any] = Field(default_factory=dict, description="System metrics")

    # Performance
    performance: Dict[str, Any] = Field(default_factory=dict, description="Performance metrics")

    @property
    def is_healthy(self) -> bool:
        """Check if all services are healthy."""
        return all(
            service.status == HealthStatus.HEALTHY
            for service in self.services.values()
        )


class CacheEntry(BaseModel):
    """Cache entry model."""

    key: str = Field(..., description="Cache key")
    value: Any = Field(..., description="Cached value")
    timestamp: float = Field(..., description="Cache timestamp")
    ttl: int = Field(..., description="Time to live (seconds)")
    hits: int = Field(0, description="Number of hits")
    size: Optional[int] = Field(None, description="Size in bytes")

    @property
    def expires_at(self) -> float:
        """Get expiration timestamp."""
        return self.timestamp + self.ttl

    @property
    def is_expired(self) -> bool:
        """Check if cache entry is expired."""
        return time.time() > self.expires_at

    @property
    def age(self) -> float:
        """Get age in seconds."""
        return time.time() - self.timestamp

    @property
    def time_remaining(self) -> float:
        """Get time remaining until expiration."""
        return max(0, self.expires_at - time.time())


class CacheStats(BaseModel):
    """Cache statistics."""

    # Size Information
    total_entries: int = Field(0, description="Total cache entries")
    valid_entries: int = Field(0, description="Valid (non-expired) entries")
    expired_entries: int = Field(0, description="Expired entries")
    total_size_bytes: Optional[int] = Field(None, description="Total size in bytes")
    avg_entry_size_bytes: Optional[float] = Field(None, description="Average entry size")

    # Performance
    hits: int = Field(0, description="Total cache hits")
    misses: int = Field(0, description="Total cache misses")
    hit_rate: float = Field(0.0, description="Cache hit rate (0-1)")
    miss_rate: float = Field(0.0, description="Cache miss rate (0-1)")

    # Operations
    sets: int = Field(0, description="Number of set operations")
    gets: int = Field(0, description="Number of get operations")
    deletes: int = Field(0, description="Number of delete operations")
    evictions: int = Field(0, description="Number of evictions")

    # Time-based Metrics
    avg_response_time_ms: float = Field(0.0, description="Average response time in ms")
    oldest_entry_age_seconds: float = Field(0.0, description="Age of oldest entry")
    newest_entry_age_seconds: float = Field(0.0, description="Age of newest entry")

    # Memory Usage
    memory_usage_percent: Optional[float] = Field(None, description="Memory usage percentage")
    max_memory_bytes: Optional[int] = Field(None, description="Maximum memory")

    @property
    def total_operations(self) -> int:
        """Get total operations."""
        return self.hits + self.misses

    @property
    def success_rate(self) -> float:
        """Get cache success rate."""
        total = self.total_operations
        return self.hits / total if total > 0 else 0.0


# =============================================================================
# Enhanced Cache System
# =============================================================================

class EnhancedCache:
    """Enhanced cache system with multiple strategies."""

    def __init__(self, strategy: CacheStrategy = CacheStrategy.MEMORY, **kwargs):
        self.strategy = strategy
        self.config = kwargs

        # Statistics
        self.stats = CacheStats(
            hits=0,
            misses=0,
            sets=0,
            gets=0,
            deletes=0,
            evictions=0,
        )

        # Memory cache (always available)
        self._memory_cache: Dict[str, CacheEntry] = {}
        self._memory_lock = asyncio.Lock()

        # Redis cache (if enabled)
        self._redis_client = None
        if strategy in [CacheStrategy.REDIS, CacheStrategy.HYBRID]:
            self._init_redis()

        # File cache (for persistence)
        self._cache_dir = Path(self.config.get("cache_dir", ".cache"))
        self._cache_dir.mkdir(parents=True, exist_ok=True)

        logger.info(f"EnhancedCache initialized with strategy: {strategy.value}")

    def _init_redis(self):
        """Initialize Redis client."""
        try:
            import redis
            self._redis_client = redis.Redis(
                host=settings.redis_host,
                port=settings.redis_port,
                db=settings.redis_db,
                password=settings.redis_password,
                decode_responses=True,
            )
            # Test connection
            self._redis_client.ping()
            logger.info("Redis cache connected successfully")
        except Exception as e:
            logger.error(f"Failed to connect to Redis: {e}")
            self._redis_client = None
            self.strategy = CacheStrategy.MEMORY

    async def get(self, key: str) -> Optional[Any]:
        """Get value from cache."""
        async with self._memory_lock:
            self.stats.gets += 1

        # Try memory cache first
        if self.strategy in [CacheStrategy.MEMORY, CacheStrategy.HYBRID]:
            async with self._memory_lock:
                if key in self._memory_cache:
                    entry = self._memory_cache[key]
                    if not entry.is_expired:
                        self.stats.hits += 1
                        entry.hits += 1
                        return entry.value
                    else:
                        # Clean up expired entry
                        del self._memory_cache[key]
                        self.stats.evictions += 1

        # Try Redis cache
        if self.strategy in [CacheStrategy.REDIS, CacheStrategy.HYBRID] and self._redis_client:
            try:
                value = self._redis_client.get(key)
                if value:
                    # Also store in memory cache for faster access
                    if self.strategy == CacheStrategy.HYBRID:
                        await self._set_memory(key, json.loads(value), settings.cache_ttl_default)

                    self.stats.hits += 1
                    return json.loads(value)
            except Exception as e:
                logger.warning(f"Redis get failed: {e}")

        # Try file cache
        if self.config.get("enable_file_cache", True):
            file_value = await self._get_from_file(key)
            if file_value is not None:
                # Also store in memory cache
                if self.strategy != CacheStrategy.NONE:
                    await self._set_memory(key, file_value, settings.cache_ttl_default)

                self.stats.hits += 1
                return file_value

        self.stats.misses += 1
        return None

    async def set(self, key: str, value: Any, ttl: Optional[int] = None) -> bool:
        """Set value in cache with TTL."""
        if ttl is None:
            ttl = settings.cache_ttl_default

        success = True

        # Store in memory cache
        if self.strategy in [CacheStrategy.MEMORY, CacheStrategy.HYBRID]:
            success = await self._set_memory(key, value, ttl)

        # Store in Redis
        if self.strategy in [CacheStrategy.REDIS, CacheStrategy.HYBRID] and self._redis_client:
            try:
                self._redis_client.setex(
                    key,
                    ttl,
                    json.dumps(value, default=str),
                )
            except Exception as e:
                logger.warning(f"Redis set failed: {e}")
                success = False

        # Store in file cache (for persistence)
        if self.config.get("enable_file_cache", True):
            await self._save_to_file(key, value, ttl)

        async with self._memory_lock:
            self.stats.sets += 1

        return success

    async def _set_memory(self, key: str, value: Any, ttl: int) -> bool:
        """Set value in memory cache."""
        try:
            entry = CacheEntry(
                key=key,
                value=value,
                timestamp=time.time(),
                ttl=ttl,
                hits=0,
            )

            async with self._memory_lock:
                # Check if we need to evict entries
                if len(self._memory_cache) >= settings.cache_max_size:
                    self._evict_entries()

                self._memory_cache[key] = entry

            return True
        except Exception as e:
            logger.error(f"Memory cache set failed: {e}")
            return False

    async def delete(self, key: str) -> bool:
        """Delete value from cache."""
        success = True

        # Delete from memory
        if self.strategy in [CacheStrategy.MEMORY, CacheStrategy.HYBRID]:
            async with self._memory_lock:
                if key in self._memory_cache:
                    del self._memory_cache[key]
                    self.stats.deletes += 1

        # Delete from Redis
        if self.strategy in [CacheStrategy.REDIS, CacheStrategy.HYBRID] and self._redis_client:
            try:
                self._redis_client.delete(key)
            except Exception as e:
                logger.warning(f"Redis delete failed: {e}")
                success = False

        # Delete from file
        await self._delete_file(key)

        return success

    async def clear(self, pattern: Optional[str] = None) -> int:
        """Clear cache entries, optionally by pattern."""
        deleted_count = 0

        # Clear memory cache
        if self.strategy in [CacheStrategy.MEMORY, CacheStrategy.HYBRID]:
            async with self._memory_lock:
                if pattern:
                    keys_to_delete = [k for k in self._memory_cache.keys() if pattern in k]
                    for key in keys_to_delete:
                        del self._memory_cache[key]
                        deleted_count += 1
                else:
                    deleted_count = len(self._memory_cache)
                    self._memory_cache.clear()

        # Clear Redis cache
        if self.strategy in [CacheStrategy.REDIS, CacheStrategy.HYBRID] and self._redis_client:
            try:
                if pattern:
                    keys = self._redis_client.keys(f"*{pattern}*")
                    if keys:
                        self._redis_client.delete(*keys)
                        deleted_count += len(keys)
                else:
                    self._redis_client.flushdb()
                    deleted_count += 1  # Count as one operation
            except Exception as e:
                logger.warning(f"Redis clear failed: {e}")

        # Clear file cache
        deleted_count += await self._clear_files(pattern)

        return deleted_count

    def _evict_entries(self):
        """Evict entries based on LRU policy."""
        # Sort by hits (least used first) and age (oldest first)
        entries = sorted(
            self._memory_cache.items(),
            key=lambda x: (x[1].hits, x[1].age)
        )

        # Remove 10% of entries or at least 10 entries
        evict_count = max(10, len(entries) // 10)
        for key, _ in entries[:evict_count]:
            del self._memory_cache[key]
            self.stats.evictions += 1

        logger.debug(f"Evicted {evict_count} entries from cache")

    async def _save_to_file(self, key: str, value: Any, ttl: int):
        """Save value to file cache."""
        try:
            # Create hash for filename
            key_hash = hashlib.md5(key.encode()).hexdigest()
            cache_file = self._cache_dir / f"{key_hash}.json"

            cache_data = {
                "key": key,
                "value": value,
                "timestamp": time.time(),
                "ttl": ttl,
            }

            # Use asyncio for file operations
            loop = asyncio.get_event_loop()
            await loop.run_in_executor(
                None,
                lambda: cache_file.write_text(json.dumps(cache_data, default=str))
            )
        except Exception as e:
            logger.debug(f"File cache save failed: {e}")

    async def _get_from_file(self, key: str) -> Optional[Any]:
        """Get value from file cache."""
        try:
            key_hash = hashlib.md5(key.encode()).hexdigest()
            cache_file = self._cache_dir / f"{key_hash}.json"

            if not cache_file.exists():
                return None

            # Use asyncio for file operations
            loop = asyncio.get_event_loop()
            content = await loop.run_in_executor(None, cache_file.read_text)
            cache_data = json.loads(content)

            # Check if expired
            if time.time() - cache_data["timestamp"] > cache_data["ttl"]:
                cache_file.unlink(missing_ok=True)
                return None

            return cache_data["value"]
        except Exception as e:
            logger.debug(f"File cache load failed: {e}")
            return None

    async def _delete_file(self, key: str):
        """Delete file cache entry."""
        try:
            key_hash = hashlib.md5(key.encode()).hexdigest()
            cache_file = self._cache_dir / f"{key_hash}.json"
            cache_file.unlink(missing_ok=True)
        except Exception:
            pass

    async def _clear_files(self, pattern: Optional[str] = None) -> int:
        """Clear file cache entries."""
        try:
            count = 0
            for cache_file in self._cache_dir.glob("*.json"):
                if pattern:
                    # Need to read file to check key
                    try:
                        content = cache_file.read_text()
                        cache_data = json.loads(content)
                        if pattern in cache_data.get("key", ""):
                            cache_file.unlink()
                            count += 1
                    except Exception:
                        pass
                else:
                    cache_file.unlink()
                    count += 1
            return count
        except Exception as e:
            logger.error(f"File cache clear failed: {e}")
            return 0

    def get_stats(self) -> CacheStats:
        """Get current cache statistics."""
        # Update statistics
        total_entries = len(self._memory_cache)
        expired_entries = sum(1 for entry in self._memory_cache.values() if entry.is_expired)
        valid_entries = total_entries - expired_entries

        # Calculate hit rate
        total_operations = self.stats.hits + self.stats.misses
        hit_rate = self.stats.hits / total_operations if total_operations > 0 else 0.0
        miss_rate = 1.0 - hit_rate if total_operations > 0 else 0.0

        # Calculate ages
        ages = [entry.age for entry in self._memory_cache.values() if not entry.is_expired]
        oldest_age = max(ages) if ages else 0.0
        newest_age = min(ages) if ages else 0.0

        # Update stats
        self.stats.total_entries = total_entries
        self.stats.valid_entries = valid_entries
        self.stats.expired_entries = expired_entries
        self.stats.hit_rate = hit_rate
        self.stats.miss_rate = miss_rate
        self.stats.oldest_entry_age_seconds = oldest_age
        self.stats.newest_entry_age_seconds = newest_age

        return self.stats

    async def cleanup(self):
        """Cleanup expired entries."""
        async with self._memory_lock:
            expired_keys = [
                key for key, entry in self._memory_cache.items()
                if entry.is_expired
            ]
            for key in expired_keys:
                del self._memory_cache[key]
                self.stats.evictions += 1

        # Cleanup file cache
        await self._cleanup_files()

        logger.debug(f"Cache cleanup completed, removed {len(expired_keys)} expired entries")

    async def _cleanup_files(self):
        """Cleanup expired file cache entries."""
        try:
            for cache_file in self._cache_dir.glob("*.json"):
                try:
                    content = cache_file.read_text()
                    cache_data = json.loads(content)
                    if time.time() - cache_data["timestamp"] > cache_data["ttl"]:
                        cache_file.unlink()
                except Exception:
                    cache_file.unlink(missing_ok=True)
        except Exception as e:
            logger.debug(f"File cache cleanup failed: {e}")


# =============================================================================
# Enhanced Quote Service
# =============================================================================

class QuoteService:
    """Enhanced quote service with multiple data sources."""

    def __init__(self, cache: EnhancedCache):
        self.cache = cache
        self.google_sheets_service: Optional[AsyncGoogleSheetsService] = None
        self.google_apps_script_client: Optional[GoogleAppsScriptClient] = None

        # Request tracking
        self.request_count = 0
        self.success_count = 0
        self.error_count = 0

        # Provider statistics
        self.provider_stats = defaultdict(lambda: {
            "requests": 0,
            "success": 0,
            "errors": 0,
            "response_time_sum": 0.0,
        })

        logger.info("QuoteService initialized")

    async def initialize(self):
        """Initialize service dependencies."""
        # --- GOOGLE SHEETS SERVICE ---
        try:
            # Ensure env vars are compatible with SheetsConfig.from_env()
            # If Render only has GOOGLE_SHEETS_CREDENTIALS + SPREADSHEET_ID,
            # we map them to what google_sheets_service expects.
            if settings.google_sheets_credentials_json and not (
                os.getenv("GOOGLE_SHEETS_CREDENTIALS_JSON") or os.getenv("GOOGLE_CREDENTIALS_PATH")
            ):
                os.environ["GOOGLE_SHEETS_CREDENTIALS_JSON"] = settings.google_sheets_credentials_json

            if settings.google_sheets_spreadsheet_id and not os.getenv("SPREADSHEET_ID"):
                os.environ["SPREADSHEET_ID"] = settings.google_sheets_spreadsheet_id

            # Some older versions of google_sheets_service may still expect this:
            if settings.google_sheets_spreadsheet_id and not os.getenv("KSA_TADAWUL_SHEET_ID"):
                os.environ["KSA_TADAWUL_SHEET_ID"] = settings.google_sheets_spreadsheet_id

            sheets_config = SheetsConfig.from_env()
            self.google_sheets_service = await get_google_sheets_service(sheets_config)
            logger.info(
                f"Google Sheets service initialized for spreadsheet "
                f"{settings.google_sheets_spreadsheet_id} / sheet '{settings.tadawul_market_sheet}'"
            )

            # Small probe to see if KSA_Tadawul has rows (for easier diagnostics)
            try:
                probe_rows = await self.google_sheets_service.read_ksa_tadawul_market(
                    use_cache=False,
                    validate=False,
                    filters=None,
                    limit=5,
                    as_dataframe=False,
                )
                probe_count = len(probe_rows) if probe_rows else 0
                sample_symbols = [
                    (row.get("symbol") or row.get("ticker"))
                    for row in (probe_rows or [])
                ]
                logger.info(
                    f"KSA Tadawul initial probe: {probe_count} rows "
                    f"(sample symbols: {sample_symbols})"
                )
            except Exception as probe_err:
                logger.warning(f"Probe read_ksa_tadawul_market failed during init: {probe_err}")

        except Exception as e:
            logger.error(f"Failed to initialize Google Sheets service: {e}")

        # --- GOOGLE APPS SCRIPT CLIENT ---
        try:
            self.google_apps_script_client = await get_google_apps_script_client()
            logger.info("Google Apps Script client initialized")
        except Exception as e:
            logger.error(f"Failed to initialize Google Apps Script client: {e}")

    async def get_quote(self, symbol: str, use_cache: bool = True) -> Optional[Quote]:
        """Get single quote with enhanced error handling."""
        self.request_count += 1

        # Normalize symbol
        symbol = symbol.strip().upper()
        if not symbol:
            return None

        # Check cache first
        cache_key = f"quote:{symbol}"
        if use_cache:
            cached = await self.cache.get(cache_key)
            if cached:
                # Convert cached dict to Quote model
                if isinstance(cached, dict):
                    cached_quote = Quote(**cached)
                    cached_quote.cached = True
                    cached_quote.cache_ttl = await self._get_cache_ttl_remaining(cache_key)
                    self.success_count += 1
                    return cached_quote

        # Try to get quote from providers
        quote = await self._get_quote_from_providers(symbol)

        if quote and quote.is_valid:
            # Cache the quote
            await self.cache.set(cache_key, quote.dict(), settings.cache_ttl_short)
            self.success_count += 1
        else:
            self.error_count += 1

        return quote

    async def get_quotes(self, symbols: List[str], use_cache: bool = True) -> QuoteResponse:
        """Get multiple quotes with parallel processing."""
        start_time = time.time()
        request_id = str(uuid.uuid4())

        # Track statistics
        cache_hits = 0
        cache_misses = 0
        success_count = 0
        error_count = 0
        providers_used = set()

        # Process symbols in parallel
        quotes: List[Quote] = []
        tasks = []

        for symbol in symbols:
            task = self._process_symbol(
                symbol,
                use_cache,
                request_id,
            )
            tasks.append(task)

        # Gather results
        results = await asyncio.gather(*tasks, return_exceptions=True)

        for result in results:
            if isinstance(result, Exception):
                error_count += 1
                continue

            symbol, quote, cache_hit, provider = result

            if cache_hit:
                cache_hits += 1
            else:
                cache_misses += 1

            if quote and quote.is_valid:
                success_count += 1
                quotes.append(quote)
                if provider:
                    providers_used.add(provider)
            else:
                error_count += 1
                # Create error quote (now supported with message field)
                error_quote = Quote(
                    ticker=symbol,
                    symbol=symbol,
                    status=QuoteStatus.ERROR,
                    quality=DataQuality.UNKNOWN,
                    message="Failed to fetch quote",
                )
                quotes.append(error_quote)

        # Build response
        execution_time = time.time() - start_time

        return QuoteResponse(
            quotes=quotes,
            request_id=request_id,
            success_count=success_count,
            error_count=error_count,
            cache_hits=cache_hits,
            cache_misses=cache_misses,
            execution_time=execution_time,
            providers_used=list(providers_used),
            status="partial" if error_count > 0 else "success",
            message=f"Retrieved {success_count} of {len(symbols)} quotes",
        )

    async def _process_symbol(
        self,
        symbol: str,
        use_cache: bool,
        request_id: str,
    ) -> Tuple[str, Optional[Quote], bool, Optional[str]]:
        """Process single symbol quote."""
        cache_hit = False
        provider = None

        # Normalize symbol for cache key
        norm_symbol = symbol.strip().upper()
        cache_key = f"quote:{norm_symbol}"

        # Check cache first
        if use_cache:
            cached = await self.cache.get(cache_key)
            if cached:
                cache_hit = True
                if isinstance(cached, dict):
                    quote = Quote(**cached)
                    quote.cached = True
                    quote.cache_ttl = await self._get_cache_ttl_remaining(cache_key)
                    return norm_symbol, quote, cache_hit, "cache"

        # Get from providers
        quote = await self._get_quote_from_providers(norm_symbol)

        if quote and quote.is_valid:
            # Cache the quote
            await self.cache.set(cache_key, quote.dict(), settings.cache_ttl_short)
            provider = quote.provider

        return norm_symbol, quote, cache_hit, provider

    async def _get_quote_from_providers(self, symbol: str) -> Optional[Quote]:
        """
        Get quote from available providers.

        Uses providers_enabled + provider_priority from settings,
        and then filters by what is actually available for this symbol.
        """
        providers_to_try: List[Tuple[str, Callable[[str], Any]]] = []

        # Build available providers for this symbol
        available_providers: Dict[str, Callable[[str], Any]] = {}

        is_tadawul = symbol.endswith(".SR") or "TADAWUL" in symbol.upper()

        # Google Sheets is valid for KSA (and optionally for others if you want)
        if self.google_sheets_service:
            available_providers["google_sheets"] = self._get_quote_from_sheets

        # Google Apps Script is only meaningful for Tadawul in this design
        if self.google_apps_script_client and is_tadawul:
            available_providers["google_apps_script"] = self._get_quote_from_gas

        # Build provider list from priority + enabled flags
        for name in settings.provider_priority:
            if name in settings.providers_enabled and name in available_providers:
                providers_to_try.append((name, available_providers[name]))

        # Fallback: if settings misconfigured and nothing selected, but something is available
        if not providers_to_try and available_providers:
            providers_to_try = list(available_providers.items())

        if not providers_to_try:
            logger.warning(f"No providers available for symbol {symbol} with current configuration")
            return None

        # Try providers in order
        for provider_name, provider_func in providers_to_try:
            try:
                start_time = time.time()
                quote = await provider_func(symbol)
                response_time = time.time() - start_time

                # Update provider statistics
                stats = self.provider_stats[provider_name]
                stats["requests"] += 1

                if quote and quote.is_valid:
                    stats["success"] += 1
                    stats["response_time_sum"] += response_time
                    quote.provider = provider_name
                    return quote
                else:
                    stats["errors"] += 1
            except Exception as e:
                logger.warning(f"Provider {provider_name} failed for {symbol}: {e}")
                self.provider_stats[provider_name]["errors"] += 1
                continue

        return None

    async def _get_quote_from_sheets(self, symbol: str) -> Optional[Quote]:
        """
        Get quote from Google Sheets (KSA_Tadawul sheet).

        This version does NOT depend on a specific filter key name inside
        google_sheets_service; instead it fetches a slice of the KSA sheet and
        matches rows where either 'symbol' or 'ticker' equals the normalized symbol.
        """
        if not self.google_sheets_service:
            return None

        try:
            # Normalize Tadawul symbol: ensure it ends with .SR only once
            if symbol.endswith(".SR"):
                normalized_symbol = symbol.upper()
            else:
                normalized_symbol = f"{symbol.upper()}.SR"

            # Read a bounded number of KSA rows and filter in Python
            market_data = await self.google_sheets_service.read_ksa_tadawul_market(
                use_cache=True,
                validate=True,
                filters=None,
                limit=500,           # enough for your watchlist / universe
                as_dataframe=False,
            )

            if not market_data:
                logger.warning(
                    f"Google Sheets returned no KSA Tadawul rows when searching for {normalized_symbol}"
                )
                return None

            # Match by 'symbol' or 'ticker' field (whichever mapping your service uses)
            matches = [
                row
                for row in market_data
                if str(row.get("symbol", "")).strip().upper() == normalized_symbol
                or str(row.get("ticker", "")).strip().upper() == normalized_symbol
            ]

            if not matches:
                logger.warning(
                    f"KSA sheet '{settings.tadawul_market_sheet}' has {len(market_data)} rows "
                    f"but none match symbol {normalized_symbol}"
                )
                return None

            data = matches[0]

            # Convert to Quote model
            quote = Quote(
                ticker=normalized_symbol,
                symbol=normalized_symbol,
                exchange="TADAWUL",
                currency=data.get("currency", "SAR"),
                name=data.get("company_name"),
                sector=data.get("sector"),
                price=data.get("last_price"),
                previous_close=data.get("previous_close"),
                open=data.get("open_price"),
                high=data.get("day_high"),
                low=data.get("day_low"),
                volume=data.get("volume"),
                avg_volume=data.get("avg_volume_30d"),
                change=data.get("change_value"),
                change_percent=data.get("change_pct"),
                market_cap=data.get("market_cap"),
                shares_outstanding=data.get("shares_outstanding"),
                free_float=data.get("free_float"),
                pe_ratio=data.get("pe"),
                pb_ratio=data.get("pb"),
                dividend_yield=data.get("dividend_yield"),
                eps=data.get("eps"),
                roe=data.get("roe"),
                rsi=data.get("rsi_14"),
                macd=data.get("macd"),
                moving_avg_20=data.get("ma_20d"),
                moving_avg_50=data.get("ma_50d"),
                volatility=data.get("volatility"),
                status=QuoteStatus.OK if data.get("last_price") not in (None, 0) else QuoteStatus.NO_DATA,
                quality=DataQuality.HIGH,
                provider="google_sheets",
                source=settings.tadawul_market_sheet,
                raw_data=data,
            )

            # Only return if it passes minimal validity check (non-null price)
            if not quote.is_valid:
                logger.warning(
                    f"KSA row found for {normalized_symbol} but price is missing or zero; treating as NO_DATA"
                )
                return None

            return quote

        except Exception as e:
            logger.error(f"Error getting quote from sheets for {symbol}: {e}")
            return None

    async def _get_quote_from_gas(self, symbol: str) -> Optional[Quote]:
        """Get quote from Google Apps Script."""
        if not self.google_apps_script_client:
            return None

        try:
            # Normalize Tadawul symbol similarly
            if symbol.endswith(".SR"):
                normalized_symbol = symbol.upper()
            else:
                normalized_symbol = f"{symbol.upper()}.SR"

            response = await self.google_apps_script_client.get_symbols_data(
                symbol=normalized_symbol,
                action="get",
                include_metadata=True,
            )

            if not response.success or not response.data:
                return None

            # Extract data from response
            data = response.data

            # Convert to Quote model
            quote = Quote(
                ticker=normalized_symbol,
                symbol=normalized_symbol,
                exchange="TADAWUL",
                currency="SAR",
                price=data.get("price"),
                previous_close=data.get("previous_close"),
                change=data.get("change"),
                change_percent=data.get("change_percent"),
                volume=data.get("volume"),
                market_cap=data.get("market_cap"),
                status=QuoteStatus.OK,
                quality=DataQuality.MEDIUM,
                provider="google_apps_script",
                source="google_apps_script",
                raw_data=data,
            )

            if not quote.is_valid:
                return None

            return quote

        except Exception as e:
            logger.error(f"Error getting quote from GAS for {symbol}: {e}")
            return None

    async def _get_cache_ttl_remaining(self, cache_key: str) -> Optional[int]:
        """Get remaining TTL for cache key (simplified)."""
        return settings.cache_ttl_short

    def get_stats(self) -> Dict[str, Any]:
        """Get service statistics."""
        total_requests = self.request_count
        success_rate = self.success_count / total_requests if total_requests > 0 else 0.0

        # Provider statistics
        provider_details: Dict[str, Any] = {}
        for provider, stats in self.provider_stats.items():
            total = stats["requests"]
            if total > 0:
                success_rate_provider = stats["success"] / total if total > 0 else 0.0
                avg_response_time = (
                    stats["response_time_sum"] / stats["success"]
                    if stats["success"] > 0 else 0.0
                )
            else:
                success_rate_provider = 0.0
                avg_response_time = 0.0

            provider_details[provider] = {
                "requests": total,
                "success": stats["success"],
                "errors": stats["errors"],
                "success_rate": success_rate_provider,
                "avg_response_time": avg_response_time,
            }

        return {
            "total_requests": total_requests,
            "successful_requests": self.success_count,
            "failed_requests": self.error_count,
            "success_rate": success_rate,
            "providers": provider_details,
        }


# =============================================================================
# Application Lifecycle & Middleware
# =============================================================================

# Global variables
app_start_time = time.time()
cache = EnhancedCache(strategy=settings.cache_strategy)
quote_service = QuoteService(cache)
limiter = Limiter(key_func=get_remote_address)
security = HTTPBearer(auto_error=False)

# Store request contexts for monitoring
request_contexts: Dict[str, RequestContext] = {}


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Application lifespan management."""
    # Startup
    logger.info(f"🚀 Starting {settings.app_name} v{settings.app_version}")
    logger.info(f"🌍 Environment: {settings.environment.value}")
    logger.info(f"🔐 Authentication: {'Enabled' if settings.require_auth else 'Disabled'}")
    logger.info(f"💾 Cache Strategy: {settings.cache_strategy.value}")
    logger.info(f"📈 Rate Limiting: {'Enabled' if settings.rate_limit_enabled else 'Disabled'}")

    # Validate configuration
    issues = settings.validate_configuration()
    if issues:
        logger.warning("⚠️ Configuration issues found:")
        for issue in issues:
            logger.warning(f"  - {issue}")

    # Initialize services
    await quote_service.initialize()

    yield

    # Shutdown
    logger.info("🛑 Shutting down...")
    await close_google_sheets_service()
    await close_google_apps_script_client()
    logger.info("✅ Cleanup completed")


# Create FastAPI app with lifespan
app = FastAPI(
    title=settings.app_name,
    version=settings.app_version,
    description=settings.app_description,
    lifespan=lifespan,
    docs_url="/docs" if settings.enable_swagger else None,
    redoc_url="/redoc" if settings.enable_redoc else None,
    default_response_class=ORJSONResponse,
)

# Add middleware
if settings.cors_enabled:
    app.add_middleware(
        CORSMiddleware,
        allow_origins=settings.cors_origins,
        allow_credentials=True,
        allow_methods=settings.cors_methods,
        allow_headers=settings.cors_headers,
    )

if settings.rate_limit_enabled:
    app.state.limiter = limiter
    app.add_exception_handler(RateLimitExceeded, _rate_limit_exceeded_handler)
    app.add_middleware(SlowAPIMiddleware)

if settings.trusted_hosts:
    app.add_middleware(
        TrustedHostMiddleware,
        allowed_hosts=settings.trusted_hosts,
    )

app.add_middleware(GZipMiddleware, minimum_size=1000)

# =============================================================================
# Enhanced Middleware for Monitoring
# =============================================================================

@app.middleware("http")
async def monitoring_middleware(request: Request, call_next):
    """Enhanced monitoring middleware."""
    # Generate request ID
    request_id = str(uuid.uuid4())

    # Create request context
    context = RequestContext(
        request_id=request_id,
        client_ip=request.client.host if request.client else None,
        user_agent=request.headers.get("user-agent"),
        endpoint=str(request.url.path),
        method=request.method,
        metadata={
            "query_params": dict(request.query_params),
            "headers": dict(request.headers),
        },
    )

    # Store context
    request_contexts[request_id] = context

    # Process request
    try:
        response = await call_next(request)

        # Add headers
        response.headers["X-Request-ID"] = request_id
        response.headers["X-Response-Time"] = f"{context.duration:.3f}"

        # Log request
        if settings.request_logging:
            logger.info(
                f"Request {request_id}: {request.method} {request.url.path} "
                f"- Status: {response.status_code} - Duration: {context.duration:.3f}s"
            )

        return response

    except Exception as e:
        # Log error
        logger.error(
            f"Request {request_id} failed: {request.method} {request.url.path} "
            f"- Error: {type(e).__name__}: {str(e)}"
        )
        raise

    finally:
        # Cleanup
        if request_id in request_contexts:
            del request_contexts[request_id]


# =============================================================================
# Enhanced Authentication
# =============================================================================

async def verify_authentication(
    request: Request,
    credentials: Optional[HTTPAuthorizationCredentials] = Depends(security),
) -> bool:
    """Enhanced authentication verification."""
    if not settings.require_auth:
        return True

    # Collect valid tokens
    valid_tokens: List[str] = []
    if settings.app_token:
        valid_tokens.append(settings.app_token)
    if settings.backup_app_token:
        valid_tokens.append(settings.backup_app_token)
    if settings.tfb_app_token:
        valid_tokens.append(settings.tfb_app_token)

    if not valid_tokens:
        if settings.is_production:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="Authentication misconfigured",
            )
        return True

    # Check various token sources
    token: Optional[str] = None

    # 1. Authorization header (Bearer)
    if credentials:
        token = credentials.credentials.strip()

    # 2. Query parameter
    if not token:
        token = request.query_params.get("token", "").strip()

    # 3. Custom headers
    if not token:
        token = (
            request.headers.get(settings.api_key_header, "").strip()
            or request.headers.get("X-API-Key", "").strip()
            or request.headers.get("X-TFB-Token", "").strip()
            or request.headers.get("X-App-Token", "").strip()
        )

    # 4. Raw Authorization (non-Bearer)
    if not token:
        auth_header = request.headers.get("Authorization", "").strip()
        if auth_header and not auth_header.lower().startswith("bearer "):
            token = auth_header

    # Validate token
    if not token:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Missing authentication token",
            headers={"WWW-Authenticate": "Bearer"},
        )

    if token not in valid_tokens:
        # Log failed attempt (with token hash for security)
        token_hash = hashlib.sha256(token.encode()).hexdigest()[:16]
        logger.warning(f"Invalid token attempt: {token_hash}")
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid authentication token",
            headers={"WWW-Authenticate": "Bearer"},
        )

    return True


# =============================================================================
# Enhanced Routes
# =============================================================================

@app.get("/", include_in_schema=False)
async def root():
    """Root endpoint with redirect to docs."""
    return RedirectResponse(url="/docs")


@app.get("/health", response_model=HealthResponse, tags=["Monitoring"])
@limiter.limit("30/minute")
async def health_check(request: Request):
    """
    Enhanced health check endpoint with comprehensive system monitoring.

    Returns detailed health status of all services and dependencies.
    """
    start_time = time.time()

    # Check services
    services: Dict[str, ServiceHealth] = {}

    # Google Sheets health
    sheets_health = ServiceHealth(
        service="google_sheets",
        status=HealthStatus.HEALTHY if quote_service.google_sheets_service else HealthStatus.UNHEALTHY,
        version=settings.app_version,
        timestamp=datetime.datetime.now(datetime.timezone.utc),
        details={
            "configured": settings.has_google_sheets,
            "initialized": quote_service.google_sheets_service is not None,
        },
    )
    services["google_sheets"] = sheets_health

    # Google Apps Script health
    gas_health = ServiceHealth(
        service="google_apps_script",
        status=HealthStatus.HEALTHY if quote_service.google_apps_script_client else HealthStatus.UNHEALTHY,
        version=settings.app_version,
        timestamp=datetime.datetime.now(datetime.timezone.utc),
        details={
            "configured": settings.has_google_apps_script,
            "initialized": quote_service.google_apps_script_client is not None,
        },
    )
    services["google_apps_script"] = gas_health

    # Cache health
    cache_stats = cache.get_stats()
    cache_health_status = (
        HealthStatus.HEALTHY if cache_stats is not None else HealthStatus.DEGRADED
    )
    cache_health = ServiceHealth(
        service="cache",
        status=cache_health_status,
        version=settings.app_version,
        timestamp=datetime.datetime.now(datetime.timezone.utc),
        details=cache_stats.dict(),
    )
    services["cache"] = cache_health

    # Quote service health
    quote_stats = quote_service.get_stats()
    if quote_stats["total_requests"] == 0:
        quote_status = HealthStatus.DEGRADED
        quote_message = "No quote requests processed yet"
    else:
        quote_status = (
            HealthStatus.HEALTHY if quote_stats["success_rate"] > 0.5 else HealthStatus.DEGRADED
        )
        quote_message = None

    quote_health = ServiceHealth(
        service="quote_service",
        status=quote_status,
        version=settings.app_version,
        timestamp=datetime.datetime.now(datetime.timezone.utc),
        details=quote_stats,
        message=quote_message,
    )
    services["quote_service"] = quote_health

    # Determine overall status
    overall_status = HealthStatus.HEALTHY
    for service in services.values():
        if service.status == HealthStatus.UNHEALTHY:
            overall_status = HealthStatus.UNHEALTHY
            break
        elif service.status == HealthStatus.DEGRADED and overall_status == HealthStatus.HEALTHY:
            overall_status = HealthStatus.DEGRADED

    # System metrics
    import psutil
    system_metrics = {
        "cpu_percent": psutil.cpu_percent(interval=0.1),
        "memory_percent": psutil.virtual_memory().percent,
        "disk_usage": psutil.disk_usage("/").percent,
        "process_memory_mb": psutil.Process().memory_info().rss / 1024 / 1024,
        "process_threads": psutil.Process().num_threads(),
    }

    # Performance metrics
    performance_metrics = {
        "response_time_ms": (time.time() - start_time) * 1000,
        "active_requests": len(request_contexts),
        "uptime_seconds": time.time() - app_start_time,
        "cache_hit_rate": cache_stats.hit_rate,
        "quote_success_rate": quote_stats["success_rate"],
    }

    return HealthResponse(
        status=overall_status,
        version=settings.app_version,
        environment=settings.environment.value,
        uptime=time.time() - app_start_time,
        services=services,
        system=system_metrics,
        performance=performance_metrics,
    )


@app.get("/v1/quote", response_model=QuoteResponse, tags=["Market Data"])
@limiter.limit(settings.rate_limit_default)
async def get_quotes(
    request: Request,
    tickers: str = Query(..., description="Comma-separated list of ticker symbols"),
    use_cache: bool = Query(True, description="Use cache for faster responses"),
    detailed: bool = Query(False, description="Include detailed financial data"),
    auth: bool = Depends(verify_authentication),
):
    """
    Enhanced quote endpoint with multiple data sources and caching.

    Supports Tadawul symbols (.SR) and provides comprehensive market data.
    """
    try:
        # Parse tickers
        symbols = [s.strip().upper() for s in tickers.split(",") if s.strip()]

        if not symbols:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="No tickers provided",
            )

        if len(symbols) > 100:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Maximum 100 tickers per request",
            )

        # Get quotes
        response = await quote_service.get_quotes(symbols, use_cache=use_cache)

        # The monitoring middleware already attaches an X-Request-ID header to the response.
        # We keep response.request_id as generated inside QuoteResponse.

        return response

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Quote endpoint error: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Internal server error: {str(e)}",
        )


@app.get("/v1/quote/{symbol}", response_model=Quote, tags=["Market Data"])
@limiter.limit(settings.rate_limit_strict)
async def get_single_quote(
    request: Request,
    symbol: str,
    use_cache: bool = Query(True, description="Use cache for faster response"),
    auth: bool = Depends(verify_authentication),
):
    """
    Get single quote with enhanced data quality assessment.

    Provides comprehensive data for individual symbols with quality metrics.
    """
    try:
        quote = await quote_service.get_quote(symbol, use_cache=use_cache)

        if not quote or not quote.is_valid:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"No data available for symbol: {symbol}",
            )

        return quote

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Single quote endpoint error for {symbol}: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Internal server error: {str(e)}",
        )


@app.get("/v1/tadawul/market", tags=["Market Data"])
@limiter.limit("30/minute")
async def get_tadawul_market(
    request: Request,
    limit: int = Query(50, ge=1, le=500, description="Maximum number of results"),
    sector: Optional[str] = Query(None, description="Filter by sector"),
    min_market_cap: Optional[float] = Query(None, ge=0, description="Minimum market cap"),
    max_market_cap: Optional[float] = Query(None, ge=0, description="Maximum market cap"),
    sort_by: str = Query("market_cap", description="Sort field"),
    sort_order: str = Query("desc", description="Sort order (asc/desc)"),
    auth: bool = Depends(verify_authentication),
):
    """
    Enhanced Tadawul market data endpoint.

    Returns comprehensive market data from Google Sheets with filtering and sorting.
    """
    try:
        if not quote_service.google_sheets_service:
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail="Google Sheets service not available",
            )

        # Build filters
        filters: Dict[str, Any] = {}
        if sector:
            filters["sector"] = {"contains": sector}
        if min_market_cap is not None:
            filters.setdefault("market_cap", {})["min"] = min_market_cap
        if max_market_cap is not None:
            filters.setdefault("market_cap", {})["max"] = max_market_cap

        # Get market data
        market_data = await quote_service.google_sheets_service.read_ksa_tadawul_market(
            use_cache=True,
            validate=True,
            filters=filters if filters else None,
            limit=limit,
            as_dataframe=False,
        )

        # Sort data
        if sort_by in ["market_cap", "last_price", "volume", "pe", "dividend_yield"]:
            market_data.sort(
                key=lambda x: x.get(sort_by, 0) or 0,
                reverse=(sort_order.lower() == "desc"),
            )

        # Add metadata
        response = {
            "timestamp": datetime.datetime.now(datetime.timezone.utc).isoformat(),
            "count": len(market_data),
            "filters": {
                "sector": sector,
                "min_market_cap": min_market_cap,
                "max_market_cap": max_market_cap,
            },
            "sorting": {
                "by": sort_by,
                "order": sort_order,
            },
            "data": market_data,
        }

        return response

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Market data endpoint error: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Internal server error: {str(e)}",
        )


@app.get("/v1/cache/stats", tags=["Cache"])
@limiter.limit("10/minute")
async def get_cache_stats(
    request: Request,
    auth: bool = Depends(verify_authentication),
):
    """Get comprehensive cache statistics."""
    stats = cache.get_stats()
    return stats


@app.post("/v1/cache/clear", tags=["Cache"])
@limiter.limit("5/minute")
async def clear_cache(
    request: Request,
    pattern: Optional[str] = Query(None, description="Pattern to match keys"),
    background_tasks: BackgroundTasks = BackgroundTasks(),
    auth: bool = Depends(verify_authentication),
):
    """
    Clear cache entries.

    Can clear all cache or specific patterns.
    """
    try:
        deleted_count = await cache.clear(pattern)

        # Schedule cleanup task
        if settings.background_tasks_enabled:
            background_tasks.add_task(cache.cleanup)

        return {
            "status": "success",
            "deleted_count": deleted_count,
            "pattern": pattern,
            "timestamp": datetime.datetime.now(datetime.timezone.utc).isoformat(),
        }

    except Exception as e:
        logger.error(f"Cache clear error: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to clear cache: {str(e)}",
        )


@app.get("/v1/system/stats", tags=["Monitoring"])
@limiter.limit("30/minute")
async def get_system_stats(
    request: Request,
    auth: bool = Depends(verify_authentication),
):
    """Get comprehensive system statistics."""
    try:
        import psutil

        # Cache stats
        cache_stats = cache.get_stats()

        # Quote service stats
        quote_stats = quote_service.get_stats()

        # System metrics
        system_stats = {
            "cpu": {
                "percent": psutil.cpu_percent(interval=0.1),
                "cores": psutil.cpu_count(logical=False),
                "logical_cores": psutil.cpu_count(logical=True),
            },
            "memory": {
                "total_gb": psutil.virtual_memory().total / 1024 / 1024 / 1024,
                "available_gb": psutil.virtual_memory().available / 1024 / 1024 / 1024,
                "percent": psutil.virtual_memory().percent,
                "used_gb": psutil.virtual_memory().used / 1024 / 1024 / 1024,
            },
            "disk": {
                "total_gb": psutil.disk_usage("/").total / 1024 / 1024 / 1024,
                "used_gb": psutil.disk_usage("/").used / 1024 / 1024 / 1024,
                "free_gb": psutil.disk_usage("/").free / 1024 / 1024 / 1024,
                "percent": psutil.disk_usage("/").percent,
            },
            "process": {
                "memory_mb": psutil.Process().memory_info().rss / 1024 / 1024,
                "cpu_percent": psutil.Process().cpu_percent(interval=0.1),
                "threads": psutil.Process().num_threads(),
                "connections": len(psutil.Process().connections()),
            },
            "network": {
                "bytes_sent": psutil.net_io_counters().bytes_sent,
                "bytes_recv": psutil.net_io_counters().bytes_recv,
                "packets_sent": psutil.net_io_counters().packets_sent,
                "packets_recv": psutil.net_io_counters().packets_recv,
            },
        }

        # Application metrics
        app_metrics = {
            "uptime_seconds": time.time() - app_start_time,
            "active_requests": len(request_contexts),
            "total_requests": quote_stats["total_requests"],
            "success_rate": quote_stats["success_rate"],
            "cache_hit_rate": cache_stats.hit_rate,
            "providers": quote_stats["providers"],
        }

        response = {
            "timestamp": datetime.datetime.now(datetime.timezone.utc).isoformat(),
            "application": app_metrics,
            "cache": cache_stats.dict(),
            "system": system_stats,
        }

        return response

    except Exception as e:
        logger.error(f"System stats error: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to get system stats: {str(e)}",
        )


@app.get("/v1/request/history", tags=["Monitoring"])
@limiter.limit("10/minute")
async def get_request_history(
    request: Request,
    limit: int = Query(50, ge=1, le=1000, description="Maximum number of requests"),
    include_active: bool = Query(False, description="Include active requests"),
    auth: bool = Depends(verify_authentication),
):
    """Get recent request history for monitoring."""
    try:
        # Currently we only track active in-memory requests
        active_requests = list(request_contexts.values()) if include_active else []

        history = []
        for context in active_requests[:limit]:
            history.append({
                "request_id": context.request_id,
                "endpoint": context.endpoint,
                "method": context.method,
                "duration_seconds": context.duration,
                "client_ip": context.client_ip,
                "user_agent": context.user_agent,
                "start_time": context.start_time,
            })

        return {
            "timestamp": datetime.datetime.now(datetime.timezone.utc).isoformat(),
            "active_count": len(active_requests),
            "total_count": len(history),
            "requests": history,
        }

    except Exception as e:
        logger.error(f"Request history error: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to get request history: {str(e)}",
        )


@app.get("/v1/status", tags=["Monitoring"])
async def get_status(request: Request):
    """Public status endpoint for monitoring."""
    try:
        # Basic health check
        services_healthy = (
            quote_service.google_sheets_service is not None
            and quote_service.google_apps_script_client is not None
        )

        return {
            "status": "operational" if services_healthy else "degraded",
            "version": settings.app_version,
            "environment": settings.environment.value,
            "uptime_seconds": time.time() - app_start_time,
            "timestamp": datetime.datetime.now(datetime.timezone.utc).isoformat(),
            "services": {
                "google_sheets": quote_service.google_sheets_service is not None,
                "google_apps_script": quote_service.google_apps_script_client is not None,
                "cache": cache.get_stats().valid_entries > 0,
            },
        }

    except Exception as e:
        return {
            "status": "error",
            "error": str(e),
            "timestamp": datetime.datetime.now(datetime.timezone.utc).isoformat(),
        }


# =============================================================================
# Error Handlers
# =============================================================================

@app.exception_handler(HTTPException)
async def http_exception_handler(request: Request, exc: HTTPException):
    """Enhanced HTTP exception handler."""
    error_id = str(uuid.uuid4())

    logger.error(
        f"HTTP Exception {error_id}: {exc.status_code} - {exc.detail} "
        f"at {request.method} {request.url.path}"
    )

    return JSONResponse(
        status_code=exc.status_code,
        content={
            "error": {
                "id": error_id,
                "code": exc.status_code,
                "message": exc.detail,
                "timestamp": datetime.datetime.now(datetime.timezone.utc).isoformat(),
                "path": str(request.url.path),
                "method": request.method,
            }
        },
        headers=exc.headers,
    )


@app.exception_handler(Exception)
async def general_exception_handler(request: Request, exc: Exception):
    """General exception handler."""
    error_id = str(uuid.uuid4())

    logger.error(
        f"Unhandled Exception {error_id}: {type(exc).__name__}: {str(exc)} "
        f"at {request.method} {request.url.path}"
    )
    logger.debug(f"Traceback for {error_id}:\n{traceback.format_exc()}")

    return JSONResponse(
        status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
        content={
            "error": {
                "id": error_id,
                "code": "INTERNAL_SERVER_ERROR",
                "message": "Internal server error",
                "timestamp": datetime.datetime.now(datetime.timezone.utc).isoformat(),
                "path": str(request.url.path),
                "method": request.method,
            }
        },
    )


# =============================================================================
# Main Entry Point
# =============================================================================

def main():
    """Enhanced main entry point."""
    # Log startup information
    logger.info("=" * 70)
    logger.info(f"🚀 {settings.app_name} v{settings.app_version}")
    logger.info(f"🌍 Environment: {settings.environment.value}")
    logger.info(f"🔐 Authentication: {'Enabled' if settings.require_auth else 'Disabled'}")
    logger.info(f"💾 Cache Strategy: {settings.cache_strategy.value}")
    logger.info(f"📈 Rate Limiting: {'Enabled' if settings.rate_limit_enabled else 'Disabled'}")
    logger.info(f"⚡ HTTP Timeout: {settings.http_timeout}s")
    logger.info(f"🔄 Max Retries: {settings.http_max_retries}")
    logger.info(f"📊 Metrics: {'Enabled' if settings.metrics_enabled else 'Disabled'}")
    logger.info(f"🌐 CORS: {'Enabled' if settings.cors_enabled else 'Disabled'}")
    logger.info(f"🔗 Host: {settings.app_host}:{settings.app_port}")
    logger.info("=" * 70)

    # Validate configuration
    issues = settings.validate_configuration()
    if issues:
        logger.warning("⚠️ Configuration issues found:")
        for issue in issues:
            logger.warning(f"  - {issue}")

        if settings.is_production and issues:
            logger.warning("⚠️ Running in production with configuration issues!")

    # Start server
    uvicorn.run(
        app,
        host=settings.app_host,
        port=settings.app_port,
        workers=settings.app_workers if settings.is_production else 1,
        reload=settings.is_development,
        log_level=settings.log_level.value,
        access_log=settings.request_logging,
        timeout_keep_alive=settings.http_timeout,
    )


if __name__ == "__main__":
    main()
