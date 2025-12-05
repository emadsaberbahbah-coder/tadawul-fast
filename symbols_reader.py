# =============================================================================
# Enhanced Google Sheets Symbols Reader
# Version: 4.0.0 | Python: 3.11.9 | Render Optimized
# Integrated with: Tadawul Stock Analysis API
# =============================================================================

import os
import sys
import json
import asyncio
import hashlib
import logging
from pathlib import Path
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Tuple, Union
from dataclasses import dataclass, field
from enum import Enum
from concurrent.futures import ThreadPoolExecutor

# Core Framework
from pydantic import BaseModel, Field, validator
from pydantic_settings import BaseSettings
from tenacity import retry, stop_after_attempt, wait_exponential, wait_fixed, retry_if_exception_type

# Google Services
import gspread
from google.oauth2.service_account import Credentials
from google.auth.transport.requests import Request as GoogleRequest
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

# Monitoring & Logging
import structlog
from prometheus_client import Counter, Histogram, Gauge

# Caching
from cachetools import TTLCache, LRUCache
import redis
from redis import Redis

# Utilities
import orjson
from dateutil import parser

# System
import psutil

# =============================================================================
# Configuration
# =============================================================================

class SymbolsReaderSettings(BaseSettings):
    """Symbols reader configuration."""
    
    # Core Configuration
    enabled: bool = Field(True, description="Enable symbols reader")
    spreadsheet_id: str = Field("", description="Google Sheets spreadsheet ID")
    default_tab_name: str = Field("Market_Leaders", description="Default sheet name")
    
    # Cache Configuration
    cache_enabled: bool = Field(True, description="Enable caching")
    cache_ttl_seconds: int = Field(1800, description="Cache TTL in seconds")
    cache_max_size: int = Field(1000, description="Maximum cache size")
    cache_backend: str = Field("memory", description="Cache backend (memory, redis, hybrid)")
    
    # Redis Configuration
    redis_enabled: bool = Field(True, description="Enable Redis cache")
    redis_url: str = Field("", description="Redis connection URL")
    redis_ttl: int = Field(1800, description="Redis cache TTL")
    
    # Google Sheets Configuration
    google_sheets_credentials: str = Field("", description="Google service account JSON")
    google_sheets_timeout: int = Field(30, description="Google Sheets API timeout")
    google_sheets_max_retries: int = Field(3, description="Maximum retry attempts")
    
    # Performance Configuration
    max_concurrent_requests: int = Field(5, description="Maximum concurrent requests")
    request_timeout: int = Field(30, description="Request timeout in seconds")
    batch_size: int = Field(50, description="Batch processing size")
    
    # Fallback Configuration
    enable_fallback: bool = Field(True, description="Enable fallback to hardcoded symbols")
    fallback_symbols: str = Field(
        "7201.SR,1211.SR,2222.SR,2380.SR,4030.SR,4200.SR,1120.SR,1150.SR,2010.SR,2020.SR",
        description="Comma-separated fallback symbols"
    )
    
    # Monitoring Configuration
    enable_metrics: bool = Field(True, description="Enable Prometheus metrics")
    log_level: str = Field("INFO", description="Logging level")
    
    model_config = {
        "env_prefix": "SYMBOLS_",
        "case_sensitive": False,
        "extra": "ignore"
    }
    
    @validator('cache_ttl_seconds')
    def validate_cache_ttl(cls, v):
        """Validate cache TTL."""
        if v < 60:
            return 60
        if v > 86400:
            return 86400
        return v
    
    @validator('google_sheets_timeout')
    def validate_timeout(cls, v):
        """Validate timeout."""
        if v < 5:
            return 5
        if v > 120:
            return 120
        return v

# =============================================================================
# Prometheus Metrics
# =============================================================================

class SymbolsReaderMetrics:
    """Prometheus metrics for symbols reader."""
    
    def __init__(self):
        # Request metrics
        self.requests_total = Counter(
            'symbols_reader_requests_total',
            'Total symbols reader requests',
            ['operation', 'status']
        )
        
        self.request_duration_seconds = Histogram(
            'symbols_reader_request_duration_seconds',
            'Symbols reader request duration',
            ['operation'],
            buckets=(0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10)
        )
        
        # Cache metrics
        self.cache_hits_total = Counter(
            'symbols_reader_cache_hits_total',
            'Total symbols reader cache hits',
            ['cache_type']
        )
        
        self.cache_misses_total = Counter(
            'symbols_reader_cache_misses_total',
            'Total symbols reader cache misses',
            ['cache_type']
        )
        
        # Data metrics
        self.symbols_processed_total = Counter(
            'symbols_reader_symbols_processed_total',
            'Total symbols processed',
            ['source']
        )
        
        self.data_quality = Gauge(
            'symbols_reader_data_quality',
            'Data quality score (0-100)'
        )
        
        # Error metrics
        self.errors_total = Counter(
            'symbols_reader_errors_total',
            'Total symbols reader errors',
            ['error_type']
        )
    
    def record_request(self, operation: str, status: str, duration: float):
        """Record request metrics."""
        self.requests_total.labels(operation=operation, status=status).inc()
        self.request_duration_seconds.labels(operation=operation).observe(duration)
    
    def record_cache_hit(self, cache_type: str):
        """Record cache hit."""
        self.cache_hits_total.labels(cache_type=cache_type).inc()
    
    def record_cache_miss(self, cache_type: str):
        """Record cache miss."""
        self.cache_misses_total.labels(cache_type=cache_type).inc()
    
    def record_symbols_processed(self, source: str, count: int):
        """Record symbols processed."""
        for _ in range(count):
            self.symbols_processed_total.labels(source=source).inc()
    
    def record_error(self, error_type: str):
        """Record error."""
        self.errors_total.labels(error_type=error_type).inc()

# =============================================================================
# Logging Configuration
# =============================================================================

def setup_symbols_reader_logger():
    """Configure structured logging for symbols reader."""
    
    structlog.configure(
        processors=[
            structlog.stdlib.filter_by_level,
            structlog.contextvars.merge_contextvars,
            structlog.stdlib.add_logger_name,
            structlog.stdlib.add_log_level,
            structlog.stdlib.PositionalArgumentsFormatter(),
            structlog.processors.TimeStamper(fmt="iso", utc=True),
            structlog.processors.StackInfoRenderer(),
            structlog.processors.format_exc_info,
            structlog.processors.JSONRenderer(),
        ],
        wrapper_class=structlog.stdlib.BoundLogger,
        logger_factory=structlog.stdlib.LoggerFactory(),
        cache_logger_on_first_use=True,
    )
    
    return structlog.get_logger("symbols_reader")

# =============================================================================
# Data Models
# =============================================================================

class SymbolType(str, Enum):
    """Symbol type enumeration."""
    SAUDI = "saudi"
    GCC = "gcc"
    INTERNATIONAL = "international"
    CRYPTO = "crypto"
    ETF = "etf"
    BOND = "bond"

class MarketSector(str, Enum):
    """Market sector enumeration."""
    BANKING = "banking"
    PETROCHEMICALS = "petrochemicals"
    TELECOM = "telecom"
    CEMENT = "cement"
    RETAIL = "retail"
    INSURANCE = "insurance"
    ENERGY = "energy"
    REAL_ESTATE = "real_estate"
    TRANSPORTATION = "transportation"
    HEALTHCARE = "healthcare"
    INDUSTRIAL = "industrial"
    AGRICULTURE = "agriculture"
    OTHER = "other"

@dataclass
class SymbolRecord:
    """Symbol data record."""
    
    # Core Identification
    symbol: str
    isin: Optional[str] = None
    cusip: Optional[str] = None
    sedol: Optional[str] = None
    
    # Names
    name_ar: Optional[str] = None
    name_en: Optional[str] = None
    short_name: Optional[str] = None
    company_name: Optional[str] = None
    
    # Classification
    symbol_type: SymbolType = SymbolType.SAUDI
    market_sector: MarketSector = MarketSector.OTHER
    industry: Optional[str] = None
    sub_industry: Optional[str] = None
    gics_sector: Optional[str] = None
    gics_industry: Optional[str] = None
    
    # Market Information
    exchange: Optional[str] = None
    market: Optional[str] = None
    currency: str = "SAR"
    country: str = "SA"
    
    # Financial Metrics
    market_cap: Optional[float] = None
    shares_outstanding: Optional[int] = None
    free_float: Optional[float] = None
    dividend_yield: Optional[float] = None
    eps: Optional[float] = None
    pe_ratio: Optional[float] = None
    pb_ratio: Optional[float] = None
    
    # Status Flags
    is_active: bool = True
    is_tradable: bool = True
    is_suspended: bool = False
    include_in_ranking: bool = True
    include_in_index: bool = False
    
    # Metadata
    data_source: str = "google_sheets"
    last_updated: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    data_quality: float = 1.0
    confidence_score: float = 1.0
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for serialization."""
        data = {
            "symbol": self.symbol,
            "symbol_type": self.symbol_type.value,
            "market_sector": self.market_sector.value,
            "currency": self.currency,
            "country": self.country,
            "is_active": self.is_active,
            "is_tradable": self.is_tradable,
            "include_in_ranking": self.include_in_ranking,
            "data_source": self.data_source,
            "last_updated": self.last_updated.isoformat(),
            "data_quality": self.data_quality,
        }
        
        # Add optional fields if they exist
        optional_fields = [
            "isin", "cusip", "sedol", "name_ar", "name_en", "short_name",
            "company_name", "industry", "sub_industry", "gics_sector",
            "gics_industry", "exchange", "market", "market_cap",
            "shares_outstanding", "free_float", "dividend_yield",
            "eps", "pe_ratio", "pb_ratio", "is_suspended", "include_in_index",
            "confidence_score"
        ]
        
        for field_name in optional_fields:
            value = getattr(self, field_name)
            if value is not None:
                data[field_name] = value
        
        return data
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "SymbolRecord":
        """Create SymbolRecord from dictionary."""
        # Parse datetime
        if "last_updated" in data and isinstance(data["last_updated"], str):
            data["last_updated"] = parser.parse(data["last_updated"])
        
        # Convert string enums
        if "symbol_type" in data:
            data["symbol_type"] = SymbolType(data["symbol_type"])
        
        if "market_sector" in data:
            data["market_sector"] = MarketSector(data["market_sector"])
        
        return cls(**data)

class SymbolsResponse(BaseModel):
    """Symbols response model."""
    
    success: bool
    timestamp: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    source: str
    count: int
    symbols: List[Dict[str, Any]]
    metadata: Dict[str, Any]
    warnings: List[str] = []
    error: Optional[str] = None
    
    model_config = {
        "json_encoders": {datetime: lambda v: v.isoformat()}
    }

class SymbolsBatchRequest(BaseModel):
    """Symbols batch request model."""
    
    sheet_names: Optional[List[str]] = None
    use_cache: bool = True
    refresh_cache: bool = False
    limit: Optional[int] = Field(None, ge=1, le=1000)

class SymbolsCacheInfo(BaseModel):
    """Cache information model."""
    
    cache_enabled: bool
    cache_backend: str
    cache_size: int
    cache_hits: int
    cache_misses: int
    cache_hit_ratio: float
    cache_ttl_seconds: int
    memory_usage_mb: float

# =============================================================================
# Cache Manager
# =============================================================================

class SymbolsCacheManager:
    """Cache manager for symbols data."""
    
    def __init__(self, settings: SymbolsReaderSettings, metrics: SymbolsReaderMetrics):
        self.settings = settings
        self.metrics = metrics
        self.logger = setup_symbols_reader_logger()
        
        # Initialize caches
        self.memory_cache: Optional[TTLCache] = None
        self.redis_client: Optional[Redis] = None
        
        self._init_caches()
        
        # Statistics
        self.hits = 0
        self.misses = 0
    
    def _init_caches(self):
        """Initialize cache backends."""
        try:
            # Initialize memory cache
            if self.settings.cache_backend in ["memory", "hybrid"]:
                self.memory_cache = TTLCache(
                    maxsize=self.settings.cache_max_size,
                    ttl=self.settings.cache_ttl_seconds
                )
                self.logger.info("Memory cache initialized")
            
            # Initialize Redis cache
            if self.settings.cache_backend in ["redis", "hybrid"] and self.settings.redis_enabled:
                if self.settings.redis_url:
                    self.redis_client = Redis.from_url(
                        self.settings.redis_url,
                        decode_responses=True,
                        socket_connect_timeout=5,
                        socket_timeout=5
                    )
                    
                    # Test connection
                    self.redis_client.ping()
                    self.logger.info("Redis cache initialized")
                else:
                    self.logger.warning("Redis URL not configured")
        
        except Exception as e:
            self.logger.error("Cache initialization failed", error=str(e))
    
    def _get_cache_key(self, key: str) -> str:
        """Generate cache key with prefix."""
        return f"symbols:{key}"
    
    async def get(self, key: str) -> Optional[Dict[str, Any]]:
        """Get data from cache."""
        if not self.settings.cache_enabled:
            return None
        
        cache_key = self._get_cache_key(key)
        
        try:
            # Try memory cache first
            if self.memory_cache:
                data = self.memory_cache.get(cache_key)
                if data:
                    self.hits += 1
                    self.metrics.record_cache_hit("memory")
                    self.logger.debug("Memory cache hit", key=key)
                    return data
            
            # Try Redis cache
            if self.redis_client:
                data = self.redis_client.get(cache_key)
                if data:
                    # Deserialize JSON
                    try:
                        parsed_data = json.loads(data)
                        # Store in memory cache for faster access
                        if self.memory_cache:
                            self.memory_cache[cache_key] = parsed_data
                        
                        self.hits += 1
                        self.metrics.record_cache_hit("redis")
                        self.logger.debug("Redis cache hit", key=key)
                        return parsed_data
                    except json.JSONDecodeError:
                        pass
            
            self.misses += 1
            self.metrics.record_cache_miss("memory")
            
        except Exception as e:
            self.logger.warning("Cache get error", key=key, error=str(e))
        
        return None
    
    async def set(self, key: str, data: Dict[str, Any], ttl: Optional[int] = None):
        """Set data in cache."""
        if not self.settings.cache_enabled:
            return
        
        cache_key = self._get_cache_key(key)
        ttl = ttl or self.settings.cache_ttl_seconds
        
        try:
            # Store in memory cache
            if self.memory_cache:
                self.memory_cache[cache_key] = data
            
            # Store in Redis
            if self.redis_client:
                serialized = json.dumps(data)
                self.redis_client.setex(cache_key, ttl, serialized)
            
            self.logger.debug("Cache set", key=key)
            
        except Exception as e:
            self.logger.warning("Cache set error", key=key, error=str(e))
    
    async def delete(self, key: str):
        """Delete data from cache."""
        cache_key = self._get_cache_key(key)
        
        try:
            if self.memory_cache and cache_key in self.memory_cache:
                del self.memory_cache[cache_key]
            
            if self.redis_client:
                self.redis_client.delete(cache_key)
            
            self.logger.debug("Cache delete", key=key)
            
        except Exception as e:
            self.logger.warning("Cache delete error", key=key, error=str(e))
    
    async def clear(self):
        """Clear cache."""
        try:
            if self.memory_cache:
                self.memory_cache.clear()
            
            if self.redis_client:
                # Delete all symbols cache keys
                pattern = "symbols:*"
                keys = self.redis_client.keys(pattern)
                if keys:
                    self.redis_client.delete(*keys)
            
            self.hits = 0
            self.misses = 0
            
            self.logger.info("Cache cleared")
            
        except Exception as e:
            self.logger.error("Cache clear error", error=str(e))
    
    async def get_info(self) -> SymbolsCacheInfo:
        """Get cache information."""
        import sys
        
        # Calculate memory usage
        memory_usage = 0
        if self.memory_cache:
            for key, value in self.memory_cache.items():
                memory_usage += sys.getsizeof(key) + sys.getsizeof(value)
        
        memory_usage_mb = memory_usage / (1024 * 1024)
        
        # Calculate hit ratio
        total_requests = self.hits + self.misses
        hit_ratio = self.hits / total_requests if total_requests > 0 else 0.0
        
        return SymbolsCacheInfo(
            cache_enabled=self.settings.cache_enabled,
            cache_backend=self.settings.cache_backend,
            cache_size=len(self.memory_cache) if self.memory_cache else 0,
            cache_hits=self.hits,
            cache_misses=self.misses,
            cache_hit_ratio=round(hit_ratio, 3),
            cache_ttl_seconds=self.settings.cache_ttl_seconds,
            memory_usage_mb=round(memory_usage_mb, 2)
        )

# =============================================================================
# Google Sheets Service
# =============================================================================

class GoogleSheetsService:
    """Google Sheets service for symbols data."""
    
    def __init__(self, settings: SymbolsReaderSettings, metrics: SymbolsReaderMetrics):
        self.settings = settings
        self.metrics = metrics
        self.logger = setup_symbols_reader_logger()
        
        self.client: Optional[gspread.Client] = None
        self.spreadsheet: Optional[gspread.Spreadsheet] = None
        self._init_client()
    
    def _init_client(self):
        """Initialize Google Sheets client."""
        if not self.settings.google_sheets_credentials:
            self.logger.warning("Google Sheets credentials not configured")
            return
        
        try:
            # Parse credentials JSON
            creds_dict = json.loads(self.settings.google_sheets_credentials)
            
            # Create credentials
            scopes = [
                "https://www.googleapis.com/auth/spreadsheets.readonly",
                "https://www.googleapis.com/auth/drive.readonly"
            ]
            
            credentials = Credentials.from_service_account_info(creds_dict, scopes=scopes)
            
            # Initialize client
            self.client = gspread.authorize(credentials)
            
            self.logger.info("Google Sheets client initialized")
            
        except json.JSONDecodeError as e:
            self.logger.error("Invalid Google credentials JSON", error=str(e))
        except Exception as e:
            self.logger.error("Google Sheets client initialization failed", error=str(e))
    
    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=2, max=10),
        retry=retry_if_exception_type((gspread.exceptions.APIError, HttpError))
    )
    async def get_spreadsheet(self) -> Optional[gspread.Spreadsheet]:
        """Get spreadsheet by ID."""
        if not self.client or not self.settings.spreadsheet_id:
            return None
        
        try:
            start_time = datetime.now(timezone.utc)
            
            # Get spreadsheet
            self.spreadsheet = self.client.open_by_key(self.settings.spreadsheet_id)
            
            duration = (datetime.now(timezone.utc) - start_time).total_seconds()
            self.metrics.record_request("get_spreadsheet", "success", duration)
            
            self.logger.info("Spreadsheet loaded", title=self.spreadsheet.title)
            
            return self.spreadsheet
            
        except gspread.exceptions.SpreadsheetNotFound:
            self.metrics.record_request("get_spreadsheet", "not_found", 0)
            self.logger.error("Spreadsheet not found", id=self.settings.spreadsheet_id)
        except Exception as e:
            self.metrics.record_request("get_spreadsheet", "error", 0)
            self.metrics.record_error(type(e).__name__)
            self.logger.error("Spreadsheet load failed", error=str(e))
        
        return None
    
    @retry(
        stop=stop_after_attempt(2),
        wait=wait_fixed(2),
        retry=retry_if_exception_type((gspread.exceptions.APIError, HttpError))
    )
    async def get_worksheet_data(self, sheet_name: str) -> Optional[List[List[str]]]:
        """Get all data from a worksheet."""
        if not self.spreadsheet:
            spreadsheet = await self.get_spreadsheet()
            if not spreadsheet:
                return None
        
        try:
            start_time = datetime.now(timezone.utc)
            
            # Get worksheet
            worksheet = self.spreadsheet.worksheet(sheet_name)
            
            # Get all values
            values = worksheet.get_all_values()
            
            duration = (datetime.now(timezone.utc) - start_time).total_seconds()
            self.metrics.record_request("get_worksheet_data", "success", duration)
            
            self.logger.info(
                "Worksheet data loaded",
                sheet_name=sheet_name,
                rows=len(values),
                columns=len(values[0]) if values else 0
            )
            
            return values
            
        except gspread.exceptions.WorksheetNotFound:
            self.metrics.record_request("get_worksheet_data", "not_found", 0)
            self.logger.warning("Worksheet not found", sheet_name=sheet_name)
        except Exception as e:
            self.metrics.record_request("get_worksheet_data", "error", 0)
            self.metrics.record_error(type(e).__name__)
            self.logger.error("Worksheet data load failed", sheet_name=sheet_name, error=str(e))
        
        return None

# =============================================================================
# Data Parser
# =============================================================================

class SymbolsDataParser:
    """Parse symbols data from various formats."""
    
    def __init__(self, metrics: SymbolsReaderMetrics):
        self.metrics = metrics
        self.logger = setup_symbols_reader_logger()
        
        # Column mappings
        self.column_mappings = {
            "symbol": ["Symbol", "Ticker", "Code", "رمز", "الرمز"],
            "name_en": ["Name (EN)", "English Name", "Company Name", "اسم انجليزي", "الاسم الإنجليزي"],
            "name_ar": ["Name (AR)", "Arabic Name", "اسم عربي", "الاسم العربي"],
            "market_sector": ["Sector", "Industry", "Market Sector", "قطاع", "القطاع"],
            "market_cap": ["Market Cap", "Market Capitalization", "Capitalization", "القيمة السوقية"],
            "exchange": ["Exchange", "Market", "Exchange Code", "سوق", "السوق"],
            "currency": ["Currency", "Curr", "عملة", "العملة"],
            "is_active": ["Active", "Status", "Is Active", "نشط", "حالة"],
            "include_in_ranking": ["Include", "Rank", "Include in Ranking", "يشمل", "مشمول"],
        }
    
    def parse_google_sheets_data(self, data: List[List[str]], sheet_name: str) -> List[SymbolRecord]:
        """Parse Google Sheets data into SymbolRecords."""
        symbols = []
        
        if not data or len(data) < 2:  # Need at least header row and one data row
            return symbols
        
        # Get header row
        headers = data[0]
        
        # Map column indices
        column_indices = self._map_columns(headers)
        
        # Parse data rows
        for row_idx, row in enumerate(data[1:], start=2):  # Start from row 2 (1-based)
            try:
                # Skip empty rows
                if not any(cell.strip() for cell in row if cell):
                    continue
                
                symbol_record = self._parse_row(row, column_indices, sheet_name)
                if symbol_record:
                    symbols.append(symbol_record)
                    
            except Exception as e:
                self.logger.warning(
                    "Row parsing failed",
                    sheet_name=sheet_name,
                    row=row_idx,
                    error=str(e)
                )
        
        self.metrics.record_symbols_processed("google_sheets", len(symbols))
        
        self.logger.info(
            "Google Sheets data parsed",
            sheet_name=sheet_name,
            symbols_count=len(symbols),
            total_rows=len(data) - 1
        )
        
        return symbols
    
    def _map_columns(self, headers: List[str]) -> Dict[str, int]:
        """Map header names to column indices."""
        column_indices = {}
        
        for col_idx, header in enumerate(headers):
            header_lower = str(header).strip().lower()
            
            for field, aliases in self.column_mappings.items():
                for alias in aliases:
                    alias_lower = alias.lower()
                    
                    # Exact match
                    if header_lower == alias_lower:
                        column_indices[field] = col_idx
                        break
                    
                    # Contains match
                    if alias_lower in header_lower or header_lower in alias_lower:
                        column_indices[field] = col_idx
                        break
                
                if field in column_indices:
                    break
        
        return column_indices
    
    def _parse_row(self, row: List[str], column_indices: Dict[str, int], sheet_name: str) -> Optional[SymbolRecord]:
        """Parse a single row into SymbolRecord."""
        # Extract symbol
        symbol_idx = column_indices.get("symbol", 0)
        if symbol_idx >= len(row):
            return None
        
        symbol = str(row[symbol_idx]).strip().upper()
        if not symbol:
            return None
        
        # Skip invalid symbols
        if symbol.lower() in ["", "n/a", "null", "none", "undefined"]:
            return None
        
        # Extract other fields
        name_en = self._get_field(row, column_indices.get("name_en"))
        name_ar = self._get_field(row, column_indices.get("name_ar"))
        
        # Parse market sector
        market_sector_str = self._get_field(row, column_indices.get("market_sector"))
        market_sector = self._parse_market_sector(market_sector_str)
        
        # Parse market cap
        market_cap_str = self._get_field(row, column_indices.get("market_cap"))
        market_cap = self._parse_market_cap(market_cap_str)
        
        # Parse flags
        is_active_str = self._get_field(row, column_indices.get("is_active"))
        is_active = self._parse_boolean(is_active_str, default=True)
        
        include_in_ranking_str = self._get_field(row, column_indices.get("include_in_ranking"))
        include_in_ranking = self._parse_boolean(include_in_ranking_str, default=True)
        
        # Determine symbol type based on sheet name and symbol format
        symbol_type = self._determine_symbol_type(symbol, sheet_name)
        
        return SymbolRecord(
            symbol=symbol,
            name_en=name_en,
            name_ar=name_ar,
            company_name=name_en or name_ar,
            market_sector=market_sector,
            market_cap=market_cap,
            is_active=is_active,
            include_in_ranking=include_in_ranking,
            symbol_type=symbol_type,
            data_source=f"google_sheets:{sheet_name}",
            data_quality=self._calculate_data_quality(row, column_indices),
        )
    
    def _get_field(self, row: List[str], col_idx: Optional[int]) -> Optional[str]:
        """Safely get field from row."""
        if col_idx is None or col_idx >= len(row):
            return None
        
        value = str(row[col_idx]).strip()
        return value if value else None
    
    def _parse_market_sector(self, sector_str: Optional[str]) -> MarketSector:
        """Parse market sector string to enum."""
        if not sector_str:
            return MarketSector.OTHER
        
        sector_lower = sector_str.lower()
        
        sector_mapping = {
            "bank": MarketSector.BANKING,
            "banks": MarketSector.BANKING,
            "banking": MarketSector.BANKING,
            "petrochemical": MarketSector.PETROCHEMICALS,
            "petrochemicals": MarketSector.PETROCHEMICALS,
            "chemical": MarketSector.PETROCHEMICALS,
            "telecom": MarketSector.TELECOM,
            "telecommunication": MarketSector.TELECOM,
            "cement": MarketSector.CEMENT,
            "construction": MarketSector.CEMENT,
            "retail": MarketSector.RETAIL,
            "consumer": MarketSector.RETAIL,
            "insurance": MarketSector.INSURANCE,
            "energy": MarketSector.ENERGY,
            "oil": MarketSector.ENERGY,
            "gas": MarketSector.ENERGY,
            "real estate": MarketSector.REAL_ESTATE,
            "property": MarketSector.REAL_ESTATE,
            "transportation": MarketSector.TRANSPORTATION,
            "transport": MarketSector.TRANSPORTATION,
            "healthcare": MarketSector.HEALTHCARE,
            "medical": MarketSector.HEALTHCARE,
            "industrial": MarketSector.INDUSTRIAL,
            "manufacturing": MarketSector.INDUSTRIAL,
            "agriculture": MarketSector.AGRICULTURE,
            "farming": MarketSector.AGRICULTURE,
        }
        
        # Try exact matches first
        for key, value in sector_mapping.items():
            if key in sector_lower:
                return value
        
        # Try Arabic keywords
        arabic_sectors = {
            "بنك": MarketSector.BANKING,
            "بتروكيماويات": MarketSector.PETROCHEMICALS,
            "اتصالات": MarketSector.TELECOM,
            "اسمنت": MarketSector.CEMENT,
            "تجزئة": MarketSector.RETAIL,
            "تأمين": MarketSector.INSURANCE,
            "طاقة": MarketSector.ENERGY,
            "عقار": MarketSector.REAL_ESTATE,
            "نقل": MarketSector.TRANSPORTATION,
            "صحة": MarketSector.HEALTHCARE,
            "صناعي": MarketSector.INDUSTRIAL,
            "زراعة": MarketSector.AGRICULTURE,
        }
        
        for arabic_key, value in arabic_sectors.items():
            if arabic_key in sector_str:
                return value
        
        return MarketSector.OTHER
    
    def _parse_market_cap(self, market_cap_str: Optional[str]) -> Optional[float]:
        """Parse market cap string to float."""
        if not market_cap_str:
            return None
        
        try:
            # Remove commas and non-numeric characters
            cleaned = market_cap_str.replace(",", "").replace(" ", "")
            
            # Handle multipliers (K, M, B, T)
            multipliers = {
                "k": 1e3,
                "m": 1e6,
                "b": 1e9,
                "t": 1e12,
            }
            
            # Check for multiplier at the end
            for suffix, multiplier in multipliers.items():
                if cleaned.lower().endswith(suffix):
                    number_part = cleaned[:-1]
                    if number_part:
                        return float(number_part) * multiplier
            
            # Try parsing as plain number
            return float(cleaned)
            
        except (ValueError, TypeError):
            return None
    
    def _parse_boolean(self, value: Optional[str], default: bool) -> bool:
        """Parse boolean string."""
        if not value:
            return default
        
        value_lower = value.lower()
        
        true_values = ["true", "yes", "y", "1", "active", "enabled", "include", "نعم", "مفعل", "نشط"]
        false_values = ["false", "no", "n", "0", "inactive", "disabled", "exclude", "لا", "غير مفعل", "غير نشط"]
        
        if value_lower in true_values:
            return True
        elif value_lower in false_values:
            return False
        
        return default
    
    def _determine_symbol_type(self, symbol: str, sheet_name: str) -> SymbolType:
        """Determine symbol type based on symbol format and sheet name."""
        symbol_lower = symbol.lower()
        sheet_lower = sheet_name.lower()
        
        # Check for Saudi symbols (.SR suffix)
        if symbol.endswith(".SR") or ".SR" in symbol:
            return SymbolType.SAUDI
        
        # Check for crypto
        if any(crypto in symbol_lower for crypto in ["btc", "eth", "xrp", "ada", "doge"]):
            return SymbolType.CRYPTO
        
        # Check for ETFs
        if any(etf in sheet_lower for etf in ["etf", "exchange traded fund"]):
            return SymbolType.ETF
        
        # Check for bonds
        if any(bond in sheet_lower for bond in ["bond", "sukuk", "fixed income"]):
            return SymbolType.BOND
        
        # Check for GCC symbols
        gcc_suffixes = [".KW", ".QA", ".BH", ".AE", ".OM"]
        if any(suffix in symbol for suffix in gcc_suffixes):
            return SymbolType.GCC
        
        return SymbolType.INTERNATIONAL
    
    def _calculate_data_quality(self, row: List[str], column_indices: Dict[str, int]) -> float:
        """Calculate data quality score (0-1)."""
        required_fields = ["symbol", "name_en", "market_sector"]
        
        quality_score = 0.0
        
        for field in required_fields:
            if field in column_indices:
                col_idx = column_indices[field]
                if col_idx < len(row) and row[col_idx]:
                    quality_score += 0.2
        
        # Bonus for having market cap
        if "market_cap" in column_indices:
            col_idx = column_indices["market_cap"]
            if col_idx < len(row) and row[col_idx]:
                quality_score += 0.2
        
        return min(quality_score, 1.0)

# =============================================================================
# Fallback Service
# =============================================================================

class SymbolsFallbackService:
    """Fallback service for symbols data."""
    
    def __init__(self, settings: SymbolsReaderSettings, metrics: SymbolsReaderMetrics):
        self.settings = settings
        self.metrics = metrics
        self.logger = setup_symbols_reader_logger()
    
    async def get_fallback_symbols(self) -> List[SymbolRecord]:
        """Get fallback symbols."""
        symbols = []
        
        # Parse fallback symbols from settings
        fallback_list = self.settings.fallback_symbols.split(",")
        
        for symbol_str in fallback_list:
            symbol = symbol_str.strip()
            if symbol:
                symbols.append(self._create_fallback_symbol(symbol))
        
        self.metrics.record_symbols_processed("fallback", len(symbols))
        
        self.logger.info("Fallback symbols loaded", count=len(symbols))
        
        return symbols
    
    def _create_fallback_symbol(self, symbol: str) -> SymbolRecord:
        """Create SymbolRecord for fallback symbol."""
        # Map well-known symbols to company names
        symbol_mapping = {
            "7201.SR": ("STC", "Saudi Telecom Company", MarketSector.TELECOM),
            "1211.SR": ("Ma'aden", "Saudi Arabian Mining Company", MarketSector.PETROCHEMICALS),
            "2222.SR": ("Aramco", "Saudi Arabian Oil Company", MarketSector.ENERGY),
            "2380.SR": ("PetroRabigh", "Rabigh Refining and Petrochemical Company", MarketSector.PETROCHEMICALS),
            "4030.SR": ("Bahri", "National Shipping Company of Saudi Arabia", MarketSector.TRANSPORTATION),
            "4200.SR": ("Al-Dawaa", "Al-Dawaa Medical Services Company", MarketSector.HEALTHCARE),
            "1120.SR": ("Al Rajhi Bank", "Al Rajhi Banking and Investment Corporation", MarketSector.BANKING),
            "1150.SR": ("Alinma Bank", "Alinma Bank", MarketSector.BANKING),
            "2010.SR": ("SABIC", "Saudi Basic Industries Corporation", MarketSector.PETROCHEMICALS),
            "2020.SR": ("SABIC Agri-Nutrients", "SABIC Agri-Nutrients Company", MarketSector.PETROCHEMICALS),
        }
        
        symbol_upper = symbol.upper()
        
        if symbol_upper in symbol_mapping:
            short_name, company_name, sector = symbol_mapping[symbol_upper]
            name_en = f"{short_name} ({symbol_upper})"
        else:
            name_en = symbol_upper
            company_name = symbol_upper
            sector = MarketSector.OTHER
        
        return SymbolRecord(
            symbol=symbol_upper,
            name_en=name_en,
            company_name=company_name,
            market_sector=sector,
            symbol_type=SymbolType.SAUDI if symbol_upper.endswith(".SR") else SymbolType.INTERNATIONAL,
            currency="SAR" if symbol_upper.endswith(".SR") else "USD",
            country="SA" if symbol_upper.endswith(".SR") else "US",
            is_active=True,
            include_in_ranking=True,
            data_source="fallback",
            data_quality=0.5,
        )

# =============================================================================
# Main Service
# =============================================================================

class SymbolsReaderService:
    """Main symbols reader service."""
    
    def __init__(self, settings: SymbolsReaderSettings):
        self.settings = settings
        self.metrics = SymbolsReaderMetrics() if settings.enable_metrics else None
        self.logger = setup_symbols_reader_logger()
        
        # Initialize components
        self.cache_manager = SymbolsCacheManager(settings, self.metrics) if self.metrics else None
        self.google_sheets_service = GoogleSheetsService(settings, self.metrics) if self.metrics else None
        self.data_parser = SymbolsDataParser(self.metrics) if self.metrics else None
        self.fallback_service = SymbolsFallbackService(settings, self.metrics) if self.metrics else None
        
        # Statistics
        self.start_time = datetime.now(timezone.utc)
        self.total_requests = 0
        self.total_symbols = 0
        self.successful_fetches = 0
        self.failed_fetches = 0
    
    async def get_symbols(self, 
                         sheet_names: Optional[List[str]] = None,
                         use_cache: bool = True,
                         refresh_cache: bool = False,
                         limit: Optional[int] = None) -> SymbolsResponse:
        """Get symbols from multiple sources with fallback."""
        start_time = datetime.now(timezone.utc)
        self.total_requests += 1
        
        try:
            # Default sheet names
            if not sheet_names:
                sheet_names = [self.settings.default_tab_name]
            
            # Check cache first
            cache_key = self._generate_cache_key(sheet_names)
            
            if use_cache and not refresh_cache and self.cache_manager:
                cached_data = await self.cache_manager.get(cache_key)
                if cached_data:
                    processing_time = (datetime.now(timezone.utc) - start_time).total_seconds()
                    
                    self.logger.info(
                        "Cache hit",
                        sheet_names=sheet_names,
                        symbols_count=len(cached_data.get("symbols", []))
                    )
                    
                    return SymbolsResponse(
                        success=True,
                        source="cache",
                        count=len(cached_data.get("symbols", [])),
                        symbols=cached_data.get("symbols", []),
                        metadata={
                            "cache_hit": True,
                            "processing_time": processing_time,
                        }
                    )
            
            # Fetch from Google Sheets
            symbols = await self._fetch_from_google_sheets(sheet_names)
            
            # If Google Sheets fails and fallback is enabled, use fallback
            if not symbols and self.settings.enable_fallback:
                self.logger.warning("Google Sheets fetch failed, using fallback")
                
                symbols = await self.fallback_service.get_fallback_symbols()
                source = "fallback"
            else:
                source = "google_sheets"
            
            # Apply limit if specified
            if limit and symbols:
                symbols = symbols[:limit]
            
            # Convert to dictionaries
            symbols_dict = [symbol.to_dict() for symbol in symbols]
            
            # Update cache
            if self.cache_manager and symbols:
                cache_data = {
                    "symbols": symbols_dict,
                    "timestamp": datetime.now(timezone.utc).isoformat(),
                }
                await self.cache_manager.set(cache_key, cache_data)
            
            processing_time = (datetime.now(timezone.utc) - start_time).total_seconds()
            
            # Update statistics
            self.total_symbols += len(symbols)
            if symbols:
                self.successful_fetches += 1
            else:
                self.failed_fetches += 1
            
            self.logger.info(
                "Symbols fetched",
                source=source,
                count=len(symbols),
                processing_time=processing_time
            )
            
            return SymbolsResponse(
                success=bool(symbols),
                source=source,
                count=len(symbols),
                symbols=symbols_dict,
                metadata={
                    "cache_hit": False,
                    "processing_time": processing_time,
                    "sheet_names": sheet_names,
                }
            )
            
        except Exception as e:
            processing_time = (datetime.now(timezone.utc) - start_time).total_seconds()
            self.failed_fetches += 1
            
            self.logger.error(
                "Symbols fetch failed",
                error=str(e),
                processing_time=processing_time
            )
            
            # Use fallback in case of error
            if self.settings.enable_fallback:
                fallback_symbols = await self.fallback_service.get_fallback_symbols()
                fallback_dict = [symbol.to_dict() for symbol in fallback_symbols]
                
                return SymbolsResponse(
                    success=True,
                    source="fallback_error",
                    count=len(fallback_dict),
                    symbols=fallback_dict,
                    metadata={
                        "processing_time": processing_time,
                        "error": str(e),
                    },
                    warnings=["Using fallback symbols due to error"]
                )
            
            return SymbolsResponse(
                success=False,
                source="error",
                count=0,
                symbols=[],
                metadata={
                    "processing_time": processing_time,
                },
                error=str(e)
            )
    
    async def _fetch_from_google_sheets(self, sheet_names: List[str]) -> List[SymbolRecord]:
        """Fetch symbols from Google Sheets."""
        if not self.google_sheets_service or not self.data_parser:
            return []
        
        all_symbols = []
        
        # Fetch from each sheet
        for sheet_name in sheet_names:
            try:
                # Get worksheet data
                data = await self.google_sheets_service.get_worksheet_data(sheet_name)
                if not data:
                    continue
                
                # Parse data
                symbols = self.data_parser.parse_google_sheets_data(data, sheet_name)
                all_symbols.extend(symbols)
                
            except Exception as e:
                self.logger.error(
                    "Sheet processing failed",
                    sheet_name=sheet_name,
                    error=str(e)
                )
        
        return all_symbols
    
    def _generate_cache_key(self, sheet_names: List[str]) -> str:
        """Generate cache key from sheet names."""
        sorted_names = sorted(sheet_names)
        key_string = "_".join(sorted_names)
        key_hash = hashlib.md5(key_string.encode()).hexdigest()[:16]
        return f"symbols_{key_hash}"
    
    async def get_cache_info(self) -> SymbolsCacheInfo:
        """Get cache information."""
        if self.cache_manager:
            return await self.cache_manager.get_info()
        
        return SymbolsCacheInfo(
            cache_enabled=False,
            cache_backend="none",
            cache_size=0,
            cache_hits=0,
            cache_misses=0,
            cache_hit_ratio=0.0,
            cache_ttl_seconds=0,
            memory_usage_mb=0.0
        )
    
    async def clear_cache(self):
        """Clear cache."""
        if self.cache_manager:
            await self.cache_manager.clear()
    
    async def get_health(self) -> Dict[str, Any]:
        """Get service health information."""
        uptime = (datetime.now(timezone.utc) - self.start_time).total_seconds()
        
        health_status = {
            "status": "healthy",
            "version": "4.0.0",
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "uptime_seconds": round(uptime, 2),
            "uptime_hours": round(uptime / 3600, 2),
            "total_requests": self.total_requests,
            "total_symbols": self.total_symbols,
            "successful_fetches": self.successful_fetches,
            "failed_fetches": self.failed_fetches,
            "success_rate": round(self.successful_fetches / max(self.total_requests, 1), 3),
        }
        
        # Check Google Sheets connectivity
        if self.google_sheets_service and self.google_sheets_service.client:
            health_status["google_sheets"] = "connected"
        else:
            health_status["google_sheets"] = "disconnected"
        
        # Check cache status
        if self.cache_manager:
            cache_info = await self.get_cache_info()
            health_status["cache"] = cache_info.dict()
        
        # Determine overall status
        if self.failed_fetches > self.successful_fetches:
            health_status["status"] = "degraded"
        elif self.failed_fetches == self.total_requests:
            health_status["status"] = "unhealthy"
        
        return health_status

# =============================================================================
# Application State Management
# =============================================================================

class SymbolsReaderAppState:
    """Application state manager for symbols reader."""
    
    def __init__(self):
        self.settings = SymbolsReaderSettings()
        self.service: Optional[SymbolsReaderService] = None
        self.initialized = False
    
    async def initialize(self):
        """Initialize symbols reader application."""
        if self.initialized:
            return
        
        self.service = SymbolsReaderService(self.settings)
        self.initialized = True
        
        logger = setup_symbols_reader_logger()
        logger.info(
            "SymbolsReader initialized",
            version="4.0.0",
            spreadsheet_id=self.settings.spreadsheet_id[:8] + "..." if self.settings.spreadsheet_id else "None",
            cache_enabled=self.settings.cache_enabled
        )
    
    async def shutdown(self):
        """Shutdown symbols reader application."""
        if self.service:
            # Cleanup if needed
            pass
        
        self.initialized = False

# =============================================================================
# FastAPI Router Integration
# =============================================================================

# Note: This section would be integrated into your main FastAPI app
# For standalone usage, you can create a separate router

from fastapi import APIRouter, HTTPException, Depends, status

def get_symbols_reader_service() -> SymbolsReaderService:
    """Dependency to get symbols reader service."""
    app_state = SymbolsReaderAppState()
    return app_state.service

# Example router (to be included in main app)
"""
symbols_router = APIRouter(prefix="/api/v1/symbols", tags=["Symbols"])

@symbols_router.get("/")
async def get_symbols(
    sheet_name: Optional[str] = None,
    use_cache: bool = True,
    limit: Optional[int] = None,
    service: SymbolsReaderService = Depends(get_symbols_reader_service)
):
    \"\"\"Get symbols from Google Sheets.\"\"\"
    sheet_names = [sheet_name] if sheet_name else None
    return await service.get_symbols(sheet_names, use_cache, limit)

@symbols_router.get("/health")
async def health_check(
    service: SymbolsReaderService = Depends(get_symbols_reader_service)
):
    \"\"\"Get symbols reader health status.\"\"\"
    return await service.get_health()

@symbols_router.get("/cache/info")
async def get_cache_info(
    service: SymbolsReaderService = Depends(get_symbols_reader_service)
):
    \"\"\"Get cache information.\"\"\"
    return await service.get_cache_info()

@symbols_router.delete("/cache/clear")
async def clear_cache(
    service: SymbolsReaderService = Depends(get_symbols_reader_service)
):
    \"\"\"Clear symbols cache.\"\"\"
    await service.clear_cache()
    return {"status": "success", "message": "Cache cleared"}
"""

# =============================================================================
# Main Entry Point
# =============================================================================

if __name__ == "__main__":
    import asyncio
    
    async def main():
        """Test the symbols reader."""
        app_state = SymbolsReaderAppState()
        await app_state.initialize()
        
        if not app_state.service:
            print("❌ Failed to initialize symbols reader")
            return
        
        # Test health check
        health = await app_state.service.get_health()
        print(f"✅ Health: {health}")
        
        # Test symbols fetch
        response = await app_state.service.get_symbols(
            sheet_names=["Market_Leaders"],
            use_cache=True,
            limit=10
        )
        
        print(f"✅ Symbols fetched: {response.count} symbols")
        print(f"✅ Source: {response.source}")
        
        if response.symbols:
            print("Sample symbols:")
            for symbol in response.symbols[:3]:
                print(f"  - {symbol.get('symbol')}: {symbol.get('name_en')}")
    
    asyncio.run(main())
