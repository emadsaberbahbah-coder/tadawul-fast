# =============================================================================
# Argaam Integration Routes - Enhanced Production Version
# Version: 4.0.0
# Integration: Tadawul Stock Analysis API
# Description: Professional Arabic/English stock data scraping with caching
# =============================================================================

import asyncio
import json
import re
import time
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Literal, Set
from urllib.parse import quote, urljoin
from enum import Enum

# Core Framework
from fastapi import APIRouter, HTTPException, Request, Response, Depends, status, BackgroundTasks
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field, validator, ConfigDict
from pydantic.functional_validators import field_validator

# HTTP Clients
import httpx
from httpx import AsyncClient, Timeout, Limits, ConnectError, ReadError

# Web Scraping
from bs4 import BeautifulSoup
import lxml.html
import html5lib

# Error Handling & Resilience
from tenacity import (
    retry, stop_after_attempt, wait_exponential, wait_random,
    retry_if_exception_type, stop_after_delay, wait_fixed
)

# Monitoring & Logging
import structlog
from prometheus_client import Counter, Histogram, Gauge

# Caching
from cachetools import TTLCache, LRUCache
from cachetools.keys import hashkey

# Utilities
import orjson
from dateutil import parser

# System
import hashlib

# =============================================================================
# Configuration Models
# =============================================================================

class ArgaamEnvironment(str, Enum):
    """Argaam environment modes."""
    PRODUCTION = "production"
    DEVELOPMENT = "development"
    TESTING = "testing"

class ArgaamSettings(BaseModel):
    """Argaam-specific configuration."""
    
    # Service Configuration
    enabled: bool = Field(True, description="Enable Argaam integration")
    base_url: str = Field("https://www.argaam.com", description="Argaam base URL")
    timeout: int = Field(30, description="HTTP request timeout in seconds")
    max_retries: int = Field(3, description="Maximum retry attempts")
    rate_limit_delay: float = Field(0.15, description="Delay between requests")
    
    # Cache Configuration
    cache_enabled: bool = Field(True, description="Enable caching")
    cache_ttl: int = Field(600, description="Cache TTL in seconds")
    cache_maxsize: int = Field(1000, description="Maximum cache size")
    
    # Rate Limiting
    requests_per_minute: int = Field(60, description="Max requests per minute")
    concurrent_requests: int = Field(10, description="Max concurrent requests")
    
    # HTML Parsing
    html_parsers: List[str] = Field(
        default=["lxml", "html.parser", "html5lib"],
        description="HTML parsers in order of preference"
    )
    
    # User Agent
    user_agent: str = Field(
        default="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36",
        description="User agent for requests"
    )
    
    # Ticker Normalization
    ticker_pattern: str = Field(r"(\d{1,4})", description="Regex for ticker normalization")
    
    model_config = ConfigDict(
        env_prefix="ARGAAM_",
        case_sensitive=False,
        extra="ignore"
    )
    
    @field_validator('timeout')
    @classmethod
    def validate_timeout(cls, v):
        if v < 5:
            return 5
        if v > 120:
            return 120
        return v
    
    @field_validator('max_retries')
    @classmethod
    def validate_max_retries(cls, v):
        if v < 0:
            return 0
        if v > 5:
            return 5
        return v

# =============================================================================
# Prometheus Metrics
# =============================================================================

class ArgaamMetrics:
    """Prometheus metrics for Argaam integration."""
    
    def __init__(self):
        # Request metrics
        self.requests_total = Counter(
            'argaam_requests_total',
            'Total Argaam API requests',
            ['endpoint', 'status']
        )
        
        self.request_duration_seconds = Histogram(
            'argaam_request_duration_seconds',
            'Argaam request duration in seconds',
            ['endpoint'],
            buckets=(0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10)
        )
        
        # Cache metrics
        self.cache_hits_total = Counter(
            'argaam_cache_hits_total',
            'Total Argaam cache hits',
            ['cache_type']
        )
        
        self.cache_misses_total = Counter(
            'argaam_cache_misses_total',
            'Total Argaam cache misses',
            ['cache_type']
        )
        
        # Data quality metrics
        self.data_quality = Counter(
            'argaam_data_quality_total',
            'Data quality levels',
            ['quality']
        )
        
        # Error metrics
        self.errors_total = Counter(
            'argaam_errors_total',
            'Total Argaam errors',
            ['error_type']
        )
    
    def record_request(self, endpoint: str, status: str, duration: float):
        """Record request metrics."""
        self.requests_total.labels(endpoint=endpoint, status=status).inc()
        self.request_duration_seconds.labels(endpoint=endpoint).observe(duration)
    
    def record_cache_hit(self, cache_type: str = "memory"):
        """Record cache hit."""
        self.cache_hits_total.labels(cache_type=cache_type).inc()
    
    def record_cache_miss(self, cache_type: str = "memory"):
        """Record cache miss."""
        self.cache_misses_total.labels(cache_type=cache_type).inc()
    
    def record_data_quality(self, quality: str):
        """Record data quality."""
        self.data_quality.labels(quality=quality).inc()
    
    def record_error(self, error_type: str):
        """Record error."""
        self.errors_total.labels(error_type=error_type).inc()

# =============================================================================
# Logging Configuration
# =============================================================================

def setup_argaam_logger():
    """Configure structured logging for Argaam routes."""
    
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
    
    return structlog.get_logger("argaam")

# =============================================================================
# Data Models
# =============================================================================

class TickerNormalization(BaseModel):
    """Ticker normalization model."""
    original: str
    normalized: str
    confidence: float = Field(ge=0.0, le=1.0)

class ArgaamCompanySnapshot(BaseModel):
    """Company snapshot from Argaam."""
    symbol: str
    name_ar: Optional[str] = None
    name_en: Optional[str] = None
    sector_ar: Optional[str] = None
    sector_en: Optional[str] = None
    industry_ar: Optional[str] = None
    industry_en: Optional[str] = None
    
    # Market Data
    price: Optional[float] = None
    change: Optional[float] = None
    change_percent: Optional[float] = None
    volume: Optional[int] = None
    market_cap: Optional[float] = None
    shares_outstanding: Optional[int] = None
    
    # Fundamentals
    eps: Optional[float] = None
    pe_ratio: Optional[float] = None
    dividend_yield: Optional[float] = None
    beta: Optional[float] = None
    high_52_week: Optional[float] = None
    low_52_week: Optional[float] = None
    
    # Technical
    day_high: Optional[float] = None
    day_low: Optional[float] = None
    open_price: Optional[float] = None
    previous_close: Optional[float] = None
    
    # Metadata
    timestamp: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    data_quality: str = Field(default="unknown", pattern="^(unknown|low|medium|high)$")
    provider: str = "argaam"
    url: Optional[str] = None
    
    model_config = ConfigDict(
        json_encoders={datetime: lambda v: v.isoformat()}
    )

class ArgaamQuoteResponse(BaseModel):
    """Argaam quote response."""
    success: bool
    symbol: str
    data: Optional[ArgaamCompanySnapshot] = None
    error: Optional[str] = None
    warnings: List[str] = []
    processing_time: float
    cache_hit: bool = False
    retry_count: int = 0

class ArgaamBatchRequest(BaseModel):
    """Batch request for multiple tickers."""
    tickers: List[str] = Field(..., min_items=1, max_items=50)
    cache_ttl: Optional[int] = None
    priority: Literal["low", "normal", "high"] = "normal"
    fallback_enabled: bool = True
    html_parsing: bool = True
    
    @field_validator('tickers')
    @classmethod
    def validate_tickers(cls, v: List[str]) -> List[str]:
        """Validate ticker list."""
        if len(v) > 50:
            raise ValueError("Maximum 50 tickers per request")
        return [t.strip() for t in v if t.strip()]

class ArgaamBatchResponse(BaseModel):
    """Batch response for multiple tickers."""
    request_id: str
    timestamp: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    processed: int
    successful: int
    failed: int
    results: Dict[str, ArgaamQuoteResponse]
    metadata: Dict[str, Any]
    
    model_config = ConfigDict(
        json_encoders={datetime: lambda v: v.isoformat()}
    )

class ArgaamHealthResponse(BaseModel):
    """Health check response."""
    status: str
    version: str
    timestamp: datetime
    uptime: float
    cache: Dict[str, Any]
    metrics: Dict[str, Any]
    configuration: Dict[str, Any]

class ArgaamCacheInfo(BaseModel):
    """Cache information."""
    size: int
    max_size: int
    hits: int
    misses: int
    hit_ratio: float
    evictions: int
    ttl: int
    memory_usage_mb: float

# =============================================================================
# Router Definition
# =============================================================================

router = APIRouter(
    prefix="/api/v1/argaam",
    tags=["Argaam Integration"],
    responses={
        401: {"description": "Unauthorized"},
        403: {"description": "Forbidden"},
        429: {"description": "Too Many Requests"},
        500: {"description": "Internal Server Error"}
    }
)

# =============================================================================
# Core Services
# =============================================================================

class TickerNormalizer:
    """Normalize ticker symbols for Argaam."""
    
    def __init__(self, settings: ArgaamSettings):
        self.settings = settings
        self.pattern = re.compile(settings.ticker_pattern)
        
        # Common aliases mapping
        self.aliases: Dict[str, str] = {
            # English aliases
            "aramco": "2222",
            "stc": "7010",
            "alrajhi": "1120",
            "sabic": "2010",
            "riyadbank": "1010",
            "alinma": "1150",
            "alahli": "1180",
            "samba": "1090",
            "anb": "1060",
            "mobily": "7020",
            "petrochem": "2001",
            "maaden": "1211",
            "yanbu": "2290",
            "dallah": "4300",
            
            # Arabic aliases
            "أرامكو": "2222",
            "الاتصالات": "7010",
            "الراجحي": "1120",
            "سابك": "2010",
            "البنك الأهلي": "1180",
            "سامبا": "1090",
            "مبائع": "7020",
            "بترورابغ": "2001",
            "معادن": "1211",
            "ينبع": "2290",
            "دلة": "4300",
        }
        
        # Sector mapping
        self.sector_symbols: Dict[str, List[str]] = {
            "banking": ["1010", "1020", "1030", "1040", "1050", "1060", "1070", "1080", "1090", "1110", "1120", "1130", "1140", "1150", "1160", "1180", "1190"],
            "petrochemicals": ["2001", "2002", "2010", "2020", "2030", "2050", "2060", "2080", "2090", "2100", "2110", "2120", "2140", "2150", "2160", "2170", "2180"],
            "telecom": ["7010", "7020", "7030"],
            "cement": ["3001", "3002", "3003", "3004", "3005", "3007", "3008", "3010", "3020", "3030", "3040", "3050", "3060", "3080", "3090"],
            "retail": ["4001", "4002", "4003", "4004", "4005", "4006", "4007", "4008", "4010", "4012", "4013", "4014"],
            "insurance": ["8010", "8012", "8020", "8030", "8040", "8050", "8070", "8080", "8090", "8100", "8110", "8120", "8130", "8140", "8150", "8160", "8180", "8190"],
            "energy": ["2222", "2380", "2381", "4003"],
            "real_estate": ["4030", "4031", "4040", "4050", "4070", "4080", "4090", "4100", "4110", "4130", "4140", "4150", "4160", "4170", "4180", "4190"],
        }
    
    def normalize(self, ticker: str) -> TickerNormalization:
        """Normalize ticker symbol."""
        original = ticker.strip()
        
        # Check for empty
        if not original:
            return TickerNormalization(
                original=original,
                normalized="",
                confidence=0.0
            )
        
        # Convert to uppercase and remove suffixes
        ticker_upper = original.upper()
        ticker_upper = ticker_upper.replace(".SR", "").replace(".SA", "")
        ticker_upper = ticker_upper.replace("TASI:", "").replace("SASE:", "")
        
        # Check aliases
        alias_key = ticker_upper.lower()
        if alias_key in self.aliases:
            normalized = self.aliases[alias_key]
            confidence = 0.95
        else:
            # Extract numeric component
            match = self.pattern.search(ticker_upper)
            if match:
                normalized = match.group(1).zfill(4)
                confidence = 0.9
            else:
                normalized = ticker_upper
                confidence = 0.5
        
        # Validate format (should be 4 digits)
        if re.match(r'^\d{4}$', normalized):
            confidence = min(confidence + 0.05, 1.0)
        
        return TickerNormalization(
            original=original,
            normalized=normalized,
            confidence=confidence
        )
    
    def get_sector(self, normalized_ticker: str) -> Optional[str]:
        """Get sector for normalized ticker."""
        for sector, symbols in self.sector_symbols.items():
            if normalized_ticker in symbols:
                return sector
        return None

class ArgaamHTMLParser:
    """Parse HTML content from Argaam."""
    
    def __init__(self, settings: ArgaamSettings):
        self.settings = settings
        self.logger = setup_argaam_logger()
        
        # Arabic to English digit translation
        self.arabic_to_english = str.maketrans("٠١٢٣٤٥٦٧٨٩", "0123456789")
        
        # Field patterns for data extraction
        self.field_patterns = {
            "price": [
                r"السعر\s*[:=]?\s*([\d,٠-٩\.]+)",
                r"Price\s*[:=]?\s*([\d,\.]+)",
                r"آخر سعر\s*[:=]?\s*([\d,٠-٩\.]+)",
                r"Last Price\s*[:=]?\s*([\d,\.]+)",
            ],
            "change": [
                r"التغيير\s*[:=]?\s*([\d,٠-٩\.\+\-]+)",
                r"Change\s*[:=]?\s*([\d,\.\+\-]+)",
            ],
            "change_percent": [
                r"التغيير\s*\(%\)\s*[:=]?\s*([\d,٠-٩\.\+\-]+)",
                r"Change\s*\(%\)\s*[:=]?\s*([\d,\.\+\-]+)",
                r"النسبة\s*[:=]?\s*([\d,٠-٩\.\+\-]+)%",
            ],
            "volume": [
                r"الكمية\s*[:=]?\s*([\d,٠-٩\.]+)",
                r"Volume\s*[:=]?\s*([\d,\.]+)",
                r"حجم التداول\s*[:=]?\s*([\d,٠-٩\.]+)",
            ],
            "market_cap": [
                r"القيمة السوقية\s*[:=]?\s*([\d,٠-٩\.]+)",
                r"Market Cap\s*[:=]?\s*([\d,\.]+)",
                r"رأس المال السوقي\s*[:=]?\s*([\d,٠-٩\.]+)",
            ],
            "eps": [
                r"ربحية السهم\s*[:=]?\s*([\d,٠-٩\.]+)",
                r"EPS\s*[:=]?\s*([\d,\.]+)",
                r"ربحية السهم الواحد\s*[:=]?\s*([\d,٠-٩\.]+)",
            ],
            "pe_ratio": [
                r"مكرر الربحية\s*[:=]?\s*([\d,٠-٩\.]+)",
                r"P/E\s*[:=]?\s*([\d,\.]+)",
                r"مكرر الأرباح\s*[:=]?\s*([\d,٠-٩\.]+)",
            ],
            "dividend_yield": [
                r"العائد\s*[:=]?\s*([\d,٠-٩\.]+)%",
                r"Dividend Yield\s*[:=]?\s*([\d,\.]+)%",
                r"عائد توزيعات\s*[:=]?\s*([\d,٠-٩\.]+)%",
            ],
        }
    
    def parse_number(self, text: str) -> Optional[float]:
        """Parse Arabic/English numbers with multipliers."""
        if not text:
            return None
        
        # Convert Arabic digits to English
        text = text.translate(self.arabic_to_english)
        
        # Remove commas and spaces
        text = text.replace(",", "").replace(" ", "")
        
        # Handle percentage and multipliers
        multipliers = {
            "k": 1e3,
            "m": 1e6,
            "b": 1e9,
            "t": 1e12,
            "%": 0.01,
        }
        
        # Extract number and suffix
        match = re.match(r"([\+\-]?\d+(?:\.\d+)?)([kmbt%]?)", text, re.IGNORECASE)
        if match:
            value = float(match.group(1))
            suffix = match.group(2).lower()
            
            if suffix in multipliers:
                value *= multipliers[suffix]
            
            return value
        
        return None
    
    def extract_with_patterns(self, text: str, patterns: List[str]) -> Optional[float]:
        """Extract value using multiple patterns."""
        for pattern in patterns:
            match = re.search(pattern, text, re.IGNORECASE)
            if match:
                value = self.parse_number(match.group(1))
                if value is not None:
                    return value
        return None
    
    def parse_html(self, html: str, symbol: str) -> ArgaamCompanySnapshot:
        """Parse HTML content and extract company data."""
        snapshot = ArgaamCompanySnapshot(symbol=symbol)
        
        if not html:
            snapshot.data_quality = "low"
            return snapshot
        
        # Try different parsers
        soup = None
        for parser in self.settings.html_parsers:
            try:
                soup = BeautifulSoup(html, parser)
                break
            except Exception as e:
                self.logger.warning(f"Parser {parser} failed", error=str(e))
                continue
        
        if not soup:
            snapshot.data_quality = "low"
            return snapshot
        
        # Extract all text
        all_text = soup.get_text(" ", strip=True)
        
        # Extract company name
        name_selectors = [
            "h1", ".company-name", ".stock-name", ".symbol-name",
            '[class*="company"]', '[class*="symbol"]', '[class*="stock"]',
            "title"
        ]
        
        for selector in name_selectors:
            element = soup.select_one(selector)
            if element:
                text = element.get_text(strip=True)
                if text and len(text) > 2:
                    # Try to separate Arabic and English
                    if re.search(r'[\u0600-\u06FF]', text):
                        snapshot.name_ar = text
                    else:
                        snapshot.name_en = text
                    break
        
        # Extract field values
        fields_found = 0
        
        for field, patterns in self.field_patterns.items():
            value = self.extract_with_patterns(all_text, patterns)
            if value is not None:
                setattr(snapshot, field, value)
                fields_found += 1
        
        # Try to extract from tables
        tables = soup.find_all("table")
        for table in tables:
            rows = table.find_all("tr")
            for row in rows:
                cells = row.find_all(["td", "th"])
                if len(cells) >= 2:
                    label = cells[0].get_text(strip=True)
                    value_text = cells[1].get_text(strip=True)
                    
                    # Match labels to fields
                    if any(keyword in label.lower() for keyword in ["price", "سعر"]):
                        value = self.parse_number(value_text)
                        if value and snapshot.price is None:
                            snapshot.price = value
                            fields_found += 1
                    
                    elif any(keyword in label.lower() for keyword in ["volume", "كمية", "تداول"]):
                        value = self.parse_number(value_text)
                        if value and snapshot.volume is None:
                            snapshot.volume = int(value) if value else None
                            fields_found += 1
        
        # Determine data quality
        if fields_found >= 8:
            snapshot.data_quality = "high"
        elif fields_found >= 5:
            snapshot.data_quality = "medium"
        elif fields_found >= 2:
            snapshot.data_quality = "low"
        else:
            snapshot.data_quality = "unknown"
        
        self.logger.info(
            "HTML parsed",
            symbol=symbol,
            fields_found=fields_found,
            quality=snapshot.data_quality
        )
        
        return snapshot

class ArgaamHTTPClient:
    """HTTP client for Argaam with rate limiting and retry."""
    
    def __init__(self, settings: ArgaamSettings, metrics: ArgaamMetrics):
        self.settings = settings
        self.metrics = metrics
        self.logger = setup_argaam_logger()
        
        # Rate limiting
        self.semaphore = asyncio.Semaphore(settings.concurrent_requests)
        self.last_request_time = 0
        
        # HTTP client configuration
        self.timeout = Timeout(settings.timeout)
        self.limits = Limits(
            max_connections=100,
            max_keepalive_connections=50
        )
        
        self.headers = {
            "User-Agent": settings.user_agent,
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.9,ar;q=0.8",
            "Accept-Encoding": "gzip, deflate, br",
            "Connection": "keep-alive",
            "Upgrade-Insecure-Requests": "1",
            "Sec-Fetch-Dest": "document",
            "Sec-Fetch-Mode": "navigate",
            "Sec-Fetch-Site": "none",
            "Sec-Fetch-User": "?1",
            "Cache-Control": "max-age=0",
        }
        
        self.client: Optional[AsyncClient] = None
    
    async def __aenter__(self):
        """Async context manager entry."""
        self.client = AsyncClient(
            timeout=self.timeout,
            limits=self.limits,
            headers=self.headers,
            follow_redirects=True,
            http2=True
        )
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit."""
        if self.client:
            await self.client.aclose()
    
    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=2, max=10),
        retry=retry_if_exception_type((ConnectError, ReadError, TimeoutError))
    )
    async def fetch_html(self, url: str) -> Optional[str]:
        """Fetch HTML content with rate limiting."""
        async with self.semaphore:
            # Rate limiting
            current_time = time.time()
            time_since_last = current_time - self.last_request_time
            if time_since_last < self.settings.rate_limit_delay:
                await asyncio.sleep(self.settings.rate_limit_delay - time_since_last)
            
            start_time = time.time()
            
            try:
                if not self.client:
                    self.client = AsyncClient(
                        timeout=self.timeout,
                        limits=self.limits,
                        headers=self.headers,
                        follow_redirects=True
                    )
                
                response = await self.client.get(url)
                duration = time.time() - start_time
                
                self.last_request_time = time.time()
                
                if response.status_code == 200:
                    content = response.text
                    if content and len(content) > 1000:
                        self.metrics.record_request("fetch_html", "success", duration)
                        self.logger.debug(
                            "HTML fetched",
                            url=url,
                            size=len(content),
                            duration=duration
                        )
                        return content
                    else:
                        self.logger.warning("Insufficient content", url=url, size=len(content))
                        self.metrics.record_request("fetch_html", "insufficient", duration)
                else:
                    self.logger.warning(
                        "HTTP error",
                        url=url,
                        status=response.status_code
                    )
                    self.metrics.record_request("fetch_html", f"error_{response.status_code}", duration)
                    
                    if response.status_code == 429:
                        # Rate limited - wait and retry
                        await asyncio.sleep(5)
                        raise ConnectError("Rate limited")
                
                return None
                
            except Exception as e:
                duration = time.time() - start_time
                self.logger.error("Fetch error", url=url, error=str(e))
                self.metrics.record_request("fetch_html", "exception", duration)
                self.metrics.record_error(type(e).__name__)
                raise

class ArgaamURLDiscoverer:
    """Discover Argaam URLs for symbols."""
    
    def __init__(self, settings: ArgaamSettings):
        self.settings = settings
        self.logger = setup_argaam_logger()
        
        # URL patterns
        self.patterns = [
            # Arabic patterns
            "{base}/ar/company/{symbol}",
            "{base}/ar/company/financials/{symbol}",
            "{base}/ar/tadawul/company?symbol={symbol}",
            "{base}/ar/tadawul/{symbol}",
            "{base}/ar/{symbol}",
            
            # English patterns
            "{base}/en/company/{symbol}",
            "{base}/en/company/financials/{symbol}",
            "{base}/en/tadawul/company?symbol={symbol}",
            "{base}/en/tadawul/{symbol}",
            "{base}/en/{symbol}",
            
            # Common patterns
            "{base}/company/{symbol}",
            "{base}/tadawul/{symbol}",
            "{base}/stock/{symbol}",
            "{base}/symbol/{symbol}",
        ]
        
        # Cache of successful patterns
        self.successful_patterns: Dict[str, str] = {}
    
    async def discover_url(self, http_client: ArgaamHTTPClient, symbol: str) -> Optional[str]:
        """Discover URL for symbol."""
        # Check cache first
        if symbol in self.successful_patterns:
            pattern = self.successful_patterns[symbol]
            url = pattern.format(base=self.settings.base_url, symbol=quote(symbol))
            self.logger.debug("Using cached URL pattern", symbol=symbol, pattern=pattern)
            return url
        
        # Try patterns
        for pattern in self.patterns:
            url = pattern.format(base=self.settings.base_url, symbol=quote(symbol))
            
            # Try to fetch
            try:
                content = await http_client.fetch_html(url)
                if content and len(content) > 2000:
                    # Valid content found
                    self.successful_patterns[symbol] = pattern
                    self.logger.info("URL discovered", symbol=symbol, url=url)
                    return url
            except Exception as e:
                self.logger.debug("Pattern failed", pattern=pattern, error=str(e))
                continue
        
        self.logger.warning("No URL discovered", symbol=symbol)
        return None

class ArgaamCacheManager:
    """Cache manager for Argaam data."""
    
    def __init__(self, settings: ArgaamSettings, metrics: ArgaamMetrics):
        self.settings = settings
        self.metrics = metrics
        self.logger = setup_argaam_logger()
        
        # Initialize cache
        self.cache = TTLCache(
            maxsize=settings.cache_maxsize,
            ttl=settings.cache_ttl
        )
        
        # Statistics
        self.hits = 0
        self.misses = 0
        self.evictions = 0
    
    def _get_cache_key(self, symbol: str) -> str:
        """Generate cache key."""
        return f"argaam:{symbol}"
    
    async def get(self, symbol: str) -> Optional[Dict[str, Any]]:
        """Get data from cache."""
        if not self.settings.cache_enabled:
            return None
        
        cache_key = self._get_cache_key(symbol)
        
        try:
            data = self.cache.get(cache_key)
            if data:
                self.hits += 1
                self.metrics.record_cache_hit("memory")
                self.logger.debug("Cache hit", symbol=symbol)
                return data
            else:
                self.misses += 1
                self.metrics.record_cache_miss("memory")
        except Exception as e:
            self.logger.warning("Cache get error", symbol=symbol, error=str(e))
        
        return None
    
    async def set(self, symbol: str, data: Dict[str, Any], ttl: Optional[int] = None):
        """Set data in cache."""
        if not self.settings.cache_enabled:
            return
        
        cache_key = self._get_cache_key(symbol)
        
        try:
            old_size = len(self.cache)
            old_data = cache_key in self.cache
            
            # Create cache data with timestamp
            cache_data = {
                "data": data,
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "ttl": ttl or self.settings.cache_ttl,
            }
            
            self.cache[cache_key] = cache_data
            
            # Track evictions
            if old_size >= self.cache.maxsize and not old_data:
                self.evictions += 1
            
            self.logger.debug("Cache set", symbol=symbol)
            
        except Exception as e:
            self.logger.warning("Cache set error", symbol=symbol, error=str(e))
    
    async def clear(self):
        """Clear cache."""
        self.cache.clear()
        self.hits = 0
        self.misses = 0
        self.evictions = 0
        self.logger.info("Cache cleared")
    
    async def get_info(self) -> ArgaamCacheInfo:
        """Get cache information."""
        import sys
        
        # Calculate memory usage (approximate)
        memory_usage = 0
        for key, value in self.cache.items():
            memory_usage += sys.getsizeof(key) + sys.getsizeof(value)
        
        memory_usage_mb = memory_usage / (1024 * 1024)
        
        total_requests = self.hits + self.misses
        hit_ratio = self.hits / total_requests if total_requests > 0 else 0.0
        
        return ArgaamCacheInfo(
            size=len(self.cache),
            max_size=self.cache.maxsize,
            hits=self.hits,
            misses=self.misses,
            hit_ratio=round(hit_ratio, 3),
            evictions=self.evictions,
            ttl=self.cache.ttl,
            memory_usage_mb=round(memory_usage_mb, 2)
        )

# =============================================================================
# Main Service
# =============================================================================

class ArgaamService:
    """Main Argaam service."""
    
    def __init__(self, settings: ArgaamSettings):
        self.settings = settings
        self.metrics = ArgaamMetrics()
        self.logger = setup_argaam_logger()
        
        # Initialize components
        self.ticker_normalizer = TickerNormalizer(settings)
        self.html_parser = ArgaamHTMLParser(settings)
        self.cache_manager = ArgaamCacheManager(settings, self.metrics)
        self.url_discoverer = ArgaamURLDiscoverer(settings)
        
        # HTTP client (will be initialized per request)
        self.http_client: Optional[ArgaamHTTPClient] = None
        
        # Statistics
        self.start_time = time.time()
        self.total_requests = 0
        self.total_symbols = 0
        self.successful_fetches = 0
        self.failed_fetches = 0
    
    async def initialize(self):
        """Initialize service."""
        self.logger.info(
            "ArgaamService initialized",
            base_url=self.settings.base_url,
            cache_enabled=self.settings.cache_enabled,
            cache_ttl=self.settings.cache_ttl
        )
    
    async def get_company_data(self, symbol: str, use_cache: bool = True) -> ArgaamQuoteResponse:
        """Get company data from Argaam."""
        start_time = time.time()
        retry_count = 0
        
        # Normalize ticker
        normalization = self.ticker_normalizer.normalize(symbol)
        normalized_symbol = normalization.normalized
        
        if not normalized_symbol:
            processing_time = time.time() - start_time
            return ArgaamQuoteResponse(
                success=False,
                symbol=symbol,
                error=f"Invalid ticker symbol: {symbol}",
                processing_time=processing_time,
                retry_count=retry_count
            )
        
        # Check cache
        cache_hit = False
        if use_cache and self.settings.cache_enabled:
            cached_data = await self.cache_manager.get(normalized_symbol)
            if cached_data:
                cache_hit = True
                processing_time = time.time() - start_time
                
                self.logger.info(
                    "Cache hit",
                    symbol=symbol,
                    normalized=normalized_symbol,
                    processing_time=processing_time
                )
                
                return ArgaamQuoteResponse(
                    success=True,
                    symbol=normalized_symbol,
                    data=cached_data["data"],
                    processing_time=processing_time,
                    cache_hit=True,
                    retry_count=retry_count
                )
        
        # Fetch from Argaam
        try:
            async with ArgaamHTTPClient(self.settings, self.metrics) as http_client:
                # Discover URL
                url = await self.url_discoverer.discover_url(http_client, normalized_symbol)
                
                if not url:
                    processing_time = time.time() - start_time
                    
                    self.logger.warning(
                        "URL not found",
                        symbol=symbol,
                        normalized=normalized_symbol
                    )
                    
                    return ArgaamQuoteResponse(
                        success=False,
                        symbol=normalized_symbol,
                        error="Company URL not found on Argaam",
                        processing_time=processing_time,
                        retry_count=retry_count
                    )
                
                # Fetch HTML
                html = await http_client.fetch_html(url)
                
                if not html:
                    processing_time = time.time() - start_time
                    
                    self.logger.warning(
                        "HTML fetch failed",
                        symbol=symbol,
                        normalized=normalized_symbol,
                        url=url
                    )
                    
                    return ArgaamQuoteResponse(
                        success=False,
                        symbol=normalized_symbol,
                        error="Failed to fetch data from Argaam",
                        processing_time=processing_time,
                        retry_count=retry_count
                    )
                
                # Parse HTML
                snapshot = self.html_parser.parse_html(html, normalized_symbol)
                snapshot.url = url
                
                # Update statistics
                self.total_requests += 1
                self.total_symbols += 1
                self.successful_fetches += 1
                
                # Cache the result
                if self.settings.cache_enabled:
                    await self.cache_manager.set(
                        normalized_symbol,
                        snapshot.dict(),
                        ttl=self.settings.cache_ttl
                    )
                
                processing_time = time.time() - start_time
                
                # Record data quality
                self.metrics.record_data_quality(snapshot.data_quality)
                
                self.logger.info(
                    "Data fetched successfully",
                    symbol=symbol,
                    normalized=normalized_symbol,
                    quality=snapshot.data_quality,
                    processing_time=processing_time,
                    cache_hit=False
                )
                
                return ArgaamQuoteResponse(
                    success=True,
                    symbol=normalized_symbol,
                    data=snapshot,
                    processing_time=processing_time,
                    cache_hit=False,
                    retry_count=retry_count
                )
                
        except Exception as e:
            processing_time = time.time() - start_time
            self.failed_fetches += 1
            
            self.logger.error(
                "Service error",
                symbol=symbol,
                normalized=normalized_symbol,
                error=str(e),
                processing_time=processing_time
            )
            
            self.metrics.record_error("service_exception")
            
            return ArgaamQuoteResponse(
                success=False,
                symbol=normalized_symbol,
                error=f"Service error: {str(e)}",
                processing_time=processing_time,
                retry_count=retry_count
            )
    
    async def get_batch_data(self, tickers: List[str], cache_ttl: Optional[int] = None) -> ArgaamBatchResponse:
        """Get batch data for multiple tickers."""
        start_time = time.time()
        request_id = f"argaam_batch_{int(time.time())}_{hash(str(tickers))}"
        
        # Update cache TTL if specified
        original_ttl = self.settings.cache_ttl
        if cache_ttl:
            self.settings.cache_ttl = cache_ttl
        
        try:
            # Process tickers in parallel with limited concurrency
            semaphore = asyncio.Semaphore(5)  # Limit concurrent requests
            
            async def process_ticker(ticker: str) -> tuple[str, ArgaamQuoteResponse]:
                async with semaphore:
                    result = await self.get_company_data(ticker)
                    return ticker, result
            
            # Create tasks
            tasks = [process_ticker(ticker) for ticker in tickers]
            results = await asyncio.gather(*tasks, return_exceptions=True)
            
            # Process results
            processed_results: Dict[str, ArgaamQuoteResponse] = {}
            successful = 0
            failed = 0
            
            for i, result in enumerate(results):
                ticker = tickers[i]
                
                if isinstance(result, Exception):
                    processed_results[ticker] = ArgaamQuoteResponse(
                        success=False,
                        symbol=ticker,
                        error=f"Processing error: {str(result)}",
                        processing_time=0.0,
                        retry_count=0
                    )
                    failed += 1
                else:
                    ticker_key, quote_response = result
                    processed_results[ticker_key] = quote_response
                    
                    if quote_response.success:
                        successful += 1
                    else:
                        failed += 1
            
            processing_time = time.time() - start_time
            
            # Get cache info
            cache_info = await self.cache_manager.get_info()
            
            # Restore original TTL
            if cache_ttl:
                self.settings.cache_ttl = original_ttl
            
            self.logger.info(
                "Batch processing completed",
                request_id=request_id,
                total=len(tickers),
                successful=successful,
                failed=failed,
                processing_time=processing_time
            )
            
            return ArgaamBatchResponse(
                request_id=request_id,
                processed=len(tickers),
                successful=successful,
                failed=failed,
                results=processed_results,
                metadata={
                    "cache": cache_info.dict(),
                    "processing_time": processing_time,
                    "timestamp": datetime.now(timezone.utc).isoformat(),
                }
            )
            
        except Exception as e:
            processing_time = time.time() - start_time
            
            self.logger.error(
                "Batch processing failed",
                request_id=request_id,
                error=str(e),
                processing_time=processing_time
            )
            
            # Restore original TTL
            if cache_ttl:
                self.settings.cache_ttl = original_ttl
            
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f"Batch processing failed: {str(e)}"
            )
    
    async def get_health(self) -> ArgaamHealthResponse:
        """Get health status."""
        uptime = time.time() - self.start_time
        
        # Get cache info
        cache_info = await self.cache_manager.get_info()
        
        # Get metrics
        metrics_data = {
            "total_requests": self.total_requests,
            "total_symbols": self.total_symbols,
            "successful_fetches": self.successful_fetches,
            "failed_fetches": self.failed_fetches,
            "uptime_hours": round(uptime / 3600, 2),
        }
        
        # Get configuration
        config_data = self.settings.dict()
        
        # Determine status
        status = "healthy"
        if self.failed_fetches > 0 and self.successful_fetches == 0:
            status = "unhealthy"
        elif self.failed_fetches > self.successful_fetches:
            status = "degraded"
        
        return ArgaamHealthResponse(
            status=status,
            version="4.0.0",
            timestamp=datetime.now(timezone.utc),
            uptime=uptime,
            cache=cache_info.dict(),
            metrics=metrics_data,
            configuration=config_data
        )

# =============================================================================
# Application State Management
# =============================================================================

class ArgaamAppState:
    """Application state manager for Argaam."""
    
    def __init__(self):
        self.settings = ArgaamSettings()
        self.service: Optional[ArgaamService] = None
        self.initialized = False
    
    async def initialize(self):
        """Initialize Argaam application state."""
        if self.initialized:
            return
        
        self.service = ArgaamService(self.settings)
        await self.service.initialize()
        self.initialized = True
    
    async def shutdown(self):
        """Shutdown Argaam application."""
        if self.service:
            # Cleanup if needed
            pass
        
        self.initialized = False

# =============================================================================
# FastAPI Router with Dependencies
# =============================================================================

# Application state
argaam_app_state = ArgaamAppState()

# Dependency for Argaam service
async def get_argaam_service() -> ArgaamService:
    """Dependency to get Argaam service."""
    if not argaam_app_state.initialized:
        await argaam_app_state.initialize()
    
    if not argaam_app_state.service:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Argaam service not initialized"
        )
    
    return argaam_app_state.service

# =============================================================================
# Route Handlers
# =============================================================================

@router.on_event("startup")
async def startup_event():
    """Startup event handler."""
    await argaam_app_state.initialize()
    
    logger = setup_argaam_logger()
    logger.info(
        "Argaam routes initialized",
        version="4.0.0",
        base_url=argaam_app_state.settings.base_url,
        cache_enabled=argaam_app_state.settings.cache_enabled
    )

@router.on_event("shutdown")
async def shutdown_event():
    """Shutdown event handler."""
    await argaam_app_state.shutdown()
    
    logger = setup_argaam_logger()
    logger.info("Argaam routes shutdown complete")

@router.get("/health", response_model=ArgaamHealthResponse, summary="Health Check")
async def health_check(
    service: ArgaamService = Depends(get_argaam_service)
):
    """
    Get Argaam service health status.
    
    Returns detailed health information including cache statistics,
    metrics, and configuration.
    """
    return await service.get_health()

@router.post("/quotes", response_model=ArgaamBatchResponse, summary="Batch Quotes")
async def get_quotes(
    request: ArgaamBatchRequest,
    service: ArgaamService = Depends(get_argaam_service)
):
    """
    Get batch quotes for multiple tickers.
    
    Supports up to 50 tickers per request with configurable
    cache TTL and priority levels.
    
    - **tickers**: List of ticker symbols (max 50)
    - **cache_ttl**: Override default cache TTL in seconds
    - **priority**: Processing priority (low/normal/high)
    - **fallback_enabled**: Enable fallback mechanisms
    - **html_parsing**: Enable HTML parsing (disable for faster responses)
    """
    return await service.get_batch_data(
        tickers=request.tickers,
        cache_ttl=request.cache_ttl
    )

@router.get("/quotes/{symbol}", response_model=ArgaamQuoteResponse, summary="Single Quote")
async def get_quote(
    symbol: str,
    use_cache: bool = True,
    service: ArgaamService = Depends(get_argaam_service)
):
    """
    Get quote for a single ticker symbol.
    
    Supports ticker normalization (e.g., "2222", "2222.SR", "Aramco").
    
    - **symbol**: Ticker symbol
    - **use_cache**: Use cached data if available
    """
    return await service.get_company_data(symbol, use_cache)

@router.get("/cache/info", response_model=ArgaamCacheInfo, summary="Cache Information")
async def get_cache_info(
    service: ArgaamService = Depends(get_argaam_service)
):
    """
    Get detailed cache information and statistics.
    
    Includes hit/miss ratios, memory usage, and eviction counts.
    """
    return await service.cache_manager.get_info()

@router.delete("/cache/clear", summary="Clear Cache")
async def clear_cache(
    service: ArgaamService = Depends(get_argaam_service)
):
    """
    Clear the Argaam cache.
    
    Removes all cached data and resets statistics.
    """
    await service.cache_manager.clear()
    
    logger = setup_argaam_logger()
    logger.info("Cache cleared via API")
    
    return {
        "status": "success",
        "message": "Cache cleared successfully",
        "timestamp": datetime.now(timezone.utc).isoformat()
    }

@router.get("/normalize/{ticker}", summary="Normalize Ticker")
async def normalize_ticker(
    ticker: str,
    service: ArgaamService = Depends(get_argaam_service)
):
    """
    Normalize a ticker symbol.
    
    Converts various ticker formats to standardized 4-digit format.
    Supports aliases (e.g., "Aramco" -> "2222").
    """
    normalization = service.ticker_normalizer.normalize(ticker)
    
    # Get sector if available
    sector = service.ticker_normalizer.get_sector(normalization.normalized)
    
    return {
        "original": normalization.original,
        "normalized": normalization.normalized,
        "confidence": normalization.confidence,
        "sector": sector,
        "timestamp": datetime.now(timezone.utc).isoformat()
    }

@router.get("/test/{ticker}", summary="Test Endpoint")
async def test_ticker(
    ticker: str,
    service: ArgaamService = Depends(get_argaam_service)
):
    """
    Test endpoint for debugging and development.
    
    Returns raw parsed data and processing metadata.
    """
    result = await service.get_company_data(ticker, use_cache=False)
    
    return {
        "test": True,
        "ticker": ticker,
        "result": result.dict(),
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "configuration": {
            "base_url": service.settings.base_url,
            "cache_enabled": service.settings.cache_enabled,
            "cache_ttl": service.settings.cache_ttl,
            "user_agent": service.settings.user_agent[:50] + "...",
        }
    }

@router.get("/stats", summary="Statistics")
async def get_stats(
    service: ArgaamService = Depends(get_argaam_service)
):
    """
    Get service statistics and metrics.
    
    Includes request counts, success rates, and performance metrics.
    """
    uptime = time.time() - service.start_time
    
    return {
        "uptime_seconds": round(uptime, 2),
        "uptime_hours": round(uptime / 3600, 2),
        "total_requests": service.total_requests,
        "total_symbols": service.total_symbols,
        "successful_fetches": service.successful_fetches,
        "failed_fetches": service.failed_fetches,
        "success_rate": round(service.successful_fetches / max(service.total_symbols, 1), 3),
        "cache_info": (await service.cache_manager.get_info()).dict(),
        "timestamp": datetime.now(timezone.utc).isoformat()
    }

@router.get("/", summary="Root Endpoint")
async def root():
    """
    Argaam Integration API root endpoint.
    
    Returns API information and available endpoints.
    """
    return {
        "name": "Argaam Integration API",
        "version": "4.0.0",
        "description": "Professional Arabic/English stock data scraping from Argaam",
        "status": "operational",
        "endpoints": {
            "health": "/api/v1/argaam/health",
            "quotes": "/api/v1/argaam/quotes",
            "single_quote": "/api/v1/argaam/quotes/{symbol}",
            "cache_info": "/api/v1/argaam/cache/info",
            "normalize": "/api/v1/argaam/normalize/{ticker}",
            "stats": "/api/v1/argaam/stats",
            "test": "/api/v1/argaam/test/{ticker}",
        },
        "features": {
            "ticker_normalization": True,
            "arabic_english_support": True,
            "html_parsing": True,
            "caching": True,
            "rate_limiting": True,
            "batch_processing": True,
        },
        "documentation": {
            "swagger": "/docs",
            "redoc": "/redoc",
        }
    }

# =============================================================================
# Error Handlers
# =============================================================================

@router.exception_handler(HTTPException)
async def http_exception_handler(request, exc):
    """Handle HTTP exceptions."""
    logger = setup_argaam_logger()
    
    logger.warning(
        "HTTP exception",
        status_code=exc.status_code,
        detail=exc.detail,
        path=request.url.path,
        client_ip=request.client.host if request.client else None
    )
    
    return JSONResponse(
        status_code=exc.status_code,
        content={
            "error": True,
            "code": exc.status_code,
            "message": exc.detail,
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "path": request.url.path
        }
    )

@router.exception_handler(Exception)
async def general_exception_handler(request, exc):
    """Handle general exceptions."""
    logger = setup_argaam_logger()
    
    logger.error(
        "Unhandled exception",
        error_type=type(exc).__name__,
        error_message=str(exc),
        path=request.url.path,
        client_ip=request.client.host if request.client else None,
        traceback=True
    )
    
    # Record error in metrics
    if argaam_app_state.service:
        argaam_app_state.service.metrics.record_error("unhandled_exception")
    
    return JSONResponse(
        status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
        content={
            "error": True,
            "code": 500,
            "message": "Internal server error",
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "request_id": request.headers.get("X-Request-ID", "unknown")
        }
    )

# =============================================================================
# Export Router
# =============================================================================

def get_argaam_router() -> APIRouter:
    """Get the Argaam router with initialized state."""
    return router

# =============================================================================
# Direct Execution (for testing)
# =============================================================================

if __name__ == "__main__":
    import uvicorn
    
    # Create a simple test app
    from fastapi import FastAPI
    
    app = FastAPI(title="Argaam Test API", version="4.0.0")
    app.include_router(router)
    
    uvicorn.run(app, host="127.0.0.1", port=8001)
