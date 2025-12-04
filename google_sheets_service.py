"""
google_sheets_service.py
Enhanced Async Google Sheets Service for Tadawul Fast Bridge
Version: 4.0.0 - Full async with structured configuration and monitoring
Aligned with google_apps_script_client.py patterns
"""

import os
import json
import time
import asyncio
import logging
import hashlib
from typing import List, Dict, Any, Optional, Union, Tuple
from datetime import datetime, timedelta, timezone
from enum import Enum
from dataclasses import dataclass, asdict
from pathlib import Path
from contextlib import asynccontextmanager

import aiofiles
import pandas as pd
from pydantic import BaseModel, Field, validator
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from tenacity import (
    retry,
    stop_after_attempt,
    wait_exponential,
    retry_if_exception_type,
    before_sleep_log
)

# Configure logger
logger = logging.getLogger(__name__)

# ======================================================================
# Configuration Models (Aligned with previous patterns)
# ======================================================================

class SheetsConfig(BaseModel):
    """Google Sheets configuration aligned with main application settings."""
    
    # Authentication
    credentials_json: Optional[str] = Field(None, description="Service account JSON credentials")
    credentials_path: Optional[str] = Field(None, description="Path to credentials file")
    
    # Sheet IDs from environment
    ksa_tadawul_sheet_id: str = Field(..., description="KSA Tadawul market data sheet")
    tadawul_all_sheet_id: Optional[str] = Field(None, description="Comprehensive Tadawul data sheet")
    market_data_sheet_id: Optional[str] = Field(None, description="Market data sheet")
    portfolio_sheet_id: Optional[str] = Field(None, description="Portfolio tracking sheet")
    
    # Performance settings
    cache_ttl: int = Field(300, ge=60, le=3600, description="Cache TTL in seconds")
    request_timeout: int = Field(30, ge=5, le=120, description="Request timeout in seconds")
    max_retries: int = Field(3, ge=0, le=10, description="Maximum retry attempts")
    rate_limit_rpm: int = Field(60, ge=10, le=300, description="Requests per minute limit")
    batch_size: int = Field(100, ge=10, le=1000, description="Batch processing size")
    
    # Cache settings
    cache_enabled: bool = Field(True, description="Enable response caching")
    cache_dir: str = Field(".cache/sheets", description="Cache directory path")
    memory_cache_size: int = Field(1000, ge=100, le=10000, description="In-memory cache size")
    
    # Data processing
    validate_data: bool = Field(True, description="Validate data quality")
    parse_numeric: bool = Field(True, description="Auto-parse numeric values")
    default_currency: str = Field("SAR", description="Default currency")
    
    @validator('credentials_json', pre=True, always=True)
    def validate_credentials(cls, v, values):
        """Validate or load credentials from environment."""
        if not v:
            # Try to load from environment variable
            env_creds = os.getenv("GOOGLE_SHEETS_CREDENTIALS_JSON")
            if env_creds:
                try:
                    # Check if it's a JSON string
                    json.loads(env_creds)
                    return env_creds
                except json.JSONDecodeError:
                    # Might be a path
                    if os.path.exists(env_creds):
                        values['credentials_path'] = env_creds
                        return None
            # Check for path in environment
            creds_path = os.getenv("GOOGLE_CREDENTIALS_PATH")
            if creds_path and os.path.exists(creds_path):
                values['credentials_path'] = creds_path
        return v
    
    @classmethod
    def from_env(cls) -> 'SheetsConfig':
        """Create configuration from environment variables."""
        return cls(
            credentials_json=os.getenv("GOOGLE_SHEETS_CREDENTIALS_JSON"),
            credentials_path=os.getenv("GOOGLE_CREDENTIALS_PATH"),
            ksa_tadawul_sheet_id=os.getenv("KSA_TADAWUL_SHEET_ID", ""),
            tadawul_all_sheet_id=os.getenv("TADAWUL_ALL_SHEET_ID"),
            market_data_sheet_id=os.getenv("MARKET_DATA_SHEET_ID"),
            portfolio_sheet_id=os.getenv("PORTFOLIO_SHEET_ID"),
            cache_ttl=int(os.getenv("SHEETS_CACHE_TTL", "300")),
            request_timeout=int(os.getenv("HTTP_TIMEOUT", "30")),
            max_retries=int(os.getenv("MAX_RETRIES", "3")),
            rate_limit_rpm=int(os.getenv("RATE_LIMIT_RPM", "60")),
            batch_size=int(os.getenv("BATCH_SIZE", "100")),
            cache_enabled=os.getenv("CACHE_ENABLED", "true").lower() == "true",
            cache_dir=os.getenv("CACHE_DIR", ".cache/sheets"),
            memory_cache_size=int(os.getenv("MEMORY_CACHE_SIZE", "1000")),
            validate_data=os.getenv("VALIDATE_DATA", "true").lower() == "true",
            parse_numeric=os.getenv("PARSE_NUMERIC", "true").lower() == "true",
            default_currency=os.getenv("DEFAULT_CURRENCY", "SAR"),
        )


# ======================================================================
# Data Models (Enhanced with Pydantic)
# ======================================================================

class DataQuality(str, Enum):
    """Data quality classification."""
    EXCELLENT = "excellent"      # Complete, validated, recent data
    HIGH = "high"               # Mostly complete, recent data
    MEDIUM = "medium"           # Basic data present
    LOW = "low"                 # Incomplete or stale data
    UNKNOWN = "unknown"         # Quality cannot be determined


class DataSource(str, Enum):
    """Data source classification."""
    GOOGLE_SHEETS = "google_sheets"
    TADAWUL_API = "tadawul_api"
    MANUAL_ENTRY = "manual_entry"
    SCRAPER = "scraper"
    UNKNOWN = "unknown"


class FinancialData(BaseModel):
    """Structured financial data with validation."""
    
    # Identification
    ticker: str = Field(..., description="Stock ticker/symbol")
    custom_tag: str = Field("", description="Custom classification tag")
    company_name: str = Field("", description="Company full name")
    sector: str = Field("", description="Industry sector")
    sub_sector: str = Field("", description="Sub-sector classification")
    trading_market: str = Field("", description="Trading market/exchange")
    currency: str = Field("SAR", description="Currency code")
    
    # Market Data
    last_price: float = Field(0.0, ge=0, description="Last traded price")
    day_high: float = Field(0.0, ge=0, description="Daily high price")
    day_low: float = Field(0.0, ge=0, description="Daily low price")
    previous_close: float = Field(0.0, ge=0, description="Previous close price")
    open_price: float = Field(0.0, ge=0, description="Opening price")
    change_value: float = Field(0.0, description="Price change value")
    change_pct: float = Field(0.0, description="Price change percentage")
    high_52w: float = Field(0.0, ge=0, description="52-week high")
    low_52w: float = Field(0.0, ge=0, description="52-week low")
    avg_price_50d: float = Field(0.0, ge=0, description="50-day average price")
    
    # Volume & Liquidity
    volume: int = Field(0, ge=0, description="Trading volume")
    avg_volume_30d: int = Field(0, ge=0, description="30-day average volume")
    value_traded: float = Field(0.0, ge=0, description="Traded value")
    turnover_rate: float = Field(0.0, ge=0, description="Turnover rate")
    bid_price: float = Field(0.0, ge=0, description="Current bid price")
    ask_price: float = Field(0.0, ge=0, description="Current ask price")
    bid_size: int = Field(0, ge=0, description="Bid size")
    ask_size: int = Field(0, ge=0, description="Ask size")
    spread_pct: float = Field(0.0, ge=0, description="Bid-ask spread %")
    liquidity_score: float = Field(0.0, ge=0, le=100, description="Liquidity score (0-100)")
    
    # Company Metrics
    shares_outstanding: float = Field(0.0, ge=0, description="Total shares outstanding")
    free_float: float = Field(0.0, ge=0, description="Free float percentage")
    market_cap: float = Field(0.0, ge=0, description="Market capitalization")
    listing_date: str = Field("", description="Stock listing date")
    
    # Valuation Ratios
    eps: float = Field(0.0, description="Earnings per share")
    pe: float = Field(0.0, ge=0, description="Price-to-earnings ratio")
    pb: float = Field(0.0, ge=0, description="Price-to-book ratio")
    dividend_yield: float = Field(0.0, ge=0, description="Dividend yield %")
    dividend_payout: float = Field(0.0, ge=0, description="Dividend payout ratio")
    roe: float = Field(0.0, description="Return on equity %")
    roa: float = Field(0.0, description="Return on assets %")
    debt_equity: float = Field(0.0, ge=0, description="Debt-to-equity ratio")
    current_ratio: float = Field(0.0, ge=0, description="Current ratio")
    quick_ratio: float = Field(0.0, ge=0, description="Quick ratio")
    
    # Growth & Profitability
    revenue_growth: float = Field(0.0, description="Revenue growth %")
    net_income_growth: float = Field(0.0, description="Net income growth %")
    ebitda_margin: float = Field(0.0, description="EBITDA margin %")
    operating_margin: float = Field(0.0, description="Operating margin %")
    net_margin: float = Field(0.0, description="Net margin %")
    ev_ebitda: float = Field(0.0, ge=0, description="EV/EBITDA ratio")
    price_sales: float = Field(0.0, ge=0, description="Price-to-sales ratio")
    price_cash_flow: float = Field(0.0, ge=0, description="Price-to-cash-flow ratio")
    peg_ratio: float = Field(0.0, description="PEG ratio")
    opportunity_score: float = Field(0.0, ge=0, le=100, description="Investment opportunity score")
    
    # Technical Indicators
    rsi_14: float = Field(0.0, ge=0, le=100, description="14-day RSI")
    macd: float = Field(0.0, description="MACD value")
    ma_20d: float = Field(0.0, ge=0, description="20-day moving average")
    ma_50d: float = Field(0.0, ge=0, description="50-day moving average")
    volatility: float = Field(0.0, ge=0, description="Volatility %")
    
    # Metadata
    last_updated: str = Field("", description="Last update timestamp")
    last_updated_riyadh: str = Field("", description="Riyadh time update")
    data_source: DataSource = Field(DataSource.UNKNOWN, description="Data source")
    data_quality: DataQuality = Field(DataQuality.UNKNOWN, description="Data quality assessment")
    timestamp: str = Field(default_factory=lambda: datetime.now(timezone.utc).isoformat())
    raw_data: Dict[str, Any] = Field(default_factory=dict, description="Raw data from source")
    
    class Config:
        use_enum_values = True
    
    @property
    def is_valid(self) -> bool:
        """Check if data meets minimum validation criteria."""
        return (
            bool(self.ticker) and
            self.last_price > 0 and
            self.data_quality != DataQuality.LOW
        )
    
    @property
    def market_cap_formatted(self) -> str:
        """Format market cap for display."""
        if self.market_cap >= 1_000_000_000:
            return f"{self.market_cap / 1_000_000_000:.2f}B"
        elif self.market_cap >= 1_000_000:
            return f"{self.market_cap / 1_000_000:.2f}M"
        else:
            return f"{self.market_cap:,.0f}"
    
    def to_dict(self, exclude_raw: bool = True) -> Dict[str, Any]:
        """Convert to dictionary with optional exclusions."""
        data = self.dict()
        if exclude_raw:
            data.pop('raw_data', None)
        return data


@dataclass
class RequestMetrics:
    """Metrics for tracking sheet requests."""
    request_id: str
    start_time: float
    end_time: Optional[float] = None
    sheet_id: Optional[str] = None
    range: Optional[str] = None
    rows_fetched: int = 0
    rows_processed: int = 0
    cache_hit: bool = False
    success: Optional[bool] = None
    error: Optional[str] = None
    
    @property
    def duration(self) -> Optional[float]:
        """Calculate request duration."""
        if self.end_time:
            return self.end_time - self.start_time
        return None
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return {
            "request_id": self.request_id,
            "duration": self.duration,
            "sheet_id": self.sheet_id,
            "range": self.range,
            "rows_fetched": self.rows_fetched,
            "rows_processed": self.rows_processed,
            "cache_hit": self.cache_hit,
            "success": self.success,
            "error": self.error,
        }


# ======================================================================
# Enhanced Async Google Sheets Service
# ======================================================================

class AsyncGoogleSheetsService:
    """
    Enhanced async Google Sheets service with comprehensive features.
    Aligned with google_apps_script_client.py patterns.
    """
    
    # Column mappings for different data sources
    COLUMN_MAPPINGS = {
        "KSA_TADAWUL": {
            "ticker": ["Symbol", "Ticker", "Code"],
            "company_name": ["Company Name", "Instrument Name"],
            "sector": ["Sector"],
            "sub_sector": ["Sub-Sector"],
            "last_price": ["Last Price", "Price"],
            "volume": ["Volume"],
            "market_cap": ["Market Cap"],
            "pe": ["P/E", "P/E Ratio"],
            "dividend_yield": ["Dividend Yield"],
            "custom_tag": ["Custom Tag", "Custom Tag / Watchlist", "Watchlist"]
        },
        "TADAWUL_ALL": {
            "ticker": ["Symbol", "Ticker"],
            "company_name": ["Company Name", "Name"],
            "sector": ["Sector", "Industry"],
            "market_cap": ["Market Cap", "Market Capitalization"],
            "price": ["Price", "Close"],
            "change": ["Change", "Change %"]
        }
    }
    
    def __init__(self, config: Optional[SheetsConfig] = None):
        """Initialize async Google Sheets service."""
        # Load configuration
        self.config = config or SheetsConfig.from_env()
        
        # Validate configuration
        self._validate_config()
        
        # Create cache directory
        self.cache_dir = Path(self.config.cache_dir)
        self.cache_dir.mkdir(parents=True, exist_ok=True)
        
        # In-memory cache
        self._memory_cache: Dict[str, Tuple[Any, float]] = {}
        self._cache_lock = asyncio.Lock()
        
        # Request tracking
        self.request_count = 0
        self.success_count = 0
        self.error_count = 0
        self.cache_hits = 0
        self.request_history: List[RequestMetrics] = []
        self.max_history = 1000
        
        # Rate limiting
        self.request_timestamps: List[float] = []
        self._rate_limit_lock = asyncio.Lock()
        
        # Initialize Google Sheets service (synchronous, will run in executor)
        self._service = None
        self._service_lock = asyncio.Lock()
        
        # Sheet IDs mapping
        self.sheet_ids = {
            "KSA_TADAWUL": self.config.ksa_tadawul_sheet_id,
            "TADAWUL_ALL": self.config.tadawul_all_sheet_id,
            "MARKET_DATA": self.config.market_data_sheet_id,
            "PORTFOLIO": self.config.portfolio_sheet_id,
        }
        
        logger.info(
            f"AsyncGoogleSheetsService initialized: "
            f"cache_enabled={self.config.cache_enabled}, "
            f"cache_ttl={self.config.cache_ttl}s, "
            f"rate_limit={self.config.rate_limit_rpm} RPM"
        )
    
    def _validate_config(self) -> None:
        """Validate configuration."""
        if not self.config.ksa_tadawul_sheet_id:
            raise ValueError("KSA_TADAWUL_SHEET_ID is required")
        
        if not self.config.credentials_json and not self.config.credentials_path:
            raise ValueError(
                "Either GOOGLE_SHEETS_CREDENTIALS_JSON or GOOGLE_CREDENTIALS_PATH is required"
            )
    
    async def _get_service(self):
        """Get or create Google Sheets service (run in executor)."""
        if self._service is None:
            async with self._service_lock:
                if self._service is None:
                    await self._initialize_service()
        return self._service
    
    async def _initialize_service(self):
        """Initialize Google Sheets service with credentials."""
        try:
            # Run synchronous initialization in executor
            loop = asyncio.get_running_loop()
            self._service = await loop.run_in_executor(
                None,
                self._initialize_service_sync
            )
            logger.info("Google Sheets service authenticated successfully")
        except Exception as e:
            logger.error(f"Failed to initialize Google Sheets service: {e}")
            raise
    
    def _initialize_service_sync(self):
        """Synchronous service initialization."""
        try:
            if self.config.credentials_json:
                credentials_info = json.loads(self.config.credentials_json)
                creds = Credentials.from_service_account_info(
                    credentials_info,
                    scopes=['https://www.googleapis.com/auth/spreadsheets.readonly']
                )
            elif self.config.credentials_path:
                creds = Credentials.from_service_account_file(
                    self.config.credentials_path,
                    scopes=['https://www.googleapis.com/auth/spreadsheets.readonly']
                )
            else:
                raise ValueError("No credentials provided")
            
            service = build('sheets', 'v4', credentials=creds)
            return service
            
        except Exception as e:
            logger.error(f"Failed to initialize Google Sheets service: {e}")
            raise
    
    async def _enforce_rate_limit(self) -> None:
        """Async rate limiting for Google Sheets API."""
        async with self._rate_limit_lock:
            now = time.time()
            window_start = now - 60  # 1 minute window
            
            # Remove old timestamps
            self.request_timestamps = [ts for ts in self.request_timestamps if ts > window_start]
            
            # Check if we're at limit
            if len(self.request_timestamps) >= self.config.rate_limit_rpm:
                oldest_timestamp = self.request_timestamps[0]
                sleep_time = 60 - (now - oldest_timestamp)
                
                if sleep_time > 0:
                    logger.warning(f"Rate limit exceeded, sleeping for {sleep_time:.2f}s")
                    await asyncio.sleep(sleep_time + 0.1)  # Add small buffer
                    
                    # Recalculate after sleep
                    now = time.time()
                    window_start = now - 60
                    self.request_timestamps = [ts for ts in self.request_timestamps if ts > window_start]
            
            # Add jitter and record this request
            self.request_timestamps.append(now)
    
    def _generate_request_id(self) -> str:
        """Generate unique request ID."""
        self.request_count += 1
        timestamp = int(time.time() * 1000)
        unique_id = hashlib.md5(str(timestamp).encode()).hexdigest()[:8]
        return f"sheets_{timestamp}_{self.request_count:06d}_{unique_id}"
    
    def _get_cache_key(self, sheet_id: str, range_name: str, **kwargs) -> str:
        """Generate cache key from parameters."""
        params = sorted([f"{k}={v}" for k, v in kwargs.items()])
        key_str = f"{sheet_id}:{range_name}:{'|'.join(params)}"
        return hashlib.md5(key_str.encode()).hexdigest()
    
    async def _load_from_cache(self, cache_key: str) -> Optional[Any]:
        """Load data from cache (memory or file)."""
        if not self.config.cache_enabled:
            return None
        
        try:
            # Check memory cache first
            async with self._cache_lock:
                if cache_key in self._memory_cache:
                    data, timestamp = self._memory_cache[cache_key]
                    if time.time() - timestamp < self.config.cache_ttl:
                        self.cache_hits += 1
                        logger.debug(f"Memory cache hit: {cache_key}")
                        return data
                    else:
                        del self._memory_cache[cache_key]
            
            # Check file cache
            cache_file = self.cache_dir / f"{cache_key}.json"
            if cache_file.exists():
                async with aiofiles.open(cache_file, 'r') as f:
                    content = await f.read()
                    cache_data = json.loads(content)
                
                # Check if cache is still valid
                if time.time() - cache_data.get('_timestamp', 0) < self.config.cache_ttl:
                    self.cache_hits += 1
                    logger.debug(f"File cache hit: {cache_key}")
                    
                    # Also store in memory cache
                    async with self._cache_lock:
                        self._memory_cache[cache_key] = (
                            cache_data['data'],
                            cache_data['_timestamp']
                        )
                    
                    return cache_data['data']
                else:
                    # Remove expired cache file
                    cache_file.unlink(missing_ok=True)
        
        except Exception as e:
            logger.debug(f"Cache load failed: {e}")
        
        return None
    
    async def _save_to_cache(self, cache_key: str, data: Any) -> None:
        """Save data to cache (memory and file)."""
        if not self.config.cache_enabled:
            return
        
        try:
            timestamp = time.time()
            cache_data = {
                'data': data,
                '_timestamp': timestamp,
                '_expires': timestamp + self.config.cache_ttl,
                '_key': cache_key,
            }
            
            # Save to memory cache
            async with self._cache_lock:
                self._memory_cache[cache_key] = (data, timestamp)
                
                # Limit memory cache size
                if len(self._memory_cache) > self.config.memory_cache_size:
                    # Remove oldest entries
                    sorted_items = sorted(self._memory_cache.items(), key=lambda x: x[1][1])
                    for key, _ in sorted_items[:100]:
                        del self._memory_cache[key]
            
            # Save to file cache
            cache_file = self.cache_dir / f"{cache_key}.json"
            async with aiofiles.open(cache_file, 'w') as f:
                await f.write(json.dumps(cache_data, indent=2))
            
            logger.debug(f"Saved to cache: {cache_key}")
            
        except Exception as e:
            logger.debug(f"Cache save failed: {e}")
    
    @retry(
        retry=retry_if_exception_type((HttpError, ConnectionError, TimeoutError)),
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=1, max=10),
        before_sleep=before_sleep_log(logger, logging.WARNING),
    )
    async def _make_sheet_request(
        self,
        service,
        request_type: str,
        spreadsheet_id: str,
        range_name: str,
        **kwargs
    ) -> Dict[str, Any]:
        """Make sheet request with retry logic."""
        try:
            # Enforce rate limit
            await self._enforce_rate_limit()
            
            loop = asyncio.get_running_loop()
            
            if request_type == "get":
                request = service.spreadsheets().values().get(
                    spreadsheetId=spreadsheet_id,
                    range=range_name,
                    **kwargs
                )
            elif request_type == "batchGet":
                request = service.spreadsheets().values().batchGet(
                    spreadsheetId=spreadsheet_id,
                    ranges=range_name,
                    **kwargs
                )
            else:
                raise ValueError(f"Unknown request type: {request_type}")
            
            # Execute in executor
            response = await loop.run_in_executor(None, request.execute)
            return response
            
        except Exception as e:
            logger.error(f"Sheet request failed: {e}")
            raise
    
    async def _add_to_history(self, metrics: RequestMetrics) -> None:
        """Add request metrics to history."""
        self.request_history.append(metrics)
        # Keep only recent history
        if len(self.request_history) > self.max_history:
            self.request_history = self.request_history[-self.max_history:]
    
    def _safe_float(self, value: Any) -> float:
        """Safely convert any value to float."""
        if value is None:
            return 0.0
        
        try:
            if isinstance(value, (int, float)):
                return float(value)
            elif isinstance(value, str):
                # Remove non-numeric characters except decimal and minus
                cleaned = ''.join(c for c in value if c.isdigit() or c in '.-')
                if cleaned and cleaned not in ['.', '-']:
                    return float(cleaned)
        except (ValueError, TypeError):
            pass
        
        return 0.0
    
    def _safe_int(self, value: Any) -> int:
        """Safely convert any value to integer."""
        return int(self._safe_float(value))
    
    def _safe_str(self, value: Any) -> str:
        """Safely convert any value to string."""
        if value is None:
            return ""
        return str(value).strip()
    
    def _map_column_name(self, row: Dict[str, Any], field_name: str, sheet_type: str) -> Any:
        """Map field name to actual column name in sheet."""
        mappings = self.COLUMN_MAPPINGS.get(sheet_type, {})
        if field_name in mappings:
            for possible_name in mappings[field_name]:
                if possible_name in row:
                    return row[possible_name]
        # Fallback to direct field name
        return row.get(field_name)
    
    def _assess_data_quality(self, data: Dict[str, Any]) -> DataQuality:
        """Assess quality of financial data."""
        score = 0
        total_fields = 0
        
        # Critical fields (must have)
        critical_fields = ['ticker', 'last_price', 'market_cap']
        for field in critical_fields:
            if field in data and data[field]:
                if field == 'last_price' and data[field] > 0:
                    score += 3
                elif field == 'ticker' and data[field]:
                    score += 3
                else:
                    score += 1
            total_fields += 3
        
        # Important fields
        important_fields = ['volume', 'pe', 'sector', 'company_name']
        for field in important_fields:
            if field in data and data[field]:
                score += 2
            total_fields += 2
        
        # Optional fields
        optional_fields = ['dividend_yield', 'eps', 'roe', 'change_pct']
        for field in optional_fields:
            if field in data and data[field]:
                score += 1
            total_fields += 1
        
        if total_fields == 0:
            return DataQuality.UNKNOWN
        
        quality_score = score / total_fields
        
        if quality_score >= 0.8:
            return DataQuality.EXCELLENT
        elif quality_score >= 0.6:
            return DataQuality.HIGH
        elif quality_score >= 0.4:
            return DataQuality.MEDIUM
        else:
            return DataQuality.LOW
    
    @asynccontextmanager
    async def request_context(self, sheet_id: str, range_name: str):
        """Context manager for sheet requests with metrics."""
        request_id = self._generate_request_id()
        metrics = RequestMetrics(
            request_id=request_id,
            start_time=time.perf_counter(),
            sheet_id=sheet_id,
            range=range_name,
        )
        
        logger.debug(f"[{metrics.request_id}] Starting sheet request")
        
        try:
            yield metrics
        except Exception as e:
            metrics.error = str(e)
            metrics.success = False
            raise
        finally:
            metrics.end_time = time.perf_counter()
            await self._add_to_history(metrics)
            logger.debug(f"[{metrics.request_id}] Request completed: {metrics.duration:.3f}s")
    
    # ==================================================================
    # Public Async API Methods
    # ==================================================================
    
    async def read_sheet_data(
        self,
        sheet_type: str = "KSA_TADAWUL",
        range_name: str = "A:Z",
        use_cache: bool = True,
        validate: bool = True,
        limit: Optional[int] = None,
        as_dataframe: bool = False,
    ) -> Union[List[Dict[str, Any]], pd.DataFrame]:
        """
        Read data from Google Sheets with caching and validation.
        
        Args:
            sheet_type: Type of sheet (KSA_TADAWUL, TADAWUL_ALL, etc.)
            range_name: Range to read (e.g., "A:Z", "Sheet1!A1:E100")
            use_cache: Whether to use cached data
            validate: Validate data quality
            limit: Maximum number of rows to return
            as_dataframe: Return as pandas DataFrame
        
        Returns:
            List of dictionaries or pandas DataFrame
        """
        sheet_id = self.sheet_ids.get(sheet_type)
        if not sheet_id:
            raise ValueError(f"Sheet type '{sheet_type}' not configured")
        
        cache_key = self._get_cache_key(sheet_id, range_name, validate=validate, limit=limit)
        
        if use_cache:
            cached_data = await self._load_from_cache(cache_key)
            if cached_data is not None:
                logger.info(f"Using cached data for {sheet_type}")
                if limit and isinstance(cached_data, list):
                    cached_data = cached_data[:limit]
                if as_dataframe:
                    return pd.DataFrame(cached_data)
                return cached_data
        
        async with self.request_context(sheet_id, range_name) as metrics:
            try:
                service = await self._get_service()
                
                # Make request
                response = await self._make_sheet_request(
                    service=service,
                    request_type="get",
                    spreadsheet_id=sheet_id,
                    range_name=range_name,
                )
                
                values = response.get('values', [])
                if not values:
                    return [] if not as_dataframe else pd.DataFrame()
                
                # Process rows
                headers = values[0]
                data_rows = []
                
                for i, row in enumerate(values[1:], start=1):
                    if not row:
                        continue
                    
                    # Pad row to match headers length
                    padded_row = row + [''] * (len(headers) - len(row))
                    data_dict = dict(zip(headers, padded_row))
                    
                    # Process based on sheet type
                    processed_row = await self._process_row(data_dict, sheet_type, validate)
                    if processed_row:
                        data_rows.append(processed_row)
                    
                    if limit and len(data_rows) >= limit:
                        break
                
                metrics.rows_fetched = len(values) - 1
                metrics.rows_processed = len(data_rows)
                metrics.success = True
                
                logger.info(f"Read {len(data_rows)} rows from {sheet_type}")
                
                # Cache the results
                if use_cache and data_rows:
                    await self._save_to_cache(cache_key, data_rows)
                
                self.success_count += 1
                
                # Return as DataFrame if requested
                if as_dataframe:
                    return pd.DataFrame(data_rows)
                
                return data_rows
                
            except Exception as e:
                metrics.error = str(e)
                metrics.success = False
                self.error_count += 1
                logger.error(f"Failed to read sheet {sheet_type}: {e}")
                raise
    
    async def _process_row(
        self,
        row: Dict[str, Any],
        sheet_type: str,
        validate: bool = True
    ) -> Optional[Dict[str, Any]]:
        """Process a single row based on sheet type."""
        try:
            if sheet_type == "KSA_TADAWUL":
                return await self._process_ksa_tadawul_row(row, validate)
            elif sheet_type == "TADAWUL_ALL":
                return await self._process_tadawul_all_row(row, validate)
            else:
                return self._process_generic_row(row, validate)
        except Exception as e:
            logger.warning(f"Failed to process row: {e}")
            return None
    
    async def _process_ksa_tadawul_row(
        self,
        row: Dict[str, Any],
        validate: bool = True
    ) -> Optional[Dict[str, Any]]:
        """Process KSA Tadawul row."""
        # Extract data using column mappings
        ticker = self._safe_str(self._map_column_name(row, 'ticker', "KSA_TADAWUL"))
        if not ticker:
            return None
        
        # Create FinancialData object
        financial_data = FinancialData(
            ticker=ticker,
            custom_tag=self._safe_str(self._map_column_name(row, 'custom_tag', "KSA_TADAWUL")),
            company_name=self._safe_str(self._map_column_name(row, 'company_name', "KSA_TADAWUL")),
            sector=self._safe_str(self._map_column_name(row, 'sector', "KSA_TADAWUL")),
            sub_sector=self._safe_str(self._map_column_name(row, 'sub_sector', "KSA_TADAWUL")),
            trading_market=self._safe_str(row.get("Trading Market", "")),
            currency=self._safe_str(row.get("Currency", self.config.default_currency)),
            
            # Market Data
            last_price=self._safe_float(self._map_column_name(row, 'last_price', "KSA_TADAWUL")),
            day_high=self._safe_float(row.get("Day High")),
            day_low=self._safe_float(row.get("Day Low")),
            previous_close=self._safe_float(row.get("Previous Close")),
            open_price=self._safe_float(row.get("Open")),
            change_value=self._safe_float(row.get("Change Value")),
            change_pct=self._safe_float(row.get("Change %")),
            high_52w=self._safe_float(row.get("52 Week High")),
            low_52w=self._safe_float(row.get("52 Week Low")),
            avg_price_50d=self._safe_float(row.get("Average Price (50D)")),
            
            # Volume & Liquidity
            volume=self._safe_int(row.get("Volume")),
            avg_volume_30d=self._safe_int(row.get("Average Volume (30D)")),
            value_traded=self._safe_float(row.get("Value Traded")),
            turnover_rate=self._safe_float(row.get("Turnover Rate")),
            bid_price=self._safe_float(row.get("Bid Price")),
            ask_price=self._safe_float(row.get("Ask Price")),
            bid_size=self._safe_int(row.get("Bid Size")),
            ask_size=self._safe_int(row.get("Ask Size")),
            spread_pct=self._safe_float(row.get("Spread %")),
            liquidity_score=self._safe_float(row.get("Liquidity Score")),
            
            # Company Metrics
            shares_outstanding=self._safe_float(row.get("Shares Outstanding")),
            free_float=self._safe_float(row.get("Free Float")),
            market_cap=self._safe_float(self._map_column_name(row, 'market_cap', "KSA_TADAWUL")),
            listing_date=self._safe_str(row.get("Listing Date")),
            
            # Valuation Ratios
            eps=self._safe_float(row.get("EPS")),
            pe=self._safe_float(self._map_column_name(row, 'pe', "KSA_TADAWUL")),
            pb=self._safe_float(row.get("P/B")),
            dividend_yield=self._safe_float(self._map_column_name(row, 'dividend_yield', "KSA_TADAWUL")),
            dividend_payout=self._safe_float(row.get("Dividend Payout")),
            roe=self._safe_float(row.get("ROE")),
            roa=self._safe_float(row.get("ROA")),
            debt_equity=self._safe_float(row.get("Debt/Equity")),
            current_ratio=self._safe_float(row.get("Current Ratio")),
            quick_ratio=self._safe_float(row.get("Quick Ratio")),
            
            # Growth & Profitability
            revenue_growth=self._safe_float(row.get("Revenue Growth")),
            net_income_growth=self._safe_float(row.get("Net Income Growth")),
            ebitda_margin=self._safe_float(row.get("EBITDA Margin")),
            operating_margin=self._safe_float(row.get("Operating Margin")),
            net_margin=self._safe_float(row.get("Net Margin")),
            ev_ebitda=self._safe_float(row.get("EV/EBITDA")),
            price_sales=self._safe_float(row.get("Price/Sales")),
            price_cash_flow=self._safe_float(row.get("Price/Cash Flow")),
            peg_ratio=self._safe_float(row.get("PEG Ratio")),
            opportunity_score=self._safe_float(row.get("Opportunity Score")),
            
            # Technical Indicators
            rsi_14=self._safe_float(row.get("RSI (14)")),
            macd=self._safe_float(row.get("MACD")),
            ma_20d=self._safe_float(row.get("Moving Avg (20D)")),
            ma_50d=self._safe_float(row.get("Moving Avg (50D)")),
            volatility=self._safe_float(row.get("Volatility")),
            
            # Metadata
            last_updated=self._safe_str(row.get("Last Updated")),
            last_updated_riyadh=self._safe_str(row.get("Last Updated (Riyadh)")),
            data_source=DataSource.GOOGLE_SHEETS,
            raw_data=row,
        )
        
        # Assess data quality
        financial_data.data_quality = self._assess_data_quality(financial_data.dict())
        
        # Validate if required
        if validate and not financial_data.is_valid:
            return None
        
        return financial_data.dict()
    
    async def _process_tadawul_all_row(
        self,
        row: Dict[str, Any],
        validate: bool = True
    ) -> Optional[Dict[str, Any]]:
        """Process Tadawul All row."""
        ticker = self._safe_str(self._map_column_name(row, 'ticker', "TADAWUL_ALL"))
        if not ticker:
            return None
        
        processed_data = {
            "ticker": ticker,
            "company_name": self._safe_str(self._map_column_name(row, 'company_name', "TADAWUL_ALL")),
            "sector": self._safe_str(self._map_column_name(row, 'sector', "TADAWUL_ALL")),
            "market_cap": self._safe_float(self._map_column_name(row, 'market_cap', "TADAWUL_ALL")),
            "price": self._safe_float(self._map_column_name(row, 'price', "TADAWUL_ALL")),
            "change": self._safe_float(self._map_column_name(row, 'change', "TADAWUL_ALL")),
            "volume": self._safe_int(row.get("Volume")),
            "pe": self._safe_float(row.get("P/E")),
            "dividend_yield": self._safe_float(row.get("Dividend Yield")),
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "data_source": DataSource.GOOGLE_SHEETS.value,
            "data_quality": DataQuality.UNKNOWN.value,
            "raw_data": row,
        }
        
        # Assess data quality
        processed_data["data_quality"] = self._assess_data_quality(processed_data)
        
        if validate and processed_data["data_quality"] == DataQuality.LOW:
            return None
        
        return processed_data
    
    def _process_generic_row(
        self,
        row: Dict[str, Any],
        validate: bool = True
    ) -> Dict[str, Any]:
        """Process generic row."""
        processed_row = {}
        for key, value in row.items():
            if self.config.parse_numeric:
                try:
                    # Try to parse as number
                    if isinstance(value, str) and value.replace('.', '', 1).replace('-', '', 1).isdigit():
                        if '.' in value:
                            processed_row[key] = float(value)
                        else:
                            processed_row[key] = int(value)
                    else:
                        processed_row[key] = value
                except (ValueError, TypeError):
                    processed_row[key] = value
            else:
                processed_row[key] = value
        
        return processed_row
    
    async def read_ksa_tadawul_market(
        self,
        use_cache: bool = True,
        validate: bool = True,
        filters: Optional[Dict[str, Any]] = None,
        limit: Optional[int] = None,
        as_dataframe: bool = False,
    ) -> Union[List[Dict[str, Any]], pd.DataFrame]:
        """
        Read KSA Tadawul market data with filtering.
        
        Args:
            use_cache: Use cached data
            validate: Validate data quality
            filters: Dictionary of filters to apply
            limit: Maximum number of results
            as_dataframe: Return as pandas DataFrame
        
        Returns:
            Filtered and processed market data
        """
        logger.info(f"Reading KSA Tadawul market data (cache: {use_cache})")
        
        data = await self.read_sheet_data(
            sheet_type="KSA_TADAWUL",
            use_cache=use_cache,
            validate=validate,
            limit=limit,
            as_dataframe=False,
        )
        
        # Apply filters if provided
        if filters and data:
            data = await self._apply_filters(data, filters)
        
        logger.info(f"Retrieved {len(data)} KSA Tadawul instruments")
        
        if as_dataframe:
            return pd.DataFrame(data)
        
        return data
    
    async def _apply_filters(
        self,
        data: List[Dict[str, Any]],
        filters: Dict[str, Any]
    ) -> List[Dict[str, Any]]:
        """Apply filters to data."""
        filtered_data = []
        
        for item in data:
            include = True
            
            for field, condition in filters.items():
                if field not in item:
                    include = False
                    break
                
                value = item[field]
                
                # Handle different filter types
                if isinstance(condition, dict):
                    if "min" in condition and value < condition["min"]:
                        include = False
                    if "max" in condition and value > condition["max"]:
                        include = False
                    if "equals" in condition and value != condition["equals"]:
                        include = False
                    if "in" in condition and value not in condition["in"]:
                        include = False
                    if "contains" in condition and condition["contains"] not in str(value):
                        include = False
                elif isinstance(condition, (list, tuple)):
                    if value not in condition:
                        include = False
                else:
                    if value != condition:
                        include = False
                
                if not include:
                    break
            
            if include:
                filtered_data.append(item)
        
        return filtered_data
    
    async def get_ticker_details(
        self,
        ticker: str,
        use_cache: bool = True,
        validate: bool = True,
    ) -> Optional[Dict[str, Any]]:
        """Get detailed information for specific ticker."""
        data = await self.read_ksa_tadawul_market(
            use_cache=use_cache,
            validate=validate,
        )
        
        for item in data:
            if item.get('ticker') == ticker:
                return item
        
        return None
    
    async def search_by_sector(
        self,
        sector: str,
        use_cache: bool = True,
        validate: bool = True,
        case_sensitive: bool = False,
    ) -> List[Dict[str, Any]]:
        """Search instruments by sector."""
        data = await self.read_ksa_tadawul_market(
            use_cache=use_cache,
            validate=validate,
        )
        
        if case_sensitive:
            return [
                item for item in data 
                if sector in item.get('sector', '')
            ]
        else:
            return [
                item for item in data 
                if sector.lower() in item.get('sector', '').lower()
            ]
    
    async def get_top_by_market_cap(
        self,
        top_n: int = 10,
        use_cache: bool = True,
        validate: bool = True,
    ) -> List[Dict[str, Any]]:
        """Get top N companies by market capitalization."""
        data = await self.read_ksa_tadawul_market(
            use_cache=use_cache,
            validate=validate,
        )
        
        # Filter and sort by market cap
        valid_data = [d for d in data if d.get('market_cap', 0) > 0]
        sorted_data = sorted(valid_data, key=lambda x: x.get('market_cap', 0), reverse=True)
        
        return sorted_data[:top_n]
    
    async def batch_read_sheets(
        self,
        sheet_types: List[str],
        use_cache: bool = True,
        validate: bool = True,
        as_dataframe: bool = False,
    ) -> Dict[str, Union[List[Dict[str, Any]], pd.DataFrame]]:
        """Read multiple sheets in batch."""
        results = {}
        
        tasks = []
        for sheet_type in sheet_types:
            task = self.read_sheet_data(
                sheet_type=sheet_type,
                use_cache=use_cache,
                validate=validate,
                as_dataframe=as_dataframe,
            )
            tasks.append((sheet_type, task))
        
        for sheet_type, task in tasks:
            try:
                data = await task
                results[sheet_type] = data
                logger.info(f"Read {len(data) if isinstance(data, list) else 'dataframe'} from {sheet_type}")
            except Exception as e:
                logger.error(f"Failed to read {sheet_type}: {e}")
                results[sheet_type] = [] if not as_dataframe else pd.DataFrame()
        
        return results
    
    async def export_to_dataframe(
        self,
        sheet_type: str = "KSA_TADAWUL",
        use_cache: bool = True,
        validate: bool = True,
    ) -> pd.DataFrame:
        """Export sheet data to pandas DataFrame."""
        data = await self.read_sheet_data(
            sheet_type=sheet_type,
            use_cache=use_cache,
            validate=validate,
            as_dataframe=True,
        )
        
        if data.empty:
            return data
        
        # Convert numeric columns
        numeric_columns = [
            'last_price', 'volume', 'market_cap', 'pe', 'dividend_yield',
            'change_pct', 'eps', 'roe'
        ]
        
        for col in numeric_columns:
            if col in data.columns:
                data[col] = pd.to_numeric(data[col], errors='coerce')
        
        return data
    
    async def clear_cache(
        self,
        sheet_type: Optional[str] = None,
        clear_memory: bool = True,
        clear_files: bool = True,
    ) -> None:
        """Clear cache for specific sheet or all sheets."""
        cleared_count = 0
        
        # Clear memory cache
        if clear_memory:
            async with self._cache_lock:
                if sheet_type:
                    # Find cache keys for specific sheet
                    sheet_id = self.sheet_ids.get(sheet_type)
                    if sheet_id:
                        keys_to_remove = [
                            key for key in self._memory_cache.keys()
                            if key.startswith(hashlib.md5(sheet_id.encode()).hexdigest()[:16])
                        ]
                        for key in keys_to_remove:
                            del self._memory_cache[key]
                            cleared_count += 1
                else:
                    self._memory_cache.clear()
                    cleared_count += 1
        
        # Clear file cache
        if clear_files:
            if sheet_type:
                # Find files for specific sheet
                sheet_id = self.sheet_ids.get(sheet_type)
                if sheet_id:
                    prefix = hashlib.md5(sheet_id.encode()).hexdigest()[:16]
                    for cache_file in self.cache_dir.glob(f"{prefix}*.json"):
                        cache_file.unlink(missing_ok=True)
                        cleared_count += 1
            else:
                for cache_file in self.cache_dir.glob("*.json"):
                    cache_file.unlink(missing_ok=True)
                    cleared_count += 1
        
        logger.info(f"Cleared {cleared_count} cache entries")
    
    async def get_stats(self) -> Dict[str, Any]:
        """Get comprehensive service statistics."""
        total_requests = self.success_count + self.error_count
        success_rate = (
            self.success_count / total_requests * 100
            if total_requests > 0
            else 0
        )
        
        # Cache statistics
        cache_size = 0
        cache_files = list(self.cache_dir.glob("*.json"))
        if cache_files:
            for cache_file in cache_files:
                cache_size += cache_file.stat().st_size
        
        return {
            "service": "AsyncGoogleSheetsService",
            "version": "4.0.0",
            "performance": {
                "total_requests": total_requests,
                "successful_requests": self.success_count,
                "failed_requests": self.error_count,
                "success_rate_percent": round(success_rate, 2),
                "cache_hits": self.cache_hits,
                "cache_hit_rate": round(self.cache_hits / max(total_requests, 1) * 100, 2),
            },
            "cache": {
                "enabled": self.config.cache_enabled,
                "ttl_seconds": self.config.cache_ttl,
                "memory_entries": len(self._memory_cache),
                "file_entries": len(cache_files),
                "total_size_bytes": cache_size,
                "directory": str(self.cache_dir),
            },
            "configuration": {
                "rate_limit_rpm": self.config.rate_limit_rpm,
                "request_timeout": self.config.request_timeout,
                "max_retries": self.config.max_retries,
                "batch_size": self.config.batch_size,
                "validate_data": self.config.validate_data,
                "parse_numeric": self.config.parse_numeric,
            },
            "sheets_configured": {
                sheet_type: bool(sheet_id)
                for sheet_type, sheet_id in self.sheet_ids.items()
            },
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }
    
    async def get_request_history(
        self,
        limit: int = 50,
        filter_success: Optional[bool] = None,
        sort_by: str = "start_time",
        reverse: bool = True,
    ) -> List[Dict[str, Any]]:
        """Get recent request history with filtering."""
        history = self.request_history
        
        # Filter by success
        if filter_success is not None:
            history = [h for h in history if h.success == filter_success]
        
        # Sort
        if sort_by == "duration":
            history.sort(key=lambda x: x.duration or 0, reverse=reverse)
        elif sort_by == "start_time":
            history.sort(key=lambda x: x.start_time, reverse=reverse)
        
        # Limit
        if limit > 0:
            history = history[:limit]
        
        return [h.to_dict() for h in history]
    
    async def close(self) -> None:
        """Cleanup resources."""
        # Clear memory cache
        async with self._cache_lock:
            self._memory_cache.clear()
        
        logger.info("Google Sheets service closed")
    
    async def __aenter__(self):
        """Async context manager entry."""
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit."""
        await self.close()


# ======================================================================
# Global Async Instance Management
# ======================================================================

# Create global async instance
_google_sheets_service: Optional[AsyncGoogleSheetsService] = None
_service_lock = asyncio.Lock()

async def get_google_sheets_service(
    config: Optional[SheetsConfig] = None,
    refresh: bool = False,
) -> AsyncGoogleSheetsService:
    """
    Get or create global Google Sheets service instance.
    
    Args:
        config: Optional custom configuration
        refresh: Force refresh of service instance
    
    Returns:
        AsyncGoogleSheetsService instance
    """
    global _google_sheets_service
    
    async with _service_lock:
        if refresh and _google_sheets_service:
            await _google_sheets_service.close()
            _google_sheets_service = None
        
        if _google_sheets_service is None:
            _google_sheets_service = AsyncGoogleSheetsService(config=config)
            logger.info("Global AsyncGoogleSheetsService instance created")
        
        return _google_sheets_service


async def close_google_sheets_service() -> None:
    """Close global Google Sheets service."""
    global _google_sheets_service
    if _google_sheets_service:
        await _google_sheets_service.close()
        _google_sheets_service = None
        logger.info("Global AsyncGoogleSheetsService closed")


# ======================================================================
# Convenience Async Functions
# ======================================================================

async def read_ksa_tadawul_market_async(
    use_cache: bool = True,
    validate: bool = True,
    filters: Optional[Dict[str, Any]] = None,
    limit: Optional[int] = None,
) -> List[Dict[str, Any]]:
    """Async convenience function for market data."""
    service = await get_google_sheets_service()
    return await service.read_ksa_tadawul_market(
        use_cache=use_cache,
        validate=validate,
        filters=filters,
        limit=limit,
    )


async def get_ticker_details_async(
    ticker: str,
    use_cache: bool = True,
    validate: bool = True,
) -> Optional[Dict[str, Any]]:
    """Async convenience function for ticker details."""
    service = await get_google_sheets_service()
    return await service.get_ticker_details(
        ticker=ticker,
        use_cache=use_cache,
        validate=validate,
    )


async def get_sheets_stats_async() -> Dict[str, Any]:
    """Async convenience function for service statistics."""
    service = await get_google_sheets_service()
    return await service.get_stats()


# ======================================================================
# Enhanced Test Function
# ======================================================================

async def test_service():
    """Enhanced test for the async Google Sheets service."""
    import sys
    
    # Configure logging
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )
    
    try:
        service = await get_google_sheets_service()
        
        print("=" * 60)
        print("Testing Async Google Sheets Service v4.0.0")
        print("=" * 60)
        
        # Show configuration
        print(f"\n=== Configuration ===")
        print(f"Cache TTL: {service.config.cache_ttl}s")
        print(f"Rate Limit: {service.config.rate_limit_rpm} RPM")
        print(f"Max Retries: {service.config.max_retries}")
        print(f"Cache Enabled: {service.config.cache_enabled}")
        
        # Test reading market data
        print(f"\n=== Reading Market Data ===")
        market_data = await service.read_ksa_tadawul_market(
            use_cache=True,
            validate=True,
            limit=5,
        )
        
        print(f"Retrieved {len(market_data)} instruments")
        
        if market_data:
            print(f"\nSample Instrument:")
            sample = market_data[0]
            print(f"  Ticker: {sample.get('ticker')}")
            print(f"  Company: {sample.get('company_name')}")
            print(f"  Price: {sample.get('last_price')}")
            print(f"  Market Cap: {sample.get('market_cap'):,.0f}")
            print(f"  Quality: {sample.get('data_quality')}")
        
        # Test filters
        print(f"\n=== Testing Filters ===")
        filtered_data = await service.read_ksa_tadawul_market(
            use_cache=True,
            filters={
                "sector": {"contains": "Bank"},
                "market_cap": {"min": 10000000000},  # > 10B SAR
            },
            limit=3,
        )
        
        print(f"Filtered Results: {len(filtered_data)}")
        for item in filtered_data[:3]:
            print(f"  - {item['ticker']}: {item['sector']} (Cap: {item['market_cap']:,.0f})")
        
        # Test statistics
        print(f"\n=== Service Statistics ===")
        stats = await service.get_stats()
        print(f"Total Requests: {stats['performance']['total_requests']}")
        print(f"Success Rate: {stats['performance']['success_rate_percent']}%")
        print(f"Cache Hits: {stats['performance']['cache_hits']}")
        print(f"Memory Cache: {stats['cache']['memory_entries']} entries")
        
        # Test DataFrame export
        print(f"\n=== DataFrame Export ===")
        df = await service.export_to_dataframe(limit=10)
        print(f"DataFrame Shape: {df.shape}")
        print(f"Columns: {list(df.columns)[:10]}...")
        
        print(f"\n=== Test Completed Successfully ===")
        print("=" * 60)
        
    except Exception as e:
        print(f"\n!!! Test Failed: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
    
    finally:
        await close_google_sheets_service()


if __name__ == "__main__":
    # Run test if executed directly
    asyncio.run(test_service())
