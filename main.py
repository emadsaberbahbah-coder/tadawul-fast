# main.py - COMPREHENSIVE UPDATE
import datetime
import json
import os
import logging
import asyncio
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import aiohttp
import gspread
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
import uvicorn

# =============================================================================
# Google Sheets Service Implementation
# =============================================================================

class GoogleSheetsService:
    """
    Service for reading data from Google Sheets with the 10-page structure.
    This provides a clean interface to access sheet data without direct gspread dependencies.
    """
    
    def __init__(self, sheets_manager):
        self.sheets_manager = sheets_manager
        self.logger = logging.getLogger(__name__)
        
    def read_ksa_tadawul_market(self, limit: int = 100) -> List[Dict[str, Any]]:
        """Read KSA Tadawul Market data"""
        try:
            spreadsheet = self.sheets_manager.get_spreadsheet()
            worksheet = spreadsheet.worksheet("KSA Tadawul Market")
            records = worksheet.get_all_records()
            
            # Transform to standardized format
            transformed = []
            for record in records[:limit]:
                transformed.append({
                    'ticker': record.get('Ticker', ''),
                    'company_name': record.get('Company Name', ''),
                    'sector': record.get('Sector', ''),
                    'trading_market': record.get('Trading Market', ''),
                    'current_price': record.get('Current Price', 0),
                    'daily_change': record.get('Daily Change %', 0),
                    'volume': record.get('Volume', 0),
                    'market_cap': record.get('Market Cap', 0),
                    'last_updated': record.get('Last Updated', '')
                })
            
            self.logger.info(f"Read {len(transformed)} records from KSA Tadawul Market")
            return transformed
            
        except Exception as e:
            self.logger.error(f"Error reading KSA Tadawul Market: {e}")
            return []

    def read_global_market_stock(self, limit: int = 50) -> List[Dict[str, Any]]:
        """Read Global Market Stock data"""
        try:
            spreadsheet = self.sheets_manager.get_spreadsheet()
            worksheet = spreadsheet.worksheet("Global Market Stock")
            records = worksheet.get_all_records()
            
            transformed = []
            for record in records[:limit]:
                transformed.append({
                    'symbol': record.get('Symbol', ''),
                    'name': record.get('Name', ''),
                    'exchange': record.get('Exchange', ''),
                    'currency': record.get('Currency', ''),
                    'price': record.get('Price', 0),
                    'change_percent': record.get('Change %', 0),
                    'market_cap': record.get('Market Cap', 0)
                })
            
            self.logger.info(f"Read {len(transformed)} records from Global Market Stock")
            return transformed
            
        except Exception as e:
            self.logger.error(f"Error reading Global Market Stock: {e}")
            return []

    def read_mutual_fund(self, limit: int = 30) -> List[Dict[str, Any]]:
        """Read Mutual Fund data"""
        try:
            spreadsheet = self.sheets_manager.get_spreadsheet()
            worksheet = spreadsheet.worksheet("Mutual Fund")
            records = worksheet.get_all_records()
            
            transformed = []
            for record in records[:limit]:
                transformed.append({
                    'fund_name': record.get('Fund Name', ''),
                    'nav': record.get('NAV', 0),
                    'daily_change': record.get('Daily Change', 0),
                    'ytd_return': record.get('YTD Return', 0),
                    'assets': record.get('Assets', 0),
                    'expense_ratio': record.get('Expense Ratio', 0)
                })
            
            self.logger.info(f"Read {len(transformed)} records from Mutual Fund")
            return transformed
            
        except Exception as e:
            self.logger.error(f"Error reading Mutual Fund: {e}")
            return []

    def read_my_portfolio(self, limit: int = 50) -> List[Dict[str, Any]]:
        """Read My Portfolio Investment data"""
        try:
            spreadsheet = self.sheets_manager.get_spreadsheet()
            worksheet = spreadsheet.worksheet("My Portfolio Investment")
            records = worksheet.get_all_records()
            
            transformed = []
            for record in records[:limit]:
                transformed.append({
                    'symbol': record.get('Symbol', ''),
                    'quantity': record.get('Quantity', 0),
                    'avg_cost': record.get('Average Cost', 0),
                    'current_price': record.get('Current Price', 0),
                    'market_value': record.get('Market Value', 0),
                    'gain_loss': record.get('Gain/Loss', 0),
                    'gain_loss_pct': record.get('Gain/Loss %', 0)
                })
            
            self.logger.info(f"Read {len(transformed)} records from My Portfolio")
            return transformed
            
        except Exception as e:
            self.logger.error(f"Error reading My Portfolio: {e}")
            return []

    def read_commodities_fx(self, limit: int = 20) -> List[Dict[str, Any]]:
        """Read Commodities & FX data"""
        try:
            spreadsheet = self.sheets_manager.get_spreadsheet()
            worksheet = spreadsheet.worksheet("Commodities & FX")
            records = worksheet.get_all_records()
            
            transformed = []
            for record in records[:limit]:
                transformed.append({
                    'commodity': record.get('Commodity', ''),
                    'price': record.get('Price', 0),
                    'change': record.get('Change', 0),
                    'change_percent': record.get('Change %', 0),
                    'unit': record.get('Unit', '')
                })
            
            self.logger.info(f"Read {len(transformed)} records from Commodities & FX")
            return transformed
            
        except Exception as e:
            self.logger.error(f"Error reading Commodities & FX: {e}")
            return []

    def read_economic_calendar(self, limit: int = 10) -> List[Dict[str, Any]]:
        """Read Economic Calendar data"""
        try:
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
                    'previous': record.get('Previous', '')
                })
            
            self.logger.info(f"Read {len(transformed)} records from Economic Calendar")
            return transformed
            
        except Exception as e:
            self.logger.error(f"Error reading Economic Calendar: {e}")
            return []

# =============================================================================
# Basic setup
# =============================================================================

BASE_DIR = Path(__file__).resolve().parent
load_dotenv(dotenv_path=BASE_DIR / ".env")

# Enhanced logging configuration
LOG_ENABLE_FILE = os.getenv("LOG_ENABLE_FILE", "false").lower() == "true"
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()

handlers: List[logging.Handler] = [logging.StreamHandler()]
if LOG_ENABLE_FILE:
    log_file = BASE_DIR / "app.log"
    handlers.append(logging.FileHandler(str(log_file), encoding='utf-8'))

logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.INFO),
    format="%(asctime)s - %(name)s - %(levelname)s - [%(filename)s:%(lineno)d] - %(message)s",
    handlers=handlers,
)
logger = logging.getLogger(__name__)

# =============================================================================
# Enhanced Configuration
# =============================================================================

class AppConfig(BaseSettings):
    # Service Configuration
    service_name: str = Field("Tadawul Stock Analysis API", env="SERVICE_NAME")
    service_version: str = Field("2.3.0", env="SERVICE_VERSION")
    app_host: str = Field("0.0.0.0", env="APP_HOST")
    app_port: int = Field(8000, env="APP_PORT")
    environment: str = Field("production", env="ENVIRONMENT")

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

    class Config:
        env_file = ".env"
        case_sensitive = False
        validate_assignment = True

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

# Initialize configuration with enhanced error handling
try:
    config = AppConfig()
    logger.info(f"✅ Configuration loaded for {config.service_name} v{config.service_version}")
except Exception as e:
    logger.error(f"❌ Configuration validation failed: {e}")
    raise

# =============================================================================
# Constants and Security Setup
# =============================================================================

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

FINANCIAL_APIS = {
    "alpha_vantage": {
        "api_key": config.alpha_vantage_api_key,
        "base_url": "https://www.alphavantage.co/query",
        "timeout": 15,
    },
    "finnhub": {
        "api_key": config.finnhub_api_key,
        "base_url": "https://finnhub.io/api/v1",
        "timeout": 15,
    },
    "eodhd": {
        "api_key": config.eodhd_api_key,
        "base_url": "https://eodhistoricaldata.com/api",
        "timeout": 15,
    },
    "twelvedata": {
        "api_key": config.twelvedata_api_key,
        "base_url": "https://api.twelvedata.com",
        "timeout": 15,
    },
}

GOOGLE_SERVICES = {
    "spreadsheet_id": config.spreadsheet_id,
    "apps_script_url": config.google_apps_script_url,
    "apps_script_backup_url": config.google_apps_script_backup_url,
}

# Security setup
security = HTTPBearer(auto_error=False)

# Rate limiter
limiter = Limiter(key_func=get_remote_address)

def rate_limit(rule: str):
    """Conditional rate limiting based on configuration"""
    if not config.enable_rate_limiting:
        def _decorator(fn):
            return fn
        return _decorator
    return limiter.limit(rule)

# =============================================================================
# Custom Exception Classes
# =============================================================================

class FinancialAPIError(Exception):
    """Custom exception for financial API errors"""
    pass

class GoogleSheetsError(Exception):
    """Custom exception for Google Sheets errors"""
    pass

class CacheError(Exception):
    """Custom exception for cache errors"""
    pass

class ConfigurationError(Exception):
    """Custom exception for configuration errors"""
    pass

class SecurityError(Exception):
    """Custom exception for security errors"""
    pass

# =============================================================================
# Enhanced Authentication
# =============================================================================

def verify_auth(credentials: Optional[HTTPAuthorizationCredentials] = Depends(security)):
    """
    Enhanced authentication dependency with better security and logging
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

    if token not in valid_tokens:
        logger.warning(f"Invalid token attempt: {token[:8]}...")
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid authentication token",
        )

    logger.info(f"Successful authentication with token: {token[:8]}...")
    return True

def public_endpoint():
    """Explicit public endpoint marker"""
    return True

# =============================================================================
# Enhanced Google Sheets Manager
# =============================================================================

class GoogleSheetsManager:
    """
    Enhanced Google Sheets manager with connection pooling and better error handling
    """
    _client: Optional[gspread.Client] = None
    _spreadsheet: Optional[gspread.Spreadsheet] = None
    _last_connection_test: Optional[datetime.datetime] = None
    _connection_status: Dict[str, Any] = {}

    @classmethod
    def get_client(cls) -> gspread.Client:
        """Get or create Google Sheets client with enhanced error handling"""
        if cls._client:
            return cls._client

        try:
            creds_json = config.google_service_account_json
            
            if not creds_json:
                logger.warning("Google Sheets credentials not configured - running in Apps Script mode")
                raise GoogleSheetsError("Google Sheets credentials not configured")

            # Validate JSON credentials
            try:
                creds_dict = json.loads(creds_json)
            except json.JSONDecodeError as e:
                logger.error(f"Invalid JSON in Google Sheets credentials: {e}")
                raise GoogleSheetsError(f"Invalid credentials JSON: {e}")

            # Enhanced credential validation
            required_fields = ["type", "project_id", "private_key_id", "private_key", "client_email"]
            missing_fields = [field for field in required_fields if field not in creds_dict]
            if missing_fields:
                logger.error(f"Missing required credential fields: {missing_fields}")
                raise GoogleSheetsError(f"Incomplete credentials: missing {missing_fields}")

            # Create credentials and client
            creds = Credentials.from_service_account_info(creds_dict, scopes=SCOPES)
            cls._client = gspread.authorize(creds)
            
            logger.info("✅ Google Sheets client initialized successfully")
            return cls._client

        except GoogleSheetsError:
            raise
        except Exception as e:
            logger.error(f"Failed to initialize Google Sheets client: {e}")
            raise GoogleSheetsError(f"Connection failed: {e}")

    @classmethod
    def get_spreadsheet(cls) -> gspread.Spreadsheet:
        """Get spreadsheet with enhanced error handling"""
        if cls._spreadsheet:
            return cls._spreadsheet

        client = cls.get_client()
        try:
            spreadsheet_id = config.spreadsheet_id
            cls._spreadsheet = client.open_by_key(spreadsheet_id)
            
            # Verify access and log details
            title = cls._spreadsheet.title
            worksheets = cls._spreadsheet.worksheets()
            
            logger.info(f"✅ Spreadsheet accessed: '{title}' with {len(worksheets)} worksheets")
            return cls._spreadsheet
            
        except gspread.SpreadsheetNotFound:
            logger.error(f"Spreadsheet not found: {config.spreadsheet_id}")
            raise GoogleSheetsError("Spreadsheet not found. Check SPREADSHEET_ID.")
        except gspread.APIError as e:
            logger.error(f"Google API error: {e}")
            raise GoogleSheetsError(f"Google API error: {e}")
        except Exception as e:
            logger.error(f"Unexpected error accessing spreadsheet: {e}")
            raise GoogleSheetsError(f"Spreadsheet access failed: {e}")

    @classmethod
    def test_connection(cls) -> Dict[str, Any]:
        """
        Enhanced connection test with detailed diagnostics
        """
        try:
            if not config.google_service_account_json:
                return {
                    "status": "DISABLED", 
                    "message": "Google Sheets credentials not configured",
                    "timestamp": datetime.datetime.utcnow().isoformat() + "Z"
                }

            start_time = datetime.datetime.now()
            spreadsheet = cls.get_spreadsheet()
            worksheets = spreadsheet.worksheets()
            elapsed = (datetime.datetime.now() - start_time).total_seconds()

            # Enhanced sheet analysis
            sheet_info = []
            expected_sheets_found = []
            expected_sheets_missing = []
            
            for ws in worksheets:
                try:
                    records = ws.get_all_records()
                    row_count = ws.row_count
                    col_count = ws.col_count
                    
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
                    })
                except Exception as e:
                    sheet_info.append({
                        "name": ws.title, 
                        "status": "ERROR",
                        "error": str(e)
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
                "sheets": sheet_info,
                "timestamp": datetime.datetime.utcnow().isoformat() + "Z"
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
                "timestamp": datetime.datetime.utcnow().isoformat() + "Z"
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

# Initialize Google Sheets Service
google_sheets_service = GoogleSheetsService(GoogleSheetsManager)

# =============================================================================
# Enhanced Financial Data Client
# =============================================================================

class FinancialDataClient:
    """
    Enhanced financial data client with better error handling and retry logic
    """
    
    def __init__(self):
        self.apis = FINANCIAL_APIS
        self.session: Optional[aiohttp.ClientSession] = None
        self.logger = logging.getLogger(__name__)

    async def get_session(self) -> aiohttp.ClientSession:
        """Get or create aiohttp session with connection pooling"""
        if self.session is None:
            timeout = aiohttp.ClientTimeout(total=30)
            connector = aiohttp.TCPConnector(limit=100, limit_per_host=20, verify_ssl=False)
            self.session = aiohttp.ClientSession(
                timeout=timeout, 
                connector=connector,
                headers={'User-Agent': f'{config.service_name}/{config.service_version}'}
            )
        return self.session

    async def close(self):
        """Close session gracefully"""
        if self.session:
            await self.session.close()
            self.session = None

    def get_api_status(self) -> Dict[str, bool]:
        """Get status of all configured APIs"""
        return {name: bool(cfg.get("api_key")) for name, cfg in self.apis.items()}

    async def get_stock_quote_with_retry(
        self, 
        symbol: str, 
        api_name: str = "alpha_vantage", 
        max_retries: int = 3,
        retry_delay: float = 1.0
    ) -> Optional[Dict[str, Any]]:
        """
        Enhanced retry logic with exponential backoff
        """
        for attempt in range(max_retries):
            try:
                self.logger.debug(f"Fetching {symbol} from {api_name} (attempt {attempt + 1})")
                result = await self.get_stock_quote(symbol, api_name)
                
                if result:
                    self.logger.info(f"✅ Successfully fetched {symbol} from {api_name}")
                    return result
                
                self.logger.warning(f"⚠️ No data returned for {symbol} from {api_name}")
                
            except Exception as e:
                self.logger.warning(f"⚠️ Attempt {attempt + 1} failed for {api_name}: {e}")
                
                if attempt < max_retries - 1:
                    wait_time = retry_delay * (2 ** attempt)  # Exponential backoff
                    self.logger.debug(f"Retrying in {wait_time:.2f}s...")
                    await asyncio.sleep(wait_time)
                else:
                    self.logger.error(f"❌ All retries failed for {symbol} from {api_name}")
        
        return None

    async def get_stock_quote(self, symbol: str, api_name: str = "alpha_vantage") -> Optional[Dict[str, Any]]:
        """Get stock quote from specified API with enhanced error handling"""
        if api_name not in self.apis:
            self.logger.error(f"API {api_name} not configured")
            return None

        api_cfg = self.apis[api_name]
        if not api_cfg.get("api_key"):
            self.logger.warning(f"API key not configured for {api_name}")
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
                    if response.status == 200:
                        data = await response.json()
                        return self._parse_alpha_vantage_quote(data)
                    else:
                        self.logger.error(f"Alpha Vantage HTTP {response.status}: {await response.text()}")

            elif api_name == "finnhub":
                params = {"symbol": symbol, "token": api_cfg["api_key"]}
                async with session.get(f"{api_cfg['base_url']}/quote", params=params, timeout=timeout) as response:
                    if response.status == 200:
                        data = await response.json()
                        return self._parse_finnhub_quote(data)
                    else:
                        self.logger.error(f"Finnhub HTTP {response.status}: {await response.text()}")

            elif api_name == "twelvedata":
                params = {
                    "symbol": symbol, 
                    "apikey": api_cfg["api_key"],
                    "source": "docs"  # Use documentation source for reliability
                }
                async with session.get(f"{api_cfg['base_url']}/quote", params=params, timeout=timeout) as response:
                    if response.status == 200:
                        data = await response.json()
                        return self._parse_twelvedata_quote(data)
                    else:
                        self.logger.error(f"TwelveData HTTP {response.status}: {await response.text()}")

        except asyncio.TimeoutError:
            self.logger.error(f"Timeout fetching data from {api_name} for {symbol}")
        except aiohttp.ClientError as e:
            self.logger.error(f"Network error fetching from {api_name}: {e}")
        except Exception as e:
            self.logger.error(f"Unexpected error from {api_name}: {e}")

        return None

    def _parse_alpha_vantage_quote(self, data: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Parse Alpha Vantage response with enhanced validation"""
        try:
            quote_data = data.get("Global Quote", {})
            if not quote_data:
                self.logger.debug("Alpha Vantage returned empty Global Quote")
                return None
                
            return {
                "price": float(quote_data.get("05. price", 0) or 0),
                "change": float(quote_data.get("09. change", 0) or 0),
                "change_percent": float(str(quote_data.get("10. change percent", "0")).rstrip("%") or 0),
                "volume": int(quote_data.get("06. volume", 0) or 0),
                "timestamp": quote_data.get("07. latest trading day"),
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
                self.logger.debug(f"TwelveData returned error status: {data.get('status')}")
                return None
                
            return {
                "price": float(data.get("close", 0) or 0),
                "change": float(data.get("change", 0) or 0),
                "change_percent": float(data.get("percent_change", 0) or 0),
                "high": float(data.get("high", 0) or 0),
                "low": float(data.get("low", 0) or 0),
                "open": float(data.get("open", 0) or 0),
                "volume": int(data.get("volume", 0) or 0),
                "timestamp": data.get("datetime"),
                "data_source": "twelvedata",
            }
        except (ValueError, TypeError) as e:
            self.logger.error(f"Error parsing TwelveData: {e}")
            return None

# Initialize financial client
financial_client = FinancialDataClient()

# =============================================================================
# Enhanced Pydantic Models
# =============================================================================

class Quote(BaseModel):
    """Enhanced quote model with better validation"""
    ticker: str = Field(..., description="Stock ticker symbol", example="7201.SR")
    company: Optional[str] = Field(None, description="Company name")
    currency: Optional[str] = Field(None, description="Currency code", example="SAR")
    price: Optional[float] = Field(None, ge=0, description="Current price")
    previous_close: Optional[float] = Field(None, ge=0, description="Previous close price")
    day_change_pct: Optional[float] = Field(None, description="Daily change percentage")
    market_cap: Optional[float] = Field(None, ge=0, description="Market capitalization")
    volume: Optional[float] = Field(None, ge=0, description="Trading volume")
    fifty_two_week_high: Optional[float] = Field(None, ge=0)
    fifty_two_week_low: Optional[float] = Field(None, ge=0)
    timestamp_utc: Optional[str] = Field(None, description="UTC timestamp")
    data_source: Optional[str] = Field(None, description="Data source identifier")

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

class QuoteUpdatePayload(BaseModel):
    """Payload for updating quotes"""
    data: List[Quote] = Field(..., description="List of quotes to update")

    @validator("data")
    def validate_data(cls, v: List[Quote]) -> List[Quote]:
        if not v:
            raise ValueError("Empty data list")
        if len(v) > 1000:
            raise ValueError("Too many quotes in single update")
        return v

class QuoteResponse(BaseModel):
    """Standardized quote response"""
    data: List[Quote] = Field(..., description="List of quotes")
    count: int = Field(..., description="Number of quotes returned")
    timestamp_utc: str = Field(..., description="Response timestamp")

class SheetVerificationResult(BaseModel):
    """Sheet verification result"""
    sheet_name: str = Field(..., description="Sheet name")
    status: str = Field(..., description="Verification status")
    row_count: int = Field(..., description="Number of rows")
    column_count: int = Field(..., description="Number of columns")
    headers: List[str] = Field(..., description="Column headers")
    sample_data: List[Dict[str, Any]] = Field(..., description="Sample data rows")
    error: Optional[str] = Field(None, description="Error message if any")

class VerificationResponse(BaseModel):
    """Sheet verification response"""
    spreadsheet_id: str = Field(..., description="Spreadsheet ID")
    overall_status: str = Field(..., description="Overall verification status")
    sheets_verified: int = Field(..., description="Number of sheets verified")
    sheets_ok: int = Field(..., description="Number of sheets with OK status")
    details: List[SheetVerificationResult] = Field(..., description="Detailed results")

class SheetDataResponse(BaseModel):
    """Sheet data response"""
    sheet_name: str = Field(..., description="Sheet name")
    total_records: int = Field(..., description="Total records in sheet")
    data: List[Dict[str, Any]] = Field(..., description="Sheet data records")

class HealthResponse(BaseModel):
    """Health check response"""
    status: str = Field(..., description="Overall health status")
    time_utc: str = Field(..., description="Current UTC time")
    version: str = Field(..., description="API version")
    google_sheets_connected: bool = Field(..., description="Google Sheets connection status")
    cache_status: Dict[str, Any] = Field(..., description="Cache status information")
    features: Dict[str, bool] = Field(..., description="Available features")
    api_status: Dict[str, bool] = Field(..., description="Financial API status")

class ErrorResponse(BaseModel):
    """Standardized error response"""
    error: str = Field(..., description="Error type")
    detail: Optional[str] = Field(None, description="Detailed error message")
    timestamp: str = Field(..., description="Error timestamp")
    request_id: Optional[str] = Field(None, description="Request identifier for debugging")

# =============================================================================
# Enhanced TTL Cache
# =============================================================================

class TTLCache:
    """
    Enhanced TTL cache with better persistence and backup capabilities
    """
    
    def __init__(self, ttl_minutes: int = 30, max_size: int = 10000):
        self.cache_path = BASE_DIR / "quote_cache.json"
        self.backup_dir = BASE_DIR / "cache_backups"
        self.backup_dir.mkdir(parents=True, exist_ok=True)
        self.data: Dict[str, Dict[str, Any]] = {}
        self.ttl = ttl_minutes * 60
        self.max_size = max_size
        self.logger = logging.getLogger(__name__)
        self.load_cache()

    def _is_valid(self, item: Dict[str, Any], now: Optional[float] = None) -> bool:
        """Check if cache item is still valid"""
        if now is None:
            now = datetime.datetime.now().timestamp()
        timestamp = item.get("_cache_timestamp", 0)
        return (now - timestamp) < self.ttl

    def load_cache(self) -> bool:
        """Load cache from disk with enhanced error handling"""
        try:
            if not self.cache_path.exists():
                self.logger.info("No cache file found, starting with empty cache")
                return True

            with open(self.cache_path, "r", encoding="utf-8") as f:
                raw_data = json.load(f)

            now = datetime.datetime.now().timestamp()
            valid_items: Dict[str, Dict[str, Any]] = {}

            # Handle different cache file formats
            if isinstance(raw_data, dict) and "data" in raw_data:
                # New format: {"data": [{...}, {...}]}
                for item in raw_data["data"]:
                    if isinstance(item, dict) and "ticker" in item and self._is_valid(item, now):
                        valid_items[item["ticker"]] = item
            elif isinstance(raw_data, dict):
                # Old format: {"TICKER": {...}}
                for ticker, item in raw_data.items():
                    if self._is_valid(item, now):
                        valid_items[ticker] = item

            self.data = valid_items
            self.logger.info(f"✅ Cache loaded with {len(self.data)} valid items")
            return True
            
        except json.JSONDecodeError as e:
            self.logger.error(f"❌ Cache file corrupted: {e}")
            # Try to backup corrupted file
            self._backup_corrupted_cache()
            return False
        except Exception as e:
            self.logger.error(f"❌ Failed to load cache: {e}")
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
        """Save cache to disk with atomic write"""
        try:
            # Enforce size limit
            if len(self.data) > self.max_size:
                self._enforce_size_limit()
                
            now = datetime.datetime.now().timestamp()
            cache_data = {
                "metadata": {
                    "version": "1.1",
                    "saved_at": datetime.datetime.utcnow().isoformat() + "Z",
                    "item_count": len(self.data),
                    "ttl_minutes": self.ttl // 60
                },
                "data": [
                    {**item, "_cache_timestamp": item.get("_cache_timestamp", now)}
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

    def create_backup(self) -> Optional[str]:
        """Create cache backup with timestamp"""
        try:
            if not self.data:
                return None
                
            timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
            backup_path = self.backup_dir / f"quote_cache_backup_{timestamp}.json"
            
            backup_data = {
                "metadata": {
                    "backup_created": datetime.datetime.utcnow().isoformat() + "Z",
                    "original_item_count": len(self.data)
                },
                "data": list(self.data.values())
            }
            
            with open(backup_path, "w", encoding="utf-8") as f:
                json.dump(backup_data, f, indent=2, ensure_ascii=False, default=str)
                
            self.logger.info(f"📦 Cache backup created: {backup_path}")
            return str(backup_path)
            
        except Exception as e:
            self.logger.error(f"❌ Backup creation failed: {e}")
            return None

    def get_quote(self, ticker: str) -> Optional[Quote]:
        """Get quote from cache with validation"""
        ticker = ticker.upper().strip()
        
        if ticker in self.data and self._is_valid(self.data[ticker]):
            # Remove cache metadata before returning
            clean_data = {k: v for k, v in self.data[ticker].items() 
                         if not k.startswith("_")}
            try:
                return Quote(**clean_data)
            except Exception as e:
                self.logger.error(f"Invalid cache data for {ticker}: {e}")
                # Remove invalid entry
                del self.data[ticker]
                self.save_cache()
                return None
        
        # Remove expired entry
        if ticker in self.data:
            del self.data[ticker]
            self.save_cache()
            
        return None

    def update_quotes(self, quotes: List[Quote]) -> Tuple[int, List[str]]:
        """Update multiple quotes in cache"""
        updated_count = 0
        errors: List[str] = []
        now = datetime.datetime.now().timestamp()

        for quote in quotes:
            try:
                quote_data = quote.dict()
                quote_data["_cache_timestamp"] = now
                quote_data["_last_updated"] = datetime.datetime.utcnow().isoformat() + "Z"
                
                self.data[quote.ticker] = quote_data
                updated_count += 1
                
            except Exception as e:
                errors.append(f"Failed to update {quote.ticker}: {e}")

        if updated_count > 0:
            self.save_cache()
            self.logger.info(f"Updated {updated_count} quotes in cache")
            
        return updated_count, errors

    def cleanup_expired(self) -> int:
        """Clean up expired cache entries"""
        now = datetime.datetime.now().timestamp()
        expired_tickers = [
            ticker for ticker, item in self.data.items() 
            if not self._is_valid(item, now)
        ]
        
        for ticker in expired_tickers:
            del self.data[ticker]
            
        if expired_tickers:
            self.save_cache()
            self.logger.info(f"🧹 Removed {len(expired_tickers)} expired cache entries")
            
        return len(expired_tickers)

    def get_stats(self) -> Dict[str, Any]:
        """Get cache statistics"""
        now = datetime.datetime.now().timestamp()
        valid_count = sum(1 for item in self.data.values() if self._is_valid(item, now))
        expired_count = len(self.data) - valid_count
        
        return {
            "total_items": len(self.data),
            "valid_items": valid_count,
            "expired_items": expired_count,
            "max_size": self.max_size,
            "ttl_minutes": self.ttl // 60,
            "cache_file": str(self.cache_path),
            "file_exists": self.cache_path.exists(),
            "file_size": self.cache_path.stat().st_size if self.cache_path.exists() else 0,
        }

# Initialize cache
ttl_seconds = int(os.getenv("CACHE_DEFAULT_TTL", "1800"))  # 30 minutes default
cache = TTLCache(ttl_minutes=max(1, ttl_seconds // 60))

# =============================================================================
# Enhanced Startup Validation
# =============================================================================

async def validate_configuration():
    """
    Enhanced configuration validation with detailed diagnostics
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
    configured_apis = [name for name, configured in api_status.items() if configured]
    
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

# =============================================================================
# FastAPI Application Setup
# =============================================================================

@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    Enhanced application lifespan with better resource management
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

    logger.info(f"🛑 Application shutdown after {uptime}")

# Create FastAPI application
app = FastAPI(
    title=config.service_name,
    version=config.service_version,
    description="Enhanced Tadawul Stock Analysis API with comprehensive financial data 📈",
    docs_url="/docs" if config.enable_swagger else None,
    redoc_url="/redoc" if config.enable_redoc else None,
    lifespan=lifespan,
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
    """Enhanced HTTP exception handler with request ID"""
    request_id = request.headers.get("X-Request-ID", "unknown")
    
    logger.warning(
        f"HTTP {exc.status_code} for {request.url.path} "
        f"[{request_id}]: {exc.detail}"
    )
    
    return JSONResponse(
        status_code=exc.status_code,
        content=ErrorResponse(
            error=exc.detail,
            timestamp=datetime.datetime.utcnow().isoformat() + "Z",
            request_id=request_id,
        ).dict(),
    )

@app.exception_handler(Exception)
async def global_exception_handler(request: Request, exc: Exception):
    """Enhanced global exception handler"""
    request_id = request.headers.get("X-Request-ID", "unknown")
    
    logger.error(
        f"Unhandled exception for {request.url.path} [{request_id}]: {exc}",
        exc_info=True
    )
    
    return JSONResponse(
        status_code=500,
        content=ErrorResponse(
            error="Internal server error",
            detail=str(exc) if config.environment != "production" else None,
            timestamp=datetime.datetime.utcnow().isoformat() + "Z",
            request_id=request_id,
        ).dict(),
    )

# =============================================================================
# Core API Endpoints
# =============================================================================

@app.get("/", response_model=Dict[str, Any])
@rate_limit(f"{config.max_requests_per_minute}/minute")
async def root(request: Request):
    """
    Enhanced root endpoint with comprehensive system status
    """
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
        except Exception:
            pass

    # Get API status
    api_status = financial_client.get_api_status()
    
    # Get cache statistics
    cache_stats = cache.get_stats()

    return {
        "service": config.service_name,
        "version": config.service_version,
        "status": "operational",
        "timestamp": datetime.datetime.utcnow().isoformat() + "Z",
        "environment": config.environment,
        "authentication_required": config.require_auth,
        "rate_limiting": config.enable_rate_limiting,
        "google_sheets": {
            "connected": sheets_connected,
            "mode": "direct_gspread" if has_sheets_creds else "apps_script_only",
            "spreadsheet": spreadsheet_title,
            "total_sheets": total_sheets,
        },
        "financial_apis": {
            name: "configured" if configured else "not_configured" 
            for name, configured in api_status.items()
        },
        "cache": {
            "total_items": cache_stats["total_items"],
            "valid_items": cache_stats["valid_items"],
            "expired_items": cache_stats["expired_items"],
            "file_exists": cache_stats["file_exists"],
            "ttl_minutes": cache_stats["ttl_minutes"],
        },
        "endpoints": {
            "health": "/health",
            "docs": "/docs" if config.enable_swagger else None,
            "quotes": "/v1/quote",
            "sheets": "/sheet-names",
        }
    }

@app.get("/health", response_model=HealthResponse)
@rate_limit("30/minute")
async def health_check(request: Request):
    """
    Enhanced health check endpoint with detailed system status
    """
    # Check Google Sheets connection
    has_sheets_creds = bool(config.google_service_account_json)
    google_connected = False
    
    if has_sheets_creds:
        try:
            sheets_status = GoogleSheetsManager.test_connection()
            google_connected = sheets_status.get("status") == "SUCCESS"
            health_status = "healthy" if google_connected else "degraded"
        except Exception:
            google_connected = False
            health_status = "degraded"
    else:
        health_status = "healthy"  # Apps Script mode is considered healthy

    # Get API status
    api_status = financial_client.get_api_status()
    
    # Get cache statistics
    cache_stats = cache.get_stats()

    # Check feature availability
    features = {
        "dashboard": False,  # Placeholder for dashboard feature
        "argaam_routes": False,  # Placeholder for Argaam routes
        "google_apps_script": bool(config.google_apps_script_url),
        "direct_sheets_access": has_sheets_creds,
    }

    return HealthResponse(
        status=health_status,
        time_utc=datetime.datetime.utcnow().isoformat() + "Z",
        version=config.service_version,
        google_sheets_connected=google_connected,
        cache_status={
            "total_items": cache_stats["total_items"],
            "valid_items": cache_stats["valid_items"],
            "file_exists": cache_stats["file_exists"],
            "file_size_bytes": cache_stats["file_size"],
            "ttl_minutes": cache_stats["ttl_minutes"],
        },
        features=features,
        api_status=api_status,
    )

@app.get("/v1/ping", response_model=Dict[str, Any])
@rate_limit("120/minute")
async def ping(request: Request):
    """
    Simple ping endpoint for connectivity testing
    """
    return {
        "status": "ok",
        "service": config.service_name,
        "version": config.service_version,
        "timestamp": datetime.datetime.utcnow().isoformat() + "Z",
        "environment": config.environment,
    }

# =============================================================================
# Protected API Endpoints (Require Authentication)
# =============================================================================

@app.get("/verify-sheets", response_model=VerificationResponse)
@rate_limit("10/minute")
async def verify_all_sheets(request: Request, auth: bool = Depends(verify_auth)):
    """
    Verify all expected sheets in the spreadsheet
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
                
                results.append(SheetVerificationResult(
                    sheet_name=sheet_name,
                    status="OK",
                    row_count=worksheet.row_count,
                    column_count=worksheet.col_count,
                    headers=headers,
                    sample_data=records[:3],  # First 3 records as sample
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
                ))
        
        # Calculate overall status
        sheets_ok = sum(1 for r in results if r.status == "OK")
        overall_status = "SUCCESS" if sheets_ok == len(EXPECTED_SHEETS) else "PARTIAL_SUCCESS"
        
        return VerificationResponse(
            spreadsheet_id=config.spreadsheet_id,
            overall_status=overall_status,
            sheets_verified=len(EXPECTED_SHEETS),
            sheets_ok=sheets_ok,
            details=results,
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
    auth: bool = Depends(verify_auth),
):
    """
    Read data from a specific sheet
    """
    if not config.google_service_account_json:
        raise HTTPException(
            status_code=400,
            detail="Google Sheets credentials not configured",
        )

    try:
        spreadsheet = GoogleSheetsManager.get_spreadsheet()
        worksheet = spreadsheet.worksheet(sheet_name)
        records = worksheet.get_all_records()
        
        return SheetDataResponse(
            sheet_name=sheet_name, 
            total_records=len(records), 
            data=records[:limit]
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
    Get all available sheet names in the spreadsheet
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
        
        return {
            "spreadsheet_title": spreadsheet.title,
            "available_sheets": available_sheets,
            "expected_sheets": EXPECTED_SHEETS,
            "missing_sheets": list(set(EXPECTED_SHEETS) - set(available_sheets)),
            "extra_sheets": list(set(available_sheets) - set(EXPECTED_SHEETS)),
            "total_sheets": len(available_sheets),
        }
        
    except Exception as e:
        logger.error(f"Error getting sheet names: {e}")
        raise HTTPException(status_code=500, detail=f"Error getting sheet names: {e}")

@app.get("/test-connection", response_model=Dict[str, Any])
@rate_limit("10/minute")
async def test_connection(request: Request, auth: bool = Depends(verify_auth)):
    """
    Test Google Sheets connection
    """
    if not config.google_service_account_json:
        return {
            "status": "DISABLED",
            "message": "Google Sheets credentials not configured",
            "timestamp": datetime.datetime.utcnow().isoformat() + "Z",
        }
        
    return GoogleSheetsManager.test_connection()

# =============================================================================
# Quote Management Endpoints
# =============================================================================

@app.get("/v1/quote", response_model=QuoteResponse)
@rate_limit("60/minute")
async def get_quotes(
    request: Request,
    tickers: str = Query(..., description="Comma-separated ticker symbols"),
    auth: bool = Depends(verify_auth),
):
    """
    Get quotes for multiple tickers from cache
    """
    symbols = [t.strip().upper() for t in tickers.split(",") if t.strip()]
    
    if not symbols:
        raise HTTPException(status_code=400, detail="No tickers provided")
        
    if len(symbols) > 100:
        raise HTTPException(status_code=400, detail="Too many tickers (max 100)")

    quotes: List[Quote] = []
    cache_hits = 0
    
    for symbol in symbols:
        quote = cache.get_quote(symbol)
        if quote:
            quotes.append(quote)
            cache_hits += 1
        else:
            # Return basic quote structure for cache misses
            quotes.append(Quote(
                ticker=symbol,
                timestamp_utc=datetime.datetime.utcnow().isoformat() + "Z"
            ))

    logger.info(f"Quote request: {len(symbols)} symbols, {cache_hits} cache hits")
    
    return QuoteResponse(
        data=quotes,
        count=len(quotes),
        timestamp_utc=datetime.datetime.utcnow().isoformat() + "Z"
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
    Update quotes in cache
    """
    updated_count, errors = cache.update_quotes(payload.data)
    
    logger.info(
        f"Quote update: {updated_count} successful, "
        f"{len(errors)} errors out of {len(payload.data)} quotes"
    )

    return {
        "status": "completed",
        "updated_count": updated_count,
        "error_count": len(errors),
        "errors": errors if errors else None,
        "total_cached": len(cache.data),
        "timestamp": datetime.datetime.utcnow().isoformat() + "Z",
    }

# =============================================================================
# Cache Management Endpoints
# =============================================================================

@app.get("/v1/cache", response_model=Dict[str, Any])
@rate_limit("20/minute")
async def get_cache_info(
    request: Request,
    limit: int = Query(50, ge=1, le=1000),
    auth: bool = Depends(verify_auth),
):
    """
    Get cache information and sample data
    """
    cache_stats = cache.get_stats()
    sample_data = list(cache.data.values())[:limit]
    
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
    }

@app.post("/v1/cache/backup", response_model=Dict[str, Any])
@rate_limit("5/minute")
async def create_cache_backup(request: Request, auth: bool = Depends(verify_auth)):
    """
    Create a backup of the cache
    """
    backup_path = cache.create_backup()
    if not backup_path:
        raise HTTPException(status_code=500, detail="Failed to create backup")
        
    return {
        "status": "success", 
        "backup_path": backup_path, 
        "item_count": len(cache.data),
        "timestamp": datetime.datetime.utcnow().isoformat() + "Z",
    }

@app.post("/v1/cache/cleanup", response_model=Dict[str, Any])
@rate_limit("5/minute")
async def cleanup_cache(request: Request, auth: bool = Depends(verify_auth)):
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

@app.delete("/v1/cache/clear", response_model=Dict[str, Any])
@rate_limit("2/minute")
async def clear_cache(request: Request, auth: bool = Depends(verify_auth)):
    """
    Clear entire cache (use with caution)
    """
    item_count = len(cache.data)
    cache.data.clear()
    
    if cache.save_cache():
        logger.warning(f"Cache cleared by user: {item_count} items removed")
        return {
            "status": "success",
            "items_removed": item_count,
            "timestamp": datetime.datetime.utcnow().isoformat() + "Z",
        }
    else:
        raise HTTPException(status_code=500, detail="Failed to clear cache")

# =============================================================================
# v4.1 Compatibility Endpoints
# =============================================================================

@app.get("/v41/quotes")
@rate_limit("60/minute")
async def v41_get_quotes(
    request: Request,
    symbols: str = Query(..., description="Comma-separated tickers"),
    cache_ttl: int = Query(60, ge=0),
    auth: bool = Depends(verify_auth),
):
    """
    v4.1 compatible quotes endpoint
    """
    tickers = [t.strip().upper() for t in symbols.split(",") if t.strip()]
    
    if not tickers:
        raise HTTPException(status_code=400, detail="No symbols provided")

    data: List[Dict[str, Any]] = []
    now_ts = datetime.datetime.utcnow().isoformat() + "Z"
    cache_hits = 0

    for symbol in tickers:
        quote = cache.get_quote(symbol)
        if quote:
            cache_hits += 1
            data.append({
                "ticker": quote.ticker,
                "price": quote.price,
                "previous_close": quote.previous_close,
                "day_change_pct": quote.day_change_pct,
                "currency": quote.currency,
                "timestamp_utc": quote.timestamp_utc,
                "data_source": quote.data_source,
            })
        else:
            data.append({
                "ticker": symbol,
                "price": None,
                "previous_close": None,
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
    v4.1 compatible quote update endpoint
    """
    updated_count, errors = cache.update_quotes(payload.data)
    now_ts = datetime.datetime.utcnow().isoformat() + "Z"
    
    return {
        "ok": True,
        "updated": updated_count,
        "errors": errors or [],
        "total_cached": len(cache.data),
        "timestamp_utc": now_ts,
    }

# =============================================================================
# Market Data Endpoints
# =============================================================================

@app.get("/api/saudi/symbols", response_model=Dict[str, Any])
@rate_limit("30/minute")
async def get_saudi_symbols(
    request: Request,
    limit: int = Query(20, ge=1, le=500),
    auth: bool = Depends(verify_auth),
):
    """
    Get Saudi market symbols from Google Sheets
    """
    try:
        # Try to read from KSA Tadawul Market sheet
        ksa_data = google_sheets_service.read_ksa_tadawul_market(limit)
        
        symbols = []
        for item in ksa_data:
            symbols.append({
                "symbol": item.get("ticker", ""),
                "company_name": item.get("company_name", ""),
                "sector": item.get("sector", ""),
                "trading_market": item.get("trading_market", ""),
                "current_price": item.get("current_price"),
                "daily_change": item.get("daily_change"),
            })

        return {
            "data": symbols,
            "count": len(symbols),
            "source": "google_sheets_ksa_tadawul",
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
    auth: bool = Depends(verify_auth),
):
    """
    Get Saudi market data with quotes
    """
    try:
        # Get symbols first
        symbols_data = await get_saudi_symbols(request, limit=limit, auth=auth)
        symbols = symbols_data.get("data", [])
        
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
                        "price": quote.price,
                        "change_percent": quote.day_change_pct,
                        "volume": quote.volume,
                        "market_cap": quote.market_cap,
                        "last_updated": quote.timestamp_utc,
                        "data_source": quote.data_source,
                    })
                else:
                    # Use data from sheet if available
                    symbol_info.update({
                        "price": symbol_info.get("current_price"),
                        "change_percent": symbol_info.get("daily_change"),
                        "last_updated": datetime.datetime.utcnow().isoformat() + "Z",
                        "data_source": "google_sheets",
                    })
                    
            market_data.append(symbol_info)

        return {
            "count": len(market_data),
            "data": market_data,
            "source": symbols_data.get("source", "unknown"),
            "cache_hits": cache_hits,
            "cache_misses": len(market_data) - cache_hits,
            "timestamp": datetime.datetime.utcnow().isoformat() + "Z",
        }
        
    except Exception as e:
        logger.error(f"Failed to fetch Saudi market data: {e}")
        raise HTTPException(status_code=500, detail=f"Market data fetch failed: {e}")

# =============================================================================
# Financial APIs Status & Data Endpoints
# =============================================================================

@app.get("/v1/financial-apis/status", response_model=Dict[str, Any])
@rate_limit("15/minute")
async def get_financial_apis_status(request: Request, auth: bool = Depends(verify_auth)):
    """
    Get status of all configured financial APIs
    """
    api_status = financial_client.get_api_status()
    test_results: List[Dict[str, Any]] = []
    
    # Test each configured API
    for api_name, configured in api_status.items():
        if not configured:
            continue
            
        try:
            start_time = datetime.datetime.now()
            test_symbol = "AAPL"  # Standard test symbol
            
            data = await financial_client.get_stock_quote_with_retry(
                test_symbol, api_name, max_retries=1
            )
            
            elapsed = (datetime.datetime.now() - start_time).total_seconds()
            
            if data:
                test_results.append({
                    "api_name": api_name,
                    "status": "SUCCESS",
                    "response_time_seconds": round(elapsed, 3),
                    "test_symbol": test_symbol,
                })
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

    return {
        "api_configuration": api_status,
        "api_tests": test_results,
        "timestamp": datetime.datetime.utcnow().isoformat() + "Z",
    }

@app.get("/v1/financial-data/{symbol}")
@rate_limit("30/minute")
async def get_financial_data(
    request: Request,
    symbol: str,
    api: str = Query("alpha_vantage"),
    auth: bool = Depends(verify_auth),
):
    """
    Get financial data for a symbol from specified API
    """
    if api not in FINANCIAL_APIS:
        raise HTTPException(
            status_code=400, 
            detail=f"API {api} not supported. Available: {list(FINANCIAL_APIS.keys())}"
        )
        
    data = await financial_client.get_stock_quote_with_retry(symbol, api)
    
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
    }

# =============================================================================
# Optional Routes Mounting
# =============================================================================

# Mount Argaam routes if available
try:
    from routes_argaam import router as argaam_router
    app.include_router(argaam_router)
    logger.info("✅ Argaam routes mounted successfully")
except ImportError:
    logger.info("ℹ️ Argaam routes not available")

# Mount dashboard routes if available
try:
    from advanced_market_dashboard import dashboard_router
    app.include_router(dashboard_router)
    logger.info("✅ Market dashboard routes mounted successfully")
except ImportError:
    logger.info("ℹ️ Market dashboard routes not available")

# =============================================================================
# Application Entry Point
# =============================================================================

if __name__ == "__main__":
    # Server configuration
    server_config = {
        "app": "main:app",
        "host": config.app_host,
        "port": config.app_port,
        "log_level": config.log_level.lower(),
        "access_log": True,
        "reload": config.environment == "development",
    }

    # Startup banner
    logger.info("=" * 60)
    logger.info(f"🚀 {config.service_name} v{config.service_version}")
    logger.info(f"🌍 Environment: {config.environment}")
    logger.info(f"🔐 Authentication: {'Enabled' if config.require_auth else 'Disabled'}")
    logger.info(f"⚡ Rate Limiting: {'Enabled' if config.enable_rate_limiting else 'Disabled'}")
    logger.info(f"📊 Google Sheets: {'Direct' if config.google_service_account_json else 'Apps Script'}")
    logger.info(f"💾 Cache: {len(cache.data)} items loaded")
    logger.info("=" * 60)

    # Start server
    uvicorn.run(**server_config)
