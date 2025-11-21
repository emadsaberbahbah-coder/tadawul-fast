from __future__ import annotations

import datetime
import json
import os
import socket
import logging
import asyncio
import aiohttp
from contextlib import closing, asynccontextmanager
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
import gspread
import pandas as pd
import numpy as np
from google.oauth2.service_account import Credentials
from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException, Query, status, BackgroundTasks, Depends
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from pydantic import BaseModel, Field, validator
import uvicorn
import httpx

# =============================================================================
# Enhanced Configuration & Logging
# =============================================================================

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('app.log') if os.getenv('LOG_ENABLE_FILE', 'true').lower() == 'true' else logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Load environment variables
BASE_DIR = Path(__file__).resolve().parent
load_dotenv(dotenv_path=BASE_DIR / ".env")

# =============================================================================
# Configuration Constants
# =============================================================================

SERVICE_NAME = os.getenv("SERVICE_NAME", "Tadawul Stock Analysis API")
SERVICE_VERSION = os.getenv("SERVICE_VERSION", "2.1.0")
APP_HOST = os.getenv("APP_HOST", "0.0.0.0")
APP_PORT = int(os.getenv("APP_PORT", "8000"))
APP_TOKEN = os.getenv("APP_TOKEN", "c0487616ceaeaa47122cabb7b3e66a09")
BACKUP_APP_TOKEN = os.getenv("BACKUP_APP_TOKEN", "441669eb92442fdde6dcae35c248f37f")
REQUIRE_AUTH = os.getenv("REQUIRE_AUTH", "false").lower() == "true"

# Google Sheets Configuration
SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive"
]

EXPECTED_SHEETS = [
    "Companies", "Financials", "Prices", "Indicators", 
    "Sectors", "Analysis", "Reports", "Users", "Settings"
]

# Financial API Configuration
FINANCIAL_APIS = {
    "alpha_vantage": {
        "api_key": os.getenv("ALPHA_VANTAGE_API_KEY", "Q0VIE9J6AXUCG99F"),
        "base_url": os.getenv("ALPHA_VANTAGE_BASE_URL", "https://www.alphavantage.co/query"),
        "timeout": int(os.getenv("ALPHA_VANTAGE_TIMEOUT", "15"))
    },
    "finnhub": {
        "api_key": os.getenv("FINNHUB_API_KEY", "d3uhd8pr01qil4aq3i5gd3uhd8pr01qil4aq3i60"),
        "base_url": os.getenv("FINNHUB_BASE_URL", "https://finnhub.io/api/v1"),
        "timeout": int(os.getenv("FINNHUB_TIMEOUT", "15"))
    },
    "eodhd": {
        "api_key": os.getenv("EODHD_API_KEY", "68fd1783ee7eb1.12039806"),
        "base_url": os.getenv("EODHD_BASE_URL", "https://eodhistoricaldata.com/api"),
        "timeout": int(os.getenv("EODHD_TIMEOUT", "15"))
    },
    "twelvedata": {
        "api_key": os.getenv("TWELVEDATA_API_KEY", "ca363b090fbb421a84c05882e4f1e393"),
        "base_url": os.getenv("TWELVEDATA_BASE_URL", "https://api.twelvedata.com"),
        "timeout": int(os.getenv("TWELVEDATA_TIMEOUT", "15"))
    },
    "marketstack": {
        "api_key": os.getenv("MARKETSTACK_API_KEY", "657b972a96392c3cac405ccc48c36b0c"),
        "base_url": os.getenv("MARKETSTACK_BASE_URL", "http://api.marketstack.com/v1"),
        "timeout": int(os.getenv("MARKETSTACK_TIMEOUT", "15"))
    },
    "fmp": {
        "api_key": os.getenv("FMP_API_KEY", "3weEgekBXByxCzDGIbXgQ0hgWGZfVKyt"),
        "base_url": os.getenv("FMP_BASE_URL", "https://financialmodelingprep.com/api/v3"),
        "timeout": int(os.getenv("FMP_TIMEOUT", "15"))
    }
}

# Google Services Configuration
GOOGLE_SERVICES = {
    "spreadsheet_id": os.getenv("SPREADSHEET_ID", "19oloY3fehdFnSRMysqd-EZ2l7FL-GRAd8GJhYUt8tmw"),
    "apps_script_url": os.getenv("GOOGLE_APPS_SCRIPT_URL", "https://script.google.com/macros/s/AKfycbwnIX0hIaffDJVnHZUxej4zoLPQZgpdMMpkA9YP1xPQVxqwvEAXuIHWcF7qBIVsntnLkg/exec"),
    "apps_script_backup_url": os.getenv("GOOGLE_APPS_SCRIPT_BACKUP_URL", "https://script.google.com/macros/s/AKfycbyI7a_xj7zvCa10CnzJP9wf8N1qPG3DYYb-LVkUCOEVEHjALzZq_Ox9YUJgbkzq86py6g/exec"),
    "sheets_csv_url": os.getenv("GOOGLE_SHEETS_CSV_URL", "https://docs.google.com/spreadsheets/d/e/2PACX-1vTbiLOkNqsCz_FKIJH3em6HPT8N6cLFAuAKCW1ALz7JP9JwrQvvSQRIvtv2OuESSSB2Hkbrh2BchoWd/pub?output=csv")
}

# Security
security = HTTPBearer(auto_error=False)

# =============================================================================
# Enhanced Dependency Management
# =============================================================================

def get_required_config(key: str) -> str:
    """Get required environment variable or raise error."""
    value = os.getenv(key)
    if not value:
        raise HTTPException(
            status_code=500, 
            detail=f"Required environment variable {key} is not set"
        )
    return value

def verify_auth(credentials: Optional[HTTPAuthorizationCredentials] = Depends(security)):
    """Enhanced authentication dependency."""
    if REQUIRE_AUTH:
        valid_tokens = [APP_TOKEN, BACKUP_APP_TOKEN]
        if not credentials or credentials.credentials not in valid_tokens:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Invalid or missing authentication token"
            )
    return True

# =============================================================================
# Enhanced Google Sheets Client
# =============================================================================

class GoogleSheetsManager:
    """Enhanced Google Sheets manager with connection pooling and error handling."""
    
    _client: Optional[gspread.Client] = None
    _spreadsheet: Optional[gspread.Spreadsheet] = None
    
    @classmethod
    def get_client(cls) -> Optional[gspread.Client]:
        """Get or create Google Sheets client with enhanced error handling."""
        if cls._client:
            return cls._client
            
        try:
            creds_json = os.getenv('GOOGLE_SHEETS_CREDENTIALS')
            if not creds_json:
                logger.error("GOOGLE_SHEETS_CREDENTIALS environment variable not set")
                return None
            
            creds_dict = json.loads(creds_json)
            creds = Credentials.from_service_account_info(creds_dict, scopes=SCOPES)
            cls._client = gspread.authorize(creds)
            logger.info("✅ Google Sheets client initialized successfully")
            return cls._client
            
        except json.JSONDecodeError as e:
            logger.error(f"❌ Invalid JSON in GOOGLE_SHEETS_CREDENTIALS: {e}")
        except Exception as e:
            logger.error(f"❌ Failed to initialize Google Sheets client: {e}")
            
        return None

    @classmethod
    def get_spreadsheet(cls) -> Optional[gspread.Spreadsheet]:
        """Get spreadsheet with caching and error handling."""
        if cls._spreadsheet:
            return cls._spreadsheet
            
        client = cls.get_client()
        if not client:
            return None
            
        try:
            spreadsheet_id = GOOGLE_SERVICES["spreadsheet_id"]
            cls._spreadsheet = client.open_by_key(spreadsheet_id)
            logger.info(f"✅ Spreadsheet accessed: {cls._spreadsheet.title}")
            return cls._spreadsheet
        except Exception as e:
            logger.error(f"❌ Failed to access spreadsheet: {e}")
            return None

    @classmethod
    def test_connection(cls) -> Dict[str, Any]:
        """Test Google Sheets connection comprehensively."""
        try:
            spreadsheet = cls.get_spreadsheet()
            if not spreadsheet:
                return {"status": "ERROR", "message": "Failed to access spreadsheet"}
                
            worksheets = spreadsheet.worksheets()
            sheet_info = []
            
            for ws in worksheets:
                try:
                    record_count = len(ws.get_all_records())
                    sheet_info.append({
                        "name": ws.title,
                        "row_count": ws.row_count,
                        "col_count": ws.col_count,
                        "record_count": record_count
                    })
                except Exception as e:
                    sheet_info.append({
                        "name": ws.title,
                        "error": str(e)
                    })
            
            return {
                "status": "SUCCESS",
                "spreadsheet_title": spreadsheet.title,
                "total_sheets": len(worksheets),
                "sheets": sheet_info
            }
            
        except Exception as e:
            return {"status": "ERROR", "message": f"Connection test failed: {str(e)}"}

# =============================================================================
# Financial Data API Client
# =============================================================================

class FinancialDataClient:
    """Enhanced financial data client with multiple API support."""
    
    def __init__(self):
        self.apis = FINANCIAL_APIS
        self.session = None
    
    async def get_session(self):
        """Get or create aiohttp session."""
        if self.session is None:
            self.session = aiohttp.ClientSession()
        return self.session
    
    async def close(self):
        """Close the session."""
        if self.session:
            await self.session.close()
    
    async def get_stock_quote(self, symbol: str, api_name: str = "alpha_vantage") -> Optional[Dict[str, Any]]:
        """Get stock quote from specified API."""
        if api_name not in self.apis:
            logger.error(f"API {api_name} not configured")
            return None
            
        api_config = self.apis[api_name]
        
        try:
            session = await self.get_session()
            
            if api_name == "alpha_vantage":
                params = {
                    'function': 'GLOBAL_QUOTE',
                    'symbol': symbol,
                    'apikey': api_config["api_key"]
                }
                async with session.get(api_config["base_url"], params=params, timeout=api_config["timeout"]) as response:
                    if response.status == 200:
                        data = await response.json()
                        return self._parse_alpha_vantage_quote(data)
            
            elif api_name == "finnhub":
                params = {
                    'symbol': symbol,
                    'token': api_config["api_key"]
                }
                async with session.get(f"{api_config['base_url']}/quote", params=params, timeout=api_config["timeout"]) as response:
                    if response.status == 200:
                        data = await response.json()
                        return self._parse_finnhub_quote(data, symbol)
            
            # Add other API implementations here...
            
        except Exception as e:
            logger.error(f"Error fetching data from {api_name}: {e}")
        
        return None
    
    def _parse_alpha_vantage_quote(self, data: Dict) -> Optional[Dict[str, Any]]:
        """Parse Alpha Vantage quote data."""
        try:
            quote = data.get('Global Quote', {})
            if not quote:
                return None
                
            return {
                'price': float(quote.get('05. price', 0)),
                'change': float(quote.get('09. change', 0)),
                'change_percent': float(quote.get('10. change percent', '0').rstrip('%')),
                'volume': int(quote.get('06. volume', 0)),
                'timestamp': quote.get('07. latest trading day')
            }
        except Exception as e:
            logger.error(f"Error parsing Alpha Vantage data: {e}")
            return None
    
    def _parse_finnhub_quote(self, data: Dict, symbol: str) -> Optional[Dict[str, Any]]:
        """Parse Finnhub quote data."""
        try:
            return {
                'price': data.get('c', 0),
                'change': data.get('d', 0),
                'change_percent': data.get('dp', 0),
                'high': data.get('h', 0),
                'low': data.get('l', 0),
                'open': data.get('o', 0),
                'previous_close': data.get('pc', 0),
                'timestamp': datetime.datetime.fromtimestamp(data.get('t', 0)).isoformat() if data.get('t') else None
            }
        except Exception as e:
            logger.error(f"Error parsing Finnhub data: {e}")
            return None
    
    def get_api_status(self) -> Dict[str, bool]:
        """Get status of all configured APIs."""
        status = {}
        for api_name, config in self.apis.items():
            status[api_name] = bool(config.get("api_key"))
        return status

# Initialize financial data client
financial_client = FinancialDataClient()

# =============================================================================
# Enhanced Data Models
# =============================================================================

class Quote(BaseModel):
    ticker: str = Field(..., description="Stock ticker symbol", example="7201.SR")
    company: Optional[str] = Field(None, description="Company name")
    currency: Optional[str] = Field(None, description="Currency code", example="SAR")
    price: Optional[float] = Field(None, ge=0, description="Current price")
    previous_close: Optional[float] = Field(None, ge=0, description="Previous close price")
    day_change_pct: Optional[float] = Field(None, description="Daily change percentage")
    market_cap: Optional[float] = Field(None, ge=0, description="Market capitalization")
    volume: Optional[float] = Field(NNone, ge=0, description="Trading volume")
    fifty_two_week_high: Optional[float] = Field(None, ge=0, description="52-week high")
    fifty_two_week_low: Optional[float] = Field(None, ge=0, description="52-week low")
    timestamp_utc: Optional[str] = Field(None, description="UTC timestamp")
    data_source: Optional[str] = Field(None, description="Data source")

    @validator('ticker')
    def validate_ticker(cls, v):
        if not v or not v.strip():
            raise ValueError('Ticker cannot be empty')
        return v.strip().upper()

class QuoteUpdatePayload(BaseModel):
    data: List[Quote] = Field(..., description="List of quotes to update")

class QuoteResponse(BaseModel):
    data: List[Quote] = Field(..., description="List of quotes")

class SheetVerificationResult(BaseModel):
    sheet_name: str
    status: str
    row_count: int
    column_count: int
    headers: List[str]
    sample_data: List[Dict]
    error: Optional[str] = None

class VerificationResponse(BaseModel):
    spreadsheet_id: str
    overall_status: str
    sheets_verified: int
    sheets_ok: int
    details: List[SheetVerificationResult]

class SheetDataResponse(BaseModel):
    sheet_name: str
    total_records: int
    data: List[Dict]

class HealthResponse(BaseModel):
    status: str
    time_utc: str
    version: str
    google_sheets_connected: bool
    cache_status: Dict[str, Any]
    features: Dict[str, bool]
    api_status: Dict[str, bool]

class ErrorResponse(BaseModel):
    error: str
    detail: Optional[str] = None
    timestamp: str

class APITestResponse(BaseModel):
    api_name: str
    status: str
    response_time: Optional[float] = None
    error: Optional[str] = None

# =============================================================================
# Enhanced Cache System
# =============================================================================

class EnhancedCache:
    """Enhanced cache system with backup and recovery."""
    
    def __init__(self):
        self.cache_path = BASE_DIR / "quote_cache.json"
        self.backup_dir = BASE_DIR / "cache_backups"
        self.backup_dir.mkdir(parents=True, exist_ok=True)
        self.data: Dict[str, Dict[str, Any]] = {}
        self.load_cache()
    
    def load_cache(self) -> bool:
        """Load cache from disk with enhanced error handling."""
        try:
            if not self.cache_path.exists():
                logger.info("No cache file found, starting with empty cache")
                return True
                
            with open(self.cache_path, 'r', encoding='utf-8') as f:
                raw_data = json.load(f)
            
            # Normalize cache format
            if isinstance(raw_data, dict) and 'data' in raw_data:
                for item in raw_data['data']:
                    if isinstance(item, dict) and 'ticker' in item:
                        self.data[item['ticker']] = item
            elif isinstance(raw_data, dict):
                self.data = raw_data
                
            logger.info(f"✅ Cache loaded with {len(self.data)} items")
            return True
            
        except Exception as e:
            logger.error(f"❌ Failed to load cache: {e}")
            self.data = {}
            return False
    
    def save_cache(self) -> bool:
        """Save cache to disk with atomic write."""
        try:
            # Atomic write using temporary file
            temp_path = self.cache_path.with_suffix('.tmp')
            with open(temp_path, 'w', encoding='utf-8') as f:
                json.dump({"data": list(self.data.values())}, f, indent=2, ensure_ascii=False)
            
            # Replace original file
            temp_path.replace(self.cache_path)
            logger.info(f"✅ Cache saved with {len(self.data)} items")
            return True
            
        except Exception as e:
            logger.error(f"❌ Failed to save cache: {e}")
            return False
    
    def create_backup(self) -> Optional[str]:
        """Create timestamped backup."""
        try:
            if not self.data:
                return None
                
            timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
            backup_path = self.backup_dir / f"quote_cache_{timestamp}.json"
            
            with open(backup_path, 'w', encoding='utf-8') as f:
                json.dump({"data": list(self.data.values())}, f, indent=2, ensure_ascii=False)
            
            logger.info(f"✅ Backup created: {backup_path}")
            return str(backup_path)
            
        except Exception as e:
            logger.error(f"❌ Backup failed: {e}")
            return None
    
    def get_quote(self, ticker: str) -> Optional[Quote]:
        """Get quote from cache."""
        ticker = ticker.upper()
        if ticker in self.data:
            return Quote(**self.data[ticker])
        return None
    
    def update_quotes(self, quotes: List[Quote]) -> Tuple[int, List[str]]:
        """Update multiple quotes in cache."""
        updated = 0
        errors = []
        
        for quote in quotes:
            try:
                self.data[quote.ticker] = quote.dict()
                updated += 1
            except Exception as e:
                errors.append(f"Failed to update {quote.ticker}: {e}")
        
        if updated > 0:
            self.save_cache()
            
        return updated, errors

# Initialize cache
cache = EnhancedCache()

# =============================================================================
# Enhanced Sheet Operations
# =============================================================================

def verify_sheet_structure(sheet_name: str) -> SheetVerificationResult:
    """Verify structure and content of a specific sheet."""
    try:
        spreadsheet = GoogleSheetsManager.get_spreadsheet()
        if not spreadsheet:
            return SheetVerificationResult(
                sheet_name=sheet_name,
                status="ERROR",
                row_count=0,
                column_count=0,
                headers=[],
                sample_data=[],
                error="Failed to access spreadsheet"
            )
        
        worksheet = spreadsheet.worksheet(sheet_name)
        all_data = worksheet.get_all_records()
        headers = worksheet.row_values(1) if worksheet.row_values(1) else []
        
        return SheetVerificationResult(
            sheet_name=sheet_name,
            status="OK",
            row_count=len(all_data) + 1,
            column_count=len(headers),
            headers=headers,
            sample_data=all_data[:3] if len(all_data) > 3 else all_data
        )
        
    except gspread.WorksheetNotFound:
        return SheetVerificationResult(
            sheet_name=sheet_name,
            status="NOT_FOUND",
            row_count=0,
            column_count=0,
            headers=[],
            sample_data=[],
            error=f"Worksheet '{sheet_name}' not found"
        )
    except Exception as e:
        return SheetVerificationResult(
            sheet_name=sheet_name,
            status="ERROR",
            row_count=0,
            column_count=0,
            headers=[],
            sample_data=[],
            error=str(e)
        )

def get_sheet_data(sheet_name: str, limit: int = 10) -> SheetDataResponse:
    """Get data from specific sheet with pagination."""
    try:
        spreadsheet = GoogleSheetsManager.get_spreadsheet()
        if not spreadsheet:
            raise HTTPException(status_code=500, detail="Failed to access spreadsheet")
        
        worksheet = spreadsheet.worksheet(sheet_name)
        data = worksheet.get_all_records()
        
        return SheetDataResponse(
            sheet_name=sheet_name,
            total_records=len(data),
            data=data[:limit]
        )
        
    except gspread.WorksheetNotFound:
        raise HTTPException(status_code=404, detail=f"Sheet '{sheet_name}' not found")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error reading sheet: {str(e)}")

# =============================================================================
# Optional Module Imports with Enhanced Error Handling
# =============================================================================

def safe_import(module_name: str, class_name: str = None):
    """Safely import optional modules with comprehensive error handling."""
    try:
        module = __import__(module_name)
        if class_name:
            return getattr(module, class_name)
        return module
    except ImportError as e:
        logger.warning(f"⚠️ {module_name} not available: {e}")
        return None
    except Exception as e:
        logger.error(f"❌ Error loading {module_name}: {e}")
        return None

# Import optional modules
AdvancedMarketDashboard = safe_import('advanced_market_dashboard', 'AdvancedMarketDashboard')
argaam_router = safe_import('routes_argaam', 'router')
close_argaam_http_client = safe_import('routes_argaam', 'close_argaam_http_client')
google_apps_script_client = safe_import('google_apps_script_client', 'google_apps_script_client')
sr = safe_import('symbols_reader')
analyzer = safe_import('advanced_analysis', 'analyzer')

# Feature flags
HAS_DASHBOARD = AdvancedMarketDashboard is not None
HAS_ARGAAM_ROUTES = argaam_router is not None
HAS_GOOGLE_APPS_SCRIPT = google_apps_script_client is not None
HAS_SYMBOLS_READER = sr is not None
HAS_ADVANCED_ANALYSIS = analyzer is not None

# =============================================================================
# FastAPI Application Setup
# =============================================================================

@asynccontextmanager
async def lifespan(app: FastAPI):
    """Enhanced application lifespan management."""
    # Startup
    startup_time = datetime.datetime.utcnow()
    logger.info(f"🚀 Starting {SERVICE_NAME} v{SERVICE_VERSION}")
    
    # Test critical connections
    sheets_status = GoogleSheetsManager.test_connection()
    if sheets_status["status"] == "SUCCESS":
        logger.info("✅ Google Sheets connection verified")
    else:
        logger.warning(f"⚠️ Google Sheets connection issue: {sheets_status.get('message')}")
    
    yield
    
    # Shutdown
    try:
        if HAS_ARGAAM_ROUTES and close_argaam_http_client:
            await close_argaam_http_client()
            logger.info("✅ Argaam HTTP client closed")
    except Exception as e:
        logger.warning(f"⚠️ Error closing Argaam client: {e}")
    
    # Close financial data client
    await financial_client.close()
    
    shutdown_time = datetime.datetime.utcnow()
    uptime = shutdown_time - startup_time
    logger.info(f"🛑 Shutting down after {uptime}")

# Create FastAPI app
app = FastAPI(
    title=SERVICE_NAME,
    version=SERVICE_VERSION,
    description="""
    ## Tadawul Stock Analysis API 📈
    
    Comprehensive API for Saudi Stock Market analysis with Google Sheets integration.
    
    ### Key Features:
    - ✅ Real-time stock data and analysis
    - ✅ Google Sheets integration with 9-page verification
    - ✅ Multi-source data aggregation (6 financial APIs)
    - ✅ Advanced caching system
    - ✅ Comprehensive health monitoring
    
    ### Integrated Financial APIs:
    - Alpha Vantage, Finnhub, EODHD, Twelve Data, MarketStack, FMP
    
    ### Main Endpoints:
    - `GET /` - API information and status
    - `GET /health` - Comprehensive health check
    - `GET /verify-sheets` - Verify all Google Sheets pages
    - `GET /sheet/{name}` - Get specific sheet data
    - `GET /api/saudi/market` - Saudi market data
    - `GET /v1/financial-apis/status` - Financial APIs status
    """,
    docs_url="/docs" if os.getenv("ENABLE_SWAGGER", "true").lower() == "true" else None,
    redoc_url="/redoc" if os.getenv("ENABLE_REDOC", "true").lower() == "true" else None,
    lifespan=lifespan
)

# CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# =============================================================================
# Enhanced Exception Handlers
# =============================================================================

@app.exception_handler(HTTPException)
async def http_exception_handler(request, exc):
    """Enhanced HTTP exception handler."""
    logger.warning(f"HTTP {exc.status_code}: {exc.detail}")
    return JSONResponse(
        status_code=exc.status_code,
        content=ErrorResponse(
            error=exc.detail,
            timestamp=datetime.datetime.utcnow().isoformat() + "Z"
        ).dict()
    )

@app.exception_handler(Exception)
async def global_exception_handler(request, exc):
    """Global exception handler for unhandled errors."""
    logger.error(f"Unhandled exception: {exc}", exc_info=True)
    return JSONResponse(
        status_code=500,
        content=ErrorResponse(
            error="Internal server error",
            detail=str(exc) if os.getenv("ENVIRONMENT") == "development" else None,
            timestamp=datetime.datetime.utcnow().isoformat() + "Z"
        ).dict()
    )

# =============================================================================
# Core API Endpoints
# =============================================================================

@app.get("/", response_model=Dict[str, Any])
async def root(auth: bool = Depends(verify_auth)):
    """Enhanced root endpoint with comprehensive API information."""
    sheets_status = GoogleSheetsManager.test_connection()
    api_status = financial_client.get_api_status()
    
    return {
        "service": SERVICE_NAME,
        "version": SERVICE_VERSION,
        "status": "operational",
        "timestamp": datetime.datetime.utcnow().isoformat() + "Z",
        "environment": os.getenv("ENVIRONMENT", "production"),
        "authentication_required": REQUIRE_AUTH,
        "google_sheets": {
            "connected": sheets_status["status"] == "SUCCESS",
            "spreadsheet": sheets_status.get("spreadsheet_title"),
            "total_sheets": sheets_status.get("total_sheets", 0)
        },
        "financial_apis": api_status,
        "features": {
            "dashboard": HAS_DASHBOARD,
            "argaam_integration": HAS_ARGAAM_ROUTES,
            "google_apps_script": HAS_GOOGLE_APPS_SCRIPT,
            "symbols_reader": HAS_SYMBOLS_READER,
            "advanced_analysis": HAS_ADVANCED_ANALYSIS
        },
        "cache": {
            "items": len(cache.data),
            "file_exists": cache.cache_path.exists()
        },
        "endpoints": {
            "health": "/health",
            "sheets_verification": "/verify-sheets",
            "sheet_data": "/sheet/{sheet_name}",
            "sheet_names": "/sheet-names",
            "connection_test": "/test-connection",
            "saudi_market": "/api/saudi/market",
            "quotes": "/v1/quote",
            "cache_info": "/v1/cache",
            "financial_apis_status": "/v1/financial-apis/status"
        }
    }

@app.get("/health", response_model=HealthResponse)
async def health_check():
    """Comprehensive health check endpoint."""
    sheets_status = GoogleSheetsManager.test_connection()
    api_status = financial_client.get_api_status()
    
    health_status = "healthy"
    if sheets_status["status"] != "SUCCESS":
        health_status = "degraded"
    
    return HealthResponse(
        status=health_status,
        time_utc=datetime.datetime.utcnow().isoformat() + "Z",
        version=SERVICE_VERSION,
        google_sheets_connected=sheets_status["status"] == "SUCCESS",
        cache_status={
            "items": len(cache.data),
            "file_exists": cache.cache_path.exists(),
            "last_updated": cache.cache_path.stat().st_mtime if cache.cache_path.exists() else None
        },
        features={
            "dashboard": HAS_DASHBOARD,
            "argaam_routes": HAS_ARGAAM_ROUTES,
            "google_apps_script": HAS_GOOGLE_APPS_SCRIPT
        },
        api_status=api_status
    )

# =============================================================================
# Google Sheets Verification Endpoints
# =============================================================================

@app.get("/verify-sheets", response_model=VerificationResponse)
async def verify_all_sheets(auth: bool = Depends(verify_auth)):
    """
    Verify all 9 Google Sheets pages with comprehensive status reporting.
    
    This endpoint checks:
    - Sheet existence and accessibility
    - Data structure and headers
    - Row and column counts
    - Sample data validation
    """
    try:
        spreadsheet = GoogleSheetsManager.get_spreadsheet()
        if not spreadsheet:
            raise HTTPException(status_code=500, detail="Failed to access Google Sheets")
        
        results = []
        for sheet_name in EXPECTED_SHEETS:
            result = verify_sheet_structure(sheet_name)
            results.append(result)
            logger.info(f"Verified {sheet_name}: {result.status}")
        
        sheets_ok = sum(1 for r in results if r.status == "OK")
        overall_status = "SUCCESS" if sheets_ok == len(EXPECTED_SHEETS) else "PARTIAL_SUCCESS"
        
        return VerificationResponse(
            spreadsheet_id=GOOGLE_SERVICES["spreadsheet_id"],
            overall_status=overall_status,
            sheets_verified=len(EXPECTED_SHEETS),
            sheets_ok=sheets_ok,
            details=results
        )
        
    except Exception as e:
        logger.error(f"Sheet verification failed: {e}")
        raise HTTPException(status_code=500, detail=f"Verification failed: {str(e)}")

@app.get("/sheet/{sheet_name}", response_model=SheetDataResponse)
async def get_sheet_data(
    sheet_name: str,
    limit: int = Query(10, ge=1, le=1000, description="Number of records to return"),
    auth: bool = Depends(verify_auth)
):
    """Get paginated data from specific Google Sheet."""
    return get_sheet_data(sheet_name, limit)

@app.get("/sheet-names", response_model=Dict[str, Any])
async def get_sheet_names(auth: bool = Depends(verify_auth)):
    """Get all available sheet names with comparison to expected sheets."""
    try:
        spreadsheet = GoogleSheetsManager.get_spreadsheet()
        if not spreadsheet:
            raise HTTPException(status_code=500, detail="Failed to access spreadsheet")
        
        worksheets = spreadsheet.worksheets()
        available_sheets = [ws.title for ws in worksheets]
        
        return {
            "spreadsheet_title": spreadsheet.title,
            "available_sheets": available_sheets,
            "expected_sheets": EXPECTED_SHEETS,
            "missing_sheets": list(set(EXPECTED_SHEETS) - set(available_sheets)),
            "extra_sheets": list(set(available_sheets) - set(EXPECTED_SHEETS)),
            "verification_required": len(EXPECTED_SHEETS)
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error getting sheet names: {str(e)}")

@app.get("/test-connection", response_model=Dict[str, Any])
async def test_connection(auth: bool = Depends(verify_auth)):
    """Comprehensive Google Sheets connection test."""
    return GoogleSheetsManager.test_connection()

# =============================================================================
# Financial APIs Endpoints
# =============================================================================

@app.get("/v1/financial-apis/status", response_model=Dict[str, Any])
async def get_financial_apis_status(auth: bool = Depends(verify_auth)):
    """Get status of all financial APIs."""
    api_status = financial_client.get_api_status()
    
    # Test each API with a simple request
    test_results = []
    for api_name in api_status.keys():
        if api_status[api_name]:  # Only test if API key is configured
            try:
                start_time = datetime.datetime.now()
                result = await financial_client.get_stock_quote("AAPL", api_name)
                response_time = (datetime.datetime.now() - start_time).total_seconds()
                
                if result:
                    test_results.append({
                        "api_name": api_name,
                        "status": "SUCCESS",
                        "response_time": round(response_time, 3),
                        "data_sample": result
                    })
                else:
                    test_results.append({
                        "api_name": api_name,
                        "status": "ERROR",
                        "error": "No data returned"
                    })
            except Exception as e:
                test_results.append({
                    "api_name": api_name,
                    "status": "ERROR",
                    "error": str(e)
                })
    
    return {
        "api_configuration": api_status,
        "api_tests": test_results,
        "timestamp": datetime.datetime.utcnow().isoformat() + "Z"
    }

@app.get("/v1/financial-data/{symbol}")
async def get_financial_data(
    symbol: str,
    api: str = Query("alpha_vantage", description="API to use"),
    auth: bool = Depends(verify_auth)
):
    """Get financial data for a symbol from specified API."""
    if api not in FINANCIAL_APIS:
        raise HTTPException(status_code=400, detail=f"API {api} not supported")
    
    data = await financial_client.get_stock_quote(symbol, api)
    if not data:
        raise HTTPException(status_code=404, detail=f"No data found for {symbol} from {api}")
    
    return {
        "symbol": symbol,
        "api": api,
        "data": data,
        "timestamp": datetime.datetime.utcnow().isoformat() + "Z"
    }

# =============================================================================
# Market Data Endpoints
# =============================================================================

@app.get("/api/saudi/symbols", response_model=Dict[str, Any])
async def get_saudi_symbols(
    limit: int = Query(20, ge=1, le=500),
    auth: bool = Depends(verify_auth)
):
    """Get Saudi market symbols with fallback strategies."""
    try:
        # Try symbols_reader first
        if HAS_SYMBOLS_READER and hasattr(sr, 'fetch_symbols'):
            payload = sr.fetch_symbols(limit)
            if payload and isinstance(payload, dict):
                return payload
        
        # Fallback to direct sheet reading
        try:
            sheet_data = get_sheet_data("Companies", limit)
            symbols = []
            for item in sheet_data.data:
                if 'symbol' in item or 'ticker' in item:
                    symbols.append({
                        "symbol": item.get('symbol') or item.get('ticker'),
                        "company_name": item.get('company_name') or item.get('company'),
                        "sector": item.get('sector') or item.get('trading_sector')
                    })
            
            return {
                "data": symbols[:limit],
                "count": len(symbols),
                "source": "google_sheets_fallback"
            }
        except:
            pass
        
        # Final fallback
        return {
            "data": [],
            "count": 0,
            "source": "fallback",
            "message": "No symbol sources available"
        }
        
    except Exception as e:
        logger.error(f"Failed to fetch symbols: {e}")
        return {
            "data": [],
            "count": 0,
            "error": str(e),
            "source": "error"
        }

@app.get("/api/saudi/market", response_model=Dict[str, Any])
async def get_saudi_market(
    limit: int = Query(20, ge=1, le=500),
    auth: bool = Depends(verify_auth)
):
    """Get comprehensive Saudi market data with cached quotes."""
    try:
        symbols_data = await get_saudi_symbols(limit, auth)
        symbols = symbols_data.get("data", [])
        
        market_data = []
        for symbol_info in symbols:
            symbol = symbol_info.get("symbol")
            if not symbol:
                continue
                
            # Enhance with cached quote data
            quote_data = cache.get_quote(symbol)
            market_data.append({
                **symbol_info,
                "price": quote_data.price if quote_data else None,
                "change_percent": quote_data.day_change_pct if quote_data else None,
                "volume": quote_data.volume if quote_data else None,
                "market_cap": quote_data.market_cap if quote_data else None,
                "last_updated": quote_data.timestamp_utc if quote_data else None
            })
        
        return {
            "count": len(market_data),
            "data": market_data,
            "source": symbols_data.get("source", "unknown"),
            "cache_hits": sum(1 for item in market_data if item.get("price") is not None)
        }
        
    except Exception as e:
        logger.error(f"Market data error: {e}")
        raise HTTPException(status_code=500, detail=f"Market data unavailable: {str(e)}")

# =============================================================================
# Quote Management Endpoints
# =============================================================================

@app.get("/v1/quote", response_model=QuoteResponse)
async def get_quotes(
    tickers: str = Query(..., description="Comma-separated ticker symbols"),
    auth: bool = Depends(verify_auth)
):
    """Get quotes for multiple tickers from cache."""
    symbols = [t.strip().upper() for t in tickers.split(",") if t.strip()]
    if not symbols:
        raise HTTPException(status_code=400, detail="No tickers provided")
    
    quotes = []
    for symbol in symbols:
        quote = cache.get_quote(symbol)
        if quote:
            quotes.append(quote)
        else:
            # Return empty quote for missing symbols
            quotes.append(Quote(
                ticker=symbol,
                timestamp_utc=datetime.datetime.utcnow().isoformat() + "Z"
            ))
    
    return QuoteResponse(data=quotes)

@app.post("/v1/quote/update", response_model=Dict[str, Any])
async def update_quotes(
    payload: QuoteUpdatePayload,
    autosave: bool = Query(True),
    auth: bool = Depends(verify_auth)
):
    """Update quotes in cache with validation."""
    updated_count, errors = cache.update_quotes(payload.data)
    
    response = {
        "status": "completed",
        "updated_count": updated_count,
        "error_count": len(errors),
        "errors": errors if errors else None,
        "total_cached": len(cache.data),
        "autosaved": autosave
    }
    
    logger.info(f"Quotes updated: {updated_count} successful, {len(errors)} errors")
    return response

@app.get("/v1/cache", response_model=Dict[str, Any])
async def get_cache_info(
    limit: int = Query(50, ge=1, le=1000),
    auth: bool = Depends(verify_auth)
):
    """Get cache information and sample data."""
    items = list(cache.data.values())[:limit]
    
    return {
        "total_items": len(cache.data),
        "sample_size": len(items),
        "sample_data": items,
        "cache_file": str(cache.cache_path),
        "file_exists": cache.cache_path.exists(),
        "file_size": cache.cache_path.stat().st_size if cache.cache_path.exists() else 0
    }

# =============================================================================
# Additional Utility Endpoints
# =============================================================================

@app.get("/v1/ping", response_model=Dict[str, Any])
async def ping():
    """Simple ping endpoint for service availability."""
    return {
        "status": "ok",
        "service": SERVICE_NAME,
        "version": SERVICE_VERSION,
        "timestamp": datetime.datetime.utcnow().isoformat() + "Z"
    }

@app.post("/v1/cache/backup", response_model=Dict[str, Any])
async def create_cache_backup(auth: bool = Depends(verify_auth)):
    """Create a backup of the current cache."""
    backup_path = cache.create_backup()
    if backup_path:
        return {
            "status": "success",
            "backup_path": backup_path,
            "item_count": len(cache.data)
        }
    else:
        raise HTTPException(status_code=500, detail="Failed to create backup")

# =============================================================================
# Include Argaam Routes if available
# =============================================================================

if HAS_ARGAAM_ROUTES:
    app.include_router(argaam_router)
    logger.info("✅ Argaam routes mounted successfully")

# =============================================================================
# Application Entry Point
# =============================================================================

if __name__ == "__main__":
    # Enhanced server configuration
    server_config = {
        "app": "main:app",
        "host": APP_HOST,
        "port": APP_PORT,
        "log_level": "info",
        "access_log": True,
        "reload": os.getenv("ENVIRONMENT") == "development"
    }
    
    logger.info(f"🚀 Starting {SERVICE_NAME} v{SERVICE_VERSION} on {APP_HOST}:{APP_PORT}")
    logger.info(f"📊 Expected sheets: {len(EXPECTED_SHEETS)}")
    logger.info(f"🔐 Authentication: {'Enabled' if REQUIRE_AUTH else 'Disabled'}")
    logger.info(f"📈 Financial APIs: {len([k for k, v in financial_client.get_api_status().items() if v])} configured")
    
    try:
        uvicorn.run(**server_config)
    except KeyboardInterrupt:
        logger.info("Server stopped by user")
    except Exception as e:
        logger.error(f"Server failed to start: {e}")
        raise
