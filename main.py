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

# Import the new Google Sheets service (10-page structure)
from google_sheets_service import google_sheets_service

# =============================================================================
# Basic setup
# =============================================================================

BASE_DIR = Path(__file__).resolve().parent
load_dotenv(dotenv_path=BASE_DIR / ".env")

# Logging
LOG_ENABLE_FILE = os.getenv("LOG_ENABLE_FILE", "false").lower() == "true"

handlers: List[logging.Handler] = [logging.StreamHandler()]
if LOG_ENABLE_FILE:
    handlers.append(logging.FileHandler(str(BASE_DIR / "app.log")))

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=handlers,
)
logger = logging.getLogger(__name__)

# =============================================================================
# Configuration
# =============================================================================

class AppConfig(BaseSettings):
    # Service
    service_name: str = Field("Tadawul Stock Analysis API", env="SERVICE_NAME")
    service_version: str = Field("2.2.0", env="SERVICE_VERSION")
    app_host: str = Field("0.0.0.0", env="APP_HOST")
    app_port: int = Field(8000, env="APP_PORT")
    environment: str = Field("production", env="ENVIRONMENT")

    # Security
    require_auth: bool = Field(True, env="REQUIRE_AUTH")
    app_token: Optional[str] = Field(None, env="APP_TOKEN")
    backup_app_token: Optional[str] = Field(None, env="BACKUP_APP_TOKEN")

    enable_rate_limiting: bool = Field(True, env="ENABLE_RATE_LIMITING")
    max_requests_per_minute: int = Field(60, env="RATE_LIMIT_REQUESTS_PER_MINUTE")

    # CORS
    cors_origins: List[str] = Field(default_factory=lambda: ["*"], env="CORS_ORIGINS")

    # Logging / Docs
    log_enable_file: bool = Field(LOG_ENABLE_FILE, env="LOG_ENABLE_FILE")
    log_level: str = Field("INFO", env="LOG_LEVEL")
    enable_swagger: bool = Field(True, env="ENABLE_SWAGGER")
    enable_redoc: bool = Field(True, env="ENABLE_REDOC")

    # Google Sheets
    spreadsheet_id: str = Field(..., env="SPREADSHEET_ID")
    google_service_account_json: Optional[str] = Field(None, env="GOOGLE_SERVICE_ACCOUNT_JSON")
    google_apps_script_url: str = Field(..., env="GOOGLE_APPS_SCRIPT_URL")
    google_apps_script_backup_url: Optional[str] = Field(None, env="GOOGLE_APPS_SCRIPT_BACKUP_URL")

    # Financial APIs (optional)
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

    @validator("spreadsheet_id")
    def validate_spreadsheet_id(cls, v: str) -> str:
        if not v or len(v) < 10:
            raise ValueError("Invalid Spreadsheet ID")
        return v

    @validator("environment")
    def validate_environment(cls, v: str) -> str:
        if v not in {"development", "staging", "production"}:
            raise ValueError("Environment must be development, staging, or production")
        return v

try:
    config = AppConfig()
    logger.info("✅ Configuration loaded successfully")
except Exception as e:
    logger.error(f"❌ Configuration validation failed: {e}")
    raise

# =============================================================================
# Constants / Security / Rate limiting
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
        "base_url": os.getenv("ALPHA_VANTAGE_BASE_URL", "https://www.alphavantage.co/query"),
        "timeout": int(os.getenv("ALPHA_VANTAGE_TIMEOUT", "15")),
    },
    "finnhub": {
        "api_key": config.finnhub_api_key,
        "base_url": os.getenv("FINNHUB_BASE_URL", "https://finnhub.io/api/v1"),
        "timeout": int(os.getenv("FINNHUB_TIMEOUT", "15")),
    },
    "eodhd": {
        "api_key": config.eodhd_api_key,
        "base_url": os.getenv("EODHD_BASE_URL", "https://eodhistoricaldata.com/api"),
        "timeout": int(os.getenv("EODHD_TIMEOUT", "15")),
    },
    "twelvedata": {
        "api_key": config.twelvedata_api_key,
        "base_url": os.getenv("TWELVEDATA_BASE_URL", "https://api.twelvedata.com"),
        "timeout": int(os.getenv("TWELVEDATA_TIMEOUT", "15")),
    },
    "marketstack": {
        "api_key": config.marketstack_api_key,
        "base_url": os.getenv("MARKETSTACK_BASE_URL", "http://api.marketstack.com/v1"),
        "timeout": int(os.getenv("MARKETSTACK_TIMEOUT", "15")),
    },
    "fmp": {
        "api_key": config.fmp_api_key,
        "base_url": os.getenv("FMP_BASE_URL", "https://financialmodelingprep.com/api/v3"),
        "timeout": int(os.getenv("FMP_TIMEOUT", "15")),
    },
}

GOOGLE_SERVICES = {
    "spreadsheet_id": config.spreadsheet_id,
    "apps_script_url": config.google_apps_script_url,
    "apps_script_backup_url": config.google_apps_script_backup_url,
}

# Security
security = HTTPBearer(auto_error=False)

# Rate limiter
limiter = Limiter(key_func=get_remote_address)

def rate_limit(rule: str):
    """Wrapper to optionally disable rate limiting from config."""
    if not config.enable_rate_limiting:
        def _decorator(fn):
            return fn
        return _decorator
    return limiter.limit(rule)

# =============================================================================
# Custom exception classes
# =============================================================================

class FinancialAPIError(Exception):
    pass

class GoogleSheetsError(Exception):
    pass

class CacheError(Exception):
    pass

class ConfigurationError(Exception):
    pass

class SecurityError(Exception):
    pass

# =============================================================================
# Auth dependency - FIXED TO ALLOW PUBLIC ENDPOINTS
# =============================================================================

def verify_auth(credentials: Optional[HTTPAuthorizationCredentials] = Depends(security)):
    """
    Authentication dependency.
    
    - If REQUIRE_AUTH=false or no tokens configured => auth is disabled
    - Otherwise, validate token
    """
    if not config.require_auth:
        return True

    valid_tokens = [t for t in [config.app_token, config.backup_app_token] if t]

    # If no tokens configured, auth is effectively disabled
    if not valid_tokens:
        logger.warning(
            "REQUIRE_AUTH is True but no APP_TOKEN/BACKUP_APP_TOKEN configured. "
            "Auth will be treated as disabled."
        )
        return True

    if not credentials:
        logger.warning("Authentication attempted without credentials")
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Missing authentication token",
        )

    if credentials.credentials not in valid_tokens:
        logger.warning(f"Invalid token attempt: {credentials.credentials[:8]}...")
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid authentication token",
        )

    logger.info(f"Successful authentication with token: {credentials.credentials[:8]}...")
    return True

# =============================================================================
# Public endpoints dependency (no auth required)
# =============================================================================

def public_endpoint():
    """Dependency for public endpoints that never require authentication"""
    return True

# =============================================================================
# Google Sheets Manager
# =============================================================================

class GoogleSheetsManager:
    _client: Optional[gspread.Client] = None
    _spreadsheet: Optional[gspread.Spreadsheet] = None

    @classmethod
    def get_client(cls) -> gspread.Client:
        if cls._client:
            return cls._client

        try:
            creds_json = config.google_service_account_json
            if not creds_json:
                logger.warning(
                    "GOOGLE_SERVICE_ACCOUNT_JSON not configured. "
                    "GoogleSheetsManager is disabled."
                )
                raise GoogleSheetsError("GOOGLE_SERVICE_ACCOUNT_JSON not configured")

            creds_dict = json.loads(creds_json)
            creds = Credentials.from_service_account_info(creds_dict, scopes=SCOPES)
            cls._client = gspread.authorize(creds)
            logger.info("✅ Google Sheets client initialized successfully")
            return cls._client

        except GoogleSheetsError:
            raise
        except json.JSONDecodeError as e:
            logger.error(f"❌ Invalid GOOGLE_SERVICE_ACCOUNT_JSON: {e}")
            raise GoogleSheetsError(f"Invalid credentials format: {e}")
        except Exception as e:
            logger.error(f"❌ Failed to initialize Google Sheets client: {e}")
            raise GoogleSheetsError(f"Connection failed: {e}")

    @classmethod
    def get_spreadsheet(cls) -> gspread.Spreadsheet:
        if cls._spreadsheet:
            return cls._spreadsheet

        client = cls.get_client()
        try:
            spreadsheet_id = GOOGLE_SERVICES["spreadsheet_id"]
            cls._spreadsheet = client.open_by_key(spreadsheet_id)
            logger.info(f"✅ Spreadsheet accessed: {cls._spreadsheet.title}")
            return cls._spreadsheet
        except Exception as e:
            logger.error(f"❌ Failed to access spreadsheet: {e}")
            raise GoogleSheetsError(f"Spreadsheet access failed: {e}")

    @classmethod
    def test_connection(cls) -> Dict[str, Any]:
        try:
            spreadsheet = cls.get_spreadsheet()
            worksheets = spreadsheet.worksheets()
            sheet_info = []

            for ws in worksheets:
                try:
                    record_count = len(ws.get_all_records())
                    sheet_info.append(
                        {
                            "name": ws.title,
                            "row_count": ws.row_count,
                            "col_count": ws.col_count,
                            "record_count": record_count,
                        }
                    )
                except Exception as e:
                    sheet_info.append({"name": ws.title, "error": str(e)})

            return {
                "status": "SUCCESS",
                "spreadsheet_title": spreadsheet.title,
                "total_sheets": len(worksheets),
                "sheets": sheet_info,
            }
        except Exception as e:
            return {"status": "ERROR", "message": f"Connection test failed: {e}"}

# =============================================================================
# Financial data client
# =============================================================================

class FinancialDataClient:
    def __init__(self):
        self.apis = FINANCIAL_APIS
        self.session: Optional[aiohttp.ClientSession] = None

    async def get_session(self) -> aiohttp.ClientSession:
        if self.session is None:
            timeout = aiohttp.ClientTimeout(total=30)
            connector = aiohttp.TCPConnector(limit=100, limit_per_host=20)
            self.session = aiohttp.ClientSession(timeout=timeout, connector=connector)
        return self.session

    async def close(self):
        if self.session:
            await self.session.close()
            self.session = None

    def get_api_status(self) -> Dict[str, bool]:
        return {name: bool(cfg.get("api_key")) for name, cfg in self.apis.items()}

    async def get_stock_quote_with_retry(
        self, symbol: str, api_name: str = "alpha_vantage", max_retries: int = 3
    ) -> Optional[Dict[str, Any]]:
        for attempt in range(max_retries):
            try:
                result = await self.get_stock_quote(symbol, api_name)
                if result:
                    logger.info(f"✅ Successfully fetched {symbol} from {api_name} (attempt {attempt+1})")
                    return result
                logger.warning(f"⚠️ No data returned for {symbol} from {api_name} (attempt {attempt+1})")
            except Exception as e:
                logger.warning(f"⚠️ Attempt {attempt+1} failed for {api_name}: {e}")
                if attempt < max_retries - 1:
                    await asyncio.sleep(2**attempt)
        return None

    async def get_stock_quote(self, symbol: str, api_name: str = "alpha_vantage") -> Optional[Dict[str, Any]]:
        if api_name not in self.apis:
            logger.error(f"API {api_name} not configured")
            return None

        api_cfg = self.apis[api_name]
        if not api_cfg.get("api_key"):
            logger.warning(f"API key not configured for {api_name}")
            return None

        try:
            session = await self.get_session()

            if api_name == "alpha_vantage":
                params = {
                    "function": "GLOBAL_QUOTE",
                    "symbol": symbol,
                    "apikey": api_cfg["api_key"],
                }
                async with session.get(api_cfg["base_url"], params=params, timeout=api_cfg["timeout"]) as r:
                    if r.status == 200:
                        data = await r.json()
                        return self._parse_alpha_vantage_quote(data)
                    logger.error(f"Alpha Vantage returned status {r.status}")

            elif api_name == "finnhub":
                params = {"symbol": symbol, "token": api_cfg["api_key"]}
                async with session.get(f"{api_cfg['base_url']}/quote", params=params, timeout=api_cfg["timeout"]) as r:
                    if r.status == 200:
                        data = await r.json()
                        return self._parse_finnhub_quote(data)
                    logger.error(f"Finnhub returned status {r.status}")

            elif api_name == "twelvedata":
                params = {"symbol": symbol, "apikey": api_cfg["api_key"]}
                async with session.get(f"{api_cfg['base_url']}/quote", params=params, timeout=api_cfg["timeout"]) as r:
                    if r.status == 200:
                        data = await r.json()
                        return self._parse_twelvedata_quote(data)
                    logger.error(f"TwelveData returned status {r.status}")

        except asyncio.TimeoutError:
            logger.error(f"Timeout fetching data from {api_name}")
        except Exception as e:
            logger.error(f"Error fetching data from {api_name}: {e}")

        return None

    def _parse_alpha_vantage_quote(self, data: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        try:
            q = data.get("Global Quote", {})
            if not q:
                return None
            return {
                "price": float(q.get("05. price", 0) or 0),
                "change": float(q.get("09. change", 0) or 0),
                "change_percent": float(str(q.get("10. change percent", "0")).rstrip("%") or 0),
                "volume": int(q.get("06. volume", 0) or 0),
                "timestamp": q.get("07. latest trading day"),
                "data_source": "alpha_vantage",
            }
        except Exception as e:
            logger.error(f"Error parsing Alpha Vantage: {e}")
            return None

    def _parse_finnhub_quote(self, data: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        try:
            ts = data.get("t")
            timestamp = datetime.datetime.fromtimestamp(ts).isoformat() if ts else None
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
        except Exception as e:
            logger.error(f"Error parsing Finnhub: {e}")
            return None

    def _parse_twelvedata_quote(self, data: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        try:
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
        except Exception as e:
            logger.error(f"Error parsing TwelveData: {e}")
            return None

financial_client = FinancialDataClient()

# =============================================================================
# Pydantic models
# =============================================================================

class Quote(BaseModel):
    ticker: str = Field(..., description="Stock ticker symbol", example="7201.SR")
    company: Optional[str] = Field(None, description="Company name")
    currency: Optional[str] = Field(None, description="Currency code", example="SAR")
    price: Optional[float] = Field(None, ge=0)
    previous_close: Optional[float] = Field(None, ge=0)
    day_change_pct: Optional[float] = None
    market_cap: Optional[float] = Field(None, ge=0)
    volume: Optional[float] = Field(None, ge=0)
    fifty_two_week_high: Optional[float] = Field(None, ge=0)
    fifty_two_week_low: Optional[float] = Field(None, ge=0)
    timestamp_utc: Optional[str] = None
    data_source: Optional[str] = None

    @validator("ticker")
    def validate_ticker(cls, v: str) -> str:
        v = v.strip()
        if not v:
            raise ValueError("Ticker cannot be empty")
        return v.upper()

class QuoteUpdatePayload(BaseModel):
    data: List[Quote]

class QuoteResponse(BaseModel):
    data: List[Quote]

class SheetVerificationResult(BaseModel):
    sheet_name: str
    status: str
    row_count: int
    column_count: int
    headers: List[str]
    sample_data: List[Dict[str, Any]]
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
    data: List[Dict[str, Any]]

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

# =============================================================================
# TTL Cache
# =============================================================================

class TTLCache:
    def __init__(self, ttl_minutes: int = 30):
        self.cache_path = BASE_DIR / "quote_cache.json"
        self.backup_dir = BASE_DIR / "cache_backups"
        self.backup_dir.mkdir(parents=True, exist_ok=True)
        self.data: Dict[str, Dict[str, Any]] = {}
        self.ttl = ttl_minutes * 60
        self.load_cache()

    def _is_valid(self, item: Dict[str, Any], now: Optional[float] = None) -> bool:
        if now is None:
            now = datetime.datetime.now().timestamp()
        ts = item.get("_cache_timestamp", 0)
        return (now - ts) < self.ttl

    def load_cache(self) -> bool:
        try:
            if not self.cache_path.exists():
                logger.info("No cache file found, starting empty")
                return True

            with open(self.cache_path, "r", encoding="utf-8") as f:
                raw = json.load(f)

            now = datetime.datetime.now().timestamp()
            valid: Dict[str, Dict[str, Any]] = {}

            if isinstance(raw, dict) and "data" in raw:
                for it in raw["data"]:
                    if isinstance(it, dict) and "ticker" in it and self._is_valid(it, now):
                        valid[it["ticker"]] = it
            elif isinstance(raw, dict):
                for ticker, it in raw.items():
                    if self._is_valid(it, now):
                        valid[ticker] = it

            self.data = valid
            logger.info(f"✅ Cache loaded with {len(self.data)} valid items")
            return True
        except Exception as e:
            logger.error(f"❌ Failed to load cache: {e}")
            self.data = {}
            return False

    def save_cache(self) -> bool:
        try:
            now = datetime.datetime.now().timestamp()
            temp = {
                "data": [
                    {**v, "_cache_timestamp": v.get("_cache_timestamp", now)}
                    for v in self.data.values()
                ]
            }
            tmp_path = self.cache_path.with_suffix(".tmp")
            with open(tmp_path, "w", encoding="utf-8") as f:
                json.dump(temp, f, indent=2, ensure_ascii=False)
            tmp_path.replace(self.cache_path)
            logger.info(f"✅ Cache saved with {len(self.data)} items")
            return True
        except Exception as e:
            logger.error(f"❌ Failed to save cache: {e}")
            return False

    def create_backup(self) -> Optional[str]:
        try:
            if not self.data:
                return None
            ts = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
            path = self.backup_dir / f"quote_cache_{ts}.json"
            with open(path, "w", encoding="utf-8") as f:
                json.dump({"data": list(self.data.values())}, f, indent=2, ensure_ascii=False)
            logger.info(f"✅ Backup created: {path}")
            return str(path)
        except Exception as e:
            logger.error(f"❌ Backup failed: {e}")
            return None

    def get_quote(self, ticker: str) -> Optional[Quote]:
        ticker = ticker.upper()
        if ticker in self.data and self._is_valid(self.data[ticker]):
            clean = {k: v for k, v in self.data[ticker].items() if not k.startswith("_")}
            return Quote(**clean)
        if ticker in self.data:
            del self.data[ticker]
            self.save_cache()
        return None

    def update_quotes(self, quotes: List[Quote]) -> Tuple[int, List[str]]:
        updated = 0
        errors: List[str] = []
        now = datetime.datetime.now().timestamp()

        for q in quotes:
            try:
                d = q.dict()
                d["_cache_timestamp"] = now
                self.data[q.ticker] = d
                updated += 1
            except Exception as e:
                errors.append(f"Failed to update {q.ticker}: {e}")

        if updated:
            self.save_cache()
        return updated, errors

    def cleanup_expired(self) -> int:
        now = datetime.datetime.now().timestamp()
        expired = [k for k, v in self.data.items() if not self._is_valid(v, now)]
        for k in expired:
            del self.data[k]
        if expired:
            self.save_cache()
            logger.info(f"🧹 Removed {len(expired)} expired cache entries")
        return len(expired)

ttl_seconds = int(os.getenv("CACHE_DEFAULT_TTL", "1800"))
cache = TTLCache(ttl_minutes=max(1, ttl_seconds // 60))

# =============================================================================
# FastAPI app & lifespan
# =============================================================================

@asynccontextmanager
async def lifespan(app: FastAPI):
    startup_time = datetime.datetime.utcnow()
    logger.info(f"🚀 Starting {config.service_name} v{config.service_version}")

    # Validate configuration
    errors: List[str] = []
    required_env = ["SPREADSHEET_ID"]
    for var in required_env:
        if not os.getenv(var):
            errors.append(f"Missing required environment variable: {var}")

    has_sa_json = bool(config.google_service_account_json)
    if not has_sa_json:
        logger.warning("GOOGLE_SERVICE_ACCOUNT_JSON not configured. Using Apps Script only mode.")
    else:
        try:
            sheets_status = GoogleSheetsManager.test_connection()
            if sheets_status.get("status") != "SUCCESS":
                errors.append(f"Google Sheets connection failed: {sheets_status.get('message')}")
        except Exception as e:
            errors.append(f"Google Sheets validation error: {e}")

    api_status = financial_client.get_api_status()
    if not any(api_status.values()):
        logger.warning("No financial APIs configured. Only cache + Google Sheets/Apps Script will be used.")

    if errors:
        logger.error("Configuration validation failed:")
        for e in errors:
            logger.error("  - " + e)
        if config.environment != "production":
            raise ConfigurationError("Application configuration invalid")
    else:
        logger.info("✅ All configuration validations passed")

    yield

    try:
        await financial_client.close()
    except Exception as e:
        logger.warning(f"⚠️ Error closing financial client: {e}")
    
    uptime = datetime.datetime.utcnow() - startup_time
    logger.info(f"🛑 Shutting down after {uptime}")

app = FastAPI(
    title=config.service_name,
    version=config.service_version,
    description="Tadawul Stock Analysis API 📈",
    docs_url="/docs" if config.enable_swagger else None,
    redoc_url="/redoc" if config.enable_redoc else None,
    lifespan=lifespan,
)

app.state.limiter = limiter
app.add_exception_handler(RateLimitExceeded, _rate_limit_exceeded_handler)

app.add_middleware(
    CORSMiddleware,
    allow_origins=config.cors_origins if config.cors_origins != ["*"] else ["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# =============================================================================
# Exception handlers
# =============================================================================

@app.exception_handler(HTTPException)
async def http_exception_handler(request: Request, exc: HTTPException):
    logger.warning(f"HTTP {exc.status_code}: {exc.detail}")
    return JSONResponse(
        status_code=exc.status_code,
        content=ErrorResponse(
            error=exc.detail,
            timestamp=datetime.datetime.utcnow().isoformat() + "Z",
        ).dict(),
    )

@app.exception_handler(Exception)
async def global_exception_handler(request: Request, exc: Exception):
    logger.error(f"Unhandled exception: {exc}", exc_info=True)
    return JSONResponse(
        status_code=500,
        content=ErrorResponse(
            error="Internal server error",
            detail=str(exc) if config.environment != "production" else None,
            timestamp=datetime.datetime.utcnow().isoformat() + "Z",
        ).dict(),
    )

# =============================================================================
# Core endpoints - FIXED AUTHENTICATION
# =============================================================================

@app.get("/", response_model=Dict[str, Any])
@rate_limit(f"{config.max_requests_per_minute}/minute")
async def root(request: Request):
    """Public root endpoint - no authentication required"""
    has_sa_json = bool(config.google_service_account_json)
    if has_sa_json:
        try:
            sheets_status = GoogleSheetsManager.test_connection()
            connected = sheets_status.get("status") == "SUCCESS"
            spreadsheet_title = sheets_status.get("spreadsheet_title")
            total_sheets = sheets_status.get("total_sheets", 0)
        except Exception:
            connected = False
            spreadsheet_title = None
            total_sheets = 0
    else:
        connected = False
        spreadsheet_title = None
        total_sheets = 0

    api_status = financial_client.get_api_status()

    return {
        "service": config.service_name,
        "version": config.service_version,
        "status": "operational",
        "timestamp": datetime.datetime.utcnow().isoformat() + "Z",
        "environment": config.environment,
        "authentication_required": config.require_auth,
        "rate_limiting": config.enable_rate_limiting,
        "google_sheets": {
            "connected": connected,
            "mode": "direct_gspread" if has_sa_json else "apps_script_or_external_only",
            "spreadsheet": spreadsheet_title,
            "total_sheets": total_sheets,
        },
        "financial_apis": {k: "configured" if v else "not_configured" for k, v in api_status.items()},
        "cache": {
            "items": len(cache.data),
            "file_exists": cache.cache_path.exists(),
            "ttl_minutes": cache.ttl // 60,
        },
    }

@app.get("/health", response_model=HealthResponse)
@rate_limit("30/minute")
async def health_check(request: Request):
    """Public health check endpoint - no authentication required"""
    has_sa_json = bool(config.google_service_account_json)
    if has_sa_json:
        try:
            sheets_status = GoogleSheetsManager.test_connection()
            google_connected = sheets_status.get("status") == "SUCCESS"
            health_status = "healthy" if google_connected else "degraded"
        except Exception:
            google_connected = False
            health_status = "degraded"
    else:
        google_connected = False
        health_status = "healthy"  # Still healthy because we're in Apps Script mode

    api_status = financial_client.get_api_status()
    
    # Safe module imports
    HAS_DASHBOARD = False
    HAS_ARGAAM_ROUTES = False
    HAS_GOOGLE_APPS_SCRIPT = False
    
    try:
        from advanced_market_dashboard import AdvancedMarketDashboard
        HAS_DASHBOARD = True
    except ImportError:
        pass
        
    try:
        from routes_argaam import router as argaam_router
        HAS_ARGAAM_ROUTES = True
    except ImportError:
        pass
        
    try:
        from google_apps_script_client import google_apps_script_client
        HAS_GOOGLE_APPS_SCRIPT = True
    except ImportError:
        pass

    return HealthResponse(
        status=health_status,
        time_utc=datetime.datetime.utcnow().isoformat() + "Z",
        version=config.service_version,
        google_sheets_connected=google_connected,
        cache_status={
            "items": len(cache.data),
            "file_exists": cache.cache_path.exists(),
            "last_updated": cache.cache_path.stat().st_mtime if cache.cache_path.exists() else None,
            "ttl_minutes": cache.ttl // 60,
        },
        features={
            "dashboard": HAS_DASHBOARD,
            "argaam_routes": HAS_ARGAAM_ROUTES,
            "google_apps_script": HAS_GOOGLE_APPS_SCRIPT,
        },
        api_status=api_status,
    )

@app.get("/v1/ping", response_model=Dict[str, Any])
@rate_limit("120/minute")
async def ping(request: Request):
    """Public ping endpoint - no authentication required"""
    return {
        "status": "ok",
        "service": config.service_name,
        "version": config.service_version,
        "timestamp": datetime.datetime.utcnow().isoformat() + "Z",
    }

# =============================================================================
# Protected endpoints (require authentication)
# =============================================================================

@app.get("/verify-sheets", response_model=VerificationResponse)
@rate_limit("10/minute")
async def verify_all_sheets(request: Request, auth: bool = Depends(verify_auth)):
    """Protected endpoint - requires authentication"""
    if not config.google_service_account_json:
        raise HTTPException(
            status_code=400,
            detail="GOOGLE_SERVICE_ACCOUNT_JSON not configured; direct Sheets verification is disabled.",
        )

    try:
        spreadsheet = GoogleSheetsManager.get_spreadsheet()
        results: List[SheetVerificationResult] = []
        for name in EXPECTED_SHEETS:
            try:
                ws = spreadsheet.worksheet(name)
                records = ws.get_all_records()
                headers = ws.row_values(1) or []
                results.append(SheetVerificationResult(
                    sheet_name=name,
                    status="OK",
                    row_count=len(records) + 1,
                    column_count=len(headers),
                    headers=headers,
                    sample_data=records[:3],
                ))
            except Exception as e:
                results.append(SheetVerificationResult(
                    sheet_name=name,
                    status="ERROR",
                    row_count=0,
                    column_count=0,
                    headers=[],
                    sample_data=[],
                    error=str(e),
                ))
        
        sheets_ok = sum(1 for r in results if r.status == "OK")
        overall_status = "SUCCESS" if sheets_ok == len(EXPECTED_SHEETS) else "PARTIAL_SUCCESS"
        
        return VerificationResponse(
            spreadsheet_id=GOOGLE_SERVICES["spreadsheet_id"],
            overall_status=overall_status,
            sheets_verified=len(EXPECTED_SHEETS),
            sheets_ok=sheets_ok,
            details=results,
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error verifying sheets: {e}")

@app.get("/sheet/{sheet_name}", response_model=SheetDataResponse)
@rate_limit("30/minute")
async def read_sheet_data(
    request: Request,
    sheet_name: str,
    limit: int = Query(10, ge=1, le=1000),
    auth: bool = Depends(verify_auth),
):
    """Protected endpoint - requires authentication"""
    if not config.google_service_account_json:
        raise HTTPException(
            status_code=400,
            detail="GOOGLE_SERVICE_ACCOUNT_JSON not configured; direct sheet access is disabled.",
        )

    try:
        spreadsheet = GoogleSheetsManager.get_spreadsheet()
        ws = spreadsheet.worksheet(sheet_name)
        records = ws.get_all_records()
        return SheetDataResponse(
            sheet_name=sheet_name, 
            total_records=len(records), 
            data=records[:limit]
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error reading sheet: {e}")

# Add other protected endpoints here...
# All endpoints below this line should have `auth: bool = Depends(verify_auth)` parameter

@app.get("/v1/quote", response_model=QuoteResponse)
@rate_limit("60/minute")
async def get_quotes(
    request: Request,
    tickers: str = Query(..., description="Comma-separated ticker symbols"),
    auth: bool = Depends(verify_auth),
):
    """Protected endpoint - requires authentication"""
    symbols = [t.strip().upper() for t in tickers.split(",") if t.strip()]
    if not symbols:
        raise HTTPException(status_code=400, detail="No tickers provided")

    quotes: List[Quote] = []
    for sym in symbols:
        q = cache.get_quote(sym)
        if q:
            quotes.append(q)
        else:
            quotes.append(Quote(ticker=sym, timestamp_utc=datetime.datetime.utcnow().isoformat() + "Z"))

    return QuoteResponse(data=quotes)

@app.post("/v1/quote/update", response_model=Dict[str, Any])
@rate_limit("30/minute")
async def update_quotes(
    request: Request,
    payload: QuoteUpdatePayload,
    autosave: bool = Query(True),
    auth: bool = Depends(verify_auth),
):
    """Protected endpoint - requires authentication"""
    updated, errors = cache.update_quotes(payload.data)
    logger.info(f"Quotes updated: {updated} successful, {len(errors)} errors")
    return {
        "status": "completed",
        "updated_count": updated,
        "error_count": len(errors),
        "errors": errors or None,
        "total_cached": len(cache.data),
        "autosaved": autosave,
    }

# =============================================================================
# v4.1 Compatibility endpoints
# =============================================================================

@app.get("/v41/quotes")
@rate_limit("60/minute")
async def v41_get_quotes(
    request: Request,
    symbols: str = Query(..., description="Comma-separated tickers"),
    cache_ttl: int = Query(60, ge=0),
    auth: bool = Depends(verify_auth),
):
    """Protected endpoint - requires authentication"""
    tickers = [t.strip().upper() for t in symbols.split(",") if t.strip()]
    if not tickers:
        raise HTTPException(status_code=400, detail="No symbols provided")

    data: List[Dict[str, Any]] = []
    now_ts = datetime.datetime.utcnow().isoformat() + "Z"

    for sym in tickers:
        q = cache.get_quote(sym)
        if q:
            data.append({
                "ticker": q.ticker,
                "price": q.price,
                "previous_close": q.previous_close,
                "day_change_pct": q.day_change_pct,
                "currency": q.currency,
                "timestamp_utc": q.timestamp_utc,
                "data_source": q.data_source,
            })
        else:
            data.append({
                "ticker": sym,
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
        "timestamp_utc": now_ts,
        "data": data,
        "quotes": data,
    }

@app.post("/v41/quotes")
@rate_limit("30/minute")
async def v41_post_quotes(
    request: Request,
    payload: QuoteUpdatePayload,
    auth: bool = Depends(verify_auth),
):
    """Protected endpoint - requires authentication"""
    updated, errors = cache.update_quotes(payload.data)
    now_ts = datetime.datetime.utcnow().isoformat() + "Z"
    return {
        "ok": True,
        "updated": updated,
        "errors": errors or [],
        "total_cached": len(cache.data),
        "timestamp_utc": now_ts,
    }

# =============================================================================
# Optional Argaam routes
# =============================================================================

try:
    from routes_argaam import router as argaam_router
    app.include_router(argaam_router)
    logger.info("✅ Argaam routes mounted successfully")
except ImportError:
    logger.warning("⚠️ Argaam routes not available")

# =============================================================================
# Entry point
# =============================================================================

if __name__ == "__main__":
    server_config = {
        "app": "main:app",
        "host": config.app_host,
        "port": config.app_port,
        "log_level": config.log_level.lower(),
        "access_log": True,
        "reload": config.environment == "development",
    }

    logger.info(f"🚀 Starting {config.service_name} v{config.service_version}")
    logger.info(f"🔐 Authentication: {'Enabled' if config.require_auth else 'Disabled'}")
    logger.info(f"⚡ Rate Limiting: {'Enabled' if config.enable_rate_limiting else 'Disabled'}")
    logger.info(f"🌍 Environment: {config.environment}")

    uvicorn.run(**server_config)
