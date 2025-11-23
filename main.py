from __future__ import annotations

import datetime
import json
import os
import logging
import asyncio
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple, Callable

import aiohttp
import gspread
import numpy as np  # currently unused, but kept for future analytics
import pandas as pd  # currently unused, but kept for future analytics
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
from pydantic import BaseModel, BaseSettings, Field, validator
from slowapi import Limiter, _rate_limit_exceeded_handler
from slowapi.errors import RateLimitExceeded
from slowapi.util import get_remote_address
import uvicorn

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
    # If CORS_ORIGINS="*" in env, this will become ["*"]
    cors_origins: List[str] = Field(default_factory=lambda: ["*"], env="CORS_ORIGINS")

    # Logging / Docs
    log_enable_file: bool = Field(LOG_ENABLE_FILE, env="LOG_ENABLE_FILE")
    log_level: str = Field("INFO", env="LOG_LEVEL")
    enable_swagger: bool = Field(True, env="ENABLE_SWAGGER")
    enable_redoc: bool = Field(True, env="ENABLE_REDOC")

    # Google Sheets
    spreadsheet_id: str = Field(..., env="SPREADSHEET_ID")
    google_sheets_credentials: str = Field(..., env="GOOGLE_SHEETS_CREDENTIALS")
    google_apps_script_url: str = Field(..., env="GOOGLE_APPS_SCRIPT_URL")
    google_apps_script_backup_url: Optional[str] = Field(
        None, env="GOOGLE_APPS_SCRIPT_BACKUP_URL"
    )

    # Financial APIs
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

# Sheets scopes
SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]

# Your 9-page Ultimate Investment Dashboard sheet names
EXPECTED_SHEETS = [
    "Market_Leaders",
    "global_markets",
    "my_portfolio",
    "mutual_funds",
    "commodities_fx",
    "insights_analysis",
    "economic_calendar",
    "Portfolio_Reports",
    "Investment_Advisor",
]

# Financial APIs (read from env)
FINANCIAL_APIS: Dict[str, Dict[str, Any]] = {
    "alpha_vantage": {
        "api_key": config.alpha_vantage_api_key,
        "base_url": os.getenv(
            "ALPHA_VANTAGE_BASE_URL", "https://www.alphavantage.co/query"
        ),
        "timeout": int(os.getenv("ALPHA_VANTAGE_TIMEOUT", "15")),
    },
    "finnhub": {
        "api_key": config.finnhub_api_key,
        "base_url": os.getenv("FINNHUB_BASE_URL", "https://finnhub.io/api/v1"),
        "timeout": int(os.getenv("FINNHUB_TIMEOUT", "15")),
    },
    "eodhd": {
        "api_key": config.eodhd_api_key,
        "base_url": os.getenv(
            "EODHD_BASE_URL", "https://eodhistoricaldata.com/api"
        ),
        "timeout": int(os.getenv("EODHD_TIMEOUT", "15")),
    },
    "twelvedata": {
        "api_key": config.twelvedata_api_key,
        "base_url": os.getenv("TWELVEDATA_BASE_URL", "https://api.twelvedata.com"),
        "timeout": int(os.getenv("TWELVEDATA_TIMEOUT", "15")),
    },
    "marketstack": {
        "api_key": config.marketstack_api_key,
        "base_url": os.getenv(
            "MARKETSTACK_BASE_URL", "http://api.marketstack.com/v1"
        ),
        "timeout": int(os.getenv("MARKETSTACK_TIMEOUT", "15")),
    },
    "fmp": {
        "api_key": config.fmp_api_key,
        "base_url": os.getenv(
            "FMP_BASE_URL", "https://financialmodelingprep.com/api/v3"
        ),
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

# Rate limiter (can be disabled by config)
limiter = Limiter(key_func=get_remote_address)


def rate_limit(rule: str):
    """Wrapper to optionally disable rate limiting from config."""
    if not config.enable_rate_limiting:
        # no-op decorator
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
# Auth dependency
# =============================================================================


def verify_auth(credentials: Optional[HTTPAuthorizationCredentials] = Depends(security)):
    """
    Authentication dependency.

    - Reads APP_TOKEN / BACKUP_APP_TOKEN
    - If REQUIRE_AUTH=false or no tokens configured => auth is effectively disabled
    """
    if not config.require_auth:
        return True

    valid_tokens = [t for t in [config.app_token, config.backup_app_token] if t]

    # If no tokens configured, do NOT block the API
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
            creds_json = config.google_sheets_credentials
            if not creds_json:
                raise GoogleSheetsError("GOOGLE_SHEETS_CREDENTIALS not configured")

            creds_dict = json.loads(creds_json)
            creds = Credentials.from_service_account_info(creds_dict, scopes=SCOPES)
            cls._client = gspread.authorize(creds)
            logger.info("✅ Google Sheets client initialized successfully")
            return cls._client
        except json.JSONDecodeError as e:
            logger.error(f"❌ Invalid GOOGLE_SHEETS_CREDENTIALS JSON: {e}")
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
                    logger.info(
                        f"✅ Successfully fetched {symbol} from {api_name} (attempt {attempt+1})"
                    )
                    return result
                logger.warning(
                    f"⚠️ No data returned for {symbol} from {api_name} (attempt {attempt+1})"
                )
            except Exception as e:
                logger.warning(f"⚠️ Attempt {attempt+1} failed for {api_name}: {e}")
                if attempt < max_retries - 1:
                    await asyncio.sleep(2**attempt)
        return None

    async def get_stock_quote(
        self, symbol: str, api_name: str = "alpha_vantage"
    ) -> Optional[Dict[str, Any]]:
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
                async with session.get(
                    api_cfg["base_url"], params=params, timeout=api_cfg["timeout"]
                ) as r:
                    if r.status == 200:
                        data = await r.json()
                        return self._parse_alpha_vantage_quote(data)
                    logger.error(f"Alpha Vantage returned status {r.status}")

            elif api_name == "finnhub":
                params = {"symbol": symbol, "token": api_cfg["api_key"]}
                async with session.get(
                    f"{api_cfg['base_url']}/quote",
                    params=params,
                    timeout=api_cfg["timeout"],
                ) as r:
                    if r.status == 200:
                        data = await r.json()
                        return self._parse_finnhub_quote(data)
                    logger.error(f"Finnhub returned status {r.status}")

            elif api_name == "twelvedata":
                params = {"symbol": symbol, "apikey": api_cfg["api_key"]}
                async with session.get(
                    f"{api_cfg['base_url']}/quote",
                    params=params,
                    timeout=api_cfg["timeout"],
                ) as r:
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
                "change_percent": float(
                    str(q.get("10. change percent", "0")).rstrip("%") or 0
                ),
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
            timestamp = (
                datetime.datetime.fromtimestamp(ts).isoformat() if ts else None
            )
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
                    if isinstance(it, dict) and "ticker" in it and self._is_valid(
                        it, now
                    ):
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
                json.dump(
                    {"data": list(self.data.values())},
                    f,
                    indent=2,
                    ensure_ascii=False,
                )
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


# Use CACHE_DEFAULT_TTL from env if present (seconds)
ttl_seconds = int(os.getenv("CACHE_DEFAULT_TTL", "1800"))  # default 30 minutes
cache = TTLCache(ttl_minutes=max(1, ttl_seconds // 60))

# =============================================================================
# Sheet helpers
# =============================================================================


def verify_sheet_structure(sheet_name: str) -> SheetVerificationResult:
    try:
        spreadsheet = GoogleSheetsManager.get_spreadsheet()
        ws = spreadsheet.worksheet(sheet_name)
        records = ws.get_all_records()
        headers = ws.row_values(1) or []
        return SheetVerificationResult(
            sheet_name=sheet_name,
            status="OK",
            row_count=len(records) + 1,
            column_count=len(headers),
            headers=headers,
            sample_data=records[:3],
        )
    except gspread.WorksheetNotFound:
        return SheetVerificationResult(
            sheet_name=sheet_name,
            status="NOT_FOUND",
            row_count=0,
            column_count=0,
            headers=[],
            sample_data=[],
            error=f"Worksheet '{sheet_name}' not found",
        )
    except Exception as e:
        return SheetVerificationResult(
            sheet_name=sheet_name,
            status="ERROR",
            row_count=0,
            column_count=0,
            headers=[],
            sample_data=[],
            error=str(e),
        )


def fetch_sheet_data(sheet_name: str, limit: int = 10) -> SheetDataResponse:
    """Internal helper (renamed to avoid recursion with endpoint)."""
    try:
        spreadsheet = GoogleSheetsManager.get_spreadsheet()
        ws = spreadsheet.worksheet(sheet_name)
        records = ws.get_all_records()
        return SheetDataResponse(
            sheet_name=sheet_name, total_records=len(records), data=records[:limit]
        )
    except gspread.WorksheetNotFound:
        raise HTTPException(status_code=404, detail=f"Sheet '{sheet_name}' not found")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error reading sheet: {e}")


# =============================================================================
# Startup validation
# =============================================================================


async def validate_configuration():
    errors: List[str] = []

    for var in ["SPREADSHEET_ID", "GOOGLE_SHEETS_CREDENTIALS"]:
        if not os.getenv(var):
            errors.append(f"Missing required environment variable: {var}")

    try:
        sheets_status = GoogleSheetsManager.test_connection()
        if sheets_status.get("status") != "SUCCESS":
            errors.append(
                f"Google Sheets connection failed: {sheets_status.get('message')}"
            )
    except Exception as e:
        errors.append(f"Google Sheets validation error: {e}")

    api_status = financial_client.get_api_status()
    if not any(api_status.values()):
        errors.append(
            "No financial APIs configured. At least one API key is recommended."
        )

    if errors:
        logger.error("Configuration validation failed:")
        for e in errors:
            logger.error("  - " + e)
        # Do not raise in production: continue but log
        if config.environment != "production":
            raise ConfigurationError("Application configuration invalid")
    else:
        logger.info("✅ All configuration validations passed")


# =============================================================================
# Optional module imports (safe_import)
# =============================================================================


def safe_import(module_name: str, attr: Optional[str] = None):
    try:
        module = __import__(module_name)
        if attr:
            return getattr(module, attr)
        return module
    except ImportError as e:
        logger.warning(f"⚠️ {module_name} not available: {e}")
        return None
    except Exception as e:
        logger.error(f"❌ Error loading {module_name}: {e}")
        return None


AdvancedMarketDashboard = safe_import(
    "advanced_market_dashboard", "AdvancedMarketDashboard"
)
argaam_router = safe_import("routes_argaam", "router")
close_argaam_http_client = safe_import("routes_argaam", "close_argaam_http_client")
google_apps_script_client = safe_import(
    "google_apps_script_client", "google_apps_script_client"
)
sr = safe_import("symbols_reader")
analyzer = safe_import("advanced_analysis", "analyzer")

HAS_DASHBOARD = AdvancedMarketDashboard is not None
HAS_ARGAAM_ROUTES = argaam_router is not None
HAS_GOOGLE_APPS_SCRIPT = google_apps_script_client is not None
HAS_SYMBOLS_READER = sr is not None
HAS_ADVANCED_ANALYSIS = analyzer is not None

# =============================================================================
# FastAPI app & lifespan
# =============================================================================


@asynccontextmanager
async def lifespan(app: FastAPI):
    startup_time = datetime.datetime.utcnow()
    logger.info(f"🚀 Starting {config.service_name} v{config.service_version}")
    await validate_configuration()
    sheets_status = GoogleSheetsManager.test_connection()
    if sheets_status.get("status") == "SUCCESS":
        logger.info("✅ Google Sheets connection verified")
    else:
        logger.warning(
            f"⚠️ Google Sheets connection issue: {sheets_status.get('message')}"
        )
    expired = cache.cleanup_expired()
    if expired:
        logger.info(f"🧹 Cleaned {expired} expired cache entries on startup")
    yield
    try:
        if HAS_ARGAAM_ROUTES and close_argaam_http_client:
            await close_argaam_http_client()
            logger.info("✅ Argaam HTTP client closed")
    except Exception as e:
        logger.warning(f"⚠️ Error closing Argaam client: {e}")
    await financial_client.close()
    uptime = datetime.datetime.utcnow() - startup_time
    logger.info(f"🛑 Shutting down after {uptime}")


app = FastAPI(
    title=config.service_name,
    version=config.service_version,
    description="""
    Tadawul Stock Analysis API 📈

    - Google Sheets integration (9 pages)
    - Multi-source financial data
    - Cache with TTL
    - Health & diagnostics
    """,
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
# Core endpoints
# =============================================================================


@app.get("/", response_model=Dict[str, Any])
@rate_limit(f"{config.max_requests_per_minute}/minute")
async def root(request: Request, auth: bool = Depends(verify_auth)):
    sheets_status = GoogleSheetsManager.test_connection()
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
            "connected": sheets_status.get("status") == "SUCCESS",
            "spreadsheet": sheets_status.get("spreadsheet_title"),
            "total_sheets": sheets_status.get("total_sheets", 0),
        },
        "financial_apis": api_status,
        "cache": {
            "items": len(cache.data),
            "file_exists": cache.cache_path.exists(),
            "ttl_minutes": cache.ttl // 60,
        },
    }


@app.get("/health", response_model=HealthResponse)
@rate_limit("30/minute")
async def health_check(request: Request):
    sheets_status = GoogleSheetsManager.test_connection()
    api_status = financial_client.get_api_status()
    health_status = "healthy" if sheets_status.get("status") == "SUCCESS" else "degraded"
    return HealthResponse(
        status=health_status,
        time_utc=datetime.datetime.utcnow().isoformat() + "Z",
        version=config.service_version,
        google_sheets_connected=sheets_status.get("status") == "SUCCESS",
        cache_status={
            "items": len(cache.data),
            "file_exists": cache.cache_path.exists(),
            "last_updated": cache.cache_path.stat().st_mtime
            if cache.cache_path.exists()
            else None,
            "ttl_minutes": cache.ttl // 60,
        },
        features={
            "dashboard": HAS_DASHBOARD,
            "argaam_routes": HAS_ARGAAM_ROUTES,
            "google_apps_script": HAS_GOOGLE_APPS_SCRIPT,
        },
        api_status=api_status,
    )


# =============================================================================
# Sheets endpoints
# =============================================================================


@app.get("/verify-sheets", response_model=VerificationResponse)
@rate_limit("10/minute")
async def verify_all_sheets(request: Request, auth: bool = Depends(verify_auth)):
    spreadsheet = GoogleSheetsManager.get_spreadsheet()
    results: List[SheetVerificationResult] = []
    for name in EXPECTED_SHEETS:
        r = verify_sheet_structure(name)
        results.append(r)
        logger.info(f"Verified {name}: {r.status}")
    sheets_ok = sum(1 for r in results if r.status == "OK")
    overall_status = (
        "SUCCESS" if sheets_ok == len(EXPECTED_SHEETS) else "PARTIAL_SUCCESS"
    )
    return VerificationResponse(
        spreadsheet_id=GOOGLE_SERVICES["spreadsheet_id"],
        overall_status=overall_status,
        sheets_verified=len(EXPECTED_SHEETS),
        sheets_ok=sheets_ok,
        details=results,
    )


@app.get("/sheet/{sheet_name}", response_model=SheetDataResponse)
@rate_limit("30/minute")
async def read_sheet_data(
    request: Request,
    sheet_name: str,
    limit: int = Query(10, ge=1, le=1000),
    auth: bool = Depends(verify_auth),
):
    return fetch_sheet_data(sheet_name, limit)


@app.get("/sheet-names", response_model=Dict[str, Any])
@rate_limit("20/minute")
async def get_sheet_names(request: Request, auth: bool = Depends(verify_auth)):
    try:
        spreadsheet = GoogleSheetsManager.get_spreadsheet()
        worksheets = spreadsheet.worksheets()
        available = [ws.title for ws in worksheets]
        return {
            "spreadsheet_title": spreadsheet.title,
            "available_sheets": available,
            "expected_sheets": EXPECTED_SHEETS,
            "missing_sheets": list(set(EXPECTED_SHEETS) - set(available)),
            "extra_sheets": list(set(available) - set(EXPECTED_SHEETS)),
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error getting sheet names: {e}")


@app.get("/test-connection", response_model=Dict[str, Any])
@rate_limit("10/minute")
async def test_connection(request: Request, auth: bool = Depends(verify_auth)):
    return GoogleSheetsManager.test_connection()


# =============================================================================
# Financial APIs status & data
# =============================================================================


@app.get("/v1/financial-apis/status", response_model=Dict[str, Any])
@rate_limit("15/minute")
async def get_financial_apis_status(request: Request, auth: bool = Depends(verify_auth)):
    api_status = financial_client.get_api_status()
    tests: List[Dict[str, Any]] = []
    for api_name, configured in api_status.items():
        if not configured:
            continue
        try:
            start = datetime.datetime.now()
            data = await financial_client.get_stock_quote_with_retry("AAPL", api_name)
            elapsed = (datetime.datetime.now() - start).total_seconds()
            if data:
                tests.append(
                    {
                        "api_name": api_name,
                        "status": "SUCCESS",
                        "response_time": round(elapsed, 3),
                    }
                )
            else:
                tests.append(
                    {
                        "api_name": api_name,
                        "status": "ERROR",
                        "error": "No data returned after retries",
                    }
                )
        except Exception as e:
            tests.append({"api_name": api_name, "status": "ERROR", "error": str(e)})

    return {
        "api_configuration": api_status,
        "api_tests": tests,
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
    if api not in FINANCIAL_APIS:
        raise HTTPException(status_code=400, detail=f"API {api} not supported")
    data = await financial_client.get_stock_quote_with_retry(symbol, api)
    if not data:
        raise HTTPException(
            status_code=404, detail=f"No data found for {symbol} from {api}"
        )
    return {
        "symbol": symbol,
        "api": api,
        "data": data,
        "timestamp": datetime.datetime.utcnow().isoformat() + "Z",
    }


# =============================================================================
# Market data endpoints (KSA)
# =============================================================================


@app.get("/api/saudi/symbols", response_model=Dict[str, Any])
@rate_limit("30/minute")
async def get_saudi_symbols(
    request: Request,
    limit: int = Query(20, ge=1, le=500),
    auth: bool = Depends(verify_auth),
):
    """
    Get Saudi symbols.
    Primary: symbols_reader.fetch_symbols()
    Fallback: read from Market_Leaders (Ticker, Company Name, Sector, Trading Market)
    """
    try:
        if HAS_SYMBOLS_READER and hasattr(sr, "fetch_symbols"):
            payload = sr.fetch_symbols(limit)
            if payload and isinstance(payload, dict):
                return payload

        # Fallback – from Market_Leaders sheet
        sheet_data = fetch_sheet_data("Market_Leaders", limit=1000)
        symbols: List[Dict[str, Any]] = []
        for row in sheet_data.data:
            ticker = row.get("Ticker") or row.get("ticker")
            company = (
                row.get("Company Name") or row.get("Company") or row.get("company")
            )
            sector = row.get("Sector")
            trading_market = row.get("Trading Market") or row.get("Market")
            if ticker:
                symbols.append(
                    {
                        "symbol": str(ticker).strip(),
                        "company_name": company,
                        "sector": sector,
                        "trading_market": trading_market,
                    }
                )

        return {
            "data": symbols[:limit],
            "count": min(len(symbols), limit),
            "source": "google_sheets_Market_Leaders",
        }
    except Exception as e:
        logger.error(f"Failed to fetch KSA symbols: {e}")
        return {"data": [], "count": 0, "error": str(e), "source": "error"}


@app.get("/api/saudi/market", response_model=Dict[str, Any])
@rate_limit("30/minute")
async def get_saudi_market(
    request: Request,
    limit: int = Query(20, ge=1, le=500),
    auth: bool = Depends(verify_auth),
):
    symbols_payload = await get_saudi_symbols(request, limit=limit, auth=auth)
    symbols = symbols_payload.get("data", [])
    market_data: List[Dict[str, Any]] = []

    for sym in symbols:
        symbol = sym.get("symbol")
        if not symbol:
            continue
        q = cache.get_quote(symbol)
        market_data.append(
            {
                **sym,
                "price": q.price if q else None,
                "change_percent": q.day_change_pct if q else None,
                "volume": q.volume if q else None,
                "market_cap": q.market_cap if q else None,
                "last_updated": q.timestamp_utc if q else None,
            }
        )

    return {
        "count": len(market_data),
        "data": market_data,
        "source": symbols_payload.get("source", "unknown"),
        "cache_hits": sum(1 for item in market_data if item.get("price") is not None),
    }


# =============================================================================
# Quotes & cache endpoints  (v1)
# =============================================================================


@app.get("/v1/quote", response_model=QuoteResponse)
@rate_limit("60/minute")
async def get_quotes(
    request: Request,
    tickers: str = Query(..., description="Comma-separated ticker symbols"),
    auth: bool = Depends(verify_auth),
):
    symbols = [t.strip().upper() for t in tickers.split(",") if t.strip()]
    if not symbols:
        raise HTTPException(status_code=400, detail="No tickers provided")

    quotes: List[Quote] = []
    for sym in symbols:
        q = cache.get_quote(sym)
        if q:
            quotes.append(q)
        else:
            quotes.append(
                Quote(
                    ticker=sym,
                    timestamp_utc=datetime.datetime.utcnow().isoformat() + "Z",
                )
            )

    return QuoteResponse(data=quotes)


@app.post("/v1/quote/update", response_model=Dict[str, Any])
@rate_limit("30/minute")
async def update_quotes(
    request: Request,
    payload: QuoteUpdatePayload,
    autosave: bool = Query(True),
    auth: bool = Depends(verify_auth),
):
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


@app.get("/v1/cache", response_model=Dict[str, Any])
@rate_limit("20/minute")
async def get_cache_info(
    request: Request,
    limit: int = Query(50, ge=1, le=1000),
    auth: bool = Depends(verify_auth),
):
    items = list(cache.data.values())[:limit]
    return {
        "total_items": len(cache.data),
        "sample_size": len(items),
        "sample_data": items,
        "cache_file": str(cache.cache_path),
        "file_exists": cache.cache_path.exists(),
        "file_size": cache.cache_path.stat().st_size
        if cache.cache_path.exists()
        else 0,
        "ttl_minutes": cache.ttl // 60,
    }


@app.post("/v1/cache/backup", response_model=Dict[str, Any])
@rate_limit("5/minute")
async def create_cache_backup(request: Request, auth: bool = Depends(verify_auth)):
    path = cache.create_backup()
    if not path:
        raise HTTPException(status_code=500, detail="Failed to create backup")
    return {"status": "success", "backup_path": path, "item_count": len(cache.data)}


@app.post("/v1/cache/cleanup", response_model=Dict[str, Any])
@rate_limit("5/minute")
async def cleanup_cache(request: Request, auth: bool = Depends(verify_auth)):
    expired = cache.cleanup_expired()
    return {
        "status": "success",
        "expired_removed": expired,
        "remaining_items": len(cache.data),
    }


@app.get("/v1/ping", response_model=Dict[str, Any])
@rate_limit("120/minute")
async def ping(request: Request):
    return {
        "status": "ok",
        "service": config.service_name,
        "version": config.service_version,
        "timestamp": datetime.datetime.utcnow().isoformat() + "Z",
    }


# =============================================================================
# v4.1 Compatibility endpoints (/v41/quotes, /v41/charts)
# =============================================================================


def _quote_to_v41_dict(q: Quote) -> Dict[str, Any]:
    return {
        "ticker": q.ticker,
        "price": q.price,
        "previous_close": q.previous_close,
        "day_change_pct": q.day_change_pct,
        "currency": q.currency,
        "timestamp_utc": q.timestamp_utc,
        "data_source": q.data_source,
    }


@app.get("/v41/quotes")
@rate_limit("60/minute")
async def v41_get_quotes(
    request: Request,
    symbols: str = Query(
        ..., description="Comma-separated tickers (e.g. 1120.SR,7010.SR)"
    ),
    cache_ttl: int = Query(
        60, ge=0, description="Cache TTL in seconds (ignored for now)"
    ),
    auth: bool = Depends(verify_auth),
):
    """
    Compatibility endpoint for existing Apps Script calls:

    GET /v41/quotes?symbols=1120.SR&cache_ttl=60
    """
    tickers = [t.strip().upper() for t in symbols.split(",") if t.strip()]
    if not tickers:
        raise HTTPException(status_code=400, detail="No symbols provided")

    data: List[Dict[str, Any]] = []
    now_ts = datetime.datetime.utcnow().isoformat() + "Z"

    for sym in tickers:
        q = cache.get_quote(sym)
        if q:
            data.append(_quote_to_v41_dict(q))
        else:
            # Return minimal structure if not cached yet
            data.append(
                {
                    "ticker": sym,
                    "price": None,
                    "previous_close": None,
                    "day_change_pct": None,
                    "currency": None,
                    "timestamp_utc": now_ts,
                    "data_source": "cache_miss",
                }
            )

    return {
        "ok": True,
        "symbols": tickers,
        "count": len(tickers),
        "timestamp_utc": now_ts,
        "data": data,
        # extra alias to be safe
        "quotes": data,
    }


@app.post("/v41/quotes")
@rate_limit("30/minute")
async def v41_post_quotes(
    request: Request,
    payload: QuoteUpdatePayload,
    auth: bool = Depends(verify_auth),
):
    """
    Compatibility endpoint for posting quotes to cache:

    POST /v41/quotes
    Body: { "data": [ {ticker, price, ...}, ... ] }
    """
    updated, errors = cache.update_quotes(payload.data)
    now_ts = datetime.datetime.utcnow().isoformat() + "Z"
    return {
        "ok": True,
        "updated": updated,
        "errors": errors or [],
        "total_cached": len(cache.data),
        "timestamp_utc": now_ts,
    }


@app.get("/v41/charts")
@rate_limit("30/minute")
async def v41_get_charts(
    request: Request,
    symbol: str = Query(..., description="Ticker symbol"),
    period: str = Query("1mo", description="Period (e.g. 1d, 5d, 1mo, 3mo)"),
    interval: str = Query("1d", description="Interval (e.g. 1m, 5m, 1d)"),
    auth: bool = Depends(verify_auth),
):
    """
    Stub endpoint for charts. Returns an empty series but ok=true
    so that existing clients don't break.
    """
    now_ts = datetime.datetime.utcnow().isoformat() + "Z"
    return {
        "ok": True,
        "symbol": symbol.upper(),
        "period": period,
        "interval": interval,
        "timestamp_utc": now_ts,
        "points": [],  # future: you can implement real OHLCV series here
        "message": "Chart data not implemented yet",
    }


@app.post("/v41/charts")
@rate_limit("10/minute")
async def v41_post_charts(
    request: Request,
    auth: bool = Depends(verify_auth),
):
    """
    Stub POST for /v41/charts – accepts any body and returns ok=true.
    """
    body = await request.json()
    now_ts = datetime.datetime.utcnow().isoformat() + "Z"
    return {
        "ok": True,
        "received": body,
        "timestamp_utc": now_ts,
        "message": "Chart POST endpoint is a stub (no processing implemented).",
    }


# =============================================================================
# Optional Argaam routes
# =============================================================================

if HAS_ARGAAM_ROUTES:
    app.include_router(argaam_router)
    logger.info("✅ Argaam routes mounted successfully")


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

    logger.info(
        f"🚀 Starting {config.service_name} v{config.service_version} on "
        f"{config.app_host}:{config.app_port}"
    )
    logger.info(
        f"🔐 Authentication: {'Enabled' if config.require_auth else 'Disabled'}"
    )
    logger.info(
        f"⚡ Rate Limiting: {'Enabled' if config.enable_rate_limiting else 'Disabled'} "
        f"({config.max_requests_per_minute}/minute)"
    )
    logger.info(
        f"📈 Financial APIs configured: "
        f"{len([k for k, v in financial_client.get_api_status().items() if v])}"
    )
    logger.info(f"🌍 Environment: {config.environment}")

    uvicorn.run(**server_config)
