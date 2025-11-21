from __future__ import annotations

import datetime
import json
import os
import socket
import logging
from contextlib import closing, asynccontextmanager
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
from advanced_analysis import analyzer
from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException, Query, status
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field, validator

# -----------------------------------------------------------------------------
# Configure logging
# -----------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# -----------------------------------------------------------------------------
# Load environment variables
# -----------------------------------------------------------------------------
BASE_DIR = Path(__file__).resolve().parent
load_dotenv(dotenv_path=BASE_DIR / ".env")

# -----------------------------------------------------------------------------
# Service-level configuration with validation
# -----------------------------------------------------------------------------
SERVICE_NAME = os.getenv("SERVICE_NAME", "Stock Market Hub Sheet API").strip()
SERVICE_VERSION = os.getenv("SERVICE_VERSION", "1.8.0").strip()
FASTAPI_BASE = os.getenv("FASTAPI_BASE", "").strip()
APP_TOKEN = os.getenv("APP_TOKEN", "").strip()
USER_AGENT = os.getenv(
    "USER_AGENT",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
).strip()

# Configuration with validation
try:
    SHEETS_HEADER_ROW = max(1, int(os.getenv("SHEETS_HEADER_ROW", "3")))
except ValueError:
    SHEETS_HEADER_ROW = 3
    logger.warning(f"Invalid SHEETS_HEADER_ROW, using default: {SHEETS_HEADER_ROW}")

# -----------------------------------------------------------------------------
# Argaam Integration Import
# -----------------------------------------------------------------------------
try:
    from routes_argaam import router as argaam_router, close_argaam_http_client
    HAS_ARGAAM_ROUTES = True
    logger.info("Argaam routes loaded successfully")
except ImportError as e:
    HAS_ARGAAM_ROUTES = False
    logger.warning(f"Argaam routes not available: {e}")
    # Create dummy functions
    async def close_argaam_http_client():
        pass

# -----------------------------------------------------------------------------
# Optional imports with better error handling
# -----------------------------------------------------------------------------
HAS_DASHBOARD = False
AdvancedMarketDashboard = None

try:
    from advanced_market_dashboard import AdvancedMarketDashboard
    HAS_DASHBOARD = True
    logger.info("AdvancedMarketDashboard loaded successfully")
except ImportError as e:
    logger.warning(f"AdvancedMarketDashboard not available: {e}")
except Exception as e:
    logger.error(f"Error loading AdvancedMarketDashboard: {e}")

# Local modules with error handling
try:
    import symbols_reader as sr
    HAS_SYMBOLS_READER = True
    logger.info("symbols_reader loaded successfully")
except ImportError as e:
    logger.error(f"Failed to import symbols_reader: {e}")
    HAS_SYMBOLS_READER = False
    # Create a dummy module to avoid crashes
    class DummySymbolsReader:
        @staticmethod
        def fetch_symbols(limit):
            return {"data": [], "count": 0, "error": "symbols_reader not available"}
    sr = DummySymbolsReader()

# -----------------------------------------------------------------------------
# Google Apps Script Client Import
# -----------------------------------------------------------------------------
try:
    from google_apps_script_client import google_apps_script_client
    HAS_GOOGLE_APPS_SCRIPT = True
    logger.info("Google Apps Script client loaded successfully")
except ImportError as e:
    HAS_GOOGLE_APPS_SCRIPT = False
    logger.warning(f"Google Apps Script client not available: {e}")
    # Create dummy client
    class DummyGoogleAppsScriptClient:
        @staticmethod
        def get_symbols_data(symbol=None):
            return type('obj', (object,), {
                'success': False,
                'data': None,
                'error': 'Google Apps Script client not available',
                'execution_time': 0
            })()
    google_apps_script_client = DummyGoogleAppsScriptClient()

# -----------------------------------------------------------------------------
# Google Sheets imports with error handling
# -----------------------------------------------------------------------------
try:
    import gspread
    from google.oauth2.service_account import Credentials
    HAS_GSHEETS = True
except ImportError:
    gspread = None
    Credentials = None
    HAS_GSHEETS = False
    logger.warning("gspread or google-auth not available")

# -----------------------------------------------------------------------------
# Enhanced Pydantic Models with Validation
# -----------------------------------------------------------------------------
class Quote(BaseModel):
    ticker: str = Field(..., description="Stock ticker symbol")
    company: Optional[str] = Field(None, description="Company name")
    currency: Optional[str] = Field(None, description="Currency code")
    price: Optional[float] = Field(None, ge=0, description="Current price")
    previous_close: Optional[float] = Field(None, ge=0, description="Previous close price")
    day_change_pct: Optional[float] = Field(None, description="Daily change percentage")
    market_cap: Optional[float] = Field(None, ge=0, description="Market capitalization")
    volume: Optional[float] = Field(None, ge=0, description="Trading volume")
    fifty_two_week_high: Optional[float] = Field(None, ge=0, description="52-week high")
    fifty_two_week_low: Optional[float] = Field(None, ge=0, description="52-week low")
    timestamp_utc: Optional[str] = Field(None, description="UTC timestamp")

    @validator('ticker')
    def validate_ticker(cls, v):
        if not v or not v.strip():
            raise ValueError('Ticker cannot be empty')
        return v.strip().upper()

    @validator('day_change_pct')
    def validate_change_pct(cls, v):
        if v is not None and (v < -100 or v > 1000):  # Reasonable bounds
            raise ValueError('Change percentage out of reasonable bounds')
        return v


class QuoteUpdatePayload(BaseModel):
    data: List[Quote] = Field(..., description="List of quotes to update")


class QuoteResponse(BaseModel):
    data: List[Quote] = Field(..., description="List of quotes")


class AnalysisRequest(BaseModel):
    ticker: str = Field(..., description="Stock ticker to analyze")
    fundamentals: Dict[str, Any] = Field(default_factory=dict, description="Fundamental data")


class AnalysisResponse(BaseModel):
    ticker: str = Field(..., description="Analyzed ticker")
    score: float = Field(..., ge=0, le=100, description="Analysis score 0-100")
    summary: str = Field(..., description="Analysis summary")
    confidence: float = Field(..., ge=0, le=1, description="Confidence level")


class HealthResponse(BaseModel):
    status: str = Field(..., description="Service status")
    time_utc: str = Field(..., description="UTC timestamp")
    count_cached: int = Field(..., ge=0, description="Number of cached quotes")
    cache_file_exists: bool = Field(..., description="Whether cache file exists")
    has_dashboard: bool = Field(..., description="Dashboard module available")
    has_gsheets: bool = Field(..., description="Google Sheets available")
    has_argaam_routes: bool = Field(..., description="Argaam routes available")


class ErrorResponse(BaseModel):
    error: str = Field(..., description="Error message")
    detail: Optional[str] = Field(None, description="Error details")
    timestamp: str = Field(..., description="Error timestamp")


# -----------------------------------------------------------------------------
# Cache + persistence with enhanced error handling
# -----------------------------------------------------------------------------
CACHE_PATH = BASE_DIR / "quote_cache.json"
BACKUP_DIR = BASE_DIR / "cache_backups"

# Ensure backup directory exists
BACKUP_DIR.mkdir(parents=True, exist_ok=True)

# In-memory cache: {"TICKER": {quote_dict}}
QUOTE_CACHE: Dict[str, Dict[str, Any]] = {}


class CacheError(Exception):
    """Custom exception for cache operations"""
    pass


def _items_to_mapping(items: List[Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
    """Convert list of quote dicts to ticker mapping."""
    out: Dict[str, Dict[str, Any]] = {}
    for q in items:
        if isinstance(q, dict) and q.get("ticker"):
            ticker = str(q["ticker"]).strip().upper()
            if ticker:
                out[ticker] = q
    return out


def _is_ticker_mapping(obj: Any) -> bool:
    """Return True if obj is a proper ticker mapping."""
    if not isinstance(obj, dict) or not obj:
        return False
    
    for k, v in obj.items():
        if not (isinstance(k, str) and isinstance(v, dict)):
            return False
        # Allow some flexibility in ticker matching
        if v.get("ticker") and v["ticker"].upper() != k.upper():
            return False
    return True


def _normalize_cache_shape(obj: Any) -> Tuple[Dict[str, Dict[str, Any]], bool]:
    """
    Normalize cache data shape with enhanced error handling.
    Returns (mapping, repaired_flag)
    """
    try:
        if _is_ticker_mapping(obj):
            return obj, False

        if isinstance(obj, dict) and isinstance(obj.get("data"), list):
            mapping = _items_to_mapping(obj["data"])
            if mapping:
                return mapping, True

        return {}, False
    except Exception as e:
        logger.error(f"Error normalizing cache shape: {e}")
        return {}, False


def _save_cache_canonical() -> int:
    """Persist QUOTE_CACHE to disk with error handling."""
    try:
        data = {"data": list(QUOTE_CACHE.values())}
        CACHE_PATH.write_text(
            json.dumps(data, ensure_ascii=False, indent=2), 
            encoding="utf-8"
        )
        logger.info(f"Cache saved with {len(data['data'])} items")
        return len(data["data"])
    except Exception as e:
        logger.error(f"Failed to save cache: {e}")
        raise CacheError(f"Failed to save cache: {e}")


def _backup_cache() -> str:
    """Create timestamped backup with error handling."""
    if not QUOTE_CACHE:
        raise CacheError("Cache is empty; nothing to back up")
    
    try:
        ts = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
        path = BACKUP_DIR / f"quote_cache_{ts}.json"
        data = {"data": list(QUOTE_CACHE.values())}
        path.write_text(
            json.dumps(data, ensure_ascii=False, indent=2), 
            encoding="utf-8"
        )
        logger.info(f"Backup created: {path}")
        return str(path)
    except Exception as e:
        logger.error(f"Backup failed: {e}")
        raise CacheError(f"Backup failed: {e}")


def _list_backups() -> List[Dict[str, Any]]:
    """List backup files with error handling."""
    try:
        if not BACKUP_DIR.exists():
            return []
        
        items: List[Dict[str, Any]] = []
        for p in sorted(
            BACKUP_DIR.glob("quote_cache_*.json"),
            key=lambda x: x.stat().st_mtime,
            reverse=True,
        ):
            try:
                st = p.stat()
                items.append({
                    "name": p.name,
                    "path": str(p),
                    "size": st.st_size,
                    "last_write_iso": datetime.datetime.fromtimestamp(st.st_mtime).isoformat(),
                })
            except Exception as e:
                logger.warning(f"Could not read backup info for {p}: {e}")
                continue
                
        return items
    except Exception as e:
        logger.error(f"Error listing backups: {e}")
        return []


def _restore_backup(name: str) -> Dict[str, Any]:
    """Restore from backup with enhanced security and error handling."""
    try:
        # Security: validate filename
        path = Path(name)
        if path.is_absolute() or '..' in name:
            raise ValueError("Invalid backup filename")
            
        backup_file = BACKUP_DIR / name
        if not backup_file.exists() or not backup_file.is_file():
            raise FileNotFoundError(f"Backup not found: {name}")

        # Read and validate backup
        raw = json.loads(backup_file.read_text(encoding="utf-8"))
        mapping, _ = _normalize_cache_shape(raw)
        if not mapping:
            raise ValueError("Backup file has no recognizable quotes")

        # Replace in-memory cache
        QUOTE_CACHE.clear()
        QUOTE_CACHE.update(mapping)

        # Persist to main cache
        try:
            _save_cache_canonical()
            persisted = True
        except Exception as e:
            logger.warning(f"Could not persist after restore: {e}")
            persisted = False

        return {
            "status": "restored",
            "from": str(backup_file),
            "count": len(QUOTE_CACHE),
            "tickers": list(QUOTE_CACHE.keys()),
            "persisted": persisted,
        }
    except Exception as e:
        logger.error(f"Restore failed: {e}")
        raise


def _quote_from_cache(obj: Dict[str, Any]) -> Quote:
    """Create Quote object from cache data with type safety."""
    return Quote(
        ticker=obj.get("ticker", ""),
        company=obj.get("company"),
        currency=obj.get("currency"),
        price=obj.get("price"),
        previous_close=obj.get("previous_close"),
        day_change_pct=obj.get("day_change_pct"),
        market_cap=obj.get("market_cap"),
        volume=obj.get("volume"),
        fifty_two_week_high=obj.get("fifty_two_week_high"),
        fifty_two_week_low=obj.get("fifty_two_week_low"),
        timestamp_utc=obj.get("timestamp_utc"),
    )


def _empty_quote(sym: str) -> Quote:
    """Create empty quote for missing ticker."""
    return Quote(
        ticker=sym,
        company=None,
        currency=None,
        price=None,
        previous_close=None,
        day_change_pct=None,
        market_cap=None,
        volume=None,
        fifty_two_week_high=None,
        fifty_two_week_low=None,
        timestamp_utc=datetime.datetime.utcnow().isoformat() + "Z",
    )


# -----------------------------------------------------------------------------
# Enhanced Lifespan Management with Argaam Integration
# -----------------------------------------------------------------------------
@asynccontextmanager
async def lifespan(app: FastAPI):
    """Enhanced lifespan with Argaam integration."""
    # Startup
    startup_time = datetime.datetime.utcnow()
    logger.info(f"Starting {SERVICE_NAME} v{SERVICE_VERSION}")
    
    # Load cache
    if CACHE_PATH.exists():
        try:
            raw = json.loads(CACHE_PATH.read_text(encoding="utf-8"))
            mapping, repaired = _normalize_cache_shape(raw)
            if mapping:
                QUOTE_CACHE.clear()
                QUOTE_CACHE.update(mapping)
                logger.info(f"Loaded {len(QUOTE_CACHE)} quotes from cache")
                if repaired:
                    logger.info("Cache was repaired from old format")
            else:
                logger.warning("Cache file exists but contains no valid data")
        except Exception as e:
            logger.error(f"Failed to load cache: {e}")
            QUOTE_CACHE.clear()
    else:
        logger.info("No cache file found, starting with empty cache")
    
    yield
    
    # Shutdown - Close Argaam HTTP client
    try:
        await close_argaam_http_client()
        logger.info("Argaam HTTP client closed successfully")
    except Exception as e:
        logger.warning(f"Error closing Argaam HTTP client: {e}")
    
    shutdown_time = datetime.datetime.utcnow()
    uptime = shutdown_time - startup_time
    logger.info(f"Shutting down after {uptime}")


# -----------------------------------------------------------------------------
# FastAPI App Configuration
# -----------------------------------------------------------------------------
app = FastAPI(
    title=SERVICE_NAME,
    version=SERVICE_VERSION,
    description=(
        "Enhanced Stock Market API with Argaam integration and improved error handling.\n\n"
        "Main endpoints:\n"
        "  • GET  /                                 -> Service info and status\n"
        "  • GET  /health                           -> Health check with diagnostics\n"
        "  • GET  /v1/ping                          -> Simple ping endpoint\n"
        "  • GET  /api/saudi/symbols?limit=N        -> Symbols from Google Sheets\n"
        "  • GET  /api/saudi/market?limit=N         -> Symbols with cached quotes\n"
        "  • POST /v1/quote/update?autosave=true    -> Update quotes cache\n"
        "  • GET  /v1/quote?tickers=A,B             -> Get quotes from cache\n"
        "  • GET  /v1/cache                         -> Cache inspection\n"
        "  • GET  /v41/argaam/quotes                -> Argaam-style quotes\n"
        "  • POST /v41/argaam/quotes                -> Argaam quotes with POST\n"
        "  • GET  /v1/multi-source/{symbol}         -> Multi-source analysis\n"
        "  • GET  /v1/google-apps-script/symbols    -> Google Apps Script data\n"
        "  • GET  /v1/apis/status                   -> API status check\n"
    ),
    lifespan=lifespan,
    responses={
        400: {"model": ErrorResponse},
        500: {"model": ErrorResponse},
        503: {"model": ErrorResponse},
    }
)

# -----------------------------------------------------------------------------
# Include Argaam Routes
# -----------------------------------------------------------------------------
if HAS_ARGAAM_ROUTES:
    app.include_router(argaam_router)
    logger.info("Argaam routes mounted successfully")
else:
    logger.warning("Argaam routes not available - endpoints will be missing")

# -----------------------------------------------------------------------------
# Enhanced CORS Configuration
# -----------------------------------------------------------------------------
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Can be restricted in production
    allow_credentials=True,
    allow_methods=["GET", "POST", "PUT", "DELETE", "OPTIONS"],
    allow_headers=["*"],
)

# -----------------------------------------------------------------------------
# Global Exception Handlers
# -----------------------------------------------------------------------------
@app.exception_handler(HTTPException)
async def http_exception_handler(request, exc):
    """Enhanced HTTP exception handler."""
    logger.warning(f"HTTPException: {exc.status_code} - {exc.detail}")
    return JSONResponse(
        status_code=exc.status_code,
        content=ErrorResponse(
            error="HTTP Error",
            detail=exc.detail,
            timestamp=datetime.datetime.utcnow().isoformat() + "Z"
        ).dict()
    )

@app.exception_handler(Exception)
async def global_exception_handler(request, exc):
    """Global exception handler for unhandled errors."""
    logger.error(f"Unhandled exception: {exc}", exc_info=True)
    return JSONResponse(
        status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
        content=ErrorResponse(
            error="Internal Server Error",
            detail="An unexpected error occurred",
            timestamp=datetime.datetime.utcnow().isoformat() + "Z"
        ).dict()
    )


# -----------------------------------------------------------------------------
# Utility Functions
# -----------------------------------------------------------------------------
def _truthy(v: Any) -> bool:
    """Safely convert to boolean."""
    if isinstance(v, bool):
        return v
    if v is None:
        return False
    s = str(v).strip().lower()
    return s in ("true", "1", "yes", "y", "on")


def _require_dashboard() -> AdvancedMarketDashboard:
    """Check if dashboard is available."""
    if not HAS_DASHBOARD:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="AdvancedMarketDashboard not available"
        )
    return AdvancedMarketDashboard()


def _get_sheets_client():
    """Get Google Sheets client with error handling."""
    if not HAS_GSHEETS:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Google Sheets client not available"
        )
    
    creds_path = os.getenv("SHEETS_JSON_CREDENTIALS_PATH", "./creds/google-service.json")
    if not os.path.exists(creds_path):
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Missing Google Sheets credentials: {creds_path}"
        )
    
    try:
        scopes = ["https://www.googleapis.com/auth/spreadsheets.readonly"]
        creds = Credentials.from_service_account_file(creds_path, scopes=scopes)
        return gspread.authorize(creds)
    except Exception as e:
        logger.error(f"Google Sheets auth failed: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Google Sheets authentication failed: {e}"
        )


# -----------------------------------------------------------------------------
# Google Sheets Integration
# -----------------------------------------------------------------------------
def _read_symbols_sheet_rows() -> Tuple[str, str, List[Dict[str, Any]]]:
    """Read symbols from Google Sheets with enhanced error handling."""
    sheet_id = os.getenv("SHEETS_SPREADSHEET_ID")
    tab = os.getenv("SHEETS_TAB_SYMBOLS", "symbols_master")
    
    if not sheet_id:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="SHEETS_SPREADSHEET_ID environment variable not set"
        )

    gc = _get_sheets_client()
    try:
        sh = gc.open_by_key(sheet_id)
        ws = sh.worksheet(tab)
        values: List[List[Any]] = ws.get_all_values()
    except Exception as e:
        logger.error(f"Google Sheets read error: {e}")
        raise HTTPException(
            status_code=status.HTTP_502_BAD_GATEWAY,
            detail=f"Failed to read from Google Sheets: {e}"
        )

    if not values or len(values) < SHEETS_HEADER_ROW:
        return sh.title, tab, []

    header_row_idx = SHEETS_HEADER_ROW - 1
    raw_headers = values[header_row_idx]

    # Create unique headers
    headers: List[str] = []
    seen: Dict[str, int] = {}
    for idx, h in enumerate(raw_headers):
        name = (h or "").strip()
        if not name:
            name = f"col_{idx+1}"
        if name in seen:
            seen[name] += 1
            name = f"{name}_{seen[name]}"
        else:
            seen[name] = 1
        headers.append(name)

    rows: List[Dict[str, Any]] = []
    for row_vals in values[header_row_idx + 1:]:
        if not any(str(x).strip() for x in row_vals):
            continue
            
        row_dict: Dict[str, Any] = {}
        for i, col_name in enumerate(headers):
            value = row_vals[i] if i < len(row_vals) else ""
            row_dict[col_name] = value
        rows.append(row_dict)

    return sh.title, tab, rows


def _build_symbols_payload_from_sheet(limit: int, only_included: bool = True) -> Dict[str, Any]:
    """Build symbols payload from Google Sheets."""
    try:
        sheet_title, tab, rows = _read_symbols_sheet_rows()
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to read symbols sheet: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to process symbols sheet: {e}"
        )

    data: List[Dict[str, Any]] = []
    for row in rows:
        include_flag = (
            row.get("include_in_ranking") or 
            row.get("Include in Ranking") or 
            row.get("Include in Ranking_2")
        )

        if only_included and not _truthy(include_flag):
            continue

        sym = (
            row.get("Ticker") or
            row.get("Ticker_2") or
            row.get("Symbol") or
            row.get("symbol") or ""
        )
        sym = str(sym).strip()
        if not sym:
            continue

        out_row = {
            "symbol": sym,
            "company_name": (
                row.get("Company Name") or
                row.get("Company") or
                row.get("company_name") or
                row.get("Company Name_2")
            ),
            "trading_sector": (
                row.get("Sector") or
                row.get("Trading Sector") or
                row.get("trading_sector")
            ),
            "financial_market": (
                row.get("Trading Market") or
                row.get("Financial Market") or
                row.get("financial_market")
            ),
        }
        data.append(out_row)

        if len(data) >= limit:
            break

    return {
        "sheet_title": sheet_title,
        "worksheet": tab,
        "count": len(data),
        "data": data,
        "info": {
            "source": "gsheet_fallback_manual_headers",
            "header_row": SHEETS_HEADER_ROW,
        },
    }

def _safe_fetch_symbols_via_sr(limit: int) -> Optional[Dict[str, Any]]:
    """Safely fetch symbols using symbols_reader."""
    if not HAS_SYMBOLS_READER:
        logger.warning("HAS_SYMBOLS_READER is False")
        return None
        
    try:
        payload = sr.fetch_symbols(limit)
        logger.info(f"symbols_reader response: {payload}")
        
        if not isinstance(payload, dict):
            logger.warning("symbols_reader returned non-dict response")
            return None
            
        data = payload.get("data")
        if data is None or not isinstance(data, list):
            logger.warning(f"symbols_reader returned invalid data: {data}")
            return None

        # Always return the payload even if data is empty
        # Fix count if needed
        if isinstance(payload.get("count"), int) and payload["count"] <= 0:
            payload["count"] = len(data)
            
        logger.info(f"symbols_reader returning {len(data)} symbols")
        return payload
    except Exception as e:
        logger.warning(f"symbols_reader.fetch_symbols failed: {e}")
        return None


def _fetch_symbol_payload(limit: int) -> Dict[str, Any]:
    """Unified symbol loader with fallback."""
    # Try symbols_reader first
    sr_payload = _safe_fetch_symbols_via_sr(limit)
    if sr_payload is not None:  # Changed this condition
        logger.info(f"Using symbols_reader data with {len(sr_payload.get('data', []))} symbols")
        return sr_payload

    # Fallback to direct sheet read
    if HAS_GSHEETS:
        logger.info("Falling back to direct Google Sheets read")
        fallback = _build_symbols_payload_from_sheet(limit, only_included=True)
        if fallback.get("count", 0) > 0:
            return fallback

    # Final fallback - return empty but valid response
    logger.warning("Both symbols_reader and Google Sheets failed, returning empty response")
    return {
        "data": [],
        "count": 0,
        "source": "fallback",
        "error": "No symbols available"
    }


# -----------------------------------------------------------------------------
# Root & Health Endpoints
# -----------------------------------------------------------------------------
@app.get("/", response_model=Dict[str, Any])
async def root() -> Dict[str, Any]:
    """Enhanced root endpoint with comprehensive info including Argaam."""
    return {
        "ok": True,
        "service": SERVICE_NAME,
        "version": SERVICE_VERSION,
        "time_utc": datetime.datetime.utcnow().isoformat() + "Z",
        "environment": {
            "fastapi_base": FASTAPI_BASE or None,
            "has_app_token": bool(APP_TOKEN),
            "cache_file_exists": CACHE_PATH.exists(),
        },
        "capabilities": {
            "has_dashboard": HAS_DASHBOARD,
            "has_gsheets": HAS_GSHEETS,
            "has_symbols_reader": HAS_SYMBOLS_READER,
            "has_argaam_routes": HAS_ARGAAM_ROUTES,  # Added Argaam capability
            "has_google_apps_script": HAS_GOOGLE_APPS_SCRIPT,
        },
        "cache": {
            "count_cached": len(QUOTE_CACHE),
            "cache_file": str(CACHE_PATH),
        },
        "endpoints": {
            "health": "/health",
            "ping": "/v1/ping",
            "saudi_symbols": "/api/saudi/symbols",
            "saudi_market": "/api/saudi/market",
            "quote_get": "/v1/quote",
            "quote_update": "/v1/quote/update",
            "argaam_quotes": "/v41/argaam/quotes",
            "argaam_health": "/v41/argaam/health",
            "cache_view": "/v1/cache",
            "multi_source_analysis": "/v1/multi-source/{symbol}",
            "google_apps_script": "/v1/google-apps-script/symbols",
            "apis_status": "/v1/apis/status",
        },
    }


@app.get("/v1/ping", response_model=Dict[str, Any])
async def ping() -> Dict[str, Any]:
    """Enhanced ping endpoint."""
    return {
        "status": "ok",
        "time_utc": datetime.datetime.utcnow().isoformat() + "Z",
        "service": SERVICE_NAME,
        "version": SERVICE_VERSION,
        "capabilities": {
            "has_dashboard": HAS_DASHBOARD,
            "has_gsheets": HAS_GSHEETS,
            "has_argaam_routes": HAS_ARGAAM_ROUTES,
            "has_google_apps_script": HAS_GOOGLE_APPS_SCRIPT,
            "cache_count": len(QUOTE_CACHE),
        }
    }


@app.get("/health", response_model=HealthResponse)
async def health() -> HealthResponse:
    """Comprehensive health check endpoint."""
    shape_ok = _is_ticker_mapping(QUOTE_CACHE)
    
    # Additional health checks
    health_status = "healthy"
    if not shape_ok and QUOTE_CACHE:
        health_status = "degraded"
    elif not HAS_GSHEETS:
        health_status = "degraded"
    
    return HealthResponse(
        status=health_status,
        time_utc=datetime.datetime.utcnow().isoformat() + "Z",
        count_cached=len(QUOTE_CACHE),
        cache_file_exists=CACHE_PATH.exists(),
        has_dashboard=HAS_DASHBOARD,
        has_gsheets=HAS_GSHEETS,
        has_argaam_routes=HAS_ARGAAM_ROUTES,
    )


# -----------------------------------------------------------------------------
# Admin Endpoints
# -----------------------------------------------------------------------------
@app.post("/diagnostics", response_model=Dict[str, Any])
async def run_diagnostics() -> Dict[str, Any]:
    """Run SAFE diagnostics."""
    dash = _require_dashboard()
    try:
        dash.run_safe_diagnostics()
        return {
            "status": "ok",
            "message": "Diagnostics executed successfully",
            "timestamp": datetime.datetime.utcnow().isoformat() + "Z",
        }
    except Exception as e:
        logger.error(f"Diagnostics failed: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Diagnostics failed: {e}"
        )


@app.post("/format/all-headers", response_model=Dict[str, Any])
async def format_all_headers() -> Dict[str, Any]:
    """Format headers for all sheets."""
    dash = _require_dashboard()
    try:
        dash.run_market_leaders_header_format()
        dash.run_other_sheets_header_format()
        return {
            "status": "ok",
            "message": "Header formatting completed for all configured sheets",
            "timestamp": datetime.datetime.utcnow().isoformat() + "Z",
        }
    except Exception as e:
        logger.error(f"Header formatting failed: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Header formatting failed: {e}"
        )


# -----------------------------------------------------------------------------
# Saudi Symbols & Market Endpoints
# -----------------------------------------------------------------------------
@app.get("/api/saudi/symbols", response_model=Dict[str, Any])
async def api_saudi_symbols(
    limit: int = Query(20, ge=1, le=500, description="Max rows to read")
) -> Dict[str, Any]:
    """Get Saudi symbols with enhanced error handling."""
    try:
        payload = _fetch_symbol_payload(limit)
        # Ensure limit is applied
        data = payload.get("data", [])
        if isinstance(data, list):
            payload["data"] = data[:limit]
            payload["count"] = len(payload["data"])
        return payload
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to read symbols: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to read symbols: {e}"
        )


@app.get("/api/saudi/market", response_model=Dict[str, Any])
async def api_saudi_market(
    limit: int = Query(20, ge=1, le=500, description="Max rows to read")
) -> Dict[str, Any]:
    """Merge symbols with cached quotes."""
    try:
        payload = _fetch_symbol_payload(limit)
        rows = payload.get("data", []) or []
        merged: List[Dict[str, Any]] = []

        for row in rows:
            sym = row.get("symbol")
            out = {
                "symbol": row.get("symbol"),
                "company_name": row.get("company_name"),
                "trading_sector": row.get("trading_sector"),
                "financial_market": row.get("financial_market"),
                # Default quote fields
                "price": None,
                "previous_close": None,
                "day_change_pct": None,
                "market_cap": None,
                "volume": None,
                "fifty_two_week_high": None,
                "fifty_two_week_low": None,
                "timestamp_utc": None,
            }
            if sym and sym in QUOTE_CACHE:
                q = QUOTE_CACHE[sym]
                out.update({
                    k: q.get(k) for k in [
                        "price", "previous_close", "day_change_pct", "market_cap",
                        "volume", "fifty_two_week_high", "fifty_two_week_low", "timestamp_utc"
                    ]
                })
            merged.append(out)

        return {"count": len(merged), "data": merged}
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to build market view: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to build market view: {e}"
        )


# -----------------------------------------------------------------------------
# Quote Management Endpoints
# -----------------------------------------------------------------------------
@app.post("/v1/quote/update", response_model=Dict[str, Any])
async def update_quotes(
    payload: QuoteUpdatePayload,
    autosave: bool = Query(True, description="Automatically save to disk")
) -> Dict[str, Any]:
    """Update quotes in cache with validation."""
    updated_count = 0
    errors = []
    
    for item in payload.data:
        try:
            # Validate the quote
            quote_dict = item.dict()
            QUOTE_CACHE[item.ticker] = quote_dict
            updated_count += 1
        except Exception as e:
            errors.append(f"Failed to update {item.ticker}: {e}")
            logger.warning(f"Quote update failed for {item.ticker}: {e}")

    autosaved = False
    autosave_error = None
    if autosave and updated_count > 0:
        try:
            _save_cache_canonical()
            autosaved = True
        except Exception as e:
            autosave_error = str(e)
            logger.error(f"Autosave failed: {e}")

    response = {
        "status": "completed",
        "updated_count": updated_count,
        "error_count": len(errors),
        "errors": errors if errors else None,
        "autosaved": autosaved,
        "autosave_error": autosave_error,
        "total_cached": len(QUOTE_CACHE),
    }
    
    logger.info(f"Quote update: {updated_count} updated, {len(errors)} errors")
    return response


@app.get("/v1/quote", response_model=QuoteResponse)
async def get_quote(
    tickers: str = Query(..., description="Comma-separated tickers")
) -> QuoteResponse:
    """Get quotes from cache."""
    symbols = [t.strip().upper() for t in tickers.split(",") if t.strip()]
    if not symbols:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="No tickers provided"
        )
    
    out_list: List[Quote] = []
    for sym in symbols:
        if sym in QUOTE_CACHE:
            out_list.append(_quote_from_cache(QUOTE_CACHE[sym]))
        else:
            out_list.append(_empty_quote(sym))
    
    return QuoteResponse(data=out_list)


# -----------------------------------------------------------------------------
# Argaam-style Quotes Endpoint (Legacy - now use /v41/argaam/quotes)
# -----------------------------------------------------------------------------
@app.get("/v41/argaam/quotes", response_model=Dict[str, Any])
async def legacy_argaam_quotes(
    tickers: str = Query(..., description="Comma-separated tickers")
) -> Dict[str, Any]:
    """Legacy Argaam-style quotes endpoint - now handled by routes_argaam."""
    # This endpoint is maintained for backward compatibility
    # The actual implementation is now in routes_argaam.py
    raw_symbols = [t.strip() for t in tickers.split(",") if t.strip()]
    if not raw_symbols:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="No tickers provided"
        )
    
    items: List[Dict[str, Any]] = []
    
    for raw in raw_symbols:
        sym_upper = raw.upper()
        
        # Try multiple cache key variants
        candidates = [sym_upper]
        if not sym_upper.endswith(".SR"):
            candidates.append(sym_upper + ".SR")
        if sym_upper.endswith(".SR"):
            candidates.append(sym_upper.replace(".SR", ""))
        
        q: Dict[str, Any] = {}
        for key in candidates:
            if key in QUOTE_CACHE:
                q = QUOTE_CACHE[key]
                break
        
        # Calculate derived fields
        last_price = q.get("price")
        prev_close = q.get("previous_close")
        change_value = None
        change_percent = q.get("day_change_pct")
        
        if last_price is not None and prev_close is not None:
            try:
                change_value = float(last_price) - float(prev_close)
                if change_percent is None and prev_close != 0:
                    change_percent = (change_value / float(prev_close)) * 100
            except (ValueError, TypeError):
                pass
        
        item = {
            "ticker": sym_upper,
            "company": q.get("company"),
            "sector": None,
            "trading_market": "TADAWUL" if any(c.endswith('.SR') or c.isdigit() for c in candidates) else None,
            "last_price": last_price,
            "day_high": None,
            "day_low": None,
            "previous_close": prev_close,
            "change_value": change_value,
            "change_percent": change_percent,
            "volume": q.get("volume"),
            "value_traded": None,
            "market_cap": q.get("market_cap"),
            "pe_ttm": None,
            "pb": None,
            "dividend_yield": None,
            "eps_ttm": None,
            "roe": None,
            "trend_direction": None,
            "support_level": None,
            "resistance_level": None,
            "momentum_score": None,
            "volatility_est": None,
            "expected_roi_1m": None,
            "expected_roi_3m": None,
            "expected_roi_12m": None,
            "risk_level": None,
            "confidence_score": None,
            "data_quality": None,
            "composite_score": None,
            "analysis_source": "QUOTE_CACHE" if q else "EMPTY",
            "external_analysis_url": None,
            "timestamp_utc": q.get("timestamp_utc"),
        }
        items.append(item)
    
    return {
        "ok": True,
        "source": "quote_cache",
        "count": len(items),
        "items": items,
        "timestamp": datetime.datetime.utcnow().isoformat() + "Z",
    }


# -----------------------------------------------------------------------------
# Cache Management Endpoints
# -----------------------------------------------------------------------------
@app.get("/v1/cache", response_model=Dict[str, Any])
async def cache_view(
    limit: Optional[int] = Query(None, ge=1, le=500, description="Limit results")
) -> Dict[str, Any]:
    """Inspect cache with enhanced information."""
    shape_ok = _is_ticker_mapping(QUOTE_CACHE)
    items = list(QUOTE_CACHE.values())
    if limit:
        items = items[:limit]
    
    return {
        "count": len(QUOTE_CACHE),
        "tickers": list(QUOTE_CACHE.keys()),
        "data": items,
        "cache_file": str(CACHE_PATH),
        "cache_file_exists": CACHE_PATH.exists(),
        "shape_ok": shape_ok,
        "shape_warning": not shape_ok,
        "timestamp": datetime.datetime.utcnow().isoformat() + "Z",
    }


@app.post("/v1/cache/save", response_model=Dict[str, Any])
async def cache_save() -> Dict[str, Any]:
    """Save cache to disk."""
    try:
        count = _save_cache_canonical()
        return {
            "status": "saved",
            "path": str(CACHE_PATH),
            "count": count,
            "timestamp": datetime.datetime.utcnow().isoformat() + "Z",
        }
    except CacheError as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=str(e)
        )


@app.post("/v1/cache/load", response_model=Dict[str, Any])
async def cache_load(body: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    """Load cache from disk or request body."""
    try:
        if body is None:
            if not CACHE_PATH.exists():
                return {
                    "status": "no_file", 
                    "path": str(CACHE_PATH),
                    "timestamp": datetime.datetime.utcnow().isoformat() + "Z",
                }
            raw = json.loads(CACHE_PATH.read_text(encoding="utf-8"))
        else:
            raw = body

        mapping, repaired = _normalize_cache_shape(raw)
        QUOTE_CACHE.clear()
        QUOTE_CACHE.update(mapping)

        return {
            "status": "loaded",
            "count": len(QUOTE_CACHE),
            "tickers": list(QUOTE_CACHE.keys()),
            "repaired_from_old_format": repaired,
            "path": str(CACHE_PATH),
            "timestamp": datetime.datetime.utcnow().isoformat() + "Z",
        }
    except Exception as e:
        logger.error(f"Cache load failed: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to load cache: {e}"
        )


@app.post("/v1/cache/backup", response_model=Dict[str, Any])
async def cache_backup() -> Dict[str, Any]:
    """Create cache backup."""
    try:
        path = _backup_cache()
        return {
            "status": "saved",
            "path": path,
            "count": len(QUOTE_CACHE),
            "timestamp": datetime.datetime.utcnow().isoformat() + "Z",
        }
    except CacheError as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(e)
        )


@app.get("/v1/cache/backups", response_model=Dict[str, Any])
async def cache_backups() -> Dict[str, Any]:
    """List cache backups."""
    bks = _list_backups()
    return {
        "count": len(bks),
        "backups": bks,
        "timestamp": datetime.datetime.utcnow().isoformat() + "Z",
    }


@app.post("/v1/cache/restore", response_model=Dict[str, Any])
async def cache_restore(
    name: str = Query(..., description="Backup filename")
) -> Dict[str, Any]:
    """Restore cache from backup."""
    try:
        result = _restore_backup(name)
        result["timestamp"] = datetime.datetime.utcnow().isoformat() + "Z"
        return result
    except FileNotFoundError:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Backup not found: {name}"
        )
    except Exception as e:
        logger.error(f"Cache restore failed: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Restore failed: {e}"
        )


@app.post("/v1/cache/clear", response_model=Dict[str, Any])
async def cache_clear(
    delete_file: bool = Query(False, description="Delete cache file")
) -> Dict[str, Any]:
    """Clear cache with option to delete file."""
    count_before = len(QUOTE_CACHE)
    QUOTE_CACHE.clear()
    
    deleted_file = False
    if delete_file and CACHE_PATH.exists():
        try:
            CACHE_PATH.unlink()
            deleted_file = True
        except Exception as e:
            logger.error(f"Failed to delete cache file: {e}")

    return {
        "status": "cleared",
        "cleared_count": count_before,
        "deleted_file": deleted_file,
        "count_cached": len(QUOTE_CACHE),
        "timestamp": datetime.datetime.utcnow().isoformat() + "Z",
    }


# -----------------------------------------------------------------------------
# Additional Endpoints
# -----------------------------------------------------------------------------
@app.get("/api/sheets/symbols", response_model=Dict[str, Any])
async def api_sheets_symbols(
    include_all: bool = Query(False, description="Include all symbols")
) -> Dict[str, Any]:
    """Direct view of symbols sheet."""
    if not HAS_GSHEETS:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Google Sheets not available"
        )
        
    try:
        sheet_title, tab, rows = _read_symbols_sheet_rows()
        
        if include_all:
            data = rows
        else:
            data = [
                row for row in rows 
                if _truthy(
                    row.get("include_in_ranking") or 
                    row.get("Include in Ranking") or 
                    row.get("Include in Ranking_2")
                )
            ]

        return {
            "ok": True,
            "spreadsheet_id": os.getenv("SHEETS_SPREADSHEET_ID"),
            "tab": tab,
            "header_row": SHEETS_HEADER_ROW,
            "count": len(data),
            "data": data,
            "timestamp": datetime.datetime.utcnow().isoformat() + "Z",
        }
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Sheet symbols endpoint failed: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to read sheet symbols: {e}"
        )


@app.post("/v1/analyze", response_model=AnalysisResponse)
async def analyze_stock(req: AnalysisRequest) -> AnalysisResponse:
    """Enhanced AI analysis endpoint."""
    try:
        # Placeholder analysis - replace with real AI logic
        base_score = 50.0
        confidence = 0.7
        
        # Simple scoring based on available fundamentals
        if req.fundamentals:
            if req.fundamentals.get("pe_ratio"):
                pe = req.fundamentals["pe_ratio"]
                if isinstance(pe, (int, float)) and 0 < pe < 20:
                    base_score += 10
                elif pe >= 50:
                    base_score -= 10
            
            if req.fundamentals.get("dividend_yield"):
                dy = req.fundamentals["dividend_yield"]
                if isinstance(dy, (int, float)) and dy > 0.03:
                    base_score += 5
        
        # Ensure score is within bounds
        score = max(0, min(100, base_score))
        
        return AnalysisResponse(
            ticker=req.ticker,
            score=score,
            summary=(
                f"Enhanced analysis for {req.ticker}. "
                f"Score: {score:.1f}/100. "
                "This endpoint is ready for advanced AI integration."
            ),
            confidence=confidence,
        )
    except Exception as e:
        logger.error(f"Analysis failed for {req.ticker}: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Analysis failed: {e}"
        )


# -----------------------------------------------------------------------------
# New Multi-Source Analysis and Google Apps Script Endpoints
# -----------------------------------------------------------------------------
@app.get("/v1/multi-source/{symbol}")
async def get_multi_source_analysis(symbol: str):
    """Get analysis from all available data sources."""
    try:
        analysis = analyzer.get_multi_source_analysis(symbol)
        return analysis
    except Exception as e:
        logger.error(f"Multi-source analysis failed for {symbol}: {e}")
        raise HTTPException(status_code=500, detail=f"Analysis failed: {e}")


@app.get("/v1/google-apps-script/symbols")
async def get_google_apps_script_data(symbol: str = None):
    """Get data from Google Apps Script."""
    result = google_apps_script_client.get_symbols_data(symbol)
    
    if result.success:
        return {
            "ok": True,
            "data": result.data,
            "execution_time": result.execution_time
        }
    else:
        raise HTTPException(
            status_code=500,
            detail=f"Google Apps Script error: {result.error}"
        )


@app.get("/v1/apis/status")
async def get_apis_status():
    """Check status of all configured APIs."""
    test_symbol = "7201.SR"  # Test with a Saudi symbol
    
    status = {
        "alpha_vantage": bool(analyzer.apis['alpha_vantage']),
        "finnhub": bool(analyzer.apis['finnhub']),
        "eodhd": bool(analyzer.apis['eodhd']),
        "twelvedata": bool(analyzer.apis['twelvedata']),
        "marketstack": bool(analyzer.apis['marketstack']),
        "fmp": bool(analyzer.apis['fmp']),
        "google_apps_script": bool(os.getenv('GOOGLE_APPS_SCRIPT_URL')),
        "timestamp": datetime.datetime.utcnow().isoformat() + "Z"
    }
    
    return status


# -----------------------------------------------------------------------------
# Server Runner with Enhanced Port Finding
# -----------------------------------------------------------------------------
def find_free_port(start_port: int = 8101, max_attempts: int = 50) -> int:
    """Find a free port starting from start_port."""
    for port in range(start_port, start_port + max_attempts):
        with closing(socket.socket(socket.AF_INET, socket.SOCK_STREAM)) as sock:
            try:
                sock.bind(('0.0.0.0', port))
                return port
            except OSError:
                continue
    # If no free port found, use a random one
    with closing(socket.socket(socket.AF_INET, socket.SOCK_STREAM)) as sock:
        sock.bind(('0.0.0.0', 0))
        return sock.getsockname()[1]


if __name__ == "__main__":
    import uvicorn
    
    # Enhanced configuration
    host = os.getenv("APP_HOST", "0.0.0.0")
    
    try:
        port = int(os.getenv("APP_PORT", "8101"))
    except ValueError:
        port = 8101
        logger.warning(f"Invalid APP_PORT, using default: {port}")

    # Find free port if default is busy
    original_port = port
    port = find_free_port(port)
    if port != original_port:
        logger.info(f"Port {original_port} busy, using port {port}")

    # Enhanced server configuration
    server_config = {
        "app": "main:app",
        "host": host,
        "port": port,
        "reload": False,
        "log_level": "info",
        "access_log": True,
    }
    
    # Development reload if enabled
    if os.getenv("ENVIRONMENT") == "development":
        server_config["reload"] = True
        logger.info("Development mode: auto-reload enabled")

    print(f"🚀 Starting {SERVICE_NAME} v{SERVICE_VERSION} on port {port}")
    
    try:
        uvicorn.run(**server_config)
    except KeyboardInterrupt:
        logger.info("Server stopped by user")
    except Exception as e:
        logger.error(f"Server failed to start: {e}")
        raise
