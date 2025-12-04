#!/usr/bin/env python3
"""
Tadawul Stock Analysis API - Enhanced Version 3.6.1
Production-ready with:
- Hybrid EODHD providers and fundamentals service
- Advanced multi-source AI Trading Analysis (advanced_analysis.py)
- Hard timeouts to avoid endless loading in web/UI
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
        analyzer,                    # Global AdvancedTradingAnalyzer instance
        generate_ai_recommendation,  # async convenience function
        get_multi_source_analysis,   # async convenience function
    )
except Exception:
    analyzer = None

    async def generate_ai_recommendation(symbol: str) -> Dict[str, Any]:
        raise RuntimeError("Analysis engine is not available on this instance")

    async def get_multi_source_analysis(symbol: str) -> Dict[str, Any]:
        raise RuntimeError("Analysis engine is not available on this instance")


# =============================================================================
# Configuration
# =============================================================================

load_dotenv()


class Settings(BaseSettings):
    """Enhanced configuration management with validation and safe defaults."""

    # Service Configuration
    service_name: str = Field("Tadawul Stock Analysis API", env="SERVICE_NAME")
    service_version: str = Field("3.6.1", env="SERVICE_VERSION")
    app_host: str = Field("0.0.0.0", env="APP_HOST")
    app_port: int = Field(8000, env="APP_PORT")
    environment: str = Field("production", env="ENVIRONMENT")
    debug: bool = Field(False, env="DEBUG")

    # Security
    require_auth: bool = Field(True, env="REQUIRE_AUTH")
    app_token: Optional[str] = Field(None, env="APP_TOKEN")
    backup_app_token: Optional[str] = Field(None, env="BACKUP_APP_TOKEN")

    # Rate Limiting
    enable_rate_limiting: bool = Field(True, env="ENABLE_RATE_LIMITING")
    max_requests_per_minute: int = Field(60, env="MAX_REQUESTS_PER_MINUTE")

    # CORS - supports JSON string or comma-separated list
    cors_origins: List[str] = Field(default_factory=lambda: ["*"], env="CORS_ORIGINS")

    # Documentation
    enable_swagger: bool = Field(True, env="ENABLE_SWAGGER")
    enable_redoc: bool = Field(True, env="ENABLE_REDOC")

    # Google Services
    spreadsheet_id: str = Field(..., env="SPREADSHEET_ID")
    google_sheets_credentials: Optional[str] = Field(None, env="GOOGLE_SHEETS_CREDENTIALS")
    google_apps_script_url: Optional[str] = Field(None, env="GOOGLE_APPS_SCRIPT_URL")
    google_apps_script_backup_url: Optional[str] = Field(None, env="GOOGLE_APPS_SCRIPT_BACKUP_URL")

    # Financial APIs (hybrid stack)
    alpha_vantage_api_key: Optional[str] = Field(None, env="ALPHA_VANTAGE_API_KEY")
    finnhub_api_key: Optional[str] = Field(None, env="FINNHUB_API_KEY")

    eodhd_api_key: Optional[str] = Field(None, env="EODHD_API_KEY")
    eodhd_base_url: str = Field("https://eodhistoricaldata.com/api", env="EODHD_BASE_URL")

    fmp_api_key: Optional[str] = Field(None, env="FMP_API_KEY")
    fmp_base_url: str = Field("https://financialmodelingprep.com/api/v3", env="FMP_BASE_URL")

    # Optional future providers (Yahoo / Argaam / Google Finance proxies)
    yahoo_finance_proxy_url: Optional[str] = Field(None, env="YAHOO_FINANCE_PROXY_URL")
    argaam_base_url: Optional[str] = Field(None, env="ARGAAM_BASE_URL")
    argaam_api_key: Optional[str] = Field(None, env="ARGAAM_API_KEY")

    # Cache
    cache_default_ttl: int = Field(1800, env="CACHE_DEFAULT_TTL")
    cache_max_size: int = Field(10000, env="CACHE_MAX_SIZE")
    cache_backup_enabled: bool = Field(True, env="CACHE_BACKUP_ENABLED")
    cache_save_interval: int = Field(10, env="CACHE_SAVE_INTERVAL")  # seconds

    # Performance (more conservative defaults to avoid endless waits)
    http_timeout: int = Field(10, env="HTTP_TIMEOUT")
    max_retries: int = Field(2, env="MAX_RETRIES")
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
            # Try JSON array first: '["https://a","https://b"]'
            if v.startswith("[") and v.endswith("]"):
                try:
                    parsed = json.loads(v)
                    if isinstance(parsed, list):
                        return [str(o).strip() for o in parsed if str(o).strip()]
                except json.JSONDecodeError:
                    pass
            # Fallback: comma-separated list
            return [origin.strip() for origin in v.split(",") if origin.strip()]
        return v

    @property
    def is_production(self) -> bool:
        return self.environment.lower() == "production"

    @property
    def is_development(self) -> bool:
        return self.environment.lower() == "development"

    @property
    def has_google_sheets_access(self) -> bool:
        """Check if we have either direct Sheets access or Apps Script."""
        return bool(self.google_sheets_credentials or self.google_apps_script_url)

    def validate_configuration(self) -> List[str]:
        """
        Validate configuration and return list of *errors*.
        Warnings are logged but do not block startup (except in strict cases).
        """
        errors: List[str] = []
        warnings: List[str] = []

        # Auth validation
        if self.require_auth and not any([self.app_token, self.backup_app_token]):
            if self.is_production:
                errors.append("Authentication required but no APP_TOKEN/BACKUP_APP_TOKEN configured.")
            else:
                warnings.append("Authentication required but no tokens configured (dev).")

        # Google services validation
        if not self.has_google_sheets_access:
            warnings.append("No Google Sheets or Apps Script configured - some features limited.")

        # Apps Script URL sanity
        if self.google_apps_script_url:
            url = self.google_apps_script_url.strip()
            if url in ["", "undefined"]:
                warnings.append("GOOGLE_APPS_SCRIPT_URL looks invalid ('undefined' or empty).")
            elif not url.startswith(("http://", "https://")):
                warnings.append("GOOGLE_APPS_SCRIPT_URL should start with http(s).")

        # Analysis engine availability vs config
        if self.advanced_analysis_enabled and analyzer is None:
            warnings.append(
                "ADVANCED_ANALYSIS_ENABLED=True but advanced_analysis engine is not available "
                "(import failed or not deployed)."
            )

        for w in warnings:
            logging.warning(f"⚠️ {w}")

        return errors

    def log_config_summary(self):
        """Log configuration summary without sensitive values."""
        logging.info(f"🔧 {self.service_name} v{self.service_version}")
        logging.info(f"🌍 Environment: {self.environment} (Production: {self.is_production})")
        logging.info(f"🔐 Auth: {'Enabled' if self.require_auth else 'Disabled'}")
        logging.info(f"⚡ Rate Limiting: {'Enabled' if self.enable_rate_limiting else 'Disabled'}")
        logging.info(f"📊 Google Services: {'Available' if self.has_google_sheets_access else 'Not Available'}")
        logging.info(f"💾 Cache: TTL={self.cache_default_ttl}s, Save Interval={self.cache_save_interval}s")

        configured_apis = sum(
            [
                bool(self.alpha_vantage_api_key),
                bool(self.finnhub_api_key),
                bool(self.eodhd_api_key),
                bool(self.fmp_api_key),
            ]
        )
        logging.info(f"📈 Financial APIs configured: {configured_apis}")
        logging.info(
            f"🤖 Analysis Engine Enabled (config): {self.advanced_analysis_enabled} | "
            f"Available (import): {analyzer is not None}"
        )


# Initialize settings
settings = Settings()

# === Global timeouts (central control) =======================================

# Bounded quote timeout: prevents endless loading from frontend
QUOTE_OVERALL_TIMEOUT_SECONDS = max(6.0, float(settings.http_timeout) + 2.0)

# Bounded analysis timeout for AI endpoints
ANALYSIS_OVERALL_TIMEOUT_SECONDS = max(8.0, float(settings.http_timeout) + 4.0)

# =============================================================================
# Logging Setup
# =============================================================================


def setup_logging():
    """Configure structured logging with request context."""
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

    # Reduce noise from uvicorn access logs
    logging.getLogger("uvicorn.access").disabled = True


setup_logging()
logger = logging.getLogger(__name__)

# =============================================================================
# Data Models
# =============================================================================


class QuoteStatus(str, Enum):
    OK = "OK"
    NO_DATA = "NO_DATA"
    ERROR = "ERROR"


class Quote(BaseModel):
    """Unified quote model for all quote-related endpoints."""

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

    # Fundamental data fields (optional, used if provider returns them)
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
    """Standardized quotes response with metadata."""

    timestamp: datetime.datetime = Field(..., description="Response timestamp")
    symbols: List[Quote] = Field(..., description="List of quotes")
    meta: Optional[Dict[str, Any]] = Field(None, description="Response metadata")


class ErrorResponse(BaseModel):
    """Standardized error response."""

    error: str = Field(..., description="Error type")
    message: str = Field(..., description="Error message")
    detail: Optional[Dict[str, Any]] = Field(None, description="Additional details")
    timestamp: datetime.datetime = Field(..., description="Error timestamp")
    request_id: Optional[str] = Field(None, description="Request ID for debugging")


class HealthResponse(BaseModel):
    """Enhanced health check response."""

    status: str = Field(..., description="Service status")
    version: str = Field(..., description="API version")
    timestamp: datetime.datetime = Field(..., description="Current timestamp")
    uptime_seconds: float = Field(..., description="Service uptime in seconds")
    dependencies: Dict[str, Any] = Field(..., description="Dependency status")


class QuoteRequest(BaseModel):
    """Quote request model for POST refresh."""

    symbols: List[str] = Field(..., description="List of symbols to fetch/refresh")
    cache_ttl: Optional[int] = Field(None, description="Reserved for per-call TTL override")
    providers: Optional[List[str]] = Field(None, description="Preferred providers (future use)")


# =============================================================================
# Enhanced Cache Implementation
# =============================================================================

import threading


class TTLCache:
    """
    Enhanced TTL cache with:
    - Thread safety
    - Batched disk writes
    - Size enforcement
    - Diagnostics (hit rate, errors, etc.)
    """

    def __init__(self, ttl_seconds: int = None, max_size: int = None, save_interval: int = None):
        self.cache_path = Path(__file__).parent / "quote_cache.json"
        self.backup_dir = Path(__file__).parent / "cache_backups"
        self.backup_dir.mkdir(parents=True, exist_ok=True)

        self.data: Dict[str, Dict[str, Any]] = {}
        self.ttl = ttl_seconds or settings.cache_default_ttl
        self.max_size = max_size or settings.cache_max_size
        self.save_interval = save_interval or settings.cache_save_interval

        self._lock = threading.Lock()
        self._dirty = False
        self._last_save = time.time()

        self.metrics = {
            "hits": 0,
            "misses": 0,
            "updates": 0,
            "expired_removals": 0,
            "errors": 0,
            "immediate_saves": 0,
        }

        self._load_cache()

    def _is_valid(self, item: Dict[str, Any], now: Optional[float] = None) -> bool:
        if now is None:
            now = time.time()
        ts = item.get("_cache_timestamp", 0)
        return (now - ts) < self.ttl

    def _load_cache(self) -> bool:
        """Load cache from disk on startup; discard expired entries."""
        try:
            if not self.cache_path.exists():
                logger.info("No existing quote_cache.json file; starting empty cache.")
                return True

            with open(self.cache_path, "r", encoding="utf-8") as f:
                raw_data = json.load(f)

            now = time.time()
            valid_items: Dict[str, Dict[str, Any]] = {}

            # Support both old + new structures
            if isinstance(raw_data, dict) and "data" in raw_data:
                for item in raw_data["data"]:
                    if isinstance(item, dict) and "ticker" in item and self._is_valid(item, now):
                        valid_items[item["ticker"]] = item
            elif isinstance(raw_data, dict):
                for key, item in raw_data.items():
                    if isinstance(item, dict) and self._is_valid(item, now):
                        valid_items[key] = item

            self.data = valid_items
            logger.info(f"✅ Cache loaded with {len(valid_items)} valid items")
            return True

        except Exception as e:
            logger.error(f"❌ Failed to load cache: {e}")
            self.metrics["errors"] += 1
            self.data = {}
            return False

    def _save_cache_immediate(self) -> bool:
        """Immediate save to disk; used by flush() and batched writes."""
        with self._lock:
            try:
                if len(self.data) > self.max_size:
                    self._enforce_size_limit()

                cache_data = {
                    "metadata": {
                        "version": "1.3",
                        "saved_at": datetime.datetime.utcnow().isoformat() + "Z",
                        "item_count": len(self.data),
                        "ttl_seconds": self.ttl,
                    },
                    "data": list(self.data.values()),
                }

                temp_path = self.cache_path.with_suffix(".tmp")
                with open(temp_path, "w", encoding="utf-8") as f:
                    json.dump(cache_data, f, indent=2, ensure_ascii=False, default=str)

                temp_path.replace(self.cache_path)
                self._dirty = False
                self._last_save = time.time()
                self.metrics["immediate_saves"] += 1
                return True

            except Exception as e:
                logger.error(f"❌ Failed to save cache: {e}")
                self.metrics["errors"] += 1
                return False

    def _schedule_save(self):
        """Schedule batched save if threshold reached."""
        now = time.time()
        if self._dirty and (now - self._last_save) >= self.save_interval:
            self._save_cache_immediate()

    def _enforce_size_limit(self):
        """Remove oldest items if cache size exceeds max_size."""
        if len(self.data) <= self.max_size:
            return

        items_to_remove = len(self.data) - self.max_size
        sorted_items = sorted(self.data.items(), key=lambda x: x[1].get("_cache_timestamp", 0))

        for ticker, _ in sorted_items[:items_to_remove]:
            del self.data[ticker]
            self.metrics["expired_removals"] += 1

    def get(self, symbol: str) -> Optional[Dict[str, Any]]:
        """Get quote from cache; returns copy without internal metadata."""
        symbol = symbol.upper().strip()

        with self._lock:
            if symbol in self.data and self._is_valid(self.data[symbol]):
                self.metrics["hits"] += 1
                clean = {k: v for k, v in self.data[symbol].items() if not k.startswith("_")}
                return clean

            # If present but expired -> remove
            if symbol in self.data:
                del self.data[symbol]
                self._dirty = True
                self._schedule_save()

            self.metrics["misses"] += 1
            return None

    def set(self, symbol: str, quote: Dict[str, Any]) -> None:
        """Insert or update a quote in cache."""
        symbol = symbol.upper().strip()
        quote_data = quote.copy()
        quote_data["_cache_timestamp"] = time.time()
        quote_data["_last_updated"] = datetime.datetime.utcnow().isoformat() + "Z"

        with self._lock:
            self.data[symbol] = quote_data
            self.metrics["updates"] += 1
            self._dirty = True
            self._schedule_save()

    def cleanup_expired(self) -> int:
        """Remove expired items; return count removed."""
        now = time.time()
        expired: List[str] = []

        with self._lock:
            expired = [t for t, item in self.data.items() if not self._is_valid(item, now)]

            for t in expired:
                del self.data[t]
                self.metrics["expired_removals"] += 1

            if expired:
                self._dirty = True
                self._schedule_save()
                logger.info(f"🧹 Removed {len(expired)} expired cache entries")

        return len(expired)

    def flush(self) -> bool:
        """Force immediate save to disk if there are pending changes."""
        if self._dirty:
            return self._save_cache_immediate()
        return True

    def get_item_count(self) -> int:
        with self._lock:
            return len(self.data)

    def get_stats(self) -> Dict[str, Any]:
        """Return diagnostic information about cache state/performance."""
        now = time.time()
        with self._lock:
            valid_count = sum(1 for item in self.data.values() if self._is_valid(item, now))
            expired_count = len(self.data) - valid_count

            total_requests = self.metrics["hits"] + self.metrics["misses"]
            hit_rate = (self.metrics["hits"] / total_requests * 100) if total_requests > 0 else 0.0

            return {
                "total_items": len(self.data),
                "valid_items": valid_count,
                "expired_items": expired_count,
                "max_size": self.max_size,
                "ttl_seconds": self.ttl,
                "dirty": self._dirty,
                "last_save_seconds_ago": time.time() - self._last_save,
                "performance": {
                    "hits": self.metrics["hits"],
                    "misses": self.metrics["misses"],
                    "hit_rate": round(hit_rate, 2),
                    "updates": self.metrics["updates"],
                    "expired_removals": self.metrics["expired_removals"],
                    "immediate_saves": self.metrics["immediate_saves"],
                    "errors": self.metrics["errors"],
                },
            }


# Global cache instance
cache = TTLCache()

# =============================================================================
# Enhanced HTTP Client with Retry Logic
# =============================================================================


class HTTPClient:
    """Shared aiohttp client with retry logic and connection pooling."""

    def __init__(self):
        self.session: Optional[aiohttp.ClientSession] = None
        self.timeout = aiohttp.ClientTimeout(total=settings.http_timeout)

    async def get_session(self) -> aiohttp.ClientSession:
        if self.session is None:
            connector = aiohttp.TCPConnector(
                limit=100,
                limit_per_host=20,
                ssl=True,
            )
            self.session = aiohttp.ClientSession(
                timeout=self.timeout,
                connector=connector,
                headers={
                    "User-Agent": f"{settings.service_name}/{settings.service_version}",
                    "Accept": "application/json",
                },
            )
        return self.session

    async def request_with_retry(
        self,
        method: str,
        url: str,
        params: Optional[Dict[str, Any]] = None,
        max_retries: Optional[int] = None,
    ) -> Optional[Any]:
        """Make HTTP request with retry & exponential backoff."""
        max_retries = max_retries or settings.max_retries
        last_exception: Optional[Exception] = None

        for attempt in range(max_retries):
            try:
                session = await self.get_session()
                async with session.request(method, url, params=params) as response:
                    if response.status == 200:
                        return await response.json()
                    else:
                        text = await response.text()
                        last_exception = Exception(f"HTTP {response.status}: {text[:200]}")
            except asyncio.TimeoutError:
                last_exception = Exception(f"Timeout after {settings.http_timeout}s")
            except Exception as e:
                last_exception = e

            if attempt < max_retries - 1:
                wait_time = settings.retry_delay * (2 ** attempt)
                logger.debug(f"Retry {attempt + 1}/{max_retries} in {wait_time:.1f}s for {url}")
                await asyncio.sleep(wait_time)

        logger.error(f"All retries failed for {url}: {last_exception}")
        return None

    async def close(self):
        if self.session:
            await self.session.close()
            self.session = None


# Global HTTP client
http_client = HTTPClient()

# =============================================================================
# Provider Abstractions (Quotes)
# =============================================================================


class ProviderClient(Protocol):
    """Provider interface for quote providers."""

    name: str
    rate_limit: int

    async def get_quote(self, symbol: str) -> Optional[Dict[str, Any]]:
        ...


class AlphaVantageProvider:
    """Alpha Vantage provider implementation (fallback)."""

    def __init__(self):
        self.name = "alpha_vantage"
        self.rate_limit = 5
        self.base_url = "https://www.alphavantage.co/query"
        self.api_key = settings.alpha_vantage_api_key
        self.enabled = bool(self.api_key)

    async def get_quote(self, symbol: str) -> Optional[Dict[str, Any]]:
        if not self.enabled:
            return None

        params = {
            "function": "GLOBAL_QUOTE",
            "symbol": symbol,
            "apikey": self.api_key,
        }

        data = await http_client.request_with_retry("GET", self.base_url, params)
        if data:
            return self._parse_response(data, symbol)
        return None

    def _parse_response(self, data: Dict[str, Any], symbol: str) -> Optional[Dict[str, Any]]:
        quote_data = data.get("Global Quote", {})
        if not quote_data:
            return None

        return {
            "ticker": symbol,
            "price": self._safe_float(quote_data.get("05. price")),
            "previous_close": self._safe_float(quote_data.get("08. previous close")),
            "change_value": self._safe_float(quote_data.get("09. change")),
            "change_percent": self._safe_float(str(quote_data.get("10. change percent", "0")).rstrip("%")),
            "volume": self._safe_int(quote_data.get("06. volume")),
            "open_price": self._safe_float(quote_data.get("02. open")),
            "high_price": self._safe_float(quote_data.get("03. high")),
            "low_price": self._safe_float(quote_data.get("04. low")),
            "provider": self.name,
        }

    def _safe_float(self, value: Any) -> Optional[float]:
        try:
            return float(value) if value not in (None, "") else None
        except (ValueError, TypeError):
            return None

    def _safe_int(self, value: Any) -> Optional[int]:
        try:
            return int(float(value)) if value not in (None, "") else None
        except (ValueError, TypeError):
            return None


class FinnhubProvider:
    """Finnhub provider implementation (global equities)."""

    def __init__(self):
        self.name = "finnhub"
        self.rate_limit = 60
        self.base_url = "https://finnhub.io/api/v1"
        self.api_key = settings.finnhub_api_key
        self.enabled = bool(self.api_key)

    async def get_quote(self, symbol: str) -> Optional[Dict[str, Any]]:
        if not self.enabled:
            return None

        params = {"symbol": symbol, "token": self.api_key}
        data = await http_client.request_with_retry("GET", f"{self.base_url}/quote", params)
        if data:
            return self._parse_response(data, symbol)
        return None

    def _parse_response(self, data: Dict[str, Any], symbol: str) -> Optional[Dict[str, Any]]:
        if not data or "c" not in data:
            return None

        timestamp = None
        if data.get("t"):
            try:
                timestamp = datetime.datetime.fromtimestamp(data["t"]).isoformat()
            except Exception:
                timestamp = None

        return {
            "ticker": symbol,
            "price": data.get("c"),
            "change_value": data.get("d"),
            "change_percent": data.get("dp"),
            "high_price": data.get("h"),
            "low_price": data.get("l"),
            "open_price": data.get("o"),
            "previous_close": data.get("pc"),
            "provider": self.name,
            "as_of": timestamp,
        }


class FMPProvider:
    """
    Financial Modeling Prep provider for quotes (DISABLED).
    Kept only for clarity; fundamentals use /profile endpoint via FundamentalsService.
    """

    def __init__(self):
        self.name = "fmp"
        self.rate_limit = 60
        self.base_url = settings.fmp_base_url.rstrip("/")
        self.api_key = settings.fmp_api_key

        # Quotes disabled due to 403 "Legacy endpoint" issues on /quote
        self.enabled = False
        if self.api_key:
            logger.info(
                "FMP quote provider is DISABLED (legacy /quote endpoint). "
                "FMP remains active for fundamentals via /profile."
            )

    async def get_quote(self, symbol: str) -> Optional[Dict[str, Any]]:
        return None


class EODHDProvider:
    """
    EOD Historical Data provider for real-time quotes.

    Endpoint: /real-time/{symbol}?api_token=...&fmt=json
    """

    def __init__(self):
        self.name = "eodhd"
        self.rate_limit = 1000  # logical default; real limit depends on plan
        self.base_url = settings.eodhd_base_url.rstrip("/")
        self.api_key = settings.eodhd_api_key
        self.enabled = bool(self.api_key)

    async def get_quote(self, symbol: str) -> Optional[Dict[str, Any]]:
        if not self.enabled:
            return None

        url = f"{self.base_url}/real-time/{symbol}"
        params = {
            "api_token": self.api_key,
            "fmt": "json",
        }

        data = await http_client.request_with_retry("GET", url, params)
        if not data or not isinstance(data, dict):
            return None

        return self._parse_quote(data, symbol)

    def _parse_quote(self, item: Dict[str, Any], symbol: str) -> Optional[Dict[str, Any]]:
        price = self._safe_float(
            item.get("close")
            or item.get("price")
            or item.get("adjusted_close")
            or item.get("last")
        )
        if price is None:
            return None

        ts = None
        ts_raw = item.get("timestamp")
        if ts_raw:
            try:
                ts = datetime.datetime.fromtimestamp(int(ts_raw)).isoformat() + "Z"
            except Exception:
                ts = None

        return {
            "ticker": symbol,
            "price": price,
            "previous_close": self._safe_float(item.get("previousClose") or item.get("previous_close")),
            "change_value": self._safe_float(item.get("change")),
            "change_percent": self._safe_float(item.get("change_p") or item.get("changePercent")),
            "volume": self._safe_float(item.get("volume")),
            "open_price": self._safe_float(item.get("open")),
            "high_price": self._safe_float(item.get("high")),
            "low_price": self._safe_float(item.get("low")),
            "market_cap": self._safe_float(item.get("market_cap") or item.get("marketCap")),
            "currency": item.get("currency") or item.get("currencyCode"),
            "exchange": item.get("exchange_short_name") or item.get("exchange"),
            "provider": self.name,
            "as_of": ts,
        }

    def _safe_float(self, value: Any) -> Optional[float]:
        try:
            return float(value) if value not in (None, "") else None
        except (ValueError, TypeError):
            return None


class ProviderManager:
    """
    Manager to orchestrate providers and implement fallback logic.
    Provider order is important.
    """

    def __init__(self):
        # Order:
        # 1) EODHD (primary for your upgraded plan)
        # 2) FMP (disabled for quotes; fundamentals only)
        # 3) Finnhub
        # 4) Alpha Vantage
        self.providers: List[ProviderClient] = [
            EODHDProvider(),
            FMPProvider(),   # remains in list but enabled=False so skipped
            FinnhubProvider(),
            AlphaVantageProvider(),
        ]

        self.enabled_providers = [p for p in self.providers if getattr(p, "enabled", True)]
        logger.info(f"✅ Enabled providers for quotes: {[p.name for p in self.enabled_providers]}")

    async def get_quote(self, symbol: str) -> Optional[Dict[str, Any]]:
        """Get quote from first provider that returns valid price."""
        errors = []

        for provider in self.enabled_providers:
            try:
                quote = await provider.get_quote(symbol)
                if quote and quote.get("price") is not None:
                    logger.info(f"✅ Quote for {symbol} from {provider.name}")
                    return quote
                else:
                    errors.append(f"{provider.name}: no data")
            except Exception as e:
                errors.append(f"{provider.name}: {e}")
                logger.warning(f"Provider {provider.name} failed for {symbol}: {e}")
                continue

        logger.warning(f"❌ All providers failed for {symbol}: {', '.join(errors)}")
        return None

    async def get_quotes(self, symbols: List[str]) -> List[Dict[str, Any]]:
        """Get multiple quotes in parallel with safe error handling."""
        tasks = [self.get_quote(sym) for sym in symbols]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        quotes: List[Dict[str, Any]] = []
        for sym, res in zip(symbols, results):
            if isinstance(res, dict):
                quotes.append(res)
            elif isinstance(res, Exception):
                logger.error(f"Quote fetch error for {sym}: {res}")
            else:
                logger.debug(f"No data available for {sym}")
        return quotes


# Global provider manager
provider_manager = ProviderManager()

# =============================================================================
# Tadawul Helper
# =============================================================================


def is_tadawul_symbol(symbol: str) -> bool:
    """
    Basic Tadawul (.SR) detector.
    """
    if not symbol:
        return False
    return symbol.strip().upper().endswith(".SR")


# =============================================================================
# Quote Service Layer
# =============================================================================


class QuoteService:
    """Service layer orchestrating cache + provider manager."""

    def __init__(self, cache: TTLCache, provider_manager: ProviderManager):
        self.cache = cache
        self.provider_manager = provider_manager

    async def get_quote(self, symbol: str) -> Quote:
        symbol_norm = symbol.strip().upper()

        # Tadawul handling: current provider stack doesn't support .SR
        if is_tadawul_symbol(symbol_norm):
            logger.info(f"Tadawul symbol detected (no market data plan): {symbol_norm}")
            return Quote(
                ticker=symbol_norm,
                status=QuoteStatus.NO_DATA,
                currency="SAR",
                exchange="TADAWUL",
                message=(
                    "Tadawul (.SR) market data is not enabled on the current data provider plan. "
                    "Global / US symbols (e.g. AAPL, MSFT, GOOGL, AMZN) are supported."
                ),
            )

        # Cache first
        cached = self.cache.get(symbol_norm)
        if cached:
            logger.debug(f"Cache hit for {symbol_norm}")
            return self._create_quote_model(cached, QuoteStatus.OK)

        # Provider fallback
        try:
            provider_data = await self.provider_manager.get_quote(symbol_norm)
            if provider_data:
                self.cache.set(symbol_norm, provider_data)
                return self._create_quote_model(provider_data, QuoteStatus.OK)
            else:
                return Quote(
                    ticker=symbol_norm,
                    status=QuoteStatus.NO_DATA,
                    message="No data available from providers",
                )
        except Exception as e:
            logger.error(f"Error fetching quote for {symbol_norm}: {e}")
            return Quote(
                ticker=symbol_norm,
                status=QuoteStatus.ERROR,
                message=f"Error fetching data: {e}",
            )

    async def get_quotes(self, symbols: List[str]) -> QuotesResponse:
        """
        Get multiple quotes with:
        - Separate treatment for Tadawul vs global
        - Cache hits recorded
        - Single provider batch for misses
        """
        cache_hits: List[Tuple[str, Dict[str, Any]]] = []
        cache_misses: List[str] = []
        sources_used = set()

        tadawul_quotes: List[Quote] = []
        normal_symbols: List[str] = []

        # 1) Split into Tadawul vs non-Tadawul
        for raw in symbols:
            sym = raw.strip().upper()
            if not sym:
                continue
            if is_tadawul_symbol(sym):
                tadawul_quotes.append(
                    Quote(
                        ticker=sym,
                        status=QuoteStatus.NO_DATA,
                        currency="SAR",
                        exchange="TADAWUL",
                        message=(
                            "Tadawul (.SR) market data is not enabled on the current data provider plan. "
                            "Global / US symbols (e.g. AAPL, MSFT, GOOGL, AMZN) are supported."
                        ),
                    )
                )
            else:
                normal_symbols.append(sym)

        # 2) Cache check for global symbols
        for sym in normal_symbols:
            cached = self.cache.get(sym)
            if cached:
                cache_hits.append((sym, cached))
                if cached.get("provider"):
                    sources_used.add(cached["provider"])
            else:
                cache_misses.append(sym)

        # 3) Provider calls for misses
        provider_quotes: List[Dict[str, Any]] = []
        if cache_misses:
            provider_quotes = await self.provider_manager.get_quotes(cache_misses)
            for q in provider_quotes:
                if q and q.get("price") is not None:
                    s = q["ticker"]
                    self.cache.set(s, q)
                    if q.get("provider"):
                        sources_used.add(q["provider"])

        # 4) Build final list for global symbols
        quotes: List[Quote] = []

        # Cache hits first
        for sym, data in cache_hits:
            quotes.append(self._create_quote_model(data, QuoteStatus.OK))

        # Provider results
        provider_map = {q["ticker"]: q for q in provider_quotes if q and "ticker" in q}
        for sym in cache_misses:
            if sym in provider_map:
                quotes.append(self._create_quote_model(provider_map[sym], QuoteStatus.OK))
            else:
                quotes.append(
                    Quote(
                        ticker=sym,
                        status=QuoteStatus.NO_DATA,
                        message="No data available from providers",
                    )
                )

        # 5) Combine with Tadawul pseudo-quotes
        all_quotes = quotes + tadawul_quotes

        meta = {
            "cache_hits": len(cache_hits),
            "cache_misses": len(cache_misses),
            "provider_successes": len(provider_map),
            "sources": list(sources_used),
            "total_symbols": len(symbols),
            "successful_quotes": len([q for q in all_quotes if q.status == QuoteStatus.OK]),
            "all_no_data": all(q.status != QuoteStatus.OK for q in all_quotes),
        }

        return QuotesResponse(
            timestamp=datetime.datetime.utcnow(),
            symbols=all_quotes,
            meta=meta,
        )

    def _create_quote_model(self, data: Dict[str, Any], status: QuoteStatus) -> Quote:
        """Map provider/cache dict into Quote model, handling as_of timestamp safely."""
        as_of = data.get("as_of")
        if as_of and isinstance(as_of, str):
            try:
                as_of = datetime.datetime.fromisoformat(as_of.replace("Z", "+00:00"))
            except (ValueError, TypeError):
                as_of = datetime.datetime.utcnow()
        elif not isinstance(as_of, datetime.datetime):
            as_of = datetime.datetime.utcnow()

        return Quote(
            ticker=data.get("ticker", ""),
            status=status,
            price=data.get("price"),
            previous_close=data.get("previous_close"),
            change_value=data.get("change_value"),
            change_percent=data.get("change_percent"),
            volume=data.get("volume"),
            market_cap=data.get("market_cap"),
            open_price=data.get("open_price"),
            high_price=data.get("high_price"),
            low_price=data.get("low_price"),
            pe_ratio=data.get("pe_ratio"),
            pb_ratio=data.get("pb_ratio"),
            roe=data.get("roe"),
            eps=data.get("eps"),
            currency=data.get("currency"),
            exchange=data.get("exchange"),
            sector=data.get("sector"),
            country=data.get("country"),
            provider=data.get("provider"),
            as_of=as_of,
        )

    def update_cache(self, quotes: List[Quote]) -> Dict[str, Any]:
        """Update cache from Quote models (used by /v1/quote/update)."""
        updated = 0
        errors: List[str] = []

        for q in quotes:
            if q.status == QuoteStatus.OK and q.price is not None:
                try:
                    self.cache.set(q.ticker, q.dict())
                    updated += 1
                except Exception as e:
                    errors.append(f"Failed to update {q.ticker}: {e}")

        return {
            "updated_count": updated,
            "errors": errors,
            "total_cached": cache.get_item_count(),
        }


# Global quote service
quote_service = QuoteService(cache, provider_manager)

# =============================================================================
# Fundamentals Service (Hybrid EODHD + FMP)
# =============================================================================


class FundamentalsService:
    """
    Fundamentals lookup:
    - Tadawul (.SR): currently returns NO_DATA with explanation.
    - Global: EODHD fundamentals, fallback to FMP profile.
    """

    async def get_fundamentals(self, symbol: str) -> Dict[str, Any]:
        symbol_norm = symbol.strip().upper()

        base: Dict[str, Any] = {
            "ticker": symbol_norm,
            "status": "NO_DATA",
            "provider": None,
            "data": {},
            "message": None,
        }

        if is_tadawul_symbol(symbol_norm):
            base["message"] = (
                "Fundamentals for Tadawul (.SR) symbols are not available from the current "
                "providers. Use static metadata or connect an Argaam/Tadawul-capable API."
            )
            return base

        # 1) Try EODHD fundamentals
        if settings.eodhd_api_key:
            try:
                eod_data = await self._get_eod_fundamentals(symbol_norm)
                if eod_data:
                    base["status"] = "OK"
                    base["provider"] = "eodhd"
                    base["data"] = eod_data
                    base["message"] = "Fundamentals from EOD Historical Data"
                    return base
            except Exception as e:
                logger.warning(f"EODHD fundamentals failed for {symbol_norm}: {e}")

        # 2) FMP profile fallback
        if settings.fmp_api_key:
            try:
                fmp_data = await self._get_fmp_fundamentals(symbol_norm)
                if fmp_data:
                    base["status"] = "OK"
                    base["provider"] = "fmp"
                    base["data"] = fmp_data
                    base["message"] = "Fundamentals from Financial Modeling Prep"
                    return base
            except Exception as e:
                logger.warning(f"FMP fundamentals failed for {symbol_norm}: {e}")

        base["message"] = "No fundamentals available from configured providers"
        return base

    async def _get_eod_fundamentals(self, symbol: str) -> Optional[Dict[str, Any]]:
        url = f"{settings.eodhd_base_url}/fundamentals/{symbol}"
        params = {"api_token": settings.eodhd_api_key, "fmt": "json"}

        raw = await http_client.request_with_retry("GET", url, params)
        if not raw or not isinstance(raw, dict):
            return None

        general = raw.get("General") or {}
        highlights = raw.get("Highlights") or {}
        valuation = raw.get("Valuation") or {}
        shares = raw.get("SharesStats") or {}
        growth = raw.get("Growth") or {}

        result: Dict[str, Any] = {
            "company_name": general.get("Name"),
            "sector": general.get("Sector"),
            "sub_sector": general.get("Industry"),
            "currency": general.get("CurrencyCode") or general.get("Currency"),
            "market": general.get("Exchange") or general.get("ExchangeShortName"),
            "isin": general.get("ISIN"),
            "shares_outstanding": shares.get("SharesOutstanding"),
            "shares_float": shares.get("SharesFloat"),
            "market_cap": highlights.get("MarketCapitalization") or valuation.get("MarketCapitalization"),
            "eps": highlights.get("EarningsShare"),
            "pe_ratio": highlights.get("PERatio"),
            "peg_ratio": highlights.get("PEGRatio"),
            "dividend_yield": highlights.get("DividendYield"),
            "dividend_payout_ratio": highlights.get("DividendPayoutRatio"),
            "roe": highlights.get("ReturnOnEquityTTM"),
            "roa": highlights.get("ReturnOnAssetsTTM"),
            "debt_to_equity": highlights.get("DebtEquityRatio") or valuation.get("DebtToEquity"),
            "current_ratio": highlights.get("CurrentRatio"),
            "quick_ratio": highlights.get("QuickRatio"),
            "price_to_sales": valuation.get("PriceSalesTTM"),
            "price_to_book": valuation.get("PriceBookMRQ"),
            "ev_to_ebitda": valuation.get("EVToEBITDA"),
            "price_to_cash_flow": valuation.get("PriceCashFlowTTM"),
            "revenue_growth": growth.get("RevenueYoY"),
            "net_income_growth": growth.get("NetIncomeYoY"),
        }
        return result

    async def _get_fmp_fundamentals(self, symbol: str) -> Optional[Dict[str, Any]]:
        url = f"{settings.fmp_base_url.rstrip('/')}/profile/{symbol}"
        params = {"apikey": settings.fmp_api_key}

        data = await http_client.request_with_retry("GET", url, params)
        if not data:
            return None

        item: Optional[Dict[str, Any]] = None
        if isinstance(data, list) and data:
            item = data[0]
        elif isinstance(data, dict):
            item = data

        if not item:
            return None

        def sfloat(v: Any) -> Optional[float]:
            try:
                return float(v) if v not in (None, "") else None
            except (TypeError, ValueError):
                return None

        result: Dict[str, Any] = {
            "company_name": item.get("companyName") or item.get("name"),
            "sector": item.get("sector"),
            "sub_sector": item.get("industry"),
            "currency": item.get("currency") or item.get("currencyCode"),
            "market": item.get("exchangeShortName") or item.get("exchange"),
            "isin": item.get("isin"),
            "shares_outstanding": sfloat(item.get("sharesOutstanding")),
            "market_cap": sfloat(item.get("mktCap")),
            "eps": sfloat(item.get("eps")),
            "pe_ratio": sfloat(item.get("pe")),
            "dividend_yield": sfloat(item.get("lastDiv")),
        }
        return result


# Global fundamentals service
fundamentals_service = FundamentalsService()

# =============================================================================
# Security & Auth
# =============================================================================

security = HTTPBearer(auto_error=False)
limiter = Limiter(key_func=get_remote_address)


def get_token_hash(token: str) -> str:
    """Hash token for logging without revealing it."""
    return hashlib.sha256(token.encode()).hexdigest()[:16]

def verify_auth(
    request: Request,
    credentials: Optional[HTTPAuthorizationCredentials] = Depends(security),
) -> bool:
    """
    Authentication supporting:
    - Authorization: Bearer <token>
    - Authorization: <token>
    - X-APP-TOKEN / X_APP_TOKEN headers (for legacy clients / Sheets)
    - ?token=<token> query parameter
    """
    if not settings.require_auth:
        return True

    # All configured valid tokens
    valid_tokens = [t for t in [settings.app_token, settings.backup_app_token] if t]

    # If nothing configured at all → configuration error (500)
    if not valid_tokens:
        if settings.is_production:
            raise HTTPException(
                status_code=500,
                detail="Authentication misconfigured - no APP_TOKEN/BACKUP_APP_TOKEN configured",
            )
        else:
            logger.warning("REQUIRE_AUTH=True but no tokens configured (DEV) -> allowing access")
            return True

    token: Optional[str] = None

    # 1) Standard Bearer token: Authorization: Bearer <token>
    if credentials:
        token = credentials.credentials.strip()

    # 2) token query parameter: ?token=<token>
    if not token:
        token = request.query_params.get("token", "").strip()

    # 3) Legacy header: X-APP-TOKEN / X_APP_TOKEN (for PowerShell / Sheets)
    if not token:
        token = (
            request.headers.get("X-APP-TOKEN", "").strip()
            or request.headers.get("X_APP_TOKEN", "").strip()
        )

    # 4) Raw Authorization header (no "Bearer ")
    if not token:
        raw = request.headers.get("Authorization", "").strip()
        if raw and not raw.lower().startswith("bearer "):
            token = raw

    # Still nothing → 401
    if not token:
        logger.warning("Authentication required but no token provided")
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Missing authentication token",
        )

    # Invalid token
    if token not in valid_tokens:
        token_hash = get_token_hash(token)
        logger.warning(f"Invalid token attempt: {token_hash}")
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid authentication token",
        )

    token_hash = get_token_hash(token)
    logger.info(f"Successful authentication (hash={token_hash})")
    return True



def rate_limit(rule: str):
    """Conditional rate limiting decorator wrapper."""
    if not settings.enable_rate_limiting:
        def noop(fn):
            return fn
        return noop

    return limiter.limit(rule)


# =============================================================================
# Custom Rate Limit Handler
# =============================================================================


async def custom_rate_limit_handler(request: Request, exc: RateLimitExceeded):
    """Custom rate limit handler with structured JSON response."""
    request_id = getattr(request.state, "request_id", "unknown")

    return JSONResponse(
        status_code=429,
        content=ErrorResponse(
            error="Rate limit exceeded",
            message="Too many requests. Please try again later.",
            detail={
                "limit": str(exc.limit),
                "retry_after": getattr(exc, "retry_after", None),
            },
            timestamp=datetime.datetime.utcnow(),
            request_id=request_id,
        ).dict(),
        headers={"Retry-After": str(getattr(exc, "retry_after", 60))},
    )


# =============================================================================
# FastAPI Application Setup
# =============================================================================

APP_START_TIME = time.time()


@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    Lifespan: validate configuration, warm cache, cleanly shutdown.
    """
    startup_time = time.time()

    try:
        # 1) Validate config
        config_errors = settings.validate_configuration()
        if config_errors:
            msg = "Configuration errors:\n" + "\n".join(f"- {e}" for e in config_errors)
            if settings.is_production:
                logger.error(f"❌ {msg}")
                raise RuntimeError("Production configuration invalid")
            else:
                logger.warning(f"⚠️ {msg}")

        # 2) Log summary
        settings.log_config_summary()

        # 3) Cleanup expired cache entries
        expired_count = cache.cleanup_expired()
        if expired_count:
            logger.info(f"🧹 Cleaned {expired_count} expired cache entries on startup")

        logger.info("✅ Application startup completed successfully")

    except Exception as e:
        logger.error(f"❌ Application startup failed: {e}")
        if settings.is_production:
            raise

    # Hand off to request handling
    yield

    # Shutdown logic
    try:
        cache.flush()
        logger.info("✅ Cache flushed to disk on shutdown")

        await http_client.close()
        logger.info("✅ HTTP client closed")

        if analyzer is not None:
            try:
                await analyzer.close()
                logger.info("✅ Advanced analysis engine closed")
            except Exception as e:
                logger.warning(f"⚠️ Error closing analysis engine: {e}")
    except Exception as e:
        logger.warning(f"⚠️ Error during shutdown: {e}")

    uptime = time.time() - startup_time    # noqa: F841
    logger.info(f"🛑 Application shutdown after {uptime:.2f} seconds")


app = FastAPI(
    title=settings.service_name,
    version=settings.service_version,
    description="""
Enhanced Tadawul Stock Analysis API with EODHD-based data providers,
fundamentals integration, and advanced AI-powered trading analysis.

Key features:
- 📈 Real-time global stock quotes (EODHD, Finnhub, Alpha Vantage)
- 📊 Fundamentals endpoint (EODHD + FMP hybrid)
- 🤖 Multi-source AI analysis & recommendations (advanced_analysis.py)
- 🧠 Enriched quote endpoint with data quality & scoring
- 💾 Persistent TTL cache with batched disk writes
- 🔐 Token-based authentication
- ⚡ Async/await architecture with hard request timeouts
""",
    docs_url="/docs" if settings.enable_swagger else None,
    redoc_url="/redoc" if settings.enable_redoc else None,
    lifespan=lifespan,
    default_response_class=ORJSONResponse,
)

# Attach routers
app.include_router(argaam_router)
app.include_router(enriched_quote_router)
app.include_router(ai_analysis_router)

# CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.cors_origins,
    allow_credentials=True,
    allow_methods=["GET", "POST", "PUT", "DELETE", "OPTIONS"],
    allow_headers=["*"],
)

# SlowAPI
app.state.limiter = limiter
app.add_exception_handler(RateLimitExceeded, custom_rate_limit_handler)
# SlowAPIMiddleware reads app.state.limiter internally
app.add_middleware(SlowAPIMiddleware)

# =============================================================================
# Middleware
# =============================================================================


@app.middleware("http")
async def add_request_id(request: Request, call_next):
    """Attach request ID + simple timing for each request."""
    request_id = str(uuid.uuid4())
    request.state.request_id = request_id

    start = time.time()
    response = await call_next(request)
    duration_ms = (time.time() - start) * 1000

    logger.info(
        f"{request.method} {request.url.path} -> {response.status_code} "
        f"[{duration_ms:.1f}ms] [req_id={request_id}]"
    )

    response.headers["X-Request-ID"] = request_id
    response.headers["X-Process-Time"] = f"{duration_ms:.1f}ms"
    return response


# =============================================================================
# Exception Handlers
# =============================================================================


@app.exception_handler(HTTPException)
async def http_exception_handler(request: Request, exc: HTTPException):
    request_id = getattr(request.state, "request_id", "unknown")

    logger.warning(
        f"HTTP {exc.status_code} {request.method} {request.url.path} "
        f"[req_id={request_id}]: {exc.detail}"
    )

    return JSONResponse(
        status_code=exc.status_code,
        content=ErrorResponse(
            error=str(exc.detail),
            message=str(exc.detail),
            timestamp=datetime.datetime.utcnow(),
            request_id=request_id,
        ).dict(),
    )


@app.exception_handler(Exception)
async def global_exception_handler(request: Request, exc: Exception):
    request_id = getattr(request.state, "request_id", "unknown")

    logger.error(
        f"Unhandled error {request.method} {request.url.path} "
        f"[req_id={request_id}]: {exc}",
        exc_info=settings.debug,
    )

    return JSONResponse(
        status_code=500,
        content=ErrorResponse(
            error="Internal server error",
            message=str(exc) if settings.debug else "An internal error occurred",
            timestamp=datetime.datetime.utcnow(),
            request_id=request_id,
        ).dict(),
    )


# =============================================================================
# Core API Endpoints
# =============================================================================


@app.get("/", response_model=Dict[str, Any])
@rate_limit(f"{settings.max_requests_per_minute}/minute")
async def root(request: Request):
    """Root endpoint: shows status, key endpoints, and basic cache metrics."""
    cache_stats = cache.get_stats()

    example_curl = (
        'curl "https://YOUR-SERVICE.onrender.com/v1/quote?'
        'tickers=AAPL,MSFT&token=YOUR_APP_TOKEN"'
    )

    return {
        "service": settings.service_name,
        "version": settings.service_version,
        "status": "operational",
        "environment": settings.environment,
        "timestamp": datetime.datetime.utcnow().isoformat() + "Z",
        "authentication_required": settings.require_auth,
        "rate_limiting": settings.enable_rate_limiting,
        "cache": {
            "total_items": cache_stats["total_items"],
            "valid_items": cache_stats["valid_items"],
            "hit_rate": cache_stats["performance"]["hit_rate"],
        },
        "documentation": "/docs" if settings.enable_swagger else None,
        "endpoints": {
            "health": "/health",
            "ping": "/ping",
            "quotes_v1": "/v1/quote",
            "quotes_v41": "/v41/quotes",
            "fundamentals": "/v1/fundamentals",
            "enriched_quote": "/v1/enriched-quote",
            "cache_info": "/v1/cache/info",
            # Built-in analysis endpoints
            "analysis_multi_source": "/v1/analysis/multi-source",
            "analysis_recommendation": "/v1/analysis/recommendation",
            "analysis_cache_info": "/v1/analysis/cache/info",
            "analysis_cache_clear": "/v1/analysis/cache/clear",
            # AI router endpoints
            "ai_recommendation": "/v1/ai/recommendation",
            "ai_multi_source": "/v1/ai/multi-source",
            "ai_cache_info": "/v1/ai/cache/info",
            "ai_cache_clear": "/v1/ai/cache/clear",
            "saudi_market": "/api/saudi/market",
            "debug_simple_quote": "/debug/simple-quote",
            "debug_saudi_market": "/debug/saudi-market",
        },
        "example_curl": example_curl,
    }


@app.get("/health", response_model=HealthResponse)
@rate_limit("30/minute")
async def health_check(request: Request):
    """
    Health check with dependency diagnostics.
    NOTE: does NOT require auth, so it's easy to test from PowerShell/Sheets.
    """
    cache_stats = cache.get_stats()

    analysis_cache_info: Optional[Dict[str, Any]] = None
    analysis_status = False

    if analyzer is not None and settings.advanced_analysis_enabled:
        try:
            analysis_cache_info = await analyzer.get_cache_info()
            analysis_status = True
        except Exception as e:
            logger.warning(f"⚠️ Analysis engine cache info failed: {e}")
            analysis_cache_info = {"error": str(e)}
            analysis_status = False

    dependencies = {
        "cache": {
            "status": True,
            "mode": "memory_persistent",
            "items": cache_stats["total_items"],
            "hit_rate": cache_stats["performance"]["hit_rate"],
        },
        "google_services": {
            "status": settings.has_google_sheets_access,
            "modes": {
                "direct_sheets": bool(settings.google_sheets_credentials),
                "apps_script": bool(settings.google_apps_script_url),
            },
        },
        "financial_apis": {
            "status": len(provider_manager.enabled_providers) > 0,
            "providers": [p.name for p in provider_manager.enabled_providers],
            "count": len(provider_manager.enabled_providers),
        },
        "analysis_engine": {
            "status": analysis_status,
            "available": analyzer is not None,
            "enabled": settings.advanced_analysis_enabled,
            "cache": analysis_cache_info,
        },
    }

    critical_services_ok = all(
        [
            dependencies["cache"]["status"],
            dependencies["financial_apis"]["status"],
        ]
    )
    health_status = "healthy" if critical_services_ok else "degraded"

    return HealthResponse(
        status=health_status,
        version=settings.service_version,
        timestamp=datetime.datetime.utcnow(),
        uptime_seconds=time.time() - APP_START_TIME,
        dependencies=dependencies,
    )


@app.get("/ping")
@rate_limit("120/minute")
async def ping(request: Request):
    """Simple connectivity check."""
    return {
        "status": "ok",
        "service": settings.service_name,
        "version": settings.service_version,
        "timestamp": datetime.datetime.utcnow().isoformat(),
        "request_id": getattr(request.state, "request_id", "unknown"),
    }


@app.get("/v1/quote", response_model=QuotesResponse)
@rate_limit("60/minute")
async def get_quotes_v1(
    request: Request,
    tickers: str = Query(..., description="Comma-separated ticker symbols"),
    auth: bool = Depends(verify_auth),
):
    """
    Get quotes for multiple symbols with cache + providers.

    - Up to 100 symbols per call.
    - Hard overall timeout to avoid endless loading.
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
            now = datetime.datetime.utcnow()
            logger.warning(
                f"Quote request timeout after {QUOTE_OVERALL_TIMEOUT_SECONDS}s "
                f"for symbols={symbols}"
            )
            quotes = [
                Quote(
                    ticker=sym,
                    status=QuoteStatus.ERROR,
                    message=(
                        f"Timeout while contacting data providers "
                        f"(overall={QUOTE_OVERALL_TIMEOUT_SECONDS}s). "
                        "Try fewer symbols or retry later."
                    ),
                )
                for sym in symbols
            ]
            return QuotesResponse(
                timestamp=now,
                symbols=quotes,
                meta={
                    "timeout": True,
                    "timeout_seconds": QUOTE_OVERALL_TIMEOUT_SECONDS,
                    "total_symbols": len(symbols),
                    "successful_quotes": 0,
                },
            )

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error fetching quotes: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")


@app.get("/v41/quotes", response_model=QuotesResponse)
@rate_limit("60/minute")
async def get_quotes_v41(
    request: Request,
    symbols: str = Query(..., description="Comma-separated ticker symbols"),
    auth: bool = Depends(verify_auth),
):
    """
    v4.1-compatible quotes endpoint; same logic as /v1/quote but
    with explicit v4.1 metadata in response.
    """
    try:
        tickers = [t.strip().upper() for t in symbols.split(",") if t.strip()]

        if not tickers:
            raise HTTPException(status_code=400, detail="No symbols provided")

        try:
            response = await asyncio.wait_for(
                quote_service.get_quotes(tickers),
                timeout=QUOTE_OVERALL_TIMEOUT_SECONDS,
            )
        except asyncio.TimeoutError:
            now = datetime.datetime.utcnow()
            logger.warning(
                f"v4.1 quote request timeout after {QUOTE_OVERALL_TIMEOUT_SECONDS}s "
                f"for symbols={tickers}"
            )
            quotes = [
                Quote(
                    ticker=sym,
                    status=QuoteStatus.ERROR,
                    message=(
                        f"Timeout while contacting data providers "
                        f"(overall={QUOTE_OVERALL_TIMEOUT_SECONDS}s). "
                        "Try fewer symbols or retry later."
                    ),
                )
                for sym in tickers
            ]
            response = QuotesResponse(
                timestamp=now,
                symbols=quotes,
                meta={
                    "timeout": True,
                    "timeout_seconds": QUOTE_OVERALL_TIMEOUT_SECONDS,
                    "total_symbols": len(tickers),
                    "successful_quotes": 0,
                },
            )

        if response.meta is None:
            response.meta = {}
        response.meta["compatibility"] = "v4.1"
        response.meta["api_version"] = "4.1"
        return response

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error in v41 quotes: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")


@app.post("/v1/quote/update")
@rate_limit("30/minute")
async def update_quotes(
    request: Request,                     # must include request for SlowAPI
    request_body: QuoteRequest,
    auth: bool = Depends(verify_auth),
):
    """
    Refresh quotes for given symbols (cache-aware).
    """
    try:
        response = await quote_service.get_quotes(request_body.symbols)
        result = {
            "status": "success",
            "refreshed_symbols": len(request_body.symbols),
            "successful_quotes": len([q for q in response.symbols if q.status == QuoteStatus.OK]),
            "cache_size": cache.get_item_count(),
            "timestamp": response.timestamp.isoformat(),
        }
        logger.info(f"Refreshed {len(request_body.symbols)} quotes in cache")
        return result

    except Exception as e:
        logger.error(f"Error updating quotes: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")


@app.get("/v1/cache/info")
@rate_limit("20/minute")
async def get_cache_info(
    request: Request,
    auth: bool = Depends(verify_auth),
):
    """Return detailed cache statistics."""
    return cache.get_stats()


@app.post("/v1/cache/cleanup")
@rate_limit("5/minute")
async def cleanup_cache(
    request: Request,
    auth: bool = Depends(verify_auth),
):
    """Remove expired cache entries."""
    expired_count = cache.cleanup_expired()
    cache.flush()
    return {
        "status": "success",
        "expired_removed": expired_count,
        "remaining_items": cache.get_item_count(),
        "timestamp": datetime.datetime.utcnow().isoformat() + "Z",
    }


@app.post("/v1/cache/flush")
@rate_limit("5/minute")
async def flush_cache(
    request: Request,
    auth: bool = Depends(verify_auth),
):
    """Force immediate cache save to disk."""
    success = cache.flush()
    return {
        "status": "success" if success else "error",
        "message": "Cache flushed to disk" if success else "Failed to flush cache",
        "timestamp": datetime.datetime.utcnow().isoformat() + "Z",
    }


# =============================================================================
# Fundamentals Endpoint
# =============================================================================


@app.get("/v1/fundamentals", response_model=Dict[str, Any])
@rate_limit("30/minute")
async def get_fundamentals_v1(
    request: Request,
    symbol: str = Query(..., description="Ticker symbol for fundamentals lookup"),
    auth: bool = Depends(verify_auth),
):
    """Hybrid fundamentals endpoint (EODHD + FMP)."""
    try:
        result = await fundamentals_service.get_fundamentals(symbol)
        return result
    except Exception as e:
        logger.error(f"Error in fundamentals lookup for {symbol}: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")


# =============================================================================
# Analysis Feature Flag Helper
# =============================================================================


def ensure_analysis_enabled():
    """Central check for analysis config and availability."""
    if not settings.advanced_analysis_enabled:
        raise HTTPException(
            status_code=503,
            detail="Analysis engine disabled (ADVANCED_ANALYSIS_ENABLED=false).",
        )
    if analyzer is None:
        raise HTTPException(
            status_code=503,
            detail="Analysis engine is not available on this instance.",
        )


# =============================================================================
# Advanced Analysis Endpoints (Using advanced_analysis.py)
# =============================================================================


@app.get("/v1/analysis/multi-source", response_model=Dict[str, Any])
@rate_limit("30/minute")
async def analysis_multi_source(
    request: Request,
    symbol: str = Query(..., description="Single ticker symbol for multi-source analysis"),
    auth: bool = Depends(verify_auth),
):
    """Multi-source price analysis via advanced_analysis."""
    ensure_analysis_enabled()

    try:
        result = await asyncio.wait_for(
            get_multi_source_analysis(symbol),
            timeout=ANALYSIS_OVERALL_TIMEOUT_SECONDS,
        )
        return result
    except asyncio.TimeoutError:
        logger.warning(
            f"Analysis multi-source timeout after {ANALYSIS_OVERALL_TIMEOUT_SECONDS}s "
            f"for symbol={symbol}"
        )
        raise HTTPException(
            status_code=504,
            detail=(
                f"Analysis timeout after {ANALYSIS_OVERALL_TIMEOUT_SECONDS}s. "
                "Try again later."
            ),
        )
    except Exception as e:
        logger.error(f"Error in multi-source analysis for {symbol}: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")


@app.get("/v1/analysis/recommendation", response_model=Dict[str, Any])
@rate_limit("20/minute")
async def analysis_recommendation(
    request: Request,
    symbol: str = Query(..., description="Ticker symbol for AI trading recommendation"),
    auth: bool = Depends(verify_auth),
):
    """AI trading recommendation via advanced_analysis."""
    ensure_analysis_enabled()

    try:
        rec = await asyncio.wait_for(
            generate_ai_recommendation(symbol),
            timeout=ANALYSIS_OVERALL_TIMEOUT_SECONDS,
        )
        return rec.to_dict() if hasattr(rec, "to_dict") else rec
    except asyncio.TimeoutError:
        logger.warning(
            f"Analysis recommendation timeout after {ANALYSIS_OVERALL_TIMEOUT_SECONDS}s "
            f"for symbol={symbol}"
        )
        raise HTTPException(
            status_code=504,
            detail=(
                f"Analysis timeout after {ANALYSIS_OVERALL_TIMEOUT_SECONDS}s. "
                "Try again later."
            ),
        )
    except Exception as e:
        logger.error(f"Error generating AI recommendation for {symbol}: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")


@app.get("/v1/analysis/cache/info", response_model=Dict[str, Any])
@rate_limit("10/minute")
async def analysis_cache_info(
    request: Request,
    auth: bool = Depends(verify_auth),
):
    """Get cache information for the analysis engine."""
    ensure_analysis_enabled()

    try:
        info = await analyzer.get_cache_info()
        return info
    except Exception as e:
        logger.error(f"Error getting analysis cache info: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")


@app.post("/v1/analysis/cache/clear", response_model=Dict[str, Any])
@rate_limit("5/minute")
async def analysis_cache_clear(
    request: Request,
    symbol: Optional[str] = Query(
        None,
        description="Optional symbol to clear from analysis cache; if omitted, clears all.",
    ),
    auth: bool = Depends(verify_auth),
):
    """Clear analysis engine cache."""
    ensure_analysis_enabled()

    try:
        result = await analyzer.clear_cache(symbol)
        return result
    except Exception as e:
        logger.error(f"Error clearing analysis cache: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")


# =============================================================================
# DEBUG / SAFE TEST ENDPOINTS (NO EXTERNAL APIS)
# =============================================================================


@app.get("/debug/simple-quote")
async def debug_simple_quote(
    tickers: str = Query(..., description="Comma-separated tickers for debug only"),
):
    """
    Debug endpoint: returns fake prices quickly (no external calls).
    Useful to verify Sheets/JS integration without hitting real APIs.
    """
    symbols = [t.strip().upper() for t in tickers.split(",") if t.strip()]
    if not symbols:
        raise HTTPException(status_code=400, detail="No tickers provided")

    now_ts = datetime.datetime.utcnow().isoformat() + "Z"

    data = []
    for i, sym in enumerate(symbols, start=1):
        data.append(
            {
                "ticker": sym,
                "price": 100 + i,
                "currency": "SAR",
                "source": "debug_stub",
                "ts": now_ts,
            }
        )

    return {
        "status": "ok",
        "count": len(data),
        "timestamp": now_ts,
        "data": data,
    }


@app.get("/debug/saudi-market")
async def debug_saudi_market(limit: int = Query(5, ge=1, le=50)):
    """
    Simple stubbed Saudi market snapshot (no external APIs).
    """
    now_ts = datetime.datetime.utcnow().isoformat() + "Z"

    base_symbols = ["1120.SR", "2010.SR", "2020.SR", "7200.SR", "2222.SR"]
    names = ["Riyad Bank", "SABIC", "Maaden", "Arabian Internet", "Aramco"]

    sample = []
    for i in range(min(limit, len(base_symbols))):
        sample.append(
            {
                "ticker": base_symbols[i],
                "name": names[i],
                "last_price": 100 + i * 2,
                "change_pct": 0.5 * (i - 2),
                "volume": 1_000_000 + i * 100_000,
                "sector": "DEBUG",
                "ts": now_ts,
            }
        )

    return {
        "status": "ok",
        "count": len(sample),
        "timestamp": now_ts,
        "data": sample,
    }


# =============================================================================
# Official Saudi Market Endpoint (currently stubbed)
# =============================================================================


@app.get("/api/saudi/market")
@rate_limit("60/minute")
async def saudi_market_official(
    request: Request,
    limit: int = Query(5, ge=1, le=50),
    auth: bool = Depends(verify_auth),
):
    """
    Official Saudi market endpoint.

    CURRENT:
      - Uses debug_saudi_market stub (no external APIs).
    FUTURE:
      - Replace stub with Argaam/Tadawul provider integration.
    """
    try:
        data = await debug_saudi_market(limit=limit)
        return data
    except Exception as e:
        logger.error(f"Error in /api/saudi/market: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")


# =============================================================================
# Application Entry Point
# =============================================================================


def main():
    """Application entry point."""
    logger.info("=" * 70)
    logger.info(f"🚀 {settings.service_name} v{settings.service_version}")
    logger.info(f"🌍 Environment: {settings.environment} (Production: {settings.is_production})")
    logger.info(f"🔐 Authentication: {'Enabled' if settings.require_auth else 'Disabled'}")
    logger.info(f"⚡ Rate Limiting: {'Enabled' if settings.enable_rate_limiting else 'Disabled'}")
    logger.info(f"📊 Google Services: {'Available' if settings.has_google_sheets_access else 'Not Available'}")
    logger.info(f"💾 Cache: {cache.get_item_count()} items, Save Interval: {settings.cache_save_interval}s")
    logger.info(
        f"📈 Financial APIs: {len(provider_manager.enabled_providers)} enabled -> "
        f"{[p.name for p in provider_manager.enabled_providers]}"
    )
    logger.info(
        f"🤖 Analysis Engine: "
        f"{'Available' if analyzer is not None else 'Not Available'} | "
        f"Enabled (config): {settings.advanced_analysis_enabled}"
    )
    logger.info(f"🔧 Debug Mode: {settings.debug}")
    logger.info(
        f"🌐 Starting server on {settings.app_host}:{settings.app_port} "
        f"(reload=False, quote_timeout={QUOTE_OVERALL_TIMEOUT_SECONDS}s, "
        f"analysis_timeout={ANALYSIS_OVERALL_TIMEOUT_SECONDS}s)"
    )
    logger.info("=" * 70)

    uvicorn.run(
        "main:app",
        host=settings.app_host,
        port=settings.app_port,
        reload=False,  # important for Render to avoid multiple workers / reload loops
        log_level="info",
    )


if __name__ == "__main__":
    main()
