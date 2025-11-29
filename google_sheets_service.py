# google_sheets_service.py
# =============================================================================
# Google Sheets Service - Ultimate Investment Dashboard (10 Pages)
# Version: 2.0.1 - Enhanced for Production with Render Optimizations
# =============================================================================
from __future__ import annotations

import os
import json
import logging
import time
import hashlib
from datetime import datetime
from typing import Dict, List, Optional, Any
from pathlib import Path
import threading

# Third-party imports with enhanced error handling
try:
    import gspread
    from google.oauth2.service_account import Credentials
    from google.auth.exceptions import GoogleAuthError
    from gspread.exceptions import APIError, SpreadsheetNotFound, WorksheetNotFound
    DEPENDENCIES_AVAILABLE = True
except ImportError as e:
    logger = logging.getLogger(__name__)
    logger.error(f"Required dependencies not available: {e}")
    gspread = None
    Credentials = None
    # Provide fallbacks so later "except GoogleAuthError / APIError" won't crash
    GoogleAuthError = Exception
    APIError = Exception
    SpreadsheetNotFound = Exception
    WorksheetNotFound = Exception
    DEPENDENCIES_AVAILABLE = False

try:
    from pydantic import BaseModel
    PYDANTIC_AVAILABLE = True
except ImportError:
    BaseModel = object
    PYDANTIC_AVAILABLE = False

logger = logging.getLogger(__name__)

# =============================================================================
# Enhanced Data Models with Fallback
# =============================================================================

if PYDANTIC_AVAILABLE:
    class StockData(BaseModel):
        symbol: str
        company_name: str
        price: float
        change: float
        change_percent: float
        volume: int
        market_cap: Optional[float] = None
        sector: Optional[str] = None
        timestamp: datetime

    class FinancialData(BaseModel):
        symbol: str
        revenue: Optional[float] = None
        net_income: Optional[float] = None
        eps: Optional[float] = None
        pe_ratio: Optional[float] = None
        dividend_yield: Optional[float] = None
        timestamp: datetime

    class TechnicalIndicator(BaseModel):
        symbol: str
        rsi: Optional[float] = None
        macd: Optional[float] = None
        moving_avg_50: Optional[float] = None
        moving_avg_200: Optional[float] = None
        timestamp: datetime

    class SheetUpdate(BaseModel):
        sheet_name: str
        range: str
        values: List[List[Any]]
        timestamp: datetime
else:
    # Fallback classes if Pydantic is not available
    class StockData:
        def __init__(
            self,
            symbol: str,
            company_name: str,
            price: float,
            change: float,
            change_percent: float,
            volume: int,
            market_cap: Optional[float] = None,
            sector: Optional[str] = None,
            timestamp: datetime = None,
        ):
            self.symbol = symbol
            self.company_name = company_name
            self.price = price
            self.change = change
            self.change_percent = change_percent
            self.volume = volume
            self.market_cap = market_cap
            self.sector = sector
            self.timestamp = timestamp or datetime.now()

    class FinancialData:
        def __init__(
            self,
            symbol: str,
            revenue: Optional[float] = None,
            net_income: Optional[float] = None,
            eps: Optional[float] = None,
            pe_ratio: Optional[float] = None,
            dividend_yield: Optional[float] = None,
            timestamp: datetime = None,
        ):
            self.symbol = symbol
            self.revenue = revenue
            self.net_income = net_income
            self.eps = eps
            self.pe_ratio = pe_ratio
            self.dividend_yield = dividend_yield
            self.timestamp = timestamp or datetime.now()

    class TechnicalIndicator:
        def __init__(
            self,
            symbol: str,
            rsi: Optional[float] = None,
            macd: Optional[float] = None,
            moving_avg_50: Optional[float] = None,
            moving_avg_200: Optional[float] = None,
            timestamp: datetime = None,
        ):
            self.symbol = symbol
            self.rsi = rsi
            self.macd = macd
            self.moving_avg_50 = moving_avg_50
            self.moving_avg_200 = moving_avg_200
            self.timestamp = timestamp or datetime.now()

    class SheetUpdate:
        def __init__(
            self,
            sheet_name: str,
            range: str,
            values: List[List[Any]],
            timestamp: datetime = None,
        ):
            self.sheet_name = sheet_name
            self.range = range
            self.values = values
            self.timestamp = timestamp or datetime.now()


# =============================================================================
# Enhanced Google Sheets Service
# =============================================================================

class GoogleSheetsService:
    """
    Ultimate Investment Dashboard – Google Sheets integration.
    Production-ready with enhanced error handling, caching, and performance optimizations.

    ENVIRONMENT VARIABLES (Render Compatible):
      - GOOGLE_SHEETS_CREDENTIALS : Service account JSON (primary)
      - GOOGLE_SERVICE_ACCOUNT_JSON : Legacy fallback
      - SPREADSHEET_ID : Ultimate Investment Dashboard Sheet ID
      - SHEETS_CACHE_TTL : Cache TTL in seconds (default: 1800)
      - SHEETS_REQUEST_TIMEOUT : Request timeout (default: 30)

    MAIN TABS / PAGES:
      1) "KSA Tadawul Market"
      2) "Global Market Stock"
      3) "Mutual Fund"
      4) "My Portfolio Investment"
      5) "Commodities & FX"
      6) "Advanced Analysis & Advice"
      7) "Economic Calendar"
      8) "Investment Income Statement YTD"
      9) "Investment Advisor Assumptions"
    """

    SHEET_NAMES: Dict[str, str] = {
        "KSA_TADAWUL": "KSA Tadawul Market",
        "GLOBAL_MARKET": "Global Market Stock",
        "MUTUAL_FUND": "Mutual Fund",
        "MY_PORTFOLIO": "My Portfolio Investment",
        "COMMODITIES_FX": "Commodities & FX",
        "ADVANCED_ANALYSIS": "Advanced Analysis & Advice",
        "ECONOMIC_CALENDAR": "Economic Calendar",
        "INCOME_STATEMENT": "Investment Income Statement YTD",
        "INVESTMENT_ADVISOR": "Investment Advisor Assumptions",
    }

    def __init__(self, spreadsheet_id: Optional[str] = None) -> None:
        if not DEPENDENCIES_AVAILABLE:
            logger.error("Google Sheets dependencies not available - service will operate in fallback mode")

        self.client: Optional["gspread.Client"] = None
        self.spreadsheet: Optional["gspread.Spreadsheet"] = None
        self.worksheets: Dict[str, "gspread.Worksheet"] = {}
        self.initialized: bool = False
        self.initialization_attempted: bool = False

        # Enhanced configuration
        self.spreadsheet_id: Optional[str] = spreadsheet_id or self._get_spreadsheet_id()
        self.request_timeout: int = self._get_request_timeout()

        # Cache configuration
        self.cache_dir = self._setup_cache_directory()
        self.cache_ttl = self._get_cache_ttl()
        self._cache_lock = threading.RLock()
        self._last_cache_cleanup: float = 0.0

        # Request management
        self.last_request_time: float = 0.0
        self.min_request_interval: float = 1.0  # Rate limiting
        self._request_lock = threading.Lock()

        # Statistics
        self.request_count: int = 0
        self.error_count: int = 0
        self.success_count: int = 0

        logger.info(f"GoogleSheetsService initialized (spreadsheet_id: {self._mask_id(self.spreadsheet_id)})")

    # ------------------------------------------------------------------
    # Configuration helpers
    # ------------------------------------------------------------------
    def _get_spreadsheet_id(self) -> Optional[str]:
        """Get spreadsheet ID with multiple fallback options."""
        env_vars = ["SPREADSHEET_ID", "GOOGLE_SHEETS_ID", "SHEETS_SPREADSHEET_ID"]

        for env_var in env_vars:
            value = os.getenv(env_var)
            if value and value.strip():
                logger.info(f"Using spreadsheet ID from {env_var}")
                return value.strip()

        logger.warning("No spreadsheet ID configured - service will not initialize")
        return None

    def _get_request_timeout(self) -> int:
        """Get request timeout configuration."""
        try:
            timeout = int(os.getenv("SHEETS_REQUEST_TIMEOUT", "30"))
            return max(10, timeout)  # Minimum 10 seconds
        except (ValueError, TypeError):
            logger.warning("Invalid SHEETS_REQUEST_TIMEOUT, using default 30 seconds")
            return 30

    def _get_cache_ttl(self) -> int:
        """Get cache TTL configuration."""
        try:
            ttl = int(os.getenv("SHEETS_CACHE_TTL", "1800"))  # 30 minutes default
            return max(300, ttl)  # Minimum 5 minutes
        except (ValueError, TypeError):
            logger.warning("Invalid SHEETS_CACHE_TTL, using default 1800 seconds")
            return 1800

    def _setup_cache_directory(self) -> Path:
        """Setup cache directory with Render compatibility."""
        cache_base = os.getenv("SHEETS_CACHE_DIR", "/tmp/sheets_cache")

        # Prefer /tmp on Render for ephemeral storage
        if os.path.exists("/tmp") and os.access("/tmp", os.W_OK):
            cache_dir = Path("/tmp/sheets_cache")
        else:
            cache_dir = Path(cache_base)

        try:
            cache_dir.mkdir(parents=True, exist_ok=True)
            logger.info(f"Cache directory: {cache_dir}")
            return cache_dir
        except Exception as e:
            logger.warning(f"Could not create cache directory {cache_dir}: {e}")
            return Path("./sheets_cache")

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------
    def _mask_id(self, value: Optional[str]) -> str:
        """Mask sensitive IDs for logging."""
        if not value:
            return "None"
        return f"{value[:8]}..." if len(value) > 8 else "***"

    def _throttle_requests(self) -> None:
        """Implement request throttling to avoid rate limits."""
        with self._request_lock:
            now = time.time()
            elapsed = now - self.last_request_time

            if elapsed < self.min_request_interval:
                sleep_time = self.min_request_interval - elapsed
                time.sleep(sleep_time)

            self.last_request_time = time.time()

    def _get_cache_key(self, sheet_key: str) -> str:
        """Generate cache key for sheet data."""
        config_string = f"{self.spreadsheet_id}_{sheet_key}"
        config_hash = hashlib.md5(config_string.encode()).hexdigest()[:12]
        return f"sheets_cache_{config_hash}.json"

    def _load_from_cache(self, sheet_key: str) -> Optional[List[Dict[str, Any]]]:
        """Load data from cache with validation."""
        with self._cache_lock:
            try:
                cache_file = self.cache_dir / self._get_cache_key(sheet_key)

                if not cache_file.exists():
                    return None

                # Check file size (avoid loading huge files)
                if cache_file.stat().st_size > 5 * 1024 * 1024:  # 5MB limit
                    logger.warning(f"Cache file too large, ignoring: {cache_file}")
                    cache_file.unlink(missing_ok=True)
                    return None

                raw_data = cache_file.read_text(encoding="utf-8")
                payload = json.loads(raw_data)

                # Validate cache structure
                if not isinstance(payload, dict) or "timestamp" not in payload or "data" not in payload:
                    logger.warning("Invalid cache structure, ignoring")
                    cache_file.unlink(missing_ok=True)
                    return None

                # Check TTL
                cache_time = datetime.fromisoformat(payload["timestamp"].replace("Z", "+00:00"))
                if (datetime.now().replace(tzinfo=cache_time.tzinfo) - cache_time).total_seconds() > self.cache_ttl:
                    return None

                data = payload.get("data", [])
                if not isinstance(data, list):
                    return None

                logger.debug(f"Cache hit for {sheet_key}: {len(data)} records")
                return data

            except Exception as e:
                logger.debug(f"Cache load failed for {sheet_key}: {e}")
                return None

    def _save_to_cache(self, sheet_key: str, data: List[Dict[str, Any]]) -> bool:
        """Save data to cache with atomic write."""
        with self._cache_lock:
            try:
                self._cleanup_old_cache()

                cache_file = self.cache_dir / self._get_cache_key(sheet_key)
                temp_file = cache_file.with_suffix(".tmp")

                payload = {
                    "timestamp": datetime.now().isoformat(),
                    "sheet_key": sheet_key,
                    "spreadsheet_id": self.spreadsheet_id,
                    "data": data,
                    "version": "2.0.1",
                }

                # Atomic write
                temp_file.write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8")
                temp_file.replace(cache_file)

                logger.debug(f"Saved {len(data)} records to cache for {sheet_key}")
                return True

            except Exception as e:
                logger.debug(f"Cache save failed for {sheet_key}: {e}")
                return False

    def _cleanup_old_cache(self) -> None:
        """Clean up old cache files."""
        try:
            now = time.time()
            # Only cleanup once per hour
            if now - self._last_cache_cleanup < 3600:
                return

            cache_files = list(self.cache_dir.glob("sheets_cache_*.json"))
            deleted_count = 0

            for cache_file in cache_files:
                try:
                    # Delete files older than 24 hours
                    if now - cache_file.stat().st_mtime > 86400:
                        cache_file.unlink(missing_ok=True)
                        deleted_count += 1
                except Exception as e:
                    logger.debug(f"Could not delete cache file {cache_file}: {e}")

            self._last_cache_cleanup = now
            if deleted_count > 0:
                logger.info(f"Cleaned up {deleted_count} old cache files")

        except Exception as e:
            logger.debug(f"Cache cleanup failed: {e}")

    # ------------------------------------------------------------------
    # Enhanced Initialization
    # ------------------------------------------------------------------
    def initialize(self, credentials_json: Optional[str] = None, max_retries: int = 3) -> bool:
        """
        Enhanced initialization with retry logic and better error handling.
        """
        if self.initialized:
            return True

        if self.initialization_attempted:
            logger.warning("Initialization already attempted, skipping")
            return self.initialized

        if not DEPENDENCIES_AVAILABLE:
            logger.error("Cannot initialize - required dependencies not available")
            self.initialization_attempted = True
            return False

        if not self.spreadsheet_id:
            logger.error("Cannot initialize - no spreadsheet ID configured")
            self.initialization_attempted = True
            return False

        for attempt in range(max_retries):
            try:
                self._throttle_requests()

                if credentials_json is None:
                    # Try multiple environment variable options
                    credentials_json = os.getenv("GOOGLE_SHEETS_CREDENTIALS") or os.getenv(
                        "GOOGLE_SERVICE_ACCOUNT_JSON"
                    )

                if not credentials_json:
                    logger.error("No Google Sheets credentials available")
                    break

                # Parse credentials
                if isinstance(credentials_json, str):
                    credentials_dict = json.loads(credentials_json)
                else:
                    credentials_dict = credentials_json

                # Validate required fields
                required_fields = ["type", "project_id", "private_key_id", "private_key", "client_email"]
                missing_fields = [field for field in required_fields if field not in credentials_dict]
                if missing_fields:
                    logger.error(f"Missing required credential fields: {missing_fields}")
                    break

                scopes = [
                    "https://spreadsheets.google.com/feeds",
                    "https://www.googleapis.com/auth/drive",
                ]

                credentials = Credentials.from_service_account_info(credentials_dict, scopes=scopes)
                self.client = gspread.authorize(credentials)

                # Open spreadsheet
                self.spreadsheet = self.client.open_by_key(self.spreadsheet_id)
                self._cache_worksheets()

                self.initialized = True
                self.initialization_attempted = True
                self.success_count += 1

                logger.info(
                    f"✅ Google Sheets service initialized successfully (spreadsheet: {self.spreadsheet.title})"
                )
                return True

            except SpreadsheetNotFound:
                logger.error(f"❌ Spreadsheet not found: {self._mask_id(self.spreadsheet_id)}")
                break
            except GoogleAuthError as e:
                logger.error(f"❌ Google authentication failed: {e}")
                break
            except APIError as e:
                logger.error(f"❌ Google API error (attempt {attempt + 1}/{max_retries}): {e}")
                if attempt < max_retries - 1:
                    wait_time = 2 ** attempt  # Exponential backoff
                    logger.info(f"Retrying in {wait_time}s...")
                    time.sleep(wait_time)
            except Exception as e:
                logger.error(f"❌ Unexpected initialization error (attempt {attempt + 1}/{max_retries}): {e}")
                if attempt < max_retries - 1:
                    time.sleep(1)

        self.initialization_attempted = True
        self.error_count += 1
        return False

    def _cache_worksheets(self) -> None:
        """Enhanced worksheet caching with error handling."""
        if not self.spreadsheet:
            logger.error("❌ Cannot cache worksheets - spreadsheet not set")
            return

        try:
            self._throttle_requests()
            worksheets = self.spreadsheet.worksheets()
            self.worksheets = {ws.title: ws for ws in worksheets}
            logger.info(f"✅ Cached {len(self.worksheets)} worksheets: {list(self.worksheets.keys())}")
        except Exception as e:
            logger.error(f"❌ Failed to cache worksheets: {e}")

    # ------------------------------------------------------------------
    # Enhanced Worksheet Access
    # ------------------------------------------------------------------
    def get_worksheet(self, sheet_name: str, use_cache: bool = True) -> Optional["gspread.Worksheet"]:
        """Enhanced worksheet access with caching and error handling."""
        if not self.initialized:
            if not self.initialize():
                return None

        # Check cache first
        if use_cache and sheet_name in self.worksheets:
            return self.worksheets[sheet_name]

        try:
            if not self.spreadsheet:
                logger.error(f"❌ Spreadsheet not initialized when accessing '{sheet_name}'")
                return None

            self._throttle_requests()
            worksheet = self.spreadsheet.worksheet(sheet_name)
            self.worksheets[sheet_name] = worksheet
            return worksheet

        except WorksheetNotFound:
            logger.error(f"❌ Worksheet '{sheet_name}' not found in spreadsheet")
            return None
        except APIError as e:
            logger.error(f"❌ API error accessing worksheet '{sheet_name}': {e}")
            return None
        except Exception as e:
            logger.error(f"❌ Unexpected error accessing worksheet '{sheet_name}': {e}")
            return None

    def get_sheet_by_key(self, key: str, use_cache: bool = True) -> Optional["gspread.Worksheet"]:
        """Get worksheet using logical key from SHEET_NAMES."""
        sheet_name = self.SHEET_NAMES.get(key)
        if not sheet_name:
            logger.error(f"❌ Unknown sheet key: {key}")
            return None
        return self.get_worksheet(sheet_name, use_cache=use_cache)

    # ------------------------------------------------------------------
    # Enhanced Data Reading Methods with Caching
    # ------------------------------------------------------------------
    def read_ksa_tadawul_market(
        self, use_cache: bool = True, limit: Optional[int] = None
    ) -> List[Dict[str, Any]]:
        """Enhanced KSA Tadawul Market data reading with caching."""
        sheet_key = "KSA_TADAWUL"

        if use_cache:
            cached_data = self._load_from_cache(sheet_key)
            if cached_data is not None:
                return cached_data[:limit] if limit else cached_data

        try:
            ws = self.get_sheet_by_key(sheet_key)
            if not ws:
                return []

            self._throttle_requests()
            rows = ws.get_all_records()
            results: List[Dict[str, Any]] = []

            for row in rows:
                ticker = str(row.get("Ticker", "")).strip()
                if not ticker:
                    continue

                entry = {
                    "ticker": ticker,
                    "custom_tag": str(
                        row.get("Custom Tag")
                        or row.get("Custom Tag / Watchlist")
                        or row.get("Watchlist")
                        or ""
                    ).strip(),
                    "company_name": str(
                        row.get("Company Name") or row.get("Instrument Name") or ""
                    ).strip(),
                    "sector": str(row.get("Sector", "")).strip(),
                    "trading_market": str(
                        row.get("Trading Market") or row.get("Market") or ""
                    ).strip(),
                    "last_price": self._safe_float(row.get("Last Price")),
                    "day_high": self._safe_float(row.get("Day High")),
                    "day_low": self._safe_float(row.get("Day Low")),
                    "previous_close": self._safe_float(row.get("Previous Close")),
                    "change_value": self._safe_float(row.get("Change Value")),
                    "change_pct": self._safe_float(row.get("Change %")),
                    "volume": self._safe_int(row.get("Volume")),
                    "value_traded": self._safe_float(row.get("Value Traded")),
                    "market_cap": self._safe_float(row.get("Market Cap")),
                    "pe": self._safe_float(row.get("P/E")),
                    "pb": self._safe_float(row.get("P/B")),
                    "dividend_yield": self._safe_float(row.get("Dividend Yield")),
                    "eps": self._safe_float(row.get("EPS")),
                    "roe": self._safe_float(row.get("ROE")),
                    "trend_direction": str(row.get("Trend Direction", "")).strip(),
                    "support_level": self._safe_float(row.get("Support Level")),
                    "resistance_level": self._safe_float(row.get("Resistance Level")),
                    "momentum_score": self._safe_float(row.get("Momentum Score")),
                    "volatility_est": self._safe_float(row.get("Volatility Est")),
                    "expected_roi_1m": self._safe_float(row.get("Expected ROI 1M")),
                    "expected_roi_3m": self._safe_float(row.get("Expected ROI 3M")),
                    "expected_roi_12m": self._safe_float(row.get("Expected ROI 12M")),
                    "risk_level": str(row.get("Risk Level", "")).strip(),
                    "confidence_score": self._safe_float(row.get("Confidence Score")),
                    "data_quality": str(row.get("Data Quality", "")).strip(),
                    "composite_score": self._safe_float(row.get("Composite Score")),
                    "rank": self._safe_int(row.get("Rank")),
                    "recommendation": str(row.get("Recommendation", "")).strip(),
                    "timestamp": datetime.now().isoformat(),
                }
                results.append(entry)

                if limit and len(results) >= limit:
                    break

            logger.info(f"📈 Read {len(results)} KSA Tadawul rows")
            self.request_count += 1
            self.success_count += 1

            # Cache the results
            if use_cache and results:
                self._save_to_cache(sheet_key, results)

            return results

        except Exception as e:
            logger.error(f"❌ Failed to read KSA Tadawul Market: {e}")
            self.error_count += 1
            return []

    def read_global_market_stock(
        self, use_cache: bool = True, limit: Optional[int] = None
    ) -> List[Dict[str, Any]]:
        """Enhanced Global Market Stock data reading with caching."""
        sheet_key = "GLOBAL_MARKET"

        if use_cache:
            cached_data = self._load_from_cache(sheet_key)
            if cached_data is not None:
                return cached_data[:limit] if limit else cached_data

        try:
            ws = self.get_sheet_by_key(sheet_key)
            if not ws:
                return []

            self._throttle_requests()
            rows = ws.get_all_records()
            results: List[Dict[str, Any]] = []

            for row in rows:
                ticker = str(row.get("Ticker") or row.get("Symbol") or "").strip()
                if not ticker:
                    continue

                entry = {
                    "ticker": ticker,
                    "custom_tag": str(
                        row.get("Custom Tag") or row.get("Custom Tag / Watchlist") or ""
                    ).strip(),
                    "company_name": str(
                        row.get("Company Name") or row.get("Instrument Name") or ""
                    ).strip(),
                    "country": str(row.get("Country", "")).strip(),
                    "sector": str(row.get("Sector", "")).strip(),
                    "currency": str(row.get("Currency", "")).strip(),
                    "exchange": str(row.get("Exchange", "")).strip(),
                    "last_price": self._safe_float(row.get("Last Price")),
                    "day_high": self._safe_float(row.get("Day High")),
                    "day_low": self._safe_float(row.get("Day Low")),
                    "previous_close": self._safe_float(row.get("Previous Close")),
                    "change_value": self._safe_float(row.get("Change Value")),
                    "change_pct": self._safe_float(row.get("Change %")),
                    "volume": self._safe_int(row.get("Volume")),
                    "value_traded": self._safe_float(row.get("Value Traded")),
                    "market_cap": self._safe_float(row.get("Market Cap")),
                    "high_52w": self._safe_float(row.get("52W High")),
                    "low_52w": self._safe_float(row.get("52W Low")),
                    "premarket_price": self._safe_float(row.get("Pre-market Price")),
                    "after_hours_price": self._safe_float(row.get("After-hours Price")),
                    "pe": self._safe_float(row.get("P/E")),
                    "pb": self._safe_float(row.get("P/B")),
                    "dividend_yield": self._safe_float(row.get("Dividend Yield")),
                    "eps": self._safe_float(row.get("EPS")),
                    "roe": self._safe_float(row.get("ROE")),
                    "trend_direction": str(row.get("Trend Direction", "")).strip(),
                    "support_level": self._safe_float(row.get("Support Level")),
                    "resistance_level": self._safe_float(row.get("Resistance Level")),
                    "momentum_score": self._safe_float(row.get("Momentum Score")),
                    "volatility_est": self._safe_float(row.get("Volatility Est")),
                    "expected_roi_1m": self._safe_float(row.get("Expected ROI 1M")),
                    "expected_roi_3m": self._safe_float(row.get("Expected ROI 3M")),
                    "expected_roi_12m": self._safe_float(row.get("Expected ROI 12M")),
                    "risk_level": str(row.get("Risk Level", "")).strip(),
                    "confidence_score": self._safe_float(row.get("Confidence Score")),
                    "data_quality": str(row.get("Data Quality", "")).strip(),
                    "composite_score": self._safe_float(row.get("Composite Score")),
                    "rank": self._safe_int(row.get("Rank")),
                    "recommendation": str(row.get("Recommendation", "")).strip(),
                    "timestamp": datetime.now().isoformat(),
                }
                results.append(entry)

                if limit and len(results) >= limit:
                    break

            logger.info(f"🌍 Read {len(results)} Global Market rows")
            self.request_count += 1
            self.success_count += 1

            if use_cache and results:
                self._save_to_cache(sheet_key, results)

            return results

        except Exception as e:
            logger.error(f"❌ Failed to read Global Market Stock: {e}")
            self.error_count += 1
            return []

    def read_mutual_funds(
        self, use_cache: bool = True, limit: Optional[int] = None
    ) -> List[Dict[str, Any]]:
        """Enhanced Mutual Funds data reading with caching."""
        sheet_key = "MUTUAL_FUND"

        if use_cache:
            cached_data = self._load_from_cache(sheet_key)
            if cached_data is not None:
                return cached_data[:limit] if limit else cached_data

        try:
            ws = self.get_sheet_by_key(sheet_key)
            if not ws:
                return []

            self._throttle_requests()
            rows = ws.get_all_records()
            results: List[Dict[str, Any]] = []

            for row in rows:
                name = str(row.get("Fund Name") or row.get("Name") or "").strip()
                if not name:
                    continue

                entry = {
                    "fund_name": name,
                    "fund_code": str(row.get("Fund Code", "")).strip(),
                    "category": str(row.get("Category", "")).strip(),
                    "nav": self._safe_float(row.get("NAV")),
                    "nav_change_pct": self._safe_float(row.get("NAV Change %")),
                    "ytd_return_pct": self._safe_float(row.get("YTD Return %")),
                    "one_year_return_pct": self._safe_float(row.get("1Y Return %")),
                    "three_year_return_pct": self._safe_float(row.get("3Y Return %")),
                    "sharpe_ratio": self._safe_float(row.get("Sharpe Ratio")),
                    "expense_ratio": self._safe_float(row.get("Expense Ratio")),
                    "fund_size": self._safe_float(row.get("Fund Size")),
                    "risk_level": str(row.get("Risk Level", "")).strip(),
                    "manager_comments": str(row.get("Manager Comments", "")).strip(),
                    "recommendation": str(row.get("Recommendation", "")).strip(),
                    "timestamp": datetime.now().isoformat(),
                }
                results.append(entry)

                if limit and len(results) >= limit:
                    break

            logger.info(f"💼 Read {len(results)} mutual funds")
            self.request_count += 1
            self.success_count += 1

            if use_cache and results:
                self._save_to_cache(sheet_key, results)

            return results

        except Exception as e:
            logger.error(f"❌ Failed to read Mutual Fund sheet: {e}")
            self.error_count += 1
            return []

    def read_my_portfolio_investment(
        self, use_cache: bool = True, limit: Optional[int] = None
    ) -> List[Dict[str, Any]]:
        """Enhanced Portfolio data reading with caching."""
        sheet_key = "MY_PORTFOLIO"

        if use_cache:
            cached_data = self._load_from_cache(sheet_key)
            if cached_data is not None:
                return cached_data[:limit] if limit else cached_data

        try:
            ws = self.get_sheet_by_key(sheet_key)
            if not ws:
                return []

            self._throttle_requests()
            rows = ws.get_all_records()
            results: List[Dict[str, Any]] = []

            for row in rows:
                ticker = str(row.get("Ticker") or row.get("Symbol") or "").strip()
                if not ticker:
                    continue

                entry = {
                    "ticker": ticker,
                    "quantity": self._safe_float(
                        row.get("Quantity") or row.get("Total Quantity (Original)")
                    ),
                    "buy_price": self._safe_float(
                        row.get("Buy Price") or row.get("Average Cost per Unit")
                    ),
                    "today_price": self._safe_float(
                        row.get("Today's Price") or row.get("Current Price")
                    ),
                    "cost_value": self._safe_float(
                        row.get("Cost Value") or row.get("Total Invested Capital")
                    ),
                    "market_value": self._safe_float(
                        row.get("Market Value") or row.get("Current Market Value")
                    ),
                    "unrealized_pl": self._safe_float(
                        row.get("Unrealized P/L") or row.get("Unrealized Profit / Loss (Current)")
                    ),
                    "realized_pl": self._safe_float(
                        row.get("Realized P/L") or row.get("Realized Profit / Loss (All Time)")
                    ),
                    "total_return_pct": self._safe_float(
                        row.get("Total Return %") or row.get("ROI Since Inception (%)")
                    ),
                    "weight_pct": self._safe_float(
                        row.get("Weight % of Portfolio")
                    ),
                    "risk_level": str(row.get("Risk Level", "")).strip(),
                    "confidence_score": self._safe_float(row.get("Confidence Score")),
                    "target_price": self._safe_float(row.get("Target Price")),
                    "investment_horizon_days": self._safe_int(
                        row.get("Investment Horizon (Days)") or row.get("Holding Period (Days)")
                    ),
                    "timestamp": datetime.now().isoformat(),
                }
                results.append(entry)

                if limit and len(results) >= limit:
                    break

            logger.info(f"📊 Read {len(results)} portfolio positions")
            self.request_count += 1
            self.success_count += 1

            if use_cache and results:
                self._save_to_cache(sheet_key, results)

            return results

        except Exception as e:
            logger.error(f"❌ Failed to read My Portfolio Investment: {e}")
            self.error_count += 1
            return []

    def read_commodities_fx(
        self, use_cache: bool = True, limit: Optional[int] = None
    ) -> List[Dict[str, Any]]:
        """Enhanced Commodities & FX data reading with caching."""
        sheet_key = "COMMODITIES_FX"

        if use_cache:
            cached_data = self._load_from_cache(sheet_key)
            if cached_data is not None:
                return cached_data[:limit] if limit else cached_data

        try:
            ws = self.get_sheet_by_key(sheet_key)
            if not ws:
                return []

            self._throttle_requests()
            rows = ws.get_all_records()
            results: List[Dict[str, Any]] = []

            for row in rows:
                asset = str(row.get("Asset", "")).strip()
                if not asset:
                    continue

                entry = {
                    "asset": asset,
                    "category": str(row.get("Category", "")).strip(),
                    "last_price": self._safe_float(row.get("Last Price")),
                    "day_change_pct": self._safe_float(row.get("Day Change %")),
                    "volatility_30d": self._safe_float(row.get("30D Volatility")),
                    "trend": str(row.get("Trend", "")).strip(),
                    "support_level": self._safe_float(row.get("Support Level")),
                    "resistance_level": self._safe_float(row.get("Resistance Level")),
                    "forecast_roi_1m": self._safe_float(row.get("Forecast ROI 1M")),
                    "forecast_roi_3m": self._safe_float(row.get("Forecast ROI 3M")),
                    "economic_sensitivity": str(row.get("Economic Sensitivity", "")).strip(),
                    "recommendation": str(row.get("Recommendation", "")).strip(),
                    "timestamp": datetime.now().isoformat(),
                }
                results.append(entry)

                if limit and len(results) >= limit:
                    break

            logger.info(f"💱 Read {len(results)} Commodities/FX rows")
            self.request_count += 1
            self.success_count += 1

            if use_cache and results:
                self._save_to_cache(sheet_key, results)

            return results

        except Exception as e:
            logger.error(f"❌ Failed to read Commodities & FX: {e}")
            self.error_count += 1
            return []

    def read_economic_calendar(
        self, use_cache: bool = True, limit: Optional[int] = None
    ) -> List[Dict[str, Any]]:
        """Enhanced Economic Calendar data reading with caching."""
        sheet_key = "ECONOMIC_CALENDAR"

        if use_cache:
            cached_data = self._load_from_cache(sheet_key)
            if cached_data is not None:
                return cached_data[:limit] if limit else cached_data

        try:
            ws = self.get_sheet_by_key(sheet_key)
            if not ws:
                return []

            self._throttle_requests()
            rows = ws.get_all_records()
            results: List[Dict[str, Any]] = []

            for row in rows:
                date_raw = str(row.get("Date", "")).strip()
                if not date_raw:
                    continue

                entry = {
                    "date": date_raw,
                    "country": str(row.get("Country", "")).strip(),
                    "event": str(row.get("Event", "")).strip(),
                    "actual": str(row.get("Actual", "")).strip(),
                    "forecast": str(row.get("Forecast", "")).strip(),
                    "previous": str(row.get("Previous", "")).strip(),
                    "impact_level": str(row.get("Impact Level", "")).strip(),
                    "market_relevance": str(row.get("Market Relevance", "")).strip(),
                    "timestamp": datetime.now().isoformat(),
                }
                results.append(entry)

                if limit and len(results) >= limit:
                    break

            logger.info(f"📅 Read {len(results)} Economic Calendar events")
            self.request_count += 1
            self.success_count += 1

            if use_cache and results:
                self._save_to_cache(sheet_key, results)

            return results

        except Exception as e:
            logger.error(f"❌ Failed to read Economic Calendar: {e}")
            self.error_count += 1
            return []

    def read_investment_income_statement_ytd(
        self, use_cache: bool = True, limit: Optional[int] = None
    ) -> List[Dict[str, Any]]:
        """Enhanced Income Statement data reading with caching."""
        sheet_key = "INCOME_STATEMENT"

        if use_cache:
            cached_data = self._load_from_cache(sheet_key)
            if cached_data is not None:
                return cached_data[:limit] if limit else cached_data

        try:
            ws = self.get_sheet_by_key(sheet_key)
            if not ws:
                return []

            self._throttle_requests()
            rows = ws.get_all_records()
            results: List[Dict[str, Any]] = []

            for row in rows:
                period = str(row.get("Period", "")).strip()
                if not period:
                    continue

                entry = {
                    "period": period,
                    "begin_value": self._safe_float(row.get("Beginning Portfolio Value")),
                    "contributions": self._safe_float(row.get("Contributions")),
                    "withdrawals": self._safe_float(row.get("Withdrawals")),
                    "realized_gains": self._safe_float(row.get("Realized Gains")),
                    "unrealized_gains": self._safe_float(row.get("Unrealized Gains")),
                    "dividends_coupons": self._safe_float(row.get("Dividends / Coupons")),
                    "total_return": self._safe_float(row.get("Total Return")),
                    "roi_pct": self._safe_float(row.get("ROI %")),
                    "ytd_vs_last_year": self._safe_float(row.get("YTD vs Last Year %")),
                    "timestamp": datetime.now().isoformat(),
                }
                results.append(entry)

                if limit and len(results) >= limit:
                    break

            logger.info(f"📘 Read {len(results)} Investment Income Statement rows")
            self.request_count += 1
            self.success_count += 1

            if use_cache and results:
                self._save_to_cache(sheet_key, results)

            return results

        except Exception as e:
            logger.error(f"❌ Failed to read Investment Income Statement YTD: {e}")
            self.error_count += 1
            return []

    def read_investment_advisor_assumptions(
        self, use_cache: bool = True, limit: Optional[int] = None
    ) -> List[Dict[str, Any]]:
        """Enhanced Investment Advisor data reading with caching."""
        sheet_key = "INVESTMENT_ADVISOR"

        if use_cache:
            cached_data = self._load_from_cache(sheet_key)
            if cached_data is not None:
                return cached_data[:limit] if limit else cached_data

        try:
            ws = self.get_sheet_by_key(sheet_key)
            if not ws:
                return []

            self._throttle_requests()
            rows = ws.get_all_records()
            results: List[Dict[str, Any]] = []

            for row in rows:
                # Use 'Investment Amount' as a marker for input rows
                if row.get("Investment Amount") in (None, "", 0):
                    continue

                period_days = self._safe_int(row.get("Investment Period (Days)", 0))

                entry = {
                    "investment_amount": self._safe_float(row.get("Investment Amount")),
                    "target_roi_pct": self._safe_float(row.get("Target ROI")),
                    "max_risk_level": str(row.get("Max Risk Level", "")).strip(),
                    "investment_period_days": period_days,
                    "preferred_sectors": str(row.get("Preferred Sectors", "")).strip(),
                    "excluded_sectors": str(row.get("Excluded Sectors", "")).strip(),
                    "market_preference": str(row.get("Market Preference", "")).strip(),
                    "timestamp": datetime.now().isoformat(),
                }
                results.append(entry)

                if limit and len(results) >= limit:
                    break

            logger.info(f"🎯 Read {len(results)} Investment Advisor scenarios")
            self.request_count += 1
            self.success_count += 1

            if use_cache and results:
                self._save_to_cache(sheet_key, results)

            return results

        except Exception as e:
            logger.error(f"❌ Failed to read Investment Advisor Assumptions: {e}")
            self.error_count += 1
            return []

    def read_advanced_analysis_advice(
        self, use_cache: bool = True, limit: Optional[int] = None
    ) -> List[Dict[str, Any]]:
        """Enhanced Advanced Analysis data reading with caching."""
        sheet_key = "ADVANCED_ANALYSIS"

        if use_cache:
            cached_data = self._load_from_cache(sheet_key)
            if cached_data is not None:
                return cached_data[:limit] if limit else cached_data

        try:
            ws = self.get_sheet_by_key(sheet_key)
            if not ws:
                return []

            self._throttle_requests()
            rows = ws.get_all_records()
            logger.info(f"🔍 Read {len(rows)} Advanced Analysis rows")
            self.request_count += 1
            self.success_count += 1

            # Add timestamp to each row
            now_iso = datetime.now().isoformat()
            for row in rows:
                row["timestamp"] = now_iso

            if limit:
                rows = rows[:limit]

            if use_cache and rows:
                self._save_to_cache(sheet_key, rows)

            return rows

        except Exception as e:
            logger.error(f"❌ Failed to read Advanced Analysis & Advice: {e}")
            self.error_count += 1
            return []

    # ------------------------------------------------------------------
    # Enhanced Utility Methods
    # ------------------------------------------------------------------
    def _safe_float(self, value) -> Optional[float]:
        """Enhanced safe float conversion with Arabic numeral support."""
        if value is None or value == "":
            return None
        try:
            if isinstance(value, str):
                # Remove common formatting and Arabic numerals
                value = value.replace(",", "").replace('"', "").strip()
                # Convert Arabic numerals to Western
                arabic_to_english = str.maketrans("٠١٢٣٤٥٦٧٨٩", "0123456789")
                value = value.translate(arabic_to_english)
            return float(value)
        except (ValueError, TypeError):
            return None

    def _safe_int(self, value) -> int:
        """Enhanced safe integer conversion."""
        if value is None or value == "":
            return 0
        try:
            if isinstance(value, str):
                value = value.replace(",", "").replace('"', "").strip()
                arabic_to_english = str.maketrans("٠١٢٣٤٥٦٧٨٩", "0123456789")
                value = value.translate(arabic_to_english)
            return int(float(value))
        except (ValueError, TypeError):
            return 0

    # ------------------------------------------------------------------
    # Enhanced Management Methods
    # ------------------------------------------------------------------
    def get_sheet_info(self) -> Dict[str, Any]:
        """Enhanced spreadsheet metadata with health status."""
        try:
            if not self.initialized:
                if not self.initialize():
                    return {"error": "Service not initialized"}

            if not self.spreadsheet:
                return {"error": "Spreadsheet not available"}

            info: Dict[str, Any] = {
                "spreadsheet_title": self.spreadsheet.title,
                "spreadsheet_id": self._mask_id(self.spreadsheet.id),
                "sheets": [],
                "last_updated": datetime.now().isoformat(),
                "service_stats": self.get_statistics(),
                "health_status": "healthy" if self.initialized else "unhealthy",
            }

            for name, ws in self.worksheets.items():
                info["sheets"].append(
                    {
                        "name": name,
                        "row_count": ws.row_count,
                        "col_count": ws.col_count,
                    }
                )

            return info

        except Exception as e:
            logger.error(f"❌ Failed to get sheet info: {e}")
            return {"error": str(e)}

    def get_statistics(self) -> Dict[str, Any]:
        """Get service usage statistics."""
        total_requests = self.request_count
        success_rate = (self.success_count / total_requests * 100) if total_requests > 0 else 0

        return {
            "total_requests": total_requests,
            "successful_requests": self.success_count,
            "failed_requests": self.error_count,
            "success_rate_percent": round(success_rate, 2),
            "initialized": self.initialized,
            "worksheets_cached": len(self.worksheets),
            "cache_ttl_seconds": self.cache_ttl,
            "request_timeout": self.request_timeout,
        }

    def clear_cache(self, sheet_key: Optional[str] = None) -> Dict[str, Any]:
        """Clear cache for specific sheet or all sheets."""
        try:
            if sheet_key:
                cache_file = self.cache_dir / self._get_cache_key(sheet_key)
                if cache_file.exists():
                    cache_file.unlink(missing_ok=True)
                    return {"status": "cleared", "sheet_key": sheet_key}
                return {"status": "not_found", "sheet_key": sheet_key}
            else:
                # Clear all cache files
                cache_files = list(self.cache_dir.glob("sheets_cache_*.json"))
                deleted_count = 0
                for cache_file in cache_files:
                    cache_file.unlink(missing_ok=True)
                    deleted_count += 1
                return {"status": "cleared", "deleted_count": deleted_count}
        except Exception as e:
            logger.error(f"❌ Failed to clear cache: {e}")
            return {"status": "error", "error": str(e)}

    def health_check(self) -> Dict[str, Any]:
        """Comprehensive health check."""
        health_status: Dict[str, Any] = {
            "service_initialized": self.initialized,
            "dependencies_available": DEPENDENCIES_AVAILABLE,
            "spreadsheet_id_configured": bool(self.spreadsheet_id),
            "worksheets_accessible": False,
            "last_checked": datetime.now().isoformat(),
        }

        if self.initialized and self.spreadsheet:
            try:
                # Test access to a worksheet
                test_ws = self.get_sheet_by_key("KSA_TADAWUL")
                health_status["worksheets_accessible"] = test_ws is not None
                health_status["spreadsheet_title"] = self.spreadsheet.title
            except Exception as e:
                health_status["worksheets_accessible"] = False
                health_status["error"] = str(e)

        health_status["statistics"] = self.get_statistics()
        return health_status

    def create_backup_sheet(self) -> bool:
        """Enhanced backup creation with error handling."""
        try:
            if not self.initialized:
                if not self.initialize():
                    return False

            if not self.spreadsheet:
                return False

            ts = datetime.now().strftime("%Y%m%d_%H%M%S")
            backup_name = f"Backup_{ts}"

            source = self.spreadsheet.sheet1
            self._throttle_requests()
            self.spreadsheet.duplicate_sheet(source.id, new_sheet_name=backup_name)

            logger.info(f"✅ Created backup sheet: {backup_name}")
            return True

        except Exception as e:
            logger.error(f"❌ Failed to create backup sheet: {e}")
            return False

    def close(self) -> None:
        """Cleanup resources."""
        try:
            if getattr(self, "client", None):
                # gspread client doesn't have explicit close, but we can clear caches
                self.worksheets.clear()
                logger.info("Google Sheets service resources cleaned up")
        except Exception as e:
            logger.debug(f"Error during service close: {e}")


# =============================================================================
# Enhanced Singleton Instance
# =============================================================================

try:
    google_sheets_service = GoogleSheetsService()
    logger.info("Global GoogleSheetsService instance created successfully")
except Exception as e:
    logger.error(f"Failed to create global GoogleSheetsService: {e}")
    google_sheets_service = None


# Convenience functions for backward compatibility
def get_ksa_tadawul_data(use_cache: bool = True, limit: Optional[int] = None) -> List[Dict[str, Any]]:
    """Convenience function for KSA Tadawul data."""
    if google_sheets_service is None:
        logger.error("GoogleSheetsService not available")
        return []
    return google_sheets_service.read_ksa_tadawul_market(use_cache=use_cache, limit=limit)


def get_global_market_data(use_cache: bool = True, limit: Optional[int] = None) -> List[Dict[str, Any]]:
    """Convenience function for Global Market data."""
    if google_sheets_service is None:
        logger.error("GoogleSheetsService not available")
        return []
    return google_sheets_service.read_global_market_stock(use_cache=use_cache, limit=limit)


def health_check_global() -> Dict[str, Any]:
    """Convenience function for health checks."""
    if google_sheets_service is None:
        return {
            "service_initialized": False,
            "error": "GoogleSheetsService not initialized",
        }
    return google_sheets_service.health_check()


if __name__ == "__main__":
    # Enhanced test functionality
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    service = GoogleSheetsService()

    try:
        # Test health check BEFORE init (should show not initialized)
        print("=== Health Check (pre-init) ===")
        print(service.health_check())

        # Test initialization
        print("\n=== Initialization ===")
        if service.initialize():
            print("✅ Service initialized successfully")

            # Test sheet info
            print("\n=== Sheet Info ===")
            info = service.get_sheet_info()
            print(f"Spreadsheet: {info.get('spreadsheet_title', 'Unknown')}")
            print(f"Sheets: {[s['name'] for s in info.get('sheets', [])]}")

            # Test data reading
            print("\n=== Test Data Reading (KSA Tadawul, 2 rows) ===")
            ksa_data = service.read_ksa_tadawul_market(limit=2)
            print(f"Rows fetched: {len(ksa_data)}")
            if ksa_data:
                print("Sample row:", ksa_data[0])

            # Test statistics
            print("\n=== Statistics ===")
            stats = service.get_statistics()
            for key, value in stats.items():
                print(f"{key}: {value}")

        else:
            print("❌ Service initialization failed")

    except Exception as e:
        print(f"❌ Test failed: {e}")
    finally:
        service.close()
        print("\n=== Service Closed ===")
