# symbols_reader.py
# Enhanced Google Sheets symbol reader - Production Ready for Render
# Version: 2.0.0 - Optimized for deployment

from __future__ import annotations

import os
import json
import logging
import time
import hashlib
from pathlib import Path
from typing import Dict, List, Optional, Any, Tuple
from datetime import datetime, timedelta
from dataclasses import dataclass
import asyncio

# Configure logger
logger = logging.getLogger(__name__)

@dataclass
class SymbolRecord:
    """Enhanced data class for symbol records with validation and serialization."""
    symbol: str
    company_name: str
    trading_sector: str
    financial_market: str
    include_in_ranking: bool = True
    market_cap: Optional[float] = None
    last_updated: Optional[str] = None
    data_source: str = "google_sheets"

    def __post_init__(self):
        """Validate data after initialization."""
        self.symbol = self.symbol.upper().strip()
        if not self.symbol:
            raise ValueError("Symbol cannot be empty")
        
        # Set default timestamp if not provided
        if not self.last_updated:
            self.last_updated = datetime.utcnow().isoformat() + "Z"

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for JSON serialization with validation."""
        return {
            "symbol": self.symbol,
            "company_name": self.company_name,
            "trading_sector": self.trading_sector,
            "financial_market": self.financial_market,
            "include_in_ranking": self.include_in_ranking,
            "market_cap": self.market_cap,
            "last_updated": self.last_updated,
            "data_source": self.data_source,
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> SymbolRecord:
        """Create SymbolRecord from dictionary with validation."""
        return cls(**data)


class SymbolsReader:
    """
    Production-ready Google Sheets symbol reader optimized for Render deployment.
    
    Features:
    - Shared configuration with main.py
    - Robust error handling and fallbacks
    - Memory-efficient caching
    - Connection pooling and request throttling
    - Render-compatible file system usage
    """

    # Class-level cache to share between instances
    _shared_cache: Dict[str, Any] = {}
    _last_cache_cleanup: float = 0.0

    def __init__(self) -> None:
        # Configuration with Render compatibility
        self.sheet_id: str = self._get_spreadsheet_id()
        self.tab_name: str = os.getenv("SHEETS_TAB_SYMBOLS", "Market_Leaders")
        
        # Header row configuration
        try:
            self.header_row: int = int(os.getenv("SHEETS_HEADER_ROW", "2"))
        except (ValueError, TypeError):
            self.header_row = 2
            logger.warning(f"Invalid SHEETS_HEADER_ROW, using default: {self.header_row}")

        # Cache configuration optimized for Render
        self.cache_dir = self._setup_cache_directory()
        self.cache_ttl = self._get_cache_ttl()
        self.max_cache_size_mb = 10  # Limit cache size on Render

        # Request management
        self.last_request_time = 0.0
        self.min_request_interval = 1.5  # More conservative throttling
        self.max_retries = 3
        self.retry_delay = 2.0

        # Google Sheets client state
        self._client = None
        self._worksheet = None
        self._client_initialized = False
        self._last_successful_fetch: Optional[float] = None

        # Column mapping with enhanced field detection
        self.column_mapping = self._initialize_column_mapping()

        logger.info(
            f"SymbolsReader initialized: "
            f"sheet_id={self.sheet_id[:8]}..., "
            f"tab_name={self.tab_name}, "
            f"cache_ttl={self.cache_ttl.total_seconds()}s, "
            f"cache_dir={self.cache_dir}"
        )

    def _get_spreadsheet_id(self) -> str:
        """Get spreadsheet ID with proper fallback hierarchy."""
        env_vars = [
            "SPREADSHEET_ID",           # Primary (same as main.py)
            "SHEETS_SPREADSHEET_ID",    # Secondary
            "GOOGLE_SHEETS_ID",         # Tertiary
        ]
        
        for env_var in env_vars:
            value = os.getenv(env_var)
            if value and value.strip():
                logger.info(f"Using spreadsheet ID from {env_var}")
                return value.strip()
        
        logger.warning("No spreadsheet ID configured - using fallback mode")
        return ""

    def _setup_cache_directory(self) -> Path:
        """Setup cache directory with Render compatibility."""
        cache_base = os.getenv("SYMBOLS_CACHE_DIR", "/tmp/symbols_cache")
        
        # On Render, prefer /tmp for ephemeral storage
        if os.path.exists("/tmp") and os.access("/tmp", os.W_OK):
            cache_dir = Path("/tmp/symbols_cache")
        else:
            cache_dir = Path(cache_base)
        
        try:
            cache_dir.mkdir(parents=True, exist_ok=True)
            logger.info(f"Cache directory: {cache_dir}")
            return cache_dir
        except Exception as e:
            logger.warning(f"Could not create cache directory {cache_dir}: {e}")
            # Fallback to current directory
            return Path("./symbols_cache")

    def _get_cache_ttl(self) -> timedelta:
        """Get cache TTL with sensible defaults."""
        try:
            ttl_seconds = int(os.getenv("SYMBOLS_CACHE_TTL", "1800"))  # 30 min default
            return timedelta(seconds=max(300, ttl_seconds))  # Minimum 5 minutes
        except (ValueError, TypeError):
            logger.warning("Invalid SYMBOLS_CACHE_TTL, using default 30 minutes")
            return timedelta(seconds=1800)

    def _initialize_column_mapping(self) -> Dict[str, List[str]]:
        """Initialize comprehensive column mapping."""
        return {
            "symbol": ["Symbol", "Ticker", "Ticker Symbol", "Code", "رمز", "الرمز"],
            "company_name": ["Company Name", "Company", "Name", "Security Name", "اسم الشركة", "الشركة"],
            "trading_sector": ["Sector", "Trading Sector", "Industry", "Industry Sector", "القطاع", "قطاع التداول"],
            "financial_market": ["Market", "Financial Market", "Exchange", "Trading Market", "السوق", "السوق المالية"],
            "include_in_ranking": ["Include", "Include in Ranking", "Active", "Enabled", "مشمول", "مشمول في التصنيف"],
            "market_cap": ["Market Cap", "Market Capitalization", "Capitalization", "القيمة السوقية", "رأس المال السوقي"],
        }

    # -------------------------------------------------------------------------
    # Enhanced Request Management
    # -------------------------------------------------------------------------
    def _throttle_requests(self) -> None:
        """Enhanced request throttling with jitter."""
        now = time.time()
        elapsed = now - self.last_request_time
        
        if elapsed < self.min_request_interval:
            sleep_time = self.min_request_interval - elapsed
            # Add jitter to avoid synchronized requests
            sleep_time += (time.time() % 0.1)  # 0-100ms jitter
            time.sleep(sleep_time)
        
        self.last_request_time = time.time()

    def _retry_operation(self, operation, operation_name: str = "operation"):
        """Enhanced retry logic with exponential backoff."""
        last_exception = None
        
        for attempt in range(self.max_retries):
            try:
                self._throttle_requests()
                return operation()
            except Exception as e:
                last_exception = e
                logger.warning(
                    f"Attempt {attempt + 1}/{self.max_retries} failed for {operation_name}: {e}"
                )
                
                if attempt < self.max_retries - 1:
                    wait_time = self.retry_delay * (2 ** attempt)  # Exponential backoff
                    logger.info(f"Retrying in {wait_time:.2f}s...")
                    time.sleep(wait_time)
        
        logger.error(f"All retries failed for {operation_name}: {last_exception}")
        raise last_exception

    # -------------------------------------------------------------------------
    # Enhanced Cache Management
    # -------------------------------------------------------------------------
    def _get_cache_key(self) -> str:
        """Generate secure cache key with configuration fingerprint."""
        config_string = f"{self.sheet_id}_{self.tab_name}_{self.header_row}"
        config_hash = hashlib.md5(config_string.encode()).hexdigest()[:12]
        return f"symbols_cache_{config_hash}.json"

    def _cleanup_old_cache_files(self) -> None:
        """Clean up old cache files to prevent disk space issues."""
        try:
            now = time.time()
            # Only cleanup once per hour
            if now - self._last_cache_cleanup < 3600:
                return

            cache_files = list(self.cache_dir.glob("symbols_cache_*.json"))
            for cache_file in cache_files:
                try:
                    # Delete files older than 24 hours
                    if now - cache_file.stat().st_mtime > 86400:
                        cache_file.unlink()
                        logger.debug(f"Cleaned up old cache file: {cache_file}")
                except Exception as e:
                    logger.debug(f"Could not delete cache file {cache_file}: {e}")

            self._last_cache_cleanup = now
        except Exception as e:
            logger.debug(f"Cache cleanup failed: {e}")

    def _get_cache_size_mb(self) -> float:
        """Calculate current cache directory size."""
        try:
            total_size = 0
            for file_path in self.cache_dir.glob("*"):
                if file_path.is_file():
                    total_size += file_path.stat().st_size
            return total_size / (1024 * 1024)
        except Exception:
            return 0.0

    def _load_from_cache(self) -> Optional[List[Dict[str, Any]]]:
        """Enhanced cache loading with size limits and validation."""
        try:
            cache_file = self.cache_dir / self._get_cache_key()
            
            if not cache_file.exists():
                return None

            # Check cache file size (avoid loading huge files)
            file_size = cache_file.stat().st_size
            if file_size > 10 * 1024 * 1024:  # 10MB limit
                logger.warning(f"Cache file too large ({file_size} bytes), ignoring")
                cache_file.unlink()
                return None

            # Load and validate cache
            raw_data = cache_file.read_text(encoding="utf-8")
            payload = json.loads(raw_data)

            # Validate cache structure
            if not isinstance(payload, dict) or "timestamp" not in payload or "data" not in payload:
                logger.warning("Invalid cache structure, ignoring")
                cache_file.unlink()
                return None

            # Check TTL
            cache_time = datetime.fromisoformat(payload["timestamp"].replace("Z", "+00:00"))
            if datetime.now().replace(tzinfo=cache_time.tzinfo) - cache_time > self.cache_ttl:
                logger.debug("Cache expired")
                return None

            data = payload.get("data", [])
            if not isinstance(data, list):
                logger.warning("Invalid cache data format")
                return None

            logger.info(f"Loaded {len(data)} symbols from cache")
            return data

        except json.JSONDecodeError as e:
            logger.warning(f"Corrupted cache file: {e}")
            try:
                cache_file.unlink()
            except Exception:
                pass
            return None
        except Exception as e:
            logger.debug(f"Cache load failed: {e}")
            return None

    def _save_to_cache(self, data: List[Dict[str, Any]]) -> bool:
        """Enhanced cache saving with size limits and atomic writes."""
        try:
            # Check cache size limits
            current_size = self._get_cache_size_mb()
            if current_size > self.max_cache_size_mb:
                logger.warning(f"Cache size ({current_size:.1f}MB) exceeds limit, cleaning up")
                self._cleanup_old_cache_files()

            cache_file = self.cache_dir / self._get_cache_key()
            temp_file = cache_file.with_suffix(".tmp")

            payload = {
                "timestamp": datetime.now().isoformat(),
                "sheet_id": self.sheet_id,
                "tab_name": self.tab_name,
                "data": data,
                "version": "2.0.0",
            }

            # Atomic write
            temp_file.write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8")
            temp_file.replace(cache_file)

            logger.info(f"Saved {len(data)} symbols to cache")
            return True

        except Exception as e:
            logger.warning(f"Cache save failed: {e}")
            return False

    # -------------------------------------------------------------------------
    # Enhanced Google Sheets Integration
    # -------------------------------------------------------------------------
    def _get_google_client(self):
        """Enhanced Google Sheets client initialization with better error handling."""
        if self._client_initialized and self._client is not None:
            return self._client

        try:
            import gspread
            from google.oauth2.service_account import Credentials
        except ImportError as e:
            logger.error(f"Google Sheets dependencies not available: {e}")
            self._client_initialized = True
            return None

        try:
            scopes = [
                "https://www.googleapis.com/auth/spreadsheets.readonly",
                "https://www.googleapis.com/auth/drive.readonly",
            ]

            creds_json = os.getenv("GOOGLE_SHEETS_CREDENTIALS")
            legacy_json = os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON")

            if creds_json:
                logger.info("Initializing Google client from GOOGLE_SHEETS_CREDENTIALS")
                creds_dict = json.loads(creds_json)
            elif legacy_json:
                logger.warning("Using legacy GOOGLE_SERVICE_ACCOUNT_JSON - please migrate to GOOGLE_SHEETS_CREDENTIALS")
                creds_dict = json.loads(legacy_json)
            else:
                # Try credential file
                cred_file = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
                if cred_file and os.path.exists(cred_file):
                    logger.info(f"Initializing Google client from file: {cred_file}")
                    with open(cred_file, 'r') as f:
                        creds_dict = json.load(f)
                else:
                    logger.error("No Google Sheets credentials available")
                    self._client_initialized = True
                    return None

            # Validate required credential fields
            required_fields = ["type", "project_id", "private_key_id", "private_key", "client_email"]
            missing_fields = [field for field in required_fields if field not in creds_dict]
            if missing_fields:
                logger.error(f"Missing required credential fields: {missing_fields}")
                self._client_initialized = True
                return None

            creds = Credentials.from_service_account_info(creds_dict, scopes=scopes)
            self._client = gspread.authorize(creds)
            self._client_initialized = True
            
            logger.info("Google Sheets client initialized successfully")
            return self._client

        except json.JSONDecodeError as e:
            logger.error(f"Invalid JSON in credentials: {e}")
            self._client_initialized = True
            return None
        except Exception as e:
            logger.error(f"Failed to initialize Google Sheets client: {e}")
            self._client_initialized = True
            return None

    def _get_worksheet(self):
        """Enhanced worksheet access with better error recovery."""
        if self._worksheet is not None:
            return self._worksheet

        if not self.sheet_id:
            logger.error("No spreadsheet ID configured")
            return None

        client = self._get_google_client()
        if client is None:
            return None

        try:
            from gspread.exceptions import WorksheetNotFound, SpreadsheetNotFound, APIError

            def open_spreadsheet():
                self._throttle_requests()
                return client.open_by_key(self.sheet_id)

            spreadsheet = self._retry_operation(open_spreadsheet, "open_spreadsheet")
            logger.info(f"Opened spreadsheet: {spreadsheet.title}")

            def get_worksheet():
                self._throttle_requests()
                return spreadsheet.worksheet(self.tab_name)

            try:
                self._worksheet = self._retry_operation(get_worksheet, f"get_worksheet_{self.tab_name}")
                logger.info(f"Using worksheet: {self.tab_name}")
            except WorksheetNotFound:
                logger.warning(f"Worksheet '{self.tab_name}' not found, trying first sheet")
                def get_first_worksheet():
                    self._throttle_requests()
                    worksheets = spreadsheet.worksheets()
                    return worksheets[0] if worksheets else None
                
                self._worksheet = self._retry_operation(get_first_worksheet, "get_first_worksheet")
                if self._worksheet:
                    logger.info(f"Using first worksheet: {self._worksheet.title}")
                else:
                    logger.error("No worksheets available")
                    return None

            return self._worksheet

        except SpreadsheetNotFound:
            logger.error(f"Spreadsheet not found: {self.sheet_id}")
            return None
        except APIError as e:
            logger.error(f"Google API error: {e}")
            return None
        except Exception as e:
            logger.error(f"Unexpected error accessing worksheet: {e}")
            return None

    # -------------------------------------------------------------------------
    # Enhanced Data Processing
    # -------------------------------------------------------------------------
    def _map_columns(self, headers: List[str]) -> Dict[str, int]:
        """Enhanced column mapping with fuzzy matching and confidence scoring."""
        column_map: Dict[str, int] = {}
        used_indices = set()

        for field, possible_names in self.column_mapping.items():
            best_match = None
            best_score = 0

            for idx, header in enumerate(headers):
                if idx in used_indices:
                    continue

                header_clean = str(header).strip().lower()
                
                for name in possible_names:
                    name_clean = name.lower()
                    
                    # Exact match
                    if header_clean == name_clean:
                        best_match = idx
                        best_score = 100
                        break
                    
                    # Contains match with score
                    if name_clean in header_clean:
                        score = len(name_clean) / len(header_clean) * 100
                        if score > best_score:
                            best_match = idx
                            best_score = score
                
                if best_score == 100:  # Exact match found
                    break

            if best_match is not None and best_score > 40:  # Minimum confidence
                column_map[field] = best_match
                used_indices.add(best_match)
                logger.debug(f"Mapped '{field}' -> column {best_match} (score: {best_score:.1f})")

        # Ensure we have at least symbol mapping
        if "symbol" not in column_map and headers:
            column_map["symbol"] = 0
            logger.warning("Symbol column not found, using first column")

        return column_map

    def _parse_row(self, row: List[str], column_map: Dict[str, int]) -> Optional[SymbolRecord]:
        """Enhanced row parsing with robust error handling."""
        try:
            # Extract symbol (required)
            sym_idx = column_map.get("symbol", 0)
            if sym_idx >= len(row):
                return None
                
            symbol = str(row[sym_idx]).strip()
            if not symbol or symbol.lower() in ['', 'n/a', 'null', 'undefined']:
                return None

            # Extract other fields with safe indexing
            company_name = self._safe_get(row, column_map.get("company_name", 1))
            trading_sector = self._safe_get(row, column_map.get("trading_sector", 2))
            financial_market = self._safe_get(row, column_map.get("financial_market", 3))
            
            # Parse boolean flag
            include_flag = True
            if "include_in_ranking" in column_map:
                inc_val = self._safe_get(row, column_map["include_in_ranking"])
                include_flag = self._parse_boolean(inc_val)

            # Parse market cap
            market_cap = None
            if "market_cap" in column_map:
                cap_val = self._safe_get(row, column_map["market_cap"])
                market_cap = self._parse_float(cap_val)

            return SymbolRecord(
                symbol=symbol,
                company_name=company_name,
                trading_sector=trading_sector,
                financial_market=financial_market,
                include_in_ranking=include_flag,
                market_cap=market_cap,
            )

        except Exception as e:
            logger.debug(f"Failed to parse row: {e}")
            return None

    def _safe_get(self, row: List[str], index: int, default: str = "") -> str:
        """Safely get value from row with bounds checking."""
        if 0 <= index < len(row):
            value = str(row[index]).strip()
            return value if value else default
        return default

    def _parse_boolean(self, value: str) -> bool:
        """Enhanced boolean parsing with Arabic support."""
        if not value:
            return True

        value_lower = value.lower().strip()
        
        true_values = {
            "true", "yes", "y", "1", "include", "enabled", "active", 
            "نعم", "مشمول", "مفعل", "يشمل", "تفعيل"
        }
        false_values = {
            "false", "no", "n", "0", "exclude", "disabled", "inactive",
            "لا", "غير مشمول", "غير مفعل", "لا يشمل", "إلغاء"
        }

        if value_lower in true_values:
            return True
        if value_lower in false_values:
            return False
        
        # Default to True for unknown values
        return True

    def _parse_float(self, value: str) -> Optional[float]:
        """Enhanced float parsing with currency and formatting support."""
        if not value:
            return None

        try:
            # Remove common formatting
            cleaned = (
                value.replace(",", "")
                .replace(" ", "")
                .replace("SAR", "")
                .replace("ر.س", "")
                .replace("$", "")
                .replace("USD", "")
                .replace("€", "")
                .replace("£", "")
                .strip()
            )
            
            # Handle Arabic numerals
            arabic_to_english = str.maketrans('٠١٢٣٤٥٦٧٨٩', '0123456789')
            cleaned = cleaned.translate(arabic_to_english)
            
            if cleaned:
                return float(cleaned)
        except (ValueError, TypeError):
            pass
            
        return None

    # -------------------------------------------------------------------------
    # Fallback Data
    # -------------------------------------------------------------------------
    def _get_hardcoded_symbols(self) -> List[Dict[str, Any]]:
        """Enhanced fallback symbols with realistic Saudi market data."""
        return [
            {
                "symbol": "2222.SR",
                "company_name": "Saudi Arabian Oil Company (Aramco)",
                "trading_sector": "Energy",
                "financial_market": "Tadawul (TASI)",
                "include_in_ranking": True,
                "market_cap": 2000000000000,
                "data_source": "hardcoded_fallback",
            },
            {
                "symbol": "1180.SR",
                "company_name": "Saudi National Bank",
                "trading_sector": "Banks",
                "financial_market": "Tadawul (TASI)",
                "include_in_ranking": True,
                "market_cap": 150000000000,
                "data_source": "hardcoded_fallback",
            },
            {
                "symbol": "1010.SR",
                "company_name": "Riyad Bank",
                "trading_sector": "Banks",
                "financial_market": "Tadawul (TASI)",
                "include_in_ranking": True,
                "market_cap": 45000000000,
                "data_source": "hardcoded_fallback",
            },
            {
                "symbol": "2010.SR",
                "company_name": "Saudi Basic Industries Corporation (SABIC)",
                "trading_sector": "Petrochemicals",
                "financial_market": "Tadawul (TASI)",
                "include_in_ranking": True,
                "market_cap": 300000000000,
                "data_source": "hardcoded_fallback",
            },
            {
                "symbol": "4030.SR",
                "company_name": "National Shipping Company of Saudi Arabia (Bahri)",
                "trading_sector": "Transportation",
                "financial_market": "Tadawul (TASI)",
                "include_in_ranking": True,
                "market_cap": 25000000000,
                "data_source": "hardcoded_fallback",
            },
        ]

    # -------------------------------------------------------------------------
    # Public API Methods
    # -------------------------------------------------------------------------
    def fetch_symbols(self, limit: Optional[int] = None, use_cache: bool = True) -> dict:
        """
        Enhanced symbol fetching with comprehensive error handling.
        
        Args:
            limit: Maximum number of symbols to return
            use_cache: Whether to use cached data
            
        Returns:
            Standardized response dictionary
        """
        start_time = time.time()
        
        try:
            # Try cache first
            if use_cache:
                cached_data = self._load_from_cache()
                if cached_data is not None:
                    if limit:
                        cached_data = cached_data[:limit]
                    
                    response = self._build_response(
                        cached_data,
                        source="cache",
                        processing_time=time.time() - start_time
                    )
                    logger.info(f"Cache hit: {len(cached_data)} symbols")
                    return response

            # Fetch from Google Sheets
            symbols_data = self._fetch_from_google_sheets(limit)
            if symbols_data:
                self._save_to_cache(symbols_data)
                self._last_successful_fetch = time.time()
                
                response = self._build_response(
                    symbols_data,
                    source="google_sheets",
                    processing_time=time.time() - start_time
                )
                logger.info(f"Google Sheets fetch successful: {len(symbols_data)} symbols")
                return response

            # Fallback to hardcoded symbols
            hardcoded_data = self._get_hardcoded_symbols()
            if limit:
                hardcoded_data = hardcoded_data[:limit]
                
            response = self._build_response(
                hardcoded_data,
                source="hardcoded_fallback",
                processing_time=time.time() - start_time,
                error="All data sources failed, using fallback symbols"
            )
            logger.warning(f"Using fallback symbols: {len(hardcoded_data)} symbols")
            return response

        except Exception as e:
            logger.error(f"Unexpected error in fetch_symbols: {e}")
            hardcoded_data = self._get_hardcoded_symbols()
            if limit:
                hardcoded_data = hardcoded_data[:limit]
                
            return self._build_response(
                hardcoded_data,
                source="hardcoded_fallback",
                processing_time=time.time() - start_time,
                error=f"Unexpected error: {str(e)}"
            )

    def _fetch_from_google_sheets(self, limit: Optional[int]) -> Optional[List[Dict[str, Any]]]:
        """Enhanced Google Sheets data fetching."""
        try:
            worksheet = self._get_worksheet()
            if not worksheet:
                return None

            def fetch_data():
                self._throttle_requests()
                return worksheet.get_all_values()

            all_values = self._retry_operation(fetch_data, "get_all_values")
            
            if not all_values or len(all_values) < self.header_row:
                logger.warning("Insufficient data in worksheet")
                return None

            headers = all_values[self.header_row - 1]
            data_rows = all_values[self.header_row:]

            column_map = self._map_columns(headers)
            symbols = []

            for row in data_rows:
                if not any(cell and str(cell).strip() for cell in row):
                    continue  # Skip empty rows

                symbol_record = self._parse_row(row, column_map)
                if symbol_record and symbol_record.include_in_ranking:
                    symbols.append(symbol_record.to_dict())

                if limit and len(symbols) >= limit:
                    break

            return symbols if symbols else None

        except Exception as e:
            logger.error(f"Google Sheets fetch failed: {e}")
            return None

    def _build_response(
        self,
        data: List[Dict[str, Any]],
        source: str,
        processing_time: float,
        error: Optional[str] = None
    ) -> dict:
        """Build standardized response with enhanced metadata."""
        response = {
            "ok": error is None,
            "source": source,
            "sheet_title": getattr(self._worksheet, "title", "Unknown") if self._worksheet else "Unknown",
            "worksheet": self.tab_name,
            "count": len(data),
            "data": data,
            "timestamp": datetime.utcnow().isoformat() + "Z",
            "processing_time_seconds": round(processing_time, 3),
            "metadata": {
                "sheet_id": self.sheet_id[:8] + "..." if self.sheet_id else "None",
                "header_row": self.header_row,
                "cache_used": source == "cache",
                "last_successful_fetch": self._last_successful_fetch,
            },
        }

        if error:
            response["error"] = error
            response["ok"] = False

        return response

    def get_status(self) -> dict:
        """Get comprehensive status of the symbols reader."""
        cache_info = self.get_cache_info()
        client_status = "initialized" if self._client_initialized else "uninitialized"
        worksheet_status = "available" if self._worksheet else "unavailable"
        
        return {
            "status": "operational",
            "client_status": client_status,
            "worksheet_status": worksheet_status,
            "spreadsheet_id_configured": bool(self.sheet_id),
            "cache_info": cache_info,
            "last_successful_fetch": self._last_successful_fetch,
            "timestamp": datetime.utcnow().isoformat() + "Z",
        }

    def clear_cache(self) -> dict:
        """Enhanced cache clearing."""
        try:
            cache_file = self.cache_dir / self._get_cache_key()
            if cache_file.exists():
                cache_file.unlink()
                logger.info(f"Symbols cache cleared: {cache_file}")
                return {
                    "status": "cleared", 
                    "cache_file": str(cache_file),
                    "timestamp": datetime.utcnow().isoformat() + "Z"
                }
            return {
                "status": "no_cache", 
                "cache_file": str(cache_file),
                "timestamp": datetime.utcnow().isoformat() + "Z"
            }
        except Exception as e:
            logger.error(f"Failed to clear cache: {e}")
            return {
                "status": "error", 
                "error": str(e),
                "timestamp": datetime.utcnow().isoformat() + "Z"
            }

    def get_cache_info(self) -> dict:
        """Enhanced cache information."""
        try:
            cache_file = self.cache_dir / self._get_cache_key()
            if not cache_file.exists():
                return {"exists": False}

            payload = json.loads(cache_file.read_text(encoding="utf-8"))
            ts = payload.get("timestamp", "2000-01-01T00:00:00")
            cache_time = datetime.fromisoformat(ts.replace("Z", "+00:00"))
            age = datetime.now().replace(tzinfo=cache_time.tzinfo) - cache_time
            
            return {
                "exists": True,
                "age_seconds": age.total_seconds(),
                "age_minutes": round(age.total_seconds() / 60, 1),
                "symbol_count": len(payload.get("data", [])),
                "last_updated": ts,
                "cache_file": str(cache_file),
                "file_size_bytes": cache_file.stat().st_size,
                "cache_version": payload.get("version", "1.0.0"),
            }
        except Exception as e:
            logger.debug(f"Failed to read cache info: {e}")
            return {"exists": False, "error": str(e)}


# Global instance with error handling
try:
    symbols_reader = SymbolsReader()
    logger.info("Global SymbolsReader instance created successfully")
except Exception as e:
    logger.error(f"Failed to create global SymbolsReader: {e}")
    symbols_reader = None


def fetch_symbols(limit: Optional[int] = None) -> dict:
    """
    Backward-compatible function for main.py integration.
    
    Args:
        limit: Maximum number of symbols to return
        
    Returns:
        Standardized symbols response
    """
    if symbols_reader is None:
        logger.error("SymbolsReader not available")
        return {
            "ok": False,
            "source": "error",
            "count": 0,
            "data": [],
            "timestamp": datetime.utcnow().isoformat() + "Z",
            "error": "SymbolsReader not initialized"
        }
    
    return symbols_reader.fetch_symbols(limit=limit)


# Test function for development
if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    
    reader = SymbolsReader()
    result = reader.fetch_symbols(limit=5)
    print(f"Status: {result['ok']}")
    print(f"Source: {result['source']}")
    print(f"Count: {result['count']}")
    print(f"Sample symbols: {result['data'][:2] if result['data'] else 'None'}")
