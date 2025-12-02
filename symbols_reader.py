# symbols_reader.py
# Enhanced Google Sheets symbol reader - Production Ready for Render
# Version: 3.2.0 - Aligned with main.py / Render architecture and caching conventions

from __future__ import annotations

import os
import json
import logging
import time
import hashlib
from pathlib import Path
from typing import Dict, List, Optional, Any
from datetime import datetime, timedelta
from dataclasses import dataclass

# Configure logger
logger = logging.getLogger(__name__)


@dataclass
class SymbolRecord:
    """
    Data class for symbol records with validation and serialization.
    Represents a single row from the Market_Leaders (or similar) sheet.
    """
    symbol: str
    company_name: str
    trading_sector: str
    financial_market: str
    include_in_ranking: bool = True
    market_cap: Optional[float] = None
    last_updated: Optional[str] = None
    data_source: str = "google_sheets"

    def __post_init__(self) -> None:
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
    def from_dict(cls, data: Dict[str, Any]) -> "SymbolRecord":
        """Create SymbolRecord from dictionary with validation."""
        return cls(**data)


class SymbolsReader:
    """
    Production-ready Google Sheets symbol reader optimized for Render deployment.

    Features:
    - Shared configuration with main.py (SPREADSHEET_ID, GOOGLE_SHEETS_CREDENTIALS)
    - Robust error handling and fallbacks
    - Memory-efficient caching to /tmp on Render
    - Request throttling / retry logic
    - Hardcoded Saudi symbols fallback for resilience
    """

    # Class-level fields (reserved for future in-memory sharing if needed)
    _shared_cache: Dict[str, Any] = {}
    _last_cache_cleanup: float = 0.0

    def __init__(self) -> None:
        # Core configuration
        self.sheet_id: str = self._get_spreadsheet_id()
        # Default tab name where you maintain the leaders list
        self.tab_name: str = os.getenv("SHEETS_TAB_SYMBOLS", "Market_Leaders")

        # Header row (1-based index) where column titles are
        try:
            self.header_row: int = int(os.getenv("SHEETS_HEADER_ROW", "2"))
        except (ValueError, TypeError):
            self.header_row = 2
            logger.warning("Invalid SHEETS_HEADER_ROW, using default: %s", self.header_row)

        # Cache configuration optimized for Render
        self.cache_dir = self._setup_cache_directory()
        self.cache_ttl = self._get_cache_ttl()
        self.max_cache_size_mb = 10  # Limit cache size on Render (defensive)

        # Request management / throttling
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

        # Dependency check
        self._dependencies_available: bool = False
        self._check_dependencies()

        short_id = (self.sheet_id[:8] + "...") if self.sheet_id else "None"
        logger.info(
            "SymbolsReader initialized: sheet_id=%s, tab_name=%s, cache_ttl=%ss, cache_dir=%s",
            short_id,
            self.tab_name,
            self.cache_ttl.total_seconds(),
            self.cache_dir,
        )

    # -------------------------------------------------------------------------
    # Dependency & Config helpers
    # -------------------------------------------------------------------------
    def _check_dependencies(self) -> None:
        """Check for required dependencies and log appropriate messages."""
        try:
            import gspread  # noqa: F401
            from google.oauth2.service_account import Credentials  # noqa: F401

            self._dependencies_available = True
            logger.debug("Google Sheets dependencies available for SymbolsReader")
        except ImportError as e:
            self._dependencies_available = False
            logger.warning("Google Sheets dependencies not available for SymbolsReader: %s", e)
            logger.info("SymbolsReader will operate in fallback mode (hardcoded symbols)")

    def _get_spreadsheet_id(self) -> str:
        """
        Get spreadsheet ID with proper fallback hierarchy.

        Primary:   SPREADSHEET_ID (shared with main.py Settings)
        Secondary: SHEETS_SPREADSHEET_ID
        Tertiary:  GOOGLE_SHEETS_ID
        """
        env_vars = [
            "SPREADSHEET_ID",           # Primary (same as main.py)
            "SHEETS_SPREADSHEET_ID",    # Secondary
            "GOOGLE_SHEETS_ID",         # Tertiary
        ]

        for env_var in env_vars:
            value = os.getenv(env_var)
            if value and value.strip():
                logger.info("SymbolsReader using spreadsheet ID from %s", env_var)
                return value.strip()

        logger.warning("No spreadsheet ID configured - SymbolsReader will use hardcoded fallback only")
        return ""

    def _setup_cache_directory(self) -> Path:
        """Setup cache directory with Render compatibility."""
        cache_base = os.getenv("SYMBOLS_CACHE_DIR", "/tmp/symbols_cache")

        # On Render, /tmp is the recommended ephemeral storage
        if os.path.exists("/tmp") and os.access("/tmp", os.W_OK):
            cache_dir = Path("/tmp/symbols_cache")
        else:
            cache_dir = Path(cache_base)

        try:
            cache_dir.mkdir(parents=True, exist_ok=True)
            logger.info("Symbols cache directory: %s", cache_dir)
            return cache_dir
        except Exception as e:
            logger.warning("Could not create cache directory %s: %s", cache_dir, e)
            # Fallback to current directory
            fallback_dir = Path("./symbols_cache")
            try:
                fallback_dir.mkdir(parents=True, exist_ok=True)
            except Exception:
                pass
            return fallback_dir

    def _get_cache_ttl(self) -> timedelta:
        """
        Get cache TTL with sensible defaults.

        Priority:
        1) SYMBOLS_CACHE_TTL or SYMBOLS_CACHE_TTL_SECONDS
        2) CACHE_DEFAULT_TTL or CACHE_DEFAULT_TTL_SECONDS (shared with other services)
        3) 1800 seconds (30 minutes)
        """
        # Try dedicated symbols TTL first
        ttl_env = os.getenv("SYMBOLS_CACHE_TTL") or os.getenv("SYMBOLS_CACHE_TTL_SECONDS")
        if ttl_env:
            try:
                ttl_seconds = int(ttl_env)
                return timedelta(seconds=max(300, ttl_seconds))
            except (ValueError, TypeError):
                logger.warning("Invalid SYMBOLS_CACHE_TTL value (%s), falling back", ttl_env)

        # Fallback to global cache TTL if available
        global_ttl_env = os.getenv("CACHE_DEFAULT_TTL") or os.getenv("CACHE_DEFAULT_TTL_SECONDS")
        if global_ttl_env:
            try:
                ttl_seconds = int(global_ttl_env)
                return timedelta(seconds=max(300, ttl_seconds))
            except (ValueError, TypeError):
                logger.warning("Invalid CACHE_DEFAULT_TTL for SymbolsReader, using default 30 minutes")

        # Final default
        return timedelta(seconds=1800)

    def _initialize_column_mapping(self) -> Dict[str, List[str]]:
        """Initialize comprehensive column mapping (English + Arabic variants)."""
        return {
            "symbol": [
                "Symbol", "Ticker", "Ticker Symbol", "Code",
                "رمز", "الرمز"
            ],
            "company_name": [
                "Company Name", "Company", "Name", "Security Name",
                "اسم الشركة", "الشركة"
            ],
            "trading_sector": [
                "Sector", "Trading Sector", "Industry", "Industry Sector",
                "القطاع", "قطاع التداول"
            ],
            "financial_market": [
                "Market", "Financial Market", "Exchange", "Trading Market",
                "السوق", "السوق المالية"
            ],
            "include_in_ranking": [
                "Include", "Include in Ranking", "Active", "Enabled",
                "مشمول", "مشمول في التصنيف"
            ],
            "market_cap": [
                "Market Cap", "Market Capitalization", "Capitalization",
                "القيمة السوقية", "رأس المال السوقي"
            ],
        }

    # -------------------------------------------------------------------------
    # Request management (throttling / retry)
    # -------------------------------------------------------------------------
    def _throttle_requests(self) -> None:
        """Enhanced request throttling with small jitter."""
        now = time.time()
        elapsed = now - self.last_request_time

        if elapsed < self.min_request_interval:
            sleep_time = self.min_request_interval - elapsed
            # Small jitter (0–100ms) to avoid synchronized bursts
            jitter = (time.time() % 0.1)
            time.sleep(sleep_time + jitter)

        self.last_request_time = time.time()

    def _retry_operation(self, operation, operation_name: str = "operation"):
        """Retry wrapper with exponential backoff."""
        last_exception = None

        for attempt in range(self.max_retries):
            try:
                self._throttle_requests()
                return operation()
            except Exception as e:
                last_exception = e
                logger.warning(
                    "Attempt %d/%d failed for %s: %s",
                    attempt + 1, self.max_retries, operation_name, e,
                )

                if attempt < self.max_retries - 1:
                    wait_time = self.retry_delay * (2 ** attempt)  # Exponential backoff
                    logger.info("Retrying %s in %.2fs...", operation_name, wait_time)
                    time.sleep(wait_time)

        logger.error("All retries failed for %s: %s", operation_name, last_exception)
        raise last_exception

    # -------------------------------------------------------------------------
    # Cache management
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
            # Only cleanup once per hour to reduce I/O
            if now - self._last_cache_cleanup < 3600:
                return

            cache_files = list(self.cache_dir.glob("symbols_cache_*.json"))
            deleted_count = 0

            for cache_file in cache_files:
                try:
                    # Delete files older than 24 hours, but keep current cache
                    if (
                        now - cache_file.stat().st_mtime > 86400
                        and cache_file.name != self._get_cache_key()
                    ):
                        cache_file.unlink()
                        deleted_count += 1
                except Exception as e:
                    logger.debug("Could not delete cache file %s: %s", cache_file, e)

            self._last_cache_cleanup = now
            if deleted_count > 0:
                logger.info("Cleaned up %d old symbols cache files", deleted_count)

        except Exception as e:
            logger.debug("Symbols cache cleanup failed: %s", e)

    def _get_cache_size_mb(self) -> float:
        """Calculate current cache directory size in MB."""
        try:
            total_size = 0
            for file_path in self.cache_dir.glob("*"):
                if file_path.is_file():
                    total_size += file_path.stat().st_size
            return total_size / (1024 * 1024)
        except Exception:
            return 0.0

    def _load_from_cache(self) -> Optional[List[Dict[str, Any]]]:
        """Load symbols from cache, honoring TTL and size limits."""
        cache_file = self.cache_dir / self._get_cache_key()

        try:
            if not cache_file.exists():
                return None

            # Check cache file size (avoid loading huge files)
            file_size = cache_file.stat().st_size
            if file_size > 10 * 1024 * 1024:  # 10MB limit
                logger.warning("Symbols cache file too large (%d bytes), deleting", file_size)
                cache_file.unlink(missing_ok=True)
                return None

            raw_data = cache_file.read_text(encoding="utf-8")
            payload = json.loads(raw_data)

            if not isinstance(payload, dict) or "timestamp" not in payload or "data" not in payload:
                logger.warning("Invalid symbols cache structure, deleting")
                cache_file.unlink(missing_ok=True)
                return None

            cache_time = datetime.fromisoformat(payload["timestamp"].replace("Z", "+00:00"))
            age = datetime.now().replace(tzinfo=cache_time.tzinfo) - cache_time
            if age > self.cache_ttl:
                logger.debug("Symbols cache expired (age: %.1fs)", age.total_seconds())
                return None

            data = payload.get("data", [])
            if not isinstance(data, list):
                logger.warning("Invalid symbols cache data format")
                return None

            logger.info("Loaded %d symbols from cache", len(data))
            return data

        except json.JSONDecodeError as e:
            logger.warning("Corrupted symbols cache file: %s", e)
            try:
                cache_file.unlink(missing_ok=True)
            except Exception:
                pass
            return None
        except Exception as e:
            logger.debug("Symbols cache load failed: %s", e)
            return None

    def _save_to_cache(self, data: List[Dict[str, Any]]) -> bool:
        """Save symbols to cache with size limits and atomic writes."""
        try:
            current_size = self._get_cache_size_mb()
            if current_size > self.max_cache_size_mb:
                logger.warning(
                    "Symbols cache size (%.1fMB) exceeds limit (%.1fMB), cleaning up",
                    current_size,
                    self.max_cache_size_mb,
                )
                self._cleanup_old_cache_files()

            cache_file = self.cache_dir / self._get_cache_key()
            temp_file = cache_file.with_suffix(".tmp")

            payload = {
                "timestamp": datetime.now().isoformat(),
                "sheet_id": self.sheet_id,
                "tab_name": self.tab_name,
                "data": data,
                "version": "3.2.0",
            }

            temp_file.write_text(
                json.dumps(payload, indent=2, ensure_ascii=False),
                encoding="utf-8",
            )
            temp_file.replace(cache_file)

            logger.info("Saved %d symbols to cache", len(data))
            return True

        except Exception as e:
            logger.warning("Symbols cache save failed: %s", e)
            return False

    # -------------------------------------------------------------------------
    # Google Sheets integration
    # -------------------------------------------------------------------------
    def _get_google_client(self):
        """Initialize Google Sheets client using GOOGLE_SHEETS_CREDENTIALS."""
        if self._client_initialized and self._client is not None:
            return self._client

        if not self._dependencies_available:
            logger.error("Google Sheets dependencies not available for SymbolsReader")
            self._client_initialized = True
            return None

        try:
            import gspread
            from google.oauth2.service_account import Credentials

            scopes = [
                "https://www.googleapis.com/auth/spreadsheets.readonly",
                "https://www.googleapis.com/auth/drive.readonly",
            ]

            creds_json = os.getenv("GOOGLE_SHEETS_CREDENTIALS")
            legacy_json = os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON")

            if creds_json:
                logger.info("Initializing SymbolsReader Google client from GOOGLE_SHEETS_CREDENTIALS")
                creds_dict = json.loads(creds_json)
            elif legacy_json:
                logger.warning(
                    "SymbolsReader using legacy GOOGLE_SERVICE_ACCOUNT_JSON - "
                    "please migrate to GOOGLE_SHEETS_CREDENTIALS",
                )
                creds_dict = json.loads(legacy_json)
            else:
                cred_file = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
                if cred_file and os.path.exists(cred_file):
                    logger.info("Initializing SymbolsReader Google client from file: %s", cred_file)
                    with open(cred_file, "r", encoding="utf-8") as f:
                        creds_dict = json.load(f)
                else:
                    logger.error("No Google Sheets credentials available for SymbolsReader")
                    self._client_initialized = True
                    return None

            required_fields = ["type", "project_id", "private_key_id", "private_key", "client_email"]
            missing_fields = [field for field in required_fields if field not in creds_dict]
            if missing_fields:
                logger.error("SymbolsReader missing required credential fields: %s", missing_fields)
                self._client_initialized = True
                return None

            creds = Credentials.from_service_account_info(creds_dict, scopes=scopes)
            self._client = gspread.authorize(creds)
            self._client_initialized = True

            logger.info("Google Sheets client initialized successfully for SymbolsReader")
            return self._client

        except json.JSONDecodeError as e:
            logger.error("Invalid JSON in SymbolsReader Google credentials: %s", e)
            self._client_initialized = True
            return None
        except Exception as e:
            logger.error("Failed to initialize SymbolsReader Google Sheets client: %s", e)
            self._client_initialized = True
            return None

    def _get_worksheet(self):
        """Get the worksheet to read symbols from."""
        if self._worksheet is not None:
            return self._worksheet

        if not self.sheet_id:
            logger.error("No spreadsheet ID configured in SymbolsReader")
            return None

        client = self._get_google_client()
        if client is None:
            return None

        try:
            from gspread.exceptions import WorksheetNotFound, SpreadsheetNotFound, APIError  # type: ignore

            def open_spreadsheet():
                self._throttle_requests()
                return client.open_by_key(self.sheet_id)

            spreadsheet = self._retry_operation(open_spreadsheet, "open_spreadsheet")
            logger.info("Opened spreadsheet for SymbolsReader: %s", spreadsheet.title)

            def get_worksheet():
                self._throttle_requests()
                return spreadsheet.worksheet(self.tab_name)

            try:
                self._worksheet = self._retry_operation(
                    get_worksheet,
                    f"get_worksheet_{self.tab_name}",
                )
                logger.info("Using worksheet for symbols: %s", self.tab_name)
            except WorksheetNotFound:
                logger.warning(
                    "Worksheet '%s' not found for SymbolsReader, trying first sheet",
                    self.tab_name,
                )

                def get_first_worksheet():
                    self._throttle_requests()
                    worksheets = spreadsheet.worksheets()
                    return worksheets[0] if worksheets else None

                self._worksheet = self._retry_operation(
                    get_first_worksheet,
                    "get_first_worksheet",
                )
                if self._worksheet:
                    logger.info("Using first worksheet for symbols: %s", self._worksheet.title)
                else:
                    logger.error("No worksheets available in spreadsheet for symbols")
                    return None

            return self._worksheet

        except SpreadsheetNotFound:
            logger.error("Spreadsheet not found for symbols: %s", self.sheet_id)
            return None
        except APIError as e:  # type: ignore
            logger.error("Google API error while reading symbols: %s", e)
            return None
        except Exception as e:
            logger.error("Unexpected error accessing worksheet for symbols: %s", e)
            return None

    # -------------------------------------------------------------------------
    # Data parsing & mapping
    # -------------------------------------------------------------------------
    def _map_columns(self, headers: List[str]) -> Dict[str, int]:
        """Map sheet headers to logical fields with simple fuzzy matching."""
        column_map: Dict[str, int] = {}
        used_indices = set()

        for field, possible_names in self.column_mapping.items():
            best_match = None
            best_score = 0.0

            for idx, header in enumerate(headers):
                if idx in used_indices:
                    continue

                header_clean = str(header).strip().lower()
                if not header_clean:
                    continue

                for name in possible_names:
                    name_clean = name.lower()

                    # Exact match
                    if header_clean == name_clean:
                        best_match = idx
                        best_score = 100.0
                        break

                    # Contains match with simple score
                    if name_clean in header_clean:
                        score = len(name_clean) / max(len(header_clean), 1) * 100
                        if score > best_score:
                            best_match = idx
                            best_score = score

                if best_score == 100.0:
                    break

            if best_match is not None and best_score > 40.0:
                column_map[field] = best_match
                used_indices.add(best_match)
                logger.debug(
                    "Mapped field '%s' -> column %d (score: %.1f)",
                    field,
                    best_match,
                    best_score,
                )

        # Ensure we have at least symbol mapping
        if "symbol" not in column_map and headers:
            column_map["symbol"] = 0
            logger.warning("Symbol column not found, using first column as fallback")

        return column_map

    def _parse_row(self, row: List[str], column_map: Dict[str, int]) -> Optional[SymbolRecord]:
        """Parse one row into a SymbolRecord if valid."""
        try:
            sym_idx = column_map.get("symbol", 0)
            if sym_idx >= len(row):
                return None

            symbol = str(row[sym_idx]).strip()
            if not symbol or symbol.lower() in {"", "n/a", "null", "undefined"}:
                return None

            company_name = self._safe_get(row, column_map.get("company_name", 1))
            trading_sector = self._safe_get(row, column_map.get("trading_sector", 2))
            financial_market = self._safe_get(row, column_map.get("financial_market", 3))

            include_flag = True
            if "include_in_ranking" in column_map:
                inc_val = self._safe_get(row, column_map["include_in_ranking"])
                include_flag = self._parse_boolean(inc_val)

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
            logger.debug("Failed to parse symbol row: %s", e)
            return None

    def _safe_get(self, row: List[str], index: int, default: str = "") -> str:
        """Safely get value from row with bounds checking."""
        if 0 <= index < len(row):
            value = str(row[index]).strip()
            return value if value else default
        return default

    def _parse_boolean(self, value: str) -> bool:
        """Parse boolean with English and Arabic variants."""
        if not value:
            return True

        value_lower = value.lower().strip()

        true_values = {
            "true", "yes", "y", "1", "include", "enabled", "active",
            "نعم", "مشمول", "مفعل", "يشمل", "تفعيل",
        }
        false_values = {
            "false", "no", "n", "0", "exclude", "disabled", "inactive",
            "لا", "غير مشمول", "غير مفعل", "لا يشمل", "إلغاء",
        }

        if value_lower in true_values:
            return True
        if value_lower in false_values:
            return False

        # Default to True for unknown values (conservative)
        return True

    def _parse_float(self, value: str) -> Optional[float]:
        """Parse float with basic currency/formatting cleanup and Arabic numerals."""
        if not value:
            return None

        try:
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

            # Arabic digits → English digits
            arabic_to_english = str.maketrans("٠١٢٣٤٥٦٧٨٩", "0123456789")
            cleaned = cleaned.translate(arabic_to_english)

            if cleaned:
                return float(cleaned)
        except (ValueError, TypeError):
            pass

        return None

    # -------------------------------------------------------------------------
    # Fallback data
    # -------------------------------------------------------------------------
    def _get_hardcoded_symbols(self) -> List[Dict[str, Any]]:
        """Hardcoded fallback symbols for Tadawul (used if everything else fails)."""
        base_ts = datetime.utcnow().isoformat() + "Z"
        return [
            {
                "symbol": "2222.SR",
                "company_name": "Saudi Arabian Oil Company (Aramco)",
                "trading_sector": "Energy",
                "financial_market": "Tadawul (TASI)",
                "include_in_ranking": True,
                "market_cap": 2_000_000_000_000,
                "data_source": "hardcoded_fallback",
                "last_updated": base_ts,
            },
            {
                "symbol": "1180.SR",
                "company_name": "Saudi National Bank",
                "trading_sector": "Banks",
                "financial_market": "Tadawul (TASI)",
                "include_in_ranking": True,
                "market_cap": 150_000_000_000,
                "data_source": "hardcoded_fallback",
                "last_updated": base_ts,
            },
            {
                "symbol": "1010.SR",
                "company_name": "Riyad Bank",
                "trading_sector": "Banks",
                "financial_market": "Tadawul (TASI)",
                "include_in_ranking": True,
                "market_cap": 45_000_000_000,
                "data_source": "hardcoded_fallback",
                "last_updated": base_ts,
            },
            {
                "symbol": "2010.SR",
                "company_name": "Saudi Basic Industries Corporation (SABIC)",
                "trading_sector": "Petrochemicals",
                "financial_market": "Tadawul (TASI)",
                "include_in_ranking": True,
                "market_cap": 300_000_000_000,
                "data_source": "hardcoded_fallback",
                "last_updated": base_ts,
            },
            {
                "symbol": "4030.SR",
                "company_name": "National Shipping Company of Saudi Arabia (Bahri)",
                "trading_sector": "Transportation",
                "financial_market": "Tadawul (TASI)",
                "include_in_ranking": True,
                "market_cap": 25_000_000_000,
                "data_source": "hardcoded_fallback",
                "last_updated": base_ts,
            },
        ]

    # -------------------------------------------------------------------------
    # Public API methods
    # -------------------------------------------------------------------------
    def fetch_symbols(
        self,
        limit: Optional[int] = None,
        use_cache: bool = True,
    ) -> dict:
        """
        Fetch symbols with full fallback chain.

        Order:
        1) Cache (if enabled and valid)
        2) Google Sheets
        3) Hardcoded fallback list
        """
        start_time = time.time()

        try:
            # Cache first
            if use_cache:
                cached_data = self._load_from_cache()
                if cached_data is not None:
                    if limit:
                        cached_data = cached_data[:limit]

                    response = self._build_response(
                        cached_data,
                        source="cache",
                        processing_time=time.time() - start_time,
                    )
                    logger.info("Symbols cache hit: %d symbols", len(cached_data))
                    return response

            # Google Sheets second
            symbols_data = self._fetch_from_google_sheets(limit)
            if symbols_data:
                self._save_to_cache(symbols_data)
                self._last_successful_fetch = time.time()

                response = self._build_response(
                    symbols_data,
                    source="google_sheets",
                    processing_time=time.time() - start_time,
                )
                logger.info("Google Sheets fetch successful for symbols: %d symbols", len(symbols_data))
                return response

            # Hardcoded fallback
            hardcoded_data = self._get_hardcoded_symbols()
            if limit:
                hardcoded_data = hardcoded_data[:limit]

            response = self._build_response(
                hardcoded_data,
                source="hardcoded_fallback",
                processing_time=time.time() - start_time,
                error="All data sources failed, using fallback symbols",
            )
            logger.warning("Using fallback symbols for SymbolsReader: %d symbols", len(hardcoded_data))
            return response

        except Exception as e:
            logger.error("Unexpected error in SymbolsReader.fetch_symbols: %s", e)
            hardcoded_data = self._get_hardcoded_symbols()
            if limit:
                hardcoded_data = hardcoded_data[:limit]

            return self._build_response(
                hardcoded_data,
                source="hardcoded_fallback",
                processing_time=time.time() - start_time,
                error=f"Unexpected error: {str(e)}",
            )

    def _fetch_from_google_sheets(self, limit: Optional[int]) -> Optional[List[Dict[str, Any]]]:
        """
        Read all values from the configured worksheet and parse symbol rows.
        """
        try:
            if not self._dependencies_available:
                logger.warning("Google Sheets dependencies not available for SymbolsReader")
                return None

            worksheet = self._get_worksheet()
            if not worksheet:
                return None

            def fetch_data():
                self._throttle_requests()
                return worksheet.get_all_values()

            all_values = self._retry_operation(fetch_data, "get_all_values")

            if not all_values or len(all_values) < self.header_row:
                logger.warning("Insufficient data in symbols worksheet")
                return None

            headers = all_values[self.header_row - 1]
            data_rows = all_values[self.header_row:]

            column_map = self._map_columns(headers)
            symbols: List[Dict[str, Any]] = []

            for row in data_rows:
                # Skip completely empty rows
                if not any(cell and str(cell).strip() for cell in row):
                    continue

                symbol_record = self._parse_row(row, column_map)
                if symbol_record and symbol_record.include_in_ranking:
                    symbols.append(symbol_record.to_dict())

                if limit and len(symbols) >= limit:
                    break

            return symbols if symbols else None

        except Exception as e:
            logger.error("Google Sheets fetch failed for symbols: %s", e)
            return None

    def _build_response(
        self,
        data: List[Dict[str, Any]],
        source: str,
        processing_time: float,
        error: Optional[str] = None,
    ) -> dict:
        """Build standardized response with enhanced metadata."""
        worksheet_title = getattr(self._worksheet, "title", "Unknown") if self._worksheet else "Unknown"
        response = {
            "ok": error is None,
            "source": source,
            "sheet_title": worksheet_title,
            "worksheet": self.tab_name,
            "count": len(data),
            "data": data,
            "timestamp": datetime.utcnow().isoformat() + "Z",
            "processing_time_seconds": round(processing_time, 3),
            "metadata": {
                "sheet_id": (self.sheet_id[:8] + "...") if self.sheet_id else "None",
                "header_row": self.header_row,
                "cache_used": source == "cache",
                "last_successful_fetch": self._last_successful_fetch,
                "dependencies_available": getattr(self, "_dependencies_available", False),
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

        # Overall status flag
        if not self.sheet_id or not self._dependencies_available:
            overall_status = "degraded"
        else:
            overall_status = "operational"

        return {
            "status": overall_status,
            "client_status": client_status,
            "worksheet_status": worksheet_status,
            "spreadsheet_id_configured": bool(self.sheet_id),
            "dependencies_available": getattr(self, "_dependencies_available", False),
            "tab_name": self.tab_name,
            "cache_info": cache_info,
            "last_successful_fetch": self._last_successful_fetch,
            "timestamp": datetime.utcnow().isoformat() + "Z",
        }

    def clear_cache(self) -> dict:
        """Clear the symbols cache file (if present)."""
        try:
            cache_file = self.cache_dir / self._get_cache_key()
            if cache_file.exists():
                cache_file.unlink()
                logger.info("Symbols cache cleared: %s", cache_file)
                return {
                    "status": "cleared",
                    "cache_file": str(cache_file),
                    "timestamp": datetime.utcnow().isoformat() + "Z",
                }
            return {
                "status": "no_cache",
                "cache_file": str(cache_file),
                "timestamp": datetime.utcnow().isoformat() + "Z",
            }
        except Exception as e:
            logger.error("Failed to clear symbols cache: %s", e)
            return {
                "status": "error",
                "error": str(e),
                "timestamp": datetime.utcnow().isoformat() + "Z",
            }

    def get_cache_info(self) -> dict:
        """Return basic info about the symbols cache file."""
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
                "cache_version": payload.get("version", "3.2.0"),
            }
        except Exception as e:
            logger.debug("Failed to read symbols cache info: %s", e)
            return {"exists": False, "error": str(e)}

    # Convenience: get just the list of tickers for ranking engine or advisor
    def get_symbol_list(
        self,
        use_cache: bool = True,
        limit: Optional[int] = None,
    ) -> List[str]:
        """
        Return a flat list of ticker symbols (e.g., for ranking engine / advisor).
        Always respects include_in_ranking.
        """
        result = self.fetch_symbols(limit=limit, use_cache=use_cache)
        return [row["symbol"] for row in result.get("data", []) if "symbol" in row]


# Global instance for main.py / routes to use
try:
    symbols_reader = SymbolsReader()
    logger.info("Global SymbolsReader instance created successfully")
except Exception as e:
    logger.error("Failed to create global SymbolsReader: %s", e)
    symbols_reader = None


def fetch_symbols(limit: Optional[int] = None, use_cache: bool = True) -> dict:
    """
    Backward-compatible function for integration from main.py / routes.

    Args:
        limit: Maximum number of symbols to return.
        use_cache: Whether to use cache (default True).

    Returns:
        Standardized symbols response (ok/source/data/count/metadata).
    """
    if symbols_reader is None:
        logger.error("SymbolsReader not available (global instance is None)")
        return {
            "ok": False,
            "source": "error",
            "count": 0,
            "data": [],
            "timestamp": datetime.utcnow().isoformat() + "Z",
            "error": "SymbolsReader not initialized",
        }

    return symbols_reader.fetch_symbols(limit=limit, use_cache=use_cache)


# Simple CLI test (local execution only)
if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    reader = SymbolsReader()
    result = reader.fetch_symbols(limit=5)
    print(f"Status: {result['ok']}")
    print(f"Source: {result['source']}")
    print(f"Count: {result['count']}")
    print(f"Sample symbols: {result['data'][:2] if result['data'] else 'None'}")
