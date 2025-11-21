# symbols_reader.py
# Enhanced Google Sheets symbol reader with robust error handling and caching

from __future__ import annotations

import os
import json
import logging
import time
from pathlib import Path
from typing import Dict, List, Optional, Any, Tuple
from datetime import datetime, timedelta
from dataclasses import dataclass

# Configure logging
logger = logging.getLogger(__name__)

@dataclass
class SymbolRecord:
    """Data class for symbol records with validation."""
    symbol: str
    company_name: str
    trading_sector: str
    financial_market: str
    include_in_ranking: bool = True
    market_cap: Optional[float] = None
    last_updated: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for JSON serialization."""
        return {
            'symbol': self.symbol,
            'company_name': self.company_name,
            'trading_sector': self.trading_sector,
            'financial_market': self.financial_market,
            'include_in_ranking': self.include_in_ranking,
            'market_cap': self.market_cap,
            'last_updated': self.last_updated or datetime.utcnow().isoformat()
        }

class SymbolsReader:
    """Enhanced Google Sheets symbol reader with caching and error handling."""
    
    def __init__(self):
        # Configuration from environment variables with fallbacks
        self.credentials_file = os.getenv(
            'GOOGLE_APPLICATION_CREDENTIALS',
            'gcp-credentials-saudi-stocks.json'
        )
        self.sheet_id = os.getenv(
            'SHEETS_SPREADSHEET_ID',
            '19oloY3fehdFnSRMysqd-EZ2l7FL-GRAd8GJhYUt8tmw'
        )
        self.tab_name = os.getenv('SHEETS_TAB_SYMBOLS', 'Market_Leaders')
        self.header_row = int(os.getenv('SHEETS_HEADER_ROW', '3'))
        
        # Cache configuration
        self.cache_dir = Path("./symbols_cache")
        self.cache_dir.mkdir(exist_ok=True)
        self.cache_ttl = timedelta(minutes=30)  # Cache for 30 minutes
        
        # Column mapping with flexible field detection
        self.column_mapping = self._detect_column_mapping()
        
        # Google Sheets client
        self._client = None
        self._worksheet = None
        
        # Request throttling
        self.last_request_time = 0
        self.min_request_interval = 1.0  # 1 second between requests

    def _throttle_requests(self):
        """Implement request throttling to avoid rate limits."""
        current_time = time.time()
        time_since_last = current_time - self.last_request_time
        if time_since_last < self.min_request_interval:
            time.sleep(self.min_request_interval - time_since_last)
        self.last_request_time = time.time()

    def _detect_column_mapping(self) -> Dict[str, Any]:
        """Detect column mapping based on common column names."""
        return {
            'symbol': ['Symbol', 'Ticker', 'Ticker Symbol', 'Code'],
            'company_name': ['Company Name', 'Company', 'Name', 'Security Name'],
            'trading_sector': ['Sector', 'Trading Sector', 'Industry', 'Industry Sector'],
            'financial_market': ['Market', 'Financial Market', 'Exchange', 'Trading Market'],
            'include_in_ranking': ['Include', 'Include in Ranking', 'Active', 'Enabled'],
            'market_cap': ['Market Cap', 'Market Capitalization', 'Capitalization']
        }

    def _get_cache_key(self) -> str:
        """Generate cache key based on sheet and configuration."""
        return f"symbols_{self.sheet_id}_{self.tab_name}_{self.header_row}.json"

    def _load_from_cache(self) -> Optional[List[Dict[str, Any]]]:
        """Load symbols from cache if valid."""
        try:
            cache_file = self.cache_dir / self._get_cache_key()
            if cache_file.exists():
                data = json.loads(cache_file.read_text(encoding="utf-8"))
                cache_time = datetime.fromisoformat(data.get('timestamp', '2000-01-01'))
                if datetime.now() - cache_time < self.cache_ttl:
                    logger.info(f"Loaded {len(data.get('data', []))} symbols from cache")
                    return data.get('data', [])
        except Exception as e:
            logger.debug(f"Cache load failed: {e}")
        return None

    def _save_to_cache(self, data: List[Dict[str, Any]]):
        """Save symbols to cache."""
        try:
            cache_file = self.cache_dir / self._get_cache_key()
            cache_data = {
                'timestamp': datetime.now().isoformat(),
                'sheet_id': self.sheet_id,
                'tab_name': self.tab_name,
                'data': data
            }
            cache_file.write_text(json.dumps(cache_data, indent=2), encoding="utf-8")
            logger.info(f"Saved {len(data)} symbols to cache")
        except Exception as e:
            logger.debug(f"Cache save failed: {e}")

    def _get_google_client(self):
        """Get or create Google Sheets client with error handling."""
        try:
            import gspread
            from google.oauth2.service_account import Credentials
            from google.auth.exceptions import GoogleAuthError
            from gspread.exceptions import APIError, WorksheetNotFound, SpreadsheetNotFound
        except ImportError:
            logger.error("Google Sheets dependencies not available")
            return None

        if self._client is None:
            try:
                if not os.path.exists(self.credentials_file):
                    logger.error(f"Google credentials file not found: {self.credentials_file}")
                    return None
                
                scopes = [
                    "https://www.googleapis.com/auth/spreadsheets.readonly",
                    "https://www.googleapis.com/auth/drive.readonly",
                ]
                
                creds = Credentials.from_service_account_file(
                    self.credentials_file, 
                    scopes=scopes
                )
                self._client = gspread.authorize(creds)
                logger.info("Google Sheets client initialized successfully")
                
            except GoogleAuthError as e:
                logger.error(f"Google authentication failed: {e}")
                return None
            except Exception as e:
                logger.error(f"Failed to initialize Google client: {e}")
                return None
        
        return self._client

    def _get_worksheet(self):
        """Get worksheet with fallback strategies."""
        if self._worksheet is None:
            client = self._get_google_client()
            if client is None:
                return None
            
            try:
                # Import here to avoid circular imports
                from gspread.exceptions import APIError, WorksheetNotFound, SpreadsheetNotFound
                
                # Open spreadsheet
                spreadsheet = client.open_by_key(self.sheet_id)
                logger.info(f"Opened spreadsheet: {spreadsheet.title}")
                
                # Try to get worksheet by name
                try:
                    self._worksheet = spreadsheet.worksheet(self.tab_name)
                    logger.info(f"Found worksheet: {self.tab_name}")
                except WorksheetNotFound:
                    # Fallback: try first worksheet
                    worksheets = spreadsheet.worksheets()
                    if worksheets:
                        self._worksheet = worksheets[0]
                        logger.warning(
                            f"Worksheet '{self.tab_name}' not found, using first worksheet: {self._worksheet.title}"
                        )
                    else:
                        logger.error("No worksheets found in spreadsheet")
                        return None
                        
            except SpreadsheetNotFound:
                logger.error(f"Spreadsheet not found with ID: {self.sheet_id}")
                return None
            except APIError as e:
                logger.error(f"Google Sheets API error: {e}")
                return None
            except Exception as e:
                logger.error(f"Unexpected error getting worksheet: {e}")
                return None
        
        return self._worksheet

    def _get_hardcoded_symbols(self) -> List[Dict[str, Any]]:
        """Return hardcoded symbols as fallback when Google Sheets fails."""
        return [
            {
                "symbol": "7201.SR",
                "company_name": "Saudi Company 7201",
                "trading_sector": "Materials",
                "financial_market": "Nomu",
                "include_in_ranking": True
            },
            {
                "symbol": "9603.SR", 
                "company_name": "Saudi Company 9603",
                "trading_sector": "Telecommunications",
                "financial_market": "Tadawul (TASI)",
                "include_in_ranking": True
            },
            {
                "symbol": "9609.SR",
                "company_name": "Saudi Company 9609", 
                "trading_sector": "Banks",
                "financial_market": "Tadawul (TASI)",
                "include_in_ranking": True
            },
            {
                "symbol": "6070.SR",
                "company_name": "Saudi Company 6070",
                "trading_sector": "Transportation", 
                "financial_market": "Nomu",
                "include_in_ranking": True
            },
            {
                "symbol": "7200.SR",
                "company_name": "Saudi Company 7200",
                "trading_sector": "Petrochemicals",
                "financial_market": "Tadawul (TASI)",
                "include_in_ranking": True
            }
        ]

    def _map_columns(self, headers: List[str]) -> Dict[str, int]:
        """Map column names to indices based on header detection."""
        column_map = {}
        
        for field, possible_names in self.column_mapping.items():
            for idx, header in enumerate(headers):
                header_clean = str(header).strip().lower()
                for name in possible_names:
                    if name.lower() in header_clean:
                        column_map[field] = idx
                        logger.debug(f"Mapped '{field}' to column {idx} ('{header}')")
                        break
                if field in column_map:
                    break
        
        # Ensure we have at least symbol mapping
        if 'symbol' not in column_map:
            # Fallback: assume first column is symbol
            column_map['symbol'] = 0
            logger.warning("Symbol column not found, using first column as symbol")
        
        return column_map

    def _parse_row(self, row: List[str], column_map: Dict[str, int]) -> Optional[SymbolRecord]:
        """Parse a single row into a SymbolRecord."""
        try:
            # Get symbol (required)
            symbol_idx = column_map.get('symbol', 0)
            symbol = str(row[symbol_idx]).strip() if len(row) > symbol_idx else ""
            if not symbol:
                return None
            
            # Get company name
            company_idx = column_map.get('company_name', 1)
            company_name = str(row[company_idx]).strip() if len(row) > company_idx else ""
            
            # Get trading sector
            sector_idx = column_map.get('trading_sector', 2)
            trading_sector = str(row[sector_idx]).strip() if len(row) > sector_idx else ""
            
            # Get financial market
            market_idx = column_map.get('financial_market', 3)
            financial_market = str(row[market_idx]).strip() if len(row) > market_idx else ""
            
            # Get include flag
            include_flag = True
            if 'include_in_ranking' in column_map:
                include_idx = column_map['include_in_ranking']
                include_value = str(row[include_idx]).strip() if len(row) > include_idx else ""
                include_flag = self._parse_boolean(include_value)
            
            # Get market cap if available
            market_cap = None
            if 'market_cap' in column_map:
                cap_idx = column_map['market_cap']
                cap_value = str(row[cap_idx]).strip() if len(row) > cap_idx else ""
                market_cap = self._parse_float(cap_value)
            
            return SymbolRecord(
                symbol=symbol,
                company_name=company_name,
                trading_sector=trading_sector,
                financial_market=financial_market,
                include_in_ranking=include_flag,
                market_cap=market_cap,
                last_updated=datetime.utcnow().isoformat()
            )
            
        except Exception as e:
            logger.warning(f"Failed to parse row: {row} - Error: {e}")
            return None

    def _parse_boolean(self, value: str) -> bool:
        """Parse boolean values from string."""
        if not value:
            return True  # Default to True if not specified
        
        true_values = ['true', 'yes', 'y', '1', 'include', 'enabled', 'active']
        false_values = ['false', 'no', 'n', '0', 'exclude', 'disabled', 'inactive']
        
        value_lower = value.lower().strip()
        if value_lower in true_values:
            return True
        elif value_lower in false_values:
            return False
        else:
            return True  # Default to True for unknown values

    def _parse_float(self, value: str) -> Optional[float]:
        """Parse float values from string."""
        try:
            # Remove common formatting
            cleaned = value.replace(',', '').replace(' ', '').replace('SAR', '').strip()
            if cleaned:
                return float(cleaned)
        except (ValueError, TypeError):
            pass
        return None

    def fetch_symbols(self, limit: int | None = None, use_cache: bool = True) -> dict:
        """
        Fetch symbols from Google Sheets with enhanced error handling and caching.
        
        Args:
            limit: Maximum number of symbols to return (None for all)
            use_cache: Whether to use cached data if available
            
        Returns:
            Dictionary with symbols data and metadata
        """
        # Try cache first
        if use_cache:
            cached_data = self._load_from_cache()
            if cached_data is not None:
                if limit:
                    cached_data = cached_data[:limit]
                return self._build_response(cached_data, source="cache")
        
        try:
            # Throttle requests
            self._throttle_requests()
            
            # Get worksheet
            worksheet = self._get_worksheet()
            if worksheet is None:
                logger.warning("Google Sheets not available, using hardcoded symbols")
                hardcoded_data = self._get_hardcoded_symbols()
                if limit:
                    hardcoded_data = hardcoded_data[:limit]
                return self._build_response(hardcoded_data, source="hardcoded_fallback")
            
            # Get all data
            all_data = worksheet.get_all_values()
            if not all_data or len(all_data) < self.header_row:
                logger.warning("No data found in worksheet or insufficient header rows")
                hardcoded_data = self._get_hardcoded_symbols()
                if limit:
                    hardcoded_data = hardcoded_data[:limit]
                return self._build_response(hardcoded_data, source="hardcoded_fallback")
            
            # Extract headers and data rows
            headers = all_data[self.header_row - 1]  # Header row (0-indexed)
            data_rows = all_data[self.header_row:]   # Data starts after header
            
            # Map columns
            column_map = self._map_columns(headers)
            
            # Parse rows
            symbols = []
            for row in data_rows:
                if not any(cell.strip() for cell in row):
                    continue  # Skip empty rows
                
                symbol_record = self._parse_row(row, column_map)
                if symbol_record and symbol_record.include_in_ranking:
                    symbols.append(symbol_record.to_dict())
                
                # Apply limit if specified
                if limit and len(symbols) >= limit:
                    break
            
            # Save to cache
            self._save_to_cache(symbols)
            
            return self._build_response(symbols, source="google_sheets", column_map=column_map)
            
        except FileNotFoundError as e:
            logger.error(f"Credentials file not found: {e}")
            hardcoded_data = self._get_hardcoded_symbols()
            if limit:
                hardcoded_data = hardcoded_data[:limit]
            return self._build_response(hardcoded_data, source="hardcoded_fallback")
        except Exception as e:
            logger.error(f"Unexpected error fetching symbols: {e}")
            hardcoded_data = self._get_hardcoded_symbols()
            if limit:
                hardcoded_data = hardcoded_data[:limit]
            return self._build_response(hardcoded_data, source="hardcoded_fallback")

    def _build_response(self, data: List[Dict[str, Any]], source: str = "unknown", 
                       column_map: Dict[str, int] = None, error: str = None) -> dict:
        """Build standardized response dictionary."""
        response = {
            "ok": error is None,
            "source": source,
            "sheet_title": getattr(self._worksheet, 'title', 'Unknown') if self._worksheet else 'Unknown',
            "worksheet": self.tab_name,
            "count": len(data),
            "data": data,
            "timestamp": datetime.utcnow().isoformat() + "Z",
            "metadata": {
                "sheet_id": self.sheet_id,
                "header_row": self.header_row,
                "cache_used": source == "cache"
            }
        }
        
        if column_map:
            response["metadata"]["column_mapping"] = column_map
        
        if error:
            response["error"] = error
            response["ok"] = False
        
        return response

    def clear_cache(self):
        """Clear the symbols cache."""
        try:
            cache_file = self.cache_dir / self._get_cache_key()
            if cache_file.exists():
                cache_file.unlink()
                logger.info("Symbols cache cleared")
            return {"status": "cleared", "cache_file": str(cache_file)}
        except Exception as e:
            logger.error(f"Failed to clear cache: {e}")
            return {"status": "error", "error": str(e)}

    def get_cache_info(self) -> dict:
        """Get cache information."""
        try:
            cache_file = self.cache_dir / self._get_cache_key()
            if cache_file.exists():
                data = json.loads(cache_file.read_text(encoding="utf-8"))
                cache_time = datetime.fromisoformat(data.get('timestamp', '2000-01-01'))
                age = datetime.now() - cache_time
                return {
                    "exists": True,
                    "age_seconds": age.total_seconds(),
                    "age_minutes": round(age.total_seconds() / 60, 1),
                    "symbol_count": len(data.get('data', [])),
                    "last_updated": data.get('timestamp')
                }
            else:
                return {"exists": False}
        except Exception as e:
            logger.error(f"Failed to get cache info: {e}")
            return {"exists": False, "error": str(e)}

# Global instance for backward compatibility
symbols_reader = SymbolsReader()

def fetch_symbols(limit: int | None = None) -> dict:
    """
    Backward compatibility function.
    Fetch symbols using the global SymbolsReader instance.
    """
    return symbols_reader.fetch_symbols(limit=limit)
