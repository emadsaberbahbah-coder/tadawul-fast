# google_sheets_service.py - ENHANCED VERSION
import os
import json
import time
import logging
from typing import List, Dict, Any, Optional, Union, Tuple
from datetime import datetime, timedelta
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from cachetools import TTLCache, cached
from dataclasses import dataclass, asdict
from enum import Enum
import pandas as pd
from pathlib import Path

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class DataQuality(Enum):
    HIGH = "high"
    MEDIUM = "medium"
    LOW = "low"
    UNKNOWN = "unknown"

@dataclass
class FinancialData:
    """Structured financial data container"""
    ticker: str
    custom_tag: str = ""
    company_name: str = ""
    sector: str = ""
    sub_sector: str = ""
    trading_market: str = ""
    currency: str = "SAR"
    
    # Market Data
    last_price: float = 0.0
    day_high: float = 0.0
    day_low: float = 0.0
    previous_close: float = 0.0
    open_price: float = 0.0
    change_value: float = 0.0
    change_pct: float = 0.0
    high_52w: float = 0.0
    low_52w: float = 0.0
    avg_price_50d: float = 0.0
    
    # Volume & Liquidity
    volume: int = 0
    avg_volume_30d: int = 0
    value_traded: float = 0.0
    turnover_rate: float = 0.0
    bid_price: float = 0.0
    ask_price: float = 0.0
    bid_size: int = 0
    ask_size: int = 0
    spread_pct: float = 0.0
    liquidity_score: float = 0.0
    
    # Company Metrics
    shares_outstanding: float = 0.0
    free_float: float = 0.0
    market_cap: float = 0.0
    listing_date: str = ""
    
    # Valuation Ratios
    eps: float = 0.0
    pe: float = 0.0
    pb: float = 0.0
    dividend_yield: float = 0.0
    dividend_payout: float = 0.0
    roe: float = 0.0
    roa: float = 0.0
    debt_equity: float = 0.0
    current_ratio: float = 0.0
    quick_ratio: float = 0.0
    
    # Growth & Profitability
    revenue_growth: float = 0.0
    net_income_growth: float = 0.0
    ebitda_margin: float = 0.0
    operating_margin: float = 0.0
    net_margin: float = 0.0
    ev_ebitda: float = 0.0
    price_sales: float = 0.0
    price_cash_flow: float = 0.0
    peg_ratio: float = 0.0
    opportunity_score: float = 0.0
    
    # Technical Indicators
    rsi_14: float = 0.0
    macd: float = 0.0
    ma_20d: float = 0.0
    ma_50d: float = 0.0
    volatility: float = 0.0
    
    # Metadata
    last_updated: str = ""
    last_updated_riyadh: str = ""
    data_source: str = ""
    data_quality: DataQuality = DataQuality.UNKNOWN
    timestamp: str = ""
    raw_data: Dict[str, Any] = None
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary with proper type handling"""
        data = asdict(self)
        data['data_quality'] = self.data_quality.value
        return data

class EnhancedGoogleSheetsService:
    """Enhanced Google Sheets Service with caching, validation, and structured data"""
    
    def __init__(self, credentials_path: str = None, cache_ttl: int = 300):
        """
        Initialize Google Sheets service
        
        Args:
            credentials_path: Path to service account credentials JSON
            cache_ttl: Cache time-to-live in seconds (default 5 minutes)
        """
        self.credentials_path = credentials_path or os.getenv('GOOGLE_CREDENTIALS_PATH')
        self.cache_ttl = cache_ttl
        self.cache_dir = Path(".cache")
        self.cache_dir.mkdir(exist_ok=True)
        
        # Initialize caches
        self._sheet_cache = TTLCache(maxsize=100, ttl=cache_ttl)
        self._data_cache = TTLCache(maxsize=50, ttl=cache_ttl)
        
        # Request tracking
        self.request_count = 0
        self.success_count = 0
        self.error_count = 0
        self.last_request_time = 0
        self.request_delay = 1.0  # Delay between requests in seconds
        
        # Initialize service
        self.service = self._initialize_service()
        
        # Column mappings for different data sources
        self.column_mappings = {
            "KSA_TADAWUL": {
                "ticker": ["Symbol", "Ticker", "Code"],
                "company_name": ["Company Name", "Instrument Name"],
                "sector": ["Sector"],
                "sub_sector": ["Sub-Sector"],
                "last_price": ["Last Price", "Price"],
                "volume": ["Volume"],
                "market_cap": ["Market Cap"],
                "pe": ["P/E", "P/E Ratio"],
                "dividend_yield": ["Dividend Yield"],
                "custom_tag": ["Custom Tag", "Custom Tag / Watchlist", "Watchlist"]
            },
            "TADAWUL_ALL": {
                "ticker": ["Symbol", "Ticker"],
                "company_name": ["Company Name", "Name"],
                "sector": ["Sector", "Industry"],
                "market_cap": ["Market Cap", "Market Capitalization"],
                "price": ["Price", "Close"],
                "change": ["Change", "Change %"]
            }
        }
        
        # Default sheet IDs/keys
        self.sheet_keys = {
            "KSA_TADAWUL": os.getenv('KSA_TADAWUL_SHEET_ID', ''),
            "TADAWUL_ALL": os.getenv('TADAWUL_ALL_SHEET_ID', ''),
            "MARKET_DATA": os.getenv('MARKET_DATA_SHEET_ID', ''),
            "PORTFOLIO": os.getenv('PORTFOLIO_SHEET_ID', '')
        }
        
        logger.info("✅ Enhanced Google Sheets Service initialized")
    
    def _initialize_service(self):
        """Initialize Google Sheets service with credentials"""
        try:
            if not self.credentials_path:
                raise ValueError("Credentials path not provided")
            
            creds = Credentials.from_service_account_file(
                self.credentials_path,
                scopes=['https://www.googleapis.com/auth/spreadsheets.readonly']
            )
            
            service = build('sheets', 'v4', credentials=creds)
            logger.info("🔑 Google Sheets service authenticated successfully")
            return service
            
        except Exception as e:
            logger.error(f"❌ Failed to initialize Google Sheets service: {e}")
            raise
    
    def _throttle_requests(self):
        """Throttle requests to avoid rate limiting"""
        current_time = time.time()
        time_since_last = current_time - self.last_request_time
        
        if time_since_last < self.request_delay:
            sleep_time = self.request_delay - time_since_last
            time.sleep(sleep_time)
        
        self.last_request_time = time.time()
    
    def _safe_float(self, value: Any) -> float:
        """Safely convert any value to float"""
        try:
            if isinstance(value, (int, float)):
                return float(value)
            elif isinstance(value, str):
                # Remove any non-numeric characters except decimal point and minus
                cleaned = ''.join(c for c in value if c.isdigit() or c in '.-')
                if cleaned and cleaned != '-':
                    return float(cleaned)
            return 0.0
        except (ValueError, TypeError):
            return 0.0
    
    def _safe_int(self, value: Any) -> int:
        """Safely convert any value to integer"""
        try:
            return int(self._safe_float(value))
        except (ValueError, TypeError):
            return 0
    
    def _safe_str(self, value: Any) -> str:
        """Safely convert any value to string"""
        if value is None:
            return ""
        return str(value).strip()
    
    def _get_cache_key(self, sheet_key: str, **kwargs) -> str:
        """Generate cache key from parameters"""
        params = sorted([f"{k}={v}" for k, v in kwargs.items()])
        return f"{sheet_key}:{'|'.join(params)}"
    
    def _load_from_cache(self, cache_key: str) -> Optional[Any]:
        """Load data from file cache"""
        try:
            cache_file = self.cache_dir / f"{cache_key}.json"
            if cache_file.exists():
                with open(cache_file, 'r') as f:
                    data = json.load(f)
                # Check if cache is still valid
                if time.time() - data.get('_timestamp', 0) < self.cache_ttl:
                    logger.debug(f"📂 Loaded from cache: {cache_key}")
                    return data.get('data')
        except Exception as e:
            logger.debug(f"Cache load failed: {e}")
        return None
    
    def _save_to_cache(self, cache_key: str, data: Any):
        """Save data to file cache"""
        try:
            cache_file = self.cache_dir / f"{cache_key}.json"
            cache_data = {
                'data': data,
                '_timestamp': time.time(),
                '_expires': time.time() + self.cache_ttl
            }
            with open(cache_file, 'w') as f:
                json.dump(cache_data, f)
            logger.debug(f"💾 Saved to cache: {cache_key}")
        except Exception as e:
            logger.debug(f"Cache save failed: {e}")
    
    def _map_column_name(self, row: Dict, field_name: str, sheet_key: str) -> Any:
        """Map field name to actual column name in sheet"""
        mappings = self.column_mappings.get(sheet_key, {})
        if field_name in mappings:
            for possible_name in mappings[field_name]:
                if possible_name in row:
                    return row[possible_name]
        # Fallback to direct field name
        return row.get(field_name)
    
    def _assess_data_quality(self, entry: Dict[str, Any]) -> DataQuality:
        """Assess quality of financial data"""
        score = 0
        total_fields = 0
        
        # Critical fields
        critical_fields = ['ticker', 'last_price', 'volume', 'market_cap']
        for field in critical_fields:
            if entry.get(field):
                if field == 'last_price' and entry[field] > 0:
                    score += 2
                elif field == 'ticker' and entry[field]:
                    score += 2
                else:
                    score += 1
            total_fields += 1
        
        # Important fields
        important_fields = ['pe', 'dividend_yield', 'sector', 'company_name']
        for field in important_fields:
            if entry.get(field):
                score += 1
            total_fields += 1
        
        if total_fields == 0:
            return DataQuality.UNKNOWN
        
        quality_score = score / total_fields
        
        if quality_score >= 0.8:
            return DataQuality.HIGH
        elif quality_score >= 0.5:
            return DataQuality.MEDIUM
        else:
            return DataQuality.LOW
    
    def get_sheet_by_key(self, sheet_key: str):
        """Get sheet by predefined key"""
        sheet_id = self.sheet_keys.get(sheet_key)
        if not sheet_id:
            logger.error(f"Sheet key '{sheet_key}' not found in configuration")
            return None
        
        try:
            self._throttle_requests()
            sheet = self.service.spreadsheets().get(spreadsheetId=sheet_id).execute()
            return sheet
        except HttpError as e:
            logger.error(f"HTTP error accessing sheet {sheet_key}: {e}")
            return None
        except Exception as e:
            logger.error(f"Error accessing sheet {sheet_key}: {e}")
            return None
    
    def read_sheet_data(self, sheet_key: str, use_cache: bool = True, 
                        limit: Optional[int] = None, 
                        validate: bool = True) -> List[Dict[str, Any]]:
        """
        Generic method to read any sheet data
        
        Args:
            sheet_key: Predefined sheet key
            use_cache: Whether to use cached data
            limit: Maximum number of rows to return
            validate: Whether to validate and filter data
        
        Returns:
            List of processed data rows
        """
        cache_key = self._get_cache_key(sheet_key, limit=limit, validate=validate)
        
        if use_cache:
            cached_data = self._load_from_cache(cache_key)
            if cached_data is not None:
                return cached_data[:limit] if limit else cached_data
        
        try:
            sheet = self.get_sheet_by_key(sheet_key)
            if not sheet:
                return []
            
            # Get first worksheet
            worksheet_title = sheet['sheets'][0]['properties']['title']
            range_name = f"{worksheet_title}!A:Z"
            
            self._throttle_requests()
            result = self.service.spreadsheets().values().get(
                spreadsheetId=self.sheet_keys[sheet_key],
                range=range_name
            ).execute()
            
            rows = result.get('values', [])
            if not rows:
                return []
            
            # Convert to list of dicts
            headers = rows[0]
            data_rows = []
            
            for row in rows[1:]:
                if not row:  # Skip empty rows
                    continue
                
                # Pad row to match headers length
                padded_row = row + [''] * (len(headers) - len(row))
                data_dict = dict(zip(headers, padded_row))
                
                if validate and not self._validate_row(data_dict, sheet_key):
                    continue
                
                data_rows.append(data_dict)
                
                if limit and len(data_rows) >= limit:
                    break
            
            logger.info(f"📊 Read {len(data_rows)} rows from {sheet_key}")
            
            # Process data based on sheet type
            processed_data = self._process_sheet_data(data_rows, sheet_key)
            
            if use_cache and processed_data:
                self._save_to_cache(cache_key, processed_data)
            
            return processed_data
            
        except Exception as e:
            logger.error(f"❌ Failed to read sheet {sheet_key}: {e}")
            self.error_count += 1
            return []
    
    def _validate_row(self, row: Dict[str, Any], sheet_key: str) -> bool:
        """Validate a single data row"""
        # Get ticker
        ticker = self._map_column_name(row, 'ticker', sheet_key)
        if not ticker:
            return False
        
        # For financial data, ensure we have at least price or volume
        if sheet_key in ["KSA_TADAWUL", "TADAWUL_ALL"]:
            price = self._safe_float(self._map_column_name(row, 'last_price', sheet_key))
            volume = self._safe_int(self._map_column_name(row, 'volume', sheet_key))
            
            # Skip if both price and volume are zero/missing
            if price == 0 and volume == 0:
                return False
        
        return True
    
    def _process_sheet_data(self, rows: List[Dict[str, Any]], sheet_key: str) -> List[Dict[str, Any]]:
        """Process sheet data based on sheet type"""
        if sheet_key == "KSA_TADAWUL":
            return self._process_ksa_tadawul_data(rows)
        elif sheet_key == "TADAWUL_ALL":
            return self._process_tadawul_all_data(rows)
        else:
            return self._process_generic_data(rows, sheet_key)
    
    def _process_ksa_tadawul_data(self, rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Process KSA Tadawul specific data"""
        processed_rows = []
        
        for row in rows:
            try:
                # Create FinancialData object
                data = FinancialData(
                    ticker=self._safe_str(self._map_column_name(row, 'ticker', "KSA_TADAWUL")),
                    custom_tag=self._safe_str(self._map_column_name(row, 'custom_tag', "KSA_TADAWUL")),
                    company_name=self._safe_str(self._map_column_name(row, 'company_name', "KSA_TADAWUL")),
                    sector=self._safe_str(self._map_column_name(row, 'sector', "KSA_TADAWUL")),
                    sub_sector=self._safe_str(self._map_column_name(row, 'sub_sector', "KSA_TADAWUL")),
                    trading_market=self._safe_str(self._map_column_name(row, 'trading_market', "KSA_TADAWUL")),
                    currency=self._safe_str(row.get("Currency", "SAR")),
                    
                    # Market Data
                    last_price=self._safe_float(self._map_column_name(row, 'last_price', "KSA_TADAWUL")),
                    day_high=self._safe_float(row.get("Day High")),
                    day_low=self._safe_float(row.get("Day Low")),
                    previous_close=self._safe_float(row.get("Previous Close")),
                    open_price=self._safe_float(row.get("Open")),
                    change_value=self._safe_float(row.get("Change Value")),
                    change_pct=self._safe_float(row.get("Change %")),
                    high_52w=self._safe_float(row.get("52 Week High")),
                    low_52w=self._safe_float(row.get("52 Week Low")),
                    avg_price_50d=self._safe_float(row.get("Average Price (50D)")),
                    
                    # Volume & Liquidity
                    volume=self._safe_int(row.get("Volume")),
                    avg_volume_30d=self._safe_int(row.get("Average Volume (30D)")),
                    value_traded=self._safe_float(row.get("Value Traded")),
                    turnover_rate=self._safe_float(row.get("Turnover Rate")),
                    bid_price=self._safe_float(row.get("Bid Price")),
                    ask_price=self._safe_float(row.get("Ask Price")),
                    bid_size=self._safe_int(row.get("Bid Size")),
                    ask_size=self._safe_int(row.get("Ask Size")),
                    spread_pct=self._safe_float(row.get("Spread %")),
                    liquidity_score=self._safe_float(row.get("Liquidity Score")),
                    
                    # Company Metrics
                    shares_outstanding=self._safe_float(row.get("Shares Outstanding")),
                    free_float=self._safe_float(row.get("Free Float")),
                    market_cap=self._safe_float(row.get("Market Cap")),
                    listing_date=self._safe_str(row.get("Listing Date")),
                    
                    # Valuation Ratios
                    eps=self._safe_float(row.get("EPS")),
                    pe=self._safe_float(self._map_column_name(row, 'pe', "KSA_TADAWUL")),
                    pb=self._safe_float(row.get("P/B")),
                    dividend_yield=self._safe_float(self._map_column_name(row, 'dividend_yield', "KSA_TADAWUL")),
                    dividend_payout=self._safe_float(row.get("Dividend Payout")),
                    roe=self._safe_float(row.get("ROE")),
                    roa=self._safe_float(row.get("ROA")),
                    debt_equity=self._safe_float(row.get("Debt/Equity")),
                    current_ratio=self._safe_float(row.get("Current Ratio")),
                    quick_ratio=self._safe_float(row.get("Quick Ratio")),
                    
                    # Growth & Profitability
                    revenue_growth=self._safe_float(row.get("Revenue Growth")),
                    net_income_growth=self._safe_float(row.get("Net Income Growth")),
                    ebitda_margin=self._safe_float(row.get("EBITDA Margin")),
                    operating_margin=self._safe_float(row.get("Operating Margin")),
                    net_margin=self._safe_float(row.get("Net Margin")),
                    ev_ebitda=self._safe_float(row.get("EV/EBITDA")),
                    price_sales=self._safe_float(row.get("Price/Sales")),
                    price_cash_flow=self._safe_float(row.get("Price/Cash Flow")),
                    peg_ratio=self._safe_float(row.get("PEG Ratio")),
                    opportunity_score=self._safe_float(row.get("Opportunity Score")),
                    
                    # Technical Indicators
                    rsi_14=self._safe_float(row.get("RSI (14)")),
                    macd=self._safe_float(row.get("MACD")),
                    ma_20d=self._safe_float(row.get("Moving Avg (20D)")),
                    ma_50d=self._safe_float(row.get("Moving Avg (50D)")),
                    volatility=self._safe_float(row.get("Volatility")),
                    
                    # Metadata
                    last_updated=self._safe_str(row.get("Last Updated")),
                    last_updated_riyadh=self._safe_str(row.get("Last Updated (Riyadh)")),
                    data_source=self._safe_str(row.get("Data Source")),
                    timestamp=datetime.now().isoformat(),
                    raw_data=row
                )
                
                # Assess data quality
                data.data_quality = self._assess_data_quality(data.to_dict())
                
                processed_rows.append(data.to_dict())
                
            except Exception as e:
                logger.warning(f"⚠️ Failed to process row: {e}")
                continue
        
        return processed_rows
    
    def _process_tadawul_all_data(self, rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Process comprehensive Tadawul data"""
        processed_rows = []
        
        for row in rows:
            try:
                processed_rows.append({
                    "ticker": self._safe_str(self._map_column_name(row, 'ticker', "TADAWUL_ALL")),
                    "company_name": self._safe_str(self._map_column_name(row, 'company_name', "TADAWUL_ALL")),
                    "sector": self._safe_str(self._map_column_name(row, 'sector', "TADAWUL_ALL")),
                    "market_cap": self._safe_float(self._map_column_name(row, 'market_cap', "TADAWUL_ALL")),
                    "price": self._safe_float(self._map_column_name(row, 'price', "TADAWUL_ALL")),
                    "change": self._safe_float(self._map_column_name(row, 'change', "TADAWUL_ALL")),
                    "volume": self._safe_int(row.get("Volume")),
                    "pe": self._safe_float(row.get("P/E")),
                    "dividend_yield": self._safe_float(row.get("Dividend Yield")),
                    "timestamp": datetime.now().isoformat(),
                    "raw_data": row
                })
            except Exception as e:
                logger.warning(f"⚠️ Failed to process Tadawul row: {e}")
                continue
        
        return processed_rows
    
    def _process_generic_data(self, rows: List[Dict[str, Any]], sheet_key: str) -> List[Dict[str, Any]]:
        """Process generic sheet data"""
        return rows  # Return as-is for generic sheets
    
    def read_ksa_tadawul_market(self, use_cache: bool = True, limit: Optional[int] = None,
                                validate: bool = True, 
                                filters: Optional[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
        """
        Enhanced KSA Tadawul Market data reading with caching and filtering
        
        Args:
            use_cache: Use cached data
            limit: Maximum number of results
            validate: Validate data quality
            filters: Dictionary of filters to apply
        
        Returns:
            Filtered and processed market data
        """
        logger.info(f"📈 Reading KSA Tadawul Market data (cache: {use_cache}, limit: {limit})")
        
        data = self.read_sheet_data("KSA_TADAWUL", use_cache, limit, validate)
        
        # Apply filters if provided
        if filters and data:
            data = self._apply_filters(data, filters)
        
        logger.info(f"✅ Retrieved {len(data)} KSA Tadawul instruments")
        return data
    
    def _apply_filters(self, data: List[Dict[str, Any]], filters: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Apply filters to data"""
        filtered_data = []
        
        for item in data:
            include = True
            
            for field, condition in filters.items():
                if field not in item:
                    include = False
                    break
                
                value = item[field]
                
                # Handle different filter types
                if isinstance(condition, dict):
                    if "min" in condition and value < condition["min"]:
                        include = False
                    if "max" in condition and value > condition["max"]:
                        include = False
                    if "equals" in condition and value != condition["equals"]:
                        include = False
                    if "in" in condition and value not in condition["in"]:
                        include = False
                elif isinstance(condition, (list, tuple)):
                    if value not in condition:
                        include = False
                else:
                    if value != condition:
                        include = False
                
                if not include:
                    break
            
            if include:
                filtered_data.append(item)
        
        return filtered_data
    
    def get_ticker_details(self, ticker: str, use_cache: bool = True) -> Optional[Dict[str, Any]]:
        """Get detailed information for specific ticker"""
        data = self.read_ksa_tadawul_market(use_cache=use_cache)
        
        for item in data:
            if item.get('ticker') == ticker:
                return item
        
        return None
    
    def search_by_sector(self, sector: str, use_cache: bool = True) -> List[Dict[str, Any]]:
        """Search instruments by sector"""
        data = self.read_ksa_tadawul_market(use_cache=use_cache)
        
        return [
            item for item in data 
            if sector.lower() in item.get('sector', '').lower()
        ]
    
    def get_top_by_market_cap(self, top_n: int = 10, use_cache: bool = True) -> List[Dict[str, Any]]:
        """Get top N companies by market capitalization"""
        data = self.read_ksa_tadawul_market(use_cache=use_cache)
        
        # Filter and sort by market cap
        valid_data = [d for d in data if d.get('market_cap', 0) > 0]
        sorted_data = sorted(valid_data, key=lambda x: x.get('market_cap', 0), reverse=True)
        
        return sorted_data[:top_n]
    
    def get_stats(self) -> Dict[str, Any]:
        """Get service statistics"""
        return {
            "total_requests": self.request_count,
            "successful_requests": self.success_count,
            "failed_requests": self.error_count,
            "success_rate": (self.success_count / max(self.request_count, 1)) * 100,
            "cache_dir": str(self.cache_dir),
            "cache_ttl": self.cache_ttl
        }
    
    def export_to_dataframe(self, sheet_key: str = "KSA_TADAWUL", 
                           use_cache: bool = True) -> Optional[pd.DataFrame]:
        """Export sheet data to pandas DataFrame"""
        data = self.read_sheet_data(sheet_key, use_cache)
        
        if not data:
            return None
        
        df = pd.DataFrame(data)
        
        # Convert numeric columns
        numeric_columns = ['last_price', 'volume', 'market_cap', 'pe', 'dividend_yield']
        for col in numeric_columns:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce')
        
        return df
    
    def clear_cache(self, sheet_key: Optional[str] = None):
        """Clear cache for specific sheet or all sheets"""
        if sheet_key:
            cache_pattern = f"{sheet_key}:*"
            for cache_file in self.cache_dir.glob(f"{cache_pattern}.json"):
                cache_file.unlink()
            logger.info(f"🧹 Cleared cache for {sheet_key}")
        else:
            for cache_file in self.cache_dir.glob("*.json"):
                cache_file.unlink()
            logger.info("🧹 Cleared all cache")
    
    def batch_read_sheets(self, sheet_keys: List[str], use_cache: bool = True) -> Dict[str, List[Dict[str, Any]]]:
        """Read multiple sheets in batch"""
        results = {}
        
        for sheet_key in sheet_keys:
            try:
                data = self.read_sheet_data(sheet_key, use_cache)
                results[sheet_key] = data
                logger.info(f"✅ Read {len(data)} rows from {sheet_key}")
            except Exception as e:
                logger.error(f"❌ Failed to read {sheet_key}: {e}")
                results[sheet_key] = []
        
        return results

# Example usage and testing
if __name__ == "__main__":
    # Initialize service
    service = EnhancedGoogleSheetsService(
        credentials_path="path/to/credentials.json",
        cache_ttl=600  # 10 minutes cache
    )
    
    # Read KSA Tadawul data with filters
    market_data = service.read_ksa_tadawul_market(
        use_cache=True,
        limit=50,
        validate=True,
        filters={
            "sector": "Banking",
            "market_cap": {"min": 1000000000},  # > 1B SAR
            "pe": {"max": 20}
        }
    )
    
    print(f"📊 Found {len(market_data)} banking stocks")
    
    # Get specific ticker details
    aramco = service.get_ticker_details("2222")
    if aramco:
        print(f"\n🏭 Aramco Details:")
        print(f"  Price: {aramco.get('last_price')}")
        print(f"  Market Cap: {aramco.get('market_cap'):,.0f}")
        print(f"  P/E: {aramco.get('pe')}")
    
    # Get top 10 by market cap
    top_10 = service.get_top_by_market_cap(top_n=10)
    print(f"\n🏆 Top 10 by Market Cap:")
    for i, company in enumerate(top_10, 1):
        print(f"  {i}. {company['ticker']} - {company['company_name']}: {company['market_cap']:,.0f}")
    
    # Export to DataFrame
    df = service.export_to_dataframe()
    if df is not None:
        print(f"\n📈 DataFrame shape: {df.shape}")
        print(f"   Columns: {list(df.columns)}")
    
    # Get service statistics
    stats = service.get_stats()
    print(f"\n📊 Service Stats:")
    for key, value in stats.items():
        print(f"  {key}: {value}")
