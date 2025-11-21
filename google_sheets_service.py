# =============================================================================
# Google Sheets Service - Comprehensive Integration
# =============================================================================

import os
import json
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any, Tuple
import asyncio

import gspread
from google.oauth2.service_account import Credentials
import pandas as pd
from pydantic import BaseModel

logger = logging.getLogger(__name__)

# =============================================================================
# Data Models
# =============================================================================

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

# =============================================================================
# Google Sheets Service
# =============================================================================

class GoogleSheetsService:
    """Comprehensive Google Sheets integration service"""
    
    def __init__(self):
        self.client = None
        self.spreadsheet = None
        self.worksheets = {}
        self.last_sync = {}
        self.initialized = False
        
    def initialize(self, credentials_json: Optional[str] = None) -> bool:
        """Initialize Google Sheets client"""
        try:
            if credentials_json is None:
                credentials_json = os.getenv('GOOGLE_SERVICE_ACCOUNT_JSON')
            
            if not credentials_json:
                logger.error("Google Sheets credentials not found")
                return False
            
            # Parse credentials JSON
            if isinstance(credentials_json, str):
                credentials_dict = json.loads(credentials_json)
            else:
                credentials_dict = credentials_json
            
            # Authenticate
            scope = [
                "https://spreadsheets.google.com/feeds",
                "https://www.googleapis.com/auth/drive"
            ]
            
            credentials = Credentials.from_service_account_info(credentials_dict, scopes=scope)
            self.client = gspread.authorize(credentials)
            
            # Open spreadsheet
            spreadsheet_id = os.getenv('SPREADSHEET_ID')
            self.spreadsheet = self.client.open_by_key(spreadsheet_id)
            
            # Cache worksheets
            self._cache_worksheets()
            
            self.initialized = True
            logger.info("✅ Google Sheets service initialized successfully")
            return True
            
        except Exception as e:
            logger.error(f"❌ Failed to initialize Google Sheets service: {e}")
            return False
    
    def _cache_worksheets(self):
        """Cache all worksheets for faster access"""
        try:
            worksheets = self.spreadsheet.worksheets()
            for worksheet in worksheets:
                self.worksheets[worksheet.title] = worksheet
            logger.info(f"✅ Cached {len(self.worksheets)} worksheets")
        except Exception as e:
            logger.error(f"❌ Failed to cache worksheets: {e}")
    
    def get_worksheet(self, sheet_name: str):
        """Get worksheet by name with caching"""
        if not self.initialized:
            if not self.initialize():
                return None
        
        if sheet_name in self.worksheets:
            return self.worksheets[sheet_name]
        
        try:
            worksheet = self.spreadsheet.worksheet(sheet_name)
            self.worksheets[sheet_name] = worksheet
            return worksheet
        except Exception as e:
            logger.error(f"❌ Worksheet '{sheet_name}' not found: {e}")
            return None
    
    # =========================================================================
    # Data Reading Methods
    # =========================================================================
    
    def read_stock_symbols(self) -> List[Dict[str, Any]]:
        """Read stock symbols from Market_Leaders sheet"""
        try:
            worksheet = self.get_worksheet("Market_Leaders")
            if not worksheet:
                return []
            
            # Get all data
            data = worksheet.get_all_records()
            symbols = []
            
            for row in data:
                if row.get('Symbol') and row.get('Symbol').strip():
                    symbols.append({
                        'symbol': row.get('Symbol', '').strip(),
                        'company_name': row.get('Company Name', ''),
                        'sector': row.get('Sector', ''),
                        'market_cap': self._safe_float(row.get('Market Cap')),
                        'weight': self._safe_float(row.get('Weight', 0)),
                        'active': True
                    })
            
            logger.info(f"📈 Read {len(symbols)} stock symbols from Google Sheets")
            return symbols
            
        except Exception as e:
            logger.error(f"❌ Failed to read stock symbols: {e}")
            return []
    
    def read_dashboard_data(self) -> Dict[str, Any]:
        """Read dashboard data from Dashboard sheet"""
        try:
            worksheet = self.get_worksheet("Dashboard")
            if not worksheet:
                return {}
            
            data = worksheet.get_all_values()
            dashboard_data = {
                'total_stocks': self._safe_int(data[1][1] if len(data) > 1 else 0),
                'market_cap': self._safe_float(data[2][1] if len(data) > 2 else 0),
                'total_volume': self._safe_float(data[3][1] if len(data) > 3 else 0),
                'advancers': self._safe_int(data[4][1] if len(data) > 4 else 0),
                'decliners': self._safe_int(data[5][1] if len(data) > 5 else 0),
                'last_updated': datetime.now()
            }
            
            return dashboard_data
            
        except Exception as e:
            logger.error(f"❌ Failed to read dashboard data: {e}")
            return {}
    
    def read_analysis_data(self) -> List[Dict[str, Any]]:
        """Read AI analysis data from AI_Analysis sheet"""
        try:
            worksheet = self.get_worksheet("AI_Analysis")
            if not worksheet:
                return []
            
            data = worksheet.get_all_records()
            analysis_data = []
            
            for row in data:
                if row.get('Symbol'):
                    analysis_data.append({
                        'symbol': row.get('Symbol', ''),
                        'sentiment': row.get('Sentiment', 'Neutral'),
                        'confidence': self._safe_float(row.get('Confidence', 0)),
                        'recommendation': row.get('Recommendation', 'Hold'),
                        'target_price': self._safe_float(row.get('Target Price')),
                        'analysis_date': datetime.now()
                    })
            
            return analysis_data
            
        except Exception as e:
            logger.error(f"❌ Failed to read analysis data: {e}")
            return []
    
    # =========================================================================
    # Data Writing Methods
    # =========================================================================
    
    def update_stock_prices(self, stock_data: List[StockData]) -> bool:
        """Update stock prices in Prices sheet"""
        try:
            worksheet = self.get_worksheet("Prices")
            if not worksheet:
                return False
            
            # Prepare data for update
            values = []
            for stock in stock_data:
                values.append([
                    stock.symbol,
                    stock.company_name,
                    stock.price,
                    stock.change,
                    stock.change_percent,
                    stock.volume,
                    stock.market_cap or '',
                    stock.timestamp.strftime('%Y-%m-%d %H:%M:%S')
                ])
            
            # Clear existing data and update
            worksheet.clear()
            header = ['Symbol', 'Company Name', 'Price', 'Change', 'Change %', 'Volume', 'Market Cap', 'Timestamp']
            worksheet.update([header] + values)
            
            logger.info(f"✅ Updated {len(stock_data)} stock prices in Google Sheets")
            return True
            
        except Exception as e:
            logger.error(f"❌ Failed to update stock prices: {e}")
            return False
    
    def update_financial_data(self, financial_data: List[FinancialData]) -> bool:
        """Update financial data in Financials sheet"""
        try:
            worksheet = self.get_worksheet("Financials")
            if not worksheet:
                return False
            
            values = []
            for data in financial_data:
                values.append([
                    data.symbol,
                    data.revenue or '',
                    data.net_income or '',
                    data.eps or '',
                    data.pe_ratio or '',
                    data.dividend_yield or '',
                    data.timestamp.strftime('%Y-%m-%d %H:%M:%S')
                ])
            
            worksheet.clear()
            header = ['Symbol', 'Revenue', 'Net Income', 'EPS', 'P/E Ratio', 'Dividend Yield', 'Timestamp']
            worksheet.update([header] + values)
            
            logger.info(f"✅ Updated {len(financial_data)} financial records in Google Sheets")
            return True
            
        except Exception as e:
            logger.error(f"❌ Failed to update financial data: {e}")
            return False
    
    def update_technical_indicators(self, indicators: List[TechnicalIndicator]) -> bool:
        """Update technical indicators in Indicators sheet"""
        try:
            worksheet = self.get_worksheet("Indicators")
            if not worksheet:
                return False
            
            values = []
            for indicator in indicators:
                values.append([
                    indicator.symbol,
                    indicator.rsi or '',
                    indicator.macd or '',
                    indicator.moving_avg_50 or '',
                    indicator.moving_avg_200 or '',
                    indicator.timestamp.strftime('%Y-%m-%d %H:%M:%S')
                ])
            
            worksheet.clear()
            header = ['Symbol', 'RSI', 'MACD', 'MA_50', 'MA_200', 'Timestamp']
            worksheet.update([header] + values)
            
            logger.info(f"✅ Updated {len(indicators)} technical indicators in Google Sheets")
            return True
            
        except Exception as e:
            logger.error(f"❌ Failed to update technical indicators: {e}")
            return False
    
    def update_ai_analysis(self, analysis_data: List[Dict[str, Any]]) -> bool:
        """Update AI analysis in AI_Analysis sheet"""
        try:
            worksheet = self.get_worksheet("AI_Analysis")
            if not worksheet:
                return False
            
            values = []
            for analysis in analysis_data:
                values.append([
                    analysis.get('symbol', ''),
                    analysis.get('sentiment', 'Neutral'),
                    analysis.get('confidence', 0),
                    analysis.get('recommendation', 'Hold'),
                    analysis.get('target_price', ''),
                    analysis.get('analysis_date', datetime.now()).strftime('%Y-%m-%d %H:%M:%S')
                ])
            
            worksheet.clear()
            header = ['Symbol', 'Sentiment', 'Confidence', 'Recommendation', 'Target Price', 'Analysis Date']
            worksheet.update([header] + values)
            
            logger.info(f"✅ Updated {len(analysis_data)} AI analysis records in Google Sheets")
            return True
            
        except Exception as e:
            logger.error(f"❌ Failed to update AI analysis: {e}")
            return False
    
    def update_dashboard(self, dashboard_data: Dict[str, Any]) -> bool:
        """Update dashboard summary data"""
        try:
            worksheet = self.get_worksheet("Dashboard")
            if not worksheet:
                return False
            
            updates = [
                ['Total Stocks', dashboard_data.get('total_stocks', 0)],
                ['Total Market Cap', dashboard_data.get('market_cap', 0)],
                ['Total Volume', dashboard_data.get('total_volume', 0)],
                ['Advancers', dashboard_data.get('advancers', 0)],
                ['Decliners', dashboard_data.get('decliners', 0)],
                ['Last Updated', datetime.now().strftime('%Y-%m-%d %H:%M:%S')]
            ]
            
            # Update cells
            for i, (label, value) in enumerate(updates):
                worksheet.update(f'B{i+2}', [[value]])
            
            logger.info("✅ Updated dashboard in Google Sheets")
            return True
            
        except Exception as e:
            logger.error(f"❌ Failed to update dashboard: {e}")
            return False
    
    # =========================================================================
    # Batch Operations
    # =========================================================================
    
    def batch_update_stocks(self, stocks_data: List[Dict[str, Any]]) -> bool:
        """Batch update multiple stock data types"""
        try:
            # Separate data by type
            stock_prices = []
            financials = []
            technicals = []
            analysis = []
            
            for stock in stocks_data:
                # Stock prices
                if 'price' in stock:
                    stock_prices.append(StockData(
                        symbol=stock['symbol'],
                        company_name=stock.get('company_name', ''),
                        price=stock['price'],
                        change=stock.get('change', 0),
                        change_percent=stock.get('change_percent', 0),
                        volume=stock.get('volume', 0),
                        market_cap=stock.get('market_cap'),
                        sector=stock.get('sector'),
                        timestamp=datetime.now()
                    ))
                
                # Financial data
                if 'revenue' in stock or 'pe_ratio' in stock:
                    financials.append(FinancialData(
                        symbol=stock['symbol'],
                        revenue=stock.get('revenue'),
                        net_income=stock.get('net_income'),
                        eps=stock.get('eps'),
                        pe_ratio=stock.get('pe_ratio'),
                        dividend_yield=stock.get('dividend_yield'),
                        timestamp=datetime.now()
                    ))
                
                # Technical indicators
                if 'rsi' in stock or 'macd' in stock:
                    technicals.append(TechnicalIndicator(
                        symbol=stock['symbol'],
                        rsi=stock.get('rsi'),
                        macd=stock.get('macd'),
                        moving_avg_50=stock.get('moving_avg_50'),
                        moving_avg_200=stock.get('moving_avg_200'),
                        timestamp=datetime.now()
                    ))
                
                # AI Analysis
                if 'sentiment' in stock:
                    analysis.append({
                        'symbol': stock['symbol'],
                        'sentiment': stock.get('sentiment', 'Neutral'),
                        'confidence': stock.get('confidence', 0),
                        'recommendation': stock.get('recommendation', 'Hold'),
                        'target_price': stock.get('target_price'),
                        'analysis_date': datetime.now()
                    })
            
            # Update all sheets
            success = True
            if stock_prices:
                success &= self.update_stock_prices(stock_prices)
            if financials:
                success &= self.update_financial_data(financials)
            if technicals:
                success &= self.update_technical_indicators(technicals)
            if analysis:
                success &= self.update_ai_analysis(analysis)
            
            return success
            
        except Exception as e:
            logger.error(f"❌ Batch update failed: {e}")
            return False
    
    # =========================================================================
    # Utility Methods
    # =========================================================================
    
    def _safe_float(self, value) -> Optional[float]:
        """Safely convert to float"""
        if value is None or value == '':
            return None
        try:
            if isinstance(value, str):
                value = value.replace(',', '').replace('"', '')
            return float(value)
        except (ValueError, TypeError):
            return None
    
    def _safe_int(self, value) -> int:
        """Safely convert to int"""
        if value is None or value == '':
            return 0
        try:
            if isinstance(value, str):
                value = value.replace(',', '').replace('"', '')
            return int(float(value))
        except (ValueError, TypeError):
            return 0
    
    def get_sheet_info(self) -> Dict[str, Any]:
        """Get information about all sheets"""
        try:
            if not self.initialized:
                return {}
            
            sheet_info = {
                'spreadsheet_title': self.spreadsheet.title,
                'spreadsheet_id': self.spreadsheet.id,
                'sheets': [],
                'last_updated': datetime.now()
            }
            
            for sheet_name, worksheet in self.worksheets.items():
                sheet_info['sheets'].append({
                    'name': sheet_name,
                    'row_count': worksheet.row_count,
                    'col_count': worksheet.col_count
                })
            
            return sheet_info
            
        except Exception as e:
            logger.error(f"❌ Failed to get sheet info: {e}")
            return {}
    
    def create_backup_sheet(self) -> bool:
        """Create a backup of current data"""
        try:
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            backup_sheet_name = f"Backup_{timestamp}"
            
            # Duplicate the entire spreadsheet
            self.spreadsheet.duplicate_sheet(
                self.spreadsheet.sheet1.id,
                new_sheet_name=backup_sheet_name
            )
            
            logger.info(f"✅ Created backup sheet: {backup_sheet_name}")
            return True
            
        except Exception as e:
            logger.error(f"❌ Failed to create backup sheet: {e}")
            return False

# =============================================================================
# Singleton Instance
# =============================================================================

google_sheets_service = GoogleSheetsService()
