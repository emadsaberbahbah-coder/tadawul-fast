#!/usr/bin/env python3
"""
TADAWUL FAST BRIDGE – ENHANCED SHEET INITIALIZER (v4.0.0)
===========================================================
PURPOSE-DRIVEN + SMART TEMPLATES + DATA-AWARE SCHEMAS + DYNAMIC LAYOUT

Core Philosophy
---------------
Each tab serves a specific purpose and should be initialized with:
- Purpose-optimized headers (not one-size-fits-all)
- Data-type appropriate formatting
- Business-logic specific columns
- Visual hierarchy matching information priority
- Auto-detected available data sources
- Dynamic column ordering based on usage patterns
- Multi-language support (English/Arabic)
- Responsive layouts for different devices
- Integration with data validation rules
- Conditional formatting based on business rules
- Protection for critical cells
- Named ranges for formulas
- Custom menus for user actions
- Charts and sparklines for visualization
- Pivot table suggestions
- Data quality indicators

Tab Types & Purposes
--------------------
📊 MARKET_LEADERS     - Top performers, momentum leaders, market movers
🇸🇦 KSA_TADAWUL       - Saudi market deep-dive with local metrics
🌍 GLOBAL_MARKETS     - International indices, ADRs, global equities
💰 MUTUAL_FUNDS       - Fund performance, NAV, expense ratios
💱 COMMODITIES_FX     - Forex pairs, commodities, futures
📋 MY_PORTFOLIO       - Position tracking, P&L, allocation management
🎯 MARKET_SCAN        - AI-screened opportunities with conviction scores
📈 INSIGHTS_ANALYSIS  - Deep research, correlations, scenario analysis
🔍 WATCHLIST          - Custom watchlist with alerts
📊 PERFORMANCE_DASH   - Portfolio performance dashboard
⚡ SCREENER_RESULTS   - Custom screening results
📝 JOURNAL            - Trading journal and notes
📊 BACKTEST_RESULTS   - Strategy backtesting results
🌙 RAMADAN_SCAN       - Special Ramadan market analysis
📈 EARNINGS_TRACKER   - Earnings calendar and estimates
💼 DIVIDEND_TRACKER   - Dividend history and projections
📊 TECHNICAL_SCAN     - Technical pattern recognition
🔧 ADMIN              - System configuration and logs

Exit Codes
----------
0: Success
1: Configuration error
2: Partial success (some tabs failed)
3: API authentication failure
4: Rate limited / quota exceeded
5: Schema validation error
6: Network/Connection error
7: Permission denied
8: Invalid spreadsheet ID
"""

from __future__ import annotations

import argparse
import base64
import hashlib
import json
import logging
import logging.config
import os
import re
import sys
import time
import uuid
import warnings
from collections import defaultdict, Counter
from dataclasses import asdict, dataclass, field, replace
from datetime import datetime, timedelta, timezone
from enum import Enum, auto
from functools import lru_cache, wraps
from pathlib import Path
from threading import Lock
from typing import (Any, Callable, Dict, List, Optional, Set, Tuple, 
                    Type, TypeVar, Union, cast, overload)
from urllib.parse import urlparse

# =============================================================================
# Path & Dependency Setup
# =============================================================================
def _ensure_project_root_on_path() -> None:
    """Ensure project root is in Python path"""
    try:
        script_dir = Path(__file__).parent.absolute()
        project_root = script_dir.parent
        
        for path in [script_dir, project_root, script_dir / 'scripts']:
            if str(path) not in sys.path:
                sys.path.insert(0, str(path))
    except Exception:
        pass

_ensure_project_root_on_path()

# =============================================================================
# Version & Core Configuration
# =============================================================================
SCRIPT_VERSION = "4.0.0"
SCRIPT_NAME = "Enhanced Sheet Initializer"
MIN_PYTHON = (3, 9)

if sys.version_info < MIN_PYTHON:
    sys.exit(f"❌ Python {MIN_PYTHON[0]}.{MIN_PYTHON[1]}+ required")

# =============================================================================
# Optional Dependencies with Graceful Degradation
# =============================================================================
# Google Sheets
try:
    import gspread
    from google.oauth2.service_account import Credentials
    from google.auth.exceptions import GoogleAuthError, RefreshError
    from googleapiclient.discovery import build
    from googleapiclient.errors import HttpError
    GSHEET_AVAILABLE = True
except ImportError:
    gspread = None
    GSHEET_AVAILABLE = False

# Data Processing
try:
    import pandas as pd
    PANDAS_AVAILABLE = True
except ImportError:
    pd = None
    PANDAS_AVAILABLE = False

try:
    import numpy as np
    NUMPY_AVAILABLE = True
except ImportError:
    np = None
    NUMPY_AVAILABLE = False

# Schema validation
try:
    import jsonschema
    from jsonschema import validate, ValidationError
    JSONSCHEMA_AVAILABLE = True
except ImportError:
    jsonschema = None
    JSONSCHEMA_AVAILABLE = False

# YAML for configs
try:
    import yaml
    YAML_AVAILABLE = True
except ImportError:
    yaml = None
    YAML_AVAILABLE = False

# Core imports
settings = None
sheets = None
schemas = None

def _strict_mode() -> bool:
    return os.getenv("SHEET_SETUP_STRICT", "").lower() in {"1", "true", "yes", "on"}

try:
    from env import settings as _settings
    settings = _settings
except ImportError:
    pass

try:
    import google_sheets_service as _sheets
    sheets = _sheets
except ImportError as e:
    if _strict_mode():
        print(f"❌ STRICT: Could not import google_sheets_service: {e}")
        sys.exit(1)
    sheets = None

try:
    from core.schemas import get_schema_for_tab, get_field_definitions
    schemas = True
except ImportError:
    schemas = None

# =============================================================================
# Logging Configuration
# =============================================================================
LOG_FORMAT = "%(asctime)s | %(levelname)8s | %(name)s | %(message)s"
DATE_FORMAT = "%Y-%m-%d %H:%M:%S"

logging.basicConfig(level=logging.INFO, format=LOG_FORMAT, datefmt=DATE_FORMAT)
logger = logging.getLogger("SheetSetup")

# =============================================================================
# Enums & Data Models
# =============================================================================
class TabType(Enum):
    """Tab purpose classification"""
    MARKET_LEADERS = "market_leaders"
    KSA_TADAWUL = "ksa_tadawul"
    GLOBAL_MARKETS = "global_markets"
    MUTUAL_FUNDS = "mutual_funds"
    COMMODITIES_FX = "commodities_fx"
    PORTFOLIO = "portfolio"
    MARKET_SCAN = "market_scan"
    INSIGHTS = "insights"
    WATCHLIST = "watchlist"
    PERFORMANCE_DASH = "performance_dash"
    SCREENER = "screener"
    JOURNAL = "journal"
    BACKTEST = "backtest"
    RAMADAN_SCAN = "ramadan_scan"
    EARNINGS_TRACKER = "earnings_tracker"
    DIVIDEND_TRACKER = "dividend_tracker"
    TECHNICAL_SCAN = "technical_scan"
    ADMIN = "admin"
    CUSTOM = "custom"
    
    @classmethod
    def from_string(cls, s: str) -> TabType:
        """Create from string with fallback"""
        try:
            return cls(s.lower())
        except ValueError:
            return cls.CUSTOM
    
    @property
    def icon(self) -> str:
        """Get tab icon"""
        icons = {
            TabType.MARKET_LEADERS: "📊",
            TabType.KSA_TADAWUL: "🇸🇦",
            TabType.GLOBAL_MARKETS: "🌍",
            TabType.MUTUAL_FUNDS: "💰",
            TabType.COMMODITIES_FX: "💱",
            TabType.PORTFOLIO: "📋",
            TabType.MARKET_SCAN: "🎯",
            TabType.INSIGHTS: "📈",
            TabType.WATCHLIST: "🔍",
            TabType.PERFORMANCE_DASH: "📊",
            TabType.SCREENER: "⚡",
            TabType.JOURNAL: "📝",
            TabType.BACKTEST: "📊",
            TabType.RAMADAN_SCAN: "🌙",
            TabType.EARNINGS_TRACKER: "📈",
            TabType.DIVIDEND_TRACKER: "💼",
            TabType.TECHNICAL_SCAN: "📊",
            TabType.ADMIN: "🔧",
        }
        return icons.get(self, "📄")

class DataCategory(Enum):
    """Data type classification"""
    METADATA = "metadata"          # Symbol, name, origin
    MARKET = "market"              # Price, volume, market data
    TECHNICAL = "technical"        # RSI, MA, volatility
    FUNDAMENTAL = "fundamental"    # PE, PB, financial ratios
    FORECAST = "forecast"          # Predictions, ROI, confidence
    PERFORMANCE = "performance"    # Returns, P&L, drawdown
    RISK = "risk"                  # Risk scores, VaR, beta
    PORTFOLIO = "portfolio"        # Position data, allocation
    TIMESTAMP = "timestamp"        # Update times
    ERROR = "error"                # Error tracking
    NOTE = "note"                  # User notes
    ALERT = "alert"                # Alert settings
    CONFIG = "config"              # Configuration
    
    @property
    def color(self) -> str:
        """Get category color for headers"""
        colors = {
            DataCategory.METADATA: "#2C3E50",
            DataCategory.MARKET: "#27AE60",
            DataCategory.TECHNICAL: "#3498DB",
            DataCategory.FUNDAMENTAL: "#9B59B6",
            DataCategory.FORECAST: "#E67E22",
            DataCategory.PERFORMANCE: "#16A085",
            DataCategory.RISK: "#E74C3C",
            DataCategory.PORTFOLIO: "#F39C12",
            DataCategory.TIMESTAMP: "#7F8C8D",
            DataCategory.ERROR: "#C0392B",
        }
        return colors.get(self, "#34495E")

class FieldType(Enum):
    """Field data type for formatting"""
    STRING = "string"
    NUMBER = "number"
    PERCENT = "percent"
    CURRENCY = "currency"
    DATE = "date"
    DATETIME = "datetime"
    TIME = "time"
    BOOLEAN = "boolean"
    SCORE = "score"                # 0-100 scale
    RATING = "rating"              # Text rating (Buy/Sell)
    ENUM = "enum"                  # Predefined choices
    FORMULA = "formula"            # Google Sheets formula
    HYPERLINK = "hyperlink"        # Clickable link
    IMAGE = "image"                # Image URL
    SPARKLINE = "sparkline"        # Inline chart
    
    @property
    def default_width(self) -> int:
        """Default column width in pixels"""
        widths = {
            FieldType.STRING: 200,
            FieldType.NUMBER: 120,
            FieldType.PERCENT: 100,
            FieldType.CURRENCY: 120,
            FieldType.DATE: 120,
            FieldType.DATETIME: 180,
            FieldType.TIME: 100,
            FieldType.BOOLEAN: 80,
            FieldType.SCORE: 100,
            FieldType.RATING: 100,
            FieldType.ENUM: 150,
            FieldType.FORMULA: 150,
            FieldType.HYPERLINK: 200,
            FieldType.IMAGE: 150,
            FieldType.SPARKLINE: 120,
        }
        return widths.get(self, 140)

class Alignment(Enum):
    """Text alignment"""
    LEFT = "LEFT"
    CENTER = "CENTER"
    RIGHT = "RIGHT"
    
    @classmethod
    def for_type(cls, field_type: FieldType) -> Alignment:
        """Get default alignment for field type"""
        alignments = {
            FieldType.NUMBER: Alignment.RIGHT,
            FieldType.PERCENT: Alignment.RIGHT,
            FieldType.CURRENCY: Alignment.RIGHT,
            FieldType.DATE: Alignment.CENTER,
            FieldType.DATETIME: Alignment.CENTER,
            FieldType.TIME: Alignment.CENTER,
            FieldType.SCORE: Alignment.CENTER,
            FieldType.RATING: Alignment.CENTER,
            FieldType.BOOLEAN: Alignment.CENTER,
            FieldType.STRING: Alignment.LEFT,
            FieldType.ENUM: Alignment.LEFT,
            FieldType.FORMULA: Alignment.LEFT,
            FieldType.HYPERLINK: Alignment.LEFT,
        }
        return alignments.get(field_type, Alignment.LEFT)

class ValidationType(Enum):
    """Data validation types"""
    NONE = "none"
    NUMBER = "number"
    DATE = "date"
    LIST = "list"
    CHECKBOX = "checkbox"
    CUSTOM = "custom"

@dataclass
class ValidationRule:
    """Data validation rule"""
    type: ValidationType
    condition: Optional[str] = None
    values: List[Any] = field(default_factory=list)
    min_value: Optional[float] = None
    max_value: Optional[float] = None
    min_date: Optional[str] = None
    max_date: Optional[str] = None
    formula: Optional[str] = None
    strict: bool = True
    show_dropdown: bool = True
    help_text: Optional[str] = None
    
    def to_api_format(self) -> Dict[str, Any]:
        """Convert to Google Sheets API format"""
        condition = {}
        
        if self.type == ValidationType.NUMBER:
            condition = {
                "type": "NUMBER_BETWEEN",
                "values": [
                    {"userEnteredValue": str(self.min_value or 0)},
                    {"userEnteredValue": str(self.max_value or 100)}
                ]
            }
        elif self.type == ValidationType.DATE:
            condition = {
                "type": "DATE_IS_VALID"
            }
        elif self.type == ValidationType.LIST and self.values:
            condition = {
                "type": "ONE_OF_LIST",
                "values": [{"userEnteredValue": str(v)} for v in self.values]
            }
        elif self.type == ValidationType.CHECKBOX:
            condition = {
                "type": "BOOLEAN"
            }
        elif self.type == ValidationType.CUSTOM and self.formula:
            condition = {
                "type": "CUSTOM_FORMULA",
                "values": [{"userEnteredValue": self.formula}]
            }
        
        return {
            "condition": condition,
            "inputMessage": self.help_text or "",
            "strict": self.strict,
            "showCustomUi": self.show_dropdown
        }

@dataclass
class FieldDefinition:
    """Definition of a sheet field"""
    # Core
    name: str
    display_name: str
    category: DataCategory
    field_type: FieldType
    
    # Metadata
    description: str = ""
    required: bool = False
    example: Any = None
    
    # Formatting
    width: int = 140
    alignment: Alignment = Alignment.LEFT
    number_format: Optional[str] = None
    visible: bool = True
    hidden_formula: bool = False
    
    # Positioning
    priority: int = 10  # Lower = higher priority (left side)
    group: Optional[str] = None
    group_order: int = 0
    
    # Validation
    validation: Optional[ValidationRule] = None
    formula: Optional[str] = None
    default_value: Optional[Any] = None
    
    # Conditional formatting
    conditional_formats: List[Dict[str, Any]] = field(default_factory=list)
    
    # Dependencies
    depends_on: List[str] = field(default_factory=list)
    
    # Multi-language support
    display_name_ar: Optional[str] = None
    description_ar: Optional[str] = None
    
    def __post_init__(self):
        if self.width == 140:  # Default
            self.width = self.field_type.default_width
        if self.alignment == Alignment.LEFT:  # Default
            self.alignment = Alignment.for_type(self.field_type)
    
    def to_dict(self, include_arabic: bool = False) -> Dict[str, Any]:
        """Convert to dictionary"""
        result = {
            'name': self.name,
            'display_name': self.display_name_ar if include_arabic and self.display_name_ar else self.display_name,
            'category': self.category.value,
            'field_type': self.field_type.value,
            'width': self.width,
            'alignment': self.alignment.value,
            'visible': self.visible,
            'priority': self.priority,
            'group': self.group,
        }
        
        if include_arabic and self.description_ar:
            result['description'] = self.description_ar
        elif self.description:
            result['description'] = self.description
            
        return result

@dataclass
class TabSchema:
    """Complete tab schema definition"""
    # Core
    tab_type: TabType
    name: str
    description: str
    fields: List[FieldDefinition]
    
    # Layout
    header_row: int = 5
    frozen_rows: int = 5
    frozen_cols: int = 2
    grid_rows: int = 1000
    grid_cols: int = 50
    
    # Defaults
    default_sort: Optional[str] = None
    default_view: str = "grid"  # grid, form, chart
    
    # Styling
    conditional_formats: List[Dict[str, Any]] = field(default_factory=list)
    data_validation: List[Dict[str, Any]] = field(default_factory=list)
    protected_ranges: List[Dict[str, Any]] = field(default_factory=list)
    named_ranges: List[Dict[str, str]] = field(default_factory=list)
    
    # Charts
    charts: List[Dict[str, Any]] = field(default_factory=list)
    
    # Automation
    triggers: List[Dict[str, Any]] = field(default_factory=list)
    custom_menu: List[Dict[str, str]] = field(default_factory=list)
    
    # Multi-language
    name_ar: Optional[str] = None
    description_ar: Optional[str] = None
    
    @property
    def visible_fields(self) -> List[FieldDefinition]:
        """Get visible fields sorted by priority"""
        return sorted(
            [f for f in self.fields if f.visible],
            key=lambda f: (f.group_order, f.priority)
        )
    
    @property
    def field_names(self) -> List[str]:
        """Get field display names"""
        return [f.display_name for f in self.visible_fields]
    
    @property
    def field_names_ar(self) -> List[str]:
        """Get Arabic field names"""
        return [f.display_name_ar or f.display_name for f in self.visible_fields]
    
    def get_field_by_name(self, name: str) -> Optional[FieldDefinition]:
        """Get field by name or display name"""
        for f in self.fields:
            if f.name == name or f.display_name == name:
                return f
        return None
    
    def get_field_by_path(self, path: str) -> Optional[FieldDefinition]:
        """Get field by dot-notation path (e.g., 'metadata.symbol')"""
        parts = path.split('.')
        if len(parts) == 2:
            category, name = parts
            for f in self.fields:
                if f.category.value == category and (f.name == name or f.display_name == name):
                    return f
        return None

# =============================================================================
# Tab Schema Definitions (Purpose-Driven)
# =============================================================================
class TabSchemas:
    """Repository of all tab schemas with comprehensive definitions"""
    
    # Common field definitions for reuse
    _COMMON_FIELDS = {
        # Metadata
        "rank": FieldDefinition(
            name="rank", display_name="#", category=DataCategory.METADATA,
            field_type=FieldType.NUMBER, width=50, priority=1,
            description="Rank based on primary sort criterion"
        ),
        "symbol": FieldDefinition(
            name="symbol", display_name="Symbol", category=DataCategory.METADATA,
            field_type=FieldType.STRING, width=100, priority=2, required=True,
            description="Ticker symbol or instrument identifier",
            validation=ValidationRule(type=ValidationType.CUSTOM, formula='=REGEXMATCH(A5, "^[A-Z0-9\\.\\-]+$")')
        ),
        "name": FieldDefinition(
            name="name", display_name="Company Name", category=DataCategory.METADATA,
            field_type=FieldType.STRING, width=250, priority=3,
            description_ar="اسم الشركة"
        ),
        "isin": FieldDefinition(
            name="isin", display_name="ISIN", category=DataCategory.METADATA,
            field_type=FieldType.STRING, width=150, priority=5,
            description="International Securities Identification Number",
            validation=ValidationRule(type=ValidationType.CUSTOM, formula='=REGEXMATCH(B5, "^[A-Z]{2}[A-Z0-9]{9}[0-9]$")')
        ),
        "exchange": FieldDefinition(
            name="exchange", display_name="Exchange", category=DataCategory.METADATA,
            field_type=FieldType.STRING, width=120, priority=4,
            description="Primary stock exchange"
        ),
        "sector": FieldDefinition(
            name="sector", display_name="Sector", category=DataCategory.METADATA,
            field_type=FieldType.ENUM, width=150, priority=6,
            validation=ValidationRule(
                type=ValidationType.LIST,
                values=["Technology", "Healthcare", "Financial", "Energy", "Consumer", "Industrial", "Utilities", "Real Estate", "Materials", "Communication"]
            )
        ),
        "industry": FieldDefinition(
            name="industry", display_name="Industry", category=DataCategory.METADATA,
            field_type=FieldType.STRING, width=180, priority=7
        ),
        "country": FieldDefinition(
            name="country", display_name="Country", category=DataCategory.METADATA,
            field_type=FieldType.ENUM, width=100, priority=8,
            validation=ValidationRule(
                type=ValidationType.LIST,
                values=["USA", "Saudi Arabia", "UAE", "Kuwait", "Qatar", "Bahrain", "Oman", "Egypt", "Jordan", "UK", "Germany", "France", "Japan", "China", "India"]
            )
        ),
        "currency": FieldDefinition(
            name="currency", display_name="Currency", category=DataCategory.METADATA,
            field_type=FieldType.ENUM, width=80, priority=9,
            validation=ValidationRule(
                type=ValidationType.LIST,
                values=["SAR", "USD", "EUR", "GBP", "JPY", "CNY", "AED", "KWD", "QAR"]
            )
        ),
        
        # Market data
        "price": FieldDefinition(
            name="price", display_name="Price", category=DataCategory.MARKET,
            field_type=FieldType.CURRENCY, width=100, priority=10,
            description="Current market price"
        ),
        "change": FieldDefinition(
            name="change", display_name="Change", category=DataCategory.MARKET,
            field_type=FieldType.CURRENCY, width=90, priority=11,
            description="Absolute price change",
            conditional_formats=[
                {"type": "positive", "color": "#00A86B"},
                {"type": "negative", "color": "#D32F2F"}
            ]
        ),
        "change_pct": FieldDefinition(
            name="change_pct", display_name="Change %", category=DataCategory.PERFORMANCE,
            field_type=FieldType.PERCENT, width=90, priority=12,
            description="Percentage price change",
            conditional_formats=[
                {"type": "positive", "color": "#00A86B"},
                {"type": "negative", "color": "#D32F2F"}
            ]
        ),
        "volume": FieldDefinition(
            name="volume", display_name="Volume", category=DataCategory.MARKET,
            field_type=FieldType.NUMBER, width=100, priority=13,
            number_format="#,##0"
        ),
        "avg_volume": FieldDefinition(
            name="avg_volume", display_name="Avg Volume", category=DataCategory.MARKET,
            field_type=FieldType.NUMBER, width=110, priority=14,
            description="Average daily volume",
            number_format="#,##0"
        ),
        "market_cap": FieldDefinition(
            name="market_cap", display_name="Market Cap", category=DataCategory.FUNDAMENTAL,
            field_type=FieldType.NUMBER, width=120, priority=15,
            description="Market capitalization",
            number_format="#,##0.0,,,B"
        ),
        
        # Technical
        "rsi": FieldDefinition(
            name="rsi", display_name="RSI (14)", category=DataCategory.TECHNICAL,
            field_type=FieldType.SCORE, width=90, priority=20,
            description="Relative Strength Index",
            validation=ValidationRule(type=ValidationType.NUMBER, min_value=0, max_value=100),
            conditional_formats=[
                {"range": ">70", "color": "#D32F2F", "message": "Overbought"},
                {"range": "<30", "color": "#00A86B", "message": "Oversold"}
            ]
        ),
        "macd": FieldDefinition(
            name="macd", display_name="MACD", category=DataCategory.TECHNICAL,
            field_type=FieldType.NUMBER, width=100, priority=21,
            description="Moving Average Convergence Divergence"
        ),
        "macd_signal": FieldDefinition(
            name="macd_signal", display_name="MACD Signal", category=DataCategory.TECHNICAL,
            field_type=FieldType.NUMBER, width=110, priority=22
        ),
        "bb_position": FieldDefinition(
            name="bb_position", display_name="BB %", category=DataCategory.TECHNICAL,
            field_type=FieldType.PERCENT, width=90, priority=23,
            description="Bollinger Band position (0-100%)"
        ),
        "ma_50": FieldDefinition(
            name="ma_50", display_name="MA (50)", category=DataCategory.TECHNICAL,
            field_type=FieldType.CURRENCY, width=100, priority=24,
            description="50-day moving average"
        ),
        "ma_200": FieldDefinition(
            name="ma_200", display_name="MA (200)", category=DataCategory.TECHNICAL,
            field_type=FieldType.CURRENCY, width=100, priority=25,
            description="200-day moving average"
        ),
        "volatility": FieldDefinition(
            name="volatility", display_name="Volatility", category=DataCategory.RISK,
            field_type=FieldType.PERCENT, width=100, priority=26,
            description="30-day historical volatility"
        ),
        
        # Fundamentals
        "pe": FieldDefinition(
            name="pe", display_name="P/E", category=DataCategory.FUNDAMENTAL,
            field_type=FieldType.NUMBER, width=90, priority=30,
            description="Price to Earnings ratio"
        ),
        "pb": FieldDefinition(
            name="pb", display_name="P/B", category=DataCategory.FUNDAMENTAL,
            field_type=FieldType.NUMBER, width=90, priority=31,
            description="Price to Book ratio"
        ),
        "ps": FieldDefinition(
            name="ps", display_name="P/S", category=DataCategory.FUNDAMENTAL,
            field_type=FieldType.NUMBER, width=90, priority=32,
            description="Price to Sales ratio"
        ),
        "pcf": FieldDefinition(
            name="pcf", display_name="P/CF", category=DataCategory.FUNDAMENTAL,
            field_type=FieldType.NUMBER, width=100, priority=33,
            description="Price to Cash Flow ratio"
        ),
        "div_yield": FieldDefinition(
            name="div_yield", display_name="Div Yield %", category=DataCategory.FUNDAMENTAL,
            field_type=FieldType.PERCENT, width=100, priority=34,
            description="Dividend yield"
        ),
        "eps": FieldDefinition(
            name="eps", display_name="EPS", category=DataCategory.FUNDAMENTAL,
            field_type=FieldType.CURRENCY, width=100, priority=35,
            description="Earnings Per Share"
        ),
        "roe": FieldDefinition(
            name="roe", display_name="ROE %", category=DataCategory.FUNDAMENTAL,
            field_type=FieldType.PERCENT, width=90, priority=36,
            description="Return on Equity"
        ),
        "debt_equity": FieldDefinition(
            name="debt_equity", display_name="D/E", category=DataCategory.FUNDAMENTAL,
            field_type=FieldType.NUMBER, width=80, priority=37,
            description="Debt to Equity ratio"
        ),
        
        # Forecast
        "fair_value": FieldDefinition(
            name="fair_value", display_name="Fair Value", category=DataCategory.FORECAST,
            field_type=FieldType.CURRENCY, width=110, priority=40,
            description="Estimated fair value"
        ),
        "upside": FieldDefinition(
            name="upside", display_name="Upside %", category=DataCategory.FORECAST,
            field_type=FieldType.PERCENT, width=90, priority=41,
            conditional_formats=[
                {"range": ">20", "color": "#00A86B"},
                {"range": ">10", "color": "#8BC34A"},
                {"range": "<-10", "color": "#D32F2F"}
            ]
        ),
        "target_price": FieldDefinition(
            name="target_price", display_name="Target Price", category=DataCategory.FORECAST,
            field_type=FieldType.CURRENCY, width=110, priority=42,
            description="Analyst target price"
        ),
        "analyst_rating": FieldDefinition(
            name="analyst_rating", display_name="Analyst Rating", category=DataCategory.FORECAST,
            field_type=FieldType.RATING, width=120, priority=43,
            validation=ValidationRule(
                type=ValidationType.LIST,
                values=["Strong Buy", "Buy", "Hold", "Sell", "Strong Sell"]
            )
        ),
        "roi_1m": FieldDefinition(
            name="roi_1m", display_name="ROI 1M %", category=DataCategory.FORECAST,
            field_type=FieldType.PERCENT, width=90, priority=44
        ),
        "roi_3m": FieldDefinition(
            name="roi_3m", display_name="ROI 3M %", category=DataCategory.FORECAST,
            field_type=FieldType.PERCENT, width=90, priority=45
        ),
        "roi_12m": FieldDefinition(
            name="roi_12m", display_name="ROI 12M %", category=DataCategory.FORECAST,
            field_type=FieldType.PERCENT, width=90, priority=46
        ),
        
        # Risk
        "beta": FieldDefinition(
            name="beta", display_name="Beta", category=DataCategory.RISK,
            field_type=FieldType.NUMBER, width=80, priority=50,
            description="Systematic risk measure"
        ),
        "sharpe": FieldDefinition(
            name="sharpe", display_name="Sharpe", category=DataCategory.RISK,
            field_type=FieldType.NUMBER, width=90, priority=51,
            description="Sharpe ratio"
        ),
        "var_95": FieldDefinition(
            name="var_95", display_name="VaR 95%", category=DataCategory.RISK,
            field_type=FieldType.PERCENT, width=90, priority=52,
            description="Value at Risk (95% confidence)"
        ),
        "max_drawdown": FieldDefinition(
            name="max_drawdown", display_name="Max DD", category=DataCategory.RISK,
            field_type=FieldType.PERCENT, width=90, priority=53,
            description="Maximum drawdown"
        ),
        "risk_level": FieldDefinition(
            name="risk_level", display_name="Risk Level", category=DataCategory.RISK,
            field_type=FieldType.RATING, width=100, priority=54,
            validation=ValidationRule(
                type=ValidationType.LIST,
                values=["Very Low", "Low", "Moderate", "High", "Very High"]
            )
        ),
        
        # Portfolio
        "quantity": FieldDefinition(
            name="quantity", display_name="Quantity", category=DataCategory.PORTFOLIO,
            field_type=FieldType.NUMBER, width=100, priority=60,
            number_format="#,##0",
            validation=ValidationRule(type=ValidationType.NUMBER, min_value=0)
        ),
        "avg_cost": FieldDefinition(
            name="avg_cost", display_name="Avg Cost", category=DataCategory.PORTFOLIO,
            field_type=FieldType.CURRENCY, width=100, priority=61,
            validation=ValidationRule(type=ValidationType.NUMBER, min_value=0)
        ),
        "cost_basis": FieldDefinition(
            name="cost_basis", display_name="Cost Basis", category=DataCategory.PORTFOLIO,
            field_type=FieldType.CURRENCY, width=100, priority=62,
            formula="=quantity * avg_cost"
        ),
        "market_value": FieldDefinition(
            name="market_value", display_name="Market Value", category=DataCategory.PORTFOLIO,
            field_type=FieldType.CURRENCY, width=110, priority=63,
            formula="=quantity * price"
        ),
        "unrealized_pl": FieldDefinition(
            name="unrealized_pl", display_name="Unrealized P&L", category=DataCategory.PERFORMANCE,
            field_type=FieldType.CURRENCY, width=120, priority=64,
            formula="=market_value - cost_basis"
        ),
        "weight": FieldDefinition(
            name="weight", display_name="Weight %", category=DataCategory.PORTFOLIO,
            field_type=FieldType.PERCENT, width=90, priority=65,
            formula="=market_value / SUM($market_value$)"
        ),
        
        # Timestamps
        "last_updated": FieldDefinition(
            name="last_updated", display_name="Last Updated", category=DataCategory.TIMESTAMP,
            field_type=FieldType.DATETIME, width=180, priority=100,
            description="Last update timestamp (Riyadh time)",
            formula="=NOW()",
            hidden_formula=True
        ),
        "last_updated_utc": FieldDefinition(
            name="last_updated_utc", display_name="Last Updated (UTC)", category=DataCategory.TIMESTAMP,
            field_type=FieldType.DATETIME, width=180, priority=101,
            hidden_formula=True
        ),
        
        # Error tracking
        "error": FieldDefinition(
            name="error", display_name="Error", category=DataCategory.ERROR,
            field_type=FieldType.STRING, width=250, priority=1000,
            visible=False
        ),
    }
    
    @classmethod
    def market_leaders(cls) -> TabSchema:
        """Market Leaders tab - Top performers, momentum"""
        fields = [
            cls._COMMON_FIELDS["rank"],
            cls._COMMON_FIELDS["symbol"],
            cls._COMMON_FIELDS["name"],
            cls._COMMON_FIELDS["sector"],
            cls._COMMON_FIELDS["exchange"],
            
            # Market data
            cls._COMMON_FIELDS["price"],
            FieldDefinition(
                name="change_pct", display_name="Change %", category=DataCategory.PERFORMANCE,
                field_type=FieldType.PERCENT, width=90, priority=11,
                conditional_formats=[
                    {"type": "positive", "color": "#00A86B"},
                    {"type": "negative", "color": "#D32F2F"}
                ]
            ),
            cls._COMMON_FIELDS["volume"],
            FieldDefinition(
                name="rel_volume", display_name="Rel Volume", category=DataCategory.TECHNICAL,
                field_type=FieldType.NUMBER, width=90, priority=13,
                description="Volume / Average Volume"
            ),
            
            # Momentum
            FieldDefinition(
                name="momentum_1m", display_name="Momentum 1M", category=DataCategory.PERFORMANCE,
                field_type=FieldType.PERCENT, width=100, priority=20
            ),
            FieldDefinition(
                name="momentum_3m", display_name="Momentum 3M", category=DataCategory.PERFORMANCE,
                field_type=FieldType.PERCENT, width=100, priority=21
            ),
            FieldDefinition(
                name="momentum_12m", display_name="Momentum 12M", category=DataCategory.PERFORMANCE,
                field_type=FieldType.PERCENT, width=100, priority=22
            ),
            
            # Technical
            cls._COMMON_FIELDS["rsi"],
            FieldDefinition(
                name="ma_50_pct", display_name="Above MA50 %", category=DataCategory.TECHNICAL,
                field_type=FieldType.PERCENT, width=110, priority=24,
                description="% above 50-day MA"
            ),
            FieldDefinition(
                name="ma_200_pct", display_name="Above MA200 %", category=DataCategory.TECHNICAL,
                field_type=FieldType.PERCENT, width=110, priority=25
            ),
            
            # Position
            FieldDefinition(
                name="week_52_high", display_name="52W High", category=DataCategory.MARKET,
                field_type=FieldType.CURRENCY, width=100, priority=30
            ),
            FieldDefinition(
                name="week_52_low", display_name="52W Low", category=DataCategory.MARKET,
                field_type=FieldType.CURRENCY, width=100, priority=31
            ),
            FieldDefinition(
                name="week_52_position", display_name="52W Position %", category=DataCategory.TECHNICAL,
                field_type=FieldType.PERCENT, width=120, priority=32,
                description="Position in 52-week range"
            ),
            
            # Scores
            FieldDefinition(
                name="leadership_score", display_name="Leadership Score", category=DataCategory.PERFORMANCE,
                field_type=FieldType.SCORE, width=130, priority=40,
                description="Composite score of market leadership"
            ),
            FieldDefinition(
                name="momentum_score", display_name="Momentum Score", category=DataCategory.PERFORMANCE,
                field_type=FieldType.SCORE, width=130, priority=41
            ),
            
            cls._COMMON_FIELDS["last_updated"],
            cls._COMMON_FIELDS["error"],
        ]
        
        return TabSchema(
            tab_type=TabType.MARKET_LEADERS,
            name="Market_Leaders",
            name_ar="قادة_السوق",
            description="Top market performers and momentum leaders",
            description_ar="أفضل أداء السوق وقادة الزخم",
            fields=fields,
            default_sort="leadership_score DESC",
            frozen_cols=2,
            conditional_formats=[
                {
                    "range": "C:C",  # Change % column
                    "type": "NUMBER",
                    "condition": "NUMBER_GREATER_THAN_EQ",
                    "value": 0,
                    "format": {"textFormat": {"foregroundColor": {"red": 0, "green": 0.6, "blue": 0}}}
                },
                {
                    "range": "C:C",
                    "type": "NUMBER",
                    "condition": "NUMBER_LESS_THAN",
                    "value": 0,
                    "format": {"textFormat": {"foregroundColor": {"red": 0.8, "green": 0, "blue": 0}}}
                },
                {
                    "range": "K:K",  # RSI column
                    "type": "NUMBER",
                    "condition": "NUMBER_GREATER_THAN",
                    "value": 70,
                    "format": {"backgroundColor": {"red": 1, "green": 0.8, "blue": 0.8}}
                },
                {
                    "range": "K:K",
                    "type": "NUMBER",
                    "condition": "NUMBER_LESS_THAN",
                    "value": 30,
                    "format": {"backgroundColor": {"red": 0.8, "green": 1, "blue": 0.8}}
                }
            ],
            protected_ranges=[
                {
                    "range": "1:4",  # Header area
                    "description": "Header Protection",
                    "warningOnly": True
                }
            ],
            named_ranges=[
                {"name": "MarketLeaders_Data", "range": "A5:Z"}
            ]
        )
    
    @classmethod
    def ksa_tadawul(cls) -> TabSchema:
        """KSA Tadawul tab - Saudi market specific"""
        fields = [
            cls._COMMON_FIELDS["rank"],
            cls._COMMON_FIELDS["symbol"],
            FieldDefinition(
                name="company_ar", display_name="الشركة", category=DataCategory.METADATA,
                field_type=FieldType.STRING, width=250, priority=3,
                display_name_ar="الشركة"
            ),
            cls._COMMON_FIELDS["name"],
            cls._COMMON_FIELDS["sector"],
            FieldDefinition(
                name="sector_ar", display_name="القطاع", category=DataCategory.METADATA,
                field_type=FieldType.STRING, width=150, priority=5,
                visible=False  # Hidden but available for Arabic display
            ),
            
            # Saudi market specific
            cls._COMMON_FIELDS["isin"],
            FieldDefinition(
                name="tadawul_code", display_name="Tadawul Code", category=DataCategory.METADATA,
                field_type=FieldType.STRING, width=120, priority=7,
                description="Saudi stock code"
            ),
            
            # Market data
            FieldDefinition(
                name="price_sar", display_name="Price (SAR)", category=DataCategory.MARKET,
                field_type=FieldType.CURRENCY, width=110, priority=10
            ),
            cls._COMMON_FIELDS["change"],
            cls._COMMON_FIELDS["change_pct"],
            cls._COMMON_FIELDS["volume"],
            FieldDefinition(
                name="value_traded", display_name="Value Traded (M)", category=DataCategory.MARKET,
                field_type=FieldType.NUMBER, width=130, priority=14,
                number_format="#,##0.0,,M"
            ),
            FieldDefinition(
                name="market_cap_sar", display_name="Market Cap (B)", category=DataCategory.FUNDAMENTAL,
                field_type=FieldType.NUMBER, width=130, priority=15,
                number_format="#,##0.0,,,B"
            ),
            
            # Technical
            cls._COMMON_FIELDS["rsi"],
            cls._COMMON_FIELDS["ma_50"],
            cls._COMMON_FIELDS["ma_200"],
            FieldDefinition(
                name="volume_ma", display_name="Volume MA", category=DataCategory.TECHNICAL,
                field_type=FieldType.NUMBER, width=100, priority=24
            ),
            
            # Fundamentals
            cls._COMMON_FIELDS["pe"],
            cls._COMMON_FIELDS["pb"],
            cls._COMMON_FIELDS["div_yield"],
            cls._COMMON_FIELDS["eps"],
            
            # Saudi-specific metrics
            FieldDefinition(
                name="foreign_ownership", display_name="Foreign Own %", category=DataCategory.FUNDAMENTAL,
                field_type=FieldType.PERCENT, width=120, priority=40,
                description="Foreign ownership percentage"
            ),
            FieldDefinition(
                name="institutional_ownership", display_name="Inst Own %", category=DataCategory.FUNDAMENTAL,
                field_type=FieldType.PERCENT, width=120, priority=41
            ),
            FieldDefinition(
                name="free_float", display_name="Free Float %", category=DataCategory.FUNDAMENTAL,
                field_type=FieldType.PERCENT, width=110, priority=42
            ),
            
            # Risk
            FieldDefinition(
                name="saudi_risk", display_name="Saudi Risk", category=DataCategory.RISK,
                field_type=FieldType.SCORE, width=110, priority=50,
                description="Country-specific risk score"
            ),
            cls._COMMON_FIELDS["beta"],
            
            # Forecast
            cls._COMMON_FIELDS["fair_value"],
            cls._COMMON_FIELDS["upside"],
            cls._COMMON_FIELDS["analyst_rating"],
            
            # Timestamps - bilingual
            FieldDefinition(
                name="last_updated_riyadh", display_name="Last Updated (Riyadh)", 
                display_name_ar="آخر تحديث (الرياض)",
                category=DataCategory.TIMESTAMP, field_type=FieldType.DATETIME,
                width=180, priority=100
            ),
        ]
        
        return TabSchema(
            tab_type=TabType.KSA_TADAWUL,
            name="KSA_Tadawul",
            name_ar="السعودي_تداول",
            description="Saudi Stock Exchange detailed analysis",
            description_ar="تحليل مفصل للسوق السعودي",
            fields=fields,
            frozen_cols=2,
            default_sort="market_cap_sar DESC",
        )
    
    @classmethod
    def global_markets(cls) -> TabSchema:
        """Global Markets tab - International exposure"""
        fields = [
            cls._COMMON_FIELDS["rank"],
            cls._COMMON_FIELDS["symbol"],
            cls._COMMON_FIELDS["name"],
            cls._COMMON_FIELDS["exchange"],
            cls._COMMON_FIELDS["country"],
            cls._COMMON_FIELDS["currency"],
            
            # Market data
            cls._COMMON_FIELDS["price"],
            cls._COMMON_FIELDS["change_pct"],
            cls._COMMON_FIELDS["volume"],
            
            # ADR info if applicable
            FieldDefinition(
                name="adr_ratio", display_name="ADR Ratio", category=DataCategory.MARKET,
                field_type=FieldType.NUMBER, width=90, priority=20,
                description="ADR to ordinary share ratio"
            ),
            FieldDefinition(
                name="local_symbol", display_name="Local Symbol", category=DataCategory.METADATA,
                field_type=FieldType.STRING, width=100, priority=21
            ),
            FieldDefinition(
                name="local_price", display_name="Local Price", category=DataCategory.MARKET,
                field_type=FieldType.CURRENCY, width=100, priority=22
            ),
            
            # Performance
            FieldDefinition(
                name="ytd_return", display_name="YTD %", category=DataCategory.PERFORMANCE,
                field_type=FieldType.PERCENT, width=90, priority=30
            ),
            FieldDefinition(
                name="year_1_return", display_name="1Y %", category=DataCategory.PERFORMANCE,
                field_type=FieldType.PERCENT, width=90, priority=31
            ),
            FieldDefinition(
                name="year_3_return", display_name="3Y %", category=DataCategory.PERFORMANCE,
                field_type=FieldType.PERCENT, width=90, priority=32
            ),
            FieldDefinition(
                name="year_5_return", display_name="5Y %", category=DataCategory.PERFORMANCE,
                field_type=FieldType.PERCENT, width=90, priority=33
            ),
            
            # Fundamentals
            cls._COMMON_FIELDS["pe"],
            cls._COMMON_FIELDS["pb"],
            cls._COMMON_FIELDS["div_yield"],
            FieldDefinition(
                name="market_cap_usd", display_name="Market Cap (USD B)", category=DataCategory.FUNDAMENTAL,
                field_type=FieldType.NUMBER, width=150, priority=40,
                number_format="#,##0.0,,,B"
            ),
            
            # Risk
            cls._COMMON_FIELDS["beta"],
            cls._COMMON_FIELDS["volatility"],
            FieldDefinition(
                name="country_risk", display_name="Country Risk", category=DataCategory.RISK,
                field_type=FieldType.SCORE, width=100, priority=53
            ),
            
            cls._COMMON_FIELDS["last_updated"],
        ]
        
        return TabSchema(
            tab_type=TabType.GLOBAL_MARKETS,
            name="Global_Markets",
            description="International equities and ADRs",
            fields=fields,
            frozen_cols=2,
        )
    
    @classmethod
    def portfolio(cls) -> TabSchema:
        """My Portfolio tab - Position tracking"""
        fields = [
            cls._COMMON_FIELDS["rank"],
            cls._COMMON_FIELDS["symbol"],
            cls._COMMON_FIELDS["name"],
            FieldDefinition(
                name="asset_class", display_name="Asset Class", category=DataCategory.PORTFOLIO,
                field_type=FieldType.ENUM, width=120, priority=4,
                validation=ValidationRule(
                    type=ValidationType.LIST,
                    values=["Stock", "ETF", "Mutual Fund", "Bond", "Commodity", "Crypto", "Forex"]
                )
            ),
            cls._COMMON_FIELDS["sector"],
            
            # Account info
            FieldDefinition(
                name="account", display_name="Account", category=DataCategory.PORTFOLIO,
                field_type=FieldType.STRING, width=120, priority=6
            ),
            FieldDefinition(
                name="portfolio_group", display_name="Portfolio Group", category=DataCategory.PORTFOLIO,
                field_type=FieldType.ENUM, width=140, priority=7,
                description="Core, Satellite, Trading, etc.",
                validation=ValidationRule(
                    type=ValidationType.LIST,
                    values=["Core", "Satellite", "Trading", "Cash", "Retirement", "Speculative"]
                )
            ),
            
            # Position details
            cls._COMMON_FIELDS["quantity"],
            cls._COMMON_FIELDS["avg_cost"],
            cls._COMMON_FIELDS["cost_basis"],
            cls._COMMON_FIELDS["price"],
            cls._COMMON_FIELDS["market_value"],
            FieldDefinition(
                name="day_change", display_name="Day Change", category=DataCategory.PERFORMANCE,
                field_type=FieldType.CURRENCY, width=100, priority=14
            ),
            FieldDefinition(
                name="day_change_pct", display_name="Day Change %", category=DataCategory.PERFORMANCE,
                field_type=FieldType.PERCENT, width=110, priority=15
            ),
            
            # P&L
            cls._COMMON_FIELDS["unrealized_pl"],
            FieldDefinition(
                name="unrealized_pl_pct", display_name="Unrealized P&L %", category=DataCategory.PERFORMANCE,
                field_type=FieldType.PERCENT, width=130, priority=17,
                formula="=IF(cost_basis=0, 0, unrealized_pl/cost_basis)"
            ),
            
            # Allocation
            FieldDefinition(
                name="portfolio_value", display_name="Portfolio Value", category=DataCategory.PORTFOLIO,
                field_type=FieldType.CURRENCY, width=130, priority=20,
                formula="=SUM($market_value$)",
                hidden_formula=True
            ),
            cls._COMMON_FIELDS["weight"],
            FieldDefinition(
                name="target_weight", display_name="Target Weight %", category=DataCategory.PORTFOLIO,
                field_type=FieldType.PERCENT, width=130, priority=22
            ),
            FieldDefinition(
                name="rebalance_needed", display_name="Rebalance Δ", category=DataCategory.PORTFOLIO,
                field_type=FieldType.CURRENCY, width=120, priority=23,
                formula="=(target_weight * portfolio_value) - market_value"
            ),
            
            # Performance metrics
            FieldDefinition(
                name="return_1d", display_name="Return 1D", category=DataCategory.PERFORMANCE,
                field_type=FieldType.PERCENT, width=90, priority=30
            ),
            FieldDefinition(
                name="return_1w", display_name="Return 1W", category=DataCategory.PERFORMANCE,
                field_type=FieldType.PERCENT, width=90, priority=31
            ),
            FieldDefinition(
                name="return_1m", display_name="Return 1M", category=DataCategory.PERFORMANCE,
                field_type=FieldType.PERCENT, width=90, priority=32
            ),
            FieldDefinition(
                name="return_ytd", display_name="Return YTD", category=DataCategory.PERFORMANCE,
                field_type=FieldType.PERCENT, width=90, priority=33
            ),
            FieldDefinition(
                name="return_since_inception", display_name="Return Since Inception", category=DataCategory.PERFORMANCE,
                field_type=FieldType.PERCENT, width=160, priority=34
            ),
            
            # Risk metrics
            cls._COMMON_FIELDS["beta"],
            cls._COMMON_FIELDS["sharpe"],
            FieldDefinition(
                name="var_95_portfolio", display_name="VaR (95%)", category=DataCategory.RISK,
                field_type=FieldType.CURRENCY, width=100, priority=42
            ),
            
            # Diversification
            FieldDefinition(
                name="correlation_spy", display_name="Correlation to SPY", category=DataCategory.RISK,
                field_type=FieldType.NUMBER, width=140, priority=50
            ),
            FieldDefinition(
                name="correlation_tlt", display_name="Correlation to TLT", category=DataCategory.RISK,
                field_type=FieldType.NUMBER, width=140, priority=51
            ),
            
            # Notes
            FieldDefinition(
                name="notes", display_name="Notes", category=DataCategory.NOTE,
                field_type=FieldType.STRING, width=300, priority=100
            ),
            FieldDefinition(
                name="entry_date", display_name="Entry Date", category=DataCategory.TIMESTAMP,
                field_type=FieldType.DATE, width=100, priority=101
            ),
            cls._COMMON_FIELDS["last_updated"],
        ]
        
        # Add sparkline for price history
        fields.append(
            FieldDefinition(
                name="price_history", display_name="Price History", category=DataCategory.TECHNICAL,
                field_type=FieldType.SPARKLINE, width=120, priority=60,
                formula="=SPARKLINE(price_range)"
            )
        )
        
        return TabSchema(
            tab_type=TabType.PORTFOLIO,
            name="My_Portfolio",
            description="Portfolio tracking and analysis",
            fields=fields,
            frozen_cols=3,
            frozen_rows=5,
            default_sort="weight DESC",
            protected_ranges=[
                {
                    "range": "A:E",  # Metadata columns
                    "description": "Portfolio metadata protection",
                    "warningOnly": True
                }
            ],
            named_ranges=[
                {"name": "Portfolio_Data", "range": "A5:ZZ"},
                {"name": "Portfolio_Total", "range": "G5", "description": "Total portfolio value"}
            ]
        )
    
    @classmethod
    def market_scan(cls) -> TabSchema:
        """Market Scan tab - AI-screened opportunities"""
        fields = [
            cls._COMMON_FIELDS["rank"],
            cls._COMMON_FIELDS["symbol"],
            FieldDefinition(
                name="origin", display_name="Origin", category=DataCategory.METADATA,
                field_type=FieldType.STRING, width=100, priority=3,
                description="Source screen that identified this"
            ),
            cls._COMMON_FIELDS["name"],
            cls._COMMON_FIELDS["sector"],
            
            # Opportunity metrics
            FieldDefinition(
                name="opportunity_score", display_name="Opportunity Score", category=DataCategory.PERFORMANCE,
                field_type=FieldType.SCORE, width=150, priority=10,
                description="0-100 score combining all factors",
                conditional_formats=[
                    {"range": ">=80", "color": "#00A86B", "message": "High Conviction"},
                    {"range": ">=60", "color": "#8BC34A", "message": "Moderate Conviction"},
                    {"range": "<40", "color": "#D32F2F", "message": "Low Conviction"}
                ]
            ),
            FieldDefinition(
                name="conviction", display_name="Conviction", category=DataCategory.PERFORMANCE,
                field_type=FieldType.RATING, width=100, priority=11,
                validation=ValidationRule(
                    type=ValidationType.LIST,
                    values=["Very High", "High", "Medium", "Low", "Very Low"]
                )
            ),
            cls._COMMON_FIELDS["price"],
            cls._COMMON_FIELDS["change_pct"],
            
            # Strategy scores
            FieldDefinition(
                name="value_score", display_name="Value Score", category=DataCategory.PERFORMANCE,
                field_type=FieldType.SCORE, width=110, priority=20
            ),
            FieldDefinition(
                name="momentum_score", display_name="Momentum Score", category=DataCategory.PERFORMANCE,
                field_type=FieldType.SCORE, width=130, priority=21
            ),
            FieldDefinition(
                name="quality_score", display_name="Quality Score", category=DataCategory.PERFORMANCE,
                field_type=FieldType.SCORE, width=120, priority=22
            ),
            FieldDefinition(
                name="growth_score", display_name="Growth Score", category=DataCategory.PERFORMANCE,
                field_type=FieldType.SCORE, width=120, priority=23
            ),
            FieldDefinition(
                name="technical_score", display_name="Technical Score", category=DataCategory.PERFORMANCE,
                field_type=FieldType.SCORE, width=130, priority=24
            ),
            
            # Forecast
            cls._COMMON_FIELDS["fair_value"],
            cls._COMMON_FIELDS["upside"],
            cls._COMMON_FIELDS["roi_1m"],
            cls._COMMON_FIELDS["roi_3m"],
            cls._COMMON_FIELDS["roi_12m"],
            FieldDefinition(
                name="confidence", display_name="Confidence %", category=DataCategory.FORECAST,
                field_type=FieldType.PERCENT, width=110, priority=35
            ),
            
            # Risk
            cls._COMMON_FIELDS["risk_level"],
            FieldDefinition(
                name="risk_score", display_name="Risk Score", category=DataCategory.RISK,
                field_type=FieldType.SCORE, width=100, priority=40
            ),
            cls._COMMON_FIELDS["volatility"],
            
            # Technical triggers
            FieldDefinition(
                name="signal", display_name="Signal", category=DataCategory.TECHNICAL,
                field_type=FieldType.RATING, width=100, priority=50,
                validation=ValidationRule(
                    type=ValidationType.LIST,
                    values=["Strong Buy", "Buy", "Neutral", "Sell", "Strong Sell"]
                )
            ),
            cls._COMMON_FIELDS["rsi"],
            FieldDefinition(
                name="ma_signal", display_name="MA Signal", category=DataCategory.TECHNICAL,
                field_type=FieldType.RATING, width=100, priority=52
            ),
            
            # Reasoning
            FieldDefinition(
                name="primary_factor", display_name="Primary Factor", category=DataCategory.METADATA,
                field_type=FieldType.STRING, width=200, priority=60
            ),
            FieldDefinition(
                name="reasoning", display_name="Reasoning", category=DataCategory.METADATA,
                field_type=FieldType.STRING, width=350, priority=61
            ),
            FieldDefinition(
                name="catalysts", display_name="Catalysts", category=DataCategory.METADATA,
                field_type=FieldType.STRING, width=300, priority=62
            ),
            
            # Quality checks
            FieldDefinition(
                name="data_quality", display_name="Data Quality", category=DataCategory.METADATA,
                field_type=FieldType.RATING, width=110, priority=70
            ),
            
            cls._COMMON_FIELDS["last_updated"],
            FieldDefinition(
                name="scan_time_riyadh", display_name="Scan Time (Riyadh)", category=DataCategory.TIMESTAMP,
                field_type=FieldType.DATETIME, width=180, priority=101
            ),
            cls._COMMON_FIELDS["error"],
        ]
        
        return TabSchema(
            tab_type=TabType.MARKET_SCAN,
            name="Market_Scan",
            description="AI-screened market opportunities",
            fields=fields,
            frozen_cols=2,
            default_sort="opportunity_score DESC, upside DESC",
            conditional_formats=[
                {
                    "range": "J:J",  # Opportunity Score
                    "type": "NUMBER",
                    "condition": "NUMBER_GREATER_THAN_EQ",
                    "value": 80,
                    "format": {"backgroundColor": {"red": 0.8, "green": 1, "blue": 0.8}}
                }
            ]
        )
    
    @classmethod
    def insights(cls) -> TabSchema:
        """Insights Analysis tab - Deep research"""
        fields = [
            FieldDefinition(
                name="section", display_name="Section", category=DataCategory.METADATA,
                field_type=FieldType.ENUM, width=120, priority=1,
                validation=ValidationRule(
                    type=ValidationType.LIST,
                    values=["Macro", "Sector", "Thematic", "Stock-Specific", "Technical", "Quant"]
                )
            ),
            cls._COMMON_FIELDS["rank"],
            cls._COMMON_FIELDS["symbol"],
            FieldDefinition(
                name="title", display_name="Title", category=DataCategory.METADATA,
                field_type=FieldType.STRING, width=250, priority=4
            ),
            
            # Insight content
            FieldDefinition(
                name="insight_type", display_name="Insight Type", category=DataCategory.METADATA,
                field_type=FieldType.ENUM, width=130, priority=5,
                validation=ValidationRule(
                    type=ValidationType.LIST,
                    values=["Earnings", "Technical", "Valuation", "Macro", "Sector Rotation", 
                           "Merger/
