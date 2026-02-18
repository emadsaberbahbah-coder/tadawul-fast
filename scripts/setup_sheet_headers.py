#!/usr/bin/env python3
"""
setup_sheet_headers.py
===========================================================
TADAWUL FAST BRIDGE – ENHANCED SHEET INITIALIZER (v3.2.0)
===========================================================
PURPOSE-DRIVEN + SMART TEMPLATES + DATA-AWARE SCHEMAS

Core Philosophy
---------------
Each tab serves a specific purpose and should be initialized with:
- Purpose-optimized headers (not one-size-fits-all)
- Data-type appropriate formatting
- Business-logic specific columns
- Visual hierarchy matching information priority
- Auto-detected available data sources

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

Exit Codes
----------
0: Success
1: Configuration error
2: Partial success (some tabs failed)
3: API authentication failure
4: Rate limited / quota exceeded
"""

from __future__ import annotations

import argparse
import hashlib
import json
import logging
import os
import sys
import time
from collections import defaultdict
from dataclasses import asdict, dataclass, field
from datetime import datetime, timedelta, timezone
from enum import Enum
from typing import Any, Dict, List, Optional, Sequence, Set, Tuple, Union

# =============================================================================
# Path & Dependency Setup
# =============================================================================
def _ensure_project_root_on_path() -> None:
    """Ensure project root is in Python path"""
    try:
        script_dir = os.path.dirname(os.path.abspath(__file__))
        project_root = os.path.dirname(script_dir)
        
        for path in (script_dir, project_root):
            if path and path not in sys.path:
                sys.path.insert(0, path)
    except Exception:
        pass

_ensure_project_root_on_path()

# Import handling with graceful degradation
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
    print(f"⚠️  Google Sheets service unavailable: {e}")
    sheets = None

try:
    from core.schemas import get_schema_for_tab, get_field_definitions
    schemas = True
except ImportError:
    schemas = None

# =============================================================================
# Version & Logging
# =============================================================================
SCRIPT_VERSION = "3.2.0"
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
    CUSTOM = "custom"

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

class FieldType(Enum):
    """Field data type for formatting"""
    STRING = "string"
    NUMBER = "number"
    PERCENT = "percent"
    CURRENCY = "currency"
    DATE = "date"
    DATETIME = "datetime"
    BOOLEAN = "boolean"
    SCORE = "score"                # 0-100 scale
    RATING = "rating"              # Text rating (Buy/Sell)

@dataclass
class FieldDefinition:
    """Definition of a sheet field"""
    name: str
    display_name: str
    category: DataCategory
    field_type: FieldType
    description: str = ""
    required: bool = False
    example: Any = None
    formula: Optional[str] = None
    validation: Optional[Dict[str, Any]] = None
    width: int = 140
    alignment: str = "LEFT"
    visible: bool = True
    group: Optional[str] = None
    priority: int = 10  # Lower = higher priority (left side)
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            'name': self.name,
            'display_name': self.display_name,
            'category': self.category.value,
            'field_type': self.field_type.value,
            'width': self.width,
            'alignment': self.alignment,
            'visible': self.visible,
            'priority': self.priority
        }

@dataclass
class TabSchema:
    """Complete tab schema definition"""
    tab_type: TabType
    name: str
    description: str
    fields: List[FieldDefinition]
    header_row: int = 5
    frozen_rows: int = 5
    frozen_cols: int = 2
    default_sort: Optional[str] = None
    conditional_formats: List[Dict[str, Any]] = field(default_factory=list)
    data_validation: List[Dict[str, Any]] = field(default_factory=list)
    
    @property
    def visible_fields(self) -> List[FieldDefinition]:
        return [f for f in self.fields if f.visible]
    
    @property
    def field_names(self) -> List[str]:
        return [f.display_name for f in self.visible_fields]
    
    def get_field_by_name(self, name: str) -> Optional[FieldDefinition]:
        for f in self.fields:
            if f.name == name or f.display_name == name:
                return f
        return None

# =============================================================================
# Tab Schema Definitions (Purpose-Driven)
# =============================================================================
class TabSchemas:
    """Repository of all tab schemas"""
    
    @staticmethod
    def market_leaders() -> TabSchema:
        """Market Leaders tab - Top performers, momentum"""
        fields = [
            FieldDefinition("rank", "Rank", DataCategory.METADATA, FieldType.NUMBER, 
                          priority=1, width=60),
            FieldDefinition("symbol", "Symbol", DataCategory.METADATA, FieldType.STRING,
                          required=True, priority=2, width=110),
            FieldDefinition("name", "Company Name", DataCategory.METADATA, FieldType.STRING,
                          width=260, priority=3),
            FieldDefinition("sector", "Sector", DataCategory.METADATA, FieldType.STRING,
                          width=160, priority=4),
            FieldDefinition("market", "Market", DataCategory.METADATA, FieldType.STRING,
                          width=100, priority=5),
            
            # Performance metrics
            FieldDefinition("price", "Price", DataCategory.MARKET, FieldType.CURRENCY,
                          width=100, priority=6),
            FieldDefinition("change_pct", "Change %", DataCategory.PERFORMANCE, FieldType.PERCENT,
                          width=90, priority=7, 
                          conditional_formats={"positive": "green", "negative": "red"}),
            FieldDefinition("volume", "Volume", DataCategory.MARKET, FieldType.NUMBER,
                          width=100, priority=8),
            FieldDefinition("relative_volume", "Rel Volume", DataCategory.TECHNICAL, FieldType.NUMBER,
                          width=90, priority=9),
            
            # Momentum indicators
            FieldDefinition("rsi_14", "RSI (14)", DataCategory.TECHNICAL, FieldType.NUMBER,
                          width=90, priority=10,
                          validation={"min": 0, "max": 100}),
            FieldDefinition("momentum_1m", "Momentum 1M", DataCategory.PERFORMANCE, FieldType.PERCENT,
                          width=100, priority=11),
            FieldDefinition("momentum_3m", "Momentum 3M", DataCategory.PERFORMANCE, FieldType.PERCENT,
                          width=100, priority=12),
            FieldDefinition("volatility", "Volatility", DataCategory.RISK, FieldType.PERCENT,
                          width=100, priority=13),
            
            # Position in range
            FieldDefinition("week_52_high", "52W High", DataCategory.MARKET, FieldType.CURRENCY,
                          width=100, priority=14),
            FieldDefinition("week_52_low", "52W Low", DataCategory.MARKET, FieldType.CURRENCY,
                          width=100, priority=15),
            FieldDefinition("week_52_position", "52W Position %", DataCategory.TECHNICAL, FieldType.PERCENT,
                          width=120, priority=16),
            
            # Scores
            FieldDefinition("leadership_score", "Leadership Score", DataCategory.PERFORMANCE, FieldType.SCORE,
                          width=130, priority=17,
                          description="Composite score of market leadership"),
            FieldDefinition("momentum_score", "Momentum Score", DataCategory.PERFORMANCE, FieldType.SCORE,
                          width=130, priority=18),
            
            # Timestamps
            FieldDefinition("last_updated", "Last Updated (Riyadh)", DataCategory.TIMESTAMP, FieldType.DATETIME,
                          width=180, priority=100),
        ]
        
        return TabSchema(
            tab_type=TabType.MARKET_LEADERS,
            name="Market_Leaders",
            description="Top market performers and momentum leaders",
            fields=fields,
            default_sort="leadership_score DESC",
            frozen_cols=2,
            conditional_formats=[
                {
                    "range": "G:G",  # Change % column
                    "type": "NUMBER",
                    "condition": "NUMBER_GREATER_THAN_EQ",
                    "value": 0,
                    "format": {"textFormat": {"foregroundColor": {"red": 0, "green": 0.6, "blue": 0}}}
                },
                {
                    "range": "G:G",
                    "type": "NUMBER",
                    "condition": "NUMBER_LESS_THAN",
                    "value": 0,
                    "format": {"textFormat": {"foregroundColor": {"red": 0.8, "green": 0, "blue": 0}}}
                }
            ]
        )
    
    @staticmethod
    def ksa_tadawul() -> TabSchema:
        """KSA Tadawul tab - Saudi market specific"""
        fields = [
            FieldDefinition("rank", "#", DataCategory.METADATA, FieldType.NUMBER, priority=1, width=50),
            FieldDefinition("symbol", "Symbol", DataCategory.METADATA, FieldType.STRING, required=True, priority=2, width=100),
            FieldDefinition("company", "Company Name", DataCategory.METADATA, FieldType.STRING, width=280, priority=3),
            FieldDefinition("sector", "Sector", DataCategory.METADATA, FieldType.STRING, width=180, priority=4),
            
            # Saudi market specific
            FieldDefinition("isin", "ISIN", DataCategory.METADATA, FieldType.STRING, width=150, priority=5),
            FieldDefinition("tadawul_code", "Tadawul Code", DataCategory.METADATA, FieldType.STRING, width=120, priority=6),
            
            # Market data
            FieldDefinition("price", "Price (SAR)", DataCategory.MARKET, FieldType.CURRENCY, width=110, priority=7),
            FieldDefinition("change", "Change", DataCategory.MARKET, FieldType.CURRENCY, width=90, priority=8),
            FieldDefinition("change_pct", "Change %", DataCategory.PERFORMANCE, FieldType.PERCENT, width=90, priority=9),
            FieldDefinition("volume", "Volume", DataCategory.MARKET, FieldType.NUMBER, width=100, priority=10),
            FieldDefinition("value_traded", "Value Traded (M)", DataCategory.MARKET, FieldType.NUMBER, width=130, priority=11),
            FieldDefinition("market_cap", "Market Cap (B)", DataCategory.FUNDAMENTAL, FieldType.NUMBER, width=130, priority=12),
            
            # Technical
            FieldDefinition("rsi", "RSI (14)", DataCategory.TECHNICAL, FieldType.NUMBER, width=90, priority=13),
            FieldDefinition("ma_50", "MA (50)", DataCategory.TECHNICAL, FieldType.CURRENCY, width=100, priority=14),
            FieldDefinition("ma_200", "MA (200)", DataCategory.TECHNICAL, FieldType.CURRENCY, width=100, priority=15),
            FieldDefinition("volume_ma", "Volume MA", DataCategory.TECHNICAL, FieldType.NUMBER, width=100, priority=16),
            
            # Fundamental ratios (local context)
            FieldDefinition("pe", "P/E", DataCategory.FUNDAMENTAL, FieldType.NUMBER, width=90, priority=17),
            FieldDefinition("pb", "P/B", DataCategory.FUNDAMENTAL, FieldType.NUMBER, width=90, priority=18),
            FieldDefinition("dividend_yield", "Div Yield %", DataCategory.FUNDAMENTAL, FieldType.PERCENT, width=100, priority=19),
            FieldDefinition("eps", "EPS", DataCategory.FUNDAMENTAL, FieldType.CURRENCY, width=100, priority=20),
            
            # Saudi-specific metrics
            FieldDefinition("foreign_ownership", "Foreign Own %", DataCategory.FUNDAMENTAL, FieldType.PERCENT, width=120, priority=21),
            FieldDefinition("institutional_ownership", "Inst Own %", DataCategory.FUNDAMENTAL, FieldType.PERCENT, width=120, priority=22),
            FieldDefinition("free_float", "Free Float %", DataCategory.FUNDAMENTAL, FieldType.PERCENT, width=110, priority=23),
            
            # Risk & quality
            FieldDefinition("volatility", "Volatility", DataCategory.RISK, FieldType.PERCENT, width=100, priority=24),
            FieldDefinition("quality_score", "Quality Score", DataCategory.RISK, FieldType.SCORE, width=110, priority=25),
            FieldDefinition("liquidity_score", "Liquidity Score", DataCategory.RISK, FieldType.SCORE, width=120, priority=26),
            
            # Forecast
            FieldDefinition("fair_value", "Fair Value", DataCategory.FORECAST, FieldType.CURRENCY, width=110, priority=27),
            FieldDefinition("upside", "Upside %", DataCategory.FORECAST, FieldType.PERCENT, width=90, priority=28),
            FieldDefinition("analyst_rating", "Analyst Rating", DataCategory.FORECAST, FieldType.RATING, width=120, priority=29),
            FieldDefinition("target_price", "Target Price", DataCategory.FORECAST, FieldType.CURRENCY, width=110, priority=30),
            
            FieldDefinition("last_updated", "Last Updated", DataCategory.TIMESTAMP, FieldType.DATETIME, width=180, priority=100),
        ]
        
        return TabSchema(
            tab_type=TabType.KSA_TADAWUL,
            name="KSA_Tadawul",
            description="Saudi Stock Exchange detailed analysis",
            fields=fields,
            frozen_cols=2,
            default_sort="market_cap DESC",
        )
    
    @staticmethod
    def global_markets() -> TabSchema:
        """Global Markets tab - International exposure"""
        fields = [
            FieldDefinition("rank", "#", DataCategory.METADATA, FieldType.NUMBER, priority=1, width=50),
            FieldDefinition("symbol", "Symbol", DataCategory.METADATA, FieldType.STRING, required=True, priority=2, width=100),
            FieldDefinition("name", "Name", DataCategory.METADATA, FieldType.STRING, width=250, priority=3),
            FieldDefinition("exchange", "Exchange", DataCategory.METADATA, FieldType.STRING, width=120, priority=4),
            FieldDefinition("country", "Country", DataCategory.METADATA, FieldType.STRING, width=100, priority=5),
            FieldDefinition("region", "Region", DataCategory.METADATA, FieldType.STRING, width=100, priority=6),
            
            # Market data
            FieldDefinition("price", "Price", DataCategory.MARKET, FieldType.CURRENCY, width=100, priority=7),
            FieldDefinition("currency", "Currency", DataCategory.MARKET, FieldType.STRING, width=80, priority=8),
            FieldDefinition("change_pct", "Change %", DataCategory.PERFORMANCE, FieldType.PERCENT, width=90, priority=9),
            FieldDefinition("volume", "Volume", DataCategory.MARKET, FieldType.NUMBER, width=100, priority=10),
            
            # ADR info if applicable
            FieldDefinition("adr_ratio", "ADR Ratio", DataCategory.MARKET, FieldType.NUMBER, width=90, priority=11,
                          description="ADR to ordinary share ratio"),
            FieldDefinition("local_symbol", "Local Symbol", DataCategory.METADATA, FieldType.STRING, width=100, priority=12),
            FieldDefinition("local_price", "Local Price", DataCategory.MARKET, FieldType.CURRENCY, width=100, priority=13),
            
            # Performance
            FieldDefinition("ytd_return", "YTD %", DataCategory.PERFORMANCE, FieldType.PERCENT, width=90, priority=14),
            FieldDefinition("year_1_return", "1Y %", DataCategory.PERFORMANCE, FieldType.PERCENT, width=90, priority=15),
            FieldDefinition("year_3_return", "3Y %", DataCategory.PERFORMANCE, FieldType.PERCENT, width=90, priority=16),
            FieldDefinition("year_5_return", "5Y %", DataCategory.PERFORMANCE, FieldType.PERCENT, width=90, priority=17),
            
            # Fundamentals
            FieldDefinition("pe", "P/E", DataCategory.FUNDAMENTAL, FieldType.NUMBER, width=90, priority=18),
            FieldDefinition("pb", "P/B", DataCategory.FUNDAMENTAL, FieldType.NUMBER, width=90, priority=19),
            FieldDefinition("div_yield", "Div Yield %", DataCategory.FUNDAMENTAL, FieldType.PERCENT, width=100, priority=20),
            FieldDefinition("market_cap_usd", "Market Cap (USD B)", DataCategory.FUNDAMENTAL, FieldType.NUMBER, width=150, priority=21),
            
            # Risk metrics
            FieldDefinition("beta", "Beta", DataCategory.RISK, FieldType.NUMBER, width=80, priority=22),
            FieldDefinition("volatility", "Volatility", DataCategory.RISK, FieldType.PERCENT, width=100, priority=23),
            FieldDefinition("country_risk", "Country Risk", DataCategory.RISK, FieldType.SCORE, width=100, priority=24),
            
            FieldDefinition("last_updated", "Updated", DataCategory.TIMESTAMP, FieldType.DATETIME, width=160, priority=100),
        ]
        
        return TabSchema(
            tab_type=TabType.GLOBAL_MARKETS,
            name="Global_Markets",
            description="International equities and ADRs",
            fields=fields,
            frozen_cols=2,
        )
    
    @staticmethod
    def mutual_funds() -> TabSchema:
        """Mutual Funds tab - Fund analysis"""
        fields = [
            FieldDefinition("rank", "#", DataCategory.METADATA, FieldType.NUMBER, width=50, priority=1),
            FieldDefinition("fund_symbol", "Fund Symbol", DataCategory.METADATA, FieldType.STRING, required=True, width=120, priority=2),
            FieldDefinition("fund_name", "Fund Name", DataCategory.METADATA, FieldType.STRING, width=300, priority=3),
            FieldDefinition("fund_family", "Fund Family", DataCategory.METADATA, FieldType.STRING, width=200, priority=4),
            FieldDefinition("category", "Category", DataCategory.METADATA, FieldType.STRING, width=150, priority=5),
            
            # Performance
            FieldDefinition("nav", "NAV", DataCategory.MARKET, FieldType.CURRENCY, width=100, priority=6),
            FieldDefinition("nav_change", "NAV Change", DataCategory.PERFORMANCE, FieldType.CURRENCY, width=100, priority=7),
            FieldDefinition("nav_change_pct", "NAV Change %", DataCategory.PERFORMANCE, FieldType.PERCENT, width=110, priority=8),
            
            # Returns
            FieldDefinition("ytd_return", "YTD Return %", DataCategory.PERFORMANCE, FieldType.PERCENT, width=120, priority=9),
            FieldDefinition("year_1_return", "1Y Return %", DataCategory.PERFORMANCE, FieldType.PERCENT, width=120, priority=10),
            FieldDefinition("year_3_return", "3Y Return %", DataCategory.PERFORMANCE, FieldType.PERCENT, width=120, priority=11),
            FieldDefinition("year_5_return", "5Y Return %", DataCategory.PERFORMANCE, FieldType.PERCENT, width=120, priority=12),
            FieldDefinition("year_10_return", "10Y Return %", DataCategory.PERFORMANCE, FieldType.PERCENT, width=120, priority=13),
            FieldDefinition("since_inception", "Since Inception %", DataCategory.PERFORMANCE, FieldType.PERCENT, width=130, priority=14),
            
            # Fees
            FieldDefinition("expense_ratio", "Expense Ratio %", DataCategory.FUNDAMENTAL, FieldType.PERCENT, width=130, priority=15),
            FieldDefinition("front_load", "Front Load %", DataCategory.FUNDAMENTAL, FieldType.PERCENT, width=110, priority=16),
            FieldDefinition("back_load", "Back Load %", DataCategory.FUNDAMENTAL, FieldType.PERCENT, width=110, priority=17),
            FieldDefinition("management_fee", "Management Fee %", DataCategory.FUNDAMENTAL, FieldType.PERCENT, width=130, priority=18),
            FieldDefinition("12b1_fee", "12b-1 Fee %", DataCategory.FUNDAMENTAL, FieldType.PERCENT, width=110, priority=19),
            
            # Holdings
            FieldDefinition("aum", "AUM (M)", DataCategory.FUNDAMENTAL, FieldType.NUMBER, width=120, priority=20),
            FieldDefinition("top_holdings", "Top Holdings", DataCategory.FUNDAMENTAL, FieldType.STRING, width=300, priority=21,
                          visible=False),  # Hidden, for reference
            FieldDefinition("holdings_count", "# Holdings", DataCategory.FUNDAMENTAL, FieldType.NUMBER, width=100, priority=22),
            FieldDefinition("turnover", "Turnover %", DataCategory.FUNDAMENTAL, FieldType.PERCENT, width=110, priority=23),
            
            # Risk
            FieldDefinition("risk_level", "Risk Level", DataCategory.RISK, FieldType.RATING, width=100, priority=24),
            FieldDefinition("sharpe_3y", "Sharpe (3Y)", DataCategory.RISK, FieldType.NUMBER, width=100, priority=25),
            FieldDefinition("alpha_3y", "Alpha (3Y)", DataCategory.PERFORMANCE, FieldType.NUMBER, width=100, priority=26),
            FieldDefinition("beta_3y", "Beta (3Y)", DataCategory.RISK, FieldType.NUMBER, width=100, priority=27),
            FieldDefinition("r_squared", "R²", DataCategory.RISK, FieldType.NUMBER, width=80, priority=28),
            FieldDefinition("std_dev", "Std Dev", DataCategory.RISK, FieldType.PERCENT, width=100, priority=29),
            
            # Ratings
            FieldDefinition("morningstar_rating", "Morningstar", DataCategory.RISK, FieldType.RATING, width=110, priority=30),
            FieldDefinition("lipper_rating", "Lipper", DataCategory.RISK, FieldType.RATING, width=100, priority=31),
            
            FieldDefinition("last_updated", "Updated", DataCategory.TIMESTAMP, FieldType.DATETIME, width=160, priority=100),
        ]
        
        return TabSchema(
            tab_type=TabType.MUTUAL_FUNDS,
            name="Mutual_Funds",
            description="Mutual fund analysis and tracking",
            fields=fields,
            frozen_cols=2,
        )
    
    @staticmethod
    def commodities_fx() -> TabSchema:
        """Commodities & Forex tab"""
        fields = [
            FieldDefinition("rank", "#", DataCategory.METADATA, FieldType.NUMBER, width=50, priority=1),
            FieldDefinition("symbol", "Symbol", DataCategory.METADATA, FieldType.STRING, required=True, width=100, priority=2),
            FieldDefinition("name", "Name", DataCategory.METADATA, FieldType.STRING, width=200, priority=3),
            FieldDefinition("type", "Type", DataCategory.METADATA, FieldType.STRING, width=100, priority=4,
                          description="Commodity, Forex, Future, Option"),
            FieldDefinition("category", "Category", DataCategory.METADATA, FieldType.STRING, width=120, priority=5,
                          description="Metal, Energy, Agriculture, Major, Minor, etc."),
            
            # Pricing
            FieldDefinition("price", "Price", DataCategory.MARKET, FieldType.CURRENCY, width=110, priority=6),
            FieldDefinition("bid", "Bid", DataCategory.MARKET, FieldType.CURRENCY, width=100, priority=7),
            FieldDefinition("ask", "Ask", DataCategory.MARKET, FieldType.CURRENCY, width=100, priority=8),
            FieldDefinition("spread", "Spread", DataCategory.MARKET, FieldType.CURRENCY, width=90, priority=9),
            FieldDefinition("change", "Change", DataCategory.MARKET, FieldType.CURRENCY, width=90, priority=10),
            FieldDefinition("change_pct", "Change %", DataCategory.PERFORMANCE, FieldType.PERCENT, width=90, priority=11),
            
            # Commodity specific
            FieldDefinition("unit", "Unit", DataCategory.METADATA, FieldType.STRING, width=80, priority=12,
                          description="oz, barrel, bushel, etc."),
            FieldDefinition("contract_month", "Contract Month", DataCategory.MARKET, FieldType.STRING, width=120, priority=13,
                          visible=False),  # Hidden unless futures
            FieldDefinition("open_interest", "Open Interest", DataCategory.MARKET, FieldType.NUMBER, width=120, priority=14,
                          visible=False),
            
            # Forex specific
            FieldDefinition("base_currency", "Base", DataCategory.METADATA, FieldType.STRING, width=80, priority=15),
            FieldDefinition("quote_currency", "Quote", DataCategory.METADATA, FieldType.STRING, width=80, priority=16),
            FieldDefinition("pip_value", "Pip Value", DataCategory.MARKET, FieldType.CURRENCY, width=90, priority=17),
            FieldDefinition("swap_long", "Swap Long", DataCategory.MARKET, FieldType.CURRENCY, width=90, priority=18),
            FieldDefinition("swap_short", "Swap Short", DataCategory.MARKET, FieldType.CURRENCY, width=90, priority=19),
            
            # Technical
            FieldDefinition("rsi", "RSI", DataCategory.TECHNICAL, FieldType.NUMBER, width=80, priority=20),
            FieldDefinition("ma_50", "MA(50)", DataCategory.TECHNICAL, FieldType.CURRENCY, width=100, priority=21),
            FieldDefinition("ma_200", "MA(200)", DataCategory.TECHNICAL, FieldType.CURRENCY, width=100, priority=22),
            
            # Seasonality
            FieldDefinition("seasonal_trend", "Seasonal Trend", DataCategory.TECHNICAL, FieldType.RATING, width=120, priority=23),
            FieldDefinition("monthly_avg", "Monthly Avg", DataCategory.PERFORMANCE, FieldType.CURRENCY, width=100, priority=24),
            
            # Economic factors
            FieldDefinition("correlation_dxy", "Correlation DXY", DataCategory.RISK, FieldType.NUMBER, width=120, priority=25,
                          description="Correlation with US Dollar Index"),
            FieldDefinition("inflation_sensitivity", "Inflation Beta", DataCategory.RISK, FieldType.NUMBER, width=140, priority=26),
            
            FieldDefinition("last_updated", "Updated", DataCategory.TIMESTAMP, FieldType.DATETIME, width=160, priority=100),
        ]
        
        return TabSchema(
            tab_type=TabType.COMMODITIES_FX,
            name="Commodities_FX",
            description="Commodities and Forex markets",
            fields=fields,
            frozen_cols=2,
        )
    
    @staticmethod
    def portfolio() -> TabSchema:
        """My Portfolio tab - Position tracking"""
        fields = [
            # Position info
            FieldDefinition("rank", "#", DataCategory.METADATA, FieldType.NUMBER, width=50, priority=1),
            FieldDefinition("symbol", "Symbol", DataCategory.METADATA, FieldType.STRING, required=True, width=100, priority=2),
            FieldDefinition("name", "Name", DataCategory.METADATA, FieldType.STRING, width=220, priority=3),
            FieldDefinition("asset_class", "Asset Class", DataCategory.METADATA, FieldType.STRING, width=120, priority=4,
                          description="Stock, ETF, Fund, Commodity, Crypto"),
            FieldDefinition("sector", "Sector", DataCategory.METADATA, FieldType.STRING, width=150, priority=5),
            
            # Account info
            FieldDefinition("account", "Account", DataCategory.PORTFOLIO, FieldType.STRING, width=120, priority=6),
            FieldDefinition("broker", "Broker", DataCategory.PORTFOLIO, FieldType.STRING, width=120, priority=7),
            FieldDefinition("portfolio_group", "Portfolio Group", DataCategory.PORTFOLIO, FieldType.STRING, width=140, priority=8,
                          description="Core, Satellite, Trading, etc."),
            
            # Position details
            FieldDefinition("quantity", "Quantity", DataCategory.PORTFOLIO, FieldType.NUMBER, width=100, priority=9),
            FieldDefinition("avg_cost", "Avg Cost", DataCategory.PORTFOLIO, FieldType.CURRENCY, width=100, priority=10),
            FieldDefinition("cost_basis", "Cost Basis", DataCategory.PORTFOLIO, FieldType.CURRENCY, width=100, priority=11,
                          formula="=quantity * avg_cost"),
            
            # Current market
            FieldDefinition("current_price", "Current Price", DataCategory.MARKET, FieldType.CURRENCY, width=110, priority=12),
            FieldDefinition("market_value", "Market Value", DataCategory.PORTFOLIO, FieldType.CURRENCY, width=110, priority=13,
                          formula="=quantity * current_price"),
            FieldDefinition("day_change", "Day Change", DataCategory.PERFORMANCE, FieldType.CURRENCY, width=100, priority=14),
            FieldDefinition("day_change_pct", "Day Change %", DataCategory.PERFORMANCE, FieldType.PERCENT, width=110, priority=15),
            
            # P&L
            FieldDefinition("total_pl", "Total P&L", DataCategory.PERFORMANCE, FieldType.CURRENCY, width=100, priority=16,
                          formula="=market_value - cost_basis"),
            FieldDefinition("total_pl_pct", "Total P&L %", DataCategory.PERFORMANCE, FieldType.PERCENT, width=110, priority=17,
                          formula="=IF(cost_basis=0, 0, total_pl/cost_basis)"),
            
            # Allocation
            FieldDefinition("portfolio_value", "Portfolio Value", DataCategory.PORTFOLIO, FieldType.CURRENCY, width=130, priority=18,
                          description="Total portfolio value for percentage calc",
                          visible=False),  # Hidden reference
            FieldDefinition("weight", "Weight %", DataCategory.PORTFOLIO, FieldType.PERCENT, width=100, priority=19,
                          formula="=market_value / portfolio_value"),
            FieldDefinition("target_weight", "Target Weight %", DataCategory.PORTFOLIO, FieldType.PERCENT, width=130, priority=20),
            FieldDefinition("rebalance_delta", "Rebalance Δ", DataCategory.PORTFOLIO, FieldType.CURRENCY, width=120, priority=21,
                          formula="=(target_weight * portfolio_value) - market_value"),
            
            # Risk metrics
            FieldDefinition("portfolio_risk", "Portfolio Risk", DataCategory.RISK, FieldType.SCORE, width=110, priority=22),
            FieldDefinition("var_95", "VaR (95%)", DataCategory.RISK, FieldType.CURRENCY, width=100, priority=23),
            FieldDefinition("beta_to_portfolio", "Beta to Portfolio", DataCategory.RISK, FieldType.NUMBER, width=140, priority=24),
            
            # Diversification
            FieldDefinition("correlation_spy", "Correlation to SPY", DataCategory.RISK, FieldType.NUMBER, width=140, priority=25),
            FieldDefinition("correlation_tlt", "Correlation to TLT", DataCategory.RISK, FieldType.NUMBER, width=140, priority=26),
            
            # Notes
            FieldDefinition("notes", "Notes", DataCategory.METADATA, FieldType.STRING, width=300, priority=100),
            FieldDefinition("entry_date", "Entry Date", DataCategory.TIMESTAMP, FieldType.DATE, width=100, priority=90),
            FieldDefinition("last_updated", "Last Updated", DataCategory.TIMESTAMP, FieldType.DATETIME, width=160, priority=101),
        ]
        
        return TabSchema(
            tab_type=TabType.PORTFOLIO,
            name="My_Portfolio",
            description="Portfolio tracking and analysis",
            fields=fields,
            frozen_cols=3,
            frozen_rows=5,
            default_sort="weight DESC",
        )
    
    @staticmethod
    def market_scan() -> TabSchema:
        """Market Scan tab - AI-screened opportunities"""
        fields = [
            FieldDefinition("rank", "Rank", DataCategory.METADATA, FieldType.NUMBER, width=60, priority=1),
            FieldDefinition("symbol", "Symbol", DataCategory.METADATA, FieldType.STRING, required=True, width=100, priority=2),
            FieldDefinition("origin", "Origin", DataCategory.METADATA, FieldType.STRING, width=100, priority=3,
                          description="Source screen that identified this"),
            FieldDefinition("name", "Name", DataCategory.METADATA, FieldType.STRING, width=250, priority=4),
            FieldDefinition("market", "Market", DataCategory.METADATA, FieldType.STRING, width=100, priority=5),
            FieldDefinition("sector", "Sector", DataCategory.METADATA, FieldType.STRING, width=150, priority=6),
            
            # Opportunity metrics
            FieldDefinition("opportunity_score", "Opportunity Score", DataCategory.PERFORMANCE, FieldType.SCORE, width=150, priority=7,
                          description="0-100 score combining all factors"),
            FieldDefinition("conviction", "Conviction", DataCategory.PERFORMANCE, FieldType.RATING, width=100, priority=8),
            FieldDefinition("price", "Price", DataCategory.MARKET, FieldType.CURRENCY, width=100, priority=9),
            FieldDefinition("change_pct", "Change %", DataCategory.PERFORMANCE, FieldType.PERCENT, width=90, priority=10),
            
            # Strategy scores
            FieldDefinition("value_score", "Value Score", DataCategory.PERFORMANCE, FieldType.SCORE, width=110, priority=11),
            FieldDefinition("momentum_score", "Momentum Score", DataCategory.PERFORMANCE, FieldType.SCORE, width=130, priority=12),
            FieldDefinition("quality_score", "Quality Score", DataCategory.PERFORMANCE, FieldType.SCORE, width=120, priority=13),
            FieldDefinition("growth_score", "Growth Score", DataCategory.PERFORMANCE, FieldType.SCORE, width=120, priority=14),
            FieldDefinition("technical_score", "Technical Score", DataCategory.PERFORMANCE, FieldType.SCORE, width=130, priority=15),
            
            # Forecast
            FieldDefinition("fair_value", "Fair Value", DataCategory.FORECAST, FieldType.CURRENCY, width=110, priority=16),
            FieldDefinition("upside", "Upside %", DataCategory.FORECAST, FieldType.PERCENT, width=90, priority=17),
            FieldDefinition("expected_roi_1m", "ROI 1M %", DataCategory.FORECAST, FieldType.PERCENT, width=100, priority=18),
            FieldDefinition("expected_roi_3m", "ROI 3M %", DataCategory.FORECAST, FieldType.PERCENT, width=100, priority=19),
            FieldDefinition("expected_roi_12m", "ROI 12M %", DataCategory.FORECAST, FieldType.PERCENT, width=100, priority=20),
            FieldDefinition("confidence", "Confidence %", DataCategory.FORECAST, FieldType.PERCENT, width=110, priority=21),
            
            # Risk
            FieldDefinition("risk_level", "Risk Level", DataCategory.RISK, FieldType.RATING, width=100, priority=22),
            FieldDefinition("risk_score", "Risk Score", DataCategory.RISK, FieldType.SCORE, width=100, priority=23),
            FieldDefinition("volatility", "Volatility", DataCategory.RISK, FieldType.PERCENT, width=100, priority=24),
            
            # Technical triggers
            FieldDefinition("signal", "Signal", DataCategory.TECHNICAL, FieldType.RATING, width=100, priority=25,
                          description="Buy, Sell, Hold signal"),
            FieldDefinition("rsi", "RSI", DataCategory.TECHNICAL, FieldType.NUMBER, width=80, priority=26),
            FieldDefinition("ma_signal", "MA Signal", DataCategory.TECHNICAL, FieldType.RATING, width=100, priority=27),
            
            # Reasoning
            FieldDefinition("primary_factor", "Primary Factor", DataCategory.METADATA, FieldType.STRING, width=200, priority=28),
            FieldDefinition("reasoning", "Reasoning", DataCategory.METADATA, FieldType.STRING, width=350, priority=29),
            FieldDefinition("catalysts", "Catalysts", DataCategory.METADATA, FieldType.STRING, width=300, priority=30),
            
            # Quality checks
            FieldDefinition("data_quality", "Data Quality", DataCategory.METADATA, FieldType.RATING, width=110, priority=31),
            FieldDefinition("error", "Error", DataCategory.ERROR, FieldType.STRING, width=250, priority=100),
            
            # Timestamps
            FieldDefinition("scan_time_utc", "Scan Time (UTC)", DataCategory.TIMESTAMP, FieldType.DATETIME, width=180, priority=32),
            FieldDefinition("scan_time_riyadh", "Scan Time (Riyadh)", DataCategory.TIMESTAMP, FieldType.DATETIME, width=180, priority=33),
        ]
        
        return TabSchema(
            tab_type=TabType.MARKET_SCAN,
            name="Market_Scan",
            description="AI-screened market opportunities",
            fields=fields,
            frozen_cols=2,
            default_sort="opportunity_score DESC, upside DESC",
        )
    
    @staticmethod
    def insights() -> TabSchema:
        """Insights Analysis tab - Deep research"""
        fields = [
            FieldDefinition("section", "Section", DataCategory.METADATA, FieldType.STRING, width=120, priority=1,
                          description="Macro, Sector, Thematic, Stock-Specific"),
            FieldDefinition("rank", "#", DataCategory.METADATA, FieldType.NUMBER, width=50, priority=2),
            FieldDefinition("symbol", "Symbol", DataCategory.METADATA, FieldType.STRING, width=100, priority=3),
            FieldDefinition("name", "Name/Title", DataCategory.METADATA, FieldType.STRING, width=250, priority=4),
            
            # Insight content
            FieldDefinition("insight_type", "Insight Type", DataCategory.METADATA, FieldType.STRING, width=130, priority=5,
                          description="Earnings, Technical, Valuation, Macro, etc."),
            FieldDefinition("summary", "Summary", DataCategory.METADATA, FieldType.STRING, width=400, priority=6),
            FieldDefinition("key_points", "Key Points", DataCategory.METADATA, FieldType.STRING, width=500, priority=7),
            
            # Recommendation
            FieldDefinition("recommendation", "Recommendation", DataCategory.PERFORMANCE, FieldType.RATING, width=130, priority=8),
            FieldDefinition("conviction", "Conviction", DataCategory.PERFORMANCE, FieldType.RATING, width=100, priority=9),
            FieldDefinition("time_horizon", "Time Horizon", DataCategory.METADATA, FieldType.STRING, width=120, priority=10),
            
            # Impact
            FieldDefinition("expected_impact", "Expected Impact", DataCategory.FORECAST, FieldType.PERCENT, width=140, priority=11),
            FieldDefinition("confidence", "Confidence", DataCategory.FORECAST, FieldType.PERCENT, width=100, priority=12),
            
            # Risk
            FieldDefinition("risk_level", "Risk Level", DataCategory.RISK, FieldType.RATING, width=100, priority=13),
            FieldDefinition("risk_factors", "Risk Factors", DataCategory.RISK, FieldType.STRING, width=350, priority=14),
            
            # Scenarios
            FieldDefinition("bull_case", "Bull Case", DataCategory.FORECAST, FieldType.STRING, width=300, priority=15),
            FieldDefinition("base_case", "Base Case", DataCategory.FORECAST, FieldType.STRING, width=300, priority=16),
            FieldDefinition("bear_case", "Bear Case", DataCategory.FORECAST, FieldType.STRING, width=300, priority=17),
            FieldDefinition("probability_bull", "Prob Bull %", DataCategory.FORECAST, FieldType.PERCENT, width=110, priority=18),
            FieldDefinition("probability_base", "Prob Base %", DataCategory.FORECAST, FieldType.PERCENT, width=110, priority=19),
            FieldDefinition("probability_bear", "Prob Bear %", DataCategory.FORECAST, FieldType.PERCENT, width=110, priority=20),
            
            # Expected value
            FieldDefinition("expected_value", "Expected Value", DataCategory.FORECAST, FieldType.CURRENCY, width=130, priority=21,
                          description="Probability-weighted expected price"),
            FieldDefinition("current_price", "Current Price", DataCategory.MARKET, FieldType.CURRENCY, width=110, priority=22),
            FieldDefinition("upside_to_ev", "Upside to EV %", DataCategory.FORECAST, FieldType.PERCENT, width=130, priority=23),
            
            # Sources
            FieldDefinition("sources", "Sources", DataCategory.METADATA, FieldType.STRING, width=250, priority=24),
            FieldDefinition("analyst", "Analyst", DataCategory.METADATA, FieldType.STRING, width=150, priority=25),
            
            # Tracking
            FieldDefinition("status", "Status", DataCategory.METADATA, FieldType.STRING, width=100, priority=26,
                          description="Active, Completed, Invalidated"),
            FieldDefinition("created_date", "Created", DataCategory.TIMESTAMP, FieldType.DATETIME, width=160, priority=27),
            FieldDefinition("last_updated", "Updated", DataCategory.TIMESTAMP, FieldType.DATETIME, width=160, priority=28),
            
            # For allocation tracking
            FieldDefinition("allocated_amount", "Allocated (SAR)", DataCategory.PORTFOLIO, FieldType.CURRENCY, width=140, priority=29),
            FieldDefinition("expected_gain_1m", "Expected Gain 1M (SAR)", DataCategory.PORTFOLIO, FieldType.CURRENCY, width=180, priority=30,
                          formula="=allocated_amount * expected_impact_1m"),
        ]
        
        return TabSchema(
            tab_type=TabType.INSIGHTS,
            name="Insights_Analysis",
            description="Deep research and investment insights",
            fields=fields,
            frozen_cols=2,
            default_sort="section, rank",
        )
    
    @classmethod
    def get_schema(cls, tab_name: str) -> TabSchema:
        """Get schema by tab name"""
        name_lower = tab_name.lower().replace("_", "").replace(" ", "")
        
        if "marketleaders" in name_lower:
            return cls.market_leaders()
        elif "ksa" in name_lower or "tadawul" in name_lower:
            return cls.ksa_tadawul()
        elif "global" in name_lower:
            return cls.global_markets()
        elif "mutual" in name_lower or "fund" in name_lower:
            return cls.mutual_funds()
        elif "commod" in name_lower or "fx" in name_lower or "forex" in name_lower:
            return cls.commodities_fx()
        elif "portfolio" in name_lower:
            return cls.portfolio()
        elif "scan" in name_lower:
            return cls.market_scan()
        elif "insight" in name_lower or "analysis" in name_lower:
            return cls.insights()
        else:
            # Default to market leaders as fallback
            logger.warning(f"No specific schema for '{tab_name}', using Market Leaders template")
            return cls.market_leaders()

# =============================================================================
# Advanced Styling Engine
# =============================================================================
@dataclass
class StyleConfig:
    """Style configuration"""
    font_family: str = "Nunito"
    header_font_size: int = 10
    header_row_height: int = 32
    data_font_size: int = 10
    
    # Colors (Google Sheets format)
    header_bg: Dict[str, float] = field(default_factory=lambda: {"red": 0.17, "green": 0.24, "blue": 0.31})
    header_text: Dict[str, float] = field(default_factory=lambda: {"red": 1.0, "green": 1.0, "blue": 1.0})
    band_light: Dict[str, float] = field(default_factory=lambda: {"red": 1.0, "green": 1.0, "blue": 1.0})
    band_dark: Dict[str, float] = field(default_factory=lambda: {"red": 0.96, "green": 0.96, "blue": 0.96})
    
    # Conditional colors
    positive_green: Dict[str, float] = field(default_factory=lambda: {"red": 0.0, "green": 0.6, "blue": 0.0})
    negative_red: Dict[str, float] = field(default_factory=lambda: {"red": 0.8, "green": 0.0, "blue": 0.0})
    neutral_gray: Dict[str, float] = field(default_factory=lambda: {"red": 0.5, "green": 0.5, "blue": 0.5})
    
    # Alignment
    alignments: Dict[FieldType, str] = field(default_factory=lambda: {
        FieldType.NUMBER: "RIGHT",
        FieldType.PERCENT: "RIGHT",
        FieldType.CURRENCY: "RIGHT",
        FieldType.DATE: "CENTER",
        FieldType.DATETIME: "CENTER",
        FieldType.SCORE: "CENTER",
        FieldType.RATING: "CENTER",
        FieldType.STRING: "LEFT",
        FieldType.BOOLEAN: "CENTER",
    })

class StyleBuilder:
    """Builds Google Sheets API requests for styling"""
    
    def __init__(self, spreadsheet_id: str, service: Any, config: StyleConfig = None):
        self.spreadsheet_id = spreadsheet_id
        self.service = service
        self.config = config or StyleConfig()
        self.requests = []
    
    def add_header_formatting(self, sheet_id: int, header_row: int, col_count: int) -> 'StyleBuilder':
        """Format header row"""
        self.requests.append({
            "repeatCell": {
                "range": {
                    "sheetId": sheet_id,
                    "startRowIndex": header_row - 1,
                    "endRowIndex": header_row,
                    "startColumnIndex": 0,
                    "endColumnIndex": col_count,
                },
                "cell": {
                    "userEnteredFormat": {
                        "backgroundColor": self.config.header_bg,
                        "horizontalAlignment": "CENTER",
                        "verticalAlignment": "MIDDLE",
                        "wrapStrategy": "WRAP",
                        "textFormat": {
                            "foregroundColor": self.config.header_text,
                            "fontFamily": self.config.font_family,
                            "fontSize": self.config.header_font_size,
                            "bold": True,
                        },
                    }
                },
                "fields": "userEnteredFormat(backgroundColor,textFormat,horizontalAlignment,verticalAlignment,wrapStrategy)",
            }
        })
        
        # Header row height
        self.requests.append({
            "updateDimensionProperties": {
                "range": {
                    "sheetId": sheet_id,
                    "dimension": "ROWS",
                    "startIndex": header_row - 1,
                    "endIndex": header_row,
                },
                "properties": {"pixelSize": self.config.header_row_height},
                "fields": "pixelSize",
            }
        })
        
        return self
    
    def add_frozen_rows(self, sheet_id: int, frozen_rows: int) -> 'StyleBuilder':
        """Freeze rows"""
        self.requests.append({
            "updateSheetProperties": {
                "properties": {
                    "sheetId": sheet_id,
                    "gridProperties": {"frozenRowCount": frozen_rows}
                },
                "fields": "gridProperties.frozenRowCount",
            }
        })
        return self
    
    def add_frozen_cols(self, sheet_id: int, frozen_cols: int) -> 'StyleBuilder':
        """Freeze columns"""
        self.requests.append({
            "updateSheetProperties": {
                "properties": {
                    "sheetId": sheet_id,
                    "gridProperties": {"frozenColumnCount": frozen_cols}
                },
                "fields": "gridProperties.frozenColumnCount",
            }
        })
        return self
    
    def add_filter(self, sheet_id: int, header_row: int, col_count: int) -> 'StyleBuilder':
        """Add filter to header row"""
        self.requests.append({
            "setBasicFilter": {
                "filter": {
                    "range": {
                        "sheetId": sheet_id,
                        "startRowIndex": header_row - 1,
                        "endRowIndex": header_row,
                        "startColumnIndex": 0,
                        "endColumnIndex": col_count,
                    }
                }
            }
        })
        return self
    
    def add_banding(self, sheet_id: int, start_row: int, col_count: int) -> 'StyleBuilder':
        """Add alternating row colors"""
        self.requests.append({
            "addBanding": {
                "bandedRange": {
                    "range": {
                        "sheetId": sheet_id,
                        "startRowIndex": start_row,
                        "startColumnIndex": 0,
                        "endColumnIndex": col_count,
                    },
                    "rowProperties": {
                        "firstBandColor": self.config.band_light,
                        "secondBandColor": self.config.band_dark,
                    },
                }
            }
        })
        return self
    
    def add_column_widths(self, sheet_id: int, fields: List[FieldDefinition]) -> 'StyleBuilder':
        """Set column widths based on field definitions"""
        for idx, field in enumerate(fields):
            if not field.visible:
                continue
                
            self.requests.append({
                "updateDimensionProperties": {
                    "range": {
                        "sheetId": sheet_id,
                        "dimension": "COLUMNS",
                        "startIndex": idx,
                        "endIndex": idx + 1,
                    },
                    "properties": {"pixelSize": field.width},
                    "fields": "pixelSize",
                }
            })
        return self
    
    def add_column_formats(self, sheet_id: int, fields: List[FieldDefinition], 
                          start_row: int) -> 'StyleBuilder':
        """Apply number formats to columns"""
        for idx, field in enumerate(fields):
            if not field.visible:
                continue
                
            format_spec = self._get_number_format(field.field_type)
            alignment = self.config.alignments.get(field.field_type, "LEFT")
            
            self.requests.append({
                "repeatCell": {
                    "range": {
                        "sheetId": sheet_id,
                        "startRowIndex": start_row,
                        "startColumnIndex": idx,
                        "endColumnIndex": idx + 1,
                    },
                    "cell": {
                        "userEnteredFormat": {
                            "numberFormat": format_spec,
                            "horizontalAlignment": alignment,
                            "textFormat": {
                                "fontFamily": self.config.font_family,
                                "fontSize": self.config.data_font_size,
                            }
                        }
                    },
                    "fields": "userEnteredFormat(numberFormat,horizontalAlignment,textFormat)",
                }
            })
        return self
    
    def add_conditional_formats(self, sheet_id: int, schema: TabSchema) -> 'StyleBuilder':
        """Add conditional formatting rules"""
        for rule in schema.conditional_formats:
            self.requests.append({
                "addConditionalFormatRule": {
                    "rule": {
                        "ranges": [{
                            "sheetId": sheet_id,
                            **self._parse_range(rule.get("range", ""))
                        }],
                        "booleanRule": {
                            "condition": {
                                "type": rule["condition"],
                                "values": [{"userEnteredValue": str(rule.get("value", 0))}]
                            },
                            "format": rule["format"]
                        }
                    },
                    "index": 0
                }
            })
        return self
    
    def add_protected_range(self, sheet_id: int, header_row: int, 
                           col_count: int, warning_only: bool = True) -> 'StyleBuilder':
        """Protect header row"""
        self.requests.append({
            "addProtectedRange": {
                "protectedRange": {
                    "range": {
                        "sheetId": sheet_id,
                        "startRowIndex": header_row - 1,
                        "endRowIndex": header_row,
                        "startColumnIndex": 0,
                        "endColumnIndex": col_count,
                    },
                    "description": "TFB Header Protection",
                    "warningOnly": warning_only,
                }
            }
        })
        return self
    
    def _get_number_format(self, field_type: FieldType) -> Dict[str, str]:
        """Get Google Sheets number format for field type"""
        formats = {
            FieldType.NUMBER: {"type": "NUMBER", "pattern": "#,##0.00"},
            FieldType.PERCENT: {"type": "NUMBER", "pattern": "0.00"},  # Display as number, format as % in sheet
            FieldType.CURRENCY: {"type": "CURRENCY", "pattern": "#,##0.00"},
            FieldType.DATE: {"type": "DATE", "pattern": "yyyy-mm-dd"},
            FieldType.DATETIME: {"type": "DATE_TIME", "pattern": "yyyy-mm-dd hh:mm:ss"},
            FieldType.SCORE: {"type": "NUMBER", "pattern": "0.0"},
            FieldType.BOOLEAN: {"type": "BOOLEAN", "pattern": ""},
        }
        return formats.get(field_type, {"type": "NUMBER", "pattern": "0"})
    
    def _parse_range(self, range_str: str) -> Dict[str, int]:
        """Parse A1 range to zero-based indices"""
        # Simplified - in production would parse properly
        return {"startColumnIndex": 0, "endColumnIndex": 1}
    
    def execute(self) -> None:
        """Execute all requests"""
        if self.requests:
            self.service.spreadsheets().batchUpdate(
                spreadsheetId=self.spreadsheet_id,
                body={"requests": self.requests}
            ).execute()
            logger.debug(f"Executed {len(self.requests)} style requests")

# =============================================================================
# Sheet Manager
# =============================================================================
class SheetManager:
    """Manages sheet operations"""
    
    def __init__(self, spreadsheet_id: str):
        self.spreadsheet_id = spreadsheet_id
        self.service = None
        self.sheets_meta: Dict[str, int] = {}
        
    def initialize(self) -> bool:
        """Initialize Sheets API connection"""
        if not sheets:
            logger.error("Google Sheets service not available")
            return False
            
        try:
            self.service = sheets.get_sheets_service()
            spreadsheet = self.service.spreadsheets().get(spreadsheetId=self.spreadsheet_id).execute()
            
            for sh in spreadsheet.get("sheets", []):
                props = sh.get("properties", {})
                title = props.get("title")
                sheet_id = props.get("sheetId")
                if title and sheet_id is not None:
                    self.sheets_meta[title] = sheet_id
                    
            logger.info(f"Connected to spreadsheet with {len(self.sheets_meta)} sheets")
            return True
            
        except Exception as e:
            logger.error(f"Failed to initialize Sheets API: {e}")
            return False
    
    def tab_exists(self, tab_name: str) -> bool:
        """Check if tab exists"""
        return tab_name in self.sheets_meta
    
    def get_sheet_id(self, tab_name: str) -> Optional[int]:
        """Get sheet ID for tab"""
        return self.sheets_meta.get(tab_name)
    
    def create_tab(self, tab_name: str) -> Optional[int]:
        """Create new tab"""
        try:
            request = {
                "requests": [{
                    "addSheet": {
                        "properties": {
                            "title": tab_name,
                            "gridProperties": {
                                "rowCount": 1000,
                                "columnCount": 50
                            }
                        }
                    }
                }]
            }
            
            response = self.service.spreadsheets().batchUpdate(
                spreadsheetId=self.spreadsheet_id,
                body=request
            ).execute()
            
            sheet_id = response["replies"][0]["addSheet"]["properties"]["sheetId"]
            self.sheets_meta[tab_name] = sheet_id
            
            logger.info(f"✅ Created tab: {tab_name}")
            return sheet_id
            
        except Exception as e:
            logger.error(f"Failed to create tab {tab_name}: {e}")
            return None
    
    def read_headers(self, tab_name: str, header_row: int) -> List[str]:
        """Read existing headers"""
        if not self.tab_exists(tab_name):
            return []
            
        try:
            a1_range = f"'{tab_name}'!{header_row}:{header_row}"
            result = self.service.spreadsheets().values().get(
                spreadsheetId=self.spreadsheet_id,
                range=a1_range
            ).execute()
            
            values = result.get("values", [])
            if values:
                return values[0]
            return []
            
        except Exception as e:
            logger.warning(f"Failed to read headers from {tab_name}: {e}")
            return []
    
    def write_headers(self, tab_name: str, headers: List[str], 
                     header_row: int, clear_first: bool = False) -> bool:
        """Write headers to sheet"""
        try:
            # Clear existing if requested
            if clear_first:
                end_col = self._col_num_to_letter(len(headers))
                clear_range = f"'{tab_name}'!A{header_row}:{end_col}{header_row}"
                self.service.spreadsheets().values().clear(
                    spreadsheetId=self.spreadsheet_id,
                    range=clear_range,
                    body={}
                ).execute()
            
            # Write headers
            a1_range = f"'{tab_name}'!A{header_row}"
            body = {"values": [headers]}
            
            self.service.spreadsheets().values().update(
                spreadsheetId=self.spreadsheet_id,
                range=a1_range,
                valueInputOption="RAW",
                body=body
            ).execute()
            
            logger.info(f"✅ Wrote {len(headers)} headers to {tab_name}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to write headers to {tab_name}: {e}")
            return False
    
    def apply_styling(self, tab_name: str, schema: TabSchema, 
                     header_row: int, no_style: bool = False) -> bool:
        """Apply styling to tab"""
        if no_style:
            return True
            
        sheet_id = self.get_sheet_id(tab_name)
        if sheet_id is None:
            logger.warning(f"Cannot style {tab_name}: sheet ID not found")
            return False
        
        try:
            builder = StyleBuilder(self.spreadsheet_id, self.service)
            visible_fields = schema.visible_fields
            col_count = len(visible_fields)
            
            # Apply styling in sequence
            (builder
             .add_header_formatting(sheet_id, header_row, col_count)
             .add_frozen_rows(sheet_id, schema.frozen_rows)
             .add_frozen_cols(sheet_id, schema.frozen_cols)
             .add_filter(sheet_id, header_row, col_count)
             .add_banding(sheet_id, header_row, col_count)
             .add_column_widths(sheet_id, visible_fields)
             .add_column_formats(sheet_id, visible_fields, header_row)
             .add_conditional_formats(sheet_id, schema)
             .add_protected_range(sheet_id, header_row, col_count, warning_only=True)
             .execute())
            
            logger.info(f"✨ Styled {tab_name} with {col_count} columns")
            return True
            
        except Exception as e:
            logger.error(f"Failed to style {tab_name}: {e}")
            return False
    
    def _col_num_to_letter(self, n: int) -> str:
        """Convert column number to letter (1-indexed)"""
        if n <= 0:
            return "A"
        
        result = ""
        while n > 0:
            n -= 1
            result = chr(65 + (n % 26)) + result
            n //= 26
        return result

# =============================================================================
# Main Application
# =============================================================================
def main(argv: Optional[List[str]] = None) -> int:
    parser = argparse.ArgumentParser(
        description="TFB Enhanced Sheet Initializer - Purpose-Driven Templates",
        formatter_class=argparse.RawDescriptionHelpFormatter
    )
    
    # Basic options
    parser.add_argument("--sheet-id", help="Target Spreadsheet ID")
    parser.add_argument("--tabs", nargs="*", help="Specific tabs to setup")
    parser.add_argument("--all-existing", action="store_true", 
                       help="Initialize ALL existing tabs")
    parser.add_argument("--create-missing", action="store_true",
                       help="Create tabs that don't exist")
    
    # Header configuration
    parser.add_argument("--row", type=int, default=5,
                       help="Header row (1-based, default: 5)")
    parser.add_argument("--force", action="store_true",
                       help="Overwrite non-empty headers")
    parser.add_argument("--clear", action="store_true",
                       help="Clear header range before writing")
    
    # Style control
    parser.add_argument("--no-style", action="store_true",
                       help="Skip styling (headers only)")
    parser.add_argument("--no-banding", action="store_true",
                       help="Disable row banding")
    parser.add_argument("--no-filter", action="store_true",
                       help="Disable filters")
    parser.add_argument("--no-formatting", action="store_true",
                       help="Disable number formatting")
    parser.add_argument("--font", default="Nunito",
                       help="Font family (default: Nunito)")
    
    # Execution mode
    parser.add_argument("--dry-run", action="store_true",
                       help="Preview changes without writing")
    parser.add_argument("--verbose", "-v", action="store_true",
                       help="Verbose logging")
    parser.add_argument("--export-schema", 
                       help="Export schema to JSON file")
    
    args = parser.parse_args(argv)
    
    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)
    
    # Get spreadsheet ID
    spreadsheet_id = args.sheet_id
    if not spreadsheet_id:
        spreadsheet_id = os.getenv("DEFAULT_SPREADSHEET_ID", "")
    
    if not spreadsheet_id:
        logger.error("❌ No spreadsheet ID provided. Set --sheet-id or DEFAULT_SPREADSHEET_ID env var")
        return 1
    
    # Initialize sheet manager
    manager = SheetManager(spreadsheet_id)
    if not manager.initialize():
        return 1
    
    # Determine target tabs
    if args.all_existing:
        target_tabs = list(manager.sheets_meta.keys())
    else:
        target_tabs = args.tabs or [
            "Market_Leaders",
            "KSA_Tadawul",
            "Global_Markets",
            "Mutual_Funds",
            "Commodities_FX",
            "My_Portfolio",
            "Market_Scan",
            "Insights_Analysis",
        ]
    
    logger.info("=" * 60)
    logger.info(f"Enhanced Sheet Initializer v{SCRIPT_VERSION}")
    logger.info(f"Spreadsheet: {spreadsheet_id}")
    logger.info(f"Target tabs: {len(target_tabs)}")
    logger.info("=" * 60)
    
    stats = {
        "success": 0,
        "skipped": 0,
        "failed": 0,
        "created": 0
    }
    
    # Process each tab
    for tab_name in target_tabs:
        tab_name = str(tab_name).strip()
        if not tab_name:
            continue
            
        logger.info(f"\n--- Processing: {tab_name} ---")
        
        # Check if tab exists
        if not manager.tab_exists(tab_name):
            if args.create_missing and not args.dry_run:
                sheet_id = manager.create_tab(tab_name)
                if sheet_id:
                    stats["created"] += 1
                else:
                    stats["failed"] += 1
                    continue
            else:
                logger.warning(f"⚠️  Tab '{tab_name}' does not exist (use --create-missing to create)")
                stats["skipped"] += 1
                continue
        
        # Get schema
        schema = TabSchemas.get_schema(tab_name)
        
        if args.export_schema:
            # Export schema to JSON
            schema_path = Path(args.export_schema) / f"{tab_name}_schema.json"
            schema_path.parent.mkdir(parents=True, exist_ok=True)
            with open(schema_path, 'w') as f:
                json.dump({
                    'tab_type': schema.tab_type.value,
                    'name': schema.name,
                    'fields': [f.to_dict() for f in schema.fields]
                }, f, indent=2)
            logger.info(f"📄 Exported schema to {schema_path}")
        
        if args.dry_run:
            logger.info(f"[DRY RUN] Would initialize {tab_name} with {len(schema.visible_fields)} headers")
            stats["success"] += 1
            continue
        
        # Check existing headers
        existing = manager.read_headers(tab_name, args.row)
        if existing and not args.force:
            logger.info(f"ℹ️  Tab '{tab_name}' already has headers (use --force to overwrite)")
            stats["skipped"] += 1
            continue
        
        # Write headers
        if not manager.write_headers(tab_name, schema.field_names, args.row, args.clear):
            stats["failed"] += 1
            continue
        
        # Apply styling
        style_success = manager.apply_styling(
            tab_name, 
            schema, 
            args.row,
            no_style=args.no_style
        )
        
        if style_success:
            stats["success"] += 1
        else:
            stats["failed"] += 1
    
    # Summary
    logger.info("\n" + "=" * 60)
    logger.info("SUMMARY")
    logger.info("=" * 60)
    logger.info(f"✅ Successful: {stats['success']}")
    logger.info(f"⏭️  Skipped: {stats['skipped']}")
    logger.info(f"❌ Failed: {stats['failed']}")
    logger.info(f"🆕 Created: {stats['created']}")
    logger.info("=" * 60)
    
    if args.export_schema:
        logger.info(f"📄 Schemas exported to {args.export_schema}")
    
    return 0 if stats["failed"] == 0 else 2

if __name__ == "__main__":
    sys.exit(main())
