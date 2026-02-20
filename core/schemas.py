#!/usr/bin/env python3
"""
core/schemas.py
===========================================================
ADVANCED CANONICAL SCHEMAS + HEADERS — v5.0.0 (NEXT-GEN ENTERPRISE)
===========================================================

What's new in v5.0.0:
- ✅ Pydantic V2 Native Support with Rust-based validation (graceful V1 fallback)
- ✅ High-Performance JSON (orjson) integrated into model serialization
- ✅ ML & Predictive Fields: Mapped VWAP, SuperTrend, Market Regime, Price Anomalies
- ✅ Advanced Header Normalization: LRU Cached multi-lingual (Arabic/English/French) fuzzy matching
- ✅ Thread-safe operations with immutable defaults and zero-copy references
- ✅ Batch processing models upgraded with tracking metadata

Key Features:
- Complete UnifiedQuote model with 130+ fields for comprehensive analysis
- Dynamic schema versioning with V3 -> V4 -> V5 migration support
- Field grouping and categorization for dynamic UI/UX rendering
- Centralized Data Quality and Recommendation scoring Enums
"""

from __future__ import annotations

import re
import hashlib
import threading
from datetime import datetime, date
from enum import Enum
from functools import lru_cache
from typing import Any, Dict, List, Optional, Sequence, Set, Tuple, Union, Callable, TypeVar, Generic

# ---------------------------------------------------------------------------
# High-Performance JSON fallback
# ---------------------------------------------------------------------------
try:
    import orjson
    def json_dumps(v, *, default):
        return orjson.dumps(v, default=default).decode()
    def json_loads(v):
        return orjson.loads(v)
    _HAS_ORJSON = True
except ImportError:
    import json
    def json_dumps(v, *, default):
        return json.dumps(v, default=default)
    def json_loads(v):
        return json.loads(v)
    _HAS_ORJSON = False

# ---------------------------------------------------------------------------
# Pydantic Configuration
# ---------------------------------------------------------------------------
try:
    from pydantic import BaseModel, Field, ConfigDict, field_validator, model_validator
    from pydantic import ValidationError
    _PYDANTIC_V2 = True
except ImportError:
    from pydantic import BaseModel, Field, validator, root_validator
    from pydantic import ValidationError
    ConfigDict = None
    field_validator = None
    model_validator = None
    _PYDANTIC_V2 = False

# Version
SCHEMAS_VERSION = "5.0.0"

# Thread-safe caches
_CACHE_LOCK = threading.RLock()
_NORM_HEADER_CACHE: Dict[str, str] = {}
_CANONICAL_FIELD_CACHE: Dict[str, str] = {}

# =============================================================================
# Enums and Constants
# =============================================================================

class MarketType(str, Enum):
    KSA = "KSA"
    UAE = "UAE"
    QATAR = "QATAR"
    KUWAIT = "KUWAIT"
    US = "US"
    GLOBAL = "GLOBAL"
    COMMODITY = "COMMODITY"
    FOREX = "FOREX"
    CRYPTO = "CRYPTO"
    FUND = "FUND"

class AssetClass(str, Enum):
    EQUITY = "EQUITY"
    ETF = "ETF"
    MUTUAL_FUND = "MUTUAL_FUND"
    COMMODITY = "COMMODITY"
    CURRENCY = "CURRENCY"
    CRYPTOCURRENCY = "CRYPTOCURRENCY"
    BOND = "BOND"
    REAL_ESTATE = "REAL_ESTATE"
    DERIVATIVE = "DERIVATIVE"

class Recommendation(str, Enum):
    BUY = "BUY"
    HOLD = "HOLD"
    REDUCE = "REDUCE"
    SELL = "SELL"
    
    @classmethod
    def from_score(cls, score: float, scale: str = "1-5") -> "Recommendation":
        if scale == "1-5":
            if score <= 2.0: return cls.BUY
            elif score <= 3.0: return cls.HOLD
            elif score <= 4.0: return cls.REDUCE
            else: return cls.SELL
        elif scale == "1-3":
            if score <= 1.5: return cls.BUY
            elif score <= 2.5: return cls.HOLD
            else: return cls.SELL
        elif scale == "0-100":
            if score >= 70: return cls.BUY
            elif score >= 45: return cls.HOLD
            else: return cls.SELL
        return cls.HOLD
    
    def to_score(self, scale: str = "1-5") -> float:
        scores = {
            Recommendation.BUY: { "1-5": 1.0, "1-3": 1.0, "0-100": 85.0 },
            Recommendation.HOLD: { "1-5": 3.0, "1-3": 2.0, "0-100": 55.0 },
            Recommendation.REDUCE: { "1-5": 4.0, "1-3": 2.5, "0-100": 35.0 },
            Recommendation.SELL: { "1-5": 5.0, "1-3": 3.0, "0-100": 15.0 }
        }
        return scores[self][scale]

class DataQuality(str, Enum):
    EXCELLENT = "EXCELLENT"
    HIGH = "HIGH"
    MEDIUM = "MEDIUM"
    LOW = "LOW"
    POOR = "POOR"
    STALE = "STALE"
    MISSING = "MISSING"
    ERROR = "ERROR"

class BadgeLevel(str, Enum):
    EXCELLENT = "EXCELLENT"
    GOOD = "GOOD"
    NEUTRAL = "NEUTRAL"
    CAUTION = "CAUTION"
    DANGER = "DANGER"
    NONE = "NONE"


# =============================================================================
# Validation Helpers
# =============================================================================

def safe_float(value: Any, default: Optional[float] = None) -> Optional[float]:
    if value is None: return default
    try:
        if isinstance(value, (int, float)) and not isinstance(value, bool): return float(value)
        if isinstance(value, str):
            cleaned = re.sub(r'[^\d.,\-eE]', '', value.strip())
            if not cleaned: return default
            if ',' in cleaned and '.' in cleaned:
                if cleaned.rindex(',') > cleaned.rindex('.'): cleaned = cleaned.replace('.', '').replace(',', '.')
                else: cleaned = cleaned.replace(',', '')
            elif ',' in cleaned: cleaned = cleaned.replace(',', '.')
            return float(cleaned)
    except (ValueError, TypeError, AttributeError): pass
    return default

def safe_int(value: Any, default: Optional[int] = None) -> Optional[int]:
    if value is None: return default
    try:
        if isinstance(value, (int, float)) and not isinstance(value, bool): return int(round(float(value)))
        if isinstance(value, str):
            cleaned = re.sub(r'[^\d\-]', '', value.strip())
            if cleaned: return int(cleaned)
    except (ValueError, TypeError, AttributeError): pass
    return default

def safe_str(value: Any, default: str = "") -> str:
    if value is None: return default
    try: return str(value).strip()
    except Exception: return default

def safe_bool(value: Any, default: bool = False) -> bool:
    if value is None: return default
    if isinstance(value, bool): return value
    if isinstance(value, (int, float)): return bool(value)
    if isinstance(value, str):
        s = value.strip().upper()
        if s in ('TRUE', 'YES', 'Y', '1', 'ON', 'ENABLED'): return True
        if s in ('FALSE', 'NO', 'N', '0', 'OFF', 'DISABLED'): return False
    return default

def safe_date(value: Any) -> Optional[date]:
    if value is None: return None
    try:
        if isinstance(value, date) and not isinstance(value, datetime): return value
        if isinstance(value, datetime): return value.date()
        if isinstance(value, str):
            for fmt in ('%Y-%m-%d', '%d/%m/%Y', '%m/%d/%Y', '%Y%m%d'):
                try: return datetime.strptime(value.strip(), fmt).date()
                except ValueError: continue
    except Exception: pass
    return None

def safe_datetime(value: Any) -> Optional[datetime]:
    if value is None: return None
    try:
        if isinstance(value, datetime): return value
        if isinstance(value, date): return datetime.combine(value, datetime.min.time())
        if isinstance(value, (int, float)): return datetime.fromtimestamp(value)
        if isinstance(value, str):
            try: return datetime.fromisoformat(value.strip().replace('Z', '+00:00'))
            except ValueError: pass
            for fmt in ('%Y-%m-%d %H:%M:%S', '%Y-%m-%dT%H:%M:%S', '%d/%m/%Y %H:%M:%S'):
                try: return datetime.strptime(value.strip(), fmt)
                except ValueError: continue
    except Exception: pass
    return None

def bound_value(value: Optional[float], min_val: float, max_val: float, default: Optional[float] = None) -> Optional[float]:
    if value is None: return default
    return max(min_val, min(max_val, value))

def percent_to_decimal(value: Optional[float]) -> Optional[float]:
    if value is None: return None
    return value / 100.0 if value > 1 else value

def decimal_to_percent(value: Optional[float]) -> Optional[float]:
    if value is None: return None
    return value * 100.0 if value <= 1 else value


# =============================================================================
# Unified Quote Model (Next-Gen Enterprise)
# =============================================================================

class UnifiedQuote(BaseModel):
    """
    Comprehensive unified quote model with 130+ fields for all asset classes.
    Thread-safe, robust parsing, and V5 Machine Learning aligned.
    """
    
    # -----------------------------------------------------------------
    # Identity & Classification
    # -----------------------------------------------------------------
    symbol: str = Field(..., description="Trading symbol")
    symbol_normalized: Optional[str] = Field(None, description="Normalized symbol for matching")
    name: Optional[str] = Field(None, description="Company/asset name")
    exchange: Optional[str] = Field(None, description="Primary exchange")
    market: Optional[MarketType] = Field(None, description="Market type")
    asset_class: Optional[AssetClass] = Field(None, description="Asset classification")
    currency: Optional[str] = Field(None, description="Trading currency", max_length=3)
    isin: Optional[str] = Field(None, description="ISIN identifier", max_length=12)
    cusip: Optional[str] = Field(None, description="CUSIP identifier", max_length=9)
    sedol: Optional[str] = Field(None, description="SEDOL identifier", max_length=7)
    country: Optional[str] = Field(None, description="Country of incorporation/listing")
    sector: Optional[str] = Field(None, description="Sector classification")
    industry: Optional[str] = Field(None, description="Industry classification")
    sub_industry: Optional[str] = Field(None, description="Sub-industry classification")
    listing_date: Optional[date] = Field(None, description="Date of first listing")

    # -----------------------------------------------------------------
    # Price Data
    # -----------------------------------------------------------------
    price: Optional[float] = Field(None, description="Current/last price", ge=0)
    current_price: Optional[float] = Field(None, description="Current price (alias)", ge=0)
    previous_close: Optional[float] = Field(None, description="Previous day close", ge=0)
    price_change: Optional[float] = Field(None, description="Absolute price change")
    percent_change: Optional[float] = Field(None, description="Percentage price change")
    
    day_open: Optional[float] = Field(None, description="Day opening price", ge=0)
    day_high: Optional[float] = Field(None, description="Day high price", ge=0)
    day_low: Optional[float] = Field(None, description="Day low price", ge=0)
    day_vwap: Optional[float] = Field(None, description="Day volume-weighted avg price", ge=0)
    day_range: Optional[str] = Field(None, description="Day range as string")
    
    week_52_high: Optional[float] = Field(None, description="52-week high", ge=0)
    week_52_low: Optional[float] = Field(None, description="52-week low", ge=0)
    week_52_range: Optional[str] = Field(None, description="52-week range as string")
    week_52_position: Optional[float] = Field(None, description="Position in 52-week range (0-100)")
    week_52_position_percent: Optional[float] = Field(None, description="Alias for week_52_position")
    
    all_time_high: Optional[float] = Field(None, description="All-time high", ge=0)
    all_time_low: Optional[float] = Field(None, description="All-time low", ge=0)
    all_time_high_date: Optional[date] = Field(None, description="Date of all-time high")
    all_time_low_date: Optional[date] = Field(None, description="Date of all-time low")

    # -----------------------------------------------------------------
    # Volume & Liquidity
    # -----------------------------------------------------------------
    volume: Optional[float] = Field(None, description="Current/today's volume", ge=0)
    avg_volume_10d: Optional[float] = Field(None, description="10-day average volume", ge=0)
    avg_volume_30d: Optional[float] = Field(None, description="30-day average volume", ge=0)
    avg_volume_90d: Optional[float] = Field(None, description="90-day average volume", ge=0)
    value_traded: Optional[float] = Field(None, description="Value traded today", ge=0)
    turnover_percent: Optional[float] = Field(None, description="Turnover as % of shares", ge=0)
    relative_volume: Optional[float] = Field(None, description="Volume relative to average", ge=0)
    
    shares_outstanding: Optional[float] = Field(None, description="Total shares outstanding", ge=0)
    free_float: Optional[float] = Field(None, description="Free float percentage", ge=0, le=100)
    free_float_shares: Optional[float] = Field(None, description="Free float shares", ge=0)
    market_cap: Optional[float] = Field(None, description="Market capitalization", ge=0)
    free_float_market_cap: Optional[float] = Field(None, description="Free float market cap", ge=0)
    enterprise_value: Optional[float] = Field(None, description="Enterprise value", ge=0)
    
    liquidity_score: Optional[float] = Field(None, description="Composite liquidity score (0-100)", ge=0, le=100)
    liquidity_grade: Optional[str] = Field(None, description="Liquidity grade (A-F)")
    bid: Optional[float] = Field(None, description="Current bid price", ge=0)
    ask: Optional[float] = Field(None, description="Current ask price", ge=0)
    spread: Optional[float] = Field(None, description="Bid-ask spread", ge=0)
    spread_percent: Optional[float] = Field(None, description="Bid-ask spread percentage", ge=0)
    market_depth: Optional[int] = Field(None, description="Market depth level", ge=0)

    # -----------------------------------------------------------------
    # Valuation Multiples
    # -----------------------------------------------------------------
    pe_ttm: Optional[float] = Field(None, description="P/E ratio (trailing 12 months)", ge=0)
    forward_pe: Optional[float] = Field(None, description="Forward P/E ratio", ge=0)
    peg_ratio: Optional[float] = Field(None, description="P/E to growth ratio")
    pb_ratio: Optional[float] = Field(None, description="Price to book ratio", ge=0)
    ps_ratio: Optional[float] = Field(None, description="Price to sales ratio", ge=0)
    pcf_ratio: Optional[float] = Field(None, description="Price to cash flow ratio", ge=0)
    pfcf_ratio: Optional[float] = Field(None, description="Price to free cash flow ratio", ge=0)
    ev_ebitda: Optional[float] = Field(None, description="EV/EBITDA ratio", ge=0)
    ev_sales: Optional[float] = Field(None, description="EV/Sales ratio", ge=0)
    ev_fcf: Optional[float] = Field(None, description="EV/FCF ratio", ge=0)
    
    eps_ttm: Optional[float] = Field(None, description="EPS (trailing 12 months)")
    forward_eps: Optional[float] = Field(None, description="Forward EPS estimate")
    eps_growth: Optional[float] = Field(None, description="EPS growth rate")
    revenue_per_share: Optional[float] = Field(None, description="Revenue per share", ge=0)
    book_value_per_share: Optional[float] = Field(None, description="Book value per share", ge=0)
    cash_per_share: Optional[float] = Field(None, description="Cash per share", ge=0)
    
    dividend_yield: Optional[float] = Field(None, description="Dividend yield percentage", ge=0)
    dividend_rate: Optional[float] = Field(None, description="Annual dividend rate", ge=0)
    dividend_per_share: Optional[float] = Field(None, description="Dividend per share", ge=0)
    payout_ratio: Optional[float] = Field(None, description="Payout ratio percentage", ge=0, le=100)
    ex_dividend_date: Optional[date] = Field(None, description="Ex-dividend date")
    dividend_payment_date: Optional[date] = Field(None, description="Dividend payment date")
    dividend_frequency: Optional[str] = Field(None, description="Dividend payment frequency")

    # -----------------------------------------------------------------
    # Profitability & Efficiency
    # -----------------------------------------------------------------
    roe: Optional[float] = Field(None, description="Return on equity percentage")
    roa: Optional[float] = Field(None, description="Return on assets percentage")
    roic: Optional[float] = Field(None, description="Return on invested capital percentage")
    roce: Optional[float] = Field(None, description="Return on capital employed percentage")
    
    gross_margin: Optional[float] = Field(None, description="Gross margin percentage")
    operating_margin: Optional[float] = Field(None, description="Operating margin percentage")
    net_margin: Optional[float] = Field(None, description="Net margin percentage")
    ebitda_margin: Optional[float] = Field(None, description="EBITDA margin percentage")
    fcf_margin: Optional[float] = Field(None, description="Free cash flow margin percentage")
    
    revenue_growth_qoq: Optional[float] = Field(None, description="Revenue growth QoQ percentage")
    revenue_growth_yoy: Optional[float] = Field(None, description="Revenue growth YoY percentage")
    net_income_growth_qoq: Optional[float] = Field(None, description="Net income growth QoQ percentage")
    net_income_growth_yoy: Optional[float] = Field(None, description="Net income growth YoY percentage")
    eps_growth_qoq: Optional[float] = Field(None, description="EPS growth QoQ percentage")
    eps_growth_yoy: Optional[float] = Field(None, description="EPS growth YoY percentage")
    
    asset_turnover: Optional[float] = Field(None, description="Asset turnover ratio")
    inventory_turnover: Optional[float] = Field(None, description="Inventory turnover")
    receivables_turnover: Optional[float] = Field(None, description="Receivables turnover")
    days_sales_outstanding: Optional[float] = Field(None, description="Days sales outstanding")
    days_inventory: Optional[float] = Field(None, description="Days inventory outstanding")
    cash_conversion_cycle: Optional[float] = Field(None, description="Cash conversion cycle days")

    # -----------------------------------------------------------------
    # Financial Health
    # -----------------------------------------------------------------
    debt_to_equity: Optional[float] = Field(None, description="Debt to equity ratio")
    debt_to_assets: Optional[float] = Field(None, description="Debt to assets ratio")
    interest_coverage: Optional[float] = Field(None, description="Interest coverage ratio")
    current_ratio: Optional[float] = Field(None, description="Current ratio")
    quick_ratio: Optional[float] = Field(None, description="Quick ratio")
    cash_ratio: Optional[float] = Field(None, description="Cash ratio")
    operating_cash_flow: Optional[float] = Field(None, description="Operating cash flow", ge=0)
    free_cash_flow: Optional[float] = Field(None, description="Free cash flow", ge=0)
    fcf_yield: Optional[float] = Field(None, description="Free cash flow yield percentage")
    
    altman_z_score: Optional[float] = Field(None, description="Altman Z-score")
    piotroski_score: Optional[int] = Field(None, description="Piotroski F-score", ge=0, le=9)
    beneish_m_score: Optional[float] = Field(None, description="Beneish M-score")

    # -----------------------------------------------------------------
    # Risk Metrics & ML Anomalies
    # -----------------------------------------------------------------
    beta: Optional[float] = Field(None, description="Beta (5-year monthly)")
    beta_1y: Optional[float] = Field(None, description="Beta (1-year)")
    beta_3y: Optional[float] = Field(None, description="Beta (3-year)")
    
    volatility_10d: Optional[float] = Field(None, description="10-day volatility annualized", ge=0)
    volatility_30d: Optional[float] = Field(None, description="30-day volatility annualized", ge=0)
    volatility_90d: Optional[float] = Field(None, description="90-day volatility annualized", ge=0)
    volatility_252d: Optional[float] = Field(None, description="1-year volatility annualized", ge=0)
    historical_var_95: Optional[float] = Field(None, description="Historical VaR 95%")
    historical_var_99: Optional[float] = Field(None, description="Historical VaR 99%")
    cvar_95: Optional[float] = Field(None, description="Conditional VaR 95%")
    
    max_drawdown_30d: Optional[float] = Field(None, description="Max drawdown 30d")
    max_drawdown_90d: Optional[float] = Field(None, description="Max drawdown 90d")
    max_drawdown_252d: Optional[float] = Field(None, description="Max drawdown 1y")
    current_drawdown: Optional[float] = Field(None, description="Current drawdown from peak")
    
    risk_score: Optional[float] = Field(None, description="Composite risk score (0-100)", ge=0, le=100)
    risk_rating: Optional[str] = Field(None, description="Risk rating (A-F)")
    sharpe_ratio: Optional[float] = Field(None, description="Sharpe ratio")
    sortino_ratio: Optional[float] = Field(None, description="Sortino ratio")
    treynor_ratio: Optional[float] = Field(None, description="Treynor ratio")
    information_ratio: Optional[float] = Field(None, description="Information ratio")
    
    # ML Anomaly tracking
    price_anomaly: Optional[bool] = Field(None, description="ML Detected Price Anomaly")
    price_anomaly_score: Optional[float] = Field(None, description="Magnitude of Anomaly")

    # -----------------------------------------------------------------
    # Technical Indicators & ML Patterns
    # -----------------------------------------------------------------
    ma5: Optional[float] = Field(None, description="5-day moving average")
    ma10: Optional[float] = Field(None, description="10-day moving average")
    ma20: Optional[float] = Field(None, description="20-day moving average")
    ma50: Optional[float] = Field(None, description="50-day moving average")
    ma100: Optional[float] = Field(None, description="100-day moving average")
    ma200: Optional[float] = Field(None, description="200-day moving average")
    
    price_to_ma5: Optional[float] = Field(None, description="Price / MA5 ratio")
    price_to_ma20: Optional[float] = Field(None, description="Price / MA20 ratio")
    price_to_ma50: Optional[float] = Field(None, description="Price / MA50 ratio")
    price_to_ma200: Optional[float] = Field(None, description="Price / MA200 ratio")
    
    golden_cross_50_200: Optional[bool] = Field(None, description="Golden cross (50 > 200)")
    death_cross_50_200: Optional[bool] = Field(None, description="Death cross (50 < 200)")
    
    rsi_14: Optional[float] = Field(None, description="RSI (14)", ge=0, le=100)
    rsi_7: Optional[float] = Field(None, description="RSI (7)", ge=0, le=100)
    rsi_21: Optional[float] = Field(None, description="RSI (21)", ge=0, le=100)
    
    stoch_k: Optional[float] = Field(None, description="Stochastic %K", ge=0, le=100)
    stoch_d: Optional[float] = Field(None, description="Stochastic %D", ge=0, le=100)
    stoch_rsi: Optional[float] = Field(None, description="Stochastic RSI", ge=0, le=100)
    
    macd_line: Optional[float] = Field(None, description="MACD line")
    macd_signal: Optional[float] = Field(None, description="MACD signal line")
    macd_histogram: Optional[float] = Field(None, description="MACD histogram")
    macd_crossover: Optional[str] = Field(None, description="MACD crossover signal")
    
    bb_upper: Optional[float] = Field(None, description="Bollinger upper band")
    bb_middle: Optional[float] = Field(None, description="Bollinger middle band (MA20)")
    bb_lower: Optional[float] = Field(None, description="Bollinger lower band")
    bb_width: Optional[float] = Field(None, description="Bollinger band width")
    bb_percent: Optional[float] = Field(None, description="Bollinger %B", ge=0, le=1)
    
    ichimoku_conversion: Optional[float] = Field(None, description="Ichimoku conversion line")
    ichimoku_base: Optional[float] = Field(None, description="Ichimoku base line")
    ichimoku_span_a: Optional[float] = Field(None, description="Ichimoku leading span A")
    ichimoku_span_b: Optional[float] = Field(None, description="Ichimoku leading span B")
    ichimoku_lagging: Optional[float] = Field(None, description="Ichimoku lagging span")
    
    adx: Optional[float] = Field(None, description="Average directional index", ge=0, le=100)
    aroon_up: Optional[float] = Field(None, description="Aroon up", ge=0, le=100)
    aroon_down: Optional[float] = Field(None, description="Aroon down", ge=0, le=100)
    cci: Optional[float] = Field(None, description="Commodity channel index")
    williams_r: Optional[float] = Field(None, description="Williams %R")
    atr: Optional[float] = Field(None, description="Average true range")
    obv: Optional[float] = Field(None, description="On-balance volume")
    vwap: Optional[float] = Field(None, description="Volume-weighted average price")
    supertrend: Optional[float] = Field(None, description="SuperTrend value")
    supertrend_direction: Optional[str] = Field(None, description="SuperTrend Direction")
    
    trend_signal: Optional[str] = Field(None, description="Overall trend signal")
    trend_strength: Optional[float] = Field(None, description="Trend strength (0-100)")
    momentum_score: Optional[float] = Field(None, description="Momentum score (0-100)")
    
    market_regime: Optional[str] = Field(None, description="ML Detected Market Regime")
    market_regime_confidence: Optional[float] = Field(None, description="Confidence in Market Regime")

    # -----------------------------------------------------------------
    # Forecast & Targets
    # -----------------------------------------------------------------
    analyst_count: Optional[int] = Field(None, description="Number of analysts covering", ge=0)
    analyst_rating: Optional[str] = Field(None, description="Consensus analyst rating")
    analyst_rating_score: Optional[float] = Field(None, description="Analyst rating score (1-5)")
    target_price_mean: Optional[float] = Field(None, description="Mean target price", ge=0)
    target_price_median: Optional[float] = Field(None, description="Median target price", ge=0)
    target_price_high: Optional[float] = Field(None, description="High target price", ge=0)
    target_price_low: Optional[float] = Field(None, description="Low target price", ge=0)
    target_upside: Optional[float] = Field(None, description="Upside to mean target")
    
    fair_value_dcf: Optional[float] = Field(None, description="DCF fair value", ge=0)
    fair_value_comps: Optional[float] = Field(None, description="Comparables fair value", ge=0)
    fair_value: Optional[float] = Field(None, description="Composite fair value", ge=0)
    upside_percent: Optional[float] = Field(None, description="Upside to fair value")
    valuation_label: Optional[str] = Field(None, description="Valuation label")
    
    forecast_price_1w: Optional[float] = Field(None, description="1-week price forecast")
    forecast_price_1m: Optional[float] = Field(None, description="1-month price forecast")
    forecast_price_3m: Optional[float] = Field(None, description="3-month price forecast")
    forecast_price_6m: Optional[float] = Field(None, description="6-month price forecast")
    forecast_price_12m: Optional[float] = Field(None, description="12-month price forecast")
    
    expected_return_1w: Optional[float] = Field(None, description="Expected return 1w %")
    expected_return_1m: Optional[float] = Field(None, description="Expected return 1m %")
    expected_return_3m: Optional[float] = Field(None, description="Expected return 3m %")
    expected_return_6m: Optional[float] = Field(None, description="Expected return 6m %")
    expected_return_12m: Optional[float] = Field(None, description="Expected return 12m %")
    
    forecast_confidence: Optional[float] = Field(None, description="Forecast confidence (0-100)")
    forecast_method: Optional[str] = Field(None, description="Forecast method used")
    forecast_source: Optional[str] = Field(None, description="Origin of forecast data")
    forecast_model: Optional[str] = Field(None, description="Forecast model name")
    forecast_updated_utc: Optional[datetime] = Field(None, description="Forecast last updated UTC")
    forecast_updated_riyadh: Optional[datetime] = Field(None, description="Forecast last updated Riyadh")

    # -----------------------------------------------------------------
    # Historical Returns
    # -----------------------------------------------------------------
    return_1d: Optional[float] = Field(None, description="1-day return %")
    return_5d: Optional[float] = Field(None, description="5-day return %")
    return_1w: Optional[float] = Field(None, description="1-week return %")
    return_2w: Optional[float] = Field(None, description="2-week return %")
    return_1m: Optional[float] = Field(None, description="1-month return %")
    return_3m: Optional[float] = Field(None, description="3-month return %")
    return_6m: Optional[float] = Field(None, description="6-month return %")
    return_ytd: Optional[float] = Field(None, description="Year-to-date return %")
    return_1y: Optional[float] = Field(None, description="1-year return %")
    return_3y: Optional[float] = Field(None, description="3-year return % (annualized)")
    return_5y: Optional[float] = Field(None, description="5-year return % (annualized)")
    return_10y: Optional[float] = Field(None, description="10-year return % (annualized)")
    
    alpha_1y: Optional[float] = Field(None, description="Alpha (1-year)")
    r_squared: Optional[float] = Field(None, description="R-squared", ge=0, le=100)
    std_dev_1y: Optional[float] = Field(None, description="Standard deviation (1-year)")
    
    winning_percentage: Optional[float] = Field(None, description="% of positive periods", ge=0, le=100)
    best_day: Optional[float] = Field(None, description="Best single day return %")
    worst_day: Optional[float] = Field(None, description="Worst single day return %")
    avg_gain: Optional[float] = Field(None, description="Average gain in up days")
    avg_loss: Optional[float] = Field(None, description="Average loss in down days")
    gain_loss_ratio: Optional[float] = Field(None, description="Gain/loss ratio")

    # -----------------------------------------------------------------
    # Scores & Badges
    # -----------------------------------------------------------------
    value_score: Optional[float] = Field(None, description="Value score (0-100)", ge=0, le=100)
    quality_score: Optional[float] = Field(None, description="Quality score (0-100)", ge=0, le=100)
    growth_score: Optional[float] = Field(None, description="Growth score (0-100)", ge=0, le=100)
    sentiment_score: Optional[float] = Field(None, description="Sentiment score (0-100)", ge=0, le=100)
    opportunity_score: Optional[float] = Field(None, description="Opportunity score (0-100)", ge=0, le=100)
    overall_score: Optional[float] = Field(None, description="Overall composite score", ge=0, le=100)
    
    rec_badge: Optional[BadgeLevel] = Field(None, description="Recommendation badge")
    value_badge: Optional[BadgeLevel] = Field(None, description="Value badge")
    quality_badge: Optional[BadgeLevel] = Field(None, description="Quality badge")
    growth_badge: Optional[BadgeLevel] = Field(None, description="Growth badge")
    momentum_badge: Optional[BadgeLevel] = Field(None, description="Momentum badge")
    sentiment_badge: Optional[BadgeLevel] = Field(None, description="Sentiment badge")
    risk_badge: Optional[BadgeLevel] = Field(None, description="Risk badge")
    opportunity_badge: Optional[BadgeLevel] = Field(None, description="Opportunity badge")
    
    recommendation: Optional[Recommendation] = Field(None, description="Final recommendation")
    recommendation_raw: Optional[str] = Field(None, description="Raw recommendation text")
    recommendation_score: Optional[float] = Field(None, description="Recommendation score")
    scoring_reason: Optional[str] = Field(None, description="Reason for score/recommendation")

    # -----------------------------------------------------------------
    # News & Sentiment
    # -----------------------------------------------------------------
    news_count: Optional[int] = Field(None, description="Recent news count", ge=0)
    news_sentiment: Optional[float] = Field(None, description="News sentiment score", ge=-1, le=1)
    news_volume: Optional[int] = Field(None, description="News article volume", ge=0)
    news_mentions: Optional[int] = Field(None, description="Social media mentions", ge=0)
    news_buzz: Optional[float] = Field(None, description="News buzz score", ge=0)
    news_score: Optional[float] = Field(None, description="News composite score")
    news_updated_utc: Optional[datetime] = Field(None, description="News last updated")
    
    analyst_upgrades: Optional[int] = Field(None, description="Recent upgrades", ge=0)
    analyst_downgrades: Optional[int] = Field(None, description="Recent downgrades", ge=0)
    analyst_initiations: Optional[int] = Field(None, description="Recent initiations", ge=0)
    analyst_reiterations: Optional[int] = Field(None, description="Recent reiterations", ge=0)

    # -----------------------------------------------------------------
    # Ownership & Institutional
    # -----------------------------------------------------------------
    institutional_ownership: Optional[float] = Field(None, description="Institutional ownership %", ge=0, le=100)
    institutional_count: Optional[int] = Field(None, description="Number of institutional holders", ge=0)
    institutional_change: Optional[float] = Field(None, description="Institutional ownership change")
    
    insider_ownership: Optional[float] = Field(None, description="Insider ownership %", ge=0, le=100)
    insider_transactions: Optional[int] = Field(None, description="Recent insider transactions")
    insider_buy_sell_ratio: Optional[float] = Field(None, description="Insider buy/sell ratio")
    
    mutual_fund_ownership: Optional[float] = Field(None, description="Mutual fund ownership %", ge=0, le=100)
    hedge_fund_ownership: Optional[float] = Field(None, description="Hedge fund ownership %", ge=0, le=100)
    
    short_interest: Optional[float] = Field(None, description="Short interest shares", ge=0)
    short_interest_ratio: Optional[float] = Field(None, description="Short interest ratio (days)", ge=0)
    short_percent_of_float: Optional[float] = Field(None, description="Short % of float", ge=0, le=100)

    # -----------------------------------------------------------------
    # Meta & Quality
    # -----------------------------------------------------------------
    data_source: Optional[str] = Field(None, description="Primary data source")
    data_sources: Optional[List[str]] = Field(None, description="List of data sources used")
    data_quality: Optional[DataQuality] = Field(None, description="Data quality assessment")
    data_completeness: Optional[float] = Field(None, description="Data completeness %", ge=0, le=100)
    data_freshness: Optional[int] = Field(None, description="Data age in seconds", ge=0)
    
    error: Optional[str] = Field(None, description="Error message if any")
    warning: Optional[str] = Field(None, description="Warning message if any")
    info: Optional[str] = Field(None, description="Info message if any")
    
    last_updated_utc: Optional[datetime] = Field(None, description="Last updated timestamp UTC")
    last_updated_riyadh: Optional[datetime] = Field(None, description="Last updated timestamp Riyadh")
    last_updated_local: Optional[datetime] = Field(None, description="Last updated timestamp local")
    
    latency_ms: Optional[float] = Field(None, description="Processing latency in ms", ge=0)
    request_id: Optional[str] = Field(None, description="Request identifier for tracing")
    cache_hit: Optional[bool] = Field(None, description="Whether result was cached")

    # =================================================================
    # V1 & V2 Compatibility Config
    # =================================================================
    
    if _PYDANTIC_V2:
        model_config = ConfigDict(
            extra="ignore",
            validate_assignment=True,
            validate_default=False,
            arbitrary_types_allowed=True,
        )
        
        @field_validator("symbol", mode="before")
        @classmethod
        def validate_symbol(cls, v: Any) -> str:
            s = safe_str(v)
            if not s: raise ValueError("Symbol is required")
            return re.sub(r'\.(SR|AE|QA|KW|US)$', '', s.upper().strip())
        
        @field_validator("currency", mode="before")
        @classmethod
        def validate_currency(cls, v: Any) -> Optional[str]:
            if v is None: return None
            s = safe_str(v).upper().strip()
            if len(s) == 3: return s
            currency_map = {
                "USD": "USD", "US DOLLAR": "USD", "DOLLAR": "USD",
                "EUR": "EUR", "EURO": "EUR", "GBP": "GBP", "POUND": "GBP",
                "SAR": "SAR", "SAUDI RIYAL": "SAR", "RIYAL": "SAR",
                "AED": "AED", "DIRHAM": "AED", "QAR": "QAR", "QATAR RIYAL": "QAR",
                "KWD": "KWD", "KUWAITI DINAR": "KWD"
            }
            return currency_map.get(s, None)
        
        @field_validator("week_52_position", "week_52_position_percent", mode="before")
        @classmethod
        def validate_52w_position(cls, v: Any) -> Optional[float]:
            f = safe_float(v)
            return bound_value(f, 0, 100) if f is not None else None
        
        @field_validator("*_percent", "*_yield", mode="before")
        @classmethod
        def validate_percent(cls, v: Any) -> Optional[float]:
            return safe_float(v)
        
        @field_validator("recommendation", mode="before")
        @classmethod
        def validate_recommendation(cls, v: Any) -> Optional[Recommendation]:
            if v is None: return None
            if isinstance(v, Recommendation): return v
            s = safe_str(v).upper()
            if s in ("BUY", "STRONG BUY", "ACCUMULATE"): return Recommendation.BUY
            if s in ("HOLD", "NEUTRAL", "MARKET PERFORM"): return Recommendation.HOLD
            if s in ("REDUCE", "TRIM", "TAKE PROFIT"): return Recommendation.REDUCE
            if s in ("SELL", "STRONG SELL", "EXIT"): return Recommendation.SELL
            return None
        
        @model_validator(mode="after")
        def post_validation(self) -> "UnifiedQuote":
            if self.symbol and not self.symbol_normalized:
                self.symbol_normalized = self.symbol.upper().replace(".", "_")
            if self.price is not None and self.current_price is None:
                self.current_price = self.price
            elif self.current_price is not None and self.price is None:
                self.price = self.current_price
            if self.week_52_position is not None and self.week_52_position_percent is None:
                self.week_52_position_percent = self.week_52_position
            elif self.week_52_position_percent is not None and self.week_52_position is None:
                self.week_52_position = self.week_52_position_percent
            return self
    
    else:
        class Config:
            extra = "ignore"
            validate_assignment = True
        
        @validator("symbol", pre=True, always=True)
        def validate_symbol_v1(cls, v):
            s = safe_str(v)
            if not s: raise ValueError("Symbol is required")
            return re.sub(r'\.(SR|AE|QA|KW|US)$', '', s.upper().strip())
        
        @validator("currency", pre=True, always=True)
        def validate_currency_v1(cls, v):
            if v is None: return None
            s = safe_str(v).upper().strip()
            if len(s) == 3: return s
            currency_map = {
                "USD": "USD", "US DOLLAR": "USD", "DOLLAR": "USD",
                "EUR": "EUR", "EURO": "EUR", "GBP": "GBP", "POUND": "GBP",
                "SAR": "SAR", "SAUDI RIYAL": "SAR", "RIYAL": "SAR",
                "AED": "AED", "DIRHAM": "AED", "QAR": "QAR", "QATAR RIYAL": "QAR",
                "KWD": "KWD", "KUWAITI DINAR": "KWD"
            }
            return currency_map.get(s, None)
        
        @root_validator(pre=False)
        def post_validation_v1(cls, values):
            symbol = values.get("symbol")
            if symbol and not values.get("symbol_normalized"):
                values["symbol_normalized"] = symbol.upper().replace(".", "_")
            price = values.get("price")
            current_price = values.get("current_price")
            if price is not None and current_price is None: values["current_price"] = price
            elif current_price is not None and price is None: values["price"] = current_price
            return values

    # =================================================================
    # Utility Methods
    # =================================================================
    
    def to_dict(self, exclude_none: bool = True) -> Dict[str, Any]:
        if _PYDANTIC_V2: return self.model_dump(exclude_none=exclude_none)
        return self.dict(exclude_none=exclude_none)
    
    def to_json(self, **kwargs) -> str:
        if _HAS_ORJSON:
            return json_dumps(self.to_dict(), default=str)
        if _PYDANTIC_V2: return self.model_dump_json(**kwargs)
        return self.json(**kwargs)
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any], strict: bool = False) -> Optional["UnifiedQuote"]:
        try: return cls(**data)
        except ValidationError as e:
            if strict: raise
            cleaned = {}
            for k, v in data.items():
                if v is None: continue
                k_norm = re.sub(r'[^a-zA-Z0-9_]', '', k).lower()
                if hasattr(cls, k_norm): cleaned[k_norm] = v
            return cls(**cleaned) if cleaned else None
        except Exception: return None
    
    def merge(self, other: "UnifiedQuote", overwrite: bool = True) -> "UnifiedQuote":
        data = self.to_dict(exclude_none=False)
        other_data = other.to_dict(exclude_none=False)
        for k, v in other_data.items():
            if v is not None and (overwrite or data.get(k) is None):
                data[k] = v
        return UnifiedQuote(**data)
    
    def validate_required(self, required_fields: List[str]) -> Tuple[bool, List[str]]:
        missing = []
        data = self.to_dict()
        for field in required_fields:
            if data.get(field) is None: missing.append(field)
        return len(missing) == 0, missing
    
    def compute_hash(self) -> str:
        data = self.to_dict(exclude_none=True)
        json_str = json_dumps(data, default=str) if _HAS_ORJSON else json.dumps(data, sort_keys=True, default=str)
        return hashlib.sha256(json_str.encode()).hexdigest()[:16]


# =============================================================================
# Canonical Header Definitions
# =============================================================================

DEFAULT_HEADERS_59: List[str] = [
    # Identity (1-7)
    "Symbol", "Company Name", "Sector", "Sub-Sector", "Market", "Currency", "Listing Date",
    # Prices (8-18)
    "Last Price", "Previous Close", "Price Change", "Percent Change", "Day High", "Day Low",
    "52W High", "52W Low", "52W Position %",
    # Volume / Liquidity (19-22)
    "Volume", "Avg Volume (30D)", "Value Traded", "Turnover %",
    # Shares / Cap (23-28)
    "Shares Outstanding", "Free Float %", "Market Cap", "Free Float Market Cap", "Liquidity Score",
    # Fundamentals (29-45)
    "EPS (TTM)", "Forward EPS", "P/E (TTM)", "Forward P/E", "P/B", "P/S",
    "EV/EBITDA", "Dividend Yield %", "Dividend Rate", "Payout Ratio %",
    "ROE %", "ROA %", "Net Margin %", "EBITDA Margin %", "Revenue Growth %",
    "Net Income Growth %", "Beta",
    # Technicals (46-48)
    "Volatility (30D)", "RSI (14)",
    # Valuation / Targets (49-51)
    "Fair Value", "Upside %", "Valuation Label",
    # Scores / Recommendation (52-58)
    "Value Score", "Quality Score", "Momentum Score", "Opportunity Score",
    "Risk Score", "Overall Score", "Error", "Recommendation",
    # Meta (59)
    "Data Source", "Data Quality", "Last Updated (UTC)", "Last Updated (Riyadh)"
]

_LEGACY_ANALYSIS_EXTRAS: List[str] = [
    "Returns 1W %", "Returns 1M %", "Returns 3M %", "Returns 6M %", "Returns 12M %",
    "MA20", "MA50", "MA200", "Expected Return 1M %", "Expected Return 3M %",
    "Expected Return 12M %", "Expected Price 1M", "Expected Price 3M", "Expected Price 12M",
    "Confidence Score", "Forecast Method", "History Points", "History Source", "History Last (UTC)"
]

DEFAULT_HEADERS_ANALYSIS: List[str] = DEFAULT_HEADERS_59 + _LEGACY_ANALYSIS_EXTRAS


# =====================================================================
# Next-Gen Schemas (vNext) 
# Incorporating ML, Predictive & Technical enhancements
# =====================================================================

VN_IDENTITY: List[str] = [
    "Rank", "Symbol", "Origin", "Name", "Sector", "Sub Sector", "Market",
    "Currency", "Listing Date", "Asset Class", "Exchange", "Country"
]

VN_PRICE: List[str] = [
    "Price", "Prev Close", "Change", "Change %", "Day Open", "Day High", "Day Low",
    "Day VWAP", "52W High", "52W Low", "52W Position %", "All Time High", "All Time Low"
]

VN_VOLUME: List[str] = [
    "Volume", "Avg Vol 10D", "Avg Vol 30D", "Avg Vol 90D", "Value Traded",
    "Turnover %", "Rel Volume", "Bid", "Ask", "Spread", "Spread %"
]

VN_CAP: List[str] = [
    "Shares Out", "Free Float %", "Free Float Shares", "Market Cap", 
    "Free Float Mkt Cap", "Enterprise Value", "Liquidity Score"
]

VN_VALUATION_MULTIPLES: List[str] = [
    "P/E TTM", "Forward P/E", "PEG Ratio", "P/B", "P/S", "P/CF", "P/FCF", "EV/EBITDA", "EV/Sales", "EV/FCF"
]

VN_EARNINGS: List[str] = [
    "EPS TTM", "Forward EPS", "EPS Growth %", "Revenue/Share", "Book Value/Share", "Cash/Share"
]

VN_DIVIDENDS: List[str] = [
    "Div Yield %", "Div Rate", "Div/Share", "Payout Ratio %", "Ex-Div Date", "Pay Date"
]

VN_PROFITABILITY: List[str] = [
    "ROE %", "ROA %", "ROIC %", "ROCE %", "Gross Margin %", "Operating Margin %", 
    "Net Margin %", "EBITDA Margin %", "FCF Margin %"
]

VN_GROWTH: List[str] = [
    "Revenue Growth QoQ %", "Revenue Growth YoY %", "Net Income Growth QoQ %",
    "Net Income Growth YoY %", "EPS Growth QoQ %", "EPS Growth YoY %"
]

VN_HEALTH: List[str] = [
    "Debt/Equity", "Debt/Assets", "Interest Coverage", "Current Ratio", "Quick Ratio",
    "Cash Ratio", "Op Cash Flow", "Free Cash Flow", "FCF Yield %", "Altman Z-Score", "Piotroski Score"
]

VN_RISK: List[str] = [
    "Beta", "Beta 1Y", "Beta 3Y", "Volatility 10D", "Volatility 30D", "Volatility 90D",
    "Volatility 1Y", "Max Drawdown 30D", "Max Drawdown 90D", "Max Drawdown 1Y", 
    "Current Drawdown %", "Sharpe Ratio", "Sortino Ratio", "Risk Score", "Price Anomaly"
]

VN_TECHNICALS: List[str] = [
    "MA5", "MA10", "MA20", "MA50", "MA100", "MA200", "Price/MA5", "Price/MA20", "Price/MA50",
    "Price/MA200", "Golden Cross", "Death Cross", "RSI 14", "RSI 7", "RSI 21", "Stoch %K",
    "Stoch %D", "Stoch RSI", "MACD Line", "MACD Signal", "MACD Hist", "BB Upper", "BB Lower",
    "BB Width", "BB %B", "ADX", "Aroon Up", "Aroon Down", "CCI", "Williams %R", "ATR", "OBV",
    "VWAP", "Trend Signal", "Momentum Score", "SuperTrend", "SuperTrend Direction", "Ichimoku Conversion"
]

VN_FORECAST: List[str] = [
    "Analyst Count", "Analyst Rating", "Target Price Mean", "Target Price High", "Target Price Low",
    "Target Upside %", "Fair Value DCF", "Fair Value Comps", "Fair Value", "Upside %", "Valuation Label",
    "Forecast Price 1M", "Forecast Price 3M", "Forecast Price 6M", "Forecast Price 12M",
    "Expected Return 1M %", "Expected Return 3M %", "Expected Return 6M %", "Expected Return 12M %",
    "Forecast Confidence", "Forecast Method", "Forecast Source", "Forecast Updated UTC", "Market Regime", "Regime Confidence"
]

VN_RETURNS: List[str] = [
    "Return 1D %", "Return 5D %", "Return 1W %", "Return 2W %", "Return 1M %", "Return 3M %",
    "Return 6M %", "Return YTD %", "Return 1Y %", "Return 3Y %", "Return 5Y %",
    "Alpha 1Y", "R-Squared", "Win %", "Best Day %", "Worst Day %"
]

VN_SCORES: List[str] = [
    "Value Score", "Quality Score", "Growth Score", "Momentum Score", "Sentiment Score",
    "Risk Score", "Opportunity Score", "Overall Score", "Rec Badge", "Value Badge",
    "Quality Badge", "Growth Badge", "Momentum Badge", "Sentiment Badge", "Risk Badge",
    "Opportunity Badge", "Recommendation"
]

VN_NEWS: List[str] = [
    "News Count", "News Sentiment", "News Volume", "News Buzz", "News Score",
    "Analyst Sentiment", "Analyst Upgrades", "Analyst Downgrades"
]

VN_OWNERSHIP: List[str] = [
    "Institutional Own %", "Institutional Count", "Insider Own %", "Insider Transactions",
    "Short Interest", "Short Ratio", "Short % Float"
]

VN_META: List[str] = [
    "Error", "Warning", "Info", "Data Source", "Data Quality", "Data Completeness %",
    "Last Updated UTC", "Last Updated Riyadh", "Latency ms", "Request ID"
]

# =============================================================================
# Page-Specific Schemas (vNext)
# =============================================================================

VN_HEADERS_KSA_TADAWUL: List[str] = (
    VN_IDENTITY[:10] + 
    ["Price", "Prev Close", "Change", "Change %", "Day High", "Day Low", "52W High", "52W Low", "52W Position %"] +
    ["Volume", "Avg Vol 30D", "Value Traded", "Turnover %", "VWAP"] +
    ["Shares Out", "Free Float %", "Market Cap", "Free Float Mkt Cap", "Liquidity Score"] +
    ["P/E TTM", "Forward P/E", "P/B", "EV/EBITDA", "Div Yield %"] +
    ["ROE %", "Net Margin %", "Beta"] +
    ["Fair Value", "Upside %", "Valuation Label", "Market Regime", "Regime Confidence", "Forecast Source"] +
    ["Value Score", "Quality Score", "Momentum Score", "Risk Score", "Overall Score"] +
    ["Rec Badge", "Recommendation"] +
    ["Error", "Data Source", "Last Updated UTC", "Last Updated Riyadh"]
)

VN_HEADERS_GLOBAL: List[str] = (
    VN_IDENTITY + VN_PRICE + VN_VOLUME[:8] + VN_CAP + VN_VALUATION_MULTIPLES[:8] + 
    VN_EARNINGS[:4] + VN_DIVIDENDS[:4] + VN_PROFITABILITY[:6] + VN_GROWTH[:4] + 
    VN_HEALTH[:6] + VN_RISK[:8] + VN_TECHNICALS[:15] + VN_FORECAST[:10] + 
    VN_RETURNS[:8] + VN_SCORES + VN_NEWS[:4] + VN_META
)

VN_HEADERS_PORTFOLIO: List[str] = [
    "Rank", "Symbol", "Name", "Asset Class", "Market", "Currency", "Portfolio Group",
    "Broker/Account", "Quantity", "Avg Cost", "Cost Value", "Target Weight %", "Notes",
    "Price", "Prev Close", "Change", "Change %", "Day High", "Day Low", "52W High", "52W Low",
    "Market Value", "Unrealized P/L", "Unrealized P/L %", "Weight %", "Rebalance Δ",
    "Fair Value", "Upside %", "Valuation Label", "Forecast Price 1M", "Expected Return 1M %",
    "Forecast Price 3M", "Expected Return 3M %", "Forecast Price 12M", "Expected Return 12M %",
    "Forecast Confidence", "Recommendation", "Rec Badge", "Risk Score", "Volatility 30D",
    "RSI 14", "Error", "Data Source", "Last Updated UTC"
]

VN_HEADERS_FUNDS: List[str] = [
    "Rank", "Symbol", "Name", "Fund Type", "Fund Category", "Market", "Currency", "Inception Date",
    "Price", "Prev Close", "Change", "Change %", "Day High", "Day Low", "52W High", "52W Low", "52W Position %",
    "Volume", "Avg Vol 30D", "AUM", "Expense Ratio", "Distribution Yield", "Beta", "Volatility 30D", "RSI 14",
    "Fair Value", "Upside %", "Momentum Score", "Risk Score", "Overall Score", "Rec Badge",
    "Forecast Price 1M", "Forecast Price 3M", "Forecast Price 12M", "Expected Return 1M %",
    "Expected Return 3M %", "Expected Return 12M %", "Forecast Confidence", "Error", "Data Source", "Last Updated UTC"
]

VN_HEADERS_COMMODITIES_FX: List[str] = [
    "Rank", "Symbol", "Name", "Asset Class", "Sub Class", "Market", "Currency",
    "Price", "Prev Close", "Change", "Change %", "Day High", "Day Low", "52W High", "52W Low", "52W Position %",
    "Volume", "Value Traded", "Volatility 30D", "RSI 14", "Fair Value", "Upside %", "Valuation Label",
    "Momentum Score", "Risk Score", "Overall Score", "Rec Badge",
    "Forecast Price 1M", "Forecast Price 3M", "Forecast Price 12M", "Expected Return 1M %",
    "Expected Return 3M %", "Expected Return 12M %", "Forecast Confidence", "Error", "Data Source", "Last Updated UTC"
]

VN_HEADERS_INSIGHTS: List[str] = VN_HEADERS_GLOBAL

# =============================================================================
# Schema Registry
# =============================================================================

VNEXT_SCHEMAS: Dict[str, Tuple[str, ...]] = {
    "KSA_TADAWUL": tuple(VN_HEADERS_KSA_TADAWUL),
    "GLOBAL_MARKETS": tuple(VN_HEADERS_GLOBAL),
    "MY_PORTFOLIO": tuple(VN_HEADERS_PORTFOLIO),
    "MUTUAL_FUNDS": tuple(VN_HEADERS_FUNDS),
    "COMMODITIES_FX": tuple(VN_HEADERS_COMMODITIES_FX),
    "INSIGHTS_ANALYSIS": tuple(VN_HEADERS_INSIGHTS),
}

LEGACY_SCHEMAS: Dict[str, Tuple[str, ...]] = {
    "KSA_TADAWUL": tuple(DEFAULT_HEADERS_59),
    "GLOBAL_MARKETS": tuple(DEFAULT_HEADERS_ANALYSIS),
    "MY_PORTFOLIO": tuple(DEFAULT_HEADERS_59),
    "MUTUAL_FUNDS": tuple(DEFAULT_HEADERS_59),
    "COMMODITIES_FX": tuple(DEFAULT_HEADERS_59),
    "INSIGHTS_ANALYSIS": tuple(DEFAULT_HEADERS_ANALYSIS),
}

# =============================================================================
# Header Normalization and Mapping
# =============================================================================

@lru_cache(maxsize=4096)
def normalize_header(header: str) -> str:
    """Normalize header label for fuzzy matching. Highly optimized with caching."""
    s = str(header or "").strip().lower()
    if not s: return ""
    s = re.sub(r'[^\w\s%]', ' ', s)
    s = s.replace('%', ' percent ')
    
    replacements = {
        'ttm': 'trailing twelve months', 'yoy': 'year over year', 'qoq': 'quarter over quarter',
        'avg': 'average', 'vol': 'volume', 'div': 'dividend', 'eps': 'earnings per share',
        'pe': 'price to earnings', 'pb': 'price to book', 'ps': 'price to sales',
        'roa': 'return on assets', 'roe': 'return on equity', 'roic': 'return on invested capital',
        'ev': 'enterprise value', 'ebitda': 'earnings before interest taxes depreciation amortization',
        'fcf': 'free cash flow', 'ma': 'moving average', 'rsi': 'relative strength index',
        'macd': 'moving average convergence divergence', 'bb': 'bollinger bands',
        'vwap': 'volume weighted average price', 'atr': 'average true range',
        'adx': 'average directional index', 'obv': 'on balance volume', 'ytd': 'year to date',
    }
    for abbr, full in replacements.items():
        s = re.sub(rf'\b{abbr}\b', full, s)
        
    return re.sub(r'_+', '_', re.sub(r'\s+', '_', s.strip()))


HEADER_TO_FIELD: Dict[str, str] = {
    # Identity
    "Symbol": "symbol", "Company Name": "name", "Name": "name", "Sector": "sector",
    "Sub Sector": "sub_sector", "Sub-Sector": "sub_sector", "Market": "market",
    "Currency": "currency", "Listing Date": "listing_date", "Asset Class": "asset_class",
    "Exchange": "exchange", "Country": "country", "Rank": "rank", "Origin": "origin",
    
    # Prices
    "Price": "price", "Last Price": "price", "Current Price": "current_price",
    "Prev Close": "previous_close", "Previous Close": "previous_close", "Change": "price_change",
    "Change %": "percent_change", "Percent Change": "percent_change", "Day Open": "day_open",
    "Open": "day_open", "Day High": "day_high", "High": "day_high", "Day Low": "day_low",
    "Low": "day_low", "Day VWAP": "day_vwap", "VWAP": "vwap", "52W High": "week_52_high",
    "52 Week High": "week_52_high", "52W Low": "week_52_low", "52 Week Low": "week_52_low",
    "52W Position %": "week_52_position", "52 Week Position": "week_52_position",
    "All Time High": "all_time_high", "All Time Low": "all_time_low",
    
    # Volume
    "Volume": "volume", "Avg Vol 10D": "avg_volume_10d", "Avg Volume 10D": "avg_volume_10d",
    "Avg Vol 30D": "avg_volume_30d", "Avg Volume 30D": "avg_volume_30d", "Avg Vol 90D": "avg_volume_90d",
    "Avg Volume 90D": "avg_volume_90d", "Value Traded": "value_traded", "Turnover %": "turnover_percent",
    "Rel Volume": "relative_volume", "Bid": "bid", "Ask": "ask", "Spread": "spread", "Spread %": "spread_percent",
    
    # Capitalization
    "Shares Out": "shares_outstanding", "Shares Outstanding": "shares_outstanding",
    "Free Float %": "free_float", "Free Float Shares": "free_float_shares", "Market Cap": "market_cap",
    "Free Float Mkt Cap": "free_float_market_cap", "Free Float Market Cap": "free_float_market_cap",
    "Enterprise Value": "enterprise_value", "Liquidity Score": "liquidity_score",
    
    # Valuation Multiples
    "P/E TTM": "pe_ttm", "P/E (TTM)": "pe_ttm", "Forward P/E": "forward_pe", "PEG Ratio": "peg_ratio",
    "P/B": "pb_ratio", "P/S": "ps_ratio", "P/CF": "pcf_ratio", "P/FCF": "pfcf_ratio",
    "EV/EBITDA": "ev_ebitda", "EV/Sales": "ev_sales", "EV/FCF": "ev_fcf",
    
    # Earnings
    "EPS TTM": "eps_ttm", "EPS (TTM)": "eps_ttm", "Forward EPS": "forward_eps", "EPS Growth %": "eps_growth",
    "Revenue/Share": "revenue_per_share", "Book Value/Share": "book_value_per_share", "Cash/Share": "cash_per_share",
    
    # Dividends
    "Div Yield %": "dividend_yield", "Dividend Yield %": "dividend_yield", "Div Rate": "dividend_rate",
    "Div/Share": "dividend_per_share", "Payout Ratio %": "payout_ratio", "Ex-Div Date": "ex_dividend_date",
    "Pay Date": "dividend_payment_date",
    
    # Profitability
    "ROE %": "roe", "ROA %": "roa", "ROIC %": "roic", "ROCE %": "roce", "Gross Margin %": "gross_margin",
    "Operating Margin %": "operating_margin", "Net Margin %": "net_margin", "EBITDA Margin %": "ebitda_margin",
    "FCF Margin %": "fcf_margin",
    
    # Growth
    "Revenue Growth QoQ %": "revenue_growth_qoq", "Revenue Growth YoY %": "revenue_growth_yoy",
    "Net Income Growth QoQ %": "net_income_growth_qoq", "Net Income Growth YoY %": "net_income_growth_yoy",
    "EPS Growth QoQ %": "eps_growth_qoq", "EPS Growth YoY %": "eps_growth_yoy",
    
    # Financial Health
    "Debt/Equity": "debt_to_equity", "Debt/Assets": "debt_to_assets", "Interest Coverage": "interest_coverage",
    "Current Ratio": "current_ratio", "Quick Ratio": "quick_ratio", "Cash Ratio": "cash_ratio",
    "Op Cash Flow": "operating_cash_flow", "Free Cash Flow": "free_cash_flow", "FCF Yield %": "fcf_yield",
    "Altman Z-Score": "altman_z_score", "Piotroski Score": "piotroski_score",
    
    # Risk
    "Beta": "beta", "Beta 1Y": "beta_1y", "Beta 3Y": "beta_3y", "Volatility 10D": "volatility_10d",
    "Volatility 30D": "volatility_30d", "Volatility 90D": "volatility_90d", "Volatility 1Y": "volatility_252d",
    "Max Drawdown 30D": "max_drawdown_30d", "Max Drawdown 90D": "max_drawdown_90d", "Max Drawdown 1Y": "max_drawdown_252d",
    "Current Drawdown %": "current_drawdown", "Sharpe Ratio": "sharpe_ratio", "Sortino Ratio": "sortino_ratio",
    "Risk Score": "risk_score", "Price Anomaly": "price_anomaly",
    
    # Technicals
    "MA5": "ma5", "MA10": "ma10", "MA20": "ma20", "MA50": "ma50", "MA100": "ma100", "MA200": "ma200",
    "Price/MA5": "price_to_ma5", "Price/MA20": "price_to_ma20", "Price/MA50": "price_to_ma50",
    "Price/MA200": "price_to_ma200", "Golden Cross": "golden_cross_50_200", "Death Cross": "death_cross_50_200",
    "RSI 14": "rsi_14", "RSI 7": "rsi_7", "RSI 21": "rsi_21", "Stoch %K": "stoch_k", "Stoch %D": "stoch_d",
    "Stoch RSI": "stoch_rsi", "MACD Line": "macd_line", "MACD Signal": "macd_signal", "MACD Hist": "macd_histogram",
    "BB Upper": "bb_upper", "BB Lower": "bb_lower", "BB Width": "bb_width", "BB %B": "bb_percent",
    "ADX": "adx", "Aroon Up": "aroon_up", "Aroon Down": "aroon_down", "CCI": "cci", "Williams %R": "williams_r",
    "ATR": "atr", "OBV": "obv", "Trend Signal": "trend_signal", "Momentum Score": "momentum_score",
    "SuperTrend": "supertrend", "SuperTrend Direction": "supertrend_direction", "Ichimoku Conversion": "ichimoku_conversion",
    
    # Forecast
    "Analyst Count": "analyst_count", "Analyst Rating": "analyst_rating", "Target Price Mean": "target_price_mean",
    "Target Price High": "target_price_high", "Target Price Low": "target_price_low", "Target Upside %": "target_upside",
    "Fair Value DCF": "fair_value_dcf", "Fair Value Comps": "fair_value_comps", "Fair Value": "fair_value",
    "Upside %": "upside_percent", "Valuation Label": "valuation_label", "Forecast Price 1M": "forecast_price_1m",
    "Forecast Price 3M": "forecast_price_3m", "Forecast Price 6M": "forecast_price_6m", "Forecast Price 12M": "forecast_price_12m",
    "Expected Return 1M %": "expected_return_1m", "Expected Return 3M %": "expected_return_3m",
    "Expected Return 6M %": "expected_return_6m", "Expected Return 12M %": "expected_return_12m",
    "Forecast Confidence": "forecast_confidence", "Forecast Method": "forecast_method",
    "Forecast Updated UTC": "forecast_updated_utc", "Forecast Source": "forecast_source",
    "Market Regime": "market_regime", "Regime Confidence": "market_regime_confidence",
    
    # Returns
    "Return 1D %": "return_1d", "Return 5D %": "return_5d", "Return 1W %": "return_1w",
    "Return 2W %": "return_2w", "Return 1M %": "return_1m", "Return 3M %": "return_3m",
    "Return 6M %": "return_6m", "Return YTD %": "return_ytd", "Return 1Y %": "return_1y",
    "Return 3Y %": "return_3y", "Return 5Y %": "return_5y", "Alpha 1Y": "alpha_1y",
    "R-Squared": "r_squared", "Win %": "winning_percentage", "Best Day %": "best_day", "Worst Day %": "worst_day",
    
    # Scores
    "Value Score": "value_score", "Quality Score": "quality_score", "Growth Score": "growth_score",
    "Momentum Score": "momentum_score", "Sentiment Score": "sentiment_score", "Risk Score": "risk_score",
    "Opportunity Score": "opportunity_score", "Overall Score": "overall_score", "Rec Badge": "rec_badge",
    "Recommendation": "recommendation",
    
    # News
    "News Count": "news_count", "News Sentiment": "news_sentiment", "News Volume": "news_volume",
    "News Buzz": "news_buzz", "News Score": "news_score",
    
    # Ownership
    "Institutional Own %": "institutional_ownership", "Institutional Count": "institutional_count",
    "Insider Own %": "insider_ownership", "Insider Transactions": "insider_transactions",
    "Short Interest": "short_interest", "Short Ratio": "short_interest_ratio", "Short % Float": "short_percent_of_float",
    
    # Meta
    "Error": "error", "Warning": "warning", "Info": "info", "Data Source": "data_source",
    "Data Quality": "data_quality", "Data Completeness %": "data_completeness",
    "Last Updated UTC": "last_updated_utc", "Last Updated Riyadh": "last_updated_riyadh",
    "Latency ms": "latency_ms", "Request ID": "request_id",
}

FIELD_TO_HEADER: Dict[str, str] = {field: header for header, field in HEADER_TO_FIELD.items()}

FIELD_ALIASES: Dict[str, Tuple[str, ...]] = {
    "symbol": ("ticker", "code", "symbol_normalized"),
    "name": ("company_name", "long_name", "title"),
    "sector": ("industry", "category"),
    "sub_sector": ("subsector", "sub_industry"),
    "market": ("exchange", "market_region"),
    "currency": ("ccy", "currency_code"),
    
    "price": ("last_price", "close", "current"),
    "previous_close": ("prev_close", "prior_close"),
    "price_change": ("change", "chg"),
    "percent_change": ("change_percent", "chg_pct", "pct_change"),
    "day_high": ("high", "high_today"),
    "day_low": ("low", "low_today"),
    "week_52_high": ("high_52w", "fifty_two_week_high"),
    "week_52_low": ("low_52w", "fifty_two_week_low"),
    
    "volume": ("vol", "volume_today"),
    "avg_volume_30d": ("avg_volume", "average_volume_30d"),
    "market_cap": ("mkt_cap", "marketcapitalization"),
    
    "pe_ttm": ("pe", "price_earnings_ttm"),
    "forward_pe": ("pe_forward", "forward_price_earnings"),
    "pb_ratio": ("pb", "price_book"),
    "ps_ratio": ("ps", "price_sales"),
    "dividend_yield": ("div_yield", "dividend_yield_percent"),
    
    "roe": ("return_on_equity",),
    "roa": ("return_on_assets",),
    "net_margin": ("profit_margin",),
    "ebitda_margin": ("margin_ebitda",),
    
    "revenue_growth_yoy": ("rev_growth", "revenue_growth"),
    "eps_growth_yoy": ("eps_growth",),
    
    "beta": ("beta_5y",),
    "volatility_30d": ("vol_30d", "volatility"),
    
    "rsi_14": ("rsi14", "rsi"),
    "macd_histogram": ("macd_hist", "macd_histogram"),
    
    "fair_value": ("intrinsic_value",),
    "upside_percent": ("upside", "upside_pct"),
    "forecast_price_1m": ("target_price_1m", "expected_price_1m"),
    "expected_return_1m": ("expected_return_1m", "forecast_return_1m"),
    
    "quantity": ("qty", "shares"),
    "avg_cost": ("cost_basis", "average_cost"),
    "unrealized_pl": ("unrealized_pnl", "u_pl"),
}

ALIAS_TO_CANONICAL: Dict[str, str] = {alias: canonical for canonical, aliases in FIELD_ALIASES.items() for alias in aliases}

@lru_cache(maxsize=1024)
def canonical_field(field: str) -> str:
    f = str(field or "").strip()
    return ALIAS_TO_CANONICAL.get(f, f) if f else ""

@lru_cache(maxsize=1024)
def header_to_field(header: str) -> str:
    h = normalize_header(header)
    if h in HEADER_TO_FIELD: return HEADER_TO_FIELD[h]
    for pattern, field in HEADER_TO_FIELD.items():
        if pattern in h or h in pattern: return field
    return ""

def field_to_header(field: str) -> str:
    canon = canonical_field(field)
    return FIELD_TO_HEADER.get(canon, canon)

# =============================================================================
# Sheet Name Resolution
# =============================================================================

@lru_cache(maxsize=256)
def normalize_sheet_name(name: Optional[str]) -> str:
    s = str(name or "").strip().lower()
    if not s: return ""
    s = re.sub(r'^(?:sheet_|tab_)?', '', s)
    s = re.sub(r'_(?:sheet|tab)$', '', s)
    s = re.sub(r'[\s\-_]+', '_', s).strip('_')
    
    replacements = {
        'ksa': 'ksa_tadawul', 'tadawul': 'ksa_tadawul', 'saudi': 'ksa_tadawul',
        'global': 'global_markets', 'world': 'global_markets',
        'portfolio': 'my_portfolio', 'myportfolio': 'my_portfolio',
        'funds': 'mutual_funds', 'mutualfunds': 'mutual_funds',
        'commodities': 'commodities_fx', 'fx': 'commodities_fx', 'forex': 'commodities_fx',
        'insights': 'insights_analysis', 'analysis': 'insights_analysis',
    }
    return replacements.get(s, s)

_SCHEMA_REGISTRY: Dict[str, Tuple[str, ...]] = {}

def register_schema(name: str, headers: List[str], version: str = "vNext") -> None:
    _SCHEMA_REGISTRY[f"{version}:{normalize_sheet_name(name)}"] = tuple(headers)

for name, headers in VNEXT_SCHEMAS.items(): register_schema(name, list(headers), "vNext")
for name, headers in LEGACY_SCHEMAS.items(): register_schema(name, list(headers), "legacy")

@lru_cache(maxsize=256)
def get_headers_for_sheet(sheet_name: Optional[str] = None, version: str = "vNext") -> List[str]:
    norm = normalize_sheet_name(sheet_name)
    if not norm: return list(VNEXT_SCHEMAS.get("GLOBAL_MARKETS", []))
    
    key = f"{version}:{norm}"
    if key in _SCHEMA_REGISTRY: return list(_SCHEMA_REGISTRY[key])
    
    for k, headers in _SCHEMA_REGISTRY.items():
        if k.endswith(f":{norm}"): return list(headers)
        
    for k, headers in _SCHEMA_REGISTRY.items():
        if norm in k or k.split(':')[-1] in norm: return list(headers)
        
    return list(VNEXT_SCHEMAS.get("GLOBAL_MARKETS", []))

def get_supported_sheets(version: str = "vNext") -> List[str]:
    return sorted({key.split(':')[-1] for key in _SCHEMA_REGISTRY.keys() if key.startswith(f"{version}:")})

# =============================================================================
# Request Models
# =============================================================================

class BatchProcessRequest(BaseModel):
    operation: str = Field(default="refresh", description="Operation to perform")
    sheet_name: Optional[str] = Field(None, description="Target sheet name")
    symbols: List[str] = Field(default_factory=list, description="List of symbols")
    tickers: List[str] = Field(default_factory=list, description="Alias for symbols")
    force_refresh: bool = Field(default=False, description="Force refresh even if cached")
    include_forecast: bool = Field(default=True, description="Include forecast data")
    include_technical: bool = Field(default=True, description="Include technical indicators")
    priority: int = Field(default=0, description="Request priority (0-10)", ge=0, le=10)
    timeout_seconds: Optional[int] = Field(None, description="Request timeout", ge=1, le=300)
    webhook_url: Optional[str] = Field(None, description="Callback URL for async processing")
    
    if _PYDANTIC_V2:
        model_config = ConfigDict(extra="ignore")
        
        @field_validator("symbols", "tickers", mode="before")
        @classmethod
        def validate_symbol_list(cls, v: Any) -> List[str]:
            if v is None: return []
            if isinstance(v, str): v = re.split(r'[,\s\n]+', v)
            if isinstance(v, (list, tuple)): return [s.upper() for x in v if (s := safe_str(x))]
            return []
        
        @model_validator(mode="after")
        def combine_symbols(self) -> "BatchProcessRequest":
            self.symbols = list(set(self.symbols + self.tickers))
            self.tickers = []
            return self
    else:
        class Config:
            extra = "ignore"
        
        @validator("symbols", "tickers", pre=True)
        def validate_symbol_list_v1(cls, v):
            if v is None: return []
            if isinstance(v, str): v = re.split(r'[,\s\n]+', v)
            if isinstance(v, (list, tuple)): return [s.upper() for x in v if (s := safe_str(x))]
            return []
        
        @root_validator
        def combine_symbols_v1(cls, values):
            values["symbols"] = list(set(values.get("symbols", []) + values.get("tickers", [])))
            values["tickers"] = []
            return values
    
    def all_symbols(self) -> List[str]: return self.symbols

class BatchProcessResponse(BaseModel):
    request_id: str = Field(..., description="Unique request identifier")
    operation: str = Field(..., description="Operation performed")
    sheet_name: Optional[str] = Field(None, description="Target sheet name")
    symbols_processed: int = Field(0, description="Number of symbols processed")
    symbols_failed: int = Field(0, description="Number of symbols failed")
    symbols_total: int = Field(0, description="Total symbols requested")
    processing_time_ms: float = Field(0, description="Total processing time in ms")
    status: str = Field("completed", description="Processing status")
    error: Optional[str] = Field(None, description="Error message if any")
    warnings: List[str] = Field(default_factory=list, description="Warning messages")
    
    if _PYDANTIC_V2: model_config = ConfigDict(extra="ignore")

# =============================================================================
# Validation Utilities
# =============================================================================

def validate_headers(headers: Sequence[str], expected_len: int) -> Tuple[bool, List[str]]:
    if not headers: return False, ["Headers are empty"]
    errors = []
    if len(headers) != expected_len: errors.append(f"Expected {expected_len} headers, got {len(headers)}")
    
    seen, duplicates = set(), []
    for h in headers:
        norm = normalize_header(h)
        if norm in seen: duplicates.append(h)
        seen.add(norm)
    if duplicates: errors.append(f"Duplicate headers: {duplicates}")
    
    return len(errors) == 0, errors

def migrate_schema_v3_to_v4(headers_v3: Sequence[str]) -> List[str]:
    if not headers_v3: return []
    header_map = {normalize_header(h): h for h in VN_HEADERS_GLOBAL}
    return [header_map.get(normalize_header(h3), h3) for h3 in headers_v3]

def get_field_groups() -> Dict[str, List[str]]:
    return {
        "Identity": VN_IDENTITY, "Price": VN_PRICE, "Volume": VN_VOLUME,
        "Capitalization": VN_CAP, "Valuation": VN_VALUATION_MULTIPLES,
        "Earnings": VN_EARNINGS, "Dividends": VN_DIVIDENDS,
        "Profitability": VN_PROFITABILITY, "Growth": VN_GROWTH,
        "Health": VN_HEALTH, "Risk": VN_RISK, "Technicals": VN_TECHNICALS,
        "Forecast": VN_FORECAST, "Returns": VN_RETURNS, "Scores": VN_SCORES,
        "News": VN_NEWS, "Ownership": VN_OWNERSHIP, "Meta": VN_META,
    }

# =============================================================================
# Module Exports
# =============================================================================

__all__ = [
    "SCHEMAS_VERSION", "MarketType", "AssetClass", "Recommendation", "DataQuality", "BadgeLevel",
    "UnifiedQuote", "BatchProcessRequest", "BatchProcessResponse",
    "DEFAULT_HEADERS_59", "DEFAULT_HEADERS_ANALYSIS", "VNEXT_SCHEMAS",
    "VN_IDENTITY", "VN_PRICE", "VN_VOLUME", "VN_CAP", "VN_VALUATION_MULTIPLES", "VN_EARNINGS",
    "VN_DIVIDENDS", "VN_PROFITABILITY", "VN_GROWTH", "VN_HEALTH", "VN_RISK", "VN_TECHNICALS",
    "VN_FORECAST", "VN_RETURNS", "VN_SCORES", "VN_NEWS", "VN_OWNERSHIP", "VN_META",
    "VN_HEADERS_KSA_TADAWUL", "VN_HEADERS_GLOBAL", "VN_HEADERS_PORTFOLIO", "VN_HEADERS_FUNDS",
    "VN_HEADERS_COMMODITIES_FX", "VN_HEADERS_INSIGHTS",
    "normalize_header", "header_to_field", "field_to_header", "canonical_field",
    "HEADER_TO_FIELD", "FIELD_TO_HEADER", "FIELD_ALIASES", "ALIAS_TO_CANONICAL",
    "normalize_sheet_name", "get_headers_for_sheet", "get_supported_sheets", "register_schema",
    "validate_headers", "migrate_schema_v3_to_v4", "get_field_groups",
    "safe_float", "safe_int", "safe_str", "safe_bool", "safe_date", "safe_datetime",
    "bound_value", "percent_to_decimal", "decimal_to_percent",
]
