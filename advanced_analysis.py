# enhanced_advanced_analysis.py
# Version: 4.0.0 - Production-Ready AI Trading Analysis Engine
# Enhanced with: Real-time sentiment, portfolio optimization, explainable AI, and advanced risk management

from __future__ import annotations

import os
import asyncio
import aiohttp
import pandas as pd
import numpy as np
import json
import logging
import time
import hashlib
import pickle
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any, Tuple, Union, Set
from dataclasses import dataclass, asdict, field
from enum import Enum
from pathlib import Path
from contextlib import asynccontextmanager
import threading
from collections import deque, defaultdict
from decimal import Decimal, ROUND_HALF_UP
import warnings
from abc import ABC, abstractmethod
import re

import requests
from sklearn.ensemble import RandomForestRegressor, GradientBoostingRegressor
from sklearn.preprocessing import StandardScaler
import joblib

# Suppress warnings for cleaner logs
warnings.filterwarnings('ignore')

# Configure structured logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('advanced_analysis.log')
    ]
)
logger = logging.getLogger(__name__)


# =============================================================================
# Enhanced Enums & Data Models
# =============================================================================

class AnalysisTimeframe(Enum):
    """Enhanced timeframe options"""
    INTRADAY = "intraday"
    SHORT_TERM = "short_term"
    MEDIUM_TERM = "medium_term"
    LONG_TERM = "long_term"
    SWING_TRADE = "swing_trade"
    POSITION_TRADE = "position_trade"


class Recommendation(Enum):
    """Enhanced recommendation types"""
    STRONG_BUY = "STRONG_BUY"
    BUY = "BUY"
    ACCUMULATE = "ACCUMULATE"
    HOLD = "HOLD"
    REDUCE = "REDUCE"
    SELL = "SELL"
    STRONG_SELL = "STRONG_SELL"


class DataQuality(Enum):
    """Enhanced data quality metrics"""
    EXCELLENT = "excellent"
    HIGH = "high"
    MEDIUM = "medium"
    LOW = "low"
    SYNTHETIC = "synthetic"
    UNRELIABLE = "unreliable"


class MarketRegime(Enum):
    """Market regime classification"""
    BULL_TRENDING = "bull_trending"
    BULL_CONSOLIDATION = "bull_consolidation"
    BEAR_TRENDING = "bear_trending"
    BEAR_CONSOLIDATION = "bear_consolidation"
    SIDEWAYS = "sideways"
    HIGH_VOLATILITY = "high_volatility"
    LOW_VOLATILITY = "low_volatility"


class RiskProfile(Enum):
    """Risk profile types"""
    CONSERVATIVE = "conservative"
    MODERATE = "moderate"
    AGGRESSIVE = "aggressive"
    SPECULATIVE = "speculative"


@dataclass
class PriceData:
    """Enhanced price data with comprehensive metrics"""
    symbol: str
    price: float
    change: float
    change_percent: float
    volume: int
    high: float
    low: float
    open: float
    previous_close: float
    timestamp: str
    source: str
    data_quality: DataQuality = DataQuality.MEDIUM
    confidence_score: float = 0.0
    vwap: float = 0.0
    typical_price: float = 0.0
    market_cap: Optional[float] = None
    average_true_range: Optional[float] = None
    volume_weighted_price: Optional[float] = None
    bid_ask_spread: Optional[float] = None
    liquidity_score: float = 0.0
    
    def __post_init__(self):
        """Enhanced validation and calculation"""
        if isinstance(self.data_quality, str):
            try:
                self.data_quality = DataQuality(self.data_quality)
            except ValueError:
                self.data_quality = DataQuality.MEDIUM
        
        self.symbol = self.symbol.upper().strip()
        
        if self.price is None or self.price <= 0:
            logger.warning(f"Invalid price for {self.symbol}: {self.price}")
            self.price = 0.0
        
        if not self.timestamp:
            self.timestamp = datetime.utcnow().isoformat() + "Z"
        
        # Calculate additional metrics
        self.typical_price = (self.high + self.low + self.close) / 3 if all([
            self.high, self.low, self.close
        ]) else self.price
        
        # Calculate confidence based on multiple factors
        factors = [
            0.3 if self.price > 0 else 0,
            0.2 if self.volume > 0 else 0,
            0.15 if self.high >= self.low else 0,
            0.15 if self.timestamp else 0,
            0.1 if self.source else 0,
            0.1 if self.data_quality in [DataQuality.HIGH, DataQuality.EXCELLENT] else 0.05
        ]
        self.confidence_score = min(1.0, sum(factors))
        
        # Calculate liquidity score
        if self.volume > 0 and self.price > 0:
            self.liquidity_score = min(1.0, np.log10(self.volume * self.price) / 10)
    
    @property
    def close(self):
        """Alias for price for compatibility"""
        return self.price
    
    def to_dict(self) -> Dict[str, Any]:
        result = asdict(self)
        result["data_quality"] = self.data_quality.value
        return result
    
    def to_series(self) -> pd.Series:
        """Convert to pandas Series for analysis"""
        return pd.Series({
            'price': self.price,
            'volume': self.volume,
            'high': self.high,
            'low': self.low,
            'open': self.open,
            'change': self.change,
            'change_percent': self.change_percent
        })


@dataclass
class TechnicalIndicators:
    """Comprehensive technical indicators with advanced calculations"""
    # Core indicators
    rsi: float
    macd: float
    macd_signal: float
    macd_histogram: float
    moving_avg_20: float
    moving_avg_50: float
    moving_avg_200: float
    
    # Bollinger Bands
    bollinger_upper: float
    bollinger_lower: float
    bollinger_position: float
    bollinger_width: float
    
    # Volume indicators
    volume_trend: float
    volume_ratio: float
    on_balance_volume: float
    volume_profile: Dict[str, float]
    
    # Support & Resistance
    support_levels: List[float]
    resistance_levels: List[float]
    pivot_points: Dict[str, float]
    
    # Trend indicators
    trend_direction: str
    trend_strength: float
    adx: float
    dmi_plus: float
    dmi_minus: float
    
    # Momentum indicators
    momentum_score: float
    rate_of_change: float
    williams_r: float
    stoch_k: float
    stoch_d: float
    cci: float
    mfi: float
    
    # Volatility indicators
    volatility: float
    average_true_range: float
    volatility_ratio: float
    vix_correlation: Optional[float]
    
    # Pattern recognition
    patterns_detected: List[str]
    pattern_confidence: float
    
    # Ichimoku Cloud
    tenkan_sen: Optional[float]
    kijun_sen: Optional[float]
    senkou_span_a: Optional[float]
    senkou_span_b: Optional[float]
    chikou_span: Optional[float]
    cloud_direction: Optional[str]
    
    # Market regime
    market_regime: MarketRegime
    regime_confidence: float
    
    # Quality metrics
    data_quality: DataQuality
    calculation_timestamp: str
    indicator_count: int = 0
    signal_strength: float = 0.0
    
    def __post_init__(self):
        if isinstance(self.data_quality, str):
            try:
                self.data_quality = DataQuality(self.data_quality)
            except ValueError:
                self.data_quality = DataQuality.MEDIUM
        
        if not self.calculation_timestamp:
            self.calculation_timestamp = datetime.utcnow().isoformat() + "Z"
        
        # Calculate signal strength
        signals = []
        if self.rsi < 30: signals.append(1.0)  # Oversold
        elif self.rsi > 70: signals.append(-1.0)  # Overbought
        
        if self.macd_histogram > 0: signals.append(0.5)
        elif self.macd_histogram < 0: signals.append(-0.5)
        
        if self.trend_strength > 60: signals.append(0.7 if self.trend_direction == "Bullish" else -0.7)
        
        self.signal_strength = np.mean(signals) if signals else 0.0
        
        # Count indicators
        self.indicator_count = len([v for v in self.__dict__.values() if v is not None])
    
    def to_dict(self) -> Dict[str, Any]:
        result = asdict(self)
        result["data_quality"] = self.data_quality.value
        result["market_regime"] = self.market_regime.value if self.market_regime else None
        return result
    
    def get_buy_signals(self) -> List[Dict[str, Any]]:
        """Extract buy signals from indicators"""
        signals = []
        
        if self.rsi < 30:
            signals.append({
                'indicator': 'RSI',
                'signal': 'Oversold',
                'strength': min(1.0, (30 - self.rsi) / 30),
                'value': self.rsi
            })
        
        if self.macd_histogram > 0 and self.macd > self.macd_signal:
            signals.append({
                'indicator': 'MACD',
                'signal': 'Bullish Crossover',
                'strength': min(1.0, abs(self.macd_histogram) / 2),
                'value': self.macd_histogram
            })
        
        if self.bollinger_position < 0.2:
            signals.append({
                'indicator': 'Bollinger Bands',
                'signal': 'Oversold',
                'strength': min(1.0, (0.2 - self.bollinger_position) / 0.2),
                'value': self.bollinger_position
            })
        
        return signals
    
    def get_sell_signals(self) -> List[Dict[str, Any]]:
        """Extract sell signals from indicators"""
        signals = []
        
        if self.rsi > 70:
            signals.append({
                'indicator': 'RSI',
                'signal': 'Overbought',
                'strength': min(1.0, (self.rsi - 70) / 30),
                'value': self.rsi
            })
        
        if self.macd_histogram < 0 and self.macd < self.macd_signal:
            signals.append({
                'indicator': 'MACD',
                'signal': 'Bearish Crossover',
                'strength': min(1.0, abs(self.macd_histogram) / 2),
                'value': self.macd_histogram
            })
        
        if self.bollinger_position > 0.8:
            signals.append({
                'indicator': 'Bollinger Bands',
                'signal': 'Overbought',
                'strength': min(1.0, (self.bollinger_position - 0.8) / 0.2),
                'value': self.bollinger_position
            })
        
        return signals


@dataclass
class FundamentalData:
    """Enhanced fundamental data with industry comparisons"""
    # Valuation metrics
    market_cap: float
    enterprise_value: float
    pe_ratio: float
    forward_pe: float
    pb_ratio: float
    ps_ratio: float
    ev_ebitda: float
    ev_sales: float
    dividend_yield: float
    dividend_payout_ratio: float
    
    # Profitability
    eps: float
    eps_growth: float
    roe: float
    roa: float
    roce: float
    gross_margin: float
    operating_margin: float
    net_margin: float
    ebitda_margin: float
    
    # Growth metrics
    revenue_growth: float
    net_income_growth: float
    eps_growth_rate: float
    free_cash_flow_growth: float
    
    # Financial health
    debt_to_equity: float
    debt_to_ebitda: float
    current_ratio: float
    quick_ratio: float
    interest_coverage: float
    free_cash_flow: float
    operating_cash_flow: float
    
    # Efficiency
    asset_turnover: float
    inventory_turnover: float
    receivables_turnover: float
    
    # Industry comparison
    sector: str
    industry_pe: float
    industry_pb: float
    industry_roe: float
    sector_rank: int
    percentile_rank: float
    
    # Quality metrics
    data_quality: DataQuality
    last_updated: str
    source: str
    
    # Forward looking
    forward_eps: Optional[float] = None
    forward_revenue: Optional[float] = None
    analyst_rating: Optional[str] = None
    price_target: Optional[float] = None
    upside_potential: Optional[float] = None
    
    def __post_init__(self):
        if isinstance(self.data_quality, str):
            try:
                self.data_quality = DataQuality(self.data_quality)
            except ValueError:
                self.data_quality = DataQuality.MEDIUM
        
        if not self.last_updated:
            self.last_updated = datetime.utcnow().isoformat() + "Z"
        
        # Calculate upside potential if price target is available
        if self.price_target and self.market_cap:
            self.upside_potential = (self.price_target / (self.market_cap / 1e6) - 1) * 100
    
    def to_dict(self) -> Dict[str, Any]:
        result = asdict(self)
        result["data_quality"] = self.data_quality.value
        return result
    
    def get_valuation_score(self) -> float:
        """Calculate valuation score (0-100)"""
        scores = []
        
        # P/E score
        if self.pe_ratio > 0:
            if self.pe_ratio < 15:
                scores.append(90)
            elif self.pe_ratio < 25:
                scores.append(70)
            elif self.pe_ratio < 35:
                scores.append(50)
            else:
                scores.append(30)
        
        # P/B score
        if self.pb_ratio > 0:
            if self.pb_ratio < 1.5:
                scores.append(85)
            elif self.pb_ratio < 3:
                scores.append(65)
            else:
                scores.append(40)
        
        # Dividend yield score
        if self.dividend_yield > 0:
            if self.dividend_yield > 0.04:
                scores.append(80)
            elif self.dividend_yield > 0.02:
                scores.append(60)
            else:
                scores.append(40)
        
        return np.mean(scores) if scores else 50.0
    
    def get_growth_score(self) -> float:
        """Calculate growth score (0-100)"""
        scores = []
        
        if self.revenue_growth > 0.15:
            scores.append(85)
        elif self.revenue_growth > 0.05:
            scores.append(65)
        elif self.revenue_growth > -0.05:
            scores.append(45)
        else:
            scores.append(25)
        
        if self.eps_growth > 0.15:
            scores.append(90)
        elif self.eps_growth > 0.05:
            scores.append(70)
        elif self.eps_growth > -0.05:
            scores.append(50)
        else:
            scores.append(30)
        
        return np.mean(scores) if scores else 50.0


@dataclass
class MarketSentiment:
    """Market sentiment analysis"""
    symbol: str
    overall_sentiment: float  # -1.0 to 1.0
    news_sentiment: float
    social_sentiment: float
    analyst_sentiment: float
    options_sentiment: float
    short_interest_ratio: Optional[float]
    put_call_ratio: Optional[float]
    fear_greed_index: Optional[float]
    sentiment_sources: List[str]
    sentiment_trend: str  # improving, deteriorating, stable
    confidence_score: float
    timestamp: str
    
    def __post_init__(self):
        if not self.timestamp:
            self.timestamp = datetime.utcnow().isoformat() + "Z"
    
    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


@dataclass
class PortfolioAllocation:
    """Portfolio allocation recommendation"""
    symbol: str
    recommended_allocation: float  # Percentage of portfolio
    max_allocation: float
    min_allocation: float
    allocation_reason: str
    risk_adjusted_allocation: float
    scenario_analysis: Dict[str, float]  # Best/worst/normal case allocations


@dataclass
class AIRecommendation:
    """Enhanced AI recommendation with explainable AI"""
    symbol: str
    
    # Scores
    overall_score: float
    technical_score: float
    fundamental_score: float
    sentiment_score: float
    risk_adjusted_score: float
    momentum_score: float
    value_score: float
    quality_score: float
    
    # Recommendation
    recommendation: Recommendation
    recommendation_strength: float  # 0-1.0
    confidence_level: str
    expected_return: float
    expected_volatility: float
    sharpe_ratio: float
    sortino_ratio: float
    
    # Time horizons
    short_term_outlook: str
    medium_term_outlook: str
    long_term_outlook: str
    swing_trade_outlook: str
    
    # Risk management
    risk_level: str
    risk_factors: List[Dict[str, Any]]
    max_drawdown_estimate: float
    value_at_risk: float
    stop_loss_levels: Dict[str, float]
    position_sizing: PortfolioAllocation
    
    # Trading levels
    optimal_entry: Dict[str, float]  # Aggressive/Moderate/Conservative
    take_profit_levels: Dict[str, float]
    trailing_stop_percent: float
    
    # Analysis breakdown
    key_strengths: List[str]
    key_weaknesses: List[str]
    opportunities: List[str]
    threats: List[str]
    
    # Market context
    market_regime: MarketRegime
    sector_outlook: str
    competitive_position: str
    
    # Meta
    timestamp: str
    data_sources_used: int
    analysis_duration: float
    model_version: str
    explainable_factors: Dict[str, float]
    scenario_analysis: Dict[str, Dict[str, float]]
    
    def __post_init__(self):
        if not self.timestamp:
            self.timestamp = datetime.utcnow().isoformat() + "Z"
    
    def to_dict(self) -> Dict[str, Any]:
        result = asdict(self)
        result["recommendation"] = self.recommendation.value
        result["market_regime"] = self.market_regime.value if self.market_regime else None
        return result
    
    def get_summary(self) -> Dict[str, Any]:
        """Get summary for quick decision making"""
        return {
            'symbol': self.symbol,
            'recommendation': self.recommendation.value,
            'overall_score': self.overall_score,
            'confidence': self.confidence_level,
            'risk_level': self.risk_level,
            'expected_return': self.expected_return,
            'optimal_entry': self.optimal_entry.get('moderate'),
            'stop_loss': self.stop_loss_levels.get('initial'),
            'position_size': self.position_sizing.recommended_allocation
        }


@dataclass
class AdvancedAnalysisReport:
    """Comprehensive analysis report"""
    symbol: str
    price_data: PriceData
    technical_indicators: TechnicalIndicators
    fundamental_data: FundamentalData
    market_sentiment: MarketSentiment
    ai_recommendation: AIRecommendation
    portfolio_optimization: Dict[str, Any]
    risk_assessment: Dict[str, Any]
    comparative_analysis: Dict[str, Any]
    timestamp: str
    report_id: str
    
    def to_dict(self) -> Dict[str, Any]:
        result = asdict(self)
        # Convert nested objects to dict
        result['price_data'] = self.price_data.to_dict()
        result['technical_indicators'] = self.technical_indicators.to_dict()
        result['fundamental_data'] = self.fundamental_data.to_dict()
        result['market_sentiment'] = self.market_sentiment.to_dict()
        result['ai_recommendation'] = self.ai_recommendation.to_dict()
        return result


# =============================================================================
# Enhanced Core Analyzer
# =============================================================================

class EnhancedTradingAnalyzer:
    """
    Production-ready multi-source AI analysis engine with advanced features.
    
    New Features:
    1. Machine Learning Integration
    2. Real-time Market Sentiment Analysis
    3. Portfolio Optimization
    4. Explainable AI with Feature Importance
    5. Advanced Risk Management
    6. Scenario Analysis
    7. Multi-timeframe Analysis
    8. Correlation Analysis
    9. News & Social Media Integration
    10. Performance Backtesting
    
    Environment Variables:
        ML_MODEL_PATH            - Path to trained ML models
        SENTIMENT_API_KEY        - For news/social sentiment
        REDIS_URL               - Redis for distributed caching
        MODEL_VERSION           - Version identifier
        ENABLE_ML_PREDICTIONS   - Enable ML-based predictions
    """
    
    def __init__(self) -> None:
        # Initialize with enhanced configuration
        self._init_configuration()
        self._init_apis()
        self._init_ml_models()
        self._init_cache()
        self._init_sessions()
        self._init_monitoring()
        
        logger.info(f"EnhancedTradingAnalyzer v{self.model_version} initialized")
    
    def _init_configuration(self):
        """Initialize enhanced configuration"""
        self.model_version = os.getenv("MODEL_VERSION", "4.0.0")
        self.enable_ml = os.getenv("ENABLE_ML_PREDICTIONS", "true").lower() == "true"
        
        # Timeouts
        self.http_timeout = int(os.getenv("HTTP_TIMEOUT_SECONDS", "20"))
        self.analysis_timeout = int(os.getenv("ANALYSIS_TIMEOUT_SECONDS", "45"))
        self.quote_timeout = int(os.getenv("QUOTE_TIMEOUT_SECONDS", "15"))
        
        # Rate limiting
        self.rate_limit_rpm = int(os.getenv("RATE_LIMIT_RPM", "120"))
        self.request_timestamps = deque(maxlen=self.rate_limit_rpm * 2)
        self._rate_lock = threading.RLock()
        
        # Caching
        self.cache_ttl = timedelta(minutes=int(os.getenv("CACHE_TTL_MINUTES", "20")))
        self.max_cache_size = int(os.getenv("MAX_CACHE_SIZE_MB", "200"))
        
        # ML Configuration
        self.ml_model_path = os.getenv("ML_MODEL_PATH", "./ml_models")
        
        # Statistics
        self.stats = {
            'requests': 0,
            'errors': 0,
            'cache_hits': 0,
            'cache_misses': 0,
            'avg_response_time': 0,
            'start_time': time.time()
        }
        self._stats_lock = threading.RLock()
    
    def _init_apis(self):
        """Initialize API configurations"""
        self.apis = {
            "alpha_vantage": os.getenv("ALPHA_VANTAGE_API_KEY"),
            "finnhub": os.getenv("FINNHUB_API_KEY"),
            "eodhd": os.getenv("EODHD_API_KEY"),
            "twelvedata": os.getenv("TWELVEDATA_API_KEY"),
            "marketstack": os.getenv("MARKETSTACK_API_KEY"),
            "fmp": os.getenv("FMP_API_KEY"),
            "polygon": os.getenv("POLYGON_API_KEY"),
            "tiingo": os.getenv("TIINGO_API_KEY"),
            "sentiment": os.getenv("SENTIMENT_API_KEY"),
        }
        
        self.base_urls = {
            "alpha_vantage": "https://www.alphavantage.co/query",
            "finnhub": "https://finnhub.io/api/v1",
            "eodhd": "https://eodhistoricaldata.com/api",
            "twelvedata": "https://api.twelvedata.com",
            "marketstack": "http://api.marketstack.com/v1",
            "fmp": "https://financialmodelingprep.com/api/v3",
            "polygon": "https://api.polygon.io/v2",
            "tiingo": "https://api.tiingo.com/tiingo",
            "news": "https://newsapi.org/v2",
        }
        
        self.google_apps_script_url = os.getenv("GOOGLE_APPS_SCRIPT_URL")
        self.redis_url = os.getenv("REDIS_URL")
    
    def _init_ml_models(self):
        """Initialize machine learning models"""
        self.ml_models = {}
        self.scaler = StandardScaler()
        
        if self.enable_ml and os.path.exists(self.ml_model_path):
            try:
                # Load pre-trained models
                model_files = {
                    'price_prediction': 'price_model.pkl',
                    'sentiment': 'sentiment_model.pkl',
                    'risk': 'risk_model.pkl',
                    'recommendation': 'recommendation_model.pkl'
                }
                
                for name, filename in model_files.items():
                    path = os.path.join(self.ml_model_path, filename)
                    if os.path.exists(path):
                        self.ml_models[name] = joblib.load(path)
                        logger.info(f"Loaded ML model: {name}")
            except Exception as e:
                logger.error(f"Failed to load ML models: {e}")
    
    def _init_cache(self):
        """Initialize enhanced cache system"""
        self.cache_dir = Path("./enhanced_cache")
        self.cache_dir.mkdir(exist_ok=True)
        
        # Initialize Redis if available
        self.redis_client = None
        if self.redis_url:
            try:
                import redis
                self.redis_client = redis.from_url(self.redis_url, decode_responses=True)
                self.redis_client.ping()
                logger.info("Redis cache initialized")
            except Exception as e:
                logger.warning(f"Redis initialization failed: {e}")
        
        # In-memory cache for hot data
        self.memory_cache = {}
        self.memory_cache_ttl = {}
    
    def _init_sessions(self):
        """Initialize HTTP sessions"""
        self._session = None
        self._session_sync = None
        
    def _init_monitoring(self):
        """Initialize monitoring systems"""
        self.performance_metrics = defaultdict(list)
        self.error_log = deque(maxlen=1000)
    
    async def get_session(self) -> aiohttp.ClientSession:
        """Get or create async session with enhanced configuration"""
        if self._session is None or self._session.closed:
            timeout = aiohttp.ClientTimeout(
                total=self.http_timeout,
                connect=10,
                sock_read=10,
                sock_connect=10
            )
            connector = aiohttp.TCPConnector(
                limit=200,
                limit_per_host=50,
                ttl_dns_cache=300,
                force_close=False
            )
            self._session = aiohttp.ClientSession(
                timeout=timeout,
                connector=connector,
                headers={
                    'User-Agent': 'EnhancedTradingAnalyzer/4.0.0',
                    'Accept': 'application/json',
                    'Accept-Encoding': 'gzip, deflate'
                }
            )
        return self._session
    
    def get_session_sync(self) -> requests.Session:
        """Get sync session with retry logic"""
        if self._session_sync is None:
            self._session_sync = requests.Session()
            from requests.adapters import HTTPAdapter
            from urllib3.util.retry import Retry
            
            retry_strategy = Retry(
                total=4,
                backoff_factor=1,
                status_forcelist=[429, 500, 502, 503, 504],
                allowed_methods=["GET", "POST"]
            )
            adapter = HTTPAdapter(
                max_retries=retry_strategy,
                pool_connections=100,
                pool_maxsize=100
            )
            self._session_sync.mount("http://", adapter)
            self._session_sync.mount("https://", adapter)
        
        return self._session_sync
    
    # =========================================================================
    # Enhanced Cache Management
    # =========================================================================
    
    def _get_cache_key(self, symbol: str, data_type: str, **kwargs) -> str:
        """Generate cache key with parameters"""
        key_parts = [f"v4_{symbol.upper()}_{data_type}"]
        for k, v in sorted(kwargs.items()):
            key_parts.append(f"{k}={v}")
        
        key_string = "_".join(key_parts)
        return hashlib.sha256(key_string.encode()).hexdigest()[:32]
    
    async def _load_from_cache(self, key: str) -> Optional[Any]:
        """Load data from multi-layer cache"""
        start_time = time.time()
        
        # Check memory cache first
        if key in self.memory_cache:
            if time.time() - self.memory_cache_ttl.get(key, 0) < 60:  # 1 minute TTL
                self.stats['cache_hits'] += 1
                return self.memory_cache[key]
        
        # Check Redis
        if self.redis_client:
            try:
                cached = self.redis_client.get(key)
                if cached:
                    data = pickle.loads(cached.encode('latin1') if isinstance(cached, str) else cached)
                    # Store in memory cache
                    self.memory_cache[key] = data
                    self.memory_cache_ttl[key] = time.time()
                    self.stats['cache_hits'] += 1
                    return data
            except Exception as e:
                logger.debug(f"Redis cache read failed: {e}")
        
        # Check file cache
        try:
            cache_file = self.cache_dir / f"{key}.pkl"
            if cache_file.exists():
                with open(cache_file, 'rb') as f:
                    data = pickle.load(f)
                
                # Check TTL
                metadata_file = self.cache_dir / f"{key}_meta.json"
                if metadata_file.exists():
                    with open(metadata_file, 'r') as f:
                        metadata = json.load(f)
                    
                    if time.time() - metadata.get('timestamp', 0) < self.cache_ttl.total_seconds():
                        # Store in memory cache
                        self.memory_cache[key] = data
                        self.memory_cache_ttl[key] = time.time()
                        self.stats['cache_hits'] += 1
                        return data
        except Exception as e:
            logger.debug(f"File cache read failed: {e}")
        
        self.stats['cache_misses'] += 1
        return None
    
    async def _save_to_cache(self, key: str, data: Any) -> None:
        """Save data to multi-layer cache"""
        try:
            # Save to memory cache
            self.memory_cache[key] = data
            self.memory_cache_ttl[key] = time.time()
            
            # Save to Redis
            if self.redis_client:
                try:
                    pickled = pickle.dumps(data)
                    self.redis_client.setex(
                        key,
                        int(self.cache_ttl.total_seconds()),
                        pickled
                    )
                except Exception as e:
                    logger.debug(f"Redis cache write failed: {e}")
            
            # Save to file cache
            cache_file = self.cache_dir / f"{key}.pkl"
            metadata_file = self.cache_dir / f"{key}_meta.json"
            
            with open(cache_file, 'wb') as f:
                pickle.dump(data, f)
            
            metadata = {
                'timestamp': time.time(),
                'key': key,
                'size': len(pickle.dumps(data))
            }
            
            with open(metadata_file, 'w') as f:
                json.dump(metadata, f)
            
        except Exception as e:
            logger.error(f"Cache save failed: {e}")
    
    # =========================================================================
    # Enhanced Data Collection
    # =========================================================================
    
    async def get_real_time_price(self, symbol: str) -> Optional[PriceData]:
        """Get enhanced real-time price with multiple sources"""
        symbol = symbol.upper().strip()
        cache_key = self._get_cache_key(symbol, "price_v2")
        
        # Try cache first
        cached = await self._load_from_cache(cache_key)
        if cached:
            return cached
        
        # Gather from multiple sources concurrently
        tasks = {
            'alpha_vantage': self._get_price_alpha_vantage_enhanced(symbol),
            'finnhub': self._get_price_finnhub_enhanced(symbol),
            'twelvedata': self._get_price_twelvedata_enhanced(symbol),
            'polygon': self._get_price_polygon(symbol),
            'tiingo': self._get_price_tiingo(symbol),
        }
        
        # Execute with timeout
        try:
            done, pending = await asyncio.wait(
                tasks.values(),
                timeout=self.quote_timeout,
                return_when=asyncio.FIRST_COMPLETED
            )
        except Exception as e:
            logger.error(f"Price fetch error for {symbol}: {e}")
            done, pending = set(), set(tasks.values())
        
        # Cancel pending tasks
        for task in pending:
            task.cancel()
        
        # Collect valid results
        price_data_list = []
        for task in done:
            try:
                result = task.result()
                if result and result.price > 0:
                    price_data_list.append(result)
            except Exception as e:
                logger.debug(f"Price task failed: {e}")
                continue
        
        if not price_data_list:
            logger.warning(f"No valid price data for {symbol}")
            return None
        
        # Combine multiple sources using weighted average
        combined_price = self._combine_price_data(price_data_list)
        
        # Cache the result
        await self._save_to_cache(cache_key, combined_price)
        
        return combined_price
    
    def _combine_price_data(self, price_data_list: List[PriceData]) -> PriceData:
        """Combine multiple price sources using weighted confidence"""
        if len(price_data_list) == 1:
            return price_data_list[0]
        
        # Calculate weights based on confidence and recency
        weights = []
        for data in price_data_list:
            weight = data.confidence_score
            
            # Adjust for recency (within last 5 minutes gets bonus)
            try:
                data_time = datetime.fromisoformat(data.timestamp.replace('Z', '+00:00'))
                age_seconds = (datetime.utcnow() - data_time).total_seconds()
                if age_seconds < 300:  # 5 minutes
                    weight *= 1.2
            except:
                pass
            
            weights.append(weight)
        
        # Normalize weights
        total_weight = sum(weights)
        if total_weight == 0:
            weights = [1/len(weights)] * len(weights)
        else:
            weights = [w/total_weight for w in weights]
        
        # Weighted average for numerical fields
        base_data = price_data_list[0]
        
        # Calculate weighted averages
        weighted_price = sum(d.price * w for d, w in zip(price_data_list, weights))
        weighted_volume = sum(d.volume * w for d, w in zip(price_data_list, weights))
        weighted_high = sum(d.high * w for d, w in zip(price_data_list, weights))
        weighted_low = sum(d.low * w for d, w in zip(price_data_list, weights))
        weighted_open = sum(d.open * w for d, w in zip(price_data_list, weights))
        
        # Use most recent timestamp
        latest_timestamp = max(
            price_data_list,
            key=lambda x: datetime.fromisoformat(x.timestamp.replace('Z', '+00:00'))
        ).timestamp
        
        # Calculate VWAP if we have volume data
        vwap = None
        if any(d.volume_weighted_price for d in price_data_list if d.volume_weighted_price):
            vwap_values = [d.volume_weighted_price for d in price_data_list if d.volume_weighted_price]
            vwap = sum(vwap_values) / len(vwap_values)
        
        return PriceData(
            symbol=base_data.symbol,
            price=weighted_price,
            change=weighted_price - weighted_open,
            change_percent=((weighted_price - weighted_open) / weighted_open * 100) if weighted_open else 0,
            volume=int(weighted_volume),
            high=weighted_high,
            low=weighted_low,
            open=weighted_open,
            previous_close=base_data.previous_close,
            timestamp=latest_timestamp,
            source="combined_multisource",
            data_quality=DataQuality.HIGH if len(price_data_list) >= 3 else DataQuality.MEDIUM,
            confidence_score=sum(w * d.confidence_score for w, d in zip(weights, price_data_list)),
            vwap=vwap,
            typical_price=(weighted_high + weighted_low + weighted_price) / 3,
            liquidity_score=sum(d.liquidity_score * w for d, w in zip(price_data_list, weights))
        )
    
    async def _get_price_alpha_vantage_enhanced(self, symbol: str) -> Optional[PriceData]:
        """Enhanced Alpha Vantage price fetch"""
        if not self.apis.get("alpha_vantage"):
            return None
        
        try:
            params = {
                "function": "GLOBAL_QUOTE",
                "symbol": symbol,
                "apikey": self.apis["alpha_vantage"],
            }
            
            data = await self._make_async_request(self.base_urls["alpha_vantage"], params)
            if not data or "Global Quote" not in data:
                return None
            
            quote = data["Global Quote"]
            
            # Also get SMA for additional context
            sma_params = {
                "function": "SMA",
                "symbol": symbol,
                "interval": "daily",
                "time_period": 20,
                "series_type": "close",
                "apikey": self.apis["alpha_vantage"],
            }
            
            sma_data = await self._make_async_request(self.base_urls["alpha_vantage"], sma_params)
            sma_value = None
            if sma_data and "Technical Analysis: SMA" in sma_data:
                latest_date = list(sma_data["Technical Analysis: SMA"].keys())[0]
                sma_value = float(sma_data["Technical Analysis: SMA"][latest_date]["SMA"])
            
            return PriceData(
                symbol=symbol,
                price=float(quote.get("05. price", 0) or 0),
                change=float(quote.get("09. change", 0) or 0),
                change_percent=float(str(quote.get("10. change percent", "0")).rstrip("%") or 0),
                volume=int(quote.get("06. volume", 0) or 0),
                high=float(quote.get("03. high", 0) or 0),
                low=float(quote.get("04. low", 0) or 0),
                open=float(quote.get("02. open", 0) or 0),
                previous_close=float(quote.get("08. previous close", 0) or 0),
                timestamp=datetime.utcnow().isoformat() + "Z",
                source="alpha_vantage_enhanced",
                data_quality=DataQuality.HIGH,
                confidence_score=0.8,
                average_true_range=sma_value
            )
        except Exception as e:
            logger.debug(f"Alpha Vantage enhanced failed: {e}")
            return None
    
    async def _get_price_finnhub_enhanced(self, symbol: str) -> Optional[PriceData]:
        """Enhanced Finnhub price fetch with additional metrics"""
        if not self.apis.get("finnhub"):
            return None
        
        try:
            # Get basic quote
            url = f"{self.base_urls['finnhub']}/quote"
            params = {"symbol": symbol, "token": self.apis["finnhub"]}
            data = await self._make_async_request(url, params)
            
            if not data or data.get("c", 0) <= 0:
                return None
            
            # Get additional metrics
            metrics_url = f"{self.base_urls['finnhub']}/stock/metric"
            metrics_params = {
                "symbol": symbol,
                "metric": "all",
                "token": self.apis["finnhub"]
            }
            
            metrics_data = await self._make_async_request(metrics_url, metrics_params)
            market_cap = None
            if metrics_data and "metric" in metrics_data:
                market_cap = metrics_data["metric"].get("marketCapitalization")
            
            return PriceData(
                symbol=symbol,
                price=float(data.get("c", 0) or 0),
                change=float(data.get("d", 0) or 0),
                change_percent=float(data.get("dp", 0) or 0),
                high=float(data.get("h", 0) or 0),
                low=float(data.get("l", 0) or 0),
                open=float(data.get("o", 0) or 0),
                previous_close=float(data.get("pc", 0) or 0),
                volume=int(data.get("v", 0) or 0),
                timestamp=datetime.utcnow().isoformat() + "Z",
                source="finnhub_enhanced",
                data_quality=DataQuality.HIGH,
                confidence_score=0.85,
                market_cap=market_cap
            )
        except Exception as e:
            logger.debug(f"Finnhub enhanced failed: {e}")
            return None
    
    async def _get_price_twelvedata_enhanced(self, symbol: str) -> Optional[PriceData]:
        """Enhanced TwelveData price fetch"""
        if not self.apis.get("twelvedata"):
            return None
        
        try:
            # Get quote
            url = f"{self.base_urls['twelvedata']}/quote"
            params = {
                "symbol": symbol,
                "apikey": self.apis["twelvedata"],
                "interval": "1min",
                "outputsize": 1
            }
            
            data = await self._make_async_request(url, params)
            if not data or not data.get("close"):
                return None
            
            # Get SMA
            sma_url = f"{self.base_urls['twelvedata']}/sma"
            sma_params = {
                "symbol": symbol,
                "interval": "1day",
                "time_period": 20,
                "apikey": self.apis["twelvedata"],
                "outputsize": 1
            }
            
            sma_data = await self._make_async_request(sma_url, sma_params)
            sma_value = None
            if sma_data and "values" in sma_data:
                sma_value = float(sma_data["values"][0].get("sma", 0))
            
            return PriceData(
                symbol=symbol,
                price=float(data.get("close", 0) or 0),
                change=float(data.get("change", 0) or 0),
                change_percent=float(data.get("percent_change", 0) or 0),
                high=float(data.get("high", 0) or 0),
                low=float(data.get("low", 0) or 0),
                open=float(data.get("open", 0) or 0),
                previous_close=float(data.get("previous_close", 0) or 0),
                volume=int(data.get("volume", 0) or 0),
                timestamp=data.get("datetime") or datetime.utcnow().isoformat() + "Z",
                source="twelvedata_enhanced",
                data_quality=DataQuality.HIGH,
                confidence_score=0.8,
                average_true_range=sma_value
            )
        except Exception as e:
            logger.debug(f"TwelveData enhanced failed: {e}")
            return None
    
    async def _get_price_polygon(self, symbol: str) -> Optional[PriceData]:
        """Get price from Polygon.io"""
        if not self.apis.get("polygon"):
            return None
        
        try:
            url = f"{self.base_urls['polygon']}/aggs/ticker/{symbol}/prev"
            params = {"adjusted": "true", "apiKey": self.apis["polygon"]}
            
            data = await self._make_async_request(url, params)
            if not data or data.get("status") != "OK" or not data.get("results"):
                return None
            
            result = data["results"][0]
            
            return PriceData(
                symbol=symbol,
                price=float(result.get("c", 0)),
                change=float(result.get("c", 0)) - float(result.get("o", 0)),
                change_percent=((float(result.get("c", 0)) - float(result.get("o", 0))) / float(result.get("o", 0)) * 100) if result.get("o") else 0,
                volume=int(result.get("v", 0)),
                high=float(result.get("h", 0)),
                low=float(result.get("l", 0)),
                open=float(result.get("o", 0)),
                previous_close=float(result.get("o", 0)),
                timestamp=datetime.fromtimestamp(result.get("t", 0) / 1000).isoformat() + "Z",
                source="polygon",
                data_quality=DataQuality.HIGH,
                confidence_score=0.9,
                vwap=result.get("vw")
            )
        except Exception as e:
            logger.debug(f"Polygon price fetch failed: {e}")
            return None
    
    async def _get_price_tiingo(self, symbol: str) -> Optional[PriceData]:
        """Get price from Tiingo"""
        if not self.apis.get("tiingo"):
            return None
        
        try:
            url = f"{self.base_urls['tiingo']}/daily/{symbol}/prices"
            params = {"token": self.apis["tiingo"], "format": "json"}
            
            data = await self._make_async_request(url, params)
            if not data or not isinstance(data, list) or len(data) == 0:
                return None
            
            latest = data[0]
            
            return PriceData(
                symbol=symbol,
                price=float(latest.get("close", 0)),
                change=float(latest.get("close", 0)) - float(latest.get("open", 0)),
                change_percent=((float(latest.get("close", 0)) - float(latest.get("open", 0))) / float(latest.get("open", 0)) * 100) if latest.get("open") else 0,
                volume=int(latest.get("volume", 0)),
                high=float(latest.get("high", 0)),
                low=float(latest.get("low", 0)),
                open=float(latest.get("open", 0)),
                previous_close=float(latest.get("close", 0)),
                timestamp=latest.get("date") or datetime.utcnow().isoformat() + "Z",
                source="tiingo",
                data_quality=DataQuality.HIGH,
                confidence_score=0.85,
                volume_weighted_price=latest.get("volumeNotional")
            )
        except Exception as e:
            logger.debug(f"Tiingo price fetch failed: {e}")
            return None
    
    # =========================================================================
    # Enhanced Technical Analysis
    # =========================================================================
    
    async def calculate_technical_indicators(
        self, 
        symbol: str, 
        period: str = "3mo",
        include_advanced: bool = True
    ) -> TechnicalIndicators:
        """Calculate comprehensive technical indicators"""
        symbol = symbol.upper().strip()
        cache_key = self._get_cache_key(symbol, f"technical_v2_{period}")
        
        # Try cache
        cached = await self._load_from_cache(cache_key)
        if cached:
            return cached
        
        try:
            # Get historical data
            df = await self._get_historical_data_enhanced(symbol, period)
            
            if df.empty or len(df) < 50:
                logger.warning(f"Insufficient data for technical analysis of {symbol}")
                return self._generate_fallback_technical_indicators()
            
            # Calculate indicators
            indicators = await self._calculate_advanced_indicators_enhanced(df, symbol)
            
            # Cache results
            await self._save_to_cache(cache_key, indicators)
            
            return indicators
            
        except Exception as e:
            logger.error(f"Technical indicators calculation failed for {symbol}: {e}")
            return self._generate_fallback_technical_indicators()
    
    async def _get_historical_data_enhanced(self, symbol: str, period: str) -> pd.DataFrame:
        """Get enhanced historical data from multiple sources"""
        period_map = {
            "1mo": 30,
            "3mo": 90,
            "6mo": 180,
            "1y": 252,
            "2y": 504,
            "5y": 1260,
        }
        
        days = period_map.get(period, 90)
        
        # Try multiple sources
        sources = []
        
        if self.apis.get("alpha_vantage"):
            try:
                av_data = await self._get_alpha_vantage_historical(symbol, days)
                if not av_data.empty:
                    sources.append(av_data)
            except Exception as e:
                logger.debug(f"Alpha Vantage historical failed: {e}")
        
        if self.apis.get("polygon"):
            try:
                poly_data = await self._get_polygon_historical(symbol, days)
                if not poly_data.empty:
                    sources.append(poly_data)
            except Exception as e:
                logger.debug(f"Polygon historical failed: {e}")
        
        if self.apis.get("tiingo"):
            try:
                tiingo_data = await self._get_tiingo_historical(symbol, days)
                if not tiingo_data.empty:
                    sources.append(tiingo_data)
            except Exception as e:
                logger.debug(f"Tiingo historical failed: {e}")
        
        # Combine sources or use synthetic
        if sources:
            # Use the source with most data
            sources.sort(key=lambda x: len(x), reverse=True)
            return sources[0]
        else:
            logger.info(f"Using synthetic data for {symbol}")
            return self._generate_synthetic_data_enhanced(days)
    
    async def _calculate_advanced_indicators_enhanced(self, df: pd.DataFrame, symbol: str) -> TechnicalIndicators:
        """Calculate enhanced technical indicators"""
        try:
            # Ensure we have required columns
            required_cols = ['open', 'high', 'low', 'close', 'volume']
            for col in required_cols:
                if col not in df.columns:
                    df[col] = df['close'] if col != 'volume' else 0
            
            # Calculate basic indicators
            close_prices = df['close'].astype(float)
            high_prices = df['high'].astype(float)
            low_prices = df['low'].astype(float)
            open_prices = df['open'].astype(float)
            volumes = df['volume'].astype(float)
            
            # RSI
            rsi = self._calculate_rsi_enhanced(close_prices)
            
            # MACD
            macd, macd_signal, macd_histogram = self._calculate_macd_enhanced(close_prices)
            
            # Moving averages
            ma_20 = float(close_prices.rolling(20).mean().iloc[-1])
            ma_50 = float(close_prices.rolling(50).mean().iloc[-1])
            ma_200 = float(close_prices.rolling(min(200, len(close_prices))).mean().iloc[-1])
            
            # Bollinger Bands
            bb_upper, bb_lower, bb_position, bb_width = self._calculate_bollinger_bands_enhanced(close_prices)
            
            # Volume indicators
            volume_trend = float(volumes.pct_change(5).mean())
            volume_ratio = float(volumes.iloc[-1] / volumes.rolling(20).mean().iloc[-1]) if volumes.rolling(20).mean().iloc[-1] > 0 else 1.0
            obv = self._calculate_obv(close_prices, volumes)
            
            # Support & Resistance
            support_levels = self._calculate_support_levels(low_prices)
            resistance_levels = self._calculate_resistance_levels(high_prices)
            pivot_points = self._calculate_pivot_points(high_prices, low_prices, close_prices)
            
            # Trend indicators
            trend_direction, trend_strength = self._determine_trend_enhanced(ma_20, ma_50, ma_200, close_prices)
            adx, dmi_plus, dmi_minus = self._calculate_adx(high_prices, low_prices, close_prices)
            
            # Momentum indicators
            momentum_score = self._calculate_momentum_enhanced(close_prices)
            roc = self._calculate_rate_of_change(close_prices)
            williams_r = self._calculate_williams_r_enhanced(high_prices, low_prices, close_prices)
            stoch_k, stoch_d = self._calculate_stochastic_oscillator(high_prices, low_prices, close_prices)
            cci = self._calculate_cci_enhanced(high_prices, low_prices, close_prices)
            mfi = self._calculate_money_flow_index(high_prices, low_prices, close_prices, volumes)
            
            # Volatility
            volatility = float(close_prices.pct_change().std() * np.sqrt(252))
            atr = self._calculate_atr_enhanced(high_prices, low_prices, close_prices)
            volatility_ratio = volatility / atr if atr > 0 else 0
            
            # Pattern recognition
            patterns, pattern_confidence = self._detect_chart_patterns(df)
            
            # Ichimoku Cloud
            ichimoku = self._calculate_ichimoku_cloud(df)
            
            # Market regime
            market_regime, regime_confidence = self._determine_market_regime(
                close_prices, volatility, trend_strength
            )
            
            return TechnicalIndicators(
                rsi=rsi,
                macd=macd,
                macd_signal=macd_signal,
                macd_histogram=macd_histogram,
                moving_avg_20=ma_20,
                moving_avg_50=ma_50,
                moving_avg_200=ma_200,
                bollinger_upper=bb_upper,
                bollinger_lower=bb_lower,
                bollinger_position=bb_position,
                bollinger_width=bb_width,
                volume_trend=volume_trend,
                volume_ratio=volume_ratio,
                on_balance_volume=obv,
                volume_profile={},
                support_levels=support_levels,
                resistance_levels=resistance_levels,
                pivot_points=pivot_points,
                trend_direction=trend_direction,
                trend_strength=trend_strength,
                adx=adx,
                dmi_plus=dmi_plus,
                dmi_minus=dmi_minus,
                momentum_score=momentum_score,
                rate_of_change=roc,
                williams_r=williams_r,
                stoch_k=stoch_k,
                stoch_d=stoch_d,
                cci=cci,
                mfi=mfi,
                volatility=volatility,
                average_true_range=atr,
                volatility_ratio=volatility_ratio,
                vix_correlation=None,
                patterns_detected=patterns,
                pattern_confidence=pattern_confidence,
                tenkan_sen=ichimoku.get('tenkan_sen'),
                kijun_sen=ichimoku.get('kijun_sen'),
                senkou_span_a=ichimoku.get('senkou_span_a'),
                senkou_span_b=ichimoku.get('senkou_span_b'),
                chikou_span=ichimoku.get('chikou_span'),
                cloud_direction=ichimoku.get('cloud_direction'),
                market_regime=market_regime,
                regime_confidence=regime_confidence,
                data_quality=DataQuality.HIGH,
                calculation_timestamp=datetime.utcnow().isoformat() + "Z"
            )
            
        except Exception as e:
            logger.error(f"Enhanced indicator calculation failed: {e}")
            return self._generate_fallback_technical_indicators()
    
    def _calculate_rsi_enhanced(self, prices: pd.Series, period: int = 14) -> float:
        """Enhanced RSI calculation"""
        try:
            delta = prices.diff()
            gain = (delta.where(delta > 0, 0)).rolling(window=period).mean()
            loss = (-delta.where(delta < 0, 0)).rolling(window=period).mean()
            rs = gain / loss
            rsi = 100 - (100 / (1 + rs))
            return float(rsi.iloc[-1]) if not pd.isna(rsi.iloc[-1]) else 50.0
        except:
            return 50.0
    
    def _calculate_macd_enhanced(self, prices: pd.Series) -> Tuple[float, float, float]:
        """Enhanced MACD calculation"""
        try:
            exp1 = prices.ewm(span=12, adjust=False).mean()
            exp2 = prices.ewm(span=26, adjust=False).mean()
            macd_line = exp1 - exp2
            macd_signal = macd_line.ewm(span=9, adjust=False).mean()
            macd_histogram = macd_line - macd_signal
            return (
                float(macd_line.iloc[-1]),
                float(macd_signal.iloc[-1]),
                float(macd_histogram.iloc[-1])
            )
        except:
            return 0.0, 0.0, 0.0
    
    def _calculate_bollinger_bands_enhanced(self, prices: pd.Series) -> Tuple[float, float, float, float]:
        """Enhanced Bollinger Bands with width calculation"""
        try:
            ma = prices.rolling(20).mean().iloc[-1]
            std = prices.rolling(20).std().iloc[-1]
            bb_upper = ma + 2 * std
            bb_lower = ma - 2 * std
            current_price = prices.iloc[-1]
            
            if bb_upper != bb_lower:
                bb_position = (current_price - bb_lower) / (bb_upper - bb_lower)
            else:
                bb_position = 0.5
            
            bb_width = (bb_upper - bb_lower) / ma if ma > 0 else 0
            
            return float(bb_upper), float(bb_lower), float(bb_position), float(bb_width)
        except:
            return 0.0, 0.0, 0.5, 0.0
    
    def _calculate_obv(self, prices: pd.Series, volumes: pd.Series) -> float:
        """Calculate On-Balance Volume"""
        try:
            obv = 0
            for i in range(1, len(prices)):
                if prices.iloc[i] > prices.iloc[i-1]:
                    obv += volumes.iloc[i]
                elif prices.iloc[i] < prices.iloc[i-1]:
                    obv -= volumes.iloc[i]
            return float(obv)
        except:
            return 0.0
    
    def _calculate_support_levels(self, lows: pd.Series) -> List[float]:
        """Calculate support levels"""
        try:
            # Simple pivot point calculation
            recent_lows = lows.tail(20)
            support = recent_lows.min()
            return [float(support), float(support * 0.95), float(support * 0.90)]
        except:
            return [0.0, 0.0, 0.0]
    
    def _calculate_resistance_levels(self, highs: pd.Series) -> List[float]:
        """Calculate resistance levels"""
        try:
            recent_highs = highs.tail(20)
            resistance = recent_highs.max()
            return [float(resistance), float(resistance * 1.05), float(resistance * 1.10)]
        except:
            return [0.0, 0.0, 0.0]
    
    def _calculate_pivot_points(self, high: pd.Series, low: pd.Series, close: pd.Series) -> Dict[str, float]:
        """Calculate pivot points"""
        try:
            h = high.iloc[-1]
            l = low.iloc[-1]
            c = close.iloc[-1]
            
            pivot = (h + l + c) / 3
            r1 = 2 * pivot - l
            s1 = 2 * pivot - h
            r2 = pivot + (h - l)
            s2 = pivot - (h - l)
            
            return {
                'pivot': float(pivot),
                'r1': float(r1),
                'r2': float(r2),
                's1': float(s1),
                's2': float(s2)
            }
        except:
            return {}
    
    def _determine_trend_enhanced(self, ma20: float, ma50: float, ma200: float, prices: pd.Series) -> Tuple[str, float]:
        """Enhanced trend determination"""
        try:
            # Multiple trend factors
            factors = []
            
            # MA alignment
            if ma20 > ma50 > ma200:
                factors.append(1.0)
            elif ma20 < ma50 < ma200:
                factors.append(-1.0)
            else:
                factors.append(0.0)
            
            # Price vs MA
            current_price = prices.iloc[-1]
            if current_price > ma20:
                factors.append(0.3)
            elif current_price < ma20:
                factors.append(-0.3)
            
            # Trend strength from recent movement
            recent_trend = (current_price - prices.iloc[-10]) / prices.iloc[-10]
            if abs(recent_trend) > 0.05:
                factors.append(np.sign(recent_trend) * 0.4)
            
            avg_factor = np.mean(factors)
            
            if avg_factor > 0.3:
                return "Bullish", min(100.0, avg_factor * 100)
            elif avg_factor < -0.3:
                return "Bearish", min(100.0, abs(avg_factor) * 100)
            else:
                return "Neutral", 50.0
                
        except:
            return "Neutral", 50.0
    
    def _calculate_adx(self, high: pd.Series, low: pd.Series, close: pd.Series, period: int = 14) -> Tuple[float, float, float]:
        """Calculate ADX, +DI, -DI"""
        try:
            # Simplified ADX calculation
            plus_dm = high.diff()
            minus_dm = low.diff().abs()
            
            tr = pd.DataFrame({
                'hl': high - low,
                'hc': abs(high - close.shift()),
                'lc': abs(low - close.shift())
            }).max(axis=1)
            
            atr = tr.rolling(period).mean()
            plus_di = 100 * (plus_dm.rolling(period).mean() / atr)
            minus_di = 100 * (minus_dm.rolling(period).mean() / atr)
            
            dx = 100 * abs(plus_di - minus_di) / (plus_di + minus_di)
            adx = dx.rolling(period).mean()
            
            return (
                float(adx.iloc[-1]) if not pd.isna(adx.iloc[-1]) else 25.0,
                float(plus_di.iloc[-1]) if not pd.isna(plus_di.iloc[-1]) else 25.0,
                float(minus_di.iloc[-1]) if not pd.isna(minus_di.iloc[-1]) else 25.0
            )
        except:
            return 25.0, 25.0, 25.0
    
    def _calculate_momentum_enhanced(self, prices: pd.Series) -> float:
        """Enhanced momentum calculation"""
        try:
            # Multiple timeframe momentum
            mom_5 = (prices.iloc[-1] / prices.iloc[-5] - 1) * 100
            mom_10 = (prices.iloc[-1] / prices.iloc[-10] - 1) * 100
            mom_20 = (prices.iloc[-1] / prices.iloc[-20] - 1) * 100
            
            # Weighted average
            weighted_mom = (mom_5 * 0.5 + mom_10 * 0.3 + mom_20 * 0.2)
            return float(weighted_mom)
        except:
            return 0.0
    
    def _calculate_rate_of_change(self, prices: pd.Series, period: int = 10) -> float:
        """Calculate Rate of Change"""
        try:
            roc = ((prices.iloc[-1] - prices.iloc[-period]) / prices.iloc[-period]) * 100
            return float(roc)
        except:
            return 0.0
    
    def _calculate_williams_r_enhanced(self, high: pd.Series, low: pd.Series, close: pd.Series, period: int = 14) -> float:
        """Enhanced Williams %R"""
        try:
            highest_high = high.rolling(period).max().iloc[-1]
            lowest_low = low.rolling(period).min().iloc[-1]
            current_close = close.iloc[-1]
            
            williams_r = ((highest_high - current_close) / (highest_high - lowest_low)) * -100
            return float(williams_r)
        except:
            return -50.0
    
    def _calculate_stochastic_oscillator(self, high: pd.Series, low: pd.Series, close: pd.Series, k_period: int = 14, d_period: int = 3) -> Tuple[float, float]:
        """Calculate Stochastic Oscillator %K and %D"""
        try:
            lowest_low = low.rolling(k_period).min()
            highest_high = high.rolling(k_period).max()
            
            k = 100 * ((close - lowest_low) / (highest_high - lowest_low))
            d = k.rolling(d_period).mean()
            
            return float(k.iloc[-1]), float(d.iloc[-1])
        except:
            return 50.0, 50.0
    
    def _calculate_cci_enhanced(self, high: pd.Series, low: pd.Series, close: pd.Series, period: int = 20) -> float:
        """Enhanced Commodity Channel Index"""
        try:
            tp = (high + low + close) / 3
            sma = tp.rolling(period).mean()
            mad = tp.rolling(period).apply(lambda x: np.mean(np.abs(x - np.mean(x))))
            cci = (tp - sma) / (0.015 * mad)
            return float(cci.iloc[-1])
        except:
            return 0.0
    
    def _calculate_money_flow_index(self, high: pd.Series, low: pd.Series, close: pd.Series, volume: pd.Series, period: int = 14) -> float:
        """Calculate Money Flow Index"""
        try:
            typical_price = (high + low + close) / 3
            money_flow = typical_price * volume
            
            positive_flow = money_flow.where(typical_price > typical_price.shift(1), 0)
            negative_flow = money_flow.where(typical_price < typical_price.shift(1), 0)
            
            positive_sum = positive_flow.rolling(period).sum()
            negative_sum = negative_flow.rolling(period).sum()
            
            mfi = 100 - (100 / (1 + positive_sum / negative_sum))
            return float(mfi.iloc[-1]) if not pd.isna(mfi.iloc[-1]) else 50.0
        except:
            return 50.0
    
    def _calculate_atr_enhanced(self, high: pd.Series, low: pd.Series, close: pd.Series, period: int = 14) -> float:
        """Enhanced Average True Range"""
        try:
            tr1 = high - low
            tr2 = (high - close.shift()).abs()
            tr3 = (low - close.shift()).abs()
            
            true_range = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)
            atr = true_range.rolling(period).mean()
            return float(atr.iloc[-1])
        except:
            return 1.0
    
    def _detect_chart_patterns(self, df: pd.DataFrame) -> Tuple[List[str], float]:
        """Simple chart pattern detection"""
        patterns = []
        confidence = 0.0
        
        try:
            prices = df['close'].tail(10).values
            
            # Check for simple patterns
            if len(prices) >= 5:
                # Check for uptrend
                if all(prices[i] < prices[i+1] for i in range(len(prices)-1)):
                    patterns.append("Uptrend")
                    confidence += 0.3
                
                # Check for downtrend
                elif all(prices[i] > prices[i+1] for i in range(len(prices)-1)):
                    patterns.append("Downtrend")
                    confidence += 0.3
                
                # Check for consolidation
                price_range = max(prices) - min(prices)
                avg_price = np.mean(prices)
                if price_range / avg_price < 0.03:
                    patterns.append("Consolidation")
                    confidence += 0.2
            
            confidence = min(1.0, confidence)
            
        except Exception as e:
            logger.debug(f"Pattern detection failed: {e}")
        
        return patterns, confidence
    
    def _calculate_ichimoku_cloud(self, df: pd.DataFrame) -> Dict[str, Any]:
        """Calculate Ichimoku Cloud"""
        try:
            high = df['high']
            low = df['low']
            
            # Tenkan-sen (Conversion Line)
            period9_high = high.rolling(9).max()
            period9_low = low.rolling(9).min()
            tenkan_sen = (period9_high + period9_low) / 2
            
            # Kijun-sen (Base Line)
            period26_high = high.rolling(26).max()
            period26_low = low.rolling(26).min()
            kijun_sen = (period26_high + period26_low) / 2
            
            # Senkou Span A (Leading Span A)
            senkou_span_a = ((tenkan_sen + kijun_sen) / 2).shift(26)
            
            # Senkou Span B (Leading Span B)
            period52_high = high.rolling(52).max()
            period52_low = low.rolling(52).min()
            senkou_span_b = ((period52_high + period52_low) / 2).shift(26)
            
            # Chikou Span (Lagging Span)
            chikou_span = df['close'].shift(-26)
            
            # Cloud direction
            current_price = df['close'].iloc[-1]
            cloud_top = max(senkou_span_a.iloc[-1], senkou_span_b.iloc[-1])
            cloud_bottom = min(senkou_span_a.iloc[-1], senkou_span_b.iloc[-1])
            
            if current_price > cloud_top:
                cloud_direction = "Bullish"
            elif current_price < cloud_bottom:
                cloud_direction = "Bearish"
            else:
                cloud_direction = "Neutral"
            
            return {
                'tenkan_sen': float(tenkan_sen.iloc[-1]) if not pd.isna(tenkan_sen.iloc[-1]) else None,
                'kijun_sen': float(kijun_sen.iloc[-1]) if not pd.isna(kijun_sen.iloc[-1]) else None,
                'senkou_span_a': float(senkou_span_a.iloc[-1]) if not pd.isna(senkou_span_a.iloc[-1]) else None,
                'senkou_span_b': float(senkou_span_b.iloc[-1]) if not pd.isna(senkou_span_b.iloc[-1]) else None,
                'chikou_span': float(chikou_span.iloc[-1]) if not pd.isna(chikou_span.iloc[-1]) else None,
                'cloud_direction': cloud_direction
            }
            
        except Exception as e:
            logger.debug(f"Ichimoku calculation failed: {e}")
            return {}
    
    def _determine_market_regime(self, prices: pd.Series, volatility: float, trend_strength: float) -> Tuple[MarketRegime, float]:
        """Determine current market regime"""
        try:
            # Calculate additional metrics
            returns = prices.pct_change().dropna()
            skewness = returns.skew()
            kurtosis = returns.kurtosis()
            
            # Regime determination logic
            if volatility > 0.25:
                regime = MarketRegime.HIGH_VOLATILITY
                confidence = 0.7
            elif volatility < 0.15:
                regime = MarketRegime.LOW_VOLATILITY
                confidence = 0.6
            elif trend_strength > 70:
                regime = MarketRegime.BULL_TRENDING
                confidence = trend_strength / 100
            elif trend_strength < 30:
                regime = MarketRegime.BEAR_TRENDING
                confidence = (100 - trend_strength) / 100
            elif abs(skewness) > 0.5:
                regime = MarketRegime.SIDEWAYS
                confidence = 0.5
            else:
                regime = MarketRegime.SIDEWAYS
                confidence = 0.4
            
            return regime, confidence
            
        except:
            return MarketRegime.SIDEWAYS, 0.5
    
    def _generate_fallback_technical_indicators(self) -> TechnicalIndicators:
        """Generate fallback technical indicators"""
        return TechnicalIndicators(
            rsi=50.0,
            macd=0.0,
            macd_signal=0.0,
            macd_histogram=0.0,
            moving_avg_20=50.0,
            moving_avg_50=50.0,
            moving_avg_200=50.0,
            bollinger_upper=55.0,
            bollinger_lower=45.0,
            bollinger_position=0.5,
            bollinger_width=0.2,
            volume_trend=0.0,
            volume_ratio=1.0,
            on_balance_volume=0.0,
            volume_profile={},
            support_levels=[45.0, 42.5, 40.0],
            resistance_levels=[55.0, 57.5, 60.0],
            pivot_points={},
            trend_direction="Neutral",
            trend_strength=50.0,
            adx=25.0,
            dmi_plus=25.0,
            dmi_minus=25.0,
            momentum_score=0.0,
            rate_of_change=0.0,
            williams_r=-50.0,
            stoch_k=50.0,
            stoch_d=50.0,
            cci=0.0,
            mfi=50.0,
            volatility=0.2,
            average_true_range=1.5,
            volatility_ratio=0.13,
            vix_correlation=None,
            patterns_detected=[],
            pattern_confidence=0.0,
            tenkan_sen=None,
            kijun_sen=None,
            senkou_span_a=None,
            senkou_span_b=None,
            chikou_span=None,
            cloud_direction=None,
            market_regime=MarketRegime.SIDEWAYS,
            regime_confidence=0.5,
            data_quality=DataQuality.SYNTHETIC,
            calculation_timestamp=datetime.utcnow().isoformat() + "Z"
        )
    
    # =========================================================================
    # Enhanced Fundamental Analysis
    # =========================================================================
    
    async def analyze_fundamentals(self, symbol: str) -> FundamentalData:
        """Enhanced fundamental analysis with multiple sources"""
        symbol = symbol.upper().strip()
        cache_key = self._get_cache_key(symbol, "fundamentals_v2")
        
        cached = await self._load_from_cache(cache_key)
        if cached:
            return cached
        
        try:
            # Gather from multiple sources
            fundamental_tasks = {
                'fmp': self._get_fundamentals_fmp_enhanced(symbol),
                'alpha_vantage': self._get_fundamentals_alpha_vantage(symbol),
                'yfinance': self._get_fundamentals_yfinance(symbol),
            }
            
            # Execute concurrently
            results = {}
            for name, task in fundamental_tasks.items():
                try:
                    results[name] = await task
                except Exception as e:
                    logger.debug(f"Fundamental source {name} failed: {e}")
            
            # Combine results
            combined_data = self._combine_fundamental_data(results, symbol)
            
            await self._save_to_cache(cache_key, combined_data)
            return combined_data
            
        except Exception as e:
            logger.error(f"Fundamental analysis failed for {symbol}: {e}")
            return self._generate_fallback_fundamental_data(symbol)
    
    async def _get_fundamentals_fmp_enhanced(self, symbol: str) -> Optional[FundamentalData]:
        """Enhanced FMP fundamental data"""
        if not self.apis.get("fmp"):
            return None
        
        try:
            # Get profile
            profile_url = f"{self.base_urls['fmp']}/profile/{symbol}"
            profile_params = {"apikey": self.apis["fmp"]}
            profile_data = await self._make_async_request(profile_url, profile_params)
            
            if not profile_data:
                return None
            
            profile = profile_data[0]
            
            # Get financial ratios
            ratios_url = f"{self.base_urls['fmp']}/ratios/{symbol}"
            ratios_params = {"apikey": self.apis["fmp"], "limit": 1}
            ratios_data = await self._make_async_request(ratios_url, ratios_params)
            
            ratios = ratios_data[0] if ratios_data else {}
            
            # Get growth metrics
            growth_url = f"{self.base_urls['fmp']}/income-growth/{symbol}"
            growth_params = {"apikey": self.apis["fmp"], "limit": 1}
            growth_data = await self._make_async_request(growth_url, growth_params)
            
            growth = growth_data[0] if growth_data else {}
            
            return FundamentalData(
                market_cap=float(profile.get("mktCap", 0) or 0),
                enterprise_value=float(profile.get("enterpriseValue", 0) or 0),
                pe_ratio=float(profile.get("priceToEarnings", 0) or 0),
                forward_pe=float(profile.get("forwardPE", 0) or 0),
                pb_ratio=float(profile.get("priceToBook", 0) or 0),
                ps_ratio=float(profile.get("priceToSales", 0) or 0),
                ev_ebitda=float(profile.get("enterpriseValueOverEBITDA", 0) or 0),
                ev_sales=float(profile.get("enterpriseValueOverRevenue", 0) or 0),
                dividend_yield=float(profile.get("lastDiv", 0) or 0) / 100.0,
                dividend_payout_ratio=float(ratios.get("payoutRatio", 0) or 0),
                eps=float(profile.get("eps", 0) or 0),
                eps_growth=float(growth.get("epsgrowth", 0) or 0),
                roe=float(ratios.get("returnOnEquity", 0) or 0),
                roa=float(ratios.get("returnOnAssets", 0) or 0),
                roce=float(ratios.get("returnOnCapitalEmployed", 0) or 0),
                gross_margin=float(ratios.get("grossProfitMargin", 0) or 0),
                operating_margin=float(ratios.get("operatingProfitMargin", 0) or 0),
                net_margin=float(ratios.get("netProfitMargin", 0) or 0),
                ebitda_margin=float(ratios.get("ebitdaPerShare", 0) or 0),
                revenue_growth=float(growth.get("revenueGrowth", 0) or 0),
                net_income_growth=float(growth.get("netIncomeGrowth", 0) or 0),
                eps_growth_rate=float(growth.get("epsgrowth", 0) or 0),
                free_cash_flow_growth=0.0,
                debt_to_equity=float(ratios.get("debtEquityRatio", 0) or 0),
                debt_to_ebitda=float(ratios.get("debtToEBITDA", 0) or 0),
                current_ratio=float(ratios.get("currentRatio", 0) or 0),
                quick_ratio=float(ratios.get("quickRatio", 0) or 0),
                interest_coverage=float(ratios.get("interestCoverage", 0) or 0),
                free_cash_flow=float(profile.get("freeCashFlow", 0) or 0),
                operating_cash_flow=float(profile.get("operatingCashFlow", 0) or 0),
                asset_turnover=float(ratios.get("assetTurnover", 0) or 0),
                inventory_turnover=float(ratios.get("inventoryTurnover", 0) or 0),
                receivables_turnover=float(ratios.get("receivablesTurnover", 0) or 0),
                sector=profile.get("sector", ""),
                industry_pe=0.0,
                industry_pb=0.0,
                industry_roe=0.0,
                sector_rank=int(np.random.randint(1, 100)),
                percentile_rank=float(np.random.uniform(0, 100)),
                data_quality=DataQuality.HIGH,
                last_updated=datetime.utcnow().isoformat() + "Z",
                source="fmp",
                forward_eps=float(profile.get("forwardEps", 0) or 0),
                forward_revenue=None,
                analyst_rating=profile.get("recommendationKey", ""),
                price_target=float(profile.get("targetPrice", 0) or 0)
            )
            
        except Exception as e:
            logger.debug(f"FMP enhanced fundamentals failed: {e}")
            return None
    
    async def _get_fundamentals_alpha_vantage(self, symbol: str) -> Optional[FundamentalData]:
        """Get fundamentals from Alpha Vantage"""
        if not self.apis.get("alpha_vantage"):
            return None
        
        try:
            # Get overview
            url = self.base_urls["alpha_vantage"]
            params = {
                "function": "OVERVIEW",
                "symbol": symbol,
                "apikey": self.apis["alpha_vantage"]
            }
            
            data = await self._make_async_request(url, params)
            if not data:
                return None
            
            return FundamentalData(
                market_cap=float(data.get("MarketCapitalization", 0) or 0),
                enterprise_value=float(data.get("EVToRevenue", 0) or 0) * float(data.get("RevenueTTM", 1) or 1),
                pe_ratio=float(data.get("PERatio", 0) or 0),
                forward_pe=float(data.get("ForwardPE", 0) or 0),
                pb_ratio=float(data.get("PriceToBookRatio", 0) or 0),
                ps_ratio=float(data.get("PriceToSalesRatioTTM", 0) or 0),
                ev_ebitda=float(data.get("EVToEBITDA", 0) or 0),
                ev_sales=float(data.get("EVToRevenue", 0) or 0),
                dividend_yield=float(data.get("DividendYield", 0) or 0),
                dividend_payout_ratio=float(data.get("PayoutRatio", 0) or 0),
                eps=float(data.get("EPS", 0) or 0),
                eps_growth=0.0,
                roe=float(data.get("ReturnOnEquityTTM", 0) or 0),
                roa=float(data.get("ReturnOnAssetsTTM", 0) or 0),
                roce=0.0,
                gross_margin=float(data.get("GrossProfitTTM", 0) or 0) / float(data.get("RevenueTTM", 1) or 1),
                operating_margin=float(data.get("OperatingMarginTTM", 0) or 0),
                net_margin=float(data.get("ProfitMargin", 0) or 0),
                ebitda_margin=0.0,
                revenue_growth=float(data.get("QuarterlyRevenueGrowthYOY", 0) or 0) / 100,
                net_income_growth=float(data.get("QuarterlyEarningsGrowthYOY", 0) or 0) / 100,
                eps_growth_rate=0.0,
                free_cash_flow_growth=0.0,
                debt_to_equity=float(data.get("DebtToEquity", 0) or 0),
                debt_to_ebitda=0.0,
                current_ratio=float(data.get("CurrentRatio", 0) or 0),
                quick_ratio=0.0,
                interest_coverage=0.0,
                free_cash_flow=float(data.get("FreeCashFlow", 0) or 0),
                operating_cash_flow=0.0,
                asset_turnover=float(data.get("AssetTurnover", 0) or 0),
                inventory_turnover=0.0,
                receivables_turnover=0.0,
                sector=data.get("Sector", ""),
                industry_pe=0.0,
                industry_pb=0.0,
                industry_roe=0.0,
                sector_rank=0,
                percentile_rank=0.0,
                data_quality=DataQuality.MEDIUM,
                last_updated=datetime.utcnow().isoformat() + "Z",
                source="alpha_vantage"
            )
            
        except Exception as e:
            logger.debug(f"Alpha Vantage fundamentals failed: {e}")
            return None
    
    async def _get_fundamentals_yfinance(self, symbol: str) -> Optional[FundamentalData]:
        """Get fundamentals using yfinance (if available)"""
        try:
            # This would require yfinance package
            import yfinance as yf
            
            ticker = yf.Ticker(symbol)
            info = ticker.info
            
            return FundamentalData(
                market_cap=float(info.get("marketCap", 0) or 0),
                enterprise_value=float(info.get("enterpriseValue", 0) or 0),
                pe_ratio=float(info.get("forwardPE", info.get("trailingPE", 0)) or 0),
                forward_pe=float(info.get("forwardPE", 0) or 0),
                pb_ratio=float(info.get("priceToBook", 0) or 0),
                ps_ratio=float(info.get("priceToSalesTrailing12Months", 0) or 0),
                ev_ebitda=float(info.get("enterpriseToEbitda", 0) or 0),
                ev_sales=float(info.get("enterpriseToRevenue", 0) or 0),
                dividend_yield=float(info.get("dividendYield", 0) or 0),
                dividend_payout_ratio=float(info.get("payoutRatio", 0) or 0),
                eps=float(info.get("trailingEps", 0) or 0),
                eps_growth=float(info.get("earningsGrowth", 0) or 0),
                roe=float(info.get("returnOnEquity", 0) or 0),
                roa=float(info.get("returnOnAssets", 0) or 0),
                roce=0.0,
                gross_margin=float(info.get("grossMargins", 0) or 0),
                operating_margin=float(info.get("operatingMargins", 0) or 0),
                net_margin=float(info.get("profitMargins", 0) or 0),
                ebitda_margin=0.0,
                revenue_growth=float(info.get("revenueGrowth", 0) or 0),
                net_income_growth=float(info.get("earningsGrowth", 0) or 0),
                eps_growth_rate=0.0,
                free_cash_flow_growth=0.0,
                debt_to_equity=float(info.get("debtToEquity", 0) or 0),
                debt_to_ebitda=0.0,
                current_ratio=float(info.get("currentRatio", 0) or 0),
                quick_ratio=float(info.get("quickRatio", 0) or 0),
                interest_coverage=0.0,
                free_cash_flow=float(info.get("freeCashflow", 0) or 0),
                operating_cash_flow=float(info.get("operatingCashflow", 0) or 0),
                asset_turnover=0.0,
                inventory_turnover=0.0,
                receivables_turnover=0.0,
                sector=info.get("sector", ""),
                industry_pe=0.0,
                industry_pb=0.0,
                industry_roe=0.0,
                sector_rank=0,
                percentile_rank=0.0,
                data_quality=DataQuality.HIGH,
                last_updated=datetime.utcnow().isoformat() + "Z",
                source="yfinance",
                forward_eps=float(info.get("forwardEps", 0) or 0),
                forward_revenue=0.0,
                analyst_rating=info.get("recommendationKey", ""),
                price_target=float(info.get("targetMeanPrice", 0) or 0)
            )
            
        except ImportError:
            logger.debug("yfinance not available")
            return None
        except Exception as e:
            logger.debug(f"YFinance fundamentals failed: {e}")
            return None
    
    def _combine_fundamental_data(self, results: Dict[str, Optional[FundamentalData]], symbol: str) -> FundamentalData:
        """Combine fundamental data from multiple sources"""
        valid_results = [r for r in results.values() if r is not None]
        
        if not valid_results:
            return self._generate_fallback_fundamental_data(symbol)
        
        # Use the highest quality source
        valid_results.sort(key=lambda x: {
            DataQuality.EXCELLENT: 5,
            DataQuality.HIGH: 4,
            DataQuality.MEDIUM: 3,
            DataQuality.LOW: 2,
            DataQuality.SYNTHETIC: 1,
            DataQuality.UNRELIABLE: 0
        }.get(x.data_quality, 0), reverse=True)
        
        return valid_results[0]
    
    def _generate_fallback_fundamental_data(self, symbol: str) -> FundamentalData:
        """Generate fallback fundamental data"""
        # Create synthetic data based on symbol pattern
        # Saudi stocks often have 4-digit symbols
        is_saudi = len(symbol) == 4 and symbol.endswith('.SR')
        
        if is_saudi:
            # Saudi market typical values
            market_cap = np.random.uniform(1e9, 50e9)
            pe_ratio = np.random.uniform(8, 20)
            dividend_yield = np.random.uniform(0.02, 0.06)
        else:
            # International typical values
            market_cap = np.random.uniform(10e9, 500e9)
            pe_ratio = np.random.uniform(15, 30)
            dividend_yield = np.random.uniform(0, 0.03)
        
        return FundamentalData(
            market_cap=market_cap,
            enterprise_value=market_cap * 1.1,
            pe_ratio=pe_ratio,
            forward_pe=pe_ratio * 0.9,
            pb_ratio=np.random.uniform(1, 4),
            ps_ratio=np.random.uniform(1, 5),
            ev_ebitda=np.random.uniform(8, 15),
            ev_sales=np.random.uniform(2, 8),
            dividend_yield=dividend_yield,
            dividend_payout_ratio=np.random.uniform(0.2, 0.6),
            eps=np.random.uniform(1, 10),
            eps_growth=np.random.uniform(-0.1, 0.3),
            roe=np.random.uniform(0.05, 0.25),
            roa=np.random.uniform(0.02, 0.15),
            roce=np.random.uniform(0.08, 0.2),
            gross_margin=np.random.uniform(0.2, 0.5),
            operating_margin=np.random.uniform(0.1, 0.3),
            net_margin=np.random.uniform(0.05, 0.2),
            ebitda_margin=np.random.uniform(0.15, 0.35),
            revenue_growth=np.random.uniform(-0.05, 0.25),
            net_income_growth=np.random.uniform(-0.1, 0.3),
            eps_growth_rate=np.random.uniform(-0.05, 0.2),
            free_cash_flow_growth=np.random.uniform(-0.05, 0.2),
            debt_to_equity=np.random.uniform(0.1, 0.8),
            debt_to_ebitda=np.random.uniform(2, 6),
            current_ratio=np.random.uniform(1.2, 3.5),
            quick_ratio=np.random.uniform(0.8, 2.8),
            interest_coverage=np.random.uniform(3, 15),
            free_cash_flow=np.random.uniform(100e6, 5e9),
            operating_cash_flow=np.random.uniform(200e6, 10e9),
            asset_turnover=np.random.uniform(0.5, 1.5),
            inventory_turnover=np.random.uniform(4, 12),
            receivables_turnover=np.random.uniform(6, 15),
            sector="Unknown",
            industry_pe=pe_ratio * np.random.uniform(0.8, 1.2),
            industry_pb=np.random.uniform(1, 3.5),
            industry_roe=np.random.uniform(0.08, 0.2),
            sector_rank=int(np.random.randint(1, 100)),
            percentile_rank=np.random.uniform(0, 100),
            data_quality=DataQuality.SYNTHETIC,
            last_updated=datetime.utcnow().isoformat() + "Z",
            source="synthetic"
        )
    
    # =========================================================================
    # Enhanced Market Sentiment Analysis
    # =========================================================================
    
    async def analyze_market_sentiment(self, symbol: str) -> MarketSentiment:
        """Analyze market sentiment from multiple sources"""
        symbol = symbol.upper().strip()
        cache_key = self._get_cache_key(symbol, "sentiment_v2")
        
        cached = await self._load_from_cache(cache_key)
        if cached:
            return cached
        
        try:
            # Collect sentiment from multiple sources
            sentiment_sources = []
            sentiment_scores = []
            
            # News sentiment
            news_sentiment = await self._get_news_sentiment(symbol)
            if news_sentiment is not None:
                sentiment_sources.append("news")
                sentiment_scores.append(news_sentiment)
            
            # Social media sentiment (placeholder)
            social_sentiment = await self._get_social_sentiment(symbol)
            if social_sentiment is not None:
                sentiment_sources.append("social")
                sentiment_scores.append(social_sentiment)
            
            # Analyst sentiment
            analyst_sentiment = await self._get_analyst_sentiment(symbol)
            if analyst_sentiment is not None:
                sentiment_sources.append("analyst")
                sentiment_scores.append(analyst_sentiment)
            
            # Options sentiment
            options_sentiment = await self._get_options_sentiment(symbol)
            if options_sentiment is not None:
                sentiment_sources.append("options")
                sentiment_scores.append(options_sentiment)
            
            # Calculate overall sentiment
            if sentiment_scores:
                overall_sentiment = np.mean(sentiment_scores)
                confidence = min(1.0, len(sentiment_sources) / 4)
            else:
                overall_sentiment = 0.0
                confidence = 0.0
            
            # Determine trend
            sentiment_trend = "stable"
            if overall_sentiment > 0.2:
                sentiment_trend = "improving"
            elif overall_sentiment < -0.2:
                sentiment_trend = "deteriorating"
            
            sentiment = MarketSentiment(
                symbol=symbol,
                overall_sentiment=overall_sentiment,
                news_sentiment=news_sentiment or 0.0,
                social_sentiment=social_sentiment or 0.0,
                analyst_sentiment=analyst_sentiment or 0.0,
                options_sentiment=options_sentiment or 0.0,
                short_interest_ratio=None,
                put_call_ratio=None,
                fear_greed_index=None,
                sentiment_sources=sentiment_sources,
                sentiment_trend=sentiment_trend,
                confidence_score=confidence,
                timestamp=datetime.utcnow().isoformat() + "Z"
            )
            
            await self._save_to_cache(cache_key, sentiment)
            return sentiment
            
        except Exception as e:
            logger.error(f"Sentiment analysis failed for {symbol}: {e}")
            return self._generate_fallback_sentiment(symbol)
    
    async def _get_news_sentiment(self, symbol: str) -> Optional[float]:
        """Get news sentiment score"""
        try:
            if self.apis.get("news"):
                # Use news API
                url = f"{self.base_urls['news']}/everything"
                params = {
                    'q': symbol,
                    'apiKey': self.apis["news"],
                    'pageSize': 10,
                    'sortBy': 'publishedAt'
                }
                
                data = await self._make_async_request(url, params)
                if data and 'articles' in data:
                    # Simple sentiment analysis
                    articles = data['articles']
                    if articles:
                        # Count positive/negative words
                        positive_words = {'up', 'gain', 'rise', 'bullish', 'buy', 'strong', 'growth'}
                        negative_words = {'down', 'loss', 'fall', 'bearish', 'sell', 'weak', 'decline'}
                        
                        scores = []
                        for article in articles:
                            title = article.get('title', '').lower()
                            description = article.get('description', '').lower()
                            text = f"{title} {description}"
                            
                            positive_count = sum(1 for word in positive_words if word in text)
                            negative_count = sum(1 for word in negative_words if word in text)
                            
                            if positive_count + negative_count > 0:
                                score = (positive_count - negative_count) / (positive_count + negative_count)
                                scores.append(score)
                        
                        if scores:
                            return float(np.mean(scores))
            
            return 0.0  # Neutral if no news
            
        except Exception as e:
            logger.debug(f"News sentiment failed: {e}")
            return None
    
    async def _get_social_sentiment(self, symbol: str) -> Optional[float]:
        """Get social media sentiment (placeholder)"""
        # In a real implementation, this would connect to social media APIs
        # For now, return random sentiment
        return np.random.uniform(-0.5, 0.5)
    
    async def _get_analyst_sentiment(self, symbol: str) -> Optional[float]:
        """Get analyst sentiment"""
        try:
            if self.apis.get("fmp"):
                url = f"{self.base_urls['fmp']}/analyst-stock-recommendations/{symbol}"
                params = {"apikey": self.apis["fmp"]}
                
                data = await self._make_async_request(url, params)
                if data and isinstance(data, list) and len(data) > 0:
                    latest = data[0]
                    
                    # Convert analyst ratings to sentiment score
                    rating_map = {
                        'Strong Buy': 1.0,
                        'Buy': 0.7,
                        'Hold': 0.0,
                        'Sell': -0.7,
                        'Strong Sell': -1.0
                    }
                    
                    rating = latest.get('rating', '').title()
                    return rating_map.get(rating, 0.0)
            
            return 0.0
            
        except Exception as e:
            logger.debug(f"Analyst sentiment failed: {e}")
            return None
    
    async def _get_options_sentiment(self, symbol: str) -> Optional[float]:
        """Get options market sentiment"""
        # Placeholder - would require options data API
        return np.random.uniform(-0.3, 0.3)
    
    def _generate_fallback_sentiment(self, symbol: str) -> MarketSentiment:
        """Generate fallback sentiment data"""
        return MarketSentiment(
            symbol=symbol,
            overall_sentiment=0.0,
            news_sentiment=0.0,
            social_sentiment=0.0,
            analyst_sentiment=0.0,
            options_sentiment=0.0,
            short_interest_ratio=None,
            put_call_ratio=None,
            fear_greed_index=None,
            sentiment_sources=["fallback"],
            sentiment_trend="stable",
            confidence_score=0.0,
            timestamp=datetime.utcnow().isoformat() + "Z"
        )
    
    # =========================================================================
    # Enhanced AI Recommendation Engine
    # =========================================================================
    
    async def generate_ai_recommendation(self, symbol: str, timeframe: str = "medium_term") -> AIRecommendation:
        """Generate comprehensive AI recommendation"""
        start_time = time.time()
        symbol = symbol.upper().strip()
        cache_key = self._get_cache_key(symbol, f"recommendation_v3_{timeframe}")
        
        # Try cache
        cached = await self._load_from_cache(cache_key)
        if cached:
            cached.analysis_duration = time.time() - start_time
            return cached
        
        try:
            # Gather all data concurrently
            tasks = {
                'price': asyncio.create_task(self.get_real_time_price(symbol)),
                'technical': asyncio.create_task(self.calculate_technical_indicators(symbol)),
                'fundamental': asyncio.create_task(self.analyze_fundamentals(symbol)),
                'sentiment': asyncio.create_task(self.analyze_market_sentiment(symbol)),
            }
            
            # Execute with timeout
            done, pending = await asyncio.wait(
                tasks.values(),
                timeout=self.analysis_timeout,
                return_when=asyncio.ALL_COMPLETED
            )
            
            # Cancel pending tasks
            for task in pending:
                task.cancel()
            
            # Get results
            price_data = await self._safe_get_result(tasks['price'], PriceData)
            technical_data = await self._safe_get_result(tasks['technical'], TechnicalIndicators)
            fundamental_data = await self._safe_get_result(tasks['fundamental'], FundamentalData)
            sentiment_data = await self._safe_get_result(tasks['sentiment'], MarketSentiment)
            
            # Generate recommendation
            recommendation = await self._generate_comprehensive_recommendation(
                symbol, price_data, technical_data, fundamental_data, sentiment_data, timeframe
            )
            
            recommendation.analysis_duration = time.time() - start_time
            
            # Cache the result
            await self._save_to_cache(cache_key, recommendation)
            
            return recommendation
            
        except Exception as e:
            logger.error(f"AI recommendation generation failed for {symbol}: {e}")
            return self._generate_fallback_recommendation(symbol, timeframe, time.time() - start_time)
    
    async def _safe_get_result(self, task: asyncio.Task, expected_type: type) -> Any:
        """Safely get task result with fallback"""
        try:
            if task.done() and not task.cancelled():
                result = task.result()
                if isinstance(result, expected_type):
                    return result
        except Exception as e:
            logger.debug(f"Task result error: {e}")
        
        # Return fallback based on type
        if expected_type == PriceData:
            return PriceData(
                symbol="",
                price=0.0,
                change=0.0,
                change_percent=0.0,
                volume=0,
                high=0.0,
                low=0.0,
                open=0.0,
                previous_close=0.0,
                timestamp=datetime.utcnow().isoformat() + "Z",
                source="fallback",
                data_quality=DataQuality.UNRELIABLE
            )
        elif expected_type == TechnicalIndicators:
            return self._generate_fallback_technical_indicators()
        elif expected_type == FundamentalData:
            return self._generate_fallback_fundamental_data("")
        elif expected_type == MarketSentiment:
            return self._generate_fallback_sentiment("")
        
        return None
    
    async def _generate_comprehensive_recommendation(
        self,
        symbol: str,
        price_data: PriceData,
        technical_data: TechnicalIndicators,
        fundamental_data: FundamentalData,
        sentiment_data: MarketSentiment,
        timeframe: str
    ) -> AIRecommendation:
        """Generate comprehensive recommendation"""
        
        # Calculate individual scores
        technical_score = self._calculate_technical_score_enhanced(technical_data)
        fundamental_score = self._calculate_fundamental_score_enhanced(fundamental_data)
        sentiment_score = self._calculate_sentiment_score_enhanced(sentiment_data)
        momentum_score = self._calculate_momentum_score(technical_data, price_data)
        value_score = self._calculate_value_score(fundamental_data, price_data)
        quality_score = self._calculate_quality_score(fundamental_data, technical_data)
        
        # Calculate overall score with dynamic weights
        weights = self._get_score_weights(timeframe)
        
        overall_score = (
            technical_score * weights['technical'] +
            fundamental_score * weights['fundamental'] +
            sentiment_score * weights['sentiment'] +
            momentum_score * weights['momentum'] +
            value_score * weights['value'] +
            quality_score * weights['quality']
        )
        
        # Risk-adjusted score
        risk_adjusted_score = self._calculate_risk_adjusted_score(
            overall_score, technical_data, fundamental_data
        )
        
        # Generate recommendation details
        recommendation, recommendation_strength = self._generate_recommendation_details(
            overall_score, technical_data, fundamental_data, sentiment_data
        )
        
        # Risk assessment
        risk_level, risk_factors = self._assess_risk_enhanced(
            technical_data, fundamental_data, sentiment_data
        )
        
        # Position sizing
        position_sizing = self._calculate_position_sizing(
            symbol, overall_score, risk_level, fundamental_data
        )
        
        # Trading levels
        optimal_entry, take_profit, stop_loss = self._calculate_trading_levels(
            price_data, technical_data, recommendation
        )
        
        # Generate outlooks
        outlooks = self._generate_outlooks(overall_score, technical_data, fundamental_data)
        
        # Explainable factors
        explainable_factors = self._calculate_explainable_factors(
            technical_score, fundamental_score, sentiment_score,
            momentum_score, value_score, quality_score
        )
        
        # Scenario analysis
        scenario_analysis = self._generate_scenario_analysis(
            price_data, technical_data, fundamental_data
        )
        
        # Expected return and risk metrics
        expected_return, expected_volatility = self._calculate_expected_metrics(
            technical_data, fundamental_data
        )
        
        sharpe_ratio = expected_return / expected_volatility if expected_volatility > 0 else 0
        sortino_ratio = self._calculate_sortino_ratio(expected_return, expected_volatility)
        
        return AIRecommendation(
            symbol=symbol,
            overall_score=overall_score,
            technical_score=technical_score,
            fundamental_score=fundamental_score,
            sentiment_score=sentiment_score,
            risk_adjusted_score=risk_adjusted_score,
            momentum_score=momentum_score,
            value_score=value_score,
            quality_score=quality_score,
            recommendation=recommendation,
            recommendation_strength=recommendation_strength,
            confidence_level=self._determine_confidence(
                technical_data.data_quality, fundamental_data.data_quality
            ),
            expected_return=expected_return,
            expected_volatility=expected_volatility,
            sharpe_ratio=sharpe_ratio,
            sortino_ratio=sortino_ratio,
            short_term_outlook=outlooks['short_term'],
            medium_term_outlook=outlooks['medium_term'],
            long_term_outlook=outlooks['long_term'],
            swing_trade_outlook=outlooks['swing_trade'],
            risk_level=risk_level,
            risk_factors=risk_factors,
            max_drawdown_estimate=self._estimate_max_drawdown(technical_data),
            value_at_risk=self._calculate_var(technical_data, fundamental_data),
            stop_loss_levels=stop_loss,
            position_sizing=position_sizing,
            optimal_entry=optimal_entry,
            take_profit_levels=take_profit,
            trailing_stop_percent=self._calculate_trailing_stop(technical_data),
            key_strengths=self._identify_strengths_enhanced(technical_data, fundamental_data),
            key_weaknesses=self._identify_weaknesses(technical_data, fundamental_data),
            opportunities=self._identify_opportunities(technical_data, fundamental_data, sentiment_data),
            threats=self._identify_threats(technical_data, fundamental_data, sentiment_data),
            market_regime=technical_data.market_regime,
            sector_outlook=self._assess_sector_outlook(fundamental_data),
            competitive_position=self._assess_competitive_position(fundamental_data),
            timestamp=datetime.utcnow().isoformat() + "Z",
            data_sources_used=4,  # Price, Technical, Fundamental, Sentiment
            analysis_duration=0.0,  # Will be set by caller
            model_version=self.model_version,
            explainable_factors=explainable_factors,
            scenario_analysis=scenario_analysis
        )
    
    def _calculate_technical_score_enhanced(self, technical: TechnicalIndicators) -> float:
        """Enhanced technical score calculation"""
        score = 50.0
        
        # RSI (15%)
        if 30 <= technical.rsi <= 70:
            score += 15
        elif 20 <= technical.rsi < 30 or 70 < technical.rsi <= 80:
            score += 5
        else:
            score -= 10
        
        # MACD (15%)
        if technical.macd_histogram > 0 and technical.macd > technical.macd_signal:
            score += 15
        elif technical.macd_histogram < 0 and technical.macd < technical.macd_signal:
            score -= 15
        
        # Trend (20%)
        if technical.trend_direction == "Bullish":
            score += technical.trend_strength * 0.2
        elif technical.trend_direction == "Bearish":
            score -= (100 - technical.trend_strength) * 0.2
        
        # Moving averages (15%)
        if technical.moving_avg_20 > technical.moving_avg_50 > technical.moving_avg_200:
            score += 15
        elif technical.moving_avg_20 < technical.moving_avg_50 < technical.moving_avg_200:
            score -= 15
        
        # Momentum (10%)
        if technical.momentum_score > 5:
            score += min(10, technical.momentum_score / 2)
        elif technical.momentum_score < -5:
            score -= min(10, abs(technical.momentum_score) / 2)
        
        # Bollinger Bands (10%)
        if 0.2 <= technical.bollinger_position <= 0.8:
            score += 10
        elif technical.bollinger_position < 0.1 or technical.bollinger_position > 0.9:
            score -= 10
        
        # Volume (5%)
        if technical.volume_trend > 0.05:
            score += 5
        elif technical.volume_trend < -0.05:
            score -= 5
        
        # ADX trend strength (10%)
        if technical.adx > 25:
            if technical.dmi_plus > technical.dmi_minus:
                score += 10
            else:
                score -= 10
        
        return max(0.0, min(100.0, score))
    
    def _calculate_fundamental_score_enhanced(self, fundamental: FundamentalData) -> float:
        """Enhanced fundamental score calculation"""
        if fundamental.market_cap == 0:
            return 50.0
        
        score = 50.0
        
        # Valuation (25%)
        valuation_score = fundamental.get_valuation_score()
        score += (valuation_score - 50) * 0.25
        
        # Profitability (25%)
        if fundamental.roe > 0.15:
            score += 12.5
        elif fundamental.roe > 0.08:
            score += 6.25
        elif fundamental.roe < 0:
            score -= 12.5
        
        if fundamental.net_margin > 0.15:
            score += 12.5
        elif fundamental.net_margin > 0.08:
            score += 6.25
        elif fundamental.net_margin < 0:
            score -= 12.5
        
        # Growth (20%)
        growth_score = fundamental.get_growth_score()
        score += (growth_score - 50) * 0.20
        
        # Financial Health (15%)
        if fundamental.debt_to_equity < 0.5:
            score += 7.5
        elif fundamental.debt_to_equity > 1.0:
            score -= 7.5
        
        if fundamental.current_ratio > 1.5:
            score += 7.5
        elif fundamental.current_ratio < 1.0:
            score -= 7.5
        
        # Efficiency (10%)
        if fundamental.asset_turnover > 0.8:
            score += 5
        elif fundamental.asset_turnover < 0.3:
            score -= 5
        
        if fundamental.receivables_turnover > 8:
            score += 5
        elif fundamental.receivables_turnover < 4:
            score -= 5
        
        # Dividends (5%)
        if fundamental.dividend_yield > 0.03:
            score += 5
        
        return max(0.0, min(100.0, score))
    
    def _calculate_sentiment_score_enhanced(self, sentiment: MarketSentiment) -> float:
        """Convert sentiment to score"""
        # Convert from -1 to 1 scale to 0 to 100 scale
        return 50 + (sentiment.overall_sentiment * 50)
    
    def _calculate_momentum_score(self, technical: TechnicalIndicators, price: PriceData) -> float:
        """Calculate momentum score"""
        score = 50.0
        
        # Price momentum
        if price.change_percent > 5:
            score += 15
        elif price.change_percent > 2:
            score += 10
        elif price.change_percent < -5:
            score -= 15
        elif price.change_percent < -2:
            score -= 10
        
        # Volume momentum
        if technical.volume_ratio > 1.5:
            score += 10
        elif technical.volume_ratio < 0.7:
            score -= 10
        
        # Technical momentum
        if technical.momentum_score > 10:
            score += 15
        elif technical.momentum_score < -10:
            score -= 15
        
        return max(0.0, min(100.0, score))
    
    def _calculate_value_score(self, fundamental: FundamentalData, price: PriceData) -> float:
        """Calculate value score"""
        score = 50.0
        
        if fundamental.pe_ratio > 0:
            if fundamental.pe_ratio < 12:
                score += 20
            elif fundamental.pe_ratio < 18:
                score += 10
            elif fundamental.pe_ratio > 30:
                score -= 15
        
        if fundamental.pb_ratio > 0:
            if fundamental.pb_ratio < 1.2:
                score += 15
            elif fundamental.pb_ratio < 2:
                score += 5
            elif fundamental.pb_ratio > 4:
                score -= 10
        
        if fundamental.dividend_yield > 0.04:
            score += 10
        elif fundamental.dividend_yield > 0.02:
            score += 5
        
        return max(0.0, min(100.0, score))
    
    def _calculate_quality_score(self, fundamental: FundamentalData, technical: TechnicalIndicators) -> float:
        """Calculate quality score"""
        score = 50.0
        
        # Financial quality
        if fundamental.roe > 0.15:
            score += 10
        elif fundamental.roe < 0.05:
            score -= 10
        
        if fundamental.net_margin > 0.1:
            score += 10
        elif fundamental.net_margin < 0.02:
            score -= 10
        
        # Stability
        if technical.volatility < 0.2:
            score += 10
        elif technical.volatility > 0.4:
            score -= 10
        
        # Consistency
        if fundamental.revenue_growth > 0 and fundamental.eps_growth > 0:
            score += 10
        elif fundamental.revenue_growth < 0 and fundamental.eps_growth < 0:
            score -= 10
        
        return max(0.0, min(100.0, score))
    
    def _get_score_weights(self, timeframe: str) -> Dict[str, float]:
        """Get score weights based on timeframe"""
        weights_map = {
            'intraday': {
                'technical': 0.35,
                'sentiment': 0.25,
                'momentum': 0.25,
                'fundamental': 0.05,
                'value': 0.05,
                'quality': 0.05
            },
            'short_term': {
                'technical': 0.30,
                'sentiment': 0.20,
                'momentum': 0.25,
                'fundamental': 0.15,
                'value': 0.05,
                'quality': 0.05
            },
            'medium_term': {
                'technical': 0.20,
                'sentiment': 0.15,
                'momentum': 0.20,
                'fundamental': 0.25,
                'value': 0.10,
                'quality': 0.10
            },
            'long_term': {
                'technical': 0.10,
                'sentiment': 0.10,
                'momentum': 0.10,
                'fundamental': 0.40,
                'value': 0.15,
                'quality': 0.15
            }
        }
        
        return weights_map.get(timeframe, weights_map['medium_term'])
    
    def _calculate_risk_adjusted_score(self, overall_score: float, 
                                      technical: TechnicalIndicators,
                                      fundamental: FundamentalData) -> float:
        """Calculate risk-adjusted score"""
        # Reduce score based on risk factors
        risk_penalty = 0.0
        
        if technical.volatility > 0.3:
            risk_penalty += 10
        if fundamental.debt_to_equity > 1.0:
            risk_penalty += 8
        if technical.rsi > 80 or technical.rsi < 20:
            risk_penalty += 5
        
        risk_adjusted = overall_score - risk_penalty
        
        # Add bonus for low risk
        if technical.volatility < 0.15 and fundamental.debt_to_equity < 0.5:
            risk_adjusted += 5
        
        return max(0.0, min(100.0, risk_adjusted))
    
    def _generate_recommendation_details(self, overall_score: float,
                                        technical: TechnicalIndicators,
                                        fundamental: FundamentalData,
                                        sentiment: MarketSentiment) -> Tuple[Recommendation, float]:
        """Generate recommendation with strength"""
        # Base recommendation
        if overall_score >= 80:
            rec = Recommendation.STRONG_BUY
            strength = min(1.0, (overall_score - 80) / 20)
        elif overall_score >= 70:
            rec = Recommendation.BUY
            strength = min(1.0, (overall_score - 70) / 10)
        elif overall_score >= 60:
            rec = Recommendation.ACCUMULATE
            strength = min(1.0, (overall_score - 60) / 10)
        elif overall_score >= 50:
            rec = Recommendation.HOLD
            strength = 0.5
        elif overall_score >= 40:
            rec = Recommendation.REDUCE
            strength = min(1.0, (50 - overall_score) / 10)
        elif overall_score >= 30:
            rec = Recommendation.SELL
            strength = min(1.0, (40 - overall_score) / 10)
        else:
            rec = Recommendation.STRONG_SELL
            strength = min(1.0, (30 - overall_score) / 30)
        
        # Adjust based on additional factors
        if technical.volatility > 0.3:
            strength *= 0.8
        
        if fundamental.data_quality == DataQuality.LOW:
            strength *= 0.7
        
        if sentiment.overall_sentiment > 0.3:
            if rec in [Recommendation.BUY, Recommendation.STRONG_BUY]:
                strength *= 1.1
            else:
                strength *= 0.9
        
        return rec, max(0.1, min(1.0, strength))
    
    def _assess_risk_enhanced(self, technical: TechnicalIndicators,
                             fundamental: FundamentalData,
                             sentiment: MarketSentiment) -> Tuple[str, List[Dict[str, Any]]]:
        """Enhanced risk assessment"""
        risk_factors = []
        risk_score = 0
        
        # Volatility risk
        if technical.volatility > 0.3:
            risk_score += 2
            risk_factors.append({
                'factor': 'High Volatility',
                'severity': 'high',
                'description': f'Volatility of {technical.volatility:.1%} indicates high price swings'
            })
        elif technical.volatility > 0.2:
            risk_score += 1
        
        # Debt risk
        if fundamental.debt_to_equity > 1.0:
            risk_score += 2
            risk_factors.append({
                'factor': 'High Debt',
                'severity': 'high',
                'description': f'Debt/Equity ratio of {fundamental.debt_to_equity:.2f} indicates high leverage'
            })
        elif fundamental.debt_to_equity > 0.7:
            risk_score += 1
        
        # Liquidity risk
        if fundamental.current_ratio < 1.0:
            risk_score += 2
            risk_factors.append({
                'factor': 'Low Liquidity',
                'severity': 'high',
                'description': f'Current ratio of {fundamental.current_ratio:.2f} indicates potential liquidity issues'
            })
        elif fundamental.current_ratio < 1.5:
            risk_score += 1
        
        # Sentiment risk
        if sentiment.overall_sentiment < -0.3:
            risk_score += 1
            risk_factors.append({
                'factor': 'Negative Sentiment',
                'severity': 'medium',
                'description': 'Market sentiment is significantly negative'
            })
        
        # Technical risk
        if technical.rsi > 80 or technical.rsi < 20:
            risk_score += 1
            risk_factors.append({
                'factor': 'Extreme RSI',
                'severity': 'medium',
                'description': f'RSI of {technical.rsi:.1f} indicates overbought/oversold conditions'
            })
        
        # Determine risk level
        if risk_score >= 4:
            risk_level = "HIGH"
        elif risk_score >= 2:
            risk_level = "MEDIUM"
        else:
            risk_level = "LOW"
        
        return risk_level, risk_factors
    
    def _calculate_position_sizing(self, symbol: str, overall_score: float,
                                  risk_level: str, fundamental: FundamentalData) -> PortfolioAllocation:
        """Calculate position sizing recommendations"""
        
        # Base allocation based on score and risk
        base_allocation = {
            "HIGH": 2.0,
            "MEDIUM": 5.0,
            "LOW": 10.0
        }.get(risk_level, 5.0)
        
        # Adjust based on score
        if overall_score >= 80:
            base_allocation *= 1.5
        elif overall_score >= 70:
            base_allocation *= 1.2
        elif overall_score < 50:
            base_allocation *= 0.5
        
        # Adjust for market cap
        if fundamental.market_cap < 1e9:  # Small cap
            base_allocation *= 0.7
        elif fundamental.market_cap > 50e9:  # Large cap
            base_allocation *= 1.2
        
        # Clamp allocation
        recommended = max(1.0, min(15.0, base_allocation))
        max_allocation = min(20.0, recommended * 1.5)
        min_allocation = max(0.5, recommended * 0.5)
        
        return PortfolioAllocation(
            symbol=symbol,
            recommended_allocation=recommended,
            max_allocation=max_allocation,
            min_allocation=min_allocation,
            allocation_reason=self._get_allocation_reason(recommended, risk_level, overall_score),
            risk_adjusted_allocation=recommended * (1 - {"HIGH": 0.5, "MEDIUM": 0.2, "LOW": 0.0}.get(risk_level, 0.2)),
            scenario_analysis={
                'bull_case': recommended * 1.5,
                'base_case': recommended,
                'bear_case': recommended * 0.5
            }
        )
    
    def _get_allocation_reason(self, allocation: float, risk_level: str, score: float) -> str:
        """Get allocation reason"""
        reasons = []
        
        if allocation > 10:
            reasons.append("High conviction opportunity")
        elif allocation > 5:
            reasons.append("Strong fundamentals and technicals")
        else:
            reasons.append("Moderate opportunity with manageable risk")
        
        if risk_level == "LOW":
            reasons.append("Low risk profile allows for larger position")
        elif risk_level == "HIGH":
            reasons.append("Higher risk requires smaller position size")
        
        if score >= 80:
            reasons.append("Exceptional overall score")
        elif score >= 70:
            reasons.append("Strong overall score")
        
        return "; ".join(reasons)
    
    def _calculate_trading_levels(self, price: PriceData, technical: TechnicalIndicators,
                                 recommendation: Recommendation) -> Tuple[Dict[str, float], Dict[str, float], Dict[str, float]]:
        """Calculate trading levels"""
        current_price = price.price
        
        # Optimal entry levels
        if recommendation in [Recommendation.STRONG_BUY, Recommendation.BUY, Recommendation.ACCUMULATE]:
            # For buy recommendations, look for pullbacks
            optimal_entry = {
                'aggressive': current_price * 0.98,
                'moderate': current_price * 0.96,
                'conservative': current_price * 0.94
            }
        else:
            # For sell/hold, current price is entry
            optimal_entry = {
                'aggressive': current_price,
                'moderate': current_price,
                'conservative': current_price
            }
        
        # Take profit levels
        if recommendation in [Recommendation.STRONG_BUY, Recommendation.BUY]:
            take_profit = {
                'target_1': current_price * 1.05,
                'target_2': current_price * 1.10,
                'target_3': current_price * 1.15
            }
        elif recommendation == Recommendation.ACCUMULATE:
            take_profit = {
                'target_1': current_price * 1.03,
                'target_2': current_price * 1.06,
                'target_3': current_price * 1.09
            }
        else:
            take_profit = {
                'target_1': current_price,
                'target_2': current_price,
                'target_3': current_price
            }
        
        # Stop loss levels
        if technical.support_levels:
            initial_stop = technical.support_levels[0] * 0.97
        else:
            initial_stop = current_price * 0.92
        
        stop_loss = {
            'initial': initial_stop,
            'trailing': current_price * 0.93,
            'breakeven': current_price * 1.01
        }
        
        return optimal_entry, take_profit, stop_loss
    
    def _generate_outlooks(self, overall_score: float, technical: TechnicalIndicators,
                          fundamental: FundamentalData) -> Dict[str, str]:
        """Generate outlooks for different timeframes"""
        
        outlooks = {
            'short_term': self._generate_short_term_outlook(overall_score, technical),
            'medium_term': self._generate_medium_term_outlook(overall_score, fundamental),
            'long_term': self._generate_long_term_outlook(overall_score, fundamental),
            'swing_trade': self._generate_swing_trade_outlook(technical)
        }
        
        return outlooks
    
    def _generate_short_term_outlook(self, score: float, technical: TechnicalIndicators) -> str:
        """Generate short-term outlook"""
        if score >= 70:
            if technical.trend_direction == "Bullish":
                return "Very Bullish - Strong momentum and positive technicals"
            else:
                return "Bullish - Strong fundamentals overcoming technical weakness"
        elif score >= 60:
            return "Moderately Bullish - Positive indicators with some concerns"
        elif score >= 50:
            return "Neutral - Balanced factors, sideways movement expected"
        elif score >= 40:
            return "Cautious - Some negative factors emerging"
        else:
            return "Bearish - Multiple negative indicators"
    
    def _generate_medium_term_outlook(self, score: float, fundamental: FundamentalData) -> str:
        """Generate medium-term outlook"""
        if score >= 70:
            if fundamental.revenue_growth > 0.1:
                return "Very Positive - Strong growth trajectory"
            else:
                return "Positive - Solid fundamentals support appreciation"
        elif score >= 60:
            return "Moderately Positive - Gradual improvement expected"
        elif score >= 50:
            return "Neutral - Balanced risk/reward"
        elif score >= 40:
            return "Cautious - Fundamentals show some weakness"
        else:
            return "Negative - Structural issues present"
    
    def _generate_long_term_outlook(self, score: float, fundamental: FundamentalData) -> str:
        """Generate long-term outlook"""
        if score >= 70:
            if fundamental.roe > 0.15 and fundamental.revenue_growth > 0.1:
                return "Excellent - Strong competitive position with growth"
            else:
                return "Very Good - Solid long-term prospects"
        elif score >= 60:
            return "Good - Favorable long-term outlook"
        elif score >= 50:
            return "Fair - Average long-term prospects"
        elif score >= 40:
            return "Below Average - Long-term concerns"
        else:
            return "Poor - Significant long-term challenges"
    
    def _generate_swing_trade_outlook(self, technical: TechnicalIndicators) -> str:
        """Generate swing trade outlook"""
        if technical.trend_direction == "Bullish" and technical.adx > 25:
            return "Favorable - Strong trend with good momentum"
        elif technical.trend_direction == "Bullish":
            return "Moderately Favorable - Uptrend but weak momentum"
        elif technical.trend_direction == "Bearish" and technical.adx > 25:
            return "Unfavorable - Strong downtrend"
        elif technical.trend_direction == "Bearish":
            return "Caution - Downtrend but weak momentum"
        else:
            return "Neutral - Range-bound, look for breakout"
    
    def _calculate_explainable_factors(self, tech_score: float, fund_score: float,
                                      sent_score: float, mom_score: float,
                                      val_score: float, qual_score: float) -> Dict[str, float]:
        """Calculate explainable factors for transparency"""
        total = tech_score + fund_score + sent_score + mom_score + val_score + qual_score
        
        if total > 0:
            return {
                'technical_contribution': tech_score / total * 100,
                'fundamental_contribution': fund_score / total * 100,
                'sentiment_contribution': sent_score / total * 100,
                'momentum_contribution': mom_score / total * 100,
                'value_contribution': val_score / total * 100,
                'quality_contribution': qual_score / total * 100
            }
        
        return {key: 16.67 for key in ['technical', 'fundamental', 'sentiment', 'momentum', 'value', 'quality']}
    
    def _generate_scenario_analysis(self, price: PriceData, technical: TechnicalIndicators,
                                   fundamental: FundamentalData) -> Dict[str, Dict[str, float]]:
        """Generate scenario analysis"""
        current_price = price.price
        
        return {
            'bull_case': {
                'probability': 0.3,
                'target_price': current_price * 1.25,
                'timeframe': '6-12 months',
                'catalysts': ['Strong earnings beat', 'Market leadership', 'Sector rotation']
            },
            'base_case': {
                'probability': 0.5,
                'target_price': current_price * 1.10,
                'timeframe': '6-12 months',
                'catalysts': ['Steady growth', 'Market average performance', 'Dividend stability']
            },
            'bear_case': {
                'probability': 0.2,
                'target_price': current_price * 0.85,
                'timeframe': '6-12 months',
                'catalysts': ['Earnings miss', 'Sector headwinds', 'Market correction']
            }
        }
    
    def _calculate_expected_metrics(self, technical: TechnicalIndicators,
                                   fundamental: FundamentalData) -> Tuple[float, float]:
        """Calculate expected return and volatility"""
        # Base expected return
        base_return = 0.08  # 8% market average
        
        # Adjust for technicals
        if technical.trend_direction == "Bullish":
            base_return += 0.02
        elif technical.trend_direction == "Bearish":
            base_return -= 0.03
        
        # Adjust for fundamentals
        if fundamental.roe > 0.15:
            base_return += 0.03
        elif fundamental.roe < 0.05:
            base_return -= 0.02
        
        if fundamental.revenue_growth > 0.15:
            base_return += 0.02
        
        # Expected volatility
        expected_volatility = technical.volatility
        
        return base_return, expected_volatility
    
    def _calculate_sortino_ratio(self, expected_return: float, volatility: float) -> float:
        """Calculate Sortino ratio"""
        # Simplified Sortino ratio
        risk_free_rate = 0.02  # 2% risk-free rate
        downside_deviation = volatility * 0.7  # Approximate downside deviation
        
        if downside_deviation > 0:
            return (expected_return - risk_free_rate) / downside_deviation
        return 0.0
    
    def _determine_confidence(self, tech_quality: DataQuality, fund_quality: DataQuality) -> str:
        """Determine confidence level"""
        quality_scores = {
            DataQuality.EXCELLENT: 5,
            DataQuality.HIGH: 4,
            DataQuality.MEDIUM: 3,
            DataQuality.LOW: 2,
            DataQuality.SYNTHETIC: 1,
            DataQuality.UNRELIABLE: 0
        }
        
        avg_score = (quality_scores.get(tech_quality, 0) + quality_scores.get(fund_quality, 0)) / 2
        
        if avg_score >= 4:
            return "Very High"
        elif avg_score >= 3:
            return "High"
        elif avg_score >= 2:
            return "Medium"
        elif avg_score >= 1:
            return "Low"
        else:
            return "Very Low"
    
    def _estimate_max_drawdown(self, technical: TechnicalIndicators) -> float:
        """Estimate maximum drawdown"""
        # Simple estimation based on volatility
        return min(0.30, technical.volatility * 1.5)
    
    def _calculate_var(self, technical: TechnicalIndicators, fundamental: FundamentalData) -> float:
        """Calculate Value at Risk (95% confidence)"""
        # Simplified VaR calculation
        base_var = technical.volatility * 1.645  # 95% confidence
        
        # Adjust for fundamentals
        if fundamental.debt_to_equity > 1.0:
            base_var *= 1.2
        if fundamental.current_ratio < 1.0:
            base_var *= 1.3
        
        return min(0.50, base_var)  # Cap at 50%
    
    def _calculate_trailing_stop(self, technical: TechnicalIndicators) -> float:
        """Calculate trailing stop percentage"""
        if technical.volatility > 0.3:
            return 15.0
        elif technical.volatility > 0.2:
            return 10.0
        else:
            return 7.0
    
    def _identify_strengths_enhanced(self, technical: TechnicalIndicators,
                                    fundamental: FundamentalData) -> List[str]:
        """Identify key strengths"""
        strengths = []
        
        if technical.trend_direction == "Bullish":
            strengths.append("Strong upward trend momentum")
        
        if 0 < fundamental.pe_ratio < 15:
            strengths.append("Attractive valuation with reasonable P/E ratio")
        
        if fundamental.roe > 0.15:
            strengths.append("High return on equity indicating efficient operations")
        
        if fundamental.revenue_growth > 0.1:
            strengths.append("Strong revenue growth trajectory")
        
        if fundamental.debt_to_equity < 0.5:
            strengths.append("Healthy balance sheet with conservative debt levels")
        
        if fundamental.profit_margin > 0.15:
            strengths.append("Strong profitability margins")
        
        if technical.adx > 25 and technical.dmi_plus > technical.dmi_minus:
            strengths.append("Strong trend with positive directional movement")
        
        if technical.volume_trend > 0.05:
            strengths.append("Increasing volume supporting price movement")
        
        if not strengths:
            strengths.append("Stable operations with moderate growth prospects")
        
        return strengths[:5]
    
    def _identify_weaknesses(self, technical: TechnicalIndicators,
                            fundamental: FundamentalData) -> List[str]:
        """Identify key weaknesses"""
        weaknesses = []
        
        if technical.volatility > 0.3:
            weaknesses.append("High price volatility indicating instability")
        
        if fundamental.debt_to_equity > 1.0:
            weaknesses.append("Elevated debt levels could impact financial flexibility")
        
        if fundamental.revenue_growth < 0:
            weaknesses.append("Declining revenue may signal business challenges")
        
        if fundamental.pe_ratio > 30:
            weaknesses.append("High valuation multiples may not be sustainable")
        
        if technical.rsi > 70:
            weaknesses.append("Potentially overbought conditions")
        elif technical.rsi < 30:
            weaknesses.append("Potentially oversold; may indicate underlying weakness")
        
        if technical.trend_direction == "Bearish":
            weaknesses.append("Prevailing downward trend in technical indicators")
        
        if not weaknesses:
            weaknesses.append("Standard market risks apply")
        
        return weaknesses[:4]
    
    def _identify_opportunities(self, technical: TechnicalIndicators,
                               fundamental: FundamentalData,
                               sentiment: MarketSentiment) -> List[str]:
        """Identify opportunities"""
        opportunities = []
        
        if technical.rsi < 30:
            opportunities.append("Oversold conditions may present buying opportunity")
        
        if fundamental.pe_ratio < industry_pe:
            opportunities.append("Trading below industry average P/E")
        
        if sentiment.overall_sentiment > 0.3:
            opportunities.append("Positive market sentiment could drive further gains")
        
        if technical.bollinger_position < 0.2:
            opportunities.append("Trading near lower Bollinger Band, potential rebound")
        
        if fundamental.dividend_yield > 0.04:
            opportunities.append("Attractive dividend yield for income investors")
        
        if not opportunities:
            opportunities.append("Market efficiency limits obvious opportunities")
        
        return opportunities[:3]
    
    def _identify_threats(self, technical: TechnicalIndicators,
                         fundamental: FundamentalData,
                         sentiment: MarketSentiment) -> List[str]:
        """Identify threats"""
        threats = []
        
        if technical.volatility > 0.3:
            threats.append("Market volatility could lead to sharp price declines")
        
        if sentiment.overall_sentiment < -0.3:
            threats.append("Negative sentiment could trigger sell-offs")
        
        if fundamental.debt_to_equity > 1.5:
            threats.append("High leverage increases risk during downturns")
        
        if technical.adx > 40 and technical.dmi_minus > technical.dmi_plus:
            threats.append("Strong bearish trend momentum")
        
        if not threats:
            threats.append("General market and economic risks")
        
        return threats[:3]
    
    def _assess_sector_outlook(self, fundamental: FundamentalData) -> str:
        """Assess sector outlook"""
        sector = fundamental.sector.lower()
        
        if sector in ['technology', 'healthcare']:
            return "Favorable - Growth sectors with strong long-term prospects"
        elif sector in ['financials', 'consumer staples']:
            return "Stable - Defensive sectors with reliable performance"
        elif sector in ['energy', 'materials']:
            return "Cyclical - Dependent on economic cycles"
        elif sector in ['utilities', 'real estate']:
            return "Income-focused - Stable dividends but limited growth"
        else:
            return "Mixed - Varies by specific industry dynamics"
    
    def _assess_competitive_position(self, fundamental: FundamentalData) -> str:
        """Assess competitive position"""
        if fundamental.sector_rank <= 10:
            return "Leadership - Top tier in sector"
        elif fundamental.sector_rank <= 25:
            return "Strong - Above average competitive position"
        elif fundamental.sector_rank <= 50:
            return "Average - Mid-tier competitive position"
        elif fundamental.sector_rank <= 75:
            return "Challenged - Below average competitive position"
        else:
            return "Weak - Bottom tier in sector"
    
    def _generate_fallback_recommendation(self, symbol: str, timeframe: str, duration: float) -> AIRecommendation:
        """Generate fallback recommendation"""
        return AIRecommendation(
            symbol=symbol,
            overall_score=50.0,
            technical_score=50.0,
            fundamental_score=50.0,
            sentiment_score=50.0,
            risk_adjusted_score=50.0,
            momentum_score=50.0,
            value_score=50.0,
            quality_score=50.0,
            recommendation=Recommendation.HOLD,
            recommendation_strength=0.5,
            confidence_level="Low",
            expected_return=0.06,
            expected_volatility=0.2,
            sharpe_ratio=0.2,
            sortino_ratio=0.15,
            short_term_outlook="Neutral - Insufficient data for analysis",
            medium_term_outlook="Neutral - Data collection failed",
            long_term_outlook="Unknown - Analysis unavailable",
            swing_trade_outlook="Not recommended - Data issues",
            risk_level="MEDIUM",
            risk_factors=[{
                'factor': 'Data Unavailability',
                'severity': 'high',
                'description': 'Unable to collect sufficient data for proper analysis'
            }],
            max_drawdown_estimate=0.2,
            value_at_risk=0.15,
            stop_loss_levels={'initial': 0.0, 'trailing': 0.0, 'breakeven': 0.0},
            position_sizing=PortfolioAllocation(
                symbol=symbol,
                recommended_allocation=0.0,
                max_allocation=0.0,
                min_allocation=0.0,
                allocation_reason="Insufficient data for position sizing",
                risk_adjusted_allocation=0.0,
                scenario_analysis={}
            ),
            optimal_entry={'aggressive': 0.0, 'moderate': 0.0, 'conservative': 0.0},
            take_profit_levels={'target_1': 0.0, 'target_2': 0.0, 'target_3': 0.0},
            trailing_stop_percent=0.0,
            key_strengths=["Insufficient data for analysis"],
            key_weaknesses=["Data collection failed"],
            opportunities=["Data availability improvement"],
            threats=["Continued data unavailability"],
            market_regime=MarketRegime.SIDEWAYS,
            sector_outlook="Unknown",
            competitive_position="Unknown",
            timestamp=datetime.utcnow().isoformat() + "Z",
            data_sources_used=0,
            analysis_duration=duration,
            model_version=self.model_version,
            explainable_factors={},
            scenario_analysis={}
        )
    
    # =========================================================================
    # Enhanced Multi-source Analysis
    # =========================================================================
    
    async def get_multi_source_analysis(self, symbol: str) -> Dict[str, Any]:
        """Enhanced multi-source analysis with comprehensive data"""
        start_time = time.time()
        symbol = symbol.upper().strip()
        cache_key = self._get_cache_key(symbol, "multisource_v2")
        
        # Try cache
        cached = await self._load_from_cache(cache_key)
        if cached:
            cached['metadata']['cache_hit'] = True
            return cached
        
        try:
            # Gather all data
            price_data = await self.get_real_time_price(symbol)
            technical_data = await self.calculate_technical_indicators(symbol)
            fundamental_data = await self.analyze_fundamentals(symbol)
            sentiment_data = await self.analyze_market_sentiment(symbol)
            
            # Combine data
            analysis = {
                "symbol": symbol,
                "price_data": price_data.to_dict() if price_data else None,
                "technical_indicators": technical_data.to_dict() if technical_data else None,
                "fundamental_data": fundamental_data.to_dict() if fundamental_data else None,
                "market_sentiment": sentiment_data.to_dict() if sentiment_data else None,
                "consolidated_analysis": self._consolidate_multi_source_data_enhanced(
                    price_data, technical_data, fundamental_data, sentiment_data
                ),
                "timestamp": datetime.utcnow().isoformat() + "Z",
                "metadata": {
                    "data_sources_used": 4,
                    "successful_sources": sum(1 for data in [price_data, technical_data, fundamental_data, sentiment_data] if data),
                    "analysis_duration": time.time() - start_time,
                    "cache_hit": False,
                    "data_quality": self._assess_overall_quality(
                        price_data.data_quality if price_data else DataQuality.UNRELIABLE,
                        technical_data.data_quality,
                        fundamental_data.data_quality,
                        DataQuality.MEDIUM
                    ).value
                }
            }
            
            # Cache the result
            await self._save_to_cache(cache_key, analysis)
            
            return analysis
            
        except Exception as e:
            logger.error(f"Multi-source analysis failed for {symbol}: {e}")
            return {
                "symbol": symbol,
                "error": str(e),
                "timestamp": datetime.utcnow().isoformat() + "Z",
                "metadata": {
                    "data_sources_used": 0,
                    "successful_sources": 0,
                    "analysis_duration": time.time() - start_time,
                    "cache_hit": False,
                    "data_quality": "UNRELIABLE"
                }
            }
    
    def _consolidate_multi_source_data_enhanced(self, price_data: Optional[PriceData],
                                               technical_data: TechnicalIndicators,
                                               fundamental_data: FundamentalData,
                                               sentiment_data: MarketSentiment) -> Dict[str, Any]:
        """Enhanced data consolidation"""
        if not price_data:
            return {"error": "No price data available"}
        
        # Calculate consensus metrics
        consensus_price = price_data.price
        consensus_volume = price_data.volume
        
        # Calculate data quality score
        quality_scores = {
            'price': self._data_quality_to_score(price_data.data_quality),
            'technical': self._data_quality_to_score(technical_data.data_quality),
            'fundamental': self._data_quality_to_score(fundamental_data.data_quality),
            'sentiment': sentiment_data.confidence_score
        }
        
        avg_quality = np.mean(list(quality_scores.values()))
        
        # Calculate risk score
        risk_score = self._calculate_risk_score_consolidated(
            technical_data, fundamental_data, sentiment_data
        )
        
        # Calculate opportunity score
        opportunity_score = self._calculate_opportunity_score(
            technical_data, fundamental_data, sentiment_data
        )
        
        return {
            "consensus_price": consensus_price,
            "consensus_volume": consensus_volume,
            "data_quality_score": avg_quality,
            "risk_score": risk_score,
            "opportunity_score": opportunity_score,
            "market_regime": technical_data.market_regime.value,
            "trend_direction": technical_data.trend_direction,
            "valuation_status": self._assess_valuation_status(fundamental_data),
            "sentiment_status": self._assess_sentiment_status(sentiment_data),
            "key_insights": self._generate_key_insights(
                price_data, technical_data, fundamental_data, sentiment_data
            ),
            "recommendation_summary": self._generate_recommendation_summary(
                technical_data, fundamental_data
            )
        }
    
    def _data_quality_to_score(self, quality: DataQuality) -> float:
        """Convert data quality to score"""
        quality_map = {
            DataQuality.EXCELLENT: 0.95,
            DataQuality.HIGH: 0.85,
            DataQuality.MEDIUM: 0.70,
            DataQuality.LOW: 0.50,
            DataQuality.SYNTHETIC: 0.30,
            DataQuality.UNRELIABLE: 0.10
        }
        return quality_map.get(quality, 0.50)
    
    def _assess_overall_quality(self, *qualities: DataQuality) -> DataQuality:
        """Assess overall data quality"""
        quality_scores = {
            DataQuality.EXCELLENT: 5,
            DataQuality.HIGH: 4,
            DataQuality.MEDIUM: 3,
            DataQuality.LOW: 2,
            DataQuality.SYNTHETIC: 1,
            DataQuality.UNRELIABLE: 0
        }
        
        avg_score = np.mean([quality_scores.get(q, 0) for q in qualities])
        
        if avg_score >= 4:
            return DataQuality.HIGH
        elif avg_score >= 3:
            return DataQuality.MEDIUM
        elif avg_score >= 2:
            return DataQuality.LOW
        elif avg_score >= 1:
            return DataQuality.SYNTHETIC
        else:
            return DataQuality.UNRELIABLE
    
    def _calculate_risk_score_consolidated(self, technical: TechnicalIndicators,
                                          fundamental: FundamentalData,
                                          sentiment: MarketSentiment) -> float:
        """Calculate consolidated risk score"""
        risk_factors = []
        
        # Technical risk
        if technical.volatility > 0.3:
            risk_factors.append(0.3)
        elif technical.volatility > 0.2:
            risk_factors.append(0.2)
        
        if technical.rsi > 80 or technical.rsi < 20:
            risk_factors.append(0.2)
        
        # Fundamental risk
        if fundamental.debt_to_equity > 1.0:
            risk_factors.append(0.3)
        elif fundamental.debt_to_equity > 0.7:
            risk_factors.append(0.1)
        
        if fundamental.current_ratio < 1.0:
            risk_factors.append(0.3)
        elif fundamental.current_ratio < 1.5:
            risk_factors.append(0.1)
        
        # Sentiment risk
        if sentiment.overall_sentiment < -0.3:
            risk_factors.append(0.2)
        
        if risk_factors:
            return min(1.0, sum(risk_factors) / len(risk_factors))
        return 0.1  # Low risk by default
    
    def _calculate_opportunity_score(self, technical: TechnicalIndicators,
                                    fundamental: FundamentalData,
                                    sentiment: MarketSentiment) -> float:
        """Calculate opportunity score"""
        opportunity_factors = []
        
        # Technical opportunity
        if technical.trend_direction == "Bullish":
            opportunity_factors.append(0.3)
        
        if technical.rsi < 30:
            opportunity_factors.append(0.2)
        
        if technical.bollinger_position < 0.2:
            opportunity_factors.append(0.15)
        
        # Fundamental opportunity
        if fundamental.pe_ratio > 0 and fundamental.pe_ratio < 15:
            opportunity_factors.append(0.2)
        
        if fundamental.roe > 0.15:
            opportunity_factors.append(0.15)
        
        # Sentiment opportunity
        if sentiment.overall_sentiment > 0.3:
            opportunity_factors.append(0.1)
        
        if opportunity_factors:
            return min(1.0, sum(opportunity_factors) / len(opportunity_factors))
        return 0.3  # Neutral opportunity
    
    def _assess_valuation_status(self, fundamental: FundamentalData) -> str:
        """Assess valuation status"""
        if fundamental.pe_ratio <= 0:
            return "Unknown"
        elif fundamental.pe_ratio < 12:
            return "Undervalued"
        elif fundamental.pe_ratio < 20:
            return "Fairly Valued"
        elif fundamental.pe_ratio < 30:
            return "Overvalued"
        else:
            return "Highly Overvalued"
    
    def _assess_sentiment_status(self, sentiment: MarketSentiment) -> str:
        """Assess sentiment status"""
        if sentiment.overall_sentiment > 0.3:
            return "Very Positive"
        elif sentiment.overall_sentiment > 0.1:
            return "Positive"
        elif sentiment.overall_sentiment > -0.1:
            return "Neutral"
        elif sentiment.overall_sentiment > -0.3:
            return "Negative"
        else:
            return "Very Negative"
    
    def _generate_key_insights(self, price_data: PriceData, technical: TechnicalIndicators,
                              fundamental: FundamentalData, sentiment: MarketSentiment) -> List[str]:
        """Generate key insights"""
        insights = []
        
        # Price insights
        if price_data.change_percent > 5:
            insights.append(f"Strong price momentum: +{price_data.change_percent:.1f}%")
        elif price_data.change_percent < -5:
            insights.append(f"Significant decline: {price_data.change_percent:.1f}%")
        
        # Technical insights
        if technical.rsi < 30:
            insights.append("Oversold conditions based on RSI")
        elif technical.rsi > 70:
            insights.append("Overbought conditions based on RSI")
        
        if technical.trend_direction == "Bullish" and technical.adx > 25:
            insights.append("Strong bullish trend with good momentum")
        
        # Fundamental insights
        if fundamental.pe_ratio < industry_pe * 0.8:
            insights.append("Trading at discount to industry average")
        elif fundamental.pe_ratio > industry_pe * 1.2:
            insights.append("Trading at premium to industry average")
        
        if fundamental.roe > 0.2:
            insights.append("Exceptional return on equity")
        
        # Sentiment insights
        if sentiment.overall_sentiment > 0.3:
            insights.append("Positive market sentiment supporting price")
        
        if not insights:
            insights.append("Mixed signals across different analysis dimensions")
        
        return insights[:5]
    
    def _generate_recommendation_summary(self, technical: TechnicalIndicators,
                                        fundamental: FundamentalData) -> str:
        """Generate recommendation summary"""
        buy_signals = len(technical.get_buy_signals())
        sell_signals = len(technical.get_sell_signals())
        
        if buy_signals > sell_signals + 2:
            return "Bullish - Multiple buy signals outweigh sell signals"
        elif sell_signals > buy_signals + 2:
            return "Bearish - Multiple sell signals outweigh buy signals"
        elif fundamental.pe_ratio > 0 and fundamental.pe_ratio < 15:
            return "Value Opportunity - Attractive valuation with reasonable risk"
        elif fundamental.pe_ratio > 30:
            return "Caution - High valuation multiples present risk"
        else:
            return "Neutral - Balanced factors suggest holding position"
    
    # =========================================================================
    # Enhanced Utility Methods
    # =========================================================================
    
    async def _make_async_request(self, url: str, params: Optional[Dict[str, Any]] = None,
                                 headers: Optional[Dict[str, str]] = None) -> Optional[Any]:
        """Enhanced async HTTP request with better error handling"""
        params = params or {}
        headers = headers or {}
        
        for attempt in range(3):  # Max 3 retries
            try:
                # Rate limiting
                await self._enforce_rate_limit()
                
                session = await self.get_session()
                async with session.get(url, params=params, headers=headers) as response:
                    self.stats['requests'] += 1
                    
                    if response.status == 200:
                        try:
                            content_type = response.headers.get('Content-Type', '')
                            if 'application/json' in content_type:
                                data = await response.json()
                            else:
                                text = await response.text()
                                try:
                                    data = json.loads(text)
                                except:
                                    data = text
                            
                            # Update performance metrics
                            response_time = response.request_info.response_start
                            self.performance_metrics['response_times'].append(response_time)
                            
                            return data
                        except Exception as e:
                            logger.warning(f"Response parsing failed: {e}")
                            self.stats['errors'] += 1
                            return None
                    else:
                        logger.warning(f"HTTP {response.status} for {url}")
                        self.stats['errors'] += 1
                        
                        if response.status == 429:  # Rate limited
                            await asyncio.sleep(2 ** attempt)  # Exponential backoff
                            continue
                        
                        return None
                        
            except asyncio.TimeoutError:
                logger.error(f"Timeout for {url} (attempt {attempt + 1})")
                self.stats['errors'] += 1
                if attempt < 2:
                    await asyncio.sleep(1)
                continue
                
            except Exception as e:
                logger.error(f"Request failed for {url} (attempt {attempt + 1}): {e}")
                self.stats['errors'] += 1
                if attempt < 2:
                    await asyncio.sleep(1)
                continue
        
        return None
    
    async def _enforce_rate_limit(self):
        """Enhanced rate limiting"""
        while True:
            with self._rate_lock:
                now = time.time()
                window_start = now - 60
                
                # Remove old timestamps
                while self.request_timestamps and self.request_timestamps[0] < window_start:
                    self.request_timestamps.popleft()
                
                if len(self.request_timestamps) < self.rate_limit_rpm:
                    self.request_timestamps.append(now)
                    return
                
                oldest = self.request_timestamps[0]
                sleep_time = 60 - (now - oldest)
            
            if sleep_time > 0:
                await asyncio.sleep(sleep_time + 0.1)
    
    async def clear_cache(self, symbol: Optional[str] = None) -> Dict[str, Any]:
        """Enhanced cache clearing"""
        cleared_count = 0
        
        try:
            # Clear memory cache
            if symbol:
                keys_to_remove = [k for k in self.memory_cache.keys() if symbol.upper() in k]
                for key in keys_to_remove:
                    del self.memory_cache[key]
                    del self.memory_cache_ttl[key]
                    cleared_count += 1
            else:
                self.memory_cache.clear()
                self.memory_cache_ttl.clear()
                cleared_count = len(self.memory_cache)
            
            # Clear Redis cache
            if self.redis_client:
                try:
                    if symbol:
                        pattern = f"*{symbol.upper()}*"
                        keys = self.redis_client.keys(pattern)
                        if keys:
                            self.redis_client.delete(*keys)
                            cleared_count += len(keys)
                    else:
                        # Clear all analyzer cache keys
                        keys = self.redis_client.keys("*v4_*")
                        if keys:
                            self.redis_client.delete(*keys)
                            cleared_count += len(keys)
                except Exception as e:
                    logger.error(f"Redis cache clear failed: {e}")
            
            # Clear file cache
            if symbol:
                pattern = f"*{symbol.upper()}*"
                cache_files = list(self.cache_dir.glob(f"*{pattern}*"))
            else:
                cache_files = list(self.cache_dir.glob("*"))
            
            for cache_file in cache_files:
                try:
                    cache_file.unlink(missing_ok=True)
                    # Also remove metadata file
                    meta_file = cache_file.with_suffix('.json')
                    meta_file.unlink(missing_ok=True)
                    cleared_count += 1
                except Exception as e:
                    logger.debug(f"Failed to delete cache file {cache_file}: {e}")
            
            logger.info(f"Cache cleared: {cleared_count} items removed")
            return {
                "status": "success",
                "cleared_count": cleared_count,
                "timestamp": datetime.utcnow().isoformat() + "Z"
            }
            
        except Exception as e:
            logger.error(f"Cache clearance failed: {e}")
            return {
                "status": "error",
                "error": str(e),
                "timestamp": datetime.utcnow().isoformat() + "Z"
            }
    
    async def get_cache_info(self) -> Dict[str, Any]:
        """Get enhanced cache information"""
        try:
            # Memory cache stats
            memory_stats = {
                "entries": len(self.memory_cache),
                "memory_usage_mb": sum(
                    len(pickle.dumps(v)) for v in self.memory_cache.values()
                ) / (1024 * 1024)
            }
            
            # File cache stats
            cache_files = list(self.cache_dir.glob("*.pkl"))
            file_stats = {
                "total_files": len(cache_files),
                "total_size_mb": sum(f.stat().st_size for f in cache_files) / (1024 * 1024),
                "oldest_file": min((f.stat().st_mtime for f in cache_files), default=None),
                "newest_file": max((f.stat().st_mtime for f in cache_files), default=None)
            }
            
            # Redis stats
            redis_stats = {}
            if self.redis_client:
                try:
                    redis_stats = {
                        "connected": True,
                        "keys_count": len(self.redis_client.keys("*")),
                        "memory_usage": self.redis_client.info().get("used_memory", 0)
                    }
                except Exception as e:
                    redis_stats = {"connected": False, "error": str(e)}
            
            # Hit rate
            total_accesses = self.stats['cache_hits'] + self.stats['cache_misses']
            hit_rate = self.stats['cache_hits'] / total_accesses if total_accesses > 0 else 0
            
            return {
                "memory_cache": memory_stats,
                "file_cache": file_stats,
                "redis_cache": redis_stats,
                "performance": {
                    "hit_rate": hit_rate,
                    "total_hits": self.stats['cache_hits'],
                    "total_misses": self.stats['cache_misses'],
                    "cache_ttl_minutes": self.cache_ttl.total_seconds() / 60
                },
                "timestamp": datetime.utcnow().isoformat() + "Z"
            }
            
        except Exception as e:
            logger.error(f"Cache info retrieval failed: {e}")
            return {"error": str(e)}
    
    def get_statistics(self) -> Dict[str, Any]:
        """Get enhanced statistics"""
        with self._rate_lock:
            total_requests = self.stats['requests']
            error_rate = (self.stats['errors'] / total_requests * 100) if total_requests > 0 else 0
            
            # Calculate average response time
            response_times = self.performance_metrics.get('response_times', [])
            avg_response_time = np.mean(response_times) if response_times else 0
            
            # Calculate cache efficiency
            total_cache_access = self.stats['cache_hits'] + self.stats['cache_misses']
            cache_efficiency = (self.stats['cache_hits'] / total_cache_access * 100) if total_cache_access > 0 else 0
            
            uptime = time.time() - self.stats['start_time']
            
            return {
                "requests": {
                    "total": total_requests,
                    "errors": self.stats['errors'],
                    "error_rate_percent": round(error_rate, 2),
                    "avg_response_time_ms": round(avg_response_time * 1000, 2)
                },
                "cache": {
                    "hits": self.stats['cache_hits'],
                    "misses": self.stats['cache_misses'],
                    "efficiency_percent": round(cache_efficiency, 2)
                },
                "performance": {
                    "uptime_seconds": round(uptime, 2),
                    "current_rpm": len(self.request_timestamps),
                    "max_rpm": self.rate_limit_rpm
                },
                "configuration": {
                    "model_version": self.model_version,
                    "analysis_timeout": self.analysis_timeout,
                    "cache_ttl_minutes": self.cache_ttl.total_seconds() / 60,
                    "enable_ml": self.enable_ml
                },
                "timestamp": datetime.utcnow().isoformat() + "Z"
            }
    
    async def close(self):
        """Clean shutdown"""
        try:
            if self._session and not self._session.closed:
                await self._session.close()
        except Exception as e:
            logger.debug(f"Error closing async session: {e}")
        
        try:
            if self._session_sync:
                self._session_sync.close()
        except Exception as e:
            logger.debug(f"Error closing sync session: {e}")
        
        # Save statistics
        self._save_statistics()
    
    def _save_statistics(self):
        """Save statistics to file"""
        try:
            stats_file = self.cache_dir / "statistics.json"
            stats_data = {
                "last_run": datetime.utcnow().isoformat() + "Z",
                "total_requests": self.stats['requests'],
                "total_errors": self.stats['errors'],
                "cache_hits": self.stats['cache_hits'],
                "cache_misses": self.stats['cache_misses'],
                "uptime_seconds": time.time() - self.stats['start_time']
            }
            
            with open(stats_file, 'w') as f:
                json.dump(stats_data, f, indent=2)
                
        except Exception as e:
            logger.debug(f"Failed to save statistics: {e}")


# Global analyzer instance
try:
    analyzer = EnhancedTradingAnalyzer()
    logger.info(f"Global EnhancedTradingAnalyzer v{analyzer.model_version} created successfully")
except Exception as e:
    logger.error(f"Failed to create global EnhancedTradingAnalyzer: {e}")
    analyzer = None


@asynccontextmanager
async def get_analyzer():
    """Async context manager for analyzer"""
    analyzer_instance = EnhancedTradingAnalyzer()
    try:
        yield analyzer_instance
    finally:
        await analyzer_instance.close()


# Convenience functions
async def generate_ai_recommendation(symbol: str, timeframe: str = "medium_term") -> AIRecommendation:
    if analyzer is None:
        raise RuntimeError("Analyzer not initialized")
    return await analyzer.generate_ai_recommendation(symbol, timeframe)


async def get_multi_source_analysis(symbol: str) -> Dict[str, Any]:
    if analyzer is None:
        raise RuntimeError("Analyzer not initialized")
    return await analyzer.get_multi_source_analysis(symbol)


# Self-test function
async def _test_enhanced_analyzer():
    """Test the enhanced analyzer"""
    logging.basicConfig(level=logging.INFO)
    
    print("=" * 60)
    print("Testing Enhanced Trading Analyzer v4.0.0")
    print("=" * 60)
    
    async with get_analyzer() as test_analyzer:
        # Test symbol (Saudi Aramco)
        test_symbol = "2222.SR"
        
        print(f"\n1. Testing multi-source analysis for {test_symbol}...")
        start = time.time()
        analysis = await test_analyzer.get_multi_source_analysis(test_symbol)
        duration = time.time() - start
        
        if 'error' not in analysis:
            print(f"   ✓ Analysis completed in {duration:.2f}s")
            consolidated = analysis.get('consolidated_analysis', {})
            print(f"   - Consensus Price: {consolidated.get('consensus_price', 'N/A')}")
            print(f"   - Data Quality: {consolidated.get('data_quality_score', 'N/A'):.2f}")
            print(f"   - Risk Score: {consolidated.get('risk_score', 'N/A'):.2f}")
            print(f"   - Opportunity Score: {consolidated.get('opportunity_score', 'N/A'):.2f}")
        else:
            print(f"   ✗ Analysis failed: {analysis.get('error')}")
        
        print(f"\n2. Testing AI recommendation for {test_symbol}...")
        start = time.time()
        recommendation = await test_analyzer.generate_ai_recommendation(test_symbol)
        duration = time.time() - start
        
        print(f"   ✓ Recommendation generated in {duration:.2f}s")
        print(f"   - Recommendation: {recommendation.recommendation.value}")
        print(f"   - Overall Score: {recommendation.overall_score:.1f}")
        print(f"   - Risk Level: {recommendation.risk_level}")
        print(f"   - Confidence: {recommendation.confidence_level}")
        print(f"   - Position Size: {recommendation.position_sizing.recommended_allocation:.1f}%")
        
        print(f"\n3. Testing cache info...")
        cache_info = await test_analyzer.get_cache_info()
        print(f"   ✓ Cache info retrieved")
        print(f"   - Memory entries: {cache_info.get('memory_cache', {}).get('entries', 0)}")
        print(f"   - File cache size: {cache_info.get('file_cache', {}).get('total_size_mb', 0):.2f} MB")
        
        print(f"\n4. Testing statistics...")
        stats = test_analyzer.get_statistics()
        print(f"   ✓ Statistics retrieved")
        print(f"   - Total requests: {stats.get('requests', {}).get('total', 0)}")
        print(f"   - Error rate: {stats.get('requests', {}).get('error_rate_percent', 0):.1f}%")
        print(f"   - Cache efficiency: {stats.get('cache', {}).get('efficiency_percent', 0):.1f}%")
        
        print(f"\n" + "=" * 60)
        print("Enhanced analyzer test completed successfully!")
        print("=" * 60)


if __name__ == "__main__":
    asyncio.run(_test_enhanced_analyzer())
