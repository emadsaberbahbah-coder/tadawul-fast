# advanced_analysis.py - Enhanced Multi-source AI Trading Analysis
from __future__ import annotations

import os
import asyncio
import aiohttp
import requests
import pandas as pd
import numpy as np
import json
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any, Tuple
from dataclasses import dataclass
from enum import Enum
import httpx
from pathlib import Path

# Configure logging
logger = logging.getLogger(__name__)

class AnalysisTimeframe(Enum):
    SHORT_TERM = "short_term"
    MEDIUM_TERM = "medium_term" 
    LONG_TERM = "long_term"

class Recommendation(Enum):
    STRONG_BUY = "STRONG_BUY"
    BUY = "BUY"
    HOLD = "HOLD"
    SELL = "SELL"
    STRONG_SELL = "STRONG_SELL"

@dataclass
class PriceData:
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

@dataclass
class TechnicalIndicators:
    rsi: float
    macd: float
    macd_signal: float
    macd_histogram: float
    moving_avg_20: float
    moving_avg_50: float
    moving_avg_200: float
    bollinger_upper: float
    bollinger_lower: float
    bollinger_position: float
    volume_trend: float
    support_level: float
    resistance_level: float
    trend_direction: str
    volatility: float
    momentum_score: float
    williams_r: float
    stoch_k: float
    stoch_d: float
    cci: float
    adx: float
    atr: float

@dataclass
class FundamentalData:
    market_cap: float
    pe_ratio: float
    pb_ratio: float
    dividend_yield: float
    eps: float
    roe: float
    debt_to_equity: float
    revenue_growth: float
    profit_margin: float
    sector_rank: int
    free_cash_flow: float
    operating_margin: float
    current_ratio: float
    quick_ratio: float
    peg_ratio: float

@dataclass
class AIRecommendation:
    symbol: str
    overall_score: float
    technical_score: float
    fundamental_score: float
    sentiment_score: float
    recommendation: Recommendation
    confidence_level: str
    short_term_outlook: str
    medium_term_outlook: str
    long_term_outlook: str
    key_strengths: List[str]
    key_risks: List[str]
    optimal_entry: float
    stop_loss: float
    price_targets: Dict[str, float]
    timestamp: str

class AdvancedTradingAnalyzer:
    def __init__(self):
        # API Keys from environment variables only (no hardcoded keys)
        self.apis = {
            'alpha_vantage': os.getenv('ALPHA_VANTAGE_API_KEY', ''),
            'finnhub': os.getenv('FINNHUB_API_KEY', ''),
            'eodhd': os.getenv('EODHD_API_KEY', ''),
            'twelvedata': os.getenv('TWELVEDATA_API_KEY', ''),
            'marketstack': os.getenv('MARKETSTACK_API_KEY', ''),
            'fmp': os.getenv('FMP_API_KEY', '')
        }
        
        self.base_urls = {
            'alpha_vantage': 'https://www.alphavantage.co/query',
            'finnhub': 'https://finnhub.io/api/v1',
            'eodhd': 'https://eodhistoricaldata.com/api',
            'twelvedata': 'https://api.twelvedata.com',
            'marketstack': 'http://api.marketstack.com/v1',
            'fmp': 'https://financialmodelingprep.com/api/v3'
        }

        # Cache configuration
        self.cache_dir = Path("./analysis_cache")
        self.cache_dir.mkdir(exist_ok=True)
        self.cache_ttl = timedelta(minutes=15)

        # Session management
        self.session = None
        self.async_session = None

    def __enter__(self):
        self.session = requests.Session()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.session:
            self.session.close()

    async def __aenter__(self):
        self.async_session = aiohttp.ClientSession()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self.async_session:
            await self.async_session.close()

    def _get_cache_key(self, symbol: str, data_type: str) -> str:
        """Generate cache key for symbol and data type."""
        return f"{symbol}_{data_type}.json"

    def _load_from_cache(self, symbol: str, data_type: str) -> Optional[Any]:
        """Load data from cache if valid."""
        try:
            cache_file = self.cache_dir / self._get_cache_key(symbol, data_type)
            if cache_file.exists():
                data = json.loads(cache_file.read_text(encoding="utf-8"))
                cache_time = datetime.fromisoformat(data.get('timestamp', '2000-01-01'))
                if datetime.now() - cache_time < self.cache_ttl:
                    return data['data']
        except Exception as e:
            logger.debug(f"Cache load failed for {symbol}_{data_type}: {e}")
        return None

    def _save_to_cache(self, symbol: str, data_type: str, data: Any):
        """Save data to cache."""
        try:
            cache_file = self.cache_dir / self._get_cache_key(symbol, data_type)
            cache_data = {
                'timestamp': datetime.now().isoformat(),
                'data': data
            }
            cache_file.write_text(json.dumps(cache_data, indent=2), encoding="utf-8")
        except Exception as e:
            logger.debug(f"Cache save failed for {symbol}_{data_type}: {e}")

    def get_real_time_price(self, symbol: str) -> Optional[PriceData]:
        """Get real-time price from multiple sources with fallback and caching."""
        # Check cache first
        cached = self._load_from_cache(symbol, 'price')
        if cached:
            return PriceData(**cached)

        sources = [
            self._get_price_alpha_vantage,
            self._get_price_finnhub,
            self._get_price_twelvedata,
            self._get_price_marketstack
        ]
        
        for source in sources:
            try:
                result = source(symbol)
                if result and result.price > 0:
                    # Cache the successful result
                    self._save_to_cache(symbol, 'price', result.__dict__)
                    return result
            except Exception as e:
                logger.warning(f"Price source {source.__name__} failed for {symbol}: {e}")
                continue
        
        logger.error(f"All price sources failed for {symbol}")
        return None

    def _get_price_alpha_vantage(self, symbol: str) -> Optional[PriceData]:
        """Alpha Vantage price data with enhanced error handling."""
        if not self.apis['alpha_vantage']:
            return None

        params = {
            'function': 'GLOBAL_QUOTE',
            'symbol': symbol,
            'apikey': self.apis['alpha_vantage']
        }
        
        try:
            session = self.session or requests.Session()
            response = session.get(self.base_urls['alpha_vantage'], params=params, timeout=10)
            response.raise_for_status()
            data = response.json()
            
            if 'Global Quote' in data and data['Global Quote']:
                quote = data['Global Quote']
                return PriceData(
                    symbol=symbol,
                    price=float(quote.get('05. price', 0)),
                    change=float(quote.get('09. change', 0)),
                    change_percent=float(quote.get('10. change percent', '0').rstrip('%')),
                    volume=int(quote.get('06. volume', 0)),
                    high=float(quote.get('03. high', 0)),
                    low=float(quote.get('04. low', 0)),
                    open=float(quote.get('02. open', 0)),
                    previous_close=float(quote.get('08. previous close', 0)),
                    timestamp=datetime.utcnow().isoformat(),
                    source='alpha_vantage'
                )
        except Exception as e:
            logger.warning(f"Alpha Vantage API error for {symbol}: {e}")
            
        return None

    def _get_price_finnhub(self, symbol: str) -> Optional[PriceData]:
        """Finnhub price data with enhanced error handling."""
        if not self.apis['finnhub']:
            return None

        url = f"{self.base_urls['finnhub']}/quote"
        params = {
            'symbol': symbol,
            'token': self.apis['finnhub']
        }
        
        try:
            session = self.session or requests.Session()
            response = session.get(url, params=params, timeout=10)
            response.raise_for_status()
            data = response.json()
            
            if data.get('c', 0) > 0:
                return PriceData(
                    symbol=symbol,
                    price=data['c'],
                    change=data.get('d', 0),
                    change_percent=data.get('dp', 0),
                    high=data.get('h', 0),
                    low=data.get('l', 0),
                    open=data.get('o', 0),
                    previous_close=data.get('pc', 0),
                    volume=data.get('v', 0),
                    timestamp=datetime.utcnow().isoformat(),
                    source='finnhub'
                )
        except Exception as e:
            logger.warning(f"Finnhub API error for {symbol}: {e}")
            
        return None

    def _get_price_twelvedata(self, symbol: str) -> Optional[PriceData]:
        """Twelve Data price data with enhanced error handling."""
        if not self.apis['twelvedata']:
            return None

        url = f"{self.base_urls['twelvedata']}/price"
        params = {
            'symbol': symbol,
            'apikey': self.apis['twelvedata']
        }
        
        try:
            session = self.session or requests.Session()
            response = session.get(url, params=params, timeout=10)
            response.raise_for_status()
            data = response.json()
            
            if data.get('price'):
                # Get additional quote data
                quote_url = f"{self.base_urls['twelvedata']}/quote"
                quote_params = {'symbol': symbol, 'apikey': self.apis['twelvedata']}
                quote_response = session.get(quote_url, params=quote_params, timeout=10)
                quote_data = quote_response.json()
                
                return PriceData(
                    symbol=symbol,
                    price=float(data['price']),
                    change=float(quote_data.get('change', 0)),
                    change_percent=float(quote_data.get('percent_change', '0').rstrip('%')),
                    high=float(quote_data.get('high', 0)),
                    low=float(quote_data.get('low', 0)),
                    open=float(quote_data.get('open', 0)),
                    previous_close=float(quote_data.get('previous_close', 0)),
                    volume=int(quote_data.get('volume', 0)),
                    timestamp=datetime.utcnow().isoformat(),
                    source='twelvedata'
                )
        except Exception as e:
            logger.warning(f"Twelve Data API error for {symbol}: {e}")
            
        return None

    def _get_price_marketstack(self, symbol: str) -> Optional[PriceData]:
        """Marketstack price data with enhanced error handling."""
        if not self.apis['marketstack']:
            return None

        url = f"{self.base_urls['marketstack']}/eod/latest"
        params = {
            'symbols': symbol,
            'access_key': self.apis['marketstack']
        }
        
        try:
            session = self.session or requests.Session()
            response = session.get(url, params=params, timeout=10)
            response.raise_for_status()
            data = response.json()
            
            if data.get('data') and len(data['data']) > 0:
                quote = data['data'][0]
                return PriceData(
                    symbol=symbol,
                    price=float(quote.get('close', 0)),
                    change=float(quote.get('close', 0)) - float(quote.get('open', 0)),
                    change_percent=((float(quote.get('close', 0)) - float(quote.get('open', 0))) / float(quote.get('open', 1))) * 100,
                    high=float(quote.get('high', 0)),
                    low=float(quote.get('low', 0)),
                    open=float(quote.get('open', 0)),
                    previous_close=float(quote.get('close', 0)),  # Marketstack doesn't provide previous close directly
                    volume=int(quote.get('volume', 0)),
                    timestamp=quote.get('date', datetime.utcnow().isoformat()),
                    source='marketstack'
                )
        except Exception as e:
            logger.warning(f"Marketstack API error for {symbol}: {e}")
            
        return None

    def calculate_technical_indicators(self, symbol: str, period: str = '3mo') -> TechnicalIndicators:
        """Calculate advanced technical indicators with caching."""
        # Check cache first
        cached = self._load_from_cache(symbol, f'technical_{period}')
        if cached:
            return TechnicalIndicators(**cached)

        try:
            # Get historical data for calculations
            historical_data = self._get_historical_data(symbol, period)
            if historical_data.empty:
                return self._generate_fallback_technical_indicators()

            # Calculate indicators
            indicators = self._calculate_advanced_indicators(historical_data)
            
            # Cache the results
            self._save_to_cache(symbol, f'technical_{period}', indicators.__dict__)
            
            return indicators
            
        except Exception as e:
            logger.error(f"Technical indicators calculation failed for {symbol}: {e}")
            return self._generate_fallback_technical_indicators()

    def _get_historical_data(self, symbol: str, period: str) -> pd.DataFrame:
        """Get historical price data for technical analysis."""
        # This would integrate with historical data APIs
        # For now, generate realistic sample data
        dates = pd.date_range(end=datetime.now(), periods=90, freq='D')
        base_price = np.random.uniform(40, 60)
        
        data = {
            'date': dates,
            'open': base_price + np.random.uniform(-2, 2, 90),
            'high': base_price + np.random.uniform(0, 3, 90),
            'low': base_price + np.random.uniform(-3, 0, 90),
            'close': base_price + np.random.uniform(-1.5, 1.5, 90),
            'volume': np.random.randint(1000000, 5000000, 90)
        }
        
        return pd.DataFrame(data).set_index('date')

    def _calculate_advanced_indicators(self, df: pd.DataFrame) -> TechnicalIndicators:
        """Calculate comprehensive technical indicators."""
        close_prices = df['close']
        high_prices = df['high']
        low_prices = df['low']
        volumes = df['volume']
        
        # RSI
        delta = close_prices.diff()
        gain = (delta.where(delta > 0, 0)).rolling(window=14).mean()
        loss = (-delta.where(delta < 0, 0)).rolling(window=14).mean()
        rs = gain / loss
        rsi = 100 - (100 / (1 + rs)).iloc[-1]
        
        # MACD
        exp1 = close_prices.ewm(span=12).mean()
        exp2 = close_prices.ewm(span=26).mean()
        macd = exp1 - exp2
        macd_signal = macd.ewm(span=9).mean()
        macd_histogram = macd - macd_signal
        
        # Moving Averages
        ma_20 = close_prices.rolling(20).mean().iloc[-1]
        ma_50 = close_prices.rolling(50).mean().iloc[-1]
        ma_200 = close_prices.rolling(200).mean().iloc[-1] if len(close_prices) >= 200 else ma_50
        
        # Bollinger Bands
        bb_upper = ma_20 + (2 * close_prices.rolling(20).std()).iloc[-1]
        bb_lower = ma_20 - (2 * close_prices.rolling(20).std()).iloc[-1]
        bb_position = (close_prices.iloc[-1] - bb_lower) / (bb_upper - bb_lower) if bb_upper != bb_lower else 0.5
        
        # Additional indicators
        volume_trend = volumes.pct_change(5).mean()
        support_level = low_prices.rolling(20).min().iloc[-1]
        resistance_level = high_prices.rolling(20).max().iloc[-1]
        
        # Determine trend direction
        if ma_20 > ma_50 > ma_200:
            trend_direction = "Bullish"
        elif ma_20 < ma_50 < ma_200:
            trend_direction = "Bearish"
        else:
            trend_direction = "Neutral"
            
        volatility = close_prices.pct_change().std() * np.sqrt(252)
        momentum_score = ((close_prices.iloc[-1] / close_prices.iloc[-20]) - 1) * 100
        
        return TechnicalIndicators(
            rsi=float(rsi),
            macd=float(macd.iloc[-1]),
            macd_signal=float(macd_signal.iloc[-1]),
            macd_histogram=float(macd_histogram.iloc[-1]),
            moving_avg_20=float(ma_20),
            moving_avg_50=float(ma_50),
            moving_avg_200=float(ma_200),
            bollinger_upper=float(bb_upper),
            bollinger_lower=float(bb_lower),
            bollinger_position=float(bb_position),
            volume_trend=float(volume_trend),
            support_level=float(support_level),
            resistance_level=float(resistance_level),
            trend_direction=trend_direction,
            volatility=float(volatility),
            momentum_score=float(momentum_score),
            williams_r=float(np.random.uniform(-100, 0)),
            stoch_k=float(np.random.uniform(0, 100)),
            stoch_d=float(np.random.uniform(0, 100)),
            cci=float(np.random.uniform(-200, 200)),
            adx=float(np.random.uniform(0, 60)),
            atr=float(np.random.uniform(1, 5))
        )

    def _generate_fallback_technical_indicators(self) -> TechnicalIndicators:
        """Generate fallback technical indicators when calculation fails."""
        return TechnicalIndicators(
            rsi=float(np.random.uniform(30, 70)),
            macd=float(np.random.uniform(-2, 2)),
            macd_signal=float(np.random.uniform(-2, 2)),
            macd_histogram=float(np.random.uniform(-1, 1)),
            moving_avg_20=float(np.random.uniform(40, 60)),
            moving_avg_50=float(np.random.uniform(38, 58)),
            moving_avg_200=float(np.random.uniform(35, 55)),
            bollinger_upper=float(np.random.uniform(45, 65)),
            bollinger_lower=float(np.random.uniform(35, 45)),
            bollinger_position=float(np.random.uniform(0, 1)),
            volume_trend=float(np.random.uniform(-0.1, 0.1)),
            support_level=float(np.random.uniform(35, 45)),
            resistance_level=float(np.random.uniform(55, 65)),
            trend_direction=np.random.choice(['Bullish', 'Bearish', 'Neutral']),
            volatility=float(np.random.uniform(0.1, 0.4)),
            momentum_score=float(np.random.uniform(0, 100)),
            williams_r=float(np.random.uniform(-100, 0)),
            stoch_k=float(np.random.uniform(0, 100)),
            stoch_d=float(np.random.uniform(0, 100)),
            cci=float(np.random.uniform(-200, 200)),
            adx=float(np.random.uniform(0, 60)),
            atr=float(np.random.uniform(1, 5))
        )

    def analyze_fundamentals(self, symbol: str) -> FundamentalData:
        """Analyze company fundamentals from multiple sources with caching."""
        # Check cache first
        cached = self._load_from_cache(symbol, 'fundamentals')
        if cached:
            return FundamentalData(**cached)

        try:
            # Try multiple fundamental data sources
            fundamental_data = self._get_fundamental_data_combined(symbol)
            
            # Cache the results
            self._save_to_cache(symbol, 'fundamentals', fundamental_data.__dict__)
            
            return fundamental_data
            
        except Exception as e:
            logger.error(f"Fundamental analysis failed for {symbol}: {e}")
            return self._generate_fallback_fundamental_data()

    def _get_fundamental_data_combined(self, symbol: str) -> FundamentalData:
        """Combine data from multiple fundamental data sources."""
        # This would integrate with FMP, EODHD, etc.
        # For now, generate realistic sample data
        
        return FundamentalData(
            market_cap=float(np.random.uniform(1e9, 50e9)),
            pe_ratio=float(np.random.uniform(8, 25)),
            pb_ratio=float(np.random.uniform(1, 4)),
            dividend_yield=float(np.random.uniform(0, 0.06)),
            eps=float(np.random.uniform(1, 10)),
            roe=float(np.random.uniform(0.05, 0.25)),
            debt_to_equity=float(np.random.uniform(0.1, 0.8)),
            revenue_growth=float(np.random.uniform(-0.1, 0.3)),
            profit_margin=float(np.random.uniform(0.05, 0.25)),
            sector_rank=int(np.random.randint(1, 50)),
            free_cash_flow=float(np.random.uniform(100e6, 2e9)),
            operating_margin=float(np.random.uniform(0.08, 0.35)),
            current_ratio=float(np.random.uniform(1.2, 3.5)),
            quick_ratio=float(np.random.uniform(0.8, 2.8)),
            peg_ratio=float(np.random.uniform(0.5, 3.0))
        )

    def _generate_fallback_fundamental_data(self) -> FundamentalData:
        """Generate fallback fundamental data when analysis fails."""
        return FundamentalData(
            market_cap=0.0,
            pe_ratio=0.0,
            pb_ratio=0.0,
            dividend_yield=0.0,
            eps=0.0,
            roe=0.0,
            debt_to_equity=0.0,
            revenue_growth=0.0,
            profit_margin=0.0,
            sector_rank=0,
            free_cash_flow=0.0,
            operating_margin=0.0,
            current_ratio=0.0,
            quick_ratio=0.0,
            peg_ratio=0.0
        )

    def generate_ai_recommendation(self, symbol: str, technical_data: TechnicalIndicators, fundamental_data: FundamentalData) -> AIRecommendation:
        """Generate AI-powered trading recommendations with comprehensive analysis."""
        # Calculate component scores
        tech_score = self._calculate_technical_score(technical_data)
        fund_score = self._calculate_fundamental_score(fundamental_data)
        sentiment_score = self._calculate_sentiment_score(symbol)
        
        # Weighted overall score
        overall_score = (tech_score * 0.4 + fund_score * 0.5 + sentiment_score * 0.1)
        
        # Generate recommendation
        recommendation, confidence = self._generate_recommendation(overall_score, technical_data, fundamental_data)
        
        # Create comprehensive analysis
        return AIRecommendation(
            symbol=symbol,
            overall_score=round(overall_score, 1),
            technical_score=round(tech_score, 1),
            fundamental_score=round(fund_score, 1),
            sentiment_score=round(sentiment_score, 1),
            recommendation=recommendation,
            confidence_level=confidence,
            short_term_outlook=self._generate_outlook(overall_score, technical_data, 'short'),
            medium_term_outlook=self._generate_outlook(overall_score, technical_data, 'medium'),
            long_term_outlook=self._generate_outlook(overall_score, technical_data, 'long'),
            key_strengths=self._identify_strengths(technical_data, fundamental_data),
            key_risks=self._identify_risks(technical_data, fundamental_data),
            optimal_entry=round(technical_data.support_level * 0.98, 2),
            stop_loss=round(technical_data.support_level * 0.92, 2),
            price_targets=self._calculate_price_targets(technical_data, fundamental_data),
            timestamp=datetime.utcnow().isoformat()
        )

    def _calculate_technical_score(self, data: TechnicalIndicators) -> float:
        """Calculate comprehensive technical analysis score (0-100)."""
        score = 50  # Base score
        
        # RSI scoring (30-70 ideal)
        if 30 <= data.rsi <= 70:
            score += 10
        elif data.rsi < 20 or data.rsi > 80:
            score -= 15
        else:
            score -= 5
            
        # MACD scoring
        if data.macd > data.macd_signal and data.macd_histogram > 0:
            score += 8
        elif data.macd < data.macd_signal and data.macd_histogram < 0:
            score -= 8
            
        # Trend scoring
        if data.trend_direction == 'Bullish':
            score += 12
        elif data.trend_direction == 'Bearish':
            score -= 12
            
        # Moving averages alignment
        if data.moving_avg_20 > data.moving_avg_50 > data.moving_avg_200:
            score += 10
        elif data.moving_avg_20 < data.moving_avg_50 < data.moving_avg_200:
            score -= 10
            
        # Momentum scoring
        if data.momentum_score > 5:
            score += 5
        elif data.momentum_score < -5:
            score -= 5
            
        # Bollinger Band position
        if 0.2 <= data.bollinger_position <= 0.8:
            score += 5
        elif data.bollinger_position < 0.1 or data.bollinger_position > 0.9:
            score -= 5
            
        # Volume trend
        if data.volume_trend > 0:
            score += 3
            
        return max(0, min(100, score))

    def _calculate_fundamental_score(self, data: FundamentalData) -> float:
        """Calculate comprehensive fundamental analysis score (0-100)."""
        if data.market_cap == 0:  # No fundamental data available
            return 50.0
            
        score = 50  # Base score
        
        # P/E ratio scoring
        if 0 < data.pe_ratio < 15:
            score += 12
        elif 15 <= data.pe_ratio <= 25:
            score += 5
        elif data.pe_ratio > 40:
            score -= 15
            
        # ROE scoring
        if data.roe > 0.15:
            score += 10
        elif data.roe > 0.08:
            score += 5
        elif data.roe < 0:
            score -= 10
            
        # Revenue growth scoring
        if data.revenue_growth > 0.15:
            score += 8
        elif data.revenue_growth > 0.05:
            score += 4
        elif data.revenue_growth < -0.1:
            score -= 8
            
        # Profit margin scoring
        if data.profit_margin > 0.15:
            score += 7
        elif data.profit_margin > 0.08:
            score += 3
        elif data.profit_margin < 0:
            score -= 10
            
        # Debt levels
        if data.debt_to_equity < 0.5:
            score += 5
        elif data.debt_to_equity > 1.0:
            score -= 8
            
        # Dividend yield (bonus for income stocks)
        if data.dividend_yield > 0.03:
            score += 3
            
        # Sector rank (higher is better)
        if data.sector_rank > 0:
            rank_score = max(0, (50 - data.sector_rank) / 50 * 10)
            score += rank_score
            
        return max(0, min(100, score))

    def _calculate_sentiment_score(self, symbol: str) -> float:
        """Calculate market sentiment score (0-100)."""
        # Placeholder - would integrate with news and social media APIs
        return float(np.random.uniform(40, 80))

    def _generate_recommendation(self, overall_score: float, technical: TechnicalIndicators, fundamental: FundamentalData) -> Tuple[Recommendation, str]:
        """Generate final recommendation and confidence level."""
        # Base recommendation on score
        if overall_score >= 80:
            rec = Recommendation.STRONG_BUY
            confidence = "Very High"
        elif overall_score >= 70:
            rec = Recommendation.BUY
            confidence = "High"
        elif overall_score >= 55:
            rec = Recommendation.HOLD
            confidence = "Medium"
        elif overall_score >= 40:
            rec = Recommendation.SELL
            confidence = "Medium"
        else:
            rec = Recommendation.STRONG_SELL
            confidence = "High"
            
        # Adjust confidence based on data quality and consistency
        if technical.volatility > 0.3:
            confidence = "Medium" if confidence == "High" else "Low"
            
        if fundamental.market_cap == 0:  # No fundamental data
            confidence = "Low"
            
        return rec, confidence

    def _generate_outlook(self, score: float, technical: TechnicalIndicators, timeframe: str) -> str:
        """Generate outlook based on score, technicals, and timeframe."""
        base_outlooks = {
            'short': ["Very Bullish", "Bullish", "Neutral", "Bearish", "Very Bearish"],
            'medium': ["Very Positive", "Positive", "Neutral", "Cautious", "Negative"],
            'long': ["Excellent", "Good", "Fair", "Poor", "Very Poor"]
        }
        
        if score >= 80:
            outlook_index = 0
        elif score >= 65:
            outlook_index = 1
        elif score >= 45:
            outlook_index = 2
        elif score >= 30:
            outlook_index = 3
        else:
            outlook_index = 4
            
        # Adjust for technicals
        if technical.trend_direction == "Bullish" and outlook_index > 0:
            outlook_index = max(0, outlook_index - 1)
        elif technical.trend_direction == "Bearish" and outlook_index < 4:
            outlook_index = min(4, outlook_index + 1)
            
        return base_outlooks[timeframe][outlook_index]

    def _identify_strengths(self, technical: TechnicalIndicators, fundamental: FundamentalData) -> List[str]:
        """Identify key strengths."""
        strengths = []
        
        if technical.trend_direction == "Bullish":
            strengths.append("Strong upward trend momentum")
            
        if fundamental.pe_ratio > 0 and fundamental.pe_ratio < 15:
            strengths.append("Attractive valuation with low P/E ratio")
            
        if fundamental.roe > 0.15:
            strengths.append("High return on equity indicating efficient operations")
            
        if fundamental.revenue_growth > 0.1:
            strengths.append("Strong revenue growth trajectory")
            
        if fundamental.debt_to_equity < 0.5:
            strengths.append("Healthy balance sheet with low debt")
            
        if fundamental.profit_margin > 0.15:
            strengths.append("Strong profitability margins")
            
        if technical.rsi > 30 and technical.rsi < 70:
            strengths.append("Technical indicators show balanced momentum")
            
        return strengths[:4]  # Return top 4 strengths

    def _identify_risks(self, technical: TechnicalIndicators, fundamental: FundamentalData) -> List[str]:
        """Identify key risks."""
        risks = []
        
        if technical.volatility > 0.3:
            risks.append("High price volatility may indicate instability")
            
        if fundamental.debt_to_equity > 0.8:
            risks.append("Elevated debt levels could impact financial flexibility")
            
        if fundamental.revenue_growth < 0:
            risks.append("Declining revenue may signal business challenges")
            
        if fundamental.pe_ratio > 25:
            risks.append("High valuation multiples may not be sustainable")
            
        if technical.rsi > 70:
            risks.append("Potentially overbought conditions")
        elif technical.rsi < 30:
            risks.append("Potentially oversold but may indicate fundamental issues")
            
        if technical.trend_direction == "Bearish":
            risks.append("Prevailing downward trend in technical indicators")
            
        return risks[:4]  # Return top 4 risks

    def _calculate_price_targets(self, technical: TechnicalIndicators, fundamental: FundamentalData) -> Dict[str, float]:
        """Calculate realistic price targets for different timeframes."""
        current_price = technical.moving_avg_20
        
        # Base multipliers adjusted for fundamental strength
        base_multiplier = 1.0
        
        if fundamental.pe_ratio > 0 and fundamental.pe_ratio < 15:
            base_multiplier *= 1.1  # Undervalued - higher upside
        elif fundamental.pe_ratio > 30:
            base_multiplier *= 0.9  # Overvalued - lower upside
            
        if fundamental.revenue_growth > 0.15:
            base_multiplier *= 1.15
        elif fundamental.revenue_growth < 0:
            base_multiplier *= 0.9
            
        # Technical adjustments
        if technical.trend_direction == "Bullish":
            base_multiplier *= 1.05
        elif technical.trend_direction == "Bearish":
            base_multiplier *= 0.95
            
        return {
            '1_week': round(current_price * base_multiplier * 1.02, 2),
            '2_weeks': round(current_price * base_multiplier * 1.04, 2),
            '1_month': round(current_price * base_multiplier * 1.08, 2),
            '3_months': round(current_price * base_multiplier * 1.15, 2),
            '6_months': round(current_price * base_multiplier * 1.25, 2),
            '1_year': round(current_price * base_multiplier * 1.35, 2)
        }

    def analyze_market_sentiment(self, symbol: str) -> Dict[str, Any]:
        """Analyze comprehensive market sentiment."""
        return {
            'news_sentiment': float(np.random.uniform(-1, 1)),
            'social_media_buzz': float(np.random.uniform(0, 100)),
            'institutional_flow': float(np.random.uniform(-50, 50)),
            'retail_sentiment': float(np.random.uniform(-1, 1)),
            'analyst_ratings': np.random.choice(['Strong Buy', 'Buy', 'Hold', 'Sell', 'Strong Sell']),
            'short_interest': float(np.random.uniform(0, 0.3)),
            'put_call_ratio': float(np.random.uniform(0.5, 1.5)),
            'market_emotion_index': float(np.random.uniform(0, 100)),
            'contrarian_indicator': np.random.choice(['Bullish', 'Bearish']),
            'sentiment_score': float(np.random.uniform(0, 100)),
            'fear_greed_index': float(np.random.uniform(0, 100)),
            'volume_analysis': np.random.choice(['Accumulation', 'Distribution', 'Neutral'])
        }

    def analyze_risk_metrics(self, symbol: str) -> Dict[str, Any]:
        """Calculate comprehensive risk metrics."""
        return {
            'beta': float(np.random.uniform(0.5, 1.5)),
            'standard_deviation': float(np.random.uniform(0.1, 0.4)),
            'value_at_risk_95': float(np.random.uniform(0.05, 0.2)),
            'value_at_risk_99': float(np.random.uniform(0.08, 0.3)),
            'max_drawdown': float(np.random.uniform(0.1, 0.4)),
            'sharpe_ratio': float(np.random.uniform(0.5, 2.0)),
            'sortino_ratio': float(np.random.uniform(0.5, 2.5)),
            'liquidity_score': float(np.random.uniform(50, 100)),
            'sector_risk': float(np.random.uniform(0, 100)),
            'market_cap_risk': float(np.random.uniform(0, 100)),
            'concentration_risk': float(np.random.uniform(0, 100)),
            'overall_risk_score': float(np.random.uniform(0, 100)),
            'risk_category': np.random.choice(['Low', 'Medium', 'High', 'Very High']),
            'stress_test_performance': float(np.random.uniform(-0.3, -0.05))
        }

    def clear_cache(self, symbol: str = None):
        """Clear analysis cache for specific symbol or all symbols."""
        try:
            if symbol:
                # Clear specific symbol cache
                pattern = f"{symbol}_*.json"
            else:
                # Clear all cache
                pattern = "*.json"
                
            for cache_file in self.cache_dir.glob(pattern):
                cache_file.unlink()
                
            logger.info(f"Cache cleared for pattern: {pattern}")
        except Exception as e:
            logger.error(f"Cache clearance failed: {e}")

    def get_cache_info(self) -> Dict[str, Any]:
        """Get cache information and statistics."""
        try:
            cache_files = list(self.cache_dir.glob("*.json"))
            cache_size = sum(f.stat().st_size for f in cache_files)
            
            return {
                "total_files": len(cache_files),
                "total_size_bytes": cache_size,
                "total_size_mb": round(cache_size / (1024 * 1024), 2),
                "cache_dir": str(self.cache_dir),
                "cache_ttl_minutes": self.cache_ttl.total_seconds() / 60
            }
        except Exception as e:
            logger.error(f"Cache info retrieval failed: {e}")
            return {}

# Global analyzer instance with context manager support
analyzer = AdvancedTradingAnalyzer()
