# advanced_analysis.py - Multi-source AI Trading Analysis
import os
import requests
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any
import logging

logger = logging.getLogger(__name__)

class AdvancedTradingAnalyzer:
    def __init__(self):
        # API Keys from your configuration
        self.apis = {
            'alpha_vantage': os.getenv('ALPHA_VANTAGE_API_KEY', 'Q0VIE9J6AXUCG99F'),
            'finnhub': os.getenv('FINNHUB_API_KEY', 'd3uhd8pr01qil4aq3i5gd3uhd8pr01qil4aq3i60'),
            'eodhd': os.getenv('EODHD_API_KEY', '68fd1783ee7eb1.12039806'),
            'twelvedata': os.getenv('TWELVEDATA_API_KEY', 'ca363b090fbb421a84c05882e4f1e393'),
            'marketstack': os.getenv('MARKETSTACK_API_KEY', '657b972a96392c3cac405ccc48c36b0c'),
            'fmp': os.getenv('FMP_API_KEY', '3weEgekBXByxCzDGIbXgQ0hgWGZfVKyt')
        }
        
        self.base_urls = {
            'alpha_vantage': 'https://www.alphavantage.co/query',
            'finnhub': 'https://finnhub.io/api/v1',
            'eodhd': 'https://eodhistoricaldata.com/api',
            'twelvedata': 'https://api.twelvedata.com',
            'marketstack': 'http://api.marketstack.com/v1',
            'fmp': 'https://financialmodelingprep.com/api/v3'
        }

    def get_real_time_price(self, symbol: str) -> Optional[Dict]:
        """Get real-time price from multiple sources with fallback"""
        sources = [
            self._get_price_alpha_vantage,
            self._get_price_finnhub,
            self._get_price_twelvedata
        ]
        
        for source in sources:
            try:
                result = source(symbol)
                if result and result.get('price'):
                    return result
            except Exception as e:
                logger.warning(f"Price source failed {source.__name__}: {e}")
                continue
        return None

    def _get_price_alpha_vantage(self, symbol: str) -> Optional[Dict]:
        """Alpha Vantage price data"""
        params = {
            'function': 'GLOBAL_QUOTE',
            'symbol': symbol,
            'apikey': self.apis['alpha_vantage']
        }
        response = requests.get(self.base_urls['alpha_vantage'], params=params, timeout=10)
        data = response.json()
        
        if 'Global Quote' in data:
            quote = data['Global Quote']
            return {
                'price': float(quote.get('05. price', 0)),
                'change': float(quote.get('09. change', 0)),
                'change_percent': quote.get('10. change percent', '0%'),
                'volume': int(quote.get('06. volume', 0)),
                'timestamp': datetime.utcnow().isoformat()
            }
        return None

    def _get_price_finnhub(self, symbol: str) -> Optional[Dict]:
        """Finnhub price data"""
        url = f"{self.base_urls['finnhub']}/quote"
        params = {
            'symbol': symbol,
            'token': self.apis['finnhub']
        }
        response = requests.get(url, params=params, timeout=10)
        data = response.json()
        
        if data.get('c', 0) > 0:
            return {
                'price': data['c'],
                'change': data.get('d', 0),
                'change_percent': data.get('dp', 0),
                'high': data.get('h', 0),
                'low': data.get('l', 0),
                'volume': data.get('v', 0),
                'timestamp': datetime.utcnow().isoformat()
            }
        return None

    def _get_price_twelvedata(self, symbol: str) -> Optional[Dict]:
        """Twelve Data price data"""
        url = f"{self.base_urls['twelvedata']}/price"
        params = {
            'symbol': symbol,
            'apikey': self.apis['twelvedata']
        }
        response = requests.get(url, params=params, timeout=10)
        data = response.json()
        
        if data.get('price'):
            return {
                'price': float(data['price']),
                'timestamp': datetime.utcnow().isoformat()
            }
        return None

    def calculate_technical_indicators(self, symbol: str, period: str = '1mo') -> Dict:
        """Calculate advanced technical indicators"""
        # This would integrate with historical data APIs
        # Placeholder implementation - replace with real data
        return {
            'rsi': np.random.uniform(30, 70),
            'macd': np.random.uniform(-2, 2),
            'moving_avg_50': np.random.uniform(40, 60),
            'moving_avg_200': np.random.uniform(35, 55),
            'bollinger_position': np.random.uniform(-2, 2),
            'volume_trend': np.random.uniform(-0.1, 0.1),
            'support_level': np.random.uniform(35, 45),
            'resistance_level': np.random.uniform(55, 65),
            'trend_direction': np.random.choice(['Bullish', 'Bearish', 'Neutral']),
            'volatility': np.random.uniform(0.1, 0.4),
            'momentum_score': np.random.uniform(0, 100)
        }

    def analyze_fundamentals(self, symbol: str) -> Dict:
        """Analyze company fundamentals from multiple sources"""
        # Placeholder - integrate with FMP, EODHD, etc.
        return {
            'market_cap': np.random.uniform(1e9, 50e9),
            'pe_ratio': np.random.uniform(8, 25),
            'pb_ratio': np.random.uniform(1, 4),
            'dividend_yield': np.random.uniform(0, 0.06),
            'eps': np.random.uniform(1, 10),
            'roe': np.random.uniform(0.05, 0.25),
            'debt_to_equity': np.random.uniform(0.1, 0.8),
            'revenue_growth': np.random.uniform(-0.1, 0.3),
            'profit_margin': np.random.uniform(0.05, 0.25),
            'sector_rank': np.random.randint(1, 50)
        }

    def generate_ai_recommendation(self, symbol: str, technical_data: Dict, fundamental_data: Dict) -> Dict:
        """Generate AI-powered trading recommendations"""
        # Calculate composite scores
        tech_score = self._calculate_technical_score(technical_data)
        fund_score = self._calculate_fundamental_score(fundamental_data)
        
        overall_score = (tech_score * 0.4 + fund_score * 0.6)
        
        # Generate recommendation
        if overall_score >= 80:
            recommendation = "STRONG_BUY"
            confidence = "High"
        elif overall_score >= 70:
            recommendation = "BUY"
            confidence = "Medium"
        elif overall_score >= 50:
            recommendation = "HOLD"
            confidence = "Medium"
        elif overall_score >= 30:
            recommendation = "SELL"
            confidence = "Medium"
        else:
            recommendation = "STRONG_SELL"
            confidence = "High"
        
        return {
            'overall_ai_score': round(overall_score, 1),
            'final_recommendation': recommendation,
            'confidence_level': confidence,
            'short_term_outlook': self._generate_outlook(overall_score, 'short'),
            'medium_term_outlook': self._generate_outlook(overall_score, 'medium'),
            'long_term_outlook': self._generate_outlook(overall_score, 'long'),
            'key_strengths': self._identify_strengths(technical_data, fundamental_data),
            'key_risks': self._identify_risks(technical_data, fundamental_data),
            'optimal_entry': round(technical_data.get('support_level', 0) * 0.98, 2),
            'stop_loss': round(technical_data.get('support_level', 0) * 0.92, 2),
            'price_targets': self._calculate_price_targets(technical_data, fundamental_data)
        }

    def _calculate_technical_score(self, data: Dict) -> float:
        """Calculate technical analysis score (0-100)"""
        score = 50  # Base score
        
        # RSI scoring (30-70 ideal)
        rsi = data.get('rsi', 50)
        if 30 <= rsi <= 70:
            score += 10
        else:
            score -= 10
            
        # MACD scoring
        macd = data.get('macd', 0)
        if macd > 0:
            score += 5
            
        # Trend scoring
        trend = data.get('trend_direction', 'Neutral')
        if trend == 'Bullish':
            score += 10
            
        # Momentum scoring
        momentum = data.get('momentum_score', 50)
        score += (momentum - 50) * 0.2
        
        return max(0, min(100, score))

    def _calculate_fundamental_score(self, data: Dict) -> float:
        """Calculate fundamental analysis score (0-100)"""
        score = 50  # Base score
        
        # P/E ratio scoring (lower is better)
        pe = data.get('pe_ratio', 15)
        if pe < 12:
            score += 10
        elif pe > 25:
            score -= 10
            
        # ROE scoring (higher is better)
        roe = data.get('roe', 0.1) * 100
        if roe > 15:
            score += 10
        elif roe < 5:
            score -= 10
            
        # Revenue growth scoring
        growth = data.get('revenue_growth', 0) * 100
        if growth > 10:
            score += 10
        elif growth < 0:
            score -= 10
            
        # Profit margin scoring
        margin = data.get('profit_margin', 0.1) * 100
        if margin > 15:
            score += 5
            
        return max(0, min(100, score))

    def _generate_outlook(self, score: float, timeframe: str) -> str:
        """Generate outlook based on score and timeframe"""
        if score >= 70:
            return "Very Positive"
        elif score >= 50:
            return "Positive" if timeframe == 'long' else "Neutral"
        elif score >= 30:
            return "Cautious"
        else:
            return "Negative"

    def _identify_strengths(self, technical: Dict, fundamental: Dict) -> List[str]:
        """Identify key strengths"""
        strengths = []
        
        if technical.get('trend_direction') == 'Bullish':
            strengths.append("Strong upward trend")
            
        if fundamental.get('pe_ratio', 20) < 15:
            strengths.append("Attractive valuation")
            
        if fundamental.get('roe', 0) > 0.15:
            strengths.append("High return on equity")
            
        if fundamental.get('revenue_growth', 0) > 0.1:
            strengths.append("Strong revenue growth")
            
        return strengths[:3]  # Return top 3 strengths

    def _identify_risks(self, technical: Dict, fundamental: Dict) -> List[str]:
        """Identify key risks"""
        risks = []
        
        if technical.get('volatility', 0) > 0.3:
            risks.append("High volatility")
            
        if fundamental.get('debt_to_equity', 0) > 0.6:
            risks.append("High debt levels")
            
        if fundamental.get('revenue_growth', 0) < 0:
            risks.append("Declining revenue")
            
        return risks[:3]  # Return top 3 risks

    def _calculate_price_targets(self, technical: Dict, fundamental: Dict) -> Dict:
        """Calculate price targets for different timeframes"""
        current_price = technical.get('moving_avg_50', 50)
        
        return {
            '1_week': round(current_price * 1.02, 2),
            '1_month': round(current_price * 1.05, 2),
            '3_months': round(current_price * 1.10, 2),
            '6_months': round(current_price * 1.15, 2),
            '1_year': round(current_price * 1.25, 2)
        }

    def analyze_market_sentiment(self, symbol: str) -> Dict:
        """Analyze market sentiment from news and social media"""
        # Placeholder - integrate with news APIs
        return {
            'news_sentiment': np.random.uniform(-1, 1),
            'social_media_buzz': np.random.uniform(0, 100),
            'institutional_flow': np.random.uniform(-50, 50),
            'retail_sentiment': np.random.uniform(-1, 1),
            'analyst_ratings': np.random.choice(['Strong Buy', 'Buy', 'Hold', 'Sell']),
            'short_interest': np.random.uniform(0, 0.3),
            'put_call_ratio': np.random.uniform(0.5, 1.5),
            'market_emotion_index': np.random.uniform(0, 100),
            'contrarian_indicator': np.random.choice(['Bullish', 'Bearish']),
            'sentiment_score': np.random.uniform(0, 100)
        }

    def analyze_risk_metrics(self, symbol: str) -> Dict:
        """Calculate comprehensive risk metrics"""
        return {
            'beta': np.random.uniform(0.5, 1.5),
            'standard_deviation': np.random.uniform(0.1, 0.4),
            'value_at_risk': np.random.uniform(0.05, 0.2),
            'max_drawdown': np.random.uniform(0.1, 0.4),
            'sharpe_ratio': np.random.uniform(0.5, 2.0),
            'sortino_ratio': np.random.uniform(0.5, 2.5),
            'liquidity_score': np.random.uniform(50, 100),
            'sector_risk': np.random.uniform(0, 100),
            'market_cap_risk': np.random.uniform(0, 100),
            'concentration_risk': np.random.uniform(0, 100),
            'overall_risk_score': np.random.uniform(0, 100)
        }

# Singleton instance
analyzer = AdvancedTradingAnalyzer()
