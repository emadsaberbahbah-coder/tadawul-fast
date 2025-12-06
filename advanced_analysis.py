from __future__ import annotations

import asyncio
import json
import logging
import math
import os
import pickle
import time
from collections import deque
from contextlib import asynccontextmanager
from dataclasses import dataclass, asdict, field
from datetime import datetime, timedelta
from enum import Enum
from pathlib import Path
from typing import Any, Deque, Dict, List, Optional, Tuple

import aiohttp
import numpy as np
import threading

logger = logging.getLogger(__name__)


# ============================================================================
# Core Enums & Data Models
# ============================================================================


class DataQuality(Enum):
    EXCELLENT = "EXCELLENT"
    HIGH = "HIGH"
    MEDIUM = "MEDIUM"
    LOW = "LOW"
    SYNTHETIC = "SYNTHETIC"
    UNRELIABLE = "UNRELIABLE"


class Recommendation(Enum):
    STRONG_BUY = "STRONG_BUY"
    BUY = "BUY"
    ACCUMULATE = "ACCUMULATE"
    HOLD = "HOLD"
    REDUCE = "REDUCE"
    SELL = "SELL"
    STRONG_SELL = "STRONG_SELL"


class MarketRegime(Enum):
    BULL = "BULL"
    BEAR = "BEAR"
    SIDEWAYS = "SIDEWAYS"


@dataclass
class PriceData:
    symbol: str
    price: float
    previous_close: float = 0.0
    open: float = 0.0
    high: float = 0.0
    low: float = 0.0
    volume: float = 0.0
    avg_volume: float = 0.0
    change: float = 0.0
    change_percent: float = 0.0
    data_quality: DataQuality = DataQuality.UNRELIABLE
    last_updated: str = ""

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


@dataclass
class TechnicalIndicators:
    symbol: str
    rsi: float = 50.0
    adx: float = 20.0
    dmi_plus: float = 0.0
    dmi_minus: float = 0.0
    volatility: float = 0.2
    trend_direction: str = "Neutral"
    bollinger_position: float = 0.5
    support_levels: List[float] = field(default_factory=list)
    resistance_levels: List[float] = field(default_factory=list)
    volume_trend: float = 0.0
    market_regime: MarketRegime = MarketRegime.SIDEWAYS
    data_quality: DataQuality = DataQuality.UNRELIABLE

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)

    # Simple placeholder implementations – replace with your real signal logic
    def get_buy_signals(self) -> List[str]:
        signals: List[str] = []
        if self.rsi < 30:
            signals.append("RSI_Oversold")
        if self.trend_direction == "Bullish":
            signals.append("Trend_Bullish")
        return signals

    def get_sell_signals(self) -> List[str]:
        signals: List[str] = []
        if self.rsi > 70:
            signals.append("RSI_Overbought")
        if self.trend_direction == "Bearish":
            signals.append("Trend_Bearish")
        return signals


@dataclass
class FundamentalData:
    symbol: str
    sector: str = ""
    industry: str = ""
    market_cap: float = 0.0
    pe_ratio: float = 0.0
    pb_ratio: float = 0.0
    dividend_yield: float = 0.0
    payout_ratio: float = 0.0
    roe: float = 0.0
    roa: float = 0.0
    debt_to_equity: float = 0.0
    current_ratio: float = 0.0
    quick_ratio: float = 0.0
    revenue_growth: float = 0.0
    net_income_growth: float = 0.0
    ebitda_margin: float = 0.0
    operating_margin: float = 0.0
    net_margin: float = 0.0
    price_to_sales: float = 0.0
    price_to_cash_flow: float = 0.0
    peg_ratio: float = 0.0
    forward_eps: float = 0.0
    forward_revenue: float = 0.0
    analyst_rating: str = ""
    price_target: float = 0.0
    industry_pe: float = 0.0
    sector_rank: Optional[int] = None
    profit_margin: Optional[float] = None
    data_quality: DataQuality = DataQuality.UNRELIABLE

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


@dataclass
class MarketSentiment:
    symbol: str
    overall_sentiment: float = 0.0    # -1.0 to +1.0
    confidence_score: float = 0.5     # 0.0 to 1.0

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


@dataclass
class PortfolioAllocation:
    symbol: str
    recommended_allocation: float
    max_allocation: float
    min_allocation: float
    allocation_reason: str
    risk_adjusted_allocation: float
    scenario_analysis: Dict[str, Any]


@dataclass
class AIRecommendation:
    symbol: str
    overall_score: float
    technical_score: float
    fundamental_score: float
    sentiment_score: float
    risk_adjusted_score: float
    momentum_score: float
    value_score: float
    quality_score: float
    recommendation: Recommendation
    recommendation_strength: float
    confidence_level: str
    expected_return: float
    expected_volatility: float
    sharpe_ratio: float
    sortino_ratio: float
    short_term_outlook: str
    medium_term_outlook: str
    long_term_outlook: str
    swing_trade_outlook: str
    risk_level: str
    risk_factors: List[Dict[str, Any]]
    max_drawdown_estimate: float
    value_at_risk: float
    stop_loss_levels: Dict[str, float]
    position_sizing: PortfolioAllocation
    optimal_entry: Dict[str, float]
    take_profit_levels: Dict[str, float]
    trailing_stop_percent: float
    key_strengths: List[str]
    key_weaknesses: List[str]
    opportunities: List[str]
    threats: List[str]
    market_regime: MarketRegime
    sector_outlook: str
    competitive_position: str
    timestamp: str
    data_sources_used: int
    analysis_duration: float
    model_version: str
    explainable_factors: Dict[str, float]
    scenario_analysis: Dict[str, Any]


# ============================================================================
# Enhanced Trading Analyzer
# ============================================================================


class EnhancedTradingAnalyzer:
    """Core AI analysis engine (self-contained version)."""

    def __init__(
        self,
        cache_dir: Optional[str | Path] = None,
        cache_ttl_minutes: int = 60,
        rate_limit_rpm: int = 60,
        analysis_timeout: int = 20,
        enable_ml: bool = True,
    ) -> None:
        self.model_version = "4.0.0"
        self.cache_dir = Path(cache_dir or "./analyzer_cache")
        self.cache_dir.mkdir(parents=True, exist_ok=True)

        self.cache_ttl = timedelta(minutes=cache_ttl_minutes)
        self.memory_cache: Dict[str, Any] = {}
        self.memory_cache_ttl: Dict[str, float] = {}

        # Optional Redis client; integrate if available
        self.redis_client = None

        self.request_timestamps: Deque[float] = deque()
        self.rate_limit_rpm = rate_limit_rpm

        self.stats: Dict[str, Any] = {
            "requests": 0,
            "errors": 0,
            "cache_hits": 0,
            "cache_misses": 0,
            "start_time": time.time(),
        }
        self.performance_metrics: Dict[str, List[float]] = {}

        self.analysis_timeout = analysis_timeout
        self.enable_ml = enable_ml

        self._session: Optional[aiohttp.ClientSession] = None
        self._session_sync = None
        self._rate_lock = threading.Lock()

        self.apis = {
            "FMP_API_KEY": os.getenv("FMP_API_KEY") or os.getenv("FMP_API_KEY".lower(), ""),
            "ALPHA_VANTAGE_API_KEY": os.getenv("ALPHA_VANTAGE_API_KEY")
            or os.getenv("ALPHA_VANTAGE_API_KEY".lower(), ""),
        }

        self.base_urls = {
            "fmp": "https://financialmodelingprep.com/api/v3",
            "alpha_vantage": "https://www.alphavantage.co/query",
        }

    # ------------------------------------------------------------------ #
    # Core data access stubs – replace with your production providers
    # ------------------------------------------------------------------ #

    async def get_real_time_price(self, symbol: str) -> Optional[PriceData]:
        """
        Fetch real-time price data.

        NOTE: This is a minimal placeholder implementation.
        Replace with calls to your production quote engine.
        """
        # In production, call your quote API here.
        # We keep a simple synthetic response to avoid runtime errors.
        now = datetime.utcnow().isoformat() + "Z"
        price = 100.0
        previous_close = 99.0
        change = price - previous_close
        change_pct = change / previous_close * 100.0

        return PriceData(
            symbol=symbol.upper(),
            price=price,
            previous_close=previous_close,
            open=99.5,
            high=101.0,
            low=98.5,
            volume=1_000_000,
            avg_volume=900_000,
            change=change,
            change_percent=change_pct,
            data_quality=DataQuality.SYNTHETIC,
            last_updated=now,
        )

    async def calculate_technical_indicators(
        self, symbol: str
    ) -> TechnicalIndicators:
        """
        Calculate technical indicators.

        NOTE: Placeholder implementation; wire to your TA engine.
        """
        return TechnicalIndicators(
            symbol=symbol.upper(),
            rsi=50.0,
            adx=20.0,
            dmi_plus=20.0,
            dmi_minus=20.0,
            volatility=0.2,
            trend_direction="Neutral",
            bollinger_position=0.5,
            support_levels=[],
            resistance_levels=[],
            volume_trend=0.0,
            market_regime=MarketRegime.SIDEWAYS,
            data_quality=DataQuality.SYNTHETIC,
        )

    async def analyze_market_sentiment(self, symbol: str) -> MarketSentiment:
        """
        Analyze market sentiment.

        NOTE: Placeholder implementation; connect to news / social feeds.
        """
        return MarketSentiment(
            symbol=symbol.upper(),
            overall_sentiment=0.0,
            confidence_score=0.5,
        )

    # =========================================================================
    # Enhanced Fundamental Analysis
    # =========================================================================

    async def analyze_fundamentals(self, symbol: str) -> FundamentalData:
        """Analyze fundamentals using multiple providers with fallback."""
        start_time = time.time()
        symbol = symbol.upper().strip()

        # Try multiple providers; failures are logged but not fatal
        providers: List[Tuple[str, Any]] = []

        try:
            fmp_data = await self._get_fundamentals_fmp_enhanced(symbol)
            if fmp_data:
                providers.append(("FMP", fmp_data))
        except Exception as e:
            logger.warning(f"FMP fundamentals failed for {symbol}: {e}")

        try:
            av_data = await self._get_fundamentals_alpha_vantage(symbol)
            if av_data:
                providers.append(("AlphaVantage", av_data))
        except Exception as e:
            logger.warning(f"AlphaVantage fundamentals failed for {symbol}: {e}")

        try:
            yf_data = await self._get_fundamentals_yfinance(symbol)
            if yf_data:
                providers.append(("YahooFinance", yf_data))
        except Exception as e:
            logger.warning(f"Yahoo Finance fundamentals failed for {symbol}: {e}")

        if not providers:
            logger.warning(
                f"No fundamental providers returned data for {symbol}, using fallback"
            )
            return self._generate_fallback_fundamental_data(symbol, time.time() - start_time)

        # Combine all successful providers
        combined = self._combine_fundamental_data(symbol, providers)
        logger.info(
            f"Fundamental analysis completed for {symbol} using "
            f"{len(providers)} providers in {time.time() - start_time:.2f}s"
        )
        return combined

    async def _get_fundamentals_fmp_enhanced(self, symbol: str) -> Optional[Dict[str, Any]]:
        """Get fundamental data from FinancialModelingPrep."""
        api_key = self.apis.get("FMP_API_KEY")
        if not api_key:
            return None

        url_profile = f"{self.base_urls['fmp']}/profile/{symbol}"
        url_key_metrics = f"{self.base_urls['fmp']}/key-metrics-ttm/{symbol}"
        url_ratios = f"{self.base_urls['fmp']}/ratios-ttm/{symbol}"

        params = {"apikey": api_key}

        profile, key_metrics, ratios = await asyncio.gather(
            self._make_async_request(url_profile, params=params),
            self._make_async_request(url_key_metrics, params=params),
            self._make_async_request(url_ratios, params=params),
        )

        if not profile:
            return None

        profile_item = profile[0] if isinstance(profile, list) and profile else profile
        key_item = key_metrics[0] if isinstance(key_metrics, list) and key_metrics else {}
        ratio_item = ratios[0] if isinstance(ratios, list) and ratios else {}

        return:
            "provider": "FMP",
            "sector": profile_item.get("sector"),
            "industry": profile_item.get("industry"),
            "market_cap": float(profile_item.get("mktCap") or 0.0),
            "pe_ratio": float(profile_item.get("pe") or 0.0),
            "pb_ratio": float(profile_item.get("pb") or 0.0),
            "dividend_yield": float(profile_item.get("lastDiv") or 0.0),
            "payout_ratio": float(ratio_item.get("payoutRatioTTM") or 0.0),
            "roe": float(ratio_item.get("roeTTM") or 0.0),
            "roa": float(ratio_item.get("roaTTM") or 0.0),
            "debt_to_equity": float(ratio_item.get("debtEquityRatioTTM") or 0.0),
            "current_ratio": float(ratio_item.get("currentRatioTTM") or 0.0),
            "quick_ratio": float(ratio_item.get("quickRatioTTM") or 0.0),
            "revenue_growth": float(key_item.get("revenueGrowthTTM") or 0.0),
            "net_income_growth": float(key_item.get("netIncomeGrowthTTM") or 0.0),
            "ebitda_margin": float(ratio_item.get("ebitdaMarginTTM") or 0.0),
            "operating_margin": float(ratio_item.get("operatingMarginTTM") or 0.0),
            "net_margin": float(ratio_item.get("netMarginTTM") or 0.0),
            "price_to_sales": float(ratio_item.get("priceToSalesRatioTTM") or 0.0),
            "price_to_cash_flow": float(ratio_item.get("priceToCashFlowRatioTTM") or 0.0),
            "peg_ratio": float(ratio_item.get("pegRatioTTM") or 0.0),
            "data_quality": DataQuality.HIGH,
        }

    async def _get_fundamentals_alpha_vantage(self, symbol: str) -> Optional[Dict[str, Any]]:
        """Get fundamental data from Alpha Vantage."""
        api_key = self.apis.get("ALPHA_VANTAGE_API_KEY")
        if not api_key:
            return None

        url = self.base_urls["alpha_vantage"]
        params = {
            "function": "OVERVIEW",
            "symbol": symbol,
            "apikey": api_key,
        }

        data = await self._make_async_request(url, params=params)
        if not data or not isinstance(data, dict) or "Symbol" not in data:
            return None

        try:
            return {
                "provider": "AlphaVantage",
                "sector": data.get("Sector"),
                "industry": data.get("Industry"),
                "market_cap": float(data.get("MarketCapitalization") or 0.0),
                "pe_ratio": float(data.get("PERatio") or 0.0),
                "pb_ratio": float(data.get("PriceToBookRatio") or 0.0),
                "dividend_yield": float(data.get("DividendYield") or 0.0),
                "payout_ratio": float(data.get("PayoutRatio") or 0.0),
                "roe": float(data.get("ReturnOnEquityTTM") or 0.0),
                "roa": float(data.get("ReturnOnAssetsTTM") or 0.0),
                "debt_to_equity": float(data.get("DebtToEquity") or 0.0),
                "current_ratio": float(data.get("CurrentRatio") or 0.0),
                "quick_ratio": float(data.get("QuickRatio") or 0.0),
                "revenue_growth": float(data.get("QuarterlyRevenueGrowthYOY") or 0.0),
                "net_income_growth": float(
                    data.get("QuarterlyEarningsGrowthYOY") or 0.0
                ),
                "ebitda_margin": 0.0,  # not directly available
                "operating_margin": float(data.get("OperatingMarginTTM") or 0.0),
                "net_margin": float(data.get("ProfitMargin") or 0.0),
                "price_to_sales": float(data.get("PriceToSalesRatioTTM") or 0.0),
                "price_to_cash_flow": float(
                    data.get("PriceToOperatingCashFlowsRatioTTM") or 0.0
                ),
                "peg_ratio": float(data.get("PEGRatio") or 0.0),
                "data_quality": DataQuality.MEDIUM,
            }
        except Exception as e:
            logger.warning(f"Alpha Vantage parsing failed for {symbol}: {e}")
            return None

    async def _get_fundamentals_yfinance(self, symbol: str) -> Optional[Dict[str, Any]]:
        """
        Get fundamentals from yfinance.

        NOTE: This implementation expects yfinance to be available in your environment.
        If not installed, it returns None gracefully.
        """
        try:
            import yfinance as yf  # type: ignore
        except Exception:
            return None

        try:
            ticker = yf.Ticker(symbol)
            info = ticker.info or {}
            if not info:
                return None

            return {
                "provider": "YahooFinance",
                "sector": info.get("sector"),
                "industry": info.get("industry"),
                "market_cap": float(info.get("marketCap") or 0.0),
                "pe_ratio": float(info.get("trailingPE") or 0.0),
                "pb_ratio": float(info.get("priceToBook") or 0.0),
                "dividend_yield": float(info.get("dividendYield") or 0.0),
                "payout_ratio": float(info.get("payoutRatio") or 0.0),
                "roe": float(info.get("returnOnEquity") or 0.0),
                "roa": float(info.get("returnOnAssets") or 0.0),
                "debt_to_equity": float(info.get("debtToEquity") or 0.0),
                "current_ratio": float(info.get("currentRatio") or 0.0),
                "quick_ratio": float(info.get("quickRatio") or 0.0),
                "revenue_growth": float(info.get("revenueGrowth") or 0.0),
                "net_income_growth": float(info.get("earningsQuarterlyGrowth") or 0.0),
                "ebitda_margin": float(info.get("ebitdaMargins") or 0.0),
                "operating_margin": float(info.get("operatingMargins") or 0.0),
                "net_margin": float(info.get("profitMargins") or 0.0),
                "price_to_sales": float(info.get("priceToSalesTrailing12Months") or 0.0),
                "price_to_cash_flow": float(info.get("priceToCashflow") or 0.0),
                "peg_ratio": float(info.get("pegRatio") or 0.0),
                "data_quality": DataQuality.MEDIUM,
            }
        except Exception as e:
            logger.warning(f"yfinance fundamentals failed for {symbol}: {e}")
            return None

    def _combine_fundamental_data(
        self,
        symbol: str,
        providers: List[Tuple[str, Dict[str, Any]]],
    ) -> FundamentalData:
        """Combine multiple fundamental providers into a single object."""
        numeric_fields = [
            "market_cap",
            "pe_ratio",
            "pb_ratio",
            "dividend_yield",
            "payout_ratio",
            "roe",
            "roa",
            "debt_to_equity",
            "current_ratio",
            "quick_ratio",
            "revenue_growth",
            "net_income_growth",
            "ebitda_margin",
            "operating_margin",
            "net_margin",
            "price_to_sales",
            "price_to_cash_flow",
            "peg_ratio",
            "industry_pe",
        ]

        combined: Dict[str, Any] = {field: 0.0 for field in numeric_fields}
        weights: Dict[str, float] = {
            "FMP": 1.0,
            "AlphaVantage": 0.8,
            "YahooFinance": 0.9,
        }

        total_weight: Dict[str, float] = {field: 0.0 for field in numeric_fields}

        sectors: List[str] = []
        industries: List[str] = []

        for provider_name, data in providers:
            weight = weights.get(provider_name, 0.5)
            for field in numeric_fields:
                value = data.get(field)
                if value is None:
                    continue
                try:
                    v = float(value)
                except (TypeError, ValueError):
                    continue
                combined[field] += v * weight
                total_weight[field] += weight

            sector = data.get("sector")
            if sector:
                sectors.append(sector)

            industry = data.get("industry")
            if industry:
                industries.append(industry)

        for field in numeric_fields:
            if total_weight[field] > 0:
                combined[field] /= total_weight[field]

        sector = max(set(sectors), key=sectors.count) if sectors else ""
        industry = max(set(industries), key=industries.count) if industries else ""

        avg_quality_score = np.mean(
            [
                self._data_quality_to_score(DataQuality.HIGH if p[0] == "FMP" else DataQuality.MEDIUM)
                for p in providers
            ]
        )
        if avg_quality_score >= 0.9:
            dq = DataQuality.EXCELLENT
        elif avg_quality_score >= 0.8:
            dq = DataQuality.HIGH
        elif avg_quality_score >= 0.6:
            dq = DataQuality.MEDIUM
        elif avg_quality_score >= 0.4:
            dq = DataQuality.LOW
        else:
            dq = DataQuality.UNRELIABLE

        return FundamentalData(
            symbol=symbol,
            sector=sector,
            industry=industry,
            market_cap=combined["market_cap"],
            pe_ratio=combined["pe_ratio"],
            pb_ratio=combined["pb_ratio"],
            dividend_yield=combined["dividend_yield"],
            payout_ratio=combined["payout_ratio"],
            roe=combined["roe"],
            roa=combined["roa"],
            debt_to_equity=combined["debt_to_equity"],
            current_ratio=combined["current_ratio"],
            quick_ratio=combined["quick_ratio"],
            revenue_growth=combined["revenue_growth"],
            net_income_growth=combined["net_income_growth"],
            ebitda_margin=combined["ebitda_margin"],
            operating_margin=combined["operating_margin"],
            net_margin=combined["net_margin"],
            price_to_sales=combined["price_to_sales"],
            price_to_cash_flow=combined["price_to_cash_flow"],
            peg_ratio=combined["peg_ratio"],
            industry_pe=combined["industry_pe"],
            data_quality=dq,
        )

    def _generate_fallback_fundamental_data(
        self, symbol: str, duration: float
    ) -> FundamentalData:
        """Fallback fundamentals used when all providers fail."""
        logger.warning(
            f"Generating synthetic fundamentals for {symbol} "
            f"after {duration:.2f}s of provider failures"
        )
        return FundamentalData(
            symbol=symbol,
            sector="Unknown",
            industry="Unknown",
            market_cap=0.0,
            pe_ratio=0.0,
            pb_ratio=0.0,
            dividend_yield=0.0,
            payout_ratio=0.0,
            roe=0.0,
            roa=0.0,
            debt_to_equity=0.0,
            current_ratio=0.0,
            quick_ratio=0.0,
            revenue_growth=0.0,
            net_income_growth=0.0,
            ebitda_margin=0.0,
            operating_margin=0.0,
            net_margin=0.0,
            price_to_sales=0.0,
            price_to_cash_flow=0.0,
            peg_ratio=0.0,
            industry_pe=0.0,
            data_quality=DataQuality.SYNTHETIC,
        )

    # =========================================================================
    # VALUE, QUALITY, RISK-ADJUSTED SCORES
    # =========================================================================

    def _calculate_value_score(
        self, fundamental: FundamentalData, price: PriceData
    ) -> float:
        """Calculate value score."""
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

        return float(max(0.0, min(100.0, score)))

    def _calculate_quality_score(
        self, fundamental: FundamentalData, technical: TechnicalIndicators
    ) -> float:
        """Calculate quality score."""
        score = 50.0

        if fundamental.roe > 0.15:
            score += 10
        elif fundamental.roe < 0.05:
            score -= 10

        if fundamental.net_margin > 0.1:
            score += 10
        elif fundamental.net_margin < 0.02:
            score -= 10

        if technical.volatility < 0.2:
            score += 10
        elif technical.volatility > 0.4:
            score -= 10

        if fundamental.revenue_growth > 0 and fundamental.net_income_growth > 0:
            score += 10
        elif fundamental.revenue_growth < 0 and fundamental.net_income_growth < 0:
            score -= 10

        return float(max(0.0, min(100.0, score)))

    def _get_score_weights(self, timeframe: str) -> Dict[str, float]:
        """Score weighting by timeframe."""
        weights = {
            "intraday": {
                "technical": 0.35,
                "sentiment": 0.25,
                "momentum": 0.25,
                "fundamental": 0.05,
                "value": 0.05,
                "quality": 0.05,
            },
            "short_term": {
                "technical": 0.30,
                "sentiment": 0.20,
                "momentum": 0.25,
                "fundamental": 0.15,
                "value": 0.05,
                "quality": 0.05,
            },
            "medium_term": {
                "technical": 0.20,
                "sentiment": 0.15,
                "momentum": 0.20,
                "fundamental": 0.25,
                "value": 0.10,
                "quality": 0.10,
            },
            "long_term": {
                "technical": 0.10,
                "sentiment": 0.10,
                "momentum": 0.10,
                "fundamental": 0.40,
                "value": 0.15,
                "quality": 0.15,
            },
        }
        return weights.get(timeframe, weights["medium_term"])

    def _calculate_risk_adjusted_score(
        self,
        overall_score: float,
        technical: TechnicalIndicators,
        fundamental: FundamentalData,
    ) -> float:
        """Risk-adjusted scoring."""
        risk_penalty = 0.0

        if technical.volatility > 0.3:
            risk_penalty += 10
        if fundamental.debt_to_equity > 1.0:
            risk_penalty += 8
        if technical.rsi > 80 or technical.rsi < 20:
            risk_penalty += 5

        risk_adjusted = overall_score - risk_penalty

        if technical.volatility < 0.15 and fundamental.debt_to_equity < 0.5:
            risk_adjusted += 5

        return float(max(0.0, min(100.0, risk_adjusted)))

    # =========================================================================
    # RECOMMENDATION ENGINE
    # =========================================================================

    def _generate_recommendation_details(
        self,
        overall_score: float,
        technical: TechnicalIndicators,
        fundamental: FundamentalData,
        sentiment: MarketSentiment,
    ) -> Tuple[Recommendation, float]:
        """Assign recommendation and strength."""
        if overall_score >= 80:
            rec, strength = Recommendation.STRONG_BUY, (overall_score - 80) / 20
        elif overall_score >= 70:
            rec, strength = Recommendation.BUY, (overall_score - 70) / 10
        elif overall_score >= 60:
            rec, strength = Recommendation.ACCUMULATE, (overall_score - 60) / 10
        elif overall_score >= 50:
            rec, strength = Recommendation.HOLD, 0.5
        elif overall_score >= 40:
            rec, strength = Recommendation.REDUCE, (50 - overall_score) / 10
        elif overall_score >= 30:
            rec, strength = Recommendation.SELL, (40 - overall_score) / 10
        else:
            rec, strength = Recommendation.STRONG_SELL, (30 - overall_score) / 30

        if technical.volatility > 0.3:
            strength *= 0.8
        if fundamental.data_quality == DataQuality.LOW:
            strength *= 0.7
        if (
            sentiment.overall_sentiment > 0.3
            and rec in (Recommendation.BUY, Recommendation.STRONG_BUY)
        ):
            strength *= 1.1

        return rec, float(max(0.1, min(1.0, strength)))

    # =========================================================================
    # RISK ENGINE
    # =========================================================================

    def _assess_risk_enhanced(
        self,
        technical: TechnicalIndicators,
        fundamental: FundamentalData,
        sentiment: MarketSentiment,
    ) -> Tuple[str, List[Dict[str, Any]]]:
        """Advanced risk assessment."""
        risk_factors: List[Dict[str, Any]] = []
        risk_score = 0

        if technical.volatility > 0.3:
            risk_score += 2
            risk_factors.append(
                {
                    "factor": "High Volatility",
                    "severity": "high",
                    "description": f"Volatility {technical.volatility:.1%}",
                }
            )
        elif technical.volatility > 0.2:
            risk_score += 1

        if fundamental.debt_to_equity > 1.0:
            risk_score += 2
            risk_factors.append(
                {
                    "factor": "High Debt",
                    "severity": "high",
                    "description": f"D/E {fundamental.debt_to_equity:.2f}",
                }
            )
        elif fundamental.debt_to_equity > 0.7:
            risk_score += 1

        if fundamental.current_ratio < 1.0:
            risk_score += 2
            risk_factors.append(
                {
                    "factor": "Low Liquidity",
                    "severity": "high",
                    "description": f"Current ratio {fundamental.current_ratio:.2f}",
                }
            )
        elif fundamental.current_ratio < 1.5:
            risk_score += 1

        if sentiment.overall_sentiment < -0.3:
            risk_score += 1

        if technical.rsi > 80 or technical.rsi < 20:
            risk_score += 1

        if risk_score >= 4:
            return "HIGH", risk_factors
        elif risk_score >= 2:
            return "MEDIUM", risk_factors
        else:
            return "LOW", risk_factors

    # =========================================================================
    # POSITION SIZING
    # =========================================================================

    def _calculate_position_sizing(
        self,
        symbol: str,
        overall_score: float,
        risk_level: str,
        fundamental: FundamentalData,
    ) -> PortfolioAllocation:
        """Position size guidance."""
        base = {"HIGH": 2.0, "MEDIUM": 5.0, "LOW": 10.0}.get(risk_level, 5.0)

        if overall_score >= 80:
            base *= 1.5
        elif overall_score >= 70:
            base *= 1.2
        elif overall_score < 50:
            base *= 0.5

        if fundamental.market_cap < 1e9:
            base *= 0.7
        elif fundamental.market_cap > 50e9:
            base *= 1.2

        recommended = float(max(1.0, min(15.0, base)))
        max_alloc = float(min(20.0, recommended * 1.5))
        min_alloc = float(max(0.5, recommended * 0.5))

        risk_factor = 1 - {"HIGH": 0.5, "MEDIUM": 0.2, "LOW": 0.0}.get(
            risk_level, 0.2
        )

        return PortfolioAllocation(
            symbol=symbol,
            recommended_allocation=recommended,
            max_allocation=max_alloc,
            min_allocation=min_alloc,
            risk_adjusted_allocation=recommended * risk_factor,
            allocation_reason=self._get_allocation_reason(
                recommended, risk_level, overall_score
            ),
            scenario_analysis={
                "bull_case": recommended * 1.5,
                "base_case": recommended,
                "bear_case": recommended * 0.5,
            },
        )

    def _get_allocation_reason(
        self, recommended: float, risk_level: str, overall_score: float
    ) -> str:
        return (
            f"Recommended allocation {recommended:.1f}% based on {risk_level} risk "
            f"profile and overall score {overall_score:.1f}"
        )

    # =========================================================================
    # TRADING LEVELS
    # =========================================================================

    def _calculate_trading_levels(
        self,
        price: PriceData,
        technical: TechnicalIndicators,
        recommendation: Recommendation,
    ) -> Tuple[Dict[str, float], Dict[str, float], Dict[str, float]]:
        """Entry / TP / Stop levels."""
        p = price.price

        optimal_entry = {
            "aggressive": p
            * (
                0.98
                if recommendation
                in (Recommendation.STRONG_BUY, Recommendation.BUY, Recommendation.ACCUMULATE)
                else 1.0
            ),
            "moderate": p
            * (
                0.96
                if recommendation
                in (Recommendation.STRONG_BUY, Recommendation.BUY, Recommendation.ACCUMULATE)
                else 1.0
            ),
            "conservative": p
            * (
                0.94
                if recommendation
                in (Recommendation.STRONG_BUY, Recommendation.BUY, Recommendation.ACCUMULATE)
                else 1.0
            ),
        }

        if recommendation in (Recommendation.STRONG_BUY, Recommendation.BUY):
            take_profit = {
                "target_1": p * 1.05,
                "target_2": p * 1.10,
                "target_3": p * 1.15,
            }
        elif recommendation == Recommendation.ACCUMULATE:
            take_profit = {
                "target_1": p * 1.03,
                "target_2": p * 1.06,
                "target_3": p * 1.09,
            }
        else:
            take_profit = {"target_1": p, "target_2": p, "target_3": p}

        stop_loss = {
            "initial": technical.support_levels[0] * 0.97
            if technical.support_levels
            else p * 0.92,
            "trailing": p * 0.93,
            "breakeven": p * 1.01,
        }

        return optimal_entry, take_profit, stop_loss

    # =========================================================================
    # OUTLOOK GENERATION (short, medium, long, swing)
    # =========================================================================

    def _generate_short_term_outlook(
        self, overall_score: float, technical: TechnicalIndicators
    ) -> str:
        if overall_score >= 75 and technical.trend_direction == "Bullish":
            return "Bullish short-term momentum with favorable technical setup"
        if overall_score <= 40 and technical.trend_direction == "Bearish":
            return "Bearish short-term outlook; caution warranted"
        return "Neutral short-term outlook with mixed technical signals"

    def _generate_medium_term_outlook(
        self, overall_score: float, fundamental: FundamentalData
    ) -> str:
        if overall_score >= 75 and fundamental.revenue_growth > 0.1:
            return "Attractive medium-term growth with solid fundamentals"
        if overall_score <= 40 and fundamental.revenue_growth <= 0:
            return "Challenging medium-term outlook given weak fundamentals"
        return "Balanced medium-term outlook with moderate risk/return profile"

    def _generate_long_term_outlook(
        self, overall_score: float, fundamental: FundamentalData
    ) -> str:
        if overall_score >= 80 and fundamental.roe > 0.15:
            return "Compelling long-term compounder with strong value creation"
        if overall_score <= 35:
            return "Uncertain long-term prospects; high selectivity required"
        return "Reasonable long-term potential contingent on execution"

    def _generate_swing_trade_outlook(
        self, technical: TechnicalIndicators
    ) -> str:
        if technical.rsi < 30 and technical.trend_direction != "Bearish":
            return "Potential swing long setup from oversold conditions"
        if technical.rsi > 70 and technical.trend_direction != "Bullish":
            return "Potential swing short / profit-taking zone"
        return "No high-conviction swing setup detected"

    def _generate_outlooks(
        self,
        overall_score: float,
        technical: TechnicalIndicators,
        fundamental: FundamentalData,
    ) -> Dict[str, str]:
        return {
            "short_term": self._generate_short_term_outlook(overall_score, technical),
            "medium_term": self._generate_medium_term_outlook(overall_score, fundamental),
            "long_term": self._generate_long_term_outlook(overall_score, fundamental),
            "swing_trade": self._generate_swing_trade_outlook(technical),
        }

    # =========================================================================
    # CONSOLIDATION & SCENARIO ANALYSIS
    # =========================================================================

    def _calculate_explainable_factors(
        self,
        tech_score: float,
        fund_score: float,
        sent_score: float,
        mom_score: float,
        val_score: float,
        qual_score: float,
    ) -> Dict[str, float]:
        """Explainability breakdown."""
        total = tech_score + fund_score + sent_score + mom_score + val_score + qual_score
        if total > 0:
            return {
                "technical_contribution": tech_score / total * 100.0,
                "fundamental_contribution": fund_score / total * 100.0,
                "sentiment_contribution": sent_score / total * 100.0,
                "momentum_contribution": mom_score / total * 100.0,
                "value_contribution": val_score / total * 100.0,
                "quality_contribution": qual_score / total * 100.0,
            }
        return {
            k: 16.67
            for k in [
                "technical_contribution",
                "fundamental_contribution",
                "sentiment_contribution",
                "momentum_contribution",
                "value_contribution",
                "quality_contribution",
            ]
        }

    def _generate_scenario_analysis(
        self,
        price: PriceData,
        technical: TechnicalIndicators,
        fundamental: FundamentalData,
    ) -> Dict[str, Any]:
        """Bull / Base / Bear scenario engine."""
        p = price.price
        return {
            "bull_case": {
                "probability": 0.30,
                "target_price": p * 1.25,
                "timeframe": "6-12 months",
                "catalysts": [
                    "Strong earnings beat",
                    "Sector rotation",
                    "Market leadership",
                ],
            },
            "base_case": {
                "probability": 0.50,
                "target_price": p * 1.10,
                "timeframe": "6-12 months",
                "catalysts": [
                    "Steady growth",
                    "Stable margins",
                    "Positive sentiment",
                ],
            },
            "bear_case": {
                "probability": 0.20,
                "target_price": p * 0.85,
                "timeframe": "6-12 months",
                "catalysts": [
                    "Earnings miss",
                    "Sector weakness",
                    "Macro correction",
                ],
            },
        }

    def _calculate_expected_metrics(
        self, technical: TechnicalIndicators, fundamental: FundamentalData
    ) -> Tuple[float, float]:
        """Expected return & volatility engine."""
        base_return = 0.08

        if technical.trend_direction == "Bullish":
            base_return += 0.02
        elif technical.trend_direction == "Bearish":
            base_return -= 0.03

        if fundamental.roe > 0.15:
            base_return += 0.03
        elif fundamental.roe < 0.05:
            base_return -= 0.02

        if fundamental.revenue_growth > 0.15:
            base_return += 0.02

        expected_volatility = technical.volatility
        return base_return, expected_volatility

    def _calculate_sortino_ratio(
        self, expected_return: float, volatility: float
    ) -> float:
        """Sortino ratio."""
        rf = 0.02
        downside_dev = volatility * 0.7
        return float((expected_return - rf) / downside_dev) if downside_dev > 0 else 0.0

    def _determine_confidence(
        self, tech_q: DataQuality, fund_q: DataQuality
    ) -> str:
        """Confidence level."""
        weights = {
            DataQuality.EXCELLENT: 5,
            DataQuality.HIGH: 4,
            DataQuality.MEDIUM: 3,
            DataQuality.LOW: 2,
            DataQuality.SYNTHETIC: 1,
            DataQuality.UNRELIABLE: 0,
        }
        avg = (weights.get(tech_q, 0) + weights.get(fund_q, 0)) / 2
        if avg >= 4:
            return "Very High"
        if avg >= 3:
            return "High"
        if avg >= 2:
            return "Medium"
        if avg >= 1:
            return "Low"
        return "Very Low"

    def _estimate_max_drawdown(self, technical: TechnicalIndicators) -> float:
        return float(min(0.30, technical.volatility * 1.5))

    def _calculate_var(
        self, technical: TechnicalIndicators, fundamental: FundamentalData
    ) -> float:
        """Value at Risk (95%)."""
        var = technical.volatility * 1.645
        if fundamental.debt_to_equity > 1.0:
            var *= 1.2
        if fundamental.current_ratio < 1.0:
            var *= 1.3
        return float(min(0.50, var))

    def _calculate_trailing_stop(self, technical: TechnicalIndicators) -> float:
        if technical.volatility > 0.3:
            return 15.0
        if technical.volatility > 0.2:
            return 10.0
        return 7.0

    # =========================================================================
    # SWOT ANALYSIS
    # =========================================================================

    def _identify_strengths_enhanced(
        self, technical: TechnicalIndicators, fundamental: FundamentalData
    ) -> List[str]:
        """Identify key strengths."""
        strengths: List[str] = []

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

        profit_margin = getattr(
            fundamental, "profit_margin", fundamental.net_margin
        )
        if profit_margin and profit_margin > 0.15:
            strengths.append("Strong profitability margins")

        if technical.adx > 25 and technical.dmi_plus > technical.dmi_minus:
            strengths.append("Strong trend with positive directional movement")

        if technical.volume_trend > 0.05:
            strengths.append("Increasing volume supporting price movement")

        if not strengths:
            strengths.append("Stable operations with moderate growth prospects")

        return strengths[:5]

    def _identify_weaknesses(
        self, technical: TechnicalIndicators, fundamental: FundamentalData
    ) -> List[str]:
        """Identify key weaknesses."""
        weaknesses: List[str] = []

        if technical.volatility > 0.3:
            weaknesses.append("High price volatility indicating instability")

        if fundamental.debt_to_equity > 1.0:
            weaknesses.append(
                "Elevated debt levels could impact financial flexibility"
            )

        if fundamental.revenue_growth < 0:
            weaknesses.append("Declining revenue may signal business challenges")

        if fundamental.pe_ratio > 30:
            weaknesses.append("High valuation multiples may not be sustainable")

        if technical.rsi > 70:
            weaknesses.append("Potentially overbought conditions")
        elif technical.rsi < 30:
            weaknesses.append(
                "Potentially oversold; may indicate underlying weakness"
            )

        if technical.trend_direction == "Bearish":
            weaknesses.append("Prevailing downward trend in technical indicators")

        if not weaknesses:
            weaknesses.append("Standard market risks apply")

        return weaknesses[:4]

    def _identify_opportunities(
        self,
        technical: TechnicalIndicators,
        fundamental: FundamentalData,
        sentiment: MarketSentiment,
    ) -> List[str]:
        """Identify opportunities."""
        opportunities: List[str] = []

        if technical.rsi < 30:
            opportunities.append("Oversold conditions may present buying opportunity")

        industry_pe = getattr(fundamental, "industry_pe", None)
        if (
            industry_pe is not None
            and industry_pe > 0
            and fundamental.pe_ratio > 0
            and fundamental.pe_ratio < industry_pe
        ):
            opportunities.append("Trading below industry average P/E")

        if sentiment.overall_sentiment > 0.3:
            opportunities.append(
                "Positive market sentiment could drive further gains"
            )

        if technical.bollinger_position < 0.2:
            opportunities.append(
                "Trading near lower Bollinger Band, potential rebound"
            )

        if fundamental.dividend_yield > 0.04:
            opportunities.append(
                "Attractive dividend yield for income-focused investors"
            )

        if not opportunities:
            opportunities.append("Market efficiency limits obvious opportunities")

        return opportunities[:3]

    def _identify_threats(
        self,
        technical: TechnicalIndicators,
        fundamental: FundamentalData,
        sentiment: MarketSentiment,
    ) -> List[str]:
        """Identify threats."""
        threats: List[str] = []

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
        """Assess sector outlook."""
        sector = (fundamental.sector or "").lower()

        if sector in ["technology", "healthcare"]:
            return "Favorable - Growth sectors with strong long-term prospects"
        elif sector in ["financials", "consumer staples"]:
            return "Stable - Defensive sectors with reliable performance"
        elif sector in ["energy", "materials"]:
            return "Cyclical - Dependent on economic cycles"
        elif sector in ["utilities", "real estate"]:
            return "Income-focused - Stable dividends but limited growth"
        else:
            return "Mixed - Varies by specific industry dynamics"

    def _assess_competitive_position(self, fundamental: FundamentalData) -> str:
        """Assess competitive position."""
        rank = getattr(fundamental, "sector_rank", None)
        if rank is None:
            return "Unknown"

        if rank <= 10:
            return "Leadership - Top tier in sector"
        elif rank <= 25:
            return "Strong - Above average competitive position"
        elif rank <= 50:
            return "Average - Mid-tier competitive position"
        elif rank <= 75:
            return "Challenged - Below average competitive position"
        else:
            return "Weak - Bottom tier in sector"

    def _generate_fallback_recommendation(
        self, symbol: str, timeframe: str, duration: float
    ) -> AIRecommendation:
        """Generate fallback recommendation when data collection fails."""
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
            risk_factors=[
                {
                    "factor": "Data Unavailability",
                    "severity": "high",
                    "description": "Unable to collect sufficient data for proper analysis",
                }
            ],
            max_drawdown_estimate=0.2,
            value_at_risk=0.15,
            stop_loss_levels={"initial": 0.0, "trailing": 0.0, "breakeven": 0.0},
            position_sizing=PortfolioAllocation(
                symbol=symbol,
                recommended_allocation=0.0,
                max_allocation=0.0,
                min_allocation=0.0,
                allocation_reason="Insufficient data for position sizing",
                risk_adjusted_allocation=0.0,
                scenario_analysis={},
            ),
            optimal_entry={"aggressive": 0.0, "moderate": 0.0, "conservative": 0.0},
            take_profit_levels={
                "target_1": 0.0,
                "target_2": 0.0,
                "target_3": 0.0,
            },
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
            scenario_analysis={},
        )

    # =========================================================================
    # Enhanced Multi-source Analysis
    # =========================================================================

    async def get_multi_source_analysis(self, symbol: str) -> Dict[str, Any]:
        """Enhanced multi-source analysis with comprehensive data."""
        start_time = time.time()
        symbol = symbol.upper().strip()
        cache_key = self._get_cache_key(symbol, "multisource_v2")

        cached = await self._load_from_cache(cache_key)
        if cached:
            cached.get("metadata", {}).update({"cache_hit": True})
            return cached

        try:
            price_data = await self.get_real_time_price(symbol)
            technical_data = await self.calculate_technical_indicators(symbol)
            fundamental_data = await self.analyze_fundamentals(symbol)
            sentiment_data = await self.analyze_market_sentiment(symbol)

            consolidated = self._consolidate_multi_source_data_enhanced(
                price_data, technical_data, fundamental_data, sentiment_data
            )

            successful_sources = sum(
                1
                for data in [
                    price_data,
                    technical_data,
                    fundamental_data,
                    sentiment_data,
                ]
                if data is not None
            )

            analysis = {
                "symbol": symbol,
                "price_data": price_data.to_dict() if price_data else None,
                "technical_indicators": (
                    technical_data.to_dict() if technical_data else None
                ),
                "fundamental_data": (
                    fundamental_data.to_dict() if fundamental_data else None
                ),
                "market_sentiment": (
                    sentiment_data.to_dict() if sentiment_data else None
                ),
                "consolidated_analysis": consolidated,
                "timestamp": datetime.utcnow().isoformat() + "Z",
                "metadata": {
                    "data_sources_used": 4,
                    "successful_sources": successful_sources,
                    "analysis_duration": time.time() - start_time,
                    "cache_hit": False,
                    "data_quality": self._assess_overall_quality(
                        price_data.data_quality
                        if price_data
                        else DataQuality.UNRELIABLE,
                        technical_data.data_quality
                        if technical_data
                        else DataQuality.UNRELIABLE,
                        fundamental_data.data_quality
                        if fundamental_data
                        else DataQuality.UNRELIABLE,
                        DataQuality.MEDIUM,
                    ).value,
                },
            }

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
                    "data_quality": "UNRELIABLE",
                },
            }

    def _consolidate_multi_source_data_enhanced(
        self,
        price_data: Optional[PriceData],
        technical_data: TechnicalIndicators,
        fundamental_data: FundamentalData,
        sentiment_data: MarketSentiment,
    ) -> Dict[str, Any]:
        """Enhanced data consolidation."""
        if not price_data:
            return {"error": "No price data available"}

        consensus_price = price_data.price
        consensus_volume = price_data.volume

        quality_scores = {
            "price": self._data_quality_to_score(price_data.data_quality),
            "technical": self._data_quality_to_score(technical_data.data_quality),
            "fundamental": self._data_quality_to_score(fundamental_data.data_quality),
            "sentiment": float(sentiment_data.confidence_score),
        }

        avg_quality = float(np.mean(list(quality_scores.values())))

        risk_score = self._calculate_risk_score_consolidated(
            technical_data, fundamental_data, sentiment_data
        )
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
            ),
        }

    def _data_quality_to_score(self, quality: DataQuality) -> float:
        """Convert data quality to score."""
        quality_map = {
            DataQuality.EXCELLENT: 0.95,
            DataQuality.HIGH: 0.85,
            DataQuality.MEDIUM: 0.70,
            DataQuality.LOW: 0.50,
            DataQuality.SYNTHETIC: 0.30,
            DataQuality.UNRELIABLE: 0.10,
        }
        return float(quality_map.get(quality, 0.50))

    def _assess_overall_quality(self, *qualities: DataQuality) -> DataQuality:
        """Assess overall data quality."""
        quality_scores = {
            DataQuality.EXCELLENT: 5,
            DataQuality.HIGH: 4,
            DataQuality.MEDIUM: 3,
            DataQuality.LOW: 2,
            DataQuality.SYNTHETIC: 1,
            DataQuality.UNRELIABLE: 0,
        }

        avg_score = float(
            np.mean([quality_scores.get(q, 0) for q in qualities])
        )

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

    def _calculate_risk_score_consolidated(
        self,
        technical: TechnicalIndicators,
        fundamental: FundamentalData,
        sentiment: MarketSentiment,
    ) -> float:
        """Calculate consolidated risk score."""
        risk_factors: List[float] = []

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
            return float(min(1.0, sum(risk_factors) / len(risk_factors)))
        return 0.1

    def _calculate_opportunity_score(
        self,
        technical: TechnicalIndicators,
        fundamental: FundamentalData,
        sentiment: MarketSentiment,
    ) -> float:
        """Calculate opportunity score."""
        opportunity_factors: List[float] = []

        # Technical opportunity
        if technical.trend_direction == "Bullish":
            opportunity_factors.append(0.3)

        if technical.rsi < 30:
            opportunity_factors.append(0.2)

        if technical.bollinger_position < 0.2:
            opportunity_factors.append(0.15)

        # Fundamental opportunity
        if 0 < fundamental.pe_ratio < 15:
            opportunity_factors.append(0.2)

        if fundamental.roe > 0.15:
            opportunity_factors.append(0.15)

        # Sentiment opportunity
        if sentiment.overall_sentiment > 0.3:
            opportunity_factors.append(0.1)

        if opportunity_factors:
            return float(min(1.0, sum(opportunity_factors) / len(opportunity_factors)))
        return 0.3

    def _assess_valuation_status(self, fundamental: FundamentalData) -> str:
        """Assess valuation status."""
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
        """Assess sentiment status."""
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

    def _generate_key_insights(
        self,
        price_data: PriceData,
        technical: TechnicalIndicators,
        fundamental: FundamentalData,
        sentiment: MarketSentiment,
    ) -> List[str]:
        """Generate key insights."""
        insights: List[str] = []

        # Price insights
        if price_data.change_percent > 5:
            insights.append(f"Strong price momentum: +{price_data.change_percent:.1f}%")
        elif price_data.change_percent < -5:
            insights.append(
                f"Significant decline: {price_data.change_percent:.1f}%"
            )

        # Technical insights
        if technical.rsi < 30:
            insights.append("Oversold conditions based on RSI")
        elif technical.rsi > 70:
            insights.append("Overbought conditions based on RSI")

        if technical.trend_direction == "Bullish" and technical.adx > 25:
            insights.append("Strong bullish trend with good momentum")

        # Fundamental vs industry P/E
        industry_pe = getattr(fundamental, "industry_pe", None)
        if (
            industry_pe is not None
            and industry_pe > 0
            and fundamental.pe_ratio > 0
        ):
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

    def _generate_recommendation_summary(
        self, technical: TechnicalIndicators, fundamental: FundamentalData
    ) -> str:
        """Generate recommendation summary."""
        buy_signals = len(technical.get_buy_signals())
        sell_signals = len(technical.get_sell_signals())

        if buy_signals > sell_signals + 2:
            return "Bullish - Multiple buy signals outweigh sell signals"
        elif sell_signals > buy_signals + 2:
            return "Bearish - Multiple sell signals outweigh buy signals"
        elif 0 < fundamental.pe_ratio < 15:
            return "Value Opportunity - Attractive valuation with reasonable risk"
        elif fundamental.pe_ratio > 30:
            return "Caution - High valuation multiples present risk"
        else:
            return "Neutral - Balanced factors suggest holding position"

    # =========================================================================
    # Enhanced Utility Methods
    # =========================================================================

    async def _make_async_request(
        self,
        url: str,
        params: Optional[Dict[str, Any]] = None,
        headers: Optional[Dict[str, str]] = None,
    ) -> Optional[Any]:
        """Enhanced async HTTP request with better error handling."""
        params = params or {}
        headers = headers or {}

        for attempt in range(3):  # Max 3 retries
            start_time = time.time()
            try:
                await self._enforce_rate_limit()

                session = await self.get_session()
                async with session.get(url, params=params, headers=headers) as response:
                    self.stats["requests"] += 1

                    if response.status == 200:
                        try:
                            content_type = response.headers.get("Content-Type", "")
                            if "application/json" in content_type:
                                data = await response.json()
                            else:
                                text = await response.text()
                                try:
                                    data = json.loads(text)
                                except Exception:
                                    data = text

                            duration = time.time() - start_time
                            self.performance_metrics.setdefault(
                                "response_times", []
                            ).append(duration)

                            return data
                        except Exception as e:
                            logger.warning(f"Response parsing failed: {e}")
                            self.stats["errors"] += 1
                            return None
                    else:
                        logger.warning(f"HTTP {response.status} for {url}")
                        self.stats["errors"] += 1

                        if response.status == 429:
                            await asyncio.sleep(2**attempt)
                            continue

                        return None

            except asyncio.TimeoutError:
                logger.error(f"Timeout for {url} (attempt {attempt + 1})")
                self.stats["errors"] += 1
                if attempt < 2:
                    await asyncio.sleep(1)
                continue
            except Exception as e:
                logger.error(f"Request failed for {url} (attempt {attempt + 1}): {e}")
                self.stats["errors"] += 1
                if attempt < 2:
                    await asyncio.sleep(1)
                continue

        return None

    async def get_session(self) -> aiohttp.ClientSession:
        if self._session is None or self._session.closed:
            timeout = aiohttp.ClientTimeout(total=self.analysis_timeout)
            self._session = aiohttp.ClientSession(timeout=timeout)
        return self._session

    async def _enforce_rate_limit(self) -> None:
        """Enhanced rate limiting."""
        while True:
            with self._rate_lock:
                now = time.time()
                window_start = now - 60

                while self.request_timestamps and self.request_timestamps[0] < window_start:
                    self.request_timestamps.popleft()

                if len(self.request_timestamps) < self.rate_limit_rpm:
                    self.request_timestamps.append(now)
                    return

                oldest = self.request_timestamps[0]
                sleep_time = 60 - (now - oldest)

            if sleep_time > 0:
                await asyncio.sleep(sleep_time + 0.1)

    def _get_cache_key(self, symbol: str, suffix: str) -> str:
        return f"{symbol.upper()}::{suffix}"

    async def _load_from_cache(self, key: str) -> Optional[Any]:
        """Load from memory/file cache."""
        now = time.time()

        # Memory cache
        ttl = self.memory_cache_ttl.get(key)
        if ttl and ttl > now:
            self.stats["cache_hits"] += 1
            return self.memory_cache.get(key)

        if key in self.memory_cache:
            self.memory_cache.pop(key, None)
            self.memory_cache_ttl.pop(key, None)

        # File cache
        cache_file = self.cache_dir / f"{key}.pkl"
        if cache_file.exists():
            try:
                if now - cache_file.stat().st_mtime <= self.cache_ttl.total_seconds():
                    with open(cache_file, "rb") as f:
                        value = pickle.load(f)
                    self.stats["cache_hits"] += 1
                    return value
                else:
                    cache_file.unlink(missing_ok=True)
            except Exception as e:
                logger.debug(f"Failed to load cache file {cache_file}: {e}")

        self.stats["cache_misses"] += 1
        return None

    async def _save_to_cache(self, key: str, value: Any) -> None:
        """Save to memory/file cache."""
        now = time.time()
        self.memory_cache[key] = value
        self.memory_cache_ttl[key] = now + self.cache_ttl.total_seconds()

        cache_file = self.cache_dir / f"{key}.pkl"
        try:
            with open(cache_file, "wb") as f:
                pickle.dump(value, f)
        except Exception as e:
            logger.debug(f"Failed to save cache file {cache_file}: {e}")

    async def clear_cache(self, symbol: Optional[str] = None) -> Dict[str, Any]:
        """Enhanced cache clearing."""
        cleared_count = 0

        try:
            # Memory cache
            if symbol:
                symbol_upper = symbol.upper()
                keys_to_remove = [
                    k for k in list(self.memory_cache.keys()) if symbol_upper in k
                ]
                for key in keys_to_remove:
                    if self.memory_cache.pop(key, None) is not None:
                        cleared_count += 1
                    self.memory_cache_ttl.pop(key, None)
            else:
                cleared_count += len(self.memory_cache)
                self.memory_cache.clear()
                self.memory_cache_ttl.clear()

            # Redis cache (optional)
            if self.redis_client:
                try:
                    if symbol:
                        pattern = f"*{symbol.upper()}*"
                        keys = self.redis_client.keys(pattern)
                        if keys:
                            self.redis_client.delete(*keys)
                            cleared_count += len(keys)
                    else:
                        keys = self.redis_client.keys("*v4_*")
                        if keys:
                            self.redis_client.delete(*keys)
                            cleared_count += len(keys)
                except Exception as e:
                    logger.error(f"Redis cache clear failed: {e}")

            # File cache
            if symbol:
                pattern = symbol.upper()
                cache_files = list(self.cache_dir.glob(f"*{pattern}*"))
            else:
                cache_files = list(self.cache_dir.glob("*.pkl"))

            for cache_file in cache_files:
                try:
                    cache_file.unlink(missing_ok=True)
                    cleared_count += 1
                except Exception as e:
                    logger.debug(f"Failed to delete cache file {cache_file}: {e}")

            logger.info(f"Cache cleared: {cleared_count} items removed")
            return {
                "status": "success",
                "cleared_count": cleared_count,
                "timestamp": datetime.utcnow().isoformat() + "Z",
            }

        except Exception as e:
            logger.error(f"Cache clearance failed: {e}")
            return {
                "status": "error",
                "error": str(e),
                "timestamp": datetime.utcnow().isoformat() + "Z",
            }

    async def get_cache_info(self) -> Dict[str, Any]:
        """Get enhanced cache information."""
        try:
            # Memory cache stats
            memory_stats = {
                "entries": len(self.memory_cache),
                "memory_usage_mb": sum(
                    len(pickle.dumps(v)) for v in self.memory_cache.values()
                )
                / (1024 * 1024)
                if self.memory_cache
                else 0.0,
            }

            # File cache stats
            cache_files = list(self.cache_dir.glob("*.pkl"))
            file_stats = {
                "total_files": len(cache_files),
                "total_size_mb": sum(f.stat().st_size for f in cache_files)
                / (1024 * 1024)
                if cache_files
                else 0.0,
                "oldest_file": min(
                    (f.stat().st_mtime for f in cache_files), default=None
                ),
                "newest_file": max(
                    (f.stat().st_mtime for f in cache_files), default=None
                ),
            }

            # Redis stats
            redis_stats: Dict[str, Any] = {}
            if self.redis_client:
                try:
                    redis_stats = {
                        "connected": True,
                        "keys_count": len(self.redis_client.keys("*")),
                        "memory_usage": self.redis_client.info().get(
                            "used_memory", 0
                        ),
                    }
                except Exception as e:
                    redis_stats = {"connected": False, "error": str(e)}

            total_accesses = (
                self.stats["cache_hits"] + self.stats["cache_misses"]
            )
            hit_rate = (
                self.stats["cache_hits"] / total_accesses
                if total_accesses > 0
                else 0.0
            )

            return {
                "memory_cache": memory_stats,
                "file_cache": file_stats,
                "redis_cache": redis_stats,
                "performance": {
                    "hit_rate": hit_rate,
                    "total_hits": self.stats["cache_hits"],
                    "total_misses": self.stats["cache_misses"],
                    "cache_ttl_minutes": self.cache_ttl.total_seconds() / 60.0,
                },
                "timestamp": datetime.utcnow().isoformat() + "Z",
            }

        except Exception as e:
            logger.error(f"Cache info retrieval failed: {e}")
            return {"error": str(e)}

    def get_statistics(self) -> Dict[str, Any]:
        """Get enhanced statistics."""
        with self._rate_lock:
            total_requests = self.stats["requests"]
            error_rate = (
                self.stats["errors"] / total_requests * 100.0
                if total_requests > 0
                else 0.0
            )

            response_times = self.performance_metrics.get("response_times", [])
            avg_response_time = (
                float(np.mean(response_times)) if response_times else 0.0
            )

            total_cache_access = (
                self.stats["cache_hits"] + self.stats["cache_misses"]
            )
            cache_efficiency = (
                self.stats["cache_hits"] / total_cache_access * 100.0
                if total_cache_access > 0
                else 0.0
            )

            uptime = time.time() - self.stats["start_time"]

            return {
                "requests": {
                    "total": total_requests,
                    "errors": self.stats["errors"],
                    "error_rate_percent": round(error_rate, 2),
                    "avg_response_time_ms": round(avg_response_time * 1000.0, 2),
                },
                "cache": {
                    "hits": self.stats["cache_hits"],
                    "misses": self.stats["cache_misses"],
                    "efficiency_percent": round(cache_efficiency, 2),
                },
                "performance": {
                    "uptime_seconds": round(uptime, 2),
                    "current_rpm": len(self.request_timestamps),
                    "max_rpm": self.rate_limit_rpm,
                },
                "configuration": {
                    "model_version": self.model_version,
                    "analysis_timeout": self.analysis_timeout,
                    "cache_ttl_minutes": self.cache_ttl.total_seconds() / 60.0,
                    "enable_ml": self.enable_ml,
                },
                "timestamp": datetime.utcnow().isoformat() + "Z",
            }

    async def close(self) -> None:
        """Clean shutdown."""
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

        self._save_statistics()

    def _save_statistics(self) -> None:
        """Save statistics to file."""
        try:
            stats_file = self.cache_dir / "statistics.json"
            stats_data = {
                "last_run": datetime.utcnow().isoformat() + "Z",
                "total_requests": self.stats["requests"],
                "total_errors": self.stats["errors"],
                "cache_hits": self.stats["cache_hits"],
                "cache_misses": self.stats["cache_misses"],
                "uptime_seconds": time.time() - self.stats["start_time"],
            }

            with open(stats_file, "w") as f:
                json.dump(stats_data, f, indent=2)
        except Exception as e:
            logger.debug(f"Failed to save statistics: {e}")


# Global analyzer instance
try:
    analyzer = EnhancedTradingAnalyzer()
    logger.info(
        f"Global EnhancedTradingAnalyzer v{analyzer.model_version} created successfully"
    )
except Exception as e:
    logger.error(f"Failed to create global EnhancedTradingAnalyzer: {e}")
    analyzer = None


@asynccontextmanager
async def get_analyzer():
    """Async context manager for analyzer."""
    analyzer_instance = EnhancedTradingAnalyzer()
    try:
        yield analyzer_instance
    finally:
        await analyzer_instance.close()


# Convenience functions
async def generate_ai_recommendation(
    symbol: str, timeframe: str = "medium_term"
) -> AIRecommendation:
    if analyzer is None:
        raise RuntimeError("Analyzer not initialized")

    start_time = time.time()
    symbol_u = symbol.upper().strip()

    # Fetch core building blocks
    price = await analyzer.get_real_time_price(symbol_u)
    technical = await analyzer.calculate_technical_indicators(symbol_u)
    fundamental = await analyzer.analyze_fundamentals(symbol_u)
    sentiment = await analyzer.analyze_market_sentiment(symbol_u)

    if not (price and technical and fundamental and sentiment):
        return analyzer._generate_fallback_recommendation(
            symbol_u, timeframe, time.time() - start_time
        )

    # Core scores
    tech_score = 60.0  # placeholder – integrate real TA scoring if available
    fund_score = 60.0  # placeholder – integrate real fundamental scoring
    sent_score = (sentiment.overall_sentiment + 1.0) * 50.0
    mom_score = max(0.0, min(100.0, 50.0 + price.change_percent))
    val_score = analyzer._calculate_value_score(fundamental, price)
    qual_score = analyzer._calculate_quality_score(fundamental, technical)

    weights = analyzer._get_score_weights(timeframe)
    overall_score = float(
        tech_score * weights["technical"]
        + fund_score * weights["fundamental"]
        + sent_score * weights["sentiment"]
        + mom_score * weights["momentum"]
        + val_score * weights["value"]
        + qual_score * weights["quality"]
    )

    risk_level, risk_factors = analyzer._assess_risk_enhanced(
        technical, fundamental, sentiment
    )
    risk_adjusted_score = analyzer._calculate_risk_adjusted_score(
        overall_score, technical, fundamental
    )

    rec, rec_strength = analyzer._generate_recommendation_details(
        overall_score, technical, fundamental, sentiment
    )

    sector_outlook = analyzer._assess_sector_outlook(fundamental)
    competitive_position = analyzer._assess_competitive_position(fundamental)

    position_sizing = analyzer._calculate_position_sizing(
        symbol_u, overall_score, risk_level, fundamental
    )

    optimal_entry, take_profit_levels, stop_loss_levels = analyzer._calculate_trading_levels(
        price, technical, rec
    )

    explainable_factors = analyzer._calculate_explainable_factors(
        tech_score, fund_score, sent_score, mom_score, val_score, qual_score
    )

    scenarios = analyzer._generate_scenario_analysis(price, technical, fundamental)

    expected_return, expected_volatility = analyzer._calculate_expected_metrics(
        technical, fundamental
    )
    sharpe_ratio = float(
        (expected_return - 0.02) / expected_volatility if expected_volatility > 0 else 0.0
    )
    sortino_ratio = analyzer._calculate_sortino_ratio(
        expected_return, expected_volatility
    )

    confidence = analyzer._determine_confidence(
        technical.data_quality, fundamental.data_quality
    )
    max_dd = analyzer._estimate_max_drawdown(technical)
    var_95 = analyzer._calculate_var(technical, fundamental)
    trailing_stop = analyzer._calculate_trailing_stop(technical)

    out = analyzer._generate_outlooks(overall_score, technical, fundamental)

    strengths = analyzer._identify_strengths_enhanced(technical, fundamental)
    weaknesses = analyzer._identify_weaknesses(technical, fundamental)
    opportunities = analyzer._identify_opportunities(technical, fundamental, sentiment)
    threats = analyzer._identify_threats(technical, fundamental, sentiment)

    duration = time.time() - start_time

    return AIRecommendation(
        symbol=symbol_u,
        overall_score=overall_score,
        technical_score=tech_score,
        fundamental_score=fund_score,
        sentiment_score=sent_score,
        risk_adjusted_score=risk_adjusted_score,
        momentum_score=mom_score,
        value_score=val_score,
        quality_score=qual_score,
        recommendation=rec,
        recommendation_strength=rec_strength,
        confidence_level=confidence,
        expected_return=expected_return,
        expected_volatility=expected_volatility,
        sharpe_ratio=sharpe_ratio,
        sortino_ratio=sortino_ratio,
        short_term_outlook=out["short_term"],
        medium_term_outlook=out["medium_term"],
        long_term_outlook=out["long_term"],
        swing_trade_outlook=out["swing_trade"],
        risk_level=risk_level,
        risk_factors=risk_factors,
        max_drawdown_estimate=max_dd,
        value_at_risk=var_95,
        stop_loss_levels=stop_loss_levels,
        position_sizing=position_sizing,
        optimal_entry=optimal_entry,
        take_profit_levels=take_profit_levels,
        trailing_stop_percent=trailing_stop,
        key_strengths=strengths,
        key_weaknesses=weaknesses,
        opportunities=opportunities,
        threats=threats,
        market_regime=technical.market_regime,
        sector_outlook=sector_outlook,
        competitive_position=competitive_position,
        timestamp=datetime.utcnow().isoformat() + "Z",
        data_sources_used=4,
        analysis_duration=duration,
        model_version=analyzer.model_version,
        explainable_factors=explainable_factors,
        scenario_analysis=scenarios,
    )


async def get_multi_source_analysis(symbol: str) -> Dict[str, Any]:
    if analyzer is None:
        raise RuntimeError("Analyzer not initialized")
    return await analyzer.get_multi_source_analysis(symbol)


# Self-test function
async def _test_enhanced_analyzer() -> None:
    """Test the enhanced analyzer."""
    logging.basicConfig(level=logging.INFO)

    print("=" * 60)
    print("Testing Enhanced Trading Analyzer v4.0.0")
    print("=" * 60)

    async with get_analyzer() as test_analyzer:
        test_symbol = "AAPL"

        print(f"\n1. Testing multi-source analysis for {test_symbol}...")
        start = time.time()
        analysis = await test_analyzer.get_multi_source_analysis(test_symbol)
        duration = time.time() - start

        if "error" not in analysis:
            print(f"   ✓ Analysis completed in {duration:.2f}s")
            consolidated = analysis.get("consolidated_analysis", {})
            print(
                f"   - Consensus Price: {consolidated.get('consensus_price', 'N/A')}"
            )
            print(
                f"   - Data Quality: {consolidated.get('data_quality_score', 0):.2f}"
            )
            print(f"   - Risk Score: {consolidated.get('risk_score', 0):.2f}")
            print(
                f"   - Opportunity Score: {consolidated.get('opportunity_score', 0):.2f}"
            )
        else:
            print(f"   ✗ Analysis failed: {analysis.get('error')}")

        print(f"\n2. Testing AI recommendation for {test_symbol}...")
        start = time.time()
        recommendation = await generate_ai_recommendation(test_symbol)
        duration = time.time() - start

        print(f"   ✓ Recommendation generated in {duration:.2f}s")
        print(f"   - Recommendation: {recommendation.recommendation.value}")
        print(f"   - Overall Score: {recommendation.overall_score:.1f}")
        print(f"   - Risk Level: {recommendation.risk_level}")
        print(f"   - Confidence: {recommendation.confidence_level}")
        print(
            f"   - Position Size: {recommendation.position_sizing.recommended_allocation:.1f}%"
        )

        print(f"\n3. Testing cache info...")
        cache_info = await test_analyzer.get_cache_info()
        print(f"   ✓ Cache info retrieved")
        print(
            f"   - Memory entries: {cache_info.get('memory_cache', {}).get('entries', 0)}"
        )
        print(
            f"   - File cache size: {cache_info.get('file_cache', {}).get('total_size_mb', 0):.2f} MB"
        )

        print(f"\n4. Testing statistics...")
        stats = test_analyzer.get_statistics()
        print(f"   ✓ Statistics retrieved")
        print(
            f"   - Total requests: {stats.get('requests', {}).get('total', 0)}"
        )
        print(
            f"   - Error rate: {stats.get('requests', {}).get('error_rate_percent', 0):.1f}%"
        )
        print(
            f"   - Cache efficiency: {stats.get('cache', {}).get('efficiency_percent', 0):.1f}%"
        )

        print("\n" + "=" * 60)
        print("Enhanced analyzer test completed successfully!")
        print("=" * 60)


if __name__ == "__main__":
    asyncio.run(_test_enhanced_analyzer())
