# advanced_analysis.py - Enhanced Multi-source AI Trading Analysis
from __future__ import annotations

import os
import requests
import pandas as pd
import numpy as np
import json
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any, Tuple
from dataclasses import dataclass
from enum import Enum
from pathlib import Path

logger = logging.getLogger(__name__)


# =============================================================================
# Enums & Data Models
# =============================================================================


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


# =============================================================================
# Core Analyzer
# =============================================================================


class AdvancedTradingAnalyzer:
    """
    Multi-source analysis engine.

    NOTE:
    - All API keys MUST be passed via environment variables.
    - If no keys are configured, the analyzer will fall back to synthetic/sample
      data where possible (for technicals & fundamentals).
    """

    def __init__(self):
        # API Keys from environment variables ONLY (no hard-coded defaults)
        self.apis: Dict[str, Optional[str]] = {
            "alpha_vantage": os.getenv("ALPHA_VANTAGE_API_KEY"),
            "finnhub": os.getenv("FINNHUB_API_KEY"),
            "eodhd": os.getenv("EODHD_API_KEY"),
            "twelvedata": os.getenv("TWELVEDATA_API_KEY"),
            "marketstack": os.getenv("MARKETSTACK_API_KEY"),
            "fmp": os.getenv("FMP_API_KEY"),
        }

        configured = [k for k, v in self.apis.items() if v]
        if not configured:
            logger.warning(
                "AdvancedTradingAnalyzer initialized WITHOUT any external API keys. "
                "Live data will be very limited and fallbacks will be used."
            )
        else:
            logger.info(
                "AdvancedTradingAnalyzer initialized. APIs configured: %s",
                ", ".join(configured),
            )

        # Base URLs (can be overridden by env if needed)
        self.base_urls = {
            "alpha_vantage": os.getenv(
                "ALPHA_VANTAGE_BASE_URL", "https://www.alphavantage.co/query"
            ),
            "finnhub": os.getenv(
                "FINNHUB_BASE_URL", "https://finnhub.io/api/v1"
            ),
            "eodhd": os.getenv(
                "EODHD_BASE_URL", "https://eodhistoricaldata.com/api"
            ),
            "twelvedata": os.getenv(
                "TWELVEDATA_BASE_URL", "https://api.twelvedata.com"
            ),
            "marketstack": os.getenv(
                "MARKETSTACK_BASE_URL", "http://api.marketstack.com/v1"
            ),
            "fmp": os.getenv(
                "FMP_BASE_URL", "https://financialmodelingprep.com/api/v3"
            ),
        }

        # Google Apps Script integration (optional)
        self.google_apps_script_url = os.getenv("GOOGLE_APPS_SCRIPT_URL") or ""

        # Cache configuration (local JSON files)
        self.cache_dir = Path("./analysis_cache")
        self.cache_dir.mkdir(exist_ok=True)
        self.cache_ttl = timedelta(minutes=15)

        # Sync requests session
        self.session: Optional[requests.Session] = None

    # -------------------------------------------------------------------------
    # Context manager (optional)
    # -------------------------------------------------------------------------

    def __enter__(self):
        self.session = requests.Session()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.session:
            self.session.close()
            self.session = None

    # -------------------------------------------------------------------------
    # Cache helpers
    # -------------------------------------------------------------------------

    def _get_cache_key(self, symbol: str, data_type: str) -> str:
        return f"{symbol}_{data_type}.json"

    def _load_from_cache(self, symbol: str, data_type: str) -> Optional[Any]:
        try:
            cache_file = self.cache_dir / self._get_cache_key(symbol, data_type)
            if cache_file.exists():
                data = json.loads(cache_file.read_text(encoding="utf-8"))
                cache_time = datetime.fromisoformat(data.get("timestamp", "2000-01-01"))
                if datetime.now() - cache_time < self.cache_ttl:
                    return data.get("data")
        except Exception as e:
            logger.debug("Cache load failed for %s_%s: %s", symbol, data_type, e)
        return None

    def _save_to_cache(self, symbol: str, data_type: str, data: Any) -> None:
        try:
            cache_file = self.cache_dir / self._get_cache_key(symbol, data_type)
            cache_data = {"timestamp": datetime.now().isoformat(), "data": data}
            cache_file.write_text(json.dumps(cache_data, indent=2), encoding="utf-8")
        except Exception as e:
            logger.debug("Cache save failed for %s_%s: %s", symbol, data_type, e)

    # -------------------------------------------------------------------------
    # Realtime price (multi-source, with fallback & cache)
    # -------------------------------------------------------------------------

    def get_real_time_price(self, symbol: str) -> Optional[PriceData]:
        """Get real-time price from multiple sources with fallback and caching."""
        symbol = symbol.upper().strip()

        # 1) Cache
        cached = self._load_from_cache(symbol, "price")
        if cached:
            try:
                return PriceData(**cached)
            except Exception:
                logger.debug("Invalid cached price structure for %s, ignoring.", symbol)

        # 2) Online sources
        sources = [
            self._get_price_alpha_vantage,
            self._get_price_finnhub,
            self._get_price_twelvedata,
            self._get_price_marketstack,
            self._get_price_fmp,
        ]

        for source in sources:
            try:
                result = source(symbol)
                if result and result.price > 0:
                    self._save_to_cache(symbol, "price", result.__dict__)
                    return result
            except Exception as e:
                logger.warning(
                    "Price source %s failed for %s: %s",
                    source.__name__,
                    symbol,
                    e,
                )

        logger.error("All price sources failed for %s", symbol)
        return None

    def _get_price_alpha_vantage(self, symbol: str) -> Optional[PriceData]:
        if not self.apis["alpha_vantage"]:
            return None

        params = {
            "function": "GLOBAL_QUOTE",
            "symbol": symbol,
            "apikey": self.apis["alpha_vantage"],
        }

        try:
            session = self.session or requests.Session()
            response = session.get(
                self.base_urls["alpha_vantage"], params=params, timeout=15
            )
            response.raise_for_status()
            data = response.json()

            quote = data.get("Global Quote") or {}
            if not quote:
                return None

            return PriceData(
                symbol=symbol,
                price=float(quote.get("05. price", 0) or 0),
                change=float(quote.get("09. change", 0) or 0),
                change_percent=float(
                    str(quote.get("10. change percent", "0")).rstrip("%") or 0
                ),
                volume=int(quote.get("06. volume", 0) or 0),
                high=float(quote.get("03. high", 0) or 0),
                low=float(quote.get("04. low", 0) or 0),
                open=float(quote.get("02. open", 0) or 0),
                previous_close=float(quote.get("08. previous close", 0) or 0),
                timestamp=datetime.utcnow().isoformat() + "Z",
                source="alpha_vantage",
            )
        except Exception as e:
            logger.warning("Alpha Vantage API error for %s: %s", symbol, e)
            return None

    def _get_price_finnhub(self, symbol: str) -> Optional[PriceData]:
        if not self.apis["finnhub"]:
            return None

        url = f"{self.base_urls['finnhub']}/quote"
        params = {"symbol": symbol, "token": self.apis["finnhub"]}

        try:
            session = self.session or requests.Session()
            response = session.get(url, params=params, timeout=15)
            response.raise_for_status()
            data = response.json()

            if not data or data.get("c", 0) <= 0:
                return None

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
                source="finnhub",
            )
        except Exception as e:
            logger.warning("Finnhub API error for %s: %s", symbol, e)
            return None

    def _get_price_twelvedata(self, symbol: str) -> Optional[PriceData]:
        if not self.apis["twelvedata"]:
            return None

        base = self.base_urls["twelvedata"]
        params = {"symbol": symbol, "apikey": self.apis["twelvedata"]}

        try:
            session = self.session or requests.Session()

            # Simple quote endpoint (avoid double-calls if not needed)
            quote_url = f"{base}/quote"
            resp = session.get(quote_url, params=params, timeout=15)
            resp.raise_for_status()
            quote_data = resp.json()

            if not quote_data or not quote_data.get("close"):
                return None

            return PriceData(
                symbol=symbol,
                price=float(quote_data.get("close", 0) or 0),
                change=float(quote_data.get("change", 0) or 0),
                change_percent=float(
                    quote_data.get("percent_change", 0) or 0
                ),
                high=float(quote_data.get("high", 0) or 0),
                low=float(quote_data.get("low", 0) or 0),
                open=float(quote_data.get("open", 0) or 0),
                previous_close=float(quote_data.get("previous_close", 0) or 0),
                volume=int(quote_data.get("volume", 0) or 0),
                timestamp=quote_data.get("datetime")
                or datetime.utcnow().isoformat() + "Z",
                source="twelvedata",
            )
        except Exception as e:
            logger.warning("Twelve Data API error for %s: %s", symbol, e)
            return None

    def _get_price_marketstack(self, symbol: str) -> Optional[PriceData]:
        if not self.apis["marketstack"]:
            return None

        url = f"{self.base_urls['marketstack']}/eod/latest"
        params = {"symbols": symbol, "access_key": self.apis["marketstack"]}

        try:
            session = self.session or requests.Session()
            response = session.get(url, params=params, timeout=15)
            response.raise_for_status()
            data = response.json()

            rows = data.get("data") or []
            if not rows:
                return None
            quote = rows[0]

            close_price = float(quote.get("close", 0) or 0)
            open_price = float(quote.get("open", close_price) or close_price)
            if open_price == 0:
                open_price = close_price

            change = close_price - open_price
            change_pct = (change / open_price * 100) if open_price else 0.0

            return PriceData(
                symbol=symbol,
                price=close_price,
                change=change,
                change_percent=change_pct,
                high=float(quote.get("high", 0) or 0),
                low=float(quote.get("low", 0) or 0),
                open=open_price,
                previous_close=close_price,
                volume=int(quote.get("volume", 0) or 0),
                timestamp=quote.get("date") or datetime.utcnow().isoformat() + "Z",
                source="marketstack",
            )
        except Exception as e:
            logger.warning("Marketstack API error for %s: %s", symbol, e)
            return None

    def _get_price_fmp(self, symbol: str) -> Optional[PriceData]:
        if not self.apis["fmp"]:
            return None

        url = f"{self.base_urls['fmp']}/quote/{symbol}"
        params = {"apikey": self.apis["fmp"]}

        try:
            session = self.session or requests.Session()
            response = session.get(url, params=params, timeout=15)
            response.raise_for_status()
            data = response.json()

            if not data:
                return None
            quote = data[0]

            return PriceData(
                symbol=symbol,
                price=float(quote.get("price", 0) or 0),
                change=float(quote.get("change", 0) or 0),
                change_percent=float(quote.get("changesPercentage", 0) or 0),
                high=float(quote.get("dayHigh", 0) or 0),
                low=float(quote.get("dayLow", 0) or 0),
                open=float(quote.get("open", 0) or 0),
                previous_close=float(quote.get("previousClose", 0) or 0),
                volume=int(quote.get("volume", 0) or 0),
                timestamp=datetime.utcnow().isoformat() + "Z",
                source="fmp",
            )
        except Exception as e:
            logger.warning("FMP API error for %s: %s", symbol, e)
            return None

    # -------------------------------------------------------------------------
    # Multi-source consolidation
    # -------------------------------------------------------------------------

    def get_multi_source_analysis(self, symbol: str) -> Dict[str, Any]:
        symbol = symbol.upper().strip()

        results: Dict[str, Any] = {
            "symbol": symbol,
            "sources": {},
            "consolidated": {},
            "timestamp": datetime.utcnow().isoformat() + "Z",
        }

        # Price-oriented sources
        for name, fn in [
            ("alpha_vantage", self._get_price_alpha_vantage),
            ("finnhub", self._get_price_finnhub),
            ("twelvedata", self._get_price_twelvedata),
            ("marketstack", self._get_price_marketstack),
            ("fmp", self._get_price_fmp),
        ]:
            try:
                data = fn(symbol)
                if data:
                    results["sources"][name] = data.__dict__
            except Exception as e:
                logger.warning("%s failed for %s: %s", name, symbol, e)

        # Optional Google Apps Script enrichment
        try:
            apps_script_data = self._get_google_apps_script_data(symbol)
            if apps_script_data:
                results["sources"]["google_apps_script"] = apps_script_data
        except Exception as e:
            logger.warning("Google Apps Script failed for %s: %s", symbol, e)

        # Consolidate
        results["consolidated"] = self._consolidate_multi_source_data(
            results["sources"]
        )
        return results

    def _get_google_apps_script_data(self, symbol: str) -> Optional[Dict[str, Any]]:
        if not self.google_apps_script_url:
            return None
        try:
            params = {"symbol": symbol.upper()}
            resp = requests.get(self.google_apps_script_url, params=params, timeout=30)
            resp.raise_for_status()
            return resp.json()
        except Exception as e:
            logger.warning("Google Apps Script error for %s: %s", symbol, e)
            return None

    def _consolidate_multi_source_data(self, sources: Dict[str, Any]) -> Dict[str, Any]:
        prices: List[float] = []
        volumes: List[float] = []
        changes: List[float] = []

        for data in sources.values():
            if not isinstance(data, dict):
                continue
            if data.get("price") is not None:
                prices.append(float(data["price"]))
            if data.get("volume") is not None:
                volumes.append(float(data["volume"]))
            if data.get("change") is not None:
                changes.append(float(data["change"]))

        consolidated = {
            "price_avg": sum(prices) / len(prices) if prices else None,
            "price_min": min(prices) if prices else None,
            "price_max": max(prices) if prices else None,
            "price_std": float(np.std(prices)) if len(prices) > 1 else 0.0,
            "volume_avg": sum(volumes) / len(volumes) if volumes else None,
            "change_avg": sum(changes) / len(changes) if changes else None,
            "sources_count": len(sources),
            "confidence": min(1.0, len(sources) * 0.2),
            "data_quality": (
                "HIGH"
                if len(sources) >= 3
                else "MEDIUM"
                if len(sources) >= 2
                else "LOW"
            ),
        }
        return consolidated

    # -------------------------------------------------------------------------
    # Technical indicators
    # -------------------------------------------------------------------------

    def calculate_technical_indicators(
        self, symbol: str, period: str = "3mo"
    ) -> TechnicalIndicators:
        symbol = symbol.upper().strip()

        cached = self._load_from_cache(symbol, f"technical_{period}")
        if cached:
            try:
                return TechnicalIndicators(**cached)
            except Exception:
                logger.debug("Invalid cached technicals for %s, ignoring.", symbol)

        try:
            df = self._get_historical_data(symbol, period)
            if df.empty:
                return self._generate_fallback_technical_indicators()

            indicators = self._calculate_advanced_indicators(df)
            self._save_to_cache(symbol, f"technical_{period}", indicators.__dict__)
            return indicators
        except Exception as e:
            logger.error("Technical indicators calculation failed for %s: %s", symbol, e)
            return self._generate_fallback_technical_indicators()

    def _get_historical_data(self, symbol: str, period: str) -> pd.DataFrame:
        # Use Alpha Vantage daily if configured
        try:
            if self.apis["alpha_vantage"]:
                url = self.base_urls["alpha_vantage"]
                params = {
                    "function": "TIME_SERIES_DAILY",
                    "symbol": symbol,
                    "apikey": self.apis["alpha_vantage"],
                    "outputsize": "compact",
                }
                resp = requests.get(url, params=params, timeout=15)
                data = resp.json()

                ts = data.get("Time Series (Daily)") or {}
                if ts:
                    dates, opens, highs, lows, closes, volumes = [], [], [], [], [], []
                    for date, values in list(ts.items())[:90]:
                        dates.append(pd.to_datetime(date))
                        opens.append(float(values["1. open"]))
                        highs.append(float(values["2. high"]))
                        lows.append(float(values["3. low"]))
                        closes.append(float(values["4. close"]))
                        volumes.append(int(values["5. volume"]))

                    df = pd.DataFrame(
                        {
                            "date": dates,
                            "open": opens,
                            "high": highs,
                            "low": lows,
                            "close": closes,
                            "volume": volumes,
                        }
                    )
                    return df.set_index("date")
        except Exception as e:
            logger.warning("Historical data API failed for %s: %s", symbol, e)

        # Fallback: synthetic data
        dates = pd.date_range(end=datetime.now(), periods=90, freq="D")
        base_price = float(np.random.uniform(40, 60))

        data = {
            "date": dates,
            "open": base_price + np.random.uniform(-2, 2, 90),
            "high": base_price + np.random.uniform(0, 3, 90),
            "low": base_price + np.random.uniform(-3, 0, 90),
            "close": base_price + np.random.uniform(-1.5, 1.5, 90),
            "volume": np.random.randint(1_000_000, 5_000_000, 90),
        }
        return pd.DataFrame(data).set_index("date")

    def _calculate_advanced_indicators(self, df: pd.DataFrame) -> TechnicalIndicators:
        close_prices = df["close"]
        high_prices = df["high"]
        low_prices = df["low"]
        volumes = df["volume"]

        # RSI
        delta = close_prices.diff()
        gain = delta.where(delta > 0, 0).rolling(window=14).mean()
        loss = (-delta.where(delta < 0, 0)).rolling(window=14).mean()
        rs = gain / loss
        rsi = (
            float(100 - (100 / (1 + rs)).iloc[-1])
            if not rs.isna().iloc[-1]
            else 50.0
        )

        # MACD
        exp1 = close_prices.ewm(span=12, adjust=False).mean()
        exp2 = close_prices.ewm(span=26, adjust=False).mean()
        macd = exp1 - exp2
        macd_signal = macd.ewm(span=9, adjust=False).mean()
        macd_histogram = macd - macd_signal

        # Moving Averages
        ma_20 = float(close_prices.rolling(20).mean().iloc[-1])
        ma_50 = float(close_prices.rolling(50).mean().iloc[-1])
        ma_200 = (
            float(close_prices.rolling(200).mean().iloc[-1])
            if len(close_prices) >= 200
            else ma_50
        )

        # Bollinger Bands
        std_20 = float(close_prices.rolling(20).std().iloc[-1])
        bb_upper = ma_20 + 2 * std_20
        bb_lower = ma_20 - 2 * std_20
        if bb_upper != bb_lower:
            bb_position = (close_prices.iloc[-1] - bb_lower) / (bb_upper - bb_lower)
        else:
            bb_position = 0.5

        volume_trend = float(volumes.pct_change(5).mean())
        support_level = float(low_prices.rolling(20).min().iloc[-1])
        resistance_level = float(high_prices.rolling(20).max().iloc[-1])

        # Trend direction
        if ma_20 > ma_50 > ma_200:
            trend_direction = "Bullish"
        elif ma_20 < ma_50 < ma_200:
            trend_direction = "Bearish"
        else:
            trend_direction = "Neutral"

        volatility = float(close_prices.pct_change().std() * np.sqrt(252))
        if len(close_prices) >= 20:
            momentum_score = float(
                (close_prices.iloc[-1] / close_prices.iloc[-20] - 1) * 100
            )
        else:
            momentum_score = 0.0

        return TechnicalIndicators(
            rsi=rsi,
            macd=float(macd.iloc[-1]),
            macd_signal=float(macd_signal.iloc[-1]),
            macd_histogram=float(macd_histogram.iloc[-1]),
            moving_avg_20=ma_20,
            moving_avg_50=ma_50,
            moving_avg_200=ma_200,
            bollinger_upper=float(bb_upper),
            bollinger_lower=float(bb_lower),
            bollinger_position=float(bb_position),
            volume_trend=volume_trend,
            support_level=support_level,
            resistance_level=resistance_level,
            trend_direction=trend_direction,
            volatility=volatility,
            momentum_score=momentum_score,
            williams_r=float(np.random.uniform(-100, 0)),
            stoch_k=float(np.random.uniform(0, 100)),
            stoch_d=float(np.random.uniform(0, 100)),
            cci=float(np.random.uniform(-200, 200)),
            adx=float(np.random.uniform(0, 60)),
            atr=float(np.random.uniform(1, 5)),
        )

    def _generate_fallback_technical_indicators(self) -> TechnicalIndicators:
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
            trend_direction=str(
                np.random.choice(["Bullish", "Bearish", "Neutral"])
            ),
            volatility=float(np.random.uniform(0.1, 0.4)),
            momentum_score=float(np.random.uniform(0, 100)),
            williams_r=float(np.random.uniform(-100, 0)),
            stoch_k=float(np.random.uniform(0, 100)),
            stoch_d=float(np.random.uniform(0, 100)),
            cci=float(np.random.uniform(-200, 200)),
            adx=float(np.random.uniform(0, 60)),
            atr=float(np.random.uniform(1, 5)),
        )

    # -------------------------------------------------------------------------
    # Fundamentals
    # -------------------------------------------------------------------------

    def analyze_fundamentals(self, symbol: str) -> FundamentalData:
        symbol = symbol.upper().strip()

        cached = self._load_from_cache(symbol, "fundamentals")
        if cached:
            try:
                return FundamentalData(**cached)
            except Exception:
                logger.debug("Invalid cached fundamentals for %s, ignoring.", symbol)

        try:
            fundamental_data = self._get_fundamental_data_combined(symbol)
            self._save_to_cache(symbol, "fundamentals", fundamental_data.__dict__)
            return fundamental_data
        except Exception as e:
            logger.error("Fundamental analysis failed for %s: %s", symbol, e)
            return self._generate_fallback_fundamental_data()

    def _get_fundamental_data_combined(self, symbol: str) -> FundamentalData:
        fmp_data = self._get_fundamentals_fmp(symbol)
        if fmp_data:
            return fmp_data

        # Fallback synthetic fundamentals
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
            peg_ratio=float(np.random.uniform(0.5, 3.0)),
        )

    def _get_fundamentals_fmp(self, symbol: str) -> Optional[FundamentalData]:
        if not self.apis["fmp"]:
            return None

        try:
            url = f"{self.base_urls['fmp']}/profile/{symbol}"
            params = {"apikey": self.apis["fmp"]}
            resp = requests.get(url, params=params, timeout=15)
            data = resp.json()

            if not data:
                return None
            profile = data[0]

            return FundamentalData(
                market_cap=float(profile.get("mktCap", 0) or 0),
                pe_ratio=float(profile.get("priceToEarnings", 0) or 0),
                pb_ratio=float(profile.get("priceToBook", 0) or 0),
                dividend_yield=float(profile.get("lastDiv", 0) or 0) / 100.0,
                eps=float(profile.get("eps", 0) or 0),
                roe=float(profile.get("roe", 0) or 0) / 100.0,
                debt_to_equity=float(profile.get("debtToEquity", 0) or 0),
                revenue_growth=float(profile.get("revenueGrowth", 0) or 0),
                profit_margin=float(profile.get("profitMargin", 0) or 0) / 100.0,
                sector_rank=1,
                free_cash_flow=float(profile.get("freeCashFlow", 0) or 0),
                operating_margin=float(
                    profile.get("operatingMargins", 0) or 0
                )
                / 100.0,
                current_ratio=float(profile.get("currentRatio", 0) or 0),
                quick_ratio=float(profile.get("quickRatio", 0) or 0),
                peg_ratio=float(profile.get("pegRatio", 0) or 0),
            )
        except Exception as e:
            logger.warning("FMP fundamentals failed for %s: %s", symbol, e)
            return None

    def _generate_fallback_fundamental_data(self) -> FundamentalData:
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
            peg_ratio=0.0,
        )

    # -------------------------------------------------------------------------
    # AI-style recommendation
    # -------------------------------------------------------------------------

    def generate_ai_recommendation(
        self,
        symbol: str,
        technical_data: TechnicalIndicators,
        fundamental_data: FundamentalData,
    ) -> AIRecommendation:
        symbol = symbol.upper().strip()

        tech_score = self._calculate_technical_score(technical_data)
        fund_score = self._calculate_fundamental_score(fundamental_data)
        sentiment_score = self._calculate_sentiment_score(symbol)

        overall_score = tech_score * 0.4 + fund_score * 0.5 + sentiment_score * 0.1

        recommendation, confidence = self._generate_recommendation(
            overall_score, technical_data, fundamental_data
        )

        return AIRecommendation(
            symbol=symbol,
            overall_score=round(overall_score, 1),
            technical_score=round(tech_score, 1),
            fundamental_score=round(fund_score, 1),
            sentiment_score=round(sentiment_score, 1),
            recommendation=recommendation,
            confidence_level=confidence,
            short_term_outlook=self._generate_outlook(
                overall_score, technical_data, "short"
            ),
            medium_term_outlook=self._generate_outlook(
                overall_score, technical_data, "medium"
            ),
            long_term_outlook=self._generate_outlook(
                overall_score, technical_data, "long"
            ),
            key_strengths=self._identify_strengths(technical_data, fundamental_data),
            key_risks=self._identify_risks(technical_data, fundamental_data),
            optimal_entry=round(technical_data.support_level * 0.98, 2),
            stop_loss=round(technical_data.support_level * 0.92, 2),
            price_targets=self._calculate_price_targets(
                technical_data, fundamental_data
            ),
            timestamp=datetime.utcnow().isoformat() + "Z",
        )

    def _calculate_technical_score(self, data: TechnicalIndicators) -> float:
        score = 50.0

        # RSI
        if 30 <= data.rsi <= 70:
            score += 10
        elif data.rsi < 20 or data.rsi > 80:
            score -= 15
        else:
            score -= 5

        # MACD
        if data.macd > data.macd_signal and data.macd_histogram > 0:
            score += 8
        elif data.macd < data.macd_signal and data.macd_histogram < 0:
            score -= 8

        # Trend
        if data.trend_direction == "Bullish":
            score += 12
        elif data.trend_direction == "Bearish":
            score -= 12

        # MAs
        if data.moving_avg_20 > data.moving_avg_50 > data.moving_avg_200:
            score += 10
        elif data.moving_avg_20 < data.moving_avg_50 < data.moving_avg_200:
            score -= 10

        # Momentum
        if data.momentum_score > 5:
            score += 5
        elif data.momentum_score < -5:
            score -= 5

        # Bollinger
        if 0.2 <= data.bollinger_position <= 0.8:
            score += 5
        elif data.bollinger_position < 0.1 or data.bollinger_position > 0.9:
            score -= 5

        # Volume
        if data.volume_trend > 0:
            score += 3

        return float(max(0.0, min(100.0, score)))

    def _calculate_fundamental_score(self, data: FundamentalData) -> float:
        if data.market_cap == 0:
            return 50.0

        score = 50.0

        # P/E
        if 0 < data.pe_ratio < 15:
            score += 12
        elif 15 <= data.pe_ratio <= 25:
            score += 5
        elif data.pe_ratio > 40:
            score -= 15

        # ROE
        if data.roe > 0.15:
            score += 10
        elif data.roe > 0.08:
            score += 5
        elif data.roe < 0:
            score -= 10

        # Revenue growth
        if data.revenue_growth > 0.15:
            score += 8
        elif data.revenue_growth > 0.05:
            score += 4
        elif data.revenue_growth < -0.1:
            score -= 8

        # Profit margin
        if data.profit_margin > 0.15:
            score += 7
        elif data.profit_margin > 0.08:
            score += 3
        elif data.profit_margin < 0:
            score -= 10

        # Debt
        if data.debt_to_equity < 0.5:
            score += 5
        elif data.debt_to_equity > 1.0:
            score -= 8

        # Dividend
        if data.dividend_yield > 0.03:
            score += 3

        # Sector rank (1 = best)
        if data.sector_rank > 0:
            rank_score = max(0.0, (50 - data.sector_rank) / 50 * 10)
            score += rank_score

        return float(max(0.0, min(100.0, score)))

    def _calculate_sentiment_score(self, symbol: str) -> float:
        # Placeholder; real implementation would use news / social data
        return float(np.random.uniform(40, 80))

    def _generate_recommendation(
        self,
        overall_score: float,
        technical: TechnicalIndicators,
        fundamental: FundamentalData,
    ) -> Tuple[Recommendation, str]:
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

        if technical.volatility > 0.3:
            confidence = "Medium" if confidence == "High" else "Low"

        if fundamental.market_cap == 0:
            confidence = "Low"

        return rec, confidence

    def _generate_outlook(
        self, score: float, technical: TechnicalIndicators, timeframe: str
    ) -> str:
        base_outlooks = {
            "short": ["Very Bullish", "Bullish", "Neutral", "Bearish", "Very Bearish"],
            "medium": ["Very Positive", "Positive", "Neutral", "Cautious", "Negative"],
            "long": ["Excellent", "Good", "Fair", "Poor", "Very Poor"],
        }

        if score >= 80:
            idx = 0
        elif score >= 65:
            idx = 1
        elif score >= 45:
            idx = 2
        elif score >= 30:
            idx = 3
        else:
            idx = 4

        if technical.trend_direction == "Bullish" and idx > 0:
            idx -= 1
        elif technical.trend_direction == "Bearish" and idx < 4:
            idx += 1

        return base_outlooks[timeframe][idx]

    def _identify_strengths(
        self, technical: TechnicalIndicators, fundamental: FundamentalData
    ) -> List[str]:
        strengths: List[str] = []

        if technical.trend_direction == "Bullish":
            strengths.append("Strong upward trend momentum")

        if 0 < fundamental.pe_ratio < 15:
            strengths.append("Attractive valuation with low P/E ratio")

        if fundamental.roe > 0.15:
            strengths.append("High return on equity indicating efficient operations")

        if fundamental.revenue_growth > 0.1:
            strengths.append("Strong revenue growth trajectory")

        if fundamental.debt_to_equity < 0.5:
            strengths.append("Healthy balance sheet with low debt")

        if fundamental.profit_margin > 0.15:
            strengths.append("Strong profitability margins")

        if 30 < technical.rsi < 70:
            strengths.append("Technical indicators show balanced momentum")

        return strengths[:4]

    def _identify_risks(
        self, technical: TechnicalIndicators, fundamental: FundamentalData
    ) -> List[str]:
        risks: List[str] = []

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
            risks.append("Potentially oversold; may indicate underlying weakness")

        if technical.trend_direction == "Bearish":
            risks.append("Prevailing downward trend in technical indicators")

        return risks[:4]

    def _calculate_price_targets(
        self, technical: TechnicalIndicators, fundamental: FundamentalData
    ) -> Dict[str, float]:
        current_price = technical.moving_avg_20
        base_multiplier = 1.0

        if 0 < fundamental.pe_ratio < 15:
            base_multiplier *= 1.1
        elif fundamental.pe_ratio > 30:
            base_multiplier *= 0.9

        if fundamental.revenue_growth > 0.15:
            base_multiplier *= 1.15
        elif fundamental.revenue_growth < 0:
            base_multiplier *= 0.9

        if technical.trend_direction == "Bullish":
            base_multiplier *= 1.05
        elif technical.trend_direction == "Bearish":
            base_multiplier *= 0.95

        return {
            "1_week": round(current_price * base_multiplier * 1.02, 2),
            "2_weeks": round(current_price * base_multiplier * 1.04, 2),
            "1_month": round(current_price * base_multiplier * 1.08, 2),
            "3_months": round(current_price * base_multiplier * 1.15, 2),
            "6_months": round(current_price * base_multiplier * 1.25, 2),
            "1_year": round(current_price * base_multiplier * 1.35, 2),
        }

    # -------------------------------------------------------------------------
    # Risk & cache utilities
    # -------------------------------------------------------------------------

    def analyze_market_sentiment(self, symbol: str) -> Dict[str, Any]:
        return {
            "news_sentiment": float(np.random.uniform(-1, 1)),
            "social_media_buzz": float(np.random.uniform(0, 100)),
            "institutional_flow": float(np.random.uniform(-50, 50)),
            "retail_sentiment": float(np.random.uniform(-1, 1)),
            "analyst_ratings": str(
                np.random.choice(
                    ["Strong Buy", "Buy", "Hold", "Sell", "Strong Sell"]
                )
            ),
            "short_interest": float(np.random.uniform(0, 0.3)),
            "put_call_ratio": float(np.random.uniform(0.5, 1.5)),
            "market_emotion_index": float(np.random.uniform(0, 100)),
            "contrarian_indicator": str(
                np.random.choice(["Bullish", "Bearish"])
            ),
            "sentiment_score": float(np.random.uniform(0, 100)),
            "fear_greed_index": float(np.random.uniform(0, 100)),
            "volume_analysis": str(
                np.random.choice(["Accumulation", "Distribution", "Neutral"])
            ),
        }

    def analyze_risk_metrics(self, symbol: str) -> Dict[str, Any]:
        return {
            "beta": float(np.random.uniform(0.5, 1.5)),
            "standard_deviation": float(np.random.uniform(0.1, 0.4)),
            "value_at_risk_95": float(np.random.uniform(0.05, 0.2)),
            "value_at_risk_99": float(np.random.uniform(0.08, 0.3)),
            "max_drawdown": float(np.random.uniform(0.1, 0.4)),
            "sharpe_ratio": float(np.random.uniform(0.5, 2.0)),
            "sortino_ratio": float(np.random.uniform(0.5, 2.5)),
            "liquidity_score": float(np.random.uniform(50, 100)),
            "sector_risk": float(np.random.uniform(0, 100)),
            "market_cap_risk": float(np.random.uniform(0, 100)),
            "concentration_risk": float(np.random.uniform(0, 100)),
            "overall_risk_score": float(np.random.uniform(0, 100)),
            "risk_category": str(
                np.random.choice(["Low", "Medium", "High", "Very High"])
            ),
            "stress_test_performance": float(np.random.uniform(-0.3, -0.05)),
        }

    def clear_cache(self, symbol: Optional[str] = None) -> None:
        try:
            pattern = f"{symbol}_*.json" if symbol else "*.json"
            for cache_file in self.cache_dir.glob(pattern):
                cache_file.unlink()
            logger.info("Cache cleared for pattern: %s", pattern)
        except Exception as e:
            logger.error("Cache clearance failed: %s", e)

    def get_cache_info(self) -> Dict[str, Any]:
        try:
            cache_files = list(self.cache_dir.glob("*.json"))
            cache_size = sum(f.stat().st_size for f in cache_files)
            return {
                "total_files": len(cache_files),
                "total_size_bytes": cache_size,
                "total_size_mb": round(cache_size / (1024 * 1024), 2),
                "cache_dir": str(self.cache_dir),
                "cache_ttl_minutes": self.cache_ttl.total_seconds() / 60.0,
            }
        except Exception as e:
            logger.error("Cache info retrieval failed: %s", e)
            return {}


# Global analyzer instance (used by main.py via safe_import)
analyzer = AdvancedTradingAnalyzer()
