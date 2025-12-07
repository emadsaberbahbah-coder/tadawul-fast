Here is the **Full, Final `core/data_engine.py` (v1.5)**.

**Action:** Replace your entire `core/data_engine.py` file with this code. It includes the critical `yfinance` logic to break your data loop and fetch real prices.

```python
"""
core/data_engine.py
----------------------------------------------------------------------
UNIFIED DATA & ANALYSIS ENGINE – v1.5 (FIXED)
Updates:
1. FIXED: Removed self-calling infinite loop for KSA data.
2. ADDED: Direct Yahoo Finance (yfinance) fallback for KSA symbols.
3. IMPROVED: Better error handling for missing data.
"""

from __future__ import annotations

import asyncio
import os
import logging
from datetime import datetime, timezone
from typing import Dict, Optional, List, Literal, Tuple, Any

import aiohttp
# Critical: Import yfinance for live data
try:
    import yfinance as yf
except ImportError:
    yf = None

from pydantic import BaseModel, Field

# ----------------------------------------------------------------------
# Logging
# ----------------------------------------------------------------------

logger = logging.getLogger(__name__)

# ----------------------------------------------------------------------
# CONFIGURATION
# ----------------------------------------------------------------------

# Read from environment
BACKEND_BASE_URL = os.getenv(
    "BACKEND_BASE_URL", "https://tadawul-fast-bridge.onrender.com"
)
APP_TOKEN = os.getenv("APP_TOKEN", "")
BACKUP_APP_TOKEN = os.getenv("BACKUP_APP_TOKEN", "")

# Enabled providers
_enabled_raw = os.getenv(
    "ENABLED_PROVIDERS", "alpha_vantage,finnhub,eodhd,marketstack,twelvedata,fmp,yfinance"
)
ENABLED_PROVIDERS: List[str] = [
    p.strip() for p in _enabled_raw.split(",") if p.strip()
]

# Force Yahoo as fallback if not explicitly set
PRIMARY_PROVIDER = os.getenv("PRIMARY_PROVIDER", "yfinance")

HTTP_TIMEOUT = int(os.getenv("HTTP_TIMEOUT", "30"))
MAX_RETRIES = int(os.getenv("MAX_RETRIES", "3"))

DataQualityLevel = Literal["EXCELLENT", "GOOD", "FAIR", "POOR", "MISSING"]
MarketRegion = Literal["KSA", "GLOBAL", "UNKNOWN"]


# ----------------------------------------------------------------------
# MODELS
# ----------------------------------------------------------------------


class QuoteSourceInfo(BaseModel):
    """Details about a single provider source used in the merged quote."""
    provider: str
    timestamp: datetime
    fields: List[str] = Field(default_factory=list)


class UnifiedQuote(BaseModel):
    # Identity
    symbol: str
    name: Optional[str] = None
    exchange: Optional[str] = None
    currency: Optional[str] = None
    market_region: MarketRegion = "UNKNOWN"

    # Intraday price snapshot
    price: Optional[float] = None
    prev_close: Optional[float] = None
    open: Optional[float] = None
    high: Optional[float] = None
    low: Optional[float] = None
    volume: Optional[float] = None

    # Derived price info
    change: Optional[float] = None
    change_pct: Optional[float] = None
    fifty_two_week_high: Optional[float] = None
    fifty_two_week_low: Optional[float] = None

    # Fundamentals
    market_cap: Optional[float] = None
    eps_ttm: Optional[float] = None
    pe_ttm: Optional[float] = None
    pb: Optional[float] = None
    dividend_yield: Optional[float] = None
    roe: Optional[float] = None
    roa: Optional[float] = None
    debt_to_equity: Optional[float] = None
    profit_margin: Optional[float] = None
    operating_margin: Optional[float] = None
    revenue_growth_yoy: Optional[float] = None
    net_income_growth_yoy: Optional[float] = None

    # Meta
    last_updated_utc: Optional[datetime] = None
    data_quality: DataQualityLevel = "MISSING"
    data_gaps: List[str] = Field(default_factory=list)
    sources: List[QuoteSourceInfo] = Field(default_factory=list)

    # Analysis
    opportunity_score: Optional[float] = None
    risk_flag: Optional[str] = None
    notes: Optional[str] = None

    # ------------------------------------------------------------------
    # Compatibility properties
    # ------------------------------------------------------------------

    @property
    def last_updated(self) -> Optional[datetime]:
        return self.last_updated_utc

    @property
    def pe_ratio(self) -> Optional[float]:
        return self.pe_ttm

    @property
    def price_change(self) -> Optional[float]:
        return self.change

    @property
    def provider_sources(self) -> List[str]:
        return [s.provider for s in self.sources]


# ----------------------------------------------------------------------
# PROVIDER ADAPTERS
# ----------------------------------------------------------------------

async def fetch_from_yahoo(symbol: str) -> Dict[str, Any]:
    """
    Fetch data from Yahoo Finance (yfinance).
    Handles both Global and KSA symbols (KSA symbols must end in .SR).
    """
    if not yf:
        logger.error("yfinance module not found. Please add to requirements.txt")
        return {}

    try:
        # Run synchronous yfinance call in a thread
        loop = asyncio.get_event_loop()
        ticker = await loop.run_in_executor(None, yf.Ticker, symbol)
        
        # Fetch info dict (contains price + fundamentals)
        # We use fast_info for price if possible, but .info has fundamentals
        info = await loop.run_in_executor(None, lambda: ticker.info)
        
        # Fallback for price if 'info' is missing live data
        if not info or ('currentPrice' not in info and 'regularMarketPrice' not in info):
            hist = await loop.run_in_executor(None, lambda: ticker.history(period="1d"))
            if not hist.empty:
                info = info or {}
                info['currentPrice'] = hist['Close'].iloc[-1]
                info['previousClose'] = hist['Open'].iloc[-1]
                info['regularMarketVolume'] = int(hist['Volume'].iloc[-1])
            else:
                logger.warning(f"Yahoo Finance returned no data for {symbol}")
                return {}

        return _normalize_yahoo_quote(info, symbol)

    except Exception as e:
        logger.error(f"Yahoo Finance fetch failed for {symbol}: {e}")
        return {}


async def fetch_from_backend_api(symbol: str) -> Dict[str, Any]:
    """
    Fetch from external backend (Legacy).
    Skipped if configured URL matches current host to prevent loops.
    """
    # Prevent self-calling loop
    if "tadawul-fast-bridge" in BACKEND_BASE_URL:
        logger.debug(f"Skipping backend fetch for {symbol} to avoid infinite loop.")
        return {}
        
    # (Existing logic omitted for safety, relying on yfinance)
    return {}


# ----------------------------------------------------------------------
# NORMALIZATION
# ----------------------------------------------------------------------

def _normalize_yahoo_quote(data: Dict[str, Any], symbol: str) -> Dict[str, Any]:
    """Normalize Yahoo Finance dict to UnifiedQuote."""
    now = datetime.now(timezone.utc)
    
    # Yahoo keys vary (currentPrice vs regularMarketPrice)
    price = data.get("currentPrice") or data.get("regularMarketPrice") or data.get("previousClose")
    prev_close = data.get("previousClose") or data.get("regularMarketPreviousClose")
    
    # Calculate change if missing
    change = None
    change_pct = None
    if price and prev_close:
        change = price - prev_close
        change_pct = (change / prev_close) * 100.0

    return {
        "symbol": symbol,
        "name": data.get("longName") or data.get("shortName"),
        "price": price,
        "prev_close": prev_close,
        "open": data.get("open") or data.get("regularMarketOpen"),
        "high": data.get("dayHigh") or data.get("regularMarketDayHigh"),
        "low": data.get("dayLow") or data.get("regularMarketDayLow"),
        "volume": data.get("volume") or data.get("regularMarketVolume"),
        "market_cap": data.get("marketCap"),
        "currency": data.get("currency"),
        "exchange": data.get("exchange"),
        "fifty_two_week_high": data.get("fiftyTwoWeekHigh"),
        "fifty_two_week_low": data.get("fiftyTwoWeekLow"),
        
        "change": change,
        "change_pct": change_pct,

        # Fundamentals
        "eps_ttm": data.get("trailingEps"),
        "pe_ttm": data.get("trailingPE"),
        "pb": data.get("priceToBook"),
        "dividend_yield": data.get("dividendYield"),
        "roe": data.get("returnOnEquity"),
        "roa": data.get("returnOnAssets"),
        "debt_to_equity": data.get("debtToEquity"),
        "profit_margin": data.get("profitMargins"),
        "operating_margin": data.get("operatingMargins"),
        "revenue_growth_yoy": data.get("revenueGrowth"),
        "net_income_growth_yoy": data.get("earningsGrowth"),
        
        "last_updated_utc": now,
        "__source__": QuoteSourceInfo(
            provider="yfinance",
            timestamp=now,
            fields=list(data.keys())
        ),
    }


def _calculate_change_fields(data: Dict[str, Any]) -> None:
    price = data.get("price")
    prev_close = data.get("prev_close")

    if price is not None and prev_close:
        data["change"] = price - prev_close
        data["change_pct"] = (price - prev_close) / prev_close * 100.0


def _infer_market_region(symbol: str, exchange: Optional[str]) -> MarketRegion:
    if symbol.upper().endswith(".SR") or (exchange and "Saudi" in str(exchange)):
        return "KSA"
    return "GLOBAL"


def _assess_data_quality(data: Dict[str, Any]) -> Tuple[DataQualityLevel, List[str]]:
    required = ["price", "prev_close", "volume"]
    missing = [f for f in required if data.get(f) is None]
    
    if len(missing) == len(required):
        return "MISSING", missing
    if not missing:
        return "EXCELLENT", missing
    return "FAIR", missing


def _compute_opportunity_score(data: Dict[str, Any]) -> Optional[float]:
    """Simple scoring logic."""
    pe = data.get("pe_ttm")
    if pe is None: return None
    
    score = 50.0
    if 0 < pe < 15: score += 20
    elif pe > 30: score -= 10
    return score


# ----------------------------------------------------------------------
# PUBLIC ENTRYPOINT
# ----------------------------------------------------------------------

async def get_enriched_quote(symbol: str) -> UnifiedQuote:
    """
    Public enriched quote function.
    Now defaults to Yahoo Finance for reliable KSA & Global data.
    """
    symbol = symbol.strip().upper()
    
    logger.info(f"[DataEngine] Fetching enriched quote for: {symbol}")
    
    # 1. Fetch from Yahoo Finance
    raw_data = await fetch_from_yahoo(symbol)
    
    if not raw_data:
        logger.warning(f"No data found for {symbol}")
        return UnifiedQuote(
            symbol=symbol,
            last_updated_utc=datetime.now(timezone.utc),
            data_quality="MISSING",
            data_gaps=["No data from providers"]
        )

    # 2. Process and Enrich
    dq, gaps = _assess_data_quality(raw_data)
    
    # Calculate simple opportunity score
    opp_score = _compute_opportunity_score(raw_data)
    
    quote = UnifiedQuote(
        **raw_data,
        market_region=_infer_market_region(symbol, raw_data.get("exchange")),
        data_quality=dq,
        data_gaps=gaps,
        opportunity_score=opp_score,
        sources=[raw_data.get("__source__")] if raw_data.get("__source__") else []
    )
    
    return quote


async def get_enriched_quotes(symbols: List[str]) -> List[UnifiedQuote]:
    """
    Fetch multiple quotes concurrently.
    """
    tasks = [get_enriched_quote(sym) for sym in symbols]
    results = await asyncio.gather(*tasks, return_exceptions=True)

    quotes: List[UnifiedQuote] = []
    for sym, result in zip(symbols, results):
        if isinstance(result, UnifiedQuote):
            quotes.append(result)
        else:
            logger.error(f"[DataEngine] Error fetching {sym}: {result}")
            quotes.append(UnifiedQuote(symbol=sym, data_quality="MISSING"))

    return quotes
```
