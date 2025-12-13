import asyncio
import logging
import re
import time
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Union

import httpx
from cachetools import TTLCache
from pydantic import BaseModel, Field, ConfigDict

# Try importing parsing libraries (graceful fallback)
try:
    from bs4 import BeautifulSoup
except ImportError:
    BeautifulSoup = None

try:
    import yfinance as yf
except ImportError:
    yf = None

# --- Internal Imports ---
from core.config import get_settings

# --- Configuration ---
settings = get_settings()
logger = logging.getLogger("data_engine_v2")

# --- Constants ---
ARGAAM_URL = "https://www.argaam.com/en/company/companies-volume/{market_id}"
USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"

# =============================================================================
# Data Models (Pydantic V2)
# =============================================================================

class UnifiedQuote(BaseModel):
    """
    Standardized Quote Model matching the Dashboard's 59-column requirement.
    """
    model_config = ConfigDict(populate_by_name=True, from_attributes=True)

    # Identity
    symbol: str
    name: Optional[str] = None
    market: str = "UNKNOWN"  # KSA, GLOBAL
    currency: str = "SAR"
    
    # Prices
    current_price: Optional[float] = None
    previous_close: Optional[float] = None
    price_change: Optional[float] = None
    percent_change: Optional[float] = None
    day_high: Optional[float] = None
    day_low: Optional[float] = None
    high_52w: Optional[float] = None
    low_52w: Optional[float] = None
    
    # Volume
    volume: Optional[float] = None
    avg_volume_30d: Optional[float] = None
    market_cap: Optional[float] = None
    
    # Fundamentals
    pe_ttm: Optional[float] = None
    eps_ttm: Optional[float] = None
    dividend_yield: Optional[float] = None
    pb: Optional[float] = None
    roe: Optional[float] = None
    net_margin: Optional[float] = None
    
    # Technicals / Scores
    rsi_14: Optional[float] = None
    macd: Optional[float] = None
    overall_score: Optional[float] = 50.0
    recommendation: Optional[str] = "HOLD"
    
    # Meta
    data_source: str = "none"
    data_quality: str = "MISSING"  # FULL, PARTIAL, MISSING
    last_updated_utc: str = Field(default_factory=lambda: datetime.now(timezone.utc).isoformat())
    error: Optional[str] = None

    def calculate_simple_scores(self):
        """Generates synthetic scores if data is missing, to populate dashboard gauges."""
        score = 50.0
        
        # 1. Momentum boost
        if self.percent_change and self.percent_change > 0:
            score += 5
        if self.current_price and self.high_52w and self.current_price > (self.high_52w * 0.9):
            score += 10
            
        # 2. Value boost
        if self.pe_ttm and 0 < self.pe_ttm < 15:
            score += 10
        elif self.pe_ttm and self.pe_ttm > 30:
            score -= 10
            
        self.overall_score = max(0.0, min(100.0, score))
        
        # Set Recommendation Label
        if self.overall_score >= 80: self.recommendation = "STRONG BUY"
        elif self.overall_score >= 65: self.recommendation = "BUY"
        elif self.overall_score <= 35: self.recommendation = "SELL"
        else: self.recommendation = "HOLD"
        
        return self

# =============================================================================
# Helper Functions
# =============================================================================

def _safe_float(val: Any) -> Optional[float]:
    if val is None: return None
    try:
        if isinstance(val, str):
            val = val.replace(",", "").replace("%", "").strip()
            if val == "-": return None
        return float(val)
    except Exception:
        return None

def normalize_symbol(ticker: str) -> str:
    """Standardizes input to Ticker.Suffix format."""
    t = str(ticker).strip().upper()
    if not t: return ""
    # Numeric (Saudi) -> 1120.SR
    if t.isdigit(): return f"{t}.SR"
    # Existing suffix
    if "." in t: return t
    # Default -> .US assumption for letters
    return f"{t}.US"

def is_ksa_symbol(ticker: str) -> bool:
    return "SR" in ticker.upper() or ticker.isdigit()

# =============================================================================
# Main Engine Class
# =============================================================================

class DataEngine:
    def __init__(self):
        # 1. Setup HTTP Client with constraints
        self.client = httpx.AsyncClient(
            timeout=settings.HTTP_TIMEOUT_SEC,
            headers={"User-Agent": USER_AGENT},
            follow_redirects=True,
            limits=httpx.Limits(max_keepalive_connections=10, max_connections=20)
        )
        
        # 2. Setup Caches (using cachetools)
        # Short cache for prices (30s), Long cache for fundamentals (6h)
        self.price_cache = TTLCache(maxsize=1000, ttl=30)
        self.fund_cache = TTLCache(maxsize=500, ttl=21600)

    async def aclose(self):
        await self.client.aclose()

    async def get_quote(self, ticker: str) -> UnifiedQuote:
        """
        Main Entry Point: Fetches the best available data for a ticker.
        """
        clean_ticker = normalize_symbol(ticker)
        
        # Check Cache
        if clean_ticker in self.price_cache:
            return self.price_cache[clean_ticker]

        # Route Logic
        if is_ksa_symbol(clean_ticker):
            quote = await self._fetch_ksa_quote(clean_ticker)
        else:
            quote = await self._fetch_global_quote(clean_ticker)

        # Finalize
        quote.calculate_simple_scores()
        self.price_cache[clean_ticker] = quote
        return quote

    async def get_quotes(self, tickers: List[str]) -> List[UnifiedQuote]:
        """Batch fetcher."""
        tasks = [self.get_quote(t) for t in tickers if t]
        return await asyncio.gather(*tasks)

    # -------------------------------------------------------------------------
    # KSA Strategy (Argaam -> EODHD Fallback)
    # -------------------------------------------------------------------------

    async def _fetch_ksa_quote(self, ticker: str) -> UnifiedQuote:
        numeric_code = ticker.split(".")[0]
        
        # 1. Try Argaam Scraper (No Cost, Fast)
        argaam_data = await self._scrape_argaam(numeric_code)
        if argaam_data:
            return UnifiedQuote(
                symbol=ticker,
                name=argaam_data.get("company"),
                market="KSA",
                currency="SAR",
                current_price=argaam_data.get("price"),
                price_change=argaam_data.get("change"),
                percent_change=argaam_data.get("change_p"),
                volume=argaam_data.get("volume"),
                avg_volume_30d=argaam_data.get("avg_volume_3m"),
                data_source="argaam",
                data_quality="PARTIAL"
            )

        # 2. Fallback: EODHD (If enabled and key exists)
        if settings.EODHD_API_KEY and settings.EODHD_FETCH_FUNDAMENTALS:
            return await self._fetch_eodhd_realtime(ticker, market="KSA", currency="SAR")
            
        # 3. Fallback: Yahoo Finance (Last resort)
        if settings.ENABLE_YFINANCE and yf:
            return await self._fetch_yfinance(ticker, market="KSA")

        return UnifiedQuote(symbol=ticker, error="No KSA data available")

    async def _scrape_argaam(self, code: str) -> Optional[Dict]:
        """
        Scrapes Argaam's market summary page. 
        Uses a cache specifically for the snapshot to avoid hitting Argaam 100 times for 100 stocks.
        """
        snapshot_key = "argaam_snapshot"
        snapshot = self.price_cache.get(snapshot_key)

        if not snapshot:
            try:
                url = ARGAAM_URL.format(market_id=3) # 3 = TASI
                resp = await self.client.get(url)
                resp.raise_for_status()
                
                snapshot = self._parse_argaam_html(resp.text)
                self.price_cache[snapshot_key] = snapshot # Cache the whole table
            except Exception as e:
                logger.error(f"Argaam scrape failed: {e}")
                return None

        return snapshot.get(code)

    def _parse_argaam_html(self, html: str) -> Dict[str, Dict]:
        """
        Robust parser for Argaam table.
        """
        data = {}
        if not BeautifulSoup:
            return data
            
        soup = BeautifulSoup(html, "lxml")
        # Find the main table. Usually has id or specific classes.
        # Fallback to finding any row with a numeric code.
        rows = soup.find_all("tr")
        
        for row in rows:
            cols = row.find_all("td")
            if len(cols) < 5: continue
            
            txts = [c.get_text(strip=True) for c in cols]
            
            # Check if first col is a ticker code (3-4 digits)
            if not re.match(r"^\d{3,5}$", txts[0]):
                continue
                
            code = txts[0]
            
            # Argaam layout changes, but usually:
            # Code | Name | Price | Change | %Change | ... | Volume
            try:
                data[code] = {
                    "company": txts[1],
                    "price": _safe_float(txts[2]),
                    "change": _safe_float(txts[3]),
                    "change_p": _safe_float(txts[4]),
                    "volume": _safe_float(txts[-2]) if len(txts) > 8 else 0, # Volume often near end
                    "avg_volume_3m": _safe_float(txts[-3]) if len(txts) > 8 else 0
                }
            except IndexError:
                continue
        return data

    # -------------------------------------------------------------------------
    # Global Strategy (EODHD -> FMP)
    # -------------------------------------------------------------------------

    async def _fetch_global_quote(self, ticker: str) -> UnifiedQuote:
        # 1. Try EODHD
        if settings.EODHD_API_KEY:
            q = await self._fetch_eodhd_realtime(ticker, market="GLOBAL", currency="USD")
            if q.data_quality != "MISSING":
                return q
                
        # 2. Try FMP
        if settings.FMP_API_KEY:
            q = await self._fetch_fmp_quote(ticker)
            if q.data_quality != "MISSING":
                return q
                
        return UnifiedQuote(symbol=ticker, error="Provider limit or missing key")

    # -------------------------------------------------------------------------
    # Provider Implementations
    # -------------------------------------------------------------------------

    async def _fetch_eodhd_realtime(self, ticker: str, market: str, currency: str) -> UnifiedQuote:
        try:
            url = f"https://eodhd.com/api/real-time/{ticker}"
            params = {"api_token": settings.EODHD_API_KEY, "fmt": "json"}
            resp = await self.client.get(url, params=params)
            resp.raise_for_status()
            d = resp.json()
            
            return UnifiedQuote(
                symbol=ticker,
                market=market,
                currency=currency,
                current_price=_safe_float(d.get("close") or d.get("previousClose")),
                price_change=_safe_float(d.get("change")),
                percent_change=_safe_float(d.get("change_p")),
                volume=_safe_float(d.get("volume")),
                day_high=_safe_float(d.get("high")),
                day_low=_safe_float(d.get("low")),
                data_source="eodhd",
                data_quality="FULL"
            )
        except Exception as e:
            return UnifiedQuote(symbol=ticker, error=str(e))

    async def _fetch_fmp_quote(self, ticker: str) -> UnifiedQuote:
        try:
            url = f"https://financialmodelingprep.com/api/v3/quote/{ticker}"
            resp = await self.client.get(url, params={"apikey": settings.FMP_API_KEY})
            d = resp.json()
            if not d: return UnifiedQuote(symbol=ticker)
            
            item = d[0]
            return UnifiedQuote(
                symbol=ticker,
                name=item.get("name"),
                market="GLOBAL",
                currency="USD",
                current_price=item.get("price"),
                price_change=item.get("change"),
                percent_change=item.get("changesPercentage"),
                day_high=item.get("dayHigh"),
                day_low=item.get("dayLow"),
                high_52w=item.get("yearHigh"),
                low_52w=item.get("yearLow"),
                volume=item.get("volume"),
                market_cap=item.get("marketCap"),
                pe_ttm=item.get("pe"),
                eps_ttm=item.get("eps"),
                data_source="fmp",
                data_quality="FULL"
            )
        except Exception as e:
            return UnifiedQuote(symbol=ticker, error=str(e))

    async def _fetch_yfinance(self, ticker: str, market: str) -> UnifiedQuote:
        try:
            # Run blocking YF call in thread
            def sync_yf():
                t = yf.Ticker(ticker)
                return t.fast_info
            
            info = await asyncio.to_thread(sync_yf)
            
            return UnifiedQuote(
                symbol=ticker,
                market=market,
                current_price=info.last_price,
                previous_close=info.previous_close,
                price_change=info.last_price - info.previous_close,
                percent_change=((info.last_price - info.previous_close)/info.previous_close)*100,
                volume=info.last_volume,
                currency=info.currency,
                data_source="yfinance",
                data_quality="PARTIAL"
            )
        except Exception as e:
            return UnifiedQuote(symbol=ticker, error=str(e))
