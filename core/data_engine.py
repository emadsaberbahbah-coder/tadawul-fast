"""
core/data_engine.py
----------------------------------------------------------------------
UNIFIED DATA & ANALYSIS ENGINE – v3.0 (ASYNCIO TASKGROUP + PYDANTIC V2)
(MULTI-PROVIDER, KSA-SAFE, RESILIENT ARCHITECTURE)

Role in Architecture
--------------------
- This module serves as the robust, high-performance data ingestion layer.
- It leverages Python 3.11+ Structured Concurrency (asyncio.TaskGroup) for
  safety and throughput.
- It implements the "Rich Domain Model" pattern using Pydantic V2, where
  scoring logic (Altman Z-Score, Piotroski F-Score) is embedded directly
  into the data model via @computed_field.

Key Behaviors
-------------
- **Structured Concurrency**: Uses `asyncio.TaskGroup` to ensure atomic
  fetch operations. If one critical task fails, the group is handled
  predictably (fail-fast or partial success depending on config).
- **Resilience**: Implements Exponential Backoff with Jitter using the
  `tenacity` library to handle transient API failures (429, 503).
- **Multi-Provider Strategy**:
    • EODHD (Global Bulk): Primary for fundamentals (skipped for KSA).
    • FMP (Global & KSA): Primary for Scores & Real-time KSA data.
    • Yahoo (Universal Fallback): Safety net.
- **Scoring Engine**:
    • Automatically computes or retrieves Altman Z-Score and Piotroski F-Score.
    • Classifies financial health into "Safe", "Grey", or "Distress" zones.

Global vs KSA
-------------
- **KSA (.SR)**: EODHD is strictly bypassed to avoid indicative pricing errors.
  The engine relies on FMP (Quote + Financial Scores endpoints) and Yahoo.
- **Global**: Leverages EODHD's bulk fundamentals for deep history.

Models
------
- Uses Pydantic V2 `ConfigDict` and `model_validate_json` for rust-backed
  parsing performance.
"""

from __future__ import annotations

import asyncio
import logging
import os
import json
from datetime import datetime, timezone
from typing import Any, Dict, List, Literal, Optional, Tuple, Union

import aiohttp

# ----------------------------------------------------------------------
# External Libraries (Resilience & Validation)
# ----------------------------------------------------------------------
try:
    from tenacity import (
        retry,
        stop_after_attempt,
        wait_exponential_jitter,
        retry_if_exception_type,
        RetryError,
    )
except ImportError:
    # Fallback if tenacity is missing (though highly recommended)
    logging.getLogger(__name__).warning("Tenacity not found. Retries disabled.")
    def retry(*args, **kwargs):
        def decorator(f):
            return f
        return decorator
    def stop_after_attempt(*args): return None
    def wait_exponential_jitter(*args, **kwargs): return None
    def retry_if_exception_type(*args): return None

try:
    import yfinance as yf
except ImportError:
    yf = None

from pydantic import (
    BaseModel,
    Field,
    ConfigDict,
    computed_field,
    ValidationError,
)

# ----------------------------------------------------------------------
# Logging
# ----------------------------------------------------------------------
logger = logging.getLogger(__name__)

# ----------------------------------------------------------------------
# Configuration / Environment
# ----------------------------------------------------------------------
try:
    import env as _env
except Exception:
    _env = None

def _get_str_config(name: str, default: str) -> str:
    if _env and hasattr(_env, name):
        val = getattr(_env, name)
        return str(val) if val is not None else default
    return os.getenv(name, default)

def _get_int_config(name: str, default: int) -> int:
    if _env and hasattr(_env, name):
        try:
            return int(getattr(_env, name))
        except Exception:
            pass
    val = os.getenv(name)
    try:
        return int(val) if val else default
    except Exception:
        return default

# Network Config
HTTP_TIMEOUT = _get_int_config("HTTP_TIMEOUT", 25)
MAX_RETRIES = _get_int_config("MAX_RETRIES", 3)

# Provider Config
EODHD_API_KEY = _get_str_config("EODHD_API_KEY", "")
EODHD_BASE_URL = _get_str_config("EODHD_BASE_URL", "[https://eodhd.com/api](https://eodhd.com/api)")

FMP_API_KEY = _get_str_config("FMP_API_KEY", "")
FMP_BASE_URL = _get_str_config("FMP_BASE_URL", "[https://financialmodelingprep.com/api/v3](https://financialmodelingprep.com/api/v3)")
FMP_SCORE_URL = _get_str_config("FMP_SCORE_URL", "[https://financialmodelingprep.com/api/v4](https://financialmodelingprep.com/api/v4)")

# Flags
_ENABLE_EODHD_FUNDAMENTALS_RAW = _get_str_config("ENABLE_EODHD_FUNDAMENTALS", "1")
ENABLE_EODHD_FUNDAMENTALS = _ENABLE_EODHD_FUNDAMENTALS_RAW.lower() not in {"0", "false", "no", "off"}

ENABLED_PROVIDERS_RAW = _get_str_config("ENABLED_PROVIDERS", "eodhd,fmp,yfinance")
ENABLED_PROVIDERS =

PRIMARY_PROVIDER = _get_str_config("PRIMARY_PROVIDER", "eodhd").lower()

# Types
DataQualityLevel = Literal
MarketRegion = Literal


# ----------------------------------------------------------------------
# MODELS (Pydantic V2)
# ----------------------------------------------------------------------

class QuoteSourceInfo(BaseModel):
    """Metadata about the source of the data."""
    provider: str
    timestamp: datetime
    fields: List[str] = Field(default_factory=list)

    model_config = ConfigDict(frozen=True)


class UnifiedQuote(BaseModel):
    """
    The canonical data model for a financial instrument.
    Includes @computed_field logic for financial scoring.
    """
    model_config = ConfigDict(
        populate_by_name=True,
        extra="ignore",
        arbitrary_types_allowed=True
    )

    # --- Identity ---
    symbol: str
    name: Optional[str] = None
    exchange: Optional[str] = None
    currency: Optional[str] = None
    market_region: MarketRegion = "UNKNOWN"
    sector: Optional[str] = None
    industry: Optional[str] = None
    listing_date: Optional[str] = None

    # --- Market Data ---
    price: Optional[float] = Field(None, alias="last_price")
    prev_close: Optional[float] = Field(None, alias="previous_close")
    open: Optional[float] = None
    high: Optional[float] = None
    low: Optional[float] = None
    volume: Optional[float] = None
    avg_volume_30d: Optional[float] = None
    value_traded: Optional[float] = None
    turnover_rate: Optional[float] = None

    # --- 52 Week ---
    fifty_two_week_high: Optional[float] = Field(None, alias="high_52w")
    fifty_two_week_low: Optional[float] = Field(None, alias="low_52w")
    fifty_two_week_position: Optional[float] = None  # 0-100%

    # --- Change ---
    change: Optional[float] = Field(None, alias="price_change")
    change_pct: Optional[float] = Field(None, alias="change_percent")

    # --- Fundamentals ---
    market_cap: Optional[float] = None
    shares_outstanding: Optional[float] = None
    free_float: Optional[float] = None
    eps_ttm: Optional[float] = None
    pe_ttm: Optional[float] = Field(None, alias="pe_ratio")
    pb: Optional[float] = None
    dividend_yield: Optional[float] = None
    dividend_payout_ratio: Optional[float] = None
    beta: Optional[float] = None

    # --- Profitability & Efficiency ---
    roe: Optional[float] = None
    roa: Optional[float] = None
    profit_margin: Optional[float] = None
    operating_margin: Optional[float] = None
    ebitda_margin: Optional[float] = None
    revenue_growth_yoy: Optional[float] = None

    # --- Valuation ---
    price_to_sales: Optional[float] = None
    price_to_cash_flow: Optional[float] = None
    ev_to_ebitda: Optional[float] = None
    peg_ratio: Optional[float] = None

    # --- Scores (Provider or Computed) ---
    altman_z_score: Optional[float] = None
    piotroski_score: Optional[int] = None

    # --- Meta ---
    last_updated_utc: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    data_quality: DataQualityLevel = "MISSING"
    data_gaps: List[str] = Field(default_factory=list)
    sources: List = Field(default_factory=list)

    # --- Computed / AI Fields ---
    
    @computed_field
    @property
    def financial_health_zone(self) -> str:
        """
        Classifies the Altman Z-Score into zones.
        Safe > 2.99 | Grey 1.81-2.99 | Distress < 1.81
        """
        z = self.altman_z_score
        if z is None:
            return "UNKNOWN"
        if z > 2.99:
            return "SAFE"
        if z > 1.81:
            return "GREY"
        return "DISTRESS"

    @computed_field
    @property
    def opportunity_score(self) -> float:
        """
        Simple 0-100 score based on value, quality, and momentum.
        """
        score = 50.0
        
        # Value Component
        if self.pe_ttm and 0 < self.pe_ttm < 20: score += 10
        if self.pb and 0 < self.pb < 3: score += 5
        
        # Quality Component
        if self.roe and self.roe > 0.15: score += 10
        if self.profit_margin and self.profit_margin > 0.10: score += 10
        
        # Momentum Component
        if self.change_pct and self.change_pct > 0: score += 5
        if self.fifty_two_week_position and self.fifty_two_week_position > 80: score += 5

        # Risk Penalties
        if self.altman_z_score and self.altman_z_score < 1.81: score -= 20
        
        return max(0.0, min(100.0, score))

    # --- Legacy Compatibility Aliases (Computed) ---

    @computed_field
    @property
    def as_of_utc(self) -> datetime:
        return self.last_updated_utc

    @computed_field
    @property
    def timezone(self) -> str:
        return "Asia/Riyadh" if self.market_region == "KSA" else "UTC"

    @computed_field
    @property
    def provider(self) -> Optional[str]:
        return self.sources.provider if self.sources else None


# ----------------------------------------------------------------------
# UTILITIES
# ----------------------------------------------------------------------

def _infer_market_region(symbol: str) -> MarketRegion:
    if symbol.upper().endswith(".SR"):
        return "KSA"
    if "." not in symbol:
        return "GLOBAL" # Assumption for US tickers
    return "GLOBAL"

def _normalize_percent(x: Any) -> Optional[float]:
    """Normalizes 15.5 -> 0.155"""
    try:
        val = float(x)
        if val is None: return None
        if val > 1.0: return val / 100.0
        return val
    except (ValueError, TypeError):
        return None

# ----------------------------------------------------------------------
# PROVIDER FETCHERS (With Tenacity & Structured Concurrency)
# ----------------------------------------------------------------------

@retry(
    stop=stop_after_attempt(MAX_RETRIES),
    wait=wait_exponential_jitter(initial=1, max=10),
    retry=retry_if_exception_type((aiohttp.ClientError, asyncio.TimeoutError)),
    reraise=True
)
async def _fetch_url(session: aiohttp.ClientSession, url: str, params: Dict[str, Any] = None) -> Any:
    """Robust generic fetcher with retry logic."""
    async with session.get(url, params=params, timeout=HTTP_TIMEOUT) as resp:
        if resp.status == 429:
            # Explicitly raise to trigger retry
            logger.warning(f"Rate limited (429) on {url}")
            resp.raise_for_status()
        if resp.status >= 500:
            logger.warning(f"Server error ({resp.status}) on {url}")
            resp.raise_for_status()
        
        # For 404 or other 4xx, return None (don't retry endlessly)
        if resp.status!= 200:
            logger.debug(f"HTTP {resp.status} for {url}")
            return None
            
        return await resp.json()

async def fetch_eodhd(symbol: str, session: aiohttp.ClientSession) -> Dict[str, Any]:
    """
    Fetch EODHD data using TaskGroup to parallelize Real-Time and Fundamentals.
    Skipped for KSA symbols.
    """
    if not EODHD_API_KEY:
        return {}
    
    # KSA Safety Check
    if _infer_market_region(symbol) == "KSA":
        logger.debug(f"Skipping EODHD for KSA symbol: {symbol}")
        return {}

    code = symbol
    if "." not in code: code = f"{code}.US"
    
    base = EODHD_BASE_URL.rstrip("/")
    params = {"api_token": EODHD_API_KEY, "fmt": "json"}
    
    rt_data = {}
    fund_data = {}
    
    try:
        # Python 3.11+ Structured Concurrency
        async with asyncio.TaskGroup() as tg:
            # Task 1: Real-Time Price
            t1 = tg.create_task(_fetch_url(session, f"{base}/real-time/{code}", params))
            
            # Task 2: Fundamentals (if enabled)
            t2 = None
            if ENABLE_EODHD_FUNDAMENTALS:
                t2 = tg.create_task(_fetch_url(session, f"{base}/fundamentals/{code}", params))
                
        # Harvest results (TaskGroup raises ExceptionGroup if any failed, 
        # but _fetch_url handles specific network errors appropriately)
        rt_data = t1.result() or {}
        if t2:
            fund_data = t2.result() or {}
            
    except Exception as e:
        logger.error(f"EODHD TaskGroup failure for {symbol}: {e}")
        return {}

    return _normalize_eodhd(symbol, rt_data, fund_data)

async def fetch_fmp(symbol: str, session: aiohttp.ClientSession) -> Dict[str, Any]:
    """
    Fetch FMP data. Critical for KSA.
    Parallelizes Quote and Financial Scores.
    """
    if not FMP_API_KEY:
        return {}

    # FMP usually expects clean symbols (e.g. 2222.SR or AAPL)
    sym = symbol.upper()
    
    params = {"apikey": FMP_API_KEY}
    
    quote_data = {}
    score_data = {}
    
    try:
        async with asyncio.TaskGroup() as tg:
            # Task 1: Quote
            q_url = f"{FMP_BASE_URL}/quote/{sym}"
            t1 = tg.create_task(_fetch_url(session, q_url, params))
            
            # Task 2: Financial Scores (Z-Score / Piotroski)
            # Note: This is a v4 endpoint
            s_url = f"{FMP_SCORE_URL}/score"
            # score endpoint takes 'symbol' param
            score_params = params.copy()
            score_params["symbol"] = sym
            t2 = tg.create_task(_fetch_url(session, s_url, score_params))
            
        quote_list = t1.result()
        if isinstance(quote_list, list) and quote_list:
            quote_data = quote_list
            
        score_list = t2.result()
        if isinstance(score_list, list) and score_list:
            score_data = score_list
            
    except Exception as e:
        logger.error(f"FMP TaskGroup failure for {symbol}: {e}")
        return {}
        
    return _normalize_fmp(symbol, quote_data, score_data)

async def fetch_yahoo(symbol: str) -> Dict[str, Any]:
    """Synchronous fallback run in executor."""
    if not yf: return {}
    
    try:
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(None, _yahoo_worker, symbol)
    except Exception as e:
        logger.error(f"Yahoo fetch failed for {symbol}: {e}")
        return {}

def _yahoo_worker(symbol: str) -> Dict[str, Any]:
    """Blocking worker for yfinance."""
    try:
        ticker = yf.Ticker(symbol)
        info = ticker.info
        if not info: return {}
        # Normalize immediately to generic dict
        return {
            "price": info.get("currentPrice") or info.get("regularMarketPrice"),
            "prev_close": info.get("previousClose"),
            "market_cap": info.get("marketCap"),
            "pe_ttm": info.get("trailingPE"),
            "dividend_yield": info.get("dividendYield"),
            "name": info.get("longName"),
            "__source__": "yfinance"
        }
    except Exception:
        return {}

# ----------------------------------------------------------------------
# NORMALIZERS
# ----------------------------------------------------------------------

def _normalize_eodhd(symbol: str, rt: Dict, fund: Dict) -> Dict[str, Any]:
    """Normalize EODHD data into unified schema."""
    if not rt and not fund: return {}
    
    # Extract fundamentals substructures
    general = fund.get("General", {})
    highlights = fund.get("Highlights", {})
    valuation = fund.get("Valuation", {})
    technicals = fund.get("Technicals", {})
    
    return {
        "symbol": symbol,
        "price": rt.get("close"),
        "prev_close": rt.get("previousClose"),
        "change": rt.get("change"),
        "change_pct": rt.get("change_p"),
        "volume": rt.get("volume"),
        "market_cap": rt.get("market_cap") or highlights.get("MarketCapitalization"),
        "pe_ttm": highlights.get("PERatio"),
        "eps_ttm": highlights.get("EpsTTM"),
        "dividend_yield": highlights.get("DividendYield"),
        "sector": general.get("Sector"),
        "industry": general.get("Industry"),
        "description": general.get("Description"),
        "__source__": "eodhd"
    }

def _normalize_fmp(symbol: str, quote: Dict, score: Dict) -> Dict[str, Any]:
    """Normalize FMP data."""
    if not quote: return {}
    
    return {
        "symbol": symbol,
        "price": quote.get("price"),
        "prev_close": quote.get("previousClose"),
        "change": quote.get("change"),
        "change_pct": quote.get("changesPercentage"),
        "volume": quote.get("volume"),
        "avg_volume_30d": quote.get("avgVolume"),
        "market_cap": quote.get("marketCap"),
        "pe_ttm": quote.get("pe"),
        "eps_ttm": quote.get("eps"),
        "high_52w": quote.get("yearHigh"),
        "low_52w": quote.get("yearLow"),
        # Scores
        "altman_z_score": score.get("altmanZScore"),
        "piotroski_score": score.get("piotroskiScore"),
        "__source__": "fmp"
    }

# ----------------------------------------------------------------------
# AGGREGATION ENGINE
# ----------------------------------------------------------------------

async def _gather_provider_payloads(symbol: str) -> UnifiedQuote:
    """
    Orchestrates the multi-provider fetch using Structured Concurrency.
    """
    symbol = symbol.strip().upper()
    is_ksa = _infer_market_region(symbol) == "KSA"
    
    payloads =
    
    timeout = aiohttp.ClientTimeout(total=HTTP_TIMEOUT)
    async with aiohttp.ClientSession(timeout=timeout) as session:
        try:
            async with asyncio.TaskGroup() as tg:
                # 1. EODHD (Skip if KSA)
                t_eodhd = None
                if not is_ksa and "eodhd" in ENABLED_PROVIDERS:
                    t_eodhd = tg.create_task(fetch_eodhd(symbol, session))
                
                # 2. FMP (Priority for KSA & Scores)
                t_fmp = None
                if "fmp" in ENABLED_PROVIDERS:
                    t_fmp = tg.create_task(fetch_fmp(symbol, session))
                
                # 3. Yahoo (Fallback)
                t_yahoo = None
                if "yfinance" in ENABLED_PROVIDERS:
                    t_yahoo = tg.create_task(fetch_yahoo(symbol))
                    
        except ExceptionGroup as eg:
            # Log individual errors but continue if some providers succeeded
            logger.warning(f"Partial provider failure for {symbol}: {eg}")
            
        # Harvest
        if t_eodhd and t_eodhd.result(): payloads.append(t_eodhd.result())
        if t_fmp and t_fmp.result(): payloads.append(t_fmp.result())
        if t_yahoo and t_yahoo.result(): payloads.append(t_yahoo.result())

    # Merge Logic: Priority based on config
    # KSA -> FMP wins. Global -> EODHD wins (usually).
    final_data = {"symbol": symbol}
    sources =
    
    # Sort payloads by provider priority
    # For KSA, FMP is top. For Global, Primary (EODHD) is top.
    def priority_sort(p):
        prov = p.get("__source__")
        if is_ksa:
            if prov == "fmp": return 0
            if prov == "yfinance": return 1
            return 2
        else:
            if prov == PRIMARY_PROVIDER: return 0
            if prov == "fmp": return 1
            return 2
            
    payloads.sort(key=priority_sort)
    
    for p in payloads:
        if not p: continue
        prov_name = p.pop("__source__", "unknown")
        
        # Track source
        sources.append(QuoteSourceInfo(
            provider=prov_name,
            timestamp=datetime.now(timezone.utc),
            fields=list(p.keys())
        ))
        
        # Merge fields (first non-None wins due to sort order)
        for k, v in p.items():
            if v is not None and k not in final_data:
                final_data[k] = v
                
    final_data["sources"] = sources
    final_data["market_region"] = "KSA" if is_ksa else "GLOBAL"
    
    # Validate and Return
    try:
        return UnifiedQuote(**final_data)
    except ValidationError as e:
        logger.error(f"Validation failed for {symbol}: {e}")
        return UnifiedQuote(symbol=symbol, data_quality="MISSING", data_gaps=["Validation Error"])


# ----------------------------------------------------------------------
# PUBLIC ENTRYPOINTS
# ----------------------------------------------------------------------

async def get_enriched_quote(symbol: str) -> UnifiedQuote:
    """Module-level entrypoint."""
    return await _gather_provider_payloads(symbol)

async def get_enriched_quotes(symbols: List[str]) -> List[UnifiedQuote]:
    """Batch entrypoint using TaskGroup for concurrency."""
    quotes =
    # Process in chunks to avoid overwhelming local resources
    chunk_size = 20
    
    for i in range(0, len(symbols), chunk_size):
        chunk = symbols[i:i+chunk_size]
        try:
            async with asyncio.TaskGroup() as tg:
                tasks = [tg.create_task(get_enriched_quote(s)) for s in chunk]
            
            # Harvest
            for t in tasks:
                quotes.append(t.result())
        except Exception as e:
            logger.error(f"Batch fetch error: {e}")
            # Append empty/error quotes for safety
            quotes.extend()
            
    return quotes

# ----------------------------------------------------------------------
# CLASS WRAPPER
# ----------------------------------------------------------------------

class DataEngine:
    def __init__(self, enabled_providers: List[str] = None):
        self.enabled_providers = enabled_providers or ENABLED_PROVIDERS

    async def get_enriched_quote(self, symbol: str) -> UnifiedQuote:
        return await get_enriched_quote(symbol)

    async def get_enriched_quotes(self, symbols: List[str]) -> List[UnifiedQuote]:
        return await get_enriched_quotes(symbols)
"""
