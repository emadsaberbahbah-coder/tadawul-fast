"""
core/data_engine.py
----------------------------------------------------------------------
UNIFIED DATA & ANALYSIS ENGINE – v1.3
Production-ready with real API integrations
Author: Emad Bahbah (with GPT-5.1 Thinking)

UPDATES v1.3:
1. REAL API INTEGRATION: Connects to your FastAPI backend instead of mock data
2. ALL CONFIGURED PROVIDERS: Uses ENABLED_PROVIDERS from environment
3. PROPER AUTH: Uses APP_TOKEN / BACKUP_APP_TOKEN for authentication
4. EODHD PRIMARY: Prioritizes EODHD for Saudi (.SR) symbols via paid subscription
5. ERROR HANDLING: Comprehensive retry and fallback logic
6. COMPATIBILITY: Adds properties expected by enriched_quote.py
7. CLEAN API: Exposes UnifiedQuote, DataQualityScore, ProviderSource, get_enriched_quote, get_enriched_quotes
"""

from __future__ import annotations

import asyncio
import os
import logging
from datetime import datetime, timezone
from typing import Dict, Optional, List, Literal, Tuple, Any

import aiohttp
from pydantic import BaseModel, Field

# ----------------------------------------------------------------------
# Logging
# ----------------------------------------------------------------------

logger = logging.getLogger(__name__)

# ----------------------------------------------------------------------
# CONFIGURATION
# ----------------------------------------------------------------------

# Read from environment (compatible with Render env and local .env)
BACKEND_BASE_URL = os.getenv(
    "BACKEND_BASE_URL", "https://tadawul-fast-bridge.onrender.com"
)
APP_TOKEN = os.getenv("APP_TOKEN", "")
BACKUP_APP_TOKEN = os.getenv("BACKUP_APP_TOKEN", "")
ENABLED_PROVIDERS = os.getenv(
    "ENABLED_PROVIDERS", "alpha_vantage,finnhub,eodhd,marketstack,twelvedata,fmp"
).split(",")
PRIMARY_PROVIDER = os.getenv("PRIMARY_PROVIDER", "eodhd")
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


class ProviderSource(BaseModel):
    """
    Simple provider descriptor, kept mostly for compatibility
    with enriched_quote and future scoring logic.
    """

    name: str
    weight: float = 1.0
    priority: int = 100


class DataQualityScore(BaseModel):
    """
    Optional structured quality description.
    Not heavily used yet but exported for future analytics / routers.
    """

    level: DataQualityLevel
    coverage_ratio: float = Field(ge=0.0, le=1.0)
    missing_fields: List[str] = Field(default_factory=list)


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
    # Compatibility properties for routes/enriched_quote.py
    # ------------------------------------------------------------------

    @property
    def last_updated(self) -> Optional[datetime]:
        """Alias for enriched_quote, which expects last_updated."""
        return self.last_updated_utc

    @property
    def pe_ratio(self) -> Optional[float]:
        """Alias mapping to P/E (TTM)."""
        return self.pe_ttm

    @property
    def price_change(self) -> Optional[float]:
        """Alias mapping for change."""
        return self.change

    @property
    def provider_sources(self) -> List[str]:
        """Return provider names used in this quote."""
        return [s.provider for s in self.sources]


# ----------------------------------------------------------------------
# HTTP CLIENT WITH RETRY LOGIC
# ----------------------------------------------------------------------


class HTTPClient:
    """Shared HTTP client for all provider calls."""

    _session: Optional[aiohttp.ClientSession] = None

    @classmethod
    async def get_session(cls) -> aiohttp.ClientSession:
        if cls._session is None or cls._session.closed:
            timeout = aiohttp.ClientTimeout(total=HTTP_TIMEOUT)
            cls._session = aiohttp.ClientSession(
                timeout=timeout,
                headers={
                    "User-Agent": "TadawulDataEngine/1.3",
                    "Accept": "application/json",
                },
            )
        return cls._session

    @classmethod
    async def close(cls) -> None:
        if cls._session and not cls._session.closed:
            await cls._session.close()

    @classmethod
    async def request_with_retry(
        cls,
        method: str,
        url: str,
        params: Optional[Dict[str, Any]] = None,
        headers: Optional[Dict[str, str]] = None,
    ) -> Optional[Dict[str, Any]]:
        """Make HTTP request with exponential backoff retry."""
        last_error: Optional[str] = None

        for attempt in range(MAX_RETRIES):
            try:
                session = await cls.get_session()
                async with session.request(
                    method=method, url=url, params=params, headers=headers
                ) as response:
                    if response.status == 200:
                        return await response.json()
                    elif response.status == 404:
                        logger.warning(f"Resource not found: {url}")
                        return None
                    else:
                        text = (await response.text())[:200]
                        last_error = f"HTTP {response.status}: {text}"
            except asyncio.TimeoutError:
                last_error = f"Timeout after {HTTP_TIMEOUT}s"
            except Exception as e:
                last_error = str(e)

            if attempt < MAX_RETRIES - 1:
                wait_time = 1.0 * (2**attempt)
                logger.debug(
                    f"Retrying {url} in {wait_time:.1f}s (attempt {attempt + 2}/{MAX_RETRIES})"
                )
                await asyncio.sleep(wait_time)

        logger.error(f"All retries failed for {url}: {last_error}")
        return None


# ----------------------------------------------------------------------
# PROVIDER ADAPTERS (REAL)
# ----------------------------------------------------------------------


async def fetch_from_backend_api(symbol: str, endpoint: str = "quote") -> Dict[str, Any]:
    """
    Fetch data from your main FastAPI backend.
    Assumes there is `/v1/quote?tickers=...&token=...` that returns
    a payload with `"symbols": [ { "ticker": "...", ... }, ... ]`.
    """
    if not BACKEND_BASE_URL or not APP_TOKEN:
        logger.warning("Backend URL or APP_TOKEN not configured for fetch_from_backend_api")
        return {}

    url = f"{BACKEND_BASE_URL.rstrip('/')}/v1/{endpoint}"
    params = {
        "tickers": symbol,
        "token": APP_TOKEN,
    }

    # Try with primary token
    data = await HTTPClient.request_with_retry("GET", url, params)
    # Fallback to backup token if available
    if not data and BACKUP_APP_TOKEN:
        params["token"] = BACKUP_APP_TOKEN
        data = await HTTPClient.request_with_retry("GET", url, params)

    if not data or "symbols" not in data:
        return {}

    for quote_data in data.get("symbols", []):
        if quote_data.get("ticker", "").upper() == symbol.upper():
            return _normalize_backend_quote(quote_data, symbol)

    return {}


async def fetch_from_eodhd_direct(symbol: str) -> Dict[str, Any]:
    """
    DIRECT EODHD API call (for premium subscription).
    Prioritized for Saudi symbols (.SR) or when PRIMARY_PROVIDER=eodhd.
    """
    eodhd_api_key = os.getenv("EODHD_API_KEY")
    eodhd_base_url = os.getenv("EODHD_BASE_URL", "https://eodhistoricaldata.com/api")

    if not eodhd_api_key:
        logger.debug("EODHD_API_KEY not configured; skipping direct EODHD call")
        return {}

    if PRIMARY_PROVIDER == "eodhd" or symbol.upper().endswith(".SR"):
        url = f"{eodhd_base_url.rstrip('/')}/real-time/{symbol}"
        params = {"api_token": eodhd_api_key, "fmt": "json"}

        data = await HTTPClient.request_with_retry("GET", url, params)
        if data and (data.get("close") or data.get("price")):
            return _normalize_eodhd_quote(data, symbol)

    return {}


async def fetch_fundamentals_from_backend(symbol: str) -> Dict[str, Any]:
    """
    Fetch fundamentals data from backend /v1/fundamentals endpoint.
    This is a secondary provider to enrich price snapshots with structural data.
    """
    if not BACKEND_BASE_URL or not APP_TOKEN:
        logger.debug("Backend URL or APP_TOKEN not configured for fundamentals")
        return {}

    url = f"{BACKEND_BASE_URL.rstrip('/')}/v1/fundamentals"
    params = {
        "symbol": symbol,
        "token": APP_TOKEN,
    }

    data = await HTTPClient.request_with_retry("GET", url, params)
    if data and data.get("status") == "OK":
        return data.get("data", {})

    return {}


def _normalize_backend_quote(data: Dict[str, Any], symbol: str) -> Dict[str, Any]:
    """Normalize backend API response to UnifiedQuote-compatible dict."""
    now = datetime.now(timezone.utc)

    result: Dict[str, Any] = {
        "symbol": symbol,
        "price": data.get("price"),
        "prev_close": data.get("previous_close"),
        "open": data.get("open_price"),
        "high": data.get("high_price"),
        "low": data.get("low_price"),
        "volume": data.get("volume"),
        "market_cap": data.get("market_cap"),
        "currency": data.get("currency"),
        "exchange": data.get("exchange"),
        "pe_ttm": data.get("pe_ratio"),
        "eps_ttm": data.get("eps"),
        "pb": data.get("pb_ratio"),
        "roe": data.get("roe"),
        "last_updated_utc": data.get("as_of") or now,
        "__source__": QuoteSourceInfo(
            provider=data.get("provider", "backend_api"),
            timestamp=now,
            fields=[k for k in data.keys() if data.get(k) is not None],
        ),
    }

    # Derived change fields
    _calculate_change_fields(result)
    return result


def _normalize_eodhd_quote(data: Dict[str, Any], symbol: str) -> Dict[str, Any]:
    """Normalize EODHD API response to UnifiedQuote-compatible dict."""
    now = datetime.now(timezone.utc)

    timestamp = now
    if data.get("timestamp"):
        try:
            timestamp = datetime.fromtimestamp(int(data["timestamp"]), tz=timezone.utc)
        except (ValueError, TypeError):
            pass

    result: Dict[str, Any] = {
        "symbol": symbol,
        "price": data.get("close") or data.get("price"),
        "prev_close": data.get("previousClose") or data.get("previous_close"),
        "open": data.get("open"),
        "high": data.get("high"),
        "low": data.get("low"),
        "volume": data.get("volume"),
        "market_cap": data.get("market_cap") or data.get("marketCap"),
        "currency": data.get("currency") or data.get("currencyCode"),
        "exchange": data.get("exchange") or data.get("exchange_short_name"),
        "last_updated_utc": timestamp,
        "__source__": QuoteSourceInfo(
            provider="eodhd_direct",
            timestamp=timestamp,
            fields=[k for k in data.keys() if data.get(k) is not None],
        ),
    }

    _calculate_change_fields(result)
    return result


# ----------------------------------------------------------------------
# PROVIDER REGISTRY WITH PRIORITY ORDER
# ----------------------------------------------------------------------

PROVIDER_REGISTRY: List[Tuple[str, Any]] = []

# Build provider list based on PRIMARY_PROVIDER and ENABLED_PROVIDERS
if PRIMARY_PROVIDER == "eodhd":
    PROVIDER_REGISTRY.append(("EODHD_DIRECT", fetch_from_eodhd_direct))
    PROVIDER_REGISTRY.append(("BACKEND_API", fetch_from_backend_api))
elif "tadawul_fast_bridge" in ENABLED_PROVIDERS:
    # If in future you add a dedicated tadawul_fast_bridge provider,
    # you can redirect to a different adapter here.
    PROVIDER_REGISTRY.append(("BACKEND_API", fetch_from_backend_api))
else:
    PROVIDER_REGISTRY.append(("BACKEND_API", fetch_from_backend_api))

# Always enrich with fundamentals last
PROVIDER_REGISTRY.append(("FUNDAMENTALS", fetch_fundamentals_from_backend))


# ----------------------------------------------------------------------
# MERGING & DATA COMPLETENESS LOGIC
# ----------------------------------------------------------------------


def _merge_dicts(primary: Dict[str, Any], secondary: Dict[str, Any]) -> Dict[str, Any]:
    """Merge two dicts, filling missing fields from secondary."""
    merged = dict(primary)
    for key, value in secondary.items():
        if key.startswith("__"):
            continue
        if merged.get(key) is None and value is not None:
            merged[key] = value
    return merged


def _infer_market_region(symbol: str, exchange: Optional[str]) -> MarketRegion:
    sym = symbol.upper()
    if sym.endswith(".SR") or (exchange and exchange.upper() in {"TADAWUL", "NOMU"}):
        return "KSA"
    if exchange and exchange.upper() in {"NASDAQ", "NYSE", "AMEX", "LSE", "XETRA"}:
        return "GLOBAL"
    return "UNKNOWN"


def _calculate_change_fields(data: Dict[str, Any]) -> None:
    price = data.get("price")
    prev_close = data.get("prev_close")

    if price is None or prev_close is None:
        return
    if prev_close == 0:
        return

    data["change"] = price - prev_close
    data["change_pct"] = (price - prev_close) / prev_close * 100.0


def _assess_data_quality(data: Dict[str, Any]) -> Tuple[DataQualityLevel, List[str]]:
    required_price_fields = ["price", "prev_close", "volume"]
    important_fundamentals = ["market_cap", "eps_ttm", "pe_ttm", "pb"]

    missing: List[str] = []

    for f in required_price_fields:
        if data.get(f) is None:
            missing.append(f)

    for f in important_fundamentals:
        if data.get(f) is None:
            missing.append(f)

    total_fields = len(required_price_fields) + len(important_fundamentals)
    missing_count = len(missing)

    if missing_count == total_fields:
        return "MISSING", missing

    coverage = 1.0 - (missing_count / float(total_fields))

    if coverage > 0.80:
        return "EXCELLENT", missing
    if coverage > 0.60:
        return "GOOD", missing
    if coverage > 0.40:
        return "FAIR", missing
    return "POOR", missing


# ----------------------------------------------------------------------
# ANALYSIS LAYER
# ----------------------------------------------------------------------


def _compute_opportunity_score(data: Dict[str, Any]) -> Optional[float]:
    pe = data.get("pe_ttm")
    roe = data.get("roe")
    pm = data.get("profit_margin")
    rev_g = data.get("revenue_growth_yoy")
    ni_g = data.get("net_income_growth_yoy")

    if all(v is None for v in [pe, roe, pm, rev_g, ni_g]):
        return None

    score = 50.0

    if pe is not None and roe is not None and pe > 0:
        pe_roe_ratio = pe / max(roe * 100.0, 0.1)
        if pe_roe_ratio < 0.5:
            score += 15
        elif pe_roe_ratio < 1.0:
            score += 8
        elif pe_roe_ratio > 2.0:
            score -= 10

    if pm is not None:
        pm_pct = pm * 100.0
        if pm_pct > 25:
            score += 10
        elif pm_pct > 15:
            score += 5
        elif pm_pct < 5:
            score -= 5

    growth_scores: List[float] = []
    for g in (rev_g, ni_g):
        if g is None:
            continue
        g_pct = g * 100.0
        if g_pct > 20:
            growth_scores.append(10)
        elif g_pct > 10:
            growth_scores.append(6)
        elif g_pct > 0:
            growth_scores.append(3)
        elif g_pct < -5:
            growth_scores.append(-5)

    if growth_scores:
        score += sum(growth_scores) / len(growth_scores)

    return max(0.0, min(100.0, score))


def _derive_risk_flag(
    data: Dict[str, Any], opportunity_score: Optional[float]
) -> Optional[str]:
    if opportunity_score is None:
        return None

    pe = data.get("pe_ttm")
    debt_equity = data.get("debt_to_equity")
    rev_g = data.get("revenue_growth_yoy")

    if opportunity_score >= 70:
        if debt_equity is not None and debt_equity > 1.5:
            return "LEVERAGED OPPORTUNITY"
        if rev_g is not None and rev_g < 0:
            return "TURNAROUND PLAY"
        return "STRONG OPPORTUNITY"

    if opportunity_score <= 35:
        if pe is not None and pe > 40:
            return "OVERVALUED / SPECULATIVE"
        if rev_g is not None and rev_g < 0:
            return "DECLINING BUSINESS"
        return "WEAK OPPORTUNITY"

    return "NEUTRAL"


def _build_notes(
    data: Dict[str, Any],
    opportunity_score: Optional[float],
    risk_flag: Optional[str],
) -> Optional[str]:
    pieces: List[str] = []

    if risk_flag:
        pieces.append(f"Risk flag: {risk_flag}.")
    if opportunity_score is not None:
        pieces.append(f"Composite opportunity score: {opportunity_score:.1f}/100.")

    pe = data.get("pe_ttm")
    roe = data.get("roe")
    pm = data.get("profit_margin")
    rev_g = data.get("revenue_growth_yoy")
    ni_g = data.get("net_income_growth_yoy")

    if pe is not None:
        pieces.append(f"P/E (TTM): {pe:.1f}.")
    if roe is not None:
        pieces.append(f"ROE: {roe * 100:.1f}%.")
    if pm is not None:
        pieces.append(f"Profit margin: {pm * 100:.1f}%.")
    if rev_g is not None:
        pieces.append(f"Revenue YoY growth: {rev_g * 100:.1f}%.")
    if ni_g is not None:
        pieces.append(f"Net income YoY growth: {ni_g * 100:.1f}%.")

    return " ".join(pieces) if pieces else None


# ----------------------------------------------------------------------
# PUBLIC ENTRYPOINTS
# ----------------------------------------------------------------------


async def get_enriched_quote(symbol: str) -> UnifiedQuote:
    """
    Public enriched quote function (REAL DATA).

    - Uses your FastAPI backend & EODHD subscription.
    - Prioritizes EODHD for Saudi (.SR) symbols.
    - Merges price + fundamentals.
    - Computes quality, opportunity score, risk flag, and notes.
    """

    symbol = symbol.strip().upper()
    if not symbol:
        raise ValueError("Symbol cannot be empty.")

    logger.info(f"[DataEngine] Fetching enriched quote for: {symbol}")

    # 1) Call all registered providers concurrently
    tasks: Dict[str, asyncio.Task] = {
        name: asyncio.create_task(func(symbol)) for name, func in PROVIDER_REGISTRY
    }

    raw_results: Dict[str, Dict[str, Any]] = {}
    sources: List[QuoteSourceInfo] = []

    for name, task in tasks.items():
        try:
            data = await task
        except Exception as e:
            logger.warning(f"[DataEngine] Provider {name} failed for {symbol}: {e}")
            continue

        if not data:
            logger.debug(f"[DataEngine] Provider {name} returned no data for {symbol}")
            continue

        raw_results[name] = data

        src = data.get("__source__")
        if isinstance(src, QuoteSourceInfo):
            sources.append(src)
        else:
            sources.append(
                QuoteSourceInfo(
                    provider=name,
                    timestamp=datetime.now(timezone.utc),
                    fields=[k for k in data.keys() if not k.startswith("__")],
                )
            )

    # 2) No data for Saudi symbols – helpful guidance
    if symbol.endswith(".SR") and not raw_results:
        logger.warning(
            f"[DataEngine] No data for Saudi symbol {symbol}. "
            f"Check EODHD subscription & /real-time endpoint."
        )
        return UnifiedQuote(
            symbol=symbol,
            market_region="KSA",
            last_updated_utc=datetime.now(timezone.utc),
            data_quality="MISSING",
            data_gaps=["No data from EODHD / backend for Saudi symbol"],
            sources=[],
            notes=f"Saudi symbol {symbol} likely requires active EODHD subscription.",
        )

    # 3) No data at all
    if not raw_results:
        logger.error(
            f"[DataEngine] No data from any provider for {symbol}. "
            f"Providers attempted: {', '.join([p[0] for p in PROVIDER_REGISTRY])}"
        )
        return UnifiedQuote(
            symbol=symbol,
            last_updated_utc=datetime.now(timezone.utc),
            data_quality="MISSING",
            data_gaps=[
                f"No data from any provider ({', '.join([p[0] for p in PROVIDER_REGISTRY])})"
            ],
            sources=[],
        )

    # 4) Merge provider data following registry order
    merged: Dict[str, Any] = {}
    for provider_name, _func in PROVIDER_REGISTRY:
        provider_data = raw_results.get(provider_name)
        if not provider_data:
            continue

        clean_data = {k: v for k, v in provider_data.items() if not k.startswith("__")}
        if not merged:
            merged = clean_data
        else:
            merged = _merge_dicts(merged, clean_data)

    # 5) Derived fields & meta
    _calculate_change_fields(merged)

    merged["market_region"] = _infer_market_region(
        merged.get("symbol") or symbol,
        merged.get("exchange"),
    )

    if merged.get("last_updated_utc") is None:
        merged["last_updated_utc"] = datetime.now(timezone.utc)

    dq, gaps = _assess_data_quality(merged)

    # 6) Analysis
    opp_score = _compute_opportunity_score(merged)
    risk_flag = _derive_risk_flag(merged, opp_score)
    notes = _build_notes(merged, opp_score, risk_flag)

    # 7) Build final quote
    quote = UnifiedQuote(
        **{
            **merged,
            "symbol": merged.get("symbol") or symbol,
            "data_quality": dq,
            "data_gaps": gaps,
            "opportunity_score": opp_score,
            "risk_flag": risk_flag,
            "notes": notes,
            "sources": sources,
        }
    )

    logger.info(
        f"[DataEngine] Done {symbol}: "
        f"quality={dq}, score={opp_score}, region={quote.market_region}"
    )
    return quote


async def get_enriched_quotes(symbols: List[str]) -> List[UnifiedQuote]:
    """
    Fetch multiple quotes concurrently with robust error handling.
    """
    tasks = [get_enriched_quote(sym) for sym in symbols]
    results = await asyncio.gather(*tasks, return_exceptions=True)

    quotes: List[UnifiedQuote] = []
    for sym, result in zip(symbols, results):
        if isinstance(result, UnifiedQuote):
            quotes.append(result)
        elif isinstance(result, Exception):
            logger.error(f"[DataEngine] Error fetching {sym}: {result}")
            quotes.append(
                UnifiedQuote(
                    symbol=sym,
                    data_quality="MISSING",
                    data_gaps=[f"Error: {str(result)}"],
                    last_updated_utc=datetime.now(timezone.utc),
                )
            )

    return quotes


# ----------------------------------------------------------------------
# CLEANUP ON SHUTDOWN
# ----------------------------------------------------------------------


async def cleanup() -> None:
    """Clean up HTTP client on shutdown."""
    await HTTPClient.close()


# ----------------------------------------------------------------------
# TEST HARNESS
# ----------------------------------------------------------------------


async def test_engine() -> None:
    """Test the data engine with sample symbols."""
    test_symbols = ["AAPL", "MSFT", "2222.SR", "1180.SR"]

    print("Testing Data Engine v1.3...")
    print("=" * 60)

    for symbol in test_symbols:
        print(f"\nFetching: {symbol}")
        try:
            quote = await get_enriched_quote(symbol)
            print(f"  Status: {quote.data_quality}")
            print(f"  Price:  {quote.price}")
            print(f"  Change: {quote.change_pct if quote.change_pct is not None else 'N/A'}")
            print(f"  Score:  {quote.opportunity_score}")
            print(f"  Sources: {[s.provider for s in quote.sources]}")
        except Exception as e:
            print(f"  ERROR: {e}")

    print("\n" + "=" * 60)
    print("Test completed.")


if __name__ == "__main__":
    asyncio.run(test_engine())
