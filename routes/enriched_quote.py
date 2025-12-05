# routes/enriched_quote.py
"""
Enhanced Enriched Quote Routes – v1.2

Key Features:
- Advanced authentication with rate limiting
- Caching with TTL for performance
- Comprehensive error handling with retries
- Detailed metrics and telemetry (Prometheus)
- Support for batch requests
- WebSocket for real-time updates
- Health monitoring and diagnostics
"""

from __future__ import annotations

import asyncio
import time
from typing import List, Optional, Dict, Any
from datetime import datetime, timedelta, timezone
from enum import Enum

from fastapi import (
    APIRouter,
    Depends,
    HTTPException,
    Query,
    status,
    WebSocket,
    WebSocketDisconnect,
    BackgroundTasks,
    Request,
)
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field, validator
from cachetools import TTLCache
import logging
from prometheus_client import Counter, Histogram, Gauge

from core.data_engine import (
    get_enriched_quote,
    UnifiedQuote,
    DataQualityScore,  # kept import for future use / compatibility
    ProviderSource,
)

# ---------------------------------------------------------------------------
# Logging & Metrics
# ---------------------------------------------------------------------------

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Prometheus metrics for monitoring
QUOTE_REQUESTS = Counter(
    "quote_requests_total", "Total quote requests", ["symbol", "provider"]
)
QUOTE_LATENCY = Histogram(
    "quote_request_latency_seconds", "Quote request latency"
)
QUOTE_ERRORS = Counter(
    "quote_errors_total", "Total quote errors", ["error_type"]
)
ACTIVE_WEBSOCKETS = Gauge(
    "active_websockets", "Active WebSocket connections"
)

# Rate limiting cache (per API key)
RATE_LIMIT_CACHE: TTLCache = TTLCache(maxsize=10_000, ttl=60)

# Response cache (per symbol + providers)
QUOTE_CACHE: TTLCache = TTLCache(maxsize=1_000, ttl=30)  # 30 seconds cache

router = APIRouter(
    prefix="/v1",
    tags=["enriched_quote"],
    responses={
        400: {"description": "Bad Request"},
        401: {"description": "Unauthorized"},
        429: {"description": "Too Many Requests"},
        500: {"description": "Internal Server Error"},
    },
)

# ---------------------------------------------------------------------------
# Models
# ---------------------------------------------------------------------------


class QuoteProvider(str, Enum):
    GOOGLE_SHEETS = "google_sheets"
    ARGAAM = "argaam"
    YAHOO_FINANCE = "yahoo_finance"
    TADAWUL = "tadawul"
    ALPHA_VANTAGE = "alpha_vantage"


class QuoteQuality(BaseModel):
    """Detailed quality assessment for quote data"""

    overall_score: float = Field(..., ge=0.0, le=1.0)
    freshness_score: float = Field(..., ge=0.0, le=1.0)
    completeness_score: float = Field(..., ge=0.0, le=1.0)
    consistency_score: float = Field(..., ge=0.0, le=1.0)
    provider_scores: Dict[QuoteProvider, float] = Field(default_factory=dict)
    validation_errors: List[str] = Field(default_factory=list)
    confidence_interval: Dict[str, float] = Field(default_factory=dict)


class RiskAssessment(BaseModel):
    """Comprehensive risk assessment"""

    risk_level: str = Field(..., pattern="^(low|medium|high|very_high)$")
    volatility_score: float = Field(..., ge=0.0, le=1.0)
    liquidity_score: float = Field(..., ge=0.0, le=1.0)
    concentration_risk: float = Field(..., ge=0.0, le=1.0)
    market_risk: float = Field(..., ge=0.0, le=1.0)
    specific_risks: List[str] = Field(default_factory=list)
    risk_factors: Dict[str, float] = Field(default_factory=dict)


class OpportunityMetrics(BaseModel):
    """Trading opportunity metrics"""

    opportunity_score: float = Field(..., ge=0.0, le=100.0)
    value_score: float = Field(..., ge=0.0, le=100.0)
    momentum_score: float = Field(..., ge=0.0, le=100.0)
    quality_score: float = Field(..., ge=0.0, le=100.0)
    growth_score: float = Field(..., ge=0.0, le=100.0)
    composite_score: float = Field(..., ge=0.0, le=100.0)
    rank_percentile: float = Field(..., ge=0.0, le=100.0)


class EnrichedQuoteResponse(BaseModel):
    """Enhanced enriched quote response"""

    symbol: str
    quote: UnifiedQuote
    quality: Optional[QuoteQuality] = None
    risk: Optional[RiskAssessment] = None
    opportunity: Optional[OpportunityMetrics] = None
    sources_used: List[QuoteProvider]
    timestamp: datetime
    cache_hit: bool = False
    processing_time_ms: float
    metadata: Dict[str, Any] = Field(default_factory=dict)

    @validator("timestamp")
    def validate_timestamp(cls, v: datetime) -> datetime:
        # Ensure timestamp is recent (within 5 minutes) – just a warning
        now = datetime.now(timezone.utc)
        if v.tzinfo is None:
            delta = (now.replace(tzinfo=None) - v).total_seconds()
        else:
            delta = (now - v.astimezone(timezone.utc)).total_seconds()

        if delta > 300:
            logger.warning(f"Stale timestamp detected in EnrichedQuoteResponse: {v}")
        return v


class BatchQuoteRequest(BaseModel):
    """Batch quote request model"""

    symbols: List[str] = Field(..., max_items=50)
    providers: List[QuoteProvider] = Field(default_factory=list)
    include_quality: bool = True
    include_risk: bool = True
    include_opportunity: bool = True
    force_refresh: bool = False

    @validator("symbols")
    def validate_symbols(cls, v: List[str]) -> List[str]:
        if len(v) > 50:
            raise ValueError("Maximum 50 symbols per batch request")
        return v


class BatchQuoteResponse(BaseModel):
    """Batch quote response"""

    quotes: List[EnrichedQuoteResponse]
    failed_symbols: List[Dict[str, Any]]
    total_symbols: int
    successful_symbols: int
    processing_time_ms: float
    timestamp: datetime


class WebSocketSubscription(BaseModel):
    """WebSocket subscription request"""

    symbols: List[str] = Field(..., max_items=20)
    interval_seconds: int = Field(default=10, ge=5, le=300)
    include_fields: List[str] = Field(default_factory=list)

    @validator("interval_seconds")
    def validate_interval(cls, v: int) -> int:
        if v < 5:
            raise ValueError("Minimum interval is 5 seconds")
        return v


# ---------------------------------------------------------------------------
# Authentication & Rate Limiting
# ---------------------------------------------------------------------------


class EnhancedAuth:
    """Enhanced authentication with simple API-key rate limiting."""

    def __init__(self) -> None:
        self.api_keys: Dict[str, Dict[str, Any]] = {
            "dev_key": {"rate_limit": 100, "role": "developer"},
            "user_key": {"rate_limit": 10, "role": "user"},
            "premium_key": {"rate_limit": 1000, "role": "premium"},
        }

    async def validate_api_key(self, api_key: str = Query(..., alias="token")) -> str:
        """Validate API key and enforce rate limit."""
        if not api_key:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="API key is required",
            )

        if api_key not in self.api_keys:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Invalid API key",
            )

        # Rate limiting per API key
        client_id = api_key
        current_time = time.time()

        if client_id in RATE_LIMIT_CACHE:
            request_times = RATE_LIMIT_CACHE[client_id]
            # Keep only last 60 seconds
            request_times = [t for t in request_times if current_time - t < 60]

            rate_limit = self.api_keys[api_key]["rate_limit"]
            if len(request_times) >= rate_limit:
                raise HTTPException(
                    status_code=status.HTTP_429_TOO_MANY_REQUESTS,
                    detail=f"Rate limit exceeded. {rate_limit} requests per minute.",
                )

            request_times.append(current_time)
            RATE_LIMIT_CACHE[client_id] = request_times
        else:
            RATE_LIMIT_CACHE[client_id] = [current_time]

        return api_key


auth = EnhancedAuth()

# ---------------------------------------------------------------------------
# Quote Cache Manager
# ---------------------------------------------------------------------------


class QuoteCacheManager:
    """Manages quote caching with TTL."""

    def __init__(self) -> None:
        self.cache: TTLCache = QUOTE_CACHE
        self.hit_count: int = 0
        self.miss_count: int = 0

    def _build_key(self, symbol: str, providers: List[str]) -> str:
        return f"{symbol}:{':'.join(sorted(providers))}"

    def get(self, symbol: str, providers: List[str]) -> Optional[UnifiedQuote]:
        """Get quote from cache."""
        cache_key = self._build_key(symbol, providers)
        if cache_key in self.cache:
            self.hit_count += 1
            return self.cache[cache_key]
        self.miss_count += 1
        return None

    def set(self, symbol: str, providers: List[str], quote: UnifiedQuote) -> None:
        """Set quote in cache."""
        cache_key = self._build_key(symbol, providers)
        self.cache[cache_key] = quote

    def get_stats(self) -> Dict[str, Any]:
        """Get cache statistics."""
        total = self.hit_count + self.miss_count
        hit_rate = self.hit_count / total if total > 0 else 0.0
        return {
            "hits": self.hit_count,
            "misses": self.miss_count,
            "hit_rate": hit_rate,
            "cache_size": len(self.cache),
            "cache_ttl": 30,
        }


cache_manager = QuoteCacheManager()


async def update_quote_metrics(symbol: str, providers: List[str], success: bool) -> None:
    """Update Prometheus monitoring metrics."""
    QUOTE_REQUESTS.labels(symbol=symbol, provider=",".join(providers)).inc()
    if not success:
        QUOTE_ERRORS.labels(error_type="fetch_error").inc()


async def process_background_cleanup() -> None:
    """Background cleanup task for rate-limit cache."""
    while True:
        await asyncio.sleep(3600)  # Run every hour
        current_time = time.time()
        for key in list(RATE_LIMIT_CACHE.keys()):
            entries = RATE_LIMIT_CACHE[key]
            cleaned = [t for t in entries if current_time - t < 3600]
            if not cleaned:
                del RATE_LIMIT_CACHE[key]
            else:
                RATE_LIMIT_CACHE[key] = cleaned


# ---------------------------------------------------------------------------
# Helper Functions – Quality, Risk, Opportunity
# ---------------------------------------------------------------------------


def assess_data_quality(quote: UnifiedQuote) -> QuoteQuality:
    """Assess the quality of quote data."""
    # Freshness
    now = datetime.now(timezone.utc)
    last_updated = quote.last_updated
    if isinstance(last_updated, datetime):
        if last_updated.tzinfo is None:
            delta_seconds = (now.replace(tzinfo=None) - last_updated).total_seconds()
        else:
            delta_seconds = (now - last_updated.astimezone(timezone.utc)).total_seconds()
    else:
        # If last_updated is missing or not datetime, treat as stale
        delta_seconds = 999999

    freshness = 1.0 if delta_seconds < 300 else 0.5

    # Completeness
    q_dict = quote.dict()
    completeness = (
        sum(1 for v in q_dict.values() if v is not None) / max(len(q_dict), 1)
    )

    # Provider quality scores (simplified placeholder)
    provider_scores: Dict[QuoteProvider, float] = {}
    if getattr(quote, "provider_sources", None):
        for provider in quote.provider_sources:
            try:
                provider_enum = QuoteProvider(provider)
                provider_scores[provider_enum] = 0.9
            except ValueError:
                # Unknown provider string; ignore for scoring
                continue

    return QuoteQuality(
        overall_score=freshness * 0.4 + completeness * 0.6,
        freshness_score=freshness,
        completeness_score=completeness,
        consistency_score=0.8,  # Would need historical data
        provider_scores=provider_scores,
        validation_errors=[],
        confidence_interval={
            "lower": float(quote.price * 0.95) if quote.price is not None else 0.0,
            "upper": float(quote.price * 1.05) if quote.price is not None else 0.0,
        },
    )


def assess_risk(quote: UnifiedQuote, symbol: str) -> RiskAssessment:
    """Assess trading risk for the symbol (simplified)."""
    volume = getattr(quote, "volume", None)
    volatility = min(volume * 0.001, 1.0) if volume else 0.5
    liquidity_score = 0.7 if volume and volume > 1_000_000 else 0.3

    return RiskAssessment(
        risk_level="medium",
        volatility_score=volatility,
        liquidity_score=liquidity_score,
        concentration_risk=0.3,
        market_risk=0.5,
        specific_risks=[],
        risk_factors={
            "volatility": volatility,
            "liquidity": liquidity_score,
        },
    )


def calculate_opportunity(quote: UnifiedQuote) -> OpportunityMetrics:
    """Calculate opportunity metrics (simplified)."""
    pe = getattr(quote, "pe_ratio", None)
    price_change = getattr(quote, "price_change", None)

    value_score = 70.0 if pe is not None and pe < 15 else 30.0
    momentum_score = 80.0 if price_change is not None and price_change > 0 else 40.0

    composite_score = (value_score + momentum_score + 75.0 + 60.0) / 4.0

    return OpportunityMetrics(
        opportunity_score=(value_score + momentum_score) / 2.0,
        value_score=value_score,
        momentum_score=momentum_score,
        quality_score=75.0,
        growth_score=60.0,
        composite_score=composite_score,
        rank_percentile=65.0,
    )


async def fetch_quote_with_retry(symbol: str, max_retries: int = 3) -> UnifiedQuote:
    """Fetch quote with retry logic."""
    last_error: Optional[Exception] = None

    for attempt in range(max_retries):
        try:
            with QUOTE_LATENCY.time():
                quote = await get_enriched_quote(symbol)
                await update_quote_metrics(symbol, ["default"], success=True)
                return quote
        except Exception as e:
            last_error = e
            logger.warning(f"Attempt {attempt + 1} failed for {symbol}: {e}")
            if attempt < max_retries - 1:
                await asyncio.sleep(1 * (attempt + 1))  # simple backoff

    await update_quote_metrics(symbol, ["default"], success=False)
    if last_error:
        raise last_error
    raise RuntimeError(f"Failed to fetch quote for {symbol} after retries")


# ---------------------------------------------------------------------------
# API Endpoints
# ---------------------------------------------------------------------------


@router.get(
    "/enriched-quote",
    response_model=EnrichedQuoteResponse,
    summary="Get enriched quote with comprehensive analysis",
    description="""
Get a fully-enriched quote for a single symbol with:
- Real-time price data
- Data quality assessment
- Risk analysis
- Opportunity scoring
- Multi-provider consolidation
""",
    response_description="Enriched quote with analysis metrics",
)
async def enriched_quote_endpoint(
    symbol: str = Query(
        ...,
        min_length=1,
        max_length=20,
        description="Ticker symbol, e.g. 1120.SR or MSFT",
        example="2222.SR",
    ),
    providers: List[QuoteProvider] = Query(
        default=[QuoteProvider.GOOGLE_SHEETS, QuoteProvider.ARGAAM],
        description="Data providers to use",
    ),
    include_quality: bool = Query(True, description="Include data quality assessment"),
    include_risk: bool = Query(True, description="Include risk assessment"),
    include_opportunity: bool = Query(True, description="Include opportunity scoring"),
    force_refresh: bool = Query(False, description="Force fresh data fetch (ignore cache)"),
    api_key: str = Depends(auth.validate_api_key),
    background_tasks: Optional[BackgroundTasks] = None,
):
    """
    Enhanced enriched quote endpoint with comprehensive analysis.

    Features:
    - Caching with TTL (30 seconds)
    - Rate limiting per API key
    - Retry logic for failed fetches
    - Detailed metrics and analytics
    - Multiple provider support
    """
    start_time = time.time()

    try:
        provider_values = [p.value for p in providers]

        # Cache
        cache_hit = False
        if not force_refresh:
            cached_quote = cache_manager.get(symbol, provider_values)
            if cached_quote:
                cache_hit = True
                quote = cached_quote
            else:
                quote = await fetch_quote_with_retry(symbol)
                cache_manager.set(symbol, provider_values, quote)
        else:
            quote = await fetch_quote_with_retry(symbol)
            cache_manager.set(symbol, provider_values, quote)

        # Analysis components
        quality = assess_data_quality(quote) if include_quality else None
        risk = assess_risk(quote, symbol) if include_risk else None
        opportunity = calculate_opportunity(quote) if include_opportunity else None

        processing_time = (time.time() - start_time) * 1000.0

        response = EnrichedQuoteResponse(
            symbol=symbol,
            quote=quote,
            quality=quality,
            risk=risk,
            opportunity=opportunity,
            sources_used=providers,
            timestamp=datetime.now(timezone.utc),
            cache_hit=cache_hit,
            processing_time_ms=processing_time,
            metadata={
                "api_key_role": auth.api_keys.get(api_key, {}).get("role", "unknown"),
                "providers_requested": provider_values,
            },
        )

        # async metrics as background task
        if background_tasks is not None:
            background_tasks.add_task(
                update_quote_metrics, symbol, provider_values, True
            )

        return response

    except HTTPException:
        raise
    except ValueError as exc:
        logger.error(f"Validation error for {symbol}: {exc}")
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Invalid symbol or parameters: {exc}",
        )
    except Exception as exc:
        logger.error(
            f"Failed to fetch enriched quote for {symbol}: {exc}", exc_info=True
        )
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to fetch enriched quote. Please try again later.",
        )


@router.post(
    "/enriched-quotes/batch",
    response_model=BatchQuoteResponse,
    summary="Get enriched quotes for multiple symbols",
    description="Batch endpoint for fetching enriched quotes for up to 50 symbols",
)
async def batch_enriched_quotes(
    request: BatchQuoteRequest,
    api_key: str = Depends(auth.validate_api_key),
):
    """Batch endpoint for multiple symbols."""
    start_time = time.time()

    quotes: List[EnrichedQuoteResponse] = []
    failed_symbols: List[Dict[str, Any]] = []

    semaphore = asyncio.Semaphore(10)  # max 10 concurrent fetches

    async def process_symbol(symbol: str) -> Dict[str, Any]:
        async with semaphore:
            try:
                quote_resp = await enriched_quote_endpoint(
                    symbol=symbol,
                    providers=(
                        request.providers
                        or [QuoteProvider.GOOGLE_SHEETS, QuoteProvider.ARGAAM]
                    ),
                    include_quality=request.include_quality,
                    include_risk=request.include_risk,
                    include_opportunity=request.include_opportunity,
                    force_refresh=request.force_refresh,
                    api_key=api_key,
                    background_tasks=None,  # we don't need background tasks per symbol here
                )
                return {"symbol": symbol, "quote": quote_resp, "error": None}
            except Exception as e:
                return {"symbol": symbol, "quote": None, "error": str(e)}

    tasks = [process_symbol(symbol) for symbol in request.symbols]
    results = await asyncio.gather(*tasks)

    for result in results:
        if result["error"]:
            failed_symbols.append(
                {"symbol": result["symbol"], "error": result["error"]}
            )
        else:
            quotes.append(result["quote"])

    processing_time = (time.time() - start_time) * 1000.0

    return BatchQuoteResponse(
        quotes=quotes,
        failed_symbols=failed_symbols,
        total_symbols=len(request.symbols),
        successful_symbols=len(quotes),
        processing_time_ms=processing_time,
        timestamp=datetime.now(timezone.utc),
    )


@router.websocket("/enriched-quotes/ws")
async def enriched_quotes_websocket(websocket: WebSocket, token: str = Query(...)):
    """WebSocket for real-time enriched quotes."""
    await websocket.accept()
    ACTIVE_WEBSOCKETS.inc()

    try:
        # Validate token (API key)
        await auth.validate_api_key(token)

        # Receive subscription request
        data = await websocket.receive_json()
        subscription = WebSocketSubscription(**data)

        logger.info(f"WebSocket subscription: {subscription.symbols}")

        while True:
            for symbol in subscription.symbols:
                try:
                    quote = await get_enriched_quote(symbol)
                    await websocket.send_json(
                        {
                            "symbol": symbol,
                            "quote": quote.dict(),
                            "timestamp": datetime.now(timezone.utc).isoformat(),
                        }
                    )
                    await asyncio.sleep(0.1)  # tiny delay between symbols
                except Exception as e:
                    logger.error(f"WebSocket error for {symbol}: {e}")
                    await websocket.send_json(
                        {
                            "symbol": symbol,
                            "error": str(e),
                            "timestamp": datetime.now(timezone.utc).isoformat(),
                        }
                    )

            await asyncio.sleep(subscription.interval_seconds)

    except WebSocketDisconnect:
        logger.info("WebSocket disconnected")
    except Exception as e:
        logger.error(f"WebSocket error: {e}")
    finally:
        ACTIVE_WEBSOCKETS.dec()


@router.get("/enriched-quote/cache/stats")
async def get_cache_stats(api_key: str = Depends(auth.validate_api_key)):
    """Get cache statistics."""
    return cache_manager.get_stats()


@router.post("/enriched-quote/cache/clear")
async def clear_quote_cache(
    symbols: Optional[List[str]] = Query(
        None, description="Symbols to clear (all if empty)"
    ),
    api_key: str = Depends(auth.validate_api_key),
):
    """Clear quote cache."""
    if not symbols:
        cleared_entries = len(cache_manager.cache)
        cache_manager.cache.clear()
        return {
            "message": "All cache cleared",
            "cleared_entries": cleared_entries,
        }

    cleared = 0
    for symbol in symbols:
        prefix = f"{symbol}:"
        for key in list(cache_manager.cache.keys()):
            if key.startswith(prefix):
                del cache_manager.cache[key]
                cleared += 1

    return {
        "message": f"Cache cleared for {len(symbols)} symbols",
        "cleared_entries": cleared,
    }


@router.get("/enriched-quote/health")
async def quote_health_check():
    """Health check endpoint for enriched quote service."""
    try:
        test_symbol = "2222.SR"
        test_quote = await get_enriched_quote(test_symbol)
        _ = test_quote  # just to avoid lints
        return {
            "status": "healthy",
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "cache_stats": cache_manager.get_stats(),
            "rate_limit_cache_size": len(RATE_LIMIT_CACHE),
            "test_symbol": test_symbol,
            "test_success": True,
            "version": "1.2",
        }
    except Exception as e:
        return {
            "status": "unhealthy",
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "error": str(e),
            "cache_stats": cache_manager.get_stats(),
            "version": "1.2",
        }


# ---------------------------------------------------------------------------
# Error Handler (router-level)
# ---------------------------------------------------------------------------


@router.exception_handler(HTTPException)
async def http_exception_handler(request: Request, exc: HTTPException):
    """Enhanced HTTP exception handler for this router."""
    logger.error(f"[EnrichedQuote] HTTP error {exc.status_code}: {exc.detail}")
    return JSONResponse(
        status_code=exc.status_code,
        content={
            "error": exc.detail,
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "path": str(request.url.path),
            "method": request.method,
        },
    )


# ---------------------------------------------------------------------------
# Startup Hook
# ---------------------------------------------------------------------------


@router.on_event("startup")
async def startup_event():
    """Startup tasks for this router."""
    asyncio.create_task(process_background_cleanup())
    logger.info("Enriched Quote API started with background cleanup task")
