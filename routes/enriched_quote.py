# routes/enriched_quote.py
"""
Enhanced Enriched Quote Routes – v1.1

Key Features:
- Advanced authentication with rate limiting
- Caching with TTL for performance
- Comprehensive error handling with retries
- Detailed metrics and telemetry
- Support for batch requests
- WebSocket for real-time updates
- Health monitoring and diagnostics
"""

from __future__ import annotations

import asyncio
import time
from typing import List, Optional, Dict, Any, Union
from datetime import datetime, timedelta
from enum import Enum

from fastapi import (
    APIRouter, Depends, HTTPException, Query, status, 
    WebSocket, WebSocketDisconnect, BackgroundTasks
)
from fastapi.responses import JSONResponse
from fastapi.security import APIKeyQuery
from pydantic import BaseModel, Field, validator
from cachetools import TTLCache
import httpx
import logging
from prometheus_client import Counter, Histogram, Gauge

from core.data_engine import (
    get_enriched_quote, 
    get_batch_enriched_quotes,
    UnifiedQuote,
    DataQualityScore,
    ProviderSource
)

# Configure structured logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Prometheus metrics for monitoring
QUOTE_REQUESTS = Counter('quote_requests_total', 'Total quote requests', ['symbol', 'provider'])
QUOTE_LATENCY = Histogram('quote_request_latency_seconds', 'Quote request latency')
QUOTE_ERRORS = Counter('quote_errors_total', 'Total quote errors', ['error_type'])
ACTIVE_WEBSOCKETS = Gauge('active_websockets', 'Active WebSocket connections')

# Rate limiting cache (per IP/symbol)
RATE_LIMIT_CACHE = TTLCache(maxsize=10000, ttl=60)

# Response cache (per symbol)
QUOTE_CACHE = TTLCache(maxsize=1000, ttl=30)  # 30 seconds cache

router = APIRouter(
    prefix="/v1",
    tags=["enriched_quote"],
    responses={
        400: {"description": "Bad Request"},
        401: {"description": "Unauthorized"},
        429: {"description": "Too Many Requests"},
        500: {"description": "Internal Server Error"}
    }
)

# ---------------------------------------------------------------------------
# Enhanced Models
# ---------------------------------------------------------------------------

class QuoteProvider(Enum):
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
    provider_scores: Dict[QuoteProvider, float] = {}
    validation_errors: List[str] = []
    confidence_interval: Dict[str, float] = {}

class RiskAssessment(BaseModel):
    """Comprehensive risk assessment"""
    risk_level: str = Field(..., pattern="^(low|medium|high|very_high)$")
    volatility_score: float = Field(..., ge=0.0, le=1.0)
    liquidity_score: float = Field(..., ge=0.0, le=1.0)
    concentration_risk: float = Field(..., ge=0.0, le=1.0)
    market_risk: float = Field(..., ge=0.0, le=1.0)
    specific_risks: List[str] = []
    risk_factors: Dict[str, float] = {}

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
    quality: QuoteQuality
    risk: RiskAssessment
    opportunity: OpportunityMetrics
    sources_used: List[QuoteProvider]
    timestamp: datetime
    cache_hit: bool = False
    processing_time_ms: float
    metadata: Dict[str, Any] = {}
    
    @validator('timestamp')
    def validate_timestamp(cls, v):
        # Ensure timestamp is recent (within 5 minutes)
        if (datetime.utcnow() - v).total_seconds() > 300:
            logger.warning(f"Stale timestamp detected: {v}")
        return v

class BatchQuoteRequest(BaseModel):
    """Batch quote request model"""
    symbols: List[str] = Field(..., max_items=50)
    providers: List[QuoteProvider] = []
    include_quality: bool = True
    include_risk: bool = True
    include_opportunity: bool = True
    force_refresh: bool = False
    
    @validator('symbols')
    def validate_symbols(cls, v):
        if len(v) > 50:
            raise ValueError('Maximum 50 symbols per batch request')
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
    include_fields: List[str] = []
    
    @validator('interval_seconds')
    def validate_interval(cls, v):
        if v < 5:
            raise ValueError('Minimum interval is 5 seconds')
        return v

# ---------------------------------------------------------------------------
# Authentication & Rate Limiting
# ---------------------------------------------------------------------------

class EnhancedAuth:
    """Enhanced authentication with rate limiting"""
    
    def __init__(self):
        self.api_keys = {
            "dev_key": {"rate_limit": 100, "role": "developer"},
            "user_key": {"rate_limit": 10, "role": "user"},
            "premium_key": {"rate_limit": 1000, "role": "premium"}
        }
    
    async def validate_api_key(self, api_key: str = Query(..., alias="token")):
        """Validate API key with rate limiting"""
        if not api_key:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="API key is required"
            )
        
        if api_key not in self.api_keys:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Invalid API key"
            )
        
        # Rate limiting check
        client_id = f"{api_key}"
        current_time = time.time()
        
        if client_id in RATE_LIMIT_CACHE:
            request_times = RATE_LIMIT_CACHE[client_id]
            # Clean old requests (last minute)
            request_times = [t for t in request_times if current_time - t < 60]
            
            rate_limit = self.api_keys[api_key]["rate_limit"]
            if len(request_times) >= rate_limit:
                raise HTTPException(
                    status_code=status.HTTP_429_TOO_MANY_REQUESTS,
                    detail=f"Rate limit exceeded. {rate_limit} requests per minute."
                )
            
            request_times.append(current_time)
            RATE_LIMIT_CACHE[client_id] = request_times
        else:
            RATE_LIMIT_CACHE[client_id] = [current_time]
        
        return api_key

auth = EnhancedAuth()

# ---------------------------------------------------------------------------
# Background Tasks & Caching
# ---------------------------------------------------------------------------

class QuoteCacheManager:
    """Manages quote caching with TTL"""
    
    def __init__(self):
        self.cache = QUOTE_CACHE
        self.hit_count = 0
        self.miss_count = 0
    
    def get(self, symbol: str, providers: List[str]) -> Optional[UnifiedQuote]:
        """Get quote from cache"""
        cache_key = f"{symbol}:{':'.join(sorted(providers))}"
        if cache_key in self.cache:
            self.hit_count += 1
            return self.cache[cache_key]
        self.miss_count += 1
        return None
    
    def set(self, symbol: str, providers: List[str], quote: UnifiedQuote):
        """Set quote in cache"""
        cache_key = f"{symbol}:{':'.join(sorted(providers))}"
        self.cache[cache_key] = quote
    
    def get_stats(self) -> Dict[str, Any]:
        """Get cache statistics"""
        hit_rate = self.hit_count / max(self.hit_count + self.miss_count, 1)
        return {
            "hits": self.hit_count,
            "misses": self.miss_count,
            "hit_rate": hit_rate,
            "cache_size": len(self.cache),
            "cache_ttl": 30
        }

cache_manager = QuoteCacheManager()

async def update_quote_metrics(symbol: str, providers: List[str], success: bool):
    """Update monitoring metrics"""
    QUOTE_REQUESTS.labels(symbol=symbol, provider=','.join(providers)).inc()
    if not success:
        QUOTE_ERRORS.labels(error_type='fetch_error').inc()

async def process_background_cleanup():
    """Background cleanup task"""
    while True:
        await asyncio.sleep(3600)  # Run every hour
        # Clean old rate limit entries
        current_time = time.time()
        for key in list(RATE_LIMIT_CACHE.keys()):
            entries = RATE_LIMIT_CACHE[key]
            cleaned = [t for t in entries if current_time - t < 3600]
            if not cleaned:
                del RATE_LIMIT_CACHE[key]
            else:
                RATE_LIMIT_CACHE[key] = cleaned

# ---------------------------------------------------------------------------
# Helper Functions
# ---------------------------------------------------------------------------

def assess_data_quality(quote: UnifiedQuote) -> QuoteQuality:
    """Assess the quality of quote data"""
    # Calculate various quality scores
    freshness = 1.0 if (datetime.utcnow() - quote.last_updated).seconds < 300 else 0.5
    
    completeness = sum(1 for v in quote.dict().values() if v is not None) / len(quote.dict())
    
    # Provider quality scores (simplified)
    provider_scores = {}
    if quote.provider_sources:
        for provider in quote.provider_sources:
            provider_scores[QuoteProvider(provider)] = 0.9  # Would be calculated based on provider reliability
    
    return QuoteQuality(
        overall_score=freshness * 0.4 + completeness * 0.6,
        freshness_score=freshness,
        completeness_score=completeness,
        consistency_score=0.8,  # Would need historical data
        provider_scores=provider_scores,
        validation_errors=[],
        confidence_interval={"lower": quote.price * 0.95, "upper": quote.price * 1.05}
    )

def assess_risk(quote: UnifiedQuote, symbol: str) -> RiskAssessment:
    """Assess trading risk for the symbol"""
    # Simplified risk assessment
    volatility = min(quote.volume * 0.001, 1.0) if quote.volume else 0.5
    
    return RiskAssessment(
        risk_level="medium",
        volatility_score=volatility,
        liquidity_score=0.7 if quote.volume and quote.volume > 1000000 else 0.3,
        concentration_risk=0.3,
        market_risk=0.5,
        specific_risks=[],
        risk_factors={
            "volatility": volatility,
            "liquidity": 0.7 if quote.volume and quote.volume > 1000000 else 0.3
        }
    )

def calculate_opportunity(quote: UnifiedQuote) -> OpportunityMetrics:
    """Calculate opportunity metrics"""
    # Simplified opportunity calculation
    value_score = 70 if quote.pe_ratio and quote.pe_ratio < 15 else 30
    momentum_score = 80 if quote.price_change and quote.price_change > 0 else 40
    
    return OpportunityMetrics(
        opportunity_score=(value_score + momentum_score) / 2,
        value_score=value_score,
        momentum_score=momentum_score,
        quality_score=75,
        growth_score=60,
        composite_score=68.75,
        rank_percentile=65.0
    )

async def fetch_quote_with_retry(symbol: str, max_retries: int = 3) -> UnifiedQuote:
    """Fetch quote with retry logic"""
    last_error = None
    
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
                await asyncio.sleep(1 * (attempt + 1))  # Exponential backoff
    
    await update_quote_metrics(symbol, ["default"], success=False)
    raise last_error

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
    response_description="Enriched quote with analysis metrics"
)
async def enriched_quote_endpoint(
    symbol: str = Query(..., 
                       min_length=1, 
                       max_length=20,
                       description="Ticker symbol, e.g. 1120.SR or MSFT",
                       example="2222.SR"),
    providers: List[QuoteProvider] = Query(
        default=[QuoteProvider.GOOGLE_SHEETS, QuoteProvider.ARGAAM],
        description="Data providers to use"
    ),
    include_quality: bool = Query(True, description="Include data quality assessment"),
    include_risk: bool = Query(True, description="Include risk assessment"),
    include_opportunity: bool = Query(True, description="Include opportunity scoring"),
    force_refresh: bool = Query(False, description="Force fresh data fetch (ignore cache)"),
    api_key: str = Depends(auth.validate_api_key),
    background_tasks: BackgroundTasks = None
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
        # Check cache first (unless force refresh)
        cache_hit = False
        if not force_refresh:
            cached_quote = cache_manager.get(symbol, [p.value for p in providers])
            if cached_quote:
                cache_hit = True
                quote = cached_quote
            else:
                quote = await fetch_quote_with_retry(symbol)
                cache_manager.set(symbol, [p.value for p in providers], quote)
        else:
            quote = await fetch_quote_with_retry(symbol)
            cache_manager.set(symbol, [p.value for p in providers], quote)
        
        # Generate analysis metrics
        quality = assess_data_quality(quote) if include_quality else None
        risk = assess_risk(quote, symbol) if include_risk else None
        opportunity = calculate_opportunity(quote) if include_opportunity else None
        
        processing_time = (time.time() - start_time) * 1000
        
        response = EnrichedQuoteResponse(
            symbol=symbol,
            quote=quote,
            quality=quality,
            risk=risk,
            opportunity=opportunity,
            sources_used=providers,
            timestamp=datetime.utcnow(),
            cache_hit=cache_hit,
            processing_time_ms=processing_time,
            metadata={
                "api_key_role": auth.api_keys.get(api_key, {}).get("role", "unknown"),
                "providers_requested": [p.value for p in providers]
            }
        )
        
        # Add background task for analytics
        if background_tasks:
            background_tasks.add_task(update_quote_metrics, symbol, [p.value for p in providers], True)
        
        return response
        
    except HTTPException:
        raise
    except ValueError as exc:
        logger.error(f"Validation error for {symbol}: {exc}")
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Invalid symbol or parameters: {exc}"
        )
    except Exception as exc:
        logger.error(f"Failed to fetch enriched quote for {symbol}: {exc}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to fetch enriched quote. Please try again later."
        )

@router.post(
    "/enriched-quotes/batch",
    response_model=BatchQuoteResponse,
    summary="Get enriched quotes for multiple symbols",
    description="Batch endpoint for fetching enriched quotes for up to 50 symbols"
)
async def batch_enriched_quotes(
    request: BatchQuoteRequest,
    api_key: str = Depends(auth.validate_api_key)
):
    """Batch endpoint for multiple symbols"""
    start_time = time.time()
    
    quotes = []
    failed_symbols = []
    
    # Process symbols concurrently with semaphore to limit concurrency
    semaphore = asyncio.Semaphore(10)  # Max 10 concurrent fetches
    
    async def process_symbol(symbol: str):
        async with semaphore:
            try:
                quote_resp = await enriched_quote_endpoint(
                    symbol=symbol,
                    providers=request.providers or [QuoteProvider.GOOGLE_SHEETS, QuoteProvider.ARGAAM],
                    include_quality=request.include_quality,
                    include_risk=request.include_risk,
                    include_opportunity=request.include_opportunity,
                    force_refresh=request.force_refresh,
                    api_key=api_key
                )
                return {"symbol": symbol, "quote": quote_resp, "error": None}
            except Exception as e:
                return {"symbol": symbol, "quote": None, "error": str(e)}
    
    # Process all symbols
    tasks = [process_symbol(symbol) for symbol in request.symbols]
    results = await asyncio.gather(*tasks)
    
    for result in results:
        if result["error"]:
            failed_symbols.append({
                "symbol": result["symbol"],
                "error": result["error"]
            })
        else:
            quotes.append(result["quote"])
    
    processing_time = (time.time() - start_time) * 1000
    
    return BatchQuoteResponse(
        quotes=quotes,
        failed_symbols=failed_symbols,
        total_symbols=len(request.symbols),
        successful_symbols=len(quotes),
        processing_time_ms=processing_time,
        timestamp=datetime.utcnow()
    )

@router.websocket("/enriched-quotes/ws")
async def enriched_quotes_websocket(
    websocket: WebSocket,
    token: str = Query(...)
):
    """WebSocket for real-time enriched quotes"""
    await websocket.accept()
    ACTIVE_WEBSOCKETS.inc()
    
    try:
        # Validate token
        if not await auth.validate_api_key(token):
            await websocket.close(code=status.WS_1008_POLICY_VIOLATION)
            return
        
        # Receive subscription request
        data = await websocket.receive_json()
        subscription = WebSocketSubscription(**data)
        
        logger.info(f"WebSocket subscription: {subscription.symbols}")
        
        # Start streaming updates
        while True:
            for symbol in subscription.symbols:
                try:
                    quote = await get_enriched_quote(symbol)
                    
                    # Send update
                    await websocket.send_json({
                        "symbol": symbol,
                        "quote": quote.dict(),
                        "timestamp": datetime.utcnow().isoformat()
                    })
                    
                    # Add small delay between symbols
                    await asyncio.sleep(0.1)
                    
                except Exception as e:
                    logger.error(f"WebSocket error for {symbol}: {e}")
                    await websocket.send_json({
                        "symbol": symbol,
                        "error": str(e),
                        "timestamp": datetime.utcnow().isoformat()
                    })
            
            # Wait for next update cycle
            await asyncio.sleep(subscription.interval_seconds)
            
    except WebSocketDisconnect:
        logger.info("WebSocket disconnected")
    except Exception as e:
        logger.error(f"WebSocket error: {e}")
    finally:
        ACTIVE_WEBSOCKETS.dec()

@router.get("/enriched-quote/cache/stats")
async def get_cache_stats(api_key: str = Depends(auth.validate_api_key)):
    """Get cache statistics"""
    return cache_manager.get_stats()

@router.post("/enriched-quote/cache/clear")
async def clear_quote_cache(
    symbols: List[str] = Query(None, description="Symbols to clear (all if empty)"),
    api_key: str = Depends(auth.validate_api_key)
):
    """Clear quote cache"""
    if not symbols:
        cache_manager.cache.clear()
        return {"message": "All cache cleared", "cleared_entries": len(cache_manager.cache)}
    else:
        cleared = 0
        for symbol in symbols:
            # Clear all provider combinations for this symbol
            for key in list(cache_manager.cache.keys()):
                if key.startswith(f"{symbol}:"):
                    del cache_manager.cache[key]
                    cleared += 1
        return {"message": f"Cache cleared for {len(symbols)} symbols", "cleared_entries": cleared}

@router.get("/enriched-quote/health")
async def quote_health_check():
    """Health check endpoint"""
    try:
        # Test with a known symbol
        test_quote = await get_enriched_quote("2222.SR")
        
        return {
            "status": "healthy",
            "timestamp": datetime.utcnow().isoformat(),
            "cache_stats": cache_manager.get_stats(),
            "rate_limit_cache_size": len(RATE_LIMIT_CACHE),
            "test_symbol": "2222.SR",
            "test_success": True,
            "version": "1.1"
        }
    except Exception as e:
        return {
            "status": "unhealthy",
            "timestamp": datetime.utcnow().isoformat(),
            "error": str(e),
            "cache_stats": cache_manager.get_stats(),
            "version": "1.1"
        }

# ---------------------------------------------------------------------------
# Error Handlers
# ---------------------------------------------------------------------------

@router.exception_handler(HTTPException)
async def http_exception_handler(request, exc):
    """Enhanced HTTP exception handler"""
    logger.error(f"HTTP error {exc.status_code}: {exc.detail}")
    return JSONResponse(
        status_code=exc.status_code,
        content={
            "error": exc.detail,
            "timestamp": datetime.utcnow().isoformat(),
            "path": request.url.path,
            "method": request.method
        }
    )

# Start background cleanup task on startup
@router.on_event("startup")
async def startup_event():
    """Startup tasks"""
    asyncio.create_task(process_background_cleanup())
    logger.info("Enriched Quote API started")
