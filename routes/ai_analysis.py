# routes/ai_analysis.py
# Enhanced AI & Multi-source Analysis Routes (v1) with improved error handling, validation, and monitoring

from typing import Optional, Any, Dict, List, Union
import logging
from datetime import datetime
from fastapi import APIRouter, HTTPException, Query, status, Depends, Request
from pydantic import BaseModel, Field, validator
from enum import Enum
import asyncio
from functools import wraps
import time

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Configuration & Constants
# ---------------------------------------------------------------------------
MAX_CONCURRENT_REQUESTS = 5
ANALYSIS_TIMEOUT_SECONDS = 30
CACHE_TTL_MINUTES = 15

class AnalysisSource(Enum):
    """Sources for multi-source analysis"""
    GOOGLE_SHEETS = "google_sheets"
    YAHOO_FINANCE = "yahoo_finance"
    ARGAAM = "argaam"
    ALPHA_VANTAGE = "alpha_vantage"
    TADAWUL = "tadawul"

class RecommendationType(Enum):
    """Types of AI recommendations"""
    STRONG_BUY = "strong_buy"
    BUY = "buy"
    HOLD = "hold"
    SELL = "sell"
    STRONG_SELL = "strong_sell"

# ---------------------------------------------------------------------------
# Enhanced Models
# ---------------------------------------------------------------------------

class AIRecommendation(BaseModel):
    """Enhanced AI recommendation model"""
    symbol: str
    recommendation: RecommendationType
    confidence_score: float = Field(..., ge=0.0, le=1.0)
    price_target: Optional[float] = None
    stop_loss: Optional[float] = None
    take_profit: Optional[float] = None
    reasoning: List[str] = []
    time_horizon: str = Field("short_term", pattern="^(short_term|medium_term|long_term)$")
    risk_level: str = Field("medium", pattern="^(low|medium|high)$")
    last_updated: datetime = Field(default_factory=datetime.now)
    sources_used: List[AnalysisSource] = []
    
    @validator('confidence_score')
    def validate_confidence(cls, v):
        if v < 0.5 and v > 0:
            logger.warning(f"Low confidence score: {v}")
        return v

class MultiSourceAnalysis(BaseModel):
    """Enhanced multi-source analysis model"""
    symbol: str
    consensus_price: Optional[float] = None
    price_range: Dict[str, float] = {}  # min, max, average
    source_breakdown: Dict[str, Dict[str, Any]] = {}
    data_freshness: Dict[str, datetime] = {}
    data_quality_score: float = Field(..., ge=0.0, le=1.0)
    discrepancies: List[Dict[str, Any]] = []
    recommendations_summary: Dict[str, int] = {}  # strong_buy: 2, buy: 3, etc.
    last_updated: datetime = Field(default_factory=datetime.now)

class AnalysisRequest(BaseModel):
    """Analysis request with validation"""
    symbol: str = Field(..., min_length=1, max_length=10, pattern="^[A-Z0-9.]+$")
    include_technical: bool = True
    include_fundamental: bool = True
    include_sentiment: bool = False
    sources: List[AnalysisSource] = Field(default_factory=lambda: [
        AnalysisSource.GOOGLE_SHEETS,
        AnalysisSource.ARGAAM
    ])
    force_refresh: bool = False
    
    @validator('symbol')
    def uppercase_symbol(cls, v):
        return v.upper()

class CacheClearRequest(BaseModel):
    """Enhanced cache clearing with safety checks"""
    symbol: Optional[str] = None
    source: Optional[AnalysisSource] = None
    confirm: bool = False  # Require confirmation for bulk clears
    dry_run: bool = False  # Preview what would be cleared
    
    @validator('symbol')
    def validate_symbol_or_source(cls, v, values):
        if v is None and values.get('source') is None and not values.get('confirm'):
            raise ValueError('Bulk cache clear requires confirmation (confirm=true)')
        return v

class CacheStats(BaseModel):
    """Detailed cache statistics"""
    total_entries: int
    entries_by_source: Dict[str, int]
    memory_usage_mb: float
    hit_rate: float
    oldest_entry: Optional[datetime]
    newest_entry: Optional[datetime]
    ttl_minutes: int

# ---------------------------------------------------------------------------
# Enhanced Imports with Fallback
# ---------------------------------------------------------------------------

class MockAnalyzer:
    """Mock analyzer for when real analyzer is unavailable"""
    
    def __init__(self):
        self.cache_stats = {
            "total_entries": 0,
            "entries_by_source": {},
            "memory_usage_mb": 0,
            "hit_rate": 0,
            "oldest_entry": None,
            "newest_entry": None,
            "ttl_minutes": CACHE_TTL_MINUTES
        }
    
    async def get_cache_info(self):
        return self.cache_stats
    
    async def clear_cache(self, symbol=None, source=None):
        return {"status": "success", "cleared": 0, "dry_run": False}

try:
    from advanced_analysis import (
        EnhancedAnalyzer,
        generate_ai_recommendation as generate_ai_recommendation_fn,
        get_multi_source_analysis as get_multi_source_analysis_fn,
        get_analyzer_stats,
        validate_symbol
    )
    analyzer = EnhancedAnalyzer()
    ADVANCED_ANALYSIS_ENABLED = True
    logger.info("✅ Advanced AI analysis engine loaded successfully")
    
except ImportError as e:
    logger.warning(f"⚠️ Advanced analysis module not available: {e}")
    analyzer = MockAnalyzer()
    ADVANCED_ANALYSIS_ENABLED = False
    
    async def generate_ai_recommendation_fn(symbol: str, **kwargs):
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="AI analysis engine is not available on this instance"
        )
    
    async def get_multi_source_analysis_fn(symbol: str, **kwargs):
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Multi-source analysis is not available on this instance"
        )

# ---------------------------------------------------------------------------
# Rate Limiting & Concurrency Control
# ---------------------------------------------------------------------------

class RateLimiter:
    """Simple rate limiter for analysis endpoints"""
    
    def __init__(self, max_requests: int = 5, time_window: int = 60):
        self.max_requests = max_requests
        self.time_window = time_window
        self.requests = {}
    
    async def check_limit(self, client_id: str):
        now = time.time()
        if client_id not in self.requests:
            self.requests[client_id] = []
        
        # Clean old requests
        self.requests[client_id] = [
            req_time for req_time in self.requests[client_id]
            if now - req_time < self.time_window
        ]
        
        if len(self.requests[client_id]) >= self.max_requests:
            raise HTTPException(
                status_code=status.HTTP_429_TOO_MANY_REQUESTS,
                detail=f"Rate limit exceeded. Max {self.max_requests} requests per minute."
            )
        
        self.requests[client_id].append(now)
        return True

rate_limiter = RateLimiter(max_requests=10, time_window=60)

async def get_client_id(request: Request) -> str:
    """Extract client identifier for rate limiting"""
    return request.client.host if request.client else "unknown"

# ---------------------------------------------------------------------------
# Router Definition
# ---------------------------------------------------------------------------

router = APIRouter(
    prefix="/v1/analysis",
    tags=["AI Analysis"],
    responses={
        404: {"description": "Not found"},
        429: {"description": "Rate limit exceeded"},
        503: {"description": "Service unavailable"}
    }
)

# ---------------------------------------------------------------------------
# Enhanced Endpoints
# ---------------------------------------------------------------------------

@router.get("/recommendation", response_model=AIRecommendation)
async def get_ai_recommendation(
    symbol: str = Query(..., min_length=1, description="Ticker symbol, e.g. 2222.SR or AAPL"),
    include_reasoning: bool = Query(True, description="Include detailed reasoning"),
    time_horizon: str = Query("short_term", pattern="^(short_term|medium_term|long_term)$"),
    request: Request = None,
    client_id: str = Depends(get_client_id)
):
    """
    Get AI trading recommendation with enhanced features.
    
    Features:
    - Rate limiting
    - Symbol validation
    - Timeout protection
    - Detailed reasoning
    - Multiple time horizons
    """
    # Apply rate limiting
    await rate_limiter.check_limit(client_id)
    
    # Validate symbol format
    if not await validate_symbol(symbol):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Invalid symbol format: {symbol}. Use format like '2222.SR' or 'AAPL'"
        )
    
    if not ADVANCED_ANALYSIS_ENABLED:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="AI analysis engine is not available on this instance"
        )
    
    try:
        # Run with timeout
        rec = await asyncio.wait_for(
            generate_ai_recommendation_fn(
                symbol=symbol,
                include_reasoning=include_reasoning,
                time_horizon=time_horizon
            ),
            timeout=ANALYSIS_TIMEOUT_SECONDS
        )
        
        # Convert to response model
        return AIRecommendation(**rec.to_dict())
        
    except asyncio.TimeoutError:
        logger.error(f"Analysis timeout for {symbol}")
        raise HTTPException(
            status_code=status.HTTP_504_GATEWAY_TIMEOUT,
            detail="Analysis timeout. Please try again later."
        )
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"AI recommendation failed for {symbol}: {e}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Analysis failed: {str(e)[:100]}..."
        )

@router.post("/batch-recommendations", response_model=List[AIRecommendation])
async def get_batch_recommendations(
    symbols: List[str] = Query(..., description="List of ticker symbols"),
    request: Request = None,
    client_id: str = Depends(get_client_id)
):
    """
    Get AI recommendations for multiple symbols in batch.
    
    Limits:
    - Max 10 symbols per request
    - Rate limited per client
    """
    if len(symbols) > 10:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Maximum 10 symbols per batch request"
        )
    
    await rate_limiter.check_limit(client_id)
    
    results = []
    for symbol in symbols:
        try:
            rec = await get_ai_recommendation(symbol, request=request)
            results.append(rec)
        except HTTPException as e:
            results.append({
                "symbol": symbol,
                "error": e.detail,
                "status_code": e.status_code
            })
    
    return results

@router.get("/multi-source", response_model=MultiSourceAnalysis)
async def get_multi_source_analysis(
    symbol: str = Query(..., min_length=1, description="Ticker symbol"),
    sources: List[AnalysisSource] = Query(
        default=[AnalysisSource.GOOGLE_SHEETS, AnalysisSource.ARGAAM],
        description="Data sources to include"
    ),
    force_refresh: bool = Query(False, description="Force fresh data fetch"),
    client_id: str = Depends(get_client_id)
):
    """
    Get consolidated multi-source analysis with source breakdown.
    
    Returns:
    - Consensus price from all sources
    - Data quality assessment
    - Source-specific data
    - Discrepancy analysis
    """
    await rate_limiter.check_limit(client_id)
    
    if not ADVANCED_ANALYSIS_ENABLED:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Multi-source analysis is not available"
        )
    
    try:
        analysis = await asyncio.wait_for(
            get_multi_source_analysis_fn(
                symbol=symbol,
                sources=[s.value for s in sources],
                force_refresh=force_refresh
            ),
            timeout=ANALYSIS_TIMEOUT_SECONDS
        )
        
        return MultiSourceAnalysis(**analysis)
        
    except asyncio.TimeoutError:
        raise HTTPException(
            status_code=status.HTTP_504_GATEWAY_TIMEOUT,
            detail="Multi-source analysis timeout"
        )
    except Exception as e:
        logger.error(f"Multi-source analysis failed for {symbol}: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Multi-source analysis failed"
        )

@router.get("/cache/info", response_model=CacheStats)
async def get_ai_cache_info():
    """Get detailed cache statistics for the AI analyzer"""
    if not ADVANCED_ANALYSIS_ENABLED:
        return CacheStats(
            total_entries=0,
            entries_by_source={},
            memory_usage_mb=0,
            hit_rate=0,
            oldest_entry=None,
            newest_entry=None,
            ttl_minutes=CACHE_TTL_MINUTES
        )
    
    try:
        info = await analyzer.get_cache_info()
        return CacheStats(**info)
    except Exception as e:
        logger.error(f"Cache info retrieval failed: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Cache info retrieval failed"
        )

@router.post("/cache/clear")
async def clear_ai_cache(
    payload: CacheClearRequest,
    client_id: str = Depends(get_client_id)
):
    """
    Enhanced cache clearing with safety features.
    
    Safety Features:
    - Confirmation required for bulk clears
    - Dry run mode to preview impact
    - Rate limiting
    - Audit logging
    """
    await rate_limiter.check_limit(client_id)
    
    if not ADVANCED_ANALYSIS_ENABLED:
        return {"status": "mock", "cleared": 0}
    
    try:
        # Log cache clear attempts for audit
        logger.info(f"Cache clear requested by {client_id}: {payload.dict()}")
        
        if payload.dry_run:
            # Preview what would be cleared
            preview = await analyzer.get_cache_info()
            return {
                "status": "dry_run",
                "would_clear": preview.get("total_entries", 0),
                "details": preview
            }
        
        result = await analyzer.clear_cache(
            symbol=payload.symbol,
            source=payload.source.value if payload.source else None
        )
        
        logger.info(f"Cache cleared: {result}")
        return result
        
    except Exception as e:
        logger.error(f"Cache clear failed: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Cache clear failed: {str(e)}"
        )

@router.get("/health")
async def analysis_health_check():
    """Health check for AI analysis service"""
    health_status = {
        "service": "ai_analysis",
        "status": "healthy" if ADVANCED_ANALYSIS_ENABLED else "degraded",
        "advanced_analysis_enabled": ADVANCED_ANALYSIS_ENABLED,
        "analyzer_available": analyzer is not None,
        "timestamp": datetime.now().isoformat(),
        "cache_enabled": True,
        "rate_limiter_active": True
    }
    
    if ADVANCED_ANALYSIS_ENABLED:
        try:
            stats = await analyzer.get_cache_info()
            health_status.update({
                "cache_entries": stats.get("total_entries", 0),
                "cache_hit_rate": stats.get("hit_rate", 0)
            })
        except:
            health_status["cache_status"] = "unavailable"
    
    return health_status

@router.get("/symbols/supported")
async def get_supported_symbols(
    exchange: Optional[str] = Query(None, description="Filter by exchange"),
    sector: Optional[str] = Query(None, description="Filter by sector")
):
    """
    Get list of symbols supported by the AI analysis engine.
    
    This endpoint helps clients discover available symbols.
    """
    if not ADVANCED_ANALYSIS_ENABLED:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Analysis engine unavailable"
        )
    
    try:
        # This would come from your analyzer or data service
        supported_symbols = await analyzer.get_supported_symbols(
            exchange=exchange,
            sector=sector
        )
        return {
            "count": len(supported_symbols),
            "symbols": supported_symbols[:100],  # Limit response size
            "total_available": len(supported_symbols)
        }
    except Exception as e:
        logger.error(f"Failed to get supported symbols: {e}")
        return {"count": 0, "symbols": [], "error": str(e)}
