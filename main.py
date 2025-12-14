import logging
import uvicorn
from contextlib import asynccontextmanager
from fastapi import FastAPI, Request, HTTPException, Depends
from fastapi.middleware.cors import CORSMiddleware
from slowapi import Limiter, _rate_limit_exceeded_handler
from slowapi.util import get_remote_address
from slowapi.errors import RateLimitExceeded

# --- Internal Imports ---
# We import the robust configuration and the modern V2 Data Engine
from core.config import get_settings
from core.data_engine_v2 import DataEngine, UnifiedQuote

# 1. Initialize Configuration
settings = get_settings()

# 2. Logging Setup
logging.basicConfig(
    level=settings.LOG_LEVEL,
    format="%(asctime)s - [%(name)s] - %(levelname)s - %(message)s",
)
logger = logging.getLogger("tadawul_fast_bridge")

# --- Lifespan Manager (Startup & Shutdown Logic) ---
@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    Manages the lifecycle of the application.
    Initializes the Data Engine on startup and cleans it up on shutdown.
    """
    # 1. Startup: Initialize Data Engine
    logger.info(f"Starting {settings.APP_NAME} v{settings.APP_VERSION} [{settings.APP_ENV}]")
    app.state.engine = DataEngine()
    logger.info("Data Engine initialized successfully.")
    
    yield
    
    # 2. Shutdown: Close Data Engine connections to prevent memory leaks
    logger.info("Shutting down application...")
    if hasattr(app.state, "engine"):
        await app.state.engine.aclose()
        logger.info("Data Engine connections closed.")

# 3. Initialize FastAPI
app = FastAPI(
    title=settings.APP_NAME,
    version=settings.APP_VERSION,
    lifespan=lifespan,
    docs_url="/docs" if settings.APP_ENV != "production" else None,
    redoc_url=None
)

# 4. Security: Rate Limiting
limiter = Limiter(key_func=get_remote_address)
app.state.limiter = limiter
app.add_exception_handler(RateLimitExceeded, _rate_limit_exceeded_handler)

# 5. Connectivity: CORS Middleware
if settings.ENABLE_CORS_ALL_ORIGINS:
    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )
    logger.info("CORS enabled for all origins.")

# --- Helper: Get Engine Dependency ---
def get_engine(request: Request) -> DataEngine:
    """Dependency to retrieve the active DataEngine instance."""
    return request.app.state.engine

# --- Routes ---

@app.get("/health", tags=["System"])
async def health_check():
    """
    Health check endpoint for Render.
    Returns 200 OK if the service is running.
    """
    return {
        "status": "healthy",
        "env": settings.APP_ENV,
        "provider": settings.PRIMARY_PROVIDER,
        "version": settings.APP_VERSION
    }

@app.get("/", tags=["System"])
@limiter.limit("10/minute")
async def root(request: Request):
    """
    Root endpoint providing service info.
    """
    return {
        "service": settings.APP_NAME,
        "version": settings.APP_VERSION,
        "status": "online",
        "docs": "disabled" if settings.APP_ENV == "production" else "/docs"
    }

@app.get("/api/v1/market-data", response_model=UnifiedQuote, tags=["Market Data"])
@limiter.limit("60/minute")
async def get_market_data(
    request: Request, 
    ticker: str, 
    engine: DataEngine = Depends(get_engine)
):
    """
    Fetches real-time market data for a specific ticker.
    - Connects to the V2 Data Engine.
    - Supports KSA (Tadawul) and Global markets automatically.
    - Falls back to Argaam -> EODHD -> FMP -> Yahoo.
    """
    if not ticker:
        raise HTTPException(status_code=400, detail="Ticker symbol is required")

    try:
        # Call the engine to get data
        quote = await engine.get_quote(ticker)
        
        # Log if we missed data, but still return the object (don't crash)
        if quote.data_quality == "MISSING":
            logger.warning(f"No data found for {ticker}")
            
        return quote

    except Exception as e:
        logger.error(f"Error fetching data for {ticker}: {str(e)}")
        # In production, we might want to return a cleaner error message
        raise HTTPException(status_code=500, detail=str(e))

# --- Future Routes (Uncomment as we modernize these files) ---
# from routes import advanced_analysis
# from routes import ai_analysis
# app.include_router(advanced_analysis.router, prefix="/api/v1/analysis", tags=["Analysis"])
# app.include_router(ai_analysis.router, prefix="/api/v1/ai", tags=["AI"])

# 6. Execution Entry Point
if __name__ == "__main__":
    # This block is used for local debugging
    uvicorn.run(
        "main:app",
        host="0.0.0.0",
        port=10000,
        log_level=settings.LOG_LEVEL.lower(),
        reload=True
    )
