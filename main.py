import os
import logging
import time
from contextlib import asynccontextmanager
from typing import Dict, Any

import uvicorn
from fastapi import FastAPI, Request, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from slowapi import Limiter, _rate_limit_exceeded_handler
from slowapi.util import get_remote_address
from slowapi.errors import RateLimitExceeded
from dotenv import load_dotenv

# 1. Load Environment Variables
load_dotenv()

# 2. Configuration & Logging Setup
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
logging.basicConfig(
    level=LOG_LEVEL,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger("tadawul_fast_bridge")

# 3. Lifespan Manager (Startup/Shutdown logic)
@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup
    logger.info(f"Starting {os.getenv('APP_NAME', 'Tadawul Fast Bridge')} v{os.getenv('APP_VERSION', '4.0.0')}")
    yield
    # Shutdown
    logger.info("Shutting down application...")

# 4. Initialize FastAPI
app = FastAPI(
    title=os.getenv("APP_NAME", "Tadawul Fast Bridge"),
    version=os.getenv("APP_VERSION", "4.0.0"),
    lifespan=lifespan,
    docs_url="/docs" if os.getenv("APP_ENV") != "production" else None,
    redoc_url=None
)

# 5. Rate Limiting Setup
limiter = Limiter(key_func=get_remote_address)
app.state.limiter = limiter
app.add_exception_handler(RateLimitExceeded, _rate_limit_exceeded_handler)

# 6. Middleware Setup (CORS)
if os.getenv("ENABLE_CORS_ALL_ORIGINS", "false").lower() == "true":
    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )
    logger.info("CORS enabled for all origins.")

# 7. Routes

@app.get("/health", tags=["System"])
async def health_check():
    """
    Health check endpoint for Render.
    """
    return {"status": "healthy", "timestamp": time.time()}

@app.get("/", tags=["System"])
@limiter.limit("10/minute")
async def root(request: Request):
    """
    Root endpoint providing service info.
    """
    return {
        "service": os.getenv("APP_NAME", "Tadawul Fast Bridge"),
        "version": os.getenv("APP_VERSION", "4.0.0"),
        "status": "online",
        "docs": "/docs" if os.getenv("APP_ENV") != "production" else "disabled"
    }

# Stub for Market Data (Place your V2 engine logic here later)
@app.get("/api/v1/market-data", tags=["Market Data"])
@limiter.limit("60/minute")
async def get_market_data(request: Request, ticker: str = None):
    if not ticker:
        return {"error": "Ticker symbol required"}
    
    # Placeholder for EODHD/YFinance/FMP logic
    logger.info(f"Received request for ticker: {ticker}")
    return {
        "ticker": ticker,
        "provider": os.getenv("PRIMARY_PROVIDER", "eodhd"),
        "price": 0.00,  # Replace with real data fetching
        "currency": "SAR"
    }

# 8. Execution Entry Point
if __name__ == "__main__":
    port = int(os.getenv("PORT", 10000))
    uvicorn.run(
        "main:app",
        host="0.0.0.0",
        port=port,
        log_level=LOG_LEVEL.lower(),
        reload=os.getenv("APP_ENV") != "production"
    )
