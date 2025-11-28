from pydantic import BaseModel, Field, validator
from typing import Optional, List, Dict, Any
from datetime import datetime
from enum import Enum

class QuoteStatus(str, Enum):
    OK = "OK"
    NO_DATA = "NO_DATA"
    ERROR = "ERROR"

class Quote(BaseModel):
    """Unified quote model for all endpoints"""
    ticker: str = Field(..., description="Stock ticker symbol")
    status: QuoteStatus = Field(QuoteStatus.OK, description="Quote status")
    price: Optional[float] = Field(None, ge=0, description="Current price")
    previous_close: Optional[float] = Field(None, ge=0, description="Previous close price")
    change_value: Optional[float] = Field(None, description="Change amount")
    change_percent: Optional[float] = Field(None, description="Change percentage")
    volume: Optional[float] = Field(None, ge=0, description="Trading volume")
    market_cap: Optional[float] = Field(None, ge=0, description="Market capitalization")
    open_price: Optional[float] = Field(None, ge=0, description="Open price")
    high_price: Optional[float] = Field(None, ge=0, description="Daily high")
    low_price: Optional[float] = Field(None, ge=0, description="Daily low")
    currency: Optional[str] = Field(None, description="Currency code")
    exchange: Optional[str] = Field(None, description="Stock exchange")
    sector: Optional[str] = Field(None, description="Company sector")
    country: Optional[str] = Field(None, description="Country code")
    provider: Optional[str] = Field(None, description="Data provider")
    as_of: Optional[datetime] = Field(None, description="Quote timestamp")
    message: Optional[str] = Field(None, description="Status message")

    @validator("ticker")
    def validate_ticker(cls, v: str) -> str:
        v = v.strip().upper()
        if not v:
            raise ValueError("Ticker cannot be empty")
        return v

class QuotesResponse(BaseModel):
    """Standardized quotes response"""
    timestamp: datetime = Field(..., description="Response timestamp")
    symbols: List[Quote] = Field(..., description="List of quotes")
    meta: Optional[Dict[str, Any]] = Field(None, description="Response metadata")

class ErrorResponse(BaseModel):
    """Standardized error response"""
    error: str = Field(..., description="Error type")
    message: str = Field(..., description="Error message")
    detail: Optional[Dict[str, Any]] = Field(None, description="Additional details")
    timestamp: datetime = Field(..., description="Error timestamp")
    request_id: Optional[str] = Field(None, description="Request ID for debugging")

class HealthResponse(BaseModel):
    """Health check response"""
    status: str = Field(..., description="Service status")
    version: str = Field(..., description="API version")
    timestamp: datetime = Field(..., description="Current timestamp")
    uptime_seconds: float = Field(..., description="Service uptime")
    dependencies: Dict[str, bool] = Field(..., description="Dependency status")

class QuoteRequest(BaseModel):
    """Quote request model"""
    symbols: List[str] = Field(..., description="List of symbols to fetch")
    cache_ttl: Optional[int] = Field(300, description="Cache TTL in seconds")
    providers: Optional[List[str]] = Field(None, description="Preferred providers")
