from typing import Optional, List, Dict, Any
from pydantic import BaseModel, Field, ConfigDict
from datetime import datetime

# --- Base Configuration (Pydantic V2) ---
class BaseSchema(BaseModel):
    model_config = ConfigDict(
        populate_by_name=True,
        from_attributes=True,
        validate_assignment=True
    )

# --- Common Models ---

class TickerRequest(BaseSchema):
    ticker: str = Field(..., description="The stock symbol (e.g., 1120.SR)")
    provider: Optional[str] = Field("eodhd", description="Data provider to use")

class MarketData(BaseSchema):
    symbol: str
    price: float
    currency: str = "SAR"
    change: Optional[float] = 0.0
    change_percent: Optional[float] = 0.0
    volume: Optional[int] = 0
    timestamp: datetime = Field(default_factory=datetime.now)

# --- Analysis Models ---

class AIAnalysisResponse(BaseSchema):
    symbol: str
    recommendation: str = Field(..., description="Buy, Sell, or Hold")
    confidence_score: float = Field(..., ge=0, le=100)
    reasoning: str
    generated_at: datetime = Field(default_factory=datetime.now)

class ScoredQuote(BaseSchema):
    symbol: str
    market_data: MarketData
    technical_score: float = 0.0
    fundamental_score: float = 0.0
    final_score: float = 0.0
    signal: str = "NEUTRAL"

# --- Batch Models ---

class BatchProcessRequest(BaseSchema):
    symbols: List[str]
    operation: str = "full_scan"  # full_scan, quick_price, ai_analysis
