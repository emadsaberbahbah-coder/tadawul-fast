# routes/price_history.py
from __future__ import annotations

from fastapi import APIRouter, Query
from datetime import datetime, timezone
from typing import Optional, Dict, Any

router = APIRouter(prefix="/v1/history", tags=["history"])

@router.get("/health")
async def health() -> Dict[str, Any]:
    return {"status": "ok", "service": "price_history", "time_utc": datetime.now(timezone.utc).isoformat()}

@router.get("/price")
async def price_history(
    symbol: str = Query(..., description="Ticker symbol, e.g. 1120.SR or AAPL.US"),
    period: str = Query("1mo", description="e.g. 1mo, 3mo, 6mo, 1y"),
    interval: str = Query("1d", description="e.g. 1d, 1h"),
) -> Dict[str, Any]:
    # Stub endpoint — extend later (e.g., via yfinance/EODHD/FMP)
    return {
        "symbol": symbol,
        "period": period,
        "interval": interval,
        "data": [],
        "note": "Stub endpoint. Implement provider-backed history later.",
        "time_utc": datetime.now(timezone.utc).isoformat(),
    }
