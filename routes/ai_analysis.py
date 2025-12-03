# routes/ai_analysis.py
# AI & Multi-source Analysis Routes (v1)

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel
from typing import Optional, Any, Dict

from advanced_analysis import (
    analyzer,
    generate_ai_recommendation as generate_ai_recommendation_fn,
    get_multi_source_analysis as get_multi_source_analysis_fn,
)

router = APIRouter(
    prefix="/v1/ai",
    tags=["AI Analysis"],
)


class ClearCacheRequest(BaseModel):
    symbol: Optional[str] = None


@router.get("/recommendation")
async def get_ai_recommendation(symbol: str = Query(..., min_length=1)):
    """
    Get AI trading recommendation for a single symbol.
    Example: /v1/ai/recommendation?symbol=AAPL
    """
    if analyzer is None:
        raise HTTPException(status_code=500, detail="Analyzer not initialized")

    try:
        rec = await generate_ai_recommendation_fn(symbol)
        return rec.to_dict()
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"AI recommendation failed: {e}")


@router.get("/multi-source")
async def get_multi_source(symbol: str = Query(..., min_length=1)):
    """
    Get consolidated multi-source price/quote view for a symbol.
    Example: /v1/ai/multi-source?symbol=MSFT
    """
    if analyzer is None:
        raise HTTPException(status_code=500, detail="Analyzer not initialized")

    try:
        analysis = await get_multi_source_analysis_fn(symbol)
        return analysis
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Multi-source analysis failed: {e}")


@router.get("/cache/info")
async def get_ai_cache_info() -> Dict[str, Any]:
    """
    Get cache statistics for the AI analyzer (file + Redis if enabled).
    """
    if analyzer is None:
        raise HTTPException(status_code=500, detail="Analyzer not initialized")

    try:
        info = await analyzer.get_cache_info()
        return info
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Cache info retrieval failed: {e}")


@router.post("/cache/clear")
async def clear_ai_cache(payload: ClearCacheRequest):
    """
    Clear AI analyzer cache.
    - If payload.symbol is provided → clear only that symbol.
    - If payload.symbol is null → clear all entries.
    """
    if analyzer is None:
        raise HTTPException(status_code=500, detail="Analyzer not initialized")

    try:
        result = await analyzer.clear_cache(symbol=payload.symbol)
        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Cache clear failed: {e}")
