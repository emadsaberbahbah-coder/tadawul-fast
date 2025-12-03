# routes/ai_analysis.py
# AI & Multi-source Analysis Routes (v1)

from typing import Optional, Any, Dict

import logging
from fastapi import APIRouter, HTTPException, Query, status
from pydantic import BaseModel

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Safe import of advanced_analysis
# ---------------------------------------------------------------------------
try:
    from advanced_analysis import (
        analyzer,
        generate_ai_recommendation as generate_ai_recommendation_fn,
        get_multi_source_analysis as get_multi_source_analysis_fn,
    )
except Exception as e:
    # If anything goes wrong, keep the API alive but mark analyzer as unavailable
    logger.warning("⚠️ AI analysis engine is not available: %s", e)
    analyzer = None

    async def generate_ai_recommendation_fn(symbol: str):
        raise RuntimeError("Analysis engine is not available on this instance")

    async def get_multi_source_analysis_fn(symbol: str):
        raise RuntimeError("Analysis engine is not available on this instance")


# ---------------------------------------------------------------------------
# Router Definition
# ---------------------------------------------------------------------------

router = APIRouter(
    prefix="/v1/analysis",   # ✅ matches main root() endpoint listing
    tags=["AI Analysis"],
)


class ClearCacheRequest(BaseModel):
    """
    Optional symbol filter for cache clearing.

    - symbol = None  -> clear ALL cache
    - symbol = "AAPL" -> clear only AAPL
    """
    symbol: Optional[str] = None


def _ensure_analyzer_available() -> None:
    """
    Central check that the analyzer is available.

    NOTE:
    - We don't read ADVANCED_ANALYSIS_ENABLED here to avoid circular imports.
      The main service already guards startup; this layer just checks analyzer.
    """
    if analyzer is None:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Analysis engine is not available on this instance",
        )


# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------

@router.get("/recommendation")
async def get_ai_recommendation(symbol: str = Query(..., min_length=1, description="Ticker symbol, e.g. AAPL")):
    """
    Get AI trading recommendation for a single symbol.

    Example:
      /v1/analysis/recommendation?symbol=AAPL
    """
    _ensure_analyzer_available()

    try:
        rec = await generate_ai_recommendation_fn(symbol)
        # rec is expected to be a rich object with .to_dict()
        return rec.to_dict()
    except HTTPException:
        # pass through explicit HTTP errors unchanged
        raise
    except Exception as e:
        logger.error("AI recommendation failed for %s: %s", symbol, e, exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="AI recommendation failed. Please try again later.",
        )


@router.get("/multi-source")
async def get_multi_source(symbol: str = Query(..., min_length=1, description="Ticker symbol, e.g. MSFT")):
    """
    Get consolidated multi-source price/quote view for a symbol.

    Example:
      /v1/analysis/multi-source?symbol=MSFT
    """
    _ensure_analyzer_available()

    try:
        analysis = await get_multi_source_analysis_fn(symbol)
        # analysis is expected to already be JSON-serializable (dict)
        return analysis
    except HTTPException:
        raise
    except Exception as e:
        logger.error("Multi-source analysis failed for %s: %s", symbol, e, exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Multi-source analysis failed. Please try again later.",
        )


@router.get("/cache/info")
async def get_ai_cache_info() -> Dict[str, Any]:
    """
    Get cache statistics for the AI analyzer (file + Redis if enabled).
    """
    _ensure_analyzer_available()

    try:
        info = await analyzer.get_cache_info()
        return info
    except HTTPException:
        raise
    except Exception as e:
        logger.error("Cache info retrieval failed: %s", e, exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Cache info retrieval failed. Please try again later.",
        )


@router.post("/cache/clear")
async def clear_ai_cache(payload: ClearCacheRequest):
    """
    Clear AI analyzer cache.

    - If payload.symbol is provided → clear only that symbol.
    - If payload.symbol is null → clear all entries.
    """
    _ensure_analyzer_available()

    try:
        result = await analyzer.clear_cache(symbol=payload.symbol)
        return result
    except HTTPException:
        raise
    except Exception as e:
        logger.error("Cache clear failed (symbol=%s): %s", payload.symbol, e, exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Cache clear failed. Please try again later.",
        )
