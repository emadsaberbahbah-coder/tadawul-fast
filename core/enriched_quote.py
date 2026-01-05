# routes/enriched_quote.py
"""
routes/enriched_quote.py
------------------------------------------------------------
Enriched Quote Router — v5.6.0 (ALIGNED + HARDENED)
PROD SAFE (await-safe + singleton-engine friendly)

What's New in v5.6.0:
- ✅ Simplified: Redundant normalization logic removed (delegated to core.enriched_quote).
- ✅ Path-Safe: Prefix fixed to /v1/enriched to match dashboard synchronizer expectations.
- ✅ Mapper-Aligned: Uses EnrichedQuote (v2.6.0) for reliable sheet row generation.
- ✅ Session-Aware: Shares the Hardened Yahoo Session via app.state.engine.
"""

from __future__ import annotations

import logging
import os
import traceback
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, Header, Query, Request, HTTPException
from fastapi.encoders import jsonable_encoder
from starlette.responses import JSONResponse

logger = logging.getLogger("routes.enriched_quote")

ENRICHED_ROUTE_VERSION = "5.6.0"
# Router prefix set specifically to avoid double-prefixing while remaining standalone
router = APIRouter(prefix="/v1/enriched", tags=["enriched"])

# =============================================================================
# Auth Logic (Aligned with AI/Advanced Routes)
# =============================================================================

def _auth_ok(token: Optional[str]) -> bool:
    """Check if the provided token matches allowed application secrets."""
    allowed = []
    for k in ("APP_TOKEN", "BACKUP_APP_TOKEN", "TFB_APP_TOKEN"):
        v = (os.getenv(k) or "").strip()
        if v: allowed.append(v)
    # If no tokens configured, allow all requests (open mode)
    if not allowed: return True
    return bool(token and token.strip() in allowed)

# =============================================================================
# Router Implementation
# =============================================================================

@router.get("/health", include_in_schema=False)
async def enriched_health():
    """Lightweight health check for the enriched router."""
    return {"status": "ok", "module": "routes.enriched_quote", "version": ENRICHED_ROUTE_VERSION}

@router.get("/quote")
async def get_single_quote(
    request: Request,
    symbol: str = Query(..., description="Ticker symbol (e.g. 1120.SR, AAPL)"),
    refresh: bool = Query(False, description="Force a fresh fetch, bypassing cache."),
    x_app_token: Optional[str] = Header(default=None, alias="X-APP-TOKEN")
):
    """Fetch single enriched quote via the hardened Data Engine."""
    if not _auth_ok(x_app_token):
        return JSONResponse(content={"status": "error", "error": "Unauthorized"}, status_code=200)

    try:
        # Resolve Engine singleton from app state (Hardened v2.8.5)
        eng = getattr(request.app.state, "engine", None)
        if not eng:
            from core.data_engine_v2 import get_engine
            eng = await get_engine()

        # Fetch Data (Triggers regional fallback Yahoo -> Argaam if needed)
        res = await eng.get_enriched_quote(symbol, refresh=refresh)
        
        # Standardize via Mapper v2.6.0 (Ensures recommendation/timestamp consistency)
        from core.enriched_quote import EnrichedQuote
        mapper = EnrichedQuote(res)
        
        return JSONResponse(content=jsonable_encoder(mapper.payload))
    except Exception as e:
        logger.error("Single Quote Route Error for %s: %s", symbol, e)
        return JSONResponse(content={
            "status": "error", 
            "error": str(e), 
            "symbol": symbol,
            "data_quality": "MISSING"
        }, status_code=200)

@router.get("/quotes")
async def get_batch_quotes(
    request: Request,
    symbols: str = Query(..., description="Comma-separated symbols list"),
    refresh: bool = Query(False, description="Force a fresh fetch for all symbols."),
    x_app_token: Optional[str] = Header(default=None, alias="X-APP-TOKEN")
):
    """Batch fetch enriched quotes (optimized for Google Sheets sync)."""
    if not _auth_ok(x_app_token):
        return JSONResponse(content={"status": "error", "error": "Unauthorized", "items": []}, status_code=200)

    sym_list = [s.strip().upper() for s in symbols.split(",") if s.strip()]
    if not sym_list:
        return JSONResponse(content={"status": "error", "error": "No symbols provided"}, status_code=200)

    try:
        # Resolve Engine
        eng = getattr(request.app.state, "engine", None)
        if not eng:
            from core.data_engine_v2 import get_engine
            eng = await get_engine()

        # Batch Fetch (Optimized internal gather concurrency)
        raw_results = await eng.get_enriched_quotes(sym_list, refresh=refresh)
        
        # Map and Standardize
        from core.enriched_quote import EnrichedQuote
        final_items = []
        for r in raw_results:
            mapper = EnrichedQuote(r)
            final_items.append(mapper.payload)
            
        return JSONResponse(content={
            "status": "success",
            "count": len(final_items),
            "items": final_items,
            "version": ENRICHED_ROUTE_VERSION
        })
    except Exception as e:
        logger.error("Batch Quotes Route Error: %s", e)
        return JSONResponse(content={"status": "error", "error": str(e)}, status_code=200)

def get_router() -> APIRouter:
    """Helper for router mounting logic in main.py."""
    return router

__all__ = ["router", "get_router"]
