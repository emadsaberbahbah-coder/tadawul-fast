# routes/enriched_quote.py
"""
routes/enriched_quote.py
------------------------------------------------------------
Enriched Quote Router — v5.5.0 (ALIGNED + HARDENED)
PROD SAFE (await-safe + singleton-engine friendly)

What's New in v5.5.0:
- ✅ Simplified: Redundant normalization logic removed (now uses core helpers).
- ✅ Path-Safe: Fixed double-prefixing bug (routes now relative to mount point).
- ✅ Mapper-Aligned: Uses EnrichedQuote mapper from core.enriched_quote.
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

ENRICHED_ROUTE_VERSION = "5.5.0"
router = APIRouter(tags=["enriched"])

# =============================================================================
# Auth Logic (Aligned with Analysis Routes)
# =============================================================================

def _auth_ok(token: Optional[str]) -> bool:
    allowed = []
    for k in ("APP_TOKEN", "BACKUP_APP_TOKEN", "TFB_APP_TOKEN"):
        v = (os.getenv(k) or "").strip()
        if v: allowed.append(v)
    if not allowed: return True
    return bool(token and token.strip() in allowed)

# =============================================================================
# Router Implementation
# =============================================================================

@router.get("/health", include_in_schema=False)
async def enriched_health():
    return {"status": "ok", "module": "routes.enriched_quote", "version": ENRICHED_ROUTE_VERSION}

@router.get("/quote")
async def get_single_quote(
    request: Request,
    symbol: str = Query(...),
    refresh: bool = Query(False),
    x_app_token: Optional[str] = Header(default=None, alias="X-APP-TOKEN")
):
    """Fetch single enriched quote via the hardened Data Engine."""
    if not _auth_ok(x_app_token):
        return JSONResponse(content={"status": "error", "error": "Unauthorized"}, status_code=200)

    try:
        # Resolve Engine
        eng = getattr(request.app.state, "engine", None)
        if not eng:
            from core.data_engine_v2 import get_engine
            eng = await get_engine()

        # Fetch Data
        res = await eng.get_enriched_quote(symbol, refresh=refresh)
        
        # Standardize via Mapper
        from core.enriched_quote import EnrichedQuote
        mapper = EnrichedQuote(res)
        
        return JSONResponse(content=jsonable_encoder(mapper.payload))
    except Exception as e:
        logger.error("Single Quote Route Error: %s", e)
        return JSONResponse(content={"status": "error", "error": str(e), "symbol": symbol}, status_code=200)

@router.get("/quotes")
async def get_batch_quotes(
    request: Request,
    symbols: str = Query(...),
    refresh: bool = Query(False),
    x_app_token: Optional[str] = Header(default=None, alias="X-APP-TOKEN")
):
    """Batch fetch enriched quotes (optimized for Google Sheets sync)."""
    if not _auth_ok(x_app_token):
        return JSONResponse(content={"status": "error", "error": "Unauthorized", "items": []}, status_code=200)

    sym_list = [s.strip().upper() for s in symbols.split(",") if s.strip()]
    if not sym_list:
        return JSONResponse(content={"status": "error", "error": "No symbols"}, status_code=200)

    try:
        # Resolve Engine
        eng = getattr(request.app.state, "engine", None)
        if not eng:
            from core.data_engine_v2 import get_engine
            eng = await get_engine()

        # Batch Fetch
        raw_results = await eng.get_enriched_quotes(sym_list, refresh=refresh)
        
        # Standardize via Mapper
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
    return router

__all__ = ["router", "get_router"]
