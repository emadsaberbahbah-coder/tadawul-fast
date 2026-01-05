# core/enriched_quote.py
"""
core/enriched_quote.py
------------------------------------------------------------
Compatibility Router + Row Mapper: Enriched Quote (PROD SAFE) — v2.5.6

What this module is for:
- Legacy/compat endpoints under /v1/enriched/* for dashboard sync.
- Bridges engine results to Google Sheets row formats.
- Ensures strict standardization of Recommendations and Timestamps.

✅ v2.5.6 Alignment:
- Engine-First: Directly utilizes app.state.engine singleton (v2.8.5 compatible).
- Schema-Aware: Uses core.schemas for intelligent header mapping if available.
- Reco-Standard: Forces strict BUY / HOLD / REDUCE / SELL enums.
- Riyadh Time: Precise localized timestamps for Tadawul market sync.
"""

from __future__ import annotations

import asyncio
import inspect
import logging
import os
import re
import traceback
from datetime import datetime, timezone, timedelta
from functools import lru_cache
from typing import Any, Dict, List, Optional, Sequence, Tuple, Union

from fastapi import APIRouter, Query, Request
from fastapi.encoders import jsonable_encoder
from starlette.responses import JSONResponse

logger = logging.getLogger("core.enriched_quote")

ROUTER_VERSION = "2.5.6"
router = APIRouter(prefix="/v1/enriched", tags=["enriched"])

# =============================================================================
# Normalization & Utility Shims
# =============================================================================

@lru_cache(maxsize=1)
def _get_reco_normalizer():
    """Try to import the dedicated normalization utility."""
    try:
        from core.reco_normalize import normalize_recommendation
        return normalize_recommendation
    except ImportError:
        def fallback(x, default="HOLD"):
            if not x: return default
            s = str(x).strip().upper()
            if "BUY" in s: return "BUY"
            if "SELL" in s: return "SELL"
            if "REDUCE" in s or "TRIM" in s: return "REDUCE"
            return "HOLD"
        return fallback

def _to_riyadh_iso(utc_any: Any) -> str:
    """Convert UTC ISO string to Riyadh Local ISO (+3)."""
    if not utc_any: return ""
    try:
        s = str(utc_any).replace("Z", "+00:00")
        dt = datetime.fromisoformat(s)
        if dt.tzinfo is None: dt = dt.replace(tzinfo=timezone.utc)
        # Riyadh is UTC+3
        riyadh_dt = dt.astimezone(timezone(timedelta(hours=3)))
        return riyadh_dt.isoformat()
    except: return ""

def _maybe_percent(v: Any) -> Optional[float]:
    """Convert ratio (0.05) to percentage (5.0) for Sheets."""
    try:
        f = float(v)
        # Only multiply if it looks like a fraction
        return f * 100.0 if abs(f) <= 1.5 else f
    except: return None

# =============================================================================
# Enriched Quote Mapping Logic
# =============================================================================

class EnrichedQuote:
    """Mapper for converting UnifiedQuote payloads to Sheet-ready rows."""
    def __init__(self, payload: Dict[str, Any]):
        self.payload = payload or {}
        self._standardize()

    def _standardize(self):
        """Standardize internal keys before mapping to rows."""
        normalize = _get_reco_normalizer()
        self.payload["recommendation"] = normalize(self.payload.get("recommendation", "HOLD"))
        
        # Ensure timestamp exists
        now_iso = datetime.now(timezone.utc).isoformat()
        self.payload.setdefault("last_updated_utc", now_iso)
        
        # localized timestamp for Saudi Dashboard
        if not self.payload.get("last_updated_riyadh"):
            self.payload["last_updated_riyadh"] = _to_riyadh_iso(self.payload["last_updated_utc"])
            
    def to_row(self, headers: Sequence[str]) -> List[Any]:
        """Maps payload keys to requested header order with alias support."""
        row = []
        for h in headers:
            # Clean header to key
            hk = h.lower().replace(" ", "_").replace("%", "percent").replace("/", "_").replace("-", "_")
            
            # Robust Alias Map for Google Sheets columns
            alias_map = {
                "last_price": "current_price",
                "price": "current_price",
                "price_change": "price_change",
                "change": "price_change",
                "change_percent": "percent_change",
                "52w_high": "week_52_high",
                "52w_low": "week_52_low",
                "pe_ttm": "pe_ratio",
                "p_e_ttm": "pe_ratio",
                "dividend_yield_percent": "dividend_yield"
            }
            
            key = alias_map.get(hk, hk)
            val = self.payload.get(key)
            
            # Auto-format percentages for specific columns
            pct_cols = {"yield", "return", "roe", "roa", "payout", "change_percent", "percent_change"}
            if any(p in hk for p in pct_cols):
                val = _maybe_percent(val)
                
            row.append(val)
        return row

# =============================================================================
# Routes
# =============================================================================

@router.get("/quote")
async def get_single_quote(request: Request, symbol: str = Query(...)):
    """Fetch single enriched quote via the active Data Engine."""
    try:
        # Access engine initialized in main.py lifespan
        eng = getattr(request.app.state, "engine", None)
        if not eng:
            from core.data_engine_v2 import get_engine
            eng = await get_engine()
            
        result = await eng.get_enriched_quote(symbol)
        mapper = EnrichedQuote(result)
        
        # Return standardized payload
        return JSONResponse(content=jsonable_encoder(mapper.payload))
    except Exception as e:
        logger.error("Enriched Route Error for %s: %s", symbol, e)
        return JSONResponse(content={
            "status": "error", 
            "error": str(e), 
            "symbol": symbol,
            "data_quality": "MISSING"
        }, status_code=200)

@router.get("/quotes")
async def get_batch_quotes(request: Request, symbols: str = Query(...)):
    """Batch fetch enriched quotes with bounded concurrency."""
    sym_list = [s.strip().upper() for s in symbols.split(",") if s.strip()]
    if not sym_list:
        return JSONResponse(content={"status": "error", "error": "No symbols provided"}, status_code=200)

    try:
        eng = getattr(request.app.state, "engine", None)
        if not eng:
            from core.data_engine_v2 import get_engine
            eng = await get_engine()

        # Batch fetch via DataEngineV2 (optimized with asyncio.gather)
        results = await eng.get_enriched_quotes(sym_list)
        
        final_items = []
        for r in results:
            mapper = EnrichedQuote(r)
            final_items.append(mapper.payload)
            
        return JSONResponse(content={
            "status": "success",
            "count": len(final_items),
            "items": final_items,
            "engine_version": getattr(eng, "ENGINE_VERSION", "unknown"),
            "router_version": ROUTER_VERSION
        })
    except Exception as e:
        logger.error("Batch Enriched Route Error: %s", e)
        return JSONResponse(content={"status": "error", "error": str(e)}, status_code=200)

@router.get("/health", include_in_schema=False)
async def health():
    return {
        "status": "ok", 
        "module": "core.enriched_quote", 
        "version": ROUTER_VERSION,
        "timezone_offset": "+03:00 (Riyadh)"
    }

def get_router() -> APIRouter:
    return router

__all__ = ["router", "get_router", "EnrichedQuote"]
