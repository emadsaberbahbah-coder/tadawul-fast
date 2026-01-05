# core/enriched_quote.py
"""
core/enriched_quote.py
------------------------------------------------------------
Compatibility Router + Row Mapper: Enriched Quote (PROD SAFE) — v2.5.5

What this module is for:
- Legacy/compat endpoints under /v1/enriched/* for dashboard sync.
- Bridges engine results to Google Sheets row formats.
- Ensures strict standardization of Recommendations and Timestamps.

✅ v2.5.5 Alignment:
- Standard Recommendation Enum: BUY / HOLD / REDUCE / SELL (Uppercase).
- Smart Riyadh Time: Fills localized timestamps for KSA market rows.
- Ratio-to-Percent: Automatically fixes yield/return formatting for Sheets.
- Concurrency: Bounded batch processing for high-volume ticker syncs.
"""

from __future__ import annotations

import asyncio
import inspect
import logging
import os
import re
import traceback
from datetime import datetime, timezone
from functools import lru_cache
from typing import Any, Dict, List, Optional, Sequence, Tuple, Union

from fastapi import APIRouter, Query, Request
from fastapi.encoders import jsonable_encoder
from starlette.responses import JSONResponse

logger = logging.getLogger("core.enriched_quote")

ROUTER_VERSION = "2.5.5"
router = APIRouter(prefix="/v1/enriched", tags=["enriched"])

_RECO_ENUM = ("BUY", "HOLD", "REDUCE", "SELL")

# =============================================================================
# Normalization & Helpers
# =============================================================================

def _normalize_recommendation(x: Any) -> str:
    """Standardize broker ratings into BUY/HOLD/REDUCE/SELL."""
    if not x: return "HOLD"
    s = str(x).strip().upper()
    
    buy_set = {"STRONG BUY", "BUY", "ACCUMULATE", "ADD", "OUTPERFORM", "OVERWEIGHT"}
    sell_set = {"SELL", "STRONG SELL", "EXIT", "AVOID", "UNDERPERFORM"}
    reduce_set = {"REDUCE", "TRIM", "LIGHTEN", "UNDERWEIGHT", "TAKE PROFIT"}
    
    if s in buy_set or "BUY" in s: return "BUY"
    if s in sell_set or "SELL" in s: return "SELL"
    if s in reduce_set or "REDUCE" in s: return "REDUCE"
    return "HOLD"

def _to_riyadh_iso(utc_any: Any) -> str:
    """Convert UTC ISO string to Riyadh Local ISO."""
    if not utc_any: return ""
    try:
        s = str(utc_any).replace("Z", "+00:00")
        dt = datetime.fromisoformat(s)
        if dt.tzinfo is None: dt = dt.replace(tzinfo=timezone.utc)
        # Shift +3 for Riyadh
        from datetime import timedelta
        riyadh_dt = dt.astimezone(timezone(timedelta(hours=3)))
        return riyadh_dt.isoformat()
    except: return ""

def _maybe_percent(v: Any) -> Optional[float]:
    """Convert 0.05 to 5.0 for sheet compatibility."""
    try:
        f = float(v)
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
        """Pre-process payload for consistency."""
        self.payload["recommendation"] = _normalize_recommendation(self.payload.get("recommendation"))
        self.payload.setdefault("last_updated_utc", datetime.now(timezone.utc).isoformat())
        if not self.payload.get("last_updated_riyadh"):
            self.payload["last_updated_riyadh"] = _to_riyadh_iso(self.payload["last_updated_utc"])
            
    def to_row(self, headers: Sequence[str]) -> List[Any]:
        """Maps payload keys to requested header order."""
        row = []
        for h in headers:
            hk = h.lower().replace(" ", "_").replace("%", "percent").replace("/", "_")
            # Field Alias Mapping
            alias_map = {
                "last_price": "current_price",
                "price": "current_price",
                "change": "price_change",
                "change_percent": "percent_change",
                "52w_high": "week_52_high",
                "52w_low": "week_52_low",
                "pe_ttm": "pe_ratio"
            }
            key = alias_map.get(hk, hk)
            val = self.payload.get(key)
            
            # Apply percentage transforms to yield/returns
            if "yield" in hk or "return" in hk or "roe" in hk or "roa" in hk:
                val = _maybe_percent(val)
                
            row.append(val)
        return row

# =============================================================================
# Endpoints
# =============================================================================

@router.get("/quote")
async def get_single_quote(request: Request, symbol: str = Query(...)):
    """Fetch single enriched quote via the active Data Engine."""
    try:
        eng = getattr(request.app.state, "engine", None)
        if not eng:
            from core.data_engine_v2 import get_engine
            eng = await get_engine()
            
        result = await eng.get_enriched_quote(symbol)
        mapper = EnrichedQuote(result)
        return JSONResponse(content=jsonable_encoder(mapper.payload))
    except Exception as e:
        logger.error("Enriched Route Error: %s", e)
        return JSONResponse(content={"status": "error", "error": str(e), "symbol": symbol}, status_code=200)

@router.get("/quotes")
async def get_batch_quotes(request: Request, symbols: str = Query(...)):
    """Batch fetch enriched quotes with bounded concurrency."""
    sym_list = [s.strip().upper() for s in symbols.split(",") if s.strip()]
    try:
        eng = getattr(request.app.state, "engine", None)
        if not eng:
            from core.data_engine_v2 import get_engine
            eng = await get_engine()

        # DataEngineV2.get_enriched_quotes uses internal gather() for speed
        results = await eng.get_enriched_quotes(sym_list)
        
        final_items = []
        for r in results:
            mapper = EnrichedQuote(r)
            final_items.append(mapper.payload)
            
        return JSONResponse(content={
            "status": "success",
            "count": len(final_items),
            "items": final_items,
            "version": ROUTER_VERSION
        })
    except Exception as e:
        logger.error("Batch Enriched Route Error: %s", e)
        return JSONResponse(content={"status": "error", "error": str(e)}, status_code=200)

@router.get("/health", include_in_schema=False)
async def health():
    return {"status": "ok", "module": "core.enriched_quote", "version": ROUTER_VERSION}

def get_router() -> APIRouter:
    return router

__all__ = ["router", "get_router", "EnrichedQuote"]
