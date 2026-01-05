# routes/advanced_analysis.py
"""
TADAWUL FAST BRIDGE – ADVANCED ANALYSIS ROUTES (v3.13.0)
PROD SAFE / HARDENED / ENGINE-ALIGNED

Key features:
- Engine-driven: Prefers app.state.engine singleton (v2.8.5 compatible).
- Reco-Standard: Uses central reco_normalize for consistent BUY/HOLD/REDUCE/SELL.
- Sheet-Safe: /sheet-rows always returns 200 OK with formatted results for GAS.
- Advanced Sorting: Scoreboard prioritizes Opportunity Score and Data Quality.
- Localized: Supports Riyadh Local ISO (+3) for Saudi market dashboarding.
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

from fastapi import APIRouter, Body, Header, HTTPException, Query, Request
from fastapi.encoders import jsonable_encoder
from starlette.responses import JSONResponse

# Pydantic support
try:
    from pydantic import BaseModel, ConfigDict, Field
    _PYDANTIC_V2 = True
except ImportError:
    from pydantic import BaseModel, Field
    ConfigDict = None
    _PYDANTIC_V2 = False

logger = logging.getLogger("routes.advanced_analysis")

ADVANCED_ANALYSIS_VERSION = "3.13.0"
router = APIRouter(prefix="/v1/advanced", tags=["Advanced Analysis"])

# =============================================================================
# Normalization & Utility Shims (Aligned with Canvas)
# =============================================================================

@lru_cache(maxsize=1)
def _get_reco_normalizer():
    """Import recommendation normalization utility from root or core package."""
    try:
        from core.reco_normalize import normalize_recommendation
        return normalize_recommendation
    except ImportError:
        try:
            from reco_normalize import normalize_recommendation
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
    """Standardize UTC ISO string to Riyadh Local ISO (+3)."""
    if not utc_any: return ""
    try:
        s = str(utc_any).replace("Z", "+00:00")
        dt = datetime.fromisoformat(s)
        if dt.tzinfo is None: dt = dt.replace(tzinfo=timezone.utc)
        riyadh_dt = dt.astimezone(timezone(timedelta(hours=3)))
        return riyadh_dt.isoformat()
    except: return ""

def _maybe_percent(v: Any) -> Optional[float]:
    """Standardize Sheets percentages (e.g., 0.05 -> 5.0)."""
    try:
        f = float(v)
        return f * 100.0 if abs(f) <= 1.5 else f
    except: return None

# =============================================================================
# Auth Logic
# =============================================================================

def _auth_ok(token: Optional[str]) -> bool:
    allowed = []
    for k in ("APP_TOKEN", "BACKUP_APP_TOKEN", "TFB_APP_TOKEN"):
        v = (os.getenv(k) or "").strip()
        if v: allowed.append(v)
    if not allowed: return True
    return bool(token and token.strip() in allowed)

# =============================================================================
# Models
# =============================================================================

class AdvancedRequest(BaseModel):
    if _PYDANTIC_V2: model_config = ConfigDict(extra="ignore")
    symbols: List[str] = Field(default_factory=list)
    tickers: List[str] = Field(default_factory=list)
    top_n: int = 50
    sheet_name: Optional[str] = None

class AdvancedSheetResponse(BaseModel):
    headers: List[str]
    rows: List[List[Any]]
    status: str = "success"
    error: Optional[str] = None

# =============================================================================
# Advanced Mapping Logic
# =============================================================================

def _map_advanced_row(uq: Dict[str, Any], headers: List[str], rank: int = 0, origin: str = "") -> List[Any]:
    """Highly specialized mapping for Advanced Advisor and Scoreboard tabs."""
    row = []
    normalize_reco = _get_reco_normalizer()
    
    # Pre-calculated specialized fields
    reco = normalize_reco(uq.get("recommendation", "HOLD"))
    last_utc = uq.get("last_updated_utc") or datetime.now(timezone.utc).isoformat()
    last_riyadh = uq.get("last_updated_riyadh") or _to_riyadh_iso(last_utc)
    
    # Handle 52W Position if missing from provider
    pos_52w = uq.get("position_52w_percent")
    if pos_52w is None:
        try:
            cp = float(uq.get("current_price", 0))
            lo = float(uq.get("week_52_low", 0))
            hi = float(uq.get("week_52_high", 0))
            if hi > lo: pos_52w = ((cp - lo) / (hi - lo)) * 100.0
        except: pass

    for h in headers:
        hk = h.lower().replace(" ", "_").replace("%", "percent").replace("/", "_").replace("-", "_").replace("(", "").replace(")", "")
        
        # Mapping rules
        if hk == "rank": row.append(rank); continue
        if hk == "origin": row.append(origin); continue
        if hk == "recommendation": row.append(reco); continue
        if hk == "last_updated_riyadh": row.append(last_riyadh); continue
        if hk == "52w_position_percent": row.append(pos_52w); continue
        
        # Field Alias Lookup
        val = uq.get(hk)
        if val is None:
            alias_map = {
                "last_price": "current_price",
                "price": "current_price",
                "change": "price_change",
                "change_percent": "percent_change",
                "52w_high": "week_52_high",
                "52w_low": "week_52_low",
                "p_e_ttm": "pe_ratio",
                "pe_ttm": "pe_ratio"
            }
            val = uq.get(alias_map.get(hk, hk))

        # Auto-percent normalization
        if any(p in hk for p in {"yield", "return", "roe", "roa", "payout", "change_percent", "upside"}):
            val = _maybe_percent(val)
            
        row.append(val)
    return row

# =============================================================================
# Endpoints
# =============================================================================

@router.get("/health", include_in_schema=False)
async def advanced_health():
    return {"status": "ok", "module": "routes.advanced_analysis", "version": ADVANCED_ANALYSIS_VERSION}

@router.post("/sheet-rows", response_model=AdvancedSheetResponse)
async def advanced_for_sheet(
    request: Request,
    body: AdvancedRequest = Body(...),
    x_app_token: Optional[str] = Header(default=None, alias="X-APP-TOKEN")
):
    """Resilient entrypoint for Google Sheets dashboard sync."""
    if not _auth_ok(x_app_token):
        return AdvancedSheetResponse(headers=[], rows=[], status="error", error="Unauthorized")

    sym_list = [s.strip().upper() for s in (body.symbols + body.tickers) if s.strip()]
    if not sym_list:
        return AdvancedSheetResponse(headers=[], rows=[], status="skipped")

    # Resolve Headers
    try:
        from core.schemas import get_headers_for_sheet
        headers = get_headers_for_sheet(body.sheet_name or "Advanced_Analysis")
    except ImportError:
        headers = ["Rank", "Symbol", "Last Price", "Upside %", "Opportunity Score", "Recommendation"]

    try:
        eng = getattr(request.app.state, "engine", None)
        if not eng:
            from core.data_engine_v2 import get_engine
            eng = await get_engine()
        
        # 1. Fetch batch
        raw_results = await eng.get_enriched_quotes(sym_list)
        
        # 2. Sort by Opportunity Score Descending
        def sort_key(x):
            return float(x.get("opportunity_score") or 0)
        raw_results.sort(key=sort_key, reverse=True)
        
        # 3. Apply Top N
        limit = body.top_n or 50
        raw_results = raw_results[:limit]
        
        # 4. Map to rows
        final_rows = []
        origin = body.sheet_name or "Advanced"
        for i, res in enumerate(raw_results, start=1):
            row = _map_advanced_row(res, headers, rank=i, origin=origin)
            final_rows.append(row)

        return AdvancedSheetResponse(headers=headers, rows=final_rows, status="success")
        
    except Exception as e:
        logger.error("Advanced Sheet-Rows Error: %s", e)
        return AdvancedSheetResponse(headers=headers, rows=[], status="error", error=str(e))

@router.get("/scoreboard")
async def get_scoreboard(request: Request, tickers: str = Query(...), x_app_token: Optional[str] = Header(default=None, alias="X-APP-TOKEN")):
    """Ranked JSON output of top opportunities."""
    if not _auth_ok(x_app_token):
        raise HTTPException(status_code=401, detail="Unauthorized")
        
    sym_list = [s.strip().upper() for s in tickers.split(",") if s.strip()]
    try:
        eng = getattr(request.app.state, "engine", None)
        if not eng:
            from core.data_engine_v2 import get_engine
            eng = await get_engine()
            
        results = await eng.get_enriched_quotes(sym_list)
        # Apply normalization and sorting
        results.sort(key=lambda x: float(x.get("opportunity_score") or 0), reverse=True)
        
        for r in results:
            r["recommendation"] = _get_reco_normalizer()(r.get("recommendation", "HOLD"))
            r["last_updated_riyadh"] = _to_riyadh_iso(r.get("last_updated_utc"))
            
        return JSONResponse(content={"status": "success", "count": len(results), "items": jsonable_encoder(results)})
    except Exception as e:
        return JSONResponse(content={"status": "error", "error": str(e)}, status_code=200)

def get_router() -> APIRouter:
    return router

__all__ = ["router", "get_router", "AdvancedRequest", "AdvancedSheetResponse"]
