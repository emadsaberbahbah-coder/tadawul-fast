# routes/ai_analysis.py
"""
AI & QUANT ANALYSIS ROUTES – GOOGLE SHEETS FRIENDLY (v4.3.0)
PROD SAFE / HARDENED / ENGINE-ALIGNED

Key features:
- Engine-driven: Prefers request.app.state.engine initialized in main.py.
- Reco-Standard: Uses central reco_normalize to ensure BUY/HOLD/REDUCE/SELL enums.
- Riyadh Time: Precise localized timestamps for Saudi dashboard alignment.
- Sheet-Ready: Normalizes ratios (0.05) to percentages (5.0) for dashboard columns.
- Auth-Aware: Enforces X-APP-TOKEN security when configured.
"""

from __future__ import annotations

import asyncio
import inspect
import logging
import os
import re
import traceback
from dataclasses import dataclass
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

logger = logging.getLogger("routes.ai_analysis")

AI_ANALYSIS_VERSION = "4.3.0"
router = APIRouter(prefix="/v1/analysis", tags=["AI & Analysis"])

# =============================================================================
# Normalization & Utility Shims
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

def _get_allowed_tokens() -> List[str]:
    tokens = []
    for k in ("APP_TOKEN", "BACKUP_APP_TOKEN", "TFB_APP_TOKEN"):
        v = (os.getenv(k) or "").strip()
        if v: tokens.append(v)
    return tokens

def _auth_ok(token: Optional[str]) -> bool:
    allowed = _get_allowed_tokens()
    if not allowed: return True
    return bool(token and token.strip() in allowed)

# =============================================================================
# Models
# =============================================================================

class AnalysisRequest(BaseModel):
    if _PYDANTIC_V2: model_config = ConfigDict(extra="ignore")
    symbols: List[str] = Field(default_factory=list)
    tickers: List[str] = Field(default_factory=list)
    sheet_name: Optional[str] = None

class SheetAnalysisResponse(BaseModel):
    headers: List[str]
    rows: List[List[Any]]
    status: str = "success"
    error: Optional[str] = None

# =============================================================================
# Row Mapping Logic (Analysis Aligned)
# =============================================================================

def _map_to_analysis_row(uq: Dict[str, Any], headers: List[str]) -> List[Any]:
    """Advanced mapping for AI/Analysis tabs with specialized transformations."""
    row = []
    normalize_reco = _get_reco_normalizer()
    
    # Pre-process calculated fields
    reco = normalize_reco(uq.get("recommendation", "HOLD"))
    last_utc = uq.get("last_updated_utc") or datetime.now(timezone.utc).isoformat()
    last_riyadh = uq.get("last_updated_riyadh") or _to_riyadh_iso(last_utc)

    for h in headers:
        hk = h.lower().replace(" ", "_").replace("%", "percent").replace("/", "_").replace("-", "_")
        
        # Mapping rules
        if hk == "recommendation": row.append(reco); continue
        if hk == "last_updated_utc": row.append(last_utc); continue
        if hk == "last_updated_riyadh": row.append(last_riyadh); continue
        
        # Field Aliases
        val = uq.get(hk)
        if val is None:
            alias_map = {
                "last_price": "current_price",
                "price": "current_price",
                "change": "price_change",
                "change_percent": "percent_change",
                "pe_ttm": "pe_ratio",
                "52w_high": "week_52_high",
                "52w_low": "week_52_low"
            }
            val = uq.get(alias_map.get(hk, hk))

        # Auto-percent for returns/yields
        if any(p in hk for p in {"yield", "return", "roe", "roa", "payout", "change_percent", "upside"}):
            val = _maybe_percent(val)
            
        row.append(val)
    return row

# =============================================================================
# Endpoints
# =============================================================================

@router.get("/health", include_in_schema=False)
async def analysis_health():
    return {"status": "ok", "module": "routes.ai_analysis", "version": AI_ANALYSIS_VERSION}

@router.post("/sheet-rows", response_model=SheetAnalysisResponse)
async def analyze_for_sheet(
    request: Request,
    body: AnalysisRequest = Body(...),
    x_app_token: Optional[str] = Header(default=None, alias="X-APP-TOKEN")
):
    """
    Highly resilient entrypoint for Google Sheets.
    Always returns 200 OK with error details inside JSON to prevent script crashes.
    """
    # 1. Auth Guard
    if not _auth_ok(x_app_token):
        return SheetAnalysisResponse(headers=[], rows=[], status="error", error="Unauthorized (Invalid Token)")

    # 2. Extract Symbols
    sym_list = [s.strip().upper() for s in (body.symbols + body.tickers) if s.strip()]
    if not sym_list:
        return SheetAnalysisResponse(headers=[], rows=[], status="skipped", error="No symbols provided")

    # 3. Resolve Headers
    try:
        from core.schemas import get_headers_for_sheet
        headers = get_headers_for_sheet(body.sheet_name or "AI_Analysis")
    except ImportError:
        headers = ["Symbol", "Last Price", "Change %", "Market Cap", "Recommendation", "Last Updated (Riyadh)"]

    # 4. Fetch & Map Data
    try:
        eng = getattr(request.app.state, "engine", None)
        if not eng:
            from core.data_engine_v2 import get_engine
            eng = await get_engine()
        
        # Batch fetch via Engine (v2.8.5+)
        raw_results = await eng.get_enriched_quotes(sym_list)
        
        final_rows = []
        for res in raw_results:
            row = _map_to_analysis_row(res, headers)
            final_rows.append(row)

        return SheetAnalysisResponse(
            headers=headers,
            rows=final_rows,
            status="success"
        )
    except Exception as e:
        logger.error("AI Analysis Sheet-Rows Error: %s", e)
        return SheetAnalysisResponse(headers=headers, rows=[], status="error", error=str(e))

@router.get("/quote")
async def analyze_single_quote(request: Request, symbol: str = Query(...), x_app_token: Optional[str] = Header(default=None, alias="X-APP-TOKEN")):
    """Detailed single-ticker analysis payload."""
    if not _auth_ok(x_app_token):
        raise HTTPException(status_code=401, detail="Unauthorized")
        
    try:
        eng = getattr(request.app.state, "engine", None)
        if not eng:
            from core.data_engine_v2 import get_engine
            eng = await get_engine()
            
        res = await eng.get_enriched_quote(symbol)
        # Apply normalization before returning JSON
        res["recommendation"] = _get_reco_normalizer()(res.get("recommendation", "HOLD"))
        res["last_updated_riyadh"] = _to_riyadh_iso(res.get("last_updated_utc"))
        
        return JSONResponse(content=jsonable_encoder(res))
    except Exception as e:
        return JSONResponse(content={"status": "error", "error": str(e)}, status_code=200)

def get_router() -> APIRouter:
    return router

__all__ = ["router", "get_router", "AnalysisRequest", "SheetAnalysisResponse"]
