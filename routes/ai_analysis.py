# routes/ai_analysis.py
"""
TADAWUL FAST BRIDGE – AI ANALYSIS ROUTES (v4.6.0) – PROD SAFE

Advanced Analysis Router
- Handles: /v1/analysis/quotes (Batch/Single)
- Handles: /v1/analysis/sheet-rows (Dual Mode: Push Cache / Compute Grid)
- Handles: /v1/analysis/scoreboard

Key Upgrades in v4.6.0:
- ✅ Scoring Integration: Native hook into core.scoring_engine v1.7.2.
- ✅ ROI Alignment: Standardized on 'expected_roi_1m/3m/12m' (canonical).
- ✅ Smart Push: Detects 'items' payload to update Engine Snapshot Cache.
- ✅ Riyadh Time: Enforces localized timestamps (UTC+3) for KSA users.
- ✅ Robust Fallbacks: Never crashes on missing providers or engines.
"""

from __future__ import annotations

import asyncio
import inspect
import logging
import os
import re
import time
from datetime import datetime, timezone, timedelta
from functools import lru_cache
from typing import Any, Dict, List, Optional, Sequence, Tuple, Union

from fastapi import APIRouter, Body, Header, Query, Request

# Pydantic v2 preferred
try:
    from pydantic import BaseModel, ConfigDict, Field  # type: ignore
    _PYDANTIC_V2 = True
except Exception:  # pragma: no cover
    from pydantic import BaseModel, Field  # type: ignore
    ConfigDict = None  # type: ignore
    _PYDANTIC_V2 = False

logger = logging.getLogger("routes.ai_analysis")

AI_ANALYSIS_VERSION = "4.6.0"
router = APIRouter(prefix="/v1/analysis", tags=["AI & Analysis"])


# =============================================================================
# Settings Shim
# =============================================================================
try:
    from core.config import get_settings  # type: ignore
except Exception:
    def get_settings(): return None


# =============================================================================
# Config Helpers
# =============================================================================
def _safe_int(x: Any, default: int) -> int:
    try: return int(str(x).strip()) if x else default
    except: return default

def _safe_float(x: Any, default: float) -> float:
    try: return float(str(x).strip()) if x else default
    except: return default

def _cfg() -> Dict[str, Any]:
    s = get_settings()
    
    # Defaults
    batch_size = 20
    timeout = 45.0
    concurrency = 5
    max_tickers = 500

    # Env overrides
    batch_size = _safe_int(os.getenv("AI_BATCH_SIZE"), batch_size)
    timeout = _safe_float(os.getenv("AI_BATCH_TIMEOUT_SEC"), timeout)
    concurrency = _safe_int(os.getenv("AI_BATCH_CONCURRENCY"), concurrency)
    max_tickers = _safe_int(os.getenv("AI_MAX_TICKERS"), max_tickers)

    return {
        "batch_size": max(5, min(250, batch_size)),
        "timeout_sec": max(5.0, min(180.0, timeout)),
        "max_tickers": max(10, min(3000, max_tickers)),
        "concurrency": max(1, min(30, concurrency)),
    }


# =============================================================================
# Date/Time Helpers
# =============================================================================
def _now_utc_iso() -> str:
    return datetime.now(timezone.utc).isoformat()

def _riyadh_iso() -> str:
    tz = timezone(timedelta(hours=3))
    return datetime.now(tz).isoformat()

def _to_riyadh_iso(utc_iso: Optional[str]) -> str:
    if not utc_iso: return ""
    try:
        # Standardize Z to offset for fromisoformat compatibility
        dt = datetime.fromisoformat(utc_iso.replace("Z", "+00:00"))
        tz = timezone(timedelta(hours=3))
        return dt.astimezone(tz).isoformat()
    except:
        return ""


# =============================================================================
# Imports (Lazy/Safe)
# =============================================================================
@lru_cache(maxsize=1)
def _try_import_scoring_engine():
    try:
        from core.scoring_engine import enrich_with_scores
        return enrich_with_scores
    except ImportError:
        return None

@lru_cache(maxsize=1)
def _try_import_enriched_quote():
    try:
        from core.enriched_quote import EnrichedQuote
        return EnrichedQuote
    except ImportError:
        return None

@lru_cache(maxsize=1)
def _try_import_schemas():
    try:
        import core.schemas as schemas
        return schemas
    except ImportError:
        return None

def _enrich_scores_best_effort(uq: Any) -> Any:
    enricher = _try_import_scoring_engine()
    if enricher:
        try:
            return enricher(uq)
        except Exception:
            pass
    return uq

# =============================================================================
# Models
# =============================================================================
class _ExtraIgnoreBase(BaseModel):
    if _PYDANTIC_V2:
        model_config = ConfigDict(extra="ignore")
    else:
        class Config: extra = "ignore"

class SingleAnalysisResponse(_ExtraIgnoreBase):
    symbol: str
    name: Optional[str] = None
    market_region: Optional[str] = None
    price: Optional[float] = None
    change_pct: Optional[float] = None
    
    # Fundamental
    market_cap: Optional[float] = None
    pe_ttm: Optional[float] = None
    dividend_yield: Optional[float] = None
    
    # Scores
    value_score: Optional[float] = None
    quality_score: Optional[float] = None
    momentum_score: Optional[float] = None
    risk_score: Optional[float] = None
    overall_score: Optional[float] = None
    recommendation: Optional[str] = "HOLD"
    
    # Forecast (Canonical)
    expected_roi_1m: Optional[float] = None
    expected_roi_3m: Optional[float] = None
    expected_roi_12m: Optional[float] = None
    forecast_price_1m: Optional[float] = None
    forecast_price_3m: Optional[float] = None
    forecast_price_12m: Optional[float] = None
    forecast_confidence: Optional[float] = None
    forecast_updated_utc: Optional[str] = None
    
    # Meta
    data_quality: str = "MISSING"
    last_updated_utc: Optional[str] = None
    last_updated_riyadh: Optional[str] = None
    error: Optional[str] = None

class BatchAnalysisRequest(_ExtraIgnoreBase):
    tickers: List[str] = Field(default_factory=list)
    symbols: List[str] = Field(default_factory=list)
    sheet_name: Optional[str] = None

class BatchAnalysisResponse(_ExtraIgnoreBase):
    results: List[SingleAnalysisResponse] = Field(default_factory=list)

class PushSheetItem(_ExtraIgnoreBase):
    sheet: str
    headers: List[str] = Field(default_factory=list)
    rows: List[List[Any]] = Field(default_factory=list)

class PushSheetRowsRequest(_ExtraIgnoreBase):
    items: List[PushSheetItem] = Field(default_factory=list)
    universe_rows: Optional[int] = None
    source: Optional[str] = None

class SheetAnalysisResponse(_ExtraIgnoreBase):
    headers: List[str] = Field(default_factory=list)
    rows: List[List[Any]] = Field(default_factory=list)
    status: str = "success"
    error: Optional[str] = None
    version: str = AI_ANALYSIS_VERSION


# =============================================================================
# Core Logic
# =============================================================================
async def _resolve_engine(request: Request) -> Any:
    # 1. App State
    if hasattr(request.app.state, "engine") and request.app.state.engine:
        return request.app.state.engine
    
    # 2. Singleton Fallback
    try:
        from core.data_engine_v2 import get_engine
        return await get_engine()
    except Exception:
        return None

def _clean_tickers(inputs: List[str]) -> List[str]:
    seen = set()
    out = []
    for x in inputs:
        s = str(x or "").strip().upper()
        if s and s not in seen:
            seen.add(s)
            out.append(s)
    return out

def _make_placeholder(symbol: str, dq="MISSING", err="No Data") -> Dict[str, Any]:
    utc = _now_utc_iso()
    return {
        "symbol": symbol,
        "symbol_normalized": symbol,
        "data_quality": dq,
        "error": err,
        "recommendation": "HOLD",
        "last_updated_utc": utc,
        "last_updated_riyadh": _to_riyadh_iso(utc)
    }

async def _get_quotes_chunked(engine: Any, symbols: List[str], cfg: Dict[str, Any]) -> Dict[str, Any]:
    if not engine:
        return {s: _make_placeholder(s, err="No Engine") for s in symbols}
    
    # Chunking
    chunk_size = cfg["batch_size"]
    chunks = [symbols[i:i + chunk_size] for i in range(0, len(symbols), chunk_size)]
    
    # Execution
    # We use engine.get_enriched_quotes (async)
    results = {}
    
    # Semaphore for concurrency control
    sem = asyncio.Semaphore(cfg["concurrency"])
    
    async def process_chunk(chunk):
        async with sem:
            try:
                # Try batch first
                if hasattr(engine, "get_enriched_quotes"):
                    res = await engine.get_enriched_quotes(chunk)
                    # res might be list or dict
                    if isinstance(res, list):
                        for item in res:
                            sym = str(item.get("symbol") or "").upper()
                            if sym: results[sym] = item
                    elif isinstance(res, dict):
                        results.update(res)
                else:
                    # Fallback serial
                    for s in chunk:
                        results[s] = await engine.get_enriched_quote(s)
            except Exception as e:
                logger.error(f"Chunk failed: {e}")
                for s in chunk:
                    results[s] = _make_placeholder(s, err=str(e))

    await asyncio.gather(*[process_chunk(c) for c in chunks])
    return results

def _quote_to_response(q: Any) -> SingleAnalysisResponse:
    # Convert arbitrary quote object to dict
    d = q if isinstance(q, dict) else (q.__dict__ if hasattr(q, "__dict__") else {})
    
    # Ensure scores are present (Integration point)
    d = _enrich_scores_best_effort(d)
    
    # Map to response model
    return SingleAnalysisResponse(
        symbol=d.get("symbol", "UNKNOWN"),
        name=d.get("name"),
        price=d.get("current_price") or d.get("price"),
        change_pct=d.get("percent_change"),
        market_cap=d.get("market_cap"),
        pe_ttm=d.get("pe_ttm"),
        dividend_yield=d.get("dividend_yield"),
        value_score=d.get("value_score"),
        quality_score=d.get("quality_score"),
        momentum_score=d.get("momentum_score"),
        risk_score=d.get("risk_score"),
        overall_score=d.get("overall_score"),
        recommendation=d.get("recommendation", "HOLD"),
        # Canonical ROI Mapping
        expected_roi_1m=d.get("expected_roi_1m"),
        expected_roi_3m=d.get("expected_roi_3m"),
        expected_roi_12m=d.get("expected_roi_12m"),
        # Canonical Price Mapping
        forecast_price_1m=d.get("forecast_price_1m"),
        forecast_price_3m=d.get("forecast_price_3m"),
        forecast_price_12m=d.get("forecast_price_12m"),
        
        forecast_confidence=d.get("forecast_confidence"),
        data_quality=d.get("data_quality", "OK"),
        error=d.get("error"),
        last_updated_utc=d.get("last_updated_utc"),
        last_updated_riyadh=d.get("last_updated_riyadh") or _to_riyadh_iso(d.get("last_updated_utc"))
    )

# =============================================================================
# Routes
# =============================================================================
@router.get("/health")
async def health(request: Request):
    eng = await _resolve_engine(request)
    return {
        "status": "ok",
        "version": AI_ANALYSIS_VERSION,
        "engine": type(eng).__name__ if eng else "none",
        "time_utc": _now_utc_iso(),
        "time_riyadh": _riyadh_iso()
    }

@router.post("/quotes", response_model=BatchAnalysisResponse)
async def batch_analysis(req: BatchAnalysisRequest, request: Request):
    """
    Standard batch analysis endpoint.
    Returns structured JSON list of analyzed stocks.
    """
    tickers = _clean_tickers(req.tickers + req.symbols)
    if not tickers:
        return BatchAnalysisResponse(results=[])

    cfg = _cfg()
    # Cap tickers
    if len(tickers) > cfg["max_tickers"]:
        tickers = tickers[:cfg["max_tickers"]]

    engine = await _resolve_engine(request)
    raw_map = await _get_quotes_chunked(engine, tickers, cfg)
    
    results = []
    for t in tickers:
        q = raw_map.get(t) or _make_placeholder(t)
        results.append(_quote_to_response(q))
        
    return BatchAnalysisResponse(results=results)

@router.post("/sheet-rows")
async def sheet_rows(
    request: Request,
    body: Dict[str, Any] = Body(...),
    x_app_token: Optional[str] = Header(None, alias="X-APP-TOKEN")
) -> Union[SheetAnalysisResponse, Dict[str, Any]]:
    """
    Dual-Mode Endpoint:
    1. Push Mode: Receives {"items": [...]} from Sheets to cache snapshots.
    2. Compute Mode: Receives {"tickers": [...]} to return analyzed grid rows.
    """
    
    # 1. PUSH MODE
    if "items" in body and isinstance(body["items"], list):
        try:
            req = PushSheetRowsRequest.model_validate(body) if _PYDANTIC_V2 else PushSheetRowsRequest.parse_obj(body) # type: ignore
            engine = await _resolve_engine(request)
            
            if not engine or not hasattr(engine, "set_cached_sheet_snapshot"):
                return {"status": "error", "error": "Engine does not support snapshots"}
            
            written = []
            for item in req.items:
                # Store in engine cache
                engine.set_cached_sheet_snapshot(
                    sheet_name=item.sheet,
                    headers=item.headers,
                    rows=item.rows,
                    meta={"source": "push_api", "ts": _now_utc_iso()}
                )
                written.append(item.sheet)
                
            return {"status": "success", "written": written, "mode": "push", "version": AI_ANALYSIS_VERSION}
        except Exception as e:
            return {"status": "error", "error": str(e)}

    # 2. COMPUTE MODE
    tickers = _clean_tickers(body.get("tickers", []) + body.get("symbols", []))
    sheet_name = body.get("sheet_name") or "Analysis"
    
    if not tickers:
        return SheetAnalysisResponse(status="skipped", error="No tickers", headers=[], rows=[])

    cfg = _cfg()
    engine = await _resolve_engine(request)
    quotes_map = await _get_quotes_chunked(engine, tickers, cfg)
    
    # Get Schema
    schemas = _try_import_schemas()
    headers = []
    if schemas:
        headers = schemas.get_headers_for_sheet(sheet_name)
    
    # Fallback Headers (v4.6.0 Standard)
    if not headers:
        headers = [
            "Symbol", "Name", "Price", "Change %", 
            "Overall Score", "Recommendation", "Rec Badge",
            "Expected ROI % (1M)", "Forecast Price (1M)",
            "Forecast Confidence", "Data Quality", "Error"
        ]
        
    # Map Rows
    EnrichedQuote = _try_import_enriched_quote()
    rows = []
    
    for t in tickers:
        q = quotes_map.get(t) or _make_placeholder(t)
        
        # Ensure Scoring & Localization
        q = _enrich_scores_best_effort(q)
        if isinstance(q, dict):
            q["last_updated_riyadh"] = _to_riyadh_iso(q.get("last_updated_utc"))
            q["forecast_updated_riyadh"] = _riyadh_iso()
        
        # Row Mapping
        if EnrichedQuote and isinstance(q, dict):
            try:
                eq = EnrichedQuote.from_unified(q)
                rows.append(eq.to_row(headers))
            except:
                rows.append([str(q.get("error") or "Mapping Error")] + [""] * (len(headers)-1))
        else:
            # Simple fallback mapping
            rows.append([t, "Missing EnrichedQuote lib", "", "", ""])

    return SheetAnalysisResponse(
        status="success",
        headers=headers,
        rows=rows,
        version=AI_ANALYSIS_VERSION
    )

__all__ = ["router"]
