"""
routes/ai_analysis.py
------------------------------------------------------------
AI & QUANT ANALYSIS ROUTES – GOOGLE SHEETS FRIENDLY (v2.8.0)

Preferred backend:
  - core.data_engine_v2.DataEngine (class-based unified engine)

Provides:
  - GET  /v1/analysis/health  (alias: /ping)
  - GET  /v1/analysis/quote?symbol=...
  - POST /v1/analysis/quotes
  - POST /v1/analysis/sheet-rows   (headers + rows for Google Sheets)

Design goals:
  - KSA-safe: router never calls providers directly.
  - Extremely defensive: bounded batch size, chunking, timeout, concurrency.
  - Never throws 502 for Sheets endpoints; returns headers + rows (maybe empty).
  - ALIGNMENT: Uses core.scoring_engine to ensure scores match Advanced Analysis.

Author: Emad Bahbah (with GPT-5.2 Thinking)
"""

from __future__ import annotations

import asyncio
import logging
import os
from datetime import datetime, timezone
from functools import lru_cache
from typing import Any, Dict, List, Optional, Sequence, Tuple, Union

from fastapi import APIRouter, Query
from pydantic import BaseModel, Field

# --- CORE INTEGRATION ---
from core.data_engine_v2 import DataEngine, UnifiedQuote
from core.enriched_quote import EnrichedQuote
from core.scoring_engine import enrich_with_scores

logger = logging.getLogger("routes.ai_analysis")

AI_ANALYSIS_VERSION = "2.8.0"

# ----------------------------------------------------------------------
# Runtime limits (env overrides)
# ----------------------------------------------------------------------
DEFAULT_BATCH_SIZE = int(os.getenv("AI_BATCH_SIZE", "20"))
DEFAULT_BATCH_TIMEOUT = float(os.getenv("AI_BATCH_TIMEOUT_SEC", "45"))
DEFAULT_CONCURRENCY = int(os.getenv("AI_BATCH_CONCURRENCY", "5"))
DEFAULT_MAX_TICKERS = int(os.getenv("AI_MAX_TICKERS", "500"))

# ----------------------------------------------------------------------
# ENGINE SINGLETON
# ----------------------------------------------------------------------
@lru_cache(maxsize=1)
def _get_engine_singleton() -> DataEngine:
    logger.info("routes.ai_analysis: initializing DataEngine v2 singleton")
    return DataEngine()

# ----------------------------------------------------------------------
# ROUTER
# ----------------------------------------------------------------------
router = APIRouter(prefix="/v1/analysis", tags=["AI & Analysis"])


# ----------------------------------------------------------------------
# MODELS (ignore extra fields to prevent 422 from Sheets client)
# ----------------------------------------------------------------------
class _ExtraIgnoreBase(BaseModel):
    class Config:
        extra = "ignore"


class SingleAnalysisResponse(_ExtraIgnoreBase):
    symbol: str
    name: Optional[str] = None
    market_region: Optional[str] = None

    price: Optional[float] = None
    change_pct: Optional[float] = None
    market_cap: Optional[float] = None
    pe_ttm: Optional[float] = None
    pb: Optional[float] = None

    dividend_yield: Optional[float] = None  # fraction 0–1 or percent, normalized later
    roe: Optional[float] = None             # fraction 0–1 or percent
    roa: Optional[float] = None             # fraction 0–1 or percent

    data_quality: str = "MISSING"

    value_score: Optional[float] = None
    quality_score: Optional[float] = None
    momentum_score: Optional[float] = None
    opportunity_score: Optional[float] = None
    overall_score: Optional[float] = None
    recommendation: Optional[str] = None

    fair_value: Optional[float] = None
    upside_percent: Optional[float] = None
    valuation_label: Optional[str] = None

    last_updated_utc: Optional[str] = None
    sources: List[str] = Field(default_factory=list)

    notes: Optional[str] = None
    error: Optional[str] = None


class BatchAnalysisRequest(_ExtraIgnoreBase):
    tickers: List[str] = Field(default_factory=list)
    sheet_name: Optional[str] = None  # optional, safe for Sheets client


class BatchAnalysisResponse(_ExtraIgnoreBase):
    results: List[SingleAnalysisResponse]


class SheetAnalysisResponse(_ExtraIgnoreBase):
    headers: List[str]
    rows: List[List[Any]]


# ----------------------------------------------------------------------
# INTERNAL HELPERS
# ----------------------------------------------------------------------
def _now_utc_iso() -> str:
    return datetime.now(timezone.utc).isoformat()

def _clean_tickers(items: Sequence[str]) -> List[str]:
    raw = [(x or "").strip() for x in (items or [])]
    raw = [x for x in raw if x]
    seen = set()
    out: List[str] = []
    for x in raw:
        ux = x.upper()
        if ux in seen:
            continue
        seen.add(ux)
        out.append(ux)
    return out

def _chunk(items: List[str], size: int) -> List[List[str]]:
    if size <= 0:
        return [items]
    return [items[i : i + size] for i in range(0, len(items), size)]

def _extract_sources(q: EnrichedQuote) -> List[str]:
    # EnrichedQuote inherits sources from UnifiedQuote
    if not q.sources:
        return [q.data_source] if q.data_source else []
    return [s.provider for s in q.sources]

# ----------------------------------------------------------------------
# ENGINE CALLS
# ----------------------------------------------------------------------
async def _get_quotes_chunked(
    symbols: List[str],
    *,
    batch_size: int = DEFAULT_BATCH_SIZE,
    timeout_sec: float = DEFAULT_BATCH_TIMEOUT,
    max_concurrency: int = DEFAULT_CONCURRENCY,
) -> Dict[str, UnifiedQuote]:
    """
    Returns map keyed by normalized input symbol.
    """
    clean = _clean_tickers(symbols)
    if not clean:
        return {}

    engine = _get_engine_singleton()
    chunks = _chunk(clean, batch_size)
    sem = asyncio.Semaphore(max(1, max_concurrency))

    async def _run_chunk(chunk_syms: List[str]) -> Tuple[List[str], List[UnifiedQuote] | Exception]:
        async with sem:
            try:
                res = await asyncio.wait_for(engine.get_enriched_quotes(chunk_syms), timeout=timeout_sec)
                return chunk_syms, res
            except Exception as e:
                return chunk_syms, e

    results = await asyncio.gather(*[_run_chunk(c) for c in chunks])

    out: Dict[str, UnifiedQuote] = {}
    for chunk_syms, res in results:
        if isinstance(res, Exception):
            msg = "Engine batch timeout" if isinstance(res, asyncio.TimeoutError) else f"Engine batch error: {res}"
            logger.warning("ai_analysis: %s for chunk(size=%d): %s", msg, len(chunk_syms), chunk_syms)
            for s in chunk_syms:
                out[s] = UnifiedQuote(symbol=s.upper(), data_quality="MISSING", error=msg).finalize()
            continue

        returned = list(res or [])
        chunk_map = {q.symbol.upper(): q for q in returned}

        for s in chunk_syms:
            s_norm = s.upper()
            if s_norm in chunk_map:
                out[s] = chunk_map[s_norm]
            else:
                out[s] = UnifiedQuote(symbol=s_norm, data_quality="MISSING", error="No data returned").finalize()

    return out


# ----------------------------------------------------------------------
# TRANSFORM: UnifiedQuote -> SingleAnalysisResponse
# ----------------------------------------------------------------------
def _quote_to_analysis(raw_symbol: str, uq: UnifiedQuote) -> SingleAnalysisResponse:
    # 1. Convert to EnrichedQuote
    eq = EnrichedQuote.from_unified(uq)
    
    # 2. Enrich with Scores (consistent logic with Scoring Engine)
    # This ensures calculated scores are populated even if engine returned raw data
    scored_eq = enrich_with_scores(eq)

    # 3. Map to Response
    return SingleAnalysisResponse(
        symbol=scored_eq.symbol or raw_symbol,
        name=scored_eq.name,
        market_region=scored_eq.market or scored_eq.market_region,
        
        price=scored_eq.current_price or scored_eq.last_price,
        change_pct=scored_eq.percent_change or scored_eq.change_percent,
        market_cap=scored_eq.market_cap,
        pe_ttm=scored_eq.pe_ttm,
        pb=scored_eq.pb,
        
        dividend_yield=scored_eq.dividend_yield,
        roe=scored_eq.roe,
        roa=scored_eq.roa,
        
        data_quality=scored_eq.data_quality,
        
        value_score=scored_eq.value_score,
        quality_score=scored_eq.quality_score,
        momentum_score=scored_eq.momentum_score,
        opportunity_score=scored_eq.opportunity_score,
        overall_score=scored_eq.overall_score,
        recommendation=scored_eq.recommendation,
        
        fair_value=scored_eq.fair_value,
        upside_percent=scored_eq.upside_percent,
        valuation_label=scored_eq.valuation_label,
        
        last_updated_utc=scored_eq.last_updated_utc,
        sources=_extract_sources(scored_eq),
        
        notes=None, # can add logic if UnifiedQuote has notes
        error=scored_eq.error
    )


# ----------------------------------------------------------------------
# SHEET HELPERS
# ----------------------------------------------------------------------
def _build_sheet_headers() -> List[str]:
    # Fixed schema for "AI Analysis" tabs in Sheets
    return [
        "Symbol",
        "Company Name",
        "Market / Region",
        "Price",
        "Change %",
        "Market Cap",
        "P/E (TTM)",
        "P/B",
        "Dividend Yield %",
        "ROE %",
        "Data Quality",
        "Value Score",
        "Quality Score",
        "Momentum Score",
        "Opportunity Score",
        "Overall Score",
        "Recommendation",
        "Last Updated (UTC)",
        "Sources",
        "Fair Value",
        "Upside %",
        "Valuation Label",
        "Error",
    ]

def _pad_row(row: List[Any], length: int) -> List[Any]:
    if len(row) == length:
        return row
    if len(row) < length:
        return row + [None] * (length - len(row))
    return row[:length]

def _analysis_to_sheet_row(a: SingleAnalysisResponse, header_len: int) -> List[Any]:
    # Normalize percentages for display (assuming 0.05 -> 5.0 for sheet logic if needed, 
    # but typically raw decimals are better for Sheets formatting. 
    # However, legacy sheet expects raw numbers often. 
    # Let's keep consistent with Scoring Engine inputs which are raw decimals usually)
    
    div_y = a.dividend_yield
    if div_y and abs(div_y) <= 1.0: div_y = div_y * 100.0 # Convert 0.05 to 5.0 for display
    
    roe = a.roe
    if roe and abs(roe) <= 1.0: roe = roe * 100.0

    row = [
        a.symbol,
        a.name or "",
        a.market_region or "",
        a.price,
        a.change_pct,
        a.market_cap,
        a.pe_ttm,
        a.pb,
        div_y,
        roe,
        a.data_quality,
        a.value_score,
        a.quality_score,
        a.momentum_score,
        a.opportunity_score,
        a.overall_score,
        a.recommendation,
        a.last_updated_utc,
        ", ".join(a.sources) if a.sources else "",
        a.fair_value,
        a.upside_percent,
        a.valuation_label or "",
        a.error or "",
    ]
    return _pad_row(row, header_len)


# ----------------------------------------------------------------------
# ROUTES
# ----------------------------------------------------------------------
@router.get("/health")
@router.get("/ping")
async def analysis_health() -> Dict[str, Any]:
    return {
        "status": "ok",
        "module": "routes.ai_analysis",
        "version": AI_ANALYSIS_VERSION,
        "engine_mode": "v2",
        "batch_size": DEFAULT_BATCH_SIZE,
        "batch_timeout_sec": DEFAULT_BATCH_TIMEOUT,
        "batch_concurrency": DEFAULT_CONCURRENCY,
        "max_tickers": DEFAULT_MAX_TICKERS,
        "timestamp_utc": _now_utc_iso(),
    }

@router.get("/quote", response_model=SingleAnalysisResponse)
async def analyze_single_quote(symbol: str = Query(..., alias="symbol")) -> SingleAnalysisResponse:
    t = (symbol or "").strip()
    if not t:
        return SingleAnalysisResponse(symbol="", data_quality="MISSING", error="Symbol is required")

    try:
        m = await _get_quotes_chunked([t], batch_size=1)
        q = m.get(t) or UnifiedQuote(symbol=t.upper(), data_quality="MISSING", error="No data returned").finalize()
        return _quote_to_analysis(t, q)
    except Exception as exc:
        logger.exception("ai_analysis: exception in /quote for %s", t)
        return SingleAnalysisResponse(symbol=t.upper(), data_quality="MISSING", error=f"Exception in analysis: {exc}")

@router.post("/quotes", response_model=BatchAnalysisResponse)
async def analyze_batch_quotes(body: BatchAnalysisRequest) -> BatchAnalysisResponse:
    tickers = _clean_tickers(body.tickers or [])
    if not tickers:
        return BatchAnalysisResponse(results=[])

    if len(tickers) > DEFAULT_MAX_TICKERS:
        tickers = tickers[:DEFAULT_MAX_TICKERS]

    try:
        m = await _get_quotes_chunked(tickers)
    except Exception as exc:
        logger.exception("ai_analysis: batch failure: %s", exc)
        return BatchAnalysisResponse(
            results=[SingleAnalysisResponse(symbol=t, data_quality="MISSING", error=f"Batch failure: {exc}") for t in tickers]
        )

    results: List[SingleAnalysisResponse] = []
    for t in tickers:
        q = m.get(t)
        if not q:
            q = UnifiedQuote(symbol=t.upper(), data_quality="MISSING", error="No data returned").finalize()
        
        try:
            results.append(_quote_to_analysis(t, q))
        except Exception as exc:
            logger.exception("ai_analysis: transform failure for %s: %s", t, exc)
            results.append(SingleAnalysisResponse(symbol=t, data_quality="MISSING", error=f"Transform failure: {exc}"))

    return BatchAnalysisResponse(results=results)

@router.post("/sheet-rows", response_model=SheetAnalysisResponse)
async def analyze_for_sheet(body: BatchAnalysisRequest) -> SheetAnalysisResponse:
    """
    Dedicated endpoint for AI Analysis sheet tab.
    """
    headers = _build_sheet_headers()
    header_len = len(headers)

    batch = await analyze_batch_quotes(body)
    rows = [_analysis_to_sheet_row(r, header_len) for r in batch.results]

    return SheetAnalysisResponse(headers=headers, rows=rows)

__all__ = ["router", "SingleAnalysisResponse", "BatchAnalysisRequest", "BatchAnalysisResponse", "SheetAnalysisResponse"]
