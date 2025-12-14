"""
routes/ai_analysis.py
------------------------------------------------------------
AI & QUANT ANALYSIS ROUTES – GOOGLE SHEETS FRIENDLY (v2.9.0)

- Uses core.data_engine_v2.DataEngine only (no direct provider calls).
- Defensive batching: chunking + timeout + bounded concurrency.
- Sheets-safe: /sheet-rows always returns 200 with headers + rows.

Author: Emad Bahbah (with GPT-5.2 Thinking)
"""

from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timezone
from functools import lru_cache
from typing import Any, Dict, List, Optional, Sequence, Tuple

from fastapi import APIRouter, Query
from pydantic import BaseModel, Field

from env import settings
from core.data_engine_v2 import DataEngine, UnifiedQuote
from core.enriched_quote import EnrichedQuote
from core.scoring_engine import enrich_with_scores

logger = logging.getLogger("routes.ai_analysis")
AI_ANALYSIS_VERSION = "2.9.0"

DEFAULT_BATCH_SIZE = int(settings.ai_batch_size)
DEFAULT_BATCH_TIMEOUT = float(settings.ai_batch_timeout_sec)
DEFAULT_CONCURRENCY = int(settings.ai_batch_concurrency)
DEFAULT_MAX_TICKERS = int(settings.ai_max_tickers)

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
# MODELS
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

    dividend_yield: Optional[float] = None
    roe: Optional[float] = None
    roa: Optional[float] = None

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
    sheet_name: Optional[str] = None

class BatchAnalysisResponse(_ExtraIgnoreBase):
    results: List[SingleAnalysisResponse]

class SheetAnalysisResponse(_ExtraIgnoreBase):
    headers: List[str]
    rows: List[List[Any]]

# ----------------------------------------------------------------------
# HELPERS
# ----------------------------------------------------------------------
def _now_utc_iso() -> str:
    return datetime.now(timezone.utc).isoformat()

def _normalize_symbol(symbol: str) -> str:
    s = (symbol or "").strip().upper()
    if not s:
        return ""
    if s.startswith("TADAWUL:"):
        s = s.split(":", 1)[1].strip()
    if s.endswith(".TADAWUL"):
        s = s.replace(".TADAWUL", ".SR")
    if s.isdigit():
        s = f"{s}.SR"
    return s

def _clean_tickers(items: Sequence[str]) -> List[str]:
    raw = [_normalize_symbol(x) for x in (items or [])]
    raw = [x for x in raw if x]
    seen = set()
    out: List[str] = []
    for x in raw:
        if x in seen:
            continue
        seen.add(x)
        out.append(x)
    return out

def _chunk(items: List[str], size: int) -> List[List[str]]:
    if size <= 0:
        return [items]
    return [items[i : i + size] for i in range(0, len(items), size)]

def _extract_sources(q: EnrichedQuote) -> List[str]:
    if getattr(q, "sources", None):
        try:
            return [s.provider for s in q.sources]  # type: ignore
        except Exception:
            pass
    ds = getattr(q, "data_source", None)
    return [ds] if ds else []

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
            logger.warning("ai_analysis: %s for chunk(size=%d)", msg, len(chunk_syms))
            for s in chunk_syms:
                out[s] = UnifiedQuote(symbol=s.upper(), data_quality="MISSING", error=msg).finalize()
            continue

        returned = list(res or [])
        chunk_map = {q.symbol.upper(): q for q in returned}

        for s in chunk_syms:
            s_norm = s.upper()
            out[s] = chunk_map.get(s_norm) or UnifiedQuote(symbol=s_norm, data_quality="MISSING", error="No data returned").finalize()

    return out

# ----------------------------------------------------------------------
# TRANSFORM
# ----------------------------------------------------------------------
def _quote_to_analysis(raw_symbol: str, uq: UnifiedQuote) -> SingleAnalysisResponse:
    eq = EnrichedQuote.from_unified(uq)
    eq = enrich_with_scores(eq)

    return SingleAnalysisResponse(
        symbol=eq.symbol or _normalize_symbol(raw_symbol) or raw_symbol,
        name=getattr(eq, "name", None),
        market_region=getattr(eq, "market", None) or getattr(eq, "market_region", None),

        price=getattr(eq, "current_price", None) or getattr(eq, "last_price", None),
        change_pct=getattr(eq, "percent_change", None) or getattr(eq, "change_percent", None),
        market_cap=getattr(eq, "market_cap", None),
        pe_ttm=getattr(eq, "pe_ttm", None),
        pb=getattr(eq, "pb", None),

        dividend_yield=getattr(eq, "dividend_yield", None),
        roe=getattr(eq, "roe", None),
        roa=getattr(eq, "roa", None),

        data_quality=getattr(eq, "data_quality", "MISSING"),

        value_score=getattr(eq, "value_score", None),
        quality_score=getattr(eq, "quality_score", None),
        momentum_score=getattr(eq, "momentum_score", None),
        opportunity_score=getattr(eq, "opportunity_score", None),
        overall_score=getattr(eq, "overall_score", None),
        recommendation=getattr(eq, "recommendation", None),

        fair_value=getattr(eq, "fair_value", None),
        upside_percent=getattr(eq, "upside_percent", None),
        valuation_label=getattr(eq, "valuation_label", None),

        last_updated_utc=getattr(eq, "last_updated_utc", None),
        sources=_extract_sources(eq),

        error=getattr(eq, "error", None),
    )

# ----------------------------------------------------------------------
# SHEET HELPERS
# ----------------------------------------------------------------------
def _build_sheet_headers() -> List[str]:
    return [
        "Symbol", "Company Name", "Market / Region", "Price", "Change %",
        "Market Cap", "P/E (TTM)", "P/B",
        "Dividend Yield %", "ROE %", "ROA %",
        "Data Quality",
        "Value Score", "Quality Score", "Momentum Score", "Opportunity Score", "Overall Score",
        "Recommendation",
        "Last Updated (UTC)", "Sources",
        "Fair Value", "Upside %", "Valuation Label",
        "Error",
    ]

def _pad_row(row: List[Any], length: int) -> List[Any]:
    if len(row) < length:
        return row + [None] * (length - len(row))
    return row[:length]

def _pct_display(v: Optional[float]) -> Optional[float]:
    if v is None:
        return None
    try:
        if abs(v) <= 1.0 and v != 0.0:
            return v * 100.0
        return v
    except Exception:
        return v

def _analysis_to_sheet_row(a: SingleAnalysisResponse, header_len: int) -> List[Any]:
    row = [
        a.symbol,
        a.name or "",
        a.market_region or "",
        a.price,
        a.change_pct,
        a.market_cap,
        a.pe_ttm,
        a.pb,
        _pct_display(a.dividend_yield),
        _pct_display(a.roe),
        _pct_display(a.roa),
        a.data_quality,
        a.value_score,
        a.quality_score,
        a.momentum_score,
        a.opportunity_score,
        a.overall_score,
        a.recommendation or "",
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
        key = _normalize_symbol(t) or t.upper()
        q = m.get(key) or UnifiedQuote(symbol=key, data_quality="MISSING", error="No data returned").finalize()
        return _quote_to_analysis(t, q)
    except Exception as exc:
        logger.exception("ai_analysis: exception in /quote for %s", t)
        return SingleAnalysisResponse(symbol=_normalize_symbol(t) or t.upper(), data_quality="MISSING", error=f"Exception in analysis: {exc}")

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
        return BatchAnalysisResponse(results=[SingleAnalysisResponse(symbol=t, data_quality="MISSING", error=f"Batch failure: {exc}") for t in tickers])

    results: List[SingleAnalysisResponse] = []
    for t in tickers:
        q = m.get(t) or UnifiedQuote(symbol=t.upper(), data_quality="MISSING", error="No data returned").finalize()
        try:
            results.append(_quote_to_analysis(t, q))
        except Exception as exc:
            logger.exception("ai_analysis: transform failure for %s: %s", t, exc)
            results.append(SingleAnalysisResponse(symbol=t, data_quality="MISSING", error=f"Transform failure: {exc}"))

    return BatchAnalysisResponse(results=results)

@router.post("/sheet-rows", response_model=SheetAnalysisResponse)
async def analyze_for_sheet(body: BatchAnalysisRequest) -> SheetAnalysisResponse:
    headers = _build_sheet_headers()
    header_len = len(headers)
    batch = await analyze_batch_quotes(body)
    rows = [_analysis_to_sheet_row(r, header_len) for r in batch.results]
    return SheetAnalysisResponse(headers=headers, rows=rows)

__all__ = ["router", "SingleAnalysisResponse", "BatchAnalysisRequest", "BatchAnalysisResponse", "SheetAnalysisResponse"]
