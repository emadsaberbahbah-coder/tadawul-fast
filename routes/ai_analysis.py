"""
routes/ai_analysis.py
------------------------------------------------------------
AI & QUANT ANALYSIS ROUTES – GOOGLE SHEETS FRIENDLY (v3.1.0)

Preferred backend:
  - core.data_engine_v2.DataEngine (class-based unified engine)

Provides:
  - GET  /v1/analysis/health   (alias: /ping)
  - GET  /v1/analysis/quote?symbol=...
  - POST /v1/analysis/quotes
  - POST /v1/analysis/sheet-rows   (headers + rows for Google Sheets)

Design goals:
  - KSA-safe: router never calls providers directly.
  - Extremely defensive: bounded batch size, chunking, timeout, concurrency.
  - Never throws 502 for Sheets endpoints; returns headers + rows (maybe empty).
  - Alignment: Uses core.scoring_engine to ensure score fields match other routes.

CRITICAL FIXES vs old versions:
  - Uses engine.get_quote / engine.get_quotes (these exist).
  - Removes calls to non-existent methods: get_enriched_quote(s), finalize().
  - Uses FastAPI app.state.engine if present (created by main.py lifespan),
    otherwise falls back to local singleton (safe standalone import).
  - Works with the current UnifiedQuote fields in core/data_engine_v2.py.
"""

from __future__ import annotations

import asyncio
import logging
import os
from datetime import datetime, timezone
from functools import lru_cache
from typing import Any, Dict, List, Optional, Sequence, Tuple

from fastapi import APIRouter, Depends, Query, Request
from pydantic import BaseModel, Field, ConfigDict

from core.config import get_settings
from core.data_engine_v2 import DataEngine, UnifiedQuote
from core.enriched_quote import EnrichedQuote
from core.scoring_engine import enrich_with_scores

logger = logging.getLogger("routes.ai_analysis")
settings = get_settings()

AI_ANALYSIS_VERSION = "3.1.0"

# ----------------------------------------------------------------------
# Runtime limits (env overrides)
# ----------------------------------------------------------------------
DEFAULT_BATCH_SIZE = int(os.getenv("AI_BATCH_SIZE", "25"))
DEFAULT_BATCH_TIMEOUT = float(os.getenv("AI_BATCH_TIMEOUT_SEC", "45"))
DEFAULT_CONCURRENCY = int(os.getenv("AI_BATCH_CONCURRENCY", "6"))
DEFAULT_MAX_TICKERS = int(os.getenv("AI_MAX_TICKERS", "500"))

# ----------------------------------------------------------------------
# Engine resolution
# ----------------------------------------------------------------------
@lru_cache(maxsize=1)
def _engine_singleton() -> DataEngine:
    logger.info("routes.ai_analysis: initializing local DataEngine singleton (fallback)")
    return DataEngine()

def get_engine(request: Request) -> DataEngine:
    """
    Prefer the engine created in main.py lifespan (request.app.state.engine).
    Fall back to a local singleton if not present.
    """
    eng = getattr(request.app.state, "engine", None)
    if isinstance(eng, DataEngine):
        return eng
    return _engine_singleton()

# ----------------------------------------------------------------------
# Router
# ----------------------------------------------------------------------
router = APIRouter(prefix="/v1/analysis", tags=["AI & Analysis"])

# ----------------------------------------------------------------------
# Models (Pydantic v2) - ignore extras to avoid 422 from Sheets clients
# ----------------------------------------------------------------------
class _ExtraIgnoreBase(BaseModel):
    model_config = ConfigDict(extra="ignore")

class SingleAnalysisResponse(_ExtraIgnoreBase):
    symbol: str
    name: Optional[str] = None
    market: Optional[str] = None
    currency: Optional[str] = None

    price: Optional[float] = None
    change_pct: Optional[float] = None
    market_cap: Optional[float] = None
    pe_ttm: Optional[float] = None
    pb: Optional[float] = None

    dividend_yield: Optional[float] = None  # may be fraction or percent; sheet normalizes
    roe: Optional[float] = None
    net_margin: Optional[float] = None

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
    sheet_name: Optional[str] = None  # reserved for future use (safe for Sheets client)

class BatchAnalysisResponse(_ExtraIgnoreBase):
    results: List[SingleAnalysisResponse] = Field(default_factory=list)

class SheetAnalysisResponse(_ExtraIgnoreBase):
    headers: List[str] = Field(default_factory=list)
    rows: List[List[Any]] = Field(default_factory=list)

# ----------------------------------------------------------------------
# Helpers
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
        out.append(x)  # keep original casing; engine normalizes internally
    return out

def _chunk(items: List[str], size: int) -> List[List[str]]:
    if size <= 0:
        return [items]
    return [items[i : i + size] for i in range(0, len(items), size)]

def _extract_sources(eq: EnrichedQuote) -> List[str]:
    # Current UnifiedQuote exposes data_source string only
    ds = getattr(eq, "data_source", None)
    return [ds] if ds else []

# ----------------------------------------------------------------------
# Engine calls (chunked + bounded concurrency)
# ----------------------------------------------------------------------
async def _get_quotes_chunked(
    engine: DataEngine,
    symbols: List[str],
    *,
    batch_size: int = DEFAULT_BATCH_SIZE,
    timeout_sec: float = DEFAULT_BATCH_TIMEOUT,
    max_concurrency: int = DEFAULT_CONCURRENCY,
) -> Dict[str, UnifiedQuote]:
    """
    Returns a map keyed by UPPERCASE SYMBOL as returned by the engine.
    Extremely defensive: never raises.
    """
    clean = _clean_tickers(symbols)
    if not clean:
        return {}

    chunks = _chunk(clean, batch_size)
    sem = asyncio.Semaphore(max(1, max_concurrency))

    async def _run_chunk(chunk_syms: List[str]) -> Tuple[List[str], List[UnifiedQuote] | Exception]:
        async with sem:
            try:
                res = await asyncio.wait_for(engine.get_quotes(chunk_syms), timeout=timeout_sec)
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
                uq = UnifiedQuote(symbol=str(s).upper(), data_quality="MISSING", error=msg)
                out[uq.symbol.upper()] = uq
            continue

        returned = list(res or [])
        for q in returned:
            out[(q.symbol or "").upper()] = q

        # Ensure every requested symbol has a placeholder (best-effort by key match)
        # NOTE: Engine may normalize (e.g., 1120 -> 1120.SR). We can't perfectly map
        # without reusing engine.normalize_symbol, so we just ensure "something" exists.
        # Caller will fallback to single fetch if needed.
        for s in chunk_syms:
            s_up = str(s).upper()
            if s_up not in out:
                out[s_up] = UnifiedQuote(symbol=s_up, data_quality="MISSING", error="No data returned")

    return out

# ----------------------------------------------------------------------
# Transform: UnifiedQuote -> SingleAnalysisResponse
# ----------------------------------------------------------------------
def _quote_to_analysis(raw_symbol: str, uq: UnifiedQuote) -> SingleAnalysisResponse:
    # Convert to EnrichedQuote then compute scores (consistent across routes)
    eq = EnrichedQuote.from_unified(uq)
    eq = enrich_with_scores(eq)

    # Prefer engine-normalized symbol, fallback to user input
    sym = (getattr(eq, "symbol", None) or raw_symbol or "").strip().upper()

    return SingleAnalysisResponse(
        symbol=sym,
        name=getattr(eq, "name", None),
        market=getattr(eq, "market", None),
        currency=getattr(eq, "currency", None),

        price=getattr(eq, "current_price", None),
        change_pct=getattr(eq, "percent_change", None),
        market_cap=getattr(eq, "market_cap", None),
        pe_ttm=getattr(eq, "pe_ttm", None),
        pb=getattr(eq, "pb", None),

        dividend_yield=getattr(eq, "dividend_yield", None),
        roe=getattr(eq, "roe", None),
        net_margin=getattr(eq, "net_margin", None),

        data_quality=getattr(eq, "data_quality", "MISSING") or "MISSING",

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

        notes=None,
        error=getattr(eq, "error", None),
    )

# ----------------------------------------------------------------------
# Sheet helpers
# ----------------------------------------------------------------------
def _build_sheet_headers() -> List[str]:
    # Fixed schema for "AI Analysis" tabs in Sheets
    return [
        "Symbol",
        "Company Name",
        "Market",
        "Currency",
        "Price",
        "Change %",
        "Market Cap",
        "P/E (TTM)",
        "P/B",
        "Dividend Yield %",
        "ROE %",
        "Net Margin %",
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

def _as_percent_maybe(x: Optional[float]) -> Optional[float]:
    # Convert 0.05 -> 5.0 but keep 5.0 as 5.0
    if x is None:
        return None
    try:
        if abs(x) <= 1.0 and x != 0:
            return x * 100.0
        return x
    except Exception:
        return x

def _analysis_to_sheet_row(a: SingleAnalysisResponse, header_len: int) -> List[Any]:
    row = [
        a.symbol,
        a.name or "",
        a.market or "",
        a.currency or "",
        a.price,
        a.change_pct,
        a.market_cap,
        a.pe_ttm,
        a.pb,
        _as_percent_maybe(a.dividend_yield),
        _as_percent_maybe(a.roe),
        _as_percent_maybe(a.net_margin),
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
# Routes
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
        "providers": {
            "eodhd_enabled": bool(getattr(settings, "EODHD_API_KEY", None)),
            "fmp_enabled": bool(getattr(settings, "FMP_API_KEY", None)),
            "yfinance_enabled": bool(getattr(settings, "ENABLE_YFINANCE", False)),
            "ksa_argaam_scrape": True,
        },
        "timestamp_utc": _now_utc_iso(),
    }

@router.get("/quote", response_model=SingleAnalysisResponse)
async def analyze_single_quote(
    request: Request,
    symbol: str = Query(..., alias="symbol"),
    engine: DataEngine = Depends(get_engine),
) -> SingleAnalysisResponse:
    t = (symbol or "").strip()
    if not t:
        return SingleAnalysisResponse(symbol="", data_quality="MISSING", error="Symbol is required")

    try:
        uq = await engine.get_quote(t)
        return _quote_to_analysis(t, uq)
    except Exception as exc:
        logger.exception("ai_analysis: exception in /quote for %s", t)
        return SingleAnalysisResponse(symbol=t.upper(), data_quality="MISSING", error=f"Exception in analysis: {exc}")

@router.post("/quotes", response_model=BatchAnalysisResponse)
async def analyze_batch_quotes(
    request: Request,
    body: BatchAnalysisRequest,
    engine: DataEngine = Depends(get_engine),
) -> BatchAnalysisResponse:
    tickers = _clean_tickers(body.tickers or [])
    if not tickers:
        return BatchAnalysisResponse(results=[])

    if len(tickers) > DEFAULT_MAX_TICKERS:
        tickers = tickers[:DEFAULT_MAX_TICKERS]

    # Chunked fetch (defensive)
    try:
        m = await _get_quotes_chunked(engine, tickers)
    except Exception as exc:
        logger.exception("ai_analysis: batch failure: %s", exc)
        return BatchAnalysisResponse(
            results=[SingleAnalysisResponse(symbol=t.upper(), data_quality="MISSING", error=f"Batch failure: {exc}") for t in tickers]
        )

    results: List[SingleAnalysisResponse] = []
    for t in tickers:
        # Best-effort lookup:
        # - Try exact key
        # - If missing, do a single fetch (keeps accuracy when engine normalizes symbols)
        uq = m.get(t.upper())
        if uq is None:
            try:
                uq = await engine.get_quote(t)
            except Exception as e:
                uq = UnifiedQuote(symbol=t.upper(), data_quality="MISSING", error=str(e))

        try:
            results.append(_quote_to_analysis(t, uq))
        except Exception as exc:
            logger.exception("ai_analysis: transform failure for %s: %s", t, exc)
            results.append(SingleAnalysisResponse(symbol=t.upper(), data_quality="MISSING", error=f"Transform failure: {exc}"))

    return BatchAnalysisResponse(results=results)

@router.post("/sheet-rows", response_model=SheetAnalysisResponse)
async def analyze_for_sheet(
    request: Request,
    body: BatchAnalysisRequest,
    engine: DataEngine = Depends(get_engine),
) -> SheetAnalysisResponse:
    """
    Dedicated endpoint for AI Analysis sheet tab.
    Must be "Sheets-safe": never raises, always returns headers + rows.
    """
    headers = _build_sheet_headers()
    header_len = len(headers)

    try:
        batch = await analyze_batch_quotes(request, body, engine)
        rows = [_analysis_to_sheet_row(r, header_len) for r in (batch.results or [])]
        return SheetAnalysisResponse(headers=headers, rows=rows)
    except Exception as exc:
        logger.exception("ai_analysis: /sheet-rows failure: %s", exc)
        return SheetAnalysisResponse(headers=headers, rows=[])

__all__ = [
    "router",
    "SingleAnalysisResponse",
    "BatchAnalysisRequest",
    "BatchAnalysisResponse",
    "SheetAnalysisResponse",
]
