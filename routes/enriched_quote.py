"""
routes/enriched_quote.py
===========================================================
Enriched Quotes Router (v3.5.0)

Unified on top of:
  - core.data_engine_v2.DataEngine (async, multi-provider, KSA-safe)
  - core.enriched_quote.EnrichedQuote (sheet-friendly model)
  - core.schemas.get_headers_for_sheet (59-column schema)
  - core.scoring_engine (AI scoring & recommendations)

Responsibilities
----------------
- GET  /v1/enriched/health        -> router + engine diagnostics
- GET  /v1/enriched/headers       -> headers only (debug / Apps Script setup)
- GET  /v1/enriched/quote         -> single EnrichedQuote JSON (with scores)
- POST /v1/enriched/quotes        -> batch EnrichedQuote JSON objects (with scores)
- POST /v1/enriched/sheet-rows    -> headers + rows for Google Sheets

Design notes
------------
- Router NEVER talks directly to market providers.
- Very defensive: chunked calls + concurrency limit + timeout per chunk.
- Singleton engine per process to avoid repeated DataEngine init.
- AUTOMATIC SCORING: All quotes are passed through core.scoring_engine.

Author: Emad Bahbah (with GPT-5.2 Thinking)
"""

from __future__ import annotations

import asyncio
import logging
import os
from functools import lru_cache
from typing import Any, Dict, List, Optional, Sequence, Tuple, Union

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel, Field

# --- CORE IMPORTS ---
from core.data_engine_v2 import DataEngine, UnifiedQuote
from core.enriched_quote import EnrichedQuote
from core.scoring_engine import enrich_with_scores
from core.schemas import get_headers_for_sheet

logger = logging.getLogger("routes.enriched_quote")

API_VERSION = "3.5.0"

# =============================================================================
# Config (env override)
# =============================================================================

DEFAULT_BATCH_SIZE = int(os.getenv("ENRICHED_BATCH_SIZE", "40") or 40)
DEFAULT_BATCH_TIMEOUT = float(os.getenv("ENRICHED_BATCH_TIMEOUT_SEC", "45") or 45)
DEFAULT_MAX_TICKERS = int(os.getenv("ENRICHED_MAX_TICKERS", "250") or 250)
DEFAULT_MAX_CONCURRENCY = int(os.getenv("ENRICHED_BATCH_CONCURRENCY", "5") or 5)

# =============================================================================
# Optional env.py integration (safe)
# =============================================================================

try:  # pragma: no cover
    import env as _env_mod  # type: ignore
except Exception:  # pragma: no cover
    _env_mod = None  # type: ignore


def _get_env_attr(name: str, default: Any = None) -> Any:
    """
    Read from env.py first (if present), then OS env vars.
    """
    if _env_mod is not None and hasattr(_env_mod, name):
        try:
            return getattr(_env_mod, name)
        except Exception:
            return default
    return os.getenv(name, default)


# =============================================================================
# Symbol normalization & list helpers
# =============================================================================

def _normalize_symbol(symbol: str) -> str:
    """
    Normalization rules (aligned with legacy_service):
      - 1120 -> 1120.SR
      - TADAWUL:1120 -> 1120.SR
      - 1120.TADAWUL -> 1120.SR
      - Trim + upper
    """
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


def _dedupe_preserve_order(items: Sequence[str]) -> List[str]:
    seen = set()
    out: List[str] = []
    for x in items or []:
        if not x:
            continue
        if x in seen:
            continue
        seen.add(x)
        out.append(x)
    return out


def _clean_tickers(tickers: Sequence[str]) -> List[str]:
    clean = [_normalize_symbol(t) for t in (tickers or [])]
    clean = [t for t in clean if t]
    return _dedupe_preserve_order(clean)


def _chunk(items: List[str], size: int) -> List[List[str]]:
    if size <= 0:
        return [items]
    return [items[i : i + size] for i in range(0, len(items), size)]


# =============================================================================
# Engine Singleton
# =============================================================================

@lru_cache(maxsize=1)
def _get_engine_singleton() -> DataEngine:
    """
    Ensure we only initialize DataEngine once per process.
    """
    logger.info("routes.enriched_quote: Initializing core.data_engine_v2.DataEngine singleton")
    return DataEngine()


# =============================================================================
# Core Async Logic
# =============================================================================

def _make_missing_unified(symbol: str, message: str) -> UnifiedQuote:
    sym = _normalize_symbol(symbol) or (symbol or "").strip().upper() or "UNKNOWN"
    return UnifiedQuote(symbol=sym, data_quality="MISSING", error=message).finalize()


async def _get_unified_quotes_chunked(
    symbols: List[str],
    *,
    batch_size: int = DEFAULT_BATCH_SIZE,
    timeout_sec: float = DEFAULT_BATCH_TIMEOUT,
    max_concurrency: int = DEFAULT_MAX_CONCURRENCY,
) -> Dict[str, UnifiedQuote]:
    """
    Chunked + concurrency-limited batch fetch using V2 Engine.
    Returns dict keyed by the (normalized) input symbol string.
    """
    clean = _clean_tickers(symbols)
    if not clean:
        return {}

    engine = _get_engine_singleton()
    chunks = _chunk(clean, batch_size)
    sem = asyncio.Semaphore(max(1, max_concurrency))

    logger.info(
        "Enriched batch: %d symbols in %d chunk(s) (batch=%d, timeout=%.1fs, conc=%d)",
        len(clean),
        len(chunks),
        batch_size,
        timeout_sec,
        max(1, max_concurrency),
    )

    async def _run_chunk(chunk_syms: List[str]) -> Tuple[List[str], List[UnifiedQuote] | Exception]:
        async with sem:
            try:
                # V2 Engine returns List[UnifiedQuote]
                result = await asyncio.wait_for(engine.get_enriched_quotes(chunk_syms), timeout=timeout_sec)
                return chunk_syms, result
            except Exception as e:
                return chunk_syms, e

    results = await asyncio.gather(*[_run_chunk(c) for c in chunks])

    out: Dict[str, UnifiedQuote] = {}
    
    for chunk_syms, res in results:
        # Handle failures
        if isinstance(res, Exception):
            msg = "Engine batch timeout" if isinstance(res, asyncio.TimeoutError) else f"Engine batch error: {res}"
            logger.warning("%s for chunk(size=%d): %s", msg, len(chunk_syms), chunk_syms)
            for s in chunk_syms:
                out[s] = _make_missing_unified(s, msg)
            continue

        # Handle success
        returned = list(res or [])
        chunk_map = {q.symbol.upper(): q for q in returned}

        # Preserve order and ensure each input has an output
        for s in chunk_syms:
            s_norm = s.upper()
            if s_norm in chunk_map:
                out[s] = chunk_map[s_norm]
            else:
                out[s] = _make_missing_unified(s, "No data returned from engine")

    return out


def _to_enriched(uq: UnifiedQuote, fallback_symbol: str) -> EnrichedQuote:
    """
    Convert UnifiedQuote -> EnrichedQuote AND apply Scoring Logic.
    """
    sym = _normalize_symbol(fallback_symbol) or (fallback_symbol or "").strip().upper() or "UNKNOWN"
    
    try:
        # 1. Convert to EnrichedQuote wrapper
        eq = EnrichedQuote.from_unified(uq)
        
        # 2. Apply Scoring Engine (Calculates scores if missing, adds recommendation)
        scored_eq = enrich_with_scores(eq)
        
        return scored_eq
        
    except Exception as exc:
        logger.exception("UnifiedQuote -> EnrichedQuote conversion failed for %s", sym, exc_info=exc)
        # Return safe error object
        return EnrichedQuote(symbol=sym, data_quality="MISSING", error=f"Conversion error: {exc}")


async def _build_sheet_rows(
    symbols: List[str],
    sheet_name: Optional[str],
) -> Tuple[List[str], List[List[Any]]]:
    """
    Fetch data and map to 59-column template for Google Sheets.
    """
    # 1. Get correct headers (universal template)
    headers = get_headers_for_sheet(sheet_name or "Global_Markets")
    
    clean = _clean_tickers(symbols)
    if not clean:
        return headers, []

    # 2. Fetch data (UnifiedQuotes)
    unified_map = await _get_unified_quotes_chunked(clean)
    
    rows: List[List[Any]] = []

    for s in clean:
        uq = unified_map.get(s) or _make_missing_unified(s, "No data returned from engine")
        
        # 3. Convert & Score
        enriched = _to_enriched(uq, s)

        # 4. Map to Sheet Row using headers
        try:
            row = enriched.to_row(headers)
            
            # Defensive padding
            if len(row) < len(headers):
                row = row + [None] * (len(headers) - len(row))
            elif len(row) > len(headers):
                row = row[: len(headers)]
                
        except Exception as exc:
            logger.exception("enriched.to_row failed for %s", s, exc_info=exc)
            # Fallback row
            row = [getattr(enriched, "symbol", s)] + [None] * (len(headers) - 1)

        rows.append(row)

    return headers, rows


# =============================================================================
# FastAPI router & request/response models
# =============================================================================

router = APIRouter(prefix="/v1/enriched", tags=["Enriched Quotes"])


class BatchEnrichedRequest(BaseModel):
    tickers: List[str] = Field(default_factory=list, description="Symbols, e.g. ['AAPL','MSFT','1120.SR']")
    sheet_name: Optional[str] = Field(default=None, description="Optional sheet name for header selection")


class BatchEnrichedResponse(BaseModel):
    results: List[EnrichedQuote]


class SheetEnrichedResponse(BaseModel):
    headers: List[str]
    rows: List[List[Any]]


# =============================================================================
# Routes
# =============================================================================

@router.get("/health")
async def enriched_health() -> Dict[str, Any]:
    return {
        "status": "ok",
        "module": "enriched_quote",
        "version": API_VERSION,
        "engine_mode": "v2",
        "engine_is_stub": False,
        "batch_size": DEFAULT_BATCH_SIZE,
        "batch_timeout_sec": DEFAULT_BATCH_TIMEOUT,
        "max_tickers": DEFAULT_MAX_TICKERS,
        "batch_concurrency": DEFAULT_MAX_CONCURRENCY,
    }


@router.get("/headers")
async def get_headers(
    sheet_name: Optional[str] = Query(default=None, description="Sheet name, e.g. 'Global_Markets'"),
) -> Dict[str, Any]:
    headers = get_headers_for_sheet(sheet_name)
    return {
        "sheet_name": sheet_name,
        "count": len(headers),
        "headers": headers,
        "version": API_VERSION,
    }


@router.get("/quote", response_model=EnrichedQuote, response_model_exclude_none=True)
async def get_enriched_quote_route(
    symbol: str = Query(..., alias="symbol"),
) -> EnrichedQuote:
    ticker = _normalize_symbol(symbol)
    if not ticker:
        raise HTTPException(status_code=400, detail="Symbol is required")

    try:
        engine = _get_engine_singleton()
        uq = await engine.get_enriched_quote(ticker)
        return _to_enriched(uq, ticker)
    except Exception as exc:
        logger.exception("engine.get_enriched_quote failed for %s", ticker, exc_info=exc)
        return _to_enriched(_make_missing_unified(ticker, f"Engine error: {exc}"), ticker)


@router.post("/quotes", response_model=BatchEnrichedResponse, response_model_exclude_none=True)
async def get_enriched_quotes_route(body: BatchEnrichedRequest) -> BatchEnrichedResponse:
    tickers = _clean_tickers(body.tickers)

    if not tickers:
        raise HTTPException(status_code=400, detail="At least one symbol is required")

    if len(tickers) > DEFAULT_MAX_TICKERS:
        raise HTTPException(
            status_code=400,
            detail=f"Too many symbols ({len(tickers)}). Max allowed is {DEFAULT_MAX_TICKERS}.",
        )

    unified_map = await _get_unified_quotes_chunked(tickers)
    results: List[EnrichedQuote] = []

    for s in tickers:
        uq = unified_map.get(s) or _make_missing_unified(s, "No data returned from engine")
        results.append(_to_enriched(uq, s))

    return BatchEnrichedResponse(results=results)


@router.post("/sheet-rows", response_model=SheetEnrichedResponse)
async def get_enriched_sheet_rows(body: BatchEnrichedRequest) -> SheetEnrichedResponse:
    tickers = _clean_tickers(body.tickers)

    # For Sheets workflows, allow empty tickers and still return headers
    if len(tickers) > DEFAULT_MAX_TICKERS:
        raise HTTPException(
            status_code=400,
            detail=f"Too many symbols ({len(tickers)}). Max allowed is {DEFAULT_MAX_TICKERS}.",
        )

    headers, rows = await _build_sheet_rows(tickers, body.sheet_name)
    return SheetEnrichedResponse(headers=headers, rows=rows)


__all__ = ["router"]
