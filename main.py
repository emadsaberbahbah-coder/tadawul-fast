"""
routes/legacy_service.py
------------------------------------------------------------
LEGACY SERVICE BRIDGE – v3.6.0 (Aligned with Engine v2)

GOAL
- Provide a stable "legacy" API layer on top of the unified data engine.
- Ensures old clients (PowerShell, early Google Sheets scripts) receiving
  /v1/quote or /v1/quotes payloads continue to work without changes.
- Maps new `UnifiedQuote` fields -> Old `LegacyQuote` schema.

KEY PRINCIPLES
- NO direct provider calls. Uses `core.data_engine_v2`.
- Enriches data using `core.scoring_engine` so legacy clients get AI scores.
- KSA (.SR) tickers handled natively by Engine v2.

Author: Emad Bahbah (with GPT-5.2 Thinking)
"""

from __future__ import annotations

import asyncio
import logging
import os
from datetime import datetime, timezone
from functools import lru_cache
from typing import Any, Dict, List, Optional, Union

from fastapi import APIRouter, HTTPException, Query, Body
from pydantic import BaseModel, Field

# --- CORE IMPORTS ---
from env import settings
from core.data_engine_v2 import DataEngine, UnifiedQuote
from core.enriched_quote import EnrichedQuote
from core.scoring_engine import enrich_with_scores

logger = logging.getLogger("routes.legacy_service")

LEGACY_VERSION = "3.6.0"

# ----------------------------------------------------------------------
# Configuration
# ----------------------------------------------------------------------
BATCH_SIZE = settings.enriched_batch_size
BATCH_TIMEOUT = settings.enriched_batch_timeout_sec
MAX_CONCURRENCY = settings.enriched_batch_concurrency

# ----------------------------------------------------------------------
# Legacy Data Models (The Schema expected by old clients)
# ----------------------------------------------------------------------

class LegacyQuoteModel(BaseModel):
    # Identity
    symbol: str
    ticker: str
    name: Optional[str] = None
    exchange: Optional[str] = None
    market: Optional[str] = None
    currency: Optional[str] = None
    sector: Optional[str] = None
    industry: Optional[str] = None
    
    # Price
    price: Optional[float] = None
    previous_close: Optional[float] = None
    open: Optional[float] = None
    high: Optional[float] = None
    low: Optional[float] = None
    change: Optional[float] = None
    change_percent: Optional[float] = None
    
    # Liquidity
    volume: Optional[float] = None
    avg_volume: Optional[float] = None
    market_cap: Optional[float] = None
    shares_outstanding: Optional[float] = None
    
    # 52 Week
    fifty_two_week_high: Optional[float] = None
    fifty_two_week_low: Optional[float] = None
    fifty_two_week_position_pct: Optional[float] = None
    
    # Fundamentals
    pe_ratio: Optional[float] = None
    pb_ratio: Optional[float] = None
    dividend_yield: Optional[float] = None
    eps: Optional[float] = None
    roe: Optional[float] = None
    roa: Optional[float] = None
    profit_margin: Optional[float] = None
    debt_to_equity: Optional[float] = None
    
    # Scores (New additions exposed to legacy if they support extra fields)
    value_score: Optional[float] = None
    quality_score: Optional[float] = None
    momentum_score: Optional[float] = None
    opportunity_score: Optional[float] = None
    recommendation: Optional[str] = None
    
    # Meta
    data_quality: str = "MISSING"
    data_source: Optional[str] = None
    last_updated_utc: Optional[str] = None
    error: Optional[str] = None

class LegacyResponse(BaseModel):
    quotes: List[LegacyQuoteModel]
    meta: Dict[str, Any]

# ----------------------------------------------------------------------
# Engine Singleton
# ----------------------------------------------------------------------

@lru_cache(maxsize=1)
def _get_engine() -> DataEngine:
    return DataEngine()

# ----------------------------------------------------------------------
# Helpers
# ----------------------------------------------------------------------

def _normalize_symbol(symbol: str) -> str:
    s = (symbol or "").strip().upper()
    if not s: return ""
    if s.startswith("TADAWUL:"): s = s.split(":", 1)[1].strip()
    if s.endswith(".TADAWUL"): s = s.replace(".TADAWUL", ".SR")
    if s.isdigit(): s = f"{s}.SR"
    return s

def _chunk(items: List[str], size: int) -> List[List[str]]:
    if size <= 0: return [items]
    return [items[i : i + size] for i in range(0, len(items), size)]

def _map_unified_to_legacy(uq: UnifiedQuote) -> LegacyQuoteModel:
    """
    Maps V2 UnifiedQuote -> EnrichedQuote (with Scores) -> LegacyQuoteModel.
    """
    # 1. Enrich (gets aliases and calculates scores)
    eq = EnrichedQuote.from_unified(uq)
    eq = enrich_with_scores(eq)

    # 2. Map to Legacy Schema
    # Note: EnrichedQuote normalizes fields like `pe_ttm` vs `pe_ratio` via aliases, 
    # but here we must be explicit for the output model.
    
    return LegacyQuoteModel(
        symbol=eq.symbol,
        ticker=eq.symbol,
        name=eq.name,
        exchange=eq.exchange,
        market=eq.market or eq.market_region,
        currency=eq.currency,
        sector=eq.sector,
        industry=eq.industry,
        
        price=eq.current_price or eq.last_price,
        previous_close=eq.previous_close,
        open=None, # Open often missing in snapshot calls, safe to skip
        high=eq.day_high or eq.high,
        low=eq.day_low or eq.low,
        change=eq.price_change or eq.change,
        change_percent=eq.percent_change or eq.change_percent,
        
        volume=eq.volume,
        avg_volume=eq.avg_volume_30d,
        market_cap=eq.market_cap,
        shares_outstanding=eq.shares_outstanding,
        
        fifty_two_week_high=eq.high_52w,
        fifty_two_week_low=eq.low_52w,
        fifty_two_week_position_pct=eq.position_52w_percent,
        
        pe_ratio=eq.pe_ttm,
        pb_ratio=eq.pb,
        dividend_yield=eq.dividend_yield,
        eps=eq.eps_ttm,
        roe=eq.roe,
        roa=eq.roa,
        profit_margin=eq.net_margin,
        debt_to_equity=eq.debt_to_equity,
        
        value_score=eq.value_score,
        quality_score=eq.quality_score,
        momentum_score=eq.momentum_score,
        opportunity_score=eq.opportunity_score,
        recommendation=eq.recommendation,
        
        data_quality=eq.data_quality,
        data_source=eq.data_source,
        last_updated_utc=eq.last_updated_utc,
        error=eq.error
    )

async def _fetch_legacy_quotes(tickers: List[str]) -> List[LegacyQuoteModel]:
    clean = [_normalize_symbol(t) for t in tickers if t.strip()]
    clean = list(dict.fromkeys(clean)) # dedupe preserve order
    
    if not clean:
        return []

    engine = _get_engine()
    chunks = _chunk(clean, BATCH_SIZE)
    sem = asyncio.Semaphore(MAX_CONCURRENCY)
    
    results_map: Dict[str, LegacyQuoteModel] = {}

    async def process_chunk(chunk_syms: List[str]):
        async with sem:
            try:
                # Fetch Unified Quotes
                uqs = await asyncio.wait_for(
                    engine.get_enriched_quotes(chunk_syms), 
                    timeout=BATCH_TIMEOUT
                )
                
                # Map to Legacy
                for uq in uqs:
                    lq = _map_unified_to_legacy(uq)
                    results_map[lq.symbol.upper()] = lq
                    
            except Exception as e:
                logger.error(f"Legacy batch error: {e}")
                # Fill missing
                for s in chunk_syms:
                    results_map[s.upper()] = LegacyQuoteModel(
                        symbol=s, ticker=s, data_quality="MISSING", error=str(e)
                    )

    await asyncio.gather(*[process_chunk(c) for c in chunks])
    
    # Return in original order
    final_list = []
    for s in clean:
        s_norm = s.upper()
        if s_norm in results_map:
            final_list.append(results_map[s_norm])
        else:
            final_list.append(LegacyQuoteModel(
                symbol=s, ticker=s, data_quality="MISSING", error="No data returned"
            ))
            
    return final_list

# ----------------------------------------------------------------------
# Router
# ----------------------------------------------------------------------

router = APIRouter(tags=["Legacy Service"])

@router.get("/v1/legacy/health")
async def legacy_health():
    return {
        "status": "ok",
        "module": "routes.legacy_service",
        "version": LEGACY_VERSION,
        "engine": "DataEngineV2"
    }

@router.get("/v1/quote", response_model=LegacyResponse)
async def get_legacy_quote(symbol: str = Query(..., alias="symbol")):
    """Legacy single quote endpoint."""
    quotes = await _fetch_legacy_quotes([symbol])
    return LegacyResponse(
        quotes=quotes,
        meta={
            "count": 1,
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "note": "Powered by Tadawul Fast Bridge V2"
        }
    )

@router.post("/v1/quotes", response_model=LegacyResponse)
async def get_legacy_quotes_batch(
    symbols: List[str] = Body(..., alias="tickers", embed=True) 
    # Accepts JSON: { "tickers": ["AAPL", "1120.SR"] }
):
    """Legacy batch quotes endpoint."""
    quotes = await _fetch_legacy_quotes(symbols)
    return LegacyResponse(
        quotes=quotes,
        meta={
            "count": len(quotes),
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "note": "Powered by Tadawul Fast Bridge V2"
        }
    )

# ----------------------------------------------------------------------
# Public Export (for main.py or direct usage)
# ----------------------------------------------------------------------
# If used as a module import rather than a router
get_legacy_quotes = _fetch_legacy_quotes 

__all__ = ["router", "get_legacy_quotes"]
