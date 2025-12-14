"""
routes/legacy_service.py
------------------------------------------------------------
LEGACY SERVICE BRIDGE – v3.7.0 (Aligned with Engine v2)

- Stable legacy API on top of Engine v2.
- No direct provider calls.
- Score enrichment included so legacy gets AI scores.
- Concurrency-safe map building (no shared dict races).
"""

from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timezone
from functools import lru_cache
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, Body, Query
from pydantic import BaseModel

from env import settings
from core.data_engine_v2 import DataEngine, UnifiedQuote
from core.enriched_quote import EnrichedQuote
from core.scoring_engine import enrich_with_scores

logger = logging.getLogger("routes.legacy_service")
LEGACY_VERSION = "3.7.0"

BATCH_SIZE = int(settings.enriched_batch_size)
BATCH_TIMEOUT = float(settings.enriched_batch_timeout_sec)
MAX_CONCURRENCY = int(settings.enriched_batch_concurrency)

# ----------------------------------------------------------------------
# Models
# ----------------------------------------------------------------------
class LegacyQuoteModel(BaseModel):
    symbol: str
    ticker: str
    name: Optional[str] = None
    exchange: Optional[str] = None
    market: Optional[str] = None
    currency: Optional[str] = None
    sector: Optional[str] = None
    industry: Optional[str] = None

    price: Optional[float] = None
    previous_close: Optional[float] = None
    open: Optional[float] = None
    high: Optional[float] = None
    low: Optional[float] = None
    change: Optional[float] = None
    change_percent: Optional[float] = None

    volume: Optional[float] = None
    avg_volume: Optional[float] = None
    market_cap: Optional[float] = None
    shares_outstanding: Optional[float] = None

    fifty_two_week_high: Optional[float] = None
    fifty_two_week_low: Optional[float] = None
    fifty_two_week_position_pct: Optional[float] = None

    pe_ratio: Optional[float] = None
    pb_ratio: Optional[float] = None
    dividend_yield: Optional[float] = None
    eps: Optional[float] = None
    roe: Optional[float] = None
    roa: Optional[float] = None
    profit_margin: Optional[float] = None
    debt_to_equity: Optional[float] = None

    value_score: Optional[float] = None
    quality_score: Optional[float] = None
    momentum_score: Optional[float] = None
    opportunity_score: Optional[float] = None
    recommendation: Optional[str] = None

    data_quality: str = "MISSING"
    data_source: Optional[str] = None
    last_updated_utc: Optional[str] = None
    error: Optional[str] = None

class LegacyResponse(BaseModel):
    quotes: List[LegacyQuoteModel]
    meta: Dict[str, Any]

# ----------------------------------------------------------------------
# Engine singleton
# ----------------------------------------------------------------------
@lru_cache(maxsize=1)
def _get_engine() -> DataEngine:
    return DataEngine()

# ----------------------------------------------------------------------
# Helpers
# ----------------------------------------------------------------------
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

def _chunk(items: List[str], size: int) -> List[List[str]]:
    if size <= 0:
        return [items]
    return [items[i : i + size] for i in range(0, len(items), size)]

def _map_unified_to_legacy(uq: UnifiedQuote) -> LegacyQuoteModel:
    eq = EnrichedQuote.from_unified(uq)
    eq = enrich_with_scores(eq)

    return LegacyQuoteModel(
        symbol=eq.symbol,
        ticker=eq.symbol,
        name=getattr(eq, "name", None),
        exchange=getattr(eq, "exchange", None),
        market=getattr(eq, "market", None) or getattr(eq, "market_region", None),
        currency=getattr(eq, "currency", None),
        sector=getattr(eq, "sector", None),
        industry=getattr(eq, "industry", None),

        price=getattr(eq, "current_price", None) or getattr(eq, "last_price", None),
        previous_close=getattr(eq, "previous_close", None),
        open=getattr(eq, "open", None),
        high=getattr(eq, "day_high", None) or getattr(eq, "high", None),
        low=getattr(eq, "day_low", None) or getattr(eq, "low", None),
        change=getattr(eq, "price_change", None) or getattr(eq, "change", None),
        change_percent=getattr(eq, "percent_change", None) or getattr(eq, "change_percent", None),

        volume=getattr(eq, "volume", None),
        avg_volume=getattr(eq, "avg_volume_30d", None),
        market_cap=getattr(eq, "market_cap", None),
        shares_outstanding=getattr(eq, "shares_outstanding", None),

        fifty_two_week_high=getattr(eq, "high_52w", None),
        fifty_two_week_low=getattr(eq, "low_52w", None),
        fifty_two_week_position_pct=getattr(eq, "position_52w_percent", None),

        pe_ratio=getattr(eq, "pe_ttm", None),
        pb_ratio=getattr(eq, "pb", None),
        dividend_yield=getattr(eq, "dividend_yield", None),
        eps=getattr(eq, "eps_ttm", None),
        roe=getattr(eq, "roe", None),
        roa=getattr(eq, "roa", None),
        profit_margin=getattr(eq, "net_margin", None),
        debt_to_equity=getattr(eq, "debt_to_equity", None),

        value_score=getattr(eq, "value_score", None),
        quality_score=getattr(eq, "quality_score", None),
        momentum_score=getattr(eq, "momentum_score", None),
        opportunity_score=getattr(eq, "opportunity_score", None),
        recommendation=getattr(eq, "recommendation", None),

        data_quality=getattr(eq, "data_quality", "MISSING"),
        data_source=getattr(eq, "data_source", None),
        last_updated_utc=getattr(eq, "last_updated_utc", None),
        error=getattr(eq, "error", None),
    )

async def _fetch_legacy_quotes(tickers: List[str]) -> List[LegacyQuoteModel]:
    clean = [_normalize_symbol(t) for t in (tickers or []) if (t or "").strip()]
    clean = list(dict.fromkeys([c for c in clean if c]))  # dedupe preserve order

    if not clean:
        return []

    engine = _get_engine()
    chunks = _chunk(clean, BATCH_SIZE)
    sem = asyncio.Semaphore(max(1, MAX_CONCURRENCY))

    async def _process_chunk(chunk_syms: List[str]) -> Dict[str, LegacyQuoteModel]:
        async with sem:
            out: Dict[str, LegacyQuoteModel] = {}
            try:
                uqs = await asyncio.wait_for(engine.get_enriched_quotes(chunk_syms), timeout=BATCH_TIMEOUT)
                uq_map = {q.symbol.upper(): q for q in (uqs or [])}
                for s in chunk_syms:
                    uq = uq_map.get(s.upper()) or UnifiedQuote(symbol=s, data_quality="MISSING", error="No data returned").finalize()
                    out[s.upper()] = _map_unified_to_legacy(uq)
            except Exception as e:
                msg = "Engine batch timeout" if isinstance(e, asyncio.TimeoutError) else str(e)
                logger.error("Legacy batch error: %s", msg)
                for s in chunk_syms:
                    out[s.upper()] = LegacyQuoteModel(symbol=s, ticker=s, data_quality="MISSING", error=msg)
            return out

    dicts = await asyncio.gather(*[_process_chunk(c) for c in chunks])
    merged: Dict[str, LegacyQuoteModel] = {}
    for d in dicts:
        merged.update(d)

    final_list: List[LegacyQuoteModel] = []
    for s in clean:
        final_list.append(merged.get(s.upper()) or LegacyQuoteModel(symbol=s, ticker=s, data_quality="MISSING", error="No data returned"))
    return final_list

# ----------------------------------------------------------------------
# Router
# ----------------------------------------------------------------------
router = APIRouter(tags=["Legacy Service"])

@router.get("/v1/legacy/health")
async def legacy_health():
    return {"status": "ok", "module": "routes.legacy_service", "version": LEGACY_VERSION, "engine": "DataEngineV2"}

@router.get("/v1/quote", response_model=LegacyResponse)
async def get_legacy_quote(symbol: str = Query(..., alias="symbol")):
    quotes = await _fetch_legacy_quotes([symbol])
    return LegacyResponse(quotes=quotes, meta={"count": len(quotes), "timestamp": datetime.now(timezone.utc).isoformat(), "note": "Powered by Tadawul Fast Bridge V2"})

@router.post("/v1/quotes", response_model=LegacyResponse)
async def get_legacy_quotes_batch(symbols: List[str] = Body(..., alias="tickers", embed=True)):
    quotes = await _fetch_legacy_quotes(symbols)
    return LegacyResponse(quotes=quotes, meta={"count": len(quotes), "timestamp": datetime.now(timezone.utc).isoformat(), "note": "Powered by Tadawul Fast Bridge V2"})

get_legacy_quotes = _fetch_legacy_quotes
__all__ = ["router", "get_legacy_quotes"]
