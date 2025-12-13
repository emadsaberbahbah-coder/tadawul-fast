"""
routes/advanced_analysis.py
================================================
TADAWUL FAST BRIDGE – ADVANCED ANALYSIS ROUTES (v2.6.0)

Purpose
-------
- Expose "advanced analysis" + "opportunity ranking" endpoints
  on top of the unified engine (core.data_engine_v2 preferred).
- Google Sheets–friendly: provides /sheet-rows (headers + rows).

Endpoints
---------
- GET  /v1/advanced/health         (alias: /ping)
- GET  /v1/advanced/scoreboard?tickers=AAPL,MSFT,1120.SR&top_n=50
- POST /v1/advanced/sheet-rows     (Sheets-friendly scoreboard)

Design notes
------------
- Router NEVER calls providers directly. It only talks to the DataEngine.
- Defensive + stable for Sheets:
    • Normalizes tickers (1120 -> 1120.SR, TADAWUL:1120 -> 1120.SR)
    • Dedupes tickers (preserves order)
    • Chunked batch requests + bounded concurrency + timeout per chunk
    • Returns 200 with MISSING items instead of 502 on provider failure
- Singleton engine per process to avoid repeated initialization.
- Sorting is deterministic and favors Opportunity, then confidence, then upside.

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

# Imports from Core
from core.data_engine_v2 import DataEngine, UnifiedQuote
from core.scoring_engine import enrich_with_scores
from core.enriched_quote import EnrichedQuote

logger = logging.getLogger("routes.advanced_analysis")

ADVANCED_ANALYSIS_VERSION = "2.6.0"

# =============================================================================
# Runtime config (env overrides)
# =============================================================================

DEFAULT_BATCH_SIZE = int(os.getenv("ADV_BATCH_SIZE", "20") or 20)
DEFAULT_BATCH_TIMEOUT = float(os.getenv("ADV_BATCH_TIMEOUT_SEC", "45") or 45)
DEFAULT_MAX_TICKERS = int(os.getenv("ADV_MAX_TICKERS", "500") or 500)
DEFAULT_CONCURRENCY = int(os.getenv("ADV_BATCH_CONCURRENCY", "5") or 5)

# =============================================================================
# Engine Singleton
# =============================================================================

@lru_cache(maxsize=1)
def _get_engine_singleton() -> DataEngine:
    logger.info(f"Initializing DataEngine v2 singleton for Advanced Analysis")
    return DataEngine()

# =============================================================================
# Utilities
# =============================================================================

def _now_utc_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _normalize_symbol(symbol: str) -> str:
    """
    Align with legacy_service + enriched_quote:
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


def _parse_tickers_csv(s: str) -> List[str]:
    if not s:
        return []
    parts = [p.strip() for p in s.split(",")]
    return _clean_tickers(parts)


def _chunk(items: List[str], size: int) -> List[List[str]]:
    if size <= 0:
        return [items]
    return [items[i : i + size] for i in range(0, len(items), size)]


def _safe_float(v: Any) -> Optional[float]:
    try:
        if v is None:
            return None
        return float(v)
    except Exception:
        return None


def _map_data_quality_to_score(label: Optional[str]) -> Optional[float]:
    if not label:
        return None
    dq = str(label).strip().upper()
    mapping = {
        "FULL": 95.0,
        "EXCELLENT": 90.0,
        "GOOD": 80.0,
        "OK": 75.0,
        "FAIR": 55.0,
        "PARTIAL": 50.0,
        "STALE": 40.0,
        "POOR": 30.0,
        "MISSING": 0.0,
    }
    return mapping.get(dq, 30.0)


def _compute_risk_bucket(opportunity: Optional[float], dq: Optional[float]) -> str:
    opp = opportunity or 0.0
    conf = dq or 0.0

    if conf < 40:
        return "LOW_CONFIDENCE"

    if opp >= 75 and conf >= 70:
        return "HIGH_OPP_HIGH_CONF"
    if 55 <= opp < 75 and conf >= 60:
        return "MED_OPP_HIGH_CONF"
    if opp >= 55 and 40 <= conf < 60:
        return "OPP_WITH_MED_CONF"
    if opp < 35 and conf >= 60:
        return "LOW_OPP_HIGH_CONF"

    return "NEUTRAL"


def _data_age_minutes(as_of_utc: Any) -> Optional[float]:
    if not as_of_utc:
        return None
    try:
        if isinstance(as_of_utc, datetime):
            ts = as_of_utc
        else:
            ts = datetime.fromisoformat(str(as_of_utc))
        if ts.tzinfo is None:
            ts = ts.replace(tzinfo=timezone.utc)
        diff = datetime.now(timezone.utc) - ts
        return round(diff.total_seconds() / 60.0, 2)
    except Exception:
        return None


# =============================================================================
# Core Async Fetch Logic
# =============================================================================

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
                # Engine v2 get_enriched_quotes returns List[UnifiedQuote]
                res = await asyncio.wait_for(engine.get_enriched_quotes(chunk_syms), timeout=timeout_sec)
                return chunk_syms, res
            except Exception as e:
                return chunk_syms, e

    results = await asyncio.gather(*[_run_chunk(c) for c in chunks])

    out: Dict[str, UnifiedQuote] = {}
    
    for chunk_syms, res in results:
        # Handle chunk failure
        if isinstance(res, Exception):
            msg = "Engine batch timeout" if isinstance(res, asyncio.TimeoutError) else f"Engine batch error: {res}"
            logger.warning("AdvancedAnalysis: %s for chunk(size=%d): %s", msg, len(chunk_syms), chunk_syms)
            for s in chunk_syms:
                # Create a placeholder error quote
                out[s] = UnifiedQuote(
                    symbol=s.upper(),
                    data_quality="MISSING", 
                    error=msg
                ).finalize()
            continue

        # Handle success
        returned_quotes = list(res or [])
        
        # Create a lookup map by symbol for this chunk
        chunk_map = {q.symbol.upper(): q for q in returned_quotes}

        # Assign to output
        for s in chunk_syms:
            s_norm = s.upper()
            if s_norm in chunk_map:
                out[s] = chunk_map[s_norm]
            else:
                out[s] = UnifiedQuote(
                    symbol=s_norm, 
                    data_quality="MISSING", 
                    error="No data returned from engine"
                ).finalize()

    return out


# =============================================================================
# Pydantic models for Response
# =============================================================================

class AdvancedItem(BaseModel):
    symbol: str
    name: Optional[str] = None
    market: Optional[str] = None
    sector: Optional[str] = None
    currency: Optional[str] = None

    last_price: Optional[float] = None
    fair_value: Optional[float] = None
    upside_percent: Optional[float] = None

    value_score: Optional[float] = None
    quality_score: Optional[float] = None
    momentum_score: Optional[float] = None
    opportunity_score: Optional[float] = None
    overall_score: Optional[float] = None

    data_quality: Optional[str] = None
    data_quality_score: Optional[float] = None

    recommendation: Optional[str] = None
    risk_label: Optional[str] = None
    risk_bucket: Optional[str] = None

    provider: Optional[str] = None
    as_of_utc: Optional[str] = None
    data_age_minutes: Optional[float] = None
    error: Optional[str] = None


class AdvancedScoreboardResponse(BaseModel):
    generated_at_utc: str
    version: str
    engine_mode: str = "v2"

    total_requested: int
    total_returned: int
    top_n_applied: bool

    tickers: List[str]
    items: List[AdvancedItem]


class AdvancedSheetRequest(BaseModel):
    tickers: List[str] = Field(default_factory=list)
    top_n: Optional[int] = Field(default=50, ge=1, le=500)


class AdvancedSheetResponse(BaseModel):
    headers: List[str]
    rows: List[List[Any]]


# =============================================================================
# Transform logic
# =============================================================================

def _to_advanced_item(raw_symbol: str, q: UnifiedQuote) -> AdvancedItem:
    
    # 1. Enhance Scores using Scoring Engine if they look default/empty
    # Convert UnifiedQuote -> EnrichedQuote (compatible for scoring engine)
    eq = EnrichedQuote.from_unified(q)
    
    # If the engine didn't provide strong scores (e.g. they are exactly default 50.0 and confidence is low),
    # run the scoring engine to recalculate based on raw data.
    # Note: UnifiedQuote.finalize() sets defaults. enrich_with_scores recalculates based on data.
    # We always run enrich_with_scores to ensure consistency with the scoring_engine logic.
    scored_q = enrich_with_scores(eq)

    # 2. Extract Data
    symbol = scored_q.symbol or raw_symbol
    market = scored_q.market or scored_q.market_region
    dq = scored_q.data_quality
    dq_score = _map_data_quality_to_score(dq)

    as_of_utc = scored_q.last_updated_utc
    age_min = _data_age_minutes(as_of_utc)
    
    risk_bucket = _compute_risk_bucket(scored_q.opportunity_score, dq_score)
    
    provider = scored_q.data_source or scored_q.primary_provider

    return AdvancedItem(
        symbol=symbol,
        name=scored_q.name,
        market=market,
        sector=scored_q.sector,
        currency=scored_q.currency,
        last_price=scored_q.current_price or scored_q.last_price, # Handle aliases
        fair_value=scored_q.fair_value,
        upside_percent=scored_q.upside_percent,
        value_score=scored_q.value_score,
        quality_score=scored_q.quality_score,
        momentum_score=scored_q.momentum_score,
        opportunity_score=scored_q.opportunity_score,
        overall_score=scored_q.overall_score,
        data_quality=dq,
        data_quality_score=dq_score,
        recommendation=scored_q.recommendation,
        risk_label=scored_q.valuation_label, # Mapping valuation label to risk label for summary
        risk_bucket=risk_bucket,
        provider=provider,
        as_of_utc=as_of_utc,
        data_age_minutes=age_min,
        error=scored_q.error,
    )


def _sort_key(it: AdvancedItem) -> float:
    # Strong priority: opportunity, then confidence, then upside
    opp = it.opportunity_score or 0.0
    dq = it.data_quality_score or 0.0
    up = it.upside_percent or 0.0
    # Add small tie-breaker using value score
    val = it.value_score or 0.0
    return (opp * 1_000_000) + (dq * 1_000) + (up * 10) + val


def _sheet_headers() -> List[str]:
    # Specific headers for the Advanced Analysis sheet
    return [
        "Symbol",
        "Company Name",
        "Market",
        "Sector",
        "Currency",
        "Last Price",
        "Fair Value",
        "Upside %",
        "Opportunity Score",
        "Value Score",
        "Quality Score",
        "Momentum Score",
        "Overall Score",
        "Data Quality",
        "Data Quality Score",
        "Recommendation",
        "Risk Bucket",
        "Provider",
        "As Of (UTC)",
        "Data Age (Minutes)",
        "Error",
    ]


def _item_to_row(it: AdvancedItem) -> List[Any]:
    # Must match _sheet_headers order exactly
    return [
        it.symbol,
        it.name,
        it.market,
        it.sector,
        it.currency,
        it.last_price,
        it.fair_value,
        it.upside_percent,
        it.opportunity_score,
        it.value_score,
        it.quality_score,
        it.momentum_score,
        it.overall_score,
        it.data_quality,
        it.data_quality_score,
        it.recommendation,
        it.risk_bucket,
        it.provider,
        it.as_of_utc,
        it.data_age_minutes,
        it.error,
    ]


# =============================================================================
# Router
# =============================================================================

router = APIRouter(prefix="/v1/advanced", tags=["Advanced Analysis"])


@router.get("/health")
@router.get("/ping")
async def advanced_health() -> Dict[str, Any]:
    return {
        "status": "ok",
        "module": "routes.advanced_analysis",
        "version": ADVANCED_ANALYSIS_VERSION,
        "engine_mode": "v2",
        "batch_size": DEFAULT_BATCH_SIZE,
        "batch_timeout_sec": DEFAULT_BATCH_TIMEOUT,
        "batch_concurrency": DEFAULT_CONCURRENCY,
        "max_tickers": DEFAULT_MAX_TICKERS,
        "timestamp_utc": _now_utc_iso(),
    }


@router.get("/scoreboard", response_model=AdvancedScoreboardResponse)
async def advanced_scoreboard(
    tickers: str = Query(..., description="Comma-separated tickers e.g. 'AAPL,MSFT,1120.SR'"),
    top_n: int = Query(50, ge=1, le=500, description="Max rows returned after sorting"),
) -> AdvancedScoreboardResponse:
    requested = _parse_tickers_csv(tickers)
    
    if not requested:
        return AdvancedScoreboardResponse(
            generated_at_utc=_now_utc_iso(),
            version=ADVANCED_ANALYSIS_VERSION,
            engine_mode="v2",
            total_requested=0,
            total_returned=0,
            top_n_applied=False,
            tickers=[],
            items=[],
        )

    if len(requested) > DEFAULT_MAX_TICKERS:
        requested = requested[:DEFAULT_MAX_TICKERS]

    unified_map = await _get_quotes_chunked(requested)

    items: List[AdvancedItem] = []
    for s in requested:
        # unified_map[s] is guaranteed to exist by _get_quotes_chunked
        quote = unified_map[s]
        items.append(_to_advanced_item(s, quote))

    items_sorted = sorted(items, key=_sort_key, reverse=True)

    top_applied = False
    if len(items_sorted) > top_n:
        items_sorted = items_sorted[:top_n]
        top_applied = True

    return AdvancedScoreboardResponse(
        generated_at_utc=_now_utc_iso(),
        version=ADVANCED_ANALYSIS_VERSION,
        engine_mode="v2",
        total_requested=len(requested),
        total_returned=len(items_sorted),
        top_n_applied=top_applied,
        tickers=requested,
        items=items_sorted,
    )


@router.post("/sheet-rows", response_model=AdvancedSheetResponse)
async def advanced_sheet_rows(body: AdvancedSheetRequest) -> AdvancedSheetResponse:
    """
    Dedicated endpoint for Google Sheets "Advanced Analysis" tab.
    Returns headers + rows sorted by opportunity.
    """
    requested = _clean_tickers(body.tickers or [])
    headers = _sheet_headers()

    if not requested:
        return AdvancedSheetResponse(headers=headers, rows=[])

    if len(requested) > DEFAULT_MAX_TICKERS:
        requested = requested[:DEFAULT_MAX_TICKERS]

    unified_map = await _get_quotes_chunked(requested)

    items: List[AdvancedItem] = []
    for s in requested:
        quote = unified_map[s]
        items.append(_to_advanced_item(s, quote))

    items_sorted = sorted(items, key=_sort_key, reverse=True)
    
    top_n = body.top_n or 50
    if len(items_sorted) > top_n:
        items_sorted = items_sorted[:top_n]

    rows = [_item_to_row(it) for it in items_sorted]
    return AdvancedSheetResponse(headers=headers, rows=rows)
