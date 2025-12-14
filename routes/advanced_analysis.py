"""
routes/advanced_analysis.py
================================================
TADAWUL FAST BRIDGE – ADVANCED ANALYSIS ROUTES (v3.0.0)

Purpose
-------
- Expose "advanced analysis" + "opportunity ranking" endpoints on top of the unified engine.
- Google Sheets–friendly: provides /sheet-rows (headers + rows).

Endpoints
---------
- GET  /v1/advanced/health         (alias: /ping)
- GET  /v1/advanced/scoreboard?tickers=AAPL,MSFT,1120.SR&top_n=50
- POST /v1/advanced/sheet-rows     (Sheets-friendly scoreboard)

Key fixes (IMPORTANT)
---------------------
- Uses engine.get_quote / engine.get_quotes (these exist in core.data_engine_v2).
- Removes calls to non-existent engine methods (get_enriched_quotes, finalize, etc.).
- Uses FastAPI app.state.engine when available (created by main.py lifespan),
  otherwise uses a safe singleton fallback.
- Extremely defensive: chunking + concurrency + timeout + placeholders.
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

logger = logging.getLogger("routes.advanced_analysis")
settings = get_settings()

ADVANCED_ANALYSIS_VERSION = "3.0.0"

# =============================================================================
# Runtime config (env overrides)
# =============================================================================
DEFAULT_BATCH_SIZE = int(os.getenv("ADV_BATCH_SIZE", "25"))
DEFAULT_BATCH_TIMEOUT = float(os.getenv("ADV_BATCH_TIMEOUT_SEC", "45"))
DEFAULT_MAX_TICKERS = int(os.getenv("ADV_MAX_TICKERS", "500"))
DEFAULT_CONCURRENCY = int(os.getenv("ADV_BATCH_CONCURRENCY", "6"))

# =============================================================================
# Engine resolution
# =============================================================================
@lru_cache(maxsize=1)
def _engine_singleton() -> DataEngine:
    logger.info("routes.advanced_analysis: initializing local DataEngine singleton (fallback)")
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

# =============================================================================
# Utilities
# =============================================================================
def _now_utc_iso() -> str:
    return datetime.now(timezone.utc).isoformat()

def _normalize_symbol(symbol: str) -> str:
    """
    Normalization aligned with dashboard expectations:
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

def _map_data_quality_to_score(label: Optional[str]) -> float:
    dq = (label or "").strip().upper()
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
    return float(mapping.get(dq, 30.0))

def _compute_risk_bucket(opportunity: Optional[float], dq_score: float) -> str:
    opp = float(opportunity or 0.0)
    conf = float(dq_score or 0.0)

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
# Core Async Fetch Logic (Chunked + bounded concurrency)
# =============================================================================
async def _get_quotes_chunked(
    engine: DataEngine,
    symbols: List[str],
    *,
    batch_size: int = DEFAULT_BATCH_SIZE,
    timeout_sec: float = DEFAULT_BATCH_TIMEOUT,
    max_concurrency: int = DEFAULT_CONCURRENCY,
) -> Dict[str, UnifiedQuote]:
    """
    Returns map keyed by UPPERCASE symbol.
    Defensive: never raises; failures become placeholder UnifiedQuote items.
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
            logger.warning("advanced_analysis: %s for chunk(size=%d): %s", msg, len(chunk_syms), chunk_syms)
            for s in chunk_syms:
                uq = UnifiedQuote(symbol=str(s).upper(), data_quality="MISSING", error=msg)
                out[uq.symbol.upper()] = uq
            continue

        returned = list(res or [])
        for q in returned:
            if q and q.symbol:
                out[q.symbol.upper()] = q

        # Best-effort placeholders for requested tickers not present as exact keys
        for s in chunk_syms:
            k = str(s).upper()
            if k not in out:
                out[k] = UnifiedQuote(symbol=k, data_quality="MISSING", error="No data returned")

    return out

# =============================================================================
# Pydantic response models (extra ignore for Sheets safety)
# =============================================================================
class _ExtraIgnore(BaseModel):
    model_config = ConfigDict(extra="ignore")

class AdvancedItem(_ExtraIgnore):
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
    valuation_label: Optional[str] = None
    risk_bucket: Optional[str] = None

    provider: Optional[str] = None
    as_of_utc: Optional[str] = None
    data_age_minutes: Optional[float] = None
    error: Optional[str] = None

class AdvancedScoreboardResponse(_ExtraIgnore):
    generated_at_utc: str
    version: str
    engine_mode: str = "v2"

    total_requested: int
    total_returned: int
    top_n_applied: bool

    tickers: List[str]
    items: List[AdvancedItem]

class AdvancedSheetRequest(_ExtraIgnore):
    tickers: List[str] = Field(default_factory=list)
    top_n: Optional[int] = Field(default=50, ge=1, le=500)

class AdvancedSheetResponse(_ExtraIgnore):
    headers: List[str]
    rows: List[List[Any]]

# =============================================================================
# Transform logic
# =============================================================================
def _to_advanced_item(raw_symbol: str, uq: UnifiedQuote) -> AdvancedItem:
    # Convert UnifiedQuote -> EnrichedQuote -> scored
    eq = EnrichedQuote.from_unified(uq)
    eq = enrich_with_scores(eq)

    symbol = (getattr(eq, "symbol", None) or raw_symbol or "").strip().upper()

    dq = getattr(eq, "data_quality", None)
    dq_score = _map_data_quality_to_score(dq)

    as_of_utc = getattr(eq, "last_updated_utc", None)
    age_min = _data_age_minutes(as_of_utc)

    opp = getattr(eq, "opportunity_score", None)
    risk_bucket = _compute_risk_bucket(opp, dq_score)

    return AdvancedItem(
        symbol=symbol,
        name=getattr(eq, "name", None),
        market=getattr(eq, "market", None),
        sector=getattr(eq, "sector", None),
        currency=getattr(eq, "currency", None),

        last_price=getattr(eq, "current_price", None),
        fair_value=getattr(eq, "fair_value", None),
        upside_percent=getattr(eq, "upside_percent", None),

        value_score=getattr(eq, "value_score", None),
        quality_score=getattr(eq, "quality_score", None),
        momentum_score=getattr(eq, "momentum_score", None),
        opportunity_score=opp,
        overall_score=getattr(eq, "overall_score", None),

        data_quality=dq,
        data_quality_score=dq_score,

        recommendation=getattr(eq, "recommendation", None),
        valuation_label=getattr(eq, "valuation_label", None),
        risk_bucket=risk_bucket,

        provider=getattr(eq, "data_source", None),
        as_of_utc=as_of_utc,
        data_age_minutes=age_min,
        error=getattr(eq, "error", None),
    )

def _sort_key(it: AdvancedItem) -> float:
    # Priority: opportunity, then confidence, then upside, then overall
    opp = float(it.opportunity_score or 0.0)
    conf = float(it.data_quality_score or 0.0)
    up = float(it.upside_percent or 0.0)
    ov = float(it.overall_score or 0.0)
    return (opp * 1_000_000) + (conf * 1_000) + (up * 10) + ov

def _sheet_headers() -> List[str]:
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
        "Valuation Label",
        "Risk Bucket",
        "Provider",
        "As Of (UTC)",
        "Data Age (Minutes)",
        "Error",
    ]

def _item_to_row(it: AdvancedItem) -> List[Any]:
    return [
        it.symbol,
        it.name or "",
        it.market or "",
        it.sector or "",
        it.currency or "",
        it.last_price,
        it.fair_value,
        it.upside_percent,
        it.opportunity_score,
        it.value_score,
        it.quality_score,
        it.momentum_score,
        it.overall_score,
        it.data_quality or "",
        it.data_quality_score,
        it.recommendation or "",
        it.valuation_label or "",
        it.risk_bucket or "",
        it.provider or "",
        it.as_of_utc or "",
        it.data_age_minutes,
        it.error or "",
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
        "providers": {
            "eodhd_enabled": bool(getattr(settings, "EODHD_API_KEY", None)),
            "fmp_enabled": bool(getattr(settings, "FMP_API_KEY", None)),
            "yfinance_enabled": bool(getattr(settings, "ENABLE_YFINANCE", False)),
            "ksa_argaam_scrape": True,
        },
        "timestamp_utc": _now_utc_iso(),
    }

@router.get("/scoreboard", response_model=AdvancedScoreboardResponse)
async def advanced_scoreboard(
    request: Request,
    tickers: str = Query(..., description="Comma-separated tickers e.g. 'AAPL,MSFT,1120.SR'"),
    top_n: int = Query(50, ge=1, le=500, description="Max rows returned after sorting"),
    engine: DataEngine = Depends(get_engine),
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

    unified_map = await _get_quotes_chunked(engine, requested)

    items: List[AdvancedItem] = []
    for s in requested:
        uq = unified_map.get(s.upper())
        if uq is None:
            # fallback single fetch (handles cases where engine normalized symbol differently)
            try:
                uq = await engine.get_quote(s)
            except Exception as e:
                uq = UnifiedQuote(symbol=s.upper(), data_quality="MISSING", error=str(e))
        items.append(_to_advanced_item(s, uq))

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
async def advanced_sheet_rows(
    request: Request,
    body: AdvancedSheetRequest,
    engine: DataEngine = Depends(get_engine),
) -> AdvancedSheetResponse:
    """
    Dedicated endpoint for Google Sheets "Advanced Analysis" tab.
    Returns headers + rows sorted by opportunity.
    Sheets-safe: never raises (best-effort).
    """
    headers = _sheet_headers()
    requested = _clean_tickers(body.tickers or [])

    if not requested:
        return AdvancedSheetResponse(headers=headers, rows=[])

    if len(requested) > DEFAULT_MAX_TICKERS:
        requested = requested[:DEFAULT_MAX_TICKERS]

    try:
        unified_map = await _get_quotes_chunked(engine, requested)
    except Exception as exc:
        logger.exception("advanced_analysis: /sheet-rows batch failure: %s", exc)
        return AdvancedSheetResponse(headers=headers, rows=[])

    items: List[AdvancedItem] = []
    for s in requested:
        uq = unified_map.get(s.upper())
        if uq is None:
            try:
                uq = await engine.get_quote(s)
            except Exception as e:
                uq = UnifiedQuote(symbol=s.upper(), data_quality="MISSING", error=str(e))
        items.append(_to_advanced_item(s, uq))

    items_sorted = sorted(items, key=_sort_key, reverse=True)

    top_n = int(body.top_n or 50)
    if len(items_sorted) > top_n:
        items_sorted = items_sorted[:top_n]

    rows = [_item_to_row(it) for it in items_sorted]
    return AdvancedSheetResponse(headers=headers, rows=rows)

__all__ = ["router", "AdvancedItem", "AdvancedScoreboardResponse", "AdvancedSheetRequest", "AdvancedSheetResponse"]
