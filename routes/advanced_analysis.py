# routes/advanced_analysis.py
"""
routes/advanced_analysis.py
================================================
TADAWUL FAST BRIDGE – ADVANCED ANALYSIS ROUTES (v3.1.0)

Design goals
- 100% engine-driven (core.data_engine_v2.DataEngine). No direct provider calls.
- Uses core.data_engine_v2.normalize_symbol as the single normalization source.
- Google Sheets–friendly:
    • /sheet-rows never raises for normal usage (always returns headers + rows).
- Defensive batching:
    • chunking + timeout + bounded concurrency + placeholders on failures.
- Engine resolution:
    • prefer request.app.state.engine (created by main.py lifespan), else singleton.

Endpoints
- GET  /v1/advanced/health   (alias: /ping)
- GET  /v1/advanced/scoreboard?tickers=AAPL,MSFT,1120.SR&top_n=50
- POST /v1/advanced/sheet-rows
"""

from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timezone
from functools import lru_cache
from typing import Any, Dict, List, Optional, Sequence, Tuple

from fastapi import APIRouter, Depends, Query, Request

# Pydantic compatibility (v2 preferred)
try:
    from pydantic import BaseModel, Field, ConfigDict  # type: ignore
    _PYDANTIC_V2 = True
except Exception:  # pragma: no cover
    from pydantic import BaseModel, Field  # type: ignore
    ConfigDict = None  # type: ignore
    _PYDANTIC_V2 = False

# Prefer env.settings (project standard). Fallback to core.config.get_settings if needed.
try:
    from env import settings  # type: ignore
except Exception:  # pragma: no cover
    try:
        from core.config import get_settings  # type: ignore
        settings = get_settings()  # type: ignore
    except Exception:
        settings = None  # type: ignore

from core.data_engine_v2 import DataEngine, UnifiedQuote, normalize_symbol
from core.enriched_quote import EnrichedQuote
from core.scoring_engine import enrich_with_scores

logger = logging.getLogger("routes.advanced_analysis")

ADVANCED_ANALYSIS_VERSION = "3.1.0"


# =============================================================================
# Optional schema helper (for sheet-specific headers)
# =============================================================================
try:
    from core.schemas import get_headers_for_sheet  # type: ignore
except Exception:  # pragma: no cover
    get_headers_for_sheet = None  # type: ignore


# =============================================================================
# Safe config helpers
# =============================================================================
def _safe_int(x: Any, default: int) -> int:
    try:
        return int(x)
    except Exception:
        return default


def _safe_float(x: Any, default: float) -> float:
    try:
        return float(x)
    except Exception:
        return default


DEFAULT_BATCH_SIZE = _safe_int(getattr(settings, "adv_batch_size", 25), 25)
DEFAULT_BATCH_TIMEOUT = _safe_float(getattr(settings, "adv_batch_timeout_sec", 45.0), 45.0)
DEFAULT_MAX_TICKERS = _safe_int(getattr(settings, "adv_max_tickers", 500), 500)
DEFAULT_CONCURRENCY = _safe_int(getattr(settings, "adv_batch_concurrency", 6), 6)


# =============================================================================
# Engine resolution
# =============================================================================
@lru_cache(maxsize=1)
def _engine_singleton() -> Optional[DataEngine]:
    try:
        logger.info("routes.advanced_analysis: initializing local DataEngine singleton (fallback)")
        return DataEngine()
    except Exception as exc:
        logger.exception("routes.advanced_analysis: failed to init DataEngine: %s", exc)
        return None


def get_engine(request: Request) -> Optional[DataEngine]:
    """
    Prefer the engine created in main.py lifespan (request.app.state.engine).
    Fall back to a local singleton if not present.
    """
    eng = getattr(getattr(request, "app", None), "state", None)
    eng = getattr(eng, "engine", None)
    if isinstance(eng, DataEngine):
        return eng
    return _engine_singleton()


# =============================================================================
# Utilities
# =============================================================================
def _now_utc_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _clean_tickers(items: Sequence[str]) -> List[str]:
    raw = [normalize_symbol(x) for x in (items or [])]
    raw = [x for x in raw if x]
    seen = set()
    out: List[str] = []
    for x in raw:
        xu = x.upper()
        if xu in seen:
            continue
        seen.add(xu)
        out.append(xu)
    return out


def _parse_tickers_csv(s: str) -> List[str]:
    if not s:
        return []
    parts = [p.strip() for p in s.split(",") if p.strip()]
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
    """
    Bucket logic is intentionally simple and stable for Sheets.
    """
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
    engine: Optional[DataEngine],
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

    if engine is None:
        msg = "Engine unavailable"
        return {s: UnifiedQuote(symbol=s, data_quality="MISSING", error=msg).finalize() for s in clean}

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
                out[s.upper()] = UnifiedQuote(symbol=s.upper(), data_quality="MISSING", error=msg).finalize()
            continue

        returned = list(res or [])
        chunk_map = {q.symbol.upper(): q for q in returned if q and getattr(q, "symbol", None)}

        for s in chunk_syms:
            k = s.upper()
            out[k] = chunk_map.get(k) or UnifiedQuote(symbol=k, data_quality="MISSING", error="No data returned").finalize()

    return out


# =============================================================================
# Pydantic response models (extra ignore for Sheets safety)
# =============================================================================
class _ExtraIgnore(BaseModel):
    if _PYDANTIC_V2:
        model_config = ConfigDict(extra="ignore")  # type: ignore
    else:  # pragma: no cover
        class Config:
            extra = "ignore"


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
    sheet_name: Optional[str] = None  # optional (for header selection)


class AdvancedSheetResponse(_ExtraIgnore):
    headers: List[str]
    rows: List[List[Any]]


# =============================================================================
# Transform logic
# =============================================================================
def _to_advanced_item(raw_symbol: str, uq: UnifiedQuote) -> AdvancedItem:
    eq = EnrichedQuote.from_unified(uq)
    eq = enrich_with_scores(eq)

    symbol = (getattr(eq, "symbol", None) or normalize_symbol(raw_symbol) or raw_symbol or "").strip().upper()

    dq = getattr(eq, "data_quality", None)
    dq_score = _map_data_quality_to_score(dq)

    as_of_utc = getattr(eq, "last_updated_utc", None)
    age_min = _data_age_minutes(as_of_utc)

    opp = getattr(eq, "opportunity_score", None)
    risk_bucket = _compute_risk_bucket(opp, dq_score)

    return AdvancedItem(
        symbol=symbol,
        name=getattr(eq, "name", None),
        market=getattr(eq, "market", None) or getattr(eq, "market_region", None),
        sector=getattr(eq, "sector", None),
        currency=getattr(eq, "currency", None),

        last_price=getattr(eq, "current_price", None) or getattr(eq, "last_price", None),
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
    return (opp * 1_000_000.0) + (conf * 1_000.0) + (up * 10.0) + ov


def _default_sheet_headers() -> List[str]:
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


def _select_sheet_headers(sheet_name: Optional[str]) -> List[str]:
    if sheet_name and get_headers_for_sheet:
        try:
            h = get_headers_for_sheet(sheet_name)
            if isinstance(h, list) and h and any(str(x).strip().lower() == "symbol" for x in h):
                return [str(x) for x in h]
        except Exception:
            pass
    return _default_sheet_headers()


def _item_value_map(it: AdvancedItem) -> Dict[str, Any]:
    return {
        "Symbol": it.symbol,
        "Company Name": it.name or "",
        "Market": it.market or "",
        "Sector": it.sector or "",
        "Currency": it.currency or "",

        "Last Price": it.last_price,
        "Fair Value": it.fair_value,
        "Upside %": it.upside_percent,

        "Opportunity Score": it.opportunity_score,
        "Value Score": it.value_score,
        "Quality Score": it.quality_score,
        "Momentum Score": it.momentum_score,
        "Overall Score": it.overall_score,

        "Data Quality": it.data_quality or "",
        "Data Quality Score": it.data_quality_score,

        "Recommendation": it.recommendation or "",
        "Valuation Label": it.valuation_label or "",
        "Risk Bucket": it.risk_bucket or "",

        "Provider": it.provider or "",
        "As Of (UTC)": it.as_of_utc or "",
        "Data Age (Minutes)": it.data_age_minutes,

        "Error": it.error or "",
    }


def _row_for_headers(it: AdvancedItem, headers: List[str]) -> List[Any]:
    m = _item_value_map(it)
    row = [m.get(h, None) for h in headers]
    if len(row) < len(headers):
        row += [None] * (len(headers) - len(row))
    return row[: len(headers)]


# =============================================================================
# Router
# =============================================================================
router = APIRouter(prefix="/v1/advanced", tags=["Advanced Analysis"])


@router.get("/health")
@router.get("/ping")
async def advanced_health(request: Request) -> Dict[str, Any]:
    eng = get_engine(request)
    return {
        "status": "ok",
        "module": "routes.advanced_analysis",
        "version": ADVANCED_ANALYSIS_VERSION,
        "engine_mode": "v2",
        "batch_size": DEFAULT_BATCH_SIZE,
        "batch_timeout_sec": DEFAULT_BATCH_TIMEOUT,
        "batch_concurrency": DEFAULT_CONCURRENCY,
        "max_tickers": DEFAULT_MAX_TICKERS,
        "providers": getattr(eng, "enabled_providers", None),
        "timestamp_utc": _now_utc_iso(),
    }


@router.get("/scoreboard", response_model=AdvancedScoreboardResponse)
async def advanced_scoreboard(
    request: Request,
    tickers: str = Query(..., description="Comma-separated tickers e.g. 'AAPL,MSFT,1120.SR'"),
    top_n: int = Query(50, ge=1, le=500, description="Max rows returned after sorting"),
    engine: Optional[DataEngine] = Depends(get_engine),
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
            # best-effort single fetch (should be rare)
            try:
                if engine is None:
                    raise RuntimeError("Engine unavailable")
                uq = await engine.get_quote(s)
            except Exception as e:
                uq = UnifiedQuote(symbol=s.upper(), data_quality="MISSING", error=str(e)).finalize()
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
    engine: Optional[DataEngine] = Depends(get_engine),
) -> AdvancedSheetResponse:
    """
    Dedicated endpoint for Google Sheets "Advanced Analysis" tab.
    Returns headers + rows sorted by opportunity.
    Sheets-safe: best-effort, never raises for normal usage.
    """
    headers = _select_sheet_headers(body.sheet_name)
    requested = _clean_tickers(body.tickers or [])

    if not requested:
        return AdvancedSheetResponse(headers=headers, rows=[])

    if len(requested) > DEFAULT_MAX_TICKERS:
        requested = requested[:DEFAULT_MAX_TICKERS]

    top_n = _safe_int(body.top_n or 50, 50)
    top_n = max(1, min(500, top_n))

    try:
        unified_map = await _get_quotes_chunked(engine, requested)
        items: List[AdvancedItem] = []
        for s in requested:
            uq = unified_map.get(s.upper())
            if uq is None:
                try:
                    if engine is None:
                        raise RuntimeError("Engine unavailable")
                    uq = await engine.get_quote(s)
                except Exception as e:
                    uq = UnifiedQuote(symbol=s.upper(), data_quality="MISSING", error=str(e)).finalize()
            items.append(_to_advanced_item(s, uq))

        items_sorted = sorted(items, key=_sort_key, reverse=True)
        if len(items_sorted) > top_n:
            items_sorted = items_sorted[:top_n]

        rows = [_row_for_headers(it, headers) for it in items_sorted]
        return AdvancedSheetResponse(headers=headers, rows=rows)

    except Exception as exc:
        logger.exception("advanced_analysis: exception in /sheet-rows: %s", exc)
        # Return placeholders so Sheets never breaks
        rows: List[List[Any]] = []
        for s in requested[:top_n]:
            it = AdvancedItem(
                symbol=s.upper(),
                data_quality="MISSING",
                data_quality_score=0.0,
                risk_bucket="LOW_CONFIDENCE",
                provider="none",
                as_of_utc=_now_utc_iso(),
                error=str(exc),
            )
            rows.append(_row_for_headers(it, headers))
        return AdvancedSheetResponse(headers=headers, rows=rows)


__all__ = [
    "router",
    "AdvancedItem",
    "AdvancedScoreboardResponse",
    "AdvancedSheetRequest",
    "AdvancedSheetResponse",
]
