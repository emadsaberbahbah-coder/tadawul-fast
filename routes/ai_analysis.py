# routes/ai_analysis.py
"""
routes/ai_analysis.py
------------------------------------------------------------
AI & QUANT ANALYSIS ROUTES – GOOGLE SHEETS FRIENDLY (v3.0.0)

Key goals
- Uses core.data_engine_v2.DataEngine only (no direct provider calls).
- Defensive batching: chunking + timeout + bounded concurrency.
- Sheets-safe: /sheet-rows always returns 200 with headers + rows.
- Symbol normalization uses core.data_engine_v2.normalize_symbol (single source).
- Header selection:
    • If body.sheet_name is provided and core.schemas.get_headers_for_sheet exists,
      it will be used (when it looks valid).
    • Otherwise falls back to a stable default analysis header set.
"""

from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timezone
from functools import lru_cache
from typing import Any, Dict, List, Optional, Sequence, Tuple

from fastapi import APIRouter, Query

# Pydantic compatibility (v2 preferred)
try:
    from pydantic import BaseModel, Field, ConfigDict  # type: ignore
    _PYDANTIC_V2 = True
except Exception:  # pragma: no cover
    from pydantic import BaseModel, Field  # type: ignore
    ConfigDict = None  # type: ignore
    _PYDANTIC_V2 = False

from env import settings
from core.data_engine_v2 import DataEngine, UnifiedQuote, normalize_symbol
from core.enriched_quote import EnrichedQuote
from core.scoring_engine import enrich_with_scores

logger = logging.getLogger("routes.ai_analysis")

AI_ANALYSIS_VERSION = "3.0.0"

# ---- defaults (hard safe fallbacks if env is missing/mis-set) ----
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

DEFAULT_BATCH_SIZE = _safe_int(getattr(settings, "ai_batch_size", 20), 20)
DEFAULT_BATCH_TIMEOUT = _safe_float(getattr(settings, "ai_batch_timeout_sec", 45.0), 45.0)
DEFAULT_CONCURRENCY = _safe_int(getattr(settings, "ai_batch_concurrency", 5), 5)
DEFAULT_MAX_TICKERS = _safe_int(getattr(settings, "ai_max_tickers", 500), 500)

# ----------------------------------------------------------------------
# Optional schema helper (preferred for sheet-specific header sets)
# ----------------------------------------------------------------------
try:
    from core.schemas import get_headers_for_sheet  # type: ignore
except Exception:  # pragma: no cover
    get_headers_for_sheet = None  # type: ignore


# ----------------------------------------------------------------------
# ENGINE SINGLETON (safe)
# ----------------------------------------------------------------------
@lru_cache(maxsize=1)
def _get_engine_singleton() -> Optional[DataEngine]:
    try:
        logger.info("routes.ai_analysis: initializing DataEngine v2 singleton")
        return DataEngine()
    except Exception as exc:
        logger.exception("routes.ai_analysis: failed to init DataEngine: %s", exc)
        return None


# ----------------------------------------------------------------------
# ROUTER
# ----------------------------------------------------------------------
router = APIRouter(prefix="/v1/analysis", tags=["AI & Analysis"])


# ----------------------------------------------------------------------
# MODELS
# ----------------------------------------------------------------------
class _ExtraIgnoreBase(BaseModel):
    if _PYDANTIC_V2:
        model_config = ConfigDict(extra="ignore")  # type: ignore
    else:  # pragma: no cover
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


def _chunk(items: List[str], size: int) -> List[List[str]]:
    if size <= 0:
        return [items]
    return [items[i : i + size] for i in range(0, len(items), size)]


def _extract_sources(q: EnrichedQuote) -> List[str]:
    # Prefer q.sources if present
    if getattr(q, "sources", None):
        try:
            return [s.provider for s in q.sources]  # type: ignore
        except Exception:
            pass
    ds = getattr(q, "data_source", None)
    return [ds] if ds else []


def _build_default_sheet_headers() -> List[str]:
    # Stable default header set (do not reorder lightly)
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
        "ROA %",
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


def _select_sheet_headers(sheet_name: Optional[str]) -> List[str]:
    # If schemas helper exists, use it when it looks valid.
    if sheet_name and get_headers_for_sheet:
        try:
            h = get_headers_for_sheet(sheet_name)
            if isinstance(h, list) and h and any(str(x).strip().lower() == "symbol" for x in h):
                return [str(x) for x in h]
        except Exception:
            pass
    return _build_default_sheet_headers()


def _pad_row(row: List[Any], length: int) -> List[Any]:
    if len(row) < length:
        return row + [None] * (length - len(row))
    return row[:length]


def _ratio_to_percent(v: Optional[float]) -> Optional[float]:
    """
    Conservative conversion for ratio-like fields (yield/roe/roa).
    - If provider returns 0.15, convert to 15.0
    - If provider returns 15.0, keep as-is
    """
    if v is None:
        return None
    try:
        return (v * 100.0) if -1.0 <= v <= 1.0 else v
    except Exception:
        return v


# ----------------------------------------------------------------------
# ENGINE CALLS (defensive)
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
    if engine is None:
        msg = "Engine unavailable"
        return {s: UnifiedQuote(symbol=s, data_quality="MISSING", error=msg).finalize() for s in clean}

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
                out[s] = UnifiedQuote(symbol=s, data_quality="MISSING", error=msg).finalize()
            continue

        returned = list(res or [])
        chunk_map = {q.symbol.upper(): q for q in returned}

        for s in chunk_syms:
            s_up = s.upper()
            out[s_up] = chunk_map.get(s_up) or UnifiedQuote(symbol=s_up, data_quality="MISSING", error="No data returned").finalize()

    return out


# ----------------------------------------------------------------------
# TRANSFORM
# ----------------------------------------------------------------------
def _quote_to_analysis(requested_symbol: str, uq: UnifiedQuote) -> SingleAnalysisResponse:
    # Unified -> Enriched -> Scores
    eq = EnrichedQuote.from_unified(uq)
    eq = enrich_with_scores(eq)

    sym = (getattr(eq, "symbol", None) or normalize_symbol(requested_symbol) or requested_symbol or "").upper()

    return SingleAnalysisResponse(
        symbol=sym,
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
# SHEET MAPPING (header-driven; supports custom headers safely)
# ----------------------------------------------------------------------
def _analysis_value_map(a: SingleAnalysisResponse) -> Dict[str, Any]:
    return {
        "Symbol": a.symbol,
        "Company Name": a.name or "",
        "Market / Region": a.market_region or "",

        "Price": a.price,
        "Change %": a.change_pct,

        "Market Cap": a.market_cap,
        "P/E (TTM)": a.pe_ttm,
        "P/B": a.pb,

        "Dividend Yield %": _ratio_to_percent(a.dividend_yield),
        "ROE %": _ratio_to_percent(a.roe),
        "ROA %": _ratio_to_percent(a.roa),

        "Data Quality": a.data_quality,

        "Value Score": a.value_score,
        "Quality Score": a.quality_score,
        "Momentum Score": a.momentum_score,
        "Opportunity Score": a.opportunity_score,
        "Overall Score": a.overall_score,

        "Recommendation": a.recommendation or "",

        "Last Updated (UTC)": a.last_updated_utc,
        "Sources": ", ".join(a.sources) if a.sources else "",

        "Fair Value": a.fair_value,
        "Upside %": a.upside_percent,
        "Valuation Label": a.valuation_label or "",

        "Error": a.error or "",
    }


def _analysis_to_sheet_row(a: SingleAnalysisResponse, headers: List[str]) -> List[Any]:
    m = _analysis_value_map(a)
    row = [m.get(h, None) for h in headers]
    return _pad_row(row, len(headers))


# ----------------------------------------------------------------------
# ROUTES (Sheets-safe)
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
        key = normalize_symbol(t).upper() if normalize_symbol(t) else t.upper()
        q = m.get(key) or UnifiedQuote(symbol=key, data_quality="MISSING", error="No data returned").finalize()
        return _quote_to_analysis(t, q)
    except Exception as exc:
        logger.exception("ai_analysis: exception in /quote for %s", t)
        sym = normalize_symbol(t).upper() if normalize_symbol(t) else t.upper()
        return SingleAnalysisResponse(symbol=sym, data_quality="MISSING", error=f"Exception in analysis: {exc}")


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
        q = m.get(t) or UnifiedQuote(symbol=t, data_quality="MISSING", error="No data returned").finalize()
        try:
            results.append(_quote_to_analysis(t, q))
        except Exception as exc:
            logger.exception("ai_analysis: transform failure for %s: %s", t, exc)
            results.append(SingleAnalysisResponse(symbol=t, data_quality="MISSING", error=f"Transform failure: {exc}"))

    return BatchAnalysisResponse(results=results)


@router.post("/sheet-rows", response_model=SheetAnalysisResponse)
async def analyze_for_sheet(body: BatchAnalysisRequest) -> SheetAnalysisResponse:
    """
    Sheets-safe: always returns headers + rows (never raises for normal usage).
    """
    try:
        headers = _select_sheet_headers(body.sheet_name)
        batch = await analyze_batch_quotes(body)
        rows = [_analysis_to_sheet_row(r, headers) for r in batch.results]
        return SheetAnalysisResponse(headers=headers, rows=rows)
    except Exception as exc:
        logger.exception("ai_analysis: exception in /sheet-rows")
        headers = _build_default_sheet_headers()
        tickers = _clean_tickers(body.tickers or [])
        rows = []
        for t in tickers:
            a = SingleAnalysisResponse(symbol=t, data_quality="MISSING", error=str(exc))
            rows.append(_analysis_to_sheet_row(a, headers))
        return SheetAnalysisResponse(headers=headers, rows=rows)


__all__ = [
    "router",
    "SingleAnalysisResponse",
    "BatchAnalysisRequest",
    "BatchAnalysisResponse",
    "SheetAnalysisResponse",
]
