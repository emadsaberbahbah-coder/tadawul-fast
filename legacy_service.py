# legacy_service.py
"""
routes/legacy_service.py
------------------------------------------------------------
LEGACY SERVICE BRIDGE – v3.9.0 (Engine v2 aligned, ultra-defensive)

Goals
- Keep old clients working (/v1/quote, /v1/quotes).
- No direct provider calls (always DataEngine v2).
- Scores included when available (scoring_engine + EnrichedQuote).
- Never crash Sheets/clients: always returns a valid JSON response.

Compatibility
- GET  /v1/quote?symbol=1120.SR   (also supports 1120 / TADAWUL:1120 / 1120.TADAWUL)
- POST /v1/quotes   body: {"tickers":[...]}  (also accepts {"symbols":[...]})
- GET  /v1/legacy/health
- POST /v1/legacy/sheet-rows  (optional helper for Sheets)
"""

from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timezone
from functools import lru_cache
from typing import Any, Dict, List, Optional, Sequence

from fastapi import APIRouter, Body, HTTPException, Query, status
from pydantic import BaseModel, Field

from env import settings
from core.data_engine_v2 import DataEngine, UnifiedQuote, normalize_symbol

logger = logging.getLogger("routes.legacy_service")
LEGACY_VERSION = "3.9.0"


# -----------------------------------------------------------------------------
# Safe settings getters (won't crash if env misses a field)
# -----------------------------------------------------------------------------
def _get_int(name: str, default: int) -> int:
    try:
        v = getattr(settings, name)
        return int(v)
    except Exception:
        return default


def _get_float(name: str, default: float) -> float:
    try:
        v = getattr(settings, name)
        return float(v)
    except Exception:
        return default


BATCH_SIZE = max(1, _get_int("enriched_batch_size", 40))
BATCH_TIMEOUT = max(5.0, _get_float("enriched_batch_timeout_sec", 45.0))
MAX_CONCURRENCY = max(1, _get_int("enriched_batch_concurrency", 5))
MAX_TICKERS = max(50, _get_int("enriched_max_tickers", 250))


# -----------------------------------------------------------------------------
# Optional schema helper for sheet-rows
# -----------------------------------------------------------------------------
try:
    from core.schemas import get_headers_for_sheet  # type: ignore
except Exception:  # pragma: no cover
    def get_headers_for_sheet(sheet_name: str) -> List[str]:
        return [
            "Symbol",
            "Company Name",
            "Market",
            "Currency",
            "Current Price",
            "Previous Close",
            "Price Change",
            "Percent Change",
            "Day High",
            "Day Low",
            "52W High",
            "52W Low",
            "Volume",
            "Market Cap",
            "P/E (TTM)",
            "P/B",
            "Dividend Yield %",
            "ROE %",
            "Value Score",
            "Quality Score",
            "Momentum Score",
            "Opportunity Score",
            "Overall Score",
            "Recommendation",
            "Data Quality",
            "Data Source",
            "Last Updated (UTC)",
            "Error",
        ]


# -----------------------------------------------------------------------------
# Optional enrichment helpers (do not hard-depend)
# -----------------------------------------------------------------------------
try:
    from core.enriched_quote import EnrichedQuote  # type: ignore
except Exception:  # pragma: no cover
    EnrichedQuote = None  # type: ignore

try:
    from core.scoring_engine import enrich_with_scores  # type: ignore
except Exception:  # pragma: no cover
    enrich_with_scores = None  # type: ignore


# -----------------------------------------------------------------------------
# Models
# -----------------------------------------------------------------------------
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
    overall_score: Optional[float] = None
    recommendation: Optional[str] = None

    data_quality: str = "MISSING"
    data_source: Optional[str] = None
    last_updated_utc: Optional[str] = None
    error: Optional[str] = None


class LegacyResponse(BaseModel):
    quotes: List[LegacyQuoteModel]
    meta: Dict[str, Any]


class LegacyBatchRequest(BaseModel):
    tickers: Optional[List[str]] = Field(default=None, description="Legacy key")
    symbols: Optional[List[str]] = Field(default=None, description="Alias key")
    sheet_name: Optional[str] = None


class SheetRowsResponse(BaseModel):
    headers: List[str]
    rows: List[List[Any]]
    meta: Dict[str, Any] = Field(default_factory=dict)


# -----------------------------------------------------------------------------
# Engine singleton
# -----------------------------------------------------------------------------
@lru_cache(maxsize=1)
def _get_engine() -> DataEngine:
    logger.info("legacy_service: Initializing DataEngine v2 singleton")
    return DataEngine()


# -----------------------------------------------------------------------------
# Helpers
# -----------------------------------------------------------------------------
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


def _normalize_symbol_legacy(raw: str) -> str:
    """
    Legacy normalization:
    - Accepts numeric KSA -> 1120.SR
    - Accepts TADAWUL:1120, 1120.TADAWUL
    - Else delegates to v2 normalize_symbol (adds .US when needed)
    """
    s = (raw or "").strip()
    if not s:
        return ""

    u = s.strip().upper()

    if u.startswith("TADAWUL:"):
        u = u.split(":", 1)[1].strip()

    if u.endswith(".TADAWUL"):
        base = u.replace(".TADAWUL", "").strip()
        if base.isdigit():
            return f"{base}.SR"
        # If weird format, still pass to v2 normalizer
        return normalize_symbol(base)

    if u.isdigit():
        return f"{u}.SR"

    # Let v2 do the final normalization (handles .SR, .US, ^GSPC, GC=F, EURUSD=X, etc.)
    return normalize_symbol(u)


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _map_unified_to_legacy(uq: UnifiedQuote) -> LegacyQuoteModel:
    """
    Best-effort mapping:
    - Prefer EnrichedQuote.from_unified + scoring (if available)
    - Else map directly from UnifiedQuote fields
    """
    # Ensure finalized to get derived fields (percent_change, etc.)
    try:
        uq = uq.finalize()
    except Exception:
        pass

    # Enriched path (preferred)
    if EnrichedQuote is not None:
        try:
            eq = EnrichedQuote.from_unified(uq)  # type: ignore
            if enrich_with_scores is not None:
                try:
                    eq = enrich_with_scores(eq)  # type: ignore
                except Exception:
                    pass

            return LegacyQuoteModel(
                symbol=getattr(eq, "symbol", uq.symbol),
                ticker=getattr(eq, "symbol", uq.symbol),
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
                overall_score=getattr(eq, "overall_score", None),
                recommendation=getattr(eq, "recommendation", None),

                data_quality=getattr(eq, "data_quality", "MISSING"),
                data_source=getattr(eq, "data_source", None),
                last_updated_utc=getattr(eq, "last_updated_utc", None),
                error=getattr(eq, "error", None),
            )
        except Exception as exc:
            # Fall back to unified mapping
            logger.warning("legacy_service: Enriched mapping failed for %s: %s", uq.symbol, exc)

    # Unified-only fallback
    return LegacyQuoteModel(
        symbol=uq.symbol,
        ticker=uq.symbol,
        name=uq.name,
        market=uq.market,
        currency=uq.currency,
        sector=uq.sector,
        industry=uq.industry,

        price=uq.current_price,
        previous_close=uq.previous_close,
        open=uq.open,
        high=uq.day_high,
        low=uq.day_low,
        change=uq.price_change,
        change_percent=uq.percent_change,

        volume=uq.volume,
        avg_volume=uq.avg_volume_30d,
        market_cap=uq.market_cap,
        shares_outstanding=uq.shares_outstanding,

        fifty_two_week_high=uq.high_52w,
        fifty_two_week_low=uq.low_52w,
        fifty_two_week_position_pct=uq.position_52w_percent,

        pe_ratio=uq.pe_ttm,
        pb_ratio=uq.pb,
        dividend_yield=uq.dividend_yield,
        eps=uq.eps_ttm,
        roe=uq.roe,
        roa=uq.roa,
        profit_margin=uq.net_margin,
        debt_to_equity=uq.debt_to_equity,

        value_score=uq.value_score,
        quality_score=uq.quality_score,
        momentum_score=uq.momentum_score,
        opportunity_score=uq.opportunity_score,
        overall_score=uq.overall_score,
        recommendation=uq.recommendation,

        data_quality=uq.data_quality or "MISSING",
        data_source=uq.data_source,
        last_updated_utc=uq.last_updated_utc,
        error=uq.error,
    )


def _chunk(items: List[str], size: int) -> List[List[str]]:
    if size <= 0:
        return [items]
    return [items[i : i + size] for i in range(0, len(items), size)]


async def _fetch_legacy_quotes(tickers: List[str]) -> List[LegacyQuoteModel]:
    # Normalize + dedupe preserve order
    clean = [_normalize_symbol_legacy(t) for t in (tickers or []) if (t or "").strip()]
    clean = [c for c in clean if c]
    clean = _dedupe_preserve_order(clean)

    if not clean:
        return []

    if len(clean) > MAX_TICKERS:
        raise HTTPException(
            status_code=status.HTTP_413_REQUEST_ENTITY_TOO_LARGE,
            detail=f"Too many tickers. Max allowed is {MAX_TICKERS}.",
        )

    engine = _get_engine()
    chunks = _chunk(clean, BATCH_SIZE)
    sem = asyncio.Semaphore(MAX_CONCURRENCY)

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
                logger.error("legacy_service: batch error: %s", msg)
                for s in chunk_syms:
                    out[s.upper()] = LegacyQuoteModel(
                        symbol=s,
                        ticker=s,
                        data_quality="MISSING",
                        last_updated_utc=_utc_now(),
                        error=msg,
                    )
            return out

    dicts = await asyncio.gather(*[_process_chunk(c) for c in chunks])
    merged: Dict[str, LegacyQuoteModel] = {}
    for d in dicts:
        merged.update(d)

    # Return in the same order as requested
    final_list: List[LegacyQuoteModel] = []
    for s in clean:
        final_list.append(
            merged.get(s.upper())
            or LegacyQuoteModel(symbol=s, ticker=s, data_quality="MISSING", last_updated_utc=_utc_now(), error="No data returned")
        )
    return final_list


def _eq_to_row(eq: Any, headers: List[str]) -> List[Any]:
    """
    Very defensive row mapper:
    - If EnrichedQuote.to_row exists, use it.
    - Else map by known header names from Unified/Legacy naming.
    """
    if hasattr(eq, "to_row"):
        try:
            row = eq.to_row(headers)  # type: ignore
            row = list(row) if isinstance(row, (list, tuple)) else [row]
            if len(row) < len(headers):
                row += [None] * (len(headers) - len(row))
            return row[: len(headers)]
        except Exception:
            pass

    # fallback basic mapping
    m = {}
    for k in dir(eq):
        if k.startswith("_"):
            continue
        try:
            m[k] = getattr(eq, k)
        except Exception:
            pass

    def pick(*names):
        for n in names:
            if n in m and m[n] is not None:
                return m[n]
        return None

    row: List[Any] = []
    for h in headers:
        key = (h or "").strip().lower()

        if key in {"symbol", "ticker"}:
            row.append(pick("symbol") or "UNKNOWN")
        elif key in {"company name", "name"}:
            row.append(pick("name"))
        elif key == "market":
            row.append(pick("market"))
        elif key == "currency":
            row.append(pick("currency"))
        elif key in {"current price", "last price", "price"}:
            row.append(pick("current_price", "last_price", "price"))
        elif key in {"previous close"}:
            row.append(pick("previous_close"))
        elif key in {"price change", "change"}:
            row.append(pick("price_change", "change"))
        elif key in {"percent change", "change %", "change percent"}:
            row.append(pick("percent_change", "change_percent"))
        elif key in {"day high", "high"}:
            row.append(pick("day_high", "high"))
        elif key in {"day low", "low"}:
            row.append(pick("day_low", "low"))
        elif key in {"52w high", "fifty_two_week_high"}:
            row.append(pick("high_52w", "fifty_two_week_high"))
        elif key in {"52w low", "fifty_two_week_low"}:
            row.append(pick("low_52w", "fifty_two_week_low"))
        elif "52w position" in key:
            row.append(pick("position_52w_percent", "fifty_two_week_position_pct"))
        elif key == "volume":
            row.append(pick("volume"))
        elif key == "market cap":
            row.append(pick("market_cap"))
        elif key in {"p/e (ttm)", "pe (ttm)", "pe_ratio"}:
            row.append(pick("pe_ttm", "pe_ratio"))
        elif key in {"p/b", "pb_ratio"}:
            row.append(pick("pb", "pb_ratio"))
        elif "dividend" in key:
            row.append(pick("dividend_yield"))
        elif key == "roe %":
            row.append(pick("roe"))
        elif key == "value score":
            row.append(pick("value_score"))
        elif key == "quality score":
            row.append(pick("quality_score"))
        elif key == "momentum score":
            row.append(pick("momentum_score"))
        elif key == "opportunity score":
            row.append(pick("opportunity_score"))
        elif key == "overall score":
            row.append(pick("overall_score"))
        elif key == "recommendation":
            row.append(pick("recommendation"))
        elif key == "data quality":
            row.append(pick("data_quality") or "MISSING")
        elif key == "data source":
            row.append(pick("data_source"))
        elif key in {"last updated (utc)", "last_updated_utc"}:
            row.append(pick("last_updated_utc") or _utc_now())
        elif key == "error":
            row.append(pick("error"))
        else:
            row.append(None)

    if len(row) < len(headers):
        row += [None] * (len(headers) - len(row))
    return row[: len(headers)]


# -----------------------------------------------------------------------------
# Router
# -----------------------------------------------------------------------------
router = APIRouter(tags=["Legacy Service"])


@router.get("/v1/legacy/health")
async def legacy_health() -> Dict[str, Any]:
    engine = _get_engine()
    return {
        "status": "ok",
        "module": "routes.legacy_service",
        "version": LEGACY_VERSION,
        "engine": "DataEngineV2",
        "providers": getattr(engine, "enabled_providers", None) or getattr(settings, "enabled_providers", None),
        "batch": {
            "size": BATCH_SIZE,
            "timeout_sec": BATCH_TIMEOUT,
            "concurrency": MAX_CONCURRENCY,
            "max_tickers": MAX_TICKERS,
        },
        "time_utc": _utc_now(),
    }


@router.get("/v1/quote", response_model=LegacyResponse)
async def get_legacy_quote(symbol: str = Query(..., alias="symbol")) -> LegacyResponse:
    quotes = await _fetch_legacy_quotes([symbol])
    return LegacyResponse(
        quotes=quotes,
        meta={
            "count": len(quotes),
            "timestamp": _utc_now(),
            "note": "Powered by Tadawul Fast Bridge V2",
        },
    )


@router.post("/v1/quotes", response_model=LegacyResponse)
async def get_legacy_quotes_batch(body: LegacyBatchRequest = Body(...)) -> LegacyResponse:
    symbols = body.tickers or body.symbols or []
    quotes = await _fetch_legacy_quotes(symbols)
    return LegacyResponse(
        quotes=quotes,
        meta={
            "count": len(quotes),
            "timestamp": _utc_now(),
            "note": "Powered by Tadawul Fast Bridge V2",
        },
    )


@router.post("/v1/legacy/sheet-rows", response_model=SheetRowsResponse)
async def legacy_sheet_rows(body: LegacyBatchRequest = Body(...)) -> SheetRowsResponse:
    """
    Optional helper for Google Sheets:
    - Returns headers + rows using EnrichedQuote.to_row when available.
    """
    raw = body.tickers or body.symbols or []
    sheet_name = body.sheet_name or "Legacy_Sheet"

    headers = get_headers_for_sheet(sheet_name)

    # Normalize & dedupe
    clean = [_normalize_symbol_legacy(t) for t in raw if (t or "").strip()]
    clean = [c for c in clean if c]
    clean = _dedupe_preserve_order(clean)

    if not clean:
        return SheetRowsResponse(headers=headers, rows=[], meta={"count": 0, "note": "No valid symbols"})

    if len(clean) > MAX_TICKERS:
        raise HTTPException(
            status_code=status.HTTP_413_REQUEST_ENTITY_TOO_LARGE,
            detail=f"Too many tickers. Max allowed is {MAX_TICKERS}.",
        )

    engine = _get_engine()
    try:
        uqs = await asyncio.wait_for(engine.get_enriched_quotes(clean), timeout=BATCH_TIMEOUT)
        uq_map = {q.symbol.upper(): q for q in (uqs or [])}
    except Exception as exc:
        # Sheets-safe fallback
        rows = [[s] + [None] * (len(headers) - 2) + [str(exc)] for s in clean]
        return SheetRowsResponse(headers=headers, rows=rows, meta={"count": len(rows), "error": str(exc), "sheet_name": sheet_name})

    rows: List[List[Any]] = []
    for s in clean:
        uq = uq_map.get(s.upper()) or UnifiedQuote(symbol=s, data_quality="MISSING", error="No data returned").finalize()

        # Try EnrichedQuote mapping for headers
        if EnrichedQuote is not None:
            try:
                eq = EnrichedQuote.from_unified(uq)  # type: ignore
                if enrich_with_scores is not None:
                    try:
                        eq = enrich_with_scores(eq)  # type: ignore
                    except Exception:
                        pass
                row = _eq_to_row(eq, headers)
                rows.append(row)
                continue
            except Exception:
                pass

        # fallback row from UnifiedQuote-like object
        rows.append(_eq_to_row(uq, headers))

    return SheetRowsResponse(
        headers=headers,
        rows=rows,
        meta={"count": len(rows), "sheet_name": sheet_name},
    )


# Backward-compatible export used elsewhere
get_legacy_quotes = _fetch_legacy_quotes

__all__ = ["router", "get_legacy_quotes"]
