"""
routes/enriched_quote.py
===========================================================
Enriched Quotes Router (v3.3.0)

Unified on top of:
    - core.data_engine_v2.DataEngine (async, multi-provider, KSA-safe)
    - core.enriched_quote.EnrichedQuote (sheet-friendly model)
    - core.schemas.get_headers_for_sheet (preferred; fallback included)

Key responsibilities
--------------------
- GET  /v1/enriched/quote       -> single EnrichedQuote JSON
- POST /v1/enriched/quotes      -> batch EnrichedQuote JSON objects
- POST /v1/enriched/sheet-rows  -> headers + rows for Google Sheets
- GET  /v1/enriched/headers     -> headers only (debug / Apps Script setup)

Design notes
------------
- Router never talks directly to providers.
- Extremely defensive: chunked batch calls + per-chunk timeout.
- Singleton engine per process to avoid repeated DataEngine init.

Author: Emad Bahbah (with GPT-5.2 Thinking)
"""

from __future__ import annotations

import asyncio
import logging
import os
from functools import lru_cache
from typing import Any, Dict, List, Optional, Sequence, Tuple

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel, Field

logger = logging.getLogger("routes.enriched_quote")

API_VERSION = "3.3.0"

# =============================================================================
# Config (env override)
# =============================================================================

DEFAULT_BATCH_SIZE = int(os.getenv("ENRICHED_BATCH_SIZE", "40"))
DEFAULT_BATCH_TIMEOUT = float(os.getenv("ENRICHED_BATCH_TIMEOUT_SEC", "30"))
DEFAULT_MAX_TICKERS = int(os.getenv("ENRICHED_MAX_TICKERS", "250"))
DEFAULT_MAX_CONCURRENCY = int(os.getenv("ENRICHED_BATCH_CONCURRENCY", "3"))


def _norm_symbol(s: str) -> str:
    return (s or "").strip()


def _dedupe_preserve_order(items: Sequence[str]) -> List[str]:
    seen = set()
    out: List[str] = []
    for x in items:
        if not x:
            continue
        if x in seen:
            continue
        seen.add(x)
        out.append(x)
    return out


def _clean_tickers(tickers: Sequence[str]) -> List[str]:
    clean = [_norm_symbol(t) for t in (tickers or [])]
    clean = [t for t in clean if t]
    return _dedupe_preserve_order(clean)


def _chunk(items: List[str], size: int) -> List[List[str]]:
    if size <= 0:
        return [items]
    return [items[i : i + size] for i in range(0, len(items), size)]


# =============================================================================
# Engine + models (real preferred, stub fallback)
# =============================================================================

_ENGINE_MODE: str = "unknown"
_ENGINE_IS_STUB: bool = False

try:
    from core.data_engine_v2 import DataEngine, UnifiedQuote  # type: ignore
    from core.enriched_quote import EnrichedQuote  # type: ignore

    @lru_cache(maxsize=1)
    def _get_engine_singleton() -> Any:
        """
        Ensure we only initialize DataEngine once per process.
        (Render logs showed multiple inits when routers are imported.)
        """
        return DataEngine()

    _ENGINE_MODE = "v2"
    _ENGINE_IS_STUB = False
    logger.info("routes.enriched_quote: Using core.data_engine_v2.DataEngine (singleton)")

except Exception as exc:  # pragma: no cover
    logger.exception(
        "routes.enriched_quote: Failed to import DataEngine/EnrichedQuote; "
        "falling back to stub engine: %s",
        exc,
    )

    class UnifiedQuote(BaseModel):  # type: ignore[no-redef]
        symbol: str
        data_quality: str = "MISSING"
        error: Optional[str] = None

    class EnrichedQuote(BaseModel):  # type: ignore[no-redef]
        symbol: str
        data_quality: str = "MISSING"
        error: Optional[str] = None

        @classmethod
        def from_unified(cls, uq: "UnifiedQuote | Dict[str, Any]") -> "EnrichedQuote":
            if isinstance(uq, dict):
                return cls(**uq)
            return cls(symbol=(uq.symbol or "").upper(), data_quality=uq.data_quality, error=uq.error)

        def to_row(self, headers: Sequence[str]) -> List[Any]:
            return [self.symbol] + [None] * (len(list(headers)) - 1)

    class _StubEngine:
        async def get_enriched_quote(self, symbol: str) -> UnifiedQuote:
            sym = (symbol or "").strip() or "UNKNOWN"
            return UnifiedQuote(
                symbol=sym.upper(),
                data_quality="MISSING",
                error="DataEngine v2 unavailable; using stub engine",
            )

        async def get_enriched_quotes(self, symbols: List[str]) -> List[UnifiedQuote]:
            return [await self.get_enriched_quote(s) for s in (symbols or [])]

    @lru_cache(maxsize=1)
    def _get_engine_singleton() -> Any:
        return _StubEngine()

    _ENGINE_MODE = "stub"
    _ENGINE_IS_STUB = True


# Backwards-compatible alias for legacy routes importing it
EnrichedQuoteResponse = EnrichedQuote  # type: ignore[misc]


# =============================================================================
# Header helper (core.schemas) with safe fallback
# =============================================================================

try:
    from core.schemas import get_headers_for_sheet as _get_headers_for_sheet  # type: ignore
except Exception as exc:  # pragma: no cover
    logger.warning(
        "routes.enriched_quote: core.schemas.get_headers_for_sheet not available; "
        "using local fallback headers. (%s)",
        exc,
    )

    def _get_headers_for_sheet(sheet_name: Optional[str]) -> List[str]:
        # Minimal, stable fallback (keep aligned with core.enriched_quote mapper)
        return [
            # Identity
            "Symbol",
            "Company Name",
            "Sector",
            "Sub-Sector",
            "Industry",
            "Market",
            "Market Region",
            "Exchange",
            "Currency",
            "Listing Date",
            # Capital structure / ownership
            "Shares Outstanding",
            "Free Float",
            "Insider Ownership %",
            "Institutional Ownership %",
            "Short Ratio",
            "Short % Float",
            # Price / liquidity
            "Last Price",
            "Previous Close",
            "Open",
            "High",
            "Low",
            "Change",
            "Change %",
            "52W High",
            "52W Low",
            "52W Position %",
            "Volume",
            "Avg Volume (30D)",
            "Value Traded",
            "Turnover Rate",
            "Bid Price",
            "Ask Price",
            "Bid Size",
            "Ask Size",
            "Spread %",
            "Liquidity Score",
            # Fundamentals
            "EPS (TTM)",
            "Forward EPS",
            "P/E (TTM)",
            "Forward P/E",
            "P/B",
            "P/S",
            "EV/EBITDA",
            "Dividend Yield %",
            "Dividend Rate",
            "Payout Ratio %",
            "Ex-Dividend Date",
            "ROE %",
            "ROA %",
            "Net Margin %",
            "EBITDA Margin %",
            "Revenue Growth %",
            "Net Income Growth %",
            "Debt/Equity",
            "Current Ratio",
            "Quick Ratio",
            "Market Cap",
            "Free Float Market Cap",
            # Risk / technical
            "Beta",
            "Volatility (30D) %",
            "RSI (14)",
            "MACD",
            "MA (20D)",
            "MA (50D)",
            # AI / valuation
            "Target Price",
            "Fair Value",
            "Upside %",
            "Valuation Label",
            "Value Score",
            "Quality Score",
            "Momentum Score",
            "Opportunity Score",
            "Overall Score",
            "Rank",
            "Recommendation",
            "Risk Label",
            "Risk Bucket",
            "Exp ROI (3M)",
            "Exp ROI (12M)",
            # Meta
            "Provider",
            "Primary Provider",
            "Data Source",
            "Data Quality",
            "Last Updated (UTC)",
            "Last Updated (Riyadh)",
            "As Of (UTC)",
            "As Of (Local)",
            "Timezone",
            "Error",
        ]


# =============================================================================
# Engine call wrappers (support alternate method names safely)
# =============================================================================

async def _engine_get_one(symbol: str) -> Any:
    eng = _get_engine_singleton()
    if hasattr(eng, "get_enriched_quote"):
        return await eng.get_enriched_quote(symbol)  # type: ignore[attr-defined]
    if hasattr(eng, "get_quote"):
        return await eng.get_quote(symbol)  # type: ignore[attr-defined]
    raise RuntimeError("Engine missing get_enriched_quote/get_quote")


async def _engine_get_many(symbols: List[str]) -> List[Any]:
    eng = _get_engine_singleton()
    if hasattr(eng, "get_enriched_quotes"):
        return await eng.get_enriched_quotes(symbols)  # type: ignore[attr-defined]
    if hasattr(eng, "get_quotes"):
        return await eng.get_quotes(symbols)  # type: ignore[attr-defined]
    # fallback to per-symbol
    out = []
    for s in symbols:
        out.append(await _engine_get_one(s))
    return out


def _make_missing_unified(symbol: str, message: str) -> Any:
    # Works for both real UnifiedQuote (with defaults) and stub.
    try:
        return UnifiedQuote(symbol=symbol.upper(), data_quality="MISSING", error=message)
    except Exception:
        # last resort: dict
        return {"symbol": symbol.upper(), "data_quality": "MISSING", "error": message}


async def _get_unified_quotes_chunked(
    symbols: List[str],
    *,
    batch_size: int = DEFAULT_BATCH_SIZE,
    timeout_sec: float = DEFAULT_BATCH_TIMEOUT,
    max_concurrency: int = DEFAULT_MAX_CONCURRENCY,
) -> Dict[str, Any]:
    """
    Chunked + concurrency-limited batch fetch.
    Returns map keyed by original input symbol (exact string from `symbols` list).
    """
    clean = _clean_tickers(symbols)
    if not clean:
        return {}

    chunks = _chunk(clean, batch_size)
    sem = asyncio.Semaphore(max(1, max_concurrency))

    logger.info(
        "Enriched batch: %d symbols in %d chunk(s) (engine=%s, batch=%d, timeout=%.1fs, conc=%d)",
        len(clean),
        len(chunks),
        _ENGINE_MODE,
        batch_size,
        timeout_sec,
        max(1, max_concurrency),
    )

    async def _run_chunk(chunk_syms: List[str]) -> Tuple[List[str], List[Any] | Exception]:
        async with sem:
            try:
                result = await asyncio.wait_for(_engine_get_many(chunk_syms), timeout=timeout_sec)
                return chunk_syms, result
            except Exception as e:
                return chunk_syms, e

    tasks = [_run_chunk(c) for c in chunks]
    results = await asyncio.gather(*tasks)

    out: Dict[str, Any] = {}

    for chunk_syms, res in results:
        if isinstance(res, Exception):
            msg = f"Engine batch error: {res}"
            if isinstance(res, asyncio.TimeoutError):
                msg = "Engine batch timeout"
            logger.warning("%s for chunk(size=%d): %s", msg, len(chunk_syms), chunk_syms)
            for s in chunk_syms:
                out[s] = _make_missing_unified(s, msg)
            continue

        # Build a symbol->quote map from returned payload (order-safe)
        by_sym: Dict[str, Any] = {}
        for q in (res or []):
            sym = getattr(q, "symbol", None) or (q.get("symbol") if isinstance(q, dict) else None)
            if isinstance(sym, str) and sym.strip():
                by_sym[sym.strip().upper()] = q

        for s in chunk_syms:
            # try exact (upper) match first
            q = by_sym.get(s.upper())
            if q is None:
                q = _make_missing_unified(s, "No data returned from engine")
            out[s] = q

    return out


def _to_enriched(uq: Any, fallback_symbol: str) -> Any:
    try:
        return EnrichedQuote.from_unified(uq)
    except Exception as exc:
        logger.exception("UnifiedQuote -> EnrichedQuote conversion failed for %s", fallback_symbol, exc_info=exc)
        try:
            return EnrichedQuote(symbol=fallback_symbol.upper(), data_quality="MISSING", error=f"Conversion error: {exc}")
        except Exception:
            # absolute last resort
            return EnrichedQuote.from_unified(
                {"symbol": fallback_symbol.upper(), "data_quality": "MISSING", "error": f"Conversion error: {exc}"}
            )


async def _build_sheet_rows(
    symbols: List[str],
    sheet_name: Optional[str],
) -> Tuple[List[str], List[List[Any]]]:
    headers = _get_headers_for_sheet(sheet_name)
    clean = _clean_tickers(symbols)
    if not clean:
        return headers, []

    unified_map = await _get_unified_quotes_chunked(clean)
    rows: List[List[Any]] = []

    for s in clean:
        uq = unified_map.get(s) or _make_missing_unified(s, "No data returned from engine")
        enriched = _to_enriched(uq, s)

        try:
            row = enriched.to_row(headers)  # type: ignore[attr-defined]
        except Exception as exc:
            logger.exception("enriched.to_row failed for %s", s, exc_info=exc)
            row = [getattr(enriched, "symbol", s.upper())] + [None] * (len(headers) - 1)

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
    eng = _get_engine_singleton()
    providers = getattr(eng, "enabled_providers", None) or getattr(eng, "providers", None)
    primary = getattr(eng, "primary_provider", None) or getattr(eng, "primary", None)
    return {
        "status": "ok",
        "module": "enriched_quote",
        "version": API_VERSION,
        "engine_mode": _ENGINE_MODE,
        "engine_is_stub": _ENGINE_IS_STUB,
        "engine_providers": providers,
        "engine_primary": primary,
        "batch_size": DEFAULT_BATCH_SIZE,
        "batch_timeout_sec": DEFAULT_BATCH_TIMEOUT,
        "max_tickers": DEFAULT_MAX_TICKERS,
        "batch_concurrency": DEFAULT_MAX_CONCURRENCY,
    }


@router.get("/headers")
async def get_headers(
    sheet_name: Optional[str] = Query(default=None, description="Sheet name, e.g. 'Global_Markets'"),
) -> Dict[str, Any]:
    headers = _get_headers_for_sheet(sheet_name)
    return {"sheet_name": sheet_name, "count": len(headers), "headers": headers}


@router.get("/quote", response_model=EnrichedQuote, response_model_exclude_none=True)
async def get_enriched_quote_route(
    symbol: str = Query(..., alias="symbol"),
) -> EnrichedQuote:
    ticker = _norm_symbol(symbol)
    if not ticker:
        raise HTTPException(status_code=400, detail="Symbol is required")

    try:
        uq = await _engine_get_one(ticker)
    except Exception as exc:
        logger.exception("engine.get_enriched_quote failed for %s", ticker, exc_info=exc)
        return _to_enriched(_make_missing_unified(ticker, f"Engine error: {exc}"), ticker)

    return _to_enriched(uq, ticker)


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
    if len(tickers) > DEFAULT_MAX_TICKERS:
        raise HTTPException(
            status_code=400,
            detail=f"Too many symbols ({len(tickers)}). Max allowed is {DEFAULT_MAX_TICKERS}.",
        )

    headers, rows = await _build_sheet_rows(tickers, body.sheet_name)
    return SheetEnrichedResponse(headers=headers, rows=rows)


__all__ = ["router", "EnrichedQuoteResponse"]
