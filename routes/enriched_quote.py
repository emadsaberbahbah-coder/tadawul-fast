"""
routes/enriched_quote.py
===========================================================
Enriched Quotes Router (v3.1)

Unified on top of:
    - core.data_engine_v2.DataEngine (async, multi-provider, KSA-safe)
    - core.enriched_quote.EnrichedQuote (sheet-friendly model)
    - core.schemas.get_headers_for_sheet (if available, otherwise fallback
      to a local Universal Market Template header list)

Key responsibilities
--------------------
- /v1/enriched/quote      -> single EnrichedQuote JSON (for APIs / tests)
- /v1/enriched/quotes     -> batch of EnrichedQuote JSON objects
- /v1/enriched/sheet-rows -> headers + rows for Google Sheets (all 9 pages)

Design notes
------------
- This router never talks directly to Yahoo / FMP / EODHD / Argaam.
  All provider logic lives in DataEngine / its v1 delegate.
- KSA (.SR) symbols are handled KSA-safe by DataEngine:
    • Engine ensures EODHD is not used for KSA (per its own design).
- Extremely defensive:
    • If engine import fails, a stub engine is used (data_quality="MISSING").
    • Batch calls are chunked and bounded by timeouts so Sheets never hangs.

Compatibility
-------------
- Exposes `EnrichedQuoteResponse` as an alias of `EnrichedQuote` so that
  legacy modules (e.g., routes_argaam.py) can still import it without changes.
"""

from __future__ import annotations

import asyncio
import logging
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel, Field

logger = logging.getLogger("routes.enriched_quote")

# ======================================================================
# Engine + models (DataEngine + UnifiedQuote + EnrichedQuote)
# ======================================================================

_ENGINE_MODE: str = "unknown"
_ENGINE_IS_STUB: bool = False

try:
    # Preferred path: real async engine + sheet model
    from core.data_engine_v2 import DataEngine, UnifiedQuote  # type: ignore
    from core.enriched_quote import EnrichedQuote  # type: ignore

    _engine: Any = DataEngine()
    _ENGINE_MODE = "v2"
    _ENGINE_IS_STUB = False
    logger.info("routes.enriched_quote: Using core.data_engine_v2.DataEngine")

except Exception as exc:  # pragma: no cover - defensive
    logger.exception(
        "routes.enriched_quote: Failed to import DataEngine/EnrichedQuote; "
        "falling back to in-process stub engine: %s",
        exc,
    )

    class UnifiedQuote(BaseModel):  # type: ignore[no-redef]
        symbol: str
        data_quality: str = "MISSING"
        error: Optional[str] = None

    class EnrichedQuote(BaseModel):  # type: ignore[no-redef]
        """
        Minimal stub so FastAPI still has a response model if imports fail.
        """

        symbol: str
        data_quality: str = "MISSING"
        error: Optional[str] = None

        @classmethod
        def from_unified(cls, uq: "UnifiedQuote") -> "EnrichedQuote":
            return cls(
                symbol=(uq.symbol or "").upper(),
                data_quality=uq.data_quality or "MISSING",
                error=uq.error,
            )

        def to_row(self, headers: List[str]) -> List[Any]:
            # First cell = symbol, remainder blank
            return [self.symbol] + [None] * (len(headers) - 1)

    class _StubEngine:
        async def get_enriched_quote(self, symbol: str) -> "UnifiedQuote":
            sym = (symbol or "").strip().upper() or "UNKNOWN"
            return UnifiedQuote(
                symbol=sym,
                data_quality="MISSING",
                error="DataEngine v2 unavailable; using stub engine",
            )

        async def get_enriched_quotes(self, symbols: List[str]) -> List["UnifiedQuote"]:
            out: List["UnifiedQuote"] = []
            for s in (symbols or []):
                out.append(await self.get_enriched_quote(s))
            return out

    _engine = _StubEngine()
    _ENGINE_MODE = "stub"
    _ENGINE_IS_STUB = True

# Backwards-compatible alias for legacy routes (e.g. routes_argaam.py)
EnrichedQuoteResponse = EnrichedQuote  # type: ignore[misc]

# ======================================================================
# Header helper (core.schemas) with safe fallback
# ======================================================================

try:
    from core.schemas import get_headers_for_sheet as _get_headers_for_sheet  # type: ignore
except Exception as exc:  # pragma: no cover - defensive
    logger.warning(
        "routes.enriched_quote: core.schemas.get_headers_for_sheet not available; "
        "using local Universal Market Template headers: %s",
        exc,
    )

    def _get_headers_for_sheet(sheet_name: Optional[str]) -> List[str]:
        """
        Fallback header builder – Universal Market Template.
        This keeps the API working even if core.schemas is missing that helper.
        """
        return [
            # Identity
            "Symbol",
            "Company Name",
            "Sector",
            "Sub-Sector",
            "Market",
            "Currency",
            "Listing Date",
            # Price / Liquidity
            "Last Price",
            "Previous Close",
            "Open",
            "High",
            "Low",
            "Change",
            "Change %",
            "52 Week High",
            "52 Week Low",
            "52W Position %",
            "Volume",
            "Average Volume (30D)",
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
            "P/E Ratio",
            "P/B Ratio",
            "Dividend Yield %",
            "Dividend Payout",
            "ROE %",
            "ROA %",
            "Debt/Equity",
            "Current Ratio",
            "Quick Ratio",
            "Market Cap",
            # Growth / Profitability
            "Revenue Growth %",
            "Net Income Growth %",
            "EBITDA Margin %",
            "Operating Margin %",
            "Net Margin %",
            # Valuation / Risk
            "EV/EBITDA",
            "Price/Sales",
            "Price/Cash Flow",
            "PEG Ratio",
            "Beta",
            "Volatility (30D) %",
            # AI Valuation & Scores
            "Fair Value",
            "Upside %",
            "Valuation Label",
            "Value Score",
            "Quality Score",
            "Momentum Score",
            "Opportunity Score",
            "Recommendation",
            # Technical
            "RSI (14)",
            "MACD",
            "Moving Avg (20D)",
            "Moving Avg (50D)",
            # Meta
            "Provider",
            "Data Quality",
            "Last Updated (UTC)",
            "Last Updated (Local)",
            "Timezone",
            "Error",
        ]

# ======================================================================
# Batch / timeout configuration
# ======================================================================

_NON_KSA_BATCH_SIZE = 40          # max symbols per engine call
_ENGINE_BATCH_TIMEOUT = 30.0      # seconds per chunk


def _chunk_list(items: List[str], size: int) -> List[List[str]]:
    if size <= 0:
        return [items]
    return [items[i : i + size] for i in range(0, len(items), size)]


async def _get_unified_quotes_chunked(symbols: List[str]) -> Dict[str, UnifiedQuote]:
    """
    Chunked wrapper around engine.get_enriched_quotes to avoid huge
    single calls (which can cause provider timeouts / 502s).
    Returns a mapping: { original_symbol: UnifiedQuote }
    """
    clean = [s.strip() for s in (symbols or []) if s and s.strip()]
    out: Dict[str, UnifiedQuote] = {}

    if not clean:
        return out

    chunks = _chunk_list(clean, _NON_KSA_BATCH_SIZE)
    logger.info(
        "Enriched engine batch: %d symbols in %d chunk(s) (mode=%s)",
        len(clean),
        len(chunks),
        _ENGINE_MODE,
    )

    for chunk in chunks:
        try:
            batch: List[UnifiedQuote] = await asyncio.wait_for(
                _engine.get_enriched_quotes(chunk),  # type: ignore[attr-defined]
                timeout=_ENGINE_BATCH_TIMEOUT,
            )
            # Map back by original ticker in the same order
            for t, q in zip(chunk, batch):
                out[t] = q
        except asyncio.TimeoutError:
            logger.warning(
                "Engine batch timeout for chunk of size %d (symbols=%s)",
                len(chunk),
                chunk,
            )
            for t in chunk:
                if t not in out:
                    out[t] = UnifiedQuote(
                        symbol=t.upper(),
                        data_quality="MISSING",
                        error="Engine batch timeout",
                    )
        except Exception as exc:  # pragma: no cover - defensive
            logger.exception(
                "Engine batch failure for chunk of size %d (symbols=%s): %s",
                len(chunk),
                chunk,
                exc,
            )
            for t in chunk:
                if t not in out:
                    out[t] = UnifiedQuote(
                        symbol=t.upper(),
                        data_quality="MISSING",
                        error=f"Engine batch error: {exc}",
                    )

    return out


async def _build_sheet_rows(
    symbols: List[str],
    sheet_name: Optional[str],
) -> (List[str], List[List[Any]]):
    """
    Core helper used by /sheet-rows and (optionally) Google Sheets client.
    """
    headers = _get_headers_for_sheet(sheet_name)
    if not symbols:
        return headers, []

    unified_map = await _get_unified_quotes_chunked(symbols)
    rows: List[List[Any]] = []

    for t in symbols:
        uq = unified_map.get(t)
        if uq is None:
            uq = UnifiedQuote(
                symbol=t.upper(),
                data_quality="MISSING",
                error="No data returned from engine",
            )

        try:
            enriched = EnrichedQuote.from_unified(uq)
        except Exception as exc:
            logger.exception(
                "Conversion UnifiedQuote -> EnrichedQuote failed for %s",
                t,
                exc_info=exc,
            )
            enriched = EnrichedQuote(
                symbol=uq.symbol or t.upper(),
                data_quality="MISSING",
                error=f"Conversion error UnifiedQuote->EnrichedQuote: {exc}",
            )

        try:
            row = enriched.to_row(headers)
        except Exception as exc:
            logger.exception("enriched.to_row failed for %s", t, exc_info=exc)
            # Fallback: symbol + blanks
            row = [enriched.symbol] + [None] * (len(headers) - 1)

        rows.append(row)

    return headers, rows


# ======================================================================
# FastAPI router & models
# ======================================================================

router = APIRouter(
    prefix="/v1/enriched",
    tags=["Enriched Quotes"],
)


class BatchEnrichedRequest(BaseModel):
    tickers: List[str] = Field(
        default_factory=list,
        description="List of symbols, e.g. ['AAPL', 'MSFT', '1120.SR']",
    )
    sheet_name: Optional[str] = Field(
        default=None,
        description=(
            "Optional sheet name (e.g. 'KSA_Tadawul', 'Global_Markets', etc.) "
            "used only by /sheet-rows to select the correct headers."
        ),
    )


class BatchEnrichedResponse(BaseModel):
    results: List[EnrichedQuote]


class SheetEnrichedResponse(BaseModel):
    headers: List[str]
    rows: List[List[Any]]


# ======================================================================
# ROUTES
# ======================================================================


@router.get("/health")
async def enriched_health() -> Dict[str, Any]:
    """
    Simple health check for this module.
    """
    return {
        "status": "ok",
        "module": "enriched_quote",
        "version": "3.1",
        "engine_mode": _ENGINE_MODE,
        "engine_is_stub": _ENGINE_IS_STUB,
    }


@router.get("/quote", response_model=EnrichedQuote)
async def get_enriched_quote_route(
    symbol: str = Query(..., alias="symbol"),
) -> EnrichedQuote:
    """
    Get enriched quote for a single symbol.

    Example:
        GET /v1/enriched/quote?symbol=AAPL
        GET /v1/enriched/quote?symbol=1120.SR
    """
    ticker = (symbol or "").strip()
    if not ticker:
        raise HTTPException(status_code=400, detail="Symbol is required")

    try:
        uq: UnifiedQuote = await _engine.get_enriched_quote(ticker)  # type: ignore[attr-defined]
    except Exception as exc:
        logger.exception("Exception in engine.get_enriched_quote for %s", ticker)
        return EnrichedQuote(
            symbol=ticker.upper(),
            data_quality="MISSING",
            error=f"Exception in DataEngine.get_enriched_quote: {exc}",
        )

    try:
        enriched = EnrichedQuote.from_unified(uq)
    except Exception as exc:
        logger.exception(
            "Conversion UnifiedQuote -> EnrichedQuote failed for %s",
            ticker,
            exc_info=exc,
        )
        enriched = EnrichedQuote(
            symbol=uq.symbol or ticker.upper(),
            data_quality="MISSING",
            error=f"Conversion error UnifiedQuote->EnrichedQuote: {exc}",
        )

    if not getattr(enriched, "data_quality", None):
        enriched.data_quality = "UNKNOWN"  # type: ignore[assignment]

    return enriched


@router.post("/quotes", response_model=BatchEnrichedResponse)
async def get_enriched_quotes_route(
    body: BatchEnrichedRequest,
) -> BatchEnrichedResponse:
    """
    Get enriched quotes for multiple symbols.

    Body:
        {
          "tickers": ["AAPL", "MSFT", "1120.SR"]
        }
    """
    tickers = [t.strip() for t in (body.tickers or []) if t and t.strip()]
    if not tickers:
        raise HTTPException(status_code=400, detail="At least one symbol is required")

    unified_map = await _get_unified_quotes_chunked(tickers)
    results: List[EnrichedQuote] = []

    for t in tickers:
        uq = unified_map.get(t)
        if uq is None:
            uq = UnifiedQuote(
                symbol=t.upper(),
                data_quality="MISSING",
                error="No data returned from engine",
            )

        try:
            enriched = EnrichedQuote.from_unified(uq)
        except Exception as exc:
            logger.exception(
                "Conversion UnifiedQuote -> EnrichedQuote failed for %s",
                t,
                exc_info=exc,
            )
            enriched = EnrichedQuote(
                symbol=uq.symbol or t.upper(),
                data_quality="MISSING",
                error=f"Conversion error UnifiedQuote->EnrichedQuote: {exc}",
            )

        if not getattr(enriched, "data_quality", None):
            enriched.data_quality = "UNKNOWN"  # type: ignore[assignment]

        results.append(enriched)

    return BatchEnrichedResponse(results=results)


@router.post("/sheet-rows", response_model=SheetEnrichedResponse)
async def get_enriched_sheet_rows(
    body: BatchEnrichedRequest,
) -> SheetEnrichedResponse:
    """
    Google Sheets–friendly endpoint.

    Returns:
        {
          "headers": [...],
          "rows": [ [row for t1], [row for t2], ... ]
        }

    Apps Script usage pattern:
        - Row 5  = headers
        - Row 6+ = values

    This endpoint is the core bridge for the Ultimate Investment Dashboard
    (KSA_Tadawul, Global_Markets, Mutual_Funds, Commodities_FX, My_Portfolio, etc.).
    """
    tickers = [t.strip() for t in (body.tickers or []) if t and t.strip()]
    sheet_name = body.sheet_name

    headers, rows = await _build_sheet_rows(tickers, sheet_name)
    return SheetEnrichedResponse(headers=headers, rows=rows)
