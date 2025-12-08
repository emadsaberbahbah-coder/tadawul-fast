"""
routes_argaam.py
------------------------------------------------------------
KSA / Argaam Gateway – v1.0

Thin KSA-only wrapper on top of the unified DataEngine v2.

- Only accepts Tadawul-style symbols (.SR or numeric).
- Uses core.data_engine_v2.DataEngine which internally:
    • delegates KSA to v1 engine / Tadawul+Argaam providers (NO EODHD),
    • uses FMP/EODHD/Finnhub for non-KSA (but we block those here).

Exposes:
    • /v1/argaam/health
    • /v1/argaam/sheet-rows  (Google Sheets–friendly, same structure as /v1/enriched/sheet-rows)
"""

from __future__ import annotations

import logging
from typing import Any, Dict, List

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field

from core.data_engine_v2 import DataEngine
from routes.enriched_quote import (
    EnrichedQuoteResponse,
    _quote_to_enriched,
    _build_sheet_headers as _enriched_headers,
    _enriched_to_sheet_row as _enriched_row,
)

logger = logging.getLogger(__name__)

router = APIRouter(
    prefix="/v1/argaam",
    tags=["KSA / Argaam"],
)

# Reuse the unified engine (KSA-safe: no EODHD for .SR inside DataEngine)
_engine = DataEngine()


def _normalize_ksa_symbol(symbol: str) -> str:
    # staticmethod from DataEngine
    return DataEngine._normalize_symbol(symbol)


def _ensure_ksa(symbol: str) -> str:
    """
    Ensure symbol is KSA (.SR / .TADAWUL or numeric) and normalize.
    """
    s = _normalize_ksa_symbol(symbol)
    if not s:
        raise HTTPException(status_code=400, detail="Symbol is required")

    # KSA only: numeric or .SR / .TADAWUL
    if not (s.isdigit() or s.endswith(".SR") or s.endswith(".TADAWUL")):
        raise HTTPException(
            status_code=400,
            detail="KSA / Argaam gateway only supports Tadawul (.SR) tickers",
        )

    # Ensure .SR suffix for pure numeric
    if s.isdigit():
        s = f"{s}.SR"

    return s


class KSABatchRequest(BaseModel):
    tickers: List[str] = Field(
        default_factory=list,
        description="List of KSA symbols, e.g. ['1120.SR','1180.SR']",
    )


class KSASheetResponse(BaseModel):
    headers: List[str]
    rows: List[List[Any]]


@router.get("/health")
async def argaam_health() -> Dict[str, Any]:
    """
    Simple health check for KSA / Argaam gateway.
    """
    return {
        "status": "ok",
        "module": "routes_argaam",
        "engine": "DataEngine_v2",
    }


@router.post("/sheet-rows", response_model=KSASheetResponse)
async def ksa_sheet_rows(body: KSABatchRequest) -> KSASheetResponse:
    """
    Google Sheets–friendly endpoint for KSA tickers only.

    Returns:
        {
          "headers": [...],        # identical to /v1/enriched/sheet-rows
          "rows": [ [row for t1], [row for t2], ... ]
        }
    """
    tickers_raw = body.tickers or []
    if not tickers_raw:
        raise HTTPException(status_code=400, detail="At least one symbol is required")

    ksa_symbols: List[str] = []
    for t in tickers_raw:
        try:
            ksa_symbols.append(_ensure_ksa(t))
        except HTTPException as exc:
            # Instead of failing the whole batch, log + skip invalid symbol
            logger.warning(
                "Skipping non-KSA symbol in /v1/argaam: %s (%s)", t, exc.detail
            )

    if not ksa_symbols:
        raise HTTPException(
            status_code=400,
            detail="No valid KSA (.SR) symbols found in request",
        )

    try:
        unified_quotes = await _engine.get_enriched_quotes(ksa_symbols)
    except Exception as exc:
        logger.exception("KSA sheet-rows engine failure for %s", ksa_symbols)
        # Return placeholder rows with error instead of 500
        headers = _enriched_headers()
        rows: List[List[Any]] = []
        for sym in ksa_symbols:
            placeholder = EnrichedQuoteResponse(
                symbol=sym,
                data_quality="MISSING",
                error=f"KSA / Argaam gateway error: {exc}",
            )
            rows.append(_enriched_row(placeholder))
        return KSASheetResponse(headers=headers, rows=rows)

    headers = _enriched_headers()
    rows: List[List[Any]] = []

    for q in unified_quotes:
        try:
            enriched = _quote_to_enriched(q)
            if enriched.data_quality == "MISSING" and not enriched.error:
                enriched.error = "No data available from KSA providers"
            rows.append(_enriched_row(enriched))
        except Exception as exc:
            logger.exception("Error mapping KSA quote to sheet row: %s", exc)
            placeholder = EnrichedQuoteResponse(
                symbol=str(getattr(q, "symbol", "")).upper(),
                data_quality="MISSING",
                error=f"Exception mapping KSA quote: {exc}",
            )
            rows.append(_enriched_row(placeholder))

    return KSASheetResponse(headers=headers, rows=rows)
