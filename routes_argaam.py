"""
routes_argaam.py
------------------------------------------------------------
KSA / Argaam Gateway – v1.1

Thin KSA-only wrapper on top of the unified DataEngine v2.

- Only accepts Tadawul-style symbols (.SR or numeric).
- Uses core.data_engine_v2.DataEngine which internally:
    • delegates KSA to v1 engine / Tadawul+Argaam providers (NO EODHD),
    • uses FMP/EODHD/Finnhub for non-KSA (but we block those here).

Exposes:
    • GET  /v1/argaam/health
    • GET  /v1/argaam/quote?symbol=1120.SR
    • POST /v1/argaam/quotes      (JSON -> list of EnrichedQuoteResponse)
    • POST /v1/argaam/sheet-rows  (Google Sheets–friendly, same structure
                                    as /v1/enriched/sheet-rows: headers + rows)
"""

from __future__ import annotations

import logging
import os
from typing import Any, Dict, List

from fastapi import APIRouter, HTTPException, Query
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

# ---------------------------------------------------------------------------
# Engine & basic config
# ---------------------------------------------------------------------------

# Reuse the unified engine (KSA-safe: no EODHD for .SR inside DataEngine)
_engine = DataEngine()

# Optional external KSA gateway URL (for future use / diagnostics)
ARGAAM_GATEWAY_URL: str = os.getenv("ARGAAM_GATEWAY_URL", "")


def _normalize_ksa_symbol(symbol: str) -> str:
    """Normalize symbol using the same rules as DataEngine."""
    # staticmethod from DataEngine (already used by main engine)
    return DataEngine._normalize_symbol(symbol)


def _ensure_ksa(symbol: str) -> str:
    """
    Ensure symbol is KSA (.SR / .TADAWUL or numeric) and normalize.

    - Accepts: "1120", "1120.SR", "TADAWUL:1120", "1120.TADAWUL"
    - Output:  "1120.SR"
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


# ---------------------------------------------------------------------------
# Request / Response models
# ---------------------------------------------------------------------------


class KSABatchRequest(BaseModel):
    tickers: List[str] = Field(
        default_factory=list,
        description="List of KSA symbols, e.g. ['1120.SR','1180.SR']",
    )


class KSASheetResponse(BaseModel):
    headers: List[str]
    rows: List[List[Any]]
    meta: Dict[str, Any] = Field(
        default_factory=dict,
        description="Lightweight metadata (requested, normalized, count, etc.)",
    )


# ---------------------------------------------------------------------------
# Health
# ---------------------------------------------------------------------------


@router.get("/health")
async def argaam_health() -> Dict[str, Any]:
    """
    Simple health check for KSA / Argaam gateway.

    This does not call any external provider – it just reports
    configuration and engine mode.
    """
    return {
        "status": "ok",
        "module": "routes_argaam",
        "engine": "DataEngine_v2",
        "enabled_providers": list(getattr(_engine, "enabled_providers", [])),
        "gateway": {
            "configured": bool(ARGAAM_GATEWAY_URL),
            "url_prefix": (ARGAAM_GATEWAY_URL or "")[:80] or None,
        },
        "notes": [
            "KSA (.SR) symbols are normalized and enforced here.",
            "DataEngine v2 internally delegates KSA to the v1 KSA engine "
            "and/or Tadawul/Argaam providers with NO EODHD for .SR.",
        ],
    }


# ---------------------------------------------------------------------------
# Core KSA quote endpoints (JSON)
# ---------------------------------------------------------------------------


@router.get("/quote", response_model=EnrichedQuoteResponse)
async def ksa_single_quote(symbol: str = Query(..., description="KSA symbol, e.g. 1120 or 1120.SR")) -> EnrichedQuoteResponse:
    """
    Single-symbol KSA quote, JSON format (EnrichedQuoteResponse).

    Example:
        GET /v1/argaam/quote?symbol=1120.SR
    """
    ksa_symbol = _ensure_ksa(symbol)

    try:
        q = await _engine.get_enriched_quote(ksa_symbol)
    except Exception as exc:
        logger.exception("KSA /argaam/quote engine failure for %s", ksa_symbol)
        return EnrichedQuoteResponse(
            symbol=ksa_symbol,
            data_quality="MISSING",
            data_source="ksa_gateway",
            error=f"KSA / Argaam gateway error: {exc}",
        )

    enriched = _quote_to_enriched(q)

    # Force KSA context & sensible defaults
    if not enriched.market:
        enriched.market = "KSA"
    if not enriched.data_source:
        enriched.data_source = "ksa_gateway"

    if enriched.data_quality == "MISSING" and not enriched.error:
        enriched.error = "No data available from KSA providers"

    return enriched


@router.post("/quotes", response_model=List[EnrichedQuoteResponse])
async def ksa_batch_quotes(body: KSABatchRequest) -> List[EnrichedQuoteResponse]:
    """
    Batch KSA quotes, JSON format (list of EnrichedQuoteResponse).

    Example body:
        {
          "tickers": ["1120.SR", "1180", "1050.SR"]
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
                "Skipping non-KSA symbol in /v1/argaam/quotes: %s (%s)", t, exc.detail
            )

    if not ksa_symbols:
        raise HTTPException(
            status_code=400,
            detail="No valid KSA (.SR) symbols found in request",
        )

    try:
        unified_quotes = await _engine.get_enriched_quotes(ksa_symbols)
    except Exception as exc:
        logger.exception("KSA /argaam/quotes engine failure for %s", ksa_symbols)
        # Return placeholders instead of 500
        results: List[EnrichedQuoteResponse] = []
        for sym in ksa_symbols:
            results.append(
                EnrichedQuoteResponse(
                    symbol=sym,
                    data_quality="MISSING",
                    data_source="ksa_gateway",
                    error=f"KSA / Argaam gateway error: {exc}",
                )
            )
        return results

    results: List[EnrichedQuoteResponse] = []
    for q in unified_quotes:
        try:
            enriched = _quote_to_enriched(q)

            # Force KSA context & provider label
            if not enriched.market:
                enriched.market = "KSA"
            if not enriched.data_source:
                enriched.data_source = "ksa_gateway"

            if enriched.data_quality == "MISSING" and not enriched.error:
                enriched.error = "No data available from KSA providers"

            results.append(enriched)
        except Exception as exc:
            logger.exception("Error mapping KSA quote in /quotes: %s", exc)
            sym = str(getattr(q, "symbol", "")).upper()
            results.append(
                EnrichedQuoteResponse(
                    symbol=sym,
                    data_quality="MISSING",
                    data_source="ksa_gateway",
                    error=f"Exception mapping KSA quote: {exc}",
                )
            )

    return results


# ---------------------------------------------------------------------------
# Google Sheets–friendly endpoint (headers + rows)
# ---------------------------------------------------------------------------


@router.post("/sheet-rows", response_model=KSASheetResponse)
async def ksa_sheet_rows(body: KSABatchRequest) -> KSASheetResponse:
    """
    Google Sheets–friendly endpoint for KSA tickers only.

    Returns:
        {
          "headers": [...],        # identical to /v1/enriched/sheet-rows
          "rows": [ [row for t1], [row for t2], ... ],
          "meta": {
              "requested": [...],
              "normalized": [...],
              "count": N,
              "ksa_only": true
          }
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
            logger.warning(
                "Skipping non-KSA symbol in /v1/argaam/sheet-rows: %s (%s)", t, exc.detail
            )

    if not ksa_symbols:
        raise HTTPException(
            status_code=400,
            detail="No valid KSA (.SR) symbols found in request",
        )

    headers = _enriched_headers()
    rows: List[List[Any]] = []

    try:
        unified_quotes = await _engine.get_enriched_quotes(ksa_symbols)
    except Exception as exc:
        logger.exception("KSA sheet-rows engine failure for %s", ksa_symbols)
        # Return placeholder rows with error instead of 500
        for sym in ksa_symbols:
            placeholder = EnrichedQuoteResponse(
                symbol=sym,
                market="KSA",
                data_quality="MISSING",
                data_source="ksa_gateway",
                error=f"KSA / Argaam gateway error: {exc}",
            )
            rows.append(_enriched_row(placeholder))

        meta = {
            "requested": list(tickers_raw),
            "normalized": list(ksa_symbols),
            "count": len(ksa_symbols),
            "ksa_only": True,
            "engine_error": str(exc),
        }
        return KSASheetResponse(headers=headers, rows=rows, meta=meta)

    for q in unified_quotes:
        try:
            enriched = _quote_to_enriched(q)

            # Force KSA context & provider label
            if not enriched.market:
                enriched.market = "KSA"
            if not enriched.data_source:
                enriched.data_source = "ksa_gateway"

            if enriched.data_quality == "MISSING" and not enriched.error:
                enriched.error = "No data available from KSA providers"

            rows.append(_enriched_row(enriched))
        except Exception as exc:
            logger.exception("Error mapping KSA quote to sheet row: %s", exc)
            placeholder = EnrichedQuoteResponse(
                symbol=str(getattr(q, "symbol", "")).upper(),
                market="KSA",
                data_quality="MISSING",
                data_source="ksa_gateway",
                error=f"Exception mapping KSA quote: {exc}",
            )
            rows.append(_enriched_row(placeholder))

    meta = {
        "requested": list(tickers_raw),
        "normalized": list(ksa_symbols),
        "count": len(ksa_symbols),
        "ksa_only": True,
    }

    return KSASheetResponse(headers=headers, rows=rows, meta=meta)
