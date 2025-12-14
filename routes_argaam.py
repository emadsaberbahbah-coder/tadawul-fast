"""
routes_argaam.py
------------------------------------------------------------
KSA / Argaam Gateway – v2.6.0 (Aligned with Core DataEngine v2)

Purpose
- KSA-only router that normalizes Tadawul symbols (1120 / 1120.SR).
- Uses DataEngine.get_quote / get_quotes (the methods that actually exist).
- Returns EnrichedQuote (UnifiedQuote + sheet mapping + scoring enrichment).
- Provides /sheet-rows for Google Sheets (headers + rows).

Key Fixes vs old file
- Removed calls to non-existent engine methods: get_enriched_quote(s).
- Engine resolution is safe:
    1) Use FastAPI app.state.engine if available (created in main lifespan)
    2) Fallback to a local singleton (for standalone router usage)
- Stronger input validation + batch limits.
- More defensive meta/health output (no reliance on engine.enabled_providers).
"""

from __future__ import annotations

import logging
import os
from functools import lru_cache
from typing import Any, Dict, List, Optional, Sequence

from fastapi import APIRouter, Depends, HTTPException, Query, Request, status
from pydantic import BaseModel, Field

from core.config import get_settings
from core.data_engine_v2 import DataEngine, UnifiedQuote
from core.enriched_quote import EnrichedQuote
from core.scoring_engine import enrich_with_scores
from core.schemas import get_headers_for_sheet

logger = logging.getLogger("routes_argaam")

settings = get_settings()
APP_VERSION = os.getenv("APP_VERSION", getattr(settings, "APP_VERSION", "4.0.0"))

MAX_BATCH = int(os.getenv("ARGAAM_MAX_BATCH", "250"))


# ---------------------------------------------------------------------------
# Engine resolution
# ---------------------------------------------------------------------------

@lru_cache(maxsize=1)
def _engine_singleton() -> DataEngine:
    logger.info("routes_argaam: initializing local DataEngine singleton (fallback)")
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


# ---------------------------------------------------------------------------
# Symbol Logic (KSA Specific)
# ---------------------------------------------------------------------------

def _normalize_ksa_symbol(symbol: str) -> str:
    """
    Enforces KSA formatting:
    - 1120 -> 1120.SR
    - TADAWUL:1120 -> 1120.SR
    - 1120.TADAWUL -> 1120.SR
    - 1120.SR -> 1120.SR
    Returns "" if invalid/non-KSA.
    """
    s = (symbol or "").strip().upper()
    if not s:
        return ""

    # Common vendor prefixes/suffixes
    if s.startswith("TADAWUL:"):
        s = s.split(":", 1)[1].strip()

    if s.endswith(".TADAWUL"):
        s = s[:-7].strip()  # remove ".TADAWUL"

    # If already .SR, ensure numeric base
    if s.endswith(".SR"):
        base = s[:-3]
        return s if base.isdigit() else ""

    # If numeric only
    if s.isdigit():
        return f"{s}.SR"

    return ""

def _ensure_ksa(symbol: str) -> str:
    s = _normalize_ksa_symbol(symbol)
    if not s:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Invalid KSA symbol '{symbol}'. Must be numeric or end in .SR",
        )
    return s

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


# ---------------------------------------------------------------------------
# Models
# ---------------------------------------------------------------------------

class ArgaamBatchRequest(BaseModel):
    symbols: List[str] = Field(default_factory=list, description="KSA symbols (e.g. ['1120', '1180.SR'])")
    tickers: Optional[List[str]] = Field(default=None, description="Alias for symbols (legacy compat)")
    sheet_name: Optional[str] = Field(default=None, description="Sheet name for header selection")

class KSASheetResponse(BaseModel):
    headers: List[str]
    rows: List[List[Any]]
    meta: Dict[str, Any] = Field(default_factory=dict)


# ---------------------------------------------------------------------------
# Transformation Logic
# ---------------------------------------------------------------------------

def _to_enriched(uq: UnifiedQuote, fallback_symbol: str) -> EnrichedQuote:
    """
    Convert UnifiedQuote -> EnrichedQuote -> Scored EnrichedQuote.
    Ensures KSA metadata in this router.
    """
    sym = _normalize_ksa_symbol(fallback_symbol) or (fallback_symbol or "").strip().upper() or "UNKNOWN"

    try:
        eq = EnrichedQuote.from_unified(uq)
        eq = enrich_with_scores(eq)

        # Force KSA metadata (router contract)
        update_data: Dict[str, Any] = {}
        if not getattr(eq, "market", None) or getattr(eq, "market", "").upper() == "UNKNOWN":
            update_data["market"] = "KSA"
        if not getattr(eq, "currency", None):
            update_data["currency"] = "SAR"
        if not getattr(eq, "data_source", None) or getattr(eq, "data_source", "") in ("none", ""):
            update_data["data_source"] = "argaam_gateway"

        if update_data and hasattr(eq, "model_copy"):
            eq = eq.model_copy(update=update_data)

        # Always ensure symbol is the normalized one
        if getattr(eq, "symbol", None) != sym and hasattr(eq, "model_copy"):
            eq = eq.model_copy(update={"symbol": sym})

        return eq

    except Exception as exc:
        logger.exception("routes_argaam: conversion error for %s", sym, exc_info=exc)
        return EnrichedQuote(
            symbol=sym,
            market="KSA",
            currency="SAR",
            data_source="argaam_gateway",
            data_quality="MISSING",
            error=f"Conversion error: {exc}",
        )


# ---------------------------------------------------------------------------
# Router
# ---------------------------------------------------------------------------

router = APIRouter(prefix="/v1/argaam", tags=["KSA / Argaam"])

@router.get("/health")
async def argaam_health() -> Dict[str, Any]:
    return {
        "status": "ok",
        "module": "routes_argaam",
        "version": APP_VERSION,
        "engine": "DataEngineV2",
        "providers": {
            "argaam_scrape": True,
            "eodhd_enabled": bool(getattr(settings, "EODHD_API_KEY", None)),
            "fmp_enabled": bool(getattr(settings, "FMP_API_KEY", None)),
            "yfinance_enabled": bool(getattr(settings, "ENABLE_YFINANCE", False)),
        },
        "limits": {"max_batch": MAX_BATCH},
    }

@router.get("/quote", response_model=EnrichedQuote)
async def ksa_single_quote(
    request: Request,
    symbol: str = Query(..., description="KSA symbol (1120 or 1120.SR)"),
    engine: DataEngine = Depends(get_engine),
) -> EnrichedQuote:
    ksa_symbol = _ensure_ksa(symbol)

    try:
        uq = await engine.get_quote(ksa_symbol)
        return _to_enriched(uq, ksa_symbol)
    except Exception as exc:
        logger.exception("routes_argaam: engine error for %s", ksa_symbol, exc_info=exc)
        return EnrichedQuote(
            symbol=ksa_symbol,
            market="KSA",
            currency="SAR",
            data_source="argaam_gateway",
            data_quality="MISSING",
            error=str(exc),
        )

@router.post("/quotes", response_model=List[EnrichedQuote])
async def ksa_batch_quotes(
    request: Request,
    body: ArgaamBatchRequest,
    engine: DataEngine = Depends(get_engine),
) -> List[EnrichedQuote]:
    raw_list = body.symbols or body.tickers or []
    if not raw_list:
        raise HTTPException(status_code=400, detail="No symbols provided")

    # Normalize + dedupe (KSA only)
    targets: List[str] = []
    skipped: List[str] = []
    for s in raw_list:
        norm = _normalize_ksa_symbol(s)
        if norm:
            targets.append(norm)
        else:
            skipped.append(s)

    targets = _dedupe_preserve_order(targets)

    if not targets:
        return []

    if len(targets) > MAX_BATCH:
        raise HTTPException(
            status_code=413,
            detail=f"Too many symbols. Max {MAX_BATCH}. Provided {len(targets)}.",
        )

    uqs = await engine.get_quotes(targets)
    uq_map = {q.symbol.upper(): q for q in uqs}

    results: List[EnrichedQuote] = []
    for t in targets:
        uq = uq_map.get(t.upper()) or UnifiedQuote(symbol=t, data_quality="MISSING", error="No data returned")
        results.append(_to_enriched(uq, t))

    # Note: skipped are not returned here to keep response_model clean
    return results

@router.post("/sheet-rows", response_model=KSASheetResponse)
async def ksa_sheet_rows(
    request: Request,
    body: ArgaamBatchRequest,
    engine: DataEngine = Depends(get_engine),
) -> KSASheetResponse:
    """
    Returns headers + rows for Google Sheets.
    Forces KSA normalization on inputs.
    """
    raw_list = body.symbols or body.tickers or []
    sheet_name = body.sheet_name or "KSA_Tadawul_Market"

    headers = get_headers_for_sheet(sheet_name)

    # Normalize inputs
    valid_targets: List[str] = []
    skipped: List[str] = []
    for s in raw_list:
        norm = _normalize_ksa_symbol(s)
        if norm:
            valid_targets.append(norm)
        else:
            skipped.append(s)

    valid_targets = _dedupe_preserve_order(valid_targets)

    if not valid_targets:
        return KSASheetResponse(
            headers=headers,
            rows=[],
            meta={"note": "No valid KSA symbols provided", "skipped": skipped, "sheet_name": sheet_name},
        )

    if len(valid_targets) > MAX_BATCH:
        raise HTTPException(
            status_code=413,
            detail=f"Too many symbols. Max {MAX_BATCH}. Provided {len(valid_targets)}.",
        )

    # Fetch data
    uqs = await engine.get_quotes(valid_targets)
    uq_map = {q.symbol.upper(): q for q in uqs}

    rows: List[List[Any]] = []
    row_errors: List[str] = []

    for t in valid_targets:
        uq = uq_map.get(t.upper()) or UnifiedQuote(symbol=t, data_quality="MISSING", error="No data")
        eq = _to_enriched(uq, t)

        try:
            row = eq.to_row(headers)
            if len(row) < len(headers):
                row += [None] * (len(headers) - len(row))
            elif len(row) > len(headers):
                row = row[: len(headers)]
            rows.append(row)
        except Exception as e:
            logger.exception("routes_argaam: row mapping failed for %s", t, exc_info=e)
            row_errors.append(f"{t}: {e}")
            rows.append([t] + [None] * (len(headers) - 1))

    return KSASheetResponse(
        headers=headers,
        rows=rows,
        meta={
            "count": len(rows),
            "skipped_non_ksa": len(skipped),
            "skipped": skipped[:50],  # keep response small
            "sheet_name": sheet_name,
            "row_errors": row_errors[:50],
        },
    )

__all__ = ["router"]
