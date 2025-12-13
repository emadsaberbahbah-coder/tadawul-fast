"""
routes_argaam.py
------------------------------------------------------------
KSA / Argaam Gateway – v2.4.0 (Aligned with Core V2)

Purpose:
  - Specialized router for KSA (.SR) market data.
  - Acts as a bridge to the Unified DataEngine (v2), ensuring KSA-specific 
    logic (Argaam scraping) is prioritized.
  - Provides /sheet-rows endpoint compatible with the main dashboard.

Key Improvements (v2.4):
  - Uses `core.enriched_quote` and `core.scoring_engine` for 100% schema parity.
  - KSA symbols get Value/Quality/Momentum scores just like Global symbols.
  - Strict input sanitization (numeric -> .SR).

Endpoints:
  - GET  /v1/argaam/health
  - GET  /v1/argaam/quote
  - POST /v1/argaam/quotes
  - POST /v1/argaam/sheet-rows
"""

from __future__ import annotations

import logging
import os
from functools import lru_cache
from typing import Any, Dict, List, Optional, Sequence, Union

from fastapi import APIRouter, HTTPException, Query, status
from pydantic import BaseModel, Field

# --- CORE IMPORTS ---
from core.data_engine_v2 import DataEngine, UnifiedQuote
from core.enriched_quote import EnrichedQuote
from core.scoring_engine import enrich_with_scores
from core.schemas import get_headers_for_sheet

logger = logging.getLogger("routes_argaam")

APP_VERSION = os.getenv("APP_VERSION", "4.0.0")

# ---------------------------------------------------------------------------
# Engine Singleton
# ---------------------------------------------------------------------------

@lru_cache(maxsize=1)
def _get_engine() -> DataEngine:
    logger.info("routes_argaam: Initializing DataEngine v2 singleton")
    return DataEngine()

# ---------------------------------------------------------------------------
# Symbol Logic (KSA Specific)
# ---------------------------------------------------------------------------

def _normalize_ksa_symbol(symbol: str) -> str:
    """
    Enforces KSA formatting:
    - 1120 -> 1120.SR
    - TADAWUL:1120 -> 1120.SR
    - 1120.TADAWUL -> 1120.SR
    - Returns "" if invalid/non-KSA.
    """
    s = (symbol or "").strip().upper()
    if not s:
        return ""
    
    if s.startswith("TADAWUL:"):
        s = s.split(":", 1)[1].strip()

    if s.endswith(".TADAWUL"):
        s = s.replace(".TADAWUL", "")

    if s.endswith(".SR"):
        base = s[:-3]
        if base.isdigit():
            return s
        return "" # invalid prefix

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
        if not x: continue
        if x in seen: continue
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
    """
    sym = _normalize_ksa_symbol(fallback_symbol) or (fallback_symbol or "").strip().upper() or "UNKNOWN"
    
    try:
        # 1. Convert to Enriched (Alias mapping)
        eq = EnrichedQuote.from_unified(uq)
        
        # 2. Add AI Scores (Value, Quality, Momentum)
        scored_eq = enrich_with_scores(eq)
        
        # 3. Force KSA metadata if missing (since this is the KSA router)
        if not scored_eq.market:
            # We use object.__setattr__ because Pydantic models might be frozen or validated
            # But EnrichedQuote is standard pydantic, setattr works if not frozen.
            # Using model_copy update is safer for V2.
            update_data = {"market": "KSA", "currency": "SAR"}
            if not scored_eq.data_source:
                update_data["data_source"] = "argaam_gateway"
            
            if hasattr(scored_eq, "model_copy"):
                scored_eq = scored_eq.model_copy(update=update_data)
            else:
                for k,v in update_data.items(): setattr(scored_eq, k, v)

        return scored_eq

    except Exception as exc:
        logger.exception("routes_argaam: conversion error for %s", sym, exc_info=exc)
        return EnrichedQuote(symbol=sym, data_quality="MISSING", error=f"Conversion error: {exc}")

# ---------------------------------------------------------------------------
# Router
# ---------------------------------------------------------------------------

router = APIRouter(prefix="/v1/argaam", tags=["KSA / Argaam"])

@router.get("/health")
async def argaam_health() -> Dict[str, Any]:
    engine = _get_engine()
    return {
        "status": "ok",
        "module": "routes_argaam",
        "version": APP_VERSION,
        "engine": "DataEngineV2",
        "providers": engine.enabled_providers if hasattr(engine, "enabled_providers") else "unknown"
    }

@router.get("/quote", response_model=EnrichedQuote)
async def ksa_single_quote(
    symbol: str = Query(..., description="KSA symbol (1120 or 1120.SR)")
) -> EnrichedQuote:
    ksa_symbol = _ensure_ksa(symbol)
    
    try:
        engine = _get_engine()
        uq = await engine.get_enriched_quote(ksa_symbol)
        return _to_enriched(uq, ksa_symbol)
    except Exception as exc:
        logger.error(f"routes_argaam: engine error for {ksa_symbol}: {exc}")
        return EnrichedQuote(symbol=ksa_symbol, data_quality="MISSING", error=str(exc))

@router.post("/quotes", response_model=List[EnrichedQuote])
async def ksa_batch_quotes(body: ArgaamBatchRequest) -> List[EnrichedQuote]:
    raw_list = body.symbols or body.tickers or []
    if not raw_list:
        raise HTTPException(status_code=400, detail="No symbols provided")

    # Normalize & Dedupe
    targets = _dedupe_preserve_order([_normalize_ksa_symbol(s) for s in raw_list if _normalize_ksa_symbol(s)])
    
    if not targets:
        # If input was valid strings but not KSA symbols (e.g. "AAPL"), return empty or error?
        # Returning empty list is safer for batch ops.
        return []

    engine = _get_engine()
    
    # Engine handles chunking internally if needed, but for massive lists we rely on
    # standard engine behavior.
    uqs = await engine.get_enriched_quotes(targets)
    
    results = []
    # Map back to preserve order (engine usually preserves, but let's be safe)
    # The V2 engine returns a list aligned with input if possible, but let's map by symbol.
    uq_map = {q.symbol.upper(): q for q in uqs}
    
    for t in targets:
        uq = uq_map.get(t) 
        if not uq:
            uq = UnifiedQuote(symbol=t, data_quality="MISSING", error="No data returned")
        results.append(_to_enriched(uq, t))
        
    return results

@router.post("/sheet-rows", response_model=KSASheetResponse)
async def ksa_sheet_rows(body: ArgaamBatchRequest) -> KSASheetResponse:
    """
    Returns headers + rows for Google Sheets.
    Forces KSA normalization on inputs.
    """
    raw_list = body.symbols or body.tickers or []
    sheet_name = body.sheet_name or "KSA_Tadawul_Market"
    
    # 1. Get Headers (Universal Schema)
    headers = get_headers_for_sheet(sheet_name)
    
    # 2. Normalize Inputs
    valid_targets = []
    skipped = []
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
            meta={"note": "No valid KSA symbols provided", "skipped": skipped}
        )

    # 3. Fetch Data
    engine = _get_engine()
    uqs = await engine.get_enriched_quotes(valid_targets)
    uq_map = {q.symbol.upper(): q for q in uqs}
    
    rows = []
    for t in valid_targets:
        uq = uq_map.get(t) or UnifiedQuote(symbol=t, data_quality="MISSING", error="No data")
        eq = _to_enriched(uq, t)
        
        # 4. Map to Row
        try:
            row = eq.to_row(headers)
            # Pad if needed
            if len(row) < len(headers):
                row += [None] * (len(headers) - len(row))
            rows.append(row)
        except Exception as e:
            logger.error(f"Row mapping failed for {t}: {e}")
            # Fallback row
            rows.append([t] + [None] * (len(headers) - 1))

    return KSASheetResponse(
        headers=headers,
        rows=rows,
        meta={
            "count": len(rows),
            "skipped_non_ksa": len(skipped),
            "sheet_name": sheet_name
        }
    )

__all__ = ["router"]
