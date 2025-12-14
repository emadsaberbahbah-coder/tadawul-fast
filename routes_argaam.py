"""
routes_argaam.py
------------------------------------------------------------
KSA / Argaam Gateway – v2.5.0 (Engine v2 aligned, Sheets-safe)

Purpose
- KSA-only router: accepts numeric or .SR symbols only.
- Delegates ALL fetching to core.data_engine_v2.DataEngine (no direct provider calls).
- Returns EnrichedQuote + Scores for parity with Global routes.
- Provides /sheet-rows (headers + rows) for Google Sheets.

Notes
- Extremely defensive: never raises 502 for Sheets usage.
- If core.schemas.get_headers_for_sheet is missing, falls back to a safe minimal schema.
"""

from __future__ import annotations

import logging
from functools import lru_cache
from typing import Any, Dict, List, Optional, Sequence

from fastapi import APIRouter, HTTPException, Query, status
from pydantic import BaseModel, Field

from env import settings
from core.data_engine_v2 import DataEngine, UnifiedQuote
from core.enriched_quote import EnrichedQuote
from core.scoring_engine import enrich_with_scores

logger = logging.getLogger("routes.argaam")
ROUTE_VERSION = "2.5.0"

# ---------------------------------------------------------------------------
# Optional schema helper (preferred)
# ---------------------------------------------------------------------------
try:
    from core.schemas import get_headers_for_sheet  # type: ignore
except Exception:  # pragma: no cover
    def get_headers_for_sheet(sheet_name: str) -> List[str]:
        # Minimal safe fallback (won't crash EnrichedQuote.to_row if it expects just Symbol first)
        return ["Symbol", "Data Quality", "Last Price", "Change %", "Market Cap", "P/E (TTM)", "P/B", "Dividend Yield %", "ROE %", "Opportunity Score", "Recommendation", "Data Source", "Last Updated (UTC)", "Error"]


# ---------------------------------------------------------------------------
# Engine singleton
# ---------------------------------------------------------------------------
@lru_cache(maxsize=1)
def _get_engine() -> DataEngine:
    logger.info("routes_argaam: Initializing DataEngine v2 singleton")
    return DataEngine()


# ---------------------------------------------------------------------------
# Symbol logic (KSA strict)
# ---------------------------------------------------------------------------
def _normalize_ksa_symbol(symbol: str) -> str:
    """
    Enforces KSA formatting:
    - 1120 -> 1120.SR
    - TADAWUL:1120 -> 1120.SR
    - 1120.TADAWUL -> 1120.SR
    - 1120.SR -> 1120.SR
    Returns "" if invalid.
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
        return s if base.isdigit() else ""

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
# Transform
# ---------------------------------------------------------------------------
def _to_scored_enriched(uq: UnifiedQuote, fallback_symbol: str) -> EnrichedQuote:
    """
    UnifiedQuote -> EnrichedQuote -> scores (Value/Quality/Momentum/Opportunity/Overall/Recommendation)
    """
    sym = _normalize_ksa_symbol(fallback_symbol) or (fallback_symbol or "").strip().upper() or "UNKNOWN"
    try:
        eq = EnrichedQuote.from_unified(uq)
        eq = enrich_with_scores(eq)

        # Force KSA metadata for this gateway
        update = {}
        if not getattr(eq, "market", None):
            update["market"] = "KSA"
        if not getattr(eq, "currency", None):
            update["currency"] = "SAR"
        if not getattr(eq, "data_source", None):
            update["data_source"] = getattr(uq, "data_source", None) or "argaam_gateway"

        if update:
            if hasattr(eq, "model_copy"):
                eq = eq.model_copy(update=update)
            else:
                eq = eq.copy(update=update)

        # Ensure symbol is correct
        if not getattr(eq, "symbol", None):
            if hasattr(eq, "model_copy"):
                eq = eq.model_copy(update={"symbol": sym})
            else:
                eq = eq.copy(update={"symbol": sym})

        return eq
    except Exception as exc:
        logger.exception("routes_argaam: transform error for %s", sym)
        return EnrichedQuote(symbol=sym, data_quality="MISSING", error=f"Transform error: {exc}")


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
        "version": ROUTE_VERSION,
        "app_version": settings.app_version,
        "engine": "DataEngineV2",
        "providers": getattr(engine, "enabled_providers", settings.enabled_providers),
        "ksa_mode": "STRICT",
    }


@router.get("/quote", response_model=EnrichedQuote)
async def ksa_single_quote(symbol: str = Query(..., description="KSA symbol (1120 or 1120.SR)")) -> EnrichedQuote:
    ksa_symbol = _ensure_ksa(symbol)
    try:
        engine = _get_engine()
        uq = await engine.get_enriched_quote(ksa_symbol)
        return _to_scored_enriched(uq, ksa_symbol)
    except Exception as exc:
        logger.exception("routes_argaam: engine error for %s", ksa_symbol)
        return EnrichedQuote(symbol=ksa_symbol, data_quality="MISSING", error=str(exc))


@router.post("/quotes", response_model=List[EnrichedQuote])
async def ksa_batch_quotes(body: ArgaamBatchRequest) -> List[EnrichedQuote]:
    raw_list = body.symbols or body.tickers or []
    if not raw_list:
        raise HTTPException(status_code=400, detail="No symbols provided")

    targets = _dedupe_preserve_order([_normalize_ksa_symbol(s) for s in raw_list if _normalize_ksa_symbol(s)])
    if not targets:
        return []

    engine = _get_engine()
    try:
        uqs = await engine.get_enriched_quotes(targets)
    except Exception as exc:
        logger.exception("routes_argaam: batch engine error")
        return [EnrichedQuote(symbol=t, data_quality="MISSING", error=str(exc)) for t in targets]

    uq_map = {q.symbol.upper(): q for q in (uqs or [])}
    out: List[EnrichedQuote] = []
    for t in targets:
        uq = uq_map.get(t.upper()) or UnifiedQuote(symbol=t, data_quality="MISSING", error="No data returned")
        out.append(_to_scored_enriched(uq, t))
    return out


@router.post("/sheet-rows", response_model=KSASheetResponse)
async def ksa_sheet_rows(body: ArgaamBatchRequest) -> KSASheetResponse:
    """
    Returns headers + rows for Google Sheets.
    """
    raw_list = body.symbols or body.tickers or []
    sheet_name = body.sheet_name or settings.sheet_ksa_tadawul or "KSA_Tadawul_Market"

    headers = get_headers_for_sheet(sheet_name)

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
        return KSASheetResponse(headers=headers, rows=[], meta={"note": "No valid KSA symbols provided", "skipped": skipped})

    engine = _get_engine()
    try:
        uqs = await engine.get_enriched_quotes(valid_targets)
        uq_map = {q.symbol.upper(): q for q in (uqs or [])}
    except Exception as exc:
        logger.exception("routes_argaam: sheet-rows engine error")
        rows = [[t] + [None] * (len(headers) - 2) + [str(exc)] for t in valid_targets]
        return KSASheetResponse(headers=headers, rows=rows, meta={"count": len(rows), "sheet_name": sheet_name, "error": str(exc)})

    rows: List[List[Any]] = []
    for t in valid_targets:
        uq = uq_map.get(t.upper()) or UnifiedQuote(symbol=t, data_quality="MISSING", error="No data")
        eq = _to_scored_enriched(uq, t)
        try:
            row = eq.to_row(headers)  # type: ignore
            if len(row) < len(headers):
                row += [None] * (len(headers) - len(row))
            rows.append(row[: len(headers)])
        except Exception as exc:
            logger.error("routes_argaam: row mapping failed for %s: %s", t, exc)
            rows.append([t] + [None] * (len(headers) - 1))

    return KSASheetResponse(
        headers=headers,
        rows=rows,
        meta={"count": len(rows), "skipped_non_ksa": len(skipped), "sheet_name": sheet_name},
    )


__all__ = ["router"]
