"""
routes_argaam.py
------------------------------------------------------------
KSA / Argaam Gateway – v2.2 (DataEngine v2 wrapper, KSA-safe)

GOALS
- Accept ONLY Tadawul-style KSA symbols (.SR / numeric / TADAWUL:code).
- Never call EODHD or any non-KSA provider directly from here.
- Reuse the SAME enriched models & sheet-rows contract as:
      routes/enriched_quote.py  (/v1/enriched/*)
- Be extremely defensive:
      • If the engine is unavailable, return MISSING placeholder data
        instead of crashing.
      • For Google Sheets /sheet-rows, never raise 4xx for "no symbols" –
        always return a valid table (headers + empty rows).

ENDPOINTS
---------
    GET  /v1/argaam/health
    GET  /v1/argaam/quote?symbol=1120.SR
    POST /v1/argaam/quotes      { "symbols": ["1120.SR","1180", ...] }
    POST /v1/argaam/sheet-rows  { "symbols": ["1120.SR","1180", ...] }

SHEET CONTRACT
--------------
- /sheet-rows uses the SAME headers + row layout as /v1/enriched/sheet-rows
  so it can drop directly into the 9-page Google Sheets dashboard:
    • KSA_Tadawul
    • Global_Markets
    • Mutual_Funds
    • Commodities_FX
    • Insights_Analysis
"""

from __future__ import annotations

import logging
import os
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, HTTPException, Query, status
from pydantic import BaseModel, Field

logger = logging.getLogger("routes_argaam")

# ---------------------------------------------------------------------------
# env.py integration – app name / version / gateway URL
# ---------------------------------------------------------------------------

APP_NAME: str = os.getenv("APP_NAME", "tadawul-fast-bridge")
APP_VERSION: str = os.getenv("APP_VERSION", "4.4.0")
ARGAAM_GATEWAY_URL: str = os.getenv("ARGAAM_GATEWAY_URL", "").strip()

try:  # pragma: no cover - env.py is optional
    import env as _env_mod  # type: ignore

    APP_NAME = getattr(_env_mod, "APP_NAME", APP_NAME)
    APP_VERSION = getattr(_env_mod, "APP_VERSION", APP_VERSION)
    if getattr(_env_mod, "ARGAAM_GATEWAY_URL", None):
        ARGAAM_GATEWAY_URL = str(getattr(_env_mod, "ARGAAM_GATEWAY_URL")).strip()

    logger.info("routes_argaam: Config loaded from env.py.")
except Exception:  # pragma: no cover - defensive
    logger.warning(
        "routes_argaam: env.py not available or failed to import. "
        "Using environment variables for APP_NAME / APP_VERSION / ARGAAM_GATEWAY_URL."
    )
    _env_mod = None  # type: ignore

# ---------------------------------------------------------------------------
# DataEngine v2 import with stub fallback (never crash the app)
# ---------------------------------------------------------------------------

_ENGINE_MODE: str = "stub"
_ENGINE_IS_STUB: bool = True
_enabled_providers: List[str] = []

try:
    from core.data_engine_v2 import DataEngine as _DataEngineClass  # type: ignore

    _ENGINE_MODE = "v2"
except Exception as exc:  # pragma: no cover - defensive
    logger.exception(
        "routes_argaam: Failed to import core.data_engine_v2.DataEngine: %s",
        exc,
    )
    _DataEngineClass = None  # type: ignore
    _ENGINE_MODE = "stub"


class _StubEngine:
    """
    Stub engine used ONLY when DataEngine v2 is unavailable.

    Always returns MISSING placeholder data, but keeps all endpoints alive.
    """

    enabled_providers: List[str] = []

    async def get_enriched_quote(self, symbol: str) -> Dict[str, Any]:
        sym = (symbol or "").strip().upper()
        now = datetime.now(timezone.utc)
        return {
            "symbol": sym,
            "market": "KSA",
            "market_region": "KSA",
            "exchange": "TADAWUL",
            "currency": "SAR",
            "data_quality": "MISSING",
            "data_source": "ksa_gateway_stub",
            "error": "DataEngine v2 unavailable in routes_argaam; using stub.",
            "last_updated_utc": now,
        }

    async def get_enriched_quotes(self, symbols: List[str]) -> List[Dict[str, Any]]:
        return [await self.get_enriched_quote(s) for s in (symbols or [])]


if _ENGINE_MODE == "v2" and _DataEngineClass is not None:
    try:
        _engine = _DataEngineClass()
        _enabled_providers = list(getattr(_engine, "enabled_providers", []))
        _ENGINE_IS_STUB = False
        logger.info(
            "routes_argaam: Using DataEngine v2 (enabled_providers=%s).",
            _enabled_providers,
        )
    except Exception as exc:  # pragma: no cover - defensive
        logger.exception(
            "routes_argaam: Failed to init DataEngine v2 instance: %s", exc
        )
        _engine = _StubEngine()
        _ENGINE_MODE = "stub"
        _ENGINE_IS_STUB = True
else:
    _engine = _StubEngine()
    _ENGINE_MODE = "stub"
    _ENGINE_IS_STUB = True
    logger.error(
        "routes_argaam: DataEngine v2 not available – using stub MISSING engine."
    )

# ---------------------------------------------------------------------------
# Import enriched models & helpers (single source of truth)
# ---------------------------------------------------------------------------

from routes.enriched_quote import (  # type: ignore
    EnrichedQuoteResponse,
    _quote_to_enriched,
    _build_sheet_headers as _enriched_headers,
    _enriched_to_sheet_row as _enriched_row,
)

# ---------------------------------------------------------------------------
# Router
# ---------------------------------------------------------------------------

router = APIRouter(
    prefix="/v1/argaam",
    tags=["KSA / Argaam"],
)

# ---------------------------------------------------------------------------
# Symbol normalization – KSA-only (.SR / numeric / TADAWUL:code)
# ---------------------------------------------------------------------------


def _normalize_ksa_symbol(symbol: str) -> str:
    """
    Normalize symbol to Tadawul-style .SR.

    Accepts:
        - "1120"
        - "1120.SR"
        - "TADAWUL:1120"
        - "1120.TADAWUL"

    Returns:
        - "1120.SR"
        - or "" if cannot be interpreted as KSA symbol
    """
    s = (symbol or "").strip().upper()
    if not s:
        return ""

    if s.startswith("TADAWUL:"):
        s = s.split(":", 1)[1].strip()

    if s.endswith(".TADAWUL"):
        s = s[: -len(".TADAWUL")]

    if s.isdigit():
        s = f"{s}.SR"

    return s


def _is_ksa_symbol(symbol: str) -> bool:
    s = (symbol or "").strip().upper()
    return bool(s) and (s.endswith(".SR") or s.endswith(".TADAWUL") or s.isdigit())


def _ensure_ksa(symbol: str) -> str:
    """
    Ensure symbol is KSA (.SR / .TADAWUL / numeric) and normalize.

    - Accepts: "1120", "1120.SR", "TADAWUL:1120", "1120.TADAWUL"
    - Output:  "1120.SR"
    """
    s = _normalize_ksa_symbol(symbol)
    if not s:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Symbol is required",
        )

    if not _is_ksa_symbol(s):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="KSA / Argaam gateway only supports Tadawul (.SR) tickers",
        )

    # Ensure .SR for pure numeric AFTER validation
    if s.isdigit():
        s = f"{s}.SR"

    return s


# ---------------------------------------------------------------------------
# Request / Response models
# ---------------------------------------------------------------------------


class ArgaamBatchRequest(BaseModel):
    symbols: List[str] = Field(
        default_factory=list,
        description="List of KSA symbols, e.g. ['1120.SR','1180','TADAWUL:1120']",
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
    Lightweight health endpoint for the KSA / Argaam gateway.

    Does NOT call any external providers – just reports configuration
    + engine mode, for use by /v1/status and external monitors.
    """
    return {
        "status": "ok",
        "module": "routes_argaam",
        "app": APP_NAME,
        "version": APP_VERSION,
        "engine_mode": _ENGINE_MODE,
        "engine_is_stub": _ENGINE_IS_STUB,
        "enabled_providers": _enabled_providers,
        "gateway": {
            "configured": bool(ARGAAM_GATEWAY_URL),
            "url_prefix": (ARGAAM_GATEWAY_URL or "")[:80] or None,
        },
        "notes": [
            "KSA (.SR) symbols are normalized and enforced here.",
            "DataEngine v2 internally delegates KSA to KSA-safe providers "
            "(Tadawul / Argaam). No direct EODHD calls for .SR inside this router.",
        ],
    }


# ---------------------------------------------------------------------------
# Core KSA quote endpoints (JSON – EnrichedQuoteResponse)
# ---------------------------------------------------------------------------


@router.get("/quote", response_model=EnrichedQuoteResponse)
async def ksa_single_quote(
    symbol: str = Query(..., description="KSA symbol, e.g. 1120 or 1120.SR"),
) -> EnrichedQuoteResponse:
    """
    Single-symbol KSA quote, JSON format (EnrichedQuoteResponse).

    Example:
        GET /v1/argaam/quote?symbol=1120.SR
        GET /v1/argaam/quote?symbol=1120
    """
    ksa_symbol = _ensure_ksa(symbol)
    logger.info("routes_argaam: /quote requested for %s", ksa_symbol)

    try:
        q = await _engine.get_enriched_quote(ksa_symbol)  # type: ignore[attr-defined]
    except Exception as exc:  # pragma: no cover - defensive
        logger.exception(
            "routes_argaam: DataEngine failure in /quote for %s", ksa_symbol
        )
        return EnrichedQuoteResponse(
            symbol=ksa_symbol,
            market="KSA",
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
async def ksa_batch_quotes(body: ArgaamBatchRequest) -> List[EnrichedQuoteResponse]:
    """
    Batch KSA quotes, JSON format (list of EnrichedQuoteResponse).

    Example body:
        {
          "symbols": ["1120.SR", "1180", "1050.SR"]
        }
    """
    symbols_raw = body.symbols or []
    if not symbols_raw:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="At least one symbol is required",
        )

    # Normalize, validate as KSA, and de-duplicate (preserve order)
    seen: set[str] = set()
    ksa_symbols: List[str] = []
    for s in symbols_raw:
        try:
            sym = _ensure_ksa(s)
        except HTTPException as exc:
            logger.warning(
                "routes_argaam: Skipping non-KSA symbol in /quotes: %s (%s)",
                s,
                exc.detail,
            )
            continue
        if sym not in seen:
            seen.add(sym)
            ksa_symbols.append(sym)

    if not ksa_symbols:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="No valid KSA (.SR) symbols found in request",
        )

    logger.info(
        "routes_argaam: /quotes for %d KSA symbols (raw=%d).",
        len(ksa_symbols),
        len(symbols_raw),
    )

    try:
        unified_quotes = await _engine.get_enriched_quotes(ksa_symbols)  # type: ignore[attr-defined]
    except Exception as exc:  # pragma: no cover - defensive
        logger.exception(
            "routes_argaam: DataEngine failure in /quotes for %s", ksa_symbols
        )
        # Return placeholders instead of 500
        results: List[EnrichedQuoteResponse] = []
        for sym in ksa_symbols:
            results.append(
                EnrichedQuoteResponse(
                    symbol=sym,
                    market="KSA",
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

            if not enriched.market:
                enriched.market = "KSA"
            if not enriched.data_source:
                enriched.data_source = "ksa_gateway"

            if enriched.data_quality == "MISSING" and not enriched.error:
                enriched.error = "No data available from KSA providers"

            results.append(enriched)
        except Exception as exc:  # pragma: no cover - defensive
            logger.exception("routes_argaam: Error mapping KSA quote in /quotes: %s", exc)
            sym = str(getattr(q, "symbol", "")).upper()
            results.append(
                EnrichedQuoteResponse(
                    symbol=sym,
                    market="KSA",
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
async def ksa_sheet_rows(body: ArgaamBatchRequest) -> KSASheetResponse:
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

    DESIGN:
    - Never raise 4xx for "no valid symbols" – always return a valid table.
    - This makes Apps Script & Sheets refresh flows robust.
    """
    symbols_raw = body.symbols or []

    headers = _enriched_headers()
    rows: List[List[Any]] = []

    if not symbols_raw:
        logger.warning("routes_argaam: /sheet-rows called with empty symbols list.")
        meta = {
            "requested": [],
            "normalized": [],
            "count": 0,
            "ksa_only": True,
            "note": "No symbols provided.",
        }
        return KSASheetResponse(headers=headers, rows=rows, meta=meta)

    # Normalize, validate as KSA, and de-duplicate (preserve order)
    seen: set[str] = set()
    ksa_symbols: List[str] = []
    for s in symbols_raw:
        try:
            sym = _ensure_ksa(s)
        except HTTPException as exc:
            logger.warning(
                "routes_argaam: Skipping non-KSA symbol in /sheet-rows: %s (%s)",
                s,
                exc.detail,
            )
            continue
        if sym not in seen:
            seen.add(sym)
            ksa_symbols.append(sym)

    if not ksa_symbols:
        logger.warning(
            "routes_argaam: /sheet-rows called with no valid KSA symbols. raw=%s",
            symbols_raw,
        )
        meta = {
            "requested": list(symbols_raw),
            "normalized": [],
            "count": 0,
            "ksa_only": True,
            "note": "No valid KSA (.SR) symbols found.",
        }
        return KSASheetResponse(headers=headers, rows=rows, meta=meta)

    logger.info(
        "routes_argaam: /sheet-rows for %d KSA symbols (raw=%d).",
        len(ksa_symbols),
        len(symbols_raw),
    )

    try:
        unified_quotes = await _engine.get_enriched_quotes(ksa_symbols)  # type: ignore[attr-defined]
    except Exception as exc:  # pragma: no cover - defensive
        logger.exception(
            "routes_argaam: DataEngine failure in /sheet-rows for %s", ksa_symbols
        )
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
            "requested": list(symbols_raw),
            "normalized": list(ksa_symbols),
            "count": len(ksa_symbols),
            "ksa_only": True,
            "engine_error": str(exc),
        }
        return KSASheetResponse(headers=headers, rows=rows, meta=meta)

    for q in unified_quotes:
        try:
            enriched = _quote_to_enriched(q)

            if not enriched.market:
                enriched.market = "KSA"
            if not enriched.data_source:
                enriched.data_source = "ksa_gateway"

            if enriched.data_quality == "MISSING" and not enriched.error:
                enriched.error = "No data available from KSA providers"

            rows.append(_enriched_row(enriched))
        except Exception as exc:  # pragma: no cover - defensive
            logger.exception(
                "routes_argaam: Error mapping KSA quote to sheet row: %s", exc
            )
            placeholder = EnrichedQuoteResponse(
                symbol=str(getattr(q, "symbol", "")).upper(),
                market="KSA",
                data_quality="MISSING",
                data_source="ksa_gateway",
                error=f"Exception mapping KSA quote: {exc}",
            )
            rows.append(_enriched_row(placeholder))

    meta = {
        "requested": list(symbols_raw),
        "normalized": list(ksa_symbols),
        "count": len(ksa_symbols),
        "ksa_only": True,
    }

    return KSASheetResponse(headers=headers, rows=rows, meta=meta)


__all__ = ["router"]
