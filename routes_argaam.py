"""
routes_argaam.py
------------------------------------------------------------
KSA / Argaam Gateway – v2.3 (DataEngine v2 wrapper, KSA-safe)

FIXES vs v2.2
-------------
- Align with routes/enriched_quote.py v3.4.x:
    * Remove imports of non-existent helpers (_quote_to_enriched/_build_sheet_headers/_enriched_to_sheet_row)
    * Use EnrichedQuoteResponse.from_unified(...) and .to_row(headers)
    * Use core.schemas.get_headers_for_sheet(...) for header selection (with safe fallback)
- Accept both request keys: "symbols" (preferred) or "tickers" (compat)
- Keep KSA-only enforcement: numeric / .SR / TADAWUL:#### / ####.TADAWUL
- Stay defensive: never crash; /sheet-rows returns headers even when empty.

ENDPOINTS
---------
    GET  /v1/argaam/health
    GET  /v1/argaam/quote?symbol=1120.SR
    POST /v1/argaam/quotes      { "symbols": ["1120.SR","1180", ...] }
    POST /v1/argaam/sheet-rows  { "symbols": ["1120.SR","1180", ...], "sheet_name": "KSA_Tadawul" }

SHEET CONTRACT
--------------
- /sheet-rows returns the SAME header names and row layout as /v1/enriched/sheet-rows
  by using the same schema header selector + EnrichedQuoteResponse.to_row(headers).
"""

from __future__ import annotations

import logging
import os
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Sequence

from fastapi import APIRouter, HTTPException, Query, status
from pydantic import BaseModel, Field

logger = logging.getLogger("routes_argaam")

# ---------------------------------------------------------------------------
# env.py integration – app name / version / gateway URL
# ---------------------------------------------------------------------------

APP_NAME: str = os.getenv("APP_NAME", "tadawul-fast-bridge")
APP_VERSION: str = os.getenv("APP_VERSION", "4.5.0")
ARGAAM_GATEWAY_URL: str = os.getenv("ARGAAM_GATEWAY_URL", "").strip()

try:  # pragma: no cover
    import env as _env_mod  # type: ignore

    APP_NAME = getattr(_env_mod, "APP_NAME", APP_NAME)
    APP_VERSION = getattr(_env_mod, "APP_VERSION", APP_VERSION)
    if getattr(_env_mod, "ARGAAM_GATEWAY_URL", None):
        ARGAAM_GATEWAY_URL = str(getattr(_env_mod, "ARGAAM_GATEWAY_URL")).strip()
    logger.info("routes_argaam: Config loaded from env.py.")
except Exception:  # pragma: no cover
    _env_mod = None  # type: ignore
    logger.warning(
        "routes_argaam: env.py not available or failed to import. Using environment variables."
    )

# ---------------------------------------------------------------------------
# Import enriched model (single source of truth for response + row mapping)
# ---------------------------------------------------------------------------

try:
    # EnrichedQuoteResponse is a backwards-compatible alias defined in routes/enriched_quote.py
    from routes.enriched_quote import EnrichedQuoteResponse  # type: ignore
except Exception as exc:  # pragma: no cover
    logger.exception("routes_argaam: Failed to import EnrichedQuoteResponse: %s", exc)

    class EnrichedQuoteResponse(BaseModel):  # type: ignore[no-redef]
        symbol: str
        market: Optional[str] = "KSA"
        currency: Optional[str] = "SAR"
        data_quality: str = "MISSING"
        data_source: Optional[str] = "ksa_gateway_stub"
        error: Optional[str] = None
        last_updated_utc: Optional[datetime] = None
        last_updated_riyadh: Optional[datetime] = None

        @classmethod
        def from_unified(cls, uq: Any) -> "EnrichedQuoteResponse":
            if isinstance(uq, dict):
                sym = str(uq.get("symbol") or uq.get("ticker") or "UNKNOWN").upper()
                return cls(
                    symbol=sym,
                    data_quality=uq.get("data_quality", "MISSING"),
                    error=uq.get("error"),
                )
            sym = str(getattr(uq, "symbol", "UNKNOWN") or "UNKNOWN").upper()
            return cls(
                symbol=sym,
                data_quality=str(getattr(uq, "data_quality", "MISSING") or "MISSING"),
                error=getattr(uq, "error", None),
            )

        def to_row(self, headers: Sequence[str]) -> List[Any]:
            hdrs = list(headers or [])
            if not hdrs:
                return [self.symbol]
            return [self.symbol] + [None] * (len(hdrs) - 1)

# ---------------------------------------------------------------------------
# Header helper (core.schemas) with robust fallback
# ---------------------------------------------------------------------------

def _fallback_headers() -> List[str]:
    return ["Symbol", "Data Quality", "Error"]

try:
    from core.schemas import get_headers_for_sheet as _get_headers_for_sheet  # type: ignore
except Exception as exc:  # pragma: no cover
    logger.warning("routes_argaam: core.schemas.get_headers_for_sheet not available (%s).", exc)

    def _get_headers_for_sheet(sheet_name: Optional[str]) -> List[str]:
        try:
            if hasattr(EnrichedQuoteResponse, "get_headers"):
                hdrs = EnrichedQuoteResponse.get_headers(sheet_name)  # type: ignore[attr-defined]
                if isinstance(hdrs, list) and hdrs:
                    return [str(x) for x in hdrs]
        except Exception:
            pass
        return _fallback_headers()

# ---------------------------------------------------------------------------
# DataEngine v2 import with stub fallback (never crash the app)
# ---------------------------------------------------------------------------

_ENGINE_MODE: str = "stub"
_ENGINE_IS_STUB: bool = True
_enabled_providers: List[str] = []

try:
    from core.data_engine_v2 import DataEngine as _DataEngineClass  # type: ignore
    _ENGINE_MODE = "v2"
except Exception as exc:  # pragma: no cover
    logger.exception("routes_argaam: Failed to import core.data_engine_v2.DataEngine: %s", exc)
    _DataEngineClass = None  # type: ignore
    _ENGINE_MODE = "stub"

class _StubEngine:
    enabled_providers: List[str] = []

    async def get_enriched_quote(self, symbol: str) -> Dict[str, Any]:
        sym = (symbol or "").strip().upper() or "UNKNOWN"
        now = datetime.now(timezone.utc)
        return {
            "symbol": sym,
            "market": "KSA",
            "exchange": "TADAWUL",
            "currency": "SAR",
            "data_quality": "MISSING",
            "data_source": "ksa_gateway_stub",
            "error": "DataEngine v2 unavailable in routes_argaam; using stub.",
            "last_updated_utc": now.isoformat(),
        }

    async def get_enriched_quotes(self, symbols: List[str]) -> List[Dict[str, Any]]:
        return [await self.get_enriched_quote(s) for s in (symbols or [])]

def _init_engine() -> Any:
    global _ENGINE_MODE, _ENGINE_IS_STUB, _enabled_providers
    if _ENGINE_MODE == "v2" and _DataEngineClass is not None:
        try:
            eng = _DataEngineClass()
            _enabled_providers = list(getattr(eng, "enabled_providers", []))
            _ENGINE_IS_STUB = False
            logger.info("routes_argaam: Using DataEngine v2 (enabled_providers=%s).", _enabled_providers)
            return eng
        except Exception as exc:  # pragma: no cover
            logger.exception("routes_argaam: Failed to init DataEngine v2 instance: %s", exc)
            _ENGINE_MODE = "stub"
            _ENGINE_IS_STUB = True
            return _StubEngine()
    _ENGINE_MODE = "stub"
    _ENGINE_IS_STUB = True
    logger.error("routes_argaam: DataEngine v2 not available – using stub MISSING engine.")
    return _StubEngine()

_engine = _init_engine()

# ---------------------------------------------------------------------------
# Router
# ---------------------------------------------------------------------------

router = APIRouter(prefix="/v1/argaam", tags=["KSA / Argaam"])

# ---------------------------------------------------------------------------
# Symbol normalization – KSA-only (.SR / numeric / TADAWUL:code)
# ---------------------------------------------------------------------------

def _normalize_ksa_symbol(symbol: str) -> str:
    s = (symbol or "").strip().upper()
    if not s:
        return ""
    if s.startswith("TADAWUL:"):
        s = s.split(":", 1)[1].strip()

    if s.endswith(".TADAWUL"):
        s = s[: -len(".TADAWUL")].strip()

    if s.endswith(".SR"):
        base = s[: -len(".SR")].strip()
        return f"{base}.SR" if base.isdigit() else ""

    if s.isdigit():
        return f"{s}.SR"

    return ""

def _ensure_ksa(symbol: str) -> str:
    s = _normalize_ksa_symbol(symbol)
    if not s:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="KSA symbol is required (numeric or .SR)",
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
# Request / Response models
# ---------------------------------------------------------------------------

class ArgaamBatchRequest(BaseModel):
    symbols: List[str] = Field(default_factory=list, description="KSA symbols (numeric/.SR/TADAWUL:####)")
    tickers: Optional[List[str]] = Field(default=None, description="Compatibility alias for symbols")
    sheet_name: Optional[str] = Field(default=None, description="Optional sheet name to pick schema headers")

class KSASheetResponse(BaseModel):
    headers: List[str]
    rows: List[List[Any]]
    meta: Dict[str, Any] = Field(default_factory=dict)

def _resolve_symbols(body: ArgaamBatchRequest) -> List[str]:
    return list(body.symbols or []) if body.symbols else list(body.tickers or [])

def _to_enriched_safe(uq: Any, fallback_symbol: str) -> EnrichedQuoteResponse:
    sym = _normalize_ksa_symbol(fallback_symbol) or (fallback_symbol or "").strip().upper() or "UNKNOWN"
    try:
        eq = EnrichedQuoteResponse.from_unified(uq)  # type: ignore[attr-defined]
        try:
            if not getattr(eq, "symbol", None):
                setattr(eq, "symbol", sym)
        except Exception:
            pass
        return eq
    except Exception as exc:
        logger.exception("routes_argaam: conversion failed for %s", sym, exc_info=exc)
        try:
            return EnrichedQuoteResponse(
                symbol=sym,
                market="KSA",  # type: ignore[arg-type]
                data_quality="MISSING",
                data_source="ksa_gateway",
                error=f"Conversion error: {exc}",
            )
        except Exception:
            return EnrichedQuoteResponse.from_unified(  # type: ignore[attr-defined]
                {"symbol": sym, "data_quality": "MISSING", "error": f"Conversion error: {exc}"}
            )

def _row_safe(enriched: EnrichedQuoteResponse, headers: List[str]) -> List[Any]:
    hdrs = list(headers or []) or _fallback_headers()
    try:
        row = enriched.to_row(hdrs)  # type: ignore[attr-defined]
        if not isinstance(row, list):
            raise ValueError("to_row did not return list")
        if len(row) < len(hdrs):
            row = row + [None] * (len(hdrs) - len(row))
        elif len(row) > len(hdrs):
            row = row[: len(hdrs)]
        return row
    except Exception as exc:
        logger.exception("routes_argaam: to_row failed", exc_info=exc)
        sym = getattr(enriched, "symbol", None) or "UNKNOWN"
        return [sym] + [None] * (len(hdrs) - 1)

# ---------------------------------------------------------------------------
# Health
# ---------------------------------------------------------------------------

@router.get("/health")
async def argaam_health() -> Dict[str, Any]:
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
            "KSA-only router: accepts numeric/.SR/TADAWUL:#### and normalizes to ####.SR",
            "Uses DataEngine v2 (KSA-safe) when available; otherwise returns MISSING placeholders.",
            "Sheet rows aligned to /v1/enriched/sheet-rows via schema headers + EnrichedQuoteResponse.to_row().",
        ],
    }

# ---------------------------------------------------------------------------
# Core endpoints
# ---------------------------------------------------------------------------

@router.get("/quote", response_model=EnrichedQuoteResponse)
async def ksa_single_quote(
    symbol: str = Query(..., description="KSA symbol, e.g. 1120 or 1120.SR"),
) -> EnrichedQuoteResponse:
    ksa_symbol = _ensure_ksa(symbol)
    logger.info("routes_argaam: /quote requested for %s", ksa_symbol)

    try:
        uq = await _engine.get_enriched_quote(ksa_symbol)  # type: ignore[attr-defined]
    except Exception as exc:
        logger.exception("routes_argaam: Engine failure in /quote", exc_info=exc)
        return EnrichedQuoteResponse(
            symbol=ksa_symbol,
            market="KSA",  # type: ignore[arg-type]
            data_quality="MISSING",
            data_source="ksa_gateway",
            error=f"KSA gateway engine error: {exc}",
        )

    enriched = _to_enriched_safe(uq, ksa_symbol)

    try:
        if not getattr(enriched, "market", None):
            setattr(enriched, "market", "KSA")
        if not getattr(enriched, "currency", None):
            setattr(enriched, "currency", "SAR")
        if not getattr(enriched, "data_source", None):
            setattr(enriched, "data_source", "ksa_gateway")
    except Exception:
        pass

    if getattr(enriched, "data_quality", None) == "MISSING" and not getattr(enriched, "error", None):
        try:
            setattr(enriched, "error", "No data available from KSA providers")
        except Exception:
            pass

    return enriched

@router.post("/quotes", response_model=List[EnrichedQuoteResponse])
async def ksa_batch_quotes(body: ArgaamBatchRequest) -> List[EnrichedQuoteResponse]:
    raw = _resolve_symbols(body)
    if not raw:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="At least one symbol is required")

    normalized: List[str] = []
    for s in raw:
        try:
            normalized.append(_ensure_ksa(s))
        except HTTPException:
            continue
    normalized = _dedupe_preserve_order(normalized)

    if not normalized:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="No valid KSA (.SR) symbols found in request")

    logger.info("routes_argaam: /quotes for %d KSA symbols (raw=%d).", len(normalized), len(raw))

    try:
        uqs = await _engine.get_enriched_quotes(normalized)  # type: ignore[attr-defined]
    except Exception as exc:
        logger.exception("routes_argaam: Engine failure in /quotes", exc_info=exc)
        return [
            EnrichedQuoteResponse(
                symbol=sym,
                market="KSA",  # type: ignore[arg-type]
                data_quality="MISSING",
                data_source="ksa_gateway",
                error=f"KSA gateway engine error: {exc}",
            )
            for sym in normalized
        ]

    out: List[EnrichedQuoteResponse] = []
    for idx, sym in enumerate(normalized):
        uq = None
        try:
            uq = uqs[idx] if isinstance(uqs, list) and idx < len(uqs) else None
        except Exception:
            uq = None

        enriched = _to_enriched_safe(uq or {"symbol": sym, "data_quality": "MISSING", "error": "No data returned"}, sym)

        try:
            if not getattr(enriched, "market", None):
                setattr(enriched, "market", "KSA")
            if not getattr(enriched, "currency", None):
                setattr(enriched, "currency", "SAR")
            if not getattr(enriched, "data_source", None):
                setattr(enriched, "data_source", "ksa_gateway")
        except Exception:
            pass

        if getattr(enriched, "data_quality", None) == "MISSING" and not getattr(enriched, "error", None):
            try:
                setattr(enriched, "error", "No data available from KSA providers")
            except Exception:
                pass

        out.append(enriched)

    return out

# ---------------------------------------------------------------------------
# Sheets endpoint
# ---------------------------------------------------------------------------

@router.post("/sheet-rows", response_model=KSASheetResponse)
async def ksa_sheet_rows(body: ArgaamBatchRequest) -> KSASheetResponse:
    raw = _resolve_symbols(body)
    sheet_name = body.sheet_name or "KSA_Tadawul"

    headers = _get_headers_for_sheet(sheet_name) or _fallback_headers()
    rows: List[List[Any]] = []

    if not raw:
        logger.warning("routes_argaam: /sheet-rows called with empty symbols list.")
        return KSASheetResponse(
            headers=headers,
            rows=[],
            meta={
                "requested": [],
                "normalized": [],
                "count": 0,
                "ksa_only": True,
                "sheet_name": sheet_name,
                "note": "No symbols provided.",
            },
        )

    normalized: List[str] = []
    skipped: List[str] = []
    for s in raw:
        try:
            normalized.append(_ensure_ksa(s))
        except HTTPException:
            skipped.append(str(s))

    normalized = _dedupe_preserve_order(normalized)

    if not normalized:
        logger.warning("routes_argaam: /sheet-rows called with no valid KSA symbols.")
        return KSASheetResponse(
            headers=headers,
            rows=[],
            meta={
                "requested": list(raw),
                "normalized": [],
                "skipped": skipped,
                "count": 0,
                "ksa_only": True,
                "sheet_name": sheet_name,
                "note": "No valid KSA (.SR) symbols found.",
            },
        )

    logger.info("routes_argaam: /sheet-rows for %d KSA symbols (raw=%d).", len(normalized), len(raw))

    try:
        uqs = await _engine.get_enriched_quotes(normalized)  # type: ignore[attr-defined]
    except Exception as exc:
        logger.exception("routes_argaam: Engine failure in /sheet-rows", exc_info=exc)
        for sym in normalized:
            placeholder = EnrichedQuoteResponse(
                symbol=sym,
                market="KSA",  # type: ignore[arg-type]
                data_quality="MISSING",
                data_source="ksa_gateway",
                error=f"KSA gateway engine error: {exc}",
            )
            rows.append(_row_safe(placeholder, headers))
        return KSASheetResponse(
            headers=headers,
            rows=rows,
            meta={
                "requested": list(raw),
                "normalized": list(normalized),
                "skipped": skipped,
                "count": len(normalized),
                "ksa_only": True,
                "sheet_name": sheet_name,
                "engine_error": str(exc),
            },
        )

    for idx, sym in enumerate(normalized):
        uq = None
        try:
            uq = uqs[idx] if isinstance(uqs, list) and idx < len(uqs) else None
        except Exception:
            uq = None

        enriched = _to_enriched_safe(
            uq or {"symbol": sym, "data_quality": "MISSING", "error": "No data returned from engine"},
            sym,
        )

        try:
            if not getattr(enriched, "market", None):
                setattr(enriched, "market", "KSA")
            if not getattr(enriched, "currency", None):
                setattr(enriched, "currency", "SAR")
            if not getattr(enriched, "data_source", None):
                setattr(enriched, "data_source", "ksa_gateway")
        except Exception:
            pass

        if getattr(enriched, "data_quality", None) == "MISSING" and not getattr(enriched, "error", None):
            try:
                setattr(enriched, "error", "No data available from KSA providers")
            except Exception:
                pass

        rows.append(_row_safe(enriched, headers))

    return KSASheetResponse(
        headers=headers,
        rows=rows,
        meta={
            "requested": list(raw),
            "normalized": list(normalized),
            "skipped": skipped,
            "count": len(normalized),
            "ksa_only": True,
            "sheet_name": sheet_name,
        },
    )

__all__ = ["router"]
