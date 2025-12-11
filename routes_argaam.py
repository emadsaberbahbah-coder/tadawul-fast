"""
routes_argaam.py
------------------------------------------------------------
KSA / Argaam Gateway – v2.1 (DataEngineV2 wrapper, KSA-only)

GOALS
- Accept ONLY Tadawul-style KSA symbols (.SR / numeric / TADAWUL:code).
- Never call EODHD or any non-KSA provider directly from here.
- Act as the dedicated KSA gateway for /v1/enriched/_fetch_ksa_via_argaam.
- Stay defensive: if engine fails, return MISSING payloads (no 500s).

ENDPOINTS
---------
    GET  /v1/argaam/health
    GET  /v1/argaam/quote?symbol=1120.SR
    POST /v1/argaam/quotes      { "symbols": ["1120.SR","1180", ...] }
    POST /v1/argaam/sheet-rows  { "symbols": ["1120.SR","1180", ...] }

SHEET CONTRACT
--------------
- /sheet-rows uses the SAME header/row layout as the dashboard
  (via DataEngineV2.get_sheet_rows for sheet_name="KSA_Tadawul").
"""

from __future__ import annotations

import logging
from datetime import datetime
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, HTTPException, Query, status
from pydantic import BaseModel, Field

logger = logging.getLogger("routes.argaam")

# ---------------------------------------------------------------------------
# Optional env.py – for fine-tuning cache etc.
# ---------------------------------------------------------------------------

try:  # pragma: no cover
    import env as _env  # type: ignore
except Exception:  # pragma: no cover
    _env = None  # type: ignore

# ---------------------------------------------------------------------------
# DataEngineV2 import with stub fallback (never break the app)
# ---------------------------------------------------------------------------

_ENGINE_MODE: str = "stub"
_ENGINE_IS_STUB: bool = False
_engine: Any = None

try:
    # New unified engine (sync class)
    from core.data_engine_v2 import DataEngineV2 as _KSAEngine  # type: ignore

    kwargs: Dict[str, Any] = {}
    if _env is not None:
        cache_ttl = getattr(_env, "KSA_ENGINE_CACHE_TTL_SECONDS", None) or getattr(
            _env, "ENGINE_CACHE_TTL_SECONDS", None
        )
        if cache_ttl is not None:
            kwargs["cache_ttl_seconds"] = int(cache_ttl)

    _engine = _KSAEngine(**kwargs)
    _ENGINE_MODE = "v2"
    logger.info("routes_argaam: Using DataEngineV2 for KSA (.SR) symbols")

except Exception as exc:  # pragma: no cover - defensive
    logger.exception("routes_argaam: Failed to initialize DataEngineV2: %s", exc)

    class _StubKSAEngine:
        """
        Stub engine when DataEngineV2 is unavailable.

        Always returns MISSING placeholder data, but keeps endpoints alive.
        """

        def get_enriched_quote(
            self, symbol: str, sheet_name: Optional[str] = None
        ) -> Dict[str, Any]:
            sym = (symbol or "").strip().upper()
            now = datetime.utcnow()
            return {
                "symbol": sym,
                "market": "KSA",
                "market_region": "KSA",
                "exchange": "TADAWUL",
                "currency": "SAR",
                "timezone": "Asia/Riyadh",
                "as_of_utc": now,
                "as_of_local": now,
                "provider": "argaam_stub",
                "data_source": "argaam_stub",
                "data_quality": "MISSING",
                "error": "KSA engine (DataEngineV2) not available – using stub.",
            }

        def get_enriched_quotes(
            self, symbols: List[str], sheet_name: Optional[str] = None
        ) -> List[Dict[str, Any]]:
            return [self.get_enriched_quote(s, sheet_name=sheet_name) for s in symbols]

        def get_sheet_rows(
            self, symbols: List[str], sheet_name: Optional[str] = None
        ):
            # Minimal, but keeps Google Sheets safe
            headers = ["Symbol", "Company Name", "Data Quality", "Error"]
            rows: List[List[Any]] = []
            for s in symbols:
                q = self.get_enriched_quote(s, sheet_name=sheet_name)
                rows.append([q.get("symbol"), None, q.get("data_quality"), q.get("error")])
            return headers, rows

    _engine = _StubKSAEngine()
    _ENGINE_MODE = "stub"
    _ENGINE_IS_STUB = True
    logger.error("routes_argaam: Using STUB KSA engine (MISSING data).")

# ---------------------------------------------------------------------------
# FastAPI router
# ---------------------------------------------------------------------------

router = APIRouter(
    prefix="/v1/argaam",
    tags=["KSA / Argaam"],
)

# ---------------------------------------------------------------------------
# Request / response models
# ---------------------------------------------------------------------------


class ArgaamBatchRequest(BaseModel):
    symbols: List[str] = Field(
        default_factory=list,
        description="List of Tadawul codes (with or without .SR), e.g. ['1120','1180.SR']",
    )


class ArgaamBatchResponse(BaseModel):
    results: List[Dict[str, Any]]


class ArgaamSheetResponse(BaseModel):
    headers: List[str]
    rows: List[List[Any]]
    meta: Dict[str, Any] = Field(
        default_factory=dict,
        description="requested, normalized, count, ksa_only, etc.",
    )


# ---------------------------------------------------------------------------
# Symbol normalization – KSA-only
# ---------------------------------------------------------------------------


def _normalize_ksa_symbol(raw: str) -> str:
    """
    Accepts: "1120", "1120.SR", "TADAWUL:1120", "1120.TADAWUL"
    Returns: "1120.SR"
    """
    sym = (raw or "").strip().upper()
    if not sym:
        return ""

    if sym.startswith("TADAWUL:"):
        sym = sym.split(":", 1)[1].strip()

    if sym.endswith(".TADAWUL"):
        sym = sym[: -len(".TADAWUL")]

    if sym.isdigit():
        return f"{sym}.SR"

    if not sym.endswith(".SR"):
        return f"{sym}.SR"

    return sym


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


def _quote_to_argaam_dict(symbol_sr: str, engine_obj: Any) -> Dict[str, Any]:
    """
    Call DataEngineV2 for one KSA symbol and adapt to the shape that
    /v1/enriched/_fetch_ksa_via_argaam expects.
    """
    quote = engine_obj.get_enriched_quote(symbol_sr, sheet_name="KSA_Tadawul")

    # Accept both Pydantic model and dict
    if hasattr(quote, "model_dump"):
        data = quote.model_dump()
    elif isinstance(quote, dict):
        data = dict(quote)
    else:
        now = datetime.utcnow()
        return {
            "symbol": symbol_sr,
            "market": "KSA",
            "market_region": "KSA",
            "exchange": "TADAWUL",
            "currency": "SAR",
            "timezone": "Asia/Riyadh",
            "as_of_utc": now,
            "as_of_local": now,
            "provider": "argaam_engine_unsupported",
            "data_source": "argaam_engine_unsupported",
            "data_quality": "MISSING",
            "error": f"Unsupported engine quote type: {type(quote)!r}",
        }

    # Ensure basic identity
    data.setdefault("symbol", symbol_sr)

    # Map typical engine fields → Argaam-style aliases
    if "open" not in data and "open_price" in data:
        data["open"] = data.get("open_price")
    if "high" not in data and "high_price" in data:
        data["high"] = data.get("high_price")
    if "low" not in data and "low_price" in data:
        data["low"] = data.get("low_price")

    # KSA context
    data.setdefault("market", "KSA")
    data.setdefault("market_region", "KSA")
    data.setdefault("exchange", "TADAWUL")
    data.setdefault("currency", data.get("currency") or "SAR")
    data.setdefault("timezone", data.get("timezone") or "Asia/Riyadh")

    # Timestamps
    now = datetime.utcnow()
    data.setdefault("as_of_utc", data.get("last_updated_utc", now))
    data.setdefault("as_of_local", data.get("last_updated_local", now))

    # Provider / source / quality
    provider = data.get("provider") or data.get("primary_provider") or "yfinance"
    data.setdefault("provider", provider)
    data.setdefault("data_source", data.get("data_source") or provider)
    data.setdefault("data_quality", data.get("data_quality") or "UNKNOWN")

    return data


# ---------------------------------------------------------------------------
# Health
# ---------------------------------------------------------------------------


@router.get("/health")
async def argaam_health() -> Dict[str, Any]:
    """
    Lightweight health endpoint – no external calls.
    """
    return {
        "status": "ok",
        "module": "argaam",
        "version": "2.1",
        "engine_mode": _ENGINE_MODE,
        "engine_is_stub": _ENGINE_IS_STUB,
    }


# ---------------------------------------------------------------------------
# Core KSA quote endpoints
# ---------------------------------------------------------------------------


@router.get("/quote")
async def argaam_quote(
    symbol: str = Query(..., alias="symbol", description="KSA symbol, e.g. 1120 or 1120.SR"),
) -> Dict[str, Any]:
    """
    Single-symbol KSA quote (JSON dict).

    Used by /v1/enriched/_fetch_ksa_via_argaam for .SR tickers.
    """
    raw = (symbol or "").strip()
    if not raw:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST, detail="Symbol is required"
        )

    ticker_sr = _normalize_ksa_symbol(raw)
    if not ticker_sr:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Invalid KSA symbol format: {symbol!r}",
        )

    try:
        payload = _quote_to_argaam_dict(ticker_sr, _engine)
        return payload
    except HTTPException:
        raise
    except Exception as exc:  # pragma: no cover - defensive
        now = datetime.utcnow()
        logger.exception("routes_argaam: Exception while fetching quote for %s", symbol)
        return {
            "symbol": ticker_sr,
            "market": "KSA",
            "market_region": "KSA",
            "exchange": "TADAWUL",
            "currency": "SAR",
            "timezone": "Asia/Riyadh",
            "as_of_utc": now,
            "as_of_local": now,
            "provider": "argaam_error",
            "data_source": "argaam_error",
            "data_quality": "MISSING",
            "error": f"Exception in KSA Argaam quote: {exc}",
        }


@router.post("/quotes", response_model=ArgaamBatchResponse)
async def argaam_quotes(
    body: ArgaamBatchRequest,
) -> ArgaamBatchResponse:
    """
    Batch KSA quotes in JSON format (for debugging / bulk checks).
    """
    raw_symbols = [s.strip() for s in (body.symbols or []) if s and s.strip()]
    if not raw_symbols:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="At least one KSA Tadawul symbol is required",
        )

    results: List[Dict[str, Any]] = []

    for raw in raw_symbols:
        ticker_sr = _normalize_ksa_symbol(raw)
        if not ticker_sr:
            now = datetime.utcnow()
            results.append(
                {
                    "symbol": raw,
                    "market": "KSA",
                    "data_quality": "MISSING",
                    "error": f"Invalid KSA symbol format: {raw!r}",
                    "as_of_utc": now,
                    "as_of_local": now,
                }
            )
            continue

        try:
            payload = _quote_to_argaam_dict(ticker_sr, _engine)
            results.append(payload)
        except Exception as exc:  # pragma: no cover
            now = datetime.utcnow()
            logger.exception(
                "routes_argaam: Exception while fetching batch quote for %s", raw
            )
            results.append(
                {
                    "symbol": ticker_sr,
                    "market": "KSA",
                    "market_region": "KSA",
                    "exchange": "TADAWUL",
                    "currency": "SAR",
                    "timezone": "Asia/Riyadh",
                    "as_of_utc": now,
                    "as_of_local": now,
                    "provider": "argaam_error",
                    "data_source": "argaam_error",
                    "data_quality": "MISSING",
                    "error": f"Exception in KSA Argaam batch quote: {exc}",
                }
            )

    return ArgaamBatchResponse(results=results)


# ---------------------------------------------------------------------------
# Google Sheets–friendly endpoint (headers + rows)
# ---------------------------------------------------------------------------


@router.post("/sheet-rows", response_model=ArgaamSheetResponse)
async def argaam_sheet_rows(
    body: ArgaamBatchRequest,
) -> ArgaamSheetResponse:
    """
    KSA-only sheet rows for the dashboard.

    - Uses DataEngineV2.get_sheet_rows(sheet_name="KSA_Tadawul").
    - Never raises 4xx for "no valid symbols" – always returns a table.
    """
    raw_symbols = [s.strip() for s in (body.symbols or []) if s and s.strip()]
    headers: List[str]
    rows: List[List[Any]] = []

    # Empty request → safe empty table
    if not raw_symbols:
        try:
            headers, _ = _engine.get_sheet_rows([], sheet_name="KSA_Tadawul")
        except Exception:
            headers = ["Symbol", "Company Name"]
        meta = {
            "requested": [],
            "normalized": [],
            "count": 0,
            "ksa_only": True,
            "note": "No symbols provided.",
        }
        return ArgaamSheetResponse(headers=headers, rows=rows, meta=meta)

    # Normalize & de-duplicate
    seen: set[str] = set()
    ksa_symbols: List[str] = []
    for raw in raw_symbols:
        sym = _normalize_ksa_symbol(raw)
        if not sym:
            continue
        if sym not in seen:
            seen.add(sym)
            ksa_symbols.append(sym)

    if not ksa_symbols:
        try:
            headers, _ = _engine.get_sheet_rows([], sheet_name="KSA_Tadawul")
        except Exception:
            headers = ["Symbol", "Company Name"]
        meta = {
            "requested": list(raw_symbols),
            "normalized": [],
            "count": 0,
            "ksa_only": True,
            "note": "No valid KSA (.SR) symbols found.",
        }
        return ArgaamSheetResponse(headers=headers, rows=rows, meta=meta)

    # Main path – delegate to DataEngineV2
    try:
        headers, rows = _engine.get_sheet_rows(ksa_symbols, sheet_name="KSA_Tadawul")
    except Exception as exc:  # pragma: no cover - defensive
        logger.exception(
            "routes_argaam: Exception in get_sheet_rows for %s", ksa_symbols
        )
        headers = ["Symbol", "Company Name", "Error"]
        rows = [[sym, None, f"KSA sheet-rows error: {exc}"] for sym in ksa_symbols]

    meta = {
        "requested": list(raw_symbols),
        "normalized": list(ksa_symbols),
        "count": len(ksa_symbols),
        "ksa_only": True,
    }
    return ArgaamSheetResponse(headers=headers, rows=rows, meta=meta)


__all__ = ["router"]
