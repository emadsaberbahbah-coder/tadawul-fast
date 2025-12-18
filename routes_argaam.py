# routes_argaam.py
"""
routes_argaam.py
------------------------------------------------------------
KSA / Argaam Gateway – v2.9.0 (Engine v2 aligned, Sheets-safe)

Purpose
- KSA-only router: accepts numeric or .SR symbols only.
- Delegates ALL fetching to core.data_engine_v2.DataEngine (no direct provider calls).
- Returns EnrichedQuote (+ scores best-effort) for parity with Global routes.
- Provides /sheet-rows (headers + rows) for Google Sheets.

Design goals
- Extremely defensive (Sheets-safe): prefer returning 200 + error payload instead of raising.
- Strict KSA normalization: always respond with 1234.SR when possible.
- Header-safe row building: never breaks if schemas/to_row mismatch.
"""

from __future__ import annotations

import logging
from functools import lru_cache
from typing import Any, Dict, List, Optional, Sequence

from fastapi import APIRouter, Query
from pydantic import BaseModel, Field

from core.config import get_settings
from core.data_engine_v2 import DataEngine, UnifiedQuote
from core.enriched_quote import EnrichedQuote

logger = logging.getLogger("routes.argaam")
ROUTE_VERSION = "2.9.0"

# ---------------------------------------------------------------------------
# Optional schema helper (preferred)
# ---------------------------------------------------------------------------
try:
    from core.schemas import get_headers_for_sheet  # type: ignore
except Exception:  # pragma: no cover

    def get_headers_for_sheet(sheet_name: str) -> List[str]:
        return [
            "Symbol",
            "Data Quality",
            "Last Price",
            "Change %",
            "Volume",
            "Market Cap",
            "P/E (TTM)",
            "P/B",
            "Dividend Yield %",
            "ROE %",
            "Opportunity Score",
            "Recommendation",
            "Data Source",
            "Last Updated (UTC)",
            "Last Updated (Riyadh)",
            "Error",
        ]


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


def _dedupe_preserve_order(items: Sequence[str]) -> List[str]:
    seen = set()
    out: List[str] = []
    for x in items or []:
        x = (x or "").strip().upper()
        if not x:
            continue
        if x in seen:
            continue
        seen.add(x)
        out.append(x)
    return out


# ---------------------------------------------------------------------------
# Header / row helpers
# ---------------------------------------------------------------------------
def _safe_headers(sheet_name: str) -> List[str]:
    """
    Ensures headers are always usable:
    - non-empty
    - includes Symbol as first column (required for most Sheets mapping)
    - includes Error column
    """
    try:
        headers = get_headers_for_sheet(sheet_name) or []
    except Exception:
        headers = []

    headers = [str(h).strip() for h in headers if h and str(h).strip()]
    if not headers:
        headers = get_headers_for_sheet("fallback")  # fallback above

    if not headers or headers[0].strip().lower() != "symbol":
        headers = [h for h in headers if h.strip().lower() != "symbol"]
        headers.insert(0, "Symbol")

    if not any(h.strip().lower() == "error" for h in headers):
        headers.append("Error")

    return headers


def _row_with_error(headers: List[str], symbol: str, error: str, data_quality: str = "MISSING") -> List[Any]:
    row: List[Any] = [None] * len(headers)

    def _idx(name: str) -> Optional[int]:
        name = name.strip().lower()
        for i, h in enumerate(headers):
            if h.strip().lower() == name:
                return i
        return None

    i_sym = _idx("symbol")
    i_err = _idx("error")
    i_dq = _idx("data quality") or _idx("data_quality")

    if i_sym is not None:
        row[i_sym] = symbol
    else:
        row[0] = symbol

    if i_dq is not None:
        row[i_dq] = data_quality

    if i_err is not None:
        row[i_err] = error
    else:
        row[-1] = error

    return row


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
# Transform: UnifiedQuote -> EnrichedQuote (+ scores best-effort)
# ---------------------------------------------------------------------------
def _to_scored_enriched(uq: UnifiedQuote, fallback_symbol: str) -> EnrichedQuote:
    sym = _normalize_ksa_symbol(fallback_symbol) or (fallback_symbol or "").strip().upper() or "UNKNOWN"

    forced: Dict[str, Any] = {"symbol": sym, "market": "KSA", "currency": "SAR"}

    try:
        eq = EnrichedQuote.from_unified(uq)

        # Force minimal KSA metadata (don’t depend on provider output)
        try:
            eq = eq.model_copy(
                update={k: v for k, v in forced.items() if getattr(eq, k, None) in (None, "", "UNKNOWN")}
            )
        except Exception:
            eq = eq.copy(update={k: v for k, v in forced.items() if getattr(eq, k, None) in (None, "", "UNKNOWN")})

        # Ensure data_source present
        if not getattr(eq, "data_source", None):
            ds = getattr(uq, "data_source", None) or "argaam_gateway"
            try:
                eq = eq.model_copy(update={"data_source": ds})
            except Exception:
                eq = eq.copy(update={"data_source": ds})

        # Scores best-effort (never fail the response)
        try:
            from core.scoring_engine import enrich_with_scores  # type: ignore

            try:
                eq2 = enrich_with_scores(eq)  # preferred path
                if isinstance(eq2, EnrichedQuote):
                    eq = eq2
            except Exception:
                # Fallback: score UnifiedQuote then re-map
                uq2 = enrich_with_scores(uq)  # type: ignore
                eq = EnrichedQuote.from_unified(uq2)  # type: ignore
        except Exception:
            pass

        # Guarantee symbol correctness
        if getattr(eq, "symbol", None) != sym:
            try:
                eq = eq.model_copy(update={"symbol": sym})
            except Exception:
                eq = eq.copy(update={"symbol": sym})

        return eq

    except Exception as exc:
        logger.exception("routes_argaam: transform error for %s", sym)
        return EnrichedQuote(symbol=sym, market="KSA", currency="SAR", data_quality="MISSING", error=f"Transform error: {exc}")


# ---------------------------------------------------------------------------
# Router
# ---------------------------------------------------------------------------
router = APIRouter(prefix="/v1/argaam", tags=["KSA / Argaam"])


@router.get("/health")
async def argaam_health() -> Dict[str, Any]:
    s = get_settings()
    engine = _get_engine()
    default_sheet = getattr(s, "sheet_ksa_tadawul", None) or "KSA_Tadawul_Market"

    return {
        "status": "ok",
        "module": "routes_argaam",
        "route_version": ROUTE_VERSION,
        "engine": "DataEngineV2",
        "providers": getattr(engine, "enabled_providers", None) or [],
        "ksa_mode": "STRICT",
        "default_sheet": default_sheet,
    }


@router.get("/quote")
async def ksa_single_quote(
    symbol: str = Query(..., description="KSA symbol (1120 or 1120.SR)"),
    debug: int = Query(0, description="debug=1 includes _meta in response"),
) -> Any:
    """
    Returns EnrichedQuote (or dict with _meta when debug=1).
    Sheets-safe: never raises for normal usage.
    """
    ksa_symbol = _normalize_ksa_symbol(symbol)
    if not ksa_symbol:
        eq = EnrichedQuote(
            symbol=(symbol or "").strip().upper() or "UNKNOWN",
            market="KSA",
            currency="SAR",
            data_quality="MISSING",
            error=f"Invalid KSA symbol '{symbol}'. Must be numeric or end in .SR",
        )
        if debug:
            d = eq.model_dump(exclude_none=False) if hasattr(eq, "model_dump") else eq.dict()
            d["_meta"] = {"debug": 1, "route_version": ROUTE_VERSION}
            return d
        return eq

    try:
        engine = _get_engine()
        uq = await engine.get_enriched_quote(ksa_symbol)
        eq = _to_scored_enriched(uq, ksa_symbol)

        if debug:
            d = eq.model_dump(exclude_none=False) if hasattr(eq, "model_dump") else eq.dict()
            d["_meta"] = {
                "debug": 1,
                "route_version": ROUTE_VERSION,
                "engine": "DataEngineV2",
                "provider": getattr(uq, "data_source", None),
                "data_quality": getattr(uq, "data_quality", None),
                "engine_error": getattr(uq, "error", None),
            }
            return d

        return eq

    except Exception as exc:
        logger.exception("routes_argaam: engine error for %s", ksa_symbol)
        eq = EnrichedQuote(symbol=ksa_symbol, market="KSA", currency="SAR", data_quality="MISSING", error=str(exc))
        if debug:
            d = eq.model_dump(exclude_none=False) if hasattr(eq, "model_dump") else eq.dict()
            d["_meta"] = {"debug": 1, "route_version": ROUTE_VERSION, "engine": "DataEngineV2", "exception": str(exc)}
            return d
        return eq


@router.post("/quotes")
async def ksa_batch_quotes(body: ArgaamBatchRequest) -> List[EnrichedQuote]:
    raw_list = body.symbols or body.tickers or []
    if not raw_list:
        return []

    normalized: List[str] = []
    invalid: List[str] = []

    for s in raw_list:
        n = _normalize_ksa_symbol(s)
        if n:
            normalized.append(n)
        else:
            if s and str(s).strip():
                invalid.append(str(s).strip())

    targets = _dedupe_preserve_order(normalized)
    if not targets:
        return [
            EnrichedQuote(
                symbol=bad.upper(),
                market="KSA",
                currency="SAR",
                data_quality="MISSING",
                error="Invalid KSA symbol (must be numeric or end in .SR)",
            )
            for bad in invalid
        ]

    engine = _get_engine()
    try:
        uqs = await engine.get_enriched_quotes(targets)
        uq_map = {q.symbol.upper(): q for q in (uqs or []) if getattr(q, "symbol", None)}
    except Exception as exc:
        logger.exception("routes_argaam: batch engine error")
        out_err = [EnrichedQuote(symbol=t, market="KSA", currency="SAR", data_quality="MISSING", error=str(exc)) for t in targets]
        for bad in invalid:
            out_err.append(
                EnrichedQuote(
                    symbol=bad.upper(),
                    market="KSA",
                    currency="SAR",
                    data_quality="MISSING",
                    error="Invalid KSA symbol (must be numeric or end in .SR)",
                )
            )
        return out_err

    out: List[EnrichedQuote] = []
    for t in targets:
        uq = uq_map.get(t.upper()) or UnifiedQuote(symbol=t, market="KSA", currency="SAR", data_quality="MISSING", error="No data returned")
        out.append(_to_scored_enriched(uq, t))

    for bad in invalid:
        out.append(
            EnrichedQuote(
                symbol=bad.upper(),
                market="KSA",
                currency="SAR",
                data_quality="MISSING",
                error="Invalid KSA symbol (must be numeric or end in .SR)",
            )
        )

    return out


@router.post("/sheet-rows")
async def ksa_sheet_rows(body: ArgaamBatchRequest) -> KSASheetResponse:
    """
    Returns headers + rows for Google Sheets.
    Never raises for normal usage; returns rows with Error column populated.
    """
    raw_list = body.symbols or body.tickers or []

    s = get_settings()
    sheet_name = body.sheet_name or getattr(s, "sheet_ksa_tadawul", None) or "KSA_Tadawul_Market"

    headers = _safe_headers(sheet_name)

    if not raw_list:
        return KSASheetResponse(headers=headers, rows=[], meta={"note": "No symbols provided", "sheet_name": sheet_name})

    valid_targets: List[str] = []
    invalid: List[str] = []
    for x in raw_list:
        n = _normalize_ksa_symbol(x)
        if n:
            valid_targets.append(n)
        else:
            if x and str(x).strip():
                invalid.append(str(x).strip())

    valid_targets = _dedupe_preserve_order(valid_targets)

    rows: List[List[Any]] = []
    for bad in invalid:
        rows.append(_row_with_error(headers, bad.upper(), "Invalid KSA symbol (must be numeric or end in .SR)"))

    if not valid_targets:
        return KSASheetResponse(
            headers=headers,
            rows=rows,
            meta={"note": "No valid KSA symbols provided", "invalid": invalid, "sheet_name": sheet_name},
        )

    engine = _get_engine()
    try:
        uqs = await engine.get_enriched_quotes(valid_targets)
        uq_map = {q.symbol.upper(): q for q in (uqs or []) if getattr(q, "symbol", None)}
    except Exception as exc:
        logger.exception("routes_argaam: sheet-rows engine error")
        for t in valid_targets:
            rows.append(_row_with_error(headers, t, f"Engine error: {exc}"))
        return KSASheetResponse(headers=headers, rows=rows, meta={"count": len(rows), "sheet_name": sheet_name, "engine_error": str(exc)})

    for t in valid_targets:
        uq = uq_map.get(t.upper()) or UnifiedQuote(symbol=t, market="KSA", currency="SAR", data_quality="MISSING", error="No data")
        eq = _to_scored_enriched(uq, t)
        try:
            row = eq.to_row(headers)  # type: ignore
            if not isinstance(row, list):
                raise ValueError("to_row did not return a list")
            if len(row) < len(headers):
                row += [None] * (len(headers) - len(row))
            rows.append(row[: len(headers)])
        except Exception as exc:
            logger.error("routes_argaam: row mapping failed for %s: %s", t, exc)
            dq = getattr(eq, "data_quality", None) or "MISSING"
            rows.append(_row_with_error(headers, t, f"Row mapping failed: {exc}", data_quality=str(dq)))

    return KSASheetResponse(
        headers=headers,
        rows=rows,
        meta={
            "count": len(rows),
            "requested": len(raw_list),
            "valid": len(valid_targets),
            "invalid": len(invalid),
            "sheet_name": sheet_name,
        },
    )


__all__ = ["router"]
