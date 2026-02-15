# routes/advisor.py
"""
routes/advisor.py
------------------------------------------------------------
Tadawul Fast Bridge — Advisor Routes (v1.5.0)
(ADVANCED + ENGINE-AWARE + GAS-SAFE + TICKERS OVERRIDE)

Updates in v1.5.0:
- ✅ Uses app.state.engine if available (shared caching).
- ✅ Accepts tickers in multiple shapes: list OR string "AAPL,MSFT 1120.SR".
- ✅ Passes tickers override into core advisor (restrict universe).
- ✅ Always returns stable headers (never empty) for Google Sheets writing.
- ✅ Injects Riyadh timestamps at route level into meta.
- ✅ Adds /run alias endpoint (optional compatibility) calling same handler.

Contract:
POST /v1/advisor/recommendations
POST /v1/advisor/run   (alias)

Response:
{ status, headers, rows, meta, error? }
"""

from __future__ import annotations

import time
import logging
from datetime import datetime, timezone, timedelta
from typing import Any, Dict, List, Optional, Tuple

from fastapi import APIRouter, Request

from schemas.advisor import (
    AdvisorRequest,
    AdvisorResponse,
    TT_ADVISOR_DEFAULT_HEADERS,
)

logger = logging.getLogger("routes.advisor")

router = APIRouter(prefix="/v1/advisor", tags=["advisor"])
ADVISOR_ROUTE_VERSION = "1.5.0"


def _riyadh_iso() -> str:
    tz = timezone(timedelta(hours=3))
    return datetime.now(tz).isoformat()


def _norm_symbol(x: Any) -> str:
    s = ("" if x is None else str(x)).strip().upper().replace(" ", "")
    if s.endswith(".SA"):
        s = s[:-3] + ".SR"
    return s


def _normalize_tickers(req: AdvisorRequest) -> List[str]:
    """
    Supports:
      - req.tickers: List[str]
      - req.symbols: List[str]
      - req.tickers: str "AAPL,MSFT 1120.SR"
      - req.symbols: str "..."
    """
    tickers: List[str] = []

    raw = None
    if getattr(req, "tickers", None):
        raw = req.tickers
    elif getattr(req, "symbols", None):
        raw = req.symbols

    if isinstance(raw, str):
        parts = raw.replace(",", " ").split()
        tickers = [_norm_symbol(p) for p in parts if p.strip()]
    elif isinstance(raw, list):
        tickers = [_norm_symbol(x) for x in raw if str(x or "").strip()]

    # drop empties + de-dup preserve order
    out: List[str] = []
    seen = set()
    for t in tickers:
        if t and t not in seen:
            seen.add(t)
            out.append(t)
    return out


@router.post("/recommendations", response_model=AdvisorResponse)
async def advisor_recommendations(req: AdvisorRequest, request: Request) -> AdvisorResponse:
    """
    Main advisor endpoint used by Google Sheets.
    """
    start_time = time.time()

    # 1) Normalize tickers override
    tickers = _normalize_tickers(req)

    # 2) Resolve Engine (Shared State)
    engine = None
    try:
        if hasattr(request.app.state, "engine"):
            engine = request.app.state.engine
    except Exception:
        engine = None

    # 3) Run Advisor Logic (Resilient Import)
    try:
        from core.investment_advisor import run_investment_advisor

        core_payload: Dict[str, Any] = {
            "sources": req.sources,
            "risk": req.risk,
            "confidence": req.confidence,
            "required_roi_1m": req.required_roi_1m,
            "required_roi_3m": req.required_roi_3m,
            "top_n": req.top_n,
            "invest_amount": req.invest_amount,
            "currency": req.currency,
            "include_news": req.include_news,
            "as_of_utc": req.as_of_utc,
            "min_price": req.min_price,
            "max_price": req.max_price,
            "exclude_sectors": getattr(req, "exclude_sectors", None),
            "liquidity_min": getattr(req, "liquidity_min", None),
            "max_position_pct": getattr(req, "max_position_pct", None),
            "min_position_pct": getattr(req, "min_position_pct", None),
        }

        # tickers override (restrict universe)
        if tickers:
            core_payload["tickers"] = tickers

        result = run_investment_advisor(core_payload, engine=engine)

    except ImportError:
        logger.error("core.investment_advisor module not found.")
        return AdvisorResponse(
            status="error",
            error="Advisor service module missing on server.",
            headers=list(TT_ADVISOR_DEFAULT_HEADERS),
            rows=[],
            meta={"route_version": ADVISOR_ROUTE_VERSION, "last_updated_riyadh": _riyadh_iso()},
        )
    except Exception as e:
        logger.exception("Advisor execution failed.")
        return AdvisorResponse(
            status="error",
            error=f"Advisor execution failed: {str(e)}",
            headers=list(TT_ADVISOR_DEFAULT_HEADERS),
            rows=[],
            meta={"route_version": ADVISOR_ROUTE_VERSION, "last_updated_riyadh": _riyadh_iso()},
        )

    # 4) Output (prefer core's headers/rows)
    headers = result.get("headers") or list(TT_ADVISOR_DEFAULT_HEADERS)
    rows = result.get("rows") or []
    meta = result.get("meta") or {}

    # Absolute GAS safety: never return empty headers
    if not isinstance(headers, list) or len(headers) == 0:
        headers = list(TT_ADVISOR_DEFAULT_HEADERS)

    processing_time = round((time.time() - start_time) * 1000, 2)

    meta.update(
        {
            "route_version": ADVISOR_ROUTE_VERSION,
            "engine_status": "injected" if engine else "fallback",
            "processing_time_ms": processing_time,
            "last_updated_riyadh": _riyadh_iso(),
            "tickers_override_count": len(tickers) if tickers else 0,
            "tickers_override_mode": True if tickers else False,
            "opportunities_found": len(rows),
        }
    )

    status = "ok"
    if isinstance(meta, dict) and meta.get("ok") is False:
        status = "error"

    return AdvisorResponse(
        status=status,
        headers=headers,
        rows=rows,
        meta=meta,
        error=meta.get("error") if status == "error" else None,
    )


@router.post("/run", response_model=AdvisorResponse)
async def advisor_run(req: AdvisorRequest, request: Request) -> AdvisorResponse:
    """
    Alias endpoint for compatibility with older GAS / backend calls.
    """
    return await advisor_recommendations(req, request)
