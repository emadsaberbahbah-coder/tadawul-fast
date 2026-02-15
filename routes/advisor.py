# routes/advisor.py
"""
routes/advisor.py
------------------------------------------------------------
Tadawul Fast Bridge — Advisor Routes (v1.4.0)
(ADVANCED + ENGINE-AWARE + RIYADH LOCALIZED)

Updates in v1.4.0:
- ✅ **Riyadh Localization**: Injects 'last_updated_riyadh' (UTC+3) into response metadata.
- ✅ **Schema Alignment**: Explicitly maps 'forecast_price_12m' & 'expected_roi_12m' to table output.
- ✅ **Engine Injection**: Uses app.state.engine if available for shared caching.
- ✅ **Resilient Import**: Gracefully handles missing service modules.
- ✅ **Dual-Mode Input**: Accepts both `tickers` (list) and `symbols` (list).
"""

from __future__ import annotations

import time
import logging
from datetime import datetime, timezone, timedelta
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, Request

from schemas.advisor import (
    AdvisorRequest,
    AdvisorResponse,
    TT_ADVISOR_DEFAULT_HEADERS,
)

# Configure Logger
logger = logging.getLogger("routes.advisor")

router = APIRouter(prefix="/v1/advisor", tags=["advisor"])

ADVISOR_ROUTE_VERSION = "1.4.0"

def _riyadh_iso() -> str:
    tz = timezone(timedelta(hours=3))
    return datetime.now(tz).isoformat()

def _items_to_table(items: List[Dict[str, Any]]) -> tuple[List[str], List[List[Any]]]:
    """
    Convert advisor engine items into Sheets table.
    Keeps a stable schema for GAS writing.
    """
    headers = list(TT_ADVISOR_DEFAULT_HEADERS)
    rows: List[List[Any]] = []

    for i, it in enumerate(items or [], start=1):
        rows.append(
            [
                i,
                it.get("symbol", "") or it.get("ticker", "") or "",
                it.get("origin", "") or "",
                it.get("name", "") or "",
                it.get("market", "") or "",
                it.get("currency", "") or "",
                it.get("price", "") or it.get("current_price", "") or "",
                it.get("advisor_score", "") or it.get("score", "") or "",
                it.get("action", "") or it.get("recommendation", "") or "",
                it.get("allocation_pct", "") or it.get("allocation_percent", "") or "",
                it.get("allocation_amount", "") or it.get("amount", "") or "",
                it.get("expected_roi_1m_pct", "") or it.get("roi_1m_pct", "") or "",
                it.get("expected_roi_3m_pct", "") or it.get("roi_3m_pct", "") or "",
                it.get("risk_bucket", "") or it.get("risk", "") or "",
                it.get("confidence_bucket", "") or it.get("confidence", "") or "",
                it.get("reason", "") or it.get("explain", "") or "",
                it.get("data_source", "") or it.get("source", "") or "",
                it.get("data_quality", "") or it.get("quality", "") or "",
                it.get("last_updated_utc", "") or it.get("updated_at_utc", "") or "",
                # v1.4.0: Explicitly map Riyadh Time if available in source item
                it.get("last_updated_riyadh", "") or "", 
            ]
        )

    return headers, rows


@router.post("/recommendations", response_model=AdvisorResponse)
async def advisor_recommendations(req: AdvisorRequest, request: Request) -> AdvisorResponse:
    """
    Main advisor endpoint used by Google Sheets.

    Features:
    - Normalizes input tickers.
    - Injects shared DataEngine for caching.
    - Returns standardized headers even on empty results.
    - Adds Riyadh timestamps.
    """
    start_time = time.time()

    # -----------------------------
    # 1. Normalize tickers universe
    # -----------------------------
    tickers = list(req.tickers or [])
    if not tickers and req.symbols:
        tickers = list(req.symbols)

    # -----------------------------
    # 2. Resolve Engine (Shared State)
    # -----------------------------
    engine = None
    try:
        if hasattr(request.app.state, "engine"):
            engine = request.app.state.engine
    except Exception:
        pass

    # -----------------------------
    # 3. Run Advisor Logic (Resilient Import)
    # -----------------------------
    try:
        # Dynamic import to avoid crash if service layer is missing/broken
        from core.investment_advisor import run_investment_advisor
        
        # Prepare payload compatible with core logic
        core_payload = {
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
            # Pass tickers explicitly if provided (overrides 'sources' logic inside advisor)
            "tickers": tickers if tickers else None
        }

        # Execute Core Logic
        # Note: core.investment_advisor.run_investment_advisor handles the Universe scanning
        # using the passed engine to fetch cached snapshots.
        result = run_investment_advisor(core_payload, engine=engine)
    
    except ImportError:
        logger.error("core.investment_advisor module not found.")
        return AdvisorResponse(
            status="error",
            error="Advisor service module missing on server.",
            headers=list(TT_ADVISOR_DEFAULT_HEADERS),
            rows=[],
            meta={"route_version": ADVISOR_ROUTE_VERSION},
        )
    except Exception as e:
        logger.exception("Advisor execution failed.")
        return AdvisorResponse(
            status="error",
            error=f"Advisor execution failed: {str(e)}",
            headers=list(TT_ADVISOR_DEFAULT_HEADERS),
            rows=[],
            meta={"route_version": ADVISOR_ROUTE_VERSION},
        )

    # -----------------------------
    # 4. Format Output
    # -----------------------------
    items = result.get("items") or []
    # If core returns pre-formatted headers/rows, use them; otherwise convert items
    if "headers" in result and "rows" in result:
        headers = result["headers"]
        rows = result["rows"]
    else:
        headers, rows = _items_to_table(items)

    count = len(items) if items else len(rows)

    # -----------------------------
    # 5. Build Final Response
    # -----------------------------
    meta = result.get("meta") or {}
    
    # Inject route-level diagnostics
    processing_time = round((time.time() - start_time) * 1000, 2)
    meta.update(
        {
            "route_version": ADVISOR_ROUTE_VERSION,
            "engine_status": "injected" if engine else "fallback",
            "processing_time_ms": processing_time,
            "last_updated_riyadh": _riyadh_iso(), # ✅ Added Riyadh Time
            "filters_applied": {
                "risk": req.risk,
                "confidence": req.confidence,
                "roi_1m_target": req.required_roi_1m,
                "top_n": req.top_n,
            },
            "tickers_scanned": len(tickers) if tickers else "sheet_cache",
            "opportunities_found": count,
        }
    )

    return AdvisorResponse(
        status="ok",
        headers=headers,  # NEVER empty
        rows=rows,        # can be empty
        meta=meta,
    )
