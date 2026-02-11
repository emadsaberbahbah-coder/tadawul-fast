# tadawul-fast/routes/advisor.py
"""
Tadawul Fast Bridge — Advisor Routes
Version: 1.3.0

Fixes in v1.3.0
- Always returns Sheets-safe response: headers are NEVER empty when status="ok"
- Converts engine 'items' output to headers+rows
- Supports payload.tickers / payload.symbols
- When 0 opportunities matched, returns default headers and rows=[]
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional

from fastapi import APIRouter

from schemas.advisor import (
    AdvisorRequest,
    AdvisorResponse,
    TT_ADVISOR_DEFAULT_HEADERS,
)

router = APIRouter(prefix="/v1/advisor", tags=["advisor"])


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
            ]
        )

    return headers, rows


@router.post("/recommendations", response_model=AdvisorResponse)
def advisor_recommendations(req: AdvisorRequest) -> AdvisorResponse:
    """
    Main advisor endpoint used by Google Sheets.

    Expected behavior:
    - If no matches (count=0), still returns headers (non-empty) + rows=[]
    """
    # -----------------------------
    # Normalize tickers universe
    # -----------------------------
    tickers = list(req.tickers or [])
    if not tickers and req.symbols:
        tickers = list(req.symbols)

    # If still empty, return an error response (client can show message)
    if not tickers:
        return AdvisorResponse(
            status="error",
            error="No tickers provided. Provide payload.tickers (list or comma string) or payload.symbol(s).",
            headers=list(TT_ADVISOR_DEFAULT_HEADERS),
            rows=[],
            meta={"route_version": "1.3.0"},
        )

    # -----------------------------
    # Run your advisor engine
    # -----------------------------
    # NOTE: Replace this import/call with your actual engine function.
    # It must return dict with: items (list), count (int), filters/meta optional.
    from services.advisor_engine import run_advisor  # adjust to your codebase

    result: Dict[str, Any] = run_advisor(
        tickers=tickers,
        sources=req.sources,
        risk=req.risk,
        confidence=req.confidence,
        required_roi_1m=req.required_roi_1m,
        required_roi_3m=req.required_roi_3m,
        top_n=req.top_n,
        invest_amount=req.invest_amount,
        currency=req.currency,
        include_news=req.include_news,
        as_of_utc=req.as_of_utc,
        min_price=req.min_price,
        max_price=req.max_price,
    )

    items = result.get("items") or []
    count = int(result.get("count") or len(items))

    headers, rows = _items_to_table(items)

    # -----------------------------
    # Build stable response (CRITICAL)
    # -----------------------------
    # Always return headers for ok, even if count == 0
    meta = result.get("meta") or {}
    meta.update(
        {
            "route_version": "1.3.0",
            "filters": {
                "risk": req.risk,
                "confidence": req.confidence,
                "required_roi_1m_pct": round(req.required_roi_1m * 100, 4),
                "required_roi_3m_pct": round(req.required_roi_3m * 100, 4),
                "top_n": req.top_n,
                "amount": req.invest_amount,
                "output_mode": "items",
            },
            "tickers_count": len(tickers),
            "count": count,
        }
    )

    return AdvisorResponse(
        status="ok",
        headers=headers,  # NEVER empty
        rows=rows,        # can be empty
        meta=meta,
    )
