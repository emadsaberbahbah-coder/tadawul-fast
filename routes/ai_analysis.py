"""
routes/ai_analysis.py
------------------------------------------------------------
AI & QUANT ANALYSIS ROUTES – GOOGLE SHEETS FRIENDLY

- Uses core.data_engine.get_enriched_quote(s) for live data
- Computes value / quality / momentum / overall scores
- Returns clean JSON for API + sheet-friendly rows
- Never throws 500 for normal data issues: returns MISSING rows
"""

from __future__ import annotations

from datetime import datetime
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field

from core.data_engine import UnifiedQuote, get_enriched_quote, get_enriched_quotes

# ----------------------------------------------------------------------
# ROUTER
# ----------------------------------------------------------------------

router = APIRouter(
    prefix="/v1/analysis",
    tags=["AI & Analysis"],
)

# ----------------------------------------------------------------------
# MODELS
# ----------------------------------------------------------------------


class SingleAnalysisResponse(BaseModel):
    """One ticker full analysis – safe for Sheets consumption."""

    symbol: str
    name: Optional[str] = None
    market_region: Optional[str] = None

    price: Optional[float] = None
    change_pct: Optional[float] = None
    market_cap: Optional[float] = None
    pe_ttm: Optional[float] = None
    pb: Optional[float] = None
    dividend_yield: Optional[float] = None
    roe: Optional[float] = None
    roa: Optional[float] = None

    data_quality: str = "MISSING"

    value_score: Optional[float] = None
    quality_score: Optional[float] = None
    momentum_score: Optional[float] = None
    opportunity_score: Optional[float] = None
    overall_score: Optional[float] = None
    recommendation: Optional[str] = None

    last_updated_utc: Optional[datetime] = None
    sources: List[str] = Field(default_factory=list)

    notes: Optional[str] = None
    error: Optional[str] = None  # text error instead of 500


class BatchAnalysisRequest(BaseModel):
    tickers: List[str]


class BatchAnalysisResponse(BaseModel):
    results: List[SingleAnalysisResponse]


class SheetAnalysisResponse(BaseModel):
    headers: List[str]
    rows: List[List[Any]]


# ----------------------------------------------------------------------
# INTERNAL HELPERS
# ----------------------------------------------------------------------


def _safe_float(x: Any) -> Optional[float]:
    try:
        if x is None:
            return None
        return float(x)
    except Exception:
        return None


def _compute_scores(q: UnifiedQuote) -> Dict[str, Optional[float]]:
    """
    Basic AI-like scoring layer. 0–100 scale for each score.

    This is intentionally simple and deterministic so Google Sheets can rely on it.
    """
    value_score: Optional[float] = None
    quality_score: Optional[float] = None
    momentum_score: Optional[float] = None

    pe = _safe_float(q.pe_ttm)
    pb = _safe_float(q.pb)
    dy = _safe_float(q.dividend_yield)
    roe = _safe_float(q.roe)
    profit_margin = _safe_float(q.profit_margin)
    change_pct = _safe_float(q.change_pct)

    # ------------------------
    # Value score (P/E, P/B, Div Yield)
    # ------------------------
    vs = 50.0
    if pe is not None:
        if 0 < pe < 12:
            vs += 20
        elif 12 <= pe <= 20:
            vs += 10
        elif pe > 35:
            vs -= 15
    if pb is not None:
        if 0 < pb < 1:
            vs += 10
        elif pb > 4:
            vs -= 10
    if dy is not None:
        if 0.02 <= dy <= 0.06:
            vs += 10
        elif dy > 0.08:
            vs -= 5
    value_score = max(0.0, min(100.0, vs))

    # ------------------------
    # Quality score (ROE, profit margin)
    # ------------------------
    qs = 50.0
    if roe is not None:
        if roe >= 0.20:
            qs += 25
        elif 0.10 <= roe < 0.20:
            qs += 10
        elif roe < 0.0:
            qs -= 15
    if profit_margin is not None:
        if profit_margin >= 0.20:
            qs += 20
        elif 0.10 <= profit_margin < 0.20:
            qs += 10
        elif profit_margin < 0.0:
            qs -= 10
    quality_score = max(0.0, min(100.0, qs))

    # ------------------------
    # Momentum score (daily change %)
    # ------------------------
    ms = 50.0
    if change_pct is not None:
        if change_pct > 0:
            ms += min(change_pct, 10) * 2  # cap upside contribution
        elif change_pct < 0:
            ms += max(change_pct, -10) * 2  # penalties for deep red
    momentum_score = max(0.0, min(100.0, ms))

    return {
        "value_score": value_score,
        "quality_score": quality_score,
        "momentum_score": momentum_score,
    }


def _compute_overall_and_reco(
    opportunity_score: Optional[float],
    value_score: Optional[float],
    quality_score: Optional[float],
    momentum_score: Optional[float],
) -> Dict[str, Optional[float]]:
    scores = [
        s for s in [opportunity_score, value_score, quality_score, momentum_score]
        if isinstance(s, (int, float))
    ]
    if not scores:
        return {"overall_score": None, "recommendation": None}

    overall = sum(scores) / len(scores)

    if overall >= 80:
        reco = "STRONG_BUY"
    elif overall >= 65:
        reco = "BUY"
    elif overall >= 45:
        reco = "HOLD"
    elif overall >= 30:
        reco = "REDUCE"
    else:
        reco = "SELL"

    return {"overall_score": overall, "recommendation": reco}


def _quote_to_analysis(quote: UnifiedQuote) -> SingleAnalysisResponse:
    base_scores = _compute_scores(quote)
    more = _compute_overall_and_reco(
        opportunity_score=quote.opportunity_score,
        value_score=base_scores["value_score"],
        quality_score=base_scores["quality_score"],
        momentum_score=base_scores["momentum_score"],
    )

    return SingleAnalysisResponse(
        symbol=quote.symbol,
        name=quote.name,
        market_region=quote.market_region,
        price=quote.price,
        change_pct=quote.change_pct,
        market_cap=quote.market_cap,
        pe_ttm=quote.pe_ttm,
        pb=quote.pb,
        dividend_yield=quote.dividend_yield,
        roe=quote.roe,
        roa=quote.roa,
        data_quality=quote.data_quality,
        value_score=base_scores["value_score"],
        quality_score=base_scores["quality_score"],
        momentum_score=base_scores["momentum_score"],
        opportunity_score=quote.opportunity_score,
        overall_score=more["overall_score"],
        recommendation=more["recommendation"],
        last_updated_utc=quote.last_updated_utc,
        sources=[src.provider for src in (quote.sources or [])],
        notes=quote.notes,
    )


def _build_sheet_headers() -> List[str]:
    """Headers to use directly in Google Sheets."""
    return [
        "Symbol",
        "Company Name",
        "Market Region",
        "Price",
        "Change %",
        "Market Cap",
        "P/E (TTM)",
        "P/B",
        "Dividend Yield %",
        "ROE %",
        "Data Quality",
        "Value Score",
        "Quality Score",
        "Momentum Score",
        "Opportunity Score",
        "Overall Score",
        "Recommendation",
        "Last Updated (UTC)",
        "Sources",
    ]


def _analysis_to_sheet_row(a: SingleAnalysisResponse) -> List[Any]:
    """
    Flatten one analysis result to a row (all primitives),
    so Apps Script can use setValues() safely.
    """
    return [
        a.symbol,
        a.name or "",
        a.market_region or "",
        a.price,
        a.change_pct,
        a.market_cap,
        a.pe_ttm,
        a.pb,
        (a.dividend_yield * 100.0) if a.dividend_yield is not None else None,
        (a.roe * 100.0) if a.roe is not None else None,
        a.data_quality,
        a.value_score,
        a.quality_score,
        a.momentum_score,
        a.opportunity_score,
        a.overall_score,
        a.recommendation,
        a.last_updated_utc.isoformat() if a.last_updated_utc else None,
        ", ".join(a.sources) if a.sources else "",
    ]


# ----------------------------------------------------------------------
# ROUTES
# ----------------------------------------------------------------------


@router.get("/quote", response_model=SingleAnalysisResponse)
async def analyze_single_quote(ticker: str) -> SingleAnalysisResponse:
    """
    High-level AI/quant analysis for a single ticker.
    Designed to be consumed by Google Sheets / Apps Script.
    """
    ticker = (ticker or "").strip()
    if not ticker:
        raise HTTPException(status_code=400, detail="Ticker is required")

    try:
        quote = await get_enriched_quote(ticker)
        analysis = _quote_to_analysis(quote)

        # If we truly have no data from providers, mark an explicit error
        if analysis.data_quality == "MISSING":
            analysis.error = "No data available from providers"
        return analysis
    except HTTPException:
        raise
    except Exception as exc:
        # NEVER crash Google Sheets – always return a valid row
        return SingleAnalysisResponse(
            symbol=ticker.upper(),
            data_quality="MISSING",
            error=f"Exception in analysis: {exc}",
        )


@router.post("/quotes", response_model=BatchAnalysisResponse)
async def analyze_batch_quotes(body: BatchAnalysisRequest) -> BatchAnalysisResponse:
    """
    Batch AI/quant analysis for multiple tickers.
    """
    tickers = [t.strip() for t in (body.tickers or []) if t and t.strip()]
    if not tickers:
        raise HTTPException(status_code=400, detail="At least one ticker is required")

    try:
        unified_quotes = await get_enriched_quotes(tickers)
    except Exception as exc:
        # Total failure – return placeholder rows for all tickers
        placeholder_results: List[SingleAnalysisResponse] = [
            SingleAnalysisResponse(
                symbol=t.upper(),
                data_quality="MISSING",
                error=f"Batch analysis failed: {exc}",
            )
            for t in tickers
        ]
        return BatchAnalysisResponse(results=placeholder_results)

    results: List[SingleAnalysisResponse] = []
    for t, q in zip(tickers, unified_quotes):
        try:
            # If get_enriched_quotes already returns a UnifiedQuote with MISSING,
            # we still convert to keep consistent structure.
            analysis = _quote_to_analysis(q)
            if analysis.data_quality == "MISSING":
                analysis.error = "No data available from providers"
            results.append(analysis)
        except Exception as exc:
            results.append(
                SingleAnalysisResponse(
                    symbol=t.upper(),
                    data_quality="MISSING",
                    error=f"Exception building analysis: {exc}",
                )
            )

    return BatchAnalysisResponse(results=results)


@router.post("/sheet-rows", response_model=SheetAnalysisResponse)
async def analyze_for_sheet(body: BatchAnalysisRequest) -> SheetAnalysisResponse:
    """
    Google Sheets-friendly endpoint.

    Returns:
        {
          "headers": [...],
          "rows": [ [row for t1], [row for t2], ... ]
        }

    Apps Script can simply setValues() using rows with the returned headers.
    """
    batch = await analyze_batch_quotes(body)
    headers = _build_sheet_headers()

    rows: List[List[Any]] = []
    for res in batch.results:
        rows.append(_analysis_to_sheet_row(res))

    return SheetAnalysisResponse(headers=headers, rows=rows)
