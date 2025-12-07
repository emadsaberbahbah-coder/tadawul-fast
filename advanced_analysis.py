"""
advanced_analysis.py
------------------------------------------------------------
ADVANCED ANALYSIS & RISK ROUTES – GOOGLE SHEETS FRIENDLY (v1.1)

- Builds on core.data_engine.UnifiedQuote (multi-provider data)
- Computes:
    • Value / Quality / Momentum / Opportunity Scores (0–100)
    • Overall Score (0–100)
    • Risk Level (LOW / MEDIUM / HIGH / UNKNOWN)
    • Confidence Score (0–100) based on data_quality & sources
    • Short textual notes for dashboards / Sheets
- Exposes:
    • /v1/advanced/health
    • /v1/advanced/symbol
    • /v1/advanced/symbols
    • /v1/advanced/sheet-rows
- Google Sheets:
    • /sheet-rows returns { headers: [...], rows: [[...], ...] }
- Never crashes Sheets: errors become data_quality="MISSING" + error text
"""

from __future__ import annotations

from datetime import datetime
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel, Field

from core.data_engine import UnifiedQuote, get_enriched_quote, get_enriched_quotes

# ----------------------------------------------------------------------
# ROUTER
# ----------------------------------------------------------------------

router = APIRouter(
    prefix="/v1/advanced",
    tags=["Advanced Analysis"],
)

# ----------------------------------------------------------------------
# MODELS
# ----------------------------------------------------------------------


class AdvancedAnalysisResponse(BaseModel):
    """
    Advanced signal pack for a single symbol:
    - scores, risk, confidence, notes
    - safe for direct use in Google Sheets.
    """

    symbol: str
    name: Optional[str] = None
    market_region: Optional[str] = None

    price: Optional[float] = None
    change_pct: Optional[float] = None
    market_cap: Optional[float] = None

    # Core scores
    value_score: Optional[float] = None
    quality_score: Optional[float] = None
    momentum_score: Optional[float] = None
    opportunity_score: Optional[float] = None
    overall_score: Optional[float] = None

    # Risk / confidence
    risk_level: Optional[str] = None  # LOW / MEDIUM / HIGH / UNKNOWN
    confidence_score: Optional[float] = None  # 0–100
    data_quality: str = "MISSING"

    # Outcome
    recommendation: Optional[str] = None

    # Meta
    last_updated_utc: Optional[datetime] = None
    sources: List[str] = Field(default_factory=list)

    # Commentary / errors
    notes: Optional[str] = None
    error: Optional[str] = None


class BatchAdvancedRequest(BaseModel):
    tickers: List[str]


class BatchAdvancedResponse(BaseModel):
    results: List[AdvancedAnalysisResponse]


class SheetAdvancedResponse(BaseModel):
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


def _base_value_quality_momentum(q: UnifiedQuote) -> Dict[str, Optional[float]]:
    """
    Core scoring layer (Value / Quality / Momentum).
    0–100 scale, deterministic so Sheets can rely on it.
    """
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


def _overall_and_reco(
    opportunity_score: Optional[float],
    value_score: Optional[float],
    quality_score: Optional[float],
    momentum_score: Optional[float],
) -> Dict[str, Optional[float]]:
    """
    Aggregate scores into Overall + Recommendation.
    """
    scores = [
        s
        for s in [opportunity_score, value_score, quality_score, momentum_score]
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


def _risk_level_from_quote(q: UnifiedQuote) -> str:
    """
    Simple risk level from 1-day move + data_quality.
    Educational / demo logic.
    """
    if q.data_quality == "MISSING":
        return "UNKNOWN"

    cp = _safe_float(q.change_pct)
    if cp is None:
        return "MEDIUM"

    abs_cp = abs(cp)
    if abs_cp <= 1.5:
        return "LOW"
    elif abs_cp <= 4.0:
        return "MEDIUM"
    else:
        return "HIGH"


def _confidence_from_quote(q: UnifiedQuote) -> float:
    """
    Confidence 0–100 based on data_quality + number of sources.
    """
    base = {
        "EXCELLENT": 85.0,
        "GOOD": 70.0,
        "FAIR": 55.0,
        "POOR": 40.0,
        "MISSING": 20.0,
    }.get(q.data_quality, 40.0)

    n_sources = len(q.sources or [])
    bonus = min(10.0, float(n_sources) * 2.5)  # cap bonus at +10

    conf = base + bonus
    return max(0.0, min(100.0, conf))


def _build_notes(
    symbol: str,
    risk_level: Optional[str],
    recommendation: Optional[str],
    opportunity_score: Optional[float],
) -> Optional[str]:
    """
    Short human-readable note for dashboard / Sheets.
    """
    parts: List[str] = []

    if recommendation:
        parts.append(f"Signal: {recommendation}")

    if risk_level:
        parts.append(f"Risk: {risk_level}")

    if opportunity_score is not None:
        parts.append(f"OppScore≈{round(float(opportunity_score), 1)}")

    if not parts:
        return None

    return " | ".join(parts)


def _quote_to_advanced(q: UnifiedQuote) -> AdvancedAnalysisResponse:
    """
    Convert UnifiedQuote -> AdvancedAnalysisResponse.
    Shared logic with Investment Advisor / Insights sheets.
    """
    base_scores = _base_value_quality_momentum(q)
    opp_score = _safe_float(q.opportunity_score)

    agg = _overall_and_reco(
        opportunity_score=opp_score,
        value_score=base_scores["value_score"],
        quality_score=base_scores["quality_score"],
        momentum_score=base_scores["momentum_score"],
    )

    risk_level = _risk_level_from_quote(q)
    confidence = _confidence_from_quote(q)
    notes = _build_notes(
        symbol=q.symbol,
        risk_level=risk_level,
        recommendation=agg["recommendation"],
        opportunity_score=opp_score,
    )

    return AdvancedAnalysisResponse(
        symbol=q.symbol,
        name=q.name,
        market_region=q.market_region,
        price=_safe_float(q.price),
        change_pct=_safe_float(q.change_pct),
        market_cap=_safe_float(q.market_cap),
        value_score=base_scores["value_score"],
        quality_score=base_scores["quality_score"],
        momentum_score=base_scores["momentum_score"],
        opportunity_score=opp_score,
        overall_score=agg["overall_score"],
        risk_level=risk_level,
        confidence_score=confidence,
        data_quality=q.data_quality,
        recommendation=agg["recommendation"],
        last_updated_utc=q.last_updated_utc,
        sources=[src.provider for src in (q.sources or [])],
        notes=notes,
    )


def _build_sheet_headers() -> List[str]:
    """
    Headers for Advanced Analysis – ready for Google Sheets.
    """
    return [
        "Symbol",
        "Company Name",
        "Market Region",
        "Price",
        "Change %",
        "Market Cap",
        "Value Score",
        "Quality Score",
        "Momentum Score",
        "Opportunity Score",
        "Overall Score",
        "Risk Level",
        "Confidence Score",
        "Recommendation",
        "Data Quality",
        "Sources",
        "Last Updated (UTC)",
        "Notes",
        "Error",
    ]


def _advanced_to_sheet_row(a: AdvancedAnalysisResponse) -> List[Any]:
    """
    Flatten one AdvancedAnalysisResponse to a primitive-only row for setValues().
    """
    return [
        a.symbol,
        a.name or "",
        a.market_region or "",
        a.price,
        a.change_pct,
        a.market_cap,
        a.value_score,
        a.quality_score,
        a.momentum_score,
        a.opportunity_score,
        a.overall_score,
        a.risk_level or "",
        a.confidence_score,
        a.recommendation or "",
        a.data_quality,
        ", ".join(a.sources) if a.sources else "",
        a.last_updated_utc.isoformat() if a.last_updated_utc else None,
        a.notes or "",
        a.error or "",
    ]


# ----------------------------------------------------------------------
# ROUTES
# ----------------------------------------------------------------------


@router.get("/health")
async def advanced_health() -> Dict[str, Any]:
    """
    Simple health check for this module.
    """
    return {
        "status": "ok",
        "module": "advanced_analysis",
        "version": "1.1",
    }


@router.get("/symbol", response_model=AdvancedAnalysisResponse)
async def analyze_symbol(
    symbol: str = Query(..., alias="symbol"),
) -> AdvancedAnalysisResponse:
    """
    Advanced analysis for a single symbol.

    Example:
        GET /v1/advanced/symbol?symbol=AAPL
        GET /v1/advanced/symbol?symbol=1120.SR
    """
    ticker = (symbol or "").strip()
    if not ticker:
        raise HTTPException(status_code=400, detail="Symbol is required")

    try:
        quote = await get_enriched_quote(ticker)
        advanced = _quote_to_advanced(quote)
        if advanced.data_quality == "MISSING":
            advanced.error = "No data available from providers"
        return advanced
    except HTTPException:
        raise
    except Exception as exc:
        # NEVER break Google Sheets – always return a valid body
        return AdvancedAnalysisResponse(
            symbol=ticker.upper(),
            data_quality="MISSING",
            error=f"Exception in advanced analysis: {exc}",
        )


@router.post("/symbols", response_model=BatchAdvancedResponse)
async def analyze_symbols(
    body: BatchAdvancedRequest,
) -> BatchAdvancedResponse:
    """
    Advanced analysis for multiple symbols at once.

    Body:
        {
          "tickers": ["AAPL", "MSFT", "1120.SR"]
        }
    """
    tickers = [t.strip() for t in (body.tickers or []) if t and t.strip()]
    if not tickers:
        raise HTTPException(status_code=400, detail="At least one symbol is required")

    try:
        unified_quotes = await get_enriched_quotes(tickers)
    except Exception as exc:
        # Total failure – placeholder entries for all
        return BatchAdvancedResponse(
            results=[
                AdvancedAnalysisResponse(
                    symbol=t.upper(),
                    data_quality="MISSING",
                    error=f"Batch advanced analysis failed: {exc}",
                )
                for t in tickers
            ]
        )

    results: List[AdvancedAnalysisResponse] = []
    for t, q in zip(tickers, unified_quotes):
        try:
            a = _quote_to_advanced(q)
            if a.data_quality == "MISSING":
                a.error = "No data available from providers"
            results.append(a)
        except Exception as exc:
            results.append(
                AdvancedAnalysisResponse(
                    symbol=t.upper(),
                    data_quality="MISSING",
                    error=f"Exception building advanced analysis: {exc}",
                )
            )

    return BatchAdvancedResponse(results=results)


@router.post("/sheet-rows", response_model=SheetAdvancedResponse)
async def advanced_sheet_rows(
    body: BatchAdvancedRequest,
) -> SheetAdvancedResponse:
    """
    Google Sheets–friendly endpoint.

    Returns:
        {
          "headers": [...],
          "rows": [ [row for s1], [row for s2], ... ]
        }

    Apps Script usage pattern:
        - First row = headers
        - Following rows = values
    """
    batch = await analyze_symbols(body)
    headers = _build_sheet_headers()
    rows: List[List[Any]] = [_advanced_to_sheet_row(a) for a in batch.results]
    return SheetAdvancedResponse(headers=headers, rows=rows)
