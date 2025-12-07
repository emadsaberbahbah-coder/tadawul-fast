"""
routes/advanced_analysis.py

TADAWUL FAST BRIDGE – ADVANCED ANALYSIS ROUTES
================================================
Purpose:
- Expose "advanced analysis" and "opportunity ranking" endpoints
  on top of the unified DataEngine (core.data_engine_v2).
- Designed to be Google Sheets–friendly and API-friendly.

Key ideas:
- Use DataEngine v2 if available, else fall back to v1, else stub.
- Take a list of tickers, fetch enriched quotes, and build a
  scoreboard sorted by Opportunity Score.
- Compute a simple risk bucket based on Opportunity & Data Quality.

Typical usage from main.py:
    from routes import advanced_analysis
    app.include_router(advanced_analysis.router, prefix="")

Endpoints:
- GET /v1/advanced/ping
- GET /v1/advanced/scoreboard?tickers=AAPL,MSFT,GOOGL&top_n=50
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel, Field

logger = logging.getLogger(__name__)

# =============================================================================
# DATA ENGINE IMPORT & FALLBACKS
# =============================================================================

_ENGINE_IS_STUB = False

try:
    # Preferred: new v2 engine
    from core.data_engine_v2 import DataEngine  # type: ignore[attr-defined]
    logger.info("AdvancedAnalysis: using DataEngine from core.data_engine_v2")
except Exception as exc_v2:
    logger.error(
        "AdvancedAnalysis: Failed to import DataEngine from core.data_engine_v2: %s",
        exc_v2,
    )
    try:
        # Fallback: legacy engine
        from core.data_engine import DataEngine  # type: ignore[attr-defined]
        logger.info("AdvancedAnalysis: using fallback DataEngine from core.data_engine")
    except Exception as exc_v1:
        logger.error(
            "AdvancedAnalysis: Failed to import DataEngine from core.data_engine: %s",
            exc_v1,
        )

        class DataEngine:  # type: ignore[no-redef]
            """
            Stub DataEngine used ONLY when the real engine fails to import.
            Always returns placeholder / missing data.
            """

            async def get_enriched_quote(self, ticker: str) -> Dict[str, Any]:
                return {
                    "symbol": ticker,
                    "name": None,
                    "market": None,
                    "sector": None,
                    "currency": None,
                    "price": None,
                    "fair_value": None,
                    "upside_percent": None,
                    "value_score": 0.0,
                    "quality_score": 0.0,
                    "momentum_score": 0.0,
                    "opportunity_score": 0.0,
                    "data_quality_score": 0.0,
                    "recommendation_label": "MISSING",
                    "risk_label": "UNKNOWN",
                    "notes": "STUB_ENGINE: DataEngine could not be imported on backend.",
                }

        _ENGINE_IS_STUB = True
        logger.error(
            "AdvancedAnalysis: Using STUB DataEngine – all responses will contain "
            "MISSING placeholder values."
        )


# Global singleton engine (lazy init)
_engine: Optional[DataEngine] = None


def get_engine() -> DataEngine:
    """
    Get (and lazily create) the shared DataEngine instance.
    """
    global _engine
    if _engine is None:
        _engine = DataEngine()  # type: ignore[call-arg]
    return _engine


# =============================================================================
# Pydantic MODELS
# =============================================================================


class AdvancedItem(BaseModel):
    """
    A compact, Sheets-friendly view of an enriched quote with
    scores and advanced signals.
    """

    symbol: str = Field(..., description="Ticker symbol, e.g. 1120.SR or AAPL")
    name: Optional[str] = Field(None, description="Company or instrument name")
    market: Optional[str] = Field(None, description="Market or exchange code")
    sector: Optional[str] = Field(None, description="Sector if available")
    currency: Optional[str] = Field(None, description="Trading currency")

    price: Optional[float] = Field(None, description="Last traded price")
    fair_value: Optional[float] = Field(None, description="Modelled fair value")
    upside_percent: Optional[float] = Field(
        None, description="(FairValue - Price)/Price * 100"
    )

    value_score: Optional[float] = Field(
        None, description="0–100 Value factor score (cheaper is higher)"
    )
    quality_score: Optional[float] = Field(
        None, description="0–100 Quality factor score (profitability/strength)"
    )
    momentum_score: Optional[float] = Field(
        None, description="0–100 Momentum factor score"
    )
    opportunity_score: Optional[float] = Field(
        None, description="0–100 composite opportunity score"
    )
    data_quality_score: Optional[float] = Field(
        None, description="0–100 confidence in the underlying data"
    )

    recommendation: Optional[str] = Field(
        None, description="Text label from engine e.g. STRONG_BUY / BUY / HOLD / SELL"
    )
    risk_label: Optional[str] = Field(
        None, description="Optional risk category from engine, if available"
    )
    risk_bucket: Optional[str] = Field(
        None,
        description=(
            "Derived bucket combining opportunity & data quality, "
            "e.g. 'HIGH_OPP_HIGH_CONF', 'MED_OPP_HIGH_CONF', 'LOW_CONF', etc."
        ),
    )

    provider: Optional[str] = Field(
        None, description="Primary data provider/source if available"
    )
    data_age_minutes: Optional[float] = Field(
        None, description="Approx. age of quote in minutes (if provided)"
    )


class AdvancedScoreboardResponse(BaseModel):
    """
    Top-level response model for /v1/advanced/scoreboard.
    """

    generated_at_utc: str = Field(
        ..., description="ISO datetime (UTC) when the scoreboard was generated"
    )
    engine_is_stub: bool = Field(
        ..., description="True if a stub DataEngine was used (no real data)."
    )
    total_requested: int = Field(
        ..., description="Total tickers requested by client (after de-dup)."
    )
    total_returned: int = Field(
        ..., description="Total tickers successfully analyzed and returned."
    )
    top_n_applied: bool = Field(
        ..., description="True if results were truncated to top_n."
    )
    tickers: List[str] = Field(
        ..., description="Cleaned list of requested tickers in order processed."
    )
    items: List[AdvancedItem] = Field(
        ..., description="List of AdvancedItem entries sorted by opportunity."
    )


# =============================================================================
# ROUTER
# =============================================================================

router = APIRouter(
    prefix="/v1/advanced",
    tags=["Advanced Analysis"],
)


# =============================================================================
# INTERNAL HELPERS
# =============================================================================


def _safe_float(value: Any) -> Optional[float]:
    """
    Convert a value to float if possible, else None.
    """
    try:
        if value is None:
            return None
        return float(value)
    except Exception:
        return None


def _compute_risk_bucket(
    opportunity: Optional[float], data_quality: Optional[float]
) -> str:
    """
    Very simple risk/opportunity bucketing, purely heuristic.
    You can refine later based on your investment logic.
    """
    opp = opportunity or 0.0
    dq = data_quality or 0.0

    if dq < 40:
        return "LOW_CONFIDENCE"

    if opp >= 75 and dq >= 70:
        return "HIGH_OPP_HIGH_CONF"
    if 55 <= opp < 75 and dq >= 60:
        return "MED_OPP_HIGH_CONF"
    if opp >= 55 and 40 <= dq < 60:
        return "OPP_WITH_MED_CONF"

    return "NEUTRAL_OR_LOW_OPP"


async def _fetch_enriched_for_tickers(tickers: List[str]) -> List[AdvancedItem]:
    """
    For each ticker, call DataEngine.get_enriched_quote and map it into AdvancedItem.
    Any failures are logged and skipped.
    """
    engine = get_engine()
    results: List[AdvancedItem] = []

    for raw_symbol in tickers:
        symbol = raw_symbol.strip()
        if not symbol:
            continue

        try:
            enriched = await engine.get_enriched_quote(symbol)  # type: ignore[attr-defined]

            # DataEngine v2 likely returns a Pydantic model; fall back to dict.
            if hasattr(enriched, "model_dump"):
                data = enriched.model_dump()  # type: ignore[call-arg]
            elif isinstance(enriched, dict):
                data = enriched
            else:
                # Unknown type, best effort via __dict__
                data = getattr(enriched, "__dict__", {}) or {}

            # Extract fields with graceful fallbacks for both v1/v2 naming variations.
            price = _safe_float(
                data.get("price")
                or data.get("last_price")
                or data.get("close")
                or data.get("last")
            )
            fair_value = _safe_float(
                data.get("fair_value")
                or data.get("intrinsic_value")
                or data.get("target_price")
            )
            upside_percent = _safe_float(
                data.get("upside_percent") or data.get("upside") or data.get("upside_pct")
            )

            value_score = _safe_float(data.get("value_score"))
            quality_score = _safe_float(data.get("quality_score"))
            momentum_score = _safe_float(data.get("momentum_score"))
            opportunity_score = _safe_float(data.get("opportunity_score"))
            data_quality_score = _safe_float(
                data.get("data_quality_score") or data.get("quality")
            )

            risk_bucket = _compute_risk_bucket(opportunity_score, data_quality_score)

            item = AdvancedItem(
                symbol=data.get("symbol")
                or data.get("ticker")
                or data.get("code")
                or symbol,
                name=data.get("name") or data.get("company_name"),
                market=data.get("market") or data.get("exchange"),
                sector=data.get("sector"),
                currency=data.get("currency"),
                price=price,
                fair_value=fair_value,
                upside_percent=upside_percent,
                value_score=value_score,
                quality_score=quality_score,
                momentum_score=momentum_score,
                opportunity_score=opportunity_score,
                data_quality_score=data_quality_score,
                recommendation=(
                    data.get("recommendation_label")
                    or data.get("recommendation")
                    or data.get("rating")
                ),
                risk_label=data.get("risk_label"),
                risk_bucket=risk_bucket,
                provider=data.get("provider") or data.get("primary_provider"),
                data_age_minutes=_safe_float(data.get("data_age_minutes")),
            )

            results.append(item)

        except Exception as exc:
            logger.exception(
                "AdvancedAnalysis: Failed to load/enrich ticker '%s': %s", symbol, exc
            )

    return results


# =============================================================================
# ENDPOINTS
# =============================================================================


@router.get("/ping", summary="Quick health-check for advanced analysis")
async def ping() -> Dict[str, Any]:
    """
    Lightweight ping endpoint to confirm the advanced analysis routes are alive.

    Does NOT require calling any external providers; just returns meta info.
    """
    return {
        "status": "ok",
        "engine_is_stub": _ENGINE_IS_STUB,
        "timestamp_utc": datetime.now(timezone.utc).isoformat(),
        "module": "routes.advanced_analysis",
    }


@router.get(
    "/scoreboard",
    response_model=AdvancedScoreboardResponse,
    summary="Build an advanced opportunity scoreboard for a list of tickers",
)
async def advanced_scoreboard(
    tickers: str = Query(
        ...,
        description=(
            "Comma-separated tickers, e.g. '1120.SR,1180.SR,2222.SR' or 'AAPL,MSFT'. "
            "Spaces are allowed."
        ),
    ),
    top_n: int = Query(
        50,
        ge=1,
        le=500,
        description="Maximum number of rows to return, sorted by Opportunity Score.",
    ),
) -> AdvancedScoreboardResponse:
    """
    Main Advanced Analysis endpoint.

    1) Parse tickers from query string.
    2) Use DataEngine to get enriched quotes per ticker.
    3) Build AdvancedItem list and sort by opportunity_score (desc).
    4) Truncate to top_n and return a clean, Sheets-ready payload.
    """
    # 1) Parse and clean tickers
    raw_list = [t.strip() for t in tickers.split(",") if t.strip()]
    cleaned: List[str] = []
    seen = set()

    for t in raw_list:
        if t not in seen:
            cleaned.append(t)
            seen.add(t)

    if not cleaned:
        raise HTTPException(
            status_code=400, detail="You must provide at least one non-empty ticker."
        )

    # 2) Fetch enriched data
    items = await _fetch_enriched_for_tickers(cleaned)

    if not items:
        # Nothing could be fetched; likely DataEngine is stub or providers failing
        raise HTTPException(
            status_code=502,
            detail=(
                "Failed to retrieve data for all requested tickers. "
                "Check provider API keys, network, or logs."
            ),
        )

    # 3) Sort by opportunity_score (desc), then data_quality_score (desc)
    def _score_key(it: AdvancedItem) -> float:
        opp = it.opportunity_score or 0.0
        dq = it.data_quality_score or 0.0
        # Slight boost for higher data quality
        return opp * 1_000 + dq

    items_sorted = sorted(items, key=_score_key, reverse=True)

    # 4) Truncate to top_n
    top_n_applied = False
    if len(items_sorted) > top_n:
        items_sorted = items_sorted[:top_n]
        top_n_applied = True

    response = AdvancedScoreboardResponse(
        generated_at_utc=datetime.now(timezone.utc).isoformat(),
        engine_is_stub=_ENGINE_IS_STUB,
        total_requested=len(cleaned),
        total_returned=len(items_sorted),
        top_n_applied=top_n_applied,
        tickers=cleaned,
        items=items_sorted,
    )
    return response
