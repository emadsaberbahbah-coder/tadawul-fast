"""
routes/advanced_analysis.py
================================================
TADAWUL FAST BRIDGE – ADVANCED ANALYSIS ROUTES

Purpose:
- Expose "advanced analysis" and "opportunity ranking" endpoints
  on top of the unified DataEngine (core.data_engine_v2 if available).
- Designed to be Google Sheets–friendly and API-friendly.

Key ideas:
- Prefer DataEngine v2 (class-based, multi-provider).
- If v2 is missing, fall back to legacy core.data_engine module-level API.
- If both fail, use a STUB engine (always returns MISSING/placeholder data).
- Take a list of tickers, fetch enriched quotes, and build a
  scoreboard sorted by Opportunity Score + Data Quality.
- Compute a simple risk bucket based on Opportunity & Data Quality.

Typical usage from main.py:
    from routes import advanced_analysis
    app.include_router(advanced_analysis.router)

Endpoints:
- GET  /v1/advanced/ping      (alias: /health)
- GET  /v1/advanced/scoreboard?tickers=AAPL,MSFT,GOOGL&top_n=50
- POST /v1/advanced/sheet-rows   (Google Sheets–friendly scoreboard)
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel, Field

logger = logging.getLogger("routes.advanced_analysis")

# =============================================================================
# Optional env.py integration (for provider config / logging only)
# =============================================================================

try:  # pragma: no cover - optional
    import env as _env  # type: ignore
except Exception:
    _env = None  # type: ignore


# =============================================================================
# DATA ENGINE IMPORT & FALLBACKS (aligned with enriched_quote / ai_analysis)
# =============================================================================

_ENGINE_MODE: str = "stub"  # "v2", "v1_module", or "stub"
_ENGINE_IS_STUB: bool = False
_engine: Any = None
_data_engine_module: Any = None

try:
    # Preferred: new v2 engine (class-based)
    from core.data_engine_v2 import DataEngine as _V2DataEngine  # type: ignore

    if _env is not None:
        # Use env-provided config when available
        cache_ttl = getattr(_env, "ENGINE_CACHE_TTL_SECONDS", None)
        enabled_providers = getattr(_env, "ENABLED_PROVIDERS", None)
        enable_adv = getattr(_env, "ENGINE_ENABLE_ADVANCED_ANALYSIS", True)
        _engine = _V2DataEngine(
            cache_ttl=cache_ttl,
            enabled_providers=enabled_providers,
            enable_advanced_analysis=enable_adv,
        )
    else:
        _engine = _V2DataEngine()

    _ENGINE_MODE = "v2"
    logger.info("AdvancedAnalysis: using DataEngine v2 from core.data_engine_v2")

except Exception as exc_v2:  # pragma: no cover - defensive
    logger.exception(
        "AdvancedAnalysis: Failed to import/use core.data_engine_v2.DataEngine: %s",
        exc_v2,
    )
    try:
        # Fallback: legacy engine module with async functions:
        #   get_enriched_quote / get_enriched_quotes
        from core import data_engine as _data_engine_module  # type: ignore

        _ENGINE_MODE = "v1_module"
        logger.warning(
            "AdvancedAnalysis: Falling back to core.data_engine module-level API"
        )
    except Exception as exc_v1:  # pragma: no cover - defensive
        logger.exception(
            "AdvancedAnalysis: Failed to import core.data_engine as fallback: %s",
            exc_v1,
        )

        class _StubEngine:
            """
            Stub DataEngine used ONLY when the real engine fails to import.
            Always returns placeholder / missing data.
            """

            async def get_enriched_quote(self, ticker: str) -> Dict[str, Any]:
                sym = (ticker or "").strip().upper()
                return {
                    "symbol": sym,
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
                    "data_quality": "MISSING",
                    "recommendation": "MISSING",
                    "risk_label": "UNKNOWN",
                    "provider": None,
                    "as_of_utc": None,
                    "notes": (
                        "STUB_ENGINE: DataEngine could not be imported on backend."
                    ),
                }

            async def get_enriched_quotes(
                self, symbols: List[str]
            ) -> List[Dict[str, Any]]:
                out: List[Dict[str, Any]] = []
                for s in symbols:
                    out.append(await self.get_enriched_quote(s))
                return out

        _engine = _StubEngine()
        _ENGINE_MODE = "stub"
        _ENGINE_IS_STUB = True
        logger.error(
            "AdvancedAnalysis: Using STUB DataEngine – all responses will contain "
            "MISSING placeholder values."
        )


# =============================================================================
# ENGINE WRAPPERS (unify v2 / v1_module / stub behaviour)
# =============================================================================


async def _engine_get_quote(symbol: str) -> Any:
    """
    Unified wrapper for a single quote.
    """
    sym = (symbol or "").strip()
    if not sym:
        return None

    if _ENGINE_MODE == "v2":
        return await _engine.get_enriched_quote(sym)
    if _ENGINE_MODE == "v1_module" and _data_engine_module is not None:
        return await _data_engine_module.get_enriched_quote(sym)  # type: ignore[attr-defined]
    # stub path
    return await _engine.get_enriched_quote(sym)


async def _engine_get_quotes(symbols: List[str]) -> List[Any]:
    """
    Unified wrapper for multiple quotes.
    """
    clean = [s.strip() for s in (symbols or []) if s and s.strip()]
    if not clean:
        return []

    if _ENGINE_MODE == "v2":
        return await _engine.get_enriched_quotes(clean)
    if _ENGINE_MODE == "v1_module" and _data_engine_module is not None:
        return await _data_engine_module.get_enriched_quotes(clean)  # type: ignore[attr-defined]
    # stub path
    return await _engine.get_enriched_quotes(clean)


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
        None, description="Text label e.g. STRONG_BUY / BUY / HOLD / SELL"
    )
    risk_label: Optional[str] = Field(
        None, description="Optional risk category from engine, if available"
    )
    risk_bucket: Optional[str] = Field(
        None,
        description=(
            "Derived bucket combining opportunity & data quality, "
            "e.g. 'HIGH_OPP_HIGH_CONF', 'MED_OPP_HIGH_CONF', 'LOW_CONFIDENCE', etc."
        ),
    )

    provider: Optional[str] = Field(
        None, description="Primary data provider/source if available"
    )
    data_age_minutes: Optional[float] = Field(
        None, description="Approx. age of quote in minutes (if as_of_utc is provided)"
    )


class AdvancedScoreboardResponse(BaseModel):
    """
    Top-level response model for /v1/advanced/scoreboard.
    """

    generated_at_utc: str = Field(
        ..., description="ISO datetime (UTC) when the scoreboard was generated"
    )
    engine_mode: str = Field(
        ..., description="Which engine mode was used (v2/v1_module/stub)"
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


class AdvancedSheetRequest(BaseModel):
    """
    Request body for /v1/advanced/sheet-rows.
    Follows the same pattern as enriched/analysis sheet endpoints.
    """

    tickers: List[str] = Field(
        default_factory=list,
        description="List of symbols, e.g. ['AAPL','MSFT','1120.SR']",
    )
    top_n: Optional[int] = Field(
        50,
        description="Optional cap on number of rows (default 50).",
        ge=1,
        le=500,
    )


class AdvancedSheetResponse(BaseModel):
    headers: List[str]
    rows: List[List[Any]]


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


def _extract_data_quality_score(data: Dict[str, Any]) -> Optional[float]:
    """
    Try to derive a numeric data_quality_score from:
        - explicit numeric field (data_quality_score)
        - or string label (data_quality / data_quality_level)
    """
    dq_score = _safe_float(
        data.get("data_quality_score") or data.get("quality_score_numeric")
    )
    if dq_score is not None:
        return dq_score

    dq_str_raw = (
        data.get("data_quality") or data.get("data_quality_level") or ""
    )
    dq_str = str(dq_str_raw).strip().upper()

    if not dq_str:
        return None

    mapping = {
        "EXCELLENT": 90.0,
        "OK": 80.0,
        "GOOD": 75.0,
        "FAIR": 55.0,
        "PARTIAL": 50.0,
        "STALE": 40.0,
        "POOR": 30.0,
        "MISSING": 20.0,
        "UNKNOWN": 30.0,
    }
    return mapping.get(dq_str, 30.0)


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


def _compute_data_age_minutes(as_of_utc: Optional[str]) -> Optional[float]:
    """
    Approximate age of data in minutes based on as_of_utc ISO string.
    """
    if not as_of_utc:
        return None
    try:
        ts = datetime.fromisoformat(as_of_utc)
        if ts.tzinfo is None:
            ts = ts.replace(tzinfo=timezone.utc)
        now = datetime.now(timezone.utc)
        diff = now - ts
        return round(diff.total_seconds() / 60.0, 2)
    except Exception:
        return None


async def _fetch_enriched_for_tickers(tickers: List[str]) -> List[AdvancedItem]:
    """
    For each ticker, call the engine and map it into AdvancedItem.
    Uses batch engine call when available for better performance.
    """
    if not tickers:
        return []

    unified_quotes = await _engine_get_quotes(tickers)
    results: List[AdvancedItem] = []

    for raw_symbol, enriched in zip(tickers, unified_quotes):
        symbol = (raw_symbol or "").strip()
        if not symbol:
            continue

        try:
            # DataEngine v2 likely returns a Pydantic model; fall back to dict.
            if hasattr(enriched, "model_dump"):
                data = enriched.model_dump()  # type: ignore[call-arg]
            elif isinstance(enriched, dict):
                data = enriched
            else:
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
                data.get("upside_percent")
                or data.get("upside")
                or data.get("upside_pct")
            )

            value_score = _safe_float(data.get("value_score"))
            quality_score = _safe_float(data.get("quality_score"))
            momentum_score = _safe_float(data.get("momentum_score"))
            opportunity_score = _safe_float(data.get("opportunity_score"))
            data_quality_score = _extract_data_quality_score(data)

            as_of_utc = (
                data.get("as_of_utc")
                or data.get("last_updated_utc")
                or data.get("timestamp_utc")
            )
            if isinstance(as_of_utc, datetime):
                as_of_str = as_of_utc.isoformat()
            else:
                as_of_str = str(as_of_utc) if as_of_utc else None
            data_age_minutes = _compute_data_age_minutes(as_of_str)

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
                data_age_minutes=data_age_minutes,
            )

            results.append(item)

        except Exception as exc:  # pragma: no cover - defensive
            logger.exception(
                "AdvancedAnalysis: Failed to load/enrich ticker '%s': %s", symbol, exc
            )

    return results


def _score_key(it: AdvancedItem) -> float:
    """
    Sorting key: primarily Opportunity Score, then Data Quality Score.

    - We multiply opportunity_score by 1000 to give it more weight.
    """
    opp = it.opportunity_score or 0.0
    dq = it.data_quality_score or 0.0
    # Slight boost for higher data quality
    return opp * 1_000 + dq


def _build_sheet_headers() -> List[str]:
    """
    Headers for the Google Sheets scoreboard view.

    This is designed to integrate cleanly with the 9-page dashboard
    (especially Insights_Analysis / Investment_Advisor) while being
    reusable for any "Top Opportunities" page.
    """
    return [
        "Symbol",
        "Company Name",
        "Market",
        "Sector",
        "Currency",
        "Price",
        "Fair Value",
        "Upside %",
        "Value Score",
        "Quality Score",
        "Momentum Score",
        "Opportunity Score",
        "Data Quality Score",
        "Recommendation",
        "Risk Label",
        "Risk Bucket",
        "Provider",
        "Data Age (Minutes)",
    ]


def _advanced_item_to_row(item: AdvancedItem) -> List[Any]:
    """
    Flatten AdvancedItem into a pure row for Sheets (setValues-safe).
    """
    return [
        item.symbol,
        item.name,
        item.market,
        item.sector,
        item.currency,
        item.price,
        item.fair_value,
        item.upside_percent,
        item.value_score,
        item.quality_score,
        item.momentum_score,
        item.opportunity_score,
        item.data_quality_score,
        item.recommendation,
        item.risk_label,
        item.risk_bucket,
        item.provider,
        item.data_age_minutes,
    ]


# =============================================================================
# ENDPOINTS
# =============================================================================


@router.get("/ping", summary="Quick health-check for advanced analysis")
@router.get("/health", summary="Quick health-check for advanced analysis (alias)")
async def ping() -> Dict[str, Any]:
    """
    Lightweight ping/health endpoint to confirm the advanced analysis routes are alive.

    Does NOT require calling any external providers; just returns meta info.
    """
    return {
        "status": "ok",
        "engine_mode": _ENGINE_MODE,
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
    items_sorted = sorted(items, key=_score_key, reverse=True)

    # 4) Truncate to top_n
    top_n_applied = False
    if len(items_sorted) > top_n:
        items_sorted = items_sorted[:top_n]
        top_n_applied = True

    response = AdvancedScoreboardResponse(
        generated_at_utc=datetime.now(timezone.utc).isoformat(),
        engine_mode=_ENGINE_MODE,
        engine_is_stub=_ENGINE_IS_STUB,
        total_requested=len(cleaned),
        total_returned=len(items_sorted),
        top_n_applied=top_n_applied,
        tickers=cleaned,
        items=items_sorted,
    )
    return response


@router.post(
    "/sheet-rows",
    response_model=AdvancedSheetResponse,
    summary="Google Sheets–friendly advanced scoreboard (Top Opportunities view)",
)
async def advanced_sheet_rows(body: AdvancedSheetRequest) -> AdvancedSheetResponse:
    """
    Google Sheets–friendly endpoint.

    Request:
        {
          "tickers": ["AAPL","MSFT","1120.SR"],
          "top_n": 50
        }

    Response:
        {
          "headers": [...],
          "rows": [ [...], [...], ... ]
        }

    - Mirrors /v1/advanced/scoreboard but guarantees a 200-style body
      for Sheets (no HTTPException on empty data – returns headers + zero rows).
    - Designed for:
        • Insights_Analysis sheet
        • Investment_Advisor / Top Opportunities sheets
        • Any ranking layer on top of the 9-page dashboard.
    """
    tickers = [t.strip() for t in (body.tickers or []) if t and t.strip()]
    if not tickers:
        # For Sheets, return headers + no rows instead of 400.
        return AdvancedSheetResponse(headers=_build_sheet_headers(), rows=[])

    top_n = body.top_n or 50

    items = await _fetch_enriched_for_tickers(tickers)
    if not items:
        # Providers completely failed – still return headers, no rows.
        return AdvancedSheetResponse(headers=_build_sheet_headers(), rows=[])

    items_sorted = sorted(items, key=_score_key, reverse=True)
    if len(items_sorted) > top_n:
        items_sorted = items_sorted[:top_n]

    rows: List[List[Any]] = [_advanced_item_to_row(it) for it in items_sorted]
    return AdvancedSheetResponse(headers=_build_sheet_headers(), rows=rows)
