"""
routes/enriched_quote.py
===========================================================
Enriched Quotes Router (v2.0+)

- Uses core.data_engine_v2.DataEngine as the primary backend.
- If not available, falls back to core.data_engine.DataEngine.
- If neither exists, uses a safe stub engine so the API still runs.

KSA (.SR) tickers and global tickers are handled by the engine.
Provides:
    • /v1/enriched/health
    • /v1/enriched/quote?symbol=...
    • /v1/enriched/quotes        (POST)
    • /v1/enriched/sheet-rows    (POST) – Google Sheets friendly
"""

from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel, Field

logger = logging.getLogger(__name__)

# ----------------------------------------------------------------------
# Data engine import (robust with fallbacks)
# ----------------------------------------------------------------------

try:
    # Preferred: new engine
    from core.data_engine_v2 import DataEngine  # type: ignore
    logger.info("Using DataEngine from core.data_engine_v2")
except ModuleNotFoundError:
    try:
        # Fallback: legacy engine
        from core.data_engine import DataEngine  # type: ignore
        logger.warning(
            "core.data_engine_v2 not found; falling back to core.data_engine.DataEngine"
        )
    except ModuleNotFoundError:
        logger.error(
            "No data engine module found (core.data_engine_v2 / core.data_engine). "
            "Using stub DataEngine with MISSING data responses."
        )

        class DataEngine:  # type: ignore[no-redef]
            """
            Safe stub engine so the service can start even if the real engine
            is missing. All responses will have data_quality='MISSING'.
            """

            async def get_enriched_quote(self, symbol: str) -> Dict[str, Any]:
                return {
                    "symbol": symbol,
                    "data_quality": "MISSING",
                    "error": (
                        "Data engine module not found "
                        "(core.data_engine_v2 / core.data_engine)"
                    ),
                }

            async def get_enriched_quotes(
                self, symbols: List[str]
            ) -> List[Dict[str, Any]]:
                return [
                    {
                        "symbol": s,
                        "data_quality": "MISSING",
                        "error": (
                            "Data engine module not found "
                            "(core.data_engine_v2 / core.data_engine)"
                        ),
                    }
                    for s in symbols
                ]


# ----------------------------------------------------------------------
# Router
# ----------------------------------------------------------------------

router = APIRouter(
    prefix="/v1/enriched",
    tags=["Enriched Quotes"],
)

# Single shared engine instance (async-safe usage via await)
_engine = DataEngine()

# ----------------------------------------------------------------------
# Pydantic models (API responses)
# ----------------------------------------------------------------------


class EnrichedQuoteResponse(BaseModel):
    # Identity
    symbol: str = Field(..., description="Ticker symbol, e.g. 1120.SR, AAPL")
    name: Optional[str] = Field(None, description="Company name")
    sector: Optional[str] = Field(None, description="Sector")
    sub_sector: Optional[str] = Field(None, description="Sub-sector / industry")
    market: Optional[str] = Field(None, description="Market / exchange, e.g. TADAWUL")
    currency: Optional[str] = Field(None, description="Trading currency, e.g. SAR, USD")
    listing_date: Optional[str] = Field(
        None, description="Listing date in YYYY-MM-DD (if available)"
    )

    # Price / liquidity
    last_price: Optional[float] = None
    previous_close: Optional[float] = None
    open: Optional[float] = None
    high: Optional[float] = None
    low: Optional[float] = None
    change: Optional[float] = None
    change_percent: Optional[float] = None
    high_52w: Optional[float] = None
    low_52w: Optional[float] = None
    position_52w_percent: Optional[float] = Field(
        None, description="Position between 52W low/high (0–100)"
    )

    volume: Optional[float] = None
    avg_volume_30d: Optional[float] = None
    value_traded: Optional[float] = None
    turnover_rate: Optional[float] = None
    bid_price: Optional[float] = None
    ask_price: Optional[float] = None
    bid_size: Optional[float] = None
    ask_size: Optional[float] = None
    spread_percent: Optional[float] = None
    liquidity_score: Optional[float] = None

    # Fundamentals
    eps_ttm: Optional[float] = None
    pe_ratio: Optional[float] = None
    pb_ratio: Optional[float] = None
    dividend_yield_percent: Optional[float] = None
    dividend_payout_ratio: Optional[float] = None
    roe_percent: Optional[float] = None
    roa_percent: Optional[float] = None
    debt_to_equity: Optional[float] = None
    current_ratio: Optional[float] = None
    quick_ratio: Optional[float] = None
    market_cap: Optional[float] = None

    # Growth / profitability
    revenue_growth_percent: Optional[float] = None
    net_income_growth_percent: Optional[float] = None
    ebitda_margin_percent: Optional[float] = None
    operating_margin_percent: Optional[float] = None
    net_margin_percent: Optional[float] = None

    # Valuation / risk
    ev_to_ebitda: Optional[float] = None
    price_to_sales: Optional[float] = None
    price_to_cash_flow: Optional[float] = None
    peg_ratio: Optional[float] = None
    beta: Optional[float] = None
    volatility_30d_percent: Optional[float] = None

    # AI valuation & scores
    fair_value: Optional[float] = None
    upside_percent: Optional[float] = None
    valuation_label: Optional[str] = None
    value_score: Optional[float] = None
    quality_score: Optional[float] = None
    momentum_score: Optional[float] = None
    opportunity_score: Optional[float] = None
    recommendation: Optional[str] = None  # BUY / HOLD / SELL or similar

    # Technicals
    rsi_14: Optional[float] = None
    macd: Optional[float] = None
    ma_20d: Optional[float] = None
    ma_50d: Optional[float] = None

    # Meta
    provider: Optional[str] = None
    data_quality: str = Field(
        "UNKNOWN",
        description="OK / PARTIAL / MISSING / STALE",
    )
    as_of_utc: Optional[str] = None
    as_of_local: Optional[str] = None
    timezone: Optional[str] = None
    error: Optional[str] = None


class BatchEnrichedRequest(BaseModel):
    tickers: List[str] = Field(
        default_factory=list,
        description="List of symbols, e.g. ['AAPL','MSFT','1120.SR']",
    )


class BatchEnrichedResponse(BaseModel):
    results: List[EnrichedQuoteResponse]


class SheetEnrichedResponse(BaseModel):
    headers: List[str]
    rows: List[List[Any]]


# ----------------------------------------------------------------------
# Internal helpers
# ----------------------------------------------------------------------


async def get_enriched_quote(symbol: str) -> Any:
    """
    Thin async wrapper around the core DataEngine.
    """
    return await _engine.get_enriched_quote(symbol)


async def get_enriched_quotes(symbols: List[str]) -> List[Any]:
    """
    Thin async wrapper for batch quotes.
    """
    return await _engine.get_enriched_quotes(symbols)


def _quote_to_enriched(raw: Any) -> EnrichedQuoteResponse:
    """
    Convert the engine's UnifiedQuote (Pydantic model or dict) into
    EnrichedQuoteResponse. Very defensive – never throws.
    """
    if raw is None:
        return EnrichedQuoteResponse(
            symbol="",
            data_quality="MISSING",
            error="No quote data returned from engine",
        )

    # Allow both Pydantic models and plain dicts
    if hasattr(raw, "model_dump"):
        data = raw.model_dump()
    elif isinstance(raw, dict):
        data = raw
    else:
        # Unknown type – best-effort
        symbol = str(getattr(raw, "symbol", "")).upper()
        return EnrichedQuoteResponse(
            symbol=symbol,
            data_quality="MISSING",
            error=f"Unsupported quote object type: {type(raw)!r}",
        )

    def g(*keys, default=None):
        for k in keys:
            if k in data and data[k] is not None:
                return data[k]
        return default

    symbol = str(g("symbol", "ticker", default="")).upper()

    return EnrichedQuoteResponse(
        # Identity
        symbol=symbol,
        name=g("name", "company_name"),
        sector=g("sector"),
        sub_sector=g("sub_sector", "industry"),
        market=g("market", "exchange"),
        currency=g("currency"),
        listing_date=g("listing_date"),
        # Price / liquidity
        last_price=g("last_price", "price"),
        previous_close=g("previous_close"),
        open=g("open"),
        high=g("high"),
        low=g("low"),
        change=g("change"),
        change_percent=g("change_percent"),
        high_52w=g("high_52w", "fifty_two_week_high"),
        low_52w=g("low_52w", "fifty_two_week_low"),
        position_52w_percent=g("position_52w_percent", "fifty_two_week_position"),
        volume=g("volume"),
        avg_volume_30d=g("avg_volume_30d", "average_volume_30d"),
        value_traded=g("value_traded"),
        turnover_rate=g("turnover_rate"),
        bid_price=g("bid_price"),
        ask_price=g("ask_price"),
        bid_size=g("bid_size"),
        ask_size=g("ask_size"),
        spread_percent=g("spread_percent"),
        liquidity_score=g("liquidity_score"),
        # Fundamentals
        eps_ttm=g("eps_ttm", "eps"),
        pe_ratio=g("pe_ratio", "pe"),
        pb_ratio=g("pb_ratio", "pb"),
        dividend_yield_percent=g("dividend_yield_percent", "dividend_yield"),
        dividend_payout_ratio=g("dividend_payout_ratio"),
        roe_percent=g("roe_percent", "roe"),
        roa_percent=g("roa_percent", "roa"),
        debt_to_equity=g("debt_to_equity"),
        current_ratio=g("current_ratio"),
        quick_ratio=g("quick_ratio"),
        market_cap=g("market_cap"),
        # Growth / profitability
        revenue_growth_percent=g("revenue_growth_percent"),
        net_income_growth_percent=g("net_income_growth_percent"),
        ebitda_margin_percent=g("ebitda_margin_percent"),
        operating_margin_percent=g("operating_margin_percent"),
        net_margin_percent=g("net_margin_percent"),
        # Valuation / risk
        ev_to_ebitda=g("ev_to_ebitda"),
        price_to_sales=g("price_to_sales"),
        price_to_cash_flow=g("price_to_cash_flow"),
        peg_ratio=g("peg_ratio"),
        beta=g("beta"),
        volatility_30d_percent=g("volatility_30d_percent", "volatility_30d"),
        # AI valuation & scores
        fair_value=g("fair_value"),
        upside_percent=g("upside_percent"),
        valuation_label=g("valuation_label"),
        value_score=g("value_score"),
        quality_score=g("quality_score"),
        momentum_score=g("momentum_score"),
        opportunity_score=g("opportunity_score"),
        recommendation=g("recommendation"),
        # Technicals
        rsi_14=g("rsi_14"),
        macd=g("macd"),
        ma_20d=g("ma_20d"),
        ma_50d=g("ma_50d"),
        # Meta
        provider=g("provider", "primary_provider"),
        data_quality=g("data_quality", default="UNKNOWN"),
        as_of_utc=g("as_of_utc"),
        as_of_local=g("as_of_local"),
        timezone=g("timezone"),
        error=g("error"),
    )


def _build_sheet_headers() -> List[str]:
    """
    Headers aligned with your 9-page dashboard philosophy:
    Identity, Price/Liquidity, Fundamentals, Growth/Profitability,
    Valuation/Risk, AI/Technical, Meta.
    """
    return [
        # Identity
        "Symbol",
        "Company Name",
        "Sector",
        "Sub-Sector",
        "Market",
        "Currency",
        "Listing Date",
        # Price / Liquidity
        "Last Price",
        "Previous Close",
        "Open",
        "High",
        "Low",
        "Change",
        "Change %",
        "52 Week High",
        "52 Week Low",
        "52W Position %",
        "Volume",
        "Average Volume (30D)",
        "Value Traded",
        "Turnover Rate",
        "Bid Price",
        "Ask Price",
        "Bid Size",
        "Ask Size",
        "Spread %",
        "Liquidity Score",
        # Fundamentals
        "EPS (TTM)",
        "P/E Ratio",
        "P/B Ratio",
        "Dividend Yield %",
        "Dividend Payout",
        "ROE %",
        "ROA %",
        "Debt/Equity",
        "Current Ratio",
        "Quick Ratio",
        "Market Cap",
        # Growth / Profitability
        "Revenue Growth %",
        "Net Income Growth %",
        "EBITDA Margin %",
        "Operating Margin %",
        "Net Margin %",
        # Valuation / Risk
        "EV/EBITDA",
        "Price/Sales",
        "Price/Cash Flow",
        "PEG Ratio",
        "Beta",
        "Volatility (30D) %",
        # AI Valuation & Scores
        "Fair Value",
        "Upside %",
        "Valuation Label",
        "Value Score",
        "Quality Score",
        "Momentum Score",
        "Opportunity Score",
        "Recommendation",
        # Technical
        "RSI (14)",
        "MACD",
        "Moving Avg (20D)",
        "Moving Avg (50D)",
        # Meta
        "Provider",
        "Data Quality",
        "Last Updated (UTC)",
        "Last Updated (Local)",
        "Timezone",
        "Error",
    ]


def _enriched_to_sheet_row(e: EnrichedQuoteResponse) -> List[Any]:
    """
    Convert EnrichedQuoteResponse to a row aligned with _build_sheet_headers.
    """
    return [
        # Identity
        e.symbol,
        e.name,
        e.sector,
        e.sub_sector,
        e.market,
        e.currency,
        e.listing_date,
        # Price / Liquidity
        e.last_price,
        e.previous_close,
        e.open,
        e.high,
        e.low,
        e.change,
        e.change_percent,
        e.high_52w,
        e.low_52w,
        e.position_52w_percent,
        e.volume,
        e.avg_volume_30d,
        e.value_traded,
        e.turnover_rate,
        e.bid_price,
        e.ask_price,
        e.bid_size,
        e.ask_size,
        e.spread_percent,
        e.liquidity_score,
        # Fundamentals
        e.eps_ttm,
        e.pe_ratio,
        e.pb_ratio,
        e.dividend_yield_percent,
        e.dividend_payout_ratio,
        e.roe_percent,
        e.roa_percent,
        e.debt_to_equity,
        e.current_ratio,
        e.quick_ratio,
        e.market_cap,
        # Growth / Profitability
        e.revenue_growth_percent,
        e.net_income_growth_percent,
        e.ebitda_margin_percent,
        e.operating_margin_percent,
        e.net_margin_percent,
        # Valuation / Risk
        e.ev_to_ebitda,
        e.price_to_sales,
        e.price_to_cash_flow,
        e.peg_ratio,
        e.beta,
        e.volatility_30d_percent,
        # AI Valuation & Scores
        e.fair_value,
        e.upside_percent,
        e.valuation_label,
        e.value_score,
        e.quality_score,
        e.momentum_score,
        e.opportunity_score,
        e.recommendation,
        # Technical
        e.rsi_14,
        e.macd,
        e.ma_20d,
        e.ma_50d,
        # Meta
        e.provider,
        e.data_quality,
        e.as_of_utc,
        e.as_of_local,
        e.timezone,
        e.error,
    ]


# ----------------------------------------------------------------------
# ROUTES
# ----------------------------------------------------------------------


@router.get("/health")
async def enriched_health() -> Dict[str, Any]:
    """
    Simple health check for this module.
    """
    return {
        "status": "ok",
        "module": "enriched_quote",
        "version": "2.0",
    }


@router.get("/quote", response_model=EnrichedQuoteResponse)
async def get_enriched_quote_route(
    symbol: str = Query(..., alias="symbol"),
) -> EnrichedQuoteResponse:
    """
    Get enriched quote for a single symbol.

    Example:
        GET /v1/enriched/quote?symbol=AAPL
        GET /v1/enriched/quote?symbol=1120.SR
    """
    ticker = (symbol or "").strip()
    if not ticker:
        raise HTTPException(status_code=400, detail="Symbol is required")

    try:
        quote = await get_enriched_quote(ticker)
        enriched = _quote_to_enriched(quote)
        if enriched.data_quality == "MISSING":
            enriched.error = enriched.error or "No data available from providers"
        return enriched
    except HTTPException:
        raise
    except Exception as exc:
        logger.exception("Exception in get_enriched_quote_route for %s", ticker)
        # Never break Google Sheets – always return a valid response
        return EnrichedQuoteResponse(
            symbol=ticker.upper(),
            data_quality="MISSING",
            error=f"Exception in enriched quote: {exc}",
        )


@router.post("/quotes", response_model=BatchEnrichedResponse)
async def get_enriched_quotes_route(
    body: BatchEnrichedRequest,
) -> BatchEnrichedResponse:
    """
    Get enriched quotes for multiple symbols.

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
        logger.exception("Batch enriched quotes failed for tickers=%s", tickers)
        # Complete failure – build placeholder entries for all
        return BatchEnrichedResponse(
            results=[
                EnrichedQuoteResponse(
                    symbol=t.upper(),
                    data_quality="MISSING",
                    error=f"Batch enriched quotes failed: {exc}",
                )
                for t in tickers
            ]
        )

    results: List[EnrichedQuoteResponse] = []
    for t, q in zip(tickers, unified_quotes):
        try:
            enriched = _quote_to_enriched(q)
            if enriched.data_quality == "MISSING":
                enriched.error = enriched.error or "No data available from providers"
            results.append(enriched)
        except Exception as exc:
            logger.exception(
                "Exception building enriched quote for %s in batch", t, exc_info=exc
            )
            results.append(
                EnrichedQuoteResponse(
                    symbol=t.upper(),
                    data_quality="MISSING",
                    error=f"Exception building enriched quote: {exc}",
                )
            )

    return BatchEnrichedResponse(results=results)


@router.post("/sheet-rows", response_model=SheetEnrichedResponse)
async def get_enriched_sheet_rows(
    body: BatchEnrichedRequest,
) -> SheetEnrichedResponse:
    """
    Google Sheets–friendly endpoint.

    Returns:
        {
          "headers": [...],
          "rows": [ [row for t1], [row for t2], ... ]
        }

    Apps Script usage pattern:
        - First row = headers
        - Following rows = values
    """
    batch = await get_enriched_quotes_route(body)
    headers = _build_sheet_headers()
    rows: List[List[Any]] = [_enriched_to_sheet_row(e) for e in batch.results]
    return SheetEnrichedResponse(headers=headers, rows=rows)
