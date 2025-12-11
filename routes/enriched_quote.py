"""
routes/enriched_quote.py
===========================================================
Enriched Quotes Router (v2.8)

- Preferred backend: core.data_engine_v2.DataEngineV2 (class-based engine).
- Fallback backend: core.data_engine (module-level async functions).
- Last-resort: in-process stub engine that always returns MISSING data
  (so the API + Google Sheets never crash even if engines are misconfigured).

KSA Integration
---------------
- For KSA symbols (".SR"), this router can fetch from the dedicated
  Argaam / Tadawul route:

      GET /v1/argaam/quote?symbol=1120

  using BACKEND_BASE_URL + APP_TOKEN (if configured).

  Priority:
      1) If symbol ends with ".SR":
            • Try Argaam route first.
            • If Argaam returns a valid payload, use it directly.
            • If Argaam fails, fall back to the generic engine (v2 / v1).
      2) For non-KSA symbols:
            • Use the configured engine (v2 or v1_module).

Endpoints:
    • /v1/enriched/health
    • /v1/enriched/quote?symbol=...
    • /v1/enriched/quotes        (POST)
    • /v1/enriched/sheet-rows    (POST) – Google Sheets friendly

Google Sheets usage:
    - KSA_Tadawul, Global_Markets, Mutual_Funds, Commodities_FX, My_Portfolio
      all call /v1/enriched/sheet-rows and map directly to the unified
      dashboard structure:

        Identity →
        Price/Liquidity →
        Fundamentals →
        Growth/Profitability →
        Valuation/Risk →
        AI/Technical →
        Meta

Alignment:
    - Header structure is aligned with your MASTER_HEADERS and Universal Template
      so all market pages share the same layout.

Performance v2.8
----------------
- Large batches (e.g. 100+ tickers from Global_Markets) are processed
  in CHUNKS to avoid provider timeouts and 502 errors.

    * Non-KSA tickers: chunked via the engine.
    * KSA tickers: fetched via Argaam one-by-one (short timeout).

- On any batch failure, the route still returns sheet rows with
  data_quality="MISSING" instead of crashing.
"""

from __future__ import annotations

import asyncio
import logging
import os
from datetime import datetime, date
from typing import Any, Dict, List, Optional

import httpx
from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel, Field

logger = logging.getLogger("routes.enriched_quote")

# ----------------------------------------------------------------------
# Optional env.py (for configuration, logging, etc.)
# ----------------------------------------------------------------------

try:  # pragma: no cover - optional
    import env as _env  # type: ignore
except Exception:
    _env = None  # type: ignore

# ----------------------------------------------------------------------
# Back-end base URL + App Token for internal Argaam calls
# ----------------------------------------------------------------------

if _env is not None:
    _BACKEND_BASE_URL = (
        getattr(_env, "BACKEND_BASE_URL", None)
        or getattr(_env, "SERVICE_BASE_URL", None)
        or "https://tadawul-fast-bridge.onrender.com"
    )
    _APP_TOKEN = (
        getattr(_env, "APP_TOKEN", None)
        or getattr(_env, "X_APP_TOKEN", None)
        or ""
    )
else:
    _BACKEND_BASE_URL = os.getenv(
        "BACKEND_BASE_URL", "https://tadawul-fast-bridge.onrender.com"
    )
    _APP_TOKEN = os.getenv("APP_TOKEN", "") or os.getenv("X_APP_TOKEN", "")

_BACKEND_BASE_URL = (_BACKEND_BASE_URL or "").rstrip("/")

# ----------------------------------------------------------------------
# Batch / timeout configuration (to avoid 502 on large requests)
# ----------------------------------------------------------------------

# Max number of non-KSA tickers per engine batch call
_NON_KSA_BATCH_SIZE = 40

# Max timeout (seconds) for one engine batch
_ENGINE_BATCH_TIMEOUT = 30.0

# Timeout for KSA / Argaam single quote (seconds)
_ARGAAM_TIMEOUT = 12.0

# ----------------------------------------------------------------------
# Data engine import (robust with fallbacks)
# ----------------------------------------------------------------------

_ENGINE_MODE: str = "stub"  # "v2", "v1_module", or "stub"
_ENGINE_IS_STUB: bool = False
_engine: Any = None
_data_engine_module: Any = None

try:
    # Preferred new engine (class-based, lives in core.data_engine_v2)
    from core.data_engine_v2 import DataEngineV2 as _V2DataEngine  # type: ignore

    if _env is not None:
        cache_ttl = getattr(_env, "ENGINE_CACHE_TTL_SECONDS", None)
        kwargs: Dict[str, Any] = {}
        if cache_ttl is not None:
            # DataEngineV2 expects cache_ttl_seconds
            kwargs["cache_ttl_seconds"] = cache_ttl
        _engine = _V2DataEngine(**kwargs)
    else:
        _engine = _V2DataEngine()

    _ENGINE_MODE = "v2"
    logger.info("routes.enriched_quote: Using DataEngineV2 from core.data_engine_v2")

except Exception as e_v2:  # pragma: no cover - defensive
    logger.exception(
        "routes.enriched_quote: Failed to import/use core.data_engine_v2.DataEngineV2: %s",
        e_v2,
    )
    try:
        # Fallback: legacy engine module (core.data_engine) which exposes
        # async functions get_enriched_quote / get_enriched_quotes.
        from core import data_engine as _data_engine_module  # type: ignore

        _ENGINE_MODE = "v1_module"
        logger.warning(
            "routes.enriched_quote: Falling back to core.data_engine module-level API"
        )
    except Exception as e_v1:  # pragma: no cover - defensive
        logger.exception(
            "routes.enriched_quote: Failed to import core.data_engine as fallback: %s",
            e_v1,
        )

        class _StubEngine:
            """
            Safe stub engine so the service can start even if the real engine
            is missing. All responses will have data_quality='MISSING'.
            """

            async def get_enriched_quote(self, symbol: str) -> Dict[str, Any]:
                sym = (symbol or "").strip().upper()
                return {
                    "symbol": sym,
                    "data_quality": "MISSING",
                    "error": (
                        "Data engine modules (core.data_engine_v2/core.data_engine) "
                        "are not available or failed to import."
                    ),
                }

            async def get_enriched_quotes(
                self, symbols: List[str]
            ) -> List[Dict[str, Any]]:
                out: List[Dict[str, Any]] = []
                for s in symbols:
                    sym = (s or "").strip().upper()
                    out.append(
                        {
                            "symbol": sym,
                            "data_quality": "MISSING",
                            "error": (
                                "Data engine modules (core.data_engine_v2/core.data_engine) "
                                "are not available or failed to import."
                            ),
                        }
                    )
                return out

        _engine = _StubEngine()
        _ENGINE_MODE = "stub"
        _ENGINE_IS_STUB = True
        logger.error(
            "routes.enriched_quote: Using in-process STUB DataEngine with MISSING data responses."
        )

# ----------------------------------------------------------------------
# Utilities
# ----------------------------------------------------------------------


def _chunk_list(items: List[str], size: int) -> List[List[str]]:
    """Split a list into chunks of max 'size' items."""
    if size <= 0:
        return [items]
    return [items[i : i + size] for i in range(0, len(items), size)]


# ----------------------------------------------------------------------
# Router
# ----------------------------------------------------------------------

router = APIRouter(
    prefix="/v1/enriched",
    tags=["Enriched Quotes"],
)

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
    shares_outstanding: Optional[float] = Field(
        None, description="Shares outstanding, if available"
    )
    free_float: Optional[float] = Field(
        None, description="Free float shares, if available"
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
    data_source: Optional[str] = Field(
        None, description="Primary data source/provider used by the engine"
    )
    provider: Optional[str] = None
    data_quality: str = Field(
        "UNKNOWN",
        description="OK / PARTIAL / MISSING / STALE / UNKNOWN (legacy labels still accepted)",
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
# Internal engine helpers
# ----------------------------------------------------------------------


async def get_enriched_quote_engine(symbol: str) -> Any:
    """
    Thin async wrapper around the configured engine for a single symbol.
    v2 engine is synchronous; v1/stub engines are async.
    """
    sym = (symbol or "").strip()
    if not sym:
        return None

    if _ENGINE_MODE == "v2":
        # DataEngineV2 is synchronous
        return _engine.get_enriched_quote(sym)

    if _ENGINE_MODE == "v1_module" and _data_engine_module is not None:
        # Legacy async module-level engine
        return await _data_engine_module.get_enriched_quote(sym)  # type: ignore[attr-defined]

    # Stub is async
    return await _engine.get_enriched_quote(sym)


async def get_enriched_quotes_engine(symbols: List[str]) -> List[Any]:
    """
    Thin async wrapper for batch quotes (no chunking here).
    """
    clean = [s.strip() for s in (symbols or []) if s and s.strip()]
    if not clean:
        return []

    if _ENGINE_MODE == "v2":
        # DataEngineV2 is synchronous
        return _engine.get_enriched_quotes(clean)

    if _ENGINE_MODE == "v1_module" and _data_engine_module is not None:
        return await _data_engine_module.get_enriched_quotes(clean)  # type: ignore[attr-defined]

    # Stub is async
    return await _engine.get_enriched_quotes(clean)


async def get_enriched_quotes_engine_chunked(symbols: List[str]) -> Dict[str, Any]:
    """
    Chunked batched engine calls to avoid timeouts for large lists (e.g. 100+ tickers).

    Returns a mapping: { symbol: raw_quote_or_None }
    """
    clean = [s.strip() for s in (symbols or []) if s and s.strip()]
    out: Dict[str, Any] = {}

    if not clean:
        return out

    chunks = _chunk_list(clean, _NON_KSA_BATCH_SIZE)
    logger.info(
        "Enriched engine batch: %d symbols in %d chunk(s) (mode=%s)",
        len(clean),
        len(chunks),
        _ENGINE_MODE,
    )

    for chunk in chunks:
        try:
            # Bound time for each chunk so we don't hang forever
            batch = await asyncio.wait_for(
                get_enriched_quotes_engine(chunk),
                timeout=_ENGINE_BATCH_TIMEOUT,
            )
            for t, q in zip(chunk, batch):
                out[t] = q
        except asyncio.TimeoutError:
            logger.warning(
                "Engine batch timeout for chunk of size %d (symbols=%s)",
                len(chunk),
                chunk,
            )
            for t in chunk:
                # Mark as missing; downstream will create MISSING rows
                out.setdefault(t, None)
        except Exception as exc:  # pragma: no cover - defensive
            logger.exception(
                "Engine batch failure for chunk of size %d (symbols=%s): %s",
                len(chunk),
                chunk,
                exc,
            )
            for t in chunk:
                out.setdefault(t, None)

    return out


# ----------------------------------------------------------------------
# KSA / Argaam helper
# ----------------------------------------------------------------------


async def _fetch_ksa_via_argaam(symbol: str) -> Optional[Dict[str, Any]]:
    """
    KSA-specific helper: fetch data for a Tadawul symbol via the
    internal /v1/argaam/quote endpoint.

    Behaviour:
        - Accepts symbol with or without ".SR" (1120 or 1120.SR).
        - Calls: GET {BACKEND_BASE_URL}/v1/argaam/quote?symbol=1120
        - Sends X-APP-TOKEN header if available.
        - Returns a dict compatible with _quote_to_enriched (very tolerant).
    """
    raw = (symbol or "").strip().upper()
    if not raw:
        return None

    # Argaam route expects "1120" (without .SR) based on your usage.
    code = raw[:-3] if raw.endswith(".SR") else raw

    if not _BACKEND_BASE_URL:
        logger.warning(
            "KSA Argaam fetch skipped for %s (%s) – BACKEND_BASE_URL not configured",
            symbol,
            code,
        )
        return None

    url = f"{_BACKEND_BASE_URL}/v1/argaam/quote"
    params = {"symbol": code}
    headers: Dict[str, str] = {}
    if _APP_TOKEN:
        headers["X-APP-TOKEN"] = _APP_TOKEN

    try:
        async with httpx.AsyncClient(timeout=_ARGAAM_TIMEOUT) as client:
            resp = await client.get(url, params=params, headers=headers)

        if resp.status_code != 200:
            text = resp.text
            logger.warning(
                "KSA Argaam HTTP %s for %s (%s): %s",
                resp.status_code,
                symbol,
                code,
                text,
            )
            return None

        data = resp.json()
        if not isinstance(data, dict):
            logger.warning(
                "KSA Argaam payload invalid for %s (%s): %s", symbol, code, data
            )
            return None

        # Ensure a few key fields are present/normalized
        data.setdefault("symbol", raw)
        data.setdefault("market", "KSA")
        data.setdefault("market_region", "KSA")
        data.setdefault("exchange", "TADAWUL")
        data.setdefault("currency", "SAR")
        data.setdefault("timezone", "Asia/Riyadh")
        now = datetime.utcnow()
        data.setdefault("as_of_utc", now)
        data.setdefault("as_of_local", now)
        data.setdefault("provider", "argaam")
        data.setdefault("data_source", "argaam")
        data.setdefault("data_quality", "EXCELLENT")

        return data

    except Exception as exc:  # pragma: no cover - defensive
        logger.exception(
            "KSA Argaam request failed for %s (%s): %s", symbol, code, exc
        )
        return None


# ----------------------------------------------------------------------
# Normalization helpers
# ----------------------------------------------------------------------


def _normalize_scalar(value: Any) -> Any:
    """
    Ensure values that might be datetime/date are converted to ISO strings.
    This keeps FastAPI/Pydantic happy and matches Google Sheets expectations.
    """
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    return value


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

    def g(*keys: str, default: Any = None) -> Any:
        for k in keys:
            if k in data and data[k] is not None:
                return _normalize_scalar(data[k])
        return default

    symbol = str(g("symbol", "ticker", default="")).upper()

    return EnrichedQuoteResponse(
        # Identity
        symbol=symbol,
        name=g("name", "company_name", "longName", "shortName"),
        sector=g("sector"),
        sub_sector=g("sub_sector", "industry"),
        market=g("market", "market_region", "exchange", "exchange_short_name"),
        currency=g("currency"),
        listing_date=g("listing_date", "ipo_date", "IPODate"),
        shares_outstanding=g("shares_outstanding", "sharesOutstanding"),
        free_float=g("free_float", "freeFloat"),
        # Price / liquidity
        last_price=g("last_price", "price", "currentPrice", "regularMarketPrice"),
        previous_close=g("previous_close", "prev_close", "previousClose"),
        open=g("open", "regularMarketOpen"),
        high=g("high", "dayHigh", "regularMarketDayHigh"),
        low=g("low", "dayLow", "regularMarketDayLow"),
        change=g("change"),
        change_percent=g("change_percent", "change_pct", "changePercent"),
        high_52w=g("high_52w", "fifty_two_week_high", "yearHigh"),
        low_52w=g("low_52w", "fifty_two_week_low", "yearLow"),
        position_52w_percent=g("position_52w_percent", "fifty_two_week_position"),
        volume=g("volume", "regularMarketVolume"),
        avg_volume_30d=g("avg_volume_30d", "average_volume_30d", "avg_volume"),
        value_traded=g("value_traded"),
        turnover_rate=g("turnover_rate"),
        bid_price=g("bid_price"),
        ask_price=g("ask_price"),
        bid_size=g("bid_size"),
        ask_size=g("ask_size"),
        spread_percent=g("spread_percent"),
        liquidity_score=g("liquidity_score"),
        # Fundamentals
        eps_ttm=g("eps_ttm", "eps", "trailingEps"),
        pe_ratio=g("pe_ratio", "pe", "pe_ttm", "trailingPE", "PERatio"),
        pb_ratio=g("pb_ratio", "pb", "priceToBook", "P_B"),
        dividend_yield_percent=g(
            "dividend_yield_percent",
            "dividend_yield",
            "dividendYield",
            "DividendYield",
        ),
        dividend_payout_ratio=g("dividend_payout_ratio"),
        roe_percent=g("roe_percent", "roe", "returnOnEquity"),
        roa_percent=g("roa_percent", "roa", "returnOnAssets"),
        debt_to_equity=g("debt_to_equity", "debtToEquity"),
        current_ratio=g("current_ratio"),
        quick_ratio=g("quick_ratio"),
        market_cap=g("market_cap", "marketCap", "MarketCapitalization"),
        # Growth / profitability
        revenue_growth_percent=g(
            "revenue_growth_percent",
            "revenue_growth_yoy",
            "revenueGrowth",
        ),
        net_income_growth_percent=g(
            "net_income_growth_percent",
            "net_income_growth_yoy",
            "earningsGrowth",
        ),
        ebitda_margin_percent=g("ebitda_margin_percent", "ebitda_margin"),
        operating_margin_percent=g(
            "operating_margin_percent",
            "operating_margin",
            "operatingMargins",
        ),
        net_margin_percent=g(
            "net_margin_percent",
            "profit_margin",
            "profitMargins",
        ),
        # Valuation / risk
        ev_to_ebitda=g("ev_to_ebitda"),
        price_to_sales=g(
            "price_to_sales",
            "priceToSales",
            "priceToSalesRatio",
        ),
        price_to_cash_flow=g(
            "price_to_cash_flow",
            "priceToCashFlow",
        ),
        peg_ratio=g("peg_ratio", "pegRatio"),
        beta=g("beta"),
        volatility_30d_percent=g(
            "volatility_30d_percent",
            "volatility_30d",
        ),
        # AI valuation & scores
        fair_value=g("fair_value"),
        upside_percent=g("upside_percent"),
        valuation_label=g("valuation_label"),
        value_score=g("value_score"),
        quality_score=g("quality_score"),
        momentum_score=g("momentum_score"),
        opportunity_score=g("opportunity_score"),
        recommendation=g("recommendation", "rating", "consensusRating"),
        # Technicals
        rsi_14=g("rsi_14"),
        macd=g("macd"),
        ma_20d=g("ma_20d"),
        ma_50d=g("ma_50d"),
        # Meta
        data_source=g("data_source", "provider", "primary_provider", "primary_source"),
        provider=g("provider", "primary_provider", "primary_source"),
        data_quality=g(
            "data_quality",
            "data_quality_level",
            default="UNKNOWN",
        ),
        as_of_utc=g("as_of_utc", "last_updated_utc"),
        as_of_local=g("as_of_local", "last_updated_riyadh", "last_updated_local"),
        timezone=g("timezone"),
        error=g("error"),
    )


def _build_sheet_headers() -> List[str]:
    """
    Headers aligned with your Universal Market Template.
    (Must match the order used by your Google Sheets and Config.gs MASTER_HEADERS.)
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
    provider = e.provider or e.data_source
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
        provider,
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
        "version": "2.8",
        "engine_mode": _ENGINE_MODE,
        "engine_is_stub": _ENGINE_IS_STUB,
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

    is_ksa = ticker.upper().endswith(".SR")

    try:
        # 1) KSA path – try Argaam / Tadawul first
        if is_ksa:
            ksa_raw = await _fetch_ksa_via_argaam(ticker)
            if ksa_raw:
                enriched = _quote_to_enriched(ksa_raw)
                if enriched.data_quality == "MISSING":
                    enriched.error = (
                        enriched.error
                        or "No data available from KSA Argaam providers"
                    )
                return enriched

        # 2) Global / fallback engine path
        quote = await get_enriched_quote_engine(ticker)
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

    results: List[EnrichedQuoteResponse] = []

    # Partition into KSA vs non-KSA for better control
    ksa_tickers = [t for t in tickers if t.upper().endswith(".SR")]
    non_ksa_tickers = [t for t in tickers if not t.upper().endswith(".SR")]

    engine_map: Dict[str, Any] = {}
    ksa_map: Dict[str, Any] = {}

    # Non-KSA via chunked engine
    if non_ksa_tickers:
        engine_map = await get_enriched_quotes_engine_chunked(non_ksa_tickers)

    # KSA via Argaam
    for t in ksa_tickers:
        try:
            ksa_raw = await _fetch_ksa_via_argaam(t)
            ksa_map[t] = ksa_raw
        except Exception as exc:
            logger.exception("Batch KSA Argaam fetch failed for %s", t)
            ksa_map[t] = None

    # Build responses in original order
    for t in tickers:
        raw = ksa_map.get(t) if t.upper().endswith(".SR") else engine_map.get(t)
        try:
            if raw is None:
                enriched = EnrichedQuoteResponse(
                    symbol=t.upper(),
                    data_quality="MISSING",
                    error="No data available from providers or KSA engine",
                )
            else:
                enriched = _quote_to_enriched(raw)
                if enriched.data_quality == "MISSING":
                    enriched.error = (
                        enriched.error
                        or "No data available from providers or KSA engine"
                    )
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
        - Row 5  = headers
        - Row 6+ = values

    This endpoint is the core bridge for the Ultimate Investment Dashboard
    (KSA_Tadawul, Global_Markets, Mutual_Funds, Commodities_FX, My_Portfolio, etc.).
    """
    headers = _build_sheet_headers()
    tickers = [t.strip() for t in (body.tickers or []) if t and t.strip()]

    # Sheets-safe: if no tickers, just return headers + empty rows (no 400).
    if not tickers:
        return SheetEnrichedResponse(headers=headers, rows=[])

    # Partition tickers
    ksa_tickers = [t for t in tickers if t.upper().endswith(".SR")]
    non_ksa_tickers = [t for t in tickers if not t.upper().endswith(".SR")]

    quotes_map: Dict[str, Any] = {}

    # 1) Non-KSA via chunked engine
    if non_ksa_tickers:
        try:
            engine_map = await get_enriched_quotes_engine_chunked(non_ksa_tickers)
            quotes_map.update(engine_map)
        except Exception as exc:
            logger.exception(
                "Enriched sheet-rows engine batch failed for non-KSA tickers=%s",
                non_ksa_tickers,
            )
            for t in non_ksa_tickers:
                quotes_map.setdefault(t, None)

    # 2) KSA via Argaam
    for t in ksa_tickers:
        try:
            ksa_raw = await _fetch_ksa_via_argaam(t)
            quotes_map[t] = ksa_raw
        except Exception as exc:
            logger.exception("Enriched sheet-rows KSA Argaam failed for %s", t)
            quotes_map[t] = None

    # 3) Build rows in original order
    rows: List[List[Any]] = []
    for t in tickers:
        raw = quotes_map.get(t)
        try:
            if raw is None:
                enriched = EnrichedQuoteResponse(
                    symbol=t.upper(),
                    data_quality="MISSING",
                    error="No data available from providers or KSA engine",
                )
            else:
                enriched = _quote_to_enriched(raw)
                if enriched.data_quality == "MISSING":
                    enriched.error = (
                        enriched.error
                        or "No data available from providers or KSA engine"
                    )
            rows.append(_enriched_to_sheet_row(enriched))
        except Exception as exc:
            logger.exception(
                "Exception building sheet row for %s", t, exc_info=exc
            )
            fallback = EnrichedQuoteResponse(
                symbol=t.upper(),
                data_quality="MISSING",
                error=f"Exception building sheet row: {exc}",
            )
            rows.append(_enriched_to_sheet_row(fallback))

    return SheetEnrichedResponse(headers=headers, rows=rows)
