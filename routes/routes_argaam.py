"""
routes_argaam.py
============================================================
KSA / Argaam routes for Tadawul Fast Bridge – v2.3

GOAL
- Dedicated KSA provider route that does NOT depend on EODHD.
- Uses your external Argaam/Tadawul "gateway" service (Java or similar)
  as a data source for .SR symbols.
- Aligned with:
    • main.py                (FastAPI app, token auth, rate limiting)
    • env.py                 (central config, incl. ARGAAM_GATEWAY_URL)
    • google_sheets_service  (sheet-rows pattern: headers + rows)
    • 9-page Google Sheets dashboard (KSA_Tadawul page in particular)

ASSUMPTIONS
- The KSA gateway exposes an HTTP JSON API like:

      GET {ARGAAM_GATEWAY_URL}/quote?symbol=1120.SR

  returning a JSON object with common stock fields.

ENV (env.py or Render environment variables)
- APP_NAME
- APP_VERSION
- ARGAAM_GATEWAY_URL  -> e.g. https://your-ksa-gateway.example.com
- ARGAAM_API_KEY      -> optional, sent as header X-API-KEY

EXPOSED ENDPOINTS
-----------------
    GET  /v1/argaam/health
    GET  /v1/argaam/quote?symbol=1120.SR
    GET  /v1/argaam/quotes?tickers=1120.SR,1180.SR
    POST /v1/argaam/sheet-rows   { "tickers": ["1120.SR","1180.SR"] }

SHEET CONTRACT
--------------
- /sheet-rows returns:

    {
      "headers": [...],
      "rows": [[...], ...]
    }

- Headers are aligned with the unified "market template" for:
    • KSA_Tadawul
    • Global_Markets
    • Mutual_Funds
    • Commodities_FX
    • Insights_Analysis

- This route typically fills:
    • Identity
    • Price/Liquidity
    • Market Cap
    • Meta (provider, data quality, timestamps, error)
  All other fields are left blank (None) so that the 9-page
  Apps Script + Sheets logic stays compatible.

IMPORTANT
---------
- This module NEVER calls EODHD directly.
- All KSA data comes from your Argaam/Tadawul gateway only.
"""

from __future__ import annotations

import asyncio
import logging
import os
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional

import httpx
from fastapi import APIRouter, HTTPException, Query, status
from pydantic import BaseModel, Field, ConfigDict

# Use a stable logger name to match production logging config
logger = logging.getLogger("routes_argaam")

# ======================================================================
# ENV CONFIG – env.py preferred, environment variables as fallback
# ======================================================================

APP_NAME: str = os.getenv("APP_NAME", "tadawul-fast-bridge")
APP_VERSION: str = os.getenv("APP_VERSION", "4.0.0")
ARGAAM_GATEWAY_URL: str = os.getenv("ARGAAM_GATEWAY_URL", "").strip()
ARGAAM_API_KEY: str = os.getenv("ARGAAM_API_KEY", "").strip()

try:  # pragma: no cover - optional env.py
    from env import (  # type: ignore
        APP_NAME as _ENV_APP_NAME,
        APP_VERSION as _ENV_APP_VERSION,
        ARGAAM_GATEWAY_URL as _ENV_GATEWAY_URL,
        ARGAAM_API_KEY as _ENV_API_KEY,
    )

    if _ENV_APP_NAME:
        APP_NAME = _ENV_APP_NAME
    if _ENV_APP_VERSION:
        APP_VERSION = _ENV_APP_VERSION
    if _ENV_GATEWAY_URL:
        ARGAAM_GATEWAY_URL = _ENV_GATEWAY_URL.strip()
    if _ENV_API_KEY:
        ARGAAM_API_KEY = _ENV_API_KEY.strip()

    logger.info("routes_argaam: Config loaded from env.py.")
except Exception:
    logger.warning(
        "routes_argaam: env.py not available or failed to import. "
        "Using environment variables for APP_NAME/APP_VERSION/"
        "ARGAAM_GATEWAY_URL/ARGAAM_API_KEY."
    )

# Optional configurable timeout (seconds) via env, default 15s
try:
    _ARGAAM_TIMEOUT: float = float(os.getenv("ARGAAM_TIMEOUT", "15.0"))
except Exception:
    _ARGAAM_TIMEOUT = 15.0

# ======================================================================
# ROUTER & TIMEZONE
# ======================================================================

router = APIRouter(
    prefix="/v1/argaam",
    tags=["KSA / Argaam"],
)

RIYADH_TZ = timezone(timedelta(hours=3))

__all__ = ["router"]  # explicit export for main.py / auto-discovery

# ======================================================================
# SYMBOL HELPERS (aligned with DataEngine conventions)
# ======================================================================


def _normalize_symbol(symbol: str) -> str:
    """
    Normalize KSA-style symbols to unified format:

      - TADAWUL:1120   -> 1120.SR
      - 1120.TADAWUL   -> 1120.SR
      - 1120           -> 1120.SR
      - 1120.SR        -> 1120.SR
    """
    s = (symbol or "").strip().upper()
    if not s:
        return ""

    if s.startswith("TADAWUL:"):
        s = s.split(":", 1)[1].strip()

    if s.endswith(".TADAWUL"):
        s = s[: -len(".TADAWUL")] + ".SR"

    if s.isdigit():
        s = f"{s}.SR"

    return s


def _is_ksa_symbol(symbol: str) -> bool:
    s = (symbol or "").strip().upper()
    return bool(s) and (s.endswith(".SR") or s.endswith(".TADAWUL") or s.isdigit())


# ======================================================================
# MODELS
# ======================================================================


class ArgaamQuote(BaseModel):
    """
    Normalized KSA quote using your Argaam / Tadawul gateway.

    NOTE:
    - remote_raw holds the original gateway JSON for debugging/mapping.
    - error is used when the gateway call fails; row still returned
      (especially important for Google Sheets safety).
    """

    model_config = ConfigDict(populate_by_name=True)

    # Identity
    symbol: str
    name: Optional[str] = None
    sector: Optional[str] = None
    sub_sector: Optional[str] = Field(default=None, alias="subSector")
    market: Optional[str] = None
    currency: Optional[str] = None

    # Price / liquidity
    last_price: Optional[float] = None
    previous_close: Optional[float] = None
    open_price: Optional[float] = None
    high_price: Optional[float] = None
    low_price: Optional[float] = None
    change: Optional[float] = None
    change_percent: Optional[float] = None

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

    market_cap: Optional[float] = None

    fifty_two_week_high: Optional[float] = Field(
        default=None, alias="fiftyTwoWeekHigh"
    )
    fifty_two_week_low: Optional[float] = Field(
        default=None, alias="fiftyTwoWeekLow"
    )

    # Timestamps
    last_updated_utc: Optional[datetime] = None
    last_updated_riyadh: Optional[datetime] = None

    # Meta
    data_source: str = "argaam-gateway"
    data_quality: Optional[str] = None
    remote_raw: Optional[Dict[str, Any]] = None
    error: Optional[str] = None


class ArgaamSheetRowsRequest(BaseModel):
    tickers: List[str] = Field(
        default_factory=list,
        description="List of KSA symbols (['1120.SR','1180.SR']). Non-KSA are ignored.",
    )


class ArgaamSheetRowsResponse(BaseModel):
    """
    Simple sheet-rows contract:
        { "headers": [...], "rows": [[...], ...] }

    Aligned with:
      - /v1/enriched/sheet-rows
      - /v1/analysis/sheet-rows
      - /v1/advanced/sheet-rows
    """

    headers: List[str]
    rows: List[List[Any]]


# ======================================================================
# INTERNAL HELPERS
# ======================================================================


def _ensure_gateway_configured() -> None:
    if not ARGAAM_GATEWAY_URL:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail=(
                "ARGAAM_GATEWAY_URL is not configured. "
                "Set it in env.py or Render environment variables."
            ),
        )


def _safe_float(x: Any) -> Optional[float]:
    try:
        if x is None or x == "":
            return None
        return float(x)
    except Exception:
        return None


def _parse_timestamp(value: Any) -> Optional[datetime]:
    if value is None:
        return None

    # ISO string (with or without Z)
    if isinstance(value, str):
        try:
            return datetime.fromisoformat(value.replace("Z", "+00:00"))
        except Exception:
            return None

    # Unix seconds
    if isinstance(value, (int, float)):
        try:
            return datetime.fromtimestamp(float(value), tz=timezone.utc)
        except Exception:
            return None

    return None


def _parse_argaam_payload(symbol: str, payload: Dict[str, Any]) -> ArgaamQuote:
    """
    Map the gateway JSON into ArgaamQuote.

    We try several common key names from stock APIs; if your gateway
    uses different fields, only this function needs to be adjusted.
    """

    # Identity
    name = payload.get("name") or payload.get("companyName") or payload.get("Name")
    sector = payload.get("sector") or payload.get("Sector")
    sub_sector = (
        payload.get("subSector")
        or payload.get("sub_sector")
        or payload.get("SubSector")
    )
    market = payload.get("market") or payload.get("exchange") or "Tadawul"
    currency = payload.get("currency") or payload.get("Currency") or "SAR"

    # Prices & change
    last_price = (
        payload.get("lastPrice")
        or payload.get("last")
        or payload.get("price")
        or payload.get("LastPrice")
    )
    prev_close = payload.get("previousClose") or payload.get("prevClose")
    open_price = payload.get("open") or payload.get("Open")
    high_price = payload.get("high") or payload.get("High")
    low_price = payload.get("low") or payload.get("Low")
    change_val = payload.get("change") or payload.get("Change")
    change_pct = (
        payload.get("changePercent")
        or payload.get("changePct")
        or payload.get("ChangePercent")
    )

    # Volume & liquidity
    volume = payload.get("volume") or payload.get("Volume")
    avg_volume_30d = (
        payload.get("averageVolume30d")
        or payload.get("avgVolume30d")
        or payload.get("AvgVolume30D")
    )
    value_traded = (
        payload.get("valueTraded")
        or payload.get("value_traded")
        or payload.get("ValueTraded")
    )
    turnover_rate = (
        payload.get("turnoverRate")
        or payload.get("TurnoverRate")
        or payload.get("turnover")
    )
    bid_price = payload.get("bid") or payload.get("BidPrice")
    ask_price = payload.get("ask") or payload.get("AskPrice")
    bid_size = payload.get("bidSize") or payload.get("BidSize")
    ask_size = payload.get("askSize") or payload.get("AskSize")
    spread_pct = (
        payload.get("spreadPercent")
        or payload.get("spread_pct")
        or payload.get("SpreadPercent")
    )
    liquidity_score = (
        payload.get("liquidityScore")
        or payload.get("LiquidityScore")
    )

    # Market Cap
    market_cap = payload.get("marketCap") or payload.get("MarketCap")

    # 52-week
    high_52w = (
        payload.get("fiftyTwoWeekHigh")
        or payload.get("fifty_two_week_high")
        or payload.get("52WeekHigh")
    )
    low_52w = (
        payload.get("fiftyTwoWeekLow")
        or payload.get("fifty_two_week_low")
        or payload.get("52WeekLow")
    )

    # Last updated
    ts = (
        payload.get("lastUpdated")
        or payload.get("lastUpdate")
        or payload.get("timestamp")
        or payload.get("LastUpdated")
    )
    last_utc = _parse_timestamp(ts)
    last_riyadh: Optional[datetime] = None
    if last_utc:
        if last_utc.tzinfo is None:
            last_utc = last_utc.replace(tzinfo=timezone.utc)
        last_riyadh = last_utc.astimezone(RIYADH_TZ)

    return ArgaamQuote(
        symbol=_normalize_symbol(symbol),
        name=name,
        sector=sector,
        sub_sector=sub_sector,
        market=market,
        currency=currency,
        last_price=_safe_float(last_price),
        previous_close=_safe_float(prev_close),
        open_price=_safe_float(open_price),
        high_price=_safe_float(high_price),
        low_price=_safe_float(low_price),
        change=_safe_float(change_val),
        change_percent=_safe_float(change_pct),
        volume=_safe_float(volume),
        avg_volume_30d=_safe_float(avg_volume_30d),
        value_traded=_safe_float(value_traded),
        turnover_rate=_safe_float(turnover_rate),
        bid_price=_safe_float(bid_price),
        ask_price=_safe_float(ask_price),
        bid_size=_safe_float(bid_size),
        ask_size=_safe_float(ask_size),
        spread_percent=_safe_float(spread_pct),
        liquidity_score=_safe_float(liquidity_score),
        market_cap=_safe_float(market_cap),
        fifty_two_week_high=_safe_float(high_52w),
        fifty_two_week_low=_safe_float(low_52w),
        last_updated_utc=last_utc,
        last_updated_riyadh=last_riyadh,
        remote_raw=payload,
    )


async def _fetch_argaam_quote(symbol: str) -> ArgaamQuote:
    """
    Call the external KSA/Argaam gateway for a single symbol.

    EXPECTED GATEWAY CONTRACT (example):
        GET {ARGAAM_GATEWAY_URL}/quote?symbol=1120.SR
    """
    _ensure_gateway_configured()

    norm = _normalize_symbol(symbol)
    if not _is_ksa_symbol(norm):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=(
                "routes_argaam only supports KSA symbols "
                "ending with '.SR', '.TADAWUL', or numeric codes."
            ),
        )

    url = ARGAAM_GATEWAY_URL.rstrip("/") + "/quote"
    params = {"symbol": norm}
    headers: Dict[str, str] = {"Accept": "application/json"}
    if ARGAAM_API_KEY:
        headers["X-API-KEY"] = ARGAAM_API_KEY

    timeout = httpx.Timeout(_ARGAAM_TIMEOUT, connect=5.0)

    async with httpx.AsyncClient(timeout=timeout) as client:
        try:
            resp = await client.get(url, params=params, headers=headers)
        except httpx.RequestError as exc:
            logger.error("routes_argaam: Request error for %s: %s", norm, exc)
            raise HTTPException(
                status_code=status.HTTP_502_BAD_GATEWAY,
                detail=f"Error connecting to Argaam gateway: {exc}",
            ) from exc

    if resp.status_code < 200 or resp.status_code >= 300:
        logger.error(
            "routes_argaam: Gateway HTTP %s for %s: %s",
            resp.status_code,
            norm,
            resp.text[:300],
        )
        raise HTTPException(
            status_code=status.HTTP_502_BAD_GATEWAY,
            detail=f"Argaam gateway HTTP {resp.status_code}: {resp.text[:200]}",
        )

    try:
        payload = resp.json()
    except Exception as exc:
        logger.error("routes_argaam: Invalid JSON for %s: %s", norm, exc)
        raise HTTPException(
            status_code=status.HTTP_502_BAD_GATEWAY,
            detail=f"Argaam gateway returned invalid JSON: {exc}",
        ) from exc

    # Some gateways might return a list; take the first item
    if isinstance(payload, list):
        if not payload:
            raise HTTPException(
                status_code=status.HTTP_502_BAD_GATEWAY,
                detail="Argaam gateway returned empty list.",
            )
        payload = payload[0]

    if not isinstance(payload, dict):
        raise HTTPException(
            status_code=status.HTTP_502_BAD_GATEWAY,
            detail=f"Unexpected Argaam gateway response type: {type(payload)}",
        )

    quote = _parse_argaam_payload(norm, payload)

    # Simple local data-quality tag
    if quote.error:
        quote.data_quality = "MISSING"
    elif quote.last_price is not None:
        quote.data_quality = "OK"
    else:
        quote.data_quality = "PARTIAL"

    return quote


async def _fetch_argaam_quotes_bulk(symbols: List[str]) -> List[ArgaamQuote]:
    """
    Fetch multiple KSA quotes concurrently from the gateway.

    VERY DEFENSIVE:
    - Never raises in normal use; instead returns ArgaamQuote with error text
      for any symbol that fails. This keeps Google Sheets safe.
    """
    if not symbols:
        return []

    normalized = [_normalize_symbol(s) for s in symbols]
    tasks = [asyncio.create_task(_fetch_argaam_quote(sym)) for sym in normalized]
    results = await asyncio.gather(*tasks, return_exceptions=True)

    quotes: List[ArgaamQuote] = []
    for sym, res in zip(normalized, results):
        if isinstance(res, Exception):
            msg = f"Argaam gateway error: {res}"
            logger.error("routes_argaam: Error fetching %s: %s", sym, res)
            quotes.append(
                ArgaamQuote(
                    symbol=_normalize_symbol(sym),
                    name=None,
                    sector=None,
                    sub_sector=None,
                    market="Tadawul",
                    currency="SAR",
                    last_price=None,
                    previous_close=None,
                    open_price=None,
                    high_price=None,
                    low_price=None,
                    change=None,
                    change_percent=None,
                    volume=None,
                    avg_volume_30d=None,
                    value_traded=None,
                    turnover_rate=None,
                    bid_price=None,
                    ask_price=None,
                    bid_size=None,
                    ask_size=None,
                    spread_percent=None,
                    liquidity_score=None,
                    market_cap=None,
                    fifty_two_week_high=None,
                    fifty_two_week_low=None,
                    last_updated_utc=None,
                    last_updated_riyadh=None,
                    data_source="argaam-gateway-error",
                    data_quality="MISSING",
                    remote_raw=None,
                    error=msg,
                )
            )
        else:
            quotes.append(res)
    return quotes


# ======================================================================
# SHEET HELPERS
# ======================================================================


def _build_sheet_headers() -> List[str]:
    """
    Headers aligned with the unified enriched sheet structure
    (Identity → Price/Liquidity → Fundamentals → Growth/Profitability
     → Valuation/Risk → AI/Technical → Meta).

    KSA / Argaam fills mainly Identity + Price/Liquidity + basic Meta.
    The rest is left blank so it can drop directly into KSA_Tadawul or
    shared 9-page templates without breaking Apps Script.
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


def _quote_to_sheet_row(q: ArgaamQuote) -> List[Any]:
    """
    Convert ArgaamQuote into a single row aligned with _build_sheet_headers.
    Only KSA-specific fields are filled; all others are left None.
    """
    # Compute 52W position %
    pos_52w: Optional[float] = None
    if (
        q.fifty_two_week_high is not None
        and q.fifty_two_week_low is not None
        and q.last_price is not None
        and q.fifty_two_week_high != q.fifty_two_week_low
    ):
        try:
            pos_52w = (
                (float(q.last_price) - float(q.fifty_two_week_low))
                / (float(q.fifty_two_week_high) - float(q.fifty_two_week_low))
                * 100.0
            )
        except Exception:
            pos_52w = None

    provider = q.data_source or "argaam-gateway"
    data_quality = q.data_quality or (
        "MISSING" if q.error else ("OK" if q.last_price is not None else "PARTIAL")
    )

    as_of_utc = q.last_updated_utc.isoformat() if q.last_updated_utc else None
    as_of_local = (
        q.last_updated_riyadh.isoformat() if q.last_updated_riyadh else None
    )

    return [
        # Identity
        q.symbol,
        q.name,
        q.sector,
        q.sub_sector,
        q.market,
        q.currency,
        None,  # Listing Date
        # Price / Liquidity
        q.last_price,
        q.previous_close,
        q.open_price,
        q.high_price,
        q.low_price,
        q.change,
        q.change_percent,
        q.fifty_two_week_high,
        q.fifty_two_week_low,
        pos_52w,
        q.volume,
        q.avg_volume_30d,
        q.value_traded,
        q.turnover_rate,
        q.bid_price,
        q.ask_price,
        q.bid_size,
        q.ask_size,
        q.spread_percent,
        q.liquidity_score,
        # Fundamentals
        None,  # EPS (TTM)
        None,  # P/E Ratio
        None,  # P/B Ratio
        None,  # Dividend Yield %
        None,  # Dividend Payout
        None,  # ROE %
        None,  # ROA %
        None,  # Debt/Equity
        None,  # Current Ratio
        None,  # Quick Ratio
        q.market_cap,
        # Growth / Profitability
        None,  # Revenue Growth %
        None,  # Net Income Growth %
        None,  # EBITDA Margin %
        None,  # Operating Margin %
        None,  # Net Margin %
        # Valuation / Risk
        None,  # EV/EBITDA
        None,  # Price/Sales
        None,  # Price/Cash Flow
        None,  # PEG Ratio
        None,  # Beta
        None,  # Volatility (30D) %
        # AI Valuation & Scores
        None,  # Fair Value
        None,  # Upside %
        None,  # Valuation Label
        None,  # Value Score
        None,  # Quality Score
        None,  # Momentum Score
        None,  # Opportunity Score
        None,  # Recommendation
        # Technical
        None,  # RSI (14)
        None,  # MACD
        None,  # Moving Avg (20D)
        None,  # Moving Avg (50D)
        # Meta
        provider,
        data_quality,
        as_of_utc,
        as_of_local,
        "Asia/Riyadh",
        q.error,
    ]


# ======================================================================
# ENDPOINTS
# ======================================================================


@router.get("/health")
async def argaam_health() -> Dict[str, Any]:
    """
    Lightweight health endpoint for KSA / Argaam gateway.

    Does NOT call the external gateway (fast & cheap).
    Just checks config and returns static info.
    """
    now = datetime.now(timezone.utc)
    return {
        "status": "ok" if ARGAAM_GATEWAY_URL else "not_configured",
        "app": APP_NAME,
        "version": APP_VERSION,
        "provider": "argaam-gateway",
        "gateway_configured": bool(ARGAAM_GATEWAY_URL),
        "timestamp_utc": now.isoformat(),
        "notes": [
            "This route is dedicated to KSA (.SR) tickers.",
            "Set ARGAAM_GATEWAY_URL (and ARGAAM_API_KEY if needed) in env.py / Render.",
        ],
    }


@router.get("/quote", response_model=ArgaamQuote)
async def get_argaam_quote(
    symbol: str = Query(..., description="KSA symbol, e.g. 1120.SR or 1120"),
) -> ArgaamQuote:
    """
    Return a single KSA quote from the Argaam/Tadawul gateway.

    Example:
        GET /v1/argaam/quote?symbol=1120.SR
        GET /v1/argaam/quote?symbol=1120
    """
    try:
        return await _fetch_argaam_quote(symbol)
    except HTTPException:
        # Pass through FastAPI-style errors for API clients
        raise
    except Exception as exc:  # pragma: no cover - defensive
        logger.exception("routes_argaam: Unexpected error in /quote for %s", symbol)
        raise HTTPException(
            status_code=status.HTTP_502_BAD_GATEWAY,
            detail=f"Unexpected error in Argaam quote: {exc}",
        )


@router.get("/quotes")
async def get_argaam_quotes(
    tickers: str = Query(
        ...,
        description=(
            "Comma-separated KSA symbols, e.g. "
            "1120.SR,1180.SR,1010.SR or numeric codes"
        ),
    )
) -> Dict[str, Any]:
    """
    Return multiple KSA quotes.

    Response:
        {
          "quotes": [ {ArgaamQuote}, ... ],
          "meta": { ... }
        }

    NOTE
    - If some symbols fail, they still appear with error filled-in.
    """
    raw_symbols = [t.strip() for t in tickers.split(",") if t.strip()]
    symbols = [
        _normalize_symbol(s)
        for s in raw_symbols
        if _is_ksa_symbol(_normalize_symbol(s))
    ]

    if not symbols:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="No valid KSA (.SR) symbols provided.",
        )

    quotes = await _fetch_argaam_quotes_bulk(symbols)
    error_count = sum(1 for q in quotes if q.error)

    return {
        "quotes": [q.model_dump(by_alias=True) for q in quotes],
        "meta": {
            "requested": raw_symbols,
            "resolved_ksa": symbols,
            "count": len(quotes),
            "error_count": error_count,
            "provider": "argaam-gateway",
            "note": "KSA quotes from Argaam/Tadawul gateway (no EODHD).",
        },
    }


@router.post("/sheet-rows", response_model=ArgaamSheetRowsResponse)
async def get_argaam_sheet_rows(
    body: ArgaamSheetRowsRequest,
) -> ArgaamSheetRowsResponse:
    """
    Sheet-friendly representation for KSA quotes:

        POST /v1/argaam/sheet-rows
        {
          "tickers": ["1120.SR","1180.SR"]
        }

    Response:
        {
          "headers": [...],
          "rows": [[...], ...]
        }

    This format is directly compatible with:
      - google_sheets_service.py (values.update)
      - Google Apps Script expecting headers+rows
      - KSA-specific sections of your 9-page dashboard.
    """
    raw_symbols = [t.strip() for t in (body.tickers or []) if t and t.strip()]
    symbols = [
        _normalize_symbol(s)
        for s in raw_symbols
        if _is_ksa_symbol(_normalize_symbol(s))
    ]

    # For Sheets: never 4xx/5xx – just return empty table if no valid KSA.
    if not symbols:
        logger.warning(
            "routes_argaam: sheet-rows called with no valid KSA symbols. Requested=%s",
            raw_symbols,
        )
        return ArgaamSheetRowsResponse(
            headers=_build_sheet_headers(),
            rows=[],
        )

    quotes = await _fetch_argaam_quotes_bulk(symbols)

    headers = _build_sheet_headers()
    rows = [_quote_to_sheet_row(q) for q in quotes]

    return ArgaamSheetRowsResponse(headers=headers, rows=rows)
