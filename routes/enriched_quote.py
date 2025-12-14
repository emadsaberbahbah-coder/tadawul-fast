"""
routes/enriched_quote.py
===========================================================
Enriched Quotes Router (v2.3)

Goals
- Stable /v1/enriched endpoints for Sheets + UI
- KSA-safe (engine decides provider; never force EODHD for .SR)
- Sheet rows endpoint always returns {headers, rows} and never crashes
"""

from __future__ import annotations

import asyncio
import logging
from typing import Any, Dict, List, Optional, Tuple

from fastapi import APIRouter, Query
from pydantic import BaseModel, Field

from env import settings

logger = logging.getLogger("routes.enriched_quote")
router = APIRouter(prefix="/v1/enriched", tags=["enriched"])

# ---------------------------------------------------------------------
# Engine import (defensive)
# ---------------------------------------------------------------------
_ENGINE = None

def _get_engine():
    global _ENGINE
    if _ENGINE is not None:
        return _ENGINE

    # Prefer v2 engine
    try:
        from core.data_engine_v2 import DataEngine  # type: ignore
        _ENGINE = DataEngine()
        logger.info("[enriched] Using core.data_engine_v2.DataEngine")
        return _ENGINE
    except Exception as e:
        logger.warning("[enriched] data_engine_v2 not available: %s", e)

    # Fallback legacy engine
    try:
        from core.data_engine import DataEngine  # type: ignore
        _ENGINE = DataEngine()
        logger.info("[enriched] Using legacy core.data_engine.DataEngine")
        return _ENGINE
    except Exception as e:
        logger.error("[enriched] No engine available: %s", e)
        _ENGINE = None
        return None

def _model_to_dict(obj: Any) -> Dict[str, Any]:
    if obj is None:
        return {}
    # pydantic v2
    if hasattr(obj, "model_dump"):
        return obj.model_dump()
    # pydantic v1
    if hasattr(obj, "dict"):
        return obj.dict()
    if isinstance(obj, dict):
        return obj
    return {"value": obj}

def _normalize_symbol(sym: str) -> str:
    s = (sym or "").strip().upper()
    if s.isdigit():
        return f"{s}.SR"
    return s

# ---------------------------------------------------------------------
# Sheet schema (keep stable)
# ---------------------------------------------------------------------
ENRICHED_HEADERS: List[str] = [
    "Symbol",
    "Company Name",
    "Sector",
    "Sub-Sector",
    "Market",
    "Currency",
    "Listing Date",
    "Last Price",
    "Previous Close",
    "Open",
    "Day High",
    "Day Low",
    "Price Change",
    "Percent Change",
    "52W High",
    "52W Low",
    "52W Position %",
    "Volume",
    "Avg Volume (30D)",
    "Value Traded",
    "Turnover %",
    "Shares Outstanding",
    "Free Float %",
    "Market Cap",
    "Free Float Market Cap",
    "Liquidity Score",
    "EPS (TTM)",
    "Forward EPS",
    "P/E (TTM)",
    "Forward P/E",
    "P/B",
    "P/S",
    "EV/EBITDA",
    "Dividend Yield %",
    "Dividend Rate",
    "Payout Ratio %",
    "ROE %",
    "ROA %",
    "Net Margin %",
    "EBITDA Margin %",
    "Revenue Growth %",
    "Net Income Growth %",
    "Beta",
    "Volatility (30D)",
    "RSI (14)",
    "Value Score",
    "Quality Score",
    "Momentum Score",
    "Opportunity Score",
    "Rank",
    "Recommendation",
    "Data Source",
    "Data Quality",
    "Last Updated (UTC)",
    "Error",
]

def _pick(d: Dict[str, Any], *keys: str) -> Any:
    for k in keys:
        if k in d and d[k] not in (None, "", "NA"):
            return d[k]
    return None

def _to_row(symbol: str, d: Dict[str, Any], error: Optional[str] = None) -> List[Any]:
    # tolerate multiple naming conventions
    last_price = _pick(d, "last_price", "price", "current_price")
    prev_close = _pick(d, "previous_close", "prev_close")
    change = _pick(d, "change", "price_change")
    change_pct = _pick(d, "change_percent", "percent_change")

    high_52w = _pick(d, "high_52w", "fifty_two_week_high", "52w_high")
    low_52w = _pick(d, "low_52w", "fifty_two_week_low", "52w_low")

    return [
        symbol,
        _pick(d, "name", "company_name"),
        _pick(d, "sector"),
        _pick(d, "sub_sector", "industry"),
        _pick(d, "market"),
        _pick(d, "currency"),
        _pick(d, "listing_date"),
        last_price,
        prev_close,
        _pick(d, "open"),
        _pick(d, "high", "day_high"),
        _pick(d, "low", "day_low"),
        change,
        change_pct,
        high_52w,
        low_52w,
        _pick(d, "position_52w_percent", "52w_position_percent"),
        _pick(d, "volume"),
        _pick(d, "avg_volume_30d", "avg_volume"),
        _pick(d, "value_traded"),
        _pick(d, "turnover", "turnover_percent"),
        _pick(d, "shares_outstanding"),
        _pick(d, "free_float", "free_float_percent"),
        _pick(d, "market_cap"),
        _pick(d, "free_float_market_cap"),
        _pick(d, "liquidity_score"),
        _pick(d, "eps_ttm", "eps", "eps_ttm_value"),
        _pick(d, "forward_eps"),
        _pick(d, "pe_ttm", "pe", "pe_ratio"),
        _pick(d, "forward_pe"),
        _pick(d, "pb", "price_to_book"),
        _pick(d, "ps", "price_to_sales"),
        _pick(d, "ev_ebitda", "ev_to_ebitda"),
        _pick(d, "dividend_yield", "dividend_yield_percent"),
        _pick(d, "dividend_rate"),
        _pick(d, "payout_ratio"),
        _pick(d, "roe"),
        _pick(d, "roa"),
        _pick(d, "net_margin"),
        _pick(d, "ebitda_margin"),
        _pick(d, "revenue_growth"),
        _pick(d, "net_income_growth"),
        _pick(d, "beta"),
        _pick(d, "volatility_30d", "volatility"),
        _pick(d, "rsi_14", "rsi"),
        _pick(d, "value_score"),
        _pick(d, "quality_score"),
        _pick(d, "momentum_score"),
        _pick(d, "opportunity_score"),
        _pick(d, "rank"),
        _pick(d, "recommendation"),
        _pick(d, "data_source", "provider"),
        _pick(d, "data_quality"),
        _pick(d, "time_utc", "last_updated_utc", "updated_at"),
        error,
    ]

# ---------------------------------------------------------------------
# Models
# ---------------------------------------------------------------------
class SheetRowsRequest(BaseModel):
    tickers: List[str] = Field(default_factory=list)
    sheet_name: str = Field(default="Unknown")

class SheetRowsResponse(BaseModel):
    headers: List[str]
    rows: List[List[Any]]

# ---------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------
@router.get("/health")
async def health() -> Dict[str, Any]:
    return {
        "status": "ok",
        "router": "enriched",
        "version": "2.3",
        "provider_mode": ",".join(settings.enabled_providers or []),
    }

@router.get("/quote")
async def quote(symbol: str = Query(..., description="Ticker, e.g. 1120.SR or AAPL")) -> Dict[str, Any]:
    sym = _normalize_symbol(symbol)
    engine = _get_engine()
    if engine is None:
        return {"symbol": sym, "status": "error", "error": "No engine available"}

    try:
        # engine methods may be async
        if hasattr(engine, "get_enriched_quote"):
            result = await engine.get_enriched_quote(sym)  # type: ignore
        else:
            # legacy function style
            from core.data_engine import get_enriched_quote  # type: ignore
            result = await get_enriched_quote(sym)  # type: ignore

        data = _model_to_dict(result)
        if "symbol" not in data:
            data["symbol"] = sym
        data.setdefault("status", "ok")
        return data
    except Exception as e:
        logger.exception("quote failed for %s", sym)
        return {"symbol": sym, "status": "error", "error": str(e)}

@router.post("/sheet-rows", response_model=SheetRowsResponse)
async def sheet_rows(req: SheetRowsRequest) -> Dict[str, Any]:
    tickers = [_normalize_symbol(t) for t in (req.tickers or []) if str(t).strip()]
    if not tickers:
        return {"headers": ENRICHED_HEADERS, "rows": []}

    engine = _get_engine()
    if engine is None:
        rows = [[t] + [None] * (len(ENRICHED_HEADERS) - 2) + ["No engine available"] for t in tickers]
        return {"headers": ENRICHED_HEADERS, "rows": rows}

    batch_size = int(getattr(settings, "enriched_batch_size", 40) or 40)
    batch_size = max(1, min(batch_size, 200))

    async def fetch_one(sym: str) -> Tuple[str, Dict[str, Any], Optional[str]]:
        try:
            if hasattr(engine, "get_enriched_quote"):
                r = await engine.get_enriched_quote(sym)  # type: ignore
            else:
                from core.data_engine import get_enriched_quote  # type: ignore
                r = await get_enriched_quote(sym)  # type: ignore
            return sym, _model_to_dict(r), None
        except Exception as e:
            return sym, {}, str(e)

    rows_out: List[List[Any]] = []

    for i in range(0, len(tickers), batch_size):
        chunk = tickers[i:i + batch_size]
        results = await asyncio.gather(*[fetch_one(s) for s in chunk])
        for sym, d, err in results:
            d.setdefault("symbol", sym)
            rows_out.append(_to_row(sym, d, err))

    return {"headers": ENRICHED_HEADERS, "rows": rows_out}
