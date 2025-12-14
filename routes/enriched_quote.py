# routes/enriched_quote.py
"""
routes/enriched_quote.py
===========================================================
Enriched Quotes Router (v2.6.0) – Engine v2 aligned, Sheets-safe

Goals
- Stable /v1/enriched endpoints for Sheets + UI
- KSA-safe (engine decides provider; never force EODHD for .SR)
- /sheet-rows always returns {headers, rows} and NEVER crashes
- Uses DataEngine v2 when available; falls back to legacy adapter safely
- Uses core.schemas.get_headers_for_sheet(sheet_name) if available to keep headers aligned
  with your 9-page dashboard, otherwise falls back to the stable default headers below.

Key Fixes
- Engine singleton via lru_cache + safe close hook readiness
- Normalization delegated to core.data_engine_v2.normalize_symbol when available
- Batching uses bounded concurrency + timeout + placeholders
- Row-mapping is tolerant and guarantees row length == headers length
- No "rank" dependency (kept if present, else blank)
"""

from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timezone
from functools import lru_cache
from typing import Any, Dict, List, Optional, Sequence, Tuple

from fastapi import APIRouter, Query
from pydantic import BaseModel, Field

# Prefer env.settings (project standard). Fallback is safe.
try:
    from env import settings  # type: ignore
except Exception:  # pragma: no cover
    settings = None  # type: ignore

logger = logging.getLogger("routes.enriched_quote")
router = APIRouter(prefix="/v1/enriched", tags=["enriched"])
ROUTE_VERSION = "2.6.0"

# ---------------------------------------------------------------------
# Optional schema helper (preferred)
# ---------------------------------------------------------------------
try:
    from core.schemas import get_headers_for_sheet  # type: ignore
except Exception:  # pragma: no cover
    get_headers_for_sheet = None  # type: ignore

# ---------------------------------------------------------------------
# Prefer normalization from v2 engine
# ---------------------------------------------------------------------
try:
    from core.data_engine_v2 import normalize_symbol as _normalize_any  # type: ignore
except Exception:  # pragma: no cover
    def _normalize_any(sym: str) -> str:
        s = (sym or "").strip().upper()
        if not s:
            return ""
        if s.startswith("TADAWUL:"):
            s = s.split(":", 1)[1].strip()
        if s.endswith(".TADAWUL"):
            s = s.replace(".TADAWUL", ".SR")
        if s.isdigit():
            return f"{s}.SR"
        return s

# ---------------------------------------------------------------------
# Engine singleton (defensive)
# ---------------------------------------------------------------------
@lru_cache(maxsize=1)
def _get_engine():
    # Prefer v2 engine
    try:
        from core.data_engine_v2 import DataEngine  # type: ignore
        eng = DataEngine()
        logger.info("[enriched] Using core.data_engine_v2.DataEngine")
        return eng
    except Exception as e:
        logger.warning("[enriched] data_engine_v2 not available: %s", e)

    # Fallback legacy engine adapter (core.data_engine bridges to v2 if present)
    try:
        from core.data_engine import DataEngine  # type: ignore
        eng = DataEngine()
        logger.info("[enriched] Using legacy core.data_engine.DataEngine")
        return eng
    except Exception as e:
        logger.error("[enriched] No engine available: %s", e)
        return None

def _model_to_dict(obj: Any) -> Dict[str, Any]:
    if obj is None:
        return {}
    # pydantic v2
    if hasattr(obj, "model_dump"):
        try:
            return obj.model_dump()
        except Exception:
            pass
    # pydantic v1
    if hasattr(obj, "dict"):
        try:
            return obj.dict()
        except Exception:
            pass
    if isinstance(obj, dict):
        return obj
    return {"value": obj}

def _safe_int(x: Any, default: int) -> int:
    try:
        return int(x)
    except Exception:
        return default

def _safe_float(x: Any) -> Optional[float]:
    try:
        if x in (None, "", "NA", "N/A", "-", "—"):
            return None
        return float(x)
    except Exception:
        return None

def _now_utc_iso() -> str:
    return datetime.now(timezone.utc).isoformat()

# ---------------------------------------------------------------------
# Default Sheet schema (stable fallback)
# ---------------------------------------------------------------------
DEFAULT_ENRICHED_HEADERS: List[str] = [
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
    "Overall Score",
    "Recommendation",
    "Data Source",
    "Data Quality",
    "Last Updated (UTC)",
    "Last Updated (Riyadh)",
    "Error",
]

def _headers_for_sheet(sheet_name: Optional[str]) -> List[str]:
    """
    Prefer core.schemas.get_headers_for_sheet(sheet_name) if present.
    Ensure 'Symbol' exists. Otherwise use DEFAULT_ENRICHED_HEADERS.
    """
    if sheet_name and get_headers_for_sheet:
        try:
            h = get_headers_for_sheet(sheet_name)
            if isinstance(h, list) and h and any(str(x).strip().lower() == "symbol" for x in h):
                return [str(x) for x in h]
        except Exception:
            pass
    return list(DEFAULT_ENRICHED_HEADERS)

def _pick(d: Dict[str, Any], *keys: str) -> Any:
    for k in keys:
        if k in d and d[k] not in (None, "", "NA", "N/A"):
            return d[k]
    return None

def _to_row_for_headers(symbol: str, d: Dict[str, Any], headers: List[str], error: Optional[str]) -> List[Any]:
    """
    Header-driven row mapping (robust + future-proof).
    Any unknown header => None.
    Guarantees len(row)==len(headers).
    """
    # Build a tolerant field map from possible keys across UnifiedQuote / EnrichedQuote / legacy
    last_price = _pick(d, "current_price", "last_price", "price", "close")
    prev_close = _pick(d, "previous_close", "prev_close", "previousClose")
    day_high = _pick(d, "day_high", "high", "dayHigh")
    day_low  = _pick(d, "day_low", "low", "dayLow")
    open_p   = _pick(d, "open")

    change = _pick(d, "price_change", "change")
    change_pct = _pick(d, "percent_change", "change_percent", "change_pct")

    high_52w = _pick(d, "high_52w", "fifty_two_week_high", "52w_high", "52WeekHigh")
    low_52w = _pick(d, "low_52w", "fifty_two_week_low", "52w_low", "52WeekLow")
    pos_52w = _pick(d, "position_52w_percent", "52w_position_percent", "fifty_two_week_position_pct")

    # common metadata
    data_quality = _pick(d, "data_quality")
    data_source = _pick(d, "data_source", "provider")

    last_updated_utc = _pick(d, "last_updated_utc", "time_utc", "updated_at")
    last_updated_riyadh = _pick(d, "last_updated_riyadh", "time_riyadh")

    # scoring
    value_score = _pick(d, "value_score")
    quality_score = _pick(d, "quality_score")
    momentum_score = _pick(d, "momentum_score")
    opportunity_score = _pick(d, "opportunity_score")
    overall_score = _pick(d, "overall_score")

    # compute value_traded if missing
    volume = _pick(d, "volume")
    value_traded = _pick(d, "value_traded")
    if value_traded is None:
        cp = _safe_float(last_price)
        vol = _safe_float(volume)
        if cp is not None and vol is not None:
            value_traded = cp * vol

    field_map: Dict[str, Any] = {
        "Symbol": symbol,
        "Company Name": _pick(d, "name", "company_name"),
        "Sector": _pick(d, "sector"),
        "Sub-Sector": _pick(d, "sub_sector", "industry"),
        "Market": _pick(d, "market", "market_region"),
        "Currency": _pick(d, "currency"),
        "Listing Date": _pick(d, "listing_date"),

        "Last Price": last_price,
        "Previous Close": prev_close,
        "Open": open_p,
        "Day High": day_high,
        "Day Low": day_low,
        "Price Change": change,
        "Percent Change": change_pct,

        "52W High": high_52w,
        "52W Low": low_52w,
        "52W Position %": pos_52w,

        "Volume": volume,
        "Avg Volume (30D)": _pick(d, "avg_volume_30d", "avg_volume"),
        "Value Traded": value_traded,
        "Turnover %": _pick(d, "turnover_percent", "turnover"),

        "Shares Outstanding": _pick(d, "shares_outstanding"),
        "Free Float %": _pick(d, "free_float", "free_float_percent"),
        "Market Cap": _pick(d, "market_cap"),
        "Free Float Market Cap": _pick(d, "free_float_market_cap"),
        "Liquidity Score": _pick(d, "liquidity_score"),

        "EPS (TTM)": _pick(d, "eps_ttm", "eps"),
        "Forward EPS": _pick(d, "forward_eps"),
        "P/E (TTM)": _pick(d, "pe_ttm", "pe", "pe_ratio"),
        "Forward P/E": _pick(d, "forward_pe"),
        "P/B": _pick(d, "pb", "price_to_book"),
        "P/S": _pick(d, "ps", "price_to_sales"),
        "EV/EBITDA": _pick(d, "ev_ebitda", "ev_to_ebitda"),

        "Dividend Yield %": _pick(d, "dividend_yield", "dividend_yield_percent"),
        "Dividend Rate": _pick(d, "dividend_rate"),
        "Payout Ratio %": _pick(d, "payout_ratio"),

        "ROE %": _pick(d, "roe"),
        "ROA %": _pick(d, "roa"),
        "Net Margin %": _pick(d, "net_margin", "profit_margin"),
        "EBITDA Margin %": _pick(d, "ebitda_margin"),
        "Revenue Growth %": _pick(d, "revenue_growth"),
        "Net Income Growth %": _pick(d, "net_income_growth"),

        "Beta": _pick(d, "beta"),
        "Volatility (30D)": _pick(d, "volatility_30d", "volatility"),
        "RSI (14)": _pick(d, "rsi_14", "rsi"),

        "Value Score": value_score,
        "Quality Score": quality_score,
        "Momentum Score": momentum_score,
        "Opportunity Score": opportunity_score,
        "Overall Score": overall_score,
        "Recommendation": _pick(d, "recommendation"),

        "Data Source": data_source,
        "Data Quality": data_quality,
        "Last Updated (UTC)": last_updated_utc,
        "Last Updated (Riyadh)": last_updated_riyadh,
        "Error": error or _pick(d, "error") or "",
    }

    row = [field_map.get(h, None) for h in headers]
    if len(row) < len(headers):
        row += [None] * (len(headers) - len(row))
    return row[: len(headers)]

# ---------------------------------------------------------------------
# Models
# ---------------------------------------------------------------------
class SheetRowsRequest(BaseModel):
    tickers: List[str] = Field(default_factory=list)
    sheet_name: Optional[str] = Field(default=None)

class SheetRowsResponse(BaseModel):
    headers: List[str]
    rows: List[List[Any]]
    meta: Dict[str, Any] = Field(default_factory=dict)

# ---------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------
@router.get("/health")
async def health() -> Dict[str, Any]:
    prov = []
    try:
        prov = list(getattr(settings, "enabled_providers", []) or [])
    except Exception:
        prov = []
    return {
        "status": "ok",
        "router": "enriched",
        "version": ROUTE_VERSION,
        "provider_mode": ",".join([str(x) for x in prov]),
        "timestamp_utc": _now_utc_iso(),
    }

@router.get("/quote")
async def quote(symbol: str = Query(..., description="Ticker, e.g. 1120.SR or AAPL")) -> Dict[str, Any]:
    sym = _normalize_any(symbol)
    engine = _get_engine()
    if engine is None:
        return {"symbol": sym, "status": "error", "error": "No engine available"}

    try:
        if hasattr(engine, "get_enriched_quote"):
            result = await engine.get_enriched_quote(sym)  # type: ignore
        elif hasattr(engine, "get_quote"):
            result = await engine.get_quote(sym)  # type: ignore
        else:
            from core.data_engine import get_enriched_quote  # type: ignore
            result = await get_enriched_quote(sym)  # type: ignore

        data = _model_to_dict(result)
        data.setdefault("symbol", sym)
        data.setdefault("status", "ok")
        return data

    except Exception as e:
        logger.exception("quote failed for %s", sym)
        return {"symbol": sym, "status": "error", "error": str(e)}

@router.post("/sheet-rows", response_model=SheetRowsResponse)
async def sheet_rows(req: SheetRowsRequest) -> Dict[str, Any]:
    tickers = [_normalize_any(t) for t in (req.tickers or []) if str(t).strip()]
    tickers = [t for t in tickers if t]

    headers = _headers_for_sheet(req.sheet_name or "Enriched")

    if not tickers:
        return {"headers": headers, "rows": [], "meta": {"count": 0}}

    engine = _get_engine()
    if engine is None:
        rows = [_to_row_for_headers(t, {"symbol": t}, headers, "No engine available") for t in tickers]
        return {"headers": headers, "rows": rows, "meta": {"count": len(rows), "error": "No engine available"}}

    # Batch config (safe defaults)
    batch_size = _safe_int(getattr(settings, "enriched_batch_size", 40) if settings else 40, 40)
    batch_size = max(1, min(batch_size, 200))

    timeout_sec = _safe_float(getattr(settings, "enriched_batch_timeout_sec", 45.0) if settings else 45.0, 45.0)
    max_conc = _safe_int(getattr(settings, "enriched_batch_concurrency", 5) if settings else 5, 5)
    sem = asyncio.Semaphore(max(1, max_conc))

    async def fetch_one(sym: str) -> Tuple[str, Dict[str, Any], Optional[str]]:
        async with sem:
            try:
                if hasattr(engine, "get_enriched_quote"):
                    r = await asyncio.wait_for(engine.get_enriched_quote(sym), timeout=timeout_sec)  # type: ignore
                elif hasattr(engine, "get_quote"):
                    r = await asyncio.wait_for(engine.get_quote(sym), timeout=timeout_sec)  # type: ignore
                else:
                    from core.data_engine import get_enriched_quote  # type: ignore
                    r = await asyncio.wait_for(get_enriched_quote(sym), timeout=timeout_sec)  # type: ignore

                d = _model_to_dict(r)
                d.setdefault("symbol", sym)
                return sym, d, None

            except Exception as e:
                msg = "Timeout" if isinstance(e, asyncio.TimeoutError) else str(e)
                return sym, {"symbol": sym, "data_quality": "MISSING", "data_source": "none"}, msg

    rows_out: List[List[Any]] = []
    skipped: List[str] = []

    for i in range(0, len(tickers), batch_size):
        chunk = tickers[i : i + batch_size]
        try:
            results = await asyncio.gather(*[fetch_one(s) for s in chunk])
        except Exception as e:
            # Extremely defensive: if gather itself fails
            logger.exception("sheet_rows chunk gather failed")
            for s in chunk:
                rows_out.append(_to_row_for_headers(s, {"symbol": s}, headers, str(e)))
            continue

        for sym, d, err in results:
            rows_out.append(_to_row_for_headers(sym, d, headers, err))

    return {
        "headers": headers,
        "rows": rows_out,
        "meta": {
            "count": len(rows_out),
            "batch_size": batch_size,
            "timeout_sec": timeout_sec,
            "concurrency": max_conc,
            "skipped": skipped,
        },
    }

__all__ = ["router"]
