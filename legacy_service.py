"""
legacy_service.py
------------------------------------------------------------
LEGACY SERVICE BRIDGE – v2.0

GOAL
- Provide a stable "legacy" layer on top of the new core.data_engine,
  so old clients (PowerShell, early Google Sheets scripts, etc.) can
  still receive the classic /v1/quote-style payload.

KEY FEATURES
- NO direct EODHD calls. All data comes via core.data_engine.
- KSA (.SR) tickers are handled by the new engine (Tadawul/Argaam
  providers) – this module does NOT care if EODHD works or not.
- Legacy quote format:
      {
          "quotes": [ { ... legacy fields ... } ],
          "meta":   { ... context ... }
      }
- Google Sheets integration helpers:
      • build_legacy_sheet_payload(tickers)
        -> { "headers": [...], "rows": [[...], ...] }

TYPICAL USAGE
-------------
1) In a FastAPI router (for full backward compatibility):

    from fastapi import APIRouter, Query
    from legacy_service import get_legacy_quotes

    router = APIRouter(prefix="/v1")

    @router.get("/quote")
    async def legacy_quote(tickers: str = Query(...)):
        # tickers="AAPL,MSFT,1120.SR"
        symbols = [t.strip() for t in tickers.split(",") if t.strip()]
        return await get_legacy_quotes(symbols)

2) In a Google Sheets helper:

    from legacy_service import build_legacy_sheet_payload

    payload = await build_legacy_sheet_payload(["AAPL", "MSFT", "1120.SR"])
    headers = payload["headers"]
    rows    = payload["rows"]
    # -> use headers + rows with Sheets API setValues()

"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional

from env import APP_NAME, APP_VERSION, BACKEND_BASE_URL
from core.data_engine import UnifiedQuote, get_enriched_quotes

# ----------------------------------------------------------------------
# CONSTANTS / TIMEZONES
# ----------------------------------------------------------------------

RIYADH_TZ = timezone(timedelta(hours=3))


# ----------------------------------------------------------------------
# DATA STRUCTURES
# ----------------------------------------------------------------------


@dataclass
class LegacyQuote:
    """
    Legacy quote representation, modeled on your original /v1/quote
    response used in PowerShell tests, but backed by UnifiedQuote.

    NOTE:
    - Many fields may be None depending on provider data.
    """

    # Identity
    ticker: str
    symbol: str
    name: Optional[str] = None
    exchange: Optional[str] = None
    market: Optional[str] = None
    currency: Optional[str] = None
    sector: Optional[str] = None
    industry: Optional[str] = None

    # Prices & change
    price: Optional[float] = None
    previous_close: Optional[float] = None
    open: Optional[float] = None
    high: Optional[float] = None
    low: Optional[float] = None
    change: Optional[float] = None
    change_percent: Optional[float] = None

    # Volumes & liquidity
    volume: Optional[float] = None
    avg_volume: Optional[float] = None
    market_cap: Optional[float] = None
    shares_outstanding: Optional[float] = None
    free_float: Optional[float] = None

    # 52-week data
    fifty_two_week_high: Optional[float] = None
    fifty_two_week_low: Optional[float] = None
    fifty_two_week_position_pct: Optional[float] = None  # 0–100

    # Fundamentals
    eps: Optional[float] = None
    pe_ratio: Optional[float] = None
    pb_ratio: Optional[float] = None
    dividend_yield: Optional[float] = None
    roe: Optional[float] = None
    roa: Optional[float] = None
    profit_margin: Optional[float] = None
    debt_to_equity: Optional[float] = None

    # Data quality & meta
    data_quality: str = "MISSING"
    data_source: Optional[str] = None
    last_updated_utc: Optional[datetime] = None
    last_updated_riyadh: Optional[datetime] = None
    error: Optional[str] = None


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


def _to_riyadh(dt: Optional[datetime]) -> Optional[datetime]:
    if dt is None:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(RIYADH_TZ)


def _compute_52w_position_pct(
    price: Optional[float],
    low_52w: Optional[float],
    high_52w: Optional[float],
) -> Optional[float]:
    if price is None or low_52w is None or high_52w is None:
        return None
    span = high_52w - low_52w
    if span <= 0:
        return None
    try:
        pct = (price - low_52w) / span * 100.0
        return max(0.0, min(100.0, pct))
    except Exception:
        return None


def _unified_to_legacy(q: UnifiedQuote) -> LegacyQuote:
    """
    Map UnifiedQuote -> LegacyQuote.
    This is the central mapping used for all legacy responses.
    """
    price = _safe_float(q.price)
    prev_close = _safe_float(q.prev_close)
    high_52w = _safe_float(q.fifty_two_week_high)
    low_52w = _safe_float(q.fifty_two_week_low)

    pos_52w = _compute_52w_position_pct(price, low_52w, high_52w)

    last_utc = q.last_updated_utc
    last_riyadh = _to_riyadh(last_utc)

    data_source = ", ".join(src.provider for src in (q.sources or []))

    return LegacyQuote(
        ticker=q.symbol,
        symbol=q.symbol,
        name=q.name,
        exchange=q.exchange,
        market=q.market_region,
        currency=q.currency,
        sector=getattr(q, "sector", None),
        industry=getattr(q, "industry", None),
        price=price,
        previous_close=prev_close,
        open=_safe_float(q.open),
        high=_safe_float(q.high),
        low=_safe_float(q.low),
        change=_safe_float(q.change),
        change_percent=_safe_float(q.change_pct),
        volume=_safe_float(q.volume),
        avg_volume=_safe_float(getattr(q, "avg_volume", None)),
        market_cap=_safe_float(q.market_cap),
        shares_outstanding=_safe_float(getattr(q, "shares_outstanding", None)),
        free_float=_safe_float(getattr(q, "free_float", None)),
        fifty_two_week_high=high_52w,
        fifty_two_week_low=low_52w,
        fifty_two_week_position_pct=pos_52w,
        eps=_safe_float(q.eps_ttm),
        pe_ratio=_safe_float(q.pe_ttm),
        pb_ratio=_safe_float(q.pb),
        dividend_yield=_safe_float(q.dividend_yield),
        roe=_safe_float(q.roe),
        roa=_safe_float(q.roa),
        profit_margin=_safe_float(q.profit_margin),
        debt_to_equity=_safe_float(q.debt_to_equity),
        data_quality=q.data_quality,
        data_source=data_source or None,
        last_updated_utc=last_utc,
        last_updated_riyadh=last_riyadh,
        error=None,
    )


def _legacy_to_dict(lq: LegacyQuote) -> Dict[str, Any]:
    """
    Convert LegacyQuote dataclass to plain dict, ready for JSON response.
    """
    return {
        "ticker": lq.ticker,
        "symbol": lq.symbol,
        "name": lq.name,
        "exchange": lq.exchange,
        "market": lq.market,
        "currency": lq.currency,
        "sector": lq.sector,
        "industry": lq.industry,
        "price": lq.price,
        "previous_close": lq.previous_close,
        "open": lq.open,
        "high": lq.high,
        "low": lq.low,
        "change": lq.change,
        "change_percent": lq.change_percent,
        "volume": lq.volume,
        "avg_volume": lq.avg_volume,
        "market_cap": lq.market_cap,
        "shares_outstanding": lq.shares_outstanding,
        "free_float": lq.free_float,
        "fifty_two_week_high": lq.fifty_two_week_high,
        "fifty_two_week_low": lq.fifty_two_week_low,
        "fifty_two_week_position_pct": lq.fifty_two_week_position_pct,
        "eps": lq.eps,
        "pe_ratio": lq.pe_ratio,
        "pb_ratio": lq.pb_ratio,
        "dividend_yield": lq.dividend_yield,
        "roe": lq.roe,
        "roa": lq.roa,
        "profit_margin": lq.profit_margin,
        "debt_to_equity": lq.debt_to_equity,
        "data_quality": lq.data_quality,
        "data_source": lq.data_source,
        "last_updated_utc": (
            lq.last_updated_utc.isoformat() if lq.last_updated_utc else None
        ),
        "last_updated_riyadh": (
            lq.last_updated_riyadh.isoformat() if lq.last_updated_riyadh else None
        ),
        "error": lq.error,
    }


def _build_sheet_headers() -> List[str]:
    """
    A compact legacy header set for Google Sheets.
    This is simpler than the 59-column advanced templates, and mirrors
    the legacy quote fields you were using early on.
    """
    return [
        "Symbol",
        "Name",
        "Exchange/Market",
        "Currency",
        "Sector",
        "Industry",
        "Price",
        "Previous Close",
        "Change",
        "Change %",
        "Volume",
        "Market Cap",
        "P/E",
        "P/B",
        "Dividend Yield",
        "ROE",
        "Profit Margin",
        "52W High",
        "52W Low",
        "52W Position %",
        "Data Quality",
        "Data Source",
        "Last Updated (UTC)",
        "Last Updated (Riyadh)",
        "Error",
    ]


def _legacy_to_sheet_row(lq: LegacyQuote) -> List[Any]:
    """
    Flatten LegacyQuote into a single row for setValues().
    """
    return [
        lq.symbol,
        lq.name or "",
        lq.exchange or lq.market or "",
        lq.currency or "",
        lq.sector or "",
        lq.industry or "",
        lq.price,
        lq.previous_close,
        lq.change,
        lq.change_percent,
        lq.volume,
        lq.market_cap,
        lq.pe_ratio,
        lq.pb_ratio,
        lq.dividend_yield,
        lq.roe,
        lq.profit_margin,
        lq.fifty_two_week_high,
        lq.fifty_two_week_low,
        lq.fifty_two_week_position_pct,
        lq.data_quality,
        lq.data_source or "",
        lq.last_updated_utc.isoformat() if lq.last_updated_utc else None,
        lq.last_updated_riyadh.isoformat() if lq.last_updated_riyadh else None,
        lq.error or "",
    ]


# ----------------------------------------------------------------------
# PUBLIC API – LEGACY QUOTES
# ----------------------------------------------------------------------


async def get_legacy_quotes(tickers: List[str]) -> Dict[str, Any]:
    """
    Core function: return legacy-style quotes payload.

    Parameters
    ----------
    tickers : list[str]
        List of symbols, e.g. ["1120.SR", "1180.SR", "AAPL"].

    RETURNS
    -------
    dict:
        {
            "quotes": [ { ... legacy fields ... } ],
            "meta":   {
                "requested": [...],
                "count": int,
                "app": ...,
                "version": ...,
                "backend_url": ...,
                "note": ...
            }
        }
    """
    symbols = [t.strip().upper() for t in (tickers or []) if t and t.strip()]
    if not symbols:
        return {
            "quotes": [],
            "meta": {
                "requested": [],
                "count": 0,
                "app": APP_NAME,
                "version": APP_VERSION,
                "backend_url": BACKEND_BASE_URL,
                "note": "No symbols provided.",
            },
        }

    # Call the new unified engine (handles KSA vs Global internally)
    try:
        unified_quotes = await get_enriched_quotes(symbols)
    except Exception as exc:
        # Total failure – return placeholders for all
        legacy_quotes: List[LegacyQuote] = []
        for t in symbols:
            legacy_quotes.append(
                LegacyQuote(
                    ticker=t,
                    symbol=t,
                    data_quality="MISSING",
                    error=f"Data engine error: {exc}",
                )
            )

        return {
            "quotes": [_legacy_to_dict(lq) for lq in legacy_quotes],
            "meta": {
                "requested": symbols,
                "count": len(symbols),
                "app": APP_NAME,
                "version": APP_VERSION,
                "backend_url": BACKEND_BASE_URL,
                "note": "Failed to fetch data from core.data_engine.",
            },
        }

    legacy_quotes: List[LegacyQuote] = []
    for sym, uq in zip(symbols, unified_quotes):
        try:
            lq = _unified_to_legacy(uq)
        except Exception as exc:
            # Local mapping failure for this symbol – still return placeholder
            lq = LegacyQuote(
                ticker=sym,
                symbol=sym,
                data_quality="MISSING",
                error=f"Legacy mapping error: {exc}",
            )
        legacy_quotes.append(lq)

    payload = {
        "quotes": [_legacy_to_dict(lq) for lq in legacy_quotes],
        "meta": {
            "requested": symbols,
            "count": len(legacy_quotes),
            "app": APP_NAME,
            "version": APP_VERSION,
            "backend_url": BACKEND_BASE_URL,
            "note": "Legacy format built on UnifiedQuote v2 (KSA via Tadawul/Argaam, no direct EODHD).",
        },
    }
    return payload


# ----------------------------------------------------------------------
# PUBLIC API – GOOGLE SHEETS HELPERS
# ----------------------------------------------------------------------


async def build_legacy_sheet_payload(tickers: List[str]) -> Dict[str, Any]:
    """
    Build a Google Sheets–friendly payload using the legacy quote format.

    RETURNS
    -------
    dict:
        {
            "headers": [...],
            "rows": [[...], ...],
            "meta": { ... same as get_legacy_quotes.meta ... }
        }

    This can be used by:
        - google_sheets_service (write_range)
        - Google Apps Script bridge
        - any script that needs simple headers/rows.
    """
    legacy_payload = await get_legacy_quotes(tickers)
    quotes = legacy_payload.get("quotes", [])
    meta = legacy_payload.get("meta", {})

    headers = _build_sheet_headers()
    rows: List[List[Any]] = []

    for q in quotes:
        # q is already a dict in legacy shape; reconstruct LegacyQuote for row:
        lq = LegacyQuote(
            ticker=q.get("ticker", ""),
            symbol=q.get("symbol", ""),
            name=q.get("name"),
            exchange=q.get("exchange"),
            market=q.get("market"),
            currency=q.get("currency"),
            sector=q.get("sector"),
            industry=q.get("industry"),
            price=q.get("price"),
            previous_close=q.get("previous_close"),
            open=q.get("open"),
            high=q.get("high"),
            low=q.get("low"),
            change=q.get("change"),
            change_percent=q.get("change_percent"),
            volume=q.get("volume"),
            avg_volume=q.get("avg_volume"),
            market_cap=q.get("market_cap"),
            shares_outstanding=q.get("shares_outstanding"),
            free_float=q.get("free_float"),
            fifty_two_week_high=q.get("fifty_two_week_high"),
            fifty_two_week_low=q.get("fifty_two_week_low"),
            fifty_two_week_position_pct=q.get("fifty_two_week_position_pct"),
            eps=q.get("eps"),
            pe_ratio=q.get("pe_ratio"),
            pb_ratio=q.get("pb_ratio"),
            dividend_yield=q.get("dividend_yield"),
            roe=q.get("roe"),
            roa=q.get("roa"),
            profit_margin=q.get("profit_margin"),
            debt_to_equity=q.get("debt_to_equity"),
            data_quality=q.get("data_quality", "MISSING"),
            data_source=q.get("data_source"),
            last_updated_utc=None,  # we render string ISO in row anyway
            last_updated_riyadh=None,
            error=q.get("error"),
        )

        # If the dict has ISO datetimes, we can parse them back for completeness
        # (optional; safe to ignore if parsing fails)
        lu_utc = q.get("last_updated_utc")
        lu_riyadh = q.get("last_updated_riyadh")
        try:
            if lu_utc:
                lq.last_updated_utc = datetime.fromisoformat(lu_utc)
        except Exception:
            pass
        try:
            if lu_riyadh:
                lq.last_updated_riyadh = datetime.fromisoformat(lu_riyadh)
        except Exception:
            pass

        rows.append(_legacy_to_sheet_row(lq))

    return {
        "headers": headers,
        "rows": rows,
        "meta": meta,
    }
