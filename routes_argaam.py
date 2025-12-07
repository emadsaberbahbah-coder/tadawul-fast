"""
routes_argaam.py
------------------------------------------------------------
KSA / Argaam routes for Tadawul Fast Bridge – v1.0

GOAL
- Provide a clean, dedicated KSA provider route that does NOT depend on EODHD.
- Designed to work alongside:
    • main.py              (FastAPI app + token auth)
    • core.data_engine     (unified engine for KSA + Global)
    • google_sheets_service.py (sheet-rows pattern)
    • google_apps_script_client.py / index.html (JavaScript / Apps Script)

ASSUMPTION
- You have (or will build) a KSA/Argaam "gateway" service (often Java)
  which exposes an HTTP JSON API for Tadawul tickers, e.g.:

    GET {ARGAAM_GATEWAY_URL}/quote?symbol=1120.SR

  or similar. This Python router calls that gateway and normalizes the data.

ENV VARS (set in Render dashboard or locally)
- ARGAAM_GATEWAY_URL  -> e.g. https://your-ksa-gateway.example.com
- ARGAAM_API_KEY      -> optional, if your gateway requires an API key header

HOW TO PLUG INTO main.py
------------------------
1) Add import:

    from routes import routes_argaam

2) Include router (with token dependency):

    app.include_router(
        routes_argaam.router,
        dependencies=[Depends(require_app_token)],
    )

Then you can call:

    GET  /v1/argaam/health
    GET  /v1/argaam/quote?symbol=1120.SR
    GET  /v1/argaam/quotes?tickers=1120.SR,1180.SR
    POST /v1/argaam/sheet-rows   { "tickers": ["1120.SR","1180.SR"] }

The /sheet-rows response is compatible with Google Sheets / JS:
    { "headers": [...], "rows": [[...], ...], "meta": {...} }
"""

from __future__ import annotations

import os
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional

import httpx
from fastapi import APIRouter, HTTPException, Query, status
from pydantic import BaseModel, Field

from env import APP_NAME, APP_VERSION

router = APIRouter(
    prefix="/v1/argaam",
    tags=["KSA / Argaam"],
)

# ----------------------------------------------------------------------
# ENV CONFIG
# ----------------------------------------------------------------------

ARGAAM_GATEWAY_URL: str = (os.getenv("ARGAAM_GATEWAY_URL") or "").strip()
ARGAAM_API_KEY: str = (os.getenv("ARGAAM_API_KEY") or "").strip()

RIYADH_TZ = timezone(timedelta(hours=3))


# ----------------------------------------------------------------------
# MODELS
# ----------------------------------------------------------------------


class ArgaamQuote(BaseModel):
    """
    Normalized KSA quote using your Argaam / Tadawul gateway.

    NOTE:
    - remote_raw can always be inspected for debugging if mappings need tuning.
    """

    symbol: str
    name: Optional[str] = None
    sector: Optional[str] = None
    market: Optional[str] = None
    currency: Optional[str] = None

    last_price: Optional[float] = None
    previous_close: Optional[float] = None
    change: Optional[float] = None
    change_percent: Optional[float] = None

    volume: Optional[float] = None
    market_cap: Optional[float] = None

    fifty_two_week_high: Optional[float] = Field(
        default=None, alias="fiftyTwoWeekHigh"
    )
    fifty_two_week_low: Optional[float] = Field(default=None, alias="fiftyTwoWeekLow")

    last_updated_utc: Optional[datetime] = None
    last_updated_riyadh: Optional[datetime] = None

    data_source: str = "argaam-gateway"
    remote_raw: Optional[Dict[str, Any]] = None

    class Config:
        allow_population_by_field_name = True


class ArgaamSheetRowsRequest(BaseModel):
    tickers: List[str] = Field(
        default_factory=list,
        description="List of KSA symbols (e.g. ['1120.SR','1180.SR']). Non-KSA are ignored.",
    )


class ArgaamSheetRowsResponse(BaseModel):
    headers: List[str]
    rows: List[List[Any]]
    meta: Dict[str, Any]


# ----------------------------------------------------------------------
# INTERNAL HELPERS
# ----------------------------------------------------------------------


def _ensure_gateway_configured() -> None:
    if not ARGAAM_GATEWAY_URL:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="ARGAAM_GATEWAY_URL is not configured. Set it in environment.",
        )


def _safe_float(x: Any) -> Optional[float]:
    try:
        if x is None:
            return None
        return float(x)
    except Exception:
        return None


def _parse_argaam_payload(symbol: str, payload: Dict[str, Any]) -> ArgaamQuote:
    """
    Map arbitrary gateway JSON into ArgaamQuote.

    We try several common key names; you can adjust if your gateway
    uses different fields.

    This is intentionally defensive and keeps `remote_raw` for debugging.
    """
    # Common key variants from typical JSON APIs
    name = payload.get("name") or payload.get("companyName") or payload.get("Name")
    sector = payload.get("sector") or payload.get("Sector")
    market = payload.get("market") or payload.get("exchange") or "Tadawul"
    currency = payload.get("currency") or payload.get("Currency") or "SAR"

    last_price = (
        payload.get("lastPrice")
        or payload.get("last")
        or payload.get("price")
        or payload.get("LastPrice")
    )
    prev_close = payload.get("previousClose") or payload.get("prevClose")
    change_val = payload.get("change") or payload.get("Change")
    change_pct = (
        payload.get("changePercent")
        or payload.get("changePct")
        or payload.get("ChangePercent")
    )

    volume = payload.get("volume") or payload.get("Volume")
    market_cap = payload.get("marketCap") or payload.get("MarketCap")

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
    last_utc: Optional[datetime] = None
    if ts:
        # Try ISO format first
        if isinstance(ts, str):
            try:
                last_utc = datetime.fromisoformat(ts.replace("Z", "+00:00"))
            except Exception:
                last_utc = None
        elif isinstance(ts, (int, float)):
            # treat as unix seconds
            try:
                last_utc = datetime.fromtimestamp(float(ts), tz=timezone.utc)
            except Exception:
                last_utc = None

    last_riyadh: Optional[datetime] = None
    if last_utc:
        if last_utc.tzinfo is None:
            last_utc = last_utc.replace(tzinfo=timezone.utc)
        last_riyadh = last_utc.astimezone(RIYADH_TZ)

    return ArgaamQuote(
        symbol=symbol.upper(),
        name=name,
        sector=sector,
        market=market,
        currency=currency,
        last_price=_safe_float(last_price),
        previous_close=_safe_float(prev_close),
        change=_safe_float(change_val),
        change_percent=_safe_float(change_pct),
        volume=_safe_float(volume),
        market_cap=_safe_float(market_cap),
        fiftyTwoWeekHigh=_safe_float(high_52w),
        fiftyTwoWeekLow=_safe_float(low_52w),
        last_updated_utc=last_utc,
        last_updated_riyadh=last_riyadh,
        remote_raw=payload,
    )


async def _fetch_argaam_quote(symbol: str) -> ArgaamQuote:
    """
    Call the external KSA/Argaam gateway for a single symbol.

    EXPECTED GATEWAY CONTRACT (example):
        GET {ARGAAM_GATEWAY_URL}/quote?symbol=1120.SR

    You can adjust the path/params if your gateway differs.
    """
    _ensure_gateway_configured()

    if not symbol.upper().endswith(".SR"):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="routes_argaam only supports KSA symbols ending with '.SR'.",
        )

    url = ARGAAM_GATEWAY_URL.rstrip("/") + "/quote"
    params = {"symbol": symbol.upper()}
    headers: Dict[str, str] = {"Accept": "application/json"}
    if ARGAAM_API_KEY:
        headers["X-API-KEY"] = ARGAAM_API_KEY

    timeout = httpx.Timeout(15.0, connect=5.0)

    async with httpx.AsyncClient(timeout=timeout) as client:
        try:
            resp = await client.get(url, params=params, headers=headers)
        except httpx.RequestError as exc:
            raise HTTPException(
                status_code=status.HTTP_502_BAD_GATEWAY,
                detail=f"Error connecting to Argaam gateway: {exc}",
            ) from exc

    if resp.status_code < 200 or resp.status_code >= 300:
        raise HTTPException(
            status_code=status.HTTP_502_BAD_GATEWAY,
            detail=f"Argaam gateway HTTP {resp.status_code}: {resp.text[:200]}",
        )

    try:
        payload = resp.json()
    except Exception as exc:
        raise HTTPException(
            status_code=status.HTTP_502_BAD_GATEWAY,
            detail=f"Argaam gateway returned invalid JSON: {exc}",
        ) from exc

    if not isinstance(payload, dict):
        raise HTTPException(
            status_code=status.HTTP_502_BAD_GATEWAY,
            detail=f"Unexpected Argaam gateway response type: {type(payload)}",
        )

    return _parse_argaam_payload(symbol, payload)


def _build_sheet_headers() -> List[str]:
    """
    Headers for Google Sheets / Apps Script (KSA Argaam view).
    """
    return [
        "Symbol",
        "Company Name",
        "Sector",
        "Market",
        "Currency",
        "Last Price",
        "Previous Close",
        "Change",
        "Change %",
        "Volume",
        "Market Cap",
        "52W High",
        "52W Low",
        "Last Updated (UTC)",
        "Last Updated (Riyadh)",
        "Data Source",
    ]


def _quote_to_sheet_row(q: ArgaamQuote) -> List[Any]:
    """
    Convert ArgaamQuote into a single row for Google Sheets.
    """
    return [
        q.symbol,
        q.name or "",
        q.sector or "",
        q.market or "",
        q.currency or "",
        q.last_price,
        q.previous_close,
        q.change,
        q.change_percent,
        q.volume,
        q.market_cap,
        q.fifty_two_week_high,
        q.fifty_two_week_low,
        q.last_updated_utc.isoformat() if q.last_updated_utc else None,
        q.last_updated_riyadh.isoformat() if q.last_updated_riyadh else None,
        q.data_source,
    ]


# ----------------------------------------------------------------------
# ENDPOINTS
# ----------------------------------------------------------------------


@router.get("/health")
async def argaam_health() -> Dict[str, Any]:
    """
    Lightweight health endpoint for KSA / Argaam gateway.

    Does NOT call the external gateway to keep it fast and cheap.
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
            "Set ARGAAM_GATEWAY_URL to plug in your KSA/Argaam Java service.",
        ],
    }


@router.get("/quote", response_model=ArgaamQuote)
async def get_argaam_quote(symbol: str = Query(..., description="KSA symbol, e.g. 1120.SR")):
    """
    Return a single KSA quote from the Argaam/Tadawul gateway.

    Example:
        GET /v1/argaam/quote?symbol=1120.SR
    """
    return await _fetch_argaam_quote(symbol)


@router.get("/quotes")
async def get_argaam_quotes(
    tickers: str = Query(
        ...,
        description="Comma-separated KSA symbols, e.g. 1120.SR,1180.SR,1010.SR",
    )
) -> Dict[str, Any]:
    """
    Return multiple KSA quotes.

    Response:
        {
          "quotes": [ {ArgaamQuote}, ... ],
          "meta": { ... }
        }
    """
    raw_symbols = [t.strip() for t in tickers.split(",") if t.strip()]
    symbols = [s for s in raw_symbols if s.upper().endswith(".SR")]

    if not symbols:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="No valid KSA (.SR) symbols provided.",
        )

    quotes: List[ArgaamQuote] = []
    # Fetch sequentially for simplicity; you can switch to asyncio.gather if desired.
    for sym in symbols:
        q = await _fetch_argaam_quote(sym)
        quotes.append(q)

    return {
        "quotes": [q.dict(by_alias=True) for q in quotes],
        "meta": {
            "requested": raw_symbols,
            "resolved_ksa": symbols,
            "count": len(quotes),
            "provider": "argaam-gateway",
            "note": "KSA quotes from Argaam/Tadawul gateway (no EODHD).",
        },
    }


@router.post("/sheet-rows", response_model=ArgaamSheetRowsResponse)
async def get_argaam_sheet_rows(body: ArgaamSheetRowsRequest) -> ArgaamSheetRowsResponse:
    """
    Sheet-friendly representation for KSA quotes:

        POST /v1/argaam/sheet-rows
        {
          "tickers": ["1120.SR","1180.SR"]
        }

    Response:
        {
          "headers": [...],
          "rows": [[...], ...],
          "meta": {...}
        }

    This format is directly compatible with:
      - google_sheets_service.py (values.update)
      - Google Apps Script / JavaScript (index.html) expecting headers+rows
    """
    raw_symbols = [t.strip() for t in (body.tickers or []) if t and t.strip()]
    symbols = [s for s in raw_symbols if s.upper().endswith(".SR")]

    if not symbols:
        return ArgaamSheetRowsResponse(
            headers=_build_sheet_headers(),
            rows=[],
            meta={
                "requested": raw_symbols,
                "resolved_ksa": [],
                "count": 0,
                "provider": "argaam-gateway",
                "note": "No valid KSA (.SR) tickers provided.",
            },
        )

    quotes: List[ArgaamQuote] = []
    for sym in symbols:
        q = await _fetch_argaam_quote(sym)
        quotes.append(q)

    headers = _build_sheet_headers()
    rows = [_quote_to_sheet_row(q) for q in quotes]

    meta = {
        "requested": raw_symbols,
        "resolved_ksa": symbols,
        "count": len(quotes),
        "provider": "argaam-gateway",
        "note": "Sheet rows for KSA (.SR) tickers using Argaam/Tadawul gateway.",
    }

    return ArgaamSheetRowsResponse(headers=headers, rows=rows, meta=meta)
