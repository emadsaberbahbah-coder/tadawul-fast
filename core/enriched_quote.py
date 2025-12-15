# routes/enriched_quote.py
"""
routes/enriched_quote.py
===========================================================
Enriched Quote Routes (Google Sheets + API) – v2.2 (PROD SAFE)

Stable endpoints (Sheets + Apps Script + API):
- GET  /v1/enriched/health
- GET  /v1/enriched/headers?sheet_name=KSA_Tadawul
- GET  /v1/enriched/quote?symbol=1120.SR&format=quote|row|both
- POST /v1/enriched/quotes      {symbols:[...], sheet_name?, operation?}  (format=rows|quotes|both)
- POST /v1/enriched/sheet-rows  {tickers:[...] OR symbols:[...], sheet_name?}  ✅ used by google_sheets_service.py

Contract for /sheet-rows (important):
- Always returns: {headers:[], rows:[], status:"success|partial|error|skipped", error?:str}

Auth:
- X-APP-TOKEN checked against APP_TOKEN / BACKUP_APP_TOKEN (from settings/env)
- If no tokens configured => OPEN mode (no auth)

Defensive limits:
- ENRICHED_MAX_TICKERS (default 250)
- ENRICHED_BATCH_SIZE (default 40)
"""

from __future__ import annotations

import asyncio
import logging
import os
from functools import lru_cache
from typing import Any, Dict, List, Optional, Sequence, Literal

from fastapi import APIRouter, Body, Header, HTTPException, Query
from pydantic import BaseModel, Field

from core.config import get_settings
from core.data_engine_v2 import DataEngine, UnifiedQuote

# Optional (preferred) helpers
try:
    from core.enriched_quote import EnrichedQuote  # type: ignore
except Exception:  # pragma: no cover
    EnrichedQuote = None  # type: ignore

try:
    from core.schemas import BatchProcessRequest, get_headers_for_sheet  # type: ignore
except Exception:  # pragma: no cover
    BatchProcessRequest = None  # type: ignore
    get_headers_for_sheet = None  # type: ignore

logger = logging.getLogger("routes.enriched_quote")
router = APIRouter(prefix="/v1/enriched", tags=["enriched"])

# =============================================================================
# Engine singleton (shared)
# =============================================================================
_ENGINE: Optional[DataEngine] = None
_ENGINE_LOCK = asyncio.Lock()


async def _get_engine() -> DataEngine:
    global _ENGINE
    if _ENGINE is not None:
        return _ENGINE
    async with _ENGINE_LOCK:
        if _ENGINE is None:
            _ENGINE = DataEngine()
            logger.info("[enriched] DataEngine initialized (singleton).")
    return _ENGINE


# =============================================================================
# Auth (X-APP-TOKEN)
# =============================================================================
@lru_cache(maxsize=1)
def _allowed_tokens() -> List[str]:
    tokens: List[str] = []

    # 1) settings
    try:
        s = get_settings()
        for attr in ("app_token", "backup_app_token"):
            v = getattr(s, attr, None)
            if isinstance(v, str) and v.strip():
                tokens.append(v.strip())
    except Exception:
        pass

    # 2) env vars
    for k in ("APP_TOKEN", "BACKUP_APP_TOKEN"):
        v = os.getenv(k, "")
        if v and v.strip():
            tokens.append(v.strip())

    # de-dup preserve order
    out: List[str] = []
    seen = set()
    for t in tokens:
        if t not in seen:
            seen.add(t)
            out.append(t)

    if not out:
        logger.warning("[enriched] No APP_TOKEN configured -> endpoints are OPEN (no auth).")
    return out


def _require_token(x_app_token: Optional[str]) -> None:
    allowed = _allowed_tokens()
    if not allowed:
        return  # open mode

    if not x_app_token or x_app_token.strip() not in allowed:
        raise HTTPException(status_code=401, detail="Unauthorized (invalid or missing X-APP-TOKEN).")


# =============================================================================
# Limits / helpers
# =============================================================================
def _get_int_setting(name: str, default: int) -> int:
    try:
        s = get_settings()
        v = getattr(s, name, None)
        if isinstance(v, int) and v > 0:
            return v
    except Exception:
        pass

    # env override
    try:
        v2 = os.getenv(name, "")
        if v2.strip():
            n = int(v2.strip())
            if n > 0:
                return n
    except Exception:
        pass

    return default


def _clean_symbols(symbols: Sequence[Any]) -> List[str]:
    out: List[str] = []
    for x in symbols or []:
        if x is None:
            continue
        s = str(x).strip()
        if not s:
            continue
        out.append(s)
    return out


def _dedup_keep_order(items: List[str]) -> List[str]:
    seen = set()
    out: List[str] = []
    for s in items:
        if s not in seen:
            seen.add(s)
            out.append(s)
    return out


def _quote_to_dict(q: Any) -> Dict[str, Any]:
    try:
        return q.model_dump(exclude_none=False)  # pydantic v2
    except Exception:
        try:
            return dict(getattr(q, "__dict__", {}) or {})
        except Exception:
            return {}


def _default_headers_59() -> List[str]:
    # Safe fallback order aligned to UnifiedQuote fields (human headers)
    return [
        "Symbol", "Company Name", "Sector", "Industry", "Sub-Sector", "Market", "Currency", "Listing Date",
        "Shares Outstanding", "Free Float %", "Market Cap", "Free Float Market Cap",
        "Current Price", "Previous Close", "Open", "Day High", "Day Low",
        "52W High", "52W Low", "52W Position %",
        "Price Change", "Percent Change",
        "Volume", "Avg Volume (30D)", "Value Traded", "Turnover %", "Liquidity Score",
        "EPS (TTM)", "Forward EPS", "P/E (TTM)", "Forward P/E", "P/B", "P/S", "EV/EBITDA",
        "Dividend Yield %", "Dividend Rate", "Payout Ratio %",
        "ROE %", "ROA %", "Net Margin %", "EBITDA Margin %",
        "Revenue Growth %", "Net Income Growth %",
        "Beta", "Debt/Equity", "Current Ratio", "Quick Ratio",
        "RSI (14)", "Volatility (30D)", "MACD", "MA20", "MA50",
        "Fair Value", "Target Price", "Upside %", "Valuation Label", "Analyst Rating",
        "Value Score", "Quality Score", "Momentum Score", "Opportunity Score", "Overall Score",
        "Recommendation", "Confidence", "Risk Score",
        "Data Source", "Data Quality", "Last Updated (UTC)", "Last Updated (Riyadh)", "Error",
    ]


def _headers_for_sheet(sheet_name: Optional[str]) -> List[str]:
    if get_headers_for_sheet:
        try:
            h = get_headers_for_sheet(sheet_name)
            if isinstance(h, list) and h:
                return [str(x).strip() for x in h if str(x).strip()]
        except Exception:
            pass
    return _default_headers_59()


def _row_from_unified_fallback(q: UnifiedQuote, headers: List[str]) -> List[Any]:
    """
    If core.enriched_quote.EnrichedQuote is missing, build a best-effort row
    from UnifiedQuote.model_dump() using common header->field mappings.
    """
    d = _quote_to_dict(q)
    m = {
        "symbol": "symbol",
        "company name": "name",
        "name": "name",
        "sector": "sector",
        "industry": "industry",
        "sub-sector": "sub_sector",
        "sub sector": "sub_sector",
        "market": "market",
        "currency": "currency",
        "listing date": "listing_date",
        "shares outstanding": "shares_outstanding",
        "free float %": "free_float",
        "market cap": "market_cap",
        "free float market cap": "free_float_market_cap",
        "current price": "current_price",
        "previous close": "previous_close",
        "open": "open",
        "day high": "day_high",
        "day low": "day_low",
        "52w high": "high_52w",
        "52w low": "low_52w",
        "52w position %": "position_52w_percent",
        "price change": "price_change",
        "percent change": "percent_change",
        "volume": "volume",
        "avg volume (30d)": "avg_volume_30d",
        "value traded": "value_traded",
        "turnover %": "turnover_percent",
        "liquidity score": "liquidity_score",
        "eps (ttm)": "eps_ttm",
        "forward eps": "forward_eps",
        "p/e (ttm)": "pe_ttm",
        "forward p/e": "forward_pe",
        "p/b": "pb",
        "p/s": "ps",
        "ev/ebitda": "ev_ebitda",
        "dividend yield %": "dividend_yield",
        "dividend rate": "dividend_rate",
        "payout ratio %": "payout_ratio",
        "roe %": "roe",
        "roa %": "roa",
        "net margin %": "net_margin",
        "ebitda margin %": "ebitda_margin",
        "revenue growth %": "revenue_growth",
        "net income growth %": "net_income_growth",
        "beta": "beta",
        "debt/equity": "debt_to_equity",
        "current ratio": "current_ratio",
        "quick ratio": "quick_ratio",
        "rsi (14)": "rsi_14",
        "volatility (30d)": "volatility_30d",
        "macd": "macd",
        "ma20": "ma20",
        "ma50": "ma50",
        "fair value": "fair_value",
        "target price": "target_price",
        "upside %": "upside_percent",
        "valuation label": "valuation_label",
        "analyst rating": "analyst_rating",
        "value score": "value_score",
        "quality score": "quality_score",
        "momentum score": "momentum_score",
        "opportunity score": "opportunity_score",
        "overall score": "overall_score",
        "recommendation": "recommendation",
        "confidence": "confidence",
        "risk score": "risk_score",
        "data source": "data_source",
        "data quality": "data_quality",
        "last updated (utc)": "last_updated_utc",
        "last updated (riyadh)": "last_updated_riyadh",
        "error": "error",
    }

    row: List[Any] = []
    for h in headers:
        key = (h or "").strip().lower()
        field = m.get(key)
        row.append(d.get(field) if field else None)
    return row


def _build_row_payload(q: UnifiedQuote, headers: List[str]) -> Dict[str, Any]:
    if EnrichedQuote:
        eq = EnrichedQuote.from_unified(q)  # type: ignore
        return {"headers": list(headers), "row": eq.to_row(headers), "quote": _quote_to_dict(eq)}
    return {"headers": list(headers), "row": _row_from_unified_fallback(q, headers), "quote": _quote_to_dict(q)}


def _sheet_rows_error_payload(symbols: List[str], err: str, headers: Optional[List[str]] = None) -> Dict[str, Any]:
    h = headers or ["Symbol", "Error"]
    rows = []
    for s in symbols:
        if "Error" in h:
            r = [None] * len(h)
            r[h.index("Symbol")] = s if "Symbol" in h else s
            r[h.index("Error")] = err
            rows.append(r)
        else:
            rows.append([s, err])
    return {"headers": h, "rows": rows, "status": "error", "error": err}


# =============================================================================
# Request Models
# =============================================================================
class SheetRowsRequest(BaseModel):
    tickers: List[str] = Field(default_factory=list)
    symbols: List[str] = Field(default_factory=list)
    sheet_name: Optional[str] = None


class _FallbackBatchProcessRequest(BaseModel):
    symbols: List[str] = Field(default_factory=list)
    sheet_name: Optional[str] = None
    operation: Optional[str] = "enriched_quotes"


# =============================================================================
# Endpoints
# =============================================================================
@router.get("/health", tags=["system"])
async def enriched_health() -> Dict[str, Any]:
    max_t = _get_int_setting("ENRICHED_MAX_TICKERS", 250)
    batch_sz = _get_int_setting("ENRICHED_BATCH_SIZE", 40)
    eng = await _get_engine()
    return {
        "status": "ok",
        "module": "routes.enriched_quote",
        "engine": "DataEngineV2",
        "providers": list(getattr(eng, "enabled_providers", []) or []),
        "limits": {"enriched_max_tickers": max_t, "enriched_batch_size": batch_sz},
        "auth": "open" if not _allowed_tokens() else "token",
    }


@router.get("/headers")
async def get_headers(
    sheet_name: Optional[str] = Query(default=None, description="Optional sheet name to resolve headers."),
    x_app_token: Optional[str] = Header(default=None, alias="X-APP-TOKEN"),
) -> Dict[str, Any]:
    _require_token(x_app_token)
    headers = _headers_for_sheet(sheet_name)
    return {"sheet_name": sheet_name, "headers": headers, "count": len(headers)}


@router.get("/quote")
async def enriched_quote(
    symbol: str = Query(..., description="Ticker symbol (e.g., 1120.SR, AAPL, ^GSPC)."),
    sheet_name: Optional[str] = Query(default=None, description="Optional sheet name for header/row alignment."),
    format: Literal["quote", "row", "both"] = Query(default="quote", description="Return quote, row, or both."),
    x_app_token: Optional[str] = Header(default=None, alias="X-APP-TOKEN"),
) -> Dict[str, Any]:
    _require_token(x_app_token)

    eng = await _get_engine()
    q = await eng.get_quote(symbol)

    if format == "quote":
        return _quote_to_dict(EnrichedQuote.from_unified(q) if EnrichedQuote else q)  # type: ignore

    headers = _headers_for_sheet(sheet_name)
    payload = _build_row_payload(q, headers)

    if format == "row":
        return {"symbol": symbol, "headers": payload["headers"], "row": payload["row"]}

    payload["sheet_name"] = sheet_name
    return payload


@router.post("/quotes")
async def enriched_quotes(
    req: Any = Body(...),
    format: Literal["rows", "quotes", "both"] = Query(default="rows", description="Return rows, quotes, or both."),
    x_app_token: Optional[str] = Header(default=None, alias="X-APP-TOKEN"),
) -> Dict[str, Any]:
    _require_token(x_app_token)

    # Accept either core.schemas.BatchProcessRequest or fallback
    if BatchProcessRequest and isinstance(req, dict):
        req_obj = BatchProcessRequest(**req)  # type: ignore
    elif BatchProcessRequest and not isinstance(req, dict):
        req_obj = req  # already parsed
    else:
        req_obj = _FallbackBatchProcessRequest(**(req or {}))

    symbols = _dedup_keep_order(_clean_symbols(getattr(req_obj, "symbols", []) or []))
    sheet_name = getattr(req_obj, "sheet_name", None)
    operation = getattr(req_obj, "operation", None) or "enriched_quotes"

    headers = _headers_for_sheet(sheet_name)
    if not symbols:
        return {"operation": operation, "sheet_name": sheet_name, "count": 0, "symbols": [], "headers": headers, "rows": [], "status": "skipped"}

    max_t = _get_int_setting("ENRICHED_MAX_TICKERS", 250)
    batch_sz = _get_int_setting("ENRICHED_BATCH_SIZE", 40)
    if len(symbols) > max_t:
        raise HTTPException(status_code=400, detail=f"Too many symbols ({len(symbols)}). Max allowed is {max_t}.")

    eng = await _get_engine()

    rows_out: List[List[Any]] = []
    quotes_out: List[Dict[str, Any]] = []

    for i in range(0, len(symbols), batch_sz):
        chunk = symbols[i : i + batch_sz]
        quotes = await eng.get_quotes(chunk)

        if format in ("rows", "both"):
            for q in quotes:
                payload = _build_row_payload(q, headers)
                rows_out.append(payload["row"])

        if format in ("quotes", "both"):
            for q in quotes:
                quotes_out.append(_quote_to_dict(EnrichedQuote.from_unified(q) if EnrichedQuote else q))  # type: ignore

    resp: Dict[str, Any] = {
        "operation": operation,
        "sheet_name": sheet_name,
        "count": len(symbols),
        "symbols": symbols,
        "headers": headers,
        "status": "success",
    }
    if format in ("rows", "both"):
        resp["rows"] = rows_out
    if format in ("quotes", "both"):
        resp["quotes"] = quotes_out
    return resp


@router.post("/sheet-rows")
async def enriched_sheet_rows(
    req: SheetRowsRequest = Body(...),
    x_app_token: Optional[str] = Header(default=None, alias="X-APP-TOKEN"),
) -> Dict[str, Any]:
    """
    ✅ Primary endpoint used by google_sheets_service.py
    Expects: {tickers:[...], sheet_name:"KSA_Tadawul"} (or symbols:[...])
    Returns: {headers, rows, status, error?}
    """
    _require_token(x_app_token)

    symbols = _dedup_keep_order(_clean_symbols((req.tickers or []) + (req.symbols or [])))
    headers = _headers_for_sheet(req.sheet_name)

    if not symbols:
        return {"headers": headers, "rows": [], "status": "skipped", "error": "No tickers provided"}

    max_t = _get_int_setting("ENRICHED_MAX_TICKERS", 250)
    batch_sz = _get_int_setting("ENRICHED_BATCH_SIZE", 40)
    if len(symbols) > max_t:
        return _sheet_rows_error_payload(symbols[:max_t], f"Too many tickers ({len(symbols)}). Max allowed is {max_t}.", headers=headers)

    eng = await _get_engine()

    rows: List[List[Any]] = []
    any_error = False

    try:
        for i in range(0, len(symbols), batch_sz):
            chunk = symbols[i : i + batch_sz]
            quotes = await eng.get_quotes(chunk)
            for q in quotes:
                payload = _build_row_payload(q, headers)
                rows.append(payload["row"])
                if getattr(q, "error", None):
                    any_error = True
    except Exception as exc:
        logger.exception("[enriched] /sheet-rows failed", exc_info=exc)
        return _sheet_rows_error_payload(symbols, str(exc), headers=headers)

    return {
        "headers": headers,
        "rows": rows,
        "status": "partial" if any_error else "success",
        "error": None,
    }
