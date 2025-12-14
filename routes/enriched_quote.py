"""
routes/enriched_quote.py
===========================================================
ENRICHED QUOTE ROUTES – Engine v2 aligned (v3.4.0)

- Uses core.data_engine_v2.DataEngine only (no direct provider calls).
- KSA-safe by delegation inside engine (KSA never uses EODHD).
- Provides:
    • GET  /v1/enriched/health
    • GET  /v1/enriched/quote?symbol=1120.SR
    • POST /v1/enriched/quotes
    • POST /v1/enriched/sheet-rows   (headers + rows for Google Sheets)

Defensive:
- Never raises for sheet pipelines; returns MISSING + error field.
"""

from __future__ import annotations

import asyncio
import logging
import re
from datetime import datetime, timezone
from functools import lru_cache
from typing import Any, Dict, List, Optional, Sequence, Tuple

from fastapi import APIRouter, Query
from pydantic import BaseModel, Field

from env import settings
from core.data_engine_v2 import DataEngine, UnifiedQuote
from core.enriched_quote import EnrichedQuote
from core.scoring_engine import enrich_with_scores

logger = logging.getLogger("routes.enriched_quote")
ROUTE_VERSION = "3.4.0"

DEFAULT_BATCH_SIZE = int(settings.enriched_batch_size)
DEFAULT_BATCH_TIMEOUT = float(settings.enriched_batch_timeout_sec)
DEFAULT_MAX_TICKERS = int(settings.enriched_max_tickers)
DEFAULT_CONCURRENCY = int(settings.enriched_batch_concurrency)

# ---------------------------------------------------------------------------
# Optional schema helper (preferred)
# ---------------------------------------------------------------------------
try:
    from core.schemas import get_headers_for_sheet  # type: ignore
except Exception:
    def get_headers_for_sheet(sheet_name: str) -> List[str]:
        # Safe rich default (works even if some fields are missing)
        return [
            "Symbol","Company Name","Sector","Sub-Sector","Market","Currency","Listing Date",
            "Last Price","Previous Close","Price Change","Percent Change",
            "Day High","Day Low","52W High","52W Low","52W Position %",
            "Volume","Avg Volume (30D)","Value Traded","Turnover %",
            "Shares Outstanding","Free Float %","Market Cap","Free Float Market Cap",
            "Liquidity Score",
            "EPS (TTM)","Forward EPS","P/E (TTM)","Forward P/E","P/B","P/S","EV/EBITDA",
            "Dividend Yield %","Dividend Rate","Payout Ratio %",
            "ROE %","ROA %","Net Margin %","EBITDA Margin %",
            "Revenue Growth %","Net Income Growth %",
            "Beta","Volatility (30D)","RSI (14)",
            "Fair Value","Upside %","Valuation Label",
            "Value Score","Quality Score","Momentum Score","Opportunity Score","Overall Score","Recommendation",
            "Data Source","Data Quality","Last Updated (UTC)","Last Updated (Local)","Error",
        ]

# ---------------------------------------------------------------------------
# Engine Singleton
# ---------------------------------------------------------------------------
@lru_cache(maxsize=1)
def _get_engine() -> DataEngine:
    logger.info("routes.enriched_quote: Initializing DataEngine v2 singleton")
    return DataEngine()

# ---------------------------------------------------------------------------
# Models
# ---------------------------------------------------------------------------
class EnrichedBatchRequest(BaseModel):
    tickers: List[str] = Field(default_factory=list)
    symbols: Optional[List[str]] = Field(default=None, description="Alias for tickers (compat)")
    sheet_name: Optional[str] = Field(default=None, description="For schema selection")

class SheetRowsResponse(BaseModel):
    headers: List[str]
    rows: List[List[Any]]
    meta: Dict[str, Any] = Field(default_factory=dict)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def _now_utc_iso() -> str:
    return datetime.now(timezone.utc).isoformat()

def _normalize_symbol(symbol: str) -> str:
    s = (symbol or "").strip().upper()
    if not s:
        return ""
    if s.startswith("TADAWUL:"):
        s = s.split(":", 1)[1].strip()
    if s.endswith(".TADAWUL"):
        s = s.replace(".TADAWUL", ".SR")
    if s.isdigit():
        s = f"{s}.SR"
    return s

def _clean_tickers(items: Sequence[str]) -> List[str]:
    raw = [_normalize_symbol(x) for x in (items or [])]
    raw = [x for x in raw if x]
    seen = set()
    out: List[str] = []
    for x in raw:
        if x in seen:
            continue
        seen.add(x)
        out.append(x)
    return out

def _chunk(items: List[str], size: int) -> List[List[str]]:
    if size <= 0:
        return [items]
    return [items[i : i + size] for i in range(0, len(items), size)]

def _pad(row: List[Any], n: int) -> List[Any]:
    if len(row) < n:
        return row + [None] * (n - len(row))
    return row[:n]

def _to_local_time_str(utc_iso: Optional[str]) -> Optional[str]:
    if not utc_iso:
        return None
    try:
        from zoneinfo import ZoneInfo
        ts = datetime.fromisoformat(str(utc_iso))
        if ts.tzinfo is None:
            ts = ts.replace(tzinfo=timezone.utc)
        local = ts.astimezone(ZoneInfo("Asia/Riyadh"))
        return local.isoformat()
    except Exception:
        return utc_iso

# ---------------------------------------------------------------------------
# Fetch (chunked + concurrency + timeout)
# ---------------------------------------------------------------------------
async def _get_quotes_chunked(
    symbols: List[str],
    *,
    batch_size: int = DEFAULT_BATCH_SIZE,
    timeout_sec: float = DEFAULT_BATCH_TIMEOUT,
    max_concurrency: int = DEFAULT_CONCURRENCY,
) -> Dict[str, UnifiedQuote]:
    clean = _clean_tickers(symbols)
    if not clean:
        return {}

    if len(clean) > DEFAULT_MAX_TICKERS:
        clean = clean[:DEFAULT_MAX_TICKERS]

    engine = _get_engine()
    chunks = _chunk(clean, batch_size)
    sem = asyncio.Semaphore(max(1, max_concurrency))

    async def _run_chunk(chunk_syms: List[str]) -> Tuple[List[str], List[UnifiedQuote] | Exception]:
        async with sem:
            try:
                res = await asyncio.wait_for(engine.get_enriched_quotes(chunk_syms), timeout=timeout_sec)
                return chunk_syms, res
            except Exception as e:
                return chunk_syms, e

    results = await asyncio.gather(*[_run_chunk(c) for c in chunks])

    out: Dict[str, UnifiedQuote] = {}
    for chunk_syms, res in results:
        if isinstance(res, Exception):
            msg = "Engine batch timeout" if isinstance(res, asyncio.TimeoutError) else f"Engine batch error: {res}"
            logger.warning("enriched_quote: %s for chunk(size=%d)", msg, len(chunk_syms))
            for s in chunk_syms:
                out[s.upper()] = UnifiedQuote(symbol=s.upper(), data_quality="MISSING", error=msg).finalize()
            continue

        returned = list(res or [])
        chunk_map = {q.symbol.upper(): q for q in returned}
        for s in chunk_syms:
            s_norm = s.upper()
            out[s_norm] = chunk_map.get(s_norm) or UnifiedQuote(symbol=s_norm, data_quality="MISSING", error="No data returned").finalize()

    return out

# ---------------------------------------------------------------------------
# UnifiedQuote -> EnrichedQuote (+ scores + local time)
# ---------------------------------------------------------------------------
def _uq_to_enriched(raw_symbol: str, uq: UnifiedQuote) -> EnrichedQuote:
    sym = _normalize_symbol(raw_symbol) or raw_symbol.strip().upper() or (uq.symbol or "UNKNOWN")

    try:
        eq = EnrichedQuote.from_unified(uq)
    except Exception:
        # fallback: try best effort using dict
        try:
            payload = uq.model_dump() if hasattr(uq, "model_dump") else dict(uq)  # type: ignore
            payload["symbol"] = payload.get("symbol") or sym
            eq = EnrichedQuote(**payload)
        except Exception as exc:
            return EnrichedQuote(symbol=sym, data_quality="MISSING", error=f"Enriched mapping failed: {exc}")

    try:
        eq = enrich_with_scores(eq)
    except Exception as exc:
        # scoring failure should never break response
        if hasattr(eq, "model_copy"):
            eq = eq.model_copy(update={"error": (getattr(eq, "error", None) or "") + f" | Scoring error: {exc}"})
        else:
            eq = eq.copy(update={"error": (getattr(eq, "error", None) or "") + f" | Scoring error: {exc}"})

    # Ensure symbol + local time
    last_utc = getattr(eq, "last_updated_utc", None)
    last_local = _to_local_time_str(last_utc)

    update = {}
    if not getattr(eq, "symbol", None):
        update["symbol"] = sym
    if last_utc and not getattr(eq, "last_updated_local", None):
        update["last_updated_local"] = last_local

    if update:
        if hasattr(eq, "model_copy"):
            eq = eq.model_copy(update=update)
        else:
            eq = eq.copy(update=update)

    return eq

# ---------------------------------------------------------------------------
# Sheet row mapping (header-name driven)
# ---------------------------------------------------------------------------
_ALIAS_MAP = {
    "symbol": ["symbol", "ticker"],
    "company_name": ["name", "company_name"],
    "sub_sector": ["sub_sector", "subsector"],
    "last_price": ["last_price", "current_price", "price"],
    "previous_close": ["previous_close"],
    "price_change": ["price_change", "change"],
    "percent_change": ["percent_change", "change_percent"],
    "day_high": ["day_high", "high"],
    "day_low": ["day_low", "low"],
    "52w_high": ["high_52w", "fifty_two_week_high"],
    "52w_low": ["low_52w", "fifty_two_week_low"],
    "52w_position_%": ["position_52w_percent", "fifty_two_week_position_pct"],
    "avg_volume_(30d)": ["avg_volume_30d", "avg_volume"],
    "value_traded": ["value_traded"],
    "turnover_%": ["turnover_percent", "turnover"],
    "free_float_%": ["free_float"],
    "market_cap": ["market_cap"],
    "free_float_market_cap": ["free_float_market_cap"],
    "liquidity_score": ["liquidity_score"],
    "eps_(ttm)": ["eps_ttm", "eps"],
    "forward_eps": ["forward_eps"],
    "p/e_(ttm)": ["pe_ttm", "pe_ratio"],
    "forward_p/e": ["forward_pe"],
    "p/b": ["pb", "pb_ratio"],
    "p/s": ["ps", "ps_ratio"],
    "ev/ebitda": ["ev_ebitda"],
    "dividend_yield_%": ["dividend_yield"],
    "dividend_rate": ["dividend_rate"],
    "payout_ratio_%": ["payout_ratio"],
    "roe_%": ["roe"],
    "roa_%": ["roa"],
    "net_margin_%": ["net_margin", "profit_margin"],
    "ebitda_margin_%": ["ebitda_margin"],
    "revenue_growth_%": ["revenue_growth"],
    "net_income_growth_%": ["net_income_growth"],
    "volatility_(30d)": ["volatility_30d"],
    "rsi_(14)": ["rsi_14"],
    "fair_value": ["fair_value"],
    "upside_%": ["upside_percent"],
    "valuation_label": ["valuation_label"],
    "value_score": ["value_score"],
    "quality_score": ["quality_score"],
    "momentum_score": ["momentum_score"],
    "opportunity_score": ["opportunity_score"],
    "overall_score": ["overall_score"],
    "recommendation": ["recommendation"],
    "data_source": ["data_source", "primary_provider"],
    "data_quality": ["data_quality"],
    "last_updated_(utc)": ["last_updated_utc"],
    "last_updated_(local)": ["last_updated_local"],
    "error": ["error"],
}

def _keyify_header(h: str) -> str:
    s = (h or "").strip().lower()
    s = re.sub(r"\s+", " ", s)
    s = s.replace("%", " %")
    s = s.replace("(", "").replace(")", "")
    s = s.replace("/", "/")
    s = s.replace("–", "-").replace("—", "-")
    return s

def _lookup(eq_dict: Dict[str, Any], header_key: str) -> Any:
    # direct by normalized header -> snake-ish (best effort)
    # then alias map
    if header_key in _ALIAS_MAP:
        for k in _ALIAS_MAP[header_key]:
            if k in eq_dict and eq_dict[k] is not None:
                return eq_dict[k]
        return None

    # fallback: try convert spaces to underscores
    snake = re.sub(r"[^a-z0-9]+", "_", header_key).strip("_")
    # fix common "p_e" -> "pe"
    snake = snake.replace("p_e", "pe").replace("p_b", "pb").replace("p_s", "ps")
    if snake in eq_dict:
        return eq_dict.get(snake)

    return None

def _eq_to_row(eq: EnrichedQuote, headers: List[str]) -> List[Any]:
    eq_dict = eq.model_dump() if hasattr(eq, "model_dump") else dict(eq)  # type: ignore
    row: List[Any] = []
    for h in headers:
        hk = _keyify_header(h)
        val = _lookup(eq_dict, hk)

        # Display-normalization for some percent fields
        if h and "%" in h and isinstance(val, (int, float)):
            # If value looks like decimal fraction, convert to percent
            if abs(val) <= 1.0 and val != 0.0:
                val = val * 100.0

        row.append(val)
    return row

def _reorder_rows(headers: List[str], rows: List[List[Any]], requested: List[str]) -> List[List[Any]]:
    if not requested or not rows:
        return rows

    m: Dict[str, List[Any]] = {}
    for r in rows:
        if not r:
            continue
        sym = str(r[0]).strip().upper() if len(r) > 0 else ""
        if not sym:
            continue
        m[sym] = r
        if sym.endswith(".SR"):
            m[sym.replace(".SR", "")] = r
        elif sym.isdigit():
            m[f"{sym}.SR"] = r

    out: List[List[Any]] = []
    for t in requested:
        k = _normalize_symbol(t).upper()
        out.append(m.get(k) or m.get(k.replace(".SR", "")) or m.get(f"{k}.SR") or _pad([k], len(headers)))
    return out

# ---------------------------------------------------------------------------
# Router
# ---------------------------------------------------------------------------
router = APIRouter(prefix="/v1/enriched", tags=["Enriched Quotes"])

@router.get("/health")
@router.get("/ping")
async def enriched_health() -> Dict[str, Any]:
    return {
        "status": "ok",
        "module": "routes.enriched_quote",
        "version": ROUTE_VERSION,
        "engine_mode": "v2",
        "batch_size": DEFAULT_BATCH_SIZE,
        "batch_timeout_sec": DEFAULT_BATCH_TIMEOUT,
        "batch_concurrency": DEFAULT_CONCURRENCY,
        "max_tickers": DEFAULT_MAX_TICKERS,
        "time_utc": _now_utc_iso(),
    }

@router.get("/quote", response_model=EnrichedQuote)
async def enriched_single(symbol: str = Query(..., description="Ticker or Symbol e.g. 1120.SR / AAPL")) -> EnrichedQuote:
    t = (symbol or "").strip()
    if not t:
        return EnrichedQuote(symbol="", data_quality="MISSING", error="Symbol is required")

    try:
        engine = _get_engine()
        uq = await engine.get_enriched_quote(_normalize_symbol(t) or t)
        eq = _uq_to_enriched(t, uq)
        return eq
    except Exception as exc:
        logger.exception("enriched_single exception for %s", t)
        return EnrichedQuote(symbol=_normalize_symbol(t) or t.upper(), data_quality="MISSING", error=str(exc))

@router.post("/quotes", response_model=List[EnrichedQuote])
async def enriched_batch(body: EnrichedBatchRequest) -> List[EnrichedQuote]:
    raw = body.tickers or body.symbols or []
    tickers = _clean_tickers(raw)
    if not tickers:
        return []

    uq_map = await _get_quotes_chunked(tickers)
    out: List[EnrichedQuote] = []
    for t in tickers:
        uq = uq_map.get(t.upper()) or UnifiedQuote(symbol=t.upper(), data_quality="MISSING", error="No data returned").finalize()
        out.append(_uq_to_enriched(t, uq))
    return out

@router.post("/sheet-rows", response_model=SheetRowsResponse)
async def enriched_sheet_rows(body: EnrichedBatchRequest) -> SheetRowsResponse:
    raw = body.tickers or body.symbols or []
    tickers = _clean_tickers(raw)

    sheet_name = body.sheet_name or "Enriched"
    headers = get_headers_for_sheet(sheet_name)
    header_len = len(headers)

    if not tickers:
        return SheetRowsResponse(headers=headers, rows=[], meta={"count": 0, "sheet_name": sheet_name})

    uq_map = await _get_quotes_chunked(tickers)

    rows: List[List[Any]] = []
    for t in tickers:
        uq = uq_map.get(t.upper()) or UnifiedQuote(symbol=t.upper(), data_quality="MISSING", error="No data returned").finalize()
        eq = _uq_to_enriched(t, uq)

        try:
            # If EnrichedQuote already provides to_row(headers), prefer it.
            if hasattr(eq, "to_row"):
                row = eq.to_row(headers)  # type: ignore
            else:
                row = _eq_to_row(eq, headers)
        except Exception as exc:
            logger.warning("Row build failed for %s: %s", t, exc)
            row = [eq.symbol] + [None] * (header_len - 2) + [str(exc)]

        rows.append(_pad(list(row), header_len))

    # Preserve request order
    rows = _reorder_rows(headers, rows, tickers)

    return SheetRowsResponse(
        headers=headers,
        rows=rows,
        meta={
            "count": len(rows),
            "sheet_name": sheet_name,
            "time_utc": _now_utc_iso(),
        },
    )

__all__ = ["router"]
